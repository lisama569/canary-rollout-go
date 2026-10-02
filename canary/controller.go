package canary

import (
	"fmt"
	"sync"
)

// Decision 记录一次调和的判定依据，便于解释与测试断言。
type Decision struct {
	// Action 为本次调和采取的动作。
	Action string
	// Reason 为采取该动作的可解释原因。
	Reason string
	// Snapshot 为参与判定的快照（若有），零值表示未消费快照。
	Snapshot *HealthSnapshot
}

// Controller 为金丝雀发布控制器，并发安全。
type Controller struct {
	mu         sync.Mutex
	hasDesired bool
	desired    DesiredState
	status     Status
	// observations 记录每个阶段已计入的合格观察数据份数。
	// 只保留按阶段下标索引的计数器，内存占用随阶段数有界，
	// 不随历史数据总量增长。
	observations []int
	// lastStepSeq 记录每个阶段已消费的最大阶段内顺序号（单调水位线），
	// 用于识别并丢弃阶段内乱序/重复投递的数据。
	lastStepSeq []uint64
}

// New 创建一个空控制器。
func New() *Controller {
	return &Controller{
		status: Status{State: StateProgressing, CurrentStep: -1},
	}
}

// Submit 提交（或重放）期望状态，返回是否接受了新的期望状态。
func (c *Controller) Submit(d DesiredState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.submitLocked(d)
}

// Reconcile 依据当前期望状态与健康快照执行一次调和。
// snap 为 nil 表示无新快照，仅按期望状态推进。
func (c *Controller) Reconcile(snap *HealthSnapshot) (Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reconcileLocked(snap)
}

// Advance 请求推进到指定阶段下标，需通过健康准入。
func (c *Controller) Advance(step int) (Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advanceLocked(step)
}

// Status 返回当前发布状态的快照。
func (c *Controller) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

func (c *Controller) submitLocked(d DesiredState) error {
	if err := validate(d); err != nil {
		return err
	}
	// 幂等重放：相同或更旧的期望版本直接视为成功的空操作。
	if c.hasDesired && d.Revision <= c.desired.Revision {
		return nil
	}
	// 已终结的发布（完成或已回滚）不得被新期望状态重新激活。
	if c.status.State == StateCompleted || c.status.State == StateRolledBack {
		return ErrTerminal
	}
	c.desired = d
	c.hasDesired = true
	c.observations = make([]int, len(d.Steps))
	c.lastStepSeq = make([]uint64, len(d.Steps))
	c.status = Status{
		State:           StateProgressing,
		CurrentStep:     -1,
		AppliedRevision: d.Revision,
	}
	return nil
}

func (c *Controller) reconcileLocked(snap *HealthSnapshot) (Decision, error) {
	if !c.hasDesired {
		return Decision{Action: "noop", Reason: "no desired state submitted"}, ErrNoDesired
	}
	// 已终结的发布不再接受调和输入。
	if c.status.State == StateCompleted || c.status.State == StateRolledBack {
		return Decision{Action: "noop", Reason: "rollout is terminal: " + string(c.status.State)}, ErrTerminal
	}
	if snap == nil {
		return Decision{Action: "noop", Reason: "no new health snapshot"}, nil
	}
	// 乱序（含重复）快照：序号必须严格递增，直接丢弃，不影响任何计数。
	if snap.Seq <= c.status.LastSnapshot {
		return Decision{
			Action:   "ignored",
			Reason:   fmt.Sprintf("stale snapshot seq=%d, last=%d", snap.Seq, c.status.LastSnapshot),
			Snapshot: snap,
		}, ErrStaleSnapshot
	}
	c.status.LastSnapshot = snap.Seq

	// 旧版数据（StepSeq == 0）：无阶段归属与阶段内顺序，按原有语义只参与
	// 健康判定，不计入观察窗、不触发自动放量。
	if snap.StepSeq == 0 {
		return c.evaluateLocked(snap, false), nil
	}

	// 新版数据必须归属当前阶段：跨阶段（含尚未进入任何阶段）的数据直接丢弃，
	// 不计入观察、不触发回滚、不影响已有阶段。
	if snap.StepIndex != c.status.CurrentStep {
		return Decision{
			Action: "ignored",
			Reason: fmt.Sprintf("snapshot stage=%d does not match current step=%d; dropped",
				snap.StepIndex, c.status.CurrentStep),
			Snapshot: snap,
		}, ErrStageMismatch
	}
	// 阶段内必须按顺序到达：乱序/重复投递的数据丢弃，不重复计数。
	if snap.StepSeq <= c.lastStepSeq[snap.StepIndex] {
		return Decision{
			Action: "ignored",
			Reason: fmt.Sprintf("snapshot stepSeq=%d <= consumed watermark=%d at step=%d; dropped",
				snap.StepSeq, c.lastStepSeq[snap.StepIndex], snap.StepIndex),
			Snapshot: snap,
		}, ErrStepSeqStale
	}
	c.lastStepSeq[snap.StepIndex] = snap.StepSeq

	return c.evaluateLocked(snap, true), nil
}

// evaluateLocked 对一份已通过归属与顺序校验的快照做健康判定。
// countable 为 true 时（新版且归属当前阶段、按序到达）合格数据计入观察窗，
// 攒够当前阶段要求的份数后自动推进到下一阶段。
func (c *Controller) evaluateLocked(snap *HealthSnapshot, countable bool) Decision {
	decision := Decision{Action: "noop", Snapshot: snap}
	step := c.status.CurrentStep

	// 健康判定同时看两个维度：请求成功率与健康实例覆盖率，都达标才算合格。
	healthy := snap.SuccessRatio >= c.desired.Thresholds.MinSuccessRatio &&
		snap.HealthyInstanceRatio >= c.desired.Thresholds.MinHealthyInstanceRatio
	switch {
	case healthy:
		// 健康输入只负责清零连续违约计数并恢复推进；
		// 单个健康快照不会自动跨阶段，避免阈值抖动造成重复推进。
		c.status.BreachStreak = 0
		if c.status.State == StatePaused {
			c.status.State = StateProgressing
			decision.Action = "resumed"
			decision.Reason = fmt.Sprintf("success ratio %.4f >= %.4f, instance coverage %.4f >= %.4f; streak reset",
				snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
				snap.HealthyInstanceRatio, c.desired.Thresholds.MinHealthyInstanceRatio)
		} else {
			decision.Action = "healthy"
			decision.Reason = fmt.Sprintf("success ratio %.4f >= %.4f, instance coverage %.4f >= %.4f; hold at step %d",
				snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
				snap.HealthyInstanceRatio, c.desired.Thresholds.MinHealthyInstanceRatio, step)
		}
		if countable && step >= 0 {
			c.observations[step]++
			c.status.Observations = c.observations[step]
			required := c.desired.Steps[step].MinObservations
			decision.Reason += fmt.Sprintf("; observation %d/%d at step %d",
				c.observations[step], required, step)
			// 观察窗攒够：自动放量到下一阶段（MinObservations 为 0 表示无观察窗）。
			// 当前已处于最后一个阶段时无可推进目标，保持原地
			// （正常流程下进入最终阶段即终结，此处为防御性保护）。
			if required > 0 && c.observations[step] >= required && step < len(c.desired.Steps)-1 {
				return c.promoteLocked(snap)
			}
		}
	case !c.status.RolledBack && c.status.BreachStreak+1 >= c.desired.Thresholds.BreachLimit:
		// 连续不达标累计到阈值：自动回滚。单次发布至多回滚一次。
		c.status.BreachStreak++
		c.status.RolledBack = true
		c.status.State = StateRolledBack
		c.status.CurrentStep = -1
		c.status.Observations = 0
		c.status.RequiredObservations = 0
		decision.Action = "rollback"
		decision.Reason = fmt.Sprintf("success ratio %.4f < %.4f or instance coverage %.4f < %.4f; %d consecutive breaches >= limit %d",
			snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
			snap.HealthyInstanceRatio, c.desired.Thresholds.MinHealthyInstanceRatio,
			c.status.BreachStreak, c.desired.Thresholds.BreachLimit)
	default:
		// 未达回滚阈值：暂停推进，等待后续快照。
		c.status.BreachStreak++
		c.status.State = StatePaused
		decision.Action = "paused"
		decision.Reason = fmt.Sprintf("success ratio %.4f < %.4f or instance coverage %.4f < %.4f; breach streak %d < limit %d",
			snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
			snap.HealthyInstanceRatio, c.desired.Thresholds.MinHealthyInstanceRatio,
			c.status.BreachStreak, c.desired.Thresholds.BreachLimit)
	}
	return decision
}

// promoteLocked 在当前阶段观察窗攒够合格数据后自动推进到下一阶段。
// 进入最后一个阶段即发布完成。调用前调用方需保证当前阶段非终结态。
func (c *Controller) promoteLocked(snap *HealthSnapshot) Decision {
	from := c.status.CurrentStep
	to := from + 1
	c.observations[to] = 0
	c.lastStepSeq[to] = 0
	c.status.CurrentStep = to
	c.status.Observations = 0
	c.status.RequiredObservations = c.desired.Steps[to].MinObservations
	target := c.desired.Steps[to]
	if to == len(c.desired.Steps)-1 {
		c.status.State = StateCompleted
		return Decision{
			Action: "completed",
			Reason: fmt.Sprintf("observation window %d/%d filled at step %d; promoted to final step %d: traffic=%d%% instances=%d%%; rollout completed",
				c.observations[from], c.desired.Steps[from].MinObservations, from, to,
				target.TrafficPercent, target.InstancePercent),
			Snapshot: snap,
		}
	}
	return Decision{
		Action: "promoted",
		Reason: fmt.Sprintf("observation window %d/%d filled at step %d; auto-promoted to step %d: traffic=%d%% instances=%d%%",
			c.observations[from], c.desired.Steps[from].MinObservations, from, to,
			target.TrafficPercent, target.InstancePercent),
		Snapshot: snap,
	}
}

func (c *Controller) advanceLocked(step int) (Decision, error) {
	if !c.hasDesired {
		return Decision{Action: "noop", Reason: "no desired state submitted"}, ErrNoDesired
	}
	if c.status.State == StateCompleted || c.status.State == StateRolledBack {
		return Decision{Action: "noop", Reason: "rollout is terminal: " + string(c.status.State)}, ErrTerminal
	}
	if step < 0 || step >= len(c.desired.Steps) {
		return Decision{
			Action: "rejected",
			Reason: fmt.Sprintf("step %d not in declared range [0,%d)", step, len(c.desired.Steps)),
		}, fmt.Errorf("%w: requested %d", ErrStepOutOfRange, step)
	}
	if step < c.status.CurrentStep {
		return Decision{
			Action: "rejected",
			Reason: fmt.Sprintf("step %d is behind current step %d", step, c.status.CurrentStep),
		}, fmt.Errorf("%w: current=%d requested=%d", ErrStepRegression, c.status.CurrentStep, step)
	}
	// 重放相同阶段：幂等成功，不改变状态。
	if step == c.status.CurrentStep {
		return Decision{
			Action: "noop",
			Reason: fmt.Sprintf("already at step %d", step),
		}, nil
	}
	// 只允许推进到相邻的下一阶段，拒绝非法跳阶。
	if step > c.status.CurrentStep+1 {
		return Decision{
			Action: "rejected",
			Reason: fmt.Sprintf("cannot skip from step %d to step %d", c.status.CurrentStep, step),
		}, fmt.Errorf("%w: current=%d requested=%d", ErrSkipStep, c.status.CurrentStep, step)
	}
	// 健康准入：暂停（存在未消化的不达标快照）时不得推进。
	if c.status.State == StatePaused {
		return Decision{
			Action: "rejected",
			Reason: fmt.Sprintf("paused at step %d with breach streak %d",
				c.status.CurrentStep, c.status.BreachStreak),
		}, ErrPaused
	}
	// 观察窗准入：当前阶段声明了观察窗且未攒够合格数据时不得推进，
	// 与健康不达标（ErrPaused）可区分。
	if c.status.CurrentStep >= 0 {
		required := c.desired.Steps[c.status.CurrentStep].MinObservations
		if got := c.observations[c.status.CurrentStep]; required > 0 && got < required {
			return Decision{
					Action: "rejected",
					Reason: fmt.Sprintf("observation window at step %d not filled: %d/%d qualified observations",
						c.status.CurrentStep, got, required),
				}, fmt.Errorf("%w: step=%d got=%d required=%d",
					ErrInsufficientObservations, c.status.CurrentStep, got, required)
		}
	}

	target := c.desired.Steps[step]
	c.status.CurrentStep = step
	c.status.Observations = c.observations[step]
	c.status.RequiredObservations = target.MinObservations
	if step == len(c.desired.Steps)-1 {
		c.status.State = StateCompleted
		return Decision{
			Action: "completed",
			Reason: fmt.Sprintf("reached final step %d: traffic=%d%% instances=%d%%",
				step, target.TrafficPercent, target.InstancePercent),
		}, nil
	}
	return Decision{
		Action: "advanced",
		Reason: fmt.Sprintf("advanced to step %d: traffic=%d%% instances=%d%%",
			step, target.TrafficPercent, target.InstancePercent),
	}, nil
}

// validate 校验期望状态的合法性。
func validate(d DesiredState) error {
	if len(d.Steps) == 0 {
		return ErrNoSteps
	}
	if d.Thresholds.MinHealthyInstanceRatio < 0 || d.Thresholds.MinHealthyInstanceRatio > 1 {
		return fmt.Errorf("%w: minHealthyInstanceRatio=%v", ErrInvalidThreshold, d.Thresholds.MinHealthyInstanceRatio)
	}
	for i, s := range d.Steps {
		if s.TrafficPercent < 0 || s.TrafficPercent > 100 ||
			s.InstancePercent < 0 || s.InstancePercent > 100 {
			return fmt.Errorf("%w: step %d", ErrInvalidRatio, i)
		}
		if s.MinObservations < 0 {
			return fmt.Errorf("%w: step %d declares %d", ErrInvalidObservations, i, s.MinObservations)
		}
		if i > 0 {
			prev := d.Steps[i-1]
			if s.TrafficPercent < prev.TrafficPercent ||
				s.InstancePercent < prev.InstancePercent {
				return fmt.Errorf("%w: step %d regresses", ErrNonMonotonic, i)
			}
			// 相邻阶段的流量与实例比例必须真的发生变化，
			// 声明成一样的放量不算合法计划。
			if s.TrafficPercent == prev.TrafficPercent &&
				s.InstancePercent == prev.InstancePercent {
				return fmt.Errorf("%w: steps %d and %d both declare traffic=%d%% instances=%d%%",
					ErrNoProgress, i-1, i, s.TrafficPercent, s.InstancePercent)
			}
		}
	}
	return nil
}
