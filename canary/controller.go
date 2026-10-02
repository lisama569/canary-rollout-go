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
	// hasStepSeq 标记当前阶段是否已消费过阶段内序号，
	// 使每个阶段的第一份数据（StepSeq 可为 0）总能被接受。
	hasStepSeq bool
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
	c.hasStepSeq = false
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
	decision := Decision{Action: "ignored", Snapshot: snap}

	// 阶段归属：只有属于当前阶段的数据才计入；来自其它阶段
	// （包括已经走过的阶段）的数据直接丢弃，不触发回滚、不改变计数。
	if snap.Step != c.status.CurrentStep {
		decision.Reason = fmt.Sprintf("data belongs to step %d, current step is %d; dropped",
			snap.Step, c.status.CurrentStep)
		return decision, fmt.Errorf("%w: data step=%d current=%d", ErrWrongStep, snap.Step, c.status.CurrentStep)
	}
	// 阶段内顺序：StepSeq 必须严格递增，重复投递或更早位置的数据丢弃。
	if c.hasStepSeq && snap.StepSeq <= c.status.LastStepSeq {
		decision.Reason = fmt.Sprintf("stale in-step seq=%d, last=%d at step %d; dropped",
			snap.StepSeq, c.status.LastStepSeq, c.status.CurrentStep)
		return decision, fmt.Errorf("%w: step=%d seq=%d last=%d",
			ErrStaleStepSeq, snap.Step, snap.StepSeq, c.status.LastStepSeq)
	}
	c.hasStepSeq = true
	c.status.LastStepSeq = snap.StepSeq

	th := c.desired.Thresholds
	successOK := snap.SuccessRatio >= th.MinSuccessRatio
	coverageOK := snap.HealthyRatio >= th.MinHealthyRatio
	healthy := successOK && coverageOK
	decision.Action = "noop"

	switch {
	case healthy:
		// 合格数据：清零连续违约计数、恢复推进，并计入当前阶段观察窗。
		c.status.BreachStreak = 0
		resumed := c.status.State == StatePaused
		if resumed {
			c.status.State = StateProgressing
		}
		c.status.Observations++
		return c.maybeAutoAdvanceLocked(decision, snap, resumed)
	case !c.status.RolledBack && c.status.BreachStreak+1 >= th.BreachLimit:
		// 连续不达标累计到阈值：自动回滚。单次发布至多回滚一次。
		c.status.BreachStreak++
		c.status.RolledBack = true
		c.status.State = StateRolledBack
		c.status.CurrentStep = -1
		decision.Action = "rollback"
		decision.Reason = fmt.Sprintf("successRatio=%.4f(ok=%v) healthyRatio=%.4f(ok=%v) vs thresholds %.4f/%.4f; %d consecutive breaches >= limit %d",
			snap.SuccessRatio, successOK, snap.HealthyRatio, coverageOK,
			th.MinSuccessRatio, th.MinHealthyRatio,
			c.status.BreachStreak, th.BreachLimit)
	default:
		// 未达回滚阈值：暂停推进，等待后续快照。
		c.status.BreachStreak++
		c.status.State = StatePaused
		decision.Action = "paused"
		decision.Reason = fmt.Sprintf("successRatio=%.4f(ok=%v) healthyRatio=%.4f(ok=%v) vs thresholds %.4f/%.4f; breach streak %d < limit %d",
			snap.SuccessRatio, successOK, snap.HealthyRatio, coverageOK,
			th.MinSuccessRatio, th.MinHealthyRatio,
			c.status.BreachStreak, th.BreachLimit)
	}
	return decision, nil
}

// maybeAutoAdvanceLocked 在计入一份合格数据后检查观察窗：
// 攒够当前阶段所需份数则自动进入下一阶段（末阶段则完成发布）。
func (c *Controller) maybeAutoAdvanceLocked(decision Decision, snap *HealthSnapshot, resumed bool) (Decision, error) {
	step := c.desired.Steps[c.status.CurrentStep]
	window := step.MinObservations
	if window > 0 && c.status.Observations >= window {
		if c.status.CurrentStep == len(c.desired.Steps)-1 {
			c.status.State = StateCompleted
			decision.Action = "completed"
			decision.Reason = fmt.Sprintf("observation window %d/%d filled at final step %d; rollout completed",
				c.status.Observations, window, c.status.CurrentStep)
			return decision, nil
		}
		from := c.status.CurrentStep
		c.enterStepLocked(from + 1)
		next := c.desired.Steps[c.status.CurrentStep]
		decision.Action = "auto-advanced"
		decision.Reason = fmt.Sprintf("observation window %d/%d filled at step %d; auto-advanced to step %d: traffic=%d%% instances=%d%%",
			c.status.Observations, window, from, from+1,
			next.TrafficPercent, next.InstancePercent)
		return decision, nil
	}
	if resumed {
		decision.Action = "resumed"
	} else {
		decision.Action = "healthy"
	}
	decision.Reason = fmt.Sprintf("qualified data at step %d (observations %d%s); successRatio=%.4f healthyRatio=%.4f",
		c.status.CurrentStep, c.status.Observations, windowSuffix(window),
		snap.SuccessRatio, snap.HealthyRatio)
	return decision, nil
}

// enterStepLocked 进入新阶段并重置阶段作用域的观察窗状态。
func (c *Controller) enterStepLocked(step int) {
	c.status.CurrentStep = step
	c.status.Observations = 0
	c.status.LastStepSeq = 0
	c.hasStepSeq = false
}

func windowSuffix(window int) string {
	if window > 0 {
		return fmt.Sprintf("/%d", window)
	}
	return " (no window)"
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
	// 观察窗准入：当前阶段声明了观察窗且尚未攒够合格数据时，
	// 拒绝提前推进（与 ErrPaused 的健康原因可区分）。
	if c.status.CurrentStep >= 0 {
		cur := c.desired.Steps[c.status.CurrentStep]
		if cur.MinObservations > 0 && c.status.Observations < cur.MinObservations {
			return Decision{
					Action: "rejected",
					Reason: fmt.Sprintf("observation window at step %d not filled: %d/%d qualified observations",
						c.status.CurrentStep, c.status.Observations, cur.MinObservations),
				}, fmt.Errorf("%w: step=%d have=%d need=%d",
					ErrInsufficientObservations, c.status.CurrentStep,
					c.status.Observations, cur.MinObservations)
		}
	}

	target := c.desired.Steps[step]
	c.enterStepLocked(step)
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
	for i, s := range d.Steps {
		if s.TrafficPercent < 0 || s.TrafficPercent > 100 ||
			s.InstancePercent < 0 || s.InstancePercent > 100 {
			return fmt.Errorf("%w: step %d", ErrInvalidRatio, i)
		}
		if s.MinObservations < 0 {
			return fmt.Errorf("%w: step %d", ErrInvalidWindow, i)
		}
		if i > 0 {
			prev := d.Steps[i-1]
			if s.TrafficPercent < prev.TrafficPercent ||
				s.InstancePercent < prev.InstancePercent {
				return fmt.Errorf("%w: step %d regresses", ErrNonMonotonic, i)
			}
			// 相邻阶段流量与实例比例完全一致，不构成有效放量，拒绝。
			if s.TrafficPercent == prev.TrafficPercent &&
				s.InstancePercent == prev.InstancePercent {
				return fmt.Errorf("%w: steps %d and %d are identical",
					ErrIdenticalSteps, i-1, i)
			}
		}
	}
	return nil
}
