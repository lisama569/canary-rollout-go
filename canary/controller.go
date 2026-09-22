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
	decision := Decision{Action: "noop", Snapshot: snap}

	healthy := snap.SuccessRatio >= c.desired.Thresholds.MinSuccessRatio
	switch {
	case healthy:
		// 健康输入只负责清零连续违约计数并恢复推进；
		// 单个健康快照不会自动跨阶段，避免阈值抖动造成重复推进。
		c.status.BreachStreak = 0
		if c.status.State == StatePaused {
			c.status.State = StateProgressing
			decision.Action = "resumed"
			decision.Reason = fmt.Sprintf("success ratio %.4f >= threshold %.4f; streak reset",
				snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio)
		} else {
			decision.Action = "healthy"
			decision.Reason = fmt.Sprintf("success ratio %.4f >= threshold %.4f; hold at step %d",
				snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio, c.status.CurrentStep)
		}
	case !c.status.RolledBack && c.status.BreachStreak+1 >= c.desired.Thresholds.BreachLimit:
		// 连续不达标累计到阈值：自动回滚。单次发布至多回滚一次。
		c.status.BreachStreak++
		c.status.RolledBack = true
		c.status.State = StateRolledBack
		c.status.CurrentStep = -1
		decision.Action = "rollback"
		decision.Reason = fmt.Sprintf("success ratio %.4f < threshold %.4f; %d consecutive breaches >= limit %d",
			snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
			c.status.BreachStreak, c.desired.Thresholds.BreachLimit)
	default:
		// 未达回滚阈值：暂停推进，等待后续快照。
		c.status.BreachStreak++
		c.status.State = StatePaused
		decision.Action = "paused"
		decision.Reason = fmt.Sprintf("success ratio %.4f < threshold %.4f; breach streak %d < limit %d",
			snap.SuccessRatio, c.desired.Thresholds.MinSuccessRatio,
			c.status.BreachStreak, c.desired.Thresholds.BreachLimit)
	}
	return decision, nil
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

	target := c.desired.Steps[step]
	c.status.CurrentStep = step
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
		if i > 0 {
			prev := d.Steps[i-1]
			if s.TrafficPercent < prev.TrafficPercent ||
				s.InstancePercent < prev.InstancePercent {
				return fmt.Errorf("%w: step %d regresses", ErrNonMonotonic, i)
			}
		}
	}
	return nil
}
