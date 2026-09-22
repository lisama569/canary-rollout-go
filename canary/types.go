// Package canary 实现一个本地金丝雀发布控制器：
// 以内存中的期望状态与实例健康快照模拟一次灰度发布。
package canary

import "errors"

// State 表示一次发布在其生命周期内所处的状态。
type State string

const (
	StateProgressing State = "Progressing" // 正在按阶段推进
	StatePaused      State = "Paused"      // 因健康指标不达标而暂停推进
	StateCompleted   State = "Completed"   // 已推进到最后一个阶段，发布完成
	StateRolledBack  State = "RolledBack"  // 已自动回滚，发布终结
)

// Step 描述灰度发布的一个阶段。
type Step struct {
	// TrafficPercent 为该阶段导入金丝雀实例的流量百分比 (0-100)。
	TrafficPercent int
	// InstancePercent 为该阶段金丝雀实例占全部实例的百分比 (0-100)。
	InstancePercent int
}

// Thresholds 描述健康准入与回滚的判定阈值。
type Thresholds struct {
	// MinSuccessRatio 为可接受的最低成功率 (0-1)。
	MinSuccessRatio float64
	// BreachLimit 为触发自动回滚所需的连续不达标快照次数。
	BreachLimit int
}

// DesiredState 为一次发布的期望状态（可重复提交，幂等）。
type DesiredState struct {
	// Revision 标识期望状态的版本，相同 Revision 的重复提交视为重放。
	Revision int64
	// Steps 为声明的发布阶段，流量与实例比例必须随阶段单调不减。
	Steps []Step
	// Thresholds 为健康判定阈值。
	Thresholds Thresholds
}

// HealthSnapshot 为一次实例健康快照，可能乱序到达。
type HealthSnapshot struct {
	// Seq 为单调递增的快照序号，用于识别并丢弃乱序旧快照。
	Seq uint64
	// SuccessRatio 为该快照窗口内的请求成功率 (0-1)。
	SuccessRatio float64
}

// Status 为控制器对外暴露的发布状态。
type Status struct {
	State         State
	CurrentStep   int  // 当前所处阶段下标，-1 表示尚未开始
	BreachStreak  int  // 连续不达标快照计数
	RolledBack    bool // 是否已回滚（单次发布至多一次）
	LastSnapshot  uint64
	AppliedRevision int64
}

// 可区分的拒绝原因。
var (
	ErrNoSteps        = errors.New("canary: desired state declares no steps")
	ErrNonMonotonic   = errors.New("canary: step ratios must be non-decreasing")
	ErrSkipStep       = errors.New("canary: cannot skip steps")
	ErrStepRegression = errors.New("canary: cannot move to an earlier step")
	ErrTerminal       = errors.New("canary: rollout is terminal and cannot be reactivated")
	ErrStaleSnapshot  = errors.New("canary: snapshot is out of order")
	ErrInvalidRatio   = errors.New("canary: step ratios must be within [0,100]")
	ErrStepOutOfRange = errors.New("canary: step index out of declared range")
	ErrPaused         = errors.New("canary: rollout is paused due to unhealthy metrics")
	ErrNoDesired      = errors.New("canary: no desired state submitted")
)
