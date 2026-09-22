// Package canary 实现一个本地（内存态）金丝雀发布控制器。
//
// 它通过内存中的期望状态（DesiredState）与实例健康快照（HealthSnapshot）
// 模拟灰度发布的阶段推进、暂停、基于健康的准入以及自动回滚，
// 并保证乱序/抖动输入、并发调和与幂等重放下结论一致且可解释。
package canary

// Phase 表示一次发布在其生命周期中所处的宏观状态。
type Phase int

const (
	// PhasePending 发布已提交但尚未开始调和。
	PhasePending Phase = iota
	// PhaseProgressing 发布正在某一金丝雀阶段中运行。
	PhaseProgressing
	// PhasePaused 发布被人工暂停，不接受自动推进。
	PhasePaused
	// PhaseSucceeded 发布已走完所有阶段并最终成功（终态）。
	PhaseSucceeded
	// PhaseRolledBack 发布因健康不达标已自动回滚（终态，单次发布至多一次）。
	PhaseRolledBack
)

func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "Pending"
	case PhaseProgressing:
		return "Progressing"
	case PhasePaused:
		return "Paused"
	case PhaseSucceeded:
		return "Succeeded"
	case PhaseRolledBack:
		return "RolledBack"
	default:
		return "Unknown"
	}
}

// Stage 声明金丝雀发布中的一个阶段。
// Weight 为引流到金丝雀实例的流量百分比（0-100），
// Replicas 为该阶段金丝雀实例数。后一阶段的二者都必须严格大于前一阶段，
// 即流量与实例比例随阶段单调变化。
type Stage struct {
	Name     string
	Weight   int
	Replicas int
}

// HealthThreshold 声明健康准入阈值。
// 一个快照被判定为健康，当且仅当：
//   - SuccessRate >= MinSuccessRate，且
//   - HealthyReplicas >= MinHealthyReplicas。
//
// 阈值边界采用“含于健康侧”的约定（>= 即健康），消除抖动歧义。
type HealthThreshold struct {
	MinSuccessRate     float64
	MinHealthyReplicas int
}

// DesiredState 是调用方提交的期望发布声明（规格）。
// Generation 为单调递增的规格代号；当前实现以 ReleaseID 为幂等键，
// 相同 ReleaseID 的重复提交不产生新发布（已终结的也不会被重新激活）。
type DesiredState struct {
	ReleaseID  string
	Stages     []Stage
	Threshold  HealthThreshold
	Generation int64

	// PromoteAfter 为“连续多少份健康快照达标”才允许推进一个阶段；
	// RollbackAfter 为“连续多少份不健康快照”才触发自动回滚。
	// 二者 <=0 时按 1 处理。借助连续性要求，阈值附近好坏交替的抖动
	// 会反复清零计数，从而既不推进也不回滚（防抖/去重复动作）。
	PromoteAfter  int
	RollbackAfter int
}

// HealthSnapshot 是某一时刻观测到的金丝雀实例健康快照。
// Seq 为单调递增的观测序号，用于在乱序到达时判定新旧、防止状态回退。
type HealthSnapshot struct {
	ReleaseID       string
	StageName       string
	Seq             int64
	SuccessRate     float64
	HealthyReplicas int
}

// ReleaseState 是控制器为某次发布维护的实际状态（内存事实）。
type ReleaseState struct {
	ReleaseID       string
	Phase           Phase
	StageIndex      int   // 当前所处阶段下标（Progressing/Paused 时有意义）
	Current         Stage // 当前阶段的副本
	AppliedWeight   int   // 已生效流量百分比
	AppliedReplicas int   // 已生效金丝雀实例数
	// LastSeq 为“当前阶段”已采信的最大健康快照序号。乱序判定按同一阶段内
	// seq 严格递增进行；阶段之间的新旧由 StageName 匹配保证。进入新阶段后
	// 它从 0 重新计起，因此旧阶段再高 seq 的迟到快照也会因阶段不符被拒绝。
	LastSeq      int64  // 当前阶段已采信的最大序号
	Healthy      bool   // 最近一次采信快照的健康结论
	RollbackOnce bool   // 是否已经发生过自动回滚
	Reason       string // 最近一次判定/拒绝的可解释原因
}

// RejectReason 以稳定的枚举值区分各类拒绝/回滚原因，便于测试与上层判别。
type RejectReason string

const (
	// ReasonNone 无拒绝。
	ReasonNone RejectReason = ""
	// ReasonNotFound 发布不存在。
	ReasonNotFound RejectReason = "NotFound"
	// ReasonAlreadyTerminal 发布已处于终态，不得被重新激活。
	ReasonAlreadyTerminal RejectReason = "AlreadyTerminal"
	// ReasonDuplicateSubmit 同一发布已存在，重复提交被幂等忽略。
	ReasonDuplicateSubmit RejectReason = "DuplicateSubmit"
	// ReasonInvalidStage 阶段声明非法（空、缺名、比例越界等）。
	ReasonInvalidStage RejectReason = "InvalidStage"
	// ReasonInvalidJump 目标阶段不是当前阶段的直接后继（非法跳阶）。
	ReasonInvalidJump RejectReason = "InvalidJump"
	// ReasonNotMonotonic 目标阶段的流量或实例数未严格大于当前（越过/低于已声明比例）。
	ReasonNotMonotonic RejectReason = "NotMonotonic"
	// ReasonPaused 发布处于暂停态，推进被拒绝。
	ReasonPaused RejectReason = "Paused"
	// ReasonStaleSnapshot 快照序号过期或阶段不匹配，被拒绝以防状态回退。
	ReasonStaleSnapshot RejectReason = "StaleSnapshot"
	// ReasonRollbackOnce 本次发布已经回滚过，拒绝再次回滚。
	ReasonRollbackOnce RejectReason = "RollbackOnce"
	// ReasonNoEvidence 尚无可采信的健康证据，不能推进。
	ReasonNoEvidence RejectReason = "NoEvidence"
	// ReasonUnhealthy 当前健康证据未达阈值，准入被拒绝（可能触发回滚）。
	ReasonUnhealthy RejectReason = "Unhealthy"
)

// RejectError 携带稳定原因码的错误，便于按原因区分不同拒绝。
type RejectError struct {
	Reason RejectReason
	Msg    string
}

func (e *RejectError) Error() string {
	return string(e.Reason) + ": " + e.Msg
}
