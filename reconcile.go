package canary

import "fmt"

// Decision 记录一次调和的可解释结论：当时所处阶段、健康输入、
// 与阈值比较后的健康判定、采取的动作以及原因。主要服务于可解释性与测试日志。
type Decision struct {
	ReleaseID       string
	StageIndex      int
	StageName       string
	Seq             int64
	SuccessRate     float64
	HealthyReplicas int
	MinSuccessRate  float64
	MinHealthy      int
	Healthy         bool
	HealthyStreak   int
	UnhealthyStreak int
	Action          string
	Reason          RejectReason
	Detail          string
	Phase           Phase
}

// 调和动作的稳定标识。
const (
	ActionReject       = "Reject"
	ActionHold         = "Hold"
	ActionAdvance      = "Advance"
	ActionSucceed      = "Succeed"
	ActionRollback     = "Rollback"
	ActionIgnorePaused = "IgnorePaused"
)

// bookkeeping 保存调和过程中“防抖动”所需的连续计数。
// 它只存在于内存、不属于对外状态：只有连续达标 PromoteAfter 次才推进，
// 连续不达标 RollbackAfter 次才回滚；好坏交替（阈值附近抖动）会使计数反复清零，
// 从而既不推进也不回滚，更不会重复动作。
type bookkeeping struct {
	healthyStreak   int
	unhealthyStreak int
}

func max1(n int) int {
	if n <= 0 {
		return 1
	}
	return n
}

// Reconcile 依据一份健康快照对发布做一次调和。全程持有 Controller 锁，
// 保证并发调用串行化、结论与单次执行一致。
//
// 关键不变量：
//   - 已终结（Succeeded/RolledBack）发布：不接受任何快照，不会被重新激活；
//   - 暂停（Paused）期间：快照被忽略，不更新计数，避免暂停期“偷跑”推进；
//   - 乱序/过期：快照阶段必须与当前阶段一致，且其 Seq 在“同一阶段内”严格
//     大于已采信的 LastSeq；二者任一不符都拒绝（StaleSnapshot）。跨阶段的旧观测
//     由阶段名拦截，同阶段的迟到/重放由 seq 拦截，故乱序不会造成状态回退或重复动作；
//   - 推进：只能进入“直接后继”阶段，目标比例由声明保证严格更大（单调）；
//   - 回滚：单次发布至多一次（RollbackOnce），之后停留在 RolledBack 终态。
func (c *Controller) Reconcile(snap HealthSnapshot) (*ReleaseState, Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	d := Decision{
		ReleaseID:       snap.ReleaseID,
		Seq:             snap.Seq,
		SuccessRate:     snap.SuccessRate,
		HealthyReplicas: snap.HealthyReplicas,
	}

	st, ok := c.releases[snap.ReleaseID]
	if !ok {
		err := &RejectError{Reason: ReasonNotFound, Msg: "发布 " + snap.ReleaseID + " 不存在"}
		d.Action = ActionReject
		d.Reason = ReasonNotFound
		d.Detail = err.Msg
		return nil, d, err
	}
	d.StageIndex = st.StageIndex
	d.StageName = st.Current.Name
	d.Phase = st.Phase

	spec := c.specs[snap.ReleaseID]
	d.MinSuccessRate = spec.Threshold.MinSuccessRate
	d.MinHealthy = spec.Threshold.MinHealthyReplicas

	// 终态：任何后续调和都被拒绝，终态不可复活。
	if st.Phase == PhaseSucceeded || st.Phase == PhaseRolledBack {
		err := &RejectError{
			Reason: ReasonAlreadyTerminal,
			Msg:    fmt.Sprintf("发布 %s 已终结于 %s，拒绝调和", snap.ReleaseID, st.Phase),
		}
		d.Action = ActionReject
		d.Reason = ReasonAlreadyTerminal
		d.Detail = err.Msg
		c.emit(d)
		return cloneState(st), d, err
	}

	// 暂停：忽略健康证据，保持暂停态，不推进也不回滚。
	if st.Phase == PhasePaused {
		d.Action = ActionIgnorePaused
		d.Reason = ReasonPaused
		d.Detail = fmt.Sprintf("发布 %s 已暂停于 %s，快照 seq=%d 被忽略",
			snap.ReleaseID, st.Current.Name, snap.Seq)
		st.Reason = d.Detail
		c.emit(d)
		return cloneState(st), d, nil
	}

	// 乱序/过期：快照必须显式声明所属阶段，且与当前生效阶段一致；
	// 跨阶段的旧观测（含空阶段名，视为来源不明）在此被拦截。
	if snap.StageName != st.Current.Name {
		err := &RejectError{
			Reason: ReasonStaleSnapshot,
			Msg: fmt.Sprintf("快照阶段 %q 与当前阶段 %q 不符，疑似乱序，已拒绝",
				snap.StageName, st.Current.Name),
		}
		d.Action = ActionReject
		d.Reason = ReasonStaleSnapshot
		d.Detail = err.Msg
		c.emit(d)
		return cloneState(st), d, err
	}
	if snap.Seq <= st.LastSeq {
		err := &RejectError{
			Reason: ReasonStaleSnapshot,
			Msg: fmt.Sprintf("阶段 %q 的快照 seq=%d 不新于已采信 seq=%d，疑似乱序/重放，已拒绝",
				st.Current.Name, snap.Seq, st.LastSeq),
		}
		d.Action = ActionReject
		d.Reason = ReasonStaleSnapshot
		d.Detail = err.Msg
		c.emit(d)
		return cloneState(st), d, err
	}

	// 采信快照：推进当前阶段 LastSeq，杜绝同阶段旧快照再次生效。
	st.LastSeq = snap.Seq
	isHealthy := healthy(spec.Threshold, snap.SuccessRate, snap.HealthyReplicas)
	st.Healthy = isHealthy
	d.Healthy = isHealthy

	bk := c.books[snap.ReleaseID]
	promoteAfter := max1(spec.PromoteAfter)
	rollbackAfter := max1(spec.RollbackAfter)

	if isHealthy {
		bk.healthyStreak++
		bk.unhealthyStreak = 0
	} else {
		bk.unhealthyStreak++
		bk.healthyStreak = 0
	}
	d.HealthyStreak = bk.healthyStreak
	d.UnhealthyStreak = bk.unhealthyStreak

	// 自动回滚优先级高于推进；单次发布至多一次。
	if bk.unhealthyStreak >= rollbackAfter {
		if st.RollbackOnce {
			err := &RejectError{
				Reason: ReasonRollbackOnce,
				Msg:    fmt.Sprintf("发布 %s 已回滚过，拒绝重复回滚", snap.ReleaseID),
			}
			d.Action = ActionReject
			d.Reason = ReasonRollbackOnce
			d.Detail = err.Msg
			c.emit(d)
			return cloneState(st), d, err
		}
		st.RollbackOnce = true
		st.Phase = PhaseRolledBack
		st.Reason = fmt.Sprintf("连续 %d 份不健康快照(阈值 rate>=%.2f 且 healthy>=%d；实测 rate=%.2f healthy=%d)，自动回滚",
			bk.unhealthyStreak, spec.Threshold.MinSuccessRate, spec.Threshold.MinHealthyReplicas,
			snap.SuccessRate, snap.HealthyReplicas)
		d.Action = ActionRollback
		d.Reason = ReasonUnhealthy
		d.Detail = st.Reason
		d.Phase = st.Phase
		c.emit(d)
		return cloneState(st), d, nil
	}

	// 健康证据足够：推进到直接后继；已在末阶段则成功。
	if bk.healthyStreak >= promoteAfter {
		if st.StageIndex >= len(spec.Stages)-1 {
			st.Phase = PhaseSucceeded
			st.Reason = fmt.Sprintf("末阶段 %s 连续 %d 份健康，发布成功",
				st.Current.Name, bk.healthyStreak)
			d.Action = ActionSucceed
			d.Reason = ReasonNone
			d.Detail = st.Reason
			d.Phase = st.Phase
			c.emit(d)
			return cloneState(st), d, nil
		}

		nextIdx := st.StageIndex + 1
		next := spec.Stages[nextIdx]
		// 防御式单调校验：只能前进到直接后继，且比例严格更大。
		if next.Weight <= st.AppliedWeight || next.Replicas <= st.AppliedReplicas {
			err := &RejectError{
				Reason: ReasonNotMonotonic,
				Msg: fmt.Sprintf("拒绝推进到 %s：目标 weight=%d replicas=%d 未严格大于当前 weight=%d replicas=%d",
					next.Name, next.Weight, next.Replicas, st.AppliedWeight, st.AppliedReplicas),
			}
			d.Action = ActionReject
			d.Reason = ReasonNotMonotonic
			d.Detail = err.Msg
			c.emit(d)
			return cloneState(st), d, err
		}

		st.StageIndex = nextIdx
		st.Current = next
		st.AppliedWeight = next.Weight
		st.AppliedReplicas = next.Replicas
		// 进入新阶段：重置 seq 基线与连续计数。旧阶段健康“惯性”不会冲过新阶段，
		// 旧阶段的高 seq 迟到快照也会因阶段名不符被拒绝。
		st.LastSeq = 0
		bk.healthyStreak = 0
		bk.unhealthyStreak = 0
		st.Reason = fmt.Sprintf("连续健康达标，推进到阶段 %s(weight=%d replicas=%d)",
			next.Name, next.Weight, next.Replicas)
		d.Action = ActionAdvance
		d.Reason = ReasonNone
		d.Detail = st.Reason
		d.StageIndex = st.StageIndex
		d.StageName = st.Current.Name
		d.HealthyStreak = 0
		d.UnhealthyStreak = 0
		d.Phase = st.Phase
		c.emit(d)
		return cloneState(st), d, nil
	}

	// 证据不足或好坏抖动：停留在当前阶段，不采取任何动作。
	st.Reason = fmt.Sprintf("证据不足，停留于 %s：healthyStreak=%d/%d unhealthyStreak=%d/%d",
		st.Current.Name, bk.healthyStreak, promoteAfter, bk.unhealthyStreak, rollbackAfter)
	d.Action = ActionHold
	d.Reason = ReasonNoEvidence
	d.Detail = st.Reason
	c.emit(d)
	return cloneState(st), d, nil
}

// emit 在持锁状态下调用观测钩子；未注册时为空操作。
func (c *Controller) emit(d Decision) {
	if c.observe != nil {
		c.observe(d)
	}
}
