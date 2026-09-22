package canary

import "fmt"

func invalidStage(format string, args ...any) error {
	return &RejectError{Reason: ReasonInvalidStage, Msg: fmt.Sprintf(format, args...)}
}

// validateDesired 校验期望发布声明。规则：
//   - ReleaseID 非空；
//   - 至少声明一个阶段，每阶段名称非空；
//   - Weight 取值 1..100 且末阶段必须为 100（全量），Replicas >= 1；
//   - 相邻阶段的 Weight 与 Replicas 都必须严格单调递增，
//     从声明层面保证“流量与实例比例随阶段单调变化”。
func validateDesired(d DesiredState) error {
	if d.ReleaseID == "" {
		return invalidStage("ReleaseID 不能为空")
	}
	if len(d.Stages) == 0 {
		return invalidStage("发布 %q 至少需要一个阶段", d.ReleaseID)
	}
	for i, st := range d.Stages {
		if st.Name == "" {
			return invalidStage("发布 %q 的第 %d 阶段名称为空", d.ReleaseID, i)
		}
		if st.Weight < 1 || st.Weight > 100 {
			return invalidStage("发布 %q 阶段 %q 的 Weight=%d 越界(1..100)",
				d.ReleaseID, st.Name, st.Weight)
		}
		if st.Replicas < 1 {
			return invalidStage("发布 %q 阶段 %q 的 Replicas=%d 必须 >=1",
				d.ReleaseID, st.Name, st.Replicas)
		}
		if i > 0 {
			prev := d.Stages[i-1]
			if st.Weight <= prev.Weight {
				return invalidStage("发布 %q 阶段 %q->%q 流量 %d->%d 未严格递增",
					d.ReleaseID, prev.Name, st.Name, prev.Weight, st.Weight)
			}
			if st.Replicas <= prev.Replicas {
				return invalidStage("发布 %q 阶段 %q->%q 实例数 %d->%d 未严格递增",
					d.ReleaseID, prev.Name, st.Name, prev.Replicas, st.Replicas)
			}
		}
	}
	if last := d.Stages[len(d.Stages)-1]; last.Weight != 100 {
		return invalidStage("发布 %q 末阶段 %q 的 Weight=%d 必须为 100(全量)",
			d.ReleaseID, last.Name, last.Weight)
	}
	if d.Threshold.MinSuccessRate < 0 || d.Threshold.MinSuccessRate > 1 {
		return invalidStage("发布 %q 的 MinSuccessRate=%v 越界(0..1)",
			d.ReleaseID, d.Threshold.MinSuccessRate)
	}
	if d.Threshold.MinHealthyReplicas < 0 {
		return invalidStage("发布 %q 的 MinHealthyReplicas 不能为负", d.ReleaseID)
	}
	return nil
}

// healthy 依据阈值判定单份健康输入是否达标。边界含于健康侧（>= 即健康），
// 因此“恰好等于阈值”被一致地判为健康，消除阈值附近的解释歧义。
func healthy(t HealthThreshold, rate float64, healthyReplicas int) bool {
	return rate >= t.MinSuccessRate && healthyReplicas >= t.MinHealthyReplicas
}
