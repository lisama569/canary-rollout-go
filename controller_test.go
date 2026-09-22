package canary

import (
	"errors"
	"strings"
	"testing"
)

// logDecision 打印一次调和的阶段、健康输入与判定依据，满足可解释性要求。
func logDecision(t *testing.T, d Decision, err error) {
	t.Helper()
	t.Logf("[调和] release=%s 阶段=%s(index=%d) seq=%d 健康输入={rate=%.3f healthy=%d} 阈值={rate>=%.3f healthy>=%d} => 健康=%v 连续健康=%d 连续不健康=%d 动作=%s 原因=%s phase=%s detail=%q err=%v",
		d.ReleaseID, d.StageName, d.StageIndex, d.Seq,
		d.SuccessRate, d.HealthyReplicas, d.MinSuccessRate, d.MinHealthy,
		d.Healthy, d.HealthyStreak, d.UnhealthyStreak,
		d.Action, d.Reason, d.Phase, d.Detail, err)
}

// attachLogger 注册控制器观测回调，使每次调和都打印判定依据。
func attachLogger(t *testing.T, c *Controller) {
	t.Helper()
	c.Observe(func(d Decision) {
		t.Logf("[观测] release=%s 阶段=%s(index=%d) seq=%d 健康输入={rate=%.3f healthy=%d} 判定健康=%v 动作=%s 原因=%s phase=%s : %s",
			d.ReleaseID, d.StageName, d.StageIndex, d.Seq,
			d.SuccessRate, d.HealthyReplicas, d.Healthy,
			d.Action, d.Reason, d.Phase, d.Detail)
	})
}

func sampleDesired(id string) DesiredState {
	return DesiredState{
		ReleaseID: id,
		Stages: []Stage{
			{Name: "canary-10", Weight: 10, Replicas: 1},
			{Name: "canary-50", Weight: 50, Replicas: 3},
			{Name: "full-100", Weight: 100, Replicas: 6},
		},
		Threshold:     HealthThreshold{MinSuccessRate: 0.95, MinHealthyReplicas: 1},
		PromoteAfter:  2,
		RollbackAfter: 3,
	}
}

func rejectReason(t *testing.T, err error) RejectReason {
	t.Helper()
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("期望 *RejectError，得到 %T: %v", err, err)
	}
	return re.Reason
}

func TestSubmitAndInvalidSpec(t *testing.T) {
	c := NewController()

	// 合法提交：立即进入第 0 阶段。
	st, err := c.Submit(sampleDesired("r1"))
	if err != nil {
		t.Fatalf("首次提交不应失败: %v", err)
	}
	if st.Phase != PhaseProgressing || st.Current.Name != "canary-10" ||
		st.AppliedWeight != 10 || st.AppliedReplicas != 1 {
		t.Fatalf("初始状态不符: %+v", st)
	}

	// 重复提交：幂等拒绝，可按原因区分，状态不变。
	st2, err := c.Submit(sampleDesired("r1"))
	if got := rejectReason(t, err); got != ReasonDuplicateSubmit {
		t.Fatalf("重复提交原因=%s，期望 %s", got, ReasonDuplicateSubmit)
	}
	if st2.StageIndex != 0 {
		t.Fatalf("重复提交不应改变状态，index=%d", st2.StageIndex)
	}

	// 非法声明：流量非单调。
	bad := sampleDesired("bad")
	bad.Stages[1].Weight = 10 // 与第 0 阶段相同，未严格递增
	_, badErr := c.Submit(bad)
	if rejectReason(t, badErr) != ReasonInvalidStage {
		t.Fatalf("非单调流量应被 InvalidStage 拒绝，得到 %v", badErr)
	}
	if !strings.Contains(badErr.Error(), "未严格递增") {
		t.Fatalf("拒绝信息应解释单调性，得到 %v", badErr)
	}

	// 末阶段必须全量。
	bad2 := sampleDesired("bad2")
	bad2.Stages[2].Weight = 90
	if _, err := c.Submit(bad2); rejectReason(t, err) != ReasonInvalidStage {
		t.Fatalf("末阶段非 100 应被拒绝，得到 %v", err)
	}

	// 不存在发布的 Get。
	if _, err := c.Get("missing"); rejectReason(t, err) != ReasonNotFound {
		t.Fatalf("缺失发布应 NotFound，得到 %v", err)
	}
}

func TestTerminalCannotReactivate(t *testing.T) {
	c := NewController()
	d := sampleDesired("term")
	d.PromoteAfter = 1
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}
	// 连续健康一路推进到成功。
	seq := int64(1)
	for i := 0; i < len(d.Stages); i++ {
		_, dec, err := c.Reconcile(HealthSnapshot{
			ReleaseID: "term", StageName: d.Stages[i].Name,
			Seq: seq, SuccessRate: 0.99, HealthyReplicas: 6,
		})
		if err != nil {
			t.Fatalf("阶段 %s 调和失败: %v", d.Stages[i].Name, err)
		}
		logDecision(t, dec, err)
		seq++
	}
	st, _ := c.Get("term")
	if st.Phase != PhaseSucceeded {
		t.Fatalf("期望成功终态，得到 %s", st.Phase)
	}
	// 终态重复提交不得重新激活。
	if _, err := c.Submit(d); rejectReason(t, err) != ReasonAlreadyTerminal {
		t.Fatalf("终态重复提交应 AlreadyTerminal，得到 %v", err)
	}
	// 终态快照被拒绝。
	_, dec, err := c.Reconcile(HealthSnapshot{
		ReleaseID: "term", StageName: "full-100", Seq: seq, SuccessRate: 0.99, HealthyReplicas: 6,
	})
	if rejectReason(t, err) != ReasonAlreadyTerminal {
		t.Fatalf("终态调和应 AlreadyTerminal，得到 %v", err)
	}
	logDecision(t, dec, err)
}

func TestPromoteToRejectsIllegalJumps(t *testing.T) {
	c := NewController()
	if _, err := c.Submit(sampleDesired("jump")); err != nil {
		t.Fatal(err)
	}

	// 跨阶（0 -> 2）：非法跳阶。
	if _, err := c.PromoteTo("jump", 2); rejectReason(t, err) != ReasonInvalidJump {
		t.Fatalf("跨阶应 InvalidJump，得到 %v", err)
	}
	// 原地（0 -> 0）：非法跳阶。
	if _, err := c.PromoteTo("jump", 0); rejectReason(t, err) != ReasonInvalidJump {
		t.Fatalf("原地推进应 InvalidJump，得到 %v", err)
	}
	// 后退（先合法前进一步再退回）。
	if _, err := c.PromoteTo("jump", 1); err != nil {
		t.Fatalf("0->1 应合法: %v", err)
	}
	if _, err := c.PromoteTo("jump", 0); rejectReason(t, err) != ReasonInvalidJump {
		t.Fatalf("后退应 InvalidJump，得到 %v", err)
	}
	// 越界下标。
	if _, err := c.PromoteTo("jump", 9); rejectReason(t, err) != ReasonInvalidJump {
		t.Fatalf("越界应 InvalidJump，得到 %v", err)
	}
	// 暂停态拒绝推进，恢复后 1->2 合法。
	if _, err := c.Pause("jump"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteTo("jump", 2); rejectReason(t, err) != ReasonPaused {
		t.Fatalf("暂停态应 Paused，得到 %v", err)
	}
	if _, err := c.Resume("jump"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteTo("jump", 2); err != nil {
		t.Fatalf("恢复后 1->2 应合法: %v", err)
	}
}
