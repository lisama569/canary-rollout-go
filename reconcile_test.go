package canary

import "testing"

// snap 构造一份健康快照，减少样板。
func snap(id, stage string, seq int64, rate float64, healthy int) HealthSnapshot {
	return HealthSnapshot{ReleaseID: id, StageName: stage, Seq: seq, SuccessRate: rate, HealthyReplicas: healthy}
}

// TestThresholdJitter 验证阈值附近好坏交替抖动时：不推进、不回滚、不重复动作。
// 阈值 rate>=0.95，需要连续 2 份健康才推进、连续 3 份不健康才回滚。
func TestThresholdJitter(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	if _, err := c.Submit(sampleDesired("jitter")); err != nil {
		t.Fatal(err)
	}

	// 输入序列：好、坏、好、好(被前面坏打断计数)... 在阈值附近反复抖动。
	inputs := []struct {
		rate    float64
		healthy int
		want    string
	}{
		{0.96, 1, ActionHold},    // 好  streak=1
		{0.94, 1, ActionHold},    // 坏  清零好计数
		{0.96, 1, ActionHold},    // 好  streak=1
		{0.95, 1, ActionAdvance}, // 恰处阈值边界(>= 即健康)，streak=2，推进
	}
	for i, in := range inputs {
		seq := int64(i + 1)
		stage := "canary-10"
		if in.want == ActionAdvance {
			// 推进后仍在同一调和返回，标签仍取推进前阶段即可。
		}
		st, d, err := c.Reconcile(snap("jitter", stage, seq, in.rate, in.healthy))
		logDecision(t, d, err)
		if err != nil {
			t.Fatalf("seq=%d 不应报错: %v", seq, err)
		}
		if d.Action != in.want {
			t.Fatalf("seq=%d 抖动输入 action=%s，期望 %s", seq, d.Action, in.want)
		}
		_ = st
	}

	st, _ := c.Get("jitter")
	if st.StageIndex != 1 || st.RollbackOnce {
		t.Fatalf("抖动后应恰好推进一次且未回滚: index=%d rollback=%v", st.StageIndex, st.RollbackOnce)
	}

	// 新阶段再制造“坏好坏”抖动：连续坏计数始终到不了 3，不得回滚。
	jitterBad := []struct {
		rate    float64
		healthy int
	}{
		{0.90, 1}, // 坏 1
		{0.99, 3}, // 好，清零
		{0.90, 1}, // 坏 1（而非 2）
		{0.95, 3}, // 好（边界，清零）
		{0.90, 1}, // 坏 1
	}
	for i, in := range jitterBad {
		seq := int64(10 + i)
		_, d, err := c.Reconcile(snap("jitter", "canary-50", seq, in.rate, in.healthy))
		logDecision(t, d, err)
		if err != nil {
			t.Fatalf("抖动 seq=%d 不应报错: %v", seq, err)
		}
		if d.Action != ActionHold {
			t.Fatalf("好坏抖动不应触发动作，seq=%d action=%s", seq, d.Action)
		}
	}
	st, _ = c.Get("jitter")
	if st.Phase != PhaseProgressing || st.RollbackOnce {
		t.Fatalf("抖动后应仍在进行中且未回滚: phase=%s rollback=%v", st.Phase, st.RollbackOnce)
	}
}

// TestBoundaryIsHealthy 验证“恰好等于阈值”被一致判为健康（边界含于健康侧）。
func TestBoundaryIsHealthy(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("boundary")
	d.PromoteAfter = 1
	d.RollbackAfter = 1
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}
	// rate 恰好 0.95、healthy 恰好 1，应判健康。
	if !healthy(d.Threshold, 0.95, 1) {
		t.Fatal("阈值边界应判为健康(>=)")
	}
	_, dec, err := c.Reconcile(snap("boundary", "canary-10", 1, 0.95, 1))
	logDecision(t, dec, err)
	if err != nil || dec.Action != ActionAdvance {
		t.Fatalf("边界健康应推进: action=%s err=%v", dec.Action, err)
	}
}

// TestOutOfOrderAndDuplicateSnapshots 验证快照乱序、重放、阶段不符均被拒绝，
// 不会造成状态回退或重复推进。
func TestOutOfOrderAndDuplicateSnapshots(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("ooo")
	d.PromoteAfter = 1
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}

	// seq=2 先到：健康，推进到 canary-50。
	_, d2, err := c.Reconcile(snap("ooo", "canary-10", 2, 0.99, 3))
	logDecision(t, d2, err)
	if err != nil || d2.Action != ActionAdvance {
		t.Fatalf("seq=2 应推进: action=%s err=%v", d2.Action, err)
	}

	// seq=1 迟到（乱序）：必须拒绝。
	_, d1, err := c.Reconcile(snap("ooo", "canary-10", 1, 0.99, 3))
	logDecision(t, d1, err)
	if rejectReason(t, err) != ReasonStaleSnapshot {
		t.Fatalf("乱序 seq=1 应 StaleSnapshot，得到 %v", err)
	}

	// seq=2 重放：拒绝，防止重复推进。
	_, d2dup, err := c.Reconcile(snap("ooo", "canary-10", 2, 0.99, 3))
	logDecision(t, d2dup, err)
	if rejectReason(t, err) != ReasonStaleSnapshot {
		t.Fatalf("重放 seq=2 应 StaleSnapshot，得到 %v", err)
	}

	// 阶段标签为旧阶段 canary-10、但 seq 更新：仍因阶段不符拒绝。
	_, dOldStage, err := c.Reconcile(snap("ooo", "canary-10", 3, 0.99, 3))
	logDecision(t, dOldStage, err)
	if rejectReason(t, err) != ReasonStaleSnapshot {
		t.Fatalf("旧阶段标签应 StaleSnapshot，得到 %v", err)
	}

	// 此时已进入 canary-50，新阶段 LastSeq 基线重置为 0；seq=1..3 均属旧阶段被拒。
	st, _ := c.Get("ooo")
	if st.StageIndex != 1 || st.LastSeq != 0 {
		t.Fatalf("乱序处理后应停留在 index=1 LastSeq=0(新基线)，得到 index=%d LastSeq=%d",
			st.StageIndex, st.LastSeq)
	}

	// 当前阶段正确标签 + 更新序号的健康快照应正常被采信。
	_, d3, err := c.Reconcile(snap("ooo", "canary-50", 4, 0.99, 3))
	logDecision(t, d3, err)
	if err != nil || d3.Action != ActionAdvance {
		t.Fatalf("合法快照应推进: action=%s err=%v", d3.Action, err)
	}
	st, _ = c.Get("ooo")
	if st.StageIndex != 2 {
		t.Fatalf("应推进到 index=2，得到 %d", st.StageIndex)
	}
}

// TestAutoRollbackOnce 验证连续不健康触发恰好一次自动回滚，之后停留在终态，
// 任何输入都不能再次回滚或重新激活。
func TestAutoRollbackOnce(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("rb")
	d.PromoteAfter = 1
	d.RollbackAfter = 2
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}

	// 两份连续不健康 -> 回滚。
	for i, seq := range []int64{1, 2} {
		_, dec, err := c.Reconcile(snap("rb", "canary-10", seq, 0.50, 0))
		logDecision(t, dec, err)
		if err != nil {
			t.Fatalf("不健康快照不应以错误返回: %v", err)
		}
		if i == 0 && dec.Action != ActionHold {
			t.Fatalf("第一份坏证据应 Hold，得到 %s", dec.Action)
		}
		if i == 1 {
			if dec.Action != ActionRollback || dec.Phase != PhaseRolledBack {
				t.Fatalf("第二份连续坏证据应回滚，得到 action=%s phase=%s", dec.Action, dec.Phase)
			}
		}
	}

	st, _ := c.Get("rb")
	if !st.RollbackOnce || st.Phase != PhaseRolledBack {
		t.Fatalf("应已回滚一次: %+v", st)
	}

	// 回滚后再来任何快照：终态拒绝，绝不二次回滚/复活。
	for _, seq := range []int64{3, 4, 100} {
		_, dec, err := c.Reconcile(snap("rb", "canary-10", seq, 0.99, 9))
		logDecision(t, dec, err)
		if rejectReason(t, err) != ReasonAlreadyTerminal {
			t.Fatalf("回滚终态应拒绝调和，seq=%d 得到 %v", seq, err)
		}
	}
	if _, err := c.Submit(d); rejectReason(t, err) != ReasonAlreadyTerminal {
		t.Fatalf("回滚终态重复提交应 AlreadyTerminal，得到 %v", err)
	}
}

// TestPausedDoesNotAdvanceOrRollback 验证暂停期间健康证据被忽略，既不推进也不回滚。
func TestPausedDoesNotAdvanceOrRollback(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("paused")
	d.PromoteAfter = 1
	d.RollbackAfter = 1
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pause("paused"); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []int64{1, 2, 3} {
		_, dec, err := c.Reconcile(snap("paused", "canary-10", seq, 0.1, 0))
		logDecision(t, dec, err)
		if err != nil || dec.Action != ActionIgnorePaused {
			t.Fatalf("暂停期应忽略快照: action=%s err=%v", dec.Action, err)
		}
	}
	st, _ := c.Get("paused")
	if st.Phase != PhasePaused || st.LastSeq != 0 || st.RollbackOnce {
		t.Fatalf("暂停后应保持暂停、不采信快照、不回滚: %+v", st)
	}
	// 恢复后第一份坏证据即应能触发回滚，证明暂停期没有累计计数。
	if _, err := c.Resume("paused"); err != nil {
		t.Fatal(err)
	}
	_, dec, err := c.Reconcile(snap("paused", "canary-10", 4, 0.1, 0))
	logDecision(t, dec, err)
	if dec.Action != ActionRollback {
		t.Fatalf("恢复后坏证据应回滚，得到 %s", dec.Action)
	}
}
