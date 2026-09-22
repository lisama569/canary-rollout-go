package canary

import (
	"sync"
	"testing"
)

// TestConcurrentReconcileDeterministic 验证多协程“重复并发调和同一份期望输入”时，
// 结论与单次执行一致：同一快照只可能被一份采信，其余并发重复全部按乱序/重放拒绝，
// 因此绝不会重复推进、重复回滚或状态回退。
//
// 说明：对一个阶段所需的“连续 N 份递增证据”，若不同 seq 的快照以最坏顺序
// （严格递减）乱序到达，系统按设计会丢弃旧证据、停留在当前阶段（宁可不动也不
// 依据旧观测推进）。因此确定性的并发等价场景是“同一份快照被并发重复投递”，
// 本测试逐阶段用屏障重复投递同一份证据，验证恰好只推进一次。
func TestConcurrentReconcileDeterministic(t *testing.T) {
	const dup = 32 // 每个阶段同一份快照被并发重复投递的次数
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("conc")
	d.PromoteAfter = 1
	d.RollbackAfter = 5
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}

	// 逐阶段屏障：多个协程并发投递“该阶段同一 seq、同一内容”的健康快照。
	// 无论锁如何交错，只有一份能采信（推进一次），其余全部 StaleSnapshot。
	for stageIdx, stDef := range d.Stages {
		var wg sync.WaitGroup
		for k := 0; k < dup; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, _ = c.Reconcile(HealthSnapshot{
					ReleaseID: "conc", StageName: stDef.Name, Seq: 1,
					SuccessRate: 0.99, HealthyReplicas: 9,
				})
			}()
		}
		wg.Wait()

		got, _ := c.Get("conc")
		if stageIdx < len(d.Stages)-1 {
			if got.StageIndex != stageIdx+1 {
				t.Fatalf("阶段 %q 并发重复投递后应恰好推进一次到 index=%d，实际 index=%d（%s）",
					stDef.Name, stageIdx+1, got.StageIndex, got.Reason)
			}
		} else {
			if got.Phase != PhaseSucceeded {
				t.Fatalf("末阶段并发重复投递后应成功，实际 phase=%s（%s）", got.Phase, got.Reason)
			}
		}
	}

	got, _ := c.Get("conc")
	if got.Phase != PhaseSucceeded || got.AppliedWeight != 100 || got.AppliedReplicas != 6 {
		t.Fatalf("并发调和后应为成功终态 100/6，得到 phase=%s %d/%d",
			got.Phase, got.AppliedWeight, got.AppliedReplicas)
	}
	if got.RollbackOnce {
		t.Fatal("全健康输入下不应发生回滚")
	}
	t.Logf("[并发结论] 每阶段 %d 份重复快照并发投递 => 唯一终态 phase=%s 阶段=%s weight=%d replicas=%d",
		dup, got.Phase, got.Current.Name, got.AppliedWeight, got.AppliedReplicas)
}

// TestConcurrentOutOfOrderConverges 验证“先固定第一份证据，再并发重复下一份证据”
// 的调和：第二份证据无论多少协程并发、重复或乱序投递，都恰好只被采信一次，
// 因而恰好推进一次，绝不重复推进。
func TestConcurrentOutOfOrderConverges(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("conv")
	d.PromoteAfter = 2
	d.RollbackAfter = 5
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}

	// 屏障一：确定采信第 1 份健康证据（streak=1，尚未推进）。
	if _, dec, err := c.Reconcile(HealthSnapshot{
		ReleaseID: "conv", StageName: "canary-10", Seq: 1,
		SuccessRate: 0.99, HealthyReplicas: 9,
	}); err != nil || dec.Action != ActionHold {
		t.Fatalf("第一份证据应 Hold，得到 action=%s err=%v", dec.Action, err)
	}

	// 屏障二：第 2 份证据（seq=2）被多个协程并发重复投递，
	// 同时夹杂 seq=1 的乱序重放；只有一份 seq=2 能采信并推进。
	const dup = 16
	var wg sync.WaitGroup
	for r := 0; r < dup; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = c.Reconcile(HealthSnapshot{
				ReleaseID: "conv", StageName: "canary-10", Seq: 2,
				SuccessRate: 0.99, HealthyReplicas: 9,
			})
		}()
		if r%2 == 0 {
			wg.Add(1)
			go func() { // 乱序重放旧证据，必须全部被拒
				defer wg.Done()
				_, _, _ = c.Reconcile(HealthSnapshot{
					ReleaseID: "conv", StageName: "canary-10", Seq: 1,
					SuccessRate: 0.99, HealthyReplicas: 9,
				})
			}()
		}
	}
	wg.Wait()

	got, _ := c.Get("conv")
	if got.StageIndex != 1 || got.Phase != PhaseProgressing {
		t.Fatalf("并发重复第 2 份证据应恰好推进到 index=1，得到 index=%d phase=%s（%s）",
			got.StageIndex, got.Phase, got.Reason)
	}
	if got.RollbackOnce {
		t.Fatal("全健康输入不应回滚")
	}
	t.Logf("[并发乱序结论] 并发重复投递后唯一状态 index=%d 阶段=%s（仅一次推进）",
		got.StageIndex, got.Current.Name)
}

// TestConcurrentSubmitIdempotent 验证同一期望状态被多个协程重复提交时，
// 只有一次创建，其余全部幂等拒绝，最终只有一个发布且状态唯一。
func TestConcurrentSubmitIdempotent(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("idem")

	const workers = 50
	var wg sync.WaitGroup
	var created, duplicate int64
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := c.Submit(d)
			switch {
			case err == nil:
				mu.Lock()
				created++
				mu.Unlock()
			case rejectReason(t, err) == ReasonDuplicateSubmit:
				mu.Lock()
				duplicate++
				mu.Unlock()
			default:
				t.Errorf("意外的提交错误: %v", err)
			}
			_ = st
		}()
	}
	wg.Wait()

	if created != 1 || duplicate != workers-1 {
		t.Fatalf("幂等提交：期望 1 创建 + %d 重复，得到 %d 创建 + %d 重复",
			workers-1, created, duplicate)
	}
	st, err := c.Get("idem")
	if err != nil {
		t.Fatalf("发布应存在: %v", err)
	}
	if st.StageIndex != 0 || st.AppliedWeight != 10 {
		t.Fatalf("重复提交后状态应唯一且处于初始阶段: %+v", st)
	}
	t.Logf("[幂等结论] %d 次并发提交 => 创建=%d 幂等忽略=%d 唯一阶段=%s",
		workers, created, duplicate, st.Current.Name)
}

// TestReplaySameStateMatchesSingleRun 验证同一批快照“重放两遍”，
// 第二遍全部因乱序/终态被拒绝，终态与只跑一遍一致。
func TestReplaySameStateMatchesSingleRun(t *testing.T) {
	c := NewController()
	attachLogger(t, c)
	d := sampleDesired("replay")
	d.PromoteAfter = 1
	if _, err := c.Submit(d); err != nil {
		t.Fatal(err)
	}

	run := func(round int) {
		seq := int64(1)
		for i, st := range d.Stages {
			_, dec, err := c.Reconcile(HealthSnapshot{
				ReleaseID: "replay", StageName: st.Name, Seq: seq,
				SuccessRate: 0.99, HealthyReplicas: 6,
			})
			if round == 1 {
				if err != nil {
					t.Fatalf("首轮 阶段%d 失败: %v", i, err)
				}
				logDecision(t, dec, err)
			} else {
				// 第二轮：末阶段为终态拒绝；前两阶段为乱序拒绝。
				if rejectReason(t, err) == ReasonNone {
					t.Fatalf("重放 round2 阶段%d 必须被拒绝，却成功", i)
				}
				logDecision(t, dec, err)
			}
			seq++
		}
	}
	run(1)
	first, _ := c.Get("replay")
	if first.Phase != PhaseSucceeded {
		t.Fatalf("首轮应成功，得到 %s", first.Phase)
	}
	run(2)
	second, _ := c.Get("replay")
	if second.Phase != PhaseSucceeded || second.LastSeq != first.LastSeq ||
		second.StageIndex != first.StageIndex {
		t.Fatalf("重放后状态必须与单轮一致：first=%+v second=%+v", first, second)
	}
	t.Logf("[重放结论] 重放前后一致: phase=%s index=%d LastSeq=%d",
		second.Phase, second.StageIndex, second.LastSeq)
}
