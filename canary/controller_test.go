package canary

import (
	"errors"
	"sync"
	"testing"
)

// logDecision 在单测日志中打印阶段、健康输入与判定依据。
func logDecision(t *testing.T, c *Controller, d Decision, err error, label string) {
	t.Helper()
	s := c.Status()
	if d.Snapshot != nil {
		t.Logf("[%s] state=%s step=%d obs=%d | health seq=%d step=%d stepSeq=%d successRatio=%.4f healthyRatio=%.4f | action=%s reason=%q err=%v",
			label, s.State, s.CurrentStep, s.Observations,
			d.Snapshot.Seq, d.Snapshot.Step, d.Snapshot.StepSeq,
			d.Snapshot.SuccessRatio, d.Snapshot.HealthyRatio,
			d.Action, d.Reason, err)
	} else {
		t.Logf("[%s] state=%s step=%d streak=%d rolledBack=%v | health=<none> | action=%s reason=%q err=%v",
			label, s.State, s.CurrentStep, s.BreachStreak, s.RolledBack,
			d.Action, d.Reason, err)
	}
}

func standardDesired() DesiredState {
	return DesiredState{
		Revision: 1,
		Steps: []Step{
			{TrafficPercent: 10, InstancePercent: 10},
			{TrafficPercent: 30, InstancePercent: 30},
			{TrafficPercent: 50, InstancePercent: 50},
			{TrafficPercent: 100, InstancePercent: 100},
		},
		Thresholds: Thresholds{MinSuccessRatio: 0.95, BreachLimit: 3},
	}
}

// windowedDesired 返回每个阶段都带观察窗（2 份合格数据）的发布计划。
func windowedDesired() DesiredState {
	return DesiredState{
		Revision: 1,
		Steps: []Step{
			{TrafficPercent: 10, InstancePercent: 10, MinObservations: 2},
			{TrafficPercent: 30, InstancePercent: 30, MinObservations: 2},
			{TrafficPercent: 100, InstancePercent: 100, MinObservations: 2},
		},
		Thresholds: Thresholds{MinSuccessRatio: 0.95, MinHealthyRatio: 0.90, BreachLimit: 3},
	}
}

// qualifiedSnap 构造一份两个维度都达标的健康数据。
func qualifiedSnap(seq uint64, step int, stepSeq uint64) *HealthSnapshot {
	return &HealthSnapshot{
		Seq: seq, Step: step, StepSeq: stepSeq,
		SuccessRatio: 0.99, HealthyRatio: 0.99,
	}
}

func TestValidateSteps(t *testing.T) {
	cases := []struct {
		name string
		d    DesiredState
		want error
	}{
		{"no steps", DesiredState{Revision: 1, Thresholds: Thresholds{MinSuccessRatio: 0.9, BreachLimit: 1}}, ErrNoSteps},
		{"ratio out of range", DesiredState{Revision: 1, Steps: []Step{{TrafficPercent: 101, InstancePercent: 0}}}, ErrInvalidRatio},
		{"traffic regresses", DesiredState{Revision: 1, Steps: []Step{{TrafficPercent: 50, InstancePercent: 10}, {TrafficPercent: 40, InstancePercent: 20}}}, ErrNonMonotonic},
		{"instances regress", DesiredState{Revision: 1, Steps: []Step{{TrafficPercent: 10, InstancePercent: 50}, {TrafficPercent: 20, InstancePercent: 40}}}, ErrNonMonotonic},
		{"identical adjacent steps", DesiredState{Revision: 1, Steps: []Step{{TrafficPercent: 10, InstancePercent: 10}, {TrafficPercent: 10, InstancePercent: 10}}}, ErrIdenticalSteps},
		{"negative observation window", DesiredState{Revision: 1, Steps: []Step{{TrafficPercent: 10, InstancePercent: 10, MinObservations: -1}}}, ErrInvalidWindow},
		{"valid", standardDesired(), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			err := c.Submit(tc.d)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Submit() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAdvanceRules(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 非法跳阶：0 -> 2 必须被拒绝。
	d, err := c.Advance(2)
	logDecision(t, c, d, err, "skip 0->2")
	if !errors.Is(err, ErrSkipStep) {
		t.Fatalf("advance 0->2 error = %v, want ErrSkipStep", err)
	}
	if got := c.Status().CurrentStep; got != -1 {
		t.Fatalf("rejected advance moved step to %d", got)
	}

	// 越过已声明阶段：下标越界必须被拒绝。
	d, err = c.Advance(4)
	logDecision(t, c, d, err, "out of range 4")
	if !errors.Is(err, ErrStepOutOfRange) {
		t.Fatalf("advance 4 error = %v, want ErrStepOutOfRange", err)
	}

	// 正常推进 0 -> 1 -> 2，比例单调上升。
	for step := 0; step <= 2; step++ {
		d, err = c.Advance(step)
		logDecision(t, c, d, err, "advance")
		if err != nil || d.Action != "advanced" {
			t.Fatalf("advance to %d: action=%s err=%v", step, d.Action, err)
		}
	}

	// 幂等重放：重复推进到当前阶段为空操作。
	d, err = c.Advance(2)
	logDecision(t, c, d, err, "replay step 2")
	if err != nil || d.Action != "noop" {
		t.Fatalf("replay advance: action=%s err=%v", d.Action, err)
	}

	// 回退到更早阶段必须被拒绝。
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "regress 2->1")
	if !errors.Is(err, ErrStepRegression) {
		t.Fatalf("regress error = %v, want ErrStepRegression", err)
	}

	// 推进到最终阶段后发布完成，终结态不可再推进。
	d, err = c.Advance(3)
	logDecision(t, c, d, err, "advance to final")
	if err != nil || c.Status().State != StateCompleted {
		t.Fatalf("final advance: action=%s state=%s err=%v", d.Action, c.Status().State, err)
	}
	d, err = c.Advance(3)
	logDecision(t, c, d, err, "advance after complete")
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("advance after complete error = %v, want ErrTerminal", err)
	}
}

func TestThresholdJitter(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 指标在阈值 0.95 附近抖动：单个健康快照即清零违约计数，
	// 交替抖动不得累计到回滚阈值，也不得引发状态回退。
	ratios := []float64{0.94, 0.96, 0.93, 0.97, 0.94, 0.95, 0.94, 0.99}
	for i, r := range ratios {
		d, err := c.Reconcile(&HealthSnapshot{Seq: uint64(i + 1), Step: 0, StepSeq: uint64(i + 1), SuccessRatio: r})
		logDecision(t, c, d, err, "jitter")
		if err != nil {
			t.Fatalf("reconcile seq=%d: %v", i+1, err)
		}
		s := c.Status()
		if s.State == StateRolledBack || s.RolledBack {
			t.Fatalf("jitter caused rollback at seq=%d", i+1)
		}
		if s.CurrentStep != 0 {
			t.Fatalf("jitter moved step to %d", s.CurrentStep)
		}
	}
}

func TestAutoRollbackAtMostOnce(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 连续 3 次不达标（达到 BreachLimit）触发恰好一次自动回滚。
	rollbacks := 0
	for seq := uint64(1); seq <= 3; seq++ {
		d, err := c.Reconcile(&HealthSnapshot{Seq: seq, Step: 0, StepSeq: seq, SuccessRatio: 0.80})
		logDecision(t, c, d, err, "breach")
		if err != nil {
			t.Fatalf("reconcile seq=%d: %v", seq, err)
		}
		if d.Action == "rollback" {
			rollbacks++
		}
	}
	if rollbacks != 1 {
		t.Fatalf("rollback count = %d, want exactly 1", rollbacks)
	}
	if s := c.Status(); s.State != StateRolledBack || !s.RolledBack {
		t.Fatalf("state = %s rolledBack=%v, want RolledBack", s.State, s.RolledBack)
	}

	// 回滚后更多坏快照不得引发第二次回滚，终结态不得被重新激活。
	d, err := c.Reconcile(&HealthSnapshot{Seq: 4, Step: 0, StepSeq: 4, SuccessRatio: 0.10})
	logDecision(t, c, d, err, "post-rollback snapshot")
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("reconcile after rollback error = %v, want ErrTerminal", err)
	}
	if c.Status().State != StateRolledBack {
		t.Fatalf("state regressed to %s", c.Status().State)
	}
	if err := c.Submit(DesiredState{Revision: 2, Steps: standardDesired().Steps, Thresholds: standardDesired().Thresholds}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("resubmit after rollback error = %v, want ErrTerminal", err)
	}
}

func TestOutOfOrderSnapshots(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 先消费 seq=1（坏），随后乱序到达的旧快照必须被丢弃，
	// 不得重复计入违约，也不得清零已有计数。
	d, err := c.Reconcile(&HealthSnapshot{Seq: 1, Step: 0, StepSeq: 1, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "seq=1 bad")
	if err != nil || c.Status().BreachStreak != 1 {
		t.Fatalf("seq=1: streak=%d err=%v", c.Status().BreachStreak, err)
	}
	for _, seq := range []uint64{1, 1, 0} {
		d, err = c.Reconcile(&HealthSnapshot{Seq: seq, Step: 0, StepSeq: seq, SuccessRatio: 0.99})
		logDecision(t, c, d, err, "stale")
		if !errors.Is(err, ErrStaleSnapshot) {
			t.Fatalf("stale seq=%d error = %v, want ErrStaleSnapshot", seq, err)
		}
		if c.Status().BreachStreak != 1 {
			t.Fatalf("stale snapshot mutated streak to %d", c.Status().BreachStreak)
		}
	}
	// 更新的乱序坏快照继续累计，达到阈值后回滚恰好一次。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 2, Step: 0, StepSeq: 2, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "seq=2 bad")
	d, err = c.Reconcile(&HealthSnapshot{Seq: 3, Step: 0, StepSeq: 3, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "seq=3 bad")
	if d.Action != "rollback" {
		t.Fatalf("seq=3 action = %s, want rollback", d.Action)
	}
}

func TestPausedAdvanceRejected(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}
	// 一次不达标使发布暂停，暂停期间推进必须被拒绝。
	d, err := c.Reconcile(&HealthSnapshot{Seq: 1, Step: 0, StepSeq: 1, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "pause")
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "advance while paused")
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("advance while paused error = %v, want ErrPaused", err)
	}
	// 健康快照恢复推进后，推进被允许。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 2, Step: 0, StepSeq: 2, SuccessRatio: 0.99})
	logDecision(t, c, d, err, "resume")
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "advance after resume")
	if err != nil || d.Action != "advanced" {
		t.Fatalf("advance after resume: action=%s err=%v", d.Action, err)
	}
}

func TestConcurrentReconcile(t *testing.T) {
	c := New()
	if err := c.Submit(standardDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 16 个协程并发调和：8 个提交相同快照（重放），8 个提交递增快照。
	// 最终状态必须与单次顺序执行一致：乱序/重复被丢弃，回滚至多一次。
	const workers = 16
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 50; i++ {
				var d Decision
				var err error
				if w%2 == 0 {
					// 重放同一快照，应被幂等丢弃。
					d, err = c.Reconcile(&HealthSnapshot{Seq: 1, Step: 0, StepSeq: 1, SuccessRatio: 0.99})
				} else {
					// 递增序号的坏快照，最终应触发恰好一次回滚。
					d, err = c.Reconcile(&HealthSnapshot{Seq: uint64(i + 2), Step: 0, StepSeq: uint64(i + 2), SuccessRatio: 0.50})
				}
				if err != nil && !errors.Is(err, ErrStaleSnapshot) && !errors.Is(err, ErrTerminal) {
					t.Errorf("worker %d iter %d: unexpected error %v (action=%s)", w, i, err, d.Action)
				}
			}
		}(w)
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	s := c.Status()
	t.Logf("[concurrent] final state=%s step=%d streak=%d rolledBack=%v lastSnapshot=%d",
		s.State, s.CurrentStep, s.BreachStreak, s.RolledBack, s.LastSnapshot)
	if s.State != StateRolledBack || !s.RolledBack {
		t.Fatalf("final state = %s rolledBack=%v, want RolledBack", s.State, s.RolledBack)
	}
	// 回滚恰由第 3 次连续违约触发；回滚后快照不再被消费，
	// 因此 lastSnapshot 落在 [3, 51] 且每个序号至多被消费一次。
	if s.LastSnapshot < 3 || s.LastSnapshot > 51 {
		t.Fatalf("lastSnapshot = %d, want within [3,51]", s.LastSnapshot)
	}
}

func TestIdempotentReplay(t *testing.T) {
	c := New()
	d := standardDesired()

	// 同一期望状态重复提交：幂等，状态不变。
	for i := 0; i < 3; i++ {
		if err := c.Submit(d); err != nil {
			t.Fatalf("submit replay %d: %v", i, err)
		}
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}
	if err := c.Submit(d); err != nil {
		t.Fatalf("submit replay after advance: %v", err)
	}
	if got := c.Status().CurrentStep; got != 0 {
		t.Fatalf("replay reset step to %d", got)
	}

	// 更旧版本的期望状态不得覆盖当前状态。
	stale := d
	stale.Revision = 0
	if err := c.Submit(stale); err != nil {
		t.Fatalf("stale submit: %v", err)
	}
	if got := c.Status().AppliedRevision; got != 1 {
		t.Fatalf("stale submit changed revision to %d", got)
	}

	// 并发提交同一期望状态 + 并发推进，最终状态与单次执行一致。
	const workers = 8
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 20; i++ {
				if err := c.Submit(d); err != nil {
					t.Errorf("concurrent submit: %v", err)
				}
				if _, err := c.Advance(1); err != nil && !errors.Is(err, ErrTerminal) {
					t.Errorf("concurrent advance: %v", err)
				}
			}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	s := c.Status()
	t.Logf("[replay] final state=%s step=%d revision=%d", s.State, s.CurrentStep, s.AppliedRevision)
	if s.CurrentStep != 1 || s.AppliedRevision != 1 {
		t.Fatalf("final step=%d revision=%d, want step=1 revision=1", s.CurrentStep, s.AppliedRevision)
	}
}

func TestAutoAdvanceOnObservationWindow(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 第 1 份合格数据：观察窗 1/2，停在原地。
	d, err := c.Reconcile(qualifiedSnap(1, 0, 1))
	logDecision(t, c, d, err, "step0 obs 1/2")
	if err != nil || d.Action != "healthy" {
		t.Fatalf("obs 1/2: action=%s err=%v", d.Action, err)
	}
	if s := c.Status(); s.CurrentStep != 0 || s.Observations != 1 {
		t.Fatalf("obs 1/2: step=%d obs=%d, want step=0 obs=1", s.CurrentStep, s.Observations)
	}

	// 第 2 份合格数据：观察窗攒满，自动进入阶段 1，计数重置。
	d, err = c.Reconcile(qualifiedSnap(2, 0, 2))
	logDecision(t, c, d, err, "step0 obs 2/2 -> auto")
	if err != nil || d.Action != "auto-advanced" {
		t.Fatalf("window filled: action=%s err=%v", d.Action, err)
	}
	if s := c.Status(); s.CurrentStep != 1 || s.Observations != 0 {
		t.Fatalf("after auto-advance: step=%d obs=%d, want step=1 obs=0", s.CurrentStep, s.Observations)
	}

	// 阶段 1 同样攒满 2 份后自动进入末阶段。
	for i := uint64(1); i <= 2; i++ {
		d, err = c.Reconcile(qualifiedSnap(2+i, 1, i))
		logDecision(t, c, d, err, "step1 obs")
	}
	if d.Action != "auto-advanced" || c.Status().CurrentStep != 2 {
		t.Fatalf("step1 window: action=%s step=%d", d.Action, c.Status().CurrentStep)
	}

	// 末阶段观察窗攒满后发布自动完成，进入终结态。
	for i := uint64(1); i <= 2; i++ {
		d, err = c.Reconcile(qualifiedSnap(4+i, 2, i))
		logDecision(t, c, d, err, "step2 obs")
	}
	if d.Action != "completed" || c.Status().State != StateCompleted {
		t.Fatalf("final window: action=%s state=%s", d.Action, c.Status().State)
	}
	// 终结态不再接受任何数据。
	d, err = c.Reconcile(qualifiedSnap(7, 2, 3))
	logDecision(t, c, d, err, "post-complete data")
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("reconcile after complete error = %v, want ErrTerminal", err)
	}
}

func TestAdvanceBlockedReasons(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 观察窗 0/2：提前推进被挡，原因是数据不够。
	d, err := c.Advance(1)
	logDecision(t, c, d, err, "advance with 0/2")
	if !errors.Is(err, ErrInsufficientObservations) {
		t.Fatalf("advance with 0/2 error = %v, want ErrInsufficientObservations", err)
	}

	// 一份不达标数据使发布暂停：推进被挡，原因是健康不达标（可与数据不够区分）。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 1, Step: 0, StepSeq: 1, SuccessRatio: 0.50, HealthyRatio: 0.99})
	logDecision(t, c, d, err, "breach -> paused")
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "advance while paused")
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("advance while paused error = %v, want ErrPaused", err)
	}
	if errors.Is(err, ErrInsufficientObservations) {
		t.Fatalf("paused rejection must be distinguishable from insufficient observations")
	}

	// 恢复健康后观察窗只有 1/2：仍然因数据不够被挡。
	d, err = c.Reconcile(qualifiedSnap(2, 0, 2))
	logDecision(t, c, d, err, "resume obs 1/2")
	if d.Action != "resumed" {
		t.Fatalf("resume: action=%s", d.Action)
	}
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "advance with 1/2")
	if !errors.Is(err, ErrInsufficientObservations) {
		t.Fatalf("advance with 1/2 error = %v, want ErrInsufficientObservations", err)
	}

	// 攒满后自动放量，无需人工推进。
	d, err = c.Reconcile(qualifiedSnap(3, 0, 3))
	logDecision(t, c, d, err, "obs 2/2 -> auto")
	if d.Action != "auto-advanced" || c.Status().CurrentStep != 1 {
		t.Fatalf("auto-advance: action=%s step=%d", d.Action, c.Status().CurrentStep)
	}
}

func TestCrossStepAndOutOfOrderDataDropped(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 属于其它阶段的数据：丢弃，不计入观察窗、不累计违约。
	d, err := c.Reconcile(qualifiedSnap(1, 1, 1))
	logDecision(t, c, d, err, "wrong step 1")
	if !errors.Is(err, ErrWrongStep) {
		t.Fatalf("wrong step error = %v, want ErrWrongStep", err)
	}
	if s := c.Status(); s.Observations != 0 || s.BreachStreak != 0 {
		t.Fatalf("wrong-step data mutated state: obs=%d streak=%d", s.Observations, s.BreachStreak)
	}

	// 阶段内第一份合格数据（StepSeq=5）：计入，obs=1。
	d, err = c.Reconcile(qualifiedSnap(2, 0, 5))
	logDecision(t, c, d, err, "step0 stepSeq=5")
	if err != nil || c.Status().Observations != 1 {
		t.Fatalf("stepSeq=5: obs=%d err=%v", c.Status().Observations, err)
	}

	// 阶段内重复与更早位置的数据：丢弃，不计数。
	for i, ss := range []uint64{5, 5, 3} {
		d, err = c.Reconcile(&HealthSnapshot{Seq: uint64(3 + i), Step: 0, StepSeq: ss, SuccessRatio: 0.99, HealthyRatio: 0.99})
		logDecision(t, c, d, err, "stale in-step")
		if !errors.Is(err, ErrStaleStepSeq) {
			t.Fatalf("stale stepSeq=%d error = %v, want ErrStaleStepSeq", ss, err)
		}
		if c.Status().Observations != 1 {
			t.Fatalf("stale in-step data mutated obs to %d", c.Status().Observations)
		}
	}

	// 顺序向前的第 2 份合格数据：攒满观察窗，自动进入阶段 1。
	d, err = c.Reconcile(qualifiedSnap(10, 0, 6))
	logDecision(t, c, d, err, "step0 stepSeq=6 -> auto")
	if d.Action != "auto-advanced" || c.Status().CurrentStep != 1 {
		t.Fatalf("auto-advance: action=%s step=%d", d.Action, c.Status().CurrentStep)
	}

	// 已走过阶段的数据（即使不健康）：丢弃，不触发回滚、不让阶段倒退。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 11, Step: 0, StepSeq: 7, SuccessRatio: 0.10, HealthyRatio: 0.10})
	logDecision(t, c, d, err, "old step bad data")
	if !errors.Is(err, ErrWrongStep) {
		t.Fatalf("old step data error = %v, want ErrWrongStep", err)
	}
	if s := c.Status(); s.CurrentStep != 1 || s.BreachStreak != 0 || s.RolledBack {
		t.Fatalf("old-step data regressed state: step=%d streak=%d rolledBack=%v",
			s.CurrentStep, s.BreachStreak, s.RolledBack)
	}
}

func TestUnhealthyCoverageBlocksWindow(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 成功率达标但实例健康覆盖不达标：不算合格数据，不计入观察窗，且累计违约。
	d, err := c.Reconcile(&HealthSnapshot{Seq: 1, Step: 0, StepSeq: 1, SuccessRatio: 0.99, HealthyRatio: 0.50})
	logDecision(t, c, d, err, "coverage breach")
	if err != nil || d.Action != "paused" {
		t.Fatalf("coverage breach: action=%s err=%v", d.Action, err)
	}
	if s := c.Status(); s.Observations != 0 || s.BreachStreak != 1 {
		t.Fatalf("coverage breach: obs=%d streak=%d, want obs=0 streak=1", s.Observations, s.BreachStreak)
	}

	// 覆盖率达标但成功率不达标：同样不算合格数据。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 2, Step: 0, StepSeq: 2, SuccessRatio: 0.50, HealthyRatio: 0.99})
	logDecision(t, c, d, err, "success breach")
	if s := c.Status(); s.Observations != 0 || s.BreachStreak != 2 {
		t.Fatalf("success breach: obs=%d streak=%d, want obs=0 streak=2", s.Observations, s.BreachStreak)
	}

	// 两个维度都达标才计入观察窗；两份后自动放量。
	d, err = c.Reconcile(qualifiedSnap(3, 0, 3))
	logDecision(t, c, d, err, "qualified 1/2")
	if s := c.Status(); s.Observations != 1 || s.BreachStreak != 0 {
		t.Fatalf("qualified 1/2: obs=%d streak=%d", s.Observations, s.BreachStreak)
	}
	d, err = c.Reconcile(qualifiedSnap(4, 0, 4))
	logDecision(t, c, d, err, "qualified 2/2 -> auto")
	if d.Action != "auto-advanced" {
		t.Fatalf("qualified 2/2: action=%s, want auto-advanced", d.Action)
	}
}

func TestConcurrentDuplicateDelivery(t *testing.T) {
	desired := windowedDesired()
	desired.Steps[0].MinObservations = 100 // 窗口大于唯一数据份数，不会提前放量
	c := New()
	if err := c.Submit(desired); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 8 个协程并发投递同一批 60 份数据：每份唯一数据只能被计入一次。
	const workers, batch = 8, 60
	var accepted int64
	var mu sync.Mutex
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 1; i <= batch; i++ {
				d, err := c.Reconcile(qualifiedSnap(uint64(i), 0, uint64(i)))
				if err == nil {
					mu.Lock()
					accepted++
					mu.Unlock()
				} else if !errors.Is(err, ErrStaleSnapshot) && !errors.Is(err, ErrStaleStepSeq) {
					t.Errorf("unexpected error: %v (action=%s)", err, d.Action)
				}
			}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	s := c.Status()
	t.Logf("[dup-delivery] accepted=%d step=%d obs=%d lastStepSeq=%d",
		accepted, s.CurrentStep, s.Observations, s.LastStepSeq)
	if accepted != batch {
		t.Fatalf("accepted = %d, want exactly %d (no double counting)", accepted, batch)
	}
	if s.Observations != batch || s.CurrentStep != 0 {
		t.Fatalf("obs=%d step=%d, want obs=%d step=0", s.Observations, s.CurrentStep, batch)
	}
}

func TestLongRunNoCountDrift(t *testing.T) {
	desired := windowedDesired()
	desired.Steps[0].MinObservations = 20000 // 足够大的窗口，期间不触发放量
	c := New()
	if err := c.Submit(desired); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 长时间投递：10000 份唯一合格数据，每份夹带 2 次重复投递。
	// 控制器只保存标量计数（内存占用与历史总量无关），计数不得漂移。
	const unique = 10000
	seq := uint64(0)
	for i := uint64(1); i <= unique; i++ {
		seq++
		if _, err := c.Reconcile(qualifiedSnap(seq, 0, i)); err != nil {
			t.Fatalf("unique stepSeq=%d: %v", i, err)
		}
		for r := 0; r < 2; r++ {
			seq++
			_, err := c.Reconcile(qualifiedSnap(seq, 0, i))
			if !errors.Is(err, ErrStaleStepSeq) {
				t.Fatalf("duplicate stepSeq=%d error = %v, want ErrStaleStepSeq", i, err)
			}
		}
	}
	s := c.Status()
	t.Logf("[long-run] unique=%d obs=%d lastStepSeq=%d lastSnapshot=%d",
		unique, s.Observations, s.LastStepSeq, s.LastSnapshot)
	if s.Observations != unique {
		t.Fatalf("obs drifted: got %d, want %d", s.Observations, unique)
	}
	if s.LastStepSeq != unique {
		t.Fatalf("lastStepSeq = %d, want %d", s.LastStepSeq, unique)
	}
}
