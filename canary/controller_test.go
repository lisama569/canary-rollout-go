package canary

import (
	"errors"
	"testing"
)

// logDecision 在单测日志中打印阶段、健康输入与判定依据。
func logDecision(t *testing.T, c *Controller, d Decision, err error, label string) {
	t.Helper()
	s := c.Status()
	if d.Snapshot != nil {
		t.Logf("[%s] state=%s step=%d | health seq=%d successRatio=%.4f | action=%s reason=%q err=%v",
			label, s.State, s.CurrentStep, d.Snapshot.Seq, d.Snapshot.SuccessRatio,
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

// windowedDesired 返回带观察窗与实例覆盖率门槛的发布计划。
func windowedDesired() DesiredState {
	return DesiredState{
		Revision: 1,
		Steps: []Step{
			{TrafficPercent: 10, InstancePercent: 10, MinObservations: 2},
			{TrafficPercent: 30, InstancePercent: 30, MinObservations: 2},
			{TrafficPercent: 100, InstancePercent: 100},
		},
		Thresholds: Thresholds{MinSuccessRatio: 0.95, MinHealthyInstanceRatio: 0.80, BreachLimit: 3},
	}
}

// obs 构造一条带阶段归属与阶段内顺序的新版健康数据。
func obs(seq uint64, step int, stepSeq uint64, success, coverage float64) *HealthSnapshot {
	return &HealthSnapshot{
		Seq:                  seq,
		StepIndex:            step,
		StepSeq:              stepSeq,
		SuccessRatio:         success,
		HealthyInstanceRatio: coverage,
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
		d, err := c.Reconcile(&HealthSnapshot{Seq: uint64(i + 1), SuccessRatio: r})
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
		d, err := c.Reconcile(&HealthSnapshot{Seq: seq, SuccessRatio: 0.80})
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
	d, err := c.Reconcile(&HealthSnapshot{Seq: 4, SuccessRatio: 0.10})
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
	d, err := c.Reconcile(&HealthSnapshot{Seq: 1, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "seq=1 bad")
	if err != nil || c.Status().BreachStreak != 1 {
		t.Fatalf("seq=1: streak=%d err=%v", c.Status().BreachStreak, err)
	}
	for _, seq := range []uint64{1, 1, 0} {
		d, err = c.Reconcile(&HealthSnapshot{Seq: seq, SuccessRatio: 0.99})
		logDecision(t, c, d, err, "stale")
		if !errors.Is(err, ErrStaleSnapshot) {
			t.Fatalf("stale seq=%d error = %v, want ErrStaleSnapshot", seq, err)
		}
		if c.Status().BreachStreak != 1 {
			t.Fatalf("stale snapshot mutated streak to %d", c.Status().BreachStreak)
		}
	}
	// 更新的乱序坏快照继续累计，达到阈值后回滚恰好一次。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 2, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "seq=2 bad")
	d, err = c.Reconcile(&HealthSnapshot{Seq: 3, SuccessRatio: 0.80})
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
	d, err := c.Reconcile(&HealthSnapshot{Seq: 1, SuccessRatio: 0.80})
	logDecision(t, c, d, err, "pause")
	d, err = c.Advance(1)
	logDecision(t, c, d, err, "advance while paused")
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("advance while paused error = %v, want ErrPaused", err)
	}
	// 健康快照恢复推进后，推进被允许。
	d, err = c.Reconcile(&HealthSnapshot{Seq: 2, SuccessRatio: 0.99})
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
					d, err = c.Reconcile(&HealthSnapshot{Seq: 1, SuccessRatio: 0.99})
				} else {
					// 递增序号的坏快照，最终应触发恰好一次回滚。
					d, err = c.Reconcile(&HealthSnapshot{Seq: uint64(i + 2), SuccessRatio: 0.50})
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

func TestObservationWindowAutoPromote(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}
	if s := c.Status(); s.RequiredObservations != 2 {
		t.Fatalf("RequiredObservations = %d, want 2", s.RequiredObservations)
	}

	// 观察窗未攒够时提前推进：被挡下，且原因可与健康不达标区分。
	d, err := c.Advance(1)
	logDecision(t, c, d, err, "advance before window filled")
	if !errors.Is(err, ErrInsufficientObservations) {
		t.Fatalf("early advance error = %v, want ErrInsufficientObservations", err)
	}
	if errors.Is(err, ErrPaused) {
		t.Fatalf("early advance error %v must be distinguishable from ErrPaused", err)
	}

	// 第 1 份合格数据：计入但不放量，停在原地。
	d, err = c.Reconcile(obs(1, 0, 1, 0.99, 0.90))
	logDecision(t, c, d, err, "obs 1/2")
	if err != nil || d.Action != "healthy" {
		t.Fatalf("obs 1/2: action=%s err=%v", d.Action, err)
	}
	if s := c.Status(); s.Observations != 1 || s.CurrentStep != 0 {
		t.Fatalf("after obs 1/2: observations=%d step=%d, want 1/0", s.Observations, s.CurrentStep)
	}
	if _, err = c.Advance(1); !errors.Is(err, ErrInsufficientObservations) {
		t.Fatalf("advance at 1/2 error = %v, want ErrInsufficientObservations", err)
	}

	// 第 2 份合格数据：攒够观察窗，自动放量到下一阶段。
	d, err = c.Reconcile(obs(2, 0, 2, 0.99, 0.90))
	logDecision(t, c, d, err, "obs 2/2 -> promote")
	if err != nil || d.Action != "promoted" {
		t.Fatalf("obs 2/2: action=%s err=%v, want promoted", d.Action, err)
	}
	if s := c.Status(); s.CurrentStep != 1 || s.Observations != 0 || s.RequiredObservations != 2 {
		t.Fatalf("after promote: step=%d observations=%d required=%d, want 1/0/2",
			s.CurrentStep, s.Observations, s.RequiredObservations)
	}

	// 健康不达标与数据不够是两种可区分的挡路原因：
	// 先制造一次不达标使发布暂停，推进应报 ErrPaused 而非 ErrInsufficientObservations。
	d, err = c.Reconcile(obs(3, 1, 1, 0.50, 0.90))
	logDecision(t, c, d, err, "breach at step 1")
	if d.Action != "paused" {
		t.Fatalf("breach: action=%s, want paused", d.Action)
	}
	_, err = c.Advance(2)
	logDecision(t, c, d, err, "advance while paused")
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("advance while paused error = %v, want ErrPaused", err)
	}

	// 恢复后继续攒数据，攒够后自动进入最终阶段并完成发布。
	d, err = c.Reconcile(obs(4, 1, 2, 0.99, 0.90))
	logDecision(t, c, d, err, "resume 1/2")
	if d.Action != "resumed" {
		t.Fatalf("resume: action=%s, want resumed", d.Action)
	}
	d, err = c.Reconcile(obs(5, 1, 3, 0.99, 0.90))
	logDecision(t, c, d, err, "obs 2/2 -> complete")
	if err != nil || d.Action != "completed" {
		t.Fatalf("final obs: action=%s err=%v, want completed", d.Action, err)
	}
	if s := c.Status(); s.State != StateCompleted {
		t.Fatalf("final state = %s, want Completed", s.State)
	}
}

func TestNoProgressPlanRejected(t *testing.T) {
	base := windowedDesired()
	cases := []struct {
		name   string
		mutate func(d *DesiredState)
		want   error
	}{
		{"adjacent identical", func(d *DesiredState) {
			d.Steps[1] = d.Steps[0]
		}, ErrNoProgress},
		{"same traffic different instances ok", func(d *DesiredState) {
			d.Steps[1].TrafficPercent = d.Steps[0].TrafficPercent
		}, nil},
		{"negative min observations", func(d *DesiredState) {
			d.Steps[0].MinObservations = -1
		}, ErrInvalidObservations},
		{"coverage threshold out of range", func(d *DesiredState) {
			d.Thresholds.MinHealthyInstanceRatio = 1.5
		}, ErrInvalidThreshold},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := base
			d.Steps = append([]Step(nil), base.Steps...)
			tc.mutate(&d)
			err := New().Submit(d)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Submit() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCrossStageAndOutOfOrderDropped(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 来自其它阶段的数据：即使严重不达标也被丢弃，
	// 不计入观察、不累计违约、不触发回滚。
	for i := 0; i < 3; i++ {
		d, err := c.Reconcile(obs(uint64(i+1), 1, uint64(i+1), 0.10, 0.10))
		logDecision(t, c, d, err, "cross-stage bad data")
		if !errors.Is(err, ErrStageMismatch) {
			t.Fatalf("cross-stage error = %v, want ErrStageMismatch", err)
		}
	}
	if s := c.Status(); s.Observations != 0 || s.BreachStreak != 0 || s.State != StateProgressing {
		t.Fatalf("cross-stage data mutated state: obs=%d streak=%d state=%s",
			s.Observations, s.BreachStreak, s.State)
	}

	// 阶段内第 1 份数据计入。
	d, err := c.Reconcile(obs(10, 0, 1, 0.99, 0.90))
	logDecision(t, c, d, err, "step0 seq1")
	if err != nil || c.Status().Observations != 1 {
		t.Fatalf("step0 seq1: obs=%d err=%v", c.Status().Observations, err)
	}
	// 重复投递与阶段内乱序（更早位置）都被丢弃，不重复计数。
	for i, stepSeq := range []uint64{1, 1, 1} {
		d, err = c.Reconcile(obs(uint64(11+i), 0, stepSeq, 0.99, 0.90))
		logDecision(t, c, d, err, "duplicate")
		if !errors.Is(err, ErrStepSeqStale) {
			t.Fatalf("duplicate stepSeq=%d error = %v, want ErrStepSeqStale", stepSeq, err)
		}
	}
	if c.Status().Observations != 1 {
		t.Fatalf("duplicates counted: obs=%d, want 1", c.Status().Observations)
	}

	// 跳号先到 stepSeq=3（计入，攒够并自动放量），随后迟到的 stepSeq=2 被丢弃。
	d, err = c.Reconcile(obs(20, 0, 3, 0.99, 0.90))
	logDecision(t, c, d, err, "step0 seq3 -> promote")
	if d.Action != "promoted" {
		t.Fatalf("step0 seq3: action=%s, want promoted", d.Action)
	}
	d, err = c.Reconcile(obs(21, 0, 2, 0.99, 0.90))
	logDecision(t, c, d, err, "late step0 seq2")
	if !errors.Is(err, ErrStageMismatch) {
		t.Fatalf("late step0 data error = %v, want ErrStageMismatch (stage already left)", err)
	}
	// 已经走过的阶段不得倒退。
	if s := c.Status(); s.CurrentStep != 1 || s.Observations != 0 {
		t.Fatalf("stage regressed: step=%d obs=%d, want 1/0", s.CurrentStep, s.Observations)
	}
}

func TestInstanceCoverageGate(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 成功率达标但实例健康覆盖不达标：不算合格数据，且计入违约并暂停。
	d, err := c.Reconcile(obs(1, 0, 1, 0.99, 0.50))
	logDecision(t, c, d, err, "low coverage")
	if d.Action != "paused" {
		t.Fatalf("low coverage: action=%s, want paused", d.Action)
	}
	if s := c.Status(); s.Observations != 0 || s.BreachStreak != 1 {
		t.Fatalf("low coverage counted: obs=%d streak=%d, want 0/1", s.Observations, s.BreachStreak)
	}
	if _, err = c.Advance(1); !errors.Is(err, ErrPaused) {
		t.Fatalf("advance after low coverage error = %v, want ErrPaused", err)
	}

	// 覆盖率达标但成功率不达标：同样不算合格数据。
	d, err = c.Reconcile(obs(2, 0, 2, 0.90, 0.99))
	logDecision(t, c, d, err, "low success ratio")
	if d.Action != "paused" || c.Status().Observations != 0 {
		t.Fatalf("low success: action=%s obs=%d, want paused/0", d.Action, c.Status().Observations)
	}

	// 两个维度都达标才算一份合格数据；攒够后自动放量。
	d, err = c.Reconcile(obs(3, 0, 3, 0.99, 0.90))
	logDecision(t, c, d, err, "qualified 1/2")
	if d.Action != "resumed" || c.Status().Observations != 1 {
		t.Fatalf("qualified 1/2: action=%s obs=%d", d.Action, c.Status().Observations)
	}
	d, err = c.Reconcile(obs(4, 0, 4, 0.99, 0.90))
	logDecision(t, c, d, err, "qualified 2/2 -> promote")
	if d.Action != "promoted" || c.Status().CurrentStep != 1 {
		t.Fatalf("qualified 2/2: action=%s step=%d, want promoted/1", d.Action, c.Status().CurrentStep)
	}
}

func TestConcurrentDuplicateDelivery(t *testing.T) {
	c := New()
	if err := c.Submit(windowedDesired()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 同一批数据（阶段 0 的两份合格观察）被 8 个协程并发重复投递：
	// 全局序号与阶段内序号都相同，只有第一份投递生效，不得重复计数。
	deliver := func(batch []HealthSnapshot) {
		for i := range batch {
			snap := batch[i]
			_, err := c.Reconcile(&snap)
			if err != nil && !errors.Is(err, ErrStaleSnapshot) &&
				!errors.Is(err, ErrStepSeqStale) && !errors.Is(err, ErrStageMismatch) &&
				!errors.Is(err, ErrTerminal) {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}
	batch0 := []HealthSnapshot{*obs(1, 0, 1, 0.99, 0.90), *obs(2, 0, 2, 0.99, 0.90)}
	const workers = 8
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func() { defer func() { done <- struct{}{} }(); deliver(batch0) }()
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	// 恰好计 2 份并触发恰好一次自动放量；若重复计数，阶段 1 会混入多余观察。
	s := c.Status()
	t.Logf("[dup-delivery] step=%d observations=%d state=%s", s.CurrentStep, s.Observations, s.State)
	if s.CurrentStep != 1 || s.Observations != 0 {
		t.Fatalf("duplicate delivery double-counted: step=%d obs=%d, want 1/0", s.CurrentStep, s.Observations)
	}

	// 阶段 1 重复并发投递同一批，攒够后进入最终阶段并完成。
	batch1 := []HealthSnapshot{*obs(3, 1, 1, 0.99, 0.90), *obs(4, 1, 2, 0.99, 0.90)}
	for w := 0; w < workers; w++ {
		go func() { defer func() { done <- struct{}{} }(); deliver(batch1) }()
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	if s = c.Status(); s.State != StateCompleted {
		t.Fatalf("final state = %s, want Completed", s.State)
	}
}

func TestLongRunningCountStable(t *testing.T) {
	d := DesiredState{
		Revision: 1,
		Steps: []Step{
			{TrafficPercent: 5, InstancePercent: 5, MinObservations: 50},
			{TrafficPercent: 20, InstancePercent: 20, MinObservations: 30},
			{TrafficPercent: 60, InstancePercent: 60, MinObservations: 20},
			{TrafficPercent: 100, InstancePercent: 100},
		},
		Thresholds: Thresholds{MinSuccessRatio: 0.95, MinHealthyInstanceRatio: 0.80, BreachLimit: 3},
	}
	c := New()
	if err := c.Submit(d); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := c.Advance(0); err != nil {
		t.Fatalf("Advance(0): %v", err)
	}

	// 长时间持续投递：每个阶段混入大量重复投递与跨阶段噪音，
	// 计数必须精确等于唯一按序到达的合格数据份数，不随投递总量漂移。
	seq := uint64(0)
	required := []int{50, 30, 20}
	for step := 0; step < 3; step++ {
		for i := 1; i <= required[step]; i++ {
			seq++
			if _, err := c.Reconcile(obs(seq, step, uint64(i), 0.99, 0.90)); err != nil {
				t.Fatalf("step=%d stepSeq=%d: %v", step, i, err)
			}
			// 每份合格数据之后跟随重复投递与跨阶段噪音，均应被丢弃。
			// 最后一份合格数据会触发自动放量，此后同阶段噪音不再是噪音，故跳过。
			if i < required[step] {
				for _, noise := range []*HealthSnapshot{
					obs(seq+1, step, uint64(i), 0.99, 0.90),       // 重复投递
					obs(seq+2, step, uint64((i+1)/2), 0.99, 0.90), // 阶段内乱序（更早位置）
					obs(seq+3, step+1, 1, 0.10, 0.10),             // 跨阶段（且不达标）
				} {
					_, err := c.Reconcile(noise)
					if err == nil {
						t.Fatalf("noise at step=%d accepted: %+v", step, noise)
					}
				}
				seq += 3
			}
		}
		s := c.Status()
		if step < 2 {
			// 攒够即自动放量，进入下一阶段时计数归零、不携带历史。
			if s.CurrentStep != step+1 || s.Observations != 0 {
				t.Fatalf("after step %d: currentStep=%d obs=%d, want %d/0",
					step, s.CurrentStep, s.Observations, step+1)
			}
		}
	}
	s := c.Status()
	t.Logf("[long-running] state=%s step=%d streak=%d lastSnapshot=%d",
		s.State, s.CurrentStep, s.BreachStreak, s.LastSnapshot)
	if s.State != StateCompleted {
		t.Fatalf("final state = %s, want Completed", s.State)
	}
	if s.BreachStreak != 0 {
		t.Fatalf("noise caused breach drift: streak=%d, want 0", s.BreachStreak)
	}
}
