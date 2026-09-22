package canary

import (
	"fmt"
	"sync"
)

// Controller 是内存态金丝雀发布控制器。所有公开方法均在同一把互斥锁下
// 串行化“判定 + 状态写入”，因此多个协程并发调和与单次串行执行结论一致。
type Controller struct {
	mu sync.Mutex

	releases map[string]*ReleaseState
	specs    map[string]DesiredState
	books    map[string]*bookkeeping

	// 仅在测试中使用的观测钩子：每次调和产生一条判定记录。
	observe func(Decision)
}

// NewController 创建一个空控制器。
func NewController() *Controller {
	return &Controller{
		releases: map[string]*ReleaseState{},
		specs:    map[string]DesiredState{},
		books:    map[string]*bookkeeping{},
	}
}

// Observe 注册调和判定记录的观测回调（主要用于测试日志）。
func (c *Controller) Observe(h func(Decision)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observe = h
}

// cloneState 在持锁状态下返回发布状态的值副本，避免调用方持有内部指针后绕过锁修改。
func cloneState(s *ReleaseState) *ReleaseState {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

// Submit 幂等提交一次发布声明。
//   - 声明非法：返回 ReasonInvalidStage，不创建任何状态；
//   - ReleaseID 已存在且非终态：返回现有状态与 ReasonDuplicateSubmit，状态不变；
//   - ReleaseID 已存在且已终结：返回现有状态与 ReasonAlreadyTerminal，绝不重新激活；
//   - 首次提交：发布进入第 0 阶段并立即生效（Progressing）。
func (c *Controller) Submit(d DesiredState) (*ReleaseState, error) {
	if err := validateDesired(d); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.releases[d.ReleaseID]; ok {
		switch existing.Phase {
		case PhaseSucceeded, PhaseRolledBack:
			return cloneState(existing), &RejectError{
				Reason: ReasonAlreadyTerminal,
				Msg:    "发布 " + d.ReleaseID + " 已终结于 " + existing.Phase.String() + "，不得重新激活",
			}
		default:
			return cloneState(existing), &RejectError{
				Reason: ReasonDuplicateSubmit,
				Msg:    "发布 " + d.ReleaseID + " 已存在，重复提交被幂等忽略",
			}
		}
	}

	first := d.Stages[0]
	st := &ReleaseState{
		ReleaseID:       d.ReleaseID,
		Phase:           PhaseProgressing,
		StageIndex:      0,
		Current:         first,
		AppliedWeight:   first.Weight,
		AppliedReplicas: first.Replicas,
		Reason:          "首次提交，进入阶段 " + first.Name,
	}
	c.releases[d.ReleaseID] = st
	c.specs[d.ReleaseID] = d
	c.books[d.ReleaseID] = &bookkeeping{}
	return cloneState(st), nil
}

// Pause 暂停指定发布：仅 Progressing 可暂停；对已暂停者幂等；
// 对已终结发布返回 ReasonAlreadyTerminal；不存在返回 ReasonNotFound。
func (c *Controller) Pause(releaseID string) (*ReleaseState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.releases[releaseID]
	if !ok {
		return nil, &RejectError{Reason: ReasonNotFound, Msg: "发布 " + releaseID + " 不存在"}
	}
	switch st.Phase {
	case PhasePaused:
		return cloneState(st), nil
	case PhaseProgressing:
		st.Phase = PhasePaused
		st.Reason = "人工暂停于阶段 " + st.Current.Name
		return cloneState(st), nil
	default:
		return cloneState(st), &RejectError{
			Reason: ReasonAlreadyTerminal,
			Msg:    "发布 " + releaseID + " 已终结，不能暂停",
		}
	}
}

// Resume 恢复被暂停的发布：仅 Paused 可恢复；对进行中者幂等；
// 对已终结发布返回 ReasonAlreadyTerminal。
func (c *Controller) Resume(releaseID string) (*ReleaseState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.releases[releaseID]
	if !ok {
		return nil, &RejectError{Reason: ReasonNotFound, Msg: "发布 " + releaseID + " 不存在"}
	}
	switch st.Phase {
	case PhaseProgressing:
		return cloneState(st), nil
	case PhasePaused:
		st.Phase = PhaseProgressing
		st.Reason = "人工恢复于阶段 " + st.Current.Name
		return cloneState(st), nil
	default:
		return cloneState(st), &RejectError{
			Reason: ReasonAlreadyTerminal,
			Msg:    "发布 " + releaseID + " 已终结，不能恢复",
		}
	}
}

// Get 返回发布的一份状态副本；不存在时返回 ReasonNotFound。
func (c *Controller) Get(releaseID string) (*ReleaseState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.releases[releaseID]
	if !ok {
		return nil, &RejectError{Reason: ReasonNotFound, Msg: "发布 " + releaseID + " 不存在"}
	}
	return cloneState(st), nil
}

// PromoteTo 尝试把发布直接推进到声明中下标为 targetIndex 的阶段，
// 用于在上层（而非健康证据驱动）请求跳阶时进行准入校验。它只允许前进到
// “当前阶段的直接后继”，并拒绝任何后退、原地、跨阶或越过已声明比例的请求。
//
// 原因码：
//   - ReasonNotFound / ReasonAlreadyTerminal / ReasonPaused；
//   - ReasonInvalidJump：下标越界、后退/原地、或跨过中间阶段；
//   - ReasonNotMonotonic：目标比例未严格大于当前（声明应已保证，此处为防御）。
func (c *Controller) PromoteTo(releaseID string, targetIndex int) (*ReleaseState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.releases[releaseID]
	if !ok {
		return nil, &RejectError{Reason: ReasonNotFound, Msg: "发布 " + releaseID + " 不存在"}
	}
	if st.Phase == PhaseSucceeded || st.Phase == PhaseRolledBack {
		return cloneState(st), &RejectError{
			Reason: ReasonAlreadyTerminal,
			Msg:    "发布 " + releaseID + " 已终结于 " + st.Phase.String() + "，不能推进",
		}
	}
	if st.Phase == PhasePaused {
		return cloneState(st), &RejectError{
			Reason: ReasonPaused,
			Msg:    "发布 " + releaseID + " 已暂停，先 Resume 再推进",
		}
	}

	spec := c.specs[releaseID]
	if targetIndex < 0 || targetIndex >= len(spec.Stages) {
		return cloneState(st), &RejectError{
			Reason: ReasonInvalidJump,
			Msg:    fmt.Sprintf("发布 %s 目标阶段下标 %d 越界(共 %d 个阶段)", releaseID, targetIndex, len(spec.Stages)),
		}
	}
	if targetIndex <= st.StageIndex {
		return cloneState(st), &RejectError{
			Reason: ReasonInvalidJump,
			Msg:    "发布 " + releaseID + " 不能后退或原地推进：当前=" + fmt.Sprint(st.StageIndex) + " 目标=" + fmt.Sprint(targetIndex),
		}
	}
	if targetIndex != st.StageIndex+1 {
		return cloneState(st), &RejectError{
			Reason: ReasonInvalidJump,
			Msg: "发布 " + releaseID + " 非法跳阶：当前=" + fmt.Sprint(st.StageIndex) +
				" 目标=" + fmt.Sprint(targetIndex) + "，只允许直接后继=" + fmt.Sprint(st.StageIndex+1),
		}
	}

	target := spec.Stages[targetIndex]
	if target.Weight <= st.AppliedWeight || target.Replicas <= st.AppliedReplicas {
		return cloneState(st), &RejectError{
			Reason: ReasonNotMonotonic,
			Msg:    "目标阶段 " + target.Name + " 比例未严格大于当前",
		}
	}

	st.StageIndex = targetIndex
	st.Current = target
	st.AppliedWeight = target.Weight
	st.AppliedReplicas = target.Replicas
	if bk := c.books[releaseID]; bk != nil {
		bk.healthyStreak = 0
		bk.unhealthyStreak = 0
	}
	st.LastSeq = 0 // 进入新阶段，乱序基线按阶段重新计起
	st.Reason = "显式推进到阶段 " + target.Name
	return cloneState(st), nil
}
