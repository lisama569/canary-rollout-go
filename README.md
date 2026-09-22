# 本地金丝雀发布控制器（canary-rollout-go）

一个纯内存、无外部依赖的本地金丝雀发布控制器。它用**期望状态（`DesiredState`）**
与**实例健康快照（`HealthSnapshot`）**模拟一次灰度发布，重点保证：在**阈值抖动、
快照乱序、并发调和与重复重放**等异常输入下，结论依然**一致、可解释、不会重复动作**。

## 一、核心抽象

| 类型 | 含义 |
| --- | --- |
| `Stage` | 一个金丝雀阶段：`Weight`（流量百分比 1–100）与 `Replicas`（实例数）。相邻阶段二者必须**严格递增**，末阶段 `Weight=100`。 |
| `HealthThreshold` | 准入阈值：`SuccessRate >= MinSuccessRate` **且** `HealthyReplicas >= MinHealthyReplicas` 才算健康（边界含于健康侧）。 |
| `DesiredState` | 期望声明：阶段表、阈值，以及防抖参数 `PromoteAfter` / `RollbackAfter`。 |
| `HealthSnapshot` | 一次健康观测：携带单调 `Seq` 与所属 `StageName`。 |
| `ReleaseState` | 控制器为某次发布维护的实际状态（宏观阶段、当前阶段、已生效比例、`LastSeq`、是否已回滚、最近原因）。 |
| `Decision` | 一次调和的可解释结论：阶段、健康输入、阈值、健康判定、连续计数、动作、原因。 |

## 二、状态机

```
                 Submit(合法声明)
   (不存在) ───────────────────────► Progressing(阶段0，立即生效)
                                         ▲   │
                              Resume ◄────┘   │ Pause
                                         │    ▼
                                         │  Paused ──(快照被忽略，不推进/不回滚)
                                         │
            连续 PromoteAfter 份健康(非末阶段)   连续 RollbackAfter 份不健康
                 ┌──────────────┘                     └──────────────┐
                 ▼                                                     ▼
          Progressing(下一阶段)                                  RolledBack(终态)
                 │                                                     ▲
   末阶段连续 PromoteAfter 份健康 ──► Succeeded(终态) ───────────────────┘
```

宏观阶段 `Phase`：`Pending / Progressing / Paused / Succeeded / RolledBack`，
其中 **`Succeeded` 与 `RolledBack` 为终态**。

阶段推进规则（双重保证单调）：

1. **声明期校验**（`validateDesired`）：阶段非空、名称非空、比例越界检查、
   相邻阶段 `Weight` 与 `Replicas` 严格递增、末阶段必须 100%；
2. **运行期准入**（`Reconcile` / `PromoteTo`）：只能前进到**直接后继**，
   并对“目标比例严格大于当前”做防御式再校验。

任何后退、原地、跨阶（跳阶）、越过已声明比例的请求都被拒绝，并通过稳定原因码区分：

| 原因码 `RejectReason` | 触发情形 |
| --- | --- |
| `NotFound` | 发布不存在 |
| `InvalidStage` | 阶段声明非法（空、越界、非单调、末阶段非 100 等） |
| `InvalidJump` | 下标越界、后退/原地、或跨过中间阶段（非法跳阶） |
| `NotMonotonic` | 目标流量/实例数未严格大于当前 |
| `Paused` | 暂停态下推进或调和 |
| `StaleSnapshot` | 快照 `Seq<=LastSeq`（乱序/重放）或阶段与当前阶段不符 |
| `Unhealthy` | 健康未达阈值（作为自动回滚的原因） |
| `RollbackOnce` | 已回滚过又试图再次回滚 |
| `NoEvidence` | 证据不足，停留当前阶段（伴随 `Hold` 动作） |
| `DuplicateSubmit` | 相同 ReleaseID 重复提交且发布未终结 |
| `AlreadyTerminal` | 已终结发布被提交/调和/暂停/恢复 |

## 三、回滚规则与抖动防护

- **健康判定**：`rate >= MinSuccessRate && healthy >= MinHealthyReplicas`，
  边界一致地算作健康，消除“恰好等于阈值”的歧义。
- **连续计数防抖**：维护 `healthyStreak` / `unhealthyStreak`。
  - 连续 **`PromoteAfter`** 份健康才推进一个阶段；
  - 连续 **`RollbackAfter`** 份不健康才自动回滚；
  - 阈值附近**好坏交替**会不断把对方计数清零，因此抖动期间始终 `Hold`，
    **既不推进也不回滚**。
- **至多回滚一次**：回滚时置 `RollbackOnce=true` 并进入 `RolledBack` 终态；
  此后任何健康快照都被 `AlreadyTerminal` 拒绝，不可能二次回滚或被重新激活。
- **回滚优先于推进**：同一次调和中若两类条件都满足，优先按不健康处理。

## 四、乱序与重放语义

调和时对每份快照依次检查（任一不过即拒绝、状态不变）：

1. 发布存在且非终态；
2. 发布不处于 `Paused`（暂停期快照被**忽略**，且不累计计数，避免暂停“偷跑”）；
3. 快照 `StageName` 与当前生效阶段一致（空阶段名视为来源不明，同样拒绝），
   用于拦截跨阶段的旧观测；
4. 快照 `Seq` 在**同一阶段内**严格大于已采信的 `LastSeq`，否则判 `StaleSnapshot`
   （同阶段的迟到/重放）。`LastSeq` 按阶段记录，进入新阶段后从 0 重新计起。

通过后才采信并推进该阶段的 `LastSeq`。因此**乱序到达不可能导致状态回退或重复推进**，
同一批快照重放第二遍会被全部拒绝，终态与只跑一遍一致。

> 设计取舍：当一批不同 `Seq` 的快照以最坏顺序（严格递减）乱序到达时，系统会丢弃
> 旧证据并停留在当前阶段——**宁可不动，也不依据已过时的观测去推进/回滚**。
> 因此“连续 N 份证据”要求快照按序到达；而对“同一份快照被并发重复投递”，系统保证
> 恰好只采信一份（见并发测试），结论与单次执行一致。

## 五、幂等与并发语义

- **单把互斥锁**：`Controller` 的所有公开方法（提交、暂停、恢复、显式推进、调和、查询）
  都在同一把 `sync.Mutex` 下把“判定 + 状态写入”作为临界区串行完成。
- **并发调和 = 串行等价**：多个 goroutine 并发提交快照，等价于按锁获取顺序串行执行；
  乱序快照被 `Seq` 与阶段校验丢弃，最终状态与单次执行一致。
- **幂等提交**：相同 `ReleaseID` 重复提交不创建新发布——未终结返回 `DuplicateSubmit`、
  已终结返回 `AlreadyTerminal`，状态都不改变。
- **终态不可复活**：`Succeeded / RolledBack` 之后的提交、调和、暂停、恢复全部被拒绝。
- **状态副本**：`Get` / 各写方法返回 `ReleaseState` 值副本，调用方无法绕过锁改内部状态。

## 六、主要 API

```go
c := canary.NewController()

// 幂等提交
st, err := c.Submit(canary.DesiredState{
    ReleaseID: "r1",
    Stages: []canary.Stage{
        {Name: "10%", Weight: 10, Replicas: 1},
        {Name: "50%", Weight: 50, Replicas: 3},
        {Name: "100%", Weight: 100, Replicas: 6},
    },
    Threshold:     canary.HealthThreshold{MinSuccessRate: 0.95, MinHealthyReplicas: 1},
    PromoteAfter:  2, // 连续 2 份健康才推进（防抖）
    RollbackAfter: 3, // 连续 3 份不健康才回滚
})

// 健康证据驱动调和
st, dec, err := c.Reconcile(canary.HealthSnapshot{
    ReleaseID: "r1", StageName: "10%", Seq: 1,
    SuccessRate: 0.99, HealthyReplicas: 3,
})

c.Pause("r1"); c.Resume("r1")
c.PromoteTo("r1", 2) // 显式跳阶准入校验
```

按原因判别错误：

```go
var re *canary.RejectError
if errors.As(err, &re) {
    switch re.Reason {
    case canary.ReasonStaleSnapshot: // 乱序/重放
    case canary.ReasonInvalidJump:   // 非法跳阶
    case canary.ReasonAlreadyTerminal:
    // ...
    }
}
```

## 七、验证方法

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...      # 功能 + 竞态
go test -v ./...                  # 查看每条用例的阶段/健康输入/判定依据日志
```

测试覆盖（`*_test.go`）：

| 测试 | 场景 |
| --- | --- |
| `TestSubmitAndInvalidSpec` | 合法提交、幂等重复、非单调/末阶段非全量等非法声明、NotFound |
| `TestTerminalCannotReactivate` | 一路成功至终态后，重复提交与终态快照均被拒绝 |
| `TestPromoteToRejectsIllegalJumps` | 跨阶/原地/后退/越界 → `InvalidJump`，暂停 → `Paused` |
| `TestThresholdJitter` | 阈值附近好坏交替抖动：只推进一次、绝不回滚 |
| `TestBoundaryIsHealthy` | 恰好等于阈值一致判为健康（`>=` 边界） |
| `TestOutOfOrderAndDuplicateSnapshots` | 迟到、重放、旧阶段标签 → `StaleSnapshot`，状态不回退 |
| `TestAutoRollbackOnce` | 连续不健康恰好回滚一次，之后全部终态拒绝 |
| `TestPausedDoesNotAdvanceOrRollback` | 暂停期忽略证据、不累计计数，恢复后行为正确 |
| `TestConcurrentReconcileDeterministic` | 多协程并发重复投递同一份快照：只采信一份、只推进一次，终态唯一（`-race`） |
| `TestConcurrentOutOfOrderConverges` | 并发重复+乱序重放混合：恰好一次推进，旧证据重放全被拒 |
| `TestConcurrentSubmitIdempotent` | 50 协程重复提交：恰 1 次创建、其余幂等忽略 |
| `TestReplaySameStateMatchesSingleRun` | 同批快照重放两遍，终态与单轮一致 |

单测通过 `t.Logf` 在每次调和打印**阶段、健康输入、阈值与判定依据**
（`[观测]` / `[调和]` 行），用 `go test -v` 即可查看。

## 八、文件结构

```
types.go       核心类型、宏观阶段、稳定拒绝原因码
validate.go    声明校验 + 单快照健康判定（边界语义）
controller.go  控制器：提交/暂停/恢复/显式推进/查询 + 互斥与状态副本
reconcile.go   调和核心：乱序丢弃、防抖计数、单调推进、至多一次回滚、Decision
*_test.go      抖动 / 乱序 / 回滚 / 幂等 / 并发 等场景测试
```
