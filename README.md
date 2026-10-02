# canary-rollout-go

本地金丝雀发布控制器：以内存中的期望状态（`DesiredState`）与实例健康快照
（`HealthSnapshot`）模拟一次灰度发布，保证阶段推进、观察窗自动放量、暂停与
自动回滚在异常输入（阈值抖动、快照乱序、跨阶段数据、并发调和、幂等重放）下
仍得到一致且可解释的结论。

## 状态机

```
                 Submit(DesiredState)
                        │
                        ▼
   ┌────────── healthy snapshot ──────────┐
   │                                      ▼
Progressing ── unhealthy (< BreachLimit) ──▶ Paused ── healthy ──▶ Progressing
   │  │                                      │
   │  ├── Advance(last step)                 └── 连续不达标达到 BreachLimit
   │  └── 观察窗攒够 → 自动放量                     ▼
   ▼       ▼                              RolledBack（回到基线，终结）
Completed
```

- `Progressing`：可接受 `Advance` 推进；当前阶段观察窗攒够合格数据时自动放量。
- `Paused`：存在未消化的不达标快照，推进被拒绝（`ErrPaused`），健康快照可恢复。
- `Completed` / `RolledBack`：终结态，任何推进、调和或新期望状态均被拒绝
  （`ErrTerminal`），发布不得被重新激活。

## 阶段推进与观察窗自动放量

- 阶段在 `DesiredState.Steps` 中声明，`TrafficPercent` 与 `InstancePercent`
  必须随阶段单调不减且在 `[0,100]` 内（`ErrNonMonotonic` / `ErrInvalidRatio`）；
  **相邻两个阶段的流量与实例比例必须真的发生变化**，完全一致的计划在提交时
  被拒绝（`ErrNoProgress`）。
- 每个阶段可通过 `MinObservations` 声明观察窗：进入该阶段后需要攒够 N 份
  合格健康数据才能继续往下走。`MinObservations = 0` 表示无观察窗（兼容旧计划）。
- 观察窗攒够的瞬间控制器**自动放量**到下一阶段（`Decision.Action = "promoted"`）；
  自动进入最后一个阶段即发布完成（`"completed"`）。
- 没攒够之前发布停在原地：此时调用 `Advance` 会被挡下并返回
  `ErrInsufficientObservations`（携带已攒/所需份数），与健康不达标导致的
  `ErrPaused` 明确区分。
- `Advance(step)` 只允许推进到相邻的下一阶段：
  - 跳阶 → `ErrSkipStep`；回退 → `ErrStepRegression`；越界 → `ErrStepOutOfRange`
  - 推进到当前阶段 → 幂等空操作（重放安全）

## 健康数据：阶段归属、顺序与双维度判定

- 一份数据**合格**需同时满足两个维度：请求成功率
  `SuccessRatio >= MinSuccessRatio` 且实例健康覆盖
  `HealthyInstanceRatio >= MinHealthyInstanceRatio`（零值表示不考核该维度）。
- 新版数据携带 `StepIndex`（阶段归属）与 `StepSeq`（阶段内顺序号，从 1 开始）：
  - 只有归属**当前阶段**的数据才计入；来自其它阶段（含尚未进入任何阶段、
    或已经走过的阶段）的数据被丢弃（`ErrStageMismatch`），不计入观察、
    不累计违约、不触发回滚、不让阶段倒退。
  - 阶段内按 `StepSeq` 单调水位线消费：重复投递或更早位置的数据被丢弃
    （`ErrStepSeqStale`），不重复计数。
- 全局 `Seq` 水位线仍然生效：`Seq <= LastSnapshot` 的快照被丢弃
  （`ErrStaleSnapshot`）。
- **兼容范围**：`StepSeq == 0` 视为旧版无归属数据，按原有语义只参与健康判定
  （按全局 `Seq` 排序），不计入观察窗、不触发自动放量；旧版数据不携带实例
  覆盖率，因此与声明了 `MinHealthyInstanceRatio > 0` 的计划混用时会被判为
  不达标，新计划应配套使用新版数据。

## 健康准入与回滚规则

- 健康快照清零连续违约计数（`BreachStreak`），若处于 `Paused` 则恢复为
  `Progressing`；不健康快照使 `BreachStreak` 加一并暂停推进。
- **连续**不达标次数达到 `BreachLimit` 时触发自动回滚：状态转为
  `RolledBack`，阶段回到基线。单次发布**至多回滚一次**，回滚后为终结态。
- 阈值附近的抖动不会累积违约：任何一个健康快照即清零计数。
- 被丢弃的数据（乱序、重复、跨阶段）不参与任何判定，不会触发回滚。

## 幂等、并发与内存保证

- `Controller` 全部方法由互斥锁保护；每次调和在锁内完成判定与迁移，
  并发调和的最终状态与某次顺序执行一致。
- 去重依赖**单调水位线**（全局 `Seq` + 阶段内 `StepSeq`）而非历史集合：
  并发重复投递同一批数据时，只有首个到达者被消费，其余被水位线挡下，
  不会重复计数。
- 内存占用只随阶段数有界增长（每阶段两个计数器：合格观察份数与消费水位线），
  **不随历史数据总量增长**，长期运行计数不漂移。
- `Submit` 按 `Revision` 幂等；已终结的发布拒绝任何新期望状态与推进请求，
  不会被重新激活。
- 每次调和返回 `Decision{Action, Reason, Snapshot}`，`Reason` 中包含阶段、
  参与判定的数据（成功率/覆盖率/阈值）与判定依据（观察窗进度、违约计数等），
  便于解释与审计。

## 验证方法

```sh
go build ./...
go vet ./...
go test ./... -race -v
```

测试覆盖（日志中打印阶段、参与判定的数据与判定依据）：

| 测试 | 场景 |
| --- | --- |
| `TestValidateSteps` | 阶段比例单调性与取值范围校验 |
| `TestAdvanceRules` | 跳阶/越界/回退拒绝、幂等重放、终结态拒绝 |
| `TestThresholdJitter` | 阈值附近抖动不累积违约、不回滚、不回退 |
| `TestAutoRollbackAtMostOnce` | 连续不达标触发恰好一次回滚，终结态不可复活 |
| `TestOutOfOrderSnapshots` | 乱序/重复快照被丢弃，不计入违约 |
| `TestPausedAdvanceRejected` | 暂停期推进被拒绝，恢复后允许 |
| `TestConcurrentReconcile` | 16 协程并发调和（重放+递增快照），回滚至多一次 |
| `TestIdempotentReplay` | 重复/过期期望状态提交与并发推进的幂等性 |
| `TestObservationWindowAutoPromote` | 观察窗攒够自动放量；未攒够时 `ErrInsufficientObservations` 与 `ErrPaused` 可区分 |
| `TestNoProgressPlanRejected` | 相邻阶段无变化的计划被拒绝；观察窗/覆盖率阈值校验 |
| `TestCrossStageAndOutOfOrderDropped` | 跨阶段、阶段内乱序/重复数据被丢弃，不计数、不回滚、不倒退 |
| `TestInstanceCoverageGate` | 实例健康覆盖不达标不算合格数据、不放行 |
| `TestConcurrentDuplicateDelivery` | 8 协程并发重复投递同一批数据，不重复计数 |
| `TestLongRunningCountStable` | 长时间投递（混入重复/乱序/跨阶段噪音）后计数精确不漂移 |

