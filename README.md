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
   │  ├── 观察窗攒满 → 自动进入下一阶段       └── 连续不达标达到 BreachLimit
   │  └── Advance(last step) / 末阶段观察窗攒满
   ▼       ▼                                          ▼
Completed                                   RolledBack（回到基线，终结）
```

- `Progressing`：可接受 `Advance` 推进，或在观察窗攒满时自动放量。
- `Paused`：存在未消化的不达标快照，推进被拒绝（`ErrPaused`），健康快照可恢复。
- `Completed` / `RolledBack`：终结态，任何推进、调和或新期望状态均被拒绝
  （`ErrTerminal`），发布不得被重新激活。

## 阶段推进规则

- 阶段在 `DesiredState.Steps` 中声明，`TrafficPercent` 与 `InstancePercent`
  必须随阶段单调不减且在 `[0,100]` 内，否则提交被拒绝（`ErrNonMonotonic` /
  `ErrInvalidRatio`）。
- **相邻两个阶段的流量与实例比例不能完全相同**：声明成一样的放量不构成合法
  计划，提交时即被拒绝（`ErrIdenticalSteps`）。`MinObservations` 必须非负
  （`ErrInvalidWindow`）。
- `Advance(step)` 只允许推进到相邻的下一阶段：
  - 跳阶 → `ErrSkipStep`
  - 回退到更早阶段 → `ErrStepRegression`
  - 越过已声明范围 → `ErrStepOutOfRange`
  - 推进到当前阶段 → 幂等空操作（重放安全）
- 推进到最后一个阶段即进入 `Completed`。

## 观察窗与自动放量

- 每个阶段可通过 `Step.MinObservations` 声明观察窗：进入该阶段后必须攒够
  指定份数的**合格数据**才允许继续放量；`0` 表示不启用观察窗（沿用人工推进）。
- 合格数据计入当前阶段后，若攒满观察窗则**自动放量**：进入下一阶段并重置
  阶段计数；末阶段攒满则发布自动完成（`Completed`）。
- 观察窗未攒满时，`Advance` 提前推进会被拒绝并返回
  `ErrInsufficientObservations`——与因健康不达标被拒的 `ErrPaused` 可明确
  区分，`Decision.Reason` 中会给出 `已攒/所需` 的具体进度。
- 观察窗进度通过 `Status.Observations` 暴露（自动放量进入新阶段后归零，
  这是新阶段的进度，不代表上一阶段没攒够）。
- **放量判定记录可审计**：触发自动放量 / 末阶段完成的那条 `Decision`
  同时以结构化字段与文字理由记录“当次攒够”的依据——`Decision.Step` 为
  刚刚填满窗口的阶段（不是进入的新阶段），`Decision.Observations` /
  `Decision.Required` 为当次份数（如 `2/2`，不是清零后的 `0/2`），
  `Reason` 中同样写明 `observation window 2/2 filled at step N`。
  回滚记录也会标明发生违约的阶段。

## 健康数据的阶段归属与顺序（含旧采集端兼容）

健康快照在全局序号 `Seq` 之外，扩展了**可选的**阶段归属 `Step *int` 与
阶段内序号 `StepSeq *uint64`（指针为 `nil` 即表示采集端未上报该字段）：

- **全局顺序（始终必填）**：`Seq <= LastSnapshot` 的乱序/重复快照一律丢弃
  （`ErrStaleSnapshot`）。这是最底层的去重防线，所有格式的数据都必须携带
  全局序号。
- **阶段归属（新格式）**：显式携带 `Step` 时，只有 `Step == 当前阶段` 的数据
  才计入判定。来自其它阶段（包括已经走过的阶段）的数据被丢弃
  （`ErrWrongStep`），不计入观察窗、不累计违约、不触发回滚，也不会让已经
  走过的阶段倒退。
- **阶段归属（旧格式，`Step == nil`）**：数据归入**当前正在调和的阶段**，
  在任意阶段（包括自动放量后进入的新阶段、末阶段）都能参与判定。旧数据不
  会因为"不属于阶段 0"而被挡在后续阶段之外。
- **阶段内顺序（新格式）**：显式携带 `StepSeq` 时，其值在当前阶段内必须
  **严格递增**；重复投递或来自阶段内更早位置的数据被丢弃
  （`ErrStaleStepSeq`），不影响任何计数。进入新阶段后序号重新从任意起点
  计数（每阶段第一条显式带序号的数据，即使 `*StepSeq == 0`，也总会被
  接受）。
- **阶段内顺序（旧格式，`StepSeq == nil`）**：不做阶段内顺序校验，每条全局
  序号递增的数据都视为该阶段的一份新数据——连续不达标会逐次累计并照旧触发
  自动回滚，合格数据会计入观察窗并在攒够后自动放量，发布不会被卡死。
  旧格式数据的重复投递仍由全局 `Seq` 去重拦截（同一份数据无论被多少协程
  投递多少次，只计一次），且不更新阶段内序号账本（`LastStepSeq`）。
- **新旧格式可混用**：混用期间，旧格式数据不推进阶段内序号，新格式数据的
  严格顺序校验不受影响（例：新格式消费过 `StepSeq=0` 后，旧格式数据不会把
  它"顶过去"，再来的 `StepSeq=0` 仍是重复，仍被丢弃）。

> 兼容范围提示：旧格式依赖全局 `Seq` 去重，要求采集端对同一份数据使用
> 相同的全局序号重投；若旧采集端对同一数据每次生成新 `Seq`，则无法与
> "每份数据只计一次"区分，这属于采集端契约问题，控制器不会猜测去重。

## 健康准入与回滚规则

- 一份数据**两个维度都达标**才算合格：请求成功率
  `SuccessRatio >= MinSuccessRatio` 且实例健康覆盖
  `HealthyRatio >= MinHealthyRatio`（健康实例占比）。
- 合格数据：清零连续违约计数（`BreachStreak`），若处于 `Paused` 则恢复为
  `Progressing`，并计入当前阶段观察窗。
- 任一维度不达标：`BreachStreak` 加一并暂停推进；**连续**不达标次数达到
  `BreachLimit` 时触发自动回滚：状态转为 `RolledBack`，阶段回到基线。
- 单次发布**至多回滚一次**：回滚后为终结态，后续坏快照不会引发第二次回滚。
- 阈值附近的抖动不会累积违约：任何一份合格数据即清零计数，交替抖动既不会
  触发回滚，也不会引发重复推进或状态回退。

## 幂等、并发与内存保证

- `Controller` 全部方法由互斥锁保护，可多协程并发调用；每次调和在锁内完成
  状态判定与迁移，并发调和的最终状态与某次顺序执行一致。
- **并发重复投递不重复计数**：新格式数据由 `(当前阶段, LastStepSeq)` 与
  全局 `LastSnapshot` 两层标量去重；旧格式数据（无阶段内序号）由全局
  `LastSnapshot` 单层去重。无论同一批数据被多少协程并发投递、重投多少次，
  每份唯一数据恰好计入一次，重复投递返回 `ErrStaleSnapshot` /
  `ErrStaleStepSeq` 且不改变任何计数。
- **内存占用有界**：控制器不保存任何历史数据或数据 ID 集合，只维护若干
  标量计数（全局序号、阶段内序号、观察窗份数、违约计数）与当前期望状态，
  内存占用与累计投递总量无关；长时间投递后计数不漂移（有 3 万次投递的
  对拍用例）。
- `Submit` 按 `Revision` 幂等：相同或更旧版本的重复提交为空操作；只有更新的
  版本才会开启一次新发布（且仅当当前发布未终结）。
- 已终结的发布（`Completed` / `RolledBack`）拒绝任何新期望状态与推进请求，
  不会被重新激活。
- 每次调和返回 `Decision{Action, Reason, Snapshot}`，记录动作、参与判定的
  数据与判定依据（两个维度的阈值比较、违约计数、观察窗进度、阶段比例等），
  便于解释与审计。

## 验证方法

```sh
go build ./...
go vet ./...
go test ./... -race -v          # 全量用例（含竞态检测），-v 输出逐条判定日志
go test ./... -race -count=10   # 反复运行，压并发用例的稳定性
```

`-v` 日志中：每个测试开头会打印一行它覆盖的业务场景；每次调和打印当前
状态/阶段/观察窗进度、参与判定的数据（`step=legacy` / `stepSeq=legacy`
表示旧格式未上报该字段）、动作与判定依据（`decidedAt=stepN window=x/y`
为结构化的判定阶段与份数，随后是文字理由 `reason=...`），便于核对覆盖了
哪些业务以及每条数据的归属与结论。

| 测试 | 场景 |
| --- | --- |
| `TestValidateSteps` | 阶段比例单调性/取值范围、相邻阶段不得相同、观察窗非负 |
| `TestAdvanceRules` | 跳阶/越界/回退拒绝、幂等重放、终结态拒绝 |
| `TestAutoAdvanceOnObservationWindow` | 观察窗攒满自动放量、末阶段攒满自动完成 |
| `TestAdvanceBlockedReasons` | 数据不够（`ErrInsufficientObservations`）与健康不达标（`ErrPaused`）可区分 |
| `TestCrossStepAndOutOfOrderDataDropped` | 跨阶段/阶段内乱序/重复数据被丢弃，不回滚不倒退 |
| `TestUnhealthyCoverageBlocksWindow` | 实例健康覆盖不达标不计入观察窗、累计违约 |
| `TestConcurrentDuplicateDelivery` | 8 协程并发投递同一批数据，每份只计一次 |
| `TestLongRunNoCountDrift` | 3 万份投递（含重复）后计数精确不漂移 |
| `TestThresholdJitter` | 阈值附近抖动不累积违约、不回滚、不回退 |
| `TestAutoRollbackAtMostOnce` | 连续不达标触发恰好一次回滚，终结态不可复活 |
| `TestOutOfOrderSnapshots` | 乱序/重复快照被丢弃，不计入违约 |
| `TestPausedAdvanceRejected` | 暂停期推进被拒绝，恢复后允许 |
| `TestConcurrentReconcile` | 16 协程并发调和（重放+递增快照），回滚至多一次 |
| `TestIdempotentReplay` | 重复/过期期望状态提交与并发推进的幂等性 |
| `TestLegacySnapshotsStillTriggerRollback` | 旧格式数据（无 Step/StepSeq）连续不达标照旧累计并触发自动回滚，重复投递不重复计违约 |
| `TestLegacySnapshotsFillWindowAndAutoAdvance` | 旧格式合格数据攒满每个阶段的观察窗并自动放量、末阶段完成；中途坏数据不计入 |
| `TestAutoAdvanceDecisionRecordsFilledCount` | 自动放量/末阶段完成的判定记录如实携带当次阶段与份数（2/2 而非 0/2） |
| `TestConcurrentDuplicateLegacyDelivery` | 8 协程并发投递同一批旧格式数据（仅全局 Seq 去重），每份只计一次 |
| `TestLegacyCompatDoesNotWeakenOrdering` | 新旧格式混用：显式 StepSeq（含 0）重复仍丢弃、跨阶段仍丢弃、阶段不倒退 |

