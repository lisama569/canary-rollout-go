# canary-rollout-go

本地金丝雀发布控制器：以内存中的期望状态（`DesiredState`）与实例健康快照
（`HealthSnapshot`）模拟一次灰度发布，保证阶段推进、暂停与自动回滚在异常输入
（阈值抖动、快照乱序、并发调和、幂等重放）下仍得到一致且可解释的结论。

## 状态机

```
                 Submit(DesiredState)
                        │
                        ▼
   ┌────────── healthy snapshot ──────────┐
   │                                      ▼
Progressing ── unhealthy (< BreachLimit) ──▶ Paused ── healthy ──▶ Progressing
   │  │                                      │
   │  └── Advance(last step)                 └── 连续不达标达到 BreachLimit
   ▼       ▼                                          ▼
Completed                                   RolledBack（回到基线，终结）
```

- `Progressing`：可接受 `Advance` 推进到下一阶段。
- `Paused`：存在未消化的不达标快照，推进被拒绝（`ErrPaused`），健康快照可恢复。
- `Completed` / `RolledBack`：终结态，任何推进、调和或新期望状态均被拒绝
  （`ErrTerminal`），发布不得被重新激活。

## 阶段推进规则

- 阶段在 `DesiredState.Steps` 中声明，`TrafficPercent` 与 `InstancePercent`
  必须随阶段单调不减且在 `[0,100]` 内，否则提交被拒绝（`ErrNonMonotonic` /
  `ErrInvalidRatio`）。
- `Advance(step)` 只允许推进到相邻的下一阶段：
  - 跳阶 → `ErrSkipStep`
  - 回退到更早阶段 → `ErrStepRegression`
  - 越过已声明范围 → `ErrStepOutOfRange`
  - 推进到当前阶段 → 幂等空操作（重放安全）
- 推进到最后一个阶段即进入 `Completed`。

## 健康准入与回滚规则

- 快照 `SuccessRatio >= MinSuccessRatio` 视为健康：清零连续违约计数
  （`BreachStreak`），若处于 `Paused` 则恢复为 `Progressing`。
- 不健康快照使 `BreachStreak` 加一并暂停推进；**连续**不达标次数达到
  `BreachLimit` 时触发自动回滚：状态转为 `RolledBack`，阶段回到基线。
- 单次发布**至多回滚一次**：回滚后为终结态，后续坏快照不会引发第二次回滚。
- 阈值附近的抖动不会累积违约：任何一个健康快照即清零计数，交替抖动既不会
  触发回滚，也不会引发重复推进或状态回退。
- 快照按 `Seq` 严格递增消费：`Seq <= LastSnapshot` 的乱序/重复快照被丢弃
  （`ErrStaleSnapshot`），不影响任何计数。

## 幂等与并发语义

- `Controller` 全部方法由互斥锁保护，可多协程并发调用；每次调和在锁内完成
  状态判定与迁移，并发调和的最终状态与某次顺序执行一致。
- `Submit` 按 `Revision` 幂等：相同或更旧版本的重复提交为空操作；只有更新的
  版本才会开启一次新发布（且仅当当前发布未终结）。
- 已终结的发布（`Completed` / `RolledBack`）拒绝任何新期望状态与推进请求，
  不会被重新激活。
- 每次调和返回 `Decision{Action, Reason, Snapshot}`，记录动作与判定依据
  （阈值、违约计数、阶段比例等），便于解释与审计。

## 验证方法

```sh
go build ./...
go vet ./...
go test ./... -race -v
```

测试覆盖（日志中打印阶段、健康输入与判定依据）：

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
