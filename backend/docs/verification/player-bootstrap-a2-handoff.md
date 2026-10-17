# PLAYER-BOOTSTRAP-AUTHORITY-A2 · GET_PLAYER_BOOTSTRAP 窄接线交接

日期：2026-08-06
状态：`COMPLETED / UNIT_PASS(ingress + actor authority 全量,含 race)`

```text
task_id:      PLAYER-BOOTSTRAP-AUTHORITY-A2
role:         PlayerIngress/Task 窄接线 Writer
branch:       codex/player-bootstrap-authority-a2-20260806
worktree:     /data/home/developer_026/CodeBuddy/20260519141303/farm-game-player-bootstrap-a2-20260806
base_commit:  935b51e (codex/integration-20260803 线上头部)
commits:
  7802273 feat(ingress): wire GET_PLAYER_BOOTSTRAP through TaskDomain (A2)
  95b35f2  chore(ingress): wire SetBootstrapTaskDomain in player-svr composition
  (后附 5 点审阅核实提交,以 git log 为准)
```

## 1. 目标与边界

使最终 `GET_PLAYER_BOOTSTRAP` 满足 28 §3.7 / 34 §Q34-02 合同：

- 同一 PlayerActor Turn 内完成幂等 `EnsureDailyPeriod` 与本周期登录一次事实；
- `player_state_revision` 取 `SubmitResult.CommittedRecordVersion`（成功提交后的记录
  版本），不再取 `EconomyVersion`；
- 返回完整七字段 Result（profile / progression / wallet / inventory /
  feature_summary / trusted_server_now_ms / player_state_revision）；
- 保持 `NO_DURABLE_OUTCOME`（不写 receipt、不建 Outcome）与安全重查（同日重复 /
  响应丢失重查不重复推进任务、不推进 revision）。

未扩张 PlayerIngress 写集：仅 `internal/player/ingress/service.go` 与对应测试；
`internal/player/task/**` 零改动（领域物理模型已由 DT0/DF0 交付完整）。未改协议
（`api/proto/**`、`api/gen/**` 零改动）；未改 FarmActor、Redis Lua/KeyBuilder、
RecoveryAdmission、GET_OPERATION_RESULT。

## 2. 改动文件（owned write set 内）

| 文件 | 改动 |
|---|---|
| `internal/player/ingress/service.go` | 新增 `taskDomain`/`nowMs` 注入缝隙与 `SetBootstrapTaskDomain`；`executePlayerBootstrap` 改为 `SubmitVersioned` 事务 Turn；`buildPlayerBootstrapResult` 去掉 revision、nowMs 一次捕获 |
| `internal/player/ingress/service_test.go` | 更新 2 个既有测试 + 新增 8 个测试 |
| `internal/player/actor/registry_authority_ingress_test.go` | 新增 Authority/legacy 双路径版本一致性测试（对应测试，登记于 §7） |
| `cmd/player-svr/main.go` | player-svr composition root 装配 `SetBootstrapTaskDomain`（commit `95b35f2`，见 §8） |
| `docs/verification/player-bootstrap-a2-handoff.md` | 本文件 |

生产代码改动限定 `internal/player/ingress/service.go` + `cmd/player-svr/main.go`
装配一行；`cmd/player-svr/main.go` 原归 Player Integrator（I0），因"只有 method 没有
装配"属于半完成状态，已在分支内补线并登记于 §8。

## 3. 实现要点

### 3.1 接线缝隙（28 §3.7 的 Service 注入）

```go
func (s *Service) SetBootstrapTaskDomain(taskRepository playertask.Repository, gameTimezone string, nowMs func() int64) error
```

- `taskRepository == nil` → 组合期 Fail Closed（返回 error）；
- `gameTimezone == ""` → 回落冻结的 `playertask.DefaultGameTimezone`（P0 时区）；
- `nowMs == nil` → 回落 `playeractor.NowMs` 真实时钟。

未组合时 `executePlayerBootstrap` 对业务返回
`REJECTED + ERR_COMMAND_UNIMPLEMENTED`（fail closed），绝不退回 `EconomyVersion`
旧视图，也不产生任何记录提交（测试 `TestGetPlayerBootstrapFailsClosedWithoutTaskDomain`）。

### 3.2 同一 Turn 登录事实（34 §Q34-02 / 28 §3.7）

```text
DomainStateFromRecord(rec)
  → EnsureDailyPeriod(state, nowMs)   // 惰性周期切换 + Growth 激活
  → RecordLogin(state, nowMs)         // 本周期登录一次事实
  → changed = ensureChanged || login.StateChanged   // 只反映真实 TaskDomain 变化
  → changed 时 ApplyDomainStateToRecord(rec, state)
```

- 首次进入：空 `DailyPeriodID` 触发惰性切换（生成当日三任务）→ 登录任务
  `IN_PROGRESS → CLAIMABLE`；
- 同日重复：`ensure` 对齐、登录已 `CLAIMABLE` → `RecordLogin` 返回 `NO_CHANGE`，
  `changed=false`，记录版本不动；
- 跨日：`DailyPeriodID` 变化触发惰性切换（丢弃前一日未领取任务、生成当日任务），
  登录再次 `IN_PROGRESS → CLAIMABLE`，revision 单调推进。

### 3.3 revision 语义

- 使用 `SubmitVersioned`，Turn 内不设置 revision；
- `SubmitVersioned` 成功后 `bootstrapResult.PlayerStateRevision =
  result.CommittedRecordVersion`（Commit 成功后再设置）；
- `changed=false` 时 `CommittedRecordVersion` 即读取版本（同日重查稳定）；
- `changed=true` 时即新提交版本（CAS 冲突由 Registry 有界重试收敛到同一成功提交，
  返回的是胜者提交版本）。

### 3.4 单次 nowMs 捕获

`nowMs := s.trustedNowMs()` 在请求开始时捕获一次，`EnsureDailyPeriod`、
`RecordLogin`、`TrustedServerNowMs` 共用同一值，避免跨调用时钟漂移破坏当日边界。

## 4. 约束满足对照

| 约束 | 实现 |
|---|---|
| Service 注入 TaskConfigRepository/TaskDomain、timezone、NowMs | `SetBootstrapTaskDomain(repo, tz, nowMs)`（§3.1） |
| 一次请求只捕获一次 nowMs | §3.4 |
| SubmitVersioned | `executePlayerBootstrap` 内 |
| DomainStateFromRecord → RecordLogin → ApplyDomainStateToRecord | §3.2（含 EnsureDailyPeriod 前置） |
| changed 只反映真实 TaskDomain 变化 | `changed || login.StateChanged` |
| Commit 成功后再设置 player_state_revision | §3.3 |
| task config 不可用 Fail Closed | `REJECTED + ERR_COMMAND_UNIMPLEMENTED`，零记录提交 |
| 不创建 Receipt/Outcome | Turn 内无 `EnsureReceiptCapacity`/`WriteReceipt`/Workflow 调用；测试断言 receipts 恒为空 |
| 返回完整七字段 Result | `buildPlayerBootstrapResult` 保持七字段非空（feature_summary 为 34 号预留空摘要） |

## 5. 测试清单（任务要求 → 测试名）

| 要求 | 测试 |
|---|---|
| 同日首次/重复 | `TestGetPlayerBootstrapSameDayFirstAndDuplicate`（首次 login→CLAIMABLE、revision 推进；重复 revision/TaskStateVersion 均不动、receipt 为空） |
| 跨日 | `TestGetPlayerBootstrapCrossDayAdvancesRevision`（day2 惰性切换、period/登录 PeriodID 刷新、revision 单调、同日重查稳定） |
| CAS conflict | `TestGetPlayerBootstrapCASConflictRetriesAndCommits`（failFirstCASStore 注入一次 CAS 冲突，有界重试收敛 revision=2 且状态正确） |
| Task 变化 revision 推进 | `TestGetPlayerBootstrapTaskChangeAdvancesRevisionWhileEconomyUnchanged`（TaskStateVersion 变化 → revision 推进；无变化不推进） |
| EconomyVersion 不变仍推进 | 同上 + `TestGetPlayerBootstrapReturnsAuthoritativeView`（revision=3 > economy=2） |
| 响应丢失重查 | `TestGetPlayerBootstrapResponseLossRequeryIsStable`（同一 operation_id 与全新 operation_id 重查均返回同一 revision） |
| 单次可信时间捕获 | `TestGetPlayerBootstrapCapturesTrustedNowOnce`（注入单调时钟，断言只调用一次、period 与 trusted_server_now_ms 同源，见 §9 点2） |
| 提交成功但调用方超时 | `TestGetPlayerBootstrapCommittedButTimeoutThenRequery`（apply-then-timeout：第一次 PROCESSING+ERR_TIMEOUT 不伪报成功；重查读已提交状态、不重复推进、返回新 CommittedRecordVersion，见 §9 点3） |
| Authority 与 legacy Registry 路径版本一致 | `TestAuthorityAndLegacyBootstrapPathVersionConsistent`（Ensure→首次→同日重查两路径 revision 完全一致且各自稳定） |
| task config 不可用 Fail Closed（补充） | `TestGetPlayerBootstrapFailsClosedWithoutTaskDomain` |
| 未初始化记录 fail closed（回归） | `TestGetPlayerBootstrapMissingRecordFailsClosed`（不隐式建账，ERR_STORE_UNAVAILABLE） |

## 6. 验证记录

```text
go build ./...                                   PASS
go vet ./internal/player/... ./cmd/player-svr/... PASS
go test ./internal/player/... -count=1           PASS(7 包)
go test ./internal/gateway/... ./internal/logic/... -count=1 PASS
go test -race ./internal/player/ingress/ -run TestGetPlayerBootstrap PASS

# 5 点审阅核实后复跑(2026-08-06):
go test ./internal/player/... -count=1           PASS(7 包,含新增 2 个测试)
go test ./internal/player/ingress/ -run TestGetPlayerBootstrap -count=1  PASS(10/10)
go vet ./internal/player/ingress/ ./internal/player/actor/ ./cmd/player-svr/ PASS
go build -buildvcs=false ./cmd/player-svr/       PASS
```

## 7. 边界/例外登记

- `internal/player/actor/registry_authority_ingress_test.go` 不在 owned write set
  的 `internal/player/ingress/**` 内，但属于本任务"对应测试"（Authority 路径版本
  一致性必须以既有 `ar1IngressProvider` 基建为基），在此登记为唯一例外；该文件仅
  新增 77 行测试代码，零生产代码改动。
- 生产代码改动严格限定 `internal/player/ingress/service.go` 一个文件。
- `internal/player/task/**` 零改动：领域物理模型已完整，无 BLOCKED 触发条件。
- 未改协议：`GET_PLAYER_BOOTSTRAP` 的 request/response 字段形状保持不变。

## 8. 装配接线（已在本分支完成）

`cmd/player-svr/main.go` composition root 已在创建 Service 后接线：

```go
if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(),
    playertask.DefaultGameTimezone, func() int64 { return time.Now().UnixMilli() }); err != nil {
    log.Fatalf("GET_PLAYER_BOOTSTRAP composition failed: %v", err)
}
```

提交 `7802273` 之后的跟进提交补充了该 main.go 装配调用（method 在
`internal/player/ingress/service.go` 已随 7802273 提交）。未接线时
`GET_PLAYER_BOOTSTRAP` 按 §3.1 fail closed（`ERR_COMMAND_UNIMPLEMENTED`），
不会出现半功能状态。

## 9. 5 点审阅核实（2026-08-06 复核）

### 点 1 · taskDomain == nil 的语义收口

结论：已在 composition 层收口，不构成长期请求级 UNIMPLEMENTED。

- 装配即 readiness：`cmd/player-svr/main.go` 以 `log.Fatalf` 组合
  `SetBootstrapTaskDomain(playertask.MustNewP0Repository(), DefaultGameTimezone, …)`。
  `MustNewP0Repository` 是内置配置（不可 nil），时区为冻结常量；组合失败即启动
  失败、不对外服务——对应"静态依赖没接 → readiness 失败"。
- 请求级 `ERR_COMMAND_UNIMPLEMENTED` 仅保留为**未接线时的防御兜底**（单测路径），
  装配正确的进程不会走到。
- TaskConfig 运行时短暂不可用（`AssignmentForPeriod` 未命中）走 `EnsureDailyPeriod`
  错误 → `applyError` → `UNAVAILABLE`，符合"临时不可用"语义；不返回
  UNIMPLEMENTED。
- GET_PLAYER_BOOTSTRAP 已在 Catalog 启用（既有状态，非本任务改动），故"命令尚未
  上线 → Catalog disabled"不适用。

### 点 2 · 一次请求只捕获一次可信时间

结论：成立，且已单独钉死。

- 实现：`executePlayerBootstrap` 入口 `nowMs := s.trustedNowMs()` 捕获一次，
  `EnsureDailyPeriod(nowMs)`、`RecordLogin(nowMs)`、`buildPlayerBootstrapResult(nowMs)`
  共用同一局部变量，Turn 内不再调用注入时钟。
- 测试：`TestGetPlayerBootstrapCapturesTrustedNowOnce` 注入单调递增时钟，断言注入
  函数每次请求只调用一次、`trusted_server_now_ms == 首次捕获值`、持久化
  `DailyPeriodID` 与 `trusted_server_now_ms` 同源。零点竞态（三处各取当前时间导致
  周期归属不一致）被该测试钉死。

### 点 3 · "提交成功但调用方收到超时"测试

结论：已补真实 apply-then-timeout 故障样板。

- `TestGetPlayerBootstrapCommittedButTimeoutThenRequery` 复用
  `commitThenTimeoutStore`（CAS 先真实写库再返回 `ErrTimeoutUnknown`）：
  - 第一次请求 → `PROCESSING + ERR_TIMEOUT`，不伪报成功；
  - 第二次安全重查 → 读到已提交的 DailyPeriod/Login（login CLAIMABLE）、不重复
    推进（revision=2 = Ensure v1 + 首次 Bootstrap v2）、receipt 恒空。
- 印证 28 §3.7 的 Unknown 恢复语义：Bootstrap 无 OperationReceipt，恢复方式是
  "安全重查当前权威状态"，而非查原操作结果。

### 点 4 · 七字段内容真实性（字段来源矩阵）

| 字段 | 来源 | 权威性 |
|---|---|---|
| `profile.player_id` | `rec.GetPlayerId()`（EnsureOwnerInitialized 时写入） | 权威 |
| `profile.display_name` | **PlayerRecord / Player 模型无 display_name 字段**（`api/proto` 视图有字段但模型无来源） | **占位（空字符串），非权威** |
| `progression.level/exp` | `rec.GetProgression()`（PlayerActor 权威状态） | 权威 |
| `wallet.coin` | `rec.GetWallet()` | 权威 |
| `inventory.items` | `rec.GetInventory()` | 权威 |
| `feature_summary` | 空 `PlayerFeatureSummaryView{}` | **占位**（34/35/37 号冻结具体摘要字段） |
| `trusted_server_now_ms` | 注入时钟一次捕获 | 权威 |

结论：**"资料显示"类问题未在本任务修复**——`display_name` 需要 Player 模型扩展 +
proto 扩展 + 资料（昵称）设置链路（PlayerProfile 子领域），协议/模型层均在本任务
forbidden 或超出范围。A2 交付的是"协议 + 版本链（revision）"完成，`display_name`/
`feature_summary` 内容真实性留待对应子领域任务。

### 点 5 · record_version 前提：Bootstrap 可见状态必须已持久化（长期不变量）

登记不变量（当前成立，SoftSave 开启前必须回归）：

> **Bootstrap 返回的所有权威字段，必须对应当前 `CommittedRecordVersion`。**
> 若未来开启 R1_ACTOR_SOFT / SoftSave / 脏状态延迟落库，则在开启任何 Player
> SoftSave 白名单前必须回归：Bootstrap 前做 Durability Barrier，或不允许未持久化
> Dirty 状态进入 Bootstrap 结果，否则可能出现"同 revision 不同内容"。

当前 `PlayerPersistenceLane` 的 soft 路径关闭，Player 写主链以可靠提交为主，
因此本任务实现（SubmitVersioned 成功提交后才返回结果）天然满足该不变量。
