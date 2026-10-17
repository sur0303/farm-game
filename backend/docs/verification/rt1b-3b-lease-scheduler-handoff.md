# RT1B-3B PER-SHARD-LEASE-SCHEDULER Handoff

- 分支：`codex/rt1b-3b-per-shard-lease-scheduler-20260806`
- worktree：`farm-game-rt1b-3b-per-shard-lease-scheduler-20260806`（独立目录）
- 约束：不通过定时调用 Reconciler.Observe 续租（Observe 只由 Assignment
  Snapshot 驱动）；不接跨进程 Watch/RouteManager/cmd/Actor Registry；不 push、
  不合并 integration（Git 候选与合并由 agentctl 控制器管理）。

## 1. Final Status

- RT1B-3B 全部 Finding 已关闭：F1（Reason 单调）✓、F2（注册原子性）✓、
  F3（Renew 超时）✓、F4（panic 包含 + 锁边界）✓、F5（线性化）✓。
- LostGeneration 隔离语义已冻结：正常 LostGeneration + 精确 Fence 成功 →
  **不**设置 Scheduler.Fatal、**不**调用 OnFatal、不拉低 ProcessReady。
- 等待 Reviewer 窄验证（只读）通过后升级：
  `RT1B-3B READY_FOR_ROUTE_PREMERGE-CODE`。

## 2. Final Commits

| Commit | 内容 |
|---|---|
| `bfe6b70` | RT1B-3B：per-shard lease scheduler（baseline） |
| `9c962b9` | FIX C1：lease recovery + reservation（F1/F2/F5） |
| `455c93b` | FIX C2：renew timeout + entry panic 边界（F3/F4） |
| `67cc71c` | stabilize supersede tests（收敛等待替代一次性断言） |
| `ee851b4` | MONOTONIC-FENCE C1：Fence Reason 单调（F1 终态） |
| `bc42c7c` | MONOTONIC-FENCE C2：fatal 包含 + 锁边界（F4） |
| `a194f3c` | LostGeneration 隔离 + FatalErr 保留 primary cause |
| `79b2a74` | supersede install 断言 race 确定性 |
| `0d8cdf7` | stale late-response 隔离：renewOnce 全部改为 generation-fenced Cancel |
| `c26d2fb` | G3 三代换代轻量验证（generation fence 不依赖两代假设） |
| （HEAD） | LostGeneration Fatal 诊断补全：FatalErr 携带完整 MembershipGeneration |

owned 写集：`internal/authoritycomposition/{lease_scheduler,membership}.go`、
`internal/logic/actor/authority/{lease_renew,dynamic_fence,generation_fence}.go`、
对应 `*_test.go`、本文档。未触碰 forbidden_write_set（cmd/api/proto/
Actor Registry/Remote Watch/RouteManager/Redis 布局/H5）。

## 3. Final Interfaces

### LeaseScheduler（Reconciler 接缝，最终接口）

```go
type LeaseScheduler interface {
    Reserve(key ShardKey, generation MembershipGeneration) (*LeaseRegistrationReservation, error)
    Activate(reservation *LeaseRegistrationReservation) error
    CancelReservation(reservation *LeaseRegistrationReservation)
    Cancel(key ShardKey, generation MembershipGeneration) error
}
```

- `commitPrepared` 顺序：`Reserve`（closed 即失败，不装 guard）→
  `CommitPreparedMembership`（装 guard）→ `Activate`（启动 Entry goroutine；
  失败则精确 Fence/Remove 回滚、不更新 installed、不推进 reconciled
  revision）。
- `Cancel(key, generation)` 只取消精确 generation 的 Entry（generation 门控），
  幂等。
- 旧 `Register` 方案已废除，见「8. Historical Fix Log」（DO_NOT_IMPLEMENT）。

### LeaseGuardRegistry 窄原语

```go
InstallIfMatches(domain, shard, expected, guard, requestStarted, result, now)
FenceIfMatches(domain, shard, expected)            // reason = MembershipRemoved
RemoveIfMatches(domain, shard, expected)           // Fence + 移除
RefreshOrRearmIfMatches(domain, shard, expected, prepared)
```

- `RefreshOrRearmIfMatches` 双重防线：reason 门控（仅 None /
  LocalDeadlineExpired 可 rearm）+ guard 层 revoked 拒绝同 generation 恢复。

### LeaseFenceReason

```go
LeaseFenceNone / LeaseFenceLocalDeadlineExpired   // recoverable
LeaseFenceMembershipRemoved / LeaseFenceLostGeneration / LeaseFenceRuntimeFatal // terminal
```

- 唯一写入口：`LeaseGuardHandle.promoteFenceReasonLocked`（终态 sticky，不可
  降级；LocalDeadlineExpired 只从 None 进入）。
- `isRecoverableFenceReason` / `isTerminalFenceReason` 辅助分类。

### PerShardLeaseSchedulerOptions（必需字段）

`Store`、`Registry`、`AuthorityLayoutVersion`、`HashVersion/HashSeed`、
`HolderID/HolderBootID`、`LeaseTTL`、`BaseDelay/MaxDelay/Jitter`、`Now`、
`RenewAttemptTimeout`、`RenewRecoveryTimeout`、可选 `OnFatal`。

## 4. Final State Machine

### Entry（每 installed Generation 一个 goroutine）

```
Active ⇄（本地 safety deadline 过期）LocalFencedRenewing（有界 Renew 可 rearm）
Active / LocalFencedRenewing →（Redis 明确失权/REMOVE/REPLACE/panic）
  → LostGeneration / Cancelled（terminal）
```

### FenceReason

| 当前 | 事件 | 结果 |
|---|---|---|
| None | 本地 deadline 过期 | → LocalDeadlineExpired（可恢复） |
| LocalDeadlineExpired | 同 generation 成功 Renew | → None（rearm） |
| None / LocalDeadlineExpired | MembershipRemoved / LostGeneration / RuntimeFatal | → 终态（sticky） |
| 任一终态 | 任何本地 deadline 检查 | 保持终态，不可降级 |
| 任一终态 | 同 generation Renew / RefreshOrRearm | 拒绝（reason 门控 + guard revoked 双防线） |

### Fatal 触发面（收紧）

仅以下情形设置 `Scheduler.Fatal` 并调用 `OnFatal`：
- Entry goroutine panic（`handleEntryPanic`）；
- LostGeneration 但本地精确 Fence 失败（guard 仍 admitted 且为同一
  generation）；
- 其他 Scheduler 内部不变量损坏 / 拓扑不兼容 / 生命周期异常。

**不**触发 Fatal 的正常路径：LocalDeadlineExpired、Transient、正常
LostGeneration（Fence 成功）、REMOVE/REPLACE 控制面事件。

## 5. Final Invariants

1. 每 installed Generation 一个 Entry；同 generation Activate 幂等。
2. ADD/Commit 成功后 Activate；Fence/Remove 先 Cancel（generation 门控）。
3. in-flight Renew 迟到响应校验 token/gen/installed/desired，丢弃不 re-admit。
4. SUCCESS 用 AcceptedTTL 刷新本地单调 deadline（InstallGrant）。
5. TRANSIENT deadline 前继续服务，指数退避 + 确定性 jitter。
6. LOST_GENERATION 精确 Fence 只影响目标 Shard，不触发 Fatal。
7. LOCAL_DEADLINE_EXPIRED 停止新 Actor Turn，可恢复。
8. 单 Shard 失败不影响其他 Shard / ProcessReady。
9. 零 Shard 零 Entry 零 goroutine，Close 幂等安全。
10. FenceReason 单调（终态 sticky）。
11. `s.mu` 内不调用 Registry/Handle/Store/回调（锁外执行，锁内只确认
    token/gen/active）。
12. OnFatal 在锁外调用，callback panic 被包含，FatalErr 保留 primary cause。
13. 无 admitted-without-entry / entry-without-installed（一致性断言）。

## 6. Final Tests

验收命令（全部真实 Exit 0）：

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
go test -buildvcs=false ./... -count=1                     PASS
go build -buildvcs=false ./cmd/...                         PASS
git diff --check                                            PASS
```

关键测试分组：
- 基础：零 Shard 关闭、单 Entry、重复安装幂等、REMOVE 取消、jitter 确定性。
- 迟到响应：remove 后不可 re-admit、G1 迟到不刷新 G2、取消 reservation
  不可 Activate、旧 Activate 不可重建 Entry。
- F1：本地 deadline 恢复、权威过期不可恢复、终态不可恢复、reason 单调
  （降级/并发/RefreshOrRearm 拒绝）。
- F2：closed Reserve 无 guard、Activate 失败回滚、revision 不推进。
- F3：Renew 超时、单 Shard 阻塞隔离、Close 不挂起。
- F4：entry panic Fence/OnFatal/无残留、OnFatal panic 包含、post-renew
  panic 不死锁、清理不持 scheduler 锁、FatalErr 保留 primary cause。
- LostGeneration 隔离：正常 LostGeneration 不 Fatal/不 OnFatal、Fence 失败
  触发 OnFatal、计划迁移不拉低 ProcessReady、单 Shard 隔离。
- 一致性：`assertMembershipRuntimeConsistent`（Reconciler installed ==
  Registry admitted guards == Scheduler active entries）接入 install/remove/
  replace/activate-failure/lost-generation/planned-migration 场景。
- Stale late response：`TestStaleLostGenerationResponseAfterReplaceDoesNotCancelNewEntry`
  ——G1 Renew 在飞时 G1→G2 Replace，G1 迟到返回 LostGeneration 只能终止
  G1（generation-fenced Cancel），不得取消 G2 Entry / Fence G2 Guard /
  修改 G2 token / 触发 Fatal/OnFatal；用 lostGenHook barrier 确定性等待
  「G1 已处理」再断言（无固定 sleep）。
- 三代换代：`TestStaleG1AndG2ResponsesCannotCancelG3` ——G1→G2→G3 连续
  Replace 后，G1 与 G2 的迟到 LostGeneration 响应均只终止各自 generation，
  G3 Entry/Guard/token/Fatal 状态完整；证明 generation fence 不依赖
  「两代」假设（旧 bug 下 5/5 FAIL，修复后 10/10 PASS）。

## 7. Remaining Work

- cmd composition root 接线（player-svr/farm-svr 注入 Scheduler +
  OnFatal→ProcessReady 联动）。
- Scheduler metrics（每 Shard 续租延迟/退避/失权计数）。
- 跨进程动态 assignment 发布/watch（RT1B 后续）。
- 真实 Redis 持久化/容灾/Lua 延迟/集群路由（Q2R-2，PROBE_PENDING；
  MemoryStore + race 不可替代）。

## 8. Historical Fix Log

### 8.1 RT1B-3B（bfe6b70）

首版 per-shard lease scheduler：每 installed MembershipGeneration 一个 Entry
goroutine，独立续租/退避/Fence；`LeaseScheduler` 接缝与
`MembershipReconcilerOptions.Scheduler`。

### 8.2 FIX-LEASE-RECOVERY-REGISTRATION（9c962b9 / 455c93b / 67cc71c）

- F1：`LeaseFenceReason` 四分类 + `PrepareRenew`（锁外远程证明）/
  `RefreshOrRearmIfMatches`（精确 generation + reason 门控）。
- F2：`Reserve → CommitPreparedMembership → Activate` 原子提交，无
  admitted-without-entry 窗口。
- F3：每 attempt 独立 `RenewAttemptTimeout` / `RenewRecoveryTimeout`。
- F4：Entry 外层 recover + 精确 Fence + OnFatal。
- 67cc71c：supersede 测试改收敛等待（`waitMembershipCondition`），非 sleep/
  放宽断言。

### 8.3 MONOTONIC-FENCE-FATAL-CONTAINMENT（ee851b4 / bc42c7c）

- F1 终态：`promoteFenceReasonLocked` 唯一写入口，终态 sticky，不可被本地
  deadline 降级。
- F4 硬化：`safeNotifyFatal` 包含 callback panic；`renewOnce` 锁边界重构
  （registry/handle/store 全锁外）。

### 8.4 LostGeneration 隔离（a194f3c / HEAD）

- `renewOnce` 的 LostGeneration 分支前置（revalidation 之前），用
  generation-aware `isAdmittedGeneration` 判定本地 Fence 是否生效；正常
  LostGeneration 不触发 Fatal，仅 Fence 失败触发 OnFatal。
- `recordFatalCallbackPanic` 保留 primary cause（%w 包装 + secondary 附加）。
- 一致性断言扩展至 admitted-guard 语义，支持终态场景。
- **stale late-response 隔离（0d8cdf7）**：`renewOnce` 内所有取消从按-key
  `cancelEntry` 改为 generation-fenced `s.Cancel(key, gen)`。G1 Renew 在飞时
  被 G1→G2 Replace 替换后，G1 迟到响应（LostGeneration / valid 失败 /
  RefreshOrRearm 失败等）只终止 G1 自己的 generation，绝不取消 G2 Entry 或
  Fence G2 Guard。测试钩子 `lostGenHook` 提供确定性 barrier（s.mu 保护）。
  同时移除既有测试中的固定 sleep。
- **G3 三代验证（c26d2fb）**：`TestStaleG1AndG2ResponsesCannotCancelG3` 证明
  generation fence 不依赖「两代」假设（旧 bug 5/5 FAIL，修复后 10/10 PASS）。
- **Fatal 诊断补全（HEAD）**：`lostGenerationFenceFatal(key, gen)` 帮助函数，
  LostGeneration 本地 Fence 失败产生的 Fatal 错误同时携带 ShardKey 与完整
  MembershipGeneration（`key=%v generation=%+v`）。`TestLostGenerationFenceFailureCallsOnFatal`
  断言 OnFatal 回调错误与 `Scheduler.FatalErr()` 均包含用本测试真实
  generation 构造的 `key=`/`generation=` 内容，且 OnFatal 恰好调用一次
  （callback 传递经 channel，计数经 atomic，无 data race）。

### Superseded Design（DO_NOT_IMPLEMENT）

旧 `LeaseScheduler.Register(key, generation) (*LeaseRegistration, error)` /
`Unregister(key, generation)` 方案已废弃：无法保证"Reserve 失败不装 guard /
Activate 失败精确回滚"的原子性，且同 generation 幂等语义被
`Reserve/Activate` + token 取代。**禁止在新代码中重新引入 Register**。
