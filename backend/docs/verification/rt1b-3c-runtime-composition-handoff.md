# RT1B-3C-A MEMBERSHIP RUNTIME COMPOSITION Handoff

- 分支：`codex/rt1b-3c-a-membership-runtime-composition-20260807`
- worktree：`farm-game-rt1b-3c-a-membership-runtime-composition-20260807`（独立目录）
- base：`cb70b3c`（RT1B-3B final，已 push）
- 范围：只组装三组件进 `authoritycomposition.Runtime`；**不改 cmd/**、不接
  Remote Watch、不改 RouteManager/Actor Registry/Redis 布局。
- Git 候选、push、合并由 agentctl/控制器管理（Agent 不自行 push）。

## 0. Frozen Checkpoint CP-ROUTE-MEMBERSHIP-RUNTIME-CODE

```text
base                         = cb70b3c
feature                      = 10ae701
lifecycle_fix                = c9ffa88
single_truth_ownership_fix   = 00b7a42
fatal_closed_split           = c72bda2
terminal_race_fix            = de166a7
inflight_commit_barrier      = 8ee3688
final_head                   = 8ee3688   (pushed to
  refs/heads/codex/rt1b-3c-a-membership-runtime-composition-20260807)
review                       = PASS
status                       = READY_FOR_ROUTE_PREMERGE-CODE
```

## 1. Final Status

- 三组件已组装：`RealMembershipAdapter + ShardMembershipReconciler +
  PerShardLeaseScheduler`，入口 `authoritycomposition.NewMembershipRuntime`。
- 新路径是**唯一 Renew Owner**：不启动 legacy `runRenewal`；每个 installed
  generation 恰好一个 scheduler entry。
- 全部硬不变量有确定性测试覆盖，`go test` / `-race` / 全仓 / build 全绿。
- 五轮边界（in-flight Commit 线性化 / Terminal 清理 / Close-after-Fatal）已
  闭环，含故障注入有效性验证；Reviewer 复核 **PASS**。
- **已冻结**：`CP-ROUTE-MEMBERSHIP-RUNTIME-CODE` @ `8ee3688`。
- 后续 cmd 接线（RT1B-3C-B / I0-ROUTE-WIRE）在**新分支**进行，本 worktree
  关闭 Writer Thread：不在该分支改 cmd / Redis Probe，不扩大 Review 范围。

### 1.1 Final Frozen Invariants（冻结不变量，RT1B-3C-B 必须维持）

1. **Fatal/Closed 后禁止新的 Membership 副作用**：`runtimeGateMembershipAdapter`
   在 Prepare/Commit/Fence/Remove 前检查 `ensureMembershipActive()`，终态一律
   `ErrRuntimeTerminal`；late snapshot → `ObserveResult{Closed:true}`。
2. **Reconciler.Close 等待所有 in-flight Action 退出**：`Close()` 持有 action
   worker（`actionMu`）并 `wg.Wait()`，in-flight commit（已过 gate、parked 在
   真实安装前）在 Close 返回前必须完成。
3. **Fatal 最终清理发生在最后一个可能 Commit 之后**：`enterFatalCleanup` 中
   `failClosedLocalState()` 严格排在 `reconciler.Close()`（barrier）之后，
   最后一次 Guard 安装 happens-before 清理扫描。
4. **Fatal cleanup 使用 ungated 本地 generation fence**：`failClosedLocalState()`
   直接 `LeaseGuardRegistry.Remove`（不查 `ensureMembershipActive`），Terminal
   状态不阻止清理自身；普通 Reconciler 路径才走 gated adapter。
5. **Close 不覆盖 Fatal 原因，但仍完整回收资源**：`Close()` 状态 CAS 失败
   （已是 Fatal/Closed）仍无条件执行 cancel+`wg.Wait` → reconciler.Close →
   scheduler.Close → failClosedLocalState → legacy leases fence；Fatal 保持
   Fatal、`FatalErr` 保留 primary cause、不写新 FatalErr、不触 OnFatal；
   重复 Close 幂等。
6. **Snapshot 是完整状态；Observer 允许跳过中间 revision 并收敛最新值**：
   生产唯一 Snapshot 输入 Owner 是 `runMembershipObserver`（轮询最新完整快照，
   可跳过 R2 直接从 R1 diff 到 R3）；Provider 是唯一验证点；Revision rollback
   仍 Fail Closed。

## 2. Final Commits

| Commit | 内容 |
|---|---|
| `cb70b3c` | base：RT1B-3B final（已 push） |
| `10ae701` | RT1B-3C-A：membership runtime composition |
| `c9ffa88` | Focused Review 一轮：P1-1..P1-5 + P2 Stale 合同 |
| `00b7a42` | Focused Review 二轮：单真值 / 所有权过滤 / ADD 隔离 / 单 Observe Owner |
| `c72bda2` | Focused Review 三轮：Fatal 数据面 Fail-Closed + Closed 终态 |
| `de166a7` | Focused Review 四轮：Fatal 防重装 Gate / 构造合同 / 终态竞态 / 跳代收敛 |
| `8ee3688` | Focused Review 五轮：in-flight Commit 线性化（reconciler.Close action barrier）/ Terminal 清理确认 / Close-after-Fatal 完整回收（**final_head，已 push**） |

owned 写集：`composition.go`、`composition_test.go`、
`runtime_membership.go`、`runtime_membership_test.go`、
`membership.go`（仅 `Close` 增加 action barrier）、`membership_adapter.go`
（仅 TEST_ONLY `commitPreparedHook`）、本文档。
未触碰：`lease_scheduler.go`、`cmd/**`、`api/proto/**`、Actor Registry、
Redis Lua/KeyBuilder、H5。

## 3. Final Interfaces

### 新入口（composition.go / runtime_membership.go）

```go
func NewMembershipRuntime(ctx context.Context, opts Options) (*Runtime, error)
```

启动顺序：
1. `buildCompositionCommon`：Static 启动 Snapshot + Holder 身份验证
   （`ErrLocalHolderNotRegistered` fail closed）+ calculator/provider/
   resolver/applier/registry/fenceSource/scripts/store/authorityProvider/
   placement。
2. 构造 `RealMembershipAdapter`（每次 install 经 store 证明完整 generation）。
3. 构造 `PerShardLeaseScheduler`（唯一 Renew Owner；`OnFatal` → `ready=false`）。
4. 构造 `ShardMembershipReconciler`（`AllowedDomains=[opts.Domain]`，首次
   Observe 封印 `TopologyFingerprint`）。
5. `Observe(启动Snapshot)` 初始收敛：`!FullyReconciled || Rejected || Stale`
   → **fail closed 返回错误**；registered zero-shard holder 收敛即
   `ProcessReady=true`。
6. 启动 `runApplier`（既有 provider watch）+ `runMembershipObserver`
   （消费 provider 快照 → reconciler.Observe；`Rejected/Stale` → ready=false）。
7. **不启动** legacy `runRenewal`。

### Runtime 新增/改造

- `ObserveMembership(snapshot) ObserveResult`：外部驱动 reconciler 的透传；
  `Rejected/Stale` → ready=false。
- `ServingShardCount()` 改为基于 `Guards.SnapshotAll()` 统计 admitted guard
  （新/旧路径统一口径）。
- `Close()`：`scheduler.Close()` + `cancel/wg.Wait`（applier/observer）+
  旧 leases fence + `reconciler.Close()`。
- `Options` 新增 scheduler 调优（零值回退默认）：
  `LeaseBaseDelay/LeaseMaxDelay/LeaseJitter/RenewAttemptTimeout/
  RenewRecoveryTimeout/MembershipMaxRetries/MembershipRetryDelay`。

### 保留（向后兼容）

- `New(ctx, opts)` 保留 legacy 路径（`installLocalGrants` + `runRenewal`），
  cmd 未切换前不受影响；`composition_test.go` 10 个旧测试全部原样通过。

## 4. Final Invariants（硬不变量 → 测试）

| # | 不变量 | 测试 |
|---|---|---|
| 1 | 新 Scheduler 启用后旧 runRenewal 绝不启动 | `TestMembershipRuntimeStartupAdmitsAssignedShard`（`len(runtime.leases)==0` 且 EntryCount==1；`NewMembershipRuntime` 不调 runRenewal） |
| 2 | 同一 generation 唯一 Renew Owner | `TestMembershipRuntimeStartupAdmitsAssignedShard`、`TestMembershipRuntimeObserverDrivesRemoveAndReadd`（各阶段 EntryCount==1/0） |
| 3 | registered holder + zero shard → 初始收敛后 ProcessReady=true | `TestMembershipRuntimeStartupZeroShardProcessReady` |
| 4 | 启动快照有本地 shard → 安全 Admit 或明确 Fail Closed 后再 Ready | `TestMembershipRuntimeStartupAdmitsAssignedShard`；`NewMembershipRuntime` 对 `!FullyReconciled` fail closed |
| 5 | 正常 LostGeneration → 只终止目标 Shard、不拉低 ProcessReady | `TestMembershipRuntimeLostGenerationKeepsProcessReady` |
| 6 | 无法 Fence 失权 Shard → ProcessReady=false | `TestMembershipRuntimeFenceFailureLowersProcessReady`（OnFatal → ready=false）；`TestMembershipRuntimeRemoveFenceFailureImmediatelyLowersReady`（REMOVE fence 失败 + 旧 gen 仍 admitted → 立即 ready=false） |
| 7 | Topology 不兼容或 Scheduler Fatal → ProcessReady=false | `TestMembershipRuntimeTopologyMismatchFailsClosed`（Rejected → ready=false）；FenceFailure 同上 |
| 8 | Runtime 支持 0→1→G1→G2→0（合法控制面序列） | 启动快照 0/1 shard + `TestMembershipRuntimeObserverDrivesRemoveAndReadd`（REMOVE→RE-ADD 迁移往返）+ `TestMembershipRuntimeMigrationRemovesOldGenerationKeepsReady` |
| 9 | Reconciler installed == Registry admitted guards == Scheduler entries | `assertMembershipRuntimeConsistent` 各场景末尾 |
| 10 | 不批量加载 Actor | Runtime 组装只构造 route/authority 接缝，无 actor registry 代码路径（构造成功即证明） |

## 4.1 Focused Review 边界（P1-1..P1-5 / P2 → 修复 + 测试）

| 边界 | 修复 | 测试 |
|---|---|---|
| P1-1 初始 Fatal 不被 ready=true 覆盖 | 单向状态机 `membershipState`（Bootstrap→Ready 用 CAS；`enterFatal` 终态不可逆，OnFatal→`enterFatal`）；构造结束 CAS 失败即 fail closed | `TestMembershipRuntimeFatalIsStickyAfterStartup`（Fatal 后合法 observe 不复活） |
| P1-2 初始部分收敛失败完整回滚 | fail-closed 路径统一：`scheduler.Close` + `reconciler.Close` + `failClosedLocalState()`（fence+remove 全部已装 guard）；构造失败不返回 runtime | `TestMembershipRuntimeStartupPartialFailureFailsClosed` |
| P1-3 Applier 与 Observer 消费同一已验证序列 | 结构确认：provider 是唯一验证点（`Publish` 时 `validateRouteSnapshot`），Applier 与 observer 都消费 provider 已接受快照（无独立校验分叉） | `TestRejectedRouteSnapshotNeverReachesMembershipReconciler`（rollback 快照被 provider 拒，reconciler 不 Observe、ready 保持） |
| P1-4 运行期 REMOVE 无法 Fence 立即拉低 Ready | `checkMembershipResult`：degraded REMOVE/REPLACE 且 `oldGenerationStillAdmitted`（精确 gen 匹配）→ 立即 `ready=false`（不等下次 renew）；fence 已成功仅清理失败 → 保持 ready 后台重试 | `TestMembershipRuntimeRemoveFenceFailureImmediatelyLowersReady`、`TestMembershipRuntimeRemoveCleanupFailureAfterSuccessfulFenceKeepsReady` |
| P1-5 Close 完整 Fail-Closed 顺序 | `ready=false` → cancel observer/applier（`wg.Wait`）→ `reconciler.Close` → `scheduler.Close` → `failClosedLocalState`（guard fence+remove）→ legacy leases fence | `TestMembershipRuntimeCloseFailClosedAndIdempotent`（无 admitted guard/entry、serving 0、幂等）、`TestMembershipRuntimeCloseRejectsLateSnapshot` |
| P2 Stale 语义 | 冻结**方案 A**：生产唯一 Snapshot 输入 Owner 是 `runMembershipObserver`（单调）；`ObserveMembership` 仅为测试/未来控制面方法；任意 Stale/Rejected 视为控制面合同破坏 → `ready=false` | `TestRejectedRouteSnapshotNeverReachesMembershipReconciler`（拒绝不降 ready）、`TestMembershipRuntimeFatalIsStickyAfterStartup`（重复/合法 observe 不恢复） |

## 4.2 Focused Review 二轮边界（单真值 / 所有权 / ADD 隔离 / 单 Owner）

| 边界 | 修复 | 测试 |
|---|---|---|
| 双真值 | **删除 `ready` 字段**：`membershipState` 是唯一生命周期真值，`ProcessReady()` 直接派生 `state == Ready`。所有降级路径（runApplier 失败、runRenewal 失败、OnFatal、Topology 拒绝、REMOVE 无法 Fence、Close）统一走 `enterFatal()`/`state=Fatal`，无独立可写的 Bool，Fatal 后不可能残留 ready=true | `TestMembershipRuntimeFatalIsStickyAfterStartup`、全仓 `ready.Store/CompareAndSwap` 搜索 **0 残留** |
| 所有权过滤 | `ownsGuard`（Domain+HolderID+HolderBootID 精确匹配）约束 `failClosedLocalState` 与 `ServingShardCount`——共享 registry / 多 Domain 组合下 Close 或计数绝不触碰其他 Runtime 的 Guard | `TestRuntimeOwnershipFilterDoesNotTouchForeignGuards`（本域 owned；外域/外 holder/外 boot 均 not owned；ServingShardCount 只算本域） |
| 运行期 ADD 失败隔离 | `checkMembershipResult` **只**对 Rejected/Stale 与「REMOVE/REPLACE fence 失败且旧 gen 仍 admitted」进入 Fatal；动态 ADD 失败（degraded Add）不拉低 Runtime，目标 shard 保持 Fail Closed，其他 shard 继续 | `TestRuntimeDynamicAddFailureKeepsProcessReadyAndTargetShardClosed`（合法 rev2 增加不可安装 shard → ready 保持、shard0 服务、shard1 closed、EntryCount==1） |
| 单一 Observe Owner | `observeMembership` 缩为**包内方法**（TEST_ONLY / CONTROL_PLANE_ONLY）；生产唯一调用 `Reconciler.Observe` 的是 `runMembershipObserver`（注释+类型双重标注），防止 RT1B-3C-B 接线引入第二 Snapshot Owner | 编译级保证（无导出入口）+ 注释合同 |

## 4.3 Focused Review 三轮边界（Fatal 数据面 Fail-Closed / Closed 终态）

| 边界 | 修复 | 测试 |
|---|---|---|
| Fatal 必须数据面 Fail-Closed | `enterFatal(err)`：state→Fatal + **立即 failClosedLocalState**（fence/remove 全部 owned guard）+ **取消全部 owned scheduler entry**（遍历 ActiveEntries → generation-fenced Cancel，不调 scheduler.Close 避免 OnFatal 自锁）+ cancel snapshot 输入 + 记 `FatalErr`；`ProcessReady=false` 不再是唯一效果 | `TestRuntimeFatalImmediatelyFencesAllOwnedGuards`（guard 全不 admitted、EntryCount 0、ServingShardCount 0、FatalErr 记录）；`TestMembershipRuntimeTopologyMismatchFailsClosed` 增强（Rejected → serving 0 + 无 admitted guard + 无 entry） |
| 正常 Close ≠ Fatal | 状态机加 **Closed** 终态：`Close()` → `state=Closed`（**不**走 enterFatal、**不**记 FatalErr、**不**触 OnFatal、不污染 fatal 指标）；真正故障才 Fatal（记 FatalErr + OnFatal）；`ProcessReady() = state==Ready` | `TestRuntimeCloseIsNotFatal`（Close 后 state==Closed、scheduler.Fatal()==false、FatalErr()==nil） |
| 所有权 layout 不变量 | `ownsGuard` 冻结 Domain+HolderID+HolderBootID；**不变量**：同一 HolderBoot 内只运行一个 Authority Layout（`Options.Profile` 冻结 v1），布局迁移不得双 Runtime 共用同一 Registry（P2 记录，当前单布局不阻塞） | `TestRuntimeOwnershipFilterDoesNotTouchForeignGuards` |
| 部分启动失败 Probe | `NewMembershipRuntime` 失败路径**返回 fail-closed 的 runtime（非 nil）+ err**，供外部 Probe：ServingShardCount=0 / EntryCount=0 / 无 admitted guard；「Shard0 先真实 install 再 Shard1 失败」由 orderedPlan 按 shard key 确定性保证 | `TestMembershipRuntimeStartupPartialFailureFailsClosed`（外部 Probe 断言清理） |

## 4.4 Focused Review 四轮边界（Fatal 防重装 / 构造合同 / 终态竞态 / 跳代收敛）

| 边界 | 修复 | 测试 |
|---|---|---|
| Fatal 后禁止重新 Admit | `enterFatalCleanup` 严格顺序（与真实代码逐条一致）：① CAS state→Fatal（先终态者获胜，不覆盖）② 原子保存 primary cause 一次（`fatalErr.Store`）③ cancel snapshot 输入（observer/applier）④ **`reconciler.Close()`**：`closed` 拦截新 commit + **actionMu barrier 等待 in-flight action 退出**（见 4.5）+ 停 retry loop + `Observe` 返回 Closed ⑤ generation-fenced 取消 owned entries ⑥ `failClosedLocalState` fence/remove owned guards（④ 的 barrier 保证最后一次 Guard 安装 happens-before ⑥）⑦ cleanup 错误仅作为 secondary 诊断，不覆盖 primary cause ⑧ 锁外、至多一次 OnFatal；另有 **Runtime Fatal Gate**（`runtimeGateMembershipAdapter` 在 Prepare/Commit/Fence/Remove 前检查 `ensureMembershipActive`，终态一律 `ErrRuntimeTerminal`），双重防线保证 Fatal 后绝不 re-install | `TestRuntimeFatalCancelsDegradedAddRetry`（Fatal 后晚到 snapshot 返回 Closed）、`TestRuntimeFatalRejectsLateCommitPrepared`（gate 拒绝 Commit/Prepare/Fence/Remove）、`TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry`（4.5） |
| 构造失败 API 合同 | 公开 `NewMembershipRuntime` **恢复 `(nil, err)`** 惯例合同（失败对象绝不进 composition root）；`newMembershipRuntimeForTest`（包内 TEST_ONLY）供测试 Probe 回滚状态 | `TestMembershipRuntimeStartupPartialFailureFailsClosed`（ForTest Probe + 生产合同 `(nil,err)` 断言） |
| Close/Fatal 并发「先终态者获胜」 | `enterFatal` 仅 CAS Bootstrap/Ready→Fatal（Closed/Fatal 已成立则返回：不覆盖、不写 FatalErr、不 OnFatal）；`Close` 同样 CAS 到 Closed（Fatal 已成立则保留 Fatal + primary cause）；并发无自锁（enterFatal 不 scheduler.Close） | `TestCloseWinsConcurrentFatalWithoutRecordingFatal`、`TestFatalWinsConcurrentCloseAndPreservesCause`、`TestCloseAndFatalConcurrentNoDeadlock` |
| Snapshot 跳代收敛合同 | 冻结：observer 轮询**最新完整快照**，可跳过中间 revision；Provider 已逐次验证 generation progression；Reconciler 从完整快照直接 diff（R1→R3），跳过 R2 不产生残留 | `TestMembershipObserverMaySkipIntermediateFullSnapshotsAndConverge`（快速 publish R2/R3 → observer 收敛到 R3、无 R2 残留、ready 保持） |
| OnFatal 通知 | `Options.OnFatal`：runtime 进入 Fatal 时（CAS 成功后、锁外、至多一次）调用；正常 Close 不调用；RT1B-3C-B 接 ProcessReady 探针 | `TestRuntimeCloseIsNotFatal`（Close 不触发 OnFatal） |

## 4.5 Focused Review 五轮边界（in-flight Commit 线性化 / Terminal 清理 / Close-after-Fatal）

| 边界 | 修复 | 测试 |
|---|---|---|
| 已通过 Gate 的 Commit 不得在 Fatal 后落地 | `reconciler.Close()` 增加 **actionMu barrier**（`actionMu.Lock/Unlock`）：in-flight action（已过 runtime gate、parked 在 gate 检查与真实 `InstallIfMatches` 之间）在 Close 返回前必须完成；ctx 已 cancel → 阻塞的网络 Prepare 中止、本地 commit 有界。`failClosedLocalState` 因此线性化发生在最后一个可能的 Guard 安装**之后**，竞态窗口被消除 | `TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry`（确定性时序：commit 过 gate → hook 阻塞真实安装 → enterFatal → 释放 commit → state==Fatal / ServingShardCount==0 / EntryCount==0 / 无 owned admitted guard）。故障注入验证：临时移除 barrier → `serving=1` FAIL，证明测试真实捕获该竞态 |
| Fatal 清理绕过 Runtime Gate | `failClosedLocalState` 直接操作 `LeaseGuardRegistry.Remove`（**ungated**、generation-fenced 本地原语），不查 `ensureMembershipActive`；普通 Reconciler 路径（Prepare/Commit/Fence/Remove）一律走 gated adapter。职责分离：普通路径 gated、终态清理 ungated，Terminal 状态不阻止清理自身 | `TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry` 尾部断言 owned admitted guard==0；`TestRuntimeFatalRejectsLateCommitPrepared`（普通路径在终态被 gate 拒绝） |
| Fatal 后 Close 完整回收且不覆盖 | `Close()` 的状态 CAS 失败（已是 Fatal/Closed）**仍无条件执行资源回收**：cancel + `wg.Wait`（observer/applier）→ `reconciler.Close` → `scheduler.Close` → `failClosedLocalState` → legacy leases fence。Fatal 保持 Fatal、FatalErr 保留 primary cause（Close 从不写 FatalErr）、不触 OnFatal；重复 Close 幂等、无 goroutine 泄漏 | `TestCloseAfterFatalPreservesCauseAndReapsAllResources`（Fatal→Close→二次 Close：state 仍 Fatal、cause 保留、serving/entries/guards 全 0、二次 Close 有界返回） |

## 5. Final Tests

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
go test -buildvcs=false ./... -count=1                     PASS
go build -buildvcs=false ./cmd/...                         PASS
git diff --check                                            PASS
```

新增测试（runtime_membership_test.go，共 27 个）：
- `TestMembershipRuntimeStartupZeroShardProcessReady`
- `TestMembershipRuntimeStartupAdmitsAssignedShard`
- `TestMembershipRuntimeMigrationRemovesOldGenerationKeepsReady`
- `TestMembershipRuntimeObserverDrivesRemoveAndReadd`
- `TestMembershipRuntimeLostGenerationKeepsProcessReady`
- `TestMembershipRuntimeFenceFailureLowersProcessReady`
- `TestMembershipRuntimeTopologyMismatchFailsClosed`
- `TestMembershipRuntimeFatalIsStickyAfterStartup`（P1-1）
- `TestMembershipRuntimeStartupPartialFailureFailsClosed`（P1-2）
- `TestRejectedRouteSnapshotNeverReachesMembershipReconciler`（P1-3）
- `TestMembershipRuntimeRemoveFenceFailureImmediatelyLowersReady`（P1-4）
- `TestMembershipRuntimeRemoveCleanupFailureAfterSuccessfulFenceKeepsReady`（P1-4）
- `TestMembershipRuntimeCloseFailClosedAndIdempotent`（P1-5）
- `TestMembershipRuntimeCloseRejectsLateSnapshot`（P1-5）
- `TestRuntimeOwnershipFilterDoesNotTouchForeignGuards`（二轮：所有权过滤）
- `TestRuntimeDynamicAddFailureKeepsProcessReadyAndTargetShardClosed`（二轮：ADD 失败隔离）
- `TestRuntimeFatalImmediatelyFencesAllOwnedGuards`（三轮：Fatal 数据面 Fail-Closed）
- `TestRuntimeCloseIsNotFatal`（三轮：Closed 终态）
- `TestCloseWinsConcurrentFatalWithoutRecordingFatal`（四轮：终态竞态）
- `TestFatalWinsConcurrentCloseAndPreservesCause`（四轮：终态竞态）
- `TestCloseAndFatalConcurrentNoDeadlock`（四轮：终态竞态）
- `TestRuntimeFatalRejectsLateCommitPrepared`（四轮：Runtime Fatal Gate）
- `TestRuntimeFatalCancelsDegradedAddRetry`（四轮：Fatal 后不重装）
- `TestMembershipObserverMaySkipIntermediateFullSnapshotsAndConverge`（四轮：跳代收敛）
- `TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry`（五轮：in-flight Commit 线性化）
- `TestCloseAfterFatalPreservesCauseAndReapsAllResources`（五轮：Close-after-Fatal 完整回收）

既有测试全部保持（composition_test.go 10 个 + RT1B-3B/3A 全套）。

## 6. Remaining Work

- **cmd 接线（RT1B-3C-B / cmd 任务）**：player-svr/farm-svr 从 `New` 切到
  `NewMembershipRuntime`，`OnFatal→ProcessReady` 联动 ready 探针，退役
  legacy `runRenewal` 代码路径。
- 跨进程动态 assignment 发布/watch 仍属控制面（不接）。
- 真实 Redis 持久化/容灾/Lua 延迟/集群路由（Q2R-2，PROBE_PENDING）。
- 运行期 reconciler degraded（REMOVE fence 失败）不主动拉低 ready（当前由
  scheduler OnFatal 覆盖失权 fence 失败；控制面 REMOVE 失败重试中）。

## 7. Superseded / 说明

- 0→1→0 的**单进程**快照序列受 actorrouting generation-progression 校验
  限制（holder 迁移必须 gen 严格前进），测试用合法控制面序列
  （transfer + gen 前进）驱动 observer 验证 REMOVE/RE-ADD。
- legacy `New` 保留（cmd 未切换），`runRenewal` 的彻底退役发生在 cmd 切换
  后；本任务保证新路径绝不启动它。
