# RT1B-2A SHARD-MEMBERSHIP-CORE Handoff

- 分支：`codex/rt1b-2a-shard-membership-core-20260806`
- base commit：`d20144c`（RT1B-1 F1 最终修复 commit）
- 任务 ID：RT1B-2A-SHARD-MEMBERSHIP-CORE
- worktree：`farm-game-rt1b-2a-shard-membership-core-20260806`（独立目录，未复用 RT1B-1 分支写代码）

## 目标与范围

基于完整 RouteSnapshot 实现本地 ShardMembershipReconciler Core，使 Holder 在
运行中可确定性计算并执行 `ADD / REMOVE / REPLACE / UNCHANGED`。

本任务只实现 **Membership 状态机、Diff、Revision 与 Fake Port**。未接
Farm/Player Registry、未接 cmd、未接跨进程 Watch、未动 Redis Authority 数据。

## 冻结不变量 → 实现映射

| # | 不变量 | 实现 |
|---|---|---|
| 1 | registered 0-Shard Holder 保持 ProcessReady | Reconciler 不触碰 composition 的 ProcessReady/ready；0 本地 shard 时 desired 为空、零 action、fully reconciled，Runtime.Membership 端口 nil 时无任何副作用 |
| 2 | 未安装 Shard 始终 CanServe=false | Reconciler 只记录成功 Install 的 membership；未安装的 shard 不会出现在 `CurrentInstalledMembership`，guard 未安装则 `CanServeShard=false`（RT1B-1 语义保持） |
| 3 | REMOVE/FENCE 优先于 ADD/ADMIT | `diffMembership` 把 REMOVE/REPLACE 排在 ADD 前；`orderedPlan` 按"先 fence 类、后 add 类"执行；单次 REPLACE 严格 Fence(old) → Install(new) |
| 4 | 旧 Snapshot 不得复活旧 Shard | `Observe` 拒绝 `GlobalRevision < lastObservedRevision` 的快照（`ErrMembershipStaleSnapshot` 语义），stale 快照不产生任何 action |
| 5 | 重复 Snapshot 幂等 | 同 revision 重复 `Observe` → `Duplicate=true`，沿用同一 desired 集；已 fully reconciled 时零 action、不重复调用 Ports |
| 6 | generation 变化必须 Fence 旧后再安装新 | REPLACE：`applyAction` 先 `FenceMembership(old)` 成功后才 `InstallMembership(new)` |
| 7 | 单 ShardAdd 失败只影响目标 Shard | `executeAction` 按 shard 独立执行；失败只记录该 shard 的 `retries`/`degraded`，其余 shard 照常安装 |
| 8 | 未完全应用不得推进 fully_reconciled_revision | `applyRevisionProgress` 仅在 `pending` 与 `degraded` 均为空时推进 `lastFullyReconciledRevision = lastObservedRevision` |
| 9 | Shard 迁移不批量加载 Actor | Core 无 Actor 枚举/加载 API；`TestMembershipNoActorBulkLoad` 证明 4 个 desired shard 恰好 4 次 install、无任何批量行为 |
| 10 | 不修改 Redis Authority 数据位置 | 未触碰 Redis Lua/KeyBuilder/Store；Ports 在本任务为 Fake，真实适配留给 RT1B-3 |

## owned_write_set 变更

- `internal/authoritycomposition/membership.go`（新增）：Reconciler 核心、Ports、
  revision、bounded retry、per-shard degraded、close/cancel。
- `internal/authoritycomposition/membership_diff.go`（新增）：
  `desiredLocalShards`、`diffMembership`、`orderedPlan` 排序辅助。
- `internal/authoritycomposition/membership_internal_test.go`（新增，package
  authoritycomposition）：未导出函数（desired/diff/revision/currentInstalled）测试。
- `internal/authoritycomposition/membership_test.go`（新增，package
  authoritycomposition_test）：集成与 race 测试。
- `internal/authoritycomposition/testkit/ports.go`（新增）：
  `FakeMembershipPorts`（install/fence/remove 记录、顺序、按 shard 注入失败、
  阻塞注入、Reset）。
- `internal/authoritycomposition/composition.go`（窄修改）：`Runtime` 新增导出
  字段 `Membership *ShardMembershipReconciler` 作为 Membership Port 暴露点；
  未改动 `Runtime.leases`/`New()`/Ready 语义。
- `docs/verification/rt1b-2a-membership-handoff.md`（本文档）。

未触碰 `forbidden_write_set`（cmd/api/proto/actor/ingress/downlink/Redis
Lua/KeyBuilder/H5/自动迁移/K8s）。

## 必须实现清单 → 落点

- `desiredLocalShards(snapshot, holder, boot)`：membership_diff.go，仅选 ACTIVE
  且 holder/boot 精确匹配的完整 route，双 domain。
- `currentInstalledMembership`：membership.go `CurrentInstalledMembership()`。
- ADD/REMOVE/REPLACE/UNCHANGED diff：membership_diff.go `diffMembership`。
- `lastObservedRevision`：`LastObservedRevision()`（stale 拒绝、duplicate 幂等）。
- `lastFullyReconciledRevision`：`LastFullyReconciledRevision()`（不变量 8）。
- ordered action execution：`orderedPlan` + `applyAction`（fence 类先于 add 类）。
- bounded retry state：`retries` + `MaxRetries`，超限后 shard 稳态 degraded。
- close/cancel lifecycle：`Close()`（幂等，cancel + 等待 retry loop）。
- per-shard degraded status：`ShardDegraded`（Shard/Action/Retries/LastErr）。
- Fake installer/fencer/remover：testkit `FakeMembershipPorts`。

## 测试（外部 + 内部 + race）

- `TestMembershipZeroOneZero`：0→1→0 全生命周期，revision 1→2→3 推进，fence+remove。
- `TestMembershipDuplicateSnapshotIsIdempotent`：重复快照零重复 install。
- `TestMembershipStaleSnapshotIsRejected`：低 revision 拒绝、不丢已装 membership。
- `TestMembershipGenerationReplacementFencesBeforeInstall`：REPLACE 顺序 fence→install，
  旧 generation 被 fence、新 generation 被 install。
- `TestMembershipRemoveBeforeAddOrdering`：同快照 REMOVE(含 fence/remove) 先于 ADD。
- `TestMembershipPartialFailureAndRetry`：单 shard 失败→degraded、revision 不推进、
  Reconcile 重试成功、fully reconciled 恢复。
- `TestMembershipFailureIsScopedToTargetShard`：失败只影响目标 shard，健康 shard 不被重装。
- `TestMembershipBoundedRetrySettlesDegraded`：超 MaxRetries 后稳态 degraded，不推进 revision。
- `TestMembershipCloseDuringReconcile`：阻塞中的 install 被 Close 取消、无泄漏、
  Close 幂等、post-close 操作 Closed=true。
- `TestMembershipNoActorBulkLoad`：4 desired shard 恰好 4 次 install。
- `TestMembershipConcurrentObserveReconcileClose`：并发 Observe/Reconcile/getter，-race 覆盖。
- 内部测试：`TestDesiredLocalShardsSelectsOnlyLocalActiveCompleteRoutes`、
  `TestDesiredLocalShardsCoversBothDomains`、`TestDiffMembershipClassifiesAddRemoveReplaceUnchanged`、
  `TestDiffMembershipOrdersRemoveBeforeAdd`、
  `TestReconcilerRevisionAndInstalledLifecycle`、
  `TestReconcilerGenerationReplacementFencesBeforeInstall`。

## 验收（全部真实 Exit 0）

```text
go test ./internal/authoritycomposition/... -count=1     PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1    PASS
go test -buildvcs=false ./... -count=1                   PASS（全仓无回归）
go build -buildvcs=false ./cmd/...                       PASS
git diff --check                                          PASS
```

## 未完成项（RT1B-2 后续）

- 后台 retry loop 使用固定 `RetryDelay` 轮询；未做指数退避（bounded 语义已满足）。
- Reconciler 当前同步执行全部 action（单 flight，持内部锁）；真实 Ports 若阻塞需
  RT1B-3 评估锁粒度。
- composition `Runtime.Membership` 仅为暴露端口，未在 `New()` 内接线（真实
  installer/fencer/remover 由 RT1B-3 注入）；`Runtime.leases` 未改动。

## RT1B-3 所需接口

1. **真实 MembershipInstaller**：把 `ShardMembership.Generation` 转化为
   `authoritystore.RenewShardAuthorityRequest` + 本地 guard 安装（复用
   `InstallHolderLeaseGrant`），实现 `InstallMembership(ctx, member)`；幂等语义
   （同 generation 重复调用返回已应用）。
2. **真实 MembershipFencer**：对旧 generation 执行本地 guard Fence 并（若必要）
   触发 Redis 侧观察/换代确认，实现 `FenceMembership(ctx, member)`。
3. **真实 MembershipRemover**：卸载本地 guard（`LeaseGuardRegistry.Remove`/观察
   换代），实现 `RemoveMembership(ctx, member)`。
4. **接线点**：`Runtime.Membership` 由 cmd composition root 构造注入；Reconciler
   的 `Observe` 输入来自现有 `SnapshotApplier`/`DynamicAssignmentProvider` 的完整
   RouteSnapshot（本任务未接，避免越界）。
5. **ProcessReady 集成决策**：是否在 membership fully-reconciled 之前将
   ProcessReady 拉低属 cmd 层决策，本任务不预设。

---

# RT1B-2B MEMBERSHIP-CORE-HARDENING（FIX，追加于 2026-08-06）

分支 `codex/rt1b-2a-shard-membership-core-20260806`；基线 `f024de4`；两个修复
Commit：`425f740`（C1 fix）、C2 refactor（本 handoff 随 C2 提交）。禁止 push 与
合并，按要求仅提交本地。

## F1~F5 关闭证据

| Finding | 关闭方式 | 验证 |
|---|---|---|
| F1 旧 Action 迟到于 N+1 污染新 generation | C2 引入 ActionToken + `beginAction`/`commitAction`：Port 返回后仅当 token 仍对应当前 `pending` 才提交；`replan` 每次新 revision 分配严格递增 token 并覆盖旧 pending | `TestBlockedOldInstallCannotCommitAfterNewRevision`、`TestBlockedOldRemoveCannotRemoveNewGeneration`、`TestBlockedOldFenceCannotFenceNewGeneration`、`TestNewRevisionIsObservedWhileOldPortIsBlocked` |
| F2 拓扑变化 Fail Closed | C1 引入 `TopologyFingerprint`（HashVersion/HashSeed/PLAYER·FARM ShardCount/AuthorityLayoutVersion）首观测封印，后续变化返回 `Rejected`，不解释为 ADD/REMOVE | `TestMembershipRejectsHashVersionChange`、`...HashSeedChange`、`...PlayerShardCountChange`、`...FarmShardCountChange` |
| F3 Port 不得持状态锁 | C2 重构：`observe` 分阶段 A（锁内 diff）→ B（锁外 `tryExecutePlan`，单 action worker `actionMu` 串行 Port）→ C（锁内提交）；`beginAction`/`commitAction` 各自短暂持锁，Port 调用全程无 `r.mu` | `TestMembershipGettersRemainResponsiveWhilePortBlocked`、`TestNewRevisionIsObservedWhileOldPortIsBlocked` |
| F4 `Runtime.Membership` 私有化 | C1 改为私有 `atomic.Pointer`，`SetMembershipReconciler` 一次性注入（替换拒绝）、`MembershipReconciler()` 无锁 getter；`Runtime.Close` 释放 reconciler | `TestRuntimeMembershipReferenceCannotBeReplaced` |
| F5 `ShardDegraded` 携带完整 generation | C1 加 DesiredGeneration/OldGeneration/SnapshotRevision；C2 加 ActionToken | `TestShardDegradedIncludesDesiredAndOldGeneration`（含 ActionToken 断言） |

## 同时收口

- **AllowedDomains**：`MembershipReconcilerOptions.AllowedDomains`；空=全部，
  player Runtime 传 `[PLAYER]`、farm 传 `[FARM]`（C1）。
- **duplicate 不重置 retry budget**：`observe` duplicate 分支不 `replan`、不碰
  `retries`（C1/C2 保持）；`TestDuplicateSnapshotDoesNotResetRetryBudget`。
- **新 generation 新 token**：`replan` 每次 `tokenSeq++`；`TestNewGenerationGetsNewActionToken`。
- **Close 取消阻塞 Port 并回收 worker**：Ports 接收 `r.ctx`，Close cancel 后
  blocked Port 返回 ctx.Err；run worker 在 ctx.Done 退出（C2）；
  `TestCloseCancelsBlockedMembershipPort`。

## 并发安全前提（RT1B-3 接线约束）

- `r.mu` 只保护 desired/installed/pending/retries/degraded/revision/topology/
  tokenSeq/inflight；**绝不**在 `r.mu` 下调用任何 Port。
- 单 action worker（`actionMu`）串行化 Port；`tryExecutePlan` 用 `TryLock`，
  worker 被阻塞 Port 占用时新 pass 跳过执行、留给重试循环，Observe/Reconcile/
  getter 不被卡死。
- Fence/Remove 的 expected generation（`OldGeneration`）作为 compare-and-act
  句柄传给 Port；真实 adapter 必须校验 installed generation 匹配后才 Fence/
  Remove（接口签名不变，语义在 RT1B-3 强化）。

## RT1B-2B 验收（全部真实 Exit 0）

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
go test -buildvcs=false ./... -count=1                     PASS
go build -buildvcs=false ./cmd/...                         PASS
git diff --check                                            PASS
```

## RT1B-3A 稳定 Port 签名

```go
// Installer: 幂等安装给定 generation（同 generation 重复调用视为已应用）。
InstallMembership(ctx context.Context, member authoritycomposition.ShardMembership) error
// Fencer: 仅当 installed generation == member.Generation（expected old）时 Fence；
// 不匹配必须返回错误（compare-and-act）。
FenceMembership(ctx context.Context, member authoritycomposition.ShardMembership) error
// Remover: 仅当 installed generation == member.Generation（expected old）时卸载；
// 不匹配必须返回错误。
RemoveMembership(ctx context.Context, member authoritycomposition.ShardMembership) error
// 构造：NewShardMembershipReconciler(MembershipReconcilerOptions{
//   HolderID, HolderBootID, Installer, Fencer, Remover,
//   AllowedDomains: []RouteDomain{<本进程 domain>},
//   AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion,
//   MaxRetries, RetryDelay, Now})
// 注入：Runtime.SetMembershipReconciler(m)（一次）；读取 Runtime.MembershipReconciler()。
```

## 未完成项（RT1B-3）

- 真实 Redis/Lease adapter 未实现（本任务仅 Fake）；adapter 必须尊重传入 ctx 以便
  Close 取消。
- 单 action worker 串行，无多 shard 并行；指数退避未做（任务明确不要求）。
- `Runtime.leases`（RT1B-1 的静态 guard 安装）未与 Reconciler 接线；真实
  Installer/Fencer/Remover 由 RT1B-3 注入后才形成闭环。
