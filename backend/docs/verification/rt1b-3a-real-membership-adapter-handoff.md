# RT1B-3A REAL-MEMBERSHIP-ADAPTER Handoff

- 分支：`codex/rt1b-3a-real-membership-adapter-20260806`
- 依赖 commit：`9819880`（RT1B-2B C2）
- 任务 ID：RT1B-3A-REAL-MEMBERSHIP-ADAPTER
- worktree：`farm-game-rt1b-3a-real-membership-adapter-20260806`（独立目录）
- 约束：不实现跨进程 Watch、不改 cmd、不实现自动迁移、不接 Actor Registry、不批量加载 Actor；不 push、不合并 integration。

## 目标

把 `ShardMembershipReconciler` 的稳定 Ports 实现为真实本地 Adapter：

- `InstallMembership`（安装 desired generation）
- `FenceMembership`（FenceIfMatches：精确 generation compare-and-act）
- `RemoveMembership`（RemoveIfMatches：精确 generation compare-and-act）

接入现有 `ShardWriteAuthority` Repository（Renew）、`HolderLeaseRuntime`、
`LeaseGuardRegistry`、`InstallHolderLeaseGrant`。

## owned_write_set 变更

- `internal/authoritycomposition/membership_adapter.go`（新增）：真实 Adapter。
- `internal/authoritycomposition/membership_adapter_test.go`（新增）：10 个测试。
- `docs/verification/rt1b-3a-real-membership-adapter-handoff.md`（本文档）。

## conditional_write_set 变更

- `internal/logic/actor/authority/generation_fence.go`（新增）：窄原语
  `LeaseGuardRegistry.InstallIfMatches / FenceIfMatches / RemoveIfMatches` 与
  `authorityGenerationEqual`。只增加，不改既有 Authority 语义（Claim/Transfer/
  Drain/Renew/WriteFence 未动）。

未触碰 forbidden_write_set（cmd/api/proto/actor/ingress/downlink/manager.go/
Remote Watch/RouteManager/Redis 布局/H5）。

## 冻结不变量 → 实现映射

| # | 不变量 | 实现 |
|---|---|---|
| 1 | Install 前证明 ShardWriteAuthority 全字段 | `InstallMembership` 先 `RenewShardAuthority`（Holder caller + ExpectedFence 全字段），再 `validateInstalledAuthority`（ACTIVE + layout/domain/shard/holder/boot/assignment/writer/lease + LeaseUntil>0）通过后才 admit |
| 2 | AcceptedTTL 转本地单调 Deadline | 复用 `InstallHolderLeaseGrant` → `HolderLeaseRuntime.InstallGrant` → `setLocalDeadlineLocked`（requestStarted + acceptedTTL − safetyMargin）；热路径用本地单调时钟，不重读权威 Unix 时间 |
| 3 | 同 generation 重复 Install 幂等 | `InstallIfMatches` 命中已装同 generation → 直接返回现有 runtime（不重新安装）；`TestDuplicateGenerationInstallIsIdempotent` |
| 4 | 旧 generation Install 不覆盖新 | `InstallIfMatches` 仅允许：无 guard / 同 generation（幂等）/ `generationStrictlyNewer(expected, installed)` 严格升级；其余拒绝（`ErrGenerationMismatch`）；`TestOldInstallCannotReplaceNewGeneration` |
| 5 | FenceIfMatches 只 Fence 精确 generation、立即 CanServeShard=false | `FenceIfMatches` 本地 `handle.Fence()`（无网络）；本地 deadline 过期也在 `snapshotLocked` 中即时 Fence；`TestOldFenceCannotFenceNewGeneration` |
| 6 | RemoveIfMatches 只移除精确 generation | `RemoveIfMatches` 仅精确匹配时 `registry.Remove`；`TestOldRemoveCannotRemoveNewGeneration` |
| 7 | Install 失败保持 Fail Closed | Renew/校验失败即返回，无 guard 安装，无"先 Admit 再验证"路径；`TestAuthorityMismatchKeepsShardFailClosed` |
| 8 | 单 Shard 失败不影响 ProcessReady/其他 shard | Reconciler per-shard degraded（RT1B-2B 不变）；`TestSingleShardFailureDoesNotAffectOtherShard` |
| 9 | 不遍历/加载 Actor | Adapter 仅操作 store/registry/guard；`TestMembershipInstallDoesNotLoadActors`（`Inspect` 无 record） |
| 10 | Baseline 拓扑来自启动快照 | 本任务不接远程 watch，无远程封印路径；指纹封印仍由 Reconciler 负责，RT1B-3B Scheduler 接线时必须用 RT1B-1 启动快照封印（见下文接缝说明） |

## 实际 Port 签名

```go
// authoritycomposition.RealMembershipAdapterOptions
type RealMembershipAdapterOptions struct {
	Store                  authoritystore.ShardWriteAuthorityStore
	Registry               *routeauthority.LeaseGuardRegistry
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	HolderID               string
	HolderBootID           string
	LeaseTTL               time.Duration
	SafetyMargin           time.Duration
	Now                    func() time.Time
}

func NewRealMembershipAdapter(opts RealMembershipAdapterOptions) (*RealMembershipAdapter, error)

// 实现接口（authoritycomposition 包）：
//   InstallMembership(ctx, member ShardMembership) error
//   FenceMembership(ctx, member ShardMembership) error
//   RemoveMembership(ctx, member ShardMembership) error

// 窄原语（internal/logic/actor/authority，新增文件 generation_fence.go）：
func (r *LeaseGuardRegistry) InstallIfMatches(domain, shard, expected ShardWriteAuthority,
	guard *HolderLeaseGuard, requestStarted time.Time, result ShardAuthorityTransitionResult,
	now func() time.Time) (*HolderLeaseRuntime, error)
func (r *LeaseGuardRegistry) FenceIfMatches(domain, shard, expected ShardWriteAuthority) error
func (r *LeaseGuardRegistry) RemoveIfMatches(domain, shard, expected ShardWriteAuthority) error
```

## 0→1→0 证据

`TestRealMembershipInstallZeroToOne`：claim → Observe(rev1 本地) → ADD →
Renew+InstallIfMatches → `registry.Snapshot(PLAYER,0).Admitted=true`、
installed=1、`LastFullyReconciledRevision=1`。
`TestRealMembershipRemoveOneToZero`：Observe(rev2 移走) → REMOVE →
FenceIfMatches+RemoveIfMatches → guard 从 registry 移除、installed=0、
reconciled revision 推进。

## generation 竞态证据

- `TestOldInstallCannotReplaceNewGeneration`：装 gen1 → store 升 gen2 → 装 gen2
  （严格升级允许）→ 旧 Install(gen1) 被 Renew/InstallIfMatches 双层拒绝，installed
  保持 gen2 且 admitted。
- `TestOldFenceCannotFenceNewGeneration`：装 gen2 后旧 Fence(gen1) 拒绝，gen2 仍
  admitted。
- `TestOldRemoveCannotRemoveNewGeneration`：装 gen2 后旧 Remove(gen1) 拒绝，guard 保留。
- `TestDuplicateGenerationInstallIsIdempotent`：同 generation 重复 Install 幂等成功。
- `TestAuthorityMismatchKeepsShardFailClosed`：desired writer_epoch 与 store 不符 →
  Renew fence mismatch → 无 guard、不 reconcile。
- `TestLocalDeadlineExpiryStopsAdmission`：AcceptedTTL(30s)−margin(5s) 后本地 deadline
  过期 → admitted=false（不依赖远程时钟）。

## 验收（全部真实 Exit 0）

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
go test -buildvcs=false ./... -count=1                     PASS（全仓无回归）
go build -buildvcs=false ./cmd/...                         PASS
git diff --check                                            PASS
```

## RT1B-3B 所需 Scheduler 接缝

1. **Scheduler 驱动 Reconciler**：后台循环以受控节奏调用
   `Reconciler.Observe(完整 RouteSnapshot)`（沿用 RT1B-2B 的 Observe 语义）；
   新快照来源为未来的动态 assignment（本任务未接跨进程 watch）。
2. **Baseline 拓扑封印**：首次 `Observe` 必须使用 RT1B-1 已验证的启动快照
   （`Runtime.New` 内 `LoadStaticAssignmentProvider` 加载的那一份）完成
   `TopologyFingerprint` 封印；任何远程快照都不得自行封印（不变量 10）。
3. **Runtime 接线**：`NewRealMembershipAdapter(Options{Store, Registry, Layout,
   HashVersion/HashSeed, HolderID/BootID, LeaseTTL, SafetyMargin, Now})` 构造
   adapter；`NewShardMembershipReconciler(Options{Installer/Fencer/Remover:
   adapter, AllowedDomains: [<本进程 domain>], AuthorityLayoutVersion, ...})`；
   经 `Runtime.SetMembershipReconciler(m)` 注入（一次性）。
4. **ProcessReady 联动决策**（cmd 层，本任务不预设）：是否在 membership
   fully-reconciled 之前拉低 ProcessReady。
5. **续租策略**：adapter 的 `Install` 负责首装与换代；常规续租由现有
   `HolderLeaseRuntime.Renew`/renewal 循环承担，adapter 不重复实现。

## 未完成项

- 跨进程动态 assignment 发布/watch（RT1B 后续，非本任务）。
- cmd composition root 接线（player-svr/farm-svr 注入 adapter + Reconciler）。
- ProcessReady 与 fully-reconciled 的联动策略。
- adapter 的真实 Redis 适配器（当前 MemoryStore 测试；Redis Store 为同接口）。

---

# RT1B-3A FIX：TWO-PHASE-INSTALL（追加于 2026-08-06）

基线 `296c721`（RT1B-3A）；本 FIX 分两个 Commit（C1 two-phase、C2 deadline/hash）。

## 变更：MembershipInstaller 拆两阶段（F1/F2 关闭）

`MembershipInstaller` 由单一 `InstallMembership` 拆为：

```go
type MembershipInstaller interface {
	PrepareMembership(ctx, member) (*PreparedMembership, error)
	CommitPreparedMembership(ctx, prepared) error
}
type PreparedMembership struct {
	Key            ShardKey
	Generation     MembershipGeneration
	RequestStarted time.Time
	AcceptedTTLms  int64
	Authority      authoritystore.ShardWriteAuthority
}
```

- **PrepareMembership**（adapter 阶段 1）：锁外执行 `RenewShardAuthority` + 全字段
  Authority 校验，返回 `PreparedMembership`；**不修改 LeaseGuardRegistry、不 Admit**。
- **CommitPreparedMembership**（adapter 阶段 2）：纯本地、无网络、短临界区，
  仅经 `LeaseGuardRegistry.InstallIfMatches` 安装/刷新 guard。
- Reconciler `executeAction`：ADD = Prepare(锁外) → commitPrepared(锁内)；
  REPLACE = Fence(old)(锁外) → Prepare(锁外) → commitPrepared(锁内)；
  REMOVE = Fence+Remove(锁外) → commitRemove(锁内)。`commitPrepared` 仅在
  token 仍对应当前 `pending` 时执行 `CommitPreparedMembership` 并原子收口
  `installed`；token 已 stale → **discard，不安装**（F1：不存在"先安装再异步
  清理"路径）。

## F1/F2 关闭证据

- F1（superseded Install 不残留旧 Guard）：Prepare 无副作用；Commit 仅 token
  匹配时执行，stale token 直接丢弃。
  - `TestPreparedOldInstallIsDiscardedAfterNewRevision`（Reconciler+Fake，
    确定性 PhaseBarrier：prepare 阻塞 → rev2 supersede → 释放 → 旧 prepared
    不 commit，最终仅 gen2 安装）。
  - `TestPreparedOldInstallCannotBecomeAdmittedAfterRemove`（prepare 阻塞 →
    rev2 移走 → 释放 → 不 install、不 installed）。
  - `TestPreparedOldInstallCannotOverwriteNewGeneration`（adapter+gate：
    gen1 prepare 成功 → gen2 先 commit → 旧 gen1 commit 被
    `InstallIfMatches` 拒绝，registry 保持 gen2）。
- F2（确定性 PhaseBarrier 测试）：上述三个测试分别覆盖"Renew 成功后 install
  前 supersede"（gate/block 注入）的确定性时序。

## testkit 适配

- `FakeMembershipPorts` 实现 `PrepareMembership`（记录 `prepare`，支持
  `SetPrepareBlock` 阻塞网络阶段）/ `CommitPreparedMembership`（记录 `install`
  并应用 commit 错误注入）；`SetInstallBlock` 语义迁移到 `SetPrepareBlock`。
- 既有 block 测试（Observe/getter/Close 响应性、blocked old install）改用
  prepare 阻塞；REPLACE 顺序断言更新为 `fence → prepare → install`。

## C1 验收（全部真实 Exit 0）

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
```

## C2：刷新精确 generation 的 lease deadline 与 hash fence（F3/F4 关闭）

- **F3（同 generation 刷新本地 deadline）**：`InstallIfMatches` 幂等路径不再直接
  返回，而是调用 `handle.InstallGrant(requestStarted, result)` 用本次
  `AcceptedTTLms` 刷新现有 runtime 的本地单调 deadline，且**复用现有 runtime，
  不创建第二个 Guard**；若 deadline 已过期 `InstallGrant` 会 Fence 并拒绝
  （fail-closed）。
- **F4（hash 进精确比较）**：`authorityGenerationEqual` 增加 `HashVersion`/
  `HashSeed` 相等比较；`FenceIfMatches`/`RemoveIfMatches` 因此对 hash 不同的
  expected 精确拒绝；`generationStrictlyNewer` 保持 hash 相等要求、不把 hash
  当数值排序。
- 新增 `LeaseGuardRegistry.SnapshotAll()`（只读，供一致性验证与未来 Scheduler）。

C2 新增测试：

- `TestSameGenerationInstallRefreshesLocalDeadline`（adapter：第二次同 gen
  Install 后本地 deadline 延至 t1+25s，t0+27s 仍 admitted）。
- `TestSameGenerationRefreshDoesNotCreateSecondGuard`（两次同 gen Install 后
  registry 仅 1 个 guard）。
- `TestSameGenerationRefreshRequiresExactFence`（同 epoch 不同 hash 的 Install
  被拒绝、guard 保留原 hash）。
- `TestFenceDoesNotMatchDifferentHashVersion` / `TestRemoveDoesNotMatchDifferentHashSeed`
  （不同 hash 的 Fence/Remove 拒绝，guard 保持 admitted）。
- `TestSameEpochButDifferentHashIsNotSameGeneration`（`authorityGenerationEqual`
  含 hash）。
- `TestMembershipAdapterConcurrentConsistency`（并发 Observe/Reconcile 结束后
  `registry.SnapshotAll()` admitted guards 与 `reconciler.CurrentInstalledMembership()`
  完全一致）。

## 完整 FIX 验收（全部真实 Exit 0）

```text
go test ./internal/authoritycomposition/... -count=1       PASS
go test -race ./internal/authoritycomposition/... -count=1 PASS
go test ./internal/logic/actor/authority/... -count=1      PASS
go test -buildvcs=false ./... -count=1                     PASS
go build -buildvcs=false ./cmd/...                         PASS
git diff --check                                            PASS
```
