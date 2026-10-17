# RT1B-1 ZERO-SHARD-READINESS Handoff

- 分支：`codex/rt1b-1-zero-shard-readiness-20260806`
- base commit：`935b51e`（integration HEAD）
- 任务 ID：RT1B-1-ZERO-SHARD-READINESS
- worktree：`farm-game-rt1b-1-zero-shard-readiness-20260806`

## 目标

允许 Authority Holder 以 **0 个本地 Shard** 启动，并将 **ProcessReady** 与
**ShardReady** 彻底分离。

权威输入：

- [27] Route/Holder/Shard Lease：Holder 无法证明 Shard Lease 安全窗口时必须
  Fail Closed；Pod 扩缩容/滚动发布不得产生双写；RouteManager 在新 Holder
  Ready 后才发布 Assignment。
- [30] ShardWriteAuthority 与 WriterEpoch：WriterEpoch 换代由存储侧 Fence，
  数据面不逐请求查询 Lease Authority。
- [31] AuthorityPersistenceBucket/Lease：Holder 迁移不搬数据，路由表不是
  Actor 明细表（不批量加载）。
- Shard 迁移与 Pod 扩缩容实施计划：0 Shard 是合法拓扑（缩容到零的中间态、
  新 Pod 先 Ready 再领 Shard）。

## 变更集（owned_write_set）

- `internal/authoritycomposition/composition.go`
- `internal/authoritycomposition/composition_test.go`
- `docs/verification/rt1b-1-zero-shard-handoff.md`（本文档）

未触碰 `forbidden_write_set`：`cmd/**`、`api/**`、`internal/{player,farm}/actor|ingress/**`、
`internal/logic/downlink/**`、`classic-farm-h5/**`、Redis Lua/KeyBuilder。

## 语义设计：ProcessReady 与 ShardReady 分离

### ProcessReady（`Runtime.ProcessReady()` / `Runtime.Ready()`）

只表达 **公共依赖健康**：

- Redis-backed seams（脚本加载、authority store、动态 route provider）构造成功；
- 控制面 applier 与租约续期循环已启动且未发生致命失败；
- `ready` 原子标志在 applier 或 renewal 循环失败时拉低，永不自我复活。

它 **不再** 要求 `len(leases) > 0`，也 **不** 逐 lease 检查
`CanStartActorTurn()`。零 Shard Holder 启动后 `ProcessReady() == true`。

`Ready()` 保留为现有 cmd readiness probe 的别名（签名不变），语义与
`ProcessReady()` 完全一致。

### ShardReady（`Runtime.CanServeShard(domain, shard)`）

独立表达 **单 Shard 准入**：

- 该 `domain/shard` 必须已本地安装 HolderLeaseGuard；
- guard 快照 `Admitted == true`；
- guard 的 Authority generation `State == ShardAuthorityActive`。

零 Shard Holder 对任意 shard 均返回 `false`。

### ServingShardCount（`Runtime.ServingShardCount()`）

可观测的本地可服务 Shard 计数：遍历已安装的 lease runtime，统计
`CanStartActorTurn()` 为 true 的数量。零 Shard Holder 为 `0`；单 Shard 激活
后为 `1`；本地 lease 过期后回到 `0`。

## 必须实现清单逐项对照

1. **0 Shard 不再是启动失败**：`New()` 不再在 `len(runtime.leases) == 0` 时
   报错；`installLocalGrants` 不再在 `localRoutes == 0` 时报错。本 Holder 在
   静态快照中无任何 Shard 时，安装 0 个 guard 后正常进入 ready。
2. **ProcessReady 只表达公共依赖健康**：`Ready()` 不再检查本地 lease，仅由
   `ready` 原子标志（公共依赖）决定。
3. **CanServeShard 独立表达单 Shard 准入**：新增 `CanServeShard`。
4. **ServingShardCount 可观测**：新增 `ServingShardCount`。
5. **静态 Assignment 兼容**：静态快照仍完整加载/安装（provider/resolver/
   applier 全部 Bootstrap）；零 Shard 只是本地 guard 数为 0，Assignment 仍
   可被观测（`GlobalRevision`、Holders 完整）。
6. **未安装 Shard 的 Actor 请求 Fail Closed**：对未安装 shard，
   `DynamicFenceSource.WriteFence` 返回 `ErrLeaseAdmission`；由此
   `Provider.WriterEpoch` 报错、`Provider.Claim` 返回 `ClaimFenced`；
   `ParticipantRouteFence.VerifyRoute` 同样拒绝。零 Shard 测试覆盖三层接缝。
7. **不批量加载 Actor**：无任何批量加载/预加载逻辑；未安装 Shard 的请求在
   fence/epoch/claim 层直接拒绝，不触发 LoadAndClaim。

## 测试

新增/调整（`internal/authoritycomposition/composition_test.go`）：

- `TestZeroShardHolderStartsProcessReadyAndFailsClosedForEveryShard`：零 Shard
  启动成功、ProcessReady/Ready 为 true、ServingShardCount=0、任意 shard
  CanServeShard=false、静态 Assignment 已安装、fence/epoch/claim 三层
  Fail Closed、participant route fence 可构造。
- `TestZeroShardDynamicAssignmentStillFailsClosedBeforeGrant`：控制面发布更新
  快照后（Revision 2→3），零 Shard Holder 保持 ProcessReady、ServingShardCount
  仍为 0、未授 shard 仍 Fail Closed。
- `TestRedisProcessCompositionRejectsStaleLeaseAndBootAndAcceptsNewWriterGeneration`：
  本地 lease 过期后改为断言 `CanServeShard`/`ServingShardCount`，并新增断言
  `ProcessReady` 在单 shard lease 过期后仍为 true（公共依赖健康不受单 shard
  影响）。
- `TestRedisProcessCompositionActivatesAuthorityLoadsCommitsAndReloads`：新增
  `CanServeShard` 与 `ServingShardCount == 1` 断言。

## 验收

```text
go test ./internal/authoritycomposition/... -count=1           PASS
go test -race ./internal/authoritycomposition/... -count=1     PASS
go test ./internal/logic/actor/authority/... -count=1          PASS
git diff --check                                                PASS
go build -buildvcs=false ./cmd/...                              PASS（Ready 语义变更未破坏 cmd probe）
go test -buildvcs=false ./... -count=1                          PASS（全仓无回归）
```

## F1 修复：Local Holder 注册门（RT1B-1-F1-HOLDER-REGISTRATION）

独立 Review Finding（F1）：静态 Assignment 启动时，"本地 Holder 已登记但无
Route"与"本地 Holder 根本不存在于 Snapshot.Holders"都会静默进入 Ready +
0 Shard，配置错误（HOLDER_POD 未注册/拼写错误）不可观测。

冻结修复语义：

1. **registered holder + zero route** → 允许启动，`ProcessReady=true`，
   `ServingShardCount=0`（合法 0-Shard 拓扑）。
2. **local holder absent from Snapshot.Holders** → 静态模式启动失败，返回
   明确 `ErrLocalHolderNotRegistered`，**不创建可运行的 renewal/applier
   残留**（校验在 `New()` 的 snapshot 加载后、goroutine 启动前完成）。
3. **不修改 `LoadStaticAssignmentProvider`** 的通用 Snapshot 解析职责；本地
   Holder 身份校验放在 `authoritycomposition` 启动层
   （`New()` → `holderRegisteredInSnapshot`）。
4. 已有 Boot mismatch、非 ACTIVE Route、未安装 Shard Fail Closed 全部保持。

变更（`internal/authoritycomposition/composition.go`）：

- 新增哨兵错误 `ErrLocalHolderNotRegistered`。
- 新增 `holderRegisteredInSnapshot`（仅检查 Holder 是否在 `Snapshot.Holders`，
  不评估路由归属）。
- `New()` 在 `GetSnapshot` 后立即校验并失败关闭；错误路径早于
  `runtime.wg.Add(2)`/`go runApplier`/`go runRenewal`，无后台残留。

F1 新增测试（`internal/authoritycomposition/composition_test.go`）：

- `TestLocalHolderMustBeRegisteredInSnapshot`：注册门正反断言 + 3 个失败
  子测试（missing local holder / typo holder id / other holders registered
  but local holder missing），均返回 `ErrLocalHolderNotRegistered`。
- `TestLocalHolderRegistrationFailureLeavesNoRunningResidue`：连续 5 次失败
  启动后 goroutine 数不增长（`runtime.NumGoroutine`，容差 +2）。
- `TestLocalHolderBootAndNonActiveProtectionRemains`：registered holder 的
  Boot mismatch 与非 ACTIVE 本地 Route 仍启动失败。
- `TestRegisteredHolderWithZeroRoutesSucceeds`：registered holder + zero
  route 启动成功，`ProcessReady=true`、`ServingShardCount=0`、
  `CanServeShard=false`。

F1 关闭证据：

```text
go test ./internal/authoritycomposition/... -count=1   PASS（含全部 F1 测试）
go test -race ./internal/authoritycomposition/...      PASS
git diff --check                                        PASS
```

## 边界与停止条件

- 未修改 Farm/Player Registry、Proto、cmd composition root、Redis Lua/KeyBuilder。
- 零 Shard 拓扑下，`runApplier`/`runRenewal` 正常空转；`Close()` 对空 lease
  集合安全。
- 保留的正确性保护：若本 Holder 在快照中 **有** Shard 但 Boot 不匹配或
  route/holder 非 ACTIVE，启动仍失败（这是防猜写权限的保护，不属于零 Shard
  场景）；若本 Holder 不在 `Snapshot.Holders`，启动以
  `ErrLocalHolderNotRegistered` 失败关闭（F1）。

## 后续项（不在本任务范围）

- 动态 Assignment 变化后新 Shard 的本地 guard 安装（当前 guard 仅在静态
  snapshot 引导时安装；动态领取需后续任务接续）。
- 缩容路径中 Shard 从本 Holder 移除后的 guard 撤销/观察（`LeaseGuardRegistry`
  已具备 `Observe`/`Remove` 原语，接线留给迁移任务）。
