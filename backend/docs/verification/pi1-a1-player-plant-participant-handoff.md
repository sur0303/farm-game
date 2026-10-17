# PI1-A1 Player Plant Participant Handoff

## 交付身份

- `task_id`: `PI1-A1-PLAYER-PLANT-PARTICIPANT`
- `base`: `3e715a481ca62091e65b2c36977b13fef1461e76`
- `branch`: `codex/pi1-a1-player-plant-participant-20260804`
- `integration owner`: `I0`
- 本轮进入修复时远端候选 tip：`f04920e2a3e49e6569d62fee288067962514071e`
- 本轮代码修复 commit：`64896b6` (`fix: close participant authority construction gaps`)
- 本轮 handoff commits：`3bff6c6` (`docs: record PI1-A1 fifth review closure`) 及本次
  finalization docs commit；最终 remote tip 以 push 后核验为准。
- 未创建 MR，未合入 integration；最终代码通过同一 feature branch fast-forward push。

## 2026-08-04 PI1_SOL_V10：Commit 旧 model 完整性闭合

本次最小修复从 `307e9907979e6451f4ef114740a76f537a8613eb` 开始，仅修改 Redis
authority 的版本化 Commit Lua、golden 与定向回归测试。V9 的 Load V2 identity 保持不变；
Commit 源码按仓库 immutable-script 约定升级为
`CommitActorMutationWithOrdersV3=74325cd2392c571813b8c22a21ec4f2b31f7b775`，避免与 V2 混用。

Commit V3 在 replay/version/capacity/order 判定及线性化写入前，对现有
`model_manifest/model_field_count/model_total_bytes` 做配置上限内的 LP 解析，逐项验证名称
合法、唯一、对应字段存在，并重算字段名与 value 的精确字节数；receipt manifest/count/bytes
继续逐项重算并验证 receipt envelope。最终 `HLEN` 使用已验证的 model/receipt manifest 数量，
不再使用可单独篡改的 model count。任何不一致统一返回 `MALFORMED_RECORD`，不会创建 receipt/order
或改变 RecordVersion/model。脚本只使用 bounded `HMGET/HGET/HLEN`，无 `HGETALL/HSCAN/SCAN/KEYS`。

新增回归覆盖 rogue receipt + 伪增 model count 的联动损坏、model manifest 缺字段、model bytes
伪造、manifest/count 不一致、非法/重复 model 名称，以及 Commit Lua 无无界扫描命令；V9 的精确
receipt 边界测试继续证明合法连续提交成功且超一字节整次拒绝。

## 2026-08-04 PI1_SOL_V9：exact Redis usage / corrupt-counter / stale-binding closure

本节记录从精确 starting tip
`09c77375756976274731e353f09466ccfbd1f1b2` 开始的窄范围 V9 corrective pass。工作仍在
`codex/pi1-a1-player-plant-participant-20260804`；未 merge/rebase integration，未创建 MR，
未修改 production composition。本节更新 V8 中“Redis commit 后可用 projection 推进
trusted usage”及“不修改 Lua”的旧结论；若与 V8 文字冲突，以本节为准。

### 三个 confirmed P1 的闭合

- **P1-1 exact post-commit usage**：`CommitActorMutationWithOrdersV2` 在同一 Lua 线性化点
  返回 `RecordVersion/model_bytes/receipt_count/receipt_bytes`；Redis `CommitCommitted`
  snapshot 直接携带该 version 的 exact `PhysicalUsage`。`Binding` 不再把
  `ProjectPhysicalCommit` 的 status/timestamp 保守预留值累加成 trusted aggregate，
  因此 actor turn 没有新增 commit 后 Redis read/network wait。MemoryStore 仍从同一
  mutex linearization point 返回 exact snapshot，Memory/Redis 成功 commit 语义保持对齐。
- **P1-2 corrupt counters fail closed**：Actor record schema 升为 `SchemaVersion=2`，新增
  bounded `_meta:receipt_manifest`；`LoadAndClaimActorV2` 在改动 claim 前验证必需
  metadata、schema/layout、正整数/非负上限、zero-count/zero-byte 交叉不变式、
  model/receipt manifest count/duplicate/field existence/actual bytes，以及根据有界 model、receipt、
  resolution count 得到的 exact hash field count。Go bounded snapshot read 仅对 manifest 列出的
  model/receipt fields 执行 pipelined `HMGET + HLEN`，重算 model digest/bytes，解码每个
  receipt 并校验其 effective idempotency digest 与 manifest 一致；没有 `HGETALL`。
  Commit V2 在新 receipt admission 前也校验当前 manifest/counters/actual receipt bytes/HLEN，
  corrupted low counters 不能绕过 Lua capacity。缺少 V2 physical metadata 的旧记录返回
  malformed/reconcile-required，不会被静默标记 trustworthy。
- **P1-3 stale Binding quarantine/drop**：CAS/idempotency conflict、writer epoch/claim loss、
  Get refresh error/unavailable/unknown、Commit error/unavailable/unknown、ResolveUnknown error/retry/
  writer mismatch/reconcile/fenced 都立即清空 cached `PhysicalUsage`。`PhysicalUsage()` 只在
  usage version 等于 Binding 当前 version 时返回 trustworthy view；只有新的成功
  Load/Commit/Fence snapshot 能恢复。Player Registry 在 Get writer mismatch、Commit writer loss 及
  `ResolutionFenced`/**`ResolutionReconcile`** 都丢弃 binding；`ResolutionRetry` 保留原
  resolver identity 但 binding 仍处于 pending-unknown + untrustworthy-usage 隔离。RecordVersion
  conflict 仍按原有有界 reload/retry；普通 deterministic business rejection 不丢 binding。

Redis script 身份没有静默改写 V1 SHA：新的
`LoadAndClaimActorV2=5469456a657d141673c3bae319c73fec9e3a4bae`，
`CommitActorMutationWithOrdersV2=ff058a5d6fc7bf653714819124eb562771a6c2c5`，并由
`TestScriptSHAGoldens` 锁定。

### V9 直接回归证据

- `TestCommitReturnsExactPhysicalUsageAndNearReceiptBoundary`：首次 Redis commit reply 的 receipt
  bytes 与实际 `receipt:op:<digest>` value 长度完全相等，且 projection 确实更大；
  第二次设定的 limit 使“exact first + projected next”恰好可进，而“projected
  first + projected next”会误拒，结果新路径成功 commit。独立子测试将 limit
  设为“exact first + exact next - 1 byte”，Lua 稳定拒绝且 version/usage 不变。
- `TestLoadRejectsMalformedOrInconsistentReceiptCounters`：覆盖 missing manifest、malformed
  count、zero count + nonzero bytes、receipt field + zero counters；Load 与后续 Commit 都 fail
  closed，无 capacity bypass。
- `TestLoadRejectsReceiptManifestAndAggregateMismatch`：覆盖 manifest 指向缺失
  receipt、receipt bytes 低报、已存在 receipt 被 zero counters/empty manifest 隐藏；
  Load/Commit 均拒绝。
- `TestBindingInvalidatesPhysicalUsageOnCASConflictUntilFreshLoad` 与
  `TestAuthorityPlayerCASConflictQuarantinesCapacityUntilFreshLoad`：CAS conflict 后 direct usage 及
  Participant capacity guard 均 fail closed；成功 fresh load 后才恢复；普通 business rejection
  保留 deterministic binding/usage。
- `TestBindingGetWriterMismatchInvalidatesUsageUntilFreshActivation`：Get writer epoch mismatch 后
  旧 binding usage 失效，新 Holder/fence activation + load 后才重建 exact usage。
- `TestBindingResolveUnknownWriterClaimMismatchAndReconcileInvalidateUsage` 覆盖第三方
  writer-claim mismatch 及持久 resolution receipt parameter reconcile；
  `TestAuthorityPlayerResolveWriterClaimMismatchDropsBinding` 直接证明 Registry 对
  `ResolutionReconcile` 也丢弃 binding，不只处理 fenced enum。

### V9 必选验证证据

在最终代码与本 handoff 收口前执行（为避免环境中只读默认 Go cache，
统一使用 `GOCACHE=/tmp/pi1-v9-go-cache`）：

```text
gofmt -w <all changed Go files>
PASS

go test -count=1 ./internal/storage/authoritystore \
  ./internal/storage/redis/authority ./internal/logic/actor/authority \
  ./internal/player/actor ./internal/player/participant ./internal/player/ingress
PASS

go test -race -count=1 ./internal/storage/authoritystore \
  ./internal/storage/redis/authority ./internal/logic/actor/authority \
  ./internal/player/actor
PASS

go test -count=1 ./...
PASS

go vet ./...
PASS: no diagnostics

go build ./cmd/...
PASS: no diagnostics

./scripts/gen-proto.sh
PASS
before api/gen+api/proto content-tree SHA-256:
e242a0e1224dba6f3465a1cf79cd8aae84496d2a89192a205a56ddc8dc93997f
after api/gen+api/proto content-tree SHA-256:
e242a0e1224dba6f3465a1cf79cd8aae84496d2a89192a205a56ddc8dc93997f
git diff --exit-code -- api/gen api/proto
PASS: empty

git diff --check
PASS
```

### V9 剩余 platform-only gaps

- **真实 Redis Cluster / MOVED-ASK / failover：未执行。** Redis adapter 证据来自
  miniredis deterministic tests；它们证明 Lua/adapter 语义，不冒充真实 Cluster 网络、
  slot migration、replication/failover 或 platform latency 证据。
- **真实 mTLS / service identity / production deployment composition：未执行。** 本轮没有
  修改 transport、cmd composition、route-manager/lease wiring；需由 platform/integration owner 在真实
  环境验证。
- 除上述真实 Cluster/mTLS/deployment probes 外，本轮要求的代码、单测、race、
  full suite、vet、build、proto-idempotence 和 diff 证据无剩余未闭合项。

V9 最终 commit SHA 及 push 后 remote tip 由交付输出记录，文档不自引用其自身
commit SHA。

## 2026-08-04 PI1_SOL_V8：outer Authority physical-usage P1 closure

本节记录从精确 starting tip
`fa7a502dc025a267484ef83c47da3ca9b332b35c` 开始的 V8 focused corrective；integration
base 仍为 `codex/integration-20260803` 的
`58f73cbfe374c26e63f06b05bb9d5f97a951b562`，未 merge/rebase integration。若本节与下文
早期“用 PlayerRecord receipt carrier 推算 Store receipt capacity”的描述冲突，以本节为准。
最终 commit SHA 及 push 后 remote tip 由交付输出记录，文档不自引用其自身 commit SHA。

### P1 根因与实现闭合

旧 admission 把 `PlayerRecord.RecentOperationReceipts` 的 nested item/byte usage 与
Authority Store 的 `MaxReceiptItems`/`MaxReceiptBytes` 比较；但这两个 Store limit 实际约束
outer Authority receipt collection。历史/其他 outer receipt 不在 PlayerRecord carrier 中，
所以旧检查可通过，最后才在 `AuthorityStore.Commit` 返回 capacity failure。

V8 将两个物理域明确分离：

- `AuthoritySnapshot.PhysicalUsage` 携带 `RecordVersion + model bytes + outer receipt items +
  outer receipt bytes + trustworthy bit`。MemoryStore 从受 mutex 保护的 concrete record 生成；
  Redis Store 从既有 `_meta:model_total_bytes/_meta:receipt_count/_meta:receipt_bytes` 经 bounded
  `HMGET` 读取，并核对 manifest 实际 model bytes。没有新增 Redis key，也没有修改 Lua。
- `PhysicalCommitUsageProjector` 由同一个 concrete Store 实现，按其真实 Commit envelope
  计算下一次完整 replacement model、outer receipt item/bytes、single receipt/result envelope
  和 `MaxCommitBytes` charge。`FaultStore` 只透传 delegate；缺 projector 的 Authority Registry
  不能构造 Participant capacity capability。
- Authority `Binding` 保存 version-bound usage；每次 Load/Claim/Fence snapshot 都刷新。确认
  `COMMITTED` 后按 concrete projection advance 到新 version；`ALREADY_COMMITTED` 只同步 Store
  snapshot，不重复 `+1`，因此 replay 不消费 capacity。
- receipt-free admission 先在 mailbox 外构造 detached nested PlayerRecord worst-case candidate，
  单独检查 1024-entry carrier/result proof/model shape；dependency proof 后用当前 Binding outer
  usage + exact next outer contribution 检查 Store 五个 limit。若期间另一个合法本地 commit
  只使 admission record version 过期，允许一次 fresh receipt/record refresh；Store usage 本身
  missing/stale/untrustworthy 时仍稳定 fail closed。
- Player mailbox 使用 `SubmitParticipantVersioned` 在同一次 fresh Authority load 后提供不可伪造
  的 `ParticipantCapacityGuard`。guard 在 retention mutation、Plant decision、资产/Reservation
  mutation 和 receipt append 之前重做 nested + outer 检查；Store Commit 保持最终原子 authority。
- replay 仍是 durable receipt-first：mailbox 外命中直接返回；并发窗口内在 mailbox 命中也以
  `changed=false` 返回。capacity 已满时 exact replay 仍成功，outer usage/version 均不增加。

所有 missing/stale/exhausted physical view 都沿既有稳定边界返回 gRPC
`ResourceExhausted` + `ERR_RATE_LIMITED` typed `ParticipantFailure`，不在业务 mutation 后才暴露
raw Store error。未修改 public Proto/generated contract、route、Redis key/Lua、Farm/Friend/
Workflow/cmd composition，也未开始 FI1。

### V8 直接回归证据

- `TestPlantOuterReceiptUsageRejectsSmallNestedCarrierBeforeMailbox`：Memory Authority outer receipt
  已等于 `MaxReceipts`，nested carrier 为 0；下一次 Plant 在 mailbox/decision 前稳定拒绝，
  record/version/asset/reservation/receipt 均不变。
- `TestPlantSuccessfulCommitAdvancesUsageAndFullReplayDoesNotConsumeAgain`：首次 terminal commit 后
  不 reactivation、不 reload 的 Binding guard 已看到 full usage；同 invocation exact replay
  成功且 outer usage/version 不变；下一新 invocation 不进 mailbox。
- `TestPlantPhysicalUsageUnavailableOrStaleFailsClosed`：usage missing 与 usage-version stale 都在
  mailbox 外以稳定 typed capacity error fail closed。
- `TestPlantInMailboxPhysicalUsageRefreshFailsClosedBeforeMutation`：mailbox 外 snapshot 可信，
  mailbox fresh load 被注入 missing usage；恰好进入一个 turn，但 guard 在任何 PlayerRecord
  mutation 前拒绝。
- `TestAuthorityParticipantFailsClosedWithoutConcreteStoreProjector`：只有 limits、没有 concrete
  commit projector 的 Store 不能获得 Participant admission capability。
- `TestMemoryStoreLoadAndClaimIsIdempotentAndFencesOldClaim`、
  `TestMemoryStoreCommitChecksReceiptBeforeRecordVersion` 与 Redis
  `TestCommitAcceptsFriendPairRootOnFarmActor` 直接断言 load/commit/replay physical usage 与 concrete
  projection。
- 原有 PI1 proof/tamper/collision/timeout/restart/concurrency suites 全部随下列 package/full/race
  matrix 执行并通过，包括 `TestPlantDurableResultProof*`、
  `TestPlantTimeoutUnknownResolutionsAndWriterFence`、`TestPlantHappyReplayAndRealRecordVersion`、
  `TestPlantCommitReleaseConcurrentSingleTransition`。

### V8 最终验证原始结论

在最终代码与 handoff 内容上执行：

```text
go test ./internal/player/... ./internal/logic/actor/authority/... \
  ./internal/storage/authoritystore/... ./internal/storage/redis/authority/...
PASS

go test ./...
PASS

go test -race ./internal/player/... ./internal/logic/actor/authority/... \
  ./internal/storage/authoritystore/...
PASS

go vet ./...
PASS: no diagnostics

go build ./cmd/...
PASS: no diagnostics

./scripts/gen-proto.sh
PASS
git diff --exit-code -- api/proto api/gen
PASS: generation added no diff
git diff --check
PASS
```

V8 intended write set：

- `internal/contracts/participant.go`；
- `internal/logic/actor/authority/authority.go`；
- `internal/player/actor/registry.go`, `registry_participant_test.go`；
- `internal/player/ingress/service.go`, `service_test.go`；
- `internal/player/participant/plant.go`, `server.go`, `plant_test.go`；
- `internal/storage/authoritystore/types.go`, `memory.go`, `fault.go`, `store_test.go`；
- `internal/storage/redis/authority/store.go`, `store_test.go`；
- 本 handoff。

## 2026-08-04 corrective closure：durable result proof 与 Store-bound admission

本节是从 immutable starting tip
`2589b2ebe6f3fadcfd82fb8c75074ef77679d282` 开始的最终纠正记录；若与下文早期
“provider profile”描述冲突，以本节为准。纠正实现 immutable commit 为
`0b697059adcd34dc090963d1b2a899d5fa0dc946`（`fix: bind participant proofs and store limits`）。
最终 handoff commit 是该实现 commit 的 documentation child；最终远端 branch tip 由
交付后的 `git rev-parse HEAD`/`git ls-remote` 记录，不在 commit 内容中自引用其自身 SHA。

### P1-A：Participant result proof 已完整耐久绑定

`PlayerOperationReceipt.result_payload` 不再保存无法证明 committed result hash 的裸
typed protobuf。新写入使用内部 `participantResultProofV1` 二进制 envelope：

```text
magic + version
+ ParticipantCommandType
+ exact participant_invocation_id
+ CanonicalStepPayloadHash
+ canonical ParticipantResultHash
+ deterministic typed-result length/bytes
```

envelope 与 namespaced Participant receipt、外层 `payload_hash/status/error`、Player
业务变更一起进入同一个 PlayerRecord Authority Commit。首次提交在 append receipt 前
计算 canonical result hash；`StepOutcome.CommittedResultHash` 只携带该 committed 值。
Server 会重算 canonical hash 做完整性校验，但 response 返回的是已验证的 committed hash，
不再把 replay-time payload 的自哈希冒充持久证明。

Replay 严格检查 magic/version、command、完整 invocation identity、外层/内层 step hash、
长度、typed schema、无 unknown fields、deterministic re-marshal bytes 及 canonical result
hash。任一 result hash/typed result/envelope 不一致按 durable corruption 以 gRPC
`Internal` fail closed；相同 invocation + 不同 request hash 仍先按冻结语义返回
`PARTICIPANT_IDEMPOTENCY_CONFLICT`。旧 namespaced receipt 中的裸 typed bytes 没有
committed result-hash proof，明确按 legacy-unproven data fail closed，不静默升级或背书。

直接证据：

- `TestPlantDurableResultProofFirstCommitReplayCollisionAndReload`：首次 envelope/hash、exact
  replay、different payload collision、Registry restart/reload replay；
- `TestPlantDurableResultProofTamperAndLegacyFailClosed`：tampered result hash、tampered valid
  typed result、malformed bytes、legacy raw typed bytes，且 corruption 不进 mailbox；
- `TestPlantHappyReplayAndRealRecordVersion`：Memory/miniredis 首次执行、hot replay、cold
  replay 都返回首次 typed result/hash/version。

### P1-B：Mailbox admission 只取 concrete Authority Store limits

删除 `RegistryOptions.ParticipantCapacityProvider`、`ParticipantCapacityProviderFunc`、
`RegistryOptions.ParticipantCapacity` 和 Participant `Options.ParticipantCapacity`；调用方已
没有独立函数/profile 可以覆盖或放宽 Store。`authority.Provider.PhysicalCapabilities()`
只从其实际 `ActorAuthorityStore` 的 optional read-only
`authoritystore.PhysicalCapabilitiesProvider` 读取 immutable snapshot；缺 capability 时
普通 Actor 路径仍兼容，但 Participant checked construction/admission fail closed。

MemoryStore 与 Redis Authority Store 都实现并实际执行同一维度：model fields/bytes、
receipt items/aggregate bytes、single receipt/result-envelope bytes、serialized commit bytes。
Redis capability 会取配置与既有 hard script-family bound 的较小值；Redis Go adapter 在
Lua 前执行 `MaxCommitBytes`/single receipt 检查，不修改 Lua mutation semantics。Memory
Store 增加同维度 receipt byte accounting。`FaultStore` 只透传 concrete delegate 的
capabilities。Player runtime 再与 frozen PlayerRecord 1024 receipt carrier 上限取较小值。

Participant receipt-free preflight 构造 detached worst-case proof envelope，得到
`receipt items + aggregate receipt bytes + single receipt bytes + model bytes + serialized
commit bytes`；Server 在 dependency proof 后、mailbox admission 前检查，Actor turn 在
新加载 record 上重复检查，Store Commit 仍是最终原子保护。

直接证据：

- `TestAuthorityRegistryCapacityComesOnlyFromConcreteStore`：Registry snapshot 精确来自实际
  MemoryStore，没有 caller override surface；
- `TestAuthorityParticipantFailsClosedWithoutConcreteStoreCapabilities`：optional capability
  缺失时 Participant fail closed；
- `TestPlantStorePhysicalCapacityRejectsBeforeMailboxAdmission`：Memory/miniredis 的真实
  single-envelope 与 MaxCommitBytes 超限都在 mailbox submit 前稳定拒绝；
- `TestParticipantPhysicalCapacityByteBoundaries`：model/aggregate/single/commit exact boundary；
- `TestMemoryStoreCapacityFailureDoesNotPartiallyCommitStateReceiptOrOrders`、Redis
  `TestCapacityFailureDoesNotPartiallyCommit`：Store capabilities 与 MaxCommitBytes 最终保护；
- `TestPlantHappyReplayAndRealRecordVersion`：正常有效 work 仍恰好进入一个 Actor turn。

### Corrective 写集与冻结边界

实现 commit 精确修改：

- `internal/contracts/fake.go`, `internal/contracts/participant.go`；
- `internal/logic/actor/authority/authority.go`；
- `internal/player/actor/registry.go`, `registry_participant_test.go`；
- `internal/player/ingress/service.go`, `service_test.go`；
- `internal/player/participant/plant.go`, `server.go`, 新增 `resultproof.go`, `plant_test.go`；
- `internal/storage/authoritystore/types.go`, `memory.go`, `fault.go`, `store_test.go`；
- `internal/storage/redis/authority/store.go`, `store_test.go`。

没有修改 `api/proto/**`、`api/gen/**`、Redis key layout、Lua 源码/原子 mutation 顺序、
routing、Farm/Friend/Workflow/cmd composition；没有扩展 RT1/SO1/FI1，没有 ICR blocker。

### Corrective 验证与剩余平台证据

编码中 focused 与 adjacent regression 结论：

```text
GOCACHE=/tmp/pi1-corrective-gocache go test -count=1 \
  ./internal/player/participant ./internal/player/ingress ./internal/player/actor \
  ./internal/storage/authoritystore ./internal/storage/redis/authority \
  ./internal/logic/actor/authority
PASS

GOCACHE=/tmp/pi1-corrective-gocache go test -race -count=1 \
  ./internal/player/participant ./internal/player/ingress ./internal/player/actor \
  ./internal/storage/authoritystore ./internal/storage/redis/authority \
  ./internal/logic/actor/authority
PASS

GOCACHE=/tmp/pi1-corrective-gocache go test -count=1 ./...
PASS
```

最终 handoff candidate 再执行用户要求的完整 matrix：`go test ./...`、
`go test -race ./...`、`go vet ./...`、`go build -buildvcs=false ./...`、
`scripts/gen-proto.sh` + proto/generated zero diff、`git diff --check`；全部 PASS 后才提交/推送
本 handoff。

剩余 platform evidence 未被本纠正伪装为已完成：真实 Redis Cluster、真实 mTLS/service
identity 仍未执行；真实 WorkRepository/Farm proof production composition 仍由 I0 接线。
本轮 Memory/miniredis 证据覆盖 deterministic correctness、reload/tamper/capacity，并不等价于
production Redis Cluster probe。

源 worktree 是一个 parent Git metadata 只读的 linked worktree；提交和 push
使用同一源树的 `/tmp` writable delivery worktree 完成。delivery worktree 的
最终 `git status --porcelain` 为空；源 linked worktree 的索引不能由本会话写入，
这一点不冒充为源索引已 clean。

## 编码前审计：复用 / 缺口 / 不改矩阵

| 审计区 | 复用 | 缺口及本轮处理 | 不改 |
| --- | --- | --- | --- |
| `api/proto/contracts/participant.proto`, `steps.proto` | Frozen caller oneof、route header、Plant payload/result、response version | 无 wire 变更；所有新校验在现有 IDL 之上 fail closed | 不改 proto、生成代码、field number、枚举 |
| `internal/contracts/catalog.go`、`fake.go` | 唯一 `ParticipantCommandCatalog`、Fake API、typed schema/replay seam | Fake 与生产 Server 使用同一 ingress kernel；Fake handler context 也 bounded，并保留 exact handler version/result/hash | 不复制 player/farm catalog，不改命令/WorkType/Step/Schema |
| `internal/identity/**` | `farm-workflow-id-v2`、root-scoped `ParticipantInvocationID`、`SeedReservationID`/`CropCycleID` | Plant 继续由 WorkID 派生 identity；没有新 `legacy workidentity.WorkflowID` | 不改 identity vector；不以 legacy WorkflowID 创建新 Workflow |
| `internal/player/actor/**` | 既有 PlayerActor mailbox/turn、legacy Registry、`NewAuthorityRegistry`、PlayerRecord receipt carrier | 增加 Authority-only `ReadOutsideMailbox`、legacy fail-closed gate、显式 `ParticipantCapacityProvider`、Binding 并发保护、exact committed version 接缝保持不变 | 不改 FarmActor；不双写 legacy/Authority |
| `internal/player/ingress/participant.go`、`service.go` | reservation/item/state helper、`RecentOperationReceipts` 物理载体 | Participant receipt 只用显式内部 namespace；移除 raw/typed structural classifier；增加 receipt item/byte preflight | 不加第二 receipt store；不改 public receipt 语义 |
| `internal/player/participant/**` | 一个 Coordinator/Worker Server、一个 Plant Handler | receipt-first mailbox-free replay、Worker 双 verifier、bounded Handler context、capacity stable mapping、Plant three steps | 不实现 Farm CommitPlant；不改 cmd composition |
| `internal/logic/actor/runtime/executor.go` | 既有 keyed mailbox、dequeue cancellation check | 在 `SubmitUserSized` admission 线性化点增加 ctx check，使已取消任务不入队；只为本轮 P1 admission race 的最小运行时接缝 | 不改 actor 调度优先级/容量语义 |

### 冻结接口边界

本轮没有修改冻结 IDL、Catalog、AuthorityStore/Redis 公共接口、Redis key/Lua
脚本、Farm/Friend/Workflow/cmd。Authority Participant capacity profile 不再由调用方
手填值作为来源：`NewAuthorityRegistry` 要求实际 Authority adapter 提供显式只读
`ParticipantCapacityProvider`；缺失、不完整或与调用方断言不一致时 profile unknown，
Participant 构造/预检 fail closed，权威提交仍是最后原子边界。真实 production
composition 注入该 provider 仍由 I0 完成，见 Pending。

没有 blocking ICR。若以后要求从冻结 `PlayerOperationReceipt` 的历史 raw
`operation_id` 推断 Participant kind，因 frozen proto 没有 kind/namespace 字段，
该要求无法安全实现；本实现选择显式 namespace + 对历史 ambiguous raw entry
fail closed，不把 public receipt 猜成 Participant。

## 本轮四个 P1 的闭合

### P1-1：receipt 不进 mailbox

`plantHandler.ResolveParticipantReceipt` 使用 `Registry.ReadOutsideMailbox`，不再
调用 `Registry.Read`/executor。legacy Registry 不再尝试 fence-then-Get，而是在构造
和 read preflight 直接 fail closed；只有 Authority `LoadAndClaim` 的原子、epoch-aware
路径可用于生产 Participant。Authority 路径用同一 Binding 的 fence/claim/version
读取，并由 Registry 的 `authorityMu` 保护 Binding 可变 snapshot/pending 状态。读取返回真实 record version；typed receipt 的
原始 `PlayerRecordVersion` 必须非零且不领先当前 record version。找到相同 invocation
和 canonical hash 时在 WorkVersion/Lease 前返回首次 typed result/version/hash；不同
hash 返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`。PlayerActor callback 只在本地已加载
record 上重查 receipt、做串行裁决和一次 versioned authority commit，不调用
Participant receipt resolver/Redis。

直接证据：`TestReadOutsideMailboxDoesNotSubmitLegacyTurn`、
`TestReadOutsideMailboxDoesNotSubmitAuthorityTurn`、
`TestLegacyReceiptReadNeverRunsFenceThenGetTOCTOU`、
`TestLegacyRegistryCannotConstructCheckedParticipant`、
`TestPlantTerminalReceiptPrecedesStaleWorkAndLease`（replay submit 计数不增加）、
hot-eviction/rebuild matrix。

### P1-2：admission/enqueue deadline race

共享 kernel 仍取 request budget、context deadline、Participant command maximum 的
最小值，并从 decode/verifier/receipt lookup elapsed time 中扣减。依赖 verifier、
receipt resolver 后和 Handler 前均再次检查 budget；Handler、verifier 和 Actor
submit 都拿 bounded context。`Executor.SubmitUserSized` 在 mailbox lookup 后、
channel send 同一 mailbox lock 下再次检查 `ctx.Err()`，dequeue 前原有检查保留；
因此 admission 后、enqueue 前已取消的任务不占 UserMailbox，也不会进入 handler。

直接证据：`TestSubmitRejectsCancellationAtEnqueueBoundary`、
`TestParticipantBudgetUsesCommandMaximumAndVerifierElapsedTime`、
`TestPlantDependencyAdmissionAndFinalBudgetAreBeforeCommit`，以及 focused race。

### P1-3：Worker ResumeStep 独立 WorkVersion

`ResumeStep` receipt-first 后先执行可注入 `WorkLeaseVerifier`，再独立执行同一
`WorkVersionVerifier`；两者都在 mailbox 前、bounded context 内。有效 lease 不能
证明 expected WorkVersion 当前，陈旧 version 直接 fail closed，不进 Handler、不写
receipt、不改 inventory。terminal same-hash replay 仍按 P1-1 规则先返回。

直接证据：`TestResumeStepRejectsStaleWorkVersionWithValidLeaseBeforeMailbox`、
`TestPlantTerminalReceiptPrecedesStaleWorkAndLease`。

### P1-4：删除 legacy structural classifier

新 Participant `operation_id` 使用显式不可与 printable public opaque ASCII 混淆的
内部 key：`\x00PI1-A1-PARTICIPANT-RECEIPT-V1\x00` + invocation id。`FindParticipant`
和 compaction 只认该 namespace；不再使用 `sid1_`、raw operation id、32-byte hash、
Plant-like result 或 PlayerRecordVersion 猜 kind。Participant invocation/Coordinator
operation ID 的共享 kernel 也限制为合法 printable opaque ASCII。历史 raw entry 无法
可靠分类，按 public data 处理并在 retention/prune 中正常移除，绝不永久 archive。

直接证据：`TestPublicSid1OperationIsNotParticipantArchive`，其中同时构造 exact
invocation/hash/Plant-result collision 并验证 compaction/prune 不误归档。

## 两个本轮 P2 的闭合

### P2-5：resolver 强制与 bounded handler

生产 `Options` 不再暴露可注入 Handler 字段；仅包内测试可用显式 unexported
`testHandler` seam。`NewServerChecked` 拒绝全部 custom Handler 注入，且默认 Plant
handler 必须同时具备 Authority Registry 的 atomic receipt read、真实 capacity
profile 和内建 durable receipt capability。`NewServer` 对不具备 resolver 的 test
handler 也 fail closed；`resolveReceipt` 不再把 resolver 当作可选。所有 terminal
APPLIED/REJECTED/NO_CHANGE outcome 必须携带内部 `DurableReceiptCommitted` proof，
否则 Server 在返回前拒绝，即使恶意 handler 返回了 APPLIED 和看似合法 typed result。
`Server.handle`、所有 verifier、receipt lookup、Fake lead 都使用由 effective remaining
budget 派生的 bounded context，不把原始无界 context 传给生产 Handler。Coordinator/
Worker 共享同一个 Handler，重复 invocation 由 durable resolver 和 Actor receipt
recheck 防止第二次业务变更。

直接证据：`TestParticipantCustomHandlerCannotBypassReceiptResolver`、
`TestParticipantTerminalResultRequiresDurableReceiptProof`、
`TestPlantHappyReplayAndRealRecordVersion`、并发 duplicate/replay tests。

### P2-6：capacity profile、item count、stable error

`ParticipantPhysicalCapacity` 增加 `MaxReceiptItems`；admission 和 Actor callback
都用 detached worst-case model bytes、receipt bytes、retention pruning 后的
`current receipt items+1` 预检，过期 public receipt 只在 detached candidate 中清理，
不修改活跃 Actor。legacy Registry 即使有 conservative profile 也不能作为 Participant
authority；Authority Registry 不再从 `authoritystore.DefaultLimits()` 回填 profile，
必须由构造时实际 adapter 的 `ParticipantCapacityProvider` 提供，缺失、unknown、
不完整或“小 provider + 大手填 profile”均 fail closed。AuthorityStore/Redis/Lua 最后未知的
`ErrCapacityExceeded` 统一在 Participant transport boundary 映射为
gRPC `ResourceExhausted` + `ERR_RATE_LIMITED` typed `ParticipantFailure`；candidate
record、资产和 ParticipantReceipt 不可观察为部分提交。

直接证据：`TestPlantReceiptItemCapacityUsesConfiguredAuthorityProfile`、
`TestAuthorityRegistryParticipantUsesSmallerLimitProfile`、
`TestAuthorityParticipantCapacityMustComeFromProvider`、
`TestParticipantCapacityPreflightPrunesExpiredPublicReceipts`、
`TestParticipantPhysicalCapacityByteBoundaries`、model/receipt byte preflight、
Memory/miniredis authority capacity fault tests。

## 第五轮独立审查收口：2 P1 + 2 P2

1. **Legacy TOCTOU P1**：legacy blobstore 的 mailbox 外 `fence` + `Get` 组合已删除；
   `ReadOutsideMailbox` 对 legacy Registry 返回
   `ErrParticipantReceiptAuthorityUnavailable`，`ParticipantAdmissionError` 和
   `NewServerChecked` 同样 fail closed。Authority 路径仍在 AuthorityStore 的
   `LoadAndClaim` 原子 fence/claim/read 边界内执行，不在 mailbox 或 Actor turn 等待
   Redis。`TestLegacyReceiptReadNeverRunsFenceThenGetTOCTOU` 用 trap store 证明旧
   路径不会在 Holder 变更窗口读取任何新旧 receipt。
2. **任意 custom Handler P1**：production `Options.Handler` 已移除，只有包内
   unexported `testHandler`；checked composition 拒绝 test seam。内建 Plant handler
   的 terminal outcome 只有在同一 Actor authoritative commit 写入 durable receipt
   后设置 proof bit，Server 不信任无 proof 的 APPLIED/REJECTED/NO_CHANGE。
3. **detached retention P2**：capacity estimate 和 item count 都 clone 当前
   `PlayerRecord`，按可信 `nowMs` 执行与 turn 相同 retention pruning，再估算候选；
   live record 不被 admission 改写，turn 重新执行并原子提交。expired/live mixed
   boundary 由 `TestParticipantCapacityPreflightPrunesExpiredPublicReceipts` 覆盖。
4. **provider-backed limits P2**：Authority Registry 只从显式
   `ParticipantCapacityProvider` 读取 profile；调用方 profile 仅可作为相等性断言，
   不能扩大 provider 上限。缺 capability/unknown/mismatch 的 Registry 只能服务
   普通 Actor 读写，Participant checked 构造失败；最终 Authority commit 的未知物理
   上限仍保留原子拒绝和稳定 `ResourceExhausted/ERR_RATE_LIMITED` 兜底。

## ParticipantReceipt 物理映射

P0 使用同一个 durable PlayerRecord carrier，不能依赖进程内 map：

```text
PlayerRecord.recent_operation_receipts
  operation_id   = ParticipantReceiptOperationKey(participant_invocation_id)
  payload_hash   = CanonicalStepPayloadHash
  result_payload = 首次 deterministic typed result bytes
  status/error   = 首次 terminal outcome/error code
  typed result   = 首次真实 Player RecordVersion
```

hot seven-day timestamp eviction 只将 Participant receipt 的 `CommittedAtMs` 置为
0，保留 key/hash/result/version；容量满时不删除 terminal key。相同 invocation +
相同 hash 返回首次结果并 `ReplayedFromReceipt=true`；不同 hash 不进入业务裁决。
TIMEOUT_UNKNOWN 不写 Participant terminal receipt，不伪装 APPLIED；后续按原调用
身份经 Authority resolution/retry。

## Plant 实现边界

仅实现：

- `RESERVE_SEED`：由 `SeedReservationID(work_id)` 派生 reservation，扣一次对应
  seed 并创建 `PREPARED`；
- `COMMIT_SEED_RESERVATION`：只允许匹配的 Farm COMMITTED decision/hash，
  `PREPARED -> COMMITTED`，不再次扣种子；
- `RELEASE_SEED_RESERVATION`：只允许匹配 Farm REJECTED decision/hash 和精确
  `FARM_REJECTED` reason，PREPARED 只返还一次；COMMITTED 禁止释放。

WorkRecord/Farm verifier seam 必须具体绑定 player/farm/plot、crop/seed definition、
quantity、rule version/hash、reservation/crop-cycle、workflow step/version 及
Commit/Release terminal state/hash。缺少 production proof 时拒绝；成功 fixture
使用 concrete mutation-proof verifier，不用 permissive no-op 证明生产路径。其它
Player Participant 命令返回 Unimplemented。

## 文件与写集

本轮新增/修改：

- `internal/contracts/participant.go`, `fake.go`；
- `internal/logic/actor/runtime/executor.go` 及直接 runtime regression test；
- `internal/player/actor/registry.go` 及 direct registry test；
- `internal/player/ingress/service.go`, `service_test.go`；
- `internal/player/participant/server.go`, `plant.go`, `plant_test.go`；
- 本 handoff。

本轮第五轮收口新增的直接回归位于 `registry_participant_test.go`、
`plant_test.go`；没有新增生产存储、Redis script 或 transport seam。

连同前序 PI1 实现，仍未写入 `api/proto/**`、`api/gen/**`、`internal/farm/**`、
`internal/friend/**`、`internal/workflow/**`、`cmd/**`、`internal/storage/redis/**`
或 common Redis key/Lua。production `cmd` 未切换；未接入 `StaticFenceSource`；没有
legacy/Authority double write；没有 FI1。

## 合同到测试映射

| 合同/语义 | 直接测试 |
| --- | --- |
| Catalog、caller oneof、WorkType/Step/placement、typed payload/result/hash | `internal/contracts/fake_test.go`, `fake_review_test.go`, `participant_review_test.go` |
| route holder/boot/shard revision/writer epoch、bucket、subject/operation binding | `TestParticipantStrictPhysicalRouteFieldsFailClosed`, `TestParticipantIngressBudgetRouteBucketAndIdentityProofs`, `TestParticipantVerifiersRunBeforeMailboxAndShareHandler` |
| min budget/deadline/max、elapsed decode/verifier/handler | `TestParticipantBudgetUsesCommandMaximumAndVerifierElapsedTime`, `TestPlantDependencyAdmissionAndFinalBudgetAreBeforeCommit`, runtime enqueue race test |
| receipt replay before stale Work/Lease、Authority-only mailbox-free read、version/hash、legacy TOCTOU fail-closed | `TestPlantTerminalReceiptPrecedesStaleWorkAndLease`, `TestReadOutsideMailboxDoesNotSubmit*`, `TestLegacyReceiptReadNeverRunsFenceThenGetTOCTOU`, `TestLegacyRegistryCannotConstructCheckedParticipant`, `TestPlantHappyReplayAndRealRecordVersion` |
| hot eviction/rebuild/conflict/inventory change | `TestPlantTerminalReceiptSurvivesHotEvictionAndInventoryChange` |
| Plant WorkRecord/Farm semantic mutation matrix/release reason | `TestPlantWorkRecordAndFarmProofMutationMatrix`, `TestPlantFarmDependencyAndReleaseReasonAreFailClosed` |
| insufficient seed、commit/release concurrency、COMMITTED release rejection | `TestPlantInsufficientSeedIsTerminalAndReplayed`, `TestPlantCommitReleaseConcurrentSingleTransition`, `TestPlantReleaseAndCommittedReleaseRejection` |
| schema-version + command + semantic hash preimage | `TestParticipantCanonicalHashFramesSchemaAndCommand` |
| item/model/receipt physical capacity、retention candidate、provider profile、stable ResourceExhausted | `TestPlantReceiptItemCapacityUsesConfiguredAuthorityProfile`, `TestAuthorityRegistryParticipantUsesSmallerLimitProfile`, `TestAuthorityParticipantCapacityMustComeFromProvider`, `TestParticipantCapacityPreflightPrunesExpiredPublicReceipts`, `TestParticipantPhysicalCapacityByteBoundaries`, `TestPlantAuthorityCapacityErrorMapsStableAndDoesNotCommit` |
| production durable handler/receipt proof、custom seam rejection | `TestParticipantCustomHandlerCannotBypassReceiptResolver`, `TestParticipantTerminalResultRequiresDurableReceiptProof` |
| explicit receipt kind/namespace and collision/retention | `TestPublicSid1OperationIsNotParticipantArchive` |
| TIMEOUT_UNKNOWN、reload、old writer fence | `TestPlantTimeoutUnknownResolutionsAndWriterFence`, `TestAuthorityPlayerStaleWriterLateWriteRejected` |
| Fake exact version/result/hash and bounded handler | `TestFakePreservesExactHandlerCommittedRecordVersion` plus existing Fake replay/conflict tests |

## 验证原始结论

以下是本轮代码 commit 后、handoff finalization 前得到的 raw conclusions；handoff
commit 后又执行了 diff/status 收口检查。

```text
GOCACHE=/tmp/pi1a1-gocache go test -count=1 -timeout=600s ./...
PASS

GOCACHE=/tmp/pi1a1-gocache go test -race -count=1 -timeout=240s \
  ./internal/contracts/... ./internal/player/actor/... \
  ./internal/player/ingress/... ./internal/player/participant/... \
  ./internal/logic/actor/runtime/...
PASS

GOCACHE=/tmp/pi1a1-gocache go test -race -count=1 -timeout=600s ./...
PASS

GOCACHE=/tmp/pi1a1-gocache go vet ./...
PASS: no diagnostics

GOCACHE=/tmp/pi1a1-gocache go build -buildvcs=false ./...
PASS: no diagnostics

bash scripts/gen-proto.sh
PASS; `git diff --exit-code -- api/gen` is empty

git diff --check 3e715a481ca62091e65b2c36977b13fef1461e76..HEAD
PASS: no whitespace errors

git status --porcelain
PASS after the final handoff commit: EMPTY
```

## 平台证据、Pending 与提交记录

- **真实 Redis Cluster：未执行。** Redis adapter tests use miniredis only；不能作为
  production Redis Cluster evidence。
- **真实 mTLS/service identity：未执行。** 本任务没有改 transport/mTLS composition。
- **真实 WorkRepository/Farm proof composition：Pending I0。** 本实现保留 fail-closed
  `WorkVersionVerifier`、`WorkLeaseVerifier`、route/bucket/identity verifier 和
  concrete Plant dependency seam；production cmd 未接线，Fake/miniredis 不冒充生产。
- **Memory AuthorityStore 与 miniredis：已执行 deterministic tests**，覆盖 happy path、
  replay、conflict、concurrent duplicate、reload、TIMEOUT_UNKNOWN、late writer fence
  和 capacity failure；这不是 external platform probe。

前序远端提交（base 后）包括：

```text
cd42dbea contracts: share participant ingress kernel
78a31ddf player: add Plant Participant authority adapter
4462961d docs: complete PI1-A1 Plant Participant handoff
96878e44 fix: harden player participant replay and ingress
3d3ac4d fix: close PI1-A1 participant review gaps
7a1ec90 fix: close PI1-A1 admission and capacity gaps
a67a05f fix: normalize ingress capacity errors
fe64961 docs: record PI1-A1 final capacity boundary
69ebd40 fix: close PI1-A1 mailbox admission gaps
64896b6 fix: close participant authority construction gaps
3bff6c6 docs: record PI1-A1 fifth review closure
```

本 handoff 是 `64896b6` 的直接 documentation child；其精确 SHA、最终 new tip、
远端 tip 和 push 原始结论以最终 `git log`/remote delivery check 为准。目标仍为
`codex/pi1-a1-player-plant-participant-20260804`，只做 fast-forward，不合并
integration、不创建 MR。
