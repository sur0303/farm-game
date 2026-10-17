# R1-REDIS-AUTHORITY-ADAPTER Handoff

- task_id: R1-REDIS-AUTHORITY-ADAPTER
- branch: `codex/r1-redis-authority-20260803`(独立 worktree,不重建分支,不合入 integration)
- worktree: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-r1-redis-authority`
- base_commit: `4e587936df183dd7e6e7725200f98dec38e69d5d`
- 日期: 2026-08-03(主体)+ 2026-08-04(硬合同收口,见 §8)
- 状态: **COMPLETED**(miniredis 证据级别 `UNIT_PASS + RACE_PASS`;REAL_REDIS_PASS 保留 Pending,见 §4)

## 0. 基线登记(开工时真实执行)

```text
git status --short --branch → ## codex/r1-redis-authority-20260803(干净)
git rev-parse HEAD          → 4e587936df183dd7e6e7725200f98dec38e69d5d
git log -1 --oneline        → 4e58793 docs-sync-round0-checkpoint-and-round1-dispatch
go test ./...               → PASS(exit=0,全部包 ok,无 FAIL)
go vet ./...                → PASS(exit=0,无输出)
```

## 1. 现有实现 → 合同原子操作 差异矩阵(施工前审计,已执行)

| 合同能力(30/31) | 现有实现 | 处置 |
|---|---|---|
| auth-v1 KeyBuilder/HashTag/CRC16 Slot/AssertSameSlot | `internal/storage/redis/rediskey`(keyspace.go/slot.go/profile.go)已冻结并有黄金测试 | **复用**,不建 keybuilder2;Adapter 只消费 `AuthorityKeys`/`PhysicalProfile` |
| StableOpaqueID/WorkRefV1/ReceiptRefV1 | `internal/workidentity`(SCB-1 黄金已锁) | **复用**,不重写 |
| OperationDigest / EffectiveIdempotencyKey | 收口前:`authoritystore.IdempotencyKey` 为 3 字段(subject/command/operation),走 `workidentity.OperationDigest` | **已按硬合同收口(§8)**:`authoritystore.IdempotencyKey = identity.EffectiveIdempotencyKey` 别名(5 字段,含 root.kind+root.canonical_id);Memory/Redis Store 与 Lua 持久化并比较完整 Key + payload_hash,Digest 只用于定位;Store 不重新派生 Root、不要求 Root==提交目标 Actor(FRIEND_PAIR Root 可合法提交进 FarmActor,Root 派生属 CommandCatalog/FirstHop/Coordinator) |
| ActorAuthorityStore 逻辑接口/typed status/Fence/ResolutionReceipt | `internal/storage/authoritystore/types.go` + MemoryStore(进程内 Fake)+ FaultStore | **复用接口与枚举**;Redis Adapter 在 `internal/storage/redis/authority` 新增,不改 authoritystore 既有文件 |
| ShardWriteAuthority Claim/Renew/Transfer/Unassign | MemoryStore 逻辑状态机(W8C) | 新增 4 个 Lua + Go 适配,语义逐项对齐 MemoryStore(Caller 鉴权在 Go 侧,TTL 由脚本按 LeaseLimits 裁决,transition replay 用 intent digest) |
| ScriptRegistry/EVALSHA/NOSCRIPT 恢复 | 无(blobstore 用内联 EVAL 字符串) | **新增** `ScriptRegistry`:内嵌脚本、SHA1、`EnsureLoaded`(Cluster 时 ForEachMaster)、NOSCRIPT 有界重载重试(≤2 次),重试耗尽映射 UNAVAILABLE_NOT_SENT |
| Redis 错误→typed status | `blobstore.classifyRedisError`(私有,blob 专用) | 策略一致,在 adapter 内实现面向 store status 的分类器;UNAVAILABLE_NOT_SENT/TIMEOUT_UNKNOWN 只由传输分类产生,脚本永不返回;CROSSSLOT 属合同违规 fail closed 为 Go error |
| LoadAndClaimActorV1 | 无 | 新增 Lua:shard fence→幂等 claim(claim_operation_id+new_claim)→初始化/返回 meta+manifest;Go 侧按 manifest 有界 HMGET 装配 Snapshot,禁止 HGETALL |
| CommitActorMutation(WithOrders)V1 | 无 | 新增单个 Lua,orders=0 即 31 §6.4 的 CommitActorMutationV1,0..N(N≤4 硬上限)即 §6.5;同一来源 Bucket 内 Actor 状态+Receipt+Order+DueIndex+receipt-due 原子提交;receipt 比较先于 RecordVersion |
| CommitParticipantMutationV1 | 无 | **不单独建脚本**:Participant 防重复用同一 receipt 机制(digest 输入由调用方按 participant_invocation_id 构造 EffectiveIdempotencyKey);若 AR1/WF1 接线时需要差异化脚本,走 ICR(见 Pending-6) |
| FenceAndClaimActorV1 | 无 | 新增 Lua:C1→C2 原子换代 + 同提交持久 ResolutionReceipt(request_digest 绑定请求);同 resolve_operation_id 重试返回持久结论,不随新写漂移;参数不同的重放返回 RECONCILE_REQUIRED |
| Work Lease(Claim/Renew/Complete) | 无(旧 workflow 主链另有 lease,不属本合同) | 新增 ClaimWorkV1/RenewWorkLeaseV1/CompleteWorkV1;Redis TIME 裁决,只收 requested_ttl_ms,返回 lease_epoch/work_version/accepted_ttl_ms;Complete 同槽 ZREM due 并维护 bucket-meta 计数 |
| Bucket Scan Lease | 无 | 新增 ClaimBucketScanLeaseV1/RenewBucketScanLeaseV1 |
| DueIndex | rediskey.SourceDueIndex/ReceiptDueIndex 已有 Key;无读写原语 | 随 Commit/CompleteWork 同槽 Lua 原子维护;member=EncodeWorkRefV1 |
| AuthorityReceipt/ResolutionReceipt 编码 | 无 | 新增有界编码(payload_hash 64 字节定长前缀+LP 字段),receipt value ≤1KiB 硬上限(脚本内强制) |
| receipt-due 生命周期索引 | 无 | Commit 内同槽 ZADD(score=redis_now+full_retention,ARGV 传入,默认 24h);CompactReceiptV1 → **Pending-3** |
| ResultDirectory | rediskey.ProjectionKeys.ResultDirectory | **已按硬合同收口(§8)**:KeyBuilder 显式生成冻结的 `proj:result:v2:<player_id>`(不走通用 byID,其他 projection 保持各自版本);新写只写 v2,无 v1/v2 双主写、无 v1 回读(v1 从未成为已发布生产格式);PublishResultEntryV1 幂等/CONFLICT + Lookup |
| LookupIdempotencyOutcome | MemoryStore 实现 | Redis 只读 HGET 精确 Field;RECOVERY_OVERDUE 证据依赖 CompactReceipt 链路 → **Pending-3** |
| bucket-meta 维护 | Key 已存在 | Commit/CompleteWork 同槽维护 pending_work_count/pending_work_bytes/receipt_due_count;hash_config_id/migration_state/Reconcile 水位 → **Pending-4** |
| GuardSync Watermark / CompleteGuardSyncOrderV1 | 无 | **Pending-5**(WF1/宠物接线范围) |
| PendingOutbox 物理布局 | 逻辑接口字段存在,31 未冻结 Key/布局 | **Pending-8**:Adapter 对非空 PendingOutbox fail closed(ErrInvalidRequest),不猜测布局 |
| 旧 blobstore.RedisStore(当前生产主链) | `internal/storage/blobstore/redis.go`(farm:v118 前缀,小 CAS Lua) | **不切换**:本任务不动 blobstore、不接 cmd/player-svr、cmd/farm-svr;主链切换属 AR1 |

## 2. 交付物清单(全部在 owned_write_set 内)

```text
internal/storage/redis/authority/scripts.go      13 个固定版本化 Lua + SHA1 注册表
internal/storage/redis/authority/registry.go     ScriptRegistry:EVALSHA、EnsureLoaded、NOSCRIPT 有界恢复
internal/storage/redis/authority/classify.go     传输错误→sendFate(not-sent/unknown)与 CROSSSLOT 合同违规
internal/storage/redis/authority/encoding.go     LP 编码、receipt/resolution/result-entry 编解码、digest
internal/storage/redis/authority/store.go        ActorAuthorityStore 实现(LoadAndClaim/Commit/FenceAndClaim/Lookup)
internal/storage/redis/authority/shard.go        ShardWriteAuthorityStore 实现(Claim/Renew/Transfer/Unassign)
internal/storage/redis/authority/worklease.go    WorkLease/ScanLease 原语与 typed status
internal/storage/redis/authority/resultdir.go    ResultDirectory 发布/查询(Projection,幂等)
internal/storage/redis/authority/golden_test.go  脚本 SHA 黄金 + slot/key 黄金
internal/storage/redis/authority/helper_test.go  miniredis 测试夹具
internal/storage/redis/authority/store_test.go   claim/commit/CAS/fence/容量/订单原子性/lookup
internal/storage/redis/authority/fault_test.go   before/after-apply 响应丢失、resolution 稳定性、旧 writer 迟到写、NOSCRIPT
internal/storage/redis/authority/shard_authority_test.go  shard 生命周期/replay/过期接管/UNAUTHORIZED
internal/storage/redis/authority/worklease_test.go        work lease 生命周期/互斥/过期接管/scan lease
internal/storage/redis/authority/resultdir_test.go        ResultDirectory 幂等/冲突/上限/NOT_FOUND
internal/storage/redis/authority/live_test.go    FARM_REDIS_URL 门控的真实 Redis 冒烟(默认 skip)
go.mod / go.sum                                  新增测试依赖 github.com/alicebob/miniredis/v2 v2.38.0(仅 _test.go 使用)
docs/verification/r1-redis-authority-handoff.md  本文件
```

未触碰(主体阶段):blobstore、cmd/*、Proto、Actor/Workflow/业务包、docs/design。
硬合同收口阶段(§8,经 ICR-R1-HARD-CONTRACT-CORRECTION 窄授权)另修改了:
`internal/storage/authoritystore/{types.go,memory.go,store_test.go}`(5 字段幂等身份统一)与
`internal/storage/redis/rediskey/{keyspace.go,keyspace_test.go}`(ResultDirectory v2)。

## 3. 测试原始结果(本 worktree 本 HEAD 真实执行)

```text
gofmt -l internal/storage/redis docs/verification → 无输出(干净)
git diff --check → PASS(exit=0)

go test -race -count=1 -timeout=120s ./internal/storage/authoritystore/... ./internal/storage/redis/...
ok  farmgame/internal/storage/authoritystore  1.034s
ok  farmgame/internal/storage/redis/authority 1.444s
ok  farmgame/internal/storage/redis/rediskey  1.020s
EXIT=0

go test -count=1 -timeout=180s ./...  → 全部包 ok,0 FAIL,EXIT=0
go vet ./...                          → PASS(exit=0,无输出)
```

覆盖的合同语义断言:

- 脚本 SHA 黄金(13 个)与 slot/key 黄金(同 atomic group 同 tag 同 slot=11817,PLAYER/FARM 不混槽);
- LoadAndClaim:首次 CLAIMED、同 op+同 claim 幂等 ALREADY_CLAIMED、同 op 异 claim / 同 claim 异 op 均 RECONCILE_REQUIRED、重新激活不重置状态、旧 claim 写被拒(WRITER_CLAIM_MISMATCH);
- Commit:receipt 先于 RecordVersion(ALREADY_COMMITTED 覆盖陈旧 expected_version)、同 operation 不同 payload→IDEMPOTENCY_CONFLICT、CAS→RECORD_VERSION_CONFLICT、五元 shard fence 逐项→WRITER_EPOCH_MISMATCH、NO_CHANGE 是持久终态且 record_version 前进、8 并发同 payload 恰好 1 个 COMMITTED 其余 ALREADY_COMMITTED(-race)、全量替换删除陈旧 model field;
- 容量:model bytes/fields、单提交订单数(>4)、receipt value 1KiB、entry 字节上限均 fail closed 且无部分写入(state/receipt/due 全部不变);
- 订单与 DueIndex 同提交:WorkRecord(PENDING)+ ZADD due(score=due_at)原子可见;同 work_id 异 payload→WORK_CONFLICT 且整体不提交;
- TIMEOUT_UNKNOWN:before-apply→FenceAndClaim 解析 NOT_COMMITTED;after-apply/response-lost→解析 COMMITTED 且带回原 outcome 与 OriginalReceiptID;fence 自身 response-lost 重试重放持久 Resolution;新 writer 后续提交后原 fence 重试仍返回原 resolution(CutRecordVersion 不漂移);参数不同的 resolve 重放→RECONCILE_REQUIRED;旧 writer 迟到写→WRITER_CLAIM_MISMATCH 且 NOT_COMMITTED 结论不翻转;
- Shard:Claim/Renew/Transfer/Unassign 生命周期、transition replay(ALREADY_APPLIED/RECONCILE_REQUIRED)、非 Holder Renew→UNAUTHORIZED、活跃 lease 冲突、过期接管(epoch+1)、陈旧 fence 写 fail closed、NOT_FOUND、layout 错配 fail closed;
- WorkLease:CLAIMED(返回 work 事实)/LEASE_HELD/NOT_DUE/过期后被接管(epoch+1)、FENCE_MISMATCH(holder/token/epoch)、RENEWED、COMPLETED(同槽 ZREM due)/ALREADY_COMPLETED;ScanLease 互斥/续期/错 epoch→FENCE_MISMATCH/过期接管;
- ResultDirectory:PUBLISHED→ALREADY_PUBLISHED(同值幂等)→CONFLICT(异值 fail closed 且不覆盖)、Lookup 命中原值、缺失 NOT_FOUND、超限拒绝;
- NOSCRIPT:空 script cache 首次 EVALSHA 自动 SCRIPT LOAD 重试成功;分类器单测(ECONNREFUSED→not-sent、deadline→unknown、CROSSSLOT→合同违规)。

## 4. 平台 Pending / ICR

- ~~ICR-1(ResultDirectory v1↔v2)~~:**已收口(§8)**。v2 为唯一冻结格式;v1 从未成为已发布生产格式,无双主写、无回读。
- ~~ICR-2(OperationDigest 3↔5 字段)~~:**已收口(§8)**。5 字段 EffectiveIdempotencyKey 已统一落到 authoritystore 逻辑接口、MemoryStore、Redis Store、Lua 与持久化编码。
- **Pending-1**:公司 Redis 持久化/noeviction/Failover/RPO/RTO/ACL/TLS 未探针(`FARM_REDIS_URL` 在本环境未设置),不申报 REAL_REDIS_PASS / PRODUCTION_DURABILITY_PASS;`live_test.go` 已留门控冒烟入口。
- **Pending-2**:Redis Cluster 真实 MOVED/ASK 与多节点 Script Cache 行为未实测(miniredis 为单节点语义;分类器已按合同映射 MOVED/ASK/CLUSTERDOWN→not-sent)。
- **Pending-3**:CompactReceiptV1(Full→Tombstone→Expired Evidence)与 RECOVERY_OVERDUE 证据链未实现,属 W31-B/C;当前 Lookup 不会返回 RECOVERY_OVERDUE。
- **Pending-4**:bucket-meta 的 hash_config_id/migration_state/Reconcile 水位与 Reconcile 重建流程未实现。
- **Pending-5**:GuardSyncWatermark Key(`<tag>:sync:guard:<player_id>`)与 CompleteGuardSyncOrderV1 未实现,属 WF1/宠物接线。
- **Pending-6**:CommitParticipantMutationV1 独立脚本未建;Participant 防重复用通用 Commit receipt 机制。
- **Pending-7**:Order 终态 GC/保留策略未冻结,CompleteWork 保留终态记录不删除。
- **Pending-8**:PendingOutbox 无冻结物理布局,Adapter fail closed 拒绝非空 Outbox。
- **Failover/RPO 未跑**:保留 Pending,不申报任何持久化等级。

## 5. 回滚方式

```text
git revert <本任务三个 commit>
# 或合入前直接删除分支:
git push origin --delete codex/r1-redis-authority-20260803
```

回滚影响面:仅删除 `internal/storage/redis/authority/`(全新目录)、go.mod/go.sum 的 miniredis 测试依赖与本 handoff;现行 blobstore 主链、authoritystore 逻辑包、rediskey 黄金均不受影响(本任务未修改任何既有文件)。

## 6. 给 AR1/WF1 的公共接口

```go
// Actor 权威(实现 authoritystore.ActorAuthorityStore + ShardWriteAuthorityStore)
store, _ := authority.NewStore(redisClient, nil /*registry 可共享*/, authority.Options{
    Profile:  physicalProfile,   // rediskey.PhysicalProfile(AuthorityLayoutVersion 形如 "v1")
    Resolver: shardResolver,     // 与 Actor 路由同一 RouteSnapshot,禁止第二持久化哈希
    Limits:   authoritystore.DefaultLimits(),
    // LeaseLimits/LimitProfileVersion/MaxBucketPendingWork/MaxOrderPayloadBytes/
    // MaxReceiptTotalBytes/ReceiptFullRetentionMs 均有保守默认
})
store.Registry().EnsureLoaded(ctx)          // 启动预热:逐 Primary SCRIPT LOAD

// Workflow/Worker(同一来源 Bucket 内的 Order 租赁)
lease := store.WorkLease()
claim, _ := lease.ClaimWork(ctx, bucket, workType, workID, holderID, leaseToken, ttlMs)
renew, _ := lease.RenewWorkLease(ctx, bucket, workType, workID, holderID, claim.LeaseEpoch, leaseToken, ttlMs)
done,  _ := lease.CompleteWork(ctx, bucket, workType, workID, holderID, claim.LeaseEpoch, claim.WorkVersion, leaseToken)
scan,  _ := lease.ClaimBucketScanLease(ctx, bucket, holderID, ttlMs) // + RenewBucketScanLease

// 公网结果查询副本(Projection,永不作权威裁决)
dir, _ := authority.NewResultDirectoryStore(redisClient, store.Registry(), namespace, 0)
status, _ := dir.PublishResult(ctx, playerID, authority.ResultEntry{...})
entry, found, _ := dir.LookupResult(ctx, playerID, operationDigest)
```

接线注意事项:

1. `AuthorityLayoutVersion` 物理取值为 `PhysicalProfile.AuthorityLayoutVersion`(形如 `v1`,rediskey 只接受 `v\d+`);MemoryStore 测试里的 `"auth-v1"` 是逻辑别名,接线时统一用 profile 值。
2. AttachedOrder 的 `OrderID` 必须是合法 StableOpaqueID(`sid1_`);OrderType 为 `[A-Z0-9_]` 稳定名;DueIndex member 由 adapter 内部用 `EncodeWorkRefV1` 生成。
3. Model 字段名限制为 `[a-z0-9_]{1,64}`(防止与 `_meta:`/`receipt:` 前缀碰撞),Commit 是全量替换语义。
4. 所有冲突/失败结果的 Snapshot 由 adapter 做 manifest 有界补读(禁止 HGETALL);调用方不得在 conflict 分支假设 ModelFields 与提交瞬间一致(线性化点以脚本返回为准)。
5. 传输分类只有两种 typed status:UNAVAILABLE_NOT_SENT(可安全重试)与 TIMEOUT_UNKNOWN(必须走 FenceAndClaim 解析,禁止当普通重试)。
6. `Store` 与 `WorkLeaseStore`/`ResultDirectoryStore` 共享同一 `ScriptRegistry`;Authority/Runtime/Projection 应按 31 §15.3 使用独立客户端配置,本包不强制。

## 7. evidence_level 自评

`UNIT_PASS + RACE_PASS`(miniredis 进程内 Redis 语义)。不申报 REAL_REDIS_PASS(未在公司 Redis 取得原始结果)、不申报 PRODUCTION_DURABILITY_PASS(Failover/RPO 未跑)。旧 writer 晚写成功、跨 slot 脚本、无界读、KEYS/SCAN 在本实现中结构上不可能出现(同槽 tag 校验 + 固定 Key 集合 + manifest 有界读)。

## 8. 硬合同收口(2026-08-04,ICR-R1-HARD-CONTRACT-CORRECTION 窄授权)

独立验收发现两处不能带入 CP2A 的硬合同偏差,本轮只修这两处,未扩张到其他功能。

### 8.1 root-scoped 五字段幂等身份

- `authoritystore.IdempotencyKey` 改为 `identity.EffectiveIdempotencyKey` 类型别名
  (`subject_player_id + stable_command_type_name + operation_id + root.kind + root.canonical_id`),
  旧 3 字段摘要(`workidentity.OperationDigest` 调用点)全部移除;`OperationDigest()` 只做定位。
- MemoryStore/Redis Store/Lua 持久化并比较完整 Key:receipt 值新增 `key_identity`
  (LP(subject||command||operation||root_kind||canonical_id)),digest 命中后完整 Key 相同 +
  payload_hash 相同才重放原结果,任一不同即 IDEMPOTENCY_CONFLICT;
  ResolutionReceipt 同样持久化完整原 Key 供 replay 重建。
- Store 不重新派生 OperationRootRef,也不要求 `key.Root == ActorAddress`。
  Root 的可信派生属 CommandCatalog/FirstHop/Coordinator;空 Root/非法 Kind/ID 由
  `identity.Validate` fail closed。曾误入的 `ExpectedRootFor/validateRootBinding`
  (Root 强制等于提交目标 Actor)已按合同语义移除——它会把 FRIEND_PAIR/INDEPENDENT_WORK
  的合法 Participant 提交错误拒绝。
- 新增证明(两 store 各有镜像):
  - FarmActor AuthorityCommit 合法携带 `FRIEND_PAIR` Root(memory+redis);
  - 相同 subject/command/operation 在不同 Root 下 digest 隔离、可分别提交、重放各自隔离(memory+redis);
  - 完整 Key 相同 + payload_hash 不同仍冲突(memory+redis 既有用例,收口后仍通过);
  - 空 Root fail closed(memory+redis)。

### 8.2 ResultDirectory 冻结 v2 Key

- `rediskey.ProjectionKeys.ResultDirectory` 显式生成 `proj:result:v2:<player_id>`
  (常量 `resultDirectoryLayoutVersion`,不走通用 `byID`;farm-opportunity/redpoint/
  public-profile 保持各自当前版本未被顺带升级)。
- 新写只写 v2,无 v1/v2 双主写;该功能未进生产主线,无真实 v1 数据,P0 不加 v1 回读。
  原 ICR-1「等待合同 owner 解决」注释已删除:v1 从未成为已发布生产格式。
- 测试断言:key builder 黄金 v2、条目物理落在 v2 Key 且 v1 Key 无任何字段、
  NOSCRIPT 用例的 Key 字面量为 v2。

### 8.3 收口验证原始结果(本 worktree 本 HEAD 真实执行)

```text
git diff --check 0f70c61..HEAD → PASS(exit=0)

go test -race -count=1 -timeout=120s ./internal/identity/... ./internal/storage/authoritystore/... ./internal/storage/redis/...
?   farmgame/internal/identity [no test files](黄金在 protocoltest)
ok  farmgame/internal/storage/authoritystore  1.035s
ok  farmgame/internal/storage/redis/authority 1.506s
ok  farmgame/internal/storage/redis/rediskey  1.021s
EXIT=0

go test -count=1 -timeout=240s ./...  → 54 包 ok,0 FAIL,EXIT=0
go vet ./...                          → PASS(exit=0,无输出)
go build -buildvcs=false ./...        → PASS(exit=0,无输出)
```

既有 NOSCRIPT、response-lost(before/after-apply)、旧 writer fence、并发 claim、
容量上限用例在 5 字段编码与 v2 Key 下全部保持通过。

收口 commit:`a0c0992`(代码+测试);本文件更新为紧随其后的文档提交。

### 8.4 明确不在本轮实现(保持 Pending)

CompactReceipt、终态 GC、RECOVERY_OVERDUE 扫描、bucket-meta 迁移水位、GuardSync、
PendingOutbox 新布局、真实公司 Redis 故障切换探针均不在本轮;不接 PlayerActor/FarmActor;
不改 26～30 已冻结语义;不动 P1B 的 Proto/CommandCatalog 写集;
证据级别仍为 `UNIT_PASS + RACE_PASS`(miniredis),不申报 REAL_REDIS_PASS。
