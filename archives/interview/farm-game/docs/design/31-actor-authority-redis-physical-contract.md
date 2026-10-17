# 31 · Actor 权威存储、Redis 物理布局与容量合同

版本：`v1.4-friend-opportunity-navigation-runtime-freeze-candidate`
日期：2026-08-03
定位：把第 30 号 Actor 写权与持久化逻辑合同映射为公司 Redis 的 Key、Hash Tag、Lua、记录、
Workflow/Order、查询副本、容量、限流、指标与故障验证合同；供 Redis Adapter、Actor Repository、
RouteManager、farm-worker、FarmSubscriptionHub、压测工具和后续实现 Agent 使用。
状态：`31-01～31-24` 已逐项确认。稳定语义不得由实现自行改变；所有带
`LimitProfile/CapacityProfile/SLOProfile v0.1` 的数值仍是压测候选，不代表已经取得生产平台实测证据。

本次修订统一 DueIndex 的紧凑 `WorkRefV1`、Receipt 到期索引编码、ResultDirectory 缺失语义、
SourceOrder 重要性分级，以及控制面 Handoff 与 Redis 数据面 Lease 状态边界；不改变双 Actor、
VirtualShard、同槽原子域或 Workflow/Order 主架构。
本版再把持久操作定位升级为 Root 作用域 Digest，把 ResultDirectory 升级为可指向 Actor/
非 Actor 权威根的 `proj:result:v2`，并将来源同槽 Order 统一表达为 `SourceAuthorityRef`。
本版继续把核心业务的 Reservation/CropCycle/CareIncident/TheftAttempt/Settlement 身份统一到
`StableOpaqueID v1`，并冻结 Farm AuthorityRecord 内有界 `FarmOpportunityHeader` 与 Projection Summary
的物理边界。
本版继续映射第 34 号任务合同：`TaskDomain` 留在 PLAYER ActorRecord，同一玩家任务与领奖复用现有
Authority Commit/Receipt；任务红点使用版本化可重建 Projection，不新增 FriendVisit Work 或每次变化
必达的 TaskSummary Order。
本版再映射第 35/35A 号邮件合同：`model:mail_claim_state` 只保存领取防重，MailEntry/已读/分页由
非 Actor PlayerMail Authority Redis 管理，claimed/红点是可重建 Projection，不把正文塞回 PlayerActor。
本版再映射第 36 号宠物合同：PetState 增加页面状态版本，GuardSync 使用统一来源 Order 身份；同步完成
水位以 PLAYER Bucket 内独立固定大小 Key 保存，避免 worker 修改 Actor `model:pet` 或依赖终态 Order 永久存在。
本版再映射第 37 号图鉴合同：永久成员与 Seen 水位保留在 PLAYER Authority Group，拆成有界 Model
Field 但不拆 WriterClaim/RecordVersion/PersistenceLane；Catalog 仅作本地不可变配置快照，图鉴查询不建
权威 Redis 副本，并增加配置发布前容量闸门。
本版进一步把 `GET_NEXT_FRIEND_OPPORTUNITY` 映射到 Runtime Redis 的连续导航窗口，并冻结好友
`SUBSCRIPTION_READY` 后由服务端按 ActiveSubscription Fence 最佳努力标记 visited：它不新增公网 Ack、
不建立持久 Work，也不替代每日访问任务的可靠确认。

本文成熟度分为三层：

- `STABLE_SEMANTIC`：数据归属、原子边界、Fence、失败语义、恢复锚点和有界原则已经确认；
- `PARAMETER_CANDIDATE`：大小、TTL、队列水位、吞吐与 SLO 是首轮配置，必须版本化且可调整；
- `PROBE_PENDING`：公司 Redis 的持久化、容灾、Lua 延迟、集群路由与成本尚待真实环境验证。

上位与相邻权威：

- [26-access-session-contract.md](./26-access-session-contract.md) 唯一负责登录、Session、Join、
  Subscription、Ready Cut 与实时下行语义；
- [27-access-routing-actor-workflow-freeze.md](./27-access-routing-actor-workflow-freeze.md) 负责 Actor
  Route/Runtime、领域权威划分和 Workflow/Order 的 L1 语义；
- [28-command-catalog-public-protocol.md](./28-command-catalog-public-protocol.md) 负责公网协议、
  CommandCatalog、Operation Journal 与结果恢复；
- [29-internal-rpc-trusted-context.md](./29-internal-rpc-trusted-context.md) 负责内部可信 Context、
  Participant/Worker RPC 和 Join 内部握手；
- [30-actor-authority-store-contract.md](./30-actor-authority-store-contract.md) 唯一负责 WriterEpoch、
  WriterClaim、RecordVersion、Unknown Outcome 与 `ActorAuthorityStore` 逻辑合同；
- [34-task-system-contract.md](./34-task-system-contract.md) 负责任务周期、实例、进度、领奖和红点业务语义；
- [37-plant-compendium-contract.md](./37-plant-compendium-contract.md) 负责图鉴身份、Plant解锁快照、
  Harvest原始Delta、活动档案和Seen Fence业务语义；
- [core-scenario-casebook.md](./core-scenario-casebook.md) 记录登录 Join 和好友机会等端到端案例。

若本文与 26～30 号的逻辑语义冲突，以 26～30 号各自权威范围为准，并停止实现、回到设计评审；
不得用 Redis 物理便利性反向改写上层业务合同。本文不以旧
[30-redis.md](./30-redis.md) 的“Redis 仅作缓存”假设为依据。

---

## 0. 一页结论

P0 使用公司 Redis 作为当前可执行的 Authority Store，但存储访问必须位于
`ActorAuthorityStore`/Repository 接口之后。若真实平台探针无法满足持久化 RPO、故障切换、
`noeviction`、Lua 吞吐或成本要求，可以替换长期权威存储，而不能推翻 Actor、幂等、Workflow 和
公网协议。

核心物理结构：

```text
PLAYER/FARM RouteDomain
→ StableHash(actor_id)得到VirtualShard
→ auth-v1中AuthorityPersistenceBucketID = VirtualShardID
→ 由固定Hash Tag进入同一Redis Cluster Slot
→ 同槽包含：
   ShardWriteAuthority
   ActorRecord
   AuthorityReceipt / ResolutionReceipt
   来源Order
   来源DueIndex
```

一次强提交的物理线性化检查顺序：

```text
Redis TIME / ShardWriteAuthority仍ACTIVE且Lease有效
→ HolderID / BootID / WriterEpoch匹配
→ Actor WriterClaimID匹配
→ EffectiveIdempotencyKey + payload_hash检查
→ RecordVersion匹配
→ 检查item/byte/model/order容量
→ 原子写ModelMutation + Receipt/Outcome + 0..N个有界Typed SourceOrder + DueIndex
→ RecordVersion++
```

P0 不使用“万能事务 Lua”，而使用固定、版本化、小范围脚本族。Lua 只负责原子检查与修改，不解析
业务规则，不跨 Slot，不执行无界扫描。

普通低风险 Player 状态可以在 Feature Toggle 打开后使用：

```text
Model级DirtyFlags
→ ActorKey去重SoftSyncQueue
→ 50～200ms候选合并窗口
→ 只序列化脏Model
→ 一次有Fence的多Field提交
```

FarmActor 的地块、作物、护理、偷菜、FarmVersion、公开变化，以及 Plant/Harvest/Theft 等跨 Actor
流程仍必须可靠提交。关键步骤通过 Durability Barrier 把此前未落库的相关 Soft 状态、Receipt 和
Reservation 一并提交。

可靠异步不依赖 MQ：

```text
来源状态 + Order + DueIndex同槽原子提交
→ DirectDispatcher进程内快速尝试
→ farm-worker通过DueIndex恢复
→ ParticipantReceipt最终防重
```

Redis 逻辑上分三类：

| 类别 | 内容 | 是否权威 | 是否允许 TTL/重建 |
|---|---|---:|---|
| Authority Redis | Actor、Receipt、Workflow、Order、ShardWriteAuthority | 是 | 当前状态和未决证据禁止 TTL |
| Runtime Redis | Ticket、SessionRoute、Navigation、限流等 | 否/短期会话权威 | 可按合同 TTL |
| Projection Redis | ResultDirectory、好友机会、红点、排行、公开资料 | 否 | 可异步、可重建 |

P0 可以物理共用公司 Redis，但 Repository、前缀、ACL、连接池、指标和容量预算必须分离；生产上可按
故障域和负载独立拆集群。

“Projection 可重建”不等于所有条目都可盲目 TTL：未完成 ResultPublishOrder、客户端仍可恢复的
ResultDirectory 和 EXPIRED 证明必须遵守第 8 章证据生命周期；好友机会、红点、排行等丢失只影响展示
及时性的投影才可直接回源/重算。

---

## 1. 术语、概念与不变量

### 1.1 VirtualShard 与 AuthorityPersistenceBucket

两者语义不同：

- `VirtualShard`：Actor Route 的稳定计算单位，回答“哪个 Holder Pod 当前处理该 Actor”；
- `AuthorityPersistenceBucket`：持久化同槽原子域，回答“哪些 Redis Key 必须可被同一 Lua 处理”。

P0 `auth-v1` 冻结物理映射：

```text
AuthorityPersistenceBucketID = VirtualShardID
```

这只是 v1 布局选择，不代表两个概念永久等价。Holder Assignment 改变时不搬 Redis Key；更改
ShardCount、HashVersion、Seed 或 AuthorityLayoutVersion 必须创建新版本并执行显式迁移，禁止原地
修改。

### 1.2 Redis Cluster Slot

Bucket ID 不是 Redis 的物理 Slot ID。Redis 根据花括号内完整 Hash Tag 计算 Cluster Slot：

```text
redis_slot = CRC16(hash_tag) mod 16384
```

不同 Bucket 的 Tag 可能落到同一物理 Slot；Redis Cluster 也可以迁移 Slot 而不改变应用层 Bucket。
实现只依赖“同一个 Bucket 的原子 Key 使用完全相同的 Tag”，不得写死 Slot 编号。

### 1.3 AuthorityPersistenceGroup

P0 每个 `PlayerActor`/`FarmActor` 只有一个主要 AuthorityPersistenceGroup：

```text
ActorAddress
+ authority_layout_version
→ 唯一Group
→ 一个Actor级WriterClaim
→ 一个RecordVersion
→ 一条PersistenceLane
```

逻辑 Model 可以拆分，但不能因此创建多个独立写权、多个 RecordVersion 或多个 Unknown 恢复锚点。
只有真实容量证据证明单 Group 成为瓶颈后，P1 才允许重新设计多 Group。

### 1.4 四个版本/代次不能混用

| 字段 | 范围 | 变化时机 | 作用 |
|---|---|---|---|
| `writer_epoch` | VirtualShard | Holder 写权换代 | Fence 旧 Holder |
| `writer_claim_id` | Actor Activation/Resolver | 每次可写激活或 Unknown 接管 | Fence 旧 Actor 实例 |
| `record_version` | Actor Authority Group | 每次成功持久业务提交 | CAS 与提交顺序 |
| `farm_version`/业务版本 | 领域对象 | 公开业务变化 | Snapshot/Delta 与客户端观察顺序 |

Claim/Fence 元数据变化不应被误当成业务状态变化；`record_version` 是否在 SoftSave 中推进由 Store
合同统一决定，`farm_version` 只在真实公开农场事实变化时推进。

### 1.5 五条总不变量

```text
I1  同一Actor可短暂多内存实例，但同一时刻只有一个有效持久写者。
I2  来源状态、来源Receipt、全部必要Orders和对应DueIndex项要么全部出现，要么全部不出现。
I3  未决Workflow/Order/Resolution证据不得因TTL、LRU或队列满被静默删除。
I4  查询副本不能反向修改权威事实，也不能作为最终偷菜/护理授权。
I5  任何队列、记录、脚本、模型和批次同时受item、byte、单项大小和等待时间约束。
```

---

## 2. Namespace、Hash Tag 与 KeyBuilder

### 2.1 Authority Hash Tag

固定格式：

```text
classic-farm:<env>:<zone>:auth:<domain>:v1:s:<virtual_shard_id>
```

作为 Redis Hash Tag 时：

```text
{classic-farm:prod:z1:auth:player:v1:s:42}
{classic-farm:prod:z1:auth:farm:v1:s:42}
```

PLAYER 和 FARM 即使 actor_id、VirtualShardID 相同，也属于不同 RouteDomain、不同 Tag 和不同权威
空间，不能因为 `player_id == farm_id` 而合并存储或路由域。

### 2.2 Authority Key 候选 v1

以下命名属于本文的 v1 物理候选；实现必须通过统一 KeyBuilder 生成，不允许业务代码字符串拼接：

```text
<tag> = {classic-farm:<env>:<zone>:auth:<domain>:v1:s:<shard>}

<tag>:shard-authority
<tag>:actor:<canonical_actor_id>
<tag>:order:<work_type>:<work_id>
<tag>:due
<tag>:receipt-due
<tag>:sync:guard:<canonical_player_id>      # 仅PLAYER Domain；GuardSyncWatermark
<tag>:bucket-meta
<tag>:scan-lease
```

同一次 Lua 所有 `KEYS[]` 的花括号内容必须逐字相同。Repository 必须重新从
`ActorAddress + authority_layout_version` 推导 Tag，并拒绝调用方携带的错误 Group/Shard 断言。

### 2.3 独立 Workflow Bucket

独立 Workflow 不跟随某个来源 Actor，使用稳定版本化分桶：

```text
workflow_bucket_count = 2^N
workflow_bucket = StableHash(workflow_id) & (workflow_bucket_count - 1)

<wf_tag> = {classic-farm:<env>:<zone>:wf:v1:b:<bucket>}
<wf_tag>:work:<workflow_id>
<wf_tag>:due
<wf_tag>:bucket-meta
<wf_tag>:scan-lease
```

BucketCount 不能随 Worker Pod 数变化。64/256 等只可作为探针候选，未在本文写死。

### 2.4 Runtime 与 Projection Key

Runtime/Projection 不与 Authority 共用原子域，示例：

```text
rt:session:v1:<player_id>
rt:friend-nav:v1:<player_id>:<session_epoch>

proj:result:v2:<player_id>                 # HASH，field=root-scoped operation_digest
proj:farm-opportunity:v1:<farm_id>
proj:redpoint:v1:<player_id>
proj:public-profile:v1:<player_id>
```

这些 Key 的最终分桶可按对应 Repository 演进；不得把 Projection Key 强行塞入来源 Authority Tag，
否则会把读取分片和跨玩家投影错误耦合到来源 Actor。

### 2.5 KeyBuilder 必须提供

```go
type AuthorityBucketRef struct {
    Domain        RouteDomain
    LayoutVersion string
    VirtualShard  uint32
}

type AuthorityKeyBuilder interface {
    ShardAuthority(bucket AuthorityBucketRef) RedisKey
    ActorRecord(bucket AuthorityBucketRef, actorID uint64) RedisKey
    SourceOrder(bucket AuthorityBucketRef, workType WorkType, workID string) RedisKey
    SourceDueIndex(bucket AuthorityBucketRef) RedisKey
    ReceiptDueIndex(bucket AuthorityBucketRef) RedisKey
    BucketMeta(bucket AuthorityBucketRef) RedisKey
    BucketScanLease(bucket AuthorityBucketRef) RedisKey
    AssertSameSlot(keys ...RedisKey) error
}
```

Repository 必须先由 `ActorAddress + authority_layout_version + HashProfile` 派生
`AuthorityBucketRef`，再将同一个 Ref 传给全部 KeyBuilder 方法。禁止出现 ActorRecord 使用 v2 Tag，
ShardAuthority/DueIndex 却仍按隐式 v1 生成的混合布局。

`bucket-meta` 至少记录 `layout_version/domain/bucket_id/hash_config_id/schema_version`，以及有界的
`pending_work_count/bytes`、`receipt_due_count`、`migration_state` 和 Reconcile 水位；这些字段用于
防错、容量和维护，不取代 ActorRecord/WorkRecord 的业务真值。

CI 必须使用黄金样例证明 Go、压测工具和 Lua 测试夹具得到完全相同的 Tag；任何 `CROSSSLOT` 都是
布局合同错误，不能在运行时通过拆成两次写绕过。

---

## 3. ShardWriteAuthority 记录与状态机

### 3.1 物理记录

`<tag>:shard-authority` 使用 Redis HASH，不设置 Key TTL：

```text
schema_version
authority_layout_version
hash_version
route_domain
virtual_shard_id

holder_instance_id
holder_boot_id
writer_epoch

lease_epoch
lease_state                 # UNASSIGNED / ACTIVE
lease_expire_at_ms          # Redis TIME绝对时间

assignment_revision
last_transition_id
last_transition_at_ms
```

Lease 到期是字段语义，不是让 Redis 自动删除 Key。删除 Key 会丢失 WriterEpoch、Revision 和迁移
证据，可能令旧 Holder 重新获得写入机会。

`assignment_revision` 是第 30 号逻辑字段的物理实现，并按第 27/29 号映射为
`ShardRoute.ShardRevision / ActorRouteHeader.shard_revision`。RouteManager 发布 Snapshot 时必须把
Holder、Boot、assignment_revision 与 writer_epoch 作为同一元组发布；不能把全局 Snapshot Revision
写进该字段。

`DRAINING/ASSIGNING` 是 RouteManager 控制面 Handoff 状态和 Holder Runtime 状态，不写入上述
`lease_state`。Redis 数据面只有 `UNASSIGNED/ACTIVE`：Transfer 线性化前是 `ACTIVE(A)`，线性化后是
`ACTIVE(B)`，显式卸载时可为 `UNASSIGNED`；不存在两个同时通过 Store Fence 的 `ACTIVE` Holder。

### 3.2 权限

| 操作 | 允许调用方 | 说明 |
|---|---|---|
| `ClaimShardAuthority` | RouteManager | 首次分配、UNASSIGNED 或确认到期接管 |
| `RenewShardAuthority` | 当前 Holder | 只延长当前 Lease |
| `TransferShardAuthority` | RouteManager | 有序 Handoff |
| `UnassignShardAuthority` | RouteManager | 故障/缩容后的显式未分配 |

服务身份必须来自 mTLS/Workload Identity/ACL，不能相信请求中的 `caller_role` 字符串。

### 3.3 Claim

Lua 使用 Redis `TIME`：

```text
验证domain/shard/layout
→ 允许：记录不存在、UNASSIGNED、或ACTIVE但expire_at <= redis_now
→ transition_id幂等检查
→ writer_epoch++
→ lease_epoch++
→ assignment_revision++
→ 写入新Holder/BootID/expire_at
→ state=ACTIVE
```

已经 ACTIVE 且未到期的 Shard 不允许被另一个 Holder 强抢。

### 3.4 Renew

```text
验证HolderID + BootID + writer_epoch + lease_epoch + ACTIVE
→ redis_now仍小于旧expire_at
→ 只更新lease_expire_at_ms
```

Renew 不增加 WriterEpoch、LeaseEpoch 或 AssignmentRevision。已经过期的 Lease 不能通过迟到 Renew 复活，
必须由 RouteManager 走新 Claim。

### 3.5 Transfer

有序迁移在一个脚本中验证旧 Holder 完整 Fence 和 `expected_assignment_revision`，然后：

```text
旧Holder → 新Holder
writer_epoch++
lease_epoch++
assignment_revision++
last_transition_id=transfer_id
```

RouteManager 只有在新记录已经提交、新 Holder 完成必要初始化并可 Ready 后，才发布新 Assignment。
RouteSnapshot 是路由查询副本；Bucket 内 ShardWriteAuthority 才是 Redis 提交的最终写权真值。

### 3.6 本地 Deadline

Holder 从 Redis 获得 `accepted_ttl_ms` 后，以本地单调时钟建立安全 Deadline。Actor 热路径不得为每次
命令同步查询 Redis Lease；但每次 AuthorityCommit 的 Lua 仍会检查 Bucket 内 ShardWriteAuthority。
本地 Deadline 到期后停止准入新写，直到成功 Renew 或收到新 Assignment。

---

## 4. ActorRecord 物理布局

### 4.1 每 Actor 一个主要 Redis HASH

`<tag>:actor:<actor_id>` 是一个 HASH，不设置 TTL。P0 一个 Actor 只有一个主要记录和一个
RecordVersion：

```text
_meta:schema_version
_meta:authority_layout_version
_meta:writer_epoch
_meta:writer_claim_id
_meta:record_version
_meta:business_state_digest
_meta:updated_at_ms
_meta:limit_profile_version
_meta:model_field_count
_meta:model_total_bytes
_meta:last_claim_operation_id

model:<name>                  # Protobuf bytes
receipt:op:<operation_digest> # 紧凑AuthorityReceipt/Outcome
receipt:resolve:<digest>      # ResolutionReceipt
```

Receipt 使用可精确 HGET 的 Field，不要求激活时扫描全部 Receipt。Receipt Field 数和总字节必须在
元数据中受控；过期清理由有界索引/维护流程处理，禁止 HSCAN 全表式清理热记录。

`activation_generation` 只属于当前进程的 ActivationEntry、MailboxHandle、Repository 回调和释放
Fence，不进入 Redis。Pod/Boot 更换后本地 generation 可以从新进程起点重新计数；持久写权只由
writer_epoch、writer_claim_id 和 record_version 保护。若未来需要持久 Claim 次序，必须新增由 Store
推进的 `claim_revision`，不能复用本地 generation。

### 4.2 PlayerActor Model

候选字段：

```text
model:profile
model:economy
model:inventory
model:task
model:mail_claim_state
model:pet
model:compendium_meta
model:compendium_membership
model:progression
```

逻辑 Wallet、Inventory、Shop、Reward 可以保持独立代码模块，但关键资产与防重 Receipt 必须在
同一 Authority Group 的可靠提交边界中。经验跨越等级/解锁边界时不能作为普通 SoftSave。
`model:mail_claim_state` 只保存与 Player 资产原子相关的 MailClaimReceipt/领取状态；MailEntry 列表和
正文仍在 PlayerActor 之外，不能因物理共槽便利重新塞进 PlayerActor 常驻状态。
同一 Claim Commit 还推进有界 `mail_projection_dirty_generation`；可选 MailClaimProjectionOrder 属于
`REBUILDABLE_PROJECTION`，其容量或投递失败不得阻止资产和 ClaimReceipt 提交。

`model:task` 是有界 `TaskDomainState` Protobuf，只保存三个当日任务、一个当前成长任务、永久累计指标、
周期/配置版本、`task_state_version` 与 `claimable_count`。Claim 与 Participant 防重继续使用统一
`receipt:op:<operation_digest>` / Participant Receipt Field，不在 `model:task` 复制无界事件流水或第二套
Receipt 历史。一个 Authority Commit 即使改变多个任务实例，仍只推进一次 `task_state_version`。

`model:compendium_meta` 保存 `unlocked_count/compendium_revision/compendium_seen_revision`；
`model:compendium_membership` 保存有界的 `crop_definition_id` 永久成员集合。二者只是同一个 ActorRecord
内的逻辑 Model Field，必须共享 Actor WriterClaim、RecordVersion 和 PersistenceLane；Harvest 首次解锁
时与资产、经验、收获计数、Receipt 和 StoredOperationOutcome 一次可靠提交。两者均不得 TTL，也不得
把 Redis Set/Bitmap 的独立写入冒充第二条提交 Lane。具体使用排序数组、稳定 ordinal Bitmap 或固定分区
Protobuf，由容量探针冻结；业务代码只能通过 CompendiumModel Adapter 访问。

`model:pet` 保存第 36 号 `PetState`，必须包含独立 `pet_state_version` 与 `guard_version`。任何实际解锁、
喂食或切换推进前者；只有喂食和真正切换推进后者。`GuardSyncWatermark` **不内嵌** `model:pet`，而是
位于同一 PLAYER Authority Bucket 的 `<tag>:sync:guard:<player_id>` 固定大小 HASH；这样 Dispatcher/
worker 完成来源 Order 时可以同槽原子推进水位，而不会绕过 PlayerActor WriterClaim 修改其缓存模型。

### 4.3 FarmActor Model

候选字段：

```text
model:farm_meta
model:plots
model:guard
model:activity_summary
model:opportunity_header
model:theft_receipt_hotset
```

Presence、JoiningBuffer、FarmChangeVersionRing、Gate 连接与 Snapshot 编码缓存是运行时状态，不进入
ActorRecord。

P0 地块数量不大，`model:plots` 先保持一块；只有真实序列化超过 32～64 KiB 或写字节成为瓶颈时，
才按固定地块范围创建版本化 `plot_bucket_n`。不能未经数据先引入任意分桶。

### 4.4 Model 级版本

内存中至少维护：

```text
actor_dirty_version
model_modified_version[model]
model_saved_version[model]
```

Redis 仍只有一个 Group `record_version`。ModelVersion 用于决定本次序列化哪些 Field，不形成第二套
CAS 真值。

### 4.5 禁止 HGETALL

Claim Lua 只返回元数据、Schema 和有界 Model Manifest。Claim 成功后 Repository 使用已知字段列表
分批 HMGET：

```text
Claim成功
→ Actor仍处于LOADING
→ bounded HMGET(model fields)
→ 全部解码、校验和迁移成功
→ Actor才进入ACTIVE
```

任何 HMGET、解码或 Schema 迁移失败都不能安装半个 Actor。Receipt 按幂等预检需要精确 HGET，
不随激活全量加载。

---

## 5. LoadAndClaimActor

### 5.1 请求输入

```text
ActorAddress
authority_layout_version
ShardWriteFence                    # Holder/Boot/AssignmentRevision/WriterEpoch/LeaseEpoch
claim_operation_id              # 稳定幂等身份
new_writer_claim_id             # 重试必须相同
optional_initial_model_fields
```

### 5.2 小 Claim Lua

脚本只做：

```text
1. 校验所有Key同槽和layout/domain/shard
2. Redis TIME校验ShardWriteAuthority ACTIVE/Lease
3. 校验Holder/BootID/AssignmentRevision/WriterEpoch/LeaseEpoch
4. 若claim_operation_id已完成且new_claim相同，返回原结果
5. 若Actor不存在，按初始化合同创建
6. 更新writer_claim_id、last_claim_operation_id
7. 返回record_version、schema_version、model manifest
```

Claim 元数据变化不推进业务 `record_version`。WriterClaim 不使用 TTL。

P0 Load 阶段只允许“不改变权威字节”的兼容解码。若 Schema 升级需要修改权威状态，Actor 保持
`LOADING/MIGRATION_REQUIRED`，不能把仅在内存中升级后的对象直接安装为 ACTIVE。后续二选一：

```text
无需改写数据
→ 兼容解码后ACTIVE

需要改写数据
→ 在同WriterClaim下执行版本化可靠Migration Commit
→ 明确处理COMMITTED/TIMEOUT_UNKNOWN
→ 成功后ACTIVE
```

首版没有 `CommitActorSchemaMigrationV1` 时就只支持前一种；实现 Agent 不得偷偷在 Load 中写回。

### 5.3 幂等与 CLAIM_UNKNOWN

网络超时后只能使用原：

```text
claim_operation_id + new_writer_claim_id
```

重入或查询；不得生成第三个 Claim。若无法证明 Claim 成败，Actor 保持 `CLAIM_UNKNOWN`，不进入
ACTIVE、不处理业务写。旧 Claim 的迟到 Commit 会被新 Claim Fence 拒绝。

### 5.4 激活 Singleflight

同 Pod 同 Actor 的并发首请求只允许一个 LoadAndClaim 执行，其他请求等待同一结果。Singleflight
只减少重复加载，不提供分布式写权；最终安全仍由 Redis Claim/Fence 保证。

---

## 6. 固定 Lua Script Family

### 6.1 ScriptRegistry

ScriptRegistry 是应用内 Go 模块，不是独立服务：

```text
内嵌不可变Lua源码
→ 对每个Redis Primary按Node维度EnsureLoaded
→ 保存相同源码SHA、脚本版本与node_id加载状态
→ 正常EVALSHA
→ MOVED/ASK或Failover到新Primary后在目标Node EnsureLoaded
→ NOSCRIPT时只在目标Node按同版本重载并有界重试
```

脚本名称与主要用途：

```text
ClaimShardAuthorityV1
RenewShardAuthorityV1
TransferShardAuthorityV1
UnassignShardAuthorityV1
LoadAndClaimActorV1
CommitActorMutationV1
CommitActorMutationWithOrdersV1
CommitParticipantMutationV1
FenceAndClaimActorV1
CreateWorkV1
ClaimWorkV1
RenewWorkLeaseV1
AdvanceWorkV1
CompleteWorkV1
ClaimBucketScanLeaseV1
RenewBucketScanLeaseV1
CompactReceiptV1
ApplyProjectionMutationV1          # 仅Projection Adapter；不与Authority跨Slot原子
```

Redis Cluster Script Cache 是节点级状态，不是整个 Cluster 的共享状态。应用可以按源码缓存全局 SHA，
但“已加载”必须按目标 Primary 维护；Failover 后的新 Primary 不能假设已经拥有旧节点脚本。

不创建一份支持任意 Keys、任意 Mutation、任意 Outbox 的“万能事务 DSL”。

### 6.2 Lua 复杂度边界

禁止：

```text
KEYS / SCAN
Lua内HGETALL
HSCAN全遍历
LRANGE 0 -1
ZRANGEBYSCORE不带LIMIT
按客户端任意长度数组循环
业务Protobuf解析和规则裁决
跨SlotKey
```

脚本只能是 O(1)、O(log N) 或对明确小上限数组的有界循环。所有容量检查必须在任何写入前完成，
禁止先写一半再因超限返回。

### 6.3 ModelMutation

```text
ModelMutation {
  field_name
  expected_modified_version
  protobuf_bytes
  payload_digest
}
```

FieldName 必须来自 Repository 注册表，不接受公网/业务 Payload 直接指定 Redis Field。每个脚本的
Key 数、Mutation 数、Receipt 数和可选 Order 数固定受 LimitProfile 约束。

### 6.4 CommitActorMutationV1

输入至少包含：

```text
ActorAddress / Group断言
ShardWriteFence
expected_writer_claim_id
expected_record_version
commit_request_id
EffectiveIdempotencyKey + payload_hash
ModelMutation[]
StoredOperationOutcome/AuthorityReceipt
SourceOrder[] + 对应DueIndex score       # 0..N，N受LimitProfile硬限制
```

脚本执行顺序：

```text
同槽/Schema/Limit检查
→ Redis TIME检查ShardWriteAuthority
→ Holder/Boot/AssignmentRevision/WriterEpoch/LeaseEpoch
→ WriterClaimID
→ 已有Receipt：同Hash返回原结果，不同Hash报IDEMPOTENCY_CONFLICT
→ RecordVersion
→ Mutation实际字节增量、Field数、全部Receipt/Order容量
→ HSET变化Model/Receipt
→ 写入0..N个Typed SourceOrder并逐项ZADD DueIndex
→ record_version++
→ 更新business_state_digest、updated_at和字节计数
→ 返回COMMITTED(new_record_version)
```

`commit_request_id` 只用于完全相同 Store 请求的传输重试；业务重试继续使用原 operation_id，但重新
构造 Candidate 时必须生成新 commit_request_id。

### 6.5 CommitActorMutationWithOrdersV1

一次来源提交允许 `0..N` 个静态注册的 Typed SourceOrder；`N` 很小且同时受 item/byte 上限约束，
`LimitProfile v0.1` 可先用 `N=4` 做探针，但它不是永久产品数值。脚本必须在任何写入前检查全部 Order
的类型、Key、Payload、计数和字节，再以有界循环一次写入并更新 DueIndex。

这不是万能事务 DSL：OrderType 必须来自注册表，Typed Payload 有固定 Schema，所有 Order 必须属于
同一来源 Authority Bucket，禁止携带任意 Redis Key。Care 等一次事实可以同时产生护理奖励、结果发布
等不同派生需求；这些 Work 生命周期不同，不应绑成无法独立重试的万能 CompositeOrder。

来源派生必须先按重要性分级：

| `SourceOrderDurabilityClass` | 语义 | 是否进入同槽 `source_orders[0..N]` | 容量不足时 |
|---|---|---|---|
| `BUSINESS_REQUIRED` | 不完成会缺失业务效果 | 是 | 整个来源提交失败 |
| `RECOVERY_DISCOVERY_REQUIRED` | 不完成会失去结果发现或可靠恢复入口 | 是 | 整个来源提交失败 |
| `REBUILDABLE_PROJECTION` | 只是可重建查询副本 | 不为每次 Mutation 创建独立必达 Order | 不得反向拒绝核心业务 |
| `BEST_EFFORT` | 统计、调试或非权威提示 | 否 | 允许丢弃并计数 |

P0 映射：

```text
CareRewardOrder                         → BUSINESS_REQUIRED
GuardSyncOrder                          → BUSINESS_REQUIRED
ResultPublishOrder                      → RECOVERY_DISCOVERY_REQUIRED
OpportunitySummaryUpdate                → REBUILDABLE_PROJECTION
TLog/非权威统计/普通通知候选             → BEST_EFFORT
```

前两类才计入 `CommitActorMutationWithOrdersV1` 的强制 `0..N` 预算；任一强制 Order 或 DueIndex
容量不足时，整个来源提交失败。`REBUILDABLE_PROJECTION` 必须随来源提交至少单调推进固定大小的
`projection_dirty_generation`，并使用“每 Actor/ProjectionKind 最多一条可合并 RefreshOrder”或
“显式、版本化、可有界的 Bucket Reconcile 索引”恢复；禁止每次操作无限新增 Projection Order。
在相应恢复机制尚未实现前，不能宣称该 Projection 具备最终重建能力。

Projection 队列或普通通知拥塞不得阻止作物、护理、资产等权威事实提交；实现也不能通过把一个
`BUSINESS_REQUIRED` Order 错标成 Projection 来绕过可靠性。第 32 号必须为每条命令冻结：强制
SourceOrder 最大数量、各 Order 的 DurabilityClass，以及哪类失败会阻止来源事实提交。

### 6.6 CommitParticipantMutationV1

Participant 调用先由 Holder/Actor Runtime 校验当前路由和可信 Caller，再在 Lua 中重复检查当前
Shard/Claim/RecordVersion，并以 `participant_invocation_id + step_hash` 防重。已有 ParticipantReceipt
时返回原结果，不能重复发奖、扣减或完成 Reservation。

### 6.7 Redis 错误分类

| 类别 | 示例 | 处理 |
|---|---|---|
| 明确成功 | Lua 返回 `COMMITTED/ALREADY_COMMITTED` | 安装状态或返回原持久终态 |
| 明确未发送 | 本地连接池无连接、请求未写出 | 可按同 Store 请求有界重试 |
| 明确未执行路由错误 | MOVED/ASK/NOSCRIPT 且脚本未进入执行 | 刷新路由/脚本后同请求重试 |
| `TIMEOUT_UNKNOWN` | 请求可能已到 Redis，响应丢失 | 禁止盲重试，进入 Resolver |
| 合同错误 | CROSSSLOT、Schema、Key断言、Limit冲突 | Fail Closed、告警，不自动改写 |

只有 ErrorClassifier 白名单中的“确定未执行”可以透明重试。Redis 客户端、连接池、KeyBuilder、
ScriptRegistry 和 ErrorClassifier 必须统一，禁止各 Repository 自行猜测。

---

## 7. FenceAndClaimActor 与 Unknown Outcome

### 7.1 为什么需要 Fence

原 Commit 超时后无法仅凭超时判断成败。Resolver 使用同 Actor Authority Slot，把旧 Claim C1 原子
换成 C2，形成迟到请求无法跨越的切点。

### 7.2 请求证据

```text
resolve_operation_id
old_writer_claim_id = C1
new_writer_claim_id = C2
original_idempotency_key
original_payload_hash
before_record_version
before_business_state_digest
expected_writer_epoch / holder fence
```

### 7.3 原子脚本

`FenceAndClaimActorV1` 在同一 Lua 中：

```text
验证ShardWriteAuthority/WriterEpoch/Holder
→ 校验或幂等复用C2
→ 检查持久ResolutionReceipt
→ 原子C1→C2
→ 检查原AuthorityReceipt与payload_hash
→ 比较before_record_version和before_state_digest
→ 写ResolutionReceipt
→ 返回固定结论
```

只有三种结论：

| 观察 | 结论 |
|---|---|
| 原 Receipt 存在且 Hash 相同 | `COMMITTED` |
| Receipt 不存在且业务版本/摘要仍是操作前 | `NOT_COMMITTED` |
| Receipt 不存在但状态发生无法解释的变化 | `RECONCILE_REQUIRED` |

ResolutionReceipt 与 C1→C2 同槽原子提交。后续即使 Actor 又发生新写，同一
`resolve_operation_id` 也返回持久结论，不重新按新状态改判。

### 7.4 两种合法竞争顺序

```text
原CAS先提交：
Commit(C1)成功并写Receipt
→ Fence C1→C2
→ Resolver看到Receipt
→ COMMITTED

Fence先提交：
Fence C1→C2，确认状态仍为before
→ NOT_COMMITTED
→ 迟到Commit(C1)
→ WRITER_CLAIM_MISMATCH
```

Shard 已迁移时由当前 Holder/WriterEpoch 执行 Resolver；旧 Holder 不能自行继续解析并写入。

---

## 8. 幂等 Receipt、ResultDirectory 与保留

### 8.1 EffectiveIdempotencyKey 与 Digest

业务幂等键：

```text
OperationRootRef {
  root_authority_kind
  canonical_root_authority_id
}

EffectiveIdempotencyKey {
  subject_player_id
  command_type
  operation_id
  operation_root
}
```

Redis Field 定位使用：

```text
LP(x) = uint32_big_endian(len(UTF8(x))) || UTF8(x)

operation_digest_bytes = SHA-256(
  LP(canonical_subject_player_id)
  || LP(stable_command_type_name)
  || LP(operation_id)
  || LP(stable_root_authority_kind)
  || LP(canonical_root_authority_id)
)

operation_digest_field = lowercase_hex(operation_digest_bytes)
```

数值 ID 使用无符号十进制 ASCII、禁止符号和前导零；Command/Work/Step 类型使用协议注册表中冻结的
大写稳定标识，不使用本地化显示名。所有 Hash 输入都是明确字段序列，不使用 `fmt.Sprintf` 拼接，
也不直接 Hash Protobuf Marshal bytes。

Value 中仍保存完整 EffectiveIdempotencyKey 和 `payload_hash`。Digest 只是定位，不代替碰撞、错 Root
或错 Payload 校验：

```text
同Key + 同payload_hash   → 返回原结果
同Key + 不同payload_hash → IDEMPOTENCY_CONFLICT
```

### 8.1A StableOpaqueID v1

第 27 号委托给本文的确定性 ID 物理算法冻结为：

```text
StableOpaqueID(namespace, fields...) {
  preimage = LP(namespace) || LP(field1) || ... || LP(fieldN)
  digest   = SHA-256(preimage)
  return "sid1_" || Base64URL_NoPadding(digest)   # 完整32字节，不截断
}
```

固定输入：

```text
workflow_id = StableOpaqueID(
  "farm-workflow-id-v2",
  canonical_subject_player_id,
  stable_command_type_name,
  operation_id,
  stable_root_authority_kind,
  canonical_root_authority_id
)

order_id = StableOpaqueID(
  "farm-source-order-id-v1",
  stable_order_type_name,
  stable_source_authority_kind,
  canonical_source_authority_id,
  source_fact_id,
  canonical_source_fact_version
)

participant_invocation_id = StableOpaqueID(
  "farm-participant-invocation-v1",
  stable_work_type_name,
  work_id,
  stable_participant_step_name,
  canonical_step_instance
)

unlock_coin_reservation_id = StableOpaqueID(
  "farm-unlock-coin-reservation-id-v1",
  unlock_plot_workflow_id
)

seed_reservation_id = StableOpaqueID(
  "farm-seed-reservation-id-v1",
  plant_workflow_id
)

crop_cycle_id = StableOpaqueID(
  "farm-crop-cycle-id-v1",
  plant_workflow_id
)

care_incident_id = StableOpaqueID(
  "farm-care-incident-id-v1",
  crop_cycle_id,
  stable_care_type_name,
  canonical_ordinal
)

theft_attempt_id = StableOpaqueID(
  "farm-theft-attempt-id-v1",
  canonical_target_farm_id,
  crop_cycle_id,
  canonical_visitor_player_id
)

theft_settlement_id = StableOpaqueID(
  "farm-theft-settlement-id-v1",
  theft_workflow_id
)

friend_relation_id = StableOpaqueID(
  "farm-friend-relation-id-v1",
  canonical_low_player_id,
  canonical_high_player_id
)

friend_quota_reservation_id = StableOpaqueID(
  "farm-friend-quota-reservation-id-v1",
  add_friend_workflow_id,
  canonical_quota_player_id,
  friend_relation_id
)

share_token_validation_receipt_id = StableOpaqueID(
  "farm-share-token-validation-receipt-id-v1",
  add_friend_workflow_id,
  canonical_inviter_player_id,
  canonical_redeemer_player_id,
  canonical_token_generation
)
```

所有 `canonical_*` 整数使用无前导零的十进制 ASCII；枚举使用注册表冻结的大写 ASCII 名称。上述
namespace 是协议的一部分，禁止实现自行改名、复用通用 UUID 或截断 Digest。

`stable_source_authority_kind` 对 Actor 来源取 `PLAYER_ACTOR/FARM_ACTOR`，对第 33 号 Pair 来源取 `FRIEND_PAIR`。
`canonical_source_authority_id` 对 Actor 是 actor_id，对 Pair 是 canonical pair/relation ID；非 Actor 来源
不得伪造 RouteDomain。好友 namespace 的跨语言黄金值由第 33A 号好友权威物理合同继续冻结；
实现必须同时导入两份黄金向量，不得自行改名 namespace。

黄金样例：

```text
StableOpaqueID("farm-workflow-id-v2", "10001", "PLANT", "op-0001",
               "FARM_ACTOR", "10001")
= sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q

StableOpaqueID("farm-source-order-id-v1", "CARE_REWARD", "FARM_ACTOR", "20002",
               "care-plot-3-cycle-7", "12")
= sid1_EqEZHYwCxXtF51d-UZlwKyxP4RMAXSaYCDAQMDkkbzI

StableOpaqueID("farm-source-order-id-v1", "GUARD_SYNC_ORDER", "PLAYER_ACTOR", "10001",
               "PET_GUARD_SLOT", "7")
= sid1_L5lEIo2wh82J0fBZYWtvm3BwxblL0NV8s_p9agW7GXg

StableOpaqueID("farm-participant-invocation-v1", "PLANT",
               "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q",
               "RESERVE_SEED", "1")
= sid1_9WktgxCvxV9zV7eR6LkxE9p5mSm0rfNFrfQMJIdZlSY

StableOpaqueID("farm-workflow-id-v2", "10001", "UNLOCK_PLOT", "op-unlock-0001",
               "FARM_ACTOR", "10001")
= sid1_UxPZD3u5t4O3VczETFXvXyR2QkXmHAZx6Geck87do1s

StableOpaqueID("farm-workflow-id-v2", "10001", "THEFT", "op-theft-0001",
               "FRIEND_PAIR", "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to")
= sid1_0wXIbmzukfEx6A60-oLe78bM6zPRJAL-SnqbwM-Y3mE

StableOpaqueID("farm-unlock-coin-reservation-id-v1",
               "sid1_UxPZD3u5t4O3VczETFXvXyR2QkXmHAZx6Geck87do1s")
= sid1_8QJEXKpUJqlPI_gv7kAWjxhsBkntcIUcLA9r7K53hBM

StableOpaqueID("farm-seed-reservation-id-v1",
               "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q")
= sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo

StableOpaqueID("farm-crop-cycle-id-v1",
               "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q")
= sid1_KDm_gSYlKXdcw1DATIGdnP1IJHPjHyK1VQUU11max00

StableOpaqueID("farm-care-incident-id-v1",
               "sid1_KDm_gSYlKXdcw1DATIGdnP1IJHPjHyK1VQUU11max00",
               "WATER", "1")
= sid1_DURawr0H1WHVobo4ivmSvQM7k5uhlPVY3Qbf_tVCXVw

StableOpaqueID("farm-theft-attempt-id-v1", "20002",
               "sid1_KDm_gSYlKXdcw1DATIGdnP1IJHPjHyK1VQUU11max00", "10001")
= sid1_TKE2-YZ3x8lUCWJSbTCeyx3YzF4g4rN52d-8ljiDMXA

StableOpaqueID("farm-theft-settlement-id-v1",
               "sid1_0wXIbmzukfEx6A60-oLe78bM6zPRJAL-SnqbwM-Y3mE")
= sid1_X-s0wd3EKTjjviu2MK4QMVVoGCtr8kvl8n6Nwe7XLuc
```

Go、H5 测试工具和 Lua 夹具必须共享黄金测试。WorkRecord/Receipt 仍保存完整来源身份与 payload_hash；
若同 StableOpaqueID 命中不同完整身份，必须 Fail Closed 并告警。该算法不使用 RouteSnapshot HashSeed，
`workflow_schema_version` 也不进入 WorkflowID 输入。

### 8.1B WorkRefV1 与 ReceiptRefV1 可逆编码

`StableOpaqueID` 用于确定性身份；DueIndex/receipt-due 的成员则必须可逆解码，不能再 Hash 一次。
两者统一复用第 8.1 节的 `LP(x)=uint32_big_endian(len(UTF8(x))) || UTF8(x)`：

```text
EncodeWorkRefV1(work_type, work_id) =
  "wr1_" || Base64URL_NoPadding(LP(work_type) || LP(work_id))

EncodeReceiptRefV1(actor_id, receipt_kind, operation_digest) =
  "rr1_" || Base64URL_NoPadding(
    LP(canonical_actor_id)
    || LP(stable_receipt_kind_name)
    || LP(lowercase_hex_operation_digest)
  )
```

规范输入：

- `work_type/receipt_kind` 使用注册表冻结的大写 ASCII 名称，`[A-Z0-9_]{1,64}`；
- `work_id` 使用已经验证的稳定 WorkID，ASCII `1..128` 字节；
- `actor_id` 使用无前导零的无符号十进制 ASCII，`1..20` 字节；
- `operation_digest` 恰好为 64 个小写十六进制字符；
- 解码器必须检查前缀、字段数量、每字段上限、UTF-8/ASCII 规范，并执行“解码后重新编码完全相等”；
- 两种 Ref 只是已知 Bucket 内的定位格式，不是认证 Token，不能接受客户端提供。

黄金样例：

```text
EncodeWorkRefV1(
  "CARE_REWARD",
  "sid1_EqEZHYwCxXtF51d-UZlwKyxP4RMAXSaYCDAQMDkkbzI"
)
= wr1_AAAAC0NBUkVfUkVXQVJEAAAAMHNpZDFfRXFFWkhZd0N4WHRGNTFkLVVabHdLeXhQNFJNQVhTYVlDREFRTURra2J6SQ

operation_digest("10001", "CARE", "op-care-0001", "FARM_ACTOR", "20002")
= 725d41258a7f25032ea34722b88eb1dbcb6a9877bec328a037b3e847db021e0d

EncodeReceiptRefV1(
  "20002",
  "AUTHORITY_RECEIPT",
  "725d41258a7f25032ea34722b88eb1dbcb6a9877bec328a037b3e847db021e0d"
)
= rr1_AAAABTIwMDAyAAAAEUFVVEhPUklUWV9SRUNFSVBUAAAAQDcyNWQ0MTI1OGE3ZjI1MDMyZWEzNDcyMmI4OGViMWRiY2I2YTk4NzdiZWMzMjhhMDM3YjNlODQ3ZGIwMjFlMGQ
```

Go Repository、Lua 测试夹具、Worker 和压测工具必须共享这些黄金样例。`WorkRefV1` 的
`placement_kind/bucket_version/bucket_id` 来自扫描上下文；加载 WorkRecord 后必须再验证完整
WorkAddress。`ReceiptRefV1` 同样只在已知 Authority Bucket 中使用。

### 8.2 三类 Receipt

| 数据 | 位置 | 作用 |
|---|---|---|
| `AuthorityReceipt` | 来源 Actor HASH 的精确 Field | 业务提交与防重真值 |
| `ResolutionReceipt` | 来源 Actor HASH 的精确 Field | 固定 Unknown 解析结论 |
| `ResultDirectory` | Projection Redis，按玩家查询 | 让玩家按 operation_id 找到结果位置/最小结果 |

ResultDirectory 是查询副本，不是第二份业务权威。
`CareOperationReceipt` 是 `AuthorityReceipt` 的场景化 Typed Outcome，使用来源 Farm Actor 的
`receipt:op:<operation_digest>` Field，并冻结 `receipt_kind=CARE_OPERATION_RECEIPT`；它不是第四类
Receipt，也绝不能写入 `receipt:resolve:*`。PlayerActor 发奖侧的 `CareRewardReceipt` 继续属于对应
Participant/Reward Receipt，三者职责不得混用。

### 8.3 跨 Slot 结果发布

例如 A 偷 B 农场：

```text
FarmActor(B)同槽提交：
  农场状态
  TheftAttemptReceipt
  AuthorityReceipt/Outcome
  ResultPublishOrder
  DueIndex

→ DirectDispatcher立即尝试
→ 写A的ResultDirectory
→ 失败时farm-worker从DueIndex恢复
```

B 的 FARM Authority Slot 与 A 的 PLAYER Projection Key 不同，禁止用跨 Slot Lua 假装原子。来源
Receipt/Order 保证结果发布可以安全重试。

`ResultDirectory` 条目至少保存 `original_command_type/operation_digest/OperationRootRef/source
WorkAddress或AuthorityReceipt引用/final_status_class`。它可以丢失后重建，因此不是业务裁决真值；
第 28 号公开查询携带原业务目标或服务端不透明 Locator，由服务端重新派生/校验 Root。Directory 缺失时
仍不能猜目标、猜 Root 或直接返回 `EXPIRED`。查询结果统一为：

```text
能按确定性地址查到未完成Workflow/Order/Unknown证据
→ PROCESSING

查到终态AuthorityReceipt/ResultDirectory
→ COMPLETED

查到Expired Evidence
→ EXPIRED

Directory缺失，且请求字段不足以定位来源Authority或可靠Work
→ NOT_FOUND
```

`NOT_FOUND` 不证明原操作未执行。以 Care 为例，H5 Journal 保存原 `target_farm_id + Payload`，查询入口
据此派生 canonical FRIEND_PAIR Root；若 ResultDirectory 尚未发布，则按可靠 Locator 或来源地址查找，
仍无法找到证据时返回 NOT_FOUND。H5 必须 Fresh Join 原目标并以原 operation_id/原Payload重发，由来源
Receipt 收敛。

第 28/29 号已冻结 `server_operation_locator` 的逻辑合同：它使用带完整性保护的不透明封装指向
已可靠创建的 Workflow、SourceOrder 或 Authority Receipt，可以无状态签发，不需要再建一张
Locator 权威表。它不是 Projection，也不是授权。密钥轮换、`not_after_ms`、可接受旧 key_id 窗口与
精确封装字节留给安全平台探针；只有 Expired Evidence 或其他可访问权威证据才能证明
`EXPIRED`。

### 8.4 Full → Dedup Tombstone → Expired Evidence

`LimitProfile v0.1`：

```text
完成后0～24小时
→ 保存Full Outcome，支持客户端恢复最小原结果

24～72小时
→ 压缩为Dedup Tombstone，继续证明已处理及payload_hash

超过72小时
→ 迁移为Expired Evidence/Archive
→ GetOperationResult可证明EXPIRED
→ Authority Commit仍拒绝旧operation重新执行
```

上述小时数是参数候选；稳定语义是“删除 Full 结果不能同时删除防重与 EXPIRED 证明”。Full Outcome
不保存完整请求 Payload；Dedup Tombstone/Expired Evidence 不保存结果 Payload，最少保留：

```text
operation_digest
完整EffectiveIdempotencyKey或可校验引用
payload_hash
first_seen_at_ms
expired_at_ms
final_status_class
```

当前 `operation_id` 是不透明字符串，没有服务端可验证的签发时间/代次，因此仅凭“72 小时前的桶已经
清理”不能证明某个 ID 已过期。P0 禁止最终删除 Expired Evidence；长期物理位置可以是更便宜的
Authority Archive，但在 Archive/查询/Commit 防重链实测前，不得把不存在映射成 `EXPIRED`，也不得
让相同 operation_id 再次生效。若未来引入服务端可验证的 operation namespace/水位，可用新协议版本
重新评审最终删除。

### 8.4A Receipt 生命周期索引

每个 Authority Bucket 增加同槽有界 ZSET：

```text
<tag>:receipt-due

score  = next_transition_at_ms
member = EncodeReceiptRefV1(actor_id, receipt_kind, operation_digest)
```

`CompactReceiptV1` 使用 Redis TIME 并在同槽原子完成：

```text
加载精确Receipt Field并复核当前阶段/到期时间
→ Full压缩为Dedup Tombstone
  或Tombstone迁移为Expired Evidence/Archive Intent
→ 更新receipt_count/receipt_bytes
→ 更新或移除receipt-due成员
```

维护器只按 Bucket 对 `receipt-due` 做带 LIMIT 的到期扫描，禁止 HSCAN 热 ActorRecord。索引缺项可由
有界 Reconcile 修复，但不能在索引缺失时直接删除 Receipt。

P0 最简单实现是把 Expired Evidence 继续保留为来源 Actor HASH 中的紧凑 Field。若未来迁往外部
Archive，`CompactReceiptV1` 只能先同槽创建 `ReceiptArchiveOrder` 并保留原 Tombstone；Archive 写入、
在线查询和 Authority Commit 防重链确认可用后，才能把原 Field 缩成带 `archive_ref` 的证据。禁止
“先删 Tombstone，再异步归档”。

### 8.5 不适用普通 TTL 的事实

- 未完成 Workflow/Order；
- 未完成 Unknown/Resolution；
- 邮件领取、任务周期奖励、永久图鉴/土地/功能解锁；
- 付费、稀有、唯一资产；
- 当前 CropCycle 的 TheftAttemptReceipt；
- 其他领域明确要求永久防重的事实。

这些由领域状态或专用归档合同管理，不能因普通 OperationResult 到期而失去正确性。

### 8.6 Receipt 容量原则

Model 状态可以合并写，但多个 operation 的 Receipt 语义不能合并消失。一次 HMSET 可以批量保存多个
紧凑 Receipt。Receipt 达到上限时先执行符合保留期的压缩/归档；仍然满则拒绝新业务，禁止随机
淘汰未过期 Receipt。

### 8.7 Expired Evidence 的生产闭环

P0 保留 Expired Evidence 能保证正确性，但不能被误解为生产容量已经闭环。按容量候选
`6 亿次权威操作/天 × 128 B`，仅原始 Evidence Value 就约 `76.8 GB/天`，尚未包含 Redis Hash Field、
对象头、Allocator 碎片、Replica、备份和索引。

正式生产 Freeze 前必须完成以下方案之一：

1. **可验证 Operation Namespace**：新协议携带服务端可验证的签发窗口/代次，使系统能证明某个窗口
   已关闭、无在途 Workflow/Unknown，随后以窗口水位拒绝旧重放；
2. **Authority Archive**：归档同时支持 `GetOperationResult`、Authority Commit 防重、故障恢复、
   归档 ACK 和可证明负查询。它不能只是 BI/离线报表库。

在二者均未落地前，可以实现热 Receipt、`CompactReceiptV1` 和短期 Evidence，但不得最终删除最后一份
防重证明，也不得宣称 3000 万 DAU 的长期存储成本已经通过。

---

## 9. Workflow、Order 与 DueIndex 的物理合同

### 9.1 两种可靠工作记录

本文延续第 27、29、30 号的区分，不把所有异步动作都做成同一种“任务”：

| 类型 | 典型场景 | 放置位置 | 谁首次推进 | 谁故障恢复 |
|---|---|---|---|---|
| 来源局部强制 `Order` | 护理奖励、守护同步、结果发布 | 与来源 Actor 同一个 Authority Bucket | 当前业务进程内 `DirectDispatcher` | `farm-worker` |
| 独立 `TypedWorkflow` | Plant、Harvest、Theft 跨 Actor 多步骤流程 | 稳定 `WorkflowBucket` | FirstHop Coordinator | `farm-worker/WorkflowRunner` |

`Order` 是“来源事实提交后还必须完成的一项后续工作”；`Coordinator` 是运行时推进者；
`WorkflowRecord` 是跨多个 Participant 的持久进度。三者不能混为一个概念。

好友机会摘要等 `REBUILDABLE_PROJECTION` 不属于“每次来源事实都必须新建一条强制 Order”的集合；
它们按第 6.5 节使用 dirty generation、可合并 RefreshOrder 或显式 Reconcile 机制。

来源局部 Order 的原子边界：

```text
AuthorityCommit(
  SourceMutation,
  SourceReceipt,
  NewOrders[0..N],
  DueIndexEntries[0..N]
)
```

如果 `BUSINESS_REQUIRED/RECOVERY_DISCOVERY_REQUIRED` Order 或 DueIndex 因容量不足无法写入，整个
来源提交失败；禁止先提交来源状态，再投递内存队列或 MQ。否则进程恰好在两步之间崩溃时，后续
工作会永久丢失。Projection/BestEffort 不得混入该强制列表反向降低核心业务可用性。

### 9.2 WorkRecord 最小公共头

Order 与 Workflow 可以拥有不同的 typed payload，但公共控制头至少包含：

```text
WorkRecord {
  WorkAddress {
    work_id
    work_type
    placement_kind          // SOURCE_PERSISTENCE_BUCKET | INDEPENDENT_WORK_BUCKET
    bucket_version
    bucket_id
    optional SourceAuthorityRef {
      authority_kind
      canonical_authority_id
      authority_layout_version
      optional actor_route_domain
    }
  }
  optional source_order_durability_class  // 仅SourceOrder；必须与静态注册表一致
  subject_player_id
  operation_id
  payload_hash

  state
  work_version
  current_step
  typed_payload

  next_due_at_ms
  retry_count
  last_error_class

  lease_holder_id
  lease_epoch
  lease_token_hash
  lease_expire_at_ms

  created_at_ms
  updated_at_ms
  completed_at_ms
}
```

`SOURCE_PERSISTENCE_BUCKET` 的 SourceAuthorityRef 必填。Actor 来源由 RouteDomain 与 ID 交叉复核；
FRIEND_PAIR 等非 Actor 来源禁止填写 Actor RouteDomain。Repository 必须依据 WorkType 静态注册表验证
placement、允许来源 Kind、TypedPayload 和 ParticipantStep，不能让调用方用任意字段组合选择 Redis Slot。

`work_version` 只在业务步骤或业务状态发生变化时增加。仅 Claim、Renew Lease 不增加业务版本，避免
Worker 心跳制造无意义冲突和写放大。

### 9.2A GuardSyncOrder 与固定大小同步水位

`GUARD_SYNC_ORDER` 的来源身份固定为：

```text
source_authority_kind = PLAYER_ACTOR
source_fact_id        = PET_GUARD_SLOT
source_fact_version   = PetState.guard_version
durability_class      = BUSINESS_REQUIRED
```

`<tag>:sync:guard:<player_id>` 是一个不设置普通 TTL 的小 HASH：

```text
schema_version
last_applied_guard_version
last_applied_snapshot_hash
last_terminal_status
updated_at_ms
```

`CompleteGuardSyncOrderV1` 只接收静态注册的 `GUARD_SYNC_ORDER`，并在同一个 PLAYER Slot 内原子执行：

```text
校验Order WorkAddress、source player、work_version和Participant Result Hash
→ 删除/更新DueIndex成员
→ 将Order推进到APPLIED或SUPERSEDED
→ 对GuardSyncWatermark做单调比较后更新
→ 返回完成后的work_version与watermark
```

规则如下：

- APPLIED 的版本高于水位时推进版本与 Hash；同版本同 Hash 幂等；同版本不同 Hash Fail Closed；
- SUPERSEDED 可以完成旧 Order，但不能把已应用水位降低或替换高版本 Hash；
- 完成脚本不修改 `<tag>:actor:<player_id>`、不推进 Player `record_version`，也不要求取得 PlayerActor
  WriterClaim，避免 worker 绕过 Actor 后让内存 `model:pet` 变旧；
- Order 可按通用终态保留策略 GC，但 Watermark 保留到玩家删除/整域迁移，确保 `GET_PET_STATE` 仍能
  判断当前 `guard_version` 是 APPLIED 还是 PENDING；
- Watermark 丢失或读取未知时只能保守返回 PENDING/FAULT，不能假设 Farm 已应用。

### 9.3 DueIndex 只是发现索引

每个 Authority Bucket/Workflow Bucket 各自维护有界 `ZSET DueIndex`：

```text
score  = next_due_at_ms
member = EncodeWorkRefV1(work_type, work_id)
```

扫描发生在已知 Bucket 中，所以 `placement_kind/bucket_version/bucket_id` 来自扫描上下文；
`EncodeWorkRefV1` 仍必须包含 `work_type + work_id`，因为来源 Order Key 是
`<tag>:order:<work_type>:<work_id>`。加载 WorkRecord 后再校验其中完整 WorkAddress 与扫描上下文一致。
禁止只拿裸 `work_id` 猜 WorkType、遍历多个 Bucket 或发给 Participant。

它回答“哪些工作已经到期、可以尝试”，不保存最终业务真值。`WorkRecord` 才是状态真值。因此：

- 创建、重试、完成 Work 时，WorkRecord 与 DueIndex 必须由同槽 Lua 一起修改；
- Worker 只按 Bucket 分片执行 `ZRANGEBYSCORE ... LIMIT`，禁止全局 `SCAN`；
- Index 缺项可由 Reconcile 根据 WorkRecord 重建；
- Index 多项或旧项可在 Claim 时校验 WorkRecord 后清理；
- 未完成 WorkRecord 不能因为 TTL/LRU 消失。

### 9.4 DirectDispatcher 快路径

`DirectDispatcher` 是产生 Order 的业务进程内组件，不是独立服务，也不是 MQ：

```text
AuthorityCommit成功
→ 将完整WorkAddress放入进程内有界队列
→ Dispatcher读取WorkRecord或使用提交结果中的确定性参数
→ 通过Unary gRPC调用Participant
→ 成功后CompleteWork
```

这个队列只降低正常路径延迟，Redis 中的 WorkRecord + DueIndex 才保证可恢复。队列满、进程退出或
RPC 失败不会丢工作，`farm-worker` 会在到期后接管。Dispatcher 不持有跨进程 WorkLease；它的完成
提交仍需校验 `work_id/work_version`，Participant 还需以 Receipt 防重。

### 9.5 Worker 恢复路径

```text
Worker扫描自己负责的Bucket DueIndex
→ ClaimWork Lua使用Redis TIME判断到期
→ 原子写lease_holder/lease_epoch/token/expire_at
→ 同时把DueIndex score移到lease_expire_at
→ WorkerRecoveryIngress校验当前Lease与work_version
→ Actor Participant执行一个有界Turn并写ParticipantReceipt
→ Worker推进下一步、重试或Complete
```

把 score 临时改成 Lease 到期时间，可以让 Worker 崩溃后该 Work 自动重新出现。续租只修改 Lease
到期时间和 DueIndex score。任何参与者最终都以 `ParticipantReceipt` 判断是否已经完成该步骤，不能
只相信 Worker 的内存状态。

多 Worker Pod 的 Bucket 扫描归属采用“稳定候选分配 + Redis Scan Lease”：服务发现得到当前 Worker
实例集合后，以版本化 Rendezvous Hash 计算每个 Authority/Workflow Bucket 的首选扫描者；首选实例仍需
在对应 Bucket 的 `<tag>:scan-lease` 上用 Redis TIME 取得短 Lease 才能扫描。成员视图短暂不一致时可能
有人重复竞争 Lease，但同一时刻只有有效 Lease 持有者执行该 Bucket 扫描；Work 自身还要再取得 Work
Lease。Worker 退出后 Scan Lease 到期，其他实例接管。禁止每个 Worker 扫描全部 Bucket，也禁止用
全局 Redis SCAN 发现 Bucket。

### 9.6 MQ 的位置

P0 正确性链路不要求 MQ：

```text
权威来源提交
→ 同槽Order/Workflow
→ DirectDispatcher
→ DueIndex + farm-worker恢复
```

未来 MQ 只可作为领域事件的解耦/加速通道，例如 BI、通知候选、非权威统计；即使 MQ 丢消息，仍应
能由权威 Outbox/Order 重发。禁止让 MQ 消费位点成为资产、作物或 Workflow 的唯一恢复事实。

---

## 10. PlayerPersistenceLane 与 Redis 写放大控制

### 10.1 为什么有状态 Actor 仍然有价值

Actor 不只是“替代数据库锁”。它在同一 Actor 内提供：

- 单一裁决顺序与内存中的热状态；
- 多个连续修改的聚合与脏 Model 合并；
- Receipt、Reservation、Workflow Participant 的本地幂等；
- 与不同 Actor 之间的大规模并行；
- Presence、Snapshot 缓存和版本变化的低成本复用。

因此，即使关键步骤使用同步可靠提交，有状态 Actor 仍能避免每个命令都重新加载整个玩家/农场并
在数据库层争锁。

### 10.2 单一 Persistence Lane

每个 PlayerActor 的以下写入必须进入同一条逻辑 Persistence Lane：

```text
IDLE
├── SOFT_SAVE_INFLIGHT
├── DURABLE_SAVE_INFLIGHT
├── RESOLVING_UNKNOWN
└── DRAINING
```

包括：

- SoftSave；
- Durability Barrier；
- ForceFlush；
- UnknownOutcomeResolve；
- WriterClaim/Fence 相关写入。

关键不变量：低版本异步保存不能晚到并覆盖高版本可靠提交。若 SoftSave(v10) 正在执行，而 Actor 已
运行到 v11，随后 Plant 需要可靠预留，则 Barrier 必须先等待 v10 结果明确，再把最新内存状态与本次
Reservation/Receipt 一起提交为新版本。

### 10.3 Soft 路径准入

首版默认所有业务写都可以先按 `CRASH_DURABLE` 实现。只有真实压测证明同步写成为主要瓶颈，并且
CommandCatalog 明确白名单后，才开启 `ACTOR_SOFT`。候选只包括纯 Player、低价值、可接受明确 RPO
的普通资产变化；以下禁止走普通写后返回：

- FarmActor 的地块、CropCycle、护理、偷菜、FarmVersion 和公开变化；
- Plant、Theft 等跨 Actor Participant 步骤；
- 经验/等级、永久解锁、付费/唯一资产；
- 邮件、任务领奖与其他要求 Receipt 原子防重的关键结果。

Soft 路径采用 UC 思路的适配版本：

```text
Actor内存修改
→ 设置Model DirtyFlag
→ ActorKey进入去重SoftSyncQueue
→ 50～200ms候选合并窗口
→ 仅序列化脏Model
→ Fence + RecordVersion检查后一次多Field提交
```

不能照搬固定“每秒保存 N 个 Actor”的数字；令牌速率、批量大小与窗口都由真实 Redis p99、网络字节、
Dirty Age 和业务 RPO 决定。

### 10.4 Durability Barrier

关键步骤前调用：

```text
EnsureDurable(
  idempotency_key,
  required_current_version,
  durable_mutation,
  participant_receipt,
  optional_orders[0..N]
)
```

这里的 `N` 与第 6.5 节 `CommitActorMutationWithOrdersV1` 使用同一版本化 `LimitProfile`；Barrier
不能另行维护一套“最多一个 Order”的物理限制，也不能携带任意 Key 或未注册 WorkType。

一次 Barrier 提交必须包含：

- 当前 PlayerActor 尚未落库的相关软状态；
- 本次关键资产变化或 Reservation；
- Reward/Charge/Participant Receipt；
- 必要的来源 Order 与 DueIndex。

例如“刚异步购买种子，随后立即 Plant”，Plant 不得只落一个 SeedReservation；Barrier 必须把尚未
落库的购买结果一并可靠提交后，FarmActor 才能创建 CropCycle。

### 10.5 写放大观测

至少记录：

```text
soft_mutation_total
soft_save_total
soft_save_model_fields_total
soft_save_bytes_total
soft_dirty_age_ms
soft_queue_items / soft_queue_bytes
durability_barrier_total / latency
force_flush_total

soft_coalescing_ratio = soft_mutation_total / soft_save_total
actual_write_amplification = redis_atomic_script_total / committed_business_operation_total
```

Model 可以合并保存，Receipt 不能合并消失；它们可以在同一次 HMSET/Lua 中批量落下，但必须保持每个
operation 的独立防重语义。

---

## 11. 三类 Redis、查询副本与好友机会

### 11.1 逻辑分类而非必须三套集群

| 逻辑用途 | 主要数据 | 丢失后果 | P0 处理 |
|---|---|---|---|
| Authority | Player/Farm、Receipt、Workflow、Order、Shard 写权 | 资产/作物错误或流程丢失 | 必须持久化探针、`noeviction`、Fence、备份/容灾 |
| Runtime | Ticket、SessionRoute、限流、好友连续导航、短期会话状态 | 重登、重新 Join 或重新计算 | TTL、有界、允许重建 |
| Projection | ResultDirectory、好友机会、红点、排行、公开资料 | 查询暂时旧或缺失 | 异步更新、允许重建、最终操作再查权威 |

P0 可共用一套公司 Redis，但必须至少拆分：Repository 接口、Key 前缀、ACL、连接池/超时预算、指标与
容量配额。生产上如果 Authority 与 Runtime 的故障/驱逐策略冲突，或查询流量影响权威 Lua p99，再按
证据拆分物理集群。

### 11.2 什么叫查询副本

查询副本是从权威事实计算出的、为了批量读取而保存的“小答案”。它不是第二个裁判：

Farm AuthorityRecord 内先保存一个固定大小、与来源事实同提交的派生头：

```text
FarmOpportunityHeader {
  source_farm_version
  opportunity_dirty_generation
  next_opportunity_at_ms
  candidate_mask
}
```

它使用同一 Actor HASH 中的有界 `model:farm_opportunity_header`（或 Farm 模型内等价固定字段），不新建
跨槽 Key，也不写无界历史。任何改变公开互动机会的 Authority Lua 必须从同一 CandidateState 更新
Header 和 dirty generation。`candidate_mask` 只表示 owner-independent 候选；时间经过、好友关系、
个人 AttemptReceipt 和当前配额仍可能使它过时，因此它不能代替精确裁决。

```text
FarmActor权威状态
→ FarmOpportunityHeader + opportunity_dirty_generation++
→ 可合并OpportunitySummaryRefreshOrder / 有界Reconcile
→ FarmOpportunitySummary（Projection Redis）

好友列表/导航
→ 批量读取Summary快速找候选
→ 对当前页或Top候选调用FarmReadAPI.BatchEvaluate
→ FarmActor/权威读最终判断能否操作
```

这样打开 300 人好友列表时，不需要激活 300 个 FarmActor，也不需要读取 300 份完整农场记录。
Projection 延迟最多影响提示及时性，不能让不合法偷菜或护理成功。

### 11.3 FarmOpportunitySummary

候选字段：

```text
FarmOpportunitySummary {
  farm_id
  summary_revision
  source_opportunity_generation
  farm_version_at_build
  opportunity_mask          // HAS_STEALABLE / NEED_WATER / HAS_PEST ...
  next_opportunity_at_ms
  preview_crop_id
  updated_at_ms
}
```

Summary 只描述“这座农场存在某类公开机会”，不保存“玩家 A 当前是否还能偷”。后者是个性化事实，
由当前 CropCycle 的 `TheftAttemptReceipt(visitor_id)`、好友关系、配额和 FarmActor 状态精确判断。

`source_opportunity_generation` 表示该 Summary 已经构建到哪一代 Authority Header。Projection
Writer 从 Authority 读到 generation=`G` 后重建，只能以 `G` 或更高代次覆盖旧 Summary；
如果构建期间 Authority 又把 required generation 推进到 `G+1`，本次完成不能清除后续
刷新需求，而应继续合并重建。`summary_revision` 是查询副本自身的单调版本，不取代
Authority generation。

任何会改变公开机会的权威提交，都应原子推进固定大小的 `opportunity_dirty_generation`；同一
Farm/ProjectionKind 的刷新工作必须合并，不能每次操作都新增一条不可缺失 Order：

- 种植或收获改变下一次机会；
- 作物按 `mature_at` 派生成熟；
- 第一个好友浇水后 `NEED_WATER` 消失；
- 虫害被处理后 `HAS_PEST` 消失；
- 可偷总额度耗尽或主人收获后 `HAS_STEALABLE` 消失。

成熟正确性不依赖 Scheduler。查询时若 `next_opportunity_at_ms <= trusted_now_ms`，即使 Summary 尚未
异步刷新，也把它作为待精确检查候选；FarmReadAPI 最终使用 `now >= mature_at` 判断。Scheduler 只
用于让提醒更及时。

### 11.4 好友列表查询

```text
H5打开好友页
→ player-svr/FriendReadAPI读取FriendIndex候选
→ 批量验证当前页canonical Pair仍ACTIVE并按需补位
→ PlayerReadAPI批量读取公开资料
→ farm-svr/FarmOpportunityReadAPI批量MGET当前页Summary
→ FarmReadAPI.BatchEvaluate当前页全部有效好友（默认20）
→ 返回精确action_mask；单卡超时为UNKNOWN而不是NONE
```

当前页默认 20 人、硬上限候选 50。Cursor 绑定 viewer、sort_mode 和 FriendIndex roster revision；revision
变化时刷新第一页，不能混用新旧 roster 继续翻页。好友关系权威由 Player 侧 FriendshipPairAuthority
读取；FriendIndex 只是候选。P0 不创建 `query-svr`。读模块可与 player-svr/farm-svr 共部署，未来只有在
查询流量、发布节奏或资源模型明显独立时再拆 Deployment。

当前页全部卡片做精确 BatchEvaluate，是为了满足好友页“当前返回标识准确”的产品要求；Summary 仍用于
粗筛、互动排序和右下角 Top 候选，不能替代 viewer 维度的 AttemptReceipt/CareIncident 检查。
BatchEvaluate 必须按 Route/Redis 节点分组并行读取 Authority ReadPath，不得逐卡激活 FarmActor。

### 11.5 连续右下角导航

用户每次回到自己农场后需要得到“下一个可访问好友”，不应每次都从 300 人好友列表头部重扫。
Runtime Redis 保存短期、允许丢失的：

```text
FriendOpportunityNavigation {
  viewer_player_id
  roster_revision
  scan_cursor
  visited_farm_ids
  candidate_window[]
  expire_at_ms
}
```

流程：

```text
首次GET_NEXT_FRIEND_OPPORTUNITY
→ 每批扫描32～64个好友Summary
→ 对Top候选BatchEvaluate
→ 保留最多8个候选窗口
→ 客户端只展示1个

玩家进入推荐农场并完成SUBSCRIPTION_READY
→ Gate按ActiveSubscription Fence最佳努力标记本轮已访问
→ 不要求偷菜或护理成功

回自己农场再次GET_NEXT_FRIEND_OPPORTUNITY
→ 先精确复核候选窗口中的下一个
→ 不够再继续scan_cursor
```

当前候选仍有效且未 visited 时优先复用，避免同一短期窗口内反复扫描；候选失效则继续检查窗口中的
下一项。visited 更新只允许来自服务端刚激活的好友 ActiveSubscription，按
`viewer_player_id + active_session_epoch + target_farm_id + subscription_id` 做 Fence 和幂等；H5 不提交
Navigation cursor、visited 集合或单独的“导航已访问”命令。

Navigation 丢失或 visited 更新失败只会重新扫描、偶尔重复推荐；不会重复奖励或改变好友关系，也不能
回滚已经 Ready 的 Join。每日访问任务仍由独立的 `CONFIRM_FRIEND_VISIT` 可靠命令推进。离线好友不接收
逐人持久 Inbox，避免一座农场成熟向 300 个好友扇出 300 次权威写。

### 11.6 ResultDirectory 与其他副本放在哪里

这些副本保存在 Projection Redis，由所属领域的读模块访问，不跟 Actor 内存一起迁移：

| 副本 | 读取模块 | 用途 | 最终权威 |
|---|---|---|---|
| ResultDirectory | player-svr OperationResultRead | 根据 operation_id 找结果来源/最小结果 | 来源 Actor Receipt/Outcome |
| FarmOpportunitySummary | farm-svr FarmOpportunityReadAPI | 好友列表和连续导航候选 | FarmActor |
| RedPointProjection | player-svr RedPointRead | 聚合任务/邮件/图鉴提示 | 各领域事实 |
| PublicProfile | player-svr PlayerReadAPI | 好友卡片的等级、昵称等公开资料 | PlayerActor/资料记录 |
| Leaderboard | player-svr RankRead | 排名查询 | 分数事件/权威统计合同 |

副本可以进入服务内短 TTL 缓存，但 Redis 是共享查询副本；Pod 重启后不能依赖本机内存保留。副本读取
失败时返回局部 Loading/Retry，不得让玩家退回登录页。

### 11.7 TaskDomain 与任务红点物理边界

任务权威继续位于 PLAYER Authority Slot：

```text
<player_tag>:actor:<player_id>
├── model:task
├── model:economy / model:inventory / model:progression
└── receipt:op:<operation_digest>
```

`CLAIM_TASK` 在同一个 `CommitActorMutationV1` 中原子提交 TaskInstance 状态、资产、等级、
AuthorityReceipt 与 StoredOperationOutcome；`CONFIRM_FRIEND_VISIT` 只提交 TaskDomain 变化与自己的
Outcome。二者不跨 Slot，也不创建 Workflow。Harvest/Care/Theft 继续在既有 Player Participant
Authority Commit 中顺带更新 `model:task`，不得再复制一套 TaskProgress Work。

任务入口红点使用已有 Projection Key 的独立 Field：

```text
proj:redpoint:v1:<player_id>                 # HASH
└── field=task
    value=TaskRedPointProjection {
      player_id
      daily_period_id
      task_state_version
      claimable_count
      source_player_record_version
      updated_at_ms
    }
```

Projection Writer 使用 `ApplyProjectionMutationV1` 做单调 CAS：更高 `task_state_version` 才能覆盖旧值；
同版本同 Payload 幂等成功，同版本不同 Payload 必须告警并触发权威重建。该 Key 可以按 Projection
Repository 的容量策略 TTL/重建，不进入 PLAYER Authority Hash Tag，也不参与任务完成或领奖裁决。

P0 不创建 `TaskSummaryChangedOrder`。PlayerActor Authority Commit 成功后，进程内 Projection Adapter
最佳努力刷新；`GET_TASK_LIST`、PlayerBootstrap 或后续 PlayerActor Turn 发现版本落后时再次刷新。
这意味着进程恰在提交后崩溃时红点可以暂时旧或缺失，但完整任务列表与领奖仍正确，下一次权威 Pull
会修复。若未来要求无后续玩家请求也必须有界最终刷新，再引入“每 Player/TaskProjection 最多一条
可合并 Refresh 状态”或 Bucket Reconcile；不得退化成每次任务变化创建一条必达 Order。

Task Receipt 继续受第 8、14 节统一 Receipt item/byte、保留和过载规则约束。生产上限必须根据真实
序列化与重试窗口压测，不在任务模块另设无法验证的 TTL；容量不足时关键领奖 Fail Closed，红点刷新
失败则局部降级。

### 11.8 PlayerMail Authority 与邮件红点物理边界

邮件存在性、正文、已读/删除、六封分页索引和 DeliveryDedup 是非 Actor Authority，物理 Key、Hash
Tag、Lua、DueIndex、GC 和容量只由
[第 35A 号](./35A-player-mail-redis-physical-contract.md) 定义。它们不使用 PLAYER VirtualShard Holder、
WriterEpoch 或 Actor WriterClaim。

附件领取仍在 PLAYER Authority Slot 原子提交：

```text
<player_tag>:actor:<player_id>
├── model:economy / model:inventory / model:progression
├── model:mail_claim_state
├── receipt:op:<operation_digest>
└── mail_projection_dirty_generation
```

MailClaimState 以 `mail_id` 提供业务唯一领取 Fence，operation Receipt 以第 8 章
EffectiveIdempotencyKey 防同一网络意图重放。两者必须同时成立；换 operation_id 不能重领同一邮件。

邮件入口红点继续复用聚合 Projection Key 的独立 Field：

```text
proj:redpoint:v1:<player_id>                 # HASH
└── field=mail
    value=MailRedPointProjection {
      player_id
      mailbox_revision
      unread_count
      claimable_attachment_count
      projected_claim_generation
      updated_at_ms
    }
```

PlayerActor 提交后最佳努力发出/合并 MailClaimProjection；Order 丢失时，邮件列表精确读取
`model:mail_claim_state` 并修复，Reconcile 也可按 dirty generation 重建。Projection 失败只允许红点或
claimed 展示暂时旧，不能重复发奖，也不能反向让 Claim Authority Commit 失败。

### 11.9 植物图鉴权威、查询与容量边界

图鉴永久事实继续位于 PLAYER Authority Slot：

```text
<player_tag>:actor:<player_id>
├── model:compendium_meta
│   ├── unlocked_count
│   ├── compendium_revision
│   └── compendium_seen_revision
├── model:compendium_membership
│   └── crop_definition_id永久成员集合（物理有界）
├── model:economy / model:inventory / model:progression / model:task
└── receipt:op:<operation_digest>
```

首次自种 Harvest 的 Player Participant 在同一 `CommitActorMutationV1` 中提交库存、经验、收获计数、
图鉴 Model Mutation、原始 `CompendiumUnlockDelta`、ParticipantReceipt 与 StoredOperationOutcome。
同一 operation 重放只读取原 Delta，不能根据当前成员集合重新推导历史 `newly_unlocked`。

`GET_COMPENDIUM` 不读取 Projection Redis。`CompendiumReadIngress` 先捕获 player-svr 本地 Watch 得到的
不可变 `PlantCatalogSnapshot`，再通过 Actor 短 Turn 读取两个图鉴 Model，最后在 Turn 外组装页面。
Catalog、活动档案和图片资源是全局配置/静态资源，不复制到每个玩家 ActorRecord；配置暂不可用时只让
图鉴页面 Loading/Retry，不能阻塞 PlayerActor 写命令。

配置平台必须用版本化 `CompendiumLimitProfile` 在发布前验证：

```text
max_catalog_entries
max_unlocked_entries
max_compendium_state_bytes
max_compendium_page_entries
max_compendium_page_bytes
```

Lua 仍按序列化后的真实 Mutation/Record 字节执行最终硬上限检查。活动档案超过页面 item/byte 上限时按
稳定游标分页；禁止静默截断。若错误配置仍令不可逆 Farm Harvest 后的 Player 图鉴写达到硬上限，该
HarvestWorkflow 保持 `PROCESSING/RECONCILE_REQUIRED` 并告警，不能只发资产、跳过永久解锁或重新创建
另一条补丁式写入 Lane。主要防线是配置发布闸门和容量告警，而不是把这个异常变成正常运行路径。

---

## 12. 热点农场的 Snapshot、JoiningBuffer 与共享版本环

本节只规定物理容量实现，Join 状态机、公开帧顺序与 Subscription Fence 仍以第 26、28、29 号为准。

### 12.1 Snapshot 逻辑拆分

同一个 `farm_version`、同一种访问者类别的共享农场视图只生成一次：

```text
SharedFarmSnapshot(farm_id, farm_version, viewer_class)
  + ViewerOverlay(viewer_id, permit/result flags)
  → 对当前连接编码FarmSnapshot
```

200 人同时 Join 不应把相同的地块/CropCycle 数据序列化 200 次；可以复用共享 Base 和编码缓存。但网络
上仍需向 200 条连接分别下发，不能把“缓存一份”误解成只发送一份。

### 12.2 JoiningBuffer 不是每人复制完整 Delta

热点农场物理实现采用：

```text
FarmSubscriptionHub（当前Farm Holder的farm-svr进程内）
├── SharedSnapshotCache          // 最近若干个FarmVersion
├── FarmChangeVersionRing        // 一份共享Delta环
├── FarmRealtimeState
│   ├── delivery_generation
│   ├── last_enqueued_version
│   ├── invalid_from_version
│   └── bounded PostCommitDeliveryGuard[]
└── SubscriptionRuntime[]
    ├── subscription_id
    ├── connection fence
    ├── snapshot_version
    ├── next_delta_version       // 游标
    ├── ready_cut_version
    ├── acked_version
    └── state
```

只有当前已进入/正在进入该农场的在线 Subscription 才有游标；离线好友不占一份 JoiningBuffer。
`FarmChangeVersionRing` 与 Snapshot Cache 都属于可丢失 Runtime，不进入 FarmActor 权威记录，也不写
Redis。Holder 崩溃后客户端通过 Fresh Join + 新 Snapshot 恢复。

### 12.2A 权威提交到实时流的失效保护

共享 VersionRing 不能替代第 27/29 号已经冻结的 `PostCommitDeliveryGuard`。每次会产生公开 FarmChange
的强提交必须：

```text
发起Authority Commit(V)前
→ 在当前Holder Runtime安装PostCommitDeliveryGuard(V, COMMITTING)

明确未提交
→ 清除Guard

TIMEOUT_UNKNOWN
→ 保留Guard，先按Authority Receipt/Version解析

确认已提交
→ 二选一且必须完成一项：
   1. 携带当前delivery_generation进入CommittedChangeQueue/VersionRing
   2. 可靠设置invalid_from_version <= V，并推进delivery_generation
```

禁止出现“Redis 已提交 V，但既没有 Change，也没有 invalid 标记”。失效是 level-triggered 状态，
Control Lane/周期检查会让受影响 Subscription 收到 `REJOIN_REQUIRED`；不能依赖向原已失败普通队列再
投一次 Resync 事件。JoinBaseline、Ring Entry 和 Subscription Cursor 都携带
`delivery_generation`，新 Snapshot 不消费旧代次迟到 Delta。

### 12.3 Ready Cut 的作用

如果一直等待“追到当前最新版本”，热门农场持续变化时 Join 可能永远完成不了。因此 Snapshot 成功
进入 Gate 队列后，冻结一个有限终点：

```text
Snapshot version = 100
ArmSnapshot建立Snapshot(100)，并开始捕获101+
Snapshot成功进入Gate FIFO时，Hub Lane当前最高连续版本 = 103
ready_cut_version = 103

101～103 → Catch-up范围
104以后  → Post-ready范围
```

客户端依次应用 Snapshot(100)、Delta(101..103) 后收到 `SUBSCRIPTION_READY(103)`，即可进入可操作
状态。104 及以后按普通实时 Push 顺序继续；不需要等农场静止。

`post_ready_buffer` 不是另一份每人完整消息数组。逻辑上它表示“该订阅 Ready Cut 后还未发送/确认的
版本区间”，物理上仍通过共享 VersionRing + 每订阅游标确定。

### 12.4 游标落后与重入

如果慢客户端的 `next_delta_version` 已小于共享环最小版本，说明所需 Delta 已被覆盖：

```text
FarmChangeVersionRing.min_version > subscription.next_delta_version
→ REJOIN_REQUIRED(reason=DELTA_GAP)
→ 同一WSS连接上重新Join
→ 获取新Snapshot
```

只有 WSS 本身断开时才重新连接；Delta 追不上不要求重新账号登录。ConnectionWriter 队列满只隔离该
慢连接，不能阻塞 FarmActor 或其他 199 名玩家。

### 12.5 200 人广播

一次浇水的业务处理只生成一个权威变化：

```text
FarmActor提交FarmChange(version=501)
→ FarmSubscriptionHub将同一编码变化按gateway分组
→ DeliverBatch到相关Gate
→ 各Gate把消息并行放入每条连接自己的有界FIFO
```

“199 份下行”表示 199 个订阅者各有一条连接队列，而不是 FarmActor 顺序等待 199 次网络发送。Gate
可以用事件循环/异步 I/O 并发写不同连接；同一连接内部保持 FIFO。两个慢连接队列满，只对这两个连接
执行降级、Rejoin 或断开。

---

## 13. 3000 万 DAU 容量基线

### 13.1 输入假设

`CapacityProfile v0.1`：

| 变量 | 设计值 | 说明 |
|---|---:|---|
| DAU | 30,000,000 | 题目要求 |
| 每人登录/进入次数 | 6 次/天 | 用户估计 5～6 次，取上界 |
| 每次在线时长 | 不超过 5 分钟 | 用户当前产品判断 |
| 设计 PCU | 3,000,000 | 留出活动/集中上线余量 |
| 压力 PCU | 5,000,000 | 极端验证目标，不等于日常预测 |
| 好友平均/上限 | 100 / 300 | 上限不能当平均容量输入 |
| 好友页大小 | 20 | 分页 |
| 同场人数硬上限 | 200 | 包括农场主 |
| 每会话农场 Join | 3 次设计、6 次压力 | 自己 + 约 2 个好友 |

按 `6 × 5min = 30min/人/日` 计算的均匀平均在线约 62.5 万；但生产容量不按平均值部署，仍使用 300 万
设计 PCU，并用 500 万验证退化行为。

### 13.2 登录与 Join 流量

```text
每日Session开始 = 3000万 × 6 = 1.8亿
全天平均         = 1.8亿 / 86400 ≈ 2,083/s
10倍集中峰值      ≈ 20,833/s
2倍压测余量       ≈ 41,667/s
```

设计 Join：

```text
每人每天Join = 6会话 × 3农场 = 18
全天平均      = 3000万 × 18 / 86400 ≈ 6,250/s
10倍峰值       ≈ 62,500/s
2倍压测余量    ≈ 125,000 Join/Snapshot/s（全局）
```

这些是全局目标，最终要按 Zone、RouteDomain、Redis Cluster、Gate/Farm Pod 分摊。不能拿全局数直接要求
单 Pod 承担。

### 13.3 权威业务写

首轮假设每人每天约 20 次需要权威提交的经营/社交操作：

```text
业务写平均 = 3000万 × 20 / 86400 ≈ 6,944/s
10倍峰值    ≈ 69,444/s
2倍压测     ≈ 138,889 business writes/s（全局）
```

单 Actor 本地强提交通常约 1 个 Redis 原子脚本；来源状态 + Order 约 2～3 个后续原子步骤；跨 Actor
Workflow 全流程可能 5～7 个。以平均写放大 3～4 粗估：

```text
全局压力上界 ≈ 41.7万～55.6万 Redis atomic script operations/s
```

这个数字用于暴露风险，不能当作公司 Redis 已验证能力。Actor Dirty 合并只能提供余量，不能拿来把安全
估算改成乐观承诺。

### 13.4 好友机会查询流量

若每个会话获取 3 次“下一个好友机会”：

```text
每人每天GET_NEXT_FRIEND_OPPORTUNITY = 6 × 3 = 18
平均             ≈ 6,250/s
10倍峰值          ≈ 62,500/s
2倍压测           ≈ 125,000/s
```

若每次都扫描 300 个好友，压力可达 3,750 万条 Summary 逻辑读取/秒。因此必须使用：分页、MGET/批量
读、每会话 Navigation 游标、候选窗口、Projection 分片和只对 Top 候选精确检查。按平均 100 好友、
每会话首次完整扫描估算，压力场景仍约 417 万条逻辑 Summary 读取/秒，必须以批量请求而非逐条 RPC
实现。

好友页与连续推荐要分开计量：好友页只下发当前页，默认对 20 张卡片执行一次分组 BatchEvaluate；连续
推荐只精确检查 Top 候选。禁止为了“当前页精确”顺手读取 300 份完整 Farm Authority。压测必须分别
记录 `friend_page_qps`、`friend_page_batch_evaluate_size`、单页 Authority Read 数量、partial/unknown
比例和 p99，确认页面精确性没有把查询流量转化成冷 Actor 激活风暴。

### 13.5 Snapshot 与广播网络

`LimitProfile v0.1` 候选：

- FarmSnapshot：P50 不高于 8 KiB，P95 不高于 16 KiB，硬上限 32 KiB；
- FarmDelta：常见 0.5～1 KiB，单条硬上限 4 KiB；
- Snapshot 只放农场展示所需事实，不放图片、长文本、完整仓库和完整在线成员资料；
- P0 每次 Join 发送完整 Snapshot；Resume Cache + Delta 作为后续优化，不改变 P0 正确性；
- 同版本共享 Snapshot Cache，减少 CPU 与序列化，不能减少真实网络扇出。

按全局压力目标 `125,000 Join/Snapshot/s` 粗算，仅业务 Payload：

```text
P95 16 KiB  → 约1.9 GiB/s → 约16.4 Gbit/s
硬上限32 KiB → 约3.8 GiB/s → 约32.8 Gbit/s
```

这还没有计入 Protobuf/FarmFrame、WebSocket、TLS、重传、跨 AZ 和网卡协议开销。因此 Join 风暴必须
同时给出全局、Zone、Gate Pod 和 farm-svr→Gate 内网预算；共享 Snapshot 只能省编码 CPU，不能省
真实出口字节。若网络预算不成立，应通过分 Zone、压缩/裁剪 Snapshot、错峰与接入限流处理，不能只
扩 Redis。

200 人同场时，一次 1 KiB Delta 的理论业务下行约 199 KiB，尚未计入协议/TLS 开销。需分别压测
FarmSubscriptionHub 分组、Gate Batch、每连接队列和网卡带宽。

### 13.6 记录与存储容量初算

首轮候选：

| 记录 | 常见目标 | 硬上限候选 | 备注 |
|---|---:|---:|---|
| PlayerActor 权威数据 | 16 KiB | 256 KiB | 多 Model HASH，禁止无界邮箱/历史 |
| FarmActor 权威数据 | 12 KiB | 512 KiB | P0 地块可先作为一个 Model |
| FarmSnapshot | 8～16 KiB | 32 KiB | 线上帧预算 |
| FarmDelta | 0.5～1 KiB | 4 KiB | 单次公开变化 |
| 紧凑 Receipt | 128～512 B | 1 KiB | Full/Tombstone/Expired Evidence 分级 |
| WorkRecord | 1～4 KiB | 16 KiB | typed payload 必须有界 |

仅按每玩家 `16 KiB Player + 12 KiB Farm` 粗算：

```text
3000万 × 28KiB ≈ 0.86 TB（约0.78 TiB）原始业务字节
若计Redis对象/Hash/Allocator约2.5倍 ≈ 2.15 TB Primary
再计一份Replica ≈ 4.3 TB
```

这还不包含好友边、Receipt、Workflow、Projection、备份与预留空间。因此正式生产必须按 Zone/Domain
分集群，或在探针后把长期冷权威迁移到更合适的持久数据库；“一套 Redis 全放下”不是冻结结论。

普通操作保留 24 小时 Full + 48 小时 Tombstone。按每人每天 20 次，稳态约 60 个近期 Receipt；若平均
128 B，仅 Value 原始字节也约 230 GB（约 215 GiB）。好友平均 100 则约 30 亿条有向索引边，必须单独估算，不能用
“好友上限 300”直接乘成常态内存。

上述 230 GB 只估算热 Full/Tombstone，不包含长期 Expired Evidence/Archive。后者在 operation_id 仍是
不透明字符串时可能持续增长，必须单独测算归档介质、查询成本和保留策略；不能把它隐含在 Redis 热集群
容量里，也不能为省容量直接删除防重证明。

---

## 14. LimitProfile、有界原则与过载语义

### 14.1 四层限制

同一类数据必须在四层都有限制，不能只依赖最外层帧大小：

1. 公网协议：Frame/Payload 最大字节；
2. 领域合同：地块、好友、同场成员、Receipt、候选窗口等业务数量；
3. Repository：序列化后的 Field/Record/Mutation 实际字节；
4. Lua：不可配置突破的绝对 item/byte/单项硬上限。

Redis 元数据维护 `model_field_count/model_bytes/receipt_count/receipt_bytes/pending_work_count` 等已提交
计数。Lua 根据旧值和新值的真实长度计算增量，不能相信调用者提交的“预计大小”。配置中的
`LimitProfileVersion` 可滚动调整软水位，但 Lua 内绝对硬上限只能随脚本版本升级。

### 14.2 `LimitProfile v0.1` 候选表

| 对象 | 告警/常见目标 | 硬上限或动作 |
|---|---:|---|
| 同场 Subscription | 常见 2～8 | 200（含农场主） |
| Farm Snapshot | P95 16 KiB | 32 KiB/单帧 |
| Shared Snapshot Cache | 最近 4 个版本 | 约 128 KiB/热点 Farm 候选 |
| FarmChange VersionRing | 512 项 | 2 MiB，且约 10 秒变化窗口候选 |
| Join 总时长 | P95 1 秒 | 10 秒后取消/重入 |
| FarmActor Mailbox | warn 256 | hard 512 |
| PlayerActor Mailbox | warn 64 | hard 128 |
| Friend 页面 | 20 | 禁止全量 300 资料下发 |
| Navigation 扫描批次 | 32～64 | 候选窗口 8 |
| Soft 合并窗口 | 50～200 ms | 超时按 RPO/Barrier 处理 |
| Player/Farm 单 Model | warn 32～64 KiB | 由记录硬上限再保护 |
| 图鉴永久成员 | 由 `CompendiumLimitProfile` 与 Catalog 发布校验 | 达硬上限禁止发布新增 Catalog；线上不得静默漏解锁 |
| 图鉴页面 | P0可在Profile内一次返回 | 超 item/byte 上限后按活动档案稳定游标分页 |
| WorkRecord | 常见 1～4 KiB | 16 KiB |

所有队列必须同时限制：`items`、`bytes`、`max_single_item_bytes` 和 `oldest_wait_ms`。只限制条数无法防止
一个巨型消息耗尽内存；只限制字节又无法防止海量小对象带来的调度开销。

### 14.3 不允许静默截断

- Snapshot/Projection 超限：显式分页、Partial 或 Loading；禁止默默少发地块/好友；
- 图鉴页面超限：按稳定档案游标分页；图鉴权威 Model 超限：发布闸门先拒绝配置，运行时 Fail Closed 并告警；
- Receipt 超限：先压缩已满足保留期的 Full 结果；仍满则拒绝新业务；
- 来源 Order/DueIndex 超限：来源业务整体不提交；
- 新 Workflow 超限：在创建边界前返回可重试 Busy；
- 已创建 Workflow、清理、补偿和控制消息：使用预留容量继续完成；
- Mailbox 超限：尚未进入 Actor 的命令可退避/重试，已经 `ADMITTED` 的操作不能无证据丢弃；
- ConnectionWriter 超限：隔离慢连接，不能拖住 Actor；
- Shared VersionRing 被覆盖：该 Subscription `REJOIN_REQUIRED`，不能编造缺失 Delta。

具体公网状态和错误码映射以第 28 号为准；本文只冻结“在哪个提交边界前拒绝”和“已经接纳后必须可
恢复”。

### 14.4 过载时的优先级

```text
1. Fence / Lease / KICK / Rejoin / Completion控制消息
2. 已ADMITTED命令的结果恢复、Participant完成与Unknown解析
3. 当前在线玩家的权威业务命令
4. Snapshot/Delta实时下行
5. Projection、提醒、统计、SoftSave等可延迟工作
```

高水位时可以停止接纳新的 Soft Mutation，但不能粗暴地把所有 Soft 操作改成同步并瞬间把 Redis 打满；
应通过 CommandCatalog 白名单关闭、限流或局部只读来恢复。重试必须有指数退避、抖动、次数/总时长上限；
耗尽后进入持久 Reconcile/DLQ 视图，禁止无限热循环。

### 14.5 Lease 与 TTL 不混用

- ShardLease、WorkLease、Subscription Deadline 使用逻辑 `expire_at_ms`，由 Redis TIME 校验；
- Actor `WriterClaimID/writer_epoch` 是 Fence 证据，不靠 TTL 自动删除；
- 未决 Receipt、Workflow、Order 不使用普通 TTL；
- Runtime Navigation/Ticket/SessionRoute 可按第 26 号合同 TTL；
- Projection 可 TTL/重建，但必须有明确来源和重建手段；
- Authority Redis 必须为 `noeviction` 或等价策略，不能依赖 LRU 保存正确性。

---

## 15. Redis 工程实现约束

### 15.1 ScriptRegistry

Lua 文件随应用版本发布并计算 SHA：

```text
启动/预热：对已知Primary逐节点SCRIPT LOAD
热路径：EVALSHA
MOVED/ASK：切到目标Node后EnsureLoaded
NOSCRIPT：只在目标Node从应用内嵌脚本重新LOAD后有界重试
```

不能把脚本文本只留在某台运维机器，也不能依赖 Redis 重启后仍保有 Script Cache。每次调用记录
`script_name/script_version/result_class/latency/keys/argument_bytes/result_bytes`，但指标 Label 禁止带
player_id/farm_id。

### 15.2 Lua 只做小范围条件更新

允许：

- `GET/HMGET/HSET/ZADD/ZREM` 的固定 Key、小 Field 集合；
- WriterEpoch/Claim/RecordVersion/Receipt/容量检查；
- 来源状态 + 0..N 个有界 Typed Order + DueIndex 原子提交；
- Work Claim/Renew/Complete。

禁止：

- `KEYS`、全局 `SCAN`、无界 `HGETALL`；
- 循环遍历玩家背包、全部 Receipt、全部好友或全部地块；
- 在 Lua 中解析复杂 Protobuf 并执行业务规则；
- 跨 Slot Key；
- 外部网络、随机数或客户端时间参与裁决；
- 把 Plant/Theft 整个跨 Actor Workflow 塞进一个脚本。

### 15.3 连接、超时与错误语义

Authority、Runtime、Projection 即使共用物理集群，也使用独立客户端配置与预算：

```text
AuthorityClient  → 短Deadline、禁止隐式危险重试、TIMEOUT_UNKNOWN进入Resolver
RuntimeClient    → 允许按会话语义重建/重试
ProjectionClient → 可降级、批量、局部Loading
```

连接池必须监控等待时间、活跃/空闲连接、重连与 Pipeline/MGET 大小。写命令超时不能直接判失败重做；
必须按第 30 号 `COMMITTED / NOT_COMMITTED / RECONCILE_REQUIRED` 解析。读副本超时则不升级为 Unknown。

### 15.4 时间来源

Lease、DueIndex Claim 和存储线性化相关时间以 Redis `TIME` 或同一平台可信时间为准。一次 Actor Turn
仍只捕获一次 `trusted_now_ms` 供领域规则使用。客户端时间只用于 UI 倒计时，不能判断成熟、邮件到期、
任务周期或 Lease。

### 15.5 安全与 ACL

- RouteManager 只允许执行 Claim/Transfer/Unassign ShardAuthority，不得修改 Actor Model；
- player-svr 当前 Holder 只允许执行 PLAYER Domain 的 Load/Claim/Commit/Fence；
- farm-svr 当前 Holder 只允许执行 FARM Domain 的 Load/Claim/Commit/Fence；
- farm-worker 只允许 Workflow/Order/DueIndex/Work Lease 与受控 Reconcile 脚本，不得直接修改 Actor Model；
- ProjectionWriter 只写 Projection Namespace；Projection Reader 只读查询副本；
- farm-gate 不直写 Actor Authority；
- Key/日志不得记录密码、ConnectionTicket、Cookie 正文和完整私密 Payload；
- Redis TLS、凭据轮换、网络白名单、备份恢复与审计能力必须通过平台探针；
- 线上禁止手工修改 WriterEpoch、ActorRecord、Receipt 或 DueIndex；修复必须走版本化管理工具与审计。

### 15.6 当前“公司 Redis”承诺边界

当前冻结的是 Repository 和正确性语义，不是“公司 Redis 已经满足生产持久化”。开发阶段可以先用公司
Redis 或本地 Redis 跑通，但如果实例没有 AOF/主从/备份和明确 RPO，演示只能证明功能，不能宣称
Logic Pod/Redis 故障后资产不丢。

进入生产候选前必须确认：

- 持久化模式、主从确认语义、故障切换 RPO/RTO；
- `noeviction` 与内存水位行为；
- Cluster/Lua/Hash Tag 支持；
- 单脚本大小、耗时限制和慢脚本处置；
- 备份恢复、跨可用区、扩容/迁移与成本。

若不满足，替换 `ActorAuthorityStore` 为 TcaplusDB 或其他权威存储；Runtime/Projection Redis 仍可保留。

---

## 16. 可观测性合同

### 16.1 接入与 Join

```text
wss_connections / connection_duration
ticket_issue/redeem/replay_reject
client_hello_latency / session_fence_reject
join_total{phase,result}
join_snapshot_bytes
join_snapshot_cache_hit_ratio
join_snapshot_queue_latency
join_ready_latency
connection_writer_items/bytes/oldest_wait
```

### 16.2 Route 与 Actor Runtime

```text
actor_active_count{domain,pod}
actor_activation/passivation/singleflight_joined
actor_mailbox_items/bytes/wait_ms
actor_turn_latency
route_redirect/revision_lag
shard_handoff_duration
writer_claim_mismatch / record_version_conflict
farm_command_qps / farm_change_rate
```

不得把 player_id/farm_id 放进 Metrics Label。热点定位使用采样结构化日志、限量 Top-K 或诊断接口，避免
3000 万高基数拖垮监控系统。

### 16.3 Authority 与 PersistenceLane

```text
redis_script_qps/latency{script,result}
redis_argument/result_bytes
actor_model_fields/bytes
soft_mutation/save/coalescing_ratio
soft_dirty_age_ms
soft_queue_items/bytes
durability_barrier_latency
force_flush_total
record_version_conflict
writer_claim_mismatch
timeout_unknown / resolution_result
receipt_full/tombstone/expired_count
```

### 16.4 Subscription 与广播

```text
farm_subscriptions{state}
snapshot_cache_hit_ratio
version_ring_items/bytes/version_span
subscription_cursor_lag
ready_cut_gap
post_ready_pending_versions
rejoin_required{reason}
broadcast_fanout / deliver_batch_size
slow_connection_isolation
```

### 16.5 好友机会与查询副本

```text
navigation_scan_count / scanned_friend_count
navigation_candidate_window_hit
opportunity_summary_mget_size
batch_evaluate_candidates/latency
summary_false_positive_ratio
projection_lag_ms
projection_rebuild_total
task_state_commit_total
task_projection_version_lag
task_projection_refresh_total{source,result}
task_claim_total{result}
friend_visit_confirm_total{result}
```

Summary 的“假阳性”表示副本说可能有机会，但精确检查后已无机会；允许存在但必须监控。若假阴性无法由
`next_opportunity_at` 补偿，会导致玩家看不到机会，属于更严重的投影缺陷。

### 16.6 Workflow 与 Worker

```text
work_unfinished{type,state}
due_index_backlog/oldest_due_age
work_claim/retry/lease_expired
participant_receipt_hit
direct_dispatch_success/fallback
workflow_completion_latency
reconcile_required / dlq_count
```

所有业务日志至少携带 `trace_id/request_id/operation_id/workflow_id/work_id/route_domain/virtual_shard/
holder_id/writer_epoch/record_version` 中适用字段；敏感字段脱敏。

---

## 17. 探针、压测矩阵与 SLO 候选

### 17.1 必做平台探针

| 探针 | 要回答的问题 | 产物 |
|---|---|---|
| Redis Lua | 不同参数/记录大小下 p50/p95/p99、CPU、阻塞 | 原始压测数据与脚本版本 |
| Redis Failover | Timeout Unknown、旧主晚写、主从切换如何表现 | COMMITTED/NOT_COMMITTED/RECONCILE 证据 |
| Redis Persistence | 重启/故障后实际 RPO/RTO | 平台配置与恢复日志 |
| WebSocket/Gate | 单 Pod 安全连接数、上下行、队列内存 | 容量曲线 |
| Transport | Direct WS/TCONND 最大帧、顺序、Send语义 | 探针报告 |
| Protobuf | Snapshot/Delta/命令真实分布 | P50/P95/P99/Max |
| Actor | 单 Actor turn、Mailbox、公平性、激活风暴 | 热点/均匀两类曲线 |
| SubscriptionHub | 200 Join、VersionRing、慢客户端 | CPU/内存/延迟/重入率 |
| Worker | DueIndex、Claim、崩溃恢复、积压清空 | 恢复吞吐与最老积压 |
| Route | Redirect、Holder 崩溃、Shard Drain | 请求成功率与停顿时间 |

### 17.2 负载场景

1. 稳态混合：登录、Join、自营、好友查询按容量模型混合；
2. 登录风暴：全局目标 41,667 Session Start/s；
3. Join 风暴：全局目标 125,000 Join/Snapshot/s；
4. 一座农场同时 200 人 Join；
5. 200 人操作不同地块与同一地块两组冲突测试；
6. 200 人中注入慢读、断线和 Gate 队列满；
7. VersionRing 覆盖，验证只让落后订阅 Fresh Join；
8. Redis 主从切换/网络超时，验证 Unknown Resolver；
9. Holder 意外崩溃、Shard Drain、滚动更新；
10. Worker 在 Claim 后/Participant 后/Complete 前分别崩溃；
11. Projection 延迟/丢失，验证好友页最终精确裁决；
12. 2 小时高压 Soak，观察内存、队列、VersionRing 和 DueIndex 是否持续单调增长；
13. 24 小时稳定 Soak，观察 Receipt/Projection/Worker 水位和压缩吞吐；
14. 使用虚拟可信时间执行覆盖至少 72 小时候选窗口的 Receipt 生命周期加速测试，验证
    Full→Tombstone→Expired Evidence、receipt-due 和旧 operation 防重。

### 17.3 `SLOProfile v0.1` 候选

- 健康网络 Join：p95 ≤ 1 s，p99 ≤ 2 s；200 人热点 Join p99 ≤ 3 s；
- 本地单 Actor 可靠命令：服务端 p99 ≤ 100 ms；
- 跨 Actor Workflow：CreateIfAbsent/Execution-Accept 完成并返回 `PROCESSING` 的同步响应 ≤ 200 ms，
  正常最终结果 ≤ 1 s；公网 `ACCEPTED` 不用于 Plant/Harvest/Theft；
- Authority Commit 到 Gate QueueAccepted：p99 ≤ 500 ms；
- Redis 小 Lua：p99 10 ms 为首轮告警候选，不是平台承诺；
- 正常压力下 VersionRing 不覆盖活跃订阅；覆盖时无错误状态、只触发 Rejoin；
- 所有 Unknown 最终进入 Committed、NotCommitted 或显式 Reconcile，不悬空；
- 不出现重复奖励、重复偷菜、旧 Holder 写入成功；
- 2 小时高压与 24 小时稳态下内存/队列/DueIndex 无无法解释的持续增长；加速生命周期测试不丢失
  EXPIRED 证明或防重证据。

客户端公网时延、弱网和渲染时间要与服务端 SLO 分开记录。Pod 数按实测安全吞吐反推：在 CPU 60～70%、
p99 达标、队列稳定的单 Pod 安全值上，增加单 Pod/单可用区故障冗余，不能直接按理论核数计算。

---

## 18. 给实现 Agent 的落地顺序

本节是依赖顺序，不授权实现 Agent 自行改变 26～31 号语义。

### 18.1 Step A：类型、版本与 KeyBuilder

先实现无网络依赖的纯函数与类型：

- `RouteDomain/VirtualShardID/AuthorityLayoutVersion`；
- `ActorAddress/WriterClaim/ShardWriteAuthority`；
- `EffectiveIdempotencyKey/payload_hash/operation_digest`；
- `StableOpaqueID/WorkRefV1/ReceiptRefV1` 及黄金样例；
- `LimitProfile/CapacityProfile`；
- Authority/Runtime/Projection `KeyBuilder`；
- 所有 Redis Error 到领域错误的分类。

必须有 Golden Test：同一输入跨进程/语言版本生成完全相同的 Shard、Hash Tag 和 Key；PLAYER/FARM
RouteDomain 不会混槽；任何带花括号的用户输入都经过安全编码。

### 18.2 Step B：小 Lua 脚本族

按第 6、7、9 章逐个实现并做黑盒合同测试：

```text
Claim/Renew/TransferShardAuthority
UnassignShardAuthority
LoadAndClaimActor
CommitActorMutation
CommitActorMutationWithOrders
CommitParticipantMutation
FenceAndClaimActor
Claim/Renew/CompleteWork
Claim/RenewBucketScanLease
CompactReceipt
```

每个脚本至少覆盖：正常、幂等重试、payload_hash 冲突、RecordVersion 冲突、WriterClaim 过期、容量
超限、NOSCRIPT、超时 Unknown 和脚本晚到。测试禁止依赖执行顺序恰好有利。

### 18.3 Step C：Repository 与 Unknown Resolver

Repository 只暴露第 30 号的逻辑接口，业务模块不能直接拼 Redis Key 或调用脚本。实现：

- bounded `HMGET` LoadProfile；
- ScriptRegistry/EVALSHA；
- WriterClaim 和 RecordVersion 回写；
- `TIMEOUT_UNKNOWN` 证据保存与 `FenceAndClaimActor`；
- Full/Tombstone/Expired Evidence/ResultDirectory 发布与 receipt-due 维护；
- 指标、Trace 与故障注入点。

在此阶段先用单 Actor 测试证明“旧 Writer 永远不能晚写成功”，再接 Player/Farm 业务。

### 18.4 Step D：Actor Runtime 与 PersistenceLane

实现：

- Activation Singleflight、ActorRegistry、Mailbox 水位；
- RouteIngress 对 Shard Revision/Holder/WriterEpoch 的本地校验；
- Actor 一个 Turn 内不等待远程网络 RPC；
- PlayerPersistenceLane；
- 默认 `CRASH_DURABLE`，SoftSave 放 Feature Toggle 后；
- Drain/Passivation 前 ForceFlush 和等待已接纳操作取得可恢复结果。

### 18.5 Step E：Order、Workflow 与 farm-worker

先跑通一个来源局部 Order，再跑 Plant Workflow：

```text
AuthorityCommitWithOrders
→ DirectDispatcher
→ ParticipantReceipt
→ CompleteOrder

PlantWorkflow CreateIfAbsent
→ ReserveSeed
→ CommitPlant
→ CommitReservation
→ CompleteWorkflow
```

随后按相同 WorkAddress/Lease/ParticipantReceipt 框架接入 HarvestWorkflow 与 TheftWorkflow；不得把
HarvestRewardOrder 或第二套 TheftSettlement 进度重新引入为并行恢复锚点。

为每个步骤提供故障注入：提交前、提交后响应前、Participant 后、Complete 前和 Lease 过期。Worker
重试必须复用原 operation/workflow/work_id。

### 18.6 Step F：Projection、好友机会、任务红点与图鉴读取

实现 `opportunity_dirty_generation`、可合并 OpportunitySummaryRefreshOrder（或显式 Bucket
Reconcile）、Projection Writer、当前页批量读、BatchEvaluate 和 Runtime Navigation。先证明
Projection 删除/延迟不会让错误操作成功，也不会因每次操作产生新 Order 而反压核心业务，再优化
MGET、候选窗口和缓存。

同一步实现任务红点的版本化 Projection Adapter，并证明 Projection 丢失只影响入口提示：
`GET_TASK_LIST` 仍从 PlayerActor 返回权威状态，领奖不读取投影，任务变化不创建必达 SourceOrder。

图鉴不走 Projection：实现有界 `CompendiumModel`、PlantCatalog 本地不可变 Snapshot、Actor 短读和
Turn 外页面组装；验证 Harvest UnlockDelta 与 Receipt 同提交、Ack Seen 只单调推进水位，并在配置发布
阶段执行 `CompendiumLimitProfile` 预检。

### 18.7 Step G：SubscriptionHub 热点能力

在不修改第 26/28/29 号公开语义的前提下实现：共享 Snapshot Cache、共享 VersionRing、每订阅游标、
Ready Cut、按 Gate 分组 Batch、慢连接隔离和 RejoinRequired。不得在 Redis 中为 200 个在线订阅复制
200 份 Delta。

### 18.8 Step H：容量与故障验收

最后才根据真实数据调整候选数值。任何调整必须提交：

```text
输入流量模型
→ 原始压测结果
→ 瓶颈定位
→ 新Limit/Capacity/SLO Profile
→ 回归与故障测试
```

禁止只为了让平均 QPS 测试通过而扩大无界队列、Receipt TTL 或 Actor 内存。

---

## 19. 冻结索引与仍待实测事项

### 19.1 已冻结的稳定语义

| 编号 | 决策 |
|---|---|
| 31-01 | `auth-v1` 中 PersistenceBucket 与 VirtualShard 一一映射，但概念保持分离 |
| 31-02 | 同一 Authority Bucket 通过固定 Hash Tag 同槽；Key 只能由 KeyBuilder 生成 |
| 31-03 | ShardWriteAuthority + WriterEpoch 决定 Shard 写权，Actor WriterClaim 再 Fence 单 Actor |
| 31-04 | Actor 使用元数据 + 有界 Model/Receipt Field 的 HASH；activation_generation只在进程内，禁止 HGETALL/全量历史 |
| 31-05 | 激活使用 LoadAndClaim + Singleflight；持久写权只依赖Shard/Claim/Version Fence，Unknown 使用 Fence 后解析 |
| 31-06 | 只使用固定、小型、版本化 Lua 脚本族，不实现万能业务事务 Lua |
| 31-07 | AuthorityReceipt 是提交真值；ResolutionReceipt 固定解析结论；ResultDirectory只是查询副本。只有可靠在途证据可证明PROCESSING，Directory缺失且无法定位来源时返回NOT_FOUND |
| 31-08 | 普通结果采用 Full → Dedup Tombstone → Expired Evidence；无可验证代次时禁止删除最后证明，生产必须在可验证Operation Namespace与Authority Archive中完成至少一种闭环 |
| 31-09 | 来源状态 + 0..N个有界强制Typed Order + DueIndex同槽原子；BUSINESS/RECOVERY类容量失败阻止提交，Projection/BestEffort不得反向阻塞核心事实 |
| 31-10 | Authority/Runtime/Projection Redis 逻辑分离；Projection 永远不代替最终权威裁决 |
| 31-11A | 所有 Record、队列、脚本、Receipt、Snapshot、Mailbox 都必须按 item + byte 有界 |
| 31-11B | 容量输入必须版本化、可追溯；当前v0.1以3000万DAU、6次/天、≤5分钟、好友均值100/上限300、同场200作候选基线 |
| 31-11C | 以指标、故障注入、热点/均匀压测和 Soak 验证，而不是只测平均串行 QPS |
| 31-12 | StableOpaqueID v1锁定Workflow、Order、Participant、Reservation、CropCycle、CareIncident、TheftAttempt与Settlement身份；WorkRefV1/ReceiptRefV1使用LP+Base64URL可逆编码；三者都以黄金样例锁定跨语言实现 |
| 31-13 | KeyBuilder全部接受版本化BucketRef；DueIndex、receipt-due、bucket-meta和scan-lease不得隐式使用另一布局版本 |
| 31-14 | Receipt转换由同槽receipt-due与CompactReceipt驱动；查不到不得猜EXPIRED，相同旧operation不得重新生效 |
| 31-15 | 热点农场保留PostCommitDeliveryGuard、delivery_generation与invalid_from_version；提交后必须有Change或可重复观察失效 |
| 31-16 | Script Cache按Redis Node维护；Worker扫描使用稳定候选分配+Scan Lease；ACL按RouteManager/Domain Holder/Worker/Projection最小授权 |
| 31-17 | DRAINING/ASSIGNING只属于控制面与Holder Runtime；Redis ShardWriteAuthority数据面lease_state只允许UNASSIGNED/ACTIVE，且任一时刻只有一个可写ACTIVE Holder |
| 31-18 | TaskDomain位于PLAYER ActorRecord；Claim/访问确认复用统一AuthorityReceipt，任务红点按task_state_version做可重建Projection CAS，不创建FriendVisit Work或每变化一条TaskSummary必达Order |
| 31-19 | 持久业务幂等Digest覆盖OperationRootRef；ResultDirectory保存Root与来源引用，公网查询用原目标或受保护Locator派生Root，不允许客户端指定Bucket/Redis Key |
| 31-20 | WorkAddress以SourceAuthorityRef统一Actor与非Actor来源；非Actor Unknown使用持久CommitAttemptCursor与精确Attempt ResolutionReceipt，不使用operation级永久Tombstone |
| 31-21 | 邮件Entry/已读/分页服从35A非Actor物理合同；领取资产与MailClaimState留在PLAYER Authority，mail_id业务防重与operation防重同时成立，claimed/红点只作可重建Projection |
| 31-22 | PetState留在PLAYER ActorRecord；GuardSyncWatermark使用同一PLAYER Bucket内固定大小Key，由Order完成脚本单调推进，不允许worker绕过Actor WriterClaim修改model:pet |
| 31-23 | 图鉴Meta与永久成员是同一PLAYER Authority Group内的有界Model；Harvest原子保存UnlockDelta，查询在Actor Turn外合并本地Catalog，配置发布须先通过CompendiumLimitProfile容量闸门 |
| 31-24 | GET_NEXT_FRIEND_OPPORTUNITY复用Runtime Redis Navigation；好友Subscription Ready后由服务端按ActiveSubscription Fence最佳努力标记visited，不新增公网Ack、持久Work或任务完成语义 |

### 19.2 参数候选，不是平台事实

以下已经可以作为首轮默认配置，但必须通过探针/压测修订：

- 4096/16384 等 ShardCount（由第 27 号 RouteProfile 最终给值）；
- Snapshot 8/16/32 KiB；VersionRing 512 项/2 MiB/10 秒；
- Mailbox 64/128 与 256/512；
- Soft 合并 50～200 ms；
- Full 24 小时 + Tombstone 48 小时；Expired Evidence 的长期介质与保留期仍待容量/平台探针；
- 单次来源提交 `source_orders` 上限候选 4；
- 平均业务写 20 次/人/天、每会话 Join 3 次、`GET_NEXT_FRIEND_OPPORTUNITY` 3 次；
- 所有 SLO 数字与单 Pod 安全容量。

### 19.3 尚未取得证据的阻塞项

1. 公司 Redis 是否具备生产所需持久化、`noeviction`、主从切换、备份恢复和 RPO/RTO；
2. Redis Cluster 中目标 Lua/Hash Tag/ACL/脚本大小限制与实测 p99；
3. Direct WebSocket 与 TCONND 最终接入路径、最大帧、Send/背压语义；
4. H5/Go Protobuf 的真实 Snapshot/Delta 字节分布；
5. 单 Gate、player-svr、farm-svr、farm-worker 的安全容量；
6. 200 人同场下 Snapshot 复用、VersionRing、广播与慢连接的真实 CPU/内存/网络；
7. Redis 存放 3000 万长期权威数据的成本是否优于 TcaplusDB/其他持久数据库；
8. Expired Evidence 的生产闭环：可验证 Operation Namespace 或支持在线查询、Authority Commit 防重、
   可靠归档 ACK 与可证明负查询的 Authority Archive；同时验证长期介质和成本；
9. Join 风暴按 Zone/Pod 分摊后的真实出口带宽、TLS/协议开销和限流阈值；
10. Worker Scan Lease、Receipt Compaction 和 Node-aware ScriptRegistry 在 Failover 下的行为；
11. 第 33A 号已冻结 FRIEND_PAIR、FRIEND_QUOTA、FRIEND_SHARE_ISSUER 的物理逻辑；仍需
    实测 Bucket/Partition 数、Lua p99、热门 Token 吞吐、Token Envelope 安全能力和长期 Redis 成本。
12. 第 35A 号 PlayerMail Bucket/Key/Lua 的公司 Redis 实测，以及 Redis 全 Entry 与 Tcaplus Body Store
    的容量和成本选择。

这些阻塞的是“生产能力宣称”和参数定版，不阻塞按接口开始实现 Redis Adapter、脚本合同测试和单机
纵向链路。任何 Agent 都不得把 Mock、单机无持久 Redis 或少量串行测试写成“已支持 3000 万 DAU”。

### 19.4 Friendship Authority 为什么需要独立物理合同

本文的 Lua、WriterClaim 和 KeyBuilder 主体围绕 PLAYER/FARM Actor Authority 设计。第 33 号好友域虽然
同样使用 Redis 强事实，但访问模式不同：

| 权威 | 路由/并发键 | 是否Actor | 必须同槽的事实 |
|---|---|---:|---|
| FRIEND_PAIR | canonical pair_id | 否 | Relation、双向Block、Admission、Pair来源Order、DueIndex |
| FRIEND_QUOTA | player_id | 否 | active relation epochs、Reservations、Participant Receipts |
| FRIEND_SHARE_ISSUER | inviter_player_id | 否 | current generation、Token状态、Validation Receipts |

非 Actor Authority 的 Unknown 恢复以“精确 CommitAttempt”为单位。物理实现必须在同一 Authority Slot
保存可由 CommitIdentity 确定性找到的 `CommitAttemptCursor(attempt_ordinal, attempt_id,
candidate_hash, state)` 与 `ResolutionReceipt`。原 Commit 和 Resolver 竞争的是同一 attempt；Resolver
先关闭旧 attempt 后，不得用 operation 级 Tombstone 阻止同一业务意图重新校验后创建下一 ordinal。
完整 Key、Lua 和 GC 由 33A 冻结。

它们不使用 Actor Lease、Mailbox、WriterClaim 或 Passivation，却仍需要 `targeted fence +
EffectiveIdempotencyKey + Receipt + CommitAttempt Unknown Fence`。因此不能直接复制 Actor Lua，也不能只写
“复用第 31 号”而不定义物理
映射。实现前必须二选一：

1. 在本文增加完整的 Friendship 物理附录；或
2. 单独建立 `33A-friendship-authority-redis-physical-contract.md`。

两者只是文档组织差异，不代表新增 `friend-svr`、FriendActor 或新的部署单元。第 29 号继续唯一拥有
Friendship Authority 的逻辑 RPC、可信身份和 Deadline/Retry 语义；33A 只负责把这些 RPC 映射到确定的
Redis Key、Hash Tag、Lua、Unknown 解析、GC、ACL 与容量门槛，不得重新定义 RPC 业务语义。

当前已建立 [33A · 好友非 Actor 权威 Redis 物理合同](./33A-friendship-authority-redis-physical-contract.md)
作为评审草案：本文主体已经围绕 Actor WriterClaim/Shard Fence 展开，而好友三个 Authority 明确
不使用 Actor Runtime。分开后，实现 Agent 不会误把 Pair/Quota/ShareIssuer 套入 Actor Lua，也能独立完成
热门 ShareToken、300 好友 Quota 和 Pair Admission 的并发压测。它是实现 33 号写链前的物理前置合同，
不阻塞继续讨论第 34 号任务产品语义。

---

## 20. 最终验收清单

文档进入 `freeze` 前必须同时满足：

- [ ] 26～30 号引用文件、术语、Join 与路由 Fence 无冲突；
- [ ] KeyBuilder Golden Test、Lua Script Contract Test、Unknown 并发测试通过；
- [ ] 所有 Redis Key 明确逻辑类别、Hash Tag、TTL/无 TTL 和重建来源；
- [ ] 所有写入明确 WriterClaim、RecordVersion、幂等键、容量检查与 Unknown 语义；
- [ ] 所有 Order/Workflow 均可从 DueIndex 恢复且有 ParticipantReceipt；
- [ ] DueIndex成员严格使用WorkRefV1，并能通过Bucket上下文+WorkRecord恢复、复核完整WorkAddress；
      同一来源提交0..N个强制Order时不存在部分写入；
- [ ] 每条命令已冻结SourceOrderDurabilityClass和强制Order预算；Projection/BestEffort拥塞不阻止核心事实；
- [ ] Receipt通过receipt-due完成压缩/归档，查不到不误报EXPIRED，旧operation不重复生效；
- [ ] 所有队列均有 item/byte/max-item/age 四类水位与过载动作；
- [ ] 共享 Snapshot/VersionRing 不复制每订阅 Delta，游标落后可正确 Rejoin；
- [ ] PostCommitDeliveryGuard保证每个已提交FarmVersion都有Change或level-triggered失效证明；
- [ ] 好友机会 Summary 只作候选，最终操作由 FarmActor 精确裁决；
- [ ] 图鉴永久成员与Meta共享Player WriterClaim/RecordVersion/Lane；Harvest原始UnlockDelta可恢复，
      Catalog发布与页面分页均通过CompendiumLimitProfile验证；
- [ ] 公司 Redis 平台探针和持久化报告有原始证据；
- [ ] 容量模型可通过真实埋点校正，指标无高基数 ID Label；
- [ ] 200 人热点、Holder/Worker/Redis 故障、2小时高压、24小时稳态及加速Receipt生命周期测试通过；
- [ ] 任何未达标项都以明确风险/降级呈现，不以增大无界缓存掩盖。

在这些条件满足前，本文件是“可指导实现的冻结候选”，不是“3000 万 DAU 已验证证明”。
