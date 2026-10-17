# 33A · 好友非 Actor 权威 Redis 物理合同

版本：`v0.3-root-attempt-reactivation-freeze-candidate`  
日期：2026-08-03  
状态：`STABLE_INPUT / PLATFORM_PARAMETERS_PENDING`  
定位：把第 33 号已经冻结的 `FRIEND_PAIR`、`FRIEND_QUOTA`、`FRIEND_SHARE_ISSUER` 三类非 Actor
权威映射为可实现、可测试的 Redis Cluster Key、Hash Tag、Lua、Receipt、Unknown Fence、Order、GC、
ACL 与容量合同。

权威关系：

- 第 29 号唯一拥有 Friendship Authority 的逻辑 RPC、可信调用身份、Deadline 与 Retry 语义；
- 第 30 号唯一拥有非 Actor Authority 的等价 CAS、幂等和 Unknown 逻辑语义；
- 第 31 号唯一拥有通用 Redis、Work、Receipt 生命周期、ResultDirectory 与平台约束；
- 第 33 号唯一拥有好友、分享、删除/拉黑、Quota、Permit 和并发产品语义；
- 本文只回答“这些逻辑语义怎样安全落进 Redis”，不得反向修改 29～33 号。

本文不新增 `friend-svr`、FriendActor、RouteManager 或部署单元。P0 仍然是：

```text
任意健康 player-svr / FriendshipDomain 实例
→ 计算稳定 Authority Bucket
→ 调用 FriendshipAuthorityRepository
→ 执行同槽小型 Lua / Primary 强读
```

本文中的 `INHERITED_FROZEN` 表示直接继承上游，不在本轮重选；`PROPOSED_33A` 表示本物理草案的推荐
选择，需评审后才能冻结；`PLATFORM_PENDING` 表示必须用公司 Redis 和真实压测取得证据。

---

## 0. 一页结论

### 0.1 三类 Authority

| Authority | 逻辑主键 | 是否 Actor | 最终真值 | 主要写入 |
|---|---|---:|---|---|
| `FRIEND_PAIR` | canonical `pair_id` | 否 | Relation、双向 Block、关系 Epoch | 删除、拉黑、激活、ActionAdmission |
| `FRIEND_QUOTA` | `player_id` | 否 | 活跃好友 Epoch、在途名额 Reservation | Reserve、Finalize、Release |
| `FRIEND_SHARE_ISSUER` | `inviter_player_id` | 否 | 当前 Token Generation、状态与准入 Receipt | Generate、Rotate、Revoke、Validate |

三者都不使用：

```text
Actor Mailbox
ActorRegistry
VirtualShard Holder
Shard WriterEpoch
Actor WriterClaim
Passivation
```

但必须使用：

```text
稳定 Authority Bucket
+ Redis Primary 线性化命令顺序
+ record_version / 领域 Epoch
+ CommitIdentity + payload_hash
+ AuthorityReceipt / ParticipantReceipt
+ 单次物理 CommitAttempt Fence
+ 来源同槽 Order + DueIndex
+ Full → Tombstone → Expired Evidence
```

### 0.2 P0 物理主链

```text
farm-gate / farm-svr / farm-worker
        │ 只调用第29号逻辑RPC
        ▼
player-svr / FriendshipDomain（无状态）
        │
        ├── PairAuthorityRepository
        ├── QuotaAuthorityRepository
        └── ShareIssuerRepository
                │
                ▼
        Authority Redis Cluster
        ├── FRIEND_PAIR Buckets
        ├── FRIEND_QUOTA Buckets
        └── FRIEND_SHARE_ISSUER Buckets
```

正常请求不查询中心路由表，也不把好友 Authority 激活到内存。Redis Cluster 客户端根据 Key 访问目标
Primary；`MOVED/ASK` 只纠正 Redis Slot，不等于 Actor Redirect。

### 0.3 五条物理不变量

```text
F1  Pair、Quota、ShareIssuer 的长期事实和未决证据不使用普通 TTL，也不受 LRU 淘汰。
F2  一次 Lua 需要原子的全部 KEYS 必须具有逐字相同的 Hash Tag；CROSSSLOT 不能拆写绕过。
F3  业务 Receipt 与状态同提交；来源必须完成的 Order 与 DueIndex 也与来源状态同提交。
F4  写请求超时未知时，不允许直接重放；先用同槽 CommitAttempt Fence 线性化解析。
F5  Lua 只能操作固定 Key 和少量明确 Field；禁止遍历 300 好友、解析复杂 Protobuf 或执行业务工作流。
```

---

## 1. 范围与非目标

### 1.1 本文必须回答

1. 三类 Authority 如何分桶，为什么不使用 Actor VirtualShard/Holder；
2. 每类记录、Receipt、Resolution、Order、DueIndex 使用哪些 Key；
3. 删除、拉黑、激活、ActionAdmission、Quota 和 ShareToken 分别由哪个 Lua 原子提交；
4. 同一个 operation 的请求超时未知时，怎样区分 `COMMITTED/NOT_COMMITTED`；
5. 删除清理与 Quota Finalize 并发时，怎样避免旧 Epoch 重新占回名额；
6. Pair 来源结果如何可靠发布到玩家 ResultDirectory；
7. Receipt 和空 Pair 怎样有界整理；
8. 热门分享链接、300 好友和 3000 万 DAU 下要压测什么；
9. 哪些调用方能够访问哪些 Redis 前缀和脚本。

### 1.2 本文不定义

- 公网 JSON/Protobuf 字段号；
- 第 29 号 RPC 的业务字段和可信身份；
- AddFriendWorkflow、FriendTerminationOrder、FriendActionDispatchOrder 的产品状态机；
- FriendIndex、好友机会、红点的最终页面语义；
- Redis 已经满足生产持久化、跨可用区或 3000 万 DAU；
- TcaplusDB 的替代实现。

如果公司 Redis 的持久化、成本或容量探针失败，应替换 `FriendshipAuthorityRepository` 的存储 Adapter，
而不是让 H5、Gate、Workflow 或第 33 号业务合同感知底层变化。

---

## 2. Authority Bucket、Hash Tag 与 KeyBuilder

### 2.1 Authority ID

`INHERITED_FROZEN`：

```text
low_player_id  = min(player_a, player_b)
high_player_id = max(player_a, player_b)

pair_id = StableOpaqueID(
  "farm-friend-relation-id-v1",
  canonical_low_player_id,
  canonical_high_player_id
)

FRIEND_PAIR authority_id         = pair_id
FRIEND_QUOTA authority_id        = canonical_player_id
FRIEND_SHARE_ISSUER authority_id = canonical_inviter_player_id
```

客户端、Gate 和其他服务不能提交 BucketID、Hash Tag、relation_epoch 或 Redis Key。

### 2.2 Bucket 计算

`FROZEN_33A`：好友 Authority 使用独立于 Pod 数量的版本化稳定分桶；具体 BucketCount 是
`PLATFORM_PENDING`：

```text
hash_input = LP("friend-authority-bucket-v1")
           || LP(stable_authority_kind)
           || LP(canonical_authority_id)

hash_value = XXH64(seed=0, input=hash_input)
bucket_id  = hash_value % bucket_count[authority_kind]
```

`LP`、整数规范字符串和 `XXH64` 实现服从第 27/31 号。必须使用 `%`，不能假设 BucketCount 一定是
2 的幂。首轮配置候选：

```text
layout_version = friend-auth-v1
pair_bucket_count  = 4096
quota_bucket_count = 4096
share_bucket_count = 4096
receipt_partition_count = 16
```

这些数值是 `PARAMETER_CANDIDATE`。一旦真实数据写入，修改 Hash 算法、Seed 或 BucketCount 必须使用新的
`layout_version` 和迁移程序，不能原地改配置让同一 Authority 突然映射到另一 Key。

### 2.3 Hash Tag

```text
<pair_tag>
= {classic-farm:<env>:<zone>:friend-auth:pair:v1:b:<bucket_id>}

<quota_tag>
= {classic-farm:<env>:<zone>:friend-auth:quota:v1:b:<bucket_id>}

<share_tag>
= {classic-farm:<env>:<zone>:friend-auth:share:v1:b:<bucket_id>}
```

`BucketID` 是应用分桶，不是 Redis Slot。Redis 仍按：

```text
redis_slot = CRC16(hash_tag) mod 16384
```

计算物理 Slot。不同 Bucket 偶然进入同一个 Slot 不影响正确性；同一 Lua 的 Key 使用不同 Tag 则必须在
CI 和 Repository 调用前直接拒绝。

### 2.4 Key 总表

#### FRIEND_PAIR

```text
<pair_tag>:record:<pair_id>                                  # HASH；Relation + 双向Block
<pair_tag>:receipt:<pair_id>:p:<partition>                   # HASH；field=typed receipt locator
<pair_tag>:attempt:<pair_id>:p:<partition>                   # HASH；field=commit_identity_digest
<pair_tag>:resolution:<pair_id>:p:<partition>                # HASH；field=commit_attempt_digest
<pair_tag>:attempt-due                                       # ZSET；OPEN Attempt超时恢复发现
<pair_tag>:order:<work_type>:<work_id>                       # HASH/typed WorkRecord
<pair_tag>:due                                               # ZSET；Pair来源Order发现索引
<pair_tag>:receipt-due                                       # ZSET；Receipt生命周期索引
<pair_tag>:bucket-meta                                       # HASH；布局与有界计数
<pair_tag>:scan-lease                                        # HASH；Worker扫描Lease
```

#### FRIEND_QUOTA

```text
<quota_tag>:record:<player_id>                               # HASH；active + reservation
<quota_tag>:receipt:<player_id>:p:<partition>                # HASH；Participant/Release Fence
<quota_tag>:attempt:<player_id>:p:<partition>                # HASH；CommitAttempt Cursor/Head
<quota_tag>:resolution:<player_id>:p:<partition>             # HASH；CommitAttempt Resolution
<quota_tag>:attempt-due                                      # ZSET；OPEN Attempt恢复发现
<quota_tag>:receipt-due                                      # ZSET
<quota_tag>:bucket-meta                                      # HASH
<quota_tag>:scan-lease                                       # HASH；只用于维护/Receipt整理
```

#### FRIEND_SHARE_ISSUER

```text
<share_tag>:record:<inviter_player_id>                       # HASH；当前Token状态
<share_tag>:receipt:<inviter_player_id>:p:<partition>        # HASH；命令/Validation Receipt
<share_tag>:attempt:<inviter_player_id>:p:<partition>        # HASH；CommitAttempt Cursor/Head
<share_tag>:resolution:<inviter_player_id>:p:<partition>     # HASH；CommitAttempt Resolution
<share_tag>:attempt-due                                      # ZSET；OPEN Attempt恢复发现
<share_tag>:receipt-due                                      # ZSET
<share_tag>:bucket-meta                                      # HASH
<share_tag>:scan-lease                                       # HASH；只用于维护/Receipt整理
```

Quota 和 ShareIssuer 没有来源业务 Order，因此 P0 不创建普通 `:due`。它们的未完成跨 Authority 工作由先
创建的 `AddFriendWorkflow` 发现；Receipt 整理仍使用 `receipt-due`。

### 2.5 Receipt Locator 与 Partition

```text
PUBLIC_OPERATION:
  locator_kind   = OP
  locator_digest = 第31号operation_digest
  field          = op:<lowercase_hex(locator_digest)>

WORKFLOW_PARTICIPANT:
  locator_kind   = PARTICIPANT
  locator_digest = SHA-256(LP("friend-participant-receipt-v1")
                           || LP(participant_invocation_id))
  field          = participant:<lowercase_hex(locator_digest)>

RELATION_RELEASE_FENCE:
  locator_kind   = RELEASE_FENCE
  locator_digest = release_fence_digest
  field          = relation-release-fence:<lowercase_hex(locator_digest)>

partition = uint16_be(locator_digest[0:2]) % receipt_partition_count
```

Partition 只拆小 Redis HASH，所有 Partition Key 仍共享同一个 Authority Bucket Tag，因此 Lua 仍可和
Record 同槽原子提交。它不会把热门邀请者拆到不同 Redis Primary；热门 Token 的单 Authority 热点只能靠
准入限流、容量和后续架构演进解决，不能用“分了 16 个 Key”掩盖。Field 必须带 `locator_kind` 前缀，
不能假设 public operation digest、Participant digest 和 ReleaseFence digest 永远不会碰撞。

`receipt_partition_count` 的结构能力保留，但首轮数量只是 `PLATFORM_PENDING` 参数：必须根据公司 Redis
单 HASH field/byte、Lua KEYS/ARGV、迁移和备份行为探针决定 1/4/16 等实际值。不得把分区数量写死在业务
代码；KeyBuilder 与 record layout version 共同冻结它。

### 2.6 KeyBuilder

```go
type FriendshipAuthorityKind string

const (
    FriendPairAuthority        FriendshipAuthorityKind = "FRIEND_PAIR"
    FriendQuotaAuthority       FriendshipAuthorityKind = "FRIEND_QUOTA"
    FriendShareIssuerAuthority FriendshipAuthorityKind = "FRIEND_SHARE_ISSUER"
)

type FriendshipBucketRef struct {
    Kind          FriendshipAuthorityKind
    LayoutVersion string
    BucketID      uint32
}

type FriendshipAuthorityKeyBuilder interface {
    BucketFor(kind FriendshipAuthorityKind, authorityID string) FriendshipBucketRef
    Record(ref FriendshipBucketRef, authorityID string) RedisKey
    Receipt(ref FriendshipBucketRef, authorityID string, locatorDigest [32]byte) RedisKey
    CommitAttempt(ref FriendshipBucketRef, authorityID string, commitIdentityDigest [32]byte) RedisKey
    Resolution(ref FriendshipBucketRef, authorityID string, attemptDigest [32]byte) RedisKey
    CommitAttemptDueIndex(ref FriendshipBucketRef) RedisKey
    SourceOrder(ref FriendshipBucketRef, workType WorkType, workID string) RedisKey
    SourceDueIndex(ref FriendshipBucketRef) RedisKey
    ReceiptDueIndex(ref FriendshipBucketRef) RedisKey
    BucketMeta(ref FriendshipBucketRef) RedisKey
    BucketScanLease(ref FriendshipBucketRef) RedisKey
    AssertSameSlot(keys ...RedisKey) error
}
```

业务模块和 RPC Handler 禁止字符串拼 Redis Key。Go、Lua 夹具、Worker 和压测工具必须共享 Golden Test。

### 2.7 Friendship StableOpaqueID Golden Vector

沿用第 31 号的 `LP + SHA-256 + Base64URL_NoPadding` 算法。以下值是跨 Go、Lua 夹具、Worker、
压测工具必须一致的黄金样例；任一字符、字段顺序或数字规范化差异都必须让测试失败：

```text
StableOpaqueID("farm-friend-relation-id-v1", "10001", "20002")
= sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to

StableOpaqueID("farm-workflow-id-v2", "20002", "REDEEM_SHARE", "op-redeem-0001",
               "FRIEND_PAIR", "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to")
= sid1_dFIIRvo0XLh3V0u3P_w20K8gTHY9M_wTy__AQPavdQI

StableOpaqueID("farm-friend-quota-reservation-id-v1",
               "sid1_dFIIRvo0XLh3V0u3P_w20K8gTHY9M_wTy__AQPavdQI",
               "10001",
               "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to")
= sid1_zhgY0uuAa9NwvuL2zz9-8QoPwTr3ss8BjFhIj5iun0g

StableOpaqueID("farm-share-token-validation-receipt-id-v1",
               "sid1_dFIIRvo0XLh3V0u3P_w20K8gTHY9M_wTy__AQPavdQI",
               "10001", "20002", "7")
= sid1_YbhYMhVTgFYgC7ZG_EDcVCrXiz2Yyp2UdujLgGdHiEA

StableOpaqueID("farm-friend-quota-release-fence-v1",
               "10001",
               "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to",
               "5")
= sid1_Ti_lMovux-TmFS8v9BOPHFuGez38omUc_qqP1yResNE

StableOpaqueID("friend-commit-attempt-id-v1",
               "FRIEND_PAIR",
               "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to",
               "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
               "1")
= sid1_iZcJ6qLkVb3PKDPZKhZNkaPubyHuM28Ha8BUxWx1Sn0
```

其中 `10001` 是 inviter/一个 Quota Player，`20002` 是 redeemer/Workflow Subject，`7` 是
`token_generation`，`5` 是被终止的 `relation_epoch`。这些说明不进入 Hash 输入，只用于避免测试样例被误读。

### 2.8 为什么这里没有 Shard→Pod Assignment

FriendshipDomain 不持有常驻内存状态，任意实例都可以执行同一 Redis 条件操作：

```text
SERVICE_LB选任意健康player-svr
→ KeyBuilder计算Bucket/Key
→ Redis Cluster Client访问Primary
→ Lua在Primary线性化裁决
```

因此这里的 Bucket 是“Redis 原子性和维护扫描分桶”，不是 Actor VirtualShard，也不需要 Holder Lease、
Handoff 或 Redirect。Redis Slot 迁移由 Redis Cluster 协议的 `MOVED/ASK` 处理；写请求在发送后连接超时
仍然必须进入 Unknown Resolver，不能因为发生 MOVED/Failover 就盲目重放。

---

## 3. 通用 Receipt、版本与 Unknown Fence

### 3.1 RecordVersion 与领域 Epoch

每个 Authority Record 都有一个物理：

```text
_meta:record_version
```

它在该 Authority 任意强写提交时单调加一。领域 Epoch 另有职责：

```text
Pair.relation_epoch      → 好友关系生命周期
Pair.low/high_block_epoch→ 两个方向拉黑生命周期
Share.current_generation → 分享Token代次
Quota relation_epoch     → 某关系占用名额的代次
```

领域 Epoch 不能替代 RecordVersion；RecordVersion 也不能冒充关系或 Token 的业务身份。

### 3.2 CommitIdentity 与 AuthorityReceipt Envelope

公网命令和 Workflow Participant 使用不同但同样稳定的幂等身份：

```text
CommitIdentity {
  identity_kind                 // PUBLIC_OPERATION | WORKFLOW_PARTICIPANT

  PUBLIC_OPERATION:
    EffectiveIdempotencyKey {
      subject_player_id
      command_type
      operation_id
      OperationRootRef          // 必须等于当前Friend Authority
    }

  WORKFLOW_PARTICIPANT:
    work_address
    participant_invocation_id
    participant_step
}
```

Participant 不能伪装成公网命令，也不依赖在线 Session；其身份服从第 29 号 WorkflowCallerContext / 
InternalCallerContext。

每个 Receipt Value 至少保存：

```text
FriendAuthorityReceipt {
  schema_version
  receipt_kind
  authority_kind
  authority_id

  optional EffectiveIdempotencyKey {
    subject_player_id
    command_type
    operation_id
    root_authority_kind
    canonical_root_authority_id
  }
  optional WorkAddress
  optional participant_step
  receipt_locator_kind
  receipt_locator_digest
  optional operation_digest
  optional participant_invocation_id
  canonical_payload_hash

  status_class                 // SUCCEEDED | REJECTED | NO_CHANGE | PARTICIPANT_COMMITTED
  typed_result_or_result_ref
  committed_attempt_id
  committed_record_version
  committed_at_authority_ms

  lifecycle_stage              // FULL | DEDUP_TOMBSTONE | EXPIRED_EVIDENCE
  next_transition_at_ms
}
```

Digest 只用于定位，Value 必须保留完整 CommitIdentity 或可验证引用与 payload hash。相同 Root 身份、不同 Hash
一律 `IDEMPOTENCY_CONFLICT`。

### 3.3 Business Operation 与 CommitAttempt 分开

物理层必须区分：

```text
operation_id / participant_invocation_id
→ 业务幂等身份；所有重试保持不变

commit_attempt_id
→ 一次可能已经发送到Redis的物理写尝试
→ 由Repository确定性生成，客户端不可提交
```

每个 Authority Bucket 以 `commit_identity_digest` 定位一个持久 Cursor：

```text
FriendCommitAttemptCursor {
  schema_version
  authority_kind
  authority_id
  commit_identity_digest
  current_attempt_ordinal
  current_attempt_id
  candidate_hash
  state                         // OPEN | COMMITTED | NOT_COMMITTED | RECONCILE_REQUIRED
  optional resolution_receipt_id
  updated_at_authority_ms
}
```

ID 与 Candidate 规则：

```text
commit_attempt_id = StableOpaqueID(
  "friend-commit-attempt-id-v1",
  authority_kind,
  canonical_authority_id,
  lowercase_hex(commit_identity_digest),
  decimal(attempt_ordinal))

candidate_hash = SHA-256(
  canonical expected_record_version / domain fences
  + canonical typed mutation
  + receipt/outcome identity and hash
  + source order/work identity and hash)
```

`PrepareFriendCommitAttemptV1` 在真正 Mutation 前同槽持久化或幂等复用 OPEN Cursor，并把紧凑
AttemptRef 写入 `attempt-due`。这样业务进程在
Mutation 发送后崩溃，新的处理者仍能按 CommitIdentity 找回准确的 attempt_id、ordinal 和 candidate_hash，
而不是凭内存随机数猜测上一写。

Prepare 自身响应丢失时使用相同确定性 attempt_id/candidate_hash 重试；它只登记恢复证据，不改变业务
Authority。Mutation COMMITTED 或 Resolver 固定终态时必须原子移除对应 attempt-due 成员。farm-worker
只扫描超过 `attempt_resolution_due_at_ms` 的 OPEN Attempt，先解析、不能替玩家生成新业务 Candidate。

规则：

1. 对完全相同的 Redis 传输重试复用同一个 `commit_attempt_id`；
2. 一旦发送后结果未知，禁止启动新 Attempt，先解析原 Attempt；
3. Resolver 明确得到 `NOT_COMMITTED` 后，业务仍要继续时，必须重新读取/校验 Authority，再保持原
   CommitIdentity、将 ordinal 加一并生成新的 Candidate/Attempt；
4. 迟到旧 Attempt 因 Cursor 已关闭而拒绝；不创建 operation 级永久 Tombstone；
5. Cursor=COMMITTED 时必须能定位同一业务 Receipt；二者不一致进入 `RECONCILE_REQUIRED`；
6. 相同 ordinal 但 attempt_id/candidate_hash 不同返回 `COMMIT_ATTEMPT_CONFLICT`。

如果把 `NOT_COMMITTED` Tombstone 只按业务 `operation_id` 保存，就会同时阻止合法的原 operation
重试；如果 attempt_id 只保存在进程内，崩溃后又无法知道应解析哪一次写。Cursor 同时解决这两个问题。

### 3.4 FenceAndResolveFriendCommitAttemptV1

```text
输入：
  authority_kind / authority_id
  CommitIdentity / payload_hash / candidate_hash
  attempt_ordinal
  commit_attempt_id
  resolution_id

同槽Lua：
  1. 若业务Receipt存在且payload_hash相同
       → 校验Receipt.committed_attempt_id
       → Cursor=COMMITTED
       → 写/返回ResolutionReceipt(COMMITTED, receipt_ref)
  2. 若业务Receipt存在但payload_hash不同
       → IDEMPOTENCY_CONFLICT
  3. 若该attempt已有ResolutionReceipt
       → 返回原Resolution
  4. 若Cursor不是该OPEN attempt/candidate
       → COMMIT_ATTEMPT_CONFLICT或RECONCILE_REQUIRED
  5. 若Authority不再满足该Candidate保存的before证据且无法由Receipt解释
       → Cursor=RECONCILE_REQUIRED并持久ResolutionReceipt
  6. 否则Cursor=NOT_COMMITTED并写ResolutionReceipt
       → 后续同commit_attempt_id的迟到Commit必须拒绝
```

Redis 对同 Slot 脚本建立唯一顺序：

```text
原Commit先执行
→ Receipt先出现
→ Resolver返回COMMITTED

Resolver先执行
→ OPEN Cursor被关闭为NOT_COMMITTED
→ 迟到的同Attempt Commit返回ATTEMPT_FENCED_NOT_COMMITTED
→ Resolver返回NOT_COMMITTED
```

Resolver 自己超时未知时，仍使用同一个 `resolution_id + commit_attempt_id` 重试。不能仅做一次 GET、等待
固定毫秒或关闭连接后判断未提交。

物理 Resolution Value：

```text
FriendCommitAttemptResolution {
  schema_version
  authority_kind
  authority_id
  commit_identity_hash
  canonical_payload_hash
  candidate_hash
  attempt_ordinal
  commit_attempt_id
  resolution_id
  resolution_status             // COMMITTED | NOT_COMMITTED | RECONCILE_REQUIRED
  optional authority_receipt_ref
  resolved_at_authority_ms
  lifecycle_stage
}
```

Commit Lua 的校验顺序必须是“先查业务 Receipt，再校验当前 OPEN Cursor”：如果新的 Attempt 已经提交同一
业务 Receipt，迟到旧 Attempt 只返回 `COMMITTED_REPLAY`，不能因为旧 Attempt 已关闭再把已经完成的业务
报告为 NOT_COMMITTED。Resolution/Cursor 不使用普通 TTL；业务 Receipt 最终存在后可压缩为指向该 Receipt
的紧凑证据，业务始终未重试时按 Expired Evidence/Archive 合同保留其 NOT_COMMITTED 证明。

### 3.5 哪些错误可以直接重试

| Redis/Repository 结果 | 是否可能执行过 | 处理 |
|---|---:|---|
| 参数/KeyBuilder本地校验失败 | 否 | 修复调用；不得发送 |
| Redis明确 `MOVED/ASK` | 错误节点未执行目标脚本 | 按客户端协议纠正后复用同Attempt有界重试 |
| `NOSCRIPT` | 否 | 在目标Primary加载脚本后复用同Attempt重试 |
| Lua返回 `RECORD_VERSION_CONFLICT` | 否 | 重读并重新校验；旧Attempt明确关闭后生成新Attempt |
| Lua返回确定性业务终态 | 是 | 返回并保存的Receipt为准 |
| 连接断开/Deadline/Failover且已发送 | 未知 | 进入FenceAndResolve，禁止直接重发 |
| Lua运行时异常 | 不可信 | 告警并Reconcile；脚本设计必须避免写后异常 |

Redis Lua 发生运行时错误并不会自动回滚已经执行的写命令。因此每个脚本必须先完成 Key 类型、Schema、
容量、Receipt、Fence 和全部业务前置校验，之后才进入无失败写入段；禁止“先 HSET，再调用可能报错的
解析或转换”。

### 3.6 Receipt 生命周期

继承第 31 号候选：

```text
完成后0～24小时   → FULL
24～72小时        → DEDUP_TOMBSTONE
超过72小时        → EXPIRED_EVIDENCE / Archive
```

小时数可配置；稳定语义是：

- 未完成 Admission、Reservation、Order、Unknown 不设置普通 TTL；
- 压缩 Full 结果不能同时删除防重证据；
- P0 在可验证 Operation Namespace/Authority Archive 完成前不最终删除 Expired Evidence；
- `receipt-due` 只负责发现，Receipt Field 才是真值；
- Compact Lua 同槽更新 Receipt、计数和 `receipt-due`。

---

## 4. FRIEND_PAIR 物理合同

### 4.1 Pair Record

`<pair_tag>:record:<pair_id>` 为 HASH，不设置 TTL。业务裁决需要 Lua 读取的字段必须是明确 Scalar，不能
藏在 Lua 无法验证的 Protobuf Blob 中：

```text
_meta:schema_version
_meta:authority_layout_version
_meta:record_version
_meta:updated_at_ms
_meta:receipt_count
_meta:receipt_bytes
_meta:pending_work_count
_meta:pending_work_bytes
_meta:limit_profile_version

identity:pair_id
identity:low_player_id
identity:high_player_id

relation:id
relation:status                       # ACTIVE | DELETED
relation:epoch
relation:activated_at_ms
relation:deleted_at_ms
relation:activation_token_id
relation:activation_token_generation
relation:termination_reason
relation:terminated_by_player_id
relation:last_change_operation_id

reactivation:state                    # NONE | PENDING | READY | CONSUMED
reactivation:terminated_relation_epoch
reactivation:barrier_revision
reactivation:low_generation_floor
reactivation:high_generation_floor

block:low_to_high:status              # ACTIVE | REMOVED
block:low_to_high:epoch
block:low_to_high:changed_at_ms
block:high_to_low:status
block:high_to_low:epoch
block:high_to_low:changed_at_ms
```

Pair 从未成为好友但发生有效 Block 时可以创建最小 Pair Record；必须受账号/IP限流，防止恶意请求制造
无界空 Pair。对不存在关系的 Delete、重复 Unblock 等纯 `NO_CHANGE` 不创建主 Pair Record，只在确定性
Pair Bucket 写有界 Receipt、ResultPublishOrder/DueIndex 和 BucketMeta 计数。空 Pair 只有在所有 Block
均 REMOVED、无未决 Work、Receipt 已可靠归档且长期防重证据仍可查询时才允许后台回收，P0 不做普通TTL删除。

### 4.2 Pair Read

| 读取 | Redis 操作 | 一致性要求 |
|---|---|---|
| `AuthorizePreparedVisit` | Primary `HMGET` 固定 Relation/Block Field | 必须读 Primary；不写 VisitPermit Record |
| 当前页 Pair Batch Validate | 按 Redis Node 分组 Pipeline `HMGET` | 每卡独立成功/UNKNOWN；不能读副本授权 |
| AddFriend Pair Validation | Primary固定字段读或只读Lua | 返回 relation/block/epoch/floor 快照 |
| OperationReceipt Lookup | 精确 `HGET receipt-partition field` | 不扫描、不HGETALL |

`AuthorizePreparedVisit` 的 Primary HMGET 本身是 Pair Authority 的线性化读。Hub 先存在 AUTHORIZING 记录；
若随后删除提交，FriendTerminationOrder 能按 pair/epoch 撤销该临时或活跃 Subscription。

### 4.3 Pair 写脚本族

P0 使用独立小脚本，不实现一个接受任意业务枚举和任意 Field 的万能 `CommitPair`：

```text
DeleteFriendV1
BlockPlayerV1
UnblockPlayerV1
ActivateFriendRelationV1
CommitReactivationGenerationFloorsV1
CommitFriendActionAdmissionV1
PrepareFriendCommitAttemptV1
FenceAndResolvePairAttemptV1
CompactPairReceiptV1
```

所有脚本都先检查：

```text
Hash Tag与Key类型
Schema/LayoutVersion
Pair identity与canonical low/high
CommitAttemptCursor仍为同一OPEN ordinal/attempt_id/candidate_hash
同CommitIdentity Receipt是否已经存在
payload_hash是否一致
记录/领域Epoch条件
新增Receipt/Order的item+byte容量
```

### 4.4 DeleteFriendV1

```text
原子提交：
  Relation ACTIVE → DELETED（否则保存NO_CHANGE）
  captured_active_epoch
  relation_epoch按第33号推进
  reactivation=PENDING(captured_active_epoch, barrier_revision++)
  AuthorityReceipt + StoredOperationOutcome
  若终止ACTIVE关系：FriendTerminationOrder
  ResultPublishOrder
  所有新Order对应DueIndex成员
  Pair/Receipt/Work容量计数
  record_version++
```

删除提交不读取 ShareIssuer，也不等待跨 Slot Generation Snapshot。`FriendTerminationOrder` 后续读取双方
Generation，并调用 `CommitReactivationGenerationFloorsV1(expected_deleted_epoch, barrier_revision,
low_floor, high_floor)`；脚本只有在 Pair 仍是同一 DELETED epoch 且 Barrier=PENDING 时才写入 Floor 并
切换 READY，旧 Order 迟到返回 NO_CHANGE。

`ResultPublishOrder` 把 Root 作用域结果可靠写入查询入口：

```text
ResultDirectory(subject, DELETE_FRIEND, operation_id, FRIEND_PAIR:pair_id)
→ Pair AuthorityReceiptRef
```

可靠写入 Projection Redis。它与 Relation 不能跨 Slot 原子，因此必须先作为 Pair 来源 Order 同提交。

### 4.5 BlockPlayerV1 / UnblockPlayerV1

`BlockPlayerV1`：

```text
设置调用方向Block=ACTIVE并推进block_epoch
若Relation原为ACTIVE：
  → DELETED
  → reactivation=PENDING
  → 创建FriendTerminationOrder
保存SUCCEEDED或确定性NO_CHANGE Outcome
创建ResultPublishOrder
record_version++
```

`UnblockPlayerV1`：

```text
仅把调用方向Block改为REMOVED并推进block_epoch
不激活Relation、不修改Quota
保存SUCCEEDED或ALREADY_UNBLOCKED Outcome
创建ResultPublishOrder
record_version++
```

### 4.6 ActivateFriendRelationV1

该脚本只由 AddFriendWorkflow 的受信 Participant 调用：

```text
校验：
  Pair当前epoch与target_relation_epoch前驱匹配
  两方向Block均非ACTIVE
  全新Pair：reactivation.state == NONE
  历史Relation为DELETED：reactivation.state == READY
  重激活时token_generation > inviter方向generation_floor
  重激活时存在匹配barrier_revision的RotateForPairReactivationReceipt
  Workflow/Participant身份与payload_hash匹配

原子提交：
  Relation → ACTIVE(target_relation_epoch)
  activation_token_id / generation
  若为重激活：reactivation.state READY → CONSUMED
  ParticipantReceipt
  record_version++
```

它不能跨 Slot 读取双方 Quota Reservation 或 Share ValidationReceipt。AddFriendWorkflow 先把这些
Participant Result Hash 可靠保存，再通过第 29 号可信 WorkflowCallerContext 调用 Pair；Lua 校验稳定引用
和 Hash，不相信公网字段。

第 29 号已经显式登记 `ValidatePairForAdd/ActivateFriendRelation` Participant RPC；本文只实现 Repository
映射，不分配 Proto field number。

### 4.7 CommitFriendActionAdmissionV1

该脚本是好友 Care/Theft 的 Execution-Accept：

```text
校验：
  Relation ACTIVE
  两方向Block均非ACTIVE
  visitor/owner构成当前canonical pair
  当前relation_epoch与Permit候选一致
  action_scope / target_binding_hash / payload_hash一致
  authority_time_sample仍在允许窗口

原子创建：
  ActionAdmissionReceipt(FULL)
  ActionPermitSnapshot / permit_hash
  FriendActionDispatchOrder(PENDING，内嵌完整不可变ActionPermitSnapshot、permit_hash、
                            target_binding、payload_hash与continuation)
  ResultPublishOrder(PENDING，目标为DispatchOrder WorkAddress)
  两个DueIndex成员
  Receipt/Work容量计数
  record_version++
```

`ActionAdmissionReceipt` 可以保存 `permit_hash + dispatch_work_address` 引用，但 Worker 恢复所需的完整
Permit 必须在 DispatchOrder 内，不得重新读取当前关系并重签。`FriendActionDispatchOrder` 负责把操作可靠
送到 Farm；`ResultPublishOrder` 只负责让玩家按 EffectiveIdempotencyKey（Root=FRIEND_PAIR）找到该
Dispatch 根锚点。两者职责不同，均不代替
FarmActor 的最终地块裁决。

### 4.8 Pair SourceOrder ID

```text
FriendTerminationOrder:
  source_fact_id      = relation_id
  source_fact_version = captured_active_epoch

FriendActionDispatchOrder:
  source_fact_id      = operation_digest
  source_fact_version = accepted_relation_epoch

ResultPublishOrder:
  source_fact_id      = operation_digest
  source_fact_version = "1"   # 每个公开operation只产生一个目录发布意图

order_id = StableOpaqueID(
  "farm-source-order-id-v1",
  work_type,
  "FRIEND_PAIR",
  pair_id,
  source_fact_id,
  source_fact_version
)
```

Order Key 与 Pair Record 使用同一 `<pair_tag>`；Due member 继续使用第 31 号 `EncodeWorkRefV1`。
Order ID 的所有输入必须在调用 Lua 前已知；禁止依赖“提交后才得到的 record_version”再补建 Order。

---

## 5. FRIEND_QUOTA 物理合同

### 5.1 Quota Record

`<quota_tag>:record:<player_id>` 是有界 HASH，不设置 TTL：

```text
_meta:schema_version
_meta:authority_layout_version
_meta:record_version
_meta:updated_at_ms
_meta:configured_limit_version
_meta:effective_max_friends
_meta:active_count
_meta:reserved_count
_meta:receipt_count
_meta:receipt_bytes
_meta:logical_bytes
_meta:limit_profile_version

active:<relation_id>                         = relation_epoch

reservation:<reservation_id>:workflow_id    = workflow_id
reservation:<reservation_id>:relation_id    = relation_id
reservation:<reservation_id>:target_epoch   = target_relation_epoch
reservation:<reservation_id>:state          = RESERVED
reservation:<reservation_id>:created_at_ms  = authority_time

reservation_by_relation:<relation_id>:<epoch> = reservation_id
```

Finalize/Release 后删除 Reservation 明细，幂等结果由 ParticipantReceipt 保留；不能把 FINALIZED/RELEASED
历史永远堆在主 HASH。`active_count/reserved_count` 只能由 Lua 维护，调用方提交的预计 Count 不可信。
`logical_bytes` 按实际 Redis Field 名和值字节的增删 Delta 由 Lua 计算（`HSTRLEN`/已知ARGV长度），用于
Quota 记录硬上限；调用方不得提交 `logical_bytes_after`。对象开销和 allocator 碎片另由内存指标/压测评估，
不能伪装成精确逻辑字节。

### 5.2 Quota 脚本族

```text
ReserveFriendSlotV1
FinalizeFriendSlotV1
ReleaseFriendReservationV1
RemoveActiveRelationIfEpochMatchesV1
FenceAndResolveQuotaAttemptV1
CompactQuotaReceiptV1
```

脚本只访问目标 player 的一个 Quota Record、对应 Receipt/Resolution Partition 和 ReceiptDueIndex；不在
Lua 中读另一方 Quota。

### 5.3 ReserveFriendSlotV1

```text
reservation_id = StableOpaqueID(
  "farm-friend-quota-reservation-id-v1",
  add_friend_workflow_id,
  player_id,
  relation_id
)
```

原子逻辑：

```text
已有同ParticipantReceipt
→ 返回原结果

已有同reservation_id且字段完全相同
→ 返回原RESERVED

存在active:<relation_id>=target_epoch
→ 返回已占用/幂等结果

occupancy = active_count + reserved_count
occupancy >= effective_max_friends
→ 持久REJECTED / FRIEND_LIMIT_REACHED

否则：
→ 写Reservation与reverse field
→ reserved_count++
→ 写ParticipantReceipt
→ record_version++
```

配置下降时允许 `occupancy > effective_max_friends` 的 grandfathered overflow，但上式会阻止任何新
Reservation；已有 Reservation 已计入 occupancy，仍可 Finalize。

### 5.4 Finalize 与删除清理竞态

仅按 `active:<relation_id>` 删除还不够。以下时序会泄漏名额：

```text
关系epoch=5已经ACTIVE，但Quota仍是RESERVED
→ 删除关系并让TerminationOrder执行Release
→ Release看到active字段尚不存在，错误返回NO_CHANGE
→ AddFriendWorkflow迟到Finalize
→ 又把epoch=5写回active
```

`FROZEN_33A`：Termination 的 Quota Participant 必须原子写
`RelationReleaseFence(relation_id, captured_epoch)`，并同时删除匹配的 Active 或 Reservation；Finalize
必须先检查该 Fence：

```text
release_fence_id = StableOpaqueID(
  "farm-friend-quota-release-fence-v1",
  canonical_quota_player_id,
  relation_id,
  captured_relation_epoch
)

release_fence_digest = SHA-256(LP(release_fence_id))
```

Fence Value 保存 `player_id/relation_id/relation_epoch/termination_order_id/participant_invocation_id/
committed_at_ms`。它使用 `release_fence_digest` 选择 Quota Receipt Partition，Field 前缀固定为
`relation-release-fence:`；Finalize Repository 必须同时提供自己的 Participant Receipt Key 和目标
ReleaseFence Key，两者虽然可能属于不同 Partition Key，但共享同一 `<quota_tag>`，仍可在一个 Lua 中
原子读取/写入。

```text
Release先执行：
  写RelationReleaseFence(R,5)
  删除reservation_by_relation:R:5及Reservation（若存在）
  删除active:R=5（若存在）
  写ParticipantReceipt

  迟到Finalize(R,5)
  → 命中ReleaseFence
  → 返回NO_CHANGE / RELATION_ALREADY_TERMINATED
  → 不能重新占回名额

Finalize先执行：
  Reservation → active:R=5
  删除Reservation与reverse field
  写Finalize Receipt

  随后Release(R,5)
  → 删除active:R=5
  → 写ReleaseFence与ParticipantReceipt
```

`RelationReleaseFence` 放在 Quota Receipt Partition，不在主 Record 中无限维护历史 map。它至少保留到相关
AddFriendWorkflow 已终态且 Expired Evidence/Archive 能阻止旧 Participant 重放；P0 不使用普通 TTL 删除。

### 5.5 FinalizeFriendSlotV1

```text
校验：
  Reservation存在且workflow/relation/target_epoch完全匹配
  RelationReleaseFence(relation_id,target_epoch)不存在
  active relation不存在，或已经是同epoch幂等结果

原子提交：
  active:<relation_id> = target_epoch
  active_count++
  删除Reservation及reverse field
  reserved_count--
  写Finalize ParticipantReceipt
  record_version++
```

如果 Pair 已被删除导致 ReleaseFence 存在，Finalize 返回已保存的确定性 `NO_CHANGE`，由原 Workflow 完成
收尾，不得重建关系。

### 5.6 两种 Release 必须分名

逻辑上存在两类不同动作：

```text
ReleaseFriendReservation
→ AddFriendWorkflow在Relation尚未激活时补偿Reservation

RemoveActiveRelationIfEpochMatches
→ FriendTerminationOrder终止已经激活的relation epoch
→ 同时建立RelationReleaseFence，处理Finalize竞态
```

第 29 号已经拆成两个 Typed Participant。实现 Agent 不得重新合并为一个“按 relation_id 删除”的通用
接口糊住差异。

### 5.7 Reconcile

Quota 主记录最多约 300 个 active 和不超过剩余容量的 Reservation，因此允许管理工具执行有界校验，
但热路径禁止 `HGETALL`。Reconcile 使用分页/明确 Field Manifest，并核对：

```text
active_count == active:*实际条数
reserved_count == reservation reverse实际条数
每个Reservation都能定位AddFriendWorkflow
每个Active relation epoch都能由Pair权威解释
```

发现差异先进入审计/修复工作流；禁止直接把裸 Count 改成扫描值后继续。

---

## 6. FRIEND_SHARE_ISSUER 物理合同

### 6.1 ShareIssuer Record

`<share_tag>:record:<inviter_player_id>` 是 HASH，不设置 TTL：

```text
_meta:schema_version
_meta:authority_layout_version
_meta:record_version
_meta:updated_at_ms
_meta:receipt_count
_meta:receipt_bytes
_meta:limit_profile_version

issuer:player_id
token:current_generation
token:current_token_id
token:status                         # ACTIVE | REVOKED；EXPIRED可由时间派生
token:purpose                        # ADD_FRIEND
token:format_version
token:key_id
token:issued_at_ms
token:expires_at_ms
token:digest                         # SHA-256(raw token)，日志仅可用截断digest
token:envelope                       # 受限敏感字段；见6.2
token:last_operation_id
```

### 6.2 Token Envelope

`PROPOSED_33A`：P0 使用经过安全评审的 authenticated-encryption envelope，而不是建立全局
`raw_token → inviter` Redis 索引：

```text
ShareTokenClaims {
  token_format_version
  key_id
  token_id                 # CSPRNG 128 bit以上
  inviter_player_id
  token_generation
  purpose = ADD_FRIEND
  issued_at_ms
  expires_at_ms
}
```

FirstHop 在进程内解密/认证 Envelope，得到 inviter 和 generation，再路由到对应 ShareIssuer Record；
ShareIssuer Lua 仍必须比较当前 token_id/generation/status/expiry，不能只相信 Envelope。

为了满足第 33 号“普通 Generate 返回当前同一链接”，当前 Envelope 作为受限敏感字段保存在 Authority
Record。它本身已经是密文但仍是 Bearer Capability，必须：

- 使用独立 ACL，仅 ShareTokenAPI 可读取；
- 不进入日志、Trace、Metrics、TLog、错误和 Redis 慢日志参数摘要；
- Redis 连接启用 TLS，备份访问受控；
- Keyring 保留最长 Token TTL 所需的旧解密 Key；
- 禁止项目自行发明密码算法，具体 AEAD/KMS 由安全评审选择。

若平台不允许在 Redis 保存可重放 Envelope，应改成 KMS 包裹后的 Token Secret 或经评审的可重复签发
格式；这属于 `SECURITY_PLATFORM_PENDING`，不改变 generation/receipt 语义。

### 6.3 Authority Time Sample

Lua 不负责密码学编码。生成新 Token 时：

```text
Repository从目标Redis Primary取得TIME样本
→ 在进程中创建Claims与Envelope
→ Generate/Rotate Lua再次读取Redis TIME
→ 若样本年龄超过max_authority_time_sample_age_ms则拒绝且不写
→ 否则原子保存Claims对应Scalar + Envelope + Receipt
```

过期判断最终使用脚本中的 Redis TIME。客户端时间和 player-svr 墙钟不参与有效性裁决。

### 6.4 ShareIssuer 脚本族

```text
GenerateShareTokenV1
RotateShareTokenV1
RotateShareTokenForPairReactivationV1
RevokeShareTokenV1
AcceptShareTokenAdmissionV1
FenceAndResolveShareAttemptV1
CompactShareReceiptV1
```

`GenerateShareTokenV1`：

```text
当前Token有效
→ 不推进generation
→ 保存本operation的SUCCEEDED/NO_CHANGE Receipt
→ 返回同一Envelope

不存在或已按Redis TIME过期
→ generation++
→ 保存候选Token Scalar + Envelope
→ 保存Receipt
→ record_version++
```

`RotateShareTokenV1` 总是推进 generation、撤销旧 Token 并安装候选新 Token；`RevokeShareTokenV1` 推进
generation并将当前状态置为 REVOKED，不自动创建新 Token。三者都保存确定性 Outcome。

`RotateShareTokenForPairReactivationV1` 只接受 FriendshipDomain 从 Pair Primary 读取并签名/Hash 绑定的
`ReactivationBarrierProof(pair_id, terminated_relation_epoch, barrier_revision, state=READY)`。脚本推进
generation、安装新 Token，并在 ShareIssuer Receipt Partition 保存：

```text
ReactivationRotateReceipt {
  pair_id
  terminated_relation_epoch
  barrier_revision
  token_id
  token_generation
  operation_id
  committed_at_authority_ms
}
```

普通 Rotate 不自动满足某个已删除 Pair 的重加证明；只有该 Typed Receipt 可证明用户在当前 Barrier READY
后显式轮换。Pair 后来再次删除会推进 barrier_revision，使旧 Receipt 自动失效。

### 6.5 AcceptShareTokenAdmissionV1

```text
校验：
  Envelope认证结果与Redis当前token_id/generation/purpose一致
  status=ACTIVE
  Redis TIME < expires_at_ms
  redeemer != inviter
  同CommitIdentity Receipt不存在或payload_hash相同
  CommitAttemptCursor仍为同一OPEN ordinal/attempt_id/candidate_hash

原子提交：
  ShareTokenValidationReceipt
  receipt-due成员
  Receipt容量计数
  record_version++
```

Receipt 先提交后，后续 Rotate/Revoke 不反向取消原 AddFriendWorkflow；Rotate/Revoke 先提交时，本脚本
看到 generation/status 不匹配并拒绝。

Token 允许多人使用，所以不能维护一个每次递减的全局剩余次数。但每个已接受 Workflow 仍必须写一条
ValidationReceipt。热门 Token 的写热点是真实存在的，不能用“无次数计数”表述为无写入。

### 6.6 热门 Token 的语义 CAS 优化

`FROZEN_33A`：以下只追加 Receipt、且能够在同一 Lua 内重新检查完整领域 Fence 的脚本，不强制调用方
先读 `record_version` 再提交：

```text
AcceptShareTokenAdmissionV1
CommitFriendActionAdmissionV1
Reserve/Finalize/Release FriendQuota脚本
```

它们仍在 Lua 内读取并推进 `record_version`，但并发条件使用更精确的：

```text
token_id + token_generation
relation_id + relation_epoch + block_epoch
reservation_id + target_relation_epoch
```

而不是让 100 个并发兑换者全部先读同一个版本、产生 99 次无意义 CAS 冲突。真正替换整个业务候选的
Delete/Block/Unblock/Activate/Generate/Rotate/Revoke 仍使用 expected_record_version。

这是对第 30 号“单记录版本条件”的性能细化：所有写仍是单 Record 线性化、无盲写，但 append/targeted
mutation 使用领域版本条件。评审若坚持所有脚本都要求外部 expected_record_version，可以先实现严格基线，
但必须压测热门 Token 冲突放大后再宣称满足目标容量。

### 6.7 Generation Snapshot 与 Reactivation Barrier

`GetGenerationSnapshot(player_id)` 使用目标 Primary 的固定 Field `HMGET`，返回：

```text
inviter_player_id
current_generation            # 记录不存在时为0
record_version
read_at_authority_ms           # 诊断
```

Delete/Block 不在 Pair Commit 前读取 Snapshot。Pair 先撤权并进入 PENDING；FriendTerminationOrder 后台
分别读取双方 Snapshot，再调用 Pair 的 `CommitReactivationGenerationFloorsV1` 一次提交两侧 floor 并切换
READY。读取或提交失败只延迟重加，不回滚删除。

Snapshot 只提供 generation，不比较不同 Redis Primary 的墙钟。READY 后的重加还必须携带当前 barrier 的
`ReactivationRotateReceipt`，因此 Snapshot 与 Rotate 并发不会让删除处理中生成的链接自动恢复关系。

---

## 7. 第 29 号 RPC 到 Repository 的映射

本文不重新定义 Proto，只冻结物理映射：

| 第29号逻辑调用 | Authority | Repository/Lua |
|---|---|---|
| DeleteFriend | FRIEND_PAIR | `DeleteFriendV1` |
| BlockPlayer | FRIEND_PAIR | `BlockPlayerV1` |
| UnblockPlayer | FRIEND_PAIR | `UnblockPlayerV1` |
| ValidatePairForAdd | FRIEND_PAIR | Primary固定字段HMGET/只读Lua（PENDING直接拒绝） |
| ActivateFriendRelation | FRIEND_PAIR | `ActivateFriendRelationV1` |
| CommitReactivationGenerationFloors | FRIEND_PAIR | `CommitReactivationGenerationFloorsV1` |
| AuthorizePreparedVisit | FRIEND_PAIR | Primary固定字段HMGET/只读Lua |
| CommitFriendActionAdmission | FRIEND_PAIR | `CommitFriendActionAdmissionV1` |
| GetFriendActionAdmission | FRIEND_PAIR | 精确Receipt HGET + Order HGET |
| ReserveFriendSlot | FRIEND_QUOTA | `ReserveFriendSlotV1` |
| FinalizeFriendSlot | FRIEND_QUOTA | `FinalizeFriendSlotV1` |
| ReleaseFriendReservation | FRIEND_QUOTA | `ReleaseFriendReservationV1` |
| RemoveActiveRelationIfEpochMatches | FRIEND_QUOTA | 同名Lua + ReleaseFence |
| Generate/Rotate/RotateForPairReactivation/Revoke | FRIEND_SHARE_ISSUER | 对应Share Lua |
| AcceptShareTokenAdmission | FRIEND_SHARE_ISSUER | `AcceptShareTokenAdmissionV1` |
| GetGenerationSnapshot | FRIEND_SHARE_ISSUER | Primary固定字段HMGET |

RPC Handler 必须从 Trusted/Workflow/Internal Caller Context 推导 subject、pair、player 和权限；Repository
只接受已规范化的 Authority ID、Canonical Payload Hash 和 Typed Mutation，不接受公网 bytes。

第 29 号已回补 Pair Add/Activate 与 Quota 两类 Release 的 Typed 边界。最终生成 Proto 时还需为
`CommitReactivationGenerationFloors` 和 `RotateForPairReactivation` 登记内部恢复调用；它们不是新增服务，
只是 FriendTerminationOrder/用户显式轮换所需的 Typed RPC。

---

## 8. Workflow、SourceOrder、DueIndex 与 ResultDirectory

### 8.1 WorkPlacement 不变

```text
AddFriendWorkflow
→ INDEPENDENT_WORK_BUCKET
→ 使用第31号Workflow Bucket/Key/Lua

FriendTerminationOrder
FriendActionDispatchOrder
ResultPublishOrder
→ SOURCE_PERSISTENCE_BUCKET
→ source_authority = {
     authority_kind = FRIEND_PAIR
     canonical_authority_id = pair_id
     authority_layout_version = friend-auth-v1
   }
→ 与Pair Record同一个Hash Tag
```

33A 不复制 AddFriendWorkflow 物理布局。它只实现 Workflow 调用的三个好友 Participant 和 Pair 来源
Order 的存储。

### 8.2 Pair DueIndex

```text
key    = <pair_tag>:due
score  = next_due_at_ms
member = EncodeWorkRefV1(work_type, work_id)
```

扫描上下文提供：

```text
placement_kind = SOURCE_PERSISTENCE_BUCKET
bucket_version = friend-auth-v1
bucket_id
source_authority = FRIEND_PAIR:<pair_id>@friend-auth-v1
```

加载 Order 后必须再验证完整 `SourceAuthorityRef/pair_id/work_type/work_id`。禁止用裸 work_id 遍历多个 Key。
FRIEND_PAIR 来源不得填写 Actor RouteDomain。静态 WorkType 注册表至少登记：

```text
FRIEND_TERMINATION_ORDER  → SOURCE_PERSISTENCE_BUCKET / FRIEND_PAIR
FRIEND_ACTION_DISPATCH_ORDER → SOURCE_PERSISTENCE_BUCKET / FRIEND_PAIR
RESULT_PUBLISH_ORDER      → SOURCE_PERSISTENCE_BUCKET / FRIEND_PAIR
```

注册表同时冻结 TypedPayloadType、允许 ParticipantStep 和最大 payload/order bytes；未知组合 Fail Closed。

### 8.3 创建 Order 的原子边界

```text
PairMutationLua(
  PairStateMutation,
  AuthorityReceipt,
  NewSourceOrders[0..N],
  DueMembers[0..N]
)
```

当前单次 Pair Commit 最多需要：

| 场景 | 强制Order |
|---|---|
| Delete ACTIVE Relation | FriendTerminationOrder + ResultPublishOrder |
| Delete NO_CHANGE | ResultPublishOrder |
| Block并终止Relation | FriendTerminationOrder + ResultPublishOrder |
| Block无Relation/Unblock | ResultPublishOrder |
| Friend Care/Theft Admission | FriendActionDispatchOrder + ResultPublishOrder |
| Activate Relation | 无Pair来源Order；由AddFriendWorkflow继续收尾 |

任一 `BUSINESS_REQUIRED/RECOVERY_DISCOVERY_REQUIRED` Order 或 DueIndex 超限，整个 Pair Mutation 不提交。
禁止先改 Relation，再把 Order 放进进程内队列或 MQ。

### 8.4 DirectDispatcher 与 farm-worker

```text
Pair Lua COMMITTED
→ 返回完整WorkAddress[]
→ player-svr进程内DirectDispatcher有界入队
→ Unary gRPC推进Participant

队列满 / 进程退出 / RPC失败
→ Pair DueIndex仍存在
→ farm-worker取得Bucket Scan Lease
→ Claim具体Order
→ 使用相同InvocationID恢复
```

DirectDispatcher 不是 MQ，也不是真值。队列满不能回滚 Pair 已提交事实；Worker 积压也不能让 Relation
重新 ACTIVE。

### 8.5 ResultDirectory 发布

Pair Authority 与 `proj:result:v2:<subject_player_id>` 不同 Slot，因此采用来源同槽
`ResultPublishOrder`：

```text
Pair Commit
→ 原子保存ResultPublishOrder + DueIndex
→ DirectDispatcher/worker幂等写ResultDirectory
```

不同命令的 Locator：

| 命令 | ResultDirectory source_ref |
|---|---|
| DELETE_FRIEND/BLOCK/UNBLOCK | Pair AuthorityReceiptRef |
| Friend CARE/THEFT | FriendActionDispatchOrder WorkAddress |

ShareToken Generate/Rotate/Revoke 可由 `subject_player_id` 确定性定位 ShareIssuer；RedeemShare 可由
对应 EffectiveIdempotencyKey 确定性定位 AddFriendWorkflow，因此它们不额外创建 Pair
ResultPublishOrder。

ResultDirectory 写成功只表示查询入口可用，不改变业务终态。Directory 丢失时来源 Order/Receipt 仍是真值；
公网查询使用原 peer/target 或服务端完整性保护 Locator 重新派生 OperationRoot；在尚未重新发布且仍无法
定位可靠来源时，读取端按第 29/31 号返回 `NOT_FOUND`，不能猜测失败或接受客户端自报 Pair Bucket。

### 8.6 Order 完成条件

```text
FriendTerminationOrder
→ 双方Quota Release Fence已提交
→ 匹配旧Epoch的Hub Subscription撤销已得到幂等结果
→ FriendIndex Dirty/修复步骤已可靠接管
→ COMPLETED

FriendActionDispatchOrder
→ CARE已取得Farm Care Receipt终态
  或THEFT Workflow已可靠CreateIfAbsent并保存WorkAddress
→ 保存final_result_ref
→ COMPLETED

ResultPublishOrder
→ 目标ResultDirectory已按更高/equal source revision幂等写入
→ COMPLETED
```

只“发出一次 RPC”不能完成 Order。Participant 成功但完成 Order 的响应丢失时，重试必须从 Participant
Receipt 收敛。

---

## 9. Lua 输入、输出与工程约束

### 9.1 Lua 只做条件更新

允许：

- 固定 Key 上 `HMGET/HGET/HSET/HDEL/HINCRBY/ZADD/ZREM/TIME`；
- 校验 Schema、RecordVersion、领域 Epoch、CommitIdentity、payload hash、Attempt Fence；
- 更新目标 Relation/Block/Token/Reservation Field；
- 写一个精确 Receipt Partition；
- 创建最多两个有界 SourceOrder 并写 DueIndex；
- 根据 `HSTRLEN` 计算真实字节增量。

禁止：

- `KEYS`、全局 `SCAN`、热路径 `HGETALL`；
- 在 Lua 遍历 300 个 active relation；
- 在 Lua 解析任意 Protobuf/JSON；
- 在 Lua 做密码学、网络请求、随机数或调用其他 Slot；
- 接受调用方提交的 `receipt_bytes_after`、`active_count_after` 等可伪造计数；
- 一个脚本同时推进整个 AddFriendWorkflow。

### 9.2 Candidate Payload

复杂 Typed Receipt/Permit/Order 由 Go 使用规范 Builder 预编码。Lua 不解析其内部业务字段，但必须同时
接收并校验关键 Scalar：

```text
schema_version
authority_id
operation_digest
payload_hash
receipt_kind
relation_epoch / block_epoch / token_generation / target_epoch
serialized_value_length
serialized_value_hash
```

Repository 从 Redis 读取 Value 时重新验证 typed value hash 和外层 Scalar。一旦不一致，返回
`CORRUPT_AUTHORITY_RECORD` 并 Fail Closed，不能使用部分数据继续授权。

### 9.3 CommitFriendActionAdmissionV1 的 Key 示例

```text
KEYS[1] PairRecord
KEYS[2] PairReceiptPartition
KEYS[3] PairCommitAttemptPartition
KEYS[4] PairResolutionPartition
KEYS[5] FriendActionDispatchOrder
KEYS[6] ResultPublishOrder
KEYS[7] PairDueIndex
KEYS[8] PairReceiptDueIndex
KEYS[9] PairBucketMeta
```

Repository 调用前执行 `AssertSameSlot(KEYS...)`。Lua 开始时再次比较所有 Key 的预期 Tag/布局元数据；
出现 CROSSSLOT/错误 Key 直接失败，不能降级成两个 EVAL。

### 9.4 统一逻辑结果码

脚本数值码由生成器最终分配，但语义至少包括：

```text
COMMITTED_NEW
COMMITTED_REPLAY
SUCCEEDED_NO_CHANGE
BUSINESS_REJECTED
IDEMPOTENCY_CONFLICT
RECORD_VERSION_CONFLICT
DOMAIN_EPOCH_CONFLICT
ATTEMPT_FENCED_NOT_COMMITTED
CAPACITY_ITEM_LIMIT
CAPACITY_BYTE_LIMIT
SCHEMA_MISMATCH
LAYOUT_MISMATCH
CORRUPT_RECORD
STALE_AUTHORITY_TIME_SAMPLE
```

业务 `REJECTED/NO_CHANGE` 与技术 `RECORD_VERSION_CONFLICT/CAPACITY/SCHEMA` 必须分开。前者在越过 Execution-Accept
后保存 Outcome；后者按 CommandCatalog 的重试/告警语义处理，不能伪装成业务结果。

### 9.5 ScriptRegistry

```text
friend_pair_delete_v1.lua
friend_pair_block_v1.lua
friend_pair_unblock_v1.lua
friend_pair_activate_v1.lua
friend_pair_commit_reactivation_floors_v1.lua
friend_pair_action_admission_v1.lua
friend_quota_reserve_v1.lua
friend_quota_finalize_v1.lua
friend_quota_release_reservation_v1.lua
friend_quota_remove_active_epoch_v1.lua
friend_share_generate_v1.lua
friend_share_rotate_v1.lua
friend_share_rotate_for_pair_reactivation_v1.lua
friend_share_revoke_v1.lua
friend_share_accept_admission_v1.lua
friend_authority_resolve_attempt_v1.lua
friend_authority_prepare_attempt_v1.lua
friend_authority_compact_receipt_v1.lua
```

脚本随程序版本发布。ScriptRegistry 的加载状态按 `(redis_node_id, script_name, script_version)` 保存，
不是 Cluster 全局布尔值：启动时对已知 Primary `SCRIPT LOAD`，热路径 `EVALSHA`；`MOVED/ASK` 后切换目标
Node 并执行 EnsureLoaded；Failover 后的新 Primary 视为未加载；`NOSCRIPT` 只能在实际目标节点加载同版本
源码后有界重试。日志记录脚本名/版本/延迟/参数字节，不记录玩家 ID、原始 Token、完整 Payload。

---

## 10. 读路径、缓存与一致性

### 10.1 不能授权的缓存

以下数据只能加速显示：

```text
FriendIndex
PublicProfile
FarmOpportunitySummary
服务内短TTL Pair Read Cache
```

它们不能签发 VisitPermit/ActionPermit，也不能决定 Delete/Block/Activate。真正权限读必须访问 Pair
Authority Primary。

### 10.2 Pair 只读缓存

P0 可以不做 Pair Cache。若压测后增加，只允许：

- 用于好友卡片预过滤；
- TTL 很短且有 relation_epoch；
- 未命中/过期回 Primary；
- 真正 Join/Care/Theft 再读或强提交 Authority；
- 删除/拉黑提交后尽力失效，但正确性不依赖失效消息。

### 10.3 Batch 读取

好友页最多 20、上限 50。FriendReadAPI：

```text
FriendIndex分页得到friend_ids
→ 计算每个canonical pair及Bucket
→ 按Redis Primary/Node分组Pipeline HMGET
→ 独立验证ACTIVE/Block
→ 对有效卡片继续FarmReadAPI.BatchEvaluate
```

某个 Node 超时只令对应卡片 `Loading/UNKNOWN`；不能因为部分未知把整个玩家踢回登录，也不能把 UNKNOWN
当作“仍是好友”。

### 10.4 Share Token 读取

Token Envelope 解密只解决“这串 Token 声称属于谁”；最终有效性必须由 ShareIssuer Primary 裁决。
禁止把解密成功直接等同于 ACTIVE，也禁止只依赖 CDN/H5 缓存判断过期。

---

## 11. 有界容量与过载

### 11.1 LimitProfile v0.1 候选

| 对象 | 常见/告警候选 | 绝对硬上限候选 | 超限动作 |
|---|---:|---:|---|
| Pair主Record | 1～2 KiB | 16 KiB | Fail Closed + 告警 |
| Pair单Receipt | 128～512 B | 2 KiB | 拒绝本次接纳 |
| Pair热Receipt数量 | 64～128 | 512 | 先压缩；仍满则拒绝新业务 |
| Pair未决SourceOrder | 0～4 | 64 | 新来源提交失败；已接纳Order保留 |
| Quota active relation | 均值约100 | 配置默认300、物理512 | Reserve拒绝 |
| Quota Reservation | 常见0～4 | 不超过物理512 | Reserve拒绝 |
| Quota主Record逻辑字节 | 依真实Field分布 | 候选256 KiB | 新Reserve拒绝；已有清理继续 |
| ShareIssuer主Record | <4 KiB | 16 KiB | 拒绝生成并告警 |
| Share Validation热Receipt | 0～512 | 4096 | 限流/Busy；不得随机淘汰 |
| 单SourceOrder | 1～4 KiB | 16 KiB | 来源提交失败 |
| 单Lua KEYS | 2～8 | 12 | CI拒绝脚本升级 |
| 单Lua ARGV总字节 | 常见<16 KiB | 64 KiB | Repository拒绝发送 |
| 单CommitAttempt Cursor | <1 KiB | 2 KiB | 拒绝Prepare并告警 |

所有值都是 `PARAMETER_CANDIDATE`，必须由真实 Payload 分布和压测修订。硬上限进入版本化 Lua；软水位
进入 LimitProfile，不能通过线上配置突破脚本绝对上限。

### 11.2 热门 ShareToken

一个 Token 被多人点击时，所有 Validation 仍访问同一 ShareIssuer Authority。P0 保护顺序：

```text
Gate账号/IP粗限流
→ ShareTokenAPI token-digest级令牌桶（Runtime Redis，可丢失重建）
→ 已认证账号级限流
→ ShareIssuer Authority容量检查
→ AddFriendWorkflow双方Quota硬上限
```

限流只在 Admission 前拒绝新请求，不能删除已经写入的 ValidationReceipt 或在途 Workflow。热门 Token
必须单独压测单 Key/单 Slot QPS、Lua p99、主从复制延迟和 Receipt GC；如果一个 Token 的目标并发超过
单 Redis Primary 安全能力，后续可把 Token Admission 改为“不可变 Token Generation + 独立分桶 Receipt”
架构，但那会改变第 33 号接受点，不能由 P0 实现私自优化。

### 11.3 30M DAU 的存储现实

若把 3000 万活跃账号和平均 100 个好友作为量级输入：

```text
有向FriendIndex边 ≈ 30,000,000 × 100 = 30亿
无向canonical Pair ≈ 15亿
```

即使每个 Pair 连 Redis 对象开销、Allocator 和索引后只按 0.5～1 KiB 粗估，也可能达到约
`0.75～1.5 TB Primary`，再加 Replica、备份、Quota、Receipt 和 FriendIndex 会继续放大。该估算不是
容量承诺，而是在提醒：

- P0 用公司 Redis 跑通正确性没有问题；
- 不能仅凭持久化 Redis 就宣称长期承载 3000 万 DAU；
- 生产候选必须拿总注册用户数、平均/分位好友数、 churn、Receipt 速率和 Redis 单价重算；
- 若成本或持久性不成立，应把长期 Pair/Quota/ShareIssuer Authority 迁移到合适的持久数据库，Redis
  保留热点缓存、Runtime 和 Projection。

存储 Adapter 变化不得把 FriendIndex 升级为关系真值，也不得破坏 Relation+Block、Receipt+Order 的
原子边界。

### 11.4 过载优先级

```text
1. Unknown Resolver / Attempt Fence / 已接纳Participant完成
2. FriendTerminationOrder（权限撤销收尾）
3. 已接纳AddFriendWorkflow和ActionDispatch
4. 当前在线玩家的新好友写
5. ResultDirectory/FriendIndex投影
6. 统计与普通通知
```

高水位时可以限制新 Generate/Redeem/Care/Theft Admission，但不能丢弃已提交的清理和 Workflow。

---

## 12. ACL、安全与敏感数据

### 12.1 访问矩阵

| 调用方 | Pair Record | Quota Record | ShareIssuer | Pair Order/Due | Projection |
|---|---:|---:|---:|---:|---:|
| player-svr FriendshipDomain | 对应脚本读写 | 对应脚本读写 | 对应脚本读写 | 创建/快速推进 | 只经Projection API |
| farm-svr | 禁止直连Redis；走29号RPC | 禁止 | 禁止 | 禁止 | 只读公开查询API |
| farm-gate | 禁止 | 禁止 | 禁止 | 禁止 | 禁止直写 |
| farm-worker | 只读Work所需来源摘要 | 禁止直接改Quota | 禁止直接改Token | Claim/Renew/Complete | 通过受控Writer发布 |
| ProjectionWriter | 只读固定字段 | 只读必要摘要 | 无 | 无 | 仅写Projection前缀 |
| 运维修复工具 | 审批+版本化脚本 | 同左 | 同左 | 同左 | 审批 |

当前 FriendshipDomain 与 player-svr 共进程可能共享一个平台身份，但代码仍分 Repository 和 Script
allowlist；未来拆 friend-svr 时不改变 Key 与 RPC 合同。

### 12.2 Token 与日志

禁止记录：

```text
raw ShareToken / token:envelope
ConnectionTicket / Cookie / SessionPermit
完整好友操作Payload
```

允许记录：

```text
token_id
截断token_digest
pair_id/operation_id的不可逆诊断Hash
authority_kind/bucket_id/script_version/result_class
```

Redis 慢日志和 APM instrumentation 也必须脱敏 ARGV；不能只约束业务日志。

---

## 13. 可观测性

至少提供：

```text
friend_authority_request_total{kind,operation,result}
friend_authority_latency_ms{kind,operation}
friend_authority_record_version_conflict_total{kind,operation}
friend_authority_domain_epoch_conflict_total{kind,operation}
friend_authority_unknown_total{kind,operation}
friend_authority_unknown_resolve_ms{kind,result}
friend_authority_attempt_fenced_total{kind}
friend_authority_open_attempt_count{kind}
friend_authority_attempt_due_lag_ms{kind}

friend_pair_record_bytes
friend_pair_receipt_count
friend_pair_pending_order_count
friend_pair_batch_read_size
friend_pair_batch_unknown_ratio
friend_pair_reactivation_barrier_count{state}
friend_pair_reactivation_pending_age_ms

friend_quota_active_count
friend_quota_reserved_count
friend_quota_grandfathered_overflow_total
friend_quota_release_fence_total
friend_quota_finalize_after_termination_total

share_token_generate_total{result}
share_token_validation_total{result}
share_token_validation_receipt_count
share_token_hot_key_qps
share_token_rate_limited_total{layer}

friend_order_due_lag_ms{type}
friend_order_retry_total{type,reason}
friend_receipt_compaction_lag_ms{kind}
friend_result_publish_lag_ms{command_type}
```

Metrics Label 禁止 player_id、pair_id、token_id、operation_id 等高基数字段。Trace/结构化日志可在受控字段
中记录截断 Hash，并带 `trace_id/script_name/version/bucket_id/record_version_before/after`。

---

## 14. 必须通过的测试

### 14.1 Key 与布局

1. Go、Lua Fixture、Worker、压测工具对相同 Authority ID 生成同 Bucket/Tag/Key；
2. 三种 Authority 即使 ID 数值相同也不共享 Tag；
3. 所有脚本 KEYS `AssertSameSlot`；故意混入另一 Bucket 必须测试失败；
4. 修改 BucketCount/Seed/LayoutVersion 的旧数据读取测试必须拒绝静默漂移。

### 14.2 Pair 并发

1. Delete 与 ActionAdmission 并发，只允许一种 Redis 顺序；
2. Block 与 Activate 并发，Block 先则 Activate 拒绝，Activate 先则 Block 终止；
3. 同 operation 同 payload 并发 100 次只产生一份 Receipt/Order；
4. 同 operation 不同 payload 返回 IdempotencyConflict；
5. 旧 FriendTerminationOrder 不撤销新 relation_epoch 的 Subscription/Quota；
6. Pair Commit 成功、ResultDirectory 写失败后，Worker 最终补齐目录。
7. 删除在两个ShareIssuer都不可用时仍立即撤权并返回成功，Barrier保持PENDING且重加被拒绝；
8. 删除处理中Rotate生成的Token不能激活；READY后显式RotateForPairReactivation才可激活；
9. 不存在关系的Delete/重复Unblock只写有界Receipt/ResultOrder，不创建空Pair主Record；
10. 同operation_id在不同FRIEND_PAIR Root不冲突；同Root不同payload返回IDEMPOTENCY_CONFLICT；
11. ActionAdmission后进程退出，Worker仅凭DispatchOrder内嵌Permit即可恢复，不重新授权。

### 14.3 Quota 并发

1. 300 个名额被并发 Reserve 时绝不出现第 301 个；
2. 配置 300→200、已有250时保留关系并拒绝新增；
3. 双方反向兑换使用稳定顺序和同一 Pair；
4. Finalize 先于 Termination Release：最终 active 不存在；
5. Termination Release 先于 Finalize：ReleaseFence 阻止迟到 Finalize；
6. 旧 epoch Release 不删除新 epoch active；
7. Reservation/Finalize/Release 每个故障点超时未知都由 Receipt/Fence 得到唯一结果。
8. Quota logical_bytes达到硬上限时新Reserve拒绝，但Finalize/Release与Termination清理仍可执行；
9. Field增删后的logical_bytes与离线精确重算一致，调用方伪造byte计数无效。

### 14.4 ShareToken

1. 同一 Token 100 个不同玩家并发兑换，形成独立 Workflow/Receipt，不维护剩余次数；
2. Validation 先于 Rotate：原 Workflow 可继续；Rotate 先：Validation拒绝；
3. Expire 与 Validation 按 Redis TIME 得到唯一顺序；
4. 普通 Generate 返回同一 Envelope；Rotate 后旧 Envelope 拒绝；
5. 原始 Token 不出现在应用日志、Trace、Metrics、错误或慢日志采集样本；
6. 热门 Token 压测记录单 Slot p50/p95/p99、冲突放大和复制延迟。
7. PENDING/READY barrier与Rotate并发时，只有匹配当前barrier_revision的ReactivationRotateReceipt有效。

### 14.5 Unknown 与 Failover

每个写脚本都执行：

```text
提交前断连
提交后响应前断连
Resolver先执行
原Commit先执行
Resolver自身超时
Redis Primary Failover
MOVED / ASK / NOSCRIPT
进程在Lua成功后、Dispatcher入队前退出
进程在PrepareAttempt成功后、Mutation发送前/后退出
```

最终只能进入：

```text
COMMITTED（有Receipt）
NOT_COMMITTED（有Attempt Fence，可按原业务身份新Attempt）
RECONCILE_REQUIRED（证据异常，禁止猜测）
```

重启后的 Repository 必须按 CommitIdentity 读取持久 AttemptCursor，继续解析原 ordinal；不得生成随机新
Attempt。Failover/MOVED/NOSCRIPT 后 ScriptRegistry 必须在实际目标 Primary 重新加载脚本，不能使用旧
Node 的“已加载”缓存。

### 14.6 Soak 与生命周期

- 2 小时高并发：Record、DueIndex、Receipt Count 不出现无法解释的单调增长；
- 24 小时稳态：Worker/Compactor 吞吐高于新增速度；
- 虚拟可信时间覆盖 72 小时：Full→Tombstone→Expired Evidence 不丢防重；
- 模拟投影集群不可用：好友权威写仍正确，ResultPublishOrder积压可恢复；
- 模拟 Authority Redis noeviction 水位：新业务有界拒绝，未决工作不被淘汰。

---

## 15. 实现顺序

### Step 1：类型与 Golden Test

```text
FriendshipAuthorityKind / BucketRef
KeyBuilder
root-scoped operation/attempt/receipt digest
CommitAttemptCursor / deterministic attempt ordinal
FriendRelationID / ReservationID / SourceOrderID
ReceiptRef / WorkRef
```

先证明跨进程结果一致，再写 Repository。

### Step 2：Fake Repository 与领域并发测试

实现与本文同样的条件结果，不模拟 Redis 延迟掩盖竞态。优先覆盖 Pair 删除/Barrier/Admission、Quota
Finalize/ReleaseFence、Token Validate/RotateForPairReactivation和Attempt重启恢复。

### Step 3：小 Lua + Script Contract Test

先实现：

```text
Pair Delete/Block/Unblock
Pair ReactivationFloor Commit / Activate
Quota Reserve/Finalize/Release
Share Generate/Rotate/RotateForPairReactivation/Revoke/Validate
Attempt Prepare / Resolver
```

每个脚本逐项测试 Key 类型、容量、幂等、不同 payload、版本冲突和写后异常禁令。

### Step 4：Order 与 ResultDirectory

实现 Pair SourceOrder/DueIndex、DirectDispatcher、Worker Claim/Complete 和 ResultPublish。先断开 Projection
Redis 验证业务事实不回滚，再恢复投影验证积压清空。

### Step 5：第 29 号 RPC Adapter

RPC Handler 只做可信上下文、Canonical Builder、Deadline、Repository 结果映射。补齐 Pair AddFriend
Participant 和 Quota 两类 Release 接口后，再允许多 Agent 并行实现业务 Handler。

### Step 6：平台探针与容量

验证公司 Redis：

```text
Cluster Hash Tag + Lua
Redis TIME
noeviction
主从确认与Failover RPO/RTO
备份恢复
ACL/TLS/慢日志脱敏
脚本/Key/Value大小限制
单Slot热门Token安全吞吐
长期存储成本
```

探针失败时保持 Repository 接口，评审 TcaplusDB/其他长期权威存储；禁止弱化 Receipt/Order 语义来迁就
存储产品。

---

## 16. 草案决策登记

| 编号 | 状态 | 草案决策 |
|---|---|---|
| 33A-01 | `INHERITED_FROZEN` | Pair/Quota/ShareIssuer均为非Actor Authority，不新增服务或Holder路由 |
| 33A-02 | `PLATFORM_PENDING` | 三类Authority使用独立friend-auth-v1稳定Bucket与Hash Tag；BucketCount/ReceiptPartitionCount由真实探针冻结，不写死4096/16 |
| 33A-03 | `FROZEN_33A` | 主Record、Receipt Partition、AttemptCursor、Resolution、Order/Due分别成Key但保持同Tag |
| 33A-04 | `FROZEN_33A` | 业务CommitIdentity与确定性commit_attempt_id分离；持久Cursor保存ordinal/candidate_hash；Unknown Fence阻断精确旧Attempt而不永久阻断原业务重试 |
| 33A-05 | `FROZEN_33A` | targeted append Lua使用领域Epoch条件并原子推进record_version，避免热门Token先读版本再CAS的冲突风暴 |
| 33A-06 | `FROZEN_33A` | Pair公开写与好友ActionAdmission原子创建ResultPublishOrder，按Root作用域可靠登记ResultDirectory根锚点 |
| 33A-07 | `FROZEN_33A` | Quota Termination Release写RelationReleaseFence并同时清理匹配Active/Reservation，阻止迟到Finalize |
| 33A-08 | `SECURITY_PLATFORM_PENDING` | Token使用经安全评审的authenticated envelope；当前Envelope作为受限敏感Authority字段保存以复用链接 |
| 33A-09 | `INHERITED_FROZEN` | 未决事实无普通TTL；Receipt服从Full→Tombstone→Expired Evidence |
| 33A-10 | `PLATFORM_PENDING` | Bucket数、Limit、SLO、Redis持久性、热门Token吞吐和长期存储成本以真实探针为准 |
| 33A-11 | `FROZEN_33A` | 删除/拉黑先撤权并写PENDING Barrier；TerminationOrder回填Generation Floor；READY后显式RotateForPairReactivation Receipt才允许重加；重激活与READY→CONSUMED同提交 |
| 33A-12 | `FROZEN_33A` | EffectiveIdempotencyKey包含OperationRootRef；Digest、ResultDirectory和查询Locator均校验Root，客户端不能指定内部Pair/Bucket |
| 33A-13 | `FROZEN_33A` | WorkAddress使用SourceAuthorityRef；ActionDispatchOrder内嵌完整不可变Permit；WorkType静态注册表拒绝非法来源/步骤组合 |
| 33A-14 | `FROZEN_33A` | 不存在关系的Delete/Unblock NO_CHANGE不创建空Pair主Record；Quota使用Lua维护logical_bytes；ScriptRegistry按Redis Node维护加载状态 |

---

## 17. 已回灌的上游修正与剩余平台问题

### 17.1 第 30 号 Unknown 已按 Attempt 收敛

第 30 号已经改为 Root 作用域 EffectiveIdempotencyKey、确定性 CommitAttemptID、持久 AttemptCursor 与
Attempt 级 ResolutionReceipt；不再使用 operation 级永久 ResolutionTombstone。

### 17.2 第 27/29/31 号 Authority 地址已统一

WorkAddress 已统一使用 `SourceAuthorityRef`，Pair Add/Activate 与 Quota 两类 Release 也已显式登记。正式
Proto 生成时只需固化 field number，并补齐 Barrier Floor/RotateForPairReactivation 的 Typed RPC。

### 17.3 Token Envelope 仍依赖安全平台

业务需要复用同一链接，但“只存 Hash”无法重新返回原 Token。本文推荐保存受控加密 Envelope；最终采用
哪种 AEAD/KMS/Keyring 必须由现有安全能力决定，不能在普通业务代码里自行发明。

### 17.4 Redis 长期容量仍需实证

15 亿量级 Pair 的 Redis 成本可能成为主要问题。33A 先证明 P0 正确性和接口，生产目标必须基于真实注册
用户、好友分布、存储开销与平台 RPO 重新选择。若换存储，必须保留本文原子集合、幂等、Unknown 和
Order 可恢复性，而不是退回“先写关系、再发内存消息”。

---

## 18. 当前收口状态

本文的正确性语义已与第 27～33 号对齐，可作为实现输入；平台参数与安全选型仍未 Freeze。后续验证顺序：

```text
CommitAttempt Prepare/Fence故障测试
→ ReactivationBarrier与Rotate并发测试
→ Quota ReleaseFence/logical_bytes测试
→ 热点Token语义CAS压测
→ Token Envelope安全评审
→ Bucket/Partition/Limit参数探针
```

前三项决定实现正确性；后续项决定安全和生产能力声明。Bucket 数与数值可以在接口不变的前提下通过探针
调整，不应重新打开已经冻结的产品规则。
