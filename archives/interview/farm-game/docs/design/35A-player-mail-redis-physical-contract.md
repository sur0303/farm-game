# 35A · PlayerMail 非 Actor 权威 Redis 物理合同

版本：`v1.0-player-mail-redis-physical-freeze-candidate`  
日期：2026-08-03  
状态：`STABLE_INPUT / PLATFORM_PARAMETERS_PENDING`  
冻结说明：`35A-01～35A-10` 的物理语义已经冻结；Redis 平台能力、容量阈值与最终脚本参数仍须通过探针和压测定版。  
定位：把第 35 号已经冻结的 MailEntryStore、已读/删除、六封分页、投递防重、claimed 投影、过期与
GC 语义映射为公司 Redis 上可实现、可测试的 Key、Hash Tag、Lua、Unknown、ACL 和容量合同。

权威关系：

- 第 28 号唯一拥有公网 Envelope、CommandCatalog、operation_id 和 Canonical Command Payload Hash；
- 第 29 号唯一拥有 Gate/FirstHop/Worker 的可信调用身份、Deadline 和 RPC 外壳；
- 第 30～31 号唯一拥有通用 Authority Commit、Receipt、ResultDirectory、Redis 时间和证据生命周期；
- 第 35 号唯一拥有邮件产品语义、TrustedMailClaimMaterial、双重防重和 PlayerActor 领取边界；
- 本文只回答“PlayerMail 非 Actor 权威怎样安全落入 Redis”，不得反向修改以上合同。

本文不新增 `mail-svr`、MailActor、应用层 Actor Route 或邮件专用 MQ。P0 仍然是：

```text
任意健康player-svr / PlayerMailDomain
→ 根据recipient_player_id计算MailPersistenceBucket
→ MailEntryRepository
→ Redis Cluster Primary上的同槽小型Lua
```

---

## 0. 一页结论

### 0.1 三类数据不能混成一个权威

| 数据 | P0 物理位置 | 是否最终权威 | 写入方 |
|---|---|---:|---|
| MailEntry、已读/删除、分页索引、MailboxSummary、DeliveryDedup | 本文 Mail Authority Redis | 是，针对邮件存在性和展示状态 | PlayerMailDomain |
| MailClaimState、MailClaimReceipt、资产、StoredOperationOutcome | 第 31 号 PLAYER Authority Redis | 是，针对领取与资产 | PlayerActor |
| claimed/红点/在线摘要 | Projection Redis | 否，可重建 | Dispatcher/worker/Reconcile |

`MailEntryStore` 是非 Actor Authority，因此不使用：

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
稳定MailPersistenceBucket
+ Redis Primary线性化
+ record/state/mailbox revision
+ stable delivery/mutation/work identity
+ 小型同槽Lua
+ item/byte双上限
+ DueIndex与有界Worker扫描
+ 防重Receipt/Tombstone
```

### 0.2 P0 主链

```text
内部投递
→ InsertMailIfAbsentV1
→ Entry + DeliveryDedup + Index + Summary + ExpireDue同提交
→ 提交后才允许Push MailSummaryChanged

邮件读取
→ MailboxIndex每页最多6封
→ HMGET Entry/State
→ 必要时批量精确读取Player MailClaimState

附件领取
→ 本文只提供TrustedMailClaimMaterial
→ PlayerActor原子发奖
→ projection_dirty_generation++
→ 最佳努力MailClaimProjectionOrder / Pull / Reconcile
→ ApplyMailClaimProjectionV1更新展示/红点
```

MailClaimProjection 是 `REBUILDABLE_PROJECTION`：Order 存在时由 Dispatcher/worker 加速，Order 未创建
或丢失时由精确 Pull/Reconcile 修复；它不得成为 PlayerActor 资产提交的强制前置条件。

### 0.3 七条物理不变量

```text
PM1 一次Lua涉及的全部Key必须具有逐字相同的Hash Tag；CROSSSLOT不能拆写绕过。
PM2 Entry、DeliveryDedup、Index、Summary必须同提交，禁止先写Entry再补索引。
PM3 claimed_projection只能false→true；Projection不能让资产重复到账。
PM4 所有列表、Lua、DueIndex、Receipt、正文和扫描都同时受item+byte上限约束。
PM5 Redis超时不能虚报成功；只能用原delivery/mutation/work身份重入并读取持久证据。
PM6 GC不能早于Claim/Unknown/Projection/客服窗口，也不能删除尚未领取的有效附件。
PM7 禁止KEYS、全库扫描、HGETALL邮箱和在Lua中解析复杂业务Workflow。
```

---

## 1. 范围与非目标

### 1.1 本文必须回答

1. 玩家邮箱怎样稳定分桶并映射到 Redis Slot；
2. Entry、State、Index、Summary、DeliveryDedup、ExpireDue、GcDue 使用哪些 Key；
3. Insert、MarkRead、Delete、ClaimProjection、Expire 和 GC 的原子边界；
4. 每页六封的排他游标怎样物理编码；
5. Redis Timeout Unknown 怎样安全重入；
6. 已领取投影延迟时怎样安全删除；
7. 60 封可见上限与物理保留上限怎样分离；
8. 3000 万 DAU 下必须采集哪些容量证据；
9. 哪些组件可以调用哪些脚本和前缀。

### 1.2 本文不定义

- H5 `.proto` field number；
- PlayerActor 内部资产提交和 OperationResult；
- 独立 mail-svr 或全服 GlobalMailCampaign 完整 Schema；
- 未来可见邮件 Scheduler；
- 公司 Redis 已经满足生产持久化、跨可用区和 3000 万 DAU 成本；
- TcaplusDB Body Store 的最终表结构。

如果公司 Redis 的持久化、成本或容量探针失败，应替换 `MailEntryRepository`，而不是改变 H5、Gate、
PlayerActor 或第 35 号领取合同。

---

## 2. Bucket、Hash Tag 与 KeyBuilder

### 2.1 Bucket 计算

P0 推荐独立于 Pod 数量的稳定分桶：

```text
hash_input = LP("player-mail-persistence-bucket-v1")
           || LP(canonical_recipient_player_id)

hash_value = XXH64(seed=0, input=hash_input)
bucket_id  = hash_value % mail_bucket_count
```

首轮参数候选：

```text
layout_version    = player-mail-v1
mail_bucket_count = 4096
```

`LP`、规范十进制 PlayerID 和 XXH64 服从第 27/31 号。BucketCount 是容量参数，不是 Redis Slot 数；一旦
存在真实数据，Hash 算法、Seed、BucketCount 或 layout_version 都不得原地修改，必须新布局迁移。

### 2.2 Hash Tag

```text
<mail_tag>
= {classic-farm:<env>:<zone>:player-mail:v1:b:<bucket_id>}
```

Redis 仍按 `CRC16(hash_tag) mod 16384` 选择 Slot。客户端、Gate 和内部业务调用方不能提交 bucket_id、
Hash Tag 或 Redis Key；只能由版本化 `PlayerMailKeyBuilder` 根据可信 recipient_player_id 生成。

### 2.3 Key 总表

```text
<mail_tag>:box:<player_id>          # HASH；Entry/State/Dedup/MutationReceipt/Summary
<mail_tag>:index:<player_id>        # ZSET；score固定0，member为SortMemberV1
<mail_tag>:safe-evict:<player_id>   # ZSET；只含已读且可安全淘汰的邮件
<mail_tag>:expire-due               # ZSET；同Bucket可见过期发现
<mail_tag>:gc-due                   # ZSET；同Bucket物理清理发现
<mail_tag>:bucket-meta              # HASH；布局、item/byte水位和扫描Cursor
<mail_tag>:scan-lease               # HASH；Worker分桶扫描短Lease
```

每个玩家使用一个 Mailbox HASH、一个可见 Index 和一个 SafeEvict Index，避免为每封邮件创建多个 Redis
Key。所有 Repository API 必须按明确 Field 做 HGET/HMGET，禁止对整个邮箱执行 HGETALL。

### 2.4 Mailbox HASH 字段

```text
_meta:schema_version
_meta:mailbox_revision
_meta:visible_count
_meta:retained_count
_meta:retained_bytes
_meta:unread_count
_meta:claimable_count
_meta:delivery_tombstone_count

entry:<mail_id>                     # Bounded MailEntryV1 bytes
state:<mail_id>                     # Bounded MailUserStateV1 bytes
delivery:<delivery_digest>          # DeliveryDedupV1
mutation:<mutation_digest>          # MarkRead/Delete稳定结果
projection:<work_digest>            # ClaimProjection幂等Receipt
```

Field 名必须经过 KeyBuilder/FieldBuilder 生成。`mail_id`、delivery_id、operation_id 不允许未经规范化直接
拼接分隔符。Hash Value 使用带 schema_version 的有界编码；Lua 只读取它需要的少量标量或固定小字段，
不在脚本中反序列化正文和 RewardBundle。

---

## 3. 六封分页的物理排序

第 35 号逻辑游标是排他的 `(created_at_ms, mail_id)` 倒序。物理上所有 Index member 使用同一 score=0，
并按 Redis Lex 排序：

```text
SortMemberV1
= uint64_big_endian(MAX_UINT64 - created_at_ms)
  || 0x00
  || UTF8(canonical_base64url_mail_id)
```

较新的时间得到更小的前缀，因此 `ZRANGEBYLEX` 正序就是“新到旧”；同毫秒用 mail_id 稳定打破平局。
对 H5 暴露的 cursor 是 Base64URL_NoPadding(SortMemberV1)，服务端使用 exclusive bound 继续扫描。

分页算法：

```text
scan_cap_items = cfg.mail.page_scan_items      # 首轮候选24
scan_cap_bytes = cfg.mail.page_scan_bytes      # 首轮候选64KiB
page_size      = 6

ZRANGEBYLEX有界取候选
→ HMGET对应entry/state
→ 过滤已删除/已过期
→ 必要时精确读取Player MailClaimState
→ 最多返回6封
```

新邮件不会插入旧 cursor 的后续页；删除/过期导致候选不足时有界多取，达到 item/byte 上限后允许少于
六封。可靠来源 Order 的 `created_at_ms` 来自来源 Authority Commit 捕获的可信时间并随快照冻结；受信
Direct Insert 使用 Mail Store Redis TIME。来源展示事件时间另放 `display_event_at_ms`，不能由 H5 或
不受信来源决定分页排序键。

必须为 Go/TS/Redis 测试生成 SortMember 黄金样例，覆盖同毫秒、最大时间、mail_id 边界和排他 cursor。

---

## 4. Lua 脚本族与原子边界

Lua 只允许固定、小型、版本化脚本；所有脚本首先验证 layout/schema、KEYS Hash Tag、ARGV item/byte
上限和调用方允许的状态版本。脚本返回结构化 ResultCode，不返回拼接日志文本。

### 4.1 InsertMailIfAbsentV1

同槽原子完成：

```text
检查Redis TIME与visible_at_ms（P0必须立即可见）
→ 查询delivery:<delivery_digest>
   ├── 同delivery + 同delivery_payload_hash：返回原mail_id
   └── 同delivery + 不同hash：DELIVERY_ID_CONFLICT
→ 校验单Entry、可见、保留、Tombstone的item/byte上限
→ 可见已满时从safe-evict有界淘汰最旧安全邮件
  （原子写auto_evicted/deleted状态、移出Index、修正Summary并登记gc-due）
→ 写entry:<mail_id>与state:<mail_id>
→ 写DeliveryDedupV1
→ ZADD index / expire-due
→ 更新mailbox_revision、visible/retained/bytes/unread/claimable
→ 返回mail_id + mailbox_revision + summary
```

安全淘汰条件只能是：`read=true && (no_attachment || claimed_projection=true)`。SafeEvict Index 没有
候选时返回 `MAILBOX_FULL_PROTECTED`；禁止脚本为了找候选遍历 60 封正文。一次 Insert 最多淘汰
`cfg.mail.max_lazy_evict_per_insert`（首轮候选 4），不足时明确失败。

### 4.2 MarkMailReadV1

```text
校验recipient/mail_id/state_version
→ 已读：返回原NO_CHANGE
→ 首次写read_at_ms=Redis TIME
→ unread_count--（仅当前仍可见时）
→ 若已满足安全淘汰条件，加入safe-evict
→ mailbox_revision++
→ 保存mutation:<digest>稳定结果
```

MarkRead 不进入 PlayerActor，也不改变 claimed 业务真值。

### 4.3 DeleteMailV1

Repository 在调用脚本前确定 `DeleteEligibility`：

```text
无附件                         → ALLOW
当前Redis TIME已超过claim_end  → ALLOW
claimed_projection=true        → ALLOW
投影显示未领取                 → 精确读取Player MailClaimState
  ├── 有ClaimReceipt           → ALLOW_WITH_CLAIM_PROOF
  ├── 确认无Receipt且未过期    → REJECT_UNCLAIMED
  └── 读取未知                 → PROCESSING/RETRY，不调用删除Lua
```

Lua 只接受内部构造、与 mail_id/claim_material_hash 匹配的 ClaimProof；H5 不得提交。允许删除时原子设置
deleted_at_ms、移出 Index/SafeEvict、修正未读/可领取/可见计数、推进 revision、写 mutation Receipt 并
登记 gc-due。异步 ClaimProjection 后到时只能更新投影/Receipt，不能清除 deleted_at 或重新 ZADD Index。

### 4.4 ApplyMailClaimProjectionV1

输入来自第 35 号 `MailClaimProjectionOrder`：

```text
work_id / work_version
player_id / mail_id
claim_receipt_ref
claim_material_hash
claimed_at_ms
```

脚本原子完成：

```text
检查projection:<work_digest>防重
→ 校验mail_id与claim_material_hash
→ claimed_projection只能false→true
→ 若仍可见且此前计入claimable，claimable_count--
→ 若已读，加入safe-evict
→ mailbox_revision++
→ 写ProjectionReceipt
```

目标邮件已经逻辑删除时仍可记录 Receipt，但不得恢复 Index；目标已物理 GC 时返回稳定
`TARGET_ALREADY_PURGED`。GC 保留窗口必须使这种情况只在 Order 已完成或超出明确恢复窗口后出现。

### 4.5 ExpireMailVisibilityV1

Worker 从 expire-due 取得候选后，脚本用 Redis TIME 重新检查 `expire_at_ms + mail_version`：

```text
未到期/版本不匹配 → NO_CHANGE并修正DueRef
已经到期
→ 从Index/SafeEvict移除
→ 修正visible/unread/claimable
→ mailbox_revision++
→ 登记gc-due
```

Worker 到期只改变“是否展示/计红点”，不替代 PlayerActor 对正在进行的 ClaimAttempt 的解析。

### 4.6 GcMailV1

GC 必须再次检查：

```text
deleted或expired
&& Redis TIME >= physical_purge_at_ms
&& physical_purge_at_ms >= claim_material_purge_at_ms
&& 不存在未完成ClaimProjection恢复要求
&& DueRef中的mail_version/state_version仍匹配
```

满足后删除 Entry/State/Index 残留并修正 retained item/byte。DeliveryDedup 按自己的
`delivery_retain_until_ms` 压缩或删除，不能随 Entry 一起提前消失；Operation/Claim Tombstone 位于
PLAYER Authority，完全不由本脚本清理。

---

## 5. Timeout Unknown 与幂等重入

MailEntryStore 没有 WriterEpoch，但“没有 Actor”不等于可以盲重试。每类写都有稳定身份：

| 操作 | 稳定身份 | Unknown 后动作 |
|---|---|---|
| Insert | recipient + delivery_id + delivery_payload_hash | 用原 delivery_id 重入，返回原 mail_id 或冲突 |
| MarkRead | recipient + mail_id + mutation_id | 读 mutation Receipt 或原身份重入 |
| Delete | recipient + mail_id + mutation_id | 读 mutation Receipt 或原身份重入 |
| ClaimProjection | work_id + work_version | 读 ProjectionReceipt 或原 Work 重入 |
| Expire/GC | DueRef + mail/state version | 重新检查当前状态，旧版本 NO_CHANGE |

Repository 对 Redis timeout 必须返回 `TIMEOUT_UNKNOWN/PROCESSING` 或执行内部解析，不能把 Socket timeout
映射成业务失败，更不能生成新 delivery_id/operation_id。重入脚本先读持久 Receipt/Dedup，再决定是否
首次执行，因此即使原 Lua 已经提交但响应丢失，也不会重复插入、重复扣摘要或复活邮件。

MarkRead/Delete 是否要求公网 operation_id 由第 28 号 CommandSpec 决定；本文的 `mutation_id` 是服务端
稳定 Mutation Identity，可以由 operation digest 或命令确定性主键派生，不能由客户端直接选择 Redis Field。

---

## 6. Receipt、保留与容量

### 6.1 逻辑可见与物理保留分开

稳定产品值：

```text
mail_page_size          = 6
max_visible_mail_count  = 60
```

首轮物理参数候选：

```text
max_retained_mail_count       = 96
max_retained_mail_bytes       = 768KiB
max_delivery_tombstone_count  = 256
max_lazy_evict_per_insert     = 4
page_scan_items               = 24
page_scan_bytes               = 64KiB
```

所有候选都必须配置化、版本化并通过真实分布修订。达到 item 或 byte 任一上限都必须 Fail Closed，禁止
只检查条数。受保护邮箱已满时 P0 返回 `MAILBOX_FULL_PROTECTED`，不设无界 overflow。

### 6.2 Claim 材料与 Receipt 下限

MailEntry/RewardBundle 的物理清理必须满足第 35 号：

```text
claim_material_purge_at_ms >= receipt_retain_until_ms

receipt_retain_until_ms
>= claim_end_at_ms
 + max(client_retry_window,
       unknown_resolve_window,
       operation_result_window,
       worker_reconcile_window,
       customer_service_query_window)
```

MailEntryStore 可以删除正文前，必须确保 PlayerAuthority 的 MailClaimReceipt/Tombstone 与旧 operation
证据仍满足第 31 号 Full → Tombstone → Expired Evidence。本文不能删除 PLAYER Authority 证据。

### 6.3 DeliveryDedup 生命周期

DeliveryDedup 至少保存：

```text
recipient_player_id
delivery_id
delivery_payload_hash
mail_id
first_committed_at_ms
delivery_retain_until_ms
```

Full Entry GC 后可压缩为 DeliveryTombstone，但在来源 Order 最长重试、DeadLetter/客服查询和审计窗口
结束前不能删除。相同 delivery_id 的“不存在”不得在旧来源仍可重放时被解释为可创建新邮件。

### 6.4 生产容量结论仍待探针

`3000万 × 60 × 8KiB ≈ 14.7TB` 只是最坏 Entry Payload，尚未包含 Key、Index、复制、备份和碎片。
因此 P0 可用公司 Redis 跑通纵向链路，但生产必须实测：

- 平均/分位标题、正文、附件和 Entry 字节；
- 每玩家可见/保留/Tombstone 分布；
- Slot/Primary 热点和大 Key；
- RDB/AOF、主从切换、备份恢复与 RPO/RTO；
- Redis 全 Entry 与 Tcaplus Body + Redis Meta/Index 两种成本和 p99。

如迁移 Body，`MailEntryRepository` 仍必须为 ClaimCoordinator 提供强读的不可变 Claim Material；不能只
保留可能丢失的缓存副本。

---

## 7. DueIndex、Worker 与 Reconcile

### 7.1 DueRef

```text
MailDueRefV1 {
    due_kind              # EXPIRE_VISIBILITY | PHYSICAL_GC
    player_id
    mail_id
    mail_version
    expected_state_version
}
```

编码使用第 31 号长度前缀 + Base64URL 规范，score 为绝对 `due_at_ms`。DueIndex 只是发现索引；脚本仍
以 Entry/State 和 Redis TIME 为真值。旧 DueRef 只能返回 NO_CHANGE/修复索引，不能误删新版本。

### 7.2 分桶扫描

farm-worker 按稳定 Bucket 候选分配并在 `<mail_tag>:scan-lease` 获取短 Lease；每批同时受：

```text
max_due_items
max_due_bytes
max_script_calls
time_budget_ms
```

限制。禁止单 Worker 扫描全库、禁止 `KEYS`。Worker 停止只会延迟隐藏/GC；ReadAPI 仍按可信当前时间
过滤过期邮件，因此不会把过期附件错误显示成可领取。

### 7.3 Reconcile

允许周期性抽样重建：

- Index 与 Entry/State 是否对应；
- Summary 计数与有界扫描结果是否一致；
- ClaimProjection 与 Player MailClaimState 是否最终一致；
- expire/gc DueRef 是否缺失或重复；
- DeliveryDedup 是否指向错误 mail_id/hash。

Reconcile 只能修复索引、摘要和投影；发现权威内容冲突必须告警/隔离，不能静默重写 RewardBundle 或
MailClaimReceipt。

---

## 8. ACL、日志与安全

推荐最小权限：

| 调用方 | 允许 |
|---|---|
| player-svr / PlayerMailDomain | Mail KeyBuilder、读 API、Insert/MarkRead/Delete 脚本 |
| player-svr / PlayerMailClaimIngress | Entry Primary 强读，构造 TrustedMailClaimMaterial |
| farm-worker | ClaimProjection/Expire/GC 脚本、DueIndex Claim、Reconcile 受限接口 |
| farm-gate / H5 | 只能调用业务 RPC，不允许访问 Redis |
| 运营/测试入口 | 只能调用带服务身份和审计的 InsertSystemMail，不允许提交 Redis Key |

Redis ACL 禁止应用账号执行 `KEYS`、`FLUSH*`、任意 `EVAL` 和未登记脚本；脚本通过版本化 SHA Registry
加载。日志/Trace 可记录 player_id 脱敏值、mail_id、delivery_id、operation_id、Hash、revision、bucket、
result code，不记录正文、密码、Cookie、完整 RewardBundle 或签名材料。

---

## 9. 指标、告警与压测

至少采集：

```text
mail_insert_total{result/source/priority}
mail_delivery_conflict_total
mailbox_visible_items / retained_items / retained_bytes histogram
mailbox_full_protected_total
mail_list_latency / scanned_items / returned_items
mail_mark_read_total / mail_delete_total
mail_projection_lag_ms / reconcile_mismatch_total
mail_expire_due_backlog / mail_gc_due_backlog / oldest_age_ms
mail_redis_timeout_unknown_total
mail_script_latency_ms{script/result}
mail_slot_bytes / hot_key_qps / redis_failover_errors
```

压测至少覆盖：

1. 离线定向补偿突发与稳定 delivery_id 重放；
2. 3000 万玩家分布模型下同时打开邮箱，每页六封；
3. 同玩家多端并发 MarkRead/Delete/ClaimProjection；
4. 60 封全部受保护和安全惰性淘汰；
5. 同毫秒大量邮件、游标分页期间插入/删除；
6. Redis timeout、主从切换、Lua 响应丢失后的原身份重入；
7. Worker 停止、Due backlog 增长、恢复和旧 DueRef；
8. 大正文、byte 上限、Slot 倾斜与大 Key；
9. Player Claim 成功但 Projection 延迟时的列表、删除和红点修复。

任何单机 Mock 或少量串行测试都不能宣称已支持 3000 万 DAU。

---

## 10. 脚本和 Repository 清单

实现 Agent 只能从以下版本化入口开始，不得创建万能事务 Lua：

```text
PlayerMailKeyBuilderV1
PlayerMailFieldBuilderV1
MailSortMemberCodecV1
MailDueRefCodecV1

InsertMailIfAbsentV1
MarkMailReadV1
DeleteMailV1
ApplyMailClaimProjectionV1
ExpireMailVisibilityV1
GcMailV1

MailEntryRepository
MailReadRepository
MailMaintenanceRepository
```

每个脚本必须有：固定 KEYS/ARGV Schema、最大输入字节、结构化结果、同槽断言、黄金样例、重复执行、
Timeout Unknown、错误 Hash、容量满和 Failover 测试。

---

## 11. 冻结索引与平台待验证项

### 11.1 语义已冻结

| 编号 | 决策 |
|---|---|
| 35A-01 | PlayerMailDomain 是非 Actor 权威，不使用 VirtualShard Holder/WriterEpoch |
| 35A-02 | recipient 稳定分桶；一次 Lua 的 Key 使用同一版本化 Hash Tag |
| 35A-03 | Entry + DeliveryDedup + Index + Summary + DueIndex 同槽原子提交 |
| 35A-04 | 每页最多6封，使用排他 `(created_at_ms, mail_id)` 与稳定 Lex SortMember |
| 35A-05 | Insert/MarkRead/Delete/Projection/Expire/GC 只使用固定小型脚本 |
| 35A-06 | Unknown 以稳定 delivery/mutation/work 身份重入，不虚报成功 |
| 35A-07 | claimed 投影单调且可修复，不能替代 Player MailClaimState |
| 35A-08 | 可见60封与物理保留/字节/Tombstone上限分离 |
| 35A-09 | GC 服从 Claim/Unknown/Projection/客服窗口，不删除 PLAYER Authority 证据 |
| 35A-10 | P0 只支持立即可见纯文本定向邮件，不实现 Campaign 和未来可见 Scheduler |

### 11.2 平台/容量待验证

1. 公司 Redis Cluster 的 Lua、Hash Tag、ACL、noeviction、持久化与故障切换能力；
2. 4096 MailPersistenceBucket 是否产生可接受的 Slot/Key 分布；
3. Mailbox HASH 60～96 Entry 下的真实字节、rehash 和 p99；
4. DueIndex、Scan Lease 与 Worker 批量参数；
5. 物理参数 96/768KiB/256/24/64KiB 是否适合真实流量；
6. Redis 全 Entry 与 Tcaplus Body Store 的生产成本选择；
7. 最终脚本 SHA、Key 前缀、`.proto` 字段号和滚动升级兼容性。

这些项目阻塞“生产容量已证明”和最终参数定版，不阻塞领域结构、Fake Repository、脚本合同测试和
单机/测试 Redis 纵向链路实现。
