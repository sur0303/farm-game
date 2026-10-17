# 35 · 系统邮件与附件领取合同

版本：`v1.2-pet-grant-exclusion-freeze`  
日期：2026-08-03  
状态：`STABLE_INPUT`  
定位：在第 26～34 号已经冻结的 Session、Actor、Authority Store、Redis、核心经营、好友和任务合同
之上，定义 P0 系统邮件的投递、分页读取、已读/红点、过期、附件领取、幂等与容量边界。本文不重新
定义 PlayerActor 资产、RewardBundle 或公网 OperationResult 语义。

## 0. 上位合同与不重复定义

- [26 · 接入与会话](./26-access-session-contract.md)：认证、在线会话与 Downlink；
- [27 · 路由、Actor 与 Workflow](./27-access-routing-actor-workflow-freeze.md)：PlayerActor、
  PlayerMailDomain、Order/Dispatcher/Worker；
- [28 · 公网协议与 CommandCatalog](./28-command-catalog-public-protocol.md)：公开命令、幂等和结果恢复；
- [29 · 内部 RPC](./29-internal-rpc-trusted-context.md)：受信内部调用、Participant 与 Worker 恢复；
- [30 · Actor 权威 Store](./30-actor-authority-store-contract.md)：Authority Commit、CAS、Unknown 和 Receipt；
- [31 · Redis 物理合同](./31-actor-authority-redis-physical-contract.md)：Player Authority、Projection、
  item/byte 有界和时间合同；
- [32 · 核心经营场景](./32-core-business-scenario-contract.md)：Player 资产和 `MailClaimReceipt` 上层边界；
- [34 · 任务系统](./34-task-system-contract.md)：RewardBundle、Player 内原子发奖和红点投影模式。
- [35A · PlayerMail 非 Actor 权威 Redis 物理合同](./35A-player-mail-redis-physical-contract.md)：
  MailEntryStore 的 Key、Hash Tag、Lua、分页索引、过期/GC 和容量实现。

冲突时服从上位合同。邮件查询副本、在线 Push 或 MailEntry 展示状态不得取代 PlayerActor 对附件
资产和防重 Receipt 的最终裁决。

本文只拥有邮件产品语义、领域权威和领取业务合同；第 35A 号只负责把这些语义落入公司 Redis，
不能反向改变本文。第 28 号继续唯一拥有公网 Envelope/CommandCatalog，第 29 号继续唯一拥有内部
调用身份和 RPC 外壳。

### 0.1 本版修订后的六条硬不变量

```text
M1  operation_id只存在于ClientCommandEnvelope，邮件Typed Payload不得重复携带。
M2  MailEntry/已读由PlayerMailDomain裁决；资产到账和MailClaimState由PlayerActor裁决。
M3  Claim必须同时受OperationKey和MailClaimKey保护，换operation_id不能重领同一mail_id。
M4  PlayerActor只接受服务端生成的不可变TrustedMailClaimMaterial，不读取MailEntryStore，也不采信客户端附件。
M5  投递Hash、领取材料Hash和公网命令Payload Hash职责分离。
M6  Full Outcome可以压缩，但防重Tombstone不得在邮件仍可领取或Unknown仍可恢复时删除。
```

---

## 1. 已确认产品输入

1. P0 邮箱只接收系统邮件，不提供好友之间互发邮件；
2. P0 至少支持邮件列表、查看正文、附件领取和邮件红点；
3. 邮件附件是普通 Player 资产奖励，不直接修改 FarmActor 地块；
4. P0 不建设完整 GM 平台，系统流程或受信测试/运营入口可以创建邮件；
5. 邮件功能是 player-svr 的领域能力，不因为存在 `MailDomain` 就自动拆成独立 MailSvr。

## 2. 已冻结边界与实现接缝

第 27、31、32 号当前给出的边界是：

```text
player-svr
├── PlayerActor(player_id)
│   └── MailClaimState / MailClaimReceipt  # 附件到账与防重权威
├── PlayerMailDomain                       # MailEntry投递、分页、正文和已读状态
├── PlayerMailReadAPI                      # 邮件列表/详情查询
└── PlayerCoordinator                      # ClaimMailAttachment首跳协调

farm-worker
└── MailDelivery/Projection/Reconcile      # 只恢复投递和查询投影，不拥有资产
```

`MailEntry` 列表和正文采用独立、有界、可分页的玩家邮件存储，不随 PlayerActor 每次激活全部载入内存；
附件领取必须进入 PlayerActor，并把奖励资产、`MailClaimReceipt` 和 OperationResult 一次可靠提交。
P0 不创建 MailActor，也不创建独立 `mail-svr`。

### 2.1 Q35-01 冻结：邮件权威、部署与路由边界

`PlayerMailDomain` 与 PlayerActor 同属 `player-svr` Deployment，但不是 PlayerActor 子状态，也不拥有
Actor Mailbox：

```text
player-svr
├── PlayerMailDomain
│   ├── MailEntryStore
│   ├── MailReadAPI / MarkMailRead
│   └── MailDeliveryAPI
└── PlayerActor(player_id)
    └── MailClaimState / MailClaimReceipt
```

权威划分如下：

| 数据 | 最终权威 |
|---|---|
| 邮件存在性、收件人、标题、正文、附件快照、创建/过期时间 | MailEntryStore |
| 已读状态 | PlayerMailDomain |
| 附件是否已经真正领取 | PlayerActor.MailClaimReceipt |
| 金币、种子、道具和经验是否到账 | PlayerActor |
| 邮件 claimed 展示状态、未读数与红点 | 可重建查询投影 |

MailEntry 的 claimed 字段即使存在，也只是由 MailClaimReceipt 异步刷新的展示副本，不能防重发奖。
完整邮件列表不进入 PlayerActor 常驻状态，避免 Actor 激活加载正文、离线投递激活 Actor、邮件 GC
占用 Actor Mailbox 以及正文变化放大 Player 资产 CAS。

不同命令采用两类路由，内部投递不属于公网命令：

```text
公网：
GET_MAIL_LIST / GET_MAIL_DETAIL / MARK_MAIL_READ / DELETE_MAIL
→ 无状态服务路由
→ 任意健康 player-svr/PlayerMailDomain Pod
→ 直接访问 MailEntryStore/Projection Redis
→ 不激活 PlayerActor

CLAIM_MAIL_ATTACHMENT
→ PLAYER Route Domain
→ hash(player_id) → PLAYER VirtualShard → Assignment Holder Pod
→ PlayerMailClaimIngress / ClaimMailCoordinator
→ 读取MailEntry并构造TrustedMailClaimMaterial
→ PlayerActor Mailbox

内部：
InsertSystemMail / ResumeMailDeliveryOrder / ApplyMailClaimProjection
→ 只接受第29号定义的受信服务身份
→ 不暴露给H5
```

无状态邮件读写不使用应用层一致性哈希绑定 Pod；Pod 由服务发现/负载均衡选择，Redis Cluster 再按
带 player_id 的 Key 决定数据分片。只有需要 PlayerActor 单一写权的命令才使用第 27 号定义的
VirtualShard Assignment。未来可以把同玩家请求优先发给某 Pod 作为缓存亲和优化，但它不参与
正确性，也不能替代 Holder/Writer Fence。

`CLAIM_MAIL_ATTACHMENT` 使用 PLAYER Actor Affinity 的目的，是先把请求送到 PlayerActor 当前 Holder，
并不表示 Gate 直接把 `mail_id` 塞进 Mailbox。Holder 内的 `PlayerMailClaimIngress` 必须先完成 MailEntry
读取和可信材料构造；这一步可以等待 Redis，但尚未占用 Actor Mailbox。只有不可变材料准备完毕后才
提交本地 Mailbox，Actor Turn 内不得再等待邮件正文存储。

### 2.2 Q35-02 冻结：邮件类型、投递来源与全服邮件边界

P0 只实现定向系统邮件，不提供玩家互发、好友赠送或公开客户端投递接口。邮件可以是纯文本或携带
普通 Player 资产附件，来源限于带服务身份和审计信息的内部领域 Order、受信测试/运营 API 或后台
补偿工具。客户端只读取和操作自己收到的邮件，不能指定收件人、正文或附件。

P0 标题和正文只支持 UTF-8 纯文本，H5 必须使用文本渲染组件，不允许 `innerHTML` 直接解释正文。
禁止任意 HTML、脚本、事件属性、`javascript:` URL 和外部 iframe。模板参数必须在服务端完成 Schema
校验、长度限制和转义；日志只记录 template/version/hash，不记录完整正文和附件敏感字段。未来如需
富文本，必须另行冻结受限 Markup 与服务端净化合同。

P0 至少用欢迎邮件或定向补偿邮件打通：

```text
玩家建档/受信投递
→ WelcomeMailDeliveryOrder/MailDeliveryOrder
→ PlayerMailDomain.InsertMailIfAbsent
→ 玩家读取邮件
→ ClaimMailAttachment进入PlayerActor
```

P0 不实现全服邮件物化。以下 `GlobalMailCampaign` 只属于 `DEFERRED_P1_BOUNDARY` 的方向示意，
不是允许 Agent 直接生成代码或固化字段号的完整 Schema：

```text
GlobalMailCampaign {
    campaign_id
    campaign_seq
    content_snapshot
    attachments
    start_at_ms
    expire_at_ms
    claim_end_at_ms
    audience_rule
}
```

玩家读取邮箱时把有效 Campaign 与个人 MailEntry 惰性合并；只有实际阅读或领取时才创建少量
`GlobalMailUserState`/MailClaimReceipt。禁止发布 Campaign 时向 3000 万玩家预写 3000 万条
MailEntry。P1 首版 audience 只应支持简单、可稳定计算的全体或固定分群，不引入实时复杂圈选。

P1 实现前还必须单独确认 Campaign/个人邮件混合游标、撤回与换版、稳定虚拟 mail_id、晚注册可见性、
Audience 复核和 GlobalMailUserState/Player MailClaimReceipt 防重；这些问题不阻塞 P0 定向系统邮件。

邮件不能替代已经冻结的核心结算恢复。例如 TheftSettlement、CareRewardOrder 或任务领奖失败时，
必须恢复原 Workflow/Order，不能擅自改成发奖励邮件，否则会破坏原 operation_id 和 Receipt 语义。

### 2.3 Q35-03 冻结：MailEntry、已读与领取状态分层

P0 使用服务端生成、不可猜测、固定有界的 Base64URL 字符串 `mail_id`；禁止空白、路径字符和客户端
自定义 ID。推荐按第 31 号 `StableOpaqueID` 确定性派生：

```text
mail_id = StableOpaqueID(
  "farm-mail-id-v1",
  canonical_recipient_player_id,
  delivery_id)
```

相同投递身份因此天然定位同一 mail_id，但 Insert 脚本仍必须校验 delivery_payload_hash，不能只凭 ID
认定内容相同。MailEntry 至少保存：

```text
MailEntry {
    mail_id
    mail_version
    recipient_player_id
    delivery_id
    source_type
    source_id
    title_snapshot
    body_snapshot
    reward_bundle_snapshot
    template_version
    reward_config_version
    delivery_payload_hash
    claim_material_hash
    created_at_ms
    visible_at_ms
    expire_at_ms
    claim_end_at_ms
}
```

投递成功后，收件人、标题/正文快照、奖励内容与 Hash、创建和过期时间不可原地修改；模板后续换版
不能改变旧邮件奖励。需要纠错时创建新的投递或使用后续显式撤销合同，不能静默篡改历史 MailEntry。

三类 Hash 必须分开，禁止继续使用一个含义模糊的 `attachment_hash`：

```text
operation_payload_hash
= CanonicalHash(ClaimMailAttachmentRequest.mail_id)
# 第28号公网命令幂等；防止同operation_id换mail_id

delivery_payload_hash
= CanonicalHash(recipient + delivery_id + source + title/body snapshot
                + claim_material_hash + visible/expire/claim_end
                + template/config version)
# InsertMailIfAbsent投递防冲突

claim_material_hash
= CanonicalHash(recipient + mail_id + mail_version + delivery_id
                + RewardBundle完整规范字段 + claim_end_at_ms
                + reward_config_version)
# 绑定PlayerActor实际领取资格与奖励内容
```

所有 Hash 输入采用固定字段顺序、长度前缀和稳定标识，不直接 Hash JSON、日志文本或未确定性的
Protobuf Marshal bytes。`purge_at_ms` 属于物理维护参数，不进入领取业务 Hash，避免调整 GC 策略时
使一封已经投递的邮件失效。

已读和删除属于独立的邮件用户状态：

```text
MailUserState {
    player_id
    mail_id
    read_at_ms
    deleted_at_ms
    claimed_projection
    claim_receipt_ref
    claim_material_hash
    claimed_projection_at_ms
    claim_projection_version
    state_version
}
```

标记已读由无状态 PlayerMailDomain 处理，不进入 PlayerActor。`claimed_projection*` 只用于快速
展示；最终领取真值位于 PlayerActor：

```text
MailClaimReceipt {
    mail_id
    first_operation_id
    mail_version
    claim_material_hash
    reward_result
    claim_decision_at_ms
    claimed_at_ms
    receipt_retain_until_ms
}
```

邮件不能压成单一 `UNREAD/READ/CLAIMED/EXPIRED` 状态，因为已读、已领取和过期是正交事实。查询时
分别根据 `read_at_ms`、可信服务端时间和 MailClaimReceipt/其查询投影计算。知道 mail_id 不能绕过
`recipient_player_id` 校验。

一封邮件的附件是不可分割的 `RewardBundle`，P0 一次全部领取，不支持部分领取。MailEntry 与
MailUserState 存在独立 MailEntryStore（P0 使用公司 Redis），MailClaimReceipt 存在 PlayerActor
Authority Redis，未读数和 claimed 展示状态存在 Projection Redis。邮件模板以版本化配置缓存提供，
但领取只认投递时冻结的 RewardBundle 与 `claim_material_hash`。

P0 邮件 RewardBundle 只允许普通金币、种子、普通道具和经验，不允许 `PetUnlockGrant`、土地直接解锁、
付费货币或跨 FarmActor 奖励。宠物金币购买和任务奖励解锁分别服从第 36/34 号；邮件不能复用任务的
PetUnlockGrant 绕过宠物配置版本、`pet_state_version` 与产品入口。未来若产品明确要求“邮件赠送宠物”，
必须先扩展本合同、TrustedMailClaimMaterial、RewardBundle Canonical Hash 和验收矩阵，不能只在
PlayerActor Handler 临时识别新字段。

### 2.4 Q35-04 冻结：幂等投递、Order 与在线 Push

每次投递使用稳定身份：

```text
MailDeliveryIdentity {
    recipient_player_id
    delivery_id
    delivery_payload_hash
}
```

`delivery_id` 不存在时创建 MailEntry；已存在且 delivery_payload_hash 相同时返回原 mail_id；同一 delivery_id
搭配不同 payload 返回 `DELIVERY_ID_CONFLICT` 并告警。欢迎邮件可以使用
`WELCOME:{player_id}:{welcome_version}` 等确定性身份。

`MailDeliveryOrder` 必须在创建时冻结完整、自包含的不可变快照：

```text
MailDeliveryOrderPayload {
    recipient_player_id
    delivery_id
    source_type / source_id
    title_snapshot / body_snapshot
    reward_bundle_snapshot
    delivery_payload_hash / claim_material_hash
    created_at_ms / visible_at_ms / expire_at_ms / claim_end_at_ms
    priority
    template_version / reward_config_version
}
```

Worker 重试只能重放这份快照，不能根据 delivery_id 重新读取“当前模板”或“当前奖励配置”，否则同一
delivery_id 会在重试时产生不同 Hash。受信 Direct API 即使不创建 Order，也必须先构造同样的不可变
快照，再调用 InsertMailIfAbsent。

`PlayerMailDomain.InsertMailIfAbsent` 必须在同一玩家邮件分桶的小范围原子操作中完成：

```text
MailEntry
+ DeliveryDedup
+ 玩家分页索引
+ mailbox_revision++
+ 未读/可领取摘要变化
+ 过期与GC发现索引
```

不能留下 Entry 存在但列表不可发现、或邮件可见但红点摘要完全未登记的半成品。具体
Redis Key/HashTag/Lua 由第 35A 号冻结。

P0 所有定向邮件都要求：

```text
visible_at_ms <= InsertMailIfAbsent线性化时的Redis TIME
```

也就是首版只支持“插入后立即可见”。未来定时可见邮件必须另增 FutureVisibleIndex 和
ActivateVisibleMail 原子脚本；在此之前不得把未来邮件提前计入分页、未读数或红点。

如果邮件是另一个已提交业务事实的副作用，来源权威必须与 `MailDeliveryOrder + DueIndex` 原子提交，
由 DirectDispatcher 立即投递、farm-worker 恢复失败；如果“插入邮件”本身就是受信 API 的唯一目的，
可以直接执行 InsertMailIfAbsent，提交成功后返回，超时用原 delivery_id 重试，不为形式创建 Workflow。

投递不查询 SessionRoute、不激活 PlayerActor，离线玩家同样先持久化 MailEntry。提交成功后才允许查询
SessionRoute 并最佳努力推送 `MailSummaryChanged`，Push 只携带 mail_id、mailbox_revision、未读数和
附件提示，不携带完整正文。Push 丢失由后续 Pull 修复，重复 Push 由 mail_id/revision 去重，Push 失败
不能回滚邮件。P0 不引入邮件专用 MQ。

### 2.5 Q35-05 冻结：分页、精确领取状态与邮件红点

邮件列表由任意健康 `player-svr/PlayerMailReadAPI` 无状态读取 MailEntryStore，不激活 PlayerActor。
P0 每页最多返回六封，使用 `(created_at_ms, mail_id)` 倒序的游标分页，不使用页码/offset；列表只返
回标题、时间、已读、附件和领取摘要，正文由 `GetMailDetail` 按 mail_id 单独读取。

游标冻结为 `exclusive(last_created_at_ms, last_mail_id)`，服务端向 H5 返回不透明编码，客户端不能
构造 Redis Key 或内部排序成员。`created_at_ms` 由 Insert 时的可信服务端时间生成；来源事件时间如需
展示，另用 `display_event_at_ms`，不能改变分页顺序。

并发分页语义：

```text
第一页返回后新插入的邮件
→ 不插入旧游标的后续页面；刷新第一页后可见

分页期间邮件被删除或过期
→ ReadAPI有界多取，尽量补满6封
→ 达到scan_item/scan_byte上限后允许本页少于6封
```

禁止为了“恰好六封”无界扫描或使用 offset 回退。相同毫秒的邮件以 `mail_id` 稳定打破平局，物理编码
由第 35A 号定义并通过黄金样例锁定。

当前页先读取 claimed 展示投影；对“有附件且投影显示未领取”的条目，ReadAPI 可以批量只读
Player Authority Redis 的 `model:mail_claim_state` 精确检查 MailClaimReceipt，不进入 Actor Mailbox。
若权威 Receipt 已存在，则本次立即显示已领取并触发投影修复。精确检查失败时显示 Loading/未知，
不能把未知误显示成可领取。

`DeleteMail` 同样不能只相信异步 claimed 投影：投影显示已领取时可以直接删除；投影显示未领取且
邮件有附件时，先精确读取 `model:mail_claim_state[mail_id]`。权威已领取则允许删除并修复投影；读取
未知返回 `PROCESSING/RETRY`；只有确认未领取且未过期时才返回 `MAIL_HAS_UNCLAIMED_ATTACHMENT`。
后到的 ClaimProjection 必须单调写 `false → true`，不得清除 `deleted_at_ms` 或把邮件重新放回列表。

邮件红点定义为：

```text
unread_count > 0 || claimable_attachment_count > 0
```

玩家打开详情后 `MarkMailRead` 首次写入 `read_at_ms`、推进 mailbox_revision 并减少未读数；重复调用
返回 `NO_CHANGE`。附件领取可以在成功后异步标记已读，但资产成功不依赖已读写入。

Projection Redis 保存 `MailRedPointProjection(player_id, mailbox_revision, unread_count,
claimable_attachment_count)`，并参与总 RedPointProjection 聚合，不拆 redpoint-svr。在线 Push 是最佳
努力，登录/打开邮箱 Pull 修复；投影不能参与领取裁决。投影不可用只影响红点，MailEntryStore 不可用
时邮件面板局部 Loading/重试，均不得阻塞登录或农场玩法。

### 2.6 Q35-06 冻结：附件领取、Receipt 与 Unknown

公网 `ClaimMailAttachmentRequest` 只携带 `mail_id`；`operation_id` 只来自第 28 号公共 Envelope，
player_id 只来自 Gate 的受信 Session。Gate 按 PLAYER Route Domain 找到 Holder Pod，Holder 内
`PlayerMailClaimIngress/ClaimMailCoordinator` 读取 MailEntry Primary，进行前置检查并构造：

```text
TrustedMailClaimMaterial {
    schema_version
    mail_id
    mail_version
    recipient_player_id
    source_type / source_id
    delivery_id
    reward_bundle_snapshot
    claim_material_hash
    visible_at_ms
    claim_end_at_ms
    issued_at_ms
    material_expire_at_ms
    issuer
    audience = PLAYER_AUTHORITY
}
```

P0 中 Coordinator 与 PlayerActor 同 Deployment，由 `PlayerMailPort` 生成强类型不可变对象并本地提交
Holder Mailbox，不要求密码学签名。未来若拆成独立 mail-svr 或跨信任域 RPC，必须使用第 29 号 mTLS
服务身份，并增加签名材料或等价可信信封；H5、Gate 和普通内部服务不得自行构造上述对象。

`material_expire_at_ms` 只限制一份内部材料能否被新的 Actor Turn 接纳；`claim_end_at_ms` 决定邮件
业务上能否领取。材料短 TTL 不能缩短原邮件领取期。命令已经形成 Authority CommitAttempt 后，即使
材料随后过期，也必须按原 Attempt/Receipt 解析，不能重新读取邮件后改判。

Coordinator 对可见时间和领取期限的检查只是快速拒绝，不是最终业务决定。PlayerActor Turn 开始时
捕获一次 `trusted_now_ms`，并按以下顺序处理：

```text
1. 校验issuer/audience/recipient/mail_version/material TTL/claim_material_hash
2. 查询OperationOutcome[operation_digest]
   ├── 同operation + 同mail_id：返回原终态
   └── 同operation + 不同mail_id：IDEMPOTENCY_CONFLICT
3. 查询MailClaimState[mail_id]
   └── 已由其他operation领取：保存本operation的NO_CHANGE终态
4. 以本Turn trusted_now_ms检查visible_at_ms/claim_end_at_ms
5. 冻结claim_decision_at_ms和RewardBundle
6. 发放金币/种子/普通道具/经验
7. 原子写MailClaimState/MailClaimReceipt、StoredOperationOutcome和projection_dirty_generation
```

这里存在两种独立防重键：

```text
OperationKey = (player_id, CLAIM_MAIL_ATTACHMENT, operation_id, PLAYER root)
MailClaimKey = (player_id, mail_id)
```

前者防同一网络业务意图重放，后者防客户端换 operation_id 重领同一邮件。不同 operation 重领已领取
mail_id 时，`NO_CHANGE/MAIL_ALREADY_CLAIMED` 本身也必须与当前 operation 的 StoredOperationOutcome
可靠保存；否则响应丢失后无法通过 GetOperationResult 恢复。

Wallet、Inventory、Experience/Level、MailClaimState、MailClaimReceipt、StoredOperationOutcome 和
`mail_projection_dirty_generation++` 必须一次 crash-durable Authority Commit。提交成功后可以在同一
输出中携带有界 `MailClaimProjectionOrder` 给 DirectDispatcher；该 Order 属于
`REBUILDABLE_PROJECTION`，容量不足或投递失败不得反向阻止资产提交。farm-worker 在 Order 已存在时
恢复，邮箱 Pull 的精确回源和 Reconcile 在 Order 丢失时修复投影。提交成功后 H5 立即把当前邮件显示
为已领取，投影失败不回滚资产。

过期的业务决定点是 PlayerActor Turn 捕获 `trusted_now_ms` 的时刻：Turn 开始时已经过期，返回
`REJECTED/MAIL_CLAIM_EXPIRED`；Turn 开始时未过期且已经构造并发出 Authority Commit，即使 Redis
响应或 Unknown Resolver 跨过 claim_end，也继续解析原 operation，不能改成过期失败。Receipt 保存
`claim_decision_at_ms` 供审计和边界测试。

不存在/非本人、无附件、已过期、材料过期和内容 Hash 冲突分别使用稳定错误。Authority Commit
超时未知返回 `PROCESSING`，客户端只能查询或重试原 operation_id。P0 一次只领取一封邮件的完整
RewardBundle，不实现批量一键领取。Actor 不读取标题/正文，也不能根据客户端提交的附件发奖。

### 2.7 Q35-07 冻结：容量、过期、删除与 GC

P0 邮箱采用以下有界配置：

```text
mail_page_size = 6
max_visible_mail_count = 60
max_retained_mail_count = cfg.mail.max_retained_count       # 首轮候选96
max_retained_mail_bytes = cfg.mail.max_retained_bytes       # 首轮候选768KiB
max_delivery_tombstone_count = cfg.mail.max_delivery_tombstones  # 首轮候选256
default_retention = 30 days
max_retention = 90 days
gc_grace = 7 days
max_title_bytes = 256
max_body_bytes = 4 KiB
max_attachment_kinds = 8
max_mail_entry_bytes = 8 KiB
```

只有 `mail_page_size=6` 和 `max_visible_mail_count=60` 是当前产品冻结值；物理保留条数、字节和 Delivery
Tombstone 数是第 35A 号的首轮有界参数，必须按真实邮件频率、平均正文大小和 Redis 成本压测调整。
“页面最多 60 封”不等于“底层最多只有 60 条”，因为已删除/过期 Entry、投递防重和领取证据仍可能
处于保留窗口。

P0 不支持永久邮件，默认 `claim_end_at_ms = expire_at_ms`。过期后邮件不再默认展示、不能领取且不
计入红点，但先保留七天 GC 宽限。玩家可以删除纯文本、已领取或已过期邮件；尚未领取附件的邮件
返回 `MAIL_HAS_UNCLAIMED_ATTACHMENT`，不能删除。删除先写 deleted_at_ms 并隐藏，后台延迟物理清理。

投递前先做有界惰性清理，只允许淘汰最旧且“已读，并且无附件或附件已领取”的安全邮件；禁止自动
删除未领取附件。如果六十封全部受保护，则返回 `MAILBOX_FULL_PROTECTED`。受信直接投递明确失败，
DeliveryOrder 退避重试并在截止后进入 DeadLetter/告警，不能静默丢弃或虚报成功。

P0 暂不增加关键邮件预留槽或 protected overflow。客服补偿邮件也必须遵守同一容量边界；如果全部
60 封都受保护，明确失败、重试并最终告警。核心资产结算和 Workflow 恢复不得把“邮件一定能插入”
作为最后正确性防线。未来有真实运营需要时，可通过配置化 critical reserved slots 演进，而不改变
领取权威。

MailEntry 写入时登记过期时间桶，farm-worker 分桶、分批 GC；打开邮箱可以顺带进行单次有界清理，
禁止 Redis 全库 `KEYS` 扫描。物理删除前必须覆盖 Claim/Operation 最长重试和 Unknown 窗口，并保留
必要 delivery 防重墓碑；mail_id 永不复用。

邮件领取证据的最小保留下限为：

```text
receipt_retain_until_ms
>= claim_end_at_ms
 + max(client_retry_window,
       unknown_resolve_window,
       operation_result_window,
       worker_reconcile_window,
       customer_service_query_window)

claim_material_purge_at_ms >= receipt_retain_until_ms
```

Full MailClaimReceipt 到期后可以压缩为包含 `mail_id + operation_digest + claim_material_hash +
claimed_at_ms + final_status` 的 MailClaimTombstone，但不能让同一 mail_id 或旧 operation 再次生效。
OperationOutcome/Expired Evidence 的长期语义继续服从第 31 号，不在本文复制第二套删除规则。逻辑删除
可以立即隐藏邮件，物理 GC 必须等待 ClaimAttempt、投影 Order 和上述证据窗口全部安全结束。

P0 MailEntryStore 使用公司 Redis。生产容量证据若证明冷正文超出 Redis 预算，可以把 MailEntry 和
长期分页数据迁到 TcaplusDB，只把热索引、MailboxSummary、红点留在 Redis；PlayerActor 中的
MailClaimReceipt 与现有领取协议不变。

按最坏上限估算，`3000万玩家 × 60封 × 8KiB` 的原始 Entry 已约 14.7 TB，尚未包含 Redis 对象、索引、
复制、备份和碎片。因此“完整正文放公司 Redis”是 P0 实现与容量探针方案，不是已经证明的 3000 万
DAU 生产结论。生产评审必须比较：Redis 全 Entry，以及 Tcaplus 保存 Body/Claim Material、Redis 只保留
Meta/Index/Summary 两种布局；无论正文冷热迁移，领取所需的不可变材料都必须可靠可读。

### 2.8 Q35-08 冻结：故障、监控、压测与验收

故障必须按权威等级降级：MailEntryStore 查询失败只让邮件面板 Loading/重试；在线 Push 或红点投影
失败由 Pull/Reconcile 修复；InsertMailIfAbsent 超时使用原 delivery_id 重试；Actor Redirect 使用原
operation_id 到新 Holder；PlayerActor Authority Commit 超时未知返回 `PROCESSING`。任何查询副本
故障都不能重复发奖，任何资产结果不确定都不能提前返回成功。

farm-worker 停止时 Delivery/Projection Order 留在 DueIndex，恢复后继续；GC 停止只允许暂时增加
存储并告警，不能误删有效邮件；模板配置非法拒绝新快照并继续使用最后有效版本；邮箱全部为受保护
邮件时明确返回 `MAILBOX_FULL_PROTECTED`。

必须监控投递结果/延迟/冲突、Order backlog 与 oldest age、DeadLetter、每玩家邮件 item/byte 分布、
列表延迟、邮箱满保护、过期与 GC backlog、领取结果/Unknown/重复、Receipt 冲突、投影延迟与修复
差异、Material 过期/Hash/issuer 校验失败、同 operation 重放、换 operation 重领、Push 成败，以及
Redis 内存/Slot 倾斜、PlayerActor Mailbox 和 Writer Fence。Trace 记录
delivery_id、mail_id、operation_id、来源、Route/Holder 和结果，不记录完整正文、密码或 Cookie；
运营/测试投递必须带审计身份。

35 号冻结后，第 28 号 CommandCatalog 实现映射至少增加：

```text
GET_MAIL_LIST
GET_MAIL_DETAIL
MARK_MAIL_READ
DELETE_MAIL
CLAIM_MAIL_ATTACHMENT
```

不存在公网 `SEND_MAIL/CREATE_MAIL`。内部接口至少区分 `InsertSystemMail`、
`ResumeMailDeliveryOrder` 与 `ApplyMailClaimProjection`。

P0 验收至少覆盖：投递幂等和内容冲突、DeliveryOrder 模板换版重试、离线投递、Push 丢失后 Pull、
每页最多六封的排他游标分页、并发插入/删除、已读与红点、claimed 投影精确修复、双端并发领取、
同 operation 换 mail 冲突、换 operation 重领同 mail、Material 篡改/过期、Claim Unknown、截止时间前后
边界、删除与投影竞态、未领附件保护、六十封满容量、Worker 恢复、无全库扫描，以及列表/详情/投递
不激活 PlayerActor、只有附件领取进入 Actor。

压测必须覆盖离线定向补偿突发、同时打开邮箱、同时领取、幂等重放、Worker 停止恢复、Redis 慢响应、
接近六十封时的分页/插入/GC 和多 Pod 无状态读/Actor Holder 路由。3000 万 DAU 的实例数和 Redis
容量只能由容量模型与真实压测给出，不能由本文宣称。

---

## 3. 冻结索引

| 编号 | 问题 | 状态 |
|---|---|---|
| Q35-01 | PlayerMailDomain、MailEntry 与 PlayerActor 的权威/部署边界 | **已冻结**：Entry独立分页；领取进入PlayerActor；读写无状态路由 |
| Q35-02 | 邮件类型、投递来源与是否支持全服邮件 | **已冻结**：P0定向纯文本系统邮件；全服Campaign仅P1边界 |
| Q35-03 | MailEntry 身份、不可变内容、已读状态与领取状态分层 | **已冻结**：三类Hash分责；内容不可变；领取只认Player Receipt |
| Q35-04 | InsertMailIfAbsent、投递 Order、重复投递与在线 Push | **已冻结**：稳定投递ID、自包含Order、立即可见、先持久化后Push |
| Q35-05 | 分页、未读/可领取红点、查询投影与精确回源 | **已冻结**：每页最多6封、排他游标、删除/claimed精确回源 |
| Q35-06 | ClaimMailAttachment 的资产、Receipt、过期与 Unknown 语义 | **已冻结**：TrustedMaterial、双重防重、Turn时间裁决、Player原子发奖 |
| Q35-07 | 邮件保留期、删除、容量上限、溢出和 GC | **已冻结**：6/页、60可见、物理保留另限、Receipt下限、保护未领奖励 |
| Q35-08 | 故障降级、监控、压测与最小验收 | **已冻结**：分层降级、全链监控、功能与容量验收 |

Q35-01～Q35-08 已全部冻结。本文成为系统邮件的现行稳定输入；Redis Key/Lua 服从第 35A 号，最终
Proto field number 仍由正式 `.proto` 兼容性评审冻结。实现 Agent 不得改变本文的权威、容量、双重
防重、过期决定点与失败语义。
