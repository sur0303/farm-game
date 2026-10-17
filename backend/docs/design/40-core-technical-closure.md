# 40 · 经典农场协议与技术收口总控

版本：`v1.1-bootstrap-friend-opportunity-contract-alignment`  
日期：2026-08-03  
状态：`STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING`  
定位：第 26～37 号现行合同的唯一导航、协议收口和实现准入总控。本文只汇总已冻结结论，不重新定义
业务规则、Proto 字段、Redis Key 或 Lua；发生冲突时，必须回到对应的唯一权威合同处理。

> 本文不再保存某台 Linux 机器、某个临时分支或某次提交的“代码现状”。实现完成度必须由带
> `repository + branch + commit + test command + raw result` 的独立验证记录证明。这样长期合同不会
> 因代码快照过期而再次变成 `NEEDS_REBASE`。

---

## 0. 收口结论

截至本版，经典农场后台已经完成 L0～L2 的语义收口：

```text
L0  整体架构与部署边界                已收口
L1  Session、路由、Actor、Workflow    已收口
L2  公网协议、内部RPC、权威存储与场景  已收口
L3  Redis平台参数、容量阈值、最终IDL   待探针/压测/代码验证
```

最终主干为：

```text
H5 / 微信小程序 WebView
        │
        ├── HTTPS + JSON
        │   注册、账密登录、Cookie免密换票、静态资源与低频接口
        │
        └── WSS + Binary Protobuf / FarmFrame
            在线命令、CommandResult、Snapshot、Delta、Push与控制帧
                              │
                              ▼
                          farm-gate
            连接、Session、协议防护、CommandCatalog、首跳与下行背压
                    │                         │
                    ▼                         ▼
               player-svr                farm-svr
          PlayerActor(player_id)     FarmActor(farm_id)
          玩家资产与成长权威          农场地块与互动权威
          Friendship/PlayerMail      SubscriptionHub与FarmRead
          非Actor权威领域模块          跨Actor场景Coordinator
                    │                         │
                    └──── Workflow / Order ──┘
                              │
                              ▼
                         farm-worker
               DueIndex恢复、重试、投影、对账与P1调度
                              │
                              ▼
                    公司 Redis（P0 实现介质）
        Actor Authority / 非Actor Authority / Work / Runtime / Projection
```

固定原则：

1. P0 只有四个 Deployment：`farm-gate`、`player-svr`、`farm-svr`、`farm-worker`。
2. 任务、邮件、宠物、图鉴、好友不是各自独立微服务；它们是有明确权威边界的领域模块。
3. 只有 `PlayerActor(player_id)` 与 `FarmActor(farm_id)` 两类权威 Actor。
4. 好友关系和 PlayerMail 是非 Actor 权威；无状态处理进程不等于数据没有权威或没有并发控制。
5. P0 正确性不依赖 MQ。来源事实与必要 Order/DueIndex 原子提交，Dispatcher 快速推进，worker 恢复。
6. 同一 FarmActor 内串行裁决，不拆 PlotActor；不同农场、不同玩家天然并行。
7. 同场上限按 200 人设计：FarmActor 只生成一次权威变化，Hub 按 Gate 批量下行，慢连接相互隔离。
8. Redis 是当前 P0 物理实现选择，不改变 Repository/AuthorityStore 抽象；后续迁移存储不改公网协议。

---

## 1. 唯一权威链与当前版本

| ID | 当前版本 | 唯一拥有的语义 | 本文如何使用 |
|---|---|---|---|
| 26 | v1.3 | Auth、Ticket、Session、PendingJoin、ActiveSubscription、200 人下行与首屏查询接缝 | 只引用接入与 Join 状态机 |
| 27 | v4.2 | Command 首跳、双 Route Domain、Actor Runtime、Workflow/Order | 只引用执行主干与部署归属 |
| 28 | v2.8 | FarmFrame、33 类 CommandSpec、公开 Result/Push | 只汇总协议形态与命令全集 |
| 29 | v2.11 | 三类可信内部调用、路由头、Join 内部握手与Ready后Navigation接缝 | 只汇总 RPC 信任链 |
| 30 | v1.0 | Writer Fence、CAS、Unknown、Persistence Lane | 只汇总逻辑持久化不变量 |
| 31 | v1.4 | Actor Redis Key/Slot/Lua/DueIndex/Projection/容量与连续导航Runtime | 只汇总 Actor 物理分区 |
| 32 | v0.29 | 核心经工商店、土地、种植、成熟、收获、护理、偷菜 | 只汇总场景执行类型 |
| 33 | v1.2 | 好友、分享、删除/拉黑、权限、再激活屏障与连续导航语义 | 只汇总产品与权限语义 |
| 33A | v0.3 | 好友非 Actor 权威的 Redis Key/Lua/GC/ACL | 只汇总物理权威形态 |
| 34 | v1.3 | 每日/成长任务、进度、领取与任务红点 | 只汇总任务接缝 |
| 35 | v1.2 | 系统邮件、六封分页、附件领取 | 只汇总邮件产品与领取边界 |
| 35A | v1.0 | PlayerMail 非 Actor 权威 Redis 物理合同 | 只汇总十项已冻结物理语义 |
| 36 | v1.1 | 宠物解锁、喂食、切换、守护同步与偷菜截留 | 只汇总宠物接缝 |
| 37 | v1.1 | 植物图鉴、活动档案、解锁与 Seen Ack | 只汇总图鉴接缝 |
| 60 | v1.5 | 接入与会话验收 | 只作为验收证据，不定义语义 |

冲突处理顺序固定为：

```text
对应编号的唯一权威合同
        > 本文汇总结论
        > core-scenario-casebook案例说明
        > legacy、下载附件和历史评审稿
```

版本号、状态和协作锁以 [CONTRACT-MANIFEST](./CONTRACT-MANIFEST.md) 为唯一总账。

---

## 2. 部署单元与进程内模块

### 2.1 farm-gate

职责：

- HTTPS Auth Adapter：注册、登录、Cookie 免密换票、ConnectionTicket；
- WSS Transport Adapter：Direct WebSocket 为默认实现，TCONND + gateproxy 仅是可替换候选；
- `ClientHello` 核销一次性 Ticket，建立受信 Session；
- `ConnectionRegistry`、每连接有界 `ConnectionWriter`、sequence 与帧大小防护；
- `CommandCatalog` 查表、Session Admission、目标字段校验与第一跳选择；
- 本地不可变 Assignment Snapshot、Holder gRPC 连接池、Redirect 处理；
- Snapshot/Delta/Result/KICK/REJOIN_REQUIRED 下行。

明确不负责：

- 不直接扣金币、种子或改地块；
- 不编排 Workflow Participant；
- 不持有好友、邮件、任务或农场业务权威；
- 不等待浏览器真正显示，也不让慢连接阻塞业务 Actor。

### 2.2 player-svr

进程内包含：

```text
PlayerActor(player_id)
├── Economy / Wallet / Inventory / ShopCounter
├── Progression / Level / PlotUnlock资格
├── TaskState / RewardReceipt
├── PetState / GuardSyncOrder
├── CompendiumState / SeenRevision
└── MailClaimState / MailClaimReceipt

FriendshipDomain（非Actor权威）
├── FriendRelation / Block / Delete
├── ShareToken / Quota / ReactivationBarrier
└── FriendRead / Permit API

PlayerMailDomain（非Actor权威）
├── MailEntryStore / DeliveryDedup
├── Read/Delete/List/Detail
└── ClaimMaterial读取；资产领取仍进入PlayerActor
```

公开资料、好友列表、邮件列表等读接口可以作为 `player-svr` 内的 Role 横向扩容，不新增
`query-svr`、`friend-svr` 或 `mail-svr`。

### 2.3 farm-svr

进程内包含：

```text
FarmActor(farm_id)
├── Plot / CropCycle
├── CareIncident
├── TheftAttemptReceipt / TheftSettlement
├── GuardSnapshot
├── FarmVersion / FarmActivitySeq
└── FarmOpportunityHeader/Summary来源事实

FarmSubscriptionHub（同Holder运行时组件，不属于FarmActor强事实）
├── Presence / Subscription
├── JoiningBuffer / post-ready buffer
├── 共享Snapshot与版本变化环
└── 按Gate分组批量下行

FarmReadAPI
└── 好友页面与互动机会的精确BatchEvaluate

FarmOperationCoordinator
└── Plant / Harvest / Theft等跨Actor Workflow首轮推进
```

`Presence`、`JoiningBuffer` 和连接投递目标不写入 FarmActor 权威记录；FarmActor 重建不应搬运
200 人连接状态。

### 2.4 farm-worker

职责：

- 从 `DueIndex` 认领未完成 Workflow/Order；
- 校验 Lease 后调用 Participant 恢复推进；
- 发布 ResultDirectory、好友机会、红点等可重建查询副本；
- 对账来源事实、Order、投影与索引；
- P1 承载成熟提醒 Scheduler，但不把作物写成 Mature。

`farm-worker` 是 Go 进程；Redis 保存 WorkRecord、DueIndex、Lease 与证据。Redis 不是“worker
实现”，MQ 也不是 P0 worker 的前提。

### 2.5 何时才拆新服务

只有至少一个条件被真实证据满足时再拆：

1. 独立容量曲线长期压垮当前 Deployment；
2. 独立发布节奏或故障域有明确收益；
3. 安全/合规要求独立 ACL；
4. 数据权威与路由域已经客观独立；
5. 压测证明拆分收益大于一次 RPC、部署和一致性成本。

---

## 3. 公网协议收口

### 3.1 通道选择

| 通道 | 编码 | 适用范围 | 原因 |
|---|---|---|---|
| HTTPS | JSON | 注册、登录、Cookie免密换票、低频静态接口 | 浏览器生态成熟，便于认证与调试 |
| WSS | Binary Protobuf | 在线命令、结果、Snapshot、Delta、Push | 双向长连接、低头部开销、强类型演进 |

传输适配层不得污染业务协议：Direct WebSocket 与 TCONND 都必须向上提供同样的“完整二进制消息、
连接标识、发送结果、断开事件”接口，`FarmFrame` 不依赖 TCONND 私有字段。

### 3.2 FarmFrame 使用混合方案

```text
少量、稳定的顶层消息大类
→ FarmFrame.oneof

数量多、持续增长的业务命令
→ ClientCommandEnvelope.command_type + typed protobuf payload bytes
→ CommandCatalog负责唯一解码、准入、路由、超时、幂等与限流
```

顶层类别固定区分：

```text
ClientHello / ServerHello
ClientCommandEnvelope
CommandResultEnvelope
ServerPushEnvelope
ControlFrame
```

这样可以在编译期区分“结果”和“主动 Push”，同时避免每增加一个业务命令就修改 FarmFrame 顶层
结构。业务 Payload 不是任意 bytes；必须由 `CommandType` 对应的唯一 `CommandSpec` 解码。

### 3.3 33 类公开命令

| 分组 | CommandType |
|---|---|
| 运行时与查询 | `JOIN_FARM`、`GET_PLAYER_BOOTSTRAP`、`GET_FRIEND_LIST`、`GET_NEXT_FRIEND_OPPORTUNITY`、`GET_OPERATION_RESULT` |
| 任务 | `GET_TASK_LIST`、`CLAIM_TASK`、`CONFIRM_FRIEND_VISIT` |
| 邮件 | `GET_MAIL_LIST`、`GET_MAIL_DETAIL`、`MARK_MAIL_READ`、`DELETE_MAIL`、`CLAIM_MAIL_ATTACHMENT` |
| 宠物 | `GET_PET_STATE`、`PET_UNLOCK_BY_COIN`、`PET_FEED`、`PET_SWITCH` |
| 图鉴 | `GET_COMPENDIUM`、`ACK_COMPENDIUM_SEEN` |
| 好友与分享 | `GENERATE_SHARE_TOKEN`、`ROTATE_SHARE_TOKEN`、`REVOKE_SHARE_TOKEN`、`REDEEM_SHARE`、`DELETE_FRIEND`、`BLOCK_PLAYER`、`UNBLOCK_PLAYER` |
| 经营 | `BUY`、`SELL`、`UNLOCK_PLOT`、`PLANT`、`HARVEST`、`CARE`、`THEFT` |

任何公开命令必须恰好注册一个 `CommandSpec`，至少声明：

```text
PayloadType / ResultType / Decoder / MaxPayloadBytes
SessionAdmissionPolicy
ExecutionKind / FirstHopRole / RouteDomain / RouteKeyExtractor
IdempotencyPolicy / RetryPolicy / TimeoutBudget / RateLimitPolicy
```

CI 必须验证：命令总数为 33、无重复注册、无缺失 Decoder、无未注册公开枚举、Payload/Result 类型
与生成代码一致。

### 3.4 Result、Push 与状态

`CommandResult` 回答某次 `request_id / operation_id`；`ServerPush` 描述 Subscription 或服务端主动
变化。切换农场后：

- 旧农场 Snapshot/Delta/FarmChange 受 `subscription_id` Fence 拦截；
- 切换前已经 ADMITTED 的命令结果仍可返回，只更新操作状态，不应用到新农场页面；
- 旧连接已失效时不把旧结果偷运到新连接，客户端用原 `operation_id` 查询恢复。

状态固定为：

| 状态 | 含义 |
|---|---|
| `ACCEPTED` | 运行时已接受，但完成条件尚未成立；典型为 Join |
| `SUCCEEDED` | 本命令要求的权威事实与可靠提交已经完成 |
| `PROCESSING` | 持久 Workflow 已建立，跨权威结果仍在推进 |
| `REJECTED` | 未产生目标业务事实 |
| `NO_CHANGE` | 命令合法但无需再次改变，例如护理已被别人完成 |

`RetryDirective` 与业务状态正交，明确客户端是重发、查询原结果、Fresh Join、重新认证还是停止。

### 3.5 三个身份字段不得混用

```text
request_id
→ 一次网络请求与响应关联，可因重试变化

operation_id
→ 一次业务意图，弱网重试必须保持

payload_hash
→ 证明同operation_id仍是同一份规范化业务内容
```

同一幂等键和相同 Hash 返回原 Workflow/结果；同一幂等键但不同 Hash 返回
`IDEMPOTENCY_CONFLICT`，不能把新操作当作旧操作重放。

---

## 4. 内部 RPC 与信任链收口

### 4.1 三类调用必须分离

| 调用链 | Context | 典型调用者 | 能证明什么 |
|---|---|---|---|
| Gate → FirstHop | `TrustedCommandContext` | farm-gate | 已认证玩家、Session Fence、当前 ActiveSubscription、Deadline |
| Coordinator → Participant | `CoordinatorWorkContext` | farm-svr/player-svr内Coordinator或DirectDispatcher | 已存在 Workflow/Order、当前 Participant Step、不可变输入 |
| Worker → Participant | `InternalCallerContext + WorkLease` | farm-worker | 对同一持久 Work 的合法恢复权，不伪造在线 Session |

`ActorRouteHeader` 与上述身份正交，只回答：

```text
RouteDomain + ActorId
VirtualShard + AssignmentRevision
ExpectedWriterEpoch/Claim
目标Holder与Redirect Fence
```

操作者 ID 不能替代目标 Actor ID；Worker 不能伪造成在线玩家；公网客户端不能提交任何内部 Context。

### 4.2 RPC 形态

玩家命令主链使用 Unary gRPC，运行在 HTTP/2 连接池上：

- 每条请求拥有独立 Deadline、状态码和 Redirect；
- gRPC 连接复用，不等于每条命令重新建 TCP；
- 同一个 Actor 的消息进入 Mailbox 后仍按顺序裁决；
- 不同玩家/农场请求可并行占用不同 HTTP/2 Stream。

长 Streaming RPC 只用于 Assignment Watch、配置或持续事件流，不用于玩家命令主链。

### 4.3 执行接受边界

```text
Gate只负责选择FirstHop并转发
→ FirstHop完成自身AcceptPolicy
→ 返回ACCEPTED / PROCESSING / Terminal
```

Gate 不得直接依次调用 PlayerActor、FarmActor 推进 Workflow。首次推进由场景所属 Coordinator
负责；超时后由 farm-worker 恢复。

Redirect 只允许发生在目标尚未接受本次执行之前。接受之后客户端使用同一 `operation_id` 查询或
重试原工作，不能在另一个 Holder 创建第二份业务。

### 4.4 内部来源防伪

P0 信任边界为：

```text
公网只暴露farm-gate
→ 业务RPC端口仅允许受信工作负载访问
→ mTLS/工作负载身份 + K8s NetworkPolicy/ACL
→ 服务端白名单校验调用角色
→ Context中的玩家与Fence由Gate从本地Session生成
```

具体证书平台与服务身份机制仍属部署探针，但代码接口必须从一开始区分外部 Payload 与内部可信
Context，不能把“以后会上 mTLS”作为当前混用结构的理由。

---

## 5. Session、Join 与实时同步收口

### 5.1 登录与建连

```text
账密注册/登录
→ HTTPS Auth
→ PBKDF2校验、限流、SessionAuthority.Begin
→ Set-Cookie: farm_session（签名HttpOnly Cookie）
→ 返回一次性ConnectionTicket

WSS连接
→ ClientHello(connection_ticket)
→ Gate原子核销Ticket并绑定会话
→ ServerHello(active_session_epoch)
→ 客户端AUTHENTICATED；此时还不能经营农场
```

弱网重连使用 Cookie 免密换取新 Ticket，不重复传密码；真正在线 Session 在 `ClientHello` 成功后
生效，而不是 HTTPS 返回 Ticket 时提前生效。

### 5.2 Join 唯一公开顺序

```text
JOIN_FARM
→ Gate创建PendingJoin(join_attempt_id)
→ JoinIngress创建Hub Joining并返回ACCEPTED元信息
→ Gate先绑定PendingJoin并把ACCEPTED放入当前连接FIFO
→ Gate显式ArmSnapshot
→ Hub读取线性化Snapshot(V)
→ FARM_SNAPSHOT(V)进入同一FIFO
→ Hub冻结有限ready_cut_version=N
→ FARM_DELTA_BATCH(V+1 ... N)*进入同一FIFO
→ SUBSCRIPTION_READY(N)进入同一FIFO
→ Gate原子PendingJoin → ActiveSubscription
→ H5连续应用到N且Recovery/Bootstrap屏障完成
→ FARM_INTERACTIVE
```

`ArmSnapshot` 解决 ACCEPTED 与 Snapshot 分属两条内部调用链时可能乱序的问题；没有 Arm，Snapshot
可能先到 Gate，而 PendingJoin 尚未绑定，导致被错误判成旧 Subscription。

### 5.3 热点农场的有界 Join

- Farm 只维护共享 Snapshot/版本变化来源；
- 每个正在 Join 的玩家只维护自己的 `subscription_id`、游标与有界运行时状态；
- Snapshot 入队确认后冻结 `ready_cut_version=N`；
- `version <= N` 用于本次 Catch-up；
- `version > N` 进入该订阅的 `post_ready_buffer`，严格排在 Ready 之后发送；
- 不能要求整个变化流变空，否则热门农场会永远 Join 不完成；
- item/byte/duration 任一越界时返回 `REJOIN_REQUIRED`，客户端 Fresh Join。

`JoiningBuffer` 和 `post_ready_buffer` 位于目标 farm-svr Holder 的 `FarmSubscriptionHub`，不是
FarmActor 持久状态，也不是 Redis 权威记录。

### 5.4 200 人同场下行

```text
FarmActor一次裁决
→ 产生一份FarmChange(version=K)
→ FarmSubscriptionHub读取在场Subscription
→ 按gateway_id分成少量DeliverBatch
→ 各Gate把消息并行放入每连接独立有界FIFO
```

队列满只影响对应慢连接；业务提交不能等待 199 个浏览器真正收到。丢失连续版本的连接 Fresh Join，
不能让慢玩家拖住 FarmActor 或其他 198 人。

---

## 6. Command 路由、Actor 与迁移收口

### 6.1 两个独立 Route Domain

```text
PLAYER: player_id → PLAYER VirtualShard → player-svr Holder
FARM:   farm_id   → FARM VirtualShard   → farm-svr Holder
```

即使当前 `farm_id == owner_player_id`，两个 Route Domain 仍独立，因为它们的：

- 负载形态不同；
- Actor 类型和状态不同；
- 扩容、热点迁移和发布节奏不同；
- 一次跨 Actor 操作不能因数字相同被误认为同一事务边界。

### 6.2 正常热路径

```text
actor_id
→ 稳定Hash计算virtual_shard
→ 查询进程内不可变AssignmentSnapshot[domain][shard]
→ 复用Holder gRPC连接
→ RouteIngress校验AssignmentRevision/Lease
→ ActorRegistry查找或Singleflight激活
→ Mailbox串行裁决
```

本地路由表大小与 VirtualShard 数量相关，不随 3000 万 DAU 线性增长；正常命令不查询 Redis 路由。
控制面通过 Snapshot + Watch 更新本地表，旧表被 Holder 用 Redirect 和 revision 纠正。

P0 不强制完整 UC Proxyless Mesh。实现使用可替换 `AssignmentProvider`：单 Pod 可静态，多 Pod 使用
轻量 RouteAssignmentManager/Discovery；平台成熟且收益明确时再接 Proxyless Provider。

### 6.3 写权与故障恢复

```text
Shard Assignment/Lease换代
→ 新Holder取得更高writer_epoch和新的writer_claim_id
→ Actor按需加载权威记录
→ 所有写携带record_version + writer claim
→ 旧Holder迟到写被Fence拒绝
```

`writer_epoch` 属于 Shard 写权代次，Actor 记录保存当前 Claim 证明；它不是客户端字段。正常滚动发布
优先 Drain/Handoff，意外崩溃等待故障确认或 Lease 到期后换代。客户端命令可能得到 Redirect、
UNAVAILABLE 或 Unknown，随后使用原 `operation_id` 重试/查询，不得生成新的业务意图。

Actor 热迁移只是热点优化：P0 正确性基线为“强事实已可靠保存，新 Holder 冷加载”。是否迁移热点
Actor 内存由迁移成本和压测决定，不作为 200 人同场正确性的前提。

---

## 7. Redis、CAS 与可靠异步收口

### 7.1 五类数据分域

| 数据域 | 示例 | 是否权威 | 丢失后语义 |
|---|---|---:|---|
| Actor Authority | PlayerRecord、FarmRecord、Receipt、PendingOrder | 是 | 资产/地块错误，不允许按缓存处理 |
| 非Actor Authority | FriendRelation、ShareToken、PlayerMail Entry | 是 | 好友/邮件真值错误，不允许无条件重建 |
| Work | Workflow、Order、DueIndex、Lease | 是，直到完成/过保留期 | 可能永久丢后续步骤 |
| Runtime | SessionRoute、Presence、JoiningBuffer | 否 | 可重新登录/Join重建 |
| Projection | ResultDirectory、好友机会摘要、红点、排行、公开资料 | 否 | 可从权威事实重建；允许短暂延迟 |

“投影”就是面向查询的副本：它让好友列表、红点或结果查询不必逐个激活 Actor，但最终操作仍回到
权威 Actor/Authority 校验。

### 7.2 Actor Authority 原子边界

每个 Actor 记录至少同时受以下 Fence 保护：

```text
writer_epoch / writer_claim_id
record_version
operation receipt / outcome
必要的PendingOrder
```

Actor 内存先改并不等于成功。关键资产、跨 Actor Participant、唯一奖励和权威互动必须完成可靠
提交后才能返回对应成功语义。

普通软资产未来可在明确白名单内采用 `ACTOR_SOFT`：Actor 内原子修改、Dirty 合并、版本化异步保存，
但必须由单一 Persistence Lane 保证旧 SoftSave 不能覆盖新 Barrier。首版可以全部可靠写，待真实
RPO、延迟和写放大压测后再逐项开启 SoftSave。

### 7.3 同槽小 Lua，不做跨Actor大事务

Lua 只用于固定 Key、固定上限、同 Cluster Slot 的条件更新：

- 校验 Claim/Fence 与 record_version；
- 更新一个权威聚合；
- 同提交 Receipt、必要 Order 与 DueIndex；
- 返回明确的 committed/conflict/fenced/unknown 信息。

禁止把 Player 与 Farm 两个 Slot 塞进同一个 Lua，也禁止在 Lua 中执行 RPC、无界扫描或复杂业务
编排。

### 7.4 Workflow、Order、Dispatcher 与 worker

```text
Workflow
→ 有多个Participant和显式状态机，例如Plant/Theft

Order
→ 来源权威提交后必须投递的一段确定后续动作，例如CareReward/GuardSync/ResultPublish
```

来源事实、Order 和 DueIndex 必须同槽原子提交。随后：

```text
DirectDispatcher：进程内有界队列，低延迟快速尝试
farm-worker：从DueIndex重新发现，Lease认领后可靠恢复
```

有界队列不是不断 CAS；它只是安排待尝试工作。真正每一步由 Participant 的幂等 Receipt/CAS 判定。
Dispatcher 丢失不影响正确性，因为 DueIndex 已经持久存在。

### 7.5 PlayerMail 物理语义已冻结

第 35A 号的 `35A-01～35A-10` 已全部冻结，包括：

- 非 Actor 权威，不使用 VirtualShard Holder/WriterEpoch；
- recipient 稳定分桶与版本化 Hash Tag；
- Entry、DeliveryDedup、Index、Summary、DueIndex 的同槽原子更新；
- 每页六封、排他游标与 Lex SortMember；
- Insert/Read/Delete/Projection/Expire/GC 小脚本；
- Unknown 稳定重入、claimed 单调投影、可见数与保留字节分离；
- GC 保护 Claim/Unknown/Projection 证据；
- P0 只有立即可见纯文本定向系统邮件。

尚待验证的是 Redis 平台、容量和最终脚本参数；35A 已是稳定输入，不再处于编辑锁状态。

### 7.6 P0 不依赖 MQ

P0 的可靠链是：

```text
Authority Commit + PendingWork/DueIndex
→ DirectDispatcher
→ farm-worker恢复
```

MQ 可在未来用于跨团队订阅、BI/日志或大规模松耦合事件分发，但不能成为资产、邮件附件、偷菜
结算或护理奖励“是否最终执行”的唯一证据。

---

## 8. 业务场景到技术主干的闭合矩阵

| 场景 | 第一跳/协调者 | 权威写入 | 执行类型 | 可靠异步/查询副本 |
|---|---|---|---|---|
| Player Bootstrap | player-svr PlayerBootstrapIngress | 只读PlayerActor；惰性EnsureDailyPeriod可在同Turn提交 | Actor Direct Query | 不进入FarmSnapshot；按player_state_revision单调应用 |
| Buy / Sell（支持指定数量） | player-svr | PlayerActor经济与库存 | Actor Direct | 无强制跨Actor工作 |
| UnlockPlot | farm-svr Coordinator | PlayerActor扣币 + FarmActor解锁地块 | Workflow | worker恢复 |
| Plant | farm-svr Coordinator | PlayerActor预留种子 + FarmActor创建CropCycle | Workflow | worker恢复；图鉴资格快照随Plant冻结 |
| Mature | FarmRead/FarmActor按需计算 | `now >= mature_at`派生 | 无成熟状态定时写 | P1 Scheduler只负责到期提醒 |
| Harvest | farm-svr Coordinator | FarmActor结束CropCycle + PlayerActor入库/经验 | Workflow | 图鉴Delta、任务事实可靠投递 |
| Care | farm-svr | FarmActor处理全局护理事件 | Actor Commit + Order | CareRewardOrder到PlayerActor；机会摘要投影 |
| Theft | farm-svr Coordinator | FarmActor扣减可偷产量 + 访客PlayerActor入库 | Workflow | 宠物截留减少实际偷得菜量；主人的等值普通金币由结算产生 |
| Join / 实时同步 | farm-svr JoinIngress/Hub | FarmActor只读Snapshot；Hub运行时状态 | Runtime Operation | Snapshot/Delta/Ready，不持久OperationResult |
| 好友/删除/拉黑 | player-svr FriendshipDomain | FriendRelation非Actor权威 | 同槽非Actor Authority | FriendIndex可重建；Permit最终校验 |
| 分享加好友 | player-svr Share/Friend Coordinator | ShareIssuer、Pair、Quota、Receipt | 非Actor Workflow/小状态机 | 分享链接可被多人使用，受配置额度和防重控制 |
| 好友列表机会 | player-svr FriendRead + farm-svr FarmRead | 不改权威 | Query | Summary先筛选，BatchEvaluate精确校验 |
| 下一好友机会 | player-svr FriendOpportunityNavigation | 只改可丢失Runtime Navigation visited | Service Query | `GET_NEXT_FRIEND_OPPORTUNITY`；Ready后服务端最佳努力标记visited |
| Task | player-svr | PlayerActor TaskState/RewardReceipt | Actor Direct/Order消费 | 三个每日任务 + 一个线性成长任务；红点投影 |
| Mail Read/Delete | player-svr PlayerMailDomain | PlayerMail非Actor权威 | 同槽Lua | 每页六封；Summary投影 |
| Claim Mail | player-svr | PlayerActor资产/ClaimReceipt + Mail claimed投影 | Actor Commit + Order | 双重防重与worker恢复 |
| Pet Unlock/Feed/Switch | player-svr | PlayerActor PetState/资产 | Actor Commit + GuardSyncOrder | FarmActor GuardSnapshot最终同步 |
| Compendium | player-svr | PlayerActor CompendiumState | Actor Direct/可靠Delta | 首次成功收获解锁；活动档案永久展示；Seen Ack单调 |

这张表只说明技术主干。产量、20% 可偷上限、单次约 2%、每好友每茬一次、24 块土地且初始 5 块、
护理次数等具体产品规则仍以第 32～37 号合同为准。

---

## 9. 好友机会、通知与查询副本

好友列表和连续导航采用“两阶段查询”：

```text
FriendIndex取得当前页好友
→ BatchGet FarmOpportunitySummary快速筛选/排序
→ 对少量候选调用FarmReadAPI.BatchEvaluate
→ 结合viewer的TheftAttemptReceipt与关系Epoch精确返回action_mask
```

FarmActor 在机会来源事实变化时更新 Summary/Order，例如种植、成熟窗口变化、护理被处理、偷菜额度
耗尽、收获结束。Summary 可能短暂延迟，因此只能做候选筛选，页面上的“可偷/可护理”必须经过
精确检查。

在线提醒与登录补偿共用当前机会：

- 在线 Push 只负责及时唤醒；
- 登录/返回自己农场时 Pull 下一座仍可互动的农场；
- 推荐好友农场完成 `SUBSCRIPTION_READY` 后由服务端标记本轮已访问，不要求操作成功；
- 返回自己农场后查询下一项；可以预取小批候选减少每次完整扫描；
- 好友列表加载失败时显示 Loading/Retry，不能把“未知”冒充“无机会”。

---

## 10. 已冻结与仍待验证的边界

### 10.1 已冻结，可供实现 Agent 直接使用

1. 四 Deployment 与模块归属；
2. HTTPS + WSS、Transport Adapter、FarmFrame 顶层 oneof；
3. `command_type + typed protobuf payload` 与 33 类 CommandSpec；
4. CommandResult/ServerPush 分离及五种 CommandStatus；
5. 三类内部 Context、ActorRouteHeader 正交、Unary gRPC 主链；
6. PLAYER/FARM 双 Route Domain、VirtualShard、本地 Assignment Snapshot 与 Redirect；
7. PlayerActor/FarmActor 权威边界、Actor Turn 不等待外部 RPC；
8. Join 的 ACCEPTED → Arm → Snapshot → Catch-up → Ready 唯一时序；
9. 共享变化来源、每订阅游标/有界缓冲与 200 人批量下行；
10. Actor Authority、非 Actor Authority、Work、Runtime、Projection 五分域；
11. Writer Fence、CAS、Unknown Resolver、Persistence Lane；
12. Workflow/Order + 同槽 DueIndex + Dispatcher/worker 恢复；
13. Buy～Theft、好友/分享、任务、邮件、宠物、图鉴的主链和产品规则；
14. 33A 与 35A 的非 Actor Redis 物理语义；
15. P0 不依赖 MQ，不新增 query/mail/friend/task/pet/compendium 独立服务。

### 10.2 不阻塞语义实现，但阻塞生产结论

1. 最终 Proto field number、`reserved`、数值 `protocol_version` 和生成代码基线；
2. 每个 Payload/Frame 的实际字节上限；
3. PLAYER/FARM VirtualShard 最终数量、Assignment Watch 与 Lease 参数；
4. RouteManager 是自研轻量实现还是接入 Proxyless Provider；
5. Direct WebSocket 与 TCONND 的生产选择及 TCONND Send/心跳/帧上限实测；
6. Redis Cluster Hash Tag、Lua 超时、ACL、noeviction、持久化和故障切换能力；
7. Mail/Friend/Actor Bucket 数、记录字节、GC/Retention、DueIndex 扫描批次；
8. Actor mailbox、Singleflight、Passivation、Dirty/SoftSave 和内存阈值；
9. 200 人热点农场的 Join P99、广播放大、慢连接与单 Actor CPU 上限；
10. 3000 万 DAU 容量模型中的峰值系数、在线率和真实操作分布；
11. mTLS/工作负载身份、K8s NetworkPolicy 和平台证书实现；
12. 监控 SLO、告警阈值、成本和跨机房容灾参数。

这些项目只能通过探针、基准测试、故障注入和平台事实定版，不能由 Agent 根据经验填一个“看起来
合理”的数值。

---

## 11. 实现与验证准入

实现 Agent 开始一个纵向切片前，必须完成以下检查：

```text
1. 找到该命令在28号中的唯一CommandSpec
2. 找到29号对应的调用Context与RPC入口
3. 找到27号的FirstHop/Coordinator/ExecutionKind
4. 找到30/31或33A/35A中的权威提交方式
5. 找到32～37中的产品不变量和失败语义
6. 增加幂等、Fence、Unknown、重试和故障测试
7. 记录真实commit与测试证据
```

建议代码落地顺序：

```text
A. 生成唯一Proto与CommandCatalog，完成31命令完整性测试
B. 接入、Session、Join、重连、Subscription与下行背压
C. 双Route Domain、Actor Runtime、Store/Fence与Redis脚本框架
D. Buy/Sell → Unlock/Plant/Mature/Harvest个人经营闭环
E. 好友/分享 → 好友Join → Care/Theft → 连续机会导航
F. Task → Mail → Pet → Compendium最小闭环
G. 200人同场、3000万DAU模型、故障注入、滚动发布与成本验证
```

实现状态只允许写入单独的验证记录，例如：

```text
repository:
branch:
commit:
environment/image:
test_command:
raw_result:
known_gap:
```

没有上述证据时，只能说“合同已冻结”，不能说“代码已完成”或“已经支持生产容量”。

---

## 12. 防漂移规则

1. 本文改变架构结论前，必须先修改对应的 26～37 唯一权威合同；
2. 新增公开命令必须同步第 28 号的 CommandSpec、数量断言和 H5 生成代码；
3. 新增内部调用不得复用错误 Context；在线、Coordinator、Worker 三条链必须继续分离；
4. 新增权威记录必须声明 Authority/Runtime/Projection 分类、路由键、原子边界和 Unknown 处理；
5. 新增跨权威场景必须声明是 Workflow 还是 Order，禁止只发内存事件；
6. 新增 Projection 必须声明来源、延迟容忍、重建方式和最终权威校验位置；
7. 任意缓存、MQ、Scheduler、TCONND 或 Proxyless 组件不得被写成业务正确性的隐式前提；
8. README、Manifest 与本文版本/状态必须在同一个变更中更新。

---

## 13. 最终状态

本版完成后：

```text
35A：十项物理语义已冻结
40：已从过期Linux快照重建为26～37现行协议总控
26～37：形成闭合的架构、协议、权威存储和业务场景链

当前允许：
→ 按唯一合同实现与测试
→ 做平台探针、容量压测和故障验证

当前不允许：
→ 依据legacy/附件重开已冻结架构
→ 把待测参数伪装成已验证容量
→ 让实现Agent自行发明命令、Context、Redis Key或跨Actor事务
```

因此，项目已经完成“协议和逻辑架构收口”，下一阶段的主要风险不再是继续讨论服务怎么拆，而是
把冻结合同转成唯一 IDL、代码、Redis 脚本、自动化测试和可复现容量证据。
