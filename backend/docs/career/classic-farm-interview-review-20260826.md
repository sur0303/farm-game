# 经典农场后台：面试复习手册

日期：`2026-08-26`

目标：能从业务、架构、正确性、性能和故障五个角度完整讲清项目，而不是只背组件名。

## 1. 先记住项目的核心矛盾

农场不是战斗游戏，单个用户操作频率不高，但存在五类工程难点：

1. 同一个玩家的资产、任务、邮件领取必须防重；
2. 同一个农场可能被主人和好友并发访问、浇水、除虫、偷菜；
3. 大量 WebSocket 长连接需要实时下行，但慢连接不能阻塞 Actor；
4. 有状态 Actor 要水平扩容、滚动发布和故障接管，同时保持唯一写权；
5. 3000 万 DAU 不能靠一个大集群承载，需要可量化的单 Cell 基线和多 Cell 规划。

由此得到总体原则：

```text
同一权威对象内串行
跨权威对象可恢复
查询与写权分离
实时Hint只做加速
容量必须以SLO和失败率判断
```

## 2. 总体架构

```text
H5 / 微信WebView
  ├─ HTTPS JSON：登录、Cookie免密换票
  └─ WSS Protobuf：命令、结果、Snapshot、Delta、Hint
                     │
                     ▼
                 farm-gate
          Auth / Session / Protocol
       ConnectionRegistry / WriterQueue
          CommandCatalog / RouteClient
                     │
           ┌─────────┴─────────┐
           ▼                   ▼
       player-svr           farm-svr
       PlayerActor          FarmActor
       资产/仓库/任务         地块/作物/护理/偷菜
       邮件领取/宠物          Presence/SubscriptionHub
           │                   │
           └─────────┬─────────┘
                     ▼
                 farm-worker
       DirtyFlush / SourceOrder / DueIndex
       Schedule / Hint / Reconcile
                     │
                     ▼
                Redis Cluster
       Authority / Runtime / Projection
```

部署单元少于领域模块。任务、邮件、图鉴、红点和宠物首先是 PlayerSvr 内的领域模块，不因为有一个名词就拆一个微服务。

## 3. 接入与会话

### 3.1 登录为什么分 HTTPS 和 WSS

```text
账号密码
→ HTTPS Login
→ PBKDF2校验
→ 签发HttpOnly Cookie和一次性ConnectionTicket

WSS建立
→ ClientHello(ticket)
→ Gate原子核销Ticket
→ 激活session_epoch
→ ServerHello
```

HTTPS 回答“你是谁”，WSS ClientHello 回答“这根连接属于谁”。ClientHello 不是 TCP 三次握手，而是应用层会话握手。

弱网重连不重新传密码：

```text
HttpOnly Cookie
→ /auth/renew
→ 新的一次性Ticket
→ 新WSS
→ 新ClientHello
```

好处是密码不长期停留在 JS，Ticket 可单次核销、短 TTL，并能让旧连接失效。

### 3.2 SessionRoute 是什么

```text
player_id
→ gateway_id
→ connection_id
→ connection_generation
→ session_epoch
```

它用于把 FarmChange、Hint、KICK 等服务端消息送到玩家当前连接。它不是 Actor 路由：

```text
SessionRoute：消息送到哪条连接
ActorRoute：命令送到哪个Holder Pod
FarmSubscription：玩家当前订阅哪座农场
```

三者拆开后，切换农场只替换 Subscription，不需要修改 SessionRoute 或迁移 Actor。

## 4. Join、ArmSnapshot 与 JoiningBuffer

### 4.1 Join 状态

```text
AUTHENTICATED
→ JOINING
→ FARM_INTERACTIVE / IN_FARM
```

`ACCEPTED` 只表示服务器已经创建 `subscription_id` 并接受本次 Join，不表示客户端已经拥有可写的农场视图。

完整顺序：

```text
H5 → Gate: JoinFarm
Gate → FarmSvr: JoinIngress
FarmSvr → Gate: ACCEPTED(subscription_id)
Gate → FarmSvr: ArmSnapshot(subscription_id)
FarmActor/SystemTurn: Snapshot(V)
Hub → Gate: Snapshot(V)
Hub → Gate: Delta(V+1 ... N)
Hub → Gate: SubscriptionReady(N)
Gate → H5: Ready
```

### 4.2 为什么有 ArmSnapshot

当前代码把命令 RPC 和大体积 Snapshot 下行拆成两条链：

```text
Join RPC：FarmSvr → Gate命令响应
Snapshot：FarmSubscriptionHub → Downlink Lane → Gate
```

如果 Hub 在 Gate 绑定 `subscription_id` 之前发送 Snapshot，Snapshot 可能先到而被判为 stale。ArmSnapshot 是显式闸门：Gate 先处理 ACCEPTED、建立 PendingJoin，再允许 Hub 下发 Snapshot。

如果未来改成单条 RPC 同步返回 Snapshot，这个乱序问题会减少，但会让大 Snapshot 占住业务 RPC、Actor Turn 和超时预算。两种方案没有绝对好坏；当前方案更适合实时广播和慢连接隔离，但必须保证下行可靠。

### 4.3 JoiningBuffer 和 Ready Cut

Snapshot 生成于版本 `V`，期间农场仍可能发生变化：

```text
Snapshot(V)
Change(V+1)
Change(V+2)
...
```

Hub 为该农场保留一份有界变化环；每个订阅者只保存自己的游标，不为每个人复制整份 Buffer。

```text
共享Buffer: V+1, V+2, ...
订阅A游标: 已到V+7
订阅B游标: 已到V+3
```

Ready Cut `N` 的作用是让 Catch-up 有一个有限终点：先追到 N，N+1 之后进入正常 Ready 下行。没有 N 时，热点农场不断产生新变化，Join 可能永远“追不上”。

## 5. Gate 的线程与队列模型

概念上分三层，不要求“一连接一线程”：

```text
网络收发层
→ 解码FarmFrame、心跳、sequence、帧上限

Backend RPC并发池
→ 调Player/Farm FirstHop
→ Deadline、限流、Redirect、结果关联

每连接Writer Queue
→ FIFO下行
→ 慢连接隔离
→ item/byte双上限
```

FarmSvr 还有到每个 Gate 的 Downlink Lane：

```text
FarmActor只生成一次FarmChange
→ Lane按目标Gate批量
→ Gate再分发到本地各连接Writer Queue
```

这样 200 人同场时，FarmActor 不等待 199 个浏览器真正写完 socket。一个慢用户只会打满自己的 Writer Queue，不会堵住 FarmActor Mailbox。

队列满时不能无限扩大。无限队列只是把“拒绝”变成“延迟和内存爆炸”；正确做法是有界队列、批处理、按字节限额、监控和过载策略。

## 6. CommandCatalog 与协议

公网顶层稳定类别使用 Protobuf `oneof`：

```text
FarmFrame
├─ ClientHello / ServerHello
├─ ClientCommand
├─ CommandResult
├─ ServerPush
└─ ControlFrame
```

业务命令持续增加，因此 `ClientCommand` 使用：

```text
command_type + bytes payload
```

CommandCatalog 把 bytes 重新约束成强合同：

```text
PayloadType / ResultType / Decoder
MaxPayloadBytes
AdmissionPolicy
ExecutionKind
FirstHopRole / RouteDomain / RouteKey
Idempotency / Retry / Timeout / RateLimit
```

它回答的不是“命令内容是什么”，而是“这条命令可以何时执行、送到哪里、如何解码和重试”。

## 7. PlayerActor 与 FarmActor

### 7.1 为什么拆成两个 Actor

PlayerActor：

```text
钱包、仓库、商店计数
等级、任务、图鉴
宠物、邮件领取
玩家公开资料源
```

FarmActor：

```text
Plot、CropCycle
CareIncident
TheftAttemptReceipt
GuardSnapshot
FarmOpportunityHeader/Facts
FarmVersion
```

拆分原因：

1. 好友访问只应竞争 FarmActor，不应阻塞玩家邮件、任务或商店；
2. 热门农场和高消费玩家的扩容模式不同；
3. 玩家资产和农场多人互动的持久化等级不同；
4. 未来一个玩家多个农场时，FarmActor 可自然扩展为一对多。

即使 `player_id == farm_id`，两个 Route Domain 仍有独立 VirtualShard 表和 Holder 集群，不保证落到同一个 Pod。

### 7.2 同一农场不同土地能否并行

当前 FarmActor 对整座农场串行。不同地块理论上可并行，但会引入：

```text
FarmVersion合并
同场广播顺序
宠物/偷菜全局额度
Snapshot一致性
更多锁和死锁风险
```

农场每人最多 24 块地，操作频率低，整农场串行更符合复杂度与收益。真正的热点应先通过 Mailbox 指标和压测证明，再考虑分区 Actor。

## 8. Actor 路由与 VirtualShard

热路径：

```text
actor_id
→ hash到固定VirtualShard
→ 本地AssignmentSnapshot[shard]
→ Holder gRPC连接池
→ RouteIngress校验revision/writer_epoch
→ Actor Mailbox
```

本地路由表只包含几千条 Shard Assignment，大小与 DAU 无关。控制面通过 Watch/Revision 自动更新，不需要人工修改每个进程。

### 8.1 为什么不是一致性哈希直接到 Pod

直接按 Pod 列表哈希无法清楚表达：

- 当前唯一 Writer；
- Drain 中的新请求去哪里；
- 滚动发布时的有序 Handoff；
- 旧 Pod 的迟到写如何拒绝；
- 热点 Shard 如何定向迁移。

VirtualShard 把“Actor→稳定桶”和“桶→当前 Pod”分成两层，扩容只移动计划中的 Shard。

### 8.2 Lease 与 writer_epoch

```text
Lease：当前Holder在一段时间内的租约
writer_epoch：写权代次，只增不减
```

Shard 从 A 迁到 B：

```text
A停止接收该Shard新请求
→ 处理或拒绝Mailbox中的请求
→ Flush强事实
→ Lease/Handoff
→ B取得更高writer_epoch
→ B开始懒加载Actor
→ A的迟到写携带旧epoch，被Redis拒绝
```

writer_epoch 通常按 Shard 写权推进，不是每次 Actor 普通写都加一。

## 9. Kubernetes 自动扩缩容

```text
Kubernetes Informer
→ 发现Pod创建、删除、Ready变化

HolderControl
→ GetRuntimeIdentity / Readiness / ServingShardCount

RouteControl
→ Shard Assignment / Revision / Lease / writer_epoch

ScalePlan
→ 生成最小迁移集合并执行
```

没有重复造服务注册中心：Pod 发现来自 Kubernetes；RouteControl 只保留可重建的 VerifiedHolderView 和游戏写权状态。

Pod B 如何知道接管：RouteController 发现 A 不再 Ready 或 Lease 到期，生成新的 Assignment；B Watch 到新 Revision 并取得 writer_epoch 后接管。不是人工通知 B。

冷迁移是 P0 基线：强事实已落存储，新 Holder 按请求懒加载 Actor；热迁移内存 Snapshot 只适合后续热点保温优化。

## 10. Redis 三个逻辑域

即使当前共用一个 Redis Cluster，也按语义分三域：

| 逻辑域 | 数据 | 丢失后果 |
|---|---|---|
| Authority | Player/Farm 权威、Receipt、Order、Route Claim | 影响资产和正确性 |
| Runtime | SessionRoute、Ticket、连接代次、临时 Lease | 可通过重连恢复 |
| Projection | PublicProfile、FriendIndex、机会导航、红点、排行榜 | 可重建，允许短暂延迟 |

使用三个独立配置项的意义是可以分别设置连接池、超时、监控、容量和未来集群，不代表第一天就必须部署三套 Redis。

### 10.1 Shard Bucket 结构

示意：

```text
{classic-farm:<env>:<zone>:auth:player:v1:s:42}:shard-authority
{classic-farm:<env>:<zone>:auth:player:v1:s:42}:actor:1001
{classic-farm:<env>:<zone>:auth:player:v1:s:42}:order:<type>:<id>
{classic-farm:<env>:<zone>:auth:player:v1:s:42}:due
```

相同 Hash Tag 让同一 Bucket 的 Actor、Order 和 DueIndex 位于同一 Redis Cluster Slot，从而允许小范围 Lua 条件提交。Lua 只做版本/Fence/同槽原子更新，不承载整套业务状态机。

## 11. Dirty Writeback 与可靠提交

### 11.1 Dirty 热路径

```text
Actor Turn
→ 内存校验与修改
→ dirty_generation++
→ 返回客户端结果/Delta
→ Scheduler合并同Actor多次修改
→ 冻结最新状态
→ 异步Flush Redis
```

正确性保护：

- Mailbox 保证 Actor 内顺序；
- soft_generation 防止旧 Flush 清掉新 Dirty；
- writer_epoch 防旧 Holder 写入；
- Drain/ForceFlush 控制迁移和下线；
- `oldest_dirty_age` 直接代表潜在 RPO。

### 11.2 哪些不能随便 Dirty

| 数据 | 建议 |
|---|---|
| 普通视觉状态、可重建计数 | Dirty 候选 |
| 普通金币、种子、作物 | 产品接受明确 RPO 后灰度 |
| 付费货币、唯一奖励 | 可靠提交 |
| 邮件附件、任务领奖 | 资产 + Receipt 原子提交 |
| 跨玩家偷菜最终结果 | Authority Receipt + 可查询结果 |

Dirty 不是“不要一致性”，而是按业务价值购买不同持久化成本。

## 12. 跨 Actor：Workflow、Order、Receipt

Actor 不应在 Mailbox 内等待另一个 Actor：

```text
FarmActor Turn
→ 如果同步等待PlayerActor
→ Player/网络/Redis慢
→ 整座农场Mailbox被堵
```

正确方式：

```text
FirstHop/Coordinator可靠创建Workflow或SourceOrder
→ Participant A幂等执行并写Receipt
→ Participant B幂等执行并写Receipt
→ DirectDispatcher立即推进
→ 失败时farm-worker从DueIndex恢复
```

区别：

```text
Coordinator：运行时推进者，可崩溃
Order/Workflow：持久事实，可被重新发现
Receipt：Participant已经完成某一步的幂等证明
```

DirectDispatcher 是进程内的低延迟快速路径，不是 MQ；DueIndex + Worker 是可靠兜底。

## 13. 好友关系

好友真值只保存一份 canonical pair：

```text
low_player_id  = min(A, B)
high_player_id = max(A, B)
FriendRelation(low, high)
```

查询列表使用两条有方向的 Projection：

```text
FriendIndex[A] → B, relation_id, relation_epoch
FriendIndex[B] → A, relation_id, relation_epoch
```

Pair 更新时原子创建投影更新 Order；快速 Dispatcher 立即更新双方 Index，失败由 Worker 重试。删除/拉黑后 canonical Pair 的 epoch 增加，旧 Index 即使暂时残留，批量验证也会过滤。

ActionPermit 绑定：

```text
viewer_id + target_farm_id + relation_id + relation_epoch
+ operation_id + short TTL
```

执行前重校验当前 Pair/epoch；因此签发后删友或拉黑，旧 Permit 会失效。

## 14. 好友机会、弹窗与列表

### 14.1 Header 与 Facts

每座农场一份紧凑 Header，属于 Farm Authority 的读切片：

```text
farm_id
farm_version
opportunity_revision
next_opportunity_at_ms
护理事件摘要
```

Facts 用于精确裁决：

```text
CropCycle/mature_at
剩余可偷份额
CareIncident
TheftAttemptReceipt(cycle, viewer)
```

Header 只粗筛，不能授权操作；不同玩家是否偷过不同，所以最终结果必须结合 viewer Receipt。

### 14.2 右下角弹窗

```text
登录/回自己农场
→ 读取最多300个好友Header
→ 按机会时间排序
→ Top 16 Facts精查
→ 保存8个CandidateWindow
→ 返回下一座可操作农场
```

后续若好友名单 `roster_revision` 未变化，复用 CandidateWindow，只精查少量候选。

### 14.3 在线实时 Hint

```text
Farm状态写入next_wakeup
→ 同Shard DueIndex(ZSET)
→ Worker每秒批量领取到期Farm
→ 读取最新Header/Facts并校验generation
→ 发布FriendOpportunityChanged(owner_id, revision)
→ Gate按SessionRoute推在线好友
→ H5定向ExactEvaluate(owner_id)
→ 有机会则弹窗；失效则回退全局Navigator
```

Hint 不包含授权结论，只是“这座农场值得重查”。Redis Schedule 不会自己触发代码，Worker 必须按 score 扫描到期项。H5 每 120 秒 Pull 是弱网兜底，调用服务端 Navigator，不直接访问 Redis。

### 14.4 好友列表

好友列表按 20 人一页：

```text
FriendIndex当前页
→ BatchValidate canonical Pair
→ BatchGet PublicProfile
→ Pipeline读20座农场Header/Facts
→ BatchEvaluate
→ 返回精确action_mask
```

点击下一页才查下一页，不扫描全部 300 人。弹窗追求“找下一位”，好友列表追求“当前页准确”，所以两条路径分开。

## 15. 成熟时间和 Scheduler

成熟不是 Worker 写成 `MATURE`：

```text
是否成熟 = trusted_now_ms >= mature_at
```

Scheduler 只负责到期唤醒提醒。即使 Worker 故障，玩家打开农场时 FarmActor 仍按时间正确判断成熟。

每座农场只维护一个最早 `next_wakeup`，不是每块地常驻一个 Timer：

```text
1号地10:05浇水
2号地10:08成熟
3号地10:12虫害
→ DueIndex只登记该农场10:05
```

Worker 到 10:05 读取最新 Facts，处理提醒，再计算下一边界 10:08 并条件更新。若旧任务迟到，generation/score Fence 会阻止覆盖新 Schedule。

## 16. 偷菜结算

规则示例：每个好友每茬一次，单次约基础产量 2%，整茬最多被偷 20%。

宠物咬人不是从访客钱包扣钱：

```text
gross_stolen
→ 宠物截留一部分作物
→ visitor_received = gross - intercepted
→ intercepted按配置估值为金币
→ 农场主金币增加
```

FarmActor 先裁决作物剩余量并写 TheftAttemptReceipt，Visitor PlayerActor 再把净作物加入仓库。跨 Actor 结果通过 Settlement/Receipt 幂等恢复。

## 17. 观测与压测

必须同时看：

```text
成功率、p95/p99
Gate连接/Backend slots/Writer queue
Join pending/accept_without_snapshot
Actor mailbox/activation
Dirty queue/oldest age/flush error
Redis RTT/error/pool wait/memory
Downlink lane queue/full/RPC failure
Worker due depth/oldest due age
CPU、内存、GC、pprof
```

本项目的严格证据：

```text
4 Gate冷登录：40K/40K，5,623.7/s
20K完整经营：2,425,296动作，9,697.4/s，p95 93ms
300好友导航：冷176ms，热11～13ms
自动扩容：1024 Shard → 341/342/341
```

25K/30K 的高峰不能作为容量，因为失败率和 Snapshot 完整性不达标。

## 18. 严苛评委可能追问的问题

### Q1：既然 Redis CAS 能保证一致性，为什么还用有状态 Actor？

Actor 不只是缓存。它提供同一实体的顺序裁决、合并写、实时订阅、派生状态和低延迟 Delta；Redis CAS 是故障边界，不应代替每条业务命令的内存状态机。

### Q2：Dirty 模式崩溃不就丢数据了吗？

是，所以必须明确 RPO 并分级。普通可回退状态用 Dirty；付费资产、领奖和跨玩家最终结果保留可靠提交。监控 `oldest_dirty_age`，超阈值时降级或拒绝低优先级写。

### Q3：为何 1024 Shard？

它是初始工程参数，不是数学真理。要求是远大于 Holder 数、Assignment 表足够小、单 Shard 负载可控。最终应根据 Actor 数、热点分布、迁移成本和压测决定 1024/4096，而不是凭 DAU 直接推导。

### Q4：为什么不把每块土地一个 Actor？

24 块地的并发收益低于跨地块版本、广播、宠物和 Snapshot 一致性的复杂度。先以整农场 Actor 保证简单正确，再用热点证据决定是否拆。

### Q5：为什么 Snapshot 不直接作为 Join RPC 返回？

同步返回更简单，但大 Snapshot 和慢网络会占住业务 RPC；独立下行可批量、广播和隔离慢连接。当前方案以 ArmSnapshot 解决双链乱序，但必须继续修复高压下下行 unavailable。

### Q6：Redis Cluster 的同 Slot Lua 会不会把业务写死在 Redis？

Repository 暴露的是逻辑 CAS/Commit 接口，Redis Hash Tag 和 Lua 是当前物理实现。Lua 保持小范围条件提交，不把完整业务状态机写进去，未来才能替换存储。

### Q7：好友列表为什么不直接读完整农场状态？

300 好友会放大 Authority IO 和解码成本。Header 粗筛、Facts 精查只读必要字段；最终操作仍由 FarmActor 权威裁决。

### Q8：如何避免 Hint 风暴？

同农场合并 revision、只对 WATER/STEALABLE 等用户可见机会推送、按 Gate 分组、客户端 300ms 合并、单次最多精查有限目标并限频。

### Q9：Pod 宕机时请求会不会丢？

未被 ADMITTED 的请求由客户端重试；已执行操作依靠 operation_id/Receipt 查询终态；新 Holder 在 Lease/Fence 后接管。纯 Dirty 且未 Flush 的普通状态按产品 RPO 允许回退。

### Q10：如何证明水平扩容有效？

不只看 Pod Ready，要看 Shard 分布、writer_epoch、pending/degraded、业务成功率、p95，以及扩容前后单 Pod CPU 和吞吐；本项目已有 2→3 后 `341/342/341` 的真实证据。

## 19. 当前真实不足与改进顺序

P0：

1. 修复高压下 FarmSvr→Gate deliver_batch unavailable；
2. 为 Join Snapshot 增加可恢复重送或显式 Rejoin，而不是只等 PendingJoin 超时；
3. 继续压缩 Actor Field Envelope、Protobuf 合并和 PlotView 分配；
4. 对 Dirty RPO 建立硬熔断阈值。

P1：

1. 高负载滚动更新和单 Pod Kill；
2. Redis 主从/Cluster 故障测试；
3. 200 人热点农场和慢客户端；
4. 2～4 小时长稳。

P2：

1. 多 Cell 路由和跨 Cell 好友访问；
2. 热点 Actor 保温迁移；
3. 权威、Runtime、Projection Redis 物理拆分。

## 20. 七天复习安排

| 天 | 目标 | 自测输出 |
|---:|---|---|
| 1 | 业务模块和四部署单元 | 3 分钟无稿讲架构图 |
| 2 | 登录、Session、Join、JoiningBuffer | 手画完整时序，解释 ArmSnapshot |
| 3 | 双 Actor、Workflow、Dirty/可靠提交 | 分别讲 Plant、Harvest、Theft |
| 4 | VirtualShard、Lease、writer_epoch、扩缩容 | 回答 Pod 宕机和滚动发布 |
| 5 | 好友关系、机会导航、Scheduler/Hint | 画出列表和弹窗两条链路 |
| 6 | 压测数据、pprof、瓶颈 | 不看文档复述所有可信数字 |
| 7 | 模拟面试 | 30 秒、90 秒、10 分钟三个版本各讲一次 |

复习标准不是记住所有字段，而是每个设计都能回答：

```text
解决什么问题？
为什么不用更简单方案？
正确性边界在哪里？
数据如何证明？
还有什么不足？
```

## 21. 文档复习地图

不要从头阅读所有历史版本。建议按下面顺序复习当前合同：

1. [核心场景案例](../design/core-scenario-casebook.md)：先建立登录、Join、经营、好友与通知的全局画面；
2. [26 · 接入与会话](../design/26-access-session-contract.md)：Ticket、Session、Subscription、Join；
3. [27 · 路由、Actor 与 Workflow](../design/27-access-routing-actor-workflow-freeze.md)：VirtualShard、Holder、Mailbox、Workflow；
4. [28 · 公网协议与 CommandCatalog](../design/28-command-catalog-public-protocol.md)：H5 可见协议；
5. [29 · 内部 RPC 与可信上下文](../design/29-internal-rpc-trusted-context.md)：Gate、Coordinator、Worker 三类调用；
6. [30 · Actor Authority Store](../design/30-actor-authority-store-contract.md) 与 [31 · Redis 物理合同](../design/31-actor-authority-redis-physical-contract.md)：CAS、writer_epoch、Key/Slot/Lua；
7. [32 · 核心业务场景](../design/32-core-business-scenario-contract.md)：Buy、Plant、Care、Harvest、Theft；
8. [33 · 好友与分享](../design/33-friendship-share-access-contract.md)：canonical Pair、Permit、导航；
9. [34～37 支撑系统](../design/34-task-system-contract.md)：任务、邮件、宠物与图鉴；
10. [61 · 性能、弱网与平滑更新](../design/61-performance-resilience-smooth-update-plan.md)：从正确性进入生产验收；
11. [本轮最终容量实测](../verification/actor-hotpath-gate-scale-capacity-final-20260826.md)：最后用数据校正架构判断。

`docs/design/legacy/` 只用于回顾演进过程，不应再作为当前实现的权威输入。
