# UC 后台设计思想，以及和农场的对照

用途：离职后面试口述。只讲机制和取舍，不写集群地址、账号、密钥。
对照代码：UC `xrpc@v0.3.91`、`zonesvr`、`gatesvr`、`coordinator`；农场 `master@280daa13` 与后续 dirty 实验分支。

---

## 0. 30 秒版本

UC 把三件事拆开：

1. **Mailbox** 保证同一个玩家同一时刻只有一个写者。
2. **Shard + K8s CR** 保证同一个分片同一时刻只有一台 Pod 持有。
3. **Dirty 回写** 保证热路径先改内存，存储只是限速的 last-write-wins 哈希表。

崩溃会丢掉还没刷盘的窗口。跨服没有持久工作流，靠同 Actor 内 event 和尽最大努力的 RPC。

农场前期走了更严的路：每条命令 Redis Lua CAS + SourceOrder，崩溃可恢复到上一笔提交。后来补了一条 UC 风格的 dirty 实验路径，热路径不再写 Redis，但跨 Actor 经营不再由 Worker 恢复。

---

## 1. UC 整体拓扑

```text
客户端
  → dirsvr          只回 gate 列表，不是分片权威
  → TCONND → gatesvr
              Session / 序号 / 推送，不改业务
              注入 x-gid，proxyless 找到 holder
  → zonesvr         PlayerActor(gid)，玩家权威
  → scenesvr        SceneActor，可开多条线
  → friendsvr / mailsvr / statesvr   无 Actor，Redis/Tcaplus
  → fightsvr / matchsvr / roomsvr    对战与房间

coordinator（K8s CR）才是「shard 归谁」
region-server 把 CR 投影成 HostShards / RemoveShards
```

面试里最容易说错的一句：`dirsvr` 不是路由权威。它只回答「这个环境有哪些 gate」。玩家落到哪台 `zonesvr`，是 `x-gid` 哈希到 shard，再由 coordinator 指定的 holder 服务。

---

## 2. UC 十二条设计原则

背这些原则，比背类名有用。

### 2.1 Mailbox 就是锁

同一个 GID 同一时刻只跑一条用户消息。用户 RPC 进 user mailbox，`Save` / `Stop` / `Started` 进更高优先级的 system mailbox。

不在模型字段上加锁，也不在 Redis 里做 CAS。单写者是运行时结构保证的。

`Await` 可以在存盘时让出 mailbox，让别的 RPC 继续跑；存盘回来用 system 消息续上。这是 IO 让出，不是并发写。

### 2.2 放置单位是 Shard，一致性单位是 Actor

控制面管不起「几百万玩家各自绑哪台 Pod」。所以：

```text
gid --hash--> shardId
coordinator CR：shardId → Pod
mailbox：一个玩家内部串行
```

扩容、滚动、故障，搬的都是 shard。热分片不均是放置问题，不是 mailbox 问题。

### 2.3 热路径先改内存，再限速回写

玩家文档很大（Tcaplus 软警告 8MB）。每条 RPC 整包落库会打爆存储。

```text
业务改 Model
  → MarkDirty
  → OnHandleEnd：version++，记下 modifiedVersion
  → Sync.Add(pid) 进 UniqueFifo
  → 全服 200/s（正式）限速
  → 发 Save 系统消息
  → 只把 modifiedVersion > savedVersion 的字段 HMSet 出去
```

`IsDirty` 不是「flags 还在」，flags 在 handle 结束时就清了。脏的定义是 `savedVersion < version`。存盘期间又改了，watermark 对不上，再入队一次。

### 2.4 版本是水位，不是存储修订号

Redis / Tcaplus 不存这个 version。`OnSaveSucc` 只把 `savedVersion` 推到这次 `OnSave` 冻结的版本。重叠的「边存边改」靠水位解决，不靠 CAS。

### 2.5 单写者是放置属性，不是存储属性

`Store.Save` 就是 last-write-wins `HMSet`，签名里没有 expected version。两台 Holder 同时写，会按字段交错。正确性 100% 押在「一个 shard 只有一个活着的 Holder」。

### 2.6 CR 是控制面，Actor 是数据面

```text
ShardMapping              期望：这些 shard 给哪个工作负载
ShardMappingAssignment    算出来：realm + shard → podEntry + generation
Holder 记录               实际谁在服务
region-server             长连接通知 Host / Remove，带 previous_holder
```

kubectl / reconcile / ownerRef / watch 都在 K8s。热路径不读 CR。客户端仍然 `x-gid` 哈希，proxyless 找「当前宣告持有这个 shard 的 Pod」。

### 2.7 跨逻辑用 Eventbus，不用 Workflow

同 Actor 内 `Publish` 是当轮扇出，嵌套事件排队（警告 10、中止 100）。所有订阅者改的是同一份 mailbox 状态，handle 结束一次 dirty。跨服 topic 是 handle 结束再尽最大努力刷出。

没有持久 saga，没有补偿日志，崩溃不重放半截业务。他们接受了这个 RPO，所以不必上工作流引擎。

### 2.8 出门先 Flush，快照是后备箱

优雅停机 / `OnStop`：阻塞 `Flush`。
还脏着：不准 passivate。
迁移：先停 Actor 并 Flush；`OnSnapshot` 时如果已经干净，只搬 Runtime，目标端从 DB 加载；如果还脏，才打包全量 + Runtime。
Kill / 超时：目标端走 DB，未刷盘的丢。

### 2.9 恐慌就放弃脏数据

Panic 把 Actor 标 `Abandoned`，`doSave` 直接跳过。损坏内存不能赢 last-write-wins。目标端加载上次成功落库。这是故意用 RPO 换存储不被毒数据污染。

### 2.10 背压是本地、显式的

每 Actor mailbox 默认 128，全引擎用户消息有上限，写回 200/s。满了丢这条（`ErrMailboxFull`），不堵存储、不堵别人。水位线是观测，不是硬门。

### 2.11 P2P 搬热状态，DB 是冷路径

新 Pod 向旧 Pod 拉 Handoff 流，按 Actor 一块 snapshot/runtime（大约 4MB 切开）。避免目标端瞬间 `Load` 打爆库。Coordinator 从不搬运 Actor 字节，只改 shard 归属。

### 2.12 接入无业务权威

`gatesvr`：Session、序号、FrameCache、推送、熔断。不改金币。
连接路由是 `x-gateid`（哪台 gate 推送）。
业务路由是 `x-gid`（哪台 zonesvr 裁决）。
两件事不能绑死：玩家可以换 gate，GID 仍必须到当前唯一 PlayerActor。

---

## 3. UC 分片迁移：CR 管归属，Handoff 管人

控制面一次迁的是整个 shard，不是单个 GID。

```text
改 ShardMapping（shard 7：pod-0 → pod-1）
  → controller 写出 Assignment
  → region-server 给 pod-1：HostShards{previous_holder=pod-0}
  → pod-1 进入 Import，向 pod-0 打 gRPC Handoff
  → pod-0 上每个 Actor：
        先 Flush
        还脏：推全量 PlayerInfo + Runtime
        已干净：只推 Runtime
  → 已迁走的 Actor 在旧 Pod 只回 AskRedirect
  → 整 shard 完成：pod-1 Normal，pod-0 Shutdown
```

单写者靠分片状态机：

| 状态 | 行为 |
|---|---|
| Import | 只处理带 AskRedirect 的请求 |
| Normal | 正常服务 |
| Handoff | 已迁走的回新地址，未迁完的让客户端重试 |
| Shutdown | 不再创建新 Actor |

并发大约一个 shard 同时迁 100 个 Actor，整次大约 10 秒超时。P2P 失败就从存储冷加载。

**为什么用 CR，而不是 Redis 路由表：** 分片所有权是集群期望状态，要和 Pod 扩缩、滚动一起 reconcile；要有 generation、watch、误删保护。Redis hint 当权威会和真实 Holder 打架。热路径仍然不读 etcd。

---

## 4. UC 的好友 / 邮件 / 场景为什么不是 Actor

| 服务 | 模型 | 原因 |
|---|---|---|
| zonesvr | PlayerActor | 玩家文档有唯一写者，生命周期长 |
| scenesvr | SceneActor | 共享 3D 场景，可开线分流 |
| friendsvr | 无 Actor，Redis + 锁 | 图，不是单一实体；邀请对加锁即可 |
| mailsvr | 无 Actor，Tcaplus | 分页邮箱，领取进 Player 才碰资产 |
| statesvr | 无 Actor，Redis 互斥表 | 「在房间 / 匹配 / 战斗」跨系统互斥，不必占 Player 邮箱 |

原则：有唯一活写者的做成 Actor；图、邮箱、互斥表做成无状态服务。

---

## 5. UC 这条路的硬伤（面试要主动讲）

1. **崩溃 RPO 原则上无上界。** 只被 200/s 和队列深度卡住。没有「脏龄超过 N 秒必须停写」的存储栅栏。
2. **没有存储 CAS。** 脑裂时按字段 last-write-wins，可能拼出两代模型。
3. **Kill / 迁移超时丢掉脏数据。** 目标端加载的是上次成功 Flush，不是没发出去的 snapshot。
4. **忘了 MarkDirty，正式环境静默。** 哈希核对只在测试开。
5. **紧急优先回写钩子基本没接上。** `Player.flush` 在业务里几乎没人置位。
6. **多 Model 的 HMSet 不是一笔事务。** 中途崩溃可能 A 到了 vN、B 还在 vN-1。
7. **热分片不能拆开。** 哈希是固定的，明星 shard 只能换哈希或加机器均摊。
8. **Eventbus 不是事务。** 跨服中途崩，级联后半截和脏数据一起没。
9. **Mailbox 满就丢用户请求。** 没有持久收件箱。
10. **一直脏的空闲 Actor 钝化不了**，可能钉在内存里。

一句话收口：*Mailbox 给单写者，Shard+CR 给单持有者，脏版本给重叠存盘，存储是笨哈希表。买到的是便宜回写和便宜放置，付出的是崩溃窗口和「绝不能脑裂」。*

---

## 6. 农场做成了什么（对照用）

农场不是抄 UC 的业务边界，是同一家族的机制，换了一套经营约束。

```text
H5
  → farm-gate     Session、CommandCatalog 首跳、下行背压；无业务写权
  → player-svr    PlayerActor：钱包、背包、成长
  → farm-svr      FarmActor：地块、作物、Presence
  → farm-worker   DueIndex 恢复（可靠模式）/ 机会唤醒
```

### 6.1 必须拆两个 Actor

UC 一个 GID 吞掉资产、登录、PVE。农场的热点是别人来浇水、偷菜、围观，会把农场主买种子堵住。

所以：

- `PLAYER:id` 只裁决资产
- `FARM:id` 只裁决地块
- 数值可以相同，路由空间必须不同

同一农场不拆 PlotActor，也不开「农场线」：地块因果必须唯一。吞吐靠多农场并行，不靠拆写权。

### 6.2 可靠模式比 UC 更严

默认 `durable_cas`：

```text
每条变更 Turn
  → Redis Lua（WriterClaim + RecordVersion CAS + 可选 SourceOrder）
  → 成功了这个 Turn 才结束
```

跨 Actor 种植 / 收获：来源 Actor 一次 Lua 里改模型 + 挂上 SourceOrder + DueIndex，目标 Direct 执行，再 `ClaimAndComplete`。Worker 扫 DueIndex 恢复。Authority 提交后只恢复不回滚。

崩溃恢复到**上一笔已经提交的命令**，不是上一笔内存。

### 6.3 路由控制面自己做了 ScalePlan

UC 用 CR + region-server。农场用 Redis 持久的 ScalePlan / RoutePlan / RouteControl：

- K8s 只发现 Pod，不授写权
- 写权是 `writer_epoch` 换代
- PLAYER / FARM 两套 1024 VirtualShard 可独立扩缩
- 迁移顺序：停收 → 排空 → 持久化 → fence → 接管

已验证：单热点分片 2 万玩家 / 2 千在线，来回约 3.15 秒，窗口约 14.7 万命令零失败。整 Pod 4→5→4 拓扑能收敛，但出现过客户端 unavailable，不能说整集群扩缩已经过关。

### 6.4 后来补的 dirty，更接近 UC

见第二份文档。要点：热路径只改内存；Flush 仍带 writer fence（这点比 UC 的 HMSet 严）；跨 Actor 不再持久 SourceOrder。

---

## 7. 对照表（面试时画这个）

| 问题 | UC | 农场可靠模式 | 农场 dirty 实验 |
|---|---|---|---|
| 谁保证单写者 | mailbox + shard 状态机 | mailbox + Redis CAS/fence | mailbox + 本地 generation，Flush 时 fence |
| 放置单位 | shard，CR 指定 Pod | shard，ScalePlan 指定 Holder | 同左 |
| 热路径是否写库 | 否，MarkDirty | 是，每 Turn Lua | 否，MarkDirty |
| 存储是否 CAS | 否，HMSet | 是 | Flush 时是 |
| 跨实体恢复 | event / RPC，尽最大努力 | SourceOrder + Worker | 直接 RPC，无 Worker |
| 崩溃 RPO | 未 Flush 窗口 | 上一笔提交 | 上次成功 Flush |
| 迁移搬什么 | 先 Flush；脏才搬全量 | 先持久化再换写权，按库重建 | Drain 先 FlushNow 再换写权 |
| 接入 | gatesvr 无业务 | farm-gate 无业务 | 同左 |
| 共享世界 | scenesvr 可开线 | FarmActor 不可拆 | 同左 |

---

## 8. 面试追问清单

**「Mailbox 和锁有什么区别？」**  
执行互斥不靠锁，靠单 goroutine。锁只保护 mailbox 记账（字节预算、inFlight）。热路径业务状态无锁。

**「为什么不把每个玩家做成一个 Pod？」**  
放置表会爆。shard 把成千上万 Actor 捆成可调和的单元。

**「为什么存储不做 CAS？」**  
UC：单 Holder 已经保证单写者，CAS 是额外往返。代价是脑裂不可检测。农场可靠模式不接受这个，所以 Lua 带版本。

**「脏回写正确性靠什么？」**  
同 Actor 串行 + `version/savedVersion` 水位 + UniqueFifo 合并 + 限速。存盘期间的新变更不会被旧成功回调清掉。

**「迁移会不会双写？」**  
Import 不接普通流量；Handoff 回 AskRedirect；写权先离开旧 Holder。农场再加 `writer_epoch`，旧 Flush 会被拒。

**「种植这种跨两个权威的，UC 怎么做？」**  
UC 尽量塞进同一个 PlayerActor，或 RPC + event。农场必须跨 Player/Farm，所以可靠模式上了 SourceOrder。这是业务逼出来的，不是为了炫技。

**「你这个项目和网关电商有什么不同？」**  
电商那条是无状态服务 + 缓存 + 消息队列。这条是有状态 Actor、分片所有权、在线迁写权。Session 和业务权威不在一层。

---

## 9. 3 分钟口述稿

我做过一套实时农场后台，对照的是公司 UC 那套有状态 Actor。

UC 的核心是：同一个玩家用 mailbox 串行；很多玩家捆在一个 shard 上，由 K8s CR 决定哪台机器持有这个 shard；热路径只改内存，后台限速回写 Redis 或 Tcaplus。存储不做 CAS，单写者完全靠「一个 shard 只有一个活 Holder」。迁移时新机器向旧机器拉 Actor 快照，脏的才搬全量。

农场不能把共享农场写进同一个玩家 Actor，否则别人偷菜会堵住农场主买东西。所以我拆成 PlayerActor 和 FarmActor，1024 分片两套路由。为了种植、收获这种跨权威操作可恢复，可靠模式每条命令走 Redis Lua CAS，并写下 SourceOrder，Worker 扫到期索引恢复。

在线迁移我们验证过：一个热点分片两万玩家、两千在线，三秒迁完，窗口里十四万多次命令零失败。做法是先停收、排空、落盘，再换 writer_epoch，不是拷两万条 Redis。

后来也做了 UC 风格的延迟落库实验：热路径不写 Redis，多次修改合并一次 Flush。单 Actor 微基准快了大约五十倍，但五万机器人连续经营还没过，脏龄到过三分钟。所以生产默认仍是 CAS，dirty 先用在可重建、允许短暂未落盘的数据。
