# 经典农场项目：简历与项目介绍素材

日期：`2026-08-26`

用途：简历、面试自我介绍、项目复盘。所有数字都区分“真实实测”和“容量模型”，禁止夸大。

## 1. 简历推荐写法

### 1.1 项目名称

**经典农场高并发游戏后台｜核心架构与性能优化**

技术栈：`Go / WebSocket / gRPC / Protobuf / Redis Cluster / Kubernetes / Actor / Prometheus / Grafana / pprof / TypeScript`

### 1.2 四条项目描述

- 设计并实现 PlayerActor、FarmActor 双权威模型，以 `player_id/farm_id → 1024 VirtualShard → Holder Pod` 完成有状态路由；通过 Lease、`writer_epoch` 和 Redirect 防止迁移期间双写，并接入 Kubernetes Pod 发现与 Shard 自动均衡。
- 构建 `HTTPS 登录 → 一次性 ConnectionTicket → WSS ClientHello → Join/Snapshot/Catch-up/Ready` 会话链路；在 Gate 内隔离连接收发、业务 RPC 和每连接有界下行队列，4 Gate 实测完成 4 万冷用户、`5,623.7` 次完整登录入场/秒，成功率 100%。
- 将普通经营热路径从每 Turn 同步 Redis CAS 优化为 Actor 内存串行裁决、Dirty 合并与延迟写回，并通过 ownership transfer、稀疏字段提交和类型化缓存减少状态复制；单 Cell 2 Player + 2 Farm 下，2 万在线 Bot 完成约 243 万次真实经营动作，稳态 `9,697 action/s`，p95/p99 为 `93/192ms`。
- 完成好友关系 canonical pair、好友机会 Header/Facts 两阶段查询、DueIndex 到期唤醒、在线 Hint 与 CandidateWindow 连续导航；300 好友场景中，冷导航约 `176ms`、热导航 `11～13ms`，成熟/浇水提醒约秒级到达，同时保留最终 FarmActor 精确裁决。

### 1.3 如果简历只能写三条

- 负责经典农场后台总体架构：双 Actor、1024 VirtualShard、有状态路由、Redis Cluster 持久化及 Kubernetes 自动扩缩容，使用 Lease + `writer_epoch` 保证 Shard 迁移期间唯一写权。
- 优化 Actor 热路径与实时下行，将普通经营改为内存 Dirty 合并写回并减少完整状态复制；2 万在线全流程实测达到约 `9.7K action/s`，p95 `93ms`，无基础设施超时。
- 建设真实 Bot 压测与可观测体系；4 Gate 下完成 4 万冷登录用户、`5.62K/s` 完整 Login→WSS→Join→Snapshot，结合 Prometheus、pprof 定位下行 Lane、Actor 编解码和 Redis 尾延迟瓶颈。

## 2. 数字使用边界

| 说法 | 是否可以 | 原因 |
|---|---|---|
| 4 Gate 实测 5,623.7 次完整冷登录/s | 可以 | 40K/40K Ready，p95 健康 |
| 20K 在线实测约 9,697 action/s | 可以 | 完整 Buy/Plant/Care/Harvest/Sell，严格通过 |
| 系统峰值 15K QPS | 不建议 | 30K 过载档虽有 15,373/s 峰值，但大量 Bot 失败 |
| 架构按 3000 万 DAU 建模 | 可以 | 明确是容量模型 |
| 已实测承载 3000 万 DAU | 不可以 | 没有 100+ Cell 的生产级多机房实测 |
| Shard 自动扩容已经实现 | 可以 | 已有 2→3 Pod、341/342/341 分布证据 |
| 高负载迁移完全无感 | 不可以 | 仍有 StoreUnavailable/重试与 Rejoin 缺口 |

面试时主动说明边界会比夸大数字更加分。

## 3. 30 秒项目介绍

> 我做的是一个经典农场游戏后台。难点不只是种菜，而是同一农场可能被多人访问，既要串行裁决偷菜和护理，又要支撑大量长连接、好友提醒和滚动扩容。我们把玩家资产和农场状态拆成 PlayerActor、FarmActor，分别按 1024 个虚拟 Shard 路由到 Holder Pod；Gate 负责登录会话、WebSocket 和下行背压，Redis Cluster 保存权威状态、路由和查询数据。后期我重点做了 Actor Dirty 热路径和真实 Bot 压测，单 Cell 验证了 2 万在线、约 9.7K 经营动作每秒，以及 4 Gate 下 5.62K/s 完整冷登录。通过 pprof 又定位出下一阶段主要是 Protobuf 编解码、状态复制和 Snapshot 下行可靠性问题。

## 4. 90 秒项目介绍

> 项目的业务是多人可访问的经典农场。客户端先通过 HTTPS 登录换一次性 Ticket，再建立 WSS，ClientHello 核销 Ticket 后形成在线 Session；Join 农场需要经过 ACCEPTED、Snapshot、Catch-up 和 Ready，Ready 前禁止写操作，避免页面状态和服务器版本出现空洞。
>
> 核心服务只有 Gate、PlayerSvr、FarmSvr 和 FarmWorker。PlayerActor 管资产、仓库、任务、宠物和邮件领取；FarmActor 管地块、作物、护理、偷菜 Receipt 和同场订阅。Actor 不直接按 Pod 数取模，而是先映射到固定 VirtualShard，所有进程本地缓存 Shard→Holder 快照；Kubernetes 只负责发现 Pod，RouteControl 负责分配 Shard、Lease、writer_epoch 和迁移。
>
> 一开始所有命令都同步 Redis CAS，正确但吞吐较低。后来我们对普通经营引入 Dirty 合并写回，关键资产仍保留可靠提交；同时消除 Actor Model 的多次克隆、减少稀疏提交和编码复制。最终在 4 Gate、2 Player、2 Farm 的单 Cell 中，4 万冷用户达到 5.62K/s 完整登录入场，2 万在线完整经营达到 9.7K action/s，p95 93ms。25K 压力档暴露了 FarmSvr→Gate Snapshot 下行失败，pprof 还显示模型编解码和内存复制是下一主要优化点。这个项目让我真正走完了架构设计、正确性合同、实现、压测、故障定位和容量规划的闭环。

## 5. 架构速记

```text
H5 / WebView
  ├─ HTTPS Login / Cookie免密换票
  └─ WSS + Protobuf FarmFrame
           │
           ▼
       farm-gate
       ├─ Session / Ticket / RateLimit
       ├─ CommandCatalog / FirstHop Route
       ├─ ConnectionRegistry
       └─ per-connection Writer Queue
           │
      ┌────┴─────────┐
      ▼              ▼
 player-svr       farm-svr
 PlayerActor      FarmActor + SubscriptionHub
      │              │
      └──────┬───────┘
             ▼
        farm-worker
   DirtyFlush / DueIndex / Retry / Hint
             │
             ▼
       Redis Cluster
 Authority / Runtime / Projection 逻辑域
```

## 6. 四个最适合面试展开的故事

### 6.1 从单 Gate 1.9K/s 到四 Gate 5.6K/s

**背景**：单 Gate 压 40K 冷登录时只完成 30,867，Login p95 接近 7 秒。

**定位**：连接、Join、SessionRoute、Snapshot 下行都集中在一个 Gate；仅扩大队列不能提高处理和网络并行度。

**行动**：将 Gate 改为稳定 StatefulSet 身份，CLB 均衡连接，SessionRoute 保存目标 `gateway_id`，FarmSvr 配置全部 Gate 的 Downlink/Control endpoint，并增加按 Gate 的连接和 Snapshot 指标。

**结果**：

```text
1 Gate: 1,919.5/s，9,133失败
2 Gate: 3,599.3/s，零失败但高延迟
4 Gate: 5,623.7/s，40K/40K成功，Join p95 47ms
```

**反思**：接入层可以水平扩展，但瓶颈会转移；25K Journey 仍暴露 FarmSvr→Gate 下行 RPC 失败，说明容量优化必须端到端看。

### 6.2 从同步 CAS 到分级 Dirty Writeback

**背景**：每个经营 Turn 都对完整 Actor 状态同步 Redis CAS，网络 RTT、编码和状态复制进入热路径。

**方案**：

```text
Actor Mailbox 串行裁决
→ 更新内存 ModelState
→ dirty_generation++
→ 相同 Actor 合并
→ 异步 Flush 最新冻结版本
→ writer_epoch 防旧 Holder 写入
```

付费资产、邮件附件、唯一奖励和跨玩家最终结果仍使用可靠提交；不是把所有一致性都删掉。

**结果**：20K 在线、2.5 秒思考时间下达到 9.7K action/s；同时通过 `oldest_dirty_age`、queue depth 和 Flush error 控制 RPO。

**反思**：Dirty 提升的是热路径，不会消灭 Worker、幂等、下行恢复和故障语义。

### 6.3 VirtualShard 与自动扩容

**背景**：直接 `hash(id) % pod_count` 在 Pod 数变化时会让大量 Actor 瞬间换节点，新旧 Pod 可能同时写。

**方案**：固定 1024 VirtualShard；本地路由快照完成 O(1) 热路径；RouteController 监听 Kubernetes Pod，再通过 HolderControl 校验进程身份，生成最小迁移计划并推进 Lease/`writer_epoch`。

**结果**：2→3 Pod 后 1024 Shard 自动均衡为 `341/342/341`，Actor 不整 Shard搬迁，冷 Actor 在新 Holder 懒加载。

**反思**：Kubernetes 解决 Pod 生死，不知道哪个 Pod 拥有某个游戏 Shard 的写权，所以 RouteControl 仍然必要。

### 6.4 好友机会为什么不是每次加载 300 个 FarmActor

**背景**：每个玩家最多约 300 好友，首页弹窗和好友列表都要知道可偷菜、可浇水状态。

**方案**：

```text
Farm Header：低成本粗筛时间边界
Farm Facts + TheftReceipt：按玩家精确裁决
CandidateWindow：连续导航加速，不作权威
DueIndex + Worker：到期唤醒
Hint：在线加速，H5收到后仍定向精查
120s Pull：弱网兜底
```

**结果**：300 好友冷导航 176ms，热导航 11～13ms；Hint 定向查询 10～12ms。

## 7. 常见追问的一句话答案

| 问题 | 一句话回答 |
|---|---|
| 为什么用 Actor？ | 同一玩家或农场的并发操作天然需要串行裁决，Actor 把锁竞争变成 Mailbox 顺序处理。 |
| 为什么拆 PlayerActor/FarmActor？ | 玩家资产和农场多人互动的扩容、访问热点和故障边界不同，不能强绑在一个大 Actor。 |
| 为什么还要 Workflow？ | Actor 只能原子修改自己的权威状态，跨 Actor 步骤需要可恢复的 Coordinator/Order 和幂等 Receipt。 |
| 为什么 WebSocket？ | 农场变化、好友 Hint、KICK 和同场广播需要服务端主动下行；普通 HTTP 只承担登录和少量查询。 |
| 为什么不直接一致性哈希到 Pod？ | Pod 数变化会导致大范围瞬时重映射，无法表达唯一写权、Drain 和有序 Handoff。 |
| Redis 是数据库还是缓存？ | 当前实验环境同时承载 Authority、Runtime、Projection 三个逻辑域，但 key、可靠性和容量预算必须分开。 |
| Dirty 会不会丢数据？ | 会有明确 RPO，所以只用于产品可接受回退的普通状态；关键资产继续可靠提交。 |
| 4 Gate 是否代表只能支持 4 万连接？ | 不是；40K 是本轮压测规模，Gate 数由连接内存、TLS/WSS、带宽、下行与 SLO 共同决定。 |
| 如何证明 3000 万 DAU？ | 先冻结用户行为模型，再用单 Cell 实测推导 Cell 数，并补多 Cell、故障、长稳和 N+1 验证。 |

## 8. 不能回避的项目不足

面试时可主动说：

1. 25K Journey 暴露了 FarmSvr→Gate 下行 unavailable，Snapshot 可靠性还需修复；
2. 高负载 Shard 迁移尚未完全对客户端无感，仍需统一同 operation_id 重试和 Fresh Join；
3. Redis 测试集群没有 Replica，不能代表生产主从切换能力；
4. pprof 显示 Actor Field Envelope、Protobuf Reflection 和视图构建分配仍然很高；
5. 3000 万 DAU 是基于单 Cell 证据的容量模型，不是全量生产实测。

能明确说出不足、证据和下一步，通常比声称“系统没有问题”更可信。
