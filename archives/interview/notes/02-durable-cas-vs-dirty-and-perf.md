# 可靠 CAS + SourceOrder，对比 Dirty 延迟落库，以及压测怎么优化、卡在哪

用途：面试讲清楚两条数据面，以及哪些数字能说、哪些不能说。
默认生产：`durable_cas`（master）。
实验开关：`FARM_GAME_DATA_MODE=dirty_best_effort`（未作为生产默认合入）。

---

## 0. 先给结论

| 问题 | 结论 |
|---|---|
| 哪条是现在能对外讲的正确性方案 | 每条变更 Turn 同步 Redis Lua CAS，跨 Actor 用 SourceOrder + Worker |
| Dirty 做成了什么 | UC 风格：热路径只改内存，合并 Flush，Flush 仍带 writer fence |
| Dirty 比 CAS 快多少 | 单 Actor 500 次加金币：1860 → 97479 turns/s，约 52 倍；500 次 EVALSHA → 1 次刷盘 |
| Dirty 全链路过了吗 | 5 万机器人登录/进房过了；连续经营没过。最好一轮 6215 action/s，最老脏龄 184.7 秒 |
| 可靠模式签过字的经营 | 5000 旅程：0 拒绝、0 超时，约 1340 action/s，P95 约 50ms |
| 登录容量 | 双 Gate 冷登录 3000/s 稳定（P95 54ms）；3200/s 短时能满 Ready，延迟已拐 |
| 迁移 | 单热点分片 23.35s → 3.15s，14.7 万命令零失败；整 Pod 4→5→4 还没过正确性 |

一句话：*Dirty 证明了「去掉每 Turn Redis」的理论上限；还没有证明「五万真人连续种菜也能又快又正确」。*

---

## 1. 可靠模式：每 Turn CAS + Order

### 1.1 一条普通资产命令

```text
Gate 校验 Session，按目录首跳到 Player 或 Farm
  → Holder 按 ActorAddress 进 mailbox
  → 首次激活：LoadAndClaim（占写权、读模型、审计 receipt）
  → 后续变更：内存算出新状态
  → Redis Lua Commit（WriterClaim + RecordVersion CAS [+ SourceOrder]）
  → 成功才结束这个 Turn
```

CAS 的意思：脚本带上「我还是当前写者、版本还是我看到的那一版」。对不上就失败或走 Unknown 调和，最多重试 5 次。

这和电商「SETNX 锁一下再扣库存」不同：锁是旁路；这里版本在权威提交里，失败不能当成功。

### 1.2 跨 Actor：SourceOrder 是什么

种植不是两台机器各改各的就完了。

```text
Player 预留种子
  同一次 Lua：改背包 + 写下 SourceOrder + 挂到 DueIndex
  → 直接 RPC Farm 落地块
  → Farm 成功后 ClaimAndComplete 这张 Order
失败 / 进程死 / RPC 超时
  → Order 还在 DueIndex
  → farm-worker 扫到、占租约、重放目标、再 Complete
```

收获、护理、偷菜方向类似，只是源权威换成 Farm，钱和作物回到 Player。

热路径后来的削减（可靠模式上已经落地）：

- 首次 LoadAndClaim 之后，BUY/SELL 复用 Binding 快照，不再每条 Load
- Commit 审计路径走到 V15：跳过无关历史 HGET，热提交大约 **12** 条 Redis 命令
- 同 claim 的 LoadAndClaim V6：**6** 条命令，不随 receipt 历史变长
- Care/Harvest Direct 成功后不再多一次 SourceOrder `HGETALL`

Worker 仍然是正确性的底：Direct 没跑完，到期索引会接手。

### 1.3 可靠模式换到了什么

- 崩溃：恢复到上一笔已经提交的命令
- 超时：客户端看到 PROCESSING，可用 `GET_OPERATION_RESULT` 再问
- 同一 `operation_id`：幂等，不会因为重试再扣一次
- 代价：热路径绑定 Redis RTT。单 Actor 微基准 500 次就是 500 次 EVALSHA，268ms

---

## 2. Dirty 模式：先写内存，再合并刷盘

配置：`FARM_GAME_DATA_MODE=dirty_best_effort`。

### 2.1 热路径

```text
命令进 mailbox
  → 改进程内最新状态
  → actor_memory_version++
  → Dirty 队列按 actor_id 合并（改 100 次也只占 1 个槽）
  → Turn 立刻结束，不访问 Redis
```

首次激活仍要 `LoadAndClaim`。之后禁止因为「物理容量视图暂时没有」再读 Redis——否则会用旧持久态盖掉还没刷的新内存。这是 loadfix 修过的真实 bug。

`actor_memory_version` 是逻辑代次。Redis `RecordVersion` 是物理刷盘次数。500 次内存修改可以只对应 1 次 Flush，两套版本不能混用。

### 2.2 三段 Flush（学 UC，但 Flush 仍 CAS）

默认：合并窗 3s，抖动 600ms，脏龄目标 10s，每进程 4 个 Flush worker，最多跟踪 1024 个脏 Actor。

```text
Prepare（mailbox 内）
  冻结 ModelState(V)

Execute（mailbox 外）
  稀疏 mutation + Redis CAS（writer_epoch + claim + 物理版本）
  慢 Redis 不堵后续 Turn

Complete（mailbox 内）
  只确认 V
  期间已经是 V+1：继续 Dirty，再入队
```

队列满：这一 Turn 退化成同步 Flush，保的是「还有人负责写回」，换的是延迟。

和 UC 的差别，面试一定要讲：

| | UC | 农场 Dirty |
|---|---|---|
| 回写 | HMSet，无 CAS | Flush 仍 writer fence + CAS |
| 限速 | 全进程 200 Actor/s UniqueFifo | 每进程 4 worker + 3s 窗 |
| 跨实体 | 尽量同一 PlayerActor / event | 直接 RPC，不写 SourceOrder |
| 迁移 | 先 Flush，脏才搬内存 | Drain 先 FlushNow，再换 epoch |

### 2.3 Dirty 明确放弃什么

- 命令返回前的 Redis 耐久
- 硬崩溃前未 Flush 的状态
- Plant / Harvest / Care / Theft / Unlock / GuardSync 的 Worker 恢复
- 跨 Actor 最终一致

farm-worker 进程还在，只是上述经营家族关掉。好友机会唤醒、邮件、投影、路由就绪还在跑。

正常 Drain：停新 Turn → 等已接纳的做完 → 该 shard 全部 `FlushNow` → 再 bump `writer_epoch`。旧 Holder 迟到 Flush 会被拒。

如果 Drain/LeaseLost 通知丢了：旧节点可能先用内存应答，随后 Flush 被拒，结果丢失。这是实验接受的窗口。

### 2.4 20260825 热路径减拷贝

一次 Dirty Turn 原来可能把完整 ModelState 连续拷四五遍。现在：

- Turn 内借用当前快照（用完不能留指针）
- 新候选所有权直接交给 Binding
- Flush 只在冻结时拷一次

单元测试和 race 过了。线上 Runtime 当时没换成这版镜像，**不能拿它当 QPS 涨了的证据**。4 机器人好友偷菜 4/4 成功，证明的是机器人，不是减拷贝。

---

## 3. 两条路径怎么选（产品语言）

```text
必须不丢、不能重做          → durable_cas
  金币、库存、种植收获结算、付费、邮件附件、任务领奖

丢了能重建、晚一秒无所谓    → 可以 dirty
  纯展示、可重算投影、本进程内的过程计数

跨两个权威、失败要补        → 先别 dirty
  现在的实现是部分成功 + 不恢复
```

不要说「我们已经全面改成延迟落库」。要说「两条可切换路径，默认仍是命令级落库」。

---

## 4. 压测数字（能说的和不能说的）

### 4.1 Dirty 微基准（唯一真 A/B）

公司 Redis 三主三从，单 PlayerActor，连续 500 次 `金币+1`，无 Gate、无种植。

| | durable_cas | dirty |
|---|---:|---:|
| 总耗时 | 268 ms | 5 ms |
| 吞吐 | 1860 turns/s | 97479 turns/s |
| p99 | 2265 μs | 52 μs |
| 热路径 Redis | 500 EVALSHA | 0 |
| 最终刷盘 | 0 | 1 次，1884 μs |
| 金币 | +500 | +500 |

只能证明：去掉每 Turn CAS 之后，**存储微基准**上限极高。不能换算成整服 QPS，更不能换算成 3000 万 DAU。

### 4.2 Dirty 5 万机器人真实链路

2 Gate + 2 Player + 2 Farm + 2 Worker，1024 shard。登录 1000/s，思考 5s，经营 3 分钟。理论约 1 万 cmd/s。

三轮都是 **50000/50000 Ready**。连续经营都没过。

| | R1 | R2 | R3 |
|---|---:|---:|---:|
| 失败机器人 | 29000 | 23618 | 2838 |
| 干净机器人 | 21000 | 26382 | 47162 |
| action/s | 3539 | 4293 | **6215** |
| 拒绝 | 50524 | 48297 | 32393 |
| 最老脏龄 | 274.9s | 39.4s | **184.7s** |
| Flush 成功峰值 | 8227/s | 8895/s | 9766/s |
| Farm 下行 queue-full | 未隔离 | 12134/s | **0** |
| Redis P95 RTT | 89ms | 113ms | 143ms |

R3 把下行打满修掉了，瓶颈挪到 Gate 后端 in-flight 8192、RPC 超时、Redis 连接等待。脏产生速度仍高于刷盘公平性，10 秒脏龄目标没达到。

**没有**「同样 5 万、同样旅程」的 durable 对照。5000 旅程 PASS 是可靠模式；5 万是 dirty。不能直接说 dirty 比 durable 更能打 5 万。

### 4.3 可靠模式签过字的经营

5000 共享旅程：5000 Ready、5000 成功，241459 次动作，拒绝 0、超时 0，最差分片 P95 49.8ms，约 **1340 action/s**。Worker 接手 CARE/HARVEST/PLANT 共 177 次，说明 Order 恢复在工作，不是摆设。

### 4.4 冷登录（可靠拓扑，和 dirty 改造无关）

双 Gate、Player/Farm 4C16G：

| 速率 | 结果 |
|---|---|
| 3000/s | 539994/539994 Ready，P95 53.7ms，P99 64ms。舒适点 |
| 3100/s | 99.9966% Ready，P95 87ms，开始拐 |
| 3200/s | 100% Ready，Ready P95 416ms。短时上沿 |
| 3400/s | Ready 约 24%。不是 Holder 极限，是 2 CPU Gate |

Gate Redis pool 从默认约 20 提到 64 之后，3200/s 才不再 `connection pool timeout`。Player/Farm 池没跟着加。

### 4.5 在线迁移

单 PLAYER 热点分片，2 万记录、2 千在线：

- 未优化 23352ms
- 优化后去 3153ms、回 3149ms，约 7.4 倍
- 窗口约 147612 命令，失败/超时/unavailable = 0
- 不拷 2 万条 Redis，只 fence 并转移写权

整 Pod 4→5→4：布局正确，78 次 unavailable，判定 FAIL。单分片成功不能外推。

---

## 5. 压测是怎么优化上来的

按「改了什么、证据是什么」记，面试别讲成玄学。

### 5.1 登录路径

1. **懒 mailbox。** 先分配 16 槽，顶 64。1000/s 时 Player 堆里 `mailboxFor` 曾占约 87%。不加这个，冷登录先被内存打死。
2. **Shard 路由查找不再整表拷贝。** `36f43b0`。3000/s 延迟下降不能单独归因这一笔，但热路径少了一大块分配。
3. **Gate Redis 池 64。** 直接解锁 3200/s 满 Ready。再往上加 Gate 并发槽，会把压力推到 Farm 下行打满——1024 槽时 476 次 BACKEND_SLOTS_FULL；提到 3096 后槽位拒绝没了，Farm queue-full 却到 52285。
4. **教训：** 接入放行必须和下游 mailbox / 下行 / Redis 池一起加。只开大门，里面会淹。

### 5.2 经营路径（可靠模式）

1. Binding 复用，避免每命令 Load
2. Lua V10→V15，热提交命令数压到约 12
3. Direct 完成后少一次 Order 读
4. DueIndex 提示缓存（5s）降低 Worker 扫，真值仍以 Claim 为准

### 5.3 迁移路径

1. 精确钉住一个 shard，而不是整 Pod 一起滑
2. 先停收、排空、落盘，再换 epoch
3. 有界 proof 重试吸收切换抖动，最终看到的是 RESUMED，不是超时
4. 观测用分片分布、ACTIVE 交接、命令矩阵、错误面板四张图一起看

### 5.4 Dirty 路径上已经试过的

1. 第二 Turn 禁止回读 Redis（否则覆盖未刷修改）
2. 热路径去掉每 Turn 的 shard fence 读（否则「不写 Redis」是假的）
3. Flush 完成只确认冻结版本
4. Drain 与后台 Flush 避免互相等待
5. 减 ModelState 拷贝（尚无 QPS 证据）
6. 5 万链路上：修下行打满后，失败机器人从 2.9 万降到 2.8 千，但延迟和脏龄变差——瓶颈会搬家，不会消失

---

## 6. 现在的瓶颈和优化空间

### 6.1 已经能指着图说的

1. **2+2+2 撑不住 5 万连续经营。** 登录可以，经营不行。脏产生 > 刷盘公平，最老脏龄到过 3 分钟。
2. **舒适登录就是 3000/s。** 再高先加 Gate CPU / 连接，不是先加 Player 规格。
3. **可靠 5000 旅程的 Redis 放大仍然高。** 文档自己写：离「单 action 20～30 条服务端命令」还远，两千 action/s 只是猜测，没爬坡证明。
4. **Mailbox 是 Holder 堆的主角**，Gate RSS 只有十几 MB。要堆在线人数，先看每 Actor 常驻，不是先看 Gate。
5. **整 Pod 扩缩的连续性还没过。** 路由交接会漏出 unavailable。

### 6.2 有空间、但还没证明的

1. **Dirty 分层。** 展示字段 dirty，金币/跨 Actor 仍 CAS。开关在代码里是整进程模式，还不是按字段/按命令。
2. **Flush 公平性。** worker 从 4 往上、按脏龄调度、分 shard 限额，目标是把 184s 拉回 10s。没有故障注入，不能承诺 RPO。
3. **减拷贝上线再 A/B。** 现在只有单测。
4. **Gate 与下行的联动配额。** 5 万 R3 说明修了一处，超时会从另一处冒出来。
5. **热分片再拆负载。** 现在哈希固定。明星农场只能加机器均摊，不能把一个 FarmActor 拆开。
6. **可靠模式继续砍 Redis 往返。** V15 之后还有 Participant proof、DueIndex、路由租约续期。每少一次 RTT，5000 旅程的尾巴更稳，不改变正确性模型。
7. **整 Pod 迁移的连续性。** 单分片那套 fence 要推广到同时搬 200 个 shard，核心是「切换瞬间的有界重试」而不是再提速 3 秒。

### 6.3 方案评价（直接表态）

**可靠 CAS + Order：作为正确性底座是对的。**  
种植这种「先扣后改、失败要退」不能靠内存回调。5000 旅程零拒绝，说明这条能在真实协议上站住。它不是容量上限方案：热路径绑 Redis，单 Actor 微基准已经比 dirty 慢两个数量级。

**Dirty：作为性能上限实验是对的，作为生产替换还早。**  
机制学 UC 学到位了：合并、mailbox 外 IO、版本冻结、Drain 强刷、旧 Flush fence。5 万登录过了，说明「不每 Turn 写库」对在线人数友好。连续经营没过、脏龄分钟级、跨 Actor 不恢复——这三条任何一条都够否掉「已经能换默认」。

**和 UC 比，农场 Dirty 其实更保守。**  
UC 回写无 CAS，崩溃窗口认了；农场 Flush 仍 fence。UC 把经营尽量塞进一个 PlayerActor；农场有第二权威，所以一关 SourceOrder，跨农场就只剩尽最大努力。这不是实现没做完，是模型换了。

**不该走的捷径：**

- 用 52 倍微基准对外说整服快了 52 倍
- 用 3000/s 登录说已经支撑 3000 万 DAU
- 用 3.15 秒单分片说任意扩缩都零错误
- 为了 QPS 把金币和种植也切 dirty，又不做崩溃注入

---

## 7. 面试怎么讲（按追问深度）

### 1 分钟

我们有两条数据面。默认每条命令 Redis CAS，跨 Actor 写 SourceOrder，Worker 能恢复，崩溃回到上一笔提交。另外做了延迟落库：热路径只改内存，几秒合并一次带写权的 Flush。单 Actor 五百次加金币从 268 毫秒降到 5 毫秒。五万机器人能进房，连续种菜还没过，最老脏数据到过三分钟，所以默认没切过去。

### 3 分钟再加

可靠模式热提交大概十二次 Redis。登录三千每秒稳定。热点分片两千在线三秒迁完、十四万命令零失败，靠的是先落盘再换 epoch。Dirty 的 Flush 在 mailbox 外做，不堵下一笔；但跨农场不再持久恢复。优化都是「找到真正打满的那一层再动」：mailbox 内存、Gate 连接池、下行队列、Lua 命令数。只开上游阀门，下游会换一个地方死。

### 被追「你会怎么继续优化」

先分层：展示 dirty，结算 CAS。再把 Flush 按脏龄调度，用杀进程实验量 RPO。5 万经营要 Gate、下行、Redis 池、Flush worker 一起加，不要单改一个。整 Pod 迁移先补连续性，不先追更快。减拷贝必须换上镜像再 A/B，不能用单测代替。

### 被追「和 Redis 锁 / 消息队列有什么不同」

锁是旁路互斥，队列是异步解耦。Actor mailbox 是结构上的单写者。SourceOrder 是跨权威的可恢复凭证，不是「丢进 MQ 就当成功」。Dirty 的合并队列也不是业务 MQ，它只负责同一 Actor 的落盘，不负责补偿种植。

---

## 8. 数字速查（只背这些）

| 场景 | 数字 | 口径 |
|---|---|---|
| Dirty 微基准 | 1860 → 97479 turns/s | 单 Actor，500 次金币 |
| Dirty 5 万 | 5 万 Ready；最好 6215 action/s；脏龄 184.7s | 连续经营未通过 |
| 可靠 5000 旅程 | 0 拒绝；~1340 action/s；P95 ~50ms | 签字 PASS |
| 冷登录 | 3000/s P95 54ms；3200/s 短时满 Ready | thick-login，非好友热路径 |
| 热点迁移 | 23.35s → 3.15s；14.7 万命令 0 失败 | 单 shard，2 千在线 |
| 整 Pod 扩缩 | 78 unavailable | FAIL，别说成成功 |
| Lua 热提交 | 约 12 命令（V15） | 可靠模式 |

---

## 9. 不要写进简历、但要能解释的边界

- 3000 万 DAU 是设计目标
- Dirty 不是生产默认
- 5 万 dirty 经营没有和 durable 同 N 对照
- 20260825 减拷贝没有 QPS A/B
- 整 Pod 迁移未过正确性
- 微基准 52 倍不是端到端 52 倍
