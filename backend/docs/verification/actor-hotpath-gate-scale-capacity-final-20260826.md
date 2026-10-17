# Actor 热路径、Gate 扩容与容量边界最终实测

日期：`2026-08-26`

代码分支：`codex/actor-hotpath-perf-r2`

代码提交：`88ca7e36`

状态：真实 Kubernetes、真实四服务链路、真实 Redis Cluster 实测；证据已保存，**未合入 Integration**。

## 1. 最终结论

本轮不再用单个峰值 QPS 代表容量，而是同时检查成功率、端到端延迟、Join/Snapshot 完整性、Redis、Dirty Queue 和资源水位。

当前能严格对外表述的结论是：

1. 在 `4 Gate + 2 PlayerSvr + 2 FarmSvr + 2 Worker` 的单 Cell 中，`40,000` 名冷用户以目标 `6,000/s` 到达时，全部完成 `HTTP Login → WSS → Hello → Join → Snapshot → Ready`，实际完整冷登录吞吐为 `5,623.7/s`，Login/Hello/Join p95 分别为 `22.74/18.68/47.14ms`。
2. 在相同业务服务拓扑下，`20,000` 名在线 Bot 以 `2.5s` 思考时间持续执行完整经营链路，完成 `2,425,296` 次动作，稳态约 `9,697.4 action/s`，业务 p95/p99 为 `93.0/192.0ms`，基础设施超时为 `0`。
3. `25,000` 和 `30,000` 档均未达到严格通过条件。它们产生过 `11,539/s` 和 `15,373/s` 的命令结果峰值，但存在 Join/Snapshot 丢失、Bot 失败或严重排队，**不能把这些峰值写成系统可承载 QPS**。
4. 当前单 Cell 的推荐安全设计点仍取 `20,000` 在线用户；`20K～25K` 是需要通过业务服务扩容和下行可靠性修复重新验证的压力区间。
5. 四个 Gate 的价值已被 A/B 数据证明，但它只解决接入、WSS 连接和下行隔离。进一步增加 Gate 不会修复 Player/Farm CPU、Actor 编解码、Dirty Writeback 或 FarmSvr→Gate 下行失败。

一句话总结：

```text
冷登录入口已经能通过 Gate 水平扩容达到 5.6K/s；
完整经营的严格通过点是 20K 在线、约 9.7K action/s；
下一瓶颈已转移到 Actor 模型分配、Player/Farm 资源和 Snapshot 下行可靠性。
```

## 2. 测试环境

```text
Kubernetes context: uc-test-plus
Namespace:          developer_026-qqfarm
Route Runtime:      sha256:f0c59d5d8381bc7103620a05c4e1a23e3c3bb31cda8badb2ee4d8536c8d92309
Git commit:         88ca7e36

Gate:               1 / 2 / 4 Pod 对照，每 Pod 2 CPU / 4 GiB
PlayerSvr:          2 Pod，每 Pod 4 CPU / 16 GiB
FarmSvr:            2 Pod，每 Pod 4 CPU / 16 GiB
FarmWorker:         2 Pod，每个负责 512 个 Farm Shard
Redis:              3 Master Redis Cluster，本测试环境没有 Replica
VirtualShard:       PLAYER 1024 + FARM 1024
```

每轮使用独立 Redis Zone，避免复用 Actor 内存和测试账号。测试结束后只清理本轮明确命名的 Zone，原始证据目录保留。

## 3. 场景口径

### 3.1 冷登录

每个 Bot 完整执行：

```text
HTTP Login
→ ConnectionTicket
→ WSS
→ ClientHello / ServerHello
→ JoinOwnFarm
→ ACCEPTED
→ ArmSnapshot
→ Snapshot / Catch-up
→ SubscriptionReady
```

统计的是“真正进入自己农场”的吞吐，不是单独的 HTTP 登录 QPS。

### 3.2 完整经营 Journey

每个 Bot 登录并保持在线，循环执行：

```text
Buy → Plant → Care → Harvest → Sell
```

动作真实经过 Gate、Actor 路由、PlayerActor/FarmActor、FarmChange 下行、内存 Dirty 状态和 Redis 延迟写回。

`reported_actions_per_second` 是 Bot 观测到的业务动作吞吐；Prometheus 的 command result 峰值还包含 Bootstrap、Join 和协议命令，两个指标不能混用。

## 4. 1 / 2 / 4 Gate 冷登录对照

三轮保持以下输入完全一致：

```text
Bot:             40,000
目标到达率:      6,000/s
Player/Farm:     2 + 2
Redis Zone:      每轮全新
账号:            每轮预初始化 40,000 个
```

### 4.1 结果

| Gate | Ready / 40K | 完整冷登录/s | Login p95 | Hello p95 | Join p95 | Redis RTT p95 | 结论 |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 30,867 | 1,919.5 | 6,918ms | 3,566ms | 2,034.6ms | 2,500ms | 失败 9,133，已过载 |
| 2 | 40,000 | 3,599.3 | 1,379ms | 731.2ms | 5,805ms | 740ms | 零失败，但延迟不可接受 |
| 4 | 40,000 | **5,623.7** | **22.74ms** | **18.68ms** | **47.14ms** | **4.77ms** | 严格通过 |

相对关系：

```text
2 Gate / 1 Gate 成功吞吐 ≈ 1.88x
4 Gate / 2 Gate 吞吐     ≈ 1.56x
4 Gate / 1 Gate 成功吞吐 ≈ 2.93x
```

它不是理想的 4 倍线性增长，因为瓶颈会逐步移动到 Redis、Actor 激活、Snapshot 生成与下行网络。

### 4.2 四个 Gate 是否真的都工作

40,000 次 Join 和 Snapshot 的最终分布：

```text
farm-gate-0: 10,007
farm-gate-1:  9,993
farm-gate-2:  9,984
farm-gate-3: 10,016
```

所有 Gate 均满足：

```text
Snapshot 数 = Join 数
rejected = 0
accept_without_snapshot = 0
snapshot_without_accept = 0
```

因此 4 Gate 不是“多起三个空 Pod”，而是 CLB、SessionRoute、四组连接表以及 FarmSvr 下行路由都真实参与了测试。

## 5. 完整经营容量阶梯

### 5.1 总表

| 在线 Bot | 到达率 | Ready / Success | 业务动作 | action/s | command result 峰值 | p95 / p99 | 结论 |
|---:|---:|---:|---:|---:|---:|---:|---|
| 20,000 | 2,000/s | 20,000 / 20,000 | 2,425,296 | **9,697.4** | 11,177.1/s | 93.0 / 192.0ms | 当前严格通过点 |
| 25,000 | 2,500/s | 20,852 / 20,852 | 2,477,069 | 9,062.9 | 11,539.0/s | 456.4 / 835.0ms | 4,148 失败，不通过 |
| 30,000 | 3,000/s | 28,164 Ready / 47 全程成功 | 1,420,959 | 6,127.6 | 15,372.8/s | 229.8 / 392.2ms | 严重不健康 |

### 5.2 20K 严格通过点

```text
基础设施 timeout:       0
expected business reject: 1,259
Dirty Queue 最大值:      33,650
Redis RTT p95 最大值:    56.11ms
Redis error 最大值:      0.755%
```

这里的 1,259 次拒绝是地块状态等预期业务竞争，不是 Redis、路由或网络错误。测试脚本仍应继续把预期业务拒绝和基础设施错误拆开统计。

### 5.3 25K 为什么不通过

25,000 次 Join 都在命令 RPC 上得到 `ACCEPTED`，但并非都收到 Snapshot：

```text
farm-gate-0 accept_without_snapshot = 779
farm-gate-3 accept_without_snapshot = 705
farm-svr-0 deliver_batch 重试耗尽 = 160 批
Farm 下行 queue_full = 0
JoiningBuffer dropped = 0
```

这说明问题不是有界队列直接打满，而是高压下某些 FarmSvr→Gate 批量下行 RPC 变为 unavailable，最终造成 `ACCEPTED` 后 Snapshot 未到，客户端在 PendingJoin 超时。

同时观察到：

```text
PlayerSvr CPU 峰值约 3.56 核
FarmSvr CPU 峰值约 3.53 核
Redis 节点内存约 5.2～5.6GiB / 6GiB
Redis RTT p95 峰值约 89ms
Dirty Queue 最大值 36,166
```

因此 25K 档是多因素压力失败，不能只靠扩大 Gate 数或队列容量解决。

### 5.4 30K 的峰值为什么不能写进简历

30K 档曾出现 `15,372.8 command results/s`，但只有 47 个 Bot 全程无失败，Join p95 上升到 2.289s。这个数只能说明系统在过载时仍处理了大量请求，不能说明它稳定承载 15K QPS。

容量数字必须同时满足：

```text
成功率
+ p95/p99
+ timeout/pending
+ Snapshot完整性
+ Dirty RPO
+ Redis/CPU余量
```

## 6. pprof 结论

pprof 取自 20K 严格通过档的 30 秒 CPU 样本和 alloc_space 样本。

### 6.1 PlayerSvr

CPU 主要热点：

| 热点 | flat 占比 |
|---|---:|
| `runtime.memmove` | 14.52% |
| size-class / allocator | 8.25% |
| syscall | 5.69% |
| Protobuf 字符串合并 | 3.67% |
| SHA-256 | 3.14% |

累计分配约 `89.24GB`，其中：

| 分配点 | alloc_space |
|---|---:|
| `fieldv2.makeEnvelope` | 23.49GB |
| Protobuf bytes merge | 13.33GB |
| 反射对象分配 | 11.93GB |
| HTTP/2 MetaFrame | 3.97GB |
| `player/actormodelv2.Encode` 累计 | 29.06GB |

### 6.2 FarmSvr

CPU 主要由 syscall、分配、SHA-256、内存复制、GC 和 Protobuf 编解码共同构成，没有单个业务函数独占 CPU。

累计分配约 `131.40GB`，主要包括：

| 分配点 | alloc_space |
|---|---:|
| Protobuf bytes merge | 24.63GB |
| 反射对象分配 | 15.58GB |
| `fieldv2.makeEnvelope` | 14.87GB |
| `plotViewWithContext` | 4.04GB |
| `decodeCareProof` 累计 | 7.95GB |

### 6.3 下一轮优化优先级

1. 缩小 Actor Model Envelope，避免每 Turn 对未修改字段重新封装；
2. 生成类型化编解码，减少 Protobuf Reflection 和 `mergeBytesNoZero`；
3. 将 FarmChange/PlotView 改为真正的字段级 Delta，减少完整 View 构建；
4. 复用 canonical bytes、SHA-256 中间结果和 HTTP/2 metadata；
5. 优化后重新比较 alloc/op、GC CPU、p95 和 Dirty Queue，不只比较平均 QPS。

## 7. 为什么使用四个 Gate

每个 Gate 独立承担：

```text
WSS连接表
Session Fence
协议解码与限流
业务RPC并发槽
每连接有界Writer队列
Snapshot/FarmChange下行
慢连接隔离
```

单 Gate 过载时，连接、Join、下行与 Redis Runtime 查询会在同一进程排队。增加 Gate 能按连接和玩家自然分片，不需要迁移 PlayerActor/FarmActor。

但 Gate 扩容必须同步更新：

```text
CLB / Headless Service
稳定 gate_id
SessionRoute
FarmSvr GATEWAY_DOWNLINK_ROUTES
Gate Control Targets
监控聚合
```

只增加 StatefulSet 副本而不增加 FarmSvr→Gate 路由，会出现 Join 已接受但 Snapshot 找不到目标 Gate 的假扩容。

## 8. 3000 万 DAU 如何严谨表达

以下是容量模型，不是实测：

```text
DAU:                 30,000,000
每人每天登录:       5.5 次
平均每次在线:       300 秒
峰值系数:           3x
在线时每 5 秒一操作
```

推导：

```text
日 Session:          165,000,000
平均登录 QPS:        1,910/s
3x 峰值登录 QPS:     5,729/s
3x 峰值在线:         1,718,750
3x 峰值业务动作:     343,750/s
```

若保守采用当前单 Cell 的 `20K 在线` 设计点：

```text
在线容量需要约 86 个活跃 Cell
加入 30% 余量后约 112 个 Cell
再考虑 N+1、机房故障与发布，规划约 120～125 个同等级 Cell
```

可以写：

> 建立 3000 万 DAU 容量模型，并完成单 Cell 20K 在线经营、9.7K action/s 和 4 Gate 5.6K/s 冷登录实测，为多 Cell 规划提供基线。

不能写：

> 系统已经实测承载 3000 万 DAU。

## 9. 后续必须补的测试

1. 修复 FarmSvr→Gate `deliver_batch unavailable`，重跑 25K；
2. `3 Player + 3 Farm` 或 `4 + 4` 下重跑 25K/30K，验证业务服务水平扩容；
3. 2～4 小时 Soak，观察 Actor 回收、Dirty RPO、连接和 Redis 内存；
4. 200 人同场热点、Snapshot 放大和慢客户端隔离；
5. 30%～100% Reconnect Storm；
6. 高负载下 Shard 扩缩容、滚动更新和单 Pod Kill；
7. Redis Cluster 节点故障、MOVED/ASK 和网络抖动；
8. 多 Cell 路由、跨 Cell 好友访问和故障隔离。

## 10. 原始证据

```text
/data/home/developer_026/load-results/actor-hotpath-20260826/perf-r18-88ca-j20k-clean
/data/home/developer_026/load-results/actor-hotpath-20260826/gate-scale-88ca/gate88ca-g1-40k-r6k
/data/home/developer_026/load-results/actor-hotpath-20260826/gate-scale-88ca/gate88ca-g2-40k-r6k
/data/home/developer_026/load-results/actor-hotpath-20260826/gate-scale-88ca/gate88ca-g4-40k-r6k
/data/home/developer_026/load-results/actor-hotpath-20260826/journey-ladder-88ca/journey88ca-25k-r25
/data/home/developer_026/load-results/actor-hotpath-20260826/journey-ladder-88ca/journey88ca-30k-r3k
```

清理说明：`cap20260826perf12`、`cap20260826g2`、`cap20260826g4`、`cap20260826journey`、`cap20260826j25` 和 `cap20260826j25b` 的测试键已精确删除，原始报告仍在上述文件目录中。删除的测试 Redis 数据不可恢复。
