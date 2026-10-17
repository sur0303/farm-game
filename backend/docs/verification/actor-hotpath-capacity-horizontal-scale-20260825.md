# Actor 热路径容量、冷登录与水平扩容实测

日期：`2026-08-25`

分支：`codex/actor-hotpath-bot-theft-r1`

状态：真实公司 Redis、真实 Kubernetes 四服务链路实测；证据已保存，**未合入 Integration**。

## 1. 结论先行

本轮已经回答三个问题。

1. 当前单 Cell 的完整经营链路在什么位置开始排队；
2. 冷登录链路扩 Gate 后是否真的获得吞吐提升；
3. Player/Farm Shard 能否自动迁移到新增 Pod。

当前结论如下。

| 项目 | 实测结论 |
|---|---|
| 3 Player + 3 Farm 完整经营链路 | `30,000` 在线时约 `7,247 action/s`，业务 p95 `86.5ms`，30,000/30,000 Bot 完成 |
| 单 Cell 极限档 | `40,000` 在线时约 `9,494 action/s`，但业务 p95 已升到 `289ms`，登录 p95 `1.24s`，不宜作为生产设计点 |
| 2 Gate 冷登录健康点 | `2,840` 次完整登录 + WSS + Join + Snapshot/s，p95 均在 `20ms` 内 |
| 4 Gate 冷登录健康候选 | `4,205/s`，登录 p95 `139ms`、Join p95 `115ms`，40,000/40,000 成功 |
| 4 Gate 冷登录极限零失败点 | `5,408/s`，但登录/Join p95 已升到 `523/452ms` |
| Gate 水平扩容 | 四个 Gate 累计连接数 `19,983 / 19,994 / 20,024 / 19,999`，流量分布均匀；相对 2 Gate 健康吞吐约 `1.9x` |
| Farm 2→3 自动扩容 | RouteController 自动发现 Pod、生成计划并迁移 `341` 个 Shard，最终分布 `341/342/341` |
| Player 2→3 自动扩容 | 最终分布同样为 `341/342/341`，无 pending/degraded Shard |

因此，当前架构已经有较强的容量与水平扩展证据，但还不能直接宣称“生产认证 3000 万 DAU”。最重要的缺口是：

- 高负载 Shard 迁移尚未完全对玩家透明；
- Opportunity Worker 在同步机会洪峰下存在分钟级提醒积压；
- 还缺最终组合拓扑的长稳、热点农场、故障和多 Cell 验证。

## 2. 环境与不可变输入

```text
Kubernetes context: uc-test-plus
Namespace:          developer_026-qqfarm
Redis:              3 Master Redis Cluster（本环境未配置 Replica）
VirtualShard:       PLAYER 1024 + FARM 1024
Holder resource:    4 CPU / 8 GiB
Bot image:          sha256:8986134cf485f2dc8578645a41257403c966e5f5c582f73f4a8ee11550b91f30
Route runtime:       sha256:1df97f4668a076dc5f2025ef160d7fa9957857ff61db4795e4b4021d4b229caa
```

容量测试使用独立 Redis Zone，未执行全局 `FLUSHALL`：

```text
完整经营与迁移：cap20260825r2
4 Gate 冷登录：cap20260825r3
```

本轮新增两个用于可重复验证的修复：

- `74130bfc`：ScalePlan 子计划复用已经验证的 Holder 视图，避免每个 Shard 子计划再次独立刷新 Kubernetes/HolderControl；
- `4d4c688b`：Reset 支持通过新 Redis Zone 隔离容量测试，不再要求清空共享 Redis Cluster。

## 3. 场景定义

### 3.1 冷登录

每名 Bot 走真实链路：

```text
HTTP Login
→ ConnectionTicket
→ WSS
→ ClientHello / ServerHello
→ Join 自己农场
→ ACCEPTED
→ Snapshot / Catch-up
→ SubscriptionReady
```

这里统计的是“完整冷登录并进入农场”的吞吐，不是单独 HTTP API QPS。

### 3.2 完整经营 Journey

每名 Bot 登录并保持在线，循环执行：

```text
Buy
→ Plant
→ Care
→ Harvest
→ Sell
```

请求经过 Gate、Actor 路由、PlayerActor/FarmActor、内存 Dirty 状态和 Redis 延迟写回。`actions/s` 是 Bot 观测到的业务动作吞吐；Prometheus 的 command result 峰值还包含 Bootstrap、Join、重试等协议命令，二者不能混为一个指标。

### 3.3 好友偷菜

好友环 Fixture 与真实 Theft 路径另行验证。当前已有 `5,000` Bot、约 `995 theft action/s` 的记录，`4,998/5,000` 成功，p95 `10.7ms`。这证明好友关系、Join 好友农场与 Theft 主链可运行，但尚未达到与 Journey 相同规模，不能把它当作偷菜容量上限。

## 4. 完整经营容量

### 4.1 结果总表

| 拓扑 | 在线 Bot | action/s | 业务 p95 | 业务 p99 | 成功 Bot | 结论 |
|---|---:|---:|---:|---:|---:|---|
| 2 Player + 2 Farm | 20,000 | 4,659 | 28.4ms | 119.8ms | 20,000 | 质量较好 |
| 2 Player + 2 Farm | 25,000 | 5,492 | 84.9ms | 261.6ms | 24,997 | 已接近 Farm CPU 边界 |
| 2 Player + 3 Farm，扩容完成后 | 20,000 | 4,862 | 10.3ms | 21.7ms | 20,000 | 增加 Farm 后延迟显著下降 |
| 3 Player + 3 Farm | 30,000 | 7,247 | 86.5ms | 197.3ms | 30,000 | 高负载可完成点 |
| 3 Player + 3 Farm | 40,000 | 9,494 | 289.3ms | 495.8ms | 40,000 | 极限档，不是安全容量 |

`strict_pass=false` 不等于 Bot 最终失败。Journey 会统计中间命令的业务拒绝或可重试结果；30k/40k 的 Bot 最终均成功且无 timeout/pending。生产认证前仍应按错误码把预期业务拒绝和基础设施错误拆开，不能只看最终 Bot 成功数。

### 4.2 30k 资源峰值

| Pod | CPU 峰值 | 内存峰值 |
|---|---:|---:|
| farm-gate-0 / 1 | 1.57 / 1.64 核 | 1.68 / 1.69 GiB |
| player-svr-0 / 1 / 2 | 2.99 / 2.71 / 2.66 核 | 约 1.0 GiB |
| farm-svr-0 / 1 / 2 | 2.82 / 2.51 / 2.50 核 | 1.8～2.4 GiB |
| Redis 三主 | 0.83 / 1.01 / 0.89 核 | 约 5.0 GiB/节点 |

### 4.3 40k 资源峰值

| Pod | CPU 峰值 | 内存峰值 |
|---|---:|---:|
| farm-gate-0 / 1 | 1.70 / 1.74 核 | 约 2.22 GiB |
| player-svr-0 / 1 / 2 | 3.25 / 3.03 / 3.03 核 | 1.38～1.43 GiB |
| farm-svr-0 / 1 / 2 | 3.07 / 2.72 / 2.88 核 | 2.89～3.06 GiB |
| Redis 三主 | 0.86 / 1.03 / 0.86 核 | 约 6.0 GiB/节点 |

40k 时 CPU 尚未完全打满，但 Redis 工作负载 p95 接近 `97ms`，Player 已超过 3 核，Dirty Queue 峰值达到 `68,521`，端到端延迟明显恶化。它说明极限并非某个进程简单 100% CPU，而是 Redis 往返、Actor 激活、Dirty Writeback 和并发排队共同形成的系统边界。

### 4.4 推荐单 Cell 设计点

当前更稳妥的设计点是：

```text
20,000 稳态在线
约 4,800 business action/s
业务 p95 约 10～30ms
```

30k/7.2k 可以作为压力线，40k/9.5k 只能作为破坏性边界。后续优化 Worker、Redis 和错误分类后可以重新提升安全线。

## 5. 冷登录与 Gate 水平扩容

### 5.1 对照结果

| Gate 数 | 目标到达率 | 实际完整冷登录/s | Login p95 | Hello p95 | Join p95 | 结果 |
|---:|---:|---:|---:|---:|---:|---|
| 2 | 3,000/s | 2,840 | 15.5ms | 11.6ms | 19.3ms | 健康点 |
| 2 | 4,000/s | 3,224 | 167.5ms | 135.2ms | 1,921ms | 已出现严重 Join 排队 |
| 4 | 4,500/s | 4,205 | 139.4ms | 34.1ms | 115.4ms | 健康候选，40k/40k 成功 |
| 4 | 6,000/s | 5,408 | 523.3ms | 191.8ms | 452.2ms | 零失败极限档，延迟不健康 |

### 5.2 扩容不是“多起两个空 Pod”

Reset 会同时生成四个稳定 Gate 身份和四条 Downlink/Control 地址：

```text
farm-gate-0 ... farm-gate-3
```

FarmSvr 的 `GATEWAY_DOWNLINK_ROUTES` 包含全部四个 Gate。两轮共 80,000 次冷登录后的 Gate 累计连接为：

```text
farm-gate-0: 19,983
farm-gate-1: 19,994
farm-gate-2: 20,024
farm-gate-3: 19,999
```

分布误差小于 `0.2%`，证明负载均衡和四条 Snapshot/Downlink 路径均真实工作。4 Gate 相比 2 Gate 健康吞吐提升约 `1.48x`（4,205/2,840），极限零失败吞吐提升约 `1.68x`（5,408/3,224）；如果以两者各自健康/极限区间比较，整体提升接近但不等于理想线性 `2x`，剩余瓶颈已经移动到 Redis、Actor 激活与 Snapshot 链路。

## 6. Shard 自动扩容与迁移

### 6.1 Farm 2→3

在 20,000 Bot 持续经营时，将 `farm-svr` 从 2 个 Pod 扩到 3 个。RouteController 完成：

```text
Kubernetes 发现 farm-svr-2 Ready
→ HolderControl 验证 Runtime 身份
→ 自动生成 ScalePlan
→ 迁移 341 个 FARM Shard
→ writer_epoch / Lease Fence
→ 新 Holder 懒加载 Actor
```

最终：

```text
farm-svr-0 = 341 Shard
farm-svr-1 = 342 Shard
farm-svr-2 = 341 Shard
pending = 0
degraded = 0
ScalePlan = SUCCEEDED
```

扩容完成后，同样 20k Journey 的 Farm CPU 从扩容前约 `2.4/2.1` 核下降到约 `1.2/1.3/1.4` 核，业务 p95 降到 `10.3ms`。

### 6.2 迁移期间的真实缺口

迁移压力场中出现：

```text
452 Bot failed
79 projection_pending
ERR_STORE_UNAVAILABLE / FARM_PARTICIPANT_AUTHORITY_UNAVAILABLE
```

这不是双写或数据丢失，而是 Shard Drain/Fence 窗口被直接暴露给 Bot。当前客户端/FirstHop 尚未对所有这类结果完成“同 operation_id 有界重试 + Redirect/Rejoin 恢复”。所以现在能证明：

- 自动发现、计划、写权切换、均衡和迁移后服务都成立；
- 不能证明高负载迁移已经对玩家无感。

### 6.3 Player 2→3

Player Shard 也最终均衡为 `341/342/341`，无 pending/degraded。迁移期间独立 Deadline Projection 刷新与 Route Snapshot CAS 出现竞争，控制面迁移时间偏长。正确性保持，但需要让 Projection 刷新退避或避开路由提交窗口。

本轮没有重新执行缩容、Pod 突然宕机和 Redis 主从切换；这些能力不能引用 ScalePlan 单元测试代替真实负载验证。

## 7. Opportunity Worker 是当前明确短板

30k Bot 同步种植会在相近时间产生大量成熟/护理唤醒。当前两个 Worker 各负责 512 个 Shard，每轮最多处理 32 个到期引用，导致：

```text
每秒总处理上限约 64 个机会任务
DueBacklog 每轮持续满 32
最老到期任务延迟曾超过 330 秒
```

主 Journey 最终仍成功，因为成熟由 `trusted_now >= mature_at` 派生，Worker 只负责提醒；但在线好友弹窗会明显延迟。Redis 错误比例峰值也主要来自 Worker 的 authority/projection 域，Gate/Player/Farm 同期为 0。

生产前至少需要：

1. 到期时连续领取多个 Batch，而不是每秒只处理一个 32 条 Batch；
2. 同一 Shard 内有界并发，保持单 Schedule Lease/Fence；
3. FARM_WORKER 动态 Shard 归属，使 Worker Pod 可以水平扩展；
4. 监控 `due_depth`、`oldest_due_age`、claim/retry 和通知延迟；
5. 将同步种植洪峰纳入回归，而不是只测平均到期流量。

## 8. 3000 万 DAU 容量模型

DAU 不能直接推出 QPS，必须先冻结行为假设。本轮使用用户给出的近似模型：

```text
DAU                         30,000,000
每人每天登录               5.5 次
平均每次在线               300 秒
峰值系数                   3x
在线时平均每 5 秒一次操作
```

推导：

```text
日 Session                 165,000,000
平均登录 QPS               1,910/s
3x 峰值登录 QPS            5,729/s
平均在线                    572,917
3x 峰值在线                 1,718,750
峰值业务动作               343,750/s
```

如果采用当前保守单 Cell 设计点 `20k 在线 / 4.8k action/s`：

```text
在线容量需要：1,718,750 / 20,000 ≈ 86 个活跃 Cell
动作容量需要：343,750 / 4,800 ≈ 72 个活跃 Cell
```

在线容量是更严格约束。考虑机房故障、发布、N+1 和 20%～30% 预留，规划量级应在约 `100～125` 个同等 Cell，而不是试图让一个 Redis Cluster 和几台 Pod 承载全部 3000 万 DAU。

4 Gate 的极限零失败冷登录 `5,408/s` 仍略低于模型中的 `5,729/s` 峰值，而且延迟不健康。生产上应由多 Cell 分摊登录洪峰；若坚持单入口 Cell 承担该峰值，至少还要验证 6～8 Gate 与扩大的 Player/Farm/Redis 组合拓扑。

## 9. 还需要什么才能正式证明 3000 万 DAU

当前能够证明：

- 一个 Cell 能承载 20k 高质量在线经营，30k 可完成，40k 进入极限区；
- Gate 增加后真实连接和冷登录吞吐会上升；
- Player/Farm Pod 增加后 RouteController 会自动均衡 Shard；
- Actor 数据不需要随整个 Shard 一次性搬运，新 Holder 可按需懒加载。

仍需补齐以下生产证据：

1. **最终组合拓扑**：4～6 Gate + 3 Player + 3 Farm + 可扩 Worker 一起压，而不是分别验证；
2. **2～4 小时 Soak**：Dirty Queue、Redis 内存、Actor 回收、连接泄漏和 Scheduler 不持续增长；
3. **200 人同一农场热点**：同一 FarmActor Mailbox、广播和慢客户端隔离；
4. **Reconnect Storm**：Gate/网络抖动后 30%～100% 在线用户集中重连；
5. **负载中扩缩容**：迁移错误通过同 operation_id 重试被客户端吸收，用户失败率满足 SLO；
6. **Pod 故障接管**：Player/Farm 突然消失后的 Lease 到期、writer_epoch 换代和 Rejoin；
7. **Redis 故障**：主从切换、Cluster MOVED/ASK、网络抖动与恢复后的 UnknownOutcome；
8. **多 Cell 路由**：用户归属、好友跨 Cell 访问、容量均衡和单 Cell 故障隔离；
9. **带宽模型**：Snapshot/Delta/WSS 下行字节、CLB/TLS 和公网带宽；
10. **统一 SLO**：成功率、p95/p99、提醒延迟、迁移错误预算和 N+1 容量线。

完成这些后，才能把“按单 Cell 线性估算可达 3000 万 DAU”升级为“经过生产形态验证可承载 3000 万 DAU”。

## 10. 原始证据

远端证据根目录：

```text
/data/home/developer_026/load-results/capacity-hotpath-20260825
```

关键目录：

```text
caphp-cold-20k-r3k
caphp-cold-20k-r4k
caphp-journey-20k-r1
caphp-journey-25k-r1
caphp-r2-scale-20k-farm-2to3
caphp-r2-postscale-20k-farm3
caphp-r2-scale33-journey-30k
caphp-r2-scale33-journey-40k
caphp-r3-cold-4gate-40k-r45
caphp-r3-cold-4gate-40k-r6k
caphp-theft-5k-r4
```

每个目录包含：

```text
summary.json
shard-*-result.json
pod-top-samples.txt
Prometheus 查询结果
测试前后 Pod/StatefulSet 快照
```

## 11. 测试结束时环境

当前 `developer_026-qqfarm` 留在冷登录对照拓扑：

```text
4 Gate
2 Player
2 Farm
2 Worker
Redis Zone = cap20260825r3
```

所有 Load Job 已删除，Dirty Queue 与 Redis error 指标均已回到 0。该状态用于保留四 Gate 证据，不代表最终生产推荐拓扑。
