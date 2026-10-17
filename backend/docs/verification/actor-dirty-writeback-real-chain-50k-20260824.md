# Actor Dirty Writeback：5 万 Bot 真实链路压测报告

日期：`2026-08-24`

代码分支：`codex/actor-dirty-writeback-loadfix-r1`

代码提交：`212acd93`

测试环境：`uc-test-plus / developer_026-qqfarm`

> 本报告是 `dirty_best_effort` 的真实 H5 等价链路容量验证，不改变 `durable_cas` 仍为默认生产模式的结论。三轮 5 万 Bot 测试都没有达到“完整业务严格通过”，不得据此宣称当前 2+2+2 部署已经承载 5 万持续经营用户。

## 1. 最终结论

当前部署已经证明：

- 50,000 个 Bot 均可完成 HTTP 登录、WSS、ClientHello、Join、Snapshot，并保持 50,000 条在线连接；
- 1024 个 PLAYER Shard 和 1024 个 FARM Shard 在 2 个 PlayerSvr、2 个 FarmSvr 间完成分配；
- 路由修复后未再出现上一轮大规模 stale route/lease deadline projection skew；
- Dirty 热路径能够显著减少每 Turn 同步 Redis CAS，并在负载结束后把队列排空；
- FarmSvr 到 Gate 的下行 Lane 增加并行度后，Snapshot/FarmChange `queue-full` 从峰值约 12,134 次/秒降为 0。

当前部署尚未证明：

- 50,000 人以每人约 5 秒一次经营操作持续运行时零拒绝、零未决；
- `oldest_dirty_age <= 10s`。最佳业务吞吐一轮的最老 Dirty 状态达到约 184.7 秒；
- 2 Gate + 2 PlayerSvr + 2 FarmSvr 可稳定提供理论约 10,000 命令/秒；
- 将金币、库存和全部跨 Actor 行为统一降级为 Dirty 后仍具备生产可接受的故障语义。

最优一轮达到：

- 50,000/50,000 Ready；
- 47,162 个 Bot 全程无失败，2,838 个 Bot 至少遇到一次失败；
- 1,938,368 次动作，约 6,215 动作/秒；
- FarmSvr 下行 Lane 零 queue-full；
- 但仍有 32,393 个业务拒绝、51,240 个 Processing、11,783 个会话结束时未决操作；
- Redis 依赖错误峰值约 7.39%，P95 RTT 峰值约 142.6 ms；
- P95/P99 最差 Driver 分别约 2.20 s / 3.49 s。

所以准确结论是：**5 万在线与 Join 已通过；5 万持续经营尚未通过。**

## 2. 真实链路与负载

每个 Bot 走真实链路：

```text
HTTP 登录
→ WSS
→ ClientHello / ServerHello
→ Join 自己农场
→ ACCEPTED / Snapshot / Catch-up / Ready
→ 解锁地块预热
→ Buy / Plant / Care / Harvest / Sell 持续经营
→ FarmChange 下行更新本地视图
```

统一负载参数：

| 参数 | 值 |
|---|---:|
| Bot 总数 | 50,000 |
| Driver Job | 50 |
| 每 Driver Bot | 1,000 |
| 聚合登录到达速率 | 1,000/s |
| 业务思考时间 | 5s |
| 业务窗口 | 3min |
| 登录后业务预热 | 2min |
| Action warmup | 20s |
| Player/Farm Shard | 各 1024 |
| Gate / PlayerSvr / FarmSvr / Worker | 2 / 2 / 2 / 2 |
| Redis | 公司 Redis Cluster，3 主 3 从 |

该负载并不等价于 5 万普通农场玩家静默在线。50,000 个 Bot 每约 5 秒持续操作，理论目标接近 10,000 条经营命令/秒，属于有意放大的高压场景。

测试入口：

```text
/tmp/qqfarm-run-online-multishard-capture-safe-20260824.sh
```

运行时镜像：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@
sha256:9679b9f4cec7c09d7297dc0f5ba86bf6b0ac82e049bc8b31b762477fe44484e2
```

## 3. 三轮对照

### 3.1 参数差异

| 项目 | R1 基线 | R2 Gate/Dirty 调优 | R3 再调优下行 Lane |
|---|---:|---:|---:|
| 每 Gate backend max in-flight | 1,536 | 4,096 | 4,096 |
| Gate admission wait | 500ms | 1s | 1s |
| 每 Actor 进程 Dirty Worker | 4 | 32 | 32 |
| Dirty jitter | 600ms | 3s | 3s |
| Dirty flush timeout | 2s | 5s | 5s |
| 每 Gate 下行子 Lane Worker | 4 | 4 | 8 |
| 每子 Lane 队列 | 256 | 256 | 1,024 |
| 下行 Batch | 32 | 32 | 64 |
| 每 FarmSvr 下行 RPC 并发 | 16 | 16 | 32 |

### 3.2 核心结果

| 指标 | R1 基线 | R2 Gate/Dirty | R3 下行 Lane | 判断 |
|---|---:|---:|---:|---|
| Ready | 50,000 | 50,000 | 50,000 | 接入链稳定 |
| Prometheus WS 峰值 | 50,000 | 49,998 | 49,999 | 采样误差内达到 5 万 |
| 无失败 Bot | 21,000 | 26,382 | 47,162 | R3 显著改善 |
| 失败 Bot | 29,000 | 23,618 | 2,838 | 仍未清零 |
| 动作数 | 1,080,012 | 1,312,395 | 1,938,368 | 吞吐释放 |
| 动作/秒 | 3,539 | 4,293 | 6,215 | R1→R3 约 +75.6% |
| Rejected | 50,524 | 48,297 | 32,393 | 仍不满足严格验收 |
| Processing | 0 | 32 | 51,240 | 压力转移到 RPC/未决解析 |
| 会话结束 Pending | 0 | 0 | 11,783 | 必须恢复或限流 |
| Projection Pending | 18,119 | 10,012 | 12,330 | 仍有视图追赶 |
| 最差 P95 | 800ms | 1,179ms | 2,199ms | 吞吐提升但尾延迟恶化 |
| 最差 P99 | 1,391ms | 2,197ms | 3,491ms | 未达到体验目标 |

### 3.3 Gate 与下行

| 指标 | R1 | R2 | R3 |
|---|---:|---:|---:|
| Gate backend in-flight 峰值 | 3,072 | 8,042 | 8,192 |
| BACKEND_SLOTS_FULL 峰值速率 | 723.6/s | 1.66/s | 333.7/s |
| Farm 下行队列峰值 / 容量 | 未单独采集 | 4,096 / 4,096 | 11,689 / 32,768 |
| Farm 下行 queue-full 峰值 | 未单独采集 | 12,134/s | 0 |
| Gate writer 队列峰值 | 未单独采集 | 56 | 159 |
| Gate writer 最老帧 | 未单独采集 | 47ms | 423ms |

R2 证明仅扩大 Gate backend pool 不够：FarmSvr 下行 Lane 打满，Snapshot/FarmChange 被拒，客户端继续使用旧 PlotVersion，继而出现大量 `ERR_VERSION_CONFLICT` 和 `ERR_NOT_IN_FARM`。

R3 消除了 Farm 下行 queue-full，业务吞吐和成功 Bot 显著提升；随后 Gate backend 再次达到 8,192 上限，并出现大量 RPC timeout/unknown outcome，说明瓶颈已从下行队列继续向 Gate、Holder 和 Redis 推进。

### 3.4 Dirty Writeback

| 指标 | R1 | R2 | R3 |
|---|---:|---:|---:|
| Dirty 唯一 Actor 队列峰值 | 88,151 | 88,256 | 98,255 |
| Dirty 最老年龄峰值 | 274.9s | 39.4s | 184.7s |
| Flush 成功峰值速率 | 8,227/s | 8,895/s | 9,766/s |
| Flush 失败峰值速率 | 7/s | 0 | 0 |
| 负载结束后残留队列 | 7 | 0 | 0 |

R2 的 32 Worker 明显改善了写回恢复速度；R3 释放更多前台动作后，Dirty 生产速度重新超过 Flush 消费速度。队列最终可以排空，但负载中 184.7 秒的最老脏龄意味着 Pod 硬崩溃可能回退数分钟状态，不能按“10 秒 RPO”上线。

`farm_actor_dirty_writeback_inflight` 当前实现还包含已经从时间堆取出、等待 Worker 的条目，并不等价于真实 Redis I/O 并发；监控帮助文案后续应修正。

### 3.5 Redis

| 指标 | R1 | R2 | R3 |
|---|---:|---:|---:|
| Redis Server ops/s 峰值 | 78,363 | 92,553 | 89,923 |
| 客户端命令速率峰值 | 36,808/s | 41,385/s | 22,591/s |
| Redis P95 RTT 峰值 | 89ms | 113ms | 143ms |
| 依赖错误百分比峰值 | 0.43% | 2.64% | 7.39% |
| Pool wait 平均时长峰值 | 83ms | 88ms | 326ms |
| Pool timeout / blocked / evicted | 0 | 0 | 0 |

R3 的客户端命令速率因 Dirty 合并而下降，但等待和尾延迟反而恶化，说明不能只看 Redis 命令数。需要按 Pod、逻辑域、Redis 节点拆分连接池等待和脚本时延，并验证 Holder 扩容后的真实改善。

## 4. 主要错误归因

### 4.1 R1

- `BACKEND_SLOTS_FULL` 约 15,802 次；
- `PLANT ERR_SERVER_BUSY` 约 7,034；
- `BUY ERR_SERVER_BUSY` 约 5,454；
- 随后的 `ERR_NOT_IN_FARM` 和版本冲突是容量拒绝后的连锁反应。

### 4.2 R2

- Gate 槽位拒绝已接近消失；
- Farm 下行 queue-full 峰值约 12,134/s；
- `HARVEST ERR_VERSION_CONFLICT` 约 28,758；
- `HARVEST/PLANT/CARE ERR_NOT_IN_FARM` 合计约 14,626。

### 4.3 R3

- Farm 下行 queue-full 为 0；
- `HARVEST ERR_VERSION_CONFLICT` 降至约 5,509；
- `ERR_NOT_IN_FARM` 合计约 4,908；
- `PLANT/HARVEST/CARE PROCESSING ERR_TIMEOUT` 约 61,265；
- Gate backend `RPC_UNAVAILABLE` 约 61,364；
- `BACKEND_SLOTS_FULL` 约 18,693；
- 少量 `ERR_STORE_UNAVAILABLE` 仍存在。

R3 的两个 `route_store_error_lines` 是采集正则误命中正常的 Participant begin/end 日志，不是真实 route/store 错误；压测脚本应收窄该正则。

## 5. 对 Dirty 方案的判断

Dirty 模式确实提升了单 Actor 热路径和全链路吞吐，但“Worker 不再需要处理异常”这个推论不成立：

- 普通内存 Turn 可以不等待 Redis；
- 进程仍需要 Dirty Scheduler 负责合并、重试、Drain Flush 和 writer fence；
- Gate/FarmChange、跨 Actor RPC、OperationResult 恢复和好友提醒仍有异步失败；
- Redis 变慢时必须保护 Dirty 队列、内存和最大 RPO；
- 直接删除恢复 Worker 只会把显式未决状态变成不可解释的数据丢失。

更合理的生产演进是分级：

| 数据 | 建议 |
|---|---|
| 普通农场视觉状态、低价值计数、可重建投影 | 可评估 Dirty |
| 普通金币/普通种子 | 只有产品接受明确 RPO 后再灰度 |
| 付费资产、唯一奖励、邮件附件、任务领奖 | 保留可靠提交 |
| 跨玩家偷菜结算、跨 Actor 奖励 | 保留 Receipt/可查询结果；不能只靠内存 |

## 6. 下一轮容量建议

不建议继续单纯扩大有界队列。下一轮应做结构化扩容：

1. 将 Gate、PlayerSvr、FarmSvr 各从 2 扩到 4，保持 1024 VirtualShard，验证自动路由与最小迁移；
2. 让 Gate backend 容量由已验证的 Holder/下行能力推导，而不是固定设置成更大的数字；
3. 保留 R3 下行参数作为候选：`lane_workers=8`、`batch=64`、`queue=1024`、`max_concurrent=32`，同时监控队列字节数；
4. 给 Dirty Scheduler 增加按 Holder/Shard 的公平调度，避免一个进程中的冷启动大批 Actor 长时间饿死；
5. 将 `oldest_dirty_age` 作为硬验收门槛；超过 RPO 时停止接受低优先级写、扩大 Flush 资源或触发保护；
6. 按 Player/Farm/Runtime/Projection Redis 域分别观测 pool wait、RTT 和脚本耗时；
7. Bot 对 `PROCESSING` 必须在测试窗口内走真实 `GET_OPERATION_RESULT` 恢复，最终 Pending 必须归零；
8. 在同一 5 万负载下执行 Pod 滚动更新和单 Pod Kill，验证 Dirty RPO、Fresh Join、Redirect 与自动 Shard 接管。

正式容量通过门槛至少应包括：

```text
ready = 50,000
failed = 0
backend slots full = 0（稳态）
downlink queue full = 0
pending / processing 最终归零
oldest_dirty_age <= 产品确认的RPO
Redis错误不连续超过阈值
P95/P99满足体验目标
滚动更新/单Pod故障期间无双写和不可恢复结果
```

## 7. 证据位置

远端原始证据：

```text
/data/home/developer_026/load-results/dirty-writeback-20260824/dirty/routefix-20k/run
/data/home/developer_026/load-results/dirty-writeback-20260824/dirty/routefix-50k/run
/data/home/developer_026/load-results/dirty-writeback-20260824/dirty/tuned-50k/run
/data/home/developer_026/load-results/dirty-writeback-20260824/dirty/downlink-tuned-50k/run
```

每个 5 万目录包含：

- 50 份 Driver JSON、stderr 和退出码；
- 测试前后 Pod 状态；
- Redis INFO 时间序列；
- WebSocket、Join、路由、Redis、Dirty 和 Downlink Prometheus 时间序列；
- 按命令、状态和错误码汇总的增量；
- 运行参数与 UTC 起止时间。

## 8. 分支状态

本轮没有合入 `Integration`。R2/R3 调优通过临时重建脚本注入，只用于 A/B 压测，没有把尚未严格通过的参数写进正式部署清单。
