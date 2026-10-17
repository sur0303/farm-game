# 76 · 好友互动机会 Summary 与 Authority Header A/B 压测方案

版本：`v1.1-preliminary-benchmark-ready`

状态：`STABLE_INPUT / PRELIMINARY_BENCH_READY / CAPACITY_PENDING`

日期：2026-08-08

定位：为第 75 号方案提供可重复、可量化的架构决策证据。本文不预设 Summary 一定更快，也不预设直接读
Authority 一定足够；最终选择由同一环境、同一数据集、同一请求轨迹下的对照结果决定。

## 1. 要回答的问题

1. 不建设 Summary 时，批读 FARM Authority Header/Facts 会不会明显抬高 Farm 写入 P99？
2. Summary 能把好友列表与连续导航延迟降低多少？
3. Summary 刷新造成多少额外 Redis 写、网络字节、Worker backlog 与投影延迟？
4. Summary 过期时产生多少假阳性、假阴性和回源；能否继续保证最终按钮准确？
5. 在平均 100、上限 300 好友和热点农场模型下，哪种方案总体成本更低、故障语义更简单？

## 2. 三组对照

### A：Authority Header/Facts，无 Summary

```text
FriendIndex分页
→ BatchGet model:opportunity_header
→ trusted_now_ms计算粗筛候选
→ 候选BatchGet model:opportunity_facts
→ BatchEvaluate(viewer)
→ 返回精确action_mask或UNKNOWN
```

写入成本：Farm Authority Commit 同时提交 `model:record/header/facts`，没有额外投影刷新。

### B：Projection Summary + Authority Facts

```text
Farm Authority Commit(record/header/facts + 可恢复刷新工作)
→ Dispatcher/Worker更新Projection Summary

FriendIndex分页
→ BatchGet Summary
→ 粗筛候选
→ 候选BatchGet Authority Facts
→ BatchEvaluate(viewer)
→ 返回精确action_mask或UNKNOWN
```

Summary 只能替代粗筛 Header 读取，不能替代 Facts 精查、ActionPermit 或 FarmActor 最终裁决。

### C：业务写隔离基线

只运行 Plant/Care/Theft/Harvest 与必要 Actor/Redis 流量，不运行好友机会查询，也不刷新 Summary。C 用来测量
A/B 对 Farm Authority Commit P95/P99 的增量影响。

## 3. 公平性约束

- A、B、C 使用相同二进制版本、Redis 规格、Pod 数、连接池、数据集和随机种子；
- A、B 使用完全相同的好友关系、Farm 状态、viewer TheftAttemptReceipt 和请求时间线；
- B 必须计入 Summary 刷新写入与恢复成本，不能只测查询缓存命中；
- A/B 的最终 `action_mask` 都必须由同一个 `BatchEvaluate` 实现产生；
- 同一轮测试先预热，再采样；轮换 A/B/C 运行顺序，避免固定顺序带来的缓存与机器温度偏差；
- 每次结果保存配置、Git commit、容器镜像、Redis 拓扑和原始指标，不只保存截图或平均值。

## 4. 数据与流量模型

### 4.1 玩家与好友

```text
好友数：20 / 100 / 300
好友页：每页20人
连续导航扫描批次：32 / 64
候选窗口：最多8人
每人每日登录：5～6次
单次在线：不超过5分钟
```

### 4.2 农场

```text
每人初始5块地，最多24块
机会密度：0% / 5% / 20% / 60%
护理：每茬最多一个WATER和一个PEST Incident
偷菜：每茬最多十个合法AttemptReceipt
```

### 4.3 场景轨迹

至少包含：

1. 登录后首次加载好友页；
2. 点击机会弹窗后连续访问、返回自己的农场、获取下一候选；
3. 好友完成护理导致旧候选失效；
4. 作物仅因墙钟经过而成熟，不发生到点业务写；
5. 300 好友的玩家批量登录；
6. 200 人进入同一热点农场，竞争同一个护理 Incident；
7. Summary Consumer 暂停、恢复和积压；
8. Redis 节点延迟、部分超时和连接池等待。

### 4.4 3000 万 DAU 换算

`3000万 × 5～6次/天` 对应全天平均约 `1736～2083` 次登录/秒。压测不能把平均值直接当峰值，至少运行：

```text
1x：平均模型
5x：区域性日常峰值
10x：活动/集中登录压力
```

真实生产峰谷系数尚未获得，因此这些倍率是验证档位，不是容量承诺。

## 5. 必须采集的指标

### 5.1 用户请求

- `GET_FRIEND_LIST` P50/P95/P99、成功率、UNKNOWN 比例；
- `GET_NEXT_FRIEND_OPPORTUNITY` P50/P95/P99、扫描批次数、补候选次数；
- 每次查询读取的 Header/Summary/Facts item 与 byte；
- 粗筛候选数、精查候选数、最终可操作数；
- 假阳性率、假阴性率、超时率。

### 5.2 Farm 权威写

- Plant/Care/Theft/Harvest Commit P50/P95/P99；
- Redis 单 Slot CPU、网络、命令数、连接池等待；
- Actor Mailbox 排队时间与 Turn 时间；
- 与 C 组相比，A/B 对 Farm 写 P99 的增幅。

### 5.3 Summary 特有成本

- 每次 Farm 变化产生的 Summary 写次数和字节；
- Dispatch/Worker backlog、重试和最大延迟；
- `source_farm_version - summary_source_version` 的版本差；
- Summary 缺失、陈旧和回源比例；
- Projection Redis 故障期间的降级行为。

## 6. 正确性门槛

性能比较之前必须先通过：

- A/B 最终精确结果一致；
- 墙钟到期无需 Scheduler 写状态，也不会漏掉成熟/护理候选；
- Summary 陈旧只允许假阳性，不允许导致最终假按钮或越权；
- Header/Summary/Facts 超时返回 UNKNOWN，不伪装为 NONE；
- 好友删除/拉黑后，页面候选验证和 ActionPermit 都会拒绝；
- 同一护理 Incident 200 并发请求只有一个成功解决者和一个 CareRewardOrder。

## 7. 决策规则

P0 默认保留 A。只有同时满足以下条件，才建议启用 B：

1. A 在目标流量下使 Farm Commit P99 相对 C 持续增加超过 10%，或好友查询无法满足冻结 SLO；
2. B 对查询或权威写竞争有稳定、可重复的显著改善；
3. B 的刷新写放大、积压、恢复与运维成本处于可接受范围；
4. B 关闭或故障时仍可降级回 A，不影响业务正确性；
5. 评审确认收益足以覆盖新增 Projection 链路复杂度。

5%～10% 作为观察区间，不凭单轮波动做架构切换。若 A 已满足 SLO 且对写 P99 影响小，选择 A，因为它的数据链更短、
故障模式更少。若 B 明显胜出，则通过 `OpportunityCandidateProvider` Feature Toggle 灰度启用。

## 8. 实施顺序

```text
第75号 I1：record/header/facts 原子提交
→ I2：AuthorityHeaderProvider + BatchEvaluate
→ 固定请求轨迹和指标埋点
→ 运行C组
→ 运行A组
→ 实现仅用于对照的ProjectionSummaryProvider与可靠刷新
→ 运行B组及故障注入
→ 生成原始数据、图表和决策报告
```

在 I1/I2 完成前，不允许为了提前压测而实现一套临时 Summary 业务语义，否则 A/B 比较会因两套粗筛定义不同而失真。

### 8.1 当前可执行检查点

当前隔离分支已经提供：

```text
loadtest.DefaultFriendOpportunityABProfile
internal/farm/readpath/ab_benchmark_test.go
scripts/run-friend-opportunity-ab
```

本地预基准使用相同的 20/100/300 好友、0/5/20/60% 机会密度、同一 Header/Facts 编解码和同一个
`BatchEvaluate`。它比较：

```text
A：分批 HMGET Authority Header → Top 8 精查 Header/Facts
B1：按64个一批 Pipeline GET Projection Summary → Top 8 精查同一 Authority Header/Facts
B2：跨Key MGET Summary → Top 8 精查（仅作为公司Redis/代理支持跨Slot MGET时的乐观下界）
```

它还单独测量一次 Summary Projection 刷新写，避免只展示 B 的读收益、不展示写成本。运行：

```bash
FARM_AB_BENCH_COUNT=5 scripts/run-friend-opportunity-ab
```

结果写入 `artifacts/friend-opportunity-ab/preliminary-go-benchmark.txt`。该文件只属于 `UNIT/PRELIMINARY`
证据：miniredis 与单机 Go Benchmark 不能模拟真实 Cluster Slot、网络、连接池争用、Farm CAS Lua 或 Worker
积压，禁止据此申报 `CAPACITY_PASS`。

### 8.2 2026-08-08 首轮预实验

环境：Linux / AMD EPYC 7K62 / miniredis / `count=1`。300 好友代表性结果：

上一轮 B 使用单机 Redis 的跨 Key `MGET`，得到约 `0.3～0.6 ms`、`1～9 commands` 的乐观结果；该结果
不能直接用于生产判断，因为原生 Redis Cluster 的不同 Summary Key 通常位于不同 Slot，跨 Slot MGET 可能失败。
正式预基准必须同时输出：

| 路径 | 生产含义 | 是否可直接作为默认候选 |
|---|---|---|
| A | Authority Hash 的 HMGET Pipeline | 是 |
| B1 | Summary String Key 的 GET Pipeline | 是 |
| B2 | 跨 Key MGET | 否，必须先通过平台能力探针 |

B1 和 A 的逻辑命令数通常接近；B1 可能因 Value 更小、解析更轻、Projection Redis 与 Authority Redis 隔离而获益，
但不应宣称“从300条命令降到1条”。只有平台证明跨 Slot MGET 代理扇出语义后，B2 才能作为该平台的有效优化。
单次 Summary Projection `SET` 的本地量级约 `0.049 ms / 1 command`，但尚未包含来源 Order、
Dispatcher/Worker 恢复和投影滞后，因此不能据此决定启用 B。

修正后的代表性预实验（300 好友、20% 机会密度、Top 8）：

| 路径 | 平均延迟 | Redis命令/往返 | Go堆分配字节/op | allocs/op |
|---|---:|---:|---:|---:|
| A Authority | 5.127 ms | 308 / 6 | 521,011 | 11,947 |
| B1 Portable Pipeline | 4.292 ms | 308 / 6 | 171,801 | 5,320 |
| B2 MGET乐观下界 | 0.515 ms | 9 / 2 | 109,527 | 2,855 |

因此当前能支持的谨慎判断是：B1 在本地预实验中以相同命令/往返换取了约 16% 的读取延迟下降和更少的
客户端分配；它是否抵得过持续 Summary 刷新写、可靠 Order、Worker 和投影延迟，仍未知。B2 的巨大优势不能
外推到生产，除非平台探针明确证明跨 Slot MGET 可用并给出其内部扇出成本。

当前结论只能是：B 值得保留为隔离 Authority 读流量和缩小返回数据的对照；A 是否会影响真实 Farm 写 P99，
B1 是否有净收益、B2 是否受平台支持，以及 B 的完整写放大是否值得，仍必须由下述服务级 A/B/C 验证决定。
P0 继续默认 A。

### 8.3 复用现有压测接缝

Linux 仓库已经存在：

```text
loadtest/                    // 类型化preset与证据规则
cmd/ws-stress                // WSS登录、Join、Plant、Hot、Reconnect
cmd/lease-stress             // 199 viewer订阅与续租
scripts/run-local-capacity-stage
scripts/run-theft-200-wss-hot-farm
Prometheus /metrics
```

本方案不得另建第二套互不兼容的 Harness。推荐扩展为：

```text
loadtest.FriendOpportunityABPreset
cmd/ws-stress -scenario friend-opportunity
scripts/run-friend-opportunity-ab
```

新场景只负责生成固定好友图、Farm机会数据和请求轨迹，并驱动现有服务；指标继续从各进程 `/metrics`、
Redis INFO/平台监控和 JSON 原始结果采集。原 `plant/hot` 场景同时作为 Farm 写与 200 人热点背景流量，
确保 A/B 观察到的是查询和写竞争，而不是纯内存微基准。

## 9. 交付物

1. 可固定 seed 的数据生成器与请求轨迹；
2. A/B/C Provider 配置与一键压测命令；
3. Prometheus/日志字段清单；
4. 原始指标文件；
5. 延迟、Redis 命令/字节、Farm 写影响和 Summary 延迟图表；
6. `keep-authority-header` 或 `enable-summary-projection` 的最终决策报告。
