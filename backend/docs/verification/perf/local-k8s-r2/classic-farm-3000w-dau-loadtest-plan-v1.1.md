# 经典农场 3000 万 DAU 容量、动态路由与好友机会压测实施计划

**Local kind · 单 Pod 安全容量 · 200 人热点农场 · 动态迁移带载 · 故障恢复 · Soak**
> 文档定位  
> 用于动态路由合流后的本地 kind 部署、正确性资格、单 Pod 容量、200 人热点农场、动态迁移带载、故障恢复与 3000 万 DAU 容量推导。本文区分“已冻结合同”“当前运行事实”“本计划建议”和“待实测参数”，不把单机小规模压测冒充生产总容量。


| 版本 | v1.1 |
| --- | --- |
| 日期 | 2026-08-11 |
| 目标环境 | 单机 Linux + Docker + kind + 本地不可变镜像 + 隔离 Redis |
| 当前工程阶段 | Dynamic Local R2 重跑 → 好友机会合流 → G1/G2/G3 → 完整产品 → 压测 |
| 结论边界 | 可形成本地容量常数和 3000 万 DAU 推导；不声明正式云容量、物理机 HA 或已实测 3000 万在线 |


# 0. 文档控制、结论与使用方法
## 0.1 一页结论
本项目的压测目标不是“把所有接口压到最大 QPS”，而是通过真实施压获得可用于容量规划的安全常数，并验证热点、好友互动、故障和 Shard 迁移期间仍然保持业务正确。最终需要回答五个问题：
- 30,000,000 DAU 在候选流量模型下，会形成怎样的峰值在线、登录、Join、好友查询/Hint、业务写、广播和后台恢复负载；
- 单个 farm-gate、player-svr、farm-svr、farm-worker、route-controller 与 Redis 分别能安全承担多少；
- 300 好友、实时机会 Hint、1+199 热点农场和动态迁移会产生怎样的读写与下行放大；
- 系统先被 CPU、内存、网络、Redis、Actor Mailbox、下行队列还是 Workflow/Due backlog 限制；
- 根据单 Pod 安全容量、N+1 余量和热点分布，应该部署多少 Pod，扩容后是否接近线性。

> 核心方法  
> 先关闭当前 R1 部署失败并完成全新 R2；再把好友机会优化收敛进最终动态产品候选；随后依次完成 G1/G2/G3、完整功能资格、极限吞吐与安全容量。极限值用于找瓶颈，安全容量才用于计算副本数。

本版冻结两个工程决策：

1. **当前 R2 先跑，不在 R2 前插入好友分支。** R1 只能保留为失败证据；三个脚本修复必须提交、全量验证并使用全新的 RunID、Redis Namespace、TLS、Secret、Pod UID、Controller Boot 与 HolderBoot 重跑。
2. **好友机会优化必须进入正式压测候选，但不直接合 Integration。** 先取得 R2/G1 控制面基线，再从该 exact tip 建立 Backend/H5 产品合流候选，合入 Backend `3a48228c...` 与 H5 `787dc00`，补齐 V2 Header fixture、shadow mismatch 指标和多 Worker OwnedShards 动态验证；合流后重跑 G1，再以同一候选完成 G2/G3、完整产品和全部容量测试。

正式测试体系采用 Scenario Coordinator 统一编排：Go Protocol Bots 负责真实 HTTPS/WSS/Protobuf 规模负载，2～4 个 Playwright H5 页面负责真实体验见证，Fault Controller 注入故障，Authority Probe 读取权威状态，Client State Oracle 验证 Snapshot/Delta 收敛，Evidence Reporter 形成机器可判定结论。
依据说明：来源于《60 · 接入与会话测试、容量验证与演示验收冻结》的冻结测试体系；本计划在此基础上增加 Dynamic Local G123、好友机会、单 Pod 容量和多 Agent 实施细节。
## 0.2 Agent 能否并行
可以并行开发测试工具、流量模型、指标采集和场景 Oracle，但同一台机器、同一 kind 集群、同一 Redis 上的正式容量运行必须串行。原因是 CPU、内存、磁盘、网络、Redis 连接池和故障注入会互相污染，导致结果不可归因。
| 工作 | 可否并行 | 规则 |
| --- | --- | --- |
| 流量模型与容量公式 | 可以 | 只改文档/配置，不占用被测集群 |
| Protocol Bot / Harness 开发 | 可以 | 单一 Harness Writer；独立 worktree |
| Metrics / Evidence Collector | 可以 | 只读采集，不改生产状态机 |
| 场景 Fixture / Authority Oracle | 可以 | 独立命名空间与数据合同 |
| 同一集群正式压力运行 | 不可以 | 一次只允许一个 Run Owner |
| CPU 饱和/单 Pod 极限测试 | 不可以 | 必须独占宿主机主要资源 |
| 迁移、Crash、Worker Kill 故障运行 | 不可以 | 故障本身属于该次唯一场景 |
| 已完成 Evidence 分析 | 可以 | 不接触正在运行的被测环境 |


## 0.3 文档标签
| 标签 | 含义 |
| --- | --- |
| 【冻结合同】 | 现有设计/验收文档已经明确，Agent 不得自行改写。 |
| 【当前事实】 | 当前代码、G21/G22/RT2Q、本地 kind 或真实 Evidence 已经证明。 |
| 【本计划建议】 | 为了尽快形成可执行压测而给出的实施方案，可在 Evidence 后调整。 |
| 【待实测冻结】 | P99、队列水位、单 Pod 安全容量、最终 Pod 数等，必须通过压测产生。 |


## 0.4 目录
1. 目标、非目标与进入条件
2. 3000 万 DAU 容量模型
3. 环境、拓扑与可复现性
4. 测试方法与有效性规则
5. 指标、Trace 与 Evidence
6. 总体执行阶段
7. 关键场景详细计划
8. 单 Pod 安全容量测试
9. 扩展性、N+1 与 Pod 数推导
10. 瓶颈定位与优化决策
11. Agent 并行编排
12. Checkpoint、PASS/FAIL 与交付物
附录 A～D：流量 Profile、指标清单、运行模板、Agent 任务卡

## 0.5 当前实现基线与差距
### 0.5.1 Dynamic Local G123 当前事实

| 项 | 当前事实 | 对压测的影响 |
| --- | --- | --- |
| Frozen convergence input | `f834383a1979...` | 动态代码基线已冻结，不从旧 Integration 重建 |
| Dynamic Writer committed head | `f7cd1d32d028...` | R2 镜像必须绑定后续 closure commit 的 exact head |
| R1 | `APPLY_FAILED_EVIDENCE_RETAINED` | 永久不可复用；不能继续该 Run |
| R1 失败原因 | `claims:null` 解析错误 + initial Fetch 5s 导致 Holder 重启 | 属于部署编排缺口，不是 G1/G2/G3 通过证据 |
| 未提交 closure | 3 个 local-k8s 脚本 | 必须 Review、测试、SSH-signed commit 后才能发 R2 镜像 |
| Dynamic Profile | 4+4 Shard、2 Player、2 Farm、2 Worker、1 Gate、1 H5 | 适合迁移与产品回归，不用于 2048 Shard 规模证据 |
| G1/G2/G3 | NOT_RUN | 正式性能结论全部阻塞 |
| Capacity | NOT_CLAIMED | 目前只能开发工具与做部署资格测试 |

### 0.5.2 好友机会优化当前事实

| 组件 | 独立候选 | 已证明 | 尚需在动态环境证明 |
| --- | --- | --- | --- |
| Backend | `3a48228c63bf...` | Header narrow、Direct Hint、Fence、Worker 有界扫描 | 100% v2 fixture、2 Worker OwnedShards、迁移/Drain 后归属更新、混合负载 |
| H5 | `787dc00` | 300ms 合并、定向精查、CandidateWindow 更新 | 与最终动态 Backend/H5 exact tip 组合、Hint 风暴和丢 Hint 恢复 |
| 真实单机链路 | 公司 Redis + 300 好友 | Header 约 6.2ms、定向精查 9～11ms、弹窗 0.82～1.38s | 动态 4+4、多 Worker、G2/G3、容量和 Soak |

### 0.5.3 本版新增的 Go/No-Go

- R2 任一 Holder 在初始 Claim 前重启，整轮立即失格，使用全新 Identity 重跑；
- `ROUTE_CONTROLLER_LEASE_PROJECTION_MODE=disabled` 只能作为短时配置候选。正式 G1、30 分钟和 2 小时 Run 前，必须证明存在另一条持续 Deadline Refresh 主链，或启用现有 Manager-owned Projection 并重跑 no-churn/restart Gate；
- 好友机会优化不阻塞 R2 基础部署，但必须在正式 G2/G3、完整产品功能和容量基线之前合入；
- 正式 `CP-G1-REMOTE-WATCH` 以好友机会合流后的最终产品候选为准；R2 前置 G1 仅登记为基础设施基线；
- 任何正式 Run 只允许一个 Run Operator，占用同一 kind/Redis/故障注入锁。

# 1. 目标、非目标与进入条件
## 1.1 总目标
在本地单机 kind 环境中，完成从“动态路由正确”到“完整产品可负载”的连续证明，并产出可以支撑答辩和容量推导的证据。最终交付包含：
- 单链路能力：登录/WSS、Join/Snapshot、Player Actor、Farm Actor、Workflow/Worker、实时广播、动态路由。
- 单 Pod 安全容量：Gate 连接/建连/下行，Player/Farm Actor Turn，Worker Step，Controller Watch，Redis Commit。
- 热点能力：1 个农场主 + 199 个访客、连续 FarmChange、Care/Theft、Slow Reader。
- 故障带载：Controller Restart、Planned Migration、Crash Takeover、Worker Kill、Gate Restart。
- 稳定性：30 分钟调试 Soak 与 2 小时正式本地 Soak。
- 容量推导：从 DAU→PCU→命令/字节/写放大→单 Pod 安全容量→N+1 Pod 数。
## 1.2 明确非目标
- 不在一台 32 核机器上模拟 3,000,000～5,000,000 条真实长连接；
- 不把 200 人同场、200 WSS 或单机最大 QPS 写成“已实测 3000 万 DAU”；
- 不在压测阶段重写业务一致性语义、Writer Fence、Workflow 成功边界；
- 不同时建设 Controller HA、HPA、外部 Registry、公网 TLS、正式云 Staging；
- 不以 CPU 100% 时的极限吞吐直接作为生产安全容量；
- 不通过关闭幂等、Fence、持久化或正确性检查来换取虚假 QPS。
## 1.3 压测进入条件
| 门禁 | 必须满足 | 未满足时允许做什么 |
| --- | --- | --- |
| 代码身份 | Backend/H5/Harness 精确 Commit、镜像 ID、Pod imageID、Manifest SHA 固定 | 仅开发调试，不登记正式基线 |
| Dynamic R2 | R1 closure commit、fresh identity、无 Holder Restart、8/8 Authority/Route/Boot 一致 | 仅允许 dry-run、工具测试与失败诊断 |
| 动态路由 | R2 基线、好友机会产品合流、G1/G2/G3 通过 | 可记录组件探索值，但不得代表最终动态版本 |
| 长周期路由 | Snapshot Deadline 有持续刷新证据，跨旧 Deadline 与 Controller Restart PASS | 不得运行 30min/2h 或登记稳定容量 |
| 好友机会 | Backend/H5 exact merge，V2 fixture、shadow mismatch、2 Worker OwnedShards PASS | 不得登记 social-heavy 和机会链容量 |
| 完整产品 | Gate/Worker/H5 接入，F1/F2/F3、好友机会与 Authority Oracle PASS | 只能做组件基准 |
| 正确性硬门 | 重复结算、旧 Writer 成功、FarmVersion Gap、Due/Unknown 残留均为 0 | 不得进入性能结论 |
| 压测器资格 | 计划负载和实际负载一致，压测器 CPU/RSS/未发送量可见 | 不得认为服务端达到容量 |
| 可观测性 | 逐 Pod Metrics、日志、Authority/Client Oracle、时间线完整 | 只可做探索性测试 |


```text
当前建议
当前不再回到 Static 主线。Dynamic Writer 先关闭 R1 并完成 R2；与此同时，好友 Backend/H5 Agent、Harness、流量 Profile、指标和 Evidence Agent 可以在独立 worktree 中并行准备，但不得占用正在执行正式 R2/G1 的集群资源。R2 基线通过后再形成好友机会产品合流候选；合流后重跑 G1，并以该 exact candidate 完成 G2/G3、完整业务和正式容量测试。
```


## 1.4 当前本地环境是否够用
当前 Linux 机器具有 32 核 CPU、约 64 GB 内存和充足磁盘，单机 kind 可以承载两 Worker 节点、约 9 个业务 Pod 和宿主机压测器。它适合测单 Pod 能力、1→2 Pod 扩展趋势、200 人热点与本地故障恢复；不适合直接复现数百万长连接。
依据说明：依据本地部署审阅：Docker 29.3.1、kind 0.32.0、kubectl 1.36.3、32 核、约 64 GB 内存、约 309 GB 可用磁盘。
# 2. 3000 万 DAU 容量模型
## 2.1 为什么 DAU 不能直接除以 86,400
DAU 是一天内出现过的唯一玩家数，不等于同时在线，也不等于某个接口的 QPS。容量必须分别计算：新会话、在线连接、Join/Snapshot、业务命令、Redis 权威提交、下行广播、Worker 恢复和故障重连。
```text
容量推导主链
DAU
→ 每日会话数
→ 峰值新会话到达率 + 峰值在线连接
→ Join / SwitchFarm 到达率
→ 各类业务命令到达率
→ Actor Turn / Redis Commit / Workflow Step 放大
→ FarmChange 下行扇出与网络字节
→ 单 Pod 安全容量
→ N+1 副本数
```


## 2.2 当前候选输入与边界
| 参数 | 当前候选 | 状态 |
| --- | --- | --- |
| DAU | 30,000,000 | 题目输入 |
| 每玩家每天会话数 | 6 | CapacityProfile v0.1 候选 |
| 单次在线时长 | ≤ 5 分钟 | 候选上界 |
| 设计 PCU / 压力 PCU | 3,000,000 / 5,000,000 | 候选，不是实测 |
| 每会话 Join | 3 次设计 / 6 次压力 | 候选 |
| 每日权威写 | 约 20 次/玩家 | 候选 |
| 好友数 | 平均 100 / 上限 300 | 产品候选 |
| 同一农场活跃会话 | 1 个农场主 + 199 个访客 | 冻结热点口径 |


由均匀日平均可得到量级基线：约 2,083 新会话/秒、约 625,000 平均在线、约 6,250 设计 Join/秒、约 6,944 平均权威写/秒。文档中更激进的 41,667 session/s、125,000 Join/s、138,889 write/s 和 3M/5M PCU 仍是候选压力点，不是已经验证的 SLO，也绝不能直接作为单 Pod 门禁。
依据说明：来源于 CapacityProfile v0.1。最终峰值因子、命令混合和会话相关性必须由本轮压测与后续真实运营数据校准。
## 2.3 必须通过压测回填的参数
| 维度 | 需要测出的值 | 用途 |
| --- | --- | --- |
| 接入 | 每 Gate 安全连接数、session/s、WSS handshake/s | Gate 副本数 |
| Join | 每 Gate/Farm Pod 安全 Join/s、Snapshot P50/P99 字节 | Join 洪峰与带宽 |
| Player | Actor turns/s、活跃 Actor 内存、Redis commit 放大 | player-svr 副本数 |
| Farm | 不同 Farm 总吞吐、单热点 Farm 吞吐、FarmChange 扇出字节 | farm-svr 副本与热点上限 |
| Worker | steps/s、claim/s、retry amplification、backlog oldest age | worker 副本数 |
| Redis | Lua/Commit P99、bytes/op、pool wait、热 Slot | 存储容量与连接预算 |
| Route | Watch apply lag、Snapshot 字节、迁移时延、Controller horizon | 动态路由预算 |


### 2.3.1 好友机会流量模型必须新增的参数

| 参数 | 含义 | 为什么必须测 |
| --- | --- | --- |
| `friend_list_reads_per_session` | 每会话好友列表/机会恢复查询次数 | 决定 Header read 和 Redis read amplification |
| `friend_count_distribution` | 20/100/300 好友占比 | 不能只用 300 上限代替真实分布 |
| `opportunity_events_per_farm_hour` | 成熟、Care、Theft 状态变化产生的候选事件 | 决定 Worker Due 与 Hint 产生率 |
| `online_friend_ratio` | 机会发生时在线好友比例 | 决定 SessionRoute 批读与 Gate 下行扇出 |
| `hint_coalescing_ratio` | 300ms 合并后实际定向查询数/原始 Hint 数 | 决定 H5 与后端查询风暴是否受控 |
| `targeted_recheck_qps` | Hint 后按目标农场精查 QPS | 决定 Friend/Farm read path 容量 |
| `opportunity_due_lag` | 到期到 Worker 处理的延迟 | 决定弹窗体验和积压风险 |
| `owned_shard_skew` | 多 Worker 持有 FARM Shard 的负载偏差 | 决定扩容是否真的分摊工作 |

候选计算链：

```text
机会事件/s
× 平均在线好友数
× Direct可达率
= Hint下行目标/s

Hint下行目标/s
× 合并后定向查询比例
= Targeted Recheck/s

FARM Due/s
÷ safe_opportunity_due_steps_per_worker
= Worker需求数
```

这些参数不能由当前单 Viewer 真实证据直接外推，必须在 T15～T17 中回填。

## 2.4 最终 Pod 数公式
```text
容量报告核心公式
gate_pods = ceil(max(
  peak_ccu / safe_connections_per_gate,
  peak_session_arrival / safe_session_arrival_per_gate,
  peak_join_qps / safe_join_per_gate,
  peak_downlink_bytes / safe_downlink_bytes_per_gate
) × redundancy)

player_pods = ceil(max(
  peak_player_actor_turns / safe_player_turns_per_pod,
  active_player_actor_bytes / safe_player_memory_per_pod,
  player_redis_commits / safe_player_redis_budget
) × redundancy)

farm_pods = ceil(max(
  peak_farm_actor_turns / safe_farm_turns_per_pod,
  active_farm_actor_bytes / safe_farm_memory_per_pod,
  farmchange_bytes / safe_farm_downlink_budget
) × redundancy)

worker_pods = ceil(
  peak_worker_steps / safe_worker_steps_per_second × redundancy
)
```


单热点农场是额外约束：同一个 FarmActor 的唯一写入顺序不能通过增加 Farm Pod 自动提高。因此需要单独验证 1+199 同场，必要时优化 Actor Turn、广播编码、SubscriptionHub 或热点 Lane，而不是破坏唯一裁决顺序。
# 3. 环境、拓扑与可复现性
## 3.1 正式本地拓扑
```text
首轮动态容量拓扑
宿主机
├── Scenario Coordinator
├── Go Protocol Bots
├── 2～4 个 Playwright H5
├── Fault Controller
├── Metrics / pprof / Evidence Collector
└── kind
    ├── route-controller ×1
    ├── farm-gate ×1（首轮）
    ├── player-svr ×2
    ├── farm-svr ×2
    ├── farm-worker ×2
    ├── farm-h5 ×1
    └── isolated Redis ×1
```


H5/Gate 是入口，Player/Farm 是有固定 Holder 身份的 Actor 承载，Worker 是通过 DueIndex/Lease 可接管的后台执行器。三类对象必须分别测容量和故障。
## 3.2 测试 Profile
| Profile | 目的 | Shard / Pod | 是否用于正式容量 |
| --- | --- | --- | --- |
| route-scale-1024x1024 | 验证路由表、Watch、Lease、Scheduler 元数据规模 | 2048 Shard，4 Holder | 否；控制面规模证据 |
| route-migration-4x4 | G1/G2/G3、精确迁移和旧写 Fence | 8 Shard，4 Holder | 功能/故障资格 |
| load-single-pod | 测某类 Pod 的突破点、稳定点、安全点 | 目标 Pod=1，其余依赖留足 | 是 |
| load-scale-out | 验证 1→2 Pod 扩展效率和负载均衡 | 目标角色 1/2 副本 | 是 |
| hot-farm-1x199 | 单 FarmActor 与下行扇出 | 1 owner + 199 visitors | 是 |
| mixed-dynamic | 真实混合业务 + 动态路由 | 完整拓扑 | 最终本地验收 |


### 3.2.1 本版冻结的五个部署/测试 Profile

| Profile | 用途 | 主要配置 | 是否形成容量结论 |
| --- | --- | --- | --- |
| `route-migration-r2-4x4` | 新 R2 Bootstrap 与基础 G1 | 4+4、2 Worker、1 Gate、1 H5；fresh identities | 否，仅部署资格 |
| `route-migration-final-4x4` | 好友合流后的 G1/G2/G3 | Remote Watch、composed、Direct Hint、narrow Header | 否，先正确性 |
| `dynamic-pressure-4x4` | 正式单 Pod/混合/热点压测 | 长周期 Deadline Refresh、固定资源、压力限流配置 | 是，本地安全容量 |
| `friend-opportunity-ab` | full/narrow、pubsub/direct 对照 | 同一数据与负载，只切单个模式变量 | 是，优化对照 |
| `route-scale-1024x1024` | Route 元数据规模回归 | 不跑业务容量，不跑 G2/G3 | 仅 Route 规模证据 |

正式压力结果不得混用不同 Profile。特别是 R2 的短时配置不能直接用于 Soak。

## 3.3 每次 Run 必须冻结
- Backend、H5、Harness、Manifest、Route Profile 的精确 Commit/SHA；
- Docker Image ID 与每个 Pod 的 status.imageID；
- CPU/内存 request/limit、Go GC、Redis 配置、ShardCount、TTL、队列上限；
- 测试数据集、账号范围、Farm 热点分布、命令混合、故障时间点；
- 压测器数量、宿主机资源、计划负载与实际负载；
- 运行前后的 Pod、Redis、Authority、Route、Client Oracle 状态。
## 3.4 资源隔离规则
| 规则 | 要求 |
| --- | --- |
| 禁用 HPA | 所有单 Pod/扩展性测试固定副本，避免结果被自动扩容改变。 |
| 清理宿主机旧进程 | 旧 Gate/Player/Farm/Worker/H5 不得同时访问相同 Redis。 |
| 隔离 Redis | 正式容量优先独立本地 Redis；禁止共享公司 Redis 并发压测。 |
| 串行正式 Run | 同一主机一次只允许一个正式压力或故障 Run。 |
| 压测器外置 | Go Bot/Playwright 在宿主机，不与被测服务争抢 Pod 资源。 |
| 固定噪音 | 关闭无关构建、历史容器、扫描任务；记录宿主机 idle 基线。 |


# 4. 测试方法与有效性规则
## 4.1 三种容量值
| 名称 | 定义 | 用途 |
| --- | --- | --- |
| breakpoint_capacity | 继续加压后出现 CPU/网络/Redis/队列饱和、P99陡升或错误扩散的极限点 | 定位瓶颈，不用于副本计算 |
| max_stable_capacity | 在稳定窗口中 P99、错误率、队列、内存不持续恶化的最大负载 | 比较优化前后 |
| safe_capacity | 保留 GC、抖动、热点、迁移和 N+1 余量后的可承诺负载 | 计算 Pod 数 |


```text
CPU 100% 的正确用法
可以把 CPU 压满以寻找 breakpoint，但正式容量必须使用 safe_capacity。某些服务会先被 FD、网络、Redis Pool、Actor Mailbox 或下行队列限制，CPU 不一定先满。
```


## 4.2 阶梯负载法
```text
单 Pod 阶梯法
预热
→ 20% 候选负载
→ 40%
→ 60%
→ 80%
→ 继续小步增加直到拐点
→ 回退一档验证可恢复性
→ 延长稳定窗口确认 max_stable
→ 加入 N+1/迁移/抖动余量冻结 safe_capacity
```


每档建议至少 5～10 分钟；热点 Farm、Worker backlog 和 Redis GC 类场景需要更长稳定窗口。首次基线不提前冻结 P99 SLO，而是完整记录 P50/P95/P99、拐点和队列趋势，再由产品体验与工程余量确定 SLOProfile v0.1。
## 4.3 开放到达与闭环模型分开
| 模型 | 适用场景 | 风险 |
| --- | --- | --- |
| 开放到达 Open Model | 登录洪峰、Join/s、业务到达率、Worker Step | 服务变慢时仍持续施压，能暴露排队 |
| 闭环 Closed Model | 固定在线 WSS、每用户思考时间、200 人同场 | 服务变慢会自然降低请求率，必须记录计划/实际差异 |
| Burst/Spike | 发布后重连、活动整点、Controller/Gate 恢复 | 短窗口可能打穿限流与连接池 |


## 4.4 冷、暖、热三种状态
| 状态 | 说明 | 必须覆盖 |
| --- | --- | --- |
| Cold | Actor 未激活、无连接池热身、首次脚本/缓存 | 登录、Join、Actor LoadAndClaim |
| Warm | 依赖连接已建立，Actor 部分激活 | 日常混合流量 |
| Hot | 热点 Actor、连接、缓存和队列持续存在 | 1+199、广播、Slow Reader、长 Soak |


## 4.5 性能结果有效性
| 门禁 | 判定 |
| --- | --- |
| 计划负载与实际负载 | 正式稳态窗口必须同时报告 planned、actual、unsent、client timeout。建议 v0.1：actual/planned ≥ 98%，否则本档无效。 |
| 压测器资源 | Driver CPU/RSS/FD/网络不能成为瓶颈；必要时多进程分片。 |
| 正确性 | 任何硬正确性指标非零，本次性能结论立即作废。 |
| 稳定窗口 | P99、队列、RSS、goroutine、Due backlog 不得持续单调上升。 |
| 期望拒绝 | 业务规则造成的 REJECTED/NO_CHANGE 单独统计，不计为系统错误。 |
| 可复现性 | 同一配置至少重复 3 轮；差异超过 10% 时先查噪音和数据分布。 |


依据说明：“planned vs actual 必须同时记录”属于冻结验收原则；98% 为本计划建议的初始有效性阈值，可由第一次 Load Generator Qualification 调整。
# 5. 指标、Trace 与 Evidence
## 5.1 五类容量常数
```text
最终必须回填
safe_connections_per_gate
safe_join_per_gate_or_farm_pod
safe_actor_turns_per_pod
safe_redis_commits_per_cluster
safe_worker_steps_per_second
```


每条主链至少拆为 Gate Admission/Queue、FirstHop RPC、Route Resolve、Actor Mailbox Wait、Actor Turn CPU、Redis Queue/RTT/Lua、Workflow Wait、Downlink Queue、Client Apply。只看端到端 P99 无法定位瓶颈。
## 5.2 逐服务核心指标
| 组件 | 容量/延迟指标 | 正确性/安全指标 |
| --- | --- | --- |
| farm-gate | connections、session/s、handshake、join、downlink bytes、writer queue、FD、CPU/RSS | sequence gap、old session reject、queue full isolation、route stale admission |
| player-svr | actor turns、mailbox wait、active actor bytes、dirty age、writeback queue、Redis commit | negative asset、duplicate grant/debit、writer epoch reject、receipt conflict |
| farm-svr | farm turns、mailbox depth、snapshot bytes、FarmChange encode/fanout、subscription count | FarmVersion gap、stolen over limit、wrong guard decision、stale holder commit |
| farm-worker | workflow create、participant steps、claim、retry amplification、backlog oldest age | InvalidOrder、Due residual、duplicate settlement、claim while route stale |
| route-controller | projection wave、Watch clients、apply lag、revision lag、migration phase latency | partial publish、deadline rollback、duplicate publisher、stale callback commit |
| Redis | RTT、Lua P50/P95/P99、pool wait、bytes/op、hot slot、memory、eviction | NOSCRIPT recovery、unknown resolution、CROSSSLOT、key/receipt capacity |


## 5.3 Trace 采样
Trace 不应把 player_id、farm_id、operation_id 作为 Prometheus Label。建议对 0.1%～1% 请求采样完整链路，并对 P99、错误、迁移中的目标 Shard 强制采样。
```text
推荐 Span 分段
Client Send
→ Gate Admission / Queue
→ FirstHop RPC
→ Route Resolve
→ Actor Mailbox Wait
→ Actor Turn
→ Redis Commit / Workflow Step
→ Downlink Queue
→ Client Apply
```


## 5.4 Evidence Bundle
| 文件 | 内容 |
| --- | --- |
| run-manifest.json | RunID、场景、时间、Profile、计划负载、故障点 |
| source-lock.json | Backend/H5/Harness Commit 与 worktree clean |
| image-lock.json | Docker Image ID 与 Pod imageID |
| rendered-manifest.yaml + sha256 | 实际 K8s 配置 |
| planned-vs-actual.json | 计划/实际连接、请求、未发送、客户端错误 |
| pod-metrics/*.ndjson | 逐 Pod CPU/RSS/GC/FD/队列/业务指标 |
| authority-oracle.json | Redis/Actor/Receipt/Route 的最终权威对账 |
| client-state-oracle.json | 每个客户端 Snapshot/Delta/FarmVersion 收敛 |
| fault-timeline.ndjson | Crash/Drain/迁移/恢复时间线 |
| friend-opportunity-metrics.ndjson | Header 模式、Hint、定向精查、Due Lag、OwnedShards 与 H5 合并指标 |
| owned-shards-timeline.ndjson | Worker 对 FARM Shard 的精确覆盖、迁移和 Drain 变化 |
| final-verdict.json | 机器可判定 PASS/FAIL 与限制 |


> 隐私与安全  
> Evidence 不保存 Redis URL/密码、TLS 私钥、Cookie 正文、ConnectionTicket 正文或完整 WriterClaim。


# 6. 总体执行阶段
| 阶段 | 目标 | 输出 |
| --- | --- | --- |
| P-2 R2 修复与重跑 | 冻结 R1 失败，提交三个 closure，fresh identity 完成动态部署 | CP-DYNAMIC-R2-BOOTSTRAP |
| P-1 产品合流 | 将好友机会 Backend/H5 收敛到 R2 exact tip，并补 V2/OwnedShards | CP-FRIEND-OPPORTUNITY-DYNAMIC-CONVERGENCE |
| P0 G1/G2/G3 | Remote Watch、三轮迁移、Proof-gated Crash Takeover | CP-ROUTE-CONTROL-MVP |
| P1 工具资格 | 证明 Bot、Fault、Metrics、Oracle 不会制造假通过 | CP-LOAD-TOOLS-QUALIFIED |
| P2 低负载功能资格 | 1～5 客户端 + F1/F2/F3 + 好友机会 + Authority Oracle | CP-FULL-PRODUCT-FUNCTIONAL |
| P3 单链路基准 | 登录、Join、机会、Plant、Harvest、Care、Theft、Worker、Route 单独测 | 链路成本与写放大 |
| P4 单 Pod 容量 | Gate/Player/Farm/Worker/Controller/Redis 突破点与安全点 | 五类安全容量常数 |
| P5 水平扩展 | 1→2 Pod，验证吞吐、OwnedShards 和负载分布 | scale_efficiency |
| P6 混合与热点 | 真实命令混合、300 好友、1+199、Slow Reader | 热点与体验基线 |
| P7 故障带载 | Controller、迁移、Crash、Worker、Gate、Redis、机会 Hint | 故障预算与安全证据 |
| P8 Soak | 30 分钟与 2 小时 | 泄漏、积压、P99 趋势 |
| P9 容量报告 | DAU→PCU→QPS→Pod 数 | 3000 万 DAU 推导报告 |


## 6.1 建议执行顺序
```text
主执行链
R1 closure提交与全新R2
→ R2基础G1（只作基础设施基线）
→ 好友机会Backend/H5产品合流
→ G1最终重跑（跨旧Deadline）
→ G2 Planned Migration A→B→A→B
→ G3 Crash Takeover
→ F1/F2/F3 + 好友机会 + Authority Oracle
→ Load Generator Qualification
→ 单链路与单Pod
→ 1→2 Pod扩展与OwnedShards
→ 300好友 / 200人热点 / SlowReader
→ 故障带载
→ 30min / 2h Soak
→ 3000万DAU容量报告
```

## 6.2 R2 必须严格执行的 Runbook

1. 对三个 closure 文件执行 Python/Shell 语法检查、`scripts/local-k8s/test-tools`、bootstrap focused test/race/vet 和全仓测试；
2. SSH-signed commit，确认 Writer worktree clean；
3. 发布绑定新 exact head 的不可变 runtime image receipt；
4. 生成新的 `ROUTE_RUN_ID`、Redis Namespace、TLS、Secret UID、Controller Boot、Pod UID 和 HolderBoot；
5. dry-run Holder 与 full manifest；
6. apply 后按阶段生成不可覆盖 receipt；
7. 初始 Claim 前任何 Holder restart 都判整轮失格，不能在原身份上继续；
8. 通过后保存 8/8 Snapshot/Authority/Boot、Watch revision、Pod UID/imageID 和业务 readiness 证据；
9. R1 Keyspace 和证据保持不动，不做全局 flush/scan。

## 6.3 好友机会优化的合入决策

### 6.3.1 结论

**需要在正式压测前合入，但不要在当前 R2 前插入，也不要直接合 Integration。**

原因：

- 它改变好友列表、机会查询、SessionRoute 批读、Gate Direct Hint、Worker Due 扫描和 H5 请求合并；这些正是 social-heavy 与混合流量的被测链路；
- 当前 Dynamic Profile 使用 2 Worker，而该优化只有接入不重叠 `OwnedShards` 后才能安全多副本；
- 若在压测之后再合，好友列表、Redis、Worker 和 Gate 的容量数字都需要重测；
- 若在 R2 前插入合，当前 R1 closure 的 exact image/receipt/identity 会再次变化，增加部署归因难度。

### 6.3.2 正确合流顺序

```text
R2基础部署与基础G1
→ 从R2 exact tip建立Backend产品合流worktree
→ 合入3a48228c...
→ 补V2 Header fixture + shadow mismatch metrics
→ 验证2 Worker OwnedShards精确覆盖/不重叠
→ 从最终H5基线合入787dc00
→ 构建Backend/H5联合候选
→ G1全量重跑
→ G2/G3与完整业务
→ 正式压测
```

### 6.3.3 最终模式与 A/B 模式

| 阶段 | Header | Hint Transport | 用途 |
| --- | --- | --- | --- |
| Compatibility baseline | `full` | `pubsub` | 兼容性能对照，不作为最终主链 |
| Shadow qualification | `narrow-shadow` | `shadow-direct` | 比较结果与成本，不改变用户主链 |
| Final dynamic candidate | `narrow` | `direct` | 正式功能、G2/G3 与压测 |

只有指标证明 Direct SessionRoute 批读成为瓶颈时，才允许设计 OnlineRouteIndex；当前计划禁止提前引入第二份在线真值。

## 6.4 长周期 Route Freshness Gate

当前 handoff 明确把 `ROUTE_CONTROLLER_LEASE_PROJECTION_MODE` 设为 disabled，但没有给出 30 分钟/2 小时运行时持续推进 Snapshot Deadline 的替代证据。因此在正式 G1 和任何容量/Soak 前必须二选一：

1. 启用现有 Manager-owned Lease Projection，并重跑 deadline-only no-churn、Controller Restart 和 Watch apply；或
2. 用 Evidence 证明另一个单一 Owner 会持续推进所有 ACTIVE Route Deadline，跨越旧 Snapshot Deadline 后业务仍成功。

未关闭该 Gate 时，只允许短时 R2/G2/G3 调试，不得登记稳定容量。


# 7. 关键场景详细计划
## 7.1 T0：Load Generator Qualification
目标：证明压测器能够按计划产生连接和请求，不因为服务变慢而自动降压。
1. 使用本地 echo/mock 协议端点与真实 Gate 各跑一轮；验证 planned/actual/unsent 统计。
1. 阶梯增加 Bot 进程数，记录 Driver CPU、RSS、FD、网络、调度延迟。
1. 模拟服务端慢响应，确认开放到达模型仍持续计划流量，闭环模型能报告降压。
重点指标：planned vs actual、unsent、client timeout、driver CPU/RSS/FD、时钟误差。
> 硬断言  
> 压测器未达到目标负载，后续所有容量结果无效。


## 7.2 T1：登录、换票与 WSS 建连洪峰
目标：测每 Gate 的新会话到达率和集中重连能力。
1. 分别运行注册/账密登录、Cookie 免密换票、WSS ClientHello 三段，以及完整组合。
1. 采用开放到达：逐步增加 session/s；另加 1 秒、5 秒、30 秒 Burst。
1. 覆盖冷连接池、暖连接池和 Gate Restart 后重连风暴。
重点指标：login P95/P99、ticket P99、WSS handshake P99、success ratio、Session Redis P99、Gate FD/network。
> 硬断言  
> Ticket 重放、旧 Session Fence、限流误杀和 Cookie 路径必须保持正确。


## 7.3 T2：稳定在线连接
目标：测单 Gate 能维持多少 WSS 连接，以及每连接基础 CPU/内存/FD/心跳成本。
1. 连接阶梯：50→100→150→200，之后可探索 500。
1. 低频心跳、Subscription Renew、少量业务命令；稳定 10～30 分钟。
1. 记录每新增 100 连接的 RSS、FD、goroutine 和网络增量。
重点指标：connections、RSS/connection、FD/connection、heartbeat cost、writer queue、disconnect rate。
> 硬断言  
> 这是连接容量，不等于业务 QPS。


## 7.4 T3：Join / Snapshot 风暴
目标：测 Join、Snapshot 生成、JoiningBuffer、Catch-up 与下行带宽。
1. 分别测不同 Farm 分散 Join 与同一热点 Farm 集中 Join。
1. 采用开放到达 Join/s；记录 Snapshot P50/P99 bytes。
1. 覆盖冷 Actor 激活、重复 Join、取消 Join、Fresh Join。
重点指标：Join P95/P99、Snapshot bytes、SubscriptionReady、JoiningBuffer item/bytes、downlink bytes。
```text
硬断言
必须保持 ACCEPTED→Snapshot→Delta*→Ready 的连续顺序。
```


## 7.5 T4：个人经营 Loop
目标：测 Buy→Plant→Mature→Harvest→Sell→Task 的完整核心闭环。
1. 先分别压 Buy/Sell、Plant、Harvest，再按真实顺序组合。
1. 分散不同玩家/农场测总吞吐；同一玩家高并发测 PlayerActor 串行。
1. 记录每命令 Actor Turn、Redis Commit、Receipt、Workflow/Order 放大。
重点指标：command P99、Player/Farm mailbox、Redis commits/op、workflow steps/op、dirty age。
> 硬断言  
> 资产、CropCycle、Task、Compendium 结果必须对账。


## 7.6 T5：Care 与社交访问
目标：测 Friend/Permit/Join/Care、奖励 Order 与实时 FarmChange。
1. 大量玩家访问不同好友 Farm，测分散吞吐。
1. 同一 Farm 多人 Care，测 FarmActor 冲突与 NO_CHANGE 比例。
1. 注入 Care Result 丢失，验证 OperationResult/Worker 恢复。
重点指标：permit P99、Care P99、reward order age、FarmChange latency、NO_CHANGE ratio。
> 硬断言  
> 好友索引不能替代 Relation/Permit，重复 Care 不得重复奖励。


## 7.7 T6：Theft 复杂链路
目标：测最复杂的跨 Actor、宠物裁决、访客入库、主人补偿和恢复。
1. 分别测分散 Theft 与同一热点 Farm Theft。
1. 重复相同 operation_id、不同 operation_id、响应丢失、Worker 重试。
1. 记录 Attempt/Settlement/Receipt/Order 的写放大和终态时间。
重点指标：Theft P99、steps/op、Redis commits/op、worker retries、settlement completion。
> 硬断言  
> 可偷上限、宠物截留、访客库存、主人补偿、FarmVersion 必须最终一致。


## 7.8 T7：1 个农场主 + 199 个访客
目标：验证单热点 FarmActor、SubscriptionHub、FarmChange 扇出和客户端收敛。
1. 200/200 达到 SubscriptionReady 后再开始业务负载。
1. 按低、中、高三档触发 Care/Theft/FarmChange。
1. 记录 FarmActor 只生成一次变化、下行按 Gate 聚合、199 个客户端应用延迟。
重点指标：Farm mailbox wait、FarmChange encode count、fanout bytes、client apply P99、version gap。
> 硬断言  
> 200 人同场与 3000 万 DAU 是两个独立证据。


## 7.9 T8：Slow Reader
目标：证明一个慢客户端不会拖慢 FarmActor、Gate 全局队列和其他 199 人。
1. 199 个健康客户端 + 1 个完全不读客户端。
1. 持续产生 FarmChange，观察慢连接队列到达上限后的隔离/断开。
1. 比较有无 Slow Reader 时健康客户端 P99 和 Farm mailbox。
重点指标：slow queue、healthy P99 delta、Farm mailbox、healthy version gap、disconnect reason。
> 硬断言  
> 第一轮只声明“一个完全不读连接”的证据，不扩大为任意慢网矩阵。


## 7.10 T9：Worker 吞吐与积压
目标：测 Workflow/Order 创建、Claim、Participant Step、重试和 backlog 清理能力。
1. 构造 Care/Theft/Harvest 等不同 Work Family。
1. 逐步增加 work create/s，观察 DueDepth 与 oldest age。
1. 停止一个 Worker，验证另一个接管；恢复后检查重复副作用。
重点指标：workflow_create、participant_step、due_claim、retry amplification、backlog oldest age。
> 硬断言  
> Worker 过载必须停止 Claim 或有界积压，不能无限吃内存。


## 7.11 T10：Remote Watch / Controller Restart 带载
目标：验证 G22/G1 在持续业务下仍能重连和维持安全。
1. 先 50 WSS，再 100/200 WSS。
1. Controller 中断小于 Horizon、超过旧重试预算、超过 Horizon 三类。
1. 观察 Gate/Holder/Worker 的 Freshness、Ready、Admission 与重连。
重点指标：watch reconnect、apply lag、revision lag、lease remaining、business success。
> 硬断言  
> Fresh 时继续服务；Stale 时停止新准入但继续重连；恢复后 Full Resync。


## 7.12 T11：Planned Migration 带载
目标：测 A→B Drain/Prepare/Transfer/Publish/Verify 对业务 P99 和正确性的影响。
1. 先 5～20 客户端低负载三轮 A→B→A→B。
1. 再 50 WSS，最后在 200 人热点 Farm 上迁移所在 Shard。
1. 分阶段记录 Source Drain、Facts Barrier、Authority Transfer、Watch Apply、Fresh Join。
重点指标：migration total、phase latency、P99 delta、old writer success、fresh join recovery。
> 硬断言  
> 切权后旧 Holder 成功写必须为 0，未迁移 Shard 持续服务。


## 7.13 T12：Crash Takeover 带载
目标：验证合法 Proof、Lease/Authority 观察、旧 Pod 恢复和新 Holder 接管。
1. 先 50 WSS，确认拒绝路径：无 Proof、Lease 未过期、Observation 不匹配。
1. 形成合法 CrashTakeoverProof 后执行 Transfer。
1. 旧 Pod 恢复后注入迟到 Renew/Commit。
重点指标：takeover time、old writer reject、business recovery、plan replay、duplicate effects。
> 硬断言  
> 安全优先于恢复速度；任何旧写成功都判 FAIL。


## 7.14 T13：混合业务流量
目标：在完整产品主链上模拟稳定在线、经营、社交、查询、恢复的组合。
1. 不要一开始拍一个唯一百分比；先运行 personal-heavy、social-heavy、join-heavy 三个 Mix。
1. 通过单链路成本回填后，再形成 TrafficProfile-v0.1-mixed。
1. 采用开放到达业务 QPS + 固定在线 WSS 的混合模型。
重点指标：actual mix、command P99、Redis amplification、downlink bytes、worker lag。
> 硬断言  
> Mix 必须记录真实落到每个命令的数量，不能只记录总 QPS。


## 7.15 T14：Soak
目标：验证内存、goroutine、FD、Actor、Due、Watch Lag 和 P99 随时间稳定。
1. 先 30 分钟调试 Soak，包含一次 Controller Restart、一次 Worker Kill、一次 Planned Migration。
1. 再 2 小时正式本地 Soak，维持 200 WSS 或稳定混合负载。
1. 每 10 秒采样资源，每 1～5 分钟做 Authority/Client 对账。
重点指标：RSS slope、goroutine slope、FD、Due residual、Watch lag、P99 drift。
> 硬断言  
> 本地答辩项目不先做 24 小时；2 小时足以发现多数泄漏与积压。


## 7.16 T15：好友 Header 与 SessionRoute 微基准
目标：在同一数据、同一 Redis 和同一资源下，量化 `full / narrow-shadow / narrow` 与 `pubsub / shadow-direct / direct` 的成本。

1. 好友规模使用 20、100、300；分别覆盖 V1 与 V2 Farm Model fixture；
2. Header 读取记录命令数、RTT、请求/响应 bytes、分配量、P50/P95/P99、UNKNOWN 与 full fallback；
3. SessionRoute 读取按在线比例 0%、10%、50%、100% 测量，并记录按 Gate 分组后的 DeliverBatch；
4. shadow 模式必须比较结果，不允许 UNKNOWN 被伪装成 NONE；
5. 每个模式至少重复 5 轮，使用相同数据和缓存状态。

本计划建议的本地初始门槛（不是生产 SLO）：

```text
narrow与full结果不一致              = 0
narrow相对full中位延迟改善           >= 30%
narrow相对full传输/解码字节下降       >= 30%
定向精查P95                          <= 50ms
错误或陈旧连接Hint成功投递             = 0
```

## 7.17 T16：好友机会端到端与 H5 合并
目标：证明“机会边界→Worker→SessionRoute→Gate→H5→定向精查→CandidateWindow”的完整链路正确且有界。

1. 准备 300 好友、可浇水和将成熟农场；
2. 记录 `next_opportunity_at`、Due claim、Hint emit、Gate enqueue、H5 receive、targeted query、popup render；
3. 同一目标在 300ms 内注入 1、5、20 个 Hint，验证只产生有界定向查询；
4. 多目标 Hint 验证按 `target_owner_id + revision` 去重而不会吞掉不同目标；
5. 丢弃 Hint 后通过登录、回家、页面可见和低频 Pull 恢复；
6. 弹窗后连续导航必须读取更新后的 CandidateWindow。

本计划建议的本地初始门槛：

```text
浇水/成熟边界到弹窗P95               <= 2s
Hint定向查询错误                      = 0
同目标300ms窗口内定向查询             <= 1次
弹窗目标与CandidateWindow不一致       = 0
错误Gate/Boot/SessionEpoch/RouteRevision投递成功 = 0
```

## 7.18 T17：多 Worker OwnedShards、迁移与机会积压
目标：证明好友机会 Worker 在 Dynamic Assignment 下可安全多副本，并在 G2/G3 后不会重复扫描或漏扫。

1. 2 Worker 对全部 FARM Shard 做精确集合对账：覆盖全集、交集为空；
2. 逐 Shard 注入 schedule due，验证只有 Owner Worker Claim；
3. 执行 FARM Shard A→B→A→B，观察 OwnedShards 更新、旧 Worker 停 Claim、轮转游标重新归一化；
4. Worker Kill 后剩余 Worker 只在新 Assignment 允许的范围接管；
5. 构造共享 DueIndex 中大量非 Schedule 前缀，测有界分页、公平性和 oldest due age；
6. 明确登记 65,536 个更老非 Schedule 成员的已知边界，不把未证明范围写成 PASS。

硬断言：

```text
owned_shard_overlap_total              = 0
owned_shard_gap_total                  = 0
duplicate_opportunity_claim_total      = 0
stale_generation_claim_success_total   = 0
wrong_gate_hint_delivery_total         = 0
opportunity_due_residual               = 0（收尾窗口后）
```

# 8. 单 Pod 安全容量测试
## 8.1 通用方法
- 目标角色固定为 1 Pod，禁用 HPA；其他依赖角色提供足够余量，不允许抢先成为瓶颈；
- 固定 CPU request/limit、内存、Go GOMAXPROCS、Redis Pool、队列上限；
- 每档负载保持稳定窗口，采集 pprof CPU/heap/block/mutex 与关键 Trace；
- 找到 breakpoint 后回退一档，延长运行，确认 max_stable；
- 按 N+1、GC、迁移和热点余量冻结 safe_capacity；
- 优化后必须使用相同 Profile、数据、资源和镜像之外的唯一变化重跑。
## 8.2 farm-gate
| 子测试 | 控制变量 | 输出 |
| --- | --- | --- |
| 连接存量 | 只建连/心跳，极低业务 | safe_connections_per_gate、RSS/FD/connection |
| 新会话到达 | 固定已有连接，增加 login/ticket/handshake/s | safe_session_arrival_per_gate |
| Join 下行 | 固定 Farm 依赖余量，增加 Join/s | safe_join_per_gate、downlink bytes |
| 业务转发 | 固定命令 Mix，依赖 Pod 过配 | Gate FirstHop QPS、queue P99 |
| 广播 | 固定 FarmChange 生成率，增加订阅数 | safe_downlink_bytes_per_gate |


Gate 可能先被连接数、FD、网络、ConnectionWriter 或 PRE_AUTH 限流限制，不应只看 CPU。
## 8.3 player-svr
| 子测试 | 分布 | 输出 |
| --- | --- | --- |
| 单 Actor 热点 | 同一 player_id 串行命令 | 单 PlayerActor turn 上限与 mailbox wait |
| 多 Actor 分散 | 大量不同 player_id | safe_player_turns_per_pod |
| 冷激活 | 未激活 Player 连续 LoadAndClaim | activation/s、Redis read、singleflight |
| 持久化 | 普通 Dirty 与强制 Durable 分开 | dirty coalesce、commit QPS、RPO/P99 |
| 内存 | 增加活跃 PlayerActor | actor_state_bytes、safe active actors |


普通 Player 命令 QPS 不等于 Redis 写 QPS。需要分别测“Actor 内存事务吞吐”“唯一脏 Player 回写”“Plant/Harvest/Theft/Claim 等强制持久化写”。
## 8.4 farm-svr
| 子测试 | 分布 | 输出 |
| --- | --- | --- |
| 多 Farm 分散 | 大量不同 farm_id | safe_farm_turns_per_pod |
| 单 Farm 热点 | 同一 farm_id 多人操作 | 单 FarmActor mailbox/turn 上限 |
| Snapshot | 不同地块/模型大小 | Snapshot P50/P99 bytes 与生成时间 |
| FarmChange | 不同订阅数 | encode count、fanout bytes、client apply |
| 活跃 Actor 内存 | 增加 FarmActor 数 | state bytes、SubscriptionHub bytes |


单热点 Farm 无法通过简单增加 farm-svr Pod 横向扩展；需要保持唯一裁决，优先优化 Actor Turn、广播复用和 Hub，而不是拆 PlotActor。
## 8.5 farm-worker
| 子测试 | 负载 | 输出 |
| --- | --- | --- |
| 正常推进 | DirectDispatcher + 少量 Due | safe_worker_steps_per_second |
| 恢复积压 | 预置大量 Due | drain rate、oldest age |
| 重试放大 | Participant 超时/失败 | retry amplification |
| 热点目标 | 大量 Work 指向同一 Holder/Actor | per-target isolation |
| Opportunity schedule | 20/100/300 好友与不同到期率 | safe_opportunity_due_steps_per_worker |
| OwnedShards | 1→2 Worker、Shard 迁移/Drain | coverage、overlap、skew、cursor fairness |
| Worker Kill | 2→1 Pod | N+1 backlog recovery |

Worker 的扩容结论必须同时证明普通 Workflow Family 和 Opportunity Schedule Family；不能仅看到 Deployment 副本数增加就认为吞吐翻倍。


## 8.6 route-controller / Watch
| 子测试 | 负载 | 输出 |
| --- | --- | --- |
| Watch fanout | 增加 Consumer 数 | apply lag、CPU、network |
| Snapshot 规模 | 4+4 与 1024+1024 | publish time、bytes、memory |
| Projection | 不同 refresh interval | wave max、lease budget |
| Plan 执行 | 串行迁移计划 | phase latency、recovery |
| Restart | Pod recreate | 恢复时间、horizon、consumer reconnect |


## 8.7 Redis
- 分别统计 Authority、Runtime、Projection 三类访问，不用一个总 QPS 混淆；
- 记录每命令 Lua/GET/HMGET/Pipeline 数量、bytes/op、P50/P95/P99、Pool wait；
- 检查热 Slot、大 Key、Receipt/Order 增长、DueIndex 桶分布；
- 禁止业务路径 KEYS、全库 SCAN 和巨型 HGETALL；
- 故障时验证 NOSCRIPT、响应丢失、Timeout Unknown 和 noeviction 语义；
- 好友机会单独记录 Header read、SessionRoute bulk read、DueIndex discovery、Targeted recheck 的 CPU/bytes/P99；
- `full/narrow`、`pubsub/direct` 对照必须在同一数据集和同一 Redis 状态下执行。
# 9. 扩展性、N+1 与 Pod 数推导
## 9.1 1→2 Pod 扩展测试
单 Pod 容量后，对 Gate、Player、Farm、Worker 分别进行 1→2 Pod 对照，保持总资源、数据分布和压测器不变。
| 指标 | 计算 |
| --- | --- |
| 吞吐扩展效率 | throughput_2pods / (2 × throughput_1pod) |
| P99 改善 | (p99_1pod - p99_2pods) / p99_1pod |
| 负载均衡偏差 | max(pod_load) / mean(pod_load) |
| 共享瓶颈 | Redis/网络/Controller 在扩容后是否提前饱和 |


如果 2 Pod 吞吐只提升很少，应先定位共享 Redis、热点 Shard、单 FarmActor、Gate 下行、全局锁或负载倾斜，而不是继续堆 Pod。
## 9.2 N+1 规则
```text
N+1 候选容量规则
总峰值负载 <= (N - 1) × 单Pod安全容量

等价地，正常均衡时单Pod负载占安全容量：
N=2 → ≤50%
N=3 → ≤67%
N=4 → ≤75%
```


这是比“所有 Pod CPU 固定 70%”更有意义的规则。最终仍要同时满足 CPU、内存、网络、Redis、队列、P99 和正确性约束。
## 9.3 Shard 与 Pod 的关系
Shard 数用于路由粒度和迁移，不直接等于 Pod 数，也不等于活跃 Actor 数。容量报告应同时给出：
- 每 Pod 平均 Shard、活跃 Shard、活跃 Actor；
- Shard 负载分布 P50/P95/P99 与最热 Shard；
- 单 Shard 迁移耗时和一轮可并行迁移数；
- 扩缩容时需要移动的 Shard 数、总时间和剩余容量。
# 10. 瓶颈定位与优化决策
## 10.1 何时值得优化
| 情况 | 是否立即优化 | 理由 |
| --- | --- | --- |
| 单接口 QPS 低，但全局需求更低且可水平扩展 | 否 | 成本和副本数合理即可 |
| 单热点 Actor 无法横向扩展，P99/队列不达标 | 是 | 影响 200 人同场和热点体验 |
| CPU 不高但吞吐上不去 | 是，先定位 | 可能是 Redis、锁、Mailbox、网络或 Pool wait |
| 复杂 Workflow QPS 低但满足业务峰值 | 不一定 | 重点看 P99、写放大、恢复与正确性 |
| 扩容效率明显低于预期 | 是 | 存在共享瓶颈或倾斜 |
| 正确性硬门非零 | 必须先修 | 性能数据无效 |


## 10.2 定位工具
| 症状 | 优先检查 |
| --- | --- |
| CPU 高 | pprof CPU、热点函数、序列化、哈希、日志、重复编码 |
| CPU 低但 P99 高 | Redis RTT/Pool wait、gRPC queue、Actor mailbox、锁、block profile |
| RSS 线性增长 | heap profile、Actor/Receipt/JoiningBuffer/VersionRing 生命周期 |
| goroutine 增长 | Watch/Dial/Timer/Worker retry/ConnectionWriter 泄漏 |
| Farm P99 高 | 单 Farm mailbox、Actor Turn、FarmChange 编码、SubscriptionHub |
| Worker backlog | create rate、step rate、retry amplification、OwnedShards 倾斜、Opportunity due lag |
| 好友机会慢 | Header模式、SessionRoute批读、在线比例、Gate队列、H5合并/定向查询 |
| 扩容不线性 | Redis/网络/Controller 共享瓶颈、Shard 倾斜、热点 Farm |


## 10.3 优化后回归
- 同一 Commit 基线只改变一个优化变量；
- 重复相同 Run 3 次；
- 同时比较 throughput、P99、CPU、内存、写放大和正确性；
- 优化不能把成功点前移到未持久化内存，也不能关闭 Fence/Receipt；
- 若优化改变流量模型或协议，需要重新走功能资格和 Evidence Review。
# 11. Agent 并行编排
## 11.1 推荐角色
| Agent | 职责 | Owned 边界 | 何时启动 |
| --- | --- | --- | --- |
| I0 集成控制 | worktree、锁、HEAD、Push、Checkpoint、Run 排期 | 原则上不改生产代码 | 立即 |
| D1 Dynamic R2 Writer | 提交 R1 closure、发布 R2 镜像、执行 Bootstrap/G1 基线 | `scripts/local-k8s/**`、dynamic profile tooling | 立即 |
| FB1 Friend Backend Writer | 合入 `3a48228c...`、V2 fixture、shadow mismatch、OwnedShards 接缝 | friend/opportunity/worker 窄写集 | R2 并行做审计，R2 后最终合流 |
| FH1 Friend H5 Writer | 合入 `787dc00`、Hint 合并与 CandidateWindow 回归 | H5 friend opportunity 窄写集 | 立即审计，R2 后最终合流 |
| C1 Product Convergence | 唯一负责 Dynamic exact tip + Friend Backend/H5 组合 | composition/merge seam 单 Writer | R2 基线后 |
| Q1 G123/Scenario | G1/G2/G3、F1/F2/F3、好友机会 Oracle/Evidence | 只改测试/工具 | 立即准备，C1 后正式运行 |
| H1 Harness Writer | Go Bots、开放到达、连接分片、planned/actual | 只改压测工具 | 立即 |
| O1 Observability | 逐 Pod metrics、pprof、Trace、Evidence Reporter | 只读或窄接缝 | 立即 |
| RO1 Run Operator | 正式环境部署、一次一 Run、故障执行、归档 | 不提交业务代码 | R2 与各 Checkpoint |
| A1 Capacity Analyst | 拐点、safe capacity、Pod 数、报告 | 不改被测代码 | 有 Evidence 后 |
| RV Reviewer | detached read-only 复核代码、配置、Evidence | 不改代码 | 每个 Checkpoint 后 |

## 11.2 第一批并行任务

```text
D1：R1 closure + R2
FB1：Backend diff/ICR、V2 fixture与OwnedShards测试准备
FH1：H5 diff/ICR与合流准备
Q1：G1/G2/G3 + 好友机会 Evidence schema
H1/O1：Harness与指标
```

最多保持 4～5 个编码 Agent。正式 R2/G1/G2/G3 或性能 Run 开始后，其他 Agent 不得在同一宿主机执行全仓测试、镜像构建或高 CPU 任务，只能做轻量编辑、离线分析或等待。

## 11.3 依赖与唯一写者

```text
D1 R2 PASS
→ C1 创建产品合流候选
→ FB1/FH1 产物进入 C1
→ RV Exact Review
→ Q1 G1最终重跑
→ Q1 G2/G3
→ Full Product
→ H1/O1/RO1正式压测
```

唯一写者约束：

- Dynamic Profile/bootstrap 只由 D1 修改；
- Friend Backend 只由 FB1 修改；
- H5 只由 FH1 修改；
- 最终 Composition/Merge Seam 只由 C1 修改；
- Q1 不得为了让场景 PASS 修改生产代码；
- RO1 是正式集群和 Redis Run 锁唯一持有者。

## 11.4 可以并行的工作
- R2 代码闭合与 Friend Backend/H5 的只读审计、测试准备可以并行；
- 流量模型、Harness、Metrics、Oracle 可以并行开发；
- 已结束 Run 的 pprof/Trace 分析可以与下一轮工具开发并行；
- Backend 与 H5 独立 Review 可以并行，但最终产品合流由 C1 单写。

## 11.5 必须串行的工作
- 同一宿主机上的正式 R2/G1/G2/G3/压力 Run；
- 同一 kind 集群上的单 Pod 极限测试；
- 同一 Redis 的容量或故障场景；
- Planned Migration、Crash Takeover、Controller/Gate/Worker Kill；
- 镜像构建、全仓测试与正式 Run 不能同时争抢宿主机资源；
- 同一 Composition Root、RoutePlan、Actor Runtime 或 Worker Assignment 接缝的生产修改。

## 11.6 Agent 阶段编排
| 阶段 | 并行 Agent | 唯一 Owner |
| --- | --- | --- |
| R2 修复 | D1 + FB1/FH1/Q1/H1/O1 轻量准备 | D1/RO1 占集群锁 |
| 产品合流 | FB1 + FH1 交付，C1 单写合并 | C1 |
| G1/G2/G3 | Q1 + O1，H1 支持 | RO1 一次一场景 |
| 功能资格 | Q1 + O1 + FH1 页面见证 | RO1 |
| 单链路/单 Pod | H1 + O1 + A1 | RO1 |
| 混合/热点 | H1 + Q1 + O1 | RO1 |
| 故障带载 | Q1 + O1 + H1 | RO1 / Fault Controller |
| 报告 | A1 + Reviewer | I0 冻结结论 |

所有任务继续遵守：一个任务一个 worktree、一个分支、一条主 Writer Thread；Agent 不自行 Push，不自行合 Integration，Finding 退回原 Writer。

# 12. Checkpoint、PASS/FAIL 与交付物
## 12.1 Checkpoint
| Checkpoint | 通过条件 |
| --- | --- |
| CP-DYNAMIC-R2-BOOTSTRAP | fresh identity、无 Holder Restart、8/8 Route/Authority/Boot、失败 receipt 完整 |
| CP-FRIEND-OPPORTUNITY-DYNAMIC-CONVERGENCE | Backend/H5 exact merge、V2 fixture、shadow mismatch=0、2 Worker OwnedShards PASS |
| CP-G1-REMOTE-WATCH | 最终产品候选四类 Consumer、重连/full resync、跨旧 Deadline、local-only resolve |
| CP-G2-PLANNED-MIGRATION | PLAYER/FARM 选定 Shard A→B→A→B、Drain/Verify/单写者 |
| CP-G3-CRASH-TAKEOVER | Proof-gated takeover、早期拒绝、旧写=0、业务恢复 |
| CP-ROUTE-CONTROL-MVP | G1/G2/G3 + detached review P0/P1/P2=0 |
| CP-LOAD-TOOLS-QUALIFIED | planned/actual、压测器资源、Fault、Oracle、Evidence 自证 |
| CP-FULL-PRODUCT-FUNCTIONAL | H5/Gate/Player/Farm/Worker/Route + F1/F2/F3 + 好友机会 + Authority Oracle |
| CP-FRIEND-OPPORTUNITY-CAPACITY | 300 好友 Header/Hint/H5/OwnedShards/Redis/Worker 容量与边界 |
| CP-SINGLE-POD-CAPACITY | Gate/Player/Farm/Worker/Redis 五类安全常数 |
| CP-HOT-FARM-200 | 1+199、FarmVersion 连续、Slow Reader 隔离 |
| CP-DYNAMIC-FAULT-UNDER-LOAD | Controller/迁移/Crash/Worker/Gate 故障带载 |
| CP-LOCAL-SOAK | 30min 调试 + 2h 正式，无泄漏/积压/P99 漂移 |
| CP-30M-DAU-CAPACITY-MODEL | 假设、实测常数、N+1 Pod 数、限制与风险完整 |


## 12.2 正确性硬门
```text
Hard Correctness Gates
任一非零，本轮性能结果无效：

duplicate_grant_total
duplicate_debit_total
stale_holder_commit_success_total
farm_version_gap_total
invalid_order_total
step_unknown_residual
due_residual
operation_terminal_unknown_residual
legacy_route_fallback_total
partial_snapshot_publish_total
deadline_rollback_total
membership_churn_due_to_deadline_total
worker_claim_while_route_stale_total
stale_stream_callback_commit_total
friend_header_shadow_mismatch_total
friend_hint_wrong_fence_delivery_total
friend_opportunity_duplicate_claim_total
owned_shard_overlap_total
owned_shard_gap_total
candidate_window_stale_after_targeted_refresh_total
```


此外还要对账 negative wallet/inventory、stolen amount over limit、theft penalty diff、task/mail claimed without reward、Receipt hash conflict 等领域指标。
## 12.3 性能 PASS 的分层
| 等级 | 含义 |
| --- | --- |
| BASELINE_RECORDED | 负载有效、正确性通过，已记录 P50/P95/P99，但尚未冻结 SLO |
| MAX_STABLE_FOUND | 已找到稳定窗口的最大吞吐 |
| SAFE_CAPACITY_FROZEN | 加入 N+1、抖动和故障余量后的容量已冻结 |
| SCALE_EFFICIENCY_PASS | 1→2 Pod 扩展趋势符合预期，未出现共享瓶颈失控 |
| LOCAL_DYNAMIC_ACCEPTANCE_PASS | 动态路由、业务、热点、故障、Soak 全部通过 |


## 12.4 最终容量报告应包含
- 全部产品假设与峰值因子，明确哪些来自题目、哪些是候选、哪些已实测；
- 每个关键链路的 breakpoint、max_stable、safe_capacity；
- Gate/Player/Farm/Worker/Redis/Route 的瓶颈和优化记录；
- 3M 设计 PCU 与 5M 压力 PCU 下的 Pod 数和 N+1 余量；
- 热点 Farm、Join 风暴、故障迁移和 Slow Reader 的独立容量；
- 本地单机无法证明的平台项，如多机网络、生产 Redis 持久化、正式 TLS/CLB；
- 答辩口径：真实压测得到容量常数，再用模型推导 3000 万 DAU，而非虚报全量实测。
# 附录 A：Traffic Profile v0.1 模板
以下比例是【本计划建议】的起点，不是已有运营数据。正式 mixed profile 应在单链路成本和产品行为确定后冻结。建议先运行三个 Mix，而不是只跑一个。
| 命令类别 | personal-heavy | social-heavy | join-heavy |
| --- | --- | --- | --- |
| 登录/换票/WSS | 5% | 5% | 15% |
| Join/SwitchFarm | 10% | 20% | 35% |
| FriendList/全局机会恢复 | 8% | 15% | 8% |
| Hint后定向机会精查 | 4% | 10% | 4% |
| Bootstrap/其他只读 | 13% | 5% | 8% |
| Buy/Sell/Task/个人写 | 20% | 10% | 8% |
| Plant/Harvest | 25% | 10% | 8% |
| Care | 5% | 10% | 5% |
| Theft | 3% | 10% | 4% |
| OperationResult/恢复 | 7% | 5% | 5% |


心跳、Subscription Renew、Route Watch 不计入业务命令 Mix，必须单独作为每连接/每 Shard 的背景负载加入。
# 附录 B：运行配置模板
```text
run.yaml 示例
run_id: <UTC timestamp>
scenario: <T0..T17>
profile: <personal-heavy|social-heavy|join-heavy|hot-farm|friend-opportunity-ab>
backend_commit: <sha>
h5_commit: <sha>
harness_commit: <sha>
manifest_sha256: <sha256>
redis_profile: local-isolated
friend_opportunity:
  header_mode: <full|narrow-shadow|narrow>
  transport_mode: <pubsub|shadow-direct|direct>
  friend_count: <20|100|300>
shards: PLAYER=4, FARM=4
pods:
  gate: 1
  player: 2
  farm: 2
  worker: 2
  route_controller: 1
load:
  planned_connections: <n>
  planned_arrivals_per_sec: <n>
  duration: <s>
fault:
  type: <none|controller_restart|migration|crash|worker_kill|gate_restart>
  at: <offset>
validity:
  actual_over_planned_min: 0.98
  correctness_incidents: 0
```


# 附录 C：Agent 任务卡摘要
| 任务 | Owned Write Set | 禁止项 | 完成输出 |
| --- | --- | --- | --- |
| D1 R2 | dynamic profile scripts/tooling | 不改业务状态机 | closure commit、R2 receipt、G1基础证据 |
| FB1 Friend Backend | friend/opportunity/worker窄写集 | 不改RoutePlan/Controller | V2 fixture、shadow metrics、OwnedShards tests |
| FH1 Friend H5 | H5 friend opportunity | 不改后端协议语义 | Hint合并、CandidateWindow、浏览器回归 |
| C1 Product Convergence | merge seams | 不新增第二套Runtime | exact combined candidate、全仓Gate |
| Q1 G123/Oracle | fault/oracle/fixtures | 不为PASS修改生产逻辑 | G1/G2/G3、好友机会、Authority/Client verdict |
| TM1 流量模型 | docs/perf-model/**、profile configs | 不改生产代码 | 公式、参数来源、Profile |
| H1 Harness | cmd/ws-stress、loadtest、testkit | 不改业务实现 | planned/actual、Bot、开放到达 |
| O1 Observability | metrics collector、evidence reporter | 不改业务语义 | 逐Pod指标、pprof、Evidence |
| RO1 Run Operator | scripts/run、环境编排 | 不提交业务代码 | 唯一Run、原始证据、清理 |
| A1 Capacity Analyst | reports/notebooks/docs | 不改被测代码 | 安全容量、Pod数、风险 |

# 附录 D：依据文档与边界
- 《60 · 接入与会话测试、容量验证与演示验收冻结》：测试体系、200 人口径、planned vs actual、未冻结容量参数。
- 《61 · 性能、弱网恢复与平滑更新统一方案》：五类容量常数、负载与故障分离、连续体验目标。
- 《22 · 经典农场后台统一生产架构》：四部署单元、双 Actor、200 人热点与 3000 万 DAU 边界。
- 《26～32 号合同》：Session/Join、命令路由、RPC、Writer Fence、Redis、Plant/Harvest/Care/Theft 成功语义。
- 《71～73 号多 Agent 文档》：一任务一 worktree、一主 Writer、测试 Agent 不修改业务实现、I0 统一合并。
- 当前 G21/G22/RT2Q 交接：2048 Shard 路由存活、Remote Watch 重连 Finding、动态 RoutePlan/G1/G2/G3。
# 附录 E：从当前状态到正式压测的控制板

| 顺序 | Owner | 任务 | 可并行项 | 正式输出 |
| --- | --- | --- | --- | --- |
| 1 | D1 | 审查并提交三处 R1 closure；全量 Gate | FB1/FH1/Q1/H1/O1 轻量准备 | signed exact head |
| 2 | RO1/D1 | 新身份 R2 apply | 其他 Agent 禁止重负载 | CP-DYNAMIC-R2-BOOTSTRAP |
| 3 | Q1 | R2 基础 G1 | FB1/FH1 完成合流产物 | infrastructure baseline |
| 4 | C1 | Dynamic + Friend Backend/H5 convergence | Reviewer 可分边界只读 | CP-FRIEND-OPPORTUNITY-DYNAMIC-CONVERGENCE |
| 5 | Q1/RO1 | G1 最终重跑，跨旧 Deadline | O1 采集 | CP-G1-REMOTE-WATCH |
| 6 | Q1/RO1 | G2 A→B→A→B | 只读分析可并行 | CP-G2-PLANNED-MIGRATION |
| 7 | Q1/RO1 | G3 Crash Takeover | 只读分析可并行 | CP-G3-CRASH-TAKEOVER |
| 8 | I0/RV | Exact Review/Freeze | 无生产写者 | CP-ROUTE-CONTROL-MVP |
| 9 | Q1/FH1 | Full Product、F1/F2/F3、好友机会 | H1/O1 | CP-FULL-PRODUCT-FUNCTIONAL |
| 10 | H1/O1/RO1 | T0～T17、单Pod、混合、热点、故障、Soak | A1 离线分析 | 各容量Checkpoint |

## E.1 正式压测前必须绑定的源码

```text
Backend exact tip
= Dynamic G123 final
+ Friend Backend 3a48228c... 的审查后合流结果

H5 exact tip
= 当前最终H5基线
+ 787dc00 的审查后合流结果

Harness exact tip
= G1/G2/G3 + T0～T17工具
```

不得把独立分支 SHA 直接当作最终运行 SHA；Evidence 必须绑定合流后的 exact tip、镜像 ID、Pod imageID、Manifest、TLS 和 Bootstrap Receipt。

## E.2 优化是否值得保留的判定

好友机会优化只有同时满足以下条件才进入最终主链：

- V1/V2 Header 结果与 full 一致；
- narrow-shadow mismatch=0；
- Direct Fence 错误投递=0；
- 2 Worker OwnedShards 无重叠/空洞，G2/G3 后能重新收敛；
- 混合负载下 Redis CPU、Authority Slot P99、Due Lag、Gate Queue 和 H5 Query Rate 不恶化到超过安全容量；
- 端到端弹窗体验保持当前量级或更好。

若只在微基准快、但混合负载或迁移时不稳定，应回到 shadow 模式，不得为了一个局部延迟数字牺牲正确性。

> 最终边界  
> 本计划可以产出本地真实容量常数、热点与故障证据，并据此推导 3000 万 DAU 所需资源；它不能替代正式多机、生产网络和生产 Redis 的平台验收。
