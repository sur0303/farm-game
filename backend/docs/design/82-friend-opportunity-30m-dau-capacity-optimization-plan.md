# 82 · 好友列表与互动弹窗 3000 万 DAU 容量优化实施计划

版本：`v0.3-c6-implemented`
日期：`2026-08-12`
基线：后端 `codex/friend-opportunity-optimized-r1@3a48228`；H5 `codex/h5-friend-opportunity-fusion-r1@787dc00`
工作分支：`codex/friend-opportunity-30m-capacity-r1`
状态：`C0-C2/C4-C6 IMPLEMENTED / C3 DATA-DRIVEN NO-GO / MULTI-POD CAPACITY EVIDENCE PENDING / NOT_MERGED_TO_INTEGRATION`

实施记录：

| 阶段 | 当前结论 | 容量分支提交 |
|---|---|---|
| C0 | 聚合观测、20/100/300 好友证据矩阵完成 | `d601c7c`、`6c71322`、`c7cbc17` |
| C1 | CandidateWindow Top-K 有界模式完成，保留 Feature Flag 回退 | `0d82e9e` |
| C2 | SessionRoute 20/100/300 容量探针完成；现有小 Hash 下暂不引入 OnlineRouteBucket | `edadd6e` |
| C3 | 暂不增加服务端第二层 Hint 合并；H5 已有 300ms 合并，现有证据不足以证明新增状态表收益 | 无代码提交 |
| C4 | Worker 按冻结 Assignment Snapshot 扫描 OwnedShards，迁移前 Claim 复核完成 | `59ae246` |
| C5 | 同槽专用 ScheduleDueIndex、迁移回填、探针与独立审阅完成 | `d8f9502` |
| C6 | Redis 三域独立 Client/Pool、服务注入、Compose 回退；Gate 每连接容量、Hint 软配额、排空量子与慢连接 Harness 已完成 | `66a7318`、`1d5af0d`、`6b619d8`、`0c822d2`、`56f3b1c`、`71d54a0` |

以上提交仅存在于 `codex/friend-opportunity-30m-capacity-r1`，尚未合入
`Integration`、尚未推送远端。单节点证据不等于 3000 万 DAU 容量证明。

## 0. 一页结论

当前版本已经证明：单个玩家拥有 300 个好友时，真实公司 Redis、真实 Gate/Player/Farm/Worker、真实浏览器链路可以正确运行，并且在线 Hint 可以在约 2 秒内形成弹窗。

它尚未证明：数十万到上百万玩家同时在线时，登录冷扫描、好友分页、SessionRoute 批读、Worker 到期扫描和 Gate 下行能够共同满足容量目标。

本计划不推翻当前方案。生产主干继续冻结为：

```text
Authority Header/Facts
+ 一座农场一个最早 Wakeup
+ 1 秒 Worker DueIndex 扫描
+ SessionRoute Direct DeliverBatch
+ H5 300ms Hint 合并与定向精查
+ 登录/回家/可见性/fallback Pull
```

优化顺序遵循：

```text
先补聚合负载证据
→ 再降低 CandidateWindow 和 SessionRoute 的聚合成本
→ 再完成 Worker 动态分片与独立 ScheduleDueIndex
→ 最后进行多 Gate、多 Worker、多 Redis 节点容量验收
```

当前不引入：

- Redis Pub/Sub 生产主链；
- 每个好友一条持久通知 Inbox；
- FarmOpportunitySummary 第二份异步真值；
- 每块土地一个常驻 Timer；
- 为容量测试改变 FarmActor 最终裁决语义；
- 未经数据证明的完整 Proxyless Mesh。

## 1. 已有基线与不可重复建设项

### 1.1 已完成能力

- 好友列表只精确检查当前页最多 20 人；
- 300 好友全局导航采用 Header 粗筛，最终候选仍走 Pair/Profile/Facts/Receipt 精查；
- Header 窄读真实公司 Redis 基准：300 份约 `6.2ms`、300 条命令、5 次 Pipeline 往返；
- CandidateWindow 已持久在 Redis，带 CAS、30 分钟 TTL、8 个热候选和有界 pending tail；
- 在线 Hint 已采用 SessionRoute Direct Transport，不依赖 Redis Pub/Sub；
- Session 下行使用完整 TargetFence，旧 Gate、旧连接、旧 Session 会被拒绝；
- H5 已按 `target_owner_id + opportunity_revision` 去重，并在 300ms 内合并 Hint；
- Worker 已有 Static OwnedShards、Claim/Discovery/并发/轮次时间预算和轮转公平性；
- 当前静态部署只允许一个 Worker 扫描全部 FARM Shard，避免重复扫描。

### 1.2 已验证但不采用的优化

Session Hash 当前只有约 12 个字段。此前将 300 个 `HGETALL` 改为 9 字段 `HMGET` 后，请求端重复发送字段名，真实总字节没有改善，因此不能把“换成 HMGET”重新列为既定优化。

轻量 Summary 的 300 好友微基准约 `5.2ms`，只比 Authority Header 窄读快约 `1ms`，却增加写入、延迟一致性、重建和第二份语义治理。当前不实施 Summary；只有混合负载证明 Authority 读写竞争成为主瓶颈时才重新评估。

## 2. 3000 万 DAU 负载模型

### 2.1 当前产品假设

采用用户已给出的估计：

```text
DAU                         = 30,000,000
每人每天登录次数             = 5～6，基准取 5.5
每次在线时长上限             = 5 分钟
好友上限                     = 300
同场可操作成员上限           = 200
```

由此得到用于压测的第一版容量模型：

```text
平均新会话速率
= 30,000,000 × 5.5 / 86,400
≈ 1,910 次/秒

按 5 分钟会话估算的平均同时在线
= 1,910 × 300
≈ 573,000

采用 3 倍峰值系数时
峰值新会话 ≈ 5,730 次/秒
峰值同时在线 ≈ 1,720,000
```

5 分钟是当前上限而非已观测平均值，因此以上并非生产结论。压测必须支持修改会话时长、峰值系数和好友分布，不能把单一估算写死进代码。

### 2.2 五条独立容量链

| 链路 | 放大方式 | 当前主要风险 |
|---|---|---|
| 登录/回家全局导航 | 新会话 QPS × 好友数 | 300 Header 冷扫聚合成 Redis Authority 读压力 |
| 好友列表分页 | 打开率 × 每页 20 人 | Pair/Profile/Facts 批读和滚动翻页突发 |
| 在线机会 Hint | 农场机会变化数 × 好友数 | 最多 300 份 SessionRoute HGETALL 与 Gate 扇出 |
| Worker 到期扫描 | FARM Shard 数 ÷ 轮询间隔 | 空 Shard 扫描、共享 DueIndex 前缀、到期积压 |
| WSS 下行 | 同时在线数 + Hint/Delta 扇出 | Gate 连接内存、有界队列、慢连接和滚动更新 |

### 2.3 必须参数化的流量变量

容量工具至少暴露：

- 好友数：`20 / 100 / 300`；
- 好友列表打开率：`10% / 30% / 50%`；
- 在线好友比例：`1% / 5% / 20% / 50%`；
- 每活跃农场机会变化：`0.1 / 1 / 3 次每会话`；
- CandidateWindow：冷启动、热命中、耗尽、好友版本变化；
- FARM Shard：`64 / 256 / 1024 / 4096 / 16384`；
- Gate 数量、Worker 数量、Redis 节点数量；
- 弱网比例、慢连接比例、Hint 丢失率。

不得假设每个玩家都有 300 个好友，也不得只用平均好友数掩盖 300 好友上限路径。

## 3. 正确性不变量

所有优化必须继续满足：

1. Header、CandidateWindow、在线路由桶和 Hint 都不是最终业务授权；
2. 是否仍为好友必须校验 canonical FriendRelation 和 relation_epoch；
3. 偷菜必须读取玩家维度 TheftAttemptReceipt；
4. 最终操作仍由 FarmActor 串行裁决；
5. 作物成熟只由 `trusted_now_ms >= mature_at_ms` 决定；
6. Scheduler 故障只影响提醒及时性，不改变成熟和经营正确性；
7. Hint 允许重复或丢失，客户端必须通过 Pull 恢复；
8. 旧 SessionRoute、旧 Gate Boot、旧连接和旧 generation 不能接收新下行；
9. Redis UNKNOWN 不得伪装成 NONE；
10. 任何查询优化均不得阻塞 FarmActor Mailbox 等待网络或大批量 Redis 读取。

## 4. 目标架构

```text
Farm权威提交
  ├─ 更新Header/Facts
  └─ 原子登记整座Farm最早Wakeup
             │
             ▼
Dedicated ScheduleDueIndex（演进后）
             │
             ▼
farm-worker（按FARM Shard动态分配）
  ├─ 1秒有界扫描
  ├─ generation校验
  ├─ 同Farm短窗口保留最高revision
  └─ 批量查询在线SessionRoute候选
             │
             ▼
按gateway_id分组 DeliverBatch
             │
             ▼
Gate完整TargetFence校验 → WSS Hint
             │
             ▼
H5 300ms合并 → hinted target定向精查
             │
             ▼
CandidateWindow Top-K更新 → 一张当前推荐卡

登录/回家/前台恢复/fallback
  → CandidateWindow热命中优先
  → 必要时300 Header冷扫
  → Top-K Facts精查
```

## 5. 分阶段实施

## C0：冻结基线并补齐聚合负载观测

实施状态：`DONE`。

### 目标

先知道每条链路的真实 Redis、CPU、网络和内存成本，不在缺少证据时直接增加新投影。

### 实施内容

1. 为以下阶段增加结构化耗时：
   - FriendIndex；
   - Header cold scan；
   - CandidateWindow load/save；
   - exact Pair/Profile/Facts/Receipt；
   - SessionRoute batch；
   - Gate grouping/enqueue/deliver；
   - Worker discovery/claim/recompute/fanout。
2. 记录命令数、请求/响应字节和 Redis 节点分布，禁止以 `player_id`、`farm_id`、`operation_id` 作为 Metrics Label。
3. 新增容量报告中的派生指标：
   - `cold_scan_rate`；
   - `candidate_window_hit_rate`；
   - `candidate_window_payload_bytes`；
   - `session_route_targets_per_hint`；
   - `active_routes_per_hint`；
   - `gate_groups_per_hint`；
   - `due_lag_ms` 和 `budget_exhausted_rate`。
4. 固化当前 `3a48228` 的 300 好友真实链路结果作为 A 组，不修改业务默认值。

### 验收

- 同一请求可分解出各阶段耗时，阶段总和可解释端到端耗时；
- 所有计数有上限，不产生高基数指标；
- 观测关闭时无明显性能回退；
- 后续每个优化都能和 A 组做相同流量矩阵对比。

## C1：CandidateWindow 物理成本优化

实施状态：`DONE / TOP-K FEATURE-FLAGGED`。

### 当前问题

当前 Redis Navigation 保存：

```text
8个热CandidateWindow
+ 最多约300个pendingCandidates尾部
+ visited/fence
+ JSON payload
+ 30分钟TTL
```

它让热查询避免再次扫描 300 Header，但在数百万活跃导航状态下可能形成显著 Redis 内存、网络和 Save CAS 写放大。

### A/B 方案

```text
CW-A：当前 Full Tail
  保存完整有序pending tail

CW-B：Top-K Only
  保存Top 16或Top 32 + visited + roster/source revision
  窗口耗尽后重新冷扫300 Header并排除visited

CW-C：Compact Wire
  语义与CW-B相同，改为有版本的紧凑二进制编码
```

第一轮只实现 `CW-B`，不同时改编码，避免把语义优化和序列化优化混在一起。

### 选择标准

- 单 Navigation P50/P95/最大字节；
- Redis `MEMORY USAGE`；
- Load/Save P95/P99；
- 窗口耗尽后额外冷扫次数；
- 访问 1、3、8、16 座好友农场时的总 Redis 命令与总延迟；
- 卡片顺序、visited 语义和 UNKNOWN 语义完全一致。

### 回退

```text
FRIEND_NAVIGATION_WINDOW_MODE=full-tail|top-k
```

不删除旧解码器；滚动发布期间必须同时读取旧版和新版 payload。

## C2：SessionRoute 聚合查询优化

实施状态：`C2.1 DONE / C2.2 NO-GO FOR NOW`。当前 Session Hash 很小，实测不足以支持引入第二份 OnlineRouteBucket；保留后续条件触发入口。

### 当前基线

Direct Hint 对最多 300 个好友执行：

```text
Redis Pipeline
  TIME
  HGETALL session:{friend_1}
  ...
  HGETALL session:{friend_300}
```

Session Hash 很小，简单改成 9 字段 HMGET 已被真实实验否决。本阶段先测并发聚合成本，再决定是否引入查询投影。

### C2.1 必做：并发批读与节点级证据

- 统计 20/100/300 targets 下的命令、字节、Redis CPU 和 P99；
- 统计 1/5/20/50% 在线比例；
- 统计按 Gateway 分组后真实 DeliverBatch 数量；
- 验证 Redis Cluster 客户端是否按节点 Pipeline，而不是串行逐 Slot；
- 保留当前 HGETALL 作为 A 组。

### C2.2 条件实施：OnlineRouteBucket 查询投影

只有 C2.1 证明 SessionRoute 成为主要瓶颈时才实现：

```text
OnlineRouteBucket[bucket]
├─ Hash: player_id → compact route candidate
└─ ZSet: player_id → lease_expire_at_ms
```

候选桶数测试 `64 / 128 / 256`，通过好友 ID 分桶后使用少量 `HMGET`。它只负责快速发现可能在线的目标：

- Session 激活、续租、踢下线时更新投影；
- 投影更新失败最多导致 Hint 延迟或重复，不影响登录权威；
- stale 投影由 lease 时间、清理器和后续心跳收敛；
- 最终 Gate 仍使用完整 TargetFence 拒绝旧路由；
- Pull 仍能恢复漏 Hint；
- 不允许用投影代替 SessionAuthority。

### 回退

```text
FRIEND_OPPORTUNITY_ROUTE_READ_MODE=session-hash|bucket-shadow|bucket
```

Shadow 只比较候选在线目标和路由 Fence，不重复向 H5 下发 Hint。

## C3：服务端 Hint 合并与扇出预算

实施状态：`DATA-DRIVEN NO-GO`。客户端已有 300ms 按目标/revision 合并，当前观测没有证明再增加 Worker 进程内状态表能显著降低 Route 查询；先保留指标和未来开关，不增加生产复杂度。

### 目标

H5 已在 300ms 内合并 Hint，但重复 Hint 在到达 H5 前已经消耗 SessionRoute、Dispatcher 和 Gate 资源。增加有界的服务端最佳努力合并。

### 设计

```text
key = farm_id
value = max(opportunity_revision) + latest boundary
window = 200～500ms，可配置
```

- 只在 farm-worker/Hint Dispatcher 侧合并，不写入业务 Authority；
- 同一 Farm 只保留最高 revision；
- 合并表有最大 farm 数、最大字节和逐出策略；
- 进程崩溃允许丢失尚未发送的 Hint，依靠 Pull 恢复；
- 不延长最终成熟判断，不改变 FarmActor；
- 队列满时优先保留最新 revision，记录 drop/coalesce 指标。

### 验收

- 同一农场 100ms 内多次 WATER/MATURE/PEST 变化只产生一次 SessionRoute 批读；
- 不同农场可以并行；
- 正常单事件增加的延迟不超过配置窗口；
- Hint 丢失后 B2 fallback 可以恢复；
- `route_lookups / authority_opportunity_change` 明显下降时才默认开启。

## C4：Worker 动态 Shard Assignment

实施状态：`DONE / MULTI-POD EVIDENCE PENDING`。

### 当前问题

当前 Static Provider 单 Worker 拥有全部 FARM Shard，正确但不能水平扩容。直接把副本数从 1 改成 N，会导致所有 Worker 扫描同一批 Shard。

### 设计

```text
RouteAssignmentManager
→ FARM_WORKER Route Domain
→ shard_id → worker_holder + assignment_revision
→ 每个Worker本地Atomic Snapshot
→ OpportunityRecovery只扫描OwnedShards
```

- FARM Actor 与 FARM Worker 使用同一 VirtualShard 编号空间，但 Holder 类型不同；
- 热路径读取本地 Assignment Snapshot，不每秒查询 Redis；
- Revision 变更时停止领取迁出 Shard 的新任务；
- 已 Claim 工作在 Lease 内完成或自然过期；
- 新 Worker 获得 Assignment 后读取原 DueIndex 继续；
- 不迁移 Schedule 数据和 Worker 内存；
- Assignment 重叠时，WorkLease/generation 仍阻止重复有效结果。

### 验收

- 任意时刻每个 Shard 最多一个正常扫描 Holder；
- `1 → 2 → 4 → 8` Worker 扩缩容不丢 Schedule；
- Drain、崩溃和滚动更新时 due lag 有界；
- 扫描总 QPS 不随 Worker 数量线性重复放大；
- 多 Worker 前不再允许静态配置“全部 Shard”。

## C5：独立 ScheduleDueIndex

实施状态：`DONE / SINGLE-NODE EVIDENCE COMPLETE / CLUSTER EVIDENCE PENDING`。

100 万普通 `wr1_` 成员、5 次隔离 Redis 探针中：共享与迁移兜底模式每次约
65,536 条命令、P50 约 8.7～8.9 秒；专用模式每次 1 条命令、P50 约
0.155ms。共享模式在有界扫描上限内为 0/5 命中。该结果只证明拆索引消除了
无关成员扫描，不证明 3000 万 DAU。

### 当前问题

Schedule 与普通 Order 当前共享 Authority DueIndex。代码已能有界跳过最多 65536 条非 Schedule 前缀，但高规模下仍会浪费扫描命令并产生极端前缀风险。

### 物理方案

在同一 Authority Bucket Hash Tag 内增加：

```text
{authority-bucket}:schedule-due
```

Schedule 记录、Authority 状态和 ScheduleDueIndex 继续由同 Slot Lua 原子提交。

### 迁移

```text
shared
→ dual-write + shadow-read
→ dedicated-read + shared fallback
→ dedicated-only
→ 有界清理旧shared成员
```

- 已存在 Schedule 必须通过扫描/重建补入新索引；
- dual-write 阶段比较 score、generation 和 work identity；
- 不允许先切读再补历史数据；
- 普通 Order 继续使用原 DueIndex，不受本轮影响。

### 验收

- 普通 Order 数量不再影响 Schedule discovery 命令数；
- 100 万非 Schedule 前缀下仍可及时发现到期 Schedule；
- dual/read 切换期间无漏任务和重复有效业务结果；
- Redis Slot 与 Lua Key 合法性测试通过。

## C6：多 Redis 域与 Gate 长连接容量

实施状态：`IMPLEMENTED / SINGLE-PROCESS AND COMPONENT EVIDENCE COMPLETE / MULTI-POD CAPACITY EVIDENCE PENDING`。

本轮已完成两个独立工作包：

- C6A：三个独立 client/连接池，初期允许都回退到同一 `FARM_REDIS_URL`；
- C6B：ConnectionWriter 接纳前 Hint 软配额、FarmChange 保留容量、慢连接矩阵、Drain 与 Pull 恢复。

实现结果：

- `FARM_AUTHORITY_REDIS_URL`、`FARM_RUNTIME_REDIS_URL`、`FARM_PROJECTION_REDIS_URL`
  分别建立独立 go-redis Client/Pool；未配置时各自仅回退到旧
  `FARM_REDIS_URL`，便于单 Redis 开发环境平滑迁移；
- AccountCredential、Player/Farm 强事实、Schedule 和 canonical Friendship
  写入 Authority；SessionAuthority、SessionRoute、Ticket、GateBootLease 写入 Runtime；
  FriendIndex、Navigation、ResultDirectory 和 Hint sidecar 写入 Projection；
- Gate 生产启动链已接入每连接总 item/byte 上限、Hint item/byte 软配额，以及
  WebSocket `DownlinkDrainQuantum`；配置必须在接纳连接前冻结；
- 默认候选值为总队列 `256 items / 1 MiB`、Hint `32 items / 64 KiB`、
  每次排空 `64 frames`。这些是可调起点，不是 3000 万 DAU 的最终定容值；
- 1% / 5% / 20% 慢连接确定性 Harness、Hint/FarmChange/CommandResult 混合流量、
  配额恢复、level-triggered re-notify 和 race 测试已经通过；
- 当前证据仍没有覆盖多 Gate、多 Worker、三个真实独立 Redis 集群与 172 万连接，
  因此不能声明已经完成 3000 万 DAU 验收。

本阶段不是立刻拆库，而是验证拆分边界：

```text
Authority Redis    → Player/Farm强事实、账号凭据、Header/Facts、Schedule
Runtime Redis      → SessionAuthority、SessionRoute、Ticket、GateBootLease
Projection Redis   → FriendIndex、Navigation/CandidateWindow、ResultDirectory、Hint sidecar
```

`PublicProfile` 当前仍由既有 PlayerReadAPI/Authority 读路径提供，本阶段不为拆连接池而
凭空制造第二份资料投影。只有资料读取在混合压测中成为主要瓶颈，才单独设计
PublicProfile Projection、更新顺序和重建合同。

要求 Repository 配置支持分别注入连接，不改变领域接口。先用同一公司 Redis 的不同 namespace 做功能验证，再在容量环境放入不同 Redis 集群。

Gate 侧同时验证：

- 单 Gate 连接数与每连接内存；
- ConnectionWriter 有界队列 item/byte 上限；
- 1%、5%、20% 慢连接；
- Hint 与 FarmChange 并存时的公平性；
- Gate Drain、重启、旧 Boot Fence；
- 队列满时丢 Hint、保留业务结果并由 Pull 恢复。

## 6. 容量压测方案

## 6.1 三层测试

### L1：Repository/Redis 微基准

- Header full/narrow；
- Navigation full-tail/top-k；
- SessionRoute session-hash/bucket-shadow；
- shared/dedicated ScheduleDueIndex；
- 使用公司 Redis 隔离 namespace；
- 记录命令、字节、CPU、连接池等待、P50/P95/P99。

### L2：真实服务链路

```text
Bot/H5
→ HTTPS Login
→ WSS ClientHello
→ JoinFarm
→ GetFriendList / GetNextOpportunity
→ Worker Hint
→ Gate DeliverBatch
→ H5/Bot定向精查
```

必须建立真实账号、真实 FriendRelation、真实 Farm Header/Facts 和真实 SessionRoute，不能用单函数循环冒充端到端容量。

### L3：多进程/多 Pod 容量

- 多 Gate；
- 多 player-svr/farm-svr；
- 多 Worker 动态 Shard；
- 多 Redis 节点；
- HPA/Drain/滚动更新；
- 故障注入和弱网。

## 6.2 流量组合

至少执行：

| 场景 | 登录 | 好友列表 | 全局导航 | Hint | 经营写 |
|---|---:|---:|---:|---:|---:|
| R1 登录高峰 | 60% | 10% | 20% | 5% | 5% |
| R2 社交高峰 | 10% | 40% | 20% | 20% | 10% |
| R3 成熟高峰 | 10% | 10% | 10% | 50% | 20% |
| R4 混合稳态 | 按容量模型 | 按打开率 | 按冷/热比 | 按机会率 | Plant/Care/Theft混合 |

百分比表示请求/事件构成，不代表线上真实比例；最终应由埋点修正。

## 6.3 分级目标

```text
S1：单机正确性，300好友，真实Redis与浏览器
S2：单节点聚合负载，逐步提高到资源70%
S3：多Pod水平扩展，证明吞吐随实例数近似增长
S4：按30M DAU峰值模型缩放验证
```

若测试环境不能真实建立 172 万连接，允许按单 Gate 实测上限推导所需实例数，但必须再做多 Gate 小规模交叉验证，不能只做纯数学外推。

## 7. 验收门槛

以下为首轮候选值，C0 基线后可以调整，但调整必须留下数据和理由：

| 指标 | 候选门槛 |
|---|---:|
| 好友列表首屏 20 人 P95/P99 | `≤150ms / ≤300ms` |
| Hint 定向精查 P95/P99 | `≤50ms / ≤100ms` |
| CandidateWindow 热查询 P95/P99 | `≤50ms / ≤100ms` |
| 300 好友冷导航 P95/P99 | `≤300ms / ≤500ms` |
| 机会边界到在线弹窗 P95/P99 | `≤2s / ≤3s` |
| Worker due lag P99 | `≤2s` |
| Gate Hint queue drop | `<0.1%`，且 Pull 可恢复 |
| 错误跨会话投递 | `0` |
| 错误可操作标识/重复资产 | `0` |
| 稳态 Redis CPU | 建议 `<65%`，突发后可恢复 |
| Worker backlog | 不持续单调增长 |

还必须给出：

- 每个优化前后的 Redis commands/sec；
- 请求与响应 MB/sec；
- Redis 每节点 CPU 和热点 Key/Slot；
- 服务 CPU、RSS、GC、goroutine；
- Gate 每连接内存和队列占用；
- 所需 Gate/Worker/Redis 节点数量及安全余量。

## 8. 工作包与并行边界

| 工作包 | 内容 | 可并行 | 主要冲突文件 |
|---|---|---|---|
| WP-A | C0 指标与容量 Harness | 可独立开始 | observability、scripts、verification |
| WP-B | C1 CandidateWindow Top-K | 可与 WP-C/D 并行 | `internal/friend/opportunity.go`、`navigation_repository.go` |
| WP-C | C2 SessionRoute A/B/可选 Bucket | 可与 WP-B/D 并行 | `internal/gateway/session/`、Direct transport |
| WP-D | C3 Hint 合并器 | 可与 WP-B/C 并行 | `internal/friend/opportunity_direct_transport.go`、worker组装 |
| WP-E | C4 Worker Assignment | 依赖现有 Route Control 合同 | `cmd/farm-worker/`、opportunityrecovery |
| WP-F | C5 ScheduleDueIndex | 与 WP-E 文件重叠，顺序实施 | Redis authority store/worklease/keyspace |
| WP-G | C6 多域配置与 Gate 容量 | 后期并行 | service wiring、deploy |
| WP-H | 最终真实链路与报告 | 依赖全部候选完成 | scripts、evidence、verification |

首批只并行 `WP-A / WP-B / WP-C`。WP-D 必须等 C0 的重复 Hint 数据，WP-E/F 必须单独 worktree，避免同时修改 Worker 和 Redis Authority 核心文件。

## 9. 分支、Worktree 与提交策略

当前计划 worktree：

```text
/data/home/developer_026/CodeBuddy/20260519141303/
farm-game-fo-30m-capacity-20260811
```

当前计划分支：

```text
codex/friend-opportunity-30m-capacity-r1
```

后续代码从该分支再拆：

```text
codex/fo-capacity-observability-r1
codex/fo-candidate-window-topk-r1
codex/fo-session-route-capacity-r1
codex/fo-hint-coalescing-r1
codex/fo-worker-dynamic-assignment-r1
codex/fo-schedule-due-index-r1
```

规则：

- 不直接在 `Integration` 开发；
- 每个工作包一个独立 worktree；
- 不把容量 Harness、业务语义调整和部署改动混在一个提交；
- 每个候选均提供 Feature Toggle 和旧路径回退；
- 先提交原始 JSON/日志，再写结论；
- 未通过真实链路和混合负载前不得合入 Integration；
- H5 当前协议无需变化，若后续确有 H5 改动，另建 H5 worktree，不在后端仓库混改。

## 10. 推荐执行顺序

```text
第1步：WP-A
固定30M流量模型、指标、基线和可重复Harness

第2步：WP-B + WP-C并行
CandidateWindow Top-K A/B
SessionRoute聚合成本A/B

第3步：根据数据决定WP-D和OnlineRouteBucket
重复Hint不高则不做合并器
SessionRoute不是瓶颈则不做Bucket投影

第4步：WP-E
接入动态Worker Shard Assignment并验证扩缩容

第5步：WP-F
迁移到独立ScheduleDueIndex

第6步：WP-G + WP-H
多Redis域、多Gate长连接、真实混合负载和故障验收
```

## 11. Go/No-Go 决策

### 保持当前实现

如果混合负载证明：

- 300 Header 冷扫只占低比例；
- CandidateWindow 内存和 CAS 写不是瓶颈；
- SessionRoute HGETALL Pipeline 可满足目标；
- Worker 1 秒扫描在分片后成本可接受；

则只完成观测、动态分片和容量报告，不为了架构“更复杂”引入 Bucket、Summary 或新缓存。

### 进入优化

只有以下证据出现才启用相应优化：

- Navigation payload/Save 成为 Projection Redis 主要成本 → Top-K/Compact；
- SessionRoute batch 成为 Runtime Redis 主要成本 → OnlineRouteBucket；
- 重复 Hint 明显放大 route reads/downlink → 服务端合并；
- shared DueIndex 导致 schedule discovery 放大 → dedicated index；
- 单 Worker due lag/backlog 超标 → 动态多 Worker。

### 最终完成定义

本计划完成时，应能够回答而不是回避：

1. 3000 万 DAU 模型下需要多少 Gate、Worker、player-svr、farm-svr 和 Redis 节点；
2. 登录冷扫描、好友分页、Hint、Worker 和 WSS 各占多少资源；
3. 哪些路径随好友数线性增长，如何被上限和缓存约束；
4. 单个热门农场、零点登录高峰、批量成熟和 Gate 滚动更新时系统怎样退化；
5. Hint 丢失、Worker 故障和 Redis 局部异常时，多久能由 Pull 恢复；
6. 所有容量结论对应哪一份原始证据，而不是只给出架构推测。
