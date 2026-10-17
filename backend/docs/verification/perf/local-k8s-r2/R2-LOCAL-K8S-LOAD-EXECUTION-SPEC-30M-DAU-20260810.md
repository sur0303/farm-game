# R2 本地 K8s 压测执行规格与 3000 万 DAU 容量模型

状态：`APPROVED_FOR_IMPLEMENTATION_WITH_FINDINGS`

```text
K3S_INSTALL_AUTHORIZED=true
DRIVER_IMPLEMENTATION_AUTHORIZED=true
STATIC_INIT_IMPLEMENTATION_AUTHORIZED=true
LOCAL_REGISTRY_AUTHORIZED=true
LOCAL_FUNCTIONAL_K8S_AUTHORIZED=true

LOW_RATE_REDIS_PROBE_AUTHORIZED=PENDING_REDIS_OWNER
ACTIVE_BINDING_AUTHORIZED=false
L2_STAIRCASE_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
MIXED_AUTHORIZED=false
SOAK_AUTHORIZED=false
LOCAL_LAB_30M_DAU_MODEL_READY=false
```

日期：2026-08-10

目标：`30,000,000 DAU` 的可复算本地容量模型

环境：`LOCAL_LINUX_K8S_WITH_COMPANY_REDIS`

关联计划：`R2-LOCAL-K8S-CAPACITY-LAB-IMPLEMENTATION-PLAN-20260810.md`

> 用户已接受第 20 节候选值作为本地列车规划默认值；这允许 Driver/Lab 实现，不授权 Redis 压力、Formal L2 或 Mixed。本文不是“已经支持 3000 万 DAU”的证明。只有精确 R2 L1、四服务 L2、Mixed 和模型审查全部通过后，才允许发布 `LOCAL_LAB_30M_DAU_MODEL_READY`；该状态仍不等于生产容量认证。

---

## 1. 目标与证据链

### 1.1 最终要回答的问题

1. 一个 4 CPU / 4 GiB Gate Pod 能安全保持多少真实 WSS、处理多少 Login/Join/frame/egress；
2. 一个 Player Pod 能安全处理多少 active actors、activation 和 Player turns/s；
3. 一个 Farm Pod 能安全处理多少 Farm turns/s、subscriptions、Join 和 FarmChange fanout；
4. 一个 Worker Pod 能安全处理多少 Due claims/work steps/s 和多 Family backlog；
5. 四类服务组合后有多少非线性损耗；
6. 把实测常数代入 3M design PCU、5M stress PCU 时需要多少 Pod；
7. 公司 Redis 在该业务放大下是否成为依赖瓶颈。

### 1.2 证明顺序

```text
CAP-REDIS-L1-R2 PASS
→ Driver / Observability Qualification
→ L2-GATE/PLAYER/FARM/WORKER-R2
→ 1/2/4 Pod scale curve
→ CAP-MIXED-B1-R2-LOCAL
→ 30M DAU model
```

不得从 DAU 直接猜单 Pod 参数；不得用一次低水位 Smoke 线性外推。

### 1.3 输出等级

```text
DEV_FAST                         配置调试，无容量资格
LOCAL_L2_SAFE_TIER               单 Pod 本机安全水位
LOCAL_MIXED_BASELINE             本地组合容量
LOCAL_LAB_30M_DAU_MODEL_READY    可复算模型，非生产认证
PRODUCTION_CAPACITY_CERTIFICATION 不在本地实验范围
```

当前继续保持：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```

---

## 2. 3000 万 DAU 输入模型

### 2.1 冻结候选输入

权威输入文件为 `deploy/load/30m-dau-assumption-manifest-v0.1.json`。设计文档只作为来源线索；`docs/design/legacy/18-final-architecture-blueprint.md` 不作为现行权威。除 DAU 目标外，其余业务行为参数均为待埋点回填的 `ASSUMED` 值。Manifest 逐项记录 value/status/source/owner/sample window/count/confidence。

| 参数 | 设计 | 压力 | 性质 |
|---|---:|---:|---|
| DAU | 30,000,000 | 30,000,000 | 目标输入 |
| 每 DAU 会话 | 6/天 | 6/天 | 候选假设 |
| 假设平均每次在线 | 5 分钟 | 5 分钟 | `ASSUMED`，不是“≤5min”的无条件推导 |
| 候选平均在线 | 625,000 | 625,000 | 由假设平均5min推导 |
| PCU | 3,000,000 | 5,000,000 | 设计/压力 |
| 每会话 Join | 3 | 6 | 设计/行为上界 |
| 每会话好友机会查询 | 3 | 3 | 候选假设 |
| 每会话好友页读取 | 1 | 1 | 候选假设 |
| 每 DAU 权威业务操作 | 20/天 | 20/天 | 待埋点回填 |
| 好友平均/上限 | 100/300 | 100/300 | 上限不能当平均 |
| 单 Farm 同场上限 | 200 | 200 | 产品合同 |
| Snapshot P95/Hard | 16/32 KiB | 16/32 KiB | 候选限制 |
| FarmDelta typical/hard | 0.5–1/4 KiB | 同左 | 候选限制 |

### 2.2 会话、Join 与业务包络

```text
每日 Session = 30,000,000 × 6 = 180,000,000
平均 Session start = 180,000,000 / 86,400 ≈ 2,083/s
设计集中峰值（10×）≈ 20,833/s
压力余量（再 ×2）≈ 41,667/s

normal_design_join_qps = 30,000,000 × 6 × 3 / 86,400 × 10 ≈ 62,500/s
normal_stress_join_qps = 30,000,000 × 6 × 6 / 86,400 × 10 ≈ 125,000/s
fault_recovery_join_qps = reconnecting_connections / recovery_window_seconds
例：5,000,000 / 40s = 125,000/s

好友机会查询设计/压力 = 62,500/s / 125,000/s
好友页读取设计/压力（1次/会话）= 20,833/s / 41,667/s

平均业务写 = 30,000,000 × 20 / 86,400 ≈ 6,944/s
设计峰值 = 6,944 × 10 ≈ 69,444/s
压力峰值 = 6,944 × 10 × 2 ≈ 138,889/s
```

PCU 敏感性：3M PCU 每 75 秒一次为 40,000/s；5M PCU 每 40 秒一次为 125,000/s。本规格取更保守包络：

```text
design_business_qps = 69,444/s
stress_business_qps = 138,889/s
```

这些是全局模型输入，不会直接施加给一台本地主机或单 Pod。正常 Join 与故障重连是两类容量问题，禁止再把 joins/session、peak factor 和 recovery factor 盲目连乘。好友页与好友机会查询是独立 read profiles，不能隐含进七类写命令 Mix。

### 2.3 连接与 Snapshot 网络包络

```text
设计连接 = 3,000,000
压力连接 = 5,000,000
```

按 Snapshot P95 16 KiB：

```text
设计 Join egress = 62,500 × 16 KiB ≈ 0.95 GiB/s ≈ 8.2 Gbit/s
压力 Join egress = 125,000 × 16 KiB ≈ 1.91 GiB/s ≈ 16.4 Gbit/s
```

按 32 KiB hard limit 再乘 2，即 125,000 Join/s 时约 3.81 GiB/s / 32.8 Gbit/s。故障恢复网络包络使用独立的 `fault_recovery_join_qps` 重算。该数字只表示全局网络风险，最终按 Zone、Gate 和 Farm Pod 分摊。

---

## 3. 真实 R2 Redis 放大锚点

来源：`CAP-REDIS-L1-R2/postmerge-h5-f22-qualification-20260810/trace/business-command-amplification.json`，Aggregate `aee5b0b3...`。

| Command | Redis ops | RESP request+response bytes | Actor commits | Workflow writes |
|---|---:|---:|---:|---:|
| BUY | 1 | 2,497 | 1 | 0 |
| SELL | 1 | 3,832 | 1 | 0 |
| CLAIM_TASK | 1 | 6,077 | 1 | 0 |
| PLANT | 10 | 44,341 | 3 | 7 |
| HARVEST | 7 | 25,696 | 2 | 5 |
| WATER | 19 | 30,199 | 1 | 0 |
| THEFT | 39 | 68,546 | 2 | 5 |

这是单次真实业务放大锚点，不是稳定吞吐均值。正式模型必须用 Mixed 多轮增量校准。

### 3.1 候选业务 Mix

| Mix | BUY | SELL | CLAIM | PLANT | HARVEST | WATER | THEFT | Redis ops/op | bytes/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BALANCED | 10% | 10% | 10% | 20% | 15% | 25% | 10% | 12.00 | 28,367.55 |
| SOCIAL_HOT | 5% | 5% | 5% | 10% | 10% | 35% | 30% | 20.20 | 38,757.45 |
| PLAYER_HOT | 35% | 30% | 35% | 0 | 0 | 0 | 0 | 1.00 | 4,150.50 |
| WORKFLOW | 0 | 0 | 0 | 35% | 25% | 20% | 20% | 16.85 | 41,692.35 |

### 3.2 七类业务写 Redis 下界外推

| Mix | Design Redis ops/s | Stress Redis ops/s | Design bytes/s | Stress bytes/s |
|---|---:|---:|---:|---:|
| Balanced | 833,328 | 1,666,668 | 1.83 GiB/s | 3.67 GiB/s |
| Social Hot | 1,402,769 | 2,805,558 | 2.51 GiB/s | 5.01 GiB/s |
| Player Hot | 69,444 | 138,889 | 0.27 GiB/s | 0.54 GiB/s |
| Workflow | 1,170,131 | 2,340,280 | 2.70 GiB/s | 5.39 GiB/s |

这张表只能命名为 `business_write_redis_lower_bound`，不能代表总 Redis 压力，也不代表公司 Redis 已支持这些流量。本地测试受批准预算限制；依赖先满判 `INCONCLUSIVE_DEPENDENCY`。

### 3.3 READ_RECOVERY Profile

七类 mutation Mix 之外必须实现独立 Profile：

```text
GET_PLAYER_BOOTSTRAP
GET_TASK_LIST
FRIEND_LIST
PUBLIC_PROFILE_BATCH
GET_NEXT_FRIEND_OPPORTUNITY
JOIN_SNAPSHOT
OPERATION_RESULT_LOOKUP
FRESH_JOIN
WORKER_DUE_SCAN/CLAIM/RENEW
```

每类先独立测量 Redis ops/request/response bytes、Player/Farm/Gate/Worker service amplification，再形成 `MIX-READ-RECOVERY-R2`；未知 amplification 不得填 0。正式 Mixed 最少覆盖 Balanced、Social Hot、Workflow Recovery、Login/Join Burst 和 Hot Farm。

---

## 4. 通用调度合同

### 4.1 Open-loop

- 每个 arrival 由外部 monotonic clock 计划；
- 服务变慢不能自动降低 planned load；
- lag >50ms 记 `scheduled_unsent`；
- 禁止 catch-up burst；
- planned/launched/unsent/completed/rejected/timeout/failed 分开；
- correctness/observability stop 后不再创建新 arrivals；
- inflight 只在 request timeout 内有限回收。

每轮 Manifest 必须记录所有同时负载维度：

```text
connections, handshakes_per_second, frames_per_second,
egress_bytes_per_second, arrivals_per_second, active_actors,
subscriptions, fanout_per_second, backlog, work_steps_per_second,
due_claims_per_second
```

### 4.2 模式与时长

`LOCAL_DEV_FAST` 只调配置，不能继承为 Formal。

| Phase | Ramp | Hold/Duration |
|---|---:|---:|
| PRECHECK | 0 | ≤10 min |
| OBSERVABILITY_QUALIFICATION | 0 | ≥5 min |
| LOAD_GENERATOR_QUALIFICATION | 0 | ≥5 min |
| SMOKE | 0 | 5 min |
| WARMUP | 0 | 10 min |
| STAIRCASE 每档 | 2 min | 10 min |
| KNEE_CONFIRMATION | 2 min | 10 min |
| SAFE_TIER_CANDIDATE | 0 | 20 min |
| FORMAL ×3 | 0 | 每轮 20 min |
| FORMAL 间 COOLDOWN | 0 | 5 min |
| SOAK | 0 | 2 h 初始 / 8 h 扩展 |

Formal 三轮必须使用新 namespace/process/pool，不复用 warmed mutable state；fixture 语义必须一致。

### 4.3 继承的 Boot/Lease 硬门

```text
holder_id      = stable StatefulSet Pod name
holder_boot_id = controller-generated run generation nonce
```

Holder 重建必须生成新 Boot ID、Snapshot SHA、Authority Init Receipt 和 Activation Receipt。固定 Pod 名不得充当 Boot。正式 L2 前必须完成 `>=3×Shard Authority Lease TTL`，并证明 route resolve无expired、membership无degraded、ServingShardCount精确、renewal deadline高于safety margin、Worker participant route failure为0。延长 Snapshot Unix expiry 不能替代活性证明。

---

## 5. 通用 PASS/STOP 条件

### 5.1 正确性与观测

所有 required correctness gates 精确为 0。任一非零、未知、负值或不完整：

```text
FAIL_CORRECTNESS 或 INCONCLUSIVE_OBSERVABILITY
立即停止新 arrivals
```

### 5.2 Delivered load 与 Driver

```text
Smoke/Warmup/Staircase/Candidate actual/planned >=95%
Formal 每轮 actual/planned >=98%
arrival lag P99 <=50ms
Driver qualification >=60 successful samples
Driver CPU <=70%
每个 requested target headroom >=1.5×
arrival-lag/event-backlog/socket-error positive slope windows =0
```

Driver 先满：`INCONCLUSIVE_LOAD_GENERATOR`。业务正常拒绝（如 NO_CHANGE/CROP_NOT_MATURE/NOT_ENOUGH_ITEM）必须与基础设施失败分开。Candidate/Formal 候选门：

```text
unexpected_failed_rate = 0
unexpected_timeout_rate <=0.1%
unexpected_rejection_rate = 0
scheduled_unsent <=2% Formal
correctness violations = 0
```

`5% rejection/timeout` 只用于快速识别饱和并停止，不是 Safe Tier 门。

`SLO_PROFILE_V0_1_CANDIDATE`：

| Path | Absolute SLO |
|---|---:|
| Gate HTTPS Login P99 | ≤500ms |
| WSS Upgrade + ClientHello P99 | ≤500ms |
| Join Ready P99 | ≤1,000ms |
| Player Turn P99 | ≤200ms |
| Farm Turn P99 | ≤200ms |
| FarmChange delivery P99 | ≤500ms |
| Worker oldest Due（steady） | ≤5,000ms |
| Worker recovery drain | ≤300s，且按冻结 backlog 重算 |

相对 2× degradation 和绝对 SLO 任一失败都不能选 Candidate。无法一对一映射时判 `INCONCLUSIVE_OBSERVABILITY`。

### 5.3 Target saturation

候选安全区目标 CPU 为 60–70%。下列任一停止：

```text
CPU >=90% sustained 60s
memory >=90% sustained 60s
FD >=85% sustained 60s
OOM kill >0
controlled rejection/timeout >=5% sustained 60s
primary queue 3 positive windows
P99 nonlinear degradation >=2× sustained
RSS/queue/oldest age 不收敛
```

### 5.4 Dependency 与 Environment

```text
Dependency CPU >=85% sustained
Dependency memory >=90% sustained
Dependency errors >=1% sustained 30s
Redis pool wait/support queue positive slope 3 windows
公司 Redis/网络触及批准预算
```

结果为 `INCONCLUSIVE_DEPENDENCY`。Identity/cgroup/config drift、restart、namespace residual、host clock step >50ms、steal >2% 30s、background CPU >10% 60s 都判环境不合格。

---

## 6. L2-GATE-R2 执行矩阵

### 6.1 G1 Connection Hold

真实 Login/Ticket/WSS/ClientHello/Join/Ready 后保持连接和读循环。

```text
connection tiers = 200, 500, 1,000, 2,000, 5,000, 10,000
extension = previous ×1.5，仅前档有效时
heartbeat/frame = 每连接 30s 一个受控应用帧；若协议无此帧则记录实际 Ping/lease 行为
Driver qualification = 15,000 connections
```

记录连接增量 RSS、goroutine、FD、GC、socket、心跳和断线。Gate Preflight 还必须冻结 `ulimit -n`、`ip_local_port_range`、`somaxconn`、`tcp_max_syn_backlog`、conntrack max/current、TIME_WAIT 和 Driver 可用源地址；任何运行中 drift 均为环境失败。

### 6.2 G2 Handshake/Login Storm

```text
session start/s = 50, 100, 200, 400, 800, 1,200, 1,800, 2,700
post-ClientHello hold = 2–5s 后主动关闭
active connections hard cap = min(50% × G1 safe connections, reviewed host/socket cap)
full path = HTTPS Login → Ticket → WSS → ClientHello
Driver qualification = 1.5× selected G2 candidate，且不突破 active hard cap
```

G2 只测握手吞吐，不隐含 162k/243k 长连接。若 G1 safe constant 尚未形成，G2 只能跑不超过 30% 的 provisional G1 tier，不能执行 qualification。

### 6.3 G3 Join/Snapshot

```text
Join/s = 25, 50, 100, 200, 400, 600, 900, 1,350
connection source = G1 已建立且稳定的连接池
login/WSS creation during measurement = 0
Snapshot = P50 fixture / P95 16KiB / near-hard 32KiB
Join per session = 1, 3, 6 分开
Driver qualification = 2,025 Join/s
```

记录 Snapshot encode、farm→gate、connection queue、Ready latency 和 egress。

### 6.4 G4 Steady Frames/Egress

在固定连接水位分别施加：

```text
frames/s = 1k, 2.5k, 5k, 10k, 20k, 40k
payload = small control / typical delta / 4KiB near-hard
Driver qualification = 60k frames/s
```

不得把本地 client write 成功当服务端处理成功。

### 6.5 G5 Slow Reader

在安全连接候选的 60% 负载下：

```text
slow readers = 1, 2, 10, 1%, 5%
```

正常连接 P99/queue 不得全局扩散；慢连接只能局部降级、Rejoin 或关闭。

### 6.6 G6 Drain/Reconnect

在候选 60% 连接和业务负载下 Drain 单 Gate：readiness 先 503；停止新接纳；已有连接受控关闭/重连；operation 不重复；reconnect 使用 Full Jitter。不得以“所有 PCU 同时重连”代表普通 RollingUpdate。

### 6.7 G7 Friendship Reads

通过真实 Gateway/H5 协议分别运行好友页和好友机会查询：

```text
read/s = 1k, 2.5k, 5k, 10k, 20k, 40k
friend counts = 0, 20, 100, 300
friend page size = 20
navigation batch = 32, 64
candidate window = 8
Driver qualification = 60k reads/s for each profile
```

两类 read 的 planned/actual、放大、partial/unknown 和 latency 分开，不与 mutation 合并。

Gate 必须输出：

```text
safe_connections_per_gate
safe_session_starts_per_second
safe_join_per_second
safe_friend_page_reads_per_second
safe_friend_opportunity_reads_per_second
safe_frames_per_second
safe_egress_bytes_per_second
```

---

## 7. L2-PLAYER-R2 执行矩阵

```text
turns/s = 50, 100, 200, 400, 800, 1,200, 1,800, 2,700
active players = 100, 500, 1,000, 5,000, 10,000, 25,000
Driver qualification = 4,050 turns/s and 37,500 addressable actors
```

Fixture IDs 均匀覆盖 virtual shard/bucket；单 hot player 另跑，不与 uniform 合并。

Profiles：

```text
P1 MIX-PLAYER-HOT-R2
P2 uniform BUY/SELL/CLAIM_TASK
P3 cold activation（每 arrival 新 Actor）
P4 single hot Player（同 mailbox 串行）
P5 Participant steps（Plant/Harvest/Care/Theft）
```

硬门：duplicate reward/debit、stale claim commit、record regression、receipt conflict 全零。

输出：

```text
safe_player_turns_per_second
safe_active_player_actors
safe_player_activations_per_second
safe_player_commit_bytes_per_second
```

---

## 8. L2-FARM-R2 执行矩阵

```text
farm turns/s = 25, 50, 100, 200, 400, 600, 900, 1,350
active farms = 100, 500, 1,000, 5,000, 10,000
Driver qualification = 2,025 turns/s and 15,000 addressable farms
```

Uniform profiles：PLANT/HARVEST/WATER/THEFT、cold activation、MIX-WORKFLOW-R2、Join/subscription 和 FarmChange fanout。

### 8.1 Hot Farm

```text
hot farm count = 1, 10
visitors/farm = 200
change/s = 1, 5, 10, 20, 50
slow readers = 0, 1, 2, 10
```

一次 1 KiB Delta ×199 viewers：

```text
1 change/s  ≈199 KiB/s payload
10 change/s ≈1.94 MiB/s
50 change/s ≈9.72 MiB/s
```

要求 FarmActor 不等待网络；FarmVersion 连续、无 duplicate PlotDelta、正常 reader 不被慢 reader 拖累。

输出：

```text
safe_farm_turns_per_second
safe_join_per_second
safe_active_farm_actors
safe_farm_change_fanout_per_second
safe_subscriptions_per_farm_pod
```

---

## 9. L2-WORKER-R2 执行矩阵

```text
backlog = 100, 1,000, 10,000, 30,000, 100,000
new due/work/s = 25, 50, 100, 200, 400, 800, 1,200
Driver seed qualification = 150,000 records（seed 时间不计入测量）
Driver arrival qualification = 1,800/s
```

Family 分离：

```text
W1 Plant
W2 Harvest
W3 Care
W4 Theft
W5 Friend Action Dispatch
W6 Result Publish
W7 Mixed Family Fairness
```

每个 Family 单独测 knee；Mixed 不得掩盖 starvation。

在候选 60% arrival/backlog 下运行 Drain 和 Kill：停止 claim new；已 claim work 完成、续租或安全释放；replacement 从 DueIndex 恢复。InvalidOrder、StepUnknown、unexpected Due、duplicate effect、wrong scope 全零，oldest Due 最终收敛。

输出：

```text
safe_work_steps_per_second
safe_due_claims_per_second
safe_due_drain_per_second
safe_backlog_per_worker
```

---

## 10. Fixture 与负载分布

### 10.1 分级 Fixture 池

不存在所有 Formal 都必须创建的 10M friend edges。每轮只创建本服务所需的最小 Profile：

| Profile | Player/Farm | Directed friend edges | Workflow seeds | 用途 |
|---|---:|---:|---:|---|
| FIXTURE_F0_FUNCTIONAL | 1,000 / 1,000 | 10,000 | 1,000 | Functional/Smoke |
| FIXTURE_F1_L2 | 10,000 起，最多100,000 | 按场景，默认最多1,000,000 | 10,000 起，最多150,000 | 单服务 L2，受Owner预算 |
| FIXTURE_F2_SOCIAL_SCALE | 100,000 / 100,000 | 10,000,000 | 场景指定 | 独立 Social Scale授权 |

共同项：Hot Farm 1/10、每 Farm 12 plots、Operation IDs 每 run 新生成。F1 的 exact count 必须由 active actor/backlog tier 反推，不能无条件取上限。F2 的百万级 Pair/10M directed edges 永不作为普通 Formal 默认输入，只能在 Redis Owner 单独批准 key/byte/write/cleanup 预算后运行。

每个 Fixture 在写入前估算 keys/bytes/single-key bytes/create ops/cleanup ops/duration；预计超授权即 Preflight 拒绝。降低 Fixture 时必须标记 `FIXTURE_SCALE_REDUCED`，不得冒充大规模内存/扫描证明。

### 10.2 分布 Profile

```text
U100 = 100% uniform Actor/Farm
H80  = 80% uniform + 20% traffic to 1% hot actors
H95  = 95% traffic to 1% hot actors
COLD = 每 arrival 选择未激活 actor
HOT1 = 单 actor/mailbox
```

每轮只选择一个分布 Profile，并写入 Manifest。30M actor hash 分布另做离线 30M ID 探针，不向 Redis 写 30M records。

### 10.3 Friendship

Balanced 使用平均 100 好友；边界 Profile 使用 0/20/100/300。详细 read tiers 见 G7。必须记录 batch size、Authority read count、partial/unknown 和 P99。

---

## 11. Staircase 与 Candidate 算法

1. 最低 tier Smoke；
2. Warmup 后按预定义 tier 逐级执行；
3. 每档保存完整 Evidence，不删除失败档；
4. 首次出现 target saturation/nonlinear knee 时停止升档；
5. Cooldown 后复跑 knee tier；
6. knee 可重复时，前一档才可成为 Candidate；
7. Driver/Dependency/Environment 先失败则 INCONCLUSIVE；
8. Candidate 运行 20 分钟；
9. Candidate PASS 后 Formal×3；
10. 三轮 throughput/P99/RSS CV 均 ≤10%。

没有出现 knee 不表示最后一档自动安全；必须扩展 tier 或标记测试范围不足。

---

## 12. Pod 扩展曲线与主机可行性

当前 32 logical CPU 主机在 `4C Target + 8C Driver + system/dependency reserve` 下，只授权正式运行：

```text
N = 1, 2
2 Pod efficiency >=85%
```

4×4C 目标会超过可信 CPU 隔离预算，当前状态固定为：

```text
4P_4C_SCALE_STATUS=NOT_RUN_INSUFFICIENT_HOST
LOCAL_LAB_30M_DAU_MODEL_READY=false
```

合法后续只有：迁移到更大/第二台 Linux 主机；或另建 `L2_SCALE_PROFILE_2C` 测趋势。2C 结果不能与 4C 单 Pod常数混用。任何规模都使用独立 Snapshot/Authority namespace：

```text
scale_efficiency_N = delivered_qps_N / (N × delivered_qps_1)
```

正确性必须为0，Driver/Dependency不先满。Player/Farm 扩 Pod 必须生成新静态 RouteSnapshot；不能在同一快照直接 `kubectl scale`，也不能通过物理核争抢伪造4 Pod曲线。

---

## 13. 本地 Mixed 执行矩阵

Mixed 只使用已 Review 的四服务 safe constants。对比例 `m`，先按实测 service amplification 计算：

```text
business_qps_m = min(service_safe_capacity × m / service_amplification)
```

Connections、Session start、Join 和好友读取另作为并行 load dimensions，并同样不得超过对应 safe capacity×m；不能独立把每个服务都打到一个不可能由真实业务产生的百分比。

```text
M1 = m 30%
M2 = m 50%
M3 = m 70%
M4 = m 80%（仅压力观察，不自动成为 baseline）
```

场景：

```text
MX1 MIX-BALANCED-R2
MX2 MIX-SOCIAL-HOT-R2
MX3 MIX-WORKFLOW-RECOVERY-R2（含READ_RECOVERY）
MX4 LOGIN-JOIN-BURST（握手/Join分别受G1 cap）
MX5 HOT-FARM-200 + background uniform
MX6 Worker backlog recovery + foreground business
MX7 Gate drain under 60% Mixed
MX8 Worker kill under 60% Mixed
MX9 Holder Boot rollover negative/recovery（新Boot/Snapshot/Authority）
```

每服务 delivered/planned 分开；第一瓶颈分类；Mixed efficiency 相对单 Pod 合成值单独报告；M4 过载数据不能选为 baseline。Formal Mixed 为 3×20min，随后 2h soak。

---

## 14. Fault 与 Soak

### 14.1 非破坏性故障矩阵

| Fault | Load | Duration | 预期 |
|---|---:|---:|---|
| Gate readiness off + Drain | Mixed 60% | 1 cycle | 停新接纳、受控重连、无重复 |
| Gate process kill | Mixed 30% | 1 cycle | 断线可恢复，错误显式 |
| Player same-Holder restart | Player 30% | 1 cycle | 静态路由原 Holder 恢复前 fail closed |
| Farm same-Holder restart | Farm 30% | 1 cycle | 不声明自动 takeover |
| Worker graceful drain | Worker 60% | 1 cycle | stop claim、已 claim 收敛 |
| Worker kill | Worker 60% | 1 cycle | Lease/DueIndex 恢复、无重复 effect |
| Prometheus proxy loss | Mixed 30% | 60s | Oracle incomplete，立即停新 arrivals |
| Evidence disk write failure | Smoke | 1 event | 立即 fail closed |

### 14.2 Redis 客户端路径 Fault

只有另行批准后，才允许在 Lab 自有 client-side proxy 上注入：

```text
latency = 50ms, 100ms, 250ms，各 120s
connection cut = 30s
packet/connection error = 1% 持续 120s
```

不修改公司 Redis 服务端，不重启、不 failover、不改配置、不清 keyspace。注入时负载最多为 safe tier 的 30%。未部署精确 namespace proxy 时本项为 `NOT_RUN`，不能手工模拟。

### 14.3 Soak

```text
SOAK-2H: MIX-BALANCED-R2, Mixed safe capacity 70%
SOAK-8H: MIX-BALANCED-R2, 50%（2h PASS 后才运行）
每 30min 插入 5min SOCIAL_HOT，强度不超过总 safe 70%
```

要求 RSS/heap/FD/goroutine、oldest Due、pool wait、queue 和 residual 无持续正 slope；周期任务不能制造 burst。

---

## 15. 公司 Redis 安全预算

以下为刻意保守的“申请/审核候选”，不是由公司 Redis 容量反推的数值；在用户和 Redis owner 明确批准前均是 `NOT_AUTHORIZED`：

```text
proposed_sustained_redis_commands_per_second = 25,000
proposed_burst_redis_commands_per_second = 50,000 for <=10s
proposed_aggregate_client_connections = 128
proposed_resp_ingress_plus_egress = 100 MiB/s
proposed_formal_window = 2h
proposed_soak_window = 8h separately approved
```

正式 Binding 必须替换为 owner 批准值：

```text
COMPANY_REDIS_MAX_SUSTAINED_OPS
COMPANY_REDIS_MAX_BURST_OPS
COMPANY_REDIS_MAX_CONNECTIONS
COMPANY_REDIS_MAX_REQUEST_BYTES_PER_SECOND
COMPANY_REDIS_MAX_RESPONSE_BYTES_PER_SECOND
COMPANY_REDIS_MAX_NAMESPACE_KEYS
COMPANY_REDIS_MAX_NAMESPACE_BYTES
COMPANY_REDIS_MAX_FIXTURE_WRITE_OPS_PER_SECOND
COMPANY_REDIS_MAX_CLEANUP_OPS_PER_SECOND
COMPANY_REDIS_MAX_CLEANUP_DURATION_SECONDS
COMPANY_REDIS_MAX_SINGLE_KEY_BYTES
COMPANY_REDIS_APPROVED_START/END
COMPANY_REDIS_OWNER_APPROVAL_SHA256
```

Harness 取本轮业务 Mix 的实测 `redis_ops/business_op`，在调度前计算 QPS ceiling；达到任一预算即停止新 arrivals，并判 `INCONCLUSIVE_DEPENDENCY`。

Namespace 必须使用现有 `rediskey.Namespace` 普通组件，不能在所有 Key 外包一层共同 `{run-id}` Hash Tag：

```text
REDIS_NAMESPACE_ENVIRONMENT=l2r2<run-id-hash>  # lower segment, <=32
REDIS_NAMESPACE_ZONE=z1
```

现有 KeyBuilder 把 Namespace 放进原业务 Bucket Tag，例如：

```text
{classic-farm:<env>:z1:auth:player:v1:s:<bucket>}:...
{classic-farm:<env>:z1:wf:v1:b:<bucket>}:...
```

必须有黄金测试：同 Authority Bucket 同 Slot；不同 Bucket 正常分散；加入 run namespace 前后 bucket 分布不退化；所有 Key 均有当前 namespace。禁止 `cap:{run-id}:<原key>` 这种全局单 Slot 写法。

要求：唯一 run-id；KeyBuilder allowlist；禁止 SCAN 全库；禁止 `FLUSHALL/FLUSHDB/SCRIPT FLUSH`；只按本轮 ledger bounded cleanup；cleanup 后 residual=0；Evidence 不保存 URL、Secret 或 raw IDs。

### 15.1 双 Redis Profile

```text
PROFILE_A_COMPANY_REDIS_COMPAT
  = Owner预算内验证真实网络/ACL/延迟；先触预算则INCONCLUSIVE_DEPENDENCY

PROFILE_B_LOCAL_DEDICATED_REDIS_KNEE
  = 独立本地Redis、依赖高余量，用于目标Pod Knee发现
```

两者 Environment/Binding/Evidence 完全隔离，不能合并成一个容量常数。若仅使用公司 Redis，必须接受部分服务只能得到依赖上限。

---

## 16. 3000 万 DAU Pod 容量换算

### 16.1 实测常数

每个常数必须来自 exact Evidence：

```text
G_conn   = safe connections/Gate Pod
G_login  = safe session starts/s/Gate Pod
G_join   = safe Join/s/Gate Pod
G_friend_page = safe friend page reads/s/Gate Pod
G_friend_opportunity = safe opportunity reads/s/Gate Pod
G_frame  = safe frames/s/Gate Pod
G_egress = safe bytes/s/Gate Pod

P_turn   = safe Player turns/s/Player Pod
P_actor  = safe active PlayerActors/Player Pod
P_act    = safe Player activations/s/Player Pod
P_friend = safe stateless Friendship/PublicProfile reads/s/Player Pod

F_turn   = safe Farm turns/s/Farm Pod
F_actor  = safe active FarmActors/Farm Pod
F_join   = safe Join/s/Farm Pod
F_fanout = safe fanout deliveries/s/Farm Pod

W_step   = safe work steps/s/Worker Pod
W_claim  = safe Due claims/s/Worker Pod
W_drain  = safe backlog drain records/s/Worker Pod
W_backlog= safe resident/observable backlog/Worker Pod
```

修正系数：

```text
E_scale = 已授权且资源隔离有效的 Pod 曲线最低扩展效率
E_mixed = Mixed delivered / isolated synthetic capacity
R_plan  = 0.70（在已测 safe tier 上再保留 30% 规划余量）
E = min(E_scale, E_mixed) × R_plan
```

不得用假定0.75代替缺失的4 Pod曲线。当前主机只生成1/2 Pod局部模型；在更大主机闭合4×4C前 `LOCAL_LAB_30M_DAU_MODEL_READY=false`。Mixed 未运行同样不得发布。

### 16.2 Gate Pod 数

```text
N_gate_conn   = ceil(PCU / (G_conn × E))
N_gate_login  = ceil(session_start_qps / (G_login × E))
N_gate_join   = ceil(join_qps / (G_join × E))
N_gate_friend_page = ceil(friend_page_qps / (G_friend_page × E))
N_gate_friend_opportunity = ceil(opportunity_qps / (G_friend_opportunity × E))
N_gate_frame  = ceil(frame_qps / (G_frame × E))
total_gate_egress_Bps = Snapshot + FarmDelta fanout + CommandResult
                      + ControlFrame + Login/Ticket response
                      + measured protocol/TLS overhead
N_gate_egress = ceil(total_gate_egress_Bps / (G_egress × E))
N_gate = max(N_gate_conn, N_gate_login, N_gate_join,
             N_gate_friend_page, N_gate_friend_opportunity,
             N_gate_frame, N_gate_egress)
```

连接、握手、Join、好友读取、frame 和 egress 中的最大值决定 Gate，不允许只按一个 QPS。

### 16.3 Player/Farm/Worker Pod 数

Mixed 运行必须实测每业务操作的服务放大：

```text
A_player_turn = Player turns / completed business ops
A_farm_turn   = Farm turns / completed business ops
A_worker_step = eventual Worker steps / completed business ops
A_due_claim   = Due claims / completed business ops
A_fanout      = deliveries / completed business ops
```

```text
N_player_qps        = ceil(business_qps × A_player_turn / (P_turn × E))
N_player_actor      = ceil(active_player_actors / (P_actor × E))
N_player_activation = ceil(player_activation_qps / (P_act × E))
N_player_friendship = ceil(player_friendship_read_qps / (P_friend × E))
N_player = max(N_player_qps, N_player_actor,
               N_player_activation, N_player_friendship)

N_farm_qps   = ceil(business_qps × A_farm_turn / (F_turn × E))
N_farm_actor = ceil(active_farm_actors / (F_actor × E))
N_farm_join  = ceil(join_qps / (F_join × E))
N_farm_fanout= ceil(business_qps × A_fanout / (F_fanout × E))
N_farm       = max(N_farm_qps, N_farm_actor, N_farm_join, N_farm_fanout)

N_worker_step  = ceil(business_qps × A_worker_step / (W_step × E))
N_worker_claim = ceil(business_qps × A_due_claim / (W_claim × E))
N_worker_backlog = ceil(target_recovery_backlog /
                        (W_drain × target_recovery_seconds × E))
N_worker = max(N_worker_step, N_worker_claim, N_worker_backlog)
```

不得直接把 Trace 的 `due` 写次数当成最终 Worker steps；必须从 Mixed 端到端计量。`W_backlog` 还用于验证单 Worker可承载的 backlog hard bound；恢复 Pod 数使用 `W_drain × recovery window`，不能只看 steady arrival。

### 16.4 Zone/N+1

生产目标表至少计算 3 Zone。若要求任一 Zone 失效后维持设计流量：

```text
N_total_with_zone_loss = ceil(N_steady × Z / (Z - 1))，Z=3
```

并在每 Zone 内再加至少一个 Pod disruption reserve。该公式是规划目标，当前单节点 k3s 不能证明 Zone loss。

### 16.5 Redis 与网络

总 Redis 模型按流量家族求和，七类业务写表只是第一项：

```text
redis_total_ops_qps =
    business_write_qps × amp_business_write
  + business_read_qps × amp_business_read
  + session_start_qps × amp_session
  + join_qps × amp_join
  + subscription_renew_qps × amp_subscription
  + shard_renew_qps × amp_shard_renew
  + worker_arrival_qps × amp_worker
  + projection_qps × amp_projection
  + reconciliation_qps × amp_reconcile
  + fixture/cleanup/observer_ops_qps
```

Request/response bytes 使用同样家族拆分，未知 amplification 必须标 `MEASUREMENT_GAP`，不能填0。Redis 生产实例/Shard 数只能使用专用生产探针的 `safe_redis_ops`、memory、network 和 failover 常数；公司共享 Redis 本地结果不能直接代入生产 Pod 数。

网络模型至少输出：

```text
login ingress/egress
Join Snapshot egress
FarmDelta fanout egress
farm→gate internal bytes
Redis RESP bytes
cross-zone amplification
TLS/protocol overhead factor（待实测）
```

### 16.6 存储基线

按候选 `16 KiB Player + 12 KiB Farm`：

```text
30M × 28KiB ≈ 0.86 TB raw（约0.78 TiB）
Primary数据集（2.5×候选）≈ 2.15 TB
单个Replica副本          ≈ 2.15 TB
Primary + 1 Replica合计  ≈ 4.30 TB
```

2.5× 是 allocator/object overhead 假设，不是平台事实；Worksheet 必须运行 2.0×/2.5×/3.0×/4.0× 敏感性。该数不含好友约 30 亿条有向边、Receipt、Workflow、Projection、备份和预留。最终表必须分别列热权威、热 Receipt、长期 Evidence/Archive、索引和备份，不能把所有数据笼统放入“一套 Redis”。

### 16.7 输出 Worksheet

最终生成 `30m-dau-capacity-model.json`，至少包含：

```text
assumption_version
business/h5/backend/binding/evidence identities
design/stress inputs
all measured safe constants
scale and mixed efficiencies
business/service/redis/network amplifications
steady and zone-loss Pod counts
Redis/storage/network requirements
sensitivity at PCU 5%/10%/20%
missing production evidence
model_sha256
supports_30m_dau=false
claim_scope=LOCAL_LAB_MODEL_ONLY
```

---

## 17. Required Metrics 与 Oracle

### 17.1 Driver

planned/launched/unsent/completed/rejected/timeout/failed、arrival lag、inflight、event backlog、socket errors、CPU/RSS/FD。

### 17.2 Gate

accepted/active/closed connections、handshake/Login/Join、frame/byte ingress/egress、per-connection queue items/bytes/oldest、slow-reader action、gRPC queue/latency/errors、CPU/RSS/FD/goroutine/GC。

### 17.3 Player/Farm

turn/activation/mailbox/actor count、commit/CAS/conflict/replay、Model Field V2 migration/sparse、remote_player、Snapshot/Delta bytes、subscriptions/fanout、holder/fence rejection、CPU/RSS/GC。

### 17.4 Worker

Due discover/claim/renew/complete/reject、Family backlog/oldest/fairness、StepUnknown、InvalidOrder、scope mismatch、duplicate effect、drain state。

### 17.5 Redis/Host/K8s

Redis ops/latency/errors/pool wait/connections/bytes；node CPU/iowait/steal/memory/network/disk；Pod throttling/restart/OOM/working set；Prometheus scrape completeness/counter reset。

### 17.6 Correctness Oracle

每个 operation 绑定 request/result/authority evidence；FarmVersion、inventory/currency/crop/task、Workflow terminal state、Due convergence、friend authorization、Holder/Fence、legacy roster=0、V1 fallback=0。任一所需 Oracle 不可读即停止。

---

## 18. Evidence 与结果结构

每次 run：

```text
results/<run-id>/
  manifest.json
  binding.json
  route-snapshot.json + sha256
  redis-budget.json
  fixture-manifest.json
  planned-load.json
  actual-load.json
  metrics/
  oracle/
  correctness.json
  stop-reason.json
  residual.json
  verdict.json
  report.md
```

Manifest 必须分开记录 Business、Verification、Harness、Driver、Collector、Oracle、Backend image、H5 image、Binding、RouteSnapshot、K8s node/kernel/cgroup、Redis endpoint hash 和 namespace。禁止 Secret、raw Redis URL、raw Operation/Session/Receipt/Workflow/Work ID。

失败结果不可删除或用后续 PASS 覆盖；报告同时展示计划负载、实际负载、成功率、失败延迟和成功延迟。

---

## 19. Verdict

```text
PASS_LOCAL_SAFE_TIER
PASS_LOCAL_MIXED_BASELINE
FAIL_CORRECTNESS
FAIL_TARGET_SATURATION
INCONCLUSIVE_LOAD_GENERATOR
INCONCLUSIVE_DEPENDENCY
INCONCLUSIVE_OBSERVABILITY
INCONCLUSIVE_ENVIRONMENT
LOAD_GENERATOR_SLOPE_OBSERVATION_INCOMPLETE
NOT_RUN_NOT_AUTHORIZED
```

只有以下全部满足才生成 `LOCAL_LAB_30M_DAU_MODEL_READY`：

1. 四服务 L2 各 Formal 3/3 PASS；
2. 1/2/4 Pod scale curve PASS（当前主机4 Pod不可执行，因此当前保持 false）；
3. Balanced、Social Hot、Workflow Recovery、Login/Join Burst、Hot Farm Mixed PASS；
4. 2h soak PASS；
5. Redis budget 未越界；
6. 30M worksheet 可复算、敏感性完整；
7. 独立 Review P0/P1/P2=0/0/0。

即使全部满足，文件仍必须写：

```text
supports_30m_dau=false
production_capacity_certified=false
claim_scope=LOCAL_LAB_MODEL_ONLY
```

生产认证还缺多节点/多 Zone、生产 Redis、真实 LB/TLS、跨 Gate、目标网卡、故障切换和真实 3M/5M 连接证据。

---

## 20. 审核后的规划决定

按最新审核冻结以下实现输入：

1. 6 sessions/day、5min/session、20 ops/day；
2. design/stress PCU=3M/5M；
3. design/stress business QPS=69,444/138,889；
4. normal design/stress Join=62,500/125,000/s；故障重连独立建模；
5. 每会话 3 次好友机会查询、1 次好友页读取；
6. 四个 mutation Mix 条件接受，新增 READ_RECOVERY；
7. Fixture 改为 F0/F1/F2；F2 Social Scale 单独 Redis Owner 授权；
8. Staircase 2min ramp +10min hold、Formal 3×20min；
9. Soak 2h，8h 另行激活；
10. `R_plan=0.70`；
11. Player/Farm/Worker tiers 条件接受；Gate G2 改为2–5s短持有且受G1连接cap；
12. 连接保持中的 30s 应用帧候选；
13. 25k sustained/50k burst/128 connections/100MiB/s 仅作为提交给 Redis Owner 的预算申请候选；
14. 允许实现自有 client-side Redis proxy fault，但运行仍受独立 Fault/Redis 授权；
15. 当前主机正式跑1/2 Pod；4×4C=`NOT_RUN_INSUFFICIENT_HOST`；
16. 三 Zone loss 只做模型，不在单节点 k3s 宣称已验证；
17. Evidence 保留周期和存储上限采用实施计划第 13 节。

这些决定授权 k3s/Driver/Static Init 和本地 Functional 实现。Redis Owner machine-readable Receipt、Hash Tag golden test、Boot/Lease、绝对SLO、CPU/Disk/Review 门未闭合前，仍不得运行公司 Redis Staircase/Formal、生成 ACTIVE L2 Binding 或开始 Mixed。

---

## 21. 上线前必须由真实数据回填

```text
sessions_per_dau
session_duration_distribution
PCU/DAU and hourly curve
commands_per_dau
command_mix by peak window
joins_per_session
friend_count distribution
hot-farm distribution
Snapshot/Delta size distribution
reconnect storm distribution
service/Redis/network amplification
region/zone traffic split
```

每个回填参数都要保留 `value/status=ASSUMED|MEASURED/source/owner/sample_window/sample_count/confidence`；模型版本随输入变更，不允许静默覆盖。至少生成以下笛卡尔敏感性切片（可用逐变量敏感性避免组合爆炸，但必须保存基准）：

```text
sessions/day       = 4 / 6 / 8
mean session       = 2 / 5 / 10 min
joins/session      = 1 / 3 / 6
business ops/day   = 10 / 20 / 40
PCU                = 1.5M / 3M / 5M
average friends    = 20 / 100 / 300
Redis overhead     = 2.0× / 2.5× / 3.0× / 4.0×
```

---

## 22. 推荐执行顺序

```text
0. Section 20 defaults accepted；conditional implementation approval
1. Driver Adapter 实现和独立 Review
2. Local k3s/Registry/Boot-aware Static Init 实现和 Review
3. CPU/Disk/low-rate Redis/Lease implementation gates
4. Provisional Binding Review
5. CP-LOCAL-K8S-FUNCTIONAL-R2 + Boot rollover + 3×TTL
6. Redis Owner formal budget + Active Binding Exact-Head Review
7. Observability Qualification
8. Load Generator Qualification
9. 四服务 L2
10. 1/2/4 Pod curve
11. Mixed Resource Profile + Balanced/Social Hot
12. Fault
13. Soak
14. 30M worksheet + independent review
```

---

## 23. 参考证据

- `docs/design/31-actor-authority-redis-physical-contract.md`
- `docs/design/60-access-session-test-acceptance-freeze.md`
- `docs/design/legacy/18-final-architecture-blueprint.md`
- `docs/verification/route-hash-profile-v2-handoff.md`
- `docs/verification/v121-m6-capacity-and-observability.md`
- `docs/verification/v121-m6-local-calibration.md`
- `docs/verification/perf/cap-single-pod/CAP-SINGLE-POD-L2-R1/profile.json`
- `docs/verification/perf/cap-single-pod/CAP-SINGLE-POD-L2-R1/contract.md`
- `docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/postmerge-h5-f22-qualification-20260810/trace/business-command-amplification.json`
- `docs/verification/perf/local-k8s-r2/R2-LOCAL-K8S-CAPACITY-LAB-IMPLEMENTATION-PLAN-20260810.md`
