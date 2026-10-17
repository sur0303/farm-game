# v1.21 M6 · 3000 万 DAU 容量模型、压测工具与可观测性

日期：2026-07-29

## 1. 结论

M6 已完成容量验证所需的代码基线、真实协议压测入口、核心指标、一版可复算模型，
以及 500 条真实协议连接的 30 分钟本地浸泡。该浸泡全部通过，但
**没有把 500 连接或本机微基准伪装成生产容量上限**。

目前能够确认：

- `ws-stress` 已从“发送服务端必然拒绝的随机二进制并统计本地 WriteMessage”
  改为真实 `Login → ClientHello → JoinFarm → FarmSnapshot`；
- `-scenario plant` 还会执行真实 `BuyOffer → PlantCrop`，覆盖 PlayerActor、
  Plant Workflow、FarmActor 和 Redis CAS；
- 400 条 Join-only 连接与 100 条 Buy+Plant 连接完成 30 分钟浸泡，
  500/500 Ready、100/100 Plant、零异常断线；
- Gate、Actor Runtime、Player/Farm Store、Workflow Worker 已有 Prometheus
  指标，Prometheus/Grafana 本地配置会抓取四个运行角色；
- operation receipt、item reservation、farm settlement 不再按 128 条静默淘汰，
  而是冻结为至少 7 天保留、窗口内硬上限 1024，满额时在业务变更前返回
  `ERR_RATE_LIMITED`；
- Farm receipt 不再重复保存整份 FarmSnapshot。当前 7 天模型下 FarmRecord
  从约 100.7 KiB 降至约 14.2 KiB。

本轮仍不能确认：

- 单 Gate 超过 500 后的安全长连接上限；
- 公司 Redis 的生产 P99、可用吞吐、主从切换和容量上限；
- 最终 Gate/Player/Farm/Worker Pod 数；
- 3000 万 DAU 已通过真实压测。

因此本阶段结论是“验证手段和容量风险已明确”，不是“3000 万 DAU 已验收”。

## 2. 为什么先修工具和记录模型

旧 `ws-stress` 写入任意字节。FarmGate 正确行为是拒绝非法 FarmFrame，所以旧工具
测到的只是客户端 socket 写耗时，既不包含登录、协议解析、Actor 路由、Redis，
也不能证明连接已经进入 `InFarm`。

旧 Player/Farm receipt 超过 128 条后静默删除。这样旧 operation_id 重试时可能被
当成新写执行，容量保护破坏了幂等正确性。直接无限保留则会让完整记录 CAS 越来越
大。因此本轮选择：

```text
正确性下限：7 天内绝不静默删除
容量上限：  每类窗口最多 1024 条
达到上限：  写入发生前明确拒绝，等待旧终态过期
进行中状态：PREPARED Reservation / 未完成 Settlement 永不按时间清理
查询缺失：  仍是 UNAVAILABLE/Unknown，不伪装为“从未执行”
```

另一项实测问题是每个 Farm receipt 都保存 12 地块 Snapshot。Snapshot 是可通过
Join/Snapshot/FarmChange 重建的投影，不是 OperationResult 的不可重建事实。
现在 receipt 只保存奖励、私有结果和 settlement_id；实时首次响应仍携带 Snapshot。

## 3. 3000 万 DAU 模型

### 3.1 输入与假设

只有 `DAU=30,000,000` 来自题面，其余均是用于敏感性分析的显式假设：

| 参数 | 本版假设 | 性质 |
|---|---:|---|
| DAU | 30,000,000 | 题面输入 |
| 每 DAU 日业务操作 | 20 | 待产品埋点回填 |
| 峰值系数 | 平均值的 6 倍 | 待流量曲线回填 |
| CCU/DAU | 5% / 10% / 20% | 敏感性区间 |
| 幂等结果热窗口 | 7 天 | 当前代码合同 |
| 模型热窗口操作数 | 140 | `20 × 7` |

业务吞吐模型：

```text
日操作量 = 30,000,000 × 20 = 600,000,000
平均 QPS = 600,000,000 / 86,400 ≈ 6,944
模型峰值 QPS = 6,944 × 6 ≈ 41,667
```

这不是压测结果。把 20 次改成 `O`、峰值系数改成 `P` 即可复算：

```text
peak_qps = 30,000,000 × O / 86,400 × P
```

### 3.2 长连接敏感性

| CCU/DAU | 同时在线连接 |
|---:|---:|
| 5% | 1,500,000 |
| 10% | 3,000,000 |
| 20% | 6,000,000 |

在得到单 Gate 实测安全水位 `C_safe` 前，不冻结 Pod 数：

```text
gate_pods = ceil(CCU / C_safe / target_utilization)
```

例如目标利用率取 0.8，只是预留 25% 余量；`C_safe` 必须由真实 WSS、心跳、
下行队列、慢连接和 GC 压测得到，不能使用“理论 fd 上限”代替。

滚动发布也不能按“所有 CCU 同时重连”估算。若有 `G` 个均衡 Gate、逐个摘流、
每个 Pod 的 DrainNotice 将重连摊到 `T` 秒，则单次滚动额外入口速率近似：

```text
rolling_reconnect_rate = CCU / G / T
```

全区域故障才使用全量重连风暴模型，并必须由客户端 Full Jitter 和入口限流削峰。

## 4. 当前记录大小证据

命令：

```bash
GOCACHE=/tmp/farm-go-cache \
go test ./internal/capacity -run TestCurrentRecordSizeModel -v
```

结果为 protobuf 净载荷，不含 Redis key、allocator、复制、AOF/RDB 和网络开销：

| 场景 | 实测字节 | 约 KiB |
|---|---:|---:|
| Player 新号 | 181 | 0.2 |
| Player 7 天模型：140 receipts + 10 reservations | 35,961 | 35.1 |
| Player 硬窗口：1024 + 1024 | 336,053 | 328.2 |
| Farm 新号 | 85 | 0.1 |
| Farm 7 天模型：6 crops + 140 receipts + 10 settlements | 14,506 | 14.2 |
| Farm 硬窗口：12 crops + 1024 receipts + 1024 settlements | 195,508 | 190.9 |

Farm receipt 去除重复 Snapshot 前，同一模型为：

```text
7 天模型：    100.7 KiB
硬窗口模型：  820.9 KiB
```

去除后分别下降约 86% 和 77%。

若非常保守地假设 3000 万用户都处于上述 7 天模型，仅 Player/Farm protobuf
净载荷约为：

```text
Player ≈ 0.981 TiB
Farm   ≈ 0.396 TiB
合计   ≈ 1.377 TiB
```

这还没有包含好友、分享、Workflow、Redis 元数据和副本。若峰值写入按 Player/Farm
各占 50%、且每次继续整记录 CAS，41.7k QPS 对应约 1 GiB/s 的应用层主写载荷。
所以“公司 Redis 能持久保存 Demo”不能推出“当前单实例/整记录模型可承载生产”。

这个结果给出下一项有证据的优化门：在真实 Redis 压测确认写放大不可接受后，将
Player operation receipt 拆为按 operation_id 的独立有 TTL 记录，或使用独立
Receipt 表；不能先把 Player/Farm 权威状态改成异步脏写来掩盖问题。

## 5. 本机微基准

环境：Linux amd64，AMD EPYC 7K62。结果只用于识别本地 CPU/分配热点。

```text
BenchmarkStaticRouteResolve
129.3 ns/op   0 B/op   0 allocs/op

BenchmarkMemoryCASEpoch（2 KiB payload）
751.4 ns/op   2048 B/op   1 alloc/op
```

第二项是内存 Store，不经过 Redis 网络、Lua、持久化或复制，不能当成数据库吞吐。
它只证明 route hash 不是当前 CPU 瓶颈，以及 Memory CASEpoch 的主要分配来自
payload copy。

## 6. 可观测性

已接入：

- Gate：连接数、接入/拒绝、慢客户端、收发帧/字节、frame size、处理延迟、
  write batch、drain lifecycle/剩余会话；
- Player/Farm Actor：active keys、mailbox queued/max depth、submit、
  mailbox rejection；
- Player/Farm Store：Get/Insert/CAS/Fence 延迟直方图和
  `ok/cas_conflict/writer_fenced/not_found/error` 结果；
- Worker：claim 数/错误、claim batch、workflow age、completed/retry；
- 四个进程：Go runtime 和 process metrics。

配置：

- `deploy/monitoring/prometheus.yml`
- `deploy/monitoring/grafana/dashboards/farm-gate-ws.json`
- `deploy/monitoring/grafana/dashboards/farm-capacity.json`

本地完整栈启动后：

```bash
cd deploy/monitoring
docker compose up -d
```

Prometheus：`http://127.0.0.1:19090`；Grafana：`http://127.0.0.1:13000`。

## 7. 真实压测执行方法与结果

Gate 和工具必须显式使用同一个测试专用 secret。secret 为空时 Gate 不接受
`load:<id>:<hmac>` 身份，避免压测后门被默认启用。

本地脚本生成一次性 secret，不输出到日志，只在本轮 Gate 和压测进程之间传递：

```bash
FARM_CAPACITY_DURATION=30m \
FARM_CAPACITY_JOIN_CONNS=400 \
FARM_CAPACITY_JOIN_RATE=40 \
FARM_CAPACITY_PLANT_CONNS=100 \
FARM_CAPACITY_PLANT_RATE=10 \
./scripts/run-local-capacity-stage
```

必须分阶段增加 `conns/rate`，并给每轮 `player-start` 使用未污染的新区间。每个阶段
同时保存：

- 工具 JSON：attempted/ready/peak_active、错误分类、ready P50/P95/P99、
  plant scenario 成功率/P95；
- Prometheus 快照：Gate latency/拒绝/慢连接、Actor mailbox、Store P99/冲突、
  Workflow age/retry；
- Pod CPU、RSS、GC、Redis CPU/内存/网络/慢日志；
- 测试配置、镜像/Commit、开始结束时间。

建议阶段：

1. 100 连接正确性；
2. 1,000 连接和真实 Plant；
3. 分离“连接容量”与“业务 QPS”，逐级爬坡；
4. 30 分钟稳态和 2 小时 soak；
5. 单 Gate drain、逐 Pod 滚动、单 Redis 节点切换；
6. 热门 Farm 和慢客户端专项。

停止条件不是只看平均延迟；任何错误结果、幂等重复发奖、writer fence、mailbox
持续积压、Workflow age 不收敛、RSS 无界增长或 Redis P99 失控都应立即停止并定位。

### 7.1 校准中发现并修复的真实问题

首轮校准先暴露 localhost 压测流量与 Gate 默认单 IP 防滥用限流冲突。日常默认
`10/s + burst 20` 保持不变，只在显式压测脚本中覆盖为 2000/4000/5000。

放开压测限流后又发现 `farm-svr` 先启动、Gate 9092 后监听时，gRPC channel
处于 reconnect backoff，Dispatcher 默认 fail-fast 会丢 Join Snapshot。修正为：

- `DeliverBatch(..., grpc.WaitForReady(true))`；
- Dispatcher 仍为 512 条有界队列，满时明确失败；
- Close 取消 context，不会因 Gate 长期不可用而阻塞退出；
- 记录 RPC error、缺失 result 与非 `QUEUE_ACCEPTED` 状态；
- Compose 让 Gate 下行监听与 farm-svr readiness 并行建立。

修正后的 30 秒校准为 80/80 Join、20/20 Buy+Plant，全部错误为 0。详见
`v121-m6-local-calibration.md`。

### 7.2 30 分钟结果

原始证据：

- `results/ws-join-20260730T044427Z.json`
- `results/ws-plant-20260730T044427Z.json`
- `results/runtime-20260730T044427Z.log`

执行窗口：`2026-07-30 04:44:46Z` 至 `05:14:46Z`。运行时采样 150 组，
第一组 `04:44:46Z`，最后一组 `05:14:42Z`。

| 场景 | configured | attempted | ready/succeeded | P95 | P99 | max | 全部错误 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Join | 400 | 400 | 400 | 7.46 ms | 8.54 ms | 24.74 ms | 0 |
| Buy+Plant | 100 | 100 | 100 | 6.89 ms（场景） | Join 7.96 ms | Join 8.82 ms | 0 |

两个进程 `unexpected_disconnects=0`。Gate `conn_rejected=0`、
`slow_client_kick=0`；Farm mailbox 最大深度、当前排队和拒绝均为 0。

测试账号正好 500 条。Gate 指标峰值为 502，因为开发页面另有 2 条交互连接；
它们不进入压测 JSON 的 attempted/ready/error 统计。

稳态资源区间（149 组 `active >= 500` 样本）：

| 进程 | CPU 区间 | RSS 区间 |
|---|---:|---:|
| farm-gate | 0–3.65% | 37.30–41.36 MiB |
| farm-svr | 0–2.68% | 18.90–23.28 MiB |
| player-svr | 0–2.77% | 13.47–21.61 MiB |
| farm-worker | 0.11–2.44% | 5.25–8.77 MiB |

Gate 空载首样本为 17.75 MiB。用全程稳态最大 RSS 计算，500 连接增量上界约
`(41.36 - 17.75) / 500 = 48.4 KiB/连接`。这只是低水位线性观察，不应直接
外推到数万连接；Gate 稳态 goroutine 为 1023–1028，约两条/连接，后续高水位
测试要重点观察栈、GC、fd 和调度开销。

Farm Store 直方图的本轮 P99 桶：

| 操作 | count | P99 所在桶 |
|---|---:|---:|
| Get | 502 | ≤ 0.5 ms |
| Insert | 500 | ≤ 0.5 ms |
| CAS | 256 | ≤ 1 ms |

Worker `claim_batch_size=0`、`claim_error_total=0`、`workflow_age_count=0`：
本轮 100 个 Plant Workflow 全部由在线 Coordinator 闭合，没有留下待恢复积压。

### 7.3 证据边界

这轮 Plant 是每连接建立后的单次真实 Buy+Plant，随后保持 WebSocket、Ping/Pong
和读循环 30 分钟。因此它证明：

- 小水位长连接稳定；
- Login/Hello/Join/Snapshot 与一条纵向写没有错误；
- 该水位下没有 mailbox/Workflow 积压或明显 RSS 漂移。

它不证明持续 30 分钟的 41.7k 业务 QPS，也不覆盖热门 Farm、持续 Theft/Care、
慢客户端、1k+ 连接、Redis 主从切换或 Gate Drain。下一轮必须把连接 soak 与
持续业务 QPS 分开测试。

## 8. 本轮验证与未执行项

已执行：

```text
当前 Player/Farm record size model             PASS
route + Memory CASEpoch microbenchmark          PASS
receipt/reservation/settlement 容量边界测试       PASS
Farm receipt Snapshot 写放大回归测试              PASS
ws-stress / Gate load-token 相关包编译与单测       PASS
Docker Compose 配置解析                           PASS
Grafana dashboard JSON 校验                       PASS
30 秒 80 Join + 20 Buy/Plant 校准                 PASS
30 分钟 400 Join + 100 Buy/Plant 连接浸泡          PASS
```

未执行项仍包括：1k+ 分级爬坡、持续写 QPS、热门 Farm、慢客户端、2 小时 soak、
逐 Gate Drain、Redis 主从切换和平台备份恢复。因此 M6 已形成第一份带实测的容量
基线，但生产 Go/No-Go 和最终 Pod/Shard/Redis 预算仍不能冻结。
