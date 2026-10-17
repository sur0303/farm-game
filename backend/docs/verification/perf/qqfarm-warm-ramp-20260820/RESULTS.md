# QQFarm Warm Ramp 与 Join 压测报告

日期：2026-08-20  
集群：`uc-test-plus`  
Namespace：`developer_026-qqfarm`  
测试入口：`farm-gate-public` / `farm-gate` Service DNS  

> 本报告是业务 Warm 压测报告，不等同于 Gate 的 3000/s 冷请求容量结论。Warm Journey 的 QPS 是实际业务命令 QPS；历史冷 Gate 结论仍是 3000/s 舒适、3100/s 尾延迟拐点、3200/s 上沿、6400/s 未通过。
>
> **2026-08-20 后续结论更新：** 本文第 1–7 节保留最初 20–100 Bot 的历史过程；最新固定 2 Gate / 2 Player / 2 Farm / 2 Worker / 3 Redis 拓扑已经严格通过 5000 Bot。完整方法、安全边界和 A/B 数据见 [`online-5000-optimization.md`](online-5000-optimization.md)，它取代本文早期“75 Bot 上限”作为当前结论。

## 1. 本次部署与安全修复

本轮 reset 使用：

```text
UC_1024_INITIAL_ASSIGNMENT_OK
challenge=481b9070a62e969f0060d34bdd5ee97e
shards=1024 holders=2+2 workers=512+512 downlink=ready
```

运行时镜像：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:70da4dbc883a279b0a9122959eacafd65564e0055615331f7c4bda9945a3c3c9
```

当前拓扑：

| 组件 | 副本/配置 |
|---|---:|
| Gate | 1 |
| Player Holder | 2 |
| Farm Holder | 2 |
| Worker | 2，512+512 shards |
| Redis | 3 |
| Shards | 1024 |
| Gate Redis pool | 64 |
| Activation | 4000 / 8192 |

本轮修复：

- `cmd/route-controller/plan_runtime.go`
  - 已知的 `Snapshot changed during refresh` 无写入冲突会释放 control token；
  - 避免 30 分钟后 control token 过期并永久毒化 Route Controller。
- `internal/livebot/runner.go`
  - 运行窗口结束时停止新 bot，但允许已发出的操作 drain；
  - active bot 等待期间也会观察 run deadline，避免卡在无限等待。
- `internal/farmbotclient/commands.go`
  - 增加 `PlantAdmission`、`HarvestAdmission`、`UnlockPlotAdmission`。
- `internal/livefarmbot/executor.go`
  - Warmup 解锁采用共享并发闸门，默认最多 4 个；
  - sustained Journey 对 `PROCESSING` 只记录 pending，不重复原 operation，也不让单个慢 Durable Workflow 拖死全部 bot；
  - SELL 仍以 Player 可见库存为准。

测试通过：

```bash
go test ./cmd/route-controller \
  ./internal/scaleplan/adapters/control \
  ./internal/farmbotclient \
  ./internal/livefarmbot \
  ./internal/livebot \
  ./cmd/live-farm-bot
```

## 2. 场景与机器人比例

### Warm Farm

- 场景：`live-farm-bot --scenario=journey`
- 机器人：自有 Farm、真实 Player/Farm 路由
- think time：5s
- full run：6m run window、5m online session
- 启用：`--unlock-plots`
- 占本次 Farm 业务机器人：100%
- 命令：`BUY / PLANT / CARE / HARVEST / SELL`
- `UNLOCK_PLOT`：warmup 阶段单独统计

### Cold Join

- 工具：`ws-stress`
- 场景：`join-accepted`
- `WS_STRESS_SKIP_FRIEND_OPPORTUNITY=1`
- 50 和 100 connections 各 100% Join 场景

### Hot Farm / THEFT

- 本轮：0 个，0%
- 原因：双向 Friendship ring 和完整真实目标 Farm fixture 尚未完成。
- 未使用 fake farm、H5 share code 或伪造 THEFT 目标。

## 3. Warm Ramp 结果

PASS 判定：

- Ready 全部成功；
- bot 无 failed/cancelled/timeout；
- rejected/unavailable 为 0；
- delivery ratio >= 1；
- Driver qualified；
- `PROCESSING` 单独计为 pending，不冒充 terminal success。

| 场景 | Bot | Action QPS | P95/P99 ms | Actions | Rejected | Cancelled | 正确率 | 判定 |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Warm Journey full | 20 | 5.21 | 35.4 / 41.0 | 1,573 | 0 | 0 | 100.000% | PASS |
| Warm Journey full | 50 | 12.92 | 35.0 / 43.0 | 3,930 | 0 | 0 | 100.000% | PASS |
| Warm Journey canary | 75 | 19.87 | 28.1 / 29.9 | 1,328 | 0 | 0 | 100.000% | PASS |
| Warm Journey full | 75 | 19.26 | 30.0 / 45.9 | 5,893 | 0 | 0 | 100.000% | PASS |
| Warm Journey canary | 80 | 21.06 | 28.5 / 33.0 | 1,413 | 0 | 0 | 100.000% | PASS-canary |
| Warm Journey full | 80 | 20.44 | 35.4 / 47.1 | 6,280 | 2 | 0 | 99.968% | STOP |
| Warm Journey canary | 90 | 23.53 | 28.0 / 29.8 | 1,592 | 1 | 0 | 99.937% | STOP |
| Warm Journey canary | 100 | 25.79 | 28.2 / 29.9 | 1,772 | 1 | 0 | 99.944% | STOP |

### 结论

- **当前可接受的持续 Warm 上限：75 bots，约 19.26 action/s。**
- 80 bots 的 full run 首次出现 2 个 `ERR_STORE_UNAVAILABLE`，对应 Gate 日志中的后端暂不可用；不把它算作通过。
- 90/100 canary 各出现 1 个拒绝，因此没有继续扩大到 100、200 或 1000 Warm。
- 这不是 Gate 冷容量极限；它是当前单 Gate、1024 shard、真实 Journey Durable/Player/Farm 业务链路的安全上限观测。

## 4. 命令构成与正确性

### Full Warm 命令数量

| Full run | BUY | PLANT | CARE | HARVEST | SELL | Actions | Warmup UNLOCK_PLOT |
|---|---:|---:|---:|---:|---:|---:|---:|
| 50 bots | 868 | 868 | 564 | 817 | 813 | 3,930 | 928/950 |
| 75 bots | 1,302 | 1,302 | 850 | 1,222 | 1,217 | 5,893 | 1,384/1,425 |
| 80 bots | 1,389 | 1,389 | 904 | 1,303 | 1,295 | 6,280 | 1,481/1,520 |

`UNLOCK_PLOT` 的分母是每个 Farm 最多 19 个 warmup unlock。出现 `PROCESSING` 时，客户端保留原 operation ID 并停止该 bot 的继续解锁，不重复发送同一块地；该 pending 状态在命令指标中单独可见。

### 80 bots 的失败信号

Prometheus/Gate command ledger 观察到：

```text
ERR_STORE_UNAVAILABLE
status=UNAVAILABLE
```

Gate 日志还记录了 HARVEST/SELL 的暂不可用返回；这类结果不能算正确，因此 80 full 判定 STOP。

## 5. Cold Join 结果

| Connections | Launch rate | Ready | Ready rate | 实际 QPS | Ready P95 | Login P95 | Join P95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 50/s | 50/50 | 100% | 48.67 | 28.9ms | 17.5ms | 7.3ms |
| 100 | 100/s | 100/100 | 100% | 97.16 | 33.7ms | 17.6ms | 8.1ms |

两轮均无 Login/Hello/Join error。

## 6. 观测与截图

Grafana：

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm \
  port-forward svc/farm-grafana 33000:3000
```

Dashboard：

```text
http://127.0.0.1:33000/d/farm-command-correctness/farm-load-c2b7-rpc-and-command-correctness
```

Dashboard UID：`farm-command-correctness`。已配置命令吞吐、结果矩阵、正确率、P95、RPC client/server 与各服务 PodMonitor。

本目录截图：

- [`screenshots/ramp-summary.png`](screenshots/ramp-summary.png)
- [`screenshots/command-mix-correctness.png`](screenshots/command-mix-correctness.png)

Grafana 容器没有 Image Renderer 插件，因此两张 PNG 是从同一 Prometheus/Grafana 数据源生成的证据快照；原始 Dashboard JSON、Prometheus targets 和每轮 JSON 结果同时保存在 `artifacts/`，不是手工编造业务数字。

当前 Prometheus target：9/9 healthy，包含 Player、Farm、Gate、Worker、Route Controller。

Route Controller 在压测期间仍会出现可恢复的：

```text
Snapshot changed during refresh
```

但本轮没有再次出现 `token expired` 或 `actor route: holder lease expired`。该 warning 仍应在正式答辩前继续治理，不能隐藏。

## 7. 当前止损与后续

已停止继续扩大 Warm 数量，原因是 80 full 首次出现 `ERR_STORE_UNAVAILABLE`，75 full 是最后一个零错误持续档位。

下一步只有在完成以下工作后才能继续向 100/200/1000：

1. 找出 80 full 的 `ERR_STORE_UNAVAILABLE` 对应的 Player/Farm/Redis store 失败来源；
2. 继续消除 Route Controller 的 Snapshot refresh conflict warning；
3. 完成真实 FarmRecord、双向好友 ring 和 Hot Farm THEFT fixture；
4. 重新验证 80 full 至少一轮零错误；
5. 再以 90、100、200 逐级扩大。

历史阶段结论：**当时 Warm Journey 的保守持续容量是 75 bots / 19.26 action/s；100 bots 不是通过档位。** 该结论已经被后续 5000 Bot 优化验证取代，不能再作为当前容量结论。

## 8. 5000 Bot 当前结论（后续更新）

最新正式签字轮次：`n5000-shared-due-observe-r1`。

| Bot | Ready/Succeeded | Actions | Reject/Timeout/Pending | Worst P95/P99 | Worker critical | Backlog end | 判定 |
|---:|---:|---:|---:|---:|---|---:|---|
| 5000 | 5000/5000 | 241459 | 0/0/0 | 49.81/453.76ms | 全程 Ready | 0 | **PASS** |

当前 runtime：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:bc1dcd5808f5c862c8725ea5002bb6ee57d90061196b618d56fff009c4c8c766
```

关键变化包括首轮业务 spread、Direct/Worker 路径分离、热 Player Binding snapshot、LoadAndClaim V5 单次快照、仅对已完整审计 WriterClaim 开放的 Commit V11 热路径、Care/Harvest 已提交 SourceOrder envelope，以及按 bucket 共享的 family DueIndex discovery/readiness cache。Durable Order、Lease/Fence、WorkLease、DueIndex、Receipt 和 UNKNOWN fail-closed 均保留。

证据：`artifacts/online-5000-optimization/n5000-shared-due-observe-r1/`。
