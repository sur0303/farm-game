# M6 本地混合负载校准

日期：2026-07-30

## 第一轮：有效失败

配置：

- 80 个 Join-only 玩家，20/s；
- 20 个 Buy+Plant 玩家，5/s；
- 30 秒；
- Direct WebSocket、四独立容器、公司 Redis。

结果文件：

- `results/ws-join-20260730T042756Z.json`
- `results/ws-plant-20260730T042756Z.json`

| 场景 | attempted | ready | ready rate | P95 | 连接错误 | Join 错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Join | 80 | 26 | 32.5% | 7.56 ms | 36 | 18 |
| Plant | 20 | 10 | 50% | 7.08 ms | 6 | 4 |

成功进入 `InFarm` 的 10 个 Plant 全部完成 Buy+Plant，scenario P95 6.97 ms。
但本轮整体必须判失败，不能只报告成功请求延迟。

## 根因

两个压测进程都从 `127.0.0.1` 建连，合计入口速率 25/s。Gate 的生产默认入口保护是：

```text
GATE_WS_RATE_PER_SEC=10
GATE_WS_RATE_BURST=20
```

日志与连接数量符合被同 IP limiter 拒绝的特征。这不是单 Gate 的连接容量水位，
而是压测流量模型与防滥用策略冲突。

## 修正

- `docker-compose.local.yml` 显式暴露三个已有 Gate 配置：
  `GATE_WS_RATE_PER_SEC`、`GATE_WS_RATE_BURST`、`GATE_WS_MAX_CONNS`。
- 日常/生产默认仍是 10、20、0，不降低防护。
- `run-local-capacity-stage` 只在压测进程中覆盖为 2000、4000、5000。
- 修正 `ws-stress -scenario plant` 的 offer id：协议中的胡萝卜种子 offer 是 `1`，
  不是物品 id `1001`。

## 第二轮：暴露启动窗口下行丢失

放开仅限压测的入口限流后，100 个连接全部完成 Login 和 WebSocket Upgrade，
但仅有一部分收到 Join Snapshot。诊断压测将错误收敛为：

```text
logic downlink: DeliverBatch failed:
rpc error: code = Unavailable
dial tcp 127.0.0.1:9092: connect: connection refused
```

根因不是 Redis、Actor mailbox 或 WebSocket 写队列，而是本地编排顺序：

1. farm-svr 先健康；
2. farm-svr 建立到 Gate 9092 的非阻塞 gRPC channel；
3. farm-gate 后启动；
4. channel 尚在 reconnect backoff 时，Dispatcher 默认 fail-fast；
5. 启动窗口内的 Snapshot 被返回 `Unavailable`，旧代码又丢弃了 RPC error。

## 下行可靠性修正

- Dispatcher 调用 `DeliverBatch` 时使用 `grpc.WaitForReady(true)`；
- Dispatcher 队列仍然有界，满时 `Enqueue` 明确失败，不会无限堆积；
- `Dispatcher.Close` 取消 context，Gate 长期不可用不会阻止进程退出；
- RPC 错误、缺失结果和非 `QUEUE_ACCEPTED` 状态按批次记录日志；
- 本地 Compose 让 Gate 在 farm-svr `service_started` 后启动，下行监听可与
  farm-svr readiness 并行建立；Gate 自身 readiness 仍检查 farm-svr 健康；
- 单测断言 Dispatcher 必须携带 WaitForReady。

这项修正不是压测“调参”。多 Pod 滚动发布时 Gate 与 farm-svr 本来就是独立启动单元；
如果不等待 channel ready，任何部署启动窗口都可能丢 Join Snapshot。

## 通过的 30 秒校准

结果：

- `results/ws-join-20260730T044145Z.json`
- `results/ws-plant-20260730T044145Z.json`

| 场景 | attempted | ready | ready rate | Join P95 | 场景 P95 | 全部错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Join | 80 | 80 | 100% | 7.79 ms | - | 0 |
| Buy+Plant | 20 | 20 | 100% | 7.21 ms | 7.42 ms | 0 |

Gate 入口拒绝、慢客户端踢除、Actor mailbox 排队与拒绝均为 0。

## 30 分钟浸泡命令

校准通过后执行：

```bash
FARM_CAPACITY_DURATION=30m \
FARM_CAPACITY_JOIN_CONNS=400 \
FARM_CAPACITY_JOIN_RATE=40 \
FARM_CAPACITY_PLANT_CONNS=100 \
FARM_CAPACITY_PLANT_RATE=10 \
./scripts/run-local-capacity-stage
```

脚本每 10 秒保存 Gate/Farm Prometheus 指标和四个容器的 CPU/内存样本。

停止条件：任一错误计数非零、ready rate 小于 100%、unexpected disconnect 非零、
Workflow backlog 不收敛，或 Redis/Gate P99 持续恶化。
