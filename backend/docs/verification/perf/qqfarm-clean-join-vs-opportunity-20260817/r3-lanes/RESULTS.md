# Clean Join vs opportunity 2026-08-17 r3 (lane workers + scan batch 64)

Gate-first clean reset。相对 r2 只改了两处、其余拓扑不变：

- Farm `DOWNLINK_LANE_WORKERS=4`（同一 Gate 上按 connection 分片的 4 条下行 lane；单连接仍 FIFO）
- Player `FRIEND_OPPORTUNITY_SCAN_BATCH_SIZE=64`（50 好友一次 Header RPC）
- `DOWNLINK_BATCH_SIZE` 仍 32，队列仍 256
- Gate 仍 `244ab98e`（recovery pending-join）
- Player/Farm 镜像 `25b3a71e`（新 farm-svr + player-svr）
- 无 Holder pprof

Farm uid `035460cd-3cdc-4db3-8bf2-54e0bc5d5e61`。证据目录即本目录。

## Join 10000 阶梯

| 档 | run | ready | disc | Ready P95 | queue-full Δ | 判定 |
|---|---|---|---|---|---|---|
| smoke 1 | `dkqz7gvbn545` | 1 | 0 | 37.6 ms | 0 | PASS |
| 2500/s | `dkqz7pjnwwjr` | 10000 | 0 | **67.8 ms** | 0 | 无损 PASS |
| 2750/s | `dkqz852cdpuh` | 10000 | 0 | **61.7 ms** | 0 | 无损 PASS（r2 同档 P95 285ms） |
| 3000/s | `dkqz8hpwqpzh` | 10000 | 0 | **79.1 ms** | 0 | **无损 PASS**（r2 同档丢 1 + queue-full +125） |
| 3500/s | `dkqz8uffbvno` | 9999 | 0 | 327.7 ms | 0 | 延迟超 200ms；1 次 login 401 |
| 4000/s | `dkqz98d9eehe` | 9845 | 0 | 557.7 ms | 0 | FAIL：155 次 `ERR_SERVER_BUSY` |

Join 极限：无损容量 **3000/s**（r2 为 2500/s）。3500 起 P95 掉出 200ms；4000 硬失败在 Join `服务器繁忙`，**downlink queue-full 仍为 0**。

4000/s 采样 CPU（4C8G limit）大约 Gate 0.7 / Farm 0.4 / Player 0.5，内存 Farm ~2.5Gi / Player ~2Gi，仍没打满。墙从「单 lane 队列满」换成了 Join 侧 busy。

## Opportunity-only thick-login 300 人阶梯

50 好友 / 密度 20%。测窗内 Gate 好友限流临时 `10000/20000`，测后已恢复 `20/40`。

| 档 | run | ready | FOUND | Ready P95 | friend startup P95 | 判定 |
|---|---|---|---|---|---|---|
| smoke 1（无好友） | `dkqza3jxn5if` | 1 | NONE | 44.2 ms | 16.1 ms | PASS |
| 100/s | `dkqzauszmy8h` | 300 | 300 | **64.9 ms** | **52.2 ms** | PASS（r2 263 / 219 ms） |
| 150/s | `dkqzbt2etmis` | 300 | 300 | **65.9 ms** | **52.7 ms** | PASS（r2 1751 / 1412 ms） |
| 200/s | `dkqzcp035873` | 300 | 300 | **80.3 ms** | **64.5 ms** | PASS（r2 2000 / 1727 ms） |
| 250/s | `dkqzdgh66zez` | 300 | 300 | **124.2 ms** | **107.1 ms** | **舒适 PASS**（r2 硬失败 100×503） |
| 300/s | `dkqzf8b98ntb` | 300 | 300 | 241.5 ms | 207.0 ms | 功能过、延迟拐点 |
| 400/s | `dkqzg0jirlw1` | 300 | 300 | 288.2 ms | 254.7 ms | 功能过 |
| 500/s | `dkqzgrnlcv2n` | 300 | 300 | 250.6 ms | 217.7 ms | 功能过、无 503 |

Opportunity 极限：舒适容量 **250/s**（r2 为 100/s）。300/s 起 Ready/好友启动 P95 > 200ms。500/s 仍 300/300 FOUND、0 个 503。300 连接在 ≥300/s 时注入窗口不到 1s，更高档不再代表持续 QPS。

100–250/s 的 PASS **不是** 生产 Gate `20/40` 下的有效容量；那个保护会先 429。

## 对照 r2

| 口径 | r2（单 lane + scan 32） | r3（4 lane + scan 64） |
|---|---|---|
| Join 无损 | 2500/s，P95 79ms | **3000/s**，P95 79ms |
| Join 硬失败 | 3000/s，queue-full +125 | **4000/s**，`ERR_SERVER_BUSY` ×155，queue-full 0 |
| Opportunity 舒适 | 100/s，P95 263/219ms | **250/s**，P95 124/107ms |
| Opportunity 硬失败 | 250/s，100×503 | 500/s 仍无 503；延迟拐点 300/s |
