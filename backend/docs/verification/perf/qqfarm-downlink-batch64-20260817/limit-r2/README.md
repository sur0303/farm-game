# Join 进入线路极限复测（batch=64 热现场）

同一 `developer_026-qqfarm` 拓扑，旧 `join` 口径，10000 玩家 / 15s。未重建环境。

| rate | Ready | P95 | Snapshot P95 | disc | queue-full Δ | run |
|---:|---:|---:|---:|---:|---:|---|
| 2500 | 10000 | 272 ms | 129 ms | 31 | 16 | `dkqww725yr9l` |
| 3000 | 10000 | 1321 ms | 1097 ms | 54 | 8578 | `dkqwwidr6z0i` |
| 3500 | 10000 | 3355 ms | 3089 ms | 79 | 28375 | `dkqwwtwp6mvo` |
| 4000 | 10000 | 3564 ms | 2845 ms | 52 | 30175 | `dkqwx5lny23h` |

先饱和的是 FarmSvr 单 Gate downlink lane（`farm_downlink_queue_full_total`）。Gate writer reject 未出现；Worker 空闲；CPU 都未打满 4 核。
