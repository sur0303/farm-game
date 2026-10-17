# qqfarm batch=32 冷退回 + Join pprof（2026-08-17）

按 `prepare-qqfarm-loadtest` 冷部署：Worker 在 revision 1 预热，12 claims 到 revision 13。Farm `DOWNLINK_BATCH_SIZE=32`，Holder/Gate 启动前打开 pprof。镜像仍是 `farm-route-runtime@sha256:0d533888...9ec026`。

| 档 | Ready | P95 | Snapshot P95 | disc | queue-full Δ | run |
|---|---:|---:|---:|---:|---:|---|
| Join smoke | 1 | 42 ms | 6 ms | 0 | 0 | `dkqx6ne9vysf` |
| 2500/s | 10000 | 78.5 ms | 22 ms | 48 | 0 | `dkqx74volhu9` |
| 3000/s | 10000 | 235 ms | 95 ms | 83 | 0 | `dkqx89lee5x2` |
| thick-login smoke | 1 | 37 ms | 5 ms | 0 | — | `dkqx8vouaiv6`（机会 `NONE`） |

3000/s 的 12s CPU pprof：Farm 13.14 CPU-s、Gate 12.42、Player 8.97。Farm 业务热点是 Authority `Commit`/`LoadAndClaim`/`MigrateV1ToV2` 和 Actor 模型校验，不是 256 队列；`DeliverBatch` 本身只有约 0.12s flat。

关 Gate pprof、窗口内不采 profile 的对照：2500/s `dkqxet5svbzv` Ready 10000、P95 76.4 ms、disc **49**、queue-full 0、slow-client kick 0。与开 pprof 的 48 次同量级，断线不是 pprof 造成的。

断线根因：`farm_gate_command_rejected_total{reason="RECOVERY_INCOMPLETE"}=49`，与 disc 一一对应。ws-stress 在收到 Snapshot 后立刻发 `ClientRecoveryComplete`；Gate 此时还没把 Join 晋升成 ActiveSubscription，`handleControl` 返回 `ErrRecoveryIncomplete`，WebSocket 层按 HandleBinary 错误直接 `conn.Close()`。
