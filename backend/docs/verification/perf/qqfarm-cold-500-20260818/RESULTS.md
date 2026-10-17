# Cold Login 500/s Results

日期：2026-08-18

## 结论

本轮为有效的 500/s 冷登录轮次。两个 Job 各以 250/s 发起全新 Player ID，持续发起 300 秒；共完成 149,993 次冷登录，Ready 149,993 次，成功率 100%。

本轮不包含在线保持连接：每个用户 Ready 后关闭 WebSocket，结束时 `active_at_end=0`。

## Load result

| 指标 | Job A | Job B |
|---|---:|---:|
| attempted | 74,995 | 74,998 |
| ready | 74,995 | 74,998 |
| launch QPS | 249.99/s | 250.00/s |
| Ready completion QPS | 249.96/s | 249.97/s |
| Ready success | 100% | 100% |
| Ready P50 | 26.20ms | 31.62ms |
| Ready P95 | 49.05ms | 55.11ms |
| Ready P99 | 267.51ms | 284.20ms |
| Ready max | 2163.54ms | 1805.76ms |
| Login P95 | 28.44ms | 29.96ms |
| Hello P95 | 7.10ms | 9.93ms |
| Join accepted P95 | 8.11ms | 9.19ms |
| Snapshot P95 | 10.30ms | 10.49ms |
| Login/connect/hello/join errors | 0 | 0 |

Friend opportunity was asynchronous and did not block Ready. Its success rate was 100%; P95 was approximately 30ms and P99 approximately 547–604ms.

## Backend state

After the round:

```text
Player: player-svr-0=512, player-svr-1=512
Farm:   farm-svr-0=512,   farm-svr-1=512
pending_actions=0
degraded_shards=0
snapshot_revision=2049
Gate active WebSocket=0
Gate accepted WebSocket total=150,594
Redis dependency error percent=0
```

No new backend OOM or restart occurred during the round. Existing restart counts from startup/recovery are not attributed to this round.

## Resource observations

Maximum observed by `kubectl top` during the sampled window:

| Pod | CPU | RSS |
|---|---:|---:|
| player-svr-0 | 392m | 4013Mi |
| player-svr-1 | 964m | 4225Mi |
| farm-svr-0 | 493m | 4457Mi |
| farm-svr-1 | 579m | 4659Mi |
| load generator A | 622m | 40Mi |
| load generator B | 558m | 39Mi |

Gate CPU was visually around 50% during the round, but the Gate CPU pprof file was truncated when the auxiliary `kubectl exec` stream closed unexpectedly. It is not used as a precise CPU result.

## pprof

Valid profiles:

- `player0.cpu.pprof`, `player1.cpu.pprof`
- `player0.heap.pprof`, `player1.heap.pprof`
- `farm0.cpu.pprof`, `farm1.cpu.pprof`
- `farm0.heap.pprof`, `farm1.heap.pprof`
- `gate.heap.pprof`

Main heap finding:

```text
Player mailboxFor: approximately 85.8% / 87.7% of in-use heap
Farm mailboxFor:   approximately 76.9% / 75.3% of in-use heap
```

At the profile point:

```text
Player heap: approximately 253–265MB per Pod
Farm heap:   approximately 293–327MB per Pod
```

CPU was dominated by syscall/socket work, Go runtime scheduling/allocation and GC scanning; there was no single dominant business function.

## Actor retirement finding

Despite `active_at_end=0`, the backend still retained most actor runtime entries immediately after the round:

```text
Player active actors: approximately 71k per Pod
Player tracked keys: approximately 75k per Pod
Player idle retired counters: approximately 3.9k–4.1k per Pod
```

This confirms that closing the WebSocket does not immediately release Player/Farm Actor runtime state. The 30s idle setting improves candidate eligibility but does not increase retirement throughput. The configured/default sweeper limit is 256 candidates per 30s tick, or about 8.5 retire attempts/s per Pod, while this round created roughly 250 new identities/s per Player Pod. The retirement queue therefore grows much faster than it drains. A separate post-load drain observation is required before claiming that the memory returned to baseline.

The next diagnostic should smooth and raise retirement throughput, for example with a 5s sweep interval and a per-sweep limit sized above the per-Pod arrival rate, then repeat with a fresh ID range. The setting must be validated against Farm persistence/RPC load before being used as a production default.

## Validity

This is a valid 500/s cold-login capacity point, not a 1000/1500/2000/s conclusion. The next stage may use the `cold-1000` ID range only after the backend drain and 512+512 assignment remain stable.
