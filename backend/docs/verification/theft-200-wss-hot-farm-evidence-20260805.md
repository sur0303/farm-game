# Theft 200-WSS hot-farm capacity evidence — 2026-08-05

## Result

**PASS.** At `51aa028` plus the capacity-only worktree changes, both the
baseline and slow-reader cases met every driver and read-only audit check.
Each case used newly built production `farm-gate`, `farm-svr`, `player-svr`
and `farm-worker` binaries, a separate fresh Redis `7.4.10` process, real
HTTPS login, and WSS through FarmGate.

The retained evidence stamp is `20260805T110040Z`. All earlier transient run
artifacts were removed.

## Composition and method

- Redis image: `redis:7.4-alpine`, reporting `redis_version=7.4.10`; persistence
  was disabled and the container was destroyed after each case.
- Gate static Authority composition: two Actor holder endpoints and two
  separately configured ServiceLB destinations. This exercises the
  `51aa028` route for `GET_OPERATION_RESULT` without ActorRoute metadata.
- TLS: Caddy internal CA in front of the production FarmGate WebSocket
  endpoint. The driver trusts only the generated CA and performs normal HTTPS
  login, WSS upgrade and `ClientHello` ticket authentication.
- Players: owner `920000000` and visitors `920000001..920000199`.
- Every client first joins its own farm and completes the public
  Snapshot/SubscriptionReady/RecoveryComplete path. The owner remains on that
  valid own-farm subscription; the 199 visitors switch to farm `920000000`.
- The owner creates the crop through public `BUY` and `PLANT`, confirms Plant
  via public operation lookup, and waits 2.5 seconds against a configured
  two-second maturity interval. The fixture helper creates only the friendship
  pair precondition and records zero Theft writes.
- After all 200 clients have observed SubscriptionReady on the hot farm, the
  199 visitors send public `THEFT` concurrently. Each operation is polled to a
  stored terminal outcome with its original operation ID. Every operation is
  also replayed once with that same operation ID; a transient unavailable
  admission is replayed immediately as directed, otherwise the replay occurs
  after terminal completion.
- The audit reads Redis authority and workflow records only after the public
  workload. It does not mutate Farm, Player, Theft or Workflow data.

Raw composition proof: [baseline authority](results/theft-200-wss-baseline-20260805T110040Z-authority.json),
[slow authority](results/theft-200-wss-slow-20260805T110040Z-authority.json),
[baseline environment](results/theft-200-wss-baseline-20260805T110040Z-environment.json),
[slow environment](results/theft-200-wss-slow-20260805T110040Z-environment.json).

## Driver results

| Observation | Baseline | Slow reader |
|---|---:|---:|
| WSS attempted / Ready / peak active | 200 / 200 / 200 | 200 / 200 / 200 |
| Login / connect / hello / join errors | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Hot SubscriptionReady barrier | 200 | 200 |
| Hot Join-to-Ready barrier elapsed | 70.803 ms | 70.974 ms |
| Hot Join-to-Ready barrier throughput | 2,824.74/s | 2,817.93/s |
| Per-client hot Join p50 / p95 / p99 | 34.178 / 69.730 / 70.069 ms | 55.555 / 69.920 / 70.314 ms |
| First sampled `farmgate_ws_conn_total=200` | 1.024 s after driver start | 1.026 s after driver start |
| Theft admission p50 / p95 / p99 | 3003.858 / 3008.690 / 6004.001 ms | 3006.242 / 3008.166 / 4687.839 ms |
| Stored terminal p50 / p95 / p99 | 6816.721 / 10014.066 / 10397.664 ms | 6443.726 / 9675.553 / 9945.980 ms |
| Immediate Processing / Unavailable | 190 / 9 | 194 / 5 |
| Public lookup queries | 9,823 | 9,628 |
| Lookup Processing observations | 9,599 | 9,408 |
| Transient lookup Unavailable retries | 25 | 21 |
| Same-operation retries / retry errors | 199 / 0 | 199 / 0 |
| Terminal Succeeded / Rejected / errors | 10 / 189 / 0 | 10 / 189 / 0 |
| Healthy connections completed | 200 | 199 plus intentional slow owner |
| Healthy FarmChange frames audited | 2,000 | 1,990 |
| Version gaps / regressions / RejoinRequired | 0 / 0 / 0 | 0 / 0 / 0 |

The admission distribution reflects the configured three-second online
operation budget. The p99 includes same-operation admission replay after the
initial unavailable response. The terminal figures stop when the original
operation ID becomes publicly queryable and terminal; they exclude the later
idempotency replay and broadcast-drain phase.

Raw driver evidence: [baseline driver](results/theft-200-wss-baseline-20260805T110040Z-driver.json),
[slow driver](results/theft-200-wss-slow-20260805T110040Z-driver.json),
[baseline samples](results/theft-200-wss-baseline-20260805T110040Z-runtime-samples.log),
[slow samples](results/theft-200-wss-slow-20260805T110040Z-runtime-samples.log).

## Frozen P0 conservation and settlement audit

Both independent cases produced the same authority result:

- base yield `500`, stealable limit `100`, final `stolen_total=100`;
- expected 2% gross per success `10`, with zero per-success mismatches;
- exactly 10 successful shares and exactly 10 unique TheftAttemptReceipt
  tuples;
- aggregate gross `100`, aggregate net `100`, visitor inventory increase
  `100`;
- guard loss `0` and owner guard compensation `0` in this deterministic
  no-guard fixture;
- 199 terminal workflows. Baseline classified them as 10 Succeeded, 74
  NoChange and 115 Rejected; slow classified them as 10 Succeeded, 62
  NoChange and 127 Rejected;
- 10 successful Farm participant receipts and 10 successful visitor Player
  participant receipts, all found by their namespaced invocation keys with
  terminal status, non-empty proof and canonical payload-hash binding;
- zero participant receipt mismatches, zero overdue/nonterminal workflows,
  zero active Farm settlements and no duplicate settlement after all 199
  same-operation replays;
- maximum worker convergence was 8,809 ms baseline and 8,583 ms slow.

The Farm public version began at `1` after Plant and ended at `11`. Every one
of the 200 healthy baseline connections observed exactly versions `2..11`.
Every one of the 199 healthy slow-case connections observed the same contiguous
sequence. Thus the ten authority commits, receipt/inventory totals and public
broadcast sequence explain one another without an extra settlement or missing
FarmChange.

All audit checks are `true`: [baseline audit](results/theft-200-wss-baseline-20260805T110040Z-audit.json),
[slow audit](results/theft-200-wss-slow-20260805T110040Z-audit.json).

## Slow-reader isolation

The owner stopped calling `ReadMessage` after the common Ready barrier and had
its TCP receive buffer requested at 1 KiB. All 199 healthy visitors still
reached public terminal outcomes, observed versions `2..11`, completed their
same-operation replay and remained free of unexpected disconnects. FarmActor
and downlink queue rejection counters remained zero.

The slow connection was **not shed**. After the 199 healthy clients completed,
the driver successfully read a buffered downlink frame and recorded
`still_connected_with_buffered_downlink`. FarmGate recorded zero
`farmgate_ws_slow_client_kick_total`; the bounded ten-change fanout did not fill
the connection send queue, so there was no production kick reason. This run
therefore proves isolation and continued responsiveness under the specified
slow-reader workload, not the higher-volume threshold required to force a
slow-client eviction.

FarmGate enqueued 2,400 downlink frames in each case: the 2,000 FarmChange
deliveries plus the per-connection snapshot/ready pushes. The slow driver's
1,990 count excludes the intentional owner's unread changes, while Gate's
2,400 total confirms those frames were still enqueued.

Raw counters: [baseline Gate metrics](results/theft-200-wss-baseline-20260805T110040Z-gate-metrics.prom),
[slow Gate metrics](results/theft-200-wss-slow-20260805T110040Z-gate-metrics.prom),
[baseline Farm metrics](results/theft-200-wss-baseline-20260805T110040Z-farm-metrics.prom),
[slow Farm metrics](results/theft-200-wss-slow-20260805T110040Z-farm-metrics.prom).

## Queue, Redis and process observations

Across both cases:

- Farm and Player mailbox rejection counters were `0`; the 250 ms samples saw
  maximum current mailbox depth and oldest wait at `0`.
- Farm downlink queue-full and RPC-failure counters were `0`.
- FarmGate reached 200 active WSS connections with zero connection rejection,
  zero slow-client kick and 2,400 successful downlink enqueues.

Maximum sampled production-process resource observations:

| Process | Baseline CPU / RSS | Slow CPU / RSS |
|---|---:|---:|
| `farm-gate` | 39.8% / 61.9 MiB | 66.0% / 62.9 MiB |
| `farm-svr` | 235.0% / 82.1 MiB | 258.0% / 81.4 MiB |
| `player-svr` | 78.6% / 65.6 MiB | 87.2% / 63.9 MiB |
| `farm-worker` | 22.6% / 30.7 MiB | 25.5% / 30.7 MiB |

`ps` CPU can exceed 100% for multithreaded processes. Maximum sampled Go
goroutines were Gate 640/640, Farm 435/436, Player 378/275 and Worker 20/21
(baseline/slow). Maximum sampled Go allocated bytes were approximately Gate
21.3/22.1 MiB, Farm 42.3/39.5 MiB, Player 29.0/30.8 MiB and Worker 5.2/4.9 MiB.

Selected Redis command observations (baseline/slow calls and mean time):

- `EXISTS`: 531,795 / 545,789 at 0.55 / 0.55 µs;
- `HGET`: 96,687 / 96,215 at 0.69 / 0.67 µs;
- `HMGET`: 42,285 / 42,736 at 5.84 / 5.69 µs;
- `EVAL`: 1,890 / 1,947 at 45.19 / 42.28 µs;
- `EVALSHA`: 2,467 / 2,457 at 267.97 / 263.25 µs.

Redis histogram bucket upper bounds were: `EXISTS` p99 2 µs, `HGET` p99
2 µs, `HMGET` p99 33 µs, `HSET` p99 33 µs, `ZRANGEBYSCORE` p99 16 µs and
`EVALSHA` p95 2.113 ms / p99 4.227 ms in both cases. Redis reported zero
rejected calls. `EVALSHA` reported 20 baseline and 32 slow failed calls while
the application completed every public operation and every audit invariant;
the raw commandstats are retained rather than hiding those contention-path
observations.

Raw Redis evidence: [baseline commandstats](results/theft-200-wss-baseline-20260805T110040Z-redis-commandstats.json),
[slow commandstats](results/theft-200-wss-slow-20260805T110040Z-redis-commandstats.json),
[baseline latency histogram](results/theft-200-wss-baseline-20260805T110040Z-redis-latency-histogram.json),
[slow latency histogram](results/theft-200-wss-slow-20260805T110040Z-redis-latency-histogram.json).

## Verification commands

The final worktree was verified with:

```text
go test -count=1 ./cmd/ws-stress ./cmd/theft-hot-audit
go test -race -count=1 ./cmd/ws-stress ./cmd/theft-hot-audit
go test ./...
git diff --check
```

No production file under `internal/**`, `farm-gate`, `farm-svr`, `player-svr`
or `farm-worker` was modified by this capacity work.
