# QQFarm333 Hot-Shard Migration Results

Date: 2026-08-20
Cluster / namespace: `uc-test-plus / developer_026-qqfarm333`
Route domain / shard: `PLAYER / 0`

## Result

A single shard containing 20,000 persisted Player records was migrated while 2,000 selected Players were kept online through authenticated Login, Player bootstrap and own-Farm Join presence traffic.

| Leg | Source → target | RoutePlan elapsed | Result | Command unavailable |
|---|---|---:|---|---:|
| Forward | `player-svr-0 → player-svr-1` | **3,153 ms** | `SUCCEEDED` | **0** |
| Move-back | `player-svr-1 → player-svr-0` | **3,149 ms** | `SUCCEEDED` | **0** |

The earlier unoptimized baseline for the same 20k/2k shape was 23,352 ms. The optimized RoutePlan is approximately **7.4× faster**, an **86.5% reduction**.

The critical transition phase instrumentation for the optimized predecessor measured approximately 1.77 seconds from source stop through target ACTIVE publication; the remaining RoutePlan time is exact discovery, durable state transitions and terminal verification.

## Dataset and online population

- Planned and provisioned Player records: **20,000**
- All IDs hash to `PLAYER shard 0` under the recorded immutable route profile.
- Final evidence ID manifest SHA-256: `ede473a1e6651296bf7c4a92796a675a5019a49be98d2ebd41ec0c43ad16327a`
- Real warm Join: attempted/ready/succeeded `20,000 / 20,000 / 20,000`
- Warm failures/timeouts: `0 / 0`
- Sustained presence connections: **2,000**
- Source Player ACTIVE before move: **2,000**
- Remaining 18,000 records were absent from the ACTIVE set; idle-sweeper/mailbox retirement evidence was retained in Prometheus.

The move does not copy 20,000 Redis records. It fences and transfers shard Authority; source in-memory generations retire and target generations reactivate from durable state on subsequent traffic.

## Correctness evidence

- Forward and move-back plans: phase `SUCCEEDED`, attempt `1`, revision `7`.
- Snapshot publication mode: `IMMEDIATE` for this authenticated, exact-pinned manual move only.
- Forward fence: assignment/writer/lease epochs advanced atomically from `1/1/1` to `2/2/2`.
- Move-back used a newly observed exact fence; it did not reuse the forward fence.
- Final shard distribution returned to 256 shards on each Player Holder.
- Holder membership pending/inflight/degraded stayed at zero at terminal state.
- Mailbox rejected and degraded-shard series stayed at zero.
- Both migration windows had zero `command unavailable` log entries.
- Transition pressure was absorbed by exact pre-mailbox proof retries; final metrics contain `RESUMED` and `SAME_GENERATION_RETRY`, not timeout/exhausted failures.

## Runtime identity

Backend runtime:

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:d86c41d93029d5791980818d3ef0850f2724ace170deb5c7feb37375bb708ece
```

Tools image:

```text
mirrors.tencent.com/uc-backend/farm-hot-shard-tools@sha256:ad799503561291b2b2e85685182bbf7f024072ac58cd98d6dfedd894f9476965
```

Source branch: `codex/scaleplan-qqfarm-alt-perf-r1`.

## Grafana

原集群入口已从本存档删除。复习用图见 `screenshots/`。

### Screenshots

Annotated graphs mark the Forward (20:22:19) and Move-back (20:26:22) instants. The ten-minute time axis is the observation window; the migration itself takes about 3 seconds.

- [Full scaling/actor/command dashboard](screenshots/hot-shard-dashboard.png)
- [Player ACTIVE per Holder (migration source→target)](screenshots/player-active-per-holder-annotated.png) — recommended
- [Shard distribution](screenshots/shard-distribution-annotated.png)
- [Route revisions](screenshots/route-revisions-annotated.png)
- [Command correctness rate (100%)](screenshots/command-correctness-rate-annotated.png)
- [Command failures (0)](screenshots/command-failures-annotated.png)
- [Command results trend](screenshots/command-results-trend-annotated.png)
- [Command result matrix](screenshots/command-result-matrix-annotated.png)
- [Migration errors and rejects (0)](screenshots/migration-errors-annotated.png)
- [Online connections and ACTIVE actors (aggregate)](screenshots/online-active-actors.png)
- [Mailbox queue](screenshots/mailbox-queue.png)
- [Mailbox oldest wait](screenshots/mailbox-wait.png)

The zero-valued migration-error and command-failure panels are retained deliberately as correctness evidence, not omitted as empty charts.

Within the migration window the presence traffic accounted for:

- GET_PLAYER_BOOTSTRAP SUCCEEDED: 73,696
- JOIN_FARM ACCEPTED: 73,916
- total ~147,612 commands with zero failures, timeouts, or unavailable outcomes.

## Raw artifacts

- `artifacts/id-manifest-r6.json`
- `artifacts/warm-20k-r6.json`
- `artifacts/routeplans-r6.jsonl`
- `artifacts/routeplans-summary-r6.json`
- `artifacts/online-job-r6.json`
- `artifacts/kubernetes-final.txt`
- final Gate and Player Prometheus expositions

## Remaining work

This result qualifies the single-hot-shard path, not whole-workload Pod scaling. The next evidence set must run Player and Farm StatefulSet `4→5→4` separately with distributed online actors, retaining the same command, Actor, mailbox, shard-distribution and ScalePlan panels.
