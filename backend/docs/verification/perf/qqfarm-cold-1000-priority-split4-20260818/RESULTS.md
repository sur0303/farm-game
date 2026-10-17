# QQFarm cold thick login: 1000/s priority split-4

Date: 2026-08-18

## Verdict

**Valid capacity point: 1000/s cold login.**

The 1000/s launch was split across four load-generator Pods at 250/s each. All four Pods used `high-cpu-priority` and remained Running for the complete five-minute launch. No load Pod was Pending or preempted during this stage.

This is a cold-login test. Connections were closed after Ready; it is not an online WebSocket-capacity result.

## Deployment gate

- Runtime image: `farm-route-runtime@sha256:cf50fc92ac3dc39204859ec3a431a3ded79cc07b499f87500a1a076292c1b9f0`
- Load image: `farm-ws-stress@sha256:d3c5c8d0931953bd9137736f37f517c19f8c491ac2cb1e9c4e67284e1335c7e6`
- Route snapshot revision: `2049`
- Player: `512 + 512`, each Holder `installed_memberships=512`
- Farm: `512 + 512`, each Holder `installed_memberships=512`
- All four Holders: `pending_actions=0`, `degraded_shards=0`, `serving_shard_count=512`
- Route Controller, Gate, Worker A/B, Player and Farm Pods: `high-cpu-priority`
- Four load Pods: `high-cpu-priority`

## Load configuration

| Job | Player ID range | Rate | Pool | Run ID |
|---|---:|---:|---:|---|
| split4-a-1 | 976000000-976082499 | 250/s | 82,500 | `dkrvxqpicow3` |
| split4-a-2 | 976082500-976164999 | 250/s | 82,500 | `dkrvxpxqz690` |
| split4-b-1 | 976165000-976247499 | 250/s | 82,500 | `dkrvxqk864dq` |
| split4-b-2 | 976247500-976329999 | 250/s | 82,500 | `dkrvxpwuc7ma` |

Launch duration was 300 seconds per Job.

## Results

| Job | Attempts | Ready | Ready rate | Actual QPS | Ready P95 | Ready P99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 74,961 | 74,956 | 99.9933% | 249.807/s | 63.675ms | 181.347ms |
| 2 | 74,919 | 74,915 | 99.9947% | 249.662/s | 64.314ms | 173.480ms |
| 3 | 74,954 | 74,950 | 99.9947% | 249.789/s | 63.937ms | 176.919ms |
| 4 | 74,957 | 74,952 | 99.9933% | 249.791/s | 68.554ms | 183.267ms |
| **Total** | **299,791** | **299,773** | **99.9940%** | **999.049/s** | — | — |

Total failure reasons: 36

- `friend_opportunity: context deadline exceeded`: 18
- `login: context deadline exceeded`: 13
- `connect: context deadline exceeded`: 4
- `connect: network timeout`: 1

There were no `cannot assign requested address` errors in this split/high-priority run.

## Resource samples

Measured from `top.tsv`; backend had 21 samples and each load Pod had 15 samples.

| Pod | CPU avg | CPU max | Memory avg | Memory max |
|---|---:|---:|---:|---:|
| Gate | 1.197 cores | 1.579 cores | 35MiB | 40MiB |
| Player-0 | 0.655 cores | 0.852 cores | 3.57GiB | 4.85GiB |
| Player-1 | 1.036 cores | 1.396 cores | 3.84GiB | 5.28GiB |
| Farm-0 | 0.781 cores | 1.093 cores | 4.24GiB | 5.82GiB |
| Farm-1 | 0.689 cores | 0.978 cores | 4.24GiB | 5.90GiB |
| Worker-A | 0.058 cores | 0.060 cores | 23MiB | 25MiB |
| Worker-B | 0.063 cores | 0.069 cores | 20MiB | 22MiB |
| Route Controller | 0.276 cores | 0.307 cores | 36MiB | 39MiB |
| Load-1 | 0.368 cores | 0.538 cores | 412MiB | 484MiB |
| Load-2 | 0.363 cores | 0.492 cores | 407MiB | 471MiB |
| Load-3 | 0.506 cores | 0.688 cores | 411MiB | 480MiB |
| Load-4 | 0.335 cores | 0.522 cores | 410MiB | 484MiB |

No new `Preempted` or `FailedScheduling` event occurred during the valid priority run. Earlier invalid split-4 events are not part of this result.

## Heap pprof

105 non-empty heap profiles were captured during the run. CPU pprof files were not successfully persisted and must not be inferred from this result.

Latest profiles were analyzed with the matching runtime binaries:

### Player

- `player-svr-0` in-use heap: about 2.37GiB
- `runtime.Executor.mailboxFor`: **87.22%** of in-use heap
- `cloneAuthorityModelState`: 3.40%
- `cloneRouteSnapshot`: 2.83%

### Farm

- `farm-svr-0` in-use heap: about 2.83GiB
- `runtime.Executor.mailboxFor`: **70.48%** of in-use heap
- `reflect.unsafe_New`: 7.23%
- `cloneRouteSnapshot`: 5.17%
- `cloneAuthorityModelState`: 2.53%
- `FarmActor.rememberJoinIdempotency`: 1.89%

### Gate

- In-use heap: about 13.4MiB
- Largest paths were gzip/flate writers, route snapshot cloning and buffered I/O; Gate was not the dominant memory bottleneck.

## Bottleneck assessment

The dominant backend retention path remains Actor mailbox allocation/retention (`Executor.mailboxFor`), not Gate memory, Worker CPU, or WebSocket active connections. The 1000/s run stayed within the configured 16GiB holder limits, but the Player/Farm heap growth remains the primary risk for higher or longer runs.

The previous single/two-Pod run's `cannot assign requested address` problem was avoided by splitting the load across four Pods. The remaining 36 failures were sparse timeout failures, mainly in the asynchronous friend-opportunity path.

## Artifacts

- `job*.log`: four complete load results
- `top.tsv`: resource samples
- `pods.tsv`: Pod state/IP timeline
- `pprof/`: heap profiles captured during the run
- `pprof-errors.log`: pprof collection errors

## Next step

A 1500/s run may be attempted with the same high-priority, multi-Pod layout, a fresh ID range, and a preflight gate requiring all load Pods to be simultaneously Running before launch. CPU pprof collection should be fixed before using CPU-hotspot conclusions.
