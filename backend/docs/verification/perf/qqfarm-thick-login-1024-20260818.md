# UC Test Plus 1024-shard Thick Login Verification

- Cluster: `uc-test-plus`
- Namespace: `developer_026-qqfarm`
- Redis tuple: `uctestscaleplanqqfarm20260816/z1`
- Topology: 2 Player holders + 2 Farm holders; 1024 PLAYER + 1024 FARM shards
- Route Controller image: `farm-route-runtime@sha256:5b47f683a91ac9204aca2186e236dc0e98a66b52990278180a45f24747ee1a1c`
- No commit made.

## Deployment

PASS. The UC-specific reset uses the A9-proven initial-assignment sequence with UC identities and ports:

```text
bootstrap-only revision 1
legacy/off Controller bootstrap health
HolderControl/runtime identity discovery
k8s-canary initial assignment batch
1024 PLAYER + 1024 FARM
2 holders per domain, 512 shards each
```

Required UC difference:

```yaml
publishNotReadyAddresses: true
```

on the `route-controller` Service. Without it, Holder init barriers cannot reach the Controller while its readiness probe is intentionally 503 during initial assignment.

Observed final Holder readiness:

```text
player-svr-0: installed_memberships=512, serving_shard_count=512
player-svr-1: installed_memberships=512, serving_shard_count=512
farm-svr-0:   installed_memberships=512, serving_shard_count=512
farm-svr-1:   installed_memberships=512, serving_shard_count=512
snapshot_revision=2049
assignment_healthy=true
degraded_shards=0
```

Farm→Gate downlink probe: PASS.

The standard UC `preflight --require-ready` rejects this environment because it is hard-coded for the historical 1-holder/4-shard topology; this is a tooling topology mismatch, not a runtime readiness failure.

## Smoke

Full Join smoke:

```text
run_id=dkrimwkvvazz
attempted=1 ready=1
login_errors=0 connect_errors=0 hello_errors=0 join_errors=0
unexpected_disconnects=0
```

Thick Login smoke:

```text
run_id=dkrj49o7ec7s
attempted=1 ready=1
bootstrap_errors=0 friend_startup_errors=0
profiles/tasks/mail/compendium/pet=1
```

## Load tests

### Control: 3000 connections at 200/s for 60s

```text
run_id=dkrj5p6yp1cx
attempted=3000 ready=3000
ready_rate=1
ready_p95=41.886ms
login/connect/hello/join errors=0
bootstrap_errors=0 friend_startup_errors=0
profiles/tasks/mail/compendium/pet=3000
```

### Target: 3000 connections at 1000/s for 60s

```text
run_id=dkrj78baewh5
attempted=3000 ready=3000
ready_rate=1
ready_p95=51.232ms
ready_p99=61.553ms
login/connect/hello/join errors=0
bootstrap_errors=0 friend_startup_errors=0
profiles/tasks/mail/compendium/pet=3000
friend_opportunity_completed=3000
```

## Bot-pool note

The returning-player fixture seed was attempted after the clean reset but failed with:

```text
blobstore: writer epoch mismatch
```

The failed viewer/fake ranges were not reused. The successful load results above use disjoint first-time player ranges, so they validate the 1024-shard Thick Login path but are not returning-player pool evidence.

## Limit staircase

All runs used disjoint first-time player ranges and 60-second Thick Login runs:

| Connections | Ramp | Ready | Errors | Ready P95 | Run ID |
|---:|---:|---:|---|---:|---|
| 5,000 | 2,000/s | 5,000/5,000 | 0 | 1.345s | `dkrnrx6z47n0` |
| 6,000 | 2,200/s | 6,000/6,000 | 0 | 1.211s | `dkrnusyy6zlo` |
| 7,500 | 2,500/s | 7,498/7,500 | 1 login + 1 store error | 3.160s | `dkrnttdndezj` |
| 10,000 | 3,000/s | 9,521/10,000 | 479 Join/opp. busy/unavailable errors | 5.504s | `dkrnsu9j83yp` |

Conclusion:

```text
strict zero-error observed boundary: 6,000 connections / 2,200/s ramp
practical degraded boundary: approximately 7,500 connections
hard overload observed: 10,000 connections
```

The `ramp` value is connection establishment rate; this is not a claim of
sustained business QPS equal to the ramp value. At 10,000, the dominant failure
was Gate/backend admission pressure (`ERR_SERVER_BUSY`), while login bootstrap
itself completed for all 10,000 attempted players.

## Bottleneck diagnosis

pprof was not enabled in the assigned Holder Pods (`HOLDER_PPROF_ENABLED` is
absent). The assigned Holders were not restarted just to enable it. Existing
metrics are sufficient to identify the first bottleneck:

```text
Gate farm_gate_command_backend_rejected_total{reason="BACKEND_SLOTS_FULL"}=476
Gate farm_gate_command_backend_rejected_total{reason="RPC_UNAVAILABLE"}=3
```

Those 476 full-slot rejects match the 476 `ERR_SERVER_BUSY` Join/opportunity
failures in the 10,000 run. Farm downlink remained healthy:

```text
farm_downlink_rpc_failure_total=0
farm_downlink deliver_batch result=ok
farm_redis_dependency_error_percent{authority,projection,runtime}=0
```

Player activation was a secondary signal, not the first limit:

```text
farm_actor_activation_load_fail_total{actor_kind="player"}=61
farm_actor_activation_throttled_total{actor_kind="player"}=0
farm_actor_mailbox_rejected_total{actor_kind="player"}=0
```

The Pods were not CPU-bound at observation time: Player was approximately
1.0 GB RSS each, Farm approximately 1.28 GB RSS each, Gate approximately
0.8 GB RSS, and CPU remained below the 2-CPU Holder / 2-CPU Gate limits. The
first actionable bottleneck is therefore Gate backend admission slots
(`GATE_BACKEND_MAX_INFLIGHT=1024`), followed by Player actor load/Redis-backed
bootstrap pressure as concurrency rises. Increasing the slot limit alone must
not be reported as a capacity improvement; it requires a controlled rerun with
fresh ranges and the same error/latency acceptance criteria.

## pprof and `GATE_BACKEND_MAX_INFLIGHT=3096` comparison

For the controlled 3096-slot rerun, pprof was enabled before Holder/Gate
creation and 30-second CPU profiles were captured:

```text
pprof-inflight3096/player.cpu.pprof
pprof-inflight3096/farm.cpu.pprof
pprof-inflight3096/gate.cpu.pprof
```

Run:

```text
run_id=dkro2somc5yi
connections=10000 ramp=3000/s duration=60s
ready=8460/10000
join_errors=1540
Ready P95=9.567s
```

The larger slot limit removed the previous direct backend-slot rejection, but
made the downstream failure worse:

```text
farm-svr-1 farm_downlink_queue_full_total=52285
farm-svr-1 farm_downlink_rpc_failure_total=52
farm-svr-1 deliver_batch unavailable=52
Gate join_pending_timeout=1067
Gate join_accept_without_snapshot=1067
```

The failed joins were `REJOIN_REQUIRED`/network timeouts, not Player bootstrap
errors. CPU profiles showed runtime syscall/allocation/protobuf work rather
than a single CPU-hot business function; the live CPU limits were not reached.
The current bottleneck is therefore the Farm→Gate downlink queue/connection
acceptance path after Gate admits too many concurrent backend commands. Keep
3096 as a diagnostic experiment only; it is not a capacity improvement over
1024.

## Guarded 1024 rerun after downlink guard

The runtime was rebuilt with downlink queue gauges and the reset procedure was
updated to verify Farm→Gate `:9092` connectivity before declaring the cell ready.
The Gate slot limit was restored to 1024. A fresh range was used:

```text
run_id=dkroocvzjobz
connections=10000 ramp=3000/s duration=60s player_start=947060000
ready=9602/10000
join_errors=398
Ready P95=4.174s
```

All 398 failures were explicit Gate backend-slot busy responses. Unlike the
3096 run, there was no downlink cascade:

```text
farm_downlink_queue_full_total delta=0
farm_downlink_rpc_failure_total delta=0
farm_gate_join_pending_timeout_total delta=0
farm_gate_join_accept_without_snapshot_total delta=0
farm_gate_command_backend_rejected_total{reason="BACKEND_SLOTS_FULL"} delta=398
```

30-second CPU profiles are in:

```text
pprof-guard-1024/player.cpu.pprof
pprof-guard-1024/farm.cpu.pprof
pprof-guard-1024/gate.cpu.pprof
```

This confirms the 1024 admission limit is currently acting as useful
backpressure: it sacrifices 398 overloaded Join attempts but keeps the Farm
→Gate delivery path lossless and avoids the 52k queue-full cascade observed at
3096.

## V2-first and bounded post-Ready admission rerun

The runtime was rebuilt with these production changes:

```text
new Player/Farm authority rows: ModelLayoutV2
GATE_BACKEND_ADMISSION_WAIT: 25ms
GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT: 256
runtime: farm-route-runtime@sha256:aa69bdd49be297b7712b6fbf1aea85483a22b7c654c9bd6dc62ac8b0a5db4b09
```

A fresh 10,000-player run completed all login Ready barriers:

```text
run_id=dkrpb8h1ikfw
ready=10000/10000
join_errors=0
Ready P95=5.374s
```

Safety counters remained clean:

```text
farm_gate_join_pending_timeout_total delta=0
farm_gate_join_accept_without_snapshot_total delta=0
farm_downlink_queue_full_total delta=0
farm_downlink_rpc_failure_total delta=0
```

The popup limiter intentionally returned bounded `ERR_OPPORTUNITY_BUSY` for
93 popup attempts after retry budget exhaustion (`9907/10000` popup answers
completed); this did not affect login Ready. The next tuning target is popup
concurrency/Retry-After policy, not Gate CPU or downlink capacity.
