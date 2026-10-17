# UC Test Plus 1024-Shard Deployment Runbook

This is the recorded deployment procedure for:

```text
context:   uc-test-plus
namespace: developer_026-qqfarm
realm:      classic-farm-dynamic-uc
Redis:      uctestscaleplanqqfarm20260816 / z1
```

Do not use `apply-r93.sh`. Do not raw-apply UC YAML containing staged
placeholders. Do not restore Redis slaves unless explicitly requested.

## One command

From the selected worktree:

```bash
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

The script performs a clean reset and is the source of truth. It:

1. stops Gate, Workers, Controller, and both Holder StatefulSets;
2. flushes only the three verified Redis masters;
3. enforces `route-controller.spec.publishNotReadyAddresses=true`;
4. pins the Controller image to:

   ```text
   mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:cf50fc92ac3dc39204859ec3a431a3ded79cc07b499f87500a1a076292c1b9f0
   ```

5. sets the verified-safe `GATE_BACKEND_MAX_INFLIGHT=1024` and permanently
   enables `GATE_PPROF_ENABLED=1` plus `HOLDER_PPROF_ENABLED=1` before any
   consumer Pod is created;
6. creates two Player and two Farm Holder Pods and captures Pod UID,
   StatefulSet UID, ordinal, boot ID, endpoints, and TLS identity;
7. writes a bootstrap-only revision-1 zero-Authority snapshot for 1024 shards;
8. starts a temporary legacy/off Controller so Holder init barriers can
   release and HolderControl can become available;
9. switches to `k8s-canary` discovery plus the Controller-owned sealed initial
   assignment batch;
10. atomically assigns 1024 PLAYER and 1024 FARM shards;
11. starts Workers and Gate only after the 1024 assignment is complete.

## Required final invariants

```text
player-svr-0: 512 PLAYER shards
player-svr-1: 512 PLAYER shards
farm-svr-0:   512 FARM shards
farm-svr-1:   512 FARM shards
```

All four Holder `/readyz` responses must report:

```text
status=ready
assignment_healthy=true
serving_shard_count=512
installed_memberships=512
pending_actions=0
degraded_shards=0
fatal_error_present=false
```

Also require Gate HTTP readiness, Farm→Gate `:9092` downlink probe PASS, and
zero Holder replacements after assignment.

## Verification order

```bash
bash /data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/scripts/probe-farm-gate-downlink.sh

KUBE_CONTEXT=uc-test-plus bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh join 1 1 10s 1787900000000

KUBE_CONTEXT=uc-test-plus bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 1 1 10s 947000000
```

pprof endpoints are exposed on the existing HTTP health/metrics listeners:

```text
Player: http://<pod>:9101/debug/pprof/
Farm:   http://<pod>:9091/debug/pprof/
Gate:   http://<pod>:8080/debug/pprof/
```

Use read-only `GET` profiles, for example:

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm exec player-svr-0 -c player-svr -- \
  wget -qO /tmp/player.cpu.pprof \
  'http://127.0.0.1:9101/debug/pprof/profile?seconds=30'
```

Prometheus `/metrics` remains enabled. Important RPC/queue metrics include
`farm_rpc_calls_total`, `farm_gate_command_backend_in_flight`,
`farm_gate_command_backend_rejected_total`, `farm_gate_command_rpc_server_seconds`,
`GATE_BACKEND_ADMISSION_WAIT=25ms`, `GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT=256`,
`farm_actor_activation_*`, `farm_downlink_rpc_*`,
`farm_downlink_queue_items`, `farm_downlink_queue_capacity`,
`farm_downlink_control_queue_items`, `farm_downlink_control_queue_capacity`, and
`farm_redis_dependency_error_percent`.

The generic UC `preflight --require-ready` still assumes the historical
1-holder/4-shard topology and may reject this valid 2+2/1024 setup. Use the
four Holder invariants above for the 1024-specific readiness gate.

## Load-test record

Successful 1024 deployment and load evidence:

```text
docs/verification/perf/qqfarm-thick-login-1024-20260818.md
```

Target result recorded:

```text
3000 connections @ 1000/s for 60s
attempted=3000 ready=3000
Ready P95=51.232ms
login/connect/hello/join/bootstrap/friend errors=0
```

## Stable 1024 benchmark recipe

The successful UC 1024 deployment recipe is preserved in
`reset-1024-initial-assignment.sh`. Do not replace it with a raw manifest
apply or combine initial assignment `auto` with `SCALEPLAN_MODE=off`.

The reset applies `high-cpu-priority` to Player, Farm, Workers, Gate, and
Route Controller while those workloads are scaled to zero. Disposable
`ws-stress` Jobs must use the same PriorityClass. This lets the benchmark
preempt lower-priority shared-cluster workloads without allowing the load
clients to preempt the QQFarm backend.

Before every new load stage, verify all four Holder invariants at revision
2049 and require every load Pod to be simultaneously `Running` before the
launch timer starts. Do not reset between stages unless the baseline, startup
configuration, or OOM state requires it. The 1000/s validated layout used
four load Pods at 250/s; 1500/s should use six load Pods at 250/s.

