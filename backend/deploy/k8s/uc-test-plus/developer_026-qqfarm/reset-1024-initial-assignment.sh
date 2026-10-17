#!/usr/bin/env bash
set -euo pipefail

# UC-only adaptation of the verified A9 initial-assignment sequence.
# Never run apply-r93.sh or raw-apply UC manifests from this script.
REPO="${QQFARM_REPO:-/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816}"
cd "$REPO"
K=(kubectl --context uc-test-plus -n developer_026-qqfarm)
ROUTE_CONTROLLER_IMAGE="${QQFARM_ROUTE_RUNTIME_IMAGE:-mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:90ca77f051be65ccec9eb0e73dd5e146c30358ebc0b6a852fae980148d801e18}"
H5_IMAGE='mirrors.tencent.com/uc-backend/farm-h5@sha256:044e35944ef042d9672086f74e17890ac77f22f0396cfefcc703bc0c57c67092'
# Until dynamic FARM_WORKER RouteManager claims are made part of the verified
# initial batch, split the 1024 DueIndex shards deterministically. The static
# provider still uses WorkLease/claim fences; it only avoids bootstrapping a
# worker Holder route before the worker is ready.
WORKER_A_SHARDS=$(seq 0 2 1022 | paste -sd, -)
WORKER_B_SHARDS=$(seq 1 2 1023 | paste -sd, -)
REDIS_CLUSTER='redis://farm-redis-0.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0,redis://farm-redis-1.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0,redis://farm-redis-2.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0'
REDIS_NAMESPACE_ENVIRONMENT="${QQFARM_REDIS_NAMESPACE_ENVIRONMENT:-uctestscaleplanqqfarm20260816}"
REDIS_NAMESPACE_ZONE="${QQFARM_REDIS_NAMESPACE_ZONE:-z1}"
REDIS_RESET_MODE="${QQFARM_REDIS_RESET_MODE:-flushall}"
CHALLENGE=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
BOOT="uc-1024-$CHALLENGE"
GATE_REPLICAS="${QQFARM_GATE_REPLICAS:-1}"
PLAYER_REPLICAS="${QQFARM_PLAYER_REPLICAS:-2}"
FARM_REPLICAS="${QQFARM_FARM_REPLICAS:-2}"
HOLDER_CPU="${QQFARM_HOLDER_CPU:-4}"
HOLDER_MEMORY="${QQFARM_HOLDER_MEMORY:-16Gi}"
ACTOR_LOADS="${QQFARM_MAX_CONCURRENT_ACTOR_LOADS:-2000}"
ACTOR_PENDING="${QQFARM_MAX_PENDING_ACTOR_LOADS:-4096}"
HOLDER_REDIS_POOL_SIZE="${QQFARM_HOLDER_REDIS_POOL_SIZE:-}"
REDIS_WORKLOAD_METRICS="${QQFARM_REDIS_WORKLOAD_METRICS:-}"
if [ -n "$HOLDER_REDIS_POOL_SIZE" ]; then
  case "$HOLDER_REDIS_POOL_SIZE" in
    ''|*[!0-9]*) echo "QQFARM_HOLDER_REDIS_POOL_SIZE must be a positive integer" >&2; exit 2 ;;
  esac
  [ "$HOLDER_REDIS_POOL_SIZE" -ge 1 ] || { echo "QQFARM_HOLDER_REDIS_POOL_SIZE must be >= 1" >&2; exit 2; }
fi
for replica_setting in GATE_REPLICAS PLAYER_REPLICAS FARM_REPLICAS; do
  replica_value="${!replica_setting}"
  case "$replica_value" in
    ''|*[!0-9]*) echo "$replica_setting must be a positive integer" >&2; exit 2 ;;
  esac
  [ "$replica_value" -ge 1 ] || { echo "$replica_setting must be >= 1" >&2; exit 2; }
done
for namespace_setting in REDIS_NAMESPACE_ENVIRONMENT REDIS_NAMESPACE_ZONE; do
  namespace_value="${!namespace_setting}"
  case "$namespace_value" in
    ''|*[!a-zA-Z0-9_-]*) echo "$namespace_setting contains unsupported characters" >&2; exit 2 ;;
  esac
done
case "$REDIS_RESET_MODE" in
  flushall|preserve) ;;
  *) echo "QQFARM_REDIS_RESET_MODE must be flushall or preserve" >&2; exit 2 ;;
esac
[ $((1024 % PLAYER_REPLICAS)) -eq 0 ] || { echo "QQFARM_PLAYER_REPLICAS must divide 1024" >&2; exit 2; }
[ $((1024 % FARM_REPLICAS)) -eq 0 ] || { echo "QQFARM_FARM_REPLICAS must divide 1024" >&2; exit 2; }
PLAYER_SHARDS=$((1024 / PLAYER_REPLICAS))
FARM_SHARDS=$((1024 / FARM_REPLICAS))
GATE_MANIFEST="$REPO/deploy/k8s/uc-test-plus/developer_026-qqfarm/50-farm-gate.yaml"
OBSERVABILITY_MANIFEST="$REPO/deploy/k8s/uc-test-plus/developer_026-qqfarm/80-observability.yaml"
COMMAND_DASHBOARD_MANIFEST="$REPO/deploy/k8s/uc-test-plus/developer_026-qqfarm/81-grafana-command-dashboard.yaml"
POD_MONITOR_MANIFEST="$REPO/deploy/k8s/uc-test-plus/developer_026-qqfarm/82-podmonitors.yaml"

wait_deploy() {
  local name=$1 timeout=${2:-1800} expected=${3:-1}
  for _ in $(seq 1 "$timeout"); do
    local r
    r=$("${K[@]}" get deploy "$name" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    [ "${r:-0}" = "$expected" ] && return 0
    sleep 1
  done
  return 1
}

wait_statefulset() {
  local name=$1 timeout=${2:-1800} expected=${3:-1}
  for _ in $(seq 1 "$timeout"); do
    local r
    r=$("${K[@]}" get sts "$name" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    [ "${r:-0}" = "$expected" ] && return 0
    sleep 1
  done
  return 1
}

wait_pods_exist() {
  for kind in player-svr farm-svr; do
    replicas=$PLAYER_REPLICAS
    [ "$kind" = farm-svr ] && replicas=$FARM_REPLICAS
    for ordinal in $(seq 0 $((replicas - 1))); do
      pod="$kind-$ordinal"
      for _ in $(seq 1 240); do
        "${K[@]}" get pod "$pod" >/dev/null 2>&1 && break
        sleep .25
      done
      "${K[@]}" get pod "$pod" >/dev/null
    done
  done
}

wait_zero() {
  for _ in $(seq 1 300); do
    local ok=1
    for kind in player-svr farm-svr; do
      replicas=$PLAYER_REPLICAS
      port=9101
      cont=player-svr
      if [ "$kind" = farm-svr ]; then replicas=$FARM_REPLICAS; port=9091; cont=farm-svr; fi
      for ordinal in $(seq 0 $((replicas - 1))); do
        x="$kind-$ordinal"
      local body
      body=$("${K[@]}" exec "$x" -c "$cont" -- wget -q -T 3 -O- "http://127.0.0.1:$port/readyz" 2>/dev/null || true)
        python3 - "$body" <<'PY' || ok=0
import json, sys
try:
    d=json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
r=d.get('route_runtime') or {}
good=(d.get('status') == 'ready' and
      r.get('assignment_healthy') is True and
      r.get('fatal_error_present') is False and
      r.get('snapshot_revision') == 1 and
      r.get('serving_shard_count') == 0)
raise SystemExit(0 if good else 1)
PY
      done
    done
    [ "$ok" = 1 ] && return 0
    sleep 1
  done
  return 1
}

wait_worker_shards() {
  for _ in $(seq 1 180); do
    local a b metrics_a metrics_b
    metrics_a=$("${K[@]}" exec deploy/farm-worker-a -c farm-worker -- \
      wget -qO- http://127.0.0.1:9201/metrics 2>/dev/null || true)
    metrics_b=$("${K[@]}" exec deploy/farm-worker-b -c farm-worker -- \
      wget -qO- http://127.0.0.1:9201/metrics 2>/dev/null || true)
    a=$(printf '%s\n' "$metrics_a" | awk '/^farm_opportunity_wakeup_owned_shards / {print $2; exit}')
    b=$(printf '%s\n' "$metrics_b" | awk '/^farm_opportunity_wakeup_owned_shards / {print $2; exit}')
    [ "${a:-0}" = 512 ] && [ "${b:-0}" = 512 ] && return 0
    sleep 1
  done
  return 1
}

wait_512() {
  for poll in $(seq 1 1800); do
    local ok=1
    for kind in player-svr farm-svr; do
      replicas=$PLAYER_REPLICAS
      port=9101
      cont=player-svr
      if [ "$kind" = farm-svr ]; then replicas=$FARM_REPLICAS; port=9091; cont=farm-svr; fi
      for ordinal in $(seq 0 $((replicas - 1))); do
        x="$kind-$ordinal"
      local body
      body=$("${K[@]}" exec "$x" -c "$cont" -- wget -q -T 3 -O- "http://127.0.0.1:$port/readyz" 2>/dev/null || true)
        expected_memberships=$PLAYER_SHARDS
        [ "$kind" = farm-svr ] && expected_memberships=$FARM_SHARDS
        python3 - "$body" "$expected_memberships" <<'PY' || ok=0
import json, sys
try:
    d=json.loads(sys.argv[1])
    expected_memberships=int(sys.argv[2])
except Exception:
    raise SystemExit(1)
r=d.get('route_runtime') or {}
m=r.get('membership') or {}
good=(d.get('status') == 'ready' and
      r.get('assignment_healthy') is True and
      r.get('fatal_error_present') is False and
      m.get('installed_memberships') == expected_memberships and
      m.get('pending_actions') == 0 and
      m.get('degraded_shards') == 0)
raise SystemExit(0 if good else 1)
PY
      done
    done
    [ "$ok" = 1 ] && return 0
    if (( poll % 30 == 0 )); then
      echo "waiting for 512+512 initial assignment: poll=$poll" >&2
    fi
    sleep 1
  done
  return 1
}

# The current UC field state is experimental/partial; this is intentionally a
# clean reset. Redis is the dedicated uctestscaleplanqqfarm20260816 tuple.
# Gate is a StatefulSet so every replica has a stable gateway_id and Downlink
# endpoint. This migration is deliberately owned by this reset entrypoint.
"${K[@]}" scale deploy/farm-gate --replicas=0 >/dev/null 2>&1 || true
"${K[@]}" scale sts/farm-gate --replicas=0 >/dev/null 2>&1 || true
"${K[@]}" delete deploy/farm-gate --ignore-not-found --wait=true >/dev/null
"${K[@]}" apply -f "$GATE_MANIFEST" >/dev/null
# Observability is reset-owned too: the command correctness ledger and the
# Grafana dashboard must be activated through this entrypoint, never by a
# separate ad-hoc apply during a load run.
"${K[@]}" apply -f "$OBSERVABILITY_MANIFEST" -f "$COMMAND_DASHBOARD_MANIFEST" -f "$POD_MONITOR_MANIFEST" >/dev/null
"${K[@]}" scale sts/farm-gate --replicas=0 >/dev/null
"${K[@]}" scale deploy/farm-worker-a deploy/farm-worker-b deploy/route-controller --replicas=0 >/dev/null
"${K[@]}" scale sts/player-svr sts/farm-svr --replicas=0 >/dev/null
"${K[@]}" wait --for=delete pod -l app.kubernetes.io/name=route-controller --timeout=180s >/dev/null || true
"${K[@]}" wait --for=delete pod -l app.kubernetes.io/name=farm-gate --timeout=180s >/dev/null || true
"${K[@]}" wait --for=delete pod -l app.kubernetes.io/name=player-svr --timeout=180s >/dev/null || true
"${K[@]}" wait --for=delete pod -l app.kubernetes.io/name=farm-svr --timeout=180s >/dev/null || true

# Redis is intentionally not restarted here: its UC Cluster uses emptyDir
# nodes.conf, and the verified reset path assumes the existing three-node
# Cluster remains intact. The live StatefulSet already carries
# high-cpu-priority; reset only clears its keys through this entrypoint.

# Keep the fixed QQFarm topology and its disposable load clients above normal
# shared-cluster workloads. This is applied while all mutable workloads are
# scaled to zero; the load Job uses the same PriorityClass at launch time.
for workload in \
  deploy/route-controller sts/farm-gate deploy/farm-worker-a deploy/farm-worker-b \
  sts/player-svr sts/farm-svr; do
  "${K[@]}" patch "$workload" --type=merge \
    -p '{"spec":{"template":{"spec":{"priorityClassName":"high-cpu-priority"}}}}' >/dev/null
done

# A dedicated namespace/zone can be reset without deleting unrelated capacity
# evidence or another Agent's workload. The historical default remains
# FLUSHALL; isolated capacity runs use preserve together with a fresh Zone.
if [ "$REDIS_RESET_MODE" = flushall ]; then
  for redis_ordinal in 0 1 2; do
    "${K[@]}" exec "farm-redis-${redis_ordinal}" -- redis-cli FLUSHALL >/dev/null
  done
fi

DYNAMIC_PREFIX="farm:${REDIS_NAMESPACE_ENVIRONMENT}:${REDIS_NAMESPACE_ZONE}:dynamic"
"${K[@]}" patch cm farm-platform --type=merge -p "$(printf '%s' "{
  \"data\": {
    \"REDIS_NAMESPACE_ENVIRONMENT\": \"${REDIS_NAMESPACE_ENVIRONMENT}\",
    \"REDIS_NAMESPACE_ZONE\": \"${REDIS_NAMESPACE_ZONE}\",
    \"FARM_STORE_PREFIX\": \"${DYNAMIC_PREFIX}:farm:v1\",
    \"PLAYER_STORE_PREFIX\": \"${DYNAMIC_PREFIX}:player:v1\",
    \"SHARE_STORE_PREFIX\": \"${DYNAMIC_PREFIX}:share:v1\",
    \"THEFT_GUARD_SNAPSHOT_PREFIX\": \"${DYNAMIC_PREFIX}:theft-guard:v1\",
    \"THEFT_TARGET_BINDING_PREFIX\": \"${DYNAMIC_PREFIX}:theft-target:v1\",
    \"WEB_ACCOUNT_STORE_PREFIX\": \"${DYNAMIC_PREFIX}:web-account:v1\",
    \"WEB_LOGIN_RATE_LIMIT_PREFIX\": \"farm:${REDIS_NAMESPACE_ENVIRONMENT}:${REDIS_NAMESPACE_ZONE}:web-login-limit\",
    \"GATE_BOOT_LEASE_PREFIX\": \"farm:${REDIS_NAMESPACE_ENVIRONMENT}:${REDIS_NAMESPACE_ZONE}:gate-boot\",
    \"WORKFLOW_STORE_PREFIX\": \"${DYNAMIC_PREFIX}:workflow:v1\"
  }
}")" >/dev/null

# Persist the UC-specific startup contract on every reset. The Service must
# route to the Controller before readiness becomes 200, because Holder init
# barriers need its bootstrap health/snapshot endpoints first.
"${K[@]}" patch svc route-controller --type=merge \
  -p '{"spec":{"publishNotReadyAddresses":true}}' >/dev/null
"${K[@]}" set image deploy/route-controller \
  route-controller="$ROUTE_CONTROLLER_IMAGE" >/dev/null
# Keep the H5 popup fix on the same approved reset-only deployment path.
"${K[@]}" set image deploy/farm-h5 \
  nginx="$H5_IMAGE" >/dev/null
# UC YAML is not raw-applied by this runbook; persist the exact runtime image
# on every workload before recreating Pods.
"${K[@]}" set image sts/player-svr \
  player-svr="$ROUTE_CONTROLLER_IMAGE" >/dev/null
"${K[@]}" set image sts/farm-svr \
  farm-svr="$ROUTE_CONTROLLER_IMAGE" >/dev/null
"${K[@]}" set image sts/farm-gate farm-gate="$ROUTE_CONTROLLER_IMAGE" >/dev/null
"${K[@]}" set image deploy/farm-worker-a farm-worker="$ROUTE_CONTROLLER_IMAGE" >/dev/null
"${K[@]}" set image deploy/farm-worker-b farm-worker="$ROUTE_CONTROLLER_IMAGE" >/dev/null
# Explicitly give the two Worker processes disjoint FARM/PLAYER source-order
# and opportunity DueIndex shards. This is the documented static fallback,
# not a duplicate all-shards scan.
"${K[@]}" set env deploy/farm-worker-a \
  FARM_WORKER_ASSIGNMENT_MODE=static \
  FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE=static \
  FARM_WORKER_OWNED_SHARDS="$WORKER_A_SHARDS" \
  FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS="$WORKER_A_SHARDS" \
  WORKER_READINESS_MAX_SCAN_AGE_V1=30s \
  WORKER_POLL_INTERVAL=5s \
  SOURCE_ORDER_WORKER_IDLE_MAX_INTERVAL=15s \
  SOURCE_ORDER_DISCOVERY_CACHE_TTL=5s \
  SOURCE_ORDER_DISCOVERY_CACHE_LIMIT=512 \
  SOURCE_ORDER_SCAN_LEASE_ENABLED=false \
  SOURCE_ORDER_RECOVERY_GRACE=60s \
  THEFT_WORKER_POLL_INTERVAL=250ms \
  THEFT_WORKER_IDLE_MAX_INTERVAL=2s \
  THEFT_DUE_OBSERVE_INTERVAL=60s \
  FARM_OPPORTUNITY_WAKEUP_IDLE_MAX_INTERVAL=5s \
  WORKER_PPROF_ENABLED=1 \
  FARM_WORKER_HOLDER_LEASE_TTL- >/dev/null
"${K[@]}" set env deploy/farm-worker-b \
  FARM_WORKER_ASSIGNMENT_MODE=static \
  FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE=static \
  FARM_WORKER_OWNED_SHARDS="$WORKER_B_SHARDS" \
  FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS="$WORKER_B_SHARDS" \
  WORKER_READINESS_MAX_SCAN_AGE_V1=30s \
  WORKER_POLL_INTERVAL=5s \
  SOURCE_ORDER_WORKER_IDLE_MAX_INTERVAL=15s \
  SOURCE_ORDER_DISCOVERY_CACHE_TTL=5s \
  SOURCE_ORDER_DISCOVERY_CACHE_LIMIT=512 \
  SOURCE_ORDER_SCAN_LEASE_ENABLED=false \
  SOURCE_ORDER_RECOVERY_GRACE=60s \
  THEFT_WORKER_POLL_INTERVAL=250ms \
  THEFT_WORKER_IDLE_MAX_INTERVAL=2s \
  THEFT_DUE_OBSERVE_INTERVAL=60s \
  FARM_OPPORTUNITY_WAKEUP_IDLE_MAX_INTERVAL=5s \
  WORKER_PPROF_ENABLED=1 \
  FARM_WORKER_HOLDER_LEASE_TTL- >/dev/null
# Cold-pool diagnostics create many short-lived identities. Persist the
# approved 2-minute idle policy, a bounded 5-second sweep cadence, and enough
# retirement work per tick to keep up with the measured per-holder arrival
# rate. Apply these changes while all holders are scaled to zero; do not patch
# assigned Pods in place.
"${K[@]}" set env sts/player-svr \
  PLAYER_MAILBOX_IDLE_TIMEOUT=2m \
  PLAYER_MAILBOX_IDLE_BUCKET_WIDTH=5s \
  PLAYER_MAILBOX_IDLE_SWEEP_INTERVAL=5s \
  PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP=16384 \
  PLAYER_MAILBOX_INITIAL_SIZE=16 \
  PLAYER_MAILBOX_MAX_SIZE=64 >/dev/null
"${K[@]}" set env sts/farm-svr \
  FARM_ACTOR_IDLE_TIMEOUT=2m \
  FARM_ACTOR_IDLE_BUCKET_WIDTH=5s \
  FARM_ACTOR_IDLE_SWEEP_INTERVAL=5s \
  FARM_ACTOR_IDLE_MAX_PER_SWEEP=16384 \
  FARM_MAILBOX_INITIAL_SIZE=16 \
  FARM_MAILBOX_MAX_SIZE=64 \
  CARE_DIRECT_WORKERS=32 \
  CARE_DIRECT_QUEUE_SIZE=131072 \
  CARE_DIRECT_QUEUE_BYTES=268435456 \
  CARE_DIRECT_COMPLETION_BATCH_SIZE=64 \
  CARE_DIRECT_COMPLETION_BATCH_WAIT=1ms >/dev/null
# Keep the tested Holder envelope by default, while allowing a capacity run to
# enlarge Player/Farm through the reset entrypoint without an in-place patch.
"${K[@]}" set resources sts/player-svr --containers=player-svr \
  --requests=cpu="$HOLDER_CPU",memory="$HOLDER_MEMORY" \
  --limits=cpu="$HOLDER_CPU",memory="$HOLDER_MEMORY" >/dev/null
"${K[@]}" set resources sts/farm-svr --containers=farm-svr \
  --requests=cpu="$HOLDER_CPU",memory="$HOLDER_MEMORY" \
  --limits=cpu="$HOLDER_CPU",memory="$HOLDER_MEMORY" >/dev/null
# Preserve the live Redis Cluster while mutable holders are stopped; its keys
# are cleared through the reset-only FLUSHALL below without rolling Redis.
# Keep Gate admission at the verified safe limit. Diagnostics are applied
# while consumers are stopped, never by restarting an assigned Holder.
"${K[@]}" set env sts/farm-gate \
  GATE_BACKEND_MAX_INFLIGHT=1536 \
  GATE_BACKEND_ADMISSION_WAIT=500ms \
  GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT=256 \
  GATE_PPROF_ENABLED=1 >/dev/null
"${K[@]}" set env sts/player-svr sts/farm-svr \
  HOLDER_PPROF_ENABLED=1 \
  MAX_CONCURRENT_ACTOR_LOADS="$ACTOR_LOADS" \
  MAX_PENDING_ACTOR_LOADS="$ACTOR_PENDING" >/dev/null
# Holder processes otherwise inherit go-redis' default pool. Capacity runs can
# opt into an explicit per-Redis-node pool without changing Gate's separately
# tuned pool or silently changing the baseline when this value is omitted.
if [ -n "$HOLDER_REDIS_POOL_SIZE" ]; then
  HOLDER_REDIS_CLUSTER="${REDIS_CLUSTER//\/0/\/0?pool_size=$HOLDER_REDIS_POOL_SIZE}"
  "${K[@]}" set env sts/player-svr sts/farm-svr \
    FARM_REDIS_CLUSTER_URLS="$HOLDER_REDIS_CLUSTER" >/dev/null
  "${K[@]}" set env deploy/farm-worker-a deploy/farm-worker-b \
    FARM_REDIS_CLUSTER_URLS="$HOLDER_REDIS_CLUSTER" >/dev/null
fi
if [ -n "$REDIS_WORKLOAD_METRICS" ]; then
  "${K[@]}" set env sts/player-svr sts/farm-svr sts/farm-gate \
    REDIS_WORKLOAD_METRICS="$REDIS_WORKLOAD_METRICS" >/dev/null
  "${K[@]}" set env deploy/farm-worker-a deploy/farm-worker-b deploy/route-controller \
    REDIS_WORKLOAD_METRICS="$REDIS_WORKLOAD_METRICS" >/dev/null
fi
# Keep the bounded home-popup navigation window explicit on Player. The live
# ConfigMap predates this tuning and reset is the only approved deployment
# entrypoint, so the workload env must pin the tested value as well.
"${K[@]}" set env sts/player-svr \
  FRIEND_NAVIGATION_WINDOW_MODE=top-k \
  FRIEND_NAVIGATION_WINDOW_SIZE=10 >/dev/null
"${K[@]}" patch cm farm-platform --type=merge \
  -p '{"data":{"FRIEND_NAVIGATION_WINDOW_MODE":"top-k","FRIEND_NAVIGATION_WINDOW_SIZE":"10","PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP":"16384","PLAYER_MAILBOX_INITIAL_SIZE":"16","PLAYER_MAILBOX_MAX_SIZE":"64","FARM_ACTOR_IDLE_MAX_PER_SWEEP":"16384","FARM_MAILBOX_INITIAL_SIZE":"16","FARM_MAILBOX_MAX_SIZE":"64","WORKER_READINESS_MAX_SCAN_AGE_V1":"30s","WORKER_POLL_INTERVAL":"5s","SOURCE_ORDER_WORKER_IDLE_MAX_INTERVAL":"15s","SOURCE_ORDER_DISCOVERY_CACHE_TTL":"5s","SOURCE_ORDER_DISCOVERY_CACHE_LIMIT":"512","SOURCE_ORDER_SCAN_LEASE_ENABLED":"false","SOURCE_ORDER_RECOVERY_GRACE":"60s","THEFT_WORKER_POLL_INTERVAL":"250ms","THEFT_WORKER_IDLE_MAX_INTERVAL":"2s","THEFT_DUE_OBSERVE_INTERVAL":"60s","FARM_OPPORTUNITY_WAKEUP_IDLE_MAX_INTERVAL":"5s","CARE_DIRECT_WORKERS":"32","CARE_DIRECT_QUEUE_SIZE":"131072","CARE_DIRECT_QUEUE_BYTES":"268435456","CARE_DIRECT_COMPLETION_BATCH_SIZE":"64","CARE_DIRECT_COMPLETION_BATCH_WAIT":"1ms","SOURCE_ORDER_COMMIT_PROOF_MODE":"prefer","SOURCE_ORDER_COMMIT_PROOF_TTL":"10s","SOURCE_ORDER_COMMIT_PROOF_LEASE_SAFETY_MARGIN":"2s"}}' >/dev/null
# Persist the logical Gate identities and their stable per-replica endpoints.
# Farm/Worker downlink routing is keyed by the Redis route_gate_id, not by the
# front-door Service name. Build all Gate routes from QQFARM_GATE_REPLICAS so
# four-Gate capacity runs do not silently publish only gate-0/gate-1 targets.
GATE0_DOWNLINK="farm-gate-0.farm-gate-headless.developer_026-qqfarm.svc.cluster.local:9092"
GATEWAY_DOWNLINK_ROUTES=""
GATE_CONTROL_TARGETS=""
for gate_ordinal in $(seq 0 $((GATE_REPLICAS - 1))); do
  gate_name="farm-gate-${gate_ordinal}"
  gate_downlink="${gate_name}.farm-gate-headless.developer_026-qqfarm.svc.cluster.local:9092"
  gate_control="${gate_name}.farm-gate-headless.developer_026-qqfarm.svc.cluster.local:9093"
  if [ -n "$GATEWAY_DOWNLINK_ROUTES" ]; then
    GATEWAY_DOWNLINK_ROUTES+=","
    GATE_CONTROL_TARGETS+=","
  fi
  GATEWAY_DOWNLINK_ROUTES+="${gate_name}=${gate_downlink}"
  GATE_CONTROL_TARGETS+="${gate_name}=${gate_control}"
done
"${K[@]}" patch cm farm-platform --type=merge -p "{\"data\":{\"GATEWAY_DOWNLINK_ADDR\":\"$GATE0_DOWNLINK\",\"GATEWAY_DOWNLINK_ROUTES\":\"$GATEWAY_DOWNLINK_ROUTES\",\"GATE_CONTROL_TARGETS\":\"$GATE_CONTROL_TARGETS\"}}" >/dev/null

# Recreate the disposable in-cluster helper; a completed pod cannot be exec'ed.
"${K[@]}" delete pod farm-route-bootstrap --ignore-not-found --wait=true >/dev/null
"${K[@]}" run farm-route-bootstrap --image=redis:7-alpine --restart=Never --command -- sleep 3600 >/dev/null
"${K[@]}" wait --for=condition=Ready pod/farm-route-bootstrap --timeout=120s >/dev/null
# Build the helper binary from this worktree before copying it into the helper.
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
  -o /tmp/rt2q-route-bootstrap-qqfarm ./cmd/rt2q-route-bootstrap
kubectl --context uc-test-plus -n developer_026-qqfarm cp \
  /tmp/rt2q-route-bootstrap-qqfarm farm-route-bootstrap:/tmp/rt2q-route-bootstrap
"${K[@]}" exec farm-route-bootstrap -- chmod +x /tmp/rt2q-route-bootstrap

# Exact A9 control-plane settings, with UC identities/tuple.
"${K[@]}" set env deploy/route-controller \
  ROUTE_CONTROLLER_BOOT="$BOOT" \
  ROUTE_CONTROLLER_BOOT_ID="$BOOT" \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_MODE=off \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_TIMEOUT=30m \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_PLAYER_HOLDERS="$PLAYER_REPLICAS" \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_FARM_HOLDERS="$FARM_REPLICAS" \
  ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT=30m \
  ROUTE_CONTROLLER_REDIS_OPERATION_TIMEOUT=15s \
  SCALEPLAN_MODE=off \
  SCALEPLAN_ROLLOUT_ENABLED=false \
  SCALEPLAN_ROUTEPLAN_PARALLELISM=32 \
  SCALEPLAN_AUTHORITY_COMMIT_WAVE_SIZE=8 \
  HOLDER_DISCOVERY_MODE=legacy \
  HOLDER_DISCOVERY_AUDIENCE=classic-farm-holder-control \
  HOLDER_DISCOVERY_RESYNC=30s \
  HOLDER_DISCOVERY_VERIFIED_TTL=45s \
  HOLDER_DISCOVERY_PLAYER_LOGIC_PORT=9100 \
  HOLDER_DISCOVERY_PLAYER_CONTROL_PORT=19100 \
  HOLDER_DISCOVERY_FARM_LOGIC_PORT=9090 \
  HOLDER_DISCOVERY_FARM_CONTROL_PORT=19101 \
  HOLDER_DISCOVERY_EXPECTED_PLAYER_REPLICAS="$PLAYER_REPLICAS" \
  HOLDER_DISCOVERY_EXPECTED_FARM_REPLICAS="$FARM_REPLICAS" \
  HOLDER_DISCOVERY_PLAYER_TLS_IDENTITY=holder-control.developer_026-qqfarm.svc.cluster.local \
  HOLDER_DISCOVERY_FARM_TLS_IDENTITY=holder-control.developer_026-qqfarm.svc.cluster.local >/dev/null

"${K[@]}" scale sts/player-svr --replicas="$PLAYER_REPLICAS" >/dev/null
"${K[@]}" scale sts/farm-svr --replicas="$FARM_REPLICAS" >/dev/null
wait_pods_exist
PWORK=$("${K[@]}" get sts player-svr -o jsonpath='{.metadata.uid}')
FWORK=$("${K[@]}" get sts farm-svr -o jsonpath='{.metadata.uid}')

COMMON=(
  --challenge "$CHALLENGE" --realm classic-farm-dynamic-uc --controller-id route-controller
  --redis-environment "$REDIS_NAMESPACE_ENVIRONMENT" --redis-zone "$REDIS_NAMESPACE_ZONE" --shard-count 1024
  --skip-worker-claims
)
letters=(a b c d)
for ordinal in $(seq 0 $((PLAYER_REPLICAS - 1))); do
  suffix=${letters[$ordinal]}
  pod="player-svr-$ordinal"
  uid=$("${K[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')
  COMMON+=(
    "--player-$suffix" "$pod" "--player-$suffix-boot" "$pod-$uid"
    "--player-$suffix-endpoint" "$pod.player-svr-headless.developer_026-qqfarm.svc.cluster.local:9100"
    "--player-$suffix-pod-uid" "$uid" "--player-$suffix-workload-uid" "$PWORK" "--player-$suffix-ordinal" "$ordinal"
    "--player-$suffix-control-endpoint" "$pod.player-svr-headless.developer_026-qqfarm.svc.cluster.local:19100"
    "--player-$suffix-tls-identity" "holder-control.developer_026-qqfarm.svc.cluster.local"
  )
done
for ordinal in $(seq 0 $((FARM_REPLICAS - 1))); do
  suffix=${letters[$ordinal]}
  pod="farm-svr-$ordinal"
  uid=$("${K[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')
  COMMON+=(
    "--farm-$suffix" "$pod" "--farm-$suffix-boot" "$pod-$uid"
    "--farm-$suffix-endpoint" "$pod.farm-svr-headless.developer_026-qqfarm.svc.cluster.local:9090"
    "--farm-$suffix-pod-uid" "$uid" "--farm-$suffix-workload-uid" "$FWORK" "--farm-$suffix-ordinal" "$ordinal"
    "--farm-$suffix-control-endpoint" "$pod.farm-svr-headless.developer_026-qqfarm.svc.cluster.local:19101"
    "--farm-$suffix-tls-identity" "holder-control.developer_026-qqfarm.svc.cluster.local"
  )
done
COMMON+=(
  --farm-worker-a farm-worker-a --farm-worker-a-boot bootstrap-worker-a
  --farm-worker-a-endpoint farm-worker-a.developer_026-qqfarm.svc.cluster.local:9201
  --farm-worker-b farm-worker-b --farm-worker-b-boot bootstrap-worker-b
  --farm-worker-b-endpoint farm-worker-b.developer_026-qqfarm.svc.cluster.local:9201
)

"${K[@]}" exec farm-route-bootstrap -- env \
  RT2Q_BOOTSTRAP_REDIS_URL=redis://farm-redis:6379/0 \
  RT2Q_BOOTSTRAP_REDIS_CLUSTER_URLS="$REDIS_CLUSTER" \
  /tmp/rt2q-route-bootstrap "${COMMON[@]}" --controller-boot "$BOOT-zero" --bootstrap-only \
  >/tmp/qqfarm-uc-1024-bootstrap.json

# Phase 1: legacy Controller only opens the snapshot/health path so Holder
# barriers can release and publish exact runtime identities.
"${K[@]}" scale deploy/route-controller --replicas=1 >/dev/null
wait_deploy route-controller 600
wait_deploy farm-prometheus 600
wait_deploy farm-grafana 600
wait_zero

# Phase 2: restart the same Controller deployment with the A9-proven
# Kubernetes-canary discovery and sealed initial assignment batch.
# Kubernetes-canary discovery and sealed initial assignment batch.
"${K[@]}" set env deploy/route-controller \
  ROUTE_CONTROLLER_BOOT="$BOOT" \
  ROUTE_CONTROLLER_BOOT_ID="$BOOT" \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_MODE=auto \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_TIMEOUT=30m \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_PLAYER_HOLDERS="$PLAYER_REPLICAS" \
  ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_FARM_HOLDERS="$FARM_REPLICAS" \
  ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT=30m \
  ROUTE_CONTROLLER_REDIS_OPERATION_TIMEOUT=15s \
  SCALEPLAN_MODE=auto \
  SCALEPLAN_ROLLOUT_ENABLED=true \
  SCALEPLAN_ROUTEPLAN_PARALLELISM=32 \
  SCALEPLAN_AUTHORITY_COMMIT_WAVE_SIZE=8 \
  HOLDER_DISCOVERY_MODE=k8s-canary \
  HOLDER_DISCOVERY_AUDIENCE=classic-farm-holder-control \
  HOLDER_DISCOVERY_RESYNC=30s \
  HOLDER_DISCOVERY_VERIFIED_TTL=45s \
  HOLDER_DISCOVERY_PLAYER_LOGIC_PORT=9100 \
  HOLDER_DISCOVERY_PLAYER_CONTROL_PORT=19100 \
  HOLDER_DISCOVERY_FARM_LOGIC_PORT=9090 \
  HOLDER_DISCOVERY_FARM_CONTROL_PORT=19101 \
  HOLDER_DISCOVERY_EXPECTED_PLAYER_REPLICAS="$PLAYER_REPLICAS" \
  HOLDER_DISCOVERY_EXPECTED_FARM_REPLICAS="$FARM_REPLICAS" \
  HOLDER_DISCOVERY_PLAYER_TLS_IDENTITY=holder-control.developer_026-qqfarm.svc.cluster.local \
  HOLDER_DISCOVERY_FARM_TLS_IDENTITY=holder-control.developer_026-qqfarm.svc.cluster.local >/dev/null
wait_deploy route-controller 1800
wait_512

# Workers and Gate are deliberately after the Controller-owned initial batch.
"${K[@]}" scale deploy/farm-worker-a deploy/farm-worker-b --replicas=1 >/dev/null
wait_deploy farm-worker-a 600; wait_deploy farm-worker-b 600
wait_worker_shards
"${K[@]}" scale sts/farm-gate --replicas="$GATE_REPLICAS" >/dev/null
wait_statefulset farm-gate 600 "$GATE_REPLICAS"

# Do not start a load run while either Farm holder still sees the Gate downlink
# service as unavailable. This catches the startup/routing race that otherwise
# manifests as a large Farm dispatcher queue-full spike.
for _ in $(seq 1 120); do
  downlink_ok=1
  for farm_ordinal in $(seq 0 $((FARM_REPLICAS - 1))); do
    farm="farm-svr-$farm_ordinal"
    for gate_ordinal in $(seq 0 $((GATE_REPLICAS - 1))); do
      if ! "${K[@]}" exec "$farm" -c farm-svr -- \
        nc -z -w 1 "farm-gate-${gate_ordinal}.farm-gate-headless.developer_026-qqfarm.svc.cluster.local" 9092 >/dev/null 2>&1; then
        downlink_ok=0
      fi
    done
  done
  [ "$downlink_ok" = 1 ] && break
  sleep 1
done
[ "$downlink_ok" = 1 ] || { echo "farm downlink readiness failed" >&2; exit 1; }

WORKER_RESULT="512+512"
if [ "${QQFARM_DIAGNOSTIC_DISABLE_WORKERS:-0}" = "1" ]; then
  # Diagnostic only: quantify Redis cost without crash-recovery/Opportunity
  # consumers. Never use this mode for a Durable capacity qualification.
  "${K[@]}" scale deploy/farm-worker-a deploy/farm-worker-b --replicas=0 >/dev/null
  "${K[@]}" rollout status deploy/farm-worker-a --timeout=300s >/dev/null
  "${K[@]}" rollout status deploy/farm-worker-b --timeout=300s >/dev/null
  WORKER_RESULT="DIAGNOSTIC_DISABLED"
fi

echo "UC_1024_INITIAL_ASSIGNMENT_OK challenge=$CHALLENGE tuple=${REDIS_NAMESPACE_ENVIRONMENT}/${REDIS_NAMESPACE_ZONE} shards=1024 holders=${PLAYER_REPLICAS}+${FARM_REPLICAS} workers=$WORKER_RESULT downlink=ready"
"${K[@]}" get pods -o wide
