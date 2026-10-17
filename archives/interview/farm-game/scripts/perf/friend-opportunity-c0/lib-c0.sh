#!/usr/bin/env bash
# Shared isolated real-process stack helpers for the C0 friend-capacity
# evidence tools. Every run gets its own ephemeral Redis container and its own
# foperf-* namespace; the runner never touches shared/company Redis and never
# issues FLUSH/CONFIG/SLOWLOG commands. Evidence is written under
# docs/verification/perf/friend-opportunity-c0/<run_id>/.
# shellcheck shell=bash
set -Eeuo pipefail

c0_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

c0_pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

c0_wait_http() {
  local url=$1 log_file=$2 timeout_seconds=${3:-60}
  local deadline=$((SECONDS + timeout_seconds))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'readiness timeout: %s\n' "$url" >&2
      [[ -f "$log_file" ]] && tail -n 60 "$log_file" >&2 || true
      return 1
    fi
    sleep 0.2
  done
}

c0_build_binaries() {
  local build_dir=$1
  mkdir -p "$build_dir"
  local cache=${FARM_GO_BUILD_CACHE:-/tmp/farmgame-go-cache-$USER}
  local service
  for service in player-svr farm-svr farm-worker farm-gate authority-smoke-fixture friend-opportunity-perf-fixture ws-stress; do
    printf 'building %s\n' "$service"
    (cd "$c0_root" && GOCACHE="$cache" CGO_ENABLED=0 go build -buildvcs=false -trimpath \
      -o "$build_dir/$service" "./cmd/$service")
  done
}

c0_slug() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9'
}

c0_redis_up() {
  local run_label=$1
  local container="foperf-${run_label}-redis"
  if docker inspect "$container" >/dev/null 2>&1; then
    printf 'refusing to start: ephemeral Redis container %s already exists\n' "$container" >&2
    return 1
  fi
  docker run -d --rm --name "$container" -p 127.0.0.1::6379 redis:7.4-alpine \
    redis-server --save '' --appendonly no >/dev/null
  local mapping port
  for _ in $(seq 1 100); do
    mapping=$(docker port "$container" 6379/tcp 2>/dev/null | head -1 || true)
    [[ -n "$mapping" ]] && break
    sleep 0.1
  done
  port=${mapping##*:}
  for _ in $(seq 1 100); do
    if docker exec "$container" redis-cli ping 2>/dev/null | grep -q PONG; then break; fi
    sleep 0.1
  done
  docker exec "$container" redis-cli ping 2>/dev/null | grep -q PONG
  printf 'redis://127.0.0.1:%s/0\n' "$port"
}

c0_redis_down() {
  local container=$1 state_dir=$2
  docker logs "$container" >"$state_dir/redis.log" 2>&1 || true
  local container_id_before
  container_id_before=$(docker inspect -f '{{.Id}}' "$container" 2>/dev/null || true)
  [[ -n "$container_id_before" ]] && printf '%s\n' "$container_id_before" >"$state_dir/redis-container-id"
  docker rm -f "$container" >/dev/null 2>&1 || true
  if docker inspect "$container" >/dev/null 2>&1; then
    printf 'failed to destroy ephemeral Redis container %s\n' "$container" >&2
    return 1
  fi
  if [[ -n "$container_id_before" ]]; then
    local remaining
    remaining=$(docker ps -aq --filter "name=^${container}$" 2>/dev/null || true)
    if [[ -n "$remaining" ]]; then
      printf 'ephemeral Redis container %s still present after removal\n' "$container" >&2
      return 1
    fi
  fi
}

c0_prepare_stack_identity() {
  # c0_prepare_stack_identity <state_dir> <namespace> <run_label>
  # Reserve this run's exact resource identity before creating Redis. A
  # pre-existing container is never adopted or removed by this run.
  local state_dir=$1 namespace=$2 run_label=$3
  local container="foperf-${run_label}-redis"
  mkdir -p "$state_dir"
  if docker inspect "$container" >/dev/null 2>&1; then
    printf 'refusing run identity: container %s already exists\n' "$container" >&2
    return 2
  fi
  printf 'container=%q\nnamespace=%q\nrun_label=%q\nstarted_at=%s\n' \
    "$container" "$namespace" "$run_label" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >"$state_dir/stack-identity.env"
  chmod 600 "$state_dir/stack-identity.env"
}

c0_stack_up() {
  # c0_stack_up <build_dir> <state_dir> <namespace> <run_label> <redis_url> <load_secret>
  local build_dir=$1 state_dir=$2 namespace=$3 run_label=$4 redis_url=$5 load_secret=$6
  mkdir -p "$state_dir"
  [[ -f "$state_dir/stack-identity.env" ]] || {
    printf 'missing reserved stack identity for run %s\n' "$run_label" >&2
    return 2
  }
  # rediskey environment segments are max 32 chars; foperf- consumes 7.
  [[ "$namespace" =~ ^foperf-[a-z0-9][a-z0-9-]{4,24}$ ]] || {
    printf 'refusing unsafe namespace: %s\n' "$namespace" >&2
    return 2
  }
  local expected_container="foperf-${run_label}-redis"
  if ! docker inspect "$expected_container" >/dev/null 2>&1; then
    printf 'ephemeral Redis container %s is not running for this stack\n' "$expected_container" >&2
    return 2
  fi
  # Persist the Redis identity before any service/fixture can fail so the EXIT
  # trap can remove the exact container even when stack startup is partial.
  printf 'PERF_REDIS_CONTAINER=%q\nPERF_REDIS_URL=%q\n' \
    "$expected_container" "$redis_url" >"$state_dir/stack.env"
  chmod 600 "$state_dir/stack.env"
  local player_port player_health farm_port farm_health worker_health gate_http gate_downlink gate_control
  player_port=$(c0_pick_port); player_health=$(c0_pick_port)
  farm_port=$(c0_pick_port); farm_health=$(c0_pick_port)
  worker_health=$(c0_pick_port)
  gate_http=$(c0_pick_port); gate_downlink=$(c0_pick_port); gate_control=$(c0_pick_port)

  local route="$state_dir/route-snapshot.json"
  local profile="foperf-${run_label}-v1" realm="foperf-${run_label}"
  local workflow_prefix="foperf:${run_label}:workflow"
  local share_prefix="foperf:${run_label}:share" web_prefix="foperf:${run_label}:web"
  local gate_lease_prefix="foperf:${run_label}:gate-lease"
  local player_holder="foperf-${run_label}-player" farm_holder="foperf-${run_label}-farm"
  local player_boot="${player_holder}-boot" farm_boot="${farm_holder}-boot"
  local permit_secret="foperf-${run_label}-permit-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"

  "$build_dir/authority-smoke-fixture" init \
    --redis-url "$redis_url" --namespace "$namespace" --zone local \
    --profile "$profile" --realm "$realm" --run-id "$run_label" \
    --player-holder "$player_holder" --player-boot "$player_boot" \
    --player-endpoint "127.0.0.1:$player_port" \
    --farm-holder "$farm_holder" --farm-boot "$farm_boot" \
    --farm-endpoint "127.0.0.1:$farm_port" \
    --route-out "$route" --json-out "$state_dir/authority-fixture.json" \
    >"$state_dir/authority-fixture.log" 2>&1

  local common_authority=(
    "ACTOR_AUTHORITY_MODE=authority" "ACTOR_ROUTE_SNAPSHOT=$route"
    "ACTOR_AUTHORITY_LEASE_TTL=55s" "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=3s"
    "ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=5s"
    "REDIS_PHYSICAL_PROFILE_VERSION=$profile" "REDIS_NAMESPACE_ENVIRONMENT=$namespace"
    "REDIS_NAMESPACE_ZONE=local" "REDIS_AUTHORITY_LAYOUT_VERSION=v1"
    "REDIS_WORKFLOW_LAYOUT_VERSION=v1" "REDIS_WORKFLOW_HASH_VERSION=1"
    "REDIS_WORKFLOW_BUCKET_COUNT=256" "FARM_REDIS_URL=$redis_url"
    "WORKFLOW_STORE_PREFIX=$workflow_prefix" "REDIS_WORKLOAD_METRICS=true"
  )

  c0_start "$state_dir" player env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$player_holder" "ACTOR_HOLDER_BOOT_ID=$player_boot" \
    "PLAYER_LISTEN=127.0.0.1:$player_port" "PLAYER_HEALTH_LISTEN=127.0.0.1:$player_health" \
    "FARM_ADDR=127.0.0.1:$farm_port" "PERMIT_SECRET=$permit_secret" \
    "FARM_FRIEND_NAVIGATION_V3_ENABLED=1" \
    "$build_dir/player-svr"
  c0_wait_http "http://127.0.0.1:$player_health/readyz" "$state_dir/player.log"

  c0_start "$state_dir" farm env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$farm_holder" "ACTOR_HOLDER_BOOT_ID=$farm_boot" \
    "FARM_LISTEN=127.0.0.1:$farm_port" "FARM_HEALTH_LISTEN=127.0.0.1:$farm_health" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "GATEWAY_DOWNLINK_ADDR=127.0.0.1:$gate_downlink" \
    "PERMIT_SECRET=$permit_secret" "THEFT_PRF_MODE=test" \
    "THEFT_PRF_HMAC_SECRET=foperf-${run_label}-theft-prf-at-least-32-bytes" \
    "FARM_CROP_MATURE_AFTER=5s" "FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED=true" \
    "FARM_OPPORTUNITY_HEADER_READ_MODE=full" "FARM_OPPORTUNITY_HEADER_FULL_FALLBACK=8" \
    "GATE_BOOT_REAPER_MODE=disabled" "FARM_SUBSCRIPTION_LEASE_INITIAL_TTL=60s" \
    "FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION=60s" \
    "$build_dir/farm-svr"
  c0_wait_http "http://127.0.0.1:$farm_health/readyz" "$state_dir/farm.log"

  c0_start "$state_dir" worker env \
    "ACTOR_AUTHORITY_MODE=authority" "ACTOR_ROUTE_SNAPSHOT=$route" "FARM_REDIS_URL=$redis_url" \
    "WORKFLOW_STORE_PREFIX=$workflow_prefix" "REDIS_WORKLOAD_METRICS=true" \
    "REDIS_PHYSICAL_PROFILE_VERSION=$profile" "REDIS_NAMESPACE_ENVIRONMENT=$namespace" \
    "REDIS_NAMESPACE_ZONE=local" "REDIS_AUTHORITY_LAYOUT_VERSION=v1" \
    "REDIS_WORKFLOW_LAYOUT_VERSION=v1" "REDIS_WORKFLOW_HASH_VERSION=1" \
    "REDIS_WORKFLOW_BUCKET_COUNT=256" "PLAYER_ADDR=127.0.0.1:$player_port" \
    "FARM_ADDR=127.0.0.1:$farm_port" "WORKER_HEALTH_LISTEN=127.0.0.1:$worker_health" \
    "WORKER_ID=foperf-${run_label}-worker" "WORKER_BOOT_ID=foperf-${run_label}-worker-boot" \
    "WORKER_LEASE_TOKEN=foperf-${run_label}-worker-lease" \
    "WORKER_POLL_INTERVAL=250ms" "WORKER_CLAIM_LEASE=5s" "WORKER_OPERATION_TIMEOUT=3s" \
    "FARM_OPPORTUNITY_WAKEUP_ENABLED=true" "FARM_OPPORTUNITY_WAKEUP_SHADOW=false" \
    "FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL=250ms" \
    "FARM_OPPORTUNITY_WAKEUP_CLAIM_LIMIT=64" "FARM_OPPORTUNITY_WAKEUP_DISCOVERY_LIMIT=256" \
    "FARM_OPPORTUNITY_WAKEUP_MAX_CONCURRENT=4" "FARM_OPPORTUNITY_WAKEUP_ROUND_TIME_BUDGET=800ms" \
    "FARM_OPPORTUNITY_HINT_TRANSPORT=direct" \
    "GATEWAY_DOWNLINK_ROUTES=foperf-${run_label}-gate=127.0.0.1:$gate_downlink" \
    "PERMIT_SECRET=$permit_secret" \
    "$build_dir/farm-worker"
  c0_wait_http "http://127.0.0.1:$worker_health/readyz" "$state_dir/worker.log"

  c0_start "$state_dir" gate env \
    "GATEWAY_LISTEN=127.0.0.1:$gate_http" "GATEWAY_ID=foperf-${run_label}-gate" \
    "FARM_APP_PROFILE=test" "ACTOR_AUTHORITY_MODE=authority" \
    "FRIENDSHIP_SERVICE_MODE=remote_player" "FARM_FRIEND_NAVIGATION_V3_ENABLED=1" \
    "FARM_OPPORTUNITY_HINT_DIAGNOSTICS=true" \
    "GATEWAY_DOWNLINK_LISTEN=127.0.0.1:$gate_downlink" \
    "GATE_CONTROL_LISTEN=127.0.0.1:$gate_control" \
    "GATE_TRANSPORT=websocket" "GATE_WS_PATH=/ws" \
    "GATE_WS_ALLOWED_ORIGINS=127.0.0.1,localhost" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "FARM_ADDR=127.0.0.1:$farm_port" \
    "LOGIC_HEALTH_ADDR=http://127.0.0.1:$farm_health" "FARM_REDIS_URL=$redis_url" \
    "REDIS_WORKLOAD_METRICS=true" "REDIS_NAMESPACE_ENVIRONMENT=$namespace" \
    "REDIS_NAMESPACE_ZONE=local" "ACTOR_ROUTE_SNAPSHOT=$route" \
    "WEB_ACCOUNT_STORE_PREFIX=$web_prefix" "WEB_ACCOUNT_PLAYER_ID_BASE=813000000" \
    "WEB_LOGIN_RATE_LIMIT_PREFIX=foperf:${run_label}:web-login-limit" \
    "WEB_LOGIN_IP_BURST=100000" "WEB_LOGIN_DEVICE_BURST=100000" \
    "SHARE_STORE_PREFIX=$share_prefix" "GATE_BOOT_LEASE_PREFIX=$gate_lease_prefix" \
    "HTTP_SESSION_COOKIE_SECURE=0" "PERMIT_SECRET=$permit_secret" \
    "GATE_WS_METRICS=1" "GATE_WS_RATE_PER_SEC=100000" "GATE_WS_RATE_BURST=200000" \
    "LOAD_TEST_TOKEN_SECRET=$load_secret" "WSS_ENDPOINT=ws://127.0.0.1:$gate_http/ws" \
    "$build_dir/farm-gate"
  c0_wait_http "http://127.0.0.1:$gate_http/readyz" "$state_dir/gate.log"

  printf 'PERF_REDIS_CONTAINER=%q\nPERF_GATE_HTTP=%q\nPERF_GATE_WS=%q\nPERF_GATE_METRICS=%q\n' \
    "foperf-${run_label}-redis" "http://127.0.0.1:$gate_http" "ws://127.0.0.1:$gate_http/ws" \
    "http://127.0.0.1:$gate_http/metrics" >"$state_dir/stack.env"
  printf 'PERF_PLAYER_ADDR=%q\nPERF_PLAYER_METRICS=%q\nPERF_FARM_METRICS=%q\nPERF_WORKER_METRICS=%q\n' \
    "127.0.0.1:$player_port" "http://127.0.0.1:$player_health/metrics" \
    "http://127.0.0.1:$farm_health/metrics" "http://127.0.0.1:$worker_health/metrics" >>"$state_dir/stack.env"
  printf 'PERF_NAMESPACE=%q\nPERF_REDIS_URL=%q\nPERF_LOAD_SECRET=%q\n' \
    "$namespace" "$redis_url" "$load_secret" >>"$state_dir/stack.env"
  printf 'PERF_RUN_ID=%q\nPERF_CELL_RUN_ID=%q\nPERF_NAMESPACE=%q\nPERF_ZONE=local\n' \
    "$run_label" "${run_label}" "$namespace" >>"$state_dir/stack.env"
  chmod 600 "$state_dir/stack.env"
}

c0_start() {
  local state_dir=$1 name=$2
  shift 2
  "$@" >"$state_dir/$name.log" 2>&1 &
  printf '%s\n' "$!" >"$state_dir/$name.pid"
}

c0_stack_down() {
  local state_dir=$1
  # shellcheck disable=SC1090
  [[ -f "$state_dir/stack.env" ]] && source "$state_dir/stack.env" || true
  local cleanup_failed=0
  local expected_pid=""
  local name pid
  for name in gate worker farm player; do
    [[ -f "$state_dir/$name.pid" ]] || continue
    pid=$(<"$state_dir/$name.pid")
    kill "$pid" 2>/dev/null || true
  done
  sleep 0.5
  for name in gate worker farm player; do
    [[ -f "$state_dir/$name.pid" ]] || continue
    pid=$(<"$state_dir/$name.pid")
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    wait "$pid" 2>/dev/null || true
    if kill -0 "$pid" 2>/dev/null; then
      printf 'failed to stop process %s pid=%s\n' "$name" "$pid" >&2
      cleanup_failed=1
    fi
  done
  if [[ -n "${PERF_REDIS_CONTAINER:-}" ]]; then
    c0_redis_down "$PERF_REDIS_CONTAINER" "$state_dir" || cleanup_failed=1
  else
    local expected_container
    expected_container=$(sed -n 's/^container=//p' "$state_dir/stack-identity.env" 2>/dev/null | head -1 || true)
    if [[ -n "$expected_container" ]]; then
      if docker inspect "$expected_container" >/dev/null 2>&1; then
        printf 'ephemeral Redis container %s still present\n' "$expected_container" >&2
        cleanup_failed=1
      fi
    fi
  fi
  if (( cleanup_failed == 1 )); then
    printf '{"cleanup":"destroy_exact_ephemeral_container_and_processes","completed":false}\n' \
      >"$state_dir/cleanup.json"
    printf 'cleanup failed for %s; see cleanup.json\n' "$state_dir" >&2
    return 1
  fi
  printf '{"cleanup":"destroy_exact_ephemeral_container_and_processes","completed":true}\n' \
    >"$state_dir/cleanup.json"
}

c0_scrape() {
  # c0_scrape <url> <out-file> — pull one /metrics snapshot. Fail closed on
  # HTTP/network errors or empty bodies; never convert scrape failure into a
  # successful zero measurement.
  local url=$1 out_file=$2
  mkdir -p "$(dirname "$out_file")"
  if ! curl -fsS --max-time 10 "$url" >"$out_file"; then
    printf 'scrape failed: %s\n' "$url" >&2
    rm -f "$out_file"
    return 1
  fi
  if [[ ! -s "$out_file" ]]; then
    printf 'scrape produced empty body: %s\n' "$url" >&2
    return 1
  fi
}

c0_cell_identities() {
  # c0_cell_identities <run_id> <friend_count>
  # Prints: viewer target_owner fake_base cell_run_id
  # Each matrix cell gets a distinct roster/account/bootstrap/operation identity.
  local run_id=$1 friend_count=$2
  local viewer=$((813010000 + friend_count * 10 + 1))
  local target=$((813010000 + friend_count * 10 + 2))
  local fake_base=$((7000000000 + friend_count * 1000000))
  local cell_run_id="${run_id}f${friend_count}"
  printf '%s %s %s %s\n' "$viewer" "$target" "$fake_base" "$cell_run_id"
}

c0_scrape_participating() {
  # c0_scrape_participating <out-dir>
  # Scrapes every participating service endpoint that exposes Redis workload
  # metrics (gate/player/farm/worker). Fail closed on any scrape error.
  local out_dir=$1
  mkdir -p "$out_dir"
  local name var url
  for name in gate player farm worker; do
    var="PERF_$(printf '%s' "$name" | tr '[:lower:]' '[:upper:]')_METRICS"
    [[ "$name" == "gate" ]] && var=PERF_GATE_METRICS
    url="${!var:-}"
    if [[ -z "$url" ]]; then
      printf 'missing participating metrics endpoint for %s\n' "$name" >&2
      return 1
    fi
    c0_scrape "$url" "$out_dir/${name}.prom" || return 1
  done
}

c0_redis_workload_aggregate() {
  # c0_redis_workload_aggregate <before-dir> <after-dir> <run_id> <cell_run_id> <scenario>
  # Aggregates command/byte deltas across every participating endpoint that
  # exposes the metric family. Incomplete endpoint sets mark the workload
  # unavailable rather than copying one farm-svr aggregate. The emitted
  # aggregate is bound to the exact run/cell/scenario/interval identity.
  # Prints JSON: {run_id, cell_run_id, scenario, interval, commands, bytes,
  # availability, endpoints, scope}
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json, re, sys
from pathlib import Path

before_dir, after_dir = Path(sys.argv[1]), Path(sys.argv[2])
run_id, cell_run_id, scenario = sys.argv[3], sys.argv[4], sys.argv[5]
required = ["gate", "player", "farm", "worker"]
endpoints = []
commands_total = 0.0
bytes_total = 0.0
interval = {
    "before": sorted(p.name for p in before_dir.iterdir() if p.is_file()),
    "after": sorted(p.name for p in after_dir.iterdir() if p.is_file()),
}

def load_commands(path: Path):
    if not path.is_file() or path.stat().st_size == 0:
        return None
    out = {}
    found = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("farm_redis_workload_commands_total{"):
            continue
        m = re.match(r'farm_redis_workload_commands_total\{([^}]*)\} ([0-9.eE+-]+)', line)
        if not m:
            continue
        found = True
        out[m.group(1)] = float(m.group(2))
    return out if found else None

def load_bytes(path: Path):
    if not path.is_file() or path.stat().st_size == 0:
        return None
    out = {}
    found_req = found_resp = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("farm_redis_workload_request_bytes_total{"):
            m = re.match(r'(farm_redis_workload_request_bytes_total)\{([^}]*)\} ([0-9.eE+-]+)', line)
            if m:
                found_req = True
                out[(m.group(1), m.group(2))] = float(m.group(3))
        elif line.startswith("farm_redis_workload_response_bytes_total{"):
            m = re.match(r'(farm_redis_workload_response_bytes_total)\{([^}]*)\} ([0-9.eE+-]+)', line)
            if m:
                found_resp = True
                out[(m.group(1), m.group(2))] = float(m.group(3))
    if not found_req or not found_resp:
        return None
    return out

for name in required:
    before = before_dir / f"{name}.prom"
    after = after_dir / f"{name}.prom"
    endpoints.append(f"{name}:{before.name}")
    before_cmds, after_cmds = load_commands(before), load_commands(after)
    before_bytes, after_bytes = load_bytes(before), load_bytes(after)
    if before_cmds is None or after_cmds is None or before_bytes is None or after_bytes is None:
        print(json.dumps({
            "run_id": run_id,
            "cell_run_id": cell_run_id,
            "scenario": scenario,
            "interval": interval,
            "commands": None,
            "bytes": None,
            "availability": "measurement_unavailable",
            "endpoints": endpoints,
            "scope": "workload_participating_services_incomplete",
            "missing": name,
        }))
        sys.exit(3)
    for key, value in after_cmds.items():
        commands_total += max(0.0, value - before_cmds.get(key, 0.0))
    for key, value in after_bytes.items():
        bytes_total += max(0.0, value - before_bytes.get(key, 0.0))

print(json.dumps({
    "run_id": run_id,
    "cell_run_id": cell_run_id,
    "scenario": scenario,
    "interval": interval,
    "commands": int(commands_total),
    "bytes": int(bytes_total),
    "availability": "ok",
    "endpoints": endpoints,
    "scope": "workload_participating_services_gate_player_farm_worker",
}))
PY
}

c0_merge_redis_evidence() {
  # c0_merge_redis_evidence <workload.json> <aggregate.json> <out.json> <run_id> <cell_run_id> <scenario>
  # aggregate.json is produced by c0_redis_workload_aggregate.
  python3 - "$1" "$2" "$3" "$4" "$5" "$6" <<'PY'
import json, sys
from pathlib import Path
src = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
aggregate = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
out_path = Path(sys.argv[3])
run_id, cell_run_id, scenario = sys.argv[4], sys.argv[5], sys.argv[6]
if aggregate.get("run_id") != run_id or aggregate.get("cell_run_id") != cell_run_id:
    raise SystemExit(f"aggregate identity mismatch: run_id={aggregate.get('run_id')} cell_run_id={aggregate.get('cell_run_id')} want {run_id}/{cell_run_id}")
if aggregate.get("scenario") != scenario:
    raise SystemExit(f"aggregate scenario mismatch: {aggregate.get('scenario')} want {scenario}")
friend = src.setdefault("friend_capacity", {})
availability = aggregate.get("availability", "measurement_unavailable")
endpoints = list(aggregate.get("endpoints") or [])
scope = aggregate.get("scope") or "workload_participating_services"
src["run_id"] = run_id
src["cell_run_id"] = cell_run_id
src["scenario"] = scenario
src["redis_evidence_interval"] = aggregate.get("interval") or {}
src["redis_evidence_bound"] = {
    "run_id": run_id,
    "cell_run_id": cell_run_id,
    "scenario": scenario,
    "interval": aggregate.get("interval") or {},
    "endpoints": endpoints,
}
friend["redis_evidence_scope"] = scope
friend["redis_evidence_endpoints"] = endpoints
friend["redis_evidence_interval"] = aggregate.get("interval") or {}
friend["redis_evidence_bound"] = src["redis_evidence_bound"]
src["redis_evidence_scope"] = scope
src["redis_evidence_endpoints"] = endpoints
if availability != "ok" or aggregate.get("commands") is None or aggregate.get("bytes") is None:
    friend["redis_commands"] = None
    friend["redis_commands_availability"] = "measurement_unavailable"
    friend["estimated_bytes"] = None
    friend["estimated_bytes_availability"] = "measurement_unavailable"
    friend["redis_measurement_gate"] = "fail"
    src["redis_commands"] = None
    src["estimated_bytes"] = None
    src["redis_measurement_gate"] = "fail"
else:
    friend["redis_commands"] = int(aggregate["commands"])
    friend["redis_commands_availability"] = "ok"
    friend["estimated_bytes"] = int(aggregate["bytes"])
    friend["estimated_bytes_availability"] = "ok"
    friend["redis_measurement_gate"] = "pass"
    src["redis_commands"] = friend["redis_commands"]
    src["estimated_bytes"] = friend["estimated_bytes"]
    src["redis_measurement_gate"] = "pass"
out_path.write_text(json.dumps(src, indent=2) + "\n", encoding="utf-8")
if availability != "ok" or aggregate.get("commands") is None or aggregate.get("bytes") is None:
    # Missing metrics produce explicit measurement_unavailable evidence and a
    # failed measurement gate; the caller decides the final verdict.
    sys.exit(0)
sys.exit(0)
PY
}
