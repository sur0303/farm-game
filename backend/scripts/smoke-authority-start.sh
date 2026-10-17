#!/usr/bin/env bash
# Authority + Static Assignment Smoke environment for A1~A4 premerge.
#
# Topology (all loopback, real Redis, authority mode):
#   player-svr 127.0.0.1:19600 (health 19601) — authority
#   farm-svr   127.0.0.1:19500 (health 19501) — authority
#   farm-worker (health 19701)                — legacy worker (needs route snapshot)
#   farm-gate  http/ws 18500, downlink 19592, GateControl 19593
#
# Usage:
#   ./scripts/smoke-authority-start.sh          # start all services
#   ./scripts/smoke-authority-start.sh --no-gate # start without gate (for Smoke D)
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
REDIS_CONFIG="${FARM_REDIS_CONFIG:-$ROOT_DIR/deploy/secrets/redis.conf.local}"
BUILD_CACHE="${FARM_GO_BUILD_CACHE:-/tmp/farmgame-go-cache}"
MATURE_AFTER="${FARM_CROP_MATURE_AFTER:-10s}"
run_id="$(date +%s%N)-$$"

# Use a unique clean namespace per run to avoid ShardWriteAuthority lease conflicts.
NAMESPACE_ENV="smoke-$(date +%s)"
NAMESPACE_ZONE="local"
PROFILE="redis-physical-v1"
REALM="smoke-premerge-a"

# Ports
PLAYER_PORT=19600
PLAYER_HEALTH=19601
FARM_PORT=19500
FARM_HEALTH=19501
WORKER_HEALTH=19701
GATE_PORT=18500
GATE_DOWNLINK=19592
GATE_CONTROL=19593

SKIP_GATE=0
if [[ "${1:-}" == "--no-gate" ]]; then
  SKIP_GATE=1
fi

if [[ ! -f "$REDIS_CONFIG" ]]; then
  echo "missing Redis config: $REDIS_CONFIG" >&2
  exit 2
fi

read_config_value() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*${key}:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\1/p" "$REDIS_CONFIG" | head -n 1
}

uri_encode() {
  local input="$1" output="" char encoded index
  LC_ALL=C
  for ((index = 0; index < ${#input}; index++)); do
    char="${input:index:1}"
    case "$char" in
      [a-zA-Z0-9.~_-]) output+="$char" ;;
      *) printf -v encoded '%%%02X' "'$char"; output+="$encoded" ;;
    esac
  done
  printf '%s' "$output"
}

redis_host=$(read_config_value host)
redis_port=$(read_config_value port)
redis_password=$(read_config_value password)
redis_db=$(read_config_value db)
redis_db="${redis_db:-0}"
if [[ -z "$redis_host" || -z "$redis_port" || -z "$redis_password" ]]; then
  echo "Redis config must contain host, port and password" >&2
  exit 2
fi
redis_url="redis://:$(uri_encode "$redis_password")@${redis_host}:${redis_port}/${redis_db}"

WORKFLOW_PREFIX="farm:smoke:${run_id}:workflow"
PERMIT_SECRET_VALUE="${PERMIT_SECRET:-smoke-permit-secret-20260806-at-least-32-bytes!!}"

# --- Build -----------------------------------------------------------------
echo "=== building services ==="
mkdir -p "$BUILD_CACHE"
BUILD_DIR="$ROOT_DIR/.smoke-build"
mkdir -p "$BUILD_DIR"
for service in player-svr farm-svr farm-worker farm-gate smoke-authority-init; do
  echo "  building $service"
  (cd "$ROOT_DIR" && GOCACHE="$BUILD_CACHE" CGO_ENABLED=0 go build -buildvcs=false -trimpath -o "$BUILD_DIR/$service" "./cmd/$service")
done

# --- Claim ShardWriteAuthority + generate route snapshot ----------------------
tmp_dir=$(mktemp -d)
PLAYER_BOOT="smoke-player-boot-${run_id}"
FARM_BOOT="smoke-farm-boot-${run_id}"
echo "=== claiming ShardWriteAuthority (player + farm) ==="
"$BUILD_DIR/smoke-authority-init" \
  --redis-url "$redis_url" \
  --namespace-env "$NAMESPACE_ENV" \
  --namespace-zone "$NAMESPACE_ZONE" \
  --profile "$PROFILE" \
  --realm "$REALM" \
  --run-id "$run_id" \
  --player-holder smoke-player --player-boot "$PLAYER_BOOT" --player-endpoint "127.0.0.1:${PLAYER_PORT}" \
  --farm-holder smoke-farm --farm-boot "$FARM_BOOT" --farm-endpoint "127.0.0.1:${FARM_PORT}" \
  --route-out "$tmp_dir/route-snapshot.json" 2>&1
echo "  route snapshot: $tmp_dir/route-snapshot.json"

# --- Common authority env ----------------------------------------------------
COMMON_AUTHORITY=(
  "ACTOR_AUTHORITY_MODE=authority"
  "ACTOR_ROUTE_SNAPSHOT=$tmp_dir/route-snapshot.json"
  "ACTOR_AUTHORITY_LEASE_TTL=55s"
  "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=3s"
  "ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=5s"
  "REDIS_PHYSICAL_PROFILE_VERSION=$PROFILE"
  "REDIS_NAMESPACE_ENVIRONMENT=$NAMESPACE_ENV"
  "REDIS_NAMESPACE_ZONE=$NAMESPACE_ZONE"
  "REDIS_AUTHORITY_LAYOUT_VERSION=v1"
  "REDIS_WORKFLOW_LAYOUT_VERSION=v1"
  "REDIS_WORKFLOW_HASH_VERSION=1"
  "REDIS_WORKFLOW_BUCKET_COUNT=256"
  "FARM_REDIS_URL=$redis_url"
  "WORKFLOW_STORE_PREFIX=$WORKFLOW_PREFIX"
)

# --- Helpers -----------------------------------------------------------------
wait_ready() {
  local endpoint="$1" service="$2" log_file="$3"
  for _ in $(seq 1 80); do
    if curl -fsS "$endpoint" >/dev/null 2>&1; then
      echo "  $service ready"
      return 0
    fi
    sleep 0.25
  done
  echo "ERROR: $service did not become ready at $endpoint" >&2
  tail -n 25 "$log_file" >&2 || true
  return 1
}

wait_tcp() {
  local port="$1" name="$2"
  for _ in $(seq 1 80); do
    if (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "ERROR: $name port $port did not accept TCP" >&2
  return 1
}

# --- PID tracking ------------------------------------------------------------
player_pid=""
farm_pid=""
worker_pid=""
gate_pid=""
cleanup() {
  echo "=== stopping services ==="
  for pid in "$gate_pid" "$worker_pid" "$farm_pid" "$player_pid"; do
    if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; fi
  done
  for pid in "$gate_pid" "$worker_pid" "$farm_pid" "$player_pid"; do
    if [[ -n "$pid" ]]; then wait "$pid" 2>/dev/null || true; fi
  done
  if [[ "${SMOKE_KEEP_TMP:-0}" == "1" ]]; then
    echo "kept tmp dir: $tmp_dir"
  else
    rm -rf "$tmp_dir"
  fi
}
trap cleanup EXIT INT TERM

# --- Start player-svr (authority) ---------------------------------------------
echo "=== starting player-svr (authority) ==="
env "${COMMON_AUTHORITY[@]}" \
  "ACTOR_HOLDER_POD=smoke-player" \
  "ACTOR_HOLDER_BOOT_ID=${PLAYER_BOOT}" \
  "PLAYER_LISTEN=127.0.0.1:${PLAYER_PORT}" \
  "PLAYER_HEALTH_LISTEN=127.0.0.1:${PLAYER_HEALTH}" \
  "PERMIT_SECRET=$PERMIT_SECRET_VALUE" \
  "$BUILD_DIR/player-svr" >"$tmp_dir/player.log" 2>&1 &
player_pid=$!
wait_ready "http://127.0.0.1:${PLAYER_HEALTH}/readyz" player-svr "$tmp_dir/player.log"

# --- Start farm-svr (authority) ------------------------------------------------
echo "=== starting farm-svr (authority) ==="
env "${COMMON_AUTHORITY[@]}" \
  "ACTOR_HOLDER_POD=smoke-farm" \
  "ACTOR_HOLDER_BOOT_ID=${FARM_BOOT}" \
  "FARM_LISTEN=127.0.0.1:${FARM_PORT}" \
  "FARM_HEALTH_LISTEN=127.0.0.1:${FARM_HEALTH}" \
  "PLAYER_ADDR=127.0.0.1:${PLAYER_PORT}" \
  "GATEWAY_DOWNLINK_ADDR=127.0.0.1:${GATE_DOWNLINK}" \
  "PERMIT_SECRET=$PERMIT_SECRET_VALUE" \
  "THEFT_PRF_MODE=test" \
  "THEFT_PRF_HMAC_SECRET=smoke-theft-prf-secret-20260806-at-least-32-bytes" \
  "FARM_CROP_MATURE_AFTER=$MATURE_AFTER" \
  "FARM_SUBSCRIPTION_LEASE_INITIAL_TTL=30s" \
  "FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION=30s" \
  "$BUILD_DIR/farm-svr" >"$tmp_dir/farm.log" 2>&1 &
farm_pid=$!
wait_ready "http://127.0.0.1:${FARM_HEALTH}/readyz" farm-svr "$tmp_dir/farm.log"

# --- Start farm-worker (legacy worker, needs route snapshot) -------------------
echo "=== starting farm-worker ==="
env \
  "ACTOR_AUTHORITY_MODE=legacy" \
  "ACTOR_ROUTE_SNAPSHOT=$tmp_dir/route-snapshot.json" \
  "FARM_REDIS_URL=$redis_url" \
  "WORKFLOW_STORE_PREFIX=$WORKFLOW_PREFIX" \
  "REDIS_PHYSICAL_PROFILE_VERSION=$PROFILE" \
  "REDIS_NAMESPACE_ENVIRONMENT=$NAMESPACE_ENV" \
  "REDIS_NAMESPACE_ZONE=$NAMESPACE_ZONE" \
  "REDIS_AUTHORITY_LAYOUT_VERSION=v1" \
  "REDIS_WORKFLOW_LAYOUT_VERSION=v1" \
  "REDIS_WORKFLOW_HASH_VERSION=1" \
  "REDIS_WORKFLOW_BUCKET_COUNT=256" \
  "PLAYER_ADDR=127.0.0.1:${PLAYER_PORT}" \
  "FARM_ADDR=127.0.0.1:${FARM_PORT}" \
  "WORKER_ID=smoke-worker" \
  "WORKER_BOOT_ID=smoke-worker-${run_id}" \
  "WORKER_LEASE_TOKEN=smoke-lease-${run_id}" \
  "WORKER_POLL_INTERVAL=100ms" \
  "WORKER_CLAIM_LIMIT=8" \
  "WORKER_CLAIM_LEASE=5s" \
  "WORKER_OPERATION_TIMEOUT=3s" \
  "WORKER_PARTICIPANT_BUDGET_MS=5000" \
  "WORKER_HEALTH_LISTEN=127.0.0.1:${WORKER_HEALTH}" \
  "$BUILD_DIR/farm-worker" >"$tmp_dir/worker.log" 2>&1 &
worker_pid=$!
wait_ready "http://127.0.0.1:${WORKER_HEALTH}/readyz" farm-worker "$tmp_dir/worker.log"

# --- Start farm-gate (optional) -------------------------------------------------
if [[ "$SKIP_GATE" == "0" ]]; then
  echo "=== starting farm-gate ==="
  env \
    "GATEWAY_LISTEN=127.0.0.1:${GATE_PORT}" \
    "GATEWAY_ID=smoke-gate" \
    "FARM_APP_PROFILE=test" \
    "GATEWAY_DOWNLINK_LISTEN=127.0.0.1:${GATE_DOWNLINK}" \
    "GATE_CONTROL_LISTEN=127.0.0.1:${GATE_CONTROL}" \
    "GATE_TRANSPORT=websocket" \
    "GATE_WS_PATH=/ws" \
    "GATE_WS_ALLOWED_ORIGINS=127.0.0.1:5173,localhost:5173,127.0.0.1:5174,localhost:5174,127.0.0.1:5184,localhost:5184,127.0.0.1:18500,localhost:18500" \
    "PLAYER_ADDR=127.0.0.1:${PLAYER_PORT}" \
    "FARM_ADDR=127.0.0.1:${FARM_PORT}" \
    "LOGIC_HEALTH_ADDR=http://127.0.0.1:${FARM_HEALTH}" \
    "FARM_REDIS_URL=$redis_url" \
    "WEB_ACCOUNT_STORE_PREFIX=farm:smoke:${run_id}:web-account" \
    "WEB_ACCOUNT_PLAYER_ID_BASE=1000000" \
    "WEB_LOGIN_RATE_LIMIT_PREFIX=farm:smoke:${run_id}:web-login-limit" \
    "DEMO_PLATFORM_TOKEN=demo-platform-token" \
    "DEMO_PLAYER_ID=10001" \
    "DEMO_SECONDARY_PLATFORM_TOKEN=demo-platform-token-b" \
    "DEMO_SECONDARY_PLAYER_ID=10002" \
    "HTTP_SESSION_TTL=2h" \
    "HTTP_SESSION_COOKIE_SECURE=0" \
    "PERMIT_SECRET=$PERMIT_SECRET_VALUE" \
    "GATEWAY_TICKET_TTL=60s" \
    "GATE_ROUTE_LEASE_TTL=30s" \
    "GATE_BOOT_LEASE_PREFIX=farm:smoke:${run_id}:gate-boot-lease" \
    "GATE_BOOT_LEASE_TTL=15s" \
    "GATE_BOOT_LEASE_RENEW_TIMEOUT=3s" \
    "INITIAL_RULE_VERSION=1" \
    "GATE_WS_METRICS=1" \
    "GATE_WS_RATE_PER_SEC=100" \
    "GATE_WS_RATE_BURST=200" \
    "WSS_ENDPOINT=ws://127.0.0.1:${GATE_PORT}/ws" \
    "$BUILD_DIR/farm-gate" >"$tmp_dir/gate.log" 2>&1 &
  gate_pid=$!
  wait_tcp "$GATE_PORT" farm-gate
  echo "  farm-gate ready"
else
  echo "=== farm-gate SKIPPED (--no-gate) ==="
fi

# --- Summary -------------------------------------------------------------------
echo ""
echo "=== Authority Smoke environment ready ==="
echo "  player-svr:  127.0.0.1:${PLAYER_PORT} (health :${PLAYER_HEALTH}) pid=$player_pid"
echo "  farm-svr:    127.0.0.1:${FARM_PORT} (health :${FARM_HEALTH}) pid=$farm_pid"
echo "  farm-worker: health :${WORKER_HEALTH} pid=$worker_pid"
if [[ "$SKIP_GATE" == "0" ]]; then
  echo "  farm-gate:   127.0.0.1:${GATE_PORT} (downlink :${GATE_DOWNLINK}) pid=$gate_pid"
else
  echo "  farm-gate:   NOT STARTED"
fi
echo "  namespace:   $NAMESPACE_ENV/$NAMESPACE_ZONE"
echo "  realm:       $REALM"
echo "  route:       $tmp_dir/route-snapshot.json"
echo "  logs:        $tmp_dir/*.log"
echo "  tmp_dir:     $tmp_dir"
echo ""

# Write PID file for external scripts
cat >"$tmp_dir/smoke-env.json" <<ENVEOF
{
  "tmp_dir": "$tmp_dir",
  "player_pid": $player_pid,
  "farm_pid": $farm_pid,
  "worker_pid": $worker_pid,
  "gate_pid": ${gate_pid:-null},
  "player_port": $PLAYER_PORT,
  "player_health": $PLAYER_HEALTH,
  "farm_port": $FARM_PORT,
  "farm_health": $FARM_HEALTH,
  "worker_health": $WORKER_HEALTH,
  "gate_port": $GATE_PORT,
  "gate_downlink": $GATE_DOWNLINK,
  "gate_control": $GATE_CONTROL,
  "redis_url": "$(echo "$redis_url" | sed 's/:[^:@]*@/:***@/')",
  "namespace": "$NAMESPACE_ENV",
  "realm": "$REALM",
  "run_id": "$run_id"
}
ENVEOF

echo "smoke-env written to $tmp_dir/smoke-env.json"
echo "SMOKE_TMP_DIR=$tmp_dir"

# Keep running until interrupted
echo "Press Ctrl+C to stop all services."
wait
