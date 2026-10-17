#!/usr/bin/env bash
set -Eeuo pipefail

# Real browser -> farm-gate -> player/farm -> company Redis A/B runner.
#
# Every variant receives an isolated Redis namespace and a freshly claimed
# one-shard PLAYER/FARM authority snapshot. Services bind random loopback ports,
# so this runner never replaces the developer's existing Compose stack.
# Secrets, Redis keys, payloads and replies are intentionally absent from the
# evidence directory.

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
h5_root="${FARM_PERF_H5_ROOT:-/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-opportunity-real-e2e-20260811}"
redis_config="${FARM_REDIS_CONFIG:-/data/home/developer_026/CodeBuddy/20260519141303/farm-game/deploy/secrets/redis.conf.local}"
friends="${FARM_PERF_FRIENDS:-300}"
plots="${FARM_PERF_PLOTS:-5}"
horizon_ms="${FARM_PERF_HORIZON_MS:-130000}"
variants="${FARM_PERF_VARIANTS:-A B1 B2}"
wakeup_poll_interval="${FARM_PERF_WAKEUP_POLL_INTERVAL:-250ms}"
header_read_mode="${FARM_PERF_HEADER_READ_MODE:-full}"
hint_transport="${FARM_PERF_HINT_TRANSPORT:-pubsub}"
run_id="${FARM_PERF_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
run_slug="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')"
evidence_root="${FARM_PERF_EVIDENCE_ROOT:-$root_dir/docs/verification/evidence/friend-opportunity-real-ab/$run_id}"
node_bin="${FARM_PERF_NODE_BIN:-/data/home/developer_026/.nvm/versions/node/v22.23.1/bin}"
build_dir="$root_dir/.local-build/friend-opportunity-real-ab"
state_root="$root_dir/.local-state/friend-opportunity-real-ab/$run_id"

mkdir -p "$evidence_root" "$build_dir" "$state_root" /tmp/farmgame-go-cache

read_config_value() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*${key}:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\\1/p" "$redis_config" | head -n 1
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

load_company_redis() {
  if [[ -n "${FARM_REDIS_URL:-}" ]]; then
    return 0
  fi
  if [[ ! -f "$redis_config" ]]; then
    printf 'missing company Redis config: %s\n' "$redis_config" >&2
    return 2
  fi
  local host port password database
  host="$(read_config_value host)"
  port="$(read_config_value port)"
  password="REPLACE_WITH_LOCAL_SECRET"
  database="$(read_config_value db)"
  database="${database:-0}"
  if [[ -z "$host" || -z "$port" || -z "$password" ]]; then
    printf 'company Redis config is incomplete\n' >&2
    return 2
  fi
  FARM_REDIS_URL="redis://:$(uri_encode "$password")@${host}:${port}/${database}"
  export FARM_REDIS_URL
  unset password
}

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_http() {
  local url="$1" log_file="$2" deadline=$((SECONDS + 45))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'readiness timeout: %s\n' "$url" >&2
      tail -n 80 "$log_file" >&2 || true
      return 1
    fi
    sleep 0.2
  done
}

start_process() {
  local state_dir="$1" name="$2"
  shift 2
  "$@" >"$state_dir/$name.log" 2>&1 &
  printf '%s\n' "$!" >"$state_dir/$name.pid"
}

stop_stack() {
  local state_dir="${1:-}"
  [[ -n "$state_dir" && -d "$state_dir" ]] || return 0
  local name pid
  for name in gate worker farm player; do
    if [[ -f "$state_dir/$name.pid" ]]; then
      pid="$(<"$state_dir/$name.pid")"
      kill "$pid" 2>/dev/null || true
    fi
  done
  for name in gate worker farm player; do
    if [[ -f "$state_dir/$name.pid" ]]; then
      pid="$(<"$state_dir/$name.pid")"
      for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
      fi
      wait "$pid" 2>/dev/null || true
    fi
  done
}

active_state=""
cleanup() {
  stop_stack "$active_state"
}
trap cleanup EXIT INT TERM

load_company_redis

cd "$root_dir"
for service in player-svr farm-svr farm-worker farm-gate authority-smoke-fixture friend-opportunity-perf-fixture; do
  GOCACHE=/tmp/farmgame-go-cache CGO_ENABLED=0 go build -buildvcs=false -trimpath \
    -o "$build_dir/$service" "./cmd/$service"
done

printf '%s\n' \
  "run_id=$run_id" \
  "redis=company-managed" \
  "topology=random-loopback-real-processes" \
  "friends=$friends" \
  "plots=$plots" \
  "horizon_ms=$horizon_ms" \
  "variants=$variants" \
  "header_read_mode=$header_read_mode" \
  >"$evidence_root/run.txt"

run_failed=false
for variant in $variants; do
  case "$variant" in
    A) attach=false; wakeup=false; client_mode=A ;;
    B1) attach=true; wakeup=true; client_mode=B1 ;;
    B2) attach=true; wakeup=true; client_mode=B2 ;;
    *) printf 'unsupported variant: %s\n' "$variant" >&2; exit 2 ;;
  esac

  variant_lower="$(printf '%s' "$variant" | tr '[:upper:]' '[:lower:]')"
  namespace="foperf-${variant_lower}-${run_slug}"
  state_dir="$state_root/$variant_lower"
  target="$evidence_root/$variant_lower"
  mkdir -p "$state_dir" "$target"
  active_state="$state_dir"

  player_port="$(pick_port)"; player_health="$(pick_port)"
  farm_port="$(pick_port)"; farm_health="$(pick_port)"
  worker_health="$(pick_port)"
  gate_http="$(pick_port)"; gate_downlink="$(pick_port)"; gate_control="$(pick_port)"
  h5_port="$(pick_port)"
  route_file="$state_dir/route-snapshot.json"
  player_holder="foperf-${variant_lower}-player"
  player_boot="${player_holder}-${run_slug}-boot"
  farm_holder="foperf-${variant_lower}-farm"
  farm_boot="${farm_holder}-${run_slug}-boot"
  permit_secret="foperf-${variant_lower}-${run_slug}-permit-secret-at-least-32-bytes"
  profile="redis-physical-v1"
  realm="foperf-${variant_lower}-${run_slug}"
  workflow_prefix="foperf:${variant_lower}:${run_slug}:workflow"

  "$build_dir/authority-smoke-fixture" init \
    --redis-url "$FARM_REDIS_URL" --namespace "$namespace" --zone z1 \
    --profile "$profile" --realm "$realm" --run-id "$run_slug-$variant_lower" \
    --player-holder "$player_holder" --player-boot "$player_boot" \
    --player-endpoint "127.0.0.1:$player_port" \
    --farm-holder "$farm_holder" --farm-boot "$farm_boot" \
    --farm-endpoint "127.0.0.1:$farm_port" \
    --route-out "$route_file" --json-out "$state_dir/authority-fixture.json" \
    >"$state_dir/authority-fixture.log" 2>&1

  common_authority=(
    "ACTOR_AUTHORITY_MODE=authority" "ACTOR_ROUTE_SNAPSHOT=$route_file"
    "ACTOR_AUTHORITY_LEASE_TTL=55s" "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=3s"
    "ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=5s" "AUTHORITY_MEMBERSHIP_MODE=false"
    "REDIS_PHYSICAL_PROFILE_VERSION=$profile" "REDIS_NAMESPACE_ENVIRONMENT=$namespace"
    "REDIS_NAMESPACE_ZONE=z1" "REDIS_AUTHORITY_LAYOUT_VERSION=v1"
    "REDIS_WORKFLOW_LAYOUT_VERSION=v1" "REDIS_WORKFLOW_HASH_VERSION=1"
    "REDIS_WORKFLOW_BUCKET_COUNT=256" "FARM_REDIS_URL=$FARM_REDIS_URL"
    "WORKFLOW_STORE_PREFIX=$workflow_prefix" "REDIS_WORKLOAD_METRICS=true"
  )

  start_process "$state_dir" player env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$player_holder" "ACTOR_HOLDER_BOOT_ID=$player_boot" \
    "PLAYER_LISTEN=127.0.0.1:$player_port" "PLAYER_HEALTH_LISTEN=127.0.0.1:$player_health" \
    "FARM_ADDR=127.0.0.1:$farm_port" "PERMIT_SECRET=$permit_secret" \
    "FARM_FRIEND_NAVIGATION_V3_ENABLED=1" \
    "$build_dir/player-svr"
  wait_http "http://127.0.0.1:$player_health/readyz" "$state_dir/player.log"

  start_process "$state_dir" farm env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$farm_holder" "ACTOR_HOLDER_BOOT_ID=$farm_boot" \
    "FARM_LISTEN=127.0.0.1:$farm_port" "FARM_HEALTH_LISTEN=127.0.0.1:$farm_health" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "GATEWAY_DOWNLINK_ADDR=127.0.0.1:$gate_downlink" \
    "PERMIT_SECRET=$permit_secret" "THEFT_PRF_MODE=test" \
    "THEFT_PRF_HMAC_SECRET=foperf-$run_slug-theft-prf-secret-at-least-32-bytes" \
    "FARM_CROP_MATURE_AFTER=10s" "FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED=$attach" \
    "FARM_OPPORTUNITY_HEADER_READ_MODE=$header_read_mode" \
    "FARM_OPPORTUNITY_HEADER_FULL_FALLBACK=8" \
    "GATE_BOOT_REAPER_MODE=disabled" "FARM_SUBSCRIPTION_LEASE_INITIAL_TTL=60s" \
    "FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION=60s" \
    "$build_dir/farm-svr"
  wait_http "http://127.0.0.1:$farm_health/readyz" "$state_dir/farm.log"

  start_process "$state_dir" worker env \
    "ACTOR_ROUTE_SNAPSHOT=$route_file" "FARM_REDIS_URL=$FARM_REDIS_URL" \
    "REDIS_WORKLOAD_METRICS=true" "REDIS_PHYSICAL_PROFILE_VERSION=$profile" \
    "REDIS_NAMESPACE_ENVIRONMENT=$namespace" "REDIS_NAMESPACE_ZONE=z1" \
    "REDIS_AUTHORITY_LAYOUT_VERSION=v1" "REDIS_WORKFLOW_LAYOUT_VERSION=v1" \
    "REDIS_WORKFLOW_HASH_VERSION=1" "REDIS_WORKFLOW_BUCKET_COUNT=256" \
    "WORKFLOW_STORE_PREFIX=$workflow_prefix" "PLAYER_ADDR=127.0.0.1:$player_port" \
    "FARM_ADDR=127.0.0.1:$farm_port" "WORKER_HEALTH_LISTEN=127.0.0.1:$worker_health" \
    "WORKER_ID=foperf-$variant_lower-worker" "WORKER_BOOT_ID=foperf-$run_slug-$variant_lower-worker-boot" \
    "WORKER_LEASE_TOKEN=foperf-$run_slug-$variant_lower-worker-lease" \
    "WORKER_POLL_INTERVAL=250ms" "WORKER_CLAIM_LEASE=5s" "WORKER_OPERATION_TIMEOUT=3s" \
    "THEFT_DUE_OBSERVE_INTERVAL=10s" "THEFT_WORKER_IDLE_MAX_INTERVAL=2s" \
    "FARM_OPPORTUNITY_WAKEUP_ENABLED=$wakeup" "FARM_OPPORTUNITY_WAKEUP_SHADOW=false" \
    "FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL=$wakeup_poll_interval" \
    "FARM_OPPORTUNITY_WAKEUP_CLAIM_LIMIT=64" \
    "FARM_OPPORTUNITY_WAKEUP_DISCOVERY_LIMIT=256" \
    "FARM_OPPORTUNITY_WAKEUP_MAX_CONCURRENT=4" \
    "FARM_OPPORTUNITY_WAKEUP_ROUND_TIME_BUDGET=800ms" \
    "FARM_OPPORTUNITY_HINT_TRANSPORT=$hint_transport" \
    "GATEWAY_DOWNLINK_ROUTES=foperf-$variant_lower-gate=127.0.0.1:$gate_downlink" \
    "PERMIT_SECRET=$permit_secret" \
    "$build_dir/farm-worker"
  wait_http "http://127.0.0.1:$worker_health/readyz" "$state_dir/worker.log"

  start_process "$state_dir" gate env \
    "GATEWAY_LISTEN=127.0.0.1:$gate_http" "GATEWAY_ID=foperf-$variant_lower-gate" \
    "FARM_APP_PROFILE=test" "ACTOR_AUTHORITY_MODE=authority" \
    "FRIENDSHIP_SERVICE_MODE=remote_player" "FARM_FRIEND_NAVIGATION_V3_ENABLED=1" \
    "FARM_OPPORTUNITY_HINT_DIAGNOSTICS=true" \
    "GATEWAY_DOWNLINK_LISTEN=127.0.0.1:$gate_downlink" \
    "GATE_CONTROL_LISTEN=127.0.0.1:$gate_control" \
    "GATE_TRANSPORT=websocket" "GATE_WS_PATH=/ws" \
    "GATE_WS_ALLOWED_ORIGINS=127.0.0.1:$h5_port,localhost:$h5_port" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "FARM_ADDR=127.0.0.1:$farm_port" \
    "LOGIC_HEALTH_ADDR=http://127.0.0.1:$farm_health" "FARM_REDIS_URL=$FARM_REDIS_URL" \
    "REDIS_WORKLOAD_METRICS=true" "REDIS_NAMESPACE_ENVIRONMENT=$namespace" \
    "REDIS_NAMESPACE_ZONE=z1" "ACTOR_ROUTE_SNAPSHOT=$route_file" \
    "WEB_ACCOUNT_STORE_PREFIX=foperf:$variant_lower:$run_slug:web-account" \
    "WEB_ACCOUNT_PLAYER_ID_BASE=813000000" \
    "WEB_LOGIN_RATE_LIMIT_PREFIX=foperf:$variant_lower:$run_slug:web-login-limit" \
    "WEB_LOGIN_IP_BURST=1000" "WEB_LOGIN_DEVICE_BURST=1000" \
    "SHARE_STORE_PREFIX=foperf:$variant_lower:$run_slug:share" \
    "GATE_BOOT_LEASE_PREFIX=foperf:$variant_lower:$run_slug:gate-boot" \
    "HTTP_SESSION_COOKIE_SECURE=0" "PERMIT_SECRET=$permit_secret" \
    "GATE_WS_METRICS=1" "GATE_WS_RATE_PER_SEC=1000" "GATE_WS_RATE_BURST=2000" \
    "WSS_ENDPOINT=ws://127.0.0.1:$gate_http/ws" \
    "$build_dir/farm-gate"
  wait_http "http://127.0.0.1:$gate_http/readyz" "$state_dir/gate.log"

  printf '%s\n' \
    "namespace=$namespace" "attach=$attach" "wakeup=$wakeup" "client_mode=$client_mode" \
    "header_read_mode=$header_read_mode" "hint_transport=$hint_transport" \
    "friend_navigation=v3-player-svr" \
    "topology=random-loopback-real-processes" >"$target/config.txt"

  if ! (
    cd "$h5_root"
    env PATH="$node_bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
      FARM_E2E_VARIANT="$client_mode" VITE_FRIEND_OPPORTUNITY_EXPERIMENT="$client_mode" \
      VITE_E2E_DIAGNOSTICS=1 \
      PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/data/home/developer_026/.cache/ms-playwright}" \
      FARM_E2E_EVIDENCE_ROOT="$target" FARM_E2E_BACKEND_ROOT="$root_dir" \
      FARM_E2E_FIXTURE_BIN="$build_dir/friend-opportunity-perf-fixture" \
      FARM_E2E_ROUTE_SNAPSHOT="$route_file" \
      FARM_E2E_H5_PORT="$h5_port" \
      FARM_E2E_PERF_FRIENDS="$friends" FARM_E2E_PERF_PLOTS="$plots" \
      FARM_E2E_PERF_HORIZON_MS="$horizon_ms" FARM_REDIS_URL="$FARM_REDIS_URL" \
      REDIS_NAMESPACE_ENVIRONMENT="$namespace" REDIS_NAMESPACE_ZONE=z1 \
      REDIS_PHYSICAL_PROFILE_VERSION="$profile" REDIS_AUTHORITY_LAYOUT_VERSION=v1 \
      REDIS_WORKFLOW_LAYOUT_VERSION=v1 REDIS_WORKFLOW_HASH_VERSION=1 \
      REDIS_WORKFLOW_BUCKET_COUNT=256 WORKFLOW_STORE_PREFIX="$workflow_prefix" \
      PLAYER_ADDR="127.0.0.1:$player_port" FARM_ADDR="127.0.0.1:$farm_port" \
      FARM_E2E_API_BASE_URL="http://127.0.0.1:$gate_http" \
      FARM_E2E_WSS_URL="ws://127.0.0.1:$gate_http/ws" \
      FARM_E2E_GATE_METRICS="http://127.0.0.1:$gate_http/metrics" \
      FARM_E2E_PLAYER_METRICS="http://127.0.0.1:$player_health/metrics" \
      FARM_E2E_FARM_METRICS="http://127.0.0.1:$farm_health/metrics" \
      FARM_E2E_WORKER_METRICS="http://127.0.0.1:$worker_health/metrics" \
      npx playwright test src/test/e2e/web-friend-opportunity-performance.spec.ts \
        --config playwright.web.config.ts --workers=1
  ); then
    run_failed=true
  fi

  cp "$state_dir/player.log" "$target/player.log"
  cp "$state_dir/farm.log" "$target/farm.log"
  cp "$state_dir/worker.log" "$target/worker.log"
  cp "$state_dir/gate.log" "$target/gate.log"
  stop_stack "$state_dir"
  active_state=""
done

if [[ "$run_failed" == "true" ]]; then
  exit 1
fi
