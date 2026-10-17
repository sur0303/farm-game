#!/usr/bin/env bash
# Isolated real-process stack for Q1 external pressure profiles.
# shellcheck shell=bash

set -Eeuo pipefail

perf_pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

perf_wait_http() {
  local url=$1 timeout_seconds=${2:-30}
  local deadline=$((SECONDS + timeout_seconds))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'readiness timeout: %s\n' "$url" >&2
      return 1
    fi
    sleep 0.1
  done
}

perf_wait_tcp() {
  local port=$1 timeout_seconds=${2:-30}
  local deadline=$((SECONDS + timeout_seconds))
  until (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      printf 'TCP readiness timeout: 127.0.0.1:%s\n' "$port" >&2
      return 1
    fi
    sleep 0.1
  done
}

perf_build_binaries() {
  local backend_root=$1 load_root=$2 build_dir=$3
  mkdir -p "$build_dir"
  local cache=${FARM_GO_BUILD_CACHE:-/tmp/farmgame-go-cache}
  local service
  for service in player-svr farm-svr farm-worker farm-gate authority-smoke-fixture; do
    printf 'building backend-under-test %s\n' "$service"
    (cd "$backend_root" && GOCACHE="$cache" CGO_ENABLED=0 go build -buildvcs=false -trimpath -o "$build_dir/$service" "./cmd/$service")
  done
  printf 'building frozen load tools ws-stress and perf-baseline journey bot\n'
  (cd "$load_root" && GOCACHE="$cache" CGO_ENABLED=0 go build -buildvcs=false -trimpath -o "$build_dir/ws-stress" ./cmd/ws-stress)
  (cd "$load_root" && GOCACHE="$cache" CGO_ENABLED=0 go build -buildvcs=false -trimpath -o "$build_dir/perf-baseline" ./cmd/perf-baseline)
}

perf_stack_start_process() {
  local state_dir=$1 name=$2
  shift 2
  "$@" >"$state_dir/$name.log" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" >"$state_dir/$name.pid"
}

perf_stack_up() {
  local build_dir=$1 state_dir=$2 namespace=$3 max_connections=$4 run_label=$5
  mkdir -p "$state_dir"
  chmod 700 "$state_dir"
  if [[ ! "$namespace" =~ ^q1perf-[a-z0-9][a-z0-9-]{5,47}$ ]]; then
    printf 'refusing unsafe namespace: %s\n' "$namespace" >&2
    return 2
  fi

  local redis_container="q1perf-${run_label//[^a-zA-Z0-9_.-]/-}-$RANDOM"
  local stack_complete=false
  redis_container=${redis_container:0:63}
  # perf_stack_up is usually called in a command-substitution subshell. Clean
  # partial processes/container inside that subshell if readiness fails before
  # stack.env can be returned to the parent coordinator.
  trap 'if [[ "$stack_complete" != true ]]; then
    local partial_name partial_pid
    for partial_name in gate worker farm player; do
      if [[ -f "$state_dir/$partial_name.pid" ]]; then
        partial_pid=$(<"$state_dir/$partial_name.pid")
        kill "$partial_pid" 2>/dev/null || true
      fi
    done
    docker rm -f "$redis_container" >/dev/null 2>&1 || true
  fi' RETURN
  docker run -d --rm --name "$redis_container" -p 127.0.0.1::6379 redis:7.4-alpine \
    redis-server --save '' --appendonly no --latency-tracking yes >"$state_dir/redis-container-id.txt"
  local redis_mapping redis_port redis_url
  for _ in $(seq 1 100); do
    redis_mapping=$(docker port "$redis_container" 6379/tcp 2>/dev/null | head -1 || true)
    [[ -n "$redis_mapping" ]] && break
    sleep 0.1
  done
  redis_port=${redis_mapping##*:}
  redis_url="redis://127.0.0.1:${redis_port}/0"
  for _ in $(seq 1 100); do
    if docker exec "$redis_container" redis-cli ping 2>/dev/null | grep -q PONG; then break; fi
    sleep 0.1
  done
  docker exec "$redis_container" redis-cli ping 2>/dev/null | grep -q PONG

  local player_port farm_port player_health farm_health worker_health gate_http gate_downlink gate_control
  player_port=$(perf_pick_port); farm_port=$(perf_pick_port)
  player_health=$(perf_pick_port); farm_health=$(perf_pick_port); worker_health=$(perf_pick_port)
  gate_http=$(perf_pick_port); gate_downlink=$(perf_pick_port); gate_control=$(perf_pick_port)

  local route="$state_dir/route-snapshot.json"
  local load_secret permit_secret profile realm workflow_prefix share_prefix web_prefix gate_lease_prefix
  load_secret=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
  permit_secret="q1-perf-${run_label}-permit-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  profile="q1-perf-${run_label}-v1"
  realm="q1-perf-${run_label}"
  workflow_prefix="q1perf:${run_label}:workflow"
  share_prefix="q1perf:${run_label}:share"
  web_prefix="q1perf:${run_label}:web"
  gate_lease_prefix="q1perf:${run_label}:gate-lease"
  local player_holder="q1perf-${run_label}-player" farm_holder="q1perf-${run_label}-farm"
  local player_boot="${player_holder}-boot" farm_boot="${farm_holder}-boot"

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
    "WORKFLOW_STORE_PREFIX=$workflow_prefix"
  )

  perf_stack_start_process "$state_dir" player env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$player_holder" "ACTOR_HOLDER_BOOT_ID=$player_boot" \
    "PLAYER_LISTEN=127.0.0.1:$player_port" "PLAYER_HEALTH_LISTEN=127.0.0.1:$player_health" \
    "PERMIT_SECRET=$permit_secret" "$build_dir/player-svr"

  perf_stack_start_process "$state_dir" farm env "${common_authority[@]}" \
    "ACTOR_HOLDER_POD=$farm_holder" "ACTOR_HOLDER_BOOT_ID=$farm_boot" \
    "FARM_LISTEN=127.0.0.1:$farm_port" "FARM_HEALTH_LISTEN=127.0.0.1:$farm_health" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "GATEWAY_DOWNLINK_ADDR=127.0.0.1:$gate_downlink" \
    "PERMIT_SECRET=$permit_secret" \
    "THEFT_PRF_MODE=test" "THEFT_PRF_HMAC_SECRET=q1perf-${run_label}-theft-prf-at-least-32-bytes" \
    "GATE_BOOT_REAPER_MODE=disabled" "FARM_SUBSCRIPTION_LEASE_INITIAL_TTL=60s" \
    "FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION=60s" "FARM_CROP_MATURE_AFTER=2s" \
    "MAX_CONCURRENT_ACTOR_LOADS=512" "MAX_PENDING_ACTOR_LOADS=2048" \
    "ACTIVATION_RATE_PER_SEC=10000" "DOWNLINK_LANE_QUEUE_SIZE=1024" "DOWNLINK_BATCH_SIZE=64" \
    "$build_dir/farm-svr"

  perf_wait_http "http://127.0.0.1:$player_health/readyz"
  perf_wait_http "http://127.0.0.1:$farm_health/readyz"

  perf_stack_start_process "$state_dir" worker env \
    "ACTOR_AUTHORITY_MODE=authority" "ACTOR_ROUTE_SNAPSHOT=$route" "FARM_REDIS_URL=$redis_url" \
    "WORKFLOW_STORE_PREFIX=$workflow_prefix" "REDIS_PHYSICAL_PROFILE_VERSION=$profile" \
    "REDIS_NAMESPACE_ENVIRONMENT=$namespace" "REDIS_NAMESPACE_ZONE=local" \
    "REDIS_AUTHORITY_LAYOUT_VERSION=v1" "REDIS_WORKFLOW_LAYOUT_VERSION=v1" \
    "REDIS_WORKFLOW_HASH_VERSION=1" "REDIS_WORKFLOW_BUCKET_COUNT=256" \
    "PLAYER_ADDR=127.0.0.1:$player_port" "FARM_ADDR=127.0.0.1:$farm_port" \
    "WORKER_ID=q1perf-${run_label}-worker" "WORKER_BOOT_ID=q1perf-${run_label}-worker-boot" \
    "WORKER_LEASE_TOKEN=q1perf-${run_label}-worker-lease" "WORKER_POLL_INTERVAL=20ms" \
    "WORKER_CLAIM_LIMIT=256" "WORKER_CLAIM_LEASE=5s" "WORKER_OPERATION_TIMEOUT=3s" \
    "WORKER_PARTICIPANT_BUDGET_MS=5000" "WORKER_HEALTH_LISTEN=127.0.0.1:$worker_health" \
    "$build_dir/farm-worker"
  perf_wait_http "http://127.0.0.1:$worker_health/readyz"

  perf_stack_start_process "$state_dir" gate env \
    "GATEWAY_LISTEN=127.0.0.1:$gate_http" "GATEWAY_ID=q1perf-${run_label}-gate" \
    "GATEWAY_DOWNLINK_LISTEN=127.0.0.1:$gate_downlink" "GATE_CONTROL_LISTEN=127.0.0.1:$gate_control" \
    "GATE_TRANSPORT=websocket" "GATE_WS_PATH=/ws" "GATE_WS_METRICS=1" \
    "GATE_WS_RATE_PER_SEC=50000" "GATE_WS_RATE_BURST=100000" \
    "GATE_WS_MAX_CONNS=$max_connections" "GATE_PREAUTH_MAX_CONNECTIONS=$max_connections" \
    "GATE_PREAUTH_MAX_CONNECTIONS_PER_IP=$max_connections" "GATE_WS_COMMAND_QUEUE_SIZE=128" \
    "ACTOR_ROUTE_SNAPSHOT=$route" "PLAYER_ADDR=127.0.0.1:$player_port" \
    "FARM_ADDR=127.0.0.1:$farm_port" "LOGIC_HEALTH_ADDR=http://127.0.0.1:$farm_health" \
    "FARM_REDIS_URL=$redis_url" \
    "SHARE_STORE_PREFIX=$share_prefix" "WEB_ACCOUNT_STORE_PREFIX=$web_prefix" \
    "GATE_BOOT_LEASE_PREFIX=$gate_lease_prefix" "WEB_LOGIN_IP_BURST=100000" \
    "WEB_LOGIN_DEVICE_BURST=100" "LOAD_TEST_TOKEN_SECRET=$load_secret" \
    "PERMIT_SECRET=$permit_secret" "WSS_ENDPOINT=ws://127.0.0.1:$gate_http/ws" \
    "$build_dir/farm-gate"
  perf_wait_http "http://127.0.0.1:$gate_http/readyz"
  perf_wait_tcp "$gate_downlink"

  local env_file="$state_dir/stack.env"
  umask 077
  {
    printf 'PERF_REDIS_CONTAINER=%q\n' "$redis_container"
    printf 'PERF_REDIS_URL=%q\n' "$redis_url"
    printf 'PERF_REDIS_TARGET=%q\n' "redis://127.0.0.1:${redis_port}/0"
    printf 'PERF_NAMESPACE=%q\n' "$namespace"
    printf 'PERF_LOAD_SECRET=%q\n' "$load_secret"
    printf 'PERF_GATE_HTTP=%q\n' "http://127.0.0.1:$gate_http"
    printf 'PERF_GATE_WS=%q\n' "ws://127.0.0.1:$gate_http/ws"
    printf 'PERF_GATE_METRICS=%q\n' "http://127.0.0.1:$gate_http/metrics"
    printf 'PERF_PLAYER_METRICS=%q\n' "http://127.0.0.1:$player_health/metrics"
    printf 'PERF_FARM_METRICS=%q\n' "http://127.0.0.1:$farm_health/metrics"
    printf 'PERF_WORKER_METRICS=%q\n' "http://127.0.0.1:$worker_health/metrics"
    printf 'PERF_PLAYER_PID=%q\n' "$(<"$state_dir/player.pid")"
    printf 'PERF_FARM_PID=%q\n' "$(<"$state_dir/farm.pid")"
    printf 'PERF_WORKER_PID=%q\n' "$(<"$state_dir/worker.pid")"
    printf 'PERF_GATE_PID=%q\n' "$(<"$state_dir/gate.pid")"
  } >"$env_file"
  chmod 600 "$env_file"
  stack_complete=true
  printf '%s\n' "$env_file"
}

perf_stack_down() {
  local state_dir=$1
  local env_file="$state_dir/stack.env"
  [[ -f "$env_file" ]] || return 0
  # shellcheck disable=SC1090
  source "$env_file"
  local pid name
  for name in gate worker farm player; do
    if [[ -f "$state_dir/$name.pid" ]]; then
      pid=$(<"$state_dir/$name.pid")
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 0.5
  for name in gate worker farm player; do
    if [[ -f "$state_dir/$name.pid" ]]; then
      pid=$(<"$state_dir/$name.pid")
      if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "${PERF_REDIS_CONTAINER:-}" ]]; then
    docker logs "$PERF_REDIS_CONTAINER" >"$state_dir/redis.log" 2>&1 || true
    docker rm -f "$PERF_REDIS_CONTAINER" >/dev/null 2>&1 || true
    if docker inspect "$PERF_REDIS_CONTAINER" >/dev/null 2>&1; then
      printf 'failed to destroy exact ephemeral Redis container %s\n' "$PERF_REDIS_CONTAINER" >&2
      return 1
    fi
  fi
  rm -f "$env_file"
  printf '{"cleanup":"destroy_exact_ephemeral_container","completed":true,"finished_at":"%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$state_dir/cleanup.json"
}
