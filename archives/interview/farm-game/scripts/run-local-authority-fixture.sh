#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REDIS_CONFIG="${FARM_REDIS_CONFIG:-$ROOT_DIR/deploy/secrets/redis.conf.local}"
NAMESPACE="${1:?usage: run-local-authority-fixture.sh <namespace> <route-out> [run-id]}"
ROUTE_OUT="${2:?usage: run-local-authority-fixture.sh <namespace> <route-out> [run-id]}"
RUN_ID="${3:-$(date +%Y%m%d%H%M%S)}"

read_config_value() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*${key}:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\1/p" "$REDIS_CONFIG" | head -n 1
}

uri_encode() {
  local input="$1" output="" char encoded index
  LC_ALL=C
  for ((index=0; index<${#input}; index++)); do
    char="${input:index:1}"
    case "$char" in
      [a-zA-Z0-9.~_-]) output+="$char" ;;
      *) printf -v encoded '%%%02X' "'$char"; output+="$encoded" ;;
    esac
  done
  printf '%s' "$output"
}

redis_host="$(read_config_value host)"
redis_port="$(read_config_value port)"
redis_password="REPLACE_WITH_LOCAL_SECRET"
redis_db="$(read_config_value db)"
redis_db="${redis_db:-0}"
if [[ -z "$redis_host" || -z "$redis_port" || -z "$redis_password" ]]; then
  echo "Redis config must contain host, port and password" >&2
  exit 1
fi

export FARM_REDIS_URL="redis://:$(uri_encode "$redis_password")@${redis_host}:${redis_port}/${redis_db}"
exec "$ROOT_DIR/.local-build/authority-smoke-fixture" init \
  --namespace "$NAMESPACE" \
  --zone z1 \
  --realm "foe2e-${RUN_ID}" \
  --run-id "$RUN_ID" \
  --player-holder foe2e-player \
  --player-boot foe2e-player-boot-1 \
  --player-endpoint 127.0.0.1:19100 \
  --farm-holder foe2e-farm \
  --farm-boot foe2e-farm-boot-1 \
  --farm-endpoint 127.0.0.1:19000 \
  --route-out "$ROUTE_OUT"
