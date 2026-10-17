#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${FARM_COMPOSE_FILE:-$ROOT_DIR/deploy/docker-compose.local.yml}"
REDIS_CONFIG="${FARM_REDIS_CONFIG:-$ROOT_DIR/deploy/secrets/redis.conf.local}"

read_config_value() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*${key}:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\\1/p" "$REDIS_CONFIG" | head -n 1
}

uri_encode() {
  local input="$1"
  local output=""
  local char encoded
  local index
  LC_ALL=C
  for ((index = 0; index < ${#input}; index++)); do
    char="${input:index:1}"
    case "$char" in
      [a-zA-Z0-9.~_-])
        output+="$char"
        ;;
      *)
        printf -v encoded '%%%02X' "'$char"
        output+="$encoded"
        ;;
    esac
  done
  printf '%s' "$output"
}

if [[ -z "${FARM_REDIS_URL:-}" && -z "${FARM_REDIS_CLUSTER_URLS:-}" ]]; then
  if [[ ! -f "$REDIS_CONFIG" ]]; then
    echo "missing Redis config: $REDIS_CONFIG" >&2
    echo "copy deploy/secrets/redis.conf.example to redis.conf.local and fill it locally" >&2
    exit 1
  fi
  redis_cluster_urls="$(read_config_value cluster_urls)"
  if [[ -n "$redis_cluster_urls" ]]; then
    FARM_REDIS_CLUSTER_URLS="$redis_cluster_urls"
    export FARM_REDIS_CLUSTER_URLS
  else
    redis_host="$(read_config_value host)"
    redis_port="$(read_config_value port)"
    redis_password="REPLACE_WITH_LOCAL_SECRET"
    redis_db="$(read_config_value db)"
    redis_db="${redis_db:-0}"
    if [[ -z "$redis_host" || -z "$redis_port" || -z "$redis_password" ]]; then
      echo "Redis config must contain cluster_urls or host, port and password" >&2
      exit 1
    fi
    encoded_password="REPLACE_WITH_LOCAL_SECRET"$redis_password")"
    FARM_REDIS_URL="redis://:${encoded_password}@${redis_host}:${redis_port}/${redis_db}"
    export FARM_REDIS_URL
  fi
fi

exec docker compose -f "$COMPOSE_FILE" "$@"
