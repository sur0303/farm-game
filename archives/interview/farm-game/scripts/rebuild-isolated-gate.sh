#!/usr/bin/env bash
set -euo pipefail

project="${1:-foe2eshadow}"
shift || true
namespace_override="${ISOLATED_REDIS_NAMESPACE_OVERRIDE:-}"
services=("$@")
if [[ ${#services[@]} -eq 0 ]]; then
  services=(farm-gate)
fi
root_dir="$(cd "$(dirname "$0")/.." && pwd)"

cd "$root_dir"
mkdir -p .local-build /tmp/farmgame-go-cache
for service in "${services[@]}"; do
  GOCACHE=/tmp/farmgame-go-cache CGO_ENABLED=0 go build -buildvcs=false -trimpath \
    -o ".local-build/$service" "./cmd/$service"
done

for source_service in player-svr farm-svr farm-worker farm-gate; do
  source_container="${project}-${source_service}-1"
  while IFS= read -r -d '' variable; do
    case "$variable" in
      HOME=*|PATH=*|HOSTNAME=*|PWD=*|SHLVL=*|_*=*) continue ;;
    esac
    export "$variable"
  done < <(docker inspect "$source_container" | python3 -c '
import json, sys
values = json.load(sys.stdin)[0]["Config"]["Env"]
sys.stdout.buffer.write(("\0".join(values) + "\0").encode())
')
done

if [[ -n "$namespace_override" ]]; then
  export REDIS_NAMESPACE_ENVIRONMENT="$namespace_override"
fi

export FARM_REDIS_CONFIG="${FARM_REDIS_CONFIG:-/data/home/developer_026/CodeBuddy/20260519141303/farm-game/deploy/secrets/redis.conf.local}"
./scripts/company-redis-compose.sh -p "$project" up -d --build --no-deps "${services[@]}"

for service in "${services[@]}"; do
  container="${project}-${service}-1"
  healthy=false
  for _ in $(seq 1 30); do
    state="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
    if [[ "$state" == "healthy" || "$state" == "running" ]]; then
      healthy=true
      break
    fi
    sleep 1
  done
  if [[ "$healthy" != "true" ]]; then
    docker logs --tail 80 "$container" >&2
    exit 1
  fi
done
