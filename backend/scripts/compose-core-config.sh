#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${FARM_REDIS_URL:?Set FARM_REDIS_URL to the DevCloud Redis URL before validating core Compose}"
: "${PERMIT_SECRET:?Set PERMIT_SECRET to at least 32 random bytes before validating core Compose}"
docker compose -f deploy/docker-compose.yml config >/dev/null
echo "core compose configuration is valid"
