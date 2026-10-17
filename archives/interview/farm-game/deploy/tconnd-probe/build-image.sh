#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
OUTPUT="$ROOT_DIR/deploy/tconnd-probe/tconnd-probe"
IMAGE="${TCONND_PROBE_IMAGE:-farm-game-tconnd-probe:local}"

cd "$ROOT_DIR"
CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
  -o "$OUTPUT" ./cmd/tconnd-probe
docker build -t "$IMAGE" "$ROOT_DIR/deploy/tconnd-probe"

echo "built $IMAGE from the host-compiled tconnd-probe binary"
