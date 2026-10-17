#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
OUTPUT="$ROOT_DIR/deploy/tconnd-responder/tconnd-responder"
IMAGE="${TCONND_RESPONDER_IMAGE:-farm-game-tconnd-responder:local}"

cd "$ROOT_DIR"
CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
  -o "$OUTPUT" ./cmd/tconnd-responder
docker build -t "$IMAGE" "$ROOT_DIR/deploy/tconnd-responder"

echo "built $IMAGE from the host-compiled tconnd-responder binary"
