#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
OUTPUT="$ROOT_DIR/deploy/ws-driver/ws-driver"
IMAGE="${WS_DRIVER_IMAGE:-farm-game-ws-driver:local}"

cd "$ROOT_DIR"
CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
  -o "$OUTPUT" ./cmd/ws-driver
docker build -t "$IMAGE" "$ROOT_DIR/deploy/ws-driver"

echo "built $IMAGE from the host-compiled ws-driver binary"
