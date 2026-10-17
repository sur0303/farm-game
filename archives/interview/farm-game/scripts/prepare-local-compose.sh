#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR="$ROOT_DIR/.local-build"
BUILD_CACHE="${FARM_GO_BUILD_CACHE:-/tmp/farmgame-go-cache}"

mkdir -p "$BUILD_DIR" "$BUILD_CACHE"

for service in player-svr farm-svr farm-gate farm-worker; do
  echo "building $service on the host"
  (
    cd "$ROOT_DIR"
    GOCACHE="$BUILD_CACHE" CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
      -o "$BUILD_DIR/$service" "./cmd/$service"
  )
done

echo "local Compose binaries are ready in $BUILD_DIR"
