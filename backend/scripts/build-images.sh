#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT_DIR"
GIT_COMMIT=$(git rev-parse HEAD)
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || {
  echo "build-images: refusing to stamp a source revision onto a dirty tree" >&2
  exit 1
}
APP_VERSION=${APP_VERSION:-0.0.0-dev}
# A source commit is factual build identity. An OCI image revision is an
# independently reviewed/published identity and must be supplied explicitly;
# never claim HEAD as that revision by default.
IMAGE_REVISION=${IMAGE_REVISION:-}
docker build --file Dockerfile.runtime --build-arg APP_PROFILE=showcase \
  --build-arg APP_VERSION="$APP_VERSION" --build-arg GIT_COMMIT="$GIT_COMMIT" \
  --build-arg IMAGE_REVISION="$IMAGE_REVISION" --tag farm-game-runtime .
docker build --file Dockerfile.tools --build-arg TOOLSET=load --tag farm-load-tools .
docker build --file Dockerfile.tools --build-arg TOOLSET=fault --tag farm-fault-tools .
node scripts/audit-runtime-images.mjs
