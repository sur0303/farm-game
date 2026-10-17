#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

go vet ./...
go test ./...
go test -race ./internal/...
go build ./...

H5_DIR="$ROOT_DIR/../classic-farm-h5"
if [[ -f "$H5_DIR/package.json" ]]; then
  (
    cd "$H5_DIR"
    npm test -- --run
    npm run build:staging
    npm run build:web
    if [[ "${FARM_VERIFY_WEB_E2E:-0}" == "1" ]]; then
      npm run e2e:web
    fi
    if [[ "${FARM_VERIFY_WEB_FAULT_E2E:-0}" == "1" ]]; then
      FARM_WORKFLOW_E2E=1 npm run e2e:web -- --grep "persistent Redis restores" --workers=1
    fi
  )
fi

echo "S2 local verification passed (including staging+web builds)."
echo "Set FARM_VERIFY_WEB_E2E=1 with a running backend for Web account E2E."
echo "Set FARM_VERIFY_WEB_FAULT_E2E=1 only for the destructive restart/recovery Web check."
echo "External TCONND/Tcaplus probe status remains in docs/verification/."
