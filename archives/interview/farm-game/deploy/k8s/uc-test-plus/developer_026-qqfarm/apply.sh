#!/usr/bin/env bash
# developer_026-qqfarm is the UC r93 port. Do not apply the directory blindly:
# 10-redis is a StatefulSet cluster, and bootstrap must stay ordered.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$ROOT/apply-r93.sh"
