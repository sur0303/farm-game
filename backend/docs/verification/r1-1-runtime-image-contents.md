# R1-1 runtime/tool image contents

Frozen Backend input: `ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c`.

The canonical image split is declared by [`r1-1-image-manifest.json`](../../deploy/images/r1-1-image-manifest.json) and built by [`scripts/build-images.sh`](../../scripts/build-images.sh):

| Image | Binaries | Boundary |
| --- | --- | --- |
| `farm-game-runtime` | `farm-gate`, `farm-svr`, `farm-worker`, `player-svr` | Runtime only; no smoke/fault/load/fixture binary |
| `farm-load-tools` | `lease-stress`, `ws-driver`, `ws-stress` | Load/stress tools only |
| `farm-fault-tools` | `authority-fault-bot`, `authority-smoke-bot`, `authority-smoke-fixture`, `gate-vertical-e2e`, `q2r2b2-smoke` | Fault/smoke/fixture tools only |

`node scripts/audit-runtime-images.mjs` passed. The runtime Dockerfile has an explicit allow-list of the four runtime services and the static audit verifies that its forbidden tool list is disjoint from the runtime binaries.

The real Docker build was attempted with `scripts/build-images.sh` and failed before building because this environment cannot access `/var/run/docker.sock` (`permission denied`). No Docker image or production-container verification is claimed.
