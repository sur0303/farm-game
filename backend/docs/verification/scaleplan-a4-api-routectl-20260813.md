# A4 ScalePlan manual gRPC API and routectl verification

Date: 2026-08-13
Worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-sp3-api-a4-20260813
Branch: codex/scaleplan-sp3-api-a4-20260813
Baseline: 10c8e5e

## Scope

- Added api/proto/scaleplan and generated api/gen/scaleplan.
- Added internal/scaleplan/service.go and package focused tests.
- Added cmd/routectl/scaleplan.go and focused tests; existing main dispatch now recognizes scaleplan.
- Did not modify route-controller composition, core state machine/ports, A1/A2/A3, or deploy.

## Contract and safety

- RPCs: SubmitScalePlan, GetScalePlan, bounded ListRecoverableScalePlans, Pause, Resume, and TerminateBlocked.
- Submit requires an explicit move manifest, idempotency key, and the core +1/-1 replica constraint. The API does not select or balance shards.
- TerminateBlocked binds plan ID, revision, blocked reason, payload digest, exact checkpoint digest, and explicit termination ID. Degraded exclusion/barrier blocked reasons are rejected.
- The service requires an exact full-method authorizer, Ready gate for mutations, and exact realm/workload allowlist. The mutation surface has no generic CAS, force unlock, or verification bypass.
- Response views contain only safe plan fields, manifest, and immutable digests. They omit idempotency key, endpoints, workload UID/resourceVersion, proof evidence, and secrets. RPC errors use stable redacted codes/messages.
- routectl mutation requires explicit --confirm-digest and preflights the immutable payload digest. terminate-blocked also requires --termination-id.

## Verification commands

All commands ran in the remote SSH session whose UID is developer_026:

    PATH=/data/home/developer_026/go/bin:/usr/bin:/bin protoc -I api/proto --go_out=paths=source_relative:api/gen --go-grpc_out=paths=source_relative:api/gen scaleplan/scale_plan.proto
    go test ./internal/scaleplan -count=1
    go test ./cmd/routectl -run 'TestRoutectlScalePlan' -count=1
    go test ./cmd/routectl -count=1
    go build -buildvcs=false ./cmd/routectl ./internal/scaleplan
    go vet ./cmd/routectl ./internal/scaleplan
    git diff --check

Expected and observed result: each command exits successfully. Focused tests cover manifest/idempotency, allowlist/authorizer/Ready, bounded list, redacted response, digest confirmation, and exact blocked termination proof.

## Identity and temporary files

The SSH login itself is developer_026 (UID 293370). This non-root session cannot invoke runuser -u developer_026; the system rejects non-root use of runuser. No sudo or other user was used. All files and the final commit are created by developer_026.

Before commit, verify:

    find . -type f \( -name 'zz-test-temp.go' -o -name '*.orig' -o -name '*.rej' -o -name '*.tmp' -o -iname '*cache*' \) -print
    test ! -e cmd/routectl/zz-test-temp.go
    git status --short

The temporary-file scan must be empty, zz-test-temp.go must not exist, and no temporary artifact may be committed.
