# Route-controller A4 ScalePlan composition verification

Date: 2026-08-13
Worktree: farm-game-scaleplan-sp3-composition-a4-20260813

## Configuration contract

- SCALEPLAN_MODE is optional and defaults to off.
- Accepted values are exactly off and manual; an empty or unknown value fails closed.
- manual requires ROUTE_CONTROLLER_REALM, controller identity/boot, and one namespace containing the allowlisted StatefulSet names from SCALEPLAN_PLAYER_NAMESPACE, SCALEPLAN_PLAYER_STATEFULSET, SCALEPLAN_FARM_NAMESPACE, and SCALEPLAN_FARM_STATEFULSET.
- Only the player and farm domain references are accepted. The workload adapter rejects every other WorkloadRef before reaching Kubernetes.

## Composition and lifecycle

off calls BindCoordinatorToMode(off, nil) and returns no scaleplanRuntime. It does not call any ScalePlan backend, coordinator, Kubernetes-client, adapter, registration, or close hook. The existing G21 route-controller path remains responsible for its own Redis client, listeners, readiness, and recovery.

manual composes the A3 coordinator, A1 durable Redis backend, the Kubernetes client from rest.InClusterConfig, the A2 allowlisted workload scaler, and A3 holder/placement/RoutePlan/quiescence adapters. The token propagation and placement-exclusion release seams are constructor hooks; their default hooks return an error rather than emulating an unsafe implementation.

The route-controller startup sequence is:

1. pending projection/lease recovery;
2. RoutePlan recovery;
3. ScalePlan Executor.Startup;
4. existing background recovery loops;
5. route-controller readiness and ScalePlan Ready.

The registration hook receives a scaleplanRuntime containing Executor, Repository, and Ready. A future API service must use Ready/RequireReady to reject mutations before startup completion. Close clears readiness and invokes its close hook exactly once; the composition owns no independent worker goroutine.

## Verification commands

Run as developer_026 in the target worktree:

    go test ./cmd/route-controller ./internal/scaleplan/... ./internal/scaleplan/adapters/control ./internal/platform/kubernetes/scaleplan ./internal/storage/redis/scaleplan -count=1
    go build -buildvcs=false ./cmd/route-controller
    git diff --check

The focused composition tests cover default/strict mode parsing, off zero-side-effect constructor spies, manual construction order, RoutePlan-before-ScalePlan startup, readiness gating, idempotent close, fail-closed missing hooks, and StatefulSet allowlist enforcement.
