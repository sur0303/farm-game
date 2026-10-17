# A4 control-token propagation verification

## Delivered seam

- `routeplan.ControlMutationOptions` is the additive composition seam. A nil coordinator is the G21/off path; a configured coordinator acquires one issuer-bound token for a top-level operation or verifies the token inherited in context.
- `routeplan.PlanExecutor` uses distinct deterministic operation IDs for submit, execute, and recovery. `Recover`, `Submit`, and `Execute` pass the bound context through `ExecutionGuard`; nested calls verify the same token instead of acquiring again.
- `authority.RouteAssignmentManager` verifies the inherited token before transition, projection, recovery, renewal, and placement-release writes, and re-verifies immediately before snapshot publication. The publisher receives the exact token.
- `leaseProjectionRuntime` labels startup as `BOOTSTRAP` and periodic waves as `PROJECTION`; `pendingProjectionRecoveryOwner` labels recovery as `RECOVERY`.
- `newPlanRuntime` remains the zero-option G21 wrapper. `newPlanRuntimeWithControl` is the manual-composition seam: it acquires a `BOOTSTRAP` token, gates the durable snapshot publisher, and injects the same options into Manager, executor, projection, and recovery owner.

## Fail-closed cases

- Manual composition without an issued token returns `ErrControlTokenRequired` before Authority mutation.
- Foreign or expired tokens are rejected by coordinator verification; context presence is never treated as authorization.
- A zero token remains accepted only by the ungated/off publisher path. `RequireControlToken` rejects zero-token Bootstrap/publication.
- An unknown durable effect does not resolve the coordinator operation; the gate remains fail-closed until recovery or lease expiry.

## Verification

Focused checks:

```text
go test -count=1 ./internal/routeplan ./internal/actorrouting/authority ./cmd/route-controller
go test -race -count=1 ./internal/routeplan ./internal/actorrouting/authority ./cmd/route-controller
git diff --check
```

The focused tests cover off no-op, manual token required, exact nested-token inheritance, Bootstrap/Recovery operation kinds, foreign/expired rejection, Manager publication propagation, and RoutePlan recovery acquisition.

A repository-wide `go test -count=1 ./...` also reached the affected packages successfully, but remains non-green on the pre-existing, out-of-scope `cmd/rt2q-route-control-harness/runner.go:3580` and `:3649` calls to `LookupTransitionRecovery` (missing the repository's current context argument). That file is outside A4's owned write set and was not changed.


## Manual composition requirement

`cmd/route-controller/main.go` is intentionally outside A4's owned write set and remains the existing off composition. Before enabling `mode=manual`, the composition root must call `scaleplan.BindCoordinatorToMode(scaleplan.RuntimeModeManual, coordinator)`, provide a valid `MutationScope` and `MutationLeaseOwner`, and call `newPlanRuntimeWithControl` with those options. A direct publisher Bootstrap call elsewhere must likewise be made inside an acquired, coordinator-verified Bootstrap token.
