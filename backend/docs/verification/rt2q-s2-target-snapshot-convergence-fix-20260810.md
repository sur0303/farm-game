# RT2Q S2 fix: post-ASSIGNING target SnapshotRevision convergence (2026-08-10)

## Defect (proven)

After Drain Stop→Wait→Facts, `DrainValidated` commits Authority and publishes a
complete ASSIGNING revision N, then synchronously required
`HolderRegistry.SnapshotRevision == N` before ColdLoad/Prepare. Authenticated
holder heartbeats had not yet observed N, so Drain fail-closed with recoverable
`authority_committed_snapshot_pending` / Plan `PAUSED` and never reached
Prepare. Re-Execute does not advance a PAUSED plan to `SUCCEEDED`.

Evidence: `docs/verification/rt2q-s2-authority-committed-snapshot-pending-20260810.md`.

## Fix (narrow)

`RouteAssignmentManager` adds a bounded, cancellation-safe convergence barrier
after ASSIGNING publication and before ColdLoad/Prepare:

- Revalidate through the existing production
  `PendingProjectionTargetValidator` only (exact holder/boot/capacity/lease/
  fence/`SnapshotRevision==N`).
- Do not hold Manager/global locks across waits.
- Do not repeat Authority / Stop / Wait / Facts.
- Deterministic `After` / backoff / `Now` seams; zero timeout keeps single-attempt
  legacy/test behavior.
- Timeout/cancel returns the last fail-closed validation error (joined with
  `ctx.Err` when canceled) so Plan recovery keeps
  authority-committed/snapshot-pending semantics and does not mutate Authority
  twice.

Production composition (`cmd/route-controller/plan_runtime.go`) sets
`TargetConvergenceTimeout=15s` and `TargetConvergenceBackoff=50ms` so the
authenticated HolderRegistry heartbeat path can observe N without fabricating
readiness.

## Non-goals

No Proto, Authority store/Lua, Redis schema, Holder, TLS, public API, dependency,
integration, or second state machine change. Exact snapshot equality is not
weakened to proxy receipt or local guesses.

## Focused tests

- `TestRouteAssignmentManagerDrainWaitsForPostAssigningTargetSnapshotConvergence`
- `TestRouteAssignmentManagerDrainConvergenceTimeoutKeepsAssigningWithoutRepeatingAuthority`
- `TestRouteAssignmentManagerDrainConvergenceCancelPreservesRecoverableAssigning`
- Existing post-Authority fail-closed / retry path (timeout=0 single attempt)

## Live follow-up

Signed fix SHA: `3c899900aa770a318102cedaeac35e87d264a0aa`.

Isolated E3 S2 runs at that tip reach PREPARING/Prepare with
`SnapshotRevision==N` on holders, then fail on a distinct Prepare
FailedPrecondition seam — see
`docs/verification/rt2q-s2-prepare-failed-precondition-20260810.md`.
