# RT2Q S2 production defect: post-ASSIGNING target SnapshotRevision race (2026-08-10)

## Verdict

Proven production defect with live evidence below. Authorized narrow fix is
documented in
`docs/verification/rt2q-s2-target-snapshot-convergence-fix-20260810.md`
(`RouteAssignmentManager` convergence barrier + route-controller composition).

## Live evidence (isolated E3 S2, real production binaries)

Artifact examples:

- `/tmp/rt2q-s2-flake-1` @ `9ddaf62e5e0a234130a17fffac81151e836829b4`
- `/tmp/rt2q-s2-flake-3` @ same tip
- earlier `/tmp/rt2q-s2-smoke-fencefix` @ `8e165886125d2f486e82984c6f8c12b42955054c`

Observed on the common path:

1. HolderControl callbacks: Stop (held) → Stop → Wait → Facts. **No Prepare**.
2. Plan phases: DRAINING → ASSIGNING, then terminal
   `PAUSED` / `error_code=authority_committed_snapshot_pending`.
3. Immediate diagnostics after move return:
   - controller `snapshot_revision=6` (was 5 at preflight)
   - source/target holder records still report `snapshot_revision=5`
   - `authority_active_lease=OK`, `authority_tuple=OK`
   - `snapshot_tuple=FAIL`
   - snapshot route matches neither source ACTIVE nor target ACTIVE endpoint
4. Production contract: re-`Execute` of a PAUSED plan does **not** advance to
   `SUCCEEDED` (immutable audit). S2 oracle requires `SUCCEEDED`.

Intermittent second failure mode under the same tip: move returns
`PENDING`/`attempt=0` before Stop (Execute never entered drain). Separate flake;
the PAUSED path above is the systematic blocker once Stop is held.

## Code seam

`RouteAssignmentManager.drain` after successful ASSIGNING `runTransitionState`:

1. `runTransitionState(..., ASSIGNING)` commits Authority and publishes snapshot
   `GlobalRevision+1`, updating in-memory `m.snapshot`.
2. Immediately afterward, `validatePendingProjectionTarget` is invoked with
   `m.snapshot.GlobalRevision` (the **new** revision).
3. Target validator requires the remote holder registry record
   `SnapshotRevision == snapshotRevision` before `ColdLoadNewHolder` (Prepare).

Remote holders can only learn/apply revision N+1 after the ASSIGNING publish /
watch notify / heartbeat path. The synchronous post-publish validation therefore
fail-closes DrainValidated after Authority is committed, before Prepare, which
PlanExecutor records as `authority_committed_snapshot_pending`.

Relevant call sites:

- `internal/actorrouting/authority/manager.go` (`drain` after ASSIGNING,
  `validatePendingProjectionTargetLocked` passes `m.snapshot.GlobalRevision`)
- `cmd/route-controller/plan_runtime.go` /
  `internal/routeplan/executor.go` target validators enforcing exact
  `SnapshotRevision` match
- `internal/routeplan/executor.go` PAUSED immutability on re-Execute

## Prior authorized production fixes (routectl)

| SHA | Fix |
| --- | --- |
| `62afe403b60c2fd399c7ac1ce9d4e029fdb116a9` | VerifyConnection must not require callback-phase `HandshakeComplete` |
| `6addd51dfa394ea3e0d51ce439440c036101e7ef` | fence flag bindings must share storage with `flag.FlagSet` |

## Harness tip

`9ddaf62e5e0a234130a17fffac81151e836829b4` — S2 Stop-hold / phase-poll isolation
and PAUSED diagnostics only. Does not weaken S2 readiness.
