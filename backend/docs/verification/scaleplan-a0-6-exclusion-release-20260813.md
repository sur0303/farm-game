# ScalePlan A0.6 exclusion-release seam verification

## Scope

This amendment is limited to the ScalePlan core seam and its deterministic test fakes. It does not implement Redis, routecontrol, A3, routeplan, actor routing, commands, or deployment changes.

The ScalePlan port now carries an exact owner/revision release request:

- `RELEASED`, `ALREADY_RELEASED`, and `BOOT_ROLLOVER` are terminal-safe outcomes.
- `NOT_FOUND`, `OWNER_MISMATCH`, `REVISION_CONFLICT`, `MALFORMED`, and `UNKNOWN` are fail-closed.
- Successful scale-in does not invoke release.
- A pre-scale failure persists `RELEASING_EXCLUSION` and `CleanupIntent`; the exclusion is released only after exact workload evidence proves `NOT_COMMITTED`.
- Unknown scale evidence leaves the plan blocked and the exclusion retained.
- Blocked safe termination may use the optional blocked-release capability only while holding the EXCLUSIVE quiescence fence. It re-captures and verifies a manifest with no active exclusion before committing the terminal receipt.

## Deterministic verification

Run:

~~~
go test -buildvcs=false ./internal/scaleplan ./internal/testkit/scaleplan -count=1
~~~

The focused cases cover:

- closed release-outcome terminality;
- successful scale-in with zero release calls;
- failure before `/scale` with release before terminal failure;
- boot-rollover release acceptance;
- unknown scale evidence blocking before release, followed by restart-safe reconciliation;
- blocked safe termination release/replay;
- blocked safe termination fail-closed for `NOT_FOUND` and `UNKNOWN`.

## Public seam

`scaleplan.PlacementExclusionPort` now requires `ReleaseForOwner(context.Context, HolderRef, PlacementExclusionRelease)`. The blocked termination path keeps `BlockedQuiescencePort` source-compatible and uses the optional `BlockedExclusionReleasePort` only for the controlled release operation.
