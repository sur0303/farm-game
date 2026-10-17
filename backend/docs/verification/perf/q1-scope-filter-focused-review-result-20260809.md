# CP-Q1-SCOPE-FILTER-CORRECTNESS — Independent Focused Review Result

## Final result

- P0: `0`
- P1: `0`
- P2: `0`
- Exact-head closure: `PASS`

Reviewed exact heads:

- Backend: `9ee567cdcddfbd3ab9f9531786ef61c2cdf97f5f`
- H5: `48ea2dbb52ed50902e15372a5d07721e46d333ca`
- Q1 Harness code: `dae92dd513872b4247fcb17311b950c48c46b5e9`

## Review loop

The first independent review returned `P0=0, P1=2`:

1. require an explicit authoritative proof for rejected-before-Work CARE result completion;
2. detect physical/orphan Pair DueIndex members, not only Due members joined to readable source orders.

The original Writer made narrow fixes in the original worktrees and branches. The second review confirmed the physical Due gate but found one remaining accepted/rejected ambiguity when `dispatch_order_id` alone was corrupted. The Writer then added an exact Dispatch-anchor existence check and fail-closed test. The final independent re-review found no P0/P1/P2 issues.

## Verified closure

- Accepted CARE Result with missing `dispatch_order_id` cannot be synthesized as rejected when its exact Dispatch anchor exists; it fails closed, remains Pending/Due, is not mutated, and does not inspect Farm.
- Genuine rejected-before-Work CARE validates the frozen Pair rejection shape, uses a Pair-specific proof, and performs no Farm or Theft lookup.
- Accepted CARE with a missing Farm receipt remains unchanged and Due.
- Generic non-Action Pair Work is `NotOwned`/`NonTheft`, not InvalidOrder.
- Legacy CARE Due routes once to CARE without Theft decode, retry growth, or persisted error.
- Physical orphan DueIndex residue is classified `PERSISTENT_PAIR_DUE_RESIDUE` and blocks Harness ingestion.
- H5 proves first-click Wire THEFT `0`, second-click Wire THEFT `1`.
- Q1 changes remain within the declared Q1 allowlist; no Proto, Actor, Gate, production Lua, or Lease semantic changes were introduced.

Focused tests, modified-package race tests, full-suite rerun, Harness tests, H5 tests/TypeScript/build, and exact ancestry/worktree checks passed. The known unrelated full Friend race flake remains explicitly outside the green claim.

`EXACT_HEAD_CLOSURE=PASS P0=0 P1=0`
