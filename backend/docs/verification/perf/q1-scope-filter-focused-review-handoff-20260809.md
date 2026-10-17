# CP-Q1-SCOPE-FILTER-CORRECTNESS — Focused Review Handoff

## Decision state

`READY_FOR_INTEGRATION` after independent Focused Review returned `P0=0`, `P1=0`, `P2=0`, `exact-head closure=PASS`.

No branch has yet been pushed, no Premerge has been created, and neither Backend nor H5 integration has been merged. The controller/I0 must now perform the push/Premerge/combination/merge sequence. Review evidence: [`q1-scope-filter-focused-review-result-20260809.md`](q1-scope-filter-focused-review-result-20260809.md).

Machine state: [`q1-scope-filter-focused-review-status-20260809.json`](q1-scope-filter-focused-review-status-20260809.json).

## Exact heads for review

| Repository item | Exact commit |
|---|---|
| Backend initial production fix | `6c15a9325a77a457bd5aa8adec87e35138c98793` |
| Backend final reviewed head | `9ee567cdcddfbd3ab9f9531786ef61c2cdf97f5f` |
| Backend local integration snapshot from which the fix branch was created | `dd7104a3c43cecdc85c8d7962014f9760bf28f2d` |
| H5 permanent two-click witness | `48ea2dbb52ed50902e15372a5d07721e46d333ca` |
| H5 local integration snapshot from which the branch was created | `fdb9849c0aad613b9a1c45ab8bf33a692c5ee967` |
| Q1 Harness final reviewed code head | `dae92dd513872b4247fcb17311b950c48c46b5e9` |
| Q1 retained 10/10 evidence commit | `fc298561da0b109dc402e890f735c6a445f6c353` |

The local integration snapshots above are not asserted to be the future controller's latest remote integration. The controller must fetch and create each Premerge from the then-current exact integration head.

## Narrow gate 1: legacy CARE Due upgrade

Test: `TestLegacyCareDueUpgradeRoutesOnceToCareAndDoesNotSpinLeaseRetryOrLastError`

The test commits both CARE Pair source orders before constructing the new scope router, reproducing the persisted upgrade shape. It proves 10/10:

- no Theft target decode, Theft advancer, or Theft result processor call;
- `InvalidOrders=0`, `Retried=0`, no persisted `last_error_class`;
- the CARE owner acquires exactly one dispatch lease;
- Result Order never acquires a Theft WorkLease;
- both orders converge and leave DueIndex;
- five later scans do not reacquire Lease or alter retry/version/state.

## Narrow gate 2: Harness negative controls

Tests:

- `test_invalid_analyzer_detects_each_negative_control`
- `test_invalid_order_and_due_negative_controls_fail_then_clean_control_passes`

Proven behavior:

| Injection | Classification | Ingest exit |
|---|---|---:|
| `InvalidOrders=1`, no remaining Due | `INVALID_ORDER_PROCESSING_WITHOUT_PERSISTENT_RESIDUE` | `1` |
| Persistent Pair Due `=1`, InvalidOrders `=0` | `PERSISTENT_PAIR_DUE_RESIDUE` | `1` |
| Restore both to zero | `NO_INVALID_PAIR_DUE_RESIDUE` | `0` |

Any nonzero InvalidOrder processing observation or persistent Pair Due residue now creates one hard correctness incident and blocks the baseline.

## Positive combined gate after restoring clean state

Run: `q1perf-reviewgate-0809134904`

- Backend: `56ceca6f7cfc8582438a899abe5aa7a90c1d4798`
- H5: `48ea2dbb52ed50902e15372a5d07721e46d333ca`
- Harness: `1d077ec98d4a1901faab89dc16212c925ace3909`
- Browser + Go Bot + Authority: `PASS`
- F1/F2/F3: `PASS`; F3 classification `F3_CHAIN_PASS`
- InvalidOrder observations: `0`
- persistent Pair Due: `0`
- first/second click Wire THEFT: `0 → 1`
- hard correctness incidents: `0`

The profile remains diagnostic with `baseline_eligible=false`; the coordinator's final `INCONCLUSIVE_ENVIRONMENT / NOT_RECORDED` is intentional and is not a performance result.

## Mechanical gates

Passed:

- `go test ./... -count=1`
- focused recovery tests `-count=10`
- `go vet ./...`
- `go build ./cmd/...` with `GOFLAGS=-buildvcs=false` for this linked worktree
- `git diff --check`
- race: modified Worker packages, Theft online package, and focused Friend recovery tests
- H5 204 unit tests, `tsc`, Web build, Playwright test discovery
- Q1 Harness 16 unit tests and shell syntax checks

Observed but not attributed to this change: one parallel gate attempt hit existing concurrency flakes. A standalone full suite passed; `TestTheftAdmissionTwoServicesRefreshConcurrentPairRecordVersion` then passed 20/20. Full `internal/friend` race remains flaky at `TestConcurrentAddBothDirectionsAndDelete` and is explicitly not claimed green. The focused modified recovery paths are race-green.

## Controller/I0 next actions

1. Push the three fixed branches without rewriting their reviewed code heads.
2. Fetch and record the then-latest Backend and H5 integration commits.
3. Create Backend `premerge/q1-scope-filter-closure` from latest Backend integration; merge Backend and Q1 Harness heads there.
4. Create a separate H5 Premerge from latest H5 integration; merge only the H5 witness head.
5. Run the complete combination Gate on both exact Premerge heads.
6. Merge the approved Premerges into their respective integration branches.
7. Record the exact post-merge Backend/H5 integration heads and run Postmerge Qualification before any eligible formal baseline.

Do not rename the retained 10/10 diagnostic evidence as a formal baseline, and do not rebase the frozen RT2D Review head solely for this closure.
