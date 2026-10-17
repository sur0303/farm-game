# CP-Q1-SCOPE-FILTER-CORRECTNESS — Premerge Gate

Result: `PASS`.

## Exact composition

- Backend integration base: `4c6195c8141bdc3368ecda3922e4f9f3252d0046`
- Backend Premerge code head: `bf0c6063201051caf08be5da4c062c7bd5189ffa`
- H5 integration base: `fdb9849c0aad613b9a1c45ab8bf33a692c5ee967`
- H5 Premerge head: `c1bc17bcad17b88688ae7c8fae77d384f21d5fbb`

The Backend Premerge contains reviewed Backend `9ee567c...` and Q1 Handoff `113332a...`. The H5 Premerge independently contains reviewed H5 `48ea2db...`.

## Gates

Passed:

- full Backend `go test ./... -count=1`
- focused race tests
- `go vet ./...`
- `go build ./cmd/...`
- `git diff --check`
- legacy CARE Due and accepted/rejected corruption controls 10×
- Harness negative controls
- H5 204 unit tests, TypeScript, Vite build, Playwright contract discovery
- Browser + Go Bot + Authority + F1/F2/F3 combination run

Combination run `q1perf-premerge2-0809144959`:

- Product scenario `PASS`
- `F3_CHAIN_PASS`
- InvalidOrder observations `0`
- physical/unmatched Pair Due residual `0/0`
- Wire THEFT `0 → 1`
- `CareCompleted=2`
- hard correctness incidents `0`
- exact ephemeral Redis cleanup complete

The diagnostic profile remains `baseline_eligible=false`; no performance/capacity conclusion is recorded.

The first environment attempt `q1perf-premerge-0809144851` is explicitly invalid: a shared Vite dependency cache contained root-owned files and prevented the Browser server from starting. It is not counted as a product Gate. The rerun used an isolated user-owned dependency cache and passed.

Machine evidence and hashes: [`q1-scope-filter-premerge-gate-20260809.json`](q1-scope-filter-premerge-gate-20260809.json).
