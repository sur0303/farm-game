# Q1-PERF-BASELINE-V1

Q1 is the harness intended to record a repeatable pre-hardening performance baseline. The original product correctness blocker is now closed by a candidate fix and passed the diagnostic product anchor 10/10, but the performance baseline is still **not recorded** because the eligible First Run has not been rerun. It does **not** claim that Farm Game supports 30 million DAU.

## Frozen source

- Backend production baseline: `260babcd93cfb6a4a58a481cc3c7f8c1f95e455a`
- H5 baseline: `df379b5fcc5eb710fc1efa5406c752ba18c3ab30`
- Backend worktree: `farm-game-q1-perf-baseline-v1`, branch `codex/q1-perf-baseline-v1`
- H5 worktree: `classic-farm-h5-q1-perf-baseline-v1`, branch `codex/q1-perf-baseline-v1`

The `cp-mvp-before-hardening-20260809` refs were observed changing while the worktrees were created. Q1 therefore verifies and records the two exact commits supplied above rather than silently following the moving ref. Q1 does not move or recreate that tag.

The backend Q1 branch may contain commits only in the Q1 write set. `baseline-run-manifest.json` records both `backend_commit` and `backend_checkout_commit`, then lists every committed path between them. Manifest generation fails if any path is outside:

- `cmd/perf-baseline/**`
- `tools/perf/**`
- `scripts/perf/**`
- `deploy/load/**`
- `docs/verification/perf/**`

The H5 checkout must remain exactly at its frozen commit; Q1 writes no H5 business source.

## Test architecture

```text
Scenario Coordinator (scripts/perf/run-baseline)
├── existing Go Protocol Bots (ws-stress / authority-smoke-bot)
├── 2 real Playwright H5 contexts (CP-B Hero witness)
├── existing Fault Controller (Gate restart/result loss/downlink failure)
├── Q1 test-only H5 Wire Tap + Fault Plan read-back witness
├── Q1 read-only ephemeral-Redis Pair/Target diagnostic dump
├── existing Authority Probe (authority-smoke-fixture / theft-hot-audit)
├── existing Client State Oracle (FarmVersion continuity in Theft bot)
└── Evidence Reporter (tools/perf/q1perf.py)
```

Playwright is a correctness witness, not the main load generator. Gate and mixed pressure use real HTTPS login, WSS, ClientHello, typed Protobuf commands, Snapshot, and Ready barriers through the existing Go bot.

## Profiles

1. **Product correctness anchor**: two-browser Hero, Task Claim, F1/F2/F3, protocol bot, and Authority audit.
2. **Gate connection staircase**: fresh isolated stack per stage, fixed open-arrival connection schedule, Ready rate, CPU/RSS/goroutine/fd/GC/network evidence.
3. **Hot farm**: existing 200-WSS Theft baseline and slow-reader cases, terminal outcomes, conservation audit, and per-client FarmVersion oracle.
4. **Mixed business staircase**: concurrent open-arrival Join connections and scheduled `cmd/perf-baseline` journeys (a write-set-isolated fork of the proven authority-smoke protocol state machine: Register/Login/Join/Bootstrap/Buy/Plant). SELL/UNLOCK/WATER/THEFT/CLAIM_TASK/Friend paths remain covered by correctness/hot-farm anchors, not yet by the open-arrival mixer; this is reported as an observability/tooling gap.

The full plan (`deploy/load/q1-perf-baseline-v1.json`) freezes connection stages 200/500/1,000/2,000/5,000. The conservative first run stops at 500 and uses `q1-perf-baseline-v1-first-run.json`. Tooling-only profiles are available as `q1-perf-baseline-v1-smoke.json` and `q1-perf-baseline-v1-f3-triage.json`; neither may be published as Baseline V1.

The frozen B0 H5 implements an explicit two-click Theft confirmation while its old committed CP-B test assumes one click. Runs against that exact B0 therefore create a hash-recorded test-only overlay under ignored `test-results/`. The correctness-fix H5 candidate permanently records the contract in the tracked CP-B test: first click proves confirmation state and Wire `THEFT=0`; second click proves Wire `THEFT=1`. No H5 product interaction code is changed.

## Run

Prepare H5 dependencies before the measured window. Do not install or update dependencies during a comparison run.

```bash
cd farm-game-q1-perf-baseline-v1

scripts/perf/run-baseline \
  --config deploy/load/q1-perf-baseline-v1-first-run.json \
  --backend-root "$PWD" \
  --backend-commit 260babcd93cfb6a4a58a481cc3c7f8c1f95e455a \
  --h5-root ../classic-farm-h5-q1-perf-baseline-v1 \
  --h5-commit df379b5fcc5eb710fc1efa5406c752ba18c3ab30
```

Replay a candidate with the same config and load-tool checkout:

```bash
scripts/perf/run-baseline \
  --run-kind candidate \
  --backend-root /path/to/candidate-worktree \
  --backend-commit <candidate-commit> \
  --h5-root ../classic-farm-h5-q1-perf-baseline-v1 \
  --h5-commit df379b5fcc5eb710fc1efa5406c752ba18c3ab30 \
  --config deploy/load/q1-perf-baseline-v1-first-run.json \
  --baseline-metrics /path/to/baseline-metrics.json
```

## Required outputs

Every completed run directory contains:

- `baseline-run-manifest.json`
- `baseline-verdict.json`
- `baseline-metrics.json`
- `observability-gaps.json`
- `performance-baseline-report.md`
- `scenarios/*.json`
- `evidence/**` raw driver, sampler, Prometheus, process, Redis, browser, and oracle evidence

The manifest records backend/H5/load commits, machine specification and hash, config hash, Redis endpoint hash and namespace, test window, planned load, and actual injected load.

## Verdicts

Machine classification is `PASS | FAIL | INCONCLUSIVE`; the domain verdict is:

- `BASELINE_RECORDED`: configured baseline completed, load reached plan, and correctness violations are zero.
- `PERF_REGRESSION_PASS`: a candidate passed correctness and comparable metrics did not exceed the frozen regression threshold.
- `FAIL_CORRECTNESS`: any duplicate effect, FarmVersion gap/regression, workflow/due residue, conservation failure, receipt mismatch, or correctness-anchor failure.
- `INCONCLUSIVE_ENVIRONMENT`: incomplete profile, missing evidence, load-generator saturation, or actual load below plan.

A faster run never overrides `FAIL_CORRECTNESS`. Correlated machine signals are grouped under one `incident_id`; for example, two persistent Source Orders from one Care Recovery Scope defect are two signals/residues but one hard correctness incident.

The original blocker and root-cause classification are documented in `q1-f3-invalid-order-followup-20260809.md`. The candidate closure is retained in `scope-filter-fix-10x-20260809/`: F3 is `F3_CHAIN_PASS` 10/10, CARE Pair orders converge, persistent Pair DueIndex residue is zero, and `InvalidOrders=0`. Independent Focused Review closed at `P0=0`, `P1=0`, exact-head closure `PASS`; Backend and H5 Premerge Gates passed and were merged. Postmerge qualification is retained in `q1-postmerge-qualification-20260809/` and passed 1/1. The ordered capacity train is frozen in `q1-postmerge-capacity-train-plan-20260809.md`. No formal performance baseline has yet been recorded.

## Redis and environment safety

- The coordinator accepts only ephemeral loopback Redis for this first implementation.
- Each stage gets a unique namespace and a fresh exact container.
- Redis credentials are never written to evidence; endpoint identity is stored only as SHA-256 of `host:port/db`.
- The runner never performs database-wide deletion. Cleanup removes only the exact ephemeral containers it created.
- The final manifest marks cleanup complete only after container removal is observed.
- Shared company Redis requires a separate reviewed adapter with exact namespace-only deletion and first-wave pressure limits; it is intentionally disabled here.

## Known gaps

Until A3 data arrives, reports must retain:

```text
actor_record_bytes = PENDING_OBSERVABILITY
commit_bytes       = PENDING_OBSERVABILITY
```

The current `ws-stress` output gives exact planned/attempted counts and a fixed ticker arrival rate, but not each event's scheduling lag. `open_arrival_lag_ms` is therefore reported as pending rather than guessed. ConnectionWriter queue depth and exact Redis SessionRoute/SubscriptionLease QPS attribution are also pending.
