# CAP-SINGLE-POD-L2-R2 Contract v0.1

## 1. Purpose and claim boundary

This train measures the pre-B2 R1 safe operating point of one `farm-gate`, `player-svr`, `farm-svr`, and `farm-worker` process at a time. It is the single-Pod control for later R1→R2 comparison.

It does **not** establish mixed-system capacity, multi-Pod capacity, production SLO, 30M DAU support, or R2 capacity. A passing train may publish `CP-PERF-R1-SINGLE-POD-BASELINE`; it keeps `baseline_eligible=false`, `capacity_claim=NONE`, and `supports_30m_dau=false` until later automatic gates.

## 2. Immutable R1 identity

Every Precheck, Smoke, Warmup, Staircase and Formal run must bind exactly:

```text
Backend Evidence Head = d1899ecab8d71248062ffd70eb4e42048e394a38
Backend Business      = 93a66ce76d509d86e8cac8e1d87563d084eba821
Backend Image         = sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7
H5 Commit             = 525c4d324100b17a891f02797c4880fa9a02f70a
H5 Image              = sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd
Model layout          = R1 authority/workflow/friend v1; Model Field V2 disabled
```

“Latest integration”, mutable tags, locally rebuilt production binaries, B2 images, Friendship cutover, legacy roster deletion, Model Field V2, and unreviewed observability patches are forbidden. Evidence-only harness code is bound separately by exact Harness Commit, binary SHA-256, Design Review Head and Harness Review Head; it may not change the production image.

## 3. Parallel-train isolation

Train A owns only:

```text
cmd/cap-single-pod/**
scripts/perf/cap-single-pod/**
deploy/load/cap-single-pod-*
docs/verification/perf/cap-single-pod/**
test-only metrics collector
test-only oracle
```

Train A does not write product Handler, Actor semantics, Redis Lua business logic, Workflow state machine, Friendship rules, H5 business code, or Route Runtime. If a required metric touches a file owned by B2, the B2 Writer supplies it; L2 only validates it.

No B2 candidate may enter the R1 runtime or fixture. B2 remains free to code in independent worktrees and branches.

## 4. Measured-service boundary

Exactly one service is measured per subtrain:

- `L2-GATE-R1`: one Gate process; Player/Farm/Redis are support dependencies.
- `L2-PLAYER-R1`: one Player process; Gate/Farm/Redis are support dependencies or test-only protocol endpoints as frozen by the profile.
- `L2-FARM-R1`: one Farm process; Gate/Player/Worker/Redis are support dependencies.
- `L2-WORKER-R1`: one Worker process; Player/Farm/Redis are support dependencies.

Support topologies and injection paths are frozen per service in `profile.json`; they cannot change between tiers. Support dependencies use fixed resource limits and must retain headroom. If Redis or a support service reaches its knee first, the run is `INCONCLUSIVE_DEPENDENCY`, not measured-service capacity.

The measured process has exactly `4 CPU`, `4GiB`, `GOMAXPROCS=4`, `GOGC=100`, and `GOMEMLIMIT=3584MiB`. The Load Generator runs in a separate `8 CPU / 8GiB` cgroup. Runtime limits and machine identity are recorded, not inferred from configuration files.

The R1 long-run lease profile is part of the test configuration, not a product change: Shard Authority TTL is 4h with 5m renewal and 15m safety margin. A run starts only when the frozen route snapshot is at most 30m old and remaining Authority lease exceeds phase duration plus 30m. Static route expiry during measurement is forbidden and is not accepted as service capacity.

## 5. Arrival and fixture contract

The primary driver is a Go protocol bot using an open-loop monotonic scheduler. Service slowdown must not reduce planned arrivals. An arrival delayed by more than 50ms is recorded as scheduled-but-unsent; catch-up bursts are forbidden. Every report records planned, launched, scheduled-but-unsent, completed, rejected, timeout, and arrival lag P50/P95/P99/max.

Two to four Playwright pages are correctness witnesses only; they never supply scale load and are charged to the Load Generator cgroup. Fixtures complete before the measurement window. Replay, cold activation, hot Actor, Due backlog, slow reader, and fanout fixtures are separate profiles and may not be silently mixed.

Raw credentials, Redis URLs, operation identities and product errors are not persisted. Identity evidence uses domain-separated hashes; product errors use allowlisted codes.

## 6. Mandatory order

```text
PRECHECK
→ OBSERVABILITY_QUALIFICATION
→ LOAD_GENERATOR_QUALIFICATION
→ SMOKE
→ WARMUP
→ STAIRCASE
→ SAFE-TIER-CANDIDATE
→ FORMAL_1
→ COOLDOWN_1
→ FORMAL_2
→ COOLDOWN_2
→ FORMAL_3
→ INDEPENDENT REVIEW
```

Durations are frozen in `profile.json`: Smoke 5 minutes, Warmup 10 minutes, Staircase ramp 2 minutes plus hold 10 minutes, Formal 20 minutes × 3, and Cooldown 5 minutes. Reviewer changes require a new profile version/hash and restart from Precheck.

No Formal run may reuse a Staircase namespace, process, connection pool, warmed Actor cache, Due backlog, or mutable route lease unless the reviewed fixture explicitly says so. The three Formal rounds use identical identity, profile, safe tier and fixture.

## 7. Load Generator qualification

Before service capacity, the exact driver binary must prove at least 1.5× every profile's frozen maximum initial tier, preferably 2×, with:

```text
actual/planned >= 98%
driver CPU <= 70% sustained
arrival lag P99 <= 50ms
arrival lag slope not growing
socket error slope not growing
event backlog slope not growing
```

Failure is `INCONCLUSIVE_LOAD_GENERATOR`. Any Staircase extension beyond a qualified maximum requires Driver requalification before that tier; a safe candidate may not exceed qualified load ÷1.5. A Driver limit is never published as Gate, Actor, or Worker capacity.

## 8. Service profiles

### 8.1 Gate

Measure safe connections, handshakes/s, frames/s, egress bytes/s, SessionRoute lease entries/s, and Subscription lease entries/s.

Profiles cover Login/Ticket/WSS/ClientHello, Bootstrap/Join/Ready, long Ping/Pong, lease batches, Snapshot/Delta/FarmChange, slow readers, and controlled drain. Connection tiers begin at `200, 500, 1000, 2000, 5000, 10000`, then grow by 1.5× only if the previous tier is valid. Holds cover at least three lease cycles.

Hard gates include zero unexpected expiry, revived stale subscription, FarmVersion gap, silent downlink drop, and sustained ConnectionWriter growth.

### 8.2 Player

Measure safe Player turns/s, active PlayerActors, activations/s, and commit bytes/s.

Profiles are uniform hot Actors, cold activation, one hot Player, and Participant steps. Business coverage is BUY, SELL, CLAIM_TASK, Bootstrap, and Player steps for Plant/Harvest/Care/Theft.

Hard gates include zero duplicate reward/debit, stale claim commit, record version regression, and receipt-conflict leakage.

### 8.3 Farm

Measure safe Farm turns/s, Join/s, active FarmActors, FarmChange fanout/s, and subscriptions per Farm Pod.

Profiles are uniform Farms, cold activation, one hot Farm × 200 users, ten hot Farms × 200 users, slow clients, and multi-Gate distribution. Business coverage is Join, Plant, Harvest, Water, Theft, and FarmChange.

Hard gates include zero FarmVersion gap, duplicate PlotDelta, Theft conservation error, slow-reader global impact, and post-Ready ordering error.

### 8.4 Worker

Measure safe work steps/s, Due claims/s, Due drain/s, and backlog per Worker.

Backlog tiers begin at `100, 1000, 10000` and may extend to `30000, 100000` only while valid. Plant, Harvest, Care, Theft, Friend Action Dispatch, Result Publish, and mixed-family fairness run separately.

Hard gates include zero InvalidOrder, StepUnknown residual, unexpected Due residual, duplicate side effect, wrong-scope dispatch, and sustained oldest-Due growth.

## 9. Safe tier and knee

The knee is the first repeatable tier where latency, queue, CPU, GC, or delivered load deteriorates nonlinearly. The safe tier is the immediately preceding measured stable tier—not the final tier before a crash.

A safe candidate requires actual/planned ≥95%, zero correctness violation, complete observation, non-positive primary queue slope, stable P99, no first dependency/driver bottleneck, no OOM/fd risk, and a successful controlled drain. Queue slope is OLS over 60-second windows with at least 55 samples; it is positive only when the lower 95% confidence bound is above zero and net item growth is at least five. Formal rounds require actual/planned ≥98% and the stricter rules in `verdict-rules.json`.

A saturated Staircase tier is retained and may identify the knee, but it is never deleted, renamed, excluded, or selected as safe.

## 10. Correctness-first stop

Any hard correctness gate immediately stops new arrivals and all remaining tiers. Inflight outcomes are observed, bounded diagnostics are captured, and the original evidence is preserved as `FAIL_CORRECTNESS`. Performance data after the signal is ineligible.

A product bug goes to the owning product Writer for a narrow fix. The fix must enter the R1 capacity branch and all affected B2 branches; the affected service restarts at Smoke. Old evidence remains and is marked superseded, never overwritten.

A performance bottleneck does not authorize a production fix inside L2. First publish the R1 baseline, then run a separate `PERF-*` A/B task with the identical profile.

## 11. Evidence

Every run emits exactly the output files listed in `profile.json`. A Manifest tier records all simultaneous load dimensions as a `targets` array (for example connections plus frames/s, or backlog plus arrivals/s); no secondary target may be kept only in a command line or log. `evidence-hash-index.json` hashes every published attachment and rejects path traversal, missing files, duplicate paths and post-index mutation.

Precheck validates `manifest.schema.json`, all contract hashes, exact image repo digests, cgroups, machine, topology, static H5 bundle, Redis isolation, driver qualification, metric availability, correctness negative control and namespace cleanliness.

The independent reviewer checks the exact evidence Head and may not repair evidence in place.

## 12. L2 and Mixed gates

L2 passes only when Gate, Player, Farm and Worker subtrains, Load Generator, Observability, and independent review all pass. It publishes `CP-PERF-R1-SINGLE-POD-BASELINE`, but does not set global baseline eligibility.

`CAP-MIXED-B0-R1` remains blocked until L2 PASS plus Load Generator, Observability, and automatic eligibility gates. After B2, the same contract/profile/verdict/evidence format is rerun as L1-R2/L2-R2/Mixed-B1-R2; only R2 may feed later 3M/5M PCU models.
