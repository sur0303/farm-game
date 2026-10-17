# CAP-SINGLE-POD-L2 generic Harness

This command is test-only infrastructure. It does not contain or modify product business semantics.

## Structure

```text
Generic Core
├── Open-loop Scheduler  scheduler.go / histogram.go
├── Collector            collector.go / collector_adapter.go / prometheus.go
├── Oracle               oracle_adapter.go
├── Tier judgments       tier.go
├── Verdict              evaluate.go / formal.go
├── Evidence             evidence.go
└── Stop Conditions      policy.go / required.go

Profiles
├── R1 Binding           deploy/load/cap-single-pod-r1-binding.json
├── R2 Binding           deploy/load/cap-single-pod-r2-binding.json
└── R2 Binding (blocked) deploy/load/cap-single-pod-r2-binding.blocked.json
```

Both bindings are immutable and active. A binding only counts once its own digest is the one
compiled into `trustedActiveBindingSHA256`, so the identity it pins cannot move without the move
being visible in Go source. The blocked R2 file is kept as the shape a binding has before it earns
that trust, and the core still refuses it.

R2 pins the dynamic-route final candidate. Its service binaries are the ones that carried
G1/G2/G3, but its login posture is not the product posture, so no run under R2 may be read as a
product acceptance verdict. R2 also narrows the required-metric set to what the deployed image
exports; every metric it drops is listed with its reason in the profile directory and is restored
at the T1 capacity staircase.

## Commands

```text
cap-single-pod verify-contract --contract-dir <absolute path>
cap-single-pod serve-collector --config <absolute path>
cap-single-pod serve-oracle --config <absolute path>
cap-single-pod run --plan <absolute path> --out <new absolute directory>
cap-single-pod finalize-formal --candidate <dir> --formal-1 <dir> --formal-2 <dir> --formal-3 <dir> --out <new dir>
```

Run `scripts/perf/cap-single-pod/run-harness` rather than invoking `run` directly. The wrapper validates the Run Plan and
Run Manifest with Draft 2020-12 JSON Schema before the Go preflight validates exact runtime bindings and hashes.

## Phase results

Qualification, Smoke, Warmup, Staircase, Candidate and individual Formal rounds only emit phase decisions. They never
emit `PASS_SAFE_TIER`. Only `finalize-formal` can emit `PASS_SAFE_TIER`, after one candidate and all three identical Formal
rounds pass the cross-round CV gates. Independent review is still required before publication.

## Safety

- Endpoints and listeners are loopback-only and credential-free.
- Arrivals over 50 ms late are unsent; no catch-up burst is generated.
- Required metrics and correctness gates are checked at Preflight and every runtime sample.
- Counter reset, missing metric, incomplete oracle and scrape gaps fail closed.
- A staircase rung declares its baseline in `metadata.tier_baseline`: either `FIRST_TIER`, or
  `PREVIOUS_TIER_EVIDENCE` with `metadata.previous_tier_evidence_path` naming the tier below.
  An undeclared baseline stops the run before it generates load; it never becomes a first tier.
- The drain, the p99 comparison against the tier below and the p99 stability of the tier are the
  harness's own judgments. Each is reported only from what the run observed.
- Correctness signals stop new arrivals before lower-priority stop conditions.
- Evidence excludes raw endpoints, secrets and operation identities.
- `baseline_eligible=false`, `capacity_claim=NONE`, and `supports_30m_dau=false` remain fixed.
