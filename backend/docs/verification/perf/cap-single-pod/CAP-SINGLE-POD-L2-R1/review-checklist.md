# CAP-SINGLE-POD-L2-R1 Design Review Checklist

Reviewer must review the exact Design Head and return P0/P1/P2 counts. Any unchecked required item keeps Harness implementation blocked.

## Identity and isolation

- [ ] All five frozen Commit/Image values match the approved R1 truth.
- [ ] Model Field V2, Friendship cutover and legacy roster deletion are explicitly absent.
- [ ] No mutable tag or “latest integration” path exists.
- [ ] Train A write scope cannot collide with B2 production Writers.
- [ ] Production semantics and H5 business code are outside L2 scope.
- [ ] One measured service, support topology and injection path are frozen per subtrain.

## Resources and environment

- [ ] Measured-service CPU/memory/Go limits are exact.
- [ ] Driver, Redis and support-service limits are exact and separately observed.
- [ ] Machine/config/route/profile hashes are mandatory Manifest fields.
- [ ] Every owned cgroup has a name, path hash, container hash and observed limits.
- [ ] Host-background CPU excludes all known owned cgroups.
- [ ] Host quiet window and runtime validity thresholds are decidable.
- [ ] Route snapshot age, 4h Authority lease profile and phase+30m remaining-lease gate prevent mid-run static expiry.
- [ ] Redis is loopback, unique, ephemeral and destroyed exactly.
- [ ] No secret/raw identity can enter evidence.

## Load model

- [ ] Scheduler is open-loop and cannot feedback-throttle on service latency.
- [ ] Planned/launched/unsent/completed/rejected/timeout are separate.
- [ ] Arrival lag quantiles and slope are required.
- [ ] Observability and Driver qualification are explicit stages before Smoke.
- [ ] Driver qualification proves ≥1.5× headroom and actual/planned ≥98%.
- [ ] Arrivals over 50ms late become scheduled-unsent; catch-up bursts are forbidden.
- [ ] Driver saturation yields only `INCONCLUSIVE_LOAD_GENERATOR`.
- [ ] Extended Staircase tiers force Driver requalification and candidate load remains ≤ qualified/1.5.
- [ ] Manifest records every simultaneous load target, not only a primary scalar.
- [ ] Playwright is limited to 2–4 correctness witnesses.

## Sequence and duration

- [ ] PRECHECK→SMOKE→WARMUP→STAIRCASE→CANDIDATE→FORMAL×3→REVIEW is fixed.
- [ ] Smoke/Warmup/Hold/Formal/Cooldown durations are versioned.
- [ ] Gate holds cover multiple lease cycles.
- [ ] Formal rounds are clean, identical and independent.
- [ ] Profile changes force a new version/hash and restart.

## Correctness and stop behavior

- [ ] Every global and service-specific hard correctness gate is represented.
- [ ] Intentional Worker backlog does not falsely trigger Due residual.
- [ ] Correctness has highest verdict precedence.
- [ ] New arrivals stop immediately after correctness signal.
- [ ] Inflight observation and bounded diagnostics happen before teardown.
- [ ] Old failed/saturated evidence is retained and ineligible.
- [ ] Product Bug and performance bottleneck routes are separate.

## Observability

- [ ] Every safe constant has sufficient numerator, denominator, latency, resource and queue metrics.
- [ ] ConnectionWriter items/bytes/age are required for Gate.
- [ ] Actor mailbox items/bytes/wait and memory/Actor are required.
- [ ] Commit bytes and Redis request/reply bytes are required.
- [ ] Worker Due depth/age/drain and per-family fairness are required.
- [ ] Missing metrics cannot be estimated from logs or substituted with L1 primitives.
- [ ] Scrape gaps, counter resets and oracle completeness are fail-closed.
- [ ] Required production-file metric changes are delegated to the existing B2 Writer.

## Safe tier and verdict

- [ ] Knee and safe tier are algorithmically distinct.
- [ ] A saturated tier can identify knee but cannot be selected safe.
- [ ] Queue slope estimator, sample count, confidence rule and net-growth floor are frozen.
- [ ] Queue slope is non-positive at candidate and Formal.
- [ ] Dependency and Driver first bottlenecks are inconclusive, not capacity.
- [ ] Three Formal rounds require actual/planned ≥98%.
- [ ] Cross-round stability threshold is explicit.
- [ ] L2 PASS requires all four services, Driver, Observability and independent review.
- [ ] L2 PASS does not set global `baseline_eligible=true` or claim 30M DAU.

## Service scope

- [ ] Gate connection, handshake, frame, egress, lease, slow-reader and drain profiles are complete.
- [ ] Player hot/cold/single-hot/Participant profiles are complete.
- [ ] Farm uniform/cold/1×200/10×200/slow-reader/multi-Gate profiles are complete.
- [ ] Worker family and mixed-family fairness profiles are complete.
- [ ] Initial tiers are safe starting suggestions, not predeclared capacity.

## Required reviewer output

```text
P0=<n>
P1=<n>
P2=<n>
IDENTITY_FREEZE=PASS|FAIL
LOAD_MODEL=PASS|FAIL
CORRECTNESS_STOP=PASS|FAIL
OBSERVABILITY_CONTRACT=PASS|FAIL
SAFE_TIER_ALGORITHM=PASS|FAIL
HARNESS_IMPLEMENTATION=AUTHORIZED|BLOCKED
MIXED_B0=BLOCKED
CAPACITY_CLAIM=NONE
```
