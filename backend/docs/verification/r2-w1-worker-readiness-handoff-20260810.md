# R2 W1 farm-worker Family readiness handoff (2026-08-10)

## Scope and base

- Exact base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`.
- Readiness contract: `worker-family-readiness-v1`.
- Ownership is limited to `cmd/farm-worker`, focused Worker recovery
  observation, `internal/worker/readiness`, and this evidence. No Friendship
  composition, Actor authority layout/Lua, Kubernetes manifest, Controller, or
  Integration state is changed.
- The Q1 typed `ClaimDueByTypes` and Pair `WorkType`/`ActionScope` routing remain
  the dispatch boundary. DueIndex observation is read-only and never replaces
  `ClaimDue`, WorkLease, or source-order truth.

## Family matrix at this base

| Family | Default | Critical | Recovery and DueIndex proof owned by farm-worker | Default-base consequence |
|---|---:|---:|---|---|
| `PLANT` | enabled | yes | Typed generic Work recovery for `PLANT` plus `UNLOCK_PLOT`; complete shared Work DueIndex must fit the bounded observation before its typed depth/oldest age is reported | Can become `READY` when repository, static route, PLAYER/FARM clients, loop, scan freshness, and scheduler are healthy |
| `HARVEST` | enabled | yes | Typed generic Work recovery for `HARVEST`; same bounded shared Work DueIndex proof | Can become `READY` under the six required capabilities |
| `CARE` | enabled | yes | Authority `CARE_REWARD_ORDER` plus CARE-scoped Pair dispatch/result orders; readiness merges both bounded DueIndex proofs and depends on the shared Pair scan loop | Can become `READY` only when both CARE sources remain recoverable |
| `THEFT` | enabled | yes | Pair DueIndex scanner, stable ActionScope routing, WorkLease and result publication | Can become `READY` under the six required capabilities |
| `FRIEND_PROJECTION` | enabled | no | The completed F1 boundary owns the FriendIndex repair queue/reconciler loop in player-svr. Theft `RESULT_PUBLISH_ORDER` is deliberately not presented as this capability | Remains visibly `FAILED`/unavailable in farm-worker, but does not mask the readiness of farm-worker-owned critical Families |
| `GUARD_SYNC` | disabled unless `GUARD_SYNC_WORKER_ENABLED=true` | yes when enabled | No implementation at this base | `DISABLED` by default and non-blocking; explicitly enabling it makes it `FAILED` |
| `MAIL` | disabled unless `MAIL_WORKER_ENABLED=true` | yes when enabled | No implementation at this base | `DISABLED` by default and non-blocking; explicitly enabling it makes it `FAILED` |

This matrix is intentionally truthful. Terminal operation-result publication
for CARE/THEFT is not the independent FriendIndex projection Family. A
farm-worker `READY` result also does not prove player-svr projection repair:
that health and delayed-projection evidence remains an F1/B2-2 system gate.

## Aggregate policy and HTTP contract

Each Family reports exactly `DISABLED`, `READY`, `DEGRADED`, or `FAILED`.

- `DISABLED`: the Family is not enabled. It never blocks the aggregate.
- `FAILED`: an enabled Family lacks an initialized repository, usable Route
  Provider, usable Participant Client, or a live scan loop.
- `DEGRADED`: those static/lifecycle capabilities exist, but the WorkLease
  scheduler's last scan failed, no scan has succeeded, the last successful
  scan is older than the versioned threshold, or a required bounded DueIndex
  depth/oldest-age proof is unavailable.
- `READY`: every required capability is present, the loop is alive, the last
  scan succeeded inside the threshold, the scheduler is healthy, and both due
  observations are proven.

Every `enabled=true && critical=true` Family must be `READY`. One non-ready
critical Family makes `ready=false` and `/readyz` return HTTP 503. Aggregate
state is `FAILED` if any blocking Family failed, otherwise `DEGRADED` if any
blocking Family is degraded, otherwise `READY`. Enabled non-critical rows stay
fully visible with their truthful state but do not affect that aggregate.

`/readyz` returns the versioned JSON snapshot with the aggregate plus all seven
bounded Family records. Each record exposes dependency booleans, scan-loop and
scheduler health, last attempt/success timestamps, last-success age, bounded
error class, due depth/availability, oldest due age/availability, and dispatch
ratio availability. `WORKER_READINESS_MAX_SCAN_AGE_V1` controls the v1
freshness threshold (default 5 seconds). `WORKER_DUE_OBSERVE_LIMIT` controls
the observation bound and is capped at 4096 (default 4096).

`/healthz` remains process liveness and returns HTTP 200 while the HTTP process
is serving. A blocked or failed Redis scan changes readiness, not liveness, so
it cannot create a transient dependency restart storm. Scan-loop startup and
shutdown use channel/mutex generation barriers; no timing sleeps establish
lifecycle state. A recovered panic marks only that loop's Family failed.

Participant Client usability is not inferred from configured map length.
farm-worker retains the exact bounded static Holder `ClientConn` sets
separately for PLAYER and FARM, explicitly calls `Connect`, and installs one
state watcher per connection. Every required connection must be gRPC
`connectivity.Ready`; an empty set, IDLE/CONNECTING startup, TRANSIENT_FAILURE,
or SHUTDOWN makes the affected PLANT/HARVEST/CARE/THEFT dependency false even
when DueIndex is empty. A later READY transition restores the dependency.
These state transitions alter `/readyz`, never `/healthz`, and add no endpoint
labels or per-endpoint metrics.

## DueIndex observation semantics

- Due depth means work claimable at repository-trusted Redis/server time, not
  all future scheduled work.
- The generic Work observer reads at most `limit+1` entries from the complete
  shared due prefix before filtering typed WorkTypes. It reports unavailable
  instead of a partial typed count when the shared view exceeds the bound.
- Authority CARE scans the known bounded FARM bucket list. Pair CARE/THEFT
  first counts every known Pair bucket and reads entries only when the complete
  due view fits the bound. A concurrent count/read mismatch, malformed member,
  unclassifiable member, dependency error, or over-bound view is unavailable.
- A proven empty DueIndex has depth 0 and availability true. Unknown depth or
  oldest age is represented by its explicit availability boolean in JSON and
  Prometheus `NaN`; no oldest age is fabricated.
- The one-second observer cache reduces scan cost but never acts as a queue or
  wakeup source, and a proof is reused only for the identical observation
  bound. Durable DueIndex plus WorkLease remains recovery truth.

## Prometheus mapping

All new metrics use prefix `farm_worker_` and exactly one bounded `family`
label whose value is one of the seven names above. No work ID, operation ID,
bucket, raw error, or other unbounded label is added. Existing
`farm_workflow_*`, `farm_plant_recovery_*`, and `farm_theft_recovery_*` metrics
retain their prior meaning and coexist with these metrics.

| Required logical metric | Exact Prometheus name | Type and mapping |
|---|---|---|
| `worker_family_ready` | `farm_worker_family_ready` | Gauge; 1 only for `READY`, otherwise 0; JSON state distinguishes disabled/degraded/failed |
| `due_depth` | `farm_worker_due_depth` | Gauge; complete proven claimable depth, otherwise `NaN` |
| `oldest_due_age_ms` | `farm_worker_oldest_due_age_ms` | Gauge; repository-time due timestamp to snapshot time, otherwise `NaN` |
| `scan_duration_ms` | `farm_worker_scan_duration_ms` | Histogram; one complete observed Family scan |
| `claimed_total` | `farm_worker_claimed_total` | Counter; successful WorkLease claims attributed to Family |
| `completed_total` | `farm_worker_completed_total` | Counter; durable terminal completions attributed to Family; shared Pair CARE completions count as CARE without inventing a second scan duration |
| `retried_total` | `farm_worker_retried_total` | Counter; durable retry/reschedule decisions |
| `step_unknown_total` | `farm_worker_step_unknown_total` | Counter; participant response boundaries requiring receipt-first resolution |
| `direct_dispatch_hit_ratio` | `farm_worker_direct_dispatch_hit_ratio` | Gauge; direct hits / (direct hits + worker takeovers), `NaN` until a bounded source-side observation is supplied |
| `worker_takeover_ratio` | `farm_worker_worker_takeover_ratio` | Gauge; worker takeovers / same denominator, `NaN` until supplied |
| `terminal_latency_ms` | `farm_worker_terminal_latency_ms` | Histogram; creation/acceptance-to-terminal only when durable timestamps prove both ends; absent samples are not zero-latency claims |

The direct/takeover ratios are intentionally unavailable in this base because
farm-worker does not own source-side direct-dispatch counters. Reporting zero
would silently redefine “no observation” as “no hits.”

## Deterministic fault evidence

| Required behavior | Deterministic evidence |
|---|---|
| Plant repository unavailable makes Worker not ready | `TestPlantRepositoryDisconnectedIsFailedAndNotReady`; transient scan fault also exercised by `TestRegistryAggregateRequiresEveryEnabledCriticalFamilyReady` |
| Care repository unavailable is isolated; aggregate is explicit | `TestCareRepositoryDisconnectedFailsAggregateButOtherFamiliesContinue`; recoverable transient path in `TestCareFaultIsIsolatedAndCanRecover` |
| Redis delay/error changes readiness, health remains 200, and readiness recovers | `TestHealthIsLivenessWhileTransientScanFailureChangesReadinessAndRecovers`; channel barriers block/release exact rounds without sleeps |
| Successful scan ages past threshold | `TestStaleSuccessAndUnknownDueProofAreDegraded` |
| Scan loop panics/exits | `TestScanLoopPanicMarksNotReadyWithoutTakingLivenessDown` and `TestLoopGenerationProvidesHappensBeforeFence` |
| Replacement recovers durable due work, no duplicate effect, no residual | `TestLocalWorkerKillReplacementRecoversDueWithoutDuplicateOrResidual`; first local worker is cancelled after the idempotency receipt/side effect, repository time crosses lease expiry, replacement completes once, and typed DueIndex depth proves zero |
| Unknown/over-bound DueIndex is not fabricated | `TestDueIndexProofIsUnavailableRatherThanPartialBeyondBound` and `TestStaleSuccessAndUnknownDueProofAreDegraded` |
| Cached proof cannot escape a smaller bound | `TestCareDueObservationCacheNeverReusesProofAcrossBounds` and `TestPairDueObservationCacheNeverReusesProofAcrossBounds` |
| Disabled GUARD_SYNC/MAIL do not block | `TestDisabledGuardAndMailNeverBlock` |
| External Friend projection remains visible but non-blocking | `TestProductionFamilyMatrixKeepsFailedExternalFriendProjectionVisibleAndNonBlocking` |
| GUARD_SYNC/MAIL are critical only when enabled | `TestEnabledOptionalFamiliesAreCriticalAndFailClosedWithoutImplementations` |
| Holder clients start fail-closed, require every connection, recover, and preserve liveness | `TestParticipantConnectionsFailClosedTransitionRecoverAndKeepLiveness` and `TestEmptyHolderConnectionSetIsNeverUsable` |
| Stable CARE/THEFT/RESULT routing remains intact | Existing Q1 tests including `TestTheftRecoveryRoutesCareBeforeTargetLookupAndLeavesCareOrdersUntouched`, `TestLegacyCareDueUpgradeRoutesOnceToCareAndDoesNotSpinLeaseRetryOrLastError`, and `TestSharedPairWorkerRoutesRejectedCareAndLeavesOtherPairWorkNotOwned`; this change does not alter the typed claim/scope router |
| Required names and bounded labels | `TestMetricsExposeOnlyBoundedFamilyLabelsAndRequiredNames` |

The replacement test is a deterministic local fixture. It is **not** an actual
Kubernetes Worker-kill PASS. A Kubernetes verdict still requires a real
cluster, deployed workload, observable pod replacement, and the same
duplicate/residual evidence.

## Verification commands and environment note

Commands use a writable cache because the sandbox's default Go cache is
read-only:

```text
GOCACHE=/tmp/farm-worker-r2w1-go-cache go test ./internal/worker/readiness ./cmd/farm-worker
GOCACHE=/tmp/farm-worker-r2w1-go-cache go test -race ./internal/worker/readiness ./cmd/farm-worker
GOCACHE=/tmp/farm-worker-r2w1-go-cache go test ./internal/workflow ./internal/worker/plantrecovery ./internal/worker/harvestrecovery ./internal/worker/carerecovery ./internal/worker/theftrecovery
GOCACHE=/tmp/farm-worker-r2w1-go-cache go test -race ./cmd/farm-worker ./internal/worker/readiness ./internal/workflow ./internal/worker/plantrecovery ./internal/worker/harvestrecovery ./internal/worker/carerecovery ./internal/worker/theftrecovery
GOCACHE=/tmp/farm-worker-r2w1-go-cache go test -run '^$' ./...
GOCACHE=/tmp/farm-worker-r2w1-go-cache go vet ./...
GOCACHE=/tmp/farm-worker-r2w1-go-cache go build -buildvcs=false ./cmd/farm-worker
```

The exact HEAD/base is independently verified with `git rev-parse HEAD` and
`git merge-base`. Focused, race, and Redis-backed Work/CARE/THEFT recovery
tests pass in this resumed socket-capable workspace, including the existing Q1
typed WorkType/ActionScope routing fixtures. This is local code/test evidence;
it is not an actual Kubernetes Worker-kill, deployed Staging, or product-load
PASS.
