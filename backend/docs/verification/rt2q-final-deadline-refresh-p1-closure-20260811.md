# RT2Q Final P1 Closure Candidate: Renewed Authority Deadline Projection (2026-08-11)

## Status

The production implementation for `RT2Q-P1-AUTHORITY-DEADLINE-PROJECTION-REFRESH`
is complete in the RT2Q Writer worktree. It is a **closure candidate**, not yet
a freeze decision: three exact-tip long-lived E3 runs, the exact-tip S0–S6
matrix, company Redis canary, and independent detached Exact Review remain
mandatory before push or checkpoint replacement.

The historical S0–S6 evidence on `f112087c9ced...` remains pre-fix,
short-horizon regression evidence only.

## Root cause and complete closure

The original P1 had two coupled production effects:

1. Holder lease schedulers renewed durable `ShardWriteAuthority.LeaseUntilMs`,
   but no Manager-owned path projected the later deadline into a new complete
   `RouteSnapshot`.
2. Once that projection was added, the Holder membership reconciler treated
   the deadline-only change as a new `MembershipGeneration` because
   `MembershipGeneration.equal` compared `LeaseUntilMs`. It therefore fenced
   and reinstalled the same holder/boot/assignment/writer/lease epochs. Real
   long-horizon Gate traffic exposed this as `blobstore: writer epoch mismatch`
   for newly activated actors even though all seven route consumers had applied
   the refreshed Snapshot.
3. RoutePlan production verification still required the current Snapshot
   deadline to equal the immutable transition recovery deadline. A legitimate
   later projection therefore made long S5 verification report transition and
   recovery inconsistency. Verification now enforces the bounded ordering
   `durable replay deadline <= projected Snapshot deadline <= current Authority
   deadline`, while retaining exact immutable generation and transition
   identity.

The closure keeps the frozen authority order intact:

`Redis ShardWriteAuthority > RouteAssignmentManager > durable complete Snapshot CAS > authenticated complete-snapshot transport > process-local immutable snapshot > views`.

### Manager-owned projection

`RouteAssignmentManager.ReconcileRenewedAuthorityDeadlines` now:

- runs under a context-aware exclusive Manager operation boundary shared with
  generation mutations and pending recovery;
- captures the current complete Snapshot under the local state mutex;
- derives only exact Authority keys for current ACTIVE routes;
- performs every Authority Redis read outside the Manager state mutex;
- requires exact layout, topology, domain/shard, holder, boot,
  AssignmentRevision, WriterEpoch, LeaseEpoch, ACTIVE state, and compatible
  LastTransitionID;
- rejects missing, expired, regressed, malformed, or generation-drifted
  Authority as an all-or-nothing batch;
- excludes ASSIGNING routes;
- revalidates the captured complete Snapshot before publication;
- advances only `LeaseUntilMs`, publishes one complete Snapshot, and increments
  `GlobalRevision` exactly once for the batch;
- performs no Plan, Transition, Authority mutation, Drain callback, Prepare,
  Stop, Wait, or Facts operation;
- preserves durable-CAS/notification ambiguity by recording a local pending
  notification and replaying the exact committed revision, never manufacturing
  another revision.

Controller startup ordering is now:

1. pending Manager projection recovery;
2. ACTIVE Authority deadline reconciliation;
3. Plan recovery;
4. Ready.

The same single recovery/execution owner runs the bounded background cadence and
guards Plan execution.

### Holder-side deadline semantics

`MembershipGeneration.equal` now compares only immutable Authority generation
identity. `LeaseUntilMs` is explicitly the monotonic mutable projection field.
A deadline-only Snapshot update advances reconciler bookkeeping without Fence,
Remove, Prepare, install, or a second scheduler entry. The sole lease scheduler
continues to refresh the live guard from the durable Renew result.

A real exploratory long-lived E3 run demonstrated why this is required: before
this membership correction all seven consumers applied the new Snapshot, but
post-horizon first-write activation failed with writer-epoch mismatch. After the
correction, 100/100 real Gate operations across both Gates and both PLAYER
shards succeeded beyond the old Snapshot deadline. The subsequent exact-tip S5
rerun exposed and closed the production verification equality assumption; the
immutable replay floor remains unchanged and is never rewritten by refresh.

## Bounded cadence and deployment contract

Production controls are strict durations:

- `ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL` — default `1s`, maximum `30s`;
- `ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD` — default `30s`, maximum `59s`.

Lead must exceed interval + Redis operation timeout + `5s` Watch/apply
headroom. The remote-watch example pins the E3-tested Holder profile on every
Holder role:

- Authority TTL `60s`;
- safety margin `10s`;
- renew interval `40s`;
- refresh interval `1s`;
- refresh lead `30s`.

One pass coalesces every currently advanced ACTIVE shard into one complete
Snapshot. A periodic Watch publication may overlap a business window; this is
control-plane propagation. The request hot path still performs zero unary route
RPCs and resolves only from the process-local immutable Snapshot.

Current single-controller availability remains explicit: request routing has no
control-plane dependency on its hot path, but long-term route liveness depends
on the controller publishing before the Snapshot lease horizon. Controller
unavailability beyond that horizon intentionally causes local fail-closed
resolution.

## Metrics and alert contract

The controller exposes `/metrics` when
`ROUTE_CONTROLLER_METRICS_LISTEN` is configured. The example binds
`0.0.0.0:9090` and exports:

- `route_deadline_refresh_pass_total{result}`;
- `route_deadline_refresh_publish_total`;
- `route_deadline_refresh_noop_total`;
- `route_deadline_refresh_changed_shards`;
- `route_deadline_refresh_cas_conflict_total`;
- `route_deadline_refresh_read_error_total`;
- `route_snapshot_min_remaining_ms`;
- `route_deadline_projection_lag_ms`.

Every remote consumer exports:

- `route_watch_apply_lag_ms`;
- `route_resolve_lease_expired_total`;
- the existing installed Snapshot revision gauge.

Production alerting must treat any sustained refresh read/CAS error, positive
lease-expired rate, or minimum remaining budget entering the bounded
poll/read/CAS/Watch/apply envelope as actionable.

## Tests completed before final exact-tip validation

- full `go test ./... -count=1` — PASS;
- full `go vet ./...` — PASS;
- focused race across Manager, remote consumer, membership composition, and
  route-controller — PASS;
- Manager deadline tests cover multi-shard one-batch publication, noop,
  regression/read/publish failure, ASSIGNING exclusion, bounded cancellation,
  serialized concurrent transition, and committed-notification replay;
- membership tests prove deadline extension is UNCHANGED immutable generation,
  advances bookkeeping, and invokes zero membership side effects;
- consumer tests prove atomic higher-revision deadline install, rollback
  rejection, and local Resolve after the superseded deadline;
- production Verification tests prove the exact durable <= Snapshot <= current
  Authority deadline ordering and reject either bound violation;
- strict config, Compose profile, Prometheus registration, and trusted oracle
  tests — PASS.

## Exploratory production evidence

A clean detached exploratory run on implementation tip
`5725729a371ca15b23bc345296fda97a947b856b` passed:

`/tmp/rt2q-deadline-refresh-572572-r0`

It proved:

- four ACTIVE Authority deadlines advanced without generation change;
- one complete refreshed Snapshot was published;
- seven production consumers exposed the post-horizon installed revision;
- wall clock crossed the maximum old Snapshot deadline;
- 100/100 real Gate business operations succeeded;
- hot-path unary route RPC delta was zero;
- Holder callback and Authority Transfer deltas were zero;
- exact-key/process cleanup and offline trusted verification passed.

This run is exploratory because subsequent harness/docs hardening changes the
exact tip. It must not be used as final freeze evidence.

## Remaining gates

1. commit this closure packet and create a fresh clean detached exact-tip
   worktree;
2. run `deadline-refresh` E3 three times on that exact tip;
3. rerun S0–S6 three times each on the same tip;
4. run company Redis exact-key canary plus build/vet/diff/secret/large-object
   gates;
5. perform independent detached read-only Exact Review and close every
   P0/P1/P2 finding in this Writer;
6. only then push RT2Q and freeze a replacement checkpoint.
