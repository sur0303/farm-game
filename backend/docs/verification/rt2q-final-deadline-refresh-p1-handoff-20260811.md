# RT2Q Final P1 Handoff: Authority Renew Deadline Is Not Republished (2026-08-11)

> **Implementation update:** the closure candidate is documented in
> `rt2q-final-deadline-refresh-p1-closure-20260811.md`. This handoff remains the
> finding-of-record, while final freeze remains blocked on exact-tip long-lived
> E3, S0–S6 rerun, company canary, and independent Exact Review.

## 1. Executive status

RT2Q S0-S6 functional verification is complete, but the candidate is **not
approved for freeze, integration, K8s rollout, or autoscaling work**.

The exact clean SSH-signed candidate reviewed was:

`d78212ee777c5d660a821113571c77a59038783c`

A final exact review found one merge-blocking P1: Holder-side Authority Renew
extends durable `ShardWriteAuthority.LeaseUntilMs`, but no Manager-owned path
republishes that extension into the complete RouteSnapshot. Every remote
resolver continues to use the old Snapshot deadline and eventually returns
`ErrLeaseExpired`, even while durable Authority remains ACTIVE and fresh.

This is a production-lifetime defect, not an evidence-only discrepancy. The
current S0-S6 runs complete inside one lease window and therefore do not prove
long-running dynamic routing.

## 2. Completed evidence before this finding

### 2.1 S0-S6 matrix

Evidence head:

`f112087c9ced47f212194e8d357a823aad28be95`

All 21 fresh isolated E3 runs passed (three per scenario):

- `/tmp/rt2q-final-s0-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s1-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s2-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s3-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s4-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s5-f112087c9ced-r{1,2,3}`
- `/tmp/rt2q-final-s6-f112087c9ced-r{1,2,3}`

Summary:

`/tmp/rt2q-final-all-f112087c9ced-summary.txt`

S0 was additionally rechecked three times after its write-path readiness fix:

- `/tmp/rt2q-final-s0-147dbb628c89-r{1,2,3}`

### 2.2 S6 Fresh Join

S6 also passed three times on code head
`d44f216075adb18e42e7f8ca7d4aa354ebc17042`:

- `/tmp/rt2q-s6-e3-d44f216075ad-r1`
- `/tmp/rt2q-s6-e3-d44f216075ad-r2`
- `/tmp/rt2q-s6-e3-d44f216075ad-r3`

It proves fresh Gate/Worker Boot convergence, authenticated full Snapshot
bootstrap, T1-T3 business operations, one committed T2 response loss, fresh
session replay with `same_receipt=true` and `duplicate_reward=false`,
subscription rejoin, four-truth reconciliation, and exact cleanup.

### 2.3 Local gates

The following were PASS before exact review:

- relevant `go test`
- focused `go test -race`
- `go vet`
- harness/business-driver/routectl builds
- diff and secret checks

These results remain useful regression evidence, but they do not close the new
long-duration P1.

## 3. P1 finding

### 3.1 User-visible failure

After the complete Snapshot's `LeaseUntilMs` passes:

1. the current Holder may still own a fresh ACTIVE durable Authority because it
   has renewed successfully;
2. Holder-local generation fencing can remain valid;
3. the remote resolver still reads its immutable local complete Snapshot;
4. `StaticResolver.Resolve` compares wall clock with the stale Snapshot
   deadline;
5. Resolve fails with `ErrLeaseExpired`.

The failure affects Gate, Worker, PLAYER/FARM holders, and any other remote
Snapshot consumer. It does not require Redis or route-controller to be down.

The default Authority lease maximum is one minute, so this is not a theoretical
multi-day horizon.

### 3.2 Exact code chain

#### Lease is capped at 60 seconds

`internal/storage/authoritystore/shard_authority.go:19-20`

```go
func DefaultLeaseLimits() LeaseLimits {
    return LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: time.Minute}
}
```

#### Holder paths renew Authority directly

`internal/authoritycomposition/membership_adapter.go:125`

`RealMembershipAdapter.PrepareMembership` invokes
`RenewShardAuthority` and may extend only the deadline of the same generation.

`internal/authoritycomposition/composition.go:614`

The runtime membership path also invokes `RenewShardAuthority` directly.

These calls mutate durable Authority, not the RouteSnapshot.

#### Manager Renew explicitly does not publish

`internal/actorrouting/authority/manager.go:1355-1357`

```go
// Renew is deliberately separate from generation transitions. A holder may
// renew only its own current tuple; this call never invokes Publish.
```

In the production topology, remote holders do not call this controller Manager
method anyway; they use their own Authority store adapter. Therefore changing
only `RouteAssignmentManager.Renew` would not close the production path.

#### Completed recovery records are skipped

`internal/actorrouting/authority/manager.go:1023-1026`

Once a recovery is `Published` and `LastState==ACTIVE`, periodic pending
projection recovery skips it. It does not compare the ACTIVE Snapshot deadline
with current durable Authority.

#### Resolve enforces the stale Snapshot deadline

`internal/actorrouting/routing.go:900-901`

```go
if assignment.LeaseUntilMs <= 0 || time.Now().UnixMilli() >= assignment.LeaseUntilMs {
    return Assignment{}, ErrLeaseExpired
}
```

#### Verification compatibility does not republish

`internal/routeplan/executor.go:883-896` allows a current Authority deadline to
be greater than or equal to the deadline captured by the durable Snapshot. This
is correct for read-only verification of one immutable replay floor, but it
cannot keep the data plane alive. It must not be treated as a substitute for a
new complete Snapshot publication.

### 3.3 Why S0-S6 did not catch it

- Scenarios complete before the original Snapshot lease expires.
- The harness Authority TTL is 60 seconds.
- The healthy renew cadence was moved so final Verify could sample the exact
  migration publication before an ordinary post-migration Renew.
- S3/S4 Verify correctly learned to accept a monotonic same-generation durable
  Renew, but Verify only reports consistency; it does not install a newer
  Snapshot in consumers.
- No mandatory scenario currently crosses one full Snapshot lease horizon and
  performs data-plane Resolve afterward.

## 4. Safety constraints for the fix

The fix must preserve all frozen contracts:

1. `Redis ShardWriteAuthority` remains the highest authority.
2. `RouteAssignmentManager` remains the only generation/projection coordinator.
3. No second transition state machine may be introduced.
4. HolderRegistry is not authority and cannot supply the new deadline.
5. Only exact Authority keys may be read; no `KEYS`, global `SCAN`, or dynamic
   key discovery is allowed.
6. Watch continues to transport complete Snapshots only; no deadline patch
   messages.
7. Resolve remains process-local with zero remote calls.
8. Holder/Boot/AssignmentRevision/WriterEpoch/LeaseEpoch must match exactly.
9. Only `LeaseUntilMs` may advance monotonically for a same generation.
10. Generation drift, deadline regression, missing Authority, expired current
    Authority, read error, publish conflict, or incomplete topology must fail
    closed.
11. Planned migration and Crash Takeover semantics must not change.
12. The controller remains single-replica, non-HA, stop-first/Recreate.

## 5. Required implementation shape

### 5.1 Manager-owned reconciliation API

Add a narrow Manager-owned operation, conceptually:

```go
ReconcileRenewedAuthorityDeadlines(ctx context.Context) (changed bool, err error)
```

The name is not frozen, but ownership is: the operation belongs to
`RouteAssignmentManager`, not HolderRegistry, PlanService, WatchHub, or the
resolver.

### 5.2 Bounded exact reads

The operation should derive the bounded set of ACTIVE routes from the current
complete Snapshot and read only each route's canonical exact Authority key
through the existing authenticated/typed `AuthorityReader`.

No Redis enumeration is needed or allowed. Pipelining exact known keys is an
optimization, not a correctness requirement.

### 5.3 Exact monotonic validation

For each ACTIVE route:

- layout/hash/domain/shard must match;
- state must remain ACTIVE;
- HolderID and HolderBootID must match exactly;
- AssignmentRevision, WriterEpoch, and LeaseEpoch must match exactly;
- LastTransitionID must remain compatible with the projected generation;
- current Authority deadline must be fresh;
- current deadline must be `>=` Snapshot deadline;
- `<` is a terminal/fail-closed regression;
- `>` is the only permitted refresh.

ASSIGNING routes must not be rewritten by this path. They remain owned by the
existing migration/recovery pipeline.

### 5.4 Serialization and revalidation

The refresh must serialize with Manager transition/projection work. A safe
shape is:

1. enter the existing single recovery/execution owner;
2. capture the immutable current Snapshot identity/revision;
3. perform bounded exact Authority reads;
4. revalidate that no Manager transition changed the Snapshot/generation;
5. clone the full Snapshot;
6. update only validated monotonic deadlines;
7. increment `GlobalRevision` exactly once for the batch;
8. publish through the existing durable complete-Snapshot publisher/CAS;
9. notify authenticated Watch consumers only after durable publication.

Do not hold a Manager mutex across unbounded network I/O unless the existing
Manager contract already supplies a bounded context and proves no deadlock.
A capture/read/revalidate pattern is preferable.

### 5.5 Cadence and liveness

The route-controller background owner currently runs on a bounded cadence. The
refresh may be integrated into that single-owner pass, provided:

- it cannot overlap Plan execution or pending projection recovery;
- it completes before projected deadlines reach the local safety margin;
- a failed pass is observable and retried within a bounded budget;
- repeated unchanged reads do not publish new Snapshot revisions;
- multiple deadline extensions are coalesced into one complete Snapshot when
  safe;
- revision growth and Redis load remain bounded.

A successful Authority Renew must not require a remote call on Resolve.

### 5.6 Replay evidence

Ordinary post-publication deadline refresh must not rewrite transition intent,
repeat Authority mutation, repeat holder callbacks, or create a new RoutePlan.
The existing CompleteRecovery/Tombstone remains the immutable transition replay
floor. Verification may compare it with current Authority using the already
narrow same-generation monotonic-deadline rule.

## 6. Mandatory regression tests

### 6.1 Manager unit tests

At minimum:

1. same generation + later durable deadline publishes one new complete
   Snapshot revision;
2. no changed deadline performs no publish;
3. deadline regression fails closed;
4. Holder/Boot drift fails closed;
5. assignment/writer/lease epoch drift fails closed;
6. missing/expired Authority fails closed;
7. ASSIGNING route is not refreshed by this path;
8. concurrent transition invalidates capture and prevents stale publication;
9. multiple changed shards publish one atomic complete Snapshot;
10. cancellation before publication performs no partial install.

Run focused tests at high count and under race.

### 6.2 Consumer tests

Prove that a full Snapshot containing only monotonic deadline extensions:

- is accepted at a higher GlobalRevision;
- preserves Holder and generation identity;
- rejects deadline rollback;
- installs atomically;
- keeps Resolve local.

### 6.3 Real E3 long-horizon proof

Add a mandatory real-production scenario or extension that:

1. starts the normal seven consumers;
2. records initial complete Snapshot deadline/revision;
3. observes a real holder Authority Renew to a later deadline;
4. observes a newer authenticated complete Snapshot containing that deadline;
5. crosses the original Snapshot deadline in wall-clock time;
6. performs real Gate business and direct Resolve after that deadline;
7. proves Resolve succeeds from the newer local Snapshot;
8. proves Snapshot unary/stream hot-path deltas remain zero during resolves;
9. proves no Plan, transition, Authority transfer, or holder callback replay;
10. proves exact key/process cleanup.

The proof must use production binaries. Advancing only a fake clock in the
oracle is insufficient for the final E3 gate.

### 6.4 Fault cases

Include at least:

- one Authority read unavailable/not-sent pass;
- one Snapshot CAS/publish failure;
- one same-generation deadline regression;
- one generation change racing refresh.

No case may publish a mixed or partial Snapshot.

## 7. Required final reruns after the fix

The previous 21/21 matrix becomes historical evidence after production code or
harness semantics change. On one new exact clean signed tip:

1. long-horizon deadline-refresh E3: at least three fresh PASS runs;
2. S0-S6: three fresh isolated E3 PASS per scenario;
3. relevant focused tests `-count=100`;
4. relevant race tests;
5. full owned-package tests, vet, and builds;
6. diff and secret gates;
7. company Redis canary using only exact keys;
8. independent exact review with all findings closed.

Only then may the branch be pushed and a replacement checkpoint frozen.

## 8. Repository/worktree state at handoff

- RT2Q writer worktree: `farm-game-rt2q-final-20260810`
- RT2Q branch: `codex/rt2q-final-20260810`
- rejected final candidate: `d78212ee777c5d660a821113571c77a59038783c`
- detached candidate: `/tmp/rt2q-final-candidate-d78212ee777c`
- detached read-only review worktree:
  `/tmp/farm-game-rt2q-final-review-d78212ee`
- known integration truth remains:
  `5ce43715363eea4f18e0925b6992429785053261`
- integration has not been modified, merged, rebased, or pushed;
- RT2Q has not been pushed;
- no final checkpoint has been frozen.

The review worktree must remain read-only. All fixes and tests return to the
RT2Q writer worktree.

## 9. Next execution order

1. Implement Manager-owned deadline refresh in the RT2Q writer worktree.
2. Add deterministic unit/race regressions.
3. Run three real long-horizon E3 proofs.
4. Re-run S0-S6 three times each on the new exact tip.
5. Run company Redis canary and final local gates.
6. Create a fresh detached read-only exact-review worktree.
7. Close all reviewer findings in the original writer worktree.
8. Push/freeze RT2Q only after review approval.
9. Create Premerge from the confirmed integration truth.
10. Run K8s deployment, restart, migration, ordinary traffic, load, G1/G2/G3.
11. Freeze `CP-ROUTE-CONTROL-MVP`.
12. Begin AS0-AS3 only after that checkpoint.

## 10. Explicit non-solutions

The following do not close the P1:

- allowing Resolve to ignore an expired Snapshot deadline;
- reading Redis/Controller from Resolve;
- treating HolderRegistry heartbeat as Authority;
- accepting Authority deadline `>=` Snapshot only in Verify;
- increasing TTL beyond the frozen Authority limit;
- publishing Snapshot patches;
- having each consumer independently read Authority;
- creating a second lease/generation state machine;
- replaying the migration Plan or holder Prepare;
- using `SCAN`/`KEYS` to discover Authority records.

The required result is a new durable, authenticated, complete Snapshot owned by
the existing Manager authority chain.
