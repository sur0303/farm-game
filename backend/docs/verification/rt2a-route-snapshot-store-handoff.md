# RT2A Route Snapshot Store Handoff

- status: REVIEW_FIX_COMPLETED
- task_id: RT2A-ROUTE-SNAPSHOT-STORE
- base_commit: 29ed7be2f7ddc6df2e837830c165ad50ca62e28f
- branch: codex/rt2a-route-snapshot-store-20260808
- worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-rt2a-route-snapshot-store-20260808
- started_at: 2026-08-08
- candidate_completed_at: 2026-08-08
- implementation_commit: 6104a4337ecae13d40e0124bfdbe7a5b74073374
- review_fix_commit: 3ab59ae
- continuation_base_commit: f6d1a7e
- continuation_fix_commit: this handoff commit; full SHA is reported in the final delivery
- real_redis_live: PROBE_PENDING

## Scope

Implementation and I0 verification are complete in the candidate tree. The prior
`BLOCKED_COMMIT_METADATA_READONLY` state is closed; this continuation is completing the normal
Git commit and clean-worktree handoff. Real Redis Live/platform probing is intentionally still
`PROBE_PENDING` and is not claimed as a completed gate.

Owned files are limited to:

- `internal/routecontrol/**`
- `internal/storage/redis/routecontrol/**`
- this handoff

## Implementation

- `internal/routecontrol/snapshot_codec.go`
  - validates complete snapshots through the existing `actorrouting` validators;
  - canonical `encoding/json` bytes and lowercase SHA-256;
  - versioned LP topology material containing only schema, realm, hash profile/seed and
    PLAYER/FARM shard counts;
  - strict envelope/schema/checksum/topology/revision validation.
- `internal/routecontrol/snapshot_store.go`
  - `RouteSnapshotStore` and independent `RouteSnapshotBootstrapper` capabilities;
  - empty-store bootstrap only accepts revision 1;
  - ordinary CAS is strict `R -> R+1`, with exact same-revision replay as
    `ALREADY_COMMITTED`;
  - in-memory reference implementation with mutex serialization.
- `internal/routecontrol/durable_publisher.go`
  - store-before-notify;
  - notify failure preserves the committed store;
  - replay retries notification without advancing revision;
  - ordinary and authority publication use separate existing route validators.
- `internal/storage/redis/routecontrol/**`
  - fixed `{classic-farm:<environment>:<zone>:route-control:v1}` hash tag;
  - fixed snapshot/recovery/tombstone keys and bounded ZSET indexes;
  - small versioned Lua family with EVALSHA and bounded NOSCRIPT reload;
  - item/byte capacity checks;
  - revision comparisons remain decimal-string exact in Lua, with no uint64
    `tonumber` comparison;
  - no Redis `KEYS`, `SCAN`, or `SCRIPT FLUSH` command.
- Transition adapter implements the existing
  `authority.TransitionRecoveryStore` and `authority.TransitionTombstoneStore` only;
  it does not duplicate the manager state machine. Interface calls without Context use
  the configured bounded internal timeout.

## Focused Continuation Fix

The focused I0 failure at `f6d1a7e` was caused by Go-side physical validation
reading meta, hashes, and indexes through separate Redis commands while an
atomic Lua mutation was in flight. The continuation adds:

- `ReadTransitionSnapshotV1`, a same-slot EVALSHA snapshot read that performs
  type/cardinality/meta/basic-record checks atomically and only calls bounded
  `HGETALL` after cardinality caps have passed;
- Go decoding of that snapshot with the existing full semantic checks for
  canonical JSON, valid IDs, record identity, disjoint recovery/tombstone IDs,
  exact byte totals, and exact meta counts;
- atomic snapshot reads for every Load/Lookup path and every mutation preflight;
  the mutation Lua scripts still validate before any write;
- nil/empty raw Redis state normalization in partial-write tests, preserving
  DeepEqual so real writes remain detectable;
- high-concurrency same-ID and different-ID Reserve coverage plus mixed
  Reserve/Load/Lookup/Commit/Delete and tombstone reader coverage, with
  noncanonical and invalid-ID corruption tests asserting zero mutation.

## Luna Review Closure

- P1-1 Bootstrap committed-plus-notify failure:
  Memory and Redis exact same revision-1 bootstrap now returns
  `ALREADY_COMMITTED`; publisher retries notification without advancing the
  revision. Different or malformed existing envelopes return the unified
  snapshot CAS conflict and never write.
- P1-2 notifier capability split:
  ordinary `Notify` and manager-only `AuthorityNotify` interfaces/adapters are
  separate. `Publish` cannot call the authority capability, and
  `PublishAuthoritySnapshot` cannot fall back to the ordinary capability.
- P1-3 Redis snapshot CAS:
  Go fully decodes the current envelope before EVALSHA and passes all seven
  fields as the compare envelope. Lua checks hash shape and byte-identical
  current fields before any HSET; current corruption or a race is fail-closed.
- P1-4 transition Lua side-effect ordering:
  Delete, commit, and tombstone scripts validate type, meta, hash/index,
  disjointness, record framing, and capacity before any HDEL/HSET/ZREM/ZADD.
- P1-5 physical integrity:
  bounded Go and Lua validation uses actual HLEN/ZCARD, both index/hash
  directions, actual value bytes, per-record limits, total limits, and exact
  meta equality. No KEYS or SCAN was added.
- P1-6 recovery/tombstone identity sets:
  load, restart, lookup, and every mutation reject overlapping IDs before
  proceeding; Lua repeats the disjointness check atomically.
- P1-7 canonical decimals:
  envelope revision and committed-at values require Parse followed by exact
  Format equality, rejecting leading signs/zeroes such as `01` and `-0`.
- P2 identity/context:
  transition IDs and fingerprints require valid UTF-8. Memory Bootstrap/CAS
  recheck context before and after the Now hook, so cancellation cannot commit.

Added negative and concurrency coverage includes exact bootstrap notification
retry, ordinary/authority pseudo-adapters, cancellation-after-Now, canonical
decimal rejection, snapshot corruption cases, Lua MALFORMED no-side-effect
checks, physical type/meta/index/overlap corruption, UTF-8 identity rejection,
transition canonical round trips, and concurrent reserve/CAS tests.

## Diff

Current candidate ownership check:

- candidate files: 14
- implementation commit: 13 files, 2781 insertions, 0 deletions
- review-fix commit: 11 files, 1169 insertions, 179 deletions
- handoff update: 1 file, 0 code deletions
- continuation fix from `f6d1a7e`: 4 files, 519 insertions, 146 deletions
- deletions: 0
- owned write-set check: PASS
- source formatting: PASS

## Review-Fix Verification

Focused verification in this continuation:

```text
GOCACHE=/tmp/rt2a-gocache go test ./internal/routecontrol -count=1: PASS
GOCACHE=/tmp/rt2a-gocache go test -race ./internal/routecontrol -count=1: PASS
GOCACHE=/tmp/rt2a-gocache go vet ./...: PASS
GOCACHE=/tmp/rt2a-gocache go test -run '^$' ./internal/storage/redis/routecontrol -count=1: PASS
Lua source parse/forbidden-command/UTF-8/canonical-record tests: PASS
git diff --check: PASS
```

The full repository test and Redis miniredis/race suites were attempted. This
continuation sandbox rejects all loopback listener creation with:

```text
listen tcp 127.0.0.1:0: socket: operation not permitted
```

The failures are from existing diagnostics/miniredis listener setup, including
the new RT2A Redis tests; no Redis operation reached a running server here.
The prior listenable-environment I0 results remain recorded below and are not
reclassified as review-fix runtime verification.

## I0 Revalidation

I0 reran the current uncommitted candidate in a listenable environment on 2026-08-08:

```text
focused unit: PASS
focused race: PASS
full repository: `go test ./...` PASS
full repository vet: `go vet ./...` PASS
`git diff --check`: PASS
```

The three earlier miniredis failures were rerun in that listenable environment and are closed.
No real Redis endpoint, shared credentials, failover window, or production/platform probe was
used; real Redis Live remains `PROBE_PENDING`.

## Continuation Environment Note

This continuation sandbox does not permit loopback listener creation. Its independent local
checks were:

```text
GOCACHE=/tmp/rt2a-gocache go test ./internal/routecontrol -count=1: PASS
GOCACHE=/tmp/rt2a-gocache go test -race ./internal/routecontrol -count=1: PASS
GOCACHE=/tmp/rt2a-gocache go vet ./...: PASS
GOCACHE=/tmp/rt2a-gocache go test ./...: environment-blocked by
  `listen tcp 127.0.0.1:0: socket: operation not permitted`
```

The local full-test failure is an environment restriction in existing loopback/miniredis
diagnostics, including the RT2A Redis adapter tests; no Redis server was contacted and this
does not replace the listenable-environment I0 PASS above.

## Focused Continuation Verification

The continuation's local checks used `GOCACHE=/tmp/farm-game-go-build`:

```text
routecontrol compile: PASS
routecontrol race compile: PASS
routecontrol Lua parse/key/UTF-8/canonical tests: PASS
GOFLAGS=-buildvcs=false go test -run '^$' ./...: PASS (117 packages compile)
go vet ./...: PASS
git diff --check: PASS
```

The requested executable focused count20, focused race, and full repository
test commands were attempted. They remain environment-blocked before Redis
operations by:

```text
listen tcp 127.0.0.1:0: socket: operation not permitted
```

The new continuation tests therefore still require rerun in a listenable
environment before claiming the focused runtime gate.

## Constraints

- No real secrets or shared Redis access.
- Real Redis Live Gate and platform Probe remain with I0 as `PROBE_PENDING`.
- No push, merge, or sub-agent use.

## Commit / Rollback

- implementation commit: `6104a4337ecae13d40e0124bfdbe7a5b74073374`
- review-fix commit: `3ab59ae`
- handoff commit: this document update; full SHA is reported in the final delivery.
- required final state: clean worktree on
  `codex/rt2a-route-snapshot-store-20260808`, with no push.
- review-fix rollback command (not run):
  `git revert --no-edit 3ab59ae`
- implementation rollback command (not run):
  `git revert --no-edit 6104a4337ecae13d40e0124bfdbe7a5b74073374`
- full candidate rollback is the handoff commit followed by the implementation commit; exact
  handoff SHA is reported in the final delivery.
