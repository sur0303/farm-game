# RT2B Dynamic Route Consumer Handoff

- task_id: `RT2B-DYNAMIC-ROUTE-CONSUMER`
- status: `COMPLETED_REVIEW_CLOSED`
- base_commit: `29ed7be2f7ddc6df2e837830c165ad50ca62e28f`
- branch: `codex/rt2b-dynamic-route-consumer-20260808`
- worktree: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-rt2b-dynamic-route-consumer-20260808`
- started_at: `2026-08-08`

## Implementation

- Added the `actorrouting.RemoteAssignmentInstaller` Pump-only seam.
- Added an explicit fail-closed remote provider constructor. Before the first
  accepted full snapshot, `GetSnapshot` and `WatchFrom` return
  `ErrNoAssignment`.
- Added `internal/actorrouting/remote` with:
  - trusted transport-neutral `RemoteSnapshotSource`;
  - `RemoteAssignmentPump` and explicit remote provider constructor;
  - `BOOTSTRAPPING`, `WATCHING`, `RESYNCING`, `DEGRADED`, and `CLOSED`;
  - arbitrary positive first revision and revision jumps such as R10 to R12;
  - duplicate idempotency, stale rejection/counting, same-revision conflict
    rejection, topology/generation/holder rollback rejection;
  - legal same-generation `ASSIGNING -> ACTIVE` and illegal reverse transition;
  - bounded full-resync retry after watch close/error;
  - cancellation of fetch, watch, and retry backoff; normal `Close` is
    non-fatal;
  - serialized local installation and zero remote calls on the local provider
    hot path.
- Existing static, dynamic, and manager provider tests remain green.
- P1 correction: `runLoop` now resyncs only for `ErrWatchUnavailable` /
  exhausted watch channels. Topology changes, same-revision conflicts, and
  invalid watch snapshots return immediately, preserve the last legal local
  snapshot, and do not perform a second Fetch.
- `CLOSED` is terminal for `Run`: after context cancellation or `Close`, a
  second `Run` is a no-op and cannot restart remote fetch.

## Luna Review Closure

Independent Luna Review of `bbd0485` reported `P0=0`, `P1=2`. Both findings
are closed by `f9316764a3767866bc6a2316001125f9cd14277f`
(`fix(actorrouting): close remote watch review findings`):

- `P1-1`: Pump validation now permits holder/boot changes only for the exact
  initial placeholder (`ASSIGNING`, assignment/writer/lease generation all
  `1`) to same-generation `ACTIVE`, matching the trusted provider/manager
  bootstrap exception. Non-initial same-generation activation still requires
  the holder and boot to remain unchanged.
- `P1-2`: WatchFrom error, nil channel, and closed channel now share bounded
  liveness backoff and `MaxWatchFailures` budget. Duplicate full fetches
  cannot create a hot loop; context and `Close` interrupt backoff immediately.
  A valid watch update resets the liveness budget. Snapshot contract errors
  remain terminal and never enter this recovery path.

## Verification

I0 external re-verification in a listener-capable environment on 2026-08-08:

```text
focused PASS
race PASS
vet PASS
GOCACHE=/tmp/rt2b-go-cache go test ./... PASS
owned write set PASS (no path overreach)
```

Local focused verification:

```text
GOCACHE=/tmp/rt2b-go-cache go test ./internal/actorrouting/...
GOCACHE=/tmp/rt2b-go-cache go test -race ./internal/actorrouting/...
GOCACHE=/tmp/rt2b-go-cache go vet ./internal/actorrouting/...
git diff --check
```

Post-review focused verification after `f931676`:

```text
focused PASS
race PASS
vet PASS
diff-check PASS
```

Continuation verification after freezing the liveness fixture:

```text
GOCACHE=/tmp/rt2b-go-cache go test -race -count=20 ./internal/actorrouting/remote -run '^TestRemotePumpWatchLivenessFailuresAreBoundedForErrorNilAndClosed$' PASS
GOCACHE=/tmp/rt2b-go-cache go test -race ./internal/actorrouting/... PASS
GOCACHE=/tmp/rt2b-go-cache go test ./... BLOCKED_BY_SANDBOX_LISTENER_RESTRICTION
```

The liveness test now creates one `base` snapshot per subtest and returns a
deep clone on every full fetch, keeping `LeaseUntilMs` stable while preserving
the duplicate-resync behavior under test for watch error, nil-channel, and
closed-channel sources.

The continuation full-repository run reached the owned
`internal/actorrouting/remote` package successfully, but the sandbox rejected
existing loopback listener setup (`listen tcp 127.0.0.1:0: socket: operation
not permitted`) in `cmd/player-svr` and Redis/miniredis-backed packages.

The full repository command was also replayed locally:

```text
GOCACHE=/tmp/rt2b-go-cache go test ./...
```

The local replay is blocked by this sandbox's listener restriction
(`listen tcp 127.0.0.1:0: socket: operation not permitted`) in existing
`cmd/player-svr`, `authoritycomposition`, Redis/miniredis, and related tests.
This does not change the I0 external full-gate PASS above.

## Commit

1. `8d54516e026acad1382e69f77cc2dcc383edc9ba`
   `feat(actorrouting): add remote route consumer`
2. `bbd048595849fc2eaca5e752decfe67b7f533a0a`
   `docs(verification): record RT2B remote consumer handoff`
3. `f9316764a3767866bc6a2316001125f9cd14277f`
   `fix(actorrouting): close remote watch review findings`
4. Review-closure documentation commit: this handoff commit; final SHA is
   reported by the completion `git log`.

## Risks

- `RemoteSnapshotSource` is intentionally trusted and transport-neutral.
  RT2C must provide authentication, identity, envelope, checksum, and
  cancellation/deadline enforcement before exposing a production adapter.
- Local full-repository replay remains environment-blocked by forbidden local
  listeners; I0 external full-gate verification is PASS.

## Rollback

Discard the three task-owned paths and restore the worktree to
`29ed7be2f7ddc6df2e837830c165ad50ca62e28f`. No forbidden path was modified.
