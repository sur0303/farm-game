# Theft Guard live-sync P1 fix verification

Date: 2026-08-05
Status: `IMPLEMENTED / VERIFIED`

## Defect closed

A hot `FarmActor` previously loaded the guard sidecar only during activation.
Later guard activation, switch, feed, or expiry could update Redis while that
actor remained active, so its next Theft turn could adjudicate from an older
`GuardSnapshot`.

The production Farm Participant endpoint now admits the existing
`APPLY_GUARD_SNAPSHOT` command and submits it through the target Farm registry's
Participant mailbox. The mailbox turn monotonically compares the snapshot,
persists the full effective snapshot and typed result in the existing bounded
Farm operation-receipt carrier, performs the existing fenced Farm authority
CAS, and updates the hot guard only after that CAS is known committed.

No Redis or network read occurs inside `ApplyGuardSnapshotWithAction` or the
following Theft actor turn. Source order, route, identity, WorkVersion, and
worker-lease proofs all run in Participant ingress before mailbox admission.

## Production path

```text
Player authority GuardSyncOrder
  -> existing CoordinatorParticipantService.ExecuteStep
     or WorkerRecoveryIngress.ResumeStep
  -> source-order WorkAddress/version/identity/lease proof
     (read-only Player authority bucket + Redis TIME)
  -> Farm route/writer fence
  -> Farm Registry.SubmitParticipant
  -> FarmActor.ApplyGuardSnapshotWithAction mailbox turn
  -> fenced Farm authority CAS + durable Participant receipt
  -> hot GuardSnapshot assignment
  -> next FarmActor Theft turn
```

`farm-svr` reuses its existing Participant verifier and service registration.
The verifier gained a read-only `SourceWorkReader` option for the statically
registered `GUARD_SYNC_ORDER`; independent Farm/Player Actor routing and the
existing workflow repository remain separate. No second workflow, dispatcher,
timer family, Redis key family, or global worker path was introduced.

## Monotonic and replay behavior

| Input relative to Farm authority | Durable result | Effective guard |
|---|---|---|
| higher `guard_version` | `APPLIED` | full incoming snapshot |
| lower, previously unseen version | `NO_CHANGE / SUPERSEDED` | unchanged |
| exact invocation + exact payload | original receipt replay | unchanged |
| same version + different hash | fail closed / idempotency or invariant conflict | unchanged |

The Farm canonical hash now uses the same fixed-order, big-endian, full
SHA-256 layout as `internal/player/pet`. Expiry is delivered as a newer
snapshot with no active guard (or an expired deadline); no per-farm timer was
added.

On activation/reload, the actor scans its durable bounded Participant proofs
for the latest GuardSync receipt and restores the complete effective snapshot.
The activation-only sidecar reader remains a compatibility input, but its
setter is monotonic and cannot overwrite the authority-restored version with
an older or conflicting snapshot.

## Required proof mapping

1. Hot v0 -> Apply v1 -> immediate Theft observes v1:
   `TestGuardSyncHotActorImmediateTheftUsesV1` (Memory and production Redis
   Farm authority adapters).
2. Active v1 -> disabled/expired v2 -> immediate Theft no longer intercepts:
   `TestGuardSyncDisabledV2ImmediateTheftAndStaleV1CannotRegress`.
3. v2 followed by an unseen stale v1 cannot regress:
   `TestGuardSyncPreviouslyUnseenStaleV1IsSupersededByV2`.
4. Same-version conflicting hash fails closed:
   `TestGuardSyncSameVersionConflictFailsClosedAndKeepsV2`.
5. Passivation/reload preserves v2 and its replay result:
   `TestGuardSyncPassivationReloadPreservesV2AndOperationReplay`.
6. Route/writer fencing and same-operation idempotency:
   `TestGuardSyncWriterEpochFenceRejectsOldHolder` and
   `TestFarmParticipantServerGuardSyncProductionPathReplayAndSourceFence`.
7. Production-composition Redis probe:
   `TestGuardSyncRedisProductionCompositionHotActorThenTheft` commits real
   source-slotted GuardSyncOrders through the production Redis authority
   adapter, applies active v1 through the coordinator endpoint, proves the
   next Theft is blocked, claims disabled v2 with the existing Redis work
   lease, applies it through the worker-recovery endpoint, and proves the next
   Theft is no longer intercepted. The same Redis adapter also persists the
   hot Farm actor.

The production-composition probe uses miniredis as the equivalent Redis server
and exercises the production key builder, source-order hashes, authority
scripts, Redis work lease, Redis TIME read, Participant admission, Farm
authority CAS, mailbox, and Theft decision. It does not claim a company Redis
Cluster/failover platform result.

## Verification executed

All commands completed with exit status 0 in the assigned worktree:

```text
go test ./internal/farm/domain ./internal/farm/actor ./internal/farm/ingress \
  ./internal/workflow/participantproof ./internal/storage/authoritystore \
  ./internal/storage/redis/authority ./cmd/farm-svr -count=1

go test -race ./internal/farm/actor ./internal/farm/ingress \
  ./internal/workflow/participantproof ./internal/storage/redis/authority -count=1

go test ./... -count=1
go vet ./...
git diff --check
```

No gameplay rule, public protocol/generated API, design contract, or capacity
harness file was edited.
