# PO1-FE FarmChange handoff

Status: `IMPLEMENTED / READY FOR INTEGRATION`

## Changed files

- `internal/farm/actor/participant.go`
  - Adds the implementation-only `CommitPlantTurnResult` bridge.
  - The applied `COMMIT_PLANT` path now retains the authoritative typed
    `ActionResult`: snapshot result, exact candidate `FarmChange`, recipients,
    activation epoch, and resync notices.
  - The existing local `FarmSubscriptionHub` handoff runs once in the
    serialized FarmActor turn after the authority CAS succeeds. Direct legacy
    `CommitPlant` callers retain their previous error behavior for a failed
    local handoff.
  - Receipt replay and rejected/no-change receipt writes return no ActionResult.

- `internal/farm/ingress/participant_server.go`
  - Adds the smallest composition option,
    `Options.CommitPlantObserver CommitPlantObserver`.
  - The observer is called only after `SubmitParticipant` returns, only for a
    newly applied result with a non-nil FarmChange, and never for replay,
    rejection, no-change, or timeout/noncommitted outcomes.
  - The typed Participant response and its canonical result hash are returned
    independently of delivery outcome.

- `internal/farm/ingress/service.go`
  - Adds `Service.EmitChange`, a thin exported adapter over the existing
    `emitChange` path so the external composition root can pass
    `service.EmitChange` as `CommitPlantObserver`.

- `internal/farm/ingress/participant_server_test.go`
  - Adds publication, replay, downlink rejection/resync, observer predicate,
    noncommitted, and callback mailbox-boundary coverage.

No Proto, generated code, routing, workflow, storage, Player, or command-root
files were changed.

## Invariants closed

1. The candidate FarmChange is constructed from the same committed candidate
   state and is handed to the existing FarmSubscriptionHub exactly once.
2. The authority CAS proves the business commit before the change handoff.
   Hub work is local and ordered; the observer runs after the FarmActor mailbox
   turn, so Gate/network downlink work is not performed in the mailbox.
3. Durable receipt replay is resolved before the handler turn and has no
   ActionResult, observer call, or second FarmChange.
4. Hub-lane saturation uses the existing invalidate/resync fence. Downlink
   rejection uses the existing exact-presence `REJOIN_REQUIRED` path. Neither
   can roll back the crop or turn an applied Participant result into a clean
   business rejection.
5. Rejected/no-change paths persist only their receipt and do not advance
   FarmVersion or publish a FarmChange.
6. Legacy Plant/Care/Theft publication and typed Participant result hashes are
   unchanged; the bridge is implementation-level Go state only.

## Verification

Passing commands:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `git diff --check`

Focused tests prove:

- two active subscriptions each receive exactly one ordered version-1 change;
- replay returns the same typed bytes/hash and adds no observer call or
  downlink item;
- a rejected change enqueue produces `REJOIN_REQUIRED` while the committed
  crop, FarmVersion, and receipt remain present;
- rejected, no-change, replay, missing-action, and noncommitted predicates do
  not invoke the observer;
- the observer can submit a follow-up actor task, proving it runs after the
  mailbox turn; focused actor/ingress race tests are clean.

## Remaining risks / integration action

The final PO1 composition still needs to pass `service.EmitChange` in the
`CommitPlantObserver` option when constructing the production typed Farm
Participant server. Command roots were intentionally left untouched in this
worktree. An unexpected Hub implementation error for which both the normal
handoff and its existing invalidation fallback fail leaves the committed
delivery guard for the existing lifecycle recovery path; it does not alter the
business result.
