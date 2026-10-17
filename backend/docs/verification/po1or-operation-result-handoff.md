# PO1-OR Operation Result Handoff

Date: 2026-08-05

## Scope

The public `GET_OPERATION_RESULT` catalog entry is enabled and now reaches the
player-svr `OperationReadAPI` path. This handoff implements the generic PLANT
read path only. It does not add a result projection, workflow runtime,
participant call, MQ path, Hook, Proto change, generated change, or command-root
composition change.

## Read contract

`OperationResultReader` receives the subject only from
`TrustedCommandContext.Session.PlayerId`. The public payload's original command,
operation ID, farm locator, and optional opaque server locator are then handled
as follows:

1. Only `PLANT` is currently supported. Other original command types fail
   closed with `ERR_COMMAND_UNIMPLEMENTED` at Player ingress.
2. `original_operation_id` must be 1–64 printable ASCII bytes. PLANT requires
   an `original_target.target_farm_id`; peer/mail locators are rejected.
3. The reader rebuilds the exact SC1 key:

   `EffectiveIdempotencyKey(subject, PLANT, original_operation_id, FARM_ACTOR/target_farm_id)`

   and derives the existing v2 `WorkflowID`, then validates its
   `IndependentWorkBucket(workflow_id, bucket_version)` W31 placement before
   calling only `WorkRepository.Get("PLANT", workflow_id)`.
4. A supplied `server_operation_locator` is bounded and remains an opaque hint;
   it is never parsed or used as an authorization/key source. The derived
   subject/command/target identity remains authoritative.
5. `CREATED`, `RUNNING`, and `STEP_UNKNOWN` return
   `OPERATION_LOOKUP_PROCESSING` with a clamped 100–5000 ms retry delay.
   `COMPLETED` returns a deterministic public `PlantResult` mapped from the
   durable `COMMIT_PLANT` receipt. `REJECTED` and `COMPENSATED` return a frozen
   `StoredOperationOutcome` with `REJECTED` and the durable error class.
   Missing records return `NOT_FOUND`; deadline-backed failed records return
   `EXPIRED`. Neither is treated as success.

The read path does not call `CreateIfAbsent`, `Advance`, `ClaimDue`,
`RenewLease`, `Engine`, or any participant invoker. WorkRecord remains the
source of truth, so repeated polling preserves the original operation ID and
result identity without a second durable result store.

## Composition seam

PO1 composition can wire the existing repository and placement resolver with:

```go
err := playerService.SetOperationResultSource(ingress.OperationResultReaderConfig{
    Repository:     existingWorkRepository,
    BucketResolver: existingPlacementResolver,
    BucketVersion:  existingWorkflowBucketVersion,
})
```

Alternatively, construct `NewOperationResultReader` and pass it to
`SetOperationResultReader`. No `cmd/*` root was changed in this worktree.

## Verification

Focused tests cover processing and every supported terminal state, missing
records, subject and target isolation, malformed operation IDs, repository
timeouts, repeated polling, bounded retry delay, public payload identity, and
the absence of WorkRecord version/state/lease mutation. The trusted-service
test also proves lookup works with no Farm subscription and with
`RecoveryCompleteAtAdmission == false`.

Commands run:

```text
go test ./internal/gateway/commandcatalog ./internal/logic/trusted ./internal/player/ingress
go test ./...
```

Both passed. The integration owner still needs to supply the existing PO1
WorkRepository and W31 resolver through the setter during final service
composition.
