# PO1 online Plant cutover preparation handoff

Status: `PREPARATION_COMPLETE / INTEGRATION_BLOCKED`

Base: `ffb7169cfee57853cd32a3306539f72342ef0be5`

Branch: `codex/po1-plant-online-20260805`

This branch is a reusable preparation baseline. It must not be merged into
`codex/integration-20260803` until PO1-FE, PO1-AC and PO1-OR below are complete
and the combined online Plant E2E passes.

## Implemented preparation

- `internal/plantonline` composes the frozen SC1 Plant Coordinator, W31
  placement, Actor route snapshots and typed Participant clients.
- `farm-svr` maps durable generic WorkRecord states to public command states:
  non-terminal durable work returns `PROCESSING` with the same `operation_id`;
  completed work returns the committed farm/private state; rejected or
  compensated work returns a durable rejection.
- player/farm typed Participant services are registered fail-closed.
- generic WorkRecord is created before any Player/Farm effect, and WR2 remains
  the only Plant recovery owner.
- no legacy Plant WorkRecord is created by the new path.

## Blocking seams

### PO1-FE: Farm commit delivery tuple

Typed `COMMIT_PLANT` currently persists the Farm mutation but drops the
`ActionResult.Change`, recipients, activation epoch and resync tuple needed by
`FarmSubscriptionHub`. Add an implementation-level post-commit observer or
equivalent typed result bridge. Do not change public Proto and do not publish
before the Farm authority commit is proven.

### PO1-AC: Authority Registry production composition

`cmd/player-svr` and `cmd/farm-svr` still construct legacy blobstore registries.
The typed Participant admission gate therefore correctly rejects production
traffic. Compose the existing Redis AuthorityStore, DynamicFenceSource,
HolderLeaseGuard runtime and `NewAuthorityRegistry` without a static/allow-all
production fence or dual write.

### PO1-OR: generic OperationResult lookup

Public `GET_OPERATION_RESULT` does not yet resolve generic PLANT WorkRecords.
Add a read-only lookup that derives the same deterministic Plant Work identity,
returns terminal outcomes or `PROCESSING`, and never advances workflow steps.

## Validation completed for the preparation baseline

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `gofmt`
- `git diff --check`

These checks prove component compatibility only. They do not prove that the
public PLANT command is production-enabled.

## Final PO1 merge gate

After the three seams are merged into this branch, run a real-process E2E:

1. H5/public PLANT reaches farm-svr.
2. one generic PLANT WorkRecord is created before effects.
3. Player seed reservation, Farm commit and Player reservation commit complete.
4. all current farm subscribers receive one ordered FarmChange.
5. an injected interruption returns `PROCESSING`; WR2 completes it.
6. `GET_OPERATION_RESULT` with the original `operation_id` returns the terminal
   result without a duplicate seed deduction or crop.
7. restart and route-fence rejection paths remain fail-closed.
