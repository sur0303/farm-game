# TH3B Theft Admission / Ingress Handoff

Date: 2026-08-05

## Delivered

- Public `THEFT` now enters the Pair ActionAdmission path from the trusted
  FirstHop. It derives viewer from `Trusted.Session.PlayerId` and target from
  the catalog-verified route key; client payload is limited to the registered
  `plot_id`, `target_farm_id`, and `crop_cycle_id` fields.
- The ingress calls `CommitFriendActionAdmission` with the operation identity,
  canonical payload hash, viewer/owner/farm, `ActionScopeTheft`, and a target
  binding hash covering viewer, owner, farm route, plot, and crop cycle. Pair
  remains the authority for relation epoch and the durable receipt/permit/
  dispatch/result/due anchors.
- Unknown commit responses are resolved once with
  `GetFriendActionAdmission` using the same identity and payload hash. The
  ingress never re-issues a permit after a lost response.
- `TheftDispatchPort` accepts only the persisted dispatch-order ID. Dispatch
  errors do not change the admitted response: the public result remains
  `PROCESSING` and worker/due-index recovery owns progress.
- Current-subscription THEFT FirstHop validation now requires the admitted
  subscription ID and all session fence dimensions: player, active session
  epoch, gateway ID/boot ID, connection ID/generation, and session route
  revision.
- Legacy `PrepareTheft` is behind the explicit
  `FARM_THEFT_LEGACY_ROLLBACK_ENABLED` switch and is false by default.

## Production composition

`cmd/farm-svr` constructs the real Redis Friendship Pair/Quota/Share
repositories from the versioned Redis namespace and preloads their scripts.
The Pair repository is wired into the new ingress. This tree does not yet
contain the concrete Workflow/Participant dispatch port, so no memory/mock
implementation is injected: composition logs the missing dependency and
`/readyz` remains unavailable. There is no implicit legacy fallback.

## Verification

Passed:

```text
go test ./cmd/farm-svr ./internal/farm/ingress ./internal/logic/trusted ./internal/friend
go test ./...
go vet ./cmd/farm-svr ./internal/farm/ingress ./internal/logic/trusted
```

Covered cases include admission before/after relation deletion, accepted
replay after deletion, response-loss recovery through `GetFriendActionAdmission`,
same-operation same-payload replay, different-payload conflict, complete
subscription/session fence rejection, dispatch loss returning `PROCESSING`,
legacy default-off behavior, and missing production dependencies failing closed.

No files under `docs/design/**` were changed. Farm/Player Participant and
Workflow state-machine implementation remain out of scope for TH3B.
