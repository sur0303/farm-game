# CP-VERTICAL-B B1 Backend Handoff

Date: 2026-08-08
Base: `1ae95ce`
Branch: `codex/cp-vertical-b-b1-friend-care-20260808`

## Delivered

- Append-only public wire:
  - `PublicProfileView.avatar_id=3` (`uint32`)
  - `PublicProfileView.level=4` (`uint32`)
  - `FarmSnapshot.owner_public_profile=5`
  - `PlayerFeatureSummaryView.tasks=31` with bounded daily/current-growth progress
- Player authority owns persisted display name/avatar; Web bootstrap provisions normalized username, legacy bootstrap persists Backend defaults. Level is read from the same PlayerRecord progression.
- `PlayerParticipantService.GetPublicProfile` returns only the validated public view; wallet, inventory, receipts and private task state never cross this boundary.
- Direct `PLAYER_ADDR` and static `ActorRouteClient` readers share the same validation semantics.
- HTTPS share/friend endpoints remain the only active friend product transport. Roster/opportunity/share responses use the Player profile and fail closed on dependency failure. Friend/share CommandCatalog availability is unchanged.
- Farm ingress reads the farm owner's profile outside the actor mailbox with a bounded timeout. The profile is cloned into the armed baseline snapshot; malformed/missing profiles remove the exact joining presence and cannot downlink a partial snapshot.
- Player Bootstrap projects TaskDomain ListView only; it never falls back to legacy `PlayerRecord.tasks`.
- Existing WATER, THEFT, FarmChange, reward/workflow and operation-result code paths were not replaced.

## Commits

- `c13364b proto: append CP-B profile and task views`
- `ee0f9e3 feat(player): own public profile and bootstrap tasks`
- `4bca99b feat(gateway): serve authoritative friend profiles`
- `183dc44 feat(farm): bind owner profile to join snapshot`

## Verification

Passed:

```text
bash scripts/gen-proto.sh
go test -buildvcs=false -count=1 \
  ./internal/player/actor ./internal/player/ingress ./internal/player/task \
  ./internal/publicprofile ./internal/gateway/auth ./internal/gatewayapp \
  ./internal/farm/actor ./internal/farm/ingress ./internal/farm/subscription \
  ./cmd/farm-svr ./cmd/player-svr
go test -buildvcs=false ./...
go test -buildvcs=false -race -count=1 \
  ./internal/player/actor ./internal/player/ingress ./internal/player/task \
  ./internal/publicprofile ./internal/gateway/auth ./internal/gatewayapp \
  ./internal/farm/actor ./internal/farm/ingress ./internal/farm/subscription
go vet -buildvcs=false ./internal/player/... ./internal/gateway/auth \
  ./internal/gatewayapp ./internal/publicprofile ./internal/farm/... \
  ./cmd/farm-svr ./cmd/player-svr
git diff --check
```

The Agent sandbox's first broad race attempt could not open miniredis loopback sockets. The controller reran the complete targeted race set outside that sandbox; all packages passed.

## Remaining gate

Backend unit/race gates are green. The checkpoint is not publishable until H5 sync and the one headed, real-stack, two-browser flow passes:

`login → buy → plant → harvest → share/friend → join friend → WATER → THEFT → return home`.
