# Theft Gate ServiceLB routing fix — 2026-08-05

## Result

The static Authority Gate now selects `ACTOR_AFFINITY` versus `SERVICE_LB`
from the frozen `CommandCatalog` entry. `GET_OPERATION_RESULT` reaches the
separately configured `player-svr` ServiceLB client with its original trusted
context, deadline, request/operation identity, trace and payload, and without
an `ActorRouteHeader`.

ActorAffinity still uses the existing Route Domain, VirtualShard, assignment,
holder and completed server-owned route-header path. A missing or failing
ServiceLB destination returns the internal `ErrServiceLBUnavailable` category
and the existing public boundary maps it to `UNAVAILABLE /
ERR_STORE_UNAVAILABLE / RETRY_SAME_OPERATION`. It never falls back to an Actor
holder.

## Root cause

`farm-gate` used `ShardedLogicClient` whenever `ACTOR_ROUTE_SNAPSHOT` was
configured. That client accepted only `ROUTE_MODE_ACTOR_AFFINITY` and required
an `ActorRouteHeader`. The Gate correctly constructed
`GET_OPERATION_RESULT` from its catalog entry as `ROUTE_MODE_SERVICE_LB` with
FirstHop `player-svr` and no Actor route, so the static sharded client rejected
the otherwise valid request before it reached `OperationReadAPI`.

The process composition also prohibited `PLAYER_ADDR`/`FARM_ADDR` together
with `ACTOR_ROUTE_SNAPSHOT`, leaving static Authority mode no separate
ServiceLB destination to select.

## Changed files

- `internal/gateway/command/sharded.go`
  - Looks up the immutable `CommandSpec` and branches on its route mode.
  - Leaves the existing ActorAffinity resolution and holder forwarding path
    intact.
  - Selects ServiceLB strictly by `CommandSpec.FirstHop` from a separate client
    map, rejects Actor metadata on that path, and reports a precise unavailable
    error when the destination is missing or fails.
- `internal/gatewayapp/app.go`
  - Allows `PLAYER_ADDR` and/or `FARM_ADDR` to be configured alongside the
    static Actor route snapshot and wires them as ServiceLB destinations.
  - Keeps static sharded bootstrap and Actor holder connections unchanged.
- `internal/gateway/command/sharded_test.go`
  - Adds the focused static Authority, ActorAffinity, public unavailable and
    Actor-only redirect regressions.
- `docs/verification/theft-gate-servicelb-fix-20260805.md`
  - This verification record.

No Theft/Care/Plant actor or workflow code, design contract, capacity harness,
or capacity evidence was changed.

## Verification

- `go test -run 'TestStaticAuthority(ServiceLB|ActorAffinity)' -count=1 ./internal/gateway/command` — PASS
- `go test -count=1 ./internal/gateway/command ./internal/gatewayapp` — PASS
- `go test -race -count=1 ./internal/gateway/command ./internal/gatewayapp` — PASS
- `go test ./...` — PASS
- `go vet -buildvcs=false ./...` — PASS
- `git diff --check` — PASS

The focused regressions prove:

1. Static Authority `GET_OPERATION_RESULT` calls only the configured
   `player-svr` ServiceLB client, carries no forged Actor route, and preserves
   trusted context, deadline and identities.
2. `BUY` still resolves the Player VirtualShard, selects its holder, and
   completes the same holder/revision/writer tuple before forwarding.
3. Both absent configuration and a failing ServiceLB client map to the public
   fail-closed unavailable result without Actor fallback.
4. An Actor redirect-shaped response from a ServiceLB destination is returned
   after one call; the Actor resolver and holders are never entered.

## Residual risks

- In static Authority deployments, `PLAYER_ADDR` must name the real
  service-discovery/load-balancer address for enabled player-svr ServiceLB
  commands. `FARM_ADDR` is the equivalent future farm-svr pool hook. Omitting a
  required pool intentionally leaves the Gate running for ActorAffinity while
  that ServiceLB command fails closed.
- This change reuses the existing gRPC dialing, readiness, deadline and retry
  behavior; it does not introduce or validate a new client-side balancing
  policy.
- The real 200-WSS Theft capacity harness was intentionally not modified or
  rerun in this narrow code worktree. Its next deployment run should supply
  `PLAYER_ADDR` alongside `ACTOR_ROUTE_SNAPSHOT` and confirm the production
  service-discovery endpoint.
