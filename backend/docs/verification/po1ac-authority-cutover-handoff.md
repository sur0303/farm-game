# PO1-AC Authority cutover handoff

Status: `IMPLEMENTED / DISTRIBUTED-CONTROL-PLANE-CONFLICT-EXPLICIT`

This worktree composes the existing Redis ActorAuthorityStore, RT1 local
route/lease fencing, and Player/Farm `NewAuthorityRegistry` into both process
roots. It does not commit or push.

## Process selection

`ACTOR_AUTHORITY_MODE=authority` selects the new path. The authority path
requires all of the following before a registry or Participant server is
constructed:

- `ACTOR_ROUTE_SNAPSHOT`
- `ACTOR_HOLDER_POD` and a fresh `ACTOR_HOLDER_BOOT_ID`
- `REDIS_PHYSICAL_PROFILE_VERSION`, `REDIS_NAMESPACE_ENVIRONMENT`,
  `REDIS_NAMESPACE_ZONE`
- `REDIS_AUTHORITY_LAYOUT_VERSION`, `REDIS_WORKFLOW_LAYOUT_VERSION`,
  `REDIS_WORKFLOW_HASH_VERSION`, `REDIS_WORKFLOW_BUCKET_COUNT`
- `ACTOR_AUTHORITY_LEASE_TTL`, `ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN`,
  `ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL`

The current physical adapter is frozen at the v1 authority/workflow layout.
The route snapshot supplies the single realm, seeded v2 hash profile, shard
counts, and complete PLAYER/FARM assignment generations. The composition uses
that same calculator and one `rediskey.PhysicalProfile` for Redis keys,
workflow placement, route fencing, and actor authority.

An empty mode, `legacy`, or `rollback` selects the short rollback path. That
branch constructs the legacy actor blobstore and does not construct an
Authority Store, Provider, or lease runtime. Authority mode constructs no
legacy actor blobstore. The two branches therefore cannot dual-write the same
actor.

## Authority startup and admission

`internal/authoritycomposition.New` performs this sequence:

1. Load and validate one complete route snapshot.
2. Create the RT1 dynamic assignment read/watch provider and whole-snapshot
   applier, then create the atomic resolver view.
3. Create and warm the existing Redis `ScriptRegistry`, then create the
   existing Redis `authority.Store` with the same physical profile and route
   calculator.
4. Create the RT1 `DynamicFenceSource` and `HolderLeaseRuntime` registry.
5. For every shard assigned to the local holder, call only
   `RenewShardAuthority` with the exact snapshot holder/boot/assignment/writer/
   lease tuple. The returned Redis authority must be ACTIVE and match the
   snapshot exactly before a `HolderLeaseGuard` is installed.
6. Create the existing actor `authority.Provider`, then pass it to Player or
   Farm `NewAuthorityRegistry`.
7. Start the local snapshot watcher and bounded lease-renewal loop. A renewal
   or watcher failure fences the generation and makes `/readyz` return 503.

Actor turns obtain fences from the local dynamic source. That source checks the
current complete snapshot and one coherent local lease-guard snapshot; it does
not perform a Redis lease lookup on a turn. No `StaticFenceSource`, allow-all
verifier, holder writer-epoch guess, or reused boot ID is used in authority
mode.

The typed Player and Farm roots call `NewServerChecked` in authority mode.
Their proof adapters use the runtime’s dynamic route fence and immutable
placement resolver, so stale holder, boot, writer, assignment, lease, route,
or local deadline generations fail closed before mailbox admission.

## Tests added

`internal/authoritycomposition/composition_test.go` uses the repository’s
miniredis Redis harness and the real Redis Lua authority adapter. It covers:

- authority activation, Player `NewAuthorityRegistry` load/commit/reload, and
  checked Player Participant construction;
- Farm `NewAuthorityRegistry` and checked Farm Participant construction;
- stale route/lease rejection, stale boot rejection, local lease expiry, and
  restart with a new holder/writer generation;
- independent PLAYER/FARM authority keys and cross-domain fence rejection;
- explicit legacy/rollback selection and incomplete/unknown mode rejection.

Validation completed:

```text
go test ./...
go test -race ./...
go vet ./...
go build -buildvcs=false ./...
git diff --check
```

## CONTRACT_CONFLICT: distributed route source is not in this repository

The local admission composition is complete, but this worktree cannot provide
the distributed RT1 control-plane transport without inventing a new component.
The exact evidence is:

- `internal/actorrouting.AssignmentProvider` only defines
  `GetSnapshot`/`Watch`; the only dynamic implementation is the in-memory
  paired `DynamicAssignmentProvider`/`DynamicAssignmentPublisher`.
- `DynamicAssignmentPublisher` has no Redis/network transport. The process
  composition can therefore bootstrap the configured full snapshot and run the
  existing watch/applier seams, but has no cross-process source to publish a
  RouteManager transition into the holder process.
- `RouteAssignmentManager` requires a durable `TransitionRecoveryStore` and an
  `AuthorityReader`. The public Redis `authority.Store` exposes
  Claim/Renew/Transfer/Unassign but no `ReadShardWriteAuthority`, and this
  worktree has no production durable transition/tombstone adapter.

This is intentionally not papered over with a file watcher, Redis key scan,
per-turn lease read, proxy, MQ, or a second route manager. The integration
owner must supply the existing RT1 publisher/watch and durable manager recovery
adapters behind their frozen interfaces before claiming multi-process route
failover, Drain, or live holder transfer production evidence. The authority
admission path remains fail-closed when that configuration is incomplete.
