# RT2C Route Control Service Handoff

- task_id: `RT2C-ROUTE-CONTROL-TRANSPORT`
- status: `COMPLETED`
- base_commit: `bfaa62b8c5bc51bb68c885e9921af671d1d4bcc7`
- branch: `codex/rt2c-route-control-service-20260808`
- implementation_commit: signed closure commit SHA is recorded by the final delivery
- push: `NOT DONE`
- merge/integration: `NOT DONE`

## Scope

For this closure continuation, owned writes are limited to:

- `internal/routecontrol/transport/**`
- this handoff

The earlier RT2C baseline already contained the proto, snapshot hub, and
route-controller work described below. This continuation does not modify
those paths, A/B/Q files, Redis scripts, Manager, existing production command
roots, deploy files, public proto files, `go.mod`, `go.sum`, or the proto
generation script.

## Frozen C-to-D Interface

The internal proto contains exactly one read-only service:

```text
routecontrol.RouteSnapshotService
  GetSnapshot(GetSnapshotRequest) returns (RouteSnapshotEnvelope)
  GetCurrentRevision(GetCurrentRevisionRequest) returns (RouteSnapshotEnvelope)
  WatchFrom(WatchFromRequest) returns (stream RouteSnapshotEnvelope)
```

`GetCurrentRevision` returns the same complete envelope as `GetSnapshot`; it
does not return a scalar revision. The envelope fields are:

```text
schema, realm, revision, canonical_json, checksum, topology,
committed_at, controller_id, boot
```

There are no patch, public, H5, mutation, Holder, or Plan RPCs.

RT2D may consume `api/gen/routecontrol` and
`internal/routecontrol/transport.SnapshotClient`, which implements B's
`RemoteSnapshotSource`:

```go
FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error)
WatchFrom(context.Context, uint64) (<-chan actorrouting.RouteSnapshot, error)
```

Every received envelope is decoded by A's
`DecodeStoredRouteSnapshot`. The client also requires the expected controller
ID, pins the first non-empty controller boot, and pins/checks realm and
topology. B's local resolver remains a zero-RPC hot path.

## Implementation

- `SnapshotWatchHub`
  - loads the durable current envelope during startup and restart;
  - serializes durable reconciliation independently from the state mutex and
    applies loaded state in a second phase, so Close, cancellation cleanup, and
    diagnostics do not wait behind Redis;
  - rejects durable rollback, same-revision envelope/shape conflicts,
    topology/layout changes, and disappearance after a found key;
  - validates every higher revision as a complete durable envelope with
    fixed topology/layout and monotonic revision only; it does not run the
    authority exactly-one activation validator or duplicate RT2B per-shard
    generation state-machine checks, so skipped revisions with multiple
    ASSIGNING -> ACTIVE activations remain acceptable to the durable projection;
  - uses bounded client count and per-client queues;
  - deterministically disconnects a queue-full slow consumer;
  - shares immutable internal revision objects across fanout queues and clones
    only when a subscriber consumes a value;
  - re-reads the durable store during notification and exact-matches the full
    canonical envelope before fanout;
  - rejects candidate Notify while the durable store is empty without
    fanout, while allowing the initial Hub to accept a later Store Bootstrap
    followed by the matching Notify;
  - runs bounded periodic reconciliation so a committed store write with a
    failed notification is eventually delivered without a request hot path;
  - exposes ordinary and Authority notifier wrappers as separate capabilities;
  - clones stored bytes and nested RouteSnapshot data at every boundary;
  - closes subscriptions on context cancellation or Hub close without a
    history buffer.
- Authenticated transport
  - strict TLS 1.3-or-newer mTLS helpers with verified client chains;
  - requires a non-empty canonical, unique client-identity allowlist;
  - accepts exactly one client URI SAN or one client DNS SAN and rejects
    URI+DNS ambiguity, multiple SAN identities, and CN fallback;
  - server peer identity comes from gRPC TLS `VerifiedChains`, not metadata or
    payload role fields;
  - server controller ID and boot are construction-time values;
  - production `RemoteSnapshotSource` construction is TLS `Dial` only;
    arbitrary-connection construction is package-private and requires an
    explicit owned close function;
  - client message size, watch queue, concurrent RPC, and unary timeout bounds;
  - maps durable rollback/conflict/missing/contract integrity failures and
    wrapped route validation errors to `FailedPrecondition`;
  - client watch contract errors latch permanently and cancel every active
    watch; transient network and backpressure errors remain recoverable
    diagnostics;
  - Client Close cancels active watch contexts, unblocks semaphore waiters,
    and returns close-function errors sticky;
  - server stream sends use a bounded timeout and map timeout/capacity to
    `ResourceExhausted`;
  - certificate identities reject all IP and email SAN identities, mixed SAN
    identity ambiguity, IP-literal allowlist entries, and IP-literal
    `ServerName`/expected server identities;
  - gRPC server max send/receive message bounds;
  - no insecure credential fallback;
  - multi-service construction snapshots every `grpc.ServiceDesc` before
    retaining it: `Methods` and `Streams` are copied, while `Metadata` is
    restricted to `nil` or immutable `string`; mutable or unknown metadata
    values are rejected as invalid configuration;
  - every collected implementation is non-nil, implements its descriptor
    interface, and every method/stream has a canonical unique name, non-nil
    handler, canonical full method, and non-empty exact ACL;
  - every copied unary/stream handler performs its own exact
    `MethodAuthorizer.Authorize` before entering the original handler. The
    gRPC interceptor remains defense-in-depth, so a custom unary handler that
    ignores its interceptor cannot bypass authorization;
  - each registration callback receives only a scoped construction registrar.
    The collector mutex linearizes registration, callback counts require at
    least one new service per callback, callback scopes close on return, and
    the final seal atomically closes the collector. Retained or concurrent
    late registrars are no-ops and cannot race with or mutate sealed entries;
  - final registration into the real `grpc.Server` is panic-contained. Any
    registration failure returns `nil, error`; a partially registered server
    is never returned as a runnable sealed wrapper.
- `cmd/route-controller`
  - requires controller ID, boot, Redis URL/namespace, and server
    certificate/key/client-CA paths;
  - requires and passes the canonical client-identity allowlist;
  - refuses startup when the durable snapshot is physically empty; bootstrap
    remains an explicit earlier-stage operation;
  - constructs only the existing Redis snapshot store, Hub, and read-only
    authenticated gRPC server;
  - does not start Manager, Plan, Holder, mutation readiness, or HA logic;
  - handles SIGINT/SIGTERM with Hub close and bounded graceful shutdown;
  - does not log credentials or endpoint values.

## Single-Controller Boundary

This task implements one explicitly constructed Route Controller identity and
boot. It does not implement leader election, an exclusive controller lease,
automatic failover, multi-controller split-brain safety, or HA. The deployment
must remain a single-controller/read-only transport shell until a later task
freezes those semantics.

The service has no mutating readiness signal and no mutation RPC. Redis
snapshot CAS and the existing Manager remain the only publication authority;
this task only exposes a durable complete-snapshot read/watch projection.

## Verification

Passed in this worktree:

```text
GOCACHE=/tmp/rt2c-gocache go test ./internal/routecontrol ./internal/routecontrol/transport ./cmd/route-controller
GOCACHE=/tmp/rt2c-gocache go test -count=20 ./internal/routecontrol ./internal/routecontrol/transport ./cmd/route-controller
GOCACHE=/tmp/rt2c-gocache go test -race -count=1 ./internal/routecontrol ./internal/routecontrol/transport ./cmd/route-controller
GOCACHE=/tmp/rt2c-gocache GOFLAGS=-buildvcs=false go vet ./...
GOCACHE=/tmp/rt2c-gocache go test -buildvcs=false -run '^$' ./...
GOCACHE=/tmp/rt2c-gocache go build -buildvcs=false ./cmd/...
./scripts/gen-proto.sh
git diff --check
```

The focused tests cover proto/schema and wire golden, allowlist and shell
empty-snapshot rejection, durable rollback/conflict/topology/disappearance,
skipped revisions with multiple activations, store-committed notification
failure compensation with periodic reconciliation, subscribe/notify races,
restart loading, duplicate/stale notifications, shared immutable fanout and
slow consumers, blocked stream cancellation, malformed
schema/checksum/topology/canonical JSON, controller/boot/realm/topology/
capacity terminal latching, Hub integrity failure through watch to sticky
valid-Fetch rejection, TLS identity ambiguity including IP/email SANs,
IP-literal identity rejection, TLS 1.2 rejection, unauthenticated rejection,
client Fetch/Watch, size bounds, capability separation, Client Close/close
errors, send timeout convergence, status mapping, and context cleanup.
The closure tests additionally cover descriptor snapshot and immutable string
metadata acceptance, mutable/unknown metadata rejection, nil/typed-nil
implementations and handlers, invalid/duplicate method and stream names,
implementation-interface mismatch, missing method ACLs, handler authorization
independent of a custom handler's interceptor behavior, callback valid+noop
rejection, retained/concurrent late registrar calls, sealed-wrapper capability
separation, and raw DER high-tag EOF table/fuzz regressions.

The requested full runtime test passed in this continuation:

```text
GOCACHE=/tmp/rt2c-gocache go test -buildvcs=false -count=1 -timeout=600s ./...
```

All packages passed, including RT2C, `cmd/route-controller`, loopback-based
suites, and Redis/miniredis tests. No real Redis endpoint or shared secret was
used.

## Final P2 Closure

This closure started from exact clean HEAD
`bfaa62b8c5bc51bb68c885e9921af671d1d4bcc7`. The review reported no P0/P1 and
three P2 findings; all three are closed here:

- `forceStreamAuthorization` now uses the existing `isNilValue` check, so a
  typed-nil `grpc.ServerStream` fails closed before `Context` is called.
- The reflective metadata deep-copy graph was removed. Service registration
  accepts only `nil` or builtin `string` metadata; every map, slice, pointer,
  struct, and other unknown value is rejected as invalid configuration.
- The former mutable-metadata isolation test now verifies immutable string
  metadata, with construction-time rejection coverage for mutable/unknown
  metadata types.

The final closure replay passed:

```text
GOCACHE=/tmp/rt2c-gocache go test -count=20 ./internal/routecontrol ./internal/routecontrol/transport ./cmd/route-controller
GOCACHE=/tmp/rt2c-race-cache go test -race -count=1 ./internal/routecontrol ./internal/routecontrol/transport ./cmd/route-controller
GOCACHE=/tmp/rt2c-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...
GOCACHE=/tmp/rt2c-vet-cache GOFLAGS=-buildvcs=false go vet ./...
GOCACHE=/tmp/rt2c-build-cache go build -buildvcs=false ./...
git diff --check
```

## Rollback

Revert the single implementation/handoff commit. Do not revert or modify
unrelated worktree paths. No push or merge was performed by this Writer.

## RT2D Holder Dependency Seam

This continuation adds only the reusable transport seam required by RT2D; it
does not implement a Holder service, Holder protobuf, mutation workflow, or
runtime composition.

- `transport.VerifiedPeerIdentity(context.Context)` returns an identity only
  from a completed TLS 1.3-or-newer handshake with `VerifiedChains` and exactly
  one accepted URI SAN or DNS SAN. CN, metadata such as `caller_role`, and
  request payload fields are not identity sources.
- `transport.NewMethodAuthorizer(map[string][]string)` constructs an immutable
  exact `fullMethod -> canonical peer identities` ACL. `MethodAuthorizer` copies
  the input map and slices, rejects missing/empty rules, noncanonical or
  duplicate identities, and has fail-closed unary and stream interceptors.
- `transport.NewGRPCServer` remains the compatibility constructor for the
  single RouteSnapshot service only. Its raw `*grpc.Server` result is not a
  Holder or other multi-service composition path.
- RT2I multi-service composition must use
  `transport.NewMultiServiceGRPCServer` with a non-nil
  `GRPCServerOptions.MethodAuthorizer` and one or more construction-time
  callbacks typed as `func(grpc.ServiceRegistrar)`. The constructor snapshots,
  validates, and seals RouteSnapshot plus all callback registrations before
  returning a `transport.SealedGRPCServer`, which exposes only runtime
  lifecycle methods (`Serve`, `GracefulStop`, `Stop`, and `GetServiceInfo`) and
  does not implement `grpc.ServiceRegistrar` or expose the raw server.
- Multi-service construction rejects nil/panicking/no-op callbacks, duplicate
  service names, nil implementations/handlers, invalid or duplicate
  method/stream/full-method names, implementations that do not satisfy the
  descriptor interface, missing per-method ACLs, and any mismatch between each
  of the three RouteSnapshot method ACL sets and
  `RouteSnapshotServer.allowedPeers`. Each copied handler also authorizes its
  exact full method before invoking the original handler. This exact equality
  prevents the interceptor, forced handler authorization, and RouteSnapshot
  method-body check from silently taking an intersection. Standalone
  RouteSnapshot serving continues to enforce its existing service allowlist.
- Passing more than one `GRPCServerOptions` value to `NewGRPCServer` or
  `Serve` is a configuration error; no value is silently ignored.

RT2D owns the future Holder proto, service implementation, exact Holder method
constants, and composition call site. Its mutating Holder methods must be
listed explicitly in the authorizer with the canonical TLS peer identity that
is allowed to invoke each method. No Holder implementation or RT2I composition
call site is present in this RT2C change; RT2I must consume only the sealed
constructor above.
