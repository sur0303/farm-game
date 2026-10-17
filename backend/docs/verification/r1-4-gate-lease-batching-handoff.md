# CP-MVP-HARDENING-R1 / R1-4 handoff

## Candidate identity and scope

- Integration baseline: `4b10d57d84fada08a309ec5a9a8d40a2432f80aa`
  (A2 -> A1 -> A3 Integration supplied for R1-4).
- Candidate: the single atomic R1-4 commit directly above that baseline; its
  final hash is reported with the clean-worktree delivery.
- Branch: `codex/r1-4-gate-lease-batching-20260809`.
- Scope is FarmGate SessionRoute/Subscription renewal scheduling, execution,
  metrics/configuration, local evidence tooling, and lease verification docs.
  No Proto, public Social API, showcase capability, Actor/Friendship/model
  business field, H5, dynamic Route runtime, K8s, or mTLS contract was changed.

## Audited physical truth

Before R1-4, both renewal lanes scanned the complete `Gate.connections` map on
every lifecycle round and formed unbounded requests. SessionRoute executed one
pipeline containing N independent `EVAL` commands (N Lua executions/storage
updates); the configured single-endpoint Redis client used one network round
trip, while Cluster/other UniversalClient implementations may split by node.
Direct Subscription renewal executed one gRPC;
the sharded client could fan one logical call into multiple physical gRPCs and
grouped only by endpoint. Network I/O was already outside `Gate.mu`, but the
lane round mutex covered the remote wait and whole-response validation could
discard valid siblings after one malformed result.

After R1-4, lifecycle ticks do no connection-map scan. Each lane owns a global
Holder heap plus per-Holder item heaps, and one tick plans at most one bounded,
single-Holder dispatch. The physical truth remains:

- SessionRoute: one go-redis `Pipeline.Exec` per planned batch and one `EVAL`
  per item. Gatewayapp's single-endpoint `redis.Client` makes this one network
  round trip; another `UniversalClient` (for example Cluster) may split by
  node. This is not represented as one batch Lua operation.
- Subscription: one `BatchRenewSubscriptions` gRPC per planned partition.
  Planning uses one farm (a conservative holder-safe partition) without any
  resolver callback under `Gate.mu`. Sharded routing resolves shard, Holder
  ID/boot, writer epoch, lease epoch and shard revision after releasing the lock
  and rejects a mixed physical generation immediately before dispatch.
- A direct/legacy client without Holder-generation visibility remains isolated
  to one farm per batch; this weaker fact is explicit rather than presented as
  unavailable exact metadata.

## Algorithm and safety invariants

- Stable SHA-256 phases use the complete local SessionRoute or Subscription
  fence, including Gate boot. The separate dispatch partition contains the
  backend ownership generation when the routed client exposes one. Neither uses
  map order, randomness, process-global mutable state, or wall time as a hash
  seed.
- A global Holder heap plus per-Holder item heaps make registration/removal
  O(log N); hot planning examines at most `max_items*4+64` entries from one
  Holder and releases no more than one batch.
- Every non-oversize batch simultaneously obeys item and conservative estimated
  byte caps. An oversize item is emitted alone once and cannot starve successors.
  Dispatch has a validated context deadline and measured actual duration.
- `interval + max_dispatch_duration < ttl - safety_margin`; jitter is no larger
  than one bucket width. Initial remaining trust must also fit one worst-case
  phase plus dispatch. Unsafe configuration fails startup; unsafe accepted renew
  TTL never extends local trust.
- A batch contains one Gate identity/boot and one farm for Subscription; the
  physical sharded dispatch then contains one exact current ownership
  generation. Explicit old exact-generation dispatch returns per-item
  `ROUTE_STALE`; normal Gate dispatch always re-resolves after releasing the
  lock.
- Executors revalidate the complete canonical generation/deadline before and
  after I/O. Success, lost/stale/expired and transient/unknown are applied per
  item. Aggregate pipeline/RPC error remains visible while valid siblings still
  apply. Transient retry preserves only the old deadline.
- Disconnect/Kick/Switch/cancel/termination removes the exact indexed key.
  Drain stops and clears both planners under `Gate.mu`; late results cannot
  register or resurrect work, and shutdown waits for all lease loops.

## Configuration and observability

The full environment table, ranges, defaults and safety proof are in
`docs/design/80-farm-gate-lease-batching.md`. Wiring is explicit in
`internal/gatewayapp/app.go`; it adds no dev/fault/auth capability and does not
relax the showcase profile.

Low-cardinality proof metrics use only closed `lane`, `outcome`, status, phase,
or reason labels. `farm_gate_lease_batch_events_total` covers planned,
attempted, succeeded, partial, lost, transient and oversize. Batch items,
estimated bytes, dispatch duration, backlog, due lag, Holder isolation, stale
non-resurrection and drain are separately observable. No session, player,
actor, farm, connection, or subscription identifier is a label.

## Verification record

PASS:

- `gofmt` and `git diff --check`.
- Focused: `go test -buildvcs=false ./internal/gateway/leasebatch ./internal/gateway/session ./internal/gateway/command ./internal/gatewayapp ./cmd/lease-batching-evidence`.
- Race: `go test -buildvcs=false -race ./internal/gateway/leasebatch ./internal/gateway/session ./internal/gateway/command`.
- Static: `go vet -buildvcs=false ./...`.
- Formal: `./scripts/test-lease-batching-evidence` generated
  `docs/verification/results/r1-4/lease-batching-evidence.json` with status
  `PASS`. It machine-checks 100,000 SessionRoute plus 100,000 Subscription
  items, stable distribution/replay, jitter, item/byte/duration limits, due lag,
  no starvation, oversize isolation, partition isolation, rollback/jump and
  drain clear/stop. Its lifecycle model executes close and handoff after an item
  becomes in-flight, rejects both old applies through one canonical adjudicator,
  accepts the new generation, and verifies post-drain registration rejection.
  These model checks are not represented as a real Gate E2E; the focused
  production tests separately cover concurrent Disconnect versus late success,
  scheduler drain, and Sharded exact-generation handoff. The Subscription lane
  is spread over 500 farm/Holder-safe partitions and models the production 10ms
  scheduler; its machine threshold is 5.1s maximum due lag.

INCONCLUSIVE (not PASS):

- `go test -buildvcs=false ./...` exited 1 because this sandbox denied loopback
  `listen tcp 127.0.0.1:0` to diagnostics and miniredis-backed packages. The
  retained complete log from the first full-tree attempt and its structured
  status are
  `docs/verification/results/r1-4/go-test-all.log` and
  `go-test-all-result.json`; the final candidate rerun reproduced the same
  listener denial. No R1-4 focused-package code failure was observed.
- `./scripts/test-lease-renew-stress` exited 2 before topology startup because
  `deploy/secrets/redis.conf.local` was unavailable. The requested topology is
  still 1 owner + 199 viewers and no mock was substituted. See
  `lease-renew-stress-real.log` and `lease-renew-stress-real.json`.

## Deliberately not claimed

No staging, Docker, real Redis, real-port 200-subscription, production capacity,
or deployment evidence was run in this workspace. Those remain external
verification tasks; neither INCONCLUSIVE command above is represented as PASS.
