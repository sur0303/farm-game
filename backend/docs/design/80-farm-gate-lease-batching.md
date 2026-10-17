# FarmGate lease batching (R1-4)

Status: implemented from Integration baseline
`4b10d57d84fada08a309ec5a9a8d40a2432f80aa`.

## Scope and preserved contracts

This change only hardens the lifecycle implementation of the existing
SessionRoute and Farm Subscription leases. Authority, complete Route fences,
Ready, recovery, break-before-make SwitchFarm, Subscription Ready, local
fail-closed admission, and WebSocket drain behavior remain the business
contracts. No public Proto/Social API, showcase capability, actor model, or
dynamic routing contract is added.

## Audited pre-R1-4 physical truth

- `RenewSessionLeases` held a round mutex, scanned every `Gate.connections`
  entry under `Gate.mu`, then passed all active routes to `RedisAuthority`.
  Redis queued one `EVAL` per route in one unbounded pipeline. With the current
  single-endpoint `redis.Client` this was one pipeline network round trip, but
  N Lua executions and N independent Redis hash updates—not one batch Lua
  operation. A Cluster/other `UniversalClient` may split a pipeline by node.
- `RenewSubscriptionLeases` also scanned all connections under `Gate.mu` and
  built one unbounded logical `BatchRenewSubscriptions` request. A direct
  farm endpoint performed one gRPC. `ShardedLogicClient` resolved every farm
  and fanned out by endpoint, so one Gate call could be multiple physical
  gRPCs. Before R1-4 it grouped only by endpoint, not Holder boot/writer/lease
  epoch.
- Both network calls were outside `Gate.mu`, but each lane's round mutex covered
  the entire remote wait. Whole-response validation meant one malformed item
  could prevent application of unrelated successful items.
- Disconnect, Kick, SwitchFarm, PendingJoin cancellation and termination used
  complete local fences, so late responses could not rewrite a replaced
  connection. There was no bounded scheduling index, and app ticker shutdown
  was deferred until after the HTTP server returned rather than linearized at
  the start of drain.

## Stable scheduling

Each lane owns a global holder-safe-partition min-heap plus one indexed item
min-heap per partition. SessionRoute uses the exact Gate ID/boot partition;
Subscription uses one farm per partition, then resolves and revalidates the
exact Farm Holder generation outside `Gate.mu` immediately before I/O.
Registration/removal is O(log N); one hot tick examines at most
`max_plan_scan = max_items*4+64` items from one partition and no unrelated
items. Hot ticks never scan `Gate.connections`, call a route resolver while
holding `Gate.mu`, or construct an unbounded pipeline/request.

The stable identity is:

- SessionRoute: Gate ID + Gate boot ID + connection ID/generation + player ID
  + session ID/epoch + route revision.
- Subscription: phase (Pending/Active) + Gate ID/boot + connection/generation
  + subscription context generation + farm/subscription/join-attempt/presence
  fence.

SHA-256 is used as a deterministic distribution function. The first word
selects a stable bucket; the second selects jitter in `[0,max_jitter]` inside
that bucket. Neither map iteration, random state, a global mutable seed, nor
`time.Now` participates in the hash. Wall time only selects the next occurrence
of the already-determined phase, while `now.Add` preserves Go's monotonic
component. Planner time rolls backward only by clamping to its last observation;
large jumps still release at most one bounded batch.

Configuration and initial/renewed lease admission are accepted only when:

`renew_interval + max_dispatch_duration < ttl - safety_margin`

and `max_jitter <= renew_interval / bucket_count`. An initial lease whose
remaining local deadline cannot hold one worst-case phase plus dispatch fails
closed. An unsafe renewal TTL is handled as transient/unknown and never extends
local trust. A transient retry is clamped to the old deadline; it cannot form a
tight loop or extend authority.

## Triple-bounded planning and execution

Every batch is simultaneously bounded by item count, conservative estimated
wire/storage bytes, and a dispatch context deadline. Session estimates account
for the Redis key, RESP framing, eight Lua arguments and decimal expansion.
Subscription estimates use deterministic Proto size plus request/framing
overhead.

An item above `max_bytes` is emitted exactly once as a one-item oversize batch
and increments `events_total{outcome="oversize"}`. It cannot block later normal
items. The executor performs I/O only after planning and canonical-fence
validation, outside `Gate.mu`.

Physical calls remain honest:

- SessionRoute: one go-redis `Pipeline.Exec` per planned batch and one
  `EVAL`/Lua execution per item. With gatewayapp's configured single-endpoint
  `redis.Client` this is one network round trip; a different
  `UniversalClient` such as Redis Cluster may physically split by node. Redis
  command results are parsed independently.
- Subscription: one `BatchRenewSubscriptions` gRPC per batch. Planning is
  conservatively isolated to one farm, which has one current Holder. After
  releasing `Gate.mu`, sharded routing resolves shard + Holder ID/boot + writer
  epoch + lease epoch + shard revision immediately before dispatch and rejects
  any mixed physical generation. Explicit exact-generation callers also get
  per-item `ROUTE_STALE` after handoff. Direct/legacy clients retain the same
  one-farm isolation without claiming unavailable Holder metadata.

## Per-item state and lifecycle fences

- Success updates only an exact still-admissible generation and schedules its
  next stable phase.
- NOT_FOUND/STALE_FENCE/CLOSED, Redis fence reject, and explicit expiry remove
  the item and use the existing stale/expiry/rejoin/cleanup semantics.
- Transient, unavailable, route-stale, missing, malformed, or unknown results
  preserve only the old deadline and retry with bounded backoff.
- A malformed/missing item cannot prevent valid siblings from applying.
- Disconnect/Kick/Switch/cancel/termination removes the exact schedule key
  while holding `Gate.mu`. A late response rechecks connection generation,
  route/session/subscription context, old deadline and terminal state.
- `StopLeaseRenewalSchedulers` is the drain fence: it rejects registration,
  clears both heaps, prevents late results from applying, and app shutdown waits
  for the three lease goroutines to exit.

## Environment configuration

Durations use Go duration syntax. Malformed/non-positive durations and malformed
integers fall back to the listed safe defaults; explicitly parsed integers or
durations outside the ranges/invariants fail startup (zero jitter is allowed).
Item range is 1–4096, byte range is 256–67,108,864, bucket range is 1–4096,
and scheduler tick range is 1ms–1s. The existing Subscription TTL remains
bounded to `[1ms, 4,294,967,295ms]`, matching its `uint32` millisecond wire
field.

| Environment variable | Default | Meaning |
|---|---:|---|
| `GATE_LEASE_SCHEDULER_TICK` | `10ms` | bounded due-work poll; not the renew phase |
| `GATE_ROUTE_LEASE_RENEW_INTERVAL` | route TTL / 3 | Session stable phase period |
| `GATE_ROUTE_LEASE_BUCKETS` | `64` | Session stable buckets |
| `GATE_ROUTE_LEASE_MAX_JITTER` | half bucket width | Session deterministic jitter cap |
| `GATE_ROUTE_LEASE_BATCH_MAX_ITEMS` | `256` | Session item cap |
| `GATE_ROUTE_LEASE_BATCH_MAX_BYTES` | `524288` | Session estimate cap |
| `GATE_ROUTE_LEASE_MAX_DISPATCH_DURATION` | `3s` | Redis pipeline deadline |
| `GATE_ROUTE_LEASE_TRANSIENT_BACKOFF` | `250ms` | Session unknown-result retry |
| `GATE_SUBSCRIPTION_LEASE_RENEW_INTERVAL` | subscription TTL / 3, minimum `1s` | Subscription stable phase period |
| `GATE_SUBSCRIPTION_LEASE_BUCKETS` | `64` | Subscription stable buckets |
| `GATE_SUBSCRIPTION_LEASE_MAX_JITTER` | half bucket width | Subscription deterministic jitter cap |
| `GATE_SUBSCRIPTION_LEASE_BATCH_MAX_ITEMS` | `256` | Subscription item cap |
| `GATE_SUBSCRIPTION_LEASE_BATCH_MAX_BYTES` | `524288` | Subscription estimate cap |
| `GATE_SUBSCRIPTION_LEASE_RENEW_TIMEOUT` | `3s` | Holder gRPC dispatch deadline (existing env retained) |
| `GATE_SUBSCRIPTION_LEASE_TRANSIENT_BACKOFF` | `250ms` | Subscription unknown-result retry |

The existing lease TTL and safety-margin variables remain unchanged. None of
these settings enables a dev, fault, auth, or showcase-only capability.

## Metrics

All labels are closed, low-cardinality sets. No session, player, actor, farm,
connection, or subscription identifier is a label.

- `farm_gate_lease_batch_events_total{lane,outcome}`: planned, attempted,
  succeeded, partial, lost, transient, oversize.
- `farm_gate_lease_batch_items{lane}` and
  `farm_gate_lease_batch_estimated_bytes{lane}`.
- `farm_gate_lease_batch_dispatch_duration_seconds{lane}`.
- `farm_gate_lease_batch_backlog{lane}` and
  `farm_gate_lease_batch_due_lag_seconds{lane}`.
- `farm_gate_lease_batch_holder_isolated_total{lane}`.
- `farm_gate_lease_batch_stale_non_resurrection_total{lane}`.
- `farm_gate_lease_batch_drain_total{lane}`.

Existing Subscription lease metrics are retained for dashboard/tool
compatibility.
