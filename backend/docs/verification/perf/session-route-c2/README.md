# SessionRoute C2.1 capacity probe

Measures the **current** Direct Hint SessionRoute path:

```text
FriendIndex viewers
  -> session.RedisAuthority.BatchGetActiveRoutes (pipeline TIME + HGETALL)
  -> friend.DirectOpportunityHintTransport
  -> downlink.Dispatcher DeliverBatch (gateway capture)
```

This is an A-group probe for the existing per-friend `HGETALL` design. It does **not** implement OnlineRouteBucket and must not be used to claim 30M DAU capacity from single-node evidence.

## Owned paths

- `cmd/cap-redis/session-route-c2/**`
- `scripts/perf/cap-redis/run-session-route-c2`
- `docs/verification/perf/session-route-c2/**`

## Safety

- Runner starts an isolated ephemeral Redis container for the probe only.
- Probe never executes `CONFIG SET`, `SLOWLOG RESET`, `FLUSH*`, or broad `SCAN+DEL`.
- Player IDs are deterministic from `run_id` + cell identity; target Session keys must not pre-exist.
- Cleanup deletes only tracked Session keys whose ownership marker/generation still belong to this run.
- Global `session:authority.epoch` is never tracked or deleted.

## Matrix

| Dimension | Values |
| --- | --- |
| Friend count | `0`, `100`, `300` |
| Online ratio | `0%`, `5%`, `20%`, `50%` |
| Dispatch batch size | `1`, `10`, `100`, `300` |
| Route states | `ACTIVE`, `missing`, `expired`, `malformed` |

## Topology labels

| Field | Value |
| --- | --- |
| `topology_label` | `SINGLE_NODE_ONLY` for this harness |
| `cluster_verification` | `NOT_VERIFIED` unless a real `redis.ClusterClient` run is executed |

## Measured vs unavailable fields

Measured when the isolated Redis client path observes them:

- latency percentiles (`latency_us`)
- successful / filtered counts (`filtered_count` is the production collapsed missing/expired/malformed total)
- fixture-derived `expected_malformed` (not an observed classification)
- actual per-RPC DeliverBatch sizes
- Redis command count
- pipeline item count
- request / response wire bytes (when the counting dialer observes traffic)

Explicitly unavailable in this probe:

- `observed_malformed` / `malformed_count` — production API collapses missing/expired/malformed
- `redis_cpu_us` — INFO CPU not collected
- `slowlog` — SLOWLOG unused because RESET is forbidden
- Cluster pipeline-per-node proof — `NOT_VERIFIED`

## Run

```bash
scripts/perf/cap-redis/run-session-route-c2 smoke
scripts/perf/cap-redis/run-session-route-c2 full
```

Evidence JSON is written under `docs/verification/perf/session-route-c2/results/<run-id>/evidence.json`.

## Tests

```bash
go test ./cmd/cap-redis/session-route-c2/...
```

Required coverage includes exact-key cleanup isolation, expired/malformed fail-closed on the production keyspace, online-ratio cardinality, dispatch batch observation after dispatcher completion, and a source scan forbidding Redis admin mutations.

## Go / No-Go

- Correctness failure → process exit nonzero, `go_no_go=NO_GO`
- Correctness pass on single-node Redis → `go_no_go=GO_FOR_MEASUREMENT_ONLY`
- Never sets `supports_30m_dau=true` from this probe
