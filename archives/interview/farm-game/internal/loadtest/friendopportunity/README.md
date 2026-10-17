# Friend-opportunity realtime synthetic benchmark

This package is the plan 79 W5/R1 offline benchmark skeleton. It is deterministic
and does not connect to Redis, the company Scheduler, or a production service.
Every artifact is marked `synthetic: true`; logical Redis operations and bytes
are arithmetic cost-model values, not measured company Redis results.

## Variants

All variants replay the same trace hashes.

- `A/Pull`: immediate GET_NEXT at session arrival plus a 30-second fallback
  pull for the rest of the run horizon.
- `B1/SchedulerSafe`: the exact same 30-second fallback pulls as A, plus one
  schedule write per farm/plot future-boundary mutation, a wakeup claim and
  current Header/Facts reread for each due upsert, online hint routing, and an
  exact pull after each delivered hint.
- `B2/SchedulerEfficient`: identical to B1 except for the fallback interval.
  It defaults to 120 seconds and is configurable with
  `-efficient-fallback`.

B2 emits `poll_savings`, including saved fallback operations/bytes, scheduler
operations/bytes, net cost versus the safe Pull baseline, and booleans stating
whether the modeled Poll savings cover the modeled Scheduler cost.

## Two independent open-loop traces

`GenerateTrace` derives separate session and boundary seeds from the recorded
master seed.

1. `session_get_next` records fixed-time user-session arrivals. An accepted
   session issues an immediate GET_NEXT and remains active until the run
   horizon, issuing fallback pulls at the selected interval.
2. `farm_boundary` records fixed-time Plant, Harvest, Care, or Theft mutations
   and their future Stealable/Water/Pest boundary. Each event causes exactly one
   farm-level schedule upsert or cancel. Cancels do not claim or fan out.

Changing `arrival-rate` changes only the session trace. Boundary count, online
targets, mutation times, schedule operations, and `boundary_trace_sha256`
remain unchanged. Plot count or density can create more farm/plot boundaries,
but no boundary is multiplied by GET_NEXT requests or hint recipients.

The fixed matrix has 216 cells:

- friends: 20, 100, 300
- plots: 5, 12, 24
- opportunity density: 0%, 5%, 20%, 50%
- online rate: 1.9%, 5%, 10%
- expiry: `uniform`, `bursty`

## Open-loop lifecycle

Planned arrival timestamps are never shifted when service slows. Every planned
session is sent. A positive `max-in-flight` can reject work, while planned and
sent counts remain unchanged. Results separately record `planned`, `sent`,
`completed`, and `rejected` for the session and boundary streams.

The synthetic worker service time controls admission only; it never feeds back
into the producer's arrival rate. The default admission cap is unlimited.

## Metrics and cost accounting

Each variant reports:

- `fallback_pulls`, `exact_pulls_after_hint`, and total `pull_count`
- `schedule_upserts`, `schedule_cancels`, and `wakeup_claims`
- `authority_header_reads`, `authority_facts_reads`,
  `friend_index_reads`, and `session_route_reads`
- `online_hint_targets`, `gate_batches`, and `evaluated_friends`
- `logical_redis_ops` and `logical_redis_bytes`
- eligible/detected/realtime boundary counts, realtime detection rate, and
  boundary-level first-detection latency min/mean/p50/p95/p99/max

A fallback pull reads the friend index and evaluates each configured friend via
synthetic Header/Facts reads. A due schedule upsert claims once, rereads that
farm's current Header/Facts once, enumerates the friend index once, reads one
session route per online target, and causes one exact Header/Facts pull per
hint. Schedule writes are one per farm mutation, never one per friend.

## Usage

Run one cell with all three variants:

~~~text
go run ./cmd/friend-opportunity-bench \
  -seed 79 -friends 100 -plots 12 -density 20 -online-rate 5 \
  -expiry bursty -arrival-rate 10 -duration 5m \
  -efficient-fallback 120s -variant all \
  -out-dir /tmp/friend-opportunity-r1-v2
~~~

Run one variant with `-variant A`, `-variant B1`, or `-variant B2`.
Run the fixed 216-cell matrix with:

~~~text
./scripts/run-friend-opportunity-realtime-ab.sh \
  -matrix -seed 79 -arrival-rate 10 -duration 5m \
  -variant all -out-dir /tmp/friend-opportunity-r1-v2-matrix
~~~

## Artifacts

Each output directory contains:

- `run-config.json`: full config, cost model, seeds, fingerprints, and hashes
- `trace.jsonl`: raw metadata plus planned session and farm-boundary records
- `results.jsonl`: raw per-variant lifecycle and metric records
- `results.csv`: the same core fields in tabular form
- `summary.json`: all selected variants and the shared trace hashes
- `manifest.json`: SHA-256 of every artifact above

The adapter API is `GenerateTrace`, `RunVariant` or `RunAll`, and
`WriteArtifacts`. A future real Scheduler adapter can replay the two persisted
streams, but must record real-service evidence separately rather than relabeling
these synthetic counters.
