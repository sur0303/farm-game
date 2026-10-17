# Plan 79 W5/R1 friend-opportunity realtime benchmark handoff

## Superseded result

The earlier `A/Pull` versus `B/synthetic-shadow` smoke result is invalid and
is superseded by this revision. The old B model incorrectly charged a complete
wakeup/hint batch once per open-loop GET_NEXT request and omitted B's fallback
Pull. It therefore multiplied Scheduler cost by query volume and did not compare
equivalent safety behavior. Do not use the old smoke numbers or trace schema v1
for design or capacity decisions.

## Corrected model

This handoff provides three synthetic variants over one shared trace:

- `A/Pull`: 30-second fallback.
- `B1/SchedulerSafe`: the same 30-second fallback plus Scheduler operations,
  claims, current authority rereads, online hint fanout, exact pulls after hints,
  and realtime discovery.
- `B2/SchedulerEfficient`: the same Scheduler path as B1, with only the
  fallback interval changed; default 120 seconds, configurable.

The trace has two independent fixed-seed streams and hashes:

- user-session/GET_NEXT arrivals
- farm/plot future-boundary mutations and expiry

The boundary stream is generated without reading arrival rate. One Plant,
Harvest, Care, or Theft mutation creates exactly one farm-level schedule upsert
or cancel. A due upsert creates one claim and one current farm Header/Facts
reread; it is not repeated per GET_NEXT request or per friend. Online targets,
route reads, gate batches, and exact pulls are derived from the boundary event.

All output remains explicitly `synthetic`. Logical Redis operations and bytes
are cost-model arithmetic and are not company Redis measurements, production
capacity claims, or evidence about a real Scheduler.

## Regression invariants

Unit tests cover these model contracts:

- B1 `fallback_pulls ==` A `fallback_pulls`.
- A, B1, and B2 carry the exact same combined/session/boundary trace hashes.
- Changing arrival rate changes the session stream but not the boundary hash,
  schedule upserts/cancels, claims, or hint targets.
- Increasing density/plots can increase boundary count, but schedule writes
  remain exactly one per boundary mutation and do not multiply by query count.
- An overloaded sender keeps planned/sent open-loop volume and records rejected
  work instead of lowering the offered rate.
- Output includes both trace streams, synthetic markers, raw JSONL, and CSV.

## Metric semantics

Results separately expose:

- fallback and exact-after-hint pulls
- schedule upserts/cancels and wakeup claims
- authority Header/Facts, friend-index, and session-route reads
- online hint targets and gate batches
- evaluated friends and logical Redis operations/bytes
- planned/sent/completed/rejected counts for session and boundary streams
- eligible/detected/realtime boundary counts and first-detection latency

B2 additionally reports saved fallback Pulls and their modeled operations/bytes,
the complete Scheduler modeled cost, whether savings cover that cost, and the
net logical operations/bytes versus the 30-second safe Pull baseline.

## Corrected fixed scenarios

The following were run with seed 79, 10 open-loop session arrivals/second, a
5-minute horizon, A/B1 fallback 30 seconds, and B2 fallback 120 seconds. These
are synthetic comparisons only.

| Scenario | Variant | Fallback Pulls | Schedule upsert/cancel | Hint targets | Logical ops | p95 detection | Poll savings cover Scheduler |
|---|---:|---:|---:|---:|---:|---:|---:|
| 20 friends, 5 plots, 5% density, 1.9% online, uniform | A | 16,500 | 0/0 | 0 | 676,500 | 83 ms | n/a |
| same | B1 | 16,500 | 4/1 | 2 | 676,527 | 83 ms | n/a |
| same | B2 | 5,400 | 4/1 | 2 | 221,427 | 83 ms | ops=yes, bytes=yes |
| 100 friends, 12 plots, 20% density, 5% online, bursty | A | 16,500 | 0/0 | 0 | 3,316,500 | 94 ms | n/a |
| same | B1 | 16,500 | 192/48 | 892 | 3,320,184 | 5 ms | n/a |
| same | B2 | 5,400 | 192/48 | 892 | 1,089,084 | 5 ms | ops=yes, bytes=yes |
| 300 friends, 24 plots, 50% density, 10% online, bursty | A | 16,500 | 0/0 | 0 | 9,916,500 | 94 ms | n/a |
| same | B1 | 16,500 | 2,880/720 | 86,526 | 10,191,198 | 5 ms | n/a |
| same | B2 | 5,400 | 2,880/720 | 86,526 | 3,520,098 | 5 ms | ops=yes, bytes=yes |

In these three configured cells, B2 saves 11,100 fallback Pulls. Its modeled
Poll savings cover Scheduler cost in both logical operations and bytes. This is
a result of the serialized synthetic constants and selected workload only; it
must not be generalized to real Redis or production without measurements.

Artifacts are under `/tmp/friend-opportunity-r1-v2/{small-sparse,medium,large-dense}`
on the verification host. Each variant within a scenario shares the trace hash.
The scenario combined hashes are:

- small-sparse:
  `c4131b044d0eec293676a3502efeafceddab10756227790311d74df7ccc1797b`
- medium:
  `b16095508c5f3a23292aa86f94939032b38e25dc3a60713fc5833511df257c55`
- large-dense:
  `9be877b9645ef975ff569fd75a43deda24f040f163734ce33c7878121f9599fc`

## Usage

One corrected cell:

~~~text
go run ./cmd/friend-opportunity-bench \
  -seed 79 -friends 100 -plots 12 -density 20 -online-rate 5 \
  -expiry bursty -arrival-rate 10 -duration 5m \
  -efficient-fallback 120s -variant all \
  -out-dir /tmp/friend-opportunity-r1-v2/medium
~~~

Full matrix:

~~~text
./scripts/run-friend-opportunity-realtime-ab.sh \
  -matrix -seed 79 -arrival-rate 10 -duration 5m \
  -efficient-fallback 120s -variant all \
  -out-dir /tmp/friend-opportunity-r1-v2-matrix
~~~

Each cell emits `run-config.json`, `trace.jsonl`, `results.jsonl`,
`results.csv`, `summary.json`, and a SHA-256 `manifest.json`.

## Verification commands

~~~text
go test -count=1 ./internal/loadtest/friendopportunity ./cmd/friend-opportunity-bench
go vet ./internal/loadtest/friendopportunity ./cmd/friend-opportunity-bench
~~~

## Scope boundary and next integration

No Go module, public loadtest helper, farm/friend/storage business logic,
farm-worker, or H5 code is required by this benchmark. The future main-agent
adapter should replay the persisted boundary stream against the real Scheduler,
preserve the trace hashes and metric names, and add separately sourced
real-service evidence. It must not relabel these synthetic counters.
