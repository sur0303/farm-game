# Friend Opportunity C0 Evidence

C0 freezes a repeatable capacity baseline for friend-list, next-opportunity, and
direct Hint delivery. It does **not** claim single-node evidence proves 30 million DAU.

## Owned paths

- `internal/observability/friendcapacity/**`
- `internal/observability/redisworkload/metrics.go` (+ test)
- `cmd/ws-stress/**`
- `cmd/friend-opportunity-perf-fixture/main.go` (+ test)
- `scripts/perf/friend-opportunity-c0/**`
- `deploy/load/friend-opportunity-c0-*.json`
- `docs/verification/perf/friend-opportunity-c0/**`

## Safety

- Ephemeral Redis container only (`foperf-<run>-redis` on loopback).
- Namespace must be `foperf-<run>`; shared/company Redis and `dev`/`prod` are rejected.
- Cleanup is exact run-scoped process + container destroy. Never `CONFIG SET`,
  `SLOWLOG RESET`, `FLUSH*`, or broad `SCAN+DEL`.
- Unsupported knobs (`online-ratio`, `friend-base`, non-`direct` `hint-transport`)
  are rejected by `ws-stress` before measurement.

## Run

```bash
# short smoke (friend_count=20)
scripts/perf/friend-opportunity-c0/run-c0-smoke \
  deploy/load/friend-opportunity-c0-smoke.json

# 20/100/300 matrix
scripts/perf/friend-opportunity-c0/run-c0-30m-model \
  deploy/load/friend-opportunity-c0-30m-model.json
```

State and binaries stay under `/tmp/farmgame-c0*` unless overridden. Evidence is
written to `docs/verification/perf/friend-opportunity-c0/<run_id>/`.

Both runners install `trap` on `EXIT`/`INT`/`TERM` and always emit `cleanup.json`.

## Evidence contract

Each run directory contains:

| Artifact | Meaning |
| --- | --- |
| `config.json` | Copied plan |
| `run-meta.json` | run_id, namespace, effective transport=`direct` |
| `fixture.json` | Verified friend cardinality + cleanup scope |
| `workload-*.json` | Raw ws-stress scenario report (`--run-id` must match cell) |
| `metrics/before-<scenario>\|after-<scenario>/*.prom` | Per-workload participating-service scrapes |
| `redis-aggregate-*.json` | Workload-scoped aggregate + endpoints/scope |
| `evidence-*.json` | Workload report with merged Redis + server phase availability |
| `verdict.json` | pass/fail gate |
| `cleanup.json` | Exact ephemeral cleanup completion |

### Availability fields

- `redis_commands` / `estimated_bytes` are nullable.
- `*_availability` is `ok`, `client_unmeasured`, or `measurement_unavailable`.
- Redis evidence is scraped before and after each individual workload and
  aggregated across every participating endpoint (`gate`/`player`/`farm`/`worker`).
  Incomplete endpoint sets mark that workload `measurement_unavailable`; one
  farm-svr aggregate is never copied into every result.
- Server-side phases that are not instrumented in current production code appear
  explicitly under `server_phase_availability` as `measurement_unavailable`.
- Response bytes from `farm_redis_workload_response_bytes_total` are **estimated**
  (`estimated=true` label). Exact wire bytes need a Harness proxy.

### Identity and matrix isolation

- C0 requires `conns=1`, explicit `--run-id`, and
  `friend-viewer == player-start == login player_id == ServerHello.player_id`.
- Cookie/ticket/ServerHello mismatch fails closed before measurement.
- Each 20/100/300 matrix cell uses a distinct viewer, target owner,
  account/bootstrap identity, fake-base, and cell operation run_id derived from
  `run_id + friend_count`. Rosters are never reused across cells; fixture
  verifies each cell's actual roster cardinality.

### Hint / next-opportunity measurement

- Hint scenario drives a deterministic Buy/Plant transition on `--hint-target-owner`
  only after the viewer WSS session is ready and the pull check has completed.
- Source Buy/Plant must reach terminal `SUCCEEDED`; `PROCESSING` and `NO_CHANGE`
  are not terminal success.
- Hint success requires WSS recipient identity, `target_owner_id`, and a causally
  new `source_farm_version` / `opportunity_revision`.
- Timeout/drop records `outcome=dropped` and is never scenario success.
- Latency is labeled `source_to_receive` when the source transition ran; if the
  transition cannot execute, the field is `unavailable`.
- HTTP 503 / `FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE` is recorded as
  `outcome=unavailable` and `scenario_errors`, never as success.
- Scenario attempts are counted before Login; a Login failure produces exactly
  one terminal scenario failure.

## Schema

`deploy/load/friend-opportunity-c0.schema.json` validates the smoke and 30m-model plans.
