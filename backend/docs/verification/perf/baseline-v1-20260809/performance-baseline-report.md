# Q1-PERF-BASELINE-V1 · Performance Baseline Report

- **Verdict:** `FAIL_CORRECTNESS` (`FAIL`)
- **Run:** `q1perf-b1-20260809100947-1793411`
- **Window:** `2026-08-09T10:09:48.039037Z` → `2026-08-09T10:11:14.247790Z`
- **Backend:** `260babcd93cfb6a4a58a481cc3c7f8c1f95e455a`
- **H5:** `df379b5fcc5eb710fc1efa5406c752ba18c3ab30`
- **Load tool:** `63a74786fb6c1eb836a7a3e89e6da4d398cece3e`
- **Config hash:** `sha256:a4ceef4f2d10f850b183f3ecab0cbe6fdb15da4399389a774d86c8c8917cee17`
- **Redis:** `ephemeral_local`, endpoint `sha256:1a7a3ae203009f77cc69f0ddb9866cffa7d07dc0b661bb8698ff2d5b5a8faf4b`, namespace `q1perf-b1-20260809100947-1793411`
- **Machine:** `AMD EPYC 7K62 48-Core Processor`, 32 logical CPUs, 66879913984 bytes RAM, spec `sha256:e2601c8b7b361b034d2ed44621ad4b461ed9625242069f78057c47a9cdcd41b5`

> Baseline performance was not recorded: the run stopped at the correctness hard gate before pressure profiles. It is **not** evidence that the system supports 30 million DAU.

## Profile results

| Scenario | Profile | Status | Planned | Actual | Reason |
|---|---|---:|---:|---:|---|
| product-correctness-anchor | product_correctness_anchor | `FAIL` | — | — | S0 or S1 failed (s0=0 h5=1 bot=0) |

## Planned versus actual load


## Correctness invariants

- **product-correctness-anchor**: `{"browser_witness_failure": 1, "hero_or_fault_failure": 1, "protocol_bot_step_failure": 0}`
- **Hard correctness violations:** `2`

## Resource and latency evidence


Raw CPU/RSS/goroutine/fd/GC/network samples are retained in each scenario evidence directory.

## Observability gaps

- `actor_record_bytes` — **PENDING_OBSERVABILITY** (A3): record-size percentiles cannot be compared yet
- `commit_bytes` — **PENDING_OBSERVABILITY** (A3): Redis request/response write amplification is not yet measured
- `open_arrival_lag_ms` — **PENDING_OBSERVABILITY** (Q1): planned and attempted counts are exact; per-arrival scheduler lag is not emitted by ws-stress
- `connection_writer_queue_depth` — **PENDING_OBSERVABILITY** (Gate): queue rejection/kick counters exist but queue depth is not exported
- `redis_session_route_qps` — **PENDING_OBSERVABILITY** (Gate/A4): Redis aggregate commandstats cannot attribute SessionRoute exactly
- `redis_subscription_lease_qps` — **PENDING_OBSERVABILITY** (A4): lease counters exist; exact Redis QPS attribution is pending
- `mixed_open_arrival_operations` — **PARTIAL** (Q1 + scenario owners): open-arrival mix drives login/Join plus authority-confirmed Buy/Plant protocol journeys; Hero and hot-farm anchors cover SELL/WATER/THEFT/CLAIM_TASK/Friend paths as closed correctness witnesses
- `safe_connections_per_gate` — **NOT_FROZEN** (Q1): first-run staircase records a growth curve only
- `supports_30m_dau` — **PROHIBITED_CLAIM** (Q1): this baseline is not final capacity proof
- `playwright_h5_witness` — **MISSING_OR_FAILED** (Q1): no passing real-H5 correctness anchor in this run

## Evidence boundary

- Go Protocol Bots carry connection and workload pressure; Playwright is limited to real-page witnesses.
- Redis secrets are not recorded and `FLUSHALL` is prohibited.
- Every local Redis target is an exact ephemeral container; cleanup destroys that container rather than touching unrelated data.
- Capacity SLOs and `safe_connections_per_gate` remain unfrozen until larger isolated staircases and soak runs complete.
