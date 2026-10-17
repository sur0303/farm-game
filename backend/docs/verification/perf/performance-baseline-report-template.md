# Q1-PERF-BASELINE-V1 · Report Format

## Identity

| Field | Value |
|---|---|
| backend_commit | exact SHA |
| h5_commit | exact SHA |
| load_tool_commit | exact SHA |
| machine_spec_hash | SHA-256 |
| config_hash | SHA-256 |
| Redis endpoint | SHA-256 only |
| Redis namespace | unique run namespace |
| test window | UTC start/end |

## Verdict

`BASELINE_RECORDED | PERF_REGRESSION_PASS | FAIL_CORRECTNESS | INCONCLUSIVE_ENVIRONMENT`

Machine class: `PASS | FAIL | INCONCLUSIVE`.

This report is not a 30 million DAU capacity proof.

## Planned versus actual

For every open-arrival stage record:

- planned QPS and arrivals;
- actual injected arrivals and inferred injection rate;
- Ready/admitted count;
- completed count and QPS;
- schedule drop;
- P50/P95/P99;
- load-generator CPU stop signal.

## Resources

Retain timestamped raw samples and maxima for:

- service and load-generator CPU;
- RSS and virtual memory;
- goroutines;
- open fd;
- GC counters/pause histogram;
- host network RX/TX (explicit host-network-namespace scope);
- application queue/lease/Actor/Workflow metrics exported by each process.

## Correctness hard gate

Every applicable count must be zero:

- duplicate business effect;
- FarmVersion gap/regression;
- workflow or due residue;
- asset conservation violation;
- participant receipt mismatch;
- unexpected healthy disconnect;
- browser/protocol correctness witness failure.

## Observability gaps

Do not infer missing metrics. Record `PENDING_OBSERVABILITY`, owner, and decision impact.
