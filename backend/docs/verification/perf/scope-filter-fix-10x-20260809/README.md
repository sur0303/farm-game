# Pair Recovery Scope Fix — Correctness 10/10

## Result

`PASS 10/10` for the combined product correctness anchor at:

- Backend: `6c15a9325a77a457bd5aa8adec87e35138c98793`
- H5: `48ea2dbb52ed50902e15372a5d07721e46d333ca`
- Q1 load/diagnostic tool: `23afdd3f68407b3c05a7b0509a252ba57c7ff897`
- Runs: `q1perf-sf10-08091314-01` through `q1perf-sf10-08091314-10`

Every round proved:

- Browser, independent Go Protocol Bot, and Authority audit: `PASS`;
- F1 lost CARE result recovered;
- CARE Pair `FRIEND_ACTION_DISPATCH_ORDER` and `RESULT_PUBLISH_ORDER` completed by the CARE-scoped handler;
- worker `InvalidOrders=0` and persistent Pair DueIndex candidates `=0`;
- F3: `F3_CHAIN_PASS`;
- first Theft click: confirmation state with Wire `THEFT=0`;
- second Theft click: Wire `THEFT=1`;
- hard correctness incidents: `0`.

The machine-readable per-run index, environment hashes, Redis endpoint hashes, unique namespaces, cleanup state, and evidence hashes are in [`evidence-index.json`](evidence-index.json). Raw evidence remains in ignored `docs/verification/perf/results/<run_id>/` directories.

## Scope and limitations

These were diagnostic correctness runs using `q1-perf-baseline-v1-f3-triage.json`, which has `baseline_eligible=false`. Their final diagnostic verdict is therefore intentionally `INCONCLUSIVE_ENVIRONMENT / NOT_RECORDED` even though the product correctness scenario passed. This is **not** a performance baseline, capacity result, or 30-million-DAU claim.

The CARE handler only terminalizes Pair orders after reading an existing terminal FarmActor receipt. If Pair admission exists but Farm has no receipt, it leaves the order unchanged and due; it does not synthesize public CARE target data or bypass Actor admission.
