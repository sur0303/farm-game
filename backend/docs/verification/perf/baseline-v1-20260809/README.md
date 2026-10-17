# Q1 Baseline V1 selected run — 2026-08-09

Selected run: `q1perf-b1-20260809100947-1793411`

> This directory preserves the selected run exactly as classified at run time. A later deterministic diagnostic proved the original F3 timeout was a stale one-click test contract (`H5_WIRE_THEFT_MISSING`), then obtained `F3_CHAIN_PASS` with a hashed test-only two-click overlay. It also found an independent pre-F3 Care Pair DueIndex product bug. See `../q1-f3-invalid-order-followup-20260809.md` and `../followup-f3diag3-20260809/`. The performance baseline remains unrecorded.

## Frozen identity

- Backend production commit: `260babcd93cfb6a4a58a481cc3c7f8c1f95e455a`
- H5 commit: `df379b5fcc5eb710fc1efa5406c752ba18c3ab30`
- Load-tool commit used by the run: `63a74786fb6c1eb836a7a3e89e6da4d398cece3e`
- Config hash: `sha256:a4ceef4f2d10f850b183f3ecab0cbe6fdb15da4399389a774d86c8c8917cee17`
- Machine-spec hash: `sha256:e2601c8b7b361b034d2ed44621ad4b461ed9625242069f78057c47a9cdcd41b5`

## Result

`FAIL_CORRECTNESS` — **Baseline performance was not recorded.**

The hard-gate run used two real headed Playwright H5 contexts plus the independent Go protocol bot, fault proxy, and read-only Authority audit.

- Go protocol bot: `PASS` through Register/Login/Hello/Join/Bootstrap/Buy/Plant/premature rejection/maturity/Harvest/bootstrap refresh.
- Browser Hero: reached Login, Buy/Plant, Harvest, friend navigation, F1 result-loss recovery, simulated reconnect, F2 Gate restart recovery, and water-cycle cleanup.
- Browser F3: timed out waiting 30 seconds for the armed fault proxy to observe `FAIL_FARM_CHANGE`; no F3 success witness was produced.
- Authority business oracle: `INCONCLUSIVE` because the browser verdict was not PASS.

The configured 200/500 connection stages, 200-WSS baseline/slow-reader cases, and mixed open-arrival stages were **not executed** after the correctness stop condition fired. Planned/actual pressure is therefore recorded as zero, not presented as a passing baseline.

This run does not support a 30 million DAU claim.

## Safety

The run used a unique namespace and an ephemeral loopback Redis. The manifest records only the endpoint hash, no Redis password. The exact container was destroyed and `cleanup_completed=true`; no database-wide deletion was used.

## Evidence

The machine-readable core bundle is in this directory. Larger raw evidence (browser traces, screenshots, videos, process logs, proxy events) remains at:

`../results/q1perf-b1-20260809100947-1793411/`

Q1 did not modify production handlers, Actor Runtime, AuthorityStore/Lua, Player/Farm/Workflow, Proto, H5 business code, or Route Runtime to work around this result.
