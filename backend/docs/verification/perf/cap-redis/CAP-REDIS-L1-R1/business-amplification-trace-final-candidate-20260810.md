# CAP-REDIS-L1-R1 Business Trace — Final Candidate Amendment

## Selected exact candidate

- Integration base: `5b42a4f587263ab0ce8369330321345e240782c7`
- Business Commit: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- H5 Commit: `525c4d324100b17a891f02797c4880fa9a02f70a`
- Trace tool Head: `3f1f9c6e1143a2c4589061ff2f50d73021c64f1f`
- Selected run: `capredis-business-e2e-20260810T072200Z`
- Aggregate SHA-256: `3a7006f2122c2cd5d6ae8e2d9af6cbd97656459a339196f84b3a49bfcbe11f89`
- Result: `PASS`

| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | RESP bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 0 | 0 | 0 | 1 | 1 | 2,246 |
| SELL | 1 | 0 | 0 | 0 | 1 | 1 | 3,580 |
| CLAIM_TASK | 1 | 0 | 0 | 0 | 1 | 1 | 5,825 |
| PLANT | 3 | 7 | 0 | 7 | 3 | 10 | 43,542 |
| HARVEST | 2 | 5 | 0 | 5 | 2 | 7 | 25,230 |
| WATER (`CARE`) | 1 | 0 | 1 | 1 | 1 | 7 | 19,284 |
| THEFT | 2 | 5 | 0 | 5 | 2 | 17 | 50,401 |

All seven rows are bound to unique domain-separated Operation hashes. Redis errors, unanswered requests and raw persisted identities are zero. Browser, independent protocol bot, Authority oracle and combined stack all passed. Redis was a unique loopback instance and was destroyed without `SCRIPT FLUSH` or `FLUSHALL`.

## Amendments after initial handoff

1. Added exact SHA-256 binding for the trace/web binaries and test/runner sources; dirty or untracked trace source now fails before startup.
2. Exact-source `NOSCRIPT → EVAL` recovery is paired by Redis script SHA and distinguished from unresolved Redis errors.
3. Stable aliases are collected only from operation-bound Workflow mutations or exact Order/Work keys. Stable IDs embedded in Actor candidate state can no longer admit unrelated friend/worker polling.
4. Added a negative control proving an unrelated stable ID in Actor state does not enter the measured row.
5. Stack-console credentials are stream-redacted before evidence output.

## Retained non-selected attempts

- `T064300Z` and `T064900Z`: stack correctness PASS; trace qualification rejected recovered NOSCRIPT under the earlier overly strict classifier.
- `T065500Z`, `T070100Z`, `T071500Z`: PASS candidates retained but superseded because cold-script or alias/same-pair warmup could vary amplification.
- `T070800Z`: retained `FAIL_CORRECTNESS`; the extended foreign-pair warmup exceeded the frozen route lease and correctly stopped with `HOLDER_LEASE_EXPIRED`. No rows were published from it.

No failed or superseded result is used by the selected table or summary.

## Eligibility boundary

This candidate still requires independent focused review, a hash-bound `business-trace/review-verdict.json`, Premerge and integration. Until those close:

```text
verdict=INCONCLUSIVE_REQUIRED_GAPS
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
L2=BLOCKED
Mixed=BLOCKED
```
