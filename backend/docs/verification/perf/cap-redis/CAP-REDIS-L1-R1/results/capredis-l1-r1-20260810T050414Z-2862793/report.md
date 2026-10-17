# CAP-REDIS-L1-R1 report

- Verdict: **SMOKE_PASS**
- Classification: current R1 Integration Redis/serialization baseline
- `baseline_eligible=false`
- `supports_30m_dau=false`
- Warmup/formal rounds: `0/0`
- Profile hash: `eb4ee6ee0a69b760423495eb42bf3778d0e69309ac55be6eb161df52efff7729`

## Primitive matrix

| Operation | Key distribution | C | Redis commands/op | Req bytes/op | Resp bytes/op | Client P99 us | Lua P99 us | Cross-round commands CV |
|---|---:|---:|---:|---:|---:|---:|---:|---:|

## Business-command amplification

The required table remains `NOT_MEASURED`; primitive results are not substituted or summed into a fake business trace.

| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | — | — | — | — | — | — | — |
| SELL | — | — | — | — | — | — | — |
| PLANT | — | — | — | — | — | — | — |
| HARVEST | — | — | — | — | — | — | — |
| WATER | — | — | — | — | — | — | — |
| THEFT | — | — | — | — | — | — | — |
| CLAIM_TASK | — | — | — | — | — | — | — |

Warmup results are retained but excluded from aggregate statistics. No Mixed B0 eligibility is granted by this report.
