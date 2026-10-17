# CAP-REDIS namespace Hash-Tag cleanup Warmup stop

## Exact run

- Integration Head: `80c2bf3720f4eba1178cf997f9e6fa2de76030ab`
- Run: `capredis-l1-r1-20260810T051814Z-3017408`
- Round: Warmup 1
- Verdict: `FAIL_OPERATION`

```text
cells completed before stop = 184
failed cell                 = shard_authority_renew / uniform / c1
round planned               = 111000
round completed             = 110400
failed in cell              = 600
API status                  = LEASE_EXPIRED
hard correctness counters   = 0
```

The runner stopped immediately. No Formal round started and the temporary Redis container was destroyed.

## Root cause

`benchmarkEnvironment.deleteNamespace` and `namespaceResidual` scanned only:

```text
classic-farm:<environment>:<zone>:*
```

Redis authority, workflow and Friend keys begin with a cluster Hash-Tag:

```text
{classic-farm:<environment>:<zone>:...}:<suffix>
```

The missing leading `{` meant reset and residual checks did not observe those keys. `claimAllActorShards` then replayed the same Claim transition rather than creating a fresh lease. Claim replay intentionally does not renew. The Authority Store clamps the requested four-hour fixture TTL to its frozen one-minute maximum, so the stale claim was expired by the final runtime cell.

This is a C1 Harness isolation/cleanup defect, not a production Lease regression. It also means prior successful cleanup-residual assertions did not cover Hash-Tag keys; those runs remain correctness diagnostics and are not promoted to baseline evidence.

## Return requirements

1. scan/delete both tagged and untagged exact namespace forms;
2. count both forms in the residual hard gate;
3. preserve other environment/zone keys in the same Redis DB;
4. add permanent tests with tagged, untagged and foreign namespace keys;
5. prove reset creates a fresh non-expired Shard lease;
6. independently review and restart from Smoke.

No `FLUSHALL`, `FLUSHDB`, global key deletion, secret retention or lease-policy change is permitted.

```text
Warmup = 0/1
Formal = 0/5
baseline_eligible = false
capacity_claim = NONE
```
