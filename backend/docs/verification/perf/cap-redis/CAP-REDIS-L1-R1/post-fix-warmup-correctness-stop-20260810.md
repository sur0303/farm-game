# CAP-REDIS-L1-R1 post-fix Warmup correctness stop

## Exact binding

- Integration Head at launch: `82ab5e2e0dd7092c24e09b15807e3e83ed6cbb64`
- Business Merge: `64b37d181828cf5b87dc65873d4370bc69f8a416`
- Evidence anchor: `9c2c924eedb0d58d373e7241aacdd18b46ffeb52`
- Harness binary: `ef2b9f3da18f80d4ffb988e1c54f6311589810e72f8fe67514b7bfd9d1576b47`
- Run: `capredis-l1-r1-20260810T045335Z-2772487`

Manifest preflight passed. The preceding 24-Primitive Smoke was `96/96/0 PASS`.

## Hard stop

Warmup stopped at cell 102:

```text
operation                = friend_quota_reserve
key distribution         = hot_quota_key_idempotent_replay
concurrency               = 10
planned                   = 600
completed                 = 0
failed                    = 600
correctness violations    = 9
Friend reconcile total    = 9
round verdict             = FAIL_CORRECTNESS
```

No Formal round started. The runner destroyed the temporary Redis container and did not continue pressure.

## Classification

The Quota hot fixture reused one identity but, unlike the already corrected Pair hot fixture, did not commit it during setup. At c10 the measured window therefore raced concurrent first executions while labelling them idempotent replay.

This is a C1 workload-contract defect. It is not evidence that the Pair atomic envelope fix regressed, and the failed run is retained without relabelling or exclusion.

The failure also exposed an evidence gap: the bounded per-operation collector was connected only to Pair activation. For this Quota cell:

```text
diagnostics expected = 600
diagnostics captured = 0
diagnostics complete = false
final classification = INSUFFICIENT_EVIDENCE
```

No claim is made about the individual durable state of the 600 operations.

## Return

Return to C1 for a narrow change:

1. prime Quota and Share hot fixtures before the measured window so their label is true committed replay;
2. add fixed-key, bounded, redacted per-operation diagnostics for all Friend authority mutation cells;
3. keep first-execution conflict/reconcile behavior visible in a separate correctness negative control;
4. independently review the exact Head;
5. restart again from Harness Smoke.

```text
warmup = 0/1
formal = 0/5
baseline_eligible = false
capacity_claim = NONE
supports_30m_dau = false
```
