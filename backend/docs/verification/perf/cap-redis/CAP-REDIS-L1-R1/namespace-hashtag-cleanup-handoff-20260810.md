# CAP-REDIS namespace Hash-Tag cleanup handoff

## Identity

- Integration/stop base: `4000eedd37c97d41d9c43751a6484537203fd18e`
- C1 code/test head: `2a268a7f577c5936b0d973daf71667a725ce2065`
- Focused run: `capredis-l1-r1-20260810T052449Z-3045980`

## Correction

The Harness now treats the exact namespace as the union of two disjoint key forms:

```text
classic-farm:<environment>:<zone>:*
{classic-farm:<environment>:<zone>:*
```

Both `deleteNamespace` and `namespaceResidual` scan those exact patterns. Deletion remains page-bounded and targets only returned keys with `DEL`; it does not use `FLUSHALL`, `FLUSHDB`, a cross-environment wildcard or a global key delete.

Permanent controls seed:

- an untagged owned key;
- Authority and Workflow Hash-Tag owned keys;
- foreign zone, foreign environment and prefix-lookalike keys.

They require all owned keys deleted, all foreign keys preserved, and residual `0`.

A second control expires the original one-minute Shard lease, resets the namespace, and requires a fresh holder renewal to return `APPLIED`. This proves reset is not replaying the expired Claim.

## Focused real Redis matrix

```text
shard_authority_renew
uniform c1/c10/c100/c300 = 2400/2400
hot c1/c10/c100/c300     = 2400/2400
total                     = 4800/4800
LEASE_EXPIRED             = 0
failed                     = 0
hard correctness gates     = 0
namespace residual         = 0
verdict                    = PASS
```

Harness binary SHA-256:

```text
bfed024275b2d2ccaa034a3737e1fb8007bfc387302408ef4f6560a35d61b83c
```

## Historical rule

The failed Warmup `capredis-l1-r1-20260810T051814Z-3017408` remains `FAIL_OPERATION`. Its prior residual result is not upgraded because the old code did not inspect Hash-Tag keys.

## Requested review

```text
P0 = ?
P1 = ?
P2 = ?
TAGGED_CLEANUP = PASS | RETURN
FOREIGN_NAMESPACE_ISOLATION = PASS | RETURN
FRESH_SHARD_LEASE = PASS | RETURN
EXACT_HEAD_CLOSURE = PASS | FAIL
```

Only after review and integration evidence rebinding may L1 restart from Smoke.
