# CAP-REDIS per-operation diagnostics bounded-read amendment

## Review return

Focused review of diagnostic head `d9ea0dc2decdc009fb66b35c49f792c0a87b7374` found one P1:

```text
P1-DIAG-BOUND-01
The collector bounded the number of matching keys but used database SCAN and
per-key HSCAN cursor loops. Work was therefore not bounded independently of
unrelated database cardinality/Hash shape.
```

The earlier validation run and handoff remain retained; they are superseded as a closure candidate, not rewritten.

## Narrow correction

- Exact bounded-locator head: `369cdd15d43e1aef81d69be914ec169d1fba0dba`
- Final fail-closed/redacted diagnostic head: `e7119770aff95461a0cae0e4b8649388b5b157aa`
- Final validation run: `capredis-l1-r1-20260810T042815Z-2611306`

A subsequent review pass also required an allowlisted API error vocabulary and explicit handling of every TYPE/HMGET/HLEN/HGET/HEXISTS/ZCARD or key-construction failure. Any such gap now forces that record to `UNKNOWN`, returns an aggregate collection error, and sets `failure_diagnostics_complete=false`. Raw product error strings are never persisted.

The fixture now computes the frozen Friend participant locator digest before execution and retains these values in memory only:

```text
exact receipt partition key
exact participant receipt field
exact attempt partition key
exact cursor state field
```

Diagnosis uses only fixed point reads:

```text
TYPE
GetPair (only when canonical Pair exists)
HEXISTS + HLEN on one exact Receipt partition
HLEN + HGET on one exact Attempt partition
fixed three-field bucket-meta HMGET + HLEN
three exact ZCARD reads
```

There is no `SCAN`, `HSCAN`, key-pattern discovery, unbounded retry, or namespace-wide traversal. A permanent vector test binds the participant locator formula.

Exact Redis keys, locator digests, player IDs, operation IDs and Pair IDs are not serialized. Persisted identity remains domain-separated SHA-256.

## Validation

The unchanged frozen product race was intentionally reproduced:

```text
planned                 = 300
completed               = 287
failed                  = 13
ErrFriendReconcile      = 13
diagnostics expected    = 13
diagnostics captured    = 13
diagnostics complete    = true
NOT_COMMITTED           = 13
COMMITTED               = 0
UNKNOWN                 = 0
verdict                  = FAIL_CORRECTNESS
aggregate evidence hash = 483a75cf1eee25288c107319c04bd64527acdd997df690ba9a940c01848efaed
```

All thirteen exact Receipt and Attempt partitions were absent before teardown. Diagnostic collection did not modify planned/completed/failed counts or the correctness verdict.

## Requested disposition

```text
P1-DIAG-BOUND-01 = PASS | RETURN_TO_C1_WRITER
P1-02            = PASS | RETURN_TO_C1_WRITER
```
