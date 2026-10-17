# FRIEND-AUTH-ATOMIC-ENVELOPE-READ-FIX focused review result

## Exact review identity

- Task: `FRIEND-AUTH-ATOMIC-ENVELOPE-READ-FIX` + `CAP-REDIS-PER-OP-FAILURE-DIAGNOSTICS`
- Composition code/test head: `553d80b679cfac2b7ea2a7cdf9f7905b8e1c0cb7`
- Composition handoff/evidence head: `c74105ccd1d4e8693ceefc26664da059948b43b8`
- Imported final C1 diagnostic evidence head: `6fa9e11` (review-local evidence-only composition)
- Frozen backend evidence base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Frozen business commit: `4824c9c02fe70873b2f9a8eab3ce274d227063a8`

Reviewer did not move a tag and did not modify product or harness code.

## Findings

```text
P0 = 0
P1 = 0
P2 = 0
P1-01 = PASS
P1-02 = PASS
EXACT_HEAD_CLOSURE = PASS
FRIEND_SIGNAL = CLOSED
CLASSIFICATION = PRODUCT_READ_SNAPSHOT_RACE_FIXED
```

`FRIEND_SIGNAL=CLOSED` is scoped to `FRIEND_AUTHORITY_ENVELOPE_READ_CONSISTENCY`; it is not a blanket approval for FriendshipDomain migration, legacy roster deletion, L2, Mixed B0, or a capacity claim.

## Review conclusions

### P1-01 product fix

PASS:

- exactly five same-slot keys and two arguments;
- exactly eleven bucket-meta fields and fixed 21-element response;
- one Redis Lua linearization point for record/meta/receipt-due/attempt-due/source-due observation;
- read-only source, with no Redis `KEYS`, `HGETALL`, `SCAN`, mutation, or unbounded loop;
- Go retains exact metadata identity, manifest HLEN, Due/count and byte/count invariants;
- stable corruption remains `ErrFriendReconcile`;
- no retry, random sleep, process mutex, reduced concurrency, or swallowed reconcile error;
- Script Registry/NOSCRIPT path and real Redis path pass;
- Pair batch preserves one pipeline round trip and one atomic envelope per Pair;
- batch error metrics account every queued EVAL before fail-closed return.

### P1-02 diagnostics

PASS:

- failed operations remain in planned/completed/failed accounting;
- point diagnosis occurs after the measured metric snapshot and before teardown;
- one exact Receipt partition/field and one exact Attempt partition/state are addressed in memory;
- no `SCAN`, `HSCAN`, cursor loop, or namespace-wide traversal;
- persisted operation and Pair identities are domain-separated hashes only;
- API errors use an allowlist and never persist raw product errors;
- every key/read failure forces `UNKNOWN`, collection error, and incomplete status;
- per-record hash index and aggregate hash independently recompute;
- historical ten failures remain `INSUFFICIENT_EVIDENCE`.

## Evidence review

Retained stop `capredis-l1-r1-20260810T040738Z-2498081` remains visible as `FAIL_CORRECTNESS`; it is not excluded from reports. It proved the old hot fixture was concurrent first execution, not committed replay. Correcting fixture setup did not relabel or erase that run.

Final exact evidence:

```text
original uniform/c10 cell   300/300/0 PASS
uniform c1/10/50/100       1200/1200/0 PASS
committed replay c1/10/50/100
                             1200/1200/0 PASS
forced same-Bucket matrix   1200/1200 PASS
real Redis PhaseBarrier     20/20 PASS
ReconcileTotal              0
TechnicalFailureTotal       0
```

Frozen failing-product diagnostic negative control:

```text
failed/expected/captured = 13/13/13
classification           = NOT_COMMITTED:13
collection complete      = true
raw identity leak        = 0
aggregate evidence hash  = 483a75cf1eee25288c107319c04bd64527acdd997df690ba9a940c01848efaed
business verdict         = FAIL_CORRECTNESS
```

## Independent mechanical rerun

```text
go test ./internal/friend -count=3                            PASS
go test ./cmd/cap-redis -count=3                              PASS
go test -race focused Friend atomic/batch/delete              PASS
go test -race ./cmd/cap-redis                                 PASS
go vet ./internal/friend ./cmd/cap-redis                      PASS
go build ./cmd/cap-redis                                      PASS
CAP manifest preflight                                        PASS
evidence/hash/raw-ID review                                   PASS
secret-pattern review                                         PASS
```

The prior Delete race failure was a test boundary mismatch with asynchronous termination cleanup. The corrected test retains the terminal invariant and passes normal 1000/1000, Race 100/100 and full Friend 20/20; production semantics were not changed.

## Authorization boundary

This review authorizes only:

```text
restart CAP-REDIS-L1-R1 from Harness Smoke
```

It does not authorize continuation from the failed matrix position. Required order remains:

```text
Smoke → Warmup 1/1 → Formal 5/5 → amplification/fault probes → evidence review
```

At this review point:

```text
baseline_eligible = false
capacity_claim = NONE
supports_30m_dau = false
L2 = BLOCKED
Mixed B0 = BLOCKED
```
