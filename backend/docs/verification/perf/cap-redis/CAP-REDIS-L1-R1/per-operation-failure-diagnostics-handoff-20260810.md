# CAP-REDIS-PER-OP-FAILURE-DIAGNOSTICS handoff

## Exact identity

- C1 Writer branch: `codex/cap-redis-l1-r1-20260809`
- Diagnostic code head: `d9ea0dc2decdc009fb66b35c49f792c0a87b7374`
- Product remains the frozen failing R1 head: `4824c9c02fe70873b2f9a8eab3ce274d227063a8`
- Validation run: `capredis-l1-r1-20260810T035833Z-2446238`
- Harness binary SHA-256: `988034f4f1aa9752068370581f0345135f2570eac86a00b8c5a8fb4a87469ad3`

This task changes only `cmd/cap-redis/**` and CAP evidence. It does not modify Friend product code, Redis Lua, planned/completed/failed accounting, or verdict rules.

## Bounded diagnostic contract

Every failed operation may carry an in-memory diagnostic locator. After all measured goroutines stop and after the performance counter snapshot, but before namespace teardown, the harness performs bounded read-only diagnosis.

Persisted identity is domain-separated SHA-256 only:

```text
operation_id_hash
pair_id_hash
bucket_id
```

No raw player ID, raw operation ID, raw Pair ID, Redis credential, token, or payload is emitted.

Each record includes:

- operation index and API error class;
- Pair before/final presence, status, relation epoch, and record version;
- bounded Receipt/Attempt presence, field counts, and Attempt states;
- bucket metadata receipt/pending/attempt counts and manifest HLEN;
- receipt/source/attempt Due cardinalities;
- `COMMITTED | NOT_COMMITTED | UNKNOWN` plus an explicit classification basis;
- per-record evidence hash, ordered hash index, and aggregate evidence hash.

The hard bound is 1024 failed operations per cell; the frozen profile has at most 600 operations per cell. A collection error or bound violation is recorded but never removes a failed request or changes its business result.

## Validation result

The unchanged product race was intentionally re-run:

```text
operation                    = friend_pair_activate
key_distribution             = uniform_pair_keys
concurrency                  = 10
planned                      = 300
completed                    = 291
failed                       = 9
ErrFriendReconcile           = 9
failure diagnostics expected = 9
failure diagnostics captured = 9
failure diagnostics complete = true
final classifications        = NOT_COMMITTED: 9
```

Aggregate evidence hash:

```text
bd16e425b1209e8bc2ba23c443b970f8ea82c920be768860b581fd1980e0017a
```

Every failed fixture had:

```text
Pair record = absent
Receipt     = absent
Attempt     = absent
classification = NOT_COMMITTED
```

Stable bucket metadata and Due counts were captured for each record. Raw-ID leak checks for `cap-activate-*` and `sid1_*` were negative.

The run correctly remains `FAIL_CORRECTNESS`; diagnostics did not hide or exclude any failure.

## Historical evidence rule

The original run's ten failures remain permanently:

```text
original_ten_per_operation_classification = INSUFFICIENT_EVIDENCE
```

This new validation does not rewrite history. It proves only that future runs can produce exact per-operation evidence before teardown.

## Requested review

Reviewer must verify:

1. expected/captured counts equal failed count;
2. raw identifiers and secrets do not appear;
3. all diagnostic Redis reads occur outside the measured metric snapshot and before teardown;
4. collection failure cannot alter planned/completed/failed or verdict;
5. hashes are deterministic and domain-separated;
6. `COMMITTED/NOT_COMMITTED/UNKNOWN` is fail-closed;
7. historical evidence remains unchanged.

Requested disposition:

```text
P1-02 = PASS | RETURN_TO_C1_WRITER
```

Product P1-01 remains independently blocking until the atomic envelope read fix passes review.
