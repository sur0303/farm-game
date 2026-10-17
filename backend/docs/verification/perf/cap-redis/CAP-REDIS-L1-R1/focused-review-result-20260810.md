# FRIEND-AUTH-BUCKET-ENVELOPE-CONSISTENCY-REVIEW

## Verdict

```text
P0 = 0
P1 = 2
P2 = 0
EXACT_HEAD_CLOSURE = FAIL
FRIEND_SIGNAL = CONFIRMED_PRODUCT
CLASSIFICATION = PRODUCT_READ_SNAPSHOT_RACE
```

Reviewed exact heads:

- Product/evidence base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Backend business commit: `4824c9c02fe70873b2f9a8eab3ce274d227063a8`
- Harness code: `239b9d5fbfea9fa7863e889417866edab385e827`
- Correctness-stop evidence: `4778d4df5e12f481096509459e1e86307ddeedf3`
- Review handoff: `81f1eb1a3ac7695c1afc9963497def68badb9eff`

The review was read-only for product and harness code. The deterministic test ran from a temporary copy and was destroyed after output capture.

## Findings

### P1-01 — Product normal-read mixed snapshot

`RedisPairRepository.ActivateFriendRelation` calls `GetPair` before Prepare or business Commit. `GetPair` calls `validateAuthorityEnvelope`, which reads one shared bucket through separate Redis commands:

```text
TYPE target record
TYPE bucket meta
HMGET bucket meta
HLEN bucket meta
ZCARD receipt-due
ZCARD attempt-due
ZCARD source-due (Pair)
```

A distinct Pair commit in the same bucket may execute after HMGET and before ZCARD. The metadata and ZSET are atomically correct at both real instants, but the client compares values from different instants and returns `ErrFriendReconcile`.

A real Redis PhaseBarrier probe forced exactly that schedule:

```text
target Pair A: HMGET returned old bucket receipt_count
writer Pair B: committed in the same bucket
Pair A: resumed and observed new receipt-due ZCARD
Pair A result: ErrFriendReconcile
target Pair A record/receipt/attempt: absent
writer Pair B: ACTIVE
stable metadata receipt_count: 2
stable receipt-due ZCARD: 2
```

The different-bucket control passed. This rules out a KeyBuilder mismatch and proves a product same-bucket observation race without introducing data corruption.

Severity is P1: the demonstrated branch is a pre-commit false rejection, not a post-commit false failure. No evidence currently justifies P0. The 3.33% preflight failure rate at concurrency 10 blocks L1 and migration/capacity claims.

Owner: original Friend Authority Writer. Required narrow task: `FRIEND-AUTH-ATOMIC-ENVELOPE-READ-FIX`.

### P1-02 — Original 10 failures lack per-operation final-state evidence

The retained CAP run records planned/completed/failed counts and the first error, but does not retain for every failed operation:

- operation ID and Pair ID/bucket;
- API result;
- Pair version/status/epoch;
- Receipt/Attempt presence and state;
- stable post-run bucket counters/ZSET cardinalities.

The deterministic reviewer probe proves the product race and proves its target was `NOT_COMMITTED`, but it cannot retroactively certify every one of the original ten operations. Exact operation-by-operation classification for that retained run is therefore `INSUFFICIENT_EVIDENCE`.

Owner: original CAP-REDIS C1 Writer. Required narrow change: capture bounded, secret-free failure diagnostics before namespace teardown. It must not change product behavior or exclude failed operations.

## Seven required answers

### 1. What do receipt_count, HLEN, and ZCARD count?

The premise needs correction:

- bucket metadata `HLEN` validates the fixed 11-field bucket-meta manifest; it is not Receipt HASH HLEN;
- `receipt_count` is the bucket-wide number of retained Authority Receipt/fence evidence charged by commit scripts;
- `receipt-due ZCARD` is the bucket-wide discovery-member count and must equal `receipt_count`;
- `attempt_count` is a conservative count of created attempt cursors and may exceed `attempt-due ZCARD` after compaction;
- Pair `pending_work_count` must equal Pair source `due ZCARD`.

Receipt partitions are authority-specific HASHes and are not globally HLEN-compared by `validateAuthorityEnvelope`.

### 2. Must the counts be exactly equal?

At one linearization point:

```text
receipt_count      == receipt-due ZCARD
pending_work_count == source-due ZCARD (Pair)
attempt_count      >= attempt-due ZCARD
bucket-meta HLEN   == 11
```

Count/byte zero-ness must also agree. The contract does not permit comparing values sampled from different committed instants as if they were one snapshot.

### 3. Which paths modify them?

- Prepare Lua creates/bounds an attempt cursor, increments attempt count/bytes, and adds attempt-due.
- Pair/Quota/Share business Lua writes Receipt evidence, increments receipt count/bytes, and adds receipt-due.
- Pair business Lua creates bounded SourceOrders, increments pending count/bytes, and adds source-due.
- Pair Work completion Lua removes source-due and decrements pending count/bytes.
- Attempt resolution/marker compaction removes attempt discovery and marker bytes while retaining conservative cursor count/evidence.
- Receipt lifecycle compaction is contractually required to update Receipt, receipt-due, and counters together; no concurrent receipt GC ran in the failing preflight.

### 4. Are related writes atomic?

The inspected Prepare, business Commit, resolution/marker compaction, and Pair Work completion changes are same-slot Lua operations. The observed stable post-barrier counts matched. The defect is the Go normal-read observer, not a demonstrated split writer.

### 5. Why does GetPair validate the whole bucket?

The implementation and SO1 handoff intentionally fail closed on malformed/missing bucket metadata and forged capacity/discovery counters before treating a target record as authoritative. Contract §4.2 permits a fixed Primary read or read-only Lua, but does not require a client-side multi-command pseudo-snapshot. Keeping the safety checks is valid; their current observation boundary is not.

### 6. Can different Pairs in one bucket cross versions?

Yes, deterministically. The PhaseBarrier proof forced exactly this schedule and produced `ErrFriendReconcile`. A writer in a different bucket did not affect the target.

### 7. Were the ten failed operations committed?

- The exact first retained error (`bucket receipt count`) is emitted by the pre-commit Go envelope read.
- The permanent PhaseBarrier reproduction of that class proves its target was `NOT_COMMITTED` with no target record/receipt/attempt.
- The retained aggregate does not identify each of the ten failures, so certifying all ten individually is impossible. Their per-operation result remains `INSUFFICIENT_EVIDENCE`, which is P1-02.
- There is no evidence of `PRODUCT_POST_COMMIT_FALSE_FAILURE` in this review.

## Alternatives excluded

- `PRODUCT_BUCKET_ACCOUNTING_BUG`: not supported by the deterministic schedule; stable metadata/ZSET counts matched.
- `HARNESS_KEY_DISTRIBUTION_BUG`: excluded; the probe and harness use the production `FriendshipKeyBuilder`. A 64-bucket frozen L1 profile increases collision frequency but is a valid product profile and does not create the read race.
- `CONTRACT_CONFLICT`: excluded; fixed, bounded, same-slot atomic observation is compatible with §4.2/§9.
- `PRODUCT_POST_COMMIT_FALSE_FAILURE`: not observed; target had no write evidence.

## Required closure gates

Product Writer must permanently add:

1. PhaseBarrier: distinct Pair, same bucket mixed snapshot;
2. same Pair concurrency and idempotent replay;
3. different-bucket isolation;
4. Commit versus Receipt lifecycle/compaction semantics;
5. real corruption still returns `ErrFriendReconcile`;
6. fixed/bounded same-slot atomic observation or another reviewed linearizable design;
7. NOSCRIPT and real Redis coverage for any new script;
8. no unbounded retry, ignored error, process mutex, weakened comparison, or serialized benchmark.

C1 Writer must add bounded failure records and post-failure read-only diagnostics before teardown.

After both P1s close, rerun independent review and then restart CAP-REDIS from Smoke, one uncounted warmup, and five formal rounds. L2, Mixed B0, FriendshipDomain cutover, and baseline eligibility remain blocked.
