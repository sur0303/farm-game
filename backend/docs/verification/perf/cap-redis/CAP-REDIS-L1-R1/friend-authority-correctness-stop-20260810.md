# CAP-REDIS-L1-R1 Friend authority correctness stop

## Classification

```text
task_id                  = CAP-REDIS-L1-R1
purpose                  = current R1 Integration Redis/serialization baseline
baseline_eligible        = false
formal_rounds_completed  = 0/5
CAP-SINGLE-POD-L2-R1     = BLOCKED
CAP-MIXED-B0-R1          = BLOCKED
```

This is a correctness-stop record, not a Redis capacity number and not a 30M DAU claim.

## Frozen product identity

- Backend business commit: `4824c9c02fe70873b2f9a8eab3ce274d227063a8`
- Backend evidence head: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- H5 commit: `525c4d324100b17a891f02797c4880fa9a02f70a`
- Backend image: `sha256:f36a4d8f5f8c05ac44ed173895e46f5348af6d5b133835e6d25d13da2b51c39b`
- H5 image: `sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd`
- Profile hash: `eb4ee6ee0a69b760423495eb42bf3778d0e69309ac55be6eb161df52efff7729`
- Harness code commit: `239b9d5fbfea9fa7863e889417866edab385e827`
- Frozen harness binary SHA-256 for both canonical attempts: `0eb983059c9171889a9fb4f89b22dda20a8fc649bd2f218d61a8142641031678`

No production Handler, Actor semantics, Redis Lua, Workflow semantics, or H5 business code was changed by the C1 harness.

## Harness viability smoke

`capredis-l1-r1-20260810T024809Z-1853273` ran all 24 primitive cells with uniform keys, concurrency 1, and four operations per cell.

Result: `PASS`, exact request/response wire bytes nonzero where Redis was used, all reported hard gates zero, namespace cleanup exact, and `correctness_observation_complete=true`.

This smoke is only a harness viability check. It is not a warmup or formal round.

## Matrix preflight correctness stop

Run: `capredis-l1-r1-20260810T024828Z-1874556`

First matrix cell requested:

```text
operation          = friend_pair_activate
key_distribution   = uniform_pair_keys
concurrency        = 10
planned_operations = 300
```

Observed:

```text
completed_operations                       = 290
failed_operations                          = 10
friend_authority_reconcile_required_total  = 10
error                                      = friend: commit attempt reconcile required: bucket receipt count
verdict                                    = FAIL_CORRECTNESS
correctness_observation_complete           = false
```

The harness stopped immediately. It did not run the warmup, five formal rounds, NOSCRIPT, reconnect, timeout-unknown, business amplification, L2, or Mixed B0.

## Focused source observation

`RedisPairRepository.ActivateFriendRelation` calls `GetPair` before its atomic Pair commit. `GetPair` calls `validateAuthorityEnvelope`.

At the frozen R1 source, `validateAuthorityEnvelope` observes one shared bucket envelope through separate Redis commands:

1. `TYPE` canonical record;
2. `TYPE` bucket metadata;
3. `HMGET` metadata counters;
4. `HLEN` metadata;
5. `ZCARD` receipt DueIndex;
6. `ZCARD` attempt DueIndex;
7. optionally `ZCARD` source DueIndex.

The equality checks are at `internal/friend/redis_authority.go:646-661`. Concurrent Pair commits in the same physical bucket can change a ZSET and its metadata counter atomically between these separate read commands. The reader can therefore observe values from different committed instants and return `ErrFriendReconcile`, even though each writer Lua mutation is atomic.

This is a **candidate explanation**, not an approved product fix. An independent Reviewer must confirm whether the signal is a real R1 concurrent-observation bug or a benchmark misuse. The C1 Agent must not modify production code to hide or retry the signal.

## Required disposition

1. Independent read-only review the exact evidence and source path.
2. If confirmed, return only this issue to the owning Friend authority Writer and original product branch/worktree for a narrow fix.
3. Preserve fail-closed semantics; do not remove envelope checks, delete DueIndex, swallow `ErrFriendReconcile`, or change Lua/business semantics.
4. Re-run the focused negative/positive concurrency witness before restarting CAP-REDIS-L1-R1.
5. After closure, restart from one uncounted warmup plus five formal rounds using the same frozen profile.

Until closure, no primitive latency, Redis QPS, byte rate, or Lua CPU value from the interrupted matrix is conclusion-eligible.
