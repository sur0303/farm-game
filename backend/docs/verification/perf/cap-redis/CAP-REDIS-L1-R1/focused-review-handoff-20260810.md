# CAP-REDIS-L1-R1 focused review handoff

## Exact heads

- Product/evidence base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Harness code head: `239b9d5fbfea9fa7863e889417866edab385e827`
- Correctness-stop evidence head: `4778d4df5e12f481096509459e1e86307ddeedf3`
- Branch: `codex/cap-redis-l1-r1-20260809`

The worktree is clean. Nothing was pushed or merged.

## Writer boundary

Changed paths are limited to:

```text
cmd/cap-redis/**
scripts/perf/cap-redis/**
docs/verification/perf/cap-redis/**
```

No production Handler, Actor rule, Redis Lua, Workflow rule, Proto, H5 code, or production metrics exporter changed.

## Mechanical gates

```text
go test ./... -count=1                          PASS
go test -race ./cmd/cap-redis -count=1          PASS
go vet ./...                                    PASS
go build ./cmd/...                              PASS
bash -n scripts/perf/cap-redis/run-l1-r1        PASS
python3 -m py_compile scripts/perf/cap-redis/*  PASS
manifest/machine preflight                      PASS
git diff --check                               PASS
```

## Evidence to review

Valid harness smoke:

- `capredis-l1-r1-20260810T024809Z-1853273`
- 24/24 primitive cells PASS at uniform concurrency 1.
- Exact wire bytes are nonzero for Redis cells.
- Fixture Due is observed before teardown: Farm SourceOrder `4/4`, Workflow Create `4/4`, Workflow Advance `4/4`.
- Hard gates zero and `correctness_observation_complete=true`.

Correctness stop:

- `capredis-l1-r1-20260810T024828Z-1874556`
- FriendPair Activate, uniform Pair keys, concurrency 10, planned 300.
- Completed `290`, failed `10`.
- All failures: `ErrFriendReconcile` (`bucket receipt count`).
- `friend_authority_reconcile_required_total=10`.
- Verdict `FAIL_CORRECTNESS`; observation incomplete; no later pressure was run.

## Required independent checks

1. Verify frozen Commit/Image/Profile/Machine bindings and absence of credentials.
2. Verify request/response bytes are exact connection-level RESP deltas and setup/diagnostics are outside the measured snapshot.
3. Verify command/pipeline counts, Redis SLOWLOG Lua time, pool wait, CPU, allocation, RTT, serialization, and payload quantiles are not mislabeled.
4. Verify expected nonterminal fixture Due is observed before exact namespace teardown and cleanup is not used to manufacture `due_residual=0`.
5. Decide whether the Friend signal is a real concurrent observation bug:
   - `ActivateFriendRelation → GetPair → validateAuthorityEnvelope`;
   - bucket metadata `HMGET` and receipt/attempt/source `ZCARD` are separate commands;
   - concurrent atomic commits can advance the shared bucket between those reads.
6. Reject any proposed workaround that removes checks, swallows `ErrFriendReconcile`, deletes DueIndex, changes Lua/business semantics, or silently serializes the benchmark.
7. Confirm the business-command amplification table remains `NOT_MEASURED`, formal rounds remain `0/5`, and L2/Mixed remain blocked.

## Requested verdict

```text
P0 = ?
P1 = ?
P2 = ?
EXACT_HEAD_CLOSURE = ?
FRIEND_SIGNAL = CONFIRMED_PRODUCT | HARNESS_MISUSE | NEEDS_MORE_EVIDENCE
```

Until an independent verdict closes this handoff, status remains `FAIL_CORRECTNESS / REVIEW_PENDING` and no CAP-REDIS-L1-R1 warmup or formal round may start.
