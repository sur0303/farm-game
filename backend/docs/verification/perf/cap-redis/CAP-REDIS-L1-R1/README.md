# CAP-REDIS-L1-R1

Current state: **`FAIL_CORRECTNESS` during matrix preflight; formal L1 not started.**

See:

- `manifest.json` — frozen product/image/environment identity
- `profile.json` — Redis/client/matrix/route/lease profile
- `status.json` — machine-readable train state
- `attempts.json` — retained-attempt classification
- `friend-authority-correctness-stop-20260810.md` — blocking incident
- `results/` — exact canonical smoke and correctness-stop evidence

## Scope and boundaries

The harness lives only under:

```text
cmd/cap-redis/**
scripts/perf/cap-redis/**
docs/verification/perf/cap-redis/**
```

It does not change production Handlers, Actor rules, Redis Lua, Workflow semantics, or H5 code.

The measured primitive registry covers:

- Player/Farm Actor `LoadAndClaim`, Commit + Receipt, Commit + SourceOrder + DueIndex, and timeout resolution through `FenceAndClaim`;
- Workflow Create, Advance, exact Claim, and Renew;
- ResultDirectory publish;
- FriendPair activation, FriendQuota reserve, ShareIssuer generation, 20-pair canonical batch read, FriendIndex batch scan, and projection repair;
- SessionRoute R1/R4 renew, stale and partial R4 handling;
- Subscription R1/R4 deterministic protobuf serialization (zero Redis commands in the current resident FarmActor design);
- Shard authority lease renew.

## Measurement method

- A temporary Redis 7.4 container is published on loopback only and destroyed by exact container name.
- Every attempt uses a unique namespace; cleanup uses bounded `SCAN` plus exact `DEL`. `FLUSHALL` is forbidden and unused.
- Redis credentials are generated per container and never written to evidence.
- A connection wrapper records exact RESP request/response bytes.
- go-redis hooks record top-level command counts and pipeline item counts.
- Redis `SLOWLOG` at threshold zero records Lua duration; setup and diagnostic calls are outside the wire-byte measurement snapshot.
- Pool `WaitDurationNs`, process allocation/CPU deltas, Redis CPU deltas, deterministic logical serialization sizes, marshal/unmarshal times, and isolated PING RTT are recorded separately.
- Payload sizes use a frozen deterministic distribution. Per-operation hard limits may bound a payload below the global maximum and the observed quantiles remain explicit.
- Nonterminal Create/Advance/Claim/Order microbench fixtures report `expected_fixture_due` and `observed_fixture_due` before teardown. Namespace deletion is cleanup only; it is never used as proof that a business Due residual was zero.

## Formal execution rule

After the correctness blocker closes:

```text
1 uncounted warmup
5 formal rounds
uniform + hot distributions
concurrency 1/10/100/300
600 operations per cell
```

The summarizer excludes warmup, reports min/max and cross-round variation, and must not synthesize the required business-command amplification table from primitive averages. BUY/SELL/PLANT/HARVEST/WATER/THEFT/CLAIM_TASK remain `NOT_MEASURED` until actual end-to-end Redis traces exist.

`baseline_eligible` remains false throughout L1. L2 and Mixed B0 remain blocked until L1 passes independent review.
