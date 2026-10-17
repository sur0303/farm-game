# CAP-REDIS-L1-R1 Fault Gates Handoff — 2026-08-10

## Exact composition

- Base integration/evidence head: `1890e70b973f193f04992642a036d41699c0e654`
- Backend business commit: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Backend image: `sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7`
- Live fault-test code head: `5fbbda4ec521c1ea7590f867dc2f7186b4ca307e`
- Code plus frozen evidence head: `620ee7db9c49168a03d15978728c6cb710d586ce`
- Full train: `capredis-l1-r1-20260810T053256Z-3107651`
- Formal result remains `5/5 PASS`, `691200/691200/0`, with no correctness violation or namespace residue.

The live tests are opt-in (`CAP_REDIS_FAULT_URL`) and test-only. They do not change the measured CAP binary or production semantics.

## Required fault contracts

| Contract | Injection and resolution | Result |
|---|---|---|
| NOSCRIPT | Use a never-loaded, unique exact script SHA; first `EVALSHA` receives NOSCRIPT, registry loads the exact source and retries. No global flush. | PASS |
| Connection reconstruction | Kill the exact application connection by Redis client ID, prove that connection is bad, return it to the pool, then require a new dial and durable readback. Only a domain-separated client-ID hash is logged. | PASS (`dials 2 -> 3`) |
| Post-write response loss | Forward the production Authority Commit `EVALSHA`, consume and discard its Redis response, close the socket, and disable command retry. Commit must classify `TIMEOUT_UNKNOWN`. A normal connection then runs `FenceAndClaim` and must resolve `COMMITTED` from the durable receipt with one record-version advance. | PASS |
| Same-identity replay | Replay the exact committed identity on real Redis. | PASS (`ALREADY_COMMITTED`) |

Primary verdict:

`docs/verification/perf/cap-redis/CAP-REDIS-L1-R1/results/capredis-l1-r1-20260810T053256Z-3107651/faults/fault-gate-verdict.json`

Attachment SHA-256 values are embedded in that verdict and revalidated by `summarize.py`. The summary rejects a missing check, wrong business binding, malformed test head, path escape, or attachment hash mismatch.

## Isolation and teardown

- Two unique Redis 7.4 loopback containers were used.
- Authentication values were ephemeral and are absent from evidence.
- Persistence was disabled.
- The CAP fault instance reached `DBSIZE=0` after exact namespace cleanup.
- The Q2R instance was destroyed in full after its unique isolated probes.
- No `SCRIPT FLUSH`, `FLUSHALL`, database `KEYS`, or namespace-wide diagnostic traversal was used by the fault tests.

## Local gates

```text
go test ./cmd/cap-redis -count=1                         PASS
go test -race ./cmd/cap-redis -count=1                   PASS
go vet ./cmd/cap-redis                                    PASS
python3 scripts/perf/cap-redis/test_summarize.py          PASS (5/5)
real Redis connection/response-loss tests                 PASS (2/2)
real Redis NOSCRIPT/replay/exact-cleanup tests             PASS (3/3)
```

## Fail-closed train effect

The three fault gaps are closed only when `fault-gate-verdict.json` and every hash-bound attachment validate. The train remains:

```text
verdict=INCONCLUSIVE_REQUIRED_GAPS
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```

The sole remaining required gap is a real end-to-end Redis trace for each of `BUY`, `SELL`, `CLAIM_TASK`, `PLANT`, `HARVEST`, `WATER`, and `THEFT`. Primitive results are not summed or relabeled as business-command amplification. L2 and Mixed remain blocked.

## Focused review request

Review exact head `620ee7db9c49168a03d15978728c6cb710d586ce` against base `1890e70b973f193f04992642a036d41699c0e654` and fail closed on:

1. any path where the injected commit may not have reached Redis before response loss;
2. any retry that could hide `TIMEOUT_UNKNOWN`;
3. resolution not bound to the original idempotency key and canonical payload hash;
4. raw client IDs, credentials, endpoints, or product error strings in evidence;
5. global Redis mutation, non-isolated cleanup, or an unverified evidence attachment;
6. any attempt to clear the business amplification gap or grant L2/Mixed eligibility.
