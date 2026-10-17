# CAP-REDIS-L1-R1 Real Business Amplification Trace Handoff — 2026-08-10

## Exact composition

- Integration base / current Evidence Head at trace start: `5b42a4f587263ab0ce8369330321345e240782c7`
- Backend Business Commit: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Backend image (unchanged): `sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7`
- H5 Commit: `525c4d324100b17a891f02797c4880fa9a02f70a`
- Frozen trace-tool Head used by the published run: `837d96be63295d23be4b94b471acb8d235acfffb`
- Code plus published evidence Head: `dbe5de43188582663115c5a58f03479637237888`
- Full primitive train: `capredis-l1-r1-20260810T053256Z-3107651`
- Business trace run: `capredis-business-e2e-20260810T063600Z`
- Trace aggregate SHA-256: `af4730da18b5348999f40ddd9fb1a5698e01db4c79fd3aeef2b8de2de4239269`

The trace has not yet been accepted by `summarize.py`: the summary remains fail-closed until an independent `business-trace/review-verdict.json` binds the exact evidence.

## Measurement contract

1. A real prebuilt static H5 bundle drives real public WebSocket commands through Gate, Player/Farm services and real Redis 7.4.
2. The browser begins a bounded command window before submission. Its decoded Wire frame binds the exact Operation ID to the loopback trace controller.
3. A transparent RESP proxy retains request/reply frames only in bounded process memory. It follows the Operation ID plus `sid1_` derived workflow/order aliases to include downstream participant and workflow work while excluding unrelated worker/friend polling.
4. Persisted evidence contains only domain-separated Operation hashes, allowlisted command/category counts and exact RESP request/reply byte counts. No RESP argument, key, payload, credential, raw identity or raw Redis error is persisted.
5. `Actor Commit` is recognized only by the frozen five-key physical vector `shard-authority / actor / due / receipt-due / bucket-meta`; `Receipt` is the corresponding durable AuthorityReceipt.
6. `Order` counts attached SourceOrder keys. `Workflow writes` and `Due` count operation-bound workflow mutation invocations, not synthesized primitive averages or unique-key estimates.
7. A row closes only after H5 state convergence and `pending-ops=0`; any Redis error, unanswered request, missing commit/receipt, missing Wire binding or failed browser window fails the trace.

## Measured table

| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total RESP bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 0 | 0 | 0 | 1 | 1 | 2,246 |
| SELL | 1 | 0 | 0 | 0 | 1 | 1 | 3,580 |
| CLAIM_TASK | 1 | 0 | 0 | 0 | 1 | 1 | 5,303 |
| PLANT | 3 | 7 | 0 | 7 | 3 | 10 | 44,484 |
| HARVEST | 2 | 5 | 0 | 5 | 2 | 7 | 25,130 |
| WATER (`CARE`) | 2 | 0 | 1 | 1 | 2 | 36 | 51,040 |
| THEFT | 2 | 5 | 0 | 5 | 2 | 25 | 60,124 |

These are observed end-to-end rows from one correctness-qualified run. They are not performance targets and do not claim throughput, DAU support or improvement.

## Correctness and isolation witnesses

```text
real H5 browser verdict            PASS
exact seven Wire commands          PASS
independent Go protocol bot         PASS
post-run Authority business oracle PASS
combined authority stack verdict   PASS
Redis errors / unanswered requests 0 / 0
operation bindings                 7 unique domain-separated hashes
H5 bundle mode                     PREBUILT_STATIC_BUNDLE
Redis teardown                     DESTROYED_UNIQUE_EPHEMERAL_INSTANCE
SCRIPT FLUSH / FLUSHALL             false / false
```

Published allowlisted evidence:

`docs/verification/perf/cap-redis/CAP-REDIS-L1-R1/results/capredis-l1-r1-20260810T053256Z-3107651/business-trace/capredis-business-e2e-20260810T063600Z/`

`qualification.json` binds SHA-256 for the measured table, prebuilt bundle manifest, redacted correctness witness and teardown witness.

## Harness-attempt ledger

No product correctness failure occurred during trace development. Tool attempts remain explicitly classified:

| Attempt | Stack correctness | Trace verdict | Classification |
|---|---|---|---|
| `diag1` | not started | not produced | HARNESS_INVALID_NAMESPACE; bounded stack namespace fixed |
| `diag2` | PASS | FAIL | TRACE_FILTER_INSUFFICIENT; temporal window included unrelated background polling |
| `diag3` | PASS | FAIL | TRACE_ALIAS_INCOMPLETE; derived participant commits were not yet linked |
| `diag4` | PASS | PASS | diagnostic proof of operation-bound alias filter |
| `capredis-business-e2e-20260810T063600Z` | PASS | PASS | published candidate |

The failed trace attempts were not relabeled as measured business rows and are not used by the summary.

## Local gates

```text
go test -race ./cmd/cap-redis/...       PASS
go vet ./cmd/cap-redis/...              PASS
go build ./cmd/cap-redis/...            PASS
business/fault summary negative tests   PASS (12/12)
H5 Playwright test discovery             PASS (1 exact test)
real published end-to-end run            PASS
```

## Focused review request

Review exact evidence Head `dbe5de43188582663115c5a58f03479637237888` against base `5b42a4f587263ab0ce8369330321345e240782c7`. Fail closed on:

1. any causal Redis request omitted because it carries neither the root Operation ID nor a derived stable alias;
2. any unrelated background request admitted through an alias collision;
3. RESP request/reply mispairing under pipelining or RESP3 framing;
4. Actor Commit, Workflow write, Order, Due or Receipt over/under-counting;
5. a raw identity, key, argument, payload, credential, endpoint or product error in published evidence;
6. mismatch between the H5 Wire command, trace row, Business/H5 commit, prebuilt bundle or correctness oracle;
7. cleanup/global-mutation violations or any attempt to synthesize this table from Primitive results;
8. enabling L2/Mixed or a capacity claim before exact-head review and Premerge closure.

Until review closes:

```text
verdict=INCONCLUSIVE_REQUIRED_GAPS
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
L2=BLOCKED
Mixed=BLOCKED
```
