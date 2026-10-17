# Friend envelope fix + CAP diagnostics exact composition handoff

## Exact composition

- Frozen Backend evidence base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- CAP evidence parent: `941673f01769dfe12fba2a4efc776aaff26665de`
- Product atomic-read code: `87444b3cf779f0306d9fe9b82d4033770ee20444`
- Product batch-metrics amendment: `b36b97cbd4ccec91194e04e8d3b98bece3156863`
- Product permanent matrix head: `5fe078a2d45157617fefb9d9fe90cda9fd9103f1`
- Delete concurrency test-boundary correction: `2785b5b866176a15adc8e5d23b106b81d5453291`
- CAP hot replay fixture head: `14f175115acc1a7034d9f804d0cb996c080bf63f`
- CAP exact bounded diagnostic head: `e7119770aff95461a0cae0e4b8649388b5b157aa`
- CAP diagnostic gap negative-control head: `2fae3ccb3348d5fce2517c47115c5d0e7606e040`
- Exact composition code/test head: `553d80b679cfac2b7ea2a7cdf9f7905b8e1c0cb7`

Only the product commits under `internal/friend/**` and C1 commits under `cmd/cap-redis/**` are composed. No H5, public wire contract, deployment, Actor/Workflow production semantics, baseline verdict, or capacity threshold changes are present.

## Review returns closed before handoff

1. Mixed-snapshot Friend bucket envelope read: replaced by one fixed read-only same-slot Lua snapshot.
2. Original per-operation evidence gap: exact bounded point locators and `COMMITTED/NOT_COMMITTED/UNKNOWN` evidence added.
3. Diagnostic `SCAN/HSCAN` traversal: removed; no cursor loop remains.
4. Raw API error persistence: replaced by an allowlisted redacted vocabulary.
5. Ignored diagnostic read errors: every point-read/key error now forces `UNKNOWN`, aggregate collection error, and incomplete status.
6. Batch metrics early return: every queued EVAL is accounted before fail-closed return.
7. Existing Delete race test: corrected to converge its contractually asynchronous termination order before asserting terminal quota state; no production semantics changed.

## Retained correctness stop

Run `capredis-l1-r1-20260810T040738Z-2498081` is intentionally retained as `FAIL_CORRECTNESS`:

```text
uniform c1/10/50/100 = 1200/1200
hot c1                = 300/300
hot c10               = 0/300
```

That run used harness binary `a8b4760...` before the hot fixture was primed. It concurrently issued the first execution of one participant identity while labelling the cell “idempotent replay”; the product correctly exposed attempt conflicts/reconcile states. The evidence is not excluded or renamed. The harness contract was corrected so hot measurement now starts only after one successful fixture commit; concurrent first execution is not represented as replay.

This retained failure is diagnostic evidence and is never baseline eligible.

## Final exact correctness evidence

### Original failing cell

Run `capredis-l1-r1-20260810T043043Z-2621668`:

```text
friend_pair_activate / uniform / c10
planned   = 300
completed = 300
failed    = 0
verdict   = PASS
```

### Friend matrix

Run `capredis-l1-r1-20260810T043045Z-2621667`, binary SHA-256 `ef2b9f3da18f80d4ffb988e1c54f6311589810e72f8fe67514b7bfd9d1576b47`:

```text
uniform distinct Pair: c1/c10/c50/c100 = 1200/1200
committed hot replay:  c1/c10/c50/c100 = 1200/1200
total                                      2400/2400
ErrFriendReconcile                         0
failed operations                          0
verdict                                    PASS
```

Permanent forced same-Bucket distinct-Pair matrix:

```text
c1/c10/c50/c100 × 300 = 1200/1200 PASS
```

Settled same-operation concurrent replay, different-Bucket isolation, Attempt compaction, stable real-corruption negative control, fixed Lua shape and NOSCRIPT reload all pass.

Real Redis 7.4 PhaseBarrier, temporary loopback instance:

```text
repeats               = 20
AtomicReadTotal       = 60
ReconcileTotal        = 0
TechnicalFailureTotal = 0
verdict                = REAL_REDIS_PASS
```

### Diagnostic negative control on frozen failing product

Final C1 validation `capredis-l1-r1-20260810T042815Z-2611306` remains on the CAP branch:

```text
failed/expected/captured = 13/13/13
diagnostics complete     = true
NOT_COMMITTED            = 13
raw identity leak         = false
business verdict          = FAIL_CORRECTNESS
aggregate evidence hash   = 483a75cf1eee25288c107319c04bd64527acdd997df690ba9a940c01848efaed
```

A closed-client negative control proves any diagnostic read gap returns `UNKNOWN` and a collection error.

## Mechanical gates

```text
go test ./internal/friend -count=1                              PASS
go test -race ./internal/friend -run AtomicEnvelope/PairBatch   PASS
go test ./cmd/cap-redis                                         PASS
go test -race ./cmd/cap-redis                                   PASS
go vet ./internal/friend ./cmd/cap-redis                        PASS
Delete race isolated normal                                    1000/1000 PASS
Delete race isolated Race                                      100/100 PASS
full internal/friend                                            20/20 PASS
```

No Redis container/process remains after real-Redis tests.

## Requested independent verdict

Reviewer must bind this exact composition and retained evidence, then publish:

```text
P0 = ?
P1 = ?
P2 = ?
P1-01 = PASS | RETURN
P1-02 = PASS | RETURN
EXACT_HEAD_CLOSURE = PASS | FAIL
FRIEND_SIGNAL = CLOSED | OPEN
```

Until that verdict is committed:

```text
L1 Warmup/Formal = NOT_RUN
baseline_eligible = false
capacity_claim = NONE
supports_30m_dau = false
```
