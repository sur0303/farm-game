# Friend hot fixture + Share bucket Due filter handoff

## Exact identity

- Integration/stop base: `be41b4ea911ec8705e10c787f78f48829cbc49d7`
- C1 hot fixture/general diagnostics head: `137cfbf95f217b3f955228d1eccc6e4277708907`
- Product Share filter code head: `684601d72b46dee953b6cfc88e514a5ac09cfd0b`
- Product real-Redis test head: `ef2327059e60184b533ce1eec47ca165385fb5cb`
- Exact composition head: `d34e031d5689106589d305ae55ddd1682fa47a0c`
- Focused matrix: `capredis-l1-r1-20260810T050414Z-2862793`

## Retained stops

The failed Warmup `capredis-l1-r1-20260810T045335Z-2772487` remains `FAIL_CORRECTNESS` with 600 failed Quota hot operations and incomplete diagnostics. Those operations remain `INSUFFICIENT_EVIDENCE`; this change does not infer or rewrite their durable state.

A subsequent setup readback, before any measured operation, found:

```text
GenerateShareToken = SUCCEEDED
GetIssuer           = ErrFriendReconcile: share receipt due locator
```

No Share measurement was started after that signal.

## C1 correction

Quota and Share cells labelled `idempotent_replay` now commit exactly one fixture operation before the measured metric snapshot. Setup verifies the canonical authority read model and record version. The measured window therefore contains replay only, matching the Pair hot contract.

Friend authority mutation diagnostics are generalized across Pair, Quota and Share:

- frozen public and participant locator formulas have permanent vectors;
- exact Record, Receipt partition/field and Attempt partition/state are retained in memory only;
- persisted operation/authority identity is domain-separated SHA-256;
- no `SCAN`, `HSCAN`, pattern lookup or cursor loop;
- Pair/Quota/Share canonical readback plus fixed bucket metadata/Due counts;
- any read/key gap forces `UNKNOWN`, aggregate collection error and incomplete status;
- raw API errors remain allowlisted/redacted.

## Product correction

`ReceiptDueIndex` is bucket-scoped. `GetIssuer` previously treated a valid Due member belonging to another Share issuer in the same bucket as corruption of the requested issuer.

The read now:

1. still rejects malformed members and wrong authority kinds with `ErrFriendReconcile`;
2. skips a valid same-kind member whose authority ID belongs to another issuer;
3. fully validates and reads only members for the requested issuer.

No Lua, write path, receipt, counter, key/slot, retry or business rule changed.

Permanent controls:

```text
same Bucket, two Share issuers, both GetIssuer reads     PASS ×100
wrong authority-kind Due member still ErrFriendReconcile PASS ×100
focused Race                                             PASS
real Redis 7.4 same-Bucket filter                         PASS ×20
```

## Focused matrix

```text
Quota uniform c1/10/50/100            1200/1200
Quota committed replay c1/10/50/100   1200/1200
Share uniform c1/10/50/100            1200/1200
Share committed replay c1/10/50/100   1200/1200
total                                  4800/4800
failed                                 0
Friend reconcile                       0
verdict                                PASS
```

Harness binary SHA-256:

```text
64a1cfb83f1969f87ffeb877aab2760b0ceea93b34269e76078b3c6de386c3da
```

Full Go test, Vet, Build and focused Race pass on the exact composition.

## Requested review

```text
P0 = ?
P1 = ?
P2 = ?
HOT_FIXTURE_CONTRACT = PASS | RETURN
GENERIC_DIAGNOSTICS = PASS | RETURN
SHARE_BUCKET_DUE_FILTER = PASS | RETURN
EXACT_HEAD_CLOSURE = PASS | FAIL
```

Formal L1 remains unauthorized until independent review, Premerge, integration/image rebinding and another restart from Smoke.
