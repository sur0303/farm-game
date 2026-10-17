# Friend hot fixture + Share bucket Due filter review result

## Exact identity

- Review base: `be41b4ea911ec8705e10c787f78f48829cbc49d7`
- C1 code: `137cfbf95f217b3f955228d1eccc6e4277708907`
- Product code: `684601d72b46dee953b6cfc88e514a5ac09cfd0b`
- Product real-Redis test: `ef2327059e60184b533ce1eec47ca165385fb5cb`
- Composition/evidence head: `244e99dd69de32aedf38e9ecc360f038c5a632c9`

## Verdict

```text
P0 = 0
P1 = 0
P2 = 0
HOT_FIXTURE_CONTRACT = PASS
GENERIC_DIAGNOSTICS = PASS
SHARE_BUCKET_DUE_FILTER = PASS
EXACT_HEAD_CLOSURE = PASS
```

## Findings

### Hot fixture contract

PASS. Pair, Quota and Share cells carrying an `idempotent_replay` label all commit and read back one fixture before metric capture. Their measured windows contain only same-identity/same-payload replay. Uniform cells continue to use fresh identities.

Setup work is excluded from measured command/byte/latency counters and is explicitly recorded in cell notes. Concurrent first execution remains visible in the retained failed Warmup; it was not renamed replay or removed.

### Generic diagnostics

PASS. Pair/Quota/Share mutations provide exact in-memory Record/Receipt/Attempt locators. Public and participant digest formulas are frozen by independent vectors. Persistence includes only domain-separated identity hashes and an allowlisted API error code.

Every Redis/key/read failure is fail-closed as `UNKNOWN` with incomplete collection. The collector contains no `SCAN`, `HSCAN`, cursor traversal, random retry or unbounded loop. Quota snapshot retries are the existing fixed product bound.

The original 600 Warmup failures remain:

```text
classification = INSUFFICIENT_EVIDENCE
```

No retroactive classification is permitted.

### Share bucket Due filter

PASS. `ReceiptDueIndex` is bucket-scoped, so a valid same-kind member for another Share issuer is expected. The fix skips only that foreign authority after validating member encoding and kind. Entries for the requested authority still receive complete digest, exact Receipt and typed RotateReceipt validation.

Wrong-kind and malformed entries remain `ErrFriendReconcile`. No write path, Lua, count invariant, key/slot, TTL or business behavior changed.

## Independent gates

```text
go test ./internal/friend ./cmd/cap-redis -count=3        PASS
focused Race                                               PASS
go vet ./...                                               PASS
go build ./cmd/...                                         PASS
scope allowlist                                            PASS
bounded/secret review                                      PASS
real Redis same-Bucket Share filter ×20                   PASS
```

Focused matrix `capredis-l1-r1-20260810T050414Z-2862793`:

```text
Quota uniform + committed replay = 2400/2400
Share uniform + committed replay = 2400/2400
total                              = 4800/4800
failed                             = 0
hard correctness gates             = 0
```

## Authorization boundary

This review authorizes Premerge only. Because product code and the CAP binary changed, integration Business/Evidence Heads and the backend image/runner binding must be updated before another restart from Smoke.

```text
Warmup = 0/1
Formal = 0/5
baseline_eligible = false
capacity_claim = NONE
supports_30m_dau = false
```
