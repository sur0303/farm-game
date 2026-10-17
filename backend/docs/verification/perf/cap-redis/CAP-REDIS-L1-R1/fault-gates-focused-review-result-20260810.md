# CAP-REDIS-L1-R1 Fault Gates — Focused Review Result — 2026-08-10

## Exact review closure

- Review base: `1890e70b973f193f04992642a036d41699c0e654`
- Initial handoff: `37c7d9879bf17ecd24c7e26e70b00afc5bc2b092`
- Initial finding: `P1-EVIDENCE-DERIVATION-01`
- Amendment code/test: `a134077365cf3dcf77683cee21c237b4faf20854`
- Final exact reviewed head: `9e5c88a480749affa3ab80eb8c329a19c55c516d`
- Frozen fault-test head: `5fbbda4ec521c1ea7590f867dc2f7186b4ca307e`
- Business commit: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Full train: `capredis-l1-r1-20260810T053256Z-3107651`

Remote `codex/cap-redis-fault-gates-20260810` resolved exactly to the reviewed head before closure.

## Finding disposition

### P1-EVIDENCE-DERIVATION-01 — CLOSED

The initial summary accepted the attachment names supplied by the verdict and trusted boolean checks after hash validation. It did not require the frozen attachment set or derive the fault outcomes from attachment content.

The amendment now requires the exact four attachments, validates every SHA-256, derives all nine checks from bounded marker content, rejects failed Go-test markers, binds the Business Commit and test Head, and compares derived checks with the verdict. Negative controls for tampering, incomplete output, false checks, wrong binding, path escape and missing attachments passed `7/7`.

## Independent execution

```text
go test ./cmd/cap-redis -count=1                         PASS
go test -race ./cmd/cap-redis -count=1                   PASS
go vet ./cmd/cap-redis                                    PASS
python3 scripts/perf/cap-redis/test_summarize.py          PASS (7/7)
summary regeneration + zero diff                          PASS
real Redis connection reconstruction                      PASS (dial 2 -> 3)
real Redis post-write response loss                       PASS (TIMEOUT_UNKNOWN -> COMMITTED)
real Redis NOSCRIPT exact-source reload                   PASS
real Redis same-identity replay                           PASS (ALREADY_COMMITTED)
CAP fault namespace post-cleanup DBSIZE                    0
Q2R isolated Redis instance teardown                      PASS
secret/scope/global-mutation static review                 PASS
```

## Final findings

```text
P0=0
P1=0
P2=0
P1-EVIDENCE-DERIVATION-01=CLOSED
EXACT_HEAD_CLOSURE=PASS
FAULT_SIGNAL=CLOSED
CLASSIFICATION=REQUIRED_FAULT_GATES_QUALIFIED
```

No production code or measured CAP binary changed. No credential, endpoint, raw client ID or raw product error was persisted. No `SCRIPT FLUSH`, `FLUSHALL`, database `KEYS`, or unbounded diagnostic traversal was introduced.

## Eligibility boundary

The fault gates are qualified, but `CAP-REDIS-L1-R1` remains `INCONCLUSIVE_REQUIRED_GAPS` because the seven-command real end-to-end business amplification trace is still absent. Therefore:

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
CAP-SINGLE-POD-L2-R1=BLOCKED
CAP-MIXED-B0-R1=BLOCKED
```
