# CAP-REDIS-L1-R2 Independent Evidence Review Input

状态：`READY_FOR_INDEPENDENT_REVIEW`

Reviewer 必须只读；不得在候选分支修改 Evidence 或生成自己的 PASS 附件。

## Exact Identity

```text
Backend Business Head = 5ce43715363eea4f18e0925b6992429785053261
Authorized Verification= 5ce43715363eea4f18e0925b6992429785053261
H5 Head               = 60707faaa30797a8d739af6686186fd8f113626a
Harness Execution Head= bcdea2d766bf381915a7a73ea8c16d601f726f51
Harness Binary SHA256 = 85cd91ef248d74ed06d4d6554f0b46a8b769613e1e11280d0e3d7a6450ba5747
Fault Test Head        = fd99df569e42232cce2eb63323c9d09a2cb41b8a
Business Trace Tool    = bdd1d588820a6c23ec19400eb7d1a98a0786dc6b
```

Agent A handoff and Reviewer receipt were re-hashed before execution:

```text
Handoff = 2e6f08c771ea55467907b03708c97cad0b3cdb4efd41bf3ccfbd436f9bea30f9
Receipt = e751bd8ba4834862be392b892fd8a5dc0c5beded4f736c1421adc002b0a4dd05
```

## Selected Train

```text
Train Run = capredis-l1-r2-20260810T114729Z-664196
Warmup    = 1/1 PASS
Formal    = 5/5 PASS
Cells     = 192 per round
Operations= 691200/691200/0
Correctness violations = 0
Evidence Index SHA256 = c9265a6dbba4d531cf530a8b8a9359191b02c60dd9c409bbbdbf1c271dea69ca
```

R2 qualification must independently confirm:

```text
legacy relationship reads  = 0
legacy relationship writes = 0
v1 fallback                = 0
Model V2 migrations        = 2 observed
Model V2 sparse commits    = 2 observed
remote_player RPC          = observed
CompleteSnapshotV2         = observed in every round
Batch script calls         = exact unique buckets in every round
InvalidOrder               = 0
StepUnknown residual       = 0
Due residual               = 0
Namespace residual         = 0
```

Fault gate candidate:

```text
faults/fault-gate-verdict.json
NOSCRIPT                       PASS
Connection Reconstruction      PASS
Post-write Response Loss       TIMEOUT_UNKNOWN
Durable resolution             COMMITTED
Same-identity replay           ALREADY_COMMITTED
```

## Selected Business Trace

Only this run is selected:

```text
business-trace/capredis-business-e2e-r2-20260810T120419Z
Trace aggregate SHA256 = 5b80dc6fd66a2b7d49c71d308cf7bf05aac038ebfe4f59074b5f63c2c6b8c37e
Bundle aggregate SHA256= afd1ea55fd3325125ea7297634e5a56fb219592b961bc39b2457e1cf653950f6
Friendship mode        = remote_player
Commands               = BUY/SELL/CLAIM_TASK/PLANT/HARVEST/WATER/THEFT
```

Publishable attachment allowlist and SHA-256:

```text
business-command-amplification.json 5aa3f45f67cd249581d38e6c4333f60b064a6d9d087195b1473dbda3df18be11
cleanup.txt                         84442a9d7c5a9a562b643400998900ada7d7c2f82a97e0f21e41731a85028648
correctness-witness.json            5bb29364e582a48f0a908f3456bd9e63b350542e1116cdda555759571b9e032f
h5-bundle.json                      ca54258c90df7f491f5724e45addab062c77b634fc84fd816c15254317f4fd7a
qualification.json                   2d869ed817741e7aacfee2d2484b6ad21ec8d4e2e18ce903f9adaa4d65e4228b
tool-artifacts.json                  95e7afefdf6e90180db2ad3a3d63a98319fb40ebf10caed42c1a3e7fc2104359
```

No stack logs, RESP arguments, raw operation/session/workflow/receipt IDs, Redis URL, credential, or raw Player ID are in the selected allowlist. Raw derivation artifacts were deleted by the qualified tool after closed witness hashes were produced. Redis and H5 temporary overlays were exactly removed.

## Required Reviewer Checks

- [ ] Exact candidate Head and Train paths exist and hashes match.
- [ ] All six rounds match frozen identity, Profile and matrix.
- [ ] Warmup excluded; Formal count is exactly five.
- [ ] All planned/completed/failed and hard correctness totals are recomputed.
- [ ] Every round observed CompleteSnapshotV2 and calls equal unique buckets.
- [ ] R2 qualification attachments and hashes close legacy/V2/remote-player gates.
- [ ] Fault attachment contents derive every closed fault check.
- [ ] Seven Trace rows are real operation-bound RESP observations, not primitive synthesis.
- [ ] Business/Verification/H5/Tool identities are exact.
- [ ] Correctness witness and remote_player mode are PASS.
- [ ] Published allowlist Secret/raw-ID scan is clean.
- [ ] Unique Redis instances were loopback-only and exactly destroyed; no `FLUSHALL`/`SCRIPT FLUSH`.
- [ ] `baseline_eligible=false`, `capacity_claim=NONE`, `supports_30m_dau=false`.

## Required Reviewer Output

```text
REVIEWED_HEAD=<exact evidence candidate head>
TESTED_R2_BUSINESS_HEAD=5ce43715363eea4f18e0925b6992429785053261
TESTED_H5_HEAD=60707faaa30797a8d739af6686186fd8f113626a
P0=<n>
P1=<n>
P2=<n>
PRIMITIVE_SMOKE=PASS|FAIL
WARMUP=PASS|FAIL
FORMAL=PASS|FAIL
FAULT_GATES=PASS|FAIL
BUSINESS_TRACE_7_OF_7=PASS|FAIL
R2_HARD_ASSERTIONS=PASS|FAIL
EVIDENCE_HASH_SCAN=PASS|FAIL
SECRET_RAW_ID_SCAN=PASS|FAIL
EXACT_HEAD_CLOSURE=PASS|FAIL
CAP_REDIS_L1_R2=PASS|FAIL
```

Writer 当前只声明 `INCONCLUSIVE_REQUIRED_GAPS`，直到独立 Reviewer 生成匹配上述 allowlist 的 `business-trace/review-verdict.json`。
