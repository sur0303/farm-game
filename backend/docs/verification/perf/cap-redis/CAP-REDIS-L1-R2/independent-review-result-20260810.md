# CAP-REDIS-L1-R2 Independent Review Result

Review role：独立 Reviewer worktree（候选分支只读）

```text
REVIEWED_HEAD=f526ae07f700b78de4c33f44207550263d8fe2ce
TESTED_R2_BUSINESS_HEAD=5ce43715363eea4f18e0925b6992429785053261
TESTED_H5_HEAD=60707faaa30797a8d739af6686186fd8f113626a
P0=0
P1=0
P2=0
EXACT_HEAD_CLOSURE=PASS
CAP_REDIS_L1_R2=PASS
```

## Identity and scope

- Agent A handoff SHA-256：`2e6f08c771ea55467907b03708c97cad0b3cdb4efd41bf3ccfbd436f9bea30f9`，PASS。
- Heavy-Evidence Reviewer receipt SHA-256：`e751bd8ba4834862be392b892fd8a5dc0c5beded4f736c1421adc002b0a4dd05`，bundle `sha256sum -c` PASS。
- `5ce4371...` 相对 `0187051...` 的非文档产品 Diff 为零。
- `5ce4371...` 到 Reviewed Head 的变更全部位于 `cmd/cap-redis/**`、`scripts/perf/cap-redis/**`、`docs/verification/perf/cap-redis/**`。
- Backend/H5 immutable image ID 与对应 Head label 精确匹配 Manifest。
- Profile、Redis、Route、Lease、Machine 和 Frozen Identity canonical Hash 全部复算匹配。

## Evidence derivation

Evidence Index：

```text
SHA256=c9265a6dbba4d531cf530a8b8a9359191b02c60dd9c409bbbdbf1c271dea69ca
entries=48
sha256sum -c=PASS
```

从六轮 JSON 独立复算：

```text
Warmup=1/1 PASS
Formal=5/5 PASS
Scenarios=24
Cells per round=192
Operations=691200/691200/0
Correctness violations=0
Hard correctness gates=0
Observability gaps inside rounds=0
```

所有轮次满足：

- Frozen/Profile/Redis/Machine/Route/Lease/Model identity 一致；
- Redis Endpoint Hash 和 Namespace 六轮唯一；
- 4 CPU、4 GiB、no swap expansion、Redis image ID 一致；
- Redis config 值一致：persistence off、3 GiB noeviction、Slowlog threshold 0；
- Warmup 在五轮 Formal 之前，轮次时间不重叠；
- 每个场景精确覆盖 uniform/hot × C1/C10/C100/C300 × 600 operations；
- 所有 fixture Due `expected == observed`，所有 outcome count 为 600。

## R2 hard assertions

```text
legacy relationship reads  = 0
legacy relationship writes = 0
v1 fallback                = 0
Model V2 migrations        = 2 observed
Model V2 sparse commits    = 2 observed
remote_player RPC          = observed
CompleteSnapshotV2         = observed every round
Batch calls                = unique buckets every round
Aggregate snapshot calls   = 504228/504228
InvalidOrder               = 0
StepUnknown residual       = 0
Due residual               = 0
Namespace residual         = 0
```

Reviewer 从 `friendAuthorityCompleteSnapshotSource` 独立计算 SHA-1，并与所有 Cell 记录的 Script SHA 精确匹配。Legacy roster 删除、禁止符号、Model V2 real-Redis migration/sparse commit 和 remote_player RPC 由 Reviewer 在新临时 Redis 上再次运行，结果 PASS，残余容器为零。

## Fault gates

附件 Hash、固定输出标记及 Verdict 全部重新派生。Reviewer 又在两个新建 loopback 临时 Redis 上重跑：

```text
NOSCRIPT exact-source reload       PASS
Connection Reconstruction          PASS
Post-write Response Loss           TIMEOUT_UNKNOWN
FenceAndClaim resolution           COMMITTED
Same-identity replay               ALREADY_COMMITTED
Exact instance teardown            PASS
FLUSHALL                            not used
SCRIPT FLUSH                        not used
```

`FAULT_GATES=PASS`。

## Seven real-H5 traces

Selected trace：`capredis-business-e2e-r2-20260810T120419Z`。

- 七命令集合精确为 BUY、SELL、CLAIM_TASK、PLANT、HARVEST、WATER、THEFT；
- 每行均有 Redis transport、Actor Commit 和等量 Receipt；
- Redis error/unanswered request 为零；
- Operation binding 为七个不同的 64-hex domain-separated Hash；
- rows JSON SHA-256 独立复算为 `5b80dc6fd66a2b7d49c71d308cf7bf05aac038ebfe4f59074b5f63c2c6b8c37e`；
- browser/protocol/authority/stack 和 `remote_player` witness 全部 PASS；
- 六个发布附件 Hash 全部匹配 Qualification；
- Trace binary 和 CAP harness binary 从精确源码重建后 SHA-256 一致；
- H5 `60707f...` 从干净源码重新构建，文件清单、逐文件 Hash 和 aggregate Hash 完全一致；
- Reviewer 在独立临时环境再次运行七命令 Trace，7/7 PASS。

```text
BUSINESS_TRACE_7_OF_7=PASS
```

## Security and cleanup

- 对全部 48 项 Selected Evidence 运行 Secret/raw-ID scan：PASS。
- Selected Trace 只有六个 allowlisted attachment；没有 stack log、RESP argument、raw Player/Operation/Session/Receipt/Workflow ID。
- 无 symlink、raw Redis URL、credential、query token 或 Cookie。
- Redis/H5 临时实例和 Overlay 均精确销毁，残余容器为零。
- 工具源码没有执行 `FLUSHALL` 或 `SCRIPT FLUSH` 的调用。

```text
EVIDENCE_HASH_SCAN=PASS
SECRET_RAW_ID_SCAN=PASS
```

## Reviewer replay gates

```text
go test -race ./cmd/cap-redis -count=20  PASS
go test ./... -count=1                   PASS
go vet ./...                             PASS
go build ./cmd/...                       PASS
Python summarizer tests 15/15            PASS
Manifest preflight                       PASS
Independent R2 live qualification        PASS
Independent Fault replay                 PASS
Independent 24-Primitive Smoke           PASS
Independent 7-command business trace     PASS
Binary/H5 reproducibility                PASS
```

独立 Smoke replay：`capredis-l1-r2-20260810T121855Z-811967`，`24/24 PASS`。

## Final reviewer verdict

```text
PRIMITIVE_SMOKE=PASS
WARMUP=PASS
FORMAL=PASS
FAULT_GATES=PASS
BUSINESS_TRACE_7_OF_7=PASS
R2_HARD_ASSERTIONS=PASS
EVIDENCE_HASH_SCAN=PASS
SECRET_RAW_ID_SCAN=PASS
EXACT_HEAD_CLOSURE=PASS
P0=0
P1=0
P2=0
CAP_REDIS_L1_R2=PASS
```

本 Review 不改变：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```
