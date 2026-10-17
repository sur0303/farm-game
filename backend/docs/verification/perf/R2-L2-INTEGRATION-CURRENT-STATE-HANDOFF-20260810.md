# R2 / L2 Integration Current-State Handoff

日期：2026-08-10

用途：给 Controller、Capacity Writer、Staging Operator 和 Reviewer 提供一份不依赖聊天历史的精确交接。

## 0. 执行摘要

当前 Backend/H5 已完成 R2 产品合入、`CAP-REDIS-L1-R2`、H5 Integration Delta 合后复验，并已推送。本文件记录的受测快照如下；承载本文件的后续 documentation-only commit 不改变受测产品身份：

```text
Backend Qualified Integration = c781775a804af0988380a9cbb0dbd758ff20f6cd
H5 Integration      = f22dcb447e154e424656b12409e041c32dfb4c5c
Backend Business    = 5ce43715363eea4f18e0925b6992429785053261
CAP-REDIS-L1-R2     = PASS
required_gaps       = 0
```

但“R2 产品完成”和“L2 容量完成”是两个维度：

```text
R2 产品 + Redis L1 资格       已完成
Single-Pod L2 Harness 修复     已完成实现，待 Round 2 正式签字
CAP-SINGLE-POD-L2-R2           未运行
CAP-MIXED-B1-R2                未运行
Static K8s Staging 正式验收    未运行
3M/5M PCU 容量模型             未解锁
```

当前治理状态必须保持：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```

## 1. 名词边界

R2 是第二批产品结构改造：FriendshipDomain/remote_player、legacy roster 删除、Model Field V2、Worker readiness、Static K8s 等。

L1/L2/Mixed 是容量证据层级：

```text
L1    Redis Primitive、故障资格、真实业务 Redis Trace
L2    单 Pod 容量、Driver/Observability、开放到达、Formal×3
Mixed 多组件组合容量
```

因此 `CAP-REDIS-L1-R2=PASS` 不等于 `CAP-SINGLE-POD-L2-R2=PASS`；后者尚未运行。

## 2. 当前权威身份

### Backend

```text
R2 Business Head            = 5ce43715363eea4f18e0925b6992429785053261
CAP Final Binding Head      = 1df5305add7df195349482546a26df93ebe80d28
Qualified Integration Head  = c781775a804af0988380a9cbb0dbd758ff20f6cd
Remote at handoff snapshot  = c781775a804af0988380a9cbb0dbd758ff20f6cd
Branch                      = codex/integration-20260803
```

`5ce4371 → 1df5305 → c781775` 只增加 CAP 测试工具、Evidence 和 H5 合后资格文档，没有修改产品 Handler、Actor、Workflow、Redis Lua、Friendship 或 Route Runtime。

### H5

```text
Original CAP tested H5 = 60707faaa30797a8d739af6686186fd8f113626a
Current H5 Integration = f22dcb447e154e424656b12409e041c32dfb4c5c
Postmerge H5 image     = sha256:eb1bb441ab6c46ef647bb07e360d9f068f9728e07a82f18450ef85b633f96f80
```

H5 `60707f → f22dcb` 的四文件 Delta 没有被静默继承；已在 Backend CAP 合入后精确重测，见第 5 节。

### R2 测试镜像

```text
Redis image       = sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33
R2 Backend image  = sha256:672cf6862dcf06a4a5bfbb6146263b98bf04de930b113f7eb8fd8e8efb3493e9
Original H5 image = sha256:2d4fcba11bc0fdd83e49f97e1cca11bc190e0434b9a93dc5a2cf2672c380faa3
Postmerge H5 image= sha256:eb1bb441ab6c46ef647bb07e360d9f068f9728e07a82f18450ef85b633f96f80
```

## 3. R2 产品列车已完成内容

### Heavy-Evidence inheritance

```text
HEAVY_EVIDENCE_INHERITANCE=PASS
PRODUCT_DIFF_FROM_0187051=0
H5_IDENTITY=EXACT
READY_FOR_CAP_REDIS_L1_R2=true
```

```text
.agentctl-cp-route-state/AGENT-A-TO-B-R2-CAP-BINDING-HANDOFF-20260810.md
SHA256=2e6f08c771ea55467907b03708c97cad0b3cdb4efd41bf3ccfbd436f9bea30f9

.agentctl-cp-route-state/evidence/r2-heavy-evidence-inheritance-receipt-20260810T105701Z/SHA256SUMS
SHA256=e751bd8ba4834862be392b892fd8a5dc0c5beded4f736c1421adc002b0a4dd05
```

### Friendship canonical-only / remote_player

已完成：

- FriendshipService 支持 `local_gate | remote_shadow | remote_player`，release 主路径使用 `remote_player`；
- Gate 在 remote_player 下不构造本地 Friend Redis Authority/Lua/repair worker；
- legacy relationship roster、reader、writer、observer、环境开关和实现文件已物理删除；
- HTTPS Share projection 与 retired roster 明确分离；
- Pair、Quota、ShareIssuer、FriendIndex、RepairQueue 保持 canonical authority；
- VisitPermit/ActionAdmission 继续按 Pair/epoch/block/delete fence；
- public Block/Unblock 边界不变。

### Friend CompleteSnapshotV2

已完成单次只读 Lua 原子读取 bucket envelope、Due cardinality 和 canonical record；BatchGetPairs 对物理 bucket 去重，每个 unique bucket 精确一次 CompleteSnapshotV2，无逐 Pair client 拼接回退。CAP 六轮观测：

```text
CompleteSnapshotV2 every round
Batch calls == unique buckets
Aggregate calls = 504228 / 504228
```

### Actor Model Field V2

已完成 Player/Farm V2 registry、V1→V2 原子 migration、旧 record 原子删除、V2 sparse commit、one-way layout、partial/mixed/unknown manifest fail-closed、TIMEOUT_UNKNOWN durable resolution、old-holder fence。

CAP 实际观测：

```text
V1 sparse commits = 2
V2 migrations     = 2
V2 sparse commits = 2
v1 fallback       = 0
namespace residual= 0
```

### Worker readiness / Static K8s

Worker family readiness、`/readyz`、static K8s base/overlay、PDB、NetworkPolicy、ServiceMonitor、release remote_player 配置和静态 validator 已进入 R2。

注意：实现/静态测试完成不等于正式 Static Staging 已执行：

```text
READY_FOR_K8S_STAGING=false
Formal Static Staging=NOT_RUN
```

主要入口：

```text
docs/verification/r2-f1a-friendship-boundary-handoff-20260810.md
docs/verification/r2-f1b-b2-2a-canonical-friendship-handoff-20260810.md
docs/verification/r2-f1b-b2-2b-physical-friendship-retirement-handoff-20260810.md
docs/verification/r2-m1-a-actor-model-field-v2-handoff-20260810.md
docs/verification/r2-m1-b-actor-model-field-v2-runtime-handoff-20260810.md
docs/verification/r2-w1-worker-readiness-handoff-20260810.md
docs/verification/r2-q1-verification-handoff.md
```

## 4. CAP-REDIS-L1-R2 完成情况

### 提交链

```text
Harness implementation       = cdde3bf01dc1eba956f74c3357653888838f385d
Harness execution            = bcdea2d766bf381915a7a73ea8c16d601f726f51
Fault head                   = fd99df569e42232cce2eb63323c9d09a2cb41b8a
Business trace tool head     = bdd1d588820a6c23ec19400eb7d1a98a0786dc6b
Evidence candidate           = f526ae07f700b78de4c33f44207550263d8fe2ce
Review result                = 6369ba47e62a753baf98100426345abfa90c8e07
Final binding                = 1df5305add7df195349482546a26df93ebe80d28
Postmerge H5 binding         = c781775a804af0988380a9cbb0dbd758ff20f6cd
```

### Selected Train

```text
Run                     = capredis-l1-r2-20260810T114729Z-664196
Primitive Smoke         = 24/24 PASS
Warmup                   = 1/1 PASS
Formal                   = 5/5 PASS
Cells/round              = 192
Operations               = 691200/691200/0
Correctness violations   = 0
Required gaps            = 0
```

六轮均绑定 frozen Business/H5/Profile/Route/Lease/Machine identity，使用唯一 loopback 临时 Redis、4 CPU、4 GiB、no swap expansion、3 GiB noeviction、Slowlog threshold 0；uniform/hot × C1/C10/C100/C300 × 600 operations；fixture Due expected 等于 observed。

### R2 硬门

```text
legacy relationship reads  = 0
legacy relationship writes = 0
v1 fallback                = 0
remote_player RPC          = observed
Model V2 migration/sparse  = observed
CompleteSnapshotV2         = observed every round
Batch calls                = unique buckets
InvalidOrder               = 0
StepUnknown residual       = 0
Due residual               = 0
Namespace residual         = 0
```

### Fault Gates

```text
NOSCRIPT                   PASS
Connection Reconstruction PASS
Post-write Response Loss  TIMEOUT_UNKNOWN
Durable resolution        COMMITTED
Same-identity replay      ALREADY_COMMITTED
Exact teardown            PASS
FLUSHALL/SCRIPT FLUSH      not used
```

### 七个真实 H5 命令

正式独立 Review 使用 H5 `60707f...`、`remote_player`，执行：

```text
BUY / SELL / CLAIM_TASK / PLANT / HARVEST / WATER / THEFT = 7/7 PASS
```

每行都有真实 Redis transport、Actor Commit、等量 Receipt、唯一 Operation-bound Hash；Redis error/unanswered 为零，browser/protocol/authority/stack witness 全 PASS。

### 独立 Review

```text
REVIEWED_HEAD=f526ae07f700b78de4c33f44207550263d8fe2ce
REVIEW_RESULT_HEAD=6369ba47e62a753baf98100426345abfa90c8e07
P0/P1/P2=0/0/0
EXACT_HEAD_CLOSURE=PASS
CAP_REDIS_L1_R2=PASS
```

Reviewer 重跑 race count=20、全仓 test/vet/build、R2 live qualification、Fault、24 Primitive Smoke、7-command Trace、Binary/H5 reproducibility 和 Secret/raw-ID scan，全部 PASS。

### Final Evidence

```text
Evidence entries     = 49
Evidence Index SHA   = 5b45bf8d58efa77a0eb6b8ac971c5d7cd5e078545034b4713e56fa93c6913905
Summary SHA          = d5a3e5a9ba1607d6361ff7a8a4f7f683a4a6ef2b44565d7f534de24fd0c13052
Report SHA           = 95deb96bbfc96fb72214be4e04e828c52450bfd4c3696191844b91ab52645042
Review Verdict SHA   = 256e487a4b98df6c81989d9230417c524811a239cd3478b275fe149b40adde22
```

入口：

```text
docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/status.json
docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/final-handoff-20260810.md
docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/independent-review-result-20260810.md
docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/results/capredis-l1-r2-20260810T114729Z-664196/
```

## 5. Integration 与 H5 f22 合后资格

Backend 已 ff-only 合入并推送：

```text
5ce4371 → 1df5305 → c781775
qualified Integration       = c781775a804af0988380a9cbb0dbd758ff20f6cd
remote before handoff commit = c781775a804af0988380a9cbb0dbd758ff20f6cd
worktree at snapshot         = clean
```

承载本文件的提交是 documentation-only successor；不得用其新 Git SHA 冒充重新执行过的 Runtime/Trace 身份。

正式 CAP 测试的是 H5 `60707f...`；H5 Integration `f22dcb...` 新增四文件 Delta，因此合入后补跑：

```text
Backend cap-redis race count=20 PASS
Backend full test/vet/build       PASS
H5 Vitest                         215/215 PASS
H5 TypeScript typecheck           PASS
H5 web build                      PASS
Real-H5 remote_player Trace       7/7 PASS
Secret/raw-ID scan                PASS
Temporary residual               0
```

```text
Trace aggregate = aee5b0b3e8b0788e171d03958cd68d22d3ad8721bded91645d83f2d12ecf96b2
H5 aggregate    = 0fe0bb8f23304d9c54eb63771b04a3ee389bd6933d8ffb128e2b50efb5294bdb
```

Evidence：

```text
docs/verification/perf/cap-redis/CAP-REDIS-L1-R2/postmerge-h5-f22-qualification-20260810/
postmerge-verdict.json SHA256=da6356e85711b0f8ca8dcaf05bb5a735690ee121cc4be96edaefd4cfc584646f
SHA256SUMS SHA256=32b8d825e8886495480b764dd73b1753f88c6712fcab8a1fd4976fdc7970f643
```

结论：Backend/H5 combined postmerge qualification `PASS`。

## 6. Single-Pod L2 已完成内容

### L2 R1 Design

```text
Design Head      = a591ec6c3c76878d72e00fff3be7f9a6f379a4de
Review Result    = 25ba9675fbab1a1ea796e39a52fefcc16de351c3
Integration Head = 5ede235d85d59fbc66abb15ea920b70d493c213a
P0/P1/P2         = 0/0/0
```

已冻结 open-loop、planned/launched/scheduled-unsent/completed/rejected/timeout 分离、>50 ms unsent/no catch-up、Driver/Dependency/Target/Environment/Observability/Correctness 优先级、Staircase/Candidate/Formal×3、完整 Evidence identity 和治理边界。

### Generic Harness

首轮候选与 Review：

```text
Candidate=ba934b9d126180a3f8e8c94f5000886087a76b91
P0/P1/P2=0/7/1
Result=REQUIRED_FINDINGS
```

修复身份：

```text
Implementation=02ebb2e5f0b9f81a5ab0a2c0f4d0184bda105f07
Fix Head      =eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb
Binary SHA256 =8ab45d58184f442461ce5bd58b461dba155d7b5f1053b2307ebae075cd49d90b
```

七个 P1 和一个 P2 已实现修复：编译期 Binding trust root、Oracle 立即停止、Evidence write fail-closed、Driver 三类 slope/1.5× qualification、Formal 完整身份、Secret/raw-ID allowlist、Correctness non-negative latch/max、真实 Snapshot transient gap。

测试：

```text
go test -race ./cmd/cap-single-pod -count=20 PASS
Python harness tools                         3/3 PASS
go test ./...                                PASS
go vet ./...                                 PASS
go build ./cmd/...                           PASS
Train-A scope                                PASS
known_findings_remaining=0                   # Writer 侧
```

当前准确状态：

```text
ready_for_review_round_2=true
round_2_independent_review=NOT_RUN
ready_for_r2_binding=false
observability_qualification=NOT_RUN
load_generator_runtime_qualification=NOT_RUN
smoke/warmup/staircase/formal=NOT_RUN
```

用户允许产品合入和试玩不等待 Harness Round 2；这不等于容量治理门被删除。发布 L2 容量结论前仍应闭合该 Review。

## 7. 尚未完成 / 禁止声明

```text
Harness Round 2 formal signature = NOT_RUN
R2 Active Binding                = BLOCKED_NO_TRUSTED_SHA
Observability Qualification      = NOT_RUN
Load Generator Qualification     = NOT_RUN
CAP-SINGLE-POD-L2-R2             = NOT_RUN
CAP-MIXED-B1-R2                  = BLOCKED
Formal Static Staging            = NOT_RUN
3M/5M PCU model                  = NOT_READY
```

不得手工提升：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```

Local kind 仍为 `PROPOSAL_ONLY_NOT_AUTHORIZED`，手工试玩也不能替代 Formal Staging 或 Capacity Evidence。

## 8. 当前试玩环境

```text
URL                = http://127.0.0.1:5174
Status             = READY
Backend            = c781775a804af0988380a9cbb0dbd758ff20f6cd
H5                 = f22dcb447e154e424656b12409e041c32dfb4c5c
Friendship mode    = remote_player
Redis namespace    = manual-play20260810123534
Crop mature after = 10s
Supervisor PID     = 886226
```

Ready 文件：

```text
.agentctl-cp-route-state/manual-h5-f22-play20260810123534/ready.json
```

使用两个独立浏览器 Profile 和不同用户名，密码至少 8 位。该环境为新 namespace。停止时执行 `kill -TERM 886226`；不要单独杀 Vite/Backend 子进程。

## 9. 关键 worktree

```text
Backend Integration
  farm-game
  codex/integration-20260803
  qualified snapshot @ c781775a804af0988380a9cbb0dbd758ff20f6cd

CAP Redis R2 Writer
  farm-game-cap-redis-l1-r2-20260810
  codex/cap-redis-l1-r2-20260810 @ 1df5305add7df195349482546a26df93ebe80d28

CAP Redis R2 Review
  farm-game-cap-redis-l1-r2-review-20260810
  review/cap-redis-l1-r2-20260810 @ 6369ba47e62a753baf98100426345abfa90c8e07

Single-Pod Harness Writer
  farm-game-cap-single-pod-l2-r1-harness-20260810
  codex/cap-single-pod-l2-r1-harness-20260810 @ eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb

Harness Round-2 Review
  farm-game-cap-single-pod-l2-r1-harness-round2-review-20260810
  review/cap-single-pod-l2-harness-round2-20260810
  result=NOT_COMMITTED

H5 f22 verification
  classic-farm-h5-cap-postmerge-f22-20260810
  verify/cap-postmerge-f22-20260810 @ f22dcb447e154e424656b12409e041c32dfb4c5c
```

## 10. 推荐后续顺序

1. 对 `eabb83c...` 完成 Harness Round 2，目标 `P0/P1/P2=0/0/0` 和 `READY_FOR_R2_BINDING=true`。
2. Controller 生成精确 R2 Binding，并把受信 Binding SHA 加入 Harness 编译期 trust root；不得由外部 Run Plan 自激活。
3. 在正式 Static Staging 先跑 Observability Qualification，再跑 Load Generator Runtime Qualification，候选 headroom 必须真实达到至少 1.5×。
4. 运行 Single-Pod Smoke、Warmup、Staircase、Candidate、Formal×3；任何 correctness/observability 违规立即停止新到达。
5. L2 PASS 后运行 `CAP-MIXED-B1-R2`。
6. 只有精确 R2 L1/L2/Mixed 数据全部闭合后，才进入 3M/5M PCU 模型。

## 11. 不变原则

- 禁止使用“最新 Integration”、移动 Tag、脏 worktree 替代冻结身份；
- planned/launched/scheduled-unsent/completed/rejected/timeout 分离；
- >50 ms arrival 为 unsent，禁止 catch-up burst；
- Driver 先满只能是 `INCONCLUSIVE_LOAD_GENERATOR`；
- Oracle incomplete、未知/负 correctness、Evidence 写失败均 fail closed；
- Redis 资格环境唯一、loopback、临时、精确销毁；禁止 `FLUSHALL`/`SCRIPT FLUSH`；
- Evidence 禁止 Secret、raw Redis URL 和 raw Operation/Session/Receipt/Workflow/Work ID；
- Product identity、Harness identity、Binding、Profile、Config、Route、Lease、Redis、Image、Adapter、Qualification 必须完整一致；
- Integration/push/tag 由 Controller 管理；容量结论不能由 Writer 自报。
