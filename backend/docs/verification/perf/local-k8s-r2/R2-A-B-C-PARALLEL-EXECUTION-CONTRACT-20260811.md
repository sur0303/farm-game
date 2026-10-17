# R2 A/B/C 并行实施合同 — 2026-08-11

状态：`APPROVED_FOR_PARALLEL_IMPLEMENTATION_FAIL_CLOSED`

北极星目标：

```text
关闭 Static Init + Product Route Liveness
→ 获得第一个合法、长期可活的本地产品部署
→ 再进入 Functional、Driver Qualification 和 L2
```

本文档只冻结三条并行实施列车的边界、共享合同、合并门和验收项，不授予公司 Redis、L2、Mixed、Fault 或 Soak 权限。

## 1. 冻结基线

```text
Integration Branch=codex/integration-20260803
Baseline Head=9c423f4
Remote Head=9c423f4
Worktree=clean
Full go test ./...=PASS
Full go vet ./...=PASS
Local tool tests=PASS
Company Redis contacted=false
```

基线由三个提交组成：

```text
585c651 perf: close local R2 capacity plan findings
cb89c27 feat: bootstrap fail-closed local K8s R2 lab
9c423f4 feat: add fail-closed R2 capacity driver contract
```

## 2. 现有 CP-ROUTE-CORE 能力结论

现有代码已经具备以下核心语义，Agent A 不得重写第二套：

```text
internal/actorrouting/authority/RouteAssignmentManager
internal/actorrouting/DynamicAssignmentProvider
internal/actorrouting/DynamicAssignmentPublisher
internal/actorrouting/SnapshotApplier
internal/actorrouting/StaticResolver
internal/authoritycomposition/NewMembershipRuntime
internal/authoritycomposition/PerShardLeaseScheduler
```

已存在的正确能力：

- `RouteAssignmentManager` 是 Claim/Transfer/Unassign 的唯一 Generation Coordinator；
- Authority Commit 在 Route Snapshot Publication 之前；
- Unknown、Rejected、过期或不完整 Authority Result 不发布；
- `RouteAssignmentManager.Renew` 只允许当前 Holder 精确代次续租，并能发布 Same-generation Fresh Lease Snapshot；
- Dynamic Provider 只传播完整 Snapshot；
- Revision Gap、Watch Close、Slow Subscriber 通过 Full Reload 恢复；
- Stale Snapshot 不覆盖新 Snapshot；
- Same-revision Different-payload、Generation Rollback、Boot Rollback、Lease Deadline Rollback Fail Closed；
- `SnapshotApplier` 对 Resolver 做原子 Whole-table Replace；
- Holder 本地 Per-shard Scheduler、Guard 和 Membership 已存在。

尚未完成的生产能力：

```text
Distributed manager-owned full-snapshot publisher/watch transport
Durable TransitionRecoveryStore/TombstoneStore adapter
Authoritative Redis shard reader adapter
Holder Renew → RouteAssignmentManager 的受信生产接线
Gate/Worker/全部 Consumer 的 Dynamic Provider 接线
Consumer Freshness/Readiness 门
```

因此 Agent A 的任务是生产接线和缺口闭合，不是重新发明 Route Model、Resolver 或 Lease State Machine。

### 2.1 已存在但尚未进入当前 Business Integration 的 RT2 候选

仓库中存在一条完整的历史 RT2 Dynamic Route 列车，最新检查点：

```text
checkpoint/route-dynamic-wave3-rt2y-20260810
Head=3c8e144bf733047635b9f26a9acad8377e21e133
Parent family=RT2A/RT2B/RT2C/RT2D/RT2I/RT2M/RT2W/RT2X/RT2Y
```

该候选包含约 199 个文件、67k 行实现，主要包括：

```text
api/proto/routecontrol/**
api/proto/holdercontrol/**
cmd/route-controller/**
cmd/routectl/**
internal/actorrouting/remote/**
internal/routecontrol/**
internal/routeplan/**
internal/holdercontrol/**
internal/storage/redis/routecontrol/**
internal/storage/redis/routeplan/**
生产 Composition 与 Deployment 示例
```

它已经实现了 Durable Snapshot/Transition Store、Sealed mTLS Transport、Dynamic Consumer、Holder Control、Route Controller、Boot Rollover 和 Plan TTL 修订，并通过了其原分支上的大量 Unit/Race/Full Test。

但是：

- 它与当前 Business/Verification Integration 在共同祖先后发生大规模双向分叉；
- 当前 `20bcd03` 不是该 Checkpoint 的后代；
- 旧分支上的公司 Redis Probe Evidence 不得继承到本地 Profile B；
- 最终 Handoff 明确保留 `RT2Q Final multi-Pod runtime evidence pending`；
- 最新 RT2Y 修订没有独立 Post-fix `NO FINDINGS` Review；
- 不能把旧分支 Head 或 Evidence 冒充当前 Business Head 的资格。

因此 Agent A 的第一项工作不是从零实现，而是：

```text
审计 3c8e144 对当前 20bcd03 的可组合性
→ 按功能序列移植/合并已 Review 的 RT2 代码
→ 解决当前 Business 线冲突
→ 在当前 Head 上重跑全部语义、Race 和 Runtime Gate
```

禁止直接 Whole-branch Merge 后跳过冲突语义 Review，也禁止因代码已存在就继承旧 Runtime Evidence。

## 3. 唯一 Projection Source 决策

冻结决策：

```text
RouteAssignmentManager-owned DynamicAssignmentPublisher
= Fresh RouteSnapshot 的唯一发布能力
```

禁止把以下对象变成第二个发布源：

- Gate；
- Worker；
- Player/Farm Holder 本地 Scheduler；
- Static Init Renderer；
- 任意 Consumer；
- 任意直接 Redis Poller 未经 Manager reconcile 后自行组装的 Snapshot。

Holder Renewal 必须最终经过 Manager 的 `Renew` 语义，或进入一个与其完全等价且由 Manager 独占发布能力验证的受信 Adapter。不得让 Holder 自报 Deadline 直接进入 Snapshot。

Static Init 负责产生初始完整 Authority Generation 和 Initial Snapshot；后续 Fresh Lease Projection 归 Agent A 的 Manager-owned Publisher 所有。

## 4. A/B 共享 Route 合同

### 4.1 不可变布局

同一 Profile 内固定：

```text
realm=classic-farm-local-l2-r2
hash_version=2
hash_seed=202608030001
player.shard_count=1024
farm.shard_count=1024
authority_layout_version=v1
workflow_layout_version=v1
workflow_hash_version=1
workflow_bucket_count=256
```

改变 Realm、Hash Version、Hash Seed 或 Shard Count 属于新布局/迁移，不能作为普通 Snapshot Update。

### 4.2 Holder Identity

```text
holder_id      = <statefulset-pod-name>
holder_boot_id = <pod-name>-<profile-id>-g<generation>-<controller-nonce>
```

真实运行必须使用新生成的至少 96-bit DNS-safe Nonce。Git 中的固定 Render-only Nonce 永不得激活。

### 4.3 Initial Placeholder

Agent B 若复用 `RouteAssignmentManager` Bootstrap，初始完整表必须使用现有 Manager 可证明的 Placeholder 语义：

```text
state=ASSIGNING
shard_revision=1
expected_writer_epoch=1
expected_lease_epoch=1
GlobalRevision>0
```

Placeholder 只存在于进程内初始化，不得作为 Activatable Final Snapshot 发布到部署输入。

### 4.4 Final Active Route

全部 2048 个 Shard 必须来自 Authority Result：

```text
state=ACTIVE
assignment_revision>0
writer_epoch>0
lease_epoch>0
lease_until_ms>now
holder_id/boot_id exact
last_transition_id exact
```

Final Snapshot 只能在 2048/2048 完整时原子写出。

### 4.5 Revision

- Global Revision 严格单调；
- 普通更新只能是 `current+1`；
- Gap 必须 Full Reload；
- Same Revision 只接受 Deep-equal Replay；
- Stale Callback 只增加 Stale Counter，不得 Replace；
- Same-generation Lease Deadline 只允许不下降；
- Boot Rollover 必须完整推进 Assignment/Writer/Lease Generation。

## 5. Agent A — RT2-FRESH-LEASE-ROUTE-PROJECTION

### 5.1 所有权

主要允许范围：

```text
internal/actorrouting/**
internal/actorrouting/authority/**
internal/authoritycomposition/**
internal/gatewayapp/**（仅 Route Provider/Readiness 接线）
cmd/farm-gate/**（仅 Composition）
cmd/farm-worker/**（仅 Composition）
cmd/player-svr/**（仅 Composition）
cmd/farm-svr/**（仅 Composition）
必要的新内部 Route control-plane transport package/proto
Agent A 独立测试和 Handoff
```

禁止范围：

```text
cmd/cap-static-route-init/**
internal/testkit/capdriver/**
新增隐藏 Performance RPC
业务 Actor/Workflow 语义改动
公司 Redis配置或探针
```

### 5.2 必须交付

1. 审计并优先复用历史 RT2 候选中的 Durable Store、Transport、Dynamic Consumer 和 Route Controller，禁止无理由重写第二套；
2. Manager-owned 分布式 Whole-snapshot Get/Watch；
3. 受信 Holder Renew 接入 Manager；
4. Durable Transition Recovery/Tombstone Adapter，或明确阻止 Restart-safe Claim 的 Fail-closed 状态；
5. Authoritative Current-shard Reader；
6. Gate/Worker/Player/Farm Consumer Dynamic Provider 接线；
7. Gap Full Reload 和 Reconnect；
8. Route Unknown/Stale 时 Readiness=False；
9. Snapshot Revision/Age/Freshness Metrics；
10. Boot Rollover；
11. `>=3×TTL` 证明工具和 Receipt；
12. 在当前 Business Head 上重跑 RT2Q Final 所需 Multi-Pod Runtime Gate。

### 5.3 必须测试

```text
初始 Route 可用
Holder 持续 Renew
Gate 观察到 Fresh Lease
Worker 观察到 Fresh Lease
Player/Farm Cross-holder Resolver 观察到 Fresh Lease
持续 >=3×TTL
Revision Gap → Full Reload
Generation Rollback → Reject
Boot Rollover → Old Boot Fenced
旧 Snapshot 迟到 → Ignore
Same Revision Different Payload → Reject
Consumer Disconnect/Reconnect → Full Reload
Slow Consumer → Close + Reload
Unknown Commit → No Publish
Route Source Unknown/Stale → Readiness False
取消发生在 Replace 前 → 不安装
```

### 5.4 P0 停止条件

任一出现即停止 A 的晋级：

- 存在两个 Snapshot Publisher；
- Holder 自报值未经 Authority 校验直接发布；
- Watch Gap 使用局部 Patch 猜测；
- Stale Callback 覆盖新表；
- Readiness 与 Freshness 脱钩；
- 使用远期 Lease；
- 通过定期重启掩盖 Deadline；
- Durable Recovery 不存在却声称 Restart-safe。

## 6. Agent B — CAP-STATIC-ROUTE-INIT-1024-R2

### 6.1 所有权

```text
cmd/cap-static-route-init/**
internal/testkit/capstaticroute/**
必要的只读 Redis Authority Reader Adapter
Static Init Schema/Receipt Validator
本地 Redis Integration Tests
deploy/k8s/static-staging/overlays/local-capacity-r2 的 Activated Runtime Materializer
Agent B 独立测试和 Handoff
```

禁止范围：

```text
Gate/Worker Dynamic Route消费语义
Route Lease Watch Transport
Driver/Observability
公司 Redis配置或网络访问
```

### 6.2 必须交付

1. 默认 Dry-run，显式 `--apply-local-profile-b` 才能访问本地 Redis；
2. 默认拒绝非 Loopback/非明确 Local Profile B Binding；
3. 生成 Run ID、Generation、Controller Nonce；
4. 构造 1024/1024 Placement；
5. 通过现有 Authority Store/Manager 语义初始化 2048 Shard；
6. 每条结果校验 Domain/Shard/Layout/Hash/Holder/Boot/Generation/Deadline/Transition ID；
7. 支持相同 Transition ID 幂等重跑；
8. Partial/Unknown 时不写 Final Snapshot；
9. 原子写 Final RouteSnapshot、Receipt、Exact Ledger；
10. Receipt 不包含 Secret、Kubeconfig、Raw Credential 或公司 Redis Endpoint。

### 6.3 Transition ID

必须由以下稳定输入确定：

```text
run_id
profile_id
generation
domain
virtual_shard
holder_id
holder_boot_id
```

同一输入生成同一 ID；任一 Identity Drift 产生不同 Fingerprint，并被已有 Authority Record 拒绝或进入明确的新运行代次。

### 6.4 必须测试

```text
4/4 Integration
1024/1024 Completeness
2048 Authority Records Exact
Same Transition ID Idempotent Rerun
Partial Failure → No Final Publish
Timeout Unknown → Fail Closed
Holder Mismatch
Boot Mismatch
Layout/Hash Mismatch
Expired Initial Lease
Initial Claim Duration Budget
Namespace普通前缀保留
同 Bucket同 Slot
不同 Bucket分散
全部 Key 属于本 Run
默认 Company Redis零访问
```

### 6.5 P0 停止条件

- 任何 Shard 未校验却生成 Activatable Snapshot；
- 从本地计数猜测 Epoch；
- 使用固定未来 Deadline；
- Timeout Unknown 后继续；
- 通过 FLUSH 清理；
- 使用全局共同 Hash Tag；
- Receipt 含 Secret；
- 默认参数可连接任意公司/远程 Redis。

## 7. Agent C — CAP-L2-R2-LEGAL-DRIVERS-OBSERVABILITY

### 7.1 并行分段

```text
C1：代码、Schema、单元测试、离线 Adapter
    与 A/B 完全并行

C2：真实产品集成、Driver Qualification
    等待 A/B Integration PASS 和产品部署
```

### 7.2 所有权

```text
internal/testkit/capdriver/**
cmd/cap-driver/**
从 cmd/ws-stress 提取的可复用公开协议 Client
deploy/load/**
Observability manifests/config
Collector/Oracle/Evidence Writer
Agent C 独立测试和 Handoff
```

禁止范围：

```text
internal/actorrouting/**
Authority/Lease语义
隐藏产品接口
Direct Actor/gRPC性能入口
Raw Redis业务 Fixture
公司 Redis访问
```

### 7.3 合法入口

```text
Gate   = HTTP Login → HTTPS/WSS → ClientHello
Player = 仍从 Public Gate/Session 进入 Player-owned Command
Farm   = JoinFarm → SubscriptionReady → 正式 Farm Command
Worker = 完整 WorkRecord + Source Order/Fact + DueIndex + Lease + Invocation
```

Worker 必须走 LoadAndClaim/Recovery Ingress，不能只向 Redis 塞一条残缺 Due。

### 7.4 必须交付

- Open-loop Monotonic Scheduler；
- Planned/Launched/Unsent/Completed/Rejected/Timeout 分离；
- No Catch-up Burst；
- Binary/Config SHA；
- Driver CPU/Memory/Queue Metrics；
- Prometheus；
- Collector；
- Oracle；
- Evidence Writer；
- Correctness Stop；
- Driver Qualification Receipt。

### 7.5 Qualification 门

```text
successful samples >=60
headroom >=1.5×
correctness unknown=0
correctness nonzero=0
Driver先满=INCONCLUSIVE_LOAD_GENERATOR
Evidence I/O完整
```

## 8. 文件冲突规则

- A/B/C 不共同编辑一个中央 Status 文件；各自生成独立 Handoff；
- Integration Owner 在合并后统一更新 Current State；
- A 不修改 B 的 CLI/Receipt；
- B 不修改 A 的 Provider/Consumer；
- C 不修改 A/B 产品语义；
- Shared Schema 变更必须先回到 Integration Contract 提交，三条线再 Rebase；
- Proto 变更只归 A，且必须生成代码无漂移；
- K8s Activated Render 归 B，Observability Overlay 归 C，Integration Overlay 由主 Agent合成。

## 9. 并行与合并顺序

开发并行：

```text
A ───────────────┐
B ───────────────┼─ Integration
C1 ──────────────┘
```

晋级顺序固定：

```text
1. B Static Init 2048/2048 PASS
2. A Route Liveness >=3×TTL PASS
3. A/B 分别独立 Review
4. Integration 合并 B
5. Integration 合并 A
6. A+B 联合 2048 + 3×TTL 测试
7. 合并 C1
8. 生成 Local Provisional Binding
9. 生成真正可激活 Runtime Render
10. 处理 CPU Admission
11. 先部署 Player/Farm
12. Holder Ready并完成本地 Lease准入
13. 再部署 Gate/Worker/H5
14. 执行 DEV_FUNCTIONAL_NOT_QUALIFICATION
15. Functional全门通过
16. 部署/校验 Prometheus、Collector、Oracle
17. 执行 C2 四 Driver Qualification
18. 才申请 L2 Staircase
```

A 可以先于 B 完成，C1 也可以先合成候选，但任何产品资格晋级必须同时满足 A+B。

## 10. 最小部署阶段

### Stage D0 — Offline

```text
Static Init Dry-run
Route/Driver Unit Test
无产品 Pod
```

### Stage D1 — 短时诊断

A 未完成但 B 已完成时，只允许在严格 Deadline Budget 内短时启动，名称必须是：

```text
DEV_FUNCTIONAL_NOT_QUALIFICATION
```

不得跨过旧 Snapshot Lease Deadline，不得生成 CP PASS。

### Stage D2 — 长期本地 Functional

必须同时满足：

```text
B 2048/2048 complete
A Fresh Projection complete
>=3×TTL PASS
Route Unknown时Readiness False
```

之后才允许生成 Local Provisional Binding 和长期可活部署。

## 11. CPU Admission 独立门

当前 Local Redis Container 位于 `4-5`，占用 Target Group。进入容量 Profile 前必须：

```text
Target CPU Anchor 先占 4-7
→ Redis/Dependencies 落到 16+
→ 删除 Anchor
→ 启动真实 Target并获得 4-7
```

Functional Profile 与 L2 Profile 的 CPU Receipt 分离。当前主机 `4×4C=NOT_RUN_INSUFFICIENT_HOST`。

## 12. Cursor 使用规则

唯一允许的 Cursor 模型：

```text
cursor-grok-4.5-high-fast
```

只允许主 Agent已经完全定义合同后的机械小任务，例如：

- Table-driven Test Case；
- JSON Schema/Validator Boilerplate；
- 重复 Manifest/Patch；
- 文档格式和静态检查。

禁止 Cursor 决策或实现：

```text
Route Liveness架构
Projection Source所有权
Lease/Revision/Generation语义
Unknown Commit处理
Static Init原子性
Worker Fixture语义
合法产品入口
资格判定
公司 Redis操作
```

Cursor 返回必须由主 Agent逐文件 Review 并重新运行测试。

## 13. 持续治理状态

直到全部晋级门关闭：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
company_redis_contacted=false
ACTIVE_BINDING_AUTHORIZED=false
L2_STAIRCASE_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
MIXED_AUTHORIZED=false
SOAK_AUTHORIZED=false
```
