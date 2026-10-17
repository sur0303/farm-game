# WF1 · Workflow/Order/Dispatcher/Worker 运行时收敛 Handoff

- task_id: `WF1-WORK-RUNTIME-CONVERGENCE`
- backend base tag: `cp2-round2-20260804`
- backend base commit: `2d894be630e5cc7faab53060bf342e9f1be9ed37`
- branch: `codex/wf1-runtime-convergence-20260804`
- worktree: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-wf1-runtime-convergence`
- integration owner: I0（本任务不合并 integration）
- 状态： `COMPLETED`（通用框架收口；不宣称 Plant/Harvest/Care/Theft 完成）
- final commit: 本分支推送时 HEAD（提交列表末位）

提交列表（base `2d894be` 之后）：

```text
c52909d docs(wf1): audit matrix for existing internal/workflow before convergence
086e789 feat(wf1): generic work engine, bounded dispatcher and single recovery mainline
681f576 feat(wf1): Redis WorkRepository with trusted-time lease Lua + miniredis proof
257489b docs(wf1): adaptation matrix, crash-point map, dispatcher bounds, ICRs
<HEAD>  docs(wf1): final commit list and verification record
```

验证记录（全部真实 Exit 0）：

```text
go test -race -count=1 -timeout=240s ./internal/workflow/... ./internal/storage/authoritystore/... ./internal/storage/redis/authority/...
go test -count=1 -timeout=300s ./...
go vet ./...
go build -buildvcs=false ./...
git diff --check cp2-round2-20260804..HEAD
git status --porcelain   # clean
```

## 1. 语义输入（只读）

- docs/design/27-access-routing-actor-workflow-freeze.md
- docs/design/29-internal-rpc-trusted-context.md
- docs/design/30-actor-authority-store-contract.md
- docs/design/31-actor-authority-redis-physical-contract.md
- docs/design/32-core-business-scenario-contract.md（只读四类场景接缝，不修改业务规则）
- docs/design/72-agent-work-package-playbook-draft.md 第 11 节
- docs/design/73-linux-multi-agent-dispatch-runbook.md 第 9.2 节
- docs/verification/implementation-lock-board.md

## 2. 现有实现审计矩阵（施工前审计，先于代码变更）

审计对象： `internal/workflow/`（`store.go`、`runner.go`、`redis.go`、`reward.go`、`routed_farm.go`、`identity.go`）。

### 2.1 能力矩阵

| 合同能力（27/29/30/31） | 现有实现 | 判定 |
|---|---|---|
| CreateIfAbsent（稳定 workflow_id + 同 intent 复用 + Hash 冲突拒绝） | `Store.CreateOrLoad`（Memory/Redis 双实现），`StableID → workidentity.WorkflowID` | **已有但有冲突**：`workidentity.WorkflowID` 使用 `farm-workflow-id-v1` namespace，输入只有 `subject/command/operation`，缺 `root_authority_kind + canonical_root_authority_id`；合同冻结为 `farm-workflow-id-v2`（27 §1.2 / 31 §8.1A）。`workidentity` 不在 WF1 写集 → ICR-1 |
| 状态版本/CAS（work_version 递增、旧版本拒绝） | `WorkflowRecord.WorkflowRevision` + `UpdateOptions.ExpectedRevision`，`ErrRevisionConflict` | 已有（legacy 族）。通用框架以 `WorkVersion` 对齐 |
| Work Lease（owner/epoch/expire、失租自 Fence） | `LeaseOwner/LeaseEpoch/LeaseUntilMs` + `ClaimDue` + `validateLease` | **已有但语义偏差**：租约到期时间由调用方本机时钟传入（`now.Add(lease)`），不是 Repository 可信时间 + `accepted_ttl_ms`（30-14/29 §7.3.1）；无 owner_token、无 Renew。通用框架按可信时间实现，legacy 族迁移列为残余风险 |
| Participant Receipt（同 invocation + 同 hash 返回原结果） | 无通用模型。`RewardPayloadHash` 只对 reward 请求做 hash 绑定；Participant 重试防重依赖 `InspectFarmOperation` 业务探测 | **缺失（通用）**：无 step/invocation 级 Receipt 模型、无稳定 step payload hash、无 `participant_invocation_id` 派生（`workidentity.ParticipantInvocationID` 存在但 legacy Runner 未使用） |
| Step/Invocation Hash | `RewardPayloadHash`（reward 请求级）；无 CanonicalStepPayloadHash | **缺失（通用）**：通用框架补齐确定性 invocation + canonical step hash + 依赖 result hash 链 |
| Order（来源同槽 Order） | 无。legacy 只有独立 WorkflowRecord | **缺失**：R1 已在 `internal/storage/redis/authority` 交付来源 Order 的 CommitWithOrders + WorkLease 原语（Participant 侧，归 AR1/PI1/FI1 消费）；WF1 交付通用 Order 运行时模型与恢复框架 |
| DueIndex（可恢复发现） | legacy RedisStore `{workflow}:due` ZSET + `ClaimDue`/`PendingDue` | 已有（legacy 族，单 bucket）。通用框架提供同语义 DueIndex + WorkRefV1 编码成员 |
| Unknown Outcome（Participant TIMEOUT_UNKNOWN 解析） | 无。Runner 把任何错误统一退避重试 | **缺失（通用）**：通用框架实现 `STEP_UNKNOWN` 不越过、同 invocation 重放命中 Receipt 解析 |
| 终态与 SUPERSEDED | `COMPLETED/COMPENSATED` 两种终态 | **部分**：无 SUPERSEDED；通用框架补齐 COMPLETED/COMPENSATED/REJECTED/SUPERSEDED/FAILED(dead-letter-like) |
| DirectDispatcher（有界队列快路径） | 无 | **缺失** → 本轮新建 |
| farm-worker 恢复主链 | 无独立组件；`Runner.Process` 只能由外部驱动 | **缺失** → 本轮新建 RecoveryLoop，并把 legacy Runner 适配进同一主链 |

### 2.2 冲突与偏差清单

1. `farm-workflow-id-v1` namespace 与合同 `farm-workflow-id-v2` 不一致（缺 Root 字段）→ ICR-1。
2. legacy lease 使用调用方时钟，违反“Repository 可信时间 + accepted_ttl_ms”（30-14）→ 通用实现纠正；legacy 族保留现状并登记风险。
3. legacy `WorkflowKind_CARE` 把 Care 建模为独立 Workflow；合同 27 §7.8.2/30-08A 冻结 Care 为“来源事实 + CareRewardOrder + 同槽 DueIndex”，不是独立 Workflow → ICR-2（迁移归 SC1/FI1，WF1 不动业务语义）。
4. legacy Runner 直接调用 `logicrpcpb` 服务接口，与 29 §7.6 ParticipantRequest/CoordinatorWorkContext/InternalCallerContext 外壳不一致 → ICR-3（生产 Participant Adapter 归 AR1/PI1/FI1）。
5. `UNLOCK_PLOT` Workflow 在现有代码中完全不存在（proto kind 仅 PLANT/HARVEST/THEFT/CARE）→ 适配矩阵登记，归 SC1。

### 2.3 收敛决策

- 不新建 `workflow2` 包、不建第二套 worker 主链：通用框架与 legacy 族同处 `internal/workflow`，共用同一 `WorkRepository` 端口抽象；`RecoveryLoop` 通过 `LegacyRepository`/`LegacyHandler` 适配器同时驱动 legacy WorkflowRecord 与通用 WorkRecord。
- 通用框架只消费 cp2-round2-20260804 已冻结的 Port/Fake（`workidentity`、`authoritystore` 类型、`actorrouting`），不假设 AR1 未合入的新类型。
- Participant 生产 Adapter、composition root、cmd 装配留给 AR1/I0；本轮不宣称 Plant/Harvest/Care/Theft 完成。

## 3. 实际写集 / 禁止写集确认

实际写集（`git diff --name-only cp2-round2-20260804..HEAD`）：

```text
internal/workflow/backoff.go            # 新增：指数退避 + 抖动（配置候选）
internal/workflow/workrecord.go         # 新增：通用 WorkRecord/StepRecord/终态/SUPERSEDED/codec
internal/workflow/workstore.go          # 新增：WorkRepository 端口 + 内存实现（可信时钟租约/CAS/DueIndex）
internal/workflow/rediswork.go          # 新增：Redis WorkRepository（自有 {work-v1} Key/Lua 族，Redis TIME）
internal/workflow/engine.go             # 新增：通用步骤引擎（invocation/step hash/Receipt/Unknown/漂移 fail-closed）
internal/workflow/dispatcher.go         # 新增：DirectDispatcher（有界 item/byte、deadline、背压）
internal/workflow/recoverer.go          # 新增：RecoveryLoop + LeaseController + EngineHandler
internal/workflow/legacy_adapter.go     # 新增：legacy Store/Runner → WorkRepository/WorkHandler 适配
internal/workflow/{workstore,engine_crash,dispatcher,recoverer,rediswork,testworkflow}_test.go
docs/verification/wf1-runtime-convergence-handoff.md
```

禁止写集确认（逐项核验，均未触碰）：

- `internal/logic/actor/**`、`internal/player/actor/**`、`internal/farm/actor/**`：未触碰。
- `internal/player/ingress/**`、`internal/farm/ingress/**`：未触碰。
- `api/proto/**`、`api/gen/**`、CommandCatalog：未触碰（未新增任何 Proto 类型；通用模型为 Go struct + JSON 存储编码）。
- `internal/storage/redis/authority/scripts.go`、公共 KeyBuilder/Lua 语义：未触碰；WF1 的 Lua 全部位于 `internal/workflow/rediswork.go` 自有 `{work-v1}` Key 族。
- `cmd/**`（含 `cmd/farm-worker`）、H5、合同正文：未触碰。
- Plant/Harvest/Care/Theft 业务数值与权威裁决：未触碰；仅只读审计并输出第 10 节适配矩阵。
- 未创建 `workflow2` 包或第二套 worker 主链；未新增 `internal/worker/**`（通用运行时全部落在既有 workflow 子包，符合“优先放在现有 workflow 子包”）。

## 4. 崩溃点 → 测试映射

| 崩溃点（任务书第 6 条） | 测试 |
|---|---|
| 创建前崩溃（无任何记录）→ 重新创建只产生一条记录 | `TestCreateIfAbsentConvergesAcrossCreateCrash`、`TestMemoryWorkRepositoryCreateIfAbsentIsAtomic`、`TestRedisWorkRepositoryCreateAdvanceAndConflict` |
| 创建后、派发前崩溃 → 从 DueIndex 恢复 | `TestCrashAfterCreateBeforeAnyDispatch` |
| Order 已提交但未派发（队列满/进程退出） | `TestOrderCommittedButDispatchDropped`、`TestDispatcherEnqueueBoundsItemsAndBytes` |
| 调用前失败（可证明未生效）→ 恢复后正常首次执行 | `TestDispatcherFastPathCompletesAndBacksOff`、`TestDefinitionDriftFailsClosed`（freeze 后丢调用） |
| 调用后响应丢失（已生效）→ 同 invocation 命中 Receipt，不重放效果 | `TestApplyThenResponseLostResolvesViaReceipt`、`TestRedisRecoveryMainline` |
| Participant TIMEOUT_UNKNOWN → 不越过该步骤，解析后继续 | `TestParticipantUnknownBlocksAdvanceUntilResolved` |
| Lease 失效/换代 → 旧 worker 被 Fence，新 worker 一次性完成 | `TestLeaseInvalidationHandsOverWithoutDoubleApply`、`TestMemoryWorkRepositoryClaimLeaseFenceAndReclaim`、`TestRedisWorkRepositoryClaimFenceRenewAndReclaim` |
| 重复恢复（多 worker/多轮） | `TestDuplicateRecoveryDoesNotReExecute`、`TestMemoryWorkRepositoryConcurrentClaimSingleWinner` |
| Ack（终态提交）前崩溃 → 全部步骤 Receipt 恢复，效果仅一次 | `TestCrashBeforeFinalAckCompletesFromReceipts` |
| Ack 后崩溃 → 终态不再派发、不再恢复 | `TestCrashAfterAckNeverRedispatches` |
| 同 invocation 重放命中 Receipt；同 invocation 不同 hash 拒绝 | `TestSameInvocationReplayHitsReceipt` |
| SUPERSEDED 终态不派发、不恢复、幂等 | `TestSupersedeSkipsDispatchAndRecovery` |
| 超 Deadline → dead-letter-like `FAILED`，不再重试 | `TestDeadlineExceededFailsClosed` |
| 业务确定拒绝 → 补偿分支（RELEASE→COMPENSATED） | `TestRejectedCommitRunsCompensationBranch`、`TestRejectedReserveTerminatesImmediately` |
| 定义漂移（冻结 payload 与定义不一致）→ fail closed | `TestDefinitionDriftFailsClosed` |
| 单一 worker 主链驱动 legacy WorkflowRecord | `TestRecoveryLoopDrivesLegacyRunner`、`TestLegacyRepositoryRoundTripsGenericView` |
| Redis（miniredis）全链路：创建→丢响应→崩溃→DueIndex 恢复→恰好一次 | `TestRedisRecoveryMainline` |

## 5. DirectDispatcher 队列/背压配置边界

`DispatcherConfig`（全部 PHYSICAL_PENDING 配置候选，必须来自版本化配置并暴露指标）：

| 参数 | 语义 | 代码默认（候选，非平台值） | 平台值 |
|---|---|---|---|
| `MaxItems` | 内存队列条目硬上限，满 → `ErrDispatcherFullItems` | 1024 | PENDING |
| `MaxBytes` | 内存队列字节硬上限，满 → `ErrDispatcherFullBytes` | 4 MiB | PENDING |
| `ItemDeadline` | 单条派发（含全部 Participant Step）的 ctx deadline | 5s | PENDING |
| `Workers` | 派发 goroutine 数 | 4 | PENDING |
| `Backoff.Base/Max/JitterPPM` | 重试退避底/顶/抖动（ppm） | 250ms / 30s / 0 | PENDING |
| `RecoveryConfig.LeaseTTL/SafetyMargin/ClaimLimit` | Work Lease 请求 TTL/安全边距/单次领取上限 | 10s / TTL/5 / 32 | PENDING |

背压语义：队列满只返回错误，**不丢工作**——来源提交已把记录与 DueIndex 落权威存储，`farm-worker` 模型到期接管；调用方不得把队列当权威。Dispatcher 不持有跨进程 Work Lease，推进以 `work_version` CAS + Participant Receipt 防重。Dispatcher/RecoveryLoop 均在 Actor Turn 之外运行，Actor Turn 不等待远程 RPC。

指标接缝（已提供内存计数，供 I0 接入注册表）：`DispatcherStats{Depth,Bytes,Enqueued,Dropped,Dispatched,Completed,Reschedule,Failed}`、`RecoveryStats{Claimed,Resumed,Completed,Retried,LeaseLost,Skipped,Failed}`、`WorkRepository.PendingDue`（due lag 观测）。

## 6. live Redis 执行情况

未执行（`FARM_REDIS_URL` 未设置；`TestLiveRedisWorkflowStore` 按既有门禁跳过）。所有 Redis 断言基于 miniredis 进程内实例（Lua/Hash/ZSET/TIME 真实执行）；**不宣称 REAL_REDIS_PASS**。

## 7. ICR 登记

### ICR-1 · workflow_id 代次（v1→v2）

- 现状： `internal/workidentity/identity.go` 的 `WorkflowID` 使用 `farm-workflow-id-v1`，输入缺 `root_authority_kind/canonical_root_authority_id`；合同 27 §1.2 与 31 §8.1A 冻结 `farm-workflow-id-v2` 五字段输入。
- 影响： 滚动升级前必须切换 namespace 并迁移在途 Work；legacy 与通用族共用同一派生函数，需一次性切换。
- 建议归属： SCB/P1B 或 I0（`internal/workidentity` 不在 WF1 写集）。

### ICR-2 · CARE 的恢复锚点形态

- 现状： legacy `WorkflowKind_CARE` 独立 Workflow；合同 27 §7.8.2 / 30-08A 冻结为来源同槽 `CareRewardOrder`（`FARM_ACTOR:farm_id`）。
- 建议归属： SC1 + FI1（Farm 来源事实提交接缝稳定后迁移）；WF1 仅提供通用 Order 运行时，不改业务语义。

### ICR-3 · Participant 生产 RPC 外壳

- 现状： legacy Runner 直连 `logicrpcpb.FarmWorkflowServiceClient`；合同 29 §7.6/§7.7 冻结 ParticipantRequest 统一外壳 + `CoordinatorParticipantService.ExecuteStep` / `WorkerRecoveryIngress.ResumeStep` 双入口 + 15 条 ParticipantCommandCatalog。
- 建议归属： AR1（Participant Ingress）→ PI1/FI1（Handler）→ SC1（Coordinator 切换）；WF1 的 `ParticipantInvoker` 端口即为适配点。

### ICR-4 · legacy Work Lease 可信时间

- 现状： legacy `Store.ClaimDue` 由调用方本机时钟计算 `lease_until`，无 owner_token、无 Renew（30-14/29 §7.3.1 要求 Repository 可信时间 + accepted_ttl_ms）。
- 建议归属： WF1 通用族（`rediswork.go`）已按可信时间实现；legacy 族是否迁移到同一 Key 族或保留至 SC1 重构，需 I0 决策。

## 8. 残余风险

1. legacy 族与通用族并存于一个包：RecoveryLoop 主链唯一，但存储族有两套 Key（`{workflow}` 与 `{work-v1}`）；farm-worker 装配（I0）需为两族各跑一次 Loop 或按 ICR-4 决策合并。
2. 通用族 WorkRecord 采用 JSON 存储编码（非 Proto）；Proto 化需等合同 IDL 评审，不影响逻辑正确性。
3. 通用 Redis 族未经真实公司 Redis Cluster 验证（CROSSSLOT/延迟/持久化未探针）；`{work-v1}` 单 Tag 保证同槽，但容量与 ACL 未测。
4. `BucketResolver` 未接入：WorkAddress 的 `bucket_id` 目前由调用方给定，`Validate(resolver)` 未在引擎路径强制（独立桶布局待 31 号 Bucket 数冻结）；引擎只校验形状。
5. DirectDispatcher 的字节上限按入队声明字节记账，真实 Participant 负载序列化大小由生产 Adapter 提供。
6. legacy `Runner` 的 Participant 语义（Inspect/Complete/ApplyReward）与 29 号 Catalog 不等价，四场景在 SC1 前仍不是合同主链。

## 9. 后续接缝（AR1/PI1/FI1/SC1/I0）

- **AR1**：Participant Ingress 真实身份/Holder Fence 校验后，将 `ExecuteStep/ResumeStep` 适配到 WF1 `ParticipantInvoker`；Work Lease 校验在 Mailbox 前（29 §7.4）。
- **PI1/FI1**：按 29 §7.8 Catalog 实现 15 条 Participant 命令与 ParticipantReceipt 原子提交；WF1 `Definition`/`StepCall` 已携带 invocation、payload hash、依赖 result hash。
- **SC1**：为 UnlockPlot/Plant/Harvest/Theft 各实现一个 `Definition`（步骤表见第 10 节），复用 `Engine.Create`（先 CreateIfAbsent 再推进）；Care 迁移到来源 Order（ICR-2）。
- **I0**：composition root 装配 Dispatcher/RecoveryLoop 与指标注册；farm-worker 的 bucket 分片扫描归属（Rendezvous + Scan Lease）由 I0 用 R1 `ClaimBucketScanLease` 原语接线，WF1 的 Loop 只负责单仓库扫描-领取-恢复。
- **Q1/Q2**：真实 Redis 探针与容量压测后回填第 5 节 PENDING 值。

## 10. Plant/Harvest/Care/Theft 只读适配矩阵（不修改业务）

合同步骤表来源：29 §7.8 ParticipantCommandCatalog、32 §2A/§3/§4/§5/§6。

| 场景 | 合同恢复锚点 | 合同步骤计划（目标 Actor） | 现有代码锚点 | 差距 | 适配归属 |
|---|---|---|---|---|---|
| UnlockPlot | 独立 Workflow（INDEPENDENT_WORK_BUCKET） | `RESERVE_UNLOCK_COIN`(Player) → `COMMIT_PLOT_UNLOCK`(Farm) → `COMMIT_UNLOCK_COIN_RESERVATION`(Player)；`FARM_REJECTED/FARM_NO_CHANGE` → `RELEASE_UNLOCK_COIN_RESERVATION`(Player) → COMPENSATED | **不存在**（proto 无 UNLOCK_PLOT kind，legacy Runner 无分支） | 全缺；需要新 Definition + Participant Handler | SC1 + PI1/FI1 |
| Plant | 独立 Workflow | `RESERVE_SEED`(Player) → `COMMIT_PLANT`(Farm) → `COMMIT_SEED_RESERVATION`(Player)；`FARM_REJECTED` → `RELEASE_SEED_RESERVATION` → COMPENSATED | legacy `WorkflowKind_PLANT`：Inspect → CommitReservation/ReleaseReservation → Complete | 无 Reservation→FarmCommit 分离步骤；无 crop_cycle 冻结步骤载荷；无 step hash 链；锚点形态接近可适配 | SC1 写 Definition；PI1/FI1 写 Handler |
| Harvest | 独立 Workflow | `COMMIT_HARVEST`(Farm) → `GRANT_HARVEST_REWARD`(Player)；依赖 `commit_harvest_result_hash` | legacy `WorkflowKind_HARVEST`：Inspect → ApplyReward → Complete | 无 HarvestDecision 结果 hash 依赖链；无 CompendiumUnlockDelta 原样传递 | SC1 + PI1/FI1 |
| Theft | 独立 Workflow（root=FRIEND_PAIR，经 ActionAdmission） | `DECIDE_THEFT`(Farm) → `SETTLE_THEFT_VISITOR`(Player visitor) → `CREDIT_GUARD_INCOME`(Player owner) | legacy `WorkflowKind_THEFT`：Inspect → ApplyReward(beneficiary) → Complete | 无 Admission/Permit 绑定；无双 Player 分别结算步骤；无 GuardDecision 快照传递 | SC1 + SO1（Admission）+ PI1/FI1 |
| Care | **来源同槽 CareRewardOrder**（SOURCE_PERSISTENCE_BUCKET，非 Workflow） | `APPLY_CARE_REWARD`(Player carer)，单步 | legacy `WorkflowKind_CARE`（独立 Workflow，settlement 多参与者） | **锚点形态冲突**（ICR-2）：应按 Farm 来源事实 + Order + DueIndex | FI1（来源提交）+ SC1（迁移） |

通用框架与合同步骤的映射证明：TestWorkflow 的 `RESERVE→COMMIT→FINALIZE`+`RELEASE` 补偿分支与 Plant/UnlockPlot 形状同构；`TEST_ORDER` 单步与 CareRewardOrder/GuardSyncOrder 形状同构；SUPERSEDED 覆盖 GuardSync 代次语义；依赖 result hash 链（`previousResultHash`）覆盖 Harvest/Theft 的 Decision hash 依赖。

公共接口缺口（除 ICR-1/2/3 外）：无新增 ICR；`Definition.BuildPayload` 的 Canonical Builder 需消费 P1B 已冻结的 CanonicalPayloadBuilder 注册表（接缝已预留：payload bytes + sha256 hash 由 Definition 产出，引擎不关心编码）。
