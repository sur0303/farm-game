# P1A-SCB1 · Shared Contract Baseline(SCB-1)交接

日期:2026-08-03
状态:`COMPLETED / UNIT_PASS`

```text
task_id:     P1A-SCB1
branch:      codex/p1a-scb1-20260803
worktree:    /data/home/developer_026/CodeBuddy/20260519141303/farm-game-p1a-scb1
base_commit: 6b78d2f5b7793afe8636d919280c85c7849d6200
commit:      1b5e53eea457f2def7942cba085418d9054faaca(SCB-1 实现);
            本 handoff 的 hash 回填为紧随其后的文档小提交,分支 HEAD 即最终交付点
```

## 1. 目标与边界

基于现有代码补齐第 29/31/32～37 号跨进程共享合同,形成独立 SCB-1:统一
EffectiveIdempotencyKey/OperationRootRef/WorkAddress/SourceAuthorityRef/
WorkType/ParticipantStep/ParticipantInvocationID,登记 Player/Farm/Friendship
Participant 及 Typed Step Payload/Result,提供 Fake Client/Server、注册完整性
测试与 Canonical Hash/StableOpaqueID 黄金向量。

本 Commit **不含任何业务裁决**:无作物规则、无钱包数学、无 Redis Key/Lua、
无 Actor/Workflow 运行时、无 CommandCatalog 12→33 迁移、无 cmd/* 入口改动。

## 2. 现有实现 → 合同要求 差异矩阵

| 项 | 现有实现(复用) | 合同要求 | 处置 |
|---|---|---|---|
| StableOpaqueID 算法 | `internal/workidentity.StableOpaqueID`(LP+SHA-256+`sid1_`,与 31 §8.1A 一致) | 同 | **复用**,identity 只做组合,未建第二套算法 |
| WorkRefV1/ReceiptRefV1 | `workidentity.EncodeWorkRefV1/EncodeReceiptRefV1`(`wr1_`/`rr1_` 黄金已冻结) | 31 §8.1B 同格式 | **复用**,不重写 |
| WorkflowID | `workidentity.WorkflowID` 用 `farm-workflow-id-v1`(3 字段) | 27 §6.1/31 §8.1A 冻结 `farm-workflow-id-v2`(5 字段,含 Root) | **冲突**。SCB-1 在 `internal/identity.WorkflowID` 落地 v2 并锁黄金;v1 仅供现行主链,迁移属后续 WF 任务(见 remaining_risks) |
| SourceOrderID | `workidentity.SourceOrderID(orderType, sourceRouteDomain, ...)` 参数名为 route domain | 31 §8.1A 第二字段是 `stable_source_authority_kind`(PLAYER_ACTOR/FARM_ACTOR/FRIEND_PAIR) | **语义澄清**。identity.SourceOrderID 按 authority kind 实现并锁黄金;结构兼容,旧调用方迁移属后续任务 |
| OperationDigest | `workidentity.OperationDigest`(3 字段:subject/command/operation_id) | 31 §8.1 冻结 5 字段(追加 root_kind+canonical_root_id) | **补齐**。`identity.EffectiveIdempotencyKey.OperationDigest` 按 5 字段实现并锁自冻结回归向量 |
| TrustedCommandContext / ActorRouteHeader | `api/proto/logicrpc/logic_service.proto` 已实现,字段与 29 §3.1/§4.1 一致 | 唯一权威在 29 | **复用**,新 ParticipantRequest 直接引用 `logicrpc.ActorRouteHeader`,不复制 |
| EffectiveIdempotencyKey/OperationRootRef | 仅有 Go 旧版 3 字段 digest | 28 §3.2/31 §8.1 | **新增** `contracts/identity.proto` + `internal/identity` 类型与校验 |
| WorkAddress/SourceAuthorityRef/WorkType/ParticipantStep | `workidentity/address.go` 有 Go 版 WorkAddress(无 proto;bucket hash 待 W31-A);ParticipantStep 只有散落字符串 | 29 §6.2 | **统一**:`contracts/identity.proto` 一次登记;`internal/identity` 冻结稳定名;`internal/contracts` WorkType 注册表(placement/允许来源/允许 step) |
| Participant 统一外壳 | 无 ParticipantRequest/Response、CoordinatorWorkContext、InternalCallerContext | 29 §6.2/§7.4/§7.6/§7.7 | **新增** `contracts/participant.proto`,含 `CoordinatorParticipantService.ExecuteStep` 与 `WorkerRecoveryIngress.ResumeStep` 两个 Unary |
| 15 个 ParticipantCommand | 无枚举、无 Catalog;步骤以裸字符串传入 | 29 §7.8 冻结 | **登记** `ParticipantCommandType`/`ParticipantStep` 枚举 + `internal/contracts` Catalog(目标 RouteDomain 按 §7.8 表逐一钉死) |
| Typed Step Payload/Result | 无 | 29 §7.9～§7.14A | **新增** `contracts/steps.proto` 全部 15 组 Schema |
| 跨边界 Snapshot | CropRuleSnapshot 等无统一 Schema | 29/32/36/37:同一 Snapshot 只能一个 Schema | **新增** steps.proto 单一定义(CropRule/StealPlan/CarePlan/CompendiumUnlock(Snapshot+Delta)/ActionPermit/Guard(Snapshot+Rule+Decision)/CareResolution/HarvestDecision/TheftTargetDecision) |
| Canonical Hash | `internal/idempotency` 全 envelope deterministic-marshal(代码内 TODO 标明违反 28 §3.5);commandcatalog 仅 PLANT 手写 builder | 29 §7.11:固定顺序+定宽整数+presence bit+计数前缀,禁止 Hash protobuf bytes | **新增** `internal/contracts.CanonicalHash`(protoreflect 按 field number 序遍历,拒绝 map/扩展/未知字段),黄金钉死 |
| 旧 Participant 服务 | `logicrpc.PlayerParticipantService/WorkerCallbackService/FarmWorkflowService` 为现行主链旧接缝 | 29 §7.7 两入口统一 Catalog | **并存不迁移**。SCB-1 不动旧主链;新旧并行属后续接线任务,禁止在本任务形成第二套业务实现(本任务也未实现任何业务) |

## 3. 交付内容

### 3.1 Proto IDL(一次性 field number 分配;演进只能追加+reserved)

- `api/proto/contracts/identity.proto`:RootAuthorityKind、OperationRootRef、
  EffectiveIdempotencyKey、WorkType(12)、WorkPlacementKind、SourceAuthorityKind、
  SourceAuthorityRef、WorkAddress、ParticipantStep(22)。
- `api/proto/contracts/participant.proto`:ParticipantCommandType(15)、
  CoordinatorWorkContext、InternalCallerContext、ParticipantRequest/Response、
  ParticipantOutcome、ParticipantFailure、两个 Unary 服务。
- `api/proto/contracts/steps.proto`:15 组 Typed Step Payload/Result + 全部跨边界
  Snapshot;字段声明顺序即 Canonical Hash 语义字段顺序。
- `api/gen/contracts/*`:由 `bash scripts/gen-proto.sh` 统一生成(唯一生成方式,
  未手改);重跑脚本无 diff(幂等已验证)。

### 3.2 Go 共享包

- `internal/identity`:EffectiveIdempotencyKey/OperationRootRef 校验与 31 §8.1
  OperationDigest;`farm-workflow-id-v2` 派生;SourceOrderID(authority kind);
  FriendRelation/QuotaReservation/QuotaReleaseFence/ShareTokenValidationReceipt/
  FriendCommitAttempt 派生;WorkType/ParticipantStep/SourceAuthorityKind 稳定名
  注册表。算法复用 `internal/workidentity`,未复制。
- `internal/contracts`:
  - `CanonicalHash`(合同 29 §7.11 protoreflect 实现,fail closed);
  - ParticipantCommandCatalog(15 项)+ WorkTypeRegistry(12 项);
  - `FakeParticipantServer`(ExecuteStep/ResumeStep 入口,执行前校验:命令注册、
    RouteHeader 存在/actor_id>0/RouteDomain 与 Catalog 目标一致、caller context
    类型、step↔command 绑定、WorkType 相等、placement/source 形状
    (SourceAuthorityKind∈AllowedSourceKinds、Actor source 复核 RouteDomain、
    FRIEND_PAIR 禁带 route domain、work_id 为合法 sid1_)、确定性 InvocationID
    绑定、MaxPayloadBytes、Canonical Step Hash、Typed Result Schema 与注册
    Result 完全一致;Receipt 重放:同 invocation+同 hash 返回原结果、不同 hash
    返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`;TIMEOUT_UNKNOWN 不落 Receipt;
    per-receipt in-flight 单飞:并发同 invocation 只执行一次 Handler,等待者
    replay 或 conflict,错误/TIMEOUT_UNKNOWN 释放等待者允许重试;replay 对
    stored Failure/Result 做 clone 隔离)与 `FakeParticipantClient`。
    Fake 不含业务裁决,handler 由测试注入;物理 bucket hash 不在此校验
    (W31-A 范围)。
- `internal/protocoltest`:合同 31 §8.1A 全部 12 个黄金、33A §2.7 全部 6 个黄金、
  OperationDigest 自冻结回归向量、Canonical Hash 3 个黄金(含逐字节 preimage
  黄金)与 fail-closed 校验测试。

## 4. 测试原始结果

基线(施工前,base_commit 6b78d2f):

```text
go test ./...  PASS(exit=0)
go vet ./...   PASS(exit=0)
```

交付态(本分支 HEAD,含代码审查修复后复跑):

```text
gofmt -l internal/contracts internal/identity internal/protocoltest api/gen/contracts
→ 无输出(干净)
git diff --cached --check → PASS
bash scripts/gen-proto.sh 重跑 → git 无新增 diff(生成幂等)

go test -race -count=1 -timeout=90s ./internal/contracts/... ./internal/identity/... ./internal/protocoltest/...
ok  farmgame/internal/contracts  1.036s
?   farmgame/internal/identity [no test files](黄金测试位于 protocoltest)
ok  farmgame/internal/protocoltest 1.024s

go test -count=1 -timeout=180s ./...  PASS(全仓 70 包 ok / 其余 [no test files],exit=0)
go vet ./...                          PASS(exit=0,无输出)
```

覆盖的关键语义断言:

- 15 命令枚举↔Catalog 一一对应、稳定名唯一、Schema 可 Hash、目标 RouteDomain
  与 29 §7.8 表逐项一致;12 WorkType placement/来源注册完整,wire 枚举名与
  稳定名一致。
- Canonical Hash:确定性、字段敏感、optional presence bit、repeated 计数前缀、
  嵌套 length-delimited、未知字段 fail closed、构造顺序无关。
- Fake 执行前校验:RouteHeader 缺失/actor_id=0/RouteDomain 与 Catalog 目标
  不一致(RESERVE_SEED 带 FARM Header、APPLY_GUARD_SNAPSHOT 带 PLAYER
  Header)均 fail closed;WorkType 不等、非法 work_id、source kind 不属于
  AllowedSourceKinds、Actor source RouteDomain 错配/缺失、FRIEND_PAIR 携带
  route domain、canonical id/layout version 缺失均 fail closed;伪造
  InvocationID、placement 错配、未注册命令、畸形 payload 均 fail closed。
- Fake Receipt 语义:重放返回原 outcome/result/hash/version;同 invocation
  不同 payload 返回 PARTICIPANT_IDEMPOTENCY_CONFLICT;错误 Typed Result
  Schema fail closed 且不落 Receipt;TIMEOUT_UNKNOWN 不持久化;Worker 恢复
  路径同 invocation 命中 Receipt;replay 对 Failure/Result clone 隔离,调用方
  污染首次响应不影响后续 replay。
- Fake 并发(-race):8 并发同 invocation Handler 只执行 1 次、7 个 replay
  且结果一致;同 invocation 不同 payload 并发时 Handler 只执行 1 次、其余
  得到 CONFLICT;不同 invocation 独立推进(2 次执行);首个 Handler 注入
  错误时等待者被释放并由新 leader 重试成功。
- 黄金:31 §8.1A(12 个)+ 33A §2.7(6 个)逐字符一致;Canonical preimage
  逐字节一致。

## 5. evidence_level

`UNIT_PASS`

- 依据:以上 `go test`/`go vet`/`gofmt`/`git diff --check`/生成幂等全部在本
  worktree 本 HEAD 真实执行。
- 不申报 DOMAIN_LOGIC_PASS:本任务按设计不含业务裁决,无领域规则可证。
- 不申报 INTEGRATION_PASS 及以上:未接 gRPC 真实进程、未接 Redis、未接
  PlayerActor/FarmActor。

## 6. not_executed

- 两个 Unary 服务的真实 gRPC 进程间调用(仅 in-process Fake)。
- Participant Ingress 完整管线(真实服务身份、Holder Fence、Lease 复核、
  Mailbox):属后续 WF/AR 接线任务。
- CommandCatalog 12→33 迁移、CanonicalPayloadBuilder 逐命令接入(P1B 范围)。
- `bucket_version/bucket_id` 物理哈希(W31-A 范围,workidentity 明确 deferred)。
- 跨语言(Lua/H5)黄金对拍:黄金值已按合同文本逐字符录入,Lua 夹具尚未存在。

## 7. remaining_risks

1. **WorkflowID v1/v2 双轨**:现行主链 `workidentity.WorkflowID` 仍是
   `farm-workflow-id-v1`(3 字段),合同冻结 v2(5 字段)。迁移需要现行
   WorkflowStore 数据一并考虑,属于后续 WF 任务;在迁移完成前,新旧 ID 体系
   不得混用于同一 WorkRecord。
2. `workidentity.SourceOrderID` 第二参数历史命名为 route domain,与 31 §8.1A 的
   authority kind 语义不同;旧调用方传入值(如 `FARM`)与合同(`FARM_ACTOR`)
   不同,接入 identity.SourceOrderID 时必须整体切换,不能半迁移。
3. `internal/idempotency.CommandPayloadHash` 的 deterministic-marshal 实现仍被
   现行主链使用且自知违反 28 §3.5;SCB-1 未触碰,替换属 P1B。
4. `logicrpc.PlayerParticipantService` 等旧 Participant 接缝与新两入口并存;
   谁先在生产接线由后续任务决定,存在过渡期双接缝风险(无双业务实现,因
   SCB-1 无业务实现)。
5. Proto field number 属合同 PHYSICAL_PENDING 层;SCB-1 已按"一次性分配+
   只追加不复用"纪律执行,后续任何 Agent 修改 contracts/*.proto 必须先走 ICR。

## 8. next_dependency

- I0 合入 SCB-1 并发布 `SHARED_CONTRACT_BASELINE=<本 Commit>`。
- P1B(CommandCatalog 33 迁移)消费 `contracts/identity.proto` 的
  EffectiveIdempotencyKey/OperationRootRef 与 `internal/contracts.CanonicalHash`。
- WF1/AR1(Workflow Runtime、Participant Ingress 接线)消费两 Unary 服务、
  Catalog、Fake 与黄金向量。
- R1(Redis Repository)消费 `identity.EffectiveIdempotencyKey.OperationDigest`
  与 WorkRefV1;不得在 Redis 包内再定义第二套算法。
- FO1/MA1(好友、邮件)消费已登记的 WorkType/ParticipantStep 与 Friend 系列
  StableOpaqueID 派生。

## 9. ICR(合同歧义记录,未自行发明)

1. 29 §6.2 的 P0 WorkType 注册表字面为 `*_WORKFLOW/*_ORDER` 后缀名,而 31 §8.1A
   黄金使用 `PLANT`/`CARE_REWARD` 等短名。本任务以黄金向量为唯一权威登记短名;
   若合同本意是长名,需 ICR 修改 31 §8.1A 黄金并重冻。
2. `release_reason`(Unlock/Seed Release Payload)合同未枚举取值,本任务按诊断
   字符串保留,不参与状态机;如需枚举化请 ICR。
3. `TraceContext` 在 29 逻辑 IDL 中出现但现行 proto 以 `trace_id string` 表达,
   本任务沿用现行表达;如需结构化 TraceContext 请 ICR。

## 10. 回滚方式

```text
git revert <本 Commit>
# 或合入前直接删除分支:
git push origin --delete codex/p1a-scb1-20260803
```

回滚影响面:仅删除新增 proto/gen/三个 Go 包与本 handoff,不触碰任何现行主链
文件;现行 12 命令主链、旧 Participant 服务、workidentity 黄金均不受影响。

## 11. 交付 Commit

files_changed(全部在 owned_write_set 内):

```text
api/proto/contracts/identity.proto            (新增)
api/proto/contracts/participant.proto         (新增)
api/proto/contracts/steps.proto               (新增)
api/gen/contracts/identity.pb.go              (生成)
api/gen/contracts/participant.pb.go           (生成)
api/gen/contracts/participant_grpc.pb.go      (生成)
api/gen/contracts/steps.pb.go                 (生成)
internal/identity/doc.go                      (新增)
internal/identity/names.go                    (新增)
internal/identity/root.go                     (新增)
internal/identity/derive.go                   (新增)
internal/contracts/canonical.go               (新增)
internal/contracts/catalog.go                 (新增)
internal/contracts/fake.go                    (新增)
internal/contracts/canonical_test.go          (新增)
internal/contracts/catalog_test.go            (新增)
internal/contracts/fake_test.go               (新增)
internal/protocoltest/scb1_stableid_golden_test.go   (新增)
internal/protocoltest/scb1_canonical_golden_test.go  (新增)
docs/verification/p1a-scb1-handoff.md         (本文件)
```

完整 commit hash:`1b5e53eea457f2def7942cba085418d9054faaca`(SCB-1 实现);
本文件的 hash 回填提交为分支 HEAD,以 `git log -1` 为准。
