# ScalePlan SP3 生产 Adapter 规划接手文档

> 文档用途：供下一位负责人在**不重新解释已冻结协议、且不越过授权边界**的前提下，接手 ScalePlan SP3 Production Adapter 规划工作。
>
> 本文是接手入口，不是生产资格结论、部署授权或性能窗口授权。

---

## 1. 当前结论（先读）

```text
worktree=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-sp3-protocols-20260812
branch=codex/scaleplan-sp3-protocols-20260812
protocol_baseline_head=5082719feed98a6b107da52a65e9b81aec00ceb8
protocol_baseline_worktree=CLEAN_BEFORE_HANDOFF_DOC
G21_RUNTIME_IDENTITY=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361

SP0-SP2_CORE=PASS
SP3_PROTOCOL_BLOCKERS_1_TO_5=PASS
SP3_0_COMPATIBILITY_REVIEW=PASS
SP3_0_MARKER=SP3_0_PASS_READY_TO_PLAN_ADAPTERS

adapter_planning_authorized=true
adapter_implementation_authorized=false
deployment_authorized=false
runtime_activation_authorized=false
performance_sampling_authorized=false
ready_for_sp3=false
supports_30m_dau=false
```

五项协议 blocker 已独立复审通过：

```text
1. SERVING_TO_READY_ZERO_SHARD_LIFECYCLE=PASS
2. DURABLE_MANAGER_WIDE_PLACEMENT_EXCLUSION=PASS
3. POD_WORKLOAD_ORDINAL_BOOT_IDENTITY_BINDING=PASS
4. ROUTEPLAN_EXACT_GET_AND_RETENTION_ALIGNMENT=PASS
5. BLOCKED_TERMINATION_QUIESCENCE_AND_PROOF_OWNERSHIP=PASS
```

SP3-0 clean-HEAD 兼容性复审身份：

```text
reviewed_head=c06ae6a552bfb3c27f8d5283dc3ae7111c1799f4
verdict=PASS
P0/P1/P2=0/0/0
marker=SP3_0_PASS_READY_TO_PLAN_ADAPTERS
```

`5082719` 只是在上述 reviewed HEAD 之后提交了状态与兼容性复审文档，没有改协议代码。

---

## 2. 接手后可以做什么、不能做什么

### 2.1 已授权

当前只授权：

1. 只读梳理现有协议与运行时接口；
2. 编写 Production Adapter 实施计划；
3. 定义 adapter package、durable schema、原子性、权限与 failure matrix；
4. 设计 fake/in-process 测试、restart/unknown-outcome 测试和后续真实低负载验证步骤；
5. 对实施计划进行独立复审。

### 2.2 尚未授权

未经用户下一次明确授权，不得：

- 编写生产 Kubernetes/Redis/Quiescence Adapter；
- 修改部署清单以启用 ScalePlan；
- 连接外部 Redis 或 Kubernetes；
- 读取公司 Redis secret；
- 执行 SCAN、Cleanup、压力或容量测试；
- 运行真实 `1 -> 2 -> 1`；
- Runtime Activation；
- 性能采样或正式性能窗口；
- 声明 `ready_for_sp3=true` 或 `supports_30m_dau=true`；
- Push、Merge、Rebase Integration 或 Tag。

### 2.3 Go 命令前置条件

执行任何 Go test/race/vet/build 前必须检查：

```bash
test -e /data/farm-locks/formal-perf.lock && echo PRESENT || echo ABSENT
```

若为 `PRESENT`，不得运行上述 Go 命令。最近确认状态为 `ABSENT`。

---

## 3. 为什么要单独做 Adapter 规划

当前通过的是**协议完整性与代码内核兼容性**，不是生产实现。

Core 已明确了 Saga 的命令式状态机、外部副作用前的 write-ahead intent、CAS、恢复、阻塞所有权与证据保留；五项 blocker 则补齐了生产必须满足的生命周期、排除、身份、RoutePlan 证据和安全终止合同。

但是以下组件仍不存在：

```text
Production Kubernetes WorkloadScaler=NOT_IMPLEMENTED
Production durable ScalePlan repository=NOT_IMPLEMENTED
Production BlockedQuiescencePort/Fence=NOT_IMPLEMENTED
Trusted Kubernetes identity join=NOT_IMPLEMENTED
Production controller composition=NOT_IMPLEMENTED
Real ScalePlan evidence writer=NOT_IMPLEMENTED
```

Adapter 规划必须先证明这些生产组件能实现冻结协议，而不是在实现过程中反向修改协议来迁就基础设施。

---

## 4. 冻结架构与不可改变的顺序

### 4.1 ScalePlan 是命令式 Saga

ScalePlan 接收显式计划，不负责决定 CPU/QPS/P99 阈值，也不负责自动决定扩缩容目标。

输入已包含：

- workload；
- expected/desired replicas；
- 明确 shard move manifest；
- RoutePlan TTL；
- ScalePlan start TTL 与 recovery retention；
- 功能限制（当前版本固定 `1/1`）。

### 4.2 Scale-in 顺序不可重排

```text
SERVING
-> PlacementExcluded
-> 仍可服务 / Renew / 作为 RoutePlan Source
-> 逐 shard 迁空
-> READY_ZERO_SHARD
-> 双 Zero-Shard Proof
-> StatefulSet 缩容
```

关键语义：

- `PlacementExcluded` 不等于 `DRAINING`；
- excluded victim 在仍有 shard 时必须保持 `SERVING`、process-ready、non-draining；
- Manager 禁止它成为新 target，但它仍是合法 source；
- 非空 manifest 的 victim 不得提前报告 `READY_ZERO_SHARD`；
- 每个 child effect 前后都要重新验证 victim 生命周期、generation、exclusion、assignment freshness、lease 和 placement capture；
- 只有迁空后才允许 `READY_ZERO_SHARD` 与最终 StatefulSet 缩容。

### 4.3 Holder 生命周期是可循环的

```text
WARMING -> READY_ZERO_SHARD <-> SERVING -> DRAINING -> DRAINED
```

Adapter 不得把 `READY_ZERO_SHARD` 当作一次性初始状态，也不得把 PlacementExclusion 映射为 Holder `DRAINING`。

---

## 5. 五项协议 blocker 的生产含义

## 5.1 Blocker 1：Holder 生命周期

已冻结：Holder 可以从 `SERVING` 在 shard 归零后回到 `READY_ZERO_SHARD`，并可再次进入 `SERVING`。

生产 Adapter/Controller 需要保留：

- assignment snapshot freshness；
- serving shard count；
- process readiness；
- placement eligibility；
- lifecycle transition 的精确一致性。

不得仅靠 Pod readiness 判断 Holder 是否可缩容。

## 5.2 Blocker 2：Manager-wide Placement Exclusion

排除事实是 durable、exact-Boot、带 owner/revision 的 Manager-wide admission fact。

所有 target 相关路径都必须执行：

- target admission；
- commit；
- publication；
- retry/recovery；
- effect wave serialization。

Production Adapter 计划必须明确排除事实存在哪里、如何 CAS、如何读取、如何在线性化点阻止新 placement，以及 Manager 重启后如何恢复。

## 5.3 Blocker 3：Pod/Workload/Ordinal/Boot 精确绑定

动态 Holder identity：

```text
Domain / HolderID / BootID / PodUID / WorkloadUID / Ordinal
```

冻结语法：

```text
visible ASCII bytes 0x21..0x7e
no whitespace
max 128 bytes
ordinal = canonical non-negative int32
```

动态模式：

```text
ACTOR_ASSIGNMENT_MODE=remote-watch
ACTOR_HOLDER_IDENTITY_MODE=kubernetes-process-v1
```

BootID 是一次进程 generation：

- 进程启动时随机生成 128-bit；
- 禁止 exact 模式从配置复用 Boot；
- Authority、HolderControl、HolderAgent 共享同一 Boot；
- PodUID/WorkloadUID/Ordinal 必须从 Kubernetes API 与 owner reference 独立可信 join；
- Holder 自报 claims 不是 Kubernetes 证明。

静态模式保留 zero deployment tuple，不能被动态 adapter 混用。

## 5.4 Blocker 4：RoutePlan exact Get 与 retention

生产 Adapter 必须使用 exact child identity/read：

- exact `GetPlan(plan_id)`；
- terminal plan 在物理保留期内仍可读取；
- source/target 完整绑定 HolderID/BootID/PodUID/WorkloadUID/Ordinal/endpoint；
- expected source Authority fence；
- owner ScalePlan ID；
- owner `RecoveryRetainUntilMs`。

保留期：

```text
MaxPlanTTL 7d + MaxRecoveryRetention 30d = 37d
production RoutePlan audit retention = 38d
```

RoutePlan v1/v2 与 Redis 旧物理格式必须按现有兼容规则读取/迁移；完整 validation 失败必须零写入。

一旦 `EnsureObserved=true`，child evidence missing/malformed/mismatch/failed/regressed/verification inconsistency 必须将父计划保持 `BLOCKED` 并保留 workload ownership。

## 5.5 Blocker 5：Blocked termination proof ownership

旧模式已经删除：

```text
VerifySafeTermination -> safe=true -> Executor generic CAS
```

当前协议：

```text
repository-shared mutation barrier
-> acquire whole-plan quiescence fence
-> canonical manifest + proof
-> final fence revalidation
-> repository-time exact terminal commit
-> atomically persist Plan + Receipt + Proof
-> atomically release workload ownership
-> bounded non-cancelled fence cleanup
```

普通 `ScalePlanRepository` 不暴露安全终止提交；generic CAS 禁止构造：

```text
ErrorCode=OPERATOR_SAFE_TERMINATION
BlockedTerminationReceipt
BlockedTerminationProof
```

Production repository 与 owner 必须绑定同一私有 identity，所有 Executor mutation 共享 repository-owned barrier。

---

## 6. Blocked termination 证据清单

Production `BlockedQuiescencePort` 必须在同一 fence 下生成完整证据。

### 6.1 Workload 与 Pod

- WorkloadRef exact match；
- UID、resourceVersion、generation、observedGeneration；
- replicas、readyReplicas；
- contiguous Pod ordinals；
- PodName/PodUID/HolderID 唯一；
- Pod `Present/Ready` 与 ready count 一致；
- 每个 Pod 有且仅有一个 exact Holder join。

### 6.2 Holder

- Domain 与 workload domain 一致；
- HolderID/BootID/PodUID/WorkloadUID/Ordinal exact；
- Boot/process identity 不重复；
- PodName join；
- assignment known fresh；
- lease 覆盖 proof expiry；
- 对 active placement holder：
  ```text
  State=SERVING
  ProcessReady=true
  Draining=false
  ServingShardCount>0
  placement endpoint == Holder.LogicEndpoint
  ```

### 6.3 Placement 完整快照

Manifest 不是只包含 submitted move keys；必须是明确 scope 下的完整 domain/workload snapshot：

- `Complete=true`；
- domain；
- snapshot revision；
- snapshot checksum；
- topology fingerprint；
- total active cardinality；
- placements 使用同一 snapshot revision；
- canonical sorted、无重复；
- 每个 submitted move key 必须存在；
- 允许并验证 move set 之外的 active placements；
- 每个 placement exact join 到 Holder、endpoint、fence、freshness 与 lease。

### 6.4 Child RoutePlan 与 Manager

每个 child 必须包含：

- parent-observed phase；
- 当前独立读取 phase；
- 当前 phase 必须 terminal；
- `DurablyStopped=true`，且其语义必须在 fence 覆盖期内保证不会恢复执行；
- source/target exact identities、endpoints、source fence；
- Manager transition ID 与 recovery ID 精确 join；
- component evidence retention 覆盖 aggregate retention。

仅仅父计划最后看到 `PAUSED` 或 provider 返回 `Quiescent=true` 不够；Adapter 计划必须定义“durably stopped”的真实生产操作、存储位置、fence/lease 和重启恢复语义。

---

## 7. Durable ScalePlan Repository 规划要求

这是 Adapter 计划中风险最高的部分之一。

### 7.1 必须提供的普通语义

- `CreateOrGet`：idempotency key + payload digest；
- workload single-flight active index；
- exact `Load(plan_id)`；
- one-revision CAS；
- bounded `ListRecoverable`；
- terminal records 不可普通 CAS 修改；
- corruption fail closed。

### 7.2 安全终止私有语义

必须在一个原子线性化操作内：

1. 读取 exact current blocked record；
2. 校验 revision/reason/payload/checkpoint；
3. 使用 repository authoritative time；
4. 校验 proof capture/expiry/retention；
5. 重新计算 proof digest；
6. 写入 terminal plan；
7. 写入 durable receipt 与完整 proof；
8. 删除/转换 workload active ownership index；
9. 支持 apply-then-unknown exact replay。

禁止分成“写 terminal plan”和“释放 workload lock”两个操作。

### 7.3 需要在计划中冻结的 Redis 物理设计

尚未设计，接手人需要提出并复审：

- keyspace/hash-tag；
- plan record schema/version；
- idempotency index；
- workload active index；
- recoverable time/index set；
- terminal retention；
- atomic Lua transaction；
- receipt/proof wire encoding；
- corruption detection/checksum；
- bounded list/recovery；
- old/new schema migration；
- unknown outcome 与 retry；
- ACL command/key scope；
- 禁止任何全库扫描依赖。

禁止命令：

```text
FLUSHALL
FLUSHDB
SCRIPT FLUSH
```

未经授权不得接触外部 Redis。计划与测试阶段只允许 in-process miniredis 或纯内存 fake。

---

## 8. Kubernetes Adapter 规划要求

## 8.1 WorkloadScaler

需要实现并证明：

- exact StatefulSet namespace/name/UID；
- resourceVersion precondition；
- expected replicas precondition；
- mutation unknown outcome 后 exact re-read/adopt；
- observedGeneration/generation convergence；
- Pod inventory 与 owner-reference join；
- scale-in 只删除最高 ordinal victim；
- workload UID replacement fail closed；
- API timeout、429、conflict、NotFound、partial observation 分类；
- 不将 readiness 当作 shard/Holder 安全证明。

## 8.2 Trusted HolderStateReader

必须同时观察 Kubernetes API 与 Holder/RouteControl claims，并精确比较：

- Pod UID；
- StatefulSet owner reference UID；
- canonical ordinal；
- Workload UID；
- HolderID/BootID；
- PodName；
- endpoints；
- lifecycle；
- assignment snapshot freshness；
- lease。

任何缺失、partial list、resourceVersion drift 或不一致都必须 fail closed。

## 8.3 Placement snapshot adapter

必须定义“完整”的 API：

- scope（realm/domain/workload）；
- snapshot revision/checksum/fingerprint；
- total active cardinality；
- canonical sorted placements；
- capture token；
- end-bracket `Revalidate`；
- 不允许逐 shard 拼接成假快照。

---

## 9. Production Quiescence Fence 规划要求

这是生产实现中最需要先写设计、后写代码的部分。

计划必须回答：

1. fence 锁住哪些写者？
   - ScalePlan Executor；
   - RoutePlan executor/recovery；
   - Manager transitions/recovery；
   - Kubernetes workload mutation；
   - placement publication/admission。
2. fence 是集中式 lease、组合 fence，还是事务内 capability？
3. 如何证明 fence 在：
   ```text
   capture -> proof -> revalidate -> repository terminal commit
   ```
   全程有效？
4. 进程崩溃后如何超时/恢复/释放？
5. terminal commit 成功但 fence release response unknown 时如何处理？
6. 如何防止两个 controller/owner 使用不同本地锁？
7. 如何让所有 child terminal 或 durable cancelled，且 controller 重启不会恢复它们？
8. fence 与 repository authoritative time 如何对齐？
9. component evidence retain-until 如何由存储层保证？

不要把进程内 mutex/channel 当生产 fence。

---

## 10. 建议的 Adapter 工作拆分

必须新建独立 Adapter 工作树，不在本协议工作树直接开发。

建议阶段：

### A0 — Adapter Architecture/Threat Model（只写计划）

输出：

- package/component map；
- trust boundaries；
- Redis/Kubernetes/Manager/RoutePlan ownership；
- exact atomicity matrix；
- failure classification；
- ACL/RBAC；
- migration/retention；
- test matrix；
- forbidden operations。

完成后独立复审，达到 `P0/P1/P2=0/0/0` 才能请求实现授权。

### A1 — Durable Repository + in-process tests

仅在明确实现授权后：

- schema/codec/keybuilder/scripts；
- miniredis tests；
- CAS/replay/corruption/retention tests；
- 不接外部 Redis。

### A2 — Kubernetes read/scale adapters + fakes

- fake Kubernetes API；
- owner-reference/UID/ordinal join；
- RV conflict/unknown outcome；
- 不部署。

### A3 — Quiescence/Manager/RoutePlan fence

- durable child stop/cancel；
- Manager recovery fence；
- full placement capture；
- cross-controller restart tests。

### A4 — Composition and restart matrix

- controller composition；
- crash before/after every external effect；
- duplicate operator；
- two controllers；
- apply-then-unknown；
- expired proof；
- workload replacement。

### A5 — Real low-load evidence

必须另行授权 Kubernetes/Redis/部署后才可执行：

```text
1 -> 2 -> 1
```

真实性验证不等于性能资格。

### A6 — Performance/Capacity

必须由用户单独授权。不得因 Adapter 功能完成而自动开始。

---

## 11. 必须覆盖的 Failure Matrix

Adapter 实施计划至少逐项列出：

| 位置 | 故障 | 期望状态 |
|---|---|---|
| Repository create | timeout before/after commit | exact idempotent re-read |
| Workload scale | unknown outcome | re-read exact UID/RV/replicas |
| Exclusion CAS | conflict/unknown | exact read/adopt or BLOCKED |
| Placement capture | partial/stale/token drift | BLOCKED，零 child effect |
| Child Ensure | apply-then-unknown | exact GetPlan/adopt |
| Child evidence | missing/malformed/regressed | BLOCKED，保留 workload lock |
| Manager recovery | active/unknown | 不得安全终止 |
| Quiescence acquire | fence + error | bounded cleanup，保持 BLOCKED |
| Proof | expired at repository point | 不提交，不释放 lock |
| Terminal commit | apply-then-unknown | same TerminationID replay |
| Fence release | response unknown | durable observability/recovery |
| Controller restart | any checkpoint | no duplicate unsafe effect |
| Workload UID drift | any phase | fail closed |
| Pod/Boot drift | child source/target | fail closed |
| Evidence corruption | read/replay | fail closed，零写入 |

---

## 12. 测试与证据要求

### 12.1 实现前计划复审

计划必须由当前可用最高强度 reviewer 独立复审，目标：

```text
P0/P1/P2=0/0/0
```

### 12.2 Code-only 阶段

允许：

- local Go tests；
- in-memory fakes；
- in-process miniredis；
- local Compose render；
- race/vet/diff-check（formal perf lock absent 时）。

要求：

```text
go test ./...
focused affected packages -count=50
go test -race affected packages
go vet affected packages
git diff --check
```

### 12.3 真实环境阶段

只有明确授权后才可：

- Kubernetes API；
- 外部 Redis；
- 部署；
- 真实 RoutePlan/Holder drain；
- 低负载功能证据。

性能采样仍需另一次明确授权。

---

## 13. 关键源码导航

### ScalePlan Core

```text
internal/scaleplan/model.go
  Plan / HolderRef / intents / ResolvedMove
  BlockedTerminationReceipt
  HolderRef.ValidateExact

internal/scaleplan/identity.go
  BuildInitialPlan
  ResolveChildMove
  BuildBlockedTerminationRequest
  BlockedCheckpointDigest
  BuildBlockedQuiescenceProof
  BlockedQuiescenceProofDigest

internal/scaleplan/state_machine.go
  ValidateInitialPlan
  ValidatePlanUpdate
  ValidateBlockedTerminationUpdate

internal/scaleplan/executor.go
  Submit / Execute / Step / Pause / Resume / ReconcileBlocked
  scale-out / scale-in state machine
  validateBeforeScaleInChildEffect
  validateBeforeScaleDownEffect
  OperatorFailBlocked

internal/scaleplan/repository.go
  ordinary ScalePlanRepository contract

internal/scaleplan/blocked_termination.go
  BlockedTerminationOwner
  repository identity binding
  mutation barrier / proof fence / terminal commit ownership

internal/scaleplan/ports.go
  WorkloadScaler
  HolderStateReader
  ShardPlacementReader
  RoutePlanPort
  PlacementExclusionPort
  BlockedQuiescencePort/Fence and manifests
```

### Route/Holder/Manager

```text
internal/routeplan/model.go
internal/routeplan/executor.go
internal/routeplan/service.go
internal/routeplan/production_verification.go
internal/storage/redis/routeplan/

internal/routecontrol/holder_registry.go
internal/storage/redis/routecontrol/
internal/holderidentity/deployment.go
internal/holdercontrol/agent.go
internal/actorrouting/authority/manager.go
```

### Testkit

```text
internal/testkit/scaleplan/repository.go
internal/testkit/scaleplan/quiescence.go
internal/testkit/scaleplan/workload_scaler.go
internal/testkit/scaleplan/placement_reader.go
internal/testkit/scaleplan/holder_reader.go
internal/testkit/scaleplan/routeplan.go
internal/testkit/scaleplan/exclusion.go
```

---

## 14. 关键文档导航

按以下顺序阅读：

1. `docs/verification/scaleplan-core-handoff.md`
2. `docs/verification/scaleplan-sp3-blocker3-holder-identity-handoff.md`
3. `docs/verification/scaleplan-sp3-blocker4-routeplan-retention-handoff.md`
4. `docs/verification/scaleplan-sp3-blocker5-blocked-termination-handoff.md`
5. `docs/verification/scaleplan-sp3-0-compatibility-review.md`
6. `docs/verification/scaleplan-sp3-protocol-status.json`
7. 本文档

Blocker 1/2 的提交链：

```text
aca078a / 110001c   repeatable lifecycle
fee0de0..b16c0c8    durable Manager-wide exclusion and serialization
```

Blocker 3/4/5 与兼容性提交：

```text
8675033  feat(routecontrol): bind holder deployment identity
dcbccc8  feat(routeplan): align exact recovery retention
547bf0a  feat(scaleplan): own blocked termination proof
c06ae6a  fix(scaleplan): close sp3 compatibility findings
5082719  docs(scaleplan): record sp3 compatibility pass
```

---

## 15. 接手后的第一份交付物

下一位负责人不应立即写 Adapter 代码。第一份交付物应是：

```text
docs/verification/scaleplan-sp3-production-adapter-implementation-plan.md
```

至少包含：

1. 精确 workspace/branch/base identity；
2. 允许与禁止的 scope；
3. 组件图与 package map；
4. Redis durable schema/key/TTL/Lua atomicity；
5. Kubernetes RBAC/API/resourceVersion/owner-reference join；
6. Manager/RoutePlan/quiescence fence 方案；
7. restart/unknown-outcome/corruption matrix；
8. fake/miniredis 测试计划；
9. real low-load evidence 计划（标记为待授权）；
10. performance 计划（标记为单独待授权）；
11. rollback/migration/compatibility；
12. 独立复审标准 `P0/P1/P2=0/0/0`。

建议新工作树命名只作为提案，必须由用户确认具体路径后再创建：

```text
farm-game-scaleplan-sp3-adapters-<date>
```

不得自行假设 R2/L2 或任何敏感 workspace 已授权。

---

## 16. 接手检查清单

```text
[ ] 确认当前协议 HEAD 与 clean status
[ ] 确认 G21 identity 未变化
[ ] 检查 formal-perf lock
[ ] 完整阅读 7 份入口文档
[ ] 不改协议工作树，准备独立 Adapter 工作树提案
[ ] 只写 Adapter 实施计划
[ ] 冻结 durable repository 原子性与 schema
[ ] 冻结 Kubernetes trusted join
[ ] 冻结 full placement snapshot scope
[ ] 冻结 child durable-stop + quiescence fence
[ ] 冻结 failure/restart/unknown-outcome matrix
[ ] 计划独立复审达到 0/0/0
[ ] 获用户明确实现授权后才开始代码
```

---

## 17. 最终状态声明

```text
protocol_complete=true
all_five_blockers_pass=true
sp3_0_compatibility_pass=true
adapter_planning_authorized=true
adapter_implementation_authorized=false
production_adapter_implemented=false
real_scale_out_evidence=false
real_scale_in_evidence=false
performance_sampling=false
ready_for_sp3=false
supports_30m_dau=false
```

本地协议 PASS 不是生产资格；后续任何生产、部署、真实证据或性能结论都必须基于新的明确授权和独立证据。
