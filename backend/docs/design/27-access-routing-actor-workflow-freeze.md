# 27 · 命令路由、Actor 运行时与跨 Actor Workflow 冻结设计

状态：`v4.2-bootstrap-opportunity-route-freeze-candidate`  
日期：2026-08-03  
适用范围：经典农场后台 P0 主干，以及面向 3000 万 DAU 的生产演进边界。  
修订定位：在 `v2.3` 单 Authority Lane 与可靠 Work 锚点合同基础上，补齐 Assignment Revision
跨 27/29/30 号文档的精确映射、好友 ActionPermit 的删除/签发线性化语义，并澄清 FarmChange
只是 Actor Turn 的已提交输出，不是 FarmActor 内长期保存的变化历史；继续冻结 P0 每 Actor 单一主要
AuthorityPersistenceGroup/单 Durable Lane、独立 Workflow 与来源同槽 Order 的唯一恢复锚点规则，
并收口 RouteManager、ShardWriteAuthority 和 Assignment Snapshot 的职责。ActorAuthorityStore、
Fence、CAS、Unknown Outcome 和 Store 结果以第 30 号文档为唯一权威。
本版再将 WorkAddress 从只能表达 Actor 的地址扩展为统一 `SourceAuthorityRef`，以同样表达
Player/Farm Actor 与 Friend Pair 等非 Actor Authority；持久写操作以 `OperationRootRef` 绑定真正
权威根，避免同一 operation_id 在不同目标间误判。
本版进一步按第 32 号 Q32-13 补齐 Harvest/Theft 的明确拒绝与无变化终态、Care 的成功边界，
并冻结“Authority 一旦提交只恢复不回滚”的四场景故障语义。
本版再按 Q32-14 明确 Buy/Sell 是“单一物品类型 + 指定正整数数量”的批量交易，并补齐
UnlockPlot/Plant 在首次 Player Reservation 被明确拒绝时的 Workflow 终态，以及 UnlockPlot 的
FARM_REJECTED/FARM_NO_CHANGE 分支。
本版再按第 33 号 Q33-13 补齐好友 Care/Theft 的可靠 ActionAdmission：Pair Authority 强提交
AdmissionReceipt、Permit、DispatchOrder 与 DueIndex 后才算 Execution-Accept，消除 Permit 只读签发与
删除好友之间的接管窗口。
本版再按第 35/35A 号登记 PlayerMailDomain、MailDeliveryOrder 与可重建 MailClaimProjection，保持
MailEntry 外置分页、附件领取进入 PlayerActor，且不新增 MailActor、mail-svr 或邮件专用 MQ。
本版继续统一 Care 业务凭证与 Unknown `ResolutionReceipt` 的命名边界，冻结有界
`FarmOpportunityHeader`、公开地块 `PlotVersion` 推进，以及已提交 FarmChange 的运行时交接语义；
不改变双 Actor、Workflow/Order 或好友 ActionAdmission 主链。
本版按第 34 号补登记任务领域命令：任务列表、领奖和好友访问确认均路由到 PLAYER Domain，且不创建
TaskActor、任务 Workflow 或任务专用 Worker。
本版再按第 36 号把 PetState 全局页面版本、GuardSyncWatermark、不可变 OperationOutcome 与
`APPLY_GUARD_SNAPSHOT` 来源同槽 Order 接缝收口；GuardSync 动态进度不得反向改写历史宠物命令结果。
本版按第 36/37 号补登记宠物与图鉴：状态查询、金币解锁、喂食、切换、图鉴查询和已读Ack均使用
PLAYER Domain；图鉴页面由CompendiumReadIngress在Actor Turn外组装，PET_UNLOCK_BY_TASK只作为任务
RewardBundle的PlayerActor内部结果，不新增宠物/图鉴Actor或服务。
本版再补齐首屏两条显式查询：`GET_PLAYER_BOOTSTRAP` 按 PLAYER Actor Affinity 进入
PlayerBootstrapIngress，`GET_NEXT_FRIEND_OPPORTUNITY` 按 SERVICE_LB 进入
FriendOpportunityNavigation；两者都不要求 Recovery 完成或当前 FarmSubscription。
对应实现与故障测试通过后再提升为正式 Freeze。

本文成熟度分为三层：带编号的设计决策属于 `STABLE_SEMANTIC`，实现不得自行改变；整份文档仍是
`CANDIDATE`，尚未获得生产验证；VirtualShard 数、Redis Key/Lua、Mailbox/SoftSave/Lease 参数和
Proxyless 接入方式属于 `PHYSICAL_PENDING`，必须由平台探针与压测决定。

> 《26 · 接入与会话详细合同》是 SessionPermit 数据来源、Join 状态机、Subscription 切换和
> 200 人同场语义的唯一权威；第 28 号文档是具体 CommandCatalog Schema 与完整 CommandSpec 的
> 唯一权威；第 29 号文档是 TrustedCommandContext、ActorRouteHeader 和内部 RPC IDL 的唯一
> 权威；第 30 号文档是 ActorAuthorityStore、Fence、CAS、Unknown Outcome 和 Store 结果的唯一
> 权威；第 33 号文档是 canonical FriendRelation、VisitPermit 和 ActionAdmission 的唯一业务权威。
> 第 34 号文档是任务周期、进度和领奖的唯一业务权威。
> 本文只拥有命令到领域/权威的语义矩阵、Actor Route/Runtime，以及跨 Actor 场景应选择
> Workflow 还是来源同槽 Order 的 L1 语义，不复制上述结构。

---

## 1. 设计目标与边界

本阶段要解决的不是完整玩法，而是把一条业务命令从客户端送到唯一权威 Actor，并在跨 Actor、超时、进程退出和路由变化时保持结果可解释。

冻结目标：

1. 客户端身份不能伪造，业务目标必须重新校验。
2. 同一 PlayerActor、FarmActor 在任一时刻只有一个合法写入者。
3. 同一农场内所有写操作拥有唯一裁决顺序。
4. Actor 路由热路径不逐请求查询 Redis 或中心服务。
5. 强事实返回成功前可靠持久化；普通玩家资产允许在明确边界内合并落库。
6. 跨 Actor 操作具有持久恢复锚点、Participant Receipt 和确定的客户端语义。
7. Pod 扩缩容、滚动发布和崩溃恢复不会产生双写。

当前不在本文冻结的参数：

- 最终 VirtualShard 数量；
- Mailbox 最终容量和字节上限；
- SoftSave 最终周期和 RPO；
- 公司 Redis 的 Lua/Cluster Slot/故障返回实测，以及生产持久化目标；
- 是否接入 UC Proxyless；
- 是否在 P1 引入 MQ；
- Workflow/Order DueIndex 的最终固定桶数；
- Actor Runtime 最终使用每 Activation goroutine 还是公平 Worker-Lane 调度器。

这些参数需要平台探针和压测决定，但不改变本文的逻辑合同。

### 1.1 面向实现 Agent 的规范用语

- **必须（MUST）**：违反后会破坏正确性、幂等性或故障恢复。
- **应该（SHOULD）**：默认实现，只有新的压测或平台证据才能偏离。
- **可以（MAY）**：不影响逻辑合同的实现选择。

Shard 数、Mailbox 上限、超时、重试次数和 SoftSave 周期必须来自配置，并暴露指标；不得把本文示例数字硬编码为业务规则。

### 1.2 ID 合同

```text
request_id
→ 一次网络请求/Trace ID，网络重试时可以变化

operation_id
→ 一次业务意图的幂等ID，Redirect、超时和重连时必须保持

workflow_id / order_id / settlement_id
→ 服务端持久执行ID；P0不得为同一业务意图随机生成多个地址
```

同一 OperationRoot 内相同 operation_id 对应不同 payload_hash 必须返回 `IDEMPOTENCY_CONFLICT`。

P0 的独立 WorkflowID 冻结为由业务幂等键确定性派生：

```text
workflow_id = StableOpaqueID(
    namespace = "farm-workflow-id-v2",
    subject_player_id,
    command_type,
    operation_id,
    root_authority_kind,
    canonical_root_authority_id
)
```

`farm-workflow-id-v2` 是 Root 作用域稳定 ID 算法代次；具体 Hash/编码与黄金样例在第 31 号物理合同
冻结。业务 `workflow_schema_version` 只保存在 WorkflowRecord 中，不进入 ID 输入，避免滚动升级
期间同一 Root 的同一 operation 分裂成两个 WorkAddress。相同 EffectiveIdempotencyKey 必须得到同一 workflow_id、同一
WorkAddress，再由 `CreateIfAbsent` 校验 CanonicalPayloadHash：Hash 相同复用原 Workflow，Hash
不同返回 `IDEMPOTENCY_CONFLICT`。
该 StableOpaqueID 不得复用可能随路由迁移变化的 RouteSnapshot.HashSeed。

来源同槽 Order 使用来源事实的稳定身份确定性派生，例如：

```text
order_id = StableOpaqueID(
    namespace = "farm-source-order-id-v1",
    order_type,
    source_authority_kind,
    canonical_source_authority_id,
    source_fact_id,
    source_fact_version
)
```

禁止先随机生成 W1/W2，再分别以不同 WorkAddress 调用 `CreateIfAbsent`；那并不能防止双创建。

---

## 2. 接入与会话主干

本节仅记录 Actor Route 的可信输入。完整流程和硬合同见
[26-access-session-contract.md](./26-access-session-contract.md)。

```text
HTTPS认证/候选Ticket
→ ClientHello原子激活Session
→ Gate完成SessionLane准入和BackendPool占位
→ 注入TrustedCommandContext
→ CommandCatalog选择执行类型和第一跳
```

本文不定义 `TrustedCommandContext` 的字段。具体 IDL 以
[第 29 号文档](./29-internal-rpc-trusted-context.md)为唯一权威；本文只依赖以下语义：

```text
普通农场经营命令
→ 必须存在FarmAdmissionContext
→ 使用admitted_farm_id/admitted_subscription_id

JOIN_FARM
→ 必须存在JoinAdmissionContext

FarmAdmissionContext与JoinAdmissionContext
→ 不得同时存在
```

`SessionPermit` 不是客户端 Token。P0 依靠 Gate 服务身份、mTLS/网络访问控制和完整 Fence
保证内部来源，不要求逐命令 HMAC。客户端只提交业务目标；`player_id`、Session Fence 和
ADMITTED 时刻的 Subscription 必须由 Gate 注入。它只证明“谁通过哪根有效连接发起命令”，
好友资格仍由 FriendRelation/ActionPermit 校验，最终地块与作物结果仍由 FarmActor 裁决。

四种路由严格分离：

```text
TransportRoute     → Frame从哪种公网Transport进入
SessionRoute       → 玩家当前在哪个Gate/Connection
FarmSubscription   → 当前连接正在看哪座农场
ActorRoute         → PlayerActor/FarmActor由哪个Holder承载
```

FarmSubscriptionHub 与 FarmActor 同 Holder，但不是 Actor，也不拥有业务真值。
`JOINING → FLUSHING → ACTIVATING → IN_FARM`、`ArmSnapshot`、`SUBSCRIPTION_READY`、
同连接单一当前 Subscription、Snapshot/Delta 无空洞和按 Gate 聚合广播全部服从第 26 号文档。

---

## 3. 命令领域语义矩阵

公网 `ClientCommandEnvelope` 和 Typed Payload 结构以第 28 号文档为唯一权威。下表只说明
“为什么命令属于某个领域、由哪个权威裁决、是否需要
Workflow”，是业务语义矩阵，不是 CommandCatalog 的实现 Schema。Payload/Result、RouteMode、
ExecutionAcceptPolicy、Recovery、限流、版本和完整 CommandSpec 统一由
[第 28 号文档](./28-command-catalog-public-protocol.md)定义；实现不得根据本节另建第二套 Catalog。

Session 状态不是可以简单比较大小的线性等级。`AUTHENTICATED`、`JOINING`、`RECOVERING`
和 `IN_FARM` 分别代表不同能力，因此准入合同必须表达：

```text
SessionAdmissionSemantic {
    requires_authenticated
    requires_recovery_complete
    requires_current_subscription
    target_policy
}
```

`target_policy` 至少区分：

```text
NONE
PAYLOAD_TARGET
SESSION_PLAYER
CURRENT_FARM
OWN_CURRENT_FARM
SUBJECT_OPERATION
CURRENT_FRIEND_FARM
```

命令执行与会话准入矩阵：

| 命令 | 执行类型/第一跳 | 恢复完成 | 当前订阅 | 目标约束 |
|---|---|---:|---:|---|
| `JOIN_FARM` | `DomainRole` → JoinIngress/Hub | 否 | 否 | `PAYLOAD_TARGET` |
| `GET_PLAYER_BOOTSTRAP` | `ActorDirect` → PlayerBootstrapIngress/PlayerActor | 否 | 否 | `SESSION_PLAYER`；私有首屏状态，不进入FarmSnapshot |
| `GET_FRIEND_LIST` | `DomainRole` → FriendReadAPI | 否 | 否 | `NONE` |
| `GET_NEXT_FRIEND_OPPORTUNITY` | `DomainRole` → FriendOpportunityNavigation | 否 | 否 | `NONE`；viewer只来自Session，Summary粗筛后BatchEvaluate |
| `GET_OPERATION_RESULT` | `DomainRole` → OperationReadAPI | 否 | 否 | `SUBJECT_OPERATION`，只能查询当前玩家 |
| `GET_TASK_LIST` | `ActorDirect` → PlayerActor/TaskReadAPI | 是 | 否 | `SESSION_PLAYER`；完整列表读取任务权威 |
| `CLAIM_TASK` | `ActorDirect` → PlayerActor/TaskCommandIngress | 是 | 否 | `SESSION_PLAYER`；任务与资产同一Player提交 |
| `CONFIRM_FRIEND_VISIT` | `ActorDirect` → PlayerActor/TaskCommandIngress | 是 | 是 | `CURRENT_FRIEND_FARM`；Gate注入已激活Subscription与relation fence |
| `BUY/SELL` | `ActorDirect` → PlayerActor | 是 | 否 | `SESSION_PLAYER`，目标 Player 只来自 SessionPermit |
| `UNLOCK_PLOT` | `WorkflowCoordinator` → farm-svr/UnlockPlotCoordinator | 是 | 是 | `OWN_CURRENT_FARM`：`target == admitted_farm == player_id` |
| `PLANT/HARVEST` | `WorkflowCoordinator` → farm-svr | 是 | 是 | `OWN_CURRENT_FARM`：`target == admitted_farm == player_id` |
| `CARE` | `DomainRole` → FriendFarmIngress/FarmActor | 是 | 是 | `CURRENT_FARM`：`target == admitted_farm`，再校验主人或好友护理权限 |
| `THEFT` | `WorkflowCoordinator` → FriendFarmCoordinator | 是 | 是 | `CURRENT_FARM`：`target == admitted_farm`，且必须通过好友 ActionPermit |
| `GET_PET_STATE` | `ActorDirect` → PlayerActor/PlayerReadAPI | 是 | 否 | `SESSION_PLAYER`；只读当前玩家 PetState |
| `PET_UNLOCK_BY_COIN` | `ActorDirect` → PlayerActor | 是 | 否 | `SESSION_PLAYER`；普通金币扣减与解锁同一 Player 提交 |
| `PET_FEED/PET_SWITCH` | `ActorDirect` → PlayerActor | 是 | 否 | `SESSION_PLAYER`，提交后产生 GuardSyncOrder |
| `GET_COMPENDIUM` | `DomainRole` → CompendiumReadIngress/PlayerActor短读 | 是 | 否 | `SESSION_PLAYER`；Actor Turn外合并Catalog |
| `ACK_COMPENDIUM_SEEN` | `ActorDirect` → PlayerActor | 是 | 否 | `SESSION_PLAYER`；单调推进已观察Revision，不进长期Journal |

Gate 只决定第一跳，不推进跨 Actor Participant。尤其 `UNLOCK_PLOT/PLANT` 不能直接调用
FarmActor 修改 Plot；它们必须先进入对应 Coordinator 并可靠创建 Workflow。

`JOIN_FARM` 的执行类型虽是 DomainRole，第一跳仍须按 `FARM:target_farm_id` 落到目标 Holder 的
JoinIngress/Hub；这条路由策略由第 28 号完整 CommandSpec 定义。它不等于把 Join 投入 FarmActor
业务 Mailbox。

`JOIN_FARM` 同时承担从 A 切换到 B 的语义；P0 不要求额外定义 `SWITCH_FARM`。如果保留
`LEAVE_FARM`，它只能释放当前完整 Subscription Fence，不能按客户端任意给出的 farm_id
删除其他 Subscription。

---

## 4. Player/Farm 双 Actor 权威边界

### 4.1 PlayerActor

```text
PlayerActor(player_id)
├── Wallet
├── Inventory
├── Shop
├── PetState
├── TaskDomain
├── Progression
├── Compendium
└── Reservation / Reward / Claim Receipt
```

`PlayerPersistenceRuntime` 是与 Activation 绑定的运行时组件，不是 PlayerActor 的业务子状态。

以下模块可以与 player-svr 共部署，但不是 PlayerActor 子状态：

```text
player-svr
├── PlayerActor                      # 玩家资产与成长权威
├── FriendshipDomain                 # canonical FriendRelation权威，无FriendActor
├── PlayerMailDomain                 # MailEntry独立存储
├── PlayerReadAPI / FriendReadAPI    # 无状态读与批量拼装
└── Player侧Coordinator              # MailClaim/TaskClaim等PLAYER首跳流程

farm-svr
├── FarmOperationCoordinator         # Plant/Harvest/Theft等FARM首跳流程
├── FriendFarmIngress                # Care/访问上下文/Permit
├── FarmActor                        # 农场业务权威
└── FarmSubscriptionHub              # 可丢实时订阅
```

邮件附件领取必须进入 PlayerActor，使奖励资产和 MailClaimReceipt 可靠防重；但 MailEntry 列表和正文不因此常驻 PlayerActor 内存。

### 4.2 FarmActor

```text
FarmActor(farm_id)
├── FarmActionState
│   ├── Plot / CropCycle
│   ├── CareIncident
│   ├── TheftTargetDecision / TheftAttemptReceipt
│   └── GuardSnapshot
├── FarmVersion
├── FarmOpportunityHeader
├── 必要Receipt热摘要
└── 本Turn产生的CommittedFarmChange输出（非AuthorityRecord历史）
```

跨 PlayerActor/FarmActor 的完整 TheftSettlement/Workflow 是外置可靠执行记录；FarmActor 只持有目标农场侧已经提交的裁决事实和用于防重、恢复映射的标识。
`CommittedFarmChange` 是一次权威提交成功后交给实时层的版本化输出；它可以短暂存在于
`PostCommitDeliveryGuard`、`CommittedChangeQueue` 或共享 VersionRing，但不得被理解为 FarmActor
长期保存全部变化历史的字段。

`FarmOpportunityHeader` 是 Farm AuthorityRecord 内固定大小的派生头，不是 Projection Redis 中的
`FarmOpportunitySummary`，也不是某位好友能否操作的最终裁判：

```text
FarmOpportunityHeader {
    source_farm_version
    opportunity_dirty_generation
    next_opportunity_at_ms
    candidate_mask              // 非viewer个性化的候选类型
}
```

任何改变公开互动机会的 Farm Authority Commit，都必须从同一 CandidateState 重算该 Header，并与
业务事实、`PlotVersion/FarmVersion` 和 `opportunity_dirty_generation` 一起提交。仅因时间经过而成熟
不会产生额外写；`next_opportunity_at_ms` 负责暴露下一次可能变化的时间。Header/Summary 只用于筛选，
好友关系、每好友每茬 AttemptReceipt 和当前 Farm 状态仍由 BatchEvaluate/FarmActor 精确裁决。

`Presence` 和 `JoiningBuffer` 不属于 FarmActor。它们由同 Holder 的 `FarmSubscriptionHub` 管理：

```text
farm-svr Holder
├── FarmActor(farm_id)              # 唯一业务写权
├── FarmSubscriptionHub(farm_id)    # 可丢实时运行态
└── FarmRealtimeState(farm_id)      # delivery generation / level-triggered失效状态
```

同 Holder 是为了在本地完成 `Snapshot(V)` 与 `FarmChange(V+1...)` 的有序交接，不代表二者拥有相同的持久化和生命周期。
`FarmRealtimeState` 不裁决农场业务，也不进入 Authority Store；它只保证已提交变化无法进入普通
实时队列时，Hub 仍能通过独立状态发现该流已失效。Holder 进程丢失后 Subscription 一并重建。

即使 `player_id == farm_id`，二者仍属于独立 Route Domain 和独立权威状态。

### 4.3 同一农场不同地块

不同地块上的无冲突操作应允许都成功，但不要求底层并行执行：

```text
B浇1号地
→ FarmVersion=101

C偷5号地
→ FarmVersion=102
```

同一 FarmActor 的写操作继续串行，以保持 FarmVersion、广播、宠物、批量操作和农场级规则的确定顺序。只有真实压测证明单 FarmActor 成为瓶颈时，才评估批处理或更细分区；P0 不引入 PlotActor。

---

## 5. Actor 路由

### 5.1 稳定映射

```text
ActorAddress = RouteDomain + ActorID
VirtualShard = Hash(ActorAddress) % ShardCount
Holder       = Assignment[RouteDomain][VirtualShard]
```

Actor 到 VirtualShard 的关系稳定；VirtualShard 到 Holder Pod 的 Assignment 可以变化。

### 5.2 本地 Assignment Snapshot

Gate、player-svr、farm-svr 和 farm-worker 在进程内保存不可变快照：

```text
RouteSnapshot {
    global_revision
    hash_version
    hash_seed
    player_routes[]
    farm_routes[]
    holders[]
}
```

它保存的是 `VirtualShard → Holder`，不是数千万个 `player_id → pod`。

热路径：

```text
Hash
→ 本地数组查询
→ 复用Holder gRPC连接
→ 直达目标Pod
```

不逐请求查询 Redis 或 RouteManager。

### 5.3 更新机制

生产推荐：

```text
启动时拉完整Snapshot
+
gRPC Watch接收Revision增量
+
定期Revision校准
+
Redirect即时纠错
```

收到 Revision 缺口时重新拉完整快照。旧 Holder 返回带新 Revision 的 `ROUTE_STALE/REDIRECT`，调用方使用原 operation_id 有界重试。

### 5.4 RouteManager 定位

P0 单 Pod 使用 StaticAssignmentProvider。多 Pod 阶段引入 RouteAssignmentManager。
它是否作为 farm-worker Leader Role、平台控制面 Role 或独立 Deployment 尚未冻结；无论物理
落点如何，都不得进入业务请求热路径。

RouteManager 负责：

- 观察 Pod 注册、Ready、Drain；
- 计算 PLAYER/FARM 独立 Assignment；
- 推进 Shard Handoff；
- 协调 Holder/Boot/writer_epoch 换代并建立初始 Lease；当前 Holder 的后续续租服从完整 Fence；
- 在 ShardWriteAuthority 提交且新 Holder Ready 后发布 Snapshot 与增量。

UC Proxyless 只作为未来可插拔 AssignmentProvider，不是系统正确性或 P0 的依赖。

### 5.5 Shard 数量

最终值按计划最大 Holder Pod 数、期望迁移粒度、Assignment 大小和压测结果确定：

```text
ShardCount >= PlannedMaxPods × TargetShardsPerPod
```

本文不指定 4096/8192/16384 中的任何一个值。实现必须从版本化配置读取，并提供跨进程黄金
样例；不得把候选数字写死为生产合同。

### 5.6 路由表不是 Actor 明细表

进程内不得保存数千万条 `actor_id → pod` 记录。路由表只保存固定长度的 Shard Assignment：

```text
HolderTable
0 → player-svr-a:9000
1 → player-svr-b:9000
2 → farm-svr-a:9000
3 → farm-svr-b:9000

PlayerShardRoutes
0 → HolderIndex 0
1 → HolderIndex 1
...

FarmShardRoutes
0 → HolderIndex 2
1 → HolderIndex 3
...
```

Assignment Snapshot 不保存 Actor 内存地址。目标 Pod 收到请求后，再由本地 ActorRegistry 找到或激活 Actor。

### 5.7 建议的数据结构

以下是逻辑合同，Agent 可以调整文件和命名，但不能删除字段语义：

```go
type RouteDomain uint8

const (
    RouteDomainPlayer RouteDomain = 1
    RouteDomainFarm   RouteDomain = 2
)

type ActorAddress struct {
    Domain  RouteDomain
    ActorID uint64
}

type RouteSnapshot struct {
    GlobalRevision uint64
    HashVersion    uint32
    HashSeed       uint64
    Player         DomainSnapshot
    Farm           DomainSnapshot
    Holders        []HolderRef
}

type DomainSnapshot struct {
    ShardCount uint32
    Routes     []ShardRoute // len必须等于ShardCount
}

type ShardRoute struct {
    HolderIndex         uint32
    State               ShardRouteState
    ShardRevision       uint64
    ExpectedWriterEpoch uint64
}

type HolderRef struct {
    InstanceID string
    BootID     string
    Endpoint   string
    State      HolderState
    Weight     uint32
}
```

Revision 的跨文档映射冻结为：

```text
RouteSnapshot.GlobalRevision
→ 整份不可变Assignment Snapshot的发布版本
→ ActorRouteHeader.source_snapshot_revision

ShardRoute.ShardRevision
→ 同RouteDomain/VirtualShard已提交ShardWriteAuthority.assignment_revision
→ ActorRouteHeader.shard_revision

Holders[ShardRoute.HolderIndex].InstanceID/BootID
+ ShardRoute.ShardRevision
+ ShardRoute.ExpectedWriterEpoch
→ 必须来自同一份已提交ShardWriteAuthority记录并在同一次Snapshot发布中原子可见
```

禁止把新 Holder、旧 `ShardRevision` 和另一个时刻的 `ExpectedWriterEpoch` 拼成一条 Route；也禁止
把 `GlobalRevision` 当成某个 Shard 的写权版本。第 29 号只承载上述字段，第 30 号拥有
`ShardWriteAuthority.assignment_revision` 的写权语义。

`player_id`、`farm_id` 和内部 ActorID 的逻辑类型统一为 `uint64`。H5/JSON 边界以十进制
字符串承载，避免 JavaScript `Number` 精度问题；Protobuf、Go 路由和存储层使用 `uint64`。
Hash 的规范字节序、算法和 Golden Samples 已在 5.8 冻结，不能用字符串拼接或语言默认序列化
代替规范编码。

使用 HolderIndex 避免在每个 Shard 条目里重复保存 Pod 地址。`BootID` 每次进程启动变化，用于识别“同一个 Pod 名称但已经是另一代进程”。

`HolderState` 与 `ShardRouteState` 只允许 `ACTIVE`、`DRAINING`、`ASSIGNING`。只有 Holder 与
ShardRoute 同时为 `ACTIVE` 时才能返回可执行 Assignment；任一处于迁移态均返回
`SHARD_DRAINING/RETRYABLE`，不得投递 Actor Mailbox。生产多 Pod 环境的 `BootID` 必须非空且在
该 InstanceID 的进程代次内唯一；本地单 Pod 静态探针允许省略。

### 5.8 Hash 版本合同

路由 Hash Profile v2 正式冻结为：

```text
HashVersion = 2
HashSeed    = RouteSnapshot.hash_seed，必须显式配置且非 0

canonical_input_v2 =
    ASCII("CFRT")                    // 4 bytes
  + uint32_be(HashVersion)           // 4 bytes
  + uint16_be(len(UTF8(realm)))      // 2 bytes，按 UTF-8 字节数
  + UTF8(realm)
  + domain_byte                      // PLAYER=0x01，FARM=0x02
  + uint64_be(actor_id)              // 8 bytes

hash_value    = XXH64(seed=HashSeed, input=canonical_input_v2)
virtual_shard = hash_value % ShardCount
```

`ShardCount` 只要求大于 0，不要求为 2 的幂。禁止用位掩码替代 `%`，否则非 2 的幂配置会得到不同
路由。Realm 必须是合法、非空且不带首尾空白的 UTF-8；长度按 UTF-8 字节数计算，最大 65535 字节。

Golden Sample：

```text
HashSeed  = 202608030001
Realm     = "classic-farm"
ActorID   = 10001
ShardCount= 1024

PLAYER canonical hex:
4346525400000002000c636c61737369632d6661726d010000000000002711
PLAYER hash_value = 11000288192871273299
PLAYER shard      = 851

FARM canonical hex:
4346525400000002000c636c61737369632d6661726d020000000000002711
FARM hash_value = 1295869252430091328
FARM shard      = 64
```

冻结约束：

1. Hash 算法、输入编码和黄金样例必须共同版本化。
2. PLAYER/FARM 必须把 RouteDomain 纳入 Hash 输入。
3. `HashSeed` 是 RouteSnapshot 的权威字段，与 HashVersion 在同一 Snapshot/Revision 原子下发；
   调用方不得从本地配置文件补一个可能不同的 Seed。
4. 同一环境的所有调用方使用相同版本。
5. H5 不计算 VirtualShard。
6. 修改 HashVersion 或 ShardCount 属于路由迁移，不能混入普通滚动发布。
7. 修改 HashSeed 或 Realm 同样属于路由与持久化 Bucket 迁移。
8. v1、缺少 HashSeed、Seed 为 0 或旧 flat Assignment JSON 必须显式返回
   `HASH_MIGRATION_REQUIRED`，禁止静默按 v2 解释。

`HashVersion` 决定算法和规范编码，`HashSeed` 给出该环境当前实际 Seed。两者缺一都不能令
`RouteReady=true`。Snapshot Watch/全量刷新必须把二者和 PLAYER/FARM Routes 作为一个不可分割
版本安装；不存在“先更新 Seed、稍后更新 Routes”的中间可服务状态。

P0 即使只有一个 Holder，也必须执行完整 Hash，再把全部 Shard 静态指向同一 Holder，避免将来重写业务 Handler。

### 5.9 ActorRouteClient

```go
type ActorRouteClient interface {
    Resolve(addr ActorAddress) (ResolvedRoute, error)
    Invoke(ctx context.Context, addr ActorAddress, cmd *InternalActorCommand) (*ActorReply, error)
    ApplyRedirect(redirect RouteRedirect) bool
}

type ResolvedRoute struct {
    Address          ActorAddress
    VirtualShard     uint32
    Holder           HolderRef
    ShardRevision    uint64
    ExpectedEpoch    uint64
    SnapshotRevision uint64
}
```

`Resolve` 热路径只允许：

```text
读取atomic snapshot指针
→ 计算Hash
→ 数组读取ShardRoute
→ HolderIndex读取HolderRef
```

不得访问 Redis、Tcaplus、K8s API 或 RouteManager。

路由客户端禁止再维护高基数 `actor_id → Assignment` Map 缓存。正常解析本来只是一次 Hash 和
固定长度数组读取；per-actor 缓存会随历史访问 Actor 数无界增长、引入全局锁竞争，并可能在
缓存命中时绕过 Lease/Holder 状态的最新检查。允许缓存的是：

```text
不可变 VirtualShard → Holder RouteSnapshot
Holder → 长连接gRPC Channel/连接池
```

不允许缓存的是：

```text
每个player_id/farm_id → Pod Assignment
```

### 5.10 内部 RPC 路由头

Gate、Coordinator 或 worker 调用 Holder 时必须携带纯路由 Header，其具体 IDL 只由
[第 29 号文档](./29-internal-rpc-trusted-context.md)定义。本文冻结它必须表达的语义：

```text
目标Actor地址：route_domain + actor_id
Shard地址：virtual_shard + shard_revision
目标Holder Fence：expected_holder_id + expected_holder_boot_id
写权前置条件：expected_writer_epoch
调用方路由视图：source_snapshot_revision + source_instance_id + source_boot_id
```

`request_id`、`operation_id`、Deadline 和 Trace 属于 ExecuteCommandRequest 或 ParticipantRequest
外层，不属于 ActorRouteHeader。路由头不得因为调用类型不同而出现第二套字段布局。

客户端命令同时携带由 Gate 构造的 `TrustedCommandContext`；其字段来源语义服从第 26 号文档，
具体内部 IDL 只服从第 29 号文档。Worker 恢复命令不伪造玩家
Session，而是携带受信任的 `InternalCallerContext`，并绑定原 `subject_player_id` 和
`workflow_id/order_id`。

`expected_writer_epoch` 只是调用方根据当前 Assignment Snapshot 携带的路由前置条件，不是
写权凭证。真正写权来自 Holder 本地有效 Shard Lease，以及 Authority Store 对
`ActorRecord.writer_epoch` 的原子校验；仅伪造 Header 不能获得写权。

### 5.11 AssignmentProvider

```go
type AssignmentProvider interface {
    GetSnapshot(ctx context.Context) (*RouteSnapshot, error)
    Watch(ctx context.Context, afterRevision uint64) (<-chan AssignmentUpdate, error)
}
```

分阶段实现：

```text
StaticAssignmentProvider
→ 本地、单Pod、P0骨架

DiscoveryAssignmentProvider
→ 多Pod，完整Snapshot + gRPC Watch

UCProxylessAssignmentProvider
→ 平台验证后可选
```

ActorRouteClient 不得依赖具体 Provider。

### 5.12 Snapshot 更新协议

启动流程：

```text
GetSnapshot
→ 校验HashVersion、HashSeed、ShardCount、Routes长度和Holder索引
→ 预热Holder gRPC连接
→ atomic.Store(snapshot)
→ RouteReady=true
```

增量流程：

```text
Watch(afterRevision=N)
→ 收到完整的N+1批次
→ 在私有Builder应用Delta
→ 校验ShardRevision单调
→ 一次atomic swap
```

如果本地是 N，却直接收到 N+2，说明丢失了更新，必须重新获取完整 Snapshot。不能猜测中间 Assignment。

后台每 30～60 秒（配置化）只比较 GlobalRevision；不一致才拉取完整 Snapshot。Watch 断开时继续使用最后一份有效快照，RouteManager 暂时不可用不应立即中断已有业务。

### 5.13 Holder RouteIngress 校验

本地 Snapshot 只是路由提示，不授予写权。Holder 收到请求后必须重新校验：

```text
1. RouteDomain属于本进程
2. virtual_shard等于Holder本地重新计算结果
3. Assignment仍指向本InstanceID + BootID
4. ShardRevision未过期
5. Holder拥有有效Lease
6. expected_writer_epoch匹配
7. Deadline尚未过期
```

失败时不得投递 Mailbox：

```text
ROUTE_STALE
→ 调用方刷新Assignment后重试

SHARD_DRAINING
→ 等待新Holder或按retry_after重试

WRITER_EPOCH_MISMATCH
→ 当前实例已经失去写权，停止对应Activation

DEADLINE_EXCEEDED
→ 不进入Mailbox
```

### 5.14 Redirect 与重试

```text
调用方使用旧Snapshot发送
→ 旧Holder返回RouteRedirect(new_holder, revision)
→ 调用方只接受更新的Revision
→ 使用相同operation_id重试
```

自动重试必须同时满足：

- 命令声明为幂等；
- Deadline 仍有预算；
- Redirect Revision 比本地更新；
- 没超过配置化上限，普通路由纠正建议最多一次；
- 业务 `REJECTED/NO_CHANGE` 不重试。

旧 Holder 不代理转发到新 Holder，避免形成多跳、循环和不可观测延迟。

### 5.15 RouteAssignmentManager

RouteManager 不处理业务数据包，它是 Holder/Boot/Epoch/迁移状态换代的唯一协调者，但不是所有
`ShardWriteAuthority` 字段的唯一调用方。逻辑职责必须分成三层：

```text
Redis ShardWriteAuthority
→ 数据面最终写权Fence

RouteAssignmentManager
→ 唯一协调Holder/Boot/WriterEpoch/ACTIVE-DRAINING-ASSIGNING换代

RouteSnapshot / Assignment Delta
→ 已提交ShardWriteAuthority的路由投影
```

RouteManager 的输入包括 K8s/服务发现中的 Pod 注册、Ready、NotReady、容量和 Drain，以及可选的
迁移意图/控制流程记录。若保留独立 `AssignmentStore`，它只能保存期望状态、Handoff 进度和审计，
不能与 Redis 中的 WriterEpoch 各自独立决定谁拥有写权。

字段更新权限冻结为：

```text
RouteManager
→ 改变holder、holder_boot_id、writer_epoch、assignment_revision和迁移状态

当前Holder
→ 只允许在holder/boot/epoch/lease_epoch等完整Fence匹配时续租
→ 不允许自行改变holder、boot、writer_epoch或assignment_revision
```

多 Pod 的安全发布顺序必须是：

```text
RouteManager完成Drain或故障确认
→ 原子切换ShardWriteAuthority到新Holder/新Epoch
→ 新Holder取得有效Lease并Ready
→ 发布与holder/boot/epoch/revision完全一致的RouteSnapshot
```

禁止先发布新 Assignment 再尝试切换写权。若写权已经切到 B、Snapshot 发布暂时失败，结果只能是
短暂不可用：旧 A 的强写被 Store Fence 拒绝，控制面恢复后重新发布投影；不得退回旧写权或形成双写。
ShardWriteAuthority 与 Actor Authority 同线性化域的具体 Redis Key、Lua 和可信时间由第 31 号决定。

P0 单 Pod 不新增 RouteManager Deployment，使用 Static Provider。多 Pod 时需要
RouteAssignmentManager，但它的物理部署位置仍未冻结：可以是平台控制面 Role、现有管理
进程中的 Leader Role，或在规模需要时独立部署。它不得因为实现方便而默认塞进 farm-worker；
Route 控制面和业务恢复 worker 是不同职责。无论最终放在哪里，它都不进入请求热路径；
控制面短暂不可用只会暂停新的扩缩容和迁移，已有 Assignment 继续服务。

#### 5.15.1 Shard Lease 的可信时间合同

Shard Lease 的 Claim/Renew/Expire 由 Lease Authority 使用自身可信时间原子裁决，调用方只提交
`requested_ttl_ms`，响应至少返回：

```text
lease_epoch
writer_epoch
accepted_ttl_ms
assignment_revision
```

Holder 在发送 Claim/Renew 前记录本地单调时钟起点，并建立保守 Deadline：

```text
shard_local_deadline
= renew_started_monotonic
 + accepted_ttl_ms
 - safety_margin
```

Holder 不得把本机墙上 Unix 时间与 Authority 中的绝对 `expire_at` 直接比较。达到本地 Deadline、
Watch 明确通知 LeaseLost，或无法证明仍处于安全窗口时，立即执行第 8.4 节 Self-Fence：停止新命令、
新激活和新 Actor Turn。

Shard Lease 与 Work Lease 的热路径不同：RouteIngress 可以使用 Holder 本地的有效 Lease 证明，
不能每条玩家命令查询中心存储；Authority Store 中的 `writer_epoch + writer_claim_id` 仍是每次
强写的最终存储 Fence。Lease Authority 的可信时间、防双持有语义、故障转移和 accepted TTL
返回必须经第 30/31 号合同与平台探针验证，不能只依靠可能丢失的 Pub/Sub 通知。

### 5.16 Shard Handoff

计划迁移状态机：

```text
ACTIVE(old, epoch=7)
→ DRAINING(old, epoch=7)
→ DRAINED
→ ASSIGNING(new)
→ ACTIVE(new, epoch=8)
```

必须满足：

1. 新 Holder Ready 后才能开始迁移。
2. 旧 Holder 进入 DRAINING 后停止接受该 Shard 新业务命令。
3. 已接受 Turn 在 Drain Deadline 内完成；尚未开始的命令返回可重试错误。
4. PlayerActor 执行 ForceFlush；FarmActor 完成当前可靠提交。
5. 旧 Lease 失效后，新 Holder 才取得更高 writer_epoch。
6. 新 Holder 可写后，Assignment 才发布为 ACTIVE。
7. 旧 writer_epoch 的任何存储写都必须失败。

意外崩溃没有 Drain：

```text
故障确认/Lease到期
→ writer_epoch换代
→ 新Holder ACTIVE
→ Actor懒加载
→ 客户端必要时重新Join
```

不复制 Actor 内存、Mailbox、Presence 或 JoiningBuffer。

### 5.17 FarmSubscriptionHub 与路由迁移

FarmSubscriptionHub 与 FarmActor 同 Holder。普通 Join/Leave、Subscription Lease、网络 ACK 和广播
调度不进入 FarmActor Mailbox；唯一需要与农场权威变化建立顺序的 Join 基线，通过一个有界
`PrepareJoinBaseline` 读/系统 turn 或等价线性化机制取得：

```text
业务命令
→ RouteIngress
→ FarmActor Mailbox
→ 权威提交FarmChange(V)
→ Holder本地CommittedChangeQueue（有界，条目携带delivery_generation）
→ FarmSubscriptionHub按gateway_id聚合广播

ACCEPTED已经进入Gate FIFO
→ ArmSnapshot
→ PrepareJoinBaseline在Farm有序Lane读取Snapshot(V)
→ JoinBaseline(subscription_id, V)进入同一CommittedChangeQueue
→ 后续已提交FarmChange(V+1...)排在Baseline之后
→ Hub安装V+1捕获游标并下发Snapshot
```

`CommittedChangeQueue` 只是 FarmActor 将“已提交变化”交给同进程 Hub 的本地队列，不是
Shard Handoff，也不拥有业务提交权。FarmActor 只在 Authority Store 提交成功后写入该队列；
队列写入失败不能回滚已经成立的业务事实。

队列失败也不能靠“再向同一个已满队列投递一条 RESYNC 事件”恢复。Holder 必须为每座活跃农场
维护独立于普通变化队列的 level-triggered 失效状态：

```text
FarmRealtimeState {
    delivery_generation
    last_enqueued_version
    invalid_from_version   // 0表示当前实时流仍可证明连续
}
```

FarmActor 成功提交 V101 后：

```text
正常入队
→ last_enqueued_version = 101

CommittedChangeQueue满 / Hub不可用 / 无法证明交接成功
→ invalid_from_version = min_non_zero(existing, 101)
→ delivery_generation++
→ 通过独立、保留容量的Control Lane唤醒Hub
```

`PostCommitDeliveryGuard(V)` 必须由同 Holder Runtime Registry 持有，并在发起强提交前安装，
不能等存储已经返回 `COMMITTED(V)` 后才创建，否则 Actor 在两步之间 panic 会留下空窗：

```text
已生成候选FarmChange(V)
→ 安装PostCommitDeliveryGuard(V, state=COMMITTING)
→ 发起Authority Store强提交

明确未提交
→ 清除Guard；不得发布FarmChange(V)

TIMEOUT_UNKNOWN
→ 保留Guard为UNKNOWN；先按Authority Receipt/Version解析

确认COMMITTED(V)
→ Guard转COMMITTED
→ 尝试CommittedChangeQueue(generation, V)
→ 成功：更新last_enqueued_version并清除Guard
→ 失败/Actor panic/局部Hub异常：Supervisor根据COMMITTED Guard幂等安装invalid_from_version<=V
```

这一小段只保护提交后的实时交接，不重新提交业务事实。实现不得存在“Persist 已成功，但既没有
入普通队列，也没有标记失效”的第三种终态。若整个 Holder 进程退出，Hub 与 Subscription 同时
丢失，客户端按 Holder 重建合同 Fresh Join；Guard 不需要跨进程持久化。

Supervisor 观察到尚处于 `COMMITTING` 的 Guard 时，不能猜测提交成功并直接广播，也不能猜测失败
后删除 Guard；它必须按 `TIMEOUT_UNKNOWN` 路径解析 Authority Receipt/Version，再选择清除、入队
或标记失效。

`JoinBaseline`、`CommittedFarmChange` 和 Hub 捕获游标都必须携带 `delivery_generation`。Hub 只能
消费当前代次的条目；旧代次队列中迟到的 Change 在新基线建立后必须丢弃，不能越过新 Snapshot
重新污染实时流。

`invalid_from_version` 必须是同 Holder Farm Runtime Registry 中可重复观察的状态型信号，而不是
一次性事件。即使 Control Lane 通知暂时失败，Hub 的周期检查、下一次 Farm 变化、Join 基线准备或
Hub 重建都必须再次观察它。一旦发现非零值：

```text
现有Subscription → 内部RESYNC_REQUIRED
Hub/Gate下行       → 公网REJOIN_REQUIRED
H5                 → Fresh Join
```

新 Join 从 Authority Store/最新权威内存生成 Snapshot，并安装新的 `delivery_generation`；只有新
基线与后续捕获游标在同一有序 Lane 成立后，才能清除对应旧失效代次。旧实时流失效不能永久禁止
该 Farm Join。Holder 进程整体退出时 Hub/Subscription 本就丢失，客户端仍通过 Fresh Join 重建。

未 Arm 的 Subscription 不得先读取 Snapshot 或注册半套监听。`Snapshot(V)`、JoinBaseline 发布与
`V+1` 之后变化的发布必须共享同一顺序边界；否则 Snapshot 读取与监听注册之间的提交会形成版本
空洞。`PrepareJoinBaseline` 是有界只读/系统工作，不等待 Gate，也不把 200 人的 Presence、续租
和广播搬进 FarmActor 邮箱。若未来使用不可变 Snapshot View 或 farm-level version ring 替代该
turn，也必须证明相同的线性化性质。

Hub 队列满或慢客户端不能反压、回滚 FarmActor 的已提交业务。只有能够证明等价的“绝对状态覆盖”
才允许合并，例如同一 Plot 最终展示状态由 V101 覆盖 V100。普通顺序 Delta、事件动画、
操作结果和 Receipt Hint 不得盲目合并。只要无法证明版本连续，就标记
`RESYNC_REQUIRED`；内部状态与公网 `REJOIN_REQUIRED` 的映射服从第 26/28 号文档。

Shard Handoff 迁移的是 `RouteDomain + VirtualShard` 的写权和 FarmActor 的承载位置，不迁移
Actor 内存、Mailbox、Hub、Subscription、JoiningBuffer 或待发送批次：

这里的 `DRAINING/ASSIGNING` 是 RouteManager 控制面与 Holder Runtime 的 Handoff 状态，不是 Redis
`ShardWriteAuthority.lease_state`。数据面写权记录只允许 `UNASSIGNED/ACTIVE`：Transfer 线性化前是
`ACTIVE(A)`，线性化后是 `ACTIVE(B)`，或在显式卸载时为 `UNASSIGNED`；任何时刻都不能存在两个
可通过 Store Fence 的 `ACTIVE` Holder。

```text
旧Holder进入DRAINING
→ 停止该Shard的新命令和新Join
→ 已ADMITTED的Turn在Deadline内完成并提交
→ 旧Hub停止接受新Join
→ Shard Lease / writer_epoch换代
→ 新Holder获得写权
→ 收到请求时懒加载FarmActor
→ 创建空的新Hub
→ 客户端通过Join + Snapshot重建Subscription
```

如果只有 farm-svr Holder 迁移，WebSocket 和 farm-gate 通常仍然存在，客户端不需要重新登录；
它收到 `REJOIN_REQUIRED`、发现 Subscription Fence 失效或检测到版本缺口后，在同一连接上
Fresh Join 当前农场。迁移期间的请求按阶段处理：

```text
已经ADMITTED
→ 允许在旧Holder完成
→ CommandResult按request_id/operation_id处理

尚未ADMITTED或尚未进入旧Holder
→ SHARD_DRAINING / ROUTE_STALE
→ 刷新Assignment
→ 使用原operation_id有界重试

依赖旧Subscription的新命令
→ STALE_SUBSCRIPTION / REJOIN_REQUIRED
→ Fresh Join后再决定是否重试
```

正在 `JOINING/FLUSHING` 的旧 Hub 上下文不跨 Holder 复制；未完成 Join 的客户端按超时和 Fence
取消旧 Joining，在新 Holder 重新 Join。对最多 200 个同场连接的集中重建必须使用客户端抖动、
Gate 限速和 Hub Join 预算，避免迁移后形成重连风暴。


连接关闭、切换农场和顶号清理必须由 Gate 携带当前已知的精确 Subscription Fence：

```text
farm_id
subscription_id
join_attempt_id
player_id / session_epoch
repository-independent connection fence
```

请求按 `FARM:farm_id` 路由到当前 Holder，并由 FarmSubscriptionHub 条件删除。正常清理复杂度
必须为 `O(1)` 路由加一次 Hub 操作；禁止在 farm-svr 上遍历全部活跃 FarmActor 或向每个
SystemQueue 投递“是否包含该连接”的探测任务。Gate 突然崩溃时才使用 Subscription Lease/TTL、
Gate Boot 失效和重新 Join 兜底。

### 5.18 各进程持有的路由

| 进程 | 使用路由的原因 |
|---|---|
| farm-gate | CommandCatalog 选择 Player/Farm 第一跳 |
| player-svr | 调用 FarmActor Participant、FarmReadAPI |
| farm-svr | 调用 PlayerActor Participant、奖励和 Guard 同步 |
| farm-worker | 恢复 Workflow/Order 时定位 Participant |
| H5 | 不持有 Actor Assignment |

同一进程的所有模块共享一份 RouteSnapshot 和 Holder gRPC 连接池。禁止每个业务模块各建一套路由表。

必须继续区分：

```text
ConnectionRoute：底层连接在哪
SessionRoute：玩家当前在哪个Gate/Connection
ActorRoute：PLAYER/FARM Shard由哪个Holder承载
Subscription：当前连接订阅哪座农场
```

### 5.19 路由可观测性

至少记录：

```text
actor_route_resolve_total{domain,result}
actor_route_redirect_total{domain,reason}
actor_route_snapshot_revision
actor_route_snapshot_age_seconds
actor_route_watch_reconnect_total
actor_route_revision_gap_total
actor_route_holder_rpc_latency
actor_route_holder_rpc_errors
shard_handoff_duration
shard_handoff_failures
writer_epoch_mismatch_total
writer_claim_mismatch_total
writer_claim_total{purpose,result}
unknown_resolver_duration_seconds
```

结构化日志必须带 `request_id`、`operation_id`、RouteDomain、脱敏 ActorID、VirtualShard、Holder
InstanceID/BootID、ShardRevision、WriterEpoch 和脱敏 WriterClaimID 指纹；不得输出可被误用为写权
凭据的完整 claim 值。

### 5.20 路由验收测试

实现 Agent 至少提供：

1. 同 ActorAddress 在所有调用进程计算出相同 VirtualShard。
2. PLAYER/FARM 相同数字 ID 不共享 Route Domain。
3. Snapshot 原子替换时并发 Resolve 不读到半张表。
4. Revision 缺口触发全量刷新。
5. Redirect 保持原 operation_id，且只做有界重试。
6. 旧 Holder 失去 epoch 后不能投递 Mailbox 和写存储。
7. RouteManager 停止时，已有 Assignment 仍能处理请求。
8. Static Provider 切换为 Discovery Provider 不修改业务 Handler。
9. 单 Pod 所有 Shard 指向一个 Holder 时仍经过完整 RouteClient。
10. Handoff 中命令要么在旧 Holder Drain 前完成，要么在新 Holder 幂等完成，不能重复执行。
11. Hub 丢失后业务事实不变，客户端重新 Join 后 Snapshot/Delta 连续。

---

## 6. Actor 激活、Mailbox 与 Turn

### 6.0 Actor 对象不要求全局唯一，有效写者必须唯一

系统不保证同一逻辑 Actor 在全局物理内存中永远只有一个实例。正常迁移、进程暂停、网络分区、
超时恢复和进程重启期间，旧 Holder 与新 Holder、旧 Activation 与新 Activation 可以短暂同时存活。

系统保证的是：同一 Actor Authority 的强写，只有在当前 `ShardWriteAuthority`、`writer_epoch`、
`writer_claim_id`、`record_version` 和 Idempotency Receipt 全部原子匹配时才能提交。旧 Holder、旧
Activation 和迟到请求即使仍然存活，也无法修改权威状态。

因此安全目标不是：

```text
全局任何时刻只有一个Actor对象
```

而是：

```text
全局任何时刻最多一个Actor写身份能够通过权威存储Fence
```

### 6.1 懒激活

ActorRegistry 只保存当前 Pod 上的活跃 Actor。首次命令触发加载，空闲后卸载，不为全部注册玩家常驻 Actor。

```text
ABSENT
→ LOADING
→ CLAIMING
→ ACTIVE
├→ RESOLVING_UNKNOWN
├→ DRAINING
├→ FENCED
└→ PASSIVATING
   → ABSENT
```

同一 Holder/Boot 的 ActorRegistry 内，同一 ActorKey 只允许一次并发 Load。LOADING 期间后续命令
共享同一个 Activation Future，加载成功后再分别进入同一个有界 Mailbox，避免本 Pod 内的加载风暴
和重复激活；全局安全仍由持久 Claim 决定，不能把本地 Singleflight 解释成全局对象锁。

P0 的每次新 Activation 在进入 `ACTIVE` 前必须通过 `LoadAndClaim/ActorWriteClaim` 建立新的持久
`writer_claim_id`；可以把加载与 Claim 合并成一次 Repository 操作。未来只有在压测证明 Claim
写放大不可接受、且能严格区分只读 Activation 时，才允许引入 `ACTIVE_READ_ONLY → CLAIMING →
ACTIVE_WRITABLE` 的惰性优化，不能由实现 Agent 默认省略 Claim。

`writer_claim_id` 不是“单 Actor 热迁移”才需要的可选机制。只要系统支持以下任一能力，它就是
Shard Epoch 之下必需的 Actor 级写权 Fence：

- Actor 钝化后重新激活；
- 同一 Holder 内 Activation 换代或崩溃重建；
- `TIMEOUT_UNKNOWN` 的 C1→C2 结果解析；
- 旧异步持久化请求、回调或迟到 Lua 仍可能到达；
- 可选的热点 Actor Snapshot Handoff。

Shard WriterEpoch 只能隔离 Holder/Shard 代次，不能区分同一 Holder 内的新旧 Actor 内存实例；这正是
持久 `writer_claim_id` 不能被 `activation_generation` 或内存 Registry 代替的原因。

`STOPPED` 仅用于进程退出或不可恢复的监督终止，不是普通空闲卸载后的状态。Actor 进入
`PASSIVATING` 后若收到新命令：

- 写权仍有效且尚未释放：取消本次卸载，回到 `ACTIVE`；
- Shard 已 Drain 或 WriterEpoch 已失效：返回 `STALE_ROUTE/RETRYABLE`，不得复活旧写者。

### 6.2 UserMailbox 与 SystemQueue

同一 Actor 单消费者、逐 Turn 串行。不同 Actor 可以并行。

运行时必须区分：

```text
UserMailbox（有界）
→ 玩家业务命令

SystemQueue（保留容量/高优先级）
→ LoadCompleted
→ PersistenceCompleted
→ UnknownResolved
→ LeaseLost / Drain / Passivate / Stop
→ ActorPanic
```

如果系统消息和玩家消息共用一个已经塞满的队列，`LeaseLost`、持久化完成或 Drain 可能永远
进不来，Actor 会卡死或继续以旧写权运行。UserMailbox 同时限制消息数量和字节数，达到硬
上限返回：

```text
ACTOR_BUSY + retry_after_ms
```

客户端使用原 operation_id 随机退避，不能无限立即重试。

候选数值如 FarmActor 256/512、PlayerActor 64/128 只用于首轮压测，未冻结。

ActorRegistry 还必须限制加载风暴：

```text
max_concurrent_actor_loads
max_pending_actor_loads
per_shard_activation_rate
load_failure_backoff
```

### 6.3 Actor Turn

Actor Turn 只修改本 Actor 的权威状态：

```text
取一条Mailbox消息
→ 校验
→ 计算候选状态
→ 必要的本地持久化
→ 安装新状态
→ 产生结果/Order/事件
→ Turn结束
```

禁止在 Actor Turn 内：

- 等待另一个 Actor；
- 等待跨领域/跨 Actor 的远程 RPC；
- 等待 Gate 下行；
- 等待长时间后台任务；
- 等待客户端真正收到广播。

Actor 可以在有界 Deadline 内等待“本 Actor 权威 Repository”的提交结果，这是当前 Turn
是否成立的一部分；不能据此在 Turn 内同步等待另一个 PlayerActor/FarmActor。跨 Actor
流程由 Actor 外 Coordinator 推进。ActionPermit 若已作为可信输入或本地只读校验可以使用，
但 Turn 内不能临时远程查询 FriendRelation。

#### 6.3.1 ActionPermit 的签发与好友删除线性化

`VisitPermit/ActionPermit` 由 FriendRelation 权威在 Actor Turn 之外签发，并作为受信内部输入交给
FriendFarmIngress/Coordinator。它不是客户端 Token，也不能由 H5 Payload 自报。每份 Permit 至少
绑定：操作者、目标农场、允许的动作范围、FriendRelation 版本/代次、当前 operation 或 participant
身份以及短期有效边界；FarmActor 仍需最终检查 CropCycle、Incident、份额和 AttemptReceipt。

两种 Permit 的可靠性不同：VisitPermit 只建立可撤销的观看 Subscription，可按第 33 号使用一次
canonical Pair Authority 线性化读取；好友 Care/Theft 会修改权威事实，必须逐 operation 执行
`CommitFriendActionAdmission`。该 Pair Authority 强提交原子写 AdmissionReceipt、ActionPermitSnapshot、
FriendActionDispatchOrder 和同桶 DueIndex，其提交本身就是该好友业务的 Execution-Accept。FarmActor
Turn 不再远程查询关系，只验证这份已提交准入与 operation/target/scope 的绑定。

删除/拉黑与好友操作采用明确的先后规则：

```text
删除/拉黑FriendRelation先提交
→ 后续CommitFriendActionAdmission拒绝
→ 新Care/Theft拒绝

CommitFriendActionAdmission先提交
→ 该已接管operation允许按原幂等身份完成
→ 删除/拉黑阻止后续新operation，不分布式取消已接管命令
```

旧页面、旧 Subscription 和旧 Permit 都不能无限复用；每个新的 Care/Theft 业务意图必须取得与自身
operation 绑定的持久 Admission。DirectDispatcher 负责低延迟推进，farm-worker 从 Pair Authority
DueIndex 恢复；不能在 Admission 提交后只依赖普通内存队列。若未来产品要求“删除提交后连已接管命令
也必须撤销”，需要另行设计跨权威取消协议，不能由当前 P0 实现 Agent 自行加强或猜测。

### 6.4 Unknown Outcome

Repository（P0 为公司 Redis）可能已经提交但响应丢失。强写超时未知时，Actor 必须进入
`RESOLVING_UNKNOWN`；禁止把内存简单回滚到 `before` 后继续以 `ACTIVE` 状态处理后续命令。

必须区分三种 Fence：

```text
writer_epoch
→ Shard Holder持久写权代次
→ Handoff/Holder换代时变化

writer_claim_id
→ 当前Authority Record可写Activation或Resolver代次
→ 每次新的可写Activation、Unknown Resolver接管时更换

activation_generation
→ 单进程内Actor/Mailbox生命周期Fence
→ 不进入业务写Lua，不能阻止已经离开进程的迟到写
```

每个强写必须在同一存储原子操作中校验：

```text
expected_writer_epoch
+ expected_writer_claim_id
+ expected_record_version
```

```text
Persist尚未发出
→ CandidateState尚未成为事实
→ 丢弃CandidateState
→ 可以确认本次未提交

Persist明确成功
→ InstallState
→ 返回原成功结果

Persist明确失败
→ 不InstallState
→ 按业务/路由错误返回

Persist已发出但结果未知
→ ACTIVE → RESOLVING_UNKNOWN
→ 暂停该物理记录新的User命令
→ 只允许ReceiptLookup / RecordReload / UnknownResolved / LeaseLost / Stop等System消息
```

解析流程：

```text
旧Activation停止UserMailbox
→ 使用有效Shard Lease进入Unknown Resolver
→ 对同一Authority Record执行FenceAndClaimActor

FenceAndClaimActor(
  expected_writer_epoch,
  old_writer_claim_id,
  new_writer_claim_id,
  resolve_operation_id
)
→ 与Authority Record同一线性化存储域
→ 返回Fence切点上的Record/Receipt摘要

Fence成功 + Receipt存在且Hash一致
→ 原写已经提交
→ 安装权威状态并返回原结果

Fence成功 + Receipt不存在 + Authority状态仍等于操作前状态
→ 原写确定未提交
→ 允许原幂等身份重试

Fence成功 + Receipt不存在 + 状态存在无法解释的变化
→ RECONCILE_REQUIRED
→ 不自动重放

Fence失败或Fence自身TIMEOUT_UNKNOWN
→ 保持RESOLVING_UNKNOWN
→ 上游返回PROCESSING/TIMEOUT_UNKNOWN
```

Redis 对同 Slot 命令给出一个线性顺序：若旧 Lua 先执行，Fence 会看到它提交的业务状态和
Receipt；若 Fence 先把 claim 从 C1 改为 C2，迟到旧 Lua 的 `expected_writer_claim_id=C1` 必须返回
`WRITER_CLAIM_MISMATCH`，不再可能提交。仅仅看到“Receipt 不存在且 RecordVersion 暂未前进”、
关闭客户端连接或等待固定 Grace Period，都不能证明网络中/Redis 输入队列中的旧 Lua 已消失。

`FenceAndClaimActor` 本身必须以 `resolve_operation_id + new_writer_claim_id` 幂等。响应丢失后，
Resolver 可以查询当前 claim：命中同一 resolve 身份时返回原 Fence 结果；仍是 old claim 时可在
Deadline 内重试；出现第三方 claim 时进入 `WRITER_CLAIM_MISMATCH/RECONCILE_REQUIRED`，不能猜测。

`RESOLVING_UNKNOWN` 期间不得返回基于旧内存的 Snapshot、NO_CHANGE 或新的成功结果。若 P0
暂未实现 `FenceAndClaimActor`，最低安全降级是驱逐该 Activation、返回 `PROCESSING`，并把该
ActorKey 放入“阻止重新激活”的 Unknown 隔离表；只有人工/worker 得出终态后才能恢复。单纯驱逐、
立即重载或固定时间后重试都不构成安全恢复。

### 6.5 ActorRegistry 实现合同

```go
type ActorRegistry interface {
    Deliver(ctx context.Context, key ActorKey, msg ActorMessage) (Future, error)
    BeginShardDrain(ctx context.Context, shardID uint32, deadline time.Time) error
    StopLostEpoch(shardID uint32, writerEpoch uint64)
    ReleaseActivation(ctx context.Context, key ActorKey, activationGeneration uint64) error
    Stats() RegistryStats
}
```

Activation 至少保存 ActorKey、VirtualShard、WriterEpoch、WriterClaimID、ActivationGeneration、State、
UserMailbox、SystemQueue、LastActiveAt 和 Load/Persistence 状态。同 ActorKey 的并发首次
请求必须合并成一次 Load；失败时不能留下半初始化 Actor。

### 6.6 Mailbox 入队合同

入队前检查：

```text
Deadline
Shard/Actor是否正在Drain
消息数量和字节上限
ActivationGeneration
```

过期消息不能进入 Mailbox。调用方取消等待不代表已经开始的 Turn 自动撤销；最终结果仍通过 operation_id/Receipt 查询。

同一连接的 connection_seq 只保证该连接顺序；不同玩家操作同一 FarmActor 时，以成功进入 Mailbox 的顺序作为权威顺序。

### 6.7 Actor Turn 编码模板

```text
RouteIngress校验Holder、本地Lease Deadline与ActorRouteHeader
→ ActorRegistry Singleflight LoadAndClaim
→ 有界Mailbox
→ 严格非重入Actor Turn
→ IdempotencyPrecheck（基于Receipt/Result）
→ Validate
→ Decide
→ BuildCandidateState（Farm写同时构造候选FarmChange(V)）
→ Farm写：Arm PostCommitDeliveryGuard(V)
→ 原子Persist（强事实）：
   ShardWriteAuthority/WriterEpoch
   → WriterClaimID
   → Idempotency Receipt/Hash
   → RecordVersion
   → State + Result + 必要Order/Outbox
→ InstallState
→ Farm写：CommittedChangeQueue接管，或安装RealtimeInvalidation
→ ProduceReply/Order
```

若 IdempotencyPrecheck 命中同 Key/同 Hash 的既有终态，直接返回原结果，不能再按当前业务状态重验；
同 Key/不同 Hash 返回 `IDEMPOTENCY_CONFLICT`。预检未命中才执行 Validate/Decide，Persist 内仍必须
再次原子检查 Receipt。Persist 返回 `TIMEOUT_UNKNOWN` 时不得 Install，进入 C1→C2
`FenceAndClaimActor` 解析。

`BuildCandidateState` 只构造临时候选对象，不得原地覆盖当前权威内存。Persist 明确成功前，候选
状态：

```text
不是权威事实
不能被后续Turn观察
不能生成Committed FarmChange
不能返回SUCCEEDED
不能写入下行队列
```

`Persist` 对 CRASH_DURABLE 事实必须在同一原子边界检查
`record_version + writer_epoch + writer_claim_id`，并按
领域需要同时保存 Receipt、Order/Outbox 和提交版本。只有 Persist 明确成功后才能
`InstallState`。明确失败不得安装；结果未知进入 `RESOLVING_UNKNOWN`，不得安装成功或回滚后
继续运行。

上面的 Guard 只适用于会产生公开 FarmChange 的 Farm 强写，并服从第 5.17 节：明确未提交时
清除，结果未知时保留到解析，确认提交后必须由普通实时队列或独立失效状态接管。业务回复不等待
客户端真正收到广播，但不能越过这一本地交接判定后留下静默版本空洞。

Handler 不得启动无法追踪的 goroutine、修改全局业务状态或直接调用另一个 Actor。跨 Actor 下一步交给 Coordinator/Dispatcher。

Actor Turn 可以等待本 Actor 强持久化，因为结果明确前不能继续相关写；但必须设置 Deadline、Unknown Outcome 解析和耗时指标。

### 6.8 过载分层

```text
farm-gate：每连接限流、帧大小、在途命令
RouteIngress：每Shard/Actor激活速率
Mailbox：item/byte上限、oldest wait
FarmSubscriptionHub：session/joining bytes/广播批次预算
ConnectionWriter：每连接有界FIFO
```

热门 FarmActor 不能通过增加同 Actor 并发写线程解决。达到 Hard Limit 返回 `ACTOR_BUSY + retry_after`，不得无限排队。

### 6.9 Activation 释放与 Generation Fence

普通 Passivation、Shard Drain、Actor Panic 和 Lost Epoch 都必须释放 Actor 业务对象与运行时执行
资源。仅从 Registry 的 Actor Map 删除对象而保留 Mailbox/channel/goroutine，不算完成
Passivation。

```text
BeginPassivate(key, generation)
→ 停止该generation接收新User消息
→ 等当前Turn有界结束
→ 处理或拒绝尚未开始的Mailbox消息
→ compare-and-delete同一mailbox指针+generation
→ 停止旧run loop
→ ABSENT
```

所有释放操作必须携带 `ActivationGeneration`。如果旧 Passivate 回调到达时同一 ActorKey 已经
重新激活为更高 Generation，旧回调只能幂等忽略，不能删除新 Mailbox 或新 Actor。

运行时必须暴露：

```text
actor_activation_count
actor_mailbox_count
actor_runloop_count / goroutine_count
activation_release_total
stale_generation_release_ignored_total
```

并通过高 churn 测试证明 Activate→Passivate→Reactivate 后这些指标稳定收敛。

### 6.10 Actor 运行时验收测试

1. 100 个并发首次请求只触发一次 Load。
2. 同一 Actor 永远不出现两个并发 Turn。
3. 不同 Actor 可以并行。
4. Mailbox 满时快速返回 ACTOR_BUSY。
5. Deadline 到期消息不执行。
6. Load 失败后下一请求可以重新激活。
7. WriterEpoch 变化立即停止旧 Activation。
8. Actor panic 后移除并重新加载，不继续使用可能损坏的内存。
9. 200 会话突发操作时 FarmVersion 单调，Hub 与 Actor 分别统计队列和 oldest wait。
10. UserMailbox 满时，LeaseLost、PersistenceCompleted 和 Drain 仍能进入 SystemQueue。
11. 激活洪峰超过预算时快速退避，不把 Redis/存储打成加载风暴。

---

## 7. 持久化分级

### 7.1 四类数据

| 类型 | 语义 | 示例 |
|---|---|---|
| `RUNTIME_ONLY` | 可丢失重建 | Presence、JoiningBuffer |
| `PROJECTION` | 可延迟重算 | 红点、排行榜、好友机会摘要 |
| `ACTOR_SOFT` | 内存先成功，允许小 RPO；必须按命令白名单启用 | 纯 PlayerActor、低风险、无外部副作用的普通资产命令 |
| `CRASH_DURABLE` | 在当前 Environment Durability Profile 下，返回成功前提交到 Authority Store | CropCycle、偷菜、等级、Claim Receipt |

异步不是第五类。Outbox、Order、Workflow 是可靠传递或恢复方式，可以与 `CRASH_DURABLE` 组合。

`CRASH_DURABLE` 默认覆盖业务进程、Pod 或节点崩溃后，Authority Store 仍可用时的恢复；它
不自动承诺 Redis 集群整体数据丢失、跨地域灾难或未经验证的主从切换零 RPO。具体保证必须
由当前 Environment Durability Profile 和基础设施探针给出。

### 7.2 FarmActor

多人共享和裁决事实默认 `CRASH_DURABLE`：

- Plot/CropCycle；
- 有效护理；
- 偷菜份额和 AttemptReceipt；
- TheftTargetDecision 与 TheftAttemptReceipt；
- FarmVersion；
- 必要的 Farm 侧 OperationResult/ResolutionReceipt。

完整 TheftSettlement 是 Actor 外 Workflow/Settlement 记录，不作为 FarmActor 内部聚合字段。

重复护理 `NO_CHANGE` 不产生奖励 Order，也不执行无意义写入。

### 7.3 PlayerActor

`ACTOR_SOFT` 不能仅根据“字段是普通金币/种子/作物”推导，必须按 CommandCatalog 中的命令
白名单开启。只有纯 PlayerActor、低风险、没有跨 Actor Participant、没有已发布外部副作用
的操作，才可候选使用 SoftSave。

以下继续可靠提交：

- 付费货币；
- 经验和等级；
- 永久解锁和图鉴；
- 任务/邮件领取；
- Reservation；
- Reward/Charge/Claim Receipt；
- 跨 Actor 结果。

只要一次操作包含 Reservation、Reward/Claim、跨 Actor Participant、已向外部发布的不可逆
事实，或需要和 Receipt 原子防重，即使修改的是普通金币、种子或作物，也必须经过
Durability Barrier，不得走 SoftSave。

实施顺序：

```text
P0b-1：所有写先CRASH_DURABLE
P0b-2：压测后对白名单普通资产开启ACTOR_SOFT
```

### 7.4 P0 单一 AuthorityPersistenceGroup 与 Durable Lane

P0 每个 `PlayerActor` / `FarmActor` 只有一个主要 AuthorityPersistenceGroup 和一条 Durable
Persistence Lane：

```text
PlayerActor / FarmActor
├── 一个Actor级WriterClaim
├── 一个Group RecordVersion/CAS边界
└── DurableAuthorityLane
    ├── SoftSave（首版关闭，未来仅对白名单开放）
    ├── DurabilityBarrier
    ├── ForceFlush
    ├── UnknownOutcomeResolve
    └── LoadAndClaim / FenceAndClaim回调安装
```

单 Group/单 Lane 不等于把所有业务状态写成一个巨大 Blob，也不等于所有玩家共用一条数据库队列。
Economy、Task、Compendium、Receipt、来源 Order 和冷模块可以是同一版本化 Hash Tag 下的多个 Redis
Key；一次命令涉及的强写只更新必要 Key，但必须共享同一个 Actor Claim、Group RecordVersion、
Unknown 隔离边界和提交顺序，并按命令要求由一次原子提交完成。

同一 Actor 的业务 Turn 本来就严格串行；不同 PlayerActor/FarmActor 仍在不同 Shard/Pod 上大规模
并行。P0 禁止仅为了并行 I/O 给 Economy、Task、Compendium 各建一条可独立 Fence 的 Lane，否则
任一 Lane 将 Claim 从 C1 换到 C2 时，其他 Lane 的 C1 在途写会同时失效，无法只隔离单条 Lane。

只有真实容量证据证明单 Group 成为瓶颈后，P1 才能设计多个 AuthorityPersistenceGroup。拆分前
必须先冻结 GroupClaim/ClaimSet、每 Group Version、命令到 Group 的依赖矩阵、跨 Group
Intent/Order，以及 Group 级 Unknown 隔离；不能把“多个 Key”直接等价为“多个独立写权 Lane”。

### 7.5 ActorAuthorityStore 权威引用

`LoadAndClaim`、`Commit`、`FenceAndClaimActor`、幂等预检、Store 返回状态和 Unknown Outcome 的
唯一逻辑合同是 [第 30 号文档](./30-actor-authority-store-contract.md)。本文不再复制第二套 CAS
请求结构或 Store 枚举，只保留调用方必须遵守的边界：

1. Claim/Fence 接口与玩家业务 Commit 分离，取得写资格不代表任何玩家业务成功；
2. `claim_operation_id + new_writer_claim_id` 必须幂等，响应丢失后不能生成新 Claim 盲重试；
3. WriterEpoch/WriterClaim 不匹配表示失权，必须停止当前 Activation，而不是普通重载；
4. RecordVersion 冲突只能按第 30 号合同有界执行“重载 → 幂等预检 → 业务重验 → 新 Candidate”；
5. 已发送但结果未知必须进入持久 Fence 解析，不能伪装成普通不可用或自动重放。

### 7.6 PlayerAuthorityGroup 的经济原子边界

逻辑 Wallet、Inventory、Shop、Reward 可以拆模块，但普通资产和对应防重 Receipt 不得拆成无法原子提交的物理记录：

```text
PlayerAuthorityGroup（同一Hash Tag，可由多个Key组成）
├── GroupMeta：RecordVersion / WriterEpoch / WriterClaimID / DirtyVersion
├── EconomyState：Wallet / Inventory / ShopCounters
├── ProgressionState：Exp / Level / PermanentUnlocks
├── SoftOperationReceipts
├── Reward / Charge / Claim Receipts
├── Reservations
└── DurableOperationResults
```

一次命令不必重写组内所有 Key，但本次资产变化与防重 Receipt 必须在同一个 Group Commit 中提交，
否则会出现“资产到账但 Receipt 丢失导致重复发奖”，或“Receipt 已完成但资产没有到账”。

### 7.7 SoftSave 与 Barrier 顺序

```text
SoftSave(v10)执行中
→ 内存继续到v11
→ Plant触发Barrier
→ 等v10结果明确
→ 基于最新内存提交v12
```

不得让旧 v10 在 v12 后覆盖。P0 的 DurableAuthorityLane 是单个 Actor Authority Group 的强写
线性化队列，不是全服共享队列；不同 Actor 仍可并行，且一次提交只更新本命令实际涉及的 Key。

### 7.8 P0 公司 Redis 暂定落地方案

当前开发阶段暂定使用公司 Redis 保存：

```text
PlayerActor / FarmActor 当前权威快照
Workflow / Order / ParticipantReceipt
DueIndex（到期工作索引）
RecordVersion / WriterEpoch / WriterClaimID
```

这是一份 **P0 开发实现选择**，不是 3000 万 DAU 生产持久性已经验收的结论。若当前 Redis
未开启满足要求的落盘、复制与备份，本文中的 `CRASH_DURABLE` 在物理上只能解释为：

```text
业务进程崩溃但Redis仍存活
→ 可以恢复

Redis集群数据丢失
→ 当前P0不承诺恢复
```

因此代码必须面向 Repository 接口，后续可替换为经过验证的持久化 Redis 或 Tcaplus，而不
修改 Actor、Workflow 和客户端语义。

### 7.8.1 路由 Shard 与持久化 Bucket：概念分离，`auth-v1` 一一映射

`VirtualShard` 回答“当前由哪个 Holder Pod 执行”，可能因为扩缩容和 Handoff 改变 Holder；
`PersistenceBucket` 回答“哪些 Redis Key 必须永久落在同一个 Cluster Slot”，其映射在数据存续期
内必须稳定。它们是两个不同概念，不能在代码中用同一个类型混为一谈；但 P0 公司 Redis 方案要让
`ShardWriteAuthority`、Actor 权威记录、Receipt 和来源同槽 Order 能在一次 Lua 中完成原子校验，
因此冻结 `auth-v1` 的物理映射为：

```text
ActorKey
├── RouteDomain + VirtualShard                → 执行与Shard写权单位，可迁移Holder
└── AuthorityPersistenceBucket(auth-v1)       → Redis同槽原子域，不随Holder迁移

AuthorityPersistenceBucketID = VirtualShardID
```

`auth-v1` 不再对同一个 Actor 运行第二套独立 Hash。计算顺序固定为：

```text
virtual_shard
  = ShardCalculator(hash_version, hash_seed, route_domain, actor_id)

authority_bucket_version = auth-v1
authority_bucket_id      = virtual_shard
```

这里的一一映射不表示两者语义相同：

- Holder 从 pod-7 迁到 pod-9，只改变 Assignment、Lease 和 WriterEpoch，不改变 BucketID，不搬数据；
- VirtualShard 数、HashVersion 或 HashSeed 一旦用于 `auth-v1` 写入数据，就不能原地修改；
- 将来确需改变分片布局，必须发布 `auth-v2`，执行显式双读/迁移/校验/切流，不能静默重算旧 Key；
- PLAYER 与 FARM 仍是独立 RouteDomain 和独立 Hash Tag 命名空间，即使数字相同也不是同一原子域；
- 第 31 号只冻结具体 ShardCount、Hash Tag、Key 和 Lua，不得重新选择一套独立 Bucket Hash。

### 7.8.2 WorkPlacementKind 与 WorkAddress

逻辑上都属于 Recoverable Work，不代表它们可以使用同一种物理定位。每个 Workflow/Order 必须
具有稳定、可验证的地址：

```text
WorkPlacementKind {
  INDEPENDENT_WORK_BUCKET
  SOURCE_PERSISTENCE_BUCKET
}

WorkAddress {
  work_id
  work_type
  placement_kind
  bucket_version
  bucket_id

  optional SourceAuthorityRef source_authority
}

SourceAuthorityRef {
  authority_kind                 // PLAYER_ACTOR | FARM_ACTOR | FRIEND_PAIR | ...
  canonical_authority_id         // uint64十进制或sid1_...的规范字符串
  authority_layout_version
  optional actor_route_domain    // 仅Actor来源填写，用于一致性复核
}
```

约束：

```text
INDEPENDENT_WORK_BUCKET
→ source字段必须为空
→ bucket由StableHash(work_id)计算

SOURCE_PERSISTENCE_BUCKET
→ source_authority必须存在
→ Actor来源填写actor_route_domain，且必须与authority_kind一致
→ 非Actor来源（例如FRIEND_PAIR）不得填写或伪造Actor RouteDomain
→ bucket由AuthorityPersistenceBucket(source_authority)计算
```

`bucket_id` 不能被 Repository 盲信。创建、读取、Claim 和推进时都必须依据对应
`bucket_version` 重新计算并核对；不一致返回 `WORK_ADDRESS_MISMATCH`，不能尝试另一个桶后继续执行。
WorkRecord 与 DirectDispatcher/Participant RPC 携带完整 `WorkAddress`；DueIndex 位于一个调用方已经
知道的 `WorkBucketRef` 中，其成员只保存第 31 号冻结的紧凑 `WorkRefV1(work_type, work_id)`。
Repository 用扫描 Bucket 上下文补齐 `placement_kind/bucket_version/bucket_id`，再加载 WorkRecord、
重新计算并验证完整 WorkAddress。这样既不使用裸 `work_id` 猜位置，也不在 ZSET 成员中重复整份地址。

当前映射：

| Work | placement_kind | 来源 |
|---|---|---|
| UnlockPlotWorkflow | `INDEPENDENT_WORK_BUCKET` | 无 |
| PlantWorkflow | `INDEPENDENT_WORK_BUCKET` | 无 |
| HarvestWorkflow | `INDEPENDENT_WORK_BUCKET` | 无 |
| TheftWorkflow | `INDEPENDENT_WORK_BUCKET` | 无 |
| AddFriendWorkflow | `INDEPENDENT_WORK_BUCKET` | 无 |
| CareRewardOrder | `SOURCE_PERSISTENCE_BUCKET` | `FARM_ACTOR:farm_id` |
| GuardSyncOrder | `SOURCE_PERSISTENCE_BUCKET` | `PLAYER_ACTOR:player_id` |
| FriendTerminationOrder | `SOURCE_PERSISTENCE_BUCKET` | `FRIEND_PAIR:pair_id` |
| FriendActionDispatchOrder | `SOURCE_PERSISTENCE_BUCKET` | `FRIEND_PAIR:pair_id` |
| MailDeliveryOrder | `SOURCE_PERSISTENCE_BUCKET` | 产生邮件副作用的来源 Actor/非Actor Authority；受信Direct Insert无Order |
| MailClaimProjectionOrder | `SOURCE_PERSISTENCE_BUCKET`（可选） | `PLAYER_ACTOR:player_id`；REBUILDABLE_PROJECTION，不得阻塞资产提交 |
| ResultPublishOrder | `SOURCE_PERSISTENCE_BUCKET` | 产生终态结果的 Actor 或非 Actor Authority |
| 与来源事实同提交的关键 Outbox/PendingIntent | `SOURCE_PERSISTENCE_BUCKET` | 对应来源 Authority |

`FRIEND_PAIR` 是第 33 号定义的非 Actor Authority Kind。它复用 WorkPlacement/WorkAddress、DueIndex 和
恢复语义，但不取得 Actor Lease、WriterClaim 或 Mailbox。其 Bucket/Hash Tag/Lua 由好友权威物理合同
定义，不能把 Pair ID 强行解释为 PLAYER/FARM Actor ID。

一项可靠业务流程只能有一个全局恢复锚点：

```text
已经可靠创建独立Workflow（Plant / Harvest / Theft）
→ Actor只原子提交本地权威事实
  + 确定性Decision
  + ParticipantReceipt
  + workflow_id引用
→ 不再为同一后续步骤创建来源RewardOrder、第二套Settlement进度或重复DueIndex

没有独立Workflow，且只是来源事实后的单向可靠交付（Care / GuardSync）
→ 来源事实 + Order + DueIndex同槽原子提交
```

如果未来要把 Harvest/Theft 从 Workflow 改为来源 Order，必须同时修改本文、CommandCatalog、内部
WorkContext 和场景合同，不能只在 Store 示例中引入第二套恢复模型。

### 7.8.3 来源权威同槽 Order

`CareRewardOrder`、`GuardSyncOrder`、`FriendTerminationOrder`、`FriendActionDispatchOrder` 和业务必达的
`MailDeliveryOrder`
必须与“产生它的来源权威事实”原子创建，因此采用来源 Authority 的
`AuthorityPersistenceBucket`，而不是 `hash(order_id)`：

```text
# Care：来源是 FarmActor(farm_id)
farm:{auth-farm-v1-<bucket>}:record:<farm_id>
order:{auth-farm-v1-<bucket>}:care:<order_id>
due:{auth-farm-v1-<bucket>}:source-order

# GuardSync：来源是 PlayerActor(player_id)
player:{auth-player-v1-<bucket>}:record:<player_id>
order:{auth-player-v1-<bucket>}:guard:<order_id>
due:{auth-player-v1-<bucket>}:source-order

# FriendTermination：来源是FRIEND_PAIR(pair_id)，不是Actor
friend:{friend-pair-v1-<bucket>}:pair:<pair_id>
order:{friend-pair-v1-<bucket>}:termination:<order_id>
due:{friend-pair-v1-<bucket>}:source-order
```

来源同槽 Order 的 `WorkAddress` 必须携带：

```text
work_id = order_id
work_type = order_type
placement_kind = SOURCE_PERSISTENCE_BUCKET
source_authority {
  authority_kind
  canonical_authority_id
  authority_layout_version
  optional actor_route_domain
}
bucket_version / bucket_id
```

这样 Repository 和 worker 无需查询全局目录，就能重新算出同一个 Slot。Lua 成功后才把
`WorkAddress` 投入 DirectDispatcher。Lua 中至少原子完成：

```text
校验RecordVersion / WriterEpoch / WriterClaimID
→ 写入来源权威状态
→ CreateIfAbsent来源同槽Order/Receipt
→ ZADD DueIndex(next_retry_at, EncodeWorkRefV1(work_type, work_id))
→ 提交新版本
```

独立的 Plant/Harvest/Theft `Workflow` 在任何 Participant 发生权威变化前先
`CreateIfAbsent`，不要求与某个 Actor 来源事实同一次提交，因此可以使用第 15.1 节定义的
独立 `WorkflowBucket = hash(workflow_id)`。Participant 自己的状态与 Receipt 仍必须在各自
AuthorityPersistenceBucket 内原子提交。

禁止把来源权威写在 `auth-*` Slot，却把需要原子创建的 Order 直接写入 `wf-*` Slot，然后仍
宣称一次 Redis Lua 可保证两者原子。若确实需要把 Order 投影到独立 Work Bucket，来源提交中
必须先原子写入 source-local PendingIntent/Outbox，再由可靠恢复通道投影；P0 不采用这条更重路径。

### 7.8.4 两个具体例子

好友 B 给农场 1001 除虫时，假设：

```text
FARM:1001当前执行路由
→ VirtualShard=905
→ Holder=farm-svr-pod-7

FARM:1001在auth-v1的持久化原子域
→ AuthorityPersistenceBucket=905
```

一次 Lua 在 `{auth-farm-v1-905}` 内同时校验 Shard 写权并写入：

```text
ShardWriteAuthority(FARM,905): holder/boot/epoch/lease
FarmRecord(1001): CareIncident ACTIVE→RESOLVED, farm_version++
CareRewardOrder(order-xyz): PENDING
DueIndex(905): next_retry_at→order-xyz
```

之后 Shard 905 从 pod-7 迁到 pod-9，只改变 Holder、Lease 和 WriterEpoch；上述 Redis 数据仍在
Bucket 905，不搬迁、不重算。worker 从 Bucket 905 取出携带来源 `FARM:1001` 的
`WorkAddress(order-xyz)`，
再通过最新 ActorRoute
调用 PlayerActor 给 B 发经验。

UnlockPlot/Plant 则不同：独立 Workflow 必须先于金币/种子 Reservation 创建，所以它可稳定落到
`WorkflowBucket=58`；随后 PlayerActor 的 Reservation/Receipt 写在玩家自己的 Authority Bucket，
FarmActor 的 Plot/CropCycle/Receipt 写在农场自己的 Authority Bucket。Workflow 用 ParticipantReceipt
连接这些提交，不假装它们是一次跨 Slot Redis 事务。

如果未来来源权威迁到 Tcaplus，而 Order 仍留在 Redis，就不能再宣称上述两份记录原子创建；
届时必须采用 Tcaplus 同事务 Outbox、聚合内 PendingIntent + 可靠发现，或其他经过验证的
提交日志方案。

### 7.9 持久化验收测试

1. SoftSave 与 Barrier 并发时低版本不能覆盖高版本。
2. Plant Barrier 包含此前尚未落库的 Buy 结果。
3. CAS 响应丢失后根据 Receipt 返回原结果。
4. 旧 writer_epoch 无法写入。
5. Passivation/Drain 完成 ForceFlush 后才能释放写权。
6. 资产和 Receipt 不会只提交一半。
7. Feature Toggle 关闭 ACTOR_SOFT 时所有写走可靠路径。
8. Redis Lua 中来源状态、来源同槽 Order 和来源 DueIndex 要么全部出现，要么都不出现。
9. Redis 未启用生产持久化时，文档、测试报告和答辩不得把它描述为跨 Redis 灾难可靠。
10. Holder 迁移、VirtualShard Assignment 变化或 Pod 扩缩容不得改变既有记录的 PersistenceBucket。
11. 来源同槽 Order 通过 `SourceAuthorityRef` 可确定性定位；Actor 与非 Actor Authority 使用同一
    地址抽象且不互相伪装；独立 Workflow
    通过版本化 `workflow_id` Bucket 定位，两类 Key 不得混算。
12. 旧 Lua 在 FenceAndClaimActor 之前提交时 Resolver 必须读到原 Receipt；Fence 先提交时旧 Lua
    必须返回 WRITER_CLAIM_MISMATCH。
13. FenceAndClaimActor 自身 TIMEOUT_UNKNOWN 时 Actor 保持隔离，不得重新 ACTIVE。
14. Authority 状态、RecordVersion、业务 Receipt 及必要 Order/Outbox 必须在同一次强写中原子出现。
15. `auth-v1` 中任意 Actor 的 `AuthorityPersistenceBucketID` 必须等于其 `VirtualShardID`；不得再运行
    第二套独立 Bucket Hash。
16. Holder 迁移只推进 Assignment/Lease/WriterEpoch，不改变 `auth-v1` Bucket；HashVersion、HashSeed
    或 ShardCount 变化必须通过新版本和显式数据迁移完成。
17. PLAYER/FARM 即使 VirtualShardID 相同，也必须落入不同的版本化 Hash Tag 命名空间。

### 7.10 Receipt 与结果记录有界合同

ParticipantReceipt、OperationResult、SoftOperationReceipt、TheftAttemptReceipt 和 ResolutionReceipt
都不能永久追加在 Actor 热记录中。每一种记录必须声明：

```text
RetentionClass
max_hot_entries / max_hot_bytes
terminal_retention
compaction_or_archive_rule
expiry_evidence
```

稳定语义：

1. `PENDING/PROCESSING/RESOLVING_UNKNOWN` 不得因数量或 TTL 被静默淘汰；达到容量上限时应拒绝
   新业务或触发背压，而不是删除仍需恢复的锚点。
2. 终态 Receipt 可以在覆盖客户端 Journal/OperationResult 恢复窗口后归档或过期；具体时长由
   第 31/32 号容量合同冻结。
3. Actor 热记录只保留当前业务周期和有界近期摘要。`TheftAttemptReceipt` 至少绑定
   `crop_cycle_id + visitor_id`；CropCycle 结束后按场景合同压缩/归档，不能随历史作物无限增长。
4. `NOT_FOUND` 不能单独证明结果已经过期；必须有 ResultDirectory 水位、过期 Tombstone、
   RetentionEpoch 或等价证据，服从第 28/29 号 OperationResult 合同。
5. 压缩、归档和 TTL 不能破坏“资产变化与对应 Receipt 同提交”的原子边界；只允许在终态提交
   已明确之后进行后台整理。
6. `terminal_retention` 到期只允许整理已经具有终态和过期证据的记录；未决记录超过预期恢复窗口
   时进入 `RECOVERY_OVERDUE`、告警并阻止相关新业务，不能仅因年龄达到阈值而删除或改写成终态。

数值上限属于 `PHYSICAL_PENDING`，但“所有热记录有界、未决记录不被容量淘汰”是稳定语义。

---

## 8. Passivation、Drain 与恢复

### 8.1 普通卸载

PlayerActor 可以在 WSS 仍在线时空闲卸载，因为 SessionAuthority 不依赖 PlayerActor 常驻。

FarmActor 的普通 Passivation 只检查自己的权威执行状态：

```text
Mailbox为空
无未决本地写入
不处于RESOLVING_UNKNOWN/FENCED
空闲达到阈值
```

满足条件后必须调用带 `ActivationGeneration` 的 `ReleaseActivation`，同步释放 Actor、Mailbox、
run loop 和 Registry 索引；只删除业务 Actor 缓存不算完成卸载。

Hub 中是否存在订阅可以作为“保持 FarmActor 热激活”的性能提示，但不能成为 FarmActor 的业务字段。是否因活跃 Hub 暂缓 Passivation 属于可配置驻留策略；即使 FarmActor 已卸载，下一条权威命令也必须能够重新激活并从存储恢复。

`FarmSubscriptionHub` 使用独立的运行时回收规则：没有 Subscription、没有 Joining、没有待发送批次且空闲达到阈值后即可删除。Hub 丢失不影响业务事实，现有客户端通过带抖动的重新 Join 恢复。

### 8.2 Shard Drain

Shard Drain 是 Shard 迁移前的排空阶段：

```text
停止接受新命令
→ 完成已接受Turn
→ PlayerActor ForceFlush
→ FarmActor完成可靠提交
→ 清理/重建Subscription与Presence运行态
→ 释放Lease
```

滚动发布由 CI/CD 和 K8s 发起 Pod 替换，RouteAssignmentManager 决定 Shard Handoff。Gate 通过 Watch 自动更新，不由人工逐台修改。

### 8.3 突然崩溃

```text
旧Lease失效
→ 新Holder取得更高writer_epoch
→ 从持久状态懒加载Actor
→ Workflow/Order由worker恢复
→ Presence通过重新Join重建
```

旧 writer_epoch 的写入必须被存储层拒绝。


### 8.4 Lost Lease、Self-Fence 与旧 Mailbox

Holder 无法证明 Shard Lease 仍处于安全有效窗口时，必须 Fail Closed，对该 Shard 执行
Self-Fence：

```text
ShardRuntime → FENCED
→ RouteIngress拒绝新命令和新激活
→ 现有Activation标记FENCED
→ 停止从UserMailbox取新的Turn
→ 仅允许LeaseLost/PersistenceCompleted/UnknownResolved/Stop等System消息
```

Self-Fence 不等于杀死整个 Pod，也不证明新 Holder 已经接管；它只表示旧 Holder 主动撤销自己
的权威执行资格。网络恢复后不得直接恢复旧 Activation 和旧 Mailbox，必须重新读取 Assignment、
取得有效 Lease、建立新 Generation、从 Repository 重载并执行新的 ActorWriteClaim。

旧 Mailbox 处理矩阵：

| 消息阶段 | 处理 |
|---|---|
| 尚在 UserMailbox、未开始 Turn | 旧 Holder 不执行、不迁移；原调用方按原幂等身份恢复 |
| 已开始但 Persist 尚未发出 | 丢弃 CandidateState，确认没有业务事实成立 |
| Persist 已发出、结果未知 | 进入 RESOLVING_UNKNOWN，通过 FenceAndClaimActor + Receipt 解析 |
| Persist 已成功、响应未返回 | 业务事实保留，后续返回原 Receipt/OperationResult |

旧 Mailbox 不是可靠日志，Shard Handoff 和故障接管不复制 Mailbox。可靠恢复来源只有 Authority
Store、Workflow/Order、Receipt、OperationResult 和稳定幂等 ID。

---

## 9. 跨 Actor 通用合同

### 9.1 三种可靠对象

```text
Workflow
→ 多阶段、可能补偿或存在不可逆提交点

Order
→ 权威提交后的一向可靠后续动作

ParticipantReceipt
→ 参与者内部防止重复执行
```

### 9.2 幂等键

```text
EffectiveIdempotencyKey {
    subject_player_id
    command_type
    operation_id
    OperationRootRef
}
```

同一 Root 内相同 Key、相同 payload_hash 返回原结果；相同 Key、不同 payload_hash 返回
`IDEMPOTENCY_CONFLICT`。Root 由 CommandCatalog 与可信目标派生，客户端不能指定内部 Authority 地址。

### 9.3 客户端结果

```text
SUCCEEDED
→ 承诺的用户可见效果已经达到成功边界

PROCESSING
→ 已有权威变化或结果未知，不能换operation_id重试

REJECTED
→ 操作未发生且需要释放的Reservation已可靠释放

NO_CHANGE
→ 合法请求但没有改变权威状态
```

---

## 9A. UnlockPlotWorkflow

第一跳：farm-svr / UnlockPlotCoordinator。第 32 号拥有等级、价格和固定顺序等场景细节；本文冻结跨
Actor 主链：

```text
CreateIfAbsent UnlockPlotWorkflow
→ PlayerActor.ReserveUnlockCoin
→ FarmActor.CommitPlotUnlock
→ PlayerActor.CommitUnlockCoinReservation
→ COMPLETED
```

状态：

```text
CREATED
├→ PLAYER_REJECTED
│  → COMPLETED(REJECTED)
└→ COIN_RESERVED
   → FARM_COMMITTING
   ├→ FARM_REJECTED
   │  → COIN_RELEASING
   │  → COMPENSATED(REJECTED)
   ├→ FARM_NO_CHANGE
   │  → COIN_RELEASING
   │  → COMPENSATED(NO_CHANGE)
   └→ FARM_COMMITTED
      → COIN_FINALIZING
      → COIN_FINALIZED
      → COMPLETED(SUCCEEDED)
```

Workflow 必须先于金币 Reservation 创建。Farm 提交前明确拒绝时释放 Reservation；Farm 已提交后 Plot
解锁不可回滚，只能继续 Finalize 扣款。Participant Receipt、Unknown、Lease 恢复和同
operation_id/different payload 冲突规则与本章通用 Workflow 合同一致。每一步的 Typed Payload、
Typed Result、InvocationID 和依赖 Result Hash 以第 29 号第 7.7～7.11 节为唯一权威。

`PLAYER_REJECTED` 表示等级或可用金币不足等 PlayerActor 已持久记录的确定拒绝；没有 Reservation，
不得调用 FarmActor。`FARM_REJECTED` 和 `FARM_NO_CHANGE` 都要求先可靠释放已有 Reservation，区别在于
最终 CommandStatus 分别为 REJECTED 和 NO_CHANGE。释放响应未知时继续 PROCESSING，不能提前发布终态。

并发购买相邻土地按 FarmActor 提交顺序线性裁决：Plot 7 在 Plot 6 尚未提交时执行则拒绝；如果 Plot 6
先提交，随后执行的 Plot 7 已成为合法 next plot，且两笔请求均已分别预留足额金币，则允许两笔都成功。
这不会允许跳地，也不要求额外的 unlock sequence token。

---

## 10. PlantWorkflow

第一跳：farm-svr。

```text
CreateIfAbsent PlantWorkflow
→ PlayerActor.ReserveSeed
→ FarmActor.CommitPlant
→ PlayerActor.CommitSeedReservation
→ COMPLETED
```

状态：

```text
CREATED
├→ PLAYER_REJECTED
│  → COMPLETED(REJECTED)
└→ SEED_RESERVED
   → FARM_COMMITTING
   ├→ FARM_REJECTED
   │  → RESERVATION_RELEASING
   │  → COMPENSATED(REJECTED)
   └→ FARM_COMMITTED
      → RESERVATION_FINALIZED
      → COMPLETED(SUCCEEDED)
```

种子从 available 转入 reserved；CommitSeedReservation 只把 Reservation 标记为永久消费，不再扣一次。

FarmActor.CommitPlant 是不可逆提交点。提交前地块版本冲突时释放 Reservation；提交后只能 Finalize，不能退款。

`COMPENSATED` 是 PlantWorkflow 的终止状态，最终 CommandStatus 仍是 FarmActor 已冻结的
`REJECTED`（以及未来合同显式允许时的 `NO_CHANGE`），不是 `SUCCEEDED`。只有确认 Reservation 已释放
后才能发布该终态；释放结果未知时继续保持 `PROCESSING`。

每一步的 Typed Payload、Typed Result、InvocationID 和依赖 Result Hash 以第 29 号第 7.7～7.11 节
为唯一权威；本文不复制第二套内部 IDL。

Reservation TTL 只能触发 Workflow 检查，不能直接自动释放。
如果释放 Reservation 的结果未知，Workflow 保持 `PROCESSING` 并查询 PlayerReceipt，不能把
“释放请求超时”直接当作已释放。

种子不足、种子不可用等 `ReserveSeed` 确定拒绝进入 `PLAYER_REJECTED → COMPLETED(REJECTED)`；此时
没有 SeedReservation，也不得调用 FarmActor。相同 Invocation 重试必须返回原拒绝，不能因为玩家后来
又买到种子而让同一个 operation 突然成功。

---

## 11. HarvestWorkflow

第一跳：farm-svr。

```text
CreateIfAbsent HarvestWorkflow
→ FarmActor.CommitHarvest
→ PlayerActor.GrantHarvestReward
→ COMPLETED
```

状态：

```text
CREATED
→ FARM_COMMITTING
├→ FARM_REJECTED
│  → COMPLETED(REJECTED)
├→ FARM_NO_CHANGE
│  → COMPLETED(NO_CHANGE)
└→ FARM_HARVEST_COMMITTED
   → PLAYER_REWARDING
   → PLAYER_REWARDED
   → COMPLETED(SUCCEEDED)
```

`COMPLETED` 表示 Workflow 已经终止，不等于命令一定成功。WorkRecord 必须同时保存最终
`CommandStatus`、稳定 ErrorCode、Participant Result Hash 和必要结果摘要。`FARM_REJECTED` 与
`FARM_NO_CHANGE` 都是 FarmActor 已明确裁决的终态分支，不再调用 PlayerActor 发奖；只有
`FARM_HARVEST_COMMITTED` 才能进入 `PLAYER_REWARDING`。

FarmActor 按 `trusted_now_ms >= mature_at` 判断成熟，并在提交时冻结：

```text
reward_item_id
reward_amount = base_yield - stolen_amount
```

Farm 提交后不能恢复 CropCycle，只能继续完成玩家到账。Farm 已提交但仓库尚未到账时返回 `PROCESSING：“收获已确认，作物到账中”`。

P0 仓库无容量上限。第一次成功收获自己种植的植物时，PlayerActor 可靠解锁图鉴。

两个 Participant 的 Typed Payload/Result、Decision/Result Hash、InvocationID 与失败恢复，以第 29 号
第 7.12 节为唯一内部 IDL；本文只拥有 Workflow 状态与不可逆点。

---

## 12. TheftWorkflow

第一跳：目标 farm-svr。

前置使用 ActionPermit 校验好友关系；最终由 FarmActor 裁决成熟、周期、偷取上限和每好友每茬一次。

```text
CREATED
→ FARM_DECIDING
├→ FARM_REJECTED
│  → COMPLETED(REJECTED)
├→ FARM_NO_CHANGE
│  → COMPLETED(NO_CHANGE)
└→ FARM_DECIDED
   → VISITOR_SETTLING
   → VISITOR_SETTLED
   ├→ [guard_coin_value == 0] COMPLETED(SUCCEEDED)
   └→ OWNER_SETTLING
      → OWNER_SETTLED
      → COMPLETED(SUCCEEDED)
```

`FARM_REJECTED` 表示请求未形成合法 TheftAttempt；`FARM_NO_CHANGE` 表示 FarmActor 已明确确认
`ALREADY_ATTEMPTED`、`NO_STEALABLE_QUOTA` 等无变化结果。两者都必须保存稳定终态，但不得继续
结算访问者或主人。只有 `FARM_DECIDED` 才表示 gross 扣减、AttemptReceipt、守护结果与 FarmChange
已经一次权威提交，此后 Workflow 只能继续完成访问者和必要的主人结算，不能回滚目标农场。

未成熟、非好友、周期结束等拒绝不消耗机会。合法 Attempt 一旦 Farm 提交，即消耗该好友本茬机会。
具体 20% 总上限、十个 2% 槽位、整数取整和未来递减曲线以第 32 号 Theft 场景为唯一权威；本文不再
保留早期固定产量示例。

### 12.1 宠物结算修正版

产品合同 `PR-THEFT-GUARD-01`：

> 守护宠物从本次 gross 偷取量中截留作物，不扣访问者已有金币；截留作物按 CropCycle
> 冻结单价转换为农场主金币。

宠物不从偷菜者钱包扣金币，而是减少实际入库作物：

```text
gross_stolen_quantity
guard_lost_quantity
net_received_quantity
guard_coin_value
```

满足：

```text
net_received_quantity
= gross_stolen_quantity - guard_lost_quantity

guard_coin_value
= guard_lost_quantity × crop_unit_coin_value
```

被宠物截下的作物仍计入本次从地块结算的 `stolen_amount`，并按本周期冻结单价转换为农场主金币：

```text
地块扣gross数量
→ net数量进入访问者仓库
→ guard_lost数量转换成农场主金币
```

宠物随机结果首次判定后固定，重试不能重新随机。访问者库存和 VisitorReceipt 原子提交；农场主金币和 GuardIncomeReceipt 原子提交。

Farm 已提交后不能返回普通失败，只能继续恢复结算。

三个 Participant 的 Typed Payload/Result、ActionPermit Snapshot、Decision/Result Hash、InvocationID
与完成边界，以第 29 号第 7.13 节为唯一内部 IDL；本文不复制第二套字段定义。

---

## 13. CareRewardOrder 与 DirectDispatcher

有效护理由 FarmActor 原子提交：

```text
CareIncident尚未resolved
+ trusted_now_ms >= due_at_ms
→ 写入resolved_by_player_id / resolved_at_ms
+ CareResolutionSnapshot
+ CareRewardOrder(PENDING)
+ CareOperationReceipt
+ FarmOpportunityHeader / opportunity_dirty_generation++
+ plot_version++
+ farm_version++
+ 构造候选CommittedFarmChange(CareResolved)
```

`ACTIVE` 只是按 `trusted_now_ms >= due_at_ms` 派生出的当前阶段，不存在 Timer 驱动的持久
`PENDING → ACTIVE` 写入。`CareOperationReceipt` 是 Care 业务提交与防重凭证；裸
`ResolutionReceipt` 只用于第 30 号 `FenceAndClaimActor` 的 Unknown 解析，不得复用为护理凭证。

同一 Incident 只有第一个有效处理者获得经验和“好友互动”任务进度。其他请求返回 `NO_CHANGE`。

Care 不需要预创建完整 Workflow。`CareRewardOrder` 同时是玩家奖励进度记录和 worker 可靠发现对象；
当前不额外复制一份内容相同的 PendingOutbox。机会摘要是第 31 号定义的可重建投影，不是该 Order 的
第二个必达业务步骤。

业务状态：

```text
PENDING
→ REWARD_DISPATCHING
→ PLAYER_REWARD_GRANTED
→ COMPLETED
```

### 13.1 DirectDispatcher

farm-svr 内：

```text
有界Go Channel
+ 固定Worker Goroutine
+ ActorRouteClient
+ gRPC
```

内存队列只传递已验证的完整 `WorkAddress`，不携带完整业务状态、不执行 CAS，也不是可靠存储。
紧凑 `WorkRefV1` 只用于已知 Bucket 中的 DueIndex 发现；禁止只传裸 `order_id`，也禁止让
Dispatcher 猜测它位于 Independent Work Bucket 还是来源 Authority Bucket。

正常快速路径不先 Claim；Order 创建时设置短暂 `next_retry_at`，DirectDispatcher 立即尝试。
PlayerActor.ApplyCareReward 必须以 `CareRewardReceipt(order_id)` 防重。P0 即使经验、TaskProgress、Receipt 使用
同一 Hash Tag 下的不同 Key，也必须经过一个 Authority Group、一条 Durable Lane 和一次 Lua/
Repository 原子提交。只有 P1 已正式拆成独立 Authority Group 后，才改为先提交玩家奖励、Receipt
和 `TaskProgressIntent`，再由目标 Group 消费 Intent；不能仅因物理分 Key 就假装拥有独立 Lane。

Participant 成功后再将 Order 标记为 `PLAYER_REWARD_GRANTED → COMPLETED`。如果更新 Order 的响应
丢失，worker 通过 `CareRewardReceipt(order_id)` 解析结果，不重复发奖。

Farm 来源提交同时推进固定大小的 `opportunity_dirty_generation`；每 Farm/ProjectionKind 只维护一条
可合并刷新状态，由 Projection Worker/Reconcile 更新 FarmOpportunitySummary。投影失败不得阻止
CareRewardOrder 完成，也不得反向拒绝新的 Care 权威提交。

### 13.2 farm-worker 恢复

只有到期的未完成 Order 才执行 Lease CAS。Lease 状态回答“当前由谁恢复”，不替代上面的业务状态：

```text
PENDING/REWARD_DISPATCHING
→ Claim恢复Lease
→ ApplyCareReward
→ PLAYER_REWARD_GRANTED
→ COMPLETED
```

失败后按退避时间回到 PENDING。Worker 使用到期索引，不全表不断扫描和 CAS。

当前不使用 MQ。未来只有在多消费者、削峰或平台收益明确时，将传输替换为 MQ；Order 和 Receipt 不变。

Typed Payload/Result、CareResolutionSnapshot、InvocationID、每日任务 Period 和投影解耦，以第 29 号
第 7.14 节为唯一内部 IDL。

### 13.3 Care 的失败与无变化终态

Care 在来源 Farm Authority Commit 之前被明确拒绝，或 Incident 已被其他人解决时，不创建
`CareRewardOrder`：

```text
ACTION_NOT_PERMITTED / CARE_NOT_DUE / STALE_*
→ REJECTED

ALREADY_RESOLVED
→ NO_CHANGE
```

一旦 `Incident=RESOLVED + CareOperationReceipt + CareRewardOrder(PENDING)` 已经同槽原子提交，Care
主操作即为 `SUCCEEDED`；Player 奖励可以是 `PENDING`。DirectDispatcher 响应丢失、进程退出或
Order 状态 CAS 响应丢失，都只能使用同一个 `ParticipantInvocationID` 重试并查询
`CareRewardReceipt(order_id)`，不得重复增加经验或任务进度。机会摘要和 FarmChange 的投递失败
均不回滚 Care：前者由合并刷新/Reconcile 修复，后者令当前 Subscription 进入 Resync/Fresh Join。

Care 的持久 `StoredOperationOutcome` 只保存稳定 `care_reward_order_id` 和
`reward_obligation_created=true`，不保存会从 `PENDING` 变为 `APPLIED` 的投递状态。当前奖励进度通过
Player 私有 Push、Bootstrap 或 Order 查询视图观察；Order 完成不得反向改写原 Care 终态结果。

---

## 14. GuardSyncOrder

PetState 由 PlayerActor 长期权威管理；FarmActor 保存 Theft 裁决所需的版本化 GuardSnapshot。

喂食或切换宠物：

```text
PlayerActor可靠提交：
资产变化
+ PetState
+ PetFeed/Switch Receipt
+ 不可变StoredOperationOutcome
+ GuardSyncOrder(PENDING)
+ DueIndex

DirectDispatcher
→ FarmActor.APPLY_GUARD_SNAPSHOT
→ APPLIED或SUPERSEDED
→ 来源Player Bucket幂等推进GuardSyncWatermark
```

GuardSyncOrder 是单向可靠同步，不执行补偿退款。它属于 `BUSINESS_REQUIRED` 来源同槽 Order：来源
PetState、Receipt、Order 和 DueIndex 必须一次 Authority Commit；Order/DueIndex 容量不足时整条
Feed/Switch 失败关闭，不能先扣狗粮再丢同步义务。

Order 身份复用统一来源 Order 公式：

```text
source_fact_id      = "PET_GUARD_SLOT"
source_fact_version = guard_version

order_id = StableOpaqueID(
  "farm-source-order-id-v1",
  "GUARD_SYNC_ORDER",
  "PLAYER_ACTOR",
  canonical_player_id,
  source_fact_id,
  canonical_guard_version
)
```

FarmActor 只接受更高的 `guard_version`，旧 Order 可以标记为 SUPERSEDED。守护有效性惰性判断：

```text
trusted_now_ms < guard_until
```

Scheduler 未来只负责到期提醒，不负责改变守护权威状态。

历史命令结果与当前同步状态必须分开：

```text
StoredOperationOutcome
→ action_status + 决定时PetStateView + guard_sync_order_id/guard_version
→ 一经提交不可修改

GET_PET_STATE
→ 比较PetState.guard_version与GuardSyncWatermark
→ 动态返回NOT_REQUIRED/APPLIED/PENDING/FAULT
```

`PENDING` 不允许客户端换 operation_id 重新扣道具；由 DirectDispatcher 与 farm-worker继续完成原
GuardSyncOrder。Order 进入通用 GC 后，固定大小 GuardSyncWatermark 仍必须足以回答当前同步状态。

GuardSnapshot 安装可能包含公开外观和私有守护规则两部分。Feed 只改变私有规则/截止时间时仅推进
Farm Actor `record_version`；真正 Switch 改变公开宠物外观时才推进 `farm_version` 并生成一份共享
FarmMetaDelta。自然到期不写状态、不广播。

---

## 15. farm-worker 的最终定位

farm-worker 不是权威 Actor，也不持有玩家和农场最终真值。

它负责：

- 恢复超时 Workflow；
- 扫描到期 Order；
- Claim Lease；
- 通过 ActorRouteClient 调用 Participant；
- 查询 Receipt 解析 Unknown Outcome；
- 重试、退避、对账和最终修复；
- 后续可选的投影、Scheduler 和 MQ Relay。

正常请求优先由请求进程快速推进；worker 只承担可恢复后台执行和异常兜底。

### 15.1 DirectDispatcher 与恢复执行器

```text
DirectDispatcher
→ 进程内有界Channel
→ 固定goroutine worker pool
→ 正常快速gRPC投递
→ 不作为可靠存储

farm-worker
→ 查询next_retry_at到期的Workflow/Order
→ CAS Claim Lease
→ ActorRouteClient定位Participant
→ Receipt解析结果
```

正常 DirectDispatcher 不必为每条任务先 Claim CAS。Order 创建时设置短暂 `next_retry_at`，给本地快速路径留出窗口；只有到期后 farm-worker 才 Claim。

Worker 不允许全表高频扫描，必须提供时间 Bucket 或等价到期索引、批量上限、类型并发上限、指数退避、Lease 过期恢复和 Pending 最老年龄告警。

当前 Redis 方案必须使用固定、版本化、可配置的 2 的幂次 Bucket 分片 DueIndex，禁止
全服 Workflow/Order 共用单一 Hash Tag 或单一 ZSET。但必须区分两类恢复对象。

#### 15.1.1 独立 Workflow Bucket

Plant/Harvest/Theft Workflow 在 Participant 修改权威事实前先独立创建，可以按稳定
`workflow_id` 分桶：

```text
workflow_bucket_version = wf-v1
workflow_bucket_count = 2^N       # 64/256等仅为候选，由压测决定
workflow_bucket = StableHash(workflow_id) & (workflow_bucket_count - 1)

wf:{wf-v1-3a}:record:<work_id>
wf:{wf-v1-3a}:due
```

同一 Workflow Bucket 内 Workflow Record 与 DueIndex 必须位于同一 Redis Cluster Slot，使
创建、推进 `next_retry_at` 和终态删除能在同一 Lua 原子边界完成。

#### 15.1.2 来源权威同槽 Order Bucket

来源同槽 Order 不能再按 `hash(order_id)` 分桶，因为它们必须和来源 Authority 的权威变化原子创建。
它们沿用第 7.8 节的 `AuthorityPersistenceBucket(SourceAuthorityRef)`：

```text
source authority record
+ source-local order record
+ source-local due index
→ 同一个Redis Hash Tag / 一次Lua
```

Worker 对 `WorkflowBucket` 和 `AuthorityPersistenceBucket` 使用两组扫描游标与并发预算；DueIndex
取出的是由“扫描 Bucket 上下文 + 解码后的 `WorkRefV1(work_type, work_id)`”组成的 `WorkRef`，后续
Claim 接口可以复用。`WorkflowBucket` 使用独立、版本化的
`hash(workflow_id)`；Actor 的 `AuthorityPersistenceBucket(auth-v1)` 直接使用来源 Actor 的
`VirtualShardID`，非 Actor Authority 使用各自版本化物理合同派生 Bucket。两类布局一经上线都不得
随 Pod 数量变化；Authority ShardCount/HashVersion/Seed
若需改变，必须新建版本化数据代次并显式迁移，不能让历史记录静默重映射。

Worker 按 Bucket 有界并发扫描、批量取到期 `WorkRef` 后再 Claim；DueIndex 成员本身不保存完整
`WorkAddress` 或 `expected_work_version`。Claim 必须加载 WorkRecord，校验完整 WorkAddress 与扫描
上下文一致，并从当前记录返回 `work_version`。DirectDispatcher 的有界 Channel 不进行 CAS，也不会
循环写 Redis。

### 15.2 RecoverableWorkRepository

```go
type WorkRef struct {
    Bucket   WorkBucketRef // 来自当前扫描上下文，不编码进DueIndex member
    WorkType WorkType
    WorkID   string
}

type RecoverableWorkRepository interface {
    CreateIfAbsent(ctx context.Context, addr WorkAddress, work RecoverableWork) (CreateResult, error)
    Get(ctx context.Context, addr WorkAddress) (RecoverableWork, error)
    CompareAndAdvance(ctx context.Context, addr WorkAddress, expected, next WorkState) error
    ListDue(ctx context.Context, bucket WorkBucketRef, limit int) ([]WorkRef, error)
    Claim(ctx context.Context, ref WorkRef, workerID, workerBootID string,
          requestedTTL time.Duration) (WorkLeaseGrant, error)
    RenewLease(ctx context.Context, ref WorkRef, ownerToken string, leaseEpoch uint64,
               requestedTTL time.Duration) (WorkLeaseGrant, error)
}

type WorkLeaseGrant struct {
    Address       WorkAddress
    OwnerToken    string
    LeaseEpoch    uint64
    WorkVersion   uint64
    AcceptedTTL   time.Duration
}
```

`ListDue` 只解码 `WorkRefV1` 并附加传入的 `WorkBucketRef`；`Claim` 加载记录后才返回经过验证的完整
`WorkAddress` 和当前 `WorkVersion`。所有方法首先按 `placement_kind + bucket_version` 重算并验证
Bucket。Workflow/Order 状态只能单调
推进。Claim/Renew 的权威存储使用自身可信时间计算内部 `expire_at`，Worker 不能传入绝对
`leaseUntil`。Worker 根据请求开始时的本地单调时钟和 `AcceptedTTL` 建立保守 Deadline；超过
Deadline 后不再发起新 Step。Lease 只减少重复调度，最终业务防重仍依赖 Participant Receipt。

---

## 16. 稳定语义决策清单（候选版）

以下决策是实现不得自行改变的稳定语义；本文仍是 freeze-candidate。Shard 数量、Redis Key、
Mailbox 水位、TTL 和迁移参数需经后续实现与压测后才能成为正式物理合同。

1. SessionPermit数据来源、Subscription与JoiningBuffer以第26号文档为唯一权威；
   TrustedCommandContext和ActorRouteHeader IDL以第29号文档为唯一权威。
2. Gate 注入的 TrustedCommandContext 决定操作者身份和ADMITTED时刻的连接/Subscription Fence；
   客户端目标 ID 一律不可信。
3. 本文只冻结命令到领域、权威和Workflow的语义矩阵；CommandCatalog Schema和完整CommandSpec
   以第28号文档为唯一权威。Gate不推进Participant。
4. PlayerActor/FarmActor 使用独立 Route Domain。
5. ActorKey 稳定映射 VirtualShard，本地 Snapshot 直连 Holder。
6. RouteManager 不进入请求热路径；Proxyless 仅为可插拔实现。
7. 同 FarmActor 所有写操作串行，不提前拆 PlotActor。
8. Actor 懒激活、单次 LoadAndClaim、有界 UserMailbox、保留 SystemQueue、单消费者 Turn；每个
   可写 Activation 具有持久 WriterClaimID。
9. Actor Turn 可以等待本 Actor Repository 的有界提交，但不等待另一个 Actor、跨领域 RPC、Gate 下行或长任务。
10. Farm 共享事实可靠提交；Player SoftSave 只能按低风险纯本地命令白名单开启，不能按资产字段自动推导。
11. P0 每个 PlayerActor/FarmActor 只有一个主要 AuthorityPersistenceGroup、一个 Actor 级
    WriterClaim、一个 Group RecordVersion/CAS 边界和一条 Durable Persistence Lane；允许同槽
    多 Key，但不允许按物理 Key 自行建立独立强写 Lane。多 Group/多 Lane 属于 P1 证据驱动演进。
12. Passivation、Shard Drain、writer_epoch、writer_claim_id 和 Redirect 共同支持发布与恢复；
    DRAINING/ASSIGNING属于控制面/运行时Handoff状态，不扩展Redis数据面UNASSIGNED/ACTIVE写权状态。
13. Plant 使用 Reservation 和可补偿 Workflow。
14. Harvest 使用独立 HarvestWorkflow；FarmActor 只提交本地 HarvestDecision 与 ParticipantReceipt，
    不创建重复 HarvestRewardOrder。
15. Theft 使用独立 TheftWorkflow；FarmActor 只提交本地 TheftTargetDecision、AttemptReceipt 与
    ParticipantReceipt，不保存跨 Participant 的完整 Settlement 进度或重复奖励 Order。
16. 宠物截下作物，不扣偷菜者钱包。
17. Care 使用 CareRewardOrder，不创建重复 Outbox。
18. DirectDispatcher 是内存快速通道，farm-worker 是可靠恢复通道。
19. Guard 状态采用 Player 权威、Farm 版本化快照和 BUSINESS_REQUIRED GuardSyncOrder；PetState 使用
    独立 `pet_state_version`，GuardSyncOrder GC 后由 PLAYER Authority Bucket 中、不内嵌 PetState 的
    固定大小 GuardSyncWatermark 提供动态同步水位。
20. `PROCESSING` 必须保留原 operation_id 查询，不能创建新命令重试。
21. P0 暂用公司 Redis 保存权威快照、Workflow/Order、Receipt 和 DueIndex；若 Redis 未启用生产持久化，只承诺 Redis 存活时可恢复。
22. 切换农场遵循第 26 号文档的 break-before-make：同一连接只有一个当前 Subscription，旧订阅的新命令返回 `STALE_SUBSCRIPTION`。
23. ActorRouteClient热路径禁止per-actor Assignment缓存，只允许不可变Shard快照和Holder连接池。
24. Passivation必须以ActivationGeneration释放Actor、Mailbox和run loop，旧回调不能删除新Activation。
25. TIMEOUT_UNKNOWN必须进入RESOLVING_UNKNOWN并执行同存储域FenceAndClaimActor；Fence自身结果
    未知时继续隔离。驱逐、重载或固定Grace Period均不能替代持久WriterClaim Fence。
26. Disconnect/切换清理按farm_id+subscription_id+完整Fence精确路由，禁止扫描全部FarmActor。
27. 独立Workflow按workflow_id进入版本化WorkflowBucket；来源同槽Order按来源Authority进入
    AuthorityPersistenceBucket。WorkRecord、DirectDispatcher与Participant RPC携带包含
    SourceAuthorityRef的完整WorkAddress；
    DueIndex只保存WorkRefV1(work_type, work_id)，由已知Bucket上下文和WorkRecord恢复、复核完整地址。
    两类Bucket都禁止全服单Slot/单ZSET，也不得跟随Holder或Pod变化重映射。
28. Lost Lease时旧Holder必须Self-Fence，旧Mailbox不迁移，旧writer_epoch写由存储最终拒绝。
29. Join只有完成ArmSnapshot、Snapshot/Catch-up入同一连接FIFO并发送SUBSCRIPTION_READY后才可激活；
    本文不另行定义第26号文档的Join状态机。
30. 业务状态、RecordVersion、WriterEpoch、WriterClaimID、Receipt 及必要Order/Outbox必须按命令
    合同原子提交；迟到旧Claim写必须由存储返回WRITER_CLAIM_MISMATCH。
31. 未Arm的Join不生成Snapshot或捕获变化；Arm后的Snapshot(V)与V+1捕获必须和FarmChange发布共用
    一个可证明的线性化顺序。
32. ActiveSubscription/FarmAdmissionContext只证明当前观看和路由上下文；好友关系、VisitPermit、
    ActionPermit与FarmActor最终状态校验仍需逐层成立。删除/拉黑先提交则不得再签发新Permit；已持有
    绑定Permit且先越过Execution-Accept的operation允许按原幂等身份完成。旧页面、旧Subscription和
    旧Permit不能绕过该线性化规则。
33. P0独立WorkflowID由Root作用域EffectiveIdempotencyKey使用farm-workflow-id-v2确定性派生；
    workflow_schema_version不进入ID输入。相同EffectiveIdempotencyKey必须命中同一WorkAddress，禁止随机
    W1/W2分别CreateIfAbsent。
34. PostCommitDeliveryGuard必须在Authority强提交前安装；提交明确失败时清除，TIMEOUT_UNKNOWN时
    保留到结果解析，确认提交后必须在“携带当前delivery_generation入CommittedChangeQueue”与
    “设置独立、可重复观察的FarmRealtimeState.invalid_from_version”之间恰好选择一种结果。
    失效通过保留Control Lane/轮询令现有Subscription进入RESYNC_REQUIRED，不得依赖原失败队列
    再投一次性Resync事件；新基线不得消费旧delivery_generation条目。
35. RouteSnapshot必须权威携带HashVersion与HashSeed并原子安装；调用方不得从本地配置补Seed。
36. Shard Lease由Authority可信时间裁决并返回accepted TTL；Holder以本地单调Deadline热路径
    Fail Closed，不逐请求查询Lease Authority，存储强写继续由writer_epoch+writer_claim_id最终Fence。
37. Work Lease Claim/Renew只接受requested TTL，由Repository可信时间计算内部到期并返回
    accepted TTL；Worker不得提交本机绝对leaseUntil。
38. Receipt、OperationResult和AttemptReceipt热记录必须有界；未决/Unknown恢复锚点不得因TTL或
    容量静默淘汰，超过恢复窗口进入RECOVERY_OVERDUE而不是自动变成终态；终态整理必须保留
    可证明的过期证据。
39. VirtualShard 与 AuthorityPersistenceBucket 语义分离；P0 `auth-v1` 冻结为同 RouteDomain 内
    `AuthorityPersistenceBucketID = VirtualShardID`。Holder 迁移不搬数据；更改 Shard
    HashVersion/HashSeed/ShardCount 必须发布新持久化版本并显式迁移。
40. 系统不保证同一逻辑Actor全局物理实例唯一；迁移、暂停、分区和恢复时允许短暂多实例。系统保证
    同一Actor Authority只有匹配当前ShardWriteAuthority、WriterEpoch、WriterClaimID、
    RecordVersion和Idempotency Receipt的强写能够原子提交。
41. 持久writer_claim_id不是Actor独立迁移的专属机制；钝化重激活、同Holder Activation换代、
    TIMEOUT_UNKNOWN解析和迟到持久化请求均要求Actor级Claim Fence。
42. RouteSnapshot.GlobalRevision只表示整份快照发布版本；ShardRoute.ShardRevision必须等于对应
    ShardWriteAuthority.assignment_revision，并映射到ActorRouteHeader.shard_revision；Holder、Boot、
    ShardRevision与ExpectedWriterEpoch必须来自同一权威记录，不得跨Revision拼接。
43. Workflow的终止状态与最终CommandStatus正交；`COMPLETED`必须保留原SUCCEEDED、REJECTED或
    NO_CHANGE及稳定ErrorCode，GetOperationResult不得把所有终止Work解释为成功。
44. Harvest/Theft的Farm Participant必须显式区分COMMITTED、REJECTED和NO_CHANGE；后两者直接进入
    Work终态且不调用后续Player Participant。只有COMMITTED跨越不可逆点。
45. Authority不可逆事实提交后，Participant、投影或下行失败只能恢复，不能回滚；Authority结果未知
    时则按第30号隔离与Fence解析，不能提前选择成功、拒绝、无变化或补偿分支。
46. Plant/Harvest/Care/Theft的并发、崩溃、重复请求、Unknown与统一Failpoint验收矩阵，以第32号
    Q32-13为唯一场景权威；第29号拥有相应内部调用与测试接缝。
47. Buy/Sell 的“单物品”表示每条命令只有一个 offer/item_definition_id，但 quantity 可以是配置上限内
    的任意正整数；一次买卖多个同类种子/作物属于 P0，一条命令混合多个物品类型不属于 P0。
48. Buy/Sell 在 Execution-Accept 后形成的确定业务拒绝也必须保存 Receipt/StoredOperationOutcome；
    结构非法和接纳前限流拒绝不进入 Authority Store。
49. UnlockPlot/Plant 的首次 Player Reservation 可以明确拒绝并直接终止 Work；不得把不存在的
    Reservation送去Release，也不得继续调用FarmActor。
50. UnlockPlot 的 FARM_REJECTED 与 FARM_NO_CHANGE 都先释放金币 Reservation 再发布对应终态；相邻地块
    并发请求按FarmActor提交顺序判断，只要提交时仍满足固定前置顺序即可依次成功。
51. 好友Care/Theft必须逐operation执行CommitFriendActionAdmission；Pair Authority原子提交Receipt、
    Permit、DispatchOrder和DueIndex才跨越Execution-Accept。删除先提交则新Admission拒绝，Admission
    先提交则该operation可恢复完成；VisitPermit仍服从第26/33号只读Join准入与可靠撤销语义。
52. TaskDomain继续属于PlayerActor。GET_TASK_LIST、CLAIM_TASK和CONFIRM_FRIEND_VISIT均进入PLAYER
    Route Domain；好友访问确认只消费Gate从ActiveSubscription注入的可信证明，不创建FriendVisit
    Work、TaskActor、task-svr或任务专用MQ/Worker。
53. PlayerMailDomain与PlayerActor共部署但不是Actor子状态；邮件读/已读/删除走Service LB，附件领取
    按PLAYER Affinity先到MailClaimIngress再进入Actor。业务必达MailDeliveryOrder服从来源同槽恢复；
    MailClaimProjection属于可重建展示，Order/Push失败不得阻塞或回滚资产领取。
54. PET_FEED/PET_SWITCH 的历史 StoredOperationOutcome 只保存决定时结果与 GuardSyncOrder 引用，不保存
    会变化的 APPLIED/PENDING；动态同步状态由 PetState.guard_version 与 GuardSyncWatermark 比较得到。
    GuardSync 的 Participant 为 APPLY_GUARD_SNAPSHOT；Feed 不产生公开 FarmChange，真正 Switch 至多产生
    一份共享 FarmMetaDelta。

---

## 17. 下一阶段

完成本文后，下一阶段不再重新讨论接入与 Actor 主干，而应继续：

1. 以第 28/29 号现有公网协议和内部 RPC 合同实现通用路由/Runtime 接缝，不再从本文生成另一套 IDL。
2. WriterEpoch、WriterClaimID、FenceAndClaimActor、Persistence Lane、Redis Key、Cluster Slot、Lua
   与物理 Bucket 布局已由第 30/31 号冻结；下一步按其合同实现 Repository 并完成平台故障探针。
3. 按第 32 号 Q32-13 和第 29 号统一 Failpoint 接缝，实现 Plant、Harvest、Theft、Care 的失败注入测试。
4. 通过单 Pod 实现验证 ActorRegistry、Mailbox、Receipt 和 Workflow。
5. 再进行多 Pod Assignment、Drain、Redirect 和容量压测。
6. 在生产容量验收前，验证持久化 Redis 或 Tcaplus 目标方案，再决定是否替换 Repository。

---

## 18. 建议的代码模块布局

逻辑布局如下；Agent 可以按现有仓库包名适配，但不得跨越职责边界：

```text
internal/
├── gateway/
│   ├── protocol/             # FarmFrame、Envelope、Proto Guard
│   ├── session/              # Ticket、SessionPermit、按26来源构造29号TrustedCommandContext
│   ├── commandcatalog/       # command_type → decoder/首跳
│   ├── connection/           # Registry、Writer、下行背压
│   └── routing/              # ActorRouteClient调用方
│
├── routing/
│   ├── address.go            # RouteDomain、ActorAddress
│   ├── hash.go               # 版本化ShardCalculator，物理算法待探针冻结
│   ├── snapshot.go           # 不可变Assignment
│   ├── provider_static.go
│   ├── provider_discovery.go
│   ├── client.go             # Resolve/Invoke/Redirect
│   └── ingress.go            # Holder校验
│
├── actor/
│   ├── registry.go
│   ├── activation.go
│   ├── user_mailbox.go
│   ├── system_queue.go
│   ├── turn.go
│   ├── passivation.go
│   └── supervision.go
│
├── player/
│   ├── actor.go
│   ├── economy/
│   ├── inventory/
│   ├── pet/
│   ├── task/
│   ├── compendium/
│   └── persistence/          # P0单Authority Group/单Durable Lane；内部可管理同槽多Key
│
├── farm/
│   ├── actor.go
│   ├── plot/
│   ├── crop/
│   ├── care/
│   ├── theft/
│   ├── guard/
│   ├── subscriptionhub/      # 非Actor的运行时订阅和广播
│   └── readpath/
│
├── workflow/
│   ├── unlockplot/
│   ├── plant/
│   ├── harvest/
│   ├── theft/
│   ├── order/
│   ├── receipt/
│   └── repository/           # WorkAddress、Redis P0适配与可替换接口
│
└── worker/
    ├── dispatcher/
    ├── recovery/
    ├── lease/
    └── reconcile/
```

FriendshipDomain、PlayerMailDomain 和 ReadAPI 可以在 player-svr 共部署，但不放入 `player/actor.go` 的持久聚合；FarmSubscriptionHub 与 FarmActor 同 Holder，但不放入 Actor Mailbox。

## 19. 实现 Agent 的施工顺序

本节只表达 Actor Route/Runtime/Workflow 模块内部的依赖顺序，不是整个项目的全局施工优先级。
跨文档实施时必须先满足 26～29 的合并阻塞项，尤其 Join Ready/Cancel、Unknown Outcome、精确
Cleanup、Participant Ingress 和 Downlink 隔离；不得因为本节先列 RouteCalculator，就跳过这些
端到端正确性合同。

### Phase A：纯内存骨架

1. RouteDomain、ActorAddress、版本化 ShardCalculator 和 Golden Samples。
2. Static Assignment Snapshot。
3. ActorRouteClient.Resolve。
4. RouteIngress 校验的 Fake Lease/Epoch。
5. ActorRegistry、Singleflight Load、有界 UserMailbox 与 SystemQueue。
6. 同 Actor 串行、不同 Actor 并行测试。

### Phase B：单 Pod Redis Actor

1. ActorStore 接口和内存 Fake。
2. 公司 Redis Adapter、RecordVersion、WriterEpoch、WriterClaimID、Lua CAS 结果分类。
3. PlayerActor/FarmActor 最小状态。
4. FarmSubscriptionHub 最小 Join/Snapshot/Delta 骨架。
5. Passivation 与重新加载。
6. LoadAndClaim、FenceAndClaimActor 与迟到 Lua 的 Unknown Outcome 故障注入。

### Phase C：单 Actor 业务闭环

```text
Buy / Sell
→ 基础PlayerActor资产闭环
→ FarmActor空地、时间派生成熟等基础裁决
```

初期全部 `CRASH_DURABLE`，不提前开启 SoftSave。

### Phase D：跨 Actor 执行

按业务主循环和依赖递增：

1. UnlockPlotWorkflow；
2. PlantWorkflow；
3. HarvestWorkflow；
4. CareRewardOrder；
5. TheftWorkflow；
6. GuardSyncOrder 与宠物截获结算。

每新增一种 Workflow/Order，必须同时提交：

- 状态转换测试；
- 每一步重复调用测试；
- 每一步前后进程退出测试；
- RPC 响应丢失测试；
- worker 接管测试；
- OperationResult 测试。
- WorkPlacementKind/WorkAddress 与 DueIndex 同槽验证。

### Phase E：多 Pod 路由

1. DiscoveryAssignmentProvider。
2. Snapshot Watch 和 Revision Gap。
3. Holder Lease、WriterEpoch 与 WriterClaimID。
4. Redirect。
5. Shard Drain/Handoff。
6. Hub 丢失与重新 Join。
7. 滚动发布和 Pod 崩溃演练。

### Phase F：性能优化

只有压测证据出现后才启用：

- ACTOR_SOFT；
- 批量 SoftSave；
- 更高 Dispatcher 并发；
- MQ；
- UC Proxyless Provider；
- Shard 再平衡策略优化。

## 20. 交付完成定义

后续 Agent 声称完成本文实现前，至少应能自动化证明：

1. H5 登录、ClientHello、JoinOwnFarm 正常。
2. 从农场 A 切到 B 后，A 的新命令携带旧 Subscription 时返回 `STALE_SUBSCRIPTION`，已 ADMITTED 的 A 命令结果仍可解释。
3. 命令经 CommandCatalog、RouteClient、RouteIngress 进入唯一 Actor。
4. 同农场并发操作得到单调 FarmVersion。
5. FarmSubscriptionHub 不拥有业务裁决，丢失后可重新 Join。
6. Actor 卸载后可以从 Redis 当前状态恢复；Redis 整体丢失的未覆盖风险在报告中明确标注。
7. UserMailbox 满时，SystemQueue 中的 LeaseLost/Drain/PersistenceCompleted 仍可执行。
8. Plant 最终只可能“种植成功且消费一颗种子”或“失败且种子已释放”。
9. Harvest 的 Farm 提交后，奖励最终必达且不重复。
10. Theft 每好友每茬最多一次，宠物截下作物并给农场主折价金币。
11. CareReward 快速投递丢失后由 worker 补发。
12. 旧 WriterEpoch 或旧 WriterClaimID 无法写入；Unknown Resolver Fence 前后两种顺序均可解释。
13. Assignment 更新、Redirect 和 Drain 不产生重复业务结果。
14. 200 会话下慢客户端只影响自身或触发 Resync，不反压 FarmActor。
15. 独立 Workflow 与来源同槽 Order 均能由 WorkAddress 定位，Dispatcher/worker 不猜测 Bucket。

未提供上述测试和故障注入证据时，不能仅凭页面演示认定 Actor、路由和 Workflow 已完成。
