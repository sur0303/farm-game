# 28 · CommandCatalog 与公网最小协议合同

版本：`v2.8-bootstrap-friend-opportunity-command-freeze-candidate`  
日期：2026-08-03  
状态：在 `v1.2` 的核心命令矩阵、`FarmDeltaBatchPush` 与 Journal 有界合同基础上，补齐
每条命令的显式 RouteKeyExtractor、`RejoinRequiredPush` 最小 IDL 和 Journal 恢复逾期语义；
继续保留内部 `RESYNC_REQUIRED` 到公网 `REJOIN_REQUIRED` 的固定映射、
`FARM_INTERACTIVE` Ready 屏障、唯一 Join 下行
路径、严格 sequence、最小 CommandResult 和 PushSpec；本次进一步冻结终态结果持久化策略、核心命令
覆盖 P0a～P0d 当前已冻结范围，以及正式 `.proto` 生成前的唯一字段号闸门；本次按第 32 号 S03A/S04
冻结结果，为 Plant 增加向后兼容的作物规则期望版本，并正式注册 UnlockPlot。最终 Proto field number、精确字节上限与
滚动兼容窗口仍待探针和实现验证。本版进一步与第 32 号统一错误矩阵对齐，补充
`completed_retry_directive` 的服务端重建合同。本版继续冻结 `FARM_CHANGE` 的公网载体、实时/追赶
应用差异、严格版本检查和不可静默丢弃规则；`FarmChange/PlotDelta` 的业务字段由第 32 号文档唯一
定义。本版按第 33 号 Q33-13 将好友 Care/Theft 的接受边界收敛到持久
`FRIEND_ACTION_ADMISSION_COMMITTED`；Owner Care 仍由 FarmActor Mailbox 接受。本版继续按第 34 号
任务合同正式注册 `GET_TASK_LIST`、`CLAIM_TASK` 和 `CONFIRM_FRIEND_VISIT`，冻结 Ready 后好友访问
可靠确认、领奖 Payload 分层与任务错误恢复语义。本版再按第 35 号正式注册五类邮件命令；
`operation_id` 只存在于公共 Envelope，附件领取由 PLAYER Holder 内的 MailClaimIngress 预处理后进入
PlayerActor，邮件列表/详情/已读/删除仍走无状态服务路由。
本版按第 36/37 号正式注册宠物与图鉴的最小公网命令：宠物状态查询、金币解锁、喂狗粮、切换守护宠物、
图鉴查询和图鉴 observed-revision 已读 Ack；首次自种收获的图鉴解锁反馈进入 `HarvestResult`。宠物和图鉴
不新增 Actor、独立服务或查询投影，均按 Player Route Domain 进入 PlayerActor/ReadIngress。
图鉴业务身份统一为 `crop_definition_id`；HarvestResult 保存原 operation 的解锁 Delta，不使用含义不明的
plant/display/activity ID，也不在恢复时按当前集合重算历史结果。
本版再按第 36 号区分宠物历史操作结果与当前 GuardSync 水位：公网写结果携带不可变 PetStateView 与
GuardSyncOrder 引用，不携带会变化的 APPLIED/PENDING；`GET_PET_STATE` 才返回当前动态同步状态。
本版进一步冻结 `OperationRootRef + EffectiveIdempotencyKey`：弱网结果查询通过原业务目标
或服务端签发的不透明 Locator 重新派生 Root，客户端不得直接指定内部 Authority、
Bucket 或 WorkAddress；重新加好友的 Share Rotate 显式携带 peer，但 Barrier 证明仍由服务端构造。
本版再补齐第 26/31/33 号已经依赖但尚未登记的两条首屏查询：
`GET_PLAYER_BOOTSTRAP` 与 `GET_NEXT_FRIEND_OPPORTUNITY`。二者均为可安全重查、无长期
OperationResult 的显式 CommandSpec；Bootstrap 独立于 FarmSnapshot，好友机会依赖失败不得伪装为 NONE。

本文成熟度分为三层：带 `L2-PROTO-*` 编号的语义属于 `STABLE_SEMANTIC`；整份协议仍是
`CANDIDATE`；Proto field number、`reserved`、Frame/Payload 字节上限和滚动兼容窗口属于
`PHYSICAL_PENDING`。实现可以补齐物理参数，但不得借此改变已编号语义。

> [26-access-session-contract.md](./26-access-session-contract.md) 是 Session、Join、
> Subscription 和实时下行语义的上位权威；[27-access-routing-actor-workflow-freeze.md](./27-access-routing-actor-workflow-freeze.md)
> 是命令到领域/权威的语义矩阵、Actor Route 和 Workflow 语义的上位权威；
> [29-internal-rpc-trusted-context.md](./29-internal-rpc-trusted-context.md) 是内部可信上下文与 RouteHeader
> IDL 的唯一权威。本文独占公网协议、CommandCatalog Schema 和完整 CommandSpec，不得反向修改
> 26/27 的业务语义，也不得复制 29 的内部结构。旧 Proto 和旧设计文档只作为迁移
> 输入，与 26/27/28 冲突时不得作为实现依据。
> [34-task-system-contract.md](./34-task-system-contract.md) 是任务周期、进度、领奖和好友访问任务成功点的
> 业务语义权威；本文只拥有这些任务命令的公网表达与 CommandSpec。

---

## 0. 一页结论

公网 WSS 使用混合协议：

```text
稳定、数量少的消息大类
→ FarmFrame顶层oneof

数量多、持续扩展的业务命令
→ command_type + bytes payload
→ CommandCatalog按类型解码和路由
```

稳定语义结论（仍待实现验证）：

1. 一个 WebSocket Binary Message 只承载一个 `FarmFrame`。
2. `FarmFrame` 顶层使用 oneof 区分 ClientHello、ServerHello、ClientCommand、
   CommandResult、ServerPush 和 ControlFrame。
3. 连接级 `sequence` 和 `protocol_version` 属于 FarmFrame/握手层，不重复进入业务命令信封。
4. `ClientCommandEnvelope` 只包含 `request_id`、`operation_id`、`command_type` 和
   `bytes payload`。
5. 操作者 `player_id`、Session Fence、Subscription、Actor Route 和 WriterEpoch 不允许由
   客户端放入命令信封；Session 来源服从第 26 号文档，内部 `TrustedCommandContext` 与
   `ActorRouteHeader` 的具体结构服从第 29 号文档。
6. `target_farm_id`、地块、作物周期和 expected version 进入各自 Typed Payload，不放公共
   Envelope。
7. CommandCatalog 是服务端静态命令说明书，负责解码、大小、准入、第一跳、RouteKey、
   幂等、重试、超时、限流与命令可用范围。
8. 数值实体 ID 与单调版本在服务端使用 `uint64/uint32`，H5 以十进制字符串或安全 Long 表示；
   `request_id/operation_id/subscription_id/join_attempt_id/workflow_id/writer_claim_id` 等不透明
   身份与 Fence 使用有界 string/bytes，不能被当成数值比较。
9. `CommandResult` 使用 `ACCEPTED / SUCCEEDED / PROCESSING / REJECTED / NO_CHANGE`
   表达业务所处阶段，并通过 `RetryDirective` 明确客户端下一步动作。
10. `ServerPush` 使用 `SESSION / PLAYER / FARM_SUBSCRIPTION` 三种 Scope；只有农场订阅级
    Push 受当前 `subscription_id` Fence 约束。
11. Gate 对业务 Payload 解码一次并保留原始 bytes；内部 RPC 继续携带原始 bytes，FirstHop
    再独立解码、校验并生成唯一权威 Canonical Payload Hash；Gate 不重新编码 Typed Payload，
    也不作为幂等 Hash 权威。
12. P0 冻结 `own_farm_id == player_id`，但 PLAYER 与 FARM 仍是独立路由域、独立
    VirtualShard 空间和独立 Actor 权威。
13. 可恢复写命令通过 `original_command_type + original_operation_id` 查询 OperationResult；
    `NOT_FOUND` 只能使用原 operation_id 重发，不能创建新业务意图。
14. `JOIN_FARM` 的 `ACCEPTED` 只通过 `JoinResultContext` 携带 Join 元信息，不得携带
    FarmSnapshot；匹配 `join_attempt_id + subscription_id` 的 `FARM_SNAPSHOT` ServerPush 是
    唯一 Snapshot 路径。
15. `join_attempt_id` 由 Gate 生成并绑定本次 Join；客户端不能生成或复用，FirstHop/Hub/Downlink
    回调都必须携带并校验。
16. H5 对可恢复写命令必须在发送前持久化原 Typed Payload；只保存 operation_id 不满足
    NOT_FOUND 后安全重放要求。
17. H5 和 Gate 都必须严格校验本方向 FarmFrame.sequence 为 `last + 1`；任何跳号进入重连或
    Fresh Join 恢复，不能只检查回退。
18. 普通农场 CommandResult 只返回最小业务结果和 committed version；完整 FarmSnapshot、
    Plot Patch 和同场状态只能通过 FARM_SUBSCRIPTION Push 下发。
19. H5 应用 Snapshot 后仍处于 `CATCHING_UP`；只有收到匹配的
    `SUBSCRIPTION_READY(ready_through_version)`、本地连续应用版本等于该值，且 Recovery/Bootstrap
    屏障完成后才能进入 `FARM_INTERACTIVE`。
20. 公网下行必须稳定保持 `CommandResult(ACCEPTED) → FARM_SNAPSHOT → FARM_DELTA_BATCH* →
    SUBSCRIPTION_READY`；具体内部握手由第 29 号合同保证。
21. CommandCatalog 的具体 Schema 与每个公开 CommandType 的完整 CommandSpec 只由本文定义；
    第 27 号文档只拥有命令到领域、权威和 Workflow 的业务语义矩阵。
22. 内部 Hub/Gate 状态统一使用 `RESYNC_REQUIRED`，公网 PushType 只使用
    `REJOIN_REQUIRED`，H5 收到后执行 Fresh Join。
23. H5 Operation Journal 必须同时受 entry 数、单条字节、总字节和保留窗口约束；未决写命令
    不得被静默淘汰，容量不足时 Fail Closed 并停止接收新的可恢复写命令。
24. Join 追赶与 Ready 后实时同步复用第 32 号唯一 `FarmChange` 结构；追赶只恢复状态，实时变化才
    播放 Detail。`FARM_CHANGE` 严格要求本地版本 `+1`，不得静默丢弃、跨版本合并或携带 viewer
    个性化字段。
25. 任务命令正式注册为 `GET_TASK_LIST / CLAIM_TASK / CONFIRM_FRIEND_VISIT`；完整列表读取
 PlayerActor 权威，领奖 operation_id 只在公共 Envelope，好友访问证明只来自 Gate 的当前
 ActiveSubscription。
26. 宠物命令正式注册为 `GET_PET_STATE / PET_UNLOCK_BY_COIN / PET_FEED / PET_SWITCH`；
    `PET_UNLOCK_BY_TASK` 不作为公网命令，任务 `RewardBundle` 直接在 PlayerActor 内完成解锁。
27. 图鉴命令正式注册为 `GET_COMPENDIUM`；首次成功收获自己种植的作物所产生的解锁反馈只通过
    `HarvestResult` 返回，不能由客户端自行声明解锁。
28. 图鉴已读正式注册为 `ACK_COMPENDIUM_SEEN`；它只携带客户端已应用的 `observed_revision`，
    不建立长期 Operation Journal，且不能把 seen_revision 推进到未观察的新解锁版本。
29. 玩家私有首屏状态正式注册为 `GET_PLAYER_BOOTSTRAP`；它按 PLAYER Route Domain 进入
    PlayerBootstrapIngress，不要求 Recovery 完成或当前 FarmSubscription，也不得嵌入 Join/FarmSnapshot。
30. 连续好友机会正式注册为 `GET_NEXT_FRIEND_OPPORTUNITY`；它按 SERVICE_LB 进入
    FriendOpportunityNavigation，`FOUND/NONE` 必须由服务端明确计算，依赖失败返回可重试的暂时不可用。

---

## 1. 协议分层

```text
WebSocket Binary Message
└── FarmFrame                    # 传输和消息大类
    └── ClientCommandEnvelope    # 客户端业务意图
        └── Typed Payload        # Plant/Harvest/Care/Theft等具体Proto

Gate完成ADMITTED后
└── ExecuteCommandRequest        # 内部RPC
    ├── request_id / operation_id / command_type
    ├── raw_payload              # 保留公网原始Typed Payload bytes
    ├── TrustedCommandContext    # 来源语义见26；内部IDL见29
    ├── DispatchContext          # 第27/29号路由合同
    ├── Deadline
    └── Trace
```

上述只是逻辑字段，不表示内部 RPC 再嵌套一份公网 `ClientCommandEnvelope`。内部结构以第 29 号
合同为唯一权威；28 只说明公网字段如何映射进去，避免实现出“嵌套 Envelope”和“字段平铺”两套接口。

各层职责：

| 层 | 负责 | 不负责 |
|---|---|---|
| FarmFrame | 协议版本、连接方向序号、消息大类 | 业务路由和权限 |
| ClientCommandEnvelope | 请求标识、业务幂等标识、命令类型 | 操作者身份和 Session Fence |
| Typed Payload | 目标业务对象、业务参数、expected version | 证明调用者有权限 |
| TrustedCommandContext | 操作者、连接、当前农场和 Subscription Fence | 业务目标和地块规则 |
| CommandCatalog | 解码、准入、第一跳、路由和可靠性策略 | 最终业务裁决 |

---

## 2. FarmFrame 顶层 oneof

以下是逻辑 IDL；最终 field number 在本文件全部消息评审完成后一次性冻结，避免边讨论边生成
多版代码：

```protobuf
message FarmFrame {
  uint32 protocol_version = 1;
  uint64 sequence = 2;

  oneof body {
    ClientHello client_hello = 10;
    ServerHello server_hello = 11;
    ClientCommandEnvelope client_command = 12;
    CommandResultEnvelope command_result = 13;
    ServerPushEnvelope server_push = 14;
    ControlFrame control = 15;
  }
}
```

oneof 只区分少量稳定消息大类，不枚举所有业务命令。协议方向必须由 ProtocolGuard 继续校验：

| 方向 | 允许 |
|---|---|
| Client → Server | ClientHello、ClientCommand、允许的客户端 ControlFrame |
| Server → Client | ServerHello、CommandResult、ServerPush、允许的服务端 ControlFrame |

客户端发送 `CommandResult` 或 `ServerPush`、服务端发送 `ClientCommand` 均为协议违规，不能仅
因为 oneof 能解码就接受。

### 2.1 为什么 CommandResult 与 ServerPush 必须是不同分支

```text
CommandResult
→ 回答一个request_id / operation_id
→ 已ADMITTED的旧农场命令在切换页面后仍可返回

ServerPush
→ Snapshot / Delta / FarmChange / KICK / REJOIN_REQUIRED
→ 农场类Push必须校验current_subscription_id
```

这保证第 26 号文档冻结的语义能够在协议上直接识别，不能把所有下行都放进一段无法分类的
bytes 后再猜测是否应用 Subscription Fence。

### 2.2 CommandResultEnvelope 与状态语义

以下是逻辑结构，具体业务结果继续由 `CommandSpec.ResultType` 对应的 Proto 编码后放入
`result_payload`：

```protobuf
message CommandResultEnvelope {
  string request_id = 1;
  string operation_id = 2;
  CommandType command_type = 3;
  CommandStatus status = 4;
  ErrorDetail error = 5;
  RetryDirective retry_directive = 6;
  uint32 retry_after_ms = 7;
  optional string server_operation_locator = 8;
  oneof result_context {
    FarmResultContext farm = 10;
    JoinResultContext join = 11;
  }
  bytes result_payload = 15;
}

message FarmResultContext {
  uint64 target_farm_id = 1;
  optional string subscription_id = 2;
}

message JoinResultContext {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
}

message FarmSnapshotPush {
  string join_attempt_id = 1;
  uint64 snapshot_version = 2;
  FarmSnapshot snapshot = 3;
}

message FarmDeltaBatchPush {
  string join_attempt_id = 1;
  uint64 from_version = 2;
  uint64 through_version = 3;
  repeated FarmChange deltas = 4;
}

message FarmChangePush {
  FarmChange change = 1;
}

message SubscriptionReadyPush {
  string join_attempt_id = 1;
  uint64 ready_through_version = 2;
}

enum RejoinReason {
  REJOIN_REASON_UNSPECIFIED = 0;
  REJOIN_REASON_STREAM_GAP = 1;
  REJOIN_REASON_DELIVERY_FAILED = 2;
  REJOIN_REASON_HOLDER_REBUILT = 3;
  REJOIN_REASON_SUBSCRIPTION_LEASE_LOST = 4;
}

message RejoinRequiredPush {
  RejoinReason reason = 1;
  optional uint64 invalid_from_version = 2;
}
```

`FarmDeltaBatchPush` 是 Join Catch-up 的最小公开合同：

```text
through_version >= from_version
deltas非空
第一条.farm_version == from_version
最后一条.farm_version == through_version
相邻FarmChange版本严格+1
```

批次不得包含空洞、重复或乱序版本。`target_farm_id/subscription_id` 已由
`FarmSubscriptionPushContext` 提供，不在 Payload 重复；每条 `FarmChange` 的具体字段由第 32 号
场景合同定义。H5 只能按同一 Subscription FIFO 串行应用，校验失败立即 Fresh Join。

`FarmDeltaBatchPush` 和 `FarmChangePush` 复用完全相同的 `FarmChange` 业务结构，但客户端表现不同：

```text
FARM_DELTA_BATCH（Join追赶）
→ 连续应用state_after
→ 不播放历史动画、Toast或声音

FARM_CHANGE（Ready后的实时变化）
→ 要求change.farm_version == applied_farm_version + 1
→ 先应用state_after，再至多播放一次公开detail动画
```

`FARM_CHANGE` 不携带 `join_attempt_id`；它只能在匹配 Subscription 已完成
`SUBSCRIPTION_READY` 后出现，并由 `FarmSubscriptionPushContext` 完成 Farm 与 Subscription Fence。
若它在 Ready 前出现、版本重复/回退/跳跃，或 Context 不匹配，H5 必须停止当前农场写入并 Fresh Join，
不能尝试按“绝对状态”跳过缺失版本。

`RejoinRequiredPush.invalid_from_version` 只用于诊断“从哪个版本起无法证明连续”，不能被 H5
解释成增量续传起点。H5 收到该 Push 后始终丢弃当前农场实时上下文并执行 Fresh Join；缺省该字段
也不改变动作。

状态冻结为：

| 状态 | 含义 | 典型场景 |
|---|---|---|
| `ACCEPTED` | 请求已被运行时接受，但完成条件尚未成立 | Join 已建立 Joining，等待 Snapshot |
| `SUCCEEDED` | 业务事实和本命令要求的可靠提交均已完成 | 收获完成、购买完成 |
| `PROCESSING` | 可靠 Workflow 已建立，跨 Actor 结果仍在推进 | Plant、Theft 等跨 Actor 流程 |
| `REJECTED` | 命令未产生目标业务事实 | 无权限、版本冲突、参数非法 |
| `NO_CHANGE` | 命令合法，但当前状态无需再次改变 | 地块已被别人浇水、护理事件已处理 |

约束：

1. `ACCEPTED` 不是 `SUCCEEDED`。客户端收到 Join 的 `ACCEPTED` 后仍保持 `Joining`；Join 元信息
   只保存在 `JoinResultContext`，`result_payload` 应为空，避免同一组 farm/subscription/attempt
   字段出现两份不一致来源。禁止包含 FarmSnapshot 或被兼容 Adapter 解释为 Snapshot。
2. 应用匹配的 `FarmSnapshotPush` 后，H5 只进入 `CATCHING_UP`。只有随后收到同一
   `join_attempt_id + subscription_id + target_farm_id` 的 `SubscriptionReadyPush`，并确认本地
   已连续应用到 `ready_through_version`，再叠加第 26 号 Recovery/Bootstrap 屏障，才能进入
   `FARM_INTERACTIVE`。若本地版本小于 N，先等待 FIFO 中的前序 Delta；有界超时仍未到 N 时
   Fresh Join。若本地版本大于 N，视为协议顺序或重复应用错误，立即执行 Fresh Join。
3. `PROCESSING` 必须对应可查询、可恢复的 `operation_id`，不能只表示“后台可能还在处理”。
4. `REJECTED` 不得留下目标业务事实；如果结果未知或已经建立可靠 Workflow，不得伪装成
   `REJECTED`。
5. `NO_CHANGE` 可以携带本命令所需的最小权威摘要、对象版本或拒绝原因，但不能作为完整
   Farm View、Plot Patch 或同场同步的第二条通道；完整农场状态仍由带 Subscription Fence 的
   Snapshot/FarmChange Push 提供。
6. `FarmResultContext` 和 `JoinResultContext` 只在结果确实涉及农场时出现。Farm 版本进入
   具体 Typed Result（例如 `PlantResult.committed_farm_version`），不使用公共的泛化
   `farm_version` 字段。客户端从 A 农场切到 B 农场后，A 的旧命令结果仍可按
   `request_id / operation_id` 更新操作状态，但其中携带的 A 农场视图不得应用到 B 页面。
7. `PlantResult/HarvestResult/CareResult/TheftResult` 只携带本命令所需的最小结果，例如
   `committed_farm_version`、`crop_cycle_id`、`settlement_id` 或奖励摘要。禁止在普通
   CommandResult 中返回完整 FarmSnapshot；否则 CommandResult 和 FarmChange 会形成两条竞争的
   农场状态输入通道。
8. 玩家私有 Bootstrap 通过独立 PlayerBootstrap 结果或 Player Push 下发，不与 FarmSnapshot
   绑定，也不作为保留 Join 双 Snapshot 路径的理由。

### 2.3 RetryDirective

```protobuf
enum RetryDirective {
  RETRY_DIRECTIVE_UNSPECIFIED = 0;
  NONE = 1;
  RETRY_SAME_OPERATION = 2;
  POLL_OPERATION_RESULT = 3;
  FRESH_JOIN = 4;
  RECONNECT = 5;
  REAUTHENTICATE = 6;
  FRESH_JOIN_THEN_RETRY = 7;
  REFRESH_AUTHORITY_STATE = 8;
}
```

| 指令 | 客户端动作 |
|---|---|
| `NONE` | 不自动重试，展示结果或等待正常 Push |
| `RETRY_SAME_OPERATION` | 保持原 `operation_id`，生成新的 `request_id` 后有界重试 |
| `POLL_OPERATION_RESULT` | 保持原 `operation_id`，在 `retry_after_ms` 后查询结果 |
| `FRESH_JOIN` | 停止当前农场写操作，重新执行 JoinFarm 和 Snapshot 恢复 |
| `RECONNECT` | 关闭当前 WSS，使用 Cookie 免密换票后建立新连接 |
| `REAUTHENTICATE` | 回到登录流程，不能继续使用当前 Cookie/Session |
| `FRESH_JOIN_THEN_RETRY` | 先恢复原目标农场 Subscription，再用原 operation_id 和原 Payload 重发 |
| `REFRESH_AUTHORITY_STATE` | 不自动重放旧写命令，重新读取相关 Farm/Player 权威状态 |

`RetryDirective` 是服务端对客户端动作的稳定合同，不等于具体错误码。错误码用于说明原因，
Directive 用于说明下一步，H5 不应在每个页面自行维护一套错误码到动作的映射。

`CommandStatus` 描述“本次服务端尝试发生了什么”，`RetryDirective` 描述“原用户意图接下来
怎么办”，两者正交：

```text
REJECTED + NONE
→ 业务规则或权限拒绝，不自动重试

REJECTED + RETRY_SAME_OPERATION
→ 本次确认尚未进入业务执行边界
→ 可以保留原operation_id重试
```

因此没有必要为了路由或临时不可用再增加第六种 CommandStatus。

### 2.4 ServerPushEnvelope 与 Push Context

```protobuf
message ServerPushEnvelope {
  PushType push_type = 1;
  oneof push_context {
    SessionPushContext session = 10;
    PlayerPushContext player = 11;
    FarmSubscriptionPushContext farm_subscription = 12;
  }
  bytes payload = 15;
}

message SessionPushContext {
  uint64 target_player_id = 1;
  uint64 session_epoch = 2;
}

message PlayerPushContext {
  uint64 target_player_id = 1;
  uint64 session_epoch = 2;
}

message FarmSubscriptionPushContext {
  uint64 target_player_id = 1;
  uint64 target_farm_id = 2;
  string subscription_id = 3;
}
```

`push_context` 的 oneof 直接决定下行 Scope，不再允许 `scope` 与上下文互相矛盾：

| 上下文 | 典型消息 | Fence |
|---|---|---|
| `session` | KICK、Drain、重连或重新认证通知 | connection generation / session epoch |
| `player` | 好友机会提示、任务、红点、个人资产变化 | player_id / session epoch |
| `farm_subscription` | Snapshot、Delta、FarmChange、同场广播 | farm_id + subscription_id |

这只是下行语义分类，不要求拆成三个服务或三套连接。Gate/客户端依据上下文应用对应 Fence：
切换农场只淘汰旧 `farm_subscription` Push，不应误删玩家级任务变化和 Session 级 KICK。
`FarmSnapshotPush`、`FarmDeltaBatchPush` 等 Typed Payload 自己携带
`snapshot_version / from_version / through_version`；Context 只负责“发给谁”和 Subscription
Fence，不负责表达农场业务版本。


每个公开 `PushType` 必须注册静态 `PushSpec`：

```text
PushSpec {
  PushType
  PayloadType
  RequiredContextKind
  MaxPayloadBytes
  DropPolicy
  CoalescingPolicy
}
```

至少冻结：

```text
FARM_SNAPSHOT
→ FarmSnapshotPush + FarmSubscriptionPushContext

FARM_DELTA_BATCH
→ FarmDeltaBatchPush + FarmSubscriptionPushContext

SUBSCRIPTION_READY
→ SubscriptionReadyPush + FarmSubscriptionPushContext

FARM_CHANGE
→ FarmChangePush + FarmSubscriptionPushContext

REJOIN_REQUIRED
→ FarmSubscriptionPushContext + RejoinRequiredPush

KICK / RECONNECT_REQUIRED
→ SessionPushContext

FRIEND_OPPORTUNITY_HINT / PLAYER_STATE_CHANGED
→ PlayerPushContext
```

CI 必须证明每个公开 PushType 恰好一个 PayloadType 和 ContextKind；`FARM_SNAPSHOT`、
`FARM_DELTA_BATCH` 与 `SUBSCRIPTION_READY` 的 Typed Payload 都必须携带 `join_attempt_id`，并且只能应用到匹配的
PendingJoin。Snapshot、Catch-up 和 Ready 的 `DropPolicy=NEVER_DROP`、`CoalescingPolicy=NONE`；
任一无法入队都必须让本次 Join 失败或进入 REJOIN_REQUIRED，不能跳过屏障继续经营。
`FARM_CHANGE` 的 `DropPolicy=NEVER_SILENT_DROP`、`CoalescingPolicy=NONE`：单个 PlotDelta 虽然携带
提交后的绝对公开状态，但正常实时流仍不得跨 `farm_version` 合并或跳过。若实时变化无法进入某个
Subscription 的有界队列，Hub/Gate 必须使该 Subscription 进入内部 `RESYNC_REQUIRED`，再映射为
公网 `REJOIN_REQUIRED`；不得回滚 FarmActor 已完成的权威提交，也不得阻塞其他连接。

同一 `FarmChange` 不得含 viewer 个性化字段。实现可按协议版本对公共 Payload 编码一次，并在
FarmSubscriptionHub 中按 Gateway 分组复用；每个目标仍使用自己的
`FarmSubscriptionPushContext` 和 ConnectionWriter FIFO。200 人同场时，FarmActor 只产生一份
业务变化，不执行 200 次业务计算或等待 200 个客户端 ACK。
`REJOIN_REQUIRED` 是终止控制 Push，也不得静默丢弃；相同 Subscription 的重复失效可以在分配
FarmFrame sequence 前合并为一次，但若当前 ConnectionWriter 连终止帧都无法接受，Gate 必须关闭
连接并通过重连恢复，不能继续保留可写 Subscription。

名称固定为：

```text
Hub/Gate内部状态或动作：RESYNC_REQUIRED
公网PushType：REJOIN_REQUIRED
客户端动作：Fresh Join
```

公网 Proto、PushSpec 和客户端 reducer 不得定义第二个 `RESYNC_REQUIRED` PushType。Hub 把内部失效
映射成 `REJOIN_REQUIRED` 时必须携带仍有效的旧 FarmSubscriptionPushContext；该终止 Push 入队或
连接确认不可达后，Gate 才能释放只读终止 Fence。

### 2.5 连接级 sequence

`FarmFrame.sequence` 按“每条 WSS 连接、每个方向”独立计数：

```text
Client → Server：ClientHello = 1，后续每帧严格 +1
Server → Client：ServerHello = 1，后续每帧严格 +1
新WSS连接：两个方向都重新从1开始
```

P0 采用严格连续策略：重复、回退或跳号均视为连接协议异常，停止业务准入并要求重连。
Gate 对 Client→Server、H5 对 Server→Client 都必须实现 `incoming == last + 1`，不能只拒绝
`incoming <= last` 而静默接受空洞。农场版本缺口使用 Fresh Join；连接 sequence 空洞使用新 WSS
重建连接级序号，二者不能混为一谈。
WebSocket 本身提供有序可靠字节流，sequence 的目的不是替代 TCP，也不是业务幂等，而是发现：

- 客户端重复发送应用帧；
- 旧连接帧误投到新连接；
- Adapter/Gate 错误复用连接上下文；
- 服务端实现错误造成的下行空洞。

服务端 sequence 应在 `ConnectionWriter` 确认该帧将进入当前连接发送序列时分配；入队前已经
拒绝的 Push 不得消耗 sequence。一旦分配 sequence 并进入 FIFO，就不能选择性删除、重排或
合并该帧；背压决策（丢弃 Hint、合并绝对状态、将内部流标记为 `RESYNC_REQUIRED` 并生成公网
`REJOIN_REQUIRED`）必须发生在分配
sequence 之前。业务重试继续依赖 `operation_id`，不能依赖 sequence。

---

## 3. ClientCommandEnvelope

冻结逻辑结构：

```protobuf
message ClientCommandEnvelope {
  string request_id = 1;
  string operation_id = 2;
  CommandType command_type = 3;
  bytes payload = 4;
}
```

### 3.1 request_id

```text
一次客户端网络请求/Trace关联ID
客户端重新发起一次网络尝试时可以变化
不得作为业务防重键
```

同一个 `operation_id` 可以对应多个重试请求的 `request_id`。

冻结格式：

- 必填；
- 不透明 ASCII 字符串；
- 长度 `1..64` 字节；
- 在当前连接的未完成请求窗口内不得重复；
- 服务端日志、Trace 和 CommandResult 必须原样回传，不能把它解析成业务 ID。

### 3.2 operation_id

```text
一次用户业务意图的幂等ID
Redirect、超时、断线恢复和PROCESSING查询必须保持
不得因为没有立即收到结果而生成新ID
```

业务幂等键按“执行根 Authority”隔离。公网客户端不直接提交可信 Root ID；Gate/FirstHop 根据
CommandCatalog 和已校验的业务目标派生：

```text
OperationRootRef {
    root_authority_kind          // PLAYER_ACTOR | FARM_ACTOR | FRIEND_PAIR | INDEPENDENT_WORK
    canonical_root_authority_id
}

EffectiveIdempotencyKey {
    subject_player_id
    command_type
    operation_id
    operation_root
}
```

同一 Root 内相同 Key、不同规范化 Payload Hash 返回 `IDEMPOTENCY_CONFLICT`。不同 Root 不共享
Receipt、Workflow 或结果目录项，避免 A 农场/好友 Pair 的旧结果污染 B 农场/好友 Pair。

典型映射：

```text
BUY / SELL / CLAIM_TASK / CLAIM_MAIL_ATTACHMENT
                              → PLAYER_ACTOR:subject_player_id
PLANT / HARVEST               → FARM_ACTOR:target_farm_id
CARE / THEFT                  → FRIEND_PAIR:canonical_pair(subject, target_owner)
DELETE_FRIEND / BLOCK_PLAYER  → FRIEND_PAIR:canonical_pair(subject, peer)
独立Workflow                  → 由CommandSpec冻结其RootPolicy，不能运行时猜测
```

冻结格式与必填策略：

- 不透明 ASCII 字符串，长度 `1..64` 字节；
- `IdempotencyPolicy=REQUIRED` 的写命令与可恢复 Workflow 必填，并进入持久 Receipt/Result 合同；
- `IdempotencyPolicy=RUNTIME_REQUIRED` 只用于 Join 等运行时意图：operation_id 必填并参与当前
  PendingJoin 防重，但不进入长期 OperationResult；
- 查询类命令可为 `NONE`；`GetOperationResult` 在自身 Payload 中携带被查询的原始
  `operation_id`；
- 客户端不得把 `request_id` 直接当作新的 `operation_id`。

### 3.3 command_type

`command_type` 是 CommandCatalog 的唯一查找键。未知、未注册或在当前协议版本不可用的命令
必须在 Gate 的 Protocol/Admission 阶段拒绝，不路由到任意 Actor 猜测处理。

### 3.4 bytes payload

`payload` 必须由 `CommandSpec.PayloadType` 指定的 Proto 解码。它不是任意透传数据：

```text
查CommandSpec
→ 检查MaxPayloadBytes
→ 使用指定PayloadType解码
→ 拒绝malformed/unknown command
→ 执行字段和Session准入校验
```

客户端 SDK 应提供 `sendPlant(PlantRequest)` 等强类型包装，业务代码不得直接拼接任意 bytes。

### 3.5 Canonical Payload Hash

Canonical Payload Hash 的首要用途是处理弱网、RPC 超时、Route Redirect 和断线恢复中的同一
业务重试：

```text
第一次Plant
operation_id=plant-10086
payload=在3号地种小麦
→ FirstHop保存payload_hash=abc123

弱网没有收到结果，客户端重试
operation_id仍为plant-10086
payload仍为在3号地种小麦
→ Hash仍为abc123
→ 返回原Workflow/OperationResult
→ 不重复扣种子、不重复种植

错误复用同一operation_id，但改成在4号地种玉米
→ Hash变为xyz789
→ IDEMPOTENCY_CONFLICT
```

因此 Hash 回答的是“这次重试是否仍然是原来的业务意图”。它不是网络包校验和、不是玩家身份
签名，也不能替代 TLS、Session Fence 或 `operation_id`。

幂等校验不得直接 Hash 原始 Protobuf bytes，也不得通过“将解码对象重新 Marshal 一遍”获得
Hash。合法 Protobuf 可能因为字段顺序、未知字段或编码实现差异产生不同 bytes，而业务语义
相同。

每个 `IdempotencyPolicy=REQUIRED` 或 `RUNTIME_REQUIRED` 的 FirstHopCommandPolicy 必须提供语义
字段明确的 `CanonicalPayloadBuilder`。例如：

```text
PLANT CanonicalPayload
├── command_type
├── target_farm_id
├── plot_id
├── crop_definition_id
├── expected_plot_version
└── expected_crop_rule_version

UNLOCK_PLOT CanonicalPayload
├── command_type
├── target_farm_id
├── plot_id
├── expected_plot_version
└── expected_unlock_rule_version
```

先按固定字段顺序和固定整数编码生成规范化表示，再计算 Hash：

```text
相同EffectiveIdempotencyKey + 相同CanonicalPayloadHash
→ 同一业务意图的重试

相同EffectiveIdempotencyKey + 不同CanonicalPayloadHash
→ IDEMPOTENCY_CONFLICT
```

参与最终奖励计算但不由客户端提交的服务器字段不进入客户端 Payload Hash；具体每条命令的
字段集合由第 32 号场景技术合同冻结。

唯一权威 Hash 由 FirstHop 在重新解码后生成，并与 Workflow/Receipt/OperationResult 同步保存。
Gate 不把自己计算的 Hash 作为内部 RPC 正确性字段：

```text
Gate新版本Canonicalizer
≠
旧FirstHop Canonicalizer

不能因此把语义相同的滚动升级请求误判为HASH_MISMATCH
```

Gate 如为日志采样生成诊断 Hash，必须标记为非权威，不能参与幂等、业务拒绝或 FirstHop 对比。

### 3.6 Gate 与 FirstHop 的解码边界

冻结为“原始 bytes 透传、两端独立解码”：

```text
H5 Typed Object
→ Protobuf编码为payload bytes
→ FarmFrame发送

farm-gate
→ 按CommandSpec解码一次
→ 结构/Session/目标/大小校验
→ 提取RouteKey
→ 保留原始payload bytes

内部RPC
→ command_type
→ raw payload bytes
→ TrustedCommandContext
→ 服务端解析出的RouteKey

FirstHop（player-svr / farm-svr）
→ 按CommandSpec再次解码原始bytes
→ 验证payload目标与RouteHeader一致
→ 生成唯一权威CanonicalPayloadHash
→ 执行业务校验和Actor调用
```

Gate 不把已解码 Typed Payload 再次编码成新的业务 bytes。内部 gRPC 仍会序列化外层
ExecuteCommandRequest，但业务 Payload 保持原始 bytes。FirstHop 的第二次解码用于建立独立
业务正确性边界，防止 Gate 路由缺陷把 A 农场命令投给 B 农场 Actor。

内部 RPC 的精确字段、可信调用身份和 RouteHeader 由第 29 号文档冻结；本节只冻结
“Gate 解码并保留 raw payload、FirstHop 独立复核”的责任边界。

### 3.7 PlayerBootstrap 与连续好友机会

两条首屏读取都使用显式 ClientCommand，不依赖未登记的 Player Push，也不进入长期 Operation Journal：

```protobuf
message GetPlayerBootstrapRequest {
}

message GetPlayerBootstrapResult {
  uint64 player_state_revision = 1;
  PlayerPrivateProfileView profile = 2;
  PlayerProgressionView progression = 3;
  WalletView wallet = 4;
  InventoryView inventory = 5;
  PlayerFeatureSummaryView feature_summary = 6;
  int64 trusted_server_now_ms = 7;
}

message FriendOpportunityHintedTarget {
  uint64 owner_id = 1;
  uint64 opportunity_revision = 2;
}

message GetNextFriendOpportunityRequest {
  repeated FriendOpportunityHintedTarget hinted_targets = 1;
}

enum FriendOpportunityLookupStatus {
  FRIEND_OPPORTUNITY_LOOKUP_STATUS_UNSPECIFIED = 0;
  FRIEND_OPPORTUNITY_FOUND = 1;
  FRIEND_OPPORTUNITY_NONE = 2;
}

message FriendOpportunityCandidateView {
  uint64 target_farm_id = 1;
  PublicProfileView public_profile = 2;
  uint32 action_mask = 3;
  uint64 opportunity_revision = 4;
  int64 validated_at_ms = 5;
}

message GetNextFriendOpportunityResult {
  FriendOpportunityLookupStatus lookup_status = 1;
  optional FriendOpportunityCandidateView candidate = 2;
  string navigation_generation = 3;
}
```

逻辑语义：

1. `GET_PLAYER_BOOTSTRAP` 的操作者和 RouteKey 只来自当前 `TrustedCommandContext.session.player_id`；
   空 Payload 不允许客户端指定另一个玩家。
2. Bootstrap 进入 PlayerActor，先执行幂等 `EnsureDailyPeriod` 与本周期登录一次事实，再返回同一 Turn
   读取到的私有状态；重复查询不会重复推进任务或发奖，因此不建立 Durable Outcome。
3. Bootstrap 只包含玩家私有资料、经济、库存、成长和支持系统摘要。Plot/Crop/FarmVersion 只能来自
   `JOIN_FARM` 的 Snapshot/Delta，不能在这里建立第二条农场状态通道。
4. H5 只应用不低于当前本地 `player_state_revision` 的 Bootstrap；旧响应可以结束本次查询，但不得
   覆盖更新的玩家私有状态。
5. `GET_NEXT_FRIEND_OPPORTUNITY` 的 viewer 只来自 Session。客户端不得提交 scan cursor、visited 集合或
   Redis Navigation Key；仅允许把刚收到的服务端 Opportunity Hint 作为有界 `hinted_targets` 回传。每项
   只含 `owner_id + opportunity_revision`，最多 16 项，仅用于定向失效与精查，不能授权好友关系或操作。
6. Navigation 先使用 FriendIndex、FarmOpportunityHeader 和短期 CandidateWindow 粗筛，再对候选执行
   Friendship Authority 与 FarmReadAPI.BatchEvaluate 精确检查。返回的 action_mask 是响应时刻的精确
   视图，点击后的 Permit/FarmActor 仍是最终裁决。
7. `FOUND` 必须携带 candidate；`NONE` 必须证明当前扫描范围已经得到明确无机会结果。FriendIndex、
   Header/Facts、Friendship 或 FarmRead 依赖不可用时，CommandResult 返回
   `REJECTED + FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE + SAFE_QUERY_RETRY`，不得返回 `NONE`。
8. 查询响应丢失后可直接重查。当前候选在仍有效且尚未被服务端标记 visited 时应优先复用；候选失效
   时可继续扫描下一个，不产生资产、关系或任务副作用。
9. 好友农场完成 `SUBSCRIPTION_READY` 后，服务端通过第 29 号受信运行时接缝最佳努力更新
   Navigation visited。该更新失败最多造成候选重复，不回滚 Join；每日访问任务仍使用独立
   `CONFIRM_FRIEND_VISIT` 可靠命令。

### 3.8 GetOperationResult

弱网、RPC 超时或断线后，客户端使用原业务标识查询：

```protobuf
message GetOperationResultRequest {
  CommandType original_command_type = 1;
  string original_operation_id = 2;
  OperationTargetLocator original_target = 3;
  optional string server_operation_locator = 4;
}

message OperationTargetLocator {
  oneof target {
    uint64 target_farm_id = 1;
    uint64 peer_player_id = 2;
    string mail_id = 3;
  }
}

enum OperationLookupStatus {
  OPERATION_LOOKUP_STATUS_UNSPECIFIED = 0;
  OPERATION_LOOKUP_NOT_FOUND = 1;
  OPERATION_LOOKUP_PROCESSING = 2;
  OPERATION_LOOKUP_COMPLETED = 3;
  OPERATION_LOOKUP_EXPIRED = 4;
}

message GetOperationResultResult {
  CommandType original_command_type = 1;
  string original_operation_id = 2;
  OperationLookupStatus lookup_status = 3;
  optional StoredOperationOutcome completed_outcome = 4;
  uint32 retry_after_ms = 5;
  RetryDirective completed_retry_directive = 6;
}

message StoredFarmOutcomeContext {
  uint64 target_farm_id = 1;
}

message StoredOperationOutcome {
  CommandType original_command_type = 1;
  string original_operation_id = 2;
  CommandStatus status = 3;
  ErrorDetail error = 4;
  optional StoredFarmOutcomeContext farm_context = 5;
  bytes result_payload = 6;
}
```

语义：

| 状态 | 含义 | 客户端动作 |
|---|---|---|
| `NOT_FOUND` | 当前没有找到可返回的 Workflow/Receipt/Result；不证明原请求从未到达 | 按 RecoveryPolicy 恢复环境后使用原 operation_id 重发 |
| `PROCESSING` | 可靠流程已存在且尚未完成 | 按 retry_after_ms 继续查询 |
| `COMPLETED` | 已有终态结果 | 返回原 SUCCEEDED/NO_CHANGE/REJECTED 及服务端重建的稳定恢复指令 |
| `EXPIRED` | 结果超过保留期，已不能安全恢复原响应 | 停止自动重试，刷新相关权威状态 |

约束：

1. 当前玩家身份只能来自新的有效 Session，不能由查询 Payload 指定。
2. 查询只允许访问当前玩家自己的 `EffectiveIdempotencyKey`。`subject_player_id` 来自当前 Session；
   `original_target` 只是派生 Root 的业务材料，必须由原 CommandSpec 的 `OperationRootExtractor`
   重新校验。客户端不能提交任意内部 Bucket、PairID、WorkAddress 或 Redis Key。
   以当前玩家为 Root 的命令可省略 `original_target`；Farm、Pair 或 Mail Root 必须携带对应
   `target_farm_id / peer_player_id / mail_id`，或使用服务端签发的 Locator。
3. 若首次 `ACCEPTED/PROCESSING` 响应已返回 `server_operation_locator`，H5 Journal 应原样保存并在查询时
   携带。它是不透明、带完整性保护的定位提示，不是授权；服务端仍校验当前 subject、command 和 Root。
4. `NOT_FOUND` 不允许生成新 operation_id。原请求可能仍在途中，同 Root 内相同 operation_id 的
   CreateIfAbsent/Receipt 必须保证并发重发仍只生效一次。
5. `PROCESSING` 不允许重新提交新的业务意图。
6. `COMPLETED.completed_outcome` 是不含网络请求身份和临时重试指令的原业务结果，不是查询
   命令本身的 CommandResult。原 `request_id`、旧 Subscription 和当时的 RetryDirective 不
   进入持久结果；`completed_retry_directive` 由服务端根据
   `original_command_type + terminal ErrorCode + ErrorRecoveryPolicy` 在查询时重建。H5 不得自行
   把 ErrorCode 映射成恢复动作。
7. JoinFarm 使用 Fresh Join；纯查询直接重查，二者都不进入持久 OperationResult。
8. `EXPIRED` 后如何刷新由原命令的 RecoveryPolicy 决定，例如农场命令 Fresh Join、玩家资产
   重新 Bootstrap；不能盲目重放旧写命令。
9. 只有服务端能根据 Result Directory 水位或保留期元数据证明结果已经过期时才能返回
   `EXPIRED`；不能仅因查不到记录就把 `NOT_FOUND` 猜成 `EXPIRED`。
10. 农场 Subscription 命令若当前已切换到另一农场，`NOT_FOUND` 后必须先恢复原
   `target_farm_id` 的 Subscription，再用原 operation_id 重发；不能在新农场上下文中直接重放。
11. H5 SDK 必须在终态前持久保留 `command_type / operation_id / 原Typed Payload / 原业务目标 /
    可选server_operation_locator`。只保存 operation_id 无法唯一确定 Root，也无法在 NOT_FOUND 后
    安全恢复原命令。
    `REDEEM_SHARE` 是安全例外：原始 ShareToken 在 Execution-Accept 前只能保留在当前标签页的
    短生命周期容器中；一旦收到携带 Locator 的 `ACCEPTED/PROCESSING`，Journal 保存 Locator 并删除
    原始 Token，不得把 Bearer Token 写入 IndexedDB/localStorage/日志。
12. `StoredOperationOutcome.status` 只能是终态 `SUCCEEDED / REJECTED / NO_CHANGE`；
    `ACCEPTED / PROCESSING` 只能由当前查询的 `lookup_status` 表达，不能伪装成已完成结果。
13. `StoredFarmOutcomeContext` 只保存业务目标 `target_farm_id`。`request_id`、`subscription_id`、
    `join_attempt_id`、Session Fence、RetryDirective 和旧连接代次都属于网络运行态，不得进入长期
    OperationResult；恢复时必须从新 Session/Fresh Join 重新生成。

CommandCatalog 必须能根据 `original_command_type` 找到对应 FirstHopRole 和
`OperationResultLookupPolicy / RecoveryPolicy`。至少支持：

```text
PLAYER_RETRY_SAME_OPERATION
FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION
REFRESH_AUTHORITY_STATE
```

结果的逻辑查找键是 EffectiveIdempotencyKey；Redis Key、结果目录、保留期和源 Receipt 的物理布局
留给第 31/32 号合同。

---

### 3.9 H5 Operation Journal

所有 `IdempotencyPolicy=REQUIRED` 且 `RecoveryPolicy` 允许恢复的写命令，H5 必须在第一次发送前
把稳定重放材料写入 IndexedDB；只记录 operation_id 不满足合同。

```text
PendingOperationRecord {
  environment
  player_id
  command_type
  operation_id
  target_farm_id?             # 农场命令必填
  peer_player_id?             # 好友Pair命令必填
  mail_id?                    # 邮件Root命令必填
  operation_root_policy
  server_operation_locator?   # 服务端已返回时保存；客户端不解析
  payload_schema_version
  payload_encoding            # 例如 protobuf-base64
  original_typed_payload
  created_at_ms
  last_lookup_status
  retry_count
}
```

不得持久保存：

```text
SessionPermit / TrustedCommandContext
ActionPermit
ActorRouteHeader / writer_epoch
旧subscription_id
```

这些可信上下文必须在恢复时重新生成。

恢复矩阵：

```text
COMPLETED
→ 应用StoredOperationOutcome并删除Journal

PROCESSING
→ 按retry_after轮询

NOT_FOUND + PLAYER_RETRY_SAME_OPERATION
→ 新request_id + 原operation_id + 原Payload重发

NOT_FOUND + FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION
→ Fresh Join原target_farm_id
→ 新request_id + 原operation_id + 原Payload重发

EXPIRED / REFRESH_AUTHORITY_STATE
→ 不自动重放旧写命令
→ 重新读取Player/Farm权威状态
```

Journal 的保留时间必须覆盖服务端 OperationResult/Receipt 的全部恢复窗口；终态确认后及时删除。

Journal 必须显式配置并度量四个边界：

```text
max_pending_entries_per_player
max_entry_bytes
max_total_bytes_per_player
max_retention_duration
```

冻结行为：

1. 单条记录超过 `max_entry_bytes` 时，第一次发送前即拒绝该命令，不得“先发网络、后记 Journal”。
2. 达到 entry/total bytes 上限时，H5 进入 `JOURNAL_CAPACITY_BLOCKED`，停止创建新的可恢复写命令，
   但继续允许查询、恢复和清理已有未决记录。
3. 未得到 `COMPLETED/EXPIRED` 或其他可证明终态前，不得按 LRU、创建时间或页面卸载静默淘汰。
4. Schema 迁移失败时保留原始 bytes 并 Fail Closed；不能用新 Schema 猜测重编码后自动重放。
5. 登出、切换账号和环境时按 `environment + player_id` 隔离 Journal；不得把 A 的 operation 在 B
   的 Session 下恢复。
6. `max_retention_duration` 是未决记录的告警与强制对账期限，不是自动删除 TTL。超过期限仍无
   可证明终态时进入 `JOURNAL_RECOVERY_OVERDUE`，继续保留并查询原记录，同时按配置阻止新的
   可恢复写命令；不得把超时本身当成 `EXPIRED`。
7. 数值由 H5 存储探针和容量模型冻结，并暴露 pending 数、总字节、最老年龄、容量阻塞、恢复
   逾期和迁移失败指标。

---

## 4. 为什么业务命令不使用大 oneof

不采用：

```protobuf
oneof command_body {
  JoinFarmRequest join_farm = 10;
  BuyRequest buy = 11;
  PlantRequest plant = 12;
  HarvestRequest harvest = 13;
  CareRequest care = 14;
  TheftRequest theft = 15;
  // 未来任务、邮件、宠物、图鉴、活动继续增长
}
```

原因：

- 每新增业务命令都会修改核心公网命令 oneof；
- Gate、H5 和所有依赖公共命令包的代码需要反复重新生成；
- CommandCatalog 已经是本架构的命令扩展点，大 oneof 会形成重复映射；
- 未知新命令在旧服务上只剩 unknown field，难以直接返回明确 CommandType 错误；
- oneof 不能替代服务端字段、权限和业务状态校验。

类型安全由“生成的 Typed Payload + 客户端 SDK + CommandCatalog Decoder + 服务端校验”共同
保证；安全边界不依赖客户端是否使用生成代码。

---

## 5. Typed Payload 与路由最小字段

本节只冻结 CommandCatalog 提取 RouteKey 和校验 Subscription 所需的最小字段；奖励计算、
Workflow Participant 和完整结果字段留给第 32 号场景技术合同。

### 5.0 P0 自有农场 ID 合同

P0 冻结：

```text
own_farm_id == player_id
```

这只是产品 ID 映射，不合并两个权威和路由域：

```text
PLAYER:10001 → PLAYER VirtualShard → player-svr → PlayerActor(10001)
FARM:10001   → FARM VirtualShard   → farm-svr   → FarmActor(10001)
```

因此即使数值相同，PlayerActor 与 FarmActor 仍可落在不同 Shard、不同 Pod，并通过第 27 号
文档的 Workflow/Order 协作。

### 5.1 JoinFarm

```protobuf
message JoinFarmRequest {
  uint64 target_farm_id = 1;
}
```

`JOIN_FARM` 不要求已经存在当前 Subscription，因为它本身负责创建或替换 Subscription。
`join_attempt_id` 不进入客户端 Payload；它由 Gate 在 ADMITTED 前生成，通过第 29 号内部可信
上下文传给 FirstHop，并由 `JoinResultContext`、`FarmSnapshotPush`、`SubscriptionReadyPush`
和全部 Join 回调原样关联。

```text
operation_id
→ 客户端逻辑Join意图
→ 同一PendingJoin重试保持

join_attempt_id
→ Gate生成的当前连接级PendingJoin Fence
→ 同一PendingJoin和内部Redirect保持
→ 新WSS或Fresh Join重新生成
```

同一连接 Fence 上相同 `operation_id + canonical_payload_hash` 必须幂等复用当前 attempt；相同
operation_id 但不同目标返回 `IDEMPOTENCY_CONFLICT`。新连接不会继承旧 attempt，旧 Snapshot、
Ready 或 Cleanup 不能凭客户端复用的 operation_id 污染新连接。

### 5.2 Plant

```protobuf
message PlantRequest {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  uint32 crop_definition_id = 3;
  uint64 expected_plot_version = 4;
  uint64 expected_crop_rule_version = 5;
}
```

必须满足：

```text
target_farm_id
== TrustedCommandContext.farm_context.admitted_farm_id
== TrustedCommandContext.session.player_id
```

`expected_crop_rule_version` 进入 CanonicalPayloadHash。FirstHop 在创建 PlantWorkflow 前比较当前可用
CropDefinition 版本；不匹配返回 `RULE_VERSION_CHANGED`。客户端不提交 seed_item_id、成熟时间、产量
或护理/偷菜数值，这些服务器字段不进入客户端 Payload Hash。

### 5.2A UnlockPlot

```protobuf
message UnlockPlotRequest {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  uint64 expected_plot_version = 3;
  uint64 expected_unlock_rule_version = 4;
}
```

必须满足自己的当前农场约束。客户端不提交等级、价格、货币类型或前置 Plot；FirstHop 从版本化
PlotUnlockRule 冻结这些服务端事实，并在创建 UnlockPlotWorkflow 前拒绝旧规则版本。

### 5.3 Harvest

```protobuf
message HarvestRequest {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  string crop_cycle_id = 3;
  uint64 expected_plot_version = 4;
}
```

必须满足自己的当前农场约束，并将 `crop_cycle_id` 绑定到客户端看到的那一茬。

`HarvestResult` 只返回本次收获的最小结果；图鉴首次解锁由 FarmActor/PlayerActor 的可靠提交共同决定，
客户端不能通过请求字段伪造：

```protobuf
message HarvestResult {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  string crop_cycle_id = 3;
  uint64 committed_farm_version = 4;
  uint32 harvested_quantity = 5;
  bool compendium_newly_unlocked = 6;
  optional uint32 unlocked_crop_definition_id = 7;
  uint64 compendium_revision = 8;
}
```

`compendium_newly_unlocked=true` 仅表示这次 Harvest 的 PlayerActor 提交首次解锁了该植物；偷菜、
护理奖励、邮件附件和任务奖励不得复用这个字段。若收获已完成但结果重试，必须返回同一解锁结果，不能重复增加
图鉴版本。

### 5.4 Care

```protobuf
message CareRequest {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  string crop_cycle_id = 3;
  string incident_id = 4;
  CareType care_type = 5;
}
```

必须满足 `target_farm_id == farm_context.admitted_farm_id`；之后由 ActionPermit 和 FarmActor 校验主人或
好友护理权限、Incident 是否仍 ACTIVE。

### 5.5 Theft

```protobuf
message TheftRequest {
  uint64 target_farm_id = 1;
  uint32 plot_id = 2;
  string crop_cycle_id = 3;
}
```

必须满足 `target_farm_id == farm_context.admitted_farm_id`，且通过有效好友 ActionPermit；成熟、份额、
每好友每茬一次和宠物结果最终由 FarmActor 裁决。

### 5.6 Task

任务命令继续使用公共 `ClientCommandEnvelope.operation_id`，Typed Payload 不得重复携带它：

```protobuf
message GetTaskListRequest {
}

message ClaimTaskRequest {
  string task_instance_id = 1;
}

message ConfirmFriendVisitRequest {
  uint64 target_farm_id = 1;
}
```

对应结果逻辑类型为 `GetTaskListResult`、`ClaimTaskResult` 和 `ConfirmFriendVisitResult`；其中任务列表、
奖励摘要、下一成长任务与 `TaskSummary` 的业务字段服从第 34 号，最终 field number 仍由唯一 Proto
生成源冻结。

`CONFIRM_FRIEND_VISIT` 只能由 H5 在收到匹配的 `SUBSCRIPTION_READY` 后自动发送，并必须满足：

```text
target_farm_id == TrustedCommandContext.farm_context.admitted_farm_id
target_farm_id != TrustedCommandContext.session.player_id
farm_context.authorization_relation_id存在
farm_context.authorization_relation_epoch存在
```

H5 不提交 `player_id`、`subscription_id`、`relation_id`、`relation_epoch` 或访问时间；这些证明全部
来自 Gate 当前 ActiveSubscription。Canonical Payload Hash 只覆盖 `target_farm_id`，相同
operation_id 不能换另一座农场。访问发生时间使用可信 FarmAdmissionContext 中的 Subscription
激活时间，而不是客户端时间或命令到达 PlayerActor 的时间。

### 5.7 好友关系与重加好友轮换

```protobuf
message DeleteFriendRequest { uint64 peer_player_id = 1; }
message BlockPlayerRequest { uint64 peer_player_id = 1; }
message UnblockPlayerRequest { uint64 peer_player_id = 1; }

message RotateShareTokenRequest {
  optional uint64 reactivation_peer_player_id = 1;
}
```

普通 Rotate 不携带 peer。若用于恢复一段已经删除/拉黑后解除的关系，客户端必须显式携带
`reactivation_peer_player_id`；FirstHop 由当前 Session 与 peer 派生 canonical Pair Root，读取当前
`ReactivationBarrier=READY` 的受信证明，再调用 ShareIssuer 的 Typed Rotate。H5 不提交 pair_id、
barrier_revision、generation floor 或 RotateReceipt；这些内部字段由服务端产生并校验。

DELETE/BLOCK/UNBLOCK 的 OperationRoot 是 `FRIEND_PAIR:canonical_pair(subject, peer)`；普通
GENERATE/ROTATE/REVOKE 的 Root 是 `FRIEND_SHARE_ISSUER:subject`；带 reactivation peer 的 ROTATE 仍以
ShareIssuer 为写 Authority，但 OperationRootExtractor 必须把 Pair 目标纳入 Root/Canonical Payload，避免
同一 operation_id 被换 peer 重放。

### 5.8 简化宠物守护

宠物页面只操作当前玩家自己的 `PetState`，不要求当前 Farm Subscription，也不创建 PetActor 或 pet-svr。
守护生效时间、已解锁宠物、当前守护宠物和 `guard_version` 均由 PlayerActor 裁决；FarmActor 只接收
`GuardSyncOrder` 形成用于偷菜裁决的 `GuardSnapshot`。

```protobuf
message GetPetStateRequest {}

message PetUnlockByCoinRequest {
  uint32 pet_id = 1;
  uint64 expected_pet_config_version = 2;
  uint64 expected_price_version = 3;
}

message PetFeedRequest {
  uint32 quantity = 1;
  uint64 expected_feed_rule_version = 2;
  uint64 expected_active_pet_config_version = 3;
  uint64 expected_guard_rule_version = 4;
}

message PetSwitchRequest {
  uint32 target_pet_id = 1;
  uint64 expected_pet_config_version = 2;
  uint64 expected_guard_rule_version = 3;
}
```

约束：

- `PET_UNLOCK_BY_COIN` 的价格、货币类型和解锁前置由服务端版本化配置决定；客户端只能提交 `pet_id`
  与页面看到的配置/价格版本，不能提交价格或扣款结果。
- `PET_FEED` 只提交狗粮数量；PlayerActor 在一次可靠提交中校验库存、更新 `guard_until_ms`，并产生
  `GuardSyncOrder`。Feed 还绑定页面看到的喂食规则、当前宠物配置和守护规则版本，避免在配置变化后
  静默采用玩家没有确认的新规则。未喂狗粮时守护不生效；切换宠物保留剩余守护时间。
- `PET_SWITCH` 只允许切换已解锁宠物，不消耗狗粮；同一目标已是当前宠物时返回 `NO_CHANGE`。
- `PET_UNLOCK_BY_TASK` 不开放公网命令。任务 `RewardBundle` 在 PlayerActor 内与任务领取同一提交中解锁宠物。

逻辑结果类型：

```text
GetPetStateResult
  → PetPageView（含pet_state_version和动态guard_sync_status；不暴露Redis Key、Order载荷或宠物概率）
PetUnlockByCoinResult
  → action_status、决定时PetStateView、pet_id、charged_coin、coin_balance、guard_sync_required=false
PetFeedResult
  → action_status、决定时PetStateView、consumed_quantity、guard_sync_required=true、
    guard_sync_order_id、guard_version_at_decision
PetSwitchResult
  → action_status、决定时PetStateView、active_pet_id、guard_sync_required、
    guard_sync_order_id、guard_version_at_decision
```

`guard_until_ms` 只返回当前玩家自己的页面所需信息；它不能进入公共 `FarmView` 或 `FarmChange`。
历史 `PetUnlock/Feed/SwitchResult` 一经保存不可因后台同步完成而改写；当前 `guard_sync_status` 只由
`GET_PET_STATE` 根据 `PetState.guard_version + GuardSyncWatermark` 动态计算。同步失败由可靠
`GuardSyncOrder` 重试，不能让客户端重复扣狗粮。H5 只应用更高 `pet_state_version` 的 PetStateView；
旧结果仍可结束对应 Operation Journal 项，但不得覆盖较新的宠物页面。

### 5.9 简化植物图鉴

图鉴是 PlayerActor 内的轻量集合，不拆 CompendiumActor 或 compendium-svr。查询不要求当前 Farm
Subscription，活动档案和灰色剪影由服务端根据版本化 PlantCatalog 与 `CompendiumState` 拼装。

```protobuf
message GetCompendiumRequest {}

message AckCompendiumSeenRequest {
  uint64 observed_revision = 1;
}
```

逻辑结果 `GetCompendiumResult` 至少包含：

```text
CompendiumView {
  catalog_version
  compendium_revision
  compendium_seen_revision
  has_new_unlock
  entries[] {
    crop_definition_id
    display_state        // UNLOCKED / LOCKED_SILHOUETTE
    archive_state        // CURRENT / ACTIVITY_ARCHIVE
    legacy_seed_plantable
  }
  next_archive_cursor?
}

AckCompendiumSeenResult {
  compendium_seen_revision_after
  current_compendium_revision
}
```

首次成功收获自己种植的作物时，Harvest Workflow 在 PlayerActor 的同一可靠提交内完成图鉴解锁；
`GET_COMPENDIUM` 只读 PlayerActor 权威结果，不使用延迟红点或好友机会 Projection 冒充完整图鉴。

`GET_COMPENDIUM` 由 `CompendiumReadIngress` 先捕获本地不可变 PlantCatalog Snapshot，再通过 PLAYER
Affinity 读取小型 CompendiumState，最后在 Actor Turn 外组装页面。`ACK_COMPENDIUM_SEEN` 在
PlayerActor 内执行：

```text
seen_after = max(current_seen, min(observed_revision, current_revision))
```

GET 本身不得修改 seen_revision；H5 只有在成功应用并渲染对应页面后才发送 Ack。重复或旧 Ack 返回
`NO_CHANGE`。Ack 是可安全重入的低风险单调写，不要求 `operation_id`，不进入长期 OperationResult。

---

## 6. CommandCatalog 合同

CommandCatalog 是服务端静态注册表，不通过公网发送，也不是可由客户端修改的动态配置。
它按责任拆成共享 Schema、Gate 策略和 FirstHop 策略，避免两边各自维护一张容易漂移的表：

```text
SharedCommandSchema {
    CommandType
    PayloadType
    ResultType
    Decoder
    StructuralValidator
    RouteKeySource
    RouteKeyExtractor
}

GateCommandPolicy {
    MaxPayloadBytes
    MinProtocolVersion
    MaxProtocolVersion
    AvailabilityPolicy
    SessionAdmissionPolicy
    FirstHopRole
    RouteMode
    RouteDomain
    RetryPolicy
    RecoveryPolicy
    TimeoutBudget
    RateLimitPolicy
}

FirstHopCommandPolicy {
    IdempotencyPolicy
    OperationRootPolicy
    OperationRootExtractor
    CanonicalPayloadBuilder
    ExecutionKind
    ExecutionAcceptPolicy
    TerminalOutcomePolicy
    BusinessAdmission
    Handler
    ResultMapper
}
```

字段职责：

| 分组 | 字段 | 作用 |
|---|---|---|
| 共享解析 | PayloadType/Decoder/StructuralValidator/RouteKeySource/RouteKeyExtractor | Gate和FirstHop共享字段语义及路由提取；明确Key来自Payload还是可信上下文 |
| 演进 | MinProtocolVersion/MaxProtocolVersion/AvailabilityPolicy | 决定命令在协议版本、灰度和功能开关中的可用范围 |
| 返回 | ResultType | 决定成功或 NO_CHANGE 的业务结果类型 |
| 准入 | SessionAdmissionPolicy/RateLimitPolicy | 检查认证、恢复、Subscription 和速率 |
| Gate路由 | FirstHopRole/RouteMode/RouteDomain/RetryPolicy/RecoveryPolicy/TimeoutBudget | 决定公网第一跳、Actor 或 Service LB 路由、预算、重试和环境恢复 |
| FirstHop可靠性 | IdempotencyPolicy/OperationRootPolicy/OperationRootExtractor/CanonicalPayloadBuilder/ExecutionKind/ExecutionAcceptPolicy/TerminalOutcomePolicy | 派生Root作用域幂等键、生成权威Hash、选择 Coordinator/Actor Handler，并定义可靠接受边界和终态保存语义 |

`RouteMode` 与路由域的组合必须满足：

```text
ACTOR_AFFINITY
→ RouteDomain、RouteKeyExtractor 必须存在
→ DispatchContext 必须携带 ActorRouteHeader

SERVICE_LB
→ 不生成 ActorRouteHeader
→ RouteDomain 可以为空，由服务发现和连接池选择实例

NONE
→ 不执行后端 Actor 路由
```

`ExecutionAcceptPolicy` 定义 FirstHop 什么时候已经正式接管业务：

| 类型 | 接受边界 |
|---|---|
| `WORK_CREATED` | `Workflow CreateIfAbsent` 成功 |
| `ACTOR_MAILBOX_ACCEPTED` | 目标 Actor Mailbox 接受带幂等身份的命令 |
| `AUTHORITY_COMMITTED` | 非 Actor Authority 已原子提交权威事实/Receipt/StoredOperationOutcome |
| `FRIEND_ACTION_ADMISSION_COMMITTED` | canonical Pair Authority 已原子提交 AdmissionReceipt、ActionPermit、DispatchOrder 与 DueIndex |
| `RUNTIME_OPERATION_ACCEPTED` | Join/运行时 Handler 创建或幂等复用 Joining 等运行时操作 |
| `STATELESS_HANDLER_STARTED` | 无状态查询 Handler 已取得有界容量并正式接管 |

Route Redirect 只允许发生在对应接受边界之前；跨过边界后，调用取消或迁移只能结束当前
同步等待，不能把已经接管的业务重新投递成第二份意图。

`TerminalOutcomePolicy` 只有两种 P0 取值：

| 类型 | 语义 |
|---|---|
| `NO_DURABLE_OUTCOME` | Join 或纯查询不建立持久业务终态；按各自运行时/查询合同恢复 |
| `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 命令越过 Execution-Accept 后，确定性的 `SUCCEEDED`、`REJECTED + NONE`、`REJECTED + REFRESH_AUTHORITY_STATE`、`NO_CHANGE` 都必须按原 EffectiveIdempotencyKey 保存终态 |

临时不可用、Redirect、Deadline 超时和 `PROCESSING` 不是可被当作业务终态保存的拒绝结果；它们继续由
Workflow/Order/Unknown 恢复语义处理。确定性业务拒绝一旦保存，后续条件即使变化，同一 operation_id
仍返回原拒绝；玩家要发起新的业务意图必须使用新的 operation_id。

### 6.1 P0a～P0d 的三十三类核心命令完整 CommandSpec 矩阵

本节是三十三类公开核心命令的完整语义注册表，不只是示例。`cfg.*` 表示字段必须存在并引用版本化
配置，具体数值仍由探针冻结；实现不得因为数值未定而省略该字段。业务 Proto 的详细字段可在
第 32 号文档扩展，但下列 PayloadType/ResultType 名称、准入、路由和可靠性策略不得由 Agent
自行选择。

“完整”仅指当前已经冻结的 P0a～P0d 三十三类命令，不代表最终游戏全部命令。首屏、好友机会、任务、
邮件、宠物和图鉴均已在本版注册；Agent 不得自行注册临时 command_type 或绕过 Catalog。

`P0_BASE` 表示包含该命令的首个正式协商协议版本，是语义占位名；最终数值在 Proto/滚动兼容
评审时一次性冻结，不允许各服务自行解释成不同版本。

基础协议与容量字段：

| CommandType | PayloadType | ResultType | MaxPayloadBytes | ProtocolRange | AvailabilityPolicy | RateLimitPolicy | TimeoutBudget |
|---|---|---|---|---|---|---|---|
| `JOIN_FARM` | `JoinFarmRequest` | `NO_RESULT_PAYLOAD`；元信息在 `JoinResultContext` | `cfg.max_payload.join_farm` | `P0_BASE..OPEN_ENDED` | `feature.join_farm` | `cfg.rate.join_farm` | `cfg.timeout.join_farm` |
| `GET_PLAYER_BOOTSTRAP` | `GetPlayerBootstrapRequest` | `GetPlayerBootstrapResult` | `cfg.max_payload.get_player_bootstrap` | `P0_BASE..OPEN_ENDED` | `feature.player_bootstrap` | `cfg.rate.player_bootstrap` | `cfg.timeout.player_bootstrap` |
| `GET_FRIEND_LIST` | `GetFriendListRequest` | `GetFriendListResult` | `cfg.max_payload.get_friend_list` | `P0_BASE..OPEN_ENDED` | `feature.friend_list` | `cfg.rate.get_friend_list` | `cfg.timeout.get_friend_list` |
| `GET_NEXT_FRIEND_OPPORTUNITY` | `GetNextFriendOpportunityRequest` | `GetNextFriendOpportunityResult` | `cfg.max_payload.get_next_friend_opportunity` | `P0_BASE..OPEN_ENDED` | `feature.friend_opportunity` | `cfg.rate.friend_opportunity` | `cfg.timeout.friend_opportunity` |
| `GET_OPERATION_RESULT` | `GetOperationResultRequest` | `GetOperationResultResult` | `cfg.max_payload.get_operation_result` | `P0_BASE..OPEN_ENDED` | `feature.operation_result` | `cfg.rate.get_operation_result` | `cfg.timeout.get_operation_result` |
| `GET_TASK_LIST` | `GetTaskListRequest` | `GetTaskListResult` | `cfg.max_payload.get_task_list` | `P0_BASE..OPEN_ENDED` | `feature.task` | `cfg.rate.get_task_list` | `cfg.timeout.task_read` |
| `CLAIM_TASK` | `ClaimTaskRequest` | `ClaimTaskResult` | `cfg.max_payload.claim_task` | `P0_BASE..OPEN_ENDED` | `feature.task` | `cfg.rate.claim_task` | `cfg.timeout.task_write` |
| `CONFIRM_FRIEND_VISIT` | `ConfirmFriendVisitRequest` | `ConfirmFriendVisitResult` | `cfg.max_payload.confirm_friend_visit` | `P0_BASE..OPEN_ENDED` | `feature.task` | `cfg.rate.confirm_friend_visit` | `cfg.timeout.task_write` |
| `GET_MAIL_LIST` | `GetMailListRequest` | `GetMailListResult` | `cfg.max_payload.get_mail_list` | `P0_BASE..OPEN_ENDED` | `feature.mail` | `cfg.rate.get_mail_list` | `cfg.timeout.mail_read` |
| `GET_MAIL_DETAIL` | `GetMailDetailRequest` | `GetMailDetailResult` | `cfg.max_payload.get_mail_detail` | `P0_BASE..OPEN_ENDED` | `feature.mail` | `cfg.rate.get_mail_detail` | `cfg.timeout.mail_read` |
| `MARK_MAIL_READ` | `MarkMailReadRequest` | `MarkMailReadResult` | `cfg.max_payload.mark_mail_read` | `P0_BASE..OPEN_ENDED` | `feature.mail` | `cfg.rate.mark_mail_read` | `cfg.timeout.mail_write` |
| `DELETE_MAIL` | `DeleteMailRequest` | `DeleteMailResult` | `cfg.max_payload.delete_mail` | `P0_BASE..OPEN_ENDED` | `feature.mail` | `cfg.rate.delete_mail` | `cfg.timeout.mail_write` |
| `CLAIM_MAIL_ATTACHMENT` | `ClaimMailAttachmentRequest` | `ClaimMailAttachmentResult` | `cfg.max_payload.claim_mail_attachment` | `P0_BASE..OPEN_ENDED` | `feature.mail` | `cfg.rate.claim_mail_attachment` | `cfg.timeout.mail_claim` |
| `GET_PET_STATE` | `GetPetStateRequest` | `GetPetStateResult` | `cfg.max_payload.get_pet_state` | `P0_BASE..OPEN_ENDED` | `feature.pet` | `cfg.rate.get_pet_state` | `cfg.timeout.pet_read` |
| `PET_UNLOCK_BY_COIN` | `PetUnlockByCoinRequest` | `PetUnlockByCoinResult` | `cfg.max_payload.pet_unlock` | `P0_BASE..OPEN_ENDED` | `feature.pet` | `cfg.rate.pet_unlock` | `cfg.timeout.pet_write` |
| `PET_FEED` | `PetFeedRequest` | `PetFeedResult` | `cfg.max_payload.pet_feed` | `P0_BASE..OPEN_ENDED` | `feature.pet` | `cfg.rate.pet_feed` | `cfg.timeout.pet_write` |
| `PET_SWITCH` | `PetSwitchRequest` | `PetSwitchResult` | `cfg.max_payload.pet_switch` | `P0_BASE..OPEN_ENDED` | `feature.pet` | `cfg.rate.pet_switch` | `cfg.timeout.pet_write` |
| `GET_COMPENDIUM` | `GetCompendiumRequest` | `GetCompendiumResult` | `cfg.max_payload.get_compendium` | `P0_BASE..OPEN_ENDED` | `feature.compendium` | `cfg.rate.get_compendium` | `cfg.timeout.compendium_read` |
| `ACK_COMPENDIUM_SEEN` | `AckCompendiumSeenRequest` | `AckCompendiumSeenResult` | `cfg.max_payload.ack_compendium_seen` | `P0_BASE..OPEN_ENDED` | `feature.compendium` | `cfg.rate.ack_compendium_seen` | `cfg.timeout.compendium_write` |
| `GENERATE_SHARE_TOKEN` | `GenerateShareTokenRequest` | `ShareTokenResult` | `cfg.max_payload.share_token_write` | `P0_BASE..OPEN_ENDED` | `feature.share_friend` | `cfg.rate.share_token_write` | `cfg.timeout.friend_authority_write` |
| `ROTATE_SHARE_TOKEN` | `RotateShareTokenRequest` | `ShareTokenResult` | `cfg.max_payload.share_token_write` | `P0_BASE..OPEN_ENDED` | `feature.share_friend` | `cfg.rate.share_token_write` | `cfg.timeout.friend_authority_write` |
| `REVOKE_SHARE_TOKEN` | `RevokeShareTokenRequest` | `RevokeShareTokenResult` | `cfg.max_payload.share_token_write` | `P0_BASE..OPEN_ENDED` | `feature.share_friend` | `cfg.rate.share_token_write` | `cfg.timeout.friend_authority_write` |
| `REDEEM_SHARE` | `RedeemShareRequest` | `RedeemShareResult` | `cfg.max_payload.redeem_share` | `P0_BASE..OPEN_ENDED` | `feature.share_friend` | `cfg.rate.redeem_share` | `cfg.timeout.add_friend_workflow` |
| `DELETE_FRIEND` | `DeleteFriendRequest` | `DeleteFriendResult` | `cfg.max_payload.friend_relation_write` | `P0_BASE..OPEN_ENDED` | `feature.friend_relation_write` | `cfg.rate.friend_relation_write` | `cfg.timeout.friend_authority_write` |
| `BLOCK_PLAYER` | `BlockPlayerRequest` | `BlockPlayerResult` | `cfg.max_payload.friend_relation_write` | `P0_BASE..OPEN_ENDED` | `feature.friend_relation_write` | `cfg.rate.friend_relation_write` | `cfg.timeout.friend_authority_write` |
| `UNBLOCK_PLAYER` | `UnblockPlayerRequest` | `UnblockPlayerResult` | `cfg.max_payload.friend_relation_write` | `P0_BASE..OPEN_ENDED` | `feature.friend_relation_write` | `cfg.rate.friend_relation_write` | `cfg.timeout.friend_authority_write` |
| `BUY` | `BuyRequest` | `BuyResult` | `cfg.max_payload.buy` | `P0_BASE..OPEN_ENDED` | `feature.buy` | `cfg.rate.buy` | `cfg.timeout.buy` |
| `SELL` | `SellRequest` | `SellResult` | `cfg.max_payload.sell` | `P0_BASE..OPEN_ENDED` | `feature.sell` | `cfg.rate.sell` | `cfg.timeout.sell` |
| `UNLOCK_PLOT` | `UnlockPlotRequest` | `UnlockPlotResult` | `cfg.max_payload.unlock_plot` | `P0_BASE..OPEN_ENDED` | `feature.unlock_plot` | `cfg.rate.unlock_plot` | `cfg.timeout.unlock_plot` |
| `PLANT` | `PlantRequest` | `PlantResult` | `cfg.max_payload.plant` | `P0_BASE..OPEN_ENDED` | `feature.plant` | `cfg.rate.plant` | `cfg.timeout.plant` |
| `HARVEST` | `HarvestRequest` | `HarvestResult` | `cfg.max_payload.harvest` | `P0_BASE..OPEN_ENDED` | `feature.harvest` | `cfg.rate.harvest` | `cfg.timeout.harvest` |
| `CARE` | `CareRequest` | `CareResult` | `cfg.max_payload.care` | `P0_BASE..OPEN_ENDED` | `feature.care` | `cfg.rate.care` | `cfg.timeout.care` |
| `THEFT` | `TheftRequest` | `TheftResult` | `cfg.max_payload.theft` | `P0_BASE..OPEN_ENDED` | `feature.theft` | `cfg.rate.theft` | `cfg.timeout.theft` |

Session 准入与目标策略：

| CommandType | authenticated | recovery complete | current subscription | target policy |
|---|---:|---:|---:|---|
| `JOIN_FARM` | 是 | 否 | 否 | `PAYLOAD_TARGET` |
| `GET_PLAYER_BOOTSTRAP` | 是 | 否 | 否 | `SESSION_PLAYER`；只能读取当前玩家私有Bootstrap |
| `GET_FRIEND_LIST` | 是 | 否 | 否 | `NONE` |
| `GET_NEXT_FRIEND_OPPORTUNITY` | 是 | 否 | 否 | `NONE`；viewer只来自Session；`hinted_targets`是不可信加速输入，不是路由键或授权 |
| `GET_OPERATION_RESULT` | 是 | 否 | 否 | `SUBJECT_ROOT_OPERATION`；主体来自当前 Session，Root 由原命令与原目标/Locator派生 |
| `GET_TASK_LIST` | 是 | 是 | 否 | `SESSION_PLAYER` |
| `CLAIM_TASK` | 是 | 是 | 否 | `SESSION_PLAYER` |
| `CONFIRM_FRIEND_VISIT` | 是 | 是 | 是 | `CURRENT_FRIEND_FARM`：`target == admitted_farm != session.player_id`，且可信上下文带 relation id/epoch |
| `GET_MAIL_LIST/GET_MAIL_DETAIL/MARK_MAIL_READ/DELETE_MAIL` | 是 | 否 | 否 | `SESSION_PLAYER`；只能读取/修改当前玩家邮箱 |
| `CLAIM_MAIL_ATTACHMENT` | 是 | 是 | 否 | `SESSION_PLAYER`；mail_id必须由MailEntry recipient校验 |
| `GET_PET_STATE/GET_COMPENDIUM` | 是 | 是 | 否 | `SESSION_PLAYER`；只读当前玩家权威状态 |
| `ACK_COMPENDIUM_SEEN` | 是 | 是 | 否 | `SESSION_PLAYER`；只确认当前玩家已观察Revision |
| `PET_UNLOCK_BY_COIN/PET_FEED/PET_SWITCH` | 是 | 是 | 否 | `SESSION_PLAYER`；PlayerActor裁决资产、PetState与规则版本 |
| `GENERATE/ROTATE/REVOKE_SHARE_TOKEN` | 是 | 是 | 否 | `SESSION_PLAYER`；inviter只能来自当前Session |
| `REDEEM_SHARE` | 是 | 是 | 否 | `TOKEN_INVITER + SESSION_REDEEMER`；redeemer只能来自当前Session |
| `DELETE_FRIEND/BLOCK_PLAYER/UNBLOCK_PLAYER` | 是 | 是 | 否 | `SESSION_CALLER + PAYLOAD_PEER`；caller只能来自当前Session |
| `BUY/SELL` | 是 | 是 | 否 | `SESSION_PLAYER` |
| `UNLOCK_PLOT` | 是 | 是 | 是 | `OWN_CURRENT_FARM`：`target == admitted_farm == session.player_id` |
| `PLANT/HARVEST` | 是 | 是 | 是 | `OWN_CURRENT_FARM`：`target == admitted_farm == session.player_id` |
| `CARE/THEFT` | 是 | 是 | 是 | `CURRENT_FARM`：`target == admitted_farm`，另做好友权限裁决 |

第一跳、路由和执行接受边界：

| CommandType | RouteMode | FirstHopRole | Domain | RouteKeySource | RouteKeyExtractor | ExecutionKind | AcceptPolicy |
|---|---|---|---|---|---|---|---|
| `JOIN_FARM` | `ACTOR_AFFINITY` | `farm-svr / JoinIngress` | `FARM` | `PAYLOAD_FIELD` | `JoinFarmRequest.target_farm_id` | `DOMAIN_ROLE` | `RUNTIME_OPERATION_ACCEPTED` |
| `GET_PLAYER_BOOTSTRAP` | `ACTOR_AFFINITY` | `player-svr / PlayerBootstrapIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `GET_FRIEND_LIST` | `SERVICE_LB` | `player-svr / FriendReadAPI` | 无 | `NONE` | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `GET_NEXT_FRIEND_OPPORTUNITY` | `SERVICE_LB` | `player-svr / FriendOpportunityNavigation` | 无 | `NONE`；viewer来自Session；Hint目标仅定向精查 | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `GET_OPERATION_RESULT` | `SERVICE_LB` | `player-svr / OperationReadAPI` | 无 | `NONE`；subject 来自 Session | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `GET_TASK_LIST` | `ACTOR_AFFINITY` | `player-svr / TaskReadAPI` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `CLAIM_TASK` | `ACTOR_AFFINITY` | `player-svr / TaskCommandIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `CONFIRM_FRIEND_VISIT` | `ACTOR_AFFINITY` | `player-svr / TaskCommandIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `GET_MAIL_LIST` | `SERVICE_LB` | `player-svr / PlayerMailReadAPI` | 无 | `NONE`；subject来自Session | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `GET_MAIL_DETAIL` | `SERVICE_LB` | `player-svr / PlayerMailReadAPI` | 无 | `NONE`；subject来自Session | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `MARK_MAIL_READ` | `SERVICE_LB` | `player-svr / PlayerMailCommandAPI` | 无 | `NONE`；subject来自Session | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `DELETE_MAIL` | `SERVICE_LB` | `player-svr / PlayerMailCommandAPI` | 无 | `NONE`；subject来自Session | 无 | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `CLAIM_MAIL_ATTACHMENT` | `ACTOR_AFFINITY` | `player-svr / PlayerMailClaimIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `DOMAIN_ROLE` | `ACTOR_MAILBOX_ACCEPTED` |
| `GET_PET_STATE` | `ACTOR_AFFINITY` | `player-svr / PlayerReadAPI` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `GET_COMPENDIUM` | `ACTOR_AFFINITY` | `player-svr / CompendiumReadIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `DOMAIN_ROLE` | `STATELESS_HANDLER_STARTED` |
| `ACK_COMPENDIUM_SEEN` | `ACTOR_AFFINITY` | `player-svr / PlayerIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `PET_UNLOCK_BY_COIN/PET_FEED/PET_SWITCH` | `ACTOR_AFFINITY` | `player-svr / PlayerIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `GENERATE_SHARE_TOKEN` | `SERVICE_LB` | `player-svr / ShareTokenAPI` | 无 | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `ROTATE_SHARE_TOKEN` | `SERVICE_LB` | `player-svr / ShareTokenAPI` | 无 | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `REVOKE_SHARE_TOKEN` | `SERVICE_LB` | `player-svr / ShareTokenAPI` | 无 | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `REDEEM_SHARE` | `SERVICE_LB` | `player-svr / AddFriendCoordinator` | 无 | `NONE` | 无；FirstHop解析Token并计算canonical pair | `WORKFLOW_COORDINATOR` | `WORK_CREATED` |
| `DELETE_FRIEND` | `SERVICE_LB` | `player-svr / FriendshipPairAPI` | 无 | `NONE` | 无；FirstHop使用Session caller+payload peer计算pair | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `BLOCK_PLAYER` | `SERVICE_LB` | `player-svr / FriendshipPairAPI` | 无 | `NONE` | 无；FirstHop使用Session caller+payload peer计算pair | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `UNBLOCK_PLAYER` | `SERVICE_LB` | `player-svr / FriendshipPairAPI` | 无 | `NONE` | 无；FirstHop使用Session caller+payload peer计算pair | `DOMAIN_ROLE` | `AUTHORITY_COMMITTED` |
| `BUY` | `ACTOR_AFFINITY` | `player-svr / PlayerIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `SELL` | `ACTOR_AFFINITY` | `player-svr / PlayerIngress` | `PLAYER` | `TRUSTED_CONTEXT_FIELD` | `session.player_id` | `ACTOR_DIRECT` | `ACTOR_MAILBOX_ACCEPTED` |
| `UNLOCK_PLOT` | `ACTOR_AFFINITY` | `farm-svr / UnlockPlotCoordinator` | `FARM` | `PAYLOAD_FIELD` | `UnlockPlotRequest.target_farm_id` | `WORKFLOW_COORDINATOR` | `WORK_CREATED` |
| `PLANT` | `ACTOR_AFFINITY` | `farm-svr / PlantCoordinator` | `FARM` | `PAYLOAD_FIELD` | `PlantRequest.target_farm_id` | `WORKFLOW_COORDINATOR` | `WORK_CREATED` |
| `HARVEST` | `ACTOR_AFFINITY` | `farm-svr / HarvestCoordinator` | `FARM` | `PAYLOAD_FIELD` | `HarvestRequest.target_farm_id` | `WORKFLOW_COORDINATOR` | `WORK_CREATED` |
| `CARE` | `ACTOR_AFFINITY` | `farm-svr / FriendFarmIngress` | `FARM` | `PAYLOAD_FIELD` | `CareRequest.target_farm_id` | `DOMAIN_ROLE` | Owner：`ACTOR_MAILBOX_ACCEPTED`；好友：`FRIEND_ACTION_ADMISSION_COMMITTED` |
| `THEFT` | `ACTOR_AFFINITY` | `farm-svr / FriendFarmCoordinator` | `FARM` | `PAYLOAD_FIELD` | `TheftRequest.target_farm_id` | `WORKFLOW_COORDINATOR` | `FRIEND_ACTION_ADMISSION_COMMITTED` |

`RouteKeySource` 是必须注册的来源类型：`PAYLOAD_FIELD` 或 `TRUSTED_CONTEXT_FIELD`。BUY/SELL 的
PlayerID 只来自 `TrustedCommandContext.session.player_id`，Payload 不重复携带操作者 ID。
`ACTOR_AFFINITY` 也不等于 `ACTOR_DIRECT`：JOIN_FARM 虽是 `DOMAIN_ROLE`，仍必须先落到目标 Farm
Holder，因为 SubscriptionHub、200 人名额和 Join 运行态在那里；它不会因此进入 FarmActor 邮箱。

持久写命令的 `OperationRootPolicy`：

| 命令 | OperationRootRef | Root派生输入 |
|---|---|---|
| `BUY/SELL/CLAIM_TASK/CONFIRM_FRIEND_VISIT/PET_UNLOCK_BY_COIN/PET_FEED/PET_SWITCH` | `PLAYER_ACTOR:session.player_id` | 仅可信Session |
| `UNLOCK_PLOT/PLANT/HARVEST` | `FARM_ACTOR:target_farm_id` | 已校验Typed Payload |
| Owner `CARE` | `FARM_ACTOR:target_farm_id` | `subject == farm owner` |
| Friend `CARE/THEFT` | `FRIEND_PAIR:canonical_pair(subject,target_owner)` | Session主体+目标农场Owner |
| `DELETE_FRIEND/BLOCK_PLAYER/UNBLOCK_PLAYER/REDEEM_SHARE` | `FRIEND_PAIR:canonical_pair(subject,peer/inviter)` | Session主体+已解析peer/inviter |
| `GENERATE/ROTATE/REVOKE_SHARE_TOKEN` | `FRIEND_SHARE_ISSUER:session.player_id` | 仅可信Session；reactivation peer进入Canonical Payload |
| `CLAIM_MAIL_ATTACHMENT` | `PLAYER_MAIL_ENTRY:subject/mail_id` | Session主体+已校验mail_id；完整规则服从第35号 |

查询、Join 和可安全重入的弱写不建立持久 OperationRoot。若同一个 CommandType 存在 Owner/Friend 两条
执行链（CARE），OperationRootExtractor 必须在 Execution-Accept 前依据可信主体与目标唯一选定，后续
不能因好友关系变化改 Root。

幂等、重试与恢复：

| CommandType | IdempotencyPolicy | CanonicalPayloadBuilder | TerminalOutcomePolicy | RetryPolicy | RecoveryPolicy |
|---|---|---|---|---|---|
| `JOIN_FARM` | `RUNTIME_REQUIRED` | `JoinFarmCanonicalPayload` | `NO_DURABLE_OUTCOME` | 同连接同意图复用 PendingJoin/attempt；Redirect 保持 operation_id | `FRESH_JOIN`；不进入持久 OperationResult |
| `GET_PLAYER_BOOTSTRAP` | `NONE` | 无；空Payload，subject来自Session | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY`；按player_state_revision单调应用 |
| `GET_FRIEND_LIST` | `NONE` | 无 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY` |
| `GET_NEXT_FRIEND_OPPORTUNITY` | `NONE` | 无；viewer来自Session；可选Hint目标不参与授权或幂等身份 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY`；暂时不可用不得转为NONE |
| `GET_OPERATION_RESULT` | `NONE` | 无 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY/POLL` | 按原 operation 查询，不创建新业务意图 |
| `GET_TASK_LIST` | `NONE` | 无 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY` |
| `CLAIM_TASK` | `REQUIRED` | `ClaimTaskCanonicalPayload(task_instance_id)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳后查询/恢复原operation | `PLAYER_RETRY_SAME_OPERATION` |
| `CONFIRM_FRIEND_VISIT` | `REQUIRED` | `ConfirmFriendVisitCanonicalPayload(target_farm_id)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳前要求当前好友Subscription；接纳后查询/恢复原operation | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |
| `GET_PET_STATE/GET_COMPENDIUM` | `NONE` | 无 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY` |
| `ACK_COMPENDIUM_SEEN` | `NONE` | 无；`observed_revision`按单调Ack安全重入 | `NO_DURABLE_OUTCOME` | `SAFE_MONOTONIC_RETRY` | `REQUERY` |
| `PET_UNLOCK_BY_COIN` | `REQUIRED` | `PetUnlockByCoinCanonicalPayload(pet_id, expected_pet_config_version, expected_price_version)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳后查询/恢复原operation | `PLAYER_RETRY_SAME_OPERATION` |
| `PET_FEED` | `REQUIRED` | `PetFeedCanonicalPayload(quantity, expected_feed_rule_version, expected_active_pet_config_version, expected_guard_rule_version)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳后查询/恢复原operation；GuardSync只由Order恢复 | `PLAYER_RETRY_SAME_OPERATION` |
| `PET_SWITCH` | `REQUIRED` | `PetSwitchCanonicalPayload(target_pet_id, expected_pet_config_version, expected_guard_rule_version)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳后查询/恢复原operation | `PLAYER_RETRY_SAME_OPERATION` |
| `GET_MAIL_LIST/GET_MAIL_DETAIL` | `NONE` | 无 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY` |
| `MARK_MAIL_READ/DELETE_MAIL` | `NONE` | 无；按player_id+mail_id的单调状态安全重入 | `NO_DURABLE_OUTCOME` | `SAFE_QUERY_RETRY` | `REQUERY` |
| `CLAIM_MAIL_ATTACHMENT` | `REQUIRED` | `ClaimMailAttachmentCanonicalPayload(mail_id)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Mailbox接纳后只查询/恢复原operation；换operation重领由MailClaimKey拒绝 | `PLAYER_RETRY_SAME_OPERATION` |
| `GENERATE_SHARE_TOKEN` | `REQUIRED` | `GenerateShareTokenCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 保持原operation；Authority Unknown先解析Receipt/Outcome | `PLAYER_RETRY_SAME_OPERATION` |
| `ROTATE_SHARE_TOKEN` | `REQUIRED` | `RotateShareTokenCanonicalPayload(optional reactivation_peer_player_id)` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 同上 | `PLAYER_RETRY_SAME_OPERATION` |
| `REVOKE_SHARE_TOKEN` | `REQUIRED` | `RevokeShareTokenCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 同上 | `PLAYER_RETRY_SAME_OPERATION` |
| `REDEEM_SHARE` | `REQUIRED` | `RedeemShareCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Workflow创建后只查询/恢复原operation | `PLAYER_RETRY_SAME_OPERATION` |
| `DELETE_FRIEND` | `REQUIRED` | `DeleteFriendCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Pair Commit后只查询原Outcome；NO_CHANGE同样持久 | `PLAYER_RETRY_SAME_OPERATION` |
| `BLOCK_PLAYER` | `REQUIRED` | `BlockPlayerCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 同上 | `PLAYER_RETRY_SAME_OPERATION` |
| `UNBLOCK_PLAYER` | `REQUIRED` | `UnblockPlayerCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 同上 | `PLAYER_RETRY_SAME_OPERATION` |
| `BUY/SELL` | `REQUIRED` | `BuyCanonicalPayload` / `SellCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 接受前可纠正路由；业务重试保持 operation_id | `PLAYER_RETRY_SAME_OPERATION` |
| `UNLOCK_PLOT` | `REQUIRED` | `UnlockPlotCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 接受前可纠正路由；Workflow创建后只查询/恢复 | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |
| `PLANT` | `REQUIRED` | `PlantCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 接受前可纠正路由；Workflow 创建后只查询/恢复 | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |
| `HARVEST` | `REQUIRED` | `HarvestCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | 同上 | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |
| `CARE` | `REQUIRED` | `CareCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Owner入Mailbox、好友Admission提交后，只查询/恢复原operation | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |
| `THEFT` | `REQUIRED` | `TheftCanonicalPayload` | `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` | Admission提交后只查询/恢复；Workflow由可靠Dispatch创建 | `FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION` |

CARE 不创建独立 CareWorkflow。Owner 护理的接受边界仍是 FarmActor Mailbox；好友护理先由 Pair
Authority 原子提交 ActionAdmission 与可靠 DispatchOrder，该提交就是接受边界。随后 Farm Authority
仍原子提交护理事实和 CareRewardOrder。两条分支由可信 viewer/owner 关系选择，不能由 H5 自报。

好友公开命令的最小字段与稳定错误符号由第 33 号业务语义约束：

```text
Generate/Rotate/RevokeShareTokenRequest
→ 不携带inviter_player_id，主体来自Session

RedeemShareRequest
→ opaque_share_token

DeleteFriend/BlockPlayer/UnblockPlayerRequest
→ peer_player_id
```

至少注册以下 `BUSINESS_FRIEND_*` 稳定错误：

```text
SHARE_TOKEN_INVALID
SHARE_TOKEN_EXPIRED
SHARE_TOKEN_REVOKED
SHARE_TOKEN_PREDATES_RELATION_DELETE
RELATION_REACTIVATION_PENDING
REACTIVATION_ROTATE_REQUIRED
SELF_FRIEND_FORBIDDEN
ALREADY_FRIENDS
FRIEND_LIMIT_REACHED
RELATION_CLEANUP_PENDING
NOT_FRIENDS
ALREADY_BLOCKED
ALREADY_UNBLOCKED
VISIT_NOT_PERMITTED
```

`ALREADY_FRIENDS/NOT_FRIENDS/ALREADY_BLOCKED/ALREADY_UNBLOCKED` 对应确定性 `NO_CHANGE`；Token 无效、
上限和权限类错误对应确定性 `REJECTED`；已存在可恢复清理或 AddFriendWorkflow 时返回 `PROCESSING`。
它们一旦越过各自 Execution-Accept，必须保存原 `StoredOperationOutcome`。

`RELATION_REACTIVATION_PENDING` 表示删除已生效、Generation Floor 尚未回填完成。对显式
`RotateForPairReactivation` 它是 Execution-Accept 前的临时拒绝，可以使用同一 operation_id 延迟重试；
对旧分享链接的 `REDEEM_SHARE`，客户端不自动循环重试，而是等待邀请者显式轮换并使用新链接发起新操作。
`REACTIVATION_ROTATE_REQUIRED` 表示 Barrier 已 READY，但当前 Token 没有绑定本次
barrier_revision 的 RotateReceipt；它与 `SHARE_TOKEN_PREDATES_RELATION_DELETE` 都不能通过原 Token 重试修复。

好友机会查询还必须注册以下稳定失败语义：

| 条件 | CommandStatus / ErrorCode | RetryDirective |
|---|---|---|
| FriendIndex、Farm Header/Facts、Friendship Authority 或 FarmRead 依赖暂时不可用，无法证明当前是否存在候选 | `REJECTED / FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE` | `SAFE_QUERY_RETRY` |

该错误属于无长期 Outcome 的查询失败，不写 OperationResult。它和成功结果中的 `NONE` 严格不同：
`NONE` 表示服务端完成了本轮有界扫描与精确检查后确认没有候选；暂时不可用只表示本次无法得出结论，
H5 应保留好友入口并显示 Loading/Retry，不能缓存成“没有互动机会”。

任务公开命令的稳定终态至少注册：

| 条件 | CommandStatus / ErrorCode | RetryDirective |
|---|---|---|
| 领取成功 | `SUCCEEDED` | `NONE` |
| 另一 operation 重领已领取实例 | `NO_CHANGE / TASK_ALREADY_CLAIMED` | `NONE` |
| 任务未完成 | `REJECTED / TASK_NOT_COMPLETED` | `NONE` |
| 每日实例跨期或活动领取期已过 | `REJECTED / TASK_INSTANCE_EXPIRED` | `REFRESH_AUTHORITY_STATE` |
| 实例不存在或不属于当前玩家 | `REJECTED / TASK_INSTANCE_NOT_FOUND` | `NONE` |
| 同 operation 改换任务实例或目标农场 | `REJECTED / IDEMPOTENCY_CONFLICT` | `NONE` |
| 本日访问任务已经被另一合法 witness 满足 | `NO_CHANGE / TASK_ALREADY_SATISFIED` | `NONE` |
| 访问发生于已经关闭的每日周期 | `NO_CHANGE / TASK_EVENT_EXPIRED` | `NONE` |
| 访问确认没有当前好友 Subscription/关系 Fence | `REJECTED / FRIEND_VISIT_CONTEXT_INVALID` | `FRESH_JOIN_THEN_RETRY` |
| Authority Commit 结果未知 | `PROCESSING` | `POLL_OPERATION_RESULT` |
| PlayerActor Mailbox 尚未接纳 | `REJECTED` | `RETRY_SAME_OPERATION` |

`CLAIM_TASK` 与 `CONFIRM_FRIEND_VISIT` 越过 `ACTOR_MAILBOX_ACCEPTED` 后，确定性 `NO_CHANGE` 或
`REJECTED + NONE/REFRESH_AUTHORITY_STATE` 都必须保存当前 operation 的 Outcome；MailBox 接纳前的
临时错误不保存为业务终态。`GET_TASK_LIST` 必须进入 PlayerActor 执行 `EnsureDailyPeriod`，不得用红点
Projection 冒充权威列表。

邮件公开 Payload 的最小字段冻结为：

```text
GetMailListRequest        → optional opaque_cursor；page_size由服务端固定为6
GetMailDetailRequest      → mail_id
MarkMailReadRequest       → mail_id
DeleteMailRequest         → mail_id
ClaimMailAttachmentRequest→ mail_id
```

五类 Payload 均不携带 player_id；主体来自当前 Session。尤其 `ClaimMailAttachmentRequest` 不得重复
携带 operation_id、RewardBundle、attachment_hash、claim_material_hash 或 Redis/Actor 地址。

邮件稳定终态至少注册：

| 条件 | CommandStatus / ErrorCode | RetryDirective |
|---|---|---|
| 附件领取成功 | `SUCCEEDED` | `NONE` |
| 另一 operation 已领取同一邮件 | `NO_CHANGE / MAIL_ALREADY_CLAIMED` | `NONE` |
| 同 operation 改换 mail_id | `REJECTED / IDEMPOTENCY_CONFLICT` | `NONE` |
| 邮件不存在或不属于当前玩家 | `REJECTED / MAIL_NOT_FOUND_OR_NOT_OWNED` | `NONE` |
| 邮件无附件 | `REJECTED / MAIL_HAS_NO_ATTACHMENT` | `NONE` |
| 领取期限已过 | `REJECTED / MAIL_CLAIM_EXPIRED` | `REFRESH_AUTHORITY_STATE` |
| 删除尚未领取的有效附件 | `REJECTED / MAIL_HAS_UNCLAIMED_ATTACHMENT` | `NONE` |
| MailEntry/ClaimState读取暂时未知 | `PROCESSING` | `RETRY_OR_POLL` |
| Player Authority Commit结果未知 | `PROCESSING` | `POLL_OPERATION_RESULT` |

Material issuer/hash/内部 Schema 不匹配属于服务端合同错误，外部统一返回安全的 `INTERNAL_UNAVAILABLE`
并告警，不能向 H5 暴露签名、Hash 预像或内部 Storage Key。CLAIM 越过 Mailbox 接受边界后的确定终态
必须保存；列表/详情/已读/删除按第 35/35A 号的单调状态安全重入，不进入持久 OperationResult。

宠物与图鉴公开结果和错误符号：

| 场景 | CommandStatus / ErrorCode | RetryDirective |
|---|---|---|
| 宠物状态或图鉴查询成功 | `SUCCEEDED` | `NONE` |
| 宠物已解锁、目标宠物已在守护 | `NO_CHANGE / PET_ALREADY_UNLOCKED` 或 `NO_CHANGE / PET_ALREADY_ACTIVE` | `NONE` |
| 金币不足 | `REJECTED / INSUFFICIENT_COIN` | `NONE` |
| 宠物不存在、未解锁或不可用 | `REJECTED / PET_NOT_FOUND`、`PET_NOT_UNLOCKED` 或 `PET_NOT_AVAILABLE` | `NONE` |
| 狗粮不足 | `REJECTED / INSUFFICIENT_DOG_FOOD` | `NONE` |
| 喂食数量、24小时窗口或checked arithmetic非法 | `REJECTED / PET_FEED_LIMIT_EXCEEDED` | `NONE` |
| 价格/配置/喂食/守护规则版本过时 | `REJECTED / PET_PRICE_CHANGED`、`PET_CONFIG_VERSION_CHANGED`、`PET_FEED_RULE_CHANGED` 或 `PET_GUARD_RULE_CHANGED` | `REFRESH_AUTHORITY_STATE` |
| 必达GuardSyncOrder/DueIndex容量不足 | 临时 `REJECTED / WORK_CAPACITY_EXCEEDED`；不保存业务终态 | `RETRY_SAME_OPERATION` |
| 同一 operation_id 改换宠物、数量或规则版本 | `REJECTED / IDEMPOTENCY_CONFLICT` | `NONE` |
| PlayerActor 已提交，GuardSync尚未完成 | 原宠物命令仍为 `SUCCEEDED`；`GET_PET_STATE.guard_sync_status=PENDING` | 不重放业务命令；后台恢复原Order |
| 图鉴Seen水位首次推进 | `SUCCEEDED` | `NONE` |
| 重复/旧图鉴Ack没有推进水位 | `NO_CHANGE / COMPENDIUM_SEEN_ALREADY_ADVANCED` | `NONE` |

图鉴不存在单独的解锁写命令；`GET_COMPENDIUM` 只读当前 PlayerActor 权威集合。活动植物结束后，
`archive_state` 和 `legacy_seed_plantable` 由服务端 PlantCatalog 决定；客户端不能通过查询请求修改解锁状态。

### 6.2 Plant 展开示例

```text
PLANT
├── PayloadType：PlantRequest
├── ResultType：PlantResult
├── MaxPayloadBytes：配置值
├── MinProtocolVersion：配置值
├── MaxProtocolVersion：配置值或OPEN_ENDED
├── AvailabilityPolicy：FeatureGate键
├── requires_authenticated：true
├── requires_recovery_complete：true
├── requires_current_subscription：true
├── target_policy：OWN_CURRENT_FARM
├── ExecutionKind：WORKFLOW_COORDINATOR
├── ExecutionAcceptPolicy：WORK_CREATED
├── TerminalOutcomePolicy：DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT
├── FirstHopRole：farm-svr / PlantCoordinator
├── RouteMode：ACTOR_AFFINITY
├── RouteDomain：FARM
├── RouteKeySource：PAYLOAD_FIELD
├── RouteKeyExtractor：PlantRequest.target_farm_id
├── IdempotencyPolicy：REQUIRED
├── CanonicalPayloadBuilder：PlantCanonicalPayload
├── RetryPolicy：SAME_OPERATION_ID
├── RecoveryPolicy：FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION
├── TimeoutBudget：配置值
└── RateLimitPolicy：配置值
```

处理流程：

```text
收到ClientCommandEnvelope
→ 根据command_type查CommandSpec
→ 校验protocol_version和AvailabilityPolicy
→ 检查payload大小
→ 按PayloadType解码
→ 校验字段、SessionAdmissionPolicy和RateLimitPolicy
→ 提取RouteKey
→ 选择FirstHopRole
→ 取得BackendPool有界容量
→ 注入TrustedCommandContext
→ 标记ADMITTED
→ 进入内部RPC
```

CommandCatalog 只负责命令解释和第一跳；PlantCoordinator、PlayerActor 和 FarmActor 继续按
第 27 号文档完成 Workflow 和最终裁决。`ExecutionKind` 由 FirstHopCommandPolicy 在
FirstHop 选择，不由 Gate 执行业务编排。

### 6.3 ADMITTED 线性化边界

公网命令只有依次完成以下步骤后才成为 `ADMITTED`：

```text
FarmFrame合法
→ 方向和sequence合法
→ CommandType已注册
→ 当前protocol_version与AvailabilityPolicy允许
→ payload大小合法
→ Typed Payload解码和结构校验成功
→ SessionAdmissionPolicy通过
→ RouteKey提取和目标约束通过
→ RateLimitPolicy通过
→ FirstHop BackendPool取得有界容量
→ ADMITTED
```

语义：

- ADMITTED 之前拒绝：后端没有接管请求，不得创建业务事实；客户端可以根据错误重新修正或
  重新发起。
- ADMITTED 之后：即使命令结果晚到、页面切换或连接断开，也必须按 `request_id /
  operation_id` 产生可解释结果；不能因为旧 Subscription 已失效就静默丢弃 CommandResult。
- “写入某个 Go channel”本身不等于 ADMITTED。必须确认目标 FirstHop 的有界请求槽位已经
  取得，避免 Gate 无界堆积。
- 具体业务的 `ACCEPTED / PROCESSING / SUCCEEDED` 边界仍由 CommandSpec 与第 32 号场景合同
  定义；ADMITTED 只是接入层正式接管边界。

### 6.4 完整性测试

CI 必须自动证明：

1. 每个公开 `CommandType` 恰好注册一个 `CommandSpec`。
2. 每个 CommandSpec 都声明 PayloadType、ResultType、Admission、Route、ExecutionAccept、
   TerminalOutcome、Retry、Timeout 和 RateLimit。
3. 不存在重复 CommandType、缺失 Decoder 或未注册公开命令。
4. 每个 Actor Affinity 命令显式声明 RouteKeySource；RouteKeyExtractor 有正常、缺字段、零 ID 和
   超范围输入测试，TrustedContext 来源不得回退读取 Payload 中的操作者 ID。
5. `command_type + malformed payload` 在 Gate 拒绝，不进入后端。
6. Payload 超过命令级上限时快速拒绝。
7. UnlockPlot/Plant/Harvest 不能在订阅好友农场时绕过 `OWN_CURRENT_FARM`。
8. Care/Theft 的 target 必须等于当前 Subscription 的 farm_id。
9. 每个需要幂等的命令在 FirstHopCommandPolicy 中都有 CanonicalPayloadBuilder，并验证同语义
   不同合法编码得到相同 Hash。
10. 每个命令声明协议版本范围和 AvailabilityPolicy；未开放命令在 Gate 拒绝。
11. 只有完整通过 ADMITTED 流程的请求才允许进入 FirstHop。
12. `DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` 命令的确定性 REJECTED/NO_CHANGE 重试返回原终态；
    临时错误和 PROCESSING 不写成持久业务拒绝。
13. FirstHop 解码后验证 Payload 目标与服务端 RouteHeader 一致。
14. UnlockPlot 的等级、价格和前置 Plot 不得来自客户端；相同 operation_id 更换 Plot 或规则版本必须
    返回 `IDEMPOTENCY_CONFLICT`。

---

## 7. ID 与浏览器表示

```text
数值实体ID与单调版本
→ 服务端Protobuf/Go/路由/存储使用uint64或uint32
→ 例如player_id、farm_id、farm_version、record_version、epoch

H5业务模型 / JSON调试输出中的上述数值
→ 十进制字符串、BigInt或安全Long映射

不透明身份、幂等ID与运行时Fence
→ 有界string或bytes
→ 例如request_id、operation_id、subscription_id、join_attempt_id、workflow_id、order_id、
  crop_cycle_id、incident_id、writer_claim_id
```

H5 可使用 protobuf runtime 的 Long、BigInt 或字符串映射，但禁止：

```javascript
Number(uint64String)
```

数值 ID 和版本一旦经过不安全的 JavaScript Number，会导致路由、版本比较和日志关联错误；
不透明字符串 ID 则只能做相等性、长度与格式校验，禁止按大小推断新旧关系。

---

## 8. 与当前旧 Proto 的迁移差异

现有旧 `ClientCommandEnvelope` 只能作为迁移输入。新版应从公共 Envelope 移除：

```text
protocol_version
client_seq
target_owner_id
expected_farm_version
expected_plot_version
expected_crop_cycle_id
```

迁移到：

```text
protocol_version / sequence
→ FarmFrame或握手层

target / expected fields
→ 具体Typed Payload
```

新版增加 `request_id`，并保留 `operation_id`。实际 `.proto` 和生成代码必须等本文件剩余下行、
错误与演进合同确认后一次性更新，当前不做半套兼容实现。

---

## 9. 稳定语义合同编号（候选版）

以下编号表示实现不得自行改变的稳定语义；本文仍是 freeze-candidate。最终 Proto field number、
`reserved`、字节上限和滚动兼容参数通过生成代码与兼容性测试后，才提升为正式线网合同。

```text
L2-PROTO-01
一个WebSocket Binary Message只包含一个FarmFrame。

L2-PROTO-02
FarmFrame顶层oneof区分稳定消息大类；业务命令不进入大oneof。

L2-PROTO-03
ClientCommandEnvelope只含request_id、operation_id、command_type和bytes payload。

L2-PROTO-04
连接sequence与protocol_version属于FarmFrame/握手层，不进入业务Envelope。

L2-PROTO-05
操作者身份、Session Fence、Subscription和Actor Route只能由服务端可信上下文提供。

L2-PROTO-06
业务目标和expected version进入Typed Payload，CommandCatalog负责解码和RouteKey提取。

L2-PROTO-07
CommandCatalog必须完整声明解析、准入、路由、可靠性和限流策略，并通过完整性测试。

L2-PROTO-08
数值实体ID和单调版本在服务端使用uint64/uint32，H5使用十进制字符串或安全Long；不透明身份、
幂等ID和运行时Fence使用有界string/bytes，禁止把两类ID混为同一种数值类型。

L2-PROTO-09
CommandResult使用ACCEPTED、SUCCEEDED、PROCESSING、REJECTED和NO_CHANGE五种状态，
并通过RetryDirective声明客户端下一步动作。

L2-PROTO-10
ServerPush使用由push_context oneof表达的SESSION、PLAYER和FARM_SUBSCRIPTION Scope；只有
farm_subscription Push受subscription_id Fence约束。

L2-PROTO-11
FarmFrame sequence按每连接、每方向从1开始严格连续；它只做连接协议检测，不承担业务幂等。

L2-PROTO-12
request_id始终必填；operation_id按IdempotencyPolicy要求。两者都是最长64字节的不透明
ASCII字符串。

L2-PROTO-13
幂等PayloadHash必须由FirstHop按每条命令声明的语义字段生成并成为唯一权威；禁止直接Hash
原始Proto bytes、依赖重新Marshal或要求Gate与FirstHop比较Hash。

L2-PROTO-14
Gate解码、准入、路由并保留原始payload bytes；FirstHop再次解码、复核RouteHeader并生成
权威Hash，Gate不重新编码Typed Payload。

L2-PROTO-15
CommandSpec必须声明协议版本与AvailabilityPolicy；只有取得FirstHop有界容量后命令才进入
ADMITTED。

L2-PROTO-16
P0中own_farm_id等于player_id，但PLAYER与FARM保持独立路由域、Shard空间和Actor权威。

L2-PROTO-17
可恢复写命令通过original_command_type和original_operation_id查询；查询主体只能来自当前
受信Session。

L2-PROTO-18
OperationLookupStatus为NOT_FOUND、PROCESSING、COMPLETED和EXPIRED；NOT_FOUND不证明原命令
未执行，只允许使用原operation_id重发。

L2-PROTO-19
JoinFarm和纯查询不进入持久OperationResult；EXPIRED不得自动重放旧写命令。

L2-PROTO-20
CommandStatus描述本次执行尝试，RetryDirective描述原用户意图的下一步；REJECTED可以与
RETRY_SAME_OPERATION组合，但只允许在确认未进入业务执行边界时使用。

L2-PROTO-21
NOT_FOUND恢复必须服从原CommandSpec RecoveryPolicy；农场命令切换页面后应先Fresh Join原目标
再用原operation_id和原Payload重试。

L2-PROTO-22
H5 SDK在终态前必须保留command_type、operation_id、原Typed Payload和原target_farm_id，
不能只保存operation_id。

L2-PROTO-23
GateCommandPolicy必须声明RouteMode；ACTOR_AFFINITY必须携带ActorRouteHeader，SERVICE_LB
和NONE不得伪造Actor Route。FirstHopCommandPolicy必须声明ExecutionAcceptPolicy。

L2-PROTO-24
CommandResult使用互斥的FarmResultContext/JoinResultContext；ServerPush使用互斥的
SessionPushContext/PlayerPushContext/FarmSubscriptionPushContext；公共Envelope不再定义泛化
farm_version，版本进入Typed Result/Push Payload。

L2-PROTO-25
持久化OperationResult使用StoredOperationOutcome，不嵌套完整CommandResultEnvelope；原
request_id、旧Subscription、join_attempt_id和临时RetryDirective不进入持久业务结果。
StoredOperationOutcome只使用不含subscription_id的StoredFarmOutcomeContext，且status只能是
SUCCEEDED、REJECTED或NO_CHANGE。

L2-PROTO-26
服务端sequence进入ConnectionWriter FIFO后不得选择性删除、重排或合并；所有背压决策必须
发生在sequence分配前。


L2-PROTO-27
JOIN_FARM的ACCEPTED只使用JoinResultContext携带元信息，result_payload为空且不得携带
FarmSnapshot；FARM_SNAPSHOT ServerPush是唯一Snapshot路径。

L2-PROTO-28
join_attempt_id由Gate生成，绑定PendingJoin、JoinResultContext、FarmSnapshot、Catch-up、Ready
和全部QueueAccepted；客户端不能生成、覆盖或跨Join复用。

L2-PROTO-29
H5对可恢复写命令在第一次发送前持久保存原Typed Payload、command_type、operation_id和原目标；
TrustedCommandContext、Permit和RouteHeader不得进入Journal。

L2-PROTO-30
Gate与H5都严格校验本方向sequence为last+1；连接sequence空洞要求重连，farm_version空洞要求
Fresh Join。

L2-PROTO-31
普通农场CommandResult不得携带完整FarmSnapshot或Plot Patch；完整农场状态只通过带
FarmSubscription Fence的Snapshot/Delta/FarmChange Push下发。

L2-PROTO-32
每个公开PushType必须注册唯一PushSpec，声明PayloadType、RequiredContextKind、大小与丢弃/合并
策略；FarmSnapshotPush、FarmDeltaBatchPush与SubscriptionReadyPush必须携带join_attempt_id。

L2-PROTO-33
H5应用FarmSnapshot后只进入CATCHING_UP；收到匹配SubscriptionReady并连续应用到
ready_through_version后，才允许叠加Recovery/Bootstrap屏障进入FARM_INTERACTIVE。

L2-PROTO-34
公开下行顺序必须为ACCEPTED→FARM_SNAPSHOT→FARM_DELTA_BATCH*→SUBSCRIPTION_READY；内部
独立RPC不得让Snapshot越过ACCEPTED。

L2-PROTO-35
operation_id标识客户端Join意图，join_attempt_id标识当前连接上的运行时attempt；同一PendingJoin
重试复用attempt，新WSS或Fresh Join生成新attempt。

L2-PROTO-36
H5收到Ready(N)时只有applied_farm_version==N才能完成Ready屏障；小于N有界等待前序Delta并在
超时后Fresh Join，大于N视为顺序/重复应用错误并立即Resync。

L2-PROTO-37
CommandCatalog具体Schema和每个公开CommandType的完整CommandSpec以本文为唯一权威；第27号
文档只拥有命令到领域、权威和Workflow的语义矩阵，不得实现第二套Catalog。

L2-PROTO-38
JOIN_FARM虽然ExecutionKind为DOMAIN_ROLE，RouteMode仍必须为ACTOR_AFFINITY，并按
FARM:target_farm_id落到目标Holder的JoinIngress/Hub；ACTOR_AFFINITY不等于ACTOR_DIRECT。

L2-PROTO-39
BUY/SELL的RouteKeySource为TrustedCommandContext.session.player_id；客户端Payload不得重复提交
操作者ID。农场命令RouteKeySource为Payload.target_farm_id并与admitted Farm Context复核。

L2-PROTO-40
FarmDeltaBatchPush必须携带join_attempt_id、from_version、through_version和严格连续的FarmChange；
空批、空洞、重复、乱序或首尾版本不匹配均要求Fresh Join。

L2-PROTO-41
内部实时失效只使用RESYNC_REQUIRED；公网只存在REJOIN_REQUIRED PushType，H5动作统一为Fresh Join。
终止Push使用RejoinRequiredPush；invalid_from_version只供诊断，不能用于增量续传。终止Push入队或
连接不可达前，Gate必须保留可校验的只读旧Subscription Fence；终止帧无法入队时关闭连接。

L2-PROTO-42
H5 Operation Journal必须受单条、条目数、总字节和保留窗口约束；未决记录不得静默淘汰，容量
不足或恢复超过最大窗口时停止新的可恢复写命令但继续恢复已有记录，Schema迁移失败必须Fail
Closed；保留窗口不是把未决结果自动判为EXPIRED的TTL。

L2-PROTO-43
CARE按可信操作者分支冻结接受边界：Owner护理为ACTOR_MAILBOX_ACCEPTED，好友护理为
FRIEND_ACTION_ADMISSION_COMMITTED；THEFT固定为FRIEND_ACTION_ADMISSION_COMMITTED。好友Admission
提交后不得再返回Redirect或把同operation当作未接管重投。

L2-PROTO-44
IdempotencyPolicy=REQUIRED的写命令越过Execution-Accept后，确定性的SUCCEEDED、
REJECTED+NONE、REJECTED+REFRESH_AUTHORITY_STATE和NO_CHANGE必须按原幂等键保存终态；
临时不可用、Redirect、PROCESSING和TIMEOUT_UNKNOWN不得伪装成持久业务拒绝。同operation_id
后续始终返回原终态。

L2-PROTO-45
本节完整CommandSpec覆盖当前P0a～P0d首屏、经营、好友、任务、邮件、宠物与图鉴的三十三类命令，均已正式登记。正式生成的.proto
文件是field number与reserved的唯一权威；文档中的逻辑IDL数字在该闸门通过前不授权多Agent各自固化。

L2-PROTO-46
GetOperationResult的StoredOperationOutcome不持久化旧连接上的RetryDirective；
COMPLETED响应必须由服务端ErrorRecoveryPolicy按终态ErrorCode重建completed_retry_directive。
第32号第10.6节是七个核心写命令ErrorCode、Directive与H5 Journal动作的唯一场景权威。

L2-PROTO-47
FARM_DELTA_BATCH与FARM_CHANGE复用第32号唯一FarmChange结构；前者只做Join状态追赶且抑制历史
动画，后者只允许在SubscriptionReady之后出现，并要求farm_version等于本地已应用版本加一。

L2-PROTO-48
FARM_CHANGE使用FarmSubscriptionPushContext，DropPolicy为NEVER_SILENT_DROP，CoalescingPolicy
为NONE。无法投递时只失效受影响Subscription并要求Fresh Join，不得回滚Farm权威提交或阻塞
其他连接。

L2-PROTO-49
FarmChange公共Payload不得携带viewer个性化字段、operation_id或私有资产结果；同一协议版本可
编码一次并按Gateway复用，目标连接身份和Subscription Fence只存在于各自Push Context。

L2-PROTO-50
FRIEND_ACTION_ADMISSION_COMMITTED表示Pair Authority已原子保存AdmissionReceipt、ActionPermitSnapshot、
FriendActionDispatchOrder和同桶DueIndex；它不是普通关系查询成功，也不要求H5传递Permit。

L2-PROTO-51
GET_TASK_LIST必须按PLAYER Route Domain进入PlayerActor执行EnsureDailyPeriod并返回权威TaskListView；
红点Projection只能用于入口摘要，不能冒充任务列表、完成状态或领奖前置真值。

L2-PROTO-52
CLAIM_TASK的Typed Payload只包含task_instance_id；operation_id只存在于ClientCommandEnvelope。
CLAIM_TASK与CONFIRM_FRIEND_VISIT均为REQUIRED幂等写，越过Mailbox接受边界后的确定终态必须保存。

L2-PROTO-53
CONFIRM_FRIEND_VISIT只允许在SUBSCRIPTION_READY后由H5自动发送。Gate必须从当前ActiveSubscription
投影target farm、subscription激活时间和authorization relation fence；H5不得提交或覆盖这些证明。
同日另一合法访问只产生NO_CHANGE/TASK_ALREADY_SATISFIED，不与首个witness发生幂等Hash冲突。

L2-PROTO-54
持久写命令使用EffectiveIdempotencyKey(subject_player_id, command_type, operation_id,
OperationRootRef)。Root由CommandCatalog的OperationRootExtractor从可信Session和已校验目标派生；
GetOperationResult必须携带原业务目标或服务端签发的不透明Locator，客户端不得直接指定内部Authority
ID、Bucket或WorkAddress。H5 Journal在终态前必须保留同一Root所需的原目标与Locator。

L2-PROTO-55
CLAIM_MAIL_ATTACHMENT的Typed Payload只含mail_id，operation_id只在公共Envelope；命令按PLAYER
Actor Affinity先到PlayerMailClaimIngress，读取MailEntry并构造TrustedMailClaimMaterial后才进入
PlayerActor Mailbox。GET_MAIL_LIST/DETAIL、MARK_MAIL_READ和DELETE_MAIL使用SERVICE_LB，不激活Actor。

L2-PROTO-56
ROTATE_SHARE_TOKEN普通轮换不携带peer；用于删除/拉黑后重新加好友时，客户端必须显式携带
reactivation_peer_player_id。FirstHop从当前Session和peer派生canonical Pair，只在当前
ReactivationBarrier=READY时调用RotateForPairReactivation。H5不提交pair_id、barrier_revision、
generation floor或RotateReceipt；这些均为服务端受信字段。

L2-PROTO-57
CommandResultEnvelope可携带服务端签发的不透明server_operation_locator。对REDEEM_SHARE，只要已越过
Execution-Accept并返回ACCEPTED/PROCESSING，必须一并返回Locator；H5保存Locator后立即清除
原始Bearer Token。Locator只定位原Root/来源事实，不代替当前Session授权。

L2-PROTO-58
GET_PET_STATE、PET_UNLOCK_BY_COIN、PET_FEED、PET_SWITCH均使用PLAYER Route Domain；宠物不创建独立
Actor或公网宠物服务。宠物写命令的操作者只能来自Trusted Session，规则版本和扣款/狗粮事实由PlayerActor
裁决；PET_UNLOCK_BY_TASK只作为任务RewardBundle的内部结果，不开放独立公网命令。

L2-PROTO-59
GET_COMPENDIUM使用PLAYER Route Domain进入CompendiumReadIngress；Ingress捕获本地不可变Catalog，短暂读取
PlayerActor的CompendiumState后在Actor Turn外组装页面。首次自种成功Harvest的图鉴解锁通过HarvestResult返回并与
PlayerActor提交保持幂等。图鉴查询不得以延迟Projection冒充权威。

L2-PROTO-60
ACK_COMPENDIUM_SEEN只携带observed_revision，按max(current_seen,min(observed,current_revision))单调推进；
GET_COMPENDIUM不得隐式清除红点。Ack不要求Farm Subscription，不进入长期Operation Journal，重复或旧Ack返回
NO_CHANGE；客户端不得让seen_revision回退或借迟到Ack清除尚未观察的新Revision。

L2-PROTO-61
HarvestResult的图鉴身份统一使用unlocked_crop_definition_id；首次GRANT_HARVEST_REWARD提交产生的
newly_unlocked、可选crop_definition_id和compendium_revision必须作为原StoredOperationOutcome恢复。
同一operation重试不得按当前图鉴集合重算，也不得使用plant_id、display_definition_id或activity实例ID。

L2-PROTO-62
GET_PLAYER_BOOTSTRAP使用PLAYER Actor Affinity进入PlayerBootstrapIngress；它不要求Recovery完成或当前
FarmSubscription，重复查询安全且不建立长期OperationResult。Bootstrap只返回玩家私有状态和摘要，
不得携带FarmSnapshot/Plot/Crop/FarmVersion，也不得依赖未登记Player Push作为唯一完成路径。

L2-PROTO-63
GET_NEXT_FRIEND_OPPORTUNITY使用SERVICE_LB进入FriendOpportunityNavigation；viewer只来自当前Session。
FOUND必须携带经精确检查的候选，NONE必须是明确无机会，依赖失败固定返回
FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE并允许安全重查。Ready后的Navigation visited是服务端运行时
更新，不替代CONFIRM_FRIEND_VISIT任务命令。

L2-PROTO-63A
GET_NEXT_FRIEND_OPPORTUNITY允许携带最多16个FriendOpportunityHintedTarget。它们必须被视为客户端可篡改的
失效提示：服务端重新校验canonical FriendRelation、PublicProfile、Farm Header/Facts和viewer Receipt。
只有精查READY且authority revision不低于Hint revision时才能返回FOUND；权威revision尚未追上时返回
FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE。CandidateWindow变更与本次回答必须由同一次Navigation CAS发布。
```

---

## 10. 后续文档与待探针项

以下不再属于本文件的业务语义争议，分别进入后续合同或基础设施探针：

1. 第 29 号文档已经拥有内部 RPC、可信调用身份、RouteHeader、FirstHop 二次校验和
   TIMEOUT_UNKNOWN 接缝；后续修改必须保持双向一致但不得复制结构；
2. [第 30 号](./30-actor-authority-store-contract.md) 已冻结 Actor CAS、WriterEpoch、WriterClaimID、
   Unknown Resolver 和原子 Intent 语义；第 31 号只继续冻结 Redis Key、Cluster Slot 和 Lua；
3. 第 32 号文档已经冻结七个核心写命令的场景 Result、Workflow 步骤、ErrorCode、
   RetryDirective、OperationResult/H5 Journal 映射，以及 FarmChange/PlotDelta 的业务字段与隐私
   边界；最终数字枚举与字段号仍由唯一 Proto 生成源决定；
4. Proto 生成前：一次性冻结 field number、`reserved`、unknown oneof 和滚动兼容规则；建议由以下
   生成源成为唯一物理权威，26～29 号中的代码块在此之前只表达逻辑结构：

   ```text
   proto/public/farm_frame.proto
   proto/public/commands.proto
   proto/internal/command_rpc.proto
   proto/internal/subscription_rpc.proto
   proto/internal/workflow_rpc.proto
   ```

   CI 必须执行生成代码一致性与兼容性检查；实现 Agent 不得从 Markdown 各自复制 field number；
5. 基础设施探针：FarmFrame 总大小、各命令 `MaxPayloadBytes`、BackendPool 上限和超时预算；
6. ErrorDetail 错误码按 `PROTOCOL / AUTH / SESSION / SUBSCRIPTION / ROUTE / ACTOR /
   WORKFLOW / BUSINESS` 分域维护；稳定符号和动作以第 32 号第 10.6 节为准，不得修改本文件已经
   冻结的状态与 RetryDirective 语义。
7. OperationResult 的保留期、逻辑目录和源 Receipt 物理布局由第 31/32 号合同与容量模型决定。
8. P0d 任务、邮件、宠物和图鉴公开命令已经按第 34～37 号完成 CommandSpec；仍待后续闸门的是实际
   `.proto` field number、`reserved`、字节上限和生成代码兼容性验证，而不是再次讨论命令归属。
