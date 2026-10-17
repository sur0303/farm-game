# 29 · 内部 RPC 与可信调用上下文合同

版本：`v2.11-bootstrap-friend-opportunity-runtime-seam-freeze-candidate`  
日期：2026-08-03  
状态：在 `v1.1` 唯一内部 IDL、Work Lease 可信时间和 Resync 接缝基础上，补齐
PostCommitDeliveryGuard/delivery generation 接缝，并令内部下行直接复用第 28 号公开
`RejoinReason`；继续保留有限 Ready 版本切割、CancelJoinAttempt、Participant Holder
Fence、精确 Cleanup 和 Execution-Accept 后 TIMEOUT_UNKNOWN 语义；本次补齐 Join ACCEPTED 的内部
元数据接缝、Global/Shard/Authority Revision 的唯一映射，并统一 ResultDirectory 缺失和 DueIndex
紧凑 WorkRef 的恢复语义。
本版再按第 35/35A 号登记 `PlayerMailClaimIngress`、`TrustedMailClaimMaterial`、
`ResumeMailDeliveryOrder` 与 `ApplyMailClaimProjection`，并明确邮件附件材料由 Holder 内受信模块生成，
不能由 H5/Gate 拼装或让 PlayerActor 等待 MailEntryStore。
本版进一步统一 Actor/非 Actor 权威调用：WorkAddress 使用 `SourceAuthorityRef`，持久写使用
Root 作用域 `EffectiveIdempotencyKey`，并补齐 Friend Pair 重新激活 Barrier、Quota 两类释放与
ResultDirectory 的受信定位边界。
本版进一步冻结 Care 奖励 Participant，并将 Plant/Harvest/Theft/Care 的好友机会更新统一对齐第 31 号
dirty generation + 合并刷新合同；公网错误恢复映射以第 32 号第 10.6 节为准。最终 Proto field number
与平台身份实现仍待故障测试和物理生成。本版进一步按第 32 号 Q32-12 冻结 Crop/Steal/Care/Guard
规则的不同生效时刻、整数槽位校验、具体 CarePlan 生成和单次 Theft 守护结果，避免热更新、重试或
Actor 迁移改变已经接管的业务结果。
本版再按第 32 号 Q32-13 收口 Plant/Harvest/Care/Theft 的明确拒绝、无变化、不可逆提交、Participant
响应丢失、投影/下行失败与同目标并发语义，并冻结一组跨 Coordinator、ActorStore、Dispatcher 和
farm-worker 共用的故障注入点。
本版继续按 Q32-14 补齐 UnlockPlot/Plant 首个 Player Participant 的确定拒绝终态，拆分 UnlockPlot
的 FARM_REJECTED/FARM_NO_CHANGE，并明确单类型多数量 Buy/Sell 的内部结果恢复边界。
本版再按第 33 号 Q33-13 增加 CommitFriendActionAdmission 内部合同，使好友 Care/Theft 的 Permit、
Execution-Accept 与可恢复 Dispatch 共用一个 Pair Authority 提交点。
本版继续统一 CareOperationReceipt、稳定 CareResult 与第 31 号 StableOpaqueID v1 的物理身份，
并明确所有公开 Plot 变化和 FarmChange 只消费同一 Authority Commit 已冻结的版本化结果。
本版按第 34 号任务合同把好友 Subscription 的 relation fence 与激活时间纳入
`FarmAdmissionContext`，并冻结 `CONFIRM_FRIEND_VISIT` 的 Gate→PlayerActor 接缝；不增加任务专用
Participant、Worker RPC 或 DueIndex。
本版再按第 36 号补齐 `APPLY_GUARD_SNAPSHOT` Typed Participant、来源/Owner 交叉校验、不可变宠物结果
与 GuardSyncWatermark 回写接缝，并明确 Feed 隐藏更新与 Switch 公开 FarmMetaDelta 的版本差异。
本版再按第 37 号冻结 Plant 时的 `CompendiumUnlockSnapshot`、Harvest 时的原样传递，以及
`CompendiumUnlockDelta` 与 ParticipantReceipt/StoredOperationOutcome 同步保存的原结果语义。
本版继续承接第 26/28 号新增的两条显式公网查询：`GET_PLAYER_BOOTSTRAP` 进入
`PlayerBootstrapIngress`，`GET_NEXT_FRIEND_OPPORTUNITY` 进入 `FriendOpportunityNavigation`；同时冻结
好友农场 `SUBSCRIPTION_READY` 后由服务端最佳努力更新 Navigation visited 的内部运行时接缝。该接缝
不接受客户端自报目标，不建立 Workflow/Order，也不替代每日任务的 `CONFIRM_FRIEND_VISIT`。

本文成熟度分为三层：带 `L2-RPC-*` 编号的语义属于 `STABLE_SEMANTIC`；整份内部接口仍是
`CANDIDATE`；Proto field number、mTLS/Workload Identity 的平台接法、Deadline/Lease/批量参数和
Redis 物理布局属于 `PHYSICAL_PENDING`。后续实现不得把物理候选误当成新的业务语义。

> [26-access-session-contract.md](./26-access-session-contract.md) 是登录、Session、Connection、
> Subscription、Join 和下行 Fence 的上位权威；
> [27-access-routing-actor-workflow-freeze.md](./27-access-routing-actor-workflow-freeze.md) 是
> RouteDomain、VirtualShard、Actor Runtime、Workflow/Order 和迁移语义的上位权威；
> [28-command-catalog-public-protocol.md](./28-command-catalog-public-protocol.md) 是公网
> FarmFrame、CommandCatalog、ADMITTED、CommandResult 和 ServerPush 的上位权威。
> [33-friendship-share-access-contract.md](./33-friendship-share-access-contract.md) 是好友关系、VisitPermit
> 与 ActionAdmission 的业务语义权威。
> [34-task-system-contract.md](./34-task-system-contract.md) 是任务进度、好友访问确认与领奖的业务语义权威。
> 本文是 `TrustedCommandContext`、`ActorRouteHeader` 和三类内部调用 IDL 的唯一权威。第 26 号
> 文档拥有 SessionPermit/PendingJoin/ActiveSubscription 的数据来源与语义，第 27 号文档拥有
> Actor Route/Runtime 与 Workflow 语义，但二者不得再次复制本文内部结构。本文只定义三者之间
> 的内部 RPC 接缝，不得反向修改其业务语义。

---

## 0. 一页结论

```text
在线玩家命令
H5 → farm-gate → FirstHop
                   └── TrustedCommandContext

Workflow/Order正常首次推进
Coordinator/DirectDispatcher → Player/Farm Participant
                               └── CoordinatorWorkContext

故障后恢复
farm-worker → Player/Farm Participant
              └── InternalCallerContext + Lease
```

稳定语义结论（仍待实现验证）：

1. Gate 只调用 `player-svr / player ingress` 或 `farm-svr / farm ingress`，不直接调用 Actor，
   也不推进跨 Actor Participant。
2. 公网业务 Payload 以原始 bytes 进入内部 RPC；Gate 解码、准入、路由并保留 raw bytes，
   FirstHop 再独立解码、复核并生成唯一权威 Canonical Payload Hash。
3. 在线命令、Workflow/Order 正常首次推进、Worker 故障恢复使用三种不同可信上下文，不能用
   一个万能 Context，也不能让 Worker 伪装在线 Session。
4. `ActorRouteHeader` 与 Session/Caller Context 完全分离：前者回答“发到哪个权威 Actor”，
   后者回答“谁以什么身份发起”。
5. FirstHop 必须验证原始 Payload 目标与 RouteHeader 一致，自行生成权威 Payload Hash，并确认
   当前 Pod 仍持有目标 Shard；Gate 诊断 Hash 不参与正确性判断。
6. P0 通过受信服务身份、内部网络边界和完整 Fence 信任 `TrustedCommandContext`；不把
   SessionPermit 做成客户端 Token，也不要求每条业务命令查询 Redis。
7. 玩家命令和 Participant 调用使用 gRPC Unary，并复用 HTTP/2 长连接；Assignment/配置 Watch
   等控制面持续订阅才使用 Streaming。
8. RouteFeedback 只允许在 FirstHop 进入业务执行边界前返回；跨过该边界后只能返回
   `PROCESSING` 或最终业务结果，不能让 Gate 重跑完整业务。
9. `TrustedCommandContext.farm_context` 只表示 Gate 已激活的 ActiveSubscription；Join 的
   PendingJoin 使用独立 `JoinAdmissionContext`，两者不得同时存在。
10. Join FirstHop 返回 `ACCEPTED` 后，Gate 必须先绑定 PendingJoin、把公开 ACCEPTED 入队，再
    显式 `ArmSnapshot`；Snapshot 不得依赖独立 RPC 的偶然先后到达。
11. Snapshot 与 Catch-up 必须通过独立 Downlink Push；Snapshot QueueAccepted 后 Hub 在同一 Lane
    冻结有限 `ready_cut_version=N`，只追赶截至 N 的连续前缀，N 之后变化排在 Ready 后继续发送。
12. DownlinkService 不得根据第一条 Push 隐式创建 Subscription；已知 `subscription_id` 使用
    `CleanupSubscriptionRequest`，未知时使用 `CancelJoinAttemptRequest`，均精确路由到目标 Farm。
13. Gate 按 Holder 批量续租既有 Subscription；Renew 只能延长完整 Fence 匹配且未关闭的记录，
    请求/响应使用 TTL，Gate 以本地单调时钟建立 Deadline；不能创建、替换或复活 Subscription。
14. Execution-Accept 后发生 RPC Deadline、连接中断或 Holder迁移，不得返回 RouteRejection；
    有可靠锚点时返回 PROCESSING，无明确结果时进入 Operation/Receipt Unknown 解析。
15. `writer_claim_id` 不由 Gate、Coordinator 或 worker 在公网/Participant Payload 中指定；它由
    目标 Holder 的 Activation Repository Claim 获得，并只在本地强写 CAS/Lua 中使用。
16. Work Lease Claim/Renew 只提交 requested TTL；Work Repository 使用可信时间裁决并返回
    owner token、lease epoch 和 accepted TTL。Worker 用本地单调时钟建立保守 Deadline，不能用
    本机 Unix 时间决定权威过期。
17. Work Lease 在 Participant Mailbox 前可以由 WorkerRecoveryIngress 查询 Work Repository；
    Shard Lease 热路径则使用 Holder 本地 Lease 证明，不能机械地逐请求查询中心 Authority。
18. Hub/Gate 内部连续性失效统一为 `RESYNC_REQUIRED`，通过下行接缝映射成公网
    `REJOIN_REQUIRED`；失效通知不得依赖已经失败的普通 FarmChange Queue。
19. UnlockPlot、Plant、Harvest、Theft 与 CareRewardOrder 的 ParticipantCommand、Typed Step Payload、Typed Result、
    依赖结果 Hash 和 Workflow 状态准入已经冻结；公网业务 Request 不得直接作为 Participant Payload
    使用。
20. 任务列表、领奖和好友访问确认统一进入 PLAYER Holder；好友访问证明来自 Ready 后当前
    ActiveSubscription 的 relation fence 与可信激活时间，不增加任务专用 Participant 或 Worker。

---

## 1. 为什么必须区分三类内部调用

三类调用的授权来源、生命周期和故障恢复方式不同：

| 调用 | 发起者 | 授权依据 | 是否要求玩家在线 | 典型服务 |
|---|---|---|---|---|
| 在线命令 | farm-gate | 已激活 Session、当前连接和 Subscription Fence | 是 | gate → player-svr/farm-svr |
| Workflow/Order 首次推进 | Coordinator/DirectDispatcher | 已可靠创建的 Workflow/Order、operation_id、participant_step | 否 | player-svr/farm-svr → Participant |
| Worker 恢复 | farm-worker | 未完成 Workflow/Order、当前 Lease、Participant Receipt | 否 | farm-worker → player-svr/farm-svr |

如果把三者都放进 `TrustedCommandContext`：

- Worker 必须伪造一个不存在的在线连接才能恢复业务；
- 玩家断线后，Plant/Theft 等可靠流程无法继续；
- Participant 无法判断调用来自在线 Gate、Coordinator/DirectDispatcher 还是后台恢复器；
- 运维或测试调用容易绕过本应存在的 Workflow/Lease 校验。

因此三类调用可以复用路由、Trace、Deadline 和错误模型，但不能复用身份语义。

---

## 2. Gate → FirstHop

### 2.1 FirstHop 是什么

FirstHop 是业务 Deployment 的命令入口，不是 Actor。以下展示主要入口分组；33 类公开命令的完整、
唯一映射仍以第 28 号 CommandCatalog 为准，本文不维护第二份命令总表：

```text
BUY / SELL
→ player-svr / PlayerIngress

GET_PLAYER_BOOTSTRAP
→ player-svr / PlayerBootstrapIngress

GET_NEXT_FRIEND_OPPORTUNITY
→ player-svr / FriendOpportunityNavigation

GET_TASK_LIST
→ player-svr / TaskReadAPI

CLAIM_TASK / CONFIRM_FRIEND_VISIT
→ player-svr / TaskCommandIngress

GET_MAIL_LIST / GET_MAIL_DETAIL
→ player-svr / PlayerMailReadAPI

MARK_MAIL_READ / DELETE_MAIL
→ player-svr / PlayerMailCommandAPI

CLAIM_MAIL_ATTACHMENT
→ player-svr / PlayerMailClaimIngress

UNLOCK_PLOT / PLANT / HARVEST / CARE / THEFT
→ farm-svr / FarmIngress
```

FirstHop 负责：

- 验证内部调用方身份；
- 重新解码 raw payload；
- 生成权威 Payload Hash，并复核 RouteKey 和 ActorRouteHeader；
- 执行本服务业务准入；
- 选择本地 Actor、远程 Holder 或 Workflow Coordinator；
- 将内部结果映射回 Gate。

Gate 不负责：

- 直接修改 PlayerActor/FarmActor；
- 创建 Reservation 或 Participant Receipt；
- 推进多个 Actor 的 Workflow；
- 等待 Actor 持有网络 RPC 的整个生命周期。

### 2.2 ExecuteCommandRequest 逻辑结构

以下为逻辑 IDL，最终 method name 和 field number 待本文后续评审一次性冻结：

```protobuf
message ExecuteCommandRequest {
  string request_id = 1;
  string operation_id = 2;
  CommandType command_type = 3;
  bytes raw_payload = 4;

  TrustedCommandContext trusted_context = 10;
  DispatchContext dispatch_context = 11;

  int64 deadline_unix_ms = 12;
  uint32 remaining_budget_ms = 13;
  TraceContext trace_context = 14;
}
```

字段来源：

| 字段 | 来源 |
|---|---|
| request_id / operation_id / command_type | 第 28 号公网信封，经 Gate 校验 |
| raw_payload | 公网原始业务 bytes，Gate 不重新编码 |
| trusted_context | Gate 当前受信 Session/Connection/Subscription 状态 |
| dispatch_context | 按 CommandSpec.RouteMode 生成 Actor Route 或 Service LB 上下文 |
| deadline/budget | Gate 按 CommandSpec TimeoutBudget 生成，不采信客户端时间 |
| trace_context | Gate 创建或续接的内部 Trace |

> 上述 field number 仅为逻辑示意，`remaining_budget_ms` 与 `trace_context` 的最终编号在正式
> Proto 评审时一次性分配，不能据此生成兼容承诺。

### 2.3 为什么内部业务调用使用 Unary

```text
CommandIngressService.ExecuteCommand
CoordinatorParticipantService.ExecuteStep
WorkerRecoveryIngress.ResumeStep
```

三类业务调用都使用 gRPC Unary。Unary 表示“一次 RPC 一个请求、一个响应”，不表示每次重新
建立网络连接；gRPC 会在长期 HTTP/2 连接上多路复用大量并发请求。

Streaming 只用于持续控制面：

- AssignmentProvider Watch；
- 配置/FeatureGate Watch；
- 其他需要持续订阅的低频控制流。

玩家命令采用 Unary 的原因：

- 每条命令可以拥有独立 Deadline、取消和 RouteFeedback；
- 某个慢请求不会要求应用层自行维护整条 Stream 的 request/response 关联；
- Stream 断开时不会一次制造大量归属不清的未决业务；
- Shard 迁移后可以对单条命令 Redirect；
- 农场是低频经营操作，不需要战斗服那种连续输入流。

不同 Unary 请求可以并发到达不同 Actor；进入同一个 PlayerActor/FarmActor 后仍按该 Actor
Mailbox 串行裁决。这是“跨 Actor 并发、单 Actor 有序”，两者不矛盾。

---

## 3. TrustedCommandContext 与 SessionPermit

### 3.1 逻辑结构

```protobuf
message TrustedCommandContext {
  SessionPermitSnapshot session = 1;
  bool recovery_complete_at_admission = 2;
  optional FarmAdmissionContext farm_context = 3;
  optional JoinAdmissionContext join_attempt = 4;
}

message SessionPermitSnapshot {
  uint64 player_id = 1;
  uint64 active_session_epoch = 2;

  string gateway_id = 3;
  string gateway_boot_id = 4;
  string connection_id = 5;
  uint64 connection_generation = 6;
  uint64 session_route_revision = 7;

  int64 admitted_at_unix_ms = 8;
}

message FarmAdmissionContext {
  uint64 admitted_farm_id = 1;
  string admitted_subscription_id = 2;
  optional string authorization_pair_id = 3;
  optional string authorization_relation_id = 4;
  optional uint64 authorization_relation_epoch = 5;
  int64 admitted_subscription_activated_at_ms = 6;
}

message JoinAdmissionContext {
  string join_attempt_id = 1;
  uint64 target_farm_id = 2;
}
```

这是第 26 号 `SessionPermit` 的最小内部投影，不是另一套 Session 结构。对于不要求农场
Subscription 的命令，`farm_context` 不存在；是否必须存在由 CommandCatalog 的
SessionAdmissionPolicy 决定。

本代码块是四份文档中 `TrustedCommandContext` 的唯一字段定义：

```text
request_id / operation_id / command_type
deadline / remaining budget / trace
→ ExecuteCommandRequest外层

SessionPermitSnapshot / recovery / admitted Farm或Join上下文
→ TrustedCommandContext
```

26/27 只能描述字段来源和消费条件，不得复制一个包含 `current_farm_id`、`request_id` 或
`deadline` 的旧结构。若未来增加可信字段，必须先版本化本文 IDL，再同步修改上游映射。

字段使用 `admitted_*` 而不是 `current_*`，因为它们表达命令通过 Gate ADMITTED 瞬间的不可变
快照。FirstHop 执行时玩家可能已经切换到另一座农场，但已经 ADMITTED 的旧命令仍按该快照
完成，不能把后来的页面状态倒灌进旧命令。

后三个 authorization 字段只在好友 Join 已经通过 Pair Authority 授权并进入 ActiveSubscription 时
存在；自己农场必须为空。`admitted_subscription_activated_at_ms` 来自 Gate 的 ActiveSubscription，
使用服务端可信时间，供 Ready 后访问确认归属正确每日周期。H5 不能提交、覆盖或回显后再反向充当
这些字段的来源。

### 3.2 SessionPermit 到底是什么

`SessionPermit` 不是 TCP 三次握手、不是客户端 Cookie，也不是由 H5 放进每条命令的 JWT。
它是 Gate 完成下列检查后得到的“服务端准入结论”：

```text
这根连接已经通过ClientHello
当前session_epoch仍有效
connection_generation仍是当前连接
恢复屏障是否完成
当前正在订阅哪座农场
当前subscription_id是什么
```

实现上可以理解为 Gate 内存中的一份只读准入状态：

```text
SessionPermit（Gate内部逻辑对象）
├── player_id
├── active_session_epoch
├── gateway_id / gateway_boot_id
├── connection Fence
├── session_route_revision
├── admitted_at
├── recovery_complete_at_admission
└── admitted FarmSubscription
```

当一条命令通过 ADMITTED 时，Gate 从这份内部状态中提取必要字段，生成
`TrustedCommandContext` 发送给 FirstHop。客户端不能提交或覆盖这些字段。

因此本文不要求把 `SessionPermit` 本身序列化成一个可在公网流转的 Token：

```text
错误理解：
H5 → 每条命令携带SessionPermit → farm-svr自行验Token

冻结方案：
H5 → 普通ClientCommand
Gate → 从本地受信Session生成TrustedCommandContext
Gate → 以受信服务身份调用FirstHop
```

### 3.3 为什么不每条命令查询 Redis

Redis 中的 SessionAuthority 用于：

- 登录代次和 Ticket 激活；
- 顶号、KICK 和跨 Gate SessionRoute；
- Gate 重启、Lease 续租与失效；
- 多 Gate 场景中的全局会话权威。

但当前活跃连接的每一帧还要经过 Gate 本地：

- ConnectionRegistry；
- connection_generation；
- session_epoch；
- ProtocolGuard；
- CommandCatalog Admission；
- current subscription Fence。

如果每次 Plant/Water/Theft 都再次查询 Redis：

- 接入延迟和 Redis QPS 会随全部业务命令线性增长；
- Redis 抖动会让本来健康的在线连接全部停顿；
- 已经存在的本地连接 Fence 和 Gate 准入价值被重复实现。

所以 P0 使用：

```text
登录/换票/顶号/续租
→ Redis SessionAuthority

连接上的每条业务命令
→ Gate本地Session Fence
→ TrustedCommandContext
→ FirstHop
```

当 Gate 收到 Session 失效或 KICK 后，必须先关闭本地准入，再关闭旧连接；已经 ADMITTED 的
旧命令继续按第 26/27 号文档产生可解释结果。

### 3.4 PendingJoin 不是 FarmAdmissionContext

Gate 本地 SessionPermit 必须区分：

```text
PendingJoin
→ 仅允许JOIN_FARM控制流、Snapshot匹配和取消清理

ActiveSubscription
→ 才能生成普通农场命令的FarmAdmissionContext
```

`JOIN_FARM` 请求必须携带 Gate 生成的 `JoinAdmissionContext`，并满足：

```text
join_attempt.target_farm_id == JoinFarmRequest.target_farm_id
farm_context不存在
```

普通 `UNLOCK_PLOT/PLANT/HARVEST/CARE/THEFT` 等要求当前 Subscription 的命令必须满足：

```text
farm_context存在
join_attempt不存在
```

`CommandResult(ACCEPTED)` 只更新 Gate PendingJoin 元信息，不得让 PendingJoin 倒灌为
`admitted_farm_id/admitted_subscription_id`。Snapshot QueueAccepted 也只允许 Hub 开始
FLUSHING；只有截至 `ready_through_version` 的 Catch-up 已按序入队、Gate 把
`SUBSCRIPTION_READY` 放入同一 ConnectionWriter FIFO 后，才能把 PendingJoin 提升为
ActiveSubscription，并在后续命令中生成 FarmAdmissionContext。

### 3.5 FirstHop 为什么可以相信它

FirstHop 信任的不是消息里的某个 `caller_service="farm-gate"` 字符串，而是 RPC 连接本身的
受信服务身份：

```text
公网
→ 只能访问farm-gate公网入口

farm-gate内部客户端
→ 使用服务身份调用player-svr/farm-svr内部端口

FirstHop内部拦截器
→ 从mTLS证书/平台身份取得真实caller
→ 仅允许farm-gate调用在线ExecuteCommand
```

生产约束：

- player-svr/farm-svr 内部 RPC 端口不暴露公网；
- 使用 mTLS 或等价平台 Workload Identity；
- K8s NetworkPolicy/安全组只允许声明的服务访问；
- RPC 拦截器按 method 校验调用方 Role；
- `caller_service` 等消息字段只能用于日志，不能单独作为授权依据。

本地开发可以在回环地址或隔离 Docker 网络中使用显式 `insecure-dev` Provider，但该模式必须
默认关闭，且生产配置启动时发现它开启应直接失败。

未来若跨不受信网络或平台要求更强，可以增加短期签名 Internal Permit；这不是 P0 每条命令
查询 Redis 的理由。

---

## 4. DispatchContext 与 ActorRouteHeader

### 4.0 不是每条命令都走 Actor Route

CommandSpec 必须声明：

```text
RouteMode
├── ACTOR_AFFINITY
├── SERVICE_LB
└── NONE
```

内部请求使用：

```protobuf
message DispatchContext {
  RouteMode route_mode = 1;
  optional ActorRouteHeader actor_route = 2;
}
```

约束：

```text
ACTOR_AFFINITY
→ 必须存在ActorRouteHeader

SERVICE_LB / NONE
→ 不得为满足字段而伪造Actor ID
```

`GET_FRIEND_LIST`、部分 ReadAPI 可以使用 Service LB；UnlockPlot/Plant/Theft 等权威命令使用 Actor
Affinity。P0 暂不为普通 Service LB 发明复杂 ServiceRouteHeader，由 gRPC 服务发现和负载均衡
选择健康实例。

### 4.1 逻辑结构

```protobuf
message ActorRouteHeader {
  RouteDomain route_domain = 1;
  uint64 actor_id = 2;
  uint32 virtual_shard = 3;

  uint64 shard_revision = 4;
  string expected_holder_id = 5;
  string expected_holder_boot_id = 6;
  uint64 expected_writer_epoch = 7;

  optional uint64 source_snapshot_revision = 8;
  string source_instance_id = 9;
  string source_boot_id = 10;
}
```

它完全由服务端 `ActorRouteClient` 生成，不接受客户端提供。
本代码块是四份文档中 `ActorRouteHeader` 的唯一字段定义。`request_id/operation_id/deadline/trace`
属于调用请求外层；任何文档或实现都不得把这些字段重新塞进 RouteHeader，也不得省略目标
Holder ID/BootID 后只靠 Endpoint 猜测当前承载者。

### 4.2 为什么与 Session 分开

两者回答不同问题：

```text
TrustedCommandContext
→ 谁在操作、当前连接是否可写、正在看哪座农场

ActorRouteHeader
→ 目标权威属于哪个RouteDomain、Shard和Holder
```

例如 B 偷 A 的菜：

```text
trusted_context.player_id = B

route_header.route_domain = FARM
route_header.actor_id = A
```

操作者是 B，不代表命令应路由到 FarmActor(B)。如果把 Session 和 RouteKey 混成一个 ID，
好友访问、偷菜和护理都会路由错误。

### 4.3 RouteHeader 是缓存快照，不是写权本身

Gate 携带的是它当前观察到的 Assignment：

```text
FARM shard=27
expected_holder=pod-A
expected_holder_boot_id=boot-37
shard_revision=102
expected_writer_epoch=7
```

真正写权仍由 Holder 本地 Lease/WriterEpoch 校验决定。Shard 已迁移到 pod-B 时，pod-A 必须
返回结构化 RouteFeedback，不能继续执行，也不能仅因为 Gate Header 还是旧值就恢复写权。

`source_snapshot_revision` 只用于诊断调用方看到的整份路由快照，不替代目标 Shard 自己的
`shard_revision`。Pod 名字相同但进程重启时，`expected_holder_boot_id` 必须让旧请求失效。

Revision 字段映射只服从第 27/30 号合同：

```text
source_snapshot_revision = RouteSnapshot.GlobalRevision
shard_revision           = ShardRoute.ShardRevision
                         = ShardWriteAuthority.assignment_revision
expected_writer_epoch    = 同一ShardWriteAuthority.writer_epoch
expected_holder_id/boot  = 同一ShardWriteAuthority的Holder/Boot投影
```

ActorRouteClient 必须从一份不可变 Snapshot 生成完整 Header；禁止分别读取缓存字段后拼出跨 Revision
组合。Header 仍只是调用方观察到的路由前置条件，不替代 Holder/Store 的最终写权 Fence。
`expected_lease_epoch` 不进入跨服务 ActorRouteHeader：它来自目标 Holder 当前本地 Shard Lease 证明，
由 Holder Repository Adapter 与 Header 字段合并为第 30 号 `ShardWriteFence` 后再调用 Store。调用方
不能伪造 LeaseEpoch，Actor 业务代码也不接触该字段。

---

## 5. FirstHop 复核顺序

FirstHop 收到在线命令后依次执行：

```text
1. 从RPC连接取得真实调用方服务身份
2. 确认该method允许farm-gate调用
3. 检查deadline尚未过期
4. 根据command_type查询内部CommandSpec
5. 按PayloadType重新解码raw_payload
6. 从Typed Payload重新提取RouteKey
7. 按RouteMode验证DispatchContext
8. ACTOR_AFFINITY时验证RouteKey == route_header(domain, actor_id)
9. 重新计算VirtualShard并比较shard_revision/Holder BootID
10. 本地校验本Pod当前Shard Lease和expected_writer_epoch
11. 生成唯一权威CanonicalPayloadHash
12. 按ExecutionKind选择接受路径
13. 执行本服务业务准入
14. 进入Coordinator、Actor Runtime或无状态Handler
```

任一步失败都不得把请求投给“猜测正确”的 Actor。

第 12 步不能被实现成“所有命令先激活 Actor”。不同 `ExecutionKind` 的接受路径为：

```text
ACTOR_DIRECT
→ ActorRegistry查找或加载目标Actor
→ 冷Actor执行LoadAndClaim/ActorWriteClaim
→ 校验Activation.writer_epoch + writer_claim_id
→ 目标Mailbox接受带幂等身份的命令

WORKFLOW_COORDINATOR
→ 只确认Coordinator所在Holder的Route/Shard Fence
→ 不为首跳提前加载PlayerActor/FarmActor
→ CreateIfAbsent Workflow
→ Workflow跨Actor调用Participant时，再由Participant Holder激活对应Actor

DOMAIN_ROLE / JOIN_FARM
→ 验证JoinAdmissionContext与Typed Payload目标一致
→ 自己农场：创建或幂等复用JOINING(subscription_id, join_attempt_id, snapshot_armed=false)
→ 好友农场：Hub先创建AUTHORIZING，再调用Pair Authority授权并绑定relation epoch，随后进入JOINING
→ 返回ACCEPTED元信息
→ 等待Gate以完整Fence显式ArmSnapshot
→ FarmSnapshot只通过独立Downlink Push发送
→ 不在CommandResult中返回Snapshot

其他DOMAIN_ROLE
→ 由具体Domain Handler决定是否需要Actor或其他权威模块

SERVICE_LB
→ 取得无状态Handler的有界容量
→ Handler正式接管查询
```

例如 UnlockPlot/Plant 的首跳虽然按 `farm_id` 路由到 `farm-svr`，但这只是在 Farm Holder 上创建
对应 Workflow；PlayerActor/FarmActor 的加载和 `LoadAndClaim/ActorWriteClaim` 发生在各自 Participant
真正被调用时。这样不会因为 Workflow 创建失败或业务准入失败而无意义地激活 FarmActor。

例如：

```text
raw_payload.target_farm_id = A
route_header.actor_id = B

→ ROUTE_TARGET_MISMATCH
→ 不调用FarmActor(A)
→ 也不调用FarmActor(B)
```

Gate 和 FirstHop 共享 `SharedCommandSchema` 中的 PayloadType、Decoder、StructuralValidator 和
RouteKeyExtractor；GatePolicy 与 FirstHopPolicy 分开。CanonicalPayloadBuilder 只由 FirstHop
用于权威幂等 Hash。该 Hash 专门识别弱网、超时、Redirect 和断线恢复时使用原
`operation_id` 发起的同一业务重试，防止重复扣资产或重复发奖；它不是网络校验和或身份签名。
CI 必须证明 Gate 暴露的命令集合与对应 FirstHop 可处理集合兼容。

### 5.1 Shard Fence 与 ActorRecord Fence

两级 Fence 不能混成“每个请求查询一次 ActorRecord”：

```text
RouteIngress热路径
→ 本地Assignment
→ HolderID/BootID
→ ShardRevision
→ 本地Shard Lease
→ expected_writer_epoch

ActorRegistry
├── Actor已ACTIVE
│   → 校验Activation.writer_epoch与本地Shard Epoch一致
│   → 后续强写携带Activation.writer_claim_id
│
└── Actor未激活
    → Load
    → LoadAndClaim生成新的writer_claim_id
    → Authority Store原子校验writer_epoch并更换ActorRecord.writer_claim_id
    → Claim成功后才能ACTIVE
```

`ActorRecord.writer_epoch + writer_claim_id` 是 Authority Store 的真实存储 Fence，只在冷激活
Claim、持久化 CAS、迁移和 Unknown 恢复中使用。禁止每条 Plant/Care/Theft 都先远程读取
ActorRecord 再进 Mailbox；Actor 从本地 Activation 取得 claim，并由最终强写 Lua 原子校验。

---

## 6. CoordinatorWorkContext：Workflow 与 Order 正常首次推进

### 6.1 Coordinator 与 DirectDispatcher

Coordinator 是业务服务中的进程内模块，不是新的独立 Deployment。它在用户命令的首条链路
中创建并立即推进可靠 Workflow：

```text
UnlockPlot
Gate
→ farm-svr / UnlockPlotCoordinator
→ CreateIfAbsent UnlockPlotWorkflow
→ PlayerActor.ReserveUnlockCoin
→ FarmActor.CommitPlotUnlock
→ PlayerActor.CommitUnlockCoinReservation
→ COMPLETED

Plant
Gate
→ farm-svr / FarmOperationCoordinator
→ CreateIfAbsent PlantWorkflow
→ PlayerActor.ReserveSeed
→ FarmActor.CommitPlant
→ PlayerActor.CommitSeedReservation
→ COMPLETED
```

`CreateIfAbsent PlantWorkflow` 的地址不是随机生成。FirstHop 必须先按第 27 号合同从
`EffectiveIdempotencyKey(subject_player_id, command_type, operation_id, OperationRootRef)` 通过
`farm-workflow-id-v2` 派生同一稳定 workflow_id 和
WorkAddress，再用 CanonicalCommandPayloadHash 执行 CreateIfAbsent。相同 ID、不同 Hash 返回
`IDEMPOTENCY_CONFLICT`；业务 schema version 不参与 workflow_id 派生。

只要当前服务、依赖 Participant 和 Deadline 正常，Coordinator 会在这次请求中尽量推进到
完成，并返回 `SUCCEEDED`；如果已经建立 Workflow 但未在同步预算内完成，则返回
`PROCESSING`。

正常 Order 快速路径由来源服务中的 DirectDispatcher 推进：

```text
CareRewardOrder
→ farm-svr DirectDispatcher
→ PlayerActor.ApplyCareReward

GuardSyncOrder
→ player-svr DirectDispatcher
→ FarmActor.ApplyGuardSnapshot
```

DirectDispatcher 不要求 Work Lease，因为它不是扫描恢复者，而是来源事实和 Order 刚提交后的
快速投递者。重复快速投递仍使用相同 `participant_invocation_id`，由 ParticipantReceipt 防重；
失败或超时后才由 farm-worker 持 Lease 接管。来源提交必须把完整 `WorkAddress` 交给
DirectDispatcher；有界队列不能只保存裸 `order_id`。

### 6.2 逻辑结构

```protobuf
enum WorkPlacementKind {
  WORK_PLACEMENT_UNSPECIFIED = 0;
  INDEPENDENT_WORK_BUCKET = 1;
  SOURCE_PERSISTENCE_BUCKET = 2;
}

message WorkAddress {
  string work_id = 1;
  WorkType work_type = 2;
  WorkPlacementKind placement_kind = 3;
  uint32 bucket_version = 4;
  uint32 bucket_id = 5;
  optional SourceAuthorityRef source_authority = 6;
}

message SourceAuthorityRef {
  SourceAuthorityKind authority_kind = 1;
  string canonical_authority_id = 2;
  string authority_layout_version = 3;
  optional RouteDomain actor_route_domain = 4;
}

message CoordinatorWorkContext {
  WorkAddress work = 1;
  uint64 expected_work_version = 2;
  ParticipantStep participant_step = 3;
  string participant_invocation_id = 4;
  string operation_id = 5;

  uint64 subject_player_id = 6;
  uint32 remaining_budget_ms = 7;
  TraceContext trace_context = 8;
}
```

`SOURCE_PERSISTENCE_BUCKET` 必须携带 `source_authority`。`PLAYER_ACTOR/FARM_ACTOR` 来源分别填写并复核
`actor_route_domain=PLAYER/FARM`；FRIEND_PAIR 等非 Actor Authority 禁止伪造 RouteDomain、ActorRouteHeader、
WriterClaim 或 Mailbox 身份。`canonical_authority_id` 使用规范字符串，因此既能表达十进制 player/farm
ID，也能表达 canonical pair 的 `sid1_...` ID。

P0 `WorkType` 注册表至少包含：

```text
UNLOCK_PLOT_WORKFLOW / PLANT_WORKFLOW / HARVEST_WORKFLOW / THEFT_WORKFLOW
ADD_FRIEND_WORKFLOW
CARE_REWARD_ORDER / GUARD_SYNC_ORDER
FRIEND_TERMINATION_ORDER / FRIEND_ACTION_DISPATCH_ORDER / RESULT_PUBLISH_ORDER
```

每个 WorkType 必须静态声明 placement、允许的 SourceAuthorityKind、TypedPayloadType 和
ParticipantStep 集合；未知组合在 Repository 或 Ingress 处 Fail Closed。

`WorkAddress` 服从第 27 号合同：独立 Workflow 不携带 source，来源同槽 Order 必须携带完整
`SourceAuthorityRef`。Actor 来源额外携带并复核 RouteDomain；非 Actor 来源不得伪造 RouteDomain、Lease 或
WriterClaim。Coordinator/Dispatcher、Work Repository 和 Participant Ingress 都必须按
`bucket_version` 重新计算并核对 `bucket_id`；不得只拿 `work_id` 猜 Bucket，也不得在地址不匹配时
遍历多个 Bucket 碰运气查找。最终 IDL field number 会统一冻结，上述编号仅表达逻辑结构。

`participant_step` 使用稳定枚举或场景专属 Typed Step，禁止用自由字符串拼写正确性状态。
每次 Participant 调用必须具有确定性 ID：

```text
participant_invocation_id
= StableOpaqueID(
    "farm-participant-invocation-v1",
    stable_work_type_name,
    work.work_id,
    stable_participant_step_name,
    canonical_step_instance
  )
```

本文后续出现的 Reservation、CropCycle、CareIncident、TheftAttempt 和 Settlement 确定性 ID，均是
第 31 号 `StableOpaqueID v1` 的场景专属 namespace 封装；`DeterministicID` 不再代表另一种算法。

Receipt：

```text
ParticipantReceiptKey {
    participant_actor
    participant_invocation_id
}

相同invocation_id + 相同payload_hash
→ 返回原结果

相同invocation_id + 不同payload_hash
→ PARTICIPANT_IDEMPOTENCY_CONFLICT
```

若未来 Workflow/Order 允许同一步骤出现多轮，`participant_step_instance` 必须包含稳定轮次，不能使用
每次重试都会变化的随机数。

Participant 的授权依据不是玩家是否在线，而是：

- Workflow/Order 已经可靠存在；
- operation_id 与 Work 绑定；
- expected_work_version 和 participant_step 仍与 Work 状态兼容；
- participant_step 符合当前状态机；
- 调用方是允许推进该 Work 的 Coordinator/DirectDispatcher Role；
- Actor 内 Participant Receipt 能防止步骤重复生效。

正常快速投递在进入 Participant Mailbox 前校验 `WorkAddress + expected_work_version + step`。
该校验属于异步 Work Ingress，不是每条玩家 Actor 命令的热路径。

---

## 7. InternalCallerContext：Worker 恢复

### 7.1 Worker 与 Coordinator 有什么不同

两者推进的是同一份 Workflow 状态机，但所处时间和写权不同：

| 对比 | Coordinator | farm-worker |
|---|---|---|
| 何时运行 | 用户命令刚到达时 | Workflow 超时、服务崩溃或进入 Due 状态后 |
| 所在服务 | player-svr/farm-svr 内的业务模块 | farm-worker Deployment |
| 是否创建 Workflow | FirstHop 负责 CreateIfAbsent | 不创建新的业务意图，只读取原 Workflow |
| 推进依据 | 当前请求 + Workflow 状态 | Workflow 状态 + 当前 Lease |
| 是否在请求延迟链路 | 是 | 否 |
| 返回给客户端 | 可以同步返回结果 | 通常写 OperationResult/事件，客户端查询或收 Push |

Worker 不是第二套业务实现。它调用和 Coordinator 相同的 Participant 命令，依靠同一状态机和
Receipt 防重，只是先取得恢复 Lease。

### 7.2 Plant 崩溃例子

```text
T1 farm-svr Coordinator
   → 创建PlantWorkflow(W1)

T2 Coordinator
   → PlayerActor.ReserveSeed成功
   → ParticipantReceipt记录PLAYER_RESERVED

T3 farm-svr进程崩溃
   → FarmActor.CommitPlant尚未执行

T4 Workflow到期
   → farm-worker扫描/发现W1
   → Claim Lease(workflow_id=W1, lease_epoch=9)

T5 farm-worker读取W1和Receipt
   → 已经ReserveSeed，不再重复扣种子
   → 调FarmActor.CommitPlant
   → 调PlayerActor.CommitSeedReservation
   → W1 COMPLETED
```

如果没有 Worker，玩家断线或 farm-svr 崩溃后，已经预留的种子可能长期悬挂。如果 Worker
伪造在线 Session，则恢复会错误依赖玩家重新登录。正确授权依据应是 W1、Lease 和 Receipt。

### 7.3 Work Lease 是什么

Work Lease 是“某个 Worker 在一小段时间内有权推进这份 Workflow/Order”的租约，防止多个
Worker 同时把同一待办向前推进：

```text
WorkLease {
    work_address
    owner_instance_id
    owner_boot_id
    owner_token
    lease_epoch
    expire_at_authority_ms  # 仅Work Repository内部按可信时间解释
}
```

例如 Worker A 取得 10 秒 Lease：

```text
0s：A Claim成功，epoch=9
5s：A可以继续推进
10s：Lease到期，A不再有权发起新步骤
11s：Worker B Claim成功，epoch=10
12s：A的迟到调用必须被拒绝或被ParticipantReceipt判定为重复
```

它不是锁死资源的永久锁：Worker 崩溃后不需要人工解锁，Lease 到期即可由另一个 Worker 接管。

不要与 Actor Shard Lease 混淆：

| Lease | 保护对象 | 持有者 | 作用 |
|---|---|---|---|
| Shard Lease | 一个 VirtualShard 的 Actor 写权 | player-svr/farm-svr Holder Pod | 防止两个 Pod 同时写 Actor |
| Work Lease | 一份 Workflow/Order 的推进权 | farm-worker 实例 | 防止多个 Worker 重复推进待办 |

#### 7.3.1 Work Lease 可信时间与本地 Deadline

Worker 不能把本机墙上时间计算出的 `leaseUntil` 交给 Repository。Claim/Renew 的逻辑合同为：

```text
ClaimWork(work_address, worker_instance_id, worker_boot_id, requested_ttl_ms)
RenewWork(work_address, owner_token, lease_epoch, requested_ttl_ms)

Work Repository / Redis Lua
→ 使用Redis TIME或等价可信时间原子判断旧Lease是否到期
→ 生成/校验owner_token与lease_epoch
→ expire_at_authority_ms = trusted_now_ms + accepted_ttl_ms
→ 返回owner_token + lease_epoch + work_version + accepted_ttl_ms
```

Worker 在发送 Claim/Renew 前记录本地单调时钟，并建立保守 Deadline：

```text
work_local_deadline
= claim_or_renew_started_monotonic
 + accepted_ttl_ms
 - configured_safety_margin
```

达到本地 Deadline 后，Worker 不再发起新的 Participant Step；它可以查询结果或重新 Claim，但
不得拿本机 Unix 时间和 `expire_at_authority_ms` 直接比较。真正进入 Participant Mailbox 前，
`WorkerRecoveryIngress` 仍按第 7.4 节向 Work Repository 验证 owner、BootID、token、epoch、可信
时间下未过期、WorkVersion 和 participant_step。这次查询属于后台恢复路径，不进入玩家命令热路径。

Shard Lease 复用“Authority 可信时间 + accepted TTL + Holder 本地单调 Deadline”的原则，但不
复用逐 Step 查询方式：RouteIngress 依赖 Holder 本地仍有效的 Shard Lease 证明，达到 Deadline
立即 Self-Fence；每次强写再由 `writer_epoch + writer_claim_id` 在 Authority Store 最终 Fence。
具体 Shard 合同以第 27 号文档为上位权威。

### 7.4 逻辑结构

```protobuf
message InternalCallerContext {
  InternalCallerType caller_type = 1;
  string caller_instance_id = 2;
  string caller_boot_id = 3;

  WorkAddress work = 4;

  uint64 expected_work_version = 5;
  ParticipantStep participant_step = 6;
  string participant_invocation_id = 7;

  string lease_owner_token = 8;
  uint64 lease_epoch = 9;
  uint32 remaining_budget_ms = 10;
  TraceContext trace_context = 11;
}
```

`lease_epoch` 只是 Worker 自报的数字，本身不能证明仍持有 Lease。P0 冻结为：

```text
farm-worker
→ WorkerRecoveryIngress
→ 根据WorkAddress + lease_owner_token + lease_epoch查询Redis Work Repository
→ Repository按可信时间校验owner/boot/token/epoch仍未过期
→ 校验expected_work_version和participant_step仍有效
→ 生成受信RecoveryCallContext
→ 投递Participant Mailbox
```

Lease 校验发生在进入 Actor Mailbox 前，Actor Turn 不等待 Redis。校验后发生 Lease 换代的竞态
由确定性 `participant_invocation_id` 和 Actor 内 ParticipantReceipt 兜底：新旧 Worker 即使
迟到并发，也只能让同一 Participant Step 生效一次。

WorkerRecoveryIngress/Participant 必须验证：

- 调用方服务身份允许恢复该 Workflow/Order；
- `WorkAddress` 的 placement、bucket 和可选 source 字段符合该 WorkType；
- Lease 仍属于该 Worker Boot，`owner_token / lease_epoch` 匹配且 Repository 以可信时间确认未过期；
- `expected_work_version` 与 Work Repository 当前版本一致；
- 当前状态仍需要执行该 participant_step；
- `participant_invocation_id` 与 work_id/work_type/step 的确定性计算一致；
- ParticipantReceipt 是否已经完成；
- 过期 Worker 的迟到请求不能覆盖当前恢复者结果。

P0 中 Workflow/Order/Lease 暂定存公司 Redis；具体 Key、Hash Tag 和 Lua 原子操作留给第 31
号物理合同。

### 7.5 Lease 换代不改变当前 Participant Step

Work Lease 换代只改变“由谁负责继续推进”，不能因为旧 Worker 超时就把当前步骤当失败：

```text
旧Worker发出CommitPlant(invocation=W1/FARM_COMMIT)
→ Lease过期
→ 请求仍在FarmActor Mailbox

新Worker取得更高Lease
→ 继续解析相同invocation
→ 不能直接释放种子或进入冲突补偿
```

冻结不变量：

1. 当前 ParticipantInvocationID 的结果未通过 ParticipantReceipt、OperationResult 或权威状态
   明确解析前，Workflow/Order 不得进入与该 Step 不兼容的后续状态。
2. 新旧恢复者必须使用相同 `participant_invocation_id` 和相同 Canonical Step Payload。
3. `expected_work_version` 拒绝明显过期的恢复调用，但不能单独解决已经入 Mailbox 的竞态。
4. 最终防重由 ParticipantReceipt 完成；超时本身不能触发破坏性补偿。

### 7.6 Participant RPC 统一外壳

逻辑结构：

```protobuf
message ParticipantRequest {
  ActorRouteHeader route_header = 1;

  oneof caller_context {
    CoordinatorWorkContext coordinator = 2;
    InternalCallerContext recovery = 3;
  }

  ParticipantCommandType participant_command = 4;
  bytes typed_payload = 5;
}
```

正常在线命令不会由 Gate 直接构造 ParticipantRequest；它必须先进入 FirstHop，由
Coordinator/DirectDispatcher 创建可靠 Work 后再调用 Participant。

Participant Ingress 依次执行：

```text
真实服务身份
→ Caller Context类型和Role
→ Typed Step Payload解码
→ CanonicalStepPayloadHash
→ 校验InvocationID与WorkAddress/participant_step的绑定
→ 校验ActorRouteHeader并确认当前Pod仍是目标Shard的有效Holder
→ 在正确Participant权威侧查询ParticipantReceipt
   ├── 已存在且hash相同 → 直接返回原Participant结果
   └── 不存在 → 继续执行后续Work校验
→ Workflow/Order版本或Worker Lease
→ Actor Mailbox
```

这里必须区分“物理路由顺序”和“业务逻辑优先级”：

```text
物理路由顺序
→ 先证明请求到达当前有效Holder
→ 再读取该Participant权威Receipt

业务逻辑优先级
→ Receipt命中后，不再因旧WorkVersion或旧Worker Lease拒绝已完成步骤
```

旧 Holder、失效 Shard Lease 或错误 RouteHeader 即使能够访问共享 Redis，也不得越过 Holder Fence
代替当前权威返回 Participant 结果。Receipt 可以优先于 WorkVersion/Worker Lease，但不能优先于
真实服务身份、Invocation/Hash 绑定和当前 Holder 身份。

公网命令 Hash 与 Participant Step Hash 是两个不同幂等层级：

```text
CanonicalCommandPayloadHash
→ FirstHop生成
→ 标识原始用户命令

CanonicalStepPayloadHash
→ Participant Ingress按Typed StepSchema生成
→ 标识某个ParticipantInvocation的具体参数
```

例如 Plant：

```text
ReserveSeed Step Hash
→ player_id/item_id/quantity/reservation_id

CommitPlant Step Hash
→ farm_id/plot_id/crop_id/expected_plot_version/reservation_id

CommitSeedReservation Step Hash
→ reservation_id
```

ParticipantReceipt 必须与业务效果原子保存：

```text
participant_invocation_id
participant_step_payload_hash
participant_result
```

规则：

```text
相同invocation_id + 相同step payload hash
→ 返回原结果

相同invocation_id + 不同step payload hash
→ PARTICIPANT_IDEMPOTENCY_CONFLICT
```

在当前 Holder 已经确认的前提下，Receipt 查询必须优先于旧 WorkVersion 或旧 Worker Lease 的
拒绝。原因是第一次 Participant 调用可能已经完成，只是响应在旧 RPC 或旧 Lease 期间丢失；
此时重试携带的旧版本/旧 Lease 不应把“已完成”误报成 `WORK_VERSION_MISMATCH`。只有 Receipt
不存在时，当前 WorkVersion、Worker Lease 和 Mailbox 准入才决定这次调用能否首次执行。Receipt 命中仍必须
满足真实服务身份、InvocationID 绑定、Step Hash 和当前 Holder Fence，不能被任意调用者或旧 Holder
用来查询结果。

禁止直接复用原始公网命令 Hash，因为不同 Participant Step 的参数集合不同。

### 7.7 Participant Service 与统一返回外壳

冻结两个 Unary 入口；二者最终进入同一套 Participant Ingress、Actor Mailbox 和领域 Handler，但
调用身份与 Work 校验不同：

```protobuf
service CoordinatorParticipantService {
  rpc ExecuteStep(ParticipantRequest) returns (ParticipantResponse);
}

service WorkerRecoveryIngress {
  rpc ResumeStep(ParticipantRequest) returns (ParticipantResponse);
}
```

- `ExecuteStep` 只接受 `CoordinatorWorkContext`，供 Coordinator/DirectDispatcher 正常快速推进；
- `ResumeStep` 只接受 `InternalCallerContext`，并在 Mailbox 前校验当前 Work Lease；
- Gate、H5、GM 和普通 Service LB 查询接口均不能调用这两个方法；
- 两个入口复用同一 `ParticipantCommandCatalog`，不能形成两套业务实现。

逻辑返回结构冻结为：

```protobuf
enum ParticipantOutcome {
  PARTICIPANT_OUTCOME_UNSPECIFIED = 0;
  PARTICIPANT_APPLIED = 1;
  PARTICIPANT_REJECTED = 2;
  PARTICIPANT_NO_CHANGE = 3;
  PARTICIPANT_TIMEOUT_UNKNOWN = 4;
}

message ParticipantResponse {
  ParticipantCommandType participant_command = 1;
  string participant_invocation_id = 2;
  ParticipantOutcome outcome = 3;
  bool replayed_from_receipt = 4;
  bytes typed_result = 5;
  bytes participant_result_hash = 6;
  optional ParticipantFailure failure = 7;
  uint64 committed_record_version = 8;
}

message ParticipantFailure {
  ErrorDetail error = 1;
  bool retryable_after_resolution = 2;
}
```

字段语义：

| outcome | 是否 Participant 终态 | 是否有 Receipt | 含义 |
|---|---:|---:|---|
| `APPLIED` | 是 | 是 | 本步骤业务效果与 Receipt 已原子提交 |
| `REJECTED` | 是 | 是 | 业务前置条件明确失败；同一 Invocation 永久返回相同拒绝 |
| `NO_CHANGE` | 是 | 是 | 请求合法但目标已经处于无需改变的状态 |
| `TIMEOUT_UNKNOWN` | 否 | 未知 | 存储请求已发出但结果不明；必须进入第 30 号 Unknown 解析 |

`ParticipantFailure.error` 使用与内部业务错误注册表一致的稳定 ErrorCode，不使用自由字符串；它
描述等级不足、余额不足、PlotVersion 冲突等领域结论，或 `TIMEOUT_UNKNOWN` 的解析要求。Participant
不决定 H5 的最终文案和 RetryDirective，Coordinator 根据 Work 终态映射成第 28 号公开结果。

`replayed_from_receipt=true` 只表示本次 RPC 没有再次执行领域变更，而是从原 Receipt 返回原
`outcome + typed_result + participant_result_hash`。Route、WorkVersion、Lease、过载和 Deadline 等
执行前失败不伪装成 `PARTICIPANT_REJECTED`，它们使用内部 Route/Retry Failure；只有已进入 Actor
并形成确定业务结论的拒绝才写 ParticipantReceipt。

`participant_result_hash` 由 Participant Ingress 对“命令类型 + Typed Result 的语义字段”按固定
字段顺序、固定整数编码计算。不得 Hash 原始 Protobuf bytes。后续依赖步骤必须携带前一步的 Result
Hash，Ingress 再与 WorkRecord 中已确认的前一步结果比对，避免跨 Workflow、跨 Reservation 或旧响应
串线。

本节 field number 是逻辑布局；正式 `workflow_rpc.proto` 必须一次性分配并使用 `reserved` 管理演进。

### 7.8 核心经营场景 ParticipantCommandCatalog

以下十五个命令名、目标 Actor、WorkType 和允许状态属于稳定语义：

| ParticipantCommandType | 目标 | WorkType | 首次执行允许的 Work 状态 |
|---|---|---|---|
| `RESERVE_UNLOCK_COIN` | PlayerActor(owner) | UNLOCK_PLOT | CREATED |
| `COMMIT_PLOT_UNLOCK` | FarmActor(farm) | UNLOCK_PLOT | COIN_RESERVED |
| `COMMIT_UNLOCK_COIN_RESERVATION` | PlayerActor(owner) | UNLOCK_PLOT | FARM_COMMITTED |
| `RELEASE_UNLOCK_COIN_RESERVATION` | PlayerActor(owner) | UNLOCK_PLOT | FARM_REJECTED / FARM_NO_CHANGE |
| `RESERVE_SEED` | PlayerActor(owner) | PLANT | CREATED |
| `COMMIT_PLANT` | FarmActor(farm) | PLANT | SEED_RESERVED / FARM_COMMITTING |
| `COMMIT_SEED_RESERVATION` | PlayerActor(owner) | PLANT | FARM_COMMITTED |
| `RELEASE_SEED_RESERVATION` | PlayerActor(owner) | PLANT | FARM_REJECTED |
| `COMMIT_HARVEST` | FarmActor(farm) | HARVEST | CREATED / FARM_COMMITTING |
| `GRANT_HARVEST_REWARD` | PlayerActor(owner) | HARVEST | FARM_HARVEST_COMMITTED / PLAYER_REWARDING |
| `DECIDE_THEFT` | FarmActor(target farm) | THEFT | CREATED / FARM_DECIDING |
| `SETTLE_THEFT_VISITOR` | PlayerActor(visitor) | THEFT | FARM_DECIDED / VISITOR_SETTLING |
| `CREDIT_GUARD_INCOME` | PlayerActor(owner) | THEFT | VISITOR_SETTLED / OWNER_SETTLING |
| `APPLY_CARE_REWARD` | PlayerActor(carer) | CARE_REWARD | PENDING / REWARD_DISPATCHING |
| `APPLY_GUARD_SNAPSHOT` | FarmActor(owner farm) | GUARD_SYNC_ORDER | PENDING / GUARD_DISPATCHING |

`FARM_REJECTED` 与 `FARM_NO_CHANGE` 都表示 Work 已经得到 Farm 明确“没有提交目标变化”的终态，
并且已有 Reservation 需要释放；两者最终 CommandStatus 不同，因此 WorkRecord 不得合并状态。
Plant 当前只有 `FARM_REJECTED` 释放分支。RPC Timeout、路由失败和 `TIMEOUT_UNKNOWN` 都不能把 Work
推进到任一释放状态。P0 不提供 Workflow 接管后的用户取消分支。

Catalog 对每项必须声明：Typed Payload、Typed Result、目标 RouteDomain、允许 Caller Role、允许
WorkType/State、CanonicalStepPayloadBuilder、MaxPayloadBytes 和 Handler。启动测试必须保证枚举与
Catalog 一一对应；未知命令、Payload 类型错配和目标 RouteDomain 错配都在进入 Actor Mailbox 前拒绝。

同一 Work 中每一步的 InvocationID 确定性派生：

```text
participant_invocation_id
= StableOpaqueID(
    "farm-participant-invocation-v1",
    stable_work_type_name,
    work_id,
    stable_participant_command_name,
    "1"
  )
```

P0 每个上述步骤只出现一次，因此 `step_instance=1`。重试、Coordinator 切换为 Worker、Worker Lease
换代都不得改变 InvocationID。Finalize 与 Release 是两个互斥命令，各自有不同 InvocationID；是否允许
首次执行由当前 Work 状态与 Reservation 状态共同限制。

下文写“FarmChange 与业务事实同一次 Authority Commit”时，准确含义是：同一 CandidateState 已经
冻结 `PlotVersion/FarmVersion`、Typed Result/Result Hash 与唯一 CommittedFarmChange 候选；Store 不
持久化无界变化历史。确认提交后，由第 27/30 号 PostCommitDeliveryGuard 发布原候选，或使该 Farm
当前实时流失效。Participant 不得在提交后重新拼装另一份 Change。

### 7.9 UnlockPlot Typed Step Schema

#### 7.9.1 RESERVE_UNLOCK_COIN

请求语义字段：

```text
ReserveUnlockCoinPayload {
  player_id
  farm_id
  plot_id
  reservation_id
  required_player_level
  amount_normal_coin
  unlock_rule_version
  unlock_rule_snapshot_hash
}
```

`reservation_id = StableOpaqueID("farm-unlock-coin-reservation-id-v1", work_id)`。Step Hash 按上面全部字段及
`RESERVE_UNLOCK_COIN` 固定顺序生成。PlayerActor 必须重新检查 `player_id` 与 ActorRouteHeader、
Work subject 一致，当前等级满足要求，且可用普通金币不少于冻结价格；客户端提交的等级、价格或余额
不进入该 Payload。

成功结果：

```text
ReserveUnlockCoinResult {
  reservation_id
  reserved_amount_normal_coin
  available_normal_coin_after
  player_level_at_reserve
  player_record_version
}
```

`available coin → reserved`、Reservation、ParticipantReceipt 和 Player RecordVersion 必须一次
Authority Commit。等级不足或余额不足是持久 `REJECTED`；相同 Invocation 重试不能因为玩家后来升级或
获得金币而改变原结论。

#### 7.9.2 COMMIT_PLOT_UNLOCK

请求语义字段：

```text
CommitPlotUnlockPayload {
  owner_player_id
  farm_id
  plot_id
  expected_plot_version
  expected_unlock_rule_version
  unlock_order
  optional predecessor_plot_id
  reservation_id
  reserve_unlock_result_hash
  unlock_rule_snapshot_hash
}
```

`reserve_unlock_result_hash` 必须等于 WorkRecord 中 `COIN_RESERVED` 转移保存的前一步 Result Hash。
FarmActor 重新检查 Owner、PlotVersion、锁定状态、固定解锁顺序和规则快照；不读取或修改 Player 钱包。

成功结果：

```text
CommitPlotUnlockResult {
  farm_id
  plot_id
  applied_unlock_rule_version
  committed_plot_version
  committed_farm_version
  unlock_decision_hash
}
```

Plot `LOCKED → UNLOCKED`、解锁来源 workflow_id、ParticipantReceipt、Plot/Farm/RecordVersion 与
`PlotUnlocked FarmChange` 一次提交。相同 Work 重放返回原 Result；已被另一 Work 解锁可形成持久
`NO_CHANGE/ALREADY_UNLOCKED`，版本或前置土地不满足形成持久 `REJECTED`。

#### 7.9.3 COMMIT_UNLOCK_COIN_RESERVATION

```text
CommitUnlockCoinReservationPayload {
  player_id
  farm_id
  plot_id
  reservation_id
  amount_normal_coin
  commit_plot_unlock_result_hash
}

CommitUnlockCoinReservationResult {
  reservation_id
  reservation_state = CONSUMED
  normal_coin_balance
  player_record_version
}
```

Ingress 只有在 WorkRecord 已保存 `FARM_COMMITTED + commit_plot_unlock_result_hash` 时才允许首次执行。
PlayerActor 校验 Reservation 仍为 RESERVED、金额和 Work 绑定一致，然后只执行 `RESERVED → CONSUMED`；
不能再扣第二次金币。

#### 7.9.4 RELEASE_UNLOCK_COIN_RESERVATION

```text
ReleaseUnlockCoinReservationPayload {
  player_id
  farm_id
  plot_id
  reservation_id
  amount_normal_coin
  release_reason
  terminal_farm_result_hash
}

ReleaseUnlockCoinReservationResult {
  reservation_id
  reservation_state = RELEASED
  available_normal_coin_after
  player_record_version
}
```

只有 Work 已明确证明 Farm 未提交，且状态进入 `FARM_REJECTED` 或 `FARM_NO_CHANGE`，才能首次 Release。
`TIMEOUT_UNKNOWN`、`FARM_COMMITTING` 或
`FARM_COMMITTED` 均禁止释放。Release 与余额返还、ParticipantReceipt 必须同一次提交。

### 7.10 Plant Typed Step Schema

PlantWorkflow 创建时必须先冻结如下服务端规则对象；客户端 PlantRequest 只提交 farm/plot/crop 和
期望版本，不能提交种子 Item、成熟时长、产量、偷菜比例或护理时间：

```text
CropRuleSnapshot {
  schema_version
  crop_definition_id
  crop_rule_version
  seed_item_id
  harvest_item_id
  grow_duration_ms
  base_yield
  harvest_exp
  crop_unit_coin_value
  legacy_seed_plantable
  CompendiumUnlockRuleSnapshot {
    crop_definition_id
    unlock_eligible
    activity_id?
    catalog_version_at_plant
    definition_version_at_plant
  }
  StealPlanTemplate {
    policy_version
    total_cap_ratio_bp
    slot_ratio_bp[]
  }
  CarePlanTemplate {
    care_rule_version
    incidents[] {
      care_type
      ordinal
      schedule_mode
      fixed_offset_ms?
      random_window_start_ms?
      random_window_end_ms?
      random_algorithm_version?
    }
  }
}
```

FirstHop 对该结构按语义字段计算 `crop_rule_snapshot_hash` 并与 PlantWorkflow 一起保存。在途 Work、
重试和 Worker 恢复始终使用该快照，不能重新读取热更新配置替换它。图鉴身份和解锁资格属于
CropRuleSnapshot 的语义字段；不能等到 Harvest 时再读取当时的活动状态或 Catalog 重新决定。

`crop_rule_version` 是组合规则代次：种子/收获物品、成熟时长、基础产量、收获经验、保护折价、
StealPlan 或 CarePlan 任一语义变化都必须推进它。子规则版本用于审计和局部演进，不能替代顶层版本。
Hash 使用显式 Canonical Builder；相同版本却出现不同 Hash 属于配置完整性错误，必须 Fail Closed 并
告警，不能当普通 `RULE_VERSION_CHANGED` 继续执行。

P0 使用整数库存和十个 2% 偷菜槽位。所有启用该策略的作物配置必须满足：

```text
total_cap_ratio_bp = 2000
slot_ratio_bp = [200 x 10]
base_yield % 50 == 0
base_yield * slot_ratio_bp[i] % 10000 == 0
slot_quantity[i] > 0
sum(slot_quantity) == base_yield * total_cap_ratio_bp / 10000
```

配置加载阶段必须检查数组数量、整数除法、加法和乘法溢出；不满足时整版配置不可 Ready，禁止在
运行时临时四舍五入。活动结束不删除仍可能被遗留种子引用的 CropDefinition，而是关闭商店供给并以
`legacy_seed_plantable=true` 保留种植能力。

#### 7.10.1 RESERVE_SEED

```text
ReserveSeedPayload {
  player_id
  farm_id
  plot_id
  reservation_id
  crop_definition_id
  seed_item_id
  quantity = 1
  crop_rule_version
  crop_rule_snapshot_hash
}

ReserveSeedResult {
  reservation_id
  seed_item_id
  reserved_quantity
  available_seed_quantity_after
  player_record_version
}
```

`reservation_id = StableOpaqueID("farm-seed-reservation-id-v1", work_id)`。PlayerActor 只使用服务端冻结的
`seed_item_id`，并在 Durability Barrier 中原子提交 `available → reserved`、Reservation、Receipt 和
RecordVersion。库存不足是持久 `REJECTED`。

#### 7.10.2 COMMIT_PLANT

```text
CommitPlantPayload {
  owner_player_id
  farm_id
  plot_id
  expected_plot_version
  reservation_id
  reserve_seed_result_hash
  crop_cycle_id
  crop_rule_snapshot
  crop_rule_snapshot_hash
}
```

`crop_cycle_id = StableOpaqueID("farm-crop-cycle-id-v1", work_id)`。Ingress 重算规则快照 Hash，并校验它与
WorkRecord 及 `reserve_seed_result_hash` 一致。FarmActor 在真正提交 Turn 捕获一次
`trusted_now_ms` 作为 `plant_at`，计算 `mature_at`、整数 `slot_quantity[]` 和确定的 Care Incident；
请求不能携带客户端 `plant_at/mature_at`。

FarmActor 从模板生成并提交的运行事实为：

```text
StealPlanSnapshot {
  policy_version
  total_cap_ratio_bp
  total_cap_quantity
  slot_ratio_bp[]          // P0固定10项
  slot_quantity[]          // 已转换为整数且每项>0
}

CarePlanSnapshot {
  care_rule_version
  incidents[] {
    incident_id
    care_type
    ordinal
    due_at_ms
    resolved_at_ms?
    resolved_by_player_id?
  }
}

CompendiumUnlockSnapshot {
  crop_definition_id
  unlock_eligible
  planted_by_player_id
  activity_id?
  catalog_version_at_plant
  definition_version_at_plant
}
```

P0 `FIXED_OFFSET` 要求 `0 < fixed_offset_ms < grow_duration_ms`，并使用
`incident_id = StableOpaqueID("farm-care-incident-id-v1", crop_cycle_id, stable_care_type_name,
canonical_ordinal)`。未来
`DETERMINISTIC_RANDOM_WINDOW` 只允许在 Plant 提交时选择一次最终 `due_at_ms`；选择结果写入
CarePlanSnapshot，恢复时不重新读取窗口或重新抽取。随机算法/密钥只能作为内部版本信息，原始随机
种子不得进入公网 FarmChange。`CompendiumUnlockSnapshot.planted_by_player_id` 由 FarmActor 使用受信
`owner_player_id` 写入，不能来自客户端；完整 Snapshot 随 CropCycle 保存并参与
`plant_decision_hash`。活动归档、Catalog 换版或 Worker 恢复均不得改写它。

成功结果：

```text
CommitPlantResult {
  farm_id
  plot_id
  crop_cycle_id
  crop_definition_id
  applied_crop_rule_version
  plant_at_ms
  mature_at_ms
  committed_plot_version
  committed_farm_version
  plant_decision_hash
}
```

CropCycle、CarePlanSnapshot、StealPlanSnapshot、ParticipantReceipt、FarmChange、
`opportunity_dirty_generation++` 与
Plot/Farm/RecordVersion 同一次 Authority Commit。机会摘要使用第 31 号每 Farm/ProjectionKind 合并刷新，
不为每次 Plant 新建投影 Order。Plot 未解锁、非空、版本冲突或规则快照不一致形成
持久 `REJECTED`；同一 Invocation 重放不得重新选择时间或生成新 Cycle。

#### 7.10.3 COMMIT_SEED_RESERVATION

```text
CommitSeedReservationPayload {
  player_id
  farm_id
  plot_id
  crop_cycle_id
  reservation_id
  commit_plant_result_hash
}

CommitSeedReservationResult {
  reservation_id
  reservation_state = CONSUMED
  seed_item_id
  available_seed_quantity
  player_record_version
}
```

只有 WorkRecord 已保存 `FARM_COMMITTED + commit_plant_result_hash` 才允许首次执行。该步骤仅把
Reservation `RESERVED → CONSUMED` 并写 Receipt，不再次减少 Inventory，也不发 Plant 经验。

#### 7.10.4 RELEASE_SEED_RESERVATION

```text
ReleaseSeedReservationPayload {
  player_id
  farm_id
  plot_id
  reservation_id
  seed_item_id
  quantity = 1
  release_reason
  terminal_farm_result_hash
}

ReleaseSeedReservationResult {
  reservation_id
  reservation_state = RELEASED
  seed_item_id
  available_seed_quantity_after
  player_record_version
}
```

Release 仅允许在 Farm 明确未提交后执行，并原子恢复 available seed。Farm Commit 结果未知时先解析
`COMMIT_PLANT` Receipt；禁止用 RPC Timeout 直接触发 Release。若 Farm 已提交，只能继续
`COMMIT_SEED_RESERVATION`。

### 7.11 Step Hash、依赖 Hash 与结果映射

上述命令都采用显式 Canonical Builder：

```text
CanonicalStepPayload =
  schema_version
  + participant_command
  + 按本节声明顺序编码的全部语义字段

CanonicalStepPayloadHash = SHA-256(CanonicalStepPayload)
```

长度可变字段使用 length-prefix；整数使用固定无符号/有符号宽度；Optional 字段先编码 presence bit；
Repeated 字段先编码元素数量再逐项编码。不得 Hash 原始 Protobuf bytes、map 的遍历顺序、Trace、Deadline、
Lease、Route Revision 或 remaining_budget。这些运行字段变化不代表业务步骤发生变化。

每一步终态提交后：

```text
Participant业务效果或确定拒绝
+ ParticipantReceipt(invocation_id, step_payload_hash, outcome, typed_result, result_hash)
+ Actor RecordVersion
→ 同一次Authority Commit
```

Coordinator/Worker 收到终态后，先把 `outcome + result_hash` 以 CAS 写入唯一 WorkRecord 并推进
WorkVersion，再创建下一步 Payload。若这次 Work CAS 失败，读取最新 Work 并复用相同 Invocation/Result，
不能重新执行已经完成的 Actor 效果。

### 7.12 Harvest Typed Step Schema

Harvest 不预留资产，也不存在 Farm 提交后的退款。FarmActor 是最终产量的唯一裁决者；Coordinator
不能根据客户端页面或旧 Snapshot 预先计算奖励。

#### 7.12.1 COMMIT_HARVEST

```text
CommitHarvestPayload {
  owner_player_id
  farm_id
  plot_id
  crop_cycle_id
  expected_plot_version
}
```

Step Hash 包含 `COMMIT_HARVEST` 和上述全部语义字段。FarmActor 在提交 Turn 捕获一次
`trusted_now_ms`，重新检查 Owner、Plot/Cycle、PlotVersion、Cycle 仍为 ACTIVE，以及
`trusted_now_ms >= mature_at`。最终数量只能按该 CropCycle 已冻结事实计算：

```text
reward_quantity = base_yield - stolen_amount
0 <= stolen_amount <= stealable_limit <= base_yield
```

成功结果：

```text
HarvestDecision {
  workflow_id
  owner_player_id
  farm_id
  plot_id
  crop_cycle_id
  crop_definition_id
  reward_item_id
  reward_quantity
  reward_exp
  crop_rule_version
  compendium_unlock_snapshot
  harvest_decision_hash
}

CommitHarvestResult {
  harvest_decision
  committed_plot_version
  committed_farm_version
  farm_record_version
}
```

`HarvestDecision` 的 Hash 使用除 `harvest_decision_hash` 自身之外的全部语义字段显式计算，包括从
CropCycle 原样复制的 `compendium_unlock_snapshot`。FarmActor 不在 Harvest 时重新读取 Catalog。
CropCycle
`ACTIVE → ENDED(HARVESTED)`、Plot
清空、HarvestDecision、ParticipantReceipt、FarmChange、`opportunity_dirty_generation++` 和版本推进
必须一次 Authority Commit。机会摘要使用合并刷新/Reconcile，不为每次 Harvest 新建投影 Order。
未成熟、Cycle/Version 不匹配形成持久 `REJECTED`；同一 Invocation 重放
返回原 Decision，不重新读取已经清空的 Plot。

#### 7.12.2 GRANT_HARVEST_REWARD

```text
GrantHarvestRewardPayload {
  player_id
  commit_harvest_result_hash
  harvest_decision
}

CompendiumUnlockDelta {
  newly_unlocked
  crop_definition_id?             // 仅首次解锁时存在
  compendium_revision_after
}

GrantHarvestRewardResult {
  crop_cycle_id
  reward_item_id
  granted_quantity
  inventory_quantity_after
  granted_exp
  player_level_after
  harvest_count_after
  compendium_unlock_delta
  player_record_version
}
```

Ingress 必须同时验证：

- WorkRecord 已处于 `FARM_HARVEST_COMMITTED/PLAYER_REWARDING`；
- `commit_harvest_result_hash` 等于 WorkRecord 保存的 Farm Participant Result Hash；
- `harvest_decision_hash` 对当前 Decision 重算一致；
- player_id、owner、workflow_id 和 crop_cycle_id 绑定一致；
- `compendium_unlock_snapshot` 的 crop、owner 和 `planted_by_player_id` 与当前 HarvestDecision/PlayerActor
  一致；PlayerActor 不为本步骤重新读取当前 Catalog。

PlayerActor 在一个 Authority Commit 中提交 Inventory、经验/等级、HarvestCounter、首次自种收获图鉴
解锁、原始 `CompendiumUnlockDelta`、RewardReceipt、ParticipantReceipt、StoredOperationOutcome 和
RecordVersion。`CompendiumUnlockDelta` 必须同时进入 ParticipantReceipt 与 StoredOperationOutcome；
重复恢复返回原 operation 首次提交时的 true/false、crop_definition_id 和 revision，不能根据当前集合
重新计算。P0 Inventory 无容量上限，因此 Farm 已提交
后的该步骤不得因“仓库满”产生业务拒绝；内部数据不一致进入 `RECONCILE_REQUIRED`，不能把已清空的
CropCycle 退回给玩家。重复调用只返回原 Result，不重复增加任一奖励或任务进度。

只有 `GRANT_HARVEST_REWARD` Receipt 已提交，HarvestWorkflow 才能进入 `PLAYER_REWARDED →
COMPLETED` 并向公网返回 `SUCCEEDED`。Farm 已提交而 Player 未完成时统一为 `PROCESSING`。

### 7.12A Friendship ActionAdmission RPC

好友 Care/Theft 在进入 FarmActor 或创建 TheftWorkflow 前，必须先调用 Pair Authority 的强准入入口。
以下为逻辑 IDL；最终 field number 仍服从统一 Proto 生成闸门：

```protobuf
service FriendshipAuthorityService {
  rpc CommitFriendActionAdmission(CommitFriendActionAdmissionRequest)
      returns (CommitFriendActionAdmissionResponse);
  rpc GetFriendActionAdmission(GetFriendActionAdmissionRequest)
      returns (GetFriendActionAdmissionResponse);
  rpc AuthorizePreparedVisit(AuthorizePreparedVisitRequest)
      returns (AuthorizePreparedVisitResponse);
}

message CommitFriendActionAdmissionRequest {
  InternalCallerContext caller = 1;
  TrustedCommandContext trusted_command = 2;
  EffectiveIdempotencyKey idempotency_key = 3; // root=FRIEND_PAIR:canonical_pair
  uint64 owner_player_id = 4;
  uint64 target_farm_id = 5;
  FriendActionScope action_scope = 6; // CARE | THEFT
  bytes canonical_payload_hash = 7;
  bytes target_binding = 8;           // 规范编码后的plot/cycle/incident等
  WorkAddress continuation = 9;
  int64 deadline_unix_ms = 10;
}

message CommitFriendActionAdmissionResponse {
  FriendActionAdmissionStatus status = 1;
  string admission_receipt_id = 2;
  ActionPermitSnapshot permit = 3;
  WorkAddress dispatch_work_address = 4;
  ErrorDetail error = 5;
}
```

`caller` 必须是受信 farm-svr FirstHop/Coordinator 身份；公网 H5 不能直接调用该 RPC。
`trusted_command` 证明本次命令已经通过 Session、Recovery 和当前 FarmAdmissionContext，操作者 ID 只能从
其中取得。Pair Authority 根据 visitor/owner 计算 canonical pair，不能相信调用方自报 relation_id 或
relation_epoch。

一次 COMMITTED 原子保存：

```text
ActionAdmissionReceipt
+ ActionPermitSnapshot
+ FriendActionDispatchOrder(PENDING，内嵌完整不可变ActionPermitSnapshot及其hash)
+ 同AuthorityPersistenceBucket DueIndex
+ StoredAdmissionOutcome
```

该提交就是第 28 号 `FRIEND_ACTION_ADMISSION_COMMITTED`。Response 丢失或 Redis 超时未知时，调用方
使用原 EffectiveIdempotencyKey 调用 Get/重试 Commit；命中 Receipt 后返回同一 Permit 和 WorkAddress，不能创建
第二个 Dispatch。相同 Key 但 payload hash 或 target binding 不同返回 `IDEMPOTENCY_CONFLICT`。

`target_binding` 必须使用 CommandCatalog 的 Canonical Payload Builder 产生规范字节/Hash，不能把任意
客户端 bytes 原样写进受信 Permit。CARE 至少绑定 plot/crop_cycle/incident/care_type；THEFT 至少绑定
plot/crop_cycle。最终 Farm Participant 仍校验自己的权威状态。

`FriendActionDispatchOrder` 必须保存完整的不可变 `ActionPermitSnapshot`、`permit_hash`、规范
`target_binding`、`canonical_payload_hash` 和 continuation。AdmissionReceipt 可以只保存
`permit_hash + dispatch_work_address` 作为定位引用，但不能要求 Worker 恢复时重新读取当前关系并重签 Permit。

DirectDispatcher 使用 `continuation` 立即推进；farm-worker 使用 `dispatch_work_address`、可信 Work Lease
和相同 InvocationID 恢复。CARE continuation 直接进入 Farm Participant，THEFT continuation
CreateIfAbsent 同一 TheftWorkflow。AdmissionOrder 在最终调用产生可查询结果或可靠下游 Work 已接管后
完成，不能在“发出一次 RPC”时提前删除。

### 7.12B Friendship Pair、Quota 与 ShareIssuer RPC 边界

FriendshipDomain P0 与 player-svr 共部署，不代表调用方可以绕过逻辑 RPC 边界直接拼 Redis Key。为了
允许未来独立扩容并防止不同 Agent 实现出不同 Fence，至少冻结三组接口：

```text
FriendshipPairAuthorityService
├── ValidatePairForAdd(WorkflowCallerContext, canonical_pair, token_generation, invocation_id)
├── ActivateFriendRelation(WorkflowCallerContext, relation_candidate, token_generation, invocation_id)
├── GetReactivationBarrierProof(TrustedCommandContext, peer_player_id)
├── CommitReactivationGenerationFloors(InternalCallerContext, pair_id,
│       expected_deleted_relation_epoch, barrier_revision, low_floor, high_floor)
├── DeleteFriend(TrustedCommandContext, peer_player_id, EffectiveIdempotencyKey)
├── BlockPlayer(TrustedCommandContext, peer_player_id, EffectiveIdempotencyKey)
├── UnblockPlayer(TrustedCommandContext, peer_player_id, EffectiveIdempotencyKey)
├── AuthorizePreparedVisit(TrustedCommandContext,
│       prepared_join_ref, join_operation_id, join_attempt_id,
│       DownlinkTargetFence)
└── CommitFriendActionAdmission(...)

FriendQuotaAuthorityService
├── ReserveFriendSlot(WorkflowCallerContext, relation_id,
│       target_relation_epoch, participant_invocation_id)
├── FinalizeFriendSlot(WorkflowCallerContext, relation_id,
│       target_relation_epoch, participant_invocation_id)
├── ReleaseFriendReservation(InternalCallerContext, reservation_id,
│       participant_invocation_id)
└── RemoveActiveRelationIfEpochMatches(InternalCallerContext, relation_id,
        captured_active_epoch, participant_invocation_id)

FriendShareIssuerService
├── Generate/Rotate/Revoke(TrustedCommandContext, EffectiveIdempotencyKey)
├── RotateForPairReactivation(TrustedCommandContext, peer_player_id,
│       ReactivationBarrierProof, EffectiveIdempotencyKey)
├── AcceptShareTokenAdmission(WorkflowCallerContext,
│       token, inviter, redeemer, operation_id, payload_hash)
└── GetGenerationSnapshot(InternalCallerContext, player_id)
```

`DeleteFriend/BlockPlayer` 不得为了等待两个 ShareIssuer 而推迟关系撤销。Pair Authority 先原子提交
`DELETED/BLOCKED + reactivation_barrier=PENDING + FriendTerminationOrder`，此刻立即停止签发新的
VisitPermit/ActionPermit并可以向 H5 返回成功。TerminationOrder 随后读取双方 ShareIssuer Generation，
再以 `expected_deleted_relation_epoch` 把 floor 提交回同一 Pair；两边都完成后 Barrier 才进入 READY。
PENDING 时 `ValidatePairForAdd/ActivateFriendRelation` 一律拒绝；READY 后 Token generation 必须严格大于
对应 floor，并要求邀请者显式 `RotateForPairReactivation` 产生绑定当前 barrier 的 RotateReceipt 和新链接。
重新激活成功时，`ActivateFriendRelation` 必须在 Pair 同一次权威提交中把当前 Barrier 从
READY 切换为 CONSUMED，使同一 `barrier_revision` 不能被第二个 Workflow 消费。
普通 Rotate 不生成该 Pair 证明。不得比较不同 Authority 的墙钟。

`AcceptShareTokenAdmission` 必须
以 Token Authority 的一次提交同时校验 ACTIVE generation 并保存 ValidationReceipt。Receipt 先提交时，
后续 Rotate/Revoke 不反向取消原 Workflow。

Quota Participant 不使用 SessionPermit；其调用身份来自 `WorkflowCallerContext + WorkAddress +
participant_invocation_id`。预约释放与已激活关系删除是两个不同命令：前者按 reservation_id，后者必须按
`relation_id + captured_active_epoch` 条件删除。Finalize 必须按 `target_relation_epoch` 匹配，均不得
使用裸 count 或裸 relation_id。

Pair、Quota、ShareIssuer 是非 Actor Authority：调用仍需服务身份、Deadline、EffectiveIdempotencyKey、payload hash
和 Unknown 解析，但不携带伪造的 ActorRouteHeader、WriterClaim 或 Actor Mailbox 身份。

### 7.13 Theft Typed Step Schema

Theft 先通过 `CommitFriendActionAdmission` 取得与本次 operation 绑定且已持久提交的内部
ActionPermit，再由对应 DispatchOrder 可靠创建 TheftWorkflow。Permit 不是客户端 Token，H5 不能提交
或复用它。

```text
ActionPermitSnapshot {
  permit_id
  visitor_player_id
  owner_player_id
  target_farm_id
  operation_id
  action_scope = THEFT
  friend_relation_epoch
  canonical_payload_hash
  target_binding_hash
  issued_at_authority_ms
  expire_at_authority_ms
  execution_accepted_at_authority_ms
}
```

Admission Commit 时必须满足 FriendRelation 仍有效。Workflow 保存 Permit、AdmissionReceipt ID、
其 Hash 以及相同的 `execution_accepted_at_authority_ms`，不能在 Workflow 创建时重新选择接受时间。
Farm Participant 校验 Permit 在该 operation 越过 Execution-Accept 时有效，而不是在 Worker 恢复时按
当前时间重新使已接管业务过期。好友随后删除或拉黑只阻止新 operation，不取消已经可靠接管的旧 Work；
该语义服从第 27 号 6.3.1 和第 33 号 Q33-13。

#### 7.13.1 DECIDE_THEFT

```text
DecideTheftPayload {
  visitor_player_id
  owner_player_id
  farm_id
  plot_id
  crop_cycle_id
  theft_attempt_id
  settlement_id
  action_permit_snapshot
  action_permit_snapshot_hash
}
```

`theft_attempt_id = StableOpaqueID("farm-theft-attempt-id-v1", canonical_farm_id, crop_cycle_id,
canonical_visitor_player_id)`，使同一好友同一茬只有一个
业务尝试身份，不因更换 operation_id 得到新的宠物抽取机会；
`settlement_id = StableOpaqueID("farm-theft-settlement-id-v1", work_id)`。Ingress 校验 Permit 与 Work、operation、
参与双方、目标 Farm 和 `THEFT` scope 完整绑定。FarmActor 在提交 Turn 重新检查 Cycle、成熟时间、
AttemptReceipt、可偷槽位、GuardSnapshot 和全部产量不变量；客户端不能指定偷取数量、槽位、宠物结果
或金币单价。

守护规则不在 Plant 时冻结。FarmActor 使用本次裁决时已经同步生效的当前 GuardSnapshot：

```text
GuardSnapshot {
  schema_version
  guard_version
  active_guard_pet_id?
  guard_until_ms
  GuardRuleSnapshot {
    pet_config_version
    guard_rule_version
    trigger_probability_bp
    intercept_policy          // P0 = FULL_GROSS
    random_algorithm_version
    random_key_version
  }
  snapshot_hash
}

GuardDecisionSnapshot {
  theft_attempt_id
  guard_version
  guard_rule_version
  trigger_probability_bp
  intercept_policy
  random_algorithm_version
  random_key_version
  random_draw_bp              // 私有审计字段，不下发公网
  guard_triggered
  gross_stolen_quantity
  guard_lost_quantity
  net_received_quantity
  crop_unit_coin_value
  guard_coin_value
}
```

首次计算推荐使用版本化服务端 PRF：

```text
random_draw_bp =
  HMAC(server_secret[random_key_version],
       theft_attempt_id,
       guard_version,
       guard_rule_version) % 10000

guard_triggered = random_draw_bp < trigger_probability_bp
```

PRF 只负责首次生成；已经提交后，Receipt 中的 GuardDecisionSnapshot 才是恢复权威。密钥内容、原始
随机种子和 draw 不得进入 FarmChange 或客户端结果。Guard 配置或宠物状态后续变化只影响新的 Attempt，
不能改变已存在的 AttemptReceipt。

FarmActor 形成并持久化唯一 Decision：

```text
TheftTargetDecision {
  workflow_id
  settlement_id
  theft_attempt_id
  visitor_player_id
  owner_player_id
  farm_id
  plot_id
  crop_cycle_id
  reward_item_id
  steal_slot_index
  applied_slot_ratio_bp
  gross_stolen_quantity
  guard_lost_quantity
  net_received_quantity
  crop_unit_coin_value
  guard_coin_value
  theft_outcome
  crop_rule_version
  guard_decision_snapshot
  theft_decision_hash
}

DecideTheftResult {
  theft_target_decision
  committed_plot_version
  committed_farm_version
  farm_record_version
}
```

`theft_decision_hash` 对 Decision 中除自身之外的全部语义字段计算，包含完整
GuardDecisionSnapshot，不能形成自引用 Hash。

必须满足：

```text
net_received_quantity = gross_stolen_quantity - guard_lost_quantity
guard_coin_value = guard_lost_quantity * crop_unit_coin_value
stolen_amount_after <= stealable_limit <= base_yield
owner_claimable_after = base_yield - stolen_amount_after
```

乘法必须执行溢出检查。P0 守护只有“不截留”或“完整截留本槽位”两种结果，但 Decision 保留以后部分
截留的字段。目标产量、AttemptReceipt、TheftTargetDecision、ParticipantReceipt、FarmActivity、
FarmChange、`opportunity_dirty_generation++` 和版本推进一次 Authority Commit。机会摘要使用合并
刷新/Reconcile，不为每次 Theft 新建投影 Order。合法 Attempt 一旦提交，
即使 `net_received_quantity=0` 也已经消耗本好友本茬机会。

ActionAdmission 明确拒绝时不创建 Work，也不调用 Farm Participant；Admission 已提交后，DispatchOrder
必须最终创建或命中同一 Work，不能因为关系随后删除而放弃。Work 已可靠创建后，Permit 字段或 Hash
不匹配属于内部合同错误并在 Mailbox 前拒绝；FarmActor 的未成熟、旧 Cycle
等业务结论形成持久 `REJECTED` 且不写 TheftAttemptReceipt，份额恰好被其他请求耗尽可以形成持久
`NO_CHANGE`。命中已有 ParticipantReceipt 时，即使当前时间、关系、宠物状态或 Guard 配置已经变化，
也返回原 Decision，不重新抽随机结果。同一 CropCycle 的不同好友 Attempt 可以使用不同的
GuardSnapshot/GuardRuleVersion，这是主人在作物生命周期内喂养或切换宠物的正常结果。

#### 7.13.2 SETTLE_THEFT_VISITOR

```text
SettleTheftVisitorPayload {
  visitor_player_id
  decide_theft_result_hash
  theft_target_decision
}

SettleTheftVisitorResult {
  settlement_id
  reward_item_id
  granted_net_quantity
  inventory_quantity_after
  friend_interaction_fact_committed
  friend_interaction_task_advanced
  player_record_version
}
```

Ingress 校验 WorkRecord 已处于 `FARM_DECIDED/VISITOR_SETTLING`，Farm Participant Result Hash 和
`theft_decision_hash` 均一致。PlayerActor 在同一 Authority Commit 中写入净作物、
TheftVisitorReceipt、好友互动事实/首版同 Authority Group 的任务进度、ParticipantReceipt 和版本。
`net_received_quantity=0` 时 Inventory 不变，但 Receipt 与合法互动事实仍必须提交；不能因为被宠物全部
截留就允许重新偷一次。

#### 7.13.3 CREDIT_GUARD_INCOME

```text
CreditGuardIncomePayload {
  owner_player_id
  decide_theft_result_hash
  settle_visitor_result_hash
  theft_target_decision
}

CreditGuardIncomeResult {
  settlement_id
  credited_normal_coin
  normal_coin_balance_after
  player_record_version
}
```

该步骤只在 `guard_coin_value > 0` 时存在，并要求 WorkRecord 已保存 `VISITOR_SETTLED +
settle_visitor_result_hash`。Owner PlayerActor 原子提交普通金币、GuardIncomeReceipt、ParticipantReceipt
和版本；它绝不能向访问者钱包扣款。`guard_coin_value=0` 时 Workflow 从 VISITOR_SETTLED 直接完成，
不得构造一个金额为零的 Owner Participant 调用。

#### 7.13.4 Theft 完成边界

```text
DECIDE_THEFT明确拒绝/无变化
→ 不调用两个Player Participant
→ Workflow发布原REJECTED/NO_CHANGE

Farm已决定、Visitor未结算
→ PROCESSING

Visitor已结算、Owner金币步骤仍Pending
→ PROCESSING

Visitor已结算且guard_coin_value=0
→ COMPLETED

Visitor与必要Owner步骤均有Receipt
→ COMPLETED / SUCCEEDED
```

Farm 决策是不可逆点；之后不存在把 gross 数量退回地块、删除 AttemptReceipt 或允许换 operation_id
重偷的补偿。Coordinator/Worker 只能继续推进原 settlement。

### 7.14 CareRewardOrder 与 APPLY_CARE_REWARD

好友 Care 在调用 FarmActor 前先提交 `CommitFriendActionAdmission`；其
FriendActionDispatchOrder 直接调用 `ResolveCareIncident`，不创建独立 CareWorkflow。Owner 在自己农场
护理不经过 FriendshipPairAuthority，仍以 FarmActor Mailbox 接受为 Execution-Accept。

Care 与前述四类跨 Actor Workflow 不同：FarmActor 在准入后的玩家命令主链中完成 Incident 裁决，并把
`CareRewardOrder` 与 RESOLVED 事实同一次来源 Authority Commit。Order 是奖励投递的唯一恢复锚点，
Farm Authority 不再为奖励创建 CareWorkflow、奖励 Outbox 或第二套奖励 DueIndex；好友准入自身的
Pair Authority DispatchOrder 只负责把原 Care operation 可靠送到 Farm，不承担玩家奖励。

#### 7.14.1 来源 CareResolutionSnapshot

Farm 提交时冻结：

```text
CareResolutionSnapshot {
  source_farm_id
  source_owner_player_id
  carer_player_id
  plot_id
  crop_cycle_id
  incident_id
  care_type
  care_context            // OWNER | FRIEND
  resolved_at_ms
  committed_plot_version
  committed_farm_version
  care_reward_rule_version
  reward_exp
  friend_interaction_eligible
  care_resolution_hash
}
```

同一次 Farm Authority Commit 还必须保存业务防重凭证：

```text
CareOperationReceipt {
  operation_id
  canonical_payload_hash
  source_farm_id
  carer_player_id
  plot_id
  crop_cycle_id
  incident_id
  care_type
  outcome = RESOLVED
  resolved_at_ms
  committed_plot_version
  committed_farm_version
  care_reward_order_id
  care_resolution_hash
}
```

`CareOperationReceipt` 固定 Care 业务提交结果；第 30 号的裸 `ResolutionReceipt` 只用于
`FenceAndClaimActor` 对 `TIMEOUT_UNKNOWN` 的解析，二者不能共用结构或 Key kind。

Incident 的 resolved 字段、CareResolutionSnapshot、CareOperationReceipt、CareRewardOrder、
`PlotVersion/FarmVersion`、FarmOpportunityHeader/dirty generation，以及候选 CommittedFarmChange 的
语义必须来自同一 CandidateState。Store 只持久化有界权威事实；提交后的 Change 由第 27/30 号
PostCommitDeliveryGuard 交给实时层或使该 Farm 当前 Subscription 失效。

公开 Care StoredOperationOutcome 只保存稳定 `care_reward_order_id` 与
`reward_obligation_created=true`；Order 的 `PENDING/APPLIED` 当前状态属于私有查询视图，不得在完成后
回写并改变原 operation 的终态 Result。

`care_resolution_hash` 对除自身之外的全部语义字段显式计算。`care_context=OWNER` 时必须满足
`carer_player_id == source_owner_player_id` 且 `friend_interaction_eligible=false`；`care_context=FRIEND`
时必须由绑定该 operation 的有效 ActionPermit 产生，并满足双方不同、
`friend_interaction_eligible=true`。Player Participant 不重新查询 FriendRelation，也不能自行改变该
上下文。

CarePlan 的 `due_at_ms` 已在 Plant 时冻结；护理奖励规则则在首次 RESOLVED 的 Farm Turn 冻结到
CareResolutionSnapshot。这样热更新不会改变已经提交的奖励 Order，而新的护理可以使用新的
`care_reward_rule_version`。奖励规则至少固定 `reward_exp` 和是否产生好友互动事实；异步
APPLY_CARE_REWARD 不得重新读取当前配置计算另一份奖励。

CareRewardOrder 使用第 31 号确定性 SourceOrder 身份：

```text
work_type = CARE_REWARD
source_authority = {
  authority_kind = FARM_ACTOR
  canonical_authority_id = decimal(source_farm_id)
  authority_layout_version = auth-v1
  actor_route_domain = FARM
}
source_fact_id = incident_id
source_fact_version = committed_farm_version

order_id = StableSourceOrderID(
  CARE_REWARD, FARM_ACTOR, source_farm_id,
  incident_id, committed_farm_version
)
```

Order Payload 保存完整 CareResolutionSnapshot 及 Hash；DueIndex 只保存紧凑 WorkRef。相同 Incident
只能由首次 RESOLVED 提交创建同一 Order，`NO_CHANGE/ALREADY_RESOLVED` 不创建 Order。

#### 7.14.2 APPLY_CARE_REWARD

```text
ApplyCareRewardPayload {
  carer_player_id
  care_reward_order_id
  care_resolution_snapshot
  care_resolution_hash
}

ApplyCareRewardResult {
  care_reward_order_id
  incident_id
  granted_exp
  player_exp_after
  player_level_after
  friend_interaction_fact_committed
  friend_interaction_task_advanced
  task_period_key
  player_record_version
}
```

Participant InvocationID 仍按 `work_type + order_id + APPLY_CARE_REWARD + step_instance=1` 确定性派生。
Ingress 校验 WorkAddress 指向来源 Farm Authority Bucket，Order 仍处于 `PENDING/REWARD_DISPATCHING`，
Typed Payload 与 Order 中冻结 Snapshot/Hash 完全一致，ActorRouteHeader 指向 carer PlayerActor。

PlayerActor 在一个 Authority Commit 中提交：

```text
Exp/Progression += reward_exp
CareRewardReceipt(order_id, incident_id)
ParticipantReceipt(APPLY_CARE_REWARD)
FriendInteractionFact(order_id, occurred_at=resolved_at_ms) // 仅FRIEND
匹配的P0 TaskProgress                              // 仅FRIEND且同Authority Group
Player RecordVersion++
```

每日任务归属使用 `resolved_at_ms` 按任务配置时区计算的 `task_period_key`，不能使用奖励实际投递时间。
如果奖励因故障延迟到次日，旧事件不能错误推进新一天任务；是否保留或忽略已关闭 Period 的任务进度由
后续任务合同决定，但 CareRewardReceipt 与基础经验仍必须完成。

Farm 已 RESOLVED 后，`APPLY_CARE_REWARD` 属于必须最终应用的业务效果。P0 不允许 PlayerActor 用普通
业务拒绝丢弃奖励；内部数据/Hash 不一致进入 `RECONCILE_REQUIRED`。相同 Invocation 重试命中 Receipt，
不重复增加经验、互动事实或任务进度。

#### 7.14.3 Order 完成与投影解耦

CareRewardOrder 的业务状态冻结为：

```text
PENDING
→ REWARD_DISPATCHING      // Worker持Lease时的执行态；DirectDispatcher可从PENDING快速投递
→ PLAYER_REWARD_GRANTED
→ COMPLETED
```

DirectDispatcher 得到 Participant 终态后，以 Result Hash CAS 更新来源 Order；若更新响应丢失，worker
重试相同 Invocation，从 Player Receipt 取得原结果后完成 Order。`TIMEOUT_UNKNOWN` 先解析 Player
Receipt，不得直接把 Order 标成完成或再次发奖。

好友机会摘要不再作为 CareRewardOrder 的第二个必达步骤。Farm 的 RESOLVED Authority Commit 同时：

```text
opportunity_dirty_generation++
→ 每Farm/ProjectionKind最多一条可合并OpportunitySummaryRefresh状态
→ Projection Worker按max(required_generation)重建Summary
→ 写Projection Redis
```

投影失败不会阻止 CareRewardOrder 在玩家奖励完成后结束，也不会回滚 Care。Reconcile 可以根据
`dirty_generation > projected_generation` 重建；禁止为每次 Care 再创建一条不可合并的
OpportunitySummaryUpdateOrder。该规则服从第 31 号 `REBUILDABLE_PROJECTION` 合同。

### 7.14A GuardSyncOrder 与 APPLY_GUARD_SNAPSHOT

GuardSyncOrder 是 PlayerActor 来源的 `BUSINESS_REQUIRED` SourceOrder。正常快速路径由 player-svr 的
DirectDispatcher 发起；故障恢复由 farm-worker 发起。二者都调用同一个 Typed Participant，不允许
直接写 Farm Redis：

```text
APPLY_GUARD_SNAPSHOT {
  order_id
  source_player_id
  target_farm_id
  guard_snapshot
  snapshot_hash
}

ApplyGuardSnapshotResult {
  guard_version
  snapshot_hash
  apply_status                 # APPLIED | SUPERSEDED
  committed_record_version
  public_appearance_changed
  committed_farm_version?      # 仅公开外观实际变化时存在
}

GuardApplyReceipt {
  participant_invocation_id
  order_id
  guard_version
  snapshot_hash
  apply_status
  committed_record_version
  committed_farm_version?
}
```

Participant Ingress 在进入 FarmActor Mailbox 前必须同时验证：

```text
work.work_type == GUARD_SYNC_ORDER
work.placement_kind == SOURCE_PERSISTENCE_BUCKET
work.source_authority.authority_kind == PLAYER_ACTOR
work.source_authority.actor_route_domain == PLAYER
work.source_authority.canonical_authority_id == canonical(source_player_id)
target_farm_id == 当前FarmActor.actor_id
source_player_id == 当前FarmActor.owner_player_id
payload.guard_snapshot.guard_version == SourceOrder.source_fact_version
snapshot_hash == CanonicalGuardSnapshotHash(guard_snapshot)
```

P0 `own_farm_id == player_id`，但实现仍应按显式 owner 字段/Actor Key 交叉校验，不能因为数值通常相同就
省略来源身份校验。任一不匹配都属于内部合同错误，在 Mailbox 前 Fail Closed 并告警。

FarmActor Turn 的唯一状态机：

```text
incoming.guard_version > current.guard_version
→ 安装完整GuardSnapshot + GuardApplyReceipt
→ 若公开guard_pet_appearance实际改变：record_version++、farm_version++、生成一份FarmMetaDelta
→ 否则仅record_version++，不产生FarmVersion/FarmChange
→ Participant APPLIED

incoming.guard_version == current.guard_version && hash相同
→ 命中Receipt或等价当前状态，APPLIED重放，不推进版本

incoming.guard_version < current.guard_version
→ 写有界ParticipantReceipt，NO_CHANGE + typed apply_status=SUPERSEDED

incoming.guard_version == current.guard_version && hash不同
→ REJECTED/INVARIANT_VIOLATION，不覆盖并高优告警
```

Participant 的 `APPLIED/SUPERSEDED` 终态由 Dispatcher/worker 以来源同槽 CAS 完成原 Order，并单调推进
Player Authority 中的 `GuardSyncWatermark`。旧 SUPERSEDED 结果不能覆盖更高版本 APPLIED 水位。
Order 状态和水位是当前同步观察；原 `PET_FEED/PET_SWITCH StoredOperationOutcome` 只保存决定时
PetStateView、order_id 和 guard_version，不得在同步完成后改写。

GuardSnapshot 自然到期只由 Theft Turn 的 `trusted_now_ms < guard_until_ms` 惰性判断，不调用本
Participant、不写 EXPIRED，也不产生公共 Delta。

### 7.15 任务命令与好友访问确认接缝

任务没有独立 Participant Service。`GET_TASK_LIST`、`CLAIM_TASK` 与
`CONFIRM_FRIEND_VISIT` 都通过第 28 号 CommandCatalog 路由到 PLAYER Holder，最终进入
`PlayerActor(player_id)` 邮箱：

```text
GET_TASK_LIST
→ EnsureDailyPeriod
→ 读取PlayerActor.TaskDomain权威状态
→ 返回TaskListView

CLAIM_TASK
→ operation_id来自ExecuteCommandRequest外层
→ Typed Payload只有task_instance_id
→ PlayerActor原子提交TaskState、资产、ClaimReceipt和StoredOperationOutcome

CONFIRM_FRIEND_VISIT
→ H5收到SUBSCRIPTION_READY后自动发送
→ Gate从当前ActiveSubscription生成FarmAdmissionContext
→ PlayerActor原子提交每日访问任务与StoredOperationOutcome
```

FirstHop 对 `CONFIRM_FRIEND_VISIT` 必须在进入 Mailbox 前验证：

```text
typed.target_farm_id == farm_context.admitted_farm_id
typed.target_farm_id != session.player_id
farm_context.admitted_subscription_id非空
farm_context.authorization_pair_id非空
farm_context.authorization_relation_id非空
farm_context.authorization_relation_epoch存在
farm_context.admitted_subscription_activated_at_ms有效
```

上述 relation fence 证明这次 ActiveSubscription 来源于一次合法好友 Join。它不是 Care/Theft 的
ActionPermit，不能被复用来执行经营动作；但足以证明“玩家已进入过已授权好友农场”这一任务事实。
命令进入 PlayerActor 后不再根据 H5 字段或当前墙钟重建访问时间：

```text
visit_occurred_at_ms
= farm_context.admitted_subscription_activated_at_ms

visit_witness_id
= StableOpaqueID(
     admitted_subscription_id,
     authorization_relation_id,
     authorization_relation_epoch,
     visit_occurred_at_ms)
```

`visit_witness_id` 只用于记录首个合法 witness 与诊断，不是公网幂等键。公网幂等仍是
`{session.player_id, CONFIRM_FRIEND_VISIT, operation_id}`；Canonical Payload Hash 只覆盖
`target_farm_id`。同一 witness 重放返回原结果；同日另一个合法 witness 在任务已经满足后返回
`NO_CHANGE/TASK_ALREADY_SATISFIED`，不能因 proof 不同误报 `IDEMPOTENCY_CONFLICT`。

命令在旧周期发生但延迟到新周期才进入 PlayerActor 时，按第 34 号严格周期语义幂等返回
`NO_CHANGE/TASK_EVENT_EXPIRED`，不能污染新一天。好友 `CareRewardOrder.APPLY_CARE_REWARD` 和
`TheftWorkflow.SETTLE_THEFT_VISITOR` 已携带更强的合法互动证明；当访问任务尚未满足时，它们可在
同一次 Player Authority Commit 中同时满足访问与互动任务。

P0 不定义 `APPLY_FRIEND_VISIT_TASK` Participant、不创建 `FriendVisitTaskProgressWork`，也不要求
farm-worker 为访问任务扫描 DueIndex。H5 必须把确认命令写入 Operation Journal 后再发送，并在
Mailbox 接纳后按普通 OperationResult 恢复；若在接纳前 Subscription 已失效，则先 Fresh Join 原目标
再使用原 operation_id 重试，或在下一次合法好友访问中以新意图重新确认。

每日登录同样不增加 Gate→Player Task RPC。PlayerBootstrap、任务命令或其他受信 PlayerActor Turn
进入时统一执行 `EnsureDailyPeriod`，并在本周期第一次提交中把 daily login 从 false 改为 true。

### 7.16 邮件领取可信材料与 Worker 接缝

`CLAIM_MAIL_ATTACHMENT` 虽按 PLAYER Actor Affinity 到达 Holder，但 FirstHop 是
`PlayerMailClaimIngress`，不是 Gate 直接调用 PlayerActor：

```text
Gate ExecuteCommand
→ PLAYER Holder / PlayerMailClaimIngress
→ 重新解码ClaimMailAttachmentRequest(mail_id)
→ 生成operation_payload_hash(mail_id)
→ MailEntryRepository Primary强读
→ 构造TrustedMailClaimMaterial
→ 本地提交PlayerActor Mailbox
```

逻辑材料结构为：

```text
TrustedMailClaimMaterial {
  schema_version
  mail_id
  mail_version
  recipient_player_id
  source_type
  source_id
  delivery_id
  reward_bundle_snapshot
  claim_material_hash
  visible_at_ms
  claim_end_at_ms
  issued_at_ms
  material_expire_at_ms
  issuer
  audience = PLAYER_AUTHORITY
}
```

该结构不属于公网 Proto，也不能被装入 H5 Operation Journal。P0 中 Ingress 与 Actor 同 Deployment，
由 `PlayerMailPort` 生成强类型不可变对象并本地入 Mailbox，不要求签名。若未来拆成跨服务 RPC，调用方
必须具备允许的 mTLS 服务身份，并用签名材料或等价可信信封保护 issuer/audience/recipient/hash/TTL；
不能把普通 `TrustedCommandContext` 当作附件内容证明。

PlayerActor 在 Turn 开始时必须校验：

```text
material.audience == PLAYER_AUTHORITY
material.recipient_player_id == actor.player_id
material.material_expire_at_ms >= trusted_now_ms
material.claim_material_hash == CanonicalMailClaimMaterialHash(material)
operation_payload_hash == CanonicalHash(request.mail_id)
material.mail_id == request.mail_id
```

Coordinator 的 `claim_end_at_ms` 检查只做快速拒绝。最终业务时间由 Actor Turn 捕获的一次
`trusted_now_ms` 决定，并作为 `claim_decision_at_ms` 进入 Authority Commit。进入 CommitAttempt 后，
Unknown Resolver 依据原 Attempt/Receipt 解析，不重新读取 MailEntry 或用新时间改判。

PlayerActor 必须同时检查：

```text
OperationOutcome[EffectiveIdempotencyKey]
MailClaimState[player_id, mail_id]
```

前者防同 operation 重放/换 mail，后者防换 operation 重领同一邮件。另一 operation 已经领取时，当前
operation 的 `NO_CHANGE/MAIL_ALREADY_CLAIMED` 也要保存为稳定 StoredOperationOutcome。

邮件后台调用分成两类，不能伪造在线 Session：

| 内部动作 | 发起者 | 调用上下文 | 目标 |
|---|---|---|---|
| `ResumeMailDeliveryOrder` | DirectDispatcher / farm-worker | `CoordinatorWorkContext` / `InternalCallerContext` | PlayerMailDomain.InsertMailIfAbsent |
| `ApplyMailClaimProjection` | DirectDispatcher / farm-worker | 同上，携带 Work Lease/Invocation | PlayerMailDomain.ApplyMailClaimProjection |

`MailDeliveryOrder` 必须携带第 35 号冻结的完整不可变内容快照；Worker 不能按 delivery_id 重读当前模板。
`MailClaimProjectionOrder` 只更新 claimed/红点查询状态，失败不回滚资产。两者的具体 Redis Key/Lua
服从第 35A 号，内部调用的 Deadline、Lease、ParticipantInvocationID 和响应外壳继续复用本文件第 6/7 章。

---

## 8. Join、QueueAccepted 与精确 Subscription 清理内部合同

### 8.1 DownlinkTargetFence

```protobuf
message DownlinkTargetFence {
  uint64 target_player_id = 1;
  string gateway_id = 2;
  string gateway_boot_id = 3;
  string connection_id = 4;
  uint64 connection_generation = 5;
  uint64 active_session_epoch = 6;
  uint64 session_route_revision = 7;
}
```

它描述本次 Join Snapshot 或清理请求绑定的真实连接目标。消息字段只用于完整 Fence；真实调用方
身份仍来自内部 RPC 连接的 mTLS/Workload Identity。

### 8.1A 好友 Join 的 AUTHORIZING 与 VisitPermit

好友 Join 不能采用“先签 Permit、再创建 Hub Subscription”的顺序，否则删除清理可能先执行完，旧
Permit 随后才创建新 Subscription。内部顺序固定为：

```text
Gate已有PendingJoin(join_operation_id, join_attempt_id, full fence)
→ Hub.PrepareFriendJoin
→ 创建AUTHORIZING临时记录并预留同场名额
→ PairAuthority.AuthorizePreparedVisit
→ Hub.AttachVisitAuthorization
→ AUTHORIZING转为正常JOINING
→ FirstHop返回ACCEPTED
```

逻辑 IDL：

```text
PrepareFriendJoinRequest {
  viewer_player_id              // 来自TrustedCommandContext
  target_farm_id
  join_operation_id
  join_attempt_id
  DownlinkTargetFence target
}

PreparedFriendJoinRef {
  prepared_join_id
  canonical_pair_id
  target_farm_id
  join_operation_id
  join_attempt_id
  DownlinkTargetFence target
}

AuthorizePreparedVisitRequest {
  TrustedCommandContext trusted_command
  PreparedFriendJoinRef prepared
}

VisitPermitSnapshot {
  permit_id
  viewer_player_id
  owner_player_id
  target_farm_id
  relation_id
  friend_relation_epoch
  join_operation_id
  join_attempt_id
  DownlinkTargetFence target
  accepted_ttl_ms
  issued_at_authority_ms         // 仅诊断
  permit_payload_hash
}

AttachVisitAuthorizationRequest {
  PreparedFriendJoinRef prepared
  VisitPermitSnapshot permit
}
```

Hub 收到 Permit 时使用本机 monotonic clock 建立本地 Deadline，并校验 prepared ref、attempt、operation、
target farm 和完整连接 Fence 全部相等。AUTHORIZING 在 Permit 绑定前保存 pair_id、attempt 和完整 Fence，
relation epoch 为 `UNBOUND`；绑定后必须保存 `authorization_relation_id + authorization_relation_epoch`。

删除先提交时 Pair Authority 不签 Permit；授权先发生时，`FriendTerminationOrder` 可以撤销已经存在的
AUTHORIZING 或后续状态。撤销请求必须携带 `terminated_relation_id + captured_active_epoch`：已绑定
epoch 的记录只按精确匹配删除；尚未绑定的同 pair AUTHORIZING 允许 Fail Closed 取消。授权失败、
Deadline 到期或 Attach Fence 不匹配都释放临时名额，不生成 Snapshot。

### 8.2 ACCEPTED QueueAccepted 与 ArmSnapshot

FirstHop 返回 Join ACCEPTED 元信息后，Gate 必须先：

```text
绑定PendingJoin.subscription_id
→ 把CommandResult(ACCEPTED)放入当前ConnectionWriter FIFO
→ 再调用ArmSnapshot
```

```protobuf
message ArmSnapshotRequest {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  string join_operation_id = 4;
  DownlinkTargetFence target = 5;
}

enum ArmSnapshotStatus {
  ARM_SNAPSHOT_UNSPECIFIED = 0;
  ARM_SNAPSHOT_ARMED = 1;
  ARM_SNAPSHOT_ALREADY_ARMED = 2;
  ARM_SNAPSHOT_NOT_FOUND = 3;
  ARM_SNAPSHOT_STALE_FENCE = 4;
  ARM_SNAPSHOT_UNAVAILABLE = 5;
}

message ArmSnapshotResponse {
  ArmSnapshotStatus status = 1;
  optional uint64 snapshot_version = 2;
}
```

Hub 只在请求完整匹配当前 JOINING 时启动 Arm；`snapshot_armed=true` 必须与唯一 Snapshot 基线
安装共同成立，不能先翻转布尔值、稍后再选择版本。重复 Arm 幂等返回原结果或加入同一 in-flight
Arm；旧 attempt、旧 subscription 或旧连接 Fence 只能忽略。ACCEPTED 入队失败时 Gate 撤销
PendingJoin 并精确 Cleanup，Hub 不得自行抢跑 Snapshot。

`ARMED` 与 `ALREADY_ARMED` 都必须返回同一个 `snapshot_version`；后者不能触发第二次 Snapshot
生成或第二次名额占用。`NOT_FOUND/STALE_FENCE` 是本次 attempt 的终止信号，Gate 应精确清理并
要求 Fresh Join；`UNAVAILABLE` 只能在本地 Join Deadline 内以同一 Fence 有界重试。

未 Arm 的 JOINING 只保留名额、Subscription 与 Fence，不预先读取 Snapshot，也不预先捕获
FarmChange。首次有效 Arm 必须在与 FarmChange 发布同序的 Farm Runtime Lane 中原子建立唯一
`Snapshot(V) + capture_from(V+1)` 基线；相同 Fence 的重复 Arm 复用同一基线或同一 in-flight
结果，不能重新选择另一个 V。该要求可以通过有界 `PrepareJoinBaseline` 读/系统 turn 加同 Holder
有序交接流实现，但 Hub 不得等待 Gate 网络 RPC，也不得让普通 Join/Leave/续租进入 FarmActor
业务邮箱。

这是必要的分布式顺序屏障：ExecuteCommandResponse 和 Hub→Gate Downlink 是不同 RPC，代码中
“先 return ACCEPTED、再发送 Snapshot”不能保证 Gate 的实际处理顺序。ArmSnapshot 把顺序从
偶然调度变成可测试协议。

### 8.3 Snapshot Downlink 与 QueueAccepted

Arm 成功后，FarmSubscriptionHub 产生：

```protobuf
message FarmSnapshotDownlinkIntent {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  uint64 snapshot_version = 4;
  DownlinkTargetFence target = 5;
  bytes farm_snapshot_payload = 6;
}
```

Gate DownlinkService 必须先验证：

```text
当前连接存在
Session Fence匹配
存在同join_attempt_id/target_farm_id的PendingJoin
subscription_id与ACCEPTED绑定一致
PendingJoin.stage至少为SNAPSHOT_ARMED
```

只有 Snapshot 成功进入当前 ConnectionWriter FIFO 后，Gate 才发送幂等回调；此时仍不得把
PendingJoin 提升为 ActiveSubscription：

```protobuf
message SnapshotQueueAcceptedRequest {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  uint64 snapshot_version = 4;
  DownlinkTargetFence target = 5;
}
```

Hub 只有在完整字段匹配当前 JOINING Subscription 时，才能执行 `JOINING → FLUSHING`。重复回调
返回原结果；旧 attempt、旧 subscription、旧 connection fence 只能幂等忽略。

QueueAccepted 回调属于连接运行态控制，不要求持久化；但 Gate 在连接仍存活时必须有界重试。
超过 `join_queue_ack_deadline` 仍无法确认时，应使该 Subscription 进入 REJOIN_REQUIRED/清理，
不能让 JoiningBuffer 无界增长。

### 8.4 Catch-up Batch QueueAccepted

Hub 在 FLUSHING 中分批发送：

```protobuf
message FarmCatchupBatchDownlinkIntent {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  uint64 from_version = 4;
  uint64 through_version = 5;
  DownlinkTargetFence target = 6;
  bytes farm_delta_batch_payload = 7;
}

message CatchupBatchQueueAcceptedRequest {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  uint64 from_version = 4;
  uint64 through_version = 5;
  DownlinkTargetFence target = 6;
}
```

Gate 只接受与当前 PendingJoin 和预期连续版本匹配的 Batch。Hub 在同一 Lane 按
`through_version` 移除已确认前缀；重复 ACK 幂等，跳跃 ACK、旧 ACK 或 Fence 不匹配不得推进
Buffer。Batch 任一无法入队时，不能跳过版本继续 Ready，应进入 RESYNC_REQUIRED/清理。

### 8.5 ActivateSubscription 与 SubscriptionReady

Snapshot QueueAccepted 回到 Hub 同一 Lane 时，Hub 必须冻结一个有限目标：

```text
state == FLUSHING
+ snapshot_version == V
→ ready_cut_version=N
→ N取当前已经连续捕获的最高版本；没有增量时N=V
→ version<=N继续进入本次Catch-up
→ 此后version>N只进入post_ready_buffer
```

Hub 只等待这个有限前缀：

```text
已连续QueueAccepted through=N
+ version<=N不存在空洞
+ version<=N不存在flush_inflight
+ state仍为FLUSHING
→ ready_through_version=N
→ 转为ACTIVATING
→ 调用ActivateSubscription
```

不得要求整个变化流或 post-ready buffer 为空。否则持续发生 Care/Theft/Harvest 的热点农场可能
不断延长 JoiningBuffer，使 Join 永远无法 Ready。固定 N 后，本次 Join 的完成目标不会再被
N+1 之后的新变化推远。

调用结构：

```protobuf
message ActivateSubscriptionRequest {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  uint64 ready_through_version = 4;
  DownlinkTargetFence target = 5;
}

message ActivateSubscriptionResponse {
  DeliveryStatus status = 1; // QUEUE_ACCEPTED / STALE_FENCE / QUEUE_FULL / UNAVAILABLE
  uint64 ready_through_version = 2;
}
```

Gate 必须在同一 Session Lane 中：

```text
复核PendingJoin和完整Fence
→ 把SUBSCRIPTION_READY(join_attempt_id,N)放入ConnectionWriter FIFO
→ 原子PendingJoin→ActiveSubscription
→ 返回QUEUE_ACCEPTED
```

Hub 收到匹配的 QUEUE_ACCEPTED 后执行 `ACTIVATING → IN_FARM`。从冻结 `ready_cut_version=N`
开始到 Ready 入队期间，N+1 之后变化必须保留在 post-ready buffer 或等价有序结构中，直到
Ready 已入队后再按版本发送。若 RPC 响应
超时，Hub 使用相同 attempt/subscription/N 幂等重试；Gate 不得重复创建 ActiveSubscription 或
重复占用名额。

### 8.5.1 好友 Subscription Ready 后的 Navigation visited 接缝

Gate 完成 `PendingJoin → ActiveSubscription` 后，如果目标不是玩家自己的农场，应在 Session Lane 外
最佳努力调用 player-svr 的运行时接口：

```protobuf
message MarkFriendOpportunityVisitedRequest {
  TrustedCommandContext trusted_context = 1;
}

enum NavigationVisitApplyStatus {
  NAVIGATION_VISIT_APPLY_STATUS_UNSPECIFIED = 0;
  NAVIGATION_VISIT_APPLIED = 1;
  NAVIGATION_VISIT_NO_CHANGE = 2;
  NAVIGATION_VISIT_STALE_FENCE = 3;
  NAVIGATION_VISIT_UNAVAILABLE = 4;
}

message MarkFriendOpportunityVisitedResponse {
  NavigationVisitApplyStatus status = 1;
}

service FriendOpportunityNavigationService {
  rpc MarkFriendOpportunityVisited(MarkFriendOpportunityVisitedRequest)
      returns (MarkFriendOpportunityVisitedResponse);
}
```

`trusted_context` 必须由刚刚激活的 ActiveSubscription 生成，至少使服务端能够复核：

```text
session.player_id
active_session_epoch
gateway/connection generation
farm_context.admitted_farm_id
farm_context.admitted_subscription_id
farm_context.authorization_relation_id/epoch
```

H5 不提交 `viewer_player_id`、`visited_farm_id`、Navigation cursor 或 visited 集合。player-svr 只在
`admitted_farm_id != session.player_id` 且 Subscription Fence 仍匹配时，把该农场标记为当前短期
Navigation 轮次已访问；重复请求返回 `NO_CHANGE`。

该接口只修改 Runtime Redis 中允许丢失的 `FriendOpportunityNavigation`：

- 不创建 Workflow、Order、ParticipantReceipt 或长期 OperationResult；
- 不阻塞 `SUBSCRIPTION_READY`，失败不得回滚已经成功的 Join；
- 失败最多导致返回自己农场后重复推荐同一好友，下一次精确检查或 TTL 重建即可修复；
- 它与公网 `CONFIRM_FRIEND_VISIT` 完全分离。后者用于可靠推进每日访问任务，仍由 H5 在收到 Ready 后
  使用独立 operation_id 发起。

### 8.5.2 实时流失效与 REJOIN_REQUIRED 下行接缝

普通 `CommittedChangeQueue` 失败后的失效来源以第 27 号 `FarmRealtimeState` 为权威。该状态不能
通过原失败队列传播；Hub 通过独立保留 Control Lane、周期观察或下一次运行时入口读取：

```text
FarmRealtimeInvalidation {
  target_farm_id
  delivery_generation
  invalid_from_version
  reason
}
```

Hub 观察到非零 `invalid_from_version` 后，将当前 Joining/Active Subscription 分别处理：

```text
JOINING / FLUSHING / ACTIVATING
→ 终止本次Join
→ 不再发送Ready

IN_FARM
→ 内部状态RESYNC_REQUIRED
→ 为每个完整Subscription Fence生成RejoinRequiredDownlinkIntent
```

```protobuf
message RejoinRequiredDownlinkIntent {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  DownlinkTargetFence target = 3;
  uint64 delivery_generation = 4;
  uint64 invalid_from_version = 5;
  RejoinReason reason = 6;
}
```

这里的 `RejoinReason` 直接复用第 28 号公网协议定义的稳定枚举，29 号不再定义第二份数值表。
`FarmRealtimeInvalidation.reason` 若使用更细的内部原因，必须在 Downlink Adapter 中显式映射到该
公开枚举；未知内部原因映射为 `REJOIN_REASON_UNSPECIFIED`，但客户端动作仍是 Fresh Join。

Gate 只把它映射为第 28 号公网 `REJOIN_REQUIRED`，不存在公网 `RESYNC_REQUIRED` PushType。Gate
必须在同一 Session Lane 复核 ActiveSubscription，先把终止 Push 放入当前 ConnectionWriter，
或确认连接不可达，再释放只读终止 Fence。若队列连终止控制帧都无法接受，Gate 关闭该连接，
不能继续把该 Subscription 当成可写。

一次 Downlink 失败不能清除 `FarmRealtimeState.invalid_from_version`。Hub 只有在所有旧订阅已终止，
且新 Join 从最新 Snapshot 安装更高 `delivery_generation` 后，才按代次条件清除旧失效状态。
失效状态不拒绝新 Join；它只禁止新 Join 复用旧 generation 或旧捕获游标。

### 8.6 禁止 Downlink 隐式绑定

DownlinkService 不得采用：

```text
如果当前subscription为空
→ 使用第一条到达的Push.subscription_id自动绑定
```

这种行为允许旧 Snapshot/旧 FarmChange 抢占新 PendingJoin。所有 FarmSubscription Push 必须
匹配显式 PendingJoin 或 ActiveSubscription；不匹配返回逐 item `STALE_SUBSCRIPTION`，不得改变
Gate 当前状态。

### 8.7 精确 CleanupSubscription 与 CancelJoinAttempt

正常 Disconnect、SwitchFarm、KICK 和 Join失败使用：

```protobuf
message CleanupSubscriptionRequest {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  DownlinkTargetFence target = 4;
  CleanupReason reason = 5;
}
```

调用方按 `FARM:target_farm_id` 使用 ActorRouteClient 路由到当前 Holder；FarmSubscriptionHub 只在
完整 Fence 匹配时条件删除。禁止把 Disconnect 广播给所有 FarmActor，也禁止在 farm-svr 中扫描
全部活跃 Actor/Hub 查找连接。

若 FirstHop 已经创建未 Arm JOINING，但 ACCEPTED 响应在到达 Gate 前丢失或超时，Gate 尚不知道
`subscription_id`，不能构造上述请求。Gate 必须先在同一 Session Lane 把对应 PendingJoin 原子
转为 `CANCELLING`；从这一刻起，该 attempt 的迟到 ACCEPTED、Arm 和 Ready 只能用于清理或忽略，
不能再推进 Join。随后使用：

```protobuf
enum CancelJoinReason {
  CANCEL_JOIN_REASON_UNSPECIFIED = 0;
  CANCEL_JOIN_REASON_ACCEPTED_UNKNOWN = 1;
  CANCEL_JOIN_REASON_PENDING_TIMEOUT = 2;
  CANCEL_JOIN_REASON_CONNECTION_CLOSED = 3;
}

message CancelJoinAttemptRequest {
  uint64 target_farm_id = 1;
  string join_attempt_id = 2;
  string join_operation_id = 3;
  optional string subscription_id = 4;
  DownlinkTargetFence target = 5;
  CancelJoinReason reason = 6;
}

enum CancelJoinAttemptStatus {
  CANCEL_JOIN_ATTEMPT_UNSPECIFIED = 0;
  CANCEL_JOIN_ATTEMPT_CANCELLED = 1;
  CANCEL_JOIN_ATTEMPT_STALE_FENCE = 2;
  CANCEL_JOIN_ATTEMPT_ALREADY_PROGRESSED = 3;
  CANCEL_JOIN_ATTEMPT_UNAVAILABLE = 4;
}

message CancelJoinAttemptResponse {
  CancelJoinAttemptStatus status = 1;
  optional string subscription_id = 2;
}
```

Hub 使用 `target_farm_id + join_attempt_id + join_operation_id + 完整连接Fence`，通过
`by_join_attempt` 或等价 O(1) 索引精确定位。Cancel 与 CreateOrReuseJoining 必须进入同一 Hub Lane。
规则：

```text
仍是JOINING且snapshot_armed=false
→ 先安装/确认CancelTombstone
→ CLOSED
→ 清空运行时缓冲
→ 释放200人名额
→ 返回CANCELLED

当前尚未找到JOINING，但请求Fence有效
→ 安装CancelTombstone
→ 返回CANCELLED
→ 后续迟到CreateOrReuseJoining命中tombstone并拒绝创建

已经Arm、FLUSHING、ACTIVATING或IN_FARM
→ 返回ALREADY_PROGRESSED及可用subscription_id
→ 不允许仅凭attempt广泛删除
→ Gate仅在本地仍匹配同attempt/同Fence的CANCELLING时改用CleanupSubscription
→ 本地已经是新attempt或不再CANCELLING时忽略迟到响应

旧attempt / 旧operation / 旧连接Fence
→ STALE_FENCE
→ 不得影响后来建立的新Subscription
```

重复取消必须由有界 attempt Tombstone 或等价幂等记录返回原结果。Tombstone TTL 至少覆盖 Join
Deadline、内部重试和最大 RPC 迟到窗口。Hub Lane 串行保证：Join 先到则 Cancel 关闭它；Cancel
先到则 Tombstone 阻止迟到 Join。`CancelJoinAttempt` 只解决
“Gate 不知道 subscription_id”的未 Arm 不确定窗口，不能代替正常 Leave/Cleanup。
Tombstone Store 必须有按 Farm 的 item/byte/TTL 上限和指标；若连最小 Tombstone 都无法保留，Hub
必须对该 Farm 的新 Join Admission 暂时 Fail Closed，直到容量恢复；不得通过提前逐出未过期
tombstone 换取表面可用性。

Gate 突然崩溃无法发送 Cleanup 时，由 Subscription Lease/TTL、Gate Boot 失效通知和客户端新
Session重新Join最终清理；这些兜底不能成为正常 Disconnect 使用全扫描的理由。

批量 Subscription 续租同样携带完整 `DownlinkTargetFence`，按 `FARM:farm_id` 路由到目标 Hub；
只有完全匹配的 Pending/Active Subscription 可以延长 Lease。续租不能通过扫描 Actor 或逐浏览器
心跳实现。

### 8.8 BatchRenewSubscriptions

Gate 按当前 Assignment 将 Subscription 按目标 Holder 分组，使用一个批次续租多座 Farm 上的
Subscription：

```protobuf
message SubscriptionLeaseEntry {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  DownlinkTargetFence target = 4;
  uint32 requested_ttl_ms = 5;
}

message BatchRenewSubscriptionsRequest {
  string source_gateway_id = 1;
  string source_gateway_boot_id = 2;
  repeated SubscriptionLeaseEntry entries = 3;
}

enum SubscriptionLeaseStatus {
  SUBSCRIPTION_LEASE_UNSPECIFIED = 0;
  SUBSCRIPTION_LEASE_RENEWED = 1;
  SUBSCRIPTION_LEASE_NOT_FOUND = 2;
  SUBSCRIPTION_LEASE_STALE_FENCE = 3;
  SUBSCRIPTION_LEASE_CLOSED = 4;
  SUBSCRIPTION_LEASE_ROUTE_STALE = 5;
  SUBSCRIPTION_LEASE_UNAVAILABLE = 6;
}

message SubscriptionLeaseResult {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
  SubscriptionLeaseStatus status = 4;
  uint32 accepted_ttl_ms = 5;
  optional int64 server_time_ms = 6; // 仅诊断，不参与Gate正确性比较
}

message BatchRenewSubscriptionsResponse {
  repeated SubscriptionLeaseResult results = 1;
}
```

Holder 收到批次后按 `target_farm_id` 投递对应 FarmSubscriptionHub Lane。每个 Entry 独立返回结果，
禁止因为其中一个 Farm 迁移或 Fence 过期而把整批误报成功。Hub 只允许：

```text
Subscription已存在
+ state属于JOINING/FLUSHING/ACTIVATING/IN_FARM
+ subscription_id/join_attempt_id/完整DownlinkTargetFence全部匹配
→ accepted_ttl_ms = min(requested_ttl_ms, configured_max_extension_ms)
→ lease_expire_at_ms = trusted_now_ms + accepted_ttl_ms
```

Renew 不得创建 Subscription、替换目标玩家、改变 attempt、把 CLOSED 记录改回 Active，或延长
一个已经由 Hub Lane 判定过期并释放名额的 Subscription。mTLS/Workload Identity 必须证明调用方
是合法 Gate 服务；`source_gateway_id/boot_id` 还必须和各 Entry 的 target Fence 一致。

Gate 只有在响应仍匹配本地同一 PendingJoin/ActiveSubscription 时才更新本地 Lease Deadline。
每个批次发送前记录 `renew_started_monotonic`，收到结果后使用：

```text
local_deadline
= renew_started_monotonic
  + accepted_ttl_ms
  - configured_safety_margin
```

不得把 Hub 的绝对 Unix 时间与 Gate 墙上时间直接比较，也不得从响应接收时刻重新获得完整 TTL；
从请求开始的单调时间计算会保守计入请求/响应耗时。`server_time_ms` 只用于时钟漂移和链路诊断。
如果续租失败直到该 Deadline：

```text
本地Subscription → INVALID
→ 停止为其生成FarmAdmissionContext
→ 可达时下发REJOIN_REQUIRED
→ Fresh Join
```

恢复网络后不能用迟到的旧 Renew Response 复活 INVALID Subscription。批量大小、周期、Lease 时长、
Safety Margin 和每 Holder 并发预算由压测配置，不属于 Proto 常量。

逻辑硬合同是“每条 Subscription 都存在完整 Fence 的存活证明并可最终过期”。P0 物理基线固定为
当前 BatchRenew，避免实现两套主链。若容量测试证明 `active_subscription_count / renew_interval`
产生的背景 Entry 流量过高，可以演进为 `GateBoot Lease + Hub反向索引 + 更低频Subscription TTL`；
GateBoot 失效用于批量清理，低频 TTL 仍负责单条 Cleanup 丢失。演进方案必须保持 Fail-Closed、
完整 Fence 和 CLOSED 不可复活，不允许仅因 Gate 进程还活着就永久保留已断开的连接订阅。

### 8.9 Join 回调与 Execution-Accept

```text
JOIN_FARM Execution-Accept
→ FirstHop已创建/幂等复用未Arm的JOINING
→ 可以返回ACCEPTED

ACCEPTED QueueAccepted
→ Gate确认PendingJoin已绑定
→ 调用ArmSnapshot

ArmSnapshot成功
→ 在同Farm有序Lane建立Snapshot(V)+V+1捕获游标

Snapshot QueueAccepted
→ Hub开始FLUSHING并冻结有限ready_cut_version=N

Catch-up QueueAccepted through=N
→ 截至N连续且无<=N的空洞/inflight后Hub进入ACTIVATING

N+1之后变化
→ 暂存post_ready_buffer，不延长本次Ready目标

SubscriptionReady(N) QueueAccepted
→ Gate本地Subscription可用于后续Admission
→ Hub进入IN_FARM
```

Join 的 Execution-Accept 不等于客户端已经有 Snapshot，也不允许把 Snapshot塞入 CommandResult。
Snapshot 或 Catch-up 下行失败不会回滚已创建的 JOINING，但必须最终清理或要求 Fresh Join；
不能在没有 SubscriptionReady 的情况下开放经营命令。

---

## 9. 内部响应与 RouteFeedback

### 9.1 ExecuteCommandResponse

```protobuf
message ExecuteCommandResponse {
  oneof outcome {
    BusinessCommandOutcome business = 1;
    RouteRejection route_rejection = 2;
  }
}

message BusinessCommandOutcome {
  CommandStatus status = 1;
  ErrorDetail error = 2;
  RetryDirective retry_directive = 3;
  uint32 retry_after_ms = 4;

  oneof outcome_context {
    JoinAcceptedMetadata join_accepted = 10;
  }

  bytes result_payload = 15;
}

message JoinAcceptedMetadata {
  uint64 target_farm_id = 1;
  string subscription_id = 2;
  string join_attempt_id = 3;
}
```

内部响应不直接携带客户端可伪造的操作者身份。Gate 使用原请求中的
`request_id / operation_id / command_type` 和该内部结果组装第 28 号
`CommandResultEnvelope`。

`join_accepted` 只允许出现在 `JOIN_FARM + ACCEPTED`：

- `target_farm_id` 必须等于原 `JoinFarmRequest` 和受信 `JoinAdmissionContext` 的目标；
- `join_attempt_id` 必须等于 Gate 为本次 PendingJoin 生成的 attempt；
- `subscription_id` 必须来自 FirstHop/Hub 已创建或幂等复用的未 Arm JOINING；
- Gate 使用三者构造第 28 号公开 `JoinResultContext`，公开 `result_payload` 仍为空；
- 非 Join 结果不得伪造该 Context，普通业务结果继续使用 `result_payload`。

没有这组内部元数据，Gate 无法在 ACCEPTED 入队前绑定 PendingJoin，也无法构造随后
`ArmSnapshotRequest` 的完整 Fence；因此它不是展示字段，而是 Join 顺序协议的必要接缝。

### 9.2 RouteFeedback

`RouteRejection` 逻辑分类：

```text
REDIRECT
SHARD_DRAINING
STALE_ASSIGNMENT
STALE_WRITER_EPOCH
ROUTE_UNAVAILABLE
```

路由反馈与业务错误分离：

```text
作物未成熟
→ BUSINESS_NOT_MATURE
→ 不Redirect

Shard已经迁移
→ REDIRECT / STALE_ASSIGNMENT
→ ActorRouteClient更新本地Snapshot
→ 使用原operation_id有界重试
```

Redirect 返回的新 Holder 和 revision 只能作为路由纠正信息；接收方仍需从 AssignmentProvider
验证，不能无限相信任意 Pod 返回的地址。

路由成功不返回 `RouteRejection.OK`；响应必须进入 `business` 分支。这样不会产生
`outcome=route_rejection, reason=OK` 的无意义组合。

### 9.3 RouteFeedback 互斥与 FirstHop 执行边界

```text
route_rejection存在
→ 请求尚未跨过FirstHop Execution-Accept边界
→ 未创建Workflow
→ 未进入Actor Mailbox
→ 未产生ParticipantReceipt或业务事实
→ 不得同时返回SUCCEEDED/PROCESSING/REJECTED/NO_CHANGE
```

Execution-Accept 边界：

```text
WORKFLOW_COORDINATOR
→ CreateIfAbsent Workflow成功

ACTOR_DIRECT
→ Route/Lease复核完成并由目标Mailbox接受带幂等身份的命令

DOMAIN_ROLE / JOIN
→ 运行时Handler创建或幂等复用Joining等操作

SERVICE_LB / 纯查询
→ 无状态Handler取得有界容量并正式接管
```

因此 `Execution-Accept` 必须由第 28 号 `ExecutionAcceptPolicy` 明确声明，不能只靠
`ExecutionKind` 的名称推断。Workflow 首跳成功不等于所有 Participant 都已完成；它只表示
Workflow 已经可靠接管，后续步骤由 Coordinator/Worker 继续推进。

跨过该边界后，即使随后发现迁移、RPC超时或响应丢失，也不能再让 Gate Redirect 并重跑完整
业务命令。FirstHop 应返回 `PROCESSING` 或最终业务结果，由 Coordinator/Worker/幂等 Receipt
完成恢复。

### 9.4 Deadline、取消和同步等待

内部同步等待以 gRPC Context Deadline 为执行依据：

```text
Gate按CommandSpec生成总预算
→ gRPC Context携带Deadline
→ remaining_budget_ms用于FirstHop/Coordinator分配预算
→ deadline_unix_ms只用于日志和诊断
```

接收端有效预算必须取最小值：

```text
effective_budget =
min(
    grpc_context_remaining,
    request.remaining_budget_ms,
    CommandSpec当前步骤最大预算
)
```

下游只能继续缩短预算，不能延长上游 Deadline；排队、解码、路由和准入已经消耗的时间必须
计入。`deadline_unix_ms` 不参与授权，也不能用于延长执行。

Gate 使用独立请求任务有界等待 FirstHop；WebSocket ReadLoop、WriteLoop、其他连接和 Actor
Mailbox 不因此阻塞。

取消语义：

```text
gRPC Context取消
→ 表示这次内部同步调用不再需要等待结果
→ 不等于玩家发起了业务Cancel命令

跨过Execution-Accept之前
→ FirstHop可以停止尚未接管的解码、路由或准入工作
→ 不创建Workflow、不进入Actor Mailbox、不产生业务事实

跨过Execution-Accept之后
→ FirstHop/Coordinator不能回滚或取消已接管的业务
→ Gate断线或RPC取消只结束当前同步等待
→ Workflow/Order或已入Mailbox的命令继续推进
→ 通过OperationResult/Receipt和Worker恢复最终结果
```

这里的“FirstHop 收到取消”可能来自客户端断线、Gate Deadline 到期或上游主动取消 Context，
不要求 FirstHop 能区分三者。它只需要依据本地的 `Execution-Accept` 事实做同一判断：接受前
可以放弃，接受后只能结束等待。接受前如果响应能够返回，可使用 `REJECTED + RETRY_SAME_OPERATION`；
如果连响应也无法送回，客户端只能按 `TIMEOUT_UNKNOWN` 恢复，不得凭“没收到响应”断言未执行。

P0 没有面向玩家的通用 Cancel 命令。未来若要支持撤销，必须新增带业务语义的 CancelCommand，
由对应 Workflow 状态机决定是否允许补偿，不能复用 gRPC Context 取消。

具体总预算、Participant 子预算和完整 `TIMEOUT_UNKNOWN` 错误码矩阵留到第 30 号合同；
但以下内部边界已经冻结：

```text
Execution-Accept前且FirstHop可以证明未接管
→ RouteRejection或REJECTED + RETRY_SAME_OPERATION

Execution-Accept后且已有Workflow/Order/Receipt等可靠锚点
→ PROCESSING / POLL_OPERATION_RESULT

Actor Mailbox已接受或Persist结果未知、尚无最终StoredOperationOutcome
→ 不返回RouteRejection
→ 使用原operation_id / participant_invocation_id解析Receipt与RecordVersion
→ 不能让Gate自动重跑完整命令
```

### 9.5 Redirect 有界重试

一次公网命令最多允许 Gate 执行一次内部 Redirect 重试：

```text
attempt=1
Gate按本地Snapshot调用pod-A
→ pod-A在Execution-Accept前返回RouteRejection

Gate
→ 不直接相信返回地址
→ 通过AssignmentProvider确认更高shard_revision
→ 原request_id、原operation_id
→ attempt=2调用新Holder
```

标识规则：

- 同一次公网请求的内部 Redirect 保持原 `request_id` 和 `operation_id`；
- 内部 Trace 增加 `route_attempt`，不放入公网幂等键；
- 客户端后来重新发起网络尝试时生成新 `request_id`，但仍保持原 `operation_id`。

第二次仍返回 Redirect：

```text
→ 停止内部重试
→ 检测相同Holder/Revision回环
→ 强制刷新Assignment Snapshot
→ 返回REJECTED + ROUTE_UNSTABLE + RETRY_SAME_OPERATION
→ 建议短暂退避
```

这里 `REJECTED` 表示本次服务端尝试确认尚未进入业务执行边界，
`RETRY_SAME_OPERATION` 表示原用户意图仍可继续：

```text
REJECTED + NONE
→ 业务或权限拒绝，不自动重试

REJECTED + RETRY_SAME_OPERATION
→ 本次未执行，保留原operation_id重试
```

CommandStatus 与 RetryDirective 正交，不需要增加新的业务状态。

原因是连续 Redirect 通常说明 Watch 版本空洞、Handoff 未收敛或路由循环；继续重试会放大故障。
`RouteRejection` 中的新 Holder 只能作为刷新线索，必须由 AssignmentProvider 验证后才能调用。

一旦跨过 Execution-Accept，禁止再向 Gate 返回 Redirect；后续迁移、超时和未知结果由
FirstHop、Coordinator、Worker 和幂等 Receipt 处理。

### 9.6 GetOperationResult 内部查找

第 28 号公网查询携带原命令、原 operation，以及派生执行 Root 所需的原业务目标或服务端返回的
不透明 Locator：

```text
original_command_type
original_operation_id
original_target | server_operation_locator
```

Gate 从当前受信 Session 得到 `subject_player_id`，再根据原 CommandSpec 找到
FirstHopRole、OperationRootPolicy/Extractor 和 OperationResultLookupPolicy：

```text
EffectiveIdempotencyKey {
    subject_player_id
    original_command_type
    original_operation_id
    OperationRootRef
}
```

`original_target` 不是可信 Authority 地址。OperationReadAPI 必须重新执行与原命令相同的结构校验与
Root 派生；`server_operation_locator` 必须带完整性保护并绑定 subject、command、operation 和 Root，
不能成为越权读取其他玩家结果的后门。

Locator 冻结为“带完整性保护的可恢复地址”，至少封装 `locator_version / subject_player_id /
command_type / operation_id / OperationRootRef / source_kind / source_ref / not_after_ms / key_id`。它可以使用
签名或认证加密的无状态封装，不要求单独建立 Locator 数据库；但 `source_ref` 必须指向已经
可靠创建的 Workflow、SourceOrder 或 Authority Receipt。校验时仍必须使用当前 Session 主体，
过期/未知 key_id/签名失败均 Fail Closed。

逻辑结果：

```text
NOT_FOUND
→ 当前未找到Workflow/Receipt/Result
→ 不证明原请求从未到达
→ 按原CommandSpec RecoveryPolicy恢复环境
→ 只能使用原operation_id和原Payload重发

PROCESSING
→ 可靠Workflow/Order已存在
→ 返回retry_after_ms
→ 只能轮询

COMPLETED
→ 返回保存的原CommandResult

EXPIRED
→ 结果目录已超过保留期
→ 停止自动重放，刷新相关权威状态
```

`EXPIRED` 必须有结果目录水位或保留期元数据作为证据，不能仅因查不到记录就从
`NOT_FOUND` 推断结果过期。

同一 Root 内相同 operation_id 的并发重发必须依靠 CreateIfAbsent、Authority 幂等 Receipt 和 Canonical Payload
Hash 收敛到同一业务意图。因此 `NOT_FOUND` 即使与原请求在途并发，也不能造成重复扣资产。

OperationResult 是逻辑可发现性合同，不要求所有终态都物理复制到一张永久全局表：

- Workflow 命令可以从 Workflow/Settlement 读取；
- 直接 Actor 命令可以从权威 Receipt 或有 TTL 的 Result Directory 读取；
- 只有能够按确定性地址读到未完成 Workflow/Order/Unknown 证据时才能返回 `PROCESSING`；
- 投影目录暂时未更新且请求中没有足够字段定位来源 Authority 时返回 `NOT_FOUND`，不能凭空猜测在途工作；
  原 operation_id 重试仍必须安全；
- 具体 Redis Key、原子性、TTL 和容量由第 31/32 号合同冻结。

P0 典型查找：

```text
UnlockPlot/Plant/Harvest/Theft
→ 由EffectiveIdempotencyKey/服务端Locator确定性定位Workflow
→ Workflow存在且未终态：PROCESSING

Buy/Sell
→ 固定定位PLAYER:subject_player_id的AuthorityReceipt

Care
→ 原命令携带target_farm_id，Root按canonical friend pair派生
→ 有可靠Locator时直接定位；没有Locator且ResultDirectory缺失时按原target重新定位来源
→ H5按Journal中的原target_farm_id Fresh Join，并以原operation_id + 原Payload重发
```

`NOT_FOUND` 只表示“当前查询入口无法定位证据”，不表示命令从未执行。已签发的
`server_operation_locator` 按上述版本、完整性、密钥轮换和保留期合同解析；它指向可靠来源，
本身不是可随意丢失的查询 Projection。

JoinFarm 不建立长期 OperationResult；纯查询直接重查。

NOT_FOUND 的恢复必须尊重原命令准入环境：

```text
纯Player命令
→ RETRY_SAME_OPERATION

Farm Subscription命令且当前仍在原农场
→ RETRY_SAME_OPERATION

Farm Subscription命令但已经切换农场
→ FRESH_JOIN_THEN_RETRY

原Payload已不可恢复或结果EXPIRED
→ REFRESH_AUTHORITY_STATE
```

因此 H5 PendingOperationStore 在终态前必须保留：

```text
command_type
operation_id
原Typed Payload
原target_farm_id
```

不能只保存 operation_id。客户端重放前必须重新通过当前 SessionAdmission；旧
`admitted_subscription_id` 不能直接复用。

---

## 10. 稳定语义合同编号（候选版）

以下编号表示实现不得自行改变的稳定语义；本文仍是 freeze-candidate。最终 Proto field number、
Redis 物理布局、TTL 数值和容量参数只有在实现、兼容性与故障测试通过后才进入正式 Freeze。

```text
L2-RPC-01
Gate只调用player-svr/farm-svr FirstHop，不直接调用Actor或推进多个Participant。

L2-RPC-02
Gate携带原始payload bytes；FirstHop再次解码、独立生成唯一权威CanonicalPayloadHash并
复核RouteKey；Gate Hash不参与业务正确性。

L2-RPC-03
在线命令、Workflow/Order正常首次推进和Worker恢复分别使用TrustedCommandContext、
CoordinatorWorkContext和InternalCallerContext。

L2-RPC-04
ActorRouteHeader与Caller Context正交；操作者ID不能替代目标Actor ID。

L2-RPC-05
FirstHop必须从RPC连接取得真实服务身份，不能仅信任消息中的caller字符串。

L2-RPC-06
SessionPermit是Gate内部准入结果，不是客户端Token；P0不要求每条命令查询Redis。

L2-RPC-07
FirstHop必须确认Payload目标、RouteMode、RouteHeader、VirtualShard、Holder BootID、
ShardRevision和本地Shard Lease一致；ActorRecord.writer_epoch只在Activation Claim/持久化中
校验，ActorRecord.writer_claim_id由本地Activation持有并在每次强写中校验，禁止每请求远程读取。

L2-RPC-08
Coordinator/DirectDispatcher位于player-svr/farm-svr进程内，分别正常推进Workflow/Order；
farm-worker持Lease恢复原Work，不得创建第二份业务意图。

L2-RPC-09
DueIndex只保存第31号定义的紧凑WorkRefV1；Repository根据扫描Bucket上下文加载WorkRecord并复核
完整WorkAddress。Coordinator/Worker发往Participant的调用必须携带该完整WorkAddress；Worker还必须
携带expected_work_version、owner token、lease_epoch和确定性invocation_id；Ingress在
Mailbox前校验Lease，ParticipantReceipt负责步骤级防重。

L2-RPC-10
业务结果与RouteRejection互斥；RouteFeedback只允许在Execution-Accept之前，Redirect重试
必须保持原operation_id。

L2-RPC-11
玩家命令和Participant调用采用gRPC Unary并复用HTTP/2长连接；持续控制面Watch可使用
Streaming。

L2-RPC-12
内部同步等待使用gRPC Context Deadline；可靠Workflow建立后，Gate取消只结束同步等待，
不得取消Workflow。

L2-RPC-13
TrustedCommandContext是第26号SessionPermit在ADMITTED时刻的完整最小投影，必须包含
Gate BootID、SessionRouteRevision和admitted_at，并使用admitted Farm命名。

L2-RPC-14
ParticipantStep使用稳定枚举；ParticipantInvocationID由workflow/order和step instance
确定性生成。

L2-RPC-15
一次公网命令最多进行一次内部Redirect；第二次Redirect停止重试、刷新Assignment并返回
ROUTE_UNSTABLE，所有重试保持原operation_id。

L2-RPC-16
RouteRejection返回的Holder必须经AssignmentProvider验证，禁止直接信任返回地址或产生
Redirect循环。

L2-RPC-17
GetOperationResult按当前subject_player_id、original_command_type、original_operation_id和由原目标或
可信Locator派生的OperationRootRef查找；NOT_FOUND不证明未执行，只允许在同一Root内使用原
operation_id和原Payload重发。

L2-RPC-18
PROCESSING只能轮询，COMPLETED返回原结果，EXPIRED停止自动重放并刷新权威状态；Join和纯查询
不进入长期OperationResult。

L2-RPC-19
Work Lease换代只改变推进者；当前ParticipantInvocation结果未明确前，Work不得进入与该Step
不兼容的状态，新旧恢复者必须使用相同invocation_id和Canonical Step Payload。

L2-RPC-20
ParticipantReceipt使用Participant Ingress根据Typed Step Payload生成的CanonicalStepPayloadHash，
不得复用公网Command Payload Hash。

L2-RPC-21
ParticipantRequest由ActorRouteHeader、CoordinatorWorkContext或InternalCallerContext、
ParticipantCommandType和typed_payload正交组成；Gate不得直接构造Participant调用。

L2-RPC-22
RouteRejection不含OK；路由成功必须进入BusinessCommandOutcome分支。

L2-RPC-23
effective_budget取grpc_context_remaining、request.remaining_budget_ms和步骤最大预算的最小值；
下游只能缩短，不能延长上游Deadline。

L2-RPC-24
CommandStatus描述本次尝试，RetryDirective描述用户意图下一步；REJECTED只有在确认未跨过
Execution-Accept时才能与RETRY_SAME_OPERATION组合。

L2-RPC-25
NOT_FOUND后的重放服从原CommandSpec RecoveryPolicy；农场上下文改变时必须Fresh Join原目标，
客户端在终态前保留原Typed Payload和target_farm_id。

L2-RPC-26
gRPC Context取消只取消当前同步等待，不构成玩家业务Cancel；Execution-Accept前可以停止
尚未接管的工作，Execution-Accept后不得取消Workflow/Order或已入Mailbox的命令。


L2-RPC-27
TrustedCommandContext的FarmAdmissionContext只来自ActiveSubscription；JOIN_FARM使用独立
JoinAdmissionContext，PendingJoin不得用于经营命令准入。

L2-RPC-28
JOIN_FARM ACCEPTED只返回Join元信息；Gate必须先把ACCEPTED放入该连接的ConnectionWriter，
再调用ArmSnapshot。未成功Arm的Join不得发送FarmSnapshot，不得依赖两条独立RPC的调用先后
推断到达顺序。

L2-RPC-29
SnapshotQueueAccepted必须携带farm_id、subscription_id、join_attempt_id、snapshot_version和完整
DownlinkTargetFence；重复回调幂等，旧Fence只能忽略。该回调只允许Hub从JOINING进入FLUSHING，
不得激活Gate Subscription或令H5进入FARM_INTERACTIVE。

L2-RPC-30
每批Catch-up都必须在同一ConnectionWriter FIFO中排在Snapshot之后，并以完整Fence确认
QueueAccepted；部分入队、批次缺口或版本不连续时不得继续激活，应取消Join或要求重新Join。

L2-RPC-31
Snapshot QueueAccepted后Hub在同一Lane冻结有限ready_cut_version=N；Hub只有在Snapshot及截至N
的连续Catch-up均确认入队后，才能调用ActivateSubscription。N之后变化进入post-ready缓冲，
不得反向延长本次Ready目标。
Gate必须把SUBSCRIPTION_READY(N)排在同一连接FIFO的上述数据之后，并原子执行PendingJoin到
ActiveSubscription的切换；Hub收到Ready QueueAccepted后才进入IN_FARM。

L2-RPC-32
DownlinkService禁止从第一条Farm Push隐式绑定Subscription；所有Push必须匹配显式PendingJoin或
ActiveSubscription。

L2-RPC-33
已知subscription_id的Disconnect/切换/顶号使用CleanupSubscriptionRequest；FirstHop已创建未Arm
JOINING但Gate未知subscription_id时使用CancelJoinAttemptRequest。两者均按FARM:farm_id精确路由，
禁止扫描全部FarmActor或向全部SystemQueue投递清理探测。

L2-RPC-34
异常断线或Gate崩溃由带完整Subscription Fence的短租约兜底；Gate按Holder批量续租，Hub仅能
延长完整Fence匹配的既有未关闭Subscription。Renew使用requested/accepted TTL；Gate以续租请求
开始时的本地单调时钟和安全余量建立Deadline。Renew不能创建、替换或复活Subscription；Gate达到
Deadline时停止生成FarmAdmissionContext并要求Fresh Join。租约只回收Presence运行态，不参与
好友权限或农场业务裁决。

L2-RPC-35
Execution-Accept后发生Deadline、连接失败或迁移时不得返回RouteRejection；有可靠锚点返回
PROCESSING，无最终结果时按原幂等身份进入Receipt/Operation Unknown解析。

L2-RPC-36
未Arm的JOINING不得预取Snapshot或预建变化捕获。首次有效Arm必须在与FarmChange发布同序的
Farm Runtime Lane建立唯一Snapshot(V)和V+1捕获游标；重复Arm不得生成第二份基线。

L2-RPC-37
Participant Ingress必须先验证真实调用身份、Invocation/Step Hash绑定以及当前Holder/Shard Lease，
再在正确权威侧查询ParticipantReceipt；Receipt只在业务判定上优先于旧WorkVersion/Worker Lease。

L2-RPC-38
Gate发起CancelJoinAttempt前必须在Session Lane原子进入CANCELLING；请求只能按farm、attempt、
operation和完整连接Fence取消仍未Arm的JOINING。已经Arm或激活时返回ALREADY_PROGRESSED，且
只有仍匹配同一CANCELLING Fence的Gate才能使用返回的subscription_id精确Cleanup。

L2-RPC-39
ActiveSubscription/FarmAdmissionContext只证明当前观看和路由上下文；FriendRelation/ActionPermit
与FarmActor最终裁决仍是Care/Theft等好友操作的业务授权边界。

L2-RPC-40
BatchRenew是P0物理基线；GateBoot Lease只能作为压测后的等价优化，不能取消完整Fence、Fail-Closed
或Subscription最终过期能力。

L2-RPC-41
CancelJoinAttempt与CreateOrReuseJoining必须在同一Hub Lane按attempt排序；有效Cancel即使暂未找到
JOINING也先安装有界Tombstone，阻止迟到的同attempt Join重新创建名额。

L2-RPC-42
本文是TrustedCommandContext和ActorRouteHeader字段结构的唯一权威；request/operation/deadline/
trace位于请求外层，26/27号文档只能定义来源或消费语义，不得复制另一套IDL。

L2-RPC-43
Work Lease Claim/Renew只接受requested TTL；Work Repository使用可信时间生成内部expire_at并返回
owner token、lease epoch、work version和accepted TTL。Worker使用本地单调Deadline，不能提交或
比较本机绝对leaseUntil。

L2-RPC-44
Worker超过本地Lease Deadline后不得发起新Participant Step；进入Mailbox前RecoveryIngress仍须
查询Work Repository，以可信时间校验当前owner/boot/token/epoch/WorkVersion/step。

L2-RPC-45
Shard Lease热路径依赖Holder本地有效Lease证明，不能逐玩家命令查询Lease Authority；达到本地
单调Deadline立即Self-Fence，存储强写继续由writer_epoch+writer_claim_id最终Fence。

L2-RPC-46
PostCommitDeliveryGuard必须覆盖Authority提交结果到实时交接；CommittedChange普通队列失败后的
RealtimeInvalidation必须来自独立level-triggered状态和Control通道，普通条目和新Join基线按
delivery_generation隔离；内部RESYNC_REQUIRED映射成公网REJOIN_REQUIRED，不能向原失败队列
再投一次性Resync事件。

L2-RPC-47
Gate发送REJOIN_REQUIRED前必须保留并复核旧ActiveSubscription终止Fence；终止Push入队或连接
确认不可达后才能释放。队列无法接受终止控制帧时必须关闭连接，不能继续允许该订阅写入。

L2-RPC-48
JOIN_FARM的内部BusinessCommandOutcome在ACCEPTED时必须携带由FirstHop产生并经受信上下文复核的
target_farm_id、subscription_id和join_attempt_id；Gate据此绑定PendingJoin并构造公开
JoinResultContext。公开result_payload保持为空，非Join结果不得携带该Context。

L2-RPC-49
ActorRouteHeader.source_snapshot_revision等于RouteSnapshot.GlobalRevision；shard_revision等于对应
ShardWriteAuthority.assignment_revision。Holder、Boot、ShardRevision和ExpectedWriterEpoch必须
来自同一不可变Snapshot记录，不得跨Revision拼接；LeaseEpoch由目标Holder本地证明注入Store Fence，
不由上游Header声明；最终写权仍由Holder与Store复核。

L2-RPC-50
CoordinatorParticipantService.ExecuteStep只接受CoordinatorWorkContext；
WorkerRecoveryIngress.ResumeStep只接受InternalCallerContext并在Mailbox前校验Work Lease。二者复用
同一ParticipantCommandCatalog、Participant Handler和Receipt逻辑，Gate不得直接调用。

L2-RPC-51
UnlockPlot和Plant的八个ParticipantCommand、目标Actor、WorkType和允许首次执行的Work状态以第7.8节
为唯一权威；未知命令、Typed Payload错配和RouteDomain错配必须在Mailbox前拒绝。

L2-RPC-52
ParticipantInvocationID由work_id、work_type、participant_command和稳定step_instance确定性派生；
重试、Coordinator切换Worker和Lease换代不得产生新ID。

L2-RPC-53
Participant Step Hash只能由Typed Step语义字段的显式Canonical Builder产生，不得Hash原始Proto bytes；
Trace、Deadline、Lease、路由Revision和预算不进入业务Hash。

L2-RPC-54
依赖前一步的Participant Payload必须携带前一步Result Hash，并与唯一WorkRecord已确认结果一致；
Finalize和Release只有在互斥Work状态允许时才能首次执行。

L2-RPC-55
Participant的APPLIED、REJECTED和NO_CHANGE都是带Receipt的持久终态；TIMEOUT_UNKNOWN不是业务终态，
不得据此推进后续步骤或释放Reservation。

L2-RPC-56
UnlockPlot/Plant的ReservationID、CropCycleID均按work_id确定性派生；Commit步骤的服务端规则快照、
Plant提交时间和依赖结果不得由客户端Payload伪造。

L2-RPC-57
Harvest最终奖励只能由FarmActor在COMMIT_HARVEST不可逆提交点根据当前CropCycle和stolen_amount冻结；
Coordinator和客户端不得预先决定数量。

L2-RPC-58
GRANT_HARVEST_REWARD必须绑定Farm Participant Result Hash和HarvestDecision Hash，并将库存、经验、
计数、图鉴与Receipt一次提交；Farm已提交后不得以仓库满或普通业务拒绝结束Workflow。

L2-RPC-59
Theft的ActionPermit是CommitFriendActionAdmission持久生成并与operation绑定的内部快照，不是客户端
Token；Admission已提交的Dispatch/Work可恢复完成，好友后续删除只阻止新operation。

L2-RPC-60
Theft的settlement_id按work_id确定性派生；偷取数量、槽位、宠物随机结果和折算单价只能由FarmActor
依据CropCycle与GuardSnapshot首次裁决并写入唯一TheftTargetDecision。

L2-RPC-61
合法Theft Attempt即使宠物完整截留也必须写AttemptReceipt；Visitor结算即使净数量为0也必须写
TheftVisitorReceipt和ParticipantReceipt，不能重新偷取。

L2-RPC-62
CREDIT_GUARD_INCOME只在guard_coin_value大于0且Visitor已结算时执行；它只给Owner增加普通金币，
绝不从Visitor钱包扣款。

L2-RPC-63
Harvest Farm Commit和Theft Farm Decide都是不可逆点；此后只能继续原Workflow，不存在恢复CropCycle、
返还gross偷取量或换operation_id重试的补偿。

L2-RPC-64
Participant领域Decision Hash与Participant Result Hash职责不同：前者固定经济裁决，后者绑定完整步骤
返回；后续步骤必须校验WorkRecord保存的Result Hash，并重算Decision Hash。

L2-RPC-65
Care的Incident RESOLVED事实、CareResolutionSnapshot和CareRewardOrder必须在同一Farm Authority
Commit中产生；不创建CareWorkflow、奖励Outbox或第二套DueIndex。

L2-RPC-66
APPLY_CARE_REWARD必须使用来源Order冻结的CareResolutionSnapshot；OWNER护理只发基础奖励，FRIEND
护理还产生唯一FriendInteractionFact并按事件发生时间归属任务Period。

L2-RPC-67
Farm已RESOLVED后，Player护理奖励属于必须最终应用的业务效果；普通业务拒绝不能结束Order，重试由
CareRewardReceipt和ParticipantReceipt防重。

L2-RPC-68
CareRewardOrder在PLAYER_REWARD_GRANTED后即可COMPLETED；OpportunitySummary不是该Order的必达
第二步骤，投影故障不得占住业务Order或反向阻止护理。

L2-RPC-69
任何改变好友机会的Farm提交必须推进opportunity_dirty_generation；投影采用每Farm/ProjectionKind
最多一条可合并刷新状态与Reconcile，禁止每次Care创建不可合并Projection Order。

L2-RPC-70
Care奖励延迟跨越每日重置时，任务Period按resolved_at_ms和配置时区计算，不得按Participant实际投递
时间把旧互动计入新一天。

L2-RPC-71
PlantWorkflow在CreateIfAbsent时冻结完整CropRuleSnapshot与Canonical Hash；当前配置后续热更新只影响
新Work。在途Work、Worker恢复和Farm Participant不得重新读取当前配置替换快照。

L2-RPC-72
P0十个2%偷菜槽位使用整数数量；启用该策略的base_yield必须为50的倍数，配置加载时证明每槽大于0、
总和不超过20%且全部运算无溢出，运行时不得临时四舍五入。

L2-RPC-73
CarePlanTemplate使用有界Incident列表；Plant Farm Commit生成确定IncidentID和最终due_at并与CropCycle
原子保存。P0固定偏移，未来随机窗口也只能在Plant时选择一次并持久化最终时间。

L2-RPC-74
Guard规则不在Plant时冻结。每个TheftAttempt使用FarmActor裁决时已生效的GuardSnapshot，并将完整
GuardDecisionSnapshot与AttemptReceipt和目标扣减原子提交；重试、迁移和配置更新不得重新抽取。

L2-RPC-75
Care奖励规则在Incident首次RESOLVED时冻结到CareResolutionSnapshot；APPLY_CARE_REWARD只执行该
快照，不读取当前奖励配置。作物保护折价单价则在Plant时冻结到CropCycle。

L2-RPC-76
Workflow的COMPLETED只表示Work已经终止，不等于公网命令SUCCEEDED。终态Work必须保存最终
CommandStatus、稳定ErrorCode、最终Result Hash和必要结果摘要，使GetOperationResult能恢复原
SUCCEEDED、REJECTED或NO_CHANGE。

L2-RPC-77
Harvest/Theft的Farm Participant必须显式返回COMMITTED、REJECTED或NO_CHANGE。REJECTED和
NO_CHANGE都是合法终态分支，不得调用后续Player Participant；只有COMMITTED才能跨越不可逆点。

L2-RPC-78
Plant的Farm Commit、HarvestDecision、TheftTargetDecision以及Care的Incident RESOLVED一旦权威提交，
不得因后续Participant、投影或下行失败回滚。Coordinator/Worker只能使用原Work/Order继续完成。

L2-RPC-79
Authority Commit返回TIMEOUT_UNKNOWN时，不得把Work推进到COMMITTED、REJECTED、NO_CHANGE或补偿
分支；必须隔离Actor并按第30号FenceAndClaimActor、AuthorityReceipt和StoredOperationOutcome解析。

L2-RPC-80
Participant响应丢失、Work/Order CAS响应丢失或Lease换代后，恢复者必须重用原
ParticipantInvocationID和CanonicalStepPayload；已提交步骤通过ParticipantReceipt返回原结果，禁止
重复扣减、发奖、增加任务或重新随机。

L2-RPC-81
Projection更新失败只保留dirty generation并由Reconcile修复；FarmChange实时交接失败只使受影响
Subscription进入RESYNC_REQUIRED/Fresh Join。两者都不得改变已提交Authority事实或Workflow终态。

L2-RPC-82
核心场景实现必须提供统一故障注入点：BEFORE_PARTICIPANT_SEND、
AFTER_AUTHORITY_COMMIT_BEFORE_RESPONSE、AFTER_PARTICIPANT_RESPONSE_BEFORE_WORK_CAS、
AFTER_WORK_CAS_BEFORE_NEXT_STEP、AFTER_PLAYER_COMMIT_BEFORE_RESPONSE、
AFTER_FARM_CHANGE_COMMIT_BEFORE_DELIVERY、BEFORE_ORDER_COMPLETE和FORCE_SHARD_HANDOFF。
测试实现可以采用Hook或Fake，不得把这些名称和开关编入生产公网协议。

L2-RPC-83
RESERVE_UNLOCK_COIN或RESERVE_SEED形成确定PARTICIPANT_REJECTED时，Work必须保存该Participant
Receipt/Result Hash并终止为PLAYER_REJECTED + COMPLETED(REJECTED)；没有Reservation，不得调用Farm
Participant，也不得发送Release步骤。

L2-RPC-84
UnlockPlot的COMMIT_PLOT_UNLOCK必须把确定未提交结果区分为FARM_REJECTED与FARM_NO_CHANGE；两者都
允许RELEASE_UNLOCK_COIN_RESERVATION，但释放完成后的最终CommandStatus分别保持REJECTED与NO_CHANGE。

L2-RPC-85
Release Reservation的Participant结果或Work CAS结果未知时，Work保持PROCESSING并使用原Invocation
查询Receipt；只有确认RELEASED后才能发布补偿终态。Farm Commit为TIMEOUT_UNKNOWN时禁止Release。

L2-RPC-86
相邻Plot的并发Unlock按FarmActor Mailbox线性裁决：后继Plot先执行且前置仍锁定则REJECTED；前置Plot
先提交后，后继请求若其余版本、资格和Reservation仍有效，可以在随后Turn成功，始终不得跳过锁定前置。

L2-RPC-87
Buy/Sell每条命令只包含一个offer_id或item_definition_id，但quantity允许为配置上限内正整数。FirstHop
和PlayerActor必须使用同一CheckedArithmetic语义计算总价和物品增减；一条命令不得携带多种物品明细。

L2-RPC-88
Buy/Sell越过Execution-Accept后的确定业务拒绝必须形成StoredOperationOutcome；结构非法、未认证、接纳前
限流和路由失败不写成业务终态。相同operation_id不能因余额、库存或配置后来变化而改变原裁决。

L2-RPC-89
好友Care/Theft必须通过CommitFriendActionAdmission逐operation准入。Pair Authority原子提交
ActionAdmissionReceipt、ActionPermitSnapshot、FriendActionDispatchOrder与同桶DueIndex时跨越
Execution-Accept；普通关系读取或内存Permit不构成接管。

L2-RPC-90
FriendActionDispatchOrder必须内嵌完整不可变ActionPermitSnapshot及其hash，并携带规范target binding、
payload hash、稳定InvocationID与continuation：
CARE直接投递Farm Participant，THEFT CreateIfAbsent确定性TheftWorkflow。Admission提交后的超时、进程
退出或删除好友都只能恢复原Dispatch，不能重新授权、改operation_id或静默丢弃。

L2-RPC-91
好友Join必须先在Hub创建AUTHORIZING临时记录，再由Pair Authority执行AuthorizePreparedVisit；Permit
绑定join operation/attempt、完整DownlinkTargetFence和relation epoch。禁止先签Permit、后创建Hub记录。

L2-RPC-92
FriendTerminationOrder撤销Subscription必须携带relation_id+captured_active_epoch；已绑定记录只按epoch
精确删除，未绑定epoch的同pair AUTHORIZING允许Fail Closed取消。旧epoch清理不得影响新关系Subscription。

L2-RPC-93
AddFriendWorkflow、FriendTerminationOrder、FriendActionDispatchOrder的WorkPlacement分别固定为
INDEPENDENT、FRIEND_PAIR来源、FRIEND_PAIR来源；Pair/Quota/ShareIssuer是非Actor Authority，不使用
ActorRouteHeader、Actor Lease或WriterClaim冒充其写权。

L2-RPC-94
FarmAdmissionContext必须从ActiveSubscription投影admitted_farm_id、subscription_id、可信激活时间和
可选好友authorization pair/relation/epoch；PendingJoin不能生成这些字段，H5也不能提交或覆盖。

L2-RPC-95
CONFIRM_FRIEND_VISIT必须验证当前好友FarmAdmissionContext后进入PlayerActor。访问周期使用Subscription
激活时间；不同合法witness满足同一一次性任务时返回NO_CHANGE而不是幂等冲突。

L2-RPC-96
P0不创建APPLY_FRIEND_VISIT_TASK、FriendVisitTaskProgressWork或任务专用Worker RPC。Harvest、Care、
Theft任务进度继续复用原Participant，任务列表、领奖和访问确认走PLAYER Actor Direct命令。

L2-RPC-97
WorkAddress以SourceAuthorityRef统一表达Actor与非Actor来源；FRIEND_PAIR来源不得填写Actor RouteDomain。
WorkType静态注册表必须冻结placement、允许来源Kind、TypedPayload和ParticipantStep，未知组合Fail Closed。

L2-RPC-98
DeleteFriend/BlockPlayer先在Pair Authority提交撤销和PENDING ReactivationBarrier并返回成功；
FriendTerminationOrder随后读取双方ShareIssuer Generation并按deleted relation epoch回填floor。Barrier
READY前禁止重加，READY后必须显式Rotate并使用generation严格大于floor的新链接；重激活成功后将
当前Barrier原子切换为CONSUMED。

L2-RPC-99
FriendQuota的ReleaseFriendReservation与RemoveActiveRelationIfEpochMatches是两个不同RPC；前者按
reservation_id释放未激活预约，后者按relation_id+captured_active_epoch删除已激活占用，不得合并成裸
ReleaseFriendSlot。

L2-RPC-100
CLAIM_MAIL_ATTACHMENT先按PLAYER Affinity到PlayerMailClaimIngress；Ingress读取MailEntry并构造
TrustedMailClaimMaterial后才进入PlayerActor。H5/Gate不得提交RewardBundle或伪造材料，Actor不得等待
MailEntryStore。

L2-RPC-101
ResumeMailDeliveryOrder与ApplyMailClaimProjection使用Coordinator/Worker上下文而非在线Session；
Delivery重放完整不可变快照，Projection只修复查询状态，不参与资产发放或领取防重。

L2-RPC-102
server_operation_locator是绑定subject/command/operation/OperationRootRef/source_ref/not_after的
带完整性保护封装；只能在来源Workflow/Order/Receipt已可靠建立后签发。它可无状态实现，
不是授权或查询Projection；REDEEM_SHARE越过Execution-Accept时必须向H5返回Locator以替代原始Token恢复。

L2-RPC-103
PlantWorkflow冻结的CropRuleSnapshot必须包含图鉴身份、unlock_eligible、Catalog版本和Definition版本；
FarmActor在COMMIT_PLANT时补入受信planted_by_player_id，形成随CropCycle持久化的
CompendiumUnlockSnapshot。Harvest不得按当前Catalog重新判断在途作物的解锁资格。

L2-RPC-104
COMMIT_HARVEST必须把CropCycle中的CompendiumUnlockSnapshot原样放入HarvestDecision并纳入
HarvestDecision Hash；GRANT_HARVEST_REWARD必须同时校验crop、owner、planted_by_player_id和当前
PlayerActor绑定，不接受客户端或Coordinator补写的解锁资格。

L2-RPC-105
GRANT_HARVEST_REWARD首次提交必须生成CompendiumUnlockDelta(newly_unlocked、可选crop_definition_id、
compendium_revision_after)，并与库存、经验、收获计数、ParticipantReceipt和StoredOperationOutcome
一次提交；该Delta属于GrantHarvestRewardResult语义字段并必须纳入Participant Result Hash。重放或
GetOperationResult返回原Delta，禁止根据当前图鉴集合重算历史操作结果。

L2-RPC-106
P0图鉴身份统一为crop_definition_id；CompendiumUnlockSnapshot、HarvestDecision、Participant Result、
公网HarvestResult和Player权威集合不得混用plant_id、display_definition_id或activity实例ID。

L2-RPC-107
GET_PLAYER_BOOTSTRAP必须由受信Gate生成TrustedCommandContext并进入PlayerBootstrapIngress；subject和
PLAYER RouteKey只来自session.player_id。该查询不要求Recovery完成或当前FarmSubscription，不得读取、
拼装或返回FarmSnapshot/Plot/Crop/FarmVersion，也不得创建长期OperationResult。

L2-RPC-108
GET_NEXT_FRIEND_OPPORTUNITY必须由受信Gate生成TrustedCommandContext并进入无状态
FriendOpportunityNavigation；viewer只来自session.player_id。依赖不可用必须返回可重试的暂时不可用，
不得伪装成NONE；候选精确性最终服从Friendship Authority与FarmReadAPI.BatchEvaluate。

L2-RPC-109
好友Farm的SUBSCRIPTION_READY成功入队并激活ActiveSubscription后，Gate可以使用该Subscription生成的
TrustedCommandContext最佳努力调用MarkFriendOpportunityVisited。它只更新Runtime Redis，不建立
Workflow/Order/OperationResult，不阻塞或回滚Join；该运行时去重接缝不得替代用于每日任务的公网
CONFIRM_FRIEND_VISIT。
```

---

## 11. 已冻结结论与物理待验证项

本节不再包含未决业务语义。第 1、2、5 项已经冻结为实现输入；第 3、4、6、7 项仍是物理参数或平台
实现待验证，不能由实现 Agent 擅自填成生产结论：

1. [第 30 号](./30-actor-authority-store-contract.md) 已冻结 AuthorityStore 的明确结果、
   `TIMEOUT_UNKNOWN`、FenceAndClaim 和隔离语义；七个核心写命令对应的 ErrorCode、CommandStatus、
   RetryDirective 和 OperationResult 恢复已由第 32 号第 10.6 节冻结；
2. UnlockPlot、Plant、Harvest、Theft、Care 的 Participant 终态与 `TIMEOUT_UNKNOWN` 语义已由第
   7.7～7.14 节冻结；它们与公网 OperationResult 的映射以第 32 号第 10.6 节为唯一场景权威，四条
   核心跨 Actor 主链的故障和并发验收以第 32 号第 10.8 节为唯一场景权威；
3. Deadline 总预算、FirstHop 保留预算、Participant 子预算、Work/Shard Lease requested TTL 与
   safety margin 的初始配置；可信时间和本地单调 Deadline 语义已经冻结；
4. OperationResult Directory、源 Receipt、WorkAddress 和 Redis Cluster Slot 的主布局已由第
   31/33A 号冻结；Locator 密钥轮换、精确 TTL/保留窗口与平台 Redis 参数仍待探针；
5. UnlockPlot、Plant、Harvest、Theft、Care 已冻结 `ExecuteStep/ResumeStep`、
   ParticipantCommandType 和 Typed Step Schema；最终物理 Proto 数字布局仍待生成；
6. mTLS/Workload Identity 在目标 Linux/K8s 环境中的具体平台实现和证书轮换；
7. 最终 Proto package、field number、`reserved` 和滚动兼容规则。逻辑 service/method 名已在
   第 7.7 节冻结；正式生成的
   `proto/internal/command_rpc.proto`、`subscription_rpc.proto`、`workflow_rpc.proto` 将成为数字布局
   的唯一物理权威；在此之前本文代码块中的 field number 只表达逻辑分组，禁止多个实现 Agent 各自
   固化不同数字。

### 11.1 本轮新增合同的退出 Candidate 验证

1. 让 Farm 在 Join Catch-up 期间持续产生 Change，验证固定 `ready_cut_version` 后 Ready P99 有界，
   且 N+1 之后变化全部排在 Ready 后；
2. 分别注入“Join先到、Cancel后到”和“Cancel先到、迟到Join后到”，验证 `CancelJoinAttempt`
   不依赖 `subscription_id` 即可释放/阻止名额，Tombstone 与旧 attempt 不影响新 Join；
3. 把 Participant 请求故意发给旧 Holder，并让共享存储中已经存在 Receipt，验证旧 Holder 仍先
   返回 RouteRejection，只有当前 Holder 能读取并返回权威 Receipt；
4. 构造 Receipt 已提交但旧 WorkVersion/Worker Lease 失效，验证当前 Holder 返回原 Participant
   结果，而不是误报版本或 Lease 冲突；
5. 注入 Gate/Holder 墙钟偏差和高 RTT，验证本地单调 Deadline 不晚于 Hub 实际 Lease 到期；
6. 按 `active_subscription_count / renew_interval` 计算并压测 Renew Entry QPS、批次 RPC QPS、Hub
   Lane 背景占用和 Gate 崩溃后的名额回收时延，再决定是否引入 GateBoot Lease 优化。
7. 对 UnlockPlot/Plant 的八个 ParticipantCommand 执行“同 Invocation + 同 Payload”重放，验证只
   返回原 Receipt；改动任一语义字段后必须返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`；
8. 在 Reserve 成功响应丢失后由 Worker 重试，验证相同 ReservationID、余额/库存只预留一次；
9. 在 Farm Commit 成功响应丢失后注入 Lease 换代，验证相同 CropCycleID/Plot 解锁结果只提交一次；
10. 让旧 Release 与新 Finalize 并发到达，验证 Work 状态、依赖 Result Hash 和 Reservation 状态只允许
    一个合法分支，Farm 已提交时绝不退款/返种；
11. 改动 CropRuleSnapshot 任一规则字段但保留旧 Hash，验证在进入 FarmActor Mailbox 前拒绝；
12. 注入 Participant AuthorityCommit `TIMEOUT_UNKNOWN`，验证 Coordinator 不推进下一步也不执行
    Release，而是按第 30 号 Fence/Receipt 解析后继续。
13. 在 COMMIT_HARVEST 成功响应丢失后迁移 Farm Shard，验证重放返回同一 HarvestDecision，Player
    奖励、经验、计数和图鉴各只增加一次；
14. 让 Harvest 与 Theft 并发进入同一 CropCycle，验证 FarmActor 产生唯一顺序，最终满足
    `base_yield = owner_reward + 全部gross_stolen`；
15. 让 DECIDE_THEFT 成功后分别在 Visitor/Owner Participant 前崩溃，验证相同 settlement_id 最终
    完成，宠物结果不重抽、Attempt 不删除；
16. 注入 `net_received_quantity=0`，验证 Visitor Receipt 和互动事实仍提交，且不会再次获得 Attempt；
17. 分别让 CommitFriendActionAdmission 与删除好友 Pair CAS 先提交：Admission 先提交时验证原
    Dispatch/Work 可恢复；删除先提交时验证不产生 AdmissionReceipt、Permit 或 DispatchOrder；
18. 篡改 TheftTargetDecision、Farm Result Hash 或 Visitor Result Hash 任一字段，验证后续 Player
    Participant 在 Mailbox 前拒绝且不修改资产。
19. 注入 Care Farm Commit 成功而 DirectDispatcher 丢消息，验证 worker 从来源 DueIndex 找到同一
    CareRewardOrder，Player 经验与互动事实只提交一次；
20. 注入 APPLY_CARE_REWARD 成功但 Order CAS 响应丢失，验证重试命中 Player Receipt 后把原 Order
    完成，不重复推进任务；
21. 让护理奖励跨每日零点延迟投递，验证任务 Period 使用 resolved_at_ms，不把昨日互动计入今日；
22. 关闭 Projection Redis 或让 Summary 刷新持续失败，验证 Care 仍成功、玩家奖励 Order 可完成，
    `dirty_generation > projected_generation` 最终由 Reconcile 修复且不会产生每次护理一条积压 Order。
23. 加载十个2%槽位但 `base_yield` 不是50倍数的作物配置，验证 ConfigRegistry 整版拒绝 Ready，运行时
    不做四舍五入；合法配置生成十个正整数槽位且总和恰好20%；
24. PlantWorkflow 创建后热更新 CropRule，验证 Reserve/Commit/Worker 恢复仍使用原 Snapshot、Hash、
    mature_at、StealPlan 和 CarePlan，新 operation 才使用新版本；
25. 在 COMMIT_PLANT 响应丢失后迁移 Farm Shard，验证相同 CropCycleID 的 IncidentID、due_at 和
    slot_quantity 完全相同，不重新随机或生成第二套计划；
26. 对同一 `theft_attempt_id` 重放 DECIDE_THEFT 一百次并跨 WriterClaim 换代，验证只产生一个
    GuardDecisionSnapshot、一个 AttemptReceipt，random_draw/trigger结果不变；
27. 在两个不同好友 Attempt 之间更新已生效 GuardSnapshot，验证旧 Attempt 返回旧 Decision，新 Attempt
    使用新 Guard 版本，且两者各自可恢复；
28. Care RESOLVED 后更新奖励配置并延迟 APPLY_CARE_REWARD，验证 Player 只得到
    CareResolutionSnapshot 中冻结的奖励和任务归属。
29. 让 COMMIT_HARVEST 明确返回 REJECTED 与 NO_CHANGE，验证 HarvestWorkflow 分别终止为原业务结果，
    不调用 GRANT_HARVEST_REWARD；GetOperationResult 可恢复相同终态；
30. 让 DECIDE_THEFT 明确返回 REJECTED 与 NO_CHANGE，验证不调用 Visitor/Owner Participant，且
    REJECTED 不创建 Attempt、ALREADY_ATTEMPTED 返回原 Attempt 摘要；
31. 两个不同 operation 同时 Plant 同一空地，验证 FarmActor 只接受一个 CropCycle，失败 Work 在确认
    Farm 拒绝后释放自己的 SeedReservation，成功 Work 不受影响；
32. 两个 Harvest 同时命中同一 CropCycle，验证只提交一个 HarvestDecision、一个玩家奖励；另一个进入
    REJECTED/NO_CHANGE 终态；
33. 多个 owner/好友同时护理同一 Incident，验证只产生一个 CareOperationReceipt 与 CareRewardOrder，
    其他操作均为 NO_CHANGE 且不增加经验或任务；
34. 同一 visitor 使用不同 operation_id 并发偷同一 CropCycle，验证确定性 theft_attempt_id 只产生一个
    Attempt/Decision，后续返回 ALREADY_ATTEMPTED，宠物结果不重抽；
35. 十一名不同好友并发偷同一 CropCycle，验证 FarmActor 串行消费至多十个2%槽位，第十一名得到
    NO_STEALABLE_QUOTA，且最终满足 `base_yield = owner_claimable + sum(gross_stolen)`；
36. 在 Authority Commit 后关闭投影 Redis 或丢弃 FarmChange，验证业务结果不回滚：投影由
    dirty generation/Reconcile 修复，在线页面通过 RESYNC_REQUIRED/Fresh Join 恢复；
37. 对 L2-RPC-82 的每个故障点分别执行超时、丢响应、进程退出和必要的 Shard Handoff，验证同一
    operation/work/invocation 最终收敛到唯一终态；
38. 在 AFTER_AUTHORITY_COMMIT_BEFORE_RESPONSE 注入故障并让 WriterClaim 换代，验证旧请求不能迟到
    覆盖，新 Holder 通过 Receipt 解析为 COMMITTED，而不是猜测失败或执行补偿；
39. 在 AFTER_PARTICIPANT_RESPONSE_BEFORE_WORK_CAS 与 BEFORE_ORDER_COMPLETE 注入故障，验证恢复者
    命中 ParticipantReceipt 后只推进 Work/Order，不重复执行业务副作用；
40. 将同一场景分别经 Coordinator 快速路径和 farm-worker 恢复路径执行，验证 Participant Catalog、
    Hash、Receipt、最终 Result 完全一致。
41. 令 RESERVE_UNLOCK_COIN 分别因等级不足和金币不足明确拒绝，验证 Work 终止为原 REJECTED，
    不创建 Reservation、不调用 FarmActor、不发送 Release；
42. 令 RESERVE_SEED 因库存不足明确拒绝后给玩家补充种子，再以相同 operation/invocation 重试，验证仍
    返回原拒绝；使用新 operation 才能按新库存重新裁决；
43. 让 COMMIT_PLOT_UNLOCK 分别返回 FARM_REJECTED 与 FARM_NO_CHANGE，验证都可靠释放金币，但最终
    GetOperationResult 分别保持 REJECTED 与 NO_CHANGE；
44. 在 RELEASE_UNLOCK_COIN_RESERVATION 提交后丢响应，并在 Work CAS 前退出进程，验证 worker 命中
    Release Receipt 后只发布一次补偿终态，金币只返还一次；
45. 在 COMMIT_PLOT_UNLOCK `TIMEOUT_UNKNOWN` 时尝试触发 Release，验证 Ingress/状态机拒绝该步骤；
    Fence解析为已提交后只能 Finalize，解析为未提交后才进入正确释放分支；
46. 并发提交 Plot 6/Plot 7：让 Plot 7 先进入 FarmActor 时必须拒绝；让 Plot 6 先提交、Plot 7 后执行时，
    在两笔独立Reservation均有效的前提下允许顺序解锁两块地；
47. Buy 20 个种子和 Sell 35 个作物，验证请求仍只有一个物品类型，数量、总价、余额和库存使用检查算术
    一次原子提交；quantity为0、超上限或乘法溢出均在写入前拒绝；
48. 对 Buy/Sell 的金币不足、库存不足和价格变化响应注入丢包，再改变玩家余额/库存/配置，以相同
    operation重试，验证返回已保存的原拒绝；新operation才重新裁决；
49. 并发执行 Buy、Sell、ReserveUnlockCoin和ReserveSeed，验证 PlayerActor唯一顺序下available/reserved
    余额与库存从不为负，Sell不能消费已经预留的种子，Finalize不发生二次扣减。
