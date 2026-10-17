# 09 · S2 v0.2 最小协议确认清单

版本：确认稿 v0.3
更新时间：2026-07-27
状态：C1～C7、双 Actor ID、Bootstrap 已按 PlayerActor/FarmActor 更新；心跳字段待 Direct
WebSocket/CLB 探针，TCONND T8 只属于候选 Profile。满足
[S2 Go/No-Go](./08-s2-go-no-go.md) 前不修改现有 Proto/生成代码。

前提：

- [系统骨架](./05-system-skeleton.md)
- [业务事实分类](./06-business-fact-classification.md)
- [Player/Farm Actor 拆分](./16-player-farm-actor-split.md)
- [协议目标合同](./10-protocol.md)
- [内部接口](./40-interfaces.md)
- [关键时序](./41-sequences.md)

---

## 0. S2 只需要支持什么

S2 唯一链路：

```text
HTTPS TestLogin
→ EnsureAccountInitialized
→ WSS ClientHello / ServerHello
→ JoinFarm(self)
→ JoiningBuffer
→ FarmSnapshot
→ 断线后重新认证和 Join
```

本轮 Proto 可以保留后续命令枚举，但不为购买、种植、偷菜、任务、邮件、宠物和 Worker 编写完整业务 payload。

---

## 1. 当前 Proto 与 v0.2 的差异

| 文件/消息 | 当前问题 | v0.2 处理 |
|---|---|---|
| `FrameMeta/自定义长度头` | total_size/meta_size 与 WSS/TFRAMEHEAD 重复；Meta 重复保存 operation_id/error_code | 删除 FrameMeta 和 8 字节头；改为单一 FarmFrame protobuf |
| `ClientCommandEnvelope` | protocol_version/client_seq 与外层 FarmFrame 重复；缺少客户端请求关联 ID | 删除重复字段并 reserved；新增唯一 `request_id` |
| `TrustedCommandContext` | 内部单独生成 request_id，会与客户端请求关联脱节 | request_id 只保留在 ClientCommandEnvelope；Permit 绑定并证明其可信上下文 |
| `CommandType` | 把 EnsureAccountInitialized 暴露为客户端命令 | 删除枚举语义，字段号 1 reserved；改走 BootstrapService |
| `CommandResultEnvelope` | 向 H5 暴露 owner_state_version | 删除并 reserved 字段号 5 |
| `ServerPushEnvelope` | server_seq 与外层 FarmFrame 重复 | 删除并 reserved 字段号 1 |
| `PushType` | 缺 OperationResult/ResyncRequired | 新增枚举值 |
| 控制消息 | 缺 ClientHello/ServerHello | 新增 `common/control.proto` |
| Farm 交互模型 | 缺 JoinFarm/FarmSnapshot/FarmChange | 新增 `farm/farm_view.proto` |
| Logic RPC | 缺 Bootstrap/Downlink；TrustedContext 少 authority epoch | 补齐服务和字段 |
| Worker RPC | ExpirePresence 错放在可靠 Worker | 删除 ExpirePresence RPC |
| OwnerMeta | 缺 bootstrap_id/applied_bootstrap_rule_version | 增加建档幂等字段；规则版本由 Logic ConfigAuthority 选择 |

字段删除后使用 `reserved`，禁止复用旧字段号或名称，避免旧客户端把新字段误解成旧语义。

---

## 2. WSS 控制消息

建议新建 `common/control.proto`。

### 2.1 ClientHello

```proto
message ClientHello {
  string connection_ticket = 1;
  reserved 2;
  reserved "protocol_version";
  string client_build = 3;
}
```

冻结为 base64url `string`：

- HTTPS 登录响应和浏览器处理更直接；
- ticket 仍然是 opaque token，禁止记录完整值；
- 不允许将 SessionPermit/FriendPermit 发给 H5。

生成与核销合同：

```text
32字节密码学安全随机数
→ base64url无padding（约43字符）
→ Redis Key只保存SHA256(ticket)
→ TTL默认30秒
→ GETDEL/Lua原子核销一次
```

ticket 绑定 player_id、session_id、session_authority_epoch 和 active_session_epoch；不绑定
IP、User-Agent、特定 farm-gate 实例或候选 Transport Adapter。失败对 H5 统一映射
`ERR_CONNECTION_TICKET_INVALID`。

### 2.2 ServerHello

```proto
message ServerHello {
  string session_id = 1;
  uint64 active_session_epoch = 2;
  uint64 session_authority_epoch = 3;
  int64 server_time_ms = 4;
  uint32 heartbeat_interval_ms = 5; // 待 Direct WebSocket/CLB 探针裁决是否保留
  uint32 max_frame_bytes = 6;
}
```

ServerHello 只表示连接认证成功。客户端必须继续 Join 并应用 FarmSnapshot 后，才能进入可操作状态。

心跳合同尚未冻结。浏览器 JavaScript 不能主动发送原生 WebSocket Ping；必须先实测 Direct
WebSocket 经 CLB 时的 Idle Timeout、应用层 Heartbeat、半开连接和前后台切换行为，再在
“删除并 `reserved` 字段 5”与“定义显式应用层 Heartbeat FarmFrame”之间二选一。候选
TCONND Profile 的传输层心跳单独验证，不得让候选平台行为决定默认协议合同。

### 2.3 ConnectionError

```proto
message ConnectionError {
  ErrorCode error_code = 1;
  string error_message = 2;
}
```

ConnectionError 只用于无法归属某个 request_id 的建连、版本和帧错误，发送后关闭连接。正常命令错误必须使用 CommandResultEnvelope。

---

## 3. FarmFrame 与三大 Envelope

### 3.1 FarmFrame

```proto
enum FrameKind {
  FRAME_KIND_UNSPECIFIED = 0;
  FRAME_KIND_CLIENT_HELLO = 1;
  FRAME_KIND_SERVER_HELLO = 2;
  FRAME_KIND_CLIENT_COMMAND = 3;
  FRAME_KIND_COMMAND_RESULT = 4;
  FRAME_KIND_SERVER_PUSH = 5;
  FRAME_KIND_CONNECTION_ERROR = 6;
}

message FarmFrame {
  uint32 protocol_version = 1;
  FrameKind kind = 2;
  uint32 connection_seq = 3;
  reserved 4, 5, 6, 7;
  reserved "server_seq", "operation_id", "error_code", "timeout_ms";
  bytes body = 15;
}
```

- 每个 WebSocket binary message 恰好承载一个 FarmFrame；
- 不再添加 total_size/meta_size；各 Transport 已提供长度与消息边界；
- connection_seq 在上下行两个方向分别从 1 自增，重连后重新开始；
- P0 不按 connection_seq ACK 或补发，业务连续性仍由 request_id/operation_id/farm_version 负责；
- 客户端本地超时与服务端命令 deadline 分开配置，不保留全局 timeout_hint。

### 3.2 ClientCommandEnvelope

```proto
message ClientCommandEnvelope {
  reserved 1, 3;
  reserved "protocol_version", "client_seq";

  string operation_id = 2;
  CommandType command_type = 4;
  uint64 target_farm_id = 5;
  uint64 expected_farm_version = 6;
  uint64 expected_plot_version = 7;
  string expected_crop_cycle_id = 8;
  string request_id = 10;
  bytes payload = 15;
}
```

- request_id 是本次网络请求尝试的关联 ID，每次重新发起可以生成新的；H5 使用 UUID/ULID 等不含用户数据的 opaque string，长度 1～64；
- operation_id 是跨超时、断线和重试保持不变的业务意图 ID，只对命令合同要求的强写/可靠流程必填；
- target_farm_id 只对农场命令必填；Player 命令从 TrustedCommandContext.player_id 推导路由。farm-gate 可按 CommandRouteDirectory 生成第一跳 ActorAddress/RouteHint，Logic 必须重新推导并复核；
- expected_farm_version 是观察基线，不是所有地块写的全农场 CAS；
- payload 继续使用 bytes，不使用 Any；farm-gate 只解通用 Envelope，Logic 按 command_type 解领域消息。

### 3.3 CommandResultEnvelope

```proto
message CommandResultEnvelope {
  string operation_id = 1;
  CommandStatus status = 2;
  ErrorCode error_code = 3;
  string error_message = 4;
  reserved 5;
  reserved "owner_state_version";
  uint64 farm_version = 6;
  string request_id = 7;
  bytes payload = 15;
}
```

响应必须原样回显 request_id。未使用 operation_id 的运行态命令或查询，响应中的 operation_id 为空。

非农场命令的 farm_version 为 0。Player/Farm 内部 state_version 只进入日志和指标。

### 3.4 request_id / operation_id 命令合同

| 命令类别 | request_id | operation_id | 持久防重来源 |
|---|---:|---:|---|
| Player 普通 Buy/Sell | 必须 | 必须 | Actor 内存 Receipt 防重；与 PlayerEconomyModel 异步同组回写或一起回退 |
| Farm 写、唯一领奖等 P-DURABLE 单权威写 | 必须 | 必须 | OperationResult/Receipt 与本地事实可靠提交 |
| 跨 Actor 可靠流程：播种、收获、偷菜等 | 必须 | 必须 | 入口 OperationResult/Intent/Settlement + 内部 txn Receipt |
| C 类运行态：Join/Leave | 必须 | 不使用 | 当前 connection/activation 内的 JoinContext/Presence |
| 普通只读查询 | 必须 | 不使用 | 无；结果仅对应本次 request_id |
| GET_OPERATION_RESULT | 必须 | Envelope 不使用 | 被查询 operation_id 放入专用 payload |

`request_id` 负责“这一次请求和响应如何对应”，不能防止重复扣款或重复发奖。`operation_id` 负责“一次业务意图只能生效一次”，不能表示帧顺序。

Join/Leave 不创建或归档持久 OperationResult：

- 同一 `connection_id + target_farm_id` 已处于 Joining：返回已有 JoinContext，不重复占位或投递第二份 Snapshot；
- 已处于 InFarm：返回当前订阅/NO_CHANGE；
- Leave 在当前连接已经不在场：返回 NO_CHANGE；
- 连接或 Actor activation 重建后，客户端使用新的 request_id 重新 Join，不查询旧 Join 的 OperationResult。

### 3.5 ServerPushEnvelope

```proto
message ServerPushEnvelope {
  reserved 1;
  reserved "server_seq";

  string subscription_id = 2;
  uint64 actor_activation_epoch = 3;
  uint64 presence_epoch = 4;
  uint64 farm_version = 5;
  PushType push_type = 6;
  bytes payload = 15;
}
```

连接观测序号只存在于 FarmFrame.connection_seq。Snapshot/Change 的业务连续性以 farm_version 为准。

---

## 4. 命令状态、命令与推送枚举

### 4.1 CommandStatus

在既有值后新增：

```proto
enum CommandStatus {
  COMMAND_STATUS_UNSPECIFIED = 0;
  SUCCEEDED = 1;
  PROCESSING = 2;
  NO_CHANGE = 3;
  REJECTED = 4;
  UNAVAILABLE = 5;
  ACCEPTED = 6;
}
```

- `ACCEPTED`：C 类短期运行态已受理，后续由指定 Push 完成，不承诺跨重启恢复；
- `PROCESSING`：W 协调的可靠流程已有持久进度，可按 operation_id 查询和恢复；
- `SUCCEEDED`：命令声明的成功边界已达到；P-ASYNC 表示 PlayerActor 内存经济事务完成，P-DURABLE 表示要求的事实已可靠持久化。

### 4.2 CommandType

旧 `ENSURE_OWNER_INITIALIZED = 1` 删除并 reserved。其余编号不重排：

```proto
enum CommandType {
  COMMAND_TYPE_UNSPECIFIED = 0;
  reserved 1;
  reserved "ENSURE_OWNER_INITIALIZED";

  BUY_OFFER = 2;
  SELL_ITEM = 3;
  PLANT_CROP = 4;
  HARVEST_CROP = 5;
  JOIN_FARM = 6;
  LEAVE_FARM = 7;
  WATER_CROP = 8;
  BEGIN_THEFT = 9;
  GET_FARM_SNAPSHOT = 10;
  GET_OPERATION_RESULT = 11;
}
```

S2 只实现 JOIN_FARM；其他枚举保留，未实现时返回 ERR_UNKNOWN_COMMAND/功能未开放映射，不得假成功。

### 4.3 PushType

```proto
enum PushType {
  PUSH_TYPE_UNSPECIFIED = 0;
  FARM_SNAPSHOT = 1;
  FARM_CHANGE = 2;
  KICK = 3;
  SYSTEM_NOTICE = 4;
  OPERATION_RESULT = 5;
  RESYNC_REQUIRED = 6;
}
```

### 4.4 ErrorCode 补充

```proto
ERR_CONNECTION_TICKET_INVALID = 2005;
```

ticket 不存在、过期、已核销或绑定代次失效都统一映射此码，不向 H5 暴露更细原因。

---

## 5. Farm 外部交互模型

建议新建 `farm/farm_view.proto`，不要直接把持久化 `FarmCore/CropCycle` 原样暴露给 H5。

### 5.1 Join

```proto
message JoinFarmRequest {}

message JoinFarmResult {
  string subscription_id = 1;
  uint64 presence_epoch = 2;
  uint64 snapshot_version = 3;
}
```

target_farm_id 已存在于 ClientCommandEnvelope，不在 payload 重复。

Join 的 ExecuteCommandResponse：

```text
status = ACCEPTED
payload = JoinFarmResult
```

这里 ACCEPTED 只表示：

```text
Joining Presence 已建立
+ Snapshot(V) 已生成
+ Snapshot 已交给有界 DownlinkDispatcher
```

它不表示 Snapshot 已进入 farm-gate ConnectionWriter，不表示 Snapshot 已被浏览器收到，也不表示客户端已经 InFarm。客户端必须保持 Joining 并等待 FARM_SNAPSHOT。

### 5.2 Snapshot

```proto
message FarmSnapshot {
  uint64 farm_id = 1;
  uint64 owner_player_id = 2;
  int64 snapshot_time_ms = 3;
  reserved "server_time_ms";
  repeated PlotView plots = 4;
}

message PlotView {
  uint32 plot_index = 1;
  uint64 plot_version = 2;
}
```

S2 只演示空农场，`PlotView` 暂时只需要地块编号和版本；字段号 3 保留为未来 `CropView crop`，但现在不提前冻结作物字段。进入个人经营 Loop 时再加入 CropView。届时公开护理状态只需要 `bool watered`：同一 CropCycle 全局首次浇水后即为 true，其他玩家不能再次有效浇水，不需要按好友保存公开护理状态。

`farm_version` 位于 ServerPushEnvelope，不在 Snapshot payload 重复。`snapshot_time_ms` 是生成本次快照所用的服务端时间基线；本项目每位 Owner 只有一个私有农场，因此不再下发重复的 `farm_id`。

### 5.3 Change

P0 建议使用“变化后完整地块视图”，而不是向客户端暴露内部事件：

```proto
message FarmChange {
  repeated PlotView changed_plots = 1;
}
```

冻结不变量：

- 每次产生公开农场变化的权威提交只把 `farm_version` 增加 1；
- `changed_plots` 是该次提交完成后的完整地块视图，不是内部事件或字段 patch；
- 同一条 FarmChange 中 `plot_index` 唯一，并按 `plot_index` 升序排列；
- 客户端仅在收到本地版本的下一连续版本时覆盖对应地块；
- 重复/旧版本 → 忽略；
- 出现缺口或某版本投递失败 → 标记订阅失效，停止接收后续增量并重新 Join；禁止静默跳过后继续。

### 5.4 ResyncRequired

```proto
enum ResyncReason {
  RESYNC_REASON_UNSPECIFIED = 0;
  VERSION_GAP = 1;
  JOIN_BUFFER_OVERFLOW = 2;
  DOWNLINK_OVERFLOW = 3;
  ACTOR_REACTIVATED = 4;
  STALE_SUBSCRIPTION = 5;
}

message ResyncRequired {
  ResyncReason reason = 1;
  uint64 latest_known_farm_version = 2;
}
```

---

## 6. farm-gate → Logic

```proto
message TrustedCommandContext {
  uint64 player_id = 1;
  uint64 active_session_epoch = 2;
  bytes session_permit = 3;
  reserved 4;
  reserved "friend_permit";
  string gateway_id = 5;
  string connection_id = 6;
  string trace_id = 7;
  uint64 session_authority_epoch = 8;
  reserved 9;
  reserved "request_id";
}

service LogicService {
  rpc ExecuteCommand(ExecuteCommandRequest)
      returns (ExecuteCommandResponse);
}
```

FriendPermit 由 Logic FriendshipAdmission 获取，不由 farm-gate 填充。

`command.request_id` 是请求关联的唯一真值。farm-gate 校验格式后，将 request_id、action、connection/session 以及适用时的 operation_id 绑定进短时 SessionPermit；Logic 不接受另一份可与 command.request_id 冲突的 request_id。

ActorAddress/RouteHint 是内部路由元数据，不放进 ClientCommandEnvelope，也不由 H5 提交。S2 至少贯穿 `actor_kind/entity_id/routing_version/virtual_shard_id`，Logic 重新计算后才投递；`shard_revision` 在多 Logic 控制面启用时生效。具体合同见 D1.15。

---

## 7. BootstrapService

```proto
message EnsureAccountInitializedRequest {
  uint64 player_id = 1;
  string bootstrap_id = 2;
  reserved 3;
  reserved "initial_rule_version";
  string trace_id = 4;
  string request_id = 5;
}

message AccountReady {
  uint64 player_id = 1;
  uint64 farm_id = 2;
  bool player_created = 3;
  bool farm_created = 4;
  int32 player_schema_version = 5;
  int32 farm_schema_version = 6;
  int32 applied_bootstrap_rule_version = 7;
}

service BootstrapService {
  rpc EnsureAccountInitialized(EnsureAccountInitializedRequest)
      returns (AccountReady);
}
```

调用方不能提交 `initial_rule_version`。Logic ConfigAuthority 选择当前生效规则；AccountBootstrapRecord 持久记录 `player_id/farm_id/bootstrap_id/rule_version` 与 `PLAYER_READY/FARM_READY/COMPLETED`。PlayerCore 和 FarmCore 分别条件创建，崩溃后从已有步骤继续；两者都 Ready 前不返回 AccountReady。

`player_created/farm_created` 只用于内部诊断与测试，不能被 H5 当作发奖依据。AccountReady 不返回初始金币/库存的权威副本；H5 在 FarmSnapshot 或后续 PlayerSnapshot 中获取展示数据。

---

## 8. Logic → farm-gate Downlink

```proto
enum DownlinkStatus {
  DOWNLINK_STATUS_UNSPECIFIED = 0;
  QUEUE_ACCEPTED = 1;
  CONNECTION_NOT_FOUND = 2;
  QUEUE_FULL = 3;
  STALE_SUBSCRIPTION = 4;
  STALE_CONNECTION = 5;
  UNAVAILABLE = 6;
}

message DownlinkItem {
  string item_id = 1;
  string connection_id = 2;
  common.ServerPushEnvelope push = 3;
}

message DeliverBatchRequest {
  string gateway_id = 1;
  string trace_id = 2;
  repeated DownlinkItem items = 3;
}

message DownlinkItemResult {
  string item_id = 1;
  DownlinkStatus status = 2;
}

message DeliverBatchResponse {
  repeated DownlinkItemResult results = 1;
}

service GatewayDownlinkService {
  rpc DeliverBatch(DeliverBatchRequest)
      returns (DeliverBatchResponse);
}
```

必须逐条返回结果，不能只给整个 Batch 一个成功值。`QUEUE_ACCEPTED` 只表示进入 farm-gate 当前 connection generation 的有界 ConnectionWriter，并已建立短时 `item_id` 防重记录。

重试与确认不变量：

- 同一逻辑投递的重试必须保持 `item_id` 不变；新消息必须使用新 item_id；
- farm-gate 在短时间窗内吸收重复 item_id，同一 item 不得进入 ConnectionWriter 两次；
- DeliverBatchResponse 按 `item_id` 关联，不依赖请求与响应数组下标相同；
- RPC 超时、结果缺失或 `UNAVAILABLE` 都是“结果未知/未接受”，不能确认 Join；
- 只有与当前 `subscription_id + connection_id + presence_epoch` 匹配的 Snapshot item 返回 `QUEUE_ACCEPTED`，DownlinkDispatcher 才能投递 ConfirmSnapshotQueued；
- 其他状态不回滚已经提交的业务事实，但必须取消 Joining 或使订阅失效，并按合同重连/重新 Join。

DownlinkItem 不重复 gateway_id/subscription_id：gateway_id 在 Batch，subscription_id/presence_epoch 在 ServerPushEnvelope。

---

## 9. Actor 内部确认

以下是 Logic 进程内 Actor 消息，不需要 Protobuf/gRPC：

```text
ConfirmSnapshotQueued {
    subscription_id
    connection_id
    presence_epoch
    snapshot_version
}

CancelJoining {
    subscription_id
    connection_id
    presence_epoch
    reason
}
```

DownlinkDispatcher 根据 DeliverBatchResponse 转成 Actor 消息。Actor 必须拒绝旧 subscription/connection/presence 的延迟确认。

---

## 10. PlayerMeta / FarmMeta

```proto
message PlayerMeta {
  uint64 player_id = 1;
  uint64 player_epoch = 2;
  uint64 state_version = 3;
  int32 schema_version = 4;
  int32 applied_bootstrap_rule_version = 5;
}

message FarmMeta {
  uint64 farm_id = 1;
  uint64 owner_player_id = 2;
  uint64 farm_epoch = 3;
  uint64 state_version = 4;
  int32 schema_version = 5;
  int32 applied_bootstrap_rule_version = 6;
}
```

S2 单节点不依赖 player_epoch/farm_epoch 完成多节点 fencing，但保留字段。`bootstrap_id` 与双记录创建进度属于 AccountBootstrapRecord；两条 Core 只保存实际采用的规则版本和自身元数据。

---

## 11. S2 暂缓

本轮不定义完整 payload：

- BuyOffer/SellItem；
- Plant/Harvest/Water/Theft；
- Task/Mail/Pet/Album；
- Friendship/Share 外部 API；
- Worker GrantLoot/MarkGranted/CompleteTheft 的新业务字段；
- 成熟提醒、排行榜；
- ACK/任意帧重放；
- Snapshot 通用分片；
- ShardLease/RemoteForward。

现有 WorkerCallbackService 只删除 ExpirePresence；其余接口保留到偷菜阶段再确认。

偷菜阶段的 Settlement Proto 已冻结以下方向，但不在 S2 生成：

- 增加 `BLOCKED_BY_PET` 终态与外部 TheftOutcome；
- 资格拒绝使用终态 OperationResult，不创建 Settlement；
- 一旦创建 Settlement，它就是该 operation_id 的唯一可变结果源；
- TARGET_COMMITTED/GRANTED 映射 PROCESSING，BLOCKED_BY_PET/COMPLETED 映射 SUCCEEDED；
- 不再同步维护一份 PROCESSING→SUCCEEDED 的 OperationResult。

---

## 12. 协议决策状态

### C1：Join 使用 ACCEPTED（已冻结）

新增 `ACCEPTED = 6`，不改变既有枚举值：

```text
JoinFarm 返回 ACCEPTED
→ Actor 已创建 Joining/版本屏障/Buffer
→ Snapshot 已进入本地有界 DownlinkDispatcher
→ 服务端和客户端均尚未 InFarm

farm-gate QueueAccepted
→ 服务端 Joining → InFarm

H5 应用 FarmSnapshot
→ 客户端 Joining → InFarm
```

`ACCEPTED` 表示 C 类运行态受理，不承诺跨重启恢复；`PROCESSING` 只用于 W 协调中已经持久化进度的可靠流程。

### C2：connection_ticket 使用 string（已冻结）

使用 32 字节密码学安全随机值的 base64url string。默认 30 秒 TTL，Redis 只保存 SHA256
Key，任意健康 farm-gate 使用 GETDEL/Lua 原子核销一次。ticket 不进入 URL，不绑定 IP、
指定 farm-gate 实例或候选 Transport Adapter，核销失败后重新登录取新票据。

### C3：Snapshot 使用外部 View（已冻结）

不直接暴露 PlayerCore/FarmCore 持久化结构。S2 使用只含 `farm_id/owner_player_id/snapshot_time_ms/plots` 的 FarmSnapshot 和只含 `plot_index/plot_version` 的空地 PlotView；CropView 延迟到个人经营 Loop 冻结。

### C4：FarmChange 粒度（已冻结）

发送权威提交后的完整 PlotView，不发送 PlantChanged/Watered/TheftChanged 内部事件。每个公开提交 farm_version 只加 1，changed_plots 的 plot_index 唯一且升序；客户端连续覆盖、旧版忽略、缺口重新 Join。

### C5：DeliverBatch 逐条状态（已冻结）

必须逐 item_id 返回 QueueAccepted/失败状态。同一逻辑投递重试保持 item_id，farm-gate 短时防重；只有当前订阅 Snapshot 的对应 item 返回 QueueAccepted 后才能确认 Join，RPC 超时或结果未知均不能确认。

### C6：request_id 与 operation_id 分工（已冻结）

所有 ClientCommandEnvelope 必须携带 request_id，响应原样回显。Player 普通 Buy/Sell 和所有 P-DURABLE/跨 Actor 流程必须另带 operation_id；普通 Buy/Sell 先由 PlayerActor 内存 Receipt 防重并随经济快照异步回写，P-DURABLE/跨 Actor 流程持久防重。Join/Leave 和普通查询不使用 operation_id。Join/Leave 只在当前 connection/activation 内依据 JoinContext/Presence 吸收重复，不写持久 OperationResult。

### C7：FarmFrame 使用单一 Protobuf（已冻结）

不保留 `total_size/meta_size/FrameMeta`。每个 WebSocket binary message 只承载一个：

```text
FarmFrame {
  protocol_version
  kind
  connection_seq
  body
}
```

上下行分别维护 connection_seq；P0 不做 ACK/任意帧重放。ClientHello、ClientCommandEnvelope 和 ServerPushEnvelope 不再保存重复的版本或连接序号。完整合同见 [11-frame-codec](./11-frame-codec.md)。

### C8：PlayerID/FarmID 使用 uint64（已冻结）

`player_id/farm_id/target_farm_id` 的逻辑与 IDL 类型统一为 uint64，0 非法。H5 使用 protobuf Long/BigInt 或十进制字符串，不得转换为 JavaScript Number。外部平台 string OpenID 只在账号映射边界转换为内部 player_id。P0 可令 farm_id 与 player_id 数值相同，但不能共用字段语义或 ActorKey。

### C9：Bootstrap 规则权威（已冻结）

EnsureAccountInitialized 只接收 `player_id + bootstrap_id`。当前生效规则由 Logic ConfigAuthority 选择；AccountBootstrapRecord 协调 PlayerCore 与 FarmCore 的幂等条件创建，两者均 Ready 后才返回。

### C10：心跳边界（待 Direct WebSocket/CLB 探针）

浏览器不能主动发送原生 WebSocket Ping。先实测 Direct WebSocket 经 CLB 时的 Idle
Timeout、应用层 Heartbeat、半开连接和前后台切换；探针完成后在“删除并 reserved
ServerHello 字段 5”与“定义显式应用层 Heartbeat”之间二选一，禁止保留语义不明确的
半套方案。TCONND/gateproxy 若启用，再执行候选 Profile 的独立探针。
