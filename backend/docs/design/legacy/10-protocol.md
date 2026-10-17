# 10 · 协议：控制消息、信封、状态机、错误码

版本：v0.3
状态：S2 目标合同。现有 `api/proto/common/*` 和 `api/gen/common/*` 是 v0.1 可执行基线，进入 S2 时按本文校正并重新生成。

对应交接文档任务一。proto 源:`api/proto/common/{enums,envelope,frame}.proto`,生成代码 `api/gen/common/`。字节帧布局见 D3(`11-frame-codec.md`)。

前提见 [00-overview](./00-overview.md)、[业务事实分类](./06-business-fact-classification.md)、[双 Actor 设计](./16-player-farm-actor-split.md) 与 [routing v2/迁移设计](./17-uc-routing-migration-adaptation.md)。Protobuf 是唯一业务 IDL;命令 payload 按 `command_type` 决定具体消息类型,置于帧 Body 段。

---

## 1. WSS 控制消息与三大信封

WSS 建立后第一条应用消息必须是 `ClientHello`。P0 中它经公网 DNS/CLB/TLS 直接到达
farm-gate，由 farm-gate 核销一次性 `connection_ticket`；认证成功后沿同一连接返回
`ServerHello`。两者只存在于 H5↔farm-gate 的应用连接，不进入 Logic gRPC。候选
TCONND/gateproxy 只替换 Transport Adapter，不改变这些控制消息。

```text
ClientHello {
  connection_ticket
  client_build
}

ServerHello {
  session_id
  session_authority_epoch
  active_session_epoch
  server_time_ms
  heartbeat_interval_ms
  max_frame_bytes
}
```

`ServerHello` 只代表连接认证成功，不代表已经进入农场。

`heartbeat_interval_ms` 当前是 S2-0 待探针字段，不是已冻结合同。浏览器 JavaScript
不能主动发原生 WebSocket Ping；P0 需要通过 Direct WebSocket/CLB 探针确定应用层
Heartbeat、CLB Idle Timeout 和半开连接回收参数。若启用 TCONND 候选 Profile，再单独
验证其传输层心跳，不能把候选平台行为写进默认协议合同。

外层 `FarmFrame.protocol_version` 是协议版本唯一真值；ClientHello 不再重复保存版本字段。无法归属具体 request_id 的建连、版本或帧错误使用 `ConnectionError`，发送后关闭连接。

| 信封 | 方向 | 用途 |
|---|---|---|
| `ClientCommandEnvelope` | C → S | 统一命令，携带请求关联、按命令适用的业务幂等键与乐观版本期望 |
| `CommandResultEnvelope` | S → C | 命令结果,携带状态机与错误码 |
| `ServerPushEnvelope` | S → C | 主动推送(快照/增量/顶号/系统通知) |

### 1.1 ClientCommandEnvelope 字段职责

| 字段 | 作用 |
|---|---|
| `request_id` | **本次网络请求尝试的关联 ID**；每次重新发起可生成新的，响应必须原样回显 |
| `operation_id` | **业务意图幂等键**；只对强写/跨 Actor 可靠流程必填，超时重试必须复用 |
| `command_type` | 命令路由与解码(枚举,见 §2) |
| `target_farm_id` | 农场类命令的目标 Farm；Player 类命令不依赖客户端目标 ID，而从可信会话取得 player_id |
| `expected_farm_version` | 可选的观察基线/缺口诊断；不能作为所有地块写的全农场严格 CAS |
| `expected_plot_version` / `expected_crop_cycle_id` | 地块命令的目标保护，防止旧页面误操作下一茬；不匹配 → `ERR_VERSION_CONFLICT` |
| `payload` | 具体命令参数(序列化后的命令消息) |

客户端**不能**提交权威价格、产量、成熟时间、权限、Permit 或最终奖励——这些一律由目标 Actor 依据冻结的 `rule_version` 裁决。

命令身份合同：

| 命令类别 | request_id | operation_id | 防重与持久化 |
|---|---:|---:|---|
| Player 普通软资产写（Buy/Sell） | 必须 | 必须 | PlayerActor 内存 Receipt 立即防重；Receipt 与 PlayerEconomyModel 异步同组回写或一起回退 |
| 单 Authority 可靠写 | 必须 | 必须 | OperationResult/Receipt 与本地事实 P-DURABLE 提交 |
| 跨 Actor 可靠流程 | 必须 | 必须 | OperationResult/Intent/Settlement + 内部 Receipt 均 P-DURABLE |
| Join/Leave | 必须 | 不使用 | 否，当前 connection/activation 内吸收重复 |
| 普通只读查询 | 必须 | 不使用 | 否 |
| GET_OPERATION_RESULT | 必须 | Envelope 不使用 | 查询目标放在专用 payload |

`request_id` 只能关联一次请求与响应，不能防止重复扣款；`operation_id` 只能标识业务意图，不能表示帧顺序。普通 Buy/Sell 在 Actor 活着时由内存 Receipt 防重；若 Player Pod 在该快照回写前硬崩溃，修改和 Receipt 一起回退，相同 operation_id 可从旧基线重新执行一次，不允许出现“Receipt 保存了但资产没保存”的半状态。

### 1.2 CommandResultEnvelope 字段职责

| 字段 | 作用 |
|---|---|
| `request_id` | 原样回显请求的 request_id |
| `operation_id` | 对应业务意图；未使用 operation_id 的命令返回空值 |
| `status` | 六态状态机(见 §3) |
| `error_code` / `error_message` | 失败原因 |
| `farm_version` | 目标农场当前公开版本 |
| `payload` | 结果数据 |

`OwnerMeta.state_version` 只进入内部 RPC、Actor/Store 指标与日志，不下发 H5，避免客户端协议绑定内部持久化模型。

### 1.3 ServerPushEnvelope 字段职责

区分两个顺序维度是关键：外层 `FarmFrame.connection_seq` 是**某条连接、某个发送方向的传输观测顺序**，`farm_version` 是**农场公开业务变化顺序**。客户端不能用连接序号代替农场版本。

| 字段 | 作用 |
|---|---|
| `subscription_id` | 订阅标识 |
| `actor_activation_epoch` | Actor 激活代次;跳变 → 客户端重新 Join |
| `presence_epoch` | 在场代次 |
| `farm_version` | 该推送对应的农场公开版本 |
| `push_type` | `FARM_SNAPSHOT`/`FARM_CHANGE`/`OPERATION_RESULT`/`RESYNC_REQUIRED`/`KICK`/`SYSTEM_NOTICE` |
| `payload` | 快照或增量数据 |

## 2. 命令类型(P0a 目录)

`CommandType` 枚举与路由键(交接文档 §12):

| 命令 | 枚举 | 路由键 |
|---|---|---|
| 购买 | `BUY_OFFER` | buyer_id |
| 出售 | `SELL_ITEM` | seller_id |
| 播种 | `PLANT_CROP` | `FARM:target_farm_id`，进入 farm-svr/FarmOperationCoordinator；Coordinator 创建 PlantWorkflow 后调用 Player/Farm Participant |
| 收获 | `HARVEST_CROP` | `FARM:target_farm_id`，再进入 Harvest Saga |
| 进入农场 | `JOIN_FARM` | `FARM:target_farm_id` |
| 离开农场 | `LEAVE_FARM` | `FARM:target_farm_id` |
| 浇水 | `WATER_CROP` | `FARM:target_farm_id` |
| 发起偷菜 | `BEGIN_THEFT` | `FARM:target_farm_id` |
| 只读快照 | `GET_FARM_SNAPSHOT` | `FARM:target_farm_id` |
| 结果查询 | `GET_OPERATION_RESULT` | 结果的入口 Actor |

设计取舍:用**枚举而非字符串** cmd,便于压测统计与紧凑编码;新增命令追加枚举值,保持前向兼容。枚举同时驱动服务端 CommandRouteDirectory：Gate 可推导第一跳 ActorAddress，Logic 必须复核，客户端不能提交可信 actor kind/shard。(现网网关用 `service/method` 字符串是为了适配 gRPC generic call 与多服务路由;本项目命令集固定且单一逻辑域,枚举更合适。)

`EnsureAccountInitialized(player_id, bootstrap_id)` 是登录建档内部命令，不允许 H5 直接调用。ConfigAuthority 选择规则版本，AccountBootstrap 协调 PlayerCore/FarmCore 分别条件创建。Plant 首跳到 `FARM:target_farm_id` 只决定 FarmOperationCoordinator 的部署位置；种子仍只能由 PlayerActor 裁决。Coordinator 必须先 `CreateIfAbsent PlantWorkflow`，随后才能调用 `ReservePlant` / `ApplyPlant` / `FinalizePlant`。这些 Participant 命令以及 `CreditHarvest` / `GrantLoot` / `MarkGranted` / `CompleteTheft` 都是可信内部命令。`ExpirePresence` 由 farm-gate 断线提示、FarmActor 惰性过期和 Logic 本地租约清理处理，不属于 Worker 可靠流程。

因此外部客户端命令目录为 10 条；建档和 Worker 回调使用独立内部接口。

## 3. 状态机(六态)

| 状态 | 含义 | 客户端应对 |
|---|---|---|
| `SUCCEEDED` | 命令声明的成功边界已达到：P-ASYNC 命令已完成 Actor 内存原子事务；P-DURABLE 命令已完成要求的可靠提交 | 应用最终结果 |
| `ACCEPTED` | C 类短期运行态已受理，后续由指定 Push 完成；不承诺跨重启恢复 | 保持等待态；超时后重新建立运行态 |
| `PROCESSING` | 意图与可恢复进度已持久化,跨 Actor 流程推进中 | 等待推送/稍后查询 |
| `NO_CHANGE` | 合法但目标已是所需状态(幂等重复) | 视为成功 |
| `REJECTED` | 权限/资源/版本/业务条件不成立 | 读 error_code 处理 |
| `UNAVAILABLE` | 无法确认是否受理 | 强写/可靠流程用同一 operation_id 查询或重试；运行态请求重新建立连接状态 |

`ACCEPTED` 与 `PROCESSING` 的区别：

```text
ACCEPTED
→ 运行态受理，可在故障后丢失
→ 典型：JoinFarm
→ 丢失后重新认证/Join，不查询持久 OperationResult

PROCESSING
→ 持久流程进度已经成立
→ 典型：已 TARGET_COMMITTED 的偷菜
→ 可按 operation_id 查询并在重启后继续推进
```

`SUCCEEDED` 不再隐含“所有命令都已经写完 Tcaplus”。CommandCatalog 必须固定：

```text
BUY_OFFER / SELL_ITEM             → P-ASYNC
PLANT/HARVEST/THEFT 参与步骤      → P-DURABLE
TASK/MAIL CLAIM、付费、转账       → P-DURABLE
FARM 权威写                       → P-DURABLE
JOIN/LEAVE                        → P-RUNTIME
```

任何内部 Coordinator 只能把 P-DURABLE ACK 当作可靠参与者完成，不能因为 PlayerActor 内存已经变化就推进 Workflow 终态。

JoinFarm 返回 ACCEPTED 后，服务端仍是 Joining，H5 也保持 Joining；farm-gate
QueueAccepted 后服务端转 InFarm，H5 收到并应用 FarmSnapshot 后客户端才转 InFarm。
QueueAccepted 只表示消息进入 farm-gate 的 ConnectionWriter，不表示当前 Transport 已写
Socket，更不表示浏览器已经收到。

关键规则:`UNAVAILABLE` ≠ 业务失败。强写/可靠流程超时或结果未知时禁止生成新 operation_id 重扣款或重发奖；Join/Leave 不查询持久结果。

同一个 `operation_id` 命中已有结果时必须返回原结果，不把原 `SUCCEEDED` 改写成 `NO_CHANGE`。`NO_CHANGE` 用于一个新的合法意图发现目标已经处于所需状态，例如另一名玩家浇水时作物已经被浇过。

C 类运行态命令只在当前 connection/activation 内依据 `connection_id + target_farm_id + JoinContext/Presence` 吸收重复，不创建持久 OperationResult；连接已经重建时，使用新的 request_id 重新 Join。

## 4. 连接序号、业务版本与恢复

- `FarmFrame.connection_seq`：上下行各自维护的连接内完整性观测序号；
- `request_id`：一次请求与响应的关联；
- `operation_id`：强写/可靠流程的业务幂等；
- `farm_version`：农场公开业务连续性；
- `ResyncRequired`：服务端明确要求客户端重新 Join。

P0 不实现完整 ACK/任意帧重放。发现 FarmChange 缺口、下行缓冲丢弃或 Actor activation 变化时，优先发送：

```text
ResyncRequired {
  reason
  latest_known_farm_version
}
```

如果该控制消息也无法进入 ConnectionWriter 有界队列，farm-gate 通过当前 Transport
断开连接，客户端重新认证和 Join。

## 5. 错误码表

分段:`1xxx` 协议/帧,`2xxx` 会话/鉴权,`3xxx` 好友/权限,`4xxx` 经济,`5xxx` 农场/种植,`6xxx` 实时/在场,`7xxx` 偷菜/结算,`9xxx` 系统/存储。完整枚举见 `enums.proto`。要点:

- `ERR_STORE_CAS_CONFLICT (9002)`:条件写版本冲突 → 逻辑层应**重试整个 turn**(重读→重验→重构造→重提交),对客户端表现为透明重试或最终 `UNAVAILABLE`。
- `ERR_TIMEOUT (9004)`:结果未知 → 走 `GET_OPERATION_RESULT` 恢复。
- `ERR_VERSION_CONFLICT (5004)`:`expected_*` 与权威不符 → 客户端刷新快照后重试。
- `ERR_ACTOR_REDIRECT (6003)`:目标 Actor 不在本节点 → 逻辑层重路由,通常对客户端透明。
- `ERR_CONNECTION_TICKET_INVALID (2005)`：ticket 不存在、过期、已核销或绑定代次无效；统一断开并重新登录，不向 H5 暴露更细原因。

## 6. FarmFrame 与 Envelope 唯一真值

- FarmFrame 只保存应用帧字段：`protocol_version/kind/connection_seq/body`；
- ClientCommandEnvelope 保存业务请求字段：`request_id/operation_id(按命令适用)/command_type/target_farm_id/expected_*`；
- CommandResultEnvelope 保存业务结果字段：`request_id/operation_id(按命令适用)/status/error_code/farm_version/payload`；
- 不增加 `total_size/meta_size` 自定义头；P0 的 WebSocket binary message 已经提供长度和
  消息边界，候选 TCONND/gateproxy Adapter 也必须无损保持这一边界；
- 不在 FarmFrame 和 Envelope 中保存两份可能矛盾的 `operation_id/error_code/farm_version`；
- timeout_hint 不属于全局帧合同；客户端页面超时与服务端命令 deadline 分别配置；
- farm-gate 解 Envelope 的通用路由字段，但不理解领域 payload。

## 7. 验收

- 按 v0.2 更新 Proto 后，`scripts/gen-proto.sh` 可重复生成且 `go build ./...` 通过；
- ClientHello 票据只能核销一次，未认证连接不能发送业务命令；
- `internal/protocoltest/envelope_test.go`：控制消息、三大信封与 FarmFrame 序列化往返、状态机与错误码枚举值稳定；
- `owner_state_version` 不出现在外部结果；
- FarmFrame 与 Envelope 无重复业务真值；
- `go test` 通过。
