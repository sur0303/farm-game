# 40 · 接口清单（TCONND/gateproxy / farm-gate / Logic / Worker）

内部 RPC 使用 Protobuf IDL + gRPC-Go。本文只冻结职责与语义，当前 `api/proto/logicrpc` 仍是旧可执行基线；目标接口应与 [双 Actor 设计](./16-player-farm-actor-split.md) 和 [routing v2/迁移设计](./17-uc-routing-migration-adaptation.md) 一致后再生成代码。

---

## 1. 进程职责

| 进程 | 状态 | 职责 |
|---|---|---|
| TCONND | 连接有状态、业务无权威 | BY_WS/WSS、Socket、网络流控、TFRAMEHEAD |
| gateproxy | 连接适配有状态、业务无权威 | TCONNAPI/TBUS 与 Unix gRPC GateStream 转换 |
| farm-gate | 应用连接有状态、业务无权威 | FarmFrame、认证、Session、限流、ConnectionWriter 与下行背压 |
| farm-logic | 无状态入口 + 有状态 Actor | Admission、ActorRouteAdapter、PlayerActor、FarmActor、同步协调器 |
| farm-worker | 无状态执行器 | PendingWork 发现、跨 Actor 流程恢复、重试、投影、通知与归档 |

Actor 拆分不新增 `player-service` 或 `farm-service` 进程。两类 Actor 先共存于 farm-logic，后续是否拆进程由压测决定。

## 2. 公网/客户端接口

### HTTPS Login

```text
POST /api/v1/login
→ BootstrapService.EnsureAccountInitialized
→ 返回 player_id、farm_id、connection_ticket
```

只有 PlayerCore 与 FarmCore 都完成幂等建档后才返回 AccountReady。H5 不能提交初始金币、种子或规则版本。

### FarmFrame 长连接

```text
H5
→ TCONND(BY_WS/WSS)
→ gateproxy GateStream
→ farm-gate
→ LogicService.ExecuteCommand
```

一个 WebSocket binary message 对应一个 FarmFrame。farm-gate 认证后构造 TrustedCommandContext；客户端不能伪造 player_id、SessionPermit、connection_id 或 Actor 路由内部字段。

## 3. LogicService（farm-gate → farm-logic）

```proto
rpc ExecuteCommand(ExecuteCommandRequest) returns (ExecuteCommandResponse);
```

入口步骤：

1. 解码并校验 command；
2. 校验 TrustedCommandContext 与 SessionPermit；
3. 按 CommandRouteDirectory 复核 farm-gate 推导的第一跳 `ActorAddress`；
4. 需要好友权限时向 FriendshipAuthority 获取/校验 FriendPermit；
5. 调用 ActorRouteAdapter，本地投递或远程转发；
6. 映射 Actor 结果为 CommandResult。

客户端命令的第一路由：

| 命令 | ActorKey |
|---|---|
| Buy、Sell、TaskClaim、MailClaim | `PLAYER:authenticated_player_id` |
| JoinFarm、LeaveFarm、GetFarmSnapshot | `FARM:target_farm_id` |
| Water、RemovePest、Steal | `FARM:target_farm_id` |
| Plant | `PLAYER:authenticated_player_id`，首次接受后生成跨 Actor PlantIntent |
| Harvest | `FARM:target_farm_id`，首次接受后生成跨 Actor HarvestSettlement |
| QueryOperationResult | 由 command type/operation scope 路由到入口 Actor |

客户端不直接指定 `actor_kind` 或 virtual shard；farm-gate 只根据已认证玩家与命令 payload 推导第一跳 ActorAddress，Logic 必须用同一命令目录重新推导并复核，防止把农场命令误投到 PlayerActor。

## 4. BootstrapService（farm-gate HTTPS Login → farm-logic）

```proto
rpc EnsureAccountInitialized(EnsureAccountInitializedRequest)
    returns (AccountReady);
```

请求只携带可信 `player_id + bootstrap_id`。Logic ConfigAuthority 选择当前规则版本。

服务端维护 AccountBootstrapRecord：

```text
CREATED -> PLAYER_READY -> FARM_READY -> COMPLETED
```

- 条件创建 PlayerCore，初始资产只发一次；
- 条件创建 FarmCore，初始地块只建一次；
- 任一步崩溃后重复调用继续，而不是重新发资产；
- P0 可令 `farm_id == player_id`，响应仍分别返回；
- 两条记录都 Ready 前不能返回 AccountReady。

## 5. ActorRouteAdapter

```text
Route(ActorKey{kind, entity_id})
→ routing v2: xxhash64(kind_byte || uint64_be(entity_id)) % 4096
→ shard_table[(kind, virtual_shard)]
→ local ActorRegistry 或 remote Logic
```

S2 单 Logic时，PlayerRegion/FarmRegion 各 4096 个 virtual shard 静态映射到本节点；保留 Remote Forward 接口但不执行。多 Logic 优先接入批准版本的 xrpc/proxyless ShardedService；不可用时使用 LogicIngress Forward。两条路径共享 `ActorAddress + routing_version + shard_revision`，并以 ShardLease/Redirect/writer_epoch 完成迁移 fencing。

类型安全封装：

```text
PlayerRouteAdapter.Route(player_id)
FarmRouteAdapter.Route(farm_id)
```

内部路由元数据至少包含：

```text
actor_kind
entity_id
routing_version
virtual_shard_id
shard_revision
hop_count
```

`ROUTE_STALE/ACTOR_REDIRECT` 保留原 request/operation id，刷新 Assignment 后最多自动重投一次；`WRITER_EPOCH_STALE` 必须停用旧 Actor，不能作为普通 CAS 冲突重试。

## 6. Actor 内部命令

### PlayerActor

客户端/入口命令：

- `BuyOffer`
- `SellItem`
- `ReservePlant`
- `TaskClaim`
- `MailClaim`

可信跨 Actor/Worker 命令：

- `FinalizePlant(plant_txn_id, outcome)`
- `CreditHarvest(harvest_txn_id, reward)`
- `GrantLoot(settlement_id, loot)`
- `GrantCareReward(care_txn_id, reward)`

这些回调必须携带服务端生成的 txn/settlement id，由 PlayerCore Receipt 吸收重复。

### FarmActor

客户端/入口命令：

- `JoinFarm`
- `LeaveFarm`
- `HarvestCrop`
- `WaterCrop`
- `RemovePest`
- `BeginTheft`

可信跨 Actor/Worker 命令：

- `ApplyPlant(plant_txn_id, player_id, farm_id, plot_id, expected_plot_version, frozen_rule)`
- `MarkPlantOutcomeDelivered(plant_txn_id)`
- `MarkHarvestGranted(harvest_txn_id)`
- `MarkTheftGranted(settlement_id)`
- `CompleteTheft(settlement_id)`

FarmActor 是 Plot/CropCycle 的唯一裁决者；PlayerActor 是 Wallet/Inventory 的唯一裁决者。

## 7. WorkerCallbackService（farm-worker → Logic/Actor）

Worker 不直接写 PlayerCore/FarmCore，只通过可信内部接口重投：

```proto
rpc ApplyPlant(...);
rpc FinalizePlant(...);
rpc CreditHarvest(...);
rpc GrantLoot(...);
rpc GrantCareReward(...);
rpc MarkGranted(...);
rpc CompleteWorkflow(...);
```

实现可保留一个通用 `ExecuteInternalCommand` RPC，再按内部 command type 分发；对外语义必须保持：

- mTLS/服务身份或等价可信来源；
- 内部 txn id 必填；
- 相同 txn + 相同 payload 幂等；
- 相同 txn + 不同 payload 拒绝并告警；
- 暂时失败可重试，永久业务拒绝返回稳定终态。

## 8. GatewayDownlinkService（Logic → farm-gate）

```proto
rpc DeliverBatch(DeliverBatchRequest) returns (DeliverBatchResponse);
```

每个 item 返回：

- `QUEUE_ACCEPTED`
- `CONNECTION_NOT_FOUND`
- `QUEUE_FULL`
- `STALE_SUBSCRIPTION`
- `STALE_CONNECTION`
- `UNAVAILABLE`

`QUEUE_ACCEPTED` 只表示进入当前 connection generation 的有界发送队列，不表示浏览器已收到。失败不回滚已提交业务，但 FarmChange 出现缺口后必须使订阅失效。

## 9. Join 与 Presence

Join 完全由 FarmActor 处理：

1. 创建 Joining、subscription_id、snapshot_version；
2. 开始缓存 `version > snapshot_version` 的 FarmChange；
3. Snapshot 进入 DownlinkDispatcher；
4. farm-gate QueueAccepted 后，以新消息回到 FarmActor；
5. Catch-up 整批进入同订阅 FIFO 后，Joining → InFarm。

FarmActor 不在一个 turn 中等待 Gateway RPC。Presence 是 activation 运行态，重启后通过重新 Join 重建。

## 10. PlantCoordinator

`FarmOperationCoordinator` 共置于 farm-svr，是 Plant 的无状态正常路径编排模块，不是第三个权威 Actor。Plant 按 `FARM:farm_id` 首跳；Coordinator 必须先可靠创建或读取 Workflow，不能先写 Reservation 再补 Workflow：

```text
CreateIfAbsent PlantWorkflow
→ PlayerActor.ReservePlant
→ FarmActor.ApplyPlant
→ PlayerActor.FinalizePlant
→ Complete PlantWorkflow
```

种子属于 PlayerActor，只约束 `ReservePlant` 的权威归属，不决定 Coordinator 的部署位置。Gate 只按 CommandRouteDirectory 选择 farm-svr 首跳，不推进 Participant。正常路径可在请求 deadline 内同步推进并返回 SUCCEEDED；超时后返回 PROCESSING，由 farm-worker Claim 原 Workflow、读取 Participant Receipt 后继续或释放 Reservation。任何 Actor mailbox 都不能等待另一 Actor 的网络调用。

## 11. S2 目标接口变更

相对旧 Proto，S2 冻结时至少需要：

- `target_owner_id` 迁移为农场命令的 `target_farm_id`；
- 登录返回 `player_id + farm_id`；
- OwnerRouteAdapter 改为 ActorRouteAdapter；
- 增加 routing v2 六个跨语言黄金向量、双 Region 静态 Assignment 和 CommandRouteDirectory 复核；
- PlayerActor/FarmActor 使用不同 Store 与 ActorKey；
- Bootstrap 从一次 OwnerCore 创建改为 AccountBootstrap 双记录流程；
- Plant 内部命令和 Receipt 字段可在 S3 Proto 落地，但 S2 目录与扩展点先保留；
- 旧 `ownercore.proto` 标记废弃并拆成 playercore/farmcore。
