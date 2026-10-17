# 14 · S2 纵向系统骨架详细实施计划

> 2026-07-29 Transport 增量冻结：S2 默认先完成
> `H5 → 公网 DNS → CLB/TLS → farm-gate Direct WebSocket → Logic` 的真实纵向链路。
> TCONND + gateproxy 工作包降级为不阻塞 P0 的平台候选探针；通过后只增加一个
> Transport Adapter，不改变协议、Session、Actor 路由和业务实现。详细合同见
> [21-direct-websocket-tlog-freeze](./21-direct-websocket-tlog-freeze.md)。

版本：v0.3

更新时间：2026-07-27

状态：执行中；当前阶段为 S2-0  

本文是 S2 的唯一实施主计划。设计合同以 `00/05/07/09/10/11/16/17/40/41` 为准，阶段准入以 [08-s2-go-no-go](./08-s2-go-no-go.md) 为准。2026-07-27 起采用 PlayerActor/FarmActor 双边界与 routing v2；旧单 PlayerFarmActor/OwnerCore 和含义曾漂移的草稿 routing v1 不再作为实施依据。

现有 H5 客户端位于独立工作区 `supernova-study/supernova/h5`。它已经具备 Vite/TypeScript、AuthProvider、BinaryTransport、WsClient、FarmProtocolAdapter、FarmStore、Mock Server、Vitest 和 Playwright 骨架。S2 复用并改造这套客户端，不在 `farm-game` 内新建第二套 H5。

---

## 1. S2 的目标与边界

S2 只完成一条最小但真实的纵向链路：

```text
H5
→ HTTPS TestLogin
→ EnsureAccountInitialized（幂等创建 PlayerCore + FarmCore）
→ connection_ticket
→ 公网 DNS
→ CLB / TLS
→ farm-gate Direct WebSocket
→ Gate Core
→ ClientHello / ServerHello
→ LogicIngress
→ CommandRouteDirectory / ActorRouteAdapter(actor_kind, entity_id, routing v2)
→ PlayerActor / FarmActor 按需激活
→ PlayerStore Load/Create/SaveIncremental/ForceFlush
→ FarmStore Load/Create/CAS
→ JoinFarm(self)
→ FarmSnapshot
→ 断线后重新认证并重新 Join
```

S2 证明以下地基成立：

- 公网连接与应用 Session 分层；
- FarmFrame、Envelope 和内部 RPC 合同可执行；
- `PLAYER:player_id` / `FARM:farm_id` 路由与同 ActorKey 串行；
- routing v2 跨语言黄金向量与 Player/Farm 独立 Assignment；
- 两类 Actor 可独立加载、卸载后恢复；
- 双记录建档在中途崩溃后可继续，不重复发初始资产或地块；
- Farm 和可靠 Player 步骤以 Tcaplus 条件写为成功点；S2 同时搭出 Player DirtyTracker/Writeback 的最小运行时骨架；
- Snapshot 与增量之间不存在版本空洞；
- 慢连接和网络 RPC 不阻塞 Actor 邮箱；
- 连接断开后可重新认证并收敛到权威 Snapshot。

S2 明确不实现：

- Buy、Sell、Plant、Harvest、Water、Steal；
- 好友列表、任务、邮件、宠物、图鉴、排行榜、成熟提醒；
- farm-worker、Plant/Harvest/Theft 等跨 Actor Settlement/Receipt（S2 只保留扩展点）；
- 多 Logic ShardLease、Actor 迁移、Proxyless Mesh；
- 任意下行 ACK/重放、复杂 Snapshot 分片；
- 生产级自动扩缩容和跨机房容灾。

---

## 2. 实施原则

1. **纵向优先**：一次只打通一条可测试链路，不同时铺开多个半成品模块。
2. **阶段门控制**：未满足 D1.8 的退出条件，不进入下一子阶段。
3. **接口先隔离平台**：Direct WebSocket Transport、候选 TCONND Adapter 和 TcaplusDB 均通过窄接口接入；Fake/Memory 实现必须模拟真实失败，不能总返回成功。
4. **持久化策略显式**：S2 Bootstrap 与 Farm 写按 P-DURABLE；P0b 普通 Player 软资产允许 Actor 内存提交后异步回写。跨 Actor 终态必须满足对应 Intent/Receipt 的 P-DURABLE 合同。
5. **Actor 不等待网络**：Actor turn 只产生状态和投递意图；下行由有界 Dispatcher/ConnectionWriter 异步推进。
6. **证据优先**：平台返回语义、心跳、帧边界和 CAS 延迟由探针结论决定，不凭 UC 经验或文档猜测。
7. **最小协议**：S2 Proto 只覆盖 Login/Hello/Join/空 Snapshot/Resync/Downlink；后续业务 payload 到对应纵切再补。
8. **可观测即验收**：每条链路必须携带关联字段并有指标、日志和自动化测试，否则不算完成。

---

## 3. 阶段总览

| 阶段 | 目标 | 关键产物 | 退出条件 |
|---|---|---|---|
| S2-0 | 合同收口与平台探针 | H5 uint64、Direct WebSocket/CLB、Tcaplus 探针报告和最终最小 Proto；TCONND 是并行候选报告 | D1.8 的 S2-0→S2-1 条件全部满足 |
| S2-1 | 进程通信与协议骨架 | 生成后的 v0.2 Proto、FakeTransport、Gate→Logic 空命令链路 | 构建/协议/超时/错误映射测试通过 |
| S2-2 | 建档、路由、Actor 与 Store | AccountBootstrap、ActorRouteAdapter、双 ActorRegistry、Memory/Tcaplus PlayerStore/FarmStore | 双记录建档、重复建档、Load/CAS、独立卸载恢复通过 |
| S2-3 | Session、Join、Snapshot 与重连 | Ticket、PRE_AUTH、ClientHello、JoiningBuffer、DownlinkDispatcher、ConnectionWriter、H5 空农场 | 真实 DNS/CLB/farm-gate Direct WebSocket + Tcaplus 下登录、Join、无空洞和重连通过 |

完成 S2-3 后进入 S3a Buy，再进入 S3b Plant Saga 与最小 Worker，最后 S3c `mature_at → Harvest Saga`。

---

## 4. S2-0：合同与真实基础设施探针

### 4.1 工作包 S20-A：仓库与执行基线

目标：所有参与者从同一份计划和准入表工作。

任务：

- [ ] 将本文加入设计索引和根 README。
- [ ] 将 D1.15 的 routing v2 六个黄金向量加入 Go/TypeScript 共用测试数据；S2 不实现 Coordinator/Proxyless。
- [ ] 冻结 CAS 冲突最小策略，写入 D1.8 与 D6。
- [ ] 确认现有 `api/proto`/`api/gen` 是 v0.1 可执行基线，不在探针前修改。
- [ ] 建立 S2 任务编号、结果记录和 Go/No-Go 检查方式。
- [ ] 确认真实凭据只保存在 `deploy/secrets/*.local`，提交前扫描敏感信息和构建产物。

交付物：

- 本文；
- 更新后的 D1.8/D6/README；
- 工蜂分支上的可追踪提交。

验收：

- 新 Agent 只阅读 README、D1、D1.8 和本文即可知道当前状态与下一步；
- 文档不存在“先改最终 Proto”与“先做探针”的冲突。

### 4.2 工作包 S20-B：H5/Go 数值与 FarmFrame 往返

目标：证明浏览器不会丢失 64 位 ID/版本，并确认一条 WebSocket binary message 对应一个 FarmFrame。

复用目录：

```text
supernova-study/supernova/h5/
├── src/protocol/
│   ├── protocol-adapter.ts       # 保留线协议替换缝
│   ├── generated/                # S2-0 通过后放服务端 Proto 生成的 TS 代码
│   ├── canonical-v02-adapter.ts  # 单一 FarmFrame 的正式适配器
│   ├── uint64.ts                 # Uint64String/BigInt 比较与加一
│   └── mock/                     # 现有手写 codec/proto 只供 Mock
└── src/test/
    ├── uint64-roundtrip.test.ts
    ├── canonical-v02-golden.test.ts
    └── join-state-machine.test.ts

farm-game/
└── internal/protocoltest/
    ├── golden_test.go
    └── testdata/                 # Go/TypeScript 共用 golden vectors
```

任务：

- [ ] 以现有 `FarmProtocolAdapter` 为替换边界，保留 WsClient/FarmStore/Mock 回归，不重写页面和状态层。
- [ ] 选定 H5 Protobuf 生成器与运行库；版本和生成命令写入仓库，禁止手写第二份 canonical 字段定义。
- [ ] 服务端 `api/proto` 是唯一 IDL；生成的 TypeScript 代码或版本化制品进入 H5，H5 的临时 `src/protocol/farm.proto` 不得成为服务端输入。
- [ ] 引入 `Uint64String`（十进制规范字符串）或生成库 Long/BigInt；`player_id/farm_id/target_farm_id/farm_version/plot_version/session epoch` 不再使用业务层 `number`。
- [ ] 使用边界值 `0`、`1`、`2^53-1`、`2^53`、`2^63-1`、`2^64-1` 做 encode→decode→encode。
- [ ] FarmStore 的版本连续性由 uint64 helper 判断，不能继续使用 `number + 1`。
- [ ] 验证空 body、未知 `msg_type`、超大 frame、重复/回退 `connection_seq` 的拒绝语义。
- [ ] 增加 `canonical-v02`/`canonical-ready` 构建档位；在正式 Proto 未冻结前，staging/web/wechat 继续 fail-closed，不能发送 Mock 帧。
- [ ] Join 请求生成 `request_id`，但不使用持久 OperationResult；收到 `ACCEPTED` 后保持 `Joining`，只有应用 Snapshot 后才进入 `InFarm`。
- [ ] ServerHello 对齐 `player_id/active_session_epoch/session_authority_epoch/server_time_ms/max_frame_bytes`；
      Heartbeat 字段等待 Direct WebSocket/CLB 探针，T8 只影响候选 TCONND Profile。
- [ ] HTTP 登录 JSON 的 `player_id` 和 epoch 使用十进制字符串；snake_case 为正式合同，camelCase 仅保留 Mock 兼容。
- [ ] 记录浏览器、Protobuf 库版本、结果和最终选型。

验收：

- 所有合法 `uint64` 往返字节一致；
- 非法零 OwnerID 在业务校验层被拒绝；
- 超限帧在反序列化业务 body 前被拒绝；
- Go 与 TypeScript 对同一 golden vector 产生完全一致的字节；
- staging/web/wechat 构建不包含 FakeFarmServer、MockAuth 和手写 Mock codec；
- Join `ACCEPTED → Snapshot → InFarm` 与版本缺口重新 Join 的测试通过；
- 测试可在本地重复执行。

### 4.3 工作包 S20-C：TCONND + gateproxy 最小探针

目标：用真实 Linux/TCONND/gateproxy 环境回答 D1.7 的 T1～T8，并决定 gateproxy 能否直接承接 BY_WS。详细操作、代码边界和回退门见 [15-tconnd-gateproxy-integration-plan](./15-tconnd-gateproxy-integration-plan.md)。

前置输入：

- TCONND 镜像/发布包及 digest；
- gateproxy 兼容镜像 tag 与固定版本 Go client；
- 专用 Linux 服务器、端口、证书与配置权限；
- BY_WS 的最小配置示例。

建议产物：

```text
cmd/tconnd-probe/
deploy/tconnd-probe/
docs/verification/tconnd-probe-report.md
```

执行顺序：

1. T7：先证明发布包、TBUS、证书和权限可以启动。
2. T1：H5 发送不同尺寸的 binary message，记录 GateStream 实际出现普通还是 WEB 命令及消息边界。
3. T2/T3：正常发送、连接不存在、主动断线、慢读、队列满、TCONND/gateproxy 停止，记录 GateStream/TCONNAPI 的可见结果。
4. T8：静默连接、浏览器前后台切换、断网半开，记录 Ping/Pong 和空闲超时。
5. T4：连续发送带序号的上下行消息，验证同 connection generation 的顺序。
6. T5：逐档增加 frame 大小，得到 WSS/TCONND/TBUS/gateproxy/gRPC/farm-gate 的共同上限。
7. T6：容器重启、强杀、TBUS 与 Unix Socket 清理，验证 Ready/NotReady。

输出必须包含：

- T1～T8 每项的环境、步骤、原始结果和结论；
- GateStream 可见结果与本地 `QUEUE_ACCEPTED/CONNECTION_NOT_FOUND/QUEUE_FULL/STALE_CONNECTION/UNAVAILABLE` 的边界；没有逐条 ACK 时不得补造平台确认；
- 应用层 Heartbeat 是否需要，以及最终字段决定；
- 推荐 frame 上限和 Access Ready 条件。

### 4.4 工作包 S20-D：TcaplusDB 最小探针

目标：证明 PlayerStore/FarmStore 所依赖的 Create/Get、Farm CAS、Player 增量 HMSet/FieldUpdate、fencing 和延迟语义。

前置输入：

- 正确的 App/Zone；
- 按 `cmd/tcaplus-verify/verifypb/probe.proto` 创建的 `owner_probe` 表；
- 本地 `deploy/secrets/tcaplus.conf.local`。

执行：

- [ ] 条件 Insert：相同 owner 并发创建只能一次成功。
- [ ] Get：读回值与 RecordVersion。
- [ ] CAS 成功：正确版本写成功且版本自增。
- [ ] CAS 冲突：旧版本写被明确拒绝。
- [ ] PlayerEconomyModel 多字段增量写：验证全成/全败；不能保证时验证单物理字段回退。
- [ ] Player 异步写仍校验 writer_epoch/captured_version，旧 Holder 和旧快照不能覆盖新值。
- [ ] 结果未知：客户端超时/断连后通过 Get 与 operation marker 判定结果。
- [ ] 延迟：Player 增量 1/2/4/8KB、Farm CAS 4/8/16KB 与大记录对照档分别测 P50/P95/P99、超时率。
- [ ] 故障：允许时做切主/短时不可用测试；不能执行则明确标记未验证。

不阻塞 S2 的内容：

- PendingWorkDiscovery；
- Outbox 扫描/变更流；
- 成熟提醒和排行榜投影。

这些在 S5/S6 前完成，不作为 S2-0 的 Proto 阻塞项。

### 4.5 工作包 S20-E：最小 Proto 最终评审

只有 S20-B 和 Direct WebSocket 心跳/Idle Timeout 探针有结论后才执行；TCONND T8 只影响
候选 Adapter，不阻塞默认协议冻结。

任务：

- [ ] 按 D1.9 同步 C1～C10。
- [ ] 检查所有字段号、删除字段的 `reserved`、错误码和枚举兼容。
- [ ] 删除 FrameMeta/自定义长度头等重复真值。
- [ ] 冻结 Heartbeat：依赖传输层，或定义完整应用层消息；禁止保留半套字段。
- [ ] 范围只包含 ClientHello/ServerHello、CommandEnvelope/Result、JoinFarm、FarmSnapshot/FarmChange、ServerPush、Resync 和内部最小 RPC。
- [ ] 评审通过后一次性修改 `api/proto`，重新生成 `api/gen`。

S2-0 Definition of Done：

- H5 uint64 round-trip 通过；
- 现有 H5 完成 canonical adapter 接入准备，Mock 与真实协议边界保持隔离；
- Direct WebSocket 心跳、PRE_AUTH、帧边界和关闭语义已写入协议；
- Proto 兼容检查通过；
- Tcaplus 至少具备可执行的真实表和 CAS 探针；若平台创建仍在等待，允许进入纯协议 S2-1，但不能宣称 S2 Store 验收完成；
- D1.8 明确将状态从 S2-0 切换到 S2-1。

---

## 5. S2-1：协议与进程通信骨架

目标：让一个没有业务副作用的请求贯穿 H5/FakeClient → Gate → Logic → Gate。

### 5.1 目标目录

```text
supernova-study/supernova/h5/
└── src/protocol/              # 复用既有 H5，通过 canonical adapter 接入

farm-game/
cmd/
├── farm-gate/
└── farm-logic/

internal/
├── gateway/
│   ├── transport/
│   ├── protocol/
│   ├── session/
│   ├── backend/
│   ├── command/
│   └── downlink/
└── logic/
    ├── ingress/
    └── routing/
```

S2-1 不创建 `farm-worker` 实现。

H5 与 farm-game 可以继续是两个仓库/工作区，但必须通过以下制品对齐：

- 固定的 `farm-game` Proto commit/tag；
- 固定版本的 TypeScript/Go 生成器；
- 生成后的 TypeScript canonical adapter；
- Go/TypeScript 共用 golden vectors；
- 一份版本化的 HTTPS/WSS 环境合同。

### 5.2 最小请求链

```text
FakeTransport.Data
→ farm-gate Decode FarmFrame
→ 校验 protocol_version / msg_type / frame limit
→ 解析 ClientCommandEnvelope
→ 建立 TrustedCommandContext
→ LogicService.ExecuteCommand
→ 返回 CommandResultEnvelope
→ FarmFrame
→ FakeTransport.Send
```

贯穿字段：

- `request_id`；
- `connection_id + transport_generation`；
- `player_id`；
- `session_authority_epoch + active_session_epoch`；
- `target_farm_id`（Join）以及 Logic 内部推导的 ActorKey；
- `routing_version=2` 以及由服务端命令目录推导、Logic 复核的第一跳 ActorAddress；
- `deadline_unix_ms`；
- 日志 `trace_id`（仅观测，可由 request_id 派生或独立生成）。

### 5.3 测试

- FarmFrame golden/round-trip；
- 未知协议版本、未知消息类型、超大帧；
- deadline 已过和 Logic 超时；
- request_id 缺失/超长；
- gRPC 错误到外部错误码映射；
- FakeTransport QueueFull/Disconnected；
- BackendPool 慢调用/满载不占用 Connection/Session Lane；
- Gate 不读取或修改 PlayerCore/FarmCore。

S2-1 Definition of Done：

- v0.2 Proto 与生成代码构建通过；
- 单测覆盖协议边界和错误映射；
- 空命令端到端通过；
- 所有日志能用 request_id 串起 Gate 与 Logic；
- 无真实业务写、无 Actor、无 Tcaplus 依赖。

---

## 6. S2-2：Bootstrap、Actor 路由、双 Actor 与双 Store

目标：首次登录幂等创建 PlayerCore 与 FarmCore；后续按不同 ActorKey 串行读取，并能分别卸载和恢复。

### 6.1 实现顺序

1. `PlayerStore` / `FarmStore` 接口与共同错误分类；
2. `MemoryPlayerStore` / `MemoryFarmStore`，真实模拟条件创建、增量保存、CAS 冲突、超时和结果未知；
3. `BootstrapService.EnsureAccountInitialized` 与 `AccountBootstrapStore`；
4. `CommandRouteDirectory`、`ActorRouteAdapter`、ActorKind、routing v2 与六个黄金向量；
5. PlayerRegion/FarmRegion 两份静态 Assignment：各自 4096 Shard 全部指向 `logic-local-1`；
6. `PlayerActorRegistry` / `FarmActorRegistry` 与两类 Actor 激活；
7. 空 `PlayerCoreState`、`FarmCoreState`、Player `PersistenceRuntime(version/savedVersion/modifiedVersion)` 与 Farm `ActivationRuntimeState`；
8. `TcaplusPlayerStore` / `TcaplusFarmStore`；
9. Player 单 in-flight save、UniqueWritebackQueue/ForceFlush 最小骨架；
10. 两类 Actor 独立空闲卸载与重新 Load；Player 卸载前 Flush。

### 6.2 PlayerStore/FarmStore 最小合同

```text
Get(entity_id)
InsertIfAbsent(entity_id, initial_record)

PlayerStore.SaveIncremental(
  entity_id, expected_writer_epoch, captured_version, changed_models
)
PlayerStore.ForceFlush(...)

FarmStore.CompareAndSwap(
  entity_id, expected_record_version, expected_actor_epoch, next_record
)
```

Player 的跨 Actor 参与步骤仍需要条件可靠写；可由 `SaveDurableEconomyTxn` 或对当前 Dirty 快照执行优先 ForceFlush 实现，但 ACK 必须发生在存储确认之后。S2 暂无 Buy/Plant，先冻结接口、版本回调和 Fake 行为，P0b 再接入真实经济命令。

最少区分：

- `NOT_FOUND`；
- `ALREADY_EXISTS`；
- `VERSION_CONFLICT`；
- `ACTOR_EPOCH_STALE`；
- `TIMEOUT_OR_UNKNOWN`；
- `UNAVAILABLE`。

### 6.3 CAS 有界重试合同

一次权威命令最多三次 CAS 尝试（第一次 + 最多两次重试），同时受 RPC deadline 限制：

```text
Load current
→ 先查 operation_id
→ 重新校验业务条件
→ 构造 next
→ CAS
```

- `VERSION_CONFLICT`：重新 Load，从幂等检查开始重跑整个 turn；不得只重发旧 next-state。
- `OWNER_EPOCH_STALE`：Actor 立即失去可写权威，停止接收新写、使激活失效并返回 `UNAVAILABLE`；不得作为普通 CAS 冲突重试。
- `TIMEOUT_OR_UNKNOWN`：不替换内存、不返回成功；通过 Get + operation_id 判断是否已经提交。
- 三次仍冲突：返回 `UNAVAILABLE` 并记录冲突指标；同 ActorKey 已串行，连续冲突应被视为双写者或路由错误信号。
- S2 单 Logic 暂不实现完整多节点 epoch 认领，但 Store API、Fake 测试和错误分支必须保留。

### 6.4 测试

- 100 个并发 Bootstrap 只生成一份初始资产；
- 相同 `bootstrap_id` 重试返回同一 `player_id + farm_id`，并从中断状态继续；
- PlayerCore 已创建、FarmCore 未创建，以及反向不完整状态都能恢复；
- 并发建档不重复发初始资产或创建第二份农场；
- 不同 bootstrap 规则冲突按合同返回；
- 同 owner Actor 邮箱串行，不同 owner 并行；
- Actor 加载期间请求合并，不重复 Load；
- Player version=10 保存期间产生 version=11，旧保存完成后仍保持 Dirty 并再次入队；
- 同一 Player 同时最多一个保存 I/O；
- 保存失败不推进 savedVersion，正常钝化前 ForceFlush；
- 邮箱满、激活失败和 Load 超时；
- CAS 第一次冲突、第二次成功；
- 连续三次冲突；
- player_epoch/farm_epoch stale；
- CAS 结果未知但实际已提交；
- Actor 卸载后从 Store 恢复一致。

S2-2 Definition of Done：

- HTTPS TestLogin 可得到 AccountReady(player_id, farm_id) 和 connection_ticket；
- 首次建档、重复建档、加载、CAS 和恢复测试通过；
- 相同 ActorKey 总是进入同一进程内串行键，`PLAYER:X` 与 `FARM:X` 不混用；
- MemoryStore 不会掩盖冲突；
- TcaplusPlayerStore/TcaplusFarmStore 完成真实 Create/Get/CAS；若真表仍未就绪，只能标记“骨架完成，最终 Store 验收未完成”。

---

## 7. S2-3：真实接入、Session、Join 与重连

目标：使用真实公网 DNS/CLB/TLS、farm-gate Direct WebSocket 和 TcaplusDB，完成登录、
ClientHello、Join 空农场和断线恢复。

### 7.1 Session 与连接

实现：

- HTTPS TestLogin；正式逻辑路径暂定 `/v1/session/login`，若公网统一增加 `/api` 前缀，则 H5 实际请求 `/api/v1/session/login`；
- 32 随机字节 base64url `connection_ticket`；
- Redis 只存 ticket 哈希，默认 30 秒；
- GETDEL/Lua 原子核销；
- `SessionAuthority`、`session_authority_epoch`、`active_session_epoch`；
- 新登录顶号，旧 epoch 拒绝取得写 Permit；
- `connection_id + transport_generation` 防底层索引复用；
- ClientHello/ServerHello 状态机。

现有 H5 的 `WebAuthProvider`、`StagingAuthProvider`、`BrowserWebSocketTransport` 和 `WsClient` 继续复用，但真实联调前必须：

- 将 canonical 登录响应固定为 `connection_ticket/player_id/active_session_epoch/wss_endpoint`；
- 只用登录响应中的 `wss_endpoint` 或部署环境的同源 `/ws`，不能使用 Docker 内网服务名；
- 为重连增加有上限的指数退避与抖动，且每次重新取得新的 connection_ticket；
- 保留单次连接/登录 single-flight，避免页面事件触发并发重连风暴；
- 记录 WebSocket close code/reason，用于区分主动退出、认证失败和网络恢复；
- 发现版本缺口或 `RESYNC_REQUIRED` 时重新 Join，不自动重放结果未知的写命令。

### 7.2 Join 状态机

权威时序：

```text
Actor turn:
  创建 Joining
  分配 subscription_id / presence_epoch
  记录 snapshot_version = V
  从此缓存 version > V 的 FarmChange
  生成 Snapshot(V)
  Snapshot 进入 Logic 本地有界 DownlinkDispatcher
  返回 Join ACCEPTED
  释放当前 turn

DownlinkDispatcher:
  调用 GatewayDownlinkService.DeliverBatch

farm-gate:
  Snapshot 进入匹配 connection generation 的 ConnectionWriter
  返回 QueueAccepted

Actor mailbox:
  收到 ConfirmSnapshotQueued
  将 JoiningBuffer 中 V+1... 作为同订阅有序批次入本地下行队列
  整批入队成功后转 InFarm

H5:
  收到并应用 Snapshot
  从 Joining 转 InFarm
  此后才允许经营命令
```

`ACCEPTED` 只表示 Join 已建立且 Snapshot 进入 Logic 本地下行队列，不表示浏览器已经收到 Snapshot。

### 7.3 有界参数初值

参数必须可配置并在压测后调整：

| 参数 | S2 初值 |
|---|---:|
| Join timeout | 5s |
| JoiningBuffer 条目数 | 64 |
| JoiningBuffer 字节数 | 256KiB |
| ConnectionWriter 队列 | 256 items 或 1MiB，先到者为准 |
| connection_ticket TTL | 30s |
| 最大 FarmFrame | 以 Direct WebSocket/CLB 帧与解码探针为准，未验证前不写死生产值 |

### 7.4 失败与恢复测试

- Snapshot 本地入队失败：不返回 ACCEPTED，取消 Joining；
- farm-gate QueueFull：取消/失效订阅，客户端重新 Join；
- Snapshot 已入队但连接断开：旧 generation 下行全部拒绝；
- JoiningBuffer 溢出：`RESYNC_REQUIRED` 或断开后重新认证；
- 重复 Join：旧 subscription 失效，不占用两个名额；
- Snapshot(V) 生成后发生 V+1：客户端最终连续收到 V、V+1；
- 慢连接：Actor 邮箱仍能处理其他 turn；
- ServerHello 后 Snapshot 超时：H5 关闭连接、重新认证并使用新的 Join request_id；
- 新设备登录后旧设备继续写：SessionAuthority 拒绝旧 epoch；
- Gate/Logic 任一进程重启或 CLB 断开：强事实不回退，客户端重新认证并 Join 收敛。

S2-3 Definition of Done：

- H5 经真实 DNS/CLB/farm-gate Direct WebSocket 完成 ClientHello/ServerHello；
- 首次登录在 Tcaplus 创建 PlayerCore/FarmCore，后续登录加载同一对实体；
- Join 自己的空农场得到权威 Snapshot；
- Snapshot 与 JoiningBuffer 无版本空洞；
- 断线和顶号后旧连接不能继续写；
- 重连后重新 Join 收敛；
- 慢下行不阻塞 Actor；
- 所有阶段退出条件有自动化结果或真实探针记录。

---

## 8. 模块实施映射

| 模块 | 所属进程 | S2 职责 | S2 是否实现 |
|---|---|---|---|
| TestLogin/Auth | farm-gate | 测试登录、调用 Bootstrap、签发 ticket | 是 |
| FarmFrameProtocol | farm-gate | 帧解析、版本/大小/类型校验 | 是 |
| SessionAuthority | farm-gate + Redis | 会话代次、顶号、Permit | 是 |
| ConnectionRegistry | farm-gate | connection generation 与 Session 绑定 | 是 |
| ConnectionWriter | farm-gate | 每连接有界 FIFO、下行背压 | 是 |
| Connection/Session Lane | farm-gate | 串行短状态转换，不等待后端 RPC | 是 |
| BackendPool | farm-gate | 有界执行 Logic RPC，完成后回到当前 Lane | 是 |
| CommandRouteDirectory | farm-gate + farm-logic | 推导/复核第一跳 ActorAddress | 是 |
| LogicIngress | farm-logic | 验证可信上下文、进入 Actor 路由 | 是 |
| ActorRouteAdapter | farm-logic | routing v2→Player/Farm 静态 Assignment→本地 Actor | 是，S2 单 Logic |
| PlayerActorRegistry / FarmActorRegistry | farm-logic | 两类实体唯一激活、autoload、基础卸载 | 是 |
| PlayerActor | farm-logic | 玩家资产权威空模型 | 是 |
| FarmActor | farm-logic | 空农场权威与 Join 版本屏障 | 是 |
| FarmPresenceState | FarmActor runtime | Joining/InFarm/订阅/Buffer | 是，可丢失 |
| DownlinkDispatcher | farm-logic | Actor 外异步下行与回调 | 是 |
| PlayerStore / FarmStore | storage | Player 增量回写/ForceFlush；Farm Get/Create/CAS | 是，S2 先骨架 |
| PlayerWritebackQueue | player-svr runtime | Dirty 去重、单 in-flight、版本回调、优先 Flush、积压保护 | 是，S2 最小骨架；P0b 启用经济写 |
| FriendshipAuthority | farm-logic 模块 | 好友访问权限 | 否，S4 |
| Trade/Farm/Economy | 两类 Actor 内模块 | Buy/Plant/Harvest | 仅保留结构，不实现命令 |
| farm-worker | 独立进程 | Outbox/Settlement | S2 不启动；S3b Plant 时启用最小实现 |

---

## 9. 可观测性基线

### 9.1 结构化日志字段

- request_id / operation_id；
- actor_kind / player_id / farm_id / target_farm_id；
- connection_id / transport_generation；
- session_authority_epoch / active_session_epoch；
- routing_version / virtual_shard / shard_revision / logic_instance；
- actor_activation_id；
- record_version / player_epoch / farm_epoch；
- subscription_id / presence_epoch / farm_version；
- stage、result、error_code、latency_ms。

日志不得记录：

- connection_ticket 原文；
- Tcaplus/Redis 密钥；
- 平台登录票据；
- 完整用户隐私数据。

### 9.2 最小指标

- Gate：连接数、ClientHello 成败、ticket 核销、上/下行队列深度、QueueFull、帧拒绝；
- Logic：RPC QPS/P95/P99、路由失败、Actor 激活数/速率、邮箱深度/等待；
- Store：Get/Insert/CAS/HMSet QPS/P95/P99、冲突率、未知结果、错误码；
- Player Writeback：queue depth、oldest dirty age、version gap、save retry、force flush latency、backpressure；
- Join：Joining 数量、成功率、耗时、Buffer 使用量、溢出、Resync；
- 进程：CPU、内存、Goroutine、GC、网络与文件描述符。

---

## 10. 测试分层与命令

| 层级 | 内容 | 是否依赖外部环境 |
|---|---|---|
| 单元测试 | codec、路由、Actor 串行、Memory CAS、Dirty version、Writeback、Join 状态机 | 否 |
| 进程内集成 | FakeTransport→Gate→Logic→MemoryStore | 否 |
| 基础设施探针 | Direct WebSocket/CLB、Tcaplus Create/Get/CAS；TCONND T1～T8 为候选 | 是 |
| 纵向集成 | H5→公网DNS/CLB→farm-gate Direct WebSocket→Logic→Tcaplus | 是 |
| 故障测试 | QueueFull、超时、断线、重启、CAS 结果未知、回写失败和 ForceFlush | 部分 |
| 小规模压测 | 连接、Join、Actor 激活、Store CAS/HMSet、回写队列 | 是 |

建议统一入口：

```text
scripts/test-unit
scripts/test-race
scripts/probe-tconnd
scripts/probe-tcaplus
scripts/test-s2-e2e
```

Windows 只要求运行 H5 和不依赖平台资源的测试；真实 CLB/Direct WebSocket、Docker、
Tcaplus 和端到端验收在 Linux 专用服务器执行。TCONND 只在候选 Profile 下单独验证。

---

## 11. Linux 云服务器与 Docker 隔离方案

Docker 适合当前项目，但目标是获得可重复环境、故障隔离和可回滚制品，不是把所有进程塞进一个容器。

### 11.1 开发与 Staging 分开

- 日常 H5 开发：Windows 上继续运行 Vite Mock，保留热更新和现有 40 个单测；
- 本地无平台依赖测试：Go/Node 可以直接运行，也可以使用固定版本的 toolchain 镜像；
- Linux Staging：只运行构建后的 H5 静态文件和真实服务进程，不运行 Vite dev server；
- 生产候选：镜像必须固定 commit/tag 或 digest，配置和 Secret 运行时注入。

### 11.2 推荐容器边界

```text
edge / h5-static
├── 多阶段 Node build
└── Nginx/Caddy 只提供 dist 静态文件与 HTTPS 路由

farm-gate
├── HTTPS Login/Auth/Ticket
├── Direct WebSocket Upgrade/PRE_AUTH
└── Session/GateCore/ConnectionWriter

farm-logic
└── 独立容器

Redis
└── 独立容器或受控测试实例

TcaplusDB
└── 远程受控实例，不装入 Compose

farm-worker
└── S2 不启动
```

每个自研进程使用独立容器，以便独立重启、限额、观测和回滚。TCONND/gateproxy 不进入
默认 Compose；需要验证候选链路时使用独立 `tconnd-candidate` Profile，并按平台要求处理
TBUS/IPC，不改变 farm-gate GateCore。

### 11.3 公网路由

S2 Staging 可以使用一个公网 443 入口：

```text
https://farm-staging.example/
  /                 → H5 静态文件
  /api/*            → farm-gate HTTPS API
  /ws               → CLB/TLS → farm-gate Direct WebSocket
```

P0 TLS 在 CLB 终止并私网回源 farm-gate。farm-gate 安全组只接受 CLB 来源，不能被公网绕过。
Optional WAF 和 TCONND/gateproxy 都是独立候选 Profile，探针通过前不进入默认路由。

Edge/Ingress 必须显式验证：

- WebSocket Upgrade/Connection；
- Origin 白名单；
- 空闲超时与前后台切换；
- 最大 frame；
- 长连接 drain；
- `/api` 的 CORS/Cookie/SameSite/Secure；
- H5 history fallback；
- access/trace 日志中的 ticket 脱敏。

### 11.4 Compose 只负责可重复部署

S2 提供两份 Compose profile：

- `core`：edge、farm-gate、farm-logic、Redis，使用 Direct WebSocket/FakeStore 做纯骨架测试；
- `staging`：公网 DNS/CLB/TLS、farm-gate、真实 Redis/TcaplusDB；
- `tconnd-candidate`：在 staging 旁显式加入固定版本 TCONND/gateproxy，仅做候选链路探针。

健康检查至少包含：

- `farm-gate /healthz`：进程存活；
- `farm-gate /readyz`：Redis、LogicIngress、Direct WebSocket 连接水位和 WritePump 正常；
- `farm-logic /healthz`：进程存活；
- `farm-logic /readyz`：PlayerStore/FarmStore 和路由表可用；
- Access Ready：CLB 健康检查、farm-gate PRE_AUTH/Session/ConnectionWriter 均可用。

Docker 不解决 Actor 一致性、CAS、Shard 或重连语义；这些仍由现有架构合同保证。S2 不需要先上 Kubernetes，Compose 能稳定复现单机多进程和故障注入即可。

---

## 12. 推荐执行顺序

严格按以下顺序推进：

1. 完成并提交本文、CAS 规则、routing v2 黄金向量和索引修正；
2. 实现 H5/Go uint64 与 FarmFrame 往返测试；
3. 确认 Linux/CLB 域名证书、TLS 回源、安全组和 Tcaplus zone/table；
4. 先运行 Direct WebSocket PRE_AUTH/帧/心跳/慢连接/Drain 探针与 Tcaplus Create/Get/CAS；
   TCONND/gateproxy T7→T1/T2/T3/T8 作为不阻塞 P0 的并行候选探针；
5. 回填探针报告，做 S2-0 Go/No-Go；
6. 修改最小 Proto 并完成 S2-1 空命令；
7. 完成 S2-2 Bootstrap/Route/Actor/Store；
8. 完成 S2-3 Session/Join/重连；
9. 运行 S2 纵向验收并形成 Demo；
10. 进入 S3 个人经营 Loop。

不要把“等待平台资源”变成整体停工：等待 CLB/Tcaplus 时，可完成本地 Direct WebSocket、
FakeTransport、MemoryPlayerStore/MemoryFarmStore、协议测试和状态机单测；但报告必须清楚
区分“本地骨架通过”和“真实基础设施验收通过”。

---

## 13. 当前阻塞项与责任边界

| 阻塞项 | 需要的输入 | 可并行工作 | 解除标志 |
|---|---|---|---|
| DNS/CLB/TLS 与私网回源 | 域名、证书、CLB监听器、安全组、Idle Timeout | 本地 Direct WebSocket 与协议测试 | H5 经 CLB 完成 WSS/ClientHello/重连 |
| PRE_AUTH 与慢连接保护 | 未认证连接预算、Frame/Decode上限、ConnectionWriter策略 | 状态机、FakeTransport、故障测试 | BLOCKER-P0A-01～06 全部通过 |
| TCONND/gateproxy 候选（不阻塞P0） | 镜像、TBUS/IPC、BY_WS配置 | 默认 Direct WebSocket 主链 | 候选 T1～T8 报告完成 |
| Tcaplus probe 表 | 正确 zone id + Player/Farm probe 表 | MemoryPlayerStore/MemoryFarmStore、Store 接口 | Create/Get/Farm CAS 与 Player HMSet/FieldUpdate/fencing 跑通 |
| Go 开发环境 | Linux/Windows Go toolchain 或项目镜像 | 文档与远端同步 | `go test ./...` 可执行 |
| H5 canonical adapter | v0.2 Proto commit/tag、TS 生成器和 golden vectors | 现有 Mock UI/Auth/Transport/Store 回归 | staging 构建通过 canonical golden tests |

这些是外部输入或环境事实，不需要重新讨论 Actor、服务拆分和业务 Loop。

---

## 14. S2 最终交付物

- 可操作的 H5 空农场页面；
- 现有 H5 的 canonical-v0.2 adapter、uint64 helper 和无 Mock 的 Staging 构建；
- 公网 DNS/CLB/TLS 配置、farm-gate Direct WebSocket、farm-logic；
- v0.2 最小 Proto 与生成代码；
- Memory/Tcaplus PlayerStore/FarmStore；
- Player DirtyTracker、版本化去重回写队列和 ForceFlush 最小骨架；
- PlayerActor/FarmActor、Actor 路由、基础 autoload/passivation；
- ClientHello/Session/Join/JoiningBuffer/Downlink；
- Direct WebSocket/CLB 与 Tcaplus 探针报告；TCONND 候选报告可独立交付；
- 单元、集成、故障和小规模压测脚本；
- S2 Demo 操作说明；
- Go/No-Go 验收记录。
- Linux Docker Compose、镜像构建、healthz/readyz 和一键回滚说明。

S2 的完成标准不是“页面显示了一个农场”，而是：

> 登录、接入、Session、ActorKey 路由、双 Actor、Player 增量回写骨架、Farm/可靠步骤 CAS、Snapshot、下行背压和重连这条真实后台主干已经可验证，后续业务沿已确认的 P-ASYNC/P-DURABLE 与单 Actor/跨 Actor 边界逐个增加纵切。
