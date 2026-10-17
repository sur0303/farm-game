# farm-game 详细设计

当前设计版本：`v1.21.2-production-architecture-freeze`（2026-07-30）
当前阶段：L0 总体生产架构已正式冻结为四个 P0 部署单元，正在进入 L2/L3 协议、Repository、
基础设施探针与纵向骨架实现。中期汇报请先阅读 [19-midterm-report](./19-midterm-report.md)。
旧 v0.3 文档和 Proto 仅作为实现参考，尚未按新蓝图逐份回改。

当前正在逐层冻结 L1 实现合同：接入与会话以
[26-access-session-contract](./26-access-session-contract.md) 为唯一权威；命令目录、Actor
路由、Actor Runtime 和跨 Actor Workflow 以
[27-access-routing-actor-workflow-freeze](./27-access-routing-actor-workflow-freeze.md) 为唯一
权威。公网命令输入的 L2 映射以
[28-command-catalog-public-protocol](./28-command-catalog-public-protocol.md) 为准。内部 RPC、
可信调用上下文和 RouteHeader 以
[29-internal-rpc-trusted-context](./29-internal-rpc-trusted-context.md) 为准。
Actor CAS、WriterEpoch、WriterClaim、Unknown Outcome、Persistence Lane 与权威 Store 以
[30-actor-authority-store-contract](./30-actor-authority-store-contract.md) 为准。
[31-actor-authority-redis-physical-contract](./31-actor-authority-redis-physical-contract.md) 负责 Redis Key、
同槽 Lua、索引和容量合同；[32-core-business-scenario-contract](./32-core-business-scenario-contract.md)
负责核心经营场景；[33-friendship-share-access-contract](./33-friendship-share-access-contract.md)
负责好友关系、分享与访问权限。[34-core-technical-closure](./34-core-technical-closure.md) 是面向实现
Agent 的总控页，汇总最终技术路线、Linux 真实代码基线、合同版本漂移、落地顺序与禁止事项。
第 26～33 号与旧 D1～D7 冲突时，优先采用对应领域的最新合同，
旧文档不得反向覆盖本轮讨论结论。
[core-scenario-casebook](./core-scenario-casebook.md) 用完整时序和故障案例解释上述合同，
便于实现与测试，但不反向覆盖 26～33 的规范条款。

第一阅读入口：[19-midterm-report](./19-midterm-report.md)（汇报视角）、
[22-production-architecture-v1.21](./22-production-architecture-v1.21.md)（仓库内唯一总体架构入口）和
[21-direct-websocket-tlog-freeze](./21-direct-websocket-tlog-freeze.md)（接入/Tlog 增量
冻结）。`18-final-architecture-blueprint` 是此前七部署单元阶段的历史稿，不再作为实现
入口。本文以下 D1～D7 是此前 v0.3 的详细设计材料；与 22 冲突时以 22 为准，再显式回改，
禁止悄悄混用。

本目录是 farm-game 后台的详细设计文档集。阅读顺序:

| 编号 | 文档 | 内容 | 可执行验证 |
|---|---|---|---|
| D0 | [22-production-architecture-v1.21](./22-production-architecture-v1.21.md) | L0 正式冻结：统一生产架构、服务边界、协议、时序、容量、可观测性、Tlog 与实施顺序 | 总体架构唯一入口 |
| D0-H | [18-final-architecture-blueprint](./18-final-architecture-blueprint.md) | 七部署单元阶段历史稿，仅用于追溯 | 禁止作为当前实现入口 |
| D0.5 | [19-midterm-report](./19-midterm-report.md) | 中期汇报：题目理解、工程问题、总体思路、当前进度与后续计划 | 汇报材料 |
| D1 | [00-overview](./00-overview.md) | 设计索引、冻结结论、术语、本阶段范围 | — |
| D1.5 | [05-system-skeleton](./05-system-skeleton.md) | H5→farm-gate→Logic→Actor→Store 主干、Shard 路由、RPC 与 Docker；接入增量以 D1.16 为准 | S2 纵向骨架验收 |
| D1.6 | [06-business-fact-classification](./06-business-fact-classification.md) | A/B/C/D 事实类别、L/W 协调属性与恢复等级 | Proto/Schema 评审依据 |
| D1.7 | [07-tconnd-access-boundary](./07-tconnd-access-boundary.md) | TCONND BY_WS、gateproxy、连接代次、下行确认与 Linux/Docker 部署 | 真实 TCONND 最小接入/故障验证 |
| D1.8 | [08-s2-go-no-go](./08-s2-go-no-go.md) | S2-0 冻结项、外部探针、编码准入和阶段顺序 | 每次进入下一子阶段前逐项检查 |
| D1.9 | [09-s2-v0.2-protocol-checklist](./09-s2-v02-protocol-checklist.md) | ClientHello、Envelope、Join、Snapshot、Downlink 的逐字段确认稿 | 确认后才修改 Proto |
| D1.10 | [12-business-module-scenario-map](./12-business-module-scenario-map.md) | 服务/RPC/模块/Actor 区分，P0 场景到权威、原子提交和异步流程映射 | 每个业务纵切开始前检查 |
| D1.11 | [13-loop-service-interaction-diagrams](./13-loop-service-interaction-diagrams.md) | 纵向骨架、个人成长、经济、实时社交、护理、偷菜等服务联动框图 | 各 Loop 方案评审与讲解 |
| D1.12 | [14-s2-implementation-plan](./14-s2-implementation-plan.md) | S2-0～S2-3 工作包、目录、依赖、测试、退出条件与交付物 | 当前唯一实施主计划 |
| D1.13 | [15-tconnd-gateproxy-integration-plan](./15-tconnd-gateproxy-integration-plan.md) | TCONND/gateproxy 复用边界、Linux 探针、BY_WS 决策门与接入步骤 | 当前真实接入专项计划 |
| D1.14 | [16-player-farm-actor-split](./16-player-farm-actor-split.md) | PlayerActor/FarmActor、双记录建档、Plant Saga、失败补偿与容量代价 | 当前 Actor/一致性基线 |
| D1.15 | [17-uc-routing-migration-adaptation](./17-uc-routing-migration-adaptation.md) | UC 能力取舍、routing v2、双 Region、Proxyless/Redirect/fencing 与迁移阶段 | 多节点演进合同 |
| D1.16 | [21-direct-websocket-tlog-freeze](./21-direct-websocket-tlog-freeze.md) | Direct WebSocket 默认主链、TCONND 候选 Adapter、连接恢复和 Tlog 分级导出 | 接入与流水冻结 |
| D1.17 | [23-task-system-design](./23-task-system-design.md) | 每日 0 点重置、好友访问/互动任务、长期成就、活动任务、事件幂等、领奖与红点 | 任务系统实现与验收合同 |
| D1.18 | [24-plant-codex-system-design](./24-plant-codex-system-design.md) | 首次自己收获解锁、活动档案、遗留种子、图鉴红点、任务联动与可靠恢复 | 植物图鉴实现与验收合同 |
| D1.19 | [25-player-persistence-runtime](./25-player-persistence-runtime.md) | PlayerActor 业务顺序、按物理记录 Lane、跨记录 Intent/Order、Unknown、钝化与迁移 | 多记录持久化实现合同 |
| D1.20 | [26-access-session-contract](./26-access-session-contract.md) | 账号登录、Cookie免密换票、Ticket两阶段激活、三套Join状态、ACCEPTED→Arm→Snapshot/Ready、Subscription Lease和200人同场下行 | 接入与会话实现、故障测试和压测合同 |
| D1.21 | [27-access-routing-actor-workflow-freeze](./27-access-routing-actor-workflow-freeze.md) | 双Route Domain、VirtualShard、Actor Runtime、WriterClaim迟到写Fence、WorkAddress、Redis P0存储与跨Actor Workflow/Order | 命令、路由、Actor和失败恢复实现合同 |
| D1.22 | [28-command-catalog-public-protocol](./28-command-catalog-public-protocol.md) | FarmFrame顶层oneof、CommandResult/RetryDirective、Push Context、Ready版本屏障、ADMITTED、权威幂等Hash和GetOperationResult四态 | 公网命令、下行分类与弱网结果恢复合同 |
| D1.23 | [29-internal-rpc-trusted-context](./29-internal-rpc-trusted-context.md) | Gate到FirstHop Unary、可信Context、ACCEPTED后Arm与Snapshot线性化切点、Coordinator/Dispatcher/Worker WorkAddress、Subscription Lease RPC、Redirect和结果恢复 | 内部RPC与服务间可信边界候选合同 |
| D1.24 | [30-actor-authority-store-contract](./30-actor-authority-store-contract.md) | 三层写入Fence、幂等预检、持久ResolutionReceipt、auth-v1 Shard/Bucket一一映射、单一主Persistence Group、可信Work Lease与有界记录 | Actor Store、Redis物理合同与故障测试的稳定语义输入 |
| D1.24a | [31-actor-authority-redis-physical-contract](./31-actor-authority-redis-physical-contract.md) | Authority/Workflow/Projection Redis 物理分层、同槽 Lua、DueIndex、查询副本、容量与热点保护 | Redis Repository、Lua、投影与容量测试合同 |
| D1.24b | [32-core-business-scenario-contract](./32-core-business-scenario-contract.md) | Buy、Sell、UnlockPlot、Plant、Harvest、Care、Theft 的权威边界、Workflow、故障与并发矩阵 | 核心经营闭环实现与故障注入合同 |
| D1.24c | [33-friendship-share-access-contract](./33-friendship-share-access-contract.md) | canonical FriendRelation、多人复用分享链接、删除/拉黑、双方精确好友上限和 Permit 边界 | 好友关系与访问权限实现候选合同 |
| D1.24d | [34-core-technical-closure](./34-core-technical-closure.md) | 四 Deployment、通信/路由/Actor/Redis 总收口，Linux 最新代码审计、合同漂移、施工门槛与纵向顺序 | 后续实现 Agent 第一阅读入口；不反向覆盖 26～33 |
| D1.25 | [core-scenario-casebook](./core-scenario-casebook.md) | 登录与Join的完整Arm/Ready时序、状态释义、故障案例和验收断言 | 实现Agent、测试Agent与答辩讲解入口；规则冲突时服从26～33 |
| D1.26 | [60-access-session-test-acceptance-freeze](./60-access-session-test-acceptance-freeze.md) | S01～S06、Actor Harness、Store Contract、Protocol Bot、200人同场、弱网、容量与Evidence Bundle | 接入会话测试语义和验收口径唯一入口；单次重跑计划与结果进入 `docs/verification/` |
| D2 | [10-protocol](./10-protocol.md) | 三大信封 + 六态状态机 + 错误码 | `internal/protocoltest` |
| D3 | [11-frame-codec](./11-frame-codec.md) | 单一 FarmFrame protobuf、连接序号与大小边界 | `internal/gateway/protocol` |
| D4 | [20-ownercore](./20-ownercore.md) | PlayerCore/FarmCore Schema + 大小预算 | 旧 ownercoresize 待拆分重测 |
| D5-H | [30-redis](./30-redis.md) | 早期“Tcaplus权威、Redis缓存”历史 Key/TTL 材料，仅供迁移和探针追溯 | 不得作为当前权威存储实现合同 |
| D6 | [40-interfaces](./40-interfaces.md) · [41-sequences](./41-sequences.md) | gRPC 接口清单 + Login/Plant/Join/Theft 时序 | `api/gen/logicrpc` 生成 |
| D7 | [50-actor-runtime](./50-actor-runtime.md) · [51-tcaplus-verify](./51-tcaplus-verify.md) · [52-runtime-performance](./52-actor-runtime-performance-evolution.md) | keyed-actor 执行器 + Tcaplus 真库验证 + UC 能力/持久化性能演进门 | `internal/logic/actor/runtime`(**-race**);`cmd/tcaplus-verify`(**连真库鉴权**) |

## S1 关键成果

1. **Actor 基线更新**：采用 PlayerActor/FarmActor 双权威；玩家资产与农场热点独立，跨边界使用 Intent/Settlement + Outbox + Receipt。
2. **已有可执行 S1 协议基线**：现有 proto、生成代码和测试仍是单 OwnerCore v0.1，只能用于探针与迁移参考，不能当作 v0.3 最终合同。
3. **容量模型待重算**：旧 OwnerCore 典型约 1.8KB 只作为字段量参考；必须分别实测 Player 内存事务、异步 HMSet、强制持久化步骤与 Farm CAS，不能再按“每个命令一次 CAS”估算。
4. **并发原语已证明**:keyed-actor 执行器同 key 串行(`-race` 下普通计数器无竞态)、异 key 并行、有界邮箱过载。
5. **真库连通**:Redis(`198.18.0.14:6380`)读写/hash-tag 验证通过;TcaplusDB(`198.18.0.5:9999`)dir 鉴权通过,CRUD/CAS/延迟实测待建表 + zone id。

## 设计与实现边界

已经冻结的是业务与系统合同：

- H5/微信小程序的 P0 WSS 默认经 `公网 DNS → CLB/TLS` 直接连接 farm-gate，HTTPS 登录同样进入 farm-gate；
- farm-gate 自己承担 WebSocket Upgrade、PRE_AUTH、Ticket 兑换、Session 绑定、FarmFrame、限流、ConnectionWriter 背压和旧连接 Fence；TCONND + gateproxy 只是探针通过后显式启用的候选 Transport Adapter；
- 每个 WebSocket binary message 只承载一个 FarmFrame protobuf，不使用 total_size/meta_size/FrameMeta；
- TCONND/gateproxy/farm-gate 均不拥有 ShardAssignment；farm-gate 可由命令目录推导第一跳 ActorAddress，Logic 必须复核；
- S2 通过任意健康 LogicIngress 进入 Logic 内 ActorRouteAdapter；多 Logic 时优先评估 xrpc/proxyless 直达当前 holder；
- `PlayerActor(player_id)` 是玩家资产权威，`FarmActor(farm_id)` 是农场业务权威；
  `FarmSubscriptionHub(farm_id)` 与 FarmActor 同 Holder，但独立保存可丢的
  Presence/Subscription/JoiningBuffer；
- Actor 安全目标是“全局有效写者唯一”，不是“全局物理对象唯一”：迁移、暂停、分区和恢复时
  允许短暂多实例，但旧 Holder/Activation 必须被 WriterEpoch + WriterClaimID + RecordVersion +
  Receipt 的存储 Fence 拒绝；writer_claim_id 同样用于钝化重激活和 Unknown 解析，不是热迁移专属；
- P0 暂用公司 Redis 保存 Player/Farm 权威快照、Workflow/Order、Receipt 和 DueIndex；
  Care/Guard 等来源同槽 Order 跟随来源 Actor 的 AuthorityPersistenceBucket 原子创建，独立
  Workflow 才按 workflow_id 分桶；Dispatcher、DueIndex 和 Repository 统一携带 WorkAddress，
  `auth-v1` 中 AuthorityPersistenceBucketID 与对应 VirtualShardID 一一映射，但保持概念分离；
  两类持久化 Bucket 均不随 Holder/Pod 迁移。每个强写同时校验 WriterEpoch、WriterClaimID 和
  RecordVersion；Unknown 恢复先 FenceAndClaim。若 Redis 尚未启用生产级
  落盘、复制与备份，只承诺 Redis 存活时可恢复，不宣称已经满足生产灾难恢复；
- `ACTOR_SOFT` 只允许压测后按“纯 PlayerActor、低风险、无外部副作用”的命令白名单开启；
  Reservation、Receipt、跨 Actor Participant 和已发布不可逆事实仍走 Barrier；
- 玩家公开信息和好友页查询先放在 player-svr 的独立 Read Role 中，不新增 query-svr/infosvr；后续按负载证据独立扩容；
- Plant 先预留种子、再 Farm 种下、最后确认消耗；永久拒绝释放预留；
- 当前 `AccountCredential` 与 P0 业务数据都通过不同 Repository/Key 空间使用公司 Redis；
  Session、业务权威、Workflow、Projection 和 Cache 仍按不同丢失语义分层，不能因物理上
  共用 Redis 就混淆职责；
- Join 采用 `JOINING → FLUSHING → ACTIVATING → IN_FARM`。Gate 先将 ACCEPTED 入队并显式
  `ArmSnapshot`；Snapshot 与截至 N 的 Catch-up 进入同一连接 FIFO 后，再以
  `SUBSCRIPTION_READY(N)` 建立版本屏障。H5 只有在 applied_farm_version=N 且恢复屏障完成后进入
  FARM_INTERACTIVE。JoiningBuffer 属于 FarmSubscriptionHub，不属于 FarmActor；
- P0 不做任意下行 ACK/重放，缺口通过 `RESYNC_REQUIRED + 重新 Join` 恢复。
- 成熟由 `mature_at` 派生；成熟提醒与排行榜均为 P1 异步能力，不阻塞核心 Loop。
- `player_id/farm_id/target_farm_id` 的逻辑与 IDL 类型统一为 `uint64`；H5 不得用 JavaScript `Number` 承载。
- 同一 CropCycle 全局只有第一次浇水有效；任何玩家成功后，其他玩家后续浇水均返回 `NO_CHANGE + 最新 PlotView`。

尚未实现或尚未验证：

- v0.2 Proto 与生成代码；
- TCONND BY_WS + gateproxy 最小链路及真实兼容性；
- BootstrapService、GatewayDownlinkService 和新版 TrustedCommandContext；
- Logic 内 ActorRouteAdapter；
- PlayerActor/FarmActor 与 PlayerStore/FarmStore；
- Join 异步状态机、有界 JoiningBuffer 和 ConnectionWriter 背压；
- Actor autoload/passivation、加载风暴保护、多节点迁移；
- Redis Lua/故障语义、未来持久化 Redis 或 Tcaplus 目标方案与 3000 万 DAU 容量结论。

因此阅读时先看 `22`，确认目标架构后再用 `21`、`16`、`17`、`00`、`05`、`12`、`13`、
`10`、`11`、`40`、`41` 理解已有设计细节，不要从旧 Proto 或旧“冻结结论”反推总体架构。

## 待外部输入

- TcaplusDB **zone id** 与在控制台按 `cmd/tcaplus-verify/verifypb/probe.proto` **建表 `owner_probe`**,以完成 D7 的 CRUD/CAS/写放大实测(见 [51-tcaplus-verify](./51-tcaplus-verify.md) §3)。

## 当前执行顺序

在 18 完成评审、服务名和路由合同回填旧文档前，先不直接执行旧 S2 主线。评审通过后再修订 [14-s2-implementation-plan](./14-s2-implementation-plan.md) 和 [08-s2-go-no-go](./08-s2-go-no-go.md)。旧顺序仅作历史参考：

1. S2-0：冻结 ActorKey、routing v2 黄金向量、player_id/farm_id、双记录 Bootstrap、C3～C5 与心跳边界，完成关键 TCONND/Tcaplus 探针；
2. S2-1：按 D2/D3/D6 校正最小 Proto 并重新生成；
3. S2-2：跑通 HTTPS 登录、双记录幂等建档、ClientHello/ServerHello 和空 PlayerCore/FarmCore 加载；
4. S2-3：跑通 Join 的 JoiningBuffer、QueueAccepted、版本化空农场 Snapshot 与重连验收。

完成后按 Buy → Plant Saga + 最小 Worker → mature_at + Harvest Saga 进入个人经营 Loop。
