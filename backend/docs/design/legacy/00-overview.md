# 00 · 设计索引与公共前提

> 2026-07-29 接入与流水增量冻结：P0 默认链路改为
> `公网 DNS → CLB/TLS → farm-gate Direct WebSocket`；Upgrade、PRE_AUTH、Ticket、
> Session、FarmFrame、限流、ConnectionWriter 和旧连接 Fence 均在 farm-gate 内。
> `TCONND + gateproxy` 只保留为显式平台候选，WAF 也是探针通过后才能启用的可选外围
> Profile。Tlog 是结构化经营流水，不是业务真值；A 级在 ACK 来源事件前必须进入独立
> Export Spool，B 成功来自已提交事实，普通失败 B/C 才允许 BestEffort。详细合同见
> [21-direct-websocket-tlog-freeze](./21-direct-websocket-tlog-freeze.md)。本文其余历史索引若与
> 21 冲突，以 21 为准。

本文件原为 farm-game v0.3 详细设计的**总索引与公共前提**。其余设计文档(D2~D7)记录该版本的协议、模型和实施材料。

当前历史设计版本：v0.3（2026-07-27）。新的唯一顶层架构入口为
[22-production-architecture-v1.21](./22-production-architecture-v1.21.md)；
`18-final-architecture-blueprint` 是此前七部署单元历史稿。仓库现有 Proto/生成代码仍是
S1 v0.1 的单 OwnerCore 可执行基线，不能用旧 Proto 反向覆盖新设计。

设计基线:项目交接文档 `study/supernova/classic-farm-agent-handoff.md`(v0.1)。本阶段沿用其冻结结论,允许提修正——已提出的修正见 [§4](#4-对基线的修正与补充)。

---

## 1. 本阶段(S1)范围

S1 = **先写完全部详细设计文档,再进业务代码**。产出物:

| 编号 | 文档 / 产物 | 对应交接文档任务 |
|---|---|---|
| D1 | `00-overview.md`(本文) | 索引与术语对齐 |
| D1.5 | `05-system-skeleton.md` | H5→farm-gate→Logic→Actor→Store 主干、Shard 路由、内部 RPC 与部署阶段；原 TCONND-only 表述由 D1.16 覆盖 |
| D1.6 | `06-business-fact-classification.md` | A/B/C/D 事实分类、L/W 协调、缓存与恢复 |
| D1.7 | `07-tconnd-access-boundary.md` | TCONND BY_WS、gateproxy、连接代次、下行确认与 Linux/Docker |
| D1.8 | `08-s2-go-no-go.md` | S2-0 冻结项、外部探针、编码准入和阶段顺序 |
| D1.9 | `09-s2-v02-protocol-checklist.md` | S2 最小 Proto 逐字段确认稿 |
| D1.10 | `12-business-module-scenario-map.md` | 服务/RPC/模块/Actor 区分，业务场景到原子提交和异步流程的统一映射 |
| D1.11 | `13-loop-service-interaction-diagrams.md` | 纵向骨架、个人成长、经济、实时社交、护理、偷菜等服务联动框图 |
| D1.12 | `14-s2-implementation-plan.md` | S2-0～S2-3 的实施顺序、目录、依赖、测试、退出条件与交付物 |
| D1.13 | `15-tconnd-gateproxy-integration-plan.md` | TCONND/gateproxy 复用边界、Linux 探针与接入步骤 |
| D1.14 | `16-player-farm-actor-split.md` | PlayerActor/FarmActor 边界、双记录建档、Plant Saga 与性能代价 |
| D1.15 | `17-uc-routing-migration-adaptation.md` | UC 接入/路由/迁移能力的 Farm 定制取舍、routing v2、双 Region 与扩缩容阶段 |
| D1.16 | `21-direct-websocket-tlog-freeze.md` | Direct WebSocket 默认主链、TCONND 候选 Adapter、连接恢复与 Tlog 分级/导出合同 |
| D2 | `10-protocol.md` + `api/proto/common/*` | 任务一:信封 + 状态机 + 错误码 |
| D3 | `11-frame-codec.md` | 任务一补充:字节帧编码 |
| D4 | `20-ownercore.md` + `api/proto/{farm,economy,settlement}/*` | 任务三:PlayerCore/FarmCore Schema + 大小预算 |
| D5 | `30-redis.md` | 任务四:Redis Key/TTL/故障恢复 |
| D6 | `40-interfaces.md` + `41-sequences.md` | 任务二+五:接口清单 + 时序图 |
| D7 | `50-actor-runtime.md` + `51-tcaplus-verify.md` + `52-actor-runtime-performance-evolution.md` + `cmd/tcaplus-verify/` | 任务六:Runtime + 真库最小验证 + UC 能力/持久化演进门 |

S1 之后先按 [S2 实施计划](./14-s2-implementation-plan.md) 完成纵向系统骨架；S3 以后再按业务 Loop 逐层展开。

## 2. 一句话架构

> H5 通过 HTTPS 进入 `farm-gate` 完成登录，默认经
> `公网 DNS → CLB/TLS → farm-gate Direct WebSocket` 长连接收发 FarmFrame；Upgrade、
> PRE_AUTH、Ticket、Session、限流和 ConnectionWriter 都在 farm-gate 内。TCONND +
> gateproxy 与 WAF 都是探针通过后显式启用的候选 Profile。`farm-gate` 管应用 Session 和下行背压但不拥有业务事实；
> player-svr/farm-svr 按 `PLAYER:player_id` 或 `FARM:farm_id` 路由到 `PlayerActor` /
> `FarmActor` 串行裁决。Player 普通软资产在 Actor 内原子修改后异步增量回写 Tcaplus，
> Farm/唯一领取/跨 Actor 步骤可靠持久化，跨 Actor 操作由 Workflow + Receipt 收敛；
> Redis 只保存可重建状态。Tlog 不参与裁决和恢复；A 级关键流水经独立 Export Spool
> 可靠交接，B 成功从已提交事实派生，普通失败 B/C 才允许 BestEffort。

## 3. 冻结结论(不得随意推翻)

沿用交接文档,列为其余文档的硬约束:

1. **权威事实单一拥有者**:任何影响资产、地块或权限的事实都只有一个权威者。客户端、接入层、Redis 缓存、广播消息、查询投影都不能反向裁决权威结果。
2. **路由键对齐冲突键**:会修改同一份权威事实的请求必须进入同一串行裁决范围。玩家资产走 `PLAYER:player_id`，农场事实与 Presence 走 `FARM:farm_id`。
3. **同 ActorKey 串行,不同 ActorKey 并行**:单 Actor 顺序只解决单实体内执行顺序，不自动解决 DB 提交、跨 Actor 事务、可靠投递、幂等、双活迁移或存储故障。
4. **提交策略显式**：普通 Buy/Sell 先在 PlayerActor 内通过 EconomyTxn 原子修改 Wallet/Inventory/ShopCounter，再标脏并异步回写；Farm 写、唯一领取、付费和跨 Actor 参与步骤仍在 Tcaplus 可靠提交后 ACK。CommandCatalog 必须声明 P-ASYNC/P-DURABLE/P-RUNTIME。
5. **PlayerCore / FarmCore 分离**:`PlayerActor` 只写 PlayerCore，`FarmActor` 只写 FarmCore；Tcaplus 无跨记录事务，Plant/Harvest/CareReward/Theft 使用持久 Intent/Settlement + Outbox + Receipt 收敛，禁止跨 Actor 直接改对方记录。
6. **广播不是业务提交**:广播失败不回滚已成立的业务事实;客户端靠版本缺口检测与重新 Join 收敛。
7. **缓存不是权威**:Redis 丢失可影响体验/命中率/限流精度，但不能成为金币、地块、关系或幂等的唯一来源。Player 普通软资产的受控崩溃 RPO 来自明确的进程内异步回写选择，不来自 Redis。
8. **四个 P0 Deployment**：`farm-gate` / `player-svr` / `farm-svr` / `farm-worker`；Friendship、Mail、Task、RedPoint 和 Read API 先作为清晰领域模块，不为名字单独拆服务。
9. **外部通信**:静态资源 HTTPS/CDN；登录与分享落地 HTTPS API 进入 farm-gate；登录后
   命令/快照/实时变化默认走公网 DNS/CLB/TLS 后的 farm-gate Direct WSS。TCONND
   BY_WS/gateproxy 与 WAF 均为候选 Profile。Protobuf 为唯一业务 IDL。
10. **应用帧**:一个 WebSocket binary message 只承载一个 FarmFrame protobuf；不使用 `total_size/meta_size/FrameMeta`，连接序号只用于链路完整性观测，不承担业务幂等。
11. **内部通信**:Protobuf IDL + Unary RPC;实现用 **gRPC-Go**(tRPC-Go 环境不可用时的既定回退)。
12. **存储**:TcaplusDB 唯一权威业务主库;Redis 保存可重建短期状态;不引入 Memcached;MongoDB 仅作回退评审项。
13. **跨 Actor 流程可靠最终一致**：Plant、Harvest、CareReward、Theft 均以各 Actor 的 P-DURABLE 局部步骤为基础，依靠持久 Workflow/Intent/Settlement + Outbox + Receipt 推进；不做跨记录大事务，也不把普通 Player Dirty 回写或内存回调当成可靠进度。
14. **顶号/Permit/epoch**:新登录提升 `active_session_epoch`,旧 epoch 不能再取得新写 Permit;踢线通知是体验不是安全边界。
15. **JoinFarm 无缺口快照**:目标 Actor 在同一 turn 内出 Snapshot(V) 并接续 V+1.. 变化,不得"先查快照再独立订阅"。
16. **幂等先查再执行**:相同 `operation_id` 到达入口权威 Actor 后先查已有 OperationResult/Intent/Settlement；跨 Actor 内部投递使用服务端生成的 txn/settlement id 幂等，结果未知时查询或用同一 id 重投，不能猜失败。
17. **实体 ID 统一为 uint64**：`player_id/farm_id/target_farm_id` 的逻辑与 Protobuf IDL 均使用 `uint64`，`0` 非法；H5 通过 protobuf Long/BigInt 或十进制字符串处理，禁止转为 JavaScript `Number`。P0 可令 farm_id 与 owner_player_id 数值相同，但类型和 ActorKey 不同。
18. **播种先预留、后落地**：PlayerActor 先持久预留种子并创建 PlantIntent，FarmActor 幂等创建 CropCycle，最后 PlayerActor确认消耗；暂时失败重试，永久业务拒绝释放预留。详见 [双 Actor 设计](./16-player-farm-actor-split.md)。
19. **路由合同现在冻结、分布式能力分阶段实现**：双 Actor 使用 routing v2（kind byte + uint64 大端 ID + xxhash64，Player/Farm 独立 Assignment）；S2 静态单 Logic，多 Logic 时优先接入批准版本的 xrpc/proxyless + Coordinator/Lease/Redirect/writer_epoch。Gate 滚动更新先重连与重新 Join，不在没有 ACK/重放合同前宣称无感 Session Handoff。详见 [路由迁移设计](./17-uc-routing-migration-adaptation.md)。

### P0 关键产品规则(约束数据模型与命令)

- 每位玩家一个私有农场;好友不能播种/正常收获/出售/消耗农场主资产;P0 好友可查看、浇水、偷菜。
- 同一 CropCycle 全局只有第一次浇水有效；无论首次有效操作者是农场主还是任一好友，之后所有浇水都返回 `NO_CHANGE + 最新 PlotView`，且不重复增加护理任务进度。
- P0 暂定每个 CropCycle 最多一个浇水事件和一个虫害事件；出现时间在播种时按版本化规则随机生成并固化。好友首次有效护理的经验/任务进度通过 `CareRewardOutbox + CareRewardReceipt` 从目标 FarmActor 可靠到账帮助者 PlayerActor。
- 作物未成熟不能偷;一茬默认产量 10,最大总可偷 3,主人至少保留 7;每名好友对每茬最多一次消耗型偷菜结算。
- 宠物拦截消耗本茬机会但不减产、不扣访客金币;P0 仓库容量视为足够大。
- 同一农场最多 8 名可操作成员(含农场主);不排队、不只读围观;同一玩家仅一个活跃可写会话。
- 无移动/聊天/战斗/物理 Tick;成熟状态由 `mature_at` 与服务器时间派生,不依赖 Timer 改写。

## 4. 对基线的修正与补充

本阶段在沿用基线的前提下,新增以下四项(经用户确认):

1. **真 TcaplusDB / Redis 实测**:两者均已申请且本机可连(Tcaplus `198.18.0.5:9999`,Redis `198.18.0.14:6380`)。D7 写连真库的最小验证程序,把交接文档 §9.6/§17.2 列为"待平台确认"的 SDK 语义(RecordVersion 条件写、写确认点、切主回退、写放大、大记录 P95/P99)实测出来,回填容量模型。
2. **字节帧编码**(D3):基线定义了逻辑信封,补齐 WSS 二进制帧的字节布局。
3. **本地不假成功**：`PlayerStore/FarmStore` 接口隔离；memory 实现**真实模拟 CAS/条件写**（冲突真失败），用于单测与离线开发；集成/压测走真库。
4. **压测工具链**(S7):Go WSS 压测客户端 + Prometheus/Grafana + pprof,产出《性能测试报告》。
5. **命令目录拆分**：`EnsureAccountInitialized` 属于登录建档 `BootstrapService`；`ApplyPlant/FinalizePlant/CreditHarvest/GrantLoot/MarkGranted/CompleteTheft` 属于可信内部命令；`ExpirePresence` 由 farm-gate 断线提示、FarmActor 惰性过期和 Logic 本地租约清理处理。
6. **版本分实体管理**(见 D4 与 D7)：PlayerCore 与 FarmCore 各自拥有 `RecordVersion`、`state_version` 和 actor epoch；两条记录的版本不可比较。`farm_version/plot_version` 是客户端公开变化版本。
7. **proto 域分阶段落地**:§6 声明的 7 域中,S1 已建 `common / farm / economy / settlement`(+ 组合表 `ownercore`、服务 `logicrpc`、表选项 `tcapluspb`);`session / social / realtime` 的独立 package 留待需要时建立(P0a 阶段:session/social 走 logicrpc + 进程内模块,realtime 复用 `common` 的 PushEnvelope)。

## 5. 术语表

| 术语 | 含义 |
|---|---|
| PlayerActor | 按 `PLAYER:player_id` 串行裁决玩家资产、成长和玩家侧流程的有状态执行单元 |
| FarmActor | 按 `FARM:farm_id` 串行裁决地块、作物、护理、同场状态和目标侧流程的有状态执行单元 |
| ActorKey | `actor_kind + entity_id`；路由、邮箱与 fencing 的唯一键 |
| ActorAddress | ActorKey 在线路由合同中的表示；客户端不能把其中 kind/shard 当作可信输入 |
| routing v2 | `xxhash64(actor_kind_byte || uint64_be(entity_id)) % 4096`，Player/Farm 使用独立 Assignment |
| PlantIntent | PlayerActor 中记录种子预留与播种流程进度的持久事实 |
| PlantReceipt | FarmActor 中吸收重复 ApplyPlant、记录 APPLIED/REJECTED 的持久回执 |
| FarmCore | Farm / Plot / CropCycle 及版本等农场业务模型 |
| FarmPresenceState | Actor 内可丢失的同场成员、订阅与 Joining buffer |
| Presence | 玩家当前是否在某农场的短期状态 |
| FarmVersion | 农场公开变化的单调业务版本 |
| player_epoch / farm_epoch | 分别防止旧 PlayerActor/FarmActor 或双活写入的所有权代次 |
| shard_revision | 一次 `(actor_kind, virtual_shard_id)` 归属变更的单调代次，用于刷新旧路由 |
| operation_id | 一个业务意图的稳定幂等标识 |
| SessionPermit | 绑定单次操作、会话代次与动作的短期凭证 |
| FriendPermit | 绑定单次操作、好友关系代次与动作的短期凭证 |
| InteractionAdmission | Permit + Presence + 目标领域状态在一个 Actor turn 中共同通过 |
| TheftSettlement | 目标 FarmActor/FarmCore 中记录偷菜跨玩家流程的持久状态机 |
| LootReceipt | 访客 Owner 中证明某次偷菜奖励已到账的幂等事实 |
| PendingOutbox | 与业务状态共同提交、等待异步执行的持久任务 |
| TargetCommitted | 目标产量已扣减且不能回退的偷菜状态 |
| RecordVersion | TcaplusDB 记录版本,用于条件写(CAS) |
| P0a / P0b | 第一版核心可运行雏形 / 补齐题面基础系统 |

## 6. 命名与文件约定

- proto package 按域划分:`common` / `session` / `farm` / `economy` / `social` / `realtime` / `settlement`。
- proto 文件 go_package 统一挂 `farmgame/api/gen/<域>`(生成代码目录,D2 建立)。
- Go 包名小写单词；玩家与农场领域分别置于 `internal/logic/actor/player/<模块>`、`internal/logic/actor/farm/<模块>`，共享 Runtime 放在 `internal/logic/actor/runtime`。
- Redis key 规范见 D5;Tcaplus 表定义规范见 D4。

## 7. 阅读顺序

D1(本文) → [D1.13 双 Actor 决策](./16-player-farm-actor-split.md) → [D1.5 系统骨架](./05-system-skeleton.md) → [D1.6 业务事实分类](./06-business-fact-classification.md) → [D1.9 S2 协议确认](./09-s2-v02-protocol-checklist.md) → D2 协议信封 → D3 帧编码 → D4 PlayerCore/FarmCore → D5 Redis → D6 接口与时序 → D7 Runtime、真库验证与[性能演进门](./52-actor-runtime-performance-evolution.md)。
