# 08 · S2-0 编码准入（Go / No-Go）

版本：v0.3
更新时间：2026-07-27
状态：S1 主体设计完成；当前只执行 S2-0，不进入购买、种植、偷菜等业务功能编码。

本文是从设计进入实现的唯一准入表。`00/05/09/10/16/17/40/41` 说明“系统应该是什么”，本文说明“什么条件满足后才能开始下一步”。具体任务顺序、目录、测试和交付物见 [14-s2-implementation-plan](./14-s2-implementation-plan.md)。

---

## 1. 已冻结，不再发散

| 主题 | 冻结结论 |
|---|---|
| 业务权威 | `PlayerActor(player_id)` 裁决资产/成长，`FarmActor(farm_id)` 裁决地块/作物/Presence；同 ActorKey 串行，不同 ActorKey 并行 |
| 接入边界 | P0：H5/微信小程序 → 公网 DNS → CLB/TLS → farm-gate Direct WebSocket；Upgrade/PRE_AUTH/Ticket/Session/FarmFrame/限流/ConnectionWriter/Fence 均在 farm-gate。TCONND + gateproxy 为不阻塞 P0 的候选 Adapter |
| Actor 路由 | routing v2：`xxhash64(actor_kind_byte || uint64_be(entity_id)) % 4096`；Player/Farm 独立 Assignment，S2 静态映射到本地 Logic |
| 实体 ID | player_id/farm_id 使用 `uint64`，0 非法；H5 禁止用 JavaScript `Number` 承载 |
| 权威存储 | TcaplusDB；Farm/P-DURABLE Player 步骤以条件写确认，普通 Player 软资产采用 Actor 内存原子提交 + DirtyModel 异步增量回写，并接受已确认的硬崩溃 RPO |
| CAS 冲突 | 单 Actor 本地 CAS 最多三次尝试且受 deadline 限制；版本冲突必须 Reload→查幂等→重验→重构造；actor epoch 不匹配立即失去可写权威 |
| 短期状态 | Redis 与 Presence/JoiningBuffer 可重建，不作为金币、库存、地块或关系权威 |
| Join | `Joining + Snapshot(V) + JoiningBuffer`；对应 Snapshot 进入当前连接队列后服务端才转 InFarm |
| C3 Snapshot | 使用外部 View；S2 只下发空农场所需最小 `PlotView`，不暴露 PlayerCore/FarmCore |
| C4 Change | 下发提交后的完整 `PlotView`；按 `farm_version` 连续覆盖，缺口重新 Join |
| C5 Downlink | DeliverBatch 逐 item 返回；同一逻辑投递重试保持 `item_id`，仅匹配 Snapshot 的 QueueAccepted 可确认 Join |
| Bootstrap | 调用方只传 `player_id + bootstrap_id`；AccountBootstrap 幂等创建 PlayerCore/FarmCore，两者 Ready 后才返回 AccountReady |
| Plant（S3） | PlayerActor 预留种子 → FarmActor 幂等种植 → PlayerActor确认消耗；永久拒绝释放预留，暂时失败用原 txn 重试 |
| 浇水规则 | 同一 CropCycle 全局仅第一次有效；其他玩家后续请求均为 `NO_CHANGE + 最新 PlotView` |

---

## 2. S2-0 两条并行工作线

### A. 协议冻结线

1. 把 D1.9 已确认的 C1～C7、双 Actor ID 和双记录 Bootstrap 规则同步到目标 Proto 草案。
2. 把 D1.15 的 routing v2 六个黄金向量写入 Go/TypeScript 共用测试数据。
3. 完成 Direct WebSocket/CLB 的 Idle Timeout、半开连接和前后台切换探针后，冻结
   ServerHello 心跳字段或新增应用层 Heartbeat。
4. 检查字段号、`reserved`、错误码、H5 64 位 ID 表示和跨语言 round-trip。
5. 通过评审后才修改 `api/proto`、重新生成代码。

### B. 外部依赖探针线

1. Direct WebSocket/CLB：确认 Upgrade、binary message 边界、Origin、PRE_AUTH 上限、
   QueueAccepted、慢读/队列满、Idle Timeout、应用心跳和浏览器前后台切换语义。
2. Tcaplus：取得 zone id、建立 Player/Farm probe 表，分别完成 Create/Get、RecordVersion
   CAS、结果未知恢复和 P95/P99 延迟测试。
3. TCONND/gateproxy T1～T8：作为不阻塞 P0 的候选 Profile，验证 BY_WS、发送确认、
   TBUS/IPC、心跳与真实部署条件；通过后也只能替换 Transport Adapter。

两条线可以并行；任何一条没有证据时，都使用文档中的保守失败语义，不凭经验假定平台行为。

---

## 3. 阶段准入

### S2-0 → S2-1：允许修改最小 Proto

必须全部满足：

- C1～C7、player_id/farm_id、ActorKey、routing v2 黄金向量、双记录 Bootstrap 字段与语义在 D1.9/D1.15 中标记“已冻结”；
- Direct WebSocket/CLB 探针已决定是否需要应用层 Heartbeat；
- Proto 字段号/名称兼容检查完成；
- H5 `uint64` round-trip 小实验通过；
- 本轮 Proto 范围仍只覆盖 Login/Hello/Join/空 FarmSnapshot/Resync/Downlink。

### S2-1 → S2-2：允许实现登录与双记录建档

必须全部满足：

- 最小 Proto 生成成功，构建与协议测试通过；
- Bootstrap 的并发/中途崩溃测试证明 PlayerCore/FarmCore 各一条、初始资产只发一次；
- MemoryPlayerStore/MemoryFarmStore 只用于骨架和单测，并真实模拟 CAS 冲突；
- Tcaplus 表未就绪时不得把内存实现称为最终验收。

### S2-2 → S2-3：允许实现真实接入与 Join

必须全部满足：

- Direct WebSocket 的 Upgrade/PRE_AUTH、binary message 边界、Origin、慢读、断线和
  ConnectionWriter 状态映射有实测证据；
- ClientHello ticket 原子核销、顶号和旧连接拒写通过；
- farm-gate ConnectionWriter 有界且能返回逐 item 状态；
- LogicIngress → ActorRouteAdapter → 本地 PlayerActor/FarmActor 链路通过；
- PlayerRegion/FarmRegion 各 4096 个静态 Assignment 指向本地 Logic；
- `PLAYER:X` 与 `FARM:X` 的 ActorKey、邮箱和 Store 不混用。

### S2 完成：允许进入个人经营 Loop

必须全部满足：

- Direct WebSocket/CLB 的顺序、最大帧、滚动重启、Idle Timeout、心跳和半开连接探针通过；
- Join 无缺口：Snapshot(V) 后能收到 JoiningBuffer 中的 V+1...；
- 重复 Join、Snapshot 排队失败、QueueFull、旧 connection generation、Buffer 溢出均按合同恢复；
- Tcaplus Create/Get/CAS/结果未知恢复与延迟测试完成；
- H5 可登录、Join 自己的空农场、断线重连并重新收敛到权威 Snapshot。

---

## 4. S2 禁止顺手加入

- 购买、出售、播种、成熟、收获、浇水、偷菜；
- 任务、邮件、宠物、图鉴、排行榜、成熟提醒；
- 多 Logic ShardLease、Actor 迁移、Proxyless Mesh；
- 任意帧 ACK/重放、复杂 Snapshot 分片；
- 仅为“以后可能需要”而提前引入新的进程或数据库。

这些能力不是取消，而是在纵向骨架验收后按 Buy → Plant Saga → Harvest Saga → 社交 Loop → 跨玩家流程逐层加入。

---

## 5. 当前结论

当前是 **No-Go for 修改最终 Proto，Go for S2-0 探针与可逆的本地测试骨架**。

解除 No-Go 的实际阻塞项只有：

1. Direct WebSocket/CLB 心跳、Idle Timeout 和半开连接结论；
2. player_id/farm_id 的 H5 `uint64` round-trip；
3. D1.9 字段兼容复核；
4. Direct WebSocket 的 Upgrade、PRE_AUTH、消息边界、慢读和 ConnectionWriter 探针；
5. Tcaplus zone/table 为 S2 最终存储验收提供证据。

TCONND/gateproxy 平台资产已从 `mfcn/gatesvr`/`mfcn/gateproxy` 定位，但 BY_WS 配置、
兼容镜像 tag 和真实 GateStream 行为仍未验证；它们不再阻塞 P0。候选专项步骤见
[15-tconnd-gateproxy-integration-plan](./15-tconnd-gateproxy-integration-plan.md)。
