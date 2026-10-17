# 06 · 四类业务事实与跨权威流程

版本：v0.4
更新时间：2026-07-28
状态：按统一架构 v1.7 回改。本文先于 Proto 阅读，用来决定字段归属、成功边界、持久化策略、缓存、协调与失败恢复。

关联文档：

- [公共前提](./00-overview.md)
- [系统骨架](./05-system-skeleton.md)
- [S2 协议确认清单](./09-s2-v02-protocol-checklist.md)
- [协议合同](./10-protocol.md)
- [Player/Farm Actor 拆分](./16-player-farm-actor-split.md)
- [PlayerCore/FarmCore](./20-ownercore.md)
- [Redis](./30-redis.md)

---

## 0. 三个维度不能混在一起

本文分别回答：

```text
事实类别 A/B/C/D
→ 这份状态是什么、谁拥有、怎样恢复

协调模式 L/W
→ 一次操作怎样跨权威边界完成

持久化策略 P-ASYNC/P-DURABLE/P-RUNTIME
→ 该命令在什么边界可以确认成功或步骤完成
```

`W` 不是第五类事实。PlantIntent、PlantReceipt、偷菜目标扣减、Settlement、Outbox、玩家库存和 Receipt 本身仍是 A 类 Actor 实体事实，只是共同参与 W 类跨权威流程。

必须继续区分：

```text
Actor 内是否线性一致
≠ 是否在响应前可靠持久化
≠ 提交后的广播、缓存和投影是否异步
```

例如普通金币购买在 PlayerActor 内必须原子串行，但允许异步回写 Tcaplus；同一个 Wallet 参与偷菜罚金、唯一领奖或 Plant Reservation 时，则必须和 Receipt/Reservation 一起可靠持久化后 ACK。FarmChange 不进入权威存储，但产生它的地块事实不能丢失。

---

## 1. 第一维：四类业务事实

| 分类 | 含义 | 成功与恢复 |
|---|---|---|
| A：Actor 实体权威事实 | 由一个 PlayerActor 或 FarmActor 串行裁决，影响资产、农场、唯一领取或可靠进度 | 再按命令选择 P-ASYNC 或 P-DURABLE；不能只凭 A 推导同步 CAS |
| B：独立持久强事实 | 不属于 PlayerCore/FarmCore，由独立 Authority 裁决 | 对应 Tcaplus 权威表条件写成功 |
| C：短期运行态权威 | 绑定连接、Actor activation、订阅或 authority generation；不承载长期资产 | 当前运行期内必须校验；故障后可整体失效并重建 |
| D：可重建派生事实 | 可由 A/B/C 重新计算，不阻塞源业务成功 | 按恢复目标重新拉取、重建、对账或允许有限丢失 |

### 1.1 持久化策略

| 策略 | 成功边界 | 示例 |
|---|---|---|
| P-ASYNC | Actor 内存事务已原子提交；随后 DirtyModel 异步回写 | 普通金币 Buy/Sell、普通库存和 ShopCounter |
| P-DURABLE | 状态与必要 Receipt/Order/Outbox 已可靠持久化 | Farm 写、Plant/Harvest/Theft 参与步骤、唯一领取、付费 |
| P-RUNTIME | 只承诺当前运行态协议边界 | Session、Presence、JoiningBuffer |

P-ASYNC 接受 Player Pod 在回写前硬崩溃时回退至最近 Tcaplus 版本；队列不是 WAL。P-DURABLE 不接受该语义。

### 1.2 C 类不是“不权威”

Presence 和 Session 虽然可在故障后失效，但正常运行期间参与准入：

```text
没有有效 SessionPermit
→ 不能发起新的写操作

没有有效 Presence
→ 不能执行需要同场准入的浇水或偷菜
```

它们是短期运行态权威，但不是金币、作物和库存的最终裁判。最终领域结果仍由目标 FarmActor 或 PlayerActor 结合 Friendship、Presence 和自身领域状态共同决定。

### 1.3 D 类按恢复目标细分

| 子类 | 含义 | 示例 | 失败恢复 |
|---|---|---|---|
| D0：读取时派生 | 不保存，读取时由强事实直接计算 | 当前成熟阶段、主人可收获量 | 重新计算 |
| D1：实时可失效传播 | 服务当前视图，不要求持久重放 | FarmChange、在线提示、普通通知 | 使订阅失效并重新拉取 |
| D2：最终收敛投影 | 可延迟，但不能永久错误 | 公开摘要、排行榜、图鉴展示索引 | Outbox/可重放流/全量重建/对账至少一种 |
| D3：允许有限丢失分析 | 不影响业务与用户资产 | 调试埋点、性能样本、非结算统计 | 允许有限丢失 |

`D` 不参与源操作成功，不等于可以随意丢弃。

---

## 2. 第二维：协调模式

| 模式 | 含义 | 示例 |
|---|---|---|
| L：单权威边界内完成 | 一次命令只需要一个 Actor 或一个独立 Authority 裁决 | 购买、出售、任务领奖、好友关系写 |
| W：跨权威边界可靠流程 | 横跨多个 A/B 权威边界，无法一次事务完成 | 播种、收获、护理奖励、偷菜、跨玩家邀请奖励 |

W 的每个不可逆参与者步骤仍然使用 P-DURABLE，只是步骤之间异步推进：

```text
起点 Actor 本地 CAS
→ 持久流程进度
→ 异步调用目标 Actor
→ 目标 Actor 本地 CAS
→ 持久回执
→ 起点流程闭合
```

---

## 3. 极简决策树

```text
丢失后会不会改变资产、作物、权限、机会或唯一领取？
├─ 会
│  ├─ 属于某个 Player/Farm 实体？
│  │  ├─ 是 → A，由对应 Actor 拥有
│  │  └─ 否 → B，由独立 Authority 拥有
│  └─ 本次操作是否跨多个权威边界？
│     ├─ 是 → 同时标记 W
│     └─ 否 → L
└─ 不会
   ├─ 是否绑定当前连接、激活、订阅或 authority generation？
   │  └─ 是 → C
   └─ 是否能从 A/B/C 重建？
      └─ 是 → D，再按 D0/D1/D2/D3 定恢复目标
```

若归为 A，还必须继续判断：

```text
是否只是单 PlayerActor 内普通软资产事务？
├─ 是 → P-ASYNC，可接受已定义硬崩溃 RPO
└─ 否：Farm写/唯一领取/付费/跨Actor步骤/转账
   → P-DURABLE
```

后续增加订单、签到、活动积分、宠物外观或好友在线状态时，都必须先经过该决策树。

---

## 4. 冻结分类表

| 业务事实 | 事实类别 | 常见协调/策略 | 权威裁决者 | 持久来源 | Redis 用途 | 成功与恢复 |
|---|---|---|---|---|---|---|
| 金币 Wallet | A | L 普通经济=P-ASYNC；W/转账/领取=P-DURABLE | PlayerActor | PlayerEconomyModel | 不缓存权威余额 | Actor 内原子；普通异步回写，可靠参与步骤强制持久化 |
| 库存 Inventory | A | L 普通经济=P-ASYNC；W/领取=P-DURABLE | PlayerActor | PlayerEconomyModel | 不缓存权威库存 | Actor 内原子；与 Wallet/ShopCounter 同一物理组 |
| ShopCounter/普通购买回执 | A | L/P-ASYNC | PlayerActor | PlayerEconomyModel | 不作权威缓存 | 与本次普通经济状态一起回写或一起回退 |
| 种子预留/PlantIntent | A | W/P-DURABLE | PlayerActor | PlayerCore | 不缓存 | 与 reserved_quantity/Outbox 同可靠写；直到终态不清理 |
| 等级/经验 | A | 普通成长=P-ASYNC；跨Actor奖励=P-DURABLE | PlayerActor | PlayerEconomyModel | 可缓存公开摘要 | 来源版本回写成功后才更新公开投影 |
| 地块 Plot | A | L/W 的农场步骤 | FarmActor | FarmCore | 可缓存公开摘要 | FarmCore CAS |
| 作物周期 CropCycle | A | W/L 的农场步骤 | FarmActor | FarmCore | 不作权威缓存 | FarmCore CAS |
| PlantReceipt | A | W 农场回执 | FarmActor | FarmCore | 不需要 | 吸收重复 ApplyPlant；APPLIED/REJECTED 为终态 |
| CareIncident | A | W/L 的农场步骤 | FarmActor | FarmCore | 可缓存公开摘要 | 播种时生成；解决时与 FarmVersion/Result/RewardOutbox 共同 CAS |
| 当前护理阶段 | D0 | L | 目标 Actor 计算 | 不单独保存 | 摘要可携带 appear_at | 由 appear_at/resolved_at 与服务器时间派生 |
| `mature_at` | A | W 的农场步骤 | FarmActor 首次 ApplyPlant 时落地 | FarmCore | 不需要 | 与 CropCycle/PlantReceipt 同 CAS |
| 当前成熟阶段/主人可收获量 | D0 | L | 目标 Actor 计算 | 不单独保存 | 不需要 | 按服务器时间和 CropCycle 重算 |
| FarmVersion | A | L/W 的农场步骤 | FarmActor | FarmCore | 摘要可携带 | 与农场变化共同 CAS |
| TaskClaim/唯一领奖 | A | L/P-DURABLE | PlayerActor | PlayerCore | 不作权威缓存 | claimed 与奖励同一次可靠写 |
| 不可重建任务计数 | A | L/W 的玩家步骤 | PlayerActor | P0 存 PlayerCore | 不作权威缓存 | 本地触发同 CAS；跨 Farm 触发用 Receipt |
| 可由当前强事实推导的任务条件 | D0 | L | PlayerActor 计算 | 不重复保存 | 不需要 | 领取时重新校验 |
| 邮件附件领取记录 | A | L/P-DURABLE | PlayerActor | PlayerCore | 不作权威缓存 | 领取标记与奖励同一次可靠写 |
| 宠物拥有/守护配置 | A | L/W 的规则输入 | PlayerActor | PlayerCore | 不作权威缓存 | PlayerCore CAS |
| 宠物公开展示摘要 | D2 | L | 来源为 PlayerCore | 可重建投影 | Redis 可缓存 | 异步收敛/全量重建 |
| AlbumUnlockFact | A | L | PlayerActor | PlayerCore | 不作权威缓存 | 与触发操作共同 CAS |
| AlbumDisplayProjection | D2 | L | 来源为解锁事实 | 可重建投影 | Redis 可缓存 | 异步收敛/重建 |
| 普通 OperationResult | A | 随源命令策略 | 入口权威 Actor | PlayerCore/FarmCore 热窗口或归档 | 可短期缓存 | 普通本地购买与经济快照同组回写；W 终态可靠闭合 |
| 好友关系 | B | L | FriendshipAuthority | Friendship 表 | 好友摘要缓存 | 条件写；权限缓存失效后回源 |
| 分享接受事实 | B | L/W 的起点 | ShareAuthority | ShareAcceptance 表 | 分享链接缓存 | 唯一接受条件写 |
| 规则配置版本 | B | L | ConfigAuthority | 版本化配置表 | 只读缓存 | 配置发布成功 |
| 偷菜目标扣减 | A | W 农场步骤 | 目标 FarmActor | FarmCore | 不作权威缓存 | 与 Settlement/Outbox 同一次 CAS |
| TheftSettlement | A | W 流程进度 | 目标 FarmActor | FarmCore/终态归档 | 可缓存查询 | 是已接受偷菜的唯一可变结果源 |
| PendingOutbox | A | W 可靠驱动 | 创建它的 PlayerActor/FarmActor | 对应 Core | 不作权威缓存 | 与本地业务步骤同一次 CAS |
| PendingWork 发现索引 | D2/运行提示 | W 工作发现 | 来源为 PendingOutbox | 可重建索引/平台能力 | Redis 可作提示但非唯一来源 | 禁止全表扫描 Core；必须可分页分片并有遗漏补偿/对账 |
| 访客库存到账 | A | W/P-DURABLE | 访客 PlayerActor | PlayerCore | 不作权威缓存 | 与 LootReceipt 同一次可靠写 |
| LootReceipt | A | W 幂等回执 | 访客 PlayerActor | PlayerCore | 不需要 | 吸收重复 GrantLoot |
| CareRewardReceipt | A | W 操作者奖励步骤 | 操作者 PlayerActor | PlayerCore | 不需要 | 与经验/任务进度共同 CAS，吸收重复 GrantCareReward |
| Presence | C | L 准入 | 目标 FarmActor | Actor 内存 | 可选在线提示 | Joining/InFarm 状态机；失效后重 Join |
| JoiningBuffer | C | L 同步屏障 | 目标 FarmActor | Actor 内存 | 不使用 | 保证 Snapshot 与后续变化同订阅 FIFO |
| WSS 连接与传输索引 | C | L | TCONND / gateproxy | 接入容器内存 | transport connection binding | 断线后重连 |
| 农场应用连接绑定 | C | L | farm-gate | farm-gate 内存 | connection_id + generation | 断线后重新 ClientHello/Join |
| Active Session/Permit | C，fail-closed | L 准入 | SessionAuthority | Redis 短期状态 | 主要运行存储 | 当前代次必须有效；丢失后整体失效重登 |
| SessionAuthorityGeneration | B | L | SessionAuthority | 独立持久/协调来源 | Redis 仅缓存 | 全量 Redis 故障时提升，防旧 Permit 复活；S2 使用固定值 |
| FarmChange | D1 | L/W 提交后传播 | 已提交状态派生 | 不单独持久化 | 通常不需要 | 无法连续投递时使订阅失效 |
| 公开农场摘要 | D2 | L | 来源为 FarmCore | 可重建投影 | Redis | 保存 source_farm_version 与时间计划；异步收敛/TTL/对账重建 |
| 玩家公开档案 PlayerPublicProfile | D2 | L | 来源为 UIC + 已保存 Player 版本 | query-svr 投影表 | Redis 批量缓存 | 只含白名单字段；来源保存成功后更新，登录/对账修复 |
| 排行榜 | D2 | L | 来源为等级/积分 | 原始事实在 PlayerCore | Redis ZSet 等 | P1，异步收敛/重建 |
| 成熟提醒 | D2 | L | 来源为 `mature_at` | 可选提醒意图 | 可用于去重 | P1，失败不影响成熟 |
| 行为统计 | D3 | L/W 提交后记录 | 日志/分析系统 | 日志系统 | 可缓冲 | 允许有限丢失 |

---

## 5. Presence 与 Join 成功边界

必须区分三个时刻：

| 语义 | 服务端边界 |
|---|---|
| Join 请求已受理 | Actor turn 创建 Joining、版本屏障和有界 Buffer，Snapshot 进入本地 DownlinkDispatcher |
| 服务端允许互动 | farm-gate QueueAccepted；Catch-up 批次已按同订阅 FIFO 入队；Presence 转为 InFarm |
| 浏览器已看到快照 | H5 收到并应用 FarmSnapshot；这是客户端本地状态，不是服务端提交点 |

协议映射：

```text
JoinFarm ExecuteCommandResponse = ACCEPTED
→ 服务端仍处于 Joining
→ H5 仍处于 Joining，禁止经营操作

farm-gate QueueAccepted
→ 服务端 Joining → InFarm

H5 应用 FarmSnapshot
→ 客户端 Joining → InFarm
```

`ACCEPTED` 不表示 Join 完成，也不承诺跨 Logic 重启恢复。Join 不创建持久 OperationResult；Snapshot 超时后客户端重新认证并使用新的 request_id Join。

---

## 6. FarmChange 与 JoiningBuffer

FarmChange 产生顺序：

```text
FarmCore CAS 成功
→ Actor 替换热状态
→ FarmVersion 已成立
→ 派生 FarmChange
→ 异步下行
```

FarmChange 不进入权威存储，但不能静默丢失后继续发送后续版本：

```text
客户端已应用 V100
→ V101 投递失败
→ 禁止继续投递 V102
→ 标记 subscription stale
→ 尽量发送 ResyncRequired
→ 控制消息也无法入队则断开
→ 客户端重新 Join
```

JoiningBuffer 只暂存 Join 窗口内 `version > snapshot_version` 的变化，确保 Snapshot(V) 与后续变化形成同订阅 FIFO。Buffer 溢出、超时或无法保证顺序时，本次订阅必须失效，不允许静默进入 InFarm。

---

## 7. PlayerCore/FarmCore 不复制到 Redis

```text
活跃 Player/Farm
→ 对应 PlayerActor/FarmActor 热状态就是运行时缓存

冷实体
→ 从 TcaplusDB 加载
```

不把 Wallet、Inventory 或完整 FarmCore 再复制到 Redis，避免 Actor 内存、Redis、Tcaplus 三份权威错觉。活跃 PlayerActor 的 P-ASYNC 普通软资产可能领先 Tcaplus，业务查询必须命中 PlayerActor；冷启动以 Tcaplus 最近保存版本恢复。Redis 只保存 Session、连接绑定、限流、短期查询缓存和 D2 投影。

---

## 8. 播种跨 Actor 提交与任务语义

播种不再是一次 OwnerCore CAS，而是三个局部原子步骤：

```text
PlayerCore CAS：
  reserved_quantity +1
  + PlantIntent(RESERVED)
  + ApplyPlantOutbox
  + OperationResult(PROCESSING)

FarmCore CAS：
  创建 CropCycle/CareIncident
  + Plot/FarmVersion
  + PlantReceipt(APPLIED)
  + OutcomeOutbox

PlayerCore CAS：
  quantity -1
  + reserved_quantity -1
  + PlantIntent(COMPLETED)
  + OperationResult(SUCCEEDED)
```

Farm 永久拒绝时，最后一步改为释放 `reserved_quantity` 并记录 REJECTED。完整状态机见 [16-player-farm-actor-split](./16-player-farm-actor-split.md)。

任务处理按语义选择：

- 唯一领奖：`claimed + Wallet/Inventory 奖励 + OperationResult` 同一次 CAS；
- PlayerActor 本地触发的不可重建计数：与触发操作共同 CAS；
- FarmActor 触发的帮助/收获进度：通过 Outbox + Player Receipt 可靠到账；
- 可由当前强事实推导：不重复写计数，展示/领取时重新计算。

“所有任务进度都同步写”与“所有任务进度都异步投影”都不是统一答案。

---

## 9. 成熟语义

只持久化：

```text
plant_at
mature_at
rule_version
base_yield
stealable_limit
```

当前展示阶段在读取时计算：

```text
now < mature_at  → Growing
now >= mature_at → Mature
```

S2 和首个个人经营 Loop 不实现成熟提醒；P1 可增加 Scheduler/提醒意图。提醒失败、延迟或重复都不能改变作物是否成熟。

---

## 10. 偷菜：Settlement 是已接受流程的唯一可变结果源

### 10.1 资格拒绝

例如非好友、未成熟、已偷过、额度耗尽：

```text
OperationResult {
    operation_id
    status = REJECTED
    error_code
}
```

不创建 Settlement，不扣目标产量，也不新增一次机会消耗。

### 10.2 被宠物拦截

宠物拦截是已经受理并产生副作用的偷菜尝试：

```text
记录该访客本茬机会已消耗
+ TheftSettlement = BLOCKED_BY_PET（终态）
→ 同一次目标 FarmCore CAS
```

不扣目标产量、不发访客奖励。外部结果由 Settlement 映射为：

```text
CommandStatus = SUCCEEDED
TheftOutcome = BLOCKED_BY_PET
```

### 10.3 成功进入跨 Actor 流程

```text
目标作物扣减
+ TheftSettlement = TARGET_COMMITTED
+ PendingOutbox(GRANT_LOOT)
→ 一次目标 FarmCore CAS
→ 对外 PROCESSING
```

推进：

```text
TARGET_COMMITTED
→ 访客 Inventory + LootReceipt
→ GRANTED
→ COMPLETED
```

已接受偷菜不再维护另一份会从 PROCESSING 变化为 SUCCEEDED 的 OperationResult。`TheftSettlement` 保存 operation_id/settlement_id、状态、结果 payload，是唯一可变结果源。

幂等查询顺序：

```text
1. 查终态 OperationResult（资格拒绝等无 Settlement 结果）
2. 查 active/archived TheftSettlement(operation_id)
3. Settlement TARGET_COMMITTED/GRANTED → PROCESSING
4. Settlement BLOCKED_BY_PET/COMPLETED → SUCCEEDED + TheftOutcome
```

---

## 11. 权威事实、热记录与归档

权威事实表示谁能裁决，不自动等同于“每次操作同步持久化”。P-DURABLE 表示可靠恢复要求；P-ASYNC 表示 Actor 内线性一致并接受受控 RPO。二者都不表示所有历史永久驻留 PlayerCore/FarmCore 热记录。

| 数据 | 热记录策略 |
|---|---|
| 当前 Wallet/Inventory/ShopCounter | 活跃时保留在 PlayerActor；PlayerEconomyModel 异步形成恢复快照 |
| 当前 Plot/CropCycle/FarmVersion | 长期保留在 FarmCore |
| 未完成 Settlement/Outbox | 必须保留直到闭合 |
| 近期普通 OperationResult | 热窗口保留 |
| 已完成很久的 Settlement | 终态后归档 |
| 历史 CropCycle | 归档或统计化 |
| 旧完整 OperationResult | 归档，但保留防重墓碑/水位 |

定义：

```text
R_query = 客户端承诺可按 operation_id 查询/重试的窗口
R_hot   = 完整结果在 PlayerCore/FarmCore 热记录中的保留窗口
```

必须满足：

```text
R_hot >= R_query + 故障恢复余量
```

初始建议值仅用于 S2/P0 测试：

```text
R_query = 10 分钟
R_hot   = 30 分钟或最近 64 条，取覆盖更广者
```

清理前必须满足：

- 操作已经终态；
- 相关 Settlement/Outbox 已闭合；
- 完整结果已安全归档，或仍保留足够幂等墓碑；
- 旧 operation_id 不会被当成新业务意图执行。

---

## 12. P0/P1 边界

P0/S2 或核心玩法：

- Player 普通软资产 P-ASYNC 的 DirtyTracker、版本化增量回写、ForceFlush 和积压背压；
- A/B 中 P-DURABLE 事实的可靠提交；
- C 类 Session/Presence/JoiningBuffer 准入；
- D1 FarmChange 失败即使订阅失效；
- W 类偷菜进入时的 Settlement/Outbox/LootReceipt；
- W 类播种的 PlantIntent/PlantReceipt/Outbox 与种子补偿；
- 任务领奖与不可重建计数的正确性。

P1 或只保留设计：

- 成熟提醒；
- 排行榜；
- 大规模 D2 公开摘要投影；
- 完整行为分析管道；
- 任务进度异步化。

---

## 13. 对 Proto 与实现的约束

- A/B 权威事实必须携带稳定幂等键或条件版本，不能只依赖 Redis；
- CommandCatalog 必须显式声明 P-ASYNC、P-DURABLE 或 P-RUNTIME；
- P-ASYNC 的 Wallet/Inventory/ShopCounter 必须通过一个 PlayerEconomyModel 一致性组 Check/Apply，Receipt 与状态一起回写或一起回退；
- P-ASYNC WritebackQueue 是进程内队列；必须暴露 Dirty Age/version gap 并在积压超限时停止新资产写；
- W 是协调属性；每个不可逆 W 本地步骤仍是 A/B 的 P-DURABLE 提交，不能用普通 Dirty ACK；
- C 不进入 PlayerCore/FarmCore，但当前运行期内不得跳过其准入校验；
- D 不作为源业务返回 SUCCEEDED 的前置条件，但必须按 D0/D1/D2/D3 完成对应恢复；
- PlayerPublicProfile 只能在来源 Player `saved_version` 达标后更新，允许落后但不能领先于重启恢复基线；
- FarmChange 投递失败后必须使订阅失效，禁止静默越过版本继续发送；
- Worker 只能推进已持久化的 W 流程；
- 已接受偷菜以 Settlement 为唯一可变结果源；
- Proto 字段归属、CommandStatus 和 RPC 成功语义必须能在本文找到依据。
