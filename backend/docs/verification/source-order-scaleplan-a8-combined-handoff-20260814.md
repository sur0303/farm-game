# A8 汇合分支完整交接：SourceOrder、生产 Worker、ScalePlan、支撑系统、Bot 与 Redis Cluster

日期：2026-08-14

仓库：`developer_001@example.invalid:developer_026/farm-game.git`

原 A8 汇合分支：`codex/source-order-scaleplan-a8-20260814`

当前接续分支：`codex/source-order-worker-production-r1`

原 A8 文档基线：`452799d docs: normalize A8 handoff formatting`

当前功能提交：`b6b23dec757bb63afa67c2137e3de54bc477b0fb feat(worker): partition source-order recovery by route assignment`

状态：A8 功能代码与 SourceOrder Worker 生产分片收敛均已提交并推送；尚未合入 `Integration`；尚未完成真实多 Worker、K8s 扩缩容与三主三从 Redis 故障验收。

---

## 0. 接手人先读

### 0.1 一句话结论

这个分支已经把以下几条原本并行开发的主线收进同一棵后端代码树：

1. 好友机会、动态 Actor 路由与容量基线；
2. Plant、UnlockPlot、Harvest、Care、Theft 的 SourceOrder 一致性链路；
3. 任务、图鉴、邮件、宠物、商店狗粮和红点等最小支撑系统；
4. `live-farm-bot`、`mixed-farm-bot` 及相关真实/离线压测工具；
5. K8s Informer + HolderControl + RouteControl 的自动扩容、计划缩容和 Holder 故障接管；
6. 三主三从 Redis Cluster 的客户端、配置、同槽 Lua 和运维工具适配；
7. SourceOrder Worker 按 `FARM_WORKER` 路由归属扫描、迁移 Fence、有界轮转、空闲退避和生产监控。

它目前是一个“功能汇合候选分支”，不是“已经完成生产验收的发布分支”。

### 0.2 状态分级

| 能力 | 代码状态 | 已有证据 | 仍缺什么 |
|---|---|---|---|
| Plant / UnlockPlot / Harvest / Care / Theft SourceOrder | 已实现 | 单测、全仓测试、真实四服务 WSS smoke | 进程重启与容量复测 |
| SourceOrder Worker 生产分片恢复 | 已实现 | Worker 专项单测、`go vet`、路由 revision/Fence 单测 | Linux race、真实多 Worker Rebalance/宕机接管、Redis workload 复测 |
| Buy / Sell | 已实现 | 单 Actor 测试与 SourceOrder WSS 支撑场景 | 大数量买卖与价格版本专项容量数据 |
| 任务 | P0 最小闭环已实现 | 真实 WSS 验证每日任务读取/领取链路 | 清理 legacy Task 写入；补更多配置 |
| 图鉴 | P0 最小闭环已实现 | 真实 WSS 验证自种自收后解锁 | 活动档案完整运营配置与专项 E2E |
| 邮件 | P0 最小闭环已实现 | 欢迎邮件、列表、读取、附件领取代码和测试 | 可靠 Worker 默认启用、运营/全服发信 |
| 宠物 | 基础守护链已实现 | 解锁、切换、喂食及 GuardSync 测试 | 守护拦截偷菜的独立真实 WSS 验收 |
| 红点 | 任务/邮件/图鉴已实现 | FeatureSummary 测试 | 宠物红点尚无正式服务端协议字段 |
| 实时好友机会 | 已从基线继承 | 既有 300 好友与真实链路报告 | 本汇合提交后未重新跑完整容量基准 |
| `live-farm-bot` | 已实现真实 HTTP/WSS 执行 | 单测及既有真实路径使用 | 用本汇合镜像重跑长稳容量场景 |
| `mixed-farm-bot` | 只实现 dry-run | 确定性计划与夹具测试 | 尚未授权真实网络执行，不能当真实压测结果 |
| Pod 自动扩容与 Shard Rebalance | 已实现 | 单测、race、K8s render 14 项 | 真实 StatefulSet `2→3` 验收 |
| Pod 计划缩容 | 已实现 | 状态机、组合与 render 测试 | 真实 StatefulSet `3→2` 验收 |
| Holder 宕机自动接管 | 已实现 | 组合故障测试、真实 Redis Fence 探针 | 真实删除 Pod + H5 重连故障注入 |
| Redis 三主三从 Cluster | 已完成代码适配 | 全仓测试、同槽/CROSSSLOT 测试、配置解析 | 真实拓扑、主从切换、Slot 迁移和 RPO/RTO |

### 0.3 绝对不要混淆的四件事

1. **K8s Pod 发现不等于 Shard 写权迁移。** Informer 只提供 Pod 事实并唤醒控制器，只有 RouteControl 可以修改 Shard Assignment、Lease 与 `writer_epoch`。
2. **Redis Cluster Slot 迁移不等于 Actor Shard 迁移。** 前者由 Redis 客户端处理 MOVED/ASK，后者由 RouteControl 处理 Holder 迁移。
3. **代码支持扩缩容不等于 HPA 已完成。** 当前控制器会在目标副本变化后自动编排 Shard，但不会根据 CPU/QPS 自行决定需要几个 Pod。
4. **支撑系统最小闭环不等于完整运营平台。** 任务、邮件、图鉴和宠物已有可运行主链，但配置规模、运营入口和全量真实场景仍需继续补齐。

---

## 1. 分支来源与边界

### 1.1 汇合关系

```text
friend-opportunity / route / capacity 基线 af8f11d
    │
    ├─ SourceOrder 公网切流
    ├─ Care / Theft 真实 WSS 证明
    ├─ 任务 / 图鉴 / 邮件 / 宠物 / 商店 / Bot 移植
    └─ Redis 热路径 IO 瘦身
           │
           ├─ K8s Holder Discovery
           ├─ ScalePlan 自动 Rebalance
           ├─ Holder 自动故障接管
           └─ Redis Cluster 适配
                  │
                  ▼
             A8：a7651c5
                  │
                  ├─ A8 文档基线：452799d
                  └─ SourceOrder Worker 生产分片收敛
                         │
                         ▼
                    b6b23de
```

SourceOrder 主要提交：

| 提交 | 内容 |
|---|---|
| `4f16fcf` | 公网 Plant/Unlock/Harvest/Care/Theft 源端切到 SourceOrder |
| `2826e26` | Care/Theft 真实 WSS 链路证明 |
| `aed88ce` | 支撑经济、任务、图鉴、邮件、宠物、GuardSync、Bot 移植 |
| `f7ad8bd` | 同一 WSS 链验证欢迎邮件、日任务、图鉴、卖菜、宠物解锁与喂食 |
| `c6fd34c` | 稀疏 Actor 写入、SourceOrder 热路径 Redis IO 瘦身、Theft WorkRecord 退役 |
| `a7651c5` | 汇入 ScalePlan、自动故障恢复并完成 Redis Cluster 适配 |
| `452799d` | 规范化 A8 总交接文档，作为本轮 Worker 生产化起点 |
| `b6b23de` | SourceOrder Worker 按路由归属扫描，增加有界轮转、迁移 Fence、退避与指标 |

### 1.2 仓库边界

- 本文只描述 `farm-game` 后端分支。
- H5 好友弹窗、定向 Hint 刷新和页面交互仍在 `classic-farm-h5` 的独立分支演进，不包含在本提交中。
- 当前分支没有合入 `Integration`，不能把 `Integration` 的部署结果当作本分支验收证据。
- 旧 `{work-v1}` 适配代码仍有部分保留用于兼容或测试，但生产接线不再运行 Plant/Unlock/Harvest/Theft dest 的旧状态机。

---

## 2. 当前整体运行结构

```text
H5 / live-farm-bot
      │ HTTPS 登录 + WSS FarmFrame
      ▼
farm-gate
      │ CommandCatalog + Session + RouteSnapshot
      ├─────────────────────────┐
      ▼                         ▼
player-svr                  farm-svr
PlayerActor                 FarmActor
任务/资产/图鉴/宠物/邮件领取  地块/作物/护理/偷菜/守卫快照
      │                         │
      └──── SourceOrder ────────┘
                 │
                 ▼
             farm-worker
      FARM_WORKER RouteAssignment
            ↓
      只扫描本实例拥有的 SourceDueIndex Bucket
            ↓
      有界轮转、WorkLease、恢复和补投

Kubernetes Pod Informer
      │ PodUID / Ready / ordinal
      ▼
HolderControl 精确握手
      │ domain / boot_id / process_ready / shard count
      ▼
VerifiedHolderView（可重建内存视图）
      │
      ▼
route-controller
      ├─ AutoRebalance：健康拓扑扩缩容
      ├─ AutoFailover：Holder 故障接管
      ├─ RoutePlan / ScalePlan 持久状态机
      └─ Assignment / Lease / writer_epoch

所有核心进程
      │ redis.UniversalClient
      ▼
三主三从 Redis Cluster
Authority / Runtime / Projection 各自按 Hash Tag 路由
```

Holder 不再主动注册一套重复的服务发现。Kubernetes 负责发现基础设施实例，HolderControl 负责证明游戏进程代次，RouteControl 负责业务 Shard 写权。

---

## 3. SourceOrder 核心业务链

### 3.1 为什么从通用 WorkRecord 切到 SourceOrder

旧链路会为每个跨 Actor 命令额外维护一张完整 `{work-v1}` WorkRecord，并在每个步骤更新状态。当前链路把“来源 Actor 事实 + Participant Receipt + SourceOrder + DueIndex”放进来源 Actor 同一次同槽 Lua 提交：

```text
Source Actor Commit
→ 改来源权威模型
→ 写幂等 Receipt
→ 创建 SourceOrder
→ 写入 DueIndex
→ 一次 Lua 原子完成
```

随后优先由进程内 DirectDispatcher 执行第二跳；若进程退出、RPC 超时或目标暂时不可用，`farm-worker` 从 DueIndex 重新领取并恢复。

这没有把 PlayerActor 与 FarmActor 合并，也没有把它们强行放到同一 Redis Slot。

### 3.2 各场景当前路径

| 场景 | Source Actor | 同次提交内容 | 第二跳 | Worker 扫描 |
|---|---|---|---|---|
| Plant | PlayerActor | 扣种子、Receipt、PLANT Order、Player DueIndex | FarmActor 下种 | PLAYER SourceDueIndex |
| UnlockPlot | PlayerActor | 扣金币、Receipt、UNLOCK Order、Player DueIndex | FarmActor 解锁地块 | PLAYER SourceDueIndex |
| Harvest | FarmActor | 清空作物、Receipt、HARVEST/GRANT Order、Farm DueIndex | PlayerActor 作物入包 | FARM SourceDueIndex |
| Care | FarmActor | Incident 终态、Receipt、CARE_REWARD Order、Farm DueIndex | PlayerActor 给访问者经验 | FARM SourceDueIndex；Pair 结果另处理 |
| Theft | FarmActor | 扣减可偷产物、Receipt、THEFT_VISITOR/可选 THEFT_GUARD Order、Farm DueIndex | 访问者入包、主人守卫收益 | FARM SourceDueIndex；Pair admission 独立恢复 |
| Buy/Sell | PlayerActor | 资产与 Receipt | 无 | 无跨 Actor Order |

好友 Care/Theft 的“能不能操作”仍由 canonical FriendRelation/Pair admission 判断，SourceOrder 只负责业务事实提交后的可靠第二跳。

### 3.3 热路径 Redis 瘦身

- `CommitActorMutationWithOrdersV10` 只传发生变化的 Model Field，不再把完整 CandidateState 塞进 Lua ARGV。
- 生产固定使用 Model Field v2 稀疏写入。
- PENDING SourceOrder 不再写空闲 Lease 字段。
- Complete 后 `HDEL` Lease 字段。
- Direct 成功路径使用 `ClaimAndCompleteWorkV1`，将 Claim + Complete 从两次 EVAL 收成一次。
- Theft dest 不再创建通用 WorkRecord。
- Worker 恢复仍保持 Claim → RPC → Complete，因为 RPC 期间必须持有 Lease。

### 3.4 SourceOrder Worker 生产分片与扫描合同

本轮把 Plant、UnlockPlot、Harvest、CareReward、TheftVisitor、TheftGuard 六类恢复 Worker 从“每个实例可能扫描全部 Bucket”收敛为“每个实例只扫描路由分配给自己的 Bucket”。机会唤醒和 SourceOrder 恢复共用一个进程级 `farmWorkerShardRuntime`，不会重复创建 Assignment Watcher 或 Authority Renewer。

```text
RouteControl 发布 FARM_WORKER Assignment(revision=R)
→ farm-worker 原子替换本地 AssignmentSnapshot
→ 计算当前实例拥有的 FARM_WORKER Shard
→ 映射到可扫描的 PLAYER/FARM Source Bucket
→ 每轮只从这些 Bucket 的 SourceDueIndex 领取到期 Order
```

当前映射规则：

- FARM Source Shard 与 FARM_WORKER Shard 数相同时按同号一对一映射；
- PLAYER Source Shard 使用稳定的 `source_shard % farm_worker_shard_count` 映射；
- 静态开发环境可显式配置 `FARM_WORKER_OWNED_SHARDS`；生产动态环境必须使用 `FARM_WORKER_ASSIGNMENT_MODE=route`。

每轮恢复固定使用同一个 Assignment revision，并在两个位置重新校验归属：

```text
扫描某个Bucket前校验一次
→ 读取到期Order
→ WorkLease Claim前再次校验
```

路由在二者之间变化时，旧 Worker 不再领取新 Order。已经成功 Claim 的 Order 可以按照现有 Lease 与 Participant Receipt 幂等语义完成；尚未 Claim 的 Order 仍保留在 DueIndex，由新 Owner 接管，不会被丢弃。

扫描与退避规则：

- `SOURCE_ORDER_WORKER_BUCKETS_PER_ROUND=256`：每轮最多扫描 256 个当前归属 Bucket，并保存轮转游标；
- `WORKER_POLL_INTERVAL=1s`：生产候选基础周期；
- 只有完整走完当前全部归属 Bucket 且确认没有到期工作时，才执行 `1s → 2s → 4s` 空闲退避；
- 只完成一段轮转批次不能据此退避，避免后半段 Bucket 中已有到期任务却被延迟；
- `SOURCE_ORDER_DUE_OBSERVE_INTERVAL=10s`：Readiness/Backlog 复用有界归属扫描结果；Assignment revision 变化会立即使缓存失效。

本轮没有引入全局 `KEYS`/`SCAN`，也没有增加非空 Bucket 目录。DueIndex 仍然是同 Authority Bucket 内的 ZSET，Worker 只是改变“谁扫描、每轮扫多少、空闲时多久再扫”。

### 3.5 关键限制

- Lua 源码不可直接覆盖旧版本；必须新建版本名并更新 SHA golden。
- Player/Farm 同槽和联合提交没有实现，也不应为了减少一次 Redis hop 临时修改 `ShardFor`。
- 旧 WorkRecord 测试代码仍可能存在，不能据此判断生产路径仍在使用旧引擎。

关键文件：

```text
internal/farm/ingress/plant_source.go
internal/farm/ingress/unlock_source.go
internal/farm/ingress/theft_source.go
internal/farm/actor/harvest_order.go
internal/farm/actor/theft_order.go
internal/player/participant/plant_source.go
internal/worker/sourceorder/
internal/worker/carerecovery/
internal/worker/readiness/
internal/storage/redis/authority/scripts.go
cmd/farm-worker/farm_worker_shard_runtime.go
cmd/farm-worker/source_order_shard_assignment.go
cmd/farm-svr/main.go
cmd/farm-worker/main.go
deploy/docker-compose.yml
deploy/docker-compose.local.yml
deploy/k8s/static-staging/base/resources/platform-config.json
```

---

## 4. 支撑系统现状

### 4.1 任务

权威归属：PlayerActor 内的 `TaskDomain`。

公网命令：`GET_TASK_LIST`、`CLAIM_TASK`。

持久化：随 Player 权威记录的 Model Field v2 保存，不新增 TaskActor。

P0 当前配置：

| Task ID | 类型 | 条件 | 奖励 |
|---|---|---|---|
| `daily.login.v1` | 每日 | 登录一次 | Exp 1 |
| `daily.friend_visit.v1` | 每日 | 访问好友一次 | Exp 1 |
| `daily.friend_interact.v1` | 每日 | 偷菜或护理成功一次 | Exp 1 |
| `growth.harvest.v1` | 成长 | 累计收获一次 | Exp 1 |

规则：

- 业务时区为 `Asia/Shanghai`；
- 不在零点扫描所有玩家；玩家读取/触发任务时惰性切换日周期；
- 领取可以发金币、物品、经验和宠物解锁；
- 当前默认任务表没有 `PetUnlock` 奖励项，只有代码路径；
- `internal/taskcatalog` 仍存在旧的新手收获/购买进度写入，但公开读取与领取只认 `internal/player/task`，这是后续清理项。

### 4.2 植物图鉴

权威归属：PlayerActor。

公网命令：`GET_COMPENDIUM`、`ACK_COMPENDIUM_SEEN`。

当前冻结行为：

- 玩家第一次成功收获自己种在自己农场的植物时解锁；
- 偷菜、护理奖励、邮件附件、只买种子不会解锁；
- 解锁发生在 Harvest 第二跳给 PlayerActor 入包的同一次权威提交中；
- `Revision` 增长，`SeenRevision < Revision` 时显示图鉴红点；
- Ack 只允许单调推进 SeenRevision。

当前实现是简易 P0 Catalog，不等于完整活动植物运营平台。活动结束归档、动态配置发布和大规模图鉴内容仍需专项实现与验收。

### 4.3 邮件

权威边界：邮件条目存于独立 PlayerMail Redis 物理域；附件进入资产时由 PlayerActor 裁决。

支持：列表、详情、标记已读、删除、附件领取；每页最多六封。

领取链：

```text
CLAIM_MAIL_ATTACHMENT
→ PlayerActor
→ 资产变化 + MailClaim Receipt 同次提交
→ 更新邮件领取状态
```

当前生产发送源只有首次 `GET_PLAYER_BOOTSTRAP` 创建的 `WELCOME_V1` 欢迎邮件，附件为三颗胡萝卜种子。任务奖励邮件、运营公告、离线结算邮件和全服邮件尚未接入。

默认 Worker 状态需要特别注意：

| 开关 | 当前默认 | 影响 |
|---|---|---|
| `MAIL_DELIVERY_WORKER_ENABLED` | 关闭 | 欢迎信依赖 bootstrap fast path；fast path 失败后 Order 暂时无人恢复 |
| `MAIL_WORKER_ENABLED` | 关闭 | 附件领取本身已由 Actor 完成；异步投影恢复不运行 |
| `MAIL_MAINTENANCE_ENABLED` | 关闭 | 过期邮件读时过滤，但不会后台 GC |

上线前必须决定是否启用这些 Worker，并补一轮故障恢复与容量测试。

### 4.4 宠物与守卫

权威归属：宠物解锁、当前选择和喂食期限属于 PlayerActor；FarmActor 保存执行偷菜裁决需要的 GuardSnapshot。

当前链路：

```text
PET_UNLOCK_BY_COIN（当前 100 金币）
→ PET_SWITCH
→ PET_FEED（狗粮来自商店）
→ Player 同次提交 GUARD_SYNC_ORDER
→ Direct / GUARD_SYNC worker
→ FarmActor.ApplyGuardSnapshot
→ DecideTheft 可得到 BLOCKED_BY_GUARD 或守卫收益
```

当前有两只 P0 配置宠物；未喂食不产生守卫效果。`GUARD_SYNC_WORKER_ENABLED` 在 authority 模式默认开启，但某些历史 staging 配置曾显式关闭，部署时必须核对最终环境变量。

已用 WSS 验证解锁与喂食；仍建议增加一条独立真实场景：主人解锁、切换并喂食 → 好友偷菜 → 验证截留数量、访问者入包、主人守卫收益与重复 operation 幂等。

### 4.5 商店与狗粮

- 种子仍由 `cropcatalog` 投影；
- `internal/shopcatalog` 增加狗粮 Offer `101`、物品 `#9001`、价格 30、版本 1；
- BUY 必须携带非零 `expected_offer_version`，版本不一致直接拒绝；
- 当前新号初始 100 金币，解锁宠物消耗 100，因此测试喂食前还需提供狗粮购买资金或夹具。

### 4.6 红点

`FeatureSummary` 当前服务端事实：

| 功能 | 判定 |
|---|---|
| 任务 | `claimable_count > 0` |
| 邮件 | 存在未读邮件或可领取附件 |
| 图鉴 | `SeenRevision < Revision` |
| 宠物 | 尚无正式协议字段 |

H5 曾使用“没有生效守卫就点亮宠物角标”的启发式逻辑，这不是服务端权威红点合同，后续需要产品确认后再补协议。

---

## 5. 好友机会与实时提醒基线

该能力来自本分支的上游基线，本轮没有改成异步 Summary：

```text
FriendIndex / canonical Pair
→ 批量读取 FarmOpportunityHeader 做低成本粗筛
→ 对候选读取 Facts + TheftAttemptReceipt 做玩家维度精查
→ 返回好友列表当前页或下一座推荐农场
```

每座农场的机会到期时间进入 Shard DueIndex。`farm-worker` 到期后读取最新 Header/Facts、校验 Schedule generation，并只为真实的 WATER/STEALABLE 等机会发布 Hint。Hint 是刷新加速，不是好友权限或业务裁决。

当前服务端融合方向：

- 已知 Hint 的 `owner_id + opportunity_revision` 走定向精查；
- 登录、回到自己农场、页面恢复及低频兜底走全局 Navigator；
- 打开好友列表只查询当前页并进行精确检查；
- CandidateWindow 只是查询加速缓存，最终仍检查关系和 Farm Facts。

本 A8 汇合提交没有重新执行 300 好友完整容量基准。相关历史证据见：

- `docs/verification/friend-opportunity-fusion-handoff-20260811.md`；
- `docs/verification/friend-opportunity-p0-optimization-20260811.md`；
- `docs/verification/friend-opportunity-real-company-redis-ab-20260811.md`。

---

## 6. Bot 与测试工具

### 6.1 `live-farm-bot`

`cmd/live-farm-bot` 是真实 HTTP 登录 + WSS FarmFrame 客户端，不是函数级 mock。它复用 `internal/farmbotclient`，遵守 ClientHello、Join ACCEPTED/Snapshot/Ready 屏障、operation 轮询和重连合同。

支持场景：

```text
join / plant / care / theft / harvest / reconnect / mixed
```

支持控制：

- 玩家数量、到达率和最大在线数；
- 在线时长、思考时间和每会话最大动作数；
- 混合动作权重；
- 多热点农场分布；
- 预先解锁 19 块土地；
- CPU、mutex、block、goroutine、heap profile；
- 有界事件留存和只读运行状态接口。

安全边界：

- 凭证通过调用方注入的 HMAC secret 提供，不进入 BotSpec 和结果日志；
- 默认只允许 loopback；访问非 loopback 环境必须显式 `--allow-non-loopback`；
- 运行环境限制为 local/ci/sandbox。

### 6.2 `mixed-farm-bot`

`cmd/mixed-farm-bot` 当前**只生成确定性的 dry-run workload recipe**：

- 构造有限玩家和动作计划；
- 生成 RunSpec、FixtureManifest 和 fingerprint；
- 拒绝生产/预发布等危险 environment/namespace；
- `TransportEnabled=false`，不会连接 HTTP、WSS 或 Redis。

因此它适合审阅流量模型和生成夹具，不能作为真实容量证明。

### 6.3 其他工具

```text
cmd/authority-smoke-bot       权威链路冒烟
cmd/authority-fault-bot       故障与恢复场景
cmd/live-bot-fixture          真实 Bot 夹具
cmd/po1-plant-e2e             SourceOrder WSS 纵向证明
cmd/friend-opportunity-*      好友机会夹具、审计与 A/B 测量
cmd/redis-production-probe    单节点/Cluster 生产探针
cmd/schedule-due-index-tool   DueIndex 检查与维护
```

---

## 7. K8s Pod 扩容、缩容与 Shard 迁移

### 7.1 组件职责

| 组件 | 负责 | 不负责 |
|---|---|---|
| K8s Informer | Pod 创建、删除、Ready、PodUID、ordinal、StatefulSet 身份 | 不修改业务路由 |
| HolderControl | 验证 domain、boot_id、进程 Ready、当前 Shard 数 | 不决定 Shard 分配 |
| VerifiedHolderView | 保存 RouteController 当前已验证的可重建运行视图 | 不是第二套注册中心 |
| RouteControl | Shard Assignment、Lease、`writer_epoch`、Route revision | 不决定 K8s 副本数策略 |
| ScalePlan | 健康拓扑下扩缩容、Drain、Handoff、K8s patch | 不处理意外 Holder 故障 |
| AutoFailover | 故障 Holder 的 CrashTakeover | 不自动创建新 Pod |

PLAYER 和 FARM 是两个独立 Route Domain。只新增 `farm-svr` Pod 时，只重分配 FARM Shard；Player Shard 不跟着迁移。

`FARM_WORKER` 也是独立 Route Domain。RouteControl 负责发布 Worker Shard 到 farm-worker Pod 的 Assignment；本轮代码负责消费这份 Assignment，并据此停止旧实例的新 Claim、让新实例接管对应 SourceDueIndex Bucket。它不改变 PLAYER/FARM Actor 的写权，也不由各 Worker 自己争抢全量 Bucket。

### 7.2 自动扩容

支持两条路径。

路径 A：通过安全目标 annotation：

```text
StatefulSet annotation = scaleplan.farm/safe-desired-replicas=N
→ 持久化 +1 ScalePlan
→ patch StatefulSet
→ Informer发现新Pod
→ K8s Ready
→ HolderControl校验PodUID/BootID/domain/process_ready
→ 新Holder成为READY_ZERO_SHARD
→ 生成确定性最少迁移清单
→ 对每个Shard执行RoutePlan Drain/Handoff
→ 达到目标后条件删除一次性annotation
```

路径 B：外部已经直接 `replicas +1`：

```text
Informer发现恰好多一个Pod
→ Controller采用ADOPTED_COMMITTED
→ 不重复patch副本数
→ 走相同READY_ZERO_SHARD与Shard迁移状态机
```

首版只按 Shard 数均衡，并在原布局已经基本均衡时移动理论最少数量；不根据热点权重迁移。直接外部扩容一次只支持 `+1`，多级扩容应使用 safe desired annotation 逐级完成。

### 7.3 安全缩容

不要先直接执行 `spec.replicas: N → N-1`。StatefulSet 会优先删除最高 ordinal，而这个 Pod 可能仍持有 Shard。

正确流程：

```text
在目标StatefulSet写safe desired=N-1
→ 排除最高ordinal，不再把新Shard分给它
→ 捕获它的完整Shard集合
→ 为每个Shard持久化并执行RoutePlan
→ Drain旧Holder
→ 提交更高writer_epoch并发布新RouteSnapshot
→ 新Holder按需冷加载Actor
→ 确认旧Pod为READY_ZERO_SHARD
→ patch StatefulSet replicas -1
→ 确认Pod消失
```

如果最高 ordinal 本来就是零 Shard，也必须走持久排除与验证后才能删 Pod。

直接外部缩容会被识别为 `UNSAFE_EXTERNAL_SCALE_IN`；它不是平滑缩容。如果 Pod 已经被删，只能按 Holder 故障恢复处理。

### 7.4 Actor 在迁移期间的行为

P0 使用冷迁移：

- 强事实先保存在 Redis；
- 不传输 Actor 内存 Snapshot；
- PlayerActor 在下一条玩家命令时于新 Holder 懒加载；
- FarmActor 在 fresh Join 时于新 Holder 懒加载；
- Presence、Subscription 和 JoiningBuffer 属于旧 Pod 运行态，不迁移；
- 受影响客户端依靠 Redirect、幂等重试和重新 Join 恢复；
- 旧 Holder 携带旧 `writer_epoch` 的迟到写会被 Redis Fence 拒绝。

### 7.5 当前不是 HPA

当前代码已经能在副本变化后自动发现并编排 Shard，但没有根据 CPU、QPS、Mailbox 排队或热点自动计算副本目标。若以后接 HPA，HPA/容量控制器只负责提出副本目标，ScalePlan 仍负责安全迁移和缩容。

对 farm-worker 也同样如此：当前代码已经具备按 `FARM_WORKER` Assignment 分片工作的执行面，但生产环境仍需让 RouteControl 在 worker Pod 增减或故障时更新 Assignment。单元测试已经覆盖 Assignment revision 变化后的 Claim Fence；真实 K8s 多 Worker 自动重分配仍属于验收项。

关键配置：

```text
SCALEPLAN_MODE=off|manual|auto
SCALEPLAN_AUTO_RECONCILE_INTERVAL=5s
SCALEPLAN_SAFE_DESIRED_ANNOTATION=scaleplan.farm/safe-desired-replicas
```

关键文件：

```text
internal/platform/kubernetes/holderdiscovery/
internal/platform/kubernetes/scaleplan/
internal/scaleplan/
internal/scaleplan/autorebalance/
internal/scaleplan/adapters/control/
cmd/route-controller/scaleplan_runtime.go
cmd/route-controller/scaleplan_auto_runtime.go
deploy/k8s/dynamic-local-scaleplan/README.md
```

---

## 8. Pod/进程宕机自动恢复

### 8.1 触发条件

RouteSnapshot 仍指向某个精确 `holder_id + boot_id`，但该进程代次不再存在于 VerifiedHolderView。

### 8.2 两条接管路径

纯消失、没有新 Boot 证明：

```text
Informer发现旧Holder消失
→ 旧Authority Lease仍有效：等待
→ Redis权威时间确认Lease到期
→ 选择同域健康且负载最少的目标Holder
→ 创建确定性CrashTakeover RoutePlan
→ TakeoverExpiredShardAuthority CAS
→ writer_epoch / lease_epoch / assignment_revision单调+1
→ 发布新RouteSnapshot
```

同一个 StatefulSet Pod 出现新 Boot：

```text
PodUID可能相同，但boot_id从B1变为B2
→ 新Boot证明旧进程代次已经失效
→ 可走HolderUnavailable接管
→ 仍需完整旧Fence、持久RoutePlan和更高writer_epoch
```

### 8.3 安全性质

- 不能仅因 Pod NotReady 就直接改路由，避免网络分区时双 Writer；
- 旧进程恢复后，Renew 和 Actor Commit 均携带旧 Fence，被 Redis 拒绝；
- 相同 transition 重试幂等返回 `ALREADY_APPLIED`；
- 一次 pass 默认最多完成 32 个 Plan、读取 128 次精确 Authority；
- 整个 Route Domain 没有健康目标时保持原事实并报警，不能伪造恢复成功；
- AutoFailover 与 AutoRebalance 共用 realm 级变更锁，避免同时修改 RouteSnapshot。

关键配置：

```text
ROUTE_FAILOVER_RECONCILE_INTERVAL=1s
ROUTE_FAILOVER_MAX_PLANS_PER_PASS=32
ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS=128
```

关键文件：

```text
internal/routeplan/autofailover/
cmd/route-controller/holder_discovery_runtime.go
cmd/route-controller/verified_holder_reader.go
cmd/route-controller/autofailover_metrics.go
internal/storage/authoritystore/shard_authority.go
internal/storage/redis/authority/shard.go
docs/design/87-k8s-holder-failure-auto-recovery.md
```

---

## 9. 三主三从 Redis Cluster 适配

### 9.1 连接方式

核心服务统一依赖 `redis.UniversalClient`：

```text
FARM_REDIS_CLUSTER_URLS=redis://master-1:6379,redis://master-2:6379,redis://master-3:6379
→ redis.NewClusterClient
→ 自动发现Primary/Replica与Slot Owner
```

配置优先级：

```text
FARM_<DOMAIN>_REDIS_CLUSTER_URLS
→ FARM_REDIS_CLUSTER_URLS
→ FARM_<DOMAIN>_REDIS_URL
→ FARM_REDIS_URL
```

Cluster 模式只允许 logical DB 0。环境隔离依靠 namespace、ACL、Key 前缀和物理 Cluster，而不是 `SELECT db`。

已接入：

- player-svr、farm-svr、farm-worker、farm-gate；
- route-controller；
- rt2q bootstrap、DueIndex 工具；
- 好友机会夹具/审计；
- Redis 生产探针。

### 9.2 同槽边界

Redis Cluster Lua 只能访问同一 Slot 的 Key。KeyBuilder 保证同一个 Authority Bucket 的以下数据共用完整 Hash Tag：

```text
ShardWriteAuthority
ActorRecord
Receipt
SourceOrder / Schedule
DueIndex
```

Player、Farm、Friend Pair、Mail、Workflow 和 RouteControl 各自拥有独立 Bucket。跨 Bucket/Actor 不使用跨 Slot Lua，继续通过 SourceOrder、ResultDirectory 或 Projection 可靠发布。

### 9.3 已修复的 Cluster 不兼容

- 无 Key 的 `TIME` 不再与带 Key 的 `HMGET` 放进同一 Cluster transaction pipeline；
- 生产探针 Lua 使用显式 Key；
- Cluster 模式不再把单节点 `WAIT` 当成持久化证明；
- 路由与 DueIndex 运维工具支持 Cluster seed，避免遇到 MOVED Slot 后失败。

### 9.4 仍未证明的内容

没有真实三主三从地址和凭证时，只能证明代码、配置和同槽合同兼容，不能证明：

- 主从切换 RTO/RPO；
- MOVED/ASK 期间客户端延迟与错误率；
- Slot 迁移；
- Replica 复制延迟；
- AOF/RDB 策略；
- Cluster 下容量和热点 Slot 分布。

真实验收入口应优先使用 `cmd/redis-production-probe`，不能拿仍为单节点语义的历史工具代替。

---

## 10. 已执行验证

### 10.1 SourceOrder 与支撑系统

已通过：

```text
go test ./... -count=1
```

真实四服务 WSS smoke 使用：

```text
PO1_E2E_MODE=SOURCE_ORDER_WSS go run ./cmd/po1-plant-e2e
```

已证明的主链包括：

- 登录、Join；
- Buy → Plant；
- 好友 Care，访问者经验到账；
- Theft，访问者产物入包；
- 主人 Harvest，奖励入包；
- SourceDueIndex 最终清空；
- 欢迎邮件、每日任务、图鉴解锁、Sell、宠物解锁与喂食。

### 10.2 ScalePlan 与 AutoFailover

已通过：

```text
go test ./internal/scaleplan/autorebalance
go test ./internal/scaleplan
go test ./internal/platform/kubernetes/scaleplan
go test ./internal/routeplan/autofailover
go test ./cmd/route-controller
go test -race ./cmd/route-controller ./internal/scaleplan ./internal/scaleplan/autorebalance ./internal/scaleplan/adapters/control
python3 scripts/local-k8s/test_dynamic_route_profile.py
```

动态 K8s 部署渲染 14 项测试通过。

真实 Redis Fence 探针证明：

```text
普通Transfer + 过期Lease → 拒绝
TakeoverExpired + 完整旧Fence → APPLIED
writer_epoch / lease_epoch / assignment_revision 单调+1
同transition重试 → ALREADY_APPLIED
旧Holder Renew → FENCE_MISMATCH
```

### 10.3 Redis Cluster 代码适配

已通过：

- 全仓 `go test ./... -count=1`；
- Redis domain、Authority、RouteControl、PlayerMail、Friend、Workflow 等专项测试；
- Compose 配置解析；
- K8s JSON/Secret 模板解析；
- shell 语法检查；
- KeyBuilder、Lua 和 CROSSSLOT 单测。

### 10.4 SourceOrder Worker 生产分片收敛

当前接续分支 `b6b23de` 已通过：

```text
go test ./cmd/farm-worker ./internal/worker/carerecovery ./internal/worker/readiness ./internal/worker/opportunityrecovery
go vet ./cmd/farm-worker ./internal/worker/carerecovery ./internal/worker/readiness ./internal/worker/opportunityrecovery
go test ./internal/worker/...
git diff --check
```

覆盖重点包括：

- Source Shard 到 FARM_WORKER Shard 的稳定映射；
- Worker 只扫描当前 Assignment 所有的 Bucket；
- Assignment revision 变化后旧归属不能再 Claim；
- 有界轮转扫描不会因局部空批次提前进入退避；
- 完整空 Sweep 后指数退避；
- Readiness/Backlog 只观察 owned Bucket，revision 变化使观察缓存失效；
- 机会唤醒与 SourceOrder 恢复共用同一个 Worker Shard Runtime。

本轮在 Windows 工作树尝试了 `go test ./...`，但未把它标记为当前提交的完整通过证据：公司私有 Gateproxy/Tcaplus 依赖无法非交互获取，`cap-redis` 包含 Linux-only syscall，另有既存时间敏感/临时目录权限/证据哈希测试失败；本轮修改涉及的包没有出现测试失败。`-race` 因当前环境未启用 CGO 也尚未执行，必须在 Linux 补跑。

K8s `platform-config.json` 已完成 JSON 解析；当前 Windows 环境没有 Docker CLI，因此 Compose 仍需在 Linux 执行 `docker compose config`。

---

## 11. 尚未执行的真实验收

以下内容不得在答辩或发布说明中写成“已验证”：

1. 隔离 K8s namespace 中的 FARM `2→3→2`；
2. PLAYER `2→3→2`；
3. 迁移过程中 route-controller 重启；
4. 删除实际持有目标 Shard 的 Pod；
5. H5 在 Farm Shard 迁移后收到重连提示并 fresh Join；
6. 玩家 operation 在 Player Shard 接管后使用原 operation_id 恢复；
7. 多 Pod 同时故障与整个 Route Domain 暂无健康目标；
8. 真实三主三从 Redis 主从切换、Slot 迁移和网络抖动；
9. 本 A8 镜像上的 300 好友、热点农场和 200 人同场容量复测；
10. 宠物守卫拦截偷菜的完整 WSS 端到端；
11. 邮件 fast path 失败后 Worker 恢复；
12. 任务/图鉴/邮件/宠物与扩缩容同时发生的联合稳定性测试；
13. 多个 farm-worker Pod 对 FARM_WORKER Shard 的无重叠、无遗漏覆盖；
14. SourceDueIndex 有积压时发生 Assignment Rebalance，验证旧 Worker 停止 Claim、新 Worker 接管；
15. farm-worker Pod 宕机/重启后的 backlog 恢复延迟与重复执行率；
16. 当前生产参数下 Worker Redis command QPS、`oldest_due_age` 和完整 Sweep 周期；
17. Linux `-race` 与 `docker compose config`。

---

## 12. 推荐下一阶段验收顺序

### 阶段 A：先做非破坏性回归

1. 用当前接续分支 `b6b23de` 镜像启动独立四服务 + Redis；
2. 跑全仓测试与 SourceOrder WSS smoke；
3. 跑任务、图鉴、邮件、宠物最小闭环；
4. 确认旧 `{work-v1}` 热路径未重新启用；
5. 至少启动两个 farm-worker，确认 FARM_WORKER Assignment 的 owned Bucket 集合无重叠、无遗漏；
6. 在 backlog 存在时切换 Assignment revision，确认旧 Worker 不再产生新 Claim；
7. 记录 SourceOrder、DueIndex、Redis workload 和 Gate 指标。

### 阶段 B：隔离 K8s 扩缩容

1. 创建独立 context/namespace，不能复用正在压测的环境；
2. `SCALEPLAN_MODE=auto`；
3. FARM `2→3`，验证只移动理论最少 Shard；
4. FARM safe desired `3→2`，验证最高 ordinal 先归零再删除；
5. 对 PLAYER 域重复；
6. 迁移中保持 H5/Bot 操作并验证幂等和重新 Join；
7. 迁移中重启 route-controller，验证 PlanID 和 Fence 单调。

### 阶段 C：故障接管

1. 删除持有测试 Farm Shard 的 Pod；
2. 观察 `missing → waiting_lease → recovered`；
3. 验证旧 Boot 迟到写被拒绝；
4. 验证 FarmActor 冷加载和客户端 fresh Join；
5. 对 Player Shard 重复，并恢复未决 operation；
6. 最后再做多 Shard、连续重启和 no-target 演练。

### 阶段 D：真实 Redis Cluster

1. 连接发现六个节点；
2. 多 Hash Tag Slot 路由；
3. Authority/Pair/Mail/RouteControl 代表性 Lua；
4. 主从切换与 MOVED/ASK；
5. RPO/RTO、AOF/RDB 与复制延迟；
6. 叠加 Bot 流量测 p50/p95/p99、CPU、连接重建与失败率。

### 阶段 E：联合验收

在扩缩容或故障注入期间同时执行：

```text
登录/重连
+ Join/切换好友农场
+ Plant/Harvest/Care/Theft
+ Task Claim
+ Mail Claim
+ Compendium Ack
+ Pet Feed/Guard Theft
```

只有这一阶段通过后，才可以把该分支称为“候选生产基线”。

---

## 13. 观测与排障重点

### Route/Scale

- 当前 RouteSnapshot revision；
- PLAYER/FARM 各 Shard Holder 分布；
- `writer_epoch`、Lease epoch 是否单调；
- ScalePlan/RoutePlan 状态与最后错误；
- missing、waiting lease、no target Shard 数；
- Drain 中 Mailbox/操作超时、Redirect 和 REJOIN_REQUIRED。

### SourceOrder/Worker

- SourceDueIndex backlog；
- PENDING、Claimed、Completed、Retry、Unknown 数；
- Direct 成功率与 Worker 接管率；
- Receipt/operation 幂等冲突；
- 旧 `{work-v1}` Key 是否还有新增；
- `farm_worker_owned_bucket_count`：本实例当前实际拥有的 Source Bucket 数；
- `farm_worker_assignment_revision`：本地已应用的 FARM_WORKER 路由版本；
- `farm_worker_bucket_scanned_total`：实际扫描 Bucket 数；
- `farm_worker_due_depth`、`farm_worker_oldest_due_age_ms`：积压深度与最老任务年龄；
- `farm_worker_scan_duration_ms_count`：可用于计算扫描 QPS；
- `farm_worker_claimed_total`、`farm_worker_completed_total`、`farm_worker_retried_total`、`farm_worker_step_unknown_total` 与 `farm_worker_terminal_latency_ms`。

建议告警重点：Assignment revision 长期不推进、预期承载实例的 owned bucket 为零、`oldest_due_age` 持续上升、Unknown/Retry 比例异常，以及多个 Worker 的 owned Bucket 集合发生重叠或缺口。

### Redis Cluster

- 每个 Primary 的 Slot、CPU、带宽和内存；
- MOVED/ASK、连接重建、timeout、CROSSSLOT；
- 热 Hash Tag 和大 HASH；
- p50/p95/p99；
- 主从复制延迟与 failover 时间。

### 支撑系统

- 每日任务惰性 rollover 与 claimable 数；
- 邮件未读/可领/过期与 Worker backlog；
- 图鉴 revision/seen revision；
- GuardSync Order backlog 与 GuardSnapshot 新鲜度；
- FeatureSummary 与真实 Player 状态是否一致。

---

## 14. 接手人禁止事项

1. 不要为了减少 Redis hop 把 Player/Farm 静默改成同一 Shard 或同一 Slot。
2. 不要把两个 Actor 合成一条 Mailbox。
3. 不要看到 Pod Ready 就直接改路由表；必须经过 HolderControl 和 RoutePlan。
4. 不要直接缩小 StatefulSet 再补迁移；安全缩容必须先 Drain 到零 Shard。
5. 不要在旧 Authority Lease 尚有效且无强故障证明时强抢写权。
6. 不要把 VerifiedHolderView 改造成 Holder 主动注册中心。
7. 不要修改旧 Lua body 却保持原版本名和 SHA。
8. 不要让 Worker 使用 `KEYS`、全局 `SCAN`、`FLUSH*` 发现任务。
9. 不要把 Redis Pub/Sub、Key 过期通知或 H5 Hint 当作业务真值。
10. 不要把 dry-run `mixed-farm-bot` 输出当成真实容量证据。
11. 不要在共享压测 namespace 删除 Pod 或做主从故障注入。
12. 不要在未做真实 Cluster 验收时宣称三主三从容灾完成。
13. 不要让每个 farm-worker 都扫描全部 PLAYER/FARM Source Bucket；生产必须由 FARM_WORKER Assignment 划分归属。
14. 不要在只扫描完一段轮转批次且暂时为空时进入指数退避；退避只能基于完整 owned Bucket Sweep。
15. 不要在动态生产模式混用 `FARM_WORKER_ASSIGNMENT_MODE=route` 与静态 `FARM_WORKER_OWNED_SHARDS`。

---

## 15. 关键参考文档

- [SourceOrder 切流与 Redis IO 瘦身交接](source-order-migration-handoff-20260814.md)
- [SourceOrder 场景与支撑系统说明](source-order-redis-io-scenarios-20260814.md)
- [SourceOrder Worker 生产分片、扫描与观测交接](source-order-worker-production-shard-ownership-20260814.md)
- [ScalePlan A6 自动 Rebalance 验证](scaleplan-auto-rebalance-a6-verification-20260814.md)
- [Route Holder 自动故障恢复 A7](route-holder-auto-failover-a7-20260814.md)
- [Redis Cluster A8 适配与验证](redis-cluster-a8-verification-20260814.md)
- [K8s Holder Discovery A5](k8s-holder-discovery-a5-implementation-20260814.md)
- [ScalePlan 自动 Rebalance 设计](../design/86-scaleplan-auto-rebalance-implementation-plan.md)
- [Holder 宕机自动恢复设计](../design/87-k8s-holder-failure-auto-recovery.md)
- [动态 K8s ScalePlan 部署说明](../../deploy/k8s/dynamic-local-scaleplan/README.md)
- [任务合同](../design/34-task-system-contract.md)
- [邮件合同](../design/35-mail-system-contract.md)
- [宠物合同](../design/36-simple-pet-guard-contract.md)
- [植物图鉴合同](../design/37-plant-compendium-contract.md)

---

## 16. 最终交接判断

当前接续分支已经完成的是：

```text
核心业务从通用Saga/WorkRecord切到SourceOrder
+ 支撑系统移植到同一权威代码树
+ Bot与真实WSS验证入口
+ K8s实例发现、自动Rebalance、安全Scale-in
+ Holder故障自动接管
+ Redis三主三从Cluster代码适配
+ SourceOrder Worker按FARM_WORKER Assignment扫描
+ 迁移revision/Claim Fence、有界轮转、完整空Sweep退避
+ owned bucket、assignment revision、Due积压与扫描指标
```

当前接续分支尚未完成的是：

```text
真实K8s 2→3→2
+ 真实Pod删除与H5恢复
+ 真实Redis Cluster故障与Slot迁移
+ 汇合镜像上的最终容量复测
+ 支撑系统与迁移/故障的联合验收
+ 真实多farm-worker Rebalance、宕机接管与Redis workload证据
+ Linux race与Compose配置验证
```

因此下一位 Agent 不应继续扩大业务范围。最有价值的工作是按照第 12 节，在隔离环境中把这些已经写好的能力变成可复现、可观测、带原始日志的验收证据。
