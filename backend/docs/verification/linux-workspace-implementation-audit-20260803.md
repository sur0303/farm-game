# Linux 工作区真实实现完成度审计

日期：2026-08-03  
状态：`POINT_IN_TIME_AUDIT / SUPERSEDED_BY_ROUND0_CHECKPOINT`
审计对象：Linux 后端、H5 集成分支以及当前运行环境  

> 本文只回答“代码仓库现在真正实现了什么”。业务与技术语义仍以第 26～37 号合同为准；本文不能反向修改合同。
> 实现状态会继续变化，后续引用时必须同时核对 branch、commit 和 dirty 状态。

## 0. 2026-08-03 Round 0 补充检查点

本文后续章节保留的是 `4c97191` 时点的详细审计，不再代表当前 HEAD。W5B 收口、合同同步和 Round 0
并行施工后，当前 Linux 后端基线已经更新为：

```text
branch = codex/integration-20260803
HEAD   = f075f5b99af416bab5253e142cbcc884cd16f088
remote = origin/codex/integration-20260803
status = clean
```

新增并已组合验证的实现：

| 工作包 | 集成提交 | 新增实现 |
|---|---|---|
| P1A / SCB-1 | `77aa0a5` + `4456762` | 共享身份、Participant IDL、Fake、Golden、并发与输入校验 |
| DF0 | `208263e` | Farm 最终规则纯领域包 |
| DP0 | `439f034` | 简化宠物与 Guard 输入纯领域包 |
| Q0 | `bb3ed36` | 测试/故障注入工具、Load Harness、合同测试矩阵 |
| DT0 | `f075f5b` | 每日/成长任务纯领域包；Receipt 明确留在 PlayerAuthority 外层 |

组合候选已执行 `go test -race`（新增共享包/领域包/测试工具）、`go test ./...`、`go vet ./...` 和
`git diff --check`，均通过。W5B 的历史脏工作树和临时分支也已经清理。当前派发与下一步状态以第
71～73 号为准；下文关于“当前 dirty”“H5 2 项失败”和 `4c97191` HEAD 的描述仅用于还原旧审计时点。

## 1. 结论摘要

现有第 71 号文档的 Linux 状态已经过时，不能再只按 W5B 描述项目进度：

- 后端 W5B 已通过 MR !6 合入集成分支，集成 HEAD 已从 `9756bf1` 前进到 `4c97191`；
- 接入、账号、Session、Join/切农场、双 Actor、Redis 业务存储、四类可恢复 Workflow、核心经营、好友分享与
  实时同步已经形成可运行主链；
- 任务不是纯骨架：已实现两条非周期成长任务、进度与领奖；
- 图鉴不是纯骨架：已实现首次成功收获解锁、持久恢复和 H5 展示；
- 新版 ActorAuthorityStore、Shard 写权和 `auth-v1` KeyBuilder 已有实现与测试，但**尚未接入 player-svr/farm-svr
  的生产入口**；当前在线主链仍使用旧 `blobstore.RedisStore`；
- 邮件、宠物、土地购买解锁、每日任务、虫害、完整好友机会查询、红点等尚未形成业务实现；
- 后端提交态测试通过；H5 构建通过，但当前 Linux Node 环境下 76 项单测有 2 项失败，不能写成“全部通过”；
- 当前健康容器创建于 2026-08-02，早于 2026-08-03 的 W5A/W5B 等最新提交，不能把容器健康等同于最新 HEAD
  已完成真实进程验收。

因此当前最准确的阶段判断是：

```text
可运行的核心农场纵向切片
+ 较完整的接入/Join/Actor 工程骨架
+ 一批尚未接入主链的新权威存储和路由组件
+ 多个尚未实现的 P0d/P1 产品模块
+ 200 人同场与 3000 万 DAU 仍缺最终证据
```

## 2. 审计口径

本次没有只看提交标题，而是按四层证据核验：

1. Git：分支、HEAD、祖先关系、合并记录、dirty 状态；
2. Source：Proto、CommandCatalog、进程入口、Actor、Store、Workflow 和 H5 调用是否真实存在；
3. Wiring：组件是否被 `cmd/*` 入口创建并接到请求主链；
4. Verification：提交态测试、构建、race、运行容器和已有 E2E 记录。

状态术语：

| 状态 | 含义 |
|---|---|
| `主链已接通` | 源码存在，进程入口已接线，并有自动化或历史真实进程证据 |
| `部分实现` | 有可用纵向切片，但尚未满足现行合同全部规则或容量要求 |
| `组件完成未接线` | 独立包和测试存在，但当前服务入口没有使用它 |
| `仅合同/尚未实现` | 冻结文档中有定义，当前源码没有对应业务闭环 |
| `证据待补` | 代码存在，但缺当前 HEAD 的真实多进程、故障或容量证据 |

## 3. 精确仓库基线

### 3.1 后端

```text
path:   /data/home/developer_026/CodeBuddy/20260519141303/farm-game
branch: codex/integration-20260803
HEAD:   4c971915cd0ef13db038d82890f8ddd51788ffcd
remote: origin/codex/integration-20260803
```

HEAD 是：

```text
4c97191 Merge branch 'codex/w5b-subscription-lease-20260803'
         into 'codex/integration-20260803' (merge request !6)
```

当前工作树不是干净的：

```text
 M cmd/dualgate-e2e/main.go
 M scripts/test-dualgate-kick
?? dualgate-e2e
```

这些改动正在补双 Gate 的 W5B Lease Loss → `REJOIN_REQUIRED` → Fresh Join 证据；未跟踪的
`dualgate-e2e` 是 Linux ELF 编译产物。它们不属于 `4c97191` 提交态，本次没有修改、删除、stash 或覆盖。

### 3.2 H5

```text
path:   /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
branch: codex/integration-20260803
HEAD:   673743823fb4fd876bf462279300f5f56985ab23
remote: origin/codex/integration-20260803
status: clean
```

### 3.3 当前代码规模

```text
后端 Go 文件：275
后端 _test.go：113
Proto 文件：20
H5 TS/TSX：72
H5 test/spec：18
```

文件数量只能说明工程规模，不能单独作为完成证明。

## 4. 提交时间线

下表只列能够解释当前实现形态的关键提交；是否“完成”仍由源码接线和测试共同决定。

| Commit | 已合入能力 |
|---|---|
| `356db1c` | PlayerActor/FarmActor 拆分与个人、经济、社交三 Loop 初版 |
| `9bc82f4` | 分享链接、好友关系持久化与安全边界 |
| `daf28a3` | Redis WorkflowStore 与 farm-worker 故障恢复 |
| `574b51c` | Player/Farm 主链切到公司 Redis Adapter |
| `c2d9d32` | v1.21 农场 Loop、探针与多进程纵向切片整合 |
| `2830520` | v0.3 账号、Ticket、Session 合同主干 |
| `3bb21a8` | 双 Gate 顶号真实进程 E2E 工具 |
| `22602c0` | CommandCatalog 与严格连接序列 |
| `36fa44a` | Join Pipeline、Unknown Freeze、Subscription 生命周期、Idle Passivation |
| `7e2810c` | 公网 Join Wire v3 |
| `810d29b` | 内部可信上下文与 RPC IDL 对齐 |
| `06d8b10` | W2A Join Barrier |
| `3443a28` | W2B 切农场 Subscription Fence 清理 |
| `9c6580d` | W3B Player Bootstrap |
| `a9fb2bf` | W7D 稳定 Workflow/Order/Receipt 身份 |
| `c7ddc90` | W31A 版本化 Redis Keyspace 与平台探针 |
| `c231f64` | W8AL ActorAuthorityStore 逻辑合同 |
| `22429c6` | W9AL Fence/Unknown 两种顺序证明 |
| `768a8f0` | W8C ShardWriteAuthority 逻辑状态机 |
| `0d1a405` | 带 Seed 的 Route Hash Profile v2 |
| `b00edbd` | 恢复准入屏障 |
| `72de661` | W5A SessionRoute 单调 Lease |
| `c86ce39` / `4c97191` | W5B Subscription Lease 批续租及集成合并 |

H5 的关键链路提交包括：

| Commit | 已合入能力 |
|---|---|
| `fb2a5bd` | 三 Loop 产品客户端接入 |
| `8987cf0` | 账号恢复与社交农场流程 |
| `9f4d138` | 公网 Join Wire v3 消费 |
| `b21c214` | JoinPhase 与 PlayerBootstrap 对齐 |
| `d61bae6` | 稳定 Work Identity Golden |
| `6737438` | RecoveryBarrier ACK 收口 |

## 5. 当前真实服务与主链

当前部署/进程边界已经形成四个后端可执行单元：

```text
H5
 ├─ HTTPS/JSON → farm-gate：账号、Cookie、Ticket、好友/分享辅助 HTTP API
 └─ WSS/Protobuf FarmFrame → farm-gate
                              ├─ player-svr → PlayerActor(player_id)
                              └─ farm-svr   → FarmActor(farm_id)
                                                ↕
                                      Redis WorkflowStore
                                                ↕
                                           farm-worker
```

`farm-gate`、`player-svr`、`farm-svr`、`farm-worker` 均有独立入口。当前运行环境中四个 local Compose
容器均显示 healthy；另有 Prometheus、Grafana 和 TCONND/gateproxy Probe 容器。但是这些业务容器日志的
启动时间为 2026-08-02，版本字符串仍为 `0.0.0-dev`，不能证明它们使用 `4c97191`。

## 6. 分能力真实完成度

### 6.1 账号、认证与接入：主链已接通

已实现：

- Web 账号注册、账号密码登录；
- PBKDF2 密码校验与登录尝试限流；
- 密码修改、全设备退出、管理员禁用账号；
- 微信小程序登录 Adapter；
- 签名 HttpOnly `farm_session` Cookie 与免密换票；
- Redis 一次性 ConnectionTicket 与 TTL；
- Direct WebSocket 当前可运行，TCONND + gateproxy 保留为 Transport Adapter/平台探针路径；
- ClientHello/ServerHello、FarmFrame v3、严格连接序号、帧大小与 pre-auth 资源上限；
- 每连接有界 FIFO、下行背压、限流、健康检查、metrics 和可选 pprof。

边界：

- TLS/WAF/CLB 由部署层提供，不是 Go 进程自身完成的生产证据；
- TCONND/gateproxy 已有真实 Probe 容器与适配代码，但当前业务默认链路仍是 Direct WebSocket；
- 正式域名、证书、密钥托管和平台权限仍属于部署验收。

### 6.2 Session、Join、切农场与实时订阅：实现较完整，证据待补

已实现：

- SessionAuthority、SessionPermit、旧连接 KICK；
- Redis SessionRoute 与 W5A 单调 Lease；
- Join v3 状态机和公开顺序：`ACCEPTED → Snapshot → DeltaBatch* → SubscriptionReady`；
- FarmSubscriptionHub、JoiningBuffer、PostReadyBuffer、版本游标与过载 Resync；
- 切换农场时旧 Snapshot/Delta 的 Subscription Fence；
- 已准入旧命令结果与旧农场 Push 分离；
- W5B `BatchRenewSubscriptions`、逐 Subscription TTL/Fence、Lease 丢失后 `REJOIN_REQUIRED`；
- Gate 下行 Queue、慢连接隔离和 FarmChange 广播；
- H5 Connected/Authenticating/Authenticated/Joining/Recovering/InFarm 与恢复屏障。

尚缺：

- 当前 HEAD 的真实双 Gate + Subscription Lease Loss + Fresh Join 完整证据；
- 200 人同场 Subscription Renew QPS/P99 和广播容量；
- GateBoot 旧机制的 shadow/退役证据；
- 热点 Farm 的真实 JoinBuffer、慢客户端和重连风暴压测。

### 6.3 CommandCatalog 与公网协议：框架已接通，命令集未完成

当前真实注册 12 类公开业务命令：

```text
GET_PLAYER_BOOTSTRAP
BUY_OFFER
SELL_ITEM
PLANT_CROP
HARVEST_CROP
JOIN_FARM
LEAVE_FARM
WATER_CROP
BEGIN_THEFT
GET_FARM_SNAPSHOT
GET_OPERATION_RESULT
CLAIM_TASK
```

已实现 CommandSpec 的 Payload/Result 类型、负载上限、Session 准入、第一跳、Route Domain、幂等、重试和
Permit 分类。PLANT 已接确定性 Canonical Payload Builder。

尚缺：

- 第 28 号合同最终 33 类命令的唯一迁移；
- 多数写命令的最终 Canonical Payload Builder；
- Mail、Pet、Compendium 查询、UnlockPlot、虫害等公开命令；
- Typed Result 与当前旧 Payload 的完整兼容/滚动升级证明。

### 6.4 Actor 路由与 Runtime：核心框架已实现，控制面和新写权未接完

已实现并已被当前主链接入：

- PLAYER/FARM 两个独立 Route Domain；
- `ActorAddress → seeded XXH64 v2 → VirtualShard → Holder`；
- 本地不可变 Assignment Snapshot 原子替换；
- Holder gRPC 复用、过期路由 Redirect 与同 operation 有界重试；
- PlayerActor/FarmActor 独立 Registry 与邮箱串行；
- UserMailbox/SystemQueue 隔离、item/byte 水位、Actor Busy；
- FarmActor Activation FSM、singleflight 激活、加载风暴限制、panic supervision；
- Player/Farm idle passivation、邮箱/goroutine 回收；
- PostCommitDeliveryGuard 与实时流失效状态。

当前限制：

- `AssignmentProvider` 生产入口只有静态 JSON Snapshot；Discovery/RouteManager Watch 明确 deferred；
- 动态 Assignment 发布、自动 Shard 搬迁、Ready/Handoff 控制面未实现；
- 热 Actor Runtime Snapshot 迁移未实现，当前迁移基线仍是持久化后冷加载；
- `ShardWriteAuthorityStore` 和 HolderLeaseGuard 是独立逻辑组件，尚未接到 RouteIngress、Registry 和每个 Turn。

### 6.5 权威存储、Redis 与 Workflow：当前旧主链可运行，新合同组件未接线

当前服务入口实际使用：

```text
player-svr → blobstore.RedisStore → farm:v118:*:player:<player_id>
farm-svr   → blobstore.RedisStore → farm:v118:*:farm:<farm_id>
friend     → Redis Friend Store
share      → Redis Share Store
workflow   → Redis Workflow Record + Due ZSet + Lease
```

Player/Farm Record 使用确定性 Protobuf blob、版本号和小 Lua CAS；支持 WriterEpoch 字段、写超时按
`TIMEOUT_UNKNOWN` 保守分类。Redis 不可用时服务 fail closed，没有内存权威回退。

已接通的可恢复 Workflow：

- PLANT：PlayerActor 预留种子 → FarmActor 种植 → Commit/Release Reservation；
- HARVEST：FarmActor Settlement → PlayerActor 发作物/经验/图鉴 → 完成 Farm Receipt；
- THEFT：FarmActor 扣可偷份额 → 多 Participant 奖励 → 完成；
- CARE：FarmActor 完成护理 → 农场主/帮助者经验结算 → 完成。

farm-worker 使用 Redis DueIndex、Claim Lease、指数退避和幂等 Participant 接管失败流程。

已经实现但**未接入当前服务入口**：

- `internal/storage/authoritystore` 的通用 AuthorityCommit/CAS/Fence/Unknown 逻辑 Store；
- ShardWriteAuthority Claim/Renew/Transfer/Unassign 状态机；
- `auth-v1` Namespace/HashTag/Slot/KeyBuilder；
- 新 Authority Bucket、Order、ReceiptDue、ResultDirectory 的生产 Redis Repository/Lua。

平台证据边界：公司 Redis 探针证明 Redis 7.1.5、`TIME`、`noeviction`、一个副本 `WAIT 1` 和小 Lua 可用；
不能据此证明平台持久化、Failover、备份恢复、RPO/RTO 或真实 Cluster MOVED/ASK。

### 6.6 核心经营与经济：主链已接通，但规则仍是演示版

已实现：

- 初始金币 100；
- 胡萝卜、番茄、玉米三类配置；
- Buy/Sell 支持一次指定数量，服务端校验范围 `1..999`；
- Plant/成熟时间派生/Harvest/Sell 纵向闭环；
- Farm/Plot/CropCycle 版本校验；
- 收获的库存、经验和图鉴经 PlayerActor 可靠结算；
- `operation_id + payload_hash` Receipt 与 H5 IndexedDB Operation Journal；
- 超时返回 PROCESSING 后通过 GET_OPERATION_RESULT 恢复。

与现行产品规则的差距：

- 当前新农场直接创建 12 块可用地；未实现“一开始 5 块、最多 24 块、玩家付金币点击解锁”；
- 当前成熟时间为 10/15/20 秒演示值；
- Crop 的偷取上限仍为固定整数 3/2/2，每次偷 1，不是冻结的“总产量 20%、单次约 2%”；
- 当前没有地块购买价格、解锁等级与 Unlock Workflow；
- 当前没有仓库容量策略和批量命令原子语义，H5 的“一键”是逐条命令执行。

### 6.7 好友、分享、护理和偷菜：基础闭环已接通，机会导航仍是占位实现

已实现：

- 多人可使用的分享 Token、24 小时 TTL；
- 分享接受建立双向好友关系；
- 好友关系持久化、relation_epoch、删除关系和好友列表更新 Push；
- Join 好友农场与 ActionPermit；
- 好友浇水、偷菜和同场 FarmChange；
- 每位好友每茬最多一次 TheftAttemptReceipt；
- 作物未成熟不能偷；护理已处理返回 NO_CHANGE；
- Care/Theft 跨 Actor Settlement 与 worker 恢复。

当前差距：

- `/api/v1/friends/opportunity` 目前只返回好友列表中的第一个好友，未读取
  FarmOpportunitySummary，也未做 BatchEvaluate 精确检查；
- 好友列表当前只显示“可拜访”，没有准确的可偷/可护理状态；
- 没有 Persistent Projection Redis 中的机会摘要、Seen/Handled Window 和连续下一农场导航；
- 只有一次浇水，没有虫害/除虫 Incident；
- 偷菜当前无宠物拦截；Owner 每次偷菜固定获得 `coin +1`，不是最终宠物截留价值规则；
- 删除/拉黑与正在进行 Workflow 的最终现行合同矩阵仍需按第 33 号重审。

### 6.8 任务：已有最小成长任务，不是最终每日任务系统

当前 PlayerRecord 已持久化 TaskInstance，PlayerActor 在权威 Turn 内推进任务，H5 可查看和领取奖励。

当前只有两条配置：

```text
新手收获：收获 3 个成熟作物
备足种子：购买 3 份种子
```

尚未实现：

- 每日登录、访问好友一次、好友互动一次三条每日任务；
- 每日 0 点按 Period 惰性换代；
- 只显示三条每日 + 一条当前成长任务的序列；
- 连续成长任务领取后推进下一条；
- 任务红点、配置版本、周期 Receipt 与完整恢复矩阵。

### 6.9 植物图鉴：最小闭环已实现

已实现：

- `PlayerRecord.codex_entries` 持久事实；
- 第一次成功收获自己农场作物时，在同一 PlayerActor 奖励 Turn 解锁；
- 重试同一 Harvest 不会重复解锁；
- Worker 接管 Harvest 仍保留 crop definition；
- PlayerBootstrap 下发图鉴，H5 展示已解锁植物和首次收获时间。

尚未实现：

- 活动植物档案与活动结束后的分类展示；
- 灰色剪影完整目录、活动配置和图鉴红点；
- 图鉴奖励或完成度系统。

### 6.10 邮件、宠物、红点：仅合同/尚未实现

当前 Go/Proto/CommandCatalog 中没有可运行的 MailEntry/MailClaim、PetState/Feed/Switch、统一 RedPoint
业务闭环。它们不能因为文档已冻结或旧目录中存在设计稿就标记完成。

## 7. H5 真实完成度

H5 已实现：

- Web 账号/注册表单、Cookie 免密换票、WSS ClientHello；
- RecoveryBarrier、PlayerBootstrap、Pending Operation 恢复；
- Join 自己/好友农场、Fresh Join、版本空洞与 `REJOIN_REQUIRED` 处理；
- Buy/Sell/Plant/Harvest/Water/Theft/ClaimTask；
- 仓库、商店、任务、最小图鉴、好友/分享 UI；
- 弱依赖好友列表失败时显示 Loading/重试，不破坏核心农场；
- Direct WebSocket、staging、wechat 三种构建模式和 Playwright 脚本。

缺失或仍是简化形态：

- 邮件、宠物、土地解锁、虫害、精确好友机会与连续导航；
- 200 人同场展示/压测客户端；
- 最终 33 命令协议迁移。

## 8. 本次重新执行的验证

### 8.1 后端提交态 `4c97191`

为避免主工作树的未提交双 Gate 改动污染结论，本次用 `git archive 4c97191` 在临时目录验证：

```text
go test ./...  PASS
go vet ./...   PASS
```

关键并发包：

```text
go test -race \
  ./internal/gateway/command \
  ./internal/gateway/downlink \
  ./internal/farm/subscription \
  ./internal/farm/ingress \
  ./internal/farm/actor \
  ./internal/logic/actor/runtime \
  ./internal/storage/authoritystore \
  ./internal/actorrouting

结果：全部 PASS
```

### 8.2 H5 提交态 `6737438`

Linux 运行时：Node `v18.20.8`，npm `10.8.2`。

```text
npm run build：PASS
npm test：13 个测试文件中 12 PASS、1 FAIL
          76 项中 74 PASS、2 FAIL
```

失败均来自 `src/test/work-identity.test.ts`：

```text
Web Crypto SHA-256 is unavailable
```

这说明产品构建通过，但当前测试环境没有提供 `globalThis.crypto.subtle`。需要补测试环境 WebCrypto Polyfill、
升级测试 Node 运行时或明确启动参数后再宣称全绿。构建后 H5 Git 工作树仍干净。

### 8.3 未在本次重跑的证据

- Playwright 真实双浏览器经营/好友 E2E；
- W5B 双 Gate Lease Loss/Fresh Join；
- 200 人同场；
- Redis 主从切换、备份恢复；
- 3000 万 DAU 容量压测。

历史 verification 文档可作为历史证据，但不能自动升级为 `4c97191 + 6737438` 的最终组合 PASS。

## 9. 当前最需要继续的工作

按真实缺口而不是文档编号，建议顺序是：

1. 保护并收口当前双 Gate dirty 工作，提交 W5B 真实 Fresh Join 证据；
2. 决定新 AuthorityStore 的主链接入包：将 player/farm 从旧 blobstore 迁移，或明确 P0 暂缓而不双轨宣传；
3. 实现动态 AssignmentProvider/RouteManager 的最小生产形态，再做 Shard Drain/Handoff；
4. 将 12 类公开命令按第 28 号迁移到最终目录，并补齐 Canonical Hash；
5. 按第 32 号重做土地 5→24 解锁、20%/2% 偷菜、虫害与批量经营规则；
6. 实现准确的 FriendOpportunitySummary + BatchEvaluate + 连续导航；
7. 完成每日任务、邮件、简化宠物和活动图鉴；
8. 最后执行 200 人同场、弱网/滚动发布和 3000 万 DAU 容量证据。

## 10. 与其他状态文档的关系

- 第 71/72 号：实施编排，引用本文的当前代码事实；
- 第 60 号：接入与会话验收合同，不替代实现清单；
- `codex-progress-handoff-20260803.md`：早于 W5B 合入的时间点交接，保留历史但已过时；
- Linux 仓库内 `docs/verification/*`：各工作包历史证据；是否仍适用于当前组合必须逐项复核；
- 本文：截至上述 commit 的最新统一实现审计。
