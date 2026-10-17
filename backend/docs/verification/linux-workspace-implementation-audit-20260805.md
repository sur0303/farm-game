# Linux 工作区真实实现进度审计

- 日期：2026-08-05
- 审计对象：`farm-game` 与 `classic-farm-h5`
- 后端分支：`codex/integration-20260803`
- 后端审计提交：`4e8f9d7d800b30f6e5f8a437dc20f0fe1059b043`
- H5 分支/审计提交：`codex/integration-20260803` / `6e582d4dea3674b0f58b6f1d65d91cf07c09f308`
- 定位：替代 2026-08-03 的点时审计，作为下一轮派发的实现事实输入；不修改第 26～37 号业务合同。

## 1. 结论

项目已经越过“搭框架”阶段，进入“产品接线与完整演示闭环”阶段。

当前已经成立的主干是：

```text
HTTPS 登录 / Cookie 免密换票 / WSS ClientHello
→ 双 Gate、SessionRoute Lease、Join/Fresh Join/Drain
→ PLAYER/FARM 双路由域与 Actor Authority
→ Redis WriterClaim/CAS/Workflow/DueIndex/Worker 恢复
→ Buy/Sell/Plant/Harvest/Care/Theft
→ Snapshot/FarmChange/200 人同场广播
```

Plant、Harvest、Care、Theft 不再只是领域包：它们已经进入生产 composition root，并具有真实 Redis、
多进程或 WSS 证据。尤其 Theft 已证明 200 个连接围绕同一热点农场并发裁决、20% 总偷取上限、每位
访客一次、访客库存结算、Workflow 终态和连续 FarmChange。

但项目还不能标记为“P0 产品完成”，原因是：

1. `GET_PLAYER_BOOTSTRAP` 未接线，H5 首屏金币、库存、任务等私有状态没有权威读取通道；
2. H5 为绕过该缺口存在一处未提交的 `manualRecoveryBarrier=false` 临时改动；
3. 好友列表仍走 Gate 内旧 HTTPS 适配，机会接口只返回第一个好友，不是 Summary 粗筛 + BatchEvaluate 精确裁决；
4. `UNLOCK_PLOT`、任务领取、邮件、宠物、图鉴查询等协议已登记但仍 feature-disabled；
5. 当前真实浏览器 v4 全玩法 E2E 没有在最新后端集成头上重新归档；
6. Theft 正确性容量已证明，但 200 人终态 p99 约 10.86s，仍需性能分析和优化；
7. 71/73 仍以 CP3 为当前状态，已经落后于 Plant/Harvest/Care/Theft 与 Agent 控制器的实际合入进度。

因此下一步不应再重做 Actor、路由或通用 Workflow，而应先闭合一个真实浏览器 Hero Scenario，再补
P0d 的最小任务、邮件、宠物和图鉴。

## 2. 审计时工作区事实

### 2.1 后端

```text
path   = /data/home/developer_026/CodeBuddy/20260519141303/farm-game
branch = codex/integration-20260803
HEAD   = 4e8f9d7
dirty  = no
remote tracking = ahead 2（当前 SSH 身份无法访问 git.internal.example.invalid，需恢复凭据后推送）
```

保留的专用 worktree：

```text
farm-game-hero-scenario-20260805  codex/hero-scenario-20260805  dirty，必须保留
farm-game-gatev-e2e              codex/gatev-e2e-20260805       dirty，必须保留
```

两者都基于较早的 Harvest 阶段提交，不能直接当成当前集成结果；后续应保留改动、从最新 integration
重建或显式 rebase，再用于最终 Hero/E2E 证据。

### 2.2 H5

```text
path   = /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
branch = codex/integration-20260803
HEAD   = 6e582d4
dirty  = src/app/App.ts
```

脏改动只把 `manualRecoveryBarrier` 从 `true` 改成 `false`，用于在 Bootstrap feature-disabled 时避免永久
卡在 Recovering。它可以帮助临时演示，但不能成为最终语义；Bootstrap 接线后应恢复严格屏障。

## 3. 当前可重复验证

审计现场执行：

```text
backend: go test ./...                         PASS
H5:      npm test                             74/74 PASS
H5:      npm run build                        PASS
```

已归档的关键运行证据：

| 能力 | 当前证据 | 判断 |
|---|---|---|
| 双 Gate、顶号、Fresh Join | W5B 真实多进程 | 完成 |
| Gate Drain 与浏览器迁移 | Gate A SIGTERM → Gate B → InFarm | 完成 |
| 200 人 Subscription Lease | 200/200 Join、4000 次续租、p99≤5ms | 完成 |
| Buy | 被 Plant/Theft 真实 E2E 作为前置使用 | 已接线；独立产品验收仍可补 |
| Sell | Actor/Ingress/协议及测试 | 已接线；缺独立真实浏览器证据 |
| Plant | 真实 WSS、Redis、Worker 恢复、结果查询 | 完成主链 |
| Harvest | 真实 Redis 跨 Actor Workflow 与恢复 | 完成主链 |
| Care | 8 好友并发同一 Incident：1 SUCCEEDED + 7 NO_CHANGE；奖励一次 | 完成主链 |
| Theft | 200 WSS 热点农场、199 并发、10 次成功、20% 上限、2000 个连续 FarmChange | 完成正确性/热点基线 |
| Task Progress | Harvest/Care/Theft 已推进 TaskDomain | 部分完成；列表/领取未开放 |
| Compendium Unlock | 首次自家 Harvest 已写入图鉴事实 | 部分完成；查询/Seen/H5 未开放 |
| Pet Guard | Theft 已读取并实时同步 GuardSnapshot | 部分完成；解锁/喂食/切换未开放 |
| Mail | 领域、Redis EntryStore、6 封分页等组件存在 | 未接产品主链 |

## 4. 公开命令真实可用面

33 类 CommandCatalog 中当前只有以下 8 类 `Enabled=true`：

```text
JOIN_FARM
GET_OPERATION_RESULT
BUY
SELL
PLANT
HARVEST
CARE
THEFT
```

这意味着“代码里有结构/领域测试”不等于“玩家可以使用”。以下高优先级命令仍被 Gate 明确拒绝：

```text
GET_PLAYER_BOOTSTRAP
GET_FRIEND_LIST / GET_NEXT_FRIEND_OPPORTUNITY
UNLOCK_PLOT
GET_TASK_LIST / CLAIM_TASK / CONFIRM_FRIEND_VISIT
邮件 5 类命令
宠物 4 类命令
GET_COMPENDIUM / ACK_COMPENDIUM_SEEN
正式分享与好友关系写命令
```

H5 已经有 Buy/Sell/Plant/Harvest/Care/Theft 的 UI 与 v4 协议适配；任务按钮也存在，但因后端禁用会收到
`ERR_COMMAND_UNIMPLEMENTED`。好友/分享暂时通过 Gate 内 HTTPS 兼容路径工作，尚未收敛到最终 SO1
好友权威与机会导航链。

## 5. 剩余工作按优先级排序

### P0-1：闭合最新集成头上的 Hero Scenario

这是下一步唯一最高优先级，不应先铺任务、邮件或更多页面。

目标链路：

```text
注册/登录
→ ClientHello / ServerHello
→ GetPlayerBootstrap
→ JoinOwnFarm / SubscriptionReady
→ 批量 Buy
→ Plant
→ 时间派生成熟
→ Harvest
→ 分享建立好友
→ JoinFriendFarm
→ Care
→ Theft
→ 返回自己农场看到库存
→ 断线/重连/RecoverPending
```

必须先完成：

1. 接通 `GET_PLAYER_BOOTSTRAP`，恢复 H5 严格 Recovery Barrier；
2. 把保留的 Hero/GateV 测试改动迁到当前 integration 基线；
3. 用真实 Redis、四进程和真实浏览器跑 v4 E2E；
4. 归档日志、服务提交、配置、运行命令和关键状态断言。

### P0-2：收敛好友发现与连续导航

当前 `/api/v1/friends/opportunity` 只返回好友列表第一人，不能说明该好友真有可操作机会。应复用已经完成
的 SO1 Redis Authority 与 `FarmOpportunitySummary`：

```text
FriendIndex
→ 批量读取 Summary 粗筛
→ FarmReadAPI.BatchEvaluate 精确检查该 viewer
→ 返回下一位真正可操作好友
→ SubscriptionReady 后标记本轮 visited
→ 返回自己农场时继续取下一位
```

保留 HTTP 或切 WSS 都可以，但只能保留一套好友真值和一套机会判断；不能继续让 Gate 兼容 Store 与
SO1 Authority 长期并存。

### P0-3：补个人成长必要入口

1. `UNLOCK_PLOT`：初始 5 块，最多 24 块，玩家主动付金币解锁；
2. Buy/Sell 批量数量在真实浏览器中验收；
3. 补 Care 的虫害 UI，当前 H5 只编码 WATER；
4. 收敛首屏金币、库存、任务摘要、红点等 Bootstrap 版本语义。

### P0d：最小但全部交付

在 Hero Scenario 稳定后按一个 PlayerAuthority 单写列车完成：

```text
Task + Compendium
→ Pet + GuardSync
→ Mail + RedPoint
```

不能同时让多个 Agent 修改 PlayerActor/PlayerIngress。领域包可以复用，重点是 Authority Commit、Receipt、
公开命令、H5 页面和恢复 E2E。

### Q2：性能与生产证据

1. 分解 Theft 10.86s 终态 p99：排队、Redis Lua、Participant RPC、Worker fallback、客户端轮询；
2. 200 人慢读/断连时验证每连接有界队列和广播降级；
3. 登录峰值、300 好友列表/机会查询、Actor 内存、Redis command/byte 放大；
4. Redis ACL/Cluster/Failover/NOSCRIPT/持久化探针；
5. 30 分钟以上 Soak、滚动发布与故障恢复；
6. 根据 3000 万 DAU、每天 5～6 次登录、单次少于 5 分钟形成容量计算和压测映射。

## 6. 下一轮 Agent 编排

不要立即并行四个玩法写者。推荐 Wave H0：

| 包 | 类型 | 写集 | 依赖/备注 |
|---|---|---|---|
| `BOOTSTRAP-1` | 编码 | player bootstrap ingress、CommandCatalog 可用性、必要 Proto 映射 | PlayerAuthority 唯一写者 |
| `HERO-E2E-1` | 编码/测试 | `cmd/farm-scenario`、protocolbot、独立 E2E 脚本与 evidence | 不改生产 Actor；消费当前主链 |
| `THEFT-PERF-1` | 只读分析/测试 | loadtest、指标、verification | 不改业务语义，先定位 10.86s |
| `I0` | 集成 | composition root、回归、checkpoint | 前三包完成后单写 |

完成后 Wave H1：

| 包 | 类型 | 写集 |
|---|---|---|
| `FRIEND-NAV-1` | 编码 | SO1 Adapter、Summary/BatchEvaluate、旧 HTTP 兼容路径收敛 |
| `UNLOCK-1` | 编码 | Unlock Coordinator + Player/Farm Participant；与 PlayerActor 写者串行 |
| `H5-HERO-1` | 编码 | App/feature reducer/E2E；先解决当前脏改动 |
| `Q2-HERO-1` | 验收 | 最新真实浏览器 Hero、弱网/重连/慢连接 |

之后才进入 P0d。Agent 控制器用于创建 worktree、锁写集、执行测试门和保存 handoff；当前不需要继续扩展
控制器功能。

## 7. 下一检查点完成条件

建议下一稳定点命名为 `hero-p0-core-20260805`，满足以下条件后才能发布：

```text
[ ] 后端 integration 与远端一致、无未推送提交
[ ] H5 integration 干净，Recovery Barrier 使用最终语义
[ ] GET_PLAYER_BOOTSTRAP Enabled 且真实读到钱包/库存/任务摘要
[ ] 最新后端 + 最新 H5 的完整 Hero 浏览器 E2E 通过
[ ] Plant/Harvest/Care/Theft 结果均可通过原 operation_id 恢复
[ ] 好友进入与最终 ActionAdmission 使用同一权威关系
[ ] 全仓 Go 测试、H5 测试/build、受影响包 race 通过
[ ] Evidence Bundle 记录 Commit、配置、日志和时间
```

## 8. 非功能性整理项

- 测试机残留大量 `gatev-redis-*`、`po1-final-redis-*` 容器，说明 E2E 清理逻辑需要补 finally/TTL；清理前先确认
  没有活跃任务引用，不应直接批量删除。
- 后端当前本地 tracking 显示 ahead 2，但本次 SSH 身份无法读取 `developer_001@example.invalid`；先修复凭据再推送，不能
  误称远端已包含 Agent 控制器最终两次提交。
- Hero 与 GateV 两个保留 worktree 不删除；在重建前先归档各自 patch 和未跟踪文件。

