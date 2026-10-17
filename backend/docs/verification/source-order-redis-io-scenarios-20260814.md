# SourceOrder 分支：相对压测加了什么，热路径怎么降 Redis IO

日期：2026-08-14  
工作树：`farm-game-source-order-migration-20260813`  
分支：`codex/source-order-migration-20260813`  
压测对照：`codex/friend-opportunity-convergence-20260812` @ `af8f11d`（工作树 `farm-game-friend-opportunity-convergence-20260812`）  
对照交接：`docs/verification/source-order-migration-handoff-20260814.md`（切流边界、Lua 版本、同槽结论）

这份文档回答四件事：

1. **这个分支比压测分支多了什么**（SourceOrder 切流 + 支撑系统移植，不是压测树上的增量）。
2. **图鉴 / 任务 / 宠物 / 邮件 / 商店 / 红点** 现在做到哪、玩家能感知什么、默认部署会不会静默失效。
3. **种收浇偷怎么降 Redis IO**，和旧 `{work-v1}` WorkRecord 差在哪。
4. **哪些故意没动**（同槽、联合提交、邮件 worker 默认关）。

后面 Redis hop 数是热路径成功一次的典型 EVAL / HMGET，不是压测曲线。种收浇偷的业务效果与切流前一致。

---

## 相对压测分支加了什么

压测还在旧树跑，**不要把本分支合回压测树当容量结论**。本树从压测 HEAD `af8f11d` 拉出，同一 git 分支不能检出两次。

已提交（`af8f11d..HEAD`，HEAD = `f7ad8bd`）：

| 提交 | 加了什么 |
|---|---|
| `4f16fcf` `switch-public-farm-commands-to-source-order` | 公网种/开地/收/浇/偷 source 切 SourceOrder；删 `{work-v1}` FirstHop 和 farm-worker legacy drain |
| `2826e26` `prove-care-and-theft-on-real-wss` | 真进程 WSS：同一棵胡萝卜 CARE + THEFT |
| `aed88ce` `port-support-economy-onto-source-order` | 把商店狗粮、任务、图鉴、邮件、宠物、GUARD_SYNC、live-bot **移植到这棵 SourceOrder 树**，不 merge 旧 bot 分支、不动当时的 authority Lua |
| `f7ad8bd` `prove-support-features-on-source-order-wss` | 同一条 WSS 链上证明欢迎信 / 日任 / 图鉴解锁 / 卖菜 / 解锁喂狗，且这些单不是 `{work-v1}` |

工作区里还有未提交的 Redis IO 瘦身（V10 稀疏 ARGV、v2 永远稀疏写、Direct `ClaimAndCompleteWorkV1`、THEFT dest 不再建 WorkRecord）。文档后半按**当前工作区**写。

压测分支上已经有的、本轮没当新产品加的：路由 / Holder / 好友机会 wakeup / 种收浇偷的旧 WorkRecord 权威、容量 harness。  
压测环境**没有**部署完整图鉴/任务/宠物/邮件面板闭环——那是 `codex/support-economy-r1` / `codex/bot-support-economy-20260813` 上做的，再 port 进本树。

两层不要混：

```text
压测 af8f11d
  └─ SourceOrder 切流（种收浇偷不再写 WorkRecord FirstHop）
       └─ 支撑系统移植（图鉴/任务/宠物/邮件/商店狗粮）
            └─ 工作区：热路径 Redis IO 再瘦一刀 + THEFT dest 也切 SourceOrder
```

---

## 图鉴 / 任务 / 宠物 / 邮件 / 商店 / 红点

命令在 `internal/gateway/commandcatalog` 都有真 handler，没有空实现。**代码通了 ≠ 默认部署玩家能感知到。** 下面是本树上的实测口径（和 support-economy 交接一致，只是现在跑在 SourceOrder 上）。

WSS 证明入口：`PO1_E2E_MODE=SOURCE_ORDER_WSS` 的 `cmd/po1-plant-e2e`，种之前领欢迎信 + 日任，收之后查图鉴、卖菜、金币解锁狗并喂食。欢迎信挂的是 `MAIL_DELIVERY_ORDER` SourceOrder，不是 WorkRecord。

### 商店 / 狗粮

压测商店只能卖三种作物种子。本分支加了 `internal/shopcatalog`：种子仍投影自 `cropcatalog`，补给从 101 起，当前只有狗粮。

| 项 | 值 |
|---|---|
| 狗粮 offer | id=101，物品 `#9001`，30 金，`OfferVersion=1` |
| BUY | 必须带非 0 `expected_offer_version`，对不上直接拒（32 号合同） |
| 新号金币 | 100；解锁狗 100，再买狗粮 30 |

没有狗粮来源时，宠物面板能喂但背包永远空。这是相对压测最明显的经济增量。

### 图鉴

权威在 Player。**唯一生产解锁点**是收获 Participant：`internal/player/participant/harvest.go` 调 `compendium.DecideUnlock`。

P0 条件写死为「自己种在自己农场、自己收成」（`internal/player/compendium/unlock.go`）。Catalog 由当前 `cropcatalog` 投影（`DefaultCatalogV1`）。

| 会解锁 | 不会解锁 |
|---|---|
| 自己种、自己收（SourceOrder HARVEST dest 入包的同一次 Player Lua） | 偷菜、浇水奖励、邮件附件、只买种子、喂宠物 |

不依赖 farm-worker，默认部署只要 harvest dest 组成成功就会生效。  
旧 `UnlockCodex` / `CodexEntries` 仍可能被内部 `ApplyReward` 写到，`StateFromPlayerRecord` 会把这类条目当成已看过，**不出红点**。Authority CARE 已不走那条。

红点：`HasNewUnlock = SeenRevision < Revision`，`ACK_COMPENDIUM_SEEN` 单调推进 seen。

### 任务

两套并存，读只认一套。

**读/领**：`internal/player/task`（`TaskDomain`）。`GET_TASK_LIST` / bootstrap / `CLAIM_TASK` 走这里。`task/mapper.go` 明确丢掉 legacy `PlayerRecord.Tasks`。

P0 配置（`internal/player/task/repository.go`）：

| ID | 类型 | 奖励 |
|---|---|---|
| `daily.login.v1` | 每日登录 | Exp:1 |
| `daily.friend_visit.v1` | 每日访友 | Exp:1 |
| `daily.friend_interact.v1` | 每日好友互动 | Exp:1 |
| `growth.harvest.v1` | 成长：累计收 1 次 | Exp:1 |

时区 `Asia/Shanghai`，日切惰性，不扫全服。`CLAIM_TASK` 已能发金币/物品/经验/宠物解锁，但 **P0 配置没有任何 `PetUnlock`**——线通了，表是空的。

**僵尸写入**：`internal/taskcatalog`（「新手收获 3 次」「商店买 3 份种子」）生产里还在写：

- `registry.normalize` / `initialRecord` → `EnsureTasks`
- BUY → `AdvanceTask(EventBuyOffer)`
- `GRANT_HARVEST_REWARD` → `AdvanceTask(EventHarvest)`

客户端看不到、`CLAIM_TASK` 领不到。清理是独立工作项。

### 宠物 / 守卫

Player 持有解锁、当前守卫宠、喂食截止。P0 两只配置宠。新号 `OwnedPetIDs` 空，必须先 `PET_UNLOCK_BY_COIN`（100 金），再 switch/feed 才有守卫。没有公开的 `PET_UNLOCK_BY_TASK` 命令。

防卫链：

```text
PET_FEED / PET_SWITCH
  → 与 Player 记录原子提交 GUARD_SYNC_ORDER
  → GUARD_SYNC worker
  → FarmActor.ApplyGuardSnapshot
  → DecideTheft 可读到 BLOCKED_BY_GUARD
```

本树 `GUARD_SYNC_WORKER_ENABLED` 在 authority 模式下**默认开**（`cmd/farm-worker`：`parseBool(..., mode == ModeAuthority)`）。压测/旧 k8s staging 曾写死 false，喂了狗农场侧没有快照，偷菜照常成功且不报错。

### 邮件

读/已读/删/领附件都是真实现。领取进 PlayerActor，资产和 Claim Receipt 同一次提交。条目在独立 `PlayerMail` Redis 域，每页最多 6 封。

**生产唯一发信点**：首次 `GET_PLAYER_BOOTSTRAP` 挂 `MAIL_DELIVERY_ORDER`（模板 `WELCOME_V1`，3 个胡萝卜种子），fast path 立即插入。本树 WSS 证明这张单是 SourceOrder。

没有：任务奖邮件、运营公告、离线结算邮件。`internal/player/maildelivery.DeliveryService` 生产调用方为零。

Worker 默认仍关（和压测一样，本轮 SourceOrder 切流明确不打开）：

| 变量 | 默认 | 关掉的后果 |
|---|---|---|
| `MAIL_DELIVERY_WORKER_ENABLED` | 关 | 欢迎信靠 bootstrap fast path；fast path 失败则订单挂着没人兑现 |
| `MAIL_WORKER_ENABLED` | 关 | 领取已在 Actor 内完成；投影靠读路径 overlay |
| `MAIL_MAINTENANCE_ENABLED` | 关 | 过期信不 GC（读路径仍按 nowMs 过滤） |

### 红点（FeatureSummary）

| 项 | 服务端 | 说明 |
|---|---|---|
| 任务 | 真 | `claimable_count > 0`；不算 `rec.Tasks` |
| 邮件 | 真 | 未读或可领附件 |
| 图鉴 | 真 | seen < revision |
| 宠物 | **协议无红点字段** | H5 曾用「没有生效守卫就亮」，新号进游戏宠物角标一直 1，要花 100 金解锁再喂才灭 |

---

## 0. 一句话（种收浇偷的 Redis）

---

## 0. 一句话

原来跨 actor 命令的耐久边界是 **generic WorkRecord**（`{work-v1}` 一整份 protobuf + DueIndex + 逐步 Claim/Complete）。  
现在耐久边界是 **source actor 的一次 Lua**：改模型、写 participant receipt、挂 SourceOrder、进 DueIndex。dest 用 Direct 打第二跳；成功后 **一次 `ClaimAndCompleteWorkV1`** 把订单收掉。

没有改 `ShardFor`，没有把 Player/Farm 同槽，没有联合提交。省下来的是：

- 不再为 FirstHop 另写一张 WorkRecord HASH
- actor commit 的 EVAL ARGV 不再把整份 CandidateState 再传一遍
- 生产永远写 Model Field v2 稀疏 field，不再走 `ENABLED=false` 的全量 v1 `record` 回写
- PENDING SourceOrder 不写空闲 lease 字段；Complete 后 `HDEL` 掉 lease
- Direct 成功路径把 Claim + Complete 收成一次 EVAL
- THEFT dest 不再 CreateIfAbsent / Claim / 逐步更新 WorkRecord

---

## 1. 共享层：所有场景都吃到的瘦身

这些改动跟「种/收/浇/偷」正交。一次 actor commit 或一张 SourceOrder 都会走。

### 1.1 `CommitActorMutationWithOrdersV10`：ARGV 只传脏 field

HSET 本来就是 sparse upsert。旧脚本把 **整个 CandidateState** 再塞进 EVAL ARGV，网络 IO 最大、分片不变。

V10 只传变更 field/value。Lua 在 HASH 上 merge。单测：`TestCommitSparseArgvOmitsUnchangedModelValues`。

种一棵菜、偷一次菜，Farm/Player HASH 里大部分 field（背包其它格、其它地块）不再随 ARGV 往返。

### 1.2 生产永远 `FullConversionPolicy`

`fieldv2.ProductionConversionPolicy` 仍解析 env（写错会启动失败），但 **不再允许 `ENABLED=false` 留全量 v1 写路径**。生产二进制固定：

- 下次 load 把 v1 行迁到 v2
- 之后每次 commit 只 upsert 变更的 v2 field，不再把整份 `record` blob 写回去

命令结果不变，变的是物理 HASH 布局。

### 1.3 PENDING SourceOrder 不写空闲 lease

创建订单时仍写身份、payload、state、work_version、retry、deadline。  
**不写**：

```text
lease_holder_id / lease_holder_boot_id / lease_epoch
lease_token_hash / lease_expire_at_ms
current_step / last_error_class
completed_at_ms / reconcile_required_at_ms
```

读路径把缺字段当空闲（`GetSourceOrder` / `ClaimWorkV4`）。Direct 热路径多数订单从未被 worker Claim，这些空字段以前每单白写一遍。

### 1.4 `CompleteWorkV3`：完成后 HDEL lease

Complete 只 HSET `state=COMPLETED`、`current_step`、`next_due_at_ms=0`、`completed_at_ms`、`updated_at_ms`，然后：

```text
HDEL lease_holder_id lease_holder_boot_id lease_epoch
     lease_token_hash lease_expire_at_ms
```

已完成订单不再长期带着空 lease 字段。`ALREADY_COMPLETED` 在 fence 之前判断，删字段不影响幂等 Complete。

### 1.5 Direct：`ClaimAndCompleteWorkV1`

热路径 dest 成功后，不再 `ClaimWorkV4` 再 `CompleteWorkV3`（两次 EVAL）。  
participant 已证明终态后，一次 EVAL：无主/过期订单直接 complete；若 farm-worker 已持有 lease，返回 `LEASE_HELD`，留给 recovery。

Worker 恢复路径 **仍是 Claim → RPC → Complete**。RPC 期间必须占着 lease，不能合成一跳。

### 1.6 本轮明确不做

| 不做 | 原因 |
|---|---|
| Player/Farm 同槽、改 `ShardFor` | 要换 HashVersion、迁存量；只同槽、两次 turn，热路径 hop 几乎不变 |
| 自己种联合提交（一次 Lua 写两边） | 改「各写各的」契约，和同槽同一季度才划算 |
| 改 V10 / ClaimWorkV4 / CompleteWorkV3 **body 却不升版本名** | `TestScriptSHAGoldens` 钉 SHA-1 |
| 打开 `MAIL_DELIVERY_WORKER` | 与本轮无关 |

---

## 2. 场景对照

读法：每个场景先写 **现在怎么实现**，再写 **原来多了什么 Redis**，最后写 **好处**。  
Pair 权威（加好友、admission、permit）本轮没改语义；THEFT/CARE 的「能不能做」仍由 Pair 说了算。

### 2.1 PLANT（自己种）

**现在**

1. farm-svr 公网 `PLANT` → `plantSourceOrder`。
2. Player coordinator `RESERVE_SEED`：一次 Lua 扣种子、写 receipt、挂 `PLANT` SourceOrder、进 Player DueIndex。
3. `Direct.DispatchOnce`：`GetSourceOrder` → Farm `COMMIT_PLANT`（一次 Lua 改地块）→ `ClaimAndCompleteWorkV1`。
4. 公网结果读 Farm snapshot + Player 背包。

失败：Player 幂等冲突 → 公网 REJECTED；dest RPC 超时 → PROCESSING，订单留在 DueIndex，farm-worker 扫 PLAYER bucket 补 dest。

**原来**

1. `CreateIfAbsent` 一张 `{work-v1}` WorkRecord（整份 protobuf + DueIndex）。
2. 每一步 participant 都要 WorkRecord fence（Get/Claim）+ 写回 step。
3. Player 扣种子、Farm 下种各一次 actor Lua。
4. WorkRecord Complete。
5. farm-worker 另有 `{work-v1}` Plant loop。

**差在哪 / 好处**

| | 原来 | 现在 |
|---|---|---|
| 耐久边界 | WorkRecord HASH + 两跳 actor | Player 一张 SourceOrder + 两跳 actor |
| FirstHop Redis | CreateIfAbsent + 逐步更新 WorkRecord | **0 张 WorkRecord** |
| dest 收单 | Claim EVAL + Complete EVAL | **1 次 ClaimAndComplete** |
| 崩溃恢复 | 扫 `{work-v1}` DueIndex | 扫 Player SourceDueIndex，family=`PLANT` |

自己种仍是两 hop（Player 再 Farm）。省的是中间那张 WorkRecord 和 ARGV 全量回写，不是 gRPC 次数。

---

### 2.2 UNLOCK_PLOT（自己开地）

和 PLANT 同构，source 仍是 Player。

**现在**：Player 一次 Lua 扣金币 + 挂 `UNLOCK_PLOT` 订单 → Direct 打 Farm `COMMIT_PLOT_UNLOCK` → ClaimAndComplete。

**原来**：WorkRecord FirstHop + 逐步 fence + `{work-v1}` Unlock loop。

**好处**：与 PLANT 相同。开地是低频，但每单少一张 WorkRecord、少两次 lease EVAL。

---

### 2.3 HARVEST（自己收）

**现在**

Farm 是 source。`FarmActor.HarvestOwner` 一次 Lua：清空地块、写 receipt、挂 `HARVEST`/`GRANT` SourceOrder、进 Farm DueIndex。  
Direct 打 Player `GRANT_HARVEST_REWARD` 入包 → ClaimAndComplete。

产量口径仍是 `cropcatalog.SnapshotBaseYield`（胡萝卜 500），不是旧 `BaseYield=10`。

**原来**：WorkRecord 先 Create，Farm 收地、Player 入包各带 WorkRecord fence，再 Complete。

**好处**：收地权威和订单在同一次 Farm Lua 里提交，不会出现「地已空、WorkRecord 还没建出来」。dest 入包失败时订单仍 due，worker 扫 FARM bucket 补发，不必重建 WorkRecord 状态机。

---

### 2.4 CARE（好友浇水）

CARE 有两层，不要混：

1. **Farm source**：Incident 结算 + `CARE_REWARD` SourceOrder（给浇水人 1 点经验）。
2. **Pair CARE**：好友 admission 通过后的 Pair 单据（和 THEFT 共用 Pair DueIndex，处理器是 `PairCareProcessor`）。

**现在（dest 发奖）**

Farm 一次 Lua 写下 Incident 结果并挂 `CARE_REWARD`。  
Direct：`GetSourceOrder` → Player `APPLY_CARE_REWARD` → ClaimAndComplete。  
无 admission → fail-closed `ERR_FRIEND_PERMIT_INVALID`，不是 UNAVAILABLE。  
不再写 `{work-v1}` CARE WorkflowRecord。

farm-worker FamilyCare 扫 FARM SourceDueIndex 补 dest；Pair CARE 结果单仍由 Pair 队列消化。

**原来**

CARE 走 WorkRecord / legacy workflow：Create 状态机、逐步 participant、Complete。dest 与 source 的中间状态写在 `{work-v1}` 上。

**好处**

- 热路径少一张 CARE WorkRecord。
- 浇水人经验仍只给 visitor（`RewardExp=1`），owner 不加 CARE 经验，语义没改。
- Pair 仍是「能不能浇」的权威；降 IO 没有把 admission 挪到 Farm。

---

### 2.5 THEFT（好友偷菜）

这是本轮相对交接文档变化最大的一块。旧交接还写着「THEFT dest 仍用 generic WorkRecord」——**生产路径已经切走**。

THEFT 拆四段看。**Pair 恢复不是删好友**，是把已经 admission 通过、但 dest 还没落地的偷菜单继续做完。

#### A. Pair admission（没改语义）

公网 THEFT 先过好友权威：permit、quota、`FRIEND_ACTION_DISPATCH_ORDER`、`RESULT_PUBLISH_ORDER`、target binding sidecar。

这层 Redis（Pair HASH / Pair DueIndex / Projection target binding）还在。本轮没有把它并进 Farm Lua。

#### B. Source（目标农场 DecideTheft）

**现在**：`CommitTheftSource` → Farm **一次 Lua**：DecideTheft（改地块/产物）+ 挂 `THEFT_VISITOR`（给访客发奖）+ 必要时 `THEFT_GUARD`（给主人记守卫收益）+ DueIndex。  
farm-svr `NewDispatch` **必须**带 `SourceCommit`，禁止 fallback `coord.Start`。

**原来**：`CreateIfAbsent` dest WorkRecord，再以 Coordinator 调 Farm `DECIDE_THEFT`，每一步都要 WorkRecord Get/Claim/写回 step。Farm 模型提交和「工作流进度」是两次 Redis 权威。

#### C. Dest（访客入包 / 主人 Guard）

旧 Coordinator 冻结计划：

```text
DECIDE_THEFT(Farm) → SETTLE_THEFT_VISITOR(Player) → CREDIT_GUARD_INCOME(Owner, 若 guard_coin_value>0)
```

每一步都围着 **同一张** `{work-v1}` WorkRecord：CreateIfAbsent、Claim、执行 participant、把 step/result 写回整份 protobuf、最后 Complete。  
farm-worker Pair 恢复还会 `CreateIfAbsentOrContinue` 这张 dest 单。

**现在**：

- dest 单据就是 Farm 挂上的 `THEFT_VISITOR` / `THEFT_GUARD` SourceOrder。
- 热路径：`careDirect.Enqueue` → `DispatchOnce` → `GetSourceOrder` → Player dest Lua → `ClaimAndCompleteWorkV1`。
- 崩溃：farm-worker 扫 FARM SourceDueIndex，family=`THEFT_VISITOR` / `THEFT_GUARD`，Claim → RPC → CompleteWorkV3。
- **不创建 dest WorkRecord**。旧路径遗留的 `{work-v1}` THEFT 单 **不再认领、不再排空**。

Farm ingress：带 `SOURCE_PERSISTENCE_BUCKET` 的 `DECIDE_THEFT` 跳过 WorkRecord fence，只做 Pair dependency 校验。

#### D. Pair 恢复 + 结果单

**Pair dispatch 恢复**（`FRIEND_ACTION_DISPATCH_ORDER` due）：

1. farm-worker 扫 Pair DueIndex，认领 Pair lease（这是 Pair 单的 lease，不是 dest WorkRecord）。
2. `SourceOrderAdvancer` 对 Farm `ResumeStep(DECIDE_THEFT)`，幂等 SourceCommit。
3. 推导 `THEFT_VISITOR` / `THEFT_GUARD` 订单 id，complete Pair lease。
4. dest 仍由 SourceOrder worker 执行。

**结果单**（`RESULT_PUBLISH_ORDER`）：按 Pair admission 终态（SUCCEEDED/REJECTED）写 ResultDirectory，然后 complete Pair 结果单。  
**不再等 dest WorkRecord Complete。** dest 有自己的 SourceOrder；结果目录表达的是「这次偷菜在 Pair 上被接受/拒绝」，不是 dest 逐步状态机。

#### 对照与好处

| | 原来 | 现在 |
|---|---|---|
| dest 耐久物 | 1 张 THEFT WorkRecord（整份状态机） | 1～2 张 Farm SourceOrder（VISITOR/GUARD） |
| DecideTheft Redis | WorkRecord Create/Claim/Update + Farm Lua | **Farm 一次 Lua**（模型+订单） |
| dest 每跳 | WorkRecord fence + actor Lua + 写回 step | actor Lua + Direct ClaimAndComplete |
| Pair 恢复 | CreateIfAbsent dest WorkRecord 再跑三步引擎 | SourceCommit + Pair lease Complete |
| 结果单 | 等 WorkRecord 终态 | Pair admission 即可 complete |
| 旧 `{work-v1}` THEFT 单 | worker 会捡起来跑 | **忽略** |

好处：

- 热路径少一整张 WorkRecord 的 Create/逐步更新/Complete，以及 farm-svr 上的 THEFT Coordinator Redis 仓库。
- Farm Decide 和 dest 订单同一次 CAS，不会「地已偷、工作流单还没建」。
- Pair 仍是执行接受权威；dest 失败可独立重试 SourceOrder，不必把三步状态机从 protobuf 里恢复。
- farm-worker 不再为 THEFT dest 打开 `workflow.NewRedisWorkRepository`。

代价（有意保留）：

- Pair admission 的 Redis 还在（跨人许可不能并进 Farm）。
- dest 仍是别人的 Player actor，同槽也消不掉这两张 SourceOrder。
- Direct 前仍有一次 `GetSourceOrder`（只读 HMGET，用来组 dest payload）。

---

### 2.6 BUY / SELL / 单 actor 命令

player-svr 本地权威，不走 SourceOrder，也没有 WorkRecord FirstHop。本轮 IO 瘦身只来自 **v2 稀疏 field + V10 ARGV**（买种子、卖作物、买狗粮仍会 commit Player HASH）。

相对压测：商店从「只能卖种子」变成 `shopcatalog`（种子 + 狗粮 101），BUY 必须带 `expected_offer_version`。见上文「商店 / 狗粮」。

---

## 3. 热路径 Redis 形态（成功一次）

「+sparse」= 同一次 Lua 的 ARGV/HASH 比全量 CandidateState / 全量 v1 `record` 小，不是少一次 EVAL。

| 场景 | 原来（WorkRecord FirstHop） | 现在 |
|---|---|---|
| PLANT | WorkRecord Create + Player Lua + WR 更新 + Farm Lua + WR 更新 + WR Complete | Player Lua+订单 + GetSourceOrder + Farm Lua + ClaimAndComplete |
| UNLOCK | 同上结构 | 同 PLANT |
| HARVEST | WorkRecord Create + Farm Lua + WR 更新 + Player Lua + WR 更新 + WR Complete | Farm Lua+订单 + GetSourceOrder + Player Lua + ClaimAndComplete |
| CARE dest | CARE WorkRecord 状态机 + 逐步 fence | Farm Lua+订单 + GetSourceOrder + Player Lua + ClaimAndComplete |
| THEFT | Pair + WorkRecord Create + 3 步（每步 WR fence + actor Lua + WR 写回）+ WR Complete + 结果单等 WR | Pair + Farm Lua（Decide+VISITOR/GUARD）+ 每 dest：Get + Player Lua + ClaimAndComplete；结果单不等 WR |
| Direct 收单 | Claim + Complete = 2 EVAL | 1 EVAL（`ClaimAndCompleteWorkV1`） |
| Worker 补单 | Claim + RPC + Complete（仍 2 EVAL，lease 必须跨 RPC） | 同左，只是扫的是 SourceDueIndex |

Actor commit 额外永远享受 V10 稀疏 ARGV 和 v2 field upsert。

---

## 4. 恢复路径现在怎么扫

| Family | 扫什么 | 做什么 |
|---|---|---|
| Plant / Unlock | PLAYER SourceDueIndex | dest Farm `COMMIT_*` |
| Harvest | FARM SourceDueIndex | dest Player `GRANT_HARVEST_REWARD` |
| Care | FARM SourceDueIndex + Pair CARE 结果单 | dest Player `APPLY_CARE_REWARD` |
| THEFT_VISITOR / THEFT_GUARD | FARM SourceDueIndex | dest Player 结算 / Guard |
| FamilyTheft | Pair DueIndex | SourceCommit `DECIDE_THEFT`，**不**建 WorkRecord |

旧 `{work-v1}` Plant/Unlock/Harvest/THEFT dest loop **不启动**。  
`internal/workflow` 里的 Legacy 适配器和 `theftonline.Coordinator` / `WorkerAdvancer` 代码还在，**生产不接线**。不要把单测里的 WorkRecord Coordinator 当成线上路径。

---

## 5. 好处汇总

1. **少一类权威 HASH**：跨 actor 命令不再为编排另写 `{work-v1}`。订单跟 source actor 同一次 Lua、同一个 hash tag。
2. **少往返字节**：V10 ARGV + v2 稀疏 field，改一格背包不必把整份模型再推一遍。
3. **少空字段**：PENDING 不写 idle lease；Complete 后 HDEL。DueIndex 里大量未认领订单更瘦。
4. **热路径少一次 EVAL**：Direct 成功用 `ClaimAndCompleteWorkV1`；worker 补单仍两跳，因为 RPC 期间要占 lease。
5. **THEFT 少一整条 dest 状态机**：三步 WorkRecord 变成「Farm 一跳 + 最多两张 SourceOrder」。Pair 恢复只补 SourceCommit，不 CreateIfAbsent。
6. **失败面更干净**：dest 超时 = SourceOrder 仍 due；不必从半截 protobuf 步骤里猜。旧 WorkRecord 遗留单直接忽略，避免双引擎抢同一笔业务。

行为不变的部分（有意）：

- 两个 actor、两次 turn、两次 receipt（公网 `operation_id` 重试）。
- Pair 决定能不能浇/偷。
- Worker 抢占、lease fence、幂等 Complete。

---

## 6. 关键符号

| 符号 | 角色 |
|---|---|
| `CommitActorMutationWithOrdersV10` | 稀疏 ARGV + 原子挂 SourceOrder |
| `ClaimWorkV4` / `CompleteWorkV3` | worker 补单；缺字段=空闲；完成后 HDEL |
| `ClaimAndCompleteWorkV1` | Direct 热路径一跳收单 |
| `fieldv2.FullConversionPolicy` | 生产写路径 |
| `plantSourceOrder` / `unlockPlotSourceOrder` / `harvestSourceOrder` | 公网 FirstHop |
| `CommitTheftSource` / `BuildTheftSourceCall` | THEFT Farm source |
| `theftonline.Dispatch` + `SourceCommit` | 公网 THEFT 传播，禁止 Coordinator |
| `theftonline.SourceOrderAdvancer` | Pair dispatch 恢复 |
| `theftonline.ResultOrderProcessor` | 结果单不等 WorkRecord |
| `carerecovery.DirectDispatcher` | dest 热路径 |
| `skipsDecideTheftWorkRecordFence` | source-order DECIDE_THEFT 跳过 WR fence |

Lua 源码不可原地改 body。升版本名，先跑 `go test -run TestScriptSHAGoldens ./internal/storage/redis/authority/`。
