# 36 · 简化宠物守护系统合同

版本：`v1.1-pet-state-version-guard-sync-hardening-freeze`  
日期：2026-08-03  
状态：`STABLE_INPUT`  
定位：在第 26～35 号合同之上，收敛 P0 宠物系统的最小闭环。P0 只实现拥有宠物、切换当前
守护宠物、消耗狗粮延长守护时间，以及偷菜时使用守护快照裁决；同时冻结宠物状态版本、不可变
操作结果、GuardSync 水位和公开农场外观版本边界，不扩展成完整宠物养成系统。

## 0. 上位合同与不重复定义

- [26 · 接入与会话](./26-access-session-contract.md)：在线 Session、命令准入和下行；
- [27 · 路由、Actor 与 Workflow](./27-access-routing-actor-workflow-freeze.md)：`PET_FEED/PET_SWITCH`
  的 Player 首跳、`PetState`、`GuardSnapshot`、`GuardSyncOrder`、Dispatcher 与 worker；
- [28 · 公网协议与 CommandCatalog](./28-command-catalog-public-protocol.md)：命令信封、结果、幂等和恢复；
- [29 · 内部 RPC](./29-internal-rpc-trusted-context.md)：Participant、Worker 恢复与可信调用；
- [30 · Actor 权威 Store](./30-actor-authority-store-contract.md)：Authority Commit、Receipt、CAS 和 Unknown；
- [31 · Redis 物理合同](./31-actor-authority-redis-physical-contract.md)：Player/Farm Authority、来源同槽
  Order、DueIndex 和容量约束；
- [32 · 核心经营场景](./32-core-business-scenario-contract.md)：偷菜额度、宠物截留、主人金币补偿和
  `GuardDecisionSnapshot` 的最终语义。

冲突时服从上位合同。本文只定义宠物功能本身，不重新定义偷菜每次可偷数量、随机算法、主人补偿
金额或跨 Actor 通用恢复框架。

---

## 1. 已确认产品范围

### 1.1 P0 必须实现

1. 宠物必须先解锁；P0 支持普通金币购买或任务奖励解锁；
2. 玩家拥有一个有界、不可重复的已解锁宠物集合；
3. 玩家可以选择其中一只作为当前守护宠物；
4. 玩家可以消耗仓库中的狗粮，延长守护有效时间；
5. 好友偷菜时，FarmActor 根据当时已经生效的守护快照判断是否截留；
6. 宠物页面能展示未解锁/已解锁、当前守护宠物和守护剩余时间。

### 1.2 P0 明确不实现

- 宠物繁殖、合成、升星、技能树、装备和战斗；
- 宠物单独等级、经验、体力、心情、疾病等复杂成长状态；
- 宠物之间的社交、交易、排行榜和独立任务链；
- 独立 `PetActor`、`pet-svr`、宠物专用 MQ 或宠物专用 Scheduler；
- 因守护到期而主动写一次“失效”状态。守护是否有效由可信时间惰性计算。

这些能力以后若确有产品需求，应作为新版本扩展，不得提前污染 P0 数据模型和主链。

---

## 2. Q36-01 冻结：权威、部署与路由边界

### 2.1 最终边界

```text
player-svr
├── PlayerActor(player_id)                    # PLAYER Route Domain
│   ├── PetDomain / PetState                  # 宠物唯一长期权威
│   │   ├── owned_pet_ids
│   │   ├── active_guard_pet_id
│   │   ├── guard_until_ms
│   │   ├── pet_state_version
│   │   ├── guard_version
│   │   └── guard_rule_snapshot
│   └── PetOperationReceipts                  # 有界防重记录，不内嵌为PetState无界数组
└── GuardSyncWatermarkStore                   # 同PLAYER Bucket固定大小水位；不属于PetState

farm-svr
└── FarmActor(farm_id)                        # FARM Route Domain
    └── GuardSnapshot                         # 偷菜裁决所需的版本化副本
        ├── schema_version
        ├── active_guard_pet_id
        ├── guard_until_ms
        ├── guard_rule_snapshot
        └── guard_version

farm-worker
└── GuardSyncOrder 恢复                       # 只续跑可靠交付，不拥有宠物状态
```

`PetState` 和 `GuardSnapshot` 不能被理解成两个平级真值：

- `PlayerActor.PetState` 是玩家拥有哪只宠物、当前选择哪只、狗粮是否扣除以及守护截止时间的唯一真值；
- `FarmActor.GuardSnapshot` 是为了让偷菜在目标农场的同一邮箱内完成裁决而保存的版本化副本；
- `pet_state_version` 表达整个宠物页面状态的单调版本，解决解锁结果、喂食结果和切换结果乱序覆盖；
- `guard_version` 只允许单调增加，FarmActor 只应用比当前版本更高的快照；
- `GuardSyncWatermark` 只记录 Farm 已确认应用到哪个守护版本；它与来源 Order 同处 PLAYER Bucket，
  但不内嵌 PetState，避免 worker 绕过 Actor WriterClaim 修改缓存模型；
- 快照同步暂时失败不会反向回滚已经成功的喂食或切换。

### 2.2 为什么不拆 PetActor 或 pet-svr

宠物 P0 的写冲突全部围绕同一玩家：扣狗粮、切换宠物、更新守护时间和生成防重 Receipt。它们与
Inventory 同属 `player_id` 冲突范围，放入 PlayerActor 可以在一个 Actor Turn 和一次 Authority Commit
中完成，不需要引入新的 Actor 路由、跨服务事务或独立扩缩容边界。

宠物本身也没有高频 Tick、战斗模拟或独立热点。拆成 `PetActor/pet-svr` 只会让每次喂食都变成
PlayerActor 与 PetActor 的跨边界 Workflow，对当前玩法没有收益。

### 2.3 为什么 FarmActor 还要保存 GuardSnapshot

如果每次偷菜都同步查询主人 PlayerActor：

```text
Theft
→ FarmActor暂停
→ RPC查询Owner PlayerActor
→ 等待宠物状态
→ 再继续偷菜裁决
```

那么 PlayerActor、网络或存储稍慢，就会阻塞同一农场内的收获、护理和其他偷菜命令。保存
`GuardSnapshot` 后，FarmActor 可以在自己的一个 Turn 中同时看到作物、偷菜额度和守护状态，并给出
唯一顺序。不同农场依然可以并行。

因此这不是复制业务权威，而是把偷菜必需的少量数据投送到其裁决边界。

### 2.4 命令首跳

```text
GET_PET_STATE / PET_UNLOCK_BY_COIN / PET_FEED / PET_SWITCH
→ farm-gate
→ PLAYER Route Domain
→ PLAYER VirtualShard Holder
→ PlayerActor(player_id)
```

Gate 从 Session 中取得 `player_id`，客户端不能指定替别人喂食或切换宠物。P0 宠物集合很小，查询可随
PlayerBootstrap 返回或直接进入 PlayerActor，不建设宠物查询服务和查询投影。

### 2.5 喂食/切换后的可靠同步主干

```text
H5
→ PET_FEED 或 PET_SWITCH
→ PlayerActor 校验拥有关系、狗粮和 operation_id
→ 一次可靠提交：
     PetState变化
   + Inventory变化（仅喂食）
   + PetFeed/SwitchReceipt
   + 不可变StoredOperationOutcome
   + GuardSyncOrder(PENDING)
   + DueIndex
→ 返回不可变PetMutationResult：
     action_status=SUCCEEDED
     resulting_pet_state
     guard_sync_required=true
     guard_sync_order_id
     guard_version

DirectDispatcher立即尝试
→ FarmActor.ApplyGuardSnapshot
→ 只接受更高guard_version
→ GuardSyncOrder=APPLIED
→ 推进Player侧GuardSyncWatermark

若进程、RPC或目标Holder故障
→ farm-worker从DueIndex发现原Order
→ 使用同一order_id重试
→ 最终APPLIED或被更高版本SUPERSEDED
```

`PetMutationResult` 是历史操作的不可变答案，不在后台同步完成后从 `PENDING` 改成 `APPLIED`。
当前是否仍在同步由 `GET_PET_STATE` 比较 `PetState.guard_version` 与 `GuardSyncWatermark` 动态给出。
同步未完成时表示宠物操作已经成功，但目标农场暂时仍使用旧守护快照；客户端不得换一个新的
`operation_id` 重复扣狗粮，Dispatcher/worker 继续原 Order。

---

## 3. Q36-02 冻结：宠物解锁、购买与任务奖励

### 3.1 三种状态必须分开

```text
unlocked
= pet_id存在于owned_pet_ids

selected
= active_guard_pet_id == pet_id

guard_effective
= unlocked
   && selected
   && trusted_now_ms < guard_until_ms
```

新玩家可以尚未拥有宠物；宠物只有经过金币购买或任务奖励后才进入 `owned_pet_ids`。解锁宠物不等于
守护已经生效，选择宠物也不等于守护已经生效。**未喂狗粮或狗粮时长已经耗尽时，不产生任何守护
效果。** `guard_until_ms` 初始为 `0`，服务端不创建到期 Timer。

### 3.2 普通金币购买

金币购买是 PlayerActor 内部单权威命令，不创建跨 Actor Workflow：

```text
PetUnlockByCoinRequest {
    pet_id
    expected_pet_config_version
    expected_price_version
}
```

```text
H5 PET_UNLOCK_BY_COIN(pet_id, expected_price_version)
→ farm-gate
→ PLAYER Route Domain
→ PlayerActor
→ 校验服务端PetDefinition、价格版本、未解锁和普通金币余额
→ 一次可靠提交：
     Wallet.normal_coin -= frozen_price
   + owned_pet_ids.add(pet_id)
   + PetState.pet_state_version++
   + PetUnlockReceipt
   + StoredOperationOutcome
→ SUCCEEDED
```

客户端不能提交成交价格。价格和可购买条件来自版本化 `PetDefinition`；页面旧价格必须刷新后使用新的
operation 重试。已经解锁时返回 `NO_CHANGE/PET_ALREADY_UNLOCKED`，绝不再次扣金币；该命令一旦越过
Execution-Accept，也要保存不可变 `StoredOperationOutcome`，以后即使宠物状态变化，原 operation 仍返回
原终态。同一 operation_id 弱网重试命中原 Receipt/Outcome。

购买宠物只改变 PlayerActor，不需要同步 FarmActor。只有后续选择该宠物或喂食导致
`active_guard_pet_id/guard_until_ms/guard_version` 变化时，才创建 `GuardSyncOrder`。

### 3.3 任务奖励解锁

任务配置的冻结 `RewardBundle` 可以包含
`PetUnlockGrant(pet_id, pet_config_version, grant_definition_version)`。领取仍在第 34 号定义的一个
PlayerActor Turn 中完成：

```text
ClaimTask
→ 校验任务已完成且未领取
→ owned_pet_ids.add(pet_id)
→ 仅在集合实际新增时PetState.pet_state_version++
→ TaskState=CLAIMED
→ TaskClaimReceipt + StoredOperationOutcome
→ 与任务其他金币/道具/经验奖励一次Authority Commit
```

任务领奖不调用 pet-svr，也不创建新的 Workflow。相同宠物重复解锁是集合幂等：不会生成第二只宠物；
如果宠物已经通过金币获得，任务本身仍可正常领取，PetUnlockGrant 为 NoChange。运营配置应尽量避免让
同一只宠物的付费金币解锁与必达任务奖励重叠，以免玩家感到已支付价值被浪费。

### 3.4 配置与边界

```text
PetCatalogSnapshot {
    catalog_version
    feed_rule
    pets[]
}

PetFeedRule {
    feed_rule_version
    dog_food_item_id
    duration_per_food_ms
    max_guard_horizon_ms
    max_quantity_per_command
}

PetDefinition {
    pet_id
    pet_config_version
    display_definition_id

    unlock_offer {
        coin_unlock_enabled
        coin_price
        price_version
    }

    guard_rule {
        guard_rule_version
        trigger_probability_bp
        intercept_policy              # P0固定FULL_GROSS
        random_algorithm_version
        random_key_version
    }
}
```

任务解锁关系保存在版本化 TaskConfig 的 RewardBundle 中，不在 PetDefinition 里反向枚举全部任务。
P0 不实现宠物抽卡、碎片合成或宠物交易。宠物解锁集合必须设置配置上限和记录大小预算；确切上限在
容量/配置阶段冻结。金额、数量和时间乘加必须使用 checked arithmetic；溢出一律拒绝，不能环绕为较小值。

第 27/28 号已完成宠物公开协议收口：`PET_UNLOCK_BY_COIN` 使用独立 CommandSpec，不得临时复用普通
`BUY` 命令或自行分配 command_type；实际 Proto field number 仍由唯一生成源统一分配。

---

## 4. Q36-03 冻结：狗粮消耗、时长叠加与上限

### 4.1 P0 默认规则

```text
duration_per_food = 8小时
max_guard_horizon = 24小时
quantity          = 正整数

base_time = max(trusted_now_ms, old_guard_until_ms)
candidate_guard_until = CheckedAdd(
    base_time,
    CheckedMul(quantity, duration_per_food)
)

必须满足：
candidate_guard_until <= trusted_now_ms + max_guard_horizon
```

一次命令允许使用多份狗粮。因为每份增加 8 小时、最多预存 24 小时，完全没有剩余时间时单次最多使用
三份；仍有剩余时间时，可接受数量相应减少。P0 不做部分消费、自动截断或“多余狗粮退回”：如果候选
截止时间超过上限，整条命令返回 `REJECTED/PET_GUARD_LIMIT_EXCEEDED`，一份狗粮也不扣。

8 小时和 24 小时属于版本化服务端配置默认值。以后可以调数值，但一次已成功喂食写入的
`guard_until_ms` 不因配置热更新重新计算或缩短。

### 4.2 公开命令逻辑字段

```text
PetFeedRequest {
    quantity
    expected_feed_rule_version
    expected_active_pet_config_version
    expected_guard_rule_version
}
```

`operation_id` 只来自公共 CommandEnvelope。客户端不提交 `player_id`、狗粮 item_id、单份时长、最终
截止时间或 guard_version；这些全部由 Session、服务端配置和 PlayerActor 当前状态决定。

`quantity` 必须大于 0，并受 CommandCatalog 的单命令数量和 Payload 上限约束。P0 可以按当前默认规则
把公开硬上限设为 3，但服务端仍必须依据当前剩余时长重新计算本次实际允许数量，不能只校验
`quantity <= 3`。

### 4.3 PlayerActor 单次可靠提交

```text
PET_FEED
→ 捕获一次trusted_now_ms
→ 校验当前已选择一只已解锁宠物
→ 校验expected_feed_rule_version
→ 校验quantity、狗粮库存与24小时上限
→ 计算candidate_guard_until
→ 从本次确认的PetDefinition构造唯一candidate_guard_rule_snapshot
→ 一次Authority Commit：
     Inventory[dog_food_item_id] -= quantity
   + PetState.guard_until_ms = candidate_guard_until
   + PetState.guard_rule_snapshot = candidate_guard_rule_snapshot
   + PetState.pet_state_version++
   + PetState.guard_version++
   + PetFeedReceipt {
         operation_id
         quantity
         consumed_item_id
         feed_rule_version
         duration_per_food_ms
         previous_guard_until_ms
         resulting_guard_until_ms
     }
   + StoredOperationOutcome
   + GuardSyncOrder(PENDING，携带同一candidate_guard_rule_snapshot)
   + DueIndex
→ 提交确认后返回SUCCEEDED
```

没有已选择宠物、宠物未解锁、狗粮不足、规则版本过时或超出时长上限，均不扣道具。若错误发生在
Execution-Accept 之前，不创建业务 Outcome；若已经越过 Execution-Accept，则以当前
`pet_state_version/guard_version` 保存确定性的 `REJECTED StoredOperationOutcome`。具体错误码分别返回，
不能统一伪装成系统失败。

### 4.4 幂等、超时与并发

- 相同 `operation_id + quantity + expected_feed_rule_version` 重试返回原结果，不重复扣狗粮；
- 相同 operation_id 携带不同数量或规则版本返回 `IDEMPOTENCY_CONFLICT`；
- Authority Commit 超时未知返回 `PROCESSING`，客户端只能查询或使用原 operation_id 重试；
- 两条不同喂食命令并发时由 PlayerActor Mailbox 串行，后一条基于前一条已经提交的新截止时间重算；
- 喂食成功结果保持 `action_status=SUCCEEDED` 并携带原 GuardSyncOrder 引用；若随后
  `GET_PET_STATE` 返回动态 `guard_sync_status=PENDING`，不得重新喂食来“催同步”；
- 旧 GuardSyncOrder 到达 FarmActor 时若版本更低，标记 `SUPERSEDED`，不能覆盖新守护快照。

### 4.5 守护到期

守护到期不创建 Timer，也不写一次“EXPIRED”：

```text
guard_effective =
    active_guard_pet_id存在
    && trusted_now_ms < guard_until_ms
```

打开宠物页面、查看农场或发生偷菜时均按可信时间计算。客户端倒计时只用于展示，不能决定是否截留。
到期后再次喂食从当前 `trusted_now_ms` 开始增加 8 小时，不补回已经过期的空档。

---

## 5. Q36-04 冻结：切换宠物与剩余守护时间

### 5.1 守护时间归属

P0 的 `guard_until_ms` 属于玩家农场的单一守护槽，不为每只宠物分别维护饱食度或剩余时间。切换到
另一只已解锁宠物时：

```text
active_guard_pet_id = new_pet_id
guard_until_ms       = old_guard_until_ms    # 原样保留
guard_version        = old_guard_version + 1
```

因此：

- 守护仍有效时切换，剩余时间不减少、不清零，也不额外消耗狗粮；
- 守护已经到期时切换，仍然没有守护效果，必须重新喂狗粮；
- 切换只改变后续 TheftAttempt 使用的宠物外观与版本化守护规则；
- 已经提交的历史 TheftAttempt 不因切换宠物重新抽取或改变结算。

### 5.2 公开命令逻辑字段

```text
PetSwitchRequest {
    target_pet_id
    expected_pet_config_version
    expected_guard_rule_version
}
```

客户端不能提交 player_id、剩余时长、守护概率或 guard_version。Gate 从 Session 取得玩家身份，
PlayerActor 根据服务端 PetDefinition 和 `owned_pet_ids` 校验目标宠物。

### 5.3 PlayerActor 提交

```text
PET_SWITCH
→ 校验target_pet_id已解锁且配置仍有效
→ 若target_pet_id == active_guard_pet_id：
     NO_CHANGE/PET_ALREADY_ACTIVE
     不增加guard_version，不创建GuardSyncOrder
→ 否则一次Authority Commit：
     active_guard_pet_id = target_pet_id
   + guard_until_ms保持不变
   + guard_rule_snapshot = 本次PetDefinition冻结的完整守护规则
   + pet_state_version++
   + guard_version++
   + PetSwitchReceipt
   + StoredOperationOutcome
   + GuardSyncOrder(PENDING，携带与PetState完全相同的guard_rule_snapshot)
   + DueIndex
→ SUCCEEDED + 不可变PetMutationResult（携带Order引用，不携带可变同步终态）
```

切换到当前宠物返回 `NO_CHANGE` 时不推进 `pet_state_version/guard_version`，但在 Execution-Accept 后仍保存
本 operation 的不可变终态。未解锁宠物返回 `REJECTED/PET_NOT_UNLOCKED`，不自动购买、不扣金币。配置版本过时返回可刷新错误，
客户端刷新宠物定义后使用新的 operation_id 再发起意图；相同 operation_id 的弱网重试只返回原结果。

### 5.4 同步与并发顺序

切换会改变 FarmActor 偷菜裁决所需的宠物规则，因此即使当前守护已经过期，也要同步新的
`GuardSnapshot`，使农场展示和下一次喂食后的状态基线一致。

喂食与切换都进入同一个 PlayerActor Mailbox，并在各自提交时生成完整快照：

```text
Feed先提交 version=20，Switch后提交 version=21
→ FarmActor最终使用 version=21：新宠物 + Feed后的截止时间

Switch先提交 version=30，Feed后提交 version=31
→ FarmActor最终使用 version=31：新宠物 + 新截止时间
```

GuardSyncOrder 可以乱序到达，但 FarmActor 只应用更高 `guard_version`。较低版本返回
`SUPERSEDED`，不能覆盖新宠物或新截止时间。

---

## 6. Q36-05 冻结：最小记录、完整快照与版本规则

### 6.1 PetState

```text
PetState {
    owned_pet_ids[]
    active_guard_pet_id?
    guard_until_ms
    pet_state_version
    guard_version
    guard_rule_snapshot?
}
```

规则如下：

1. `owned_pet_ids` 是去重集合，持久化编码必须稳定；P0 整个 PetCatalog 最多 32 只，因此单玩家集合也
   最多 32 项。配置发布超过上限时拒绝配置，而不是等任务领奖时才因容量不足失败；
2. `active_guard_pet_id` 在玩家尚未选择宠物时可以为空；非空时必须存在于 `owned_pet_ids`；
3. `guard_until_ms` 使用 UTC Unix Milliseconds，初始为 0；
4. `pet_state_version` 对任何实际 PetState 变化单调加一：成功解锁、成功喂食、真正切换都会推进；
5. `guard_version` 只对成功喂食或真正切换单调加一，解锁但未选择宠物不增加；
6. `NO_CHANGE/REJECTED` 不推进两种版本，但在 Execution-Accept 后仍保存该 operation 的决定版本；
7. `guard_rule_snapshot` 保存当前 guard_version 对应的冻结规则，不能只保存一个指向可变热配置的引用。

两种版本不能互换：只使用 `guard_version` 会漏掉“解锁了新宠物但守护槽没变化”的页面更新；只使用
`pet_state_version` 又无法让 FarmActor 判断守护快照是否乱序。

`PetUnlockReceipt`、`PetFeedReceipt`、`PetSwitchReceipt` 与 PetState 位于同一 Player Authority Commit
边界，但使用第 30/31 号的有界 Operation Receipt/Outcome 机制保存，不能作为数组无限追加到 PetState。

### 6.2 GuardSnapshot

```text
GuardSnapshot {
    schema_version
    guard_version
    active_guard_pet_id?
    guard_until_ms

    rule {
        pet_config_version
        guard_rule_version
        trigger_probability_bp
        intercept_policy            # P0固定FULL_GROSS
        random_algorithm_version
        random_key_version
    }

    snapshot_hash
}
```

FarmActor 只依赖这份快照完成 TheftAttempt，不同步查询 PlayerActor，也不在偷菜时读取最新宠物热配置。
FarmActor 所在的 farm_id 已经给出主人身份，因此快照不重复保存可由 ActorKey 唯一推导的 owner 字段。
`snapshot_hash` 覆盖 `schema_version + guard_version + active_guard_pet_id + guard_until_ms + rule` 的
Canonical 编码，不覆盖自身；禁止直接 Hash 不稳定的 JSON 或默认 Protobuf bytes。

### 6.3 GuardSyncOrder

```text
GuardSyncOrder {
    order_id
    work_type                       # GUARD_SYNC_ORDER
    source_player_id
    target_farm_id
    source_operation_id
    source_fact_id                  # 固定PET_GUARD_SLOT
    source_fact_version             # 等于guard_version

    guard_version
    guard_snapshot                  # 完整目标快照，不是差量命令
    snapshot_hash

    status
    attempt_count
    next_due_at_ms
}
```

`order_id` 必须复用第 27/31 号的来源同槽 Order 公式，不再创建宠物私有 ID 算法：

```text
order_id = StableOpaqueID(
    "farm-source-order-id-v1",
    "GUARD_SYNC_ORDER",
    "PLAYER_ACTOR",
    canonical_player_id,
    "PET_GUARD_SLOT",
    canonical_guard_version
)
```

同一个 guard_version 只能对应一份 Canonical GuardSnapshot；`snapshot_hash` 用于识别同版本载荷分裂，
不用于替代完整字段校验。`source_operation_id` 用于审计和结果关联，不参与上面的稳定 Order 身份。

Order、来源 PlayerState 变化和 DueIndex 按第 27/31 号规则在 Player AuthorityPersistenceBucket 同槽原子
创建。Order 携带完整结果状态，禁止保存“再增加 8 小时”或“把宠物改成 X”这种需要依赖目标当前
状态重放的差量补丁。`GUARD_SYNC_ORDER` 属于 `BUSINESS_REQUIRED`：若来源 Bucket 的 Order/DueIndex
容量不足，Feed/Switch 整次 Authority Commit 必须失败关闭，不得先扣狗粮或修改 PetState 再丢同步义务。

### 6.4 FarmActor 应用合同

```text
incoming.guard_version > current.guard_version
→ 校验source_player_id == farm owner
→ 校验target_farm_id == 当前FarmActor ID
→ 一次Farm Authority Commit写入GuardSnapshot + GuardApplyReceipt
→ APPLIED

incoming.guard_version == current.guard_version
且snapshot_hash相同
→ 返回原APPLIED结果

incoming.guard_version < current.guard_version
→ SUPERSEDED

incoming.guard_version == current.guard_version
但snapshot_hash不同
→ INVARIANT_VIOLATION
→ 拒绝、告警，不覆盖当前快照
```

Farm 提交成功但响应丢失时，Dispatcher/worker 使用同一 order_id 重试；相同版本、相同 Hash 命中
`GuardApplyReceipt`，不会重复产生 FarmChange。来源 Order 最终根据 Participant 结果进入 APPLIED 或
SUPERSEDED。

Typed Participant 固定为 `APPLY_GUARD_SNAPSHOT`。其 Payload 至少包含 `order_id`、`source_player_id`、
`target_farm_id`、`guard_snapshot` 和 `snapshot_hash`；ParticipantReceipt 至少包含 `order_id`、
`applied_guard_version`、`applied_snapshot_hash` 和 `result`。FarmIngress 必须同时校验 WorkAddress 的来源权威为
`PLAYER_ACTOR:source_player_id`，不能只相信 Payload 中的 owner 字段。

### 6.5 GuardSyncWatermark

GuardSyncOrder 可以按通用保留策略 GC，因此宠物页面不能永远依赖“找到最新 Order”判断同步状态。
Player AuthorityPersistenceBucket 使用独立同槽 Key 保存一个固定大小水位，不把它嵌入 `model:pet`：

```text
GuardSyncWatermark {
    last_applied_guard_version
    last_applied_snapshot_hash
    last_terminal_status           # APPLIED | SUPERSEDED | INVARIANT_VIOLATION
}
```

DirectDispatcher 或 worker 收到 Participant 终态后，通过有界、幂等的来源回写推进水位。页面动态状态为：

```text
PetState.guard_version == 0
→ NOT_REQUIRED

watermark.last_applied_guard_version == PetState.guard_version
且Hash相同
→ APPLIED

watermark.last_applied_guard_version < PetState.guard_version
→ PENDING

同版本Hash不同或watermark反向回退
→ INVARIANT_VIOLATION并告警
```

水位只能单调推进，旧 Order 的 `SUPERSEDED` 回写不能覆盖更高版本的 APPLIED 水位。

### 6.6 不可变 OperationOutcome 与动态同步状态

`StoredOperationOutcome` 回答“原 operation 当时决定了什么”，GuardSyncWatermark 回答“当前投送到哪”。
二者禁止揉成一个会变化的结果：

```text
PetMutationResult {
    action_status                  # SUCCEEDED | NO_CHANGE | REJECTED
    error_code?
    resulting_pet_state_view
    guard_sync_required
    guard_sync_order_id?
    guard_version_at_decision
}

StoredOperationOutcome {
    operation_id
    canonical_payload_hash
    action_status
    error_code?
    pet_state_version_at_decision
    guard_version_at_decision
    decided_at_ms
    immutable_result_payload
}
```

越过 Execution-Accept 后的确定性 `SUCCEEDED/NO_CHANGE/REJECTED` 都必须保存 Outcome。结构错误、认证失败、
路由失败或尚未越过 Execution-Accept 的准入失败不保存。旧 operation 查询永远返回原不可变结果；客户端
若还需要当前同步状态，应另取 `GET_PET_STATE`。

`WORK_CAPACITY_EXCEEDED`、存储暂时不可用、Redirect、Deadline 和 `TIMEOUT_UNKNOWN` 属于可恢复执行
状态，不得伪装成确定性业务 Outcome。尤其 GuardSyncOrder 容量不足时没有任何 PetState/资产提交，
客户端按 RetryDirective 保持原 operation 重试。

### 6.7 为什么完整快照更适合本项目

例如：

```text
Feed提交version=10：小狗，守护到20:00
Switch提交version=11：小猫，仍守护到20:00

如果version=10后到FarmActor
→ 当前已经是version=11
→ 直接SUPERSEDED
```

目标 FarmActor 不需要推断 version=10 是“加时”还是“切换前状态”，也不需要按历史命令重新计算。
这会让乱序、Worker恢复和 Actor 迁移都退化为一次版本比较。

### 6.8 配置热更新

宠物拦截概率或规则配置变化时：

- 已经提交的 PetState、GuardSnapshot 和历史 TheftAttempt 不原地重算；
- 下一次成功喂食或真正切换宠物时读取新配置，生成新的 guard_version 和完整规则快照；
- 切换到当前宠物仍是 NO_CHANGE，不能把它伪装成无成本刷新规则命令；
- 如果未来确需立即全量刷新在线守护，必须另立运营迁移合同，P0 不实现。

---

## 7. Q36-06 冻结：页面读取、客户端状态与红点

### 7.1 读取主链

P0 不建设 PetReadProjection、pet-query-svr 或宠物缓存副本。宠物数量最多 32，只需读取小型
PlayerActor PetState，再与 player-svr 内缓存的版本化 PetCatalog 合并：

```text
H5打开宠物页面
→ GET_PET_STATE
→ farm-gate
→ PLAYER Route Domain
→ PlayerActor读取PetState，PetViewAssembler读取同Bucket GuardSyncWatermark
→ PetViewAssembler合并只读PetCatalog
→ 返回PetPageView
```

PlayerBootstrap 可以携带宠物摘要以绘制首页图标；真正打开宠物页面时仍重新读取，避免长期使用旧的
解锁集合或倒计时。读取失败只让宠物页面显示 Loading/重试，不影响玩家进入和经营自己的农场。

### 7.2 PetPageView

```text
PetPageView {
    server_now_ms
    pet_catalog_version
    pet_state_version
    active_guard_pet_id?
    guard_until_ms
    guard_effective
    guard_version
    guard_sync_status              # NOT_REQUIRED | APPLIED | PENDING | FAULT

    feed_rule_version
    dog_food_item_id
    duration_per_food_ms
    max_guard_horizon_ms
    max_quantity_per_command

    pets[] {
        pet_id
        pet_config_version
        display_definition_id
        state                      # LOCKED | UNLOCKED_NOT_SELECTED |
                                   # SELECTED_INACTIVE | GUARDING
        coin_unlock_enabled
        coin_price?
        price_version?
        guard_rule_version
        task_unlock_hint?
    }
}
```

四种页面状态定义为：

| 状态 | 条件 | 主要操作 |
|---|---|---|
| `LOCKED` | 不在 `owned_pet_ids` | 显示金币购买或任务解锁提示 |
| `UNLOCKED_NOT_SELECTED` | 已解锁但不是当前宠物 | 可切换 |
| `SELECTED_INACTIVE` | 当前宠物，但 `trusted_now_ms >= guard_until_ms` | 可喂狗粮 |
| `GUARDING` | 当前宠物且守护未到期 | 显示剩余时间，可继续在上限内喂食或切换 |

服务端状态是最终权威。客户端以 `server_now_ms` 和本地单调时间展示倒计时，倒计时归零时可以在本地把
`GUARDING` 切成 `SELECTED_INACTIVE`；应用恢复前台或时钟偏移明显变化时重新读取 PetPageView。客户端
时间不能参与 Theft 或喂食上限判断。

### 7.3 命令后的页面更新

`PET_UNLOCK_BY_COIN`、`PET_FEED` 和 `PET_SWITCH` 的确定性结果都应携带决定时的权威 PetStateView，
客户端可立即更新页面，不必成功后再发一次 GET。响应丢失时仍按 operation_id 查询原 OperationResult。

客户端 Reducer 必须使用 `pet_state_version` 防止乱序覆盖：

```text
incoming.pet_state_version > local.pet_state_version
→ 应用incoming PetStateView

incoming.pet_state_version == local.pet_state_version
→ 作为幂等重复处理

incoming.pet_state_version < local.pet_state_version
→ 只结束对应operation的等待状态
→ 不覆盖当前宠物页面
```

例如先发送“解锁宠物B”，随后“切换到B”；切换结果先到、旧解锁结果后到时，旧结果不能把页面退回
较早状态。

动态 `guard_sync_status=PENDING` 时显示“守护同步中”。它表示 PlayerActor 操作已经提交，不能使用新的
operation_id 重放同一用户意图；但玩家后续确实要追加狗粮或切换宠物时，仍可在规则允许范围内创建
新的业务命令和更高 guard_version，旧同步最终会被新版本覆盖或标记 SUPERSEDED。

页面再次读取时，PlayerActor 比较当前 `guard_version` 与固定大小 `GuardSyncWatermark` 得到动态状态，
不要求最新 GuardSyncOrder 永久存在，也不把 `guard_sync_status` 作为重复字段写进 PetState 或历史
OperationOutcome。

### 7.4 红点规则

P0 不增加宠物专属红点：

- 守护到期不产生红点、不创建 Scheduler Job，也不向客户端主动 Push；
- 狗粮库存充足、金币足够购买宠物都不自动点亮红点；
- 任务完成并可领取宠物解锁奖励时，只显示第 34 号已有的任务红点；
- 领取任务后宠物进入已解锁集合，任务红点按任务合同消失，不再复制一个宠物红点；
- 以后若产品需要“守护到期提醒”，作为通知策略扩展，不能把提醒失败解释为守护仍有效。

### 7.5 对其他玩家公开的信息

好友进入农场时，公共 FarmSnapshot/FarmChange 最多展示当前守护宠物的公共外观标识。不得公开：

- `guard_until_ms`；
- 拦截概率、随机值和规则版本；
- GuardSyncOrder 状态；
- 主人狗粮库存、金币或宠物购买价格个性化信息。

访客看到宠物外观不代表守护一定生效。是否生效以及是否截留只在 TheftAttempt 中由 FarmActor 使用
可信时间和 GuardSnapshot 裁决，结果再通过该次偷菜的私有 CommandResult 告知操作者和主人相关结算。

切换宠物后可广播一次不含私有字段的外观 FarmChange；普通喂食和自然到期不需要向 200 名同场玩家
广播倒计时变化，从而避免无业务价值的下行扇出。

公开模型在第 32 号中使用以下边界：

```text
FarmSnapshot {
    farm_meta: FarmMetaView
    plots[]
}

FarmMetaView {
    guard_pet_appearance?
}

FarmChange {
    farm_version
    committed_at_ms
    farm_meta_delta?
    plot_deltas[]
}

FarmMetaDelta {
    guard_pet_appearance_after?
}
```

- Feed 只安装不可公开的 GuardSnapshot：推进 Farm Actor `record_version`，不推进公开 `farm_version`，
  不产生 FarmChange；
- Switch 真正改变公开宠物外观：同时推进 `record_version` 与 `farm_version`，产生一份不可变
  `FarmMetaDelta`，由 Hub 对 200 个订阅者共享扇出；
- 自然到期不写状态、不推进任何版本、不广播；
- 同版本同 Hash 重试只命中 GuardApplyReceipt，不重复推进版本或产生第二份 FarmChange。

---

## 8. Q36-07 冻结：配置、失败语义、并发与验收

### 8.1 版本化配置与下架

PetCatalog、金币价格、喂食规则和守护规则必须版本化。配置发布至少验证：

- PetCatalog 总数不超过 32；
- `pet_id` 唯一且历史版本可读取；
- 普通金币价格非负；
- `pet_config_version/price_version/feed_rule_version/guard_rule_version` 可追溯且不可原地改语义；
- `trigger_probability_bp` 位于 `[0, 10000]`；
- `intercept_policy` P0 只能为 `FULL_GROSS`；
- `random_algorithm_version/random_key_version` 必须引用仍可用于在途 Attempt 恢复的版本；
- `duration_per_food_ms > 0`，P0 默认 8 小时；
- `max_guard_horizon_ms >= duration_per_food_ms`，P0 默认 24 小时；
- `max_quantity_per_command > 0`，且所有时间与数量乘加通过 checked arithmetic；
- 展示资源、守护规则和可购买标记引用完整。

宠物下架采用“停止新增、保留既有”的兼容语义：

```text
coin_unlock_enabled=false
→ 新金币购买拒绝
→ 已解锁玩家仍可查看、切换和喂食
→ 历史GuardSnapshot与TheftAttempt继续按冻结版本恢复
```

禁止删除仍被 PetState、GuardSnapshot、Receipt 或 Task RewardBundle 引用的历史配置。若未来因严重漏洞
必须强制停用既有宠物，应使用单独、可审计的紧急运营合同，不能把普通“商城下架”解释成静默没收。

购买价格版本过时返回 `REJECTED/PET_PRICE_CHANGED`；喂食规则版本过时返回
`REJECTED/PET_FEED_RULE_CHANGED`。两者都不扣资产，客户端刷新配置后用新的 operation_id 表达新意图。

### 8.2 失败语义

| 场景 | 对客户端/调用方结果 | 恢复方式 | 不变量 |
|---|---|---|---|
| 宠物未解锁、余额/狗粮不足、数量或上限非法 | `REJECTED` | 修正输入或资产后新 operation | 不扣资产、不写GuardSync |
| Player Authority Commit 前临时不可用 | 临时失败，按第28号RetryDirective | 原 operation 可重试 | 没有已接纳副作用 |
| Player Commit 超时未知 | `PROCESSING` | 原 operation 查询/重试，Resolver查Receipt | 不重复扣金币或狗粮 |
| Player Commit成功，响应丢失 | 返回链路未知 | 原 operation命中StoredOutcome | 只提交一次 |
| Player成功，GuardSync尚未发送/失败 | 原命令 `SUCCEEDED`；当前查询为 `PENDING` | DueIndex + Dispatcher/worker续投 | 不回滚解锁、喂食或切换 |
| 来源Order/DueIndex容量不足 | 临时 `REJECTED/WORK_CAPACITY_EXCEEDED` | `RETRY_SAME_OPERATION`；不保存业务终态 | 不扣狗粮、不修改PetState |
| Farm应用成功但ACK丢失 | Order继续PENDING | 同order_id重试命中GuardApplyReceipt | 外观变化FarmChange最多提交一次 |
| 旧GuardSync晚到 | `SUPERSEDED` | 结束旧Order | 不能覆盖高版本 |
| 同版本、不同snapshot_hash | `INVARIANT_VIOLATION` | 拒绝、告警、人工排查 | 不选择任一载荷覆盖 |
| Holder崩溃/迁移 | 请求可能Redirect或短暂失败 | Writer Fence后重载Actor，Order继续 | 旧Writer不能继续写 |

业务 `SUCCEEDED` 的成功点是 PlayerActor Authority Commit，不是 FarmActor 已经应用；`PENDING` 是
随后读取到的动态同步状态，不是对历史 StoredOutcome 的改写。客户端不得因
同步慢而撤销本地已成功的资产变化，也不得创建新 operation 重放同一购买/喂食/切换意图。

`PET_SWITCH` 必须配置玩家维度限流，因为真正切换会让 200 人同场产生一次公开外观扇出。准确阈值由
压测冻结；实现不得把阈值硬编码在 Handler，也不得用限流结果推进任何状态版本。

### 8.3 并发矩阵

| 并发 | 裁决方式 | 最终结果 |
|---|---|---|
| 两次金币购买同一宠物 | PlayerActor串行 | 一次成功扣款；另一条 `NO_CHANGE` 且不扣款 |
| 任务解锁 vs 金币购买同一宠物 | PlayerActor串行 | 首个加入集合；后续集合NoChange，购买后到则不扣钱 |
| 两次喂食 | PlayerActor串行并重新计算24小时上限 | 合法的都成功；超过上限的整条拒绝 |
| 喂食 vs 切换 | PlayerActor提交完整递增版本 | 后提交版本包含最终宠物与最终截止时间 |
| 切换到同一宠物 | 当前状态判断 | `NO_CHANGE`，不加版本、不建Order |
| GuardSync vs Theft | FarmActor Mailbox给出顺序 | Theft只使用该Turn已应用的快照 |
| 多个GuardSync乱序 | guard_version + snapshot_hash | 最高合法版本生效；旧版本SUPERSEDED |
| 到期边界 vs Theft | 单Turn `trusted_now_ms` | `now < until`才有效；`now == until`已失效 |

任务解锁和金币购买配置应避免让同一宠物的必达任务奖励与付费金币入口重叠；即便错误重叠，技术上也
必须保证集合幂等和不重复扣款。

### 8.4 最小验收矩阵

实现至少通过以下自动化测试：

1. 同一金币购买 operation 重放 100 次，只扣一次金币、只解锁一只宠物；
2. 同一喂食 operation 重放 100 次，只扣一次狗粮、只增加一次时长；
3. 相同 operation_id 携带不同 pet_id 或 quantity 返回 `IDEMPOTENCY_CONFLICT`；
4. 两条并发喂食在 24 小时边界得到可解释顺序，绝不部分扣道具；
5. 未喂食、恰好到期和已过期三个边界均不触发守护；
6. Feed/Switch 的 GuardSync 反序到达，FarmActor 最终保持最高版本；
7. 同版本同 Hash 重试幂等；同版本不同 Hash 拒绝并产生高优告警；
8. 在 Player Commit 后响应前崩溃，重启后原 operation 可恢复且不重复扣资产；
9. 在 Order 创建后投递前崩溃，worker 能从 DueIndex 发现并完成 GuardSync；
10. 在 Farm Commit 后 ACK 前崩溃，重试不产生第二次 FarmChange；
11. 配置热更新不改变已有 guard_until 和历史 Attempt，新 Feed/Switch 使用新规则版本；
12. 下架宠物不能再金币购买，但既有玩家仍能切换、喂食和恢复历史状态；
13. 切换宠物在 200 人同场只生成一份公共外观变化，由 Hub 扇出；普通喂食和自然到期不产生公共
    倒计时广播；
14. 访客 FarmView 不包含截止时间、概率、规则版本、随机值和主人资产；
15. PetCatalog 第 33 只配置在发布阶段被拒绝，不能进入运行时。
16. 解锁结果与切换结果反序到达 H5，旧 `pet_state_version` 只能结束旧 operation，不得覆盖新页面；
17. 已保存的 `NO_CHANGE/REJECTED` 在玩家后来解锁、喂食或切换后仍返回原决定，不被当前状态改写；
18. GuardSyncOrder 进入 GC 后，`GET_PET_STATE` 仍能仅凭 GuardSyncWatermark 正确返回 APPLIED/PENDING；
19. 同一个 TheftAttempt 在算法或密钥热更新、Actor 重载后仍使用已冻结的
    `random_algorithm_version/random_key_version`，不得重抽；
20. `source_player_id`、目标 farm owner、WorkAddress 来源权威任一不匹配时，Participant 在 Mailbox 前拒绝；
21. 来源 Order 或 DueIndex 达到上限时 Feed/Switch 失败关闭，狗粮、PetState 和版本均不变化；
22. Feed 安装隐藏 GuardSnapshot 只推进 Farm `record_version`，不推进公开 `farm_version`、不产生 FarmChange；
23. 真正 Switch 只推进一次公开 `farm_version`，200 个订阅者共享同一 FarmMetaDelta；
24. `PetState` 与 `GuardSyncOrder` 中的规则快照字节级 Canonical Hash 一致，不允许提交后再次读热配置拼装；
25. 旧 Order 的 SUPERSEDED 回写不能让 GuardSyncWatermark 从更高 APPLIED 版本回退。

### 8.5 可观测性

至少记录以下指标：

- `pet_unlock_total{result,source}`；
- `pet_feed_total{result}` 与狗粮消费数量；
- `pet_switch_total{result}`；
- `guard_sync_pending`、`guard_sync_oldest_age_ms`、重试次数和 `SUPERSEDED` 数量；
- `guard_sync_watermark_lag_versions`、水位回写失败数和 Order GC 后动态状态查询错误数；
- `guard_snapshot_invariant_violation_total`，必须配置高优告警；
- `pet_switch_rate_limited_total` 与切换导致的 FarmMetaDelta 扇出数量；
- 守护有效 TheftAttempt 数、截留数和截留比例，用于校验配置效果。

日志只记录业务 ID、版本、Hash、结果码和延迟，不记录密码、Ticket、完整资产快照或服务端随机密钥。

---

## 9. 已继承的偷菜守护语义

本文直接继承第 32 号合同：

1. 每次 TheftAttempt 使用其 FarmActor Turn 当时已生效的 `GuardSnapshot`；
2. 守护有效条件为 `trusted_now_ms < guard_until_ms`，不依赖到期 Timer 改状态；
3. 同一次 Attempt 的随机结果、快照版本和规则版本一经提交永久固定，重试不能重新抽取；
4. P0 守护结果为“不截留”或“截留本次全部 gross stolen crop”；
5. 宠物不扣偷菜玩家钱包。被截留的是本次原本能偷到的作物；其冻结价值转换为农场主普通金币；
6. 即使全部被宠物截留，这仍是一次已经完成的合法偷菜互动和该好友本茬唯一尝试。

---

## 10. 冻结清单

| 编号 | 需要冻结的问题 | 当前状态 |
|---|---|---|
| Q36-01 | 宠物范围、权威、部署和路由 | **已冻结** |
| Q36-02 | 初始宠物、拥有列表及宠物获得方式 | **已冻结：金币购买或任务奖励解锁** |
| Q36-03 | 狗粮消耗、单次时长、叠加方式和最大守护时长 | **已冻结：每份8小时，最多预存24小时** |
| Q36-04 | 守护期间切换宠物后，截止时间是否保留 | **已冻结：保留剩余时间，不额外消耗狗粮** |
| Q36-05 | PetState、GuardSnapshot、GuardSyncOrder 最小字段与版本规则 | **已冻结：32只上限、完整快照、版本+Hash Fence** |
| Q36-06 | 查询页面、剩余时间、红点及客户端可见状态 | **已冻结：直读PlayerActor，无宠物专属红点** |
| Q36-07 | 配置版本、失败语义、并发与验收矩阵 | **已冻结** |
| Q36-08 | PetState乱序、不可变Outcome、GuardSyncWatermark、Typed Participant与公开外观版本 | **已冻结** |

Q36-01～Q36-08 已全部冻结。本文可以作为宠物领域实现输入，但必须同时遵守第 26～35 号上位合同。

---

## 11. 协议登记状态与物理门槛

第 27/28 号已经受控登记以下公网命令：

- `GET_PET_STATE`；
- `PET_UNLOCK_BY_COIN`；
- `PET_FEED`；
- `PET_SWITCH`。

上述四类 CommandType、PLAYER RouteKey、Payload/Result、幂等、Payload Hash、大小、Deadline、限流、
RetryDirective 和错误码均已进入现行合同；第 29 号也已经登记 `APPLY_GUARD_SNAPSHOT`。后续正式
`.proto` 生成仍必须通过字段号、`reserved` 和滚动兼容评审。Agent 不得自行分配另一组 command_type、
绕过 `pet_state_version`/不可变结果合同，也不得复用普通 `BUY` 命令绕过宠物价格与解锁语义。
