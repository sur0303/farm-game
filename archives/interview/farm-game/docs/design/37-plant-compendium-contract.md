# 37 · 植物图鉴与活动档案合同

版本：`v1.1-unlock-snapshot-seen-fence-capacity-freeze-candidate`  
日期：2026-08-03  
状态：产品与权威语义 `STABLE_INPUT`；Proto field number、Redis 物理分区、页面字节阈值为 `PHYSICAL_PENDING`  
定位：在第 26～36 号合同之上，定义 P0 植物图鉴的永久解锁、活动档案、页面查询和红点边界。
本文只做简单收藏展示，不引入独立图鉴服务、图鉴 Actor 或复杂收藏养成。

## 0. 上位合同与不重复定义

- [26 · 接入与会话](./26-access-session-contract.md)：Session、Join、Subscription 和下行；
- [27 · 路由、Actor 与 Workflow](./27-access-routing-actor-workflow-freeze.md)：PlayerActor、FarmActor、
  Harvest Workflow 和 Player/Farm 路由；
- [28 · 公网协议与 CommandCatalog](./28-command-catalog-public-protocol.md)：HarvestResult、查询命令、
  结果恢复和红点消息的协议边界；
- [29 · 内部 RPC](./29-internal-rpc-trusted-context.md)：Harvest Participant 调用和 Receipt；
- [30 · Actor Authority Store](./30-actor-authority-store-contract.md) 与
  [31 · Redis 物理合同](./31-actor-authority-redis-physical-contract.md)：Player Authority、Receipt、
  Model Field、容量和查询边界；
- [32 · 核心经营场景](./32-core-business-scenario-contract.md)：首次成功收获自己种植的植物才解锁图鉴、
  活动作物、遗留种子和 Harvest 原子提交；
- [34 · 任务系统](./34-task-system-contract.md)：任务领奖和已有任务红点；
- [36 · 简化宠物守护系统](./36-simple-pet-guard-contract.md)：同一 PlayerActor 内的简单个人收藏状态边界。

冲突时服从上位合同。本文不修改 Harvest、Plant、TaskClaim 或 PlayerActor 的既有成功语义。

---

## 1. P0 产品范围

### 1.1 必须实现

1. 展示植物的已解锁、未解锁灰色剪影和基础说明；
2. 玩家第一次成功收获自己农场种植的某种植物时，永久解锁该植物；
3. 活动植物在活动期间归入当前活动展示，活动结束后转入活动档案；
4. 活动结束后，已解锁植物在活动档案中永久完整展示；
5. 活动结束后，未解锁植物在活动档案中继续显示灰色剪影；
6. 活动结束后，只要玩家仍持有合法遗留种子，仍允许继续播种；
7. 解锁后的图鉴状态可以在 Actor 重启、迁移和重复登录后恢复。

### 1.2 P0 不实现

- 植物图鉴等级、收藏积分、成就链和排行榜；
- 图鉴兑换、抽奖、碎片和重复解锁奖励；
- 独立 `CompendiumActor`、`compendium-svr` 或图鉴专用 MQ；
- 因活动结束自动收回遗留种子；
- 让偷来的植物、仓库直接购买的作物或好友奖励替代“首次成功收获自己种植”的解锁条件；
- 复杂的全量通知和图鉴专用 Scheduler。

---

## 2. 权威与服务边界

```text
player-svr
└── PlayerActor(player_id)
    └── CompendiumState
        ├── unlocked_count
        ├── unlocked_crop_definition_ids（逻辑集合，物理必须有界）
        ├── compendium_revision
        └── compendium_seen_revision

player-svr
├── PlantCatalogReadAPI       # 本地不可变Snapshot，Watch更新
└── CompendiumReadIngress     # Actor外组装PlayerState + Catalog
```

`CompendiumState` 是玩家永久解锁事实的唯一权威，放入 PlayerActor 的 Player Authority。它与收获奖励
同时变更，不能拆成“收获到账后再异步解锁图鉴”，否则会出现玩家收获成功但图鉴永久漏解锁。

P0 冻结唯一图鉴身份：

```text
compendium_entry_id == crop_definition_id
```

所有永久集合、Harvest 解锁 Delta、Catalog Entry 和公网结果统一使用 `crop_definition_id`；
`display_definition_id` 只标识展示资源，不参与解锁。如果同一个 `crop_definition_id` 被活动 A、B 复用，
玩家在任一来源首次解锁后，它在两个活动档案中都显示为已解锁。若未来要求“同一种植物在不同活动分别
收集”，必须另立 `activity_collection_entry_id` 合同，P0 不允许 Agent 自行扩展。

图鉴不新增 Actor、Deployment 或查询 Redis 副本。逻辑集合可以放在 Player Authority Bucket 的独立
`model:compendium_*` Field 中，但必须与 Wallet/Inventory/Progression 共享同一个 WriterClaim、
RecordVersion 和 Durable Lane；它不是第二条 Persistence Lane。植物正文、图片和活动标题属于全局配置/
静态资源，不复制进每个玩家的权威记录。

---

## 3. 图鉴解锁规则

### 3.1 唯一解锁条件

```text
PlantWorkflow读取一次版本化Catalog/CropRule
→ 生成CompendiumUnlockSnapshot
→ FarmActor随CropCycle持久化

FarmActor确认Harvest成功
→ 把CropCycle中的CompendiumUnlockSnapshot原样复制进HarvestDecision
→ PlayerActor只校验受信Snapshot，不重新查询当前Catalog
→ 一次Authority Commit：
     Inventory + HarvestCounter + Experience/Level
   + CompendiumState.unlocked_crop_definition_ids.add(crop_definition_id)
   + CompendiumUnlockDelta
   + HarvestReward/ParticipantReceipt
   + StoredOperationOutcome
```

Plant 时冻结：

```text
CompendiumUnlockSnapshot {
    crop_definition_id              # P0唯一图鉴身份
    unlock_eligible
    planted_by_player_id
    activity_id?
    catalog_version_at_plant
    definition_version_at_plant
}
```

`unlock_eligible` 表示该作物属于可收集图鉴项；活动已经归档但
`legacy_seed_plantable=true` 时，合法 Plant 仍可把它冻结为 true。该 Snapshot 是 CropCycle 的长期事实，
参与 CropCycle/Plant Decision Hash。活动结束、配置换版、Actor 迁移和 Worker 重试都不得重算它。

只有“自己在自己的农场种下，并且成功收获”的作物触发首次解锁：

| 来源 | 是否解锁图鉴 | 原因 |
|---|---:|---|
| 自己种植、自己成功收获 | 是 | 唯一 P0 解锁条件 |
| 偷到好友的作物 | 否 | 偷菜只是仓库收益 |
| 好友护理奖励 | 否 | 奖励不是自己种植的收获 |
| 邮件附件得到作物 | 否 | 不代表完成种植收获 |
| 仓库已有遗留种子 | 否 | 直到它被自己种下并成功收获 |
| 已经解锁的植物再次收获 | 不重复写解锁 | 集合幂等，仍正常发放收获奖励 |

PlayerActor 只在以下条件同时满足时尝试新增集合成员：

```text
snapshot.unlock_eligible == true
snapshot.crop_definition_id == HarvestDecision.crop_definition_id
snapshot.planted_by_player_id == HarvestDecision.owner_player_id
HarvestDecision.owner_player_id == 当前PlayerActor.player_id
```

`crop_definition_id` 是业务解锁 ID，不是客户端展示文本或临时活动实例 ID。相同植物不同批次、不同
CropCycle 只对应一个永久解锁集合成员。

### 3.2 与 Harvest 的原子边界

图鉴解锁属于 Harvest 的 Player 侧奖励事实：

```text
FarmActor提交HarvestDecision
→ Coordinator调用PlayerActor GrantHarvestReward
→ PlayerActor一次提交：
     普通作物库存
   + 经验/等级
   + 收获次数
   + 图鉴解锁（若首次）
   + HarvestReward/Participant Receipt
   + 原始CompendiumUnlockDelta
   + StoredOperationOutcome
→ 返回 HarvestResult(compendium_unlock_delta)
```

如果 Player Commit 响应丢失，客户端使用原 operation_id 查询；如果 Receipt 已存在，重复恢复只返回
原结果，不重复加库存、经验、收获次数或图鉴状态。图鉴解锁失败不能被当作“可忽略的查询投影失败”。

原操作结果必须持久为：

```text
CompendiumUnlockDelta {
    newly_unlocked
    crop_definition_id?        # 仅首次解锁时存在
    compendium_revision_after
}
```

Delta 必须嵌入 `GRANT_HARVEST_REWARD` 的 ParticipantReceipt 和 Harvest 的 StoredOperationOutcome。
不能在重试时根据当前集合重新计算 `newly_unlocked`：即使另一条 Harvest 后来已解锁同一植物，原操作仍要
返回自己首次提交时的 true/false、条目 ID 和 Revision。P0 不建立第二套无界
`CompendiumUnlockReceipt` 家族；若保留领域审计摘要，其生命周期必须服从原 Harvest Receipt 的压缩与
容量合同。

---

## 4. 活动植物与活动档案

### 4.1 配置模型

```text
PlantCatalogSnapshot {
    catalog_version
    entries[]
    activity_archives[]
}

PlantCatalogEntry {
    crop_definition_id
    definition_version
    display_definition_id
    category                  # NORMAL | ACTIVITY
    activity_id?
    activity_state            # ACTIVE | ARCHIVED
    silhouette_asset_id
    detail_asset_id
    legacy_seed_plantable
}

ActivityArchiveDefinition {
    activity_id
    archive_version
    crop_definition_ids[]
    archive_title_snapshot
    archive_order
    end_at_ms
}
```

`PlantCatalogSnapshot.catalog_version` 表示整份目录版本；单条植物使用 `definition_version`；活动档案
使用 `archive_version`，三者不能混成一个字段。活动状态和展示分类来自服务端版本化配置。结束活动不修改
玩家的 `unlocked_crop_definition_ids`，只让页面把
该植物从“当前活动”归入“活动档案”。

活动进入 `ARCHIVED` 时形成不可变 `ActivityArchiveDefinition`。历史展示含义不再原地改写；资源路径必须
版本化，不能被普通 CDN GC 覆盖或复用。保留策略以“该活动档案仍属于产品可展示范围”为依据，全局保留
Archive Catalog 与资源，不通过扫描 3000 万玩家判断是否仍有人引用。

### 4.2 活动结束后的表现

```text
活动结束
→ 不删除植物定义、剪影资源或历史版本
→ 已解锁：活动档案显示完整图鉴
→ 未解锁：活动档案显示灰色剪影
→ 持有legacy_seed且legacy_seed_plantable=true：仍可Plant
→ 后续自己成功Harvest：仍按普通解锁规则写入图鉴
```

活动结束只影响新活动入口和页面分类，不影响已经持有的遗留种子。Plant 是否允许使用遗留种子仍由
`CropDefinition.legacy_seed_plantable` 决定，不能因为活动状态变成 `ARCHIVED` 就统一拒绝。

已解锁的活动植物，即使活动配置下线，也必须保留用于页面展示的历史 `display_definition_id`、活动
标题和资源引用。如果永久解锁集合中存在当前 Catalog 找不到的 `crop_definition_id`，服务端必须保留
`UNLOCKED` 事实、返回安全占位并产生 `catalog_missing` 告警；不得把它降级为未解锁、静默隐藏或删除
玩家事实。

---

## 5. 查询与客户端表现

### 5.1 查询主链

```text
H5打开图鉴
→ GET_COMPENDIUM
→ farm-gate
→ PLAYER Route Domain
→ CompendiumReadIngress捕获本地不可变PlantCatalogSnapshot(version=V)
→ PlayerActor在一个短Turn返回CompendiumState Snapshot
→ Actor Turn结束
→ CompendiumReadIngress在Turn外合并Catalog/ActivityArchive
→ 返回CompendiumPageView
```

配置通过 Watch/本地缓存进入 player-svr。禁止 PlayerActor 在 Mailbox Turn 内远程等待 Config 服务；
Catalog 暂不可用时查询失败并允许页面重试，不阻塞 PlayerActor 其他写命令。

图鉴是玩家私有页面，不向好友广播。读取失败只显示 Loading/重试，不影响进入自己的农场和经营命令。

### 5.2 页面状态

```text
CompendiumPageView {
    catalog_version
    compendium_revision
    compendium_seen_revision
    has_new_unlock
    categories[] {
        category_id
        title
        entries[] {
            crop_definition_id
            display_state       # UNLOCKED_FULL | LOCKED_SILHOUETTE
            display_definition_id?
            silhouette_asset_id
            activity_id?
        }
    }
    next_archive_cursor?
}
```

`UNLOCKED_FULL` 展示植物完整图片、名称和已配置说明；`LOCKED_SILHOUETTE` 只展示灰色剪影和基础占位
信息，不能暴露未解锁活动植物的完整细节。

`compendium_revision` 只在首次解锁新成员时增加：

```text
has_new_unlock = compendium_seen_revision < compendium_revision
```

`GET_COMPENDIUM` 只读，不得隐藏地修改已读水位。H5 成功应用并渲染返回页面后发送：

```text
ACK_COMPENDIUM_SEEN(observed_revision)
```

PlayerActor 原子执行：

```text
seen_after = max(
    current_seen_revision,
    min(observed_revision, current_compendium_revision)
)
```

这样 GET 返回 revision=5 后并发解锁 revision=6，迟到的 Ack(5) 不会清除 6 的红点；旧 Ack 不会让水位
回退，重复 Ack 返回 `NO_CHANGE`。这是低风险单调 UI Ack：不进入长期 Operation Journal、不要求 Farm
Subscription，Ack 丢失最多导致红点重复。

### 5.3 HarvestResult 快速反馈

HarvestResult 可以携带：

```text
compendium_newly_unlocked
unlocked_crop_definition_id?
compendium_revision
```

这样客户端收获后可以立即弹出“图鉴解锁”，不必额外轮询整页。正式图鉴页面仍以 PlayerActor 查询结果
为准。H5 只在返回 Revision 大于当前本地图鉴 Revision 时推进本地摘要；较小 Revision 只结束对应
Harvest operation，不能回滚较新的页面状态。同一 operation 的解锁动画最多播放一次。

### 5.4 容量与分页演进边界

逻辑集合和页面必须受版本化 `CompendiumLimitProfile` 约束：

```text
max_catalog_entries
max_unlocked_entries
max_compendium_state_bytes
max_compendium_page_entries
max_compendium_page_bytes
```

P0 可以在上述硬上限内一次返回普通植物和当前活动，活动档案也可暂不分页；配置平台在发布新植物或活动前
必须证明“现有 Catalog + 新增内容”不超过当前 Profile。达到页面 item/byte 上限后，活动档案按稳定
`archive_order + activity_id` 游标分页，不能静默截断。

运行时若仍触发权威状态硬上限，PlayerActor 不得先发奖励再静默跳过永久解锁。由于 Farm Harvest 已经是
不可逆点，HarvestWorkflow 保持 `PROCESSING/RECONCILE_REQUIRED` 并告警，待容量配置或数据修复后继续原
Participant；主要防线必须是发布前容量校验。物理 P0 可选有界排序数组或稳定 ordinal Bitmap；若未来按
固定区间分区，所有分区仍位于同一 Player Authority Bucket，共享同一个 RecordVersion、WriterClaim 和
Durable Lane。

---

## 6. 失败、并发与验收

| 场景 | 结果 | 说明 |
|---|---|---|
| 自种Harvest成功，Player响应丢失 | 原 operation 查询返回原结果 | 图鉴与奖励只提交一次 |
| 同一Harvest重放 | Receipt命中 | 不重复解锁 |
| 两个Harvest并发同一植物 | PlayerActor串行集合写入 | 一个新增，另一个幂等 |
| 偷菜获得植物 | 不解锁 | 仍可正常入库 |
| 活动结束后遗留种子Plant | 按legacy_seed_plantable判断 | 不被活动结束自动否决 |
| 活动结束后遗留种子Harvest | 可触发首次解锁 | 仍满足自己种植、自己收获 |
| 活动配置下线 | 已解锁可展示 | 历史资源和配置版本保留 |
| 图鉴页面读取失败 | 页面重试 | 不影响 FarmActor/PlayerActor 主链 |
| 图鉴已读状态写入失败 | 红点可能重复 | 不影响永久解锁 |
| GET返回revision=5后并发解锁revision=6，再Ack(5) | seen只推进到5 | revision=6红点仍存在 |
| 永久解锁ID缺少Catalog配置 | 保持UNLOCKED并返回占位 | 告警，不降级、不静默消失 |
| Catalog/状态接近硬上限 | 发布前拒绝扩容配置或启用档案分页 | 不静默漏解锁/截断页面 |

P0 至少验收：

1. 自种植物第一次 Harvest 能同时到账并解锁图鉴；
2. 同一 operation_id 重放 100 次只新增一次解锁；
3. 偷菜、邮件、护理奖励不能解锁；
4. 24 块地不同作物的解锁集合稳定去重；
5. 活动结束后已解锁显示完整、未解锁显示剪影；
6. 活动结束后遗留种子仍能按配置播种并在自收获后解锁；
7. 配置下线后已解锁图鉴仍可读取；
8. 图鉴查询失败不阻塞登录、Join 和经营命令；
9. 图鉴红点只因新解锁产生，Ack 失败允许重复但不产生资产副作用；
10. 两个 Harvest 并发命中同一种未解锁植物，只有一个 Delta 为 true，两个 operation 重试都返回各自原结果；
11. 首次解锁响应丢失后又收获同种植物，原 operation 仍返回原 true、ID 和 Revision；
12. GET 返回 revision=5 后产生 revision=6，Ack(5) 不清除 6；旧 Ack 乱序到达不回退 seen_revision；
13. 活动结束、Actor 重启后使用遗留种子 Harvest，仍按 CropCycle 冻结 Snapshot 解锁；
14. Harvest 期间 Catalog 热更新/下线不改变已冻结的 unlock_eligible 和 crop_definition_id；
15. 玩家持有永久解锁 ID 但 Catalog 缺失时返回安全占位并告警；
16. 同一植物被两个活动引用时，在两个档案中都按同一永久成员显示已解锁；
17. Catalog 达到 LimitProfile 时拒绝配置发布，线上 Harvest 不得静默漏解锁；
18. CompendiumPageView 接近 MaxPayloadBytes 时启用档案分页，不静默截断；
19. 两个 HarvestResult 乱序到 H5 时，本地 compendium_revision 只前进不回退；
20. CompendiumReadIngress 的 Catalog 获取和页面组装均发生在 Actor Turn 外。

---

## 7. 冻结决策索引

| 编号 | 问题 | 当前状态 |
|---|---|---|
| Q37-01 | 首次自种成功收获解锁图鉴 | **已冻结：Plant时冻结资格，Harvest奖励同提交解锁** |
| Q37-02 | 活动结束后档案、灰色剪影与遗留种子 | **已冻结：不可变Archive，遗留种子可继续解锁** |
| Q37-03 | PlayerActor 权威、无独立图鉴服务 | **已冻结：可拆Model Field，不拆Fence/Lane** |
| Q37-04 | 图鉴红点和已读 revision | **已冻结：显式observed_revision Ack Fence** |
| Q37-05 | 配置下线、历史资源保留与验收 | **已冻结：全局保留Archive，缺配置用占位并告警** |
| Q37-06 | 图鉴唯一身份 | **已冻结：compendium_entry_id == crop_definition_id** |
| Q37-07 | Harvest解锁资格与原结果 | **已冻结：Plant Snapshot + Harvest原始UnlockDelta** |
| Q37-08 | 容量与分页 | **已冻结有界原则；具体阈值/物理编码待探针** |

Q37-01～Q37-08 的产品和逻辑合同已冻结。第 28 号已经登记 `GET_COMPENDIUM`、
`ACK_COMPENDIUM_SEEN`，并把 `HarvestResult` 的解锁 ID 与 Revision 纳入结果；第 29/32 号已经让
`CompendiumUnlockSnapshot/Delta` 进入 Harvest Decision、Participant Result Hash 和原 Receipt；第 31 号
已经提供有界模型与发布前容量闸门。实际 Proto field number、Redis Bitmap/分区编码和分页阈值仍由唯一
生成源与容量探针统一冻结，本文不自行分配物理编号。
