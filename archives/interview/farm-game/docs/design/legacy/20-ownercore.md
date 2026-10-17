# 20 · PlayerCore / FarmCore Schema 与大小预算

本文保留原文件名以维持引用稳定，但 2026-07-27 起不再采用单条 `OwnerCoreRecord`。新的权威记录是 `PlayerCoreRecord` 与 `FarmCoreRecord`，详细跨 Actor 语义见 [16-player-farm-actor-split](./16-player-farm-actor-split.md)。

当前 `api/proto/ownercore/ownercore.proto` 和 `internal/storage/ownercoresize/size_test.go` 仍是旧 v0.1 可执行基线，不代表目标 Schema。进入 S2 存储冻结时必须拆分 Proto、生成代码与容量测试。

---

## 1. 拆分原则

TcaplusDB 的条件写只保证对应记录/字段组的物理合同：

- PlayerActor 修改 PlayerCore；普通软资产可先在 Actor 内原子修改后异步增量回写；
- FarmActor 修改 FarmCore；
- 两类记录之间不宣称数据库事务；
- 跨 Actor 业务使用 Intent/Settlement + Outbox + Receipt 收敛。

不能通过让 PlayerActor 直接写 FarmCore、或 FarmActor 直接写 PlayerCore 来“省一次消息”，否则 Actor 所有权和路由边界会失效。

## 2. PlayerCoreRecord

```text
PlayerCoreRecord (primary_key = player_id)
├── player_meta
│   ├── player_id
│   ├── player_epoch
│   ├── state_version
│   ├── schema_version
│   └── applied_bootstrap_rule_version
├── profile
├── player_economy_model              // 一个物理一致性组
│   ├── progression
│   ├── wallet
│   ├── inventory[]
│   │   ├── item_id
│   │   ├── quantity
│   │   └── reserved_quantity
│   ├── shop_counters[]
│   └── recent_local_purchase_results[]
├── task_state
├── mail_claim_receipts[]
├── pet_state
├── album_state
├── plant_intents[]
├── recent_operation_results[]
├── pending_outbox[]
└── inbound_receipts[]
    ├── harvest_receipts[]
    ├── loot_receipts[]
    ├── care_reward_receipts[]
    └── share_reward_receipts[]
```

Actor 运行时另有，不进入业务 Schema：

```text
version
saved_version
modified_version[model]
dirty_models
save_inflight
```

### 2.1 PlayerCore 内局部一致性组

| 命令/步骤 | 提交边界 |
|---|---|
| Buy | Wallet 扣款 + Inventory 增加 + ShopCounter + OperationResult 同一 EconomyTxn 内存 Apply；异步同组 HMSet |
| Sell | Inventory 扣除 + Wallet 增加 + OperationResult 同一 EconomyTxn 内存 Apply；异步同组 HMSet |
| Plant Reserve | `reserved_quantity + 1` + PlantIntent + ApplyPlantOutbox + PROCESSING，P-DURABLE |
| Plant Finalize | `quantity - 1` + `reserved_quantity - 1` + PlantIntent COMPLETED + SUCCEEDED，P-DURABLE |
| Plant Reject | `reserved_quantity - 1` + PlantIntent REJECTED_REFUNDED + REJECTED，P-DURABLE |
| Harvest 到账 | Inventory/Progression 增加 + HarvestReceipt，P-DURABLE |
| 任务/邮件领奖 | claimed/receipt + Wallet/Inventory/Progression 奖励，P-DURABLE |

库存恒满足：

```text
0 <= reserved_quantity <= quantity
available_quantity = quantity - reserved_quantity
```

## 3. FarmCoreRecord

```text
FarmCoreRecord (primary_key = farm_id)
├── farm_meta
│   ├── farm_id
│   ├── owner_player_id
│   ├── farm_epoch
│   ├── state_version
│   ├── schema_version
│   └── applied_bootstrap_rule_version
├── farm_version
├── plots[]
├── active_crop_cycles[]
├── active_care_incidents[]
├── plant_receipts[]
├── active_harvest_settlements[]
├── active_theft_settlements[]
├── farm_operation_results[]
└── pending_outbox[]
```

### 3.1 FarmCore 内局部原子组

| 命令/步骤 | 同一次 FarmCore CAS |
|---|---|
| ApplyPlant 成功 | Plot/CropCycle/CareIncident + FarmVersion + PlantReceipt(APPLIED) + OutcomeOutbox |
| ApplyPlant 拒绝 | PlantReceipt(REJECTED) + OutcomeOutbox；不改变地块 |
| Harvest 接受 | CropCycle 结束 + FarmVersion + HarvestSettlement + CreditHarvestOutbox |
| Water/RemovePest | CareIncident resolved + FarmVersion + 目标侧 Result + 可选 CareRewardOutbox |
| Steal | 可偷量扣减 + FarmVersion + TheftSettlement + GrantLootOutbox |

Presence、JoiningContext、JoiningBuffer 和 Subscription 是 FarmActor activation 内的 C 类运行态，不进入 FarmCore。

## 4. 作物模型

- `GROWING/MATURE` 由 `mature_at` 与服务器时间派生，不靠 Timer 写状态；
- CropCycle 创建时冻结 `rule_version / plant_at / mature_at / base_yield / stealable_limit / mutation_seed`；
- P0 每茬最多一个 WaterIncident 和一个 PestIncident，出现时间在播种时按冻结规则生成；
- 作物未成熟不能偷；
- CropCycle 结束后从活跃集合移出，长期审计按需进入归档表；
- 重复 ApplyPlant 必须先查 `PlantReceipt(plant_txn_id)`，不得重新随机护理事件或创建第二茬。

## 5. 版本

每一类记录拥有独立版本：

| 概念 | PlayerCore | FarmCore |
|---|---|---|
| Tcaplus 物理版本 | `RecordVersion` | `RecordVersion` |
| 已保存逻辑版本 | `player_meta.state_version` / Actor `saved_version` | `farm_meta.state_version` |
| Actor 当前内存版本 | Actor `version`，可能领先 Store | 与已提交 Farm state 对齐 |
| Actor fencing | `player_epoch` | `farm_epoch` |
| 客户端公开变化版本 | 不直接下发 | `farm_version / plot_version` |

Player 与 Farm 的 `RecordVersion` 不可比较，也不能组合成一个“全局 Owner 版本”。

## 6. 热窗口与可靠流程

- 已完成 OperationResult 在 `R_hot` 内保留完整响应，归档后仍留最小防重事实；
- 未完成 PlantIntent、Settlement、Outbox 不得按普通 TTL 删除；
- Receipt 保留时间必须覆盖上游可能重投的最大窗口；
- PendingWork 不能靠 Worker 内存或只靠 Redis；源事实随 PlayerCore/FarmCore CAS 持久化；
- 发现索引可以异步构建，但必须有遗漏补偿和对账。

## 7. TcaplusDB 表

| 表 | Key | 内容 |
|---|---|---|
| `player_core` | player_id | 玩家资产、成长、玩家侧意图/结果/回执 |
| `farm_core` | farm_id | 农场、地块、作物、目标侧流程与 Outbox |
| `account_bootstrap` | 平台身份键或 player_id | player_id/farm_id 映射与双记录建档状态 |
| `friendship` | 规范化好友对 | 关系、friendship_epoch、拉黑 |
| `share_link` | share_token_hash | 邀请者、有效期、规则 |
| `share_acceptance` | 分享者+接受者 | 防重复接受与奖励流程 |
| `operation_archive` | actor_kind+entity_id+operation_id/时间分桶 | 移出热窗口的终态结果与防重事实 |
| 配置/定义表 | definition_id+version | 作物、商品、任务、邮件定义 |

## 8. 容量预算

旧 `OwnerCore ~1.8KB` 只能作为字段量参考，不能直接外推新容量。必须重新实测：

| 样本 | PlayerCore | FarmCore |
|---|---:|---:|
| 新玩家/空农场 | 待实测 | 待实测 |
| 典型玩家/典型农场 | 待实测 | 待实测 |
| 热窗口接近上限 | 待实测 | 待实测 |
| 未完成流程上限 | 待实测 | 待实测 |
| 最坏合法记录 | 待实测 | 待实测 |

还要分别测 Player 增量 HMSet、Player 强制持久化和 Farm CAS 的 P50/P95/P99，以及 Player 多次操作合并率、Dirty Age、一次 Plant 的可靠写放大。目标仍建议让典型单记录保持在小记录档位、最坏合法记录不超过 16KB；真实结果优先于文档估计。

## 9. S2 验收

- 条件创建 PlayerCore 与 FarmCore 的探针均通过；
- AccountBootstrap 在任一步崩溃后可重试到 COMPLETED，不重复发初始资产；
- 两类 RecordVersion CAS 冲突均能被识别；
- 旧 epoch 写入 100% 被拒绝；
- 新的 `playercoresize/farmcoresize` 测试替换旧 ownercoresize 假设；
- `go build ./...`、生成代码和存储测试通过。
