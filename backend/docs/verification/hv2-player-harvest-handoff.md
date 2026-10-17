# HV2 Player Harvest Participant handoff

本分支完成 `PlayerActor(player_id)` 权威侧 typed `GRANT_HARVEST_REWARD`，没有修改 Proto/generated、cmd、Farm、Workflow/Coordinator、路由控制面或设计合同。

## 已完成

- `Player Participant` 现在路由 `GRANT_HARVEST_REWARD`；Plant 的三个步骤仍走原有路径。
- 增加 `HarvestDependencyVerifier` 接缝。未提供 Work/Farm 物理证明时 fail closed；证明接缝负责核对保存的 `COMMIT_HARVEST` result hash、Harvest 状态/阶段和 player/owner/workflow/cycle 绑定。
- 校验 `HarvestDecisionHash`，并提供 `CommitHarvestResultHash` 的 typed canonical hash helper。
- 一个 PlayerActor mailbox turn 内完成库存、经验/等级、HarvestCounter、Harvest task、首次自种收获图鉴、ParticipantReceipt 和 RecordVersion；P0 不做库存容量业务拒绝。
- Harvest 的 typed result proof 上限单独扩展，并把原始 `GrantHarvestRewardResult` 同步放进 Authority `StoredOperationOutcome`。因此 commit 后响应丢失可恢复原结果。
- 同 invocation 同 hash 从 durable receipt 返回原始 result/delta；不同 hash 返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`。replay 不重新按当前图鉴集合计算，也不重复资产或任务变更。
- 图鉴资格只使用 HarvestDecision 携带的冻结 snapshot，复用 `compendium.DecideUnlock`；活动归档、遗留种子和偷来的作物不会重新读取当前 Catalog 或错误解锁。

由于本任务冻结 Proto/generated，HarvestCounter 以每个 namespaced ParticipantReceipt 中的已提交 `HarvestCountAfter` 作为现有 durable receipt carrier 上的单一递增事实；新 turn 从该 carrier 校验并递增，不另建 PlayerActor 状态源。

## 验证覆盖

新增 Participant 测试覆盖：首次解锁、已解锁、活动归档/遗留资格冻结、偷取作物、同 hash replay、hash 冲突、任务只推进一次、缺物理证明、commit 后响应丢失、TimeoutUnknown 前后提交恢复。Plant 原有测试仍通过。

## 当前边界

本分支只实现 Player 权威侧 Participant；Work/Farm proof 的生产组合仍由上层注入 `HarvestDependencyVerifier`，没有在这里建立 Farm/Workflow 状态源或入口 wiring。
