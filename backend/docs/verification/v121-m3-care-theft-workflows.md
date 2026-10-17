# M3：Care 与 Theft 多 Participant Workflow

版本：v1.21-m3  
日期：2026-07-29  
状态：实现与故障恢复测试通过

## 1. 为什么原实现不够

原 Water 只修改 `CropCycle.watered`，没有护理事件、帮助者奖励、农场主奖励和
可恢复流程。原 Theft 在 FarmRecord 扣一份可偷量后，只给访客一个胡萝卜，
没有农场主收益；如果发奖过程中失败，也无法表达多个 Player Participant 的
完成情况。

M3 要保证：

- Gate 只把 Care/Theft 首跳到 farm-svr；
- Workflow 必须先于权限和 Farm Participant 创建；
- FriendshipDomain 签发绑定 operation/action/relation epoch 的 Permit；
- FarmActor 唯一裁决作物状态、护理事实和可偷份额；
- PlayerActor 分别裁决各玩家资产；
- 任意两个 Participant 之间退出都能由 Worker 恢复；
- 同一 Participant 奖励最多生效一次。

## 2. 数据模型

`FarmSettlement` 增加参与者列表：

```text
FarmSettlement
  kind = HARVEST | CARE | THEFT
  state = TARGET_COMMITTED | COMPLETED
  crop_cycle_id
  relation_epoch
  participants[]
    player_id
    role = OWNER | VISITOR | HELPER | HARVESTER
    item_id / item_count
    coin_delta
    exp_delta
```

PlayerActor 新增内部可信 `ApplyReward` Participant。一次调用可以在一个
PlayerRecord CAS 中原子修改金币、物品、经验和等级，并写
`PlayerOperationReceipt`。

Reward Receipt 的 Hash 不是简单复用外层命令 Hash，而是：

```text
SHA256("farm-reward-v1" + workflow_payload_hash
       + deterministic(FarmSettlementParticipant))
```

因此同一个 operation 下，不同玩家、角色或奖励值具有不同的 Participant
意图；重试同一步返回原结果，改变奖励值会触发幂等冲突。

## 3. CareWorkflow

好友护理时序：

```text
farm-gate
→ farm-svr / FarmOperationCoordinator
→ CreateIfAbsent CareWorkflow
→ FriendshipDomain.IssueActionPermit(CARE)
→ FarmActor.PrepareCare
   - 校验地块、CropCycle、expected version
   - 唯一提交 watered 与 CareAttemptReceipt
   - 创建 CARE FarmSettlement
→ Helper PlayerActor.ApplyReward(exp +2)
→ Owner PlayerActor.ApplyReward(exp +1)
→ FarmActor.CompleteOperation
→ CareWorkflow COMPLETED
```

自己的农场浇水仍是单 FarmActor 操作，不制造跨 Actor Workflow。好友护理才
进入 CareWorkflow。

如果作物已经被护理，返回 `NO_CHANGE` 并闭合 Workflow，不重复发奖励。

## 4. TheftWorkflow

偷菜时序：

```text
farm-gate
→ farm-svr / FarmOperationCoordinator
→ CreateIfAbsent TheftWorkflow
→ FriendshipDomain.IssueActionPermit(THEFT)
→ FarmActor.PrepareTheft
   - 校验成熟、份额、访客本茬唯一 Attempt
   - stolen_total +1
   - 写 TheftAttemptReceipt
   - 创建 THEFT FarmSettlement
→ Visitor PlayerActor.ApplyReward(carrot +1, exp +1)
→ Owner PlayerActor.ApplyReward(coin +1)
→ FarmActor.CompleteOperation
→ TheftWorkflow COMPLETED
```

客户端、Gate 和 Worker都不能自行计算可偷数量或奖励。Worker只读取已经由
FarmActor 冻结的 Settlement。

## 5. 故障恢复

在线路径每完成 FarmActor 本地提交后，将 Workflow 标为
`FARM_COMMITTED`。后续任一步失败返回 `PROCESSING`，不回滚已经成立的
Farm 事实。

Worker Claim 后：

1. 查询 Farm Operation Receipt；
2. 对 Settlement 的每个 Participant 调用同一 `ApplyReward`；
3. 已到账 Participant 返回 `NO_CHANGE`；
4. 尚未到账 Participant 完成 CAS 与 Receipt；
5. 全部闭合后完成 Farm Operation 和 Workflow。

已验证的关键故障点：

```text
访客胡萝卜已到账
→ 农场主金币调用超时
→ 在线请求返回 PROCESSING
→ Worker Claim
→ 访客 Reward Receipt 防重
→ 农场主 coin +1
→ Farm/Workflow COMPLETED
```

恢复后访客仍只有一个新增胡萝卜，农场主只增加一个金币。

## 6. 验证

- Care：Helper exp +2，Owner exp +1，CareWorkflow 完成；
- Theft：Visitor carrot +1 / exp +1，Owner coin +1；
- 重放 Theft 不重复任何奖励；
- Worker 可以完成两个 Care Participant；
- Worker 可以从 Theft 两个 Player Participant 中间恢复；
- ApplyReward 同时修改 item/coin/exp，并拒绝同 ID 不同奖励；
- M1 的 Revision/Lease fencing 和公司持久 Redis Workflow Store 被直接复用。
