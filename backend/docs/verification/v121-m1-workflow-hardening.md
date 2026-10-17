# M1：跨权威 Workflow 可靠性加固

版本：v1.21-m1  
日期：2026-07-29  
状态：代码与 Redis 实测通过

## 1. 要解决的问题

Plant、Harvest、Theft 会依次修改 Workflow、PlayerActor 和 FarmActor
三份独立权威记录。进程可能在任意两步之间退出，网络超时也不能证明远端
Participant 没有成功。如果只有在线调用链，没有持久 Workflow 和本地
Receipt，会出现以下错误：

- 相同 `operation_id` 携带不同参数时误复用旧结果；
- Farm 已经种下，但种子 Reservation 永远没有提交；
- Farm 已结束作物周期，但收获或偷菜奖励没有到账；
- 两个 Worker 或在线 Coordinator 同时推进同一 Workflow；
- 旧 Worker 在 Lease 过期后覆盖新 Worker 的进度；
- Redis Cluster 中多 Key Lua 因跨 slot 失败。

## 2. 方案选择

当前阶段使用公司持久 Redis 作为业务数据库。Workflow 是可恢复执行状态，
Player/Farm Receipt 才是各领域已经完成本地步骤的证明。

采用以下约束：

1. Coordinator 必须先 `CreateOrLoad` Workflow，再调用任何 Participant。
2. `workflow_id == operation_id`，并与规范化命令的 SHA-256
   `payload_hash` 绑定。
3. `request_id` 和 `operation_id` 本身不参与业务载荷 Hash；业务参数、
   目标 Owner 和 expected version 参与。
4. Player Reservation、Player Reward Receipt、Farm Operation Receipt
   同样保存并校验 `payload_hash`。
5. Workflow 每次变更都校验 `workflow_revision`。
6. Worker 只能使用 `lease_owner + lease_epoch + lease_until_ms` 推进；
   在线 Coordinator 不能覆盖仍然有效的 Worker Lease。
7. Participant 调用可以重复，Participant 本地事实只能提交一次。
8. Worker 不根据内存中的步骤猜测结果，而是查询 Farm Receipt/Settlement
   后决定继续、补偿或闭合。

没有采用分布式事务。Redis、PlayerActor 和 FarmActor 不是同一原子写单元，
强行包装成同步事务会把网络故障变成锁和资源悬挂问题。可恢复 Workflow +
Participant Receipt 更符合游戏中跨 Owner 长流程。

## 3. 实现结果

### Workflow Store

- `CreateOrLoad` 原子创建或加载，并校验不可变业务意图；
- Memory Store 与 Redis Store 使用同一 Revision/Lease 合同；
- Redis 使用 Lua 原子创建、更新、Claim；
- Claim 会同时递增 Revision 和 Lease Epoch；
- 成功更新释放 Lease，失败重试重新进入 Due Queue；
- 终态从 Due Queue 删除；
- Redis Key 统一包含 `{workflow}` hash tag，保证一个 Lua 中的记录和
  Due Queue 位于同一 Redis Cluster slot。

### Participant 幂等

- Player 普通经济命令保存 `PlayerOperationReceipt.payload_hash`；
- Plant Reservation 保存 `ItemReservation.payload_hash`；
- Harvest/Theft 发奖保存 Reward Receipt 的 `payload_hash`；
- Farm 的 Plant/Water/Harvest/Theft Receipt 保存 `payload_hash`；
- 相同 ID、不同载荷统一返回 `ERR_IDEMPOTENCY_CONFLICT`。

### Worker 恢复

- Farm 没有 Operation Receipt：Plant 释放 Reservation，Workflow
  进入 `COMPENSATED`；
- Farm Receipt 为 `PROCESSING`：根据 Workflow Kind 提交 Reservation
  或发放奖励，再完成 Farm Operation；
- Farm Receipt 已为 `SUCCEEDED`：只闭合 Workflow，不重复 Participant；
- Participant 暂时不可用：带退避写回 `RETRY_WAIT`；
- Lease 过期或 Revision 落后：旧推进者写回失败，由新 Claim 继续。

## 4. 验证证据

已通过以下测试：

```text
go test ./...
PASS
```

覆盖内容包括：

- 32 个并发创建者中只有一个成功创建 Workflow；
- 相同 Workflow ID、不同 Payload Hash 被拒绝；
- 在线写者不能绕过 Worker Lease；
- Lease 过期重领后 Revision 和 Epoch 均递增；
- 旧 Worker 不能覆盖新 Worker；
- Plant 未提交 Farm 时释放 Reservation；
- Plant 已提交 Farm 时 Worker 完成种子提交；
- Harvest 发奖失败后，新 Worker Claim 并恢复；
- Farm 已完成时不重复 Player Participant；
- FarmActor、PlayerActor 独立校验同 ID 不同载荷；
- farm-svr 重启后从 Farm Receipt 恢复 Harvest，奖励只到账一次。

真实公司持久 Redis 还运行了 `TestLiveRedisWorkflowStore`，验证：

- Hash 与 ZSET 可用；
- Lua 原子创建和加载；
- 幂等冲突；
- Claim 和 Lease fencing；
- Worker 完成写回；
- 终态不会再次被 Claim。

测试使用唯一临时前缀，并在结束后删除精确的测试 Key，不读取或修改正式玩家
数据。

## 5. M1 之后的边界

M1 只建立跨权威可靠执行底座。以下内容由后续里程碑完成：

- M2：canonical 好友关系、Share MULTI_ACCEPT、relation epoch、ActionPermit；
- M3：在相同底座上实现完整 Care/Theft 多 Participant 结算；
- M4：弱网重放、Gate drain、下行背压和滚动发布；
- M5：多 Pod Actor Assignment、writer epoch 和 handoff；
- M6：任务、邮件、宠物、图鉴、红点、机会和 TLog。
