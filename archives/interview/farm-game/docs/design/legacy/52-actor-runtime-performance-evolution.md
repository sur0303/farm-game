# 52 · 双 Actor Runtime、Player 异步回写与持久化性能

本文定义当前必须实现的性能保护、从 UC 借鉴的 Player Model 异步回写思想，以及 Farm/Workflow 仍保持可靠提交的边界。业务边界见 [16-player-farm-actor-split](./16-player-farm-actor-split.md)，统一结论见 [18-final-architecture-blueprint](./18-final-architecture-blueprint.md)。

---

## 1. 当前基线

1. 使用 `PlayerActor(player_id)` 与 `FarmActor(farm_id)`；
2. 同 ActorKey 串行，不同 ActorKey 并行；
3. Player 普通金币、普通库存、ShopCounter 和普通成长由同一 PlayerActor 内的 EconomyTxn 原子修改，返回成功后异步增量回写 Tcaplus；
4. Player 普通软资产接受“Pod 在回写前硬崩溃时回退至最近 Tcaplus 版本”的受控 RPO；
5. FarmCore、付费货币、唯一领取以及 Plant/Harvest/Theft 等跨 Actor 参与步骤仍以可靠持久化为 ACK 边界；
6. Plant/Harvest 等跨 Actor 操作由持久 Workflow/Intent/Settlement + Outbox + Receipt 收敛；
7. Broadcast/Projection 异步，不回滚已提交业务；PlayerPublicProfile 只能在来源 Player 版本回写成功后更新；
8. Writeback Queue 是 player-svr 进程内队列，不冒充 WAL、EventBus 或可靠消息。

## 2. Player 异步回写的准确边界

这次采用的不是“资产最终一致、谁都可以写”，而是：

```text
同一 PlayerActor 内串行
→ EconomyTxn 先完整 Check
→ Wallet + Inventory + ShopCounter + 本地 Receipt 一次内存 Apply
→ DirtyModel / version++
→ 普通 Buy/Sell 返回 SUCCEEDED
→ Versioned Unique Writeback Queue
→ 增量 HMSet Tcaplus
```

Actor 活着期间，后续请求始终读取最新内存，在线一致性仍是强的；放宽的是非优雅崩溃的持久性。以下内容不得进入普通 Dirty 成功路径：

- SeedReservation、PlantReceipt；
- Harvest/Loot/Care 的跨 Actor RewardReceipt；
- 偷菜罚金与农场主金币转账；
- Task/Mail 唯一领取；
- CropCycle、偷菜份额和 CareIncident；
- 付费货币、订单和支付流水；
- Workflow、Settlement 和关键 Outbox。

这些步骤可以复用同一个 PlayerEconomyModel，但必须使用 `EconomyTxn(commit_mode=DURABLE)`，强制保存包含资产变化和 Receipt 的版本后再向 Coordinator ACK。

普通 P-ASYNC 事务的外部投影/BI 事件只能在 `OnSaveSucc(captured_version)` 后释放。Actor 内任务变化可与经济模型一起标脏；必须可靠触发跨服务结果的命令应升级为 P-DURABLE + Outbox，不能从尚未保存的内存状态直接发布可靠事件。

若未来连 Farm 事实也要改成“日志确认、状态异步物化”，必须另外设计持久顺序日志、复制、恢复重放、快照和积压治理；不能从 Player 的进程内 Dirty Queue 推导出来。

## 3. 拆分后的性能变化

### 3.1 优点

- 普通 Buy/Sell 不再把 Tcaplus P99 放在接口关键路径；
- 同一玩家连续操作可以合并为一次增量回写；
- 只序列化发生变化的 Model，降低写放大；
- 热门农场的 Join/Care/Theft 不阻塞 owner 的 Buy/Task/Mail；
- 好友只访问农场时不必激活目标 PlayerActor；
- PlayerActor 与 FarmActor 可独立钝化和迁移；
- 邮箱、内存和 Store 延迟能按业务类型治理。

### 3.2 成本

- Player Pod 硬崩溃可能丢失尚未回写的普通软资产；
- 需要 DirtyTracker、版本捕获、去重队列、ForceFlush、积压背压和独立监控；
- 活跃 Player 的 DB 快照可能落后，查询必须找 PlayerActor；公开投影只允许展示；
- 同时在线时 Actor 数最多接近旧模型两倍；
- Plant 至少 3 次 CAS：Player reserve、Farm apply、Player finalize；
- Harvest 通常至少 3 次 CAS：Farm accept、Player credit、Farm complete；
- 增加 Coordinator/Worker 消息、Outbox/Receipt 字段和 PROCESSING 状态；
- S3 就需要最小可靠 Worker，而不是等偷菜阶段再引入。

## 4. 容量模型必须重算

不能再用：

```text
业务写 QPS = OwnerCore CAS QPS
```

应按工作流步骤展开：

```text
Player内存经济命令QPS
= Buy + Sell + 普通成长 + 普通库存变化

Player异步回写QPS
= 每秒唯一脏Player数
  × 平均增量字段数
  × 回写重试系数

Player可靠写QPS
= Plant * 2
  + HarvestCredit
  + CareReward
  + TheftLoot
  + Task/MailClaim
  + 付费/转账

Farm CAS QPS
= PlantApply
  + Harvest * 2
  + Care
  + Theft * 2
```

实际还需乘：

- CAS 冲突重试系数；
- Player 多次操作合并率和 Dirty Age；
- RPC/Store 结果未知重投系数；
- 终态归档/投影写放大；
- 峰值在线与热门农场系数。

30M DAU 验收必须给出 Player 内存事务、Player 异步回写、Player 强制持久化、Farm CAS 四套 QPS，以及记录大小、Actor 内存和热点分布，不能只给总 DAU。

## 5. S2/S3 必须具备

### S2

- ActorKey 含 kind；
- PlayerActorRegistry/FarmActorRegistry；
- 两类 Actor 独立 autoload/passivation；
- 有界 mailbox 和 enqueue timeout；
- Load singleflight 与激活速率限制；
- PlayerStore/FarmStore CAS 指标；
- Player DirtyTracker、version/saved_version/modified_version；
- 每 Player 单 in-flight save、UniqueWritebackQueue 与保存失败重入队；
- ForceFlush、脏龄/版本差/队列深度背压；
- Join 下行不阻塞 FarmActor；
- Stop 时拒绝新任务并完成已接收 turn；Player 正常钝化前 Flush。

### S3 Plant

- PlantIntent/PlantReceipt/Outbox 持久化；
- ReserveSeed 使用 `DURABLE` EconomyTxn，不能以普通 Dirty 内存提交回复 RESERVED；
- PendingWork 可分页分片发现，不全表扫描；
- Workflow age、retry、permanent reject、refund 指标；
- 相同 txn 重投测试；
- Farm 已 APPLIED、Player finalize 延迟恢复测试；
- Worker 重启/重复消费测试；
- 端到端 PROCESSING 比例和最长收敛时间。

## 6. UC 值得借鉴的能力

借鉴思想，不复制旧项目私有实现：

| 能力 | 新项目用途 | 引入阶段 |
|---|---|---|
| 稳定 GID/Shard 计算 | ActorKey → Virtual Shard | S2 |
| autoload/passivation | 控制双 Actor 常驻内存 | S2 |
| modifiedVersion/savedVersion | 防止旧保存完成后误清除新修改 | S2/P0b |
| UniqueFifo 增量回写 | 合并同玩家多次普通软资产修改 | P0b |
| mailbox watermark | 热点过载和背压 | S2/压测前 |
| Actor 数量/内存上限 | 防节点 OOM | 压测前 |
| 激活速率限制 | 防登录/热门农场加载风暴 | S2 |
| Gateway 下行背压 | 慢客户端不拖 FarmActor | S2 |
| Session Lane / BackendPool 分离 | Gate 不持有用户串行上下文等待 Logic RPC | S2 |
| Redirect/shard revision | 多 Logic 路由过期修正 | 多节点前 |
| Shard handoff/fencing | 滚动发布、扩缩容、避免双写 | 多节点前 |

不直接复用 UC 的 PlayerActor 合并边界、zonesvr、Session/业务 Proto 或 ACK/FrameCache 语义，也不把 UC 的进程内 Sync FIFO 宣称为可靠消息。平台批准版本的 gateproxy 与 xrpc/proxyless 可以作为公共基础设施依赖，但 Farm 必须实现自己的 ActorAddress、双 Region、routing v2、Store fencing、Player Writeback 和业务工作流。

多节点演进固定为：

```text
S2：静态单 Logic + routing v2 黄金向量
→ 多 Logic：xrpc/proxyless 优先，LogicIngress Forward 回退
→ Shard Lease/revision + Redirect + writer_epoch
→ DB Reload Handoff
→ 只有恢复时间不达标时才增加 P2P 热状态 Handoff
```

Gate 滚动更新不沿用 Logic Shard 迁移语义。当前没有任意下行 ACK/重放，先 drain + reconnect + 重新 Join；无感 Session Handoff 是独立的后续体验优化。

## 7. 热点治理

FarmActor 热点来源：

- 多好友同时 Join；
- 同一热门农场护理/偷菜；
- Snapshot 大量生成；
- 广播扇出。

PlayerActor 热点来源：

- 重复购买/领奖；
- Worker 奖励集中到账；
- 多端/重试风暴。

治理顺序：

1. 有界邮箱与 per-command 限流；
2. Join 人数上限和广播批处理；
3. Snapshot/Projection 避免无谓加载 PlayerActor；
4. 慢下行断开而非阻塞；
5. 热点 Actor 指标和告警；
6. 真正需要时再做 Shard 迁移或独立扩容。

不能把一个 FarmActor 拆成多个并发写者来消除热点，否则会破坏地块唯一裁决顺序。

## 8. Co-location

P0 可让同一玩家的 PlayerActor 与 FarmActor 倾向落在同一 Logic 以减少 Plant/Harvest 一跳，但：

- 路由合同仍以独立 ActorKey 为准；
- co-location 只是优化，不是正确性前提；
- 迁移时允许分开；
- Intent/Receipt 不能因当前同进程而省略。

只有压测证明跨 Logic RPC 显著占比后，再评审“关联 Actor 亲和放置”。不要因此恢复成合并 Actor。

## 9. 异步回写保护门

普通 Player 软资产允许异步回写，但满足任一保护条件时必须停止接受新的此类写命令：

1. 最老 Dirty Age 超过配置阈值；
2. `version - saved_version` 超过单 Player 阈值；
3. UniqueWritebackQueue item/byte 水位超过上限；
4. Tcaplus 连续保存失败或出现 fencing 不确定；
5. PlayerActor 正在 Drain/Handoff/passivation；
6. 无法保证同一 Player 只有一个 in-flight save；
7. HMSet/FieldUpdate 原子性探针不满足 PlayerEconomyModel 一致性组。

保护触发后：

```text
查询仍从 Actor 内存服务
→ 新普通资产写返回 RETRYABLE/SYSTEM_BUSY
→ 优先队列 ForceFlush
→ 恢复到低水位后再开放
```

若未来希望把已经接受的 Player 普通软资产也做到硬崩溃零丢失，候选是“持久日志确认后成功、Tcaplus 状态异步物化”，而不是把进程内队列换个名字。该升级需要日志复制、恢复重放、幂等物化和 RPO/RTO 评审。

## 10. 压测验收

至少分四组：

1. **单 Player 热点**：Buy/Sell 并发、邮箱过载、Dirty 合并、回写积压与强制持久化竞争；
2. **单 Farm 热点**：8 人 Join/Care/Theft、Snapshot 与广播；
3. **跨 Actor Plant/Harvest**：正常、超时、重复、Worker 重启、永久拒绝；
4. **混合流量**：热门 Farm 高流量时，同 owner 的 PlayerActor Buy 延迟是否保持稳定。

报告必须包含：

- active Player/Farm Actor 数和内存；
- mailbox depth/oldest wait/reject；
- Player/Farm CAS P50/P95/P99、冲突率和结果未知；
- Player Writeback 合并率、队列深度、最老 Dirty Age、version gap、ForceFlush P99 和背压拒绝；
- 普通 Buy/Sell 接口延迟与“写回前 kill -9”后的已确认 RPO 证据；
- Plant/Harvest 总延迟、PROCESSING 比例、最终收敛时间；
- Worker backlog 与最老 PendingWork；
- 下行 QueueFull/断线/Resync；
- 节点 CPU、内存、GC、网络与 Tcaplus 吞吐。
