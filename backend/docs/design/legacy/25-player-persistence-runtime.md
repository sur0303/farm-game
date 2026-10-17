# 25 · PlayerPersistenceRuntime 与多记录 Lane 合同

版本：`v0.1-record-lane-freeze-candidate`  
日期：2026-07-29  
定位：定义 PlayerActor 的业务裁决顺序、物理记录写入顺序、跨记录恢复、Unknown Outcome、钝化与迁移。  
总体架构以 [22-production-architecture-v1.21](./22-production-architecture-v1.21.md) 为准。

---

## 1. 结论

PlayerActor 仍然是同一 `player_id` 的唯一业务裁决者，但玩家数据不再只有一个物理记录：

```text
PlayerEconomyRecord
PlayerTaskState
PlayerCodexMeta
PlayerCodexCollection
PlayerPetState（若物理分离）
```

因此将原来的 `PlayerPersistenceLane` 精确定义为：

> 每个物理权威记录都有自己的有序写 Lane；这些 Lane 由同一个 PlayerPersistenceRuntime 管理。

原 Economy 持久化状态机不推翻：

```text
SoftFlush
DurabilityBarrier
ForceFlush
UnknownOutcomeResolve
```

仍然全部进入同一 `EconomyLane`。

---

## 2. 三种不同的顺序

### 2.1 业务裁决顺序

```text
PlayerActor Mailbox
```

保证同一玩家的购买、领奖、任务、宠物和图鉴命令按确定顺序执行。它是内存状态的唯一写入者。

### 2.2 单物理记录写入顺序

```text
PersistenceLane(physical_record_key)
```

保证同一 Tcaplus 物理记录任意时刻最多一个权威写请求在途。

### 2.3 跨记录业务一致性

```text
Intent / Order / Receipt / Workflow
```

保证不同记录之间不存在永久半完成。Mailbox 串行和多 Lane 都不能提供跨记录原子事务。

---

## 3. LaneKey

```text
LaneKey {
  player_id
  record_kind
  record_partition
}
```

示例：

```text
(10001, ECONOMY, 0)
(10001, TASK, DAILY)
(10001, TASK, ACHIEVEMENT)
(10001, CODEX_META, 0)
(10001, CODEX_COLLECTION, SPRING_2026)
```

规则：

- 相同 LaneKey 严格串行；
- 不同 LaneKey 在没有显式依赖时允许并行；
- Lane 是逻辑 keyed queue，不等于独占线程；
- 所有 Lane 共用有界 I/O 执行池和连接预算；
- 每次写携带 record version、writer_epoch、payload_hash、deadline 和 trace context；
- 不得使用一份全局 storage_record_version 代替各记录独立 CAS 版本。

---

## 4. PlayerPersistenceRuntime

```text
PlayerPersistenceRuntime
├── ActorAuthorityContext
│   ├── player_id
│   ├── actor_epoch
│   ├── holder_lease
│   └── authority_state
├── LaneRegistry<LaneKey, PersistenceLane>
├── SharedIoExecutor
├── CompletionMailboxAdapter
├── DrainCoordinator
└── Metrics
```

职责：

- 创建、查找和回收 Lane；
- 对同 LaneKey 的写请求排序；
- 控制全玩家 I/O 并发和队列预算；
- 把存储完成、超时和 Unknown 解析结果重新投递到 PlayerActor Mailbox；
- 区分 Record Unknown 与 Authority Unknown；
- 在钝化和迁移前完成 Drain 审计。

后台 I/O 线程禁止直接修改：

- EconomyModel；
- TaskState；
- CodexMeta/Collection；
- Actor authority state；
- OperationResult。

---

## 5. EconomyLane

`EconomyLane(player_id, ECONOMY, 0)` 保留原 PlayerPersistenceLane 的完整能力：

```text
economy_version
persisted_economy_version
economy_storage_record_version
dirty_models
dirty_since_ms
inflight_snapshot_hash
lane_state
```

状态：

```text
IDLE
↔ SOFT_SAVE_INFLIGHT
↔ DURABLE_SAVE_INFLIGHT
↔ RESOLVING_RECORD_UNKNOWN
→ DRAINING
→ STOPPED
```

不变量：

- SoftFlush、DurabilityBarrier、ForceFlush 必须共享该 Lane；
- 旧 Soft Snapshot 不得晚于新 Durable Snapshot 覆盖记录；
- Barrier 必须吸收当前全部待落库 Economy Dirty；
- Unknown 未解析时，不得对 EconomyRecord 创建下一次权威写；
- Actor 失去 writer_epoch 时，不能将其当作普通 CAS 冲突重试。

---

## 6. TaskLane 与 CodexLane

### 6.1 TaskLane

按实际物理布局建立：

```text
TaskLane(player_id, TASK, task_partition)
```

维护：

```text
task_version
task_storage_record_version
source_watermarks
event_receipts
inflight_mutation_hash
```

负责：

- ApplyTaskEvent；
- TaskEventReceipt；
- Task 周期实例；
- Task Claim Reserve/Finalize 的 Task 侧步骤。

### 6.2 CodexLane

P0 采用两层记录：

```text
CodexMetaLane(player_id, CODEX_META, 0)
CodexCollectionLane(player_id, CODEX_COLLECTION, collection_id)
```

维护：

```text
codex_version
codex_storage_record_version
unlock_seq
inflight_mutation_hash
```

不在首版为每个植物创建独立 EntryLane。只有单 Collection 达到记录大小或 CAS 写放大阈值时，才增加 Entry Partition。

---

## 7. 跨 Lane 不能依赖完成顺序

错误实现：

```text
发送TaskLane: Task→CLAIMED
并行发送EconomyLane: Coin+100
等待两者返回
```

任何一边失败都会产生永久半完成。

正确实现：

```text
TaskClaimOrder
CREATED
→ TASK_RESERVED
→ REWARD_GRANTED
→ COMPLETED
```

其他典型跨记录流程：

| 来源记录 | 目标记录 | 恢复事实 |
|---|---|---|
| Economy | Task | TaskProgressIntent + TaskEventReceipt |
| Economy/HarvestReceipt | Codex | CodexUnlockIntent + CodexUnlockReceipt |
| Task | Economy | TaskClaimOrder + RewardReceipt |
| Codex | Task | CodexEntryUnlocked Outbox + TaskEventReceipt |

即使不同 Lane 可以并行，也必须由 Order/Intent 状态显式决定下一步，禁止通过“先发送哪个写请求”推测提交顺序。

---

## 8. TaskProgressIntent 的统一规则

只要来源事实和 TaskState 不在同一物理写单元，就必须：

```text
来源事实 + TaskProgressIntent
→ 同一可靠来源提交

TaskIntentConsumer
→ PlayerActor.ApplyTaskEvents
→ TaskLane提交TaskState + EventReceipt
```

### LOCAL_DURABLE 来源

```text
PurchaseCommitted
+ TaskProgressIntent
→ EconomyLane可靠提交
```

### ACTOR_SOFT 来源

```text
内存经济变化
+ PendingTaskIntent(source_economy_version)
→ 同一 Economy Snapshot

Snapshot Flush成功
→ Intent才允许发布
```

### 外部权威来源

```text
Farm/Workflow最终事实 + Outbox
→ DirectDispatcher
→ PlayerActor.ApplyTaskEvents
```

内存中可以提前更新任务 View 作为体验优化，但它不能替代可靠 Intent。

---

## 9. Unknown Outcome

### 9.1 Record-level Unknown

例如某个 CodexCollection CAS 超时且结果未知：

```text
RECORD_UNKNOWN(lane_key)
```

语义：

- 冻结同 LaneKey 的后续权威写；
- 启动 Get/Receipt/Version 解析；
- 与该记录无依赖的 Economy 命令可以继续；
- 依赖该记录的命令返回 PROCESSING 或 RETRYABLE；
- 不能释放相关 Lane 状态。

### 9.2 Authority-level Unknown

例如：

- writer_epoch 不匹配；
- Holder Lease 可能丢失；
- STALE_WRITER；
- ActorEpochClaim 状态不确定。

进入：

```text
AUTHORITY_UNKNOWN(player_id)
```

语义：

- 冻结 PlayerActor 全部 Lane；
- 拒绝新权威命令；
- 停止发布新的不可逆 Intent；
- 解析当前 holder 和 actor_epoch；
- 不能把问题降级成某条记录的普通 CAS 重试。

### 9.3 命令依赖

```text
CommandRecordDependencies {
  required_lane_keys[]
}
```

某条记录 Unknown 只阻塞依赖它的命令；Authority Unknown 阻塞全部命令。

---

## 10. 完成回调

统一时序：

```text
PlayerActor turn
→ Prepare immutable mutation
→ Enqueue Lane write
→ 结束当前turn

I/O完成
→ CompletionMailboxAdapter
→ PlayerActor mailbox
→ 校验actor_epoch / lane_request_id / payload_hash
→ 提交内存状态或进入Unknown解析
```

旧 Actor 代次的完成回调只能丢弃或交给恢复逻辑，不能修改新 Actor 内存。

---

## 11. 钝化、迁移与 Drain

Actor 释放写权前必须满足：

```text
1. Economy Dirty已ForceFlush
2. 当前权威写已完成或Unknown已解析
3. 不存在仅在内存中且无可靠来源的权威事实
4. 所有Lane停止接收新写
5. writer_epoch/Lease按迁移合同交接
```

不需要等待：

- 已有可靠 TaskProgressIntent 的 TaskConsumer 完成；
- 已有可靠 CodexUnlockIntent 的 CodexConsumer 完成；
- 已有可靠 Outbox 的 RedPoint/Tlog/Projection 完成。

判断原则：

```text
只有内存事实、无外部可靠来源
→ 必须Flush

已经有Intent/Order/Outbox可靠覆盖
→ Worker可以恢复，Actor允许钝化
```

---

## 12. P0 实现与生产演进

合同从现在开始按 Record Lane 冻结。

P0 可以保守实现：

```text
所有LaneKey
→ 映射到同一Player写执行队列
```

这在正确性上安全，只会产生额外 Head-of-Line Blocking。

压测后演进：

```text
EconomyLane独立
TaskLane按TaskSet分区
CodexMeta/Collection按record_key分区
```

无论 P0 是否真实并行，代码、日志和指标都必须携带 LaneKey，避免把“暂时共用执行队列”固化成业务合同。

---

## 13. 背压和容量

```text
player_persistence_lane_queue_items{record_kind}
player_persistence_lane_queue_bytes{record_kind}
player_persistence_lane_wait_ms{record_kind}
player_persistence_lane_inflight{record_kind}
player_persistence_lane_unknown_total{record_kind,scope}
player_persistence_lane_cas_conflict_total{record_kind}
player_persistence_lane_write_latency_ms{record_kind}
player_persistence_runtime_active_lanes
player_persistence_completion_mailbox_lag_ms
```

高基数 player_id、collection_id 不进入 Metrics Label，只进入 Trace、采样日志和 TopK。

限流层级：

- 单 Lane item/byte/age；
- 单 Player 活跃 Lane 数；
- 单 Pod I/O 并发；
- 单表连接与 QPS；
- 全局激活/回写风暴保护。

---

## 14. 硬合同

```text
HC-PERSIST-01
PlayerActor Mailbox负责同一player_id的业务裁决顺序；
PersistenceLane负责同一物理权威记录的存储写顺序，二者不得混为一谈。

HC-PERSIST-02
同一physical_record_key任意时刻最多一个权威写请求在途；
不同physical_record_key在没有显式依赖时允许并行。

HC-PERSIST-03
PlayerEconomyRecord的SoftFlush、DurabilityBarrier和ForceFlush
必须共享同一EconomyLane。

HC-PERSIST-04
TaskState、PlayerCodexMeta/Collection与PlayerEconomyRecord物理分离时，
不得依赖多个Lane获得跨记录原子性；必须使用Intent、Order、Receipt或Workflow恢复。

HC-PERSIST-05
所有持久化完成和Unknown Outcome解析结果必须重新进入PlayerActor Mailbox；
后台I/O线程不得直接修改Actor状态。

HC-PERSIST-06
Record-level Unknown只冻结相关Lane；
writer_epoch或Lease不确定属于Authority-level Unknown，必须冻结整个PlayerActor。

HC-PERSIST-07
Actor释放写权前，所有未被可靠Intent、Order或Outbox覆盖的内存权威事实必须完成ForceFlush。

HC-PERSIST-08
跨Lane并行写不得依赖完成顺序；
存在业务先后依赖时必须由Order或Workflow状态显式推进。
```

---

## 15. 验收标准

1. 同 EconomyLane 的 SoftFlush、Barrier、ForceFlush 不会并发 CAS。
2. 不同物理记录在启用并行后能够同时写入。
3. Codex Record Unknown 不会无条件阻塞普通 Buy。
4. writer_epoch 不匹配会冻结整个 PlayerActor。
5. 存储完成回调不会从 I/O 线程直接修改 Actor 状态。
6. TaskClaim 任意步骤崩溃后不会重复发奖或永久丢奖。
7. Economy 提交后 TaskProgressIntent 可恢复任务进度。
8. Harvest 资产到账后 CodexUnlockIntent 可恢复图鉴。
9. Actor 钝化前无可靠来源的 Economy Dirty 必须 ForceFlush。
10. 已有可靠 Intent/Outbox 的下游未完成工作不阻塞 Actor 钝化。
11. P0 共用执行队列和生产多 Lane 实现通过同一业务测试集。
12. Metrics 能区分 Economy、Task、Codex 的等待、延迟、Unknown 和冲突。
