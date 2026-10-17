# 88 · Actor Dirty Writeback 实验实现基线

状态：`v1.0-implemented-experiment`

日期：`2026-08-24`

基线：`origin/master@280daa13bc004560e3c5d5665db492de429c54c6`

实验分支：`codex/actor-dirty-writeback-experiment-r1`

实验工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-actor-dirty-writeback-20260824`

> 本文记录已经落地的独立性能实验，不替代 26～37 号可靠生产合同，也不改变默认运行模式。`durable_cas` 仍是默认值；只有显式配置 `FARM_GAME_DATA_MODE=dirty_best_effort` 才会启用本实验语义。

## 1. 实验结论

本分支已经实现一条 UC 风格的 Actor Dirty Writeback 路径：

```text
Actor命令
→ Mailbox串行裁决
→ 修改进程内最新状态
→ actor_memory_version++
→ Dirty队列按actor_id合并
→ 立即结束当前Actor Turn

后台到期
→ Mailbox内冻结Snapshot(V)
→ Mailbox外执行Redis CAS
→ Mailbox内安装完成结果
→ 若期间已产生V+1，则继续保持Dirty并再次排队
```

它把 PlayerActor/FarmActor 的业务 Redis 写移出普通命令热路径，同时保留：

- Actor Mailbox 的单 Actor 串行；
- Redis 恢复快照；
- `writer_epoch + writer_claim_id + persisted_record_version` 的最终写 Fence；
- 正常 Drain 时的强制 Flush；
- 同一 Actor 在进程存活期间的内存幂等 Receipt。

它明确放弃：

- 每条业务命令返回前的 Redis 耐久性；
- 硬崩溃前未 Flush 状态的恢复；
- Plant、Harvest、Care、Theft、Unlock、GuardSync 的持久 SourceOrder 恢复；
- 跨 Actor 业务结果的最终一致保证。

因此它适合作为性能上限和体验取舍实验，不能在没有故障注入、RPO 验收和产品接受的情况下直接替代生产可靠模式。

## 2. 两种运行模式

| 模式 | 配置 | 普通 Actor Turn | 跨 Actor | 崩溃恢复 |
|---|---|---|---|---|
| 可靠模式 | `FARM_GAME_DATA_MODE=durable_cas` 或不配置 | 每次变更同步、带 Fence 的 Redis CAS | SourceOrder + DueIndex + Direct + Worker | 恢复到最近一次命令提交 |
| 实验模式 | `FARM_GAME_DATA_MODE=dirty_best_effort` | 只改内存并 MarkDirty，后台合并 Flush | 直接调用 Participant；不持久化 SourceOrder | 只恢复到最近一次成功 Flush |

账号密码、登录 Ticket、SessionAuthority、SessionRoute、连接队列、Actor 路由控制、Shard Lease、canonical FriendRelation、MailEntry 等不属于本次 Dirty 改造，继续使用原有存储和生命周期。

## 3. 从 UC 借鉴了什么

已核对 Linux Go module 缓存中的 UC `xrpc@v0.3.91`。本实验采用了以下核心模式：

1. 业务 Turn 只推进内存版本并标 Dirty；
2. Dirty 队列按 Actor 唯一合并，连续多次修改不产生同等数量的存储任务；
3. Flush 时冻结版本 V，Redis I/O 不占用 Actor Mailbox；
4. 仅确认冻结版本 V，Flush 期间出现的 V+1 仍保持 Dirty；
5. 保存失败时在进程内重新排队。

本实验没有原样照搬 UC：

- PlayerActor 的 Binding 缓存最新编码 `ModelState`，每个 Turn 从内存解码 `PlayerRecord`；它不是额外长期持有一份可变的已解码对象，但不会再次读取 Redis。
- FarmActor 继续持有现有可变 `FarmRecord`。
- UC 的 Redis 保存路径不提供本项目这套持久 CAS/Fence；本实验仍在最终 Flush 时校验 `writer_epoch` 和 `writer_claim_id`，避免旧 Holder 的迟到 Flush 覆盖新 Holder。
- 当前没有实现可用于完整业务重放的 TLog。日志只能作为观测方向，不能宣称能恢复未 Flush 的状态。

## 4. Actor 内存与版本模型

### 4.1 PlayerActor

Player 的最新编码模型位于 `authority.Binding`：

```text
Binding
├── models                 最新进程内ModelState
├── version                最新actor_memory_version
├── persistedModels        最近成功Flush的ModelState
├── persistedVersion       Redis物理RecordVersion
├── softGeneration         本地Dirty代次
├── softDirty
├── softSchedules
└── PersistenceLane
```

首次激活执行一次 `LoadAndClaim`。后续 Turn 从 Binding 内存快照解码 PlayerRecord；不得因物理容量快照暂不可用而退回 Redis，否则会用旧持久态覆盖未 Flush 的新状态。

### 4.2 FarmActor

FarmActor 继续直接持有 `FarmRecord`，并把最新编码候选同步安装到 Binding：

```text
FarmActor.record
→ build Farm ModelState + OpportunityHeader + OpportunityFacts
→ Binding.StageSoftModelState
→ recordVersion推进为逻辑memory version
→ Registry.MarkDirty(farm_id)
```

`actor_memory_version` 已加入 Player/Farm Proto。它表达逻辑 Actor 状态版本；Redis `RecordVersion` 只表达物理 Flush 次数。连续 500 个内存 Turn 可以只对应 1 次后续 Flush，因此二者不能再混用。

## 5. Dirty Scheduler 与 Flush

每个 player-svr/farm-svr 进程拥有一个 `dirtywriteback.Scheduler`：

```text
entries: map[actor_id]*entry
priority queue: 按due时间排序
workers: 有界并发Flush
```

同一 Actor 无论在窗口内修改多少次，只占一个 `entries` 项。该项在 Flush in-flight 时仍保留；此时再次 Mark 会设置 `remarked`，完成后重新入队，避免漏掉并发的新版本。

默认参数：

| 环境变量 | 默认值 | 作用 |
|---|---:|---|
| `ACTOR_DIRTY_FLUSH_INTERVAL` | `3s` | 普通合并窗口 |
| `ACTOR_DIRTY_FLUSH_JITTER` | `600ms` | 分散批量 Flush 峰值 |
| `ACTOR_DIRTY_MAX_AGE` | `10s` | 单 Actor 最长脏龄目标 |
| `ACTOR_DIRTY_RETRY_DELAY` | `1s` | 保存失败后的进程内重试 |
| `ACTOR_DIRTY_FLUSH_TIMEOUT` | `2s` | 单次外部 Redis I/O 上限 |
| `ACTOR_DIRTY_FLUSH_WORKERS` | `4` | 每进程 Flush 并发 |
| `ACTOR_DIRTY_QUEUE_SIZE` | `1024` | 每进程最多跟踪的唯一脏 Actor 数 |
| `ACTOR_DIRTY_DRAIN_TIMEOUT` | `8s` | 停服最终 Flush 预算 |

队列满时不能留下“已变更但无人负责 Flush”的 Actor。当前实现会在该 Turn 内退化为一次同步 Flush；这会牺牲该请求延迟，但保持状态仍有写回责任人。

Player/Farm 进程现有 `/metrics` 已统一暴露：

```text
farm_actor_dirty_writeback_marks_total
farm_actor_dirty_writeback_coalesced_marks_total
farm_actor_dirty_writeback_flush_attempts_total
farm_actor_dirty_writeback_flush_succeeded_total
farm_actor_dirty_writeback_flush_failed_total
farm_actor_dirty_writeback_queue_depth
farm_actor_dirty_writeback_inflight
farm_actor_dirty_writeback_oldest_dirty_age_seconds
farm_actor_dirty_writeback_max_queue_depth
```

其中 `oldest_dirty_age_seconds`、`flush_failed_total` 和队列容量比平均 Flush 延迟更适合做首批安全告警。

### 5.1 三段式 Flush

```text
Prepare（Actor System Turn）
├── 冻结不可变ModelState(V)
├── 记录softGeneration
└── BeginSoft(V)

Execute（Actor Mailbox之外）
├── 生成稀疏ModelMutation
├── Redis CAS
└── 必要时读取物理基线解析冲突/Unknown

Complete（Actor System Turn）
├── 安装新的persistedVersion
├── 只确认V
├── 当前仍为V：清Dirty
└── 当前大于V：保留Dirty，继续排队
```

Redis 网络等待不在 UserMailbox 中，所以某个 Flush 变慢不会阻塞该 Actor 的后续业务 Turn。每个 Actor 同时最多一个 Flush in-flight。

## 6. 跨 Actor 场景的真实实现

Dirty 模式仍复用已存在的 Typed Participant Payload、目标 Actor Receipt、路由和 Deadline，但 `CreatedSourceOrderEnvelope` 只是本次进程内调用的不可变信封，不代表 Redis 已存在 SourceOrder。

| 场景 | 实验模式首个权威 | 后续调用 | 故障语义 |
|---|---|---|---|
| Buy / Sell / Task / Compendium | PlayerActor | 无 | 单 Actor Dirty |
| Plant | PlayerActor 扣种子 | 直接调用 FarmActor CommitPlant | 失败返回部分结果；无 Worker 恢复 |
| UnlockPlot | PlayerActor 扣金币 | 直接调用 FarmActor Unlock | 同上 |
| Harvest | FarmActor 结束 CropCycle | 直接调用 PlayerActor 发作物/经验 | 同上 |
| Care | FarmActor 结束 Incident | 直接调用 PlayerActor 发护理奖励 | 同上 |
| Theft | FarmActor 裁决份额与宠物截留 | 直接调用 Visitor/Owner PlayerActor；沿用直接协调入口 | 同上 |
| Pet Feed/Switch | PlayerActor 修改宠物状态 | 直接同步 GuardSnapshot 到 FarmActor | 同 operation 重试可重建调用，但无后台恢复 |
| MailClaim | MailEntry 仍按原物理路径 | Player 奖励进入 Dirty 状态 | 邮件记录与资产不再构成强原子边界 |

这里没有新增通用的常驻“BestEffort 队列服务”。Plant/Harvest/Care 等使用当前请求路径上的有界 Unary RPC；GuardSync 使用专门的直接 Dispatcher。调用发生在来源 Actor Turn 结束之后，目标 Actor 仍通过自己的 Mailbox 串行裁决。

正常路径等待必要 Participant ACK 后才返回成功。RPC 超时或失败时返回 `UNAVAILABLE/PARTIAL_BEST_EFFORT` 类结果；不会创建持久补偿任务，也不会由 farm-worker 重放。

## 7. farm-worker 的变化

`dirty_best_effort` 下关闭以下业务恢复家族：

- Plant；
- UnlockPlot；
- Harvest；
- CareReward；
- TheftVisitor / TheftGuard；
- GuardSync。

farm-worker 进程仍可运行，并继续承担与本实验无关的职责，例如：

- 好友机会到期唤醒；
- 邮件投递/维护；
- 可重建查询投影；
- 路由 Holder/readiness；
- 其他显式开启的非业务恢复循环。

因此准确表述是“Worker 不再恢复跨 Actor 经营结果”，而不是“删除 farm-worker”。

## 8. 路由、Drain 与崩溃

### 8.1 普通热路径

Dirty 模式不会每条 Actor 命令查询 Redis 验证当前 Shard Lease。它依赖 RouteControl/HolderControl 在 Drain 或 LeaseLost 时退休本地 Binding；最终 Flush 仍由 Redis writer fence 兜底。

这样才能真正移除热路径 Redis I/O，但代价必须明确：如果生命周期通知丢失，旧 Holder 可能先以内存结果回应，随后 Flush 被更高 `writer_epoch` 拒绝，该结果会丢失。这属于实验模式接受的不一致窗口。

### 8.2 正常 Shard Drain

```text
HolderControl停止该Shard新激活/新Turn
→ 等待已接纳Turn结束
→ 对该Shard全部Dirty Player/Farm执行FlushNow
→ Redis确认最新事实
→ 退休并移除本地Binding/Actor
→ RouteControl换代writer_epoch并交给新Holder
→ 新Holder按需从Redis懒加载
```

Facts Barrier 的预 Flush 在 Drain Mailbox 外等待，避免和已经 in-flight 的后台 Flush 互相等待形成死锁。

### 8.3 进程硬崩溃

硬崩溃没有机会执行最终 Flush：

```text
旧Pod退出
→ 新Holder取得更高writer_epoch
→ 从Redis最近成功Flush恢复Actor
→ 未Flush的业务变化与内存Receipt一起丢失
```

客户端用相同 operation_id 重试时，如果该 Receipt 也尚未 Flush，操作可能再次执行。这是 Dirty 模式的真实 RPO，不可用“有 operation_id”掩盖。

## 9. 存储语义

- Flush 继续写原 Authority Redis Actor Key，不引入第二套 Actor 真值。
- 每次 Flush 以最近成功持久态为 baseline，生成稀疏 Model Field upsert/delete；不是每条命令写 Redis。
- Farm OpportunityHeader/Facts 和最新 Schedule 随 Farm Snapshot 一起 Flush。
- 业务 SourceOrder、DurableOperationOutcome 和恢复 DueIndex 不进入 Dirty Flush。
- `writer_epoch`/claim 只在物理 Flush 时提供旧 Holder 写保护。
- 当前没有完整 TLog 重放链；若未来增加，只能先作为非阻塞审计流，除非另行设计可靠提交与回放协议。

机会 Schedule 最多会比内存农场状态晚一个 Flush 窗口进入 Redis，因此在线实时提醒延迟也可能增加；玩家 Join/打开好友列表时仍应依据最新 Actor 或权威读取路径重新裁决，不能把 Hint 当最终事实。

## 10. 已完成验证

详见 `docs/verification/actor-dirty-writeback-experiment-20260824.md`。本轮已完成：

- Scheduler 合并、容量、重试、in-flight remark、Close/FlushNow 测试；
- Player/Farm 多 Turn 不回读 Redis；
- Flush I/O 不占用 Mailbox；
- Frozen V Flush 期间写入 V+1 不丢失；
- 重启后恢复 `actor_memory_version`；
- Player/Farm Drain 强制 Flush；
- writer fence 失配拒绝旧 Flush；
- 全仓 `go test ./...`；
- 关键包 `go test -race`；
- 公司三主三从 Redis Cluster 的隔离 A/B 微基准与最终状态校验。

## 11. 尚未完成与禁止过度宣称

本分支尚未证明：

1. HTTP/WSS 登录、Join、经营全链路在 Dirty 模式下的容量；
2. 多 Pod 扩缩容时 Dirty Drain 的真实 K8s 故障注入；
3. 硬 Kill 在 Flush 窗口内的实际丢失分布；
4. Redis 长时间不可用时队列、内存和降级策略是否足够；
5. 跨 Actor 部分结果是否能被产品接受；
6. TLog 是否能够可靠采集，更不能声称可据此完整重放；
7. 机会 Schedule 延迟一个 Flush 窗口后的实际弹窗体验。

在上述项目完成前，这条分支应保持实验分支，不默认合入 Integration，更不能把 `dirty_best_effort` 设为生产默认值。

## 12. 下一轮建议

按顺序执行：

1. 在隔离 namespace 跑真实 H5 → Gate → Player/Farm → Redis 的经营闭环 A/B；
2. 注入 player-svr/farm-svr `SIGKILL`，测 0～10 秒不同脏龄下的真实 RPO；
3. 压测 200 人同场广播和热点 FarmActor，观察 Mailbox、Dirty 队列和 Flush 延迟；
4. 在 K8s 自动扩缩容中验证 Stop → Wait → Flush Barrier → Handoff；
5. 单独评审 Plant/Harvest/Theft 部分结果是否允许；
6. 如果产品不能接受资产回退或跨 Actor 丢步，保留 Dirty 仅用于低价值、可重算字段，不推广到经济状态。
