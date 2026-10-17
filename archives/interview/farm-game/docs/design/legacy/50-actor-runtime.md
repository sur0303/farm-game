# 50 · Actor Runtime

实现基线：`internal/logic/actor/runtime/executor.go`。当前执行器只证明 keyed serial executor；目标业务边界为 `PlayerActor` 与 `FarmActor`，见 [16-player-farm-actor-split](./16-player-farm-actor-split.md)。

---

## 1. ActorKey

```go
type ActorKind uint8

const (
    ActorKindPlayer ActorKind = 1
    ActorKindFarm   ActorKind = 2
)

type ActorKey struct {
    Kind ActorKind
    ID   uint64
}
```

`PLAYER:10001` 与 `FARM:10001` 必须落入不同邮箱，即使 P0 的 farm_id 与 owner_player_id 数值相同。

跨节点路由使用 routing v2：

```text
xxhash64(actor_kind_byte || uint64_be(entity_id)) % 4096
```

PlayerRegion/FarmRegion 分别拥有 4096 个 ShardAssignment；完整黄金向量与迁移合同见 [17](./17-uc-routing-migration-adaptation.md)。

## 2. 并发语义

- 同 ActorKey 的任务由单邮箱串行执行，领域代码不需要实体内互斥锁；
- 不同 ActorKey 并行，PlayerActor 与自己的 FarmActor 也可以并行；
- 邮箱有界，满时返回过载，不允许无限积压；
- 一个 Actor 的慢 I/O 不阻塞其他 key；
- Actor turn 只完成本 Actor 的读取、校验和内存状态转换；Farm/P-DURABLE turn 构造并提交条件写，Player P-ASYNC turn 标记 Dirty 并把保存交给 turn 外回写器；
- 跨 Actor RPC 由 turn 外的 Coordinator/Dispatcher/Worker 发起，禁止 Actor 持有邮箱等待另一个 Actor。

## 3. Runtime 与上层职责

| 语义 | Runtime | Player/Farm 上层 |
|---|---|---|
| 同 key 串行、异 key 并行 | 是 | |
| 有界邮箱与过载 | 是 | 映射 BUSY/重试 |
| ActorKey → virtual shard | | ActorRouteAdapter / routing v2 |
| 激活 singleflight | | ActorRegistry |
| 加载、CAS、增量保存 | | PlayerStore/FarmStore |
| Dirty version/去重回写/ForceFlush | 基础队列可复用 | PlayerActor PersistenceRuntime |
| fencing | | Shard Lease + player_epoch/farm_epoch（writer_epoch） |
| passivation | | 两类 Actor 独立策略 |
| 跨 Actor 最终一致 | 否 | Intent/Settlement + Outbox + Receipt |
| Join/Presence | 否 | FarmActor activation runtime |

当前 executor 不是生产级完整 Actor Runtime。多 Logic 前还需 ShardLease/shard_revision、Redirect、DB Reload Handoff 与 writer_epoch 认领；S2 不提前实现这些控制面能力。

## 4. 激活

### PlayerActor

```text
请求到达
→ ActorRouteAdapter(PLAYER, player_id)
→ PlayerActorRegistry singleflight
→ PlayerStore.Get(player_id)
→ 建立 mailbox + PlayerCore 热状态
→ 初始化 version=saved_version、modified_version 与空 DirtySet
```

### FarmActor

```text
请求到达
→ ActorRouteAdapter(FARM, farm_id)
→ FarmActorRegistry singleflight
→ FarmStore.Get(farm_id)
→ 建立 mailbox + FarmCore 热状态 + 空 PresenceRuntime
```

两类 Registry 可以共享通用泛型基础设施，但指标、内存预算、邮箱阈值和钝化策略必须分开统计。

## 5. 钝化

通用条件：

- 邮箱为空；
- 无执行中的本地 turn；
- 所有未完成 Intent/Settlement/Outbox 已持久化；
- 没有 Coordinator 等待内存中的唯一结果。

额外条件：

- FarmActor 无活跃 Presence/Joining；存在长时间作物不要求常驻，成熟由 `mature_at` 派生；
- PlayerActor 没有必须留在内存的会话事实；先停止新资产写并 ForceFlush 所有 DirtyModel，成功后才可钝化；未完成 PlantIntent 已持久化即可钝化。
- FarmActor 热状态必须与已确认 Store 提交一致。

PlayerActor 异步保存：

```text
turn结束发现 Dirty
→ UniqueWritebackQueue(player_id)
→ Actor turn 内 OnSave 捕获 immutable bytes + captured_version
→ turn 外执行 Tcaplus HMSet/FieldUpdate
→ 回到 Actor mailbox 调 OnSaveSucc(captured_version)
→ 只推进 saved_version；若 version 更大则再次入队
```

同一 Player 最多一个 in-flight save。保存失败不推进 `saved_version`；脏龄、version gap 或队列超过上限时拒绝新的 P-ASYNC 资产写。该进程内队列无法覆盖硬崩溃 RPO。

## 6. Presence 过期

Presence 只属于 FarmActor activation：

- FarmActor 每次处理命令时惰性清理过期 lease；
- farm-gate Disconnect/Leave 发送非可靠清理提示；
- Runtime 可用低频本地租约轮辅助；
- FarmActor 钝化后 Presence 整体消失，客户端重新 Join；
- ExpirePresence 对不存在成员返回 NO_CHANGE，不进入 farm-worker 可靠流程。

## 7. 跨 Actor 调用约束

错误：

```text
PlayerActor turn
→ 同步等待 FarmActor RPC
→ FarmActor 又等待 PlayerActor
```

这会造成邮箱长时间占用，甚至循环等待。

正确：

```text
PlayerActor turn：持久化 Intent/Outbox，结束
→ Coordinator/Worker 调 FarmActor
→ FarmActor turn：持久化 Receipt/Outbox，结束
→ Coordinator/Worker 再调 PlayerActor
```

正常链路可以在一个客户端请求内快速串联多个短 turn，但可靠性来源始终是持久状态，不是调用栈。

## 8. 指标

必须按 actor_kind 区分：

- active_actor_count；
- activation/passivation rate；
- mailbox depth / oldest wait / rejection；
- per-turn P50/P95/P99；
- store load/CAS latency；
- Player writeback queue depth/oldest dirty age/version gap/save retry/force flush latency；
- actor hot-state bytes；
- Plant/Harvest workflow age 与 retry；
- 热门 Farm 对 Player 命令延迟的隔离效果。

## 9. 当前实现边界与验收

当前 keyed executor 应继续通过：

```text
go test -race ./internal/logic/actor/runtime/
```

S2 还需新增：

- ActorKey 包含 kind 的串行/隔离测试；
- PlayerActorRegistry/FarmActorRegistry 分离激活测试；
- 同数值 player_id/farm_id 不共享邮箱测试；
- 两类 Store 加载失败与 CAS 冲突测试；
- Player version=10 保存期间产生 version=11，旧完成回调不能清除 v11 Dirty；
- 保存失败重入队、同 Player 单 in-flight、正常 Stop/钝化 ForceFlush；
- Stop 时拒绝新提交、等待已接收 turn；
- mailbox watermark、激活限速和加载风暴保护在容量压测前补齐。

## 10. 多 Logic 与迁移边界

多 Logic 时 Player/Farm 使用独立 Region。推荐数据面是批准版本的 xrpc/proxyless ShardedService；不可用时由 LogicIngress Forward，业务 Actor 不感知差异。

首版迁移只使用 DB Reload：

```text
Shard HANDOFF
→ 停止新激活/新 turn
→ 完成已接收 turn
→ Player ForceFlush DirtyModel；Farm/P-DURABLE 步骤确认已提交
→ Coordinator 切 holder、提升 shard_revision
→ 目标从 Tcaplus Load
→ CAS 认领更高 player_epoch/farm_epoch
→ ACTIVE
```

Farm/P-DURABLE 条件写与 Player P-ASYNC 回写都必须同时满足：

- 当前 Shard Lease 仍有效；
- Store 的 RecordVersion 匹配；
- 当前绑定 writer_epoch 匹配。

任一不满足都不得继续写 Store。P-ASYNC 普通命令只有在 Lease 明确有效时才允许修改内存；Lease 不确定时 fail-closed。Presence/JoiningBuffer 不迁移，客户端重新 Join。P2P 热快照只用于未来降低恢复延迟，不是正确性前提。
