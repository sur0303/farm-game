# ScalePlan 滑动并发、Pod 扩缩容与滚动更新接手文档

- 日期：2026-08-17
- 分支：`codex/scaleplan-local-kind-r1`
- 基线：`origin/codex/integration-20260803@890c81fc04d0fbed068c7a1941400b0226b56195`
- 实现提交：`bea913a496801ad7450dd4dd765c0fd5c36cbb4c`
- 本地验收环境：`kind-a9-scaleplan / a9-scaleplan`

目标读者：接手 ScalePlan、RoutePlan、RouteControl、HolderControl 和 Kubernetes 托管发布的开发者或 Agent

## 1. 交付结论

本提交把原来的跨 Shard 固定批次改成了 UC 风格的控制拓扑：

```text
一次规划完整 Shard 迁移意图
→ 每个 Shard 持久化独立 RoutePlan
→ 最多 32 个独立槽位并行推进
→ 任意一个 Shard 完成就立即补入下一个
→ 每个 Shard 独立切 Authority、发布 Snapshot、完成验证
```

已经完成的真实本地验收：

- 1024 FARM Shard、2→1 Pod 的安全缩容通过；
- 128 FARM Shard、2→3→2 Pod 的托管滚动更新通过；
- 32 个槽位是滑动并发上限，不再是“必须 32 个全部完成”的固定波次；
- 旧 Pod 只有在零 Shard、无路由、精确 PodUID 仍匹配时才会删除；
- 每个 Shard 的 `writer_epoch`、`lease_epoch` 和 Authority Fence 保持单调且独立；
- 目标 Pod 的 PodUID、StatefulSet UID、ordinal、boot_id、mTLS 身份和 ServiceAccount TokenReview 没有删除；
- 全仓 `go test -count=1 ./...`、`go vet ./...` 和 `git diff --check` 通过。

必须准确描述的边界：

- 本轮 **没有** 完成 1024 Shard 的完整滚动更新；
- 128 Shard 滚动更新证明状态机和安全屏障完整，不是 1024 Shard 性能验收；
- 本轮迁移期间没有持续业务压测，不能据此声明客户端无损 SLO；
- `developer_026-qqfarm` 的 `SCALEPLAN_MODE` 仍为 `off`，本提交没有在 `uc-test-plus` 上发布或验收；
- 本地 bootstrap 使用 `--skip-worker-claims`，所以验收对象是 PLAYER/FARM Holder，FarmWorker 路由保持 `ASSIGNING` 不属于失败。

## 2. 为什么旧实现会反复卡住

### 2.1 固定 Authority Wave 冻结了跨 Shard 的全局 Revision

曾尝试使用：

```text
32 个 RoutePlan 执行窗口
→ 固定分成 4 组 Authority Wave，每组 8 个
```

第一组发布 Snapshot 后，全局 Revision 已经前进。后三组仍持有创建时冻结的旧全局 Revision，随后在源 Holder 校验中触发 `FENCE_CONFLICT`。这不是 Redis 容量不足，而是把全局分发游标误当成了不同 Shard 共享的正确性 Fence。

现在的规则是：

- ScalePlan child 不再生成 `OwnerWaveID/Size/Position`；
- 每个 child 使用自己的 Shard Authority Fence；
- 全局 Snapshot Revision 仅是分发游标；
- 另一个 Shard 发布 Snapshot，不能让当前 Shard 的 Authority Fence 失效；
- 同一个 Shard 的来源、Holder、boot、assignment/writer/lease epoch 变化仍然 Fail Closed。

### 2.2 RoutePlan CAS 热路径每次全表扫描

旧 Redis PlanStore 在每次现有 RoutePlan CAS 前都会：

```text
进入单进程 reconcileGate
→ 读取完整 RoutePlan index
→ 加载全部 Plan Hash
→ 重算 accounting/restart/index
→ 执行 Lua CAS
```

到第 5 个 32 并发窗口时已有约 160 条子记录。32 个 CAS 在单锁后排队，部分超过 `ROUTE_CONTROLLER_REDIS_OPERATION_TIMEOUT=10s`，因此在真正进入 Drain/HolderControl 前就以 `TIMEOUT` 暂停。

现在：

- 现有 Plan 更新走单记录 `loadKey + exact Lua CAS`；
- Lua 原子复核 revision、canonical envelope、meta/index/accounting/restart 和 TTL；
- 同一 foreground batch 冻结一个 `CreatedAtMs`，同 tail 的后续创建走 O(1) create；
- 只有共享 retention tail 必须增长时才退回有界全量证明；
- exact replay 先识别 `ALREADY_COMMITTED`，不会因原 expected revision 重放而误报冲突；
- generic recovery 与一次 foreground batch 互斥，但 batch 内不同 Shard 不互锁。

### 2.3 Holder 身份刷新把 32 并发退化为串行

旧代码会对同一 Route Domain 的每个 Shard重复执行：

```text
Kubernetes Pod 查询
→ HolderControl mTLS
→ TokenReview
→ VerifiedHolderView 更新
```

现在 `RefreshDomain` 按 Route Domain 合并并发请求。同一时刻 32 个 FARM child 只触发一次真实刷新，所有等待者仍各自校验自己的 Holder generation 和 Snapshot 条件。失败结果同样共享并 Fail Closed，PLAYER/FARM 互不合并。

### 2.4 Manager 锁内轮询目标 Revision

旧 `validateTargetForSnapshot` 会在 Manager 全局锁内循环等待 Holder 缓存追上 Snapshot。一个 Shard 最多可以占锁等待 discovery resync，其他 Shard 无法发布。

现在：

- Manager 回调只做一次读取，绝不睡眠或轮询；
- 等待收敛发生在 Manager 锁外；
- 每个 child 冻结自己 ASSIGNING Snapshot 的最小 Revision；
- 目标已经应用更晚的无关 Snapshot 时接受 `>= required revision`；
- 不再追逐不断前进的最新全局 Revision。

### 2.5 一个坏 Shard 污染整个进程 Readiness

旧 `AssignmentComposition.Healthy()` 会扫描全部 ACTIVE Route 的 Lease。一个无关 Shard Lease 过期，会让所有 Farm/Player Pod 的 `/readyz` 变成 503，Holder 心跳随后又停止，形成级联故障。

现在：

- Process readiness 只证明进程级共同依赖和完整 Snapshot 安装；
- 单个 Shard 的 ACTIVE/ASSIGNING、Lease 和 generation 在 `Resolve/ObserveShard/LeaseGuard/Authority CAS` 中逐 Shard Fail Closed；
- 一个坏 Shard 不再撤销同进程其他健康 Shard 的服务能力；
- 真正的 Pump、Resolver/Applier revision 或进程终态错误仍然使进程不健康。

### 2.6 缩容最后一个 Shard 的无解屏障

本轮 1024 Shard 缩容曾出现：512 个 child 全部 `SUCCEEDED`，但 Victim Pod 无法删除。

直接错误是：

```text
routecontrol: invalid holder state transition:
zero-shard return requires snapshot progression
```

真实顺序是：

```text
Holder 已在 Snapshot N 上报告 SERVING
→ 本地 membership 异步移除最后一个 Shard guard
→ Holder 在同一个 Snapshot N 上报告 READY_ZERO_SHARD
```

没有新的路由变化就不会有 N+1，因而旧条件永远无法满足。现在允许 `SERVING@N → READY_ZERO_SHARD@N`，但仍强制：

- Snapshot Revision 不得回退；
- exact PodUID/workload UID/ordinal/boot/endpoints；
- `ProcessReady=true`；
- `Draining=false`；
- `ServingShardCount=0`；
- ScalePlan 删除前再次证明没有任何 Route 放在该 Holder；
- 删除对象仍绑定精确旧 PodUID。

这个改动只移除了无解的全局 Revision 屏障，没有授予任何 Shard 写权。

## 3. 当前多 Shard 并发模型

### 3.1 ScalePlan 是批量意图，不是批量完成屏障

`MaxRoutePlansInflight=32` 的含义是“最多 32 个占用槽位的 child”，不是一个 32 成员 Wave。

```text
初始创建 Shard 1..32
Shard 7 完成
→ 立即创建 Shard 33
Shard 2 完成
→ 立即创建 Shard 34
慢 Shard 12 只占用自己的一个槽位
```

父计划为完整 intent set 捕获一次来源布局和 exact Fence，然后由 `reconcileSlidingChildren`：

1. 观察已有 child；
2. 把成功 child 立即精确 Verify；
3. 统计仍占用槽位的 child；
4. 按 manifest 顺序补到 32；
5. 用一次父 CAS 保存本轮观察/验证结果；
6. 所有 Shard 独立完成后进入最终验证。

单个 child 的 transient `CANCELED/TIMEOUT/DEPENDENCY_UNAVAILABLE` 只保留自己的槽位。恢复必须继续同一个 immutable transition，不生成任意新目标。

### 3.2 单 Shard 安全状态机

每个 Shard 独立执行：

```text
PENDING
→ DRAINING source
→ Stop/Wait/Facts barrier
→ Redis Authority CAS
→ publish ASSIGNING RouteSnapshot
→ target PrepareShardGeneration / ColdLoad
→ publish ACTIVE RouteSnapshot
→ Verify exact writer generation
→ SUCCEEDED
```

不同 Shard 可以并发到达不同阶段。每次 Snapshot 发布由 Manager 串行合并到当前 Snapshot，防止丢更新，但不形成跨 Shard 完成屏障。

### 3.3 CrashTakeover 也使用滑动槽

AutoFailover 不再“先串行 Submit 全批，再等待整批 Execute 完成”。现在每个 worker 在同一个槽位中完成 Submit+Execute，任意 worker 结束就补充候选。

目标 replacement generation 会先通过 Kubernetes + HolderControl 完成验证。若同名 StatefulSet Pod 已以新 boot 重建而旧 boot Lease 尚有效，控制器等待旧 Lease 到期，绝不覆盖旧 boot。这个约 30 秒窗口是写安全保护，不是迁移死锁。

## 4. 保留的安全边界

以下检查没有因为借鉴 UC 的并发拓扑而删除：

- Kubernetes PodUID、StatefulSet UID、ordinal；
- Holder `boot_id`；
- HolderControl mTLS DNS identity；
- ServiceAccount TokenReview；
- source Stop/Wait/Facts barrier；
- target exact `PrepareShardGeneration`；
- per-shard `assignment_revision/writer_epoch/lease_epoch`；
- Redis Authority compare-and-swap；
- RouteSnapshot shard tuple；
- 旧 Writer 的 Redis write fence；
- ScaleIn placement exclusion；
- 删除前零 Shard、无 Route 和 exact PodUID 复验。

UC 借鉴的是“批量下发、每 Shard 独立推进、一个完成立即上报”的并发拓扑。Farm Game 自己的 Redis writer fence 比 UC 示例更强，不能抄掉。

## 5. 本地部署的唯一推荐流程

本地操作必须使用技能：

```text
/data/home/developer_026/.codex/skills/deploy-a9-scaleplan-kind/SKILL.md
```

固定边界：

```text
context:   kind-a9-scaleplan
namespace: a9-scaleplan
```

所有 `kubectl` 命令必须显式写 `--context kind-a9-scaleplan -n a9-scaleplan`，因为终端默认 Context 可能是 `uc-test-plus`。

### 5.1 冷基线

不要手工拼装 bootstrap。使用：

```bash
bash /data/home/developer_026/.codex/skills/deploy-a9-scaleplan-kind/scripts/reset_baseline.sh \
  /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-20260816 \
  farm-l2-r2-registry:5000/farm/route-runtime@sha256:<DIGEST> \
  1024 \
  --confirm-reset-a9
```

脚本会：

1. 只停止隔离 A9 的应用；
2. 清理旧 safe-desired/rollout annotations；
3. 安装同一个不可变镜像；
4. 只清理隔离 A9 Redis；
5. 创建 2 Player + 2 Farm Holder；
6. 写 revision 1 topology-only Snapshot；
7. 由 Controller 的 sealed Initial Authority Batch 初始化全部 Authority；
8. Holder 全部 Ready 后再启动 Gate。

禁止停 Controller 后外部逐 Shard claim 1024 次；那会发布数千个全局 Revision 并让 Holder Lease 追不上。

### 5.2 扩容

只能设置安全期望副本 annotation，不直接 patch replicas：

```bash
kubectl --context kind-a9-scaleplan -n a9-scaleplan annotate sts farm-svr \
  scaleplan.farm/safe-desired-replicas=3 --overwrite
```

验收必须同时满足：

- 新 ordinal 先成为 verified `READY_ZERO_SHARD`；
- max inflight 不超过 32；
- child 完成后立即补位；
- parent `SUCCEEDED`；
- 全部 child `SUCCEEDED`；
- Shard 最终均衡且全部 ACTIVE；
- 所有 Pod Ready。

### 5.3 缩容

```bash
kubectl --context kind-a9-scaleplan -n a9-scaleplan annotate sts farm-svr \
  scaleplan.farm/safe-desired-replicas=1 --overwrite
```

禁止先删 Pod。正确顺序必须是：

```text
placement exclusion
→ 全部 Shard 迁出
→ victim READY_ZERO_SHARD
→ Snapshot/Authority 中无 victim
→ exact PodUID 删除
→ StatefulSet replicas 收敛
```

### 5.4 托管滚动更新

必须同时提供新不可变 Digest、expected replicas 和有界 deadline：

```json
{
  "schema_version": "statefulset-holder-rollout-intent-v1",
  "release_id": "<release-id>",
  "target_image_digest": "sha256:<digest>",
  "expected_replicas": 2,
  "deadline_at_ms": 0
}
```

先把 StatefulSet 主容器和 `dynamic-route-controller-barrier` 都设置为同一个新 Digest，再写 `scaleplan.farm/rollout-intent`。不要手工删除旧 Pod。

状态机应依次出现：

```text
ROLLOUT_START
→ ROLLOUT_EXPAND
→ ROLLOUT_WAIT_REPLACEMENT
→ ROLLOUT_DRAIN_HOLDER
→ ROLLOUT_DELETE_OLD_POD
→ ROLLOUT_WAIT_REPLACEMENT
→ ...
→ ROLLOUT_CONTRACT_SPARE
→ ROLLOUT_COMPLETE
```

## 6. 本轮真实验收证据

### 6.1 1024 Shard ScaleIn

镜像：

```text
farm-l2-r2-registry:5000/farm/route-runtime@
sha256:181907c057d1330c329dfdb784800b9f05871c81448f44c3e3203b61656ea535
```

初始分布：

```text
farm-svr-0  512
farm-svr-1  512
```

- 操作：`safe-desired-replicas=1`
- 开始：`1786935961437`
- 父计划：`sp-v2-cd53b5473994620a067e97724eb2211048a70709eec312e68391c614d3492b34`
- 父计划完成：`1786936850916`

结果：

```text
耗时                  889479 ms = 14m49.479s
RoutePlan             512/512 SUCCEEDED
精确验证              512/512
PAUSED/FAILED          0
最大活动槽位           32
最终 farm-svr-0       1024 Shard
最终 farm-svr-1       0 Shard，PodUID 已删除
```

过程中明确观察到一个 child 终态后补入下一个，活动窗口持续保持在 32 左右，不是等 32 个全部结束。

### 6.2 128 Shard Managed Rollout

源镜像是上一节 Digest。为了快速验证完整状态机，使用同一二进制内容但不同不可变镜像身份作为目标：

```text
sha256:10a103b0309e81084690d349e997cb0a0f825dbada6f8a89709cc61a6d576f08
```

- 操作开始：`1786937109332`
- 最终 `ROLLOUT_COMPLETE`：约 2026-08-17 03:29:35
- 端到端耗时：约 `4m26s`

旧 PodUID：

```text
farm-svr-0  9b919315-bf68-4fb5-a256-523ac0982bde
farm-svr-1  39464dc8-7f85-4794-aba3-797b8494377d
```

最终 PodUID：

```text
farm-svr-0  4d3d117d-c648-4961-b455-352e6a96d9c7
farm-svr-1  0a5f8554-77b0-4582-86cd-a49bdd050420
```

三个父计划：

| 操作 | Shard 移动 | 父计划 | 耗时 |
|---|---:|---|---:|
| Drain 第一个旧 Holder | 64 | `sp-v2-9e6ab70126046525b7b21d07a8b0765c22a09ce7b3c06f1b1fc964caeab4a742` | 37.566s |
| Drain 第二个旧 Holder | 64 | `sp-v2-97c653e3a8200f7d477cb68522153ecfebd7ea7c715f46b8a510f75405b92354` | 50.343s |
| ScaleIn 临时 Spare | 64 | `sp-v2-d7ad520a683e04c2824ccf16c1ca480186945e30a1002097b765bac25cef3f90` | 78.075s |

最终证明：

```text
RoutePlan                  192/192 SUCCEEDED
RouteSnapshot revision     641
FARM                       128/128 ACTIVE
farm-svr-0                 64，SERVING，Ready，restart=0
farm-svr-1                 64，SERVING，Ready，restart=0
临时 farm-svr-2            已安全删除
StatefulSet                replicas=2, ready=2, strategy=OnDelete
旧 Digest / 旧 PodUID      全部消失
rollout intent/state       全部清理
```

这里的 192 是迁移次数，不是 192 个唯一 Shard：128 个唯一 Shard 中，有 64 个在临时 Spare 收缩时再次迁移。

### 6.3 为什么没有做 1024 Shard Rollout

1024 Shard 的 2→1 ScaleIn 已耗时约 14m49s。相同 2 副本滚动更新通常包含三段约 512 Shard 的移动，因此按本轮速度粗略估计约 45 分钟，另加新 Pod 启动和旧 Lease 保护窗口。

这个数字只是估算。只有真实跑完 1024 Shard Rollout 并取得所有 durable terminal 证据后，才能写成验收结果。

## 7. 测试覆盖

提交前执行：

```bash
gofmt -w <本次修改的 Go 文件>
git diff --check
go test -count=1 ./...
go vet ./...
```

重点新增/强化的测试包括：

- 第 1 个 child 完成后第 33 个立即启动；
- 32 个并发 Holder Refresh 只触发一次底层刷新；
- ScalePlan child 不再带 OwnerWave；
- 无关 Shard 推进全局 Revision 不使当前 Shard `FENCE_CONFLICT`；
- Manager 锁内目标校验只读一次；
- transient child pause 不阻止其他 Shard；
- Redis existing CAS 不等待 full reconcile gate；
- 原 expected revision 的 CAS replay 幂等；
- CrashTakeover 滑动补位；
- foreground batch 与 generic recovery 互斥且不串行化 Shard；
- exact replacement boot Lease 到期前不可覆盖；
- 一个 expired Shard 不污染无关进程 readiness；
- `SERVING@N → READY_ZERO_SHARD@N` 合法且错误 flags 仍拒绝；
- ScaleIn child effect barrier 防止删除前 Holder 身份漂移；
- blocked rollout deadline 只能通过显式恢复命令延长。

## 8. 已知边界与后续工作

### 8.1 Redis 缺少独立的 durable per-shard active-owner index

当前 foreground batch 会在单 Controller 进程内一次读取 incomplete plans，并阻止 generic recovery 抢占；Authority CAS 仍能防止同一 Shard 双写。

但 Redis PlanStore 还没有单独的 `ActiveShards` 原子 owner 索引。若未来允许多个 Controller、manual MoveShard 和 ScalePlan adapter 真正并发创建不同 PlanID 的同一 Shard 计划，仍可能留下两个 durable Plan，其中一个最终由 Authority Fence 拒绝。

生产 HA 前建议增加同 hash-slot 的 per-shard active owner，并在 create、terminal、DurableStop Lua 中原子 claim/release。不要通过删除 Authority Fence 或放宽 Plan 校验解决。

### 8.2 Gate endpoint readiness 仍有全局耦合

本提交移除了“一个坏 Shard Lease 使整个进程不 Ready”的耦合，但 Gate 的 endpoint pool 仍可能因一个无关 Holder 连接不是 READY 而整体不健康。后续若继续 UC 化，应把连接可用性按目标 Shard/Holder 隔离，并补生产 FirstHop generation guard 的完整接线证明。

### 8.3 业务流量验收尚未完成

下一轮需要在扩容、缩容和滚动更新期间持续运行真实 Join/命令流量，记录：

- 成功率、P95/P99；
- Redirect/STALE_ROUTE/Retry 分类；
- Fresh Join 恢复；
- operation_id/Receipt 最终结果；
- Redis write fence rejection；
- Actor 冷加载耗时；
- Gate downlink queue-full；
- Holder mailbox reject。

## 9. 上集群前检查表

1. 从本提交构建所有 runtime 二进制，使用一个不可变 Digest；
2. 明确目标 Context/Namespace，禁止依赖默认 Context；
3. `SCALEPLAN_MODE=auto`，Informer/RBAC/HolderControl TLS/TokenReview 全部就绪；
4. clean baseline 时只清理专用 Redis，不能修补旧失败 Plan；
5. Holder 心跳连续超过 2 个 Lease TTL；
6. 操作前不存在旧 nonterminal ScalePlan/RoutePlan；
7. 一次只触发一个扩容、缩容或 rollout；
8. 失败时先保存父/子计划、Authority、Snapshot、Holder 和 Controller 日志；
9. 不反复 Resume、不覆盖 HolderRecord、不手改 writer_epoch；
10. 只有 durable terminal + Pod + Holder + Authority + Snapshot 全部一致才记 PASS。

## 10. 快速判断卡点

| 现象 | 优先检查 |
|---|---|
| 同时只有 8 个 child | 旧计划持久化的 parallelism 仍是 8 |
| 32 个 child，但每批刷新 32 次身份 | Holder discovery singleflight 未生效 |
| child revision=3 很快 TIMEOUT | Redis PlanStore CAS 仍走全表 reconcile gate |
| 第一组成功、后续 FENCE_CONFLICT | 仍在使用固定 OwnerWave 或全局 Snapshot exact fence |
| Authority 已提交、Snapshot 未完成 | 恢复应只补 pending projection，不重复 ColdLoad |
| 新同名 Pod 约 30 秒无计划 | 旧 boot Lease 保护窗，禁止直接覆盖 |
| 全部 Pod `/readyz` 503 | 检查是否仍把单 Shard Lease 聚合进进程 readiness |
| 所有 child 成功但 victim 不删除 | 检查 `READY_ZERO_SHARD` 和无路由证明，不要要求无关 Snapshot N+1 |
| rollout 一直 WAIT_REPLACEMENT | 对比 exact target Digest、PodUID、boot、HolderControl admission |

最终原则：**批量规划，逐 Shard 独立状态机，32 槽滑动补位；全局 Revision 不充当跨 Shard Fence，真正的写安全继续由 exact identity、per-shard Authority CAS 和 writer epoch 保证。**
