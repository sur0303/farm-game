# 86 · K8s Pod 自动扩缩容与 Shard Rebalance 实施方案

状态：`v1.0-a6-implemented-candidate`
日期：2026-08-14
分支：`codex/scaleplan-auto-rebalance-a6-20260814`
约束：不修改 26～37 业务合同；不合入 Integration；不操作正在压测的集群。

## 1. 目标

在 PLAYER 与 FARM 两个独立路由域中实现：

1. K8s 新增一个有状态 Pod 后，系统自动发现、验证并迁移最少数量的 Shard；
2. 请求缩容时，先迁走最高 ordinal Pod 的全部 Shard，再删除 Pod；
3. 迁移过程中只有一个 Holder 拥有写权，失败后可依据持久 Plan 恢复；
4. 不建立第二套 Pod 注册中心，K8s 是 Pod 存在性来源；
5. P0 使用冷 Actor 迁移，不搬运 Actor 内存。

## 2. 责任边界

```text
Kubernetes Informer
├── Pod 创建/删除/Ready
├── PodUID / StatefulSet UID / ordinal
└── 仅发现和唤醒
        ↓
HolderControl.GetRuntimeIdentity
├── domain / boot_id
├── process_ready
├── serving_shard_count
└── HolderControl 身份认证与 TokenReview
        ↓
VerifiedHolderView（可重建内存视图）
        ↓
AutoRebalanceController（PLAYER / FARM 各一套）
├── 读取完整 Assignment Snapshot
├── 计算最少迁移清单
├── 提交持久 ScalePlan
└── 周期恢复同一个 PlanID
        ↓
ScalePlan / RoutePlan / RouteControl
├── PlacementExclusion
├── Drain / Handoff
├── assignment_revision
├── Lease / writer_epoch Fence
└── StatefulSet scale
```

Informer 不能直接改路由；K8s Ready 也不能直接授予 Shard 写权。写权仍由 RouteControl 和 Authority Store Fence 决定。

## 3. 自动扩容

### 3.1 外部直接新增一个 Pod

```text
StatefulSet replicas: N → N+1
→ Informer发现新Pod
→ 等K8s Ready
→ HolderControl精确验证PodUID/BootID/domain/process_ready
→ 新Pod进入READY_ZERO_SHARD
→ Controller识别“已发生但尚未迁Shard”的+1
→ 创建Adopted SCALE_OUT Plan
→ Executor记录K8s扩容为ADOPTED_COMMITTED
→ 自动迁移最少Shard
```

控制器不会重复修改 replicas。P0 只支持一次直接增加一个 Pod；多级目标应使用安全 desired annotation，让控制器逐级执行 +1。

### 3.2 控制器拥有的计划扩容

设置：

```text
scaleplan.farm/safe-desired-replicas = M
```

当 `M > 当前 replicas` 时，控制器每次只执行 +1：先持久化 Plan，再修改 StatefulSet，等待新 Holder 就绪后迁移 Shard，直到达到 M。

该 annotation 是一次性控制意图，不是永久 DesiredState。达到最终 M 后，控制器使用 UID、ResourceVersion 和 annotation 原值作为条件自动删除它；如果运维在此期间提交了新目标，旧控制器的条件删除会失败，不能误删新请求。这样完成一次安全计划后，后续直接 `replicas +1` 仍可走 Adopted Scale-out，不会被旧 annotation 错判成缩容。

### 3.3 最少迁移策略

P0 只按 Shard 数均衡，不猜测热点：

```text
12 Shards，2 Pod → 3 Pod
目标分布：4 / 4 / 4
只迁移4个Shard
```

从当前 Shard 数最多的 Holder 取出 Shard；同数时按 ordinal、holder_id 和 shard_id 稳定排序，保证相同拓扑必然生成相同计划和幂等键。

首版 Scale-out 只允许当前 Holder 的 Shard 数已经相差不超过 1；在这个前提下，只把理论最少数量迁到新 Holder 就能得到全局均衡。如果此前已经做过导致数量明显倾斜的热点迁移，自动扩容会 fail-closed，交给后续独立的热点/全局 Rebalance 计划处理，不能用一个“只指向新 Pod”的清单伪称已经均衡。

## 4. 自动缩容

直接执行 `spec.replicas: N → N-1` 不安全，因为 StatefulSet 可能先删除最高 ordinal Pod。安全入口仍使用 desired annotation。

```text
desired = N-1
→ 选择最高ordinal为Victim
→ PlacementExclusion禁止接收新Shard
→ 完整捕获Victim当前Shard集合
→ 将全部Shard分配给保留Pod
→ 每次迁移一个Shard并验证writer_epoch/Lease
→ Victim成为READY_ZERO_SHARD
→ 再把StatefulSet replicas改为N-1
→ 验证Victim Pod消失
```

若 Victim 原本就是零 Shard，也必须经过持久排除、完整捕获、零 Shard 复核和 StatefulSet CAS；只是 Move Manifest 为空，不会绕过状态机。

## 5. Actor 与客户端表现

### 5.1 PlayerActor

- 完成当前 Turn；
- 强事实已经持久化；
- 新 Holder 取得更高 writer_epoch 后，下一条玩家命令懒加载 PlayerActor；
- 客户端通常只感知一次幂等重试或短暂 Redirect，不需要重新 Join Farm。

### 5.2 FarmActor

- 完成当前 Turn并冻结旧写权；
- Presence、JoiningBuffer、连接队列不迁移；
- 受影响订阅走 `REJOIN_REQUIRED` / fresh Join；
- 新 Holder 从权威存储懒加载 FarmActor，重新建立 Snapshot/Catch-up/Ready Lane。

P0 不做 Actor Runtime Snapshot 热迁移。热点农场热迁移是后续优化，不是扩缩容正确性的前提。

## 6. 活动计划恢复

自动控制器不能只重新观察拓扑后重新规划。第一批 Shard 已迁入目标 Pod 时，拓扑可能已经不再满足“新 Pod 为零 Shard”，但父 ScalePlan 仍需继续验证或迁移下一批。

实现规则：

1. route-controller 启动时先执行 ScalePlan Recover；
2. 再读取剩余非终态 Plan；
3. 按 Workload 把同一个 PlanID 交还 PLAYER/FARM Controller；
4. Controller 每次先推进 active PlanID，再考虑创建新 Plan；
5. Submit 返回 `WORKLOAD_BUSY` 时，采用返回的既有 PlanID，不创建第二条迁移；
6. PLAYER/FARM 共用一个本地 reconcile lock，与 realm 级 mutation lease 对齐。

父 ScalePlan 创建的 child RoutePlan 不能只停留在 Redis `PENDING`。`EnsureMove` 成功后只发送一个进程内合并唤醒，不在父计划持有控制锁时内联执行；后台 RoutePlan Recovery Owner 等父操作释放 mutation lease 后执行 child。即使唤醒丢失，周期恢复仍会扫描非终态 RoutePlan。父 ScalePlan 随后使用同一 child ID 观察、验证并继续推进。

连续目标例如 `2 → 4` 仍严格拆成两个 `+1`：如果运维已经先把 StatefulSet 加到 3，Controller 必须先 Adopt ordinal 2 并完成均衡，再规划 ordinal 3，不能跳过现存零 Shard Pod直接创建下一档。

零 Shard Victim 在 Placement Capture Revalidate 漂移后，可以在排除状态、Holder 身份、Lease 和“仍为零 Shard”全部重新验证后回到 `WAITING_ZERO_SHARD`；非零 Victim 才回到 `MOVING_WAVE`。

## 7. 配置合同

```text
SCALEPLAN_MODE=auto
HOLDER_DISCOVERY_MODE=k8s        # k8s-canary也可用于渐进启用
SCALEPLAN_AUTO_RECONCILE_INTERVAL=5s
SCALEPLAN_SAFE_DESIRED_ANNOTATION=scaleplan.farm/safe-desired-replicas
```

`off` 不授予 RBAC；`manual` 保留显式 API；`auto` 使用同一最小 RBAC，并增加自动编排。

PLAYER/FARM StatefulSet 可分别设置 annotation，因此只增加 farm-svr Pod 时不会触发 player-svr 迁移。

## 8. 失败语义

| 情况 | 处理 |
|---|---|
| Pod K8s Ready，但 HolderControl 未就绪 | 不分配 Shard，周期重试 |
| 规划后 Assignment 发生变化 | Revalidate 失败，不提交旧计划 |
| RoutePlan 执行中断 | 继续同一 PlanID / child ID |
| child RoutePlan 创建后进程内唤醒丢失 | 周期 RoutePlan Recovery 扫描并执行同一 child ID |
| Controller 重启 | 从 Redis 非终态 Plan 恢复 |
| 旧 Holder 恢复并尝试写 | 旧 writer_epoch 被权威存储拒绝 |
| 直接外部缩容已经删 Pod | 报 `UNSAFE_EXTERNAL_SCALE_IN`，不伪装成成功迁移 |
| `spec.replicas` 已等于 safe desired，但仍有越界 Shard | 不清理 annotation；先报不安全外部缩容 |
| Plan PAUSED/BLOCKED | 保留证据并报警，不创建并行 Plan |

## 9. 验收分层

### A6-Unit

- +1 最少迁移和确定性；
- 外部 +1 Adopt，不重复 K8s Scale；
- -1 只迁 Victim 全部 Shard；
- 零 Shard Victim 安全缩容；
- 零 Shard Capture 漂移后只读重验并恢复；
- 拓扑漂移提交前拒绝；
- 活动 PlanID 在拓扑改变后仍优先恢复；
- child RoutePlan 的唤醒执行与周期恢复兜底；
- 连续扩容先 Adopt 已存在的新 Pod，再规划下一档；
- safe desired 仅在 Placement 安全扫描后确认并删除；
- PLAYER/FARM 自动配置和 K8s Render 合同。

### A6-Isolated-Kind（后续单独执行）

- 独立 namespace 与 Redis；
- FARM 2→3 自动发现与均衡；
- PLAYER 不受影响；
- FARM 3→2 先 Drain 后删 Pod；
- 中途重启 route-controller 后继续相同 PlanID；
- Farm 客户端能够 fresh Join，Player 命令幂等恢复。

本轮只实现和验证代码、纯 Render 与进程内状态机，不对当前压测集群执行 apply。

## 10. A7 宕机恢复扩展

86 号解决“已知健康 Pod 拓扑变化时如何自动均衡与安全缩容”；意外 Pod/进程消失后的自动写权接管由 [87 · K8s Holder 宕机自动恢复合同](87-k8s-holder-failure-auto-recovery.md) 继续冻结。两条控制链复用 RoutePlan、Authority Fence 和同一控制锁，但触发条件不同：

- AutoRebalance：健康拓扑发生增减，目标是均衡或安全删除；
- AutoFailover：RouteSnapshot 指向的精确 Holder Boot 不再健康，目标是恢复可用性。
