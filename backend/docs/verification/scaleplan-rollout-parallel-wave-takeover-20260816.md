# ScalePlan 自动扩缩容、滚动更新与并发 Shard 迁移接手文档

日期：2026-08-16  
适用代码：`codex/rolloutplan-parallel-wave-r1` 及其合入后的集成主线  
目标读者：接手 RouteControl、ScalePlan、Kubernetes 发布和容量验证的开发者/Agent

合入状态：已于 2026-08-16 快进合入远程 `codex/integration-20260803`；首次合入落点为 `8cd210daf198a554836fa96a02b1d25424b9a010`。`master` 未修改。

## 1. 一页结论

当前已经完成并验证的主链是：

```text
Kubernetes Informer发现Player/Farm Pod变化
→ HolderControl核验真实进程身份和承载状态
→ RouteControl维护Shard → Holder、assignment_revision和writer_epoch
→ ScalePlan生成最少迁移计划或托管Rollout计划
→ RoutePlan按有界并发波次迁移Shard
→ 新Holder取得更高writer_epoch
→ RouteSnapshot发布新路由
→ 旧Holder归零后才允许删除
```

可以确认：

- 新增 Pod 后可以自动接纳并按数量均衡 Shard；
- 删除、缩容或计划内 Drain 时，会先迁走 Shard，再删除 Pod；
- Player 与 Farm 是两个独立 Route Domain，只重排发生变化的域；
- 托管滚动更新可以执行 `N → N+1 → N`，把旧版本 Pod 的全部 Shard 定向迁入新版本 surge Pod，再逐个替换旧 Pod；
- Shard 迁移支持有界并发波次，不再只能串行处理；
- `writer_epoch`、Lease、PodUID、boot_id 和 Route Revision 共同防止新旧 Pod 同时写；
- 真实 kind、真实 Redis、真实 HolderControl 路径已完成一次完整 Player Rollout。

尚未完成：

- 根据 CPU、Mailbox、Presence、广播等指标自动识别热点 Shard；
- 热/冷 Shard 交换和 HPA 自动加 Pod；
- RouteController 中断超过一个 Lease TTL 后的完全无人工恢复；
- Rollout 期间持续业务 Bot 的成功率与客户端 Redirect/Retry/Rejoin 验收；
- 1024 个真实活跃 Actor 的生产 Drain 压测。

## 2. 代码与提交血缘

功能分支：

```text
codex/rolloutplan-parallel-wave-r1
```

关键提交：

```text
c6b93df  feat(scaleplan): add safe StatefulSet rollout orchestration
ab22757  feat(scaleplan): pin rollout drain to exact replacement
d6c17d3  fix(scaleplan): complete managed rollout recovery
ca0d962  feat-scaleplan-bounded-parallel-waves
8a4bcc5  optimize parallel rollout wave checkpoints
f29369c  document parallel wave rollout verification
```

本轮完整提交链建立在：

```text
codex/friend-opportunity-convergence-20260812
→ codex/source-order-scaleplan-a9-r93-convergence-r1
→ codex/rolloutplan-a1-safe-statefulset-r1
→ codex/rolloutplan-parallel-wave-r1
```

详细实测报告：

- `docs/verification/scaleplan-a9-deployment-and-managed-rollout-handoff-20260815.md`
- `docs/verification/scaleplan-managed-rollout-hardening-20260815.md`
- `docs/verification/scaleplan-parallel-wave-verification-20260816.md`

## 3. 组件职责

### 3.1 Kubernetes Informer

只负责发现基础设施事实：

```text
Pod创建/删除
Pod Ready变化
PodUID、Workload UID、ordinal、labels、PodIP
StatefulSet副本数和PodTemplate变化
```

它不决定 Shard 写权，也不直接执行迁移。

### 3.2 HolderControl

每个 `player-svr` / `farm-svr` Holder 暴露内部控制接口，用于确认：

```text
domain
holder_id
pod_uid / workload_uid / ordinal
boot_id
process_ready
serving_shard_count
observed_route_revision
control_endpoint
```

这不是第二套注册中心。Pod 发现来自 Kubernetes；HolderControl 只是对已发现 Pod 做游戏进程级身份与承载能力验证。

### 3.3 VerifiedHolderView

RouteController 内部保留的可重建运行视图，只记录通过 Kubernetes 身份和 HolderControl 握手的 Holder。它不能代替 Route Authority，也不能仅凭 K8s Ready 自动授予写权。

### 3.4 RouteControl

维护每个 Route Domain 的控制事实：

```text
VirtualShard → Holder
assignment_revision
writer_epoch
Lease / Transition / Recovery
RouteSnapshot
Redirect目标
```

PLAYER 与 FARM 各有独立 Shard 空间和 Assignment；只新增 `farm-svr` 时，不迁移 PLAYER Shard。

### 3.5 ScalePlan

负责“应该迁哪些 Shard、迁移顺序和计划状态”：

```text
SCALE_OUT
SCALE_IN
DRAIN_HOLDER
托管ROLLING_UPDATE
```

ScalePlan 是可恢复父计划；具体某个 Shard 的写权切换由子 RoutePlan 执行。

### 3.6 RoutePlan

负责一个 Shard 的安全 Handoff：

```text
冻结目标与来源身份
→ Drain旧Holder
→ 持久化强事实/停止接纳新Turn
→ 目标取得更高writer_epoch
→ 发布RouteSnapshot
→ 校验唯一Writer
→ 完成或Fail Closed
```

迟到的旧 Holder 写入携带旧 `writer_epoch`，会被 Redis Authority 拒绝。

## 4. 普通扩容如何分配 Shard

当前普通扩缩容使用“数量均衡 + 最少迁移”。假设 FARM 域有 1024 个 VirtualShard，从两个 Pod 扩到三个 Pod：

```text
扩容前：
farm-svr-0  512
farm-svr-1  512

新增farm-svr-2后：
farm-svr-0 → farm-svr-2 约170个
farm-svr-1 → farm-svr-2 约171个

稳定后：
farm-svr-0  342
farm-svr-1  341
farm-svr-2  341
```

不会重新洗牌全部 1024 个 Shard，只迁移新 Pod 达到均衡所需的约 341 个。除不尽时允许各 Pod 相差 1 个 Shard。

该算法只按 Shard 数量均衡，不感知热点。某个 Shard 比其他 Shard 热很多时，当前控制器仍可能得到“数量均衡但负载不均”的布局。

## 5. 托管滚动更新如何工作

滚动更新与普通扩容不同。目标不是长期保留第三个 Pod，而是使用 surge Pod 作为安全中转：

```text
副本N
→ 显式RolloutIntent绑定目标不可变镜像Digest
→ 临时扩容N+1
→ 新surge Pod经K8s Ready + HolderControl验证
→ 选择一个旧版本Victim
→ DRAIN_HOLDER把Victim全部Shard定向迁到surge Pod
→ 确认Victim为零Shard
→ 按精确旧PodUID删除Victim
→ StatefulSet以相同ordinal创建新版本Pod
→ 重复替换剩余旧Pod
→ 回收临时surge Pod
→ 恢复副本N并清理Intent/State
```

关键安全屏障：

- 必须使用不可变镜像 Digest，不接受浮动 Tag 作为发布身份；
- 必须有显式 RolloutIntent，仅观察到 PodTemplate 变化不会自动发布；
- surge Pod 必须通过 HolderControl，不只看 K8s Ready；
- Victim 全部 Shard 归零后才允许删除；
- 删除绑定精确 PodUID，避免误删同 ordinal 的新 Pod；
- 每个 Shard 的 `writer_epoch` 只前进不回退；
- 未完成的父/子计划持久化，控制器重启后恢复同一计划，不重新随意选目标。

## 6. 并发迁移波次

### 6.1 为什么需要波次

1024 Shard 若完全串行，迁移耗时会随 Shard 数线性增长；若一次并发 1024 个，又会瞬间压垮 Redis、Holder Drain 和网络。因此采用有界波次：

```text
1024 Shard，parallelism=8
→ 128个波次
→ 每个波次最多8个RoutePlan并行
→ 当前波次通过精确验证后再进入下一波
```

### 6.2 父计划批量检查点优化

一个波次内所有子 RoutePlan 都精确 `SUCCEEDED` 时：

```text
observeCompletedParallelWave
→ 一次父CAS保存整波EnsureObserved、ChildPhase和阶段推进

verifyCompletedParallelWave
→ 一次父CAS完成整波精确验证并进入下一波
```

任何子计划缺失、非终态、结果非精确或验证失败，都回退到原逐条 Fail Closed 路径，不会为性能放宽正确性。

### 6.3 1024 Shard 控制面结果

```text
并发1：  71.239s，14.37 Shard/s，1024波
并发8：  22.790s，44.93 Shard/s，128波
并发16： 19.351s，52.92 Shard/s，64波
并发32： 17.774s，57.61 Shard/s，32波
```

建议默认 `parallelism=8`。16/32 只在真实 Redis、Holder Drain 和网络容量确认后开放。该数据证明的是控制面编排与父计划 CAS 已收敛，不代表 1024 个真实活跃 Actor 的业务迁移耗时。

## 7. 请求在迁移期间如何处理

Shard 进入 Drain 后：

```text
尚未进入旧Holder Mailbox的新请求
→ 返回Redirect/Retryable结果
→ 调用方刷新RouteSnapshot并使用原operation_id重试

已被ADMITTED且正在执行的Actor Turn
→ 在有界Drain期限内完成
→ 可靠提交后才能释放旧写权

排队但尚未开始且超过Drain预算的请求
→ 不静默丢弃
→ 返回可重试结果或由上层通过operation_id恢复最终结果
```

Actor 内存不要求整体搬迁：

```text
旧Holder停止写并持久化强事实
→ 新Holder取得更高writer_epoch
→ 请求到达时按需懒加载Actor
→ 在线订阅必要时Fresh Join
```

因此热迁移 Actor Snapshot 是后续体验优化，不是当前正确性前提。

## 8. 真实验证环境与结果

隔离边界：

```text
Linux user: developer_026
K8s context: kind-a9-scaleplan
namespace: a9-scaleplan
```

未触碰 `developer_026-qqfarm` 和其他 Namespace。

本轮镜像：

```text
farm-l2-r2-registry:5000/farm/route-runtime@
sha256:073a44bc29b8b158e512c57d8e6c53c8e926dddfec7c05c250f7475c3adfb641
```

真实 Player Rollout：

```text
2 → 3
→ player-svr-2 Ready并通过HolderControl
→ player-svr-1全部Shard迁入player-svr-2并删除旧UID
→ 新版player-svr-1建立
→ player-svr-0全部Shard迁入player-svr-2并删除旧UID
→ 新版player-svr-0建立
→ 清空并回收player-svr-2
→ 3 → 2
```

总耗时约 154 秒。最终两个 Player Pod 都是新 Digest，临时 surge Pod 已移除，PLAYER/FARM 所有 Shard 均保持唯一 Writer，Gate、Redis、Holder 和 RouteController 均 Ready。

## 9. 回归与复验命令

普通全量回归：

```text
go test ./... -count=1
```

并发与共享状态回归：

```text
go test -race ./internal/scaleplan -count=1
```

控制面定向回归：

```text
go test ./internal/scaleplan \
        ./internal/routeplan \
        ./cmd/route-controller \
        ./internal/platform/kubernetes/scaleplan -count=1
```

1024 Shard 压测默认不运行，显式开启：

```text
SCALEPLAN_RUN_1024_PRESSURE=1 \
go test ./internal/scaleplan \
  -run TestScalePlan1024ShardParallelProfilesPressure -count=1 -v
```

复验真实 K8s 前必须确认：

```text
当前context == kind-a9-scaleplan
当前namespace == a9-scaleplan
没有触碰developer_026-qqfarm
目标镜像使用不可变Digest
RolloutIntent、expected_replicas和deadline明确
```

不得通过清空生产 Redis、手工改 `writer_epoch` 或直接删除持有 Shard 的 Pod 来制造“成功”。

## 10. 监控与验收指标

控制面至少记录：

```text
Plan总耗时 / 当前Phase / Pause原因
每波Shard数 / 波次耗时 / 重试次数
RoutePlan成功、失败、非精确结果
HolderControl验证失败原因
RouteSnapshot revision推进
writer_epoch冲突与旧Writer拒绝
Drain中Mailbox长度和最长等待
Redirect / Retry / Rejoin数量
Victim归零耗时
Rollout期间业务请求成功率和重复operation结果
```

默认并发度不能只根据控制面基准提高，应同时观察 Redis 延迟、Holder CPU、Mailbox、网络和业务 P99。

## 11. 当前最重要的遗留缺口

### 11.1 RouteController 长中断恢复

故障注入曾暴露：

```text
旧Transition未发布或冷加载Deadline与续约Deadline不一致
+ Holder durable heartbeat过期
+ AutoFailover控制令牌过期
→ Controller持续Fail Closed
→ 旧Holder无法重新进入可承载状态
```

下一任务应覆盖：

- AutoFailover 每次重试重新签发/刷新控制令牌；
- Heartbeat 过期但 K8s 身份仍合法的 Holder，允许以新 `boot_id` 回到 WARMING 并重新验证；
- Transition 恢复允许识别合法 Lease Deadline 续约，避免永久 stale conflict；
- RouteController 中断超过一个 Lease TTL 后，无需清库即可收敛。

在此闭合前，不能宣称控制面长时间故障后可完全自动恢复。

### 11.2 Rollout 业务连续性

需要在 Rollout 同时持续运行真实 Bot：

```text
Login / Join
Plant / Harvest
Care / Theft
好友机会查询
断线重连和Fresh Join
```

记录成功率、最长停顿、Redirect、Retry、Rejoin、UNKNOWN 和重复结果。控制面成功不等于业务体验已经验收。

### 11.3 热点 Shard 与自动加 Pod

当前不会因 Pod CPU 高自动扩容，也不会按热点迁移。推荐后续分阶段：

```text
H0 Shadow：采集每Shard Turn、Mailbox、Presence、Snapshot和广播负载
H1 建议模式：输出热Shard与目标Holder，不执行
H2 FARM域热/冷Shard成对交换，复用现有RoutePlan
H3 与HPA/容量控制联动：无空闲目标时先扩容，再迁热点
```

单个热门 FarmActor 即使迁到空闲 Pod，也仍受单 Actor 串行上限约束；需要另做 Snapshot 缓存、广播复用和 SubscriptionHub 优化。

## 12. 推荐接手顺序

1. 先阅读本文件和三份验证报告，运行全量测试与 race 测试；
2. 在隔离 kind 环境复验一次普通 2→3 ScaleOut，确认 1024 Shard 最少迁移布局；
3. 同时运行真实业务 Bot，复验一次 Player 托管 Rollout；
4. 修复 RouteController 长中断恢复并增加超过一个 Lease TTL 的自动化测试；
5. 建立 1024 真实 Shard/Actor Drain 压测，校准默认并发度 8；
6. 最后才进入热点采样 Shadow 和 HPA 联动，不要把热点调度与恢复缺口混在同一提交。

## 13. 不得破坏的硬边界

- Kubernetes 负责 Pod 发现，RouteControl 负责 Shard 写权，二者不能混为一谈；
- K8s Ready 不是写权授权；
- HolderControl 是进程验证，不是重新建设注册中心；
- ScalePlan 决定迁移集合，RoutePlan 执行单 Shard 安全切换；
- PLAYER/FARM Route Domain 独立；
- 旧 Pod 未归零不得删除；
- `writer_epoch` 只增不减；
- 非精确子计划结果必须 Fail Closed；
- `operation_id` 重试保持不变，不能把超时直接当失败重做业务；
- 不得用清 Redis 掩盖恢复问题；
- 不得直接在正在压测的 Namespace 做破坏性 Rollout 实验。

## 14. 合入说明

本功能分支相对 `codex/integration-20260803` 为纯前进历史，合入时可以使用快进。旧的 Integration worktree 若存在未提交文件，不应强行重置或覆盖；应在独立 worktree 完成回归并将远程集成分支安全快进。
