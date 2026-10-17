# ScalePlan 托管滚动更新：旧 Pod 全量定向接管实现交接

日期：2026-08-15  
分支：`codex/rolloutplan-a1-safe-statefulset-r1`  
状态：代码与聚焦回归完成；尚未宣称真实 Kubernetes 全链路验收完成  
边界：不合入 `Integration`，不触碰 `developer_026-qqfarm`

## 1. 本轮实现的准确语义

托管滚动更新的单轮替换现在固定为：

```text
StatefulSet N → N+1
→ K8s 发现新版本 Pod
→ HolderControl 验证该进程为 READY_ZERO_SHARD
→ 选择一个旧版本 Victim Pod
→ 持久绑定一个精确 Replacement Generation
→ Victim 的全部 Shard 只迁到该 Replacement
→ Victim 清空后通过删除屏障
→ 按精确 PodUID 删除 Victim
→ 同 ordinal 新版本 Pod Ready
→ 进入下一轮
```

这里的“一对一”指：

```text
一个旧 Pod 的全部 Shard
→ 同一台指定的新 Pod
```

并不是把整个集群所有旧 Pod 的 Shard 同时塞入一台新 Pod。每替换一台旧 Pod，Kubernetes 会补出下一台新版本 Pod，再进入下一轮。

## 2. 为什么原实现不能满足该语义

原 `DRAIN_HOLDER` 是通用缩容能力：它会将 Victim 的 Shard 按当前数量分散到多个低负载 Holder。这个策略适合普通缩容，但不等价于版本替换。

托管发布需要的是：

```text
旧 Pod A：Shard 1、3、8、11
指定新 Pod B

结果必须是：
1、3、8、11 → 全部到 B
```

不能出现：

```text
Shard 1、3 → B
Shard 8    → C
Shard 11   → D
```

否则旧 Pod 与新 Pod 之间没有明确接管关系，也无法给出“这台新 Pod 完整替换了哪台旧 Pod”的发布证据。

## 3. 核心实现

### 3.1 新增定向规划原语

新增：

```go
DrainHolderTo(topology, victimHolderID, replacement, policy)
```

要求 `replacement` 是完整、精确且当前经过验证的 `READY_ZERO_SHARD` Holder：

```text
domain
holder_id
ordinal
workload_uid
pod_uid
boot_id
```

Planner 为 Victim 的每一个 Shard 生成相同目标，不再运行最低负载目标选择。

通用 `DrainHolder` 保留原有分散迁移语义，普通缩容行为未被改变。

### 3.2 持久绑定 Replacement Generation

`ScalePlan.Submission` 新增：

```text
PinnedTarget HolderRef
```

它与完整 ScalePlan 一起持久化，并进入 `SubmissionPayloadDigest`。因此：

- Controller 重启后仍恢复同一目标；
- ScalePlan Executor 重启后仍恢复同一目标；
- 同一个 holder_id 发生容器重启、`boot_id` 改变时，不会被当成原目标；
- Pod 重建、`pod_uid` 改变时，不会被当成原目标；
- 不能在后续 Wave 中悄悄改为另一台 Pod。

`RolloutState` 保存 `DrainPlanID`；精确 Replacement 身份由该持久 ScalePlan 保存，避免两份可漂移的目标真值。

### 3.3 Controller 只接受空的新版本 Replacement

托管 Rollout 进入 `DRAINING_OLD_HOLDER` 时，只选择：

```text
目标 controllerRevision / imageDigest
+ K8s Ready
+ HolderControl ProcessReady
+ AssignmentKnownFresh
+ PlacementEligible
+ READY_ZERO_SHARD
+ ServingShardCount == 0
```

如果当前没有这样的新 Pod，Controller 只等待，不会回退到通用 `DrainHolder`，也不会把 Shard 分流给其他 Holder。

### 3.4 Executor 每个阶段都校验同一 Generation

Executor 在两个位置校验目标：

1. 选择 Victim、冻结删除前写入屏障时；
2. 每一个 Shard Wave 真正创建 RoutePlan 前。

如果目标 Pod 或进程在中途重启：

```text
pod_uid 或 boot_id 改变
→ DRAIN_TARGET_GENERATION_CHANGED
→ ScalePlan BLOCKED
→ 不创建面向新 Generation 的后续 ShardMove
→ 不删除 Victim
```

这是故障关闭，不会为了“继续发布”而牺牲接管关系。

## 4. 本轮代码范围

```text
internal/scaleplan/model.go
internal/scaleplan/identity.go
internal/scaleplan/codec/codec.go
internal/scaleplan/state_machine.go
internal/scaleplan/executor.go
internal/scaleplan/autorebalance/planner.go
internal/scaleplan/autorebalance/controller.go
```

相应回归覆盖：

```text
internal/scaleplan/autorebalance/planner_test.go
internal/scaleplan/autorebalance/controller_test.go
internal/scaleplan/executor_test.go
internal/scaleplan/codec/codec_test.go
```

## 5. 已验证的不变量

1. Victim 的所有 Shard 只指向一个 Replacement。
2. 其他更空闲的 Holder 不会抢走托管发布的 Shard。
3. 没有 `READY_ZERO_SHARD` Replacement 时只等待，不创建分散迁移计划。
4. 完整 Replacement 身份经过持久化编码、解码后不变。
5. `PinnedTarget` 参与 ScalePlan 摘要，目标代次变化不能复用旧计划身份。
6. Replacement 重启后计划阻断，未创建的 ShardMove 不会改投新代次。
7. 普通扩缩容仍使用原有最少迁移/均衡策略。
8. Kubernetes、RouteController 和 ScalePlan 聚焦包回归通过。

已通过：

```text
go test ./internal/scaleplan/autorebalance \
        ./internal/scaleplan \
        ./internal/scaleplan/codec

go test ./internal/scaleplan/... \
        ./internal/platform/kubernetes/scaleplan \
        ./cmd/route-controller
```

`go test ./...` 在 Windows 上仍存在与本改动无关的既有失败，包括公司私有依赖未登录、Linux 专用 `syscall`、压测证据摘要漂移和 Windows 临时目录权限；本轮涉及的包均已通过。

## 6. 还不能声称的内容

当前可以声称：

```text
托管 Rollout 的“旧 Pod 全量定向到指定新 Pod”代码合同已实现并回归通过。
```

当前还不能声称：

```text
真实 Kubernetes 中从 v1 到 v2 的完整 N → N+1 → N 发布已经验收。
```

真实验收仍需保留运行中的 v1 Pod 和 Redis，提交不可变镜像 Digest 与显式 RolloutIntent，记录：

- 每轮 Victim 与 Replacement 的 PodUID、boot_id；
- 每个持久 DrainPlan 的 `PinnedTarget`；
- Victim 全部 Shard 的目标；
- writer_epoch 单调变化；
- Victim 零 Shard 与删除屏障；
- 新 Pod Ready 后进入下一轮；
- 最终全部 Pod 为目标 Revision，副本数恢复 N；
- 发布期间请求成功率、Redirect、幂等恢复和 Rejoin 结果。

