# ScalePlan 托管滚动发布加固交接

日期：2026-08-15  
分支：`codex/rolloutplan-a1-safe-statefulset-r1`  
状态：独立分支验证中，禁止直接合入 `Integration`。

## 1. 当前发布主链

```text
发布系统更新不可变镜像 Digest + 写入显式 RolloutIntent
→ route-controller 建立持久 RolloutState
→ StatefulSet 临时扩容 N → N+1
→ 验证新 Holder 的 PodUID / BootID / Revision / ImageDigest / Ready
→ DRAIN_HOLDER 迁走旧 Holder 的 Shard，并保留精确 PlacementExclusion
→ 删除前重新 Capture、校验零 Shard、Exclusion Owner/Revision、WorkloadUID 与 PodUID
→ UID 前置条件删除旧 Pod
→ 验证相同 ordinal 的新 Pod 和新 Holder
→ 逐个替换全部旧 Pod
→ 安全缩容 N+1 → N
→ 删除 RolloutState 与 RolloutIntent
```

`DRAIN_HOLDER` 是托管发布内部子操作。公网/普通 `routectl scaleplan submit` 已拒绝独立提交，避免产生无人负责清理的 Holder Exclusion。

## 2. 显式发布意图

仅修改 PodTemplate 不会自动发布。发布系统必须同时写入：

```json
{
  "schema_version":"statefulset-holder-rollout-intent-v1",
  "release_id":"release-20260815-001",
  "target_image_digest":"sha256:<64 lowercase hex>",
  "expected_replicas":2,
  "deadline_at_ms":1786800000000
}
```

注解名：`scaleplan.farm/rollout-intent`。

开始发布前会校验：

- StatefulSet 使用 `OnDelete`；
- `expected_replicas` 等于当前副本数；
- 目标容器使用 `repo@sha256:...`，不能使用可变 Tag；
- Deadline 尚未到期；
- WorkloadUID、SourceRevision、TargetRevision 和 IntentHash 被写入持久 checkpoint。

## 3. 零 Shard Holder

旧 Holder 已经是 `READY_ZERO_SHARD` 时仍会创建一个空 `Moves` 的持久 `DRAIN_HOLDER`：

```text
绑定 VictimHolderID
→ 建立精确 Boot Exclusion
→ Capture 证明零 Shard
→ Revalidate
→ 成功进入删除前屏障
```

因此不会再因 `ErrNoPlan` 永久停在 `DRAINING_OLD_HOLDER`。

## 4. BLOCKED 恢复

`BLOCKED` 不能通过直接改 Phase 恢复。运维或发布控制器必须写入精确恢复指令：

```json
{
  "schema_version":"statefulset-holder-rollout-recovery-v1",
  "command_id":"resume-release-20260815-001-01",
  "rollout_id":"rollout-v2-...",
  "intent_hash":"<64 lowercase hex>",
  "action":"RESUME",
  "deadline_at_ms":1786800000000
}
```

注解名：`scaleplan.farm/rollout-recovery`。`action` 可为：

- `RESUME`：只有 WorkloadUID、OnDelete、目标 Revision、Digest、Intent 和 Deadline 全部重新校验通过后才恢复原 checkpoint；
- `ABORT`：只允许第一个旧 Pod 尚未 Drain/替换时执行，随后通过普通安全 ScaleIn 回收 Surge；已经部分替换时拒绝自动回滚，必须提交新的 forward rollout。

恢复指令消费和 RolloutState 切换在同一次 StatefulSet Update 中完成，避免重放或半消费。

## 5. 删除前硬屏障

每次实际删除前都重新读取并验证：

- 当前 WorkloadUID 与副本数 `OriginalReplicas + 1`；
- 当前 TargetRevision；
- Victim 的 HolderID、BootID、PodUID、ordinal；
- 当前完整 Route Placement 中 Victim 为零 Shard；
- HolderControl 仍为 Ready、零 Shard、不可接收新 Placement；
- PlacementExclusion 仍由当前 DrainPlanID 持有，Revision 完全一致；
- CaptureToken 仍可 Revalidate；
- Kubernetes Pod OwnerReference、WorkloadUID 和 PodUID 三重匹配。

删除只使用 UID precondition。过去成功过的 Drain 记录不能单独授权当前删除。

## 6. 新增监控

- `farm_scaleplan_rollout_active`
- `farm_scaleplan_rollout_phase`
- `farm_scaleplan_rollout_duration_seconds`
- `farm_scaleplan_rollout_current_ordinal`
- `farm_scaleplan_rollout_blocked_total{reason}`
- `farm_scaleplan_rollout_pod_delete_total`
- `farm_scaleplan_rollout_drain_plan_total`
- `farm_scaleplan_rollout_old_revision_pods`
- `farm_scaleplan_rollout_target_revision_ready_pods`

## 7. 验证边界

Windows 只用于代码审阅和可移植的 Go 单测；动态清单脚本、竞态测试、镜像构建和真实 kind OnDelete 发布必须在 Linux 执行。真实验证固定使用：

```text
Kubernetes context: kind-a9-scaleplan
namespace:          a9-scaleplan
```

禁止触碰 `developer_026-qqfarm`。
