# K8s Holder 自动发现 A5 实现交接

日期：2026-08-14  
分支：`codex/scaleplan-kube-discovery-a5-20260813`  
工作区：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-kube-discovery-a5-20260813`

## 本轮闭合的链路

```text
显式提交 ScalePlan +1（含 ShardMove manifest）
→ ScalePlan 修改 StatefulSet replicas
→ Kubernetes 创建新 Pod
→ Pod Informer 触发 Reconcile
→ 精确读取 StatefulSet 与 Pod UID / ordinal / Ready / PodIP
→ mTLS 调用 HolderControl.GetRuntimeIdentity
→ TokenReview 校验 Pod 绑定的 ServiceAccount Token
→ 原子替换 VerifiedHolderView
→ 新 Pod 以 READY_ZERO_SHARD 成为 ScalePlan target
→ 原持久 ScalePlan 自动继续 Drain / Handoff / writer_epoch 换代
```

Kubernetes 只承担实例发现。Shard Assignment、Lease、`writer_epoch`、
Drain、Handoff 和 Redirect 仍由 RouteControl 负责；Pod Ready 不会直接
获得写权。

## 主要实现

- HolderControl 增加运行代次与零 Shard 身份确认，包含 Pod UID、
  StatefulSet UID、ordinal、`boot_id`、路由 revision 和当前承载状态。
- Route Controller 使用 SharedInformer 监听 Pod 变化；Informer 事件只负责
  唤醒，作决定前始终执行 Kubernetes 精确读取。
- Route Controller 使用 mTLS + TokenReview 双重验证 Holder 进程与 Pod
  身份，并按 Workload 原子发布完整 `VerifiedHolderView`。
- `k8s-canary` / `k8s` 模式下，ScalePlan 的 HolderStateReader 直接读取
  `VerifiedHolderView`，不再要求新 Pod 先主动注册到 HolderRegistry。
- 新 Pod 只有在 `READY_ZERO_SHARD`、零 inflight、路由版本已加载且验证未
  过期时，才可在尚无旧 PlacementExclusion 记录的情况下作为扩容目标。
  Serving 或 Draining Holder 缺少排除记录时仍然 fail closed。
- 动态 K8s profile 为 Route Controller 配置最小 Pod/StatefulSet RBAC、
  TokenReview 单权限 ClusterRole；Player/Farm Pod 使用独立无 RBAC
  ServiceAccount 和 600 秒轮换的 Pod-bound Token。

## 模式

- `legacy`：不启动 K8s discovery，维持旧路径。
- `shadow`：执行发现和验证，但不作为 ScalePlan 候选权威。
- `k8s-canary`：K8s 验证视图成为 Holder 候选来源，适合当前动态本地验证。
- `k8s`：生产切换目标，语义与 canary 相同，发布治理不同。

动态 profile 在 `SCALEPLAN_MODE=manual` 时默认 `k8s-canary`；
`SCALEPLAN_MODE=off` 时保持 `legacy` 且不授予 ScalePlan RBAC。

## 明确不包含

- 外部人员直接执行 `kubectl scale` 后，系统不会凭 Pod 数变化自动生成
  ScalePlan 或猜测 ShardMove manifest。
- 本轮没有引入 HPA 自动容量决策。
- PlacementExclusion 的独立持久存储迁移仍是后续 M3.5；当前新建零 Shard
  Pod 有严格的过渡准入，其他状态继续 fail closed。
- 未合入 Integration，也未修改正在压测的工作区或集群。

## 已执行验证

- K8s materializer 13 项渲染与 RBAC/Token 合同测试。
- Holder discovery、TokenReview、VerifiedHolderView、HolderControl、
  ScalePlan verified-holder adapter 和三个服务入口的聚焦 Go 测试。
- `git diff --check`。

真实 kind `+1` 迁移属于下一道隔离验收：需要发布本分支镜像，在独立
namespace 提交一个带 move manifest 的 ScalePlan，并核对新 Pod 从
Informer 发现到唯一 Writer Handoff 的完整证据。
