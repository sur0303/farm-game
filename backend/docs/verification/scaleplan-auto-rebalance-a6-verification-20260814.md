# ScalePlan A6 自动 Rebalance 实现交接

日期：2026-08-14
分支：`codex/scaleplan-auto-rebalance-a6-20260814`
基线：A5 `1a21647`
状态：代码与离线验证完成；未合入 Integration；未部署到正在压测的集群。

## 已实现

- `SCALEPLAN_MODE=auto`；
- K8s Holder Discovery 变化唤醒 PLAYER/FARM 自动控制器；
- 直接 +1 Pod 的 Adopted Scale-out；
- annotation 驱动的逐级计划扩容与安全缩容；
- 目标到达后使用 UID/ResourceVersion/原值条件确认并清理一次性 annotation；
- Shard 数均衡、最少迁移、确定性幂等清单；
- 提交前完整 Assignment Capture/Revalidate；
- 复用 ScalePlan/RoutePlan Drain、Handoff、Lease、writer_epoch；
- 零 Shard 最高 ordinal 的安全 scale-only 流程；
- 活动 PlanID 启动恢复和周期续跑；
- ScalePlan 创建 child RoutePlan 后的合并唤醒与周期执行兜底；
- 连续多档扩容优先 Adopt 当前已存在的零 Shard Pod；
- 零 Shard Victim 在 Capture 漂移后的可恢复状态机；
- safe desired 清理前的完整 Placement 越界检查；
- PLAYER/FARM 共用 realm 级本地控制锁；
- 自动模式的最小 K8s RBAC 和 Render 合同。

## 关键文件

```text
internal/scaleplan/autorebalance/
cmd/route-controller/scaleplan_auto_runtime.go
internal/platform/kubernetes/scaleplan/desired_replicas.go
internal/scaleplan/executor.go
internal/scaleplan/model.go
internal/scaleplan/state_machine.go
scripts/local-k8s/materialize-dynamic-route-profile.py
deploy/k8s/dynamic-local-scaleplan/README.md
```

## 已执行验证

```text
go test ./internal/scaleplan/autorebalance
go test ./internal/scaleplan
go test ./internal/platform/kubernetes/scaleplan
go test ./cmd/route-controller
go test ./...
go vet ./...
go test -race ./cmd/route-controller ./internal/scaleplan ./internal/scaleplan/autorebalance ./internal/scaleplan/adapters/control
python3 scripts/local-k8s/test_dynamic_route_profile.py
```

上述目标均通过；动态部署渲染共 14 项测试通过。最终提交前仅剩 `git diff --check`、提交与推送。

## 暂未执行

- 未对正在压测的 kind/Redis 环境修改 StatefulSet；
- 未执行真实 Pod 2→3→2；
- 未做热点权重迁移；
- 未实现 admission webhook 拦截直接 `spec.replicas -1`；
- 未实现 Actor 内存热迁移。

## 独立 Kind 验收建议

1. 复制动态路由 profile 到独立 namespace；
2. 启用 `SCALEPLAN_MODE=auto`；
3. 直接将 `farm-svr` 从 2 增到 3，验证新 Pod 被发现且只移动理论最小 Shard；
4. 设置 farm-svr safe desired annotation 为 2，验证先零 Shard 后删除 ordinal 2；
5. 在迁移中重启 route-controller，验证 PlanID、writer_epoch 和 assignment_revision 单调；
6. 用真实 H5 验证受影响 Farm 重 Join、非受影响 Farm 和 PLAYER 域持续可用。
