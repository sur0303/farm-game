# ScalePlan A9 自动均衡与托管滚动更新交接

日期：2026-08-15

状态：隔离 kind 环境真实验收通过；相关能力已于 2026-08-16 随 `8cd210daf198a554836fa96a02b1d25424b9a010` 快进合入远程 `codex/integration-20260803`

分支：`codex/rolloutplan-a1-safe-statefulset-r1`

基线提交：`58dc0fc`；最终修复和本文随同一提交交付

## 1. 最终结论

本轮已经真实跑通两条控制链路：

```text
K8s发现2个Player/Farm Holder
→ 初始Shard集中在*-0
→ 自动生成ScalePlan
→ PLAYER/FARM分别完成2/2均衡
```

```text
2个旧版Player Pod持续运行
→ 提交不可变新Digest和RolloutIntent
→ 临时扩容到3
→ 新版player-svr-2通过Ready和HolderControl验证
→ 旧player-svr-1全部Shard定向迁入player-svr-2
→ 旧player-svr-1归零并按PodUID删除
→ 新版player-svr-1重建
→ 旧player-svr-0全部Shard定向迁入player-svr-2
→ 旧player-svr-0归零并按PodUID删除
→ 新版player-svr-0重建
→ 安全回收临时player-svr-2
→ ROLLOUT_COMPLETE，恢复2副本
```

因此可以确认：当前代码已具备“发现新 Pod、定向接管旧 Pod 全部 Shard、
逐个替换旧 Pod、最终恢复原副本数”的托管滚动更新能力。

## 2. 安全边界

本轮只操作：

```text
Linux user: developer_026
K8s context: kind-a9-scaleplan
namespace: a9-scaleplan
```

没有触碰：

- `developer_026-qqfarm`；
- 其他 Kubernetes Context/Namespace；
- `Integration` 分支；
- 正在运行的压测工作区。

冷基线构造阶段只清理过 `a9-scaleplan` 自己的隔离 Redis。正式 Rollout
开始后没有清空 Redis、没有把全部 Holder 缩到 0，也没有人工修改
`writer_epoch` 或 Authority。

## 3. 本轮新增修复

### 3.1 接受 Redis Authority 的合法 TTL 裁剪

真实 Redis Authority 会把 RoutePlan 请求的 1 小时 TTL 裁剪为 60 秒。
原校验错误地要求：

```text
accepted_ttl_ms == requested_ttl_ms
```

这会把已经成功提交的迁移误判为 `CHILD_VERIFICATION_NON_EXACT`。

修复后的合同是：

```text
0 < accepted_ttl_ms <= requested_ttl_ms
```

修改位置：

- `internal/routeplan/production_verification.go`
- `internal/routeplan/production_verification_test.go`

新增测试同时覆盖 Transition、Recovery 和 Tombstone 三类证明。

### 3.2 自动恢复依赖短暂不可用的 ScalePlan

RouteSnapshot 发布、HolderRecord 心跳和 K8s Holder 验证不是同一原子操作。
迁移波次之间可能短暂出现一致性窗口，Executor 会安全地把计划暂停为：

```text
DEPENDENCY_UNAVAILABLE
DEPENDENCY_TEMPORARILY_UNAVAILABLE
```

原自动控制器只会不断看到 `PAUSED`，不会调用 `Resume`，导致计划永久停住。

现在 `autorebalance.Controller` 会在下个调和周期：

```text
读取同一个持久PlanID
→ 仅识别依赖类暂停
→ Resume
→ 继续Execute同一计划
```

以下暂停仍不会被自动恢复：

```text
OPERATOR_PAUSED
其他未知暂停原因
```

修改位置：

- `internal/scaleplan/autorebalance/controller.go`
- `internal/scaleplan/autorebalance/controller_test.go`

## 4. 真实镜像证据

最终构建并推送的不可变镜像：

```text
registry digest:
sha256:3e89b032621c8cabca0f8da75cf495338bd2c1aa7471115c91c68e9f22466f7e

in-cluster image:
farm-l2-r2-registry:5000/farm/route-runtime@sha256:3e89b032621c8cabca0f8da75cf495338bd2c1aa7471115c91c68e9f22466f7e
```

RolloutIntent 明确绑定该 Digest、`expected_replicas=2` 和有界 Deadline。
仅修改 PodTemplate 而没有显式 Intent 时，控制器仍然 Fail Closed，不会擅自发布。

## 5. 自动均衡证据

初始 PLAYER/FARM 的四个 Shard 都在 `*-0`，两个 `*-1` 为零 Shard。
控制器依次执行 FARM、PLAYER 两个域，最终自动完成 2/2 均衡。

过程中真实观察到：

```text
FARM ADOPT_SCALE_OUT
→ 两个迁移波次
→ SUCCEEDED

PLAYER ADOPT_SCALE_OUT
→ RUNNING
→ DEPENDENCY_UNAVAILABLE/PAUSED
→ 自动恢复RUNNING
→ SUCCEEDED
```

这证明自动恢复不是单元测试中的假路径，而是在真实 Redis、真实 Holder 和真实
Kubernetes 调和窗口中触发并成功完成。

## 6. 托管 Rollout 证据

### 6.1 精确 Pod 身份

复验前旧 Pod：

```text
player-svr-0 PodUID=15ab3475-3ca9-4897-b1af-602a4f9440fe
player-svr-1 PodUID=e3706c76-34df-4a42-a96e-088d4fe9de04
```

Surge Replacement：

```text
player-svr-2 PodUID=61bf6b8f-684b-4804-9dd7-0e30213e8ab9
```

最终新 Pod：

```text
player-svr-0 PodUID=5c972b47-50a1-4e65-9a73-0f59e5e2c831
player-svr-1 PodUID=b3c85f73-4564-4e7e-b033-9c59d8b40f3f
```

旧 PodUID 均已删除，最终 `player-svr-2` 也已由安全 ScaleIn 回收。

### 6.2 真实状态机日志

日志完整出现：

```text
ROLLOUT_START
ROLLOUT_EXPAND
ROLLOUT_WAIT_REPLACEMENT
ROLLOUT_DRAIN_HOLDER
RESUME_ACTIVE_PLAN
ROLLOUT_DELETE_OLD_POD
ROLLOUT_WAIT_REPLACEMENT
ROLLOUT_DRAIN_HOLDER
RESUME_ACTIVE_PLAN
ROLLOUT_DELETE_OLD_POD
ROLLOUT_CONTRACT_SPARE
SAFE_SCALE_IN
RESUME_ACTIVE_PLAN
ROLLOUT_COMPLETE
```

两次 `DRAIN_HOLDER` 都带持久 `PinnedTarget`，不是依靠“目标当前最空闲”的偶然结果。
第一轮明确把旧 `player-svr-1` 的 Shard 2、3 全部迁到
`player-svr-2`；第二轮同样把旧 `player-svr-0` 的全部 Shard 迁到该
Replacement，旧 Pod 归零后才执行精确 PodUID 删除。

## 7. 最终 Authority 状态

```text
PLAYER[0] → player-svr-1  writer_epoch=2
PLAYER[1] → player-svr-1  writer_epoch=2
PLAYER[2] → player-svr-0  writer_epoch=4
PLAYER[3] → player-svr-0  writer_epoch=4

FARM[0]   → farm-svr-0    writer_epoch=1
FARM[1]   → farm-svr-0    writer_epoch=1
FARM[2]   → farm-svr-1    writer_epoch=2
FARM[3]   → farm-svr-1    writer_epoch=2
```

最终性质：

- PLAYER/FARM 都是 2/2 分布；
- 每个 Shard 只有一个 ACTIVE Writer；
- `writer_epoch` 在迁移中只增不减；
- 旧 PodUID 不再持有任何 Authority；
- Redis 中不存在 corruption latch；
- `player-svr` 最终 `2/2 Ready`，两个 Pod 都是新 Digest；
- Gate、Redis、Farm Holder、RouteController 均保持 Ready。

## 8. 测试结果

Windows 定向测试：

```text
go test ./internal/scaleplan/autorebalance \
        ./internal/scaleplan \
        ./internal/routeplan \
        ./cmd/route-controller
```

结果：全部通过。

Linux 正式全量测试：

```text
go test ./...
```

结果：全部通过。

Windows `go test ./...` 仍会受以下既有环境因素影响：

- 无法免密读取公司私有 Go 依赖；
- `syscall.Getrusage` 等 Linux-only 代码；
- Windows 临时目录权限和 shell 差异；
- 与本轮无关的既有证据摘要测试。

这些不构成本轮回归；本项目的正式全量验证环境是 Linux。

## 9. 已验收与未验收边界

本轮已经验收：

- K8s Informer 发现 Holder；
- HolderControl 进程身份校验；
- 自动 ScaleOut/均衡；
- 依赖暂停自动恢复；
- N→N+1→N；
- Victim 全量定向迁移；
- `writer_epoch` Fence；
- Victim 零 Shard；
- PodUID 精确删除；
- 同 ordinal 新版 Pod 重建；
- Rollout 最终完成并清理 Intent/State。

本轮没有验收：

- Rollout 窗口中持续发送 Plant/Harvest/Care/Theft 的业务成功率；
- 客户端 Redirect/Retry/Rejoin 的端到端体验；
- RouteController 自身高可用；
- FARM_WORKER 的真实 Holder 部署；
- 热点 Shard 自动识别、迁移或 HPA 联动。

所以可以宣称“控制面托管滚动更新已真实跑通”，不能宣称“业务连续性和全部生产
容灾已经完成”。

## 10. 下一步建议

优先级建议：

1. 在同一隔离环境运行持续业务 Bot，同时再执行一次 Rollout；
2. 记录请求成功率、Redirect、Retry、Rejoin、重复操作和最长恢复时间；
3. 为 `active holder is not fresh` 增加结构化原因指标，减少排障时间；
4. 让 HolderRegistry/RouteSnapshot 的心跳与发布收敛减少无效暂停；
5. 再验证 Pod 突然宕机的自动 Failover，而不是把它和计划内 Rollout 混为一谈；
6. FARM_WORKER 部署完成后再纳入 Route Domain 验收；
7. 热点迁移先做 Shadow 采样，不要和本轮安全发布代码一起合入。

## 11. 交接限制

在合入前不得：

- 直接修改 `Integration`；
- 触碰 `developer_026-qqfarm`；
- 用 `FLUSHALL` 掩盖 Rollout 恢复问题；
- 绕过 `DRAIN_HOLDER` 删除 Holder Pod；
- 人工推进 `writer_epoch`；
- 把浮动 Tag 当作发布身份；
- 把 K8s Ready 当作唯一接管授权；
- 把依赖类自动恢复扩展到 `OPERATOR_PAUSED`。

## 12. 相关文档

- `docs/design/85-k8s-informer-route-control-scaleplan-migration-plan.md`
- `docs/verification/scaleplan-managed-rollout-hardening-20260815.md`
- `docs/verification/scaleplan-auto-rebalance-a6-verification-20260814.md`
- `docs/verification/source-order-scaleplan-a8-combined-handoff-20260814.md`
