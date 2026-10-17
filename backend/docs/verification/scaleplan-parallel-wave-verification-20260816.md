# ScalePlan 并发迁移波次与真实滚动更新验证

日期：2026-08-16  
状态：独立分支已实现并真实验证；已于 2026-08-16 快进合入远程 `codex/integration-20260803`，首次合入落点为 `8cd210daf198a554836fa96a02b1d25424b9a010`

## 1. 分支与代码

- 工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-rollout-parallel-wave-r1-20260815`
- 分支：`codex/rolloutplan-parallel-wave-r1`
- 核心提交：`8a4bcc5ff157705e7e6895bc14a1acf3b3dcddc0`
- 提交说明：`optimize parallel rollout wave checkpoints`

本轮没有修改 `Integration`，也没有触碰 `developer_026-qqfarm` 或正在运行的容量压测环境。

## 2. 实现内容

ScalePlan 原本虽然能让一个波次并发创建多个子 RoutePlan，但在所有子计划已经成功后，父计划仍按子计划逐条保存观察和验证结果。1024 Shard 下，子计划数量下降了，父计划 CAS 次数却仍随 Shard 数线性增长。

本轮增加两条批量快路径：

1. `observeCompletedParallelWave`
   - 当前波次所有子 RoutePlan 均为精确 `SUCCEEDED` 时；
   - 一次父计划 CAS 保存整波 `EnsureObserved`、`ChildPhase` 和阶段推进。
2. `verifyCompletedParallelWave`
   - 当前波次全部子结果均通过精确验证时；
   - 一次父计划 CAS 标记整波已验证并进入下一波或最终验证。

任一子计划缺失、非终态、结果非精确或验证失败时，不走批量快路径，回退到原有逐条、Fail Closed 的详细处理。因此优化没有放宽：

- RoutePlan 结果精确性；
- `writer_epoch` / Holder 身份检查；
- 父计划 CAS；
- 暂停、恢复和失败语义。

新增常规回归测试证明：并发度 8、16、32 时，一个已完成波次从检查点到终态均只产生 4 次父计划 revision 推进，与波次中的 Shard 数无关。

## 3. 自动化测试

通过：

```text
go test ./... -count=1
go test -race ./internal/scaleplan -count=1
go test ./internal/scaleplan ./internal/routeplan \
        ./cmd/route-controller \
        ./internal/platform/kubernetes/scaleplan -count=1
```

1024 Shard 压测由环境变量显式开启，默认测试不会额外增加两分钟耗时：

```text
SCALEPLAN_RUN_1024_PRESSURE=1 \
go test ./internal/scaleplan \
  -run TestScalePlan1024ShardParallelProfilesPressure -count=1 -v
```

## 4. 1024 Shard 控制面压测

| 并发度 | 波次数 | Execute 调用 | 总耗时 | 吞吐 |
|---:|---:|---:|---:|---:|
| 1 | 1024 | 1025 | 71.239s | 14.37 Shard/s |
| 8 | 128 | 129 | 22.790s | 44.93 Shard/s |
| 16 | 64 | 65 | 19.351s | 52.92 Shard/s |
| 32 | 32 | 33 | 17.774s | 57.61 Shard/s |

同一代码在批量父检查点优化前，并发度 8 约为 49.795s；优化后为 22.790s，耗时减少约 54%，吞吐约为 2.2 倍。

建议默认并发度仍为 8：它相对串行已经获得主要收益，同时避免给 Redis、Holder Drain 和网络施加过大的瞬时压力。16/32 只作为大型迁移的可调档位，需要真实依赖容量数据支持。

该压测覆盖 ScalePlan 编排、父计划 CAS 和确定性子计划适配器，不包含真实网络、Actor Drain、Redis Authority 提交时延。因此它证明控制面不会再因 1024 次父检查点线性退化，不能单独证明生产迁移总耗时。

## 5. 真实 kind 滚动更新

环境边界：

```text
K8s context: kind-a9-scaleplan
Namespace:   a9-scaleplan
Linux user:  developer_026
```

因前一轮故障注入留下了过期控制令牌、过期 Holder 心跳和未发布 Transition，旧测试状态无法形成可信基线。本轮停止了该隔离命名空间内的 Gate、Holder 和 RouteController，重建其无持久卷 Redis，并按以下顺序恢复：

```text
空 Redis
→ bootstrap revision=1 的零 Shard Snapshot
→ 启动 RouteController
→ K8s 发现四个 WARMING Holder
→ HolderControl 身份验证
→ 自动初始分配
→ PLAYER/FARM 均形成 2/2 Shard 分布
→ Gate Ready
```

本轮镜像：

```text
farm-l2-r2-registry:5000/farm/route-runtime@
sha256:073a44bc29b8b158e512c57d8e6c53c8e926dddfec7c05c250f7475c3adfb641
```

Player 托管 Rollout 真实出现并完成：

```text
ROLLOUT_START
→ ROLLOUT_EXPAND（2 → 3）
→ 新 player-svr-2 Ready + HolderControl 验证
→ DRAIN player-svr-1 全部 Shard到 player-svr-2
→ player-svr-1 零 Shard后按旧 PodUID删除
→ 新版 player-svr-1 重建
→ DRAIN player-svr-0 全部 Shard到 player-svr-2
→ player-svr-0 零 Shard后按旧 PodUID删除
→ 新版 player-svr-0 重建
→ CONTRACT_SPARE
→ 临时 player-svr-2 清空并回收（3 → 2）
→ ROLLOUT_COMPLETE
```

时间约为 22:32:38 至 22:35:12，共约 154 秒。期间两个 DRAIN 子 ScalePlan 均进入并最终恢复为 `SUCCEEDED`。

最终状态：

```text
player-svr-0  Ready=true  新Digest
player-svr-1  Ready=true  新Digest
replicas=2, readyReplicas=2
rollout-intent=<empty>
managed-rollout-state=<empty>

PLAYER[0] -> player-svr-1, writer_epoch=2
PLAYER[1] -> player-svr-0, writer_epoch=3
PLAYER[2] -> player-svr-1, writer_epoch=2
PLAYER[3] -> player-svr-0, writer_epoch=3

FARM 四个 Shard仍保持唯一Writer和2/2分布。
Gate、Redis、Player/Farm Holder、RouteController均Ready。
```

旧 Player PodUID：

```text
player-svr-0 e5bf90fa-8793-463e-ab27-7fc50803fff1
player-svr-1 ff59a56f-bf42-4d39-857f-86c62ca26b9d
```

最终新 PodUID：

```text
player-svr-0 374a0a0f-63dd-402d-ad31-cfc65b0fc9af
player-svr-1 0d2688ab-64b9-49b7-9395-2d180b5dad00
```

旧 UID 均不存在，临时 `player-svr-2` 已不存在。

## 6. 已发现但未在本轮扩大范围修复的问题

此前故障注入在 RouteController 长时间中断后暴露了一个独立恢复缺口：

```text
旧 Transition未发布或冷加载Deadline与续约Deadline不一致
+ Holder durable heartbeat已经过期
+ AutoFailover持有的控制令牌已经过期
→ Controller持续Fail Closed
→ 旧 Holder无法重新进入可承载状态
```

这是“控制面长时间中断后的 Holder 重新接纳与令牌刷新”问题，不是本轮并发波次导致。当前干净启动和正常托管 Rollout 均通过，但生产前应单独建立故障恢复任务，至少覆盖：

- AutoFailover 每次重试刷新控制令牌；
- 已存在但 Heartbeat 过期的 Holder 允许通过 K8s 身份 + 新 boot_id 重新进入 WARMING 验证；
- Transition 恢复对合法 Lease Deadline 续约不产生永久冷加载冲突；
- RouteController 中断超过 1 个 Lease TTL 后无需清库即可恢复。

不得用本轮隔离环境重建来宣称该长中断恢复缺口已经修复。

## 7. 结论

可以确认：

- 旧 Pod 的全部 Shard 可以安全定向迁到新版本 surge Pod；
- Shard 不再只能串行一条一条推进，波次可按配置并发；
- 1024 Shard 控制面父计划检查点已从逐 Shard 线性写入收敛为逐波次有界写入；
- 新镜像在真实 K8s、真实 Redis、真实 HolderControl 和真实 WriterEpoch 路径完成一次完整 Player Rollout。

不能据此宣称：

- 30M DAU 容量已经证明；
- 1024 个真实 Actor Drain 已经完成生产压测；
- 长时间 RouteController 故障后的无人工恢复已经闭合；
- Rollout 期间持续业务 Bot 的成功率已经验收。

下一步建议是：先合并本分支到专用候选分支，再在同一隔离环境同时运行业务 Bot 和 Rollout，测量请求成功率、Redirect/Retry/Rejoin、最长恢复时间和重复操作；随后单独修复上面的长中断恢复缺口。
