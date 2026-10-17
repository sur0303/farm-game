# Route Holder 自动故障恢复 A7 验证与交接

日期：2026-08-14
分支：`codex/scaleplan-holder-failover-a7-20260814`
基线：A6 `aa3f283`
状态：代码、组合故障测试、真实 Redis 原子探针与部署渲染验证通过；未合入 Integration；未改动正在压测的集群。

## 1. 本轮完成

- 增加 PLAYER/FARM Holder 故障自动 Reconcile；
- K8s VerifiedHolderView 与 RoutePlan 目标读取正式接通；
- RoutePlan 精确 Holder 查询会主动重验目标域，避免等待 30 秒 Informer 周期 resync 才看到新路由版本；
- RoutePlan 启动前等待 PLAYER/FARM 两个 Informer 首轮快照，防止半空视图误判故障；
- 精确识别 `holder_id + boot_id` 代次，而不是只看 Pod 名；
- 新 Boot 可作为旧进程失效证据，纯消失则等待 Authority Lease 到期；
- 自动选择健康、负载最少的目标 Holder；
- 使用确定性 CrashTakeover Plan/Transition ID；
- RoutePlan 提交与执行分别获得独立的有界 Deadline，避免提交阶段耗时挤占执行预算；
- 增加独立 `TakeoverExpiredShardAuthority` Memory/Redis/Lua 合同；
- 修复“LeaseExpired 证明却仍调用普通 Transfer CAS”的真实错误；
- AutoFailover 与 AutoRebalance 共用控制锁；
- 增加 Pass/Plan/Authority Read/missing/waiting/no-target Prometheus 指标；
- K8s renderer、ConfigMap、Pod Env 和不可变配置 Hash 纳入故障恢复参数；
- 增加真实 Redis exact-key 探针，禁止 KEYS/SCAN/FLUSH。

## 2. 关键验证

### 2.1 自动状态推进

组合测试模拟真实 Informer 输出的 VerifiedHolderView：

```text
视图包含source boot-A + target boot-B
→ 无故障

Informer原子替换视图，删除source boot-A
→ Snapshot仍指向source
→ Lease存活，控制器等待

Redis Authority时间推进到Lease过期
→ 自动生成CrashTakeover
→ 目标为target boot-B
→ Proof=LeaseExpired
```

同时验证：恢复一次后新 Snapshot 稳定时不会重复创建 Plan；相同输入产生相同 PlanID。

额外验证 RoutePlan HolderReader 的主动刷新边界：精确读取 FARM 目标前只刷新 FARM
Discovery；刷新后的 `ObservedRouteRevision` 立即可见。重验短暂失败时只允许复用仍在
`VerifiedUntil` 内的精确缓存代次；目标不存在且无法重验时查询失败关闭，不能继续执行接管。

### 2.2 Redis 物理接管

真实 Redis 探针通过本机只读 port-forward 访问 `farm-l2-r2-deps` Redis，使用纳秒级唯一 namespace，只创建并删除一个精确 ShardAuthority Key。结果：

```text
普通Transfer + 已过期Lease → LEASE_EXPIRED（拒绝）
TakeoverExpired + 完整旧Fence → APPLIED
writer_epoch: 1 → 2
lease_epoch: 1 → 2
assignment_revision: 1 → 2
相同transition重试 → ALREADY_APPLIED
旧Holder Renew → FENCE_MISMATCH
```

原始测试输出：

```text
Q2R2-FAILOVER OK holder=q2r2-recovery-pod
writer_epoch=2 lease_epoch=2 assignment_revision=2
```

探针结束后按确切 Key `DEL` 并验证 `EXISTS=0`，没有执行全局扫描或清库。

### 2.3 测试命令

```text
go test ./internal/storage/authoritystore
go test ./internal/storage/redis/authority
go test ./internal/actorrouting/authority
go test ./internal/routeplan/autofailover
go test ./cmd/route-controller
go test ./...
python3 scripts/local-k8s/test_dynamic_route_profile.py
```

动态部署渲染 14 项测试通过。全仓 Go 测试通过。

真实 Redis 探针入口：

```text
AUTO_FAILOVER_REDIS_PROBE=1 \
KUBE_CONTEXT=<isolated-context> \
KUBE_NAMESPACE=<route-namespace> \
REDIS_PORT_FORWARD_SERVICE=<redis-service> \
REDIS_PORT_FORWARD_NAMESPACE=<redis-namespace> \
scripts/local-k8s/verify-holder-failover-redis-cas
```

## 3. 关键文件

```text
internal/routeplan/autofailover/
cmd/route-controller/verified_holder_reader.go
cmd/route-controller/autofailover_metrics.go
cmd/route-controller/holder_discovery_runtime.go
cmd/route-controller/scaleplan_auto_runtime.go
internal/storage/authoritystore/shard_authority.go
internal/storage/redis/authority/shard.go
internal/storage/redis/authority/scripts.go
scripts/local-k8s/materialize-dynamic-route-profile.py
scripts/local-k8s/verify-holder-failover-redis-cas
deploy/k8s/dynamic-local-scaleplan/README.md
docs/design/87-k8s-holder-failure-auto-recovery.md
```

## 4. 未执行与原因

- 未在正在压测的 `farm-l2-r2` 业务 namespace 删除真实 Player/Farm Pod；
- 未合入 Integration；
- 未执行 StatefulSet 真实缩扩容；
- 未做多 Pod 同时故障和整个路由域无目标的容量演练；
- 未做客户端 H5 重 Join 的真实故障注入。

原因是当前集群被其他压测/Agent 共用。删除 Pod 会影响它们，不能把共享环境上的破坏性测试当作隔离验收。代码层已完成 Informer 视图故障组合测试和真实 Redis Fence 验证；下一步应在独立 context/namespace 用本分支镜像执行 Pod 删除验收。

## 5. 独立 K8s 最终验收清单

1. FARM 两 Pod、每个拥有 Shard，保持 H5 在线；
2. 删除持有目标农场 Shard 的 Pod；
3. 观察 `missing → waiting_lease → recovered` 指标；
4. 验证 RoutePlan 为确定性 CrashTakeover，writer_epoch 单调 +1；
5. 验证旧 Boot 迟到 Renew/Commit 被拒绝；
6. 验证目标 FarmActor 从 Redis 冷加载，H5 fresh Join 后状态一致；
7. 对 Player Shard 重复一次，验证原 operation_id 幂等重试；
8. route-controller 在等待 Lease 和执行 Plan 两个阶段分别重启，验证恢复同一 PlanID；
9. 连续删除/恢复同一 StatefulSet ordinal，验证 boot_id 代次不会回退；
10. 同时故障超过 32 个 Shard，验证分批恢复且控制器不饿死其他工作。
