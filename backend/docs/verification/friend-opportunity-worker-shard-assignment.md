# 好友机会 Worker Shard Assignment 验证记录

状态：`UC_STATIC_SPLIT_VERIFIED / DYNAMIC_ROUTE_PENDING`

范围：82 号计划 C4。本文只证明 `FARM_OPPORTUNITY_WAKEUP` Worker 的分片领取逻辑、迁移 Fence 和单机确定性测试；不宣称已证明 3000 万 DAU，也不宣称已经接入生产 RouteManager Discovery。

## 1. 解决的问题

旧实现的默认行为是一个 Worker 扫描全部 FARM Authority DueIndex。若直接将 Deployment 从 1 副本扩成 N 副本，每个副本都会重复扫描全部 Shard，空轮询 QPS 会接近按 N 倍放大。

本次增加独立路由域：

```text
FARM
  farm_id -> FARM virtual_shard -> FarmActor holder

FARM_WORKER
  同一 farm_id -> 同一 virtual_shard 编号 -> farm-worker holder
```

两个域共享 FARM 的 ShardCount 和 `farm_id -> shard` 哈希结果，但 Holder、Boot、Lease、Assignment Revision 和本地路由表相互独立。获得 `FARM_WORKER shard=7` 只表示可以扫描 FARM shard 7 的 DueIndex，不获得 FarmActor 写权。

## 2. Worker 热路径

```text
RevisionedAssignmentProvider
  -> 完整 RouteSnapshot Watch
  -> RouteWorkerShardAssignmentProvider 本地不可变视图

每一轮 RunOnce：
  1. 固定 assignment_revision + owned_shards
  2. 仅 ListDue(owned_shards)
  3. 每个 DueRef 在 Claim WorkLease 前再次 CanClaimShard
  4. 仍属于本 Worker且revision未变化 -> Claim
  5. 已迁出、过期、Watch失效 -> 拒绝新 Claim
```

Assignment 查询和 Claim Admission 都只读进程内状态，不按轮访问 Redis 或控制面。DueIndex 和 Schedule 数据仍留在原 FARM Authority Bucket，不随 Worker 迁移。

## 3. 迁移语义

- 迁出 Shard：新的 WorkLease Claim 被 revision/ownership Fence 拒绝；
- 已经 Claim 的任务：继续在原 WorkLease 内执行，或等待 Lease 自然过期；
- 迁入 Shard：新 Worker 直接从同一 DueIndex 继续发现；
- Watch gap、同 revision 不同内容、无首份 Snapshot、Holder Lease 过期：本地状态 Fail Closed，不再领取新任务；
- 零 OwnedShards：合法，不扫描、不报错；
- WorkLease 仍是极小竞态窗口内的最终重复领取 Fence。

## 4. 开关与回退

```text
FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE=static  # 默认，保持当前单 Worker行为
FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE=route   # 使用RouteSnapshot中的FARM_WORKER域
```

`static` 模式可用 `FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS` 指定显式子集。`route` 模式禁止同时设置该变量，并要求：

- `ACTOR_ROUTE_SNAPSHOT` 包含完整 `farm_worker` 域；
- `WORKER_ID`、`WORKER_BOOT_ID` 与 Holder 身份一致；
- FARM_WORKER ShardCount 与 FARM ShardCount 完全相等。

关闭 `route` 即可回到当前单 Worker 基线，不删除 DueIndex、不迁移数据。

## 5. 可观测性

新增低基数指标：

- `farm_opportunity_wakeup_assignment_revision`
- `farm_opportunity_wakeup_assignment_ready`
- `farm_opportunity_wakeup_assignment_watch_gaps`
- `farm_opportunity_wakeup_assignment_full_reloads`
- `farm_opportunity_wakeup_owned_shards`
- `farm_opportunity_wakeup_claim_rejected_total`

## 6. 已验证内容

确定性测试覆盖：

- FARM 与 FARM_WORKER 对同一 farm_id 计算出相同 Shard；
- 两域使用不同 Holder；
- FARM_WORKER ShardCount 不匹配时拒绝快照；
- Snapshot 深拷贝不会遗漏 FarmWorker Routes；
- 迁移后旧 revision Claim 被拒绝；
- Holder Lease 过期后 OwnedShards 为空；
- Claim 前二次校验失败时不会调用 Redis WorkLease Claim；
- Watch 出现 revision gap 时先 Fail Closed，完整重载期间拒绝新 Claim，随后从新 revision 重新订阅；
- 1 -> 2 -> 4 -> 8 Worker 时 8 个 Shard 始终恰好覆盖一次，不产生重复扫描；
- 旧版本全量 Reload 被拒绝。

验收命令：

```bash
go test ./internal/actorrouting/... ./internal/authoritycomposition/... ./internal/worker/opportunityrecovery/... ./cmd/farm-worker/...
go test -race ./internal/actorrouting/... ./internal/authoritycomposition/... ./internal/worker/opportunityrecovery/... ./cmd/farm-worker/...
go test ./...
git diff --check
```

## 7. 尚未伪装成已完成的平台能力

当前仓库只有 Static 文件源和进程内 Dynamic Provider，没有真实跨 Pod 的 RouteManager Discovery/Watch Adapter。代码已经冻结 `RevisionedAssignmentProvider` 接口与 Fail-Closed 行为，但上线多 Worker 前仍需接入一种真实完整快照来源，并执行：

1. 1/2/4/8 个真实 Worker Pod 的 Assignment 更新；
2. 稳定 revision 下每个 Shard 最多一个普通扫描 Holder；
3. 扩缩容期间无 Schedule 丢失；
4. 总 DueIndex 扫描 QPS 不随副本数重复放大；
5. Watch 断开、revision gap 和 Lease 过期演练。

在这些真实多 Pod 证据产生前，本功能是“逻辑与接口就绪”，不是“3000 万 DAU 已验收”。

## 8. UC 1024 当前可重复部署方法（已实测）

当前 `uc-test-plus/developer_026-qqfarm` 使用 static split 作为安全回退，避免在 Worker Holder 尚未完成 READY_ZERO_SHARD 注册时提前 Claim `FARM_WORKER` Route：

- `farm-worker-a` 固定扫描偶数分片 `0,2,...,1022`；
- `farm-worker-b` 固定扫描奇数分片 `1,3,...,1023`；
- 两者合计覆盖 1024 个 FARM/PLAYER DueIndex 分片且无重叠；
- 每个 DueRef 仍经过 WorkLease、payload/version 和 authority fence，static split 不获得 FarmActor 写权限。

唯一部署入口：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

不要执行 `apply-r93.sh`、裸 `kubectl apply` 或手工 Claim Route。Reset 会在 Worker 启动前写入两个 disjoint static shard 列表，并在启动 Gate 前等待：

```text
Player/Farm holders: 2+2
farm-worker-a owned_shards: 512
farm-worker-b owned_shards: 512
PLANT/HARVEST/CARE/THEFT family: READY
Gate downlink: ready
```

检查命令：

```bash
for p in farm-worker-a farm-worker-b; do
  kubectl --context uc-test-plus -n developer_026-qqfarm exec deploy/$p -c farm-worker -- \
    wget -qO- http://127.0.0.1:9201/readyz
  kubectl --context uc-test-plus -n developer_026-qqfarm exec deploy/$p -c farm-worker -- \
    wget -qO- http://127.0.0.1:9201/metrics | \
    grep '^farm_opportunity_wakeup_owned_shards '
done
```

在 static 模式下，`farm_opportunity_wakeup_assignment_ready` 是 dynamic Route provider 专用指标，不作为 static deployment 的就绪判断；必须以 `owned_shards=512`、Worker `/readyz` 和 family readiness 为准。动态 `FARM_WORKER` RouteManager 初始 Claim 仍是后续独立改造项，不能混入当前 reset 流程。
