# SourceOrder Worker 生产分片与扫描收敛交接

日期：2026-08-14  
分支：`codex/source-order-worker-production-r1`  
基线：`codex/source-order-scaleplan-a8-20260814@452799d`

## 1. 本轮目标

将 Plant、UnlockPlot、Harvest、CareReward、TheftVisitor、TheftGuard 的 SourceOrder 恢复，从“每个 farm-worker 周期性扫描全部 Authority Shard”收敛为生产模型：

```text
RouteControl FARM_WORKER Assignment
→ 每个 farm-worker 只看到自己负责的恢复分片
→ 每轮只轮转扫描有限 Bucket
→ Claim WorkLease 前重新校验 assignment revision
→ Order 幂等恢复
```

本轮没有改变 SourceOrder、Participant Receipt、WorkLease 或 DueIndex 的物理格式，也没有改变业务最终一致性语义。

## 2. 分片归属

进程内只有一份 `farmWorkerShardRuntime`，由机会唤醒 Worker 和 SourceOrder Worker 共用，避免同一进程重复启动 Assignment Watch 和 Authority Renewer。

生产动态路由模式：

```text
RouteControl Watch
→ 本地不可变 Assignment Snapshot
→ FARM_WORKER shard → holder worker Pod
```

- FARM Authority Shard 与 FARM_WORKER Shard 数相等时一一映射。
- PLAYER Authority Shard 使用 `player_source_shard % farm_worker_shard_count` 稳定映射。
- 扩缩容只改变 FARM_WORKER Shard 的 Holder，不改变 Authority Key、Order 或 DueIndex。
- 本地/单 Worker 环境可保留 `static` 模式；生产存在动态 Assignment Provider 时默认使用 `route` 模式。

## 3. 迁移期间的执行规则

每轮扫描开始读取一次本地 Assignment：

```text
revision=R，owned_shards=[...]
```

随后在两个位置校验：

1. 开始扫描 Bucket 前；
2. 对候选 Order 申请 WorkLease 前。

如果 RouteControl 已推进到新 revision，旧 Worker 停止领取新任务。已经取得 WorkLease 的任务允许依照现有幂等 Participant 合同完成；没有领取的 Order 继续留在 DueIndex，由新 Holder 后续发现。WorkLease 仍是最终防重边界，Assignment 是减少重复扫描和缩小故障域的调度边界。

## 4. 有界扫描与空闲退避

生产默认：

```text
WORKER_POLL_INTERVAL=1s
SOURCE_ORDER_WORKER_BUCKETS_PER_ROUND=256
SOURCE_ORDER_WORKER_IDLE_MAX_INTERVAL=4s
SOURCE_ORDER_DUE_OBSERVE_INTERVAL=10s
```

- 一轮最多扫描 256 个当前归属 Bucket；超过时通过进程内游标轮转，不会一次创建与全部 Shard 等量的扫描任务。
- 有候选、扫描错误或任务重试时恢复 1 秒扫描。
- 完整空轮后按 1s → 2s → 4s 退避，降低长期空桶 Redis 开销。
- Readiness 的 Due 深度观测也只检查当前归属 Bucket，并缓存 10 秒。
- Assignment revision 变化会立即使 Due 观测缓存失效，不等待 10 秒。
- 旧 `farm_workflow_backlog` 监控不再每秒遍历全部 Bucket；它复用同一份归属内 Due 观测，无法在有界读取内证明深度时上报 `-1`。

这是一版有界生产基线。非空 Bucket 目录和基于最近到期时间的自适应唤醒仍可作为后续压测优化，不是当前正确性前提。

## 5. 可观测性

保留已有指标：

- `farm_worker_due_depth`
- `farm_worker_oldest_due_age_ms`
- `farm_worker_scan_duration_ms`（其 `_count` 可计算扫描 QPS）
- `farm_worker_claimed_total`
- `farm_worker_completed_total`
- `farm_worker_retried_total`
- `farm_worker_step_unknown_total`
- `farm_worker_terminal_latency_ms`

新增：

- `farm_worker_owned_bucket_count`
- `farm_worker_assignment_revision`
- `farm_worker_bucket_scanned_total`

建议告警关注：Assignment revision 长期不推进、owned bucket 为零但实例预期承载、oldest due age 持续上升、重试率或 Unknown 比例异常。

## 6. 部署参数

Docker Compose 与 static-staging 默认轮询已从 250ms 收敛到 1s，并加入上述 SourceOrder 参数。

```text
FARM_WORKER_ASSIGNMENT_MODE=route   # 生产动态路由
FARM_WORKER_ASSIGNMENT_MODE=static  # 单机/静态兼容
FARM_WORKER_OWNED_SHARDS=...        # 仅 static 测试模式可用
```

`route` 模式禁止同时设置静态 owned shards。动态模式依赖 RouteControl Assignment Provider、Holder 身份和 Authority Renewal 健康；任一条件失效时，Worker 对新 Claim 失败关闭。

## 7. 验证范围

新增单测覆盖：

- 只扫描当前归属 Shard；
- Bucket 数超过单轮上限时轮转；
- Assignment 在扫描和 Claim 之间变化时不申请 WorkLease；
- PLAYER/FARM Source Shard 到 FARM_WORKER Shard 的稳定映射；
- Due 观测只读取归属 Bucket；
- Assignment revision 变化立即废弃旧 Due 缓存；
- 空轮指数退避及有任务/错误时复位。

已通过：

```text
go test ./cmd/farm-worker ./internal/worker/carerecovery \
  ./internal/worker/readiness ./internal/worker/opportunityrecovery

go vet ./cmd/farm-worker ./internal/worker/carerecovery \
  ./internal/worker/readiness ./internal/worker/opportunityrecovery
```

`go test ./...` 已执行，但当前 Windows 环境不能作为全仓通过证据：公司私有 Gateproxy/Tcaplus 依赖无法免交互下载，部分包使用 Linux 专用 syscall，且基线存在容量证据哈希、临时目录权限和时间敏感测试失败。本轮涉及的四个包没有失败。`-race` 在当前环境因 `CGO_ENABLED=0` 无法执行，应在 Linux CI 补跑。K8s ConfigMap JSON 已通过解析；当前 Windows 环境没有 Docker CLI，因此 Compose 的 `config` 校验也需要在 Linux 补跑。

## 8. 未包含范围

- 未引入新的 PLAYER_WORKER Route Domain；当前复用 FARM_WORKER 控制分区。
- 未实现非空 Due Bucket 目录。
- 未改变邮件维护等已有独立 Worker 的分片合同。
- 未合入 Integration，需通过代码评审与多 Worker Redis/K8s 验收后再合并。
