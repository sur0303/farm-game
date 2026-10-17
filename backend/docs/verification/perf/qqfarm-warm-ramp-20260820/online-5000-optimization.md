# 5000 在线经营优化记录

日期：2026-08-20  
集群：`uc-test-plus/developer_026-qqfarm`  
固定拓扑：2 Gate、2 Player、2 Farm、2 Worker、3 Redis、1024 shards  
固定业务合同：5000 个唯一账号、单颗 BUY、5 秒 think-time、真实 BUY/PLANT/CARE/HARVEST/SELL、4 块顺序解锁。

## 最终签字结果

测试：`n5000-shared-due-observe-r1`

- Runtime：`mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:bc1dcd5808f5c862c8725ea5002bb6ee57d90061196b618d56fff009c4c8c766`
- Bot：`mirrors.tencent.com/uc-backend/farm-live-bot@sha256:e2d63eb16a7f8eb861e18f0fbf20513d2526a3103d7cbbb19c7d35ea63fae07c`
- Reset challenge：`83f8555fac315de4db7151671a452042`
- 5000/5000 Ready、5000/5000 Succeeded
- actions=241459，turns=186471，delivery ratio=1.00795
- rejected=0，timeouts=0，pending=0，projection_pending=0
- `PROCESSING=59` 是被 Durable/Projection proof 最终解析的 admission 计数；结束时没有 unresolved pending，不把 admission 本身计作 terminal success。
- Overall worst-shard P95=49.81ms，P99=453.76ms
- Direct drop=0，Worker CARE/HARVEST/PLANT takeover=177
- Worker critical 全程 Ready；workflow backlog max=262、end=0
- route/store error lines=0
- Redis dependency error 未持续 30 秒；pool timeout=0

持久证据位于：

- `artifacts/online-5000-optimization/n5000-shared-due-observe-r1/summary.json`
- `artifacts/online-5000-optimization/n5000-shared-due-observe-r1/shard-*-result.json`
- `artifacts/online-5000-optimization/n5000-shared-due-observe-r1/redis-info.csv`

## A/B 演进

| 轮次 | 关键变化 | Reject | Pending | Worst P95/P99 | Server commands/action | Direct drop | Worker takeover | 判定 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `n5000-warm-stable-r1` | Direct=16，首轮业务 spread | 351 | 15 | 427/1453ms | 236.74 | 6533 | 15077 | FAIL |
| `n5000-direct32-poll2-r1` | Direct 16→32，Worker poll 1s→2s | 718 | 18 | 710/1695ms | 239.46 | 3053 | 7094 | FAIL，回退 Direct=16 |
| `n5000-hot-player-r1` | 热 Player 单 Actor 写复用 Binding snapshot | 10 | 6 | 203/1143ms | 223.63 | 0 | 5326 | FAIL |
| `n5000-load-v4-r1` | LoadAndClaim 单次 Lua 返回完整已验证快照 | 0 | 4 | 179/998ms | 220.47 | 0 | 2081 | FAIL，仅剩 pending |
| `n5000-audited-claim-r1` | 热 Claim/Commit 不再扫描无关历史 Receipt | 0 | 0 | 44/469ms | 171.09 | 0 | 680 | **PASS** |
| `n5000-direct-envelope-poll5-r1` | Care/Harvest 去掉 SourceOrder 回读；recovery poll=5s | 0 | 0 | 46/554ms | 174.75* | 0 | 508 | **PASS** |
| `n5000-shared-due-cache-r1` | family 共享 DueIndex bucket discovery；scan lease 关闭 | 0 | 0 | 39/469ms | 166.75 | 0 | 270 | **PASS** |
| `n5000-shared-due-observe-r1` | readiness Due 观测也复用 cache/Redis TIME | 0 | 0 | 50/454ms | 168.75* | 0 | 177 | **PASS** |
| `n5000-theft-observe60-r1` | Theft poll=250ms；4096-bucket ZCOUNT cache=60s | 0 | 0 | 49/467ms | 168.22* | 0 | — | Driver-only STOP |

`n5000-theft-observe60-r1` 的业务负载全部 qualified，5000/5000 成功且 Worker 全程 Ready；仅 shard-2 压测容器 `sched_latency_p99_ms_max=10.49ms` 超过 Driver 自身阈值，因此工具总判定 false。它不是后端业务失败，仍不将该轮升级为正式 PASS。

\* `total_commands_processed` 包含 Route Controller 和全部后台 Lua 内部命令，本轮 Route Controller 命令比前轮高，导致该比值受背景噪声影响。可归因客户端指标仍明确改善：Farm `HGETALL` 235913→150559，client commands 3449943→3086635，round trips 2581789→2185854，pool waits 峰值 929/s→465/s。

### 失败尝试：Direct=32

Direct goroutine 是 `farm-svr` 内部执行器，不是 `farm-worker` Pod。16→32 虽然减少了 Worker takeover 和 Direct drop，却把并发直接推到 Player/Redis：Reject 从 351 增至 718，Plant P95 约从 1.0s 恶化到 1.5s，Redis/Worker 持续红。因此回退到 16；不能用无限提高本地并发掩盖下游瓶颈。

## 已实施优化及安全边界

### 1. 登录、解锁、业务分阶段和首轮 spread

`BusinessStartAt` 之后，每个 Bot 使用稳定身份派生的 `spec.JitterFraction(0)`，将第一轮经营请求确定性分散到一个 5 秒 think-time 内。它不减少 Bot 数、业务动作或写入，只消除屏障释放时不真实的同毫秒 BUY 洪峰。

### 2. Direct 主路径与 Worker 恢复路径分离

- CARE/HARVEST 正常路径由 `farm-svr` 内的 `DirectDispatcher` 执行。
- `farm-worker` 的 SourceOrder scanner 只做崩溃、队列溢出和 RPC 失败恢复。
- Direct concurrency 最终固定为 16；queue=8192、queue bytes=32MiB。
- SourceOrder Worker active poll 从 1s 逐步调整为 2s、最终 5s，idle backoff 最大 15s；这是异常恢复延迟与固定扫描成本的显式取舍，Opportunity 主路径仍为 1s。
- `carerecovery.ErrRetryDeferred` 等已安全 reschedule 的预期结果不再错误拉红 readiness。

Durable SourceOrder、WorkLease、DueIndex、Participant idempotency proof 和 completion proof 均保留。

### 3. 热 Player Binding snapshot

文件：`internal/player/actor/registry.go`

- 第一次激活仍执行完整 `LoadAndClaim`。
- BUY/SELL 等纯 Player 热写在 mailbox + authorityCoord 串行域内使用 `Binding.CurrentModelSnapshot()`，不再每条命令重复读取 Redis。
- 最终 Authority CAS 仍原子校验 RecordVersion、WriterClaim、WriterEpoch、Lease/Fence 和幂等 Receipt。
- CAS conflict、UNKNOWN、失权、无可信 PhysicalUsage 时强制 Redis refresh。
- Participant turn 在 mailbox 内的 PhysicalUsage refresh 是安全边界，明确保留，未为了性能删除。

新增回归：连续热写不增加 LoadAndClaim 次数；公开 fenced read 仍访问 Store；stale writer 在 Commit 边界被拒绝。

### 4. LoadAndClaim V4/V5

文件：

- `internal/storage/redis/authority/scripts.go`
- `internal/storage/redis/authority/store.go`

V4：Lua 本来已经读取并精确验证 model/receipt values 与 HLEN，Go 随后又执行 `HMGET+HLEN`。V4 将 Lua 已读取的同一快照直接返回，删除重复 Redis dispatch，保留完全相同的字段、摘要、字节数、manifest 和 hash cardinality 校验。

V5：

- 新 WriterClaim、Reconcile 或 Holder 接管继续执行完整历史 Receipt value audit。
- 精确 `ALREADY_CLAIMED` 热重入依赖该 Claim 已完成的全审计和 Commit-only mutation invariant，不再逐条 HGET 无关历史 Receipt。
- Receipt manifest、count、bytes bounds、digest shape、duplicate、HLEN、model values/digest 仍检查。
- 目标 Receipt 在 replay/Commit 时仍逐条解析并验证完整幂等身份及 payload hash。

### 5. Commit V11 与 `AuditedClaimCommitter`

文件：

- `internal/storage/authoritystore/types.go`
- `internal/logic/actor/authority/authority.go`
- `internal/storage/redis/authority/store.go`
- `internal/storage/redis/authority/scripts.go`

`ActorAuthorityStore.Commit` 继续走完整审计，适用于无 Claim 证明的普通调用和 corruption 测试。只有已经通过完整 LoadAndClaim 建立的 `authority.Binding` 才能使用可选 `AuditedClaimCommitter`：

- 保留 ShardWriteFence、WriterClaim、RecordVersion、目标 Receipt、模型完整性、容量、AttachedOrder/Schedule 的原子校验；
- 不再在每次热 Commit 中 HGET 所有无关历史 Receipt；
- WriterClaim 变化后必须重新走完整 LoadAndClaim，不能继承旧审计证明。

新增测试覆盖：普通 Commit 的 corruption fail-closed 不变；Audited Commit 省去历史 Receipt 次数，并保持精确 PhysicalUsage；完整 `go test ./...` 和相关 race 测试通过。

### 6. 已提交 SourceOrder envelope

Care/Harvest 的 source actor 在 Authority Commit 成功后，将同一份 `AttachedOrder` 深拷贝为不可变 Direct envelope。Direct 校验 WorkID、WorkType、OperationID、Subject、PayloadHash、bucket/domain 后直接发 Participant RPC，不再对刚写入的 SourceOrder 执行一次 `HGETALL`。Redis SourceOrder 和 DueIndex 仍先落盘；队列丢失、进程崩溃或 RPC 失败时，Worker 仍从 Redis 恢复，completion 仍走 WorkLease + proof。

该变化使 Farm `HGETALL` 减少约 8.5 万次，client round trips 减少约 15.3%。新增测试使用一个强制报错的 SourceOrder reader，证明 committed-envelope 路径不会回读，同时仍完成 Participant 和 WorkLease completion。

### 7. SourceOrder family 共享 DueIndex cache

Plant、Unlock、Harvest、Care、Theft source recovery 原先分别读取相同物理 bucket，并各自过滤 work type。`SharedDueIndexCache` 现在每个 bucket 最多执行一次有界物理读取，其他 family 本地过滤；并发读取通过 singleflight 合并。缓存只是 discovery hint，任何 stale ref 仍必须通过 WorkLease Claim，不能授权执行。

- cache TTL=5s，单 bucket 最大512 refs；
- disjoint FARM_WORKER assignment 下关闭可选 bucket scan lease；per-order WorkLease 仍是正确性边界；
- readiness Due 观测复用同一缓存和共享 Redis TIME；
- observer 修复为按实际 family 过滤，旧实现对 Harvest/Plant 也硬编码 CARE_REWARD；
- `n5000-shared-due-cache-r1` 相比前轮 client commands 3086635→2758236、round trips 2185854→1879500、Worker takeover 508→270；
- 继续共享 observation 后 takeover 降到177。

### 8. Pair/Theft 空载观测降频

Pair Authority 的4096 buckets 用于好友 CARE/THEFT 的 Durable dispatch/result recovery。此前 readiness 每10秒对全部 bucket 执行 ZCOUNT，即使没有 Theft 也产生约26.6万命令。当前配置将 exact backlog cache 调整为60秒、恢复分页 poll 从100ms调整为250ms；正常 Theft fast path 不变，WorkLease 和全量安全 sweep 仍保留。A/B 中 Worker ZCOUNT 约26.6万→4.1万，Redis client commands 2679758→2216145。

## 当前吞吐解释

5000 Bot、5 秒 cadence 本轮：

- 业务命令 30 秒窗口峰值约 1388/s；
- 所有 Gate 命令峰值约 1641/s；
- 业务 active window 平均约 1340 action/s；
- 约 1030 turn/s。

当前可签字的是约 1340 action/s，而不是最大容量。按 Redis 稳态约 21–22 万 server ops/s、已承受瞬时约 37 万/s 粗估，在进一步优化前当前拓扑的上沿约 2000–2300 action/s，必须通过后续 ramp 实测，不能当成已证明结论。

## 尚余瓶颈与下一步

当前 server command 放大仍远高于 50000 Bot / >=10000 turn/s 所需的 20–30。主要剩余项：

1. `farm-worker` 高频扫描：多个 SourceOrder family 会读取同一个物理 DueIndex；正在加入按 bucket 共享、按 family 本地过滤的 5 秒有界 discovery cache，WorkLease Claim 仍是执行权威。
2. Completion：按 Redis node/slot pipeline，并评估同 bucket 批量 Lua；崩溃时仍由幂等 Participant + Worker completion 恢复。
3. Participant admission/mailbox：保留物理容量复核，但改成轻量 claim/usage validation，避免完整模型重复读取。
4. Route Controller 固定 per-shard 读取：继续降低无变化扫描和单 shard script 数。
5. Friend Opportunity wakeup：单独批量 DueIndex discovery/advance，不能与 SourceOrder crash recovery 混为一条链路。

任何后续优化都必须维持：单颗 BUY、5 秒 cadence、相同业务动作、Durable Order、Route Fence、WorkLease、DueIndex、Receipt、未知结果 fail-closed。
