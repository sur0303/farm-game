# 80-bot `ERR_STORE_UNAVAILABLE` 原因定位

## 结论

这两次不是 Redis 连接池耗尽，也不是业务库存/成熟度拒绝；它们是 Route Snapshot 世代切换期间，Gate 数据面发生的本地 route/pool generation fail-closed，最后被 Gate 统一映射成公网 `ERR_STORE_UNAVAILABLE`。

## 时间相关性

| 时间（UTC） | Gate | Route Controller |
|---|---|---|
| 17:10:16 | `HARVEST` -> `ERR_STORE_UNAVAILABLE` | `independent deadline refresh success=true`, `changed_shards=915`, `snapshot_revision=2457` |
| 17:11:20 | `SELL` -> `ERR_STORE_UNAVAILABLE` | `recovery/deadline pass success=true`, `changed_shards=986`, `snapshot_revision=2464` |

Route Controller 每约 9 秒发布一次 deadline refresh；每次有约 860–1012 个 shard 的 deadline 改变。两条日志还显示另一条并发 refresh 同时得到：

```text
routeplan: verification failed
Snapshot changed during refresh
```

## 代码路径

1. `cmd/route-controller/plan_runtime.go:StartRecovery` 同时启动 recovery ticker 和 independent deadline-refresh ticker。
2. 两条路径都调用 `ReconcileRenewedAuthorityDeadlines`，对同一 Route Snapshot 做 CAS；一条成功发布，另一条看到 revision 已变化，于是出现 `Snapshot changed during refresh`。
3. Gate 的 `ShardedLogicClient` 使用 `PooledResolver`/`PooledClientConn`，要求 resolver assignment、endpoint-pool generation、writer/lease/snapshot revision 完全一致。
4. 在 resolver 已安装新 revision、endpoint pool 尚未完成同 revision 安装，或 outgoing route 在切换窗口变旧时，数据面会 fail closed，返回 `ErrEndpointPoolGeneration`/`ErrStaleRoute`。
5. `internal/gateway/command/gate.go:1429-1431` 对任意 `logic.ExecuteCommand` error 调用 `backendUnavailable`，对外统一给出 `ERR_STORE_UNAVAILABLE`，因此公网错误名称掩盖了真正的 route/pool 原因。

## 排除项

- `farm_gate_command_backend_rejected_total` 的累计 reason 是 `RPC_UNAVAILABLE`，不是 `BACKEND_SLOTS_FULL`。
- 两个失败时间点附近 Player participant RPC 仍大量 `grpc_code=OK`；没有对应 Redis pool timeout、i/o timeout、holder lease expired 或 writer epoch mismatch。
- Gate 当前 `farm_actor_writer_epoch_mismatch_total=0`。
- 因 Prometheus 已过该历史窗口保留期，无法再从历史 PromQL 读取原始 error string；现有 Gate 日志只记录了公网映射后的错误，没有记录 `logic.ExecuteCommand` 的原始 error。

## 已实现的第一阶段修复

- `EndpointKey = HolderID + HolderBootID + Endpoint` 已加入 Endpoint Pool。
- `GlobalRevision` 只推进 pool 的 observed revision；deadline-only 更新不替换连接、不递增 transport generation。
- Holder/Boot/Endpoint 变化才会预连接并切换 transport generation。
- ActorRouteHeader 增加 `expected_lease_epoch`；deadline-only revision 差异只有在完整 actor Route Fence 一致时才允许通过。
- Gate 已将 `ROUTE_GENERATION_TRANSITION`、`STALE_ROUTE` 与普通 `RPC_UNAVAILABLE` 分开统计和记录原始错误。
- Route Controller 的 deadline projection 已加入单 in-flight gate；已知无写入的 Snapshot race 不会并发污染 control token。
- 增加了 opt-in `authority-delegated` 模式：Manager 可保存独立 Lease Projection 而不发布 Assignment Snapshot；由于 FARM_WORKER、Gate Verification、RoutePlan/ScalePlan 等消费者需要独立使用 Authority freshness，当前部署默认未打开该模式。
- FARM_WORKER 已拆分 `WorkerRouteView` 与 `WorkerLeaseView`：Redis 成功续租结果直接更新本地 Lease View，普通 Snapshot deadline 不再决定 delegated Worker 的扫描/Claim 准入。
- Gate Verification、RoutePlan final freshness、ScalePlan placement capture 已增加 delegated 分支：拓扑/Holder/Fence 仍来自 Snapshot，实时 deadline 来自 Authority 读结果。

## 尚未做的最后区分

当前证据已足以把根因归到 Snapshot/pool generation transition，但尚未区分单次失败究竟发生在：

- `PooledResolver.Resolve` 的 `ErrEndpointPoolGeneration`，还是
- `PooledClientConn.connection` 的 outgoing route `ErrStaleRoute`。

下一版应在 Gate backend reject metric/log 中增加有界的 route/pool error class（不记录 actor ID），并把 recovery 与 independent deadline refresh 串行化或去重；否则 75 bots 的结果只能作为该历史阶段的业务安全档位，不能宣称 route-control 完全稳定。

## 后续修复与根因演进（2026-08-20）

上述 Route Snapshot/Endpoint Pool 问题随后已完成分层修复：EndpointKey 绑定 HolderID+BootID+Endpoint，deadline-only 更新不切 transport generation，Gate 将 ROUTE_GENERATION_TRANSITION/STALE_ROUTE/RPC error 分开统计，Route Controller 去除了重复 deadline refresh，并修复 control token 释放边界。最新 `n5000-audited-claim-r1` 的 route/store error lines=0，因此早期 80 Bot 的 route generation 问题不再是当前容量主因。

5000 Bot 阶段的新主因是 Redis round-trip、历史 Receipt 重复审计和 Worker 恢复扫描放大：

- 初始 `server commands/action=236.74`；
- 热 Player snapshot 后为 223.63；
- LoadAndClaim 单次 Lua snapshot 后为 220.47；
- 新 Claim 全审计 + 同 Claim Audited Commit 后为 171.09；
- Reject 从 351 降至 0，Worst P95 从约 427ms 降至 43.88ms；
- Worker takeover 从 15077 降至 680，Direct drop 从 6533 降至 0。

详细代码路径、安全边界、失败的 Direct=32 试验和原始证据见 `../online-5000-optimization.md`。当前剩余瓶颈已经转为 Worker 的固定 `ZRANGEBYSCORE/ZCOUNT/TIME` 扫描、Direct SourceOrder 回读、completion 单条写以及 Participant 双重读取，不能再用旧的 Route Snapshot 根因解释。
