# W5B · Subscription Lease（BatchRenewSubscriptions）实施交接

日期：2026-08-03  
基线：`codex/integration-20260803` / `9756bf11cfb2e4aa3cc56e57d039baf6dd184e38`  
实现分支：`codex/w5b-subscription-lease-20260803`（已合入 integration）  
状态：`COMPLETED`（实现 + 双 Gate/Drain/Fresh Join E2E + 200 人续租容量 + GateBoot 退役翻转证据齐备）

## 1. 本次边界

本包关闭第 26 号合同 §11.5 的逐 Subscription Lease 物理差距：GateBoot Reaper 与
Presence `FallbackExpiresAt` 不再是订阅真值，续租以完整 Fence 逐订阅进行。
不冒充完成：真实双 Gate/Drain/Fresh Join E2E、200 人 Renew QPS、GateBoot 退役。

冻结链路为：

```text
Gate 快照现有 Pending(accepted)/Active 订阅候选(完整 SubscriptionFence + DownlinkTargetFence)
→ 按当前 FARM Holder 批量 BatchRenewSubscriptions
→ Hub Lane 内精确 Fence 校验 + TTL 钳制(maxExtension),逐项返回 accepted_ttl
→ Gate 建立 renew_started_monotonic + accepted_ttl - safety_margin 本地单调 Deadline
→ 临时失败保留旧 Deadline;显式 Fence 失败与本地过期 fail closed(不可复活终态)
→ 终止帧 REJOIN_REQUIRED(SUBSCRIPTION_LEASE_LOST) 经连接 FIFO 下发,客户端 Fresh Join
→ CleanupSubscription(CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED) 精确异步清理
```

Subscription Lease 独立于 SessionRoute Lease(W5A)：过期只 Fence 当前农场订阅,
不拆除认证、不延长/复活 SessionRoute;已 ADMITTED 的命令按原 operation 完成。

## 2. 实现内容

### 2.1 IDL（复用 W6C Fence）

- `BatchRenewSubscriptions` RPC 与 `SubscriptionLeaseEntry`
  （farm/subscription/join_attempt_id + 完整 `DownlinkTargetFence` + `requested_ttl_ms`）；
- `SubscriptionLeaseStatus` 六值：RENEWED/NOT_FOUND/STALE_FENCE/CLOSED/ROUTE_STALE/UNAVAILABLE；
- `JoinAcceptedMetadata.accepted_subscription_ttl_ms`（Holder 接受的初始 TTL,
  是 duration 而非跨机绝对时间）；
- `CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED`；
- `server_time_ms` 仅诊断,Gate 正确性不得与本地时钟比较；
- Proto 重新生成无漂移（临时副本重跑 `gen-proto.sh`,`diff -rq` 为空）。

### 2.2 Hub 侧（`internal/farm/subscription`）

- `Presence.LeaseExpiresAt` 短租约;`SetSubscriptionLeaseConfig(initialTTL, maxExtension)`
  （构造期钩子,值仍是探针参数而非协议常量）;
- `RenewSubscription`:仅 `existingLiveLane`(缺失误触不建 Lane)、目标 Farm Lane 内序列化;
  惰性过期移除且不可复活;四 PresencePhase(JOINING/FLUSHING/ACTIVATING/IN_FARM)可续租;
  九字段完整 Fence 匹配;TTL 按 `maxExtension` 钳制;
- `PruneExpired` 租约感知:优先 `LeaseExpiresAt`,仅 legacy Presence 回退 fallback TTL;
- 指标 observer 注入（与 invalidObserver 同模式）:
  `farm_subscription_lease_results_total{status}`、`farm_subscription_lease_expired_total`
  （含 Prune 清扫与续租时惰性移除,fallback 过期不计入）。

### 2.3 farm-ingress

- `BatchRenewSubscriptions` 服务端:单一 `trustedNow`、逐 entry 独立结果、
  按 RouteRevision/Holder 状态给出 ROUTE_STALE;
- Join Accepted 元数据携带 `accepted_subscription_ttl_ms`。

### 2.4 Gate 侧（`internal/gateway/command`）

- `RenewSubscriptionLeases`:快照 Pending(accepted)/Active 候选,完整 Fence 批次,
  `renewMu` 串行化重叠轮次;
- 本地单调 Deadline:`renewStarted + accepted_ttl - safetyMargin`;RPC 往返消耗 TTL
  而非延长信任;迟到的 RENEWED 若落地时已越过旧 Deadline 仍判终态;
- 临时失败(RPC 错误)保留旧 Deadline;NOT_FOUND/STALE_FENCE/CLOSED 立即终态;
  ROUTE_STALE/UNAVAILABLE/UNSPECIFIED 仅在 Deadline 内保留,Deadline 越过即终态;
- 畸形响应 fail closed:结果数不符/身份不符/accepted_ttl=0 或超过 requested/renewer
  不支持——返回错误并清扫已过期租约,但不误伤仍在 Deadline 内的租约;
- 候选匹配:generation + subscriptionContextGeneration + Route Fence + Player +
  SessionEpoch + farm/sub/attempt(+presenceEpoch);跨代/翻代迟到响应丢弃;
- 终止投递:REJOIN_REQUIRED 经 `DeliverBatch` 控制通道(共享 ConnectionWriter FIFO);
  QUEUE_FULL 或 Fence 拒绝 → `terminateSubscription` 精确终止;Cleanup 异步;
- downlink Pending 阶段投递:AcceptedQueued ≤ stage < Active 接受终态帧
  （PresenceEpoch 允许为 0,attempt/subscription/target Fence 仍强制精确）;
  pre-accepted 与 stale attempt 拒绝且不误终止新 attempt;
- 指标（全部有界标签）:`farm_gate_subscription_lease_renew_batches_total`、
  `renew_entries_total`、`results_total{status}`、`renew_duration_seconds`、
  `local_expired_total{phase}`、`fail_closed_total{reason}`;
- 配置:`GATE_SUBSCRIPTION_LEASE_TTL`(默认 30s)、safety margin(默认 TTL/10,封顶 1s)、
  renew interval(默认 TTL/3,下限 1s)、renew timeout;`SetSubscriptionLeaseConfig`
  启动期边界校验(TTL≥1ms 且 ≤ uint32ms 上限,margin < TTL)。

## 3. 验证证据

- Hub 层(`internal/farm/subscription/lease_test.go`):四阶段续租+TTL 钳制、
  stale/closed/expired 不可复活、缺失误触不建 Lane;
- ingress 层(`internal/farm/ingress/subscription_lease_test.go`):单一 trustedNow、
  逐 entry 独立结果、stale fence;
- sharded 层(`internal/gateway/command/sharded_test.go`):按 Farm Holder 分组保序、
  per-entry 畸形结果拒绝;
- Gate 层 15 个测试(`internal/gateway/command/subscription_lease_test.go`):
  Active/Pending 续租与 Fence/Deadline 窗口、临时失败保留与恢复、三类终态不可复活、
  UNAVAILABLE 族 Deadline 内保留、迟到响应三形态(deadline 越过/generation 替换/
  context 翻代)、本地清扫幂等、畸形 fail-closed 五用例+RPC 期间过期清扫、
  renewer 不支持、配置边界、指标计数、Pending 阶段 REJOIN_REQUIRED 端到端投递、
  投递拒绝 terminate 兜底;
- downlink 层(`internal/gateway/downlink/w5b_lease_terminal_test.go`):
  Pending(AcceptedQueued)可投递、pre-accepted 拒绝、stale attempt 拒绝且不误终止;
  Active 阶段由既有 `w2b_terminal_test.go` 两测试覆盖;
- 执行:

```text
gofmt -l internal/ cmd/ api/          # 干净(两个历史遗留文件除外,不在本包)
go vet ./...                          # PASS
go test ./... -count=1                # PASS(全仓)
go test -race ./internal/gateway/... ./internal/farm/... ./internal/player/...  # PASS
```

## 4. 运行级证据（2026-08-03 补齐,全部在公司 Redis 真实进程上执行）

1. **双 Gate / Fresh Join**（`f90b11b`）:`cmd/dualgate-e2e` 重写为 protocol v3
   （typed Join 屏障 + ClientRecoveryComplete + REJOIN 重试的 H5 语义）;
   `scripts/test-dualgate-kick` 全绿:A 激活 → v3 Join → B 顶号(epoch 1→2) →
   跨 Gate KICK → 旧命令拒绝 → 旧 Disconnect 不误删 Route/Presence/业务写 →
   **farm-svr 重启 → REJOIN_REQUIRED(SUBSCRIPTION_LEASE_LOST) → 同一连接
   Fresh Join → 经营恢复**。
2. **Drain**（`b2f65e2`）:`scripts/test-multigate-drain` 全绿(真实 H5 浏览器):
   Gate A SIGTERM → DrainNotice/1012 → multigate-proxy → Gate B →
   RecoveryBarrier → InFarm。
3. **200 人续租容量**（`22b84e9`）:`cmd/lease-stress` + `scripts/test-lease-renew-stress`:
   1 业主 + 199 围观同一场农场(=订阅上限),200/200 Join 165ms(1209/s);
   稳态 20 批次 × 200 条目 = **4000 次续租(66.7/s),renewed=4000,
   expired=0,fail_closed=0,p99≤5ms**;客户端 0 REJOIN;
   1600 个 FARM_CHANGE 覆盖 200 连接。
4. **GateBoot 退役翻转**（`dc235ef`）:`GATE_BOOT_REAPER_MODE=enabled|shadow|disabled`
   落地;`disabled` 模式下完整双 Gate E2E 全绿(farm-svr 日志确认模式生效),
   证明订阅生命周期已由逐订阅续租 + 精确断开清理独立承担;shadow 模式
   `would_reap` 收敛探针与单测就位。
5. 说明:加入阶段的瞬时投递围栏(gRPC 启动退避)属协议内 fail-closed +
   重试恢复,稳态窗口单独断言零过期/零 fail-closed(三点采样)。

## 5. 明确未完成

1. 60 号 S05/S06 场景验收(多 bot 围观矩阵、慢读者处置等)属 60 号范围,
   不由本包宣称;
2. shadow 模式的 would_reap 收敛运行证据(需 Gate 死亡场景)留作后续
   Evidence Bundle;
3. GateBoot `enabled` 仍是默认部署形态,disabled/shadow 的生产翻转节奏
   由发布纪律决定。
