# TH3C Theft recovery handoff

Status: `RECOVERY_CORE_READY / INTEGRATION_WIRING_PENDING`

本轮交付边界是 Theft 的恢复核心和 Redis/Friend Pair 适配，不是生产进程装配。`internal/worker/theftrecovery` 保留单一 durable recovery state machine：从 FRIEND_PAIR 的 bounded DueIndex 发现 `FRIEND_ACTION_DISPATCH_ORDER`，读取并校验 authority-owned source order，取得 fenced Redis WorkLease，再以稳定 Theft identity 调用窄的 workflow advancer。

`cmd/farm-worker` 与 TH3A Engine 由 integration 分支接线。本提交不修改 `cmd/farm-worker`，不实现 TH3A Engine、Participant、Actor route 或进程 readiness wiring；没有在缺少 TH3A 的分支上制造装配替代物。

## 已交付

- `internal/worker/theftrecovery/recovery.go`：稳定 `WorkflowID`/`SettlementID`/`AttemptID` 派生、claim → renew → advance/receipt → complete/reschedule 状态机、retry exhaustion 到 `RECONCILE_REQUIRED`。
- `internal/worker/theftrecovery/redisdiscover.go`：只从 Pair Authority `SourceDueIndex` 做 bounded bucket/cursor discovery；不使用 SCAN、内存队列或删除坏 member；foreign work type 不进入 dispatch loader。
- `internal/worker/theftrecovery/redislease.go`：Redis Lua WorkLease、Redis `TIME` adjudication、lease epoch/holder/boot/token/proof fence，以及 Work/DueIndex/meta/recovery sidecar 的原子 claim/renew/complete/reschedule。
- `internal/worker/theftrecovery/friendpair.go` 与 `internal/friend/redis_recovery.go`：复用 33A FRIEND_PAIR key builder 和精确 SourceOrder manifest；读取前绑定 work address、payload、permit、dispatch ID、DueIndex score 和 deadline，异常统一 fail-closed 为 reconcile/error。

Lease metadata 写入 recovery sidecar，不扩张 FRIEND_PAIR exact SourceOrder manifest。Friend Pair 的 Work、DueIndex、bucket meta 和 recovery sidecar 使用同一个 authority hash tag；source order 本身仍是唯一业务发现锚点。

## Fail-closed 证据

| 风险 | 证据 | 结果 |
| --- | --- | --- |
| 坏 prefix / foreign DueIndex member | `TestTheftRecoveryMalformedAndForeignMembersAreBoundedAndUntouched`、`TestRedisPairDiscoveryAdvancesPastBadPrefixAndOtherBusinessMember` | bounded cursor 消费页面，不删除 member、不调用 foreign loader |
| source manifest、mutable field 或 DueIndex 损坏 | `TestFriendPairRecoveryCorruptSourceOrDueIndexFailsClosed` | `ReadPairDispatchOrder` 返回 `friend.ErrFriendReconcile`，不构造可执行 order |
| payload/address/claim proof 伪造 | `TestTheftRecoveryRejectsPayloadHashForgeryAndAddressDrift`、`TestTheftRecoveryForgedClaimProofStopsBeforeAdvancer` | 不 claim/renew/advance；状态机记录 lease lost/invalid order |
| lease takeover 后旧 worker 写入 | `TestTheftRecoveryOldLeaseCannotCompleteAfterTakeover` | 旧 epoch complete 返回 fence mismatch |
| response loss / 重启 / retry exhaustion | `TestTheftRecoveryDispatchResponseLossUsesReceiptFirstAndCompletesOneWork`、`TestTheftRecoveryRestartPreservesRetryCountAndReclaimsSameWork`、`TestTheftRecoveryRetryExhaustionPersistsReconcileRequired` | receipt-first；retry count durable；无法证明成功时进入 reconcile |
| 真实 Friend Pair DueIndex/lease 物理接缝 | `TestFriendPairRecoveryUsesValidatedDueIndexAndSameSlotLeaseKeys` | 读取 authority DueIndex，claim/renew 更新其 lease score，complete 原子移除 member 并保存 result proof |
| 缺少 composition 依赖 | `TestFriendPairRecoveryAdaptersRejectMissingDependencies`、lease identity/bounds test | constructor/input 直接返回配置错误，不降级到内存或默认装配 |

上述 focused Redis 证据使用 Redis-compatible `miniredis` 驱动真实 `redis.UniversalClient` Lua/key adapter；没有把它表述为外部生产 Redis 或 TH3A E2E 证据。race 运行覆盖同一组 recovery/friend 包。

## 文件边界

本提交只包含：

- `internal/worker/theftrecovery/**`
- `internal/friend/redis_recovery.go`
- 本 handoff

没有提交 `cmd/farm-worker` 装配、TH3A Engine、Participant client、Actor routing 或 online Theft producer。integration 分支应在拥有 TH3A 合同和生成代码后，把这些端口接到现有恢复核心；接线不得改变 WorkAddress、source proof、stable identity、lease fence 或 receipt-first 语义。

## Verification

- `go test ./...` — PASS
- `go test -race -count=1 ./internal/worker/theftrecovery` — PASS
- `go test -race -count=1 ./internal/friend` — PASS
- `git diff --check` — PASS

最终提交 hash 由交付响应返回；本文件不把未来的 integration wiring 或生产 Redis probe 伪装成本轮证据。
