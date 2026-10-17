# W8C-L · ShardWriteAuthority 逻辑实现交接

日期：2026-08-03
状态：`COMPLETED_LOGIC_PASS / REDIS_LUA_AND_RUNTIME_WIRING_PENDING`

## 1. 本包证明什么

本包按 30 §8 和 31 §3 实现存储无关的 Shard 数据面写权状态机，并与 W8A-L 的 Actor Commit 共用
`MemoryStore` 同一线性化锁。已经证明：

- RouteManager 才能执行 Claim、Transfer 和 Unassign；当前 Holder 只能执行 Renew；
- Claim 只允许记录不存在、`UNASSIGNED` 或 `ACTIVE` 但已到期的 Shard；
- Renew 必须匹配 Holder、Boot、AssignmentRevision、WriterEpoch、LeaseEpoch，且不能复活已过期 Lease；
- Transfer 原子更换 Holder/Boot，并同时推进 WriterEpoch、LeaseEpoch、AssignmentRevision；
- Shard Transfer 与 Actor Commit 只有一个存储顺序：旧 Commit 先到则可提交，Transfer 先到则旧 Commit 必须拒绝；
- 相同 `transition_id` 在提交后响应丢失时可安全重试，不会二次推进 Epoch；
- Holder 使用请求发出前的本地单调时钟起点和 `accepted_ttl_ms - safety_margin` 建立保守 Deadline；
- Deadline 到期后同时禁止新写命令、Actor 激活和新 Actor Turn；观察到更新的 Holder 元组时立即 Fence。

ActorRecord 的 `writer_claim_id + record_version` 没有被 Shard Lease 取代。合法强写仍要求：

```text
ShardWriteAuthority完整Fence
+ Actor writer_claim_id
+ Actor record_version
+ Idempotency Receipt
```

## 2. 改动文件

```text
internal/storage/authoritystore/types.go
internal/storage/authoritystore/memory.go
internal/storage/authoritystore/fault.go
internal/storage/authoritystore/shard_authority.go
internal/storage/authoritystore/lease_guard.go
internal/storage/authoritystore/shard_authority_test.go
```

主要类型：

```text
ShardWriteAuthorityStore
├── ClaimShardAuthority
├── RenewShardAuthority
├── TransferShardAuthority
└── UnassignShardAuthority

HolderLeaseGuard
├── InstallGrant(request_started_monotonic, accepted_ttl)
├── CanAdmitWrite
├── CanActivateActor
├── CanStartActorTurn
└── ObserveAuthority / Fence
```

`TrustedShardAuthorityCaller` 只表示已经由服务端适配层认证的调用身份，不是客户端可提交的 role 字段。
生产适配器仍必须使用 mTLS、Workload Identity 或网络 ACL 构造它。

## 3. 状态与换代语义

数据面只持久化 `UNASSIGNED/ACTIVE`，不把控制面的 `DRAINING/ASSIGNING` 写进 Authority Record。

| 操作 | 前置条件 | WriterEpoch | LeaseEpoch | AssignmentRevision | 结果 |
|---|---|---:|---:|---:|---|
| Claim | 不存在、UNASSIGNED 或 ACTIVE 已到期 | +1 | +1 | +1 | ACTIVE(new holder) |
| Renew | 当前 ACTIVE 完整 Fence，且未到期 | 不变 | 不变 | 不变 | 只延长 expire_at |
| Transfer | 当前 ACTIVE、未到期、旧完整 Fence | +1 | +1 | +1 | ACTIVE(new holder) |
| Unassign | 当前 ACTIVE、旧完整 Fence | +1 | +1 | +1 | UNASSIGNED |

31 号只冻结了 Unassign 的调用权限和终态，没有逐字段展开。实现采用与 30 号“Assignment 状态换代必须
先切换数据面写权”一致的保守语义：Unassign 推进三种代次、清空 Holder/Boot/Expiry，确保旧 Holder
不能续租或迟到写。重复的新 Unassign 不继续空转推进 Epoch；只有原 `transition_id` 重试返回
`ALREADY_APPLIED`。

TTL 使用毫秒输入并受 `LeaseLimits` 约束。Renew 取 `max(old_expire_at, trusted_now + accepted_ttl)`，
不会因为短 TTL 请求缩短当前有效 Lease。

## 4. Unknown 与幂等

`FaultStore` 新增四种 Shard Authority 方法的传输边界故障注入：

```text
TIMEOUT_UNKNOWN_BEFORE_APPLY
→ Store未变化
→ 同一请求重试首次APPLIED

TIMEOUT_UNKNOWN_AFTER_APPLY
→ Authority已经换代
→ 同一transition_id重试ALREADY_APPLIED
→ WriterEpoch不再次增加
```

幂等内容不绑定某个 RouteManager 进程实例。控制面故障切换后，新的已认证 RouteManager 可以使用同一
稳定 `transition_id` 接管重试；若同一 ID 对应不同 Holder、Fence 或 TTL，则返回
`RECONCILE_REQUIRED`。

## 5. 本地 Deadline

Holder 必须在发送 Claim/Renew/Transfer 请求之前记录 `request_started_monotonic`。Guard 使用：

```text
deadline = request_started_monotonic
         + accepted_ttl
         - safety_margin
```

不能在收到响应后重新从完整 TTL 起算，否则网络延迟会令本地 Lease 超过 Store 真值。响应到达时若保守
Deadline 已过，Guard 立即进入 `FENCED`。旧 Revision 的迟到回调不能覆盖新 Grant；同 Revision 但
Holder/Boot/Epoch 不一致被视为损坏元组。

## 6. 验证证据

聚焦测试覆盖：

- Claim 幂等、RouteManager 故障切换重试、活跃 Lease 不可强抢；
- Renew 不推进代次、错误 Holder 拒绝、到期不可复活；
- Transfer/Unassign/Reclaim 完整换代；
- 100 并发相同 Claim 和 Transfer：恰好一次 `APPLIED`，其余 `ALREADY_APPLIED`；
- 100 个不同 Holder/transition 同时 Claim：恰好一个 `ACTIVE`，其余 `ACTIVE_LEASE_CONFLICT`；
- E8 Commit → E9 Transfer：Commit 与 Receipt 保留；
- E9 Transfer → 迟到 E8 Commit：`WRITER_EPOCH_MISMATCH` 且不存在 Receipt；
- Unknown Before/After Apply；
- Deadline、续租延长、迟到响应、旧 Grant、显式 Drain Fence 与 Transfer 观察 Fence；
- 更高 Revision 但 Epoch 回退的损坏元组 fail-closed，且不能降低本地比较基线。

执行结果：

```text
go test ./internal/storage/authoritystore                 PASS
go test -race ./internal/storage/authoritystore           PASS
go vet ./internal/storage/authoritystore                  PASS
go test ./...                                             PASS
go vet ./...                                              PASS
git diff --check                                          PASS
```

## 7. 明确未完成

本包不是生产 Redis 完成证明，以下仍由后续包负责：

- W31-B/C：Redis `TIME`、Hash Tag 同槽、小 Lua、EVALSHA/NOSCRIPT、MOVED/ASK、真实 Timeout Unknown；
- W31-D：把 HolderLeaseGuard 接到 RouteIngress、ActorRegistry、Mailbox Turn 与 Drain/Passivation；
- RouteManager、Assignment 发布和 Ready/Handoff 控制面；
- mTLS/Workload Identity/ACL 的真实身份适配；
- `GAP-ROUTE-HASH-PROFILE` 关闭前的 VirtualShard 上界和最终 HashSeed；
- TTL、SafetyMargin、Renew 周期、单 Pod Shard 数量的真实配置与压测；
- Lease/Transition 指标、告警与双 Pod 故障演练。

`SetShardWriteAuthority` 仍只是旧测试的确定性注入 seam，生产代码不得绕过
`ShardWriteAuthorityStore` 状态机。
