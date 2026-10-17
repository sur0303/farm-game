# W8A-L ActorAuthorityStore 逻辑合同交接

日期：2026-08-03
状态：`IMPLEMENTATION_PASS / PRODUCTION_ADAPTER_PENDING`

## 1. 本工作包完成内容

本包以 30、31 号合同为唯一语义基线，新增了与 Redis/Tcaplus 无关的 `ActorAuthorityStore` 逻辑层：

- `LoadAndClaim`、`Commit`、`FenceAndClaim`、`LookupIdempotencyOutcome` 四个独立接口；
- PLAYER/FARM `ActorAddress` 到 Authority Group 的唯一派生和可选断言校验；
- `HolderID + HolderBootID + AssignmentRevision + WriterEpoch + LeaseEpoch + ACTIVE + LeaseUntil`
  完整 ShardWriteFence；
- Actor `WriterClaimID` 与 `RecordVersion` 两层附加 Fence；
- Receipt 优先于 RecordVersion 的幂等预检；
- `COMMITTED / ALREADY_COMMITTED / IDEMPOTENCY_CONFLICT / RECORD_VERSION_CONFLICT /
  WRITER_EPOCH_MISMATCH / WRITER_CLAIM_MISMATCH / UNAVAILABLE_NOT_SENT / TIMEOUT_UNKNOWN`
  明确分类；
- `SUCCEEDED / REJECTED / NO_CHANGE` 确定性终态均可保存 Receipt 和 Outcome；
- 权威状态、Receipt、来源 AttachedOrder 和 PendingOutbox 在 InMemory Fake 的同一线性化点提交；
- AttachedOrder/Outbox 冲突、模型/Receipt/Order/Outbox/单次提交大小上限及失败原子性；
- Full Receipt 已过恢复窗口时返回 `RECOVERY_OVERDUE`，禁止把证据缺失误判为 `NOT_FOUND` 或重新执行；
- `FenceAndClaim` 生成稳定 `ResolutionReceipt`，并用新 Claim Fence 旧 Writer；
- `FaultStore` 可确定性制造 NotSent、Unknown-Before-Apply、Unknown-After-Apply；
- LoadAndClaim 独立 `CLAIM_UNKNOWN`，不安装半 Actor；
- `ACTIVE → QUIESCING → PASSIVATING → ABSENT` 两阶段钝化，只有 QUIESCING 可取消；
- 单 Actor `Persistence Lane` 串行化 SoftSave、Durable Barrier、Unknown 解析和 Drain；SoftSave 默认关闭；
- 有界 Activation Singleflight：100 个同 Actor 首次请求共享一个 Activation Identity/Future，等待者取消不会取消共享恢复。

## 2. 关键文件

```text
internal/storage/authoritystore/types.go
internal/storage/authoritystore/memory.go
internal/storage/authoritystore/fault.go
internal/storage/authoritystore/store_test.go

internal/logic/actor/activation/activation.go
internal/logic/actor/activation/singleflight.go
internal/logic/actor/activation/contract30_test.go
internal/logic/actor/activation/singleflight_test.go

internal/logic/actor/persistence/lane.go
internal/logic/actor/persistence/lane_test.go

internal/farm/actor/registry.go
internal/farm/actor/actor_test.go
```

## 3. 已证明的反例

测试明确覆盖：

1. 同一幂等键并发 100 次，只产生一次状态效果，其余返回 `ALREADY_COMMITTED`；
2. Receipt 已存在但重试携带旧 RecordVersion，仍返回原结果，不误报版本冲突；
3. Holder、Boot、AssignmentRevision、WriterEpoch、LeaseEpoch、LeaseDeadline、WriterClaim、RecordVersion
   任一不匹配均不能改变权威记录；
4. 容量超限时，CandidateState、Receipt、Order 均不出现部分提交；
5. Unknown-Before-Apply 没有存储效果；Unknown-After-Apply 可由原 IdempotencyKey 找回 Receipt；
6. LoadAndClaim Unknown-After-Apply 使用原 `claim_operation_id + new_writer_claim_id` 恢复为
   `ALREADY_CLAIMED`，不生成第三个 Claim；
7. Fence 先于旧 Commit 时，解析为 `NOT_COMMITTED`，迟到旧 Commit 被 Claim Fence 拒绝；
8. 原 Commit 先于 Fence 时，解析为 `COMMITTED`；之后的新 Writer 写入不会改变原 ResolutionReceipt；
9. Recovery Evidence 过期后，Lookup、Commit、Fence 均失败关闭；
10. QUIESCING 被新命令取消后，迟到 FinishPassivate 不会删除已恢复的 Actor；
11. Singleflight 中某个等待者 Deadline 到达只移除该等待者，不取消共享加载/恢复；
12. Persistence Lane 不允许 SoftSave、Durable Barrier、Unknown Resolution、Drain 交叠，也不允许低版本回调覆盖高版本。

## 4. 验证命令

```text
go test ./internal/storage/authoritystore \
  ./internal/logic/actor/persistence \
  ./internal/logic/actor/activation \
  ./internal/farm/actor -count=1

go test -race ./internal/storage/authoritystore \
  ./internal/logic/actor/persistence \
  ./internal/logic/actor/activation \
  ./internal/farm/actor -count=1

go vet ./internal/storage/authoritystore \
  ./internal/logic/actor/persistence \
  ./internal/logic/actor/activation \
  ./internal/farm/actor

go test ./... -count=1
git diff --check
```

上述命令在本分支均已通过。提交前还需执行一次 `go vet ./...`。

## 5. 明确未完成的边界

本包不能被解释为生产权威存储已经上线：

- `MemoryStore.SetShardWriteAuthority` 只是确定性测试缝；真实 Claim/Renew/Transfer/Lease 属于 W8C-L；
- `FaultStore` 只是故障模型；真实 Redis Lua、Repository、NOSCRIPT/MOVED/ASK、连接超时映射属于 W31-B/C；
- W9A-L 仍需用可控 R→F、F→R 竞态测试证明 FenceAndClaim 的真实线性化顺序；
- `MarkRecoveryOverdue` 只是压缩/归档测试缝；生产 Dedup Tombstone、Expired Evidence 和 Archive 属于 W31-B/C；
- Activation Singleflight 已有独立逻辑组件和并发测试，但旧 Farm Registry 仍使用 BlobStore，尚未接入
  `ActorAuthorityStore + LoadFutureGroup`；生产接线属于 W31-D；
- PlayerActor/FarmActor 当前生产路径尚未切换到新 Store，不能宣称资产和地块已经由新 Fence 保护；
- `GAP-ROUTE-HASH-PROFILE` 与 `GAP-REDIS-PLATFORM-EVIDENCE` 仍未关闭，禁止生成真实 auth-v1 资产数据；
- 200 人同场、热点农场、Redis Failover/RPO/RTO 不在本工作包证明范围内。

## 6. 下一步

```text
W9A-L  可控 FenceAndClaim 竞态与 ResolutionReceipt 证明
  ↓
W8C-L  ShardWriteAuthority Claim/Renew/Transfer/Lease Self-Fence
  ↓
关闭 GAP-ROUTE-HASH-PROFILE
  ↓
W31-B/C Redis Script Family、Repository、Unknown Resolver
  ↓
W31-D  Actor Registry + Singleflight + Persistence Lane 生产接线
```

接手者不得把当前 BlobStore 适配器静默包装成新 `ActorAuthorityStore`，也不得在缺少完整 Shard Fence
和 Unknown 解析时提前切换生产主链。
