# Care 集成可靠性收口 Handoff（2026-08-05）

基线：`27c89e1cab36a2a72423694a2e05b7abefad915e`。本次没有修改 `docs/design/**`，实现沿用 29/30/31/32/34 的冻结语义。

## 已闭合项

- `player-svr` authority 组合注入真实 `RedisCareDependencyVerifier`，使用同一 Authority Redis 的 SourceOrder、DueIndex、WorkLease 和同一 Farm bucket resolver。验证在 PlayerActor admission、mailbox turn 之前完成；Coordinator 只接受未带 Worker lease 的 `PENDING` Direct 证明，Worker 只接受 Redis TIME 裁决的 holder/boot/epoch/token/expire 证明。
- Verifier 绑定 CareRewardOrder 的类型、ID、Farm source、subject、operation、step、WorkAddress、canonical payload hash、冻结 `CareResolutionSnapshot` hash、DueIndex score 和 work version；缺配置、缺 verifier 或证明不完整均 fail closed。
- `TaskConfigRepository` 已由不可变、版本化的 `P0Repository` 生产装配，包含每日登录、好友访问、好友互动和成长收获占位 ID；P0 时区固定为服务端 `Asia/Shanghai`，不从请求读取。
- Authority Redis Complete/Reschedule/Claim/Renew 脚本使用 Redis `TIME`。Complete 在 Lua 内校验 holder、boot、lease epoch/token、work version、source identity、payload hash 和 Participant terminal result proof；过期旧 Worker 不能完成，新 Worker 可重新 Claim 后重放 Receipt 并完成。
- Worker 在调用 Player Participant 前校验 canonical Care payload hash；重试次数、next due、deadline、最大重试次数、错误类和 `RECONCILE_REQUIRED` 终点均落在 SourceOrder WorkRecord，不依赖进程内 map。
- DueIndex discovery 使用有界分页预算（不做全表扫描、不删除其他 Worker 可能合法拥有的成员），跳过坏 member、未知类型和其他业务类型，保留后续 Care 进度。
- Authority Care 入口只走 Farm Authority Incident/Order 路径；legacy Care 默认关闭，仅显式 `CARE_LEGACY_ROLLBACK_ENABLED=true`（或测试 setter）开启。好友护理先取得真实 FriendRelation/ActionPermit 并在 FarmActor turn 再验 permit，不能通过 OWNER/缺 permit 分支绕过。
- `farm-svr` 已装配共享 Authority SourceOrder/WorkLease 的 DirectDispatcher；队列只作快速提示，失败由 DueIndex Worker 接管。Player Participant Receipt 先于业务 turn 解析，重放不重复经验、好友访问/互动任务进度或奖励。

## 测试证据

聚焦测试已覆盖：Water/Pest Care、同 Incident 并发单次成功、SourceOrder 与 DueIndex commit、缺 verifier/伪造 Order/hash mismatch/过期 lease/旧 boot/错误 result proof、Worker payload hash 拒绝、重启后 retry 保留并进入 `RECONCILE_REQUIRED`、坏 member/非 Care 满页/跨页 Care、Friend Visit/Interact 同提交推进、跨日事实忽略、legacy 默认关闭和显式开关、Participant Receipt 重放。

本轮验收命令均通过：

```text
go test -count=1 ./...
go test -race -count=1 ./internal/farm/actor ./internal/farm/ingress ./internal/player/participant ./internal/player/task ./internal/worker/carerecovery ./internal/storage/redis/authority
go vet -buildvcs=false ./...
git diff --check
```

另外，聚焦阶段通过了 `go test -count=1 ./internal/player/participant ./internal/player/task`、`./internal/storage/redis/authority ./internal/storage/authoritystore`、`./internal/worker/carerecovery`、`./internal/farm/actor ./internal/farm/ingress ./internal/plantonline` 和 `./cmd/farm-svr ./cmd/player-svr`。

当前新增 Redis/Verifier 测试使用 `miniredis` 或测试 double；它们不是生产 Redis 证据。

## 下一任务必须执行的真实 Redis / E2E seam

- 使用公司真实 Redis（含目标版本/集群）验证 `TIME` 原子过期边界、Lua `EVALSHA`/`NOSCRIPT`/failover、同 hash-tag 同槽、脚本容量与 latency；观察 Complete 与 Claim、Reschedule 与 DueIndex 的原子结果。
- 启动真实 `farm-svr`、`player-svr`、`farm-worker` 多进程，验证 route move、holder boot 更换、旧 lease 过期、Worker 崩溃/重启、Participant 响应丢失和 Receipt replay；同时压测 Direct/Worker 竞争同一 Order。
- 验证真实 FriendRelation/ActionPermit 服务、Water/Pest 好友护理并发、同 Incident 的唯一成功；核对 Player 经验、`FRIEND_VISIT`、`FRIEND_INTERACT`、昨日 resolved_at 跨日投递和单 Authority Commit。
- 验证 Redis ACL、连接池错误分类、`TIMEOUT_UNKNOWN` 对账流程及 `RECONCILE_REQUIRED` 人工处理；确认真实 P0 配置发布/回滚不会让调用方覆盖时区或混用版本。

## 合同冲突与取舍

未发现需要改变冻结合同的冲突，也未修改设计文档。`InternalCallerContext` 按冻结 IDL 没有 `operation_id` 字段，因此 Worker admission 通过 SourceOrder 持久化的 operation 与确定性 invocation/step 绑定，而不新增字段或让 Worker 自报 operation。Direct 在 Coordinator 请求中不携带 Worker lease；为满足 Complete 的 Redis fence，Direct 在已取得 Participant terminal proof 后短暂 Claim 既有 WorkLease，仅用于有界、可恢复的 Complete，失败仍回到 DueIndex Worker。

为兼容既有非 Care `AttachedOrder` 调用，Redis adapter 对缺省 order payload hash 生成 raw-payload hash；Farm Care 生产路径始终写入 canonical Participant hash，Verifier/Worker 对 Care 缺失或不一致 hash fail closed。
