# CR2 PlayerActor Care Reward Handoff

基线：`52b30a67b532ee2344f723f75d6610ed86ec9a07`

## 本次 P1 范围

本变更继续使用既有 `APPLY_CARE_REWARD` Participant Receipt、PlayerActor
Authority Commit 和 Care 经验实现，补齐了来源权威证明与 TaskDomain 接缝。

### Care 来源证明

- `CareDependencyVerifier` 是显式、可注入的 Participant admission 依赖；未装配时使用
  rejecting verifier，Care 首次执行在进入 PlayerActor mailbox 前 fail closed。
- `CareDependencyProof` 必须绑定真实存在的 Farm Authority `CareRewardOrder`、
  `CARE_REWARD` 类型、Work/SourceAuthority、完整 `CareResolutionSnapshot`、snapshot
  hash、确定性 `CareRewardOrderID`、来源状态、DueIndex、invocation/step 和 fence。
  Coordinator 只能使用无 lease 的 coordinator proof；Worker recovery 必须带匹配的
  lease owner/epoch/expiry proof，并要求 verifier 显式证明 lease 仍有效。
- verifier 只在 `admitFirstExecution` 调用；PlayerActor turn 不持有 Farm client，也不
  跨服务读取 Farm。Participant Receipt 仍是同 invocation 重放的权威幂等边界。
- 本分支没有 CR3 的真实 Redis SourceOrder/DueIndex/Claim adapter，因此生产装配尚未
  接入真实 verifier。`cmd/player-svr` 保持缺失依赖的 fail-closed 装配；不得把现有
  Work verifier 视为 Care 来源证明。CR3 integration wiring 是显式未完成 seam。

### TaskDomain

- Care 路径已删除旧 `[]task.TaskInstance` 的硬编码 task ID/target/更新时间写入。
- FRIEND Care 在同一 Player Authority Commit 内调用 `TaskDomain.RecordFriendInteraction`：
  `occurredAtMs` 来自来源 Snapshot 的 `resolved_at_ms`，`nowMs` 来自本次可信 Player
  turn；周期由服务端统一配置解析，P0 默认 `Asia/Shanghai`。
- 新增版本化 `TaskDomainState` 物理 carrier，并由
  `internal/player/task/mapper.go` 做有界 mapper：固定三个 daily slot、current growth、
  配置/状态版本、`ClaimableCount` 和累计指标。mapper 不从旧 `PlayerRecord.Tasks` 回填，
  旧字段仅作为 Plant/Harvest 兼容 carrier。
- `PlayerRecord.TaskDomainState` 目前是 Registry 的兼容承载层；这是后续 W31 多 Model
  物理拆分的明确接缝，TaskDomain 语义不依赖旧 `[]TaskInstance`。
- TaskConfigRepository 或 TaskDomain 映射缺失时 Care fail closed；测试使用不可变 fake
  repository。OWNER Care 不触碰 TaskDomain。

## 覆盖测试

`internal/player/participant/care_test.go` 覆盖：

- 缺 verifier、伪造同 incident 的第二 OrderID、来源状态非法、来源 snapshot/hash 不匹配；
- 合法来源一次发奖、同 invocation/响应丢失/并发重放不重复增加经验或 TaskStateVersion；
- FRIEND Care 同批完成 `FRIEND_VISIT` 与 `FRIEND_INTERACT`，以及跨日延迟事实不污染新日；
- Repository 提供的 config version/task ID/ClaimableCount、OWNER 不推进好友任务；
- 服务实例使用统一服务端 timezone，不接受调用方 timezone；invocation/fence proof 失败；
- 缺少 TaskConfigRepository fail closed。

`internal/player/task/mapper_test.go` 覆盖有界 model:task round-trip、拒绝旧
`TaskInstance` fallback 和 malformed daily shape。

## 验证命令

本次交接应至少保留以下证据：

```text
go test -count=1 ./internal/player/participant ./internal/player/task
go test ./...
go test -race ./internal/player/participant ./internal/player/task
go vet ./...
git diff --check
```
