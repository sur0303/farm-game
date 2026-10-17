# FI1-A1 Farm CommitPlant Participant Handoff

日期：2026-08-04
平台：Linux `linux/amd64`，Go `go1.26.3-X:nodwarf5`
分支：`codex/fi1-a1-farm-commit-plant-20260804`
基线：`ac009ea888cf9d26b1f3fb0471d9a777921286ac`（PI1 已合入后的 integration checkpoint）

## 交付范围

本轮只接入 Farm Participant 的 `COMMIT_PLANT` 单写链：

- `internal/farm/ingress/participant_server.go` 提供 Coordinator/WorkerRecovery 两个共享 Participant 服务的注册描述；只接受 `COMMIT_PLANT`，其它命令 fail closed。
- `internal/farm/actor/participant.go` 实现 Farm 侧业务 payload 校验、冻结快照构造、确定性 CropCycle/Care Incident、ParticipantReceipt proof、typed result replay 与 FarmActor Turn。
- `internal/farm/actor/registry.go` 增加 Authority receipt/admission read、物理容量 Guard 与 Participant mailbox seam；仍复用现有 Registry、Binding、CAS、Unknown 和 DeliveryGuard。
- `internal/farm/actor/actor.go` / `delivery_guard.go` 让 Participant 在 CAS 前安装从同一候选记录构造的唯一 `CommittedFarmChange`，提交后复用该候选进入现有 FarmSubscriptionHub。

没有修改 `internal/contracts`、`api/proto`、`api/gen`、Redis/key/Lua、Coordinator/Worker、cmd 或其它 Participant；没有实现 Player Participant、Unlock、Harvest、Care、Theft。

## 关键语义

1. 共享 `contracts.ValidateAndDecodeWithOptions` 先完成 route/WorkAddress/identity/canonical payload/budget；随后在 mailbox 外完成 receipt-first、WorkVersion/WorkLease、Farm/Work/owner/target、ReserveSeed Result Hash、CropRuleSnapshot 版本与完整字段校验及 Authority 物理容量 admission。
2. FarmActor Turn 只做本地串行裁决；Plot locked/non-empty/version conflict 生成带 ParticipantReceipt 的持久 `REJECTED`/`NO_CHANGE`，不推进 FarmVersion、不发布 FarmChange。
3. 成功使用 Work 冻结的 CropRuleSnapshot；由受信 owner 写入 `planted_by_player_id`，生成 `mature_at`、整数十槽 2% steal plan、Care IncidentID/due_at、Compendium snapshot、decision/result hash。
4. 一次 CASEpoch 保存 FarmRecord/CropCycle、ParticipantReceipt proof、PlotVersion/FarmVersion 和 proof 中的 OpportunityHeader/dirty generation；现有 Authority Binding 同步写入通用 `StoredOperationOutcome`。由于冻结的现有 `FarmRecord` Proto 没有 Participant 专用快照/Header 字段，完整 typed result 与冻结运行快照使用 namespaced `FarmOperationReceipt.result_payload` 的 deterministic proof 持久化，未创建第二套 Authority store 或修改 Proto。
5. 相同 Invocation + Step Hash 从持久 Receipt 返回相同 typed result/result hash/record version；不同 Hash 返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`。Unknown、passivation/reload 和 WriterClaim 变化都以权威 Receipt/Version 为准，不重新随机、不生成第二 CropCycle。
6. PostCommitDeliveryGuard 在 CAS 前持有唯一候选变化；提交后只调用现有 `publish`/FarmSubscriptionHub。lane 或其它投递失败只有在 `InvalidateFarm` 与 `MarkFarmResyncRequired` 都成功后才清 Guard。TIMEOUT_UNKNOWN 的 COMMITTED 解析先 reload 权威记录，再消费 Guard 中的不可变 `FarmChange`；交接无定论时继续保持 UNKNOWN/Guard。
7. Authority 协调采用 `farm_id` 维度的短生命周期 gate：`Registry` 全局 mutex 只保护 gate 索引/ref 计数，不包围 AuthorityStore I/O 或 Actor turn；同一 farm 的 mailbox turn、mailbox 外 receipt/capacity read、Unknown resolve 共用一个 gate，不同 farm 使用不同 gate 并行，refs 归零后回收历史 farm 的 gate。外部 Participant admission 还登记 lifecycle ref，passivation/lease eviction 必须在同一 gate 内完成交接或拒绝删除。
8. mailbox 外 receipt/capacity admission 优先读取已激活 FarmActor 的不可变 Protobuf clone，不调用 `Binding.Get`/`LoadAndClaim`，不改变 Binding，也不等待同 farm 正在进行的慢 AuthorityStore I/O：gate 忙时立即 fail closed。冷 farm 可由 `RegistryOptions.ParticipantAdmissionSnapshotReader` 注入一个必须遵守 context 的有界只读 seam；当前 production composition 尚未接入该 seam，未激活且无 adapter 时明确 fail closed。最终 Capacity Guard 与 CASEpoch 仍在 Actor/Authority 主线复核。
9. UNKNOWN 解析阶段失去 Holder/写权时，legacy 与 Authority 两条路径统一复用 `evictFenced`；只有实时交接或 `InvalidateFarm` + `MarkFarmResyncRequired` 已证明成功后才移除 Actor。交接失败则保留 Actor、UNKNOWN 上下文与 Guard，返回 `ERR_STALE_ROUTE` 等可恢复结果等待后续重试。

## 测试与静态检查

本轮已通过：

- `GOCACHE=/tmp/farm-fi1-gocache go test -count=1 -run 'TestAuthorityCoordination' ./internal/farm/actor`
- `GOCACHE=/tmp/farm-fi1-race-cache go test -race -count=1 -run 'TestAuthorityCoordination' ./internal/farm/actor`
- `GOCACHE=/tmp/farm-fi1-gocache go vet ./internal/farm/actor ./internal/farm/ingress`
- `git diff --check -- internal/farm/actor internal/farm/ingress docs/verification/fi1-a1-farm-commit-plant-handoff.md`

本轮按要求执行的完整定向 test 命令：

- `GOCACHE=/tmp/farm-fi1-gocache go test -count=1 ./internal/farm/actor ./internal/farm/ingress`：`internal/farm/ingress` 通过；actor 的三个既有 Redis 变体用例 `TestAuthorityFarmEmptyActivation`、`TestAuthorityFarmPlantCommitAndReplay`、`TestAuthorityFarmColdLoadRecovery` 因当前沙箱禁止 `127.0.0.1:0` socket（`operation not permitted`）失败，非 FI1 断言失败。
- `GOCACHE=/tmp/farm-fi1-race-cache go test -race -count=1 ./internal/farm/actor ./internal/farm/ingress`：同一受限 miniredis socket 原因；ingress race 通过，新的协调 race 测试通过。

上一轮已通过的仓库/目标包检查仍保留在本 handoff 的历史记录中；本轮没有重新运行全仓检查或全仓 `diff --check`。

此前已完成、本轮未重跑的检查包括：目标包普通/race test、`go vet`，整仓 test/race/vet/build，`scripts/gen-proto.sh` 后 `api/**` 零差异，以及当时的静态 diff 检查。

为排除本地 socket 限制后的目标包回归，本轮另以 `-skip` 排除上述三个 miniredis 用例运行：

- `GOCACHE=/tmp/farm-fi1-gocache go test -count=1 -skip 'TestAuthorityFarmEmptyActivation|TestAuthorityFarmPlantCommitAndReplay|TestAuthorityFarmColdLoadRecovery' ./internal/farm/actor ./internal/farm/ingress`：通过。
- `GOCACHE=/tmp/farm-fi1-race-cache go test -race -count=1 -skip 'TestAuthorityFarmEmptyActivation|TestAuthorityFarmPlantCommitAndReplay|TestAuthorityFarmColdLoadRecovery' ./internal/farm/actor ./internal/farm/ingress`：通过。

覆盖向量包括：成功一次提交、同 Invocation replay/conflict、locked/non-empty/version rejection、前置 Unknown 与提交后响应丢失、pending Unknown 即时重试、并发 resolver 单成功、passivation/reload、冻结快照与一百次 Incident replay、Plot1/Plot24/边界与重复调用、同地块并发单成功、稳定容量/非容量错误、mailbox 前依赖/预算拒绝、已激活 snapshot 不触发 LoadAndClaim、冷 admission fail closed、慢 Authority gate 不等待、admission lifecycle blocker、同 farm gate 串行与 refs 回收，以及 farm A 权威访问阻塞时 farm B Participant turn 继续执行。
本轮新增回归还覆盖：`TIMEOUT_UNKNOWN-after-apply` 后在解析阶段失去写权时，Authority 主线与 legacy 主线均先完成 Guard fence，再允许 Actor eviction；未完成 fence 时不会直接 `removeActor`。

## 未验证及 Pending

- 真实 Redis AuthorityStore、Redis key/Lua、mTLS Work/Farm verifier、production cmd/composition root 尚未在本任务接入；测试中的 Memory/Fault store 只用于测试，不能视作 production proof。
- 生产 WorkRecord/Farm dependency verifier 需由后续 composition 注入；当前缺失 verifier 时 Server 默认拒绝。
- `ParticipantAdmissionSnapshotReader` 只定义在 Farm Registry 内作为最小 Port；当前 composition 未提供冷 farm 的生产只读实现，因此冷 admission 不会伪造成功，也不会回退到 `LoadAndClaim`。
- 当前执行环境禁止 miniredis 绑定本地临时端口，故本轮完整 actor target test/race 的既有 Redis 变体未能在此环境完成；需在允许本地 socket 的 Linux runner 重跑相同两条命令。
- 仅新增一个最小 `internal/farm/actor` Port：`ParticipantAdmissionSnapshotReader`，用于未来 composition 注入冷 farm 的有界只读快照；没有修改合同、Proto 或其他公共路径。PI1 的共享 ingress/canonical/hash/result/receipt 与现有 Authority Binding/Registry 接缝仍承担本写集，完整运行事实采用现有 Receipt proof carrier，待未来 Proto/物理 Schema 正式冻结时再做兼容迁移。

上一轮远端实现 tip：`a8c470ecd1c5f9f3b9b982a3b720c2ba41012a3b`；本轮按控制器要求未 commit/push，按 farm_id 隔离的 Registry 修复与本测试追加保留在当前 worktree，待上层控制器纳入后续提交。本 handoff 与实现保持在上述 owned write set 内。
