# HV1 Farm typed `COMMIT_HARVEST` handoff

## 已完成的 Farm 侧能力

Farm Participant admission/dispatch 现在同时支持：

- `PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT`
- `PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST`

`COMMIT_HARVEST` 的入口先做 receipt-first 查询；未命中时才执行 WorkVersion、WorkLease、route/address/identity 和 Harvest dependency verifier，再进入 `FarmActor` mailbox。Actor turn 内重新校验 owner、farm/plot、cycle、expected plot version 和 `trusted_now_ms >= mature_at`。

成功提交在同一个候选记录和一次 authority CAS 中完成：

- `CropCycle` 结束为 `HARVESTED`，清空 Plot，并推进 Plot/Farm/Opportunity 版本；
- 由 Plant 时冻结的 CropRule、产量、经验、HarvestItem 和 Compendium snapshot 生成完整 `HarvestDecision`；
- `reward_quantity = base_yield - stolen_amount`，其中 legacy Theft 已提交的 `stolen_total` 会在 Harvest turn 中并入冻结 cycle 的结算事实；
- `CommitHarvestResult`、result hash 和带版本/状态校验的 ParticipantReceipt 一并持久化；
- 从同一候选记录构造一个 FarmChange，只有新建的 `APPLIED` 提交向 observer 交付一次。

`FarmRecord` 的 `CropCycles` 只保存 active cycles；因此完整 HarvestDecision 使用 namespaced `FarmOperationReceipt.result_payload` 中的 `FARM-COMMIT-HARVEST-RESULT-PROOF-V1` durable proof 保存，重放只解析该 proof，不读取已清空 Plot，也不重新广播。相同 Invocation + Hash 返回原始 typed result；不同 Hash 返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`。拒绝和 `NO_CHANGE` 只保存 receipt，不推进 FarmVersion、不生成 FarmChange。

## 集成接缝

`internal/farm/ingress/participant_server.go` 暴露：

- `CommitHarvestDependencyVerifier.VerifyCommitHarvestDependency`：必须证明 WorkRecord 的 owner/farm/plot/cycle/expected plot version 及对应冻结依赖；缺少物理证明时入口 fail closed；verifier 不得写 Farm authority。
- `Options.CommitHarvestDependency`：生产组合必须注入上面的 verifier；未注入时请求返回 `FailedPrecondition`。
- `Options.CommitHarvestObserver`：复用现有 FarmChange delivery adapter。observer 在 mailbox turn 返回后调用，负责 Gate/downlink；不得把 PlayerActor、库存、经验、任务或图鉴奖励放进 Farm 侧。
- `Options.NowMs`：提供受信时间来源；FarmActor 只在 turn 内捕获一次。

`CommitHarvestObserver` 未配置时兼容复用 `CommitPlantObserver`。集成者仍需在现有进程组合层接入真实 Work/lease/dependency verifiers 和 observer；本分支没有修改 `cmd`、路由控制面或 Workflow/Coordinator。

## 覆盖的测试

Actor 测试覆盖：happy path、完整 Decision/hash、未成熟、stale cycle/plot version、非 owner、同 Hash replay、Hash conflict、legacy Theft→Harvest 与 Harvest→Theft 顺序、receipt 容量和 `TimeoutUnknown` 后重放。

Ingress 测试覆盖：typed dispatch、一次 FarmChange observer、replay 静默、缺少 dependency proof fail closed，以及响应丢失/`TimeoutUnknown` 后重放不重复 observer。

本次验证：

```text
go test -count=1 ./internal/farm/domain ./internal/farm/actor ./internal/farm/ingress
go test -race -count=1 ./internal/farm/actor ./internal/farm/ingress
go vet -buildvcs=false ./internal/farm/...
git diff --check
```

以上命令均通过。
