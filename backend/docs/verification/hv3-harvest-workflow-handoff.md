# HV3 Harvest Workflow handoff

本轮建立了 Harvest 的 generic WorkRecord 主干，但没有接入进程入口、FirstHop、FarmActor、PlayerActor 或 Participant 实现。后续集成只需要把 typed 入口和现有 generic repository/route 装配到以下边界。

## 已冻结的主链

`HarvestInput` 在可信入口处必须已经包含 `subject_player_id`、`operation_id`、自有 `farm_id`、`plot_id`、`crop_cycle_id` 和 `expected_plot_version`。`internal/scenario/harvest` 以 `HARVEST + operation_id + Farm root` 派生稳定 workflow ID，并以 deterministic `CommitHarvestPayload` 生成 root payload/hash。

online 入口先调用 `harvestonline.Coordinator.CreateOrLoad`，完成独立 Harvest bucket placement 与 `CreateIfAbsent`；只有拿到 WorkRecord 后，才能调用 `Start`/`Resume`。generic Engine 只推进：

```text
CREATED
  -> COMMIT_HARVEST(Farm)
  -> 保存 Farm typed outcome/result_hash
  -> GRANT_HARVEST_REWARD(Player)
  -> 保存 Player typed outcome
  -> COMPLETED
```

Farm 的 `HarvestDecision` 是唯一 reward quantity、exp、compendium 语义来源。Coordinator/Worker 只构造 Grant payload、绑定 Farm result hash 并转发冻结 Decision；不得自行计算或重建 reward。

Farm `REJECTED`/`NO_CHANGE` 是可查询终态且不调用 Player。Farm 已成功而 Player 尚未完成时，`harvestonline.StatusForRecord` 映射为 `PROCESSING`。成功公开结果由 `harvestonline.PublicResult` 从 WorkRecord 中的 Farm/Player typed result 解码，不使用第二结果库。

HV3 P1 修正：Player Grant 的 `RECONCILE_REQUIRED` 或其他未证明成功的拒绝在 Harvest Participant 边界转为同 invocation 的 `UNKNOWN` 重放；只有明确的 Participant 幂等冲突才形成稳定 `REJECTED/IDEMPOTENCY_CONFLICT`。Participant RPC 对幂等冲突、明确 `UNAVAILABLE_NOT_SENT` 和 response-lost 分层处理；明确未发送会先以原 invocation 做安全重试。

## 后续组合接缝

### Proof / typed Participant

- Farm Participant 应实现已登记的 `COMMIT_HARVEST`，以 `CommitHarvestPayload` 的 `owner_player_id/farm_id/plot_id/crop_cycle_id/expected_plot_version` 做权威校验。
- Farm 成功时原子写入业务变更、receipt/outcome 和 `CommitHarvestResult`；重复 invocation 必须从同一 receipt 返回原结果。拒绝必须返回可持久化的 `REJECTED` 或 `NO_CHANGE` 结果，不产生 Player 调用。
- Player Participant 应实现已登记的 `GRANT_HARVEST_REWARD`，只消费 Grant payload 中的 `commit_harvest_result_hash` 与冻结 `HarvestDecision`，原子写 inventory/exp/harvest count/compendium、receipt/outcome。不得重新计算 reward。
- 两者的 typed payload/result、canonical participant hash、route domain 必须继续通过 `internal/contracts` catalog 校验。接入方将具体 `ExecuteStep`/`ResumeStep` client 提供给 `harvestonline.Config` 或 `harvestrecovery.Config`。

### Coordinator composition

- 可信 FirstHop/gate 将 admitted command 映射为 `harvest.HarvestInput`，调用 `harvestonline.New` 生成的 Coordinator。
- 在线请求必须把 `CommandResult.Record` 作为同 operation 的查询/重试锚点；重复 operation 重新 `CreateOrLoad`，不能另造业务 operation 或另存结果。
- `CommandResult.Status` 的 `SUCCEEDED`、`PROCESSING`、`REJECTED`、`NO_CHANGE`、`UNAVAILABLE` 由 durable WorkRecord 推导。`PROCESSING` 不代表 Farm 尚未执行，也不允许公网层直接重试为新 operation。
- 当前实现故意没有修改 `cmd/*`、FirstHop、Gate 或既有 Plant composition；集成者需在这些既有入口中装配本包，不应把 Harvest 命令加入 `plant` 包。

### Placement / route

- `harvest.RootIntent.AddressForResolver` 使用独立 Harvest Work bucket；Worker/online 两侧必须使用同一 `BucketResolver`、bucket version 和 WorkAddress。
- `harvestonline.CoordinatorRouteProvider` 与 `harvestrecovery` 的 route provider 只接受 Harvest 两个 command，并分别校验 Farm/Player route domain、actor ID 与当前 assignment fence。
- 真实 Redis/placement 接入点是 `harvestonline.NewPlacementResolver` 返回的 `workidentity.BucketResolver`；本轮没有扫描 Redis 全库，也没有新增 MQ、Hook 或文件轮询。

### FirstHop / public result / E2E

- FirstHop 需要补齐：trusted context、operation/idempotency admission、HarvestInput 构造和调用 `CreateOrLoad` 的 wiring；它不应直接调用 Farm/Player Participant。
- public result reader 需要按相同 `subject_player_id + operation_id + Farm root` 重建 workflow ID，并从 generic repository 读取 `HARVEST` WorkRecord 后调用 `StatusForRecord`/`PublicResult`。
- E2E 需要验证：WorkRecord 先于 Participant effect；Farm unknown 的同 invocation receipt replay；Farm 完成后 Player pending 的 PROCESSING；Player unknown replay；terminal replay 不再触发 Participant；payload hash 冲突返回 idempotency conflict；lease takeover 不改 WorkAddress、payload、result hash 或 invocation ID。
- P1 定向 E2E 还需验证：Player `REJECTED/RECONCILE_REQUIRED` 不进入 FAILED、Worker 继续同 invocation；`PARTICIPANT_IDEMPOTENCY_CONFLICT` 公开为稳定 `REJECTED/IDEMPOTENCY_CONFLICT`；同 operation 不同 canonical payload 在 Online 入口直接返回公开冲突且不 Resume。
- FirstHop、proof、真实 route/placement、Actor handler 和 `cmd/farm-worker` 的装配尚未在本轮修改，属于下一轮 composition/E2E 范围。

## 本轮文件边界

- `internal/scenario/harvest/`：canonical root/input、稳定 step/invocation identity、typed payload/result、状态决策、RPC adapter。
- `internal/harvestonline/`：CreateOrLoad、Coordinator online replay/status projection、Harvest route/placement adapter。
- `internal/worker/harvestrecovery/`：HARVEST handler、claim/lease deadline adapter、`ResumeStep` recovery composition 与 takeover tests。
- `docs/verification/hv3-harvest-workflow-handoff.md`：本交接说明。

未修改 Plant 文件、generated/proto、Actor/Participant、`cmd`、设计合同、agentctl；没有 commit/push。
