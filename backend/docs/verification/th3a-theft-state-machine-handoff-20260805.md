# TH3A · Theft 持久状态机交接

日期：2026-08-05
状态：IMPLEMENTATION_PASS（聚焦、scenario、全量测试与 vet 已通过）

## 1. 交付范围

本次只新增：

- `internal/scenario/theft/theft.go`：Theft root、确定性 workflow/settlement/attempt identity、三 Participant 状态机、WorkRepository/Engine composition、状态和最终结果投影；
- `internal/scenario/theft/rpc.go`：复用既有 Participant RPC 外壳的 Coordinator/Worker typed invoker，包含 route/step/invocation、deterministic protobuf、canonical payload/result hash 和 response-lost 分类；
- `internal/scenario/theft/publisher.go`：`ResultPublisher` 端口；本包不构造 Gate/Redis key；
- `internal/scenario/theft/theft_test.go`：内存 WorkRepository + fake Participant 崩溃点与并发/fencing 测试。

未修改 Farm/Player 业务、Friend Redis、cmd、`docs/design/**` 或既有 Workflow 文件。

## 2. 冻结状态机

```text
CREATED
  → FARM_DECIDING
  → FARM_DECIDED
  → VISITOR_SETTLING
  → VISITOR_SETTLED
  → OWNER_SETTLING       (仅 guard_coin_value > 0)
  → COMPLETED
```

终态投影另有：`REJECTED`、`NO_CHANGE`、`RECONCILE_REQUIRED`。状态由持久
`WorkRecord` 的 step payload/status/result 派生，不依赖进程内状态。

Farm `APPLIED` 是不可逆点。Farm 之后的每一个 payload 都从已持久的 Farm
Decision/Receipt 重建，包含原 settlement、attempt、visitor/owner/farm/plot/cycle
和结果 hash；不会根据当前 Farm/Player 状态重算。

## 3. 持久身份与 typed steps

`Create` 先构造并 `CreateIfAbsent`：

- `workflow_id = identity.WorkflowID`（THEFT + visitor subject + farm root + operation）；
- `settlement_id = identity.TheftSettlementID(workflow_id)`；
- `theft_attempt_id = identity.TheftAttemptID(farm, crop_cycle, visitor)`；
- root `DecideTheftPayload` deterministic protobuf + `contracts.CanonicalHash`；
- nested ActionPermit snapshot canonical hash、target-binding/Admission hash、
  `execution_accepted_at_authority_ms`、permit payload hash全部冻结；
- `WorkRecord.OperationID/PayloadHash` 与 deterministic step invocation 一起受
  `WorkRepository.CreateIfAbsent` 幂等冲突保护。

步骤固定为：

1. `DECIDE_THEFT` → Farm；
2. `SETTLE_THEFT_VISITOR` → visitor Player；
3. `CREDIT_GUARD_INCOME` → owner Player，仅当 Farm Decision 的
   `guard_coin_value > 0`。

每次调用都携带现有 `workflow.StepCall` 的稳定 invocation、当前 WorkVersion、
step payload hash 和 dependency result hash。RPC response 还校验 command、invocation、
deterministic typed result、Participant canonical result hash 与 committed record
version；不一致直接 fail closed。

## 4. 关键语义

- Farm 明确 `REJECTED` 或 `NO_CHANGE` 时立即持久终止，visitor/owner 均不调用；
- `net_received_quantity == 0` 仍执行 visitor step；
- `guard_coin_value == 0` 跳过 owner step，visitor receipt 完成后即可 `COMPLETED`；
- 只有 Farm Decision、visitor receipt，以及必要时 owner receipt 全部持久完成，
  `PublicResult` 才生成最终 `SUCCEEDED` `public.TheftResult`；中间均为 `PROCESSING`；
- response-lost 不改变 invocation/payload，generic Engine 将 step 置 UNKNOWN、释放 due，
  后续 direct/worker 以同 invocation 重放；fake receipt 命中时只返回原 result；
- not-sent transport 仅在 RPC adapter 内做有界一次重试；response-lost 不猜测是否执行；
- 单 invocation 默认最多 4 次尝试，deadline 仍由既有 Engine/WorkRepository 裁决，耗尽或
  hash/step/version 不一致持久为 `RECONCILE_REQUIRED`；
- 完成结果通过 `ResultPublisher` port 交给 composition 层，发布失败不回滚已成立的
  Participant/Work事实。

## 5. 故障与并发证据

`theft_test.go` 覆盖：

- CreateIfAbsent response loss：第一次已写入但返回错误，重试加载同一 record，变更 payload
  得到 idempotency conflict，且没有 Participant side effect；
- Farm、visitor、owner response loss：receipt 已落地后丢响应，下一次使用同一 invocation/
  payload 命中 replay，三步最终只产生一次业务效果；
- 重复推进、net=0、coin=0、Farm reject、Farm no-change；
- typed result hash、Farm Decision identity/hash 不一致时停止后续 Player 调用并进入
  `RECONCILE_REQUIRED`；
- bounded retry exhaustion；
- live worker lease 下 DirectDispatcher 被 `ErrLeaseLost` fencing，worker 使用同一
  WorkVersion/lease fence 完成且没有 Direct 重复调用。

## 6. 验证命令

已通过：

```text
go test ./internal/scenario/theft
go test -race -count=1 ./internal/scenario/theft
go test ./internal/scenario/...
go vet ./internal/scenario/theft
```

提交前还执行并通过：

```text
go test ./...
git diff --check
git status --short
```

## 7. 接线边界 / 后续

本提交没有修改 cmd 或线上 composition。生产侧只需把现有 WorkRepository、bucket resolver、
typed Coordinator/Worker Participant client 和 `ResultPublisher` 注入 `CoordinatorConfig`；
结果发布端口的具体目录/Redis 投影由外层 composition 实现。
