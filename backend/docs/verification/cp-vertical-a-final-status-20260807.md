# CP-VERTICAL-A Final Status — 候选真值 + 最终 Review + Merge Queue

日期：2026-08-07
作者：AI agent（session 续接）
状态：**READY_FOR_MERGE_QUEUE（业务候选已由 297044c 替代 5fe0f2e；H5 最终 commit 为 a189063）**

---

## 1. 候选 Commit 真值（最终登记）

```text
backend_previous_candidate = 5fe0f2e   # 已被服务端 Workflow 修复替代，不再是最终业务候选
backend_workflow_fix        = 297044c  # 真正改变产品 Workflow Runtime 的代码点（C1+C2）
backend_business_head       = 297044c  # 业务语义变化点
backend_verification_head   = 9eb71bc  # Handoff + 全量回归 Evidence
backend_merge_head          = 9eb71bc  # 合并分支可从此 Head 拉取（业务语义变化记录在 297044c）

h5_business_head            = a189063  # 最终 H5 commit（fe15152 + S1A 硬化）
```

H5 提交链：`fe15152`（PROBLEM A/B + PROCESSING 慢路径闭合 + FarmChange 原子性）
→ `a189063`（S1A 硬化：`data-pending-ops` 诊断 + `data-private-ready` / `data-join-phase` /
pending-cleanup 硬断言）。`a189063` 为干净、明确、含全部诊断与断言的最终 commit。

## 2. 提交链线性确认（全部通过）

```bash
git merge-base --is-ancestor 5b87799 297044c   # OK
git merge-base --is-ancestor 297044c 563ac91   # OK
git merge-base --is-ancestor 563ac91 9eb71bc   # OK
```

`5fe0f2e → 5b87799 → 297044c → 563ac91 → 9eb71bc` 线性无分叉。

## 3. 两个 worktree 干净确认

```bash
git -C <backend> status --porcelain   # 无输出
git -C <h5>      status --porcelain   # 无输出
```

- Backend worktree：`farm-game-premerge-a-final-20260806`，HEAD=`9eb71bc`
- H5 worktree：`classic-farm-h5-premerge-a-final-20260806`，HEAD=`a189063`
- Evidence 目录（`docs/verification/results/`）已按 `5fe0f2e` 的 dirty-check 排除规则处理为**有意提交或仓库外**，不依赖 Runner 自身排除。

## 4. 旧文档状态更新

`docs/verification/q1-smoke-bot-fix-and-h5-flake-handoff-20260807.md` 顶部已标记：

```text
status = SUPERSEDED
superseded_by = WF-WORKVERSION-PROOF-RACE Handoff（563ac91）
               + CP-VERTICAL-A Final Status（本文档）
```

旧文档"**H5 侧 1 个抖动缺陷未修复，阻断连续两轮 PASS 门槛**"为历史快照；
H5 刷新抖动已由 H5-REFRESH（purpose 分离）、PROBLEM A/B 与 S1A 硬断言修复并验收。

---

## 5. 最终代码 Review（窄范围：仅 297044c）

### 5.1 ErrStaleWorkProof 分类稳定性 — PASS（已补精确单测）

- `staleWorkProofError` 当前在 `internal/scenario/plant/rpc.go` 与
  `internal/scenario/harvest/rpc.go` **各一份重复实现**（复制粘贴，非共享 helper）；
  两处逻辑一致：`status.Code(err)==FailedPrecondition && strings.Contains(msg,"participant proof: WorkVersion")`
  → `errors.Join(workflow.ErrStaleWorkProof, err)`。
- 精确性核对：参与方 verifier 只有 `participantproof/verifier.go` 的
  `ErrWorkVersionMismatch = "participant proof: WorkVersion mismatch"` 命中该前缀；
  `identity mismatch` / `step "..." is not registered` / `...verifier is not configured`
  （`participant_server.go:366`、`player/participant/server.go:778`）均**不**命中，不会把其他
  FailedPrecondition 错当 stale proof。
- **本次补测**：`TestStaleWorkProofErrorClassification`（plant + harvest 各一）锁定
  正向唯一命中 + 非 FailedPrecondition code 不命中 + 5 类其他 FailedPrecondition 文本不命中。
- 更长期：建议下沉共享 helper 并改用 gRPC `ErrorInfo reason=STALE_WORK_PROOF`（同仓库
  单版本 RC 不做 Proto 扩张，登记为 WF1 后续）。

### 5.2 DirectDispatcher defer 锚点可靠 — PASS

- `dispatcher.go` Enqueue 契约明确：record 与 due index 在 enqueue **之前已 durable 提交**
  （"The caller must have already durably committed the record and its due index entry"）；
  队列满/进程崩溃/RPC 失败都不丢 work，recovery loop 从 due index 恢复。
- `dispatch()` 从 `repo.Get` 读 record（不存在则 ErrNotFound，不算 defer）；
  preflight `fenceWorkLease` 失败（dispatcher `fence.Owner==""`）或 persist
  `ErrRevisionConflict` → defer 时 **record 与 DueIndex 必然已存在**，worker 可重新 Claim。
- `TestCASLoserReloadsWithoutParticipantCall` 锁定：dispatcher 不偷 lease、不发 participant
  call、work 仍归 worker 且可恢复。不存在"Dispatcher 放弃但 DueIndex 尚未建立"。

### 5.3 reload/continue 有界 — PASS（指标未落地，登记后续）

- 约束：worker 路径受循环顶部 `stop()`（lease budget）与 context deadline；dispatcher 路径受
  `stepCtx` `ItemDeadline=5s`（`dispatcher.go:200`）。持续 stale proof 时 reload→continue 至多
  在本轮预算内发生，预算耗尽即 pause/返回，worker 从 DueIndex 恢复。
- `TestConvergenceWithStaleProofOnEveryStep`（8 轮 recover）收敛、无死循环、无 StepUnknown 残留。
- **差距登记**：当前仅有 6 个结构化日志点（`workflow_preflight_reload`、
  `workflow_preflight_dispatcher_defer`、`workflow_preflight_lease_lost`、
  `workflow_stale_proof_reload`、`workflow_persist_dispatcher_defer`、
  `workflow_persist_worker_reload`），**未落 Prometheus 指标**
  （`internal/workflow` 无 metrics 基础设施）。建议后续接入项目 grpcmetrics 模式，
  提供 `workflow_stale_proof_total` / `workflow_stale_proof_reload_total` /
  `workflow_dispatcher_deferred_total`。

### 5.4 修复覆盖范围 — 诚实登记（两处未覆盖）

**已覆盖（CP-VERTICAL-A 主链）**：Plant/Harvest **WorkerRecoveryInvoker**（worker 驱动路径）。
装配：`internal/worker/plantrecovery/recovery.go`、`internal/worker/harvestrecovery/recovery.go`
用 `NewWorkerRecoveryInvoker`；`staleWorkProofError` 在 plant 直接包装、harvest 在通用
RPC 分类器**之前**包装。

**未覆盖（差距，不泄漏 REJECTED，但比 reload 多一次 pause）**：

| 路径 | 装配点 | stale 包装 |
|---|---|---|
| Plant/Harvest **CoordinatorInvoker**（DirectDispatcher 快路径，`fence.Owner==""`） | `internal/plantonline/composition.go:104,108`、`internal/harvestonline/composition.go:81,85` | 无 |
| **Theft 全部路径**（CoordinatorInvoker + WorkerRecoveryInvoker） | `internal/theftonline/composition.go:85,89`、`internal/theftonline/worker.go:185,189` | 无 |

影响评估：C2 preflight（fresh-read + `fenceWorkLease`）已把 dispatcher 发 stale call 的概率
压到 preflight Get→Invoke 的极窄窗口（worker 需同时完成 claim+advance）；该窗口内若 CoordinatorInvoker
收到 `FailedPrecondition "participant proof: WorkVersion mismatch"`，engine 走 StepUnknown+pause，
worker 随后从 DueIndex 恢复并收敛——**不会泄漏公网 REJECTED**（未进入 REJECTED 分类），只是
较 reload 多一次 pause。Theft 不在 CP-VERTICAL-A 验收范围。

**登记结论**：可以宣称"**CP-VERTICAL-A 覆盖的 Plant/Harvest worker 驱动主链已关闭
WorkVersion race**"；不得宣称"所有 Workflow 已关闭"。后续（WF1 收敛）：把
`staleWorkProofError` 下沉为共享 Workflow/Participant RPC 层分类器，统一覆盖
CoordinatorInvoker + WorkerRecoveryInvoker + theft。

### 5.5 Public Outcome 单终态 — PASS

- `TestStaleProofDoesNotSurfaceAsRejected`：stale-proof reload 后所有 step 不记 REJECTED、
  收敛 COMPLETED。
- `TestWorkVersionMismatchReloadsInsteadOfRejecting`：无 StepUnknown、applyCount==1
  （reload 不重复 apply）。
- `TestParticipantCallUsesCommittedWorkVersion`：每次 participant call 携带 committed
  WorkVersion，不超最终提交版本。
- S1A 3×10 运行时证据：`public_store_cas_conflict=0`、`step_unknown_residual=0`、`due_residual=0`。
- 说明：public outcome 由 step 终态派生，engine 层证明已覆盖"同一操作不先 REJECTED 后 SUCCEEDED"；
  operation-result store 层面的直接断言可作为后续增强，不阻塞本次 RC。

---

## 6. Merge Queue（合并后需执行的验收）

### 6.1 Backend Merge Queue（以 9eb71bc 为 Head 合入最新 integration 新 worktree）

```bash
go test ./internal/workflow/... -count=1
go test -race ./internal/workflow/... -count=1
go test ./internal/scenario/plant/... ./internal/scenario/harvest/... -count=1
go test ./internal/player/participant/... -count=1
go test -buildvcs=false ./... -count=1
go build -buildvcs=false ./cmd/...
git diff --check
```

### 6.2 H5 Merge Queue（合入 a189063）

```bash
npm test -- --run
npx tsc --noEmit
npm run build
npm run audit:build
git diff --check
```

测试数不得低于当前最终记录。

### 6.3 合并后的运行证明

Workflow Runtime 已改变，合入最新 integration 后：

- 无手工冲突：S1A 10x 一轮 + 正常完整 Smoke 一轮 + S2/S3 一轮 4/4。
- Workflow / Participant / OperationResult / H5 CommandService 出现冲突则加码：
  S1A 2×10 + 正常 Smoke 2 轮 + S2/S3 2 轮。

## 7. 发布 Checkpoint（合并完成后回填）

```text
CP-VERTICAL-A
Backend:
  business_fix      = 297044c
  verification_head = 9eb71bc
  integration_head  = <最终 merge commit>
H5:
  candidate_head    = a189063
  integration_head  = <最终 merge commit>
Evidence:
  S1A 30/30
  normal smoke 2/2
  S2/S3 2 × 4/4
  public_store_cas_conflict = 0
  step_unknown_residual = 0
  due_residual = 0
```
