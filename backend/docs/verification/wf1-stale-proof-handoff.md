# WF1-STALE-PROOF-UNIFICATION — Handoff

日期：2026-08-07
分支：`codex/cp-wf1-stale-proof-20260807`
基线：`449bcb2`（CP-WF1-STALE-PROOF 独立 checkpoint，不依赖 CP-VERTICAL-A merge queue）

## 一、任务卡

把 plant/harvest 上点状修复的 stale work proof 处理（`297044c` / `0f49c7d`）统一收敛为
workflow 引擎层**单一原语**，并覆盖此前未覆盖的三条路径：

1. **Worker 恢复路径**（plantrecovery / harvestrecovery）：`NewWorkerRecoveryInvoker`
   此前完全没有 stale proof 分类，`FailedPrecondition "participant proof: WorkVersion"`
   会落 `INVOCATION_UNKNOWN` → StepUnknown pause。
2. **Theft 路径**：`theftonline` 的 Coordinator invoker 与 `NewWorkerRecoveryInvoker`
   此前同样未覆盖。
3. **同一 invocation 不重复 reserve/grant**：stale reload 必须复用原 invocation ID +
   payload，receipt 幂等保证 apply 恰好一次。
4. **decision persist 冲突不回归**：并发双驱动（DirectDispatcher + worker）下
   `ErrTerminal` 与 `ErrRevisionConflict` 一律不泄漏给调用方。

## 二、改动

### 共享分类器（唯一原语）

`internal/workflow/stale_proof.go`（新增）：

```go
func ClassifyStaleWorkProof(err error) error
```

- `grpc status.Code(err)==FailedPrecondition` 且消息含 `"participant proof: WorkVersion"`
  → `errors.Join(ErrStaleWorkProof, err)`；
- 其余错误原样返回（nil→nil，绝不误吞）。

`ErrStaleWorkProof` sentinel 仍在 `internal/workflow/engine.go` L30（引擎 reload-on-stale
逻辑 L325-334 未动）。workflow 包首次引入 `google.golang.org/grpc/codes` + `status`
依赖（go.mod 已有 grpc v1.82.1，无循环）。

### 三场景接线

| 位置 | 改动 |
|---|---|
| `internal/scenario/plant/rpc.go` | 删私有 `staleWorkProofError`（修剪 import）；`RPCInvoker.Invoke` 错误路径补 `workflow.ClassifyStaleWorkProof(err)`（Coordinator/DirectDispatcher 快路径与 worker 路径共用）；`WorkerRecoveryInvoker` 改调共享函数 |
| `internal/scenario/harvest/rpc.go` | 删私有 `staleWorkProofError`（RouteProvider 保留）；两个 invoker 均 **stale-first**（先 `ClassifyStaleWorkProof` 再 `participantRPCErrorResult`） |
| `internal/scenario/theft/rpc.go` | 两个 invoker（Coordinator L225-247、WorkerRecovery L249-281）均加 **stale-first** 包装（否则 stale 的 FailedPrecondition 被保守分类当 response-lost → StepUnknown pause） |

### 测试

- `plant_test.go` L1395 与 `harvest/rpc_test.go` L118 的分类测试改测
  `workflow.ClassifyStaleWorkProof`（正反样例保留）。
- `internal/scenario/theft/theft_test.go`：`fakeParticipant` 加 `staleOnce`（按 invocation
  one-shot 返回 `workflow.ErrStaleWorkProof`）；新增 Coordinator 双驱动与 worker-lease
  双驱动两个测试；`assertTheftSingleApply` 按 participant→step 索引绑定断言恰好一次 apply。
- `internal/scenario/theft/rpc_test.go`（新增）：`NewCoordinatorInvoker` 与
  `NewWorkerRecoveryInvoker` 的 stale 包装锁定（FailedPrecondition→ErrStaleWorkProof，
  且 idempotency conflict 仍落 `StepOutcomeRejected` 不误吞）。
- `internal/worker/plantrecovery/recovery_test.go`：`workerSpy` 加 `staleOnce`（one-shot
  raw gRPC FailedPrecondition，让 RPC 适配层分类）；新增
  `TestPlantRecoveryStaleWorkProofReloadsSameInvocation`（RESERVE_SEED stale 一次 →
  reload → 同 invocation 同 payload 二次调用 → StepApplied，farm 恰好 1 次，无
  StepUnknown/REJECTED）。
- `internal/worker/harvestrecovery/recovery_test.go`：`recoveryParticipant` 加 `staleOnce`；
  新增 `TestHarvestRecoveryStaleWorkProofReloadsSameInvocation`（Farm COMMIT_HARVEST stale
  一次 → reload 同 invocation → 两步均 StepApplied/COMPLETED，Player GRANT 恰好 1 次）。
- `internal/workflow/wf_workversion_race_test.go` 新增：
  - `TestStaleProofReloadKeepsSameInvocationIdempotent`：reload 复用同 invocation ID，
    apply 恰好一次；
  - `TestConcurrentDualDriverResumeConvergesDeterministically`：两个独立 Engine 实例
    （dispatcher 无 lease + worker 持 lease）并发 Resume 同一 record，确定性收敛
    COMPLETED、无 StepUnknown/REJECTED、每 invocation apply==1。

### 并发测试暴露的引擎修复

`internal/workflow/engine.go` `persistWithRaceHandling`：

- 并发双驱动下，一方把 record 推进到 terminal 时，另一方的 decision persist 命中
  `ErrTerminal`（repository 的 `Advance` 先查 `Terminal()` 再查 `WorkVersion`），原代码
  只处理 `ErrRevisionConflict` → `ErrTerminal` 泄漏给调用方。
- 修复：`ErrTerminal` 与 `ErrRevisionConflict` 同路径 —— dispatcher defer（nil 返回，
  worker 已收尾）、worker reload（循环在 terminal state 退出）。绝不泄漏。

## 三、已提交

```text
91259f6 refactor(workflow): unify stale work proof classifier（step 1）
7c26bef  test(theft): cover stale WorkVersion proof on coordinator and worker paths（step 2）
e416c95  test(theft): lock stale-proof classification in both RPC invokers（step 3）
ab94fac  test(plantrecovery): stale WorkVersion proof reloads same invocation（step 4）
751cc06  test(harvestrecovery): stale WorkVersion proof reloads same invocation（step 5）
fbd6901  test(workflow): same-invocation idempotency and concurrent dual-driver convergence（step 6）
```

## 四、验收命令（6 组，原样输出）

```bash
# 1. 全量构建
go build -buildvcs=false ./...

# 2. workflow 引擎全量（含新并发/幂等测试）
go test ./internal/workflow/... -count=1
go test -race ./internal/workflow/... -count=1

# 3. plant/harvest/theft scenario 全量
go test ./internal/scenario/plant/... ./internal/scenario/harvest/... ./internal/scenario/theft/... -count=1

# 4. worker 恢复路径（plantrecovery / harvestrecovery）
go test ./internal/worker/plantrecovery/... ./internal/worker/harvestrecovery/... -count=1

# 5. 全仓测试
go test -buildvcs=false ./... -count=1

# 6. vet + diff 卫生
go vet ./internal/workflow/...
git diff --check
```

## 五、测试清单（新增，全部确定性、无 Redis 依赖）

| 测试 | 路径 | 断言要点 |
|---|---|---|
| TestPlantRecoveryStaleWorkProofReloadsSameInvocation | plantrecovery | RESERVE_SEED stale→reload→同 invocation/payload 二次调用→StepApplied；farm 1 次；stats.Plant.Completed==1 |
| TestHarvestRecoveryStaleWorkProofReloadsSameInvocation | harvestrecovery | COMMIT_HARVEST stale→reload→同 invocation；两步 StepApplied/COMPLETED；Player 1 次；无 error class 泄漏 |
| TestTheftStaleProofCoordinatorReloadsAndCompletes | scenario/theft | Coordinator.Resume 全步骤 staleOnce→COMPLETED、无 StepUnknown/REJECTED、每步恰好 1 stale+1 success |
| TestTheftStaleProofWorkerLeaseReloadsAndCompletes | scenario/theft | repo.ClaimDue+Engine.Resume+LeaseFence 同模式 |
| TestTheftRPCStaleWorkProofWrapsBeforeGenericClassifier | scenario/theft | 两个 RPC invoker：FailedPrecondition→ErrStaleWorkProof；idempotency conflict 不误吞 |
| TestStaleProofReloadKeepsSameInvocationIdempotent | workflow | reload 复用同 invocation ID；apply 恰好 1 次 |
| TestConcurrentDualDriverResumeConvergesDeterministically | workflow | 双 Engine 并发→COMPLETED、无 StepUnknown/REJECTED、apply==1 |

## 六、未完成项（诚实登记，均不阻塞本 checkpoint）

1. **metrics 未落地**：`plantrecovery/metrics.go`、`theftonline` 等未补 stale-proof 专项
   指标（沿用既有 `workflow_stale_proof_reload` log 事件）。
2. **ErrorInfo reason 未接**：`api/proto` 未触碰（约束），FailedPrecondition 的
   ErrorInfo reason 未加；共享分类器按消息文本匹配，若 proto 后续引入结构化 reason
   需同步扩展 `ClassifyStaleWorkProof`。
3. **Redis 租约路径**：worker-lease 双驱动测试用 `MemoryWorkRepository`；
   `internal/worker/theftrecovery/redislease.go` 的真实 Redis 租约未在本次覆盖
   （同既有 theftrecovery 测试模式，不引入 Redis 依赖）。

## 七、回滚方式

- 引擎修复（ErrTerminal 处理）回滚：`git revert fbd6901` 后仅保留
  `wf_workversion_race_test.go` 中幂等测试（并发测试会因泄漏失败，属预期）。
- 共享分类器回滚：`git revert 91259f6`，plant/harvest 恢复私有 `staleWorkProofError`
  行为不变；theft/恢复路径回到未覆盖状态（与基线一致）。
- 全部回滚：`git reset --hard 449bcb2`（工作区基线，所有提交均为本分支新增）。
