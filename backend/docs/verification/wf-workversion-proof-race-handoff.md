# WF-WORKVERSION-PROOF-RACE — Handoff

日期：2026-08-07
分支：`codex/premerge-a-backend-final-20260806`
修复 commit：`297044c`（C1 安全补丁 + C2 根因修复）

## 一、根因（ICR）

S1A 低频失败（30 次中 1 次）：
```
participant proof: WorkVersion got=6 want=7   (got=call.ExpectedWorkVersion, want=record.WorkVersion)
step=COMMIT_SEED_RESERVATION / RESERVE_SEED / COMMIT_HARVEST
grpc=FailedPrecondition → H5 曾收到 REJECTED ERR_STORE_CAS_CONFLICT
```

**机制**：DirectDispatcher（不 claim，`LeaseOwner` 空）与 farm-worker（`ClaimDue` 时 `WorkVersion++` 并持 lease）并发驱动同一 WorkRecord。当 step.Payload 已持久化时，engine 原本**跳过 CAS/fence** 直接用 loaded version 构造 Participant Call；另一推进者已 Advance 版本 → participant proof 强读（`loadRecord` 用 `repository.Get`）得到更高版本 → `ErrWorkVersionMismatch`。

## 二、修复

### C1（安全补丁）
- `internal/workflow/engine.go`：新增 `ErrStaleWorkProof` sentinel；`invoker.Invoke` 错误处 `errors.Is(err, ErrStaleWorkProof)` → **强读 reload 权威 record 后 `continue` 重估**（绝不 StepUnknown / 绝不 pause / 绝不泄漏公网 REJECTED）。
- `internal/scenario/plant/rpc.go` + `harvest/rpc.go`：`staleWorkProofError` 把 `FailedPrecondition "participant proof: WorkVersion"` 包装为 `errors.Join(ErrStaleWorkProof, err)`（harvest 在通用 RPC 分类器**之前**识别）。

### C2（根因）
- **Preflight**：构造 Participant Call 前强读 record，若 `fresh.WorkVersion != record.WorkVersion` → reload + `continue`；`fenceWorkLease` 失败时：dispatcher（`fence.Owner==""`）**defer 本轮**（worker 从 DueIndex 恢复），worker 则 reload。
- **persistWithRaceHandling**：deadline/decision/build-payload 三处 persist 的 `ErrRevisionConflict` → dispatcher defer / worker reload。CAS loser 绝不发送 stale call。
- 不变量：Call 的 `ExpectedWorkVersion` 来自 preflight 强读的当前 committed version；participant proof 校验保持精确（未放宽 ±1）。

## 三、确定性测试（全绿）

`internal/workflow/wf_workversion_race_test.go`：
1. TestWorkVersionMismatchReloadsInsteadOfRejecting
2. TestDirectDispatcherAndWorkerRaceOnlyOneDriverInvokes
3. TestStaleProofDoesNotSurfaceAsRejected
4. TestConvergenceWithStaleProofOnEveryStep
5. TestParticipantCallUsesCommittedWorkVersion
6. TestCASLoserReloadsWithoutParticipantCall

新增 `faultStaleWorkProof` 到 fakeParticipant。

## 四、验收（触碰 Workflow runtime，全量重跑）

| 项 | 结果 |
|---|---|
| go test workflow/scenario/participant | ✅ |
| go test -race workflow | ✅ |
| go test ./... | ✅（无 FAIL） |
| **S1A 3轮×10 = 30/30** | ✅（evidence: s1a-hardened-20260807T14..Z 三轮） |
| workflow 终态（StepUnknown=0, due=0） | ✅（liveness=1） |
| worker 无永久循环 | ✅（loop=1） |
| 资源（fd 平 / RSS 温和） | ✅ |
| 完整 Smoke 2 轮 | ✅（authority-vertical-20260807T070530Z / T070617Z PASS） |
| S2/S3 | 见 S2/S3 证据 |

## 五、结论

`CP-VERTICAL-A-RC1` 的 WorkVersion proof 竞态已修复：内部 CAS/proof 冲突不再泄漏为公网终态拒绝；CAS loser 不发 stale call；workflow 可靠收敛。待 S2/S3 回归确认后恢复 `READY_FOR_MERGE_QUEUE`。
