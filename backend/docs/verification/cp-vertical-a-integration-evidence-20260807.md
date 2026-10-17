# CP-VERTICAL-A — Integration Merge + 最终回归 Evidence

日期：2026-08-07
状态：**CP-VERTICAL-A = COMPLETED**（Backend integration=`0f49c7d`，H5 integration=`e8f90ed`）

---

## 1. 最终 Manifest

```json
{
  "checkpoint": "CP-VERTICAL-A",
  "backend_business_fix": "297044c",
  "backend_candidate_head": "08a00dc",
  "backend_integration_commit": "0f49c7d",
  "h5_candidate_head": "a189063",
  "h5_integration_commit": "e8f90ed",
  "store_mode": "authority",
  "assignment_mode": "static",
  "dynamic_watch": false,
  "backend_dirty": false,
  "h5_dirty": false
}
```

## 2. 合并记录

| 分支 | 候选 Head | integration 旧 Head | merge commit | integration 新 Head |
|---|---|---|---|---|
| Backend | `08a00dc` | `935b51e` | `480e415`（--no-ff） | `0f49c7d`（含后续修复） |
| H5 | `a189063` | `9a204fc` | `e8f90ed`（--no-ff） | `e8f90ed` |

合并均无手工冲突（全为新增/自动合并）。

## 3. 回归暴露缺陷与修复（必须记录）

- **现象**：在 integration `480e415` 上跑 Normal Smoke，real-H5 Playwright 断言
  `data-last-error-code` 得到 `ERR_STORE_CAS_CONFLICT`(9002)，bot 协议层 PASS、
  backend participant RPC 全部 OK → `H5_FAIL_SERVER_PASS`。
  Evidence（修复前）：`results/authority-vertical-20260807T073850Z-2710456/`。
- **根因**：`internal/workflow/engine.go` 的 **decision persist 是裸 `e.persist`**
  （Invoke 成功后 decision 折叠处，旧第 388 行），未走 297044c 引入的
  `persistWithRaceHandling`。双驱动（DirectDispatcher + farm-worker）下，另一 driver
  在 Invoke→decision-persist 窗口推进 record → 本 driver 的 decision persist 返回
  `ErrRevisionConflict` → 同步 command 提交路径（plantonline/harvestonline）冒泡 →
  `internal/farm/ingress/service.go:1308` 映射为公网 `ERR_STORE_CAS_CONFLICT`。
  player.log 实证双驱动现场：`COMMIT_SEED_RESERVATION` 由 COORDINATOR(v6) 与
  WORKER(v7 replay) 同时驱动。
- **修复**（commit `0f49c7d`）：decision persist 改为 `persistWithRaceHandling`
  ——dispatcher defer（worker 从 DueIndex 恢复）、worker reload 权威 record 重评估；
  `ErrRevisionConflict` 永不泄漏到 command 提交路径。新增确定性测试：
  - `TestDecisionPersistConflictWorkerReloadsAndConverges`
  - `TestDecisionPersistConflictDispatcherDefersWithoutLeak`
- **验证**：`go test ./...`、`-race ./internal/workflow/...`、8 个 WorkVersion
  竞态测试、scenario/farm/participant 全绿；`go build ./cmd/...` OK。

## 4. Integration 最终回归（绑定 `0f49c7d` + `e8f90ed`）

### 4.1 S1A 同栈 10x — PASS（10/10）
Evidence：`results/s1a-hardened-20260807T154741Z/`
- 20 workflows 全部 `state=4(COMPLETED)`、`due_keys=0`、无 StepUnknown 残留
- worker 无 perpetual Claimed/Retried 循环
- player fd 平 33→34、RSS 温和增长（预期累计）

### 4.2 正常完整 Authority Smoke — PASS
Evidence：`results/authority-vertical-20260807T074651Z-2733818/`
- S0 + S1A(real-H5 Playwright) + S1B(Go 协议 bot) 全过
- H5：Bootstrap applied / Join READY / Buy / Plant / MATURE(trusted-time) /
  Harvest / pending-ops 归 0 / `data-last-error-code` 为空
- bot：`protocol-verdict.json = PASS`

### 4.3 S2/S3 故障 Smoke — PASS（4/4）
Evidence：`results/authority-fault-20260807T154741Z/`
- **s2** PASS：Ready 先于 post-ready change、PresenceEpoch=6>0、QUEUE_ACCEPTED、
  无 STALE_CONNECTION、guest FarmVersion 收敛无 gap
- **s3a** PASS：at_most_once（单 FarmVersion advance）、无重复 PlotDelta、
  no_fresh_join、unavailable_retries injected=2 final_deliver=1
- **s3b** PASS：drop_and_retry、gate idempotent dedup、无 duplicate PlotDelta、
  无 asset double-refresh、no_fresh_join
- **s3c** PASS：fresh_join（business fact 不回滚）、REJOIN_REQUIRED 正确下发

### 4.4 硬判定清单

| 判据 | 结果 |
|---|---|
| S1A | 10/10 PASS |
| 正常完整 Smoke | PASS |
| S2 / S3-A / S3-B / S3-C | PASS ×4 |
| public_store_cas_conflict | 0（本次回归无 9002） |
| step_unknown_residual | 0 |
| due_residual | 0 |
| pending_operations | 0 |
| backend_dirty | false |
| h5_dirty | false |

## 5. 历史失败证据（供追溯）

- `results/authority-vertical-20260807T073850Z-2710456/`：`480e415` 上 Normal Smoke
  H5 `ERR_STORE_CAS_CONFLICT` 失败（修复前），保留作缺陷证据。
- `results/s1a-hardened-20260807T153601Z/`：`480e415` 上 S1A 10x PASS（修复前基线）。
