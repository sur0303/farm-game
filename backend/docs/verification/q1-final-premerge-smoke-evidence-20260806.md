# Q1 Final Premerge Authority Smoke — Evidence Report

日期：2026-08-06
状态：**静态验证 PASS；Authority Smoke S0 PASS / S1 FAIL_CONTRACT_MISMATCH（已按第 32 号合同对齐 Smoke 契约）；Merge BLOCKED**

> 方向修正（Q1 CONTRACT_ALIGNMENT / A1B）：成熟是**时间派生视图**（trusted_now_ms >= mature_at），不是持久状态迁移；服务端**按合同不推 MATURE FarmChange**。原先建议"后端补成熟推送"的方向是错的，已撤销。正确修复是 H5 用受信服务端时钟派生展示阶段（`effectiveCropPhase`），Smoke 断言 wire=GROWING / effective=MATURE / FarmVersion 不变，由 FarmActor 在 Harvest 时按 trusted_now_ms 最终裁决。

## 1. 最终候选绑定

| 侧 | Worktree | 分支 | Commit |
|---|---|---|---|
| Backend | `farm-game-premerge-a-final-20260806` | `codex/premerge-a-backend-final-20260806` | `13156f0` |
| H5 | `classic-farm-h5-premerge-a-final-20260806` | `codex/premerge-a-h5-final-20260806` | `b5434a8` |

Backend 候选 ancestry（相对 `b8d8f39` 提升到完整 Q1 tip `13156f0`，原因见 §4）：

- PRESENT: `b6a8717`(A2 bootstrap), `1cac975`(A2 task domain), `9479626`(A2 single-now), `064367f`(A3 dispatcher), `6284422`(A3 delivery guard), `935b51e`(h5-mainline-fix), `0411892`(old smoke base)

H5 候选主链：`8909038`(A1) → `e4679cf`(WS proxy) → `97f4a63`(Bootstrap correlation/refresh) + 移植 Smoke 配置（`b9f8659` 的 `VITE_WS_URL=/ws` + `VITE_WS_PROXY_TARGET`）与 `6079b84` 的 `authority-vertical-smoke.spec.ts`。**未 merge** `codex/q1-authority-smoke-hardening-h5-20260806` 整个分支。

## 2. 静态验证

### Backend（`13156f0`）

| 命令 | 结果 |
|---|---|
| `git status --short --branch` / `git diff --check` | 干净 / 通过 |
| `go test ./internal/player/ingress/... ./internal/player/task/... -count=1` | ok |
| `go test ./internal/logic/downlink/... -count=1` | ok |
| `go test -race ./internal/logic/downlink/... -count=1` | ok |
| `go test ./internal/farm/ingress/... -count=1` | ok |
| `go test -race ./internal/farm/ingress/... ./internal/logic/downlink/... -count=1` | ok |
| `go test -buildvcs=false ./... -count=1` | ok（0 失败） |
| `go build -buildvcs=false ./cmd/...` | ok |
| `go test -race ./cmd/gate-vertical-e2e/` | 5/5（mutex 修复覆盖） |

### H5（`b5434a8`）

| 命令 | 结果 |
|---|---|
| `git status --short --branch` / `git diff --check` | 干净 / 通过 |
| `npm test -- --run` | **107/107（14 文件）**，含 `bootstrap-recovery.test.ts`、`plot-guards.test.ts`、Bootstrap refresh 测试；未倒退 |
| `npx tsc --noEmit` | 通过 |
| `npm run build` | 通过 |
| `npm run audit:build` | build:web/staging/wechat real-backend boundary audit passed |

## 3. Authority Smoke 运行记录

- 命令：`H5_ROOT=<h5-final> ./scripts/run-authority-vertical-smoke`（Backend 最终候选）
- 运行时固定：Player/Farm Store=Authority、Worker=Authority、Assignment=Static、Dynamic Watch=OFF、Legacy=OFF（S0 日志确认：`authority mode enabled`、`no legacy fallback detected`）
- 数据源：公司 Redis `198.18.0.14:6380`（密码存于 gitignored `deploy/secrets/redis.conf.local`）
- 命名空间隔离：`smoke-<stamp>`（无 FLUSHALL）
- **退出码：1（FAIL）**

运行 OUT：`docs/verification/results/authority-vertical-20260806T131013Z-176933/`

| 阶段 | 结果 |
|---|---|
| fixture init（Claim ShardWriteAuthority） | ok（player/farm epoch=1 assignment_rev=1） |
| 服务启动 + ready | ok（player/farm/worker/gate） |
| S0 authority 日志 | ok（player+farm authority，无 legacy） |
| S1 Playwright | **FAIL：plot 恒 `growing`，15s 内未变 `mature`** |
| S1 Go bot | 未执行（S1 整体阻断） |

Playwright 失败（`playwright.log`）：

```
Error: expect(locator).toHaveClass(expected) failed
Locator: locator('[data-plot-id="plot-0"]')
Expected pattern: /\bmature\b/
Received string:  "plot growing   selected      stage-ripen"
```

（stage 从 sprout→leaf→ripen 正常推进，但 `status` 恒为 GROWING，页面按 A1 规则不将其提升为成熟。）

## 4. S1 MATURE 阻断根因（CONTRACT_MISMATCH，已对齐）

证据链：

1. 现行合同（第 32 号）：成熟是**时间派生视图**，不是持久状态迁移。CropCycle 在 Plant 时一次保存 `plant_at/mature_at/…`，成熟时**不追加状态写、不写 status=MATURE、不推进 Farm RecordVersion/FarmVersion**；只有 Harvest/Theft/Care 等真正改变业务事实的命令才推进 FarmVersion。FARM_CHANGE 要求严格本地版本 +1，成熟时发送 `FARM_CHANGE` 只能违反合同（同版本变化 or 为纯时间视图伪造版本递增）。
2. 服务端实现符合合同：`internal/farm/actor/actor.go` `plotView` **读取时**按 `now >= cycle.MatureAt` 派生 `CropPhase_MATURE`；`downlink.go` `enqueueChange` 仅操作提交时推 FARM_CHANGE；无成熟 ticker。
3. **H5 的错误**（A1 收紧过头）：`isMaturePlot = status === 'MATURE'` 要求服务端一定主动推送 MATURE，而合同不要求这条 Push → 长连接收不到 MATURE，收获被阻塞。
4. **Smoke 的错误**：Playwright 与 Bot 都在等一条合同不要求的 `MATURE FarmChange`。

**正确修复（Q1 CONTRACT_ALIGNMENT + A1B，已实施，未改后端）**：

- H5 建立受信服务端时钟（`trusted_server_now_ms` + `performance.now()` 锚点，禁 `Date.now`），新增 `effectiveCropPhase(plot, trustedNowMs)` 纯函数派生**展示阶段**；wire status 保持 GROWING、不推进 FarmVersion；单 Timer + `visibilitychange` 重算；Harvest/Theft UI 按 effective phase 开放，最终由 FarmActor 以 `trusted_now_ms >= mature_at` 裁决（偏早返回 `REJECTED CROP_NOT_MATURE`，H5 不自动重放）。
- Smoke：Playwright 断言 `data-wire-crop-phase="growing"` / `data-effective-crop-phase="mature"` / `data-crop-phase-source="trusted-time"`，并断言成熟展示前后 FarmVersion 不变；Go Bot 不等待 MATURE Push，从 Plant 后 FARM_CHANGE 读 `plant_at_ms/mature_at_ms`，成熟前 Harvest 断言 `CROP_NOT_MATURE`，按 `growDuration + 安全余量` 本地等待后 Harvest，断言 FarmVersion 恰好 +1；S1A(real H5) 与 S1B(protocol bot) 独立运行，verdict 区分 `PASS / H5_FAIL_SERVER_PASS / SERVER_FAIL / INCONCLUSIVE`。

禁止项：FarmActor、业务 Handler、Proto、Redis Repository 均未修改。

## 5. 判定

- 静态验证：**PASS**（两个候选，命令齐备，测试数 107 未倒退）。
- Authority Smoke：S0 **PASS**；S1 因服务端无成熟事件推送而 **INCONCLUSIVE（阻断）**，退出码 1。
- 未合入 integration、未创建 MR。合入须在 MATURE 配套（ICR）落地并通过 S0+S1 后由用户人工执行。
