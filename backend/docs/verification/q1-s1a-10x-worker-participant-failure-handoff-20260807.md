# Q1 S1A 10 次硬断言 — 服务端 participant 间歇失败 — 交接文档

日期：2026-08-07
状态：**S1A 10 次循环暴露服务端真实缺陷（worker→player-svr participant RPC 间歇 INVOCATION_UNKNOWN），需人工处理**。
按用户约定：遇到难题暂停，写清现象/根因/证据/建议方向，交用户。

---

## 1. 任务与结果

任务：实现 S1A 10 次硬断言（真实 H5 Playwright，每次全新账号，断言清单见 §2），连续 10 次全过。

**结果：10 次中 3 次 PASS（run-1/2/3），7 次 FAIL**。
证据：`docs/verification/results/s1a-hardened-20260807T130128Z/`（run-1..10.log + 服务日志 + protocol-events）。

- 每次是**独立新账号**（s1a<ts>a1..a10），但**复用同一 authority 栈**（同一 namespace/worker/player/gate）。
- 单次 smoke（每次新栈，如 run26）一直 PASS → **新栈不暴露，同一栈连续运行后退化**。

## 2. S1A 硬断言清单（已实现，单次验证 PASS）

spec：`src/test/e2e/authority-vertical-smoke.spec.ts`（H5 worktree `classic-farm-h5-premerge-a-final-20260806`）
1. Bootstrap applied（`data-bootstrap-state=applied`）
2. privateReady=true（`data-private-ready=true`）
3. JoinPhase=READY（`data-join-phase=READY`）
4. Plant 快路径或 PROCESSING 慢路径均能完成（refresh-sent/applied 增加 + seed 收敛）
5. Seed 1→0
6. FarmVersion 与 PlotDelta 原子落地（farmVersion +1 + plot 非 empty）
7. Mature 用 trusted-time 派生（`data-effective-crop-phase=mature` + `data-crop-phase-source=trusted-time` + FarmVersion 不变）
8. Harvest 成功（plot empty + FarmVersion +1）
9. Bootstrap refresh applied（refresh-applied 增加）
10. **Operation Journal 最终清理 / 无残留 pending**（新增 `data-pending-ops`，poll 到 0）
11. 无 last-error

新增诊断：App render `data-pending-ops`（`commandService.getPending().length`）。

## 3. 失败模式

| run | 失败点 | 现象 |
|---|---|---|
| 1/2/3 | — | PASS（14.3s） |
| 4 | line 118 Harvest | 点击 harvest 后 plot 15s 内仍 `mature`，未变 empty |
| 5~10 | line 85 Plant | 点击 empty plot 后 plot 仍 `empty`（乐观更新未生效，`tool-active` 保留） |

H5 侧共同点：**命令点击后 plot 状态不变化**（plant 后不 grow、harvest 后不 empty）。

## 4. 服务端证据（核心）

### 4.1 worker 持续重试
worker.log 从**第一条轮次（13:02:34）起**就是：
```
recovery round combined={Claimed:1 Resumed:1 Completed:0 Retried:1 ...}
```
持续到 13:03:44（共 43 轮），**始终有一条 workflow 被 resume 后 retry，永不完成**。

### 4.2 Redis 卡死的 workflow
`s1a10` namespace 共 19 条 workflow，**6 条卡死**（`state=3` = StepUnknown，`last_error_class=INVOCATION_UNKNOWN`）：

| step0 | attempts | 语义 |
|---|---|---|
| RESERVE_SEED ×4 | 5/6/7/8 | plant 的 player 侧第一步（扣种子） |
| COMMIT_HARVEST ×2 | 8/9 | harvest 的 player 侧一步（奖励入账） |

其余 13 条 `state=4`（完成）。

**INVOCATION_UNKNOWN** = worker 调 player-svr 的 participant gRPC 返回了**非 typed rejection 的错误**（Unavailable/Unknown/超时等），按 contract 25 语义 worker 无限重试同 invocation id，永不前进。

### 4.3 时间线
- 13:01:34 服务启动
- 13:02:34 worker 首轮即卡住（此时 run-1 刚开始 plant）
- run-1/2/3 仍 PASS（plant 的 FarmActor CommitPlant 提交 + FARM_CHANGE 推送成功，H5 端 farm 侧成功；但 player 侧 RESERVE_SEED 已间歇失败）
- run-4 起 harvest 失败、run-5 起 plant 失败（累计卡死 workflow 增多，worker 处理能力被卡住的占据）

## 5. 根因分析

**核心缺陷（服务端）**：**Plant/Harvest 的 workflow 中，player-svr 的 participant step（RESERVE_SEED / COMMIT_HARVEST）间歇性返回 INVOCATION_UNKNOWN**，导致：
- 种子扣减（plant）/奖励入账（harvest）卡住
- workflow 无限重试（不前进、不释放）
- 同一 authority 栈连续运行后累积，后续命令受影响

**单次 smoke 不暴露**：每次新栈（新 worker/player/Redis namespace），worker 干净。同一栈连续运行（10 次循环）才出现累积退化。

**未确定的确切错误**：worker/player 日志均无 participant 调用的运行时错误日志（player-svr 只打了 2 行启动日志）。`INVOCATION_UNKNOWN` 的具体 gRPC 错误未知。

**候选方向**（按可能性）：
1. **player-svr 的 participant handler（ReserveItem/GrantItem）间歇性返回错误**——如 actor 未就绪、claim 失败、超时（`registry.Submit` 在 ClaimUnavailable/Unknown 时返回 err，见 participant.go:55-57 / registry.go:388）。
2. **worker 调 player-svr 的 RPC 超时**：`WORKER_PARTICIPANT_BUDGET_MS=5000`，若 player-svr 处理慢（actor 冷加载、连接池）超过 5s → Unknown。
3. **多次运行累积**：player-svr 的 actor/连接/Redis 键累积（每条卡住 workflow 的 due record 一直占用 worker 轮询）。

## 6. H5 侧疑惑（待确认）

run-5~10 的 plant 失败：点击后 plot 仍 `empty`、`tool-active` 保留。`execute()` 前置是 `ws.status !== 'InFarm'` 直接 return（不发出命令、不乐观更新）。**疑似 plant 时 ws.status 非 InFarm**（连接问题），或 commandService.execute 抛错。error-context.md 未含页面快照（缺 data-status/data-last-error 证据）。此问题**可能独立于服务端卡住**（本地乐观更新不依赖服务端），需补充页面状态证据确认。

## 7. 建议方向

1. **先定位 player-svr participant 错误**：给 `internal/player/ingress/participant.go` 的 `ReserveItem`/`GrantItem` 及 `registry.Submit` 失败路径加结构化日志（错误码/类/玩家/请求），重跑 10 次拿到确切错误。
2. **检查 worker participant 超时/预算**：`WORKER_PARTICIPANT_BUDGET_MS`、player-svr 调用 timeout，确认是否超时导致 Unknown。
3. **确认多次运行累积**：对比首次运行 vs 第 5 次运行时 player-svr 的 actor/连接状态。
4. **H5 侧**：spec 在 plant 点击前断言 `data-status=InFarm` + 失败时把 `data-status/data-last-error/data-pending-ops` 写入 error-context，区分 H5 连接问题 vs 服务端。
5. 若确认为 player-svr 间歇失败（服务端缺陷），修复后再重跑 10 次。

## 8. 当前代码/提交状态

- H5 worktree（`classic-farm-h5-premerge-a-final-20260806`，HEAD `fe15152`）：
  - **未提交**：`src/app/App.ts`（`data-pending-ops`）、`src/test/e2e/authority-vertical-smoke.spec.ts`（硬断言清单）。
- backend worktree（`farm-game-premerge-a-final-20260806`，HEAD `464535f`）：
  - **未提交**：`scripts/run-s1a-hardened-10x`（10 次循环脚本）。
- S1A 硬断言**单次验证 PASS**（run26，完整 smoke）；10 次循环暴露上述服务端问题。

## 9. 结论

**这不是测试脚本/断言的问题**（单次 run26 全断言通过；3/10 也过）。是**同一 authority 栈连续运行后，worker→player-svr 的 participant RPC 间歇 INVOCATION_UNKNOWN 导致 plant/harvest 卡死**，属**服务端真实缺陷**。需按 §7 定位 player-svr 侧确切错误后修复，再重跑 S1A 10 次。

---

## 10. 诊断结论（WF-PARTICIPANT-INVOCATION-UNKNOWN-DIAG，根因已定位并验证）

### 10.1 根因（决定性证据）

**`actor route: holder lease expired`——worker 的静态 route 快照 lease 固定，55s 后过期。**

证据链：
1. 新增观测（worker participant client begin/end、player server begin/end、engine workflow_step_unknown）显示：
   ```
   workflow_step_unknown ... step=COMMIT_HARVEST ... cause=actor route: holder lease expired
   ```
   **`participant_rpc_client_begin` = 0**（请求根本没发出）——错误发生在 `prepareParticipantCall` 的 route 解析，**在构造 ParticipantRequest 之前**。
2. `internal/actorrouting/routing.go:900`：`StaticResolver.resolve` 用 `time.Now().UnixMilli() >= assignment.LeaseUntilMs` 检查过期，返回 `ErrLeaseExpired`（"actor route: holder lease expired"）。
3. `cmd/farm-worker` 用 `ACTOR_ROUTE_SNAPSHOT` 静态快照 resolver（`LoadStaticResolver`），**启动时一次性加载**并缓存（`StaticResolver.table`），不随 player-svr 动态 renew 更新。
4. `route-snapshot.json` 的 `lease_expire_at_ms` 是 **fixture init 写入的固定值**（实测 `1786083147825` = init 时 now + 55s）。
5. 时序吻合：run-1/2/3（<55s）PASS，run-4 开始（>55s）失败。

### 10.2 验证（控制实验 E + lease 刷新）

修改 `run-s1a-hardened-10x`：fixture init 后、worker 启动前，把 route-snapshot.json 的 `lease_expire_at_ms` 刷新为 now+1h。
**重跑 S1A 10 次：10/10 全 PASS**（evidence：`s1a-hardened-20260807T141653Z`）。

决定性证明：**根因是静态快照 lease 固定，不是 player-svr handler/receipt/workflow 状态推进 bug**。

### 10.3 结论定性

- **不是产品 handler bug**：RESERVE_SEED/COMMIT_HARVEST 的 INVOCATION_UNKNOWN 全部来自 route 解析层（`ErrLeaseExpired`），请求从未到达 player participant server（server_begin 缺失）。
- **是测试栈配置缺陷 + 一个产品活性缺口**：
  1. 测试栈：worker 用静态快照，lease 固定 → 长期运行（>55s）后 route 全拦。
  2. 产品活性缺口：`StaticResolver` 基于固定快照 lease 做活性判断，**没有动态 lease 刷新路径**（生产若 worker 长期运行且 holder 靠动态 renew，同样会过期）。

### 10.4 REQUIRED_FIX 建议（诊断任务不实施语义修复）

1. **测试栈（立即）**：fixture init 快照 lease 设为覆盖测试总时长的值（如 now + 30min），或 S1A 循环前刷新快照（当前脚本已做 now+1h）。
2. **产品（根治，待人工决策）**：
   - 选项 A：`StaticResolver`/worker 增加**动态 lease 刷新**（route 解析时读 Redis shard-authority 的实际 lease，或周期 reload 快照），避免静态快照 lease 过期；
   - 选项 B：快照 lease 仅用于 holder 身份/epoch 绑定的路由选择，**lease 活性改由动态 shard-authority 检查**；
   - 选项 C：fixture/部署时快照 lease 与 `ACTOR_AUTHORITY_LEASE_TTL` 解耦，给足运行窗口（仅测试场景）。
3. **归属**：`internal/actorrouting`（StaticResolver lease 活性）是产品代码，超出诊断任务 owned_write_set；建议单独立任务处理。

### 10.5 本轮新增观测（owned_write_set，已提交）

- `internal/scenario/plant/rpc.go` + `internal/scenario/harvest/rpc.go`：`participant_rpc_client_begin/end`（workflow/step/invocation/player/duration/grpc_code/outcome/replay）。
- `internal/player/participant/server.go`：`participant_rpc_server_begin/end`（ExecuteStep/ResumeStep，caller=COORDINATOR/WORKER）。
- `internal/workflow/engine.go`：`workflow_step_unknown`（INVOCATION_UNKNOWN 产生点 + 物理 cause）。
- `scripts/run-s1a-hardened-10x`：快照 lease 刷新（诊断验证）。
- 定向测试：`internal/workflow/...`、`internal/scenario/...`、`internal/player/participant` 全过。

### 10.6 验收状态更新

- S1A 10 次硬断言：**10/10 PASS**（lease 刷新后，evidence `s1a-hardened-20260807T141653Z`）。
- 正常 Smoke（run23/24/25）+ S2/S3（两轮 4/4）：保持有效。
- **CP-VERTICAL-A-RC1 状态**：`BLOCKED_BY_WORKFLOW_PARTICIPANT_LIVENESS` → **根因已定位为测试栈快照 lease 配置**，产品缺陷待人工决策（§10.4）。若确认产品仅测试场景受影响，RC1 业务代码本身无缺陷。

---

## 11. 追加发现：第二轮 9/10 —— WorkVersion proof 竞态（独立于 route lease 修复）

### 11.1 现象

第二轮 S1A 10 次（lease 修复后，evidence `s1a-hardened-20260807T142413Z`）：**9/10**。run-4 失败：
```
data-last-error-code = "ERR_STORE_CAS_CONFLICT"
```
其余 9 次 PASS。workflow liveness=1（全 COMPLETED、无 StepUnknown、无 due 残留）、worker loop=1（无 Retried 永久循环）、资源采样 fd 33→34 / RSS 25M→28M（温和，无泄漏阶梯）。

### 11.2 根因线索（观测日志）

run-4 有一条 `workflow_step_unknown`（我加的观测）：
```
workflow=... step=COMMIT_SEED_RESERVATION ... player=2000009 attempt=1
err_class=INVOCATION_UNKNOWN cause=rpc error: code = FailedPrecondition
desc = participant proof: WorkVersion got=6 want=7
```
- `participantproof/verifier.go:349`：`call.ExpectedWorkVersion(7) != record.WorkVersion(6)` → `ErrWorkVersionMismatch`。
- `workflow/engine.go:249`：`call.WorkVersion = record.WorkVersion`（构造 call 用当前版本）。
- 故 **worker resume 时的 record 视图是 version 7，而 participant server 校验时读到的持久 record 是 version 6**——两者不一致（worker 视图超前 1）。

### 11.3 定性

- **独立于 route lease 修复**（lease 已修，此竞态仍出现）。
- 低频（9/10 中 1 次）+ 可恢复（该 workflow 最终 COMPLETED，worker 无永久循环）。
- 但 **H5 在某个命令收到一次 REJECTED（`ERR_STORE_CAS_CONFLICT`）→ spec 的 last-error 断言失败**。
- 疑似 **worker 恢复与 DirectDispatcher/Coordinator 并发推进的 WorkVersion CAS 竞态**（用户 §7.3 预判方向）：worker 的 resume call 携带的 ExpectedWorkVersion 与 participant server proof 校验时的持久 record 版本不一致。

### 11.4 待办（交人工决策）

1. 确认 `ErrWorkVersionMismatch` 的触发条件：worker 何时用超前版本构造 call；participant server proof 校验读的 record 是否与 worker 同一份（receipt 快照 vs 实时 record）。
2. 检查 worker claim/Advance 与 DirectDispatcher 的并发 CAS（record_version / WorkVersion）。
3. 是否允许 worker resume 在版本不匹配时**幂等重试**（proof 失败重读 record），而不是直接 REJECTED。
4. 若修复触碰 Workflow runtime / Participant RPC，需按用户规则重跑：两轮完整 Smoke + 两轮 S2/S3 + S1A 10x。
