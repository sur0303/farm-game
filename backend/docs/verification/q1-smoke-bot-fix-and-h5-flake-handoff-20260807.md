# Q1 Authority Smoke — Bot 修复完成 + H5 种子刷新抖动未决 — 交接文档

> **status = SUPERSEDED**
>
> superseded_by =
> - WF-WORKVERSION-PROOF-RACE Handoff：`docs/verification/wf-workversion-proof-race-handoff.md`（commit `563ac91`）
> - CP-VERTICAL-A Final Status（候选真值 + 五点 Review + Merge Queue）：本 repo 最终 Checkpoint 登记
>
> 本页顶部原状态"**H5 侧 1 个抖动缺陷未修复，阻断连续两轮 PASS 门槛**"是**历史快照**。此后已完成：
> H5 刷新抖动已由 `H5-REFRESH`（purpose 分离 BootstrapRecovery + PlayerBootstrapRefresh）、
> PROBLEM A/B（PROCESSING 慢路径闭合 + FarmChange 原子性）与 S1A 硬断言修复并验收；
> WorkVersion proof race 已由 C1+C2（reload-on-stale-proof，`297044c`）关闭。本文仅作历史记录。

日期：2026-08-07
作者：AI agent（session 续接）
状态：**Bot 侧 3 个 bug 已修复并验证 PASS；H5 侧 1 个抖动缺陷未修复，阻断"连续两轮 PASS"门槛**

---

## 1. 目标与候选

最终目标（用户指令链）：静态 Gate → 两轮连续正常 Smoke（全新 namespace/账号）→ S2/S3 → 全部通过后合并：

- 后端候选：`13156f0`（worktree `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-premerge-a-final-20260806`，branch `codex/premerge-a-backend-final-20260806`）
- H5 候选：`b5434a8` + 未提交的 A1B trusted-clock 改动（worktree `/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-premerge-a-final-20260806`，branch `codex/premerge-a-h5-final-20260806`）

Smoke 运行方式（后端 worktree 内）：

```bash
./scripts/run-authority-vertical-smoke
# 退出码 0=PASS 1=FAIL 2=INCONCLUSIVE；证据在 docs/verification/results/authority-vertical-<ts>/
```

Redis 为公司实例（配置在 gitignored `deploy/secrets/redis.conf.local`）。每轮全新 namespace/账号（`smoke_a<epoch>` / `smoke_bot<epoch>`）。

---

## 2. 本轮已修复（后端 worktree 未提交改动）

改动文件（`git status` 脏，尚未 commit）：

- `cmd/authority-smoke-bot/main.go` — 3 个 bot bug 修复（见下）
- `scripts/run-authority-vertical-smoke` — H5_ROOT 默认值修正（见 2.4）

### 2.1 Bot panic：`waitGrow` 用读死线轮询，毒化 gorilla 连接（run12 根因）

**现象**：run12 bot.log `panic: repeated read on failed websocket connection`（`waitGrow` main.go:755 → `readFrame` :974），exit=2，`protocol-verdict.json` 未写出，脚本判 INCONCLUSIVE。

**根因**：gorilla/websocket 在**任何读错误（含 SetReadDeadline 超时）后永久毒化连接**，下一次 `ReadMessage` 直接 panic。旧 `waitGrow` 用 250ms 读死线轮询等帧；grow 期间按契约本就该无帧 → 第一次超时 → `continue` → 第二次读 → panic。run12 是首个活到 `waitGrow` 的 run（此前各 run 在更早的 bot bug 上失败），所以此时才暴露。

**修复**：全 bot 改为**单读 goroutine（frame pump）+ channel 分发**（`startFramePump`/`nextFrame`），所有等待用应用层 `select`+timer（哨兵 `errFrameTimeout`），全程不设任何读死线；同时删掉 `connect()` 里 15s 全局读死线（同族隐患：超过后所有读必超时毒化）。`main()` 加 recover 安全网：任何 panic 也落 `protocol-verdict.json`（exit 2）。

**验证**：run14/15/16 grow 步骤稳定 ok（12s 窗口干净走完，连接后续 harvest 正常）。

### 2.2 Bot harvest 乱序门控（run13 根因，与 run9 plant 同类）

**现象**：run13 `harvest timeout waiting version advance`。

**根因**：`harvest()` 旧代码要求 `accepted==true`（先收 SUCCEEDED）才认可 version+1 的 FARM_CHANGE。run13 证据（`protocol-events.ndjson` line 7 + `bot-frames.ndjson`）：harvest 的 `change:2` 在 03:05:51.361 推送，SUCCEEDED 在 .368 到达——**FARM_CHANGE 先于终态结果 7ms**，被门控丢弃后再无帧，15s 超时。FARM_CHANGE 仅在已提交时推送（enqueueChange），push 到达即提交证明，不需要等结果帧次序。

**修复**：删除 `accepted` 门控，`change.GetFarmVersion() == planted.farmVersion+1` 即返回成功。

**验证**：run14/15/16 harvest ok（`FarmVersionAdvanced 1 -> 2`）。

### 2.3 Bot bootstrap_refresh 断言语义错误（run14/run15 根因）

**现象**：run14 `harvest reward not converged: coin 100 -> 90`；run15 `harvest produce not converged: items 1 -> 1`。

**根因**（两层）：
1. 旧断言期望"收获后金币增加"。**语义错误**：收获奖励是作物**入库存**（items），金币只有 SELL 才增加。run14 的 `coin 100->90` 完全正确（100 初始 − 10 买入），是断言错了。
2. 基线取自首个 bootstrap（coin=100），没算 BUY 扣费；且用条目**计数**判定——plant 消耗种子、harvest 发放产物，计数 1→1 不变但**内容**已从种子变成产物（run15 证据：buy 后 inventory={seed:1}，harvest 后 bootstrap 显示 `revision=6 coin=90 items=1`，内容是产物）。

**修复**：`buy()` 解码 BUY 终态 `BuyResult.privateState` 作为精确基线（coin=90、seed_item_id、库存内容）；`waitTerminal` 改为返回终态 payload；新增 `lastInventory map[itemID]count` 跟踪；refresh 断言 = **库存中出现非种子条目（产物入库）且 coin 与 harvest 前一致（无漂移）**。

**验证**：run16 bot 全 13 步 ok，exit=0，verdict PASS。

### 2.4 脚本 H5_ROOT 默认值回退（run13 根因）

**现象**：run12 manifest `h5_commit=b5434a8`（正确候选），run13 变成 `b9f8659`。

**根因**：脚本默认 H5_ROOT 优先 `classic-farm-h5-q1-authority-smoke-20260806` worktree，其 HEAD 钉在**旧中间提交 b9f8659**（缺少 97f4a63 资产刷新、A1B trusted-clock 等 H5 修复）。run12 显式传了 H5_ROOT 指向最终候选，run13 没传 → 默认解析到旧 rig → H5 在 line 77 失败。**不是产品 bug，是被测版本错了。**

**修复**：脚本默认改为优先 `classic-farm-h5-premerge-a-final-20260806`（最终候选 b5434a8+A1B），注释写明原因；`H5_ROOT` 环境变量仍可覆盖。

---

## 3. 未决问题：H5 Playwright line 77 种子扣减抖动（P1，阻断连续两轮 PASS）

### 3.1 现象

`authority-vertical-smoke.spec.ts:77`：

```ts
// Plant commits asynchronously via WSS; wait for seed deduction.
await expect.poll(seeds, { timeout: 15_000 }).toBe(seedsBefore);
```

15s 内种子数未从 1 回到 0（`Expected: 0n, Received: 1n`）。**同一代码下时过时而不过**：run14 PASS、run15 PASS、run16 FAIL（13.1s/13.1s/失败）。

失败时页面快照（run16 error-context.md）一致显示：
- 金币 90（BUY 已生效）、胡萝卜种子 ×1（**未扣减**）
- 地块 #0 已种上且显示"已成熟"（`farm_version 1`）——**plant 服务端已提交，FARM_CHANGE 已到 H5**，作物甚至到了成熟期
- 即：服务端完全正常（bot 同轮全 PASS 也佐证），是 H5 客户端私有状态刷新链路抖动

### 3.2 机制链路（为什么种子扣减只依赖 Bootstrap 刷新）

1. H5 解码层（`final-protocol-adapter.ts:512-519`）对 **PLANT 终态结果不解析 privateState**（公网 `PlantResult` 本身不带私有状态；只有 BUY/SELL/GET_PLAYER_BOOTSTRAP 会填 `value.privateState`）。所以 BUY 后种子 0→1 是结果帧直接带的（`App.ts:867-872`），而 **plant 后种子扣减只能走"资产命令终态成功后安全重查 Bootstrap"**（97f4a63 引入）。
2. 刷新触发：`App.ts:875-876`：`status==='SUCCEEDED' && isAssetAffectingCommand(commandType)` → `refreshPlayerBootstrap()`。
3. `refreshPlayerBootstrap()`（`App.ts:1067-1077`）有三道门：
   - `ws.status !== 'InFarm'` → 不发
   - `bootstrapOutcome === 'pending'` → 不发
   - **`!this.bootstrapRecovery.hasBudget()` → 不发** ← 最大嫌疑
4. 响应关联：`handlePlayerBootstrapResult`（`App.ts:1012`）按 `bootstrapRecovery.ownsResult(operationId, generation)` 认领，`store.applyPlayerBootstrap` 按 `playerStateRevision` 单调应用。

### 3.3 头号嫌疑：BootstrapRecovery 预算被资产刷新共享耗尽

`bootstrap-recovery.ts`：`BootstrapRecovery(MAX_BOOTSTRAP_ATTEMPTS=3)`，`sentCount` 对**所有**发送计数——恢复屏障重试和资产刷新**共用一个每 generation 3 次的预算**。

干净路径的预算账本（一个 generation 内）：

| 发送者 | sentCount（发送后） |
|---|---|
| Recovery 首次 bootstrap | 1 |
| BUY SUCCEEDED → refresh | 2 |
| PLANT SUCCEEDED → refresh | 3（`hasBudget` 检查 2<3，**刚好挤进**） |
| HARVEST SUCCEEDED → refresh | 被挡（3<3  false） |

结论：**干净路径下 PLANT 刷新恰好用掉最后一格预算**。只要 Recovery 阶段多用一次重试（首次 750ms 退避超时等任何时序抖动），链条变成 Recovery×2 + BUY refresh = 3，**PLANT 刷新被 `hasBudget()` 静默拦截** → 种子永远不扣 → line 77 15s 超时。这与 run14/15 PASS、run16 FAIL 的抖动表现完全吻合。

注意：run16 没有直接证据证明 Recovery 重试了一次（gate/player/farm 日志均无 bootstrap 帧级记录；`grep -ci bootstrap` 全为 0），但预算共享是代码层面确定存在的缺陷，且是唯一能在"服务端正常、响应本地可达"前提下确定性拦截刷新的路径。

### 3.4 次要嫌疑（未能排除，按可能性排序）

- **S2：PLANT 终态 SUCCEEDED 帧晚到/未到**，刷新根本没触发。plant 走异步 workflow（worker 100ms 轮询），正常 <1s；但 run16 无 H5 侧帧证据。若 SUCCEEDED 在 15s 窗口外到达，同样表现为超时。
- **S3：刷新响应未被应用**。`applyPlayerBootstrap` 按 revision 单调拒绝旧响应；若 BUY 刷新响应（rev 3）与 PLANT 刷新响应（rev 4）乱序，先到者应用、后到者按 revision 拒绝——逻辑上安全（最终态仍是 rev 4），但若 PLANT 响应的 strict 7-field 解码失败（`classifyBootstrapResult`）则会被丢弃且不 retry。
- **S4：刷新单发无重试**。`refreshPlayerBootstrap` 只发一次，无超时重发（Recovery 路径才有退避重试）。响应若丢失（本地理论不会）则无挽回。

### 3.5 建议修复（未实施）

首选方案（小、对症）：**把资产刷新预算从 Recovery 屏障预算中分离**。

- `bootstrap-recovery.ts`：新增 `trackRefresh(operationId)`——只把 operationId 加入相关性集合、**不递增 `sentCount`**（刷新由用户命令终态触发，天然有界，无循环风险）。
- `App.ts` `refreshPlayerBootstrap()`：去掉 `hasBudget()` 门（或改用独立的刷新预算），改用 `trackRefresh`。
- `bootstrap-recovery.test.ts`：补单测——"Recovery 重试耗尽预算后，PLANT 刷新仍被发送且响应可认领"。

辅助手段（建议先做以拿到铁证）：
- 在 H5 加临时可观测性：`data-app-shell` 上落 `data-bootstrap-refresh-sent` / `data-bootstrap-refresh-applied` 计数器，跑一次失败即可区分 S1（未发送）vs S2/S3（发了没应用）。
- 或解包 run16 的 `test-results/.../trace.zip` 看 console/ws 帧。

### 3.6 修复后流程

1. H5 worktree（`classic-farm-h5-premerge-a-final-20260806`）跑 `npx vitest run` 确认单测全绿。
2. 连续两轮 `./scripts/run-authority-vertical-smoke`（脚本已默认对准最终 H5 候选），均 PASS 才满足用户门槛。
3. S2（post-ready PresenceEpoch 故障）/ S3（Dispatcher / Delivery Guard 故障）。
4. 全过后合并后端 `13156f0`（+未提交的 smoke 改动需先 commit）与 H5 候选（b5434a8 + A1B/A1B 后续未提交改动需先 commit），发布 CP-VERTICAL-A。

---

## 4. 运行历史（本轮全部证据在 `docs/verification/results/`）

| run | 目录后缀 | H5 commit | S1A(Playwright) | S1B(bot) | 总判定 | 根因 |
|---|---|---|---|---|---|---|
| run12 | 20260807T023642Z-1743549 | b5434a8 | PASS | exit2 panic | INCONCLUSIVE | §2.1 waitGrow 读死线 panic |
| run13 | 20260807T025112Z-1793964 | **b9f8659（错）** | FAIL line77 | FAIL harvest 超时 | FAIL | §2.4 H5 版本回退 + §2.2 accepted 门控 |
| run14 | 20260807T030515Z-1826663 | b5434a8 | PASS | FAIL bootstrap_refresh | FAIL | §2.3 coin 断言语义错 |
| run15 | 20260807T031021Z-1841029 | b5434a8 | PASS | FAIL bootstrap_refresh | FAIL | §2.3 计数断言 1→1 误报 |
| run16 | 20260807T031455Z-1858516 | b5434a8 | **FAIL line77** | **PASS（13/13 步）** | FAIL(H5_FAIL_SERVER_PASS) | §3 H5 刷新抖动（未决） |

## 5. 仓库状态提醒

- 后端 worktree 全部 smoke 改动（含此前会话的 bot 修复 + 本轮 3 处修复 + 脚本 H5_ROOT 默认值）**均未 commit**。`backend_dirty=true` 会如实写入每轮 manifest。
- H5 worktree 的 A1B trusted-clock 改动（`trusted-server-clock.ts`、`App.ts`、`plot-guards.ts`、spec、`plot-guards.test.ts`）**均未 commit**。§3.5 的刷新预算修复若实施也请先落在此 worktree。
- 勿在 q1-authority-smoke H5 worktree（b9f8659）上跑最终验证——它是中间 rig，缺 H5 修复（脚本默认已改，但手动传 H5_ROOT 时注意）。
- 已知服务端 P2 缺口（不要求修）：premature harvest 的 terminal error 未映射为 `ERR_CROP_NOT_mature`，返回 `ERROR_CODE_UNSPECIFIED`（bot 已容忍任意错误码，只断言 REJECTED 行为）。

---

## 6. 追加进展（2026-08-07 后续）：H5 预算分离已实现并验证，新暴露独立 flaky

本节更新 §3 的未决问题：H5 Bootstrap 预算分离**已实现并提交**，run20 完整 PASS 证明修复生效；但连续验证时 run21/22 暴露了**另一个独立的 H5 plant flaky**，仍阻断"连续两轮 PASS"门槛。

### 6.1 已完成的 H5 预算分离修复（run20 PASS）

H5 worktree 提交（branch `codex/premerge-a-h5-final-20260806`）：
- `db6124c` — A1B trusted-clock + `PlayerBootstrapRefresh` 预算分离（见下）
- `d95b5b0` — spec 断言 refresh 计数器 + render 修复
- `3aa5777` — spec baseline 在 plant 提交前读取

实现要点（对应 §3.5 与用户规格）：
- `bootstrap-recovery.ts`：`BootstrapRecovery` 改带用途的 Map（`BootstrapRequestPurpose = 'recovery' | 'refresh'`）；`recoverySentCount` 只由 recovery 推进；新增 `trackRecovery`/`trackRefresh`/`claim`（认领即消费，防集合无限增长）/`forget`。
- `player-bootstrap-refresh.ts`（新增）：独立控制器，`dirtySequence` + 单 in-flight + `maxAttempts=3` + 有界重试；`markDirty`/`canStart`/`begin`/`claim`/`succeed`/`fail`。
- `App.ts`：BUY/SELL 带 privateState 时不额外 refresh（只 `applyPrivateState`）；PLANT/HARVEST/THEFT 无 privateState → `markDirty + pumpPlayerBootstrapRefresh`（不查 `hasBudget`）；结果按 purpose 分流（`handleMandatoryBootstrapResult` vs `handlePlayerBootstrapRefreshResult`）；独立 refresh 重试（3s 超时，250/750/1500ms backoff）；诊断属性 `data-bootstrap-recovery-attempts`/`-refresh-dirty-seq`/`-refresh-sent`/`-refresh-applied`/`-refresh-inflight`/`-refresh-retries`；`pump` 发送后立即 render（否则 sent 计数不反映到 dataset）。
- 测试：`src/test/bootstrap-recovery.test.ts` 重写为 purpose 语义 + `PlayerBootstrapRefresh` 单飞/重试/generation 隔离（16 用例全绿）；`src/test/trusted-server-clock.test.ts`。

后端 worktree 提交 `544c704`：
- bot `bootstrap_refresh` 改轮询（最多 8 次、500ms）等 harvest reward 落库。FarmVersion 推进（farm actor）与 GRANT_HARVEST_REWARD 入账（player actor）非原子，单次 GET_PLAYER_BOOTSTRAP 可能读到旧 revision（run18 rev5 空库 vs run17 rev6 有产物）。
- 脚本 H5_ROOT 默认指向 premerge-A FINAL worktree。
- `.gitignore` 忽略 `results/*/tmp/`。

### 6.2 run17~run20 验证

| run | H5 commit | backend | S1A(H5) | S1B(bot) | verdict |
|---|---|---|---|---|---|
| run17 | db6124c | 13156f0+dirty | PASS | PASS | **PASS** |
| run18 | db6124c | 13156f0+dirty | FAIL(sent>1) | FAIL(produce) | FAIL |
| run19 | d95b5b0 | 13156f0+dirty | FAIL(sent>1) | PASS | H5_FAIL |
| run20 | 3aa5777 | 13156f0+dirty | PASS | PASS | **PASS** |

run18/19 是我在 spec 里的断言时序 bug（baseline 在 plant 后读 → 读到已发送的 sent），run20 修正后 PASS。**预算修复本体 run20 证明有效**（PLANT 后 refresh-sent/applied 增加、seed 1→0、FarmVersion 0→1）。

### 6.3 新暴露的独立 flaky：run21/22 H5 plant 后无 refresh（未解决，阻断合并）

后端提交 `544c704` 后跑连续验证：

| run | H5 commit | backend | S1A(H5) | S1B(bot) | verdict |
|---|---|---|---|---|---|
| run21 | 3aa5777 | 544c704 | FAIL | PASS | H5_FAIL_SERVER_PASS |
| run22 | 3aa5777 | 544c704 | FAIL | PASS | H5_FAIL_SERVER_PASS |

**现象**（run22 证据完整）：
- 页面快照：金币 90、胡萝卜种子 1（未扣）、farm_version 1、全地块"空地"、`data-bootstrap-refresh-sent=0`
- 但服务端：worker `recovery round Completed:1`（12:00:12），protocol-events 里 H5 订阅有 `change:1`（12:00:12）
- bot（gen=2，稍后加入）：`FarmSnapshot version=0` → `FarmChange plot=0 phase=GROWING plot_version=2`（**这是 H5 plant 的提交**，version 0→1）→ bot harvest 后 `FarmVersionAdvanced 1→2`

**解读**：H5 的 plant **服务端已成功提交**（FARM_CHANGE `change:1` 已下发，version 0→1，地块 plot_version=2）。但 H5 侧：
- plot 仍显示"空地"（未应用 change:1 的地块状态）
- seed 未扣（refresh 未发，`refresh-sent=0`）
- farm_version 显示 1（change 的 version 被应用到了 version 计数，但 plot 数据未应用？）

**根因判断**：H5 收到了 plant 的 FARM_CHANGE（`change:1`），但**没收到/没处理 plant 的 CommandResult SUCCEEDED**。`handleFrame` 的 refresh 分支依赖 `frame.value.status === 'SUCCEEDED'`；若 SUCCEEDED 结果帧未到 H5，refresh 不触发 → seed 不扣。同时 plot 状态应用与 version 计数应用不一致，疑似 `applyJoinDeltaBatch`/`recordFarmChange` 对 FARM_CHANGE 的应用在某条路径上没完整落到 plot。

**与服务端/预算修复的关系**：protocol-events 只记录 downlink（FARM_CHANGE 走 farm→gate→H5 的投递队列），CommandResult 是另一条响应路径。run20 PASS（plant 的 SUCCEEDED 到了、refresh 触发）vs run21/22 FAIL（SUCCEEDED 未到）——**CommandResult 投递不稳定**是头号嫌疑，属**服务端/链路问题，非 H5 预算分离修复的回归**（该修复只改客户端 refresh 逻辑，不改 FARM_CHANGE 应用）。

**待办**（后续调查方向，勿合并）：
1. 确认 H5 是否收到 plant 的 CommandResult SUCCEEDED（需 H5 侧帧/console 证据；当前 protocol-events 不覆盖命令结果响应路径）。
2. 检查 H5 `applyJoinDeltaBatch`/`recordFarmChange` 对 FARM_CHANGE 的应用是否完整（version 与 plot 一致）。
3. 若为服务端 CommandResult 投递丢失，属独立 P1，需在 farm-gate/farm-svr 的命令响应投递路径查重试/乱序。
4. **未达"连续两轮 PASS"，不可合并**。预算分离（run20）本身已验证有效，但 run21/22 表明同一栈下 H5 plant 偶发不触发 refresh，需先解决再走 S2/S3。

---

## 7. 追加进展（2026-08-07 晚）：PROCESSING 轮询闭环 + FarmChange 原子性已修复，连续两轮 PASS（run23/run24）

按用户深入分析（§6.3 根因并非"CommandResult 投递不稳定"，而是 Plant 两种合法完成路径只处理了快路径），已完成修复：

### 7.1 问题 A：闭合 PROCESSING → GET_OPERATION_RESULT → COMPLETED → refresh 链

- `command-service.ts` 本已有完整轮询链（PROCESSING → scheduleResultPoll → GET_OPERATION_RESULT → COMPLETED → resolveTerminal），但 **COMPLETED 后没有触发资产 refresh**（refresh 只在 handleFrame 的 `status==='SUCCEEDED'` 结果帧分支触发）。
- 修复（H5 commit `fe15152`）：`handleFrame` 增加 `operationLookup.lookupStatus==='COMPLETED' && completedStatus==='SUCCEEDED' && isAssetAffectingCommand(originalCommandType)` → `markDirty + pumpPlayerBootstrapRefresh`。慢路径下 refreshSent 不再恒 0。
- 快/慢两条路径现在都收敛到同一终态处理。

### 7.2 问题 B：FarmChange 版本与地块 delta 原子应用

- `farm-store.ts`：`applyChange` 先 `validPlotDeltas(change.state.plots)` 全量校验（非空 plotId、plotVersion 存在、status 合法），任一 malformed → 返回 `'gap'`（不修改 farmVersion），由调用方触发 Fresh Join/Resync（fail closed）。
- 新增确定性测试：合法 Plant FarmChange（from=0 through=1, plot0=GROWING plotVersion=2）→ FarmVersion=1 + plot0=GROWING + cropCycleId 一起落地；malformed PlotDelta（空 plotId）→ 整体拒绝、FarmVersion 仍 0。

### 7.3 证据增强

- `gate-vertical-e2e`（backend `935c784`）：protocol-events 现在解析 FARM_CHANGE frame，记录 `delta_count/plot_id/crop_phase/plot_version/crop_cycle_id`，不再只有 from/through version。

### 7.4 验证：连续两轮完整 Smoke PASS

| run | H5 commit | backend | S1A(H5) | S1B(bot) | verdict |
|---|---|---|---|---|---|
| run23 | fe15152 | 935c784 | PASS | PASS | **PASS** |
| run24 | fe15152 | 935c784 | PASS | PASS | **PASS** |

- run23 manifest：backend_dirty=false, h5_dirty=false。
- run24 的 backend_dirty=true 仅因 run23 遗留 evidence 未提交（非代码改动）。
- 脚本 dirty 检测已改为排除 evidence 目录（`docs/verification/results/` 与 H5 `test-results/`/`dist/`），使连续验收不被测试产物掩盖。

### 7.5 下一步

1. **连续两轮 PASS 已达成**（run23/run24）。补跑更多轮确认无 flake（用户规格建议 S1A 连续 10 次）。
2. 后端 bot 仍无 Operation 时间线 witness（T0-T19）；如需完整时间线证据，可在 authority-smoke-bot 增加 process/retry_directive/retry_after_ms 记录（后续增强，非阻塞）。
3. S2（post-ready PresenceEpoch）/ S3（Dispatcher / Delivery Guard 故障）。
4. 全过后合并 backend `935c784` 与 H5 `fe15152`，发布 CP-VERTICAL-A。

---

## 8. S2/S3 故障冒烟已完成（2026-08-07 追加，backend commit `46cbfcd`）

按用户 S2/S3 需求，新增 `cmd/authority-fault-bot` + `scripts/run-authority-fault-smoke`，通过
gate-vertical-e2e 故障注入代理跑确定性故障场景。**连续两轮 4/4 场景 PASS**
（evidence：`docs/verification/results/authority-fault-20260807T124457Z`、`-124517Z`）。

### 8.1 新增工具

- `cmd/authority-fault-bot`：S2/S3 故障驱动。注册 web 账号（owner+guest）→ join → buy → plant，
  经 proxy HTTP admin 注入故障，断言协议不变量，输出 verdict JSON。
- `scripts/run-authority-fault-smoke`：独立 authority 栈部署 + 顺序跑 s3a/s3b/s3c/s2，
  每场景独立 verdict/witness/protocol-events。
- `cmd/gate-vertical-e2e` 修复：`containsFarmChange`/`containsRejoinRequired` 改为解析
  `item.frame` 的 push 类型，不再依赖 item 顶层版本字段（Gate 并不总是填充该字段，
  原实现导致 S3-C 的 FAIL 故障从未触发）。

### 8.2 场景与断言（全部 PASS）

| 场景 | 故障 | 关键断言 |
|---|---|---|
| S3-A | 前 2 次 DeliverBatch 返回 Unavailable | 有界重试、同 item_id、客户端只 +1、无 Fresh Join、健康 Gate 不受影响 |
| S3-B | 第一次转发后被丢响应（Dispatcher 重试） | 同 item_id 重试、Gate 幂等去重（QUEUE_ACCEPTED 或 CONNECTION_NOT_FOUND）、可见效果恰好一次 |
| S3-C | 所有 FARM_CHANGE 永久 Unavailable，REJOIN 放行 | Farm commit 成功（FarmVersion 推进）、REJOIN_REQUIRED 收到、Fresh Join 新 Snapshot 含已提交 Plant（业务事实不回滚） |
| S2 | Hold ActivateSubscription（Join 期间 post-ready Change） | post-ready Change PresenceEpoch>0、Gate 返回 QUEUE_ACCEPTED（非 STALE_CONNECTION）、Ready 先于 post-ready Change、guest 收敛无 gap、无多余 REJOIN_REQUIRED |

### 8.3 修复过程中发现/修正的问题

1. **gate-vertical-e2e `containsFarmChange` bug**：原实现靠 `item.GetFromVersion()>0 || GetThroughVersion()>0`
   判断 FARM_CHANGE，但 Gate 的 DownlinkItem 顶层版本字段并非总是填充（版本在
   `frame` 的 FarmChangePush 内）。导致 S3-C 的 `fail_farm_change_allow_rejoin` 从未拦截
   FARM_CHANGE。改为解析 `frame` 的 push 类型后 S3-C 通过。
2. **S2 guest 收敛**：`join` 在 SUBSCRIPTION_READY 后即返回 snapshot 版本，post-ready Change
   在 Ready 之后才到。新增 `waitConverge` 继续读帧直到 FarmVersion 收敛到 owner 提交版本。
3. **S2 好友关系**：guest 访问 owner 农场需好友（`ERR_NOT_FRIEND`）。新增 share/create + accept。
4. **S3-B 断言时序**：客户端在第一次投递（Gate 已接受）就收到 FARM_CHANGE，而 Dispatcher
   重试滞后几百 ms；改为轮询 events 直到同 item_id 重试 DELIVER 出现。
5. **setPlan 竞态**：S3-C/S2 在 plant/join 前 `verifyPlan`（set 后读回确认生效）+ 小 sleep，
   避免 FARM_CHANGE 在故障生效前投递。

### 8.4 当前验收状态

- 正常 Smoke：run23/24/25 三轮 PASS（S0 + S1A real H5 + S1B bot）。
- 故障 Smoke：S2/S3 连续两轮 4/4 PASS。
- CP-VERTICAL-A-RC1 冻结：backend `5fe0f2e`（代码）、H5 `fe15152`；正常 smoke 证据在
  `results/authority-vertical-20260807T041925Z-2131722/`。
- 后续 S2/S3 证据 commit `46cbfcd`（不影响被冻结的代码 commit；S2/S3 是测试工具，非业务功能）。

### 8.5 待办（非阻塞）

- 若需生产级 S2/S3 覆盖，可将 fault-bot 接入 CI 定时跑（当前是本地脚本）。
- Operation 时间线 witness（T0-T19）仍可增强，非阻塞。
