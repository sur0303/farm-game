# A1~A4 Premerge 候选与 Authority Smoke 交接文档

日期：2026-08-06
作者：CodeBuddy agent
状态：**构建完成，Smoke 进行中（S0 通过，S1 部分通过，发现 WS Origin 修复）**

---

## 1. 背景与目标

将 A1~A4 四个工作包组合为两个 Premerge 候选，并用 **Authority Store + Static Assignment** 的真实链路 Smoke 验证主链。

- **A1**（H5 仓库）：H5 恢复屏障、Bootstrap 严格解码、MATURE 规则、乱序恢复
- **A2**（farm-game）：PlayerBootstrap 走 TaskDomain + Authority CAS Commit + CommittedRecordVersion
- **A3**（farm-game）：FarmChange 投递失败后 RealtimeInvalidation + REJOIN_REQUIRED + Fresh Join
- **A4**（farm-game）：Downlink Dispatcher 重试、关闭、并发边界硬化

**为什么必须 Authority 模式**：Legacy 模式下 typed Participant / Plant / Harvest / Theft 全部 disabled，A2/A3/A4 的核心语义无法被真实证明，会得到假阳性 PASS。当前组合为 **Authority Store + Static Assignment Snapshot + Dynamic Watch OFF**。

---

## 2. 分支与 commit

### 2.1 后端 `farm-game` 仓库

**分支**：`codex/premerge-a-backend-20260806`（已推送 origin）

Commit 栈（自下而上）：

| Commit | 内容 |
|--------|------|
| `935b51e` | integration 基线（h5-mainline-fix） |
| `b6a8717` | A2 feat: wire GET_PLAYER_BOOTSTRAP through TaskDomain |
| `1cac975` | A2 chore: SetBootstrapTaskDomain in player-svr composition |
| `9479626` | A2 test: single-now capture + apply-then-timeout requery |
| `064367f` | A4 feat: harden dispatcher lifecycle, capacity, budget closure |
| `6284422` | A3 farm-ingress: proven-accepted delivery guard |
| `0411892` | **test(authority-smoke): fixture + proxy + run scripts（本次新增）** |

`0411892` 包含：
- `cmd/authority-smoke-fixture/`（init + audit 子命令）
- `cmd/gate-vertical-e2e/`（Downlink 故障注入 proxy）
- `cmd/smoke-authority-init/`（早期初始化工具，被 fixture 取代，保留作参考）
- `scripts/run-authority-vertical-smoke`（整合脚本，0/1/2 退出码）
- `scripts/smoke-authority-start.sh`（本地 Authority 环境启动）
- `deploy/docker-compose.local.yml`（本地 compose 修正：farm-svr healthz、zoneinfo 挂载、THEFT_PRF_MODE）
- `docs/verification/results/`（Smoke 证据 JSON）

### 2.2 前端 `classic-farm-h5` 仓库

**分支**：`codex/premerge-a-h5-20260806`（已推送 origin）

| Commit | 内容 |
|--------|------|
| `9a204fc` | A1: bootstrap recovery barrier + navigation fallback + quantity payloads（= integration HEAD） |
| `6079b84` | **fix(ws-proxy): /ws changeOrigin:false（本次新增，修复真实页面 WS 失败）** |

`6079b84` 包含：
- `vite.config.ts`（`/ws` 代理 `changeOrigin:false` 修复）
- `playwright.authority-smoke.config.ts`
- `src/test/e2e/authority-vertical-smoke.spec.ts`

---

## 3. 已完成验证

### 3.1 定向测试（全部 PASS）

| 工作包 | 检查 |
|--------|------|
| A2 | `go test ./internal/player/ingress/... ./internal/player/task/...` |
| A4 | `go test ./internal/logic/downlink/...` + `go test -race` |
| A3 | `go test ./internal/farm/ingress/...` + `go test -race ./internal/farm/ingress/... ./internal/logic/downlink/...` |
| A1 | `npm test -- --run`（92/92）、`npx tsc --noEmit`、`npm run build` |

### 3.2 Authority Smoke（S0 通过）

`scripts/run-authority-vertical-smoke` 的 S0 部分已验证：
- player-svr / farm-svr 日志含 `authority mode enabled`，无 legacy fallback
- farm-svr `readyz` 通过（Authority 模式下 TheftIngress 可用）
- ShardWriteAuthority Claim 成功（player/farm 各 1 shard，writer_epoch>0）
- 四服务（player/farm/worker/gate）全部 ready

### 3.3 H5 真实页面（S1 部分通过）

通过 Playwright + 真实后端验证：
- ✅ 注册（Web 账号模式）→ 201 + connection_ticket
- ✅ WSS 连接 → InFarm
- ✅ `data-bootstrap-state=applied`（A2 Bootstrap 生效）
- ✅ `data-recovery-barrier=open`（A1 恢复屏障生效）
- ✅ Buy 命令：种子 0→1（Authority Commit 生效）
- ❌ Plant 命令：种子未减少（见 §5 遗留问题）

---

## 4. 环境搭建与运行

### 4.1 启动 Authority 后端（四服务）

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-premerge-a-20260806
./scripts/smoke-authority-start.sh        # 前台运行，Ctrl+C 停止
```

脚本自动：
1. 编译 5 个服务（player-svr/farm-svr/farm-worker/farm-gate/smoke-authority-init）
2. 从 `deploy/secrets/redis.conf.local` 读 Redis 配置
3. 运行 `smoke-authority-init` Claim PLAYER/FARM ShardWriteAuthority + 生成 route-snapshot.json
4. 按顺序启动 player-svr → farm-svr → farm-worker → farm-gate

**注意**：
- namespace 每次运行唯一（`smoke-<timestamp>`），避免 ShardWriteAuthority lease 冲突
- Redis 用公司实例 `198.18.0.14:6380`（secret 在 redis.conf.local），每个 run 用独立 key 前缀，不会污染生产
- 端口固定：player:19600/19601、farm:19500/19501、worker:19701、gate:18500/19592/19593
- **Gate `GATE_WS_ALLOWED_ORIGINS` 已包含 `127.0.0.1:5174` 和 `127.0.0.1:18500`**

### 4.2 启动 H5 dev server

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-premerge-a-20260806
VITE_API_PROXY_TARGET=http://127.0.0.1:18500 \
VITE_WS_PROXY_TARGET=http://127.0.0.1:18500 \
npm run dev:web -- --host 127.0.0.1 --port 5174
```

**关键**：
- **不要设置 `VITE_WS_URL`**！设置了会让 H5 直连而非走 vite 代理，导致超时
- H5 走同源 `/ws`，由 vite 代理转发到 Gate
- `/ws` 代理必须 `changeOrigin:false`（见 §5 根因 1）

### 4.3 跑完整 Smoke

```bash
./scripts/run-authority-vertical-smoke   # 0=PASS 1=FAIL 2=INCONCLUSIVE
```

---

## 5. 遇到的问题与根因（重要）

### 5.1 根因 1：真实页面 WebSocket transport error（已修复）

**症状**：浏览器连接 `ws://127.0.0.1:5174/ws` → `WebSocket open timed out after 10000ms` / `WebSocket transport error`。Gate 日志只有 `connected` + 5 秒后 `closed`，无 ClientHello 处理。

**根因**：vite `/ws` 代理 `changeOrigin:true` 把 WebSocket 升级请求的 **Origin 头改写成代理 target**（`http://127.0.0.1:18500`）。farm-gate 校验 Origin 时 `u.Host=127.0.0.1:18500` 不在 `GATE_WS_ALLOWED_ORIGINS` 白名单（只有 5173/5174/5184），**拒绝升级**。

**为什么之前 Playwright 测试能连上**：Playwright 直连 `ws://127.0.0.1:46657/ws`（不走 vite 代理），Origin 保持浏览器地址，白名单匹配通过。

**修复**（H5 `6079b84`）：
- `vite.config.ts`：`/ws` 代理 `changeOrigin:false`，保留浏览器 Origin
- Gate 白名单补 `127.0.0.1:18500`（双保险）
- 已验证：Node 原生 WebSocket 经 vite 代理 → Gate 握手 101 成功

### 5.2 根因 2：金币/种子显示 "…"（未完全解决）

**症状**：进入农场后金币/种子显示 "…"，`data-private` 未填充。

**分析**：H5 `privateReady = wallet!==null && seeds!==null`，显示 "…" 说明 Bootstrap 的 `privateState` 未应用。`applyPlayerBootstrap(privateState)` 在 `privateState===undefined` 时直接 return。

**已确认**：
- player-svr 日志 `[BOOTSTRAP] ... fallback to fresh activation; serving locally`——Bootstrap 被处理
- H5 `final-protocol-adapter.ts` 有 `GET_PLAYER_BOOTSTRAP` 解码分支，期望 `decoded.wallet?.coin` 和 `decoded.inventory?.items`
- 服务端 `buildPlayerBootstrapResult` 返回 `wallet.coin` + `inventory.items`——结构表面匹配

**未确认**：为什么 `result.privateState` 最终为 undefined。**待查**：命令响应帧（`GetPlayerBootstrapResult`）从 player-svr → farm-gate → H5 传递过程中，payload 是否完整到达（可能被 gate 的 command dispatch 过滤或截断）。**注意**：真实页面验证时 H5 显示 "…"，但 Playwright 中 `data-bootstrap-state=applied` 通过——两个观察存在矛盾，需要核对 `data-bootstrap-state` 与 `privateState` 的更新时序。

### 5.3 根因 3：Plant 命令未生效（未解决）

**症状**：Buy 成功（种子 0→1）后 Plant，种子未减少（预期 1→0），但地块乐观更新显示已种植。

**分析**：H5 乐观更新地块，但服务端 PLANT 命令未确认（种子未扣减）。可能：
- PLANT 命令被 Gate/服务端拒绝（恢复屏障、权限、Availability）
- 或 H5 的 `data-quick-seed` 交互未真正发出命令
- **待查**：farm-svr / player-svr 日志中无任何 BUY/PLANT 命令记录（日志级别可能不打印命令），需在 Gate 加命令级日志或用 Go WSS 客户端直连验证

### 5.4 环境注意点（已踩坑）

- **ShardWriteAuthority lease 冲突**：同 namespace 重复启动会 `ACTIVE_LEASE_CONFLICT`（TTL 55s）。已改为每次运行唯一 namespace
- **Gate bind 失败**：残留进程占用 19593 导致 Gate 启动即退。启动前 `lsof -i :19593` 确认空闲
- **farm-svr readyz**：Legacy 模式 TheftIngress 不可用导致 readyz 503。Authority 模式无此问题；compose 环境用 `healthz` 规避
- **Dockerfile tzdata**：Alpine 镜像缺 `Asia/Shanghai`（A2 TaskDomain 需要），apk 网络不可达，改用挂载宿主机 `/usr/share/zoneinfo`

---

## 6. 遗留问题与下一步建议

### 6.1 优先级排序

1. **金币/种子 "…"（根因 2）**——阻塞 S1 主链验收，需先定位
2. **Plant 未生效（根因 3）**——可能同根因（命令链路未通）
3. **S2/S3 故障注入场景**——proxy 已实现但未接入完整 Smoke
4. **H5 金币显示与 bootstrap-state 矛盾**——核对更新时序

### 6.2 推荐下一步

1. **用 Go WSS 客户端直连 Gate**（`cmd/gate-vertical-e2e` 增加 `run` 子命令或写独立 probe），走完整协议链（登录→ticket→ClientHello→JOIN→BUY→PLANT），100% 区分客户端/服务端问题。这是当前最缺的确定性诊断手段。
2. 在 Gate command dispatch 增加命令级日志（`DEBUG` 级别），确认 ClientHello/JOIN/BUY/PLANT/GET_PLAYER_BOOTSTRAP 各帧是否到达。
3. 修复 6.1-1/2 后，跑完整 `run-authority-vertical-smoke`，期望 S1 PASS。
4. 然后接入 proxy 故障注入跑 S2（post-ready PresenceEpoch）和 S3（Dispatcher/Delivery Guard）。
5. 全部通过后，由 agentctl/I0 合入 integration（Agent 不自行合入）。

### 6.3 回归风险

- H5 `vite.config.ts` 改动只影响本地 dev（`changeOrigin:false`），staging/prod 不走 vite 代理，无影响
- 后端 `deploy/docker-compose.local.yml` 只影响本地 compose，生产不受影响
- `cmd/` 新增工具均为测试限定，不接生产线

---

## 7. 关键文件索引

| 文件 | 说明 |
|------|------|
| `cmd/authority-smoke-fixture/main.go` | Authority init/audit 工具 |
| `cmd/gate-vertical-e2e/main.go` | Downlink 故障注入 proxy（S2/S3 用） |
| `scripts/smoke-authority-start.sh` | Authority 环境一键启动 |
| `scripts/run-authority-vertical-smoke` | 整合 Smoke 运行器 |
| `docs/verification/results/authority-vertical-20260806T093045Z-3764551/` | S0 PASS 的完整证据 |
| H5 `playwright.authority-smoke.config.ts` | Playwright 配置（web 模式） |
| H5 `src/test/e2e/authority-vertical-smoke.spec.ts` | S1 场景用例 |

---

## 8. 一句话结论

**A1~A4 定向测试全过，Authority Smoke S0 通过，S1 的真实页面验证已打通注册/WSS/Bootstrap/Buy，剩余金币显示与 Plant 两个点需要继续用确定性 WS 客户端定位；同时已修复真实页面 WS 失败的根因（vite changeOrigin 破坏 Gate Origin 白名单）。**
