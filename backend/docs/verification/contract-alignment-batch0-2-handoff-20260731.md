hdi# 26/27/28 合同对齐 · Batch 0–2 交接文档

日期：2026-07-31  
交接对象：下一位开发 / 审核 / 架构 Agent  
权威施工计划：[`docs/design/60-contract-alignment-plan.md`](../design/60-contract-alignment-plan.md)  
冻结合同：26（接入与会话）、27（路由 / Actor / Workflow）、28（CommandCatalog 与公网协议）  
（三份合同正文尚未归档进本目录；业务语义以对话中冻结的正式稿为准，本交接与 60 号计划不得反向改合同。）

仓库：

| 仓库 | 路径 | 角色 |
| --- | --- | --- |
| farm-game | `/data/home/developer_026/CodeBuddy/20260519141303/farm-game` | 服务端（本交接主体） |
| classic-farm-h5 | `/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5` | H5 客户端（Batch 0.2 / 0.3 有提交） |

**注意**：`farm-game/web/` 是空目录。凡涉及客户端的改动都在 `classic-farm-h5`，不要误判成「H5 未改」。

---

## 1. 一句话状态

已按 `60-contract-alignment-plan.md` 完成 **Batch 0（地基）+ Batch 1（Gate Catalog 进程内）+ Batch 2（Actor Runtime / 路由控制面进程内）**。  
**未改冻结信封 proto**（`ClientCommandEnvelope` / `CommandResultEnvelope` / `ServerPushEnvelope` / `FarmFrame`）；公网字段与 RouteIngress RPC 属 **Batch 3**，卡在 29/30/31/32 + field number 冻结。

当前可运行的仍是：H5 + farm-gate + player-svr + farm-svr + farm-worker + 公司 Redis 的本地纵切；Batch 2 新增的是进程内能力与指标，多数 **尚未挂满生产主链**（见 §6）。

---

## 2. 分支与 HEAD

### farm-game

```text
branch: codex/v03-access-session-hardening-20260730
关键补丁: 057aa09 fix(farm): route lifecycle work through SystemQueue
（其后为文档钉 hash / handoff 同步提交；以 `git log -5 --oneline` 为准）
```

工作区应干净。历史误放的 `docs/verification/gf-*.json` 来自 **另一分支**
`codex/v03-access-session-contract-20260730` 的 gate 快照，**不是**本 hardening 分支产物，
已删除；勿再提交、勿当作当前 `gate.go`。开始工作前：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
git branch --show-current
git status --short
git log -15 --oneline
```

### classic-farm-h5

```text
branch: codex/v03-access-session-hardening-20260730
相关提交:
  4fa5a6d fix: keep operation_id stable across retries of one Join intent   # 0.2 配套
  d96cbe1 feat: Fresh Join on RESYNC_REQUIRED and local farm_version gaps  # 0.3 配套
```

两仓加固分支 historically 受环境限制可能未推远程；接手后先确认 `git status` / tracking，**不要**在未确认前 force-push。

---

## 3. 决策与边界（接手必读）

| 决策 | 结论 |
| --- | --- |
| Gen1 | **删除**：`cmd/farm-logic`、`cmd/farm-gateway`、`internal/logic/{ingress,routing}`、Gen1 `PlayerFarmActor`。保留 `internal/logic/actor/runtime`、`downlink`、`trusted`、`logicrpc` |
| ownerstore | **保留但隔离**（Tcaplus 探针）；切断 Gate 静默 fallback |
| Proto wire | **Batch 0–2 不改信封**；遗留字段经 `LegacyEnvelopeAdapter` 过渡，等 29 号删除 |
| Hash 路由 | Batch 2.3 **保持** HashVersion v1 字节布局；golden player=379 / farm=136 @ 1024 **不变** |
| 审核习惯 | 关键验收测试由审核者**自己重跑**；不要只信 Agent 报告。勿把 commit hash 用脆弱 `sed` 写进计划（曾弄坏 Slice C 正文，已修） |

---

## 4. 本轮完成总览

### 4.1 Batch 0 · 零依赖前置（已完成）

| 项 | Commit | 做了什么 |
| --- | --- | --- |
| 0.1 + 0.6 | `ab5a2c7` | 删除 Gen1；切断 ownerstore 回落 |
| 0.5 | `77c967a` | blobstore 五分类 CAS + Unknown Outcome API |
| fencing 修复 | `92249d6` | 失权 FarmActor 移出 registry（真问题是 fence 后仍陈旧读缓存） |
| 0.2 | `a98d204` + H5 `4fa5a6d` | 服务端签发 `subscription_id`；Join 幂等用 runtime map，不写 FarmRecord |
| 0.3 | `49d1c53` + H5 `d96cbe1` | 多路径下发 `RESYNC_REQUIRED`；H5 Fresh Join；堵住静默 leave |
| 0.4 | `0aa38f6` | `ErrRouteLeaseExpired` / `ErrStaleSession` 互斥；`farm_gate_command_rejected_total{reason=...}` |

**Batch 0 仍开放的缺口**（计划 §3 表）：

- 纯读路径不复核 writer epoch → 失权进程可能继续用缓存供读（属 Actor Lease，非 Batch 0）
- `CompareAndSwapWithRetry` 基本只有测试在用
- Join snapshot payload 内容断言偏弱
- Resync 自己也入不了队 → log + 清 presence

### 4.2 Batch 1 · Gate CommandCatalog（进程内，已完成）

| Slice | Commit | 做了什么 |
| --- | --- | --- |
| A | `22602c0` | `internal/gateway/commandcatalog/`：11 Specs；拒 unknown/UNSPECIFIED；**严格 seq +1** |
| B | `8830e3c` | `LegacyEnvelopeAdapter` TODO(29)；PLANT `CanonicalHash`；`OWN_CURRENT_FARM` / `CURRENT_FARM` 准入（logic 不调用测试） |
| C | `9bcc688` | payload 过大 / decode 失败；`ADMITTED` 指标；`PushScope`（`internal/gateway/downlink/push_scope.go`） |
| D | `58eaa6b` | `DecideRetryDirective`：TIMEOUT→POLL（禁止盲 RETRY_SAME）；**未上线格式字段** |

**Batch 1 遗留**：RateLimit 算法、protocol_version/Availability、RouteKey 边角、非 PLANT canonical、RetryDirective 上线（Batch 3）。

### 4.3 Batch 2 · 第 27 号进程内部分（本会话主体，已全部完成）

| 项 | Commit | 做了什么 |
| --- | --- | --- |
| 2.1 | `c0eb836` | UserMailbox（item+byte）+ SystemQueue；系统优先调度 |
| 2.2 | `4990ecd` | `activation.Controller` FSM / 加载风暴 / panic 监督；Farm Registry 接线 |
| 2.3 | `71b80f0` | `RouteSnapshot` / `DomainSnapshot` / `ShardRoute` / `HolderRef`；旧 JSON 双读 |
| 2.4 | `f28da60` | `AssignmentProvider` + Static + `atomic.Value` 换表 + revision gap 全量刷新 |
| 2.5 | `168acb4` | 进程内 `RouteRedirect` / `RetryingResolver`；同 `operation_id`；无代理 |
| 2.6 | `6b0df45` | 路由 / mailbox / activation Prometheus 真打点 |

文档钉 hash：`5c0c4d8` / `bf9df33` / `90f3db3` / `e2f60e2` / `244df3e` / `22b3d09` 等。

---

## 5. Batch 2 详细说明（能力与落点）

### 5.1 UserMailbox / SystemQueue（`internal/logic/actor/runtime`）

**问题**：单一有界 mailbox 满时，`LeaseLost` / `Drain` / `PersistenceCompleted` 进不来 → Actor 卡死或以失效写权继续跑。

**现状**：

- `Submit` / `SubmitUser` / `SubmitUserSized` → UserMailbox  
- `SubmitSystem(ctx, key, kind, task)` → SystemQueue（独立容量，默认 32）  
- 同 key 单 goroutine；循环先非阻塞取 system，再 `select` system/user/closing  
- User 满：`ErrMailboxFull` / `ErrActorBusy`（`errors.Is` 互认）+ `RetryAfterBusy=100ms`  
- System 满：`ErrSystemQueueFull`（与 user 满隔离）

**硬验收**：`TestSystemQueueProgressesWhenUserMailboxFull`（27 §6.9 #10）。

**生产接线（已补，P0）**：Farm registry 已把生命周期路径接到 SystemQueue：

| API | Kind |
| --- | --- |
| `NotifyLeaseLost` | `LeaseLost` |
| `FinishPassivate` | `Passivate` |
| `CleanupConnection` / `CleanupConnectionFence` | `Stop` |
| `SubmitSystem` | 通用入口 |

验收：`TestRegistrySystemPathProgressesWhenUserMailboxFull`（User 满时 `NotifyLeaseLost` / `CleanupConnectionFence` 仍可完成）。

仍未接：`PersistenceCompleted` / `Drain` 的业务事件源；路由层主动调用 `NotifyLeaseLost` 的触发点；用户 turn 内 epoch mismatch / panic 保持**同步**处理（避免同 key 再入队死锁）。

**Player**（已补 SystemQueue API）：`SubmitSystem` / `NotifyLeaseLost` +
`TestPlayerSystemPathProgressesWhenUserMailboxFull`。Player **无** Presence /
`CleanupConnection`（断线清理在 farm-svr）；无缓存 actor 可丢弃，LeaseLost turn
是可调度屏障。完整 activation FSM 仍未接。

### 5.2 激活 FSM（`internal/logic/actor/activation`）

状态：

```text
ABSENT → LOADING → ACTIVE → (RESOLVING_UNKNOWN | DRAINING | FENCED | PASSIVATING) → ABSENT
```

默认 Options：`MaxConcurrentLoads=64`、`MaxPendingLoads=256`、`ActivationRatePerSec=1000`（**进程级** token bucket）、`LoadFailureBackoff=200ms`。

**PASSIVATING 双路径**（已测）：

1. `writable==true` → 取消钝化回 `ACTIVE`，复用同一 actor 指针  
2. `writable==false` → `ErrStaleRoute`，保持 PASSIVATING，**不得**把旧指针标回 ACTIVE；`FinishPassivate` 后重激活用新指针  

**Runtime panic**：`execJob` recover → `ErrActorPanic` + `OnPanic(key)`；mailbox 不退出；Farm registry 丢弃缓存并 `OnPanic` → ABSENT。

**接线范围**：

| 组件 | 状态 |
| --- | --- |
| Farm `Registry` | 已接 Admit / BeginLoad / fence MarkAbsent / Passivate API / panic |
| Player `Registry` | **未**接完整 FSM（无缓存激活；仅注释） |
| idle passivation timer | **未做**（只有可测的 `BeginPassivate` / `FinishPassivate`） |
| `RESOLVING_UNKNOWN` 业务 | 状态可设，业务未接 |

### 5.3 路由改形（`internal/actorrouting`）

| 旧 | 新 |
| --- | --- |
| `Kind` | `RouteDomain`（`Kind` 仍为别名） |
| 扁平 `Assignment[]` | `RouteSnapshot` + `DomainSnapshot.Routes[]` + `Holders[]` |
| `routing_version` / `writer_epoch` | `HashVersion` / `ExpectedWriterEpoch`（Resolve 投影仍为 `Assignment.WriterEpoch`） |

- Hash v1 字节布局**未变** → golden **379 / 136** 保持  
- `ParseRouteSnapshotJSON` **双读**旧扁平 JSON；`deploy/actor-route.static.example.json` 已改新格式  
- fail-closed：缺任一 shard → `ErrInvalidSnapshot`  
- 生产调用方 API（`LoadStaticResolver` / `Resolve` / `Address`）尽量兼容，Gate 主链未被迫大改

### 5.4 AssignmentProvider（2.4）

- 接口：`GetSnapshot` / `Watch`  
- `StaticAssignmentProvider`：启动校验；Watch **不推送**，ctx 取消关 channel  
- `StaticResolver.Replace`：`atomic.Value(*routeTable)` 整表替换；非法快照拒绝，旧表继续  
- `SnapshotApplier`：维护 `lastGlobalRevision`；`rev > last+1` → `GetSnapshot` 全量刷新  
- Discovery Provider：**未做**

### 5.5 Redirect（2.5，进程内 only）

- `RouteRedirect` / `RedirectApplier.ApplyRedirect` / `RetryingResolver.Invoke`  
- 只接受 **严格更新** 的 GlobalRevision（及可选 ShardRevision）  
- 重试保持原 `operation_id`；超限 → `ErrTooManyRedirects`  
- **无 `Forward` API**；旧 Holder 用 `HandleLocalMiss` 更新本地视图后返回 `RedirectError`  
- **未**落 RPC / proto 字段（属第 29 号）

注意：`HandleLocalMiss` 会先 `ApplyRedirect` 再返回 `RedirectError`。它面向**旧 Holder 本进程**；调用方进程的 `RetryingResolver` 应对**自己的**表再 Apply。不要在同一 resolver 上把 `HandleLocalMiss` 塞进 `Invoke` 的 AttemptFunc，否则会二次 Apply 撞 stale。

### 5.6 指标（2.6）

路由：`farm_actor_route_resolve_total`、`redirect_total`、`route_snapshot_revision`、`route_snapshot_age_seconds`、`writer_epoch_mismatch_total`。  
Actor：mailbox bytes / oldest_wait（user）、activation 各状态当前计数、throttled/backoff/panic/load_fail。

注册：`RegisterRouteMetrics`、`RegisterActivationMetrics`；`runtime.RegisterMetrics` 签名兼容扩展。  
生产：`cmd/farm-svr`（route+runtime+activation）、`cmd/player-svr`（route+runtime）。

缺口：状态 duration 直方图；system queue oldest_wait；Applier 的 StaleIgnored/Duplicates/GapReloads 未单独升 Prometheus。

---

## 6. 生产接线真实程度（避免高估）

| 能力 | 代码就位 | 生产主链 |
| --- | --- | --- |
| CommandCatalog + seq +1 + target-farm 准入 | 是 | Gate 已用 |
| RetryDirective 决策函数 | 是 | **未上线格式** |
| User/System 双队列 | 是 | Farm：LeaseLost/Passivate/Cleanup 已走 System；Player：`NotifyLeaseLost`/`SubmitSystem` 已接；业务仍 `Submit` user |
| 激活 FSM + 风暴门 | 是 | **Farm 已接**；Player 未接；无 idle timer |
| RouteSnapshot JSON | 是 | Static 加载路径可用；Discovery 无 |
| Redirect / RetryingResolver | 是 | **进程内库**；Gate/RPC 未挂 |
| RESYNC_REQUIRED | 是 | farm → Gate downlink；H5 Fresh Join |
| 信封 / FarmFrame oneof | 否 | Batch 3 |

---

## 7. 关键验收测试（接手时优先重跑）

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
export -n FARM_REDIS_URL   # 或 env -u FARM_REDIS_URL

# Batch 2 核心
go test -race -count=1 ./internal/logic/actor/runtime/ \
  -run 'TestSystemQueueProgressesWhenUserMailboxFull|TestSystemPriorityOverUser|TestSystemQueueFullIsolatedFromUser'

go test -race -count=1 ./internal/logic/actor/activation/ ./internal/farm/actor/ \
  -run 'TestFSM_|TestLoadStorm_|TestPassivating_|TestRegistryPassivating|TestRegistryLoadStorm|TestRegistryPanic|TestRegistryEvicts'

go test -race -count=1 ./internal/actorrouting/ \
  -run 'TestCalculatorHasFrozen|TestStaticResolverRequires|TestStaticAssignment|TestStaticResolverReplace|TestSnapshotApplier|TestApplyRedirect|TestRetryingResolver|TestOldHolder|TestRouteMetrics'

# 回归面
go build ./...
env -u FARM_REDIS_URL go test -count=1 ./...
```

H5（若动客户端）：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
# 按该仓既有脚本跑与 RESYNC / Join 相关单测
```

容量 / 双 Gate 等回归清单见计划 §7（`test-dualgate-kick`、`run-local-capacity-stage`、PRE_AUTH smoke）。本交接对应的 Batch 2 改动以单测为主，**未**要求重跑 10m 浸泡才算完成。

---

## 8. 下一步建议

### 8.1 可立即做的补洞（不依赖 29/30/31/32）

1. ~~**把系统消息接入 `SubmitSystem`**~~ **已做（Farm）**：`NotifyLeaseLost` / `FinishPassivate` / `CleanupConnection*`。余下：`PersistenceCompleted` / `Drain` 事件源、以及路由层在失权时**调用** `NotifyLeaseLost`（若尚无外部调用方，能力在 registry，触发点需挂 assignment/lease 失权信号）。  
2. **Player 激活 FSM**（若要与 Farm 对称）或明确永久保持「无缓存、每 turn 解析」。  
3. **idle passivation timer** + Presence/脏写约束（勿在有未持久化事实时钝化）。  
4. **纯读路径 epoch 复核**（缩短失权陈旧读窗口）。

### 8.2 必须等合同的 Batch 3

```text
FarmFrame oneof / 信封瘦身与补字段
RetryDirective / retry_after_ms / PushScope 上线格式
删除 LegacyEnvelopeAdapter
ActorRouteHeader + RouteIngress 七校验          → 29
Redis Key / Lua CAS / WriterEpoch / DueIndex   → 30/31
持久化分级 / Workflow 状态机 / 偷菜护卫         → 32
多 Pod Shard Lease、Handoff、Discovery Provider
```

在 field number 冻结前，**禁止**为了「先接上」而改冻结信封并靠兼容凑合。

---

## 9. 文件地图（Batch 2 新增 / 主改）

```text
internal/logic/actor/runtime/
  executor.go          # 双队列 + panic recover + oldest_wait
  metrics.go

internal/logic/actor/activation/
  activation.go        # FSM / 风暴 / Admit
  metrics.go
  *_test.go

internal/actorrouting/
  routing.go           # RouteSnapshot 改形 + atomic 换表
  provider.go          # AssignmentProvider / SnapshotApplier
  redirect.go          # RouteRedirect / RetryingResolver
  metrics.go
  deploy 样例: deploy/actor-route.static.example.json

internal/farm/actor/registry.go   # 激活接线 + route metrics
cmd/farm-svr/main.go              # RegisterRoute/Activation metrics
cmd/player-svr/main.go

docs/design/60-contract-alignment-plan.md   # 进度与验收表（权威）
```

---

## 10. 工作方式备忘（给下一任 Agent）

1. 编排者审核：重跑能红能绿的测试，再 commit；不要 amend 远程已推提交。  
2. 子 Agent 模型偏好（本轮）：`cursor-grok-4.5-high-fast`（LUNA 额度问题后切换）。  
3. 不要提交 `docs/verification/gf-*.json`。  
4. 钉计划里的 commit hash 用精确字符串替换 / 小 Python，避免 `sed` 吞掉 `*`。  
5. Join 幂等是 **runtime map**，不要再改回写入 FarmRecord。  
6. `HandleLocalMiss` vs `RetryingResolver` 的 Apply 职责见 §5.5。

---

## 11. 提交索引（方便 `git show`）

### Batch 0（节选）

```text
ab5a2c7 refactor: remove the Gen1 farm-logic runtime and its ownerstore fallback
77c967a          # blobstore CAS 五分类（见计划表）
92249d6 fix(farm): drop a fenced actor instead of serving its stale cache
a98d204 fix(gate): let farm-svr issue subscription_id instead of reusing request_id
49d1c53 feat(farm): emit RESYNC_REQUIRED instead of silently dropping presence
0aa38f6 feat(gate): distinguish route lease expiry from stale session
```

### Batch 1

```text
22602c0 feat(gate): add CommandCatalog and enforce strict connection sequences
8830e3c feat(gate): enforce target-farm admission and PLANT canonical hashes
9bcc688 feat(gate): admit with payload limits and fence farm push scope
58eaa6b feat(gate): map ErrorCode to RetryDirective without wire changes
```

### Batch 2

```text
c0eb836 feat(actor): split UserMailbox and SystemQueue in keyed runtime
4990ecd feat(actor): add activation FSM, load storm gates, and panic supervision
71b80f0 feat(routing): reshape static snapshot to RouteDomain contract types
f28da60 feat(routing): add AssignmentProvider with atomic static table swap
168acb4 feat(routing): add in-process RouteRedirect with bounded same-op retries
6b0df45 feat(observability): wire route and actor activation prometheus metrics
6b0df45 feat(observability): wire route and actor activation prometheus metrics
057aa09 fix(farm): route lifecycle work through SystemQueue   # SystemQueue 生产接线
```

### H5

```text
4fa5a6d fix: keep operation_id stable across retries of one Join intent
d96cbe1 feat: Fresh Join on RESYNC_REQUIRED and local farm_version gaps
```

---

## 12. 验收口径复述

- **Batch 0–2 完成** = 计划文档对应节勾完 + 表内 commit 存在 + 关键测试本地可复现绿色。  
- **不等于** 26/27/28 合同生产闭环，也不等于多 Pod 动态路由 / 信封最终形态。  
- 细节与指标全表以 `60-contract-alignment-plan.md` 为准；本文是接手导航，冲突时以计划与代码为准。
