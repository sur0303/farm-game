# S2 当前工作区施工交接

版本：`2026-08-01-real-backend-only-stage6-next-web-runbook`

日期：2026-08-01

接手对象：下一位开发、验证或架构 Agent

工作区根目录：`/data/home/developer_026/CodeBuddy/20260519141303`

> **权威头（2026-08-02 更新）**：本文是阶段 0~6 的历史交接与运行手册，当前交接已移至
> **`s7-handoff.md`**。当前设计权威是 `docs/design/CONTRACT-MANIFEST.md` 登记的 26～33 号合同；
> 60 号定义验收，70 号定义施工顺序，当前源码与实施记录提供实现事实。若本文与它们冲突，
> 必须以当前合同、验收、计划和源码为准。阶段 0~6 已完成并提交；阶段 7 已按 70 号计划重排，
> 本文中"下一项主线是阶段 6"等表述不再代表当前状态；§10~§11 的启动与验证操作步骤
> 仍现行有效。

## 0. 先读结论

当前已经不是 2026-07-26 那个“TCONND/gateproxy 只差 3 行映射”的状态。本文旧版把它写成
唯一阻塞，现已证明确认不成立：TCONND `0.0.13` 的 `BY_WS` 能完成 WebSocket 101，
但没有把 Web 事件经 TBUS 交给 gateproxy；仅修改 gateproxy 不能打通该版本的端到端链路。

当前业务开发与真实浏览器验证使用：

```text
H5
  ├─ HTTPS/JSON ──> farm-gate AuthHTTP
  └─ Direct WebSocket/Protobuf ──> farm-gate
                                      ├─ player-svr
                                      └─ farm-svr ──> farm-worker
                                              │
                                              ▼
                                         公司持久 Redis
```

在当前脏工作区中，PR-0、PR-1A～1C、PR-2、阶段 3、阶段 4、阶段 5 已实现并完成相应
单测、Race、构建和真实 Web/Staging 验证。下一项主线是阶段 6：完成 Actor Activation 的
自动钝化、Mailbox/goroutine/Controller 条目回收和安全重新激活。

2026-08-01 最新增量：H5 可运行 Mock 产品环境已经删除。`npm run dev`、`npm run build` 和
`npm run e2e` 默认均为真实 Web 账号链路；Staging/WeChat 也只使用 canonical v0.2 与真实
farm-gate。`farm-gate`、`player-svr`、`farm-svr` 缺少 `FARM_REDIS_URL` 会拒绝启动，不能再
静默退回进程内 Store。Vitest 中仍有 `src/test/helpers/` 下的纯测试 double，但任何产品入口、
Vite 构建和 Docker 构建都不会导入它们。

最重要的交接风险是：两个仓库的阶段改动都还没有提交到当前新分支。接手 Agent 不得执行
`git reset --hard`、`git checkout -- .`、清理 untracked 文件、切回 master 后覆盖，或把远端旧分支
强行拉到当前工作区。

## 1. 当前仓库快照

以下是 2026-08-01 交接时的精确快照。后续开始工作前必须重新执行 `git status`，不要假设数字不变。

### 1.1 `farm-game`

```text
path:    /data/home/developer_026/CodeBuddy/20260519141303/farm-game
branch:  codex/pr0-pr2-join-unknown-20260801
HEAD:    6460b9971e8b10863c177e29fca08bf8b9de399e
subject: fix(player): wire SubmitSystem and NotifyLeaseLost under user overload
upstream: 未配置
working tree: 48 个 tracked 文件有改动，16 个 untracked 文件
```

当前 HEAD 与 `origin/codex/v03-access-session-hardening-20260730` 指向同一提交；当前新分支自身
没有 upstream。阶段 0～5 的主体改动在工作区 diff 中，而不是 HEAD 中。

新增且尚未纳入 Git 的关键文件包括：

- `docs/verification/draft-dependency-matrix-20260801.md`
- `docs/verification/pr0-pr2-implementation-record-20260801.md`
- `internal/farm/actor/subscription_hub.go`
- `internal/farm/actor/subscription_hub_test.go`
- `internal/farm/actor/unknown_test.go`
- `internal/farm/subscriptionreaper/`
- `internal/gateway/bootlease/`
- `internal/grpcmetrics/`
- 多个新增指标、Race 和故障样板测试

### 1.2 `classic-farm-h5`

```text
path:    /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
branch:  codex/pr0-pr2-join-unknown-20260801
HEAD:    25f5e403de3f3ed0aa1ec952229fc28002c99bbd
subject: test(e2e): add web-mode recovery spec and widen web testMatch
upstream: 未配置
working tree: 41 个 tracked 文件有改动，2 个 untracked 条目
```

当前 HEAD 与 `origin/codex/v03-access-session-hardening-20260730` 指向同一提交。除阶段 5 的
Join/Recovery 改动外，最新未提交增量还包括：

- `src/app/App.ts`
- `.env.mock`、`src/auth/mock-auth-provider.ts`、`src/mock/fake-farm-server.ts` 和旧产品 codec 删除；
- 默认 dev/build/e2e 切到 Web + canonical v0.2；
- HTTP 好友/分享、Web/Staging/WeChat Auth 和 BrowserWebSocketTransport 成为唯一运行路径；
- 旧协议服务器、测试票据和测试 Transport 移到 `src/test/helpers/`，只服务单元测试；
- `src/network/ws-client.ts`
- `src/protocol/canonical-v02-adapter.ts`
- `src/protocol/protocol-adapter.ts`
- `src/protocol/types.ts`
- Join/Recovery 对应测试；
- README、服务端接入说明、Docker BUILD_MODE 和构建审计同步更新。

### 1.3 当前进程状态

2026-08-01 20:55（Asia/Shanghai）已按当前工作区重新执行：

```bash
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build --force-recreate \
  player-svr farm-svr farm-worker farm-gate
```

四个容器均为 `healthy`，`http://127.0.0.1:8080/readyz` 返回 200；farm-gate 启动日志包含
`using redis-backed session/ticket/account/friend stores`。H5 当前以 Web 模式监听
`0.0.0.0:5174`，开发网络访问地址为 `http://198.18.0.6:5174/`，本机地址为
`http://127.0.0.1:5174/`。未登录访问
`http://127.0.0.1:5174/api/v1/friends` 返回真实后端 `401 ERR_UNAUTHENTICATED`，证明 `/api`
代理没有落到 H5 内存实现。H5 随后改为同源 `/ws` 代理并实测 `5174/ws` 返回 WebSocket 101；远程浏览器
只需转发 5174，不需要再单独转发 8080。

Compose 还检测到一个 2026-07-29 创建的 `farm-game-local-redis-1` orphan：状态为 `exited`，
镜像为 `redis:7.4-alpine`，带 `/data` volume。当前四服务不引用它，全部使用公司 Redis；由于 volume
可能包含历史数据，本次没有擅自删除。后续若要清理，必须先由用户确认容器和 volume 的保留策略。

### 1.4 最新明确复跑的验证

- `classic-farm-h5`: Vitest `61/61`；
- Web、Staging、WeChat 三种真实构建及产物边界审计通过；
- Web E2E：Redis-backed 注册、重新登录、分享加好友通过，故障停服用例按显式开关跳过；
- Staging E2E：`5/5`，包含个人经营、顶号、好友实时刷新、好友农场、重连和重复拜访拦截；
- `farm-game`: `go test ./...` 全部通过；
- 两仓 `git diff --check` 通过。

## 2. 施工依据和权威顺序

发生文档冲突时按以下顺序判断：

1. 当前两个仓库的真实源码、测试和工作区 diff；
2. `docs/verification/pr0-pr2-implementation-record-20260801.md`；
3. 本文记录的阶段 6 不变量和等待门；
4. `docs/verification/v03-access-session-handoff.md`；
5. `docs/verification/current-workspace-two-repos-handoff-20260730.md`；
6. 旧设计、旧探针和历史交接只作为背景，不能覆盖当前实现。

特别说明：

- 26～29 号合同仍是用户修改中的草稿，不归档到仓库，也不能宣称已经冻结；
- `docs/design/60-contract-alignment-plan.md` 是历史阶段记录，不是当前施工主计划；
- `docs/design/14-s2-implementation-plan.md`、`docs/design/16-local-docker-deployment-plan.md` 和本文旧版
  对 TCONND/Tcaplus 的描述只能作为历史背景；
- 不能提前冻结公网 v3 Proto field number、Redis Key/Hash Tag/Lua 原子边界、Workflow Bucket
  物理格式或最终 `StoredOperationOutcome`；
- 当前阶段 5 新增的 GateBoot Lease 是短 TTL 运行态数据，属于已明确允许的例外。

草稿依赖和等待门详见：

- `docs/verification/draft-dependency-matrix-20260801.md`
- `docs/verification/pr0-pr2-implementation-record-20260801.md`

## 3. 当前真实架构边界

### 3.1 接入与会话

```text
AccountCredential ──> Redis（当前授权实现；未来可迁移 Tcaplus）

H5 ── HTTPS/JSON ──> farm-gate AuthHTTP
                         ├─ WebSession：Redis
                         └─ ConnectionTicket：Redis

H5 ── WS/Protobuf ──> Direct WebSocket Transport
                         │
                         ▼
                     farm-gate
                       ├─ ClientHello / Candidate Ticket 原子激活
                       ├─ SessionAuthority / SessionRoute / Route Lease
                       ├─ ConnectionRegistry / ConnectionWriter
                       ├─ PendingJoin / ActiveSubscription
                       ├─ Protocol / RateLimit / PRE_AUTH Budget
                       └─ RouteAdapter
                              │
                       ┌──────┴──────┐
                       ▼             ▼
                   player-svr     farm-svr
```

TCONND + gateproxy 是生产 Transport Adapter 候选/兼容接入线，不是当前 Direct WebSocket 业务
开发的前置阻塞。不要为了阶段 6 回滚 Direct WebSocket，也不要删除已有 gateproxy 适配代码。

### 3.2 服务权威

- `farm-gate`：身份入口、Ticket、Session Fence、连接、协议、序号、限流、首跳选择和下行；
  不修改玩家资产，不直接修改地块，不推进跨 Actor Participant。
- `player-svr / PlayerActor`：钱包、背包、Seed Reservation、Grant、商店、任务、图鉴及其 Receipt。
- `farm-svr / FarmActor`：地块、CropCycle、农场版本、浇水、成熟、农场侧收获和偷菜真值。
- `farm-svr / FarmOperationCoordinator`：Plant/Care/Theft 等以目标农场为中心的 Workflow 协调；
  Coordinator 不绕过 PlayerActor/FarmActor 的权威裁决。
- `farm-worker`：接管持久化的未完成 Workflow，通过 Participant Receipt 幂等恢复；不直接编辑
  Player/Farm blob。
- `SubscriptionHub`：运行态 Presence、Join 屏障、per-farm 有序事件 Lane 和 GateBoot 索引；
  Presence 不是好友权限或业务操作权限。

Plant 当前正确首跳是：

```text
H5
  -> farm-gate
  -> farm-svr / FarmOperationCoordinator
  -> CreateIfAbsent PlantWorkflow
  -> PlayerActor.ReserveSeed
  -> FarmActor.CommitPlant
  -> PlayerActor.CommitReservation
  -> CompleteWorkflow
```

“种子由 PlayerActor 裁决”不等于 “PlantCoordinator 必须在 player-svr”。Gate 只选择首跳，
不能直接编排多个 Participant。

### 3.3 当前持久化决定

用户已经明确允许在现阶段使用公司改造的持久 Redis 作为业务数据库，暂不让 Tcaplus 阻塞玩法和
一致性施工。当前 Player、Farm、Friend、Share、Workflow、Account、Session、Ticket 均可使用 Redis。

Tcaplus 已经完成 `OwnerCoreRecord` 和 CAS 探针，但目前是迁移候选，不是当前三个 Loop 的运行前置。
任何 Agent 都不得把 Redis 密码、完整连接 URL、Cookie key、Permit secret 或平台镜像凭据写进文档、
日志、Compose 或 Git。

## 4. 已完成的施工阶段

完整实现动机、故障和指标见 `docs/verification/pr0-pr2-implementation-record-20260801.md`。
以下“完成”表示已经存在于当前工作区并通过对应验证，不表示已经提交/推送，也不表示生产观察窗完成。

### 4.1 PR-0：验证基线

- 固定双仓起始点和脏工作区事实；
- `scripts/verify-s2.sh` 覆盖 Go vet/test/race/build、H5 单测、Staging/Web 构建；
- Web E2E 和会停服务的故障 E2E 必须显式开启；
- pprof、block profile、SessionEpoch cache 等压测遗留改为 opt-in；
- 建立草稿依赖矩阵，不归档 26～29 号草稿。

### 4.2 PR-1A～1C：Push-only Join 单一闭环

- Join `ACCEPTED` 只解析 CommandResult，不携带/冒充入场 Snapshot；
- H5 和 Gate 都有有界 `PendingJoin`，Snapshot 与 ACCEPTED 可乱序，但必须匹配相同
  operation/farm/subscription/fence；
- 两个锁存器满足后才提升为 `ActiveSubscription`；
- PendingJoin 具有共享 deadline 和 `Pending -> Canceling -> Closed` 终态；
- 关闭后的迟到回调只计数丢弃，不能复活订阅；
- `CancelJoin` 使用目标 Farm 的 SystemQueue O(1) 精确执行；
- server connection sequence 空洞进入恢复；
- `STRICT_PENDING_JOIN_FENCE=0` 仍是兼容/观测默认值。

### 4.3 PR-2：TIMEOUT_UNKNOWN 同 Turn 冻结

- Farm CAS 不确定时保存 domain-specific `UnknownContext`；
- 同一 Actor Turn 内进入 `RESOLVING_UNKNOWN`，已排队 User Task 不能越过未知点；
- resolver 只在 SystemQueue 内权威重读；
- receipt 证明 committed，成功权威读且无 receipt 才判定 not committed；
- Player 强写不确定统一返回 `PROCESSING`，下一 Turn 重新加载权威记录；
- `GET_OPERATION_RESULT` 不再用查询 Envelope hash 错误比较原业务 payload hash；
- Buy、Plant、Water 已有 committed/not-committed 和幂等恢复故障样板。

### 4.4 阶段 3：ActiveSubscription 精确 Disconnect

- Disconnect 捕获本地 ActiveSubscription/PendingJoin 后再删连接；
- `CleanupFarmSubscription` 精确路由到 `FARM:farm_id` 的 SystemQueue；
- 清理 at-least-once、可重放、完整 Fence 匹配，旧连接不能删除新 Presence；
- ABSENT Actor 的清理不重新激活 Actor；
- 旧全 Actor 扫描接口暂留兼容，但生产 `gatewayapp` 不挂载；
- `subscription_cleanup_actor_scan_total` 可证明是否误入旧扫描。

滚动发布必须先 `farm-svr` 再 `farm-gate`，新 Gate 不会对旧 farm-svr 的 Unimplemented
静默回退成全 Actor 扫描。

### 4.5 阶段 4：per-Gate Downlink 隔离

- `gateway_id -> 独立 bounded lane -> 独立 batch -> 独立 RPC deadline`；
- 不使用无限 `WaitForReady`；
- 一个坏 Gate 不阻塞其他 Gate；
- 同 Gate FIFO 和 QueueAccepted/失败 callback 顺序不变；
- 全局并发、每 Lane 队列和 Lane 总数都有上限；
- 空闲 Lane 自动回收；
- route 缺失、Lane 满/超限 fail closed，并由 Farm Resync 恢复。

### 4.6 阶段 5：SubscriptionHub、FarmEventLane、GateBoot Lease

阶段 5 已完成三个生命周期拆分：

1. Presence 从 FarmActor 指针移到独立 `SubscriptionHub`；普通 Actor passivation 不清 Presence，
   Actor 再激活可挂回同一 Hub；
2. `JoinPrepared(Snapshot V)` 与 `CommittedFarmChange(V+1...)` 进入同一条 per-farm FIFO Lane；
3. 每个 Gate Boot 只续约一份 15 秒 Lease，farm-svr Reaper 只检查本 Holder 的 Hub 中真实出现的 Boot。

Lane 满或版本空洞的原则是：业务 CAS 已成功就不能回滚或改报失败；应清理不完整 Join 并发送
`RESYNC_REQUIRED`。System cleanup admission 与业务 Lane 分离，业务拥塞不能饿死 Disconnect/Cancel/Reap。

GateBoot 续租失败不会延长本地 `trusted_until`。超过可信截止时间后：

- `/readyz` 返回 503；
- Direct WebSocket 在 Upgrade 前拒绝；
- gateproxy 兼容入口在 ClientHello 拒绝；
- owner-write 停止准入；
- read-only 恢复查询仍允许；
- Redis 恢复并成功续约后可以不重启进程恢复 Ready。

Presence 的长 TTL 只作低频兜底，不按每个 subscription 高频续租。

## 5. 最近一次验收证据

### 5.1 静态、单元和 Race

最近一次明确记录的结果：

```text
go vet ./...                         PASS
go test ./...                        PASS
go test -race ./internal/...         PASS
go build ./...                       PASS
H5 Vitest                            13 files / 72 tests PASS
npm run build:staging                PASS
npm run build:web                    PASS
```

覆盖包括 Join/CancelJoin、Unknown、精确 Disconnect、旧 Fence、Absent Actor、SystemQueue 满载、
per-Gate 隔离、Lane 上限/退休、SubscriptionHub 跨 Actor passivation、GateBoot 恢复和 Reaper。

### 5.2 真实浏览器与公司 Redis

使用公司 Redis、`GATE_TRANSPORT=websocket` 重建四服务后：

```text
Web account E2E                      1/1 PASS
Staging Direct WebSocket E2E         5/5 PASS
```

Staging 覆盖个人经济/重连、好友实时更新、顶号、访问好友后返回自己农场、双端浇水和偷菜同步。
Web 故障 E2E 曾验证 Worker/Player 停启、未决收获恢复、全后端重启和 Redis 恢复，但它会主动停启服务，
不能作为普通验证默认执行。

阶段 5 观察样本：

```text
farm_gate_boot_lease_trusted                         1
renew attempts / successes / failures              26 / 26 / 0
farm_event_lane_full_total                          0
farm_event_lane_resync_total                        0
farm_gate_boot_reaper dead/errors/removals          0 / 0 / 0
CleanupFarmSubscription OK                          17
subscription_cleanup_actor_scan_total               0
浏览器退出后 farm_subscription_hub_boot_fences      0
farm_subscription_hub_lanes_retired_total           0
```

最后一项不是阶段 5 失败：真实运行没有自动触发 Actor idle passivation。Actor 变为 ABSENT 后 Hub
独立退休已有单测，自动 idle passivation 正是阶段 6 的工作。

### 5.3 不能从这些证据推出的结论

- 一次本地 `legacy_implicit_bind_total=0` 不能替代生产观察窗；
- Reaper 样本中没有 dead Boot，不能宣称真实 Gate 崩溃清理已完成生产验证；
- Redis 能恢复数据不能替代平台切主、备份恢复、RPO/RTO 验证；
- Direct WebSocket E2E 不能证明 TCONND `BY_WS` 已打通；
- 本地静态 Shard 验证不能证明动态迁移和 3000 万 DAU 容量完成。

## 6. 下一主线：阶段 6 Activation 完整生命周期

### 6.1 为什么必须做

阶段 5 解决的是 Presence 不再钉住 FarmActor，但当前仍没有自动 idle passivation：

- `internal/logic/actor/activation/activation.go` 已有 `PASSIVATING/ABSENT` 状态，但没有 idle 时间、
  blocker 快照或分桶扫描；进入 ABSENT 的 Controller entry 仍可能留在 map；
- `internal/farm/actor/registry.go` 的 Begin/FinishPassivate 主要是手工/测试入口，没有自动触发；
- `internal/logic/actor/runtime/executor.go` 为每个 Actor Key 建立 Mailbox 和 goroutine，当前只有整个
  Executor `Stop()` 才统一关闭，缺少按 Key 安全退休；
- PlayerRegistry 不缓存 Actor，但每个历史 Player Key 使用过的 Executor Mailbox/goroutine 仍会保留。

如果不补阶段 6，资源会按“历史访问过的唯一 Actor 数量”增长，而不是按“当前活跃 Actor 数量”回落。
在大量游客、重连和 3000 万 DAU 目标下，这会直接变成内存和 goroutine 泄漏模型。

### 6.2 阶段 6 的硬不变量

Actor passivation 只允许被以下条件阻塞：

- 当前 Turn 正在执行；
- Mailbox/SystemQueue 有必须完成的任务；
- persistence in-flight；
- 状态为 `RESOLVING_UNKNOWN`；
- dirty state 尚未安全持久化或交接；
- 本地 Participant 结果尚未在权威记录/Receipt 中锚定。

以下条件不得永久阻塞 Actor passivation：

- 存在 Presence；
- 存在持久化的 `RETRY_WAIT` Workflow。

原因是 Presence 已属于 Hub；持久化 Workflow 已由 Store + Worker 接管。把它们继续当 Actor blocker
会重新引入阶段 5 刚拆掉的生命周期耦合。

完成一次 passivation 必须释放：

- Registry 中的 Actor；
- Executor 中对应 Key 的 Mailbox；
- Mailbox goroutine；
- Activation Controller entry；
- 该 Actor 专属但不再需要的临时状态。

不得使用“一 Actor 一个 Timer”。必须使用分桶 Idle Sweeper/时间轮或等价的有界批量扫描方案。

### 6.3 推荐施工工作包

#### 6.3.1 Executor 按 Key 退休基础

先给 Executor 建立明确的 per-key 生命周期，再接 Idle Sweeper：

- Mailbox 记录 generation、queued、in-flight、system task 和 last activity；
- 增加只退休指定 Key 的安全 API；
- 退休必须验证 map 中仍是同一 Mailbox/generation；
- 有正在执行/排队任务时不能关闭；
- 新任务与退休并发时，要么取消退休并复用，要么创建新 generation，不能投递到已经关闭的 channel；
- goroutine 退出、map 删除和 Controller 删除都必须可观察、可测试；
- `Stop()` 仍保留全局关闭语义，但不能被拿来模拟普通 Actor passivation。

不要一开始先写 Sweeper，否则会把已有 Mailbox 永久化问题变成高频竞态。

#### 6.3.2 PassivationBlockers 快照

建立一个稳定的 blocker 判定边界，至少能区分：

```text
turn_running
user_queue_nonempty
system_queue_nonempty
persistence_in_flight
resolving_unknown
dirty_unhanded
participant_result_unanchored
```

当前 Redis CAS 是 Turn 内同步调用，通常已经被 `turn_running` 覆盖；不要为了“实现指标”擅自把
持久化异步化。可以先提供显式计数/接口，为以后异步 persistence 留边界。

不得把 Presence 数量或 durable Workflow `RETRY_WAIT` 数量加入 blocker。

#### 6.3.3 分桶 Idle Sweeper

- Actor/Key 活跃时只更新 deadline/桶归属，不创建独立 Timer；
- 每个 tick 只处理当前到期桶，扫描成本与该桶候选数相关；
- 候选仍需二次校验 idle deadline、generation 和 blocker；
- 被阻塞的候选应按原因统计，并放入后续桶，不能 busy loop；
- 扫描周期、idle timeout、单 tick 上限应可配置；
- 时钟测试使用 fake clock 或可控 tick，不能依赖长时间 sleep。

#### 6.3.4 FarmActor 两阶段 passivation

建议保持两阶段语义：

```text
ACTIVE
  -> BeginPassivate / PASSIVATING
  -> 二次检查 blocker + generation
  -> 删除 Registry Actor
  -> 退休 Executor Mailbox/goroutine
  -> 删除 Activation entry
  -> ABSENT
```

新 User Command 到达且旧 Actor 尚可安全继续时，应取消 passivation；已经完成删除则重新激活并从权威
Store 加载。Holder 失权、panic、Unknown 和普通 idle passivation 是不同语义，不能共用粗暴 `MarkAbsent`。

普通 FarmActor passivation 不清理 Hub Presence；当 Actor 已 ABSENT 且 Hub 最后一个 Presence 后续被
精确清除时，Hub 才独立退休空 Lane。Holder 失权/panic 才触发 Drain/Resync。

#### 6.3.5 Player Executor Mailbox 回收

PlayerRegistry 当前每 Turn 重新加载权威记录，通常不需要复制 Farm 的常驻 Activation FSM；但它仍通过
Executor 对 Player Key 串行化。因此阶段 6 至少要让长期空闲 Player Key 的 Mailbox/goroutine 被安全回收，
并验证下一次命令能新建 generation、重新加载权威记录且不丢任务。

不要为了代码对称强行给 Player 增加一套没有业务需要的 Farm Activation Controller。

#### 6.3.6 指标和验收

建议至少增加：

- active Actor、Mailbox、Mailbox goroutine、Activation entry gauge；
- passivation attempted/completed/canceled/blocked total；
- blocked reason；
- reactivation total；
- sweeper scanned/candidates/lag/bucket gauge 或 histogram；
- stale generation/retire race 计数。

阶段 6 必须覆盖以下测试：

1. idle FarmActor 自动变为 ABSENT，并释放 Actor/Mailbox/goroutine/Controller entry；
2. Presence 存在时 FarmActor 仍可 passivate，SubscriptionHub 状态保留；
3. 持久化 `RETRY_WAIT` Workflow 不钉住 Actor；
4. running Turn、SystemQueue、Unknown、dirty/unanchored result 分别会阻止退休；
5. 新命令与退休并发，不丢、不重、不 panic、不投递到 closed channel；
6. 重新激活从权威 Store 恢复，旧 generation 不能影响新 Actor；
7. Player Mailbox idle 回收后再次执行正确；
8. 大量不同 Farm/Player Key 访问后，等待 idle timeout，Actor/Mailbox/goroutine 数明显回落；
9. `go test -race` 覆盖 Executor、Activation、FarmRegistry、PlayerRegistry；
10. 公司 Redis 下 Web account E2E 和 Staging Direct WebSocket 5/5 都要复跑，不能只测 Mock；
11. Actor 自动 ABSENT 后关闭浏览器，验证空 Hub Lane 的真实退休指标增长；
12. 整个过程 `subscription_cleanup_actor_scan_total` 必须保持 0。

### 6.4 阶段 6 明确不做

- 不修改公网 v3 Proto field number；
- 不迁移 Workflow Bucket 物理格式；
- 不实现 Journal v2；
- 不删除 PendingJoin 兼容分支；
- 不把 Presence 权限化或持久化成 Actor blocker；
- 不把 TCONND 修复混进 Activation 提交；
- 不为了生命周期对称重写整个 PlayerActor；
- 不顺便实现动态 Shard、任务、宠物、邮件等产品功能。

## 7. 阶段 5 仍需补的运行级证据

这些是运行/发布证据尾项，不代表阶段 5 主体代码未完成：

1. 受控制造一个真实 Gate Boot Lease 过期，验证 farm-svr Reaper 精确清理对应 Presence；
2. 受控 Redis 失联超过 trusted deadline，验证 Gate Ready、Upgrade、owner-write、read-only 查询和
   恢复路径；
3. 阶段 6 自动 passivation 后，观察真实 Hub empty-lane retirement；
4. 多 Gate/慢 Gate 下验证坏 Lane 不阻塞健康 Lane；
5. 故障期间 Redis authority 不确定时必须保留 Presence 重试，不能误删。

不得在没有用户明确同意时破坏公司共享 Redis、停止其他人的服务，或执行会删除业务数据的测试。
故障测试必须使用独立 key prefix、独立账号和精确清理。

## 8. PR-1D 观察门仍未通过

当前默认必须保持：

```text
STRICT_PENDING_JOIN_FENCE=0
```

`farm_gate_legacy_implicit_bind_total == 0` 的作用是证明所有线上 H5/旧连接/乱序路径都不再依赖
“第一条 Farm Push 自动绑定订阅”的历史兼容行为。只有经过真实版本和足够时间的观察窗，且至少同时满足：

- `legacy_implicit_bind_total` 持续为 0；
- PendingJoin timeout/mismatch/stale callback 无异常增长；
- PendingJoin current/age 能正常回落；
- 新旧客户端和滚动发布链路均验证；

才能进入 PR-1D：开启严格模式、再删除隐式绑定和兼容分支。一次本地 E2E 为 0 不能越过此门。

## 9. 阶段 6 之后的施工顺序

### 9.1 基础设施主线

```text
阶段 7  Operation Journal v2
  -> Route Cache 删除 / 普通 Result 进一步瘦身
  -> 等待物理草稿稳定
  -> 完整 Unknown Resolver
  -> Workflow 有状态迁移与分桶
  -> v2 Plant/Workflow 故障样板
  -> v3 Schema 冻结与双协议灰度
  -> Journal RetryDirective
  -> v3 端到端样板
  -> 200 人围观 / 慢 Gate / Hub 重建
  -> 动态 Shard
```

Journal v2 的重放材料名称已约定为 `replayPayloadBytes`，不能把 request ID、Frame sequence、Ticket、
Cookie、SessionPermit、ActionPermit、ActorRouteHeader 或 writer epoch 当业务重放材料。最终结构仍在等待门内。

Workflow 物理迁移必须是可恢复的有状态迁移，不能简单做“新 Bucket CreateIfAbsent 后给 legacy 写
tombstone”，因为跨 Redis Slot 两步不原子，会双位置 Claim。该工作不属于阶段 6。

### 9.2 产品玩法并行线

当前已有账号密码、多用户、三种作物、买卖/种植/浇水/收获、分享好友、访问好友、好友浇水与偷菜、
基础任务和图鉴。仍未完成的产品范围包括但不限于：

- 更完整任务链、周期任务、赛季和领奖体验；
- 图鉴奖励和更完整的作物/商品/数值平衡；
- 邮件、宠物等外围系统；
- 更完整好友机会索引、社交提示和多人围观体验；
- 产品级 UI、美术和引导；
- 3000 万 DAU 容量估算、瓶颈分析和正式压测报告。

产品线可以并行规划，但修改同一 Actor/Workflow 核心文件时必须与阶段 6 串行，避免把生命周期竞态和
玩法变更混在一个不可审查的 diff 中。

## 10. 正确启动方式

账号密码 Web 模式的完整运行链路是：

```text
浏览器 :5174
  ├─ /api ──> Vite HTTP Proxy ──> farm-gate :8080 AuthHTTP
  └─ /ws  ──> Vite WS Proxy   ──> farm-gate :8080 Direct WebSocket
                                         ├─ player-svr
                                         └─ farm-svr ──> farm-worker
                                                │
                                                ▼
                                           公司持久 Redis
```

浏览器只访问 `5174`。`8080` 是开发机内部的 Vite 代理目标，Redis 和业务 gRPC 端口也不应暴露给
浏览器。下面各步骤必须按顺序执行；仅启动 H5、仅启动 Compose，或只执行 `docker compose --build`
都不是完整 Web 环境。

### 10.1 先检查，不覆盖工作区

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
git branch --show-current
git status --short
git log -3 --oneline

cd ../classic-farm-h5
git branch --show-current
git status --short
git log -3 --oneline
```

两个仓库都应看到 `codex/pr0-pr2-join-unknown-20260801`，但没有 upstream。若现场不同，先核对用户或
前一 Agent 是否有新提交，不要把本文快照强行还原到工作区。

### 10.2 核对 Web 配置

`classic-farm-h5/.env.web.local` 是 Git ignored 的本机配置，应为：

```dotenv
VITE_FARM_ENV=web
VITE_FARM_PROTOCOL=canonical-v02
VITE_API_BASE=/api
VITE_API_PROXY_TARGET=http://127.0.0.1:8080
VITE_WS_PROXY_TARGET=ws://127.0.0.1:8080
VITE_WS_URL=/ws
```

其中 `VITE_WS_URL=/ws` 是单端口访问的关键：浏览器连接当前页面同源的 `/ws`，再由开发机上的
Vite 转发到 farm-gate。不要写成 `ws://127.0.0.1:8080/ws`；对远程 Windows 浏览器而言，那个
`127.0.0.1` 指向 Windows 自己，而不是开发机。

### 10.3 编译并启动后端四服务

本地 Compose 使用宿主机预编译静态 Go ELF；单独 `docker compose up --build` 不会重新编译源码，
可能出现“源码已有接口但运行时 404”或“新逻辑未生效”。正确顺序：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
./scripts/prepare-local-compose.sh
GATE_WS_ALLOWED_ORIGINS="127.0.0.1:5174,localhost:5174,127.0.0.1:5175,localhost:5175,127.0.0.1:5184,localhost:5184,198.18.0.6:5174" \
./scripts/company-redis-compose.sh up -d --build --force-recreate \
  player-svr farm-svr farm-worker farm-gate
./scripts/company-redis-compose.sh ps
curl -i http://127.0.0.1:8080/readyz
```

公司 Redis 配置来自 Git ignored 文件：

```text
deploy/secrets/redis.conf.local
```

不要打印、复制或提交它。`prepare-local-compose.sh` 使用 `CGO_ENABLED=0`；这是必须的，因为运行镜像是
Alpine，宿主机动态链接 ELF 会表现成文件存在但启动报 `no such file or directory`。

`GATE_WS_ALLOWED_ORIGINS` 必须包含浏览器地址的 `host:port`，不是 farm-gate 地址。当前直连地址为
`198.18.0.6:5174`；开发机 IP 或 H5 端口变化时必须同步更新。该默认值也已写入
`deploy/docker-compose.local.yml`，命令中显式声明是为了让交接现场可见、避免误用旧容器环境。

预期 `ps` 中 `player-svr`、`farm-svr`、`farm-worker`、`farm-gate` 均为 `healthy`，`readyz` 返回：

```json
{"status":"ready"}
```

主要端口：

| 组件 | gRPC/HTTP | 健康与指标 |
| --- | --- | --- |
| farm-gate | `127.0.0.1:8080` | `/readyz`、`/metrics` |
| farm-svr | `127.0.0.1:19000` | `127.0.0.1:19001/readyz`、`/metrics` |
| player-svr | `127.0.0.1:19100` | `127.0.0.1:19101/readyz`、`/metrics` |
| farm-worker | — | `127.0.0.1:19201/readyz`、`/metrics` |
| Gate Downlink | `127.0.0.1:9092` | — |
| Gate Control | `127.0.0.1:9093` | — |

### 10.4 启动 H5 Web 账号密码模式

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
npm run dev:web -- --host 0.0.0.0 --port 5174 --strictPort
```

`.env.web.local` 已正确配置时不必在命令行重复三个 `VITE_*` 参数；CI、临时端口或排障时可以用命令行
环境变量覆盖。启动日志必须包含类似：

```text
[vite.config] mode=web apiProxyTarget=http://127.0.0.1:8080 wsProxyTarget=ws://127.0.0.1:8080
```

开发网络直接访问 `http://198.18.0.6:5174/`；服务器本机访问 `http://127.0.0.1:5174/`。
页面应显示账号、密码、“登录”和“注册并进入”。Web 模式是任意多个持久账号的真实浏览器入口，
不再限制为两个固定测试用户。直连 IP 时 Gate Origin 白名单必须包含 `198.18.0.6:5174`；当前
`docker-compose.local.yml` 的本机开发默认值已经包含。

### 10.5 验证 Web 链路不是 Mock

先验证 H5 页面和经 Vite 代理的真实 HTTP：

```bash
curl -I http://127.0.0.1:5174/
curl -i http://127.0.0.1:5174/api/v1/friends
```

预期页面为 `200`；未登录好友请求为真实 farm-gate 返回的 `401 ERR_UNAUTHENTICATED`，而不是 H5
内存数据。再验证 WebSocket Upgrade：

```bash
curl --http1.1 --max-time 2 -i \
  -H 'Origin: http://198.18.0.6:5174' \
  -H 'Connection: Upgrade' \
  -H 'Upgrade: websocket' \
  -H 'Sec-WebSocket-Version: 13' \
  -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
  http://127.0.0.1:5174/ws
```

响应首行应为 `HTTP/1.1 101 Switching Protocols`。由于 curl 不会继续发送游戏协议帧，随后超时退出
是正常的。`101` 只证明“浏览器同源 `/ws` -> Vite -> farm-gate”握手与 Origin 白名单已打通；完整
业务还必须在页面登录后继续完成 `ClientHello + ConnectionTicket`、Join、Snapshot 和经营命令。

浏览器中还可以打开 DevTools：账号登录应在 Network/Fetch 中看到 `/api`；种植、浇水、收获等经营
命令复用已经建立的 WebSocket，所以不一定出现新的 Fetch/XHR，应该在 Network/WS/Frames 中观察。

### 10.6 直连与 SSH 转发

若本机能访问 `http://198.18.0.6:5174/`，不需要 SSH 转发。只有开发网络不能直连时，才从浏览器
所在的 Windows 本机执行：

```powershell
ssh -p 36000 -N -o ExitOnForwardFailure=yes -L 5174:127.0.0.1:5174 developer_026@198.18.0.6
```

保持终端运行后打开 `http://127.0.0.1:5174/`。若 Windows 报
`bind [127.0.0.1]:5174: Permission denied`，说明本机 5174 被占用或保留，可改用已在 Gate 白名单中的
5175：

```powershell
ssh -p 36000 -N -o ExitOnForwardFailure=yes -L 5175:127.0.0.1:5174 developer_026@198.18.0.6
```

随后打开 `http://127.0.0.1:5175/`。Vite 已代理 `/api` 和 `/ws` 到 8080，所以始终只需转发一个
H5 端口，不要另行暴露 Redis、8080 或后端 gRPC 端口。

### 10.7 Staging 双账号模式

```bash
VITE_API_PROXY_TARGET=http://127.0.0.1:8080 \
VITE_WS_URL=ws://127.0.0.1:8080/ws \
npm run dev:staging -- --host 0.0.0.0 --port 5184 --strictPort
```

Staging 的 `player-a/10001`、`player-b/10002` 是固定双身份自动化入口，不是账号密码功能回退。

### 10.8 不存在产品 Mock 回退

```bash
npm run dev
```

现在等价于 `npm run dev:web`。缺少 farm-gate、Redis 或业务服务时，注册/登录、HTTP 或 WSS 会明确失败，
客户端不会创建内存账号、好友、农场或结算结果。若页面在后端停机时仍能经营，应直接按缺陷处理。

SSH LocalForward 必须由浏览器所在的本机发起；服务器端不能替 Windows 创建本地监听端口。也可以使用
远程开发工具的 Ports 面板转发 5174。

## 11. 验证命令和使用边界

### 11.1 默认验证

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
./scripts/verify-s2.sh
```

等价核心项：

```bash
go vet ./...
go test ./...
go test -race ./internal/...
go build ./...

cd ../classic-farm-h5
npm test -- --run
npm run build:staging
npm run build:web
```

### 11.2 Web E2E

四个后端服务已经启动且 healthy 时：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
npm run e2e:web
npm run e2e:staging
```

或让统一脚本启用普通 Web E2E：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
FARM_VERIFY_WEB_E2E=1 ./scripts/verify-s2.sh
```

### 11.3 故障 E2E

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
FARM_VERIFY_WEB_FAULT_E2E=1 ./scripts/verify-s2.sh
```

该模式会主动停启服务，只能在确认不会影响其他 Agent/用户后执行。它不是普通提交前检查。

### 11.4 阶段 5/6 观察指标

服务启动后可从对应 `/metrics` 检查：

```text
farm_gate_pending_join_*
farm_gate_legacy_implicit_bind_total
subscription_cleanup_actor_scan_total
farm_downlink_lanes_*
farm_downlink_queue_full_total
farm_downlink_rpc_failure_total
farm_subscription_hub_*
farm_event_lane_*
farm_gate_boot_lease_*
farm_gate_boot_reaper_*
```

阶段 6 新指标应能回答四个问题：当前有多少 Actor/Mailbox/goroutine/Activation entry，为什么某 Actor
不能退休，退休是否成功，以及空闲后总量是否真正回落。

## 12. 历史 TCONND/gateproxy 结论

保留这一节是为了避免下一位 Agent 重复走弯路，不再把它放在当前主线首页。

已确认：

- TCONND 和 gateproxy 官方/内部资产曾成功拉取、运行；
- gateproxy 与 farm-gate 的 `GateStream` 和 heartbeat 可工作；
- gateproxy 对 `WEBSTART/WEBINPROC/WEBSTOP` 的 fallthrough 修复有意义；
- TCONND `0.0.13` 的 `BY_WS` 能完成 101 Upgrade；
- 但该 TCONND 本体没有把 Web 事件送入 TBUS，因此 gateproxy 收不到事件；问题不只在 gateproxy。

结论：

- 当前 Direct WebSocket 是可运行、可验证的开发/演示路径；
- TCONND + gateproxy 仍是生产 Transport Adapter 候选，需要平台支持的已验证版本/配置或后续独立探针；
- 不要从 `/tmp/gongfeng-gateproxy.*` 临时目录复制整套仓库进 `farm-game`；
- 不要再把“重编译 3 行 gateproxy”写成唯一剩余工作；
- 生产接入问题应独立于阶段 6 Actor 生命周期施工。

历史证据：

- `docs/verification/tconnd-probe-report.md`
- `docs/design/15-tconnd-gateproxy-integration-plan.md`
- `docs/design/16-local-docker-deployment-plan.md`

## 13. 接手 Agent 的建议执行顺序

1. 读取本文和 `pr0-pr2-implementation-record-20260801.md`；
2. 检查两个仓库分支、HEAD、status 和完整 diff，确认没有新 Agent 继续写入；
3. 不清理当前 `farm-game 48+16` 和 `classic-farm-h5 41+2` 个工作区改动；
4. 先跑阶段 6 涉及包的现有定向测试，建立基线；
5. 先实现 Executor per-key retirement，不先写 Sweeper；
6. 再实现 blocker 快照、分桶 Sweeper、Farm 两阶段 passivation；
7. 补 Player Mailbox 回收；
8. 用 fake clock、竞态和大 Key 数测试证明安全与回落；
9. 跑 `go test -race` 和完整 `verify-s2.sh`；
10. 重建四容器，分别复跑 Web 账号模式和 Staging Direct WebSocket；
11. 记录阶段 6 指标前后对比和真实 Hub Lane retirement；
12. 更新实施记录后再向用户报告；提交、推送、设置 upstream 需要遵守用户当时的明确指示。

建议把阶段 6 拆成可审查的小提交，不要把当前阶段 0～5 的大脏 diff、TCONND、Journal v2、产品玩法
和 Actor 生命周期压成一个提交。如果需要先保存现状，应先与用户确认提交边界和远端分支名。

## 14. 常见误区

- 页面接口 404：先确认是否运行了旧 `.local-build`，重新执行 `prepare-local-compose.sh`；
- 页面不是账号密码：确认启动的是 `dev:web`，不是 `dev:staging`/`dev:wechat`，并重启 Vite 让 mode 生效；
- 点击种植在 Fetch/XHR 中没有新请求是正常的：经营命令走已有 WebSocket，应查看 DevTools 的 WS Frames；
- 只能买胡萝卜/offer unavailable：先确认运行中的 `player-svr` 是否为当前二进制；
- 好友列表只有一边：检查 FriendRosterChanged Push、连接是否仍为 ActiveSubscription 和恢复屏障；
- 好友农场不能返回自己农场：检查目标农场上下文是否被陈旧恢复状态覆盖；
- 成熟作物仍可浇水：服务端必须裁决，不能只禁用 H5 按钮；
- `lanes_retired_total=0`：先确认 Actor 是否真的自动 ABSENT，不要直接判断 Hub 泄漏；
- `docker compose --build`：不会替代宿主机 Go 静态编译；
- `legacy_implicit_bind_total=0`：本地一次为 0 不是生产观察门已通过；
- Presence：表示谁正在看农场，不是好友关系、操作权限或 Actor 常驻理由。

## 15. 安全和工作区纪律

- 不提交 `deploy/secrets/redis.conf.local`、`.env.local`、Cookie/Permit secret、Ticket 或平台凭据；
- 不在输出里打印完整 Redis URL；
- 不对公司共享 Redis 使用宽泛 `FLUSH*`、全库扫描删除或未隔离的压测 key；
- 不覆盖其他 Agent 的未提交改动；
- 不使用破坏性 Git 命令恢复工作区；
- 不把历史文档、旧 master 或远端旧分支当作当前源码权威；
- 不把单元测试通过、一次本地 E2E、生产观察窗和 3000 万 DAU 验收混为同一个“完成”。

## 16. 关键文档索引

- 当前阶段实施证据：`docs/verification/pr0-pr2-implementation-record-20260801.md`
- 草稿等待门：`docs/verification/draft-dependency-matrix-20260801.md`
- 接入与会话专项：`docs/verification/v03-access-session-handoff.md`
- 双仓产品/架构全景：`docs/verification/current-workspace-two-repos-handoff-20260730.md`
- 当前工作区交接：`docs/verification/s2-handoff.md`
- TCONND 历史探针：`docs/verification/tconnd-probe-report.md`
- Tcaplus 历史探针：`docs/verification/tcaplus-probe-report.md`
- H5 模式与启动说明：`../classic-farm-h5/README.md`

接手时优先相信当前代码和本次 2026-08-01 实施记录；旧文档中凡是“唯一阻塞”“全部完成”“正在运行”
等绝对表述，都必须用当前源码、进程和指标重新核验。
