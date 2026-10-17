# v0.3 接入与会话专项交接

日期：2026-07-30  
适用仓库：`farm-game`、`classic-farm-h5`  
目标文档：`#26 · 接入与会话详细合同 v0.3-access-session-freeze`  
当前加固分支：`codex/v03-access-session-hardening-20260730`

来源分支：`codex/v03-access-session-contract-20260730`

## 1. 一句话结论

本轮不是重写农场业务，而是把“账号认证、Cookie、一次性 Ticket、WSS 激活、SessionRoute、
跨 Gate 顶号、命令准入、FarmPresence 与 H5 恢复”从能演示的链路收紧成一条有明确线性化点、
完整 Fence 和有界资源的 P0 主链。

当前状态应表述为：

```text
v0.3 核心会话正确性主链：已实现
本地 Direct WebSocket + 公司 Redis：已验证
Web 账号密码代码：已保留并可用
全部安全/观测/生产探针：尚未完成
微信真实票据、动态多 Gate/跨节点故障、CLB 与 TCONND：尚未完成端到端验收
```

不能表述为“v0.3 全部冻结完成”。

## 2. 代码位置与提交

### farm-game

```text
branch: codex/v03-access-session-contract-20260730
base:   c2d9d32 feat: complete v1.21 farm loops and production probes
main:   2830520 feat: enforce v0.3 access session contracts
fix:    a32a0f2 fix: renew Redis route leases without script cache
handoff: 3ec4a42 docs: record HC-ACCESS-19 dual-gate E2E and web baseline evidence

hardening branch: codex/v03-access-session-hardening-20260730
hardening base:   3ec4a42
hardening 1:      7dde3c4 test: harden dual-gate session takeover verification
hardening 2:      ebece72 feat: add rotating HTTP session cookie keyring
hardening 3:      919a5d6 feat: add all-device session revocation endpoint
hardening 4:      d8ce33c feat: enforce distributed web login risk limits
hardening 5:      2b0fc71 feat: bound pre-auth transport resources before ClientHello
hardening 6:      4cae35f test: inject join queue-full and version catch-up faults
hardening 7:      c4451a5 docs: record company Redis dual-gate and limiter probes
hardening 8:      88245fa feat: add password-change and admin account disable
hardening 9:      36d86a4 docs: record hardening capacity soak and PRE_AUTH smoke
```

### classic-farm-h5

```text
branch: codex/v03-access-session-contract-20260730
base:   8987cf0 feat: complete H5 recovery accounts and social farm flows
main:   7354973 feat: align H5 recovery with v0.3 session contract

hardening branch: codex/v03-access-session-hardening-20260730
hardening base:   7354973
hardening 1:      daae3e8 fix: send device risk signal and forget failed passwords
hardening 2:      0feed17 feat: add web account password change and logout-all UI
```

来源分支提交已推送到远端且未合并 master。加固分支当前只有本地提交；受本执行环境外部
Git/额度限制，尚未推送，不能把本地提交误写成远端已存在。

## 3. 为什么要做这次修改

### 3.1 HTTPS 登录不能直接等于 WSS 上线

旧模型容易把以下三个时刻混成一个：

```text
账号认证成功
Ticket 签发成功
新 WSS 真正建立成功
```

如果 HTTPS 登录时就推进 `active_session_epoch`，会出现：

```text
旧 WSS 正常运行
→ 页面申请一张新 Ticket
→ active_session_epoch 已变化
→ 但新 WSS 因弱网、页面关闭或 Gate 故障没有建立
→ 旧 WSS 被提前废弃
→ 玩家两边都不可用
```

因此选择“候选代次”：

```text
HTTPS Login/Renew
→ reserved_session_epoch
→ 不修改 active_session_epoch

ClientHello 原子核销 Ticket
→ active_session_epoch = reserved_session_epoch
→ 替换 SessionRoute
```

代价是 Ticket、SessionAuthority 和激活结果之间需要更严格的原子合同；收益是登录页刷新、
弱网换票和失败重连不会无故踢掉当前可用连接。

### 3.2 签名 Cookie 不能绑定某一条 WSS

Cookie 的职责是“允许浏览器免密再申请一次 Ticket”，不是证明某条 Socket 仍然存在。

如果 Cookie 绑定 `active_session_epoch`：

```text
WSS 重连或顶号
→ active_session_epoch 改变
→ Cookie 同时失效
→ 每次弱网都要求重新输入密码
```

本轮改为绑定：

```text
player_id
session_subject
authority_epoch
issued_at_ms / expire_at_ms
cookie_version
```

`authority_epoch` 只在修改密码、封禁、退出所有设备等安全动作时提升。普通 WSS 替换不使
Cookie 失效。

### 3.3 `connection_id` 本身不是充分 Fence

连接 ID、generation 和 route revision 都只在特定 Gate/boot/session 语境中有意义。
只比较其中一个字段会产生典型 ABA 问题：

```text
旧连接断开回调延迟到达
→ connection_id 被复用或新 route 已建立
→ 旧回调误删新 SessionRoute / 新 FarmPresence
```

所以当前可信 Fence 是：

```text
player_id
authority_epoch
active_session_epoch
gate_id
gate_boot_id
connection_id
connection_generation
route_revision
```

旧 Disconnect、KICK、Route Lease、SessionPermit、FarmPresence Leave 和异步 QueueAccepted
都必须比较适用范围内的完整 Fence。

### 3.4 ReadLoop 不能等待任意时长的业务 RPC

旧的同步实现容易形成：

```text
WebSocket ReadLoop
→ HandleBinary
→ 等后端 RPC
→ RPC 慢时 ReadLoop 停止读帧和 Pong
```

如果再为每个请求无限创建 goroutine，只是把阻塞改成内存和调度器失控。

本轮选择：

```text
ReadLoop
→ 每连接有界 CommandQueue
→ 每连接一个串行 SessionLane Worker
→ 全 Gate 有界 BackendPool
→ RPC Deadline
```

这样保留同连接命令顺序，同时把慢请求限制在明确的 item 数和 deadline 内。

### 3.5 SessionRoute 与 FarmPresence 不能合并

二者回答的问题不同：

```text
SessionRoute：这个玩家当前唯一可信连接在哪里？
FarmPresence：哪些连接正在观看某个农场？
```

如果把 `current_farm_id` 写进 Redis SessionAuthority：

- 每次访问好友农场都会写玩家全局 Session；
- 热门 Join/Leave 会放大 Redis 写；
- 登录路由和农场订阅形成双份权威；
- 农场广播仍需寻找真实观看者，不能只看农场主人是否在线。

因此：

```text
SessionAuthority
└── active SessionRoute

Gate ConnectionSessionContext
├── current_farm_id
└── current_subscription_id

FarmActor/FarmSubscriptionHub
└── FarmPresence by player / subscription
```

### 3.6 Snapshot 入队不等于 Join 已经完成

Join 时如果先读 Snapshot(V)，再直接把后续 FarmChange 当普通广播，可能出现：

```text
读取 Snapshot(V)
→ FarmChange(V+1) 提交
→ V+1 先到浏览器
→ Snapshot(V) 后到
```

或者 Snapshot 在 Gate 队列满时丢失，但 Presence 已经标为在线。

本轮使用：

```text
ABSENT
→ JOINING
→ Snapshot QueueAccepted
→ FLUSHING
→ 所有 Catch-up Delta QueueAccepted
→ IN_FARM
```

`QueueAccepted` 只表示进入精确连接的本地 FIFO，不表示浏览器已经渲染；但它足以成为
FarmSubscriptionHub 与 Gate 之间的交接确认点。

## 4. 最终主链

### 4.1 Web 账号密码

```text
H5
→ POST /api/v1/accounts/register 或 /api/v1/session/login
→ AccountCredential（公司持久 Redis）
→ PBKDF2 校验 / 渐进升级
→ SessionAuthority.Reserve
→ player-svr EnsurePlayerInitialized
→ Set-Cookie: farm_session
→ 返回一次性 ConnectionTicket
```

密码只在显式登录/注册请求内使用。之后断线：

```text
H5
→ POST /api/v1/auth/renew + HttpOnly Cookie
→ 校验 Cookie + authority_epoch
→ 新候选 Ticket
→ 不重复发送密码
```

### 4.2 WSS 激活

```text
H5
→ WSS Binary ClientHello(ticket, client_build)
→ Transport Adapter
→ Gate Core
→ Redis Lua：Ticket 状态 + SessionAuthority 原子检查
→ active_session_epoch / active route 提升
→ 本地 ConnectionRegistry 绑定
→ ServerHello(player_id, epochs)
```

若返回旧 Route：

```text
同 Gate/boot
→ 本地精确 KICK

不同 Gate
→ GateControlService.KickSession(old full fence)
→ 旧 Gate 比较完整 Fence
→ 标记旧 SessionLane STALE
→ 尝试发送 KICK Push
```

新 Route 成功不因旧 Gate KICK 失败而回滚。

### 4.3 命令准入

```text
SessionLane 本地 ACTIVE
+ Route Lease 未过期
+ client_seq 单调
+ 占用 BackendPool 槽位成功
= ADMITTED
```

ADMITTED 后即使另一端随后激活新 Session，当前已经离开 Gate 的命令允许完成。尚未 ADMITTED
的旧连接在观察到 KICK、续租 Fence 失败或 Lease 到期后不再进入后端。

### 4.4 Join 与切换农场

```text
Gate 保存 current_farm_id/current_subscription_id

首次 Join：
→ BeginSubscription
→ farm-svr Join
→ JOINING/FLUSHING/IN_FARM

切换农场：
→ 精确 LEAVE 旧 farm + old subscription
→ 清 Gate 旧下行绑定
→ BeginSubscription(new)
→ Join 新农场
→ Gate 上下文只指向新农场
```

这项是在提交前最终合同审查中发现并补齐的。此前 Gate 虽然能把下行切到新
`subscription_id`，旧 FarmActor Presence 却可能继续占人数并收广播。

## 5. 主要实现清单

### 5.1 AccountCredential

关键文件：

- `internal/gateway/account/service.go`
- `internal/gateway/account/redis.go`
- `internal/gateway/account/memory.go`

新增/收紧：

- `password_kdf_version`
- `password_kdf_params`
- `account_status`
- `created_at`
- `password_changed_at`
- 弱 PBKDF2 参数成功登录后 CAS 升级
- Disabled 与错误密码统一为 `ErrInvalidCredential`

全设备退出、修改密码和管理员禁用 HTTP 入口已完成；仍缺少找回密码与 MFA。

### 5.2 Cookie

关键文件：

- `internal/gateway/auth/http_session.go`
- `internal/gateway/auth/login.go`

变化：

- Cookie Secret 与 Permit Secret 做用途隔离派生；
- Claims 使用毫秒时间并兼容旧秒级字段；
- Cookie 绑定 `authority_epoch`，不绑定 active WSS；
- `CookieKeyVersion` 已进入 Claims；
- 新旧 WSS 替换不使 Cookie 失效；
- `CookieKeyRing` 按 claims 中的版本选择验签 key，新 Cookie 只使用 active version；
- active version 缺失、未知版本、退役版本和短 key 均 fail-closed；
- `HTTP_SESSION_COOKIE_ACTIVE_VERSION` 与 `HTTP_SESSION_COOKIE_KEYRING` 支持 Secret 注入；
- 已记录 old+new、切 active、等待 TTL+发布窗口、删除 old 的三阶段轮换流程。

未配置 KeyRing 的本地开发仍兼容历史 Permit Secret 派生 key。正式环境仍需把 KeyRing 放入
密钥托管系统，并完成一次真实多 Gate 滚动轮换演练。

### 5.3 Candidate Ticket 与原子激活

关键文件：

- `internal/gateway/auth/ticket/ticket.go`
- `internal/gateway/auth/ticket/redis.go`
- `internal/gateway/session/authority.go`
- `internal/gateway/session/authority_redis.go`
- `internal/storage/redis/rediskey/rediskey.go`

变化：

- Token 形状为 `v1.<player_id>.<opaque random>`；
- Redis 只保存 Hash；
- Ticket 与 SessionAuthority 使用同一 `{player_id}` Redis Cluster hash tag；
- HTTPS `Reserve` 只推进 `next_epoch`；
- ClientHello `Activate` 才推进 `active_epoch`；
- 状态支持 UNUSED / CONSUMED / SUPERSEDED；
- CONSUMED Tombstone 保留激活结果；
- 同 Ticket + 同实际连接 Fence 可解析 Unknown Outcome；
- 同 Ticket + 不同 Fence 拒绝；
- 高候选激活后低候选不能抢回。

实现中曾发现一个边界：如果先检查 Ticket 原始 `expires_at`，CONSUMED Tombstone 在原 Ticket
过期后无法解析 Unknown Outcome。当前顺序已改为先处理 CONSUMED，再处理 UNUSED 过期。

### 5.4 SessionPermit

关键文件：

- `internal/gateway/session/permit.go`
- `internal/gateway/session/codec.go`
- `internal/logic/trusted/validate.go`
- `api/proto/logicrpc/logic_service.proto`

Permit 当前签名并绑定：

```text
player_id
authority_epoch
active_session_epoch
gateway_id
gateway_boot_id
connection_id
connection_generation
route_revision
request_id
action
operation_id
expires_at
```

Logic 不信任 H5 自报身份，也不能只验证签名而忽略 TrustedCommandContext 中的 Route Fence。
测试会逐项变异 Fence，并要求全部拒绝。

### 5.5 Route Lease

关键文件：

- `internal/gateway/session/authority_redis.go`
- `internal/gateway/command/gate.go`
- `internal/gatewayapp/app.go`

环境变量：

```text
GATE_ROUTE_LEASE_TTL，默认 30s
```

Gate 以约 TTL/3 批量续租。每条续租比较完整 Route Fence；失败只让对应本地 SessionLane
进入 stale。

真实公司 Redis 暴露过一个问题：

```text
redis.NewScript.Run(ctx, Pipeline, ...)
→ Pipeline.Exec 时才看到 NOSCRIPT
→ go-redis 无机会执行单命令模式下的 EVAL fallback
→ Gate 每轮续租失败
```

修复为在 pipeline 中直接排队 `EVAL`。续租本来已经是单次网络批处理，不需要依赖脚本缓存
来减少 round trip。真实 Redis 回归已覆盖该场景。

### 5.6 GateControl 与跨 Gate KICK

关键文件：

- `internal/gatewayapp/gate_control.go`
- `internal/gateway/command/gate.go`
- `api/proto/logicrpc/logic_service.proto`

新增：

```text
GateControlService.KickSession
GateKickStatus:
  KICKED
  STALE_TARGET
  TARGET_NOT_FOUND
```

配置：

```text
GATE_CONTROL_LISTEN=:9093
GATE_CONTROL_TARGETS=gate-a=host:9093,gate-b=host:9093
```

当前是静态地址映射。单元测试使用两个 Gate 实例验证了新 Gate 调用旧 Gate、旧命令停止准入、
KICK 进入精确旧 writer。

2026-07-30 补充真实进程 E2E（`scripts/test-dualgate-kick` + `cmd/dualgate-e2e`）：两个真实
farm-gate 进程共享公司持久 Redis SessionAuthority， farm-svr 使用
`GATEWAY_DOWNLINK_ROUTES` 双路由，Gate 使用 `GATE_CONTROL_LISTEN` +
`GATE_CONTROL_TARGETS` 静态映射。已验证 A 激活、B 顶号（epoch 1→2）、跨 Gate
`KickSession -> KICKED`、旧连接收到 KICK Push（ERR_SESSION_EXPIRED）、旧命令被拒绝
（gate 关闭 stale 连接，close 1006）、旧 Disconnect 不误删新 SessionRoute 与
FarmPresence（B 侧 GET_FARM_SNAPSHOT/Buy/Plant 成功且收到 FARM_CHANGE）。三个玩家
（10007 warm、10008/10009 首次激活）连续通过。跨 Gate KICK 路径同时补齐了发起/受理
两侧日志（此前错误被静默吞掉）。真实双 Pod 服务发现和跨节点故障注入仍待完成。

### 5.7 Gate 有界执行

关键文件：

- `internal/gateway/transport/websocket/websocket.go`
- `internal/gateway/command/gate.go`
- `internal/gateway/downlink/service.go`

配置：

```text
GATE_WS_COMMAND_QUEUE_SIZE=16
GATE_BACKEND_MAX_INFLIGHT=1024
GATE_BACKEND_RPC_TIMEOUT=3s
```

行为：

- ReadLoop 只负责读取、Binary/大小边界和有界入队；
- 每连接一个 Worker 串行调用 Gate Core；
- 全 Gate BackendPool 满时返回 `ERR_SERVER_BUSY`；
- RPC Deadline 覆盖 Permit 获取和后端调用；
- 同步 Response 和异步 Push 共享 ConnectionRegistry 的 server sequence；
- per-connection writer/command queue 满只淘汰该连接。

### 5.8 FarmPresence 与 Join Barrier

关键文件：

- `internal/farm/actor/actor.go`
- `internal/farm/actor/registry.go`
- `internal/farm/ingress/downlink.go`
- `internal/farm/ingress/service.go`

Presence 包含：

```text
viewer_player_id
session_epoch
gateway_id / gateway_boot_id
connection_id / generation
route_revision
subscription_id
presence_epoch
snapshot_version
phase
joining_buffer
```

规则：

- 同一玩家在同一农场最多一个当前 Presence；
- 高 session epoch 原子替换旧 Presence，不增加人数；
- 同 epoch + 同完整 Fence 幂等；
- 同 epoch + 不同 Fence 冲突，不能比较 generation 大小猜新旧；
- 200 人按不同玩家计数；
- JoiningBuffer 有 item、byte、duration 上限；
- 旧 QueueAccepted/Disconnect/失败回调不能删新 Presence；
- Queue Full 只移除慢 Presence，不回滚已提交业务。

### 5.9 H5 RecoveryBarrier

关键文件：

- `classic-farm-h5/src/app/App.ts`
- `classic-farm-h5/src/friends/game-friend-service.ts`
- `classic-farm-h5/src/protocol/canonical-v02-adapter.ts`
- `classic-farm-h5/src/network/ws-client.ts`（原有恢复核心）

ServerHello 现在带 `player_id`，浏览器不再只能从登录响应猜连接身份。

连接恢复并行发起：

```text
JoinOwnFarm
Pending Operation Recovery
Friend Roster
Friend Opportunity
必要时恢复目标好友农场
```

里程碑：

```text
IN_FARM
= 自己农场私有状态/Snapshot 就绪
+ Pending/目标恢复达到可写条件

HOME_READY
= IN_FARM
+ Friend Opportunity 查询完成
```

好友列表和机会查询失败不会伪装成完成，而是进入可重试状态；也不会阻塞已经安全恢复的核心农场。

新增 `/api/v1/friends/opportunity` 目前只是最小实现：返回第一名已确认好友或 NONE。它还不是
根据 FarmPublicSummary、成熟、可护理和可偷状态计算的真实机会索引。

## 6. 环境不是同一种认证方式

所有模式共享同一份 H5 源码，但 AuthAdapter 不同：

| H5 mode | 启动命令 | AuthAdapter | 是否验证真实后端会话合同 |
| --- | --- | --- | --- |
| mock | `npm run dev` | MockAuth + FakeServer | 否 |
| staging | `npm run dev:staging` | 固定测试 platform token | 是 |
| web | `npm run dev:web` | 账号密码 + Cookie renew | 是 |
| wechat | `npm run dev:wechat` | WeChat launch ticket | 尚未完整联调 |

本轮验收时曾出现“为什么不是账号密码页面”的误解。原因不是代码回退，而是运行中的 Vite 命令为：

```text
npm run dev:staging -- --host 0.0.0.0 --port 5174
```

要验收 Web 账号必须明确运行：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
npm run dev:web -- --host 0.0.0.0 --port 5174
```

## 7. 本轮执行顺序

实际实现按风险从内向外推进：

1. 冻结 Candidate/Active/Authority/Route 数据模型；
2. 实现 Memory Authority，先固定纯业务语义；
3. 实现 Redis 原子 Reserve/Activate/Clear/Renew/Revoke；
4. 把 Ticket 激活与 SessionRoute 更新合成同 slot Lua；
5. 扩展 Permit/TrustedCommandContext 完整 Fence；
6. 增加 Route Lease；
7. 增加跨 Gate KICK；
8. 将 WebSocket ReadLoop 改为有界队列；
9. 收紧 AccountCredential 和 Cookie；
10. 实现 FarmPresence JOINING/FLUSHING/IN_FARM；
11. 增加连接断开和农场切换的精确清理；
12. 对齐 H5 恢复屏障；
13. 跑单元、全量、H5 和公司 Redis 真实测试；
14. 构建本地二进制并重启四容器；
15. 从运行日志发现并修复 pipeline NOSCRIPT。

先做内存语义再做 Redis 是为了避免把 Lua、网络错误和业务状态机同时调试。先固定 Fence 再改
Gate/Farm，是为了让下游只能接受一种可信上下文。

## 8. 实施中遇到的问题

### 8.1 新前端请求新接口，但 8080 仍是旧容器

现象：

```text
GET /api/v1/friends/opportunity
→ 404
```

源码中路由已经存在。直接请求 8080 也 404，证明不是 Vite rewrite，而是前后端版本错配。

### 8.2 `docker compose --build` 不等于 Go 二进制已经更新

本地 Docker 使用 `Dockerfile.prebuilt`：

```text
宿主机先构建 .local-build/*
→ Docker 只 COPY 二进制
```

只运行：

```bash
scripts/company-redis-compose.sh up -d --build
```

可能继续 COPY 旧 `.local-build`。正确顺序：

```bash
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build --force-recreate
```

这是为了绕过容器内无法解析公司 Go 模块域名，不是普通 Docker 多阶段源码构建。

### 8.3 Redis Pipeline 的 NOSCRIPT

见 5.5。该问题只有冷启动后的真实公司 Redis + 活跃 WSS + 第一个续租周期才暴露，纯单元测试
无法代表。修复后增加了真实 Route Lease 回归。

### 8.4 农场切换旧 Presence 泄漏

仅调用 `ConnectionRegistry.BeginSubscription(new)` 能拒绝旧 Push，却不会通知旧 FarmActor
释放 Presence。最终补入 Gate 本地 current farm/subscription，并在切换前发送精确 Leave。

### 8.5 Staging 与 Web 被误认为功能开关回退

`staging` 的固定测试身份用于双账号自动化；`web` 才展示账号密码。两个模式共享后续的
SessionAuthority/Ticket/Gate/Farm 合同。交接时必须先报告当前 Vite mode。

### 8.6 首次双 Gate E2E 一次性 KICK 未达（未复现）

2026-07-30 首次运行 `test-dualgate-kick`（玩家 10007 首次激活）时，跨 Gate KICK 未在 6s
内到达旧连接，且当时 KICK 路径全程无日志（错误被 `_ =` 静默吞掉，无法定位环节）。补日志
后以同玩家 warm 状态与两个全新玩家（10008/10009 首次激活）连续三次复跑均通过，未再复现。
现已具备的诊断面：gate 发起侧记录 `GateControl KickSession ... -> <status>/rpc error`，
受理侧记录 `KickSession KICKED ... kick_push_enqueued=<bool>`。若再遇到 KICK 未达，先看
这两行确定失败环节；疑似方向是冷启动后首个 gRPC 连接建立超过 kick 的 1s 超时。

### 8.7 双 Gate 验证器复审与加固

后续复审发现原 E2E 的 `expectStaleClosed` 把所有读取错误都当作“服务端已经关闭”，其中也
包括纯读取超时。虽然原三次证据同时有 gate-a 的 `handle: gate: unauthenticated` 和
close 1006，能够支撑当次旧命令确实被拒绝，但验证器自身仍可能在其他运行中产生假阳性。

加固分支做了以下修改：

- 读取超时现在判为失败；只有 WebSocket Close、EOF、closed connection 或非 timeout
  网络关闭才算旧连接真正关闭；
- 新增分类单测，覆盖 timeout、WebSocket Close、EOF、`net.ErrClosed` 和 connection reset；
- `test-dualgate-kick` 不再复用本地 compose 的 player-svr，而是自启独立 player-svr；
- 默认生成随机测试 player_id，Player/Farm/Friend/Workflow/Share/WebAccount 使用
  `farm:test:dualgate:<run_id>` 运行级前缀；
- E2E 结束后仅删除根据 player_id、ticket hash、workflow operation_id 推导出的精确测试
  key；拒绝非 `farm:test:dualgate:` 前缀，不使用 `KEYS`、`FLUSHDB` 或宽泛删除；
- GateControl Client 创建后立即 `Connect()`，KICK RPC 使用 `WaitForReady`；
- KICK timeout 从硬编码 1s 改为 `GATE_CONTROL_KICK_TIMEOUT`，默认 3s；
- 每条旧连接命令仍在 `IssuePermit` 时读取 Redis 当前 active epoch；因此 KICK 超时影响的是
  旧页面提示/关闭时延，不会让旧 Gate 取得新的有效 Permit。

加固后的 `go test ./...` 已通过。当前受限执行环境内隔离 player-svr 未能就绪，外部执行又因
额度限制未获批准，因此新的隔离脚本尚缺一次公司 Redis 实跑。原提交的三次真实进程结果保留为
历史证据，但冻结前必须再跑一次新脚本。

## 9. 已执行验证

### 后端

```text
GOCACHE=/tmp/farm-game-go-cache go test ./...
PASS
```

覆盖新增重点：

- Candidate 登录不提前 Fence 旧 WSS；
- Cookie 跨 WSS 替换仍有效；
- Revoke 后旧 Cookie 失效；
- Ticket 同 Fence Tombstone；
- Ticket 不同 Fence 重放失败；
- 高候选阻止旧候选抢回；
- 旧 Disconnect 不清新 Route；
- Permit 每个 Fence 字段变异均失败；
- BackendPool 满时有界拒绝；
- 每连接命令 Lane 有界；
- 跨 Gate KICK 精确旧 Route；
- FarmPresence 替换、容量和 JoiningBuffer；
- Snapshot/Delta QueueAccepted 屏障；
- 切换农场先 Leave 旧订阅。

### 公司 Redis

真实测试：

```text
TestLiveRedisTicketActivation
PASS
```

覆盖 Ticket 激活、Tombstone、SUPERSEDED、旧 Disconnect 和 Route Lease EVAL pipeline。
测试使用临时 player key 并清理，不输出凭据。

### 双真实 Gate 进程 E2E（2026-07-30 原始证据）

```text
DUALGATE_DEMO_PLAYER_ID=10007/10008/10009 ./scripts/test-dualgate-kick
PASS ×3（10007 warm；10008、10009 首次激活）
```

断言覆盖：A 激活 ServerHello；A JoinOwnFarm snapshot；B 顶号 active_session_epoch 1→2；
旧连接收 KICK Push（ERR_SESSION_EXPIRED）；stale 连接命令被拒绝（服务端 close 1006，
gate-a 日志 `handle: gate: unauthenticated`）；B Join snapshot；旧 Disconnect settle 后
B 侧 GET_FARM_SNAPSHOT 成功（Route 未误删）、Buy+Plant 成功、收到 FARM_CHANGE
（Presence 未误删）。编排侧同时验证 `GATEWAY_DOWNLINK_ROUTES` 双 Gate 下行路由。

加固后的推荐命令不再指定固定玩家：

```text
./scripts/test-dualgate-kick
```

脚本会自启 player-svr、生成运行级前缀和测试玩家，并在结束时精确清理。当前代码证据：

```text
bash -n scripts/test-dualgate-kick            PASS
go test ./...                                 PASS
```

2026-07-31 在公司持久 Redis 上复跑加固脚本（随机玩家 `772463643884348`，运行级前缀，
结束精确清理 8 个 key）：

```text
./scripts/test-dualgate-kick                  PASS
step1 A 激活 active_session_epoch=1
step2 A JoinOwnFarm snapshot
step3 B 顶号 active_session_epoch 1 -> 2
step4 跨 Gate KICK ERR_SESSION_EXPIRED
step5 旧命令拒绝（websocket close 1006 / unauthenticated）
step6 B JoinOwnFarm snapshot
step7a Route 未被旧 Disconnect 删除
step7b BuyOffer + PlantCrop 在 B 成功
step7c Presence 未被旧 Disconnect 删除（FARM_CHANGE）
cleanup removed 8 exact Redis test keys
```

gate-b 日志确认 `GateControl KickSession ... -> KICKED`；gate-a 确认
`KickSession KICKED ... kick_push_enqueued=true`。

同时在本日重新取得 Web 账号基线证据：`go test ./...` PASS（32 包）、H5 62 测试 PASS、
`build:web` PASS、`e2e:web` Playwright 注册/加好友/重登 PASS、经 Vite 代理的
register/login/renew/opportunity HTTP 冒烟 PASS。

### H5

```text
npm run gen:proto
npm run build:web
npm test -- --run
```

结果：

```text
build PASS
13 test files PASS
62 tests PASS
```

### Cookie KeyRing 加固

```text
go test ./internal/gateway/auth ./internal/gatewayapp             PASS
go test -race ./internal/gateway/auth ./internal/gatewayapp       PASS
go test ./...                                                     PASS
go vet ./internal/gateway/auth ./internal/gatewayapp              PASS
```

覆盖旧 key 轮换窗口、新 active key 签发、旧部署拒绝未来版本、旧 key 退役后立即失效、
active version 缺失、短 key、重复版本和非法 base64url 配置。

### 全设备退出与安全撤销

新增 HTTP 入口：

```text
POST /v1/auth/logout-all
POST /api/v1/auth/logout-all
```

它不是只删除当前浏览器 Cookie，而是执行：

```text
校验 farm_session 签名、过期时间和 authority_epoch
→ SessionAuthority.RevokeIfCurrent(player_id, expected_authority_epoch)
→ 在 Authority 内原子 compare-and-revoke
→ 同时推进 player authority fence 与 active session fence
→ 清除当前浏览器 farm_session
```

选择 compare-and-revoke，而不是“先 GET epoch 再无条件 Revoke”，是为了关闭并发窗口：延迟到达
的旧注销请求不得撤销其后建立的新 authority generation。内存实现用同一把锁完成，Redis 实现
用单条 Lua 脚本完成。

验证结果：

```text
go test ./...                                                        PASS
go test -race ./internal/gateway/session ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
go vet ./internal/gateway/session ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
```

回归覆盖：

- 成功响应清除 `HttpOnly`/`Secure`/`SameSite`/`Path=/` Cookie；
- 原 Cookie 立即无法认证、无法 renew；
- 注销前的活跃 WSS 无法再获得新 SessionPermit；
- stale authority 的延迟注销不能撤销更新代次；
- 非 POST 和无认证 Cookie 被拒绝。

已准入命令和已签发 Permit 不做分布式“撤回”；Permit 只活到其短 TTL。此行为与
HC-ACCESS-17 的准入线性化合同一致。

### 账号/IP/设备联合登录限流

账号登录和注册现在都在读取账号记录和执行 PBKDF2 前经过三维风险桶：

```text
account token bucket
ip token bucket
device token bucket
```

引入 Redis 不是为了“多一个组件”，而是解决原内存 `failuresByUsername` 可通过切换 Gate 绕过，
且无法在昂贵 PBKDF2 前限制 IP/设备总流量的问题。实现选择如下：

- 有 Redis：所有 Gate 共享三个 Redis Token Bucket；
- 无 Redis：单 Gate 本地开发使用同语义 Memory limiter；
- 三个维度各自单 key Lua，以 Redis `TIME` 作为共享时钟；
- 三条命令 Pipeline 降低往返，但不伪装成跨 Cluster slot 事务；
- 成功只清空账号桶，IP/设备仍统计总 KDF/注册负载；
- Redis 错误 fail-closed 为 503，限流返回 429 + `Retry-After`；
- H5 显式发送稳定 `device_id`，旧客户端回退 `bootstrap_id`；
- `device_id` 可伪造，只是风险信号，不参与认证或 Actor/资产路由；
- 用户名、IP、设备 ID 在 Redis key 前经用途隔离 HMAC，不保存原文；
- HTTP XFF 只在可信代理后采纳，并从右向左剥离可信代理，避免 append 型代理让伪造首项绕过；
- Prometheus 仅使用 operation/result/dimension 固定标签，不引入原始标识和高基数。

同时修正 H5 既有偏差：账号密码在构造一次 HTTPS 请求后立即从 Provider 字段清除，401、
网络错误和成功路径都不继续持有密码。

验证结果：

```text
go test ./internal/gateway/account ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
go test -race ./internal/gateway/account ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
go vet ./internal/gateway/account ./internal/gateway/auth \
  ./internal/gateway/transport/websocket ./internal/gatewayapp      PASS
ClientIP 防伪定向单测                                                PASS
H5 13 test files / 63 tests                                        PASS
H5 build:web                                                        PASS
Redis 双 limiter 实例共享 Lua 探针                                  PASS（2026-07-31）
```

Redis live probe 为显式 opt-in，使用随机 `farm:test:web-login-limit:*` 前缀并精确清理。
2026-07-31 在公司 Redis 上：

```text
FARM_REDIS_LIVE_TEST=1 go test ./internal/gateway/account \
  -run TestRedisAttemptLimiterSharedAcrossGateInstancesLive -count=1
PASS
```

断言：Gate A 第一次允许、Gate B 第二次允许、共享账号桶第三次拒绝并带 Retry-After、
ResetAccount 后再次允许。Go 全量测试除需要 TCP listener 的既有 WebSocket 包在部分沙箱
会被拒绝外，其余包通过；ClientIP 防伪单测可独立运行并通过。

### PRE_AUTH 未认证资源边界

HTTP Upgrade 或 TCONND START 只代表 transport 已建立，不能让连接在 ClientHello 激活前无限
占用 Gate 内存与调度。实现把预算放在传输无关的 `preauth.Controller`，由 Direct WebSocket 与
gateproxy 共用；唯一成功释放点是 `command.Gate` 在 ClientHello 原子激活后调用
`Authenticate`。

边界语义：

```text
Open(connection_id, generation, client_ip)
→ Admit 恰好一帧 FarmFrame（默认）并计入 connection/IP/Pod 字节
→ ClientHello 激活成功 → Authenticate 释放全部预算
→ 超时 / 断开 / 非法首帧 / 第二帧 / 超限 → Timeout 或 Close 释放同一 Fence
```

关键选择：

- Transport 在真正认证前不启动普通 SessionLane / 命令队列；
- Gate 定义认证完成点，Transport 不猜测响应内容；
- gateproxy 使用 TCONND START 提供的来源 IP；失败只 STOP 对应玩家连接，不关闭复用 GateStream；
- Ping/Pong 与 GateStream heartbeat 不计作 FarmFrame；
- 无法解析的 IP 仍消耗 Pod 预算，但不创建可由客户端任意命名的 IP bucket；
- `GATE_WS_METRICS=1` 时暴露 connections/bytes/rejected/transitions/duration，标签仅用固定
  reason/result，不含 IP、连接 ID 或玩家 ID。

运维默认（不是 3000 万 DAU 容量结论）：

```text
GATE_PREAUTH_MAX_CONNECTIONS=4096
GATE_PREAUTH_MAX_CONNECTIONS_PER_IP=64
GATE_PREAUTH_MAX_FRAMES_PER_CONNECTION=1
GATE_PREAUTH_MAX_BYTES_PER_CONNECTION=65536
GATE_PREAUTH_MAX_BYTES_PER_IP=1048576
GATE_PREAUTH_MAX_BYTES_POD=67108864
GATE_PREAUTH_HELLO_TIMEOUT=10s
```

验证结果：

```text
go test ./internal/gateway/preauth ./internal/gateway/command \
  ./internal/gateway/transport/gateproxy ./internal/gatewayapp      PASS
go test -race ./internal/gateway/preauth ./internal/gateway/command \
  ./internal/gateway/transport/gateproxy ./internal/gatewayapp      PASS
go vet ./internal/gateway/preauth ./internal/gateway/command \
  ./internal/gateway/transport/gateproxy \
  ./internal/gateway/transport/websocket ./internal/gatewayapp      PASS
go test $(go list ./... | grep -v websocket$)                      PASS
docker compose config (local/prod, 占位 Redis URL)                   PASS
需要 TCP listener 的 websocket 包整包测试                           PENDING（沙箱禁 listener）
```

### 加固回归压测（公司 Redis，2026-07-31）

在包含 PRE_AUTH、登录限流、KeyRing、Revoke、Join 故障注入的加固二进制上复跑容量脚本。
原始证据：

```text
docs/verification/results/ws-join-20260731T044220Z.json   # 60s 校准
docs/verification/results/ws-plant-20260731T044220Z.json
docs/verification/results/ws-join-20260731T044400Z.json   # 10m 浸泡
docs/verification/results/ws-plant-20260731T044400Z.json
docs/verification/results/runtime-20260731T044400Z.log
docs/verification/results/preauth-timeout-smoke-20260731T045400Z.json
```

| 阶段 | Join | Plant | Ready/Succeeded | 错误 | Ready P95 | Plant P95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 60s 校准 | 100 | 25 | 100 / 25 | 0 | 7.60 ms | 9.79 ms |
| 10m 浸泡 | 200 | 50 | 200 / 50 | 0 | 7.49 ms | 9.68 ms |

10m 浸泡期间采样：`conn_rejected=0`、`slow_client_kick=0`、`mailbox_rejected=0`、
`unexpected_disconnects=0`。PRE_AUTH 指标在浸泡结束时 `authenticated=250`
（200 Join + 50 Plant）。这不是 3000 万 DAU 结论，只证明加固边界未在中等真实协议
负载下误伤正常 ClientHello/Join/Plant。

额外冒烟：

```text
PRE_AUTH：打开 32 条未发 ClientHello 的 WSS → connections=32
          Hello timeout 后 closed=32/32，transitions{timeout}=32，connections=0
登录限流共享 Lua live probe                                 PASS
加固双 Gate ./scripts/test-dualgate-kick                     PASS
```

### 修改密码与管理员禁用

```text
POST /v1/accounts/change-password
POST /api/v1/accounts/change-password
→ 校验当前密码 → CAS 更新 verifier → SessionAuthority.Revoke(player_id)
→ 清除 farm_session Cookie（强制重登）

POST /v1/admin/accounts/disable
Header: X-Farm-Admin-Token / Authorization Bearer
→ account_status=DISABLED → SessionAuthority.Revoke(player_id)
```

与 `logout-all` 的区别：改密/禁用使用**无条件** Revoke，确保并发新登录也会被一起踢掉；
`logout-all` 使用 compare-and-revoke，避免延迟旧请求误撤更新代次。禁用路由仅在配置
`WEB_ACCOUNT_ADMIN_TOKEN` 时注册。微信 `/auth/exchange` 已明确排除出 v0.3 Web P0。

验证结果：

```text
go test ./internal/gateway/account ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
go test -race ./internal/gateway/account ./internal/gateway/auth \
  ./internal/gatewayapp                                             PASS
```

### Join Snapshot/Delta 故障注入

针对生产路径 `farm-svr`（`internal/farm/*`）补齐合同要求的故障注入，而不是只依赖遗留
`internal/logic` 路径：

```text
Snapshot QUEUE_FULL → 只移除慢 Presence，已提交 Plant 不回滚
catch-up Delta QUEUE_FULL → Presence leave，业务 CAS 结果保留
JOINING 期间两次 publish → 连续 V/V+1 catch-up 后进入 IN_FARM
旧 gateway_boot_id LeaveConnectionFence → 不能删除新 boot Presence
Gate ConnectionWriter QUEUE_FULL → DeliverBatch 映射 QUEUE_FULL 且保留已入队帧
```

验证结果：

```text
go test ./internal/farm/ingress ./internal/farm/actor \
  ./internal/gateway/downlink                                    PASS
go test -race ./internal/farm/ingress ./internal/farm/actor \
  ./internal/gateway/downlink                                    PASS
```

当前恢复信号仍是静默 Presence leave；设计文档中的 `RESYNC_REQUIRED` 推送尚未发出。H5 已有
本地 version gap → re-Join 路径。是否在 P0 补服务端 Resync 推送待产品确认。

### 运行态

四个本地容器：

```text
player-svr healthy
farm-svr healthy
farm-worker healthy
farm-gate healthy
```

经 Vite 代理实测：

```text
POST /api/v1/auth/login                  200
GET  /api/v1/friends/opportunity         200
Route Lease 至少一个续租周期无 NOSCRIPT
```

## 10. HC-ACCESS 映射

| 合同 | 当前状态 | 说明 |
| --- | --- | --- |
| HC-ACCESS-01 | 已实现 | HTTPS 只 Reserve，不提前替换 active |
| HC-ACCESS-02 | 已实现 | Cookie renew 免密；Ticket 单次短 TTL |
| HC-ACCESS-03 | 已实现 | ClientHello 原子激活 |
| HC-ACCESS-04 | 已实现 | 单 Ticket、SUPERSEDED、同 Fence Tombstone |
| HC-ACCESS-05 | 已实现 | CompareAndClear 完整 Route Fence |
| HC-ACCESS-06 | 已实现 | 传输无关 PRE_AUTH：连接/IP/帧/字节/Pod 预算、Hello timeout、指标；Direct WS 与 gateproxy 共用；监听型 WS 整包测试仍受沙箱限制 |
| HC-ACCESS-07 | 已实现 | 一个 WS Binary Message 对应一个 FarmFrame |
| HC-ACCESS-08 | 代码边界已实现 | Direct WS/gateproxy 共用 Gate Core；TCONND 真实端到端仍未通过 |
| HC-ACCESS-09 | 已实现 | 每连接队列、BackendPool、Deadline |
| HC-ACCESS-10 | 已实现 | Response/Push/Control 共用 FIFO/sequence |
| HC-ACCESS-11 | 已实现 | SessionRoute 与 FarmPresence 分离 |
| HC-ACCESS-12 | 已实现 | 同玩家同 Farm 单 Presence |
| HC-ACCESS-13 | 已实现 | JOINING/FLUSHING/IN_FARM |
| HC-ACCESS-14 | 代码路径已具备 | Actor 产出 Recipient，Dispatcher/Gate 下行；热门 Farm 扇出压测仍待 |
| HC-ACCESS-15 | 已实现（farm 路径故障注入） | Snapshot/catch-up QUEUE_FULL 只移慢 Presence、不回滚已提交业务；JOINING 缓冲连续 catch-up；旧 Gate boot fence 不能删新 Presence；服务端仍未推 RESYNC_REQUIRED（静默 leave，客户端 gap/重连自愈） |
| HC-ACCESS-16 | 已实现 | Drain/断线通过 Renew + Re-Join + Snapshot |
| HC-ACCESS-17 | 已实现 | ADMITTED 请求允许完成，stale 后不再准入 |
| HC-ACCESS-18 | 已实现并实测 | 批量续租、Lease 到期 fail-closed |
| HC-ACCESS-19 | 已实现并实测 | 原 E2E ×3 玩家通过；加固隔离脚本 2026-07-31 公司 Redis 复跑 PASS；动态服务发现与跨节点网络故障注入仍待 P1 |

## 11. 尚未完成与优先级

### P0：完成 v0.3 冻结前必须收口

1. ~~Redis/统一风控级账号 + IP + device 登录限流~~（代码、HMAC、可信代理、指标与
   2026-07-31 公司 Redis 共享 Lua live probe 均 PASS；正式阈值调优与告警仍属 P1）；
2. ~~Cookie KeyRing、当前/历史密钥和轮换窗口~~
   （代码、环境合同和轮换/退役测试已完成；正式 Secret 托管与真实滚动演练仍归 P1）；
3. ~~退出所有设备 / 修改密码 / 禁用账号的 Revoke 产品入口~~
   （`logout-all` 用 `RevokeIfCurrent`；`change-password` 与 admin `disable` 用无条件
   `SessionAuthority.Revoke`；H5 UI 未做；找回密码仍未做）；
4. ~~PRE_AUTH 未认证连接、字节、帧数预算及指标~~（代码、双 Transport 接线、Compose/K8s
   配置、控制器/Gate/gateproxy 单测与 race 已完成；监听型 WebSocket 整包测试待可监听环境）；
5. ~~双 Gate 加固验证器真实复跑~~（2026-07-31 公司 Redis：A 激活、B 顶号、KICK、旧命令拒绝、
   旧 Disconnect 不误删 Route/Presence，PASS 并精确清理）；
6. ~~Join Snapshot/Delta 的 Queue Full、Gate 重启、版本空洞故障注入~~
   （farm-svr 路径单测 + race：Snapshot/catch-up QUEUE_FULL、连续 catch-up、旧 boot fence；
   真实多进程 Gate kill / 滚动发布仍属 P1；是否补推 RESYNC_REQUIRED 待确认）；
7. ~~Web 账号密码真实浏览器回归重新运行并保存本分支证据~~
   （2026-07-30 已完成：`e2e:web` Playwright 通过 + HTTP 冒烟，证据见 §9）；
8. ~~统一微信 H5 `/auth/exchange`~~：**明确排除在 v0.3 Web P0 外**。Web 账号密码路径是本期
   冻结范围；微信 launch_ticket / `/auth/exchange` 归后续微信联调里程碑，不阻塞 Web P0。

### P1：生产接入

1. CLB WSS、Origin、Trusted Proxy、真实 IP、Idle Timeout；
2. `GATE_CONTROL_TARGETS` 动态服务发现或可运维配置；
3. KICK/Lease/BackendPool/Join Barrier 指标与告警；
4. Cookie/Permit Secret 正式密钥托管；
5. 真实滚动发布和弱网矩阵；
6. 热门 Farm 200 人和慢客户端扇出测试。

### P2：产品体验

1. FriendOpportunity 改为真实 FarmPublicSummary/FriendIndex；
2. 好友有可护理/可偷标记；
3. 机会索引延迟和错误不阻塞 Home，但提供明确 UI 状态。

## 12. 下一位 Agent 建议执行顺序

```text
1. 先确认两个仓库都在 v0.3 分支，工作区干净
2. 确认 H5 mode：web 还是 staging，不要混用验收结论
3. 重跑 go test ./...、npm test、build:web
4. ~~跑 web account Playwright~~（2026-07-30 已通过）
5. ~~在公司 Redis 运行加固后的 `./scripts/test-dualgate-kick`~~（2026-07-31 PASS）
6. ~~实现退出所有设备的原子 Revoke HTTP 闭环~~（本地完成，待推送）
7. ~~实现 Redis 账号/IP/device 登录限流代码与隔离探针~~（2026-07-31 公司 Redis PASS）
8. ~~做 PRE_AUTH 预算~~（本地完成；监听型 WS 整包测试待可监听环境）
9. ~~做 Queue Full/版本空洞/Gate boot fence 故障注入~~（farm 路径单测完成；滚动发布属 P1）
10. ~~实现修改密码/禁用账号入口并调用无条件 `SessionAuthority.Revoke`~~
11. ~~明确微信 `/auth/exchange` 排除出 v0.3 Web P0~~
12. 推送加固分支；需要时再做 H5 change-password UI / RESYNC_REQUIRED 推送
13. 更新本文件 HC 表，只有有运行证据才改“完成”
```

## 13. 安全与操作注意

- 不把 `deploy/secrets/redis.conf.local`、Redis URL、Cookie、Ticket 或密码写入日志/文档；
- 不直接运行 compose 就假定二进制更新，先 `prepare-local-compose.sh`；
- 不用 Staging 固定身份验收 Web 账号表单；
- 不用 Mock 环境证明 Redis/Ticket/Route 合同；
- 不因 KICK 失败回滚已成功的新 active Route；
- 不用 connection generation 大小跨 Gate 判断新旧；
- 不在 Gate 编排 Player/Farm 业务 Participant；
- 不把 FarmPresence 写入 SessionAuthority。
