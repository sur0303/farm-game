# 经典农场当前两个仓库整体交接

日期：2026-07-30  
交接对象：下一位开发、验证或架构 Agent  
仓库：

- `/data/home/developer_026/CodeBuddy/20260519141303/farm-game`
- `/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5`

## 1. 一句话状态

当前已经形成一套可本地运行的 H5 + Go 多进程农场纵切：支持 Web 账号或 Staging 身份、
三种作物、经济/种植/好友护理与偷菜 Loop、任务与图鉴、可靠 Workflow、公司持久 Redis、
Direct WebSocket、弱网恢复和第一版静态 Actor Shard 路由。

它不是“整个 3000 万 DAU 生产游戏已完成”。主要缺口仍是 Redis 平台真实切主/备份恢复、
动态 Actor 控制面、v0.3 接入合同剩余安全/生产探针、真实微信/TCONND/CLB 和更多外围玩法。

## 2. 当前分支和提交

### farm-game

```text
branch: codex/v03-access-session-hardening-20260730
base:   3ec4a42 docs: record HC-ACCESS-19 dual-gate E2E and web baseline evidence
source: codex/v03-access-session-contract-20260730
local:  7dde3c4 test: harden dual-gate session takeover verification
local:  ebece72 feat: add rotating HTTP session cookie keyring
local:  919a5d6 feat: add all-device session revocation endpoint
local:  d8ce33c feat: enforce distributed web login risk limits
local:  2b0fc71 feat: bound pre-auth transport resources before ClientHello
local:  4cae35f test: inject join queue-full and version catch-up faults
local:  c4451a5 docs: record company Redis dual-gate and limiter probes
local:  f202edc docs: pin company Redis probe evidence commit hash
local:  88245fa feat: add password-change and admin account disable
local:  271a6fc docs: record H5 password-change and logout-all UI evidence
local:  36d86a4 docs: record hardening capacity soak and PRE_AUTH smoke
```

### classic-farm-h5

```text
branch: codex/v03-access-session-hardening-20260730
base:   7354973 feat: align H5 recovery with v0.3 session contract
local:  daae3e8 fix: send device risk signal and forget failed passwords
local:  0feed17 feat: add web account password change and logout-all UI
source base: 8987cf0 feat: complete H5 recovery accounts and social farm flows
```

`classic-farm-h5` 的合同来源分支和 `farm-game` 的来源分支已推送，均未合并 master。
两个仓库的加固分支当前只有本地提交；受本执行环境外部 Git/额度限制尚未推送。开始工作前先执行：

```bash
git branch --show-current
git status --short
git log -3 --oneline
```

不要直接切 master 后把当前运行结果当作 master 能力。

## 3. 当前运行拓扑

```text
Browser
  │
  ├── HTTP /api
  └── WebSocket /ws
          │
          ▼
Vite :5174（本地代理）
          │
          ▼
farm-gate :8080
  ├── Auth HTTP / Cookie / Ticket
  ├── Direct WebSocket Transport
  ├── SessionAuthority / SessionRoute
  ├── Command Catalog / RouteAdapter
  ├── GatewayDownlink :9092
  └── GateControl :9093
          │
    ┌─────┴─────┐
    ▼           ▼
player-svr    farm-svr
:19100        :19000
PlayerActor   FarmActor / FarmPresence
    ▲           │
    └─────┬─────┘
          ▼
     farm-worker
        :19201 health
          │
          ▼
公司持久 Redis
Player/Farm/Friend/Share/Workflow/Account/Session/Ticket
```

本地 compose 使用 host network。端口：

| 组件 | 地址 |
| --- | --- |
| H5 Vite | `http://127.0.0.1:5174` |
| farm-gate HTTP/WS | `127.0.0.1:8080` |
| Gate downlink | `127.0.0.1:9092` |
| Gate control | `127.0.0.1:9093` |
| player-svr gRPC/health | `127.0.0.1:19100` / `19101` |
| farm-svr gRPC/health | `127.0.0.1:19000` / `19001` |
| farm-worker health | `127.0.0.1:19201` |

## 4. 正确启动方式

### 4.1 后端

本地镜像不是在 Docker 内编译 Go，而是 COPY 宿主机预编译文件：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build --force-recreate
./scripts/company-redis-compose.sh ps
```

`company-redis-compose.sh` 从 Git ignored 的：

```text
deploy/secrets/redis.conf.local
```

读取公司 Redis 配置。禁止打印或提交该文件。

仅运行 `docker compose --build` 可能继续使用旧 `.local-build`，造成“源码有接口、运行时 404”
或“前端有多作物、服务端只认胡萝卜”。

### 4.2 H5 Web 账号模式

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
npm run dev:web -- --host 0.0.0.0 --port 5174
```

页面显示账号、密码、登录和“注册并进入”。

### 4.3 H5 Staging 双账号模式

```bash
npm run dev:staging -- --host 0.0.0.0 --port 5174
```

页面显示 `player-a/10001` 和 `player-b/10002`，用于固定双人自动化。它不是账号密码功能回退。

### 4.4 Mock

```bash
npm run dev
```

只使用 FakeServer，不能用于证明 Redis、Ticket、SessionRoute、Workflow 和多进程正确性。

## 5. 服务权威边界

### farm-gate

负责：

- HTTP 身份入口和 Cookie；
- Candidate Ticket；
- WSS ClientHello；
- SessionRoute、Route Lease、KICK；
- FarmFrame、序号、限流、背压；
- 选择命令首跳；
- 下行 ConnectionWriter。

不负责：

- 修改钱包/背包；
- 直接修改地块；
- 推进 Plant/Harvest/Theft Participant；
- 把客户端自报 player_id 当权威。

### player-svr / PlayerActor

权威状态：

- 金币、经验；
- 种子和作物库存；
- 商店购买/出售 Receipt；
- Reservation/Grant；
- 任务实例和领奖；
- 植物图鉴。

### farm-svr / FarmActor

权威状态：

- 12 块地；
- CropCycle；
- 农场版本和地块版本；
- 浇水、成熟、收获、偷菜；
- Theft Settlement；
- FarmPresence 和 Join Buffer。

### farm-worker

只接管未完成 Workflow，不直接编辑 Player/Farm blob。它根据权威 Receipt 幂等调用 Participant，
避免重复扣种子、重复发奖或重复偷菜到账。

## 6. 命令首跳与 Workflow

路由按命令目标，不按“第一个参与者是谁”机械决定。

```text
经济/任务命令
→ player_id
→ player-svr

农场目标命令
→ farm_id / target_owner_id
→ farm-svr
```

Plant 当前保持：

```text
H5
→ farm-gate
→ farm-svr / FarmOperationCoordinator
→ CreateIfAbsent PlantWorkflow
→ PlayerActor.ReserveSeed
→ FarmActor.CommitPlant
→ PlayerActor.CommitReservation
→ CompleteWorkflow
```

种子属于 PlayerActor，只说明 `ReserveSeed` 必须由 PlayerActor 裁决，不推出 PlantCoordinator
必须部署在 player-svr。Gate 只选首跳，不编排 Participant。

Care/Theft 以目标农场为中心，由 farm-svr 内对应协调路径处理；涉及访客收益时通过 Player
Participant 和持久 Workflow/Settlement 完成。

## 7. 已实现产品能力

### 7.1 身份与会话

- Web 账号注册、密码登录；
- Staging 固定双账号；
- PBKDF2 随机盐和持久账号；
- HttpOnly `farm_session`；
- Cookie 免密换 Ticket；
- Candidate Ticket 原子 ClientHello 激活；
- 顶号、Route Lease、跨 Gate KICK 代码；
- Drain/断线重新 Join；
- H5 RecoveryBarrier。

专项细节见 `v03-access-session-handoff.md`。

### 7.2 个人农场

- 12 地块；
- 空地选择；
- 购买种子；
- 播种；
- 服务端时间成熟；
- 浇水；
- 收获；
- 农场/地块版本冲突；
- 断线 Snapshot 恢复；
- 成熟作物服务端拒绝再次浇水。

### 7.3 作物目录

服务端权威目录 `internal/cropcatalog` 当前包含：

| ID | 作物 | 购买 | 成熟 | 基础产量 | 出售 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 胡萝卜 | 10 | 10s | 10 | 15 |
| 2 | 番茄 | 15 | 15s | 8 | 22 |
| 3 | 玉米 | 20 | 20s | 6 | 30 |

H5 展示目录不裁决价格。出现 `offer is unavailable` 时先检查运行中的 `player-svr` 是否为新二进制。

### 7.4 经济

- 金币；
- 种子和成熟作物库存；
- 买种子；
- 单个/全部出售；
- 收获和偷菜发物品；
- OperationReceipt 幂等；
- Unknown/PROCESSING 查询和恢复。

### 7.5 好友与实时社交

- 创建分享链接；
- 接受分享并双向建好友；
- 好友列表；
- FriendRosterChanged 实时通知；
- 解除好友；
- 访问好友农场；
- 返回自己农场；
- 给好友浇水；
- 偷菜；
- 双方 FarmChange；
- 好友权限和次数由服务端裁决。

当前 `/friends/opportunity` 仅返回一个已确认好友，不是精确的可护理/可偷索引。

### 7.6 任务与图鉴

任务：

- 新手收获：收获任意 3 个成熟作物；
- 备足种子：购买 3 份种子；
- 服务端推进；
- CLAIMABLE/CLAIMED；
- 金币、经验和 Receipt 同一次 PlayerActor 写入。

图鉴：

- 首次收获对应作物后解锁；
- 图鉴状态持久化；
- 断线/进程重启后恢复。

未实现周期任务、任务链、赛季、图鉴奖励和邮件补偿。

## 8. 持久化

当前授权决定：先使用公司改造的持久 Redis 作为业务数据库，不以前置 Tcaplus 阻塞业务开发。

权威数据：

- PlayerRecord；
- FarmRecord；
- Friend relation；
- Share token；
- Workflow；
- Web Account。

临时/可重建数据：

- SessionAuthority/Route；
- ConnectionTicket；
- FarmPresence（进程内）；
- 路由缓存。

Player/Farm 使用确定性 protobuf blob、CAS version 和 writer epoch。Workflow 使用 record、
due zset、claim lease。不要把“Redis 能重启后读回”夸大成平台 RPO/RTO 已完成。

Tcaplus 已有：

- 集群与表格组；
- `OwnerCoreRecord` 真表；
- Insert/Get/RecordVersion/CAS 探针；
- 多表迁移 descriptor 形状。

它现在是迁移候选，不是本地三 Loop 前置依赖。

## 9. Actor 路由

已实现静态第一版：

```text
ActorAddress(realm, kind, id)
→ ShardCalculator
→ Virtual Shard
→ Static ShardAssignment
→ ActorRouteClient cache
→ holder endpoint
→ 存储 writer_epoch Fence
```

已验证 kind 分离 golden、完整 assignment、revision cache invalidation 和旧 holder 写 Fence。

未实现：

- 动态 Coordinator；
- Watch/Lease；
- 在线 Actor 迁移；
- Redirect；
- 多 Holder 真实切换；
- 加载风暴控制。

## 10. 弱网和滚动发布

已有：

- OperationJournal；
- Cookie renew；
- Candidate Ticket；
- WSS reconnect backoff；
- RecoveryBarrier；
- DrainNotice；
- 多 Gate readiness proxy 本地测试；
- pending operation 查询；
- Farm Snapshot 重建；
- ConnectionWriter 背压；
- Route Lease 和 KICK。

不做透明 Socket 迁移。滚动发布策略是：

```text
Gate readiness false
→ DrainNotice
→ H5 按抖动重连
→ Cookie Renew Ticket
→ 新 Gate ClientHello
→ Re-Join + Snapshot
```

## 11. 验证证据

### 当前分支新增验证

```text
go test ./...                                  PASS
公司 Redis Ticket + Route Lease live test      PASS
npm run build:web                              PASS
npm test -- --run                              62 PASS
四容器 health                                  PASS
/friends/opportunity authenticated smoke       200
```

2026-07-30 复跑并新增：

```text
go test ./...                                  PASS（32 包）
npm test -- --run                              62 PASS
npm run build:web                              PASS
e2e:web Playwright（注册×2/分享加好友/重登）    PASS
Web HTTP 冒烟（register/login/renew/opportunity）PASS
双真实 Gate 进程 E2E（scripts/test-dualgate-kick）
  ×3 玩家（含 2 个首次激活）                   PASS
```

上述双 Gate 三次结果属于 `3ec4a42` 之前的原验证器。后续复审并加固：

```text
旧连接读取 timeout 不再被误判成 transport close
双 Gate 脚本自启独立 player-svr
默认随机 player_id + farm:test:dualgate:<run_id> 前缀
结束后仅精确删除本次 Player/Farm/Workflow/Session/Ticket/Presence key
GateControl Connect 预热 + WaitForReady
GATE_CONTROL_KICK_TIMEOUT 默认 3s
go test ./...                                  PASS
bash -n scripts/test-dualgate-kick             PASS
加固脚本公司 Redis 真实复跑（2026-07-31）       PASS
  随机玩家 + 运行级前缀；KICK/stale/Route/Presence 全过；精确清理 8 key
```

同一加固分支继续完成 Cookie KeyRing：

```text
新 Cookie 只用 active version 签名
旧 Cookie 按 claims version 在轮换窗口内验签
删除旧 key 后旧 Cookie 立即失效
active key 缺失/短 key/重复版本/非法配置 fail-closed
Compose/Kubernetes 支持 Secret 注入版本化 KeyRing
go test ./...                                  PASS
auth/gatewayapp race + vet                     PASS
```

同一加固分支继续完成全设备退出：

```text
POST /v1/auth/logout-all
POST /api/v1/auth/logout-all
Cookie 鉴权后原子 compare-and-revoke authority_epoch
成功后清除当前浏览器 farm_session
旧 Cookie 认证/renew 立即失败
旧 WSS 不能取得新 SessionPermit
延迟 stale 注销不能撤销更新 authority generation
go test ./...                                  PASS
session/auth/gatewayapp race + vet             PASS
```

这里使用 compare-and-revoke 是必要的：如果先读取 epoch 再无条件推进，两个并发注销请求可能
交错，延迟的旧请求会错误撤销其后建立的新登录。Redis 实现把比较、推进 authority/active fence
和清除 route 放在一条 Lua 脚本里。

同一加固阶段继续完成 Web 登录风险限流：

```text
login/register 在 PBKDF2 前检查 account/IP/device Token Bucket
Redis 模式由所有 Gate 共享；无 Redis 本地模式保持同语义内存保护
每维度单 key Lua + Redis TIME；Pipeline 不制造跨 slot 热点
Redis 错误 503 fail-closed；限流 429 + Retry-After
原始 username/IP/device 只做 HMAC，不进入 Redis key 或指标标签
可信代理 XFF 从右向左剥离，抵抗客户端预置首项
H5 显式发送 device_id；该字段不参与身份和路由
H5 成功、401、网络失败都不继续持有密码字段
后端定向/race/vet                            PASS
H5 13 test files / 63 tests                  PASS
H5 build:web                                  PASS
共享 Redis Lua live probe（2026-07-31）        PASS
```

2026-07-31 加固二进制容量回归（公司 Redis，`run-local-capacity-stage`）：

```text
60s 校准  Join 100/100 + Plant 25/25           PASS（全错误 0）
10m 浸泡  Join 200/200 + Plant 50/50           PASS（全错误 0）
conn_rejected / slow_client_kick / mailbox_rejected = 0
PRE_AUTH smoke：32 未认证连接 timeout 后全部释放
双 Gate kick + 登录限流 Lua 复跑                 PASS
证据：results/ws-*-20260731T044220Z.json
      results/ws-*-20260731T044400Z.json
      results/runtime-20260731T044400Z.log
```

同一加固阶段继续完成 PRE_AUTH 未认证资源边界：

```text
preauth.Controller 传输无关：连接/IP/帧/字节/Pod 预算
Direct WS 与 gateproxy 共用同一 Controller
唯一成功释放点：Gate ClientHello 原子激活后 Authenticate
认证前不启动普通 SessionLane；第二帧/超限/超时只关单连接
TCONND START IP 进入预算；畸形 IP 不建可伪造 bucket
Compose/K8s 暴露 GATE_PREAUTH_*；指标标签无原始标识
preauth/command/gateproxy/gatewayapp 单测 + race + vet PASS
全量 go test（排除需 listener 的 websocket 包） PASS
监听型 websocket 整包测试                      PENDING（沙箱禁 TCP listener）
```

### v1.21 既有证据

- 三 Loop staging 浏览器 E2E；
- Worker/Player 故障后 exactly-once 恢复；
- 全业务进程重启从 Redis 恢复；
- 多 Gate Drain 本地 E2E；
- 静态 Actor route/writer epoch 测试；
- 30 分钟 400 Join + 100 Buy/Plant 本地基线；
- Join/Plant 场景零错误；
- Redis HA 稳态写和 WAIT 读回；
- Tcaplus OwnerCore CAS 探针。

详细报告：

- `v121-final-completion-audit.md`
- `v121-milestone-audit.md`
- `v121-m6-capacity-and-observability.md`
- `v122-web-account-login.md`
- `v03-access-session-handoff.md`

## 12. 已知运行问题与诊断顺序

### 页面接口 404

先直接请求 8080。若 8080 也 404，通常是旧 farm-gate 容器，不是 Vite 路径。

### 只能买胡萝卜

新目录在 player-svr 裁决。重建 H5 不够，必须重新宿主机构建并替换 player-svr。

### 页面不是账号密码

检查 Vite 进程。`dev:staging` 是固定测试身份，`dev:web` 才是账号密码。

### 登录后约 30 秒命令失效

检查 Gate 日志是否有：

```text
session route lease renew failed
NOSCRIPT
```

当前 `a32a0f2` 已修复。若复现，先确认运行镜像包含该提交。

### 好友列表只有一边更新

检查：

- 接受分享是否写入双向 relation；
- FriendRosterChanged 是否进入正确 Gate writer；
- HTTP 好友列表是否带当前 Cookie；
- 浏览器是否是两个独立 Profile/Context；
- 同一 Profile 的 Cookie 是否被另一账号覆盖。

### 好友农场不能返回自己农场

Gate 现在保存 current farm/subscription，H5 顶部玩家卡和农场导航应重新 Join own farm。
若旧 Presence 仍占位，检查是否运行 `2830520` 之后的 Gate。

## 13. 当前不应宣称完成的内容

### 平台与存储

- Redis 真实主从切换；
- ACK 后主故障；
- 备份恢复；
- RPO/RTO；
- 禁止 eviction 的正式平台确认。

### 接入

- 正式微信认证；
- Cookie KeyRing 的正式 Secret 托管和真实多 Gate 滚动轮换演练
  （代码、环境合同、轮换/退役测试已完成）；
- PRE_AUTH 监听型 WebSocket 整包复跑
  （控制器、双 Transport 接线、配置与非 listener 单测已完成）；
- 找回密码产品入口
  （HTTP change-password / admin disable + H5「更多」面板改密/全端退出已完成）；
- 微信 `/auth/exchange` 真实联调（已排除出 v0.3 Web P0）；
- CLB/Ingress WSS；
- 真实多 Gate GateControl 的动态服务发现与跨节点网络故障注入；
- TCONND BY_WS 端到端。

### 分布式 Actor

- 动态路由控制面；
- Actor 在线迁移；
- 多 Region；
- 热点 Farm 自动拆分。

### 产品

- 宠物；
- 邮件；
- 完整图鉴奖励；
- 复杂任务；
- 排行榜；
- 运营活动；
- GM/管理后台。

### 容量

- 3000 万 DAU 生产压测；
- 1k+ 长连接持续业务 QPS；
- 热门 Farm 200 人真实扇出；
- 慢客户端矩阵；
- 跨可用区故障。

## 14. 下一步建议

### 第一阶段：收口 v0.3 接入合同

1. ~~切 H5 `dev:web`，重跑账号注册/登录/重连/顶号 E2E~~（2026-07-30 完成）；
2. ~~全设备退出 / 修改密码 / admin 禁用~~（无条件 Revoke；H5 UI / 找回密码仍待）；
3. ~~Redis 登录限流代码：账号 + IP + device~~（2026-07-31 公司 Redis live probe PASS）；
4. ~~PRE_AUTH budget~~（本地完成；监听型 WS 整包测试待可监听环境）；
5. ~~加固后 `./scripts/test-dualgate-kick` 公司 Redis 复跑~~（2026-07-31 PASS）；
6. ~~Join Queue Full / 版本空洞 / Gate boot fence 故障注入~~
   （farm 路径单测完成；真实滚动发布与 RESYNC_REQUIRED 推送待确认）；
7. ~~修改密码 / 禁用账号入口（无条件 Revoke）~~；~~微信 `/auth/exchange` 排除出 Web P0~~。

### 第二阶段：产品完善

1. FriendOpportunity 真实索引；
2. 多作物全浏览器回归；
3. 任务/图鉴完成度和奖励；
4. 邮件作为 Workflow 补偿出口；
5. 宠物最小纵切。

### 第三阶段：平台与容量

1. Redis 维护窗口切主和备份恢复；
2. 动态 ActorRoute 控制面；
3. CLB WSS 与正式域名；
4. 1k+ 连接、持续 QPS、热门 Farm、慢连接；
5. 形成 3000 万 DAU 容量模型与压测报告。

## 15. 接手 Agent 的第一天清单

```text
[ ] 阅读本文件和 v03-access-session-handoff.md
[ ] 确认两个仓库分支/HEAD
[ ] 确认工作区没有用户未提交修改
[ ] 确认当前 Vite mode
[ ] prepare-local-compose + compose health
[ ] go test ./...
[ ] npm test + build:web
[ ] 用两个隔离浏览器账号完成登录/好友/拜访/返回
[ ] 记录任何运行态与源码不一致时的镜像/二进制时间
[ ] 只根据新证据更新“完成”结论
```

## 16. 安全注意

- 不在文档、命令输出、提交或聊天中暴露公司 Redis 密码；
- 不提交 `deploy/secrets/redis.conf.local`；
- 不记录明文密码、Cookie 或 Ticket；
- 不让 H5 自报 player_id 决定身份；
- 不把分享 URL 等同于好友授权；
- 不用同一浏览器 Profile 冒充两个独立账号；
- 不执行破坏性 Redis 清库；
- 实时探针只使用隔离 key，并在测试结束清理。
