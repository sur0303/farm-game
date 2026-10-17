# 经典农场服务端交接单

> **历史归档（2026-08-01）**：服务端已在同级 `farm-game` 落地，现行联调方式以 `SERVER-INTEGRATION.md` 和两仓源码为准。本文中“只能继续本地模拟”的内容不再代表当前状态。

服务端准备好后，把下面的信息一次性提供给客户端联调。没有这些信息
时，客户端只能继续使用 Mock，不能把测试地址猜成生产地址。

## 1. 环境地址

```text
环境名：staging / production
H5_ORIGIN：https://...
API_ORIGIN：https://...
WS_URL：wss://.../ws
```

如果 H5、API 和 WSS 使用同一个域名，也请明确写出完整的三个地址。

## 2. 认证接口

### Staging 浏览器登录

```http
POST {API_ORIGIN}/api/v1/dev/session
Content-Type: application/json
```

返回字段至少包括：

```json
{
  "value": "one-time-connection-ticket",
  "playerId": "player-a",
  "sessionEpoch": "epoch-1",
  "environment": "staging"
}
```

### 微信小程序登录

小程序壳调用：

```http
POST {API_ORIGIN}/api/v1/auth/wx-miniprogram-login
{ "code": "wx-login-code" }
```

返回：

```json
{ "launch_ticket": "short-lived-one-time-ticket" }
```

H5 再调用：

```http
POST {API_ORIGIN}/api/v1/auth/exchange
{ "launchTicket": "short-lived-one-time-ticket" }
```

返回格式与 `ConnectionTicket` 相同。`AppSecret` 和 `session_key` 不得出现在
H5 响应、URL、日志或客户端构建产物中。

首次 exchange 成功时请设置短期 `Secure; HttpOnly` 会话 Cookie。客户端在发起 exchange
前会清理地址栏中的一次性 `launch_ticket`，因此网络超时或异常响应后不会重放旧票据；用户需要由壳层重新发起登录。
WSS 断线重连不能重复使用已经消费的 `launch_ticket`，客户端后续会调用：

```http
POST {API_ORIGIN}/api/v1/auth/renew
```

请求携带该 Cookie，响应仍返回 `ConnectionTicket`。该接口必须支持幂等重试、
校验会话和环境，并且不能把长期平台 Token 返回给 H5 JavaScript。

## 3. Proto 交接

当前发现两份服务端来源：Gongfeng `developer_026/farm-game` 项目（项目 ID
`1862451`）和 2026-07-26 提供的 `farm-game-master.zip`。zip 的设计文档已写到
v0.2/S2 目标合同，但 `api/proto/`、`api/gen/`、Go codec 和可执行入口仍是
v0.1/验证代码；没有可运行 Gateway/Logic/Worker。请服务端确认最终实际 Staging
合同，不能只回复设计仓库或 zip 地址。

请提供：

- 最终 `.proto` 文件的仓库路径和 commit/tag
- 是否采用 v0.2 目标的单一 `FarmFrame(protocol_version, kind, connection_seq, body)`；不要把旧 v0.1 的 `total_size + meta_size + FrameMeta + Body` 当成最终合同
- `connection_ticket` 在 v0.2 `ClientHello` 中核销的最终字段编号、错误码和已生成/实际运行版本
- `ClientCommandEnvelope.payload` 的各个 P0a 命令具体 message，以及 Snapshot/Change/Result payload 的具体 message
- `request_id`、`operation_id`、`connection_seq` 和 `CommandStatus.ACCEPTED` 的最终语义
- `protocol_version` 的正式值
- TypeScript 代码生成命令和生成器版本
- Go 服务端使用的生成器版本
- 是否启用压缩、WebSocket 子协议或额外帧头
- `uint64` 在 TypeScript 生成代码中的约定；业务层固定转成十进制字符串或 BigInt，禁止通过 JavaScript `Number`
- `operation_id` 的正式长度和编码；`player_id/owner_id/target_owner_id` 按 v0.2 `uint64` 交付，当前 H5 Mock 的字符串 Owner ID 只作为本地 fixture

在正式 Proto 到位后，替换 `src/protocol/codec.ts` 的临时代码，并保留现有
codec、状态机和端到端测试作为回归基线。

## 4. 第一轮联调数据

至少准备一个测试账号，并保证：

- 有一个初始空地块
- 可以购买种子、播种、收获和出售
- 有一个可访问的好友农场
- 有一个已经成熟的偷菜目标
- 能返回 `PROCESSING` 后再返回 `SUCCEEDED`
- 可以用同一个 `operation_id` 查询最终结果
- 可以人为制造一次 `farm_version` gap

每个测试账号请附带：`player_id`、可用环境、重置方式和预期初始快照。


## 5. 联调验收顺序

```text
认证 → ClientHello/ServerHello → JoinFarm/Snapshot
→ BuyOffer → PlantCrop → HarvestCrop → SellItem
→ WaterCrop → BeginTheft/Processing
→ 断线 → 原 operation_id 查询 → 最终状态
→ farm_version gap → 重新 JoinFarm
```

只有这条链路通过后，才进入微信开发者工具真机验证、域名配置和发布审核。

## 6. 分享和好友关系交接

H5 已经准备 `ShareLinkCreator` 和 `ShareIntentResolver` 两个替换边界。服务端准备好后，请补充：

- 创建分享的最终 HTTPS 路径、请求字段和返回的短期 `share_token`；
- 分享卡片标题、封面、微信小程序分享路径，以及独立 H5 分享 URL 的生成规则；
- `/v1/share/resolve` 的最终响应字段：邀请人公开摘要、过期状态和是否需要登录；
- 接受分享的最终 HTTPS 路径或 WSS 命令，以及成功后如何绑定好友关系和 `FriendPermit`；
- token 的过期、撤销、自邀、重复接受和同一分享者/接受者幂等键；
- 无效、过期、已接受、已撤销、频率限制和服务不可用的稳定错误码；
- 好友 Snapshot 的公开字段白名单，不能包含 Wallet、完整 Inventory 或内部结算信息。

客户端验收顺序为：

```text
CreateShareLink
→ 生成短期 share_token
→ 好友打开链接
→ ResolveShare（可匿名预览或登录后预览，按最终合同）
→ 登录
→ AcceptShare（幂等建立 Friendship/FriendPermit）
→ JoinFarm
→ WaterCrop / BeginTheft 按服务端权限裁决
```

## 7. 服务端可联调交付包

为避免客户端只能“猜接口”，服务端交付时请一次性提供以下材料：

### 7.1 协议冻结材料

- 正式 `.proto` 的 commit/tag；
- WSS 首帧、鉴权位置、消息名、字段编号和 `int64/uint64` 客户端表示；
- 错误码、超时、重连、`operation_id` 幂等规则；
- TypeScript/Go 生成工具版本；
- ClientHello、ServerHello、JoinFarm、CommandResult、Snapshot、FarmChange golden vectors。

### 7.2 环境路径对照

- 独立 H5 生产登录：逻辑路径 `/v1/session/login`；配置 `VITE_API_BASE=/api` 时实际为 `/api/v1/session/login`；
- WSS ticket 续发：`/v1/session/refresh-ticket`；
- Staging 开发登录：`/v1/dev/session`，仅限测试环境；
- 微信壳交换：`/v1/auth/exchange`，只接收短期 `launch_ticket`。

请同时给出 Cookie、CORS、WebSocket `Origin`、TLS、超时和稳定错误码合同。

### 7.3 阶段验收门槛

- G0：协议、测试域名、测试账号、重置方式、健康检查；
- G1：登录、续票、WSS 鉴权、JoinFarm、Snapshot；
- G2：个人经营、断线恢复和原 `operation_id` 查询；
- G3：分享、好友列表、AcceptShare、FriendPermit、好友 Join；
- G4：WaterCrop、BeginTheft/Processing、跨 Owner 结算；
- G5：真机验收、监控告警、限流封禁、迁移回滚、隐私删除和降级演练。

每个门槛都必须附可重复执行的验收脚本和失败时的重置方式。服务端只有设计文档、没有可运行 Gateway/Logic/Worker 时，客户端只能保持 Mock/待接入状态。

## 8. Linux / Go / Docker 运行交接

服务端运行环境约定为 Linux，业务服务使用 Go 实现，测试和生产可以通过 Docker
部署。客户端不依赖容器内部地址，只使用交接包中的公网 HTTPS/WSS 入口。

请同时提供：

- Linux 架构（例如 `linux/amd64`）和 Go/toolchain 版本；
- Gateway、Logic、Worker 的镜像名、commit/tag 或 digest；
- 容器监听端口、内部服务名和是否经过 TCONND BY_WS/WSS 或其他接入层；
- H5/API/WSS 反向代理或 Ingress 配置，确认 `Upgrade`、`Connection`、Origin、超时和最大消息大小；
- `healthz`、`readyz`、优雅退出和 WSS drain 行为；
- TcaplusDB、Redis 等外部依赖的连接方式，明确哪些服务不能暴露公网；
- Secret 注入方式，确认 AppSecret、session_key、数据库密码和签名密钥不进入镜像、H5 或日志；
- 结构化日志、指标、trace_id 传递方式，以及容器重启、迁移和回滚流程。

推荐拓扑：

```text
H5 / 微信 WebView
  → HTTPS/WSS Ingress:443
  → /api/*: Go farm-gate
  → /ws: TCONND BY_WS/WSS → gateproxy Unix gRPC GateStream → Go farm-gate
  → Go farm-logic / farm-worker（容器内网）
  → TcaplusDB / Redis（受控依赖）
```

Docker 只改变进程交付方式，不改变 Gateway、Actor、Worker、`FriendPermit`、
`operation_id` 和 TcaplusDB 的权威边界。服务端必须先在 Linux/Docker Staging
跑通 G0/G1，H5 才开始替换 `canonical-pending` 适配器。
