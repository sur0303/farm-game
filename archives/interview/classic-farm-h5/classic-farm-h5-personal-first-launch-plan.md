# 经典农场：个人主体优先的独立 H5 首发方案

> **历史说明（2026-08-01）**：本文保留早期阶段判断。仓库现已删除可运行的本地模拟产品环境，所有现行入口只连接真实 farm-game 后端。

版本：v1.0  
更新时间：2026-07-25  
状态：首发路线建议

关联文档：

- [H5 电脑端开发实施方案](./classic-farm-h5-desktop-development-plan.md)
- [H5 服务端接入清单](./h5/SERVER-INTEGRATION.md)
- [H5 服务端交接单](./h5/SERVER-HANDOFF-REQUEST.md)
- [H5 接入微信小程序实施方案](./classic-farm-h5-wechat-miniprogram-deployment.md)

## 1. 先给结论

如果目前只能申请个人主体，第一版不把小程序注册和 H5 上线绑定在一起：

~~~
独立 HTTPS H5
  → 浏览器登录
  → 服务端创建游戏身份
  → WSS 进入自己的农场
  → 分享链接
  → 游戏好友关系
  → 好友访问、浇水、偷菜
~~~

这条路线不需要小程序账号，也不需要微信支付。链接可以复制到微信、QQ 或普通浏览器中打开。

但“独立 H5”不等于“没有发布条件”：仍需要公网 HTTPS、可稳定访问的域名、服务端和基础反滥用策略。把链接发到微信后能否顺利打开，还取决于域名可访问性、浏览器环境和平台风控，不能把它当成小程序审核能力的替代品。

之后再单独验证个人主体的微信能力：

~~~
个人主体后台允许 web-view
  → 复用现有 H5，接入微信小程序壳

个人主体后台不允许 web-view
  → 保持独立 H5
  → 如果必须进入微信，再另做原生小游戏客户端
~~~

不能把独立 H5、微信小程序 web-view 和原生微信小游戏当成同一件事。它们可以共用服务端和游戏规则，但运行时和发布边界不同。

## 2. “好友系统”到底要做什么

需要做的是游戏内部好友关系，不是微信通讯录：

- 不读取微信好友列表；
- 不添加微信联系人；
- 不要求用户绑定或授权全部通讯录；
- 不需要微信支付；
- 不把 owner_id 放进 URL 后就直接授予访问权。

最小可玩的关系可以采用“分享即建立游戏好友关系”：

~~~
A 点击分享
  → 服务端创建短期 share_token
  → B 打开链接
  → B 登录
  → 服务端解析并接受分享
  → 建立唯一 Friendship
  → B 获得访问 A 农场的 FriendPermit
~~~

第一版甚至不必先做复杂的好友搜索、备注、黑名单和申请列表。先实现：

1. 我的账号；
2. 我的分享链接；
3. 分享链接落地；
4. 游戏好友关系；
5. 好友农场访问；
6. 浇水和偷菜权限；
7. 取消关系或撤销访问。

即使不做完整好友列表，也必须有最小的反滥用能力：分享创建限频、token 过期、重复接受幂等、自邀拒绝、撤销关系和举报/封禁入口。否则“分享即加好友”很容易变成骚扰入口。

## 3. 最小服务端数据

权威数据必须在服务端，浏览器只保存会话和最小未决操作日志。

### 3.1 玩家身份

~~~
Player
  player_id
  login_provider
  provider_subject_hash
  created_at
  status
~~~

独立 H5 可以先使用邮箱、手机号、用户名密码或受控测试账号。无论采用哪种登录方式，都必须由服务端建立稳定 player_id，不能把浏览器指纹当成长期身份。

### 3.2 分享链接

~~~
ShareLink
  share_token_hash
  inviter_player_id
  purpose
  expires_at
  revoked_at
  created_at
~~~

token 建议使用随机不可猜值，数据库保存哈希或等价不可逆形式。URL 不放长期会话 Token、AppSecret、session_key 或可伪造的邀请人 ID。

### 3.3 游戏好友关系

~~~
Friendship
  friendship_id
  player_a_id
  player_b_id
  status
  source_share_id
  created_at
  updated_at
~~~

同一对玩家只能有一个有效关系。接受分享必须使用稳定幂等键，重复点击不能重复建关系、发奖励或创建多份权限。

### 3.4 权限

FriendPermit 是服务端内部权限上下文，不下发给 H5，也不允许 H5 自己伪造。它至少要绑定：

- 登录玩家；
- 目标农场 Owner；
- Friendship 版本；
- 当前连接或 Presence；
- 允许的动作；
- 过期或撤销状态。

## 4. 最小 HTTPS API

实际路径以服务端最终合同为准，下面是首发所需的逻辑接口。

### 登录

~~~
POST /v1/session/login
POST /v1/session/refresh-ticket
~~~

登录成功返回短期 connection_ticket，并建立受保护的 HTTPS 会话。浏览器不能持有长期平台密钥。

### 分享

~~~
POST /v1/share/create
POST /v1/share/resolve
POST /v1/share/accept
POST /v1/share/revoke
~~~

语义要求：

- create 只创建分享事实，不直接建立好友关系；
- resolve 可以返回邀请人公开摘要、过期状态和是否需要登录；
- accept 在登录身份下幂等建立 Friendship；
- 自己接受自己的链接必须拒绝；
- 过期、撤销和伪造 token 必须有稳定错误码；
- accept 成功后返回目标 Owner 的公开摘要或可安全 Join 的上下文；
- H5 不提交 FriendPermit。

### 游戏通信

~~~
HTTPS 登录
  → 一次性 connection_ticket
  → WSS 鉴权
  → JoinFarm
  → Buy / Plant / Harvest / Sell
  → WaterCrop / BeginTheft
~~~

好友访问、浇水和偷菜都必须再次由 Gateway/Logic/Actor 校验，不能只相信之前一次 accept 的结果。

## 5. 客户端当前状态和服务端缺口

当前 H5 已经具备：

- Mock 登录入口；
- Staging/WeChat 登录适配边界；
- WSS、农场 Snapshot、FarmChange 和重连状态机；
- 分享链接创建边界；
- share_token 暂存和地址栏清理；
- 分享预览与接受好友关系的 Mock 适配器；
- 好友农场落地；
- 微信壳对 share_token 的透传；
- 非 Mock 构建不会带入 Mock 分享 fixture。

当前服务端仍缺：

- 可访问的 HTTPS 登录接口；
- 可核销的 WSS connection ticket；
- canonical Proto 的完整命令和 Snapshot payload；
- 分享创建、解析和接受接口；
- Friendship 持久化；
- FriendPermit 校验；
- 至少两个可重置的测试账号；
- 可联调的 Gateway/Logic/Worker。

因此客户端下一次真实替换点是：

~~~
MockAuthProvider
  → Staging/正式 AuthProvider

MockShareLinkCreator
  → HTTPS create-share adapter

MockShareIntentResolver
  → HTTPS resolve/accept adapter

FakeFarmServer
  → canonical Proto + 真实 WSS Gateway
~~~

农场页面、Store、Pending 操作和业务按钮不应因这次替换而重写。

## 6. 独立 H5 首发阶段

### 阶段 A：可公开访问

- 准备一个手机浏览器可访问的 HTTPS 域名；
- 静态 H5 使用 CDN 或静态托管；
- API 与 WSS 使用有效 TLS；
- 配置 CORS、WebSocket Upgrade、Origin 校验；
- 不依赖小程序业务域名；
- 不把测试登录接口暴露到生产。

通过标准：手机浏览器直接打开 H5，不经过小程序也能看到登录页。

### 阶段 B：单用户经营

- 登录并恢复稳定玩家身份；
- 初始化 Owner；
- 购买、播种、收获、出售；
- 刷新和断线后资产不丢；
- 相同 operation_id 重试不会重复扣款。

### 阶段 C：游戏好友

- A 生成分享链接；
- B 使用浏览器打开；
- B 登录后接受分享；
- B 看到 A 的公开农场；
- B 可以浇水；
- B 可以偷取成熟作物；
- A 撤销关系后，B 的后续 Join 和操作被拒绝；
- 重复点击链接不重复建立关系。
- 分享创建和接受有频率限制，异常账号可以被封禁；
- 玩家可以撤销或屏蔽关系，不能继续访问已撤销的农场；
- 好友视图只展示公开农场状态，不展示金币、完整仓库和私有任务。

### 阶段 D：公开小规模测试

先邀请少量真实用户测试，不直接以大规模并发作为第一发布门槛。重点观察：

- 登录成功率；
- 分享链接打开率；
- 分享接受成功率；
- 好友 Join 成功率；
- NOT_FRIEND 和 FRIEND_PERMIT_INVALID；
- WSS 重连率；
- PROCESSING 偷菜最终收敛率；
- 误授权和越权日志。

## 7. 微信能力的后续分支

独立 H5 稳定后再验证个人主体：

### 分支 1：个人主体可以承载当前 H5

继续使用现有 wechat-shell：

~~~
wx.login
  → 服务端换 launch_ticket
  → H5 exchange
  → WSS
~~~

分享卡片由小程序壳承接，H5 只把短期分享数据交给壳层；好友关系和农场权限仍然走同一套服务端。

### 分支 2：个人主体不能承载当前 H5

独立 H5 继续作为正式网页版本。如果必须让用户从微信内使用，则另做原生小游戏：

- 复用 player_id、Friendship、FriendPermit；
- 复用 HTTPS 登录、分享和 WSS 业务合同；
- 重新实现 Canvas/小游戏渲染和输入；
- 不复制一份新的农场权威逻辑；
- 不尝试通过代理、跳转或未授权域名绕过平台限制。

## 8. 发布前硬门禁

以下任一项未通过，都不能对外宣称“好友偷菜已上线”：

- H5 手机浏览器能完成真实登录；
- 分享 token 不可伪造且可过期；
- 接受分享是服务端幂等操作；
- 未接受分享的玩家无法 Join 目标农场；
- 好友 Snapshot 不泄露 Wallet 和完整 Inventory；
- 浇水、偷菜由服务端再次鉴权；
- 断线后 operation_id 可查询；
- WSS 使用正式 Proto 和 golden vectors；
- API、WSS、数据库和日志中不记录长期平台密钥；
- 有撤销关系和异常账号处理办法；
- 有隐私政策、用户反馈和数据删除/注销方案。

## 9. 当前推荐执行顺序

~~~
现在
  → 继续 Mock 和客户端验收
  → 把服务端交接单交给后端

服务端开始
  → 先实现真实登录 + JoinFarm
  → 再实现单用户经营
  → 再实现 share create/resolve/accept
  → 再实现 FriendPermit + 好友 Join
  → 最后接 WaterCrop / BeginTheft

H5 Staging 通过
  → 独立 H5 小规模发布
  → 再验证个人主体 web-view
  → 决定是否启动原生小游戏分支
~~~

这条顺序保证：即使个人小程序能力最终不可用，已经完成的 H5、服务端和游戏好友系统仍然可以直接使用，不会全部返工。

## 10. Grill-me 审查后的修订门槛

本方案已经按审查意见补充以下上线前置条件。它们不是“以后再优化”的事项，而是服务端交付和发布验收的硬门槛。

### 10.1 协议冻结包

服务端进入真实联调前，必须提供一个可追溯的协议冻结包：

- 正式 `.proto` 的 commit/tag；
- 若采用 `farm-game` v0.2 目标合同，必须确认单一 `FarmFrame(protocol_version, kind, connection_seq, body)` 已由 Go 代码实际实现；zip 中的 v0.1 `FrameMeta`/旧 codec 只能作为历史基线，不能直接接入 H5；
- WSS 首帧、鉴权位置、`ClientHello`/`ServerHello` 和业务消息名；
- 所有字段类型、编号、`int64/uint64` 的 JavaScript 表示；
- 错误码、超时、重连和 `operation_id` 幂等语义；
- 生成 TypeScript/Go 代码的工具版本；
- 至少一组 ClientHello、ServerHello、JoinFarm、CommandResult、Snapshot 和 FarmChange golden vectors。

没有这份冻结包，H5 只能继续使用 Mock，不能宣称“已接入真实服务端”。

### 10.2 环境和登录路径统一

文档中出现的路径按下面规则区分：

| 场景 | 逻辑接口 | 说明 |
|---|---|---|
| 独立 H5 生产登录 | `/v1/session/login` | 在配置了 `VITE_API_BASE=/api` 时，实际 URL 为 `/api/v1/session/login` |
| WSS ticket 续发 | `/v1/session/refresh-ticket` | 不能复活旧 session，也不能代替写操作 Permit |
| Staging 开发登录 | `/v1/dev/session` | 仅测试环境，不得暴露到生产 |
| 微信壳登录交换 | `/v1/auth/exchange` | 只接收短期 `launch_ticket` |

服务端交付时还必须同时给出 Cookie、CORS、WebSocket `Origin`、TLS、超时和错误码合同。

### 10.3 分享接口必须可执行

服务端不能只说“支持分享”，必须冻结 create/resolve/accept 的路径、请求 JSON、响应 JSON、匿名 resolve 规则、登录要求、状态机和错误码。至少要覆盖：

- 无效、过期、撤销、自邀、重复接受、频率限制和服务不可用；
- 同一分享者/接受者的幂等键；
- AcceptShare 成功后返回的目标 `owner_id`、好友摘要和关系版本；
- `FriendPermit` 如何绑定 Join 与 WaterCrop/BeginTheft；
- 好友公开 Snapshot 的字段白名单。

Resolve 只能预览，不能建立好友或发放奖励。

### 10.4 分阶段服务端交付门槛

- G0：协议冻结包、测试域名、测试账号、重置方式和健康检查；
- G1：登录、续票、WSS 鉴权、JoinFarm、Snapshot；
- G2：购买、播种、收获、出售、断线后按原 `operation_id` 查询；
- G3：分享、好友列表、AcceptShare、FriendPermit、好友 Join；
- G4：WaterCrop、BeginTheft/Processing、跨 Owner 结算；
- G5：真机验收、监控告警、限流封禁、迁移回滚和隐私删除演练。

每一阶段都必须有可重复执行的验收脚本，不能只凭“接口已经部署”进入下一阶段。

### 10.5 生产运维门槛

正式发布前还必须确认 HTTPS/WSS 域名和证书、健康检查、监控告警、限流与封禁、数据库迁移和回滚、隐私删除/注销、服务不可用时的降级提示，以及不记录长期平台密钥的日志策略。
