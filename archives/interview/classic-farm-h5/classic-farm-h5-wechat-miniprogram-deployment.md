# 经典农场 H5 接入微信小程序实施方案

> **历史说明（2026-08-01）**：文中早期本地模拟链路已移除。当前微信承载构建与 Web/Staging 一样，只连接真实 farm-game HTTPS/WSS；涉及旧目录或电脑模拟启动的段落仅供追溯。

版本：v0.1  
更新时间：2026-07-25  
状态：部署路线与验收基线。H5 客户端和最小微信壳已完成，正式服务端、域名、真机和发布验收仍未完成。

配套通信合同：[《经典农场小游戏》P0 通信合同](./classic-farm-communication-contract.md)。

## 1. 先确定产品形态

当前项目的客户端约束是 H5。对应的微信落地形态应为：

```text
普通微信小程序
  └── web-view
        └── HTTPS 托管的农场 H5
              ├── HTTPS 登录/查询
              └── WSS 游戏命令/实时同步
```

这和“微信小游戏包”是两条路线：

| 路线 | 客户端形态 | 是否适合当前项目 |
|---|---|---|
| 小程序 web-view | H5 继续运行在网页容器中 | 是，保持当前交接文档的 H5 约束 |
| 微信小游戏包 | 改造 H5 运行时、工程配置和微信 API | 否，除非以后决定迁移到小游戏运行时 |

因此本方案当前不把 H5 改写成原生小游戏，也不把 `game.json` 当成第一步产物；如果个人主体实测不支持 `web-view`，则按后文决策树另立原生小游戏客户端分支，复用业务层但不伪称为 H5 承载。

## 2. 现有交接文档如何映射到微信

交接文档已经冻结：

- H5 静态资源：HTTPS + CDN；
- 登录和分享落地：HTTPS API；
- 登录后的命令、快照和实时变化：WSS；
- Gateway 负责连接，不拥有农场权威事实；
- `PlayerFarmActor(owner_id)` 串行裁决；
- TcaplusDB 是权威业务存储，Redis 只能保存可重建状态；
- H5 重连后用原 `operation_id` 查询，并重新 `JoinFarm` 获取快照。

微信只新增一个“平台壳”：

```text
小程序壳
  ├── wx.login()：取得临时 code
  ├── wx.request()：把 code 交给 farm-gate
  ├── web-view：打开 H5
  ├── onShareAppMessage：负责微信分享入口
  └── wx.miniProgram：H5 与小程序页面之间的有限桥接
```

H5 仍然通过浏览器的 `fetch`、`WebSocket`、`ArrayBuffer` 和 `localStorage` 工作；金币、地块、好友和偷菜结果仍由现有后端裁决。

## 3. 上线前的准入门槛

### 3.1 小程序账号主体

先在微信公众平台确认账号主体、类目和后台是否出现“业务域名”配置项。个人主体并非绝对不能注册小程序，但本项目采用 `web-view` 承载 H5，业务域名和后续商业化能力受主体限制；因此正式发布优先选择企业或机构主体。若后台没有业务域名能力，需要先解决账号主体/资质，不能靠代码绕过。开发阶段不需要先注册账号，电脑端继续使用 Mock H5。

#### 个人主体下的决策树

如果当前只能申请个人主体，先不要把“好友偷菜不收费”误认为可以绕过承载限制。好友访问、浇水和偷菜可以由服务端基于登录身份、分享链接/邀请码和 `FriendPermit` 实现，不需要读取微信好友列表，也不依赖微信支付；但这不能改变个人主体对任意 H5 `web-view` 的平台限制。

个人主体只按下面顺序选择：

1. **先验证后台能力**：在个人主体的小程序后台确认是否能添加业务域名、是否能使用 `web-view`。以当前官方后台实际显示为准，不以开发者工具中的模拟结果为准。
2. **如果支持 `web-view`**：继续当前 H5 方案，只需把 `wechat-shell` 的域名、登录接口和 `launch_ticket` 换成正式配置，并完成真机验收。
3. **如果不支持 `web-view`**：不寻找代理域名、跳转页或其他规避方式。保留当前 H5 作为电脑/手机浏览器版本，同时另做个人主体可发布的原生小游戏客户端；农场规则、服务端协议、好友权限和 `operation_id` 可以复用，但当前 DOM/CSS 画面不能直接复用，需要改为小游戏 Canvas/渲染和输入适配。
4. **如果既不支持 `web-view`，又暂时不做原生小游戏迁移**：微信端先不发布，继续发布独立 H5；这不会阻塞服务端和农场业务开发。

因此，个人主体下不存在“完全不改代码就把当前 H5 放进小程序”的可靠方案；最小风险路径是先验证后台能力，再决定保留 H5 承载还是增加原生小游戏渲染层。

### 3.2 域名和证书

至少准备一个已经备案、可使用 HTTPS 的业务域名，例如：

```text
https://farm.example.com       H5 页面、HTTPS API
wss://farm.example.com/ws      WSS 长连接
```

生产环境使用受信任证书和 443 端口。建议 H5、API、WSS 共用一个公开域名，通过反向代理按路径转发，减少跨域和微信域名配置数量。

### 3.3 微信身份资料

需要取得小程序 AppID 和 AppSecret。AppSecret 只能放在服务端配置中，不能进入 H5、URL、日志或客户端构建产物。

## 4. 推荐的登录链路

不要把 `session_key` 或 AppSecret 交给 H5。推荐使用一次性登录票据：

```text
小程序壳 wx.login()
  → code
  → farm-gate /api/v1/auth/wx-miniprogram-login
  → 服务端调用 code2Session
  → openid + session_key 只留在服务端
  → 创建 active_session_epoch
  → 返回一次性 launch_ticket
  → （仅在账号支持 web-view 时）打开 H5?launch_ticket=...
  → H5 在发起 /api/v1/auth/exchange 前清除URL中的launch_ticket
  → 服务端设置 HttpOnly 会话 Cookie
  → 返回一次性 connection_ticket + WSS 地址
  → H5 建立 WSS
  → 第一条ClientHello携带connection_ticket
  → Gateway核销ticket并返回ServerHello
  → WSS断线后 H5 通过 HttpOnly 会话调用 /api/v1/auth/renew 获取新 connection_ticket
```

票据要求：

- 一次性消费；
- 短时有效；
- 绑定小程序身份、设备/会话和环境；
- 不能直接包含 openid、session_key 或可长期使用的 token；
- 发起 exchange 前从 URL 中清除，避免网络超时或异常响应后重放一次性票据；兑换成功依赖服务端会话 Cookie，而不是地址栏票据。
- `launch_ticket` 只用于小程序壳向 H5 交接身份，`connection_ticket` 只用于认证一条 WSS，二者不能互相替代；
- 如 H5 还需要已认证 HTTPS API，优先由 exchange 设置 `Secure + HttpOnly` 会话 Cookie，不能把长期平台 Token 暴露给 H5 JavaScript。
- WSS 重连不能重复兑换已经消费的 `launch_ticket`；服务端必须提供 `/api/v1/auth/renew`，并为每次新的 ClientHello 核销新的 `connection_ticket`。

服务端登录成功后继续执行交接文档中的 `EnsureOwnerInitialized(bootstrap_id)`，不能在小程序壳或 H5 中初始化金币、仓库和土地。

## 5. 小程序壳的最小工程

需要新建一个极薄的小程序工程，只承担平台能力：

```text
farm-miniprogram-shell/
  app.js
  app.json
  pages/
    game/
      game.js
      game.wxml
      game.json
    share/
      share.js
      share.wxml
```

它遵循微信小程序开发框架的标准分层：`app.json` 声明页面，`app.js` 管理小程序级生命周期，各页面通过 `Page` 注册并管理自己的生命周期；真正的游戏画面仍由 `web-view` 中的 H5 绘制。官方框架入口：[微信小程序开发框架](https://developers.weixin.qq.com/miniprogram/dev/framework/)。

### 5.1 官方框架核对结论

我按官方框架目录核对了本项目会用到的结构、配置、App Service、页面路由、生命周期、网络、开放能力、`web-view`、分包、性能和调试相关章节。对农场项目真正有约束力的结论是：

| 框架规则 | 对农场项目的影响 |
|---|---|
| 根目录由 `app.js`、`app.json` 组成，页面通常由同名的 `.js`、`.wxml`、`.json`、`.wxss` 文件组成 | 小程序壳只保留 `game` 和 `share` 两个薄页面，不把 H5 源码混进壳工程 |
| `app.js` 使用 `App()` 注册，页面使用 `Page()` 注册；页面由框架管理生命周期 | `onShow` 只能做恢复/状态检查，不能在每次前台切换时重新建档或发奖励 |
| 路由由小程序框架管理；`navigateTo` 入栈、`redirectTo` 替换当前页、`navigateBack` 返回、`reLaunch` 重建入口 | H5 的页面跳转必须通过统一适配层调用 `wx.miniProgram.*`，并设置页面栈降级策略 |
| 小程序逻辑层不是浏览器环境，不能假设存在 `window`、`document` | 壳层不运行游戏渲染和浏览器网络代码；这些代码必须留在 `web-view` 的 H5 内 |
| `web-view` 自动铺满页面，网页地址需要配置业务域名；网页到壳层的通信受 JSSDK 能力限制 | 壳层只承载 H5，不在 H5 和壳之间设计任意消息通道；业务事实继续走 HTTPS/WSS 后端 |
| 网页通过 `postMessage` 发给小程序的消息只在特定时机到达，例如返回、销毁或分享 | 不能用它传输实时快照、操作确认或心跳；分享标题、封面等非实时数据才适合走这条链路 |

这里引用的可访问框架镜像是较早的 v2.21.0 文档，适合核对稳定的框架语义；版本限制、账号能力、隐私和审核规则在上线前仍必须以微信官方当前文档和公众平台后台为准。

对农场壳层的生命周期约定：

| 生命周期 | 农场用途 |
|---|---|
| `onLoad(options)` | 读取分享参数、环境参数，发起一次登录票据流程 |
| `onShow()` | 从分享页返回或小程序回到前台时检查 H5 是否需要恢复 |
| `onHide()` | 记录进入后台，不在壳层修改农场业务事实 |
| `onUnload()` | 清理壳层临时状态；H5 自己负责 WSS 关闭/重连策略 |
| `bindload` / `binderror` | 记录 H5 加载成功、域名、证书和资源错误 |

`onShow()` 可能多次触发，不能在每次触发时重复建档或重复发放业务奖励；建档和业务命令仍由服务端的 `bootstrap_id`、`operation_id` 和 Actor 规则保证幂等。

核心页面结构：

```xml
<web-view src="{{h5Url}}" bindload="onH5Load" binderror="onH5Error" />
```

`game.js` 的职责：

1. 读取分享参数和环境参数；
2. 调用 `wx.login()`；
3. 用 `wx.request()` 把 code 发给服务端；
4. 获取一次性 `launch_ticket`；
5. 拼接 H5 URL 并设置 `h5Url`；
6. 处理 H5 加载失败、返回和重新进入；
7. 配置 `onShareAppMessage` 和 `onShareTimeline`（如果产品需要且平台允许）。

小程序壳不保存金币、仓库、地块和偷菜结果，也不直接调用 TcaplusDB 或 Redis。

## 6. H5 端需要增加的平台适配层

现有 H5 不应在业务页面中散落微信判断，增加统一适配层：

```text
h5/src/platform/
  runtime.ts       浏览器 / 小程序 web-view 环境识别
  launch-ticket.ts 一次性票据兑换
  share-bridge.ts  分享和分享落地跳转
  navigation.ts    返回小程序壳或普通 H5 页面

h5/src/network/
  http-client.ts   HTTPS 请求
  ws-client.ts     WSS、心跳、重连、二进制协议
  protocol/        Protobuf 编解码
```

平台适配层只负责环境差异；`PlantCrop`、`HarvestCrop`、`JoinFarm`、`BeginTheft` 等业务命令仍走统一的 `ClientCommandEnvelope`。

H5 WSS 客户端必须满足：

- `binaryType = "arraybuffer"`；
- 建连后的第一条应用消息是 `ClientHello(connection_ticket)`，收到 `ServerHello` 前不发送业务命令；
- `operation_id` 超时重试时保持不变；
- 不把客户端时间当作成熟依据；
- 发现 `farm_version` 跳跃时重新 Join；
- 断线后先用原 operation_id 查询不确定命令，再获取快照；
- 只把服务端确认的结果写入本地展示状态。

### 6.1 SPA 路由不能只依赖 Hash

如果 H5 使用 `#/farm/123` 这类 Hash 路由，登录授权或外部重定向后，服务器收不到 `#` 后面的内容，回跳时可能落到首页。建议所有需要跨小程序页面、登录和分享传递的路由都同时保存一份 query 形式，例如：

```text
https://farm.example.com/game?_router=%23%2Ffarm%2F123
```

H5 启动时按以下顺序恢复路由：

1. 有正常 Hash 时使用 Hash；
2. Hash 缺失但有 `_router` 时解码恢复；
3. 两者都没有时进入默认农场首页。

这部分应封装为统一的 `LinkGenerator`，不要让每个业务按钮自己拼 URL。

### 6.2 小程序页面栈需要有上限策略

H5 内部跳转不能全部使用 `location.href`。在小程序环境中由适配层统一调用 `wx.miniProgram.navigateTo`，并由壳层承载目标 H5 URL。连续打开页面接近页面栈上限时，降级为 `redirectTo` 或回到固定的游戏入口，避免用户最终无法返回。

## 7. 分享和好友建立

H5 内的分享按钮不能假设自己拥有完整的小程序 `wx.*` 能力。推荐使用独立的原生分享落地页：

```text
H5 请求后端创建 share_token
  → H5 通过 wx.miniProgram.navigateTo 进入小程序 share 页面
  → share 页面配置 onShareAppMessage
  → 分享路径携带短期 share_token
  → 好友打开 share 页面
  → 小程序壳完成登录
  → 后端校验并幂等接受分享
  → 再进入 H5 的好友农场
```

分享 token 必须由后端生成并保存分享事实，不能由 H5 自己拼接好友关系或奖励。重复点击使用同一稳定接受键时，返回原结果，不重复建立关系或发奖。

不要把 `wx.miniProgram.postMessage` 当成实时 RPC。它适合把分享标题、封面和目标 URL 预先交给小程序壳，但接收时机受 web-view 生命周期和用户分享/返回动作影响。因此农场分享采用“后端创建分享数据 + 原生分享页承接”的方式，H5 只负责发起进入分享页。

农场分享卡片的推荐数据流：

```text
H5 请求后端创建 share_token
  → 后端返回标题、封面 CDN URL、分享路径
  → H5 进入小程序 share 页面
  → share 页面 onShareAppMessage 使用已准备好的数据
  → 好友打开分享路径后登录并接受分享
```

标题和封面可以预先生成并缓存，不能在用户点击分享的关键路径上依赖一次昂贵的截图或不稳定的网络请求；失败时使用固定默认封面。

## 8. 域名配置清单

在小程序后台逐项配置并实际验证：

| 配置 | 用途 | 建议值 |
|---|---|---|
| 业务域名 | `web-view` 打开 H5 | `farm.example.com` |
| request 合法域名 | 小程序壳登录和票据兑换 | `https://farm.example.com` |
| socket 合法域名 | 只有壳层使用 `wx.connectSocket` 时才需要 | `wss://farm.example.com` |
| upload/download 合法域名 | 图片、头像、附件等平台上传下载 | 按实际需要添加 |

配置业务域名时，按后台要求下载校验文件，放到域名要求的位置后再保存。H5 页面、API 和 WebSocket 入口都必须有有效 TLS 配置；本地开发只能在开发者工具的“不校验合法域名、TLS 和证书”选项下调试，不能作为线上配置。

如果使用 WOA 或其他公司内网域名，还要额外确认微信侧能访问该域名以及域名校验请求不会被统一登录拦截。KM 的内部实践显示，校验文件本身正确但被登录网关拦截时，仍可能无法完成配置；这类情况需要让域名管理员放行校验路径，并按公司流程向小程序助手申请域名放行。公网用户使用的正式环境应优先采用手机微信可直接访问的公网域名。

因为当前业务长连接由 H5 的浏览器 `WebSocket` 建立，不能只看壳层的 socket 合法域名，还要额外检查：

- H5 页面是否能直接访问 WSS 地址；
- 反向代理是否正确转发 `Upgrade` 和 `Connection` 头；
- H5/API 是否同源，若不同源是否正确配置 CORS；
- iOS 和 Android 真机是否都能保持长连接。

也就是说，`wx.connectSocket` 的合法域名配置和 H5 浏览器 `WebSocket` 的可达性是两项不同的验收项。本项目若始终让 H5 建立 WSS，壳层不需要为了这条连接调用 `wx.connectSocket`；但只要未来把连接迁移到壳层，就必须补齐该配置并重新验证协议适配。

## 9. 后端和部署拓扑

推荐先用单域名入口，内部保持现有三进程：

```text
farm.example.com/
  ├── /                  CDN 或静态托管的 H5
  ├── /api/*             HTTPS → farm-gate
  └── /ws                WSS → TCONND → gateproxy → GateStream → farm-gate

TCONND / gateproxy        公网长连接与 GateStream 传输事件
farm-gate                应用鉴权、Session、协议、限流、路由和下行背压
farm-logic               私网、Actor路由和权威业务裁决
farm-worker              私网、Outbox和跨Owner结算恢复
TcaplusDB                权威业务存储
Redis                    可重建Session、路由、限流和热缓存
```

### 9.1 Linux + Go + Docker 部署基线

真实服务端按 Linux 环境运行，业务进程使用 Go 实现，并优先通过 Docker
镜像交付。H5 不依赖容器内部地址，只依赖服务端交接的三个公网入口：

```text
H5_ORIGIN   https://farm.example.com
API_ORIGIN  https://farm.example.com
WS_URL      wss://farm.example.com/ws
```

推荐的测试/生产链路为：

```text
浏览器或微信 WebView
  → HTTPS/WSS 反向代理或 Ingress（Linux:443）
  → TCONND BY_WS/WSS
  → gateproxy Unix gRPC GateStream → Go farm-gate
  → Go farm-logic
  → Go farm-worker
  → TcaplusDB / Redis
```

容器化交付必须满足：

- Go 版本、Proto 生成器和构建参数固定在仓库或镜像构建元数据中；
- 使用 Linux multi-stage build，运行时镜像只包含二进制、CA 证书和必要配置；
- 运行容器使用非 root 用户，AppSecret、数据库凭证和签名密钥通过部署平台注入，不能写入镜像或日志；
- Gateway、Logic、Worker 之间使用容器服务名或内部 DNS，不把 `localhost` 当成跨容器地址；
- Gateway 对外只暴露 HTTPS/WSS，Logic、Worker、TcaplusDB 和 Redis 不直接暴露公网；
- 每个服务提供 `healthz` 和 `readyz`，未准备好时不能接收新连接；
- 收到 `SIGTERM` 后先停止新连接/新请求，再等待现有 WSS、Actor 和 Worker 任务完成或超时退出；
- 日志使用结构化字段串联 `trace_id`、`session_id`、`operation_id`、`owner_id` 和 `farm_version`；
- 镜像使用不可变 tag 或 digest，保留上一版本，支持静态 H5 与 Go 服务端的回滚；
- 如果先用 Docker Compose 做 Staging，Compose 只负责编排，不改变正式环境的内部权限、TLS、限流和健康检查要求；后续迁移 Kubernetes 时保持同样的服务边界。

服务端交付给 H5 时，除了公网地址，还必须说明：镜像版本/commit、Linux 架构、容器监听端口、反向代理的 WebSocket Upgrade 配置、健康检查地址、超时、最大消息大小和优雅退出时间。H5 不需要知道容器数量，也不应把 Docker 内网地址写入 `.env`。

发布 H5 静态资源时使用带哈希的 JS/CSS/图片文件和版本清单，先上传新资源，再切换入口 HTML，避免旧 H5 引用不到资源。后端协议需要保持向后兼容，因为已经打开的 web-view 可能暂时运行旧版本 H5。

CloudBase 可以作为 H5 静态托管或图片/配置服务，但不能因此改变农场权威边界。若使用 CloudBase 登录，也必须把身份转换成现有 SessionPermit，并由 `PlayerFarmActor` 继续裁决资产和地块。

## 10. 分阶段实施

### 阶段 0：平台可行性验证

- 确认小程序主体和业务域名能力；
- 申请/确认 AppID、AppSecret；
- 准备 HTTPS 域名和证书；
- 建立空壳小程序；
- 用 `web-view` 打开一个最小 H5；
- 用真机验证 iOS/Android 加载和返回。

通过标准：不是只在开发者工具中显示，至少两台真机能打开正式 HTTPS 地址。

### 阶段 1：登录纵向闭环

```text
小程序 wx.login
→ 服务端 code2Session
→ 一次性 launch_ticket
→ H5 exchange
→ SessionPermit
→ EnsureOwnerInitialized
→ H5 展示农场快照
```

通过标准：首次登录不重复建档，重新进入能恢复同一资产，AppSecret/session_key 不出服务端。

### 阶段 2：个人经营

实现 `BuyOffer`、`PlantCrop`、`HarvestCrop`、`SellItem`，验证：

- 双击播种只扣一次；
- 重复收获不重复入库；
- 客户端改时间不能提前成熟；
- 请求超时后使用相同 operation_id 查询；
- TcaplusDB 提交失败时不更新 Actor 内存、不广播成功。

### 阶段 3：好友访问和分享

实现分享 token、好友关系、好友列表和 `JoinFarm`。必须验证分享重复点击、过期 token、自己加自己、权限撤销和离线访问。

### 阶段 4：实时互动

实现 `WaterCrop`、`BeginTheft`、`GrantLoot`、`CompleteTheft`，再做两个或多个真机同时操作同一地块的测试。重点验证 `farm_version` 顺序、8 人名额、下行背压、断线重连和跨 Owner `Processing` 恢复。

### 阶段 5：提交审核和正式发布

- 补齐隐私保护说明、服务协议和用户反馈入口；
- 检查游戏内容、分享文案和功能描述一致；
- 关闭测试环境域名绕过；
- 上传正式 H5 资源和后端版本；
- 通过开发者工具上传代码；
- 使用体验版和真机回归；
- 提交审核；
- 发布后观察登录成功率、WSS 连接率、Join 成功率、`Unavailable` 比例、Outbox 积压和 TcaplusDB P99。

## 11. 当前代码产物与剩余工作

客户端和微信壳层已经在仓库中，当前实际结构如下；服务端目录仍由后续服务端实现提供：

```text
h5/
  src/app/
  src/auth/
  src/domain/
  src/mock/
  src/network/
  src/persistence/
  src/protocol/
  src/state/
  src/test/

wechat-shell/
  app.json
  app.js
  pages/game/
  pages/share/

server/（待服务端实现）
  cmd/farm-gate/
  cmd/farm-logic/
  cmd/farm-worker/
  api/proto/
```

客户端本地 Mock 链路已经验收；正式环境的第一条联调链路是：

```text
小程序壳登录
→ H5打开
→ H5兑换launch_ticket
→ HTTPS/WSS建立
→ 初始化农场
→ Join自己的农场
→ 返回明确版本的空农场Snapshot
```

只有这条链路跑通后，才开始加入播种和收获。

## 12. 最终用户使用链路

正式发布后，用户看到的是下面这条链路：

```text
管理员在微信公众平台发布小程序审核通过的版本
  → 用户搜索小程序、扫描小程序码或点击好友分享
  → 进入小程序 share/game 页面
  → 小程序壳 wx.login
  → 服务端创建/恢复游戏身份
  → web-view 打开 H5
  → H5 兑换 launch_ticket
  → WSS 建连并 JoinFarm
  → 用户开始种植、收获、访问好友和偷菜
```

好友分享链路是：

```text
A 在农场内发起分享
  → 后端创建 share_token 和分享卡片数据
  → 小程序原生分享页生成分享卡片
  → B 点击分享卡片
  → B 登录并幂等接受分享
  → B 进入 A 的农场 H5
```

### 13.1 更新边界

H5 的图片、样式和经过审核允许的前端资源可以通过 CDN 发布新版本，但不能把“热更新”当成绕过小程序审核的手段。涉及玩法、权限、支付、广告、隐私、分享文案或平台能力的变化，应重新评估审核影响。

小程序壳代码、页面路由、登录桥接和分享页发生变化时，需要重新构建、上传和发布小程序版本。H5 和后端协议必须保持向后兼容，保证旧版本壳和已经打开的 H5 不会立刻失效。

### 13.2 上线后的最低运维闭环

每次发布都应记录：

- 小程序版本号；
- H5 静态资源版本号；
- Protobuf `protocol_version`；
- 后端镜像版本；
- 配置和规则版本；
- 数据库迁移/兼容状态。

发布后观察：

- 小程序进入成功率；
- launch_ticket 兑换成功率；
- WSS 建连和重连成功率；
- `JoinFarm` 成功率；
- `Unavailable` 和 `Processing` 命令比例；
- `farm_version` 缺口率；
- TcaplusDB P99、Redis 故障和 Outbox 积压。

出现异常时，先停止 H5 入口版本切换或回滚静态资源，再根据 operation_id 和协议版本定位；不能只清 Redis 或重启 Gateway 来“修复”权威业务事实。

## 13. 资料和依据

- 项目交接基线：`classic-farm-agent-handoff.md`；
- 微信小游戏 Skill：[wechat-minigame-dev](https://knot.internal.example.invalid/redacted-reference)；
- CloudBase Skill：[CloudBase 云开发](https://knot.internal.example.invalid/redacted-reference)；
- KM H5 兼容实践：[企业名片H5兼容微信小程序实践](https://km.internal.example.invalid/redacted-reference)；
- KM 业务域名实践：[微信小程序 WOA 业务域名配置实践](https://km.internal.example.invalid/redacted-reference)；
- KM H5 性能实践：[WebView 预创建提速 H5 首屏](https://km.internal.example.invalid/redacted-reference)；
- KM 小程序发布实践：[频道微信小程序发布审批流程智研自动化](https://km.internal.example.invalid/redacted-reference)；它补充了版本发布前的质量红线、并行审批、测试/产品/运营验收和发布后状态闭环，这些应纳入农场的正式发布门禁；
- 腾讯云关于 H5 通过 `web-view` 发布和与小程序通信的资料：[H5 实时通信](https://intl.cloud.tencent.com/zh/document/product/1219/64442)；
- 腾讯云关于小程序业务域名配置和校验文件的资料：[小程序业务域名配置指南](https://cloud.tencent.com/document/product/1301/100434)。
- 微信框架结构/配置/路由/生命周期的可访问版本镜像：[框架文档目录](https://wdk-docs.github.io/wxadev-docs/framework/)、[目录结构](https://wdk-docs.github.io/wxadev-docs/framework/structure.html)、[页面路由](https://wdk-docs.github.io/wxadev-docs/framework/app-service/route.html)。
- `web-view` 组件的可访问版本镜像：[web-view](https://wdk-docs.github.io/wxadev-docs/component/web-view.html)；该镜像为旧版本资料，不能替代上线前的官方当前规则核对。

平台能力、账号主体、审核规则和基础库行为可能变化，最终提交前应以微信公众平台当前后台和官方开放文档为准。
## 14. Repository implementation status

The minimal shell is now present in `./wechat-shell/` and can be imported as a
standalone Mini Program project in WeChat DevTools. It is the web-view route
and therefore depends on the account subject being allowed to configure a
business domain; it contains only the platform handoff:

```text
pages/game/game.js
  wx.login()
  -> POST /api/v1/auth/wx-miniprogram-login { code }
  -> web-view opens h5BaseUrl?launch_ticket=...
```

Before real testing, replace the public endpoints in
`wechat-shell/config.js`, configure the Mini Program request and business
domains, and implement the endpoint response
`{ "launch_ticket": "..." }`. The shell does not replace the formal server
integration checklist in `h5/SERVER-INTEGRATION.md`; it only completes the
client-side platform handoff that was previously a placeholder.
