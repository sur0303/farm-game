# 经典农场 WeChat Mini-Program

原生微信小程序，通过 WebSocket 直连 farm-gate。

## 架构

```
WeChat Mini-Program (原生)
  └── wx.login() → farm-gate /api/v1/auth/wx-miniprogram-login
  └── wx.connectSocket() → CLB/Ingress → farm-gate /ws
       └── PlayerActor / FarmActor
```

**为什么不用 web-view？** WeChat mini-program 的 `<web-view>` 组件不支持 WebSocket，而农场游戏需要实时双向通信。必须用原生小程序 + `wx.connectSocket()`。

## 文件结构

```
miniprogram/
├── app.json          # 页面路由 + 窗口配置
├── app.js            # 应用入口（session 恢复）
├── app.wxss          # 全局样式
├── sitemap.json      # 索引配置
├── pages/
│   ├── login/        # 登录页（wx.login → farm-gate 认证）
│   │   ├── login.js
│   │   ├── login.wxml
│   │   └── login.wxss
│   └── index/        # 农场主页（WS 连接 + 地块交互）
│       ├── index.js
│       ├── index.wxml
│       └── index.wxss
└── utils/
    ├── config.js     # farm-gate API/WSS 地址
    ├── auth.js       # wx.login → ticket 流程
    └── ws-client.js  # Direct FarmGate WebSocket 客户端
```

## 开发模式（Dev）

### 1. 启动后端服务

```bash
# farm-gate（含登录、分享与 Direct WebSocket）
cd ..
CGO_ENABLED=0 GOOS=linux go build -trimpath -o deploy/farm-gate/farm-gate ./cmd/farm-gate
docker build -t farm-game-farm-gate:local -f deploy/farm-gate/Dockerfile deploy/farm-gate
docker run -d --name farm-gate-live -p 8082:8082 \
  -e GATE_TRANSPORT=websocket \
  -e GATEWAY_LISTEN=:8082 -e GATEWAY_DOWNLINK_LISTEN=:9093 \
  farm-game-farm-gate:local
```

### 2. 打开 WeChat DevTools

1. 打开微信开发者工具
2. 导入项目 → 选择 `classic-farm-h5/miniprogram` 目录
3. 填写 AppID（开发模式可用测试号）
4. 在"本地设置"中勾选"不校验合法域名、web-view（业务域名）、TLS 版本以及 HTTPS 证书"

### 3. 配置服务器地址

编辑 `miniprogram/utils/config.js`：

```javascript
apiBaseUrl: 'http://localhost:8082',   // farm-gate
wsBaseUrl: 'ws://localhost:8082',      // farm-gate
```

### 4. 验证登录

1. 小程序打开后进入登录页
2. 点击"微信一键登录"
3. 后端返回 `connection_ticket` + `player_id`
4. 自动跳转到农场页面
5. WebSocket 101 成功 → 立即发送 ClientHello → 进入农场

## 生产部署

### 前提条件

- HTTPS/WSS 域名（farm-gate API + Direct WSS）
- WeChat AppID
- WeChat 后台配置合法域名

### 步骤

1. **配置 HTTPS** — CLB/Ingress 为 farm-gate 配置 HTTPS/WSS 证书
2. **WeChat 后台配置** — 添加 request 合法域名 + socket 合法域名
3. **填写 AppID** — `miniprogram/utils/config.js` 中 `appId` 字段
4. **修改后端地址** — `apiBaseUrl` 和 `wsBaseUrl` 指向生产域名
5. **生产鉴权** — `wechat.go` 中将 `WeChatDevVerifier` 替换为真实 WeChat API 验证（调用 `https://api.weixin.qq.com/sns/jscode2session`）

## 当前状态

| 功能 | 状态 |
|---|---|
| wx.login() → farm-gate auth | ✅ 已完成（dev 模式） |
| WebSocket 直连 farm-gate | ✅ 已完成 |
| 地块点击/种植/浇水/收获 | ✅ 已完成（UI 层） |
| 分享农场（WeChat 分享） | ✅ 已完成 |
| 好友接受/访问好友农场 | ✅ 已完成（骨架） |
| 二进制 Protobuf 协议 | ⚠️ TODO（当前 JSON 简化） |
| 生产 WeChat API 鉴权 | ⚠️ TODO（当前 dev 模式） |
| 好友关系持久化 | ⚠️ TODO（当前 in-memory） |
| HTTPS/WSS 生产配置 | ⚠️ TODO |

## 下一步

1. **集成 Protobuf** — 将 H5 的 `canonical-v02-adapter.ts` 生成的 protobuf 代码移植到小程序，替换 JSON 简化协议
2. **生产鉴权** — 调用 WeChat jscode2session API 获取 openid + session_key
3. **农场逻辑** — 完善 Actor 交互（快照同步、指令确认、通知处理）
