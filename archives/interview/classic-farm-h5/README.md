# 经典农场 H5

经典农场的 H5 客户端只连接真实 `farm-game` 后端。仓库不再提供可运行的本地模拟登录、内存好友/分享服务或浏览器内游戏服务器。

```text
H5
  ├─ HTTPS/JSON → farm-gate（账号、会话、好友、分享）
  └─ WSS/Protobuf FarmFrame v0.2 → farm-gate
                                      ├─ player-svr
                                      ├─ farm-svr
                                      └─ farm-worker
                                           └─ 公司持久 Redis
```

## 本地启动

先启动同级 `farm-game` 的真实本地栈：

```bash
cd ../farm-game/deploy
docker compose -f docker-compose.local.yml up -d --build
```

然后启动 H5：

```bash
npm install
VITE_API_PROXY_TARGET=http://127.0.0.1:8080 \
VITE_WS_PROXY_TARGET=ws://127.0.0.1:8080 \
VITE_WS_URL=/ws \
npm run dev -- --host 0.0.0.0 --port 5174 --strictPort
```

打开 `http://127.0.0.1:5174`，注册网页账号或使用已有账号登录。`npm run dev` 等价于 Web 模式；如果 farm-gate、Redis 或业务服务未启动，页面会明确失败，不会退回浏览器内数据。

当前开发机网络可达时，直接打开 `http://198.18.0.6:5174/`；farm-gate 的开发 Origin 白名单必须包含
`198.18.0.6:5174`。如果本机无法直连开发机，再从本机建立 SSH LocalForward。开发服务器让 `/api`
和 `/ws` 都走同一个 H5 端口，因此只需要转发一个端口：

```powershell
ssh -p 36000 -N -L 5174:127.0.0.1:5174 developer_026@198.18.0.6
```

也可以在当前远程开发工具的“端口/Ports”面板中转发 `5174`。保持该终端运行，然后在本机打开
`http://127.0.0.1:5174/`。不要把 `VITE_WS_URL` 配成浏览器本机的 `127.0.0.1:8080`，否则还会额外要求
转发 8080。

## 环境

- `npm run dev` / `npm run dev:web`：独立 Web 账号模式，调用 `/api/v1/accounts/register`、`/api/v1/session/login` 和真实 WSS。
- `npm run dev:staging`：隔离测试身份模式，调用 `/api/v1/auth/login`，仍然连接真实 farm-gate 与业务服务。
- `npm run dev:wechat`：微信 `web-view` 承载模式，使用小程序交接的 `launch_ticket`。
- `npm run build` / `npm run build:web`：Web canonical v0.2 构建。
- `npm run build:staging`、`npm run build:wechat`：对应环境的 canonical v0.2 构建。

所有环境都使用从 `farm-game/api/proto` 生成的 canonical FarmFrame v0.2 绑定。`WsClient` 必须显式注入协议适配器，不存在旧协议或本地服务器回退。

## 验证

```bash
npm run build
npm run build:staging
npm run build:wechat
npm run audit:build
npm test
npm run e2e:web
npm run e2e:staging
npm run e2e:workflow
```

- `e2e:web` 使用真实 Redis 支持的网页账号完成注册、重新登录和分享加好友。
- `e2e:staging` 使用两个真实浏览器身份验证经营、好友实时刷新、好友农场浇水/偷菜和重连。
- `e2e:workflow` 会停止并恢复本地业务进程，验证持久 Workflow 的接管；只在本地 Compose 栈上运行。
- `audit:build` 分别构建 Web、Staging、WeChat，并检查已移除的本地运行模块不会重新进入产物。

Vitest 中仍保留纯单元测试 double（测试票据、测试传输和测试协议服务器）。它们位于 `src/test/helpers/`，不会被任何 Vite 产品入口导入，也不能作为一个游戏环境启动。

## 接入说明

- 远程开发时 Vite 同时把同源 `/api` 和 `/ws` 转发给 farm-gate，使 SSH 只需转发 H5 端口；部署环境通常使用登录响应中的 `wss_endpoint`。
- Web 账号、HTTP Session、ConnectionTicket、好友关系、分享 token、玩家/农场状态和 Workflow 均由服务端持久化。
- URL 中只有不透明 `share_token`；H5 不会把 `owner_id` 当作好友权限。
- 微信路线见 [`wechat-shell/README.md`](./wechat-shell/README.md)。
- 服务端交接与 canonical 协议细节见 [`SERVER-INTEGRATION.md`](./SERVER-INTEGRATION.md) 和 [`PROTOCOL-RECONCILIATION.md`](./PROTOCOL-RECONCILIATION.md)。
