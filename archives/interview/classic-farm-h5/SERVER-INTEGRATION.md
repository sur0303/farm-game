# 真实服务端联调

当前 H5 已完成 `farm-game` canonical v0.2 接入，不再提供浏览器内游戏服务器或本地数据回退。

## 链路

```text
Web 账号：POST /api/v1/accounts/register 或 /api/v1/session/login
Staging：POST /api/v1/auth/login
WeChat：POST /api/v1/auth/exchange
             │
             ├─ HttpOnly WebSession（Redis）
             └─ 一次性 ConnectionTicket（Redis）
                          │
H5 ── binary WSS / FarmFrame v0.2 ──> farm-gate
                                         ├─ player-svr
                                         ├─ farm-svr
                                         └─ farm-worker
                                              └─ 公司持久 Redis
```

WebSocket 101 后，H5 立即发送 `ClientHello(connection_ticket)`。Ticket 由 farm-gate 原子核销；随后客户端发送 `JOIN_FARM`，并等待同一 operation 对应的 `ACCEPTED` 和 subscription snapshot 后才开放业务写入。

## 本地联调

```bash
cd ../farm-game/deploy
docker compose -f docker-compose.local.yml up -d --build

cd ../../classic-farm-h5
VITE_API_PROXY_TARGET=http://127.0.0.1:8080 \
VITE_WS_PROXY_TARGET=ws://127.0.0.1:8080 \
VITE_WS_URL=/ws \
npm run dev -- --host 0.0.0.0 --port 5174 --strictPort
```

开发网络可达时直接访问 `http://198.18.0.6:5174/`，并把 `198.18.0.6:5174` 加入 Gate Origin
白名单。无法直连时，再由浏览器本机发起 SSH LocalForward：

```powershell
ssh -p 36000 -N -L 5174:127.0.0.1:5174 developer_026@198.18.0.6
```

Vite 同源代理 `/api` 和 `/ws`，所以只需转发 5174。

`farm-game` 的运行进程要求配置 `FARM_REDIS_URL`；缺少配置会拒绝启动，不会回退进程内 Store。Compose 通过 `scripts/company-redis-compose.sh` 注入公司 Redis 地址。

## 环境合同

| 环境 | HTTPS 身份入口 | WSS 协议 | 数据 |
|---|---|---|---|
| Web | 网页账号与密码 | canonical v0.2 | farm-game + Redis |
| Staging | 固定隔离测试票据 | canonical v0.2 | farm-game + Redis |
| WeChat | 小程序 `launch_ticket` | canonical v0.2 | farm-game + Redis |

所有环境的好友列表、分享 token、访问权限、经营命令和 Workflow 均由服务端裁决。H5 只保存 UI 状态和可恢复的 Operation Journal；它不能生成好友权限或修改权威游戏数据。

## 验收

```bash
npm test
npm run audit:build
npm run e2e:web
npm run e2e:staging
```

需要主动停止服务验证 Workflow 接管时，再显式运行 `npm run e2e:workflow`。生产地址交接前可继续使用 `npm run validate:handoff` 检查 Origin、HTTPS/WSS 路径和占位域名。
