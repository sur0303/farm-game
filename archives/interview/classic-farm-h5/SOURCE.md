# classic-farm-h5 源码快照

来自 Gongfeng `developer_026/classic-farm-h5` 的 `origin/master`（`1fb6dd1f`，2026-08-21）。

这是你自己写的 H5 客户端，和 `farm-game` 后台对齐。存档时已去掉：

- `node_modules` / `dist` / Playwright 报告
- 本地覆盖文件（`.env.staging.local`、`.env.web.local`）

留下的 `.env.example` / `.env.web` / `.env.staging` / `.env.wechat` 只有模式开关和 demo token，没有集群密码。

对照后端：同仓 `../farm-game`。协议入口看 `src/protocol/`，登录看 `src/auth/`，WSS 看 `src` 里的 transport / journal。
