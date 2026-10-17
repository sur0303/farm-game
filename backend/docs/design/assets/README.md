# 架构图资产

当前 L0 Freeze 与中期汇报共同使用的总体架构图：

- `classic-farm-overall-architecture-freeze.png`
- `classic-farm-overall-architecture-midterm.png`
- 可维护源文件：`classic-farm-overall-architecture-midterm-source.html`

中期版基于 `v3.1` 非破坏性派生，原 `classic-farm-overall-architecture-v3.1-freeze.png`
保留为历史发布图。当前唯一可维护的统一架构图源为
`classic-farm-overall-architecture-midterm-source.html`；它同时导出中期副本和不带版本号的
`classic-farm-overall-architecture-freeze.png`。`v3.2-source.html` 保留为历史评审画布，
不再作为当前发布源。当前图删除标题版本号和右上角内部评审说明，并按最新复核修正接入、
Friendship 请求局部性、Tlog 可靠链路、Player Task/Codex 权威存储与好友连续导航。

中期版主干已继续收敛到 `v1.21.1-production-architecture-freeze`
主干对齐。P0
只保留 `farm-gate / player-svr / farm-svr / farm-worker` 四个 Deployment；FriendshipDomain、
PlayerMail 和 Player/Friend Read API 共部署在 player-svr，但仍保持独立模块、Repository、
执行池、错误码与逻辑发现别名。图中同步更新双 Actor 独立路由域、三类 Redis、Opportunity
Header、偷菜双层结果、邮件领取和 Worker Role 边界。接入层默认使用
`公网 DNS → CLB/TLS → farm-gate Direct WebSocket`；WAF 与 `TCONND + gateproxy` 都是探针通过后
显式启用的候选外围 Adapter。A 级 Tlog 使用
`Outbox → ExportConsumer → Tcaplus ExportSpool → TlogRelay → TGLog` 可靠交接；
B 级成功流水来自提交事件，C 级使用 Pod 内有界队列。

当前图还纳入了 `v1.21` 的 Task/Codex 合同：二者都是 PlayerActor 内领域模块，不新增
TaskActor、CodexActor 或独立 Deployment；图中同时标出 PlayerTaskState、
PlayerCodexMeta/Collection、Task/Codex Consumer，以及
`HarvestReceipt + CodexUnlockIntent → Collection/Meta 幂等推进 → Task/RedPoint 投影` 链路。

导出 PNG 为 `1920×1080` 完整画布。导出前必须检查 `body/sidebar/footer` 边界以及所有卡片的
`scrollWidth/scrollHeight`，禁止仅凭局部滚动截图判断无裁切。

`v2.1`～`v3.2` 是此前评审过程中的历史版本，仅用于视觉对比，不再作为当前生产合同依据。
