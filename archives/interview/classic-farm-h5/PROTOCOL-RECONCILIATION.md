# H5 与 farm-game canonical Proto 对齐记录

> **状态（2026-07-31 刷新）**：下文主体写于 **2026-07-26**，描述当时 zip / 设计稿审计结论
> （「后端只有 tcaplus-verify、无 runnable farm-gate」等）。**该结论已过时，不要再当作当前联调状态。**
>
> **当前事实**（`classic-farm-h5` + 同级 `farm-game`，分支
> `codex/v03-access-session-hardening-20260730`）：
>
> - `farm-gate` / `player-svr` / `farm-svr` / `farm-worker` 可本地运行；
> - Web 账号 / Cookie / Ticket / Direct WSS、Join、种植与好友链路已可纵切；
> - 服务端已完成合同对齐 Batch 0–2（进程内 Catalog / Actor 双队列 / 路由控制面等）；
> - 冻结信封 `FarmFrame` oneof 与公网 RetryDirective 字段仍属 Batch 3（等 29/30/31/32）；
> - 交接导航见 `farm-game/docs/verification/contract-alignment-batch0-2-handoff-20260731.md`
>   与 `farm-game/docs/verification/current-workspace-two-repos-handoff-20260730.md`。
>
> 生成的 TS / proto 对齐以当前两仓代码与生成产物为准；下文保留作历史追溯。

更新时间：2026-07-26（正文）；状态横幅 2026-07-31

## 结论（历史 · 2026-07-26）

当时审计对象有两份来源：Gongfeng 项目 `developer_026/farm-game`（项目 ID
`1862451`）和用户提供的 `farm-game-master.zip`（2026-07-26 下载快照）。zip
比远程项目初始提交包含更完整的 v0.2 设计稿，但它在**当时**仍不是可联调的服务端。

当时证据：

- zip README 仍标记项目处于 S1 详细设计阶段；设计文档已经写到 v0.2/S2
  目标合同；
- zip 的 `api/proto/`、`api/gen/` 和 `internal/gateway/protocol/codec.go` 仍
  是 v0.1 可执行基线，不是 v0.2 目标合同的实现；
- 后端的 `cmd/` 只有 `tcaplus-verify`，没有可启动的 `farm-gate`、
  `farm-logic`、`farm-worker` 入口；
- 后端目前只有探针用途的 Docker 构建占位，尚无 farm-gate/farm-logic 的可运行镜像或 docker-compose；
- 没有可供 H5 访问的 HTTPS 登录接口、WSS 地址或已验收的 TCONND/gateproxy staging 环境。

因此，**在 2026-07-26 时点** H5 继续使用 Mock 垂直链路是正确的。该判断已被后续加固分支推翻；见文首状态横幅。

## v0.2 目标合同与当前可执行代码的差异

zip 的 v0.2 目标合同已经改为单一 `FarmFrame` Protobuf：

```text
FarmFrame {
  protocol_version
  kind
  connection_seq
  body
}
```

每个 WebSocket binary message 承载一个 FarmFrame；v0.2 明确删除
`total_size`、`meta_size` 和 `FrameMeta`。因此不能再把旧的 8 字节大端头当成
最新 canonical 协议。

FarmFrame 的 `body` 再按 `kind` 解码为：

- `ClientHello` / `ServerHello`；
- `common.ClientCommandEnvelope`；
- `common.CommandResultEnvelope`；
- `common.ServerPushEnvelope`；
- `ConnectionError`。

v0.2 目标合同还新增/调整了：`request_id`、单向 `connection_seq`、
`CommandStatus.ACCEPTED`、`session_authority_epoch`、
`heartbeat_interval_ms` 和 `max_frame_bytes`。`ClientHello` 的协议版本由外层
FarmFrame 承担，不应在 body 中重复维护。

但 zip 中当前实际 `api/proto/common/*` 仍保留 v0.1 的 `FrameMeta`、旧字段和旧
信封；`api/gen/` 与 Go codec 也对应旧版本。设计稿不能证明 v0.2 已经生成或可运行。

以上内容是早期差异记录。当前 H5 已从同级 `farm-game/api/proto` 生成 TypeScript
绑定，并在 Web、Staging、WeChat 三种运行环境统一使用 canonical v0.2：

- `FarmFrame`、`connection_seq`、`request_id` 和 `operation_id` 走正式 Protobuf；
- `target_owner_id`、版本号和 epoch 在业务边界使用十进制字符串/BigInt，避免
  JavaScript `Number` 精度损失；
- `JOIN_FARM` 同时等待匹配的 `ACCEPTED` 与 `FARM_SNAPSHOT`；
- 业务命令、结果、快照、变化、重同步和好友名单变更均由 canonical adapter 解码；
- `WsClient` 必须显式注入 canonical adapter，没有第二套运行时线协议；
- 本地 UI 也通过 HTTPS/WSS 连接真实 farm-gate，不存在浏览器内游戏服务器。

当前 `src/protocol/canonical-frame.ts` 仍是早期 8 字节 Meta/Body 研究辅助代码，
没有接入 Staging/WeChat 构建；它不能代表 zip v0.2 的 FarmFrame 实现。真正联调
时应直接接入 v0.2 生成的 TypeScript Proto 和 Go golden vectors，并删除或隔离这份
旧辅助代码。

`WsClient` 通过 `src/protocol/protocol-adapter.ts` 注入线协议适配器；产品入口只注入
`canonicalV02ProtocolAdapter`。单元测试的协议 double 位于 `src/test/helpers/`，不会
被产品入口导入或进入 Vite 产物。

## 服务端交付前必须确认的 7 项

1. v0.2 目标合同的 FarmFrame、ClientHello/ServerHello、ConnectionError 和
   `connection_ticket` 核销流程是否就是实际 Staging 合同；设计稿已定义首帧，
   但代码仍是 v0.1，不能只凭文档接入。
2. HTTPS 登录接口的正式路径、请求/响应 JSON、Cookie 属性、票据 TTL 和错误码。
3. `ClientCommandEnvelope.payload` 对应的每个 P0a 命令 message 及其字段编号。
4. `FARM_SNAPSHOT`、`FARM_CHANGE`、`CommandResultEnvelope.payload` 的具体 Body
   message、字段编号和是否允许未知字段。
5. `operation_id` 的正式长度和编码，以及 `player_id/owner_id/target_owner_id`
   的 `uint64` 生成代码表示；H5 业务层统一使用十进制字符串或 BigInt。
6. TypeScript 代码生成工具、版本、生成目录和 Go 端的 golden vectors；客户端不应
   手写第二份 Proto 字段定义。
7. 可访问的 HTTPS/WSS Staging 地址、测试账号、协议版本和一条可重置的初始 Owner
   数据。

## 交付后的实施顺序

1. 固定 canonical Proto 的 commit/tag，并将 Proto 作为客户端生成代码的唯一来源。
2. 生成 TypeScript message 类型和 protobuf 编解码代码；保留当前 Mock codec 只供
   Mock 回归，不让它进入 Staging/WeChat 构建。
3. 按 v0.2 的 `ClientHello/ServerHello` 目标合同改造 `WsClient`；字段编号、
   票据核销错误和 golden vectors 仍以最终生成代码为准，不手写第二套定义。
4. 实现 v0.2 单一 FarmFrame 适配器，禁止恢复 `total_size + meta + body`；先用
   Go codec 的 golden vectors 做字节级往返测试。
5. 让 Fake Server 同时走 canonical 帧和真实 envelope，再执行 Join、个人命令、
   断线恢复、幂等查询、版本缺口、好友浇水和偷菜结算测试。
6. 通过 Staging 验收后，才接入微信壳的正式 HTTPS 域名、`launch_ticket` 和真机。

## 联调通过标准

- H5 能完成一次性登录、WSS 鉴权和 `JOIN_FARM`；
- Snapshot 与 FarmChange 依据 `farm_version` 收敛，出现缺口自动重新 Join；
- `SUCCEEDED / PROCESSING / NO_CHANGE / REJECTED / UNAVAILABLE` 语义与服务端一致；
- 超时、刷新、断线和重复点击复用同一 `operation_id`，不会重复扣款或发奖；
- `BEGIN_THEFT` 能从 `PROCESSING` 最终收敛到 `SUCCEEDED`；
- Go 与 TypeScript 对同一 golden vector 生成完全一致的帧字节；
- 客户端日志能用 `trace_id / operation_id / owner_id / farm_version` 与服务端串联。
