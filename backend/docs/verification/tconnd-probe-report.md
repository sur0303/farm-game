# TCONND S2 Probe Report

日期：2026-07-26
状态：本地 Docker probe-only/完整 Compose 骨架已落地；真实平台验收仍被正式镜像权限、
BY_WS 配置和平台运行时证据阻塞。Go transport、最小探针以及 Gongfeng 资产的本地
构建已完成，但 T1～T8 仍未通过。

## 计划与已知平台资产

本报告按 [TCONND + gateproxy 接入实施计划](../design/15-tconnd-gateproxy-integration-plan.md) 执行。该计划来自 Gongfeng 分支 `codex/tconnd-gateproxy-plan`，提交 `bc5aa48`。

计划中已定位、但尚未在本项目 Linux 服务器实测冻结的资产：

- TCONND 镜像线索：`mirrors.tencent.com/mfcn/tconnd:0.0.7`、当前 infra-test
  发布记录中的 `mirrors.tencent.com/mfcn/tconnd:0.0.13`；
- gateproxy 镜像线索：当前 infra-test 发布记录中的
  `mirrors.tencent.com/mfcn/gateproxy:0.0.34`；
- `mfcn/gateproxy` 的 Unix gRPC `ConnectorService.GateStream`；
- 固定 Go client 版本：`git.internal.example.invalid/mfcn/gateproxy/go-client v0.0.9`；
- TCONND 3.4.36 的 TCONNAPI/TFRAMEHEAD/TBUS 资产；
- `WEBSTART/WEBINPROC/WEBSTOP/WEBRESP` 定义；
- `mfcn/gatesvr` 中 TCONND、gateproxy 和 Go 进程共享 IPC/Unix Socket 的部署参考。

仍未确认：

1. `tconnd:0.0.7` 的实际版本、架构和 BY_WS 能力；
2. 可运行的 BY_WS/WSS 配置、监听端口和证书；
3. 与 Go client v0.0.9 匹配的 gateproxy 镜像 tag/digest；
4. BY_WS 是否被 gateproxy 归一化为普通 `START/INPROC/STOP`；
5. GateStream 下行写入、慢连接、队列满、重启和心跳的真实语义。

本地只读复核补充（2026-07-26）：工作区 `infra-test` 的发布记录显示 TCONND
`0.0.13`、gateproxy `0.0.34`；其生成的 TCONND 配置仍使用
`PDULENPARSERID_BY_GCP`，没有 BY_WS/WSS 模板。尝试拉取这两个镜像均返回 registry
`pull access denied`，因此没有把本地 gatesvr 代码或 TCP/GCP 配置当作真实 BY_WS
证据。当前新增 `deploy/tconnd/tconnd_gcp.xml` 只是一份候选配置，必须经过目标镜像
启动和 WebSocket + GateStream 实测。

此前直接从候选 registry 拉取两个官方镜像时均返回 `pull access denied`。本轮通过
Gongfeng MCP 拉取源码/LFS 资产并完成了本地临时镜像构建，已可启动 probe-only 的
诊断尝试；这不改变正式镜像权限、BY_WS 配置和 T1～T8 仍未验收的结论。

补充资产线索：同级 Gongfeng 工作区有 `tconnd 3.2.1` ELF，静态字符串包含
`PDULENPARSERID_BY_WS`、WebSocket Upgrade 和 `TFRAMEHEAD_CMD_WEBSTART`。尝试用候选
配置单独启动时因缺少 TAPPCTX/平台运行环境失败；该二进制既不是待验收镜像，也没有
配套 gateproxy，因此这只能支持“配置方向可继续探查”，不能作为 T7/T1～T8 结果。

本地可执行入口：

- `deploy/docker-compose.probe-only.yml`：TCONND + gateproxy + `tconnd-probe`；
- `deploy/docker-compose.local.yml`：Redis + farm-logic + TCONND + gateproxy + farm-gate；
- `scripts/probe-tconnd-assets`：镜像存在性、digest/架构和配置字符串只读检查。

Gongfeng MCP 资产补充（2026-07-26）：已从 `mfcn/gateproxy`（项目 ID `1199993`）
拉取带 LFS 的 TCONND 3.4.36 资产，并在 `mirrors.tencent.com/red/ci/tlinix2-gcc9.4:0.0.6`
中成功构建 gateproxy。当前本机临时 tag 为 `local/gateproxy:gf-v0.0.34` 和
`local/tconnd:gf-3.4.36`；二者仅供探针诊断，不是官方发布镜像。资产只读检查已能
确认二进制和动态库存在，但仍不能替代正式镜像、BY_WS/WSS、证书和 GateStream 实测。

Compose 已修正 Docker 与 Gongfeng Pod 的关键差异：gateproxy 使用
`ipc: service:tconnd` 共享 `--use-bus` 的 SysV IPC；只共享 Unix socket volume
是不够的。

没有用 FakeTransport 或本地 transport 单测冒充平台结论。

## 当前代码边界

- `internal/gateway/transport/gateproxy`：farm-gate 内直接消费 GateStream，维护 `id/conn_idx/extend` 到 `connection_id + generation`，只转发 opaque FarmFrame bytes；
- `cmd/farm-gate`：设置 `GATEPROXY_SOCKET` 后直接建立 Unix gRPC GateStream；stream 断开时 /readyz 变为 NotReady，连接代次失效；`cmd/farm-gateway` 仅保留为兼容名称并调用同一实现；
- `cmd/tconnd-probe`：独立 GateStream 观察器，记录命令类型、连接代次、消息长度和 body SHA-256，不记录 body、ticket 或 openid；
- Docker staging：只编排 TCONND、gateproxy 和直接连接 GateStream 的 farm-gate，不再创建第二个公网 WebSocket Gateway 或独立 farm-tconnd-adapter 进程；
- `git.internal.example.invalid/mfcn/gateproxy/go-client` 已固定为 `v0.0.9`。

静态 Proto 对照补充：工作区缓存的 gateproxy Go client `v0.0.9` 与 `v0.0.34`
的 `connector_service.proto` 保留了既有 GateStream、TconndHead、START/STOP/INPROC
字段，仅在枚举和文件尾部追加字段/Meta；因此 wire 兼容性有依据，但这不替代
`gateproxy:0.0.34` 镜像的真实 GateStream 启动验证。

本地可重复验证：

```text
go test ./internal/gateway/transport ./internal/gateway/transport/gateproxy   ./internal/gateway/downlink ./internal/gateway/command
```

## T1～T8

| 探针 | 真实结果 | 当前结论 |
|---|---|---|
| T1 消息边界/命令类型 | 未执行 | 需要真实 BY_WS 客户端 + GateStream 原始事件，确认普通还是 WEB 命令 |
| T2 正常下行 | 未执行 | 只定义本地 QueueAccepted；未证明浏览器已收到 |
| T3 断线/队列/慢读 | 未执行 | 本地 ConnectionWriter/Registry 有单测，不能替代 gateproxy/TCONND |
| T4 顺序 | 未执行 | transport mapper 同 generation 串行发送，仍需真实链路验证 |
| T5 最大帧 | 未执行 | `MAX_FRAME_BYTES` 未冻结，等待 WSS/TCONND/TBUS/GateStream 共同边界 |
| T6 重启/Ready | 未执行 | 需验证 IPC、Unix Socket 清理、重连和 /readyz |
| T7 发布资产启动 | 未执行 | 等待镜像权限、tag/digest、证书和 BY_WS 配置 |
| T8 心跳/半开 | 未执行 | transport 保留 GateStream heartbeat；应用层字段和超时职责等待真实结果 |

## 执行顺序

严格执行：

```text
TP0 只读资产确认
→ TP1 GateStream probe
→ TP2 启动 TCONND + gateproxy
→ T7
→ T1/T2/T3/T8
→ 决策 gateproxy 是否直接承接 BY_WS
→ T4/T5/T6
→ H5 ClientHello/ServerHello/Join/Snapshot
```

在 T1/T2/T3/T7 没有原始证据前，不宣布 S2 真实接入完成；若 gateproxy 不能承接 BY_WS，先获取平台支持版本，不在 farm-gate 中引入 cgo/TBUS 或第二套公网 WSS。
