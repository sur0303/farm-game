# farm-game — 经典农场小游戏后台

按 **3000w DAU** 目标设计的农场游戏后台。前端 H5 仅用于演示后端能力,考察重点是架构设计、容量估算、瓶颈分析与压测验证。

## 状态

当前分支已经完成 **v1.18 三 Loop 第一、二阶段本地纵切**：Direct FarmGate WebSocket、`PlayerActor + FarmActor`、双账号分享/好友实时协作，以及持久 Workflow + 独立 `farm-worker` 的进程故障恢复均可运行。按最新决策，Player/Farm/Friend/Share/Workflow 直接使用公司持久 Redis；Tcaplus 迁移暂缓。多节点迁移、Redis 故障语义和 3000 万 DAU 压测仍是生产前置项。详细状态见 [`v118-three-loop-implementation.md`](docs/verification/v118-three-loop-implementation.md)。

## 架构一句话

H5 通过 HTTPS 登录并经 CLB/Ingress 直连 `farm-gate` WSS；`farm-gate` 管 Session、协议、路由和下行背压，`PlayerActor(player_id)` 裁决玩家资产，`FarmActor(farm_id)` 裁决农场、地块与 Presence，跨 Actor 操作用持久 Workflow/Settlement 推进。公司持久 Redis 是当前业务恢复来源，并同时承载 Session、短期分享和可重建连接状态。

当前冻结结论见 [`00-overview.md`](docs/design/00-overview.md) 和 [`17-access-routing-migration.md`](docs/design/17-access-routing-migration.md)；`study/supernova/classic-farm-agent-handoff.md` 只作为历史讨论基线。

## 目标目录

下表同时包含当前纵切和后续生产目标；未实现部分在验证记录中单独列出。

| 路径 | 用途 |
|---|---|
| `cmd/` | 目标 farm-gate / player-svr / farm-svr / farm-worker 进程入口，以及兼容探针 |
| `api/proto/` | Protobuf 业务 IDL(唯一 IDL) |
| `internal/gateway/` | farm-gate 应用接入层(FarmFrame、Session、限流、下行背压；业务无权威) |
| `internal/player/` / `internal/farm/` | 当前 PlayerActor/FarmActor、入口服务与 participant 协调 |
| `internal/logic/` | 迁移基线、通用 Actor 原语、下行和后续双 Region 路由 |
| `internal/workflow/` / `cmd/farm-worker` | 当前跨权威 WorkflowStore、重试执行器和独立 Worker 进程 |
| `internal/storage/` | PlayerStore/FarmStore/WorkflowStore 目标接口；当前仍保留待迁移 OwnerStore |
| 客户端独立工作区 | 现有 H5 位于同级目录 `../classic-farm-h5`；通过服务端 Proto commit/tag、TS 生成代码和 golden vectors 对齐，不在本仓库复制第二套客户端 |
| `deploy/` | 逐阶段补齐 docker-compose 和部署配置；`secrets/` 存真实连接凭据(不入库) |
| `loadtest/` | Go WSS 压测客户端 |
| `docs/` | 设计文档、架构与容量估算、性能测试报告 |

## 外部依赖(已申请,本机可连)

- **公司持久 Redis** 当前业务主库、会话与缓存 — 本地连接配置见 `deploy/secrets/redis.conf.local`（不入库）
- **TcaplusDB** 已有探针资产保留为可选迁移路径，不是当前阶段前置项

## 构建

```sh
go build ./...
```

`go build ./...` 会构建 canonical `cmd/farm-gate`、当前 `cmd/player-svr` / `cmd/farm-svr`，以及迁移基线、Worker 占位和基础设施探针。日常 local/staging Compose 使用 Direct WSS；TCONND/gateproxy 只使用专用 probe Compose。

完全隔离的本地联调：

```sh
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build
```

宿主机预编译入口用于绕过 Docker 内无法解析内部 Go 模块域名的问题。Compose 包装脚本从被 Git 忽略的本地配置读取公司 Redis 凭据，不要把密码写入仓库。持久 Redis 决策见 [`31-persistent-redis-authority.md`](docs/design/31-persistent-redis-authority.md)。
