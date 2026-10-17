# PO1-FINAL：在线 Plant 最终接线交接

日期：2026-08-05

## 已完成的实现

- `farm-svr` 的 authority 与 legacy typed Farm Participant 构造都传入同一个现有 `service.EmitChange` observer。typed `COMMIT_PLANT` 成功提交后的 observer 负责一次 FarmSubscriptionHub 下行；当该 observer 已配置时，public PLANT FirstHop 不再把同一个 ActionResult 走第二条变化发布路径。Participant replay、拒绝和 no-change 不会重复发布。
- `player-svr` 用同一份 W31 placement 接线 `SetOperationResultSource`：authority 模式复用 `authorityRuntime.Placement`，legacy 模式把同一个 placement 对象同时交给 OperationResult reader 与 typed Participant proof。WorkRepository、placement 或 bucket version 缺失时保持 `ERR_COMMAND_UNIMPLEMENTED` 的 fail-closed 语义，不伪装成 `NOT_FOUND`。
- `PLANT` 与 `GET_OPERATION_RESULT` 继续使用现有 Catalog/FirstHop/Ingress 和同一 generic `PLANT` WorkRecord；没有新增结果库、Actor、Workflow、路由或 Gate 编排。
- 修正 W31 placement 自检 probe，改用现有 `workidentity.StableOpaqueID` 生成规范 `sid1_` identity；不改变 bucket 算法或物理布局。
- 新增 `cmd/po1-plant-e2e`。它启动真实 Redis（优先本机 `redis-server`，否则使用本地 Docker Redis 镜像）、编译并启动真实 `player-svr`、`farm-svr`、`farm-worker`。只有 GatewayDownlink 接口使用范围受控的网络 capture sink；没有 mock Actor、Participant 或 WorkRepository。

## 可重复命令与证据

精确在线 E2E 命令：

```text
go run ./cmd/po1-plant-e2e
```

本次运行通过，输出证明了：

- public FirstHop PLANT 先建立唯一 generic WorkRecord；Player 进程中断后返回 `PROCESSING`，Redis WorkRecord 为 `STEP_UNKNOWN/UNKNOWN`。
- Player 重启后，真实 WR2 `farm-worker` 继续同一 WorkRecord；终态为 `COMPLETED`，`RESERVE_SEED`、`COMMIT_PLANT`、`COMMIT_SEED_RESERVATION` 均有 applied receipt。
- 初始 1 枚 carrot seed 最终为 0，Farm snapshot 中目标 crop cycle 只出现一茬。
- 两个当前订阅者各收到 1 条 `FARM_CHANGE`；Farm version 与确定性 payload 相同。原 operation 重试最终成功，但没有新增扣种、种植或广播。
- 恢复期间同一 operation 的 `GET_OPERATION_RESULT` 返回 `PROCESSING`；恢复完成后同一 operation 返回终态 `COMPLETED/SUCCEEDED` 的 public `PlantResult`。
- 变造 writer epoch 和 holder boot 的 typed Participant 请求均 fail closed。

其余验证命令：

```text
go test -count=1 ./cmd/player-svr ./cmd/farm-svr ./internal/plantonline/...
go test -race -count=1 ./cmd/player-svr ./cmd/farm-svr ./internal/plantonline/...
go test -count=1 ./...
go vet -buildvcs=false ./...
go build -buildvcs=false ./...
git diff --check
```

以上命令在本次 worktree 均通过。

## 仍未覆盖的生产能力

- E2E 使用启动时冻结的完整 `AssignmentSnapshot`。仓库仍没有跨进程 RouteManager publisher/watch，也没有可宣称生产多 Pod 热迁移完成的 durable recovery adapter；本 E2E 的 Player 重启保持同一 holder boot，不能作为热迁移证据。
- E2E 直接调用真实 Logic FirstHop gRPC，并以 downlink contract sink 观察下行；没有启动完整公网 WSS `farm-gate`/真实浏览器连接。因此它证明 Catalog/FirstHop/Ingress 与 Actor/Participant/WorkRecord 接线真实可达，不扩大为完整边缘平台验收。
- 仍未覆盖跨 Pod 路由快照热更新、Authority transition 发布/订阅、真实 Gateway 连接生命周期，以及生产 Tcaplus/外部平台适配器。这些不在本 PO1 任务内，也没有用 Hook、文件轮询、MQ、Redis 扫描或第二控制面掩盖。
