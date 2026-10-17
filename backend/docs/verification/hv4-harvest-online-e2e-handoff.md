# HV4：Harvest online E2E 交接

日期：2026-08-05

## 改动

- `farm-svr` 的公开 `HARVEST` 通过现有 FirstHop、CommandCatalog、TrustedContext、RouteFence、Placement 接入 `harvestonline.Coordinator`；不再走 legacy 单 Actor Harvest 快捷路径。
- 生产 Farm Participant 组合注入了 `COMMIT_HARVEST` 的 WorkRecord dependency proof 和 FarmChange observer；生产 Player Participant 组合注入了 `GRANT_HARVEST_REWARD` 的同一 WorkRecord/Farm result proof。
- `GET_OPERATION_RESULT` 读取同一 generic `WorkRecord`，同时支持 `PLANT` 与 `HARVEST` 的 `StoredOperationOutcome`。
- `farm-worker` 在同一进程、同一 generic recovery loop/lease/writer fence 下注册 `PLANT` 与 `HARVEST` handler；Harvest 恢复复用原 invocation 和 WorkRecord。
- 做了必要的窄适配：Harvest route-domain 接入现有 RouteFence，Farm/Scenario/Proof 共用 canonical `HarvestDecision` hash，公共 Plot 坐标保持 0-based；未新增 Hook、MQ、路由或 Workflow 框架。

## 组件测试证据

组件测试覆盖 Farm ingress、Player ingress、Harvest online/scenario、Participant proof、Harvest recovery，以及 worker 的共享 Plant/Harvest 组合。最终验收命令如下，结果应以本次 worktree 的命令输出为准：

```text
go test -count=1 ./internal/farm/ingress ./internal/player/ingress ./internal/harvestonline/... ./internal/scenario/harvest/... ./internal/worker/harvestrecovery/...
go test -race -count=1 ./internal/farm/ingress ./internal/player/ingress ./internal/harvestonline/... ./internal/worker/harvestrecovery/...
go vet -buildvcs=false ./cmd/farm-svr ./cmd/player-svr ./cmd/farm-worker ./internal/farm/ingress ./internal/player/ingress ./internal/harvestonline/... ./internal/worker/harvestrecovery/...
go test ./...
git diff --check
```

## 真实 Redis 进程证据

可重复执行命令：

```text
PO1_E2E_MODE=HARVEST go run ./cmd/po1-plant-e2e
```

该模式复用已验证的 PO1 进程组合：真实 Redis（本机 `redis-server`，或本地 Docker 的 `redis:7.4-alpine`/`redis:7`）、真实 `farm-svr`、`player-svr`、`farm-worker`；只有现有 `GatewayDownlink` 使用受控 capture sink。没有用 miniredis 冒充真实 Redis，也没有 mock Actor、Participant 或 WorkRepository。

一次成功运行的原始结论摘录：

以下 Redis 端口是该次运行分配的临时端口；重新执行时会随机变化。

```text
HV4 raw evidence: backend=real Redis (127.0.0.1:46241), processes=farm-svr/player-svr/farm-worker, mocks=GatewayDownlink capture only
HV4 raw evidence: planted crop_cycle_id=sid1_M7amxg3u1kHUgVczLR1eb1jPBwNhEOaED5Eq8VLNsO0 plot_version=2 waiting_for_maturity=9.991s
HV4 raw evidence: WorkRecord work_type=HARVEST work_id=sid1_sFfR2LA1kvgJpk9bdr70cTkP8_VTZJvd9jYtQdDaQGI Farm_step=APPLIED Player_step=UNKNOWN state=STEP_UNKNOWN version=5
HV4 raw evidence: interrupted public HARVEST response status=PROCESSING error=<nil> transport_error=<nil>
HV4 raw evidence: GET_OPERATION_RESULT original_command=HARVEST lookup_status=OPERATION_LOOKUP_PROCESSING
HV4 raw evidence: stale writer_epoch fail-closed error=rpc error: code = Internal desc = farm participant internal failure
HV4 raw evidence: stale lease_epoch fail-closed error=rpc error: code = FailedPrecondition desc = participant proof: WorkRecord has no live worker lease
farm-worker shared generic recovery enabled for PLANT and HARVEST
recovery round combined={Claimed:1 Resumed:1 Completed:1 Retried:0 LeaseLost:0 Skipped:0 Failed:0} legacy={Claimed:0 Resumed:0 Completed:0 Retried:0 LeaseLost:0 Skipped:0 Failed:0}
HV4 raw evidence: worker restart completed same WorkRecord; OperationResult=SUCCEEDED crop_cycle_id=sid1_M7amxg3u1kHUgVczLR1eb1jPBwNhEOaED5Eq8VLNsO0 quantity=500 farm_version=2
HV4 raw evidence: Player exactly-once inventory item=2001 +500 exp +10 task_definition=1 +1 codex +1 economy_version +1
HV4 raw evidence: FarmChange exactly once per subscriber version=2 changed_plot=0 crop_cycle_after=empty
HV4 raw evidence: same operation_id replayed identical result without state/change; different payload hash returned ERR_IDEMPOTENCY_CONFLICT; stale writer_epoch and lease rejected
HV4 HARVEST E2E PASS: real Redis -> Farm commit -> coordinator/process interruption -> PROCESSING result -> same WorkRecord worker recovery -> exactly-once state/change/replay/fence checks
```

这证明了成熟作物准备、Farm 先提交、协调进程退出/响应丢失、`PROCESSING` 查询、服务与共享 worker 重启、同一 WorkRecord 终态 `SUCCEEDED`，以及作物清空、库存/经验/任务/图鉴单次增加、FarmChange 单次发布、同 operation replay、payload conflict 和 stale fence fail-closed。

## 尚未完成的生产容量证据

本次没有宣称生产容量或高可用 sign-off：未覆盖压力/容量基线、长时间运行、跨分片多 Pod 并发、Redis 故障转移、RouteSnapshot 热迁移或完整公网 Gateway/WSS 生命周期。这些属于容量/平台证据，不是本 HV4 窄组合和真实 Redis 纵向 E2E 的结论。
