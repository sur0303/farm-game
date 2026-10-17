# A4 前置兼容性修复验证

日期：2026-08-13

基线：`c7063b45c6e045a84f9251c4dfda40b84722de39`

本次变更范围仅为 `cmd/rt2q-route-bootstrap/**`、`cmd/routectl/**`、`go.sum` 和本文件；未修改 ScalePlan core、A1/A1R/A2/A3。

## 修复记录

1. `rt2q-route-bootstrap` 调用 `SnapshotStore.Bootstrap` 时传入 `core.NestedControlOperationToken{}`。这是 mode=off 的无 coordinator token 零值；Redis adapter 将其编码为空 operation、epoch `0`，不会创建 bootstrap fence。测试用 miniredis 精确断言 fence key 不存在。
2. `routectl` 测试替身补齐当前 `RoutePlanServiceClient.GetPlan` 及请求/响应字段；恢复 `validPlanIDPrefix`，按当前服务端 `prefix + "/" + domain + "/" + shard` 组合为 `FARM_WORKER` 和最大 uint32 十进制后缀预留长度，并继续使用 `model.ValidatePlanID` 做 exact plan ID 验证。
3. `c7063b4` 已将 gorilla/websocket 从 `v1.5.3` 升至 `v1.5.4-0.20250319132907-e064f32e3674`，原 `go.sum` 仅有该版本的 `/go.mod` 校验。本次只补模块内容校验 `h1:JeSE6pjso5THxAzdVpqr6/geYxZytqFMBCOtn/ujyeo=`；未运行 `go mod tidy`，未修改 `go.mod`。
4. 当前 `RouteAssignmentManager` 要求 placement-exclusion controller；命令沿用现有 `HolderStore`/`HolderRegistry` 能力并在 claim 前登记 bootstrap target generations，保证既有生产 manager contract 在 focused test 中成立，未绕过 placement validation。

## 验证结果

以下命令均以远端 worktree 的 `developer_026` 身份执行，并通过 `runuser -u developer_026 -- ...` 进入：

```text
go test -count=1 ./cmd/rt2q-route-bootstrap ./cmd/routectl       PASS
go build -buildvcs=false ./cmd/rt2q-route-bootstrap ./cmd/routectl PASS
git diff --check                                               PASS
```

默认 `go build` 的源码编译未报错，但 Go VCS stamping 在该 worktree 环境返回 `error obtaining VCS status: exit status 128`；使用 `-buildvcs=false` 完成了实际两个命令包的 build 验证。无 go test 进程残留。
