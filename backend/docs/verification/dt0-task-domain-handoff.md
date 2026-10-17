# DT0 · TaskDomain 纯领域实现交接

日期：2026-08-03  
分支：`codex/dt0-task-domain-20260803`

## 结论

DT0 已完成任务系统的纯领域层，覆盖三个每日任务、单条成长任务链、零点惰性换日、累计收获/等级指标、领奖状态机、配置版本冻结、红点权威摘要和失败关闭语义。

本模块只维护任务真值，不维护网络命令幂等或跨参与者幂等：

- `operation_id`、`payload_hash`、同操作重放和 `StoredOperationOutcome` 由 `PlayerActor` 的统一 Operation Receipt 层负责；
- Harvest/Care/Theft 来源重放由 `PlayerActor` 的统一 Participant Receipt 层负责；
- 调用领域方法前必须完成对应 Receipt 预检查，任务状态变化与 Receipt/奖励在同一次 Player Authority Commit 中提交；
- `DomainState` 不包含 `ClaimReceipt`、`AppliedSource` 或无界历史集合。

## 代码范围

`internal/player/task/`：

- `config.go`：每日任务快照、按日 Assignment、成长任务链及校验；
- `period.go`：按游戏时区计算 `daily_period_id` 和下一次零点；
- `types.go`：有界任务真值、任务实例、奖励和查询视图；
- `outcome.go`：事实消费与领奖的确定性结果；
- `domain.go`：惰性换日、事实推进、成长链推进、领奖和查询；
- `domain_test.go` / `period_test.go`：状态机、版本、配置、时区、边界和失败关闭测试。

## 已固定行为

- 每日任务固定显示：登录、访问好友、好友互动，各完成一次即可领取；
- 换日由玩家下一次权威 Turn 惰性触发，不在零点批量扫描玩家；
- 前一日未领取任务直接失效，不补历史天数；
- 好友互动可以同时满足“访问好友”和“好友互动”，整个调用只增加一次 `task_state_version`；
- 成长任务只保留一个当前实例，领取后激活下一项；如果累计指标已满足，下一项立即 `CLAIMABLE`，但不会自动领取；
- 奖励在任务实例创建时冻结，不读取最新配置重新计算；
- 失败或 `NO_CHANGE` 不改变任务真值；成功变化每次调用最多增加一次版本；
- 配置、时区或成长链不可用时失败关闭；每日换日和成长激活先在候选状态中完成，全部校验成功后才发布，避免半切换。

## 后续接入约束

DT0 还没有接入 `PlayerActor`。接入 Agent 需要：

1. 把 `DomainState` 映射到 Player 权威存储的 `model:task`；
2. 在调用 `RecordHarvest`、`RecordFriendInteraction` 前检查 Participant Receipt；
3. 在调用 `Claim` 前检查 Operation Receipt；
4. 将任务变化、资产奖励、Receipt、StoredOperationOutcome 和必要 Outbox 放入同一次 Player Authority Commit；
5. 将 `Summary` 异步投影为任务红点，但投影不能参与完成或领奖裁决。

## 验证证据

已真实执行并通过：

```text
go test -race -count=1 -timeout=90s ./internal/player/task/...
go test -count=1 -timeout=180s ./...
go vet ./...
git diff --check
```

