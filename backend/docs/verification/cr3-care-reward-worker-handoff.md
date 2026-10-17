# CR3 CareRewardOrder / farm-worker handoff

基线：`3804c5d511674137facd3bdc24d8f814fb271725`。

本提交只增加 source-attached `CareRewardOrder` 的派发/恢复组合层和现有
authority lease 的只读发现视图；没有修改 Farm/Player 的业务裁决、Proto、MQ
或服务拓扑，也没有把 Opportunity projection 加入奖励 Order。实现不依赖尚未
合入的 CR1/CR2 分支：Player 侧只通过已冻结的 Participant RPC 接口调用。

## 可靠性边界

```text
FARM SourceDueIndex
  -> bounded ListDueWork
  -> ClaimWork / accepted TTL + safety margin
  -> APPLY_CARE_REWARD / PLAYER RouteDomain
  -> Player terminal receipt/result proof
  -> same fence CompleteWork
```

- DueIndex 只做发现；SourceOrder/WorkClaim 才提供 payload、work version、subject
  和 lease fence。使用的是现有 `WorkLeaseStore.ClaimWork`、`RenewWorkLease`、
  `CompleteWork`，没有新增 Redis key 或 retry schema。
- Payload 按确定性 protobuf 解码，并校验 CareResolutionSnapshot、完整
  `care_resolution_hash`、OWNER/FRIEND 绑定、稳定 incident/order ID 和冻结规则。
- Worker 使用确定性的
  `CARE_REWARD + order_id + APPLY_CARE_REWARD + step_instance=1` invocation；路由
  每次尝试重新解析到 carer PlayerActor，并通过 `ResumeStep` 携带当前 lease、
  work version、epoch 和 route fence。
- RPC 错误或 `TIMEOUT_UNKNOWN` 不会 Complete，也不会换 invocation。下一次
  `ResumeStep` 由 Player ingress 的 receipt-first 入口解析同一 ParticipantReceipt；
  只有验证过 terminal outcome、typed result、result hash 和 committed record
  version 后才调用 CompleteWork。
- 每个 order 有独立 operation deadline；扫描和 claim 有界，候选批次和并发数有
  上限，lease safety margin 不足会先 Renew 或退避。坏 payload 不进入 Player，
  只延迟自身 DueIndex，不阻塞同批其它 order。
- DirectDispatcher 是来源进程内的有界快速尝试：读取现有 SourceOrder 后先调用
  Coordinator `ExecuteStep`，RPC/证明失败不 claim、不丢权威 Order；拿到 terminal
  proof 后才取得现有 WorkLease fence 并 Complete。队列关闭、进程崩溃、队列满或
  响应丢失均由 FARM SourceOrder + DueIndex 留给 farm-worker 恢复。

## 组合接缝

- `internal/worker/carerecovery` 提供 `NewWorker`、`NewDirectDispatcher` 和
  `Composition`。Direct 的来源调用方只需把完整 `workidentity.WorkAddress`
  入队；不能只传裸 order ID。
- `cmd/farm-worker` 增量装配 auth-v1 FARM bucket、现有 Redis WorkLease、PLAYER
  WorkerRecovery clients，并独立运行 Care recovery round；Plant/Harvest 的
  原有 recovery loop 未改业务行为。
- 基线没有 CR1 Farm Care authority commit 的生产 hook，也没有 CR2 Player Care
  handler；本提交不伪造这两个业务接缝。CR1 合入来源提交后，将其提交返回的完整
  WorkAddress 交给 `NewDirectDispatcher` 即可接上快速路径，失败仍由当前 worker
  接管。

## 验证

重点测试覆盖：单次成功、投递前失败/重启、Participant 已提交但响应丢失、
`TIMEOUT_UNKNOWN` 同 invocation 重试、lease fence/过期、并发双 worker、坏
payload 隔离、Direct RPC 失败保留 durable Order，以及 DueIndex 读取不抢 lease。

已运行：

- `scripts/test-unit.sh`（manifest，等价于 `go test ./...`）
- `go test -race ./internal/worker/carerecovery ./internal/storage/redis/authority`
- `go vet ./cmd/farm-worker ./internal/worker/carerecovery ./internal/storage/redis/authority`
- `go test -count=50 ./internal/worker/carerecovery`
- `git diff --check`

以上测试均不依赖 CR1/CR2 分支；未宣称 live Redis 验证。
