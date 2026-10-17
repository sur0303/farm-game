# Care + Theft Merge Integration Verification（2026-08-05）

## Merge 范围

- ours：`07368cc160b65617e99833c7c9f5e4618094a397`（Theft）
- theirs：`d7fbdee8125e216cbea67191bd0d90d5478fe875`（Care P1）
- merge base：`3804c5d511674137facd3bdc24d8f814fb271725`
- 集成分支：`codex/gameplay-care-theft-integration-20260805`

本次只处理 Care/Theft 合并冲突和两侧组合后暴露的装配断裂；没有新增玩法、修改冻结奖励/结算规则或更新大设计文档。

## 冲突选择

| 文件 | 语义合并结果 |
| --- | --- |
| `api/proto/player/player.proto`、`api/proto/task/task.proto` | 两侧 schema 一致，保留 Care 对 TaskDomain 独立真源和 bounded carrier 的完整注释；以 proto 为真源运行 `scripts/gen-proto.sh`，没有手工维护 `player.pb.go`/`task.pb.go`。 |
| `cmd/farm-svr/main.go` | 同时保留 Theft PRF、GuardSnapshot、ResultPublisher/Coordinator/Dispatch 和 Care direct dispatcher、Pair ActionAdmission。Care 与 Theft 共享 `openStores` 已构造并预热的同一 Pair authority，未用第二个 repository 覆盖装配。 |
| `cmd/farm-worker/main.go` | 同时注册 Theft Pair DueIndex recovery/result processor 与 Care SourceOrder/DueIndex recovery；两侧 client adapter、统计、错误处理和 Authority 模式 Care fail-closed 均保留。 |
| `cmd/player-svr/main.go` | 同时注册 Theft dependency verifier 与 Care source-order verifier/TaskDomain repository；保留 `GET_OPERATION_RESULT` 的 ResultDirectory projection 和 Work/Pair authority fallback。 |
| `cmd/po1-plant-e2e/main.go` | 合并 Care、Theft、Theft Pet 和既有模式；统一以真实 Pair authority 激活好友关系，避免重复 repository/重复激活；为启用 Permit Secret fail-closed 的 farm-svr 子进程注入仅限本次 E2E、按 run ID 隔离且长度合规的 secret。 |
| `internal/farm/ingress/service.go` | 同时保留 Theft admission/dispatch 与 Care ActionAdmission/order enqueue/public `CareResult` 出口，setter 和运行时字段不互相覆盖。 |
| `internal/friend/redis_authority.go` | ActionAdmission candidate 同时冻结 Theft 的 permit/workflow TTL、完整 dispatch/result order 绑定，以及 Care 的 `record_version` guarded refresh；版本冲突只做有界重读重试。 |
| `internal/logic/trusted/validate.go` | 使用 Care 的完整 Subscription Fence 实现，并明确覆盖 Care/Theft：farm/subscription identity 加完整 SessionPermit epoch/gateway/connection/generation/route revision。保留 Theft 的 ServiceLB/operation-result 目标策略。 |
| `internal/player/participant/plant.go`、`server.go` | 同时保留 Theft settlement/guard income 及 Care reward/TaskDomain command、payload/replay/capacity 语义；Care 只走 SourceOrder dependency proof，Theft 继续走通用 Work proof，均 fail closed。 |
| `internal/player/task/mapper.go`、`repository.go` | 两侧实现语义一致；保留 TaskDomain 不回退 legacy tasks、版本化配置和批量奖励推进语义。 |

## 自动合并接缝审计

- `internal/farm/actor/{actor,participant,registry}.go` 同时保留 Theft FarmActor 权威裁决、GuardSnapshot 热更新与 Care 原子 Farm commit + CareRewardOrder。
- `internal/plantonline/route.go` 同时路由 Care `APPLY_CARE_REWARD`、Theft Farm/Player participant commands；Gate ServiceLB 修复未回退。
- `internal/storage/authoritystore` 与 Redis Authority 同时保留 Care order subject=carer、ClaimWorkV3/deadline/retry fence，以及 Theft ResultDirectory/Pair result 生命周期。
- Pair `RESULT_PUBLISH_ORDER` 终态脚本仍按精确 charged bytes 释放 `pending_work_count`/`pending_work_bytes` 并删除 DueIndex；计数异常保持 due 且 fail closed。
- Permit Secret 仍为 Authority/默认生产模式 fail-closed，没有恢复已移除的默认 secret。

组合测试发现 Care 的 `ClaimWorkV3` 会把已领取 SourceOrder 持久化为 `REWARD_DISPATCHING`，而 Theft GuardSync source proof 原先只识别 `PENDING`。合并后显式接受该合法非终态，并把 Care 引入的 `lease_holder_boot_id` 纳入 GuardSync worker lease fence；生产组合测试使用匹配 boot 的 `ClaimWorkWithBoot`。终态和 `RECONCILE_REQUIRED` 仍拒绝执行。

## 验证结果

以下命令均在最终合并内容上执行：

```text
./scripts/gen-proto.sh
git diff --check
git diff --cached --check

go test -count=1 ./cmd/farm-svr ./cmd/farm-worker ./cmd/player-svr \
  ./cmd/ws-stress ./cmd/theft-hot-audit ./internal/authoritycomposition \
  ./internal/farm/domain ./internal/farm/actor ./internal/farm/ingress \
  ./internal/friend ./internal/gateway/command ./internal/gateway/commandcatalog \
  ./internal/gatewayapp ./internal/logic/trusted ./internal/plantonline \
  ./internal/player/ingress ./internal/player/participant ./internal/player/task \
  ./internal/scenario/theft ./internal/storage/authoritystore \
  ./internal/storage/redis/authority ./internal/theftonline \
  ./internal/worker/carerecovery ./internal/worker/theftrecovery \
  ./internal/workflow ./internal/workflow/participantproof

go test -count=1 ./...

go test -race -count=1 ./cmd/farm-svr ./cmd/player-svr ./cmd/ws-stress \
  ./cmd/theft-hot-audit ./internal/farm/actor ./internal/farm/ingress \
  ./internal/friend ./internal/gateway/command ./internal/gatewayapp \
  ./internal/logic/trusted ./internal/player/ingress \
  ./internal/player/participant ./internal/player/task ./internal/scenario/theft \
  ./internal/storage/redis/authority ./internal/theftonline \
  ./internal/worker/carerecovery ./internal/worker/theftrecovery \
  ./internal/workflow/participantproof

go vet ./...
```

结果：全部 PASS。

本地 Docker `redis:7.4-alpine`（Redis 7.4.10）真实多进程验证：

```text
PO1_E2E_MODE=CARE go run ./cmd/po1-plant-e2e
PO1_E2E_MODE=THEFT go run ./cmd/po1-plant-e2e
```

- Care：PASS。8 个好友经 Pair ActionAdmission 并发同一 WATER Incident，`1 SUCCEEDED + 7 NO_CHANGE`；CareRewardOrder `COMPLETED`，winner EXP `0 -> 1`。
- Theft：PASS。覆盖未成熟拒绝、成熟并发单次 2% slot、跨 Farm/Player 结算、ResultPublishOrder、响应丢失，以及 Farm/Player/Worker 重启后同一 FRIEND_PAIR workflow 恢复并完成全部 receipts。

未发现无法同时满足的 Care/Theft 真实语义冲突。
