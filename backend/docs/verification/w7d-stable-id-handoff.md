# W7D StableOpaqueID 与 WorkAddress 交接

日期：2026-08-03
状态：`IMPLEMENTATION_VALIDATED`
权威合同：27、29、31；实施顺序：70 的 W7D。

## 1. 本批完成范围

本批实现了合同 31 §8.1A 的确定性身份基础，并把当前 Plant 纵向链从临时字符串身份迁移到稳定身份：

```text
IdempotencyKey(subject_player_id, command_type, operation_id)
  ├─ workflow_id = StableOpaqueID(...)
  ├─ seed_reservation_id = StableOpaqueID(workflow_id)
  └─ crop_cycle_id = StableOpaqueID(workflow_id)
```

具体包括：

- Go `StableOpaqueID v1`、`OperationDigest` 及合同 31 的 Workflow/Order/Participant/Reservation/CropCycle/Care/Theft 派生器；
- `WorkRefV1`、`ReceiptRefV1` 的 LP + Base64URL 无填充编码、解码和 Fail Closed 校验；
- `WorkAddress` 的 Placement 形状校验和可替换 `BucketResolver`；
- H5 Web Crypto 实现与 Go 共用相同黄金向量；
- `WorkflowRecord.workflow_id` 与客户端 `operation_id` 正式分离；
- online Coordinator 与 farm-worker 均使用原 `operation_id` 查询 Authority Receipt、完成业务操作；
- Plant 的 Seed Reservation 与 CropCycle 使用由 `workflow_id` 派生的 Stable ID，重试和 Worker 恢复得到同一值。

## 2. 关键不变量

1. `workflow_id != operation_id`，但同一完整 IdempotencyKey 必须得到相同 `workflow_id`；
2. Stable ID 使用完整 32 字节 SHA-256，不截断，不使用 Route HashSeed；
3. 所有 canonical 整数都是无前导零十进制 ASCII，稳定枚举名只允许大写 ASCII；
4. WorkRef/ReceiptRef 解码后必须重新编码并完全相等，填充、截断、前导零和非规范枚举一律拒绝；
5. Worker 恢复不能把 `workflow_id` 当 Authority Receipt 的 `operation_id`；
6. Plant 在线路径、故障恢复路径和重试路径必须复用同一 Reservation/CropCycle ID。

## 3. 代码位置

后端：

- `internal/workidentity/identity.go`
- `internal/workidentity/ref.go`
- `internal/workidentity/address.go`
- `internal/workflow/identity.go`
- `api/proto/workflow/workflow.proto`
- `internal/workflow/{store,runner}.go`
- `internal/farm/ingress/service.go`
- `internal/farm/actor/actor.go`

H5：

- `src/protocol/work-identity.ts`
- `src/test/work-identity.test.ts`

## 4. 验证证据

后端 W7D worktree：

```text
go test ./... -count=1                                      PASS
go vet ./...                                                PASS
go test -race ./internal/workidentity ./internal/workflow \
  ./internal/farm/actor ./internal/farm/ingress -count=1    PASS
git diff --check                                            PASS
```

H5 W7D worktree：

```text
npx tsc --noEmit                                            PASS
npm test                                                    PASS（13 files / 74 tests）
build:web / build:staging / build:wechat                    PASS
audit:build                                                 PASS
```

Go 与 H5 均验证合同 31 的 Workflow、SourceOrder、ParticipantInvocation、OperationDigest、WorkRef 和 ReceiptRef 黄金向量。

## 5. 兼容与迁移边界

旧开发环境 Redis 中的 `WorkflowRecord` 可能仍满足：

```text
workflow_id == operation_id
operation_id 字段缺失
```

新代码会 Fail Closed，不会把旧记录猜测升级。接入现有 Redis 前必须二选一：

1. 开发环境清理旧 Workflow namespace；
2. W31-A 引入版本化 namespace/key prefix 后迁移到新前缀。

不得在读取时静默把 `operation_id = workflow_id` 作为生产兼容逻辑，否则会重新破坏本批身份分离。

## 6. 明确未提前实现的内容

- `BucketResolver` 的真实 HashSeed、BucketVersion 与 Redis KeyBuilder 属于 W31-A；
- CareIncident、TheftAttempt、TheftSettlement 的派生算法和黄金向量已实现，但业务记录接线分别属于 W32-F/W32-G；
- ParticipantInvocation ID 接入最终 Participant IDL 属于 W6D；
- 本批不宣称 Redis Cluster Slot、Lua、Lease、Unknown Resolver 已完成。

下一包应进入 W31-A：版本化 Namespace、KeyBuilder、Slot Golden Test 和公司 Redis 物理 Probe。
