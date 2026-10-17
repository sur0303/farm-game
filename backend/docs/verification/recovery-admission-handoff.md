# Recovery Admission 交接与验证

状态：`IMPLEMENTATION_PASS / REAL_E2E_AND_ROLLING_COMPAT_PENDING`

日期：2026-08-03

对应合同：

- `26 · 接入与会话详细合同`：Connection/Recovery/Join 三维状态、`FARM_INTERACTIVE` 屏障；
- `28 · CommandCatalog 与公网最小协议合同`：`requires_recovery_complete`；
- `29 · 内部 RPC 与可信调用上下文合同`：`recovery_complete_at_admission` 只能由 Gate 构造；
- `60 · 接入与会话测试及验收冻结合同`：H5 本地布尔值不能直接冒充 Gate 的可信准入事实。

## 1. 本轮关闭的代码缺口

此前 H5 会在本地阻止恢复期经营写入，但 Gate 对所有命令固定生成：

```text
recovery_complete_at_admission = false
```

因此实现与 CommandCatalog 不闭合：若直接启用 FirstHop 的恢复检查，所有经营写命令都会永久失败；若忽略
该字段，定制客户端又可绕过 H5 本地屏障。

本轮增加连接级恢复握手：

```text
新 connection_generation
→ Gate recoveryComplete=false

H5 并行完成
├── PlayerBootstrap
├── Pending Operation 首轮恢复
└── OwnFarm Snapshot + Catch-up + SubscriptionReady

H5
→ ClientRecoveryComplete ControlFrame

Gate（同一 SessionLane FIFO）
→ 校验 connection_id + generation
→ 校验 Session 仍 ACTIVE
→ 校验 Route Lease 未过期
→ 校验当前自有农场已经是 ActiveSubscription
→ Gate 内部 recoveryComplete=true

后续经营写命令
→ CommandCatalog RequiresRecoveryComplete
→ Gate 在 ADMITTED 点再次读取当前状态
→ TrustedCommandContext.recovery_complete_at_admission=true
→ FirstHop 按同一 CommandCatalog 复核
```

经营命令 Payload 不含恢复布尔值；客户端不能在每条命令中提交或覆盖
`recovery_complete_at_admission`。ControlFrame 也不携带 player/session/subscription 身份，Gate 只使用当前
Transport Fence 和自己的 ActiveSubscription 状态。

## 2. 公开协议与错误语义

新增：

```protobuf
FRAME_KIND_CONTROL = 7;

message ClientRecoveryComplete {}

message ControlFrame {
  oneof body {
    ClientRecoveryComplete client_recovery_complete = 1;
  }
}

ERR_RECOVERY_INCOMPLETE = 2006;
```

规则：

- ControlFrame 不是业务命令，不创建 `operation_id`、Workflow 或 Actor 写入；
- 同一连接代次重复发送标记是幂等操作；
- 新连接代次始终重置为 `false`，旧代次不能修改替换后的连接；
- 在当前自有农场 ActiveSubscription 成立前发送标记属于非法提前完成，Gate 拒绝；
- 恢复未完成的经营写命令返回 `CommandResult(REJECTED, ERR_RECOVERY_INCOMPLETE)`，不进入 FirstHop，
  也不应因为正常恢复等待而关闭连接；
- `GET_PLAYER_BOOTSTRAP`、`GET_OPERATION_RESULT`、Join 等恢复所需读/控制请求不要求恢复完成；
- 当前连接已经完成一次启动恢复后，普通 A→B 农场切换不重置连接级恢复状态；断线产生新
  `connection_generation` 时必须重新恢复。

## 3. 为什么没有额外 ACK

H5 的恢复完成标记与下一条业务命令使用同一条 WebSocket，Direct WebSocket Transport 对每个连接使用
一个有界 `commandQueue` 和一个串行 SessionLane Worker：

```text
ClientRecoveryComplete
→ Gate HandleBinary 完成并更新连接状态
→ 下一条 ClientCommand 才进入 HandleBinary
```

因此 H5 只在标记成功交给 WebSocket 后打开本地写屏障，不需要再等待一轮 Gate ACK。标记发送失败时，
H5 保持 `Recovering`，经营写入继续失败关闭并等待重连。

这个结论依赖 Transport 的同连接 FIFO。未来启用 TCONND + gateproxy 时，必须用真实平台探针证明同一
连接事件不会并行或重排；否则需要在 Adapter 中恢复串行 SessionLane，不能把并发帧直接调用 Gate。

## 4. 信任边界说明

本轮建立的是“受支持客户端的恢复协议 + Gate 服务端准入状态”，不是浏览器远程证明：

- H5 负责判断本地 PlayerBootstrap、Pending 恢复和 Reducer 是否完成；
- Gate 不相信业务命令中的客户端布尔值，只把一个经过当前连接/Session/Lease/ActiveSubscription Fence
  校验且按 FIFO 到达的控制标记转换为内部状态；
- Gate 至少独立证明 Join 服务端里程碑已经成立，ClientHello 后立即伪造标记不能打开写入；
- 即使定制客户端谎报本地 UI 已应用，Actor 权威、幂等 Receipt、版本检查和 Workflow 合同仍是业务
  正确性的最终边界，恢复标记不是资产授权或反作弊 Token。

若未来产品要求 Gate 对 Pending Operation 集合也做完全服务端证明，需要新增“服务端枚举当前玩家待恢复
操作并返回带 Fence 的 RecoveryEpoch”合同；当前 26～29 没有冻结该 API，本轮不擅自发明。

## 5. H5 行为

`FarmApp.tryCompleteRecovery()` 只有同时满足以下条件才调用 `WsClient.completeRecovery()`：

```text
WsClient.status == Recovering
JoinPhase == READY
自有农场 Snapshot 已应用
PlayerBootstrap 已应用
Pending Operation 首轮恢复完成
当前页面仍是恢复目标农场
```

`completeRecovery()`：

```text
若当前代次已经完成 → 幂等返回
发送 recovery-complete ControlFrame
发送成功 → recoveryBarrierComplete=true
重新计算 FARM_INTERACTIVE/InFarm
发送失败 → 保持 Recovering，不开放经营写入
```

## 6. CommandCatalog 准入矩阵

当前要求恢复完成的公开写命令：

```text
BUY_OFFER
SELL_ITEM
PLANT_CROP
HARVEST_CROP
WATER_CROP
BEGIN_THEFT
CLAIM_TASK
```

恢复读、Join/Leave、Snapshot 与 OperationResult 查询不要求该能力。新增公开命令时，Catalog 完整性测试
必须显式决定是否要求恢复完成，不能依赖命令枚举大小或默认值猜测。

## 7. 主要代码

后端：

- `api/proto/common/{frame,control,enums}.proto`
- `internal/gateway/command/gate.go`
- `internal/gateway/command/recovery_admission_test.go`
- `internal/gateway/commandcatalog/{catalog,catalog_test}.go`
- `internal/logic/trusted/{validate,validate_test}.go`

H5：

- `src/network/ws-client.ts`
- `src/app/App.ts`
- `src/protocol/{types,canonical-v02-adapter}.ts`
- `src/test/{canonical-v02-adapter,ws-client-recovery}.test.ts`

`src/protocol/generated/workflow/workflow.ts` 的同步变化来自当前后端 `workflow.proto` 已包含 Stable Workflow
ID 与原始 `operation_id`，不是本轮手写协议；H5 生成器以当前后端 Proto 全量确定性再生，因此一并提交。

## 8. 已覆盖验证

后端测试覆盖：

- 恢复前读请求可进入 FirstHop，并携带 `false`；
- 恢复前写请求被 Gate 显式拒绝，不进入 FirstHop；
- ActiveSubscription 前的提前标记不能打开恢复状态；
- 正常标记后写请求携带 `true`；
- 重复标记幂等；
- 新连接代次重置；
- 旧连接代次不能修改新连接；
- FirstHop 独立拒绝伪造/缺失的恢复完成上下文；
- Catalog 写/读矩阵完整。

H5 测试覆盖：

- ControlFrame 编码；
- 标记只发送一次；
- 标记先于后续写命令；
- 标记编码/发送失败时保持 Recovering；
- TypeScript 构建通过。

提交前执行：

```text
# backend
go test ./...
go test -race ./...
go vet ./...
git diff --check

# H5
npm test -- --run
npm run build
npm run audit:build
npm run validate:handoff -- \
  --environment staging \
  --h5-origin https://<real-h5-host> \
  --api-origin https://<real-api-host> \
  --ws-url wss://<real-ws-host>/ws
git diff --check
```

`validate:handoff` 必须使用部署系统实际分配的三个地址。本分支没有可证明的发布地址，因此不使用虚构域名
制造通过记录；单测、构建和 `audit:build` 已执行，部署参数校验留在真实 E2E/发布步骤。

## 9. 未完成与发布门禁

以下证据尚未完成，不能写成生产可用：

1. 真实 H5 + Direct WebSocket + 公司 Redis 的完整启动、早到写命令、断线重连 E2E；
2. 双 Gate SessionRoute/Lease/顶号/Drain 下的新代次重置 E2E；
3. TCONND + gateproxy 是否保持同连接 FIFO 的真实探针；
4. 混合版本滚动发布：新 H5 向旧 Gate 发送未知 ControlFrame 会断开；旧 H5 连接新 Gate 永远不会发送
   标记，写命令会持续 `ERR_RECOVERY_INCOMPLETE`。

当前只允许开发环境前后端协调升级。生产发布前必须二选一：

```text
A. ServerHello capability + Gate Feature Gate
   → 先发布识别 ControlFrame 但暂不强制的 Gate
   → 再发布发送标记的 H5
   → 观察覆盖率后开启强制准入

B. 升级新的 protocol_version
   → 新旧客户端分别进入明确版本处理链
```

不得在没有门禁的情况下直接滚动混跑。
