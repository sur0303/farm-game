# W11A 公网 Join 核心 Wire 冻结实施记录

日期：2026-08-02
计划：`docs/design/70-w0-contract-alignment-implementation-plan.md` §4.2 + §5 W11A 卡片
合同依据：28 §2.2（CommandResult/JoinResultContext/Typed Payload）、§2.3（RetryDirective）、
§2.4（ServerPushEnvelope/push_context）、§6（CommandCatalog + §6.4 完整性 CI）；
决策 2 条件一（显式版本升级）。

## 1. 冻结内容

**CommandResultEnvelope（28 §2.2）**
- 新增 `ErrorDetail error = 20`、`RetryDirective retry_directive = 21`、`retry_after_ms = 22`、
  `command_type = 23`、`oneof result_context { FarmResultContext farm = 24; JoinResultContext join = 25; }`
- 删除旧 `error_code=3 / error_message=4 / farm_version=6`（reserved；错误走 ErrorDetail，
  Farm 版本不进公共泛化字段——§2.2 约束 6）
- `JoinResultContext = target_farm_id / subscription_id / join_attempt_id`；Join ACCEPTED 时
  由 Gate 组装（attempt 来自 Gate PendingJoin）
- `RetryDirective` 取 28 §2.3 九个值；内部 logicrpc 枚举同步对齐
- Gate 本地失败路径（`encodeCommandUnavailable`）现在真实填充 `retry_directive`
  （commandcatalog `DecideRetryDirective` → 公网枚举映射）

**ServerPushEnvelope（28 §2.4）**
- `oneof push_context { session=10; player=11; farm_subscription=12; }`，
  `FarmSubscriptionPushContext = target_player_id/target_farm_id/subscription_id`（三字段，
  Context 不携带业务版本）
- 删除旧平铺字段 `subscription_id=2 / actor_activation_epoch=3 / presence_epoch=4 /
  farm_version=5`（reserved；fence 归内部 DownlinkItem，版本归 Typed Payload）
- KICK/DRAIN_NOTICE → session 域；FRIEND_ROSTER_CHANGED → player 域；
  农场类 → farm_subscription 域（`push_type=6` 沿用旧号 [设计]，32 号评审统一）

**Typed Payload（28 §2.2，farm_view.proto）**
- `FarmSnapshotPush{join_attempt_id, snapshot_version, snapshot}`
- `FarmDeltaBatchPush{join_attempt_id, from_version, through_version, deltas}`（含连续性约束注释）
- `SubscriptionReadyPush{join_attempt_id, ready_through_version}`
- `RejoinRequiredPush{reason, invalid_from_version}` + `RejoinReason` 五值
- PushType 新增 `FARM_DELTA_BATCH=20 / SUBSCRIPTION_READY=21 / REJOIN_REQUIRED=22`；
  当前生产端仍走旧双锁存器类型（W2/W3 切换），`RESYNC_REQUIRED` 标记过渡

**版本与号段 [设计]**
- `FarmFrame.protocol_version` 2→3；v2 客户端被 Gate 显式拒绝并计
  `farm_gate_command_protocol_version_rejected_total{version}`
- Join 相关字段占 20~39 段；40~99 整段保留给 W11B；`ClientCommandEnvelope`
  旧 5~8（target_owner_id/expected_*）保留不建新语义，W11B 物理删除

**CommandCatalog（28 §6/§6.4）**
- 新 `pushspec.go`：每个公开 PushType 的静态 PushSpec（PayloadType + Scope）
- `ValidatePushCatalog` / `ValidateCommandCatalog` 完整性自检 + CI 测试
  （枚举未注册即失败）+ Scope 归属表测试

**跨端黄金向量**
- Go（`internal/protocoltest/envelope_test.go`）与 H5（`src/test/w11a-golden.test.ts`）
  共用同一组固定字节：FARM_CHANGE push（farm_subscription context）、Join ACCEPTED
  （JoinResultContext）、错误结果（ErrorDetail）。两端各自解码断言字段一致；
  编码器字段发射顺序差异（Go 按内部字段表、ts-proto 按字段号升序，同为合法 protobuf）
  以语义往返替代逐字节相等。

## 2. 过渡边界（明确不做什么）

- Join 握手链路仍是双锁存器（ACCEPTED + FARM_SNAPSHOT），W2/W3 才切换到
  Arm/Ready 链；新 Typed Payload 与 PushType 已入 IDL 但暂无生产者
- Join ACCEPTED 的 result payload 仍携带 `FarmCommandResult{subscription_id,
  private_state}` 作为过渡载体（28 §2.2 约束 1 要求 payload 为空 + private bootstrap
  独立——随 W11B/W3 完成）；H5 已改为优先从 JoinResultContext 读 subscription_id
- 结果里的公共 `farm_version` 停止填充；H5 农场版本只认 FarmChange/Snapshot（本就如此，
  farm-store 不消费 result.farmVersion）

## 3. 验收证据

- Go：`go build/vet/test ./...` PASS；`go test -race`（gateway/...、farm/ingress）PASS
- CommandSpec/PushSpec 完整性 CI 通过；`protocol_version_rejected_total` 已注册
- H5：Vitest 64/64（含 3 个跨端黄金向量）；staging/web/wechat 三构建 + audit 通过
- `verify-s2.sh` PASS
- 四容器（公司 Redis，protocol v3）真实 E2E：Web 1/1、Staging 5/5 PASS
- 旧版本客户端验证：v2 帧被 Gate 拒绝（codec/websocket/gate 测试覆盖）

## 4. 改动面

farm-game：envelope.proto、farm_view.proto、logic_service.proto（枚举对齐）+ gen、
`internal/gateway/protocol/codec.go`（v3）、`internal/gateway/commandcatalog/`
（版本 + pushspec.go/pushspec_test.go）、`internal/gateway/command/gate.go`
（结果组装 + JoinResultContext + 版本拒绝指标）、`internal/gateway/downlink/service.go`
（推送 context 化）、`internal/farm/ingress/downlink.go`（pushFrame 重写）、
ws-stress/dualgate-e2e、9 个测试文件、protocoltest 黄金测试。

classic-farm-h5：`src/protocol/canonical-v02-adapter.ts`（v3 + ErrorDetail +
context 读取 + 停填 farmVersion）、`src/protocol/generated/`（重新生成）、
`src/test/canonical-v02-adapter.test.ts`、`src/test/canonical-frame.test.ts`、
`src/test/w11a-golden.test.ts`（新）。
