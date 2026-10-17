# W3B-0 · PlayerBootstrap、受信 Player 路由与公开 FarmChange 包装交接

日期：2026-08-03
状态：`READY_TO_COMMIT`
工作树：`farm-game-w3b`
分支：`codex/w3b-player-bootstrap-20260803`

## 1. 本批目标

为 W3A H5 提供独立玩家私有状态读取，并消除 Join/Farm Push 与当前合同之间的三个物理漂移：

```text
GET_PLAYER_BOOTSTRAP
→ PLAYER RouteDomain
→ PlayerActor只读Turn
→ PlayerBootstrapResult(private_state + economy_version)

FARM_CHANGE
→ FarmChangePush { change }

Player域路由键
→ TrustedCommandContext.session.player_id
→ 不信任客户端target_owner_id
```

## 2. 已实现

1. `CommandType.GET_PLAYER_BOOTSTRAP = 13`，并补齐 Proto、生成代码、Decoder 和 CommandCatalog；
2. PlayerIngress 在 PlayerActor Mailbox 内生成只读 Bootstrap，不创建业务 Receipt、不推进经济版本；
3. `GET_PLAYER_BOOTSTRAP/BUY/SELL/CLAIM_TASK` 的 ShardedLogicClient 使用 Gate 生成的受信 player_id 路由；即使客户端 target_owner_id 不同，也不会路由到其他 PlayerActor；
4. `GET_OPERATION_RESULT` 没有可靠 Farm Locator 时可回落到受信 Player 路由；
5. `FARM_CHANGE` 统一为 `FarmChangePush` 包装，Go 发送、PushSpec、TS 解码和黄金向量一致；
6. `FARM_SNAPSHOT` PushSpec 修正为 `farm.FarmSnapshotPush`，移除旧 DoubleLatch 标记；
7. 保留当前 Join ACCEPTED 的空 payload 边界，Player 私有状态不回流 FarmSnapshot。

## 3. 验证

```text
go test ./... -count=1
→ 全仓通过

go test ./internal/gateway/command ./internal/gateway/commandcatalog \
        ./internal/player/ingress ./internal/farm/ingress \
        ./internal/protocoltest -count=1
→ 全部通过
```

与 W3A 配对后，四服务镜像强制重建，真实 Playwright 账号/好友与破坏性恢复 E2E 2/2 通过。

## 4. 已知差异

1. Gate 生成的 `TrustedCommandContext.recovery_complete_at_admission` 仍恒为 false，业务 FirstHop 目前没有据此实施完整写准入；需要单独设计受信 RecoveryAdmission 信号。
2. 28 号最终 FarmFrame oneof/Typed Payload 尚未物理迁移；当前改动只增加命令和修正现有包装，不宣称 W11B-W 完成。
3. `GET_OPERATION_RESULT` 的 ResultDirectory、完整 Locator 与 RetryDirective 仍依赖 31/32 的权威存储实现。
4. 本批没有证明双 Gate、TCONND、200 人或 3000 万 DAU。

## 5. 版本纪律

- 本文件所在提交即实现基线；
- 后续变更必须以 Linux 已同步的 26～33 为语义源；
- 不得为迁就旧 H5 恢复 Join 内嵌私有状态；
- 任何生成文件变化必须与 `.proto` 同提交并通过 Go/TS 黄金测试。
