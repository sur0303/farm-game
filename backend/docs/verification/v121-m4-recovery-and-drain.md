# v1.21 M4 · 弱网恢复屏障与滚动摘流

日期：2026-07-29

## 1. 为什么改

已有 W0 能在断网后重连，但 Snapshot 一到就把页面标为 `InFarm`。这时玩家私有
资产、断线前正在看的好友农场和 IndexedDB 中未决操作可能尚未恢复；若开放写入，
客户端会在旧版本或错误目标上继续操作。服务端原 drain 也只发 1001 后固定等待，
`/readyz` 没有进入 not-ready，滚动时新连接可能再次进入旧实例。

不引入 RecoveryBarrier 的后果是假 READY；不引入动态 readiness 的后果是旧 Pod
无法真正摘流。Session Handoff 能减少 Snapshot，但不能修复这两个正确性问题，
所以本阶段不引入。

## 2. 客户端 W1

连接状态增加 `Recovering`。开启手动屏障的生产客户端按以下顺序恢复：

```text
ServerHello
→ 查询 IndexedDB journal，并发出所有 GET_OPERATION_RESULT
→ Join 自己的农场，恢复私有 Wallet/Inventory
→ Join 断线前目标农场（自己或好友），应用权威 Snapshot
→ completeRecovery
→ InFarm，重新开放强写
```

恢复期间：

- UI 的购买、出售、播种、浇水、收获和偷菜入口禁用；
- `CommandService.execute` 的发送边界再次要求 `InFarm`；
- FarmChange 缺口仍触发 Join/Snapshot，而不是猜测；
- reconnect UI 显示离线状态、尝试次数和 retry delay；
- 服务端 DrainNotice 的稳定抖动 hint 会覆盖下一次普通 full-jitter delay。

## 3. 认证续期

新增 `POST /v1/auth/renew` 与 `/api/v1/auth/renew`。它验证 HttpOnly
`farm_session`、检查 authority/active epoch 仍为当前值，并签发新的单次
connection ticket。

续期不调用 `Authority.Begin`。网络抖动不是一次新登录，若每次重连都提升
`active_session_epoch`，会无意义地 fence 同一页面并制造多端竞争。

## 4. 未决结果语义

当前 Player/Farm Record 的结果热窗口是有界的，因此“热窗口中没找到”不能证明
原请求从未被接受。`ERR_SETTLEMENT_NOT_FOUND` 在 operation query 中保持
`UNAVAILABLE`，H5 保留 journal 并继续查询，不自动换 operation_id 重发。

M6 已把热窗口冻结为至少 7 天、窗口内最多 1024 条；达到上限时在业务状态变化前
明确返回 `ERR_RATE_LIMITED`，不再按 128 条静默淘汰。进行中的 Reservation 和
Settlement 不按时间删除。容量模型及拆表门见
[`v121-m6-capacity-and-observability`](./v121-m6-capacity-and-observability.md)。

## 5. Gate R0 基础

SIGTERM 后：

```text
SERVING
→ draining=true
→ /readyz=503，/healthz=200，新 Upgrade=503
→ DrainNotice 进入每连接既有 ConnectionWriter
→ 100ms flush budget
→ WebSocket Close 1012 Service Restart
→ 等待可配置 drain budget
→ HTTP shutdown
```

控制通知没有另开并发 socket writer，因此不会越过已经接受的 response/push。
`DrainNotice` 包含 `drain_id`、基于连接稳定散列的 `reconnect_after_ms` 和
deadline；不暴露 Pod 地址。旧客户端不识别通知时仍由 1012 触发普通恢复。

新增指标：

- `farmgate_ws_lifecycle_serving`
- `farmgate_ws_drain_total`
- `farmgate_ws_drain_sessions_remaining`

部署样例使用两个副本、`maxUnavailable: 0`、`maxSurge: 1`、PDB 和 60 秒
termination grace。该样例只冻结滚动合同；真正双 Gate 的 SessionRoute 与精确
downlink 验证属于 M5。

## 6. 验证证据

已通过：

```text
H5 vitest                         56/56 PASS
H5 TypeScript + staging build          PASS
gateway auth tests                     PASS
player/farm ingress tests              PASS
downlink DrainNotice/sequence test     PASS
websocket package compile              PASS
```

WebSocket 真实端口测试在受限 sandbox 中会因禁止 `listen` 而失败；这不是断言
失败。应在最终回归或可启动容器的环境执行：

```bash
go test ./internal/gateway/transport/websocket
```

M4 不宣称完成双 Gate 滚动发布；完成的是让客户端恢复具备正确屏障，并让单 Gate
具备可被编排系统安全摘流的协议和生命周期基础。

## 7. 2026-07-30 导航状态机回归修正

真实页面发现普通 `InFarm → Join 好友农场` 的 Snapshot 也会无条件打开
RecoveryBarrier，导致状态永久停在 `Recovering`，从而同时禁用好友浇水和返回
自己的农场。

修正后，WsClient 在发出 JoinFarm 前记录该 Join 是否属于连接恢复：

```text
Authenticated/Recovering → JoinFarm → Snapshot → Recovering（等待应用屏障）
InFarm                   → JoinFarm → Snapshot → InFarm（普通农场导航）
```

新增单测覆盖“完成初始 RecoveryBarrier 后访问好友农场仍保持 InFarm，并允许发送
WATER_CROP”，staging E2E 也增加“好友农场 → 我的农场 → 好友农场”的往返检查。

随后在“访问好友农场 → 模拟断线 → 恢复原好友农场 → 点击我的农场”中发现第二个
状态泄漏：`recoveryTargetOwnerId` 在恢复完成后仍保留好友 ID。普通导航到自己的
Snapshot 虽然成功，App 随即把它误判成恢复中间步骤，又自动 Join 回旧好友。

修正后的约束是：

```text
自动二段 Join 只允许发生在：
1. ws.status == Recovering；或
2. 首次分享接受仍有 pendingShareOwnerId。

目标 Snapshot 完成后：
recoveryTargetOwnerId = ownOwnerId
```

下次真正断线时，`Reconnecting` 状态回调会重新从当时的 `currentOwnerId` 捕获
恢复目标，因此清除一次性的恢复目标不会丢失“恢复到断线前好友农场”的能力。

验证结果：

```text
短链路：B 登录 → 好友农场 → 重连 → 我的农场       PASS（2.2s）
完整双浏览器：分享 → 浇水 → 偷菜 → 重连 → 往返      PASS（14.1s）
Staging 全量 E2E                                      4/4 PASS（31.8s）
H5 Vitest                                             58/58 PASS
```
