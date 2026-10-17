# 21 · Direct WebSocket 接入与 Tlog 流水冻结

状态：`v1.1-production-contract-freeze-candidate`  
日期：2026-07-29  
范围：接入 Transport 决策、连接恢复、Tlog/TGLog 流水边界。本文不改变
`PlayerActor / FarmActor`、双 Route Domain、四部署单元和权威存储合同。

## 1. 冻结结论

### 1.1 P0 默认接入

```text
H5 / 微信小程序
→ HTTPS Login
→ connection_ticket + wss_url
→ 公网 DNS
→ CLB / TLS
→ farm-gate Direct WebSocket
→ Gate Core
→ player-svr / farm-svr
```

P0 对公网默认使用 `Direct WebSocket`。公网 DNS 解析到 CLB；TLS 在 CLB 终止，CLB
把受信私网 WebSocket 流量转发给 farm-gate。P0 不把 WAF 放进已经冻结的主链，未来若
真实安全策略要求 WAF 代理 WSS，必须作为独立 Deployment Profile 完成兼容、容量和
Idle Timeout 探针后再显式启用。

farm-gate 自己承担：

- WebSocket Upgrade；
- ClientHello 认证和一次性 Ticket 兑换；
- PRE_AUTH 连接保护；
- Session 绑定；
- FarmFrame 解析；
- 命令限流；
- ConnectionWriter 背压；
- 旧连接 Fence。

TCONND + gateproxy 保留为平台候选
Transport Adapter：

```text
H5 / 微信小程序
→ TCONND(BY_WS)
→ gateproxy
→ farm-gate Gate Core
```

两个 Transport 复用同一个 Gate Core 和业务协议。一个环境、一个发布版本只能显式启用一种
公网 Transport，不做运行时自动回退。TCONND 路径只有在真实 BY_WS 消息边界、下行确认、
心跳、QueueFull 和故障恢复探针通过后才能进入生产候选。

### 1.2 P0 部署单元不增加

```text
farm-gate
player-svr
farm-svr
farm-worker
```

Direct WebSocket 是 `farm-gate` 进程内的 Transport 实现，不是新服务。`farm-tlog` 是共享库，
`TlogExport` 和 `TlogRelay` 是 `farm-worker` 内的 Role，也不新增第五个服务。

### 1.3 Tlog 的定位

Tlog 是结构化经营流水，用于经营分析、对账辅助和问题追踪；它不是：

- Player/Farm 权威状态；
- 幂等或防重依据；
- Workflow 恢复来源；
- Actor 崩溃后的恢复快照；
- 业务成功的同步依赖。

业务成功不得等待 TGLog，TGLog 故障不得回滚或拒绝玩家操作。

## 2. Transport 中立边界

```text
farm-gate/DirectWebSocketTransport ─┐
                        ├→ Gate.HandleBinary
TconndGateproxyAdapter ─┘
```

两条 Transport 都只负责：

- 建连、断连和 `connection_id + transport_generation`；
- 一个 Binary Message 对应一个完整 `FarmFrame`；
- 连接级帧大小、超时、流控和有界下行；
- 将 ClientHello/Command 交给 Gate Core；
- 将 Gate Core/Logic 的 Response 与 Push 写回连接。

Transport 不得访问 Tcaplus、Actor、Workflow 或业务 Repository。

## 3. Direct WebSocket 生产合同

### 3.1 外部协议

| 链路 | 合同 |
|---|---|
| 登录 | HTTPS + JSON；H5 的 uint64 ID/版本使用 string |
| P0 公网链路 | 公网 DNS → CLB/TLS → 私网 farm-gate；farm-gate 不直接暴露公网 |
| 在线连接 | 外部 `wss://`；TLS 在 CLB 终止，CLB 到 Pod 使用受信私网 `ws://` 或平台确认后的私网 TLS |
| 应用帧 | 一个 WebSocket Binary Message = 一个 Protobuf `FarmFrame` |
| 首帧 | 必须是 `ClientHello(connection_ticket)` |
| 业务幂等 | `operation_id`；不能依赖 TCP/WebSocket 重传 |
| 连接防旧写 | `connection_id + transport_generation + session_epoch` |

`connection_seq/server_seq` 用于连接内单调性、重放诊断和缺口发现，不替代
`operation_id`、Receipt 或 Workflow。

#### 3.1.1 连接状态机

```text
TCP_ACCEPTED
→ WS_UPGRADED / PRE_AUTH
→ AUTHENTICATED
→ ROUTE_BOUND
→ ACTIVE
→ DRAINING
→ CLOSED
```

- `TCP_ACCEPTED`：仅进行 Upgrade 速率、Host 和来源检查；
- `PRE_AUTH`：WebSocket 已建立，但 Ticket 尚未兑换；不能进入业务路由；
- `AUTHENTICATED`：一次性 Ticket 已成功兑换并取得可信 player/session；
- `ROUTE_BOUND`：SessionRoute 已按 `session_epoch + route_revision` 成功绑定；
- `ACTIVE`：ServerHello 已进入当前 ConnectionWriter，允许业务命令；
- `DRAINING`：停止接收新业务命令，只发送必要结果或重连控制帧；
- `CLOSED`：释放连接、队列和 SessionRoute；旧 generation 永久失效。

新设备或新连接绑定成功时，旧连接即使仍能发送字节，也必须在 SessionAdmission 和
Downlink 两侧被 `session_epoch/transport_generation` Fence。

#### 3.1.2 PRE_AUTH 硬限制

以下配置名和失败动作属于合同；初始数值由 Linux + CLB 探针回填：

```text
client_hello_timeout_ms
max_pre_auth_connections_per_ip
max_pre_auth_connections_per_pod
max_pre_auth_frames
max_pre_auth_bytes
websocket_upgrade_rate_limit
ticket_redeem_rate_limit
```

PRE_AUTH 只允许一个合法 Binary `ClientHello` 以及必要的 Ping/Pong/Close 控制帧。收到
业务命令、文本消息、超限消息、第二个 ClientHello 或超时，立即关闭连接并记录低基数原因码。

#### 3.1.3 消息边界、压缩与来源校验

```text
一个 WebSocket Binary Message
= 一个完整 FarmFrame
```

- 不允许一个 FarmFrame 跨多个 WebSocket message；
- 不允许一个 WebSocket message 打包多个 FarmFrame；
- 文本消息一律拒绝；
- P0 关闭 `permessage-deflate`；
- 同时限制 WebSocket message、FarmFrame 和解码后 payload 大小；
- 校验 Host/SNI；Browser H5 必须匹配 Origin Allowlist；
- 微信小程序缺少标准浏览器 Origin 时，按 `client_type + ticket audience + 合法域名` 校验，
  不能把“没有 Origin”解释成全局允许；
- Ticket 只能放在 Binary ClientHello 中，不得放入 URL Query；
- farm-gate 只信任来自已知 CLB 网段或受信代理协议的客户端 IP 信息，忽略公网伪造的
  `X-Forwarded-For`。

### 3.2 安全与流控

生产必须具备：

- Host/SNI 与按客户端类型区分的 Origin/域名合同；
- 安全组只接受 CLB 来源，只信任受控 CLB 注入的转发信息；
- 一次性 ticket、短 TTL、Redeem 防重放；
- Hello 超时、空闲超时和应用心跳；
- Binary-only、最大帧和单条最大解码预算；
- 每 IP、每 player、每 ticket 的建连限流；
- 每连接 item/byte 双上限 FIFO；
- 慢连接只淘汰自己，不能拖住同 shard 的其他连接；
- 压缩默认关闭，除非单独完成内存与压缩攻击验证；
- Pod 连接水位、Readiness、Drain 和重连退避。

心跳和基础设施必须满足：

```text
application_ping_interval
pong_timeout
connection_idle_timeout
write_timeout
max_connection_age

CLB idle_timeout
> application_ping_interval + pong_timeout + network_jitter_budget
```

`max_connection_age` 到期只触发带抖动的优雅重连，不能让全部连接在同一时间重建。

### 3.3 下行链

```text
同步响应：
Gate Core
→ ConnectionWriter
→ WebSocket WritePump
→ Client

异步推送：
Logic DownlinkDispatcher
→ GatewayDownlinkService.DeliverBatch
→ ConnectionRegistry
→ 每连接有界 ConnectionWriter
→ WebSocket WritePump
→ Client
```

同一连接的 Response 和 Push 必须共享一个单调 `server_seq` 和一个物理 WritePump。
`QUEUE_ACCEPTED` 只表示进入 farm-gate 的有界队列，不表示浏览器已经收到或展示。

ConnectionWriter 同时限制：

```text
max_queue_items
max_queue_bytes
max_oldest_age_ms
max_single_message_bytes
```

| 消息类型 | 队列过载语义 |
|---|---|
| `CommandResult` | 不随意丢弃；无法入队时关闭连接，客户端按相同 operation_id 查询或恢复 |
| `FarmSnapshot` | 只允许在相同 connection generation + subscription 下由更新快照替代旧快照；同时废弃旧增量并要求从新版本继续 |
| `FarmChange` | 只合并连续版本；出现版本空洞时发送/记录 `RESYNC_REQUIRED` |
| 普通 Hint、红点、曝光 | 可以合并、覆盖或受控丢弃 |
| `ReconnectRequired` 等 Control | 高优先级保留；仍无法入队时直接关闭连接 |

保护顺序固定为：

```text
合并可合并消息
→ 丢弃可丢 Hint
→ 标记 RESYNC_REQUIRED
→ 超过硬上限关闭当前连接
```

不能为保护慢连接而阻塞 Actor、BackendPool 或其他连接。

### 3.4 断线与发布

```text
连接断开 / Pod Drain
→ 旧 generation 失效
→ 客户端通过已有平台凭证静默刷新 ticket
→ 新 WSS + ClientHello
→ 新 session/generation
→ JoinFarm
→ Snapshot + JoiningBuffer Catch-up
```

P0 不实现透明 Socket 迁移，也不要求用户手工重新输入账号。连接恢复依靠重新认证、重新
Join 和最新 Snapshot，不依赖 TCONND Handoff。

### 3.5 当前实现进入真实压测前的阻塞项

1. 异步 `ConnectionWriter` 必须接到真实 WebSocket WritePump，不能只入队不消费；
2. 同一 Gorilla WebSocket 连接只能有一个业务 Writer，Ping/Close 也必须遵守并发合同；
3. 不把逐条 `WriteMessage` 描述成“一次批量 syscall”；
4. 未实际接入读取路径的 `sync.Pool` 不计为已完成优化；
5. 共享 Writer shard 必须证明单慢连接不会造成同 shard Head-of-Line Blocking；
6. 压测必须发送合法 ClientHello、JoinFarm、Snapshot、业务命令和广播；
7. 客户端 `WriteMessage` 返回耗时不能当作业务 RTT 或服务端 P99；
8. 容量按 3000 万 DAU 的峰值 CCU、消息率、广播扇出和带宽建模，不能从 1 万连接线性外推。

P0 优先使用容易证明正确的每连接有界 FIFO + 单 WritePump。只有真实 Profile 证明 Writer
goroutine 或内存成为瓶颈后，才启用共享分片 Writer。

生产接入还必须通过：

```text
BLOCKER-P0A-01  PRE_AUTH状态、ClientHello超时和未认证连接容量保护
BLOCKER-P0A-02  Binary FarmFrame边界、最大帧、解码上限和压缩策略
BLOCKER-P0A-03  ConnectionWriter item/byte/age上限、消息优先级、合并和关闭语义
BLOCKER-P0A-04  CLB Idle Timeout、心跳、Drain、重连风暴和容量探针
BLOCKER-P0A-05  Host/SNI/Origin/Trusted Proxy、TLS终止和公网绕过防护
BLOCKER-P0A-06  connection generation、SessionRoute CAS和新旧连接Fence
```

这些阻塞公网生产验收，不阻塞先实现 S2 纵向骨架；实现期使用配置化保守默认值，探针后
回填生产值。

## 4. Tlog 分级

### 4.1 A 级：关键资产和结算流水

不采样，必须存在可重放的业务侧来源：

- 金币、种子、作物、道具变化；
- 邮件附件和任务奖励到账；
- 偷菜作物到账、实际宠物罚金、农场主金币收入；
- 付费货币、永久奖励；
- 跨玩家 Workflow 最终结算。

```text
Authority State
+ Receipt / Settlement
+ immutable AuditFact
→ 可靠提交
→ 来源Outbox/EventDelivery
→ farm-worker TlogExportConsumer
→ InsertTlogExportRecordIfAbsent
→ 独立Export Spool
→ farm-worker TlogRelay
→ TGLog
```

`InsertTlogExportRecordIfAbsent(flow_id, payload_hash)` 成功才是 A 级从业务事件域到
Tlog 导出域的可靠交接点。TGLog SDK 内存入队不算可靠交接。TGLog 投递允许至少一次；
每行携带稳定 `flow_id`，TDW 规范查询或下游物化层按 `flow_id` 去重。

### 4.2 B 级：核心玩法流水

成功结果从已经提交的 DomainEvent、AuditFact 或 Outbox 派生，不能由 Handler 在提交前
临时记录“成功”；已提交的 B 级成功复用 TlogExportConsumer/Export Spool，但使用独立于
A 级的保留和容量预算。普通失败可走 Pod 内 BestEffortSink 并采样：

- Buy/Sell、Plant/Harvest；
- Steal/Care 裁决；
- 好友建立、删除和拉黑；
- 邮件投递、读取、过期；
- 任务状态变化。

### 4.3 C 级：展示和点击

通过 Pod 内存有界队列批量发送，允许采样和受控丢失：

- 打开好友列表；
- 右下角推荐曝光和点击；
- 查看邮件正文；
- 页面进入和活动入口点击；
- 普通 `NOT_MATURE`、`NO_CHANGE` 提示。

队列满时直接丢弃并增加指标，不能阻塞 Gate、Read API 或 Actor。

## 5. 持久化等级与 Tlog

### 5.1 ACTOR_SOFT

```text
PlayerActor：
  资产变化
  + SoftReceipt
  + PendingAuditFact
  + economy_version
→ 同一 Economy Snapshot

Snapshot Flush 成功
→ AuditFact 才可导出
```

Flush 前崩溃时，软资产和对应最终资产流水一起回滚；不能出现“TGLog 已记录成功，但权威
资产因 RPO 回退”的矛盾。

### 5.2 LOCAL_DURABLE

例如邮件领取：

```text
奖励资产
+ MailClaimReceipt
+ MailRewardAuditFact
→ 同一 PlayerEconomyRecord 可靠提交
→ 提交成功后异步导出
```

### 5.3 WORKFLOW_DURABLE

例如偷菜：

```text
FarmDecisionFlow       FarmActor 已提交的目标侧裁决
Item/CurrencyFlow      PlayerActor 已提交的本地资产变化
TheftSettlementFlow    Workflow 最终终态
```

经营报表中的“成功偷菜次数”只统计最终 `TheftSettlementFlow(SUCCEEDED)`；参与者的资产
流水不能再次计为一次偷菜。

## 6. 产生方与语义

| 流水 | 事实产生方 | 依据 |
|---|---|---|
| MoneyFlow / ItemFlow | PlayerActor | 资产 + Receipt + AuditFact |
| Plot/Crop/FarmActionFlow | FarmActor | Farm 权威提交 |
| WorkflowSettlementFlow | farm-worker | WorkflowRecord 终态 |
| FriendshipFlow | FriendshipDomain | canonical FriendRelation |
| MailLifecycleFlow | PlayerMail | MailEntry / MailReadState |
| MailRewardFlow | PlayerActor | 资产 + MailClaimReceipt |
| TaskStateFlow | PlayerActor/TaskDomain | TaskState |
| CodexUnlockFlow | PlayerActor/PlantCodexDomain | PlayerCodexCollection + CodexUnlockReceipt |
| CodexCollectionCompleteFlow | PlayerActor/TaskDomain | Codex里程碑任务状态 + RewardReceipt |
| Login/Logout/Kick | SessionAuthority | Session 状态转换 |
| 页面曝光/点击 | Gate/Read API | 最佳努力行为 |

Gate 不能因为响应进入队列就替 Actor 记录业务成功。PlayerMail 也不能在附件尚未由
PlayerActor 可靠提交时记录“奖励已到账”。

## 7. P0 流水与统一 ID

P0 只定义：

- `FarmMoneyFlow`
- `FarmItemFlow`
- `FarmActionFlow`
- `WorkflowSettlementFlow`
- `FriendshipFlow`
- `MailFlow`
- `PlayerSessionFlow`

统一关联字段：

```text
flow_id          每条流水的稳定唯一 ID
correlation_id   同一次业务的多条流水关联
source_fact_id   可重放来源事实的稳定 ID
operation_id     客户端幂等操作
workflow_id      跨权威流程
line_key         同一事件内的资产行
```

```text
Outbox来源：
  source_fact_id = event_id

Receipt来源：
  source_fact_id = receipt_type + receipt_id + receipt_revision

Workflow来源：
  source_fact_id = workflow_id + terminal_revision

Settlement来源：
  source_fact_id = settlement_id + settlement_revision

flow_id = DeterministicID(
  producer_namespace,
  source_fact_id,
  flow_schema,
  line_key
)
```

`line_key` 必须稳定描述同一来源事实中的逻辑流水行，例如
`currency:COIN`、`item:SEED_1001`、`shop_counter:SHOP_01:ITEM_1001`。

```text
相同flow_id + 相同payload_hash
→ 幂等重复

相同flow_id + 不同payload_hash
→ FLOW_ID_CONFLICT
→ 禁止覆盖并告警
```

Mapper 只能把不可变 AuditFact 映射成 Tlog 行，不能延迟后回查当前钱包或库存，否则
`before/after` 已经不是行为发生时的值。

## 8. 存储与导出

| 数据 | 位置 |
|---|---|
| C 级与普通失败 B 级 | 当前 Pod 内存有界队列 |
| A 级恢复来源 | 原有 Tcaplus Authority / Receipt / Workflow / Outbox / AuditFact |
| A 级与已提交 B 成功导出 Spool | P0 使用 Tcaplus `TlogExportRecord`，按等级设置保留；未来可替换为通过可靠性探针的持久 MQ Topic |
| 最终流水 | TGLog，后续进入 TDW/经营分析 |
| Redis | 不保存 Tlog |
| 本地磁盘 | 生产不作为恢复依赖 |

Authority 不在玩家请求同步路径中额外写 ExportRecord。它只可靠提交业务事实、Receipt、
AuditFact 和必要 Outbox。TlogExportConsumer 异步消费来源事件，并在 ACK 来源
EventDelivery 之前执行 `InsertTlogExportRecordIfAbsent`。因此新增 Spool 会增加后台写入，
但不会把 TGLog 延迟放进 Actor turn，也不会在 TGLog 故障时让 Player/Farm 热 Aggregate
中的 PendingOutbox 无界增长。

ExportRecord 可以保存紧凑结构化 payload，或者保存对不可变、保留期受约束来源事实的引用；
若使用引用，来源事实不得早于 ExportRecord 清理。Spool 只是临时导出队列，不是资产账本。

## 9. TGLog 故障语义

```text
B 级普通失败 / C 级：
队列达到 item/byte 上限
→ 采样或丢弃
→ 游戏继续

A 级 / 已提交B级成功：
TGLog 不可用
→ 独立Export Spool积压
→ Worker 退避重试
→ 游戏继续
```

不能因为 TGLog 故障拒绝 Buy、Harvest、Steal、MailClaim 或 TaskClaim。积压超过预算时，
先降低/丢弃 C 级、采样普通失败 B 级、扩容 TlogRelay；已提交 B 成功和 A 级继续进入
Spool，不得丢弃业务权威。

## 10. 容量和故障预算

```text
tlog_rows_per_second
= Σ(业务峰值QPS × 每操作平均Flow行数 × 记录比例)

tlog_bytes_per_second
= Σ(flow_rows_per_second × average_encoded_bytes)

export_spool_growth_bytes
= (A级bytes_per_second + 已提交B级成功bytes_per_second)
  × TGLog最长不可用秒数
```

容量报告必须分别给出：

- A/B/C 峰值 rows/s；
- 各 Flow P50/P95/P99 编码字节；
- 每种操作平均和最大 Flow 行数；
- SDK 队列 item/byte 上限；
- TGLog 不可用 1 小时、6 小时、24 小时的 Spool 增长；
- 单 TlogRelay Pod 的读取、发送与清理吞吐；
- TDW 按 flow_id 去重的计算和存储成本。

固定过载顺序：

```text
降低C级采样
→ 丢弃C级
→ 采样普通失败B级
→ 保留已提交B级成功
→ 始终优先保留A级
→ 扩容TlogRelay
→ A级Spool达到保护线时告警并扩容存储/缩短非关键保留
```

Spool 配额必须独立预算，不能无界吃满 Tcaplus；但不能通过拒绝资产操作来保护 Tlog。

## 11. 指标和隐私

```text
tlog_best_effort_queue_items
tlog_best_effort_queue_bytes
tlog_best_effort_drop_total
tlog_export_pending_count
tlog_export_oldest_age
tlog_send_retry_total
tlog_sdk_queue_bytes
tlog_sdk_drop_total
tlog_flow_id_conflict_total
tlog_batch_items
tlog_batch_bytes
tlog_send_latency
```

`player_id/farm_id/workflow_id` 不作为 Prometheus label。Tlog 禁止写入 Ticket、Token、
Permit、签名、OpenID、邮件正文、完整附件 JSON、FarmSnapshot、好友列表和调用栈。

## 12. 冻结合同

```text
HC-TRANSPORT-01  P0主链为公网DNS→CLB/TLS→farm-gate Direct WebSocket。
HC-TRANSPORT-02  TCONND+gateproxy仅为显式候选；同环境只启用一种公网Transport。
HC-TRANSPORT-03  farm-gate负责Upgrade、PRE_AUTH、Ticket、Session、FarmFrame、限流、背压和旧连接Fence。
HC-TRANSPORT-04  PRE_AUTH只能接收一个Binary ClientHello和必要控制帧，并受超时、连接数和字节硬限制。
HC-TRANSPORT-05  一个Binary WebSocket message等于一个完整FarmFrame；文本、多帧打包和P0压缩均禁止。
HC-TRANSPORT-06  CLB idle_timeout必须大于应用心跳窗口；Drain和max_connection_age必须带抖动。
HC-TRANSPORT-07  ConnectionWriter按消息类型执行保留、合并、Resync或关闭，不能反压Actor。
HC-TRANSPORT-08  断线依靠新generation+重新认证/Join/Snapshot恢复，不做透明Socket迁移。
HC-TRANSPORT-09  farm-gate不得被公网绕过；只信任受信CLB来源、Host/SNI和客户端类型安全合同。
HC-TRANSPORT-10  同连接所有Response/Push/Control写入满足单Writer和单调server_seq。

HC-TLOG-01  Tlog 不是业务权威、幂等依据或恢复来源。
HC-TLOG-02  业务成功不得等待 TGLog，TGLog 故障不得回滚业务。
HC-TLOG-03  C级和普通失败B级使用Pod内有界队列，允许采样和受控丢失；B级成功不得在权威提交前记录。
HC-TLOG-04  A 级从已提交 Receipt/Settlement/AuditFact 派生。
HC-TLOG-05  ACTOR_SOFT 的最终资产流水只能在对应 Snapshot Flush 后导出。
HC-TLOG-06  跨 Actor 成功次数只以 Workflow 终态流水统计。
HC-TLOG-07  每条流水具有稳定 flow_id，发送可重复、下游去重。
HC-TLOG-08  Mapper 不回查当前业务状态，不在 Actor mailbox 中进行网络发送。
HC-TLOG-09  敏感凭据、正文和大对象禁止进入 Tlog。
HC-TLOG-10  farm-tlog 是共享库，TlogExport/TlogRelay 属于 farm-worker Role。
HC-TLOG-11  A级流水必须在ACK来源事件前可靠交接到独立Export Spool；TGLog SDK入队不算可靠交接。
HC-TLOG-12  来源EventDelivery完成点是ExportRecord已持久化，不是TGLog接收或TDW入仓。
HC-TLOG-13  TGLog故障不得使Player/Farm Authority Aggregate内的PendingOutbox无界增长。
```
