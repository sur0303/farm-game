# W6 内部 IDL 对齐实施记录（合同 29）

日期：2026-08-02
计划：`docs/design/70-w0-contract-alignment-implementation-plan.md` §5 W6 卡片
范围：W6A TrustedCommandContext 唯一结构 + G4（ExecuteCommandResponse oneof）、
W6B DispatchContext/ActorRouteHeader 唯一结构、W6C Join 公共 Fence（DownlinkTargetFence/
SubscriptionFence，供 W2/W5 共用）。

## 1. 合同依据与关键决策

- 29 §3.1：TrustedCommandContext = SessionPermitSnapshot + recovery_complete_at_admission
  + 可选 FarmAdmissionContext + 可选 JoinAdmissionContext，唯一字段定义。
- 29 §3.2/§3.5：Gate 从本地受信 Session 生成上下文；FirstHop 信任内部 RPC 服务身份，
  **不做逐命令签名**。据此删除了 ExecuteCommand 链路的 HMAC SessionPermit 签发/验证
  （`auth.IssuePermitToken` 两个调用点、`trusted.ValidateCommand` 的 crypto 分支、
  farm/player ingress 的 permitSecret 接线）。`session/permit.go` 与 `IssuePermitToken`
  作为 29 §3.5 的"短期签名 Internal Permit"未来可选项保留，当前无调用。
- 29 §3.4 准入策略由 CommandCatalog `SessionAdmissionPolicy.RequiresCurrentSubscription`
  驱动（合同要求的单一事实源）：
  - JOIN_FARM：必须 `join_attempt`（含 Gate 生成的 join_attempt_id）且不得有 farm_context；
  - PLANT/HARVEST/WATER/BEGIN_THEFT/LEAVE_FARM：必须 `farm_context` 且
    `admitted_farm_id == target`，不得有 join_attempt；
  - GET_FARM_SNAPSHOT/GET_OPERATION_RESULT（只读）：不要求 farm_context；
  - 玩家域命令：两者都不得携带，`target == session.player_id`。
- 29 §4：DispatchContext/ActorRouteHeader 入 IDL；Gate 对 FirstHop player/farm-svr 的命令
  填充 ACTOR_AFFINITY + route_domain + actor_id；静态单 Holder 部署下 expected_* 留空，
  Holder 本地 Lease/WriterEpoch 校验仍是最终裁决（29 §4.3）。
- 29 §9.1（G4）：ExecuteCommandResponse 改 oneof `business | route_rejection`。
  内部响应不再携带 operation_id/request_id，Gate 用原请求组装公网
  CommandResultEnvelope。`BusinessCommandOutcome.farm_version=6` 是 [设计] 扩展，
  保留现网 envelope.farm_version 语义，32 号评审决定是否下沉 Typed Result。
- 29 §8.1 + 26 HC-ACCESS-32：DownlinkTargetFence/SubscriptionFence 入 IDL（W6C），
  W2 的 Join RPC 家族与 W5 的 BatchRenewSubscriptions 直接复用。
- join_attempt_id：Gate 在 prepareJoin 时为新 Join intent 生成（newTraceID），
  同一 durable operation 的重连复用同一 intent 与 attempt（PR-1 语义不变）。

## 2. 改动面

| 文件 | 内容 |
|---|---|
| `api/proto/logicrpc/logic_service.proto` | 全部新结构（§3.1/§4/§9.1/§8.1）+ proto gen |
| `internal/logic/trusted/validate.go` | 重写为结构校验（SessionAdmissionPolicy 驱动） |
| `internal/gateway/command/gate.go` | 4 处上下文构建改嵌套；去逐命令签名；dispatch 填充；
  `commandResultFromOutcome` 组装公网响应；pendingJoinState.attemptID |
| `internal/gateway/command/{routed,sharded}.go` | GET_OPERATION_RESULT 回退判定改 business outcome |
| `internal/farm/ingress/{service,downlink}.go` | 校验/字段映射/outcome 装配；移除 permitSecret |
| `internal/player/ingress/service.go` | 同上 |
| `cmd/{farm,player}-svr/main.go` | 移除 ingress SetPermitSecret 接线（gate 侧 PERMIT_SECRET 保留） |
| 7 个测试文件 | 上下文构造/断言迁移到新结构；validate 测试重写为准入策略矩阵 |

## 3. 验收证据

- `go build ./...` / `go vet ./...` / `go test ./...` 全 PASS
- `go test -race`（gateway/command、farm/player ingress、trusted）PASS
- `scripts/verify-s2.sh` PASS（含 H5 Vitest + staging/web 构建）
- 四容器（公司 Redis）重建后真实 E2E：Web 账号 1/1 PASS、Staging Direct WSS 5/5 PASS
  （个人/经济 Loop 与重连、好友 roster、顶号、好友导航回切、双浏览器广播）

## 4. 语义不变性声明与已知边界

- 公网协议、H5、ws-stress 未动；E2E 原样通过证明对外行为不变。
- 变化的是 gate↔logic 的内部信任模型：从逐命令 HMAC 改为内部服务身份 + 结构校验。
  生产化时按 29 §3.5 接入 mTLS/Workload Identity 与服务身份拦截器（本包不含）。
- `recovery_complete_at_admission` 当前恒 false（26 §6 恢复面随 W2/W3 接线）。
- `RouteRejection` 分支已入 IDL，当前无生产者；Gate 侧收到时按后端不可用处理。
- 旧 flat TrustedCommandContext（session_permit/session_authority_epoch/trace_id/
  transport_generation/route_revision/subscription_id）一次性删除，无并行保留。
