# P1B · 12→33 CommandCatalog 唯一迁移交接

日期：2026-08-04
状态：`COMPLETED / UNIT_PASS`

```text
task_id:     P1B-COMMAND-CATALOG-33
branch:      codex/p1b-command-catalog-20260803
worktree:    /data/home/developer_026/CodeBuddy/20260519141303/farm-game-p1b-command-catalog
base_commit: 4e587936df183dd7e6e7725200f98dec38e69d5d
commit:      28292830b1968740b314626877aad94e6486cd8c(实现);本文件 hash 回填为分支 HEAD
```

## 1. 目标与边界

将旧 12 条公开命令一次迁移为第 28 号 §6.1 冻结的 33 类公开命令,运行时只有一套
CommandCatalog;FarmFrame 顶层 oneof 区分稳定大类,业务命令 `command_type + bytes
payload`;每个公开命令恰好一个 CommandSpec;CanonicalPayloadBuilder 全量落地并冻结
公网黄金。不含任何业务裁决改动:Player/Farm Actor、Redis、Workflow、H5、其他 cmd
composition root 均未触碰。

## 2. 12→33 迁移矩阵

### 2.1 保留(4 条,编号不变)

| 旧命令 | 新命令 | 编号 | 说明 |
|---|---|---|---|
| JOIN_FARM | JOIN_FARM | 6 | Payload 改为 `JoinFarmRequest{target_farm_id}` |
| GET_OPERATION_RESULT | GET_OPERATION_RESULT | 11 | Payload/Result 改为 28 §3.8 结构;本轮 feature-disabled(见 §6) |
| CLAIM_TASK | CLAIM_TASK | 12 | Payload 改为 `task_instance_id`;本轮 feature-disabled(34 未接线) |
| GET_PLAYER_BOOTSTRAP | GET_PLAYER_BOOTSTRAP | 13 | Result 改为 28 §3.7 七字段结构;本轮 feature-disabled(Result 映射需 PlayerIngress 重接线) |

### 2.2 重命名(6 条,新名沿用旧编号,旧符号为 deprecated alias)

| 旧命令 | 新命令 | 编号 | Payload(新) |
|---|---|---|---|
| BUY_OFFER | BUY | 2 | `BuyRequest{offer_id,count}`(布局与旧 BuyOfferRequest 一致) |
| SELL_ITEM | SELL | 3 | `SellRequest{item_id,count}`(布局与旧 SellItemRequest 一致) |
| PLANT_CROP | PLANT | 4 | `PlantRequest{plot_id,crop_definition_id,target_farm_id,expected_plot_version,expected_crop_rule_version}` |
| HARVEST_CROP | HARVEST | 5 | `HarvestRequest{plot_id,target_farm_id,crop_cycle_id,expected_plot_version}` |
| WATER_CROP | CARE | 8 | `CareRequest{plot_id,target_farm_id,crop_cycle_id,incident_id,care_type}` |
| BEGIN_THEFT | THEFT | 9 | `TheftRequest{plot_id,target_farm_id,crop_cycle_id}` |

### 2.3 删除(2 条,deprecated 值保留编译窗口,Catalog 无注册)

| 旧命令 | 编号 | 处置 |
|---|---|---|
| LEAVE_FARM | 7 | 不再公开;释放 Presence 由切农场/断连的 CleanupSubscription 承接;`ValidateCommandCatalog` 双向证明"删除即不可路由" |
| GET_FARM_SNAPSHOT | 10 | 不再公开;完整快照只经 FARM_SNAPSHOT Push(L2-PROTO-27/31) |

### 2.4 新增(23 条,编号 14~36)

GET_FRIEND_LIST=14、GET_NEXT_FRIEND_OPPORTUNITY=15、GET_TASK_LIST=16、
CONFIRM_FRIEND_VISIT=17、GET_MAIL_LIST=18、GET_MAIL_DETAIL=19、MARK_MAIL_READ=20、
DELETE_MAIL=21、CLAIM_MAIL_ATTACHMENT=22、GET_PET_STATE=23、PET_UNLOCK_BY_COIN=24、
PET_FEED=25、PET_SWITCH=26、GET_COMPENDIUM=27、ACK_COMPENDIUM_SEEN=28、
GENERATE_SHARE_TOKEN=29、ROTATE_SHARE_TOKEN=30、REVOKE_SHARE_TOKEN=31、
REDEEM_SHARE=32、DELETE_FRIEND=33、BLOCK_PLAYER=34、UNBLOCK_PLAYER=35、UNLOCK_PLOT=36。

矩阵已由 `internal/protocoltest/p1b_schema_test.go` 与
`internal/gateway/commandcatalog/catalog_test.go` 钉死。

## 3. 33 命令清单与本轮接线状态

`Availability.Enabled=true`(7 条,端到端可用):JOIN_FARM、BUY、SELL、PLANT、
HARVEST、CARE(仅 CARE_TYPE_WATER 且无 incident_id)、THEFT。

`Availability.Enabled=false`(26 条,合同已注册,Gate 准入阶段以
`REJECTED + ERR_COMMAND_UNIMPLEMENTED + RetryDirective NONE` 明确拒绝,不路由、
不伪造业务成功):GET_PLAYER_BOOTSTRAP、GET_FRIEND_LIST、
GET_NEXT_FRIEND_OPPORTUNITY、GET_OPERATION_RESULT、GET_TASK_LIST、CLAIM_TASK、
CONFIRM_FRIEND_VISIT、GET_MAIL_LIST、GET_MAIL_DETAIL、MARK_MAIL_READ、DELETE_MAIL、
CLAIM_MAIL_ATTACHMENT、GET_PET_STATE、PET_UNLOCK_BY_COIN、PET_FEED、PET_SWITCH、
GET_COMPENDIUM、ACK_COMPENDIUM_SEEN、GENERATE_SHARE_TOKEN、ROTATE_SHARE_TOKEN、
REVOKE_SHARE_TOKEN、REDEEM_SHARE、DELETE_FRIEND、BLOCK_PLAYER、UNBLOCK_PLAYER、
UNLOCK_PLOT。

每条 CommandSpec 均声明:Payload/Result descriptor(protoregistry 可解析)、
MaxPayloadBytes、ProtocolRange(P0_BASE..OPEN_ENDED)、Availability(FeatureKey)、
SessionAdmission(auth/recovery/subscription/target policy)、ExecutionKind、
ExecutionAcceptPolicy、TerminalOutcomePolicy、FirstHopRole+名称、RouteMode、
RouteDomain、RouteKeySource/Extractor、OperationRoot、Idempotency、
CanonicalPayloadBuilder(REQUIRED/RUNTIME_REQUIRED 全量)、RetryPolicy、
RecoveryPolicy、TimeoutBudget(cfg key)、RateLimitPolicy(cfg key)、PermitClass。
完整断言:`catalog_test.go` 的 33/33 exactly-once、准入矩阵、Retry/Recovery 矩阵、
RouteKey(正常/零 ID/TrustedContext 不回退读 Payload)测试。

## 4. ICR-P1B-PROTOCOL-CONSUMERS(消费点适配逐文件说明)

本轮对 owned_write_set 之外文件的全部修改均经用户两轮 ICR 批准,分两类:
**(A) 机械适配**(新 oneof/uint64 sequence/新公开 Payload 类型/新 Catalog 字段导致,
无业务语义变化);**(B) 第 28 号已冻结规则的直接实现**(Catalog 驱动的解码/准入/路由)。

| 文件 | 类 | 说明 |
|---|---|---|
| internal/gateway/protocol/codec.go + codec_test.go | A+B | oneof Encode/Decode、uint64 sequence、方向校验(28 §2)、构造助手;测试含 legacy 形状 fail-closed、uint64 边界、方向违规、CommandResult/ServerPush 分支分离、未知 oneof fail-closed |
| internal/gateway/command/gate.go | A+B | HandleBinary 按 oneof 分发+方向校验+严格 seq;handleCommand 按冻结 Catalog 解码一次、Availability 拒绝、Typed target 准入(28 §6.1,取代 LegacyEnvelopeAdapter);trustedContextFor 按 RequiresCurrentSubscription 携带 FarmContext(29 §3.4);dispatchForCommand 按 RouteMode/RouteDomain/RouteKey 生成 DispatchContext(L2-PROTO-23)。**兼容接缝(机械)**:deprecated envelope 字段由 Gate 按 Catalog 从 Payload/可信 Session 重派生覆盖(TargetOwnerId=RouteKey、ExpectedPlotVersion/ExpectedCropCycleId 投影),仅供未重接线 FirstHop 结构校验;Gate 不读取客户端提交值 |
| internal/gateway/command/metrics.go | A | 两个有界拒绝原因标签(COMMAND_UNIMPLEMENTED、ROUTE_KEY_INVALID) |
| internal/gateway/command/subscription_lease.go + 直接测试 | A | REJOIN_REQUIRED 帧 oneof 构造 |
| internal/gateway/downlink/service.go、push_scope.go + 直接测试 | A | 帧构造改 oneof 助手、nextSeq uint64、GetServerPush() 解析;Join/背压/合并语义未变 |
| internal/farm/ingress/downlink.go + 直接测试(join_barrier/resync/service/w2a_join) | A | pushFrame oneof 构造;测试 frame 解析改 GetServerPush() |
| internal/gateway/transport/websocket/websocket.go + 直接测试 | A | outboundSequencer uint64,读 GetSequence() |
| cmd/dualgate-e2e/main.go | A | oneof/uint64/JOIN payload/新公开请求类型;step7 无新增资产副作用:原那一次 BUY 同时证明 Route 存活与经营链(用户最终澄清);step5 探针改用已注册命令 |

**未修改**:Player/Farm Actor、Redis、Workflow、cmd/{farm-gate,player-svr,farm-svr,
farm-worker,ws-stress,lease-stress}(后两者经 proto 层 deprecated 编译兼容窗口保持
编译,未改一行)、H5、docs/design、internal/logic/trusted、routed.go、sharded.go、
idempotency、player/ingress、farm/ingress/service.go。

### 4.1 编译兼容窗口(deprecated,P1B-CONSUMERS-2 删除)

- `FarmFrame.kind/connection_seq/body` + `FrameKind`:仅供未迁移离线工具编译;
  Gate 收到旧形状帧 fail closed(`codec_test.TestRejectsLegacyKindBodyShape`)。
- `ClientCommandEnvelope.target_owner_id/expected_*`:仅供未重接线 FirstHop
  (trusted.ValidateCommand、ingress)结构校验;Gate 不读客户端值,按 Catalog 重派生。
- `CommandType` 旧符号:`allow_alias` deprecated alias(BUY_OFFER≡BUY 等同一命令,
  无双注册);LEAVE_FARM=7/GET_FARM_SNAPSHOT=10 deprecated 且未注册。

## 5. H5 消费清单

1. **协议版本**:FarmFrame.protocol_version=4;旧版本被 Gate 显式拒绝。
2. **帧结构**:`protocol_version=1`、`sequence=8(uint64,每连接每方向从 1 严格 +1)`、
   `oneof frame_body { client_hello=10, server_hello=11, client_command=12,
   command_result=13, server_push=14, control=16 }`。CommandResult 与 ServerPush 是
   不同分支;控制帧区分客户端(ClientRecoveryComplete)与服务端(ConnectionError)作用域。
   **不得发送/依赖** deprecated 的 kind/connection_seq/body。
3. **Envelope**:ClientCommandEnvelope 只填 request_id(1..64B 不透明 ASCII,必填)、
   operation_id(按命令幂等策略)、command_type、bytes payload;不得填
   target_owner_id/expected_*(服务器忽略并以覆盖为准)。
4. **命令表**:33 条见 §3;当前可用 JOIN_FARM/BUY/SELL/PLANT/HARVEST/CARE(WATER)/
   THEFT,其余返回 `ERR_COMMAND_UNIMPLEMENTED=1007` + REJECTED + NONE。
   **GET_PLAYER_BOOTSTRAP 与 GET_NEXT_FRIEND_OPPORTUNITY 已注册但本轮未接线**。
5. **Payload 编码**:全部见 `api/proto/public/commands.proto`;数值 ID 为 uint64
   (H5 用十进制字符串/BigInt),不透明 ID 为有界 string;Payload 不含操作者 ID、
   Session Fence、Permit、VirtualShard、Redis Key、writer claim。
6. **Typed Result**:PlantResult/HarvestResult/CareResult/TheftResult 只含最小结果 +
   committed_farm_version(完整状态只经订阅 Push);BuyResult/SellResult 沿用
   private_state + delta;HarvestResult 含图鉴解锁字段(L2-PROTO-61)。
7. **Canonical Payload Hash 黄金**(重放/冲突判定):
   `uint32_be(command_type) || CanonicalBytes(payload)`(整数定宽大端、字符串
   LP(u32be len+bytes)、presence bit、repeated 计数前缀、无字段 tag、无 JSON/map)后
   SHA-256。公开黄金:PLANT{plot_id=3,crop=1,farm=101,epv=9,ecrv=2} →
   preimage `000000040000000300000001000000000000006500000000000000090000000000000002`,
   hash `583aa0b435d99a54c3f771f980eb77da2b99036baf6b4ad9b6ea6539fd08d7b1`
   (`internal/protocoltest/p1b_canonical_golden_test.go`)。
8. **帧黄金**:ClientHello/ClientCommand/CommandResult/ServerPush 编码见
   `internal/protocoltest/golden_test.go`。
9. **H5 Journal 注意**:可恢复写命令终态前持久化 command_type + operation_id +
   原 Typed Payload + 原 target_farm_id;GET_OPERATION_RESULT 本轮未接线,
   TIMEOUT→PROCESSING 的轮询恢复路径在 P1B-CONSUMERS-2 前不可用(blocker §9.2)。

## 6. 未接线命令清单(feature-disabled 原因)

| 命令 | 阻塞原因 | 待 |
|---|---|---|
| GET_PLAYER_BOOTSTRAP | 28 §3.7 七字段 Result 需 PlayerIngress 视图映射重写 | P1B-CONSUMERS-2 |
| GET_OPERATION_RESULT | 新 §3.8 Payload/StoredOperationOutcome 与 SERVICE_LB 结果目录需 ingress/ResultDirectory 重接线 | CONSUMERS-2 + R1 |
| CLAIM_TASK / GET_TASK_LIST / CONFIRM_FRIEND_VISIT | 34 号每日实例模型(task_instance_id)未接入 PlayerActor | PI1 |
| GET_MAIL_* / MARK_MAIL_READ / DELETE_MAIL / CLAIM_MAIL_ATTACHMENT | 35/35A 邮箱权威未实现 | MA0 |
| GET_PET_STATE / PET_* | 36 号 PetState/GuardSync 未接入 PlayerActor(DP0 纯领域已备) | PI1 |
| GET_COMPENDIUM / ACK_COMPENDIUM_SEEN | 37 号 CompendiumView/Seen 水位未接入(DC0 纯领域已备) | PI1 |
| GET_FRIEND_LIST / GET_NEXT_FRIEND_OPPORTUNITY | 33 号 FriendReadAPI/Navigation 未接线 | SO0 |
| GENERATE/ROTATE/REVOKE_SHARE_TOKEN、REDEEM_SHARE、DELETE/BLOCK/UNBLOCK | 33/33A 好友权威未接线 | SO0 |
| UNLOCK_PLOT | 32 号 UnlockPlotCoordinator/PlotUnlockRule 未实现 | SC1 |
| CARE 非 WATER 类型 / incident_id | 32 号 Incident 护理未实现(Gate fail closed) | SC1 |

## 7. 测试原始结论

基线(base_commit 4e58793):`go test ./...` PASS、`go vet ./...` PASS。

交付态(本分支工作树,2026-08-04 真实执行):

```text
gofmt -l api/gen internal cmd                  → 无输出
bash scripts/gen-proto.sh 重跑                  → git 无新增 diff(生成幂等)

go test -race -count=1 -timeout=120s ./internal/gateway/commandcatalog/... ./internal/gateway/protocol/... ./internal/protocoltest/...
ok  farmgame/internal/gateway/commandcatalog  1.037s
ok  farmgame/internal/gateway/protocol        1.018s
ok  farmgame/internal/protocoltest            1.038s

go test -race -count=1 -timeout=180s ./internal/gateway/command/ ./internal/gateway/downlink/ ./internal/farm/ingress/
ok  farmgame/internal/gateway/command  3.965s
ok  farmgame/internal/gateway/downlink 1.043s
ok  farmgame/internal/farm/ingress     1.116s

go test -count=1 -timeout=240s ./...            PASS(全仓,exit=0)
go vet ./...                                    PASS(exit=0,无输出)
go build -buildvcs=false ./...                  PASS(exit=0)
git diff --check                                PASS(exit=0)
```

覆盖要点(等价或强于被替换的旧覆盖):
33/33 exactly-once 注册、无重复/无遗漏、旧命令(7/10)无双注册、每条
Payload/Result descriptor 可解析、Admission/Retry/Recovery 矩阵钉死 28 §6.1、
每条 RouteKey(正常/零 ID/TrustedContext 不回退)、payload 上限(等值/超 1 字节/
Unlimited 语义)、畸形 bytes fail closed、33 条 Decoder 空载/合法/截断解码、
确定性 canonical hash(同 operation 同 payload 重放、不同 payload 冲突、
envelope 噪声不变、字段顺序编码不变、未知字段 fail closed)、
uint64(0..2^64-1)/string(空/64B/UTF-8)边界、顶层 oneof 方向校验(Gate 入站 +
codec 双向)、协议版本拒绝、legacy 帧形状 fail closed、CommandResult 与
ServerPush 分支分离(帧黄金)、未知 oneof 成员 fail closed、sequence uint64
边界(2^32-1/2^32/2^63-1)、公开 canonical 黄金(逐字节 preimage + hash)、
12→33 编号矩阵、Envelope/Frame schema 形状。

删除的测试说明:legacy_envelope_test.go 随 LegacyEnvelopeAdapter 正式退役删除
(envelope 权威字段已迁入 Typed Payload,adapter 无存在意义);catalog_test.go /
decoders_test.go / canonical_test.go 为同名重写,覆盖条目严格多于旧版(见上)。

## 8. evidence_level

`UNIT_PASS`。依据:§7 全部命令在本 worktree 真实执行。
不申报 INTEGRATION_PASS:未起真实多进程;dualgate-e2e 工具已适配可编译,
真实双 Gate 复跑属后续验证任务。

## 9. remaining_risks / blocker

1. **P1B-CONSUMERS-2(必需)**:重接线 FirstHop 消费点——trusted.ValidateCommand、
   player/farm ingress、idempotency.CommandPayloadHash(仍按旧 envelope 哈希,
   自知违反 28 §3.5,SCB-1 已标记)、workflow.CommandPayloadHash 包装,
   完成后删除 §4.1 全部 deprecated 兼容窗口与 Gate 兼容接缝。
2. **GET_OPERATION_RESULT 恢复空洞**:TIMEOUT→PROCESSING 提示客户端轮询,但该
   命令本轮 disabled;弱网恢复依赖客户端按 RecoveryPolicy 原 operation 重发,
   结果目录轮询在 CONSUMERS-2 + R1 后恢复。
3. **expected_crop_rule_version / expected_*_rule_version**:字段已冻结进 Payload
   与 Canonical Hash,服务端规则版本校验待 32 接线,当前不裁决(与旧行为一致)。
4. **CARE 只接 WATER**:pest/incident 由 Gate StructuralValidator fail closed,
   非绕过路径;32 接线时仅需移除该 validator。
5. **ws-stress/lease-stress**:经 deprecated 窗口保持编译,但其场景命令(PLANT
   旧 payload 等)对新 Gate 运行时不可用(协议 v4 + oneof),重写属 H1/工具任务。
6. **alias 语义**:CommandType_name 对同编号只显示新名;日志/指标中的命令名为
   新名(BUY 而非 BUY_OFFER),属预期。

## 10. 迁移/回滚说明

- 迁移:本分支一次切换,无运行时双 Catalog;旧 H5 因协议 v4 + oneof 被拒绝,
  H1 按 §5 清单迁移。服务端滚动发布时新旧 Gate 不可混部(帧不兼容)。
- 回滚:`git revert <本 Commit>` 或删除分支;回滚后恢复旧 12 命令主链与 v3 帧,
  新增 proto/Catalog/测试随之移除;无数据迁移(Redis 中的 Receipt/Workflow 记录
  由旧代码按旧 envelope hash 读写,回滚后自愈一致)。
- 生成纪律:api/gen 只由 `bash scripts/gen-proto.sh` 产生(已验证幂等);
  禁止手改 generated 文件。

## 11. 交付 Commit

files_changed(全部在最终允许写集内;`git diff --name-status` 为证):

```text
api/proto/common/frame.proto / envelope.proto / control.proto / enums.proto   (修改)
api/proto/public/commands.proto                                              (新增)
api/gen/common/{frame,envelope,control,enums}.pb.go                          (生成)
api/gen/public/commands.pb.go                                                (生成)
internal/gateway/commandcatalog/{spec,catalog,decoders,routekey,canonical,pushspec}.go (重写/新增)
internal/gateway/commandcatalog/{catalog_test,decoders_test,canonical_test}.go (重写恢复)
internal/gateway/commandcatalog/legacy_envelope.go + legacy_envelope_test.go (随 adapter 退役删除)
internal/gateway/protocol/codec.go + codec_test.go                           (重写)
internal/gateway/command/{gate,metrics,subscription_lease}.go + 直接测试      (适配)
internal/gateway/downlink/{service,push_scope}.go + 直接测试                  (适配)
internal/farm/ingress/downlink.go + 直接测试                                  (适配)
internal/gateway/transport/websocket/websocket.go + 直接测试                  (适配)
internal/protocoltest/{golden,envelope}_test.go (重写) + p1b_canonical_golden_test.go + p1b_schema_test.go (新增)
cmd/dualgate-e2e/main.go                                                     (适配)
docs/verification/p1b-command-catalog-handoff.md                             (本文件)
```

完整 commit hash:`28292830b1968740b314626877aad94e6486cd8c`(实现);本文件的回填提交为分支 HEAD,以 `git log -1` 为准。
