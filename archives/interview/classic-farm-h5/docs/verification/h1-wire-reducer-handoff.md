# H1 · H5 Wire/Reducer 收敛到最终公网协议交接

日期:2026-08-04
状态:`COMPLETED / UNIT_PASS`

```text
task_id:     H1-H5-WIRE-REDUCER
repo:        classic-farm-h5
worktree:    /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-h1-wire-reducer
branch:      codex/h1-wire-reducer-20260804
base_commit: 673743823fb4fd876bf462279300f5f56985ab23
final_commit: 见 §2(分支 HEAD)
backend baseline(只读): farm-game @ cp2-round2-20260804
  (551e31d18d69b146b584ef5b1a3b655302d50093;工作树 HEAD 多 1 个纯文档 commit,
   协议/代码与 tag 完全一致;本任务未写入 backend 任何文件)
integration owner: I0(本任务不合 integration、不建 MR)
```

语义输入(全部来自只读 backend baseline):docs/design/26、28、60、72 §14、73 §9.6;
api/proto/common/{frame,envelope,control,enums}.proto;api/proto/public/commands.proto;
api/proto/farm/farm_view.proto;internal/protocoltest 黄金;internal/gateway/commandcatalog
33 CommandSpec;docs/verification/p1b-command-catalog-handoff.md(§5 H5 消费清单)。

## 1. 审计:现状与最终协议的差距(编码前冻结)

### 1.1 后端最终公网协议事实(唯一语义输入结论)

| 项 | 最终合同 | 出处 |
|---|---|---|
| protocol_version | **4**(P0_BASE 冻结值;v3 被 Gate 显式拒绝) | internal/gateway/protocol/codec.go:23 |
| FarmFrame | `protocol_version=1`、`sequence=8(uint64,每连接每方向从 1 严格 +1)`、`oneof frame_body{client_hello=10, server_hello=11, client_command=12, command_result=13, server_push=14, control=16}`;deprecated `kind=2/connection_seq=3/body=15` Gate fail closed | frame.proto;codec.go:77-85 |
| 方向守卫 | C→S 仅 client_hello/client_command/control.client_recovery_complete;S→C 仅 server_hello/command_result/server_push/control.connection_error | codec.go:93-121 |
| ClientCommandEnvelope | 只填 `request_id=10(必填,1..64B ASCII)`、`operation_id=2`、`command_type=4`、`payload=15`;**不得填** deprecated `target_owner_id=5/expected_*=6/7/8`(Gate 不读) | envelope.proto:85-106;p1b-handoff §5.3 |
| CommandResultEnvelope | operation_id=1、status=2、request_id=7、payload=15、error=20、retry_directive=21、retry_after_ms=22、command_type=23、result_context{farm=24,join=25}、server_operation_locator=26 | envelope.proto:115-137 |
| ServerPushEnvelope | push_type=6、push_context oneof{session=10,player=11,farm_subscription=12}、payload=15 | envelope.proto:196-225 |
| Join 下行唯一顺序 | ACCEPTED(仅 JoinResultContext,result_payload 空)→ FARM_SNAPSHOT → FARM_DELTA_BATCH* → SUBSCRIPTION_READY;反序即协议错误 | 28 §2.2;26 §12;p1b-handoff §5 |
| 33 命令可用性(本基线) | **可用 7 条**:JOIN_FARM/BUY/SELL/PLANT/HARVEST/CARE(仅 WATER 无 incident)/THEFT;**feature-disabled 26 条**:Gate 准入阶段 REJECTED + ERR_COMMAND_UNIMPLEMENTED(1007) + RetryDirective NONE,不路由、不伪造 | commandcatalog/catalog.go;p1b-handoff §3/§6 |
| Typed Payload | 全部 api/proto/public/commands.proto;target_farm_id/expected_* 进入各自 Payload | commands.proto |
| Typed Result | Plant/Harvest/Care/TheftResult 最小结果+committed_farm_version;Buy/SellResult=player.PrivateState+delta;HarvestResult 含图鉴解锁字段 | commands.proto;p1b-handoff §5.6 |
| Canonical Payload Hash | `uint32_be(command_type) ‖ CanonicalBytes(payload)`(字段号升序、定宽大端整数、字符串 LP(u32be len‖bytes)、presence bit、repeated 计数前缀、无字段 tag)后 SHA-256;PLANT 黄金 preimage/hash 见 §4 | internal/contracts/canonical.go;p1b_canonical_golden_test.go |
| 恢复屏障 | ClientRecoveryComplete 只在自己农场 ActiveSubscription 后被 Gate 接受;RequiresRecoveryComplete 写命令否则 REJECTED+ERR_RECOVERY_INCOMPLETE(2006) | gate.go handleControl |
| REJOIN_REQUIRED | push_type=22,只终止农场上下文,客户端 Fresh Join;invalid_from_version 仅诊断 | subscription_lease.go;farm_view.proto |
| feature-disabled 客户端语义 | REJECTED+1007+NONE:明确不可用,不自动重放、不伪造成功 | p1b-handoff §3;任务纪律 8 |

### 1.2 H5 现状差距矩阵(v0.2 → 最终)

| # | 位置 | 现状(base) | 最终要求 | 处置 |
|---|---|---|---|---|
| 1 | canonical-v02-adapter.ts:86-98 | protocol_version=3;帧用 deprecated `kind/connection_seq/body` | v4 + `frame_body` oneof + `sequence=8` | 重写为 final adapter |
| 2 | canonical-v02-adapter.ts:257-285 | Envelope 填 deprecated `targetOwnerId/expectedFarmVersion/expectedPlotVersion/expectedCropCycleId` | 只填 request_id/operation_id/command_type/payload | 重写 |
| 3 | canonical-v02-adapter.ts:62-72,192-220 | 9 条旧命令名(BUY_OFFER/PLANT_CROP/WATER_CROP/BEGIN_THEFT/SELL_ITEM);payload 用旧 farm/farm.proto 类型;CLAIM_TASK payload=taskDefinitionId | 33 目录新名;payload 用 public/commands.proto(target/expected 入内);CLAIM_TASK=task_instance_id | 重写;可用 7 条全接,disabled 命令按 1007 呈现 |
| 4 | canonical-v02-adapter.ts:249-267 | QUERY_OPERATION 只带 operation_id,requestId 伪造 `query:${op}` | GetOperationResultRequest{original_command_type, original_operation_id, original_target, server_operation_locator?} | 重写为 typed 查询 |
| 5 | canonical-v02-adapter.ts:308-356 | 结果类型靠客户端 pendingCommandTypes Map;errorCode 数字字符串 | 信封 command_type 权威(本地 Map 仅兜底);错误码符号名;RetryDirective/retry_after_ms/server_operation_locator 透出 | 重写 |
| 6 | canonical-v02-adapter.ts:314-319 | Bootstrap 解旧 player.PlayerBootstrapResult;Buy/Sell 解旧 EconomyCommandResult | GetPlayerBootstrapResult(28 §3.7);BuyResult/SellResult;占位 Result 不解析字段 | 重写 |
| 7 | canonical-v02-adapter.ts:133-143 | PlotState 注入本地 crop-catalog 的 cropName;status 只有 EMPTY/GROWING | wire 派生状态不混本地规则数据;CropPhase 服务端枚举直读(含 MATURE) | 移除本地注入;status 由 crop_phase 映射 |
| 8 | src/protocol/canonical-frame.ts + farm.proto | v0.1 死代码(8 字节头、payload_json) | 删除 | 已删(含对应测试) |
| 9 | operation-journal.ts | 无 typed payload/hash/target/root policy/locator;key 含 sessionEpoch;无四边界 | 28 §3.9 全字段 + 四边界 Fail Closed | 重写 |
| 10 | command-service.ts | 无 RetryDirective 执行;恢复只能查询不能重放;1007 无特殊语义 | RetryDirective 全映射;只自动重放可证明未 Accepted;1007=明确不可用 | 重写(仅协议/Journal 接缝) |
| 11 | ws-client.ts | join/query/bootstrap 用旧 IR;无 connection_error 处理;sequence number | typed payload Join/查询;control.connection_error 处理;sequence bigint 严格 +1 | 适配 |
| 12 | farm-store.ts | 无本地经济计算(干净);economyVersion 单一 fence | 增加 player_state_revision 独立 fence + 私有数据可用性标记 | 适配 |
| 13 | test/helpers/test-codec.ts | 第二套测试线协议(v0.1 形状) | 不保留两套并行命令主链(含测试双) | 删除;TestFarmServer 改讲最终协议,WsClient 测试走生产 adapter |
| 14 | App.ts | execute 用旧命令名/plotId 字符串/expected 在信封 | 编译兼容最小改动 | 见 §7 |
| 15 | environment.ts | ProtocolProfile 仅 'canonical-v02' | 'canonical-final' | 已改 |
| 16 | App.ts(违规边缘,记录不改) | 本地成熟推导(matureAt<=serverNow)与写死 totalMs=10_000、crop-catalog 本地价格 | 属页面层,不在 H1 Core 写集;reducer 已提供服务端 crop_phase/mature_at | 记录为 H2 接缝(§9) |

### 1.3 设计决策(编码前冻结,实施与之一致)

1. **单一生产主链**:`src/protocol/final-protocol-adapter.ts` 取代 canonical-v02-adapter;`protocol-adapter.ts` seam 不变;产品入口只注入 final adapter;测试不再保留第二线协议。
2. **命令面**:生产 adapter 支持发送 7 条可用命令 + GET_PLAYER_BOOTSTRAP/GET_OPERATION_RESULT/CLAIM_TASK(合同目标接线,1007 降级);其余 23 条 disabled 命令不进入客户端发送面(无页面),占位 Result 解码按"无可解析字段"处理。
3. **1007 降级(不伪造)**:
   - GET_PLAYER_BOOTSTRAP → REJECTED+1007:不伪造私有数据;store 不写入假 0;恢复屏障按"后端明确不可用"降级完成(Gate 的受信写准入只要求 ClientRecoveryComplete + 自己农场 ActiveSubscription);wallet/种子显示未知(…),以首个权威 Buy/Sell private_state 为准;资产未知时 App 不做本地价格门控(服务端裁决并回权威错误)。
   - GET_OPERATION_RESULT → 1007:查询通道不可用是服务端明确终态。停止全部结果轮询,Journal 记录保留为 Unknown,不淘汰、不盲放;`replayOperation`(同 operation_id+原 payload,服务端幂等只生效一次)是唯一重放接缝。首轮恢复不再因此死锁农场(详见 §5 决策记录)。
   - 好友机会/好友列表(HTTPS 链路,本次未改)→ 维持 Loading/Retry,不退出自己农场。
   - CLAIM_TASK 等业务 disabled → pending operation 直接 Resolved(明确不可用文案),不进入轮询。
4. **uint64 纪律**:frame sequence、farm_version、plot_version、target_farm_id、ready_through_version、expected_*、player_state_revision 在 JS 边界全部 string/bigint;adapter 内 bigint 计数/比较;绝不 `Number(uint64)`。int64 业务值(coin/server_time/mature_at)经 safe-integer 校验后转 number(28 §7 只约束 uint64 与单调版本)。
5. **Journal**:字段按 28 §3.9;key=`env:playerId:operationId`(去掉 sessionEpoch,恢复必须跨 Session);`payloadSchemaVersion='p1b-v1'`,未知版本 Fail Closed 只查询不重放;四边界默认 max_pending=64 / max_entry_bytes=8KiB / max_total_bytes=256KiB / max_retention=7d(超限=JOURNAL_CAPACITY_BLOCKED 停新写、继续恢复;retention 超期=JOURNAL_RECOVERY_OVERDUE 告警+停新写,不作 TTL 删除)。Canonical Hash 按 SCB-1 规则本地复算,复现 PLANT 黄金(§4)。
6. **兼容壳删除条件**:v0.2 adapter/canonical-frame/farm.proto/test-codec 本批直接删除(后端 Gate 已 fail closed 旧形状,无运行时消费者);types.ts 内部 IR 的 `plotId: 'plot-N'` 字符串与 `PrivateState.tasks/codex` 展示形状属 App 展示兼容,删除条件为 H2 页面改用数值 plot_index 与 34/37 视图模型。

## 2. 交付 commit 与提交列表

```text
base:   673743823fb4fd876bf462279300f5f56985ab23
branch: codex/h1-wire-reducer-20260804

ee9cba7 feat(h1): converge wire/journal/reducer to final public protocol (v4 oneof + typed payloads)
f7f6abc docs(h1): wire-reducer handoff + e2e spec tolerance for v4 baseline
<回填提交> docs(h1): backfill delivered commit hashes
```

最终 commit hash 以分支 HEAD 为准(`git log -1`)。

### 写集(全部在任务允许范围内)

```text
新增:src/protocol/final-protocol-adapter.ts、src/protocol/canonical-payload.ts、
     src/protocol/generated/public/commands.ts(生成)、
     src/test/final-protocol-adapter.test.ts、src/test/canonical-payload.test.ts、
     docs/verification/h1-wire-reducer-handoff.md
重写:src/protocol/types.ts、src/persistence/operation-journal.ts、
     src/domain/command-service.ts、src/state/farm-store.ts、
     src/test/helpers/test-farm-server.ts、src/test/command-service.test.ts
适配:src/network/ws-client.ts、src/protocol/protocol-adapter.ts、
     scripts/gen-proto.mjs(+public/commands.proto)、src/app/App.ts(§7)、
     src/app/environment.ts、src/test/ws-client-recovery.test.ts、
     src/test/e2e/{web-recovery,workflow-recovery,staging}.spec.ts、.gitignore(.codebuddy/)
删除:src/protocol/{canonical-frame.ts,canonical-v02-adapter.ts,farm.proto}、
     src/test/{canonical-frame.test.ts,canonical-v02-adapter.test.ts,codec.test.ts,
     protocol-adapter.test.ts}、src/test/helpers/{test-codec.ts,test-protocol-adapter.ts}
再生成:src/protocol/generated/{common/*,logicrpc/*}(backend baseline proto 唯一来源)
```

## 3. v0.2 → 最终 Wire 差异矩阵(编码结果)

| 维度 | v0.2(base) | 最终(本批) |
|---|---|---|
| protocol_version | 3(Gate 拒绝) | 4 |
| 帧大类 | deprecated `kind/connection_seq/body` | `frame_body` oneof + `sequence=8`(bigint,严格 +1,跳号即重连) |
| 方向守卫 | 无 | 服务端发来 client_hello/client_command/客户端控制帧 → fail closed |
| Envelope 字段 | target_owner_id/expected_farm_version/expected_plot_version/expected_crop_cycle_id | 永不设置;target/expected 全在 Typed Payload |
| 命令命名 | BUY_OFFER/SELL_ITEM/PLANT_CROP/HARVEST_CROP/WATER_CROP/BEGIN_THEFT | BUY/SELL/PLANT/HARVEST/CARE/THEFT(编号相同) |
| Payload 类型 | farm/farm.proto 旧内部类型 + JSON payload | public/commands.proto Typed Payload(强类型判别联合) |
| 查询 | GET_OPERATION_RESULT 空 payload + 伪造 requestId | GetOperationResultRequest{original_command_type, original_operation_id, original_target, locator} |
| Result 解码 | pendingCommandTypes Map 关联 + JSON/旧内部 Result | 信封 command_type 权威 + Typed Result(Buy/Sell/Plant/Harvest/Care/Theft/Bootstrap/OperationLookup) |
| 错误码 | 数字字符串 | 符号名(ERR_*)+ errorCodeNumber + featureUnavailable(1007) |
| RetryDirective | 忽略 | 全 8 值映射执行(command-service) |
| server_operation_locator | 丢弃 | 透出并存入 Journal,查询原样回带 |
| Plot 成熟 | 本地 matureAt<=now 推导(status 仅 EMPTY/GROWING) | 服务端 CropPhase 直读(EMPTY/GROWING/MATURE) |
| cropName | adapter 注入本地 catalog 名 | 移除(App 渲染层自取自 catalog,不进 wire 状态) |
| Bootstrap | 旧 player.PlayerBootstrapResult | GetPlayerBootstrapResult(七字段)+ player_state_revision 单调 fence + 1007 降级 |
| 私有数据未知态 | 默认 wallet=0(伪数据) | privateStateAvailable=false → UI 显示 …;未知时不做本地价格门控 |
| 兼容壳 | canonical-frame.ts、farm.proto、test-codec 第二线协议 | 全部删除;无残留旧链(§1.3.6 记录 IR 展示兼容及删除条件) |

## 4. 黄金向量与测试映射

### 4.1 黄金(与 farm-game/internal/protocoltest 同源)

| 黄金 | 来源 | H5 测试 | 断言方式 |
|---|---|---|---|
| ClientHello 帧 `0804400152100a01741a0b6661726d2d68352f302e32` | golden_test.go TestFarmFrameGoldenClientHello | final-protocol-adapter.test.ts「encodes ClientHello byte-identical」 | **逐字节相等** |
| ClientCommand 帧 `08044002621112036f70312002520272317a0408011002` | golden_test.go TestFarmFrameGoldenClientCommand | 同上「round-trips the Go ClientCommand golden semantically」 | 字段级一致+往返(ts-proto 按声明序、Go 按字段号序,皆合法 protobuf,不逐字节) |
| CommandResult/ServerPush 分离 `080440036a0610013a027231` / `0804400472023015` | golden_test.go TestFarmFrameGoldenCommandResultAndServerPushSeparated | 同上「decodes the Go CommandResult/ServerPush separation goldens」 | oneof 分支逐字节可区分+语义解码 |
| W11A Envelope 三向量(PUSH/JOIN_RESULT/ERROR) | envelope_test.go | w11a-golden.test.ts(保留)+「decodes the shared W11A envelope goldens」 | 解码+往返 |
| PLANT Canonical preimage `00000004…0002` / hash `583aa0b4…d7b1` | p1b_canonical_golden_test.go | canonical-payload.test.ts「reproduces the PLANT golden preimage and hash byte-exactly」 | **preimage 与 SHA-256 逐字节相等** |
| 重放/冲突家族(JOIN_FARM/BUY/HARVEST/CLAIM_TASK 等) | p1b_canonical_golden_test.go TestP1BCanonicalGoldenFamilies | canonical-payload.test.ts「stable for replay and conflicts」 | 同 payload 同 hash、语义字段变化即冲突 |

### 4.2 状态机 / Journal / fence 测试映射

| 合同点 | 测试 |
|---|---|
| Connecting→Authenticating→Authenticated→Joining/Recovering→InFarm;Authenticated≠可经营 | ws-client-recovery.test.ts「keeps business writes closed until the application completes RecoveryBarrier」「fails closed when the RecoveryComplete control frame cannot be sent」 |
| ACCEPTED→Snapshot→Delta*→Ready 唯一顺序;反序/fence 不符丢弃 | ws-client-recovery.test.ts「opens canonical Join only after matching ACCEPTED, Snapshot and Ready」「ignores stale subscription catch-up and holds Ready until the missing version is applied」;final-protocol-adapter.test.ts snapshot_version 不一致/missing fence fail closed |
| Catch-up 严格 +1、空洞 Fresh Join(新 operation_id) | 「Fresh Joins with a new operation_id when catch-up contains a version hole」 |
| Ready(N) applied==N 才提升;切农场轮换 operation_id | 「reuses Join operation_id across recovery and rotates it when switching farms」「Fresh Joins … while InFarm」 |
| REJOIN_REQUIRED → Fresh Join 不重放写 | 「Fresh Joins with a new operation_id after REJOIN_REQUIRED without replaying writes」 |
| 普通导航不重开恢复屏障 | 「does not reopen RecoveryBarrier for ordinary in-session farm navigation」 |
| sequence 严格 +1/版本拒绝/legacy 帧 fail closed/方向守卫 | final-protocol-adapter.test.ts「enforces strict per-direction sequence continuity」「fails closed on legacy kind/body frames and wrong protocol versions」「rejects server frames in the client direction」 |
| deprecated 字段零填充 | 「never sets deprecated frame/envelope fields」 |
| Join payload/查询 payload typed 编码 | 「encodes JoinFarm with target_farm_id inside the typed payload」「encodes GET_OPERATION_RESULT with original identity, target and locator」 |
| JOIN ACCEPTED 禁带 payload(L2-PROTO-27) | 「rejects a JOIN_FARM result that carries a forbidden payload」 |
| subscription fence(change 只认当前 subscription) | 「decodes FARM_CHANGE only from the matching subscription context」+ ws-client acceptLiveChange 既有用例 |
| Journal:发送前落账+hash+终态清理 | command-service.test.ts「journals a recoverable write before sending and clears it on terminal results」 |
| Journal:PROCESSING 按原 identity+locator 轮询 | 「polls GET_OPERATION_RESULT with the original identity while PROCESSING」 |
| Journal:只重放从未发送记录;admitted-unknown 只查询 | 「replays only never-sent records with the same operation_id after InFarm opens」 |
| Journal:未知 schema Fail Closed | 「refuses to replay records with an unknown payload schema version」 |
| Journal:首轮查询失败保持屏障 | 「fails closed when the first pending-operation recovery query cannot be queued」 |
| Journal 四边界(entry/entries/total 拒绝;retention 只告警不删除) | command-service.test.ts BoundedOperationJournal 两个用例 |
| 1007 明确不可用不轮询;查询目录 1007 停轮询保留记录 | 「marks feature-disabled commands resolved without retry loops」「stops auto-polling and keeps records when the result directory is feature-disabled」 |
| farm-store:reducer 只消费 Receipt/Push、economy_version/player_state_revision 双 fence | farm-store.test.ts(既有)+ applyPlayerBootstrap 新路径 |

## 5. Journal 与恢复矩阵(本批语义)

```text
PendingOperationRecord{
  environment, playerId, operationId, commandType,
  targetFarmId?/peerPlayerId?/mailId?, operationRootPolicy,
  serverOperationLocator?, payloadSchemaVersion='p1b-v1',
  payloadEncoding='protobuf-base64', originalTypedPayload,
  payloadIr(同源 IR JSON,重放重建用),
  canonicalPayloadHash, createdAtMs, lastStatus, lastLookupStatus?, retryCount
}
```

| 场景 | 行为 |
|---|---|
| 首次发送前 | 落 Journal(含 payload+hash);容量超限在发送前拒绝(JournalCapacityError) |
| lastStatus=Created(从未上线) | InFarm 后同 operation_id+原 payload 重放(可证明未 Accepted;幂等只生效一次) |
| AwaitingResult/Processing/Unknown(可能 ADMITTED) | 只按原 operation_id+原目标+locator 查询;不换 ID、不盲放 |
| 查询 COMPLETED | 应用 StoredOperationOutcome 终态并删 Journal |
| 查询 PROCESSING | 按 retry_after_ms 继续轮询 |
| 查询 NOT_FOUND | 按 RecoveryPolicy:农场命令 Fresh Join 原目标后同 operation 重发;玩家命令同 operation 重发(有界 3 次) |
| 查询 EXPIRED | 停止自动重试,刷新权威状态,记录保留人工处置 |
| 查询通道 1007(本基线) | 停全部轮询;记录保留 Unknown;不淘汰不盲放;replayOperation 人工接缝 |
| RetryDirective=RETRY_SAME_OPERATION | 同 operation_id 有界自动重试(≤3 次) |
| =POLL_OPERATION_RESULT | retry_after_ms 后查询 |
| =FRESH_JOIN / =FRESH_JOIN_THEN_RETRY | Fresh Join(后者 InFarm 后重放原 operation) |
| =RECONNECT / =REAUTHENTICATE | retryNow() / 回登录流程(App returnToAuth) |
| =REFRESH_AUTHORITY_STATE | 重查 Bootstrap+Fresh Join,不重放旧写 |
| 1007 业务命令 | Resolved(明确不可用),不轮询 |
| 容量四边界 | entry 超限发送前拒绝;entries/total 满→JOURNAL_CAPACITY_BLOCKED 停新写继续恢复;retention 超期→JOURNAL_RECOVERY_OVERDUE 告警不删除;未知 schema 只查询 |

## 6. 33 命令客户端行为矩阵

可用(7):发送面 + Typed Payload/Result + Journal(写命令)。

| 命令 | 客户端行为 |
|---|---|
| JOIN_FARM | 唯一 Join 主链;ACCEPTED→Snapshot→Delta*→Ready;RuntimeRequired 幂等,不入长期 Journal |
| BUY / SELL | Typed BuyRequest/SellRequest;BuyResult/SellResult private_state 单调应用(economy_version fence);可恢复写,入 Journal,PLAYER root |
| PLANT / HARVEST | Typed Payload(target_farm_id/expected_plot_version/expected_crop_rule_version/crop_cycle_id);最小 Result;入 Journal,FARM root,RecoveryPolicy=FRESH_JOIN_THEN_RETRY |
| CARE | 仅 CARE_TYPE_WATER 且无 incident_id(adapter 对其他取值本地 fail-closed,与 Gate StructuralValidator 对齐);入 Journal,FRIEND_PAIR root |
| THEFT | PROCESSING→查询收敛;入 Journal,FRIEND_PAIR root |

feature-disabled(26):合同已注册、Gate 一律 REJECTED+1007+NONE。

| 命令 | 客户端行为 |
|---|---|
| GET_PLAYER_BOOTSTRAP | 照常发送;1007 → 屏障降级完成,私有数据保持未知,不伪造;接线后自动恢复(单调 player_state_revision) |
| GET_OPERATION_RESULT | 照常发送;1007 → 见 §5"查询通道 1007" |
| CLAIM_TASK | App 现有入口照常发送;1007 → Resolved「该功能尚未开放」 |
| GET_NEXT_FRIEND_OPPORTUNITY / GET_FRIEND_LIST | 不在 WSS 发送面(App 现走 HTTPS friends 链路,本次未改);1007 语义同上,Loading/Retry 不退场 |
| GET_TASK_LIST、CONFIRM_FRIEND_VISIT、GET_MAIL_*、MARK_MAIL_READ、DELETE_MAIL、CLAIM_MAIL_ATTACHMENT、GET_PET_STATE、PET_*、GET_COMPENDIUM、ACK_COMPENDIUM_SEEN、GENERATE/ROTATE/REVOKE_SHARE_TOKEN、REDEEM_SHARE、DELETE_FRIEND、BLOCK_PLAYER、UNBLOCK_PLAYER、UNLOCK_PLOT | 无页面入口,不进入发送面;接线后由 H2 页面任务接入;若误发将收 1007 并按明确不可用呈现 |

另:LEAVE_FARM(7)/GET_FARM_SNAPSHOT(10)已从公网删除,客户端不发送;切农场/断连由 Gate Cleanup 承接。

## 7. App 编译兼容改动逐项(禁止写集之外的最小例外)

1. `src/app/App.ts`
   - import/注入 `finalProtocolAdapter`(取代 canonicalV02ProtocolAdapter);
   - `execute()` 助手:旧命令名→新名,plotId `plot-N`→数值 plot_id,构造 Typed Payload(target_farm_id=当前农场,expected_crop_rule_version=0——客户端无权威规则版本来源,服务端 32 接线前不裁决,见 backend p1b-handoff §9.3);
   - CLAIM_TASK payload:taskDefinitionId→task_instance_id 字符串占位(该命令当前 1007,34 接线后由任务页面替换);
   - `handlePlayerBootstrapResult`:applyPrivateState→applyPlayerBootstrap(player_state_revision fence);新增 1007 降级分支(§1.3.3);
   - 资产显示:`hasPrivateState()` 为 false 时金币/种子显示 …,`data-wallet-count='unknown'`;renderUtility 收 `wallet: number|null`,未知时不做本地价格门控(服务端裁决);
   - CommandService 注入 `events.reauthenticate → returnToAuth`;Journal 包 `BoundedOperationJournal`。
2. `src/app/environment.ts`:ProtocolProfile `'canonical-v02'`→`'canonical-final'`(VITE_FARM_PROTOCOL 同步;部署若显式设置该变量需更新)。
3. `src/test/e2e/{web-recovery,workflow-recovery,staging}.spec.ts`:钱包读数对 `unknown`(NaN)容错——首屏资产未知时先经首笔 BUY 的权威 private_state 再断言;无玩法改动。
4. `.gitignore`:追加 `.codebuddy/`(会话日志目录,保证 git status 为空)。

未改:src/app 页面结构/样式、friends/share HTTPS 链路、crop-catalog/task-catalog、auth 三 provider、miniprogram/wechat-shell。

## 8. 原始验证结果

2026-08-04 在本 worktree 真实执行:

```text
npm test
→ Test Files 11 passed (11);Tests 74 passed (74)

npm run build:web      → tsc --noEmit + vite build --mode web      ✓ built
npm run build:staging  → tsc --noEmit + vite build --mode staging  ✓ built
npm run audit:build    → 三种 mode real-backend boundary audit passed

npm run validate:handoff
→ 不适用:该脚本校验部署交接 URL(environment/h5-origin/api-origin/ws-url
  非占位、协议正确),本任务无部署地址交接;脚本本身未修改。

git diff --check 673743823fb4fd876bf462279300f5f56985ab23..HEAD → PASS(无输出)
git status --porcelain → 为空(.codebuddy/ 已入 .gitignore)

FARM_GAME_DIR=<backend baseline> npm run gen:proto → 生成幂等,生成物即当前提交内容
```

## 9. 后端缺口/ICR、未跑 E2E 与 H2 接缝

### 9.1 后端接口缺口/ICR(按阻塞度)

1. **GET_PLAYER_BOOTSTRAP 未接线(P1B-CONSUMERS-2)**:H5 首屏私有资产无权威读取通道;本批按 §1.3.3 降级(不伪造、显示未知、屏障降级完成)。接线后 H5 无需改代码即自动恢复(单调 player_state_revision)。
2. **GET_OPERATION_RESULT 未接线(CONSUMERS-2 + R1)**:TIMEOUT/PROCESSING 的轮询恢复不可用;本批 fail-closed 保留 Journal + 人工 replay 接缝。接线后恢复矩阵(§5)自动生效,无需改代码。
3. **expected_crop_rule_version 无客户端权威来源**:当前发 0(§7.1);待 32 号合同冻结作物规则版本下发通道后由 H2 替换。
4. **CLAIM_TASK 的 task_instance_id**:现为 taskDefinitionId 字符串占位(§7.1);待 34 号每日实例模型接线后由任务页面(H2)替换。
5. **FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE 错误码未进 enums.proto**(28 合同层有、枚举无):H5 对好友机会失败按 Loading/Retry 处理,不依赖该码值;待后端枚举补全。
6. **好友/分享链路仍走 HTTPS 既有端点**(非 33 命令):与 SO0 的 GET_FRIEND_LIST/GET_NEXT_FRIEND_OPPORTUNITY/GENERATE_SHARE_TOKEN 等收敛由 H2 页面任务处理。

### 9.2 未跑的真实浏览器 E2E

- 本机运行的 docker 栈(`farm-game-local-*`,8080/19201)是 2026-08-03 从 `farm-game-w3b` 构建的 **P1B 前 v3 协议**镜像,与本批 v4 客户端不兼容;重建/替换该共享 compose 项目会影响其他工作树使用者,本批未动它。
- 待跑(建议由 H2 或独立验证任务在 v4 栈上执行):
  ```text
  cd <farm-game @ cp2-round2+> && bash scripts/company-redis-compose.sh up -d --build
  cd classic-farm-h5-h1-wire-reducer
  npm run e2e:web                                   # web-account(注册/登录/好友)
  FARM_WORKFLOW_E2E=1 FARM_GAME_ROOT=<同上> npm run e2e:web   # W5B 恢复链
  npm run e2e:multigate                             # 双 Gate Lease/Drain
  ```
- e2e 规格已按 v4 基线行为修正钱包未知容错(§7.3);**遗留**:staging.spec 的图鉴断言(expectCodexUnlocked)依赖 Harvest 回包携带 codex,最终协议 HarvestResult 不带 PrivateState,图鉴数据通道属 GET_COMPENDIUM(H2);该断言在 H2 图鉴接线前会失败,已在规格中保留原样以便 H2 一并处理。

### 9.3 H2 接缝

- 页面层本地成熟推导/写死 totalMs=10_000/crop-catalog 价格镜像:reducer 已提供 crop_phase/mature_at,H2 页面切换到服务端字段直读。
- types.ts IR 的 `plotId: 'plot-N'` 字符串与 PrivateState.tasks/codex 展示形状:H2 页面改用数值 plot_index 与 34/37 视图模型后删除(§1.3.6)。
- CONFIRM_FRIEND_VISIT(SUBSCRIPTION_READY 后自动发送)、GET_NEXT_FRIEND_OPPORTUNITY 首屏并行、REDEEM_SHARE 的 Locator 保存+Token 清除(28 §3.8 约束 11):协议面已就绪,待 H2 好友/分享页面接入。
- 命令目录客户端镜像(可用性/特性开关展示):当前依赖 1007 响应逐命令呈现;若产品需要入口级置灰,需后端下发可用性清单(合同外,建议 ICR)。
