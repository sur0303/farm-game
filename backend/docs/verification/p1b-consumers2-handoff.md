# P1B-CONSUMERS-2 · FirstHop/Ingress 最终消费者收口交接

日期：2026-08-04
状态：`COMPLETED / UNIT_PASS(含主集成审查 P1 修复,见 §11)`

```text
task_id:     P1B-CONSUMERS-2
dispatch:    round2-p1b-consumers2-20260804
branch:      codex/p1b-consumers2-20260804
worktree:    /data/home/developer_026/CodeBuddy/20260519141303/farm-game-p1b-consumers2
base_commit: 83e17ad2ce44d1b5f589e62ac0f0c71cb1fa8fa1
final_commit: 见 §11(本文件回填提交为分支 HEAD,以 git log -1 为准)
commits:
  e5b3c2b docs(verification): P1B-CONSUMERS-2 audit matrix frozen before coding
  7419cfc feat(p1b-consumers2): migrate FirstHop consumers to unique CommandCatalog
  7fa3d54 feat(p1b-consumers2): delete deprecated proto compat window
  8bb38a7 test(p1b-consumers2): FirstHop completeness, oversize fail-closed, retired-field forgery proof
  c26ad7e chore(p1b-consumers2): drop trailing blank line in workflow identity
  f678b08 docs(verification): P1B-CONSUMERS-2 handoff final
  5a08ce3 docs(verification): P1B-CONSUMERS-2 handoff head hash
  (P1 修复提交见 §11)
```

## 1. 目标与边界

把 P1B 已交付的 33/33 唯一 CommandCatalog 接回全部 FirstHop 消费者：FirstHop 从原始
typed payload 独立解码，使用 Catalog 的 Decoder/RouteKeyExtractor/StructuralValidator/
CanonicalPayloadBuilder 与 TrustedCommandContext 做权威校验；幂等与 Workflow 不再哈希旧
ClientCommandEnvelope；随后删除 p1b handoff §4.1 登记的 deprecated 编译窗口与 Gate 兼容投影。
未重写 Catalog，未启用任何 feature-disabled 业务命令。

## 2. 审计矩阵(旧消费点 → 最终去向;先于编码冻结,commit e5b3c2b)

复用图例：`Spec`=commandcatalog.Lookup，`Dec`=Spec.Decoder，`RK`=Spec.RouteKeyExtractor，
`CB`=Spec.CanonicalPayloadBuilder，`Val`=Spec.Validator，`TFP`=TargetFarmIDFromPayload。

| # | 旧消费点 | 现状(旧行为) | 最终去向(复用/改/不改) |
|---|---|---|---|
| 1 | internal/logic/trusted/validate.go `ValidateCommand` | 依赖 `cmd.TargetOwnerId`(deprecated 字段)做结构校验;不解码 payload;不看 Dispatch | 重写为 `ValidateCommand(cmd, trusted, dispatch) (*VerifiedCommand, error)`:Spec 查找→Availability→MaxPayloadBytes→Dec 独立解码→Val→Idempotency 策略检查 operation_id→29 §3.4 TrustedContext 形状(join_attempt/farm_context 与 typed target 比对)→RK 重算→DispatchContext 按 RouteMode 复核(ACTOR_AFFINITY domain+actor_id==RK;SERVICE_LB/NONE 不得有 actor_route)→CB 生成唯一权威 hash。复用 Spec/Dec/Val/RK/CB/TFP;不改:不做逐命令签名、不查 Redis、permit 语义 |
| 2 | internal/player/ingress/service.go `ExecuteCommand` | switch 旧 alias(BUY_OFFER/SELL_ITEM);actor turn 内 proto.Unmarshal 旧 playerpb.BuyOfferRequest/SellItemRequest;`idempotency.CommandPayloadHash(整信封)`;实现 GET_PLAYER_BOOTSTRAP/CLAIM_TASK/GET_OPERATION_RESULT | trusted.ValidateCommand 返回 decoded+hash;disabled 命令(含上述三条)在业务接受前 REJECTED+ERR_COMMAND_UNIMPLEMENTED;BUY/SELL 改用 decoded publicpb.BuyRequest/SellRequest(映射 offer_id/count、item_id/count 进现有领域调用);receipt 冲突比较改用权威 hash。不改:Registry.Submit、经济数学、receipt 保留窗、结果 playerpb.EconomyCommandResult 映射 |
| 3 | internal/farm/ingress/service.go `ExecuteCommand` | switch 旧 alias(PLANT_CROP/HARVEST_CROP/WATER_CROP/BEGIN_THEFT)+已删除命令 GET_FARM_SNAPSHOT/LEAVE_FARM 死分支+GET_OPERATION_RESULT;解码旧 farmpb.*Request;消费 cmd.TargetOwnerId/ExpectedFarmVersion/ExpectedPlotVersion/ExpectedCropCycleId;HARVEST/THEFT/CARE 用旧信封 hash,PLANT 已用 CanonicalHash | trusted.ValidateCommand 统一入口;typed payload(publicpb.Plant/Harvest/Care/Theft/JoinFarmRequest)映射进现有 farmpb actor 请求(plot_index/crop_definition_id);expected_plot_version/crop_cycle_id 来自 typed payload;expected_farm_version 无公网来源恒 0(validatePlot 对 0 跳过,28 §8 由 Subscription Fence 承接);四条命令统一 CB 权威 hash;owner/friend 判定用 typed target_farm_id vs session.player_id;删除 GET_FARM_SNAPSHOT/LEAVE_FARM/GET_OPERATION_RESULT 死分支(disabled/unregistered 在业务接受前拒绝)。不改:FarmActor 方法、Workflow 状态机、结算、好友 Permit、emitChange |
| 4 | internal/farm/ingress/downlink.go `join` | `registry.Submit(ctx, req.Command.TargetOwnerId, ...)` | 改用 decoded JoinFarmRequest.target_farm_id(经 RK/join_attempt 双重复核后的同一值)。不改:Hub/ArmSnapshot/回调链 |
| 5 | internal/idempotency/hash.go `CommandPayloadHash` | deterministic marshal 整信封(自知违反 28 §3.5,TODO(32)) | 消费者迁走后无生产调用方:整个 internal/idempotency 包删除(git rm);无第二 hash 路径残留 |
| 6 | internal/workflow/identity.go `CommandPayloadHash` | 包装 idempotency.CommandPayloadHash | 同上:包装删除;StableID/StableKindName 不动(不哈希信封);identity.go 只留注释指向唯一权威来源 |
| 7 | internal/gateway/command/gate.go 兼容投影(约 L970-998) | Gate 把 RK 回填 `cmd.TargetOwnerId`,把 typed expected_plot_version/crop_cycle_id 投影到 deprecated envelope 字段 | 整块删除:RK 留在 Gate 局部变量供 JOIN 流程使用;envelope 原样透传(28 §3.6 Gate 不重编码);FirstHop 自行解码取值。JOIN 流程全部 `cmd.TargetOwnerId` 改用该局部 RK;`trustedContextFor` 的 JoinAttempt.TargetFarmId 同改 |
| 8 | internal/gateway/command/gate.go `leaveCurrentFarm` + `updateFarmContext` LEAVE_FARM 分支 | 死代码:LEAVE_FARM 自 P1B 起未注册,Gate Lookup 即拒;`leaveCurrentFarm` 无调用者 | 机械删除(随 LEAVE_FARM 枚举符号删除否则无法编译);Presence 释放仍由 CleanupSubscription 承接(P1B 已冻结) |
| 9 | internal/gateway/command/routed.go | 硬编码命令 switch(BUY_OFFER/SELL_ITEM/CLAIM_TASK/GET_PLAYER_BOOTSTRAP→player;GET_OPERATION_RESULT 双段回退) | 按 Spec.FirstHop 路由(PlayerSvr→player client;FarmSvr→farm client);未注册/其他 fail closed;GET_OPERATION_RESULT 双段回退删除(disabled 命令在 Gate 准入即拒,不可达) |
| 10 | internal/gateway/command/sharded.go | 同上硬编码 switch + `req.Command.TargetOwnerId` 取 Farm 路由键 | 按 `req.Dispatch.ActorRoute`(Gate 由 Catalog 生成的 RouteDomain+ActorId)解析 Kind/ID;无 actor_route 的 ACTOR_AFFINITY 请求 fail closed;删除旧信封字段读取 |
| 11 | cmd/ws-stress/main.go | v2 线格式:FrameKind/kind/connection_seq/body、TargetOwnerId、旧枚举名、旧 farmpb/playerpb payload、旧 join 屏障 | 机械迁移到 v4:protocol_version=4、uint64 sequence、frame_body oneof、typed payload(publicpb.JoinFarmRequest/BuyRequest/PlantRequest/HarvestRequest/CareRequest CARE_TYPE_WATER)、Join 屏障 ACCEPTED(JoinResultContext)→FARM_SNAPSHOT(FarmSnapshotPush)→SUBSCRIPTION_READY→自家农场发 ClientRecoveryComplete(非自家不发,与 lease-stress 同纪律) |
| 12 | cmd/lease-stress/main.go | v3 线格式:同上旧帧字段+旧枚举+TargetOwnerId | 同 #11;ownerAction 的 buy/plant/water 改新枚举+typed payload |
| 13 | cmd/dualgate-e2e/main.go | P1B 已迁移 | 仅复核(git grep 证明无旧符号);不改 |
| 14 | api/proto/common/frame.proto | FarmFrame.kind=2/connection_seq=3/body=15 deprecated + FrameKind 枚举 | 删除三字段与整个 FrameKind 枚举;`reserved 2, 3, 15;` + `reserved "kind","connection_seq","body";`;oneof 10~14/16 与 sequence=8 不动 |
| 15 | api/proto/common/envelope.proto | ClientCommandEnvelope 字段 5/6/7/8 deprecated;CommandType 6 个别名 + LEAVE_FARM=7/GET_FARM_SNAPSHOT=10 deprecated | 信封:`reserved 5,6,7,8;`+对应四名 reserved。枚举:去 allow_alias;别名只 reserved 名称(2/3/4/5/8/9 仍被新名占用,不得 reserved 数值);LEAVE_FARM/GET_FARM_SNAPSHOT 数值+名称都 reserved(7、10 未注册) |
| 16 | api/proto/common/enums.proto | 无 deprecated 成员 | 不改 |
| 17 | internal/gateway/commandcatalog/pushspec.go + catalog_test.go | 以 `commonpb.CommandType_LEAVE_FARM/GET_FARM_SNAPSHOT` 符号做"未注册"哨兵断言 | 符号删除后的编译机械适配:改用数值 `commonpb.CommandType(7)/(10)` 哨兵;Catalog 33 清单/Spec/Decoder/Canonical 语义零改动(禁止写集内的唯一触点,在此登记) |
| 18 | internal/protocoltest/p1b_schema_test.go | 断言 deprecated 字段/别名存在(编译窗口形状) | 窗口关闭后改为断言:字段号 5~8 与 2/3/15 不再存在(reserved)、别名与 LEAVE_FARM/GET_FARM_SNAPSHOT 名称消失、7/10 仍未注册、33 spec/现行字段号不动;公开黄金(p1b_canonical_golden_test.go 等)不改 |
| 19 | internal/gateway/protocol/codec_test.go `TestRejectsLegacyKindBodyShape` | 用 deprecated 字段构造旧形状帧 | 改用手工 wire bytes(field2 varint + field15 bytes 成为未知字段)证明 Gate 仍 fail closed(ErrEmptyBody);codec.go 不动 |
| 20 | 各消费者直接测试(trusted/player ingress/farm ingress/gate/sharded/routed/idempotency/player actor registry_authority_ingress_test) | 旧枚举名+旧信封字段构造 | 机械迁移到 typed payload+新枚举;player/actor 与 farm/actor 测试仅改信封构造/等值别名替换(禁写集内的 Actor 生产代码零语义改动,在此登记;见 §4 例外说明) |

## 3. 第 28/29 号条款 → 代码 → 测试映射

| 条款 | 代码 | 测试 |
|---|---|---|
| 28 §3.5/L2-PROTO-13 唯一权威 hash=command_type+CB(typed payload) | internal/logic/trusted/validate.go `ValidateCommand` 生成 `VerifiedCommand.PayloadHash`;player/ingress/service.go、farm/ingress/service.go 全部消费该值 | trusted/validate_test.go `TestValidateCommandCanonicalHash`(重放稳定/噪声不变/字段顺序无关/不同语义冲突);player/ingress/service_test.go `TestBuySellAndOperationIdempotency`、`TestBuyCommittedButTimeoutReturnsProcessingThenReplayWithoutDoubleMutation`;farm/ingress/service_test.go `TestPlantWorkflowRejectsOperationIDWithDifferentIntent`、`TestFriendCareAndTheftLoop`(重试不重复结算) |
| 28 §3.6/L2-PROTO-14 Gate 解码一次+保留 raw;FirstHop 独立再解码 | gateway/command/gate.go `handleCommand`(保留原有单次解码,envelope 不再回填);trusted.ValidateCommand 独立 `spec.Decoder(cmd.Payload)` | trusted 全部测试直接构造 raw bytes;protocoltest/golden 不变 |
| 28 §3.4 大小/畸形/未知字段 fail closed | validate.go: `PayloadExceedsLimit`→`spec.Decoder`→`spec.Validator`→CB(未知字段经 contracts.CanonicalBytes 拒绝) | `TestOversizePayloadFailsClosed`、`TestValidateCommandAdmissionPolicy`(malformed/unknown-fields 用例)、player ingress `TestFirstHopRejectsMalformedAndForgedRoute` |
| 28 §6.1 Availability:disabled 明确拒绝 | Gate 原有准入拒绝(不动)+validate.go `ErrCommandDisabled`;ingress 映射 REJECTED+ERR_COMMAND_UNIMPLEMENTED | trusted `TestFirstHopCompletenessAcrossCatalog`(26 条 disabled 逐条拒绝);player ingress `TestDisabledCommandsRejectedBeforeBusiness`、`TestHarvestTaskProgressAccruesWhileClaimTaskStaysDisabled` |
| 28 §6.4-13/29 §5 FirstHop 复核 RouteKey 与 RouteHeader 一致 | validate.go `verifyDispatch`:RK 重算==ActorRoute.ActorId 且 domain 匹配 | trusted `TestValidateCommandAdmissionPolicy`(forged actor route/wrong domain/missing header/zero target);player ingress `TestFirstHopRejectsMalformedAndForgedRoute`(RPC 层 fail closed) |
| 29 §3.4 JOIN join_attempt 互斥 farm_context;订阅命令反之且 admitted_farm_id==typed target | validate.go `validateTrustedShape` | trusted admission 矩阵(join ok/缺 attempt/farm_context 误带/target mismatch;farm 命令缺 farm_context/admitted mismatch/误带 join_attempt) |
| 29 §4.0/L2-PROTO-23 ACTOR_AFFINITY 必有 header;SERVICE_LB/NONE 不得伪造 | gate.go `dispatchForCommand`(原有)+validate.go `verifyDispatch` 双向复核 | trusted admission 矩阵 SERVICE_LB/NONE 分支;sharded_test.go 路由测试 |
| L2-PROTO-05 操作者身份只来自可信上下文;退役字段不可伪造 | envelope.proto 字段 5/6/7/8 删除+reserved;routeKeyFromSession 不回退 payload(原有) | trusted `TestRetiredEnvelopeFieldsCannotForged`(wire 注入退役字段不影响 route key/hash);protocoltest/p1b_schema_test.go 断言字段号不存在 |
| L2-PROTO-39 BUY/SELL RK=session.player_id | routeKeyFromSession(原有)+verifyDispatch 与 dispatch actor_id 比对 | trusted `TestFirstHopCompletenessAcrossCatalog`(BUY/SELL player 域) |
| 31 §8 EffectiveIdempotencyKey+payload_hash 重放/冲突 | player receipt、farm workflow record 均存 FirstHop 权威 hash 并比对 | player actor `TestAuthorityIngressBuySellAndReplayAfterRestart`(崩溃冷加载后原 operation 重放命中持久 receipt);farm ingress workflow 恢复测试 |

## 4. 实际修改文件与禁止写集核对

`git diff --name-status 83e17ad..HEAD` 全量清单:

```text
M  api/gen/common/envelope.pb.go                    (gen-proto.sh 生成)
M  api/gen/common/frame.pb.go                       (gen-proto.sh 生成)
M  api/proto/common/envelope.proto                  (deprecated 窗口删除+reserved)
M  api/proto/common/frame.proto                     (deprecated 窗口+FrameKind 删除+reserved)
M  cmd/lease-stress/main.go                         (v4 机械迁移)
M  cmd/ws-stress/main.go + main_test.go             (v4 机械迁移+屏障测试)
A  docs/verification/p1b-consumers2-handoff.md      (本文件)
M  internal/farm/actor/actor.go                     (例外①:等值别名替换,见下)
M  internal/farm/actor/delivery_guard_test.go       (例外①:同上,仅测试)
M  internal/farm/actor/unknown_test.go              (例外①:同上,仅测试)
M  internal/farm/ingress/{service,downlink}.go + 4 个直接测试
M  internal/gateway/command/{gate,routed,sharded}.go + 5 个直接测试
M  internal/gateway/commandcatalog/pushspec.go + catalog_test.go (例外②:哨兵数值化)
M  internal/gateway/protocol/codec_test.go          (例外③:旧形状测试手工 wire 化)
D  internal/idempotency/hash.go + hash_test.go      (旧信封 hash 删除,无生产调用方)
M  internal/logic/trusted/validate.go + validate_test.go
M  internal/player/actor/registry_authority_ingress_test.go (例外④:仅测试信封构造)
M  internal/player/ingress/service.go + service_test.go
M  internal/protocoltest/p1b_schema_test.go         (例外⑤:窗口存在断言→缺席断言)
M  internal/workflow/identity.go                    (删除旧 hash 包装;StableID 不动)
```

禁止写集逐项核对:

- **CommandCatalog 33 清单/CommandSpec/Decoder/Canonical 语义/Payload/Result/公开黄金**:
  catalog.go、spec.go、decoders.go、canonical.go、routekey.go、retry_directive.go 与
  api/proto/public/** 对 base **零 diff**;canonical 黄金测试未改。
  例外②(pushspec.go/catalog_test.go)仅把"未注册哨兵"从已删除符号换成等值数值
  `commonpb.CommandType(7)/(10)`,断言语义不变。
- **v4 现行字段号、33 个 CommandType 编号**:frame.proto oneof 10~14/16、sequence=8、
  protocol_version=1 不动;envelope.proto request_id=10/operation_id=2/command_type=4/
  payload=15 不动;CommandType 现行 33 个编号不动;只删除已登记 deprecated 成员并
  reserved(别名只 reserved 名称,2/3/4/5/8/9 数值仍由新名占用;7/10 数值+名称 reserved)。
- **Actor Runtime/Registry/AuthorityStore、PlayerActor/FarmActor 状态与领域规则**:
  例外①——actor.go 的 5 处 `a.save(..., commonpb.CommandType_PLANT_CROP 等)` 是
  allow_alias 等值符号(PLANT_CROP≡PLANT=4 等),替换为规范名后持久化数值逐字节相同,
  属于枚举符号删除的机械编译适配,不是规则改动;两个 actor 测试同理。
  例外④——registry_authority_ingress_test.go 只改测试信封构造(BUY/SELL typed payload
  +DispatchContext),并把 disabled 的 GET_OPERATION_RESULT 轮询改为同 operation 重放
  (等效且更符合 28 §3.2);Actor 生产代码未动。
- **Workflow 状态机/Repository/Worker/Dispatcher**:仅 identity.go 删除
  CommandPayloadHash 包装(消费者已全部改用 CB);状态机/存储/Worker 零 diff。
- **Redis 公共 KeyBuilder/Lua/物理布局**:零 diff。
- **H5、26~37 合同正文、cmd composition root(player-svr/farm-svr/farm-gate/farm-worker)**:零 diff。
- **api/proto/public/**:零 diff;无需 ICR(未发现合同缺口)。
- **GET_OPERATION_RESULT/GET_PLAYER_BOOTSTRAP 等 disabled 业务**:未实现、未启用;
  其在 ingress 内的旧残留实现分支被移除,FirstHop 现在与 Gate 一致地明确拒绝(§9)。

例外汇总(均已在 §2 审计矩阵预先登记):①farm/actor 等值别名替换(含 2 个测试);
②commandcatalog 哨兵数值化;③protocol codec_test 旧形状测试手工 wire 化;
④player/actor ingress 集成测试信封构造;⑤protocoltest schema 形状断言反转。
除此之外无任何禁止写集触点。

## 5. deprecated 删除证明与剩余 git grep

删除前清零证明(消费者迁移完成后、proto 删除前执行):生产代码、测试与工具对旧成员的
引用已全部迁移(见 commit 7419cfc)。

删除后证据(HEAD 上执行):

```text
# 旧命令符号/FrameKind(排除生成的 api/gen):
grep -rn --include=*.go -E "CommandType_(BUY_OFFER|SELL_ITEM|PLANT_CROP|HARVEST_CROP|WATER_CROP|BEGIN_THEFT|LEAVE_FARM|GET_FARM_SNAPSHOT)\b|FrameKind\b" . | grep -v "^./api/gen/"
→ 仅剩 internal/gateway/command/{gate_test,w2a_join_test}.go 的 **testFrameKind**
  (测试本地帧大类判别符,非 proto 符号;FrameKind 删除后测试编译所需的合法替代)

# 信封退役字段访问器(排除 api/gen 与其他消息的合法同名字段):
grep -rn --include=*.go -E "\.(TargetOwnerId|ExpectedFarmVersion|ExpectedPlotVersion|ExpectedCropCycleId|ConnectionSeq)\b|Get(TargetOwnerId|ExpectedFarmVersion|ExpectedCropCycleId|ConnectionSeq)\(\)" internal/ cmd/
→ 仅剩:
  internal/friend/permit.go        friendpb.ActionPermit.TargetOwnerId(好友 Permit 内部消息字段,合法)
  internal/farm/domain/{plant,transition}.go 领域结构 ExpectedPlotVersion(DF0 内部领域类型,合法)
  internal/gateway/auth/share.go   HTTP 分享预览 JSON 字段(合法)
  internal/storage/ownercoresize/size_test.go settlement.TargetOwnerId(合法)
  各测试中的 publicpb/farmpb typed payload 字段(新合同字段,合法)

# api/gen 内:旧 Go 标识符(TargetOwnerId/FrameKind/CommandType_BUY_OFFER 等)零残留;
# reserved 名称只出现在 descriptor 字节串中(proto 运行时元数据,合法)。
```

## 6. 验证命令原始结论

全部在 HEAD(c26ad7e)真实执行:

```text
bash scripts/gen-proto.sh;再次执行后 git status --porcelain api/gen/ 计数 = 0(幂等零差异;
  另经 md5sum 双跑比对 OK)

go test -race -count=1 -timeout=300s ./internal/gateway/commandcatalog/... ./internal/gateway/command/... \
  ./internal/logic/trusted/... ./internal/player/ingress/... ./internal/farm/ingress/... ./internal/workflow/...
ok  farmgame/internal/gateway/commandcatalog  1.038s
ok  farmgame/internal/gateway/command         3.962s
ok  farmgame/internal/logic/trusted           1.035s
ok  farmgame/internal/player/ingress          1.039s
ok  farmgame/internal/farm/ingress            1.113s
ok  farmgame/internal/workflow                1.109s
(注:internal/idempotency 包已删除,验收命令按实际包名适配后不含该包)

go test -race -count=1 -timeout=600s ./...   PASS(全仓,无 FAIL/panic 输出)
go vet ./...                                  PASS(exit=0)
go build -buildvcs=false ./...                PASS(exit=0)
git diff --check 83e17ad2ce44d1b5f589e62ac0f0c71cb1fa8fa1..HEAD  PASS(exit=0)
git status --porcelain                        空(exit=0 输出 0 行)
```

已知与本任务无关的既有项:`gofmt -l internal/workflow/` 在 base 即列出
backoff.go/engine_crash_test.go/testworkflow_test.go/workrecord.go(WF1 交付时的
格式漂移);该目录除 identity.go 外属禁止写集,未顺手格式化。

## 7. enabled/disabled 命令清单未变化的机械证明

- `git diff 83e17ad..HEAD -- internal/gateway/commandcatalog/catalog.go` 为空(全部 33 条
  CommandSpec 定义字节不变,含每条 Availability/FeatureKey)。
- spec.go/decoders.go/canonical.go/routekey.go/retry_directive.go 与 api/proto/public/**
  对 base 零 diff。
- 运行时枚举证明:trusted `TestFirstHopCompletenessAcrossCatalog` 对 `commandcatalog.All()`
  逐条执行 FirstHop 验证,断言 enabled=7/disabled=26 且逐条名称级通过
  (JOIN_FARM/BUY/SELL/PLANT/HARVEST/CARE/THEFT 启用,其余 26 条 ErrCommandDisabled)。
- commandcatalog 既有 catalog_test.go(33/33 exactly-once、准入/Retry/Recovery 矩阵)全绿。

## 8. 未解决项 / ICR / 对 CP3/PI1/FI1 的接缝

- P0:无。P1:主集成审查发现 1 个合并前 P1,已在本分支修复(§11);修复后无已知 P0/P1。
  ICR:无(未发现合同缺口,api/proto/public 零改动)。
- P2(既有,非本轮引入):`trusted.MaxRequestIDBytes=128` 与 28 §3.1 的 request_id 1..64
  上限不一致(Gate 未强制,FirstHop 用 128);本轮保持原值不改行为,留给后续接入层硬化任务。
- P2(行为说明):旧 player ingress 对 BUY/SELL 空 payload 有"默认 1 份胡萝卜种子/胡萝卜"
  的填充语义;新 typed 契约下 offer_id/item_id 为必填语义字段,零值按 OFFER_NOT_FOUND/
  INSUFFICIENT_ITEM 拒绝。H5(H1)与 dualgate-e2e 均显式提交,无线上影响。
- P2(安全硬化说明):迁移前 Gate 的兼容投影不覆盖 CARE/THEFT 的 expected_plot_version,
  客户端可在旧 envelope 字段夹带;迁移后 CareRequest/TheftRequest 无该字段,FirstHop 恒传 0
  (跳过校验),夹带通道在 schema 层消失。
- GET_OPERATION_RESULT 恢复空洞(沿用 P1B §9.2):TIMEOUT→PROCESSING 的客户端轮询仍不可用
  (命令 disabled);弱网恢复按 RecoveryPolicy 以原 operation_id+原 payload 重发,已由
  FirstHop 权威 hash + 持久 receipt/workflow 命中证明(玩家域含崩溃冷加载重放)。
  结果目录轮询接线属后续(R1 ResultDirectory + 专门接线任务)。
- 对 CP3:本分支即候选;建议的独立只读审查重点:trusted.ValidateCommand 复核顺序、
  ingress 映射层、proto reserved 合法性与 §4 五项例外。
- 对 PI1/FI1:ingress 与 Actor 之间的新接缝是 `trusted.VerifiedCommand`
  (Decoded proto.Message + RouteKey + PayloadHash);PI1 接 GET_PLAYER_BOOTSTRAP 时应消费
  decoded GetPlayerBootstrapRequest 并按 §3.7 七字段 Result 映射,不需要再解码。
- 对 H5:无新动作;H1 已按最终协议交付,本轮未改任何公网语义。

## 9. 明确未启用的业务命令

GET_OPERATION_RESULT、GET_PLAYER_BOOTSTRAP 及其余 24 条 disabled 命令本轮未接线:
Gate 准入(既有)与 FirstHop 复核(新增,ErrCommandDisabled→REJECTED+
ERR_COMMAND_UNIMPLEMENTED)双层明确拒绝,无任何伪成功路径;player/farm ingress 内
旧有的 bootstrap/claimTask/GET_OPERATION_RESULT 实现分支已随 disabled 边界一并移除。
enabled 集合保持 7 条:JOIN_FARM、BUY、SELL、PLANT、HARVEST、CARE(仅 WATER)、THEFT。

## 10. evidence_level

`UNIT_PASS`。依据:§6 与 §11 全部命令在本 worktree 真实执行。不申报 INTEGRATION_PASS:
未起真实多进程;dualgate-e2e/ws-stress/lease-stress 已迁移至 v4 可编译可用,
真实多进程复跑属后续验证任务。

## 11. 主集成审查 P1 修复记录(2026-08-04,同一 writer 会话续作)

**发现**:主集成审查在候选 5a08ce3 上发现 1 个合并前 P1。

**根因**:`internal/logic/trusted/validate.go::validateTrustedShape` 只按
`RequiresCurrentSubscription` 检查 `target == admitted_farm`,没有执行 Catalog 的完整
`SessionAdmission.TargetFarmPolicy`(28 §6.1)。对 `TargetFarmOwnCurrentFarm`,合同要求
`target == admitted_farm_id == session.player_id`;旧实现会接受"玩家已订阅好友农场、
PLANT/HARVEST payload 目标也是该好友农场"的请求。farm ingress 业务方法当时仍会二次拒绝,
因此不是现时可利用越权,但违反 FirstHop 独立复核合同(29 §5),并给未来 enabled 命令留下
绕过风险。`TestFirstHopCompletenessAcrossCatalog` 此前用 `farmID=20002` 配
`session playerID=10001` 构造 PLANT/HARVEST"合法"样例,掩盖了缺口。

**修复**(只动 trusted 包与其直接测试;Catalog/Gate/ingress 业务校验/Actor/Workflow 零改动):
- `validateTrustedShape` 末尾接入新的 `validateTargetFarmPolicy`,按 policy 完整复核并
  fail closed:PAYLOAD_TARGET(typed target 存在非 0;JOIN 仍比对 join_attempt)、
  SESSION_PLAYER(操作者只来自 session,payload/上下文不得改向)、
  OWN_CURRENT_FARM(target==admitted==session.player_id)、
  CURRENT_FARM(target==admitted)、
  CURRENT_FRIEND_FARM(target==admitted 且 target!=session.player_id;对应命令当前
  disabled,提前防启用即绕过);其余 policy(SUBJECT_ROOT_OPERATION/TOKEN_INVITER/
  CALLER_PAYLOAD_PEER/NONE)当前仅属 disabled 命令,若在 enabled 命令上出现则 fail closed。
- 修正 `TestFirstHopCompletenessAcrossCatalog`:PLANT/HARVEST 合法目标改为 playerID,
  CARE/THEFT 用当前订阅 farmID;farm_context/dispatch 跟随各命令 typed target。
- 新增 `TestTargetFarmPolicyFirstHopEnforcement` 定点回归:session=A+admitted=B+
  PLANT/HARVEST target=B 拒绝;session=A+admitted=A+PLANT/HARVEST target=A 接受;
  CURRENT_FARM 的 CARE/THEFT target==admitted 接受;target≠admitted、route header≠
  重算 target 均 fail closed。

**验证**(修复后 HEAD 真实执行):

```text
go test -race -count=1 ./internal/logic/trusted/ ./internal/farm/ingress/ \
  ./internal/player/ingress/ ./internal/gateway/command/   全 ok
go test -race -count=1 -timeout=600s ./...                  PASS(无 FAIL/panic)
go vet ./...                                                PASS
go build -buildvcs=false ./...                              PASS
bash scripts/gen-proto.sh 后 git status --porcelain api/gen/ = 0(零差异)
git diff --check 83e17ad..HEAD                              PASS
git status --porcelain                                      空
```

**P1 修复提交**:

```text
c8181f0 fix(p1b-consumers2): FirstHop enforces full TargetFarmPolicy (integration review P1)
```
