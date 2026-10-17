# RT2M routeplan / routectl migration handoff

日期：2026-08-10
角色：RT2M 唯一主 Writer
ICR：Executor final-zero READY
本轮 Executor final-zero 修复的精确 base：`8ff4d2af7d414cbdcb401706f9c0d436ce1463fb`
前轮 ultimate review 修复的精确 base：`fb4621cd8ebd50355383f87204eb14c81e871e6b`
前轮 final exact review 修复的精确 base：`15f2b66db50d10d500044af0bc9d94f3f47e9834`
分支：`codex/rt2m-routectl-migration-20260809`
远端：本轮不 push；最终 SSH 签名 commit 见 branch tip。

## RT2W unified closure addendum（2026-08-10）

以下 addendum 仅取代本文后部“production VerificationPort 尚未接线”的历史阻断结论；此前
RT2M exact heads 的实现与证据仍按原文保留。

- C1 `7fb4b7ed0ff560c51df3eadaf9cce03fda4d095d`：完整分离 LogicEndpoint 与
  HolderControlEndpoint；
- C2 `79c82db4381b6d759df4dd3aca91f14178622539`：所有 remote role 共享完整 Redis
  tuple，并只公开 versioned SHA-256 fingerprint；
- C3 `2ac16ffb33d3efafe8e16bfc0dedc3dfca4b918b`：production `newPlanRuntime` 注入真实
  non-nil/non-noop VerificationPort。Plan 执行与 verification 共用唯一 Plan→Manager typed
  request builder；Manager 保留 exact committed+published recovery，重启后仍可只读观察，并在 hot
  memo eviction 时原子替换为 tombstone；Redis observer 只读取已知五个 key 与 exact transition ID。
- exact-head Endpoint/Redis closure
  `c65226a09fc2c1df3b80e25421d07daff6e725ac`：Go 与 Holder Lua 独立执行同一
  canonical socket grammar；每个 Manager side effect 前重验 Logic/HolderControl endpoint；
  company Redis live probe 仅显式 opt-in，使用独占 SETNX key 并以 DEL+EXISTS=0 证明清理。
- exact-head VerificationPort closure
  `ff34b9a3bc1b8ea09b5bf3eb8cf277065965c4bf`：三个 Manager predicate 接收同一不可变
  Authority/Snapshot，绑定 exact terminal result 与 durable recovery/tombstone；Gate client auth
  仅接受 exact single DNS leaf，Gate/Executor 在最终 PASS 前重新执行 lease freshness 边界；
  present malformed/zero/negative/out-of-bound verifier timeout 启动失败。
- final exact-review closure
  `38ce1074c9e62d11f0ff6de7490aab39c53e15fb`：最终 `tombstone` check 接受 Manager 正常保留、
  fully validated 的 `CompleteRecovery` durable replay floor，或 capacity eviction 原子生成的
  exact Tombstone successor；transition/current generation/result/fingerprint 与独立 recovery
  predicate 不降级。真实 Manager→PlanExecutor→ProductionVerification 用例在 eviction 前后均证明
  routectl-compatible 十一个 check（含 final freshness）整体 PASS。缺失、pending、mismatch、corrupt
  与 generation N/current N+1 负例继续 fail closed。该 SHA 同时收口 Holder Lua IPv4-mapped IPv6、
  company Redis probe 原子 owned-marker cleanup，以及 remote Player/Farm tuple namespace 构造三项 P2。

既有检查名称/code 保持独立，并新增 code-only `final_freshness`：每个 Gate 的 exact
tuple/generation/durable revision、Manager exact typed transition、无 unresolved projection 的
recovery 证据、以及 durable replay floor。
任何 unknown、timeout、legacy gap、corruption、missing 或 dependency unavailable 都 overall false，
Verify 不恢复或修改 Plan/Snapshot/transition/recovery/tombstone。此处只确认真实 production adapter
已组成并通过 unit/component、loopback mTLS 与 exact company-Redis probe；不宣称 RT2Q Final、
runtime multi-Pod、Premerge、G1/G2/G3、CP 或 autoscaling。前两份与 final exact Reviews 的发现已由
唯一 Writer 收口，但未宣称 post-fix 独立 `NO FINDINGS` 复审。single-controller non-HA 合同不变。

Final closure SHA 的 affected packages `-count=20`、全部新增 exact `-count=100` 与
`-race -count=100`、full/full-race、vet、build、Proto clean、base+remote Compose 均通过且无 rerun。
显式 opt-in 的 company Redis production/live run 通过；cryptographic unique tuple key 只执行一个
fixed atomic marker-equals-then-DEL cleanup，并仅在 owned delete 后验证 `EXISTS=0`，无 Redis URL 或
credential 输出。此处仍只是 Writer closure evidence，不宣称 post-fix 独立 `NO FINDINGS`；RT2Q
Final、Premerge、G1/G2/G3、CP 与 autoscaling 继续 pending。

## Scope

本轮保持既有 owned set，并加入用户明确授权的一个最小扩展，只写入：

- `internal/routeplan/**`
- `internal/storage/redis/routeplan/**`
- `internal/storage/redis/authority/**` 中既有 Store/Transport owned set：exact-key AuthorityReader、digest helper 委托、write-fate/no-retry 收口及测试
- 用户授权的最小扩展：`internal/actorrouting/authority/manager.go` 及其测试中的 Manager-owned pending projection/ambiguous-commit recovery seam
- 用户授权的最小扩展：`internal/storage/redis/routecontrol/**` 中 recovery codec 与 fixed Lua physical bounds
- 用户授权的纯值 digest helper：`internal/storage/authoritystore/types.go`
- `cmd/route-controller/**` 中 Plan service 的 sealed inline composition
- `cmd/routectl/**`
- 本 handoff

没有修改 RT2I 四 roots/deploy、RT2C public transport 合同、RT2D holder、public proto/generated、`go.mod` 或 `go.sum`。Manager 仍是唯一 generation/transition coordinator；扩展只在既有 transition 内增加 durable attempt/projection recovery seam，没有第二状态机、leader election 或 HA。public proto 未加入 `caller_role`、secret 或 generation；`routectl` 未导入 Redis。

## Executor / codec / store hardening

- AuthorityStore 调用前先持久写入 attempted 边界。新 recovery wire 固定显式编码 snake-case `authority_attempted:false/true`，禁用 `omitempty`；codec 另行记录字段 presence 与 wire version。parent canonical wire 缺字段时一律视为 MAY_HAVE_ATTEMPTED，只能以 exact Authority observation + intent digest 对账，绝不直接删除；只有 current wire 明确 false 才允许 durable cleanup。旧 committed=false/缺字段但 Authority 已提交的 upgrade restart 路径 Authority mutation 总数保持 1，并补发 exact Snapshot。可能已发送/已提交的 error、`TIMEOUT_UNKNOWN` 和 `UNKNOWN` 都保留唯一 durable ambiguous precommit reservation，只有明确 `NOT_SENT`/definitive rejection 才释放。
- Authority observation 原子携带 `last_transition_digest`。digest 使用唯一的 authoritystore 纯值 LP-SHA256 helper，绑定 kind/layout/domain/shard/fence/target holder+boot/TTL；Redis transition 写入和 Manager recovery proof 共享该实现。仅合法 64-hex 但 intent 错误不能 promote，生产 recovery 缺 digest proof 时 fail closed。
- Manager recovery 对 cold-load checkpoint 2 使用可选的 Manager-owned `ColdLoadPrepared` inspector。若 target `GetStatus` 证明 exact `PreparedGeneration`，只补 durable marker 后发布 ACTIVE，不重复 Prepare；未 prepared 才允许一次 prepare-only ColdLoad；mismatch、expired 或生产缺 inspector 均 fail closed。marker 持久绑定 Prepare 接受的完整 Authority（含 `LeaseUntilMs`）；marker 后同 generation renewal 不会在未重新 Prepare 时被发布 ACTIVE，而是 sticky stale、保留 pending。既有 Drain checkpoint/flight/fingerprint 状态机仍是唯一实现。
- `AuthorityReader` 已显示 exact target/transition、但 Snapshot 仍旧时，Executor 持久化精确 `authority_committed_snapshot_pending`，绝不再次调用 `Drain`/`Transfer`。planned、crash、Manager 返回错误/commit-unknown 和 restart recovery 均先按现有 Manager recovery + Authority + Snapshot 对账，事实完成后才成功。
- Manager 从自身 durable recovery deterministic bounded 枚举 committed+projectionPending transition，不依赖 Plan discovery。普通 transition 只以 exact Authority 重发原完整 Snapshot；若精确 ACTIVE Snapshot 已发布但 recovery cleanup 失败，则仅校验并删除 recovery，零重复发布。internal Drain 复用既有 fingerprint/flight/checkpoint/Drain state machine，checkpoint 1/2 跳过 Stop/Wait/Facts、最多执行未完成的 prepare-only cold-load，checkpoint 3 只补发 ACTIVE。整个 seam 不调用 Claim/Transfer/Unassign，不产生第二次 AuthorityStore mutation；stale/mismatch sticky block，失败保留 pending。
- route-controller 启动先做有界 Manager recovery，随后由同一个 process-local owner 持续重试；Plan submit/execute 共用该 guard 并持有到整次操作结束。只有 recovery 成功才可执行下一 Plan，Manager recovery 与 Plan 不会同进程并行；Executor 不重放 Drain/Transfer。
- Submit、每次 Manager 副作用紧前、recovery 和 Verify 都校验 target `CapacityWeight > 0` 及完整 holder ID/boot/endpoint/TLS、可接收状态、fresh lease、Snapshot revision、source fence/Authority。Executor 只调用 Manager 的 validated seam；planned Drain 在 Stop/Wait/Facts 每个 callback 前、Facts 后紧邻 Authority commit、Authority-backed ASSIGNING 后紧邻 ColdLoad、以及 ColdLoad 后紧邻 ACTIVE 发布都用同一 exact validator 重验。Crash Transfer 在 Authority 调用紧前及提交后 Snapshot 发布紧前重验。提交前失效时 Authority/Manager 调用计数为零；提交后失效保持 durable projection pending，重试只续 Manager checkpoint/投影且 Authority mutation 仍为 1。
- 副作用前按 overflow-safe 时间运算要求 logical TTL 严格大于 `OperationTimeout + safety margin`；Manager context 有界。Plan 的物理 audit retention 长于 logical TTL；logical expiry 前先按 Authority/Snapshot 事实对账，已提交操作记录 pending/成功，只有事实仍在 source 才持久 FAILED。
- 所有 background `failPlan`/`pausePlan` 使用独立、最大不超过 `OperationTimeout` 的 bounded context；请求 context 已取消时仍可有界记录 PAUSED，不会永久占 shard singleflight。
- Plan constructor 必须提供不可变 single-controller attestation、controller ID 和 boot ID，否则启动失败。同进程 shard singleflight 保留；没有实现 leader election、HA 或第二套状态机。
- Plan 初次 CAS 原子要求 `PENDING/attempt=0/started=0/non-terminal`；更新 attempt 只允许 same 或 `+1`，禁止 MaxUint overflow，started/terminal/frozen 字段单调。同 phase 只允许冻结字段保持且审计时间合法推进；`Succeeded`/`Failed`/`Paused` 内容不可变，仅允许 exact replay。Go 做类型化校验；Lua 在同一 CAS 内对当前/候选 canonical JSON 的固定顺序 raw immutable segments 直接逐段比较，避免 Lua number 的 2^53 损失，也不信任调用方传入 digest/pre-read。
- `DecodePlan` 在 checksum/hash/JSON 前先以字段上限推导的固定常量限制 canonical、checksum 和 envelope bytes，拒绝 oversized payload。

## Store / transport hardening

- Snapshot read identities 与 operator identities 分离。`ROUTE_CONTROLLER_OPERATOR_IDENTITIES`/flag 是启用 Plan 的必需 canonical unique 集合；Snapshot ACL 仍为原 exact allowed clients。Plan read 使用显式 read set；Move/Drain/Takeover 仅 operator set。普通 Snapshot client 的 Plan mutation 在 service ACL 返回 `PermissionDenied`。
- Redis Plan store 使用 bounded expiry ZSET + fixed Lua reconcile。新写入使用 exact 11-field `route-plan-v2` hash；既有 exact 8-field `route-plan-v1` 记录保持只读/列举兼容，并在首次成功 CAS 原子升级到 v2。每次写前核对 meta/index/restart/accounting 全成员集合、每 Plan exact schema/revision/canonical/checksum/phase/audit/bytes 及 totals。只要 index/accounting 仍存在，任何 Plan hash 缺失都永久判 `CORRUPT`，不因 logical/audit deadline 过去而豁免；logical expired 只从 restart 移除，所有 hash/index/accounting/meta 证据保留到共同 tail TTL 一起消失，因此 retention 内不能重用 PlanID。没有 Redis `SCAN`/`KEYS`。`ALREADY` 只在完整 reconcile 后核对 exact replay；corrupt 情况零写。prepare + Lua 在 single-controller store 内串行，避免合法并发 prepare 的陈旧 batch。
- direct `Load` 使用另一个 fixed read-only Lua/EVALSHA，单次原子完成 TYPE、exact HLEN/HEXISTS、每字段 HSTRLEN 和总上限检查后才 HMGET，污染巨值不会先分配，也没有多命令 TOCTOU。CAS Lua 在首次 mutation 前完成全部错误结论；meta count/bytes 必须 canonical decimal roundtrip，拒绝 `01`、`1.0`、`1e0` 且零 mutation；计数与 TTL 计算避开 2^53 和 overflow。
- Plan index 枚举不再调用普通 ZRANGE：CAS/List 先用 O(1) `ZCARD` 校验 cardinality，再由 fixed read-only Lua 以 `pcall MEMORY USAGE ... SAMPLES 0` 全量精确核对 checked 物理预算，之后才校验 member 长度、lowercase hex canonical identity 与 integral score并返回 bounded list。仅当 Redis/miniredis 不支持 `SAMPLES` 语法且集合不超过5项时允许默认5样本 fallback（此时覆盖全体）；大于5项一律 fail closed，不能被偏斜样本低估。巨 member 在返回 Go 前即拒绝。Plan CAS EVALSHA 使用 command-level `NoRetry`，仅 NOSCRIPT 可 bounded reload/re-exec；commit 后丢 response 保持 ambiguity，caller 通过 exact Load/replay 收敛。expired accounting 的 Redis raw text 与 caller canonical decimal raw exact 比较，`01`/`1e3` 零写拒绝。direct Load 也执行配置后的 logical PlanTTL 上限。
- Transition recovery store 的 fixed Lua 先用 O(1) HLEN/ZCARD 读取 meta/recovery/index/tombstone/index 五个 exact key 的基数，再在 HKEYS/HGET/ZRANGE/HGETALL 前逐一用 `pcall MEMORY USAGE ... SAMPLES 0` 核对由 MaxItems/MaxBytes/MaxRecord/ID overhead checked 推导的 physical ceiling；同样只允许 `<=5` 且 SAMPLES 语法不支持时采用覆盖全体的默认 fallback，`>5` fail closed。recovery/tombstone hash ceiling 使用 checked `4*MaxBytes + indexCeiling`，每个乘加先按 Lua max safe integer 验证，以覆盖 jemalloc size class/SDS/hash allocator rounding；index ceiling 保持原 checked 固定边界。每次 HGET 前已有 HSTRLEN 上限，写入 ID/fingerprint/value 在状态枚举前严格 byte-bound。单个巨 ID/value 只能消耗固定物理预算并 fail closed，不产生无界 Lua table/response；无 KEYS/SCAN。
- Verify 的 PASS 必须同时具有 ACTIVE Authority 且 lease fresh、target holder/boot/lease/SnapshotRevision、Snapshot tuple、Gate resolve、transition/recovery/tombstone 和 Plan 状态的独立事实。任何缺失均生成明确 FAIL check 且 overall false；不会省略或用重复 check 伪造 PASS。
- RPC 原始 TTL 毫秒先校验 `1..MaxRequestedTTL` 再转 duration；shard 直接 `ParseUint(..., 32)`。`drain-holder` 先要求 source holder ID/boot exact、fresh，且必须找到 matching active shards，否则 RPC/CLI 非成功。
- `routectl` dial/RPC timeout 限制 1ms..30s；target 为 bounded canonical `host:port`。DNS 仅 ASCII label、63/253 上限、无 wildcard；IP canonical。TLS 保留标准 chain/hostname 验证，并在 `VerifyConnection` 对 raw SAN 强制恰好一个 exact DNS，拒绝 wildcard、多 SAN、URI/IP/email/CN fallback。
- `routectl` 的 shard flag 显式记录 presence：所有 shard-scoped 命令省略 `--shard` 都返回 usage，显式 `--shard=0` 合法。`shard` 响应必须与 request domain/shard exact，嵌套 Authority 和每个 related Plan 也必须同 identity，nil/mismatch 均非零。`verify` 还要求响应 PlanID/domain/shard exact，required checks 完整、唯一、`ok=true`，并按真实 Executor 合同逐项要求精确 success code：lease/snapshot read/target 为 `OK`，其余分别为 `AUTHORITY_TUPLE`、`SNAPSHOT_TUPLE`、`GATE_RESOLUTION`、`TRANSITION`、`RECOVERY`、`TOMBSTONE`、`PLAN_STATE`、`FINAL_FRESHNESS`；任一错误码即非零。返回 Plan 必须具有 known operation/phase、revision>0、完整 IDs/fence/TTL、lowercase SHA-256 checksum 与单调审计时间，且 consistent PASS 只能对应 `SUCCEEDED`。真实 Executor Verify 经 PlanService 生成的 proto response 已由 CLI 跨层测试接受；proto zero placeholder、bad phase/revision/checksum/times 均非零。
- routecontrol 内部 transition ID/fingerprint 保持 v1 的1..256 byte canonical UTF-8合同。Go 使用 `utf8.ValidString` 并拒绝 `unicode.IsControl/IsSpace`；Lua 在同一原子脚本内逐码点执行严格 canonical UTF-8 decoder，拒绝 overlong、surrogate、`>U+10FFFF`、非法 continuation、Cc 与 Go `unicode.IsSpace` 精确集合。中文、emoji、UUID/hex 均保持可读可写；`0xff`、NBSP、C1/control/whitespace 均 fail closed，因此 parent v1 recovery 与上游 Unicode Move 不会被误拒。
- AuthorityReader 使用 fixed read-only Lua/EVALSHA 单次原子完成 TYPE、exact HLEN/HEXISTS、逐字段 HSTRLEN 上限和 HMGET；没有 HKEYS、多命令撕裂或 mutation。NOSCRIPT reload 有界，key/context exact。
- Redis Authority 写错误只在可证明的 dial 阶段（包括 dial-time `ECONNREFUSED`/DNS/不可达）判定 `UNAVAILABLE_NOT_SENT`；phase-less 或 post-connect read/write errno 一律保持 commit-unknown，防止误删唯一 durable reservation。Claim/Transfer/Unassign 的 Lua pre-mutation layout mismatch 也返回结构化 `UNAVAILABLE_NOT_SENT,LAYOUT_MISMATCH`；Go decoder 仅对 exact 完整 reply 给出 definitive result，short/malformed reply 保持 UNKNOWN。Manager 先按 result 释放 reservation，再传播可选 error。四个 Authority mutation 命令显式关闭 go-redis 自动重试；只保留 bounded `NOSCRIPT` reload/re-exec，因此客户端不会在 Manager 之外暗中重复写。

## Production deployment gate（阻断项）

RT2M 明确冻结为 single-controller non-HA。生产部署必须由 RT2I 在 deploy 变更中证明并 gate：

- controller 始终恰好 1 replica；升级策略为 `Recreate`，新旧实例绝不重叠；
- 注入 immutable controller ID、boot ID 和精确 attestation `single-replica-recreate-no-overlap-v1`；
- 不允许两个 Executor/两个 store 实例并发。违反该合同是 documented P2 deployment defect，不在 RT2M 内新增 leader election 或第二状态机；
- 历史阻断项已由上方 RT2W C3 在 production command root 完成真实只读接线；任何事实缺失仍
  fail closed。该接线与 component evidence 不替代仍待执行的 RT2Q Final multi-Pod runtime evidence。

本轮遵守 owned set，未代写 RT2I deploy。

## Verification

已执行并通过：

- `./scripts/gen-proto.sh`
- `go test -count=20 ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- `go test -count=100 -run 'TestRouteAssignmentManager(CommitThenLostResponseRecoversWithoutMutation|WrongLegalIntentDigestCannotPromote|UnattemptedRecoveryCleanupIsDurableAndRetryable|DefinitiveNotSentErrorReleasesReservation|ValidatedDrainRevalidatesAfterAuthorityBeforeColdLoadAndActive|ValidatedCrashTransferRevalidatesBeforePublication|PreparedGenerationClosesColdLoadMarkerCrashWindow|PendingDrainRecoveryUsesFreshRenewedAuthority|RecoveryPersistsCompletedColdLoadWithoutRepeatingCallback)' ./internal/actorrouting/authority`
- `go test -count=100 -run 'TestPlanExecutor(AuthorityCommitSnapshotPendingNeverReplaysManager|RevalidatesTargetAfterDrainFactsBeforeAuthority|RecoverRevalidatesTargetBeforeManager|RequiresStrictSideEffectTTLBudget|CommitUnknownPausedRecordFactuallyReconciles|RecoveryGuardFailurePreservesCommittedSnapshotPendingReason|VerifyFailsClosedAndRequiresEveryCheck)' ./internal/routeplan`
- `go test -count=100 -run 'TestRedisPlan(StoreDirectLoadUsesBoundedAtomicScript|StoreRejectsNonCanonicalMetaIntegersWithoutWrite|LuaEnforcesInitialAndAttemptAuditInvariants|StoreMissingLogicalExpiredHashIsCorrupt|StoreMissingTailHashIsCorrupt)' ./internal/storage/redis/routeplan`
- `go test -count=100 -run 'TestShard(TransitionCommandsDisableAutomaticRedisRetries|AuthorityReaderExactTupleAndCorruption|AuthorityReaderRejectsDigestAndTupleDrift|AuthorityIntentDigestCrossContract)' ./internal/storage/redis/authority`
- `go test -race ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `git diff --check`

本次基于 `15f2b66db50d10d500044af0bc9d94f3f47e9834` 的 final exact review 另行执行并通过：

- `go test -count=20 ./internal/actorrouting/authority ./internal/storage/redis/authority ./internal/storage/redis/routecontrol ./internal/storage/redis/routeplan ./cmd/routectl`
- 新增 legacy attempted/commit-loss、structured NOT_SENT、physical MEMORY bound、Plan NoRetry/response-loss、expired raw accounting、direct Load TTL 与 routectl Verify structure 用例均 `-count=100`
- transition 巨 ID/巨 value/physical ordering 子用例 `-count=100`
- `go test -race ./internal/actorrouting/authority ./internal/storage/redis/authority ./internal/storage/redis/routecontrol ./internal/storage/redis/routeplan ./cmd/routectl`
- transition concurrent full-physical validation race 压力用例 `-count=3`
- `go test ./...`
- `go test -race ./...`
- `./scripts/gen-proto.sh`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `git diff --check`，以及 proto/generated/go.mod/go.sum diff、routectl Redis import、Lua KEYS/SCAN、added-line secret pattern 的 fail-closed 静态检查

本次基于 `fb4621cd8ebd50355383f87204eb14c81e871e6b` 的 ultimate review 另行执行并通过：

- `go test -count=20 ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/routecontrol ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- routectl 精确 Verify code 与真实 Executor→PlanService→CLI 跨层用例、Plan index `SAMPLES 0`/偏斜大集合、routecontrol `SAMPLES 0`/大 record/当轮 identity 边界用例，以及 NoRetry/attempted/PlanTTL/LAYOUT 回归均 `-count=100`
- `go test -race ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/routecontrol ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- `go test ./...`
- `go test -race ./...`
- `./scripts/gen-proto.sh`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `git diff --check`，以及 owned set、proto/generated/go.mod/go.sum diff、routectl Redis import、Lua KEYS/SCAN、added-line secret pattern 的 fail-closed 静态检查

前轮新增证据覆盖生产 Executor success code 的逐项精确映射和错误码拒绝；Redis 生产 Lua 使用 `MEMORY USAGE ... SAMPLES 0` 全量核算，并证明不支持该语法时 `<=5` 才能完整 fallback、`>5` 偏斜集合 fail closed。前轮 visible-ASCII identity 结论由下述 final-zero 兼容修复明确取代。

本次基于 `8ff4d2af7d414cbdcb401706f9c0d436ce1463fb` 的 Executor final-zero review 另行执行并通过：

- `go test -count=20 ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/routecontrol ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- canonical UTF-8 Go/Lua parity、中文/emoji Redis roundtrip、overlong/surrogate/Unicode越界/坏continuation/NBSP/C1/control fail-closed、checked allocator expansion 最大配置与900 KiB合法record用例均 `-count=100`
- routectl 真实 Executor success codes、Plan CAS NoRetry response-loss、legacy attempted codec 与 Manager committed-Authority reconcile 回归均 `-count=100`
- `go test -race ./internal/actorrouting/authority ./internal/routeplan ./internal/storage/redis/routeplan ./internal/storage/redis/routecontrol ./internal/storage/redis/authority ./cmd/route-controller ./cmd/routectl`
- `go test ./...`
- `go test -race ./...`
- `./scripts/gen-proto.sh`
- `go vet ./...`
- `go build -buildvcs=false ./...`
- `git diff --check`，以及 owned set、proto/generated/go.mod/go.sum diff、routectl Redis import、Lua KEYS/SCAN、added-line secret pattern 的 fail-closed 静态检查

本轮明确取代上一轮 visible-ASCII identity 结论：v1 canonical UTF-8 是兼容合同，中文与 emoji 必须通过；严格拒绝的是非 canonical UTF-8、Unicode Cc 与 `unicode.IsSpace`，而不是所有 non-ASCII codepoint。

覆盖包括 store 真实 commit 后丢 response 的 same-process/restart recovery（Authority mutation 均为 1）、dial/post-connect errno fate、错误合法 digest 拒绝 promote、cold-load Prepare 后 marker 前崩溃的 exact prepared recovery（callback 0）与 mismatch fail-closed、planned Drain 在 Facts 后 target capacity/lease/fence 失效时 Authority calls=0、Manager 真实顺序中 ASSIGNING 后 target 失效不 ColdLoad/不 ACTIVE 且恢复不重复 Authority/Stop/Wait/Facts、Crash 提交后 target 失效不发布且重试 mutation=1、planned/crash snapshot-pending、每次副作用前 target/fence/TTL revalidation、canceled-context 持久 PAUSED、Verify response exact/完整/唯一/一致、shard presence 与显式 0、CAS initial/terminal/frozen/attempt/overflow、direct Load 巨值 bound、非 canonical meta 零写、Redis 多 Plan/同基数替换/duplicate restart/bytes drift/missing hash/exact replay/v1-to-v2 CAS upgrade/retention 内禁止 ID reuse/共同 tail TTL，以及 Snapshot/operator ACL、source-holder、TTL/target/TLS SAN、Authority Lua 原子读取和 digest cross-contract。

裸 `go build ./...` 在当前 linked-worktree 环境仍被 Go VCS stamping 阻断：工具在 module 外层执行 `git status` 并返回 128；这是 stamping 环境错误，不是编译错误。关闭 stamping 的完整产物构建已通过，未修改代码或仓库配置来规避。

静态检查确认变更均在上述 owned set；public proto/generated 文件、`go.mod`、`go.sum` 无 diff；routectl 无 Redis import；Redis Lua 无 `KEYS`/`SCAN`；planned 唯一调用 `Drain`，crash-proof 分支唯一调用 `Transfer`。

受控 nested 独立审计固定 `PATH` 到 explicit OpenAI wrapper，并锁定 official OpenAI `gpt-5.6-sol` / `xhigh`、`model_provider=openai`、`approval=never`、`sandbox=read-only`。首轮审计提出的 durable attempt stage、cold-loaded lease renewal、Lua terminal/frozen 原子校验、`routectl shard` response binding 和 handoff scope 五项均已修正；对 commit-unknown、cold-load、Lua CAS、routectl 四个最终高风险窗口的 bounded follow-up 均返回 `NO ACTIONABLE P1/P2 FINDINGS`。

## Commit

- 最终 commit 使用 SSH 签名；以 commit object 的 `gpgsig` SSH signature 核验
- Commit SHA：见最终 branch tip（本行不自引用 SHA）
- Push：未执行
