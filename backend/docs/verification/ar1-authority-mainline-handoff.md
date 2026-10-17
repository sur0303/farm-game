# AR1-AUTHORITY-MAINLINE Handoff

```text
task_id: AR1-AUTHORITY-MAINLINE
contract_baseline: a78196a48d044a350a3b761c1d73b404f44a18c1
code_baseline: cp2-round2-20260804 (2d894be630e5cc7faab53060bf342e9f1be9ed37)
shared_contract_baseline: 44567622d18f65e41f63fe46d164363e4d1aa8e1
branch: codex/ar1-authority-mainline-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-ar1-authority-mainline
status: COMPLETED
```

## 1. 任务边界

目标：把 Player/Farm Actor Runtime 从旧 blobstore 接缝收敛到已合入的 AuthorityStore /
WriterClaim / Writer Fence 主线（合同 27/29/30/31），复用现有
activation/persistence/runtime 与两个 Registry，不创建 actor2/runtime2/第二套 Store，
不顺手补业务玩法。

owned_write_set（本任务实际使用）：

- `internal/logic/actor/**`（新增 `authority` Persistence Adapter 包）
- `internal/player/actor/registry.go` + 新增 Registry/Authority 接线测试
- `internal/farm/actor/registry.go` + 新增 Registry/Authority 接线测试
- `docs/verification/ar1-authority-mainline-handoff.md`

forbidden_write_set（全程未修改，见 §7 证明）：

- `api/proto/**`、`api/gen/**`、CommandCatalog、公网协议
- `internal/player/actor/state.go`、`internal/farm/actor/actor.go` 及领域数值/玩法规则
- `internal/player/ingress/**`、`internal/farm/ingress/**`
- `internal/workflow/**`、Workflow 业务状态机
- `internal/storage/redis/authority/scripts.go`、公共 KeyBuilder/Lua 语义
- `cmd/**`（composition root 由 I0 落地）、H5、`docs/design/26～73` 合同正文

## 2. 复用 / 缺口 / 不改 矩阵（审计先行，编码前完成）

### 2.1 复用（现状已具备，本任务只接线不重写）

| 组件 | 位置 | 现状 |
|---|---|---|
| 有界双队列 Mailbox（User item+byte 双限 / System 独立、System 优先调度、generation 退休） | `internal/logic/actor/runtime/executor.go` | 已具备，两 Registry 均经 `exec.Submit/SubmitSystem` 串行化同 key turn |
| 激活 FSM（ABSENT→LOADING→CLAIM_UNKNOWN/ACTIVE→QUIESCING→PASSIVATING→ABSENT，含 MarkClaimUnknown/ResolveClaimUnknown*/FenceClaimUnknown/MarkLoadReconcileRequired） | `internal/logic/actor/activation/activation.go` | FSM 与加载硬门已具备；CLAIM_UNKNOWN 转移簇此前未被 Registry 调用（为 LoadAndClaim 预留） |
| 同 Actor 并发激活 singleflight（LoadIdentity = ActivationGeneration+ClaimOperationID+NewWriterClaimID，与 LoadAndClaimRequest 字段对齐） | `internal/logic/actor/activation/singleflight.go` | 已具备未接线；本任务接入 Farm 激活路径 |
| 每 Actor 单 Persistence Lane（IDLE/DURABLE_SAVE_INFLIGHT/RESOLVING_UNKNOWN/DRAINING；SoftSave 可关） | `internal/logic/actor/persistence/lane.go` | 已具备未接线；本任务接入 Binding，EnableSoftSave=false |
| idle passivation（两阶段、blocker 快照、mailbox generation 校验退休） | `internal/logic/actor/idle/sweeper.go` + farm registry.go | 已具备，authority 模式原样复用 |
| 新 AuthorityStore 主线（LoadAndClaim/Commit/FenceAndClaim/LookupIdempotencyOutcome；MemoryStore/FaultStore/HolderLeaseGuard） | `internal/storage/authoritystore/**` | R1 已交付，本任务只消费接口 |
| Redis 生产适配器（含 Lua 脚本族、WorkLease、ResultDirectory） | `internal/storage/redis/authority/**` | R1 已交付，未接线；本任务经接口消费（测试用 miniredis） |
| Unknown 隔离与恢复骨架（RESOLVING_UNKNOWN、scheduleUnknownResolution、PostCommitDeliveryGuard） | farm registry.go / delivery_guard.go | 已具备；authority 模式把“权威读 + 记录内 Receipt”升级为 FenceAndClaim(C1→C2) 证明 |
| 失权驱逐（ErrWriterEpochMismatch→evictFenced、NotifyLeaseLost SystemQueue 屏障） | farm registry.go / player registry.go | 已具备，authority 模式复用同一错误语义 |

### 2.2 缺口（本任务补齐）

| 缺口 | 补法 |
|---|---|
| Registry 只接 `blobstore.Store` 旧接缝（player registry.go:67、farm registry.go:72） | 新增 `internal/logic/actor/authority`：Provider/Binding 把 ActorAuthorityStore 映射为 Registry 可用接缝；Registry 新增 `NewAuthorityRegistry` 构造（旧构造原样保留为显式回滚选项） |
| LoadAndClaim 未驱动激活；writer_claim_id 未建立 | Farm `actor()` authority 分支：singleflight 身份 → LoadAndClaim（InitialModelFields=空 Farm InitialRecord）；Player 首次 turn 建立 per-actor claim 绑定 |
| CLAIM_UNKNOWN 恢复未接线 | Farm 激活 authority 分支：Load TIMEOUT_UNKNOWN → MarkClaimUnknown → 原身份幂等重入（有界）→ Active/Fenced/Absent(backoff) |
| Commit 的 WriterClaim/epoch/Receipt 三层 Fence 未进写路径 | Binding.CASEpoch→Commit（ExpectedWriterClaimID+ExpectedRecordVersion+ShardWriteFence+合成幂等 Receipt），状态机八值映射到 blobstore 哨兵错误，ingress 错误映射不变 |
| Timeout Unknown 缺存储级 Resolver | Binding.ResolveUnknown→FenceAndClaim(C1→C2)（resolve 身份稳定幂等）；farm ResolveUnknown authority 分支使用；player 在 turn 内同步解析：Committed→返回原值，NotCommitted→重载后有界重试，其余→ErrTimeoutUnknown 上交 PROCESSING |
| RECORD_VERSION_CONFLICT 无界重试风险 | authority 模式同一 Claim 下首次冲突→重载重算，再次冲突→Fail Closed（不再无限 CAS） |
| 指标接缝缺 authority 侧计数 | authority 包 Metrics：claim/commit/fence/lookup 按结果分类计数；actor 数/激活/Mailbox 深度/passivation/失权沿用既有 activation/executor/idle/actorrouting 指标接缝 |

### 2.3 不改（明确排除）

- `internal/farm/actor/actor.go`、`internal/player/actor/state.go`：领域与 save 主流程不动；FarmActor.save 的 `blobstore.CASAtEpoch` 调用点由 Binding（实现 blobstore.EpochStore）承接。
- `internal/storage/blobstore/**`：旧接缝原样保留（73 §2.1 允许的显式回滚选项；同一 Authority 同一时刻只由构造路径选择一条写主链，无双写）。
- 两个 Registry 的既有公共 API 与 legacy 模式行为：cmd 与全部既有测试继续编译运行。
- ingress / workflow / proto / catalog / scripts.go / rediskey / cmd / H5。
- 生产在线迁移：只记录为后续项；测试环境明确清空重建（新 namespace/独立 miniredis）。
- ACTOR_SOFT：Lane.EnableSoftSave=false 硬接线，不新增任何“内存先成功后异步落库”路径。

## 3. 27/30/31 条款 → 代码/测试映射

| 条款 | 代码 | 测试 |
|---|---|---|
| 30 §5.1/§5.3 激活主链:Singleflight 合并加载 + LoadAndClaim(claim_operation_id+new_writer_claim_id 幂等) | `internal/farm/actor/registry.go` `actorAuthority`/`loadAndClaimActor`(LoadFutureGroup 共享 LoadIdentity);`internal/logic/actor/authority/authority.go` `Provider.Claim`/`NewClaimIdentity` | `TestAuthorityFarmEmptyActivation`(memory+redis,claim 恰好 1 次);`TestAuthorityFarmConcurrentActivationSingleLoad`(32 并发,StartedTotal=1,LoadAndClaim=claim+read 共 2 次);`TestClaimUnknownIdempotentRecovery` |
| 30 §5.4 CLAIM_UNKNOWN 五义务(不安装半初始化/原身份幂等重入/不生成 C3/有界恢复) | `loadAndClaimActor`:MarkClaimUnknown → 原身份有界重入 ×3 → ResolveClaimUnknownToActive / FenceClaimUnknown / ResolveClaimUnknownToAbsent(退避) | `TestClaimUnknownIdempotentRecovery`(AfterApply 后原身份重入成功,无新 Claim 身份) |
| 27 §6.1 每次新 Activation 建立新持久 writer_claim_id | `Provider.NewClaimIdentity`(bootID+actor+单调序号);钝化保留旧 Claim,重激活原子换代 | `TestStaleWriterLateWriteRejected`(claim 换代断言);`TestAuthorityFarmPassivationReloadsReceipt` |
| 30 §6.2 Commit 八值分类;RECORD_VERSION_CONFLICT 同 Claim 最多重载重算一次 | `Binding.CASEpoch` 状态映射;`player submitAuthority` conflicts≥2 → Fail Closed | `TestClaimGetInsertCommitRoundTrip`(过期版本 CAS 冲突);合成键按版本划定作用域,IdempotencyConflict(同版本键不同内容)≡ 版本过期,映射为 CAS 冲突(注释记录) |
| 30 §6.1/§10.2 COMMITTED 后才 InstallState;业务效果与 Receipt 同提交 | `Binding.CASEpoch`:一次 Commit 原子携带 CandidateState+Receipt+Outcome;Actor 侧 save/turn 逻辑未改 | `TestAuthorityFarmCommitUnknownResolvedCommitted`(效果恰好一次);`TestAuthorityPlayerActorSoftStaysDisabled`(Durable 水位==RecordVersion) |
| 30 §7/27 §6.4 TIMEOUT_UNKNOWN → RESOLVING_UNKNOWN → FenceAndClaim(C1→C2),不盲重试 | `Binding.ResolveUnknown`(稳定 Resolver 身份:`resolve_<cas_op>` + `wf_<hash>`);`farm resolveUnknownAuthority`(Fence 结论 + 记录内业务 Receipt 交叉校验);`player submitAuthority`/`resolvePending` | `TestAuthorityFarmCommitUnknownResolved{Committed,NotCommitted}`;`TestAuthorityPlayerCommitUnknownResolved{Committed,NotCommitted}`;`TestAuthorityPlayerUnknownBlocksThenNextTurnResolves`;`fence_unknown_keeps_stable_resolver_identity` |
| 30 §7.6 Unknown 未解析前保守阻塞该 Actor 全部业务写 | `Binding.CASEpoch`/`InstallInitial` pending 守卫;Farm 由 RESOLVING_UNKNOWN FSM 阻塞 | `TestCommitUnknownResolvedCommittedAndNotCommitted`(blocked 断言) |
| 30 §9.1-9.5 生命周期/Passivation;30 §9.3 钝化保留旧 Claim | 复用既有 activation.Controller 两阶段钝化;不触碰持久 Claim | `TestAuthorityFarmPassivationReloadsReceipt`(重激活从 AuthorityRecord 恢复 Receipt/Outcome,ReactivatedTotal=1) |
| 30 §12.1/§12.2 Lane 四态 + R1_ACTOR_SOFT 关闭 | `Binding.lane = persistence.NewLane(EnableSoftSave=false)`;所有写走 BeginDurable/Complete/ResolveUnknown | `TestAuthorityFarmActorSoftStaysDisabled`/`TestAuthorityPlayerActorSoftStaysDisabled`(BeginSoft→ErrSoftSaveDisabled);`TestAuthorityPlayerCommitUnknownResolvedCommitted`(AfterApply 时命令不返回内存成功) |
| 27 §6.4/§16-28/30 失权自 Fence;旧 Holder/旧 Claim 迟到写被拒 | `Binding.CASEpoch`/`Get`:WRITER_EPOCH/CLAIM_MISMATCH → blobstore.ErrWriterEpochMismatch;Farm 走既有 evictFenced;Player dropBinding | `TestAuthorityFarmStaleWriterLateWriteRejected`(驱逐至 ABSENT);`TestAuthorityPlayerStaleWriterLateWriteRejected`(绑定丢弃+指标);`TestStaleWriterLateWriteRejected`(新 Holder 接管读到旧提交) |
| 31 §6.7 错误分类(明确未发送可重试/已发送未知进 Resolver/合同错误 Fail Closed) | `Binding`:UnavailableNotSent→ErrUnavailable;TimeoutUnknown→ErrTimeoutUnknown+pending;Reconcile/地址错误原样上交 | authority 包 FaultStore 表驱动用例 |
| 30 §15/31 §16 指标接缝 | `authority.Metrics`(claim/commit/resolve 按结果分类)+ 复用 activation/executor/idle 既有 Stats;`Registry.AuthorityMetrics/AuthorityStats/LoadFutureStats`(每 Actor 记录字节、激活、Mailbox 深度/等待、钝化、失权) | 各测试内指标断言 |
| 崩溃冷加载恢复(27 §20-6) | 持久 Claim 由新 Activation 原子换代;记录/Receipt 全量在 AuthorityRecord | `TestAuthorityFarmColdLoadRecovery`(memory+redis);`TestAuthorityPlayerEnsureBuySellAndColdLoad`;`TestAuthorityIngressBuySellAndReplayAfterRestart`(重启后 GET_OPERATION_RESULT 重放) |
| 29 ActorRouteHeader/TrustedContext | 未触碰(P1B-CONSUMERS-2 范围);FenceSource 是 Holder 侧注入接缝(29 §4.3:LeaseEpoch 由 Holder 本地证明注入,不进 Header) | — |

## 4. 验证原始结果

base:`cp2-round2-20260804`(2d894be)。环境说明:本机 `go build ./...`/`go vet ./...` 的 VCS stamping 在此 worktree 报 `error obtaining VCS status: exit status 128`(worktree gitdir + safe.directory 配置所致,基线即如此,非本任务引入);按最低验证清单使用 `-buildvcs=false`/`GOFLAGS=-buildvcs=false`。

以下为 P1-1 修复后的最终一轮验证(与首轮结果一致,仅 farm/actor 时长微差):

```text
$ go test -race -count=1 -timeout=240s ./internal/logic/actor/... ./internal/player/actor/... ./internal/farm/actor/... ./internal/storage/authoritystore/... ./internal/storage/redis/authority/...
ok  farmgame/internal/logic/actor/activation   1.024s
ok  farmgame/internal/logic/actor/authority    1.022s
ok  farmgame/internal/logic/actor/idle         1.054s
ok  farmgame/internal/logic/actor/persistence  1.014s
ok  farmgame/internal/logic/actor/runtime      1.411s
ok  farmgame/internal/player/actor             1.206s
ok  farmgame/internal/farm/actor               2.270s
ok  farmgame/internal/storage/authoritystore   1.037s
ok  farmgame/internal/storage/redis/authority  1.487s

$ go test -count=1 -timeout=300s ./...
exit=0;58 个包 ok;无 FAIL(/tmp/ar1_full_test_p1.log)

$ GOFLAGS=-buildvcs=false go vet ./...
VET_OK

$ go build -buildvcs=false ./...
BUILD_OK

$ git diff --check cp2-round2-20260804..HEAD
DIFF_CHECK_OK(无输出)

$ git status --porcelain
(空)
```

证据等级:DOMAIN_LOGIC_PASS / UNIT_PASS / INTEGRATION_PASS(miniredis 执行真实 Lua 脚本族)。非 REAL_REDIS_PASS、非 MULTI_PROCESS_E2E_PASS、非 CAPACITY_PASS。

## 4A. 独立审阅 P1-1 修复(2026-08-04 第二轮)

**根因**:`internal/farm/actor/registry.go` `loadAndClaimActor` —— 首次 LoadAndClaim 得到 TIMEOUT_UNKNOWN,MarkClaimUnknown 后以原身份重入成功时,旧实现立即 `ResolveClaimUnknownToActive`(registry.go:967 旧版);随后 `binding.Get`、记录解码或 farm_id 完整性检查若失败,恢复路径(viaRecovery)跳过 FailLoad,FSM 停留 ACTIVE 而 `r.actors[farmID]` 未安装。后果:后续命令 Admit 看到 ACTIVE 却找不到内存 Actor,`BeginLoad` 从 ACTIVE 报非法转移,farm 永久 fail-closed,直到进程重启或外部失权通知。

**修复**(最小改动,合同一致):
1. CLAIM_UNKNOWN→ACTIVE 的转移推迟到 Actor 安全安装(或确认并发已安装)之后;
2. 安装前所有失败统一走 `fail` 清理:recovery 路径 `ResolveClaimUnknownToAbsent`(释放加载槽并进入失败退避),正常路径 `FailLoad` —— 两条路径都不留 ACTIVE-without-actor;Binding 是局部变量随失败丢弃,持久 Claim 按 30 §9.3 保留,不泄漏;
3. 顺手修复:恢复循环最终以 ClaimUnavailable 结束时保留 `blobstore.ErrUnavailable` 分类(旧代码统一误报 ErrTimeoutUnknown)。

**回归测试**(`internal/farm/actor/registry_authority_test.go`):
- `TestAuthorityFarmClaimUnknownRecoveryGetFailureDoesNotWedge`(get_timeout_unknown / get_unavailable 两子例):ClaimUnknown→原身份重入 ClaimAcquired→激活读失败;断言失败后 ABSENT、无 ACTIVE-without-actor、退避内 ErrLoadBackoff(可重试安全态而非非法转移)、退避后重新激活成功并读到 12 块地;
- `TestAuthorityFarmClaimRecoveryUnavailableKeepsClassification`:恢复循环 ClaimUnavailable 分类保持;
- `TestAuthorityFarmCorruptRecordFailsClosedWithoutWedge`:recovery 分支解码失败(farm_id 完整性检查与解码共用同一 `fail` 路径,该分支即覆盖两者)同样回 ABSENT,且持续干净 fail-closed 不楔死。

**仍保留的 P2**:
- Player Read(stub)→Ensure(InstallInitial)的 Registry 级专项测试未补(stub 语义已在 authority 包单测与 ingress 集成测试中间接覆盖),不阻塞。

## 5. live Redis 执行情况

未执行真实公司 Redis(本任务无公司 Redis 环境/凭据)。已执行:miniredis v2 + go-redis 真实协议栈,跑通 R1 冻结 Lua 脚本族(LoadAndClaimActorV1 / CommitActorMutationWithOrdersV1 / FenceAndClaimActorV1 / ClaimShardAuthorityV1 / TransferShardAuthorityV1),覆盖 Player/Farm 的激活、提交、Unknown 解析、失权拒写、崩溃冷加载全链路。
**PENDING: REAL_REDIS_PASS(公司 Redis ACL/Cluster/Failover/容量探针,归 Q1)。**

## 6. 未解决风险 / ICR / 跨任务接缝

ICR:无(未触碰任何禁止写集,无需合同变更)。

未解决风险:
1. Binding.Get 以幂等 LoadAndClaim(ALREADY_CLAIMED)作读路径:Player 每 turn 一次 LoadAndClaim 调用(与 legacy 每 turn GET 相当,但经 Lua);热路径性能待 Q1 压测。
2. Player 对缺失记录的读/写会先经空 initial Claim 创建 stub 再报 NotFound(外部语义与 legacy 一致);stub 在 Ensure 时经 InstallInitial 幂等补装,无残留业务影响。
3. Shard Lease 续租循环与本地单调 Deadline 热路径 Self-Fence(HolderLeaseGuard)未在 Registry 接线:FenceSource 是注入接缝,生产装配归 I0/composition root(cmd 禁止本任务写入);存储侧 ShardWriteFence 是最终 Fence 且已被测试证明。
4. Actor panic/失权时若有未解析 Unknown,持久结论由记录内 Receipt 与(可选)后续 Resolver 恢复;本地 pending 随绑定丢弃——重激活后重放语义由记录内 Receipt 保证(已测)。
5. 生产旧数据:无双写;切换方式=测试环境清空重建或一次性离线迁移工具(后续任务;cmd 切换归 I0)。

跨任务接缝:
- WF1:Binding Commit 透传 AttachedOrders;当前业务 turn 不产生 Order;PendingOutbox 在 Redis 适配器 fail-closed(R1 已知限制,WF1/后续冻结物理布局)。
- P1B-CONSUMERS-2:ingress 错误映射契约不变(blobstore 哨兵错误语义保持);TrustedContext/FirstHop 未触碰。
- PI1/FI1:装配入口 `NewAuthorityRegistry(provider, opts)`;cmd 主链切换由 I0 落地。
- Q1:live Redis 探针与容量压测;ACTOR_SOFT 白名单评估必须另立任务。

ACTOR_SOFT:仍关闭。authority 模式无任何 SoftSave 开关暴露,Lane 硬接线 EnableSoftSave=false;不存在"内存成功后异步落库"路径(成功返回前 Commit 必须 COMMITTED/ALREADY_COMMITTED,或经 FenceAndClaim 证明 COMMITTED)。
双写:无。Registry 构造二选一(legacy blobstore / authority Provider),同一 Registry 同一时刻只有一条写主链;cmd/* 仍在 legacy(显式回滚选项,73 §2.1 允许)。

## 7. 写集证明

实际修改/新增文件(git diff --stat cp2-round2-20260804..HEAD):

```text
docs/verification/ar1-authority-mainline-handoff.md        | 新增
internal/farm/actor/registry.go                            | 修改(authority 模式接线)
internal/farm/actor/registry_authority_test.go             | 新增
internal/logic/actor/authority/authority.go                | 新增
internal/logic/actor/authority/authority_test.go           | 新增
internal/player/actor/registry.go                          | 修改(authority 模式接线)
internal/player/actor/registry_authority_ingress_test.go   | 新增
internal/player/actor/registry_authority_test.go           | 新增
```

禁止写集未修改证明:`git diff --name-only cp2-round2-20260804..HEAD` 仅含上述 8 个文件;不含 api/proto、api/gen、catalog、ingress、workflow、cmd、scripts.go、rediskey、actor.go/state.go、docs/design。

提交列表(base→HEAD):

```text
0ccbef8 feat(actor): AR1 authority persistence adapter + handoff audit matrix
3de96c7 feat(actor): AR1 wire Player/Farm Registry to AuthorityStore mainline
0466530 test(actor): AR1 authority mainline integration tests (Buy/Sell + 空 Farm)
e05c739 feat(actor): AR1 farm registry per-actor memory stats seam
97e5bb6 docs(verification): AR1 authority mainline handoff 完成版
fee814d fix(actor): AR1 review P1-1 CLAIM_UNKNOWN recovery must not wedge ACTIVE-without-actor
<final> docs(verification): AR1 handoff 记录 P1-1 修复(本文件,见 git log -1)
```

回滚方式:authority 模式仅由新构造函数接入,cmd 与全部既有路径仍走 legacy blobstore;回滚 = 不使用 NewAuthorityRegistry 装配(或整分支 revert,不影响 integration)。

next_dependency:I0 合入评估(CP3);Q1 live Redis 探针;WF1 消费 Participant/Order 接缝;P1B-CONSUMERS-2 在 AR1 稳定后串行收口。
