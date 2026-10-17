# Q0 合同条款 → 测试/证据矩阵(docs/design 26~37)

- 生成: 2026-08-03,Q0-TEST-HARNESS,base_commit `6b78d2f`
- 权威顺序: 26~37 → CONTRACT-MANIFEST → 71/72/73 → linux-workspace-implementation-audit-20260803 → 当前源码/W5B 证据
- 证据等级(72 §16.5): `UNIT_PASS` / `RACE_PASS` / `INTEGRATION_PASS` / `REAL_REDIS_PASS` / `MULTI_PROCESS_E2E_PASS` / `CAPACITY_PASS`;另用 73 §6 的 `DOMAIN_LOGIC_PASS` 标记纯内存/Fake 域逻辑证据。**低级不得冒充高级;Mock 结果不得标 CAPACITY_PASS。**
- 状态列含义: PASS(已有该等级证据)/ PARTIAL(有证据但低于条款要求或范围不全)/ GAP(无证据)。
- Owner: Q1=故障/容量证据执行角色(72 §1.2);BIZ=对应业务实现 Owner(73 §7 工作包,如 DT0=任务域);Q0=本任务(只提供工具,不产出业务证据)。

## 0. 现有工具 → 条款能力映射(重复能力清单,禁止重造)

| 能力 | 既有工具 | Q0 接缝 | 条款 |
|---|---|---|---|
| 可控时钟 | 3 处重复 fakeClock(散见 registry_retire_test/passivation_test/sweeper_test)+ SetClock 接缝 | **testkit/clock 已收敛**;存量测试的替换属各 Owner | 26§HC-18, 27§D35, 30§30-14 |
| RPC 错误/延迟/断连 | authoritystore.FaultStore(仅 authority 边界) | **testkit/fault.Injector+Do+LogicClient** 覆盖任意一元 RPC 接缝 | 29§L2-RPC 全文 |
| Redis timeout/response-lost | FaultStore(authority);blobstore 哨兵错误 | **testkit/fault.BlobStore**(blobstore 边界 before/after apply) | 30§30-04/05, 31§31-06 |
| Actor crash point | actor_test.go panic 用例(一次性写法) | **testkit/fault Crash action + BlobStore BlobCrash** | 27§D8, 30§30-07 |
| 慢连接/有界队列 | transport.FakeTransport | **testkit/slowconn**(Fill/Drain/SlowConsumer 步进) | 26§HC-08/10, 28§L2-49 |
| 同 operation 重试断言 | 散见 7+ 测试(见 §26 行) | **testkit/assert.SameOperationRetried** | 28§L2-13 |
| payload 冲突断言 | ingress/actor/store 测试各自断言 | **testkit/assert.PayloadConflict** | 28§L2-13, 30§30-04 |
| TIMEOUT_UNKNOWN 断言 | unknown_test/lane_test/outcome_test | **testkit/assert.TimeoutUnknown(+Processing)** | 27§D22, 30§30-05 |
| 旧 writer Fence 断言 | memory_test/fence_race_test 等 | **testkit/assert.OldWriterFenced** | 30§30-01/06 |
| 旧 Subscription 断言 | downlink/lease/gate 测试各自断言 | **testkit/assert.StaleSubscriptionPush/Renew** | 26§HC-29, 27§D20 |
| 200 订阅/多 Gate/排干 | cmd/lease-stress、cmd/dualgate-e2e、scripts/test-multigate-drain(W5B) | **loadtest/ preset 参数化复用,不重建** | 26§HC-36, 60§S05/S06 |

## 1. 26-access-session(HC-ACCESS-01..48, UX-BOOTSTRAP-01)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| HC-01..06 HTTPS/Ticket/Session 激活 | gateway/auth ticket 单测;recovery-admission handoff | UNIT_PASS + RACE_PASS | PARTIAL | 真实 H5+双 Gate E2E 重跑(60§S01)→ Q1 |
| HC-07..10 帧/有界队列/不阻塞循环 | transport fake_test;gate 单测 | UNIT_PASS | PARTIAL | 慢连接背压不阻塞健康 Lane 的运行时证据(60§S05)→ Q1(工具: slowconn) |
| HC-11..12 Route≠Presence,唯一订阅 | gate/hub 单测 | UNIT_PASS | PASS(单元级) | 跨进程双 Gate 组合证据 → Q1 |
| HC-13..17 Join FLUSHING/ACTIVATING,ADMITTED 边界 | join_barrier_test;commandcatalog retry_directive_test | UNIT_PASS | PARTIAL | ADMITTED 边界确定性竞态(60§S03)→ Q1 |
| HC-18..20 Lease fail-closed,KICK 验 Fence | W5A SessionRoute(IMPLEMENTATION_PASS);W5B Subscription Lease 双 Gate 实跑(f90b11b) | MULTI_PROCESS_E2E_PASS(限 W5B 已跑场景) | PASS(W5B 范围) | 60§S05 弱网/重连风暴全量 → Q1 |
| HC-21..28 Snapshot/Arm/Ready(N)/精确清理 | downlink service_test;gate_test;w5b_lease_terminal_test | UNIT_PASS | PARTIAL | 多批 Delta 竞态、authority Hash(60§S04)→ Q1 |
| HC-29..35 下行 Lane 隔离,seq 连续,续租不复活 | subscription lease_test;stage5 E3(健康 Lane 不被坏 Lane 阻塞) | INTEGRATION_PASS | PARTIAL | E3 无客户端 Reducer 观察(60§17.12)→ Q1 |
| HC-36..39 200 上限,Ready 后变更缓冲 | lease-stress 实跑 22b84e9:200/200 Join、4000 renews、P99≤5ms | REAL_REDIS_PASS(续租路径) | PARTIAL | 60§S06 完整 200 人场景(广播+玩法)未证 → Q1,**不得提前标 CAPACITY_PASS** |
| HC-40..44 AdmissionContext/CancelTombstone/REJOIN | ingress/gate 单测;dualgate-e2e REJOIN→Fresh Join(f90b11b) | MULTI_PROCESS_E2E_PASS(限场景) | PARTIAL | AUTHORIZING/RelationEpoch 撤销(60§S04)→ BIZ(好友域)+Q1 |
| HC-45..48 好友 AUTHORIZING→permit,BOOTSTRAP/JOIN 独立链 | player ingress 单测(bootstrap 独立只读) | UNIT_PASS | PARTIAL | 好友 Pair authority 未实现(审计 §6.10)→ BIZ |
| UX-BOOTSTRAP-01 机会失败不伪造 NONE | H5 侧;后端 opportunity 未实现 | — | GAP | BIZ |

## 2. 27-access-routing-actor-workflow-freeze(D1..D54)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| D4..D8 双 RouteDomain/lazy 激活/Singleflight/有界 mailbox | actorrouting、farm/player actor 测试;registry panic 重载用例 | UNIT_PASS + RACE_PASS | PASS(单元级) | — |
| D9..D12 Turn 不等跨域/单一持久化组/SoftSave 白名单 | persistence lane_test | UNIT_PASS | PASS(单元级) | — |
| D13..D19 Plant=Reservation+Workflow;Theft/Harvest 独立;Care RewardOrder | workflow runner_test/store_test;farm actor 测试 | UNIT_PASS | PARTIAL | §19 Phase D 每 workflow 故障集(步间崩溃/RPC 丢失/worker 接管)部分缺 → BIZ+Q1(工具: fault.BlobStore/Injector) |
| D20..D28 PROCESSING 保 operation_id;break-before-make;TIMEOUT_UNKNOWN→RESOLVING | gate/ingress/actor unknown_test | UNIT_PASS | PASS(单元级) | 跨进程 Unknown 证据 → Q1 |
| D29..D34 Join 激活次序;原子提交;deterministic workflow-id-v2 | join_barrier_test;workidentity 测试 | UNIT_PASS | PASS(单元级) | — |
| D35..D45 RouteSnapshot Hash;Lease 单调 deadline;RECOVERY_OVERDUE;writer_claim_id 跨代 fence | authoritystore store_test/fence_race_test;W5B lease 测试 | UNIT_PASS + RACE_PASS | PASS(单元级) | Fence 跨进程竞态(30§18 出口 11)→ Q1 |
| D46..D54 Q32-13 失败矩阵唯一权威;TaskDomain in PlayerActor;PET outcome 不可变 | 部分(任务域 DT0 进行中);pet 未实现 | — | GAP | BIZ(DT0 等) |
| §20 交付完成定义 15 项 | 部分被上述覆盖 | — | PARTIAL |  reconnect/STALE_SUBSCRIPTION、单调 FarmVersion、Plant exactly-once、worker 重投递等需按 15 项逐条挂证据 → Q1 维护 |

## 3. 28-command-catalog(L2-PROTO-01..63)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| L2-01..08 帧/Envelope/Catalog 完备性 | protocoltest golden+envelope;catalog_test 完备性 | UNIT_PASS | PASS | — |
| L2-09..15 5 状态+RetryDirective;FirstHop Hash 唯一权威 | retry_directive_test;idempotency hash_test | UNIT_PASS | PASS | — |
| L2-16..22 可恢复写查询;REJECTED+RETRY_SAME 仅执行前 | player ingress service_test(含冲突/未知) | UNIT_PASS | PASS(已接线命令) | 未接线 21 条命令 → BIZ |
| L2-23..31 RouteMode;StoredOperationOutcome;seq 不重排;join_attempt_id | commandcatalog、downlink 测试 | UNIT_PASS | PASS(单元级) | — |
| L2-32..40 PushSpec;ACCEPTED→SNAPSHOT→DELTA*→READY;Fresh Join 边界 | downlink service_test;join_barrier_test | UNIT_PASS | PARTIAL | 多批 Delta 严格连续的运行时证据 → Q1 |
| L2-41..50 REJOIN 仅公共;Journal 有界 fail-closed;CARE/THEFT 边界;FARM_CHANGE NEVER_SILENT_DROP | dualgate-e2e;downlink 测试 | MULTI_PROCESS_E2E_PASS(限场景) | PARTIAL | 广播不静默丢帧的容量证据 → Q1(loadtest BroadcastSlowConn) |
| L2-51..63 task/mail/pet/compendium/bootstrap/opportunity | CLAIM_TASK/BOOTSTRAP 已接线并单测;其余未实现 | UNIT_PASS(限 2 条) | GAP(余者) | BIZ |

## 4. 29-internal-rpc(L2-RPC-01..109)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| L2-01..16 FirstHop 重解码/Hash;RouteHeader;单跳 Redirect | gate/command 测试;actorrouting 测试 | UNIT_PASS | PASS(单元级) | — |
| L2-17..26 OperationResult 语义;PROCESSING 只轮询;budget min-of-three | ingress 测试 | UNIT_PASS | PASS(单元级) | — |
| L2-27..49 Join/Subscription 内缝(Arm 线性化、QueueAccepted fence、BatchRenew、Self-Fence) | join_barrier/w5b_lease_terminal/subscription lease 测试 | UNIT_PASS + RACE_PASS | PASS(单元级) | 跨 Gate KICK+renew 组合竞态 → Q1 |
| L2-50..109 Participant 目录;确定性 ID;Receipt 重放;统一 failpoints;不可撤销点 | idempotency/workidentity;FaultStore 驱动的 actor/authority 测试 | UNIT_PASS | PARTIAL | **§11.1 全部 49 项验收测试未逐条建账** → Q1 用 testkit/fault 逐项执行并挂证据(当前 failpoint 名称未全部落地,属 BIZ 接缝) |
| L2-82 统一 failpoint 枚举(BEFORE_PARTICIPANT_SEND…FORCE_SHARD_HANDOFF) | 无统一注入点;现有注入分散 | — | GAP | BIZ(需生产侧 failpoint 挂钩,Q0 只提供 Injector 原语,见 ICR-Q0-3) |

## 5. 30-actor-authority-store(30-01..30-23)与 31-redis-physical(31-01..24)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| 30-01..06 强写 fence/WriterEpoch/Singleflight/8 分类/Unknown 解决/数据面 fence | authoritystore 全套单测+FaultStore 故障测试;fence_race_test | UNIT_PASS + RACE_PASS | PASS(单元级) | Redis 集成实证(30§18 出口 8-10)→ Q1 REAL_REDIS_PASS |
| 30-07..10 Passivation 前置;原子提交;单 Group/Claim/Lane | passivation_test;store_test | UNIT_PASS | PASS(单元级) | — |
| 30-11..20 有界记录;CLAIM_UNKNOWN;统一 ShardWriteFence 注入 | authoritystore 测试 | UNIT_PASS | PARTIAL | **AuthorityStore 主链未切换(审计 §6.4:生产仍用旧 blobstore.RedisStore)** → BIZ 决策(审计 §9 P1) |
| 30-21..23 规则快照冻结;好友 Pair/Quota/Share CAS/Unknown | 规则快照部分(farm actor);好友 authority 未实现 | UNIT_PASS(部分) | GAP(好友) | BIZ |
| 31-01..10 KeyBuilder/same-tag/Lua 族/Receipt 生命周期 | w31a-keybuilder-probe;capacity record_size/tcaplus_schema 测试 | UNIT_PASS | PARTIAL | Lua 合同测试与真 Redis 验证(31§20)→ Q1 |
| 31-11A..C 全面有界+容量基线 | capacity 记录尺寸模型(纯计算) | DOMAIN_LOGIC_PASS | PARTIAL | 30M DAU 容量实证最后做(审计 §9)→ Q1,**未跑前不得 CAPACITY_PASS** |
| 31-12..24 StableOpaqueID 金样;ACL;Task/Pet/Compendium 物理位 | w7d-stable-id 金样测试 | UNIT_PASS(限 ID) | GAP(余者) | BIZ + 平台探针(31§17.1)→ Q1 |
| 30-redis.md(REFERENCE_REVIEW) | — | — | 不作准 | 仅保留 Key 格式/GETDEL 等历史测试参照,不得从中提取规范 |

## 6. 32-core-business-scenario(Q32-01..15)

| 条款 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| Q32-01..03 Buy/Sell/库存混合栈 | player ingress service_test(Buy/Sell 原子+冲突+未知) | UNIT_PASS | PASS(单元级) | — |
| Q32-04..05 浇水/除虫事件;Care 首解者 XP/RewardOrder | farm actor water 测试(含 committed-but-timeout 重放) | UNIT_PASS | PARTIAL | Care 流程好友侧未通 → BIZ |
| Q32-06A/B 20%/2% 槽位;宠物拦截 | theft 上限单测;**pet 未实现;经济参数为 demo 值(审计 §6.10)** | UNIT_PASS(部分) | GAP(pet/参数) | BIZ |
| Q32-07..08 Harvest 原子含图鉴解锁;Care 恰好一单 | harvest+compendium 首收解锁单测(审计 §6.9) | UNIT_PASS | PASS(单元级) | — |
| Q32-09 24 地/5 初始/UnlockPlot 三段 | **未实现(当前 12 块免费地)** | — | GAP | BIZ |
| Q32-10..12 5 状态正交;V+1;规则冻结时刻/base_yield 50 倍数 | actor/ingress 测试 | UNIT_PASS | PARTIAL | base_yield 倍数等数值规则需随真实经济参数复核 → BIZ |
| Q32-13/14 失败+并发矩阵(唯一权威) | 矩阵中部分行有单测 | UNIT_PASS(部分) | PARTIAL | **§10.8/§10.9 全矩阵逐行建账缺失** → Q1 用 testkit 断言族补齐执行 |

## 7. 33/33A 好友与分享(Q33-01..20, 33A-01..14)

| 条款组 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| Q33 基础链路(添加/删除/分享 token/ roster) | 审计 §6.7:基本 loop 已通;friend 包单测 | UNIT_PASS | PARTIAL | §9 的 16 项并发不变量(100 并发 redeem、迟到 TerminationOrder 等)未逐项执行 → BIZ+Q1 |
| Q33-12/13/15 AUTHORIZING 加入/逐操作准入 | ingress 有 AdmissionContext 骨架 | UNIT_PASS(骨架) | GAP(准确机会/完整准入) | BIZ(审计 §6.10:准确 friend-opportunity 未实现) |
| 33A 物理合同(Pair CAS/attempt cursor/ Lua) | 未实现(组件级仓库存在但未接线,审计 §6.5) | — | GAP | BIZ + 平台探针(33A-02/10 PLATFORM_PENDING)→ Q1 |

## 8. 34 任务(Q34-01..08)

| 条款 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| Q34-01/08 TaskDomain in PlayerActor;3 日常+1 成长;可重建红点 | 审计 §6.8:2 个成长任务已通;taskcatalog 包无测试 | UNIT_PASS(极小范围) | PARTIAL | DT0-TASK-DOMAIN 进行中(跨 worktree),日常任务/跨日惰性切换等 → BIZ(DT0) |
| Q34-02..07 跨日/进度来源/幂等/原子领取 | CLAIM_TASK 已接线单测(原子授予) | UNIT_PASS(限单条) | PARTIAL | DT0 表驱动测试矩阵 → BIZ(DT0) |

## 9. 35/35A 邮件(Q35-01..08, 35A-01..10)

| 条款 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| 全部 | **未实现(审计 §6.10)** | — | GAP | BIZ;35A§11.2 七项平台探针 → Q1 |

## 10. 36 宠物(Q36-01..08)

| 条款 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| 全部 | **未实现(审计 §6.10)**;25 项冻结验收集存在(manifest 锁) | — | GAP | BIZ |

## 11. 37 图鉴(Q37-01..08)

| 条款 | 现有证据 | 等级 | 状态 | 缺口 / Owner |
|---|---|---|---|---|
| Q37-01/06/07 首收解锁/身份=definition_id/原始 UnlockDelta | 审计 §6.9:首收解锁最小链路已通并单测 | UNIT_PASS | PARTIAL | ACK 单调 fence、下架占位、LimitProfile 探针 → BIZ+Q1 |
| Q37-02..05/08 归档不可变/PlayerActor 权威/有界阈值 | 部分 | UNIT_PASS(部分) | PARTIAL | BIZ;Q37-08 阈值 probe-pending → Q1 |

## 12. 汇总:Q0 之后最高优先级证据缺口

1. 60§S06 完整 200 人同农场(广播+玩法)——Q1,用 loadtest BroadcastSlowConn/FullGameplay preset,**未实跑前 INCONCLUSIVE**。
2. 60§S03 ADMITTED 边界确定性竞态、§S04 多批 Delta/authority Hash——Q1,用 testkit/fault+slowconn。
3. 29§11.1 四十九项与 32§10.8/10.9 矩阵逐行建账——Q1 执行,BIZ 补 failpoint 挂钩(ICR-Q0-3)。
4. AuthorityStore 主链切换决策(审计 §9 P1)——BIZ;切换后 30§18 出口证据 → Q1。
5. 31§17.1/§20 平台探针与 2h/24h soak——Q1,loadtest Soak preset。
