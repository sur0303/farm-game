# RT1B-3D LEASE RECOVERY BUDGET HARDENING Handoff

- 分支：`codex/rt1b-3d-lease-recovery-budget-20260808`
- worktree：`farm-game-rt1b-3d-lease-recovery-budget-20260808`（独立目录）
- base：`cb1e59a`（RF-OBS-1H fault-mode 语义拆分）
- 原候选：`533575d`；最终 Review 修复：`7e49f18`
- 来源：X0 实测证明普通 Transient 退避可增长到 13.3s > SafetyMargin 5s，网络恢复后
  首次有效 Renew 晚于 Authority 过期 → 同代恢复无预算保证（P1 可用性缺口，非 P0
  双写：存储层 Fence 仍兜底）。
- 首轮关闭三个 P1：最终有效参数无条件预算校验、保守 2×AttemptTimeout 上界、
  Deadline-aware Recovery Wakeup。
- 最终独立 Review（Luna max）再发现并关闭 4 个 P1：默认 recovery timeout cap
  不可达、initial timer 未受 deadline cap、共享 recovery PRNG data race、负 jitter/
  duration shift 可触发立即重试。所有 Finding 均有先失败后通过的回归测试。

## 1. 目标

把"同代恢复能否及时完成"从运气变成可证明的预算：

1. 进入 `LeaseEntryLocalFencedRenewing` 时重置普通 Transient backoff（不继承
   13.3s 级退避），使用**独立 Recovery cadence**。
2. **P1 #1（已关闭）**：启动期不变量必须对**最终有效参数**无条件执行：
   `2*RenewRecoveryTimeout + RecoveryMaxDelay + RecoveryJitterMax + RecoverySchedulerSlack < SafetyMargin`。
   不允许"RecoveryMaxDelay 显式非零才校验"——任何部分显式配置（如显式
   jitter/slack + 派生 max）都不能绕过校验。
3. **P1 #2（已关闭）**：预算覆盖最坏恢复时序——网络恢复时已有 Recovery Attempt A
   在飞（A 不可复用，消耗完整 attempt timeout）→ recovery cadence（max+jitter）→
   Attempt B 再消耗一次 attempt timeout。因此预算含 **2×timeout**。结构性保证：
   单 goroutine 串行执行 renewOnce + 每次 attempt 有独立 context deadline，任何时刻
   至多一个 attempt 在飞（不存在第三个 timeout）。**在飞 attempt 不跨 Deadline**：
   `attemptTimeout = min(RenewAttemptTimeout, remaining local deadline)`，普通 attempt
   最晚在本地 Deadline 时刻返回，因此进入 Fence 后的第一个 attempt 一定是 Recovery
   attempt（用 RenewRecoveryTimeout），2×timeout 预算严格成立。
4. **P1 #3（已关闭，Deadline-aware Recovery Wakeup）**：Entry 的 initial timer 与
   后续每次重试都受 `min(重试时间, 本地安全 Deadline)` 约束，普通 backoff（可到秒级）
   **永远不能睡过本地 Deadline**。Deadline 到达时立即唤醒 → 确认精确 generation 仍
   installed/desired → 进入 LocalFencedRenewing → 普通 backoff 只重置一次 → 独立
   Recovery cadence。`markFenceTransition` 在 renewOnce 内 attempt 之后再次观察
   （attempt 的 Snapshot 副作用此刻才 promote fence），保证重置不滞后一个周期。
5. 冻结 `latest_safe_restore_at = authority_expire_at − RecoveryCompletionBudget`：
   该时刻前网络恢复 → 必须完成同代 Renew；之后恢复 → 只保证安全 Fail Closed。
6. 不引入 busy loop：recovery delay 下界为 effective base，指数移位与 jitter 加法
   饱和而不溢出；共享 PRNG 串行访问。保持 per-shard 隔离、迟到 generation fence、
   终态 Fence（MembershipRemoved/LostGeneration/RuntimeFatal）不进入恢复态、
   G1 旧 timer 不干扰 G2、Close 有界取消。

## 2. Changes

| 文件 | 改动 |
|---|---|
| `internal/authoritycomposition/lease_scheduler.go` | 解析最终 effective 参数并无条件校验 `2*timeout+max+jitter+slack < margin`；zero-valued timeout 在 margin/4 cap 前不再被覆盖；initial/retry timer 均 deadline-aware；recovery backoff 移位/加法饱和且 delay 不低于 base；共享 recovery PRNG 加锁；保留 generation/terminal fence/Close 语义 |
| `internal/authoritycomposition/composition.go` | Options 增 recovery 参数，冻结注释改为 2×timeout 公式；`Runtime` 增只读 recovery budget |
| `internal/authoritycomposition/runtime_membership.go` | 透传 SafetyMargin + recovery 参数（0 = scheduler 内安全派生），暴露最终有效预算 |
| `internal/authoritycomposition/lease_recovery_test.go` | 30 个定向测试：预算、初始/重试 deadline、负 jitter 下界、duration overflow、并发 PRNG race、generation/fence/Close |
| `internal/authoritycomposition/runtime_membership_test.go` | 测试 helper 使用满足 2×timeout 公式的参数；相关 Go 文件统一 gofmt |
| `cmd/player-svr/main.go` / `cmd/farm-svr/main.go` | 启动日志输出 margin/base/max/jitter/attempt_timeout/slack/completion_budget/satisfied |
| `cmd/player-svr/q2r2diag.go` | 双环境门 + 显式 loopback-only 的 test diagnostics 增加直接 `lease_fence_reason` / `runtime_fatal` 只读字段；拒绝 `:0` wildcard |
| `cmd/rfobs1-smoke/faultproxy.go` / `faultproxy_test.go` | RESP 命令规范化为大写，确保 go-redis 小写 `evalsha` 能提取 Renew SHA；回归测试防止 selective loss 静默退化成 passthrough |
| `cmd/rfobs1-smoke/main.go` | H1/H2/H3 分场景；以 `leaseRestore` 为 post-restore 推进基线；预算毫秒向上取整；直接断言 local fence/fatal/admission 与 late safe outcome |
| `docs/verification/results/rfobs1h-20260808T120054Z/**` | 公司 Proxy Redis 真进程 H1/H2/H3 脱敏 Verdict 与 recovery-budget evidence |

未触碰：RouteManager、Remote Watch、Assignment Store、routectl、Autoscaler、
Kubernetes 接线、Redis Lua/KeyBuilder、Actor Registry 业务加载、公网 Proto/H5。

## 3. Frozen Semantics

```text
普通准入态：  LeaseEntryActive → Transient 普通指数退避（有界）
恢复态：      LeaseEntryActive → 本地 Deadline 过期 → LeaseEntryLocalFencedRenewing
               1. 清除普通 Transient backoff；
               2. 立即/有界启动 Recovery Renew；
               3. 使用独立 Recovery cadence（不继承 13.3s 退避）。
Deadline-aware：
              initial_wakeup / next_wakeup = min(重试时间, 本地安全 Deadline)
              initial timer 与普通 backoff 永远不能睡过本地 Deadline；
              Deadline 到达（或已过）→ 立即唤醒 → 进入 LocalFencedRenewing。
              终态 Fence（MembershipRemoved/LostGeneration/RuntimeFatal）
              不进入恢复态；fenced 态不因 wakeup 产生 busy loop。
启动校验：    无条件（SafetyMargin>0 即执行，与字段是否显式无关）：
              effective budget = 2*RenewRecoveryTimeout + RecoveryMaxDelay +
              RecoveryJitterMax + RecoverySchedulerSlack < SafetyMargin
              （任一条件不满足 → 启动失败，fail closed）
              另校验：margin>=0、timeout>0、0<base<=max（派生 base 自动 cap 到
              effective max）、jitter>=0、slack>=0、duration 加法无 overflow。
运行时承诺：  latest_safe_restore_at = authority_expire_at − RecoveryCompletionBudget
              → 该时刻前恢复必须同代 Renew；
              → 之后恢复只保证 Fail Closed（允许 LostGeneration，不 Fatal）。
时钟语义：    Authority deadline 来自最后一次已确认 Renew 的 AcceptedTTL + 本地
              monotonic deadline（Real Membership Adapter 冻结）；不直接比较
              Redis lease_expire_at_ms 与本机 Unix 时间；响应丢失产生的未确认
              Lease 延长只算额外余量，不进入安全承诺。
```

## 4. Tests（30 个，全 PASS）

基础/运行时 12 个：原 10 个保持不变，并新增：

| 测试 | 冻结 |
|---|---|
| TestRecoveryDelayNeverFallsBelowBase | 1000 次 jitter 采样均不低于 base；极端 duration shift 饱和到 max、不回绕 |
| TestConcurrentRecoveryJitterIsRaceFree | 8 goroutine 并发访问 shared PRNG，`go test -race` 无竞态 |
| TestRecoveryJitterIsBoundedAndStable | jitter 有界；相同 seed 的两个 scheduler 产生相同序列 |
| 其余基础 9 个 | startup safe/unsafe、fence transition、同代 rearm、expiry no-revive、Shard 隔离、Close、stale generation 均 PASS |

P1 #1/#2 预算 9 个：

| 测试 | 冻结 |
|---|---|
| TestPartialExplicitRecoveryOptionsStillValidateBudget / TestJitterAndSlackCanMakeDefaultMaxDelayUnsafe | 部分显式配置也必须对最终 effective budget fail closed |
| TestTimeoutAloneLargerThanMarginIsRejected | `2×timeout >= margin` 拒绝 |
| TestDefaultOptionsProduceSafeEffectiveBudget | 全默认预算精确且 Satisfied |
| TestDefaultRecoveryTimeoutIsCappedBySafetyMargin | 普通 timeout=4s、recovery timeout=0 时，最终 cap 为 margin/4=1.25s（原代码失败） |
| TestExplicitRecoveryBaseAboveMaxRejected / TestRecoveryDurationOverflowRejected | 配置矛盾与 duration overflow 拒绝 |
| TestRecoveryBudgetInfoExposesEffectiveValues | 对外只读值等于最终 effective 参数 |
| TestRestoreWhileRecoveryAttemptAlreadyBlockedMeetsBudget | barrier 等待 Attempt A timeout（无固定 Sleep）→ restore → B 在 CompletionBudget 内成功 |

Deadline-aware 9 个：

| 测试 | 冻结 |
|---|---|
| TestOrdinaryBackoffCannotSleepPastLocalDeadline | 30s 普通 backoff 被约 30ms deadline cap |
| TestInitialWakeCannotSleepPastLocalDeadline | 15s initial scatter 同样被约 30ms deadline cap（原代码失败） |
| TestDeadlineWakeResetsOrdinaryBackoff | deadline 后只重置一次并进入 recovery cadence |
| TestRestoreBeforeLatestSafePointDuringLongOrdinaryBackoffRearms | safe point 前恢复保持同代准入 |
| TestDeadlineWakeDoesNotBusyLoop | 持续 Transient 时重试频率有界 |
| TestTerminalFenceDoesNotEnterRecoveryCadence | 终态 fence 直接取消、不 Fatal |
| TestStaleDeadlineWakeCannotAffectReplacementGeneration | G1 timer 不影响 G2 |
| TestCloseCancelsDeadlineWakeAndRecovery | Close 有界取消 timer/attempt |
| TestDefaultBaseWithExplicitSmallerMaxIsCapped | 派生 base cap 到显式较小 max |

复跑：定向包、`-race`、vet 全 PASS；6 个最终 Review 回归测试 `-count=20` PASS。

## 5. 配置审计（P1 后默认派生 vs 真实配置）

| 环境 | SafetyMargin | MaxDelay | 派生 RecoveryMax | 派生 Timeout | 派生 Jitter | 有效 Budget | 结果 |
|---|---|---|---|---|---|---|---|
| rfobs1-smoke（H1/H2/H3） | 5s | 20s(=LeaseTTL) | min(20s, 1.25s)=1.25s | min(300ms, 1.25s)=300ms | 312.5ms | 2×300ms+1.25s+312.5ms=2.16s | PASS（启动不受影响） |
| q2r2b1/q2r2b2-smoke | 1.5s | 8s | min(8s, 375ms)=375ms | min(300ms, 375ms)=300ms | 93.75ms | 2×300ms+375ms+93.75ms≈1.07s | PASS |
| farm-worker（composed 未接线） | 2s/0 | — | — | — | — | — | 不构造 scheduler，无影响 |
| 单元测试 testOptions | 100ms | 4s | 25ms | min(750ms,25ms)=25ms | 6.25ms | 81.25ms | PASS（runtime 测试 helper 已显式合法化） |

结论：S 线/H 线全部真实 Smoke 配置均满足新预算，启动不会意外 Fail Closed。启动日志
（player-svr/farm-svr）输出 effective budget 供部署时核对。

## 6. RF-OBS-1H1/H2/H3 真进程复验

测试 Commit：`65f5e887ee54a383e40e48e701bf4fe6d6987c95`；公司 Proxy Redis；Static
Assignment；Remote Watch OFF；精确 key DEL，无 KEYS/SCAN/SCRIPT FLUSH。预算分类
固定使用故障前最后可确认的 authority deadline；故障中未确认的 Lease 延长只算额外余量。

| 场景 | restore 时 Authority 余量 | Budget 分类 | 结果 |
|---|---:|---|---|
| H1 selective renew-response loss | 4596ms | 早于 latest safe（budget ceil=2163ms） | 4 个 Renew reply 被确定性丢弃；直接观测 LocalDeadline fence；约 1807ms 后观测同代恢复；fence=None；re-admit；不 Fatal |
| H2 all-response blackhole | 4596ms | 早于 latest safe | 直接观测 LocalDeadline fence；约 1807ms 后观测同代恢复；fence=None；re-admit；不 Fatal |
| H3 no-forward blackhole | 1057ms | 晚于 latest safe、Authority 尚未过期 | LOCAL_DEADLINE_FAIL_CLOSED（fence=LocalDeadlineExpired、serving=0、canServe=false）；未复活；不 Fatal；允许后续安全转 LostGeneration |

三个场景均在故障中直接观测 `LocalDeadlineExpired + serving=0 + canServe=false`，
随后命令无成功业务 mutation。当前 Player Binding 在 mailbox turn 内获取，因此本证据
**不声称 ingress 在进入 mailbox 前拒绝**；该接线不属于 RT1B-3D。
脱敏 Artifact：`docs/verification/results/rfobs1h-20260808T120054Z/`，每场景保存
`verdict.json` 与 `recovery-budget.json`，包含 effective budget、known authority
deadline、latest safe、restore/first renew 时间、generation before/after 和 result。

下一步仅允许从最新 integration 重建 Route Foundation Premerge，再复跑
Q2R-2A/Q2R-2B1/Q2R-2B2 与全仓 Gate；不得在本分支继续开发控制面。

## 7. 已知限制（如实记录）

- initial timer 与普通 backoff 睡过 Deadline、共享 PRNG race、负 jitter 立即重试、
  默认 timeout cap 不可达均已关闭；selective fault classifier 的大小写缺口也已关闭。
- ingress-before-mailbox admission 不在 RT1B-3D 写集；本冻结只声明 local Guard
  fail-closed、命令无成功 mutation、Writer Fence 最终防双写。
- 平台项（direct-cluster / proxy failover / capacity-latency-cost）保持
  PROBE_PENDING（第 31 号），不影响 Lease Runtime MVP 冻结。

## 8. 冻结登记与后续边界

```text
CP-ROUTE-LEASE-RUNTIME-MVP

fault_mode_semantic_split       = cb1e59a
recovery_budget_initial         = 1da7885
effective_budget_fix            = 9441475
deadline_aware_wakeup           = 533575d
final_review_fixes              = 7e49f18
h1_h2_h3_tested_code            = 65f5e88
final_head_ref                  = refs/heads/codex/rt1b-3d-lease-recovery-budget-20260808

focused_review                  = PASS
rf_obs_1_h1_h2_h3               = PASS
status                          = PASS
```

本分支冻结后禁止继续加入 AssignmentStore、Remote Watch、RouteManager、routectl、
Holder Registry、Kubernetes 扩缩容或 Autoscaler。后续只能由控制器 push/freeze，
并从最新 integration 建独立 `premerge/route-foundation-mvp-20260808`，只合入本分支
最终 head 一次；动态控制面必须等 Route Foundation 合入新的 integration 后再开工。
