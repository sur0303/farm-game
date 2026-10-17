# ROUTE FOUNDATION MVP PREMERGE 20260808 — FINAL HANDOFF

> **Post-merge update（2026-08-08）**：本 Premerge 已通过 merge commit
> `10b80d1b844edc22c8b6acb7254900012979e82a` 合入
> `codex/integration-20260803`，post-merge 全仓与 Live Gate 均 PASS。最终状态见
> `docs/verification/route-foundation-mvp-integration-20260808-handoff.md`。本文其余
> “AWAITING_INTEGRATION_APPROVAL” 内容记录的是 Premerge 冻结时点，不再代表当前状态。
>
> 本文是当前 Route Foundation MVP 的主交接文档。历史文档
> `route-foundation-premerge-handoff.md` 只描述 2026-08-07 的旧候选及当时尚未关闭的
> RF-OBS 限制，不代表当前结论；当前状态与后续操作以本文为准。

## 0. 一页状态

| 项 | 当前值 |
|---|---|
| Integration 基线 | `origin/codex/integration-20260803@1ae95ceba0040f7ed788537f312d2125ce209498` |
| 冻结 RT1B-3D 分支 | `codex/rt1b-3d-lease-recovery-budget-20260808` |
| 冻结 RT1B-3D head | `6bede089bb96e8052e693adeba10bac488f2f8bb`（已推送） |
| Premerge 分支 | `premerge/route-foundation-mvp-20260808`（已推送） |
| 代码 Merge Commit | `112c2a4dfdb35b7dcc7dfb0ebd416c2bdc3db222` |
| Gate/Evidence Commit | `6c2e6b7134874a45da7c4eb42e81cf943e9e17c9` |
| 最终交接 Head | 以 `refs/heads/premerge/route-foundation-mvp-20260808` 为准（本文提交只改文档） |
| 当前状态 | `PUSHED / ALL_GATES_PASS / AWAITING_INTEGRATION_APPROVAL` |
| Integration 合入 | **未执行；必须等待用户明确通知** |
| Static Assignment | PASS，保持冻结 |
| Remote Watch / 动态控制面 | OFF / NOT IMPLEMENTED |

当前已经完成三件事：

1. Route Foundation 的 Static Authority/Membership Runtime 代码链完成并冻结；
2. Lease Deadline 后恢复预算与真实故障证据完成，Luna Max 最终复核无 Finding；
3. 从最新 integration 重建 Premerge，完成全仓与公司 Proxy Redis Gate，并推送远端。

当前没有待修复代码 Finding。唯一待办是：收到用户明确通知后，按本文第 11 节决定
直接合入或从更新后的 integration 重新建 Premerge。禁止提前合入。

---

## 1. Git 拓扑与可追溯性

### 1.1 Premerge 拓扑

```text
origin/codex/integration-20260803
  1ae95ceba0040f7ed788537f312d2125ce209498
          \
           \  merge --no-ff（无冲突，只合最终 RT1B-3D head 一次）
            \
             112c2a4dfdb35b7dcc7dfb0ebd416c2bdc3db222
             parents:
               P1 = 1ae95ceba0040f7ed788537f312d2125ce209498
               P2 = 6bede089bb96e8052e693adeba10bac488f2f8bb
                       |
                       +-- 6c2e6b7134874a45da7c4eb42e81cf943e9e17c9
                           Gate evidence + 两处历史 EOF 空白清理
                       |
                       +-- refs/heads/premerge/route-foundation-mvp-20260808
                           本文详细交接更新（docs-only）
```

`112c2a4` 的第二父提交就是最终 RT1B-3D head `6bede08`。没有逐个重复 merge
`cb1e59a`、`1da7885`、`9441475`、`533575d`、`7e49f18`，也没有再次 merge
2026-08-07 的历史 Premerge heads。

### 1.2 测试 Provenance

- RT1B-3D 最终公司 Redis H1/H2/H3 的本地 live-run commit 标识为
  `65f5e887ee54a383e40e48e701bf4fe6d6987c95`。该 commit 在首次 push 前被 amend，
  因此不是远端最终分支的祖先；它与远端 `6bede08` 具有同一父提交 `7e49f18`。
  两棵树的差异只包含最终 Evidence/Handoff 和 `cmd/rfobs1-smoke/main.go` 的注释
  边界修订，没有运行逻辑差异。Manifest 保留 live-run SHA，远端接手人以
  `6bede08` 的功能代码和本文映射复核 provenance。
- Premerge 全仓与 Live Gate 的代码 commit：`112c2a4...`。
- `112c2a4..6c2e6b7` 只有 `docs/verification/**`：Gate 证据、详细结果和两个历史
  Handoff EOF 空行清理；没有 Go、部署或运行时代码漂移。
- 本文后续提交仍为 docs-only；最终 SHA 不写入自身以避免自引用提交循环，交接时
  必须解析远端 branch ref。

### 1.3 已推送远端

```text
refs/heads/codex/rt1b-3d-lease-recovery-budget-20260808
  = 6bede089bb96e8052e693adeba10bac488f2f8bb

refs/heads/premerge/route-foundation-mvp-20260808
  = 以远端 ref 为准（当前交接分支）

refs/heads/codex/integration-20260803
  = 1ae95ceba0040f7ed788537f312d2125ce209498
  （写本文时仍未变化、未被本任务更新）
```

---

## 2. 累计完成项

### 2.1 RT1B-1：Zero-Shard Readiness

完成 Process 与 Shard 准入语义拆分：

- `ProcessReady()` / `Ready()` 只表达公共依赖和 Membership Runtime 生命周期健康；
- 合法注册的 Holder 即使当前分配 0 个 Shard，也允许 Process Ready；
- `CanServeShard(domain, shard)` 独立检查目标 Shard 的本地 Guard/Generation；
- `ServingShardCount()` 提供可观测的本地可服务 Shard 数量；
- 未安装 Shard 的 WriterEpoch、Claim、Participant route fence 全部 Fail Closed；
- 0 Shard 不触发 Actor 批量加载或预热；
- Static 启动仍要求本地 Holder 正确注册，未注册不得伪 Ready。

主提交：`7ed8161`、`d20144c`。
详细文档：`docs/verification/rt1b-1-zero-shard-handoff.md`。

### 2.2 RT1B-2A/2B：Shard Membership Core

完成单进程 Membership Reconciler 内核和并发收口：

- 接收完整、单调的 Route Snapshot，计算 ADD/REMOVE/REPLACE；
- Topology 与 Generation 状态均受 Fence，不允许旧 Revision/旧 Generation 回写；
- Installer/Fencer/Remover port 调用移到状态锁外，避免锁内网络/存储调用；
- Action 有序执行、失败重试有界、Close 可取消并等待 action worker；
- Installed membership、Desired snapshot 和 Revision 语义冻结；
- 为 Real Adapter/Scheduler 暴露稳定窄接口；
- 该层只是本地执行内核，不包含 Remote Watch、RouteManager 或人工迁移入口。

主提交：`f024de4`、`425f740`、`9819880`。
详细文档：`docs/verification/rt1b-2a-membership-handoff.md`。

### 2.3 RT1B-3A：Real Membership Adapter

完成 Repository-proved 的本地 Membership Adapter：

- Install 前后采用 two-phase 语义，只有 Authority Repository 证明成功后才安装
  本地 Guard；
- 0→1→0 和 Generation 替换都使用精确 generation fence；
- Holder/Boot/Assignment Revision/Writer Epoch/Lease Epoch/Hash Version/Hash Seed
  共同构成匹配边界；
- 同 generation Renew 只刷新 deadline，不创建第二个 Guard；
- 不同 hash 或 stale generation 的 Fence/Remove 不得影响当前 Guard；
- 并发 Observe/Reconcile 后，installed membership 与 admitted guards 保持一致。

主提交：`296c721`、`e7072bc`、`478b60c`。
详细文档：`docs/verification/rt1b-3a-real-membership-adapter-handoff.md`。

### 2.4 RT1B-3B：Per-Shard Lease Scheduler

完成每 installed Generation 一个续租 Entry 的调度器：

```text
Active ⇄ LocalFencedRenewing（LocalDeadlineExpired，可同代恢复）
Active / LocalFencedRenewing → LostGeneration / Cancelled（终态）
```

冻结不变量：

- 同一 installed Generation 恰好一个 Renew Owner；Activate 幂等；
- ADD Commit 成功后才 Activate；Fence/Remove 先 generation-fenced Cancel；
- 迟到 G1/G2 响应不能取消 G3、刷新 G3 或重新 Admit；
- Transient 在本地 deadline 前允许继续服务，采用有界退避；
- LocalDeadlineExpired 关闭目标 Shard 准入，但允许同 generation 恢复；
- MembershipRemoved、LostGeneration、RuntimeFatal 为 sticky 终态；
- 正常 LostGeneration 只终止目标 Shard，不拉低整个进程；
- Fence 失败、内部不变量损坏或 goroutine panic 才触发 Scheduler Fatal；
- Renew attempt 有 timeout；单 Shard 阻塞不拖住其他 Shard；Close 有界；
- Scheduler 锁内不调用 Registry/Store/回调，OnFatal panic 被包含。

主提交：`bfe6b70` 至 `cb70b3c`。
详细文档：`docs/verification/rt1b-3b-lease-scheduler-handoff.md`。

### 2.5 RT1B-3C-A：Membership Runtime Composition

完成以下三组件的唯一 Composition Root：

```text
RealMembershipAdapter
  + ShardMembershipReconciler
  + PerShardLeaseScheduler
  = NewMembershipRuntime
```

冻结内容：

- composed 路径不启动 legacy `runRenewal`，每 Shard 只有一个续租 Owner；
- 启动 Snapshot 必须完整收敛，否则构造 `(nil, err)` Fail Closed；
- registered zero-shard holder 可 Ready；本地 assigned shard 安全 Admit 后 Ready；
- `membershipState` 是唯一生命周期真值：Bootstrap / Ready / Fatal / Closed；
- Fatal/Closed 后禁止新的 Membership 副作用；late snapshot 返回 Closed；
- Reconciler Close 等待 in-flight action，最后一次可能 Commit happens-before Fatal
  本地清理；
- Fatal 立即取消 owned entries、精确 Fence/Remove owned guards、Serving=0；
- Close 不覆盖 Fatal primary cause，但仍回收 observer/applier/reconciler/scheduler；
- Close 与 Fatal 并发采用“先终态者获胜”，无死锁；
- 所有权过滤限定 Domain + HolderID + HolderBootID，不触碰其他 Runtime Guard；
- 完整 Snapshot 可跳过中间 Revision 并从 R1 直接收敛 R3。

主提交：`10ae701` 至 `8ee3688`。
详细文档：`docs/verification/rt1b-3c-runtime-composition-handoff.md`。

### 2.6 RT1B-3C-B：player-svr/farm-svr Route Wiring

完成启动接线与部署接缝：

| `AUTHORITY_MEMBERSHIP_MODE` | 路径 | 互斥保证 |
|---|---|---|
| composed/true | `NewMembershipRuntime` | legacy `runRenewal` 不启动 |
| legacy/false/空 | 旧 `New` | 新 Per-Shard Scheduler 不启动 |
| 非法值 | 启动失败 | 不静默回退 |

同时完成：

- player-svr/farm-svr 构造后错误统一经 `run() error` 返回，确保 Runtime Close；
- authority 初始化失败不回退 legacy；
- 两端 readyz 均使用 `ProcessReady()`；
- 基础 compose 默认保持 legacy，`docker-compose.membership.yml` 提供显式 composed
  override；生产必须显式配置，不依赖空值默认；
- 启动日志输出 membership mode、domain、holder、boot、snapshot revision；
- 没有接入 Remote Watch。

主提交：`7c30860`、`425ab7c`。
详细文档：`docs/verification/rt1b-3c-route-wire-handoff.md`。

### 2.7 RT1B-3C-C：Static Boot Mismatch Fail Closed

完成 Static 模式 Holder Boot 身份校验：

- snapshot 中本地 Holder Boot 与进程 Boot 不一致且分配了本地 Shard时，启动失败；
- readiness 不得打开，不允许 legacy fallback；
- 合法 zero-route Holder Boot mismatch 保持 0-shard Ready 的冻结兼容语义；
- Player/Farm 真实进程路径均有公司 Proxy Redis 证据。

主提交：`6ac12e4`、`7990af2`。
详细文档：`docs/verification/rt1b-3c-c-static-boot-mismatch-handoff.md`。

### 2.8 Q2R-2A/B1/B2：真实 Redis 与真实进程验证工具

完成三个双门控工具面：

- Q2R-2A：Repository/Lua Lease 语义；
- Q2R-2B1：Player/Farm 启动、renew、fail-closed、SIGTERM；
- Q2R-2B2：connect refused、blackhole、deadline、authority expiry、response drop、
  blocked shutdown；
- 所有 live 工具必须同时满足 `Q2R2_LIVE=1` 与受控 Redis URL；缺任一不触网；
- 唯一 namespace、精确 key DEL + EXISTS；无 KEYS/全局 SCAN；无 SCRIPT FLUSH；
- 共享 Redis Endpoint 与 secret 不进入 Git artifact。

主提交：`a18a99d`、`eacc9c8`、`9565e9f`、`510c09c`、`ea1a17c`。
详细文档：

- `docs/verification/q2r2a-redis-probe-handoff.md`
- `docs/verification/q2r2b1-process-smoke-handoff.md`
- `docs/verification/q2r2b2-network-fault-smoke-handoff.md`

### 2.9 RF-OBS-1 与 RT1B-3D：Deadline Recovery Budget

完成直接 Local Fence 观测、恢复预算和 H1/H2/H3：

- test-only diagnostics 使用双环境门，只允许显式 loopback bind；
- 直接暴露 `lease_fence_reason`、`runtime_fatal`、`serving_shard_count`、
  `can_serve_shard`；拒绝 `:0`、`[::]:0`、wildcard 和非 loopback host；
- Fault Proxy 区分 request no-forward、all-response loss、selective renew reply loss；
- go-redis 的小写 `evalsha` 统一规范化并提取 SHA，避免 selective loss 静默
  退化为 passthrough；有单元回归测试；
- H1/H2 在本地 deadline 后直接看到目标 Shard Fail Closed，并在 latest safe 前
  同 generation 恢复；
- H3 在 latest safe 后只要求安全结果：精确同代恢复，或 LocalDeadline/
  LostGeneration Fail Closed；不得 ambiguous pass；
- 命令证据只声明 local fence 期间无成功业务 mutation；不声称 ingress 在进入
  Actor mailbox 前拒绝。

RT1B-3D 恢复预算冻结为：

```text
effective recovery budget =
  2 * RenewRecoveryTimeout
  + RecoveryMaxDelay
  + RecoveryJitterMax
  + RecoverySchedulerSlack

启动硬条件：effective recovery budget < SafetyMargin
latest_safe_restore_at = known_authority_deadline - effective recovery budget
```

实现保证：

- 校验最终 effective 参数，而不是只校验显式字段；
- zero-valued timeout 安全派生并在 margin/4 cap 后参与校验；
- initial timer 与 ordinary backoff 均 deadline-aware，不能睡过本地 deadline；
- Recovery cadence 独立于普通 13.3s 级退避；
- Recovery jitter 的共享 PRNG 有锁，无 data race；
- duration shift/add 饱和，负 jitter 不能把 delay 消成 0 或回绕；
- terminal fence 不进入 Recovery cadence；Close 和 stale generation timer 有界隔离；
- 30 个定向恢复测试全部通过。

主提交：`1da7885`、`9441475`、`533575d`、`7e49f18`、`6bede08`。
详细文档：`docs/verification/rt1b-3d-lease-recovery-budget-handoff.md`。

---

## 3. Luna Max Review 与 Finding 关闭记录

复核使用 openai-codex `gpt-5.6-luna`，`model_reasoning_effort=max`，Reviewer
只读；修复由原 Writer/控制器执行。

### 3.1 Scheduler Focused Review：4 个 P1，全部关闭

| Finding | 关闭方式 |
|---|---|
| zero-valued `RenewRecoveryTimeout` 在 margin/4 cap 前被默认值覆盖，cap 不可达 | 重排最终 effective 参数派生；新增默认 timeout cap 测试 |
| initial timer 未走 `deadlineAwareDelay`，可能睡过本地 deadline | initial/retry timer 统一 deadline-aware；新增 initial scatter 回归 |
| recovery jitter 共享 `math/rand.Rand` 存在并发 data race | PRNG 访问串行化；`-race` 并发测试 |
| duration shift/jitter 可能负值、overflow 或立即 retry | shift/add 饱和、delay 不低于 base；极端 duration 与负 jitter 测试 |

修复 Commit：`7e49f18`。

### 3.2 Evidence Review 继续发现并关闭

| Finding | 处理 |
|---|---|
| H3 只靠 Lease 未推进间接推断 LostGeneration | diagnostics 直接观测 fence/fatal/serving/canServe；H3 改为两种明确安全结果 |
| `known_authority_deadline` 混入故障中未确认的 Redis Lease 延长 | 固定为故障前最后确认 deadline；未确认延长只算额外余量 |
| 2162.5ms 预算在毫秒证据中截断 | 改为向上取整 2163ms |
| 命令可能已进入 mailbox turn，不能声称 “before Actor Turn” | 实现、manifest、Handoff、源码注释全部改为“local fence 期间无成功 mutation” |
| diagnostics `:0` 可绑定 wildcard | 必须显式 loopback host；测试覆盖 `:0`、`[::]:0`、IPv4 wildcard、非 loopback |
| Fault Proxy 只识别大写 `EVALSHA`，go-redis 实际发小写 `evalsha`，H1 故障未生效 | 命令统一转大写并新增 classifier 测试；废弃旧 H1 证据，真实重跑确认丢 4 个 Renew reply |
| 最终源码仍有两处旧 `before Actor Turn` 注释 | 精确 HEAD P2 closure 修订；二次 Luna Max 结论 P0/P1/P2 均无 |

最终精确 Head `6bede089...` 复核结论：

```text
P0: none
P1: none
P2: none
NO FINDINGS
NO BLOCKING FINDINGS
```

特别说明：一次真实 H1 复跑曾因 lowercase `evalsha` classifier 缺口失败；该轮旧
artifact 已明确废弃，没有被用于冻结。当前 Git 证据来自 classifier 修复后的真实重跑。

---

## 4. Premerge Gate

### 4.1 静态 Gate

以下命令在 `112c2a4` 的代码树真实 Exit 0：

```text
go test -buildvcs=false -count=1 -timeout=600s ./...

go test -race -buildvcs=false -count=1 -timeout=600s \
  ./internal/authoritycomposition/... \
  ./internal/storage/redis/authority/... \
  ./internal/logic/actor/authority/... \
  ./cmd/player-svr \
  ./cmd/rfobs1-smoke

go vet ./...
go build -buildvcs=false ./cmd/...
git fsck --no-dangling
git diff --check origin/codex/integration-20260803
```

结果：

| Gate | 结果 |
|---|---|
| 全仓 `go test` | PASS |
| Authoritycomposition / Redis Authority / Actor Authority / diagnostics race | PASS |
| 全仓 `go vet` | PASS |
| `go build ./cmd/...` | PASS |
| `git fsck` | PASS |
| `git diff --check` | PASS（清理两个历史 Handoff EOF 空行后） |
| 新增 Git blob >1 MiB | 0 |
| Secret/Endpoint | PASS，无 credential/private key/真实 Endpoint |

工作树里两个大于 1 MiB 的 PNG 是 integration 已存在设计资产，不是本候选新增对象。
Secret 扫描中的 loopback、拒绝测试用 RFC1918 地址、URL 构造源码和 `<redacted>`
占位文本属于预期源码，不是泄漏。

### 4.2 Q2R-2A：Repository/Lua

公司 Proxy Redis 真运行结果：9 PASS + 1 profile SKIP。

| 场景 | 结果 |
|---|---|
| Claim/Renew AcceptedTTL + deadline refresh | PASS |
| 未发送 Transient transport failure，deadline 前保持准入 | PASS |
| Fence mismatch 分类 LostGeneration | PASS |
| 本地 deadline 后、Authority 尚有效时同代 Renew | PASS |
| Authority Lease 过期后不能复活 | PASS |
| G1/G2 stale fence 不能影响 G3 | PASS |
| 唯一 SHA 触发 NOSCRIPT 后自动 reload，无全局 flush | PASS |
| Commit unknown 后同 identity replay | PASS |
| exact-key DEL + EXISTS==0 | PASS |
| CLUSTER/MOVED/ASK profile | SKIP：company-proxy 隐藏 CLUSTER，保持 PROBE_PENDING |

### 4.3 Q2R-2B1：真实进程启动/关闭

| 场景 | 结果 |
|---|---|
| S1 Player+Farm registered zero-shard Ready，无 legacy renewal | PASS |
| S2A Player one-shard，真实 Renew 至少推进两轮 | PASS |
| S2B Farm one-shard，真实 Renew 至少推进两轮 | PASS |
| S3 holder-unregistered / authority mismatch / illegal mode / assigned boot mismatch Fail Closed；合法 zero-route boot mismatch 仍 0-shard Ready | PASS |
| S4 Player+Farm SIGTERM 有界 clean exit，keys 清理 | PASS |

### 4.4 Q2R-2B2：真实网络故障

| 场景 | 结果 |
|---|---|
| N1 connect refused | PASS |
| N2 short blackhole | PASS |
| N3 cross local deadline | PASS |
| N4 authority expiry | PASS |
| N5 response drop | PASS |
| N6 blocked Player/Farm SIGTERM | PASS |

最终 Verdict：N1–N6 全部 `pass=true`、`overall=PASS`、无 Runtime Fatal。

### 4.5 RF-OBS H1/H2/H3

本轮 effective recovery budget：`2163ms`（纳秒值 2162500000，毫秒向上取整）。

| 场景 | Restore 时已确认 Authority 余量 | 直接 Fence | 结果 |
|---|---:|---|---|
| H1 selective renew reply loss | 4595ms | `LocalDeadlineExpired + serving=0 + canServe=false`；确定性丢 4 个 Renew reply | 约 1806ms 后同 generation 恢复，re-admit，无 Fatal |
| H2 all-response blackhole | 4596ms | 同上 | 约 1807ms 后同 generation 恢复，re-admit，无 Fatal |
| H3 no-forward late restore | 1061ms，晚于 latest safe | 同上 | 保持 `LocalDeadlineExpired` Fail Closed，无复活、无 Fatal |

H1/H2 满足“latest safe 前恢复必须同代 Renew”；H3 只使用“latest safe 后安全
Fail Closed”承诺，不宣称 liveness。

### 4.6 Redis 安全边界

- Redis secret 只从 ignored 本地配置读取；
- artifact 中 Endpoint 为 redacted；
- 每次运行使用唯一 namespace；
- 清理为已跟踪精确 key 的 DEL + per-key EXISTS；
- 不执行 KEYS、无界/全局 SCAN、SCRIPT FLUSH；
- 不为了管理性验残留在共享 Redis 做全库扫描。

---

## 5. 证据索引

### 5.1 当前 Premerge 证据

```text
docs/verification/results/route-foundation-mvp-20260808T121330Z/
├── run-manifest.json
├── q2r2b2/
│   ├── run-manifest.json
│   └── verdict.json
└── rfobs1h/
    ├── H1-selective-renew-loss/{verdict.json,recovery-budget.json}
    ├── H2-all-response-early-restore/{verdict.json,recovery-budget.json}
    └── H3-no-forward-late-restore/{verdict.json,recovery-budget.json}
```

总 Manifest 固定了 integration base、merge parents、tested merge commit、静态 Gate、
Live Gate、清理方式与 `integration_merge=NOT_PERFORMED_AWAITING_USER_APPROVAL`。

### 5.2 RT1B-3D 最终证据

```text
docs/verification/results/rfobs1h-20260808T120054Z/
```

该目录是 RT1B-3D 冻结分支上的最终 H1/H2/H3 脱敏证据；Premerge 目录是合入
integration 基线后再次运行的独立证据，两者不能混为同一次运行。

### 5.3 主交接文档

- 本文：`docs/verification/route-foundation-mvp-premerge-20260808-handoff.md`
- Lease Recovery：`docs/verification/rt1b-3d-lease-recovery-budget-handoff.md`
- Direct Fence：`docs/verification/rf-obs1-direct-shard-admission-handoff.md`
- Recovery Evidence：`docs/verification/rf-obs1-recovery-handoff.md`
- 各阶段文档见第 2 节对应路径。

---

## 6. 主要代码与工具落点

| 区域 | 作用 |
|---|---|
| `internal/authoritycomposition/composition.go` | Runtime 公共构造、Process/Shard readiness、recovery options |
| `internal/authoritycomposition/membership.go` | Snapshot diff、Reconciler、action/Close barrier |
| `internal/authoritycomposition/membership_adapter.go` | Repository-proved two-phase install/fence/remove |
| `internal/authoritycomposition/lease_scheduler.go` | Per-Shard Renew、deadline/recovery cadence、generation isolation |
| `internal/authoritycomposition/runtime_membership.go` | 三组件组装、Fatal/Closed、ownership cleanup、observer |
| `internal/authoritycomposition/membership_mode.go` | composed/legacy 模式互斥构造 |
| `internal/logic/actor/authority/dynamic_fence.go` | Local Guard admission/fence |
| `internal/logic/actor/authority/generation_fence.go` | generation identity/fence |
| `internal/logic/actor/authority/lease_renew.go` | 同代 Renew/Rearm 接缝 |
| `cmd/player-svr/main.go` / `cmd/farm-svr/main.go` | composition root、ready、Close、启动预算日志 |
| `cmd/player-svr/q2r2diag.go` | 双门控、loopback-only test diagnostics |
| `cmd/q2r2b1-smoke` | 真实进程启动/关闭 Gate |
| `cmd/q2r2b2-smoke` | 真实网络故障 Gate |
| `cmd/rfobs1-smoke` | H1/H2/H3 direct-fence/recovery Gate |
| `internal/storage/redis/authority/q2r2_probe_test.go` | Repository/Lua live probe |

累计变更相对 integration 基线约 72 个文件、约 17.9k 行新增，主体是
Authoritycomposition 实现、确定性测试、真实进程 Smoke 和验证文档；没有公网 Proto、
H5、动态控制面或 Kubernetes Autoscaling 接线。

---

## 7. 冻结语义摘要

### 7.1 Readiness

```text
ProcessReady = Membership Runtime 公共生命周期健康
CanServeShard = 指定 Shard Guard 当前 admitted
ServingShardCount = 本 Runtime owned admitted guards 数量
```

Process Ready 不等于所有 Shard Ready；单 Shard LostGeneration 不应杀死其他 Shard。
Fatal/Closed 后 ProcessReady=false 且 owned data plane Fail Closed。

### 7.2 Renew Ownership

```text
composed mode: PerShardLeaseScheduler 是唯一 Renew Owner
legacy mode:   runRenewal 是唯一 Renew Owner
两者禁止同时运行；构造失败禁止 fallback
```

### 7.3 Fence

```text
None → LocalDeadlineExpired → 同 generation 成功 Renew 可回 None
None/LocalDeadlineExpired → MembershipRemoved/LostGeneration/RuntimeFatal 为 sticky 终态
旧 generation 的迟到响应永远不能影响新 generation
```

### 7.4 Recovery

```text
restore < latest_safe_restore_at  => 必须同 generation Renew
restore >= latest_safe_restore_at => 只保证安全 Fail Closed；不保证 liveness
```

### 7.5 Evidence Claim

当前证据证明：直接 Local Fence 期间目标 Shard `serving=0`、`canServe=false`，命令无
成功业务 mutation，Writer Fence 最终防双写。当前 Player Binding 在 serialized
mailbox turn 内获取，因此**不声明 ingress-before-mailbox rejection**。

---

## 8. 未完成项与明确禁止项

### 8.1 本 checkpoint 不包含

- AssignmentStore；
- Remote Assignment Watch；
- RouteManager 控制面实现；
- routectl；
- Holder Registry 动态注册控制面；
- 人工迁移 API/CLI；
- Pod Crash 自动接管；
- Kubernetes Lease/Operator/HPA/Autoscaler；
- 自动扩缩容与容量驱动迁移；
- 动态控制面发布/回滚协议。

Runtime 单元测试支持合法完整 Snapshot 的 0→1→replace→0 收敛，不代表人工迁移或
Remote Watch 已实现。

### 8.2 平台 Pending

- Direct Cluster 的 MOVED/ASK、目标 Primary Script Load、Failover；
- proxy failover；
- capacity/latency/cost 实测。

原因：当前公司 Proxy Redis 隐藏 CLUSTER 命令。上述保持 `PROBE_PENDING`，不影响
Static Route Foundation MVP 冻结，但不能被文档写成已验证。

### 8.3 上线注意

- 基础 compose 默认 legacy；启用 composed 必须显式使用 membership override/
  `AUTHORITY_MEMBERSHIP_MODE=true`；
- 启动日志必须核对 membership mode、domain、holder/boot、snapshot revision 和
  effective recovery budget；
- diagnostics 仅用于 test，必须同时满足双环境门并显式绑定 loopback；
- 生产环境不得开放 diagnostics wildcard/public bind；
- 不得在本 Premerge 上继续追加动态控制面开发。

---

## 9. 工作区与 Writer 边界

- RT1B-3D Writer worktree：`farm-game-rt1b-3d-lease-recovery-budget-20260808`；
- Premerge Writer worktree：`farm-game-premerge-route-foundation-mvp-20260808`；
- Luna Reviewer worktrees 为 detached、只读；
- 主 `farm-game` worktree 的 `tools/agentctl/__pycache__/` 属无关未跟踪项，未纳入
  任何提交；
- 当前冻结后只允许控制器做 fetch/核验/合入；不允许复用本分支开发新 Feature。

---

## 10. 快速复核命令

以下命令不含 secret，可用于接手人确认 Git 状态：

```bash
git fetch --prune origin

git ls-remote --heads origin \
  refs/heads/codex/rt1b-3d-lease-recovery-budget-20260808 \
  refs/heads/premerge/route-foundation-mvp-20260808 \
  refs/heads/codex/integration-20260803

git show --no-patch --pretty=raw 112c2a4dfdb35b7dcc7dfb0ebd416c2bdc3db222
# 必须看到 parent 1ae95ce... 和 parent 6bede08...

git diff --check origin/codex/integration-20260803..origin/premerge/route-foundation-mvp-20260808

git diff --name-only 112c2a4dfdb35b7dcc7dfb0ebd416c2bdc3db222..origin/premerge/route-foundation-mvp-20260808
# 预期只有 docs/verification/**
```

Live Redis 命令只应在受控环境由控制器执行，secret 不得复制到命令日志或文档。

---

## 11. 收到 Integration 合入通知后的决策流程

### 11.1 合入前强制检查

1. `git fetch --prune origin`；
2. 解析并记录远端 Premerge head；
3. 确认工作树 clean、远端 Premerge SHA 与本地一致；
4. 重新读取 `origin/codex/integration-20260803` 当前 SHA；
5. 检查 Premerge Gate 之后是否有任何非 docs 代码漂移；
6. 用户必须已经明确发出“允许合入 integration”的通知。

### 11.2 如果 Integration 仍是 `1ae95ce...`

可以把冻结的远端 Premerge head 作为单一候选进入 Merge Queue；合入后核对：

- integration 新 head 包含 Premerge head；
- `112c2a4` 仍只出现一次；
- 不额外 merge 历史 RT1B heads；
- integration 工作树 clean；
- 远端 ref SHA 与本地核对一致。

### 11.3 如果 Integration 已前进

**不得直接把当前 Gate 结论套到新基线，也不得 force/rebase 已冻结 Premerge。**

必须：

1. 从新的远端 integration head 建全新独立 Premerge worktree/branch；
2. 只合入最终 RT1B-3D head `6bede089...` 一次；
3. 解决冲突后重跑本文第 4 节全部静态 Gate；
4. 重跑 Q2R-2A/B1/B2 和 RF-OBS H1/H2/H3；
5. 重新生成脱敏 Evidence/Manifest；
6. 推送新 Premerge，再等待新的明确合入确认。

### 11.4 合入前禁止

- 禁止在未通知情况下自行 merge；
- 禁止把当前分支 rebase/force-push 到 integration；
- 禁止跳过新 integration 基线的 Gate；
- 禁止先开动态控制面、人工迁移或自动扩缩容开发。

---

## 12. 最终冻结登记

```text
CP-ROUTE-FOUNDATION-MVP

integration_base                 = 1ae95ceba0040f7ed788537f312d2125ce209498
rt1b_3d_final_head               = 6bede089bb96e8052e693adeba10bac488f2f8bb
premerge_code_commit             = 112c2a4dfdb35b7dcc7dfb0ebd416c2bdc3db222
premerge_gate_evidence_commit    = 6c2e6b7134874a45da7c4eb42e81cf943e9e17c9
premerge_final_ref               = refs/heads/premerge/route-foundation-mvp-20260808

luna_max_final_review            = PASS / NO FINDINGS
full_repository_test             = PASS
authority_race                   = PASS
go_vet                           = PASS
cmd_build                        = PASS
q2r2a                             = PASS_WITH_CLUSTER_PROFILE_SKIP
q2r2b1                            = PASS
q2r2b2                            = PASS
rf_obs_h1_h2_h3                  = PASS
large_object_gate                = PASS
secret_endpoint_gate             = PASS
exact_key_cleanup                = PASS
global_scan                      = NOT_RUN_BY_DESIGN
global_script_flush              = NOT_RUN

static_assignment                = FROZEN_PASS
remote_watch                     = OFF_NOT_IMPLEMENTED
dynamic_control_plane            = NOT_STARTED
manual_migration                 = NOT_STARTED
autoscaling                      = NOT_STARTED

integration_merge                = NOT_PERFORMED_AWAITING_USER_APPROVAL
status                           = PUSHED_ALL_GATES_PASS_AWAITING_INTEGRATION_APPROVAL
```
