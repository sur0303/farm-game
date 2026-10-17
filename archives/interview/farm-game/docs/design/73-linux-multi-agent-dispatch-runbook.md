# 73 · Linux 多 Agent 施工调度与 Prompt 手册

- 版本：`v1.9-hero-closure-dispatch`
- 日期：2026-08-05
- 状态：`ACTIVE_IMPLEMENTATION / CORE_GAMEPLAY_INTEGRATED / HERO_CLOSURE_READY`
- 合同内容基线：`a78196a48d044a350a3b761c1d73b404f44a18c1`
- CP2A 代码合并点：`94155566b08bccfb453b344fdee046918f199953`
- CP2A 派发名称：`cp2a-20260804`
- CP2 代码合并点：`5e31c2dea65136fa3c0486c6d77765dd5815588e`
- CP2 代码检查点：`cp2-20260804`
- Round 2 最终派发名称：`cp2-round2-20260804`
- Round 2 后端合并点：`0564a5194d0c1df88b50d899db6fa8ecf8208bf8`
- Round 2 H5 合并点：`6e582d4dea3674b0f58b6f1d65d91cf07c09f308`
- P1B-CONSUMERS-2 派发名称：`round2-p1b-consumers2-20260804`
- P1B-CONSUMERS-2 最终候选：`a22dca9814680bd7d5d874ad7fc42ec6e4eb3015`
- P1B-CONSUMERS-2 后端合并点：`74de91925409efdfa9a5e53e6d551b394d4942ab`
- CP3 稳定名称：`cp3-20260804`

> 2026-08-05 当前派发覆盖说明：CP3/Round 3 表格保留为历史记录。当前后端实现头为 `4e8f9d7`，
> Plant、Harvest、Care、Theft 已合入；下一轮不得再派发旧 PI1/FI1/SC1 卡。实际剩余项、保留 worktree、
> H5 Bootstrap 缺口和 Wave H0/H1 任务以
> [2026-08-05 Linux 工作区真实实现进度审计](../verification/linux-workspace-implementation-audit-20260805.md)
> 为准。

上位输入：

- [26～37 现行合同总账](./CONTRACT-MANIFEST.md)；
- [71 · 多 Agent 实施编排](./71-multi-agent-implementation-plan-draft.md)；
- [72 · Agent 工作包手册](./72-agent-work-package-playbook-draft.md)；
- [Linux 工作区真实实现进度审计（2026-08-05）](../verification/linux-workspace-implementation-audit-20260805.md)；
- [Linux 工作区旧点时审计（2026-08-03）](../verification/linux-workspace-implementation-audit-20260803.md)；
- [AR2 · 生产 Authority 切换与路由控制面决策](../verification/ar2-production-authority-cutover-decision.md)；
- Linux `docs/verification/w5b-subscription-lease-handoff.md` 与 `s7-handoff.md` 的实现证据。

其中实现审计和 S7 只证明代码/测试事实；它们复制的旧合同版本、旧 70 号计划和旧施工顺序不具备
语义权威。设计与施工唯一真源是本地同步后的 26～37、Manifest、71～73。

本文只回答三个工程问题：

1. 当前真实代码之后，下一步先做什么；
2. 哪些工作可以安全并行，哪些必须单写；
3. 在 Linux 上给每个 Agent 复制什么 Prompt。

本文不重新决定业务规则。第 26～37 号与本文冲突时，永远服从第 26～37 号。

---

## 0. 先给结论

当前项目不是“还没开始”，也不适合再按服务从头重写。真实状态是：

```text
已经可运行：
接入/账号/Session/Join/双Actor/静态Shard路由
+ Redis旧主链
+ Plant/Harvest/Care/Theft Workflow
+ 基础好友、任务、图鉴和H5

已经新增并合入：
SCB-1共享合同 + Farm/Task/Pet/Compendium纯领域包 + Q0 Harness
+ AuthorityStore Redis Adapter + 33/33 CommandCatalog + Friend Authority核心

已完成的 Round 2 主线：
cp2-round2-20260804
→ AR1 Authority Store/Registry 组件主链接线（32c9964；生产 cmd 尚未切换）
→ WF1 Workflow/Order/Dispatcher收敛（0564a51）
→ H1 H5 Wire/Journal/Reducer并行接入（6e582d4）
→ 三轨独立审查与组合验证完成

已完成的尾部收口：
round2-p1b-consumers2-20260804
→ P1B Consumers-2 单写收口（a22dca9）
→ 两轮独立只读审查 + 后端整仓验证
→ 合入74de919并发布CP3

下一步唯一主线：
cp3-20260804
→ 真实代码缺口审计（已完成）
→ PI1-A/FI1-A/SC1-A/SO1-A短分支收口
→ RT1最小RouteAuthority控制面并行推进
→ 按现有代码差距补玩法
→ 纵向E2E
→ I0生产Authority切换
→ 200人/3000万DAU证据
```

最重要的调度原则：

- 同时运行 **4～5 个编码 Agent**，不是十几个；
- 并行的是纯领域包、独立 Repository、H5 和测试工具；
- Proto、Redis 公共物理层、PlayerActor、FarmActor、Workflow Runtime、composition root 各自只有一个写者；
- Agent 不按“任务系统服务”“宠物服务”拆，因为首版没有这些独立服务；
- Agent 不按“一个人包办 Player 全部功能”拆，避免一个分支占锁数天；
- 动态 RouteManager 不阻塞玩法补齐；静态 Assignment 足够支撑功能开发，动态迁移在容量阶段前接入；
- 现有 Plant/Harvest/Care/Theft 先审计复用，不创建 `workflow2`、`actor2`、`runtime2`。

---

## 1. 正式派发前的 CP0

### 1.1 当前已知代码点

```text
backend audit baseline:
  branch = codex/integration-20260803
  cp1_checkpoint  = f075f5b99af416bab5253e142cbcc884cd16f088
  round1_base     = 4e587936df183dd7e6e7725200f98dec38e69d5d
  cp2a_code_merge = 94155566b08bccfb453b344fdee046918f199953
  ma0_candidate   = f4e543eb3f7d2ac0b5fd1576894e6f972a618921
  cp2_code_merge  = 5e31c2dea65136fa3c0486c6d77765dd5815588e
  code_checkpoint = cp2-20260804
  round2_dispatch_base = cp2-round2-20260804
  round2_merge    = 0564a5194d0c1df88b50d899db6fa8ecf8208bf8
  p1b_candidate  = a22dca9814680bd7d5d874ad7fc42ec6e4eb3015
  p1b_merge      = 74de91925409efdfa9a5e53e6d551b394d4942ab
  next_dispatch_base = cp3-20260804
  code/evidence closure = a949761d360bcbda9bbf0190c9d3c3f99bc1b60d

H5 audit baseline:
  branch = codex/integration-20260803
  commit = 6e582d4dea3674b0f58b6f1d65d91cf07c09f308

W5B:
  c86ce39 经 MR !6 合入 4c97191
  f90b11b/b2f65e2/22b84e9/dc235ef 补齐运行证据
  a949761 完成交接；状态 COMPLETED

contract content baseline:
  a78196a48d044a350a3b761c1d73b404f44a18c1

shared contract baseline:
  44567622d18f65e41f63fe46d164363e4d1aa8e1

Round 0 integration chain:
  77aa0a5 -> 4456762 -> 208263e -> 439f034 -> bb3ed36 -> f075f5b

Round 1 integration chain:
  abcb4b6 + 9ecea40/e826786 + d0289a4..930a072 + c3adf06/e1bf29a
  -> merge 9415556

Round 1.5 / MA0 integration chain:
  7062aee -> a0cbf5d -> f588f3d -> f4e543e
  -> merge 5e31c2d -> tag cp2-20260804

Round 2 integration chain:
  AR1 3ac4269 -> merge 32c9964
  WF1 dc2065b -> merge 0564a51
  H1  d7d053b -> H5 merge 6e582d4
  independent review = PASS / no remaining P0-P1
```

合同内容基线已经登记；代码 Commit 是审计点。每个工作包派发前，I0 仍须重新读取当前 HEAD，并把
包含 `a78196a` 的实际 base_commit 写进该任务 handoff/锁表。

### 1.2 当前必须保护的事实

后端 integration 当前干净。W5B 的历史 worktree 与本地/远端功能分支已经删除，不再存在需要 E0
独占保护的脏改动。任何新任务都必须从登记后的干净 Baseline 建独立 worktree。

```text
W5B completion = a949761
current backend code merge = 74de919
current code checkpoint = cp3-20260804
current dispatch checkpoint = cp3-20260804
shared contract baseline = 4456762
historical W5B worktree/branch = removed
```

H5 已在 Node `v22.23.1` 下复核 76/76 单测、`tsc --noEmit` 与 Web Vite build 全绿。此前 build
失败来自 `dist` 的 root 文件归属，修正目录属主后已通过，未修改源码；H0 不再派发。

### 1.3 CP0 完成条件

```text
[x] 26～37、Manifest、71～73 已提交并推送为 CONTRACT_BASELINE a78196a
[x] backend integration 分支干净，W5B 历史 worktree/branch 已清理
[x] W5B 双Gate/Drain/Fresh Join、200人续租与GateBoot证据已完成
[x] H5 baseline Commit 已登记为 6737438
[x] backend go test ./...、go vet ./... 与受影响包 race 可重复
[x] H5 76/76测试、build、tsc 可重复
[x] Round 0每个Agent拥有独立branch、worktree、写集和handoff文件
[x] Round 1的R1/P1B/SO0/DC0已从统一4e587936登记独立worktree和写锁并完成
[x] 四轨候选与integration merge均通过Proto幂等、整仓race、vet、build和diff检查
[x] 71～73与锁表同步后创建并推送最终cp2a-20260804 Tag
[x] MA0两轮修复与独立只读审查无P0/P1，5e31c2d合入并推送cp2-20260804
[x] CP2整仓race/vet/build/diff通过，Round 2最终派发基线登记为cp2-round2-20260804
[x] P1B-CONSUMERS-2修复TargetFarmPolicy P1，第二轮独立复审无P0/P1并合入74de919
[x] integration整仓race/vet/build/gen-proto/diff/clean通过，发布cp3-20260804
```

若未来合同变化而新的 `CONTRACT_BASELINE` 尚未登记，只能做只读审计和 worktree/测试工具准备，不能
让 Agent 自行补业务语义；当前基线 `a78196a` 已登记。

---

## 2. 从真实差距反推的关键路径

```text
CP0：合同与干净代码基线（已完成）
  │
  ├─ P1A：Shared Contract Baseline（已完成）──────────────┐
  ├─ DF0：Farm最终规则纯领域包（已完成）                  │
  ├─ DT0：Task纯领域包（已完成）                          │
  ├─ DP0：Pet纯领域包（已完成）                           │
  ├─ Q0 ：测试/故障注入骨架（已完成基础版）              │
                                                           ▼
CP1：f075f5b（已完成）
  │
  ├─ R1 ：新AuthorityStore Redis Adapter/Lua（已完成组件）
  ├─ P1B：12→33唯一CommandCatalog迁移（已完成）
  ├─ SO0：Friend Authority核心（已完成纯逻辑/Port）
  └─ DC0：Compendium纯领域缺口（已完成）
                                                            ▼
CP2A：9415556 / cp2a-20260804（四轨组合验证已通过）
  │
  └─ MA0：PlayerMail EntryStore（已完成，f4e543e）
                                                            ▼
CP2：5e31c2d / cp2-20260804（已完成整仓race/vet/build）
  │
  ├─ AR1：新AuthorityStore + WriterClaim接入Actor主链  ← 关键串行点
  ├─ WF1：现有Workflow/Order/Dispatcher收敛
  ├─ H1 ：最终公网Wire/Reducer
  └─ Q1：只读审查、真实Redis Probe与组合证据
  │
  └─ AR1接线稳定后串行补 P1B-CONSUMERS-2
                                                           ▼
CP3：存储与Runtime主链稳定
  │
  ├─ PI1：PlayerActor单写接线
  ├─ FI1：FarmActor单写接线
  ├─ SC1：跨Actor Coordinator
  ├─ SO1：好友Join/Admission/机会导航接线
  └─ H2 ：按纵向里程碑接页面
                                                           ▼
CP4：P0纵向业务闭环
  │
  ├─ RT1：动态Assignment/RouteManager/Drain
  ├─ O1 ：日志、指标、Trace、告警
  ├─ PL1：Docker/多进程/云部署
  └─ Q2 ：200人、弱网、故障、3000万DAU容量证据
```

### 2.1 为什么 AR1 必须是关键串行点

当前旧 `blobstore.RedisStore` 在生产主链，新 `authoritystore`、`auth-v1` KeyBuilder 和写权状态机只是
“组件完成未接线”。如果一边让多个玩法 Agent 修改 Actor，一边切换 Store，会同时修改 Actor Record、
Registry、Ingress、composition root 和故障语义，合并风险最高。

因此顺序固定为：

```text
R1先完成独立Adapter
→ AR1独占接线并证明Buy/Sell + 空Farm Load/Commit
→ PI1/FI1再接业务差距
```

切换时不做新旧 Store 双写。项目还没有正式存量用户，可以选择：

- 测试环境清空旧 Namespace 后单次切换；或
- 提供一次性离线迁移工具，再切新 Namespace。

允许保留旧 Adapter 作为显式回滚启动选项，但同一 Authority 同一时刻只能有一条写主链。

### 2.2 为什么动态路由不阻塞玩法开发

当前已有：

```text
ActorAddress
→ seeded hash
→ VirtualShard
→ 本地不可变Assignment Snapshot
→ Holder gRPC
→ Redirect纠正
```

这已经足够在固定 Pod 集合中开发和验证业务。缺少的是 Assignment 的动态发布、Shard Drain 与自动
迁移，而不是命令如何定位 Actor。RT1 在 CP4 前完成即可；提前做会让路由控制面和 Store 切换同时
修改 Runtime，延长关键路径。

---

## 3. 工作包总表

| ID | 目标 | 启动依赖 | 主要写集 | 可与谁并行 | 结束检查点 |
|---|---|---|---|---|---|
| B0 | Windows 权威合同同步、Baseline 与版本登记 | 立即 | `docs/design/**` 治理文件 | Q0 | CP0 |
| E0（历史） | W5B 双 Gate/Fresh Join/Drain/200 人续租/GateBoot | `a949761` 已完成，不再派发 | 无 | 无 | COMPLETED |
| H0（历史） | H5 WebCrypto/构建基线复核 | 76/76、tsc、build 已通过，不再派发 | 无 | 无 | COMPLETED |
| P1A（完成） | SCB-1：共享 Wire/身份/Participant IDL | `4456762` | Proto/gen/contracts/identity | 不再派发 | CP1 |
| DF0（完成） | 5→24、Care/Pest、20%/2% Theft 纯领域规则 | `208263e` | Farm 纯领域新包和测试 | 不再派发 | CP1 |
| DT0（完成） | 三每日+一成长任务纯领域规则 | `f075f5b` | Task 纯领域包和测试 | 不再派发 | CP1 |
| Q0（基础完成） | 可控时钟/RPC/崩溃/慢连接/Load Harness | `bb3ed36` | testkit/loadtest/probe | 后续只增量扩展 | 持续 |
| P1B（完成） | 12→33 唯一命令迁移 | `c3adf06/e1bf29a` | Proto/gen/CommandCatalog | 不再重复派发 | CP2A |
| R1（组件完成） | AuthorityStore Redis Adapter、Lua、Unknown、DueIndex | `d0289a4..930a072` | Redis公共物理层 | 后续只做平台Probe/AR1接线 | CP2A |
| SO0（完成） | Friend Pair/Quota/Share/Permit和机会查询核心 | `9ecea40/e826786` | Friendship独立包 | 后续SO1接线 | CP2A |
| MA0（完成） | PlayerMail EntryStore、六封分页/GC/Unknown | `f4e543e` / merge `5e31c2d` | Mail独立包 | 禁止重复派发 | CP2 |
| DP0（完成） | Pet 纯领域状态与 GuardSync 输入 | `439f034` | Player Pet纯领域包 | 不再派发 | CP1 |
| DC0（完成） | 活动图鉴/灰图/Seen 纯领域缺口 | `abcb4b6` | Compendium纯领域包 | 不再重复派发 | CP2A |
| AR1（组件完成） | 新 Store、WriterClaim、Fence 接入 Actor Runtime | `32c9964` | Actor Runtime/Registry/Store Adapter接线；cmd切换由RT1+I0 | 不再派发 | Round2 |
| WF1（完成） | 复用并收敛 Workflow/Order/Dispatcher/worker | `0564a51` | workflow/worker通用包 | 不再派发 | Round2 |
| H1（完成） | 最终 FarmFrame/33命令/Journal/Reducer | `6e582d4` | H5 wire/store/reducer | 不再派发 | Round2 |
| P1B-CONSUMERS-2（完成） | FirstHop/Ingress/Canonical Hash/兼容接缝收口 | `a22dca9` / merge `74de919` | player/farm ingress与消费侧校验 | 不再派发 | CP3 |
| PI1 | PlayerActor 接线单写通道 | CP3 | PlayerActor/Ingress | FI1/SC1/H2 | 每个里程碑后合并 |
| FI1 | FarmActor 接线单写通道 | CP3 | FarmActor/Ingress | PI1/SC1/H2 | 每个里程碑后合并 |
| SC1 | Unlock/Plant/Harvest/Theft Coordinator | CP3 + Participant接口 | 独立Coordinator包 | PI1/FI1/H2 | 纵向里程碑 |
| SO1 | Join/Admission/Opportunity Navigation 集成 | SO0 + FI1接缝 | Friendship Adapter/导航包 | PI1/H2 | M4/M5 |
| RT1 | 最小 Route Authority 控制面、动态 Assignment/RouteManager/Drain | CP3 | actorrouting/control-plane | I0生产Authority切换/Q2 | Authority切换前 |
| H2 | 产品页面分批接入 | 对应后端里程碑 | H5 feature/reducer；全局Store单写 | 后端各包 | 各E2E |
| Q2 | 200人/故障/容量/证据 | 每个里程碑 | 测试工具/verification | 持续 | 最终验收 |

### 3.1 不允许的“假并行”

以下组合禁止同时写：

```text
P1A 与 P1B之外的任何Proto Agent
R1 与任何手拼Redis Key/Lua的领域Agent
AR1 与 PI1/FI1 的 Actor主链接线
两个 PI1 分支同时编辑 PlayerActor
两个 FI1 分支同时编辑 FarmActor
WF1 与场景Agent同时重写通用 Workflow Runtime
两个 H5 Agent 同时编辑 WsClient / OperationJournal / 全局Store
任何功能Agent与 I0 同时编辑 cmd/*/main.go、go.mod、生成注册表
```

如果要增加并发度，只能继续拆纯领域包、页面组件、测试工具或独立 Repository，不能复制第二套共享框架。

---

## 4. 推荐实际轮次

### Round 0：已完成

```text
Slot 1：P1A SCB-1             → 77aa0a5 + 4456762
Slot 2：DF0 Farm纯领域规则    → 208263e
Slot 3：DT0 Task纯领域规则    → f075f5b
Slot 4：DP0 Pet纯领域规则     → 439f034
Slot 5：Q0 Test Harness       → bb3ed36
```

B0/I0 已完成独立 worktree、锁表、合并列车和组合验证。以上卡转为历史完成卡，禁止重复派发。

### Round 1：四轨已完成并合入 CP2A

```text
Slot 1：R1 AuthorityStore Redis Adapter      → d0289a4..930a072
Slot 2：P1B 12→33 CommandCatalog             → c3adf06/e1bf29a
Slot 3：SO0 Friend Authority/Opportunity核心 → 9ecea40/e826786
Slot 4：DC0 Compendium活动档案/Seen纯领域     → abcb4b6
组合候选：e1bf29a
integration merge：9415556
```

四条旧任务卡已经关闭，禁止重建分支或重复启动 Agent。候选和 integration merge 均通过 Proto 幂等、
整仓 race、vet、build 与 diff 检查；证据等级为 `COMBINED_RACE_PASS`，不是公司 Redis 生产证据。

### Round 1.5：MA0 已完成并进入 CP2

```text
MA0候选：f4e543e
integration merge：5e31c2d
代码检查点：cp2-20260804
审查：第二轮独立只读审查 PASS / no P0-P1
验证：整仓 race/vet/build/diff 全绿
```

MA0 的 branch/worktree/session 仅供审计，不得继续写入或重复派发。Receipt 有界清理、DueRef 崩溃窗口、
真实公司 Redis Cluster/ACL/Failover 与平台容量参数仍是显式 Pending，不因组件合入而自动变成生产证据。

### Round 2：共享基础接线与尾部串行收口已完成

```text
已完成：AR1 → 32c9964
已完成：WF1 → 0564a51
已完成：H1  → 6e582d4
已完成：Q1 独立审查，AR1原P1修复复审关闭，无剩余P0/P1
已完成：P1B-CONSUMERS-2 → a22dca9 / merge 74de919
已完成：第二轮独立复审无P0/P1，integration整仓验证通过
当前单写：无
```

P1B-CONSUMERS-2 已关闭且不得重复派发。Round 3 的短包必须从 `cp3-20260804` 分叉；I0 要先核验
PI1/FI1 的真实代码差距、现有测试与接缝，再登记 owned_write_set，不能直接启动长期大分支。

### Round 3：按里程碑短分支，不开长期“大一统功能分支”

```text
M1  Buy/Sell + 新Store幂等/Unknown
M2  UnlockPlot + Plant + 5→24土地
M3  Harvest + 每日/成长任务 + 图鉴
M4  Friend Relation/Join + Care/Pest + 实时广播 + 连续导航
M5  Pet/GuardSync + Theft 20%/2% + 双方结算
M6  Mail List/Detail/Read/Delete/Claim + 红点
```

每个里程碑最多保留这些并行角色：

```text
PI1：只改Player权威
FI1：只改Farm权威
SC1/SO1：只改Coordinator或非Actor权威
H2：只改H5
Q2：只改测试/证据
```

每个里程碑合入后重新发布 Integration Checkpoint，下一里程碑从新 Commit 分叉。

### Round 4：性能与生产证明

1. RT1 接 composition root，演练 Assignment Watch、Redirect、Shard Drain、Holder 崩溃；
2. 200 人同场：共享 Snapshot/VersionRing，每订阅只保留 Fence/游标/有界状态；
3. 按 Gate 聚合广播，慢客户端只堵自己的有界队列；
4. Redis Lua P99、Unknown、NOSCRIPT、Failover、RPO/RTO；
5. Docker 四进程、多 Gate、滚动更新、Readiness/Drain；
6. 从 3000 万 DAU、每天 5～6 次登录、每次小于 5 分钟、最多 300 好友推导 PCU/QPS；
7. 输出证据等级，不用单元测试冒充容量证明。

---

## 5. 合并列车

```text
CP0  Contract + clean Backend/H5 baseline（已完成）
  ↓
CP1  f075f5b：SCB-1 + Farm/Task/Pet纯领域 + Q0（已完成）
  ↓
CP2A 9415556 / cp2a-20260804：R1 + P1B + SO0 + DC0（已验证）
  ↓
CP2  MA0（消费R1接口）+ Round1组合验证
  ↓
CP3  AR1 Store主线 + WF1 + H1
  ↓
M1   Buy/Sell
  ↓
M2   Unlock/Plant
  ↓
M3   Harvest/Task/Compendium
  ↓
M4   Friendship/Join/Care/Opportunity
  ↓
M5   Pet/Guard/Theft
  ↓
M6   Mail/RedPoint
  ↓
CP4  Route/200人/故障/部署/容量
```

I0 每次合入执行：

```text
审查handoff与写集
→ 合并单个Commit/小序列Commit
→ gofmt / 生成代码一致性
→ go test ./...
→ go vet ./...
→ 关键共享包go test -race
→ H5 test/tsc/build（涉及H5时）
→ 更新integration checkpoint
→ 通知后续Agent rebase或重新建worktree
```

不允许一个功能分支跨越两个 Checkpoint 后继续开发。

---

## 6. 所有 Agent 共用的 Prompt 前缀

给 Linux Agent 派发时，先复制本节，再追加第 7～9 节中对应的任务体。

```text
你正在实现“经典农场小游戏”后端/H5。先读完本任务列出的合同章节，再检查当前源码；不要从零重写已有能力。

权威顺序：
1. docs/design/26～37 现行合同；
2. docs/design/CONTRACT-MANIFEST.md；
3. docs/design/71、72、73；
4. docs/verification/linux-workspace-implementation-audit-20260803.md；
5. 当前源码和测试证据。

工作规则：
- 开始先输出 git status --short --branch、git rev-parse HEAD、git log -1 --oneline。
- 只从任务给定的干净 base_commit 建独立 branch/worktree；禁止直接使用 integration 主工作树施工。
- 先做“现有实现→合同要求”的差异矩阵，复用现有包；禁止创建 runtime2/workflow2/actor2 或第二套主链。
- 严格遵守 owned_write_set 和 forbidden_write_set；不得修改 docs/design/26～37 的语义。
- 遇到合同歧义、缺少共享Proto/稳定ID/Redis公共原语时停止扩张，在handoff提出 ICR，不自行发明。
- 不修改其他Agent的分支，不直接提交integration，不用ours/theirs整文件解决冲突。
- 不提交密码、Cookie、Ticket、Redis凭据、证书私钥、编译产物或临时日志。
- 每个写命令都检查幂等、相同operation不同payload、Timeout Unknown、旧Fence和响应丢失。
- 结论必须区分 DOMAIN_LOGIC_PASS、UNIT_PASS、INTEGRATION_PASS、REAL_REDIS_PASS、MULTI_PROCESS_E2E_PASS、CAPACITY_PASS。

开始前必须运行并记录：
  go test ./...
  go vet ./...
H5任务还要运行：
  npm test -- --run
  npx tsc --noEmit
  npm run build

交付必须包含：
1. 一个可独立审查的小Commit或明确的小序列Commit；
2. docs/verification/<agent-id>-<task>-handoff.md；
3. 修改文件、测试原始结果、未执行平台项、剩余风险和回滚方法；
4. push后的branch名和完整commit hash。

不要只给建议。完成允许范围内的代码、测试、文档和Commit；若真正阻塞，给出精确阻塞证据。
```

---

## 7. Round 0 / Round 1 首批可直接追加的任务体

### 7.1 I0 · 集成控制 Agent

```text
任务ID：I0-INTEGRATION-CONTROL
目标：建立唯一施工基线、worktree登记、写锁表和合并列车；你不是功能开发Agent。

必须完成：
1. 读取当前backend/H5/合同仓库HEAD与dirty状态，登记CONTRACT_BASELINE、BACKEND_BASELINE、H5_BASELINE。
2. 确认 integration 干净；所有功能任务从干净 Commit 建独立 worktree。
3. 建立 docs/verification/implementation-lock-board.md，记录task、owner、branch、worktree、base、写集、依赖、状态。
4. 为首轮任务分配branch/worktree，不替功能Agent实现代码。
5. 每次合并前检查diff、handoff和测试；每次合并后发布新checkpoint。

允许修改：集成分支、锁表、integration verification、最终composition root。
禁止修改：业务规则、26～37语义、功能包内部实现。

完成时返回当前三类Baseline、已创建任务列表、写集冲突检查和第一个可用checkpoint。
```

### 7.2 E0 · W5B Evidence（历史完成卡，禁止再次派发）

```text
任务ID：E0-DUALGATE-EVIDENCE（COMPLETED / DO_NOT_DISPATCH）
完成提交：f90b11b、b2f65e2、22b84e9、dc235ef、a949761。
证据：双Gate/Fresh Join、Drain真实浏览器、200人4000次续租P99≤5ms、GateBoot三态退役。
后续如修改相关写集，由Q0/Q2按变更范围复测；不得重建旧W5B分支或把本卡再次派给Agent。
```

### 7.3 H0 · H5 测试基线（历史完成卡，禁止再次派发）

```text
任务ID：H0-H5-BASELINE（COMPLETED / DO_NOT_DISPATCH）
仓库：classic-farm-h5 @ 673743823fb4fd876bf462279300f5f56985ab23
环境：Node v22.23.1
证据：npm test -- --run = 76/76；npx tsc --noEmit = PASS；npm run build = PASS。
备注：首次build失败仅因dist内残留root属主文件；修正为developer_026:users后通过，源码无改动。
```

### 7.4 P1A · Shared Contract Baseline（已完成，禁止重复派发）

```text
任务ID：P1A-SCB1
状态：COMPLETED / DO_NOT_DISPATCH
集成提交：77aa0a5 + 4456762
交接：docs/verification/p1a-scb1-handoff.md
目标：基于现有代码补齐第29/31/32～37号跨进程共享合同，形成独立SCB-1；不实现业务裁决，不在本任务完成12→33全迁移。

合同：27的地址/Work语义，28公共Envelope/Error/Result，29三类可信RPC，31稳定身份，32～37 Participant接缝。

owned_write_set：
  api/proto/**
  api/gen/**
  internal/contracts/**
  internal/identity/**
  internal/protocoltest/**
  统一生成脚本

forbidden_write_set：
  CommandCatalog业务迁移、PlayerActor、FarmActor、Redis Key/Lua、Workflow实现、H5、cmd/*/main.go。

必须完成：
1. 审计当前TrustedContext、RouteHeader、WorkIdentity、Participant IDL，列出复用/缺失/冲突。
2. 统一EffectiveIdempotencyKey、OperationRootRef、WorkAddress、SourceAuthorityRef、WorkType、ParticipantStep、ParticipantInvocationID。
3. 登记Player/Farm/Friendship Participant及Typed Step Payload/Result；跨边界Snapshot只有一个Schema。
4. 提供Fake Client/Server、注册完整性测试、Canonical Hash和StableOpaqueID黄金向量。
5. generated code只由脚本产生；reserved编号不复用。
6. SCB-1必须是无业务裁决的独立Commit，优先交I0合入并发布SHARED_CONTRACT_BASELINE。
```

### 7.5 DF0 · Farm 最终规则纯领域包（已完成，禁止重复派发）

```text
任务ID：DF0-FARM-DOMAIN-RULES
状态：COMPLETED / DO_NOT_DISPATCH
集成提交：208263e
交接：docs/verification/df0-farm-domain-rules-handoff.md
目标：只实现第32/36号尚缺的纯领域规则，不接Actor/Redis/RPC。

owned_write_set：internal/farm下明确的新纯领域包、配置模型、mapper-free单元测试；先适配仓库现有命名。
forbidden_write_set：internal/farm/actor、farm/ingress、farm/subscription、Proto、Redis、Workflow、cmd。

必须完成：
1. 初始5块已解锁、最多24块；解锁顺序/等级/价格用版本化配置，输出CandidateState。
2. CropCycle冻结plant_at/mature_at/rule snapshot/base_yield；成熟只按trusted_now派生。
3. 浇水与虫害是独立Incident；P0每茬各一次、首个成功、其余NO_CHANGE；时间先由固定配置决定并可后续随机化。
4. Theft：未成熟拒绝；每好友每茬一次；总上限20%；每槽2%，最多10个有效槽；整数舍入规则确定且不超20%。
5. GuardDecision不扣访客钱包；截留作物减少访客净入库，并按冻结单价补偿农场主金币。
6. OpportunityHeader随CandidateState计算；任何投影都不替代最终裁决。
7. 用表驱动测试覆盖24地块、旧cycle、Harvest/Theft并发、Care重复、10槽与整数边界。
```

### 7.6 DT0 · Task 纯领域包（已完成，禁止重复派发）

```text
任务ID：DT0-TASK-DOMAIN
状态：COMPLETED / DO_NOT_DISPATCH
集成提交：f075f5b
交接：docs/verification/dt0-task-domain-handoff.md
目标：实现第34号纯TaskDomain，不接PlayerActor、Redis、Proto、MQ。

owned_write_set：internal/player/task/**及测试。
forbidden_write_set：PlayerActor/Ingress、Proto、Redis Key/Lua、Workflow、cmd。

必须完成：
1. 固定展示3个每日任务：登录、访问好友一次、好友互动一次。
2. 每日0点按配置时区和trusted_now惰性换Period；不做全服零点扫描。
3. 一条线性成长任务；领取后推进下一条，若新任务已满足则可立即领取，但不自动领奖。
4. 已完成的一次性事实不继续产生无用事件；不新增TaskActor/task-svr/task MQ。
5. Claim只裁决任务真值；operation_id/payload_hash重放与来源Participant去重由PlayerActor统一Receipt层负责，
   TaskDomain不保存ClaimReceipt/AppliedSource；配置版本、period_id、红点revision保持有界并有表驱动测试。
```

### 7.7 Q0 · 测试与故障注入骨架（基础版已完成，后续增量扩展）

```text
任务ID：Q0-TEST-HARNESS
状态：BASELINE_COMPLETED / EXTEND_INCREMENTALLY
集成提交：bb3ed36
交接：docs/verification/q0-test-harness-handoff.md
目标：建立后续所有Agent共享的测试工具和合同追踪，不修改业务实现。

owned_write_set：internal/testkit/**、loadtest/**、明确的cmd/*-probe或*-e2e、docs/verification/q0-*。
forbidden_write_set：业务Actor/领域规则、Proto、Redis生产实现、cmd生产入口。

必须完成：
1. 建立26～37条款→测试矩阵，标出现有PASS与缺口。
2. 提供可控Clock、RPC Gate、Redis超时/响应丢失、Actor崩溃点、慢连接工具。
3. 为同operation重试、payload冲突、Unknown、旧Fence、旧Subscription提供可复用断言。
4. 复用 W5B 已有 200 订阅/多 Gate 工具，扩展广播慢连接、完整玩法和 Soak；不得重复造第二套 Harness，
   Mock 结果也不得标记 CAPACITY_PASS。
5. 保存当前baseline的测试耗时和命令/Actor/Workflow清单，供后续diff比较。
```

---

## 8. CP1/CP2 已完成任务体（历史验收规格，禁止重复派发）

### 8.1 R1 · AuthorityStore Redis Adapter

```text
任务ID：R1-REDIS-AUTHORITY-ADAPTER
状态：COMPLETED_VERIFIED / DO_NOT_DISPATCH
base_commit：4e587936df183dd7e6e7725200f98dec38e69d5d
功能分支：faa5dcf/d8ef450/a0c0992/b2e7ffe
integration提交：d0289a4..930a072
依赖：SHARED_CONTRACT_BASELINE已合入。
目标：把现有authoritystore逻辑合同和auth-v1 KeyBuilder实现为生产Redis Adapter；本任务不接Player/Farm主入口。

owned_write_set：internal/storage/redis/**、authoritystore中的Redis Adapter新文件、Redis probe和测试。
forbidden_write_set：Actor/业务规则、Proto/identity算法、Friend/Mail业务Repository、cmd/player-svr、cmd/farm-svr。

必须完成：
1. 先审计旧blobstore与新31号合同，禁止复制第二套KeyBuilder。
2. Slot Golden、ScriptRegistry、EVALSHA/NOSCRIPT。
3. LoadAndClaim、Commit、CommitWithOrders、FenceAndClaim、WorkLease、Receipt、DueIndex、ResultDirectory。
4. TIMEOUT_UNKNOWN使用真正解析线性化点；覆盖“脚本已执行但响应丢失”。
5. 旧writer迟到写拒绝、item/byte上限、Typed Status映射。
6. 有公司Redis权限则跑真实Probe；持久化/Failover未知必须保留Pending。
```

### 8.2 P1B · 12→33 CommandCatalog 唯一迁移

```text
任务ID：P1B-COMMAND-CATALOG-33
状态：COMPLETED_VERIFIED / DO_NOT_DISPATCH
base_commit：4e587936df183dd7e6e7725200f98dec38e69d5d
功能分支：2829283/561d662
integration提交：c3adf06/e1bf29a
依赖：SCB-1。
目标：将当前12条旧命令一次迁移为第28号33类公开命令；不保留运行时双主链。

owned_write_set：api/proto/api-gen（P1单写）、internal/gateway/commandcatalog、protocoltest、迁移黄金向量。
forbidden_write_set：业务Handler裁决、Actor、Redis、H5页面、cmd composition root。

必须完成：
1. 输出12→33保留/重命名/删除/reserved/新增矩阵。
2. 每个命令恰好一个CommandSpec：Typed Payload/Result、Admission、FirstHop、RouteKey、Root、Idempotency、Retry、Timeout、RateLimit。
3. 必须包含GET_PLAYER_BOOTSTRAP和GET_NEXT_FRIEND_OPPORTUNITY。
4. 完成CanonicalPayloadBuilder；同operation不同payload稳定冲突。
5. FarmFrame顶层oneof，业务命令command_type+bytes；CommandResult与ServerPush分开。
6. 生成H5消费清单和公网黄金向量，不向H5暴露VirtualShard/Redis Key。
```

### 8.3 SO0 · Friendship Authority 与机会发现核心

```text
任务ID：SO0-FRIEND-AUTHORITY
状态：COMPLETED / DO_NOT_DISPATCH（真实Redis Adapter与Join/Admission接线仍后续）
base_commit：4e587936df183dd7e6e7725200f98dec38e69d5d
功能分支：9501ad8/09b5852
integration提交：9ecea40/e826786
依赖：SCB-1；R1公共Redis接口稳定后才接真实Adapter。
目标：收敛现有好友/分享代码到第33/33A号，不接FarmActor内部。

owned_write_set：Friend Pair/Quota/ShareIssuer Repository、Relation/Block/Delete/Permit纯逻辑、Opportunity查询核心及测试。
forbidden_write_set：PlayerActor/FarmActor、farm/subscription、Proto、公共Redis KeyBuilder/Lua框架、cmd入口。

必须完成：
1. canonical FriendRelation是真值；索引可重建，Permit不依赖延迟索引。
2. 多人兑换分享、RelationEpoch、删除/拉黑立即阻止新操作、重加Barrier。
3. Summary只做候选；BatchEvaluate做少量精确检查。
4. 连续导航排除本轮visited；进入推荐农场并Ready后视为visited，不要求操作成功。
5. 不为一个成熟农场向全部离线好友做逐人持久扇出。
```

### 8.4 MA0 · PlayerMail EntryStore

```text
任务ID：MA0-PLAYER-MAIL-ENTRY-STORE
状态：COMPLETED_VERIFIED / DO_NOT_DISPATCH
base_commit：cp2a-20260804
候选提交：f4e543eb3f7d2ac0b5fd1576894e6f972a618921
integration merge：5e31c2dea65136fa3c0486c6d77765dd5815588e
检查点：cp2-20260804
依赖：SCB-1与R1公共Redis接口已进入CP2A；不得手拼Key/Lua。
目标：实现第35/35A号非Actor邮箱Authority，不接PlayerActor附件资产。

owned_write_set：PlayerMail Entry/State/Index/Summary Repository、邮件专属小Lua、GC和测试。
forbidden_write_set：PlayerActor、Proto、公共Redis KeyBuilder、mail-svr/MailActor、全服Campaign、cmd入口。

必须完成：
1. 系统定向邮件、每页6封、最多60封、默认30天、未领取附件保护。
2. InsertIfAbsent投递防重、详情、已读、删除、稳定游标和有界多取。
3. 生成TrustedMailClaimMaterial；客户端Claim只提交mail_id。
4. Unknown、GC、容量、ACL边界有测试；平台参数保持Pending。
5. 不实现玩家互发、未来可见Scheduler、邮件MQ或独立服务。

完成证据：两轮审查、定向 race、整仓 race/vet/build/diff 全绿，第二轮独立审查无 P0/P1。后续新任务只处理
Receipt 有界清理、DueRef 重登记崩溃窗口、真实 Redis 平台探针和已登记 P2，不得重做 MA0 主体。
```

### 8.5 DP0 · 简化宠物纯领域（已完成，禁止重复派发）

```text
任务ID：DP0-PET-DOMAIN
状态：COMPLETED / DO_NOT_DISPATCH
集成提交：439f034
交接：docs/verification/dp0-pet-domain-handoff.md
启动：CP0 后即可开始；虽然任务卡列在本节，但纯领域阶段不依赖 SCB-1。接 Actor/Proto/Redis 必须等待后续检查点。
目标：实现第36号PetState和GuardSnapshot输入，不接Actor/Redis/RPC。

owned_write_set：internal/player/pet/**及测试。
forbidden_write_set：PlayerActor、FarmActor、Proto、Redis、Workflow、cmd。

必须完成：金币购买或任务奖励解锁；最多32只；狗粮每份8小时、最多24小时；未喂食无守护；切换不扣粮；
版本化PetState/Guard规则；生成不可变GuardSync输入；下架不删除已拥有宠物；完整并发/重复测试。
```

### 8.6 DC0 · 图鉴活动档案缺口

```text
任务ID：DC0-COMPENDIUM-DOMAIN
状态：COMPLETED / DO_NOT_DISPATCH
base_commit：4e587936df183dd7e6e7725200f98dec38e69d5d
功能分支：95e8e61
integration提交：abcb4b6
目标：保留现有首次自收获解锁，补第37号活动档案、灰图目录和Seen语义的纯领域部分。

owned_write_set：internal/player/compendium/**及测试。
forbidden_write_set：PlayerActor、Proto、Redis、H5全局Store、cmd。

必须完成：crop_definition_id唯一身份；Plant冻结资格；活动结束后已解锁进入永久档案、未解锁保持灰剪影；
遗留种子仍可种；revision/seen_revision单调；缺Catalog只占位告警不删事实；状态有界。
```

---

## 9. CP2 之后任务体

### 9.1 AR1 · AuthorityStore 主链接线

```text
任务ID：AR1-AUTHORITY-MAINLINE
依赖：R1、SCB-1、P1B接口已合入。
目标：把player-svr/farm-svr从旧blobstore切到新AuthorityStore，并接WriterClaim/Fence；本任务不顺手补玩法。

owned_write_set：Actor Runtime/Registry/Persistence Adapter、Player/Farm Store装配接缝；composition root最终由I0落地。
forbidden_write_set：领域规则、Proto、Redis脚本、Workflow业务状态机、H5。

必须完成：
1. LoadAndClaim、singleflight激活、Mailbox、AuthorityCommit、失权自Fence、passivation/drain。
2. 明确旧数据处理：测试环境清空或一次性离线迁移；禁止双写。
3. 用最小Buy/Sell和空Farm写链证明CAS、Unknown、旧writer迟到拒绝。
4. 进程崩溃后冷加载恢复；不实现Actor内存热迁移。
5. 每Actor内存、邮箱水位、激活/回收指标接缝存在。
```

### 9.2 WF1 · Workflow/Order/Dispatcher 收敛

```text
任务ID：WF1-WORK-RUNTIME-CONVERGENCE
依赖：R1、SCB-1。
目标：复用现有四类Workflow，不重写；补齐来源同槽Order、DirectDispatcher快路径和farm-worker DueIndex恢复。

owned_write_set：internal/workflow/**、internal/worker/**、测试；最终cmd装配由I0。
forbidden_write_set：Actor内部状态、Proto、Redis公共脚本、业务规则、Gate编排。

必须完成：CreateIfAbsent、WorkLease、ParticipantReceipt、稳定Step Hash、Dispatcher有界队列、退避和恢复；
来源事实+Order+DueIndex可恢复发现；队列只加速不当权威；用TestWorkflow覆盖每个崩溃点，再适配现有Plant/Harvest/Care/Theft。
```

### 9.2A P1B-CONSUMERS-2 · FirstHop/Ingress 最终消费者收口

```text
任务ID：P1B-CONSUMERS-2
状态：COMPLETED_VERIFIED / DO_NOT_REDISPATCH
最终候选：a22dca9814680bd7d5d874ad7fc42ec6e4eb3015
integration合并：74de91925409efdfa9a5e53e6d551b394d4942ab
依赖：AR1/WF1已进入后端0564a51，H1已进入H5 6e582d4；第28/29号与p1b handoff为语义输入。
目标：不重建Proto/Catalog，只把现有FirstHop消费者迁移到最终CommandCatalog decoder、trusted Validate、
RouteKey校验和权威Canonical Payload Hash，移除不再需要的旧Envelope Hash/compatibility seam。

owned_write_set：internal/player/ingress/**、internal/farm/ingress/**、消费侧trusted/idempotency/workflow兼容薄层、
Gate/sharded/stress工具中仍读取旧Envelope字段的机械消费者、对应测试和
docs/verification/p1b-consumers2-handoff.md。所有消费者迁移后，额外允许机械删除api/proto/common中原P1B
handoff登记的deprecated编译窗口并重生成api/gen；不能新增或改变现行协议语义。
forbidden_write_set：CommandCatalog定义/33命令/Payload/Result/v4现行字段号、Actor Runtime/Registry、
业务Actor规则、Workflow状态机、Redis公共KeyBuilder/Lua、H5、cmd composition root、26～37合同正文。

必须先交付“旧消费者→最终Catalog/Decoder/CanonicalBuilder”审计矩阵，再最小修改。必须证明：
1. 同operation_id + 同语义payload得到相同权威hash；同operation_id + 不同语义payload产生冲突；
2. request_id、连接/路由等传输噪声不改变权威hash，客户端旧兼容字段不能改变身份、目标或hash；
3. FirstHop从原始typed payload独立解码并校验RouteKey/TrustedContext，畸形、错型、未知字段fail closed；
4. Redirect/弱网重试保留原operation_id；仍feature-disabled的26条命令继续明确拒绝，禁止伪成功；
5. Proto只允许删除原P1B handoff §4.1 已登记的deprecated窗口、合法reserved并重生成；任何新增字段、
   编号/语义变化或Catalog改动都必须停工提交ICR。

验收：目标包race、整仓race、vet、build、gen-proto零差异、diff/clean；候选必须再经独立detached只读审查。
```

### 9.3 PI1 · PlayerActor 单写接线

```text
任务ID：PI1-PLAYER-ACTOR-INTEGRATION-<MILESTONE>
每次只接一个里程碑，不建立跨M2～M6的长期分支。
目标：把已合入的Player纯领域Transition接入单PlayerActor Mailbox和一次AuthorityCommit。

owned_write_set：internal/player/actor/**、internal/player/ingress/**；cmd由I0。
forbidden_write_set：FarmActor、Proto、Redis Key/Lua、Workflow Runtime、其他里程碑未授权功能。

共同要求：Root幂等→CandidateState→Receipt/Outcome/Orders→一次Commit；Unknown返回PROCESSING；Participant相同Invocation重放命中Receipt。
按里程碑分别接：M1 Economy；M2 Coin/Seed Reservation；M3 Harvest+Task+Compendium；M5 Pet/双方Theft结算；M6 MailClaim。
```

### 9.4 FI1 · FarmActor 单写接线

```text
任务ID：FI1-FARM-ACTOR-INTEGRATION-<MILESTONE>
每次只接一个里程碑。
目标：把Farm纯领域规则接入单FarmActor Mailbox、AuthorityCommit、FarmVersion与PostCommitDeliveryGuard。

owned_write_set：internal/farm/actor/**、internal/farm/ingress/**；subscription只通过冻结接口使用。
forbidden_write_set：PlayerActor、Proto、Redis Key/Lua、Workflow Runtime、farm/subscription内部重写、cmd。

共同要求：绑定plot/crop_cycle/incident/attempt ID；一次Commit写权威事实、Receipt、Outcome、Orders；
Commit后只生成一次FarmChange；投递失败使实时流失效并要求Fresh Join，不能回滚权威事实。
按里程碑分别接：M2 Unlock/Plant；M3 HarvestDecision；M4 Care/Pest/Opportunity；M5 Guard/TheftDecision。
```

### 9.5 SC1 · 跨 Actor Coordinator

```text
任务ID：SC1-SCENARIO-<UNLOCK|PLANT|HARVEST|THEFT>
依赖：Participant接口和对应PI1/FI1 Handler已合入。
目标：Actor外推进一个持久Workflow；Gate不编排Participant，Actor Turn不等待远程RPC。

owned_write_set：一个独立Scenario/Coordinator包、消费侧小Port、Fake、状态机测试。
forbidden_write_set：PlayerActor/FarmActor内部、通用Workflow、Proto、Redis Key/Lua、Gate。

必须完成：先CreateIfAbsent，再按合同推进Participant；每步稳定Invocation/Hash；响应丢失只查Receipt；
Coordinator退出后worker能接管；同operation不同payload冲突；输出完整故障矩阵。
```

### 9.6 H1/H2 · H5 集成

```text
任务ID：H1-H5-WIRE 或 H2-H5-<MILESTONE>
仓库：classic-farm-h5
目标：H1一次接最终公网协议和Reducer；H2每次只接一个业务里程碑页面。

单写区域：WsClient、OperationJournal、全局Store、Farm Snapshot/Delta reducer同时只有一个H5 Integration Owner。
页面组件可并行，但只能消费稳定selector/action。

必须保持：Authenticated不等于InFarm；ACCEPTED→Snapshot→Delta*→Ready；旧subscription push丢弃、旧已准入CommandResult仍按operation处理；
PROCESSING进入Journal恢复；好友/邮件等弱依赖失败显示Loading/重试，不退出自己的农场；所有uint64以string处理。
```

### 9.7 RT1 · 最小 Route Authority 控制面、动态 Assignment 与 Shard Drain

```text
任务ID：RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL
依赖：AR1主链稳定。
目标：在现有ActorRouteClient/静态Snapshot之上补齐 RouteManager→ShardWriteAuthority→Assignment Watch→
HolderLeaseGuard→FirstHop Route Fence 的最小生产闭环，不引入完整UC Proxyless依赖。

owned_write_set：internal/actorrouting/**、独立RouteManager/AssignmentStore包、测试；最终composition root由I0。
forbidden_write_set：业务Actor状态、Proto公网协议、Redis Authority脚本、玩法规则。

必须完成：RouteManager先提交ShardWriteAuthority再发布Snapshot、Snapshot+revision Watch、原子本地表替换、
Holder连接池、Redirect纠旧、Lease/epoch校验、HolderLeaseGuard本地单调Deadline和FirstHop完整RouteHeader校验；
Drain时停止新Actor激活、完成当前Turn、强事实持久化、转移Shard、新Holder冷加载；崩溃时Lease到期/新epoch防旧写。
不实现透明Socket迁移和Actor内存热迁移；热点农场先冷加载+客户端Fresh Join。

RT1完成前禁止把测试StaticFenceSource接入生产cmd；业务短包可以并行，但生产NewAuthorityRegistry切换由I0
在RT1合入后串行完成。详细决策见ar2-production-authority-cutover-decision.md。
```

### 9.8 Q2 · 容量与生产证据

```text
任务ID：Q2-CAPACITY-AND-FAILURE
目标：只测已经合入的真实主链，生成可复现Evidence Bundle。

必须覆盖：
- 200人同场Join/续租/广播、共享Snapshot/VersionRing、慢连接和Fresh Join；
- 热点Farm Mailbox、不同Plot操作、同Plot冲突；
- Redis Lua P50/P95/P99、Unknown、NOSCRIPT、断连/Failover；
- Holder崩溃、Shard Drain、旧writer迟到写；
- 3000万DAU→PCU→命令QPS→Redis/Actor/下行容量推导；
- 每人每天5～6次、每次小于5分钟、最多300好友的基线与压力模型。

不得修改业务实现让测试通过；不得用Fake结果标REAL_REDIS_PASS或CAPACITY_PASS。
```

---

## 10. 用户在 Linux 上的实际操作顺序

1. `[已完成]` P1B-CONSUMERS-2 由唯一 writer 从派发 Tag 实现，TargetFarmPolicy P1 交回原 session 修复；
2. `[已完成]` 第二轮 detached 只读复审无 P0/P1，I0 完成整仓 race/vet/build/gen-proto/diff/clean；
3. `[已完成]` `74de919` 合入 integration，并发布 `cp3-20260804`；
4. `[已完成]` 从 CP3 读取真实代码与 handoff，确认 Authority Registry 已有、生产 cmd 仍为 legacy，冻结 AR2 决策；
5. 只从 CP3 派 PI1-A/FI1-A/SC1-A/SO1-A 短包，每个里程碑立即做组件验证与纵向 E2E；
6. RT1 作为独立基础轨并行推进；完成前不得把生产 cmd 切到 NewAuthorityRegistry；
7. 公司 Redis Probe 可并行提供证据，但没有持久化/Failover 事实时不得提升证据等级；
8. 最后由 I0 完成 Authority composition 切换，再做 200 人完整广播和容量证明。

CodeBuddy 使用 Kimi-K3 时若出现错误码 `14003`（模型资源满/排队），这不代表 worktree 内容丢失。
保持原任务和原 worktree，向同一 Agent 发送“请继续执行未完成的任务。”即可；不要因此新建第二个分支、
重做已经完成的步骤或让两个 Agent 同时接管同一写集。只有 Agent 明确报告源码/工作树损坏时，I0 才做
只读状态核验后决定是否恢复。

若返回 `429 当前无可用Token额度`，保持原 Session/Worktree，不循环创建新会话；I0 继续只读审计与
集成准备，团队额度恢复后再向原会话发送继续指令。

推荐 Agent 汇报格式：

```text
task_id:
branch:
worktree:
base_commit:
commit:
owned_write_set:
files_changed:
tests:
evidence_level:
not_executed:
remaining_risks:
next_dependency:
```

如果 Agent 只返回“已经实现”而没有 Commit、测试原始结果和 handoff，不视为完成。
