# Linux Implementation Lock Board

- 状态：`CP3_PUBLISHED / POST_CP3_AUDIT_FROZEN / ROUND3_PACKAGES_READY`
- 更新时间：`2026-08-04T12:20:00+08:00`
- 施工根目录：`/data/home/developer_026/CodeBuddy/20260519141303`

## 1. 冻结基线

| 名称 | Commit / Tag | 说明 |
|---|---|---|
| `CONTRACT_BASELINE` | `a78196a48d044a350a3b761c1d73b404f44a18c1` | Windows 权威 `docs/design` 合同内容基线；业务语义仍以本地 26～37 与 Manifest 为准 |
| `BACKEND_BASELINE / CP0` | `6b78d2f5b7793afe8636d919280c85c7849d6200` | W5B 与合同登记后的后端基线 |
| `SHARED_CONTRACT_BASELINE` | `44567622d18f65e41f63fe46d164363e4d1aa8e1` | SCB-1 共享身份、Participant IDL 与黄金向量 |
| `BACKEND_CHECKPOINT / CP1` | `f075f5b99af416bab5253e142cbcc884cd16f088` | Round 0 组合检查点 |
| `ROUND1_DISPATCH_BASE` | `4e587936df183dd7e6e7725200f98dec38e69d5d` | R1/P1B/SO0/DC0 的统一分叉点 |
| `ROUND1_MERGE_CANDIDATE` | `e1bf29ae3db472e52c7014f16bb4bd2f0571f0f3` | 四轨候选；分支 `codex/round1-merge-candidate-20260804` |
| `CP2A_CODE_MERGE` | `94155566b08bccfb453b344fdee046918f199953` | 候选与 integration 文档历史的显式 merge commit |
| `CP2A_DISPATCH_CHECKPOINT` | `cp2a-20260804` | 71～73 与本锁表同步后创建的稳定 Tag；后续 Agent 只从该 Tag 分叉 |
| `MA0_CANDIDATE` | `f4e543eb3f7d2ac0b5fd1576894e6f972a618921` | PlayerMail 两轮修复与独立只读审查通过的最终候选 |
| `CP2_CODE_MERGE` | `5e31c2dea65136fa3c0486c6d77765dd5815588e` | MA0 显式合入 integration 的 merge commit |
| `CP2_CODE_CHECKPOINT` | `cp2-20260804` | W5B + Round 0/1 + MA0 的代码证据检查点 |
| `ROUND2_DISPATCH_CHECKPOINT` | `cp2-round2-20260804` | WF1并行依赖澄清后创建；AR1/WF1/H1 只能从该 Tag 分叉 |
| `ROUND2_BACKEND_MERGE` | `0564a5194d0c1df88b50d899db6fa8ecf8208bf8` | AR1 与 WF1 已独立审查、整仓验证并合入后端 integration |
| `ROUND2_H5_MERGE` | `6e582d4dea3674b0f58b6f1d65d91cf07c09f308` | H1 已独立审查、全量测试/构建并合入 H5 integration |
| `P1B_CONSUMERS2_DISPATCH` | `round2-p1b-consumers2-20260804` | 71～73 与锁表对齐后发布；P1B-CONSUMERS-2 的唯一分叉点 |
| `P1B_CONSUMERS2_CANDIDATE` | `a22dca9814680bd7d5d874ad7fc42ec6e4eb3015` | 原 writer 修复 TargetFarmPolicy P1 后的最终候选；第二轮独立审查无 P0/P1 |
| `P1B_CONSUMERS2_MERGE` | `74de91925409efdfa9a5e53e6d551b394d4942ab` | P1B-CONSUMERS-2 显式合入 backend integration 的代码合并点 |
| `CP3_CODE_CHECKPOINT` | `cp3-20260804` | P1B、71～73 与锁表收口后的稳定分叉点；Round 3 只从该 Tag 分叉 |
| `W5B_COMPLETED` | `a949761d360bcbda9bbf0190c9d3c3f99bc1b60d` | 双 Gate、Fresh Join、Drain、200 人 Lease、GateBoot 证据闭合 |
| `H5_BASELINE` | `673743823fb4fd876bf462279300f5f56985ab23` | Node v22.23.1，76/76 tests、tsc、build 通过 |

## 2. Round 0 已关闭写锁

| Task | 集成提交 | 状态 |
|---|---|---|
| `P1A-SCB1` | `77aa0a5` + `4456762` | `COMPLETED / DO_NOT_DISPATCH` |
| `DF0-FARM-DOMAIN-RULES` | `208263e` | `COMPLETED / DO_NOT_DISPATCH` |
| `DT0-TASK-DOMAIN` | `f075f5b` | `COMPLETED / DO_NOT_DISPATCH` |
| `DP0-PET-DOMAIN` | `439f034` | `COMPLETED / DO_NOT_DISPATCH` |
| `Q0-TEST-HARNESS` | `bb3ed36` | `BASELINE_COMPLETED / EXTEND_INCREMENTALLY` |

## 3. Round 1 已关闭写锁

四个任务均已停止 CodeBuddy Session，功能分支保留供审计，不得重新启动相同任务或让新 Agent 继续占用旧锁。

| Task | 功能分支交付 | integration 提交 | 结果 | 状态 |
|---|---|---|---|---|
| `R1-REDIS-AUTHORITY-ADAPTER` | `faa5dcf`、`d8ef450`、`a0c0992`、`b2e7ffe` | `d0289a4`～`930a072` | Redis Adapter/Lua、Fence/CAS/Unknown、WorkLease、DueIndex、ResultDirectory v2；unit/race/full/vet/build 通过，未申报真实公司 Redis | `COMPLETED_VERIFIED` |
| `P1B-COMMAND-CATALOG-33` | `2829283` + `561d662` | `c3adf06` + `e1bf29a` | 33/33 唯一 Catalog、FarmFrame v4 oneof、uint64 sequence、Typed Payload/Result 与 Golden；7 enabled / 26 feature-disabled | `COMPLETED_VERIFIED` |
| `SO0-FRIEND-AUTHORITY` | `9501ad8` + `09b5852` | `9ecea40` + `e826786` | Friend Pair/Quota/Share/Permit/Opportunity 纯逻辑与 Repository Port | `COMPLETED` |
| `DC0-COMPENDIUM-DOMAIN` | `95e8e61` | `abcb4b6` | 活动档案、灰图、Seen/Revision 纯领域 | `COMPLETED` |

## 4. CP2 组合证据

Round 1 候选 `e1bf29a`、MA0 候选 `f4e543e` 与 integration merge `5e31c2d` 均独立通过：

```text
bash scripts/gen-proto.sh                 # 重生成零差异
git diff --check
go test -race -count=1 -timeout=600s ./...
go vet ./...
go build -buildvcs=false ./...
git status --porcelain                    # clean
```

MA0 第二轮只读审查结论：`PASS / no P0-P1`。CP2 额外执行整仓
`go test -race -count=1 -timeout=600s ./...`。证据等级：`COMBINED_RACE_PASS`。这不等于 `REAL_REDIS_PASS`、
`PRODUCT_E2E_PASS` 或生产容量证明。

首次 merge 因 integration 中 root 属主文件失败；残留已备份到：
`/data/home/developer_026/CodeBuddy/20260519141303/.merge-recovery/round1-failed-merge-QLZl5O`。
工作区属主已统一还给 `developer_026:users`，随后 merge 与全量验证成功。

## 5. 当前写锁与下一轮派发

### MA0 · PlayerMail EntryStore（已关闭）

```text
status: COMPLETED_VERIFIED / DO_NOT_REDISPATCH
base: cp2a-20260804
base_commit: f63cf8935b203196fe13cd0334b059bd777e2e6e
candidate_commit: f4e543eb3f7d2ac0b5fd1576894e6f972a618921
integration_merge: 5e31c2dea65136fa3c0486c6d77765dd5815588e
checkpoint: cp2-20260804
branch: codex/ma0-player-mail-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-ma0-player-mail
session_id: round15-ma0-player-mail-20260804
model: kimi-k3-ioa
orchestrator: I0
owned_write_set:
  - 新增 PlayerMail Entry/Index/Summary Repository 包
  - 邮件专属小 Lua、分页/GC/Unknown 测试
  - docs/verification/ma0-player-mail-handoff.md
forbidden_write_set:
  - PlayerActor 与附件资产领取
  - Proto / CommandCatalog
  - R1 公共 KeyBuilder / ScriptRegistry 主体
  - mail-svr / MailActor / MQ / cmd composition root
```

规则：每页 6 封、最多 60 封、默认 30 天、未领取附件保护；只实现非 Actor 邮箱 Authority。

残余项：Receipt 有界清理、DueRef 重登记崩溃窗口、真实 Redis Cluster/ACL/Failover/容量参数和已登记
P2；这些需要新任务与新写锁，不能继续复用 MA0。

### Round 2 · 已关闭写锁

#### AR1 · Actor Authority Mainline

```text
status: COMPLETED_VERIFIED / DO_NOT_REDISPATCH
base: cp2-round2-20260804
base_commit: 2d894be630e5cc7faab53060bf342e9f1be9ed37
branch: codex/ar1-authority-mainline-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-ar1-authority-mainline
session_id: round2-ar1-authority-mainline-20260804
candidate_commit: 3ac4269695dd609a573f728a53e9de6cb85f8728
integration_merge: 32c99643512adb3bf8bbcc459747ba5c60437ce3
review: PASS / original P1 closed / no remaining P0-P1
model: kimi-k3-ioa
owned_write_set:
  - internal/logic/actor/**
  - internal/actorrouting/** 的 Runtime/Holder 接缝（不含动态控制面）
  - Player/Farm Registry 的 Authority 接线与测试
  - docs/verification/ar1-authority-mainline-handoff.md
forbidden_write_set:
  - Proto/CommandCatalog、Ingress、业务 Actor 状态与规则
  - Workflow、Redis Lua/KeyBuilder、cmd、H5、合同正文
```

#### WF1 · Workflow Runtime Convergence

```text
status: COMPLETED_VERIFIED / DO_NOT_REDISPATCH
base: cp2-round2-20260804
base_commit: 2d894be630e5cc7faab53060bf342e9f1be9ed37
branch: codex/wf1-runtime-convergence-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-wf1-runtime-convergence
session_id: round2-wf1-runtime-convergence-20260804
candidate_commit: dc2065b6c0f19940a5b121b6b7dcb1bb6cdc7f3c
integration_merge: 0564a5194d0c1df88b50d899db6fa8ecf8208bf8
review: PASS / no P0-P1
model: kimi-k3-ioa
owned_write_set:
  - internal/workflow/**
  - 必要时新增的纯通用 internal/worker/**
  - docs/verification/wf1-runtime-convergence-handoff.md
forbidden_write_set:
  - Actor/Ingress、Proto、Redis公共Lua/KeyBuilder、cmd、业务规则、H5
```

WF1 与 AR1 的组合候选已经通过整仓 race、vet、build、diff 与 clean 检查。Participant 生产 Adapter 和
composition root 仍属于后续纵向里程碑，不得借 WF1 名义继续占用旧写锁。

#### H1 · H5 Wire / Journal / Reducer

```text
status: COMPLETED_VERIFIED / DO_NOT_REDISPATCH
h5_base_commit: 673743823fb4fd876bf462279300f5f56985ab23
backend_readonly_base: cp2-round2-20260804
branch: codex/h1-wire-reducer-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-h1-wire-reducer
session_id: round2-h1-wire-reducer-20260804
candidate_commit: d7d053bcaedf324fe684b24f50ecda707db95460
integration_merge: 6e582d4dea3674b0f58b6f1d65d91cf07c09f308
review: PASS / no P0-P1
model: kimi-k3-ioa
owned_write_set:
  - src/protocol/**、WsClient、Operation Journal、farm-store、协议接缝测试
  - docs/verification/h1-wire-reducer-handoff.md
forbidden_write_set:
  - backend、具体玩法页面/数值、内部Actor/Redis字段、双协议可写主链
```

#### Q1 · 独立验证

状态：`COMPLETED_FOR_AR1_WF1_H1_P1B`。AR1/WF1/H1 均完成独立只读审查；AR1 的唯一 P1 已由原
writer session 修复并复审关闭。P1B 主审发现的 TargetFarmPolicy P1 也由原 writer 修复，第二轮独立
detached 复审 PASS、无 P0/P1。P2 已进入各自 handoff，不阻塞 Round 3。

### P1B-CONSUMERS-2

```text
status: COMPLETED_VERIFIED / DO_NOT_REDISPATCH
dispatch_tag: round2-p1b-consumers2-20260804
effective_base_commit: 83e17ad2ce44d1b5f589e62ac0f0c71cb1fa8fa1
candidate_commit: a22dca9814680bd7d5d874ad7fc42ec6e4eb3015
integration_merge: 74de91925409efdfa9a5e53e6d551b394d4942ab
branch: codex/p1b-consumers2-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-p1b-consumers2
session_id: round2-p1b-consumers2-20260804
pid_at_launch: 2381003
model: kimi-k3-ioa
owned_write_set:
  - internal/logic/trusted/**
  - internal/player/ingress/**、internal/farm/ingress/**
  - internal/idempotency/**、internal/workflow/identity.go 薄层
  - Gate/sharded/stress 工具中旧 Envelope 消费接缝
  - 所有消费者迁移后，原 P1B handoff 登记的 deprecated Proto 编译窗口机械删除与生成代码
  - docs/verification/p1b-consumers2-handoff.md
forbidden_write_set:
  - CommandCatalog 33 命令、Payload/Result、v4 正式线格式和现行编号
  - Actor Runtime/Registry/业务状态、Workflow 状态机、Redis 公共 KeyBuilder/Lua
  - H5、服务 composition root、26～37 合同正文
```

它已完成 trusted Validate、player/farm ingress、完整 TargetFarmPolicy、权威 Canonical Payload Hash
与 deprecated 兼容窗口收口。最终候选经第二轮独立 detached 复审无 P0/P1，integration 整仓
race/vet/build/gen-proto/diff/clean 通过。旧 branch/worktree/session 仅供审计，不得恢复为新 writer。

## 6. CP3 后生产装配审计

审计已确认：AR1 完成的是 Authority Store → Registry 组件接缝，当前 `cmd/player-svr`、
`cmd/farm-svr` 仍使用 legacy blobstore，`cmd/farm-worker` 仍使用 legacy Workflow Runner。
`HolderLeaseGuard` 尚未接入生产 Registry Admission，RouteManager/Assignment Watch/完整 FirstHop
Route Fence 也没有形成生产闭环。

冻结边界：

```text
PI1-A / FI1-A / SC1-A / SO1-A
→ 可以从CP3派发短包
→ 只导出模块、Handler、Definition和测试
→ 禁止修改cmd composition root

RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL
→ 独立基础轨
→ 完成后才允许I0把生产cmd切到NewAuthorityRegistry
```

禁止把测试 `StaticFenceSource` 直接接入生产入口。详细依据：
`docs/verification/ar2-production-authority-cutover-decision.md`。

当前无运行中的 writer 进程。三个 CP3 只读审计与下面两个 Round 3 Writer 会话均因团队 Token 配额
返回 `429`，没有产生代码或工作树修改；I0 已完成人工交叉审计，不把模型配额问题当成代码失败。

### Round 3 已派发但等待外部 Agent 接管

```text
PI1-A1-PLAYER-PLANT-PARTICIPANT
  status: QUOTA_WAIT / PAUSED_BY_USER / WORKTREE_CLEAN
  branch: codex/pi1-a1-player-plant-participant-20260804
  worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-pi1-a1-player-plant
  session: round3-pi1-a1-player-plant-20260804
  prompt: .agents/round2-20260804/prompts/pi1-a1-player-plant-participant.prompt.txt

SO1-A-FRIEND-REDIS-ADAPTER
  status: QUOTA_WAIT / PAUSED_BY_USER / WORKTREE_CLEAN
  branch: codex/so1-a-friend-redis-adapter-20260804
  worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-so1-a-friend-redis
  session: round3-so1-a-friend-redis-20260804
  prompt: .agents/round2-20260804/prompts/so1-a-friend-redis-adapter.prompt.txt
```

详细接管说明：`docs/verification/round3-linux-agent-handoff-20260804.md`。原 CodeBuddy Session 与外部
Linux Agent 不得同时写同一工作树；接管前必须确认原 PID 已退出。

## 7. 当前真实未完成项

- R1：公司 Redis ACL/持久化/Failover/延迟 Probe、CompactReceipt/GC；AR1 Registry 接缝已完成，生产 composition 尚未切换；
- P1B：Consumers-2 已关闭；GET_OPERATION_RESULT 等 26 条业务命令仍按 Feature Disabled 明确拒绝，并随业务里程碑逐条启用；
- SO0：真实 Redis Adapter、Join/ActionAdmission/连续导航接线；
- DC0：PlayerActor/Proto/H5/红点接线；
- 产品 E2E、200 人完整广播、动态 Assignment、TCONND 与 3000 万 DAU 容量证据仍后置。

## 8. 合并列车

```text
CP2A
→ MA0（已完成）
→ CP2（历史检查点，5e31c2d）
→ AR1 + WF1 + H1 + Q1（已完成；后端 0564a51，H5 6e582d4）
→ P1B-CONSUMERS-2（已完成：a22dca9 / merge 74de919）
→ CP3（已发布：cp3-20260804）
→ PI1-A/FI1-A/SC1-A/SO1-A（短包，可并行）
→ RT1 + I0 Authority production cutover
→ M1～M6 纵向业务里程碑
→ 路由/200人/故障/部署/容量证据
```

## 9. 14003 / 429 恢复纪律

Kimi-K3 出现 `14003` 只表示模型资源满，不代表 worktree 丢失。保持原 branch、worktree 和 CodeBuddy
session，向同一会话发送：

```text
请继续执行未完成的任务。
```

不得创建第二分支、重复已完成步骤或让两个 Agent 同时写同一写集。只有 Agent 明确报告工作树损坏时，
I0 才先做只读核验。

若返回 `429 当前无可用Token额度`，说明团队 Token 配额不可用。保持原 Session/Worktree，不循环创建
新会话；I0 继续做只读审计、文档和可独立完成的集成工作，额度恢复后再向原会话发送继续指令。
