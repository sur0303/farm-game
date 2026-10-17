# 72 · 多 Agent 工作包执行手册

- 版本：`v2.0-post-cp3-audit`
- 日期：2026-08-04
- 状态：`ACTIVE_IMPLEMENTATION / CP3_PUBLISHED / POST_CP3_AUDIT_FROZEN / ROUND3_PACKAGES_READY`
- 合同内容基线：`a78196a48d044a350a3b761c1d73b404f44a18c1`
- CP2A 代码合并点：`94155566b08bccfb453b344fdee046918f199953`
- CP2A 稳定名称：`cp2a-20260804`
- CP2 代码合并点：`5e31c2dea65136fa3c0486c6d77765dd5815588e`
- CP2 代码检查点：`cp2-20260804`
- Round 2 最终派发名称：`cp2-round2-20260804`
- Round 2 后端合并点：`0564a5194d0c1df88b50d899db6fa8ecf8208bf8`
- Round 2 H5 合并点：`6e582d4dea3674b0f58b6f1d65d91cf07c09f308`
- P1B-CONSUMERS-2 派发名称：`round2-p1b-consumers2-20260804`
- P1B-CONSUMERS-2 最终候选：`a22dca9814680bd7d5d874ad7fc42ec6e4eb3015`
- P1B-CONSUMERS-2 后端合并点：`74de91925409efdfa9a5e53e6d551b394d4942ab`
- CP3 稳定名称：`cp3-20260804`
- 上位计划：[71 · 多 Agent 实施编排基线](./71-multi-agent-implementation-plan-draft.md)

当前 Linux 基线下的实际派发顺序、检查点以及可直接复制的 Prompt 见：
[73 · Linux 多 Agent 施工调度与 Prompt 手册](./73-linux-multi-agent-dispatch-runbook.md)。本文保留完整角色卡，
第 73 号负责把大角色拆成短分支工作包，避免一个 Agent 长期占用 PlayerActor、FarmActor 或 Proto 写锁。

## 0. 怎么使用本手册

本文把第 71 号的实施波次进一步拆成可以直接派给 Linux Agent 的任务卡。每张卡都包含：

```text
何时启动
读取哪些合同
允许修改哪些文件
禁止修改哪些文件
按什么步骤完成
必须产出什么
必须执行哪些测试
什么条件下才算完成
交给谁继续
```

第 26～37 号是唯一语义输入，本文不修改它们。业务与架构语义以及 G0 文档内容已经闭合；W5B 的
实现与运行证据也已经完成，历史 worktree/分支均已清理。Windows 权威 `docs/design` 已同步到 Linux
并登记合同内容基线；I0 派发时使用以下值：

```text
CONTRACT_BASELINE = a78196a48d044a350a3b761c1d73b404f44a18c1
ROUND1_DISPATCH_BASE = 4e587936df183dd7e6e7725200f98dec38e69d5d
CP2A_CODE_MERGE   = 94155566b08bccfb453b344fdee046918f199953
MA0_CANDIDATE      = f4e543eb3f7d2ac0b5fd1576894e6f972a618921
CP2_CODE_MERGE     = 5e31c2dea65136fa3c0486c6d77765dd5815588e
ROUND2_BACKEND_MERGE = 0564a5194d0c1df88b50d899db6fa8ecf8208bf8
ROUND2_H5_MERGE      = 6e582d4dea3674b0f58b6f1d65d91cf07c09f308
P1B_CONSUMERS2_CANDIDATE = a22dca9814680bd7d5d874ad7fc42ec6e4eb3015
P1B_CONSUMERS2_MERGE = 74de91925409efdfa9a5e53e6d551b394d4942ab
CURRENT_BACKEND_BASELINE = cp3-20260804
H5_BASELINE       = 6e582d4dea3674b0f58b6f1d65d91cf07c09f308
W5B_COMPLETION    = a949761d360bcbda9bbf0190c9d3c3f99bc1b60d
SHARED_CONTRACT_BASELINE = 44567622d18f65e41f63fe46d164363e4d1aa8e1
```

任何 Agent 都必须从指定 Baseline 建立独立 worktree，不允许复用 integration 主工作树、已删除的 W5B
历史 worktree 或彼此的 worktree，也不允许两个 Agent 在同一分支提交。

---

## 1. 总体角色与轮次

### 1.1 角色总表

| Agent ID | 角色 | 核心职责 | 是否写业务代码 |
|---|---|---|---|
| B0 | 最终基线登记 Agent | 机械复核 26～37、验证 33 类命令与两条首屏接缝、登记唯一 Baseline | 否 |
| I0 | 集成控制 Agent | 建基线、维护写锁、合并、全仓验证 | 原则上否 |
| S0/E0（历史） | W5B 实现与 Evidence Agent | 实现、双 Gate/Drain/Fresh Join、200 人续租、GateBoot 已在 `a949761` 闭合 | 已退休，禁止再次派发 |
| P1（P1A/P1B 已完成） | Protocol/Shared Contract/Catalog Agent | SCB-1 与 33/33 唯一 Catalog 已进入 CP2A；后续只做登记后的 Consumers-2 增量 | 是，单写 |
| R1（基础 Adapter 已完成） | Redis Authority Agent | Lua、Repository、Unknown、DueIndex、ResultDirectory v2 已进入 CP2A；真实 Redis/AR1 接线仍后续 | 是，单写 |
| DP1 | Player Domain Agent | Economy/Task/Pet/Compendium/MailClaim 纯领域与 Player Participant | 是 |
| DF1 | Farm Domain Agent | Plot/Crop/Care/Theft/Guard/Opportunity 纯领域与 Farm Participant | 是 |
| AR1 | Actor Runtime Agent | 激活、Mailbox、Persistence Lane、Passivation | 是 |
| WF1 | Workflow Runtime Agent | Workflow/Order/Dispatcher/Worker 通用框架 | 是 |
| SO1 | Friendship Authority Agent | Relation/Quota/Share/Permit/撤权 | 是 |
| MA0（已完成） | Mail EntryStore Agent | 邮件非 Actor Authority、六封分页、投递防重、GC/Unknown；候选 `f4e543e` 已进入 CP2 | 禁止重复派发 |
| H1 | H5 Integration Agent | Wire、Journal、Reducer、页面接入 | 是 |
| SC1 | Scenario Coordinator Agent | Unlock/Plant/Harvest/Theft 等跨 Actor 编排 | 是 |
| Q1 | Verification/Capacity Agent | 黑盒验收、故障注入、200 人与容量证据 | 只写测试/工具 |
| O1 | Observability Agent | Metrics/Trace/Log/Dashboard/Alert | 少量接线，后置 |
| PL1 | Platform/Deployment Agent | Docker、多进程环境、K8s 候选、健康检查与 Drain 组装 | 否 |

### 1.2 当前启动轮次

```text
已完成 Round 0 / CP1（integration f075f5b）
├── P1A Shared Contract Baseline：77aa0a5 + 4456762
├── DF0 Farm纯领域：208263e
├── DT0 Task纯领域：f075f5b
├── DP0 Pet纯领域：439f034
└── Q0 Fault/Load Harness：bb3ed36

已完成 Round 1 / CP2A（代码合并点 9415556）
├── R1  AuthorityStore Redis Adapter：完成组件与测试，真实公司Redis/AR1接线待后续
├── P1B 12→33 CommandCatalog唯一迁移：完成；Consumers-2 后续已在 CP3 前关闭
├── SO0 Friendship Authority纯逻辑与Repository Port：完成
├── DC0 Compendium活动档案纯领域缺口：完成
└── I0/Q0 已完成候选与integration两次整仓race/vet/build验证

已完成 Round 1.5 / CP2（`f4e543e` → merge `5e31c2d`）
├── MA0 PlayerMail EntryStore：完成两轮修复与独立只读审查
├── I0/Q0：整仓 race/vet/build/diff 通过
└── `cp2-20260804` 已推送；Mail Receipt/DueRef/平台参数仍显式 Pending

已完成 Round 2（统一从 `cp2-round2-20260804` 分叉）
├── AR1 → `3ac4269`，integration merge `32c9964`，独立审查 PASS
├── WF1 → `dc2065b`，integration merge `0564a51`，独立审查 PASS
├── H1  → `d7d053b`，H5 integration merge `6e582d4`，独立审查 PASS
└── Q1  → AR1 原 P1 已由同一 writer session 修复并复审关闭；三轨均无剩余 P0/P1

P1B-CONSUMERS-2 也已完成：最终候选 `a22dca9` 修复主审发现的 TargetFarmPolicy P1，第二轮独立
只读审查 PASS、无 P0/P1，整仓 race/vet/build/gen-proto/diff/clean 全绿，并以 `74de919` 合入。
CP3 已发布。当前无活动 writer；下一步先核对 CP3 后真实代码差距，再从 `cp3-20260804` 派发短 PI1/FI1。

Round 3（运行时稳定后，按纵向里程碑推进）
├── DP1 Player Participant/PlayerActor
├── DF1 Farm Participant/FarmActor
├── SC1 Scenario Coordinator
├── SO1 Friend Join/ActionAdmission
├── H1  对应H5闭环
└── Q1  场景验收

Round 4
├── Q1  200人/容量/故障矩阵
├── O1  可观测性与告警
├── PL1 Docker/多进程/部署证据
└── I0  最终集成与Evidence Bundle
```

同一轮次可以并发编码，但不代表可以任意顺序合并。合并顺序由 I0 控制。

---

## 2. 所有 Agent 的共同开工协议

每个 Agent 开始前必须执行并记录：

```bash
git status --short --branch
git rev-parse HEAD
git log -1 --oneline
go test ./...
go vet ./...
```

H5 Agent 还必须执行：

```bash
npm test -- --run
npx tsc --noEmit
npm run build
```

如果基线本身失败，Agent 不得顺手修复不属于本工作包的问题；应立即向 I0 报告并暂停创建功能 Commit。

每个分支必须先新增自己的交接文件：

```text
docs/verification/<agent-id>-<package>-handoff.md
```

交接文件开头先写：

```text
contract_baseline
code_baseline
shared_contract_baseline（Round 2及以后必填）
owned_write_set
forbidden_write_set
dependencies
status = STARTED
```

完成时再补测试原始结果、Commit、未完成项和回滚方式。

### 2.1 跨 Agent 接口协作规则

多个 Agent 共同依赖的类型、RPC、稳定 ID、错误语义、不可变 Snapshot 与测试桩，必须先由 P1 形成
`SCB-1` 并由 I0 合入；“先合并接口”不等于先实现业务。`SCB-1` 可以让 Handler 暂时只有 Fake，但必须
编译通过并锁定：

```text
公共Envelope / Error / Result
Trusted/Workflow/Internal Caller Context
EffectiveIdempotencyKey / OperationRootRef
WorkAddress / SourceAuthorityRef
WorkType / ParticipantStep / ParticipantInvocationID
StableOpaqueID / CanonicalPayloadHash及黄金向量
Player/Farm/Friendship Participant IDL
CropRuleSnapshot / ActionPermitSnapshot / CareResolutionSnapshot等跨边界语义
Fake Client/Server与注册完整性测试
```

边界规则：

- P1 拥有跨进程 Wire Message；领域包不得私改 Proto；
- Actor 内存状态仍使用领域类型，禁止直接把生成 Proto 当 Authority State；
- 消费者可以在自己的包中定义最小 Port Interface 和 Fake，生产 Adapter 包装统一生成的 RPC Client；
- 禁止建立同时包含 Player、Farm、Friendship 数十个方法的 `EverythingService`；
- DP1 只修改 Player 权威，DF1 只修改 Farm 权威，SO1 只修改 Friendship 权威，SC1 只推进 Workflow；
- 最终 `main.go`、服务注册表和 composition root 只由 I0 修改。

如果实现中发现共享接口缺字段，先提交独立 ICR：问题、合同依据、字段/Hash/Receipt 影响、兼容性和测试
向量。P1 合入 `SCB-1.x` 后，生产者与消费者同时 rebase；任何业务 PR 都不得夹带未登记的公共合同修改。

```text
SCB-1
├── DP1：Player Participant Handler（含ApplyCareReward）
├── DF1：Farm Participant Handler（含CommitPlant/ResolveCareIncident）
├── SO1：Friendship Admission与Dispatch来源
├── SC1：Plant/Harvest/Theft等Coordinator与消费侧Port
└── WF1：通用Work/Order恢复与Participant调用
        ↓
I0：注册真实Server/Adapter、修改composition root、运行E2E
```

---

## 3. Agent B0：最终基线登记

### 3.1 启动条件

第 26～37 号正文被合同 Owner 确认为最终候选后立即执行。

### 3.2 唯一目标

不重写合同正文，只证明第 26～37 号已经构成唯一实现输入，关闭第 71 号 G0，并产生
可被所有 Linux worktree 引用的合同 Baseline Commit。

### 3.3 允许修改

```text
docs/design/CONTRACT-MANIFEST.md
docs/design/README.md
docs/design/71-multi-agent-implementation-plan-draft.md
docs/design/72-agent-work-package-playbook-draft.md
独立的基线复核/验证记录
经合同Owner明确授权的26～37机械修正（仅版本、数量、导航、过时措辞）
```

### 3.4 禁止修改

```text
任何 Go/TS/Proto 代码
Linux W5B 历史提交分支；已合入并删除，只允许 I0/Q0 读取既有证据，不得重建或再次合并
26～37 的产品、业务、权威、失败语义或新的协议选择
```

### 3.5 实施步骤

1. 复核每个文档标题版本、Manifest 与 README 一致；
2. 复核第 28 号将 `GET_PLAYER_BOOTSTRAP` 和 `GET_NEXT_FRIEND_OPPORTUNITY` 冻结为两条
   显式可重查命令，不存在 Push/合并查询的第二主链；
3. 将第 28 号 **33 类** Command Inventory 与第 26/32～37 号逐项对表，并确认无旧数量残留；
4. 复核 Manifest/40 已同步 26 v1.3、27 v4.2、28 v2.8/33类、29 v2.11、31 v1.4、
   32 v0.29、33 v1.2、40 v1.1、60 v1.5，并验证 35A 与 Q37-01～Q37-08 导航；
5. 复核第 32 号只使用既有 Participant/CareRewardOrder/访问确认接缝，并只引用现行下游合同；
6. 核对 31、33A、35A 的稳定语义和平台参数 Pending 边界；
7. 确认第 40 号作为现行收口导航与最新版本/33类命令一致，且没有另一份
   70 号实施计划与 71/72/73 并行授权；
8. 填写 71/72/73 的 Baseline 字段，生成一个只含治理和登记的合同基线 Commit。

### 3.6 完成产物

```text
contract_baseline_commit
版本总账
仍待平台Probe的参数清单
不允许实现Agent自行决定的清单
```

### 3.7 完成定义

- `rg` 不再发现已经关闭的编辑锁、历史待办导航或业务待决标题；平台参数 Pending 除外；
- 26～37 的互相引用能形成单向权威关系；
- Manifest 版本和正文一致；
- 33 类命令和两条首屏协议接缝可以从 26/32～37 追溯到 28 和代码工作包；
- 一个独立 Commit，可被 I0 作为所有工作树共同起点。

---

## 4. Agent I0：集成控制与写锁管理

### 4.1 启动条件

从现在开始持续存在。I0 不是普通功能 Agent。

### 4.2 唯一目标

保证多个 Agent 不从不同基线工作、不覆盖共享文件、不把独立分支误报成已集成。

### 4.3 允许修改

```text
集成分支
合并记录
docs/verification/integration-*.md
composition root（只在各模块已合并后统一接线）
go.mod/go.sum（集中处理依赖）
```

### 4.4 禁止修改

- 不在没有工作包的情况下重写业务规则；
- 不替某个功能 Agent 顺手完成剩余代码；
- 不用 `ours/theirs` 整文件覆盖冲突；
- 不把未提交脏工作树直接复制进集成分支。

### 4.5 实施步骤

1. 记录 B0 同步核对后的唯一 Contract/Backend/H5 Baseline，并登记 W5B 完成提交；
2. 建立写锁表：Owner、目录、分支、起始 Commit、预期结束时间；
3. 为 Round 1 创建独立 worktree/branch；
4. 每个 Agent 开工前检查写集不重叠；
5. 每个 Agent 完成后先审查 handoff、测试和 diff，再合并；
6. 每次合并后跑全仓测试并生成新的 integration checkpoint；
7. 后续 Agent 必须从最新 checkpoint 分叉；
8. 合入 P1 的 `SCB-1` 后填写 `SHARED_CONTRACT_BASELINE`，后续 Participant/Coordinator/Adapter
   分支只能从该 checkpoint 或更新版本分叉；
9. 若接口变更，先登记 ICR，由 P1 合入 `SCB-1.x`，再让生产者与消费者同时 rebase；
10. 各模块只导出构造器、Handler 和注册描述；I0 独占四个 Deployment 的 composition root 与最终装配；
11. 每个纵向里程碑结束生成 Evidence Bundle；
12. 维护“逻辑完成/集成完成/E2E完成/容量完成”四级状态。

### 4.6 完成定义

I0 本身没有一次性终点。每一轮结束时必须满足：

- 集成分支干净；
- 所有合入 Commit 可追溯到工作包；
- 全仓测试结果已记录；
- 没有孤立生成代码、未登记 Redis Key 或重复协议主链；
- 下一轮 Baseline 已发布。

---

## 5. S0/E0：W5B Subscription Lease 完成记录（历史卡，禁止再次派发）

> `c86ce39` 已通过 MR !6 合入 `4c97191`，后续运行证据与退役开关最终在 `a949761` 闭合。
> W5B worktree 及本地/远端功能分支已经删除。本节只保留验收来源，不得再次派发、重建分支或重复合并。

### 5.1 启动条件

完成链如下：

```text
feature_commit          = c86ce39
merge_commit            = 4c97191
dualgate_fresh_join     = f90b11b
multigate_drain         = b2f65e2
renew_200               = 22b84e9
gateboot_retirement     = dc235ef
completion_commit       = a949761d360bcbda9bbf0190c9d3c3f99bc1b60d
status                  = COMPLETED
```

### 5.2 合同输入

- 26：Subscription Lease、Fresh Join、Fence、200 人；
- 29：内部续租 RPC、DownlinkTargetFence；
- 60：接入会话验收。

### 5.3 允许修改

现有 W5B 已占用的 Gate、logicrpc、FarmSubscriptionHub、FarmIngress、相关测试和 `cmd/farm-svr`。

### 5.4 禁止修改

- 不开始 32～37 业务命令；
- 不重写 Proto 总框架；
- 不修改 Redis Authority Store；
- 不同时解决 TCONND、任务、邮件或宠物。

### 5.5 已完成步骤

1. `BatchRenewSubscriptions`、完整 Fence、单调 Deadline、不可复活状态和 Fresh Join 语义已实现；
2. 双 Gate 顶号与 Holder 重启后的 Fresh Join 已在公司 Redis 真实进程验证；
3. Drain 已通过真实 H5 浏览器验证；
4. 200/200 Join、4000 次稳态续租、P99 ≤ 5ms、零过期/零 fail-closed/零 REJOIN 已留存证据；
5. GateBoot `enabled|shadow|disabled` 三态和 disabled E2E 已验证；
6. 定向测试、race、`go test ./... -count=1`、`go vet ./...` 已通过；
7. 历史工作树和功能分支已经清理。

### 5.6 必须测试

```bash
go test ./internal/farm/subscription ./internal/farm/ingress
go test ./internal/gateway/command ./internal/gateway/downlink
go test -race ./internal/farm/subscription ./internal/farm/ingress
go test -race ./internal/gateway/command ./internal/gateway/downlink
go test ./...
go vet ./...
```

这些命令与运行证据已经执行；后续若修改 W5B 写集，必须重新运行受影响部分，不能沿用旧结论。

### 5.7 完成产物

- W5B 实现与合并提交：`c86ce39`、`4c97191`；
- 运行证据提交：`f90b11b`、`b2f65e2`、`22b84e9`、`dc235ef`；
- 完成交接提交：`a949761`；
- `docs/verification/w5b-subscription-lease-handoff.md`；
- `docs/verification/s7-handoff.md`。

### 5.8 完成定义

本卡已达到 `COMPLETED` 并解锁 P1/R1/纯领域工作包。该结论仅覆盖 Subscription Lease 的 200 人
续租与对应多进程场景；完整产品的 TCONND、广播慢连接、Soak 与 3000 万 DAU 容量仍由 Q0/Q2 证明。

---

## 6. Agent P1：Protocol 与 CommandCatalog 单写者（P1A/P1B 已完成）

状态：`COMPLETED_AT_CP3 / DO_NOT_REDISPATCH`。本卡保留为验收规格。后续
`P1B-CONSUMERS-2` 已按独立写集完成 FirstHop/Ingress、权威 Hash 与 deprecated 兼容接缝收口，
不得重建第二套 Proto/Catalog，也不得再次派发同名任务。

### 6.1 启动条件

B0 完成并登记干净 Backend/H5 Baseline；W5B 已包含在 Backend Baseline。第 28 号 33 类公开命令已机械校验。

### 6.2 合同输入

- 28 为公网协议唯一权威；
- 29 为内部 RPC 唯一权威；
- 31 为 `StableOpaqueID/WorkAddress/EffectiveIdempotencyKey` 的跨实现黄金合同；Redis Key、HashTag、
  `WorkRefV1/ReceiptRefV1` 的物理编码仍归 R1；
- 26/27 提供 Session/Route 上下文；
- 32～37 提供具体 Payload/Result 和错误语义。

### 6.3 允许修改

```text
api/proto/**
api/gen/**
internal/contracts/**
internal/identity/**
internal/gateway/commandcatalog/**
internal/protocoltest/**
协议生成脚本/黄金向量
```

### 6.4 禁止修改

```text
PlayerActor/FarmActor
Redis Repository/Lua
Workflow业务逻辑
H5页面
26～37合同
```

### 6.5 实施步骤

1. 先输出“现有 12 条命令→最终 33 条命令”迁移矩阵：保留/重命名/删除/保留编号/新增；
2. 先形成独立 `SCB-1` Commit，统一公共 Envelope/Error/Result、三类可信调用上下文、
   `EffectiveIdempotencyKey/OperationRootRef`、`WorkAddress/SourceAuthorityRef`、
   `WorkType/ParticipantStep/ParticipantInvocationID`、`StableOpaqueID/CanonicalPayloadHash`；
3. 在 `SCB-1` 中一次登记第 29/32～37 号已冻结的 Player/Farm/Friendship Participant IDL、Typed
   Step Payload/Result，以及 `CropRuleSnapshot/ActionPermitSnapshot/CareResolutionSnapshot` 等跨边界
   不可变语义；不实现 Participant 的业务裁决；
4. 为 `SCB-1` 提供生成 Client/Server、Fake Adapter、注册完整性测试、稳定 ID/Hash 黄金向量；
   由 I0 单独合入并发布 `SHARED_CONTRACT_BASELINE`；
5. 从 28 生成 33 类 Command Inventory：命令、Payload、Result、RouteDomain、Root、Admission、幂等、错误；
   必须包含 `GET_PLAYER_BOOTSTRAP` 与 `GET_NEXT_FRIEND_OPPORTUNITY`，不允许实现自行合并或用 Push 替代。
6. 显式处理 `BUY_OFFER→BUY`、`SELL_ITEM→SELL`、`PLANT_CROP→PLANT`、
   `HARVEST_CROP→HARVEST`、`WATER_CROP→CARE`、`BEGIN_THEFT→THEFT`；
   `LEAVE_FARM/GET_FARM_SNAPSHOT` 不再作为最终公开业务命令，旧编号必须 reserved 或仅留在离线迁移测试。
7. 从公共 Envelope 移除旧 `target_owner_id/expected_*`权威语义，这些字段进入各 Typed Payload；
   被废弃的 field/enum number 标为 reserved，不复用。
8. 保持 FarmFrame 顶层 oneof，业务命令继续 `command_type + bytes payload`；
9. 为 32～37 定义 Typed Payload/Result；
10. 为每个命令注册 CommandSpec；
11. 实现 Decoder、Canonical Payload Hash 和 MaxPayloadBytes；
12. 注册 SessionAdmission、RouteKeyExtractor、AcceptBoundary、RetryDirective、Timeout、RateLimit；
13. 补 CommandResult 与 ServerPush 分离测试；
14. 补未知命令、错误 payload、重复 field、reserved 号码、新 H5 与新后端原子升级测试；
    无已发布存量客户端时，不建立旧 12 条命令的运行时双主链。
15. 用仓库统一脚本生成 Go 代码，禁止手改生成文件；
16. 输出 H5 需要消费的协议变更清单和黄金向量。H5 向量只包含公网 Wire、
    字符串 ID、公开版本和必要的 Payload Hash；不包含 `VirtualShard`、`ActorAddress`、
    `WorkRef`、Redis Key 或 Hash Tag。

### 6.6 必须测试

```bash
go test ./internal/gateway/commandcatalog ./internal/protocoltest
go test ./internal/contracts/... ./internal/identity/...
go test ./api/...
go test ./...
go vet ./...
git diff --check
```

### 6.7 完成产物

- `SCB-1` 共享合同 Commit 与 `SHARED_CONTRACT_BASELINE`；
- 接缝注册表、Fake Client/Server、稳定 ID/Hash 黄金向量；
- 命令完整性矩阵；
- 唯一 Proto/生成代码 Commit；
- Go 黄金向量；
- H5 消费说明；
- 旧命令迁移/Reserved 说明。

### 6.8 完成定义

- 每个公开命令恰好一个 CommandSpec；
- 命令数精确为 33，且每一条都能追溯到 26/32～37 的唯一使用点；
- 没有未注册 oneof/command_type；
- 同 operation_id + 不同 payload 能稳定判冲突；
- 旧命令不会与新命令形成双主链；
- 每个 Participant/WorkType/ParticipantStep 恰好一个注册项，跨边界 Snapshot 有唯一 Wire Schema；
- 相同稳定身份输入在所有黄金样例中产生相同 ID/Hash；
- 代码只定义协议，不偷偷实现业务裁决。

---

## 7. Agent R1：Redis Authority 与 Repository（基础 Adapter 已完成）

状态：`COMPONENT_COMPLETED_AT_CP2A / PLATFORM_AND_RUNTIME_PENDING / DO_NOT_REDISPATCH`。
已完成代码与 unit/race 证据不等于 `REDIS_INTEGRATION_PASS`；公司 Redis Probe、CompactReceipt/GC
和 AR1 主链接线应作为后续独立任务，不得重跑整张 R1 卡。

### 7.1 启动条件

B0 与干净代码 Baseline 已登记。可在 SCB-1 后与 P1B、纯领域包并行；涉及稳定身份、WorkAddress 和操作 Digest 的实现必须消费
`SCB-1`，不得在 Redis 包内再定义第二套算法。

### 7.2 合同输入

- 30：逻辑 CAS、Writer Fence、Unknown、Persistence Lane；
- 31：Key/HashTag/Lua/Work/Projection/容量；
- 33A、35A：后续非 Actor Authority 的物理扩展要求。

### 7.3 允许修改

```text
internal/storage/redis/**
internal/storage/authoritystore/** 中Redis Adapter新文件
cmd/redis-production-probe/**
cmd/redis-ha-probe/**
Redis脚本与黄金测试
```

### 7.4 禁止修改

```text
Actor业务状态转换
CommandCatalog/Proto
internal/contracts/**、internal/identity/**（只消费SCB-1）
Friend/Mail具体业务Repository（只提供公共原语）
cmd/player-svr/main.go
cmd/farm-svr/main.go
```

### 7.5 实施步骤

1. 先审计现有 `authoritystore/blobstore/rediskey/ownerstore` 与第 30/31 号的差异，标出复用、
   迁移、隔离和最终删除项；新主链不再把旧 OwnerCore/Tcaplus Store 当成权威。
2. 将现有 KeyBuilder 与 BucketResolver 对齐 31；稳定身份与 Canonical Hash 调用 `internal/identity`
   的 `SCB-1` 实现，Redis 侧只拥有 `WorkRefV1/ReceiptRefV1`、HashTag、Key 和 Slot 的物理编码；
3. 写 Slot Golden，证明同 Authority 原子组同槽、不同域独立；
4. 实现 ScriptRegistry、SHA 缓存和 NOSCRIPT 重载；
5. 依次实现小脚本：Shard Claim/Renew、LoadAndClaim、Commit、CommitWithOrders、FenceAndClaim；
6. 实现 Work Lease、Receipt、DueIndex 和 ResultDirectory 原语；
7. 每个脚本先定义 Typed 输入/输出，再写 Lua；
8. Repository 将 Redis 错误映射为稳定 StoreStatus；
9. 实现 TIMEOUT_UNKNOWN Resolver，普通 GET 不能冒充解析线性化点；
10. 实现旧 Writer 迟到写拒绝和 Epoch Claim；
11. 增加 item/byte 上限，拒绝必须先于部分写入；
12. 做 EVAL/EVALSHA/NOSCRIPT、断连、超时、脚本已执行但响应丢失测试；
13. 在公司 Redis 执行 Probe，记录 persistence/ACL/failover 未知项。

Redis Slot、Key、Hash Tag、WorkRef 和 ResultDirectory 是后端内部物理合同。它们的 Golden
由 Go/Lua 合同测试消费，不得生成为 H5 TypeScript 常量。

### 7.6 必须测试

```bash
go test ./internal/storage/redis/... ./internal/storage/authoritystore/...
go test -race ./internal/storage/redis/... ./internal/storage/authoritystore/...
go test ./...
go vet ./...
```

真实 Probe 必须保留原始日志，不能将 InMemory/Fake 结果写成平台能力。

### 7.7 完成产物

- Script 输入输出表；
- Repository 接口/实现；
- Unknown 状态机和故障注入证据；
- Redis 平台 Profile；
- RPO/RTO/ACL/Failover 待确认清单。

### 7.8 完成定义

逻辑与真实 Redis Probe 通过后标记 `REDIS_INTEGRATION_PASS`；没有持久化/Failover 证据时不得标记
`PRODUCTION_DURABILITY_PASS`。

---

## 8. Agent DP1：Player 领域与 PlayerActor 单写者

### 8.1 启动条件

纯领域阶段可在 Round 1 启动；修改 PlayerActor 必须等待 `SCB-1`、R1、AR1 接口合入，并从
`SHARED_CONTRACT_BASELINE` 或更新 checkpoint rebase。

### 8.2 合同输入

- 27：PlayerActor 边界；
- 30/31：Player Authority Commit；
- 32：Buy/Sell/Reservation/Harvest到账；
- 34：TaskDomain；
- 35：MailClaim；
- 36：PetState；
- 37：CompendiumState。

### 8.3 允许修改

纯领域阶段：

```text
internal/player/economy/**
internal/player/task/**
internal/player/pet/**
internal/player/compendium/**
internal/player/mail/**
对应测试
```

集成阶段独占：

```text
internal/player/actor/**
internal/player/ingress/**
```

### 8.4 禁止修改

```text
api/proto/**
internal/farm/actor/**
internal/workflow通用框架
Redis Key/Lua
cmd/player-svr/main.go（由I0最终接线）
```

### 8.5 纯领域实施步骤

1. 定义 PlayerState 的有界子域，不把 Proto 当状态；
2. Economy：批量 Buy/Sell、checked arithmetic、配置版本、库存堆叠；
3. Reservation：Reserve/Finalize/Release 的确定性 ID 和 Receipt；
4. Task：period_id、惰性换期、3 个每日任务、线性成长任务、Claim 防重；
5. Pet：购买/任务解锁、喂食 8h、24h 上限、切换、版本/Hash；
6. Compendium：首次自种自收获解锁、活动档案、revision/seen_revision；
7. MailClaim：可信附件材料、mail_id 二次防重、RewardBundle；
8. 所有写命令返回 CandidateState + ImmutableOutcome，不直接写 Redis；
9. 实现 Economy/Task/Pet/Compendium 各自的强类型版本化配置和校验器，仅消费不可变
   `ConfigRegistry` 快照，Actor Turn 内不回源；
10. 为重复 operation、规则版本变化、溢出、并发顺序写表驱动测试；
11. 证明每个集合有容量上限，不在 ActorRecord 中形成无界历史。

### 8.6 PlayerActor 集成步骤

1. 把领域 transition 接入单 Mailbox；
2. 每条命令先查 Root-scoped idempotency；
3. 构造 CandidateState、Receipt、Outcome 和 0..N Orders；
4. 通过 Persistence Lane 一次 Authority Commit；
5. Unknown 返回 PROCESSING，不自行重算；
6. Harvest Participant 一次提交 Inventory/Exp/Task/Compendium；
7. PetFeed/Switch 一次提交 PetState/Receipt/GuardSyncOrder；
8. MailClaim/TaskClaim 一次提交资产+Receipt+Outcome；
9. 增加 PlayerActor Mailbox 顺序与重复恢复测试；
10. 实现 `ReserveSeed/CommitSeedReservation/ReleaseSeedReservation` 与 `ApplyCareReward` 等第 29 号
    Player Participant；相同 Invocation + 相同 Payload 返回原 Receipt，不同 Payload 返回冲突；
11. `ApplyCareReward` 只消费 Farm 提交时冻结的 `CareResolutionSnapshot`：OWNER 只发基础奖励，
    FRIEND 才推进好友互动事实；不得重新查询 FriendRelation 或当前奖励配置；
12. 只暴露 Participant/Ingress Handler 与模块构造器，不调用 FarmActor RPC，不修改 composition root。

### 8.7 必须测试

```bash
go test ./internal/player/...
go test -race ./internal/player/actor ./internal/player/ingress
go test ./...
go vet ./...
```

Player Participant 还必须有合同测试：相同 Invocation + 相同 Payload 返回原结果；相同 Invocation + 不同
Payload 返回 `PARTICIPANT_IDEMPOTENCY_CONFLICT`；`ApplyCareReward` 响应丢失后重试不重复增加经验/任务；
OWNER Care 不推进好友互动，FRIEND Care 最多推进一次。

### 8.8 完成产物

- PlayerState 记录大小预算；
- 每个子域状态转换测试；
- Player Participant API；
- PlayerActor 集成 Commit；
- Buy/Sell、Harvest到账、Task/Pet/Compendium/MailClaim 故障矩阵。

### 8.9 完成定义

领域包可以先标记 `DOMAIN_LOGIC_PASS`。只有 Redis Authority Commit、Actor race 和纵向 E2E 完成后，相关
功能才能升级为 `INTEGRATION_PASS`。

---

## 9. Agent DF1：Farm 领域与 FarmActor 单写者

### 9.1 启动条件

纯领域阶段可在 Round 1 启动；FarmActor 修改必须等待当前 Backend Baseline 与 `SCB-1` 合入，并等待 R1/AR1 接口稳定，
再从 `SHARED_CONTRACT_BASELINE` 或更新 checkpoint rebase。

### 9.2 合同输入

- 26：Subscription/FarmChange；
- 27：FarmActor 边界；
- 30/31：Farm Authority Commit/Shared Snapshot；
- 32：Unlock/Plant/Mature/Harvest/Care/Theft；
- 36：GuardSnapshot。

### 9.3 允许修改

纯领域阶段建议：

```text
internal/farm/domain/**
internal/cropcatalog/** 中明确归属的规则文件
对应测试
```

集成阶段独占：

```text
internal/farm/actor/**
internal/farm/ingress/**
```

### 9.4 禁止修改

```text
internal/farm/subscription/**（W5B已冻结，只通过接口调用）
api/proto/**
PlayerActor
Redis Key/Lua
通用 Workflow Runtime
cmd/farm-svr/main.go
```

### 9.5 纯领域实施步骤

1. 定义最多 24 Plot、初始 5 解锁和 Unlock 邻接/等级/价格规则；
2. CropCycle 固定 plant_at/mature_at/rule snapshot/base_yield；
3. Mature 由 trusted_now_ms 惰性派生，不写定时 Mature 状态；
4. Harvest 冻结 owner claimable 和本次目标结果；
5. CareIncident：浇水/虫害独立、首个成功、后续 NO_CHANGE；
6. Theft：每好友每茬一次、总 20%、单次 2%、最多 10 槽；
7. GuardDecision：使用当前 GuardSnapshot 固定随机输入，历史不重抽；
8. FarmOpportunityHeader 与权威状态同 CandidateState 计算；
9. 实现 Shop/Crop/Care/Theft 所需强类型版本化配置、Canonical Hash、快照冻结和发布校验；
   无效配置 Fail Closed，不修改已创建 CropCycle/Decision。
10. FarmChange/PlotDelta 从已提交 CandidateState 生成，不保存无界变化历史；
11. 对 Harvest/Theft、Care/Harvest、GuardSync/Theft、不同 Plot 并发写确定性测试。

### 9.6 FarmActor 集成步骤

1. 将 Farm 命令串行投递到单 FarmActor Mailbox；
2. 绑定 crop_cycle_id/incident_id/theft_attempt_id，拒绝旧视图误操作下一茬；
3. 一次 Authority Commit 写 FarmRecord、Receipt、Outcome、FarmVersion、必要 Order；
4. Commit 成功后通过 PostCommitDeliveryGuard 产生 FarmChange；
5. Snapshot/VersionRing 只暴露已提交版本；
6. ApplyGuardSnapshot 只接受更高 guard_version；
7. Friend Care/Theft 只消费已提交 ActionAdmission；
8. Actor Turn 不等待 PlayerActor、Gate 或 Dispatcher；
9. 增加 passivation/重载后结果一致测试；
10. 实现 `CommitPlant/ResolveCareIncident` 等第 29 号 Farm Participant；Wire DTO 进入 Mailbox 前转换为
    领域输入，Authority State 不直接保存生成 Proto；
11. `ResolveCareIncident` 首次成功时冻结 `CareResolutionSnapshot`，与 Incident、Receipt、
    `CareRewardOrder` 同一次来源 Authority Commit；不得同步等待或直接修改 PlayerActor；
12. 只导出 Farm Participant Handler、领域模块构造器和注册描述，不修改 composition root；
13. 验证 200 人广播只生成一次业务 Delta。

在 SC1 接入 Plant/Harvest 前，DF1 必须先用一个测试命令或最小 Farm 变更证明：

```text
Authority Commit(V)
→ PostCommitDeliveryGuard
→ FarmSubscriptionHub
→ FARM_CHANGE(V)
→ H5/Probe 按版本应用
```

这是 Plant/Harvest 的正确性前置。共享 VersionRing、200 人批量下行和热点优化可以在后续
里程碑加强，但不能用 CommandResult 代替这条 FarmChange 交付链。

### 9.7 必须测试

```bash
go test ./internal/farm/...
go test -race ./internal/farm/actor ./internal/farm/ingress ./internal/farm/subscription
go test ./...
go vet ./...
```

Farm Participant 还必须验证：相同 Invocation 重试返回原 Receipt；`CommitPlant` 使用 Workflow 冻结的
CropRuleSnapshot/ID/Hash；多人同时护理同一 Incident 只提交一个 `CareResolutionSnapshot +
CareRewardOrder`，其余稳定返回 `NO_CHANGE/ALREADY_RESOLVED`。

### 9.8 完成产物

- FarmRecord 大小预算；
- Farm 转换表和冲突矩阵；
- Farm Participant API；
- FarmActor 集成 Commit；
- FarmChange/Snapshot/Version 测试向量。

---

## 10. Agent AR1：Actor Runtime 与 Persistence Lane

### 10.1 启动条件

R1 Repository 接口已经合入；W5B 已在当前 Baseline；P1 不再修改 Actor Runtime 接口。

### 10.2 允许修改

```text
internal/logic/actor/**
internal/actorrouting/** 中Runtime接缝
Actor Registry/Activation公共代码
Runtime专用测试
```

### 10.3 禁止修改

```text
Player/Farm具体业务规则
Proto/CommandCatalog
Workflow场景
Redis Lua
cmd/*/main.go
```

### 10.4 实施步骤

1. 先对现有 `activation/persistence/runtime`、Player Registry 和 Farm Registry 建立第 27/30 号条款差异矩阵；
   在原包上补齐，不新建第二套 Runtime。
2. 接入 LoadAndClaim 和 ShardWriteFence；
3. 验证/补齐 Singleflight 合并同 Actor 并发激活；
4. 建立有界 Mailbox 和 watermark；
5. 过期 Lease/WriterClaim 立即停止新 Turn；
6. 实现每 Actor 单 Persistence Lane；
7. 首轮只开启 `CRASH_DURABLE`/`WORKFLOW_DURABLE`，权威提交成功后才返回成功；
8. 保留 Persistence Mode 和 Barrier/ForceFlush 的类型边界，但 `ACTOR_SOFT` 开关保持关闭；
   本工作包不以“标记脏后延迟落库”替代首轮权威提交。
9. Drain/Passivation 等待 in-flight 结果明确；
10. Store Timeout Unknown 交给 Resolver，Actor 不盲重试；
11. Actor 重载恢复 Receipt/Outcome/Order；
12. 增加激活风暴、Mailbox 满、失权、迁移、崩溃测试；
13. 输出每 Actor 内存与激活速率指标。

### 10.5 完成定义

- Fake 与真实 Redis Adapter 均通过；
- race 无数据竞争；
- 旧 Holder 不能晚写；
- Passivation 后可从 AuthorityRecord 恢复；
- 以测试证明 `ACTOR_SOFT` 未开启时不存在未落库即返回资产成功的路径；
- Actor Runtime 不包含任何具体农场数值规则。

---

## 11. Agent WF1：Workflow/Order/Dispatcher/Worker

### 11.1 启动条件

R1、SCB-1 与 29 的 Work/Participant IDL 已合入。WF1 的通用框架阶段可与 AR1 并行，但只能消费
冻结 Port/Fake；Participant 生产 Adapter 和 composition root 必须等待 AR1 接缝稳定后由 I0 收口。

### 11.2 允许修改

```text
internal/workflow/**
internal/worker/**
通用Dispatcher/Lease/Retry测试
```

### 11.3 禁止修改

```text
PlayerActor/FarmActor
Proto
Redis公共KeyBuilder/Lua
业务数值规则
cmd/player-svr/main.go
cmd/farm-svr/main.go
cmd/farm-worker/**（由I0最终装配）
```

### 11.4 实施步骤

1. 先审计现有 `internal/workflow` 对 CreateIfAbsent、Lease、Receipt、Order、Unknown 的覆盖，
   在原包上收敛，不新建 `workflow2`或第二套 worker 主链；
2. 实现/补齐 CreateIfAbsent 与稳定 workflow_id；
3. 实现状态机版本/CAS 和 Step Receipt；
4. 实现 Work Lease、续租、超时和失租自 Fence；
5. 实现来源同槽 Order + DueIndex 发现；
6. DirectDispatcher 使用有界内存队列快速尝试；
7. 队列满或调用失败不丢 Order，交给 worker；
8. worker 按 Bucket 分片扫描、Claim、调用、Ack/Retry；
9. Retry 使用原 operation/work/invocation id；
10. 实现 SUPERSEDED/COMPLETED/REJECTED 等终态；
11. 使用 TestWorkflow 验证进程崩溃、重复投递、Participant Unknown；
12. 输出队列深度、due lag、retry 次数、dead-letter-like 终态指标；
13. 不在本包宣称 Plant/Harvest/Theft 已完成。

### 11.5 完成定义

通用框架在任意一步崩溃后都能从 Redis Work/Order/Receipt 恢复，且重复恢复不重复发放资产或重复修改 Farm。

---

## 12. Agent SO1：Friendship Authority、Join 与 ActionAdmission

### 12.1 启动条件

第一阶段 Repository 可在 R1 公共原语合入后开始；Join/Action 集成等待 W5B Baseline、`SCB-1`、WF1，并从
`SHARED_CONTRACT_BASELINE` 或更新 checkpoint rebase。

### 12.2 允许修改

```text
internal/friend/**
好友Authority专用Repository/Lua调用层
好友独立测试
```

集成 Join 时，只通过评审后的接口向 Gate/Hub 提交小型接缝；共享 Gate/Hub 文件由 I0 指定单写者处理。

### 12.3 实施步骤

1. 实现 canonical pair_id 和 Relation 状态；
2. 实现双方 300 好友上限和 Quota Reservation；
3. 实现 ShareIssuer generation、兑换防重和重放；
4. 实现 Delete/Block/Unblock/ReactivationBarrier；
5. 删除/拉黑先撤销未来 Permit，再可靠清理在途 Join；
6. 实现 VisitPermit 与完整 Join Fence；
7. 实现 ActionAdmission Receipt + ActionPermit + DispatchOrder；
8. Admission 先提交则后续必须恢复，删除先提交则新操作拒绝；
9. FriendIndex 只做分页查询，不参与权威授权；
10. 实现关系查询、分享、删除、并发兑换和 quota 故障测试；
11. 只通过 `SCB-1` 的 Friendship/Farm Participant Contract 与 DF1 接通 Friend Care/Theft 准入；
    `FriendActionDispatchOrder` 保存完整不可变 `ActionPermitSnapshot`，SO1 不读取或修改 FarmActor 内部状态；
12. 与 H1 接通好友列表和分享 URL 清理。

### 12.4 完成定义

- 好友不会自然过期；
- 删除/拉黑立即阻止新 Permit；
- 旧 epoch 的清理不能删除重加后的新关系；
- 300 上限在并发下精确；
- ActionAdmission 不依赖延迟投影；
- Join 名额和权限失败都能正确释放。

---

## 13. Agent MA0：PlayerMail EntryStore（已完成，禁止重复派发）

状态：`COMPLETED_VERIFIED_AT_CP2 / DO_NOT_REDISPATCH`。

```text
base: cp2a-20260804
candidate: f4e543eb3f7d2ac0b5fd1576894e6f972a618921
integration_merge: 5e31c2dea65136fa3c0486c6d77765dd5815588e
checkpoint: cp2-20260804
handoff: docs/verification/ma0-player-mail-handoff.md
icr: docs/verification/ma0-player-mail-icr.md
```

### 13.1 启动条件

历史启动条件已经满足并执行完成。35A 仍是 `STABLE_INPUT / PLATFORM_PARAMETERS_PENDING`；未完成容量、
Receipt 清理与 Failover Probe 不否定 MA0 组件完成，但阻塞生产参数与生产耐久性冻结。

### 13.2 允许修改

```text
新增的PlayerMail Authority/Repository包
邮件分页/已读/删除/投递/GC测试
```

### 13.3 禁止修改

```text
PlayerActor资产领取
Proto
公共Redis KeyBuilder/Lua注册表（通过R1接口扩展）
独立mail-svr/MailActor
```

### 13.4 实施步骤

1. 实现每玩家 MailEntry 分桶和六封分页索引；
2. 实现 InsertMailIfAbsent；
3. 实现详情、MarkRead、Delete；
4. 默认最多 60 封、30 天、未领取附件保护；
5. 实现 ExpireVisibility 与 GC；
6. 实现投递 Receipt 和 Unknown 解析；
7. 生成 TrustedMailClaimMaterial，只能由受信模块读取；
8. 实现 ApplyClaimProjection，投影不替代 PlayerActor Claim Receipt；
9. 补分页游标稳定、重复投递、过期、删除、GC、Unknown 测试；
10. 输出容量和热 Key 估算。

### 13.5 完成定义

邮件列表路径可独立工作，附件资产仍只由 DP1 PlayerActor 最终领取；没有新增邮件服务或邮件 MQ。
两轮审查与 integration 复验无 P0/P1，完成定义已满足。后续只允许通过新任务关闭：Receipt 有界清理、
DueRef 重登记崩溃窗口、真实 Redis Cluster/ACL/Failover/容量参数，以及已登记的 P2 Fake/硬化差异。

---

## 14. Agent H1：H5 Wire、状态机与页面闭环

### 14.1 启动条件

H5 Core 只能在 P1 协议 Commit 合入后开始。具体页面等待对应后端纵向链 E2E 稳定。

### 14.2 允许修改

```text
classic-farm-h5 中协议codec、WebSocket client、Operation Journal、全局Store
对应领域reducer、页面组件和测试
```

### 14.3 禁止修改

```text
后端Proto/Go生成代码
客户端自行补算金币、产量、成熟或宠物随机
通过前端状态替代后端Receipt
同时保留旧新两套命令主链
```

### 14.4 实施步骤

1. 消费 P1 黄金向量并生成/维护 TS 类型；
   只消费公网 Wire 和公开 ID/版本，不实现 `VirtualShard`、`ActorAddress`、`WorkRef`、
   Redis Key 或 Hash Tag。
2. FarmFrame 顶层严格区分 Result/Push/Control；
3. Operation Journal 保存原 operation_id、typed payload、hash 和 admission 边界；
4. 只自动重试可证明未 Accepted 的请求；
5. PROCESSING 轮询原结果，禁止换 operation_id；
6. 保持 Authenticated/Joining/Recovering/InFarm 状态机；
7. Snapshot/Delta/Ready 按 subscription/version 串行应用；
8. 旧 Farm Push 丢弃，旧命令 Result 只更新 operation 状态；
9. ServerHello 后并行发起 `GET_PLAYER_BOOTSTRAP`、JoinOwnFarm 和弱依赖加载；只有 Snapshot/Ready
   与私有恢复屏障都满足后才进入可写 InFarm；
10. 首屏好友机会与返回自己农场后的下一条导航通过 `GET_NEXT_FRIEND_OPPORTUNITY`；
    好友加载失败显示 Loading/Retry，不退回登录页；
11. 分阶段接 Buy/Sell/Plant/Harvest、好友/Care/Theft、Task/Mail/Pet/Compendium；
12. 机会弹窗进入一个农场即消费本轮候选，回来查询下一条；
13. 弱网、断线、Fresh Join、QueueFull、Timeout Unknown 做页面测试。

### 14.5 完成定义

每条后端纵向链都有真实 Web 页面 E2E；前端不持有业务权威，不因重复点击造成重复资产变化。

---

## 15. Agent SC1：跨 Actor 场景 Coordinator

### 15.1 启动条件

`SCB-1`、DP1/DF1 Participant Handler、WF1 Runtime 已合入；DF1 的最小 FarmChange 交付链已通过。

### 15.2 允许修改

```text
明确的新Coordinator/Scenario包
每个Workflow状态机与测试
消费侧小Port Interface、Fake与生产gRPC Adapter
```

### 15.3 禁止修改

```text
PlayerActor/FarmActor内部状态
通用Workflow Runtime
Proto
Redis KeyBuilder/Lua
Gate多Participant编排
PlayerActor/FarmActor的具体实现包依赖
```

### 15.4 场景实施顺序

#### V1 · UnlockPlot

1. CreateIfAbsent Workflow；
2. Player ReserveCoin；
3. Farm CommitUnlock；
4. Player Finalize 或在明确未提交时 Release；
5. Unknown 只查 Receipt；
6. 完成状态持久化。

#### V2 · Plant

1. 创建 Workflow，并冻结 `CropRuleSnapshot`、稳定 ID 与 Participant Step Hash；
2. Player ReserveSeed；
3. Farm 创建 CropCycle；
4. Player Finalize Reservation；
5. FarmChange 在 Farm Commit 后产生；
6. 任一步崩溃可恢复。

#### V3 · Harvest

1. Farm 冻结并提交 HarvestTargetDecision；
2. Player 原子到账 Inventory/Exp/Task/Compendium；
3. Workflow 完成；
4. Farm 已提交后不做业务回滚，只恢复 Player 到账。

#### V4 · Theft

1. ActionAdmission 已提交；
2. Farm 冻结 gross/intercept/net/owner compensation；
3. Visitor PlayerActor 入库 net 作物；
4. Owner PlayerActor 增加补偿金币；
5. ResultDirectory 发布给访问者；
6. 任一 Participant 重复调用命中 Receipt。

### 15.5 完成定义

每个 Workflow 都必须有步骤表、状态转换、Participant Receipt、故障注入矩阵和恢复测试；Coordinator 退出
不会使流程永久卡住，Gate 不参与 Participant 推进。

---

## 16. Agent Q1：验证、故障注入与容量

### 16.1 启动条件

Round 0 可准备工具；每个功能合入后持续追加黑盒场景。

Round 0 的“只读基线审计”不是重做一遍架构评审，而是先保存日后可对比的工程证据：

```text
当前分支/Commit/dirty状态
全仓测试与耗时
当前12条旧CommandCatalog清单
现有Actor/Workflow/Redis包和测试覆盖
可控Clock/RPC/Redis/崩溃注入器是否可复用
W5B已完成项及完整产品仍待实测项
```

这个记录只用于判断“之后的代码究竟推进了什么”，不修改业务代码，也不会阻塞 B0。

### 16.2 允许修改

```text
internal/testkit/**
loadtest/**
cmd/*-probe、cmd/*-e2e、cmd/ws-stress 中明确测试工具
docs/verification/**
```

### 16.3 禁止修改

- 不修改业务实现使测试通过；
- 不在测试里复制一套业务裁决；
- 不使用 Mock 结果宣称 Redis/双 Gate/容量已经通过。

### 16.4 实施步骤

1. 建立合同条款→测试用例追踪矩阵；
   Round 0 先增加“第 28 号公开命令库存↔第 26/32～37 号使用点”的只读检查，
   明确报告两条首屏接缝，但不替合同 Owner 修改语义；
2. 为 Clock、RPC、Redis 和 Actor 生命周期建立可控故障点；
3. 每个纵向链覆盖 Before Accept、After Accept、Before Commit、Commit Unknown、Response Lost；
4. 覆盖重复 operation、不同 payload、旧 Fence、旧 subscription；
5. 覆盖 Actor/Holder 崩溃和 Shard Drain；
6. 覆盖慢客户端、Gate 队列满、Ready Gap、Fresh Join；
7. 构造 200 人同场，多 Gate 分布和不同 Plot 操作；
8. 验证一个 FarmChange 只计算一次、按 Gate 批量分发；
9. 记录 Actor Mailbox、Redis P99、Workflow due lag、下行队列；
10. 从登录频次、在线时长和好友上限推导 PCU/QPS；
11. 设计 3M PCU 与 5M 压力 PCU 的分层模型，不把全局 QPS 塞给单 Pod；
12. 验证每个 CommandType、WorkType、ParticipantStep 只有一个注册项，稳定 ID/Hash 黄金向量无漂移；
13. 增加依赖方向检查：Care/Friendship/Coordinator 不得导入 PlayerActor 内部包，Plant/Coordinator 不得
    导入 FarmActor 内部状态，业务包不得手拼 Redis Key 或绕过 `SCB-1`；
14. 生成每个里程碑 Evidence Bundle。

### 16.5 完成定义

测试结论必须标注证据等级：

```text
UNIT_PASS
RACE_PASS
INTEGRATION_PASS
REAL_REDIS_PASS
MULTI_PROCESS_E2E_PASS
CAPACITY_PASS
```

低等级证据不能冒充高等级。

---

## 17. Agent O1：日志、监控、Trace 与告警

### 17.1 启动条件

核心接口稳定后开始，避免在大量重构期同时修改所有调用点。

### 17.2 允许修改

```text
internal/observability/**
internal/grpcmetrics/**
deploy/monitoring/**
dashboard/alert配置
少量经I0批准的统一埋点接缝
```

### 17.3 实施步骤

1. 统一 Trace 字段：request_id、operation_id、workflow_id、actor address、shard、writer epoch；
2. 禁止日志输出密码、Ticket、Cookie、附件秘密材料；
3. Gate 指标：连接、队列、帧拒绝、续租、下行失败；
4. Actor 指标：激活、Mailbox、Turn、Passivation、失权；
5. Redis 指标：Lua 延迟、Unknown、NOSCRIPT、容量拒绝；
6. Workflow 指标：Due lag、Lease、Retry、Pending 年龄；
7. Farm 热点指标：在线人数、广播扇出、VersionRing 覆盖、Fresh Join；
8. 领域指标：Buy/Plant/Harvest/Care/Theft 结果码，不记录敏感 payload；
9. 设置告警：Lease 大量丢失、Unknown 上升、Due lag、队列水位、Actor 激活风暴；
10. 用故障注入验证告警确实触发并可定位。

### 17.4 完成定义

一次跨 Actor Plant/Theft 能从 Gate Trace 到 Coordinator、Participant、Redis Commit 和 Result；发生故障时
指标能区分路由、Actor、Redis、Workflow 和下行问题。

---

## 18. Agent PL1：Linux、Docker 与部署组装

### 18.1 启动条件

四个 Deployment 的 composition root 和环境变量已稳定，至少 M1/M2 已通过单机多进程 E2E。

### 18.2 允许修改

```text
Dockerfile*
deploy/**
scripts/ 中的启动、健康检查、部署与验收脚本
必要的 cmd/* 启动参数接缝（由I0审批）
docs/verification/platform-*.md
```

### 18.3 禁止修改

```text
Actor/领域规则
Proto/CommandCatalog
Redis Lua/Key 语义
Workflow 状态机
在仓库写入真实密码、Cookie、Ticket、Redis凭据或证书私钥
```

### 18.4 实施步骤

1. 为 `farm-gate/player-svr/farm-svr/farm-worker` 产生四个可独立启动的 Linux 镜像/入口；
2. 建立本地/测试 Docker Compose，可以同时启动四进程、测试 Redis 和 H5 静态资源；
3. 公司 Redis、配置中心、TLS 证书和密钥通过环境/挂载注入，不烧入镜像；
4. 实现 liveness/readiness：进程存活不等于已加载有效配置、RouteSnapshot 与必需依赖；
5. 验证优雅终止：Gate 停止新连接/新命令，Player/Farm Holder Drain，worker 停止新 Claim 并完成已取任务；
6. 先交付单机多进程和多 Gate E2E，再增加 K8s Deployment/Service/Config/Secret 引用；
7. CPU/内存 request/limit、副本数和 HPA 阈值使用 Q1 实测结果，不从 3000 万 DAU 直接猜每 Pod 参数；
8. 执行滚动更新、Redis 短故障、单 Pod 退出和配置失效演练，保留原始日志。

### 18.5 完成定义

从空 Linux 主机可以按说明启动四个 Deployment，跑通真实 WSS 纵向链，某个进程退出不造成
凭据泄漏或静默数据丢失。只有真实多进程/多 Pod 证据完成后才能标记相应 E2E/容量等级。

---

## 19. 纵向里程碑中各 Agent 如何配合

### M1 · Buy/Sell

```text
P1  已提供命令/结果
DP1 实现领域与PlayerActor
R1  提供Authority Commit
AR1 提供Runtime
H1  接页面
Q1  做重复/Unknown/批量/溢出验收
I0  最终接线与合并
```

完成标准：真实 H5 批量买卖，重复请求不重复扣/加资产，崩溃后结果可恢复。

### M2 · Unlock/Plant

```text
DP1 实现Coin/Seed Participant
DF1 实现Plot/Crop Participant
SC1 实现Workflow
WF1 提供恢复框架
H1  接页面
Q1  穿透每个故障点
I0  修改composition root
```

完成标准：任一步崩溃后最终要么未扣资源且未种植，要么资源已结算且 CropCycle 唯一存在。

### M3 · Harvest/Task/Compendium

```text
DF1 冻结HarvestTargetDecision
DP1 一次提交Inventory/Exp/Task/Compendium
SC1 推进Workflow
H1  展示收获和图鉴新解锁
Q1  验证重放/升级/首次解锁
```

完成标准：不会出现作物已收走但奖励永久丢失，也不会重复到账或重复解锁。

### M4 · Friendship Join/Care

```text
SO1 Relation/Permit/Admission
W5B冻结接口 AUTHORIZING Join接缝
SO1 FriendActionDispatchOrder → DF1 ResolveCareIncident
DF1 Care裁决/FarmChange + 冻结CareResolutionSnapshot/CareRewardOrder
WF1 投递Order → DP1 ApplyCareReward/Task互动
H1 好友列表/进入/返回下一机会
Q1 删除好友并发、200人广播
```

Care 不新增独立 `CareWorkflow`：Owner Care 直接进入 FarmActor；Friend Care 由 Pair Authority 的
`FriendActionDispatchOrder` 可靠送达 Farm；Farm 成功提交后再由 `CareRewardOrder` 可靠应用 Player 奖励。

### M5 · Pet/Theft

```text
DP1 PetState/GuardSync/Visitor与Owner结算
DF1 GuardSnapshot/TheftTargetDecision
SO1 ActionAdmission
SC1 TheftWorkflow
WF1 GuardSyncOrder/Workflow恢复
H1 偷菜和宠物提示
Q1 10槽、并发、截留和响应丢失
```

### M6 · Mail/其余 P0d

```text
MA1 MailEntryStore
DP1 MailClaim/TaskClaim/Pet/Compendium查询
P1 已有协议
H1 页面/红点
Q1 分页、过期、重复领取和局部降级
```

---

## 20. I0 每轮派发前的检查表

```text
[ ] 合同Baseline已填写
[ ] 代码Baseline已填写
[ ] Round 2及以后已填写SHARED_CONTRACT_BASELINE，消费者已rebase
[ ] 上游Commit已合入而不是只存在于其他分支
[ ] worktree干净且路径唯一
[ ] Agent写集和禁写集已登记
[ ] Proto/Actor/Redis/Workflow单写者没有冲突
[ ] composition root由I0保留
[ ] Agent知道必须执行的测试
[ ] Agent知道何时停止并上报合同歧义
[ ] handoff文件名已分配
```

若其中任意一项为空，不派发该 Agent。

---

## 21. 当前正式派发状态

Round 0、Round 1、MA0、AR1、WF1、H1 与 P1B-CONSUMERS-2 已完成。P1B 最终候选 `a22dca9`
经两轮独立审查关闭唯一 P1，并以 `74de919` 合入；integration 的整仓 race/vet/build/gen-proto/diff/clean
均通过。H5 合并点 `6e582d4` 已通过 tests 与多环境 build/audit；不得按旧 Prompt 重建这些任务。

当前没有活动编码 Agent或共享写锁：

```text
P1B-CONSUMERS-2  COMPLETED_VERIFIED / DO_NOT_REDISPATCH
CP3               cp3-20260804
下一步             从CP3审计PI1/FI1真实缺口，再拆成短工作包
```

P1B 已完成且不得重建或扩展 Proto/Catalog；仍 feature-disabled 的 26 条命令继续按里程碑启用，不能因为
Consumers-2 完成就伪造成功。下一轮由 I0 先读取 CP3 真实实现、现有 handoff 和测试，再为 PI1/FI1
登记互斥写集；动态 RT1 仍不阻塞最小经营闭环。

### 21.1 CP3 后审计冻结

审计确认 `NewAuthorityRegistry`、`HolderLeaseGuard` 和 Redis AuthorityStore 已有组件实现，但生产
`cmd/player-svr`、`cmd/farm-svr` 仍走 legacy Store，且 RouteManager/Assignment Watch/Holder Lease
尚未组成生产闭环。施工顺序冻结为：

```text
可立即并行：PI1-A Player Participant Adapter
            FI1-A Farm Participant Adapter
            SC1-A Plant Definition/Coordinator（消费已冻结接口）
            SO1-A Friendship Redis Adapter

独立基础轨：RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL

最终由I0串行完成：composition root切换 + E2E + rollback开关
```

RT1 前所有业务 Agent 禁止修改 `cmd/*`，禁止直接使用 `StaticFenceSource` 伪造生产写权。决策依据见
`docs/verification/ar2-production-authority-cutover-decision.md`。

本手册已是正式执行输入；I0 仍须为每个 Agent 单独填写：

```text
branch_name
worktree_path
base_commit
预计Commit数量
对应handoff文件
```

这些信息由 I0 在派发当下读取真实 Linux HEAD 后生成，不能复用上一轮值或伪造。
