# Round 3 Linux Agent 施工交接

- 日期：`2026-08-04`
- 状态：`PAUSED_BY_USER / READY_FOR_EXTERNAL_LINUX_AGENT`
- Linux 根目录：`/data/home/developer_026/CodeBuddy/20260519141303`
- 后端仓库：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game`
- 集成分支：`codex/integration-20260803`
- 两个功能分支的冻结施工基线：`3e715a481ca62091e65b2c36977b13fef1461e76`
- CP3 Tag：`cp3-20260804`

## 1. 本轮已经完成的工作

1. W5B 已完成并进入 `codex/integration-20260803`，包含双 Gate/Fresh Join、Drain、200 人续租与 GateBoot 三态退役证据。
2. CP3 后重新审计了 AuthorityStore、Player/Farm Registry、Workflow、生产 `cmd/*` 和路由控制面。
3. 冻结了生产 Authority 切换边界：
   - `NewAuthorityRegistry`、Redis AuthorityStore、`HolderLeaseGuard` 已有组件实现；
   - 当前 `cmd/player-svr`、`cmd/farm-svr`、`cmd/farm-worker` 仍是 legacy composition；
   - RouteManager、Assignment Watch、Holder Lease 与 FirstHop 完整 Route Fence 尚未形成生产闭环；
   - RT1 完成前禁止把测试 `StaticFenceSource` 接入生产 `cmd/*`，也禁止 legacy + authority 双写。
4. 已更新并推送：
   - `docs/design/71-multi-agent-implementation-plan-draft.md`
   - `docs/design/72-agent-work-package-playbook-draft.md`
   - `docs/design/73-linux-multi-agent-dispatch-runbook.md`
   - `docs/verification/ar2-production-authority-cutover-decision.md`
   - `docs/verification/implementation-lock-board.md`
5. 上述文档提交为：

```text
3e715a481ca62091e65b2c36977b13fef1461e76
docs:freeze-post-cp3-authority-cutover-boundary
```

两个功能 worktree 派发时，本地与远端 `origin/codex/integration-20260803` 已核对为同一施工基线，集成
工作区当时 clean。本交接文件自身会由后续文档提交发布到 integration，因此 integration HEAD 可能高于
`3e715a48...`；两个功能分支仍以已冻结的 `3e715a48...` 施工，不需要仅为本交接文档重基。

## 2. 当前两个施工工作树

### 2.1 PI1-A1 Player Plant Participant

```text
task_id: PI1-A1-PLAYER-PLANT-PARTICIPANT
branch: codex/pi1-a1-player-plant-participant-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-pi1-a1-player-plant
base: 3e715a481ca62091e65b2c36977b13fef1461e76
CodeBuddy session: round3-pi1-a1-player-plant-20260804
log: /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/logs/pi1-a1-player-plant.log
prompt: /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/prompts/pi1-a1-player-plant-participant.prompt.txt
status: QUOTA_WAIT / NO_CODE_CHANGE / WORKTREE_CLEAN
```

任务只实现 Plant 所需三个 Player Participant：

```text
RESERVE_SEED
COMMIT_SEED_RESERVATION
RELEASE_SEED_RESERVATION
```

同时抽取 Player/Farm 可共用的 Participant 入站校验内核。详细写集、禁止项、测试和 handoff 要求均在 Prompt 中，不要仅凭本交接缩写施工。

### 2.2 SO1-A Friend Redis Adapter

```text
task_id: SO1-A-FRIEND-REDIS-ADAPTER
branch: codex/so1-a-friend-redis-adapter-20260804
worktree: /data/home/developer_026/CodeBuddy/20260519141303/farm-game-so1-a-friend-redis
base: 3e715a481ca62091e65b2c36977b13fef1461e76
CodeBuddy session: round3-so1-a-friend-redis-20260804
log: /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/logs/so1-a-friend-redis.log
prompt: /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/prompts/so1-a-friend-redis-adapter.prompt.txt
status: QUOTA_WAIT / NO_CODE_CHANGE / WORKTREE_CLEAN
```

任务只把 SO0 已冻结的 `PairRepository`、`QuotaRepository`、`ShareIssuerRepository` 接到真实 Redis 物理 Adapter，不修改好友领域语义，不创建 FriendActor，不修改生产 `cmd/*`。

## 3. 为什么两个 CodeBuddy Agent 没有产出

两个 Kimi-K3 会话启动后均立即返回：

```text
429 当前无可用Token额度
```

这不是代码错误，也不是 worktree 损坏。两个进程均已退出，分支和 worktree 没有任何修改。不要因为 429 删除工作树、重建同任务分支或重复派两个 Writer。

如果继续使用原 CodeBuddy Session：

```bash
sudo -u developer_026 -H \
  /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/control/resume-codebuddy-agent.sh \
  pi1-a1-player-plant \
  /data/home/developer_026/CodeBuddy/20260519141303/farm-game-pi1-a1-player-plant \
  round3-pi1-a1-player-plant-20260804

sudo -u developer_026 -H \
  /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/control/resume-codebuddy-agent.sh \
  so1-a-friend-redis \
  /data/home/developer_026/CodeBuddy/20260519141303/farm-game-so1-a-friend-redis \
  round3-so1-a-friend-redis-20260804
```

默认恢复文本就是：

```text
请继续执行未完成的任务。
```

如果改由别的 Linux Agent 实现，应直接进入相应现有 worktree，先完整读取对应 Prompt；同时保持原 CodeBuddy Session 不运行，确保一个写集只有一个 Writer。

## 4. CP3 后代码审计的关键事实

### 4.1 已具备

- `api/proto/contracts/participant.proto` 已冻结 15 个 Participant 命令；
- `internal/contracts/catalog.go` 已有唯一 ParticipantCommandCatalog；
- `internal/contracts/fake.go` 已有较完整的测试 Fake、请求形状校验、Canonical Hash、Receipt 重放/冲突语义；
- `internal/identity/**` 已实现合同要求的 `farm-workflow-id-v2` Root 作用域身份；
- `internal/player/actor.NewAuthorityRegistry`、`internal/farm/actor.NewAuthorityRegistry` 已存在并有 Memory/Redis 组件测试；
- `internal/storage/authoritystore` 和 Redis AuthorityStore 已支持 AuthorityReceipt、StoredOperationOutcome、AttachedOrder、PendingOutbox、Unknown 解析和 Writer Fence；
- Farm 现有领域方法已有 Plant/PrepareHarvest/PrepareCare/PrepareTheft 等 legacy 主链能力；
- Friend Pair/Quota/Share/Permit/Opportunity 的领域模型和 Repository Port 已完成。

### 4.2 真实缺口

- 生产 Participant gRPC 目前只有 Fake，没有 Player/Farm 真实 Handler；
- `internal/contracts/fake.go` 的入站校验尚未抽成生产可复用内核；
- Player/Farm 的 Actor 内 ParticipantReceipt、typed result 重放与精确 `committed_record_version` 接缝尚未完成；
- 通用 Workflow Engine/Dispatcher/RecoveryLoop 尚未装入生产 `cmd/*`；
- legacy `internal/workidentity.WorkflowID` 仍是 v1，但新实现必须使用 `internal/identity.WorkflowID` v2；不要把 v1 扩散到新链路；
- Friend 新领域 Repository 尚未完全映射到符合 33A 的 Redis Authority/HashTag/Lua 物理布局；
- RT1 路由写权控制面未完成，生产 composition 不能切 Authority。

## 5. 冻结的施工顺序

```text
当前可并行：
  PI1-A1 Player Plant Participant
  SO1-A  Friend Redis Adapter

PI1-A1 合入后：
  FI1-A1 Farm CommitPlant Participant

PI1-A1 + FI1-A1 合入后：
  SC1-A1 Plant Workflow Definition / Coordinator

独立基础轨：
  RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL

RT1 完成后由 I0 串行执行：
  cmd composition root authority 切换
  rollback mode
  禁止双写
  E2E / 故障 / 容量验证
```

不要提前并行 FI1-A1：它应复用 PI1-A1 抽出的唯一 Participant 入站校验内核，避免两套校验漂移。

## 6. 外部 Linux Agent 开工检查

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
git rev-parse HEAD
git rev-parse origin/codex/integration-20260803
git status --short --branch

cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-pi1-a1-player-plant
git status --short --branch
sed -n '1,260p' /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/prompts/pi1-a1-player-plant-participant.prompt.txt

cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-so1-a-friend-redis
git status --short --branch
sed -n '1,260p' /data/home/developer_026/CodeBuddy/20260519141303/.agents/round2-20260804/prompts/so1-a-friend-redis-adapter.prompt.txt
```

预期：三个工作区均 clean；集成 HEAD 与远端一致且包含本交接；两个功能分支仍基于
`3e715a48...` 且尚无功能提交。

## 7. 绝对禁止项

- 不修改或清理 Windows 的旧脏工作区 `C:\Users\developer_026\Desktop\supernova-study\farm-game-clone-test`；它包含用户文档变化；
- 不使用 `git reset --hard`、`git checkout --` 清理用户内容；
- 不在 RT1 前把 `StaticFenceSource` 放进生产 `cmd/*`；
- 不让 Holder 自行推进 WriterEpoch；
- 不做 legacy + authority 双写；
- 不创建第二套 CommandCatalog、Participant 校验、Hash、KeyBuilder 或 ScriptRegistry；
- 不因 Feature Disabled 而伪造业务成功；
- 不让两个 Agent 同时写同一 worktree/写集；
- 不把 miniredis 结果标成公司真实 Redis 证据。

## 8. 交付与合并纪律

每个实现 Agent：

1. 只推送自己的功能分支；
2. 不创建 MR、不合入 integration；
3. handoff 写明 base、commit、文件、合同映射、验证、真实 Redis 证据、ICR；
4. `git status --porcelain` 必须为空；
5. I0 只在独立审查通过后按顺序合并；
6. 每次合并后重新执行针对包 race、整仓 test、vet、build、gen-proto 零差异与 diff-check。

当前没有正在运行的 Writer 进程。本交接完成后，原调度方停止施工，等待外部 Linux Agent 接管。
