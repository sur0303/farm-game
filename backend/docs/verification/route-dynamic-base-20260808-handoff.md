# CP-ROUTE-DYNAMIC-BASE Handoff

## 1. Identity

```text
integration_branch = codex/integration-20260803
base_commit         = 29ed7be2f7ddc6df2e837830c165ad50ca62e28f
baseline_branch     = checkpoint/route-dynamic-base-20260808
baseline_worktree   = farm-game-route-dynamic-base-20260808
route_foundation    = PRESENT_ONCE
static_assignment   = PASS
composed_runtime    = PASS
remote_watch        = NOT_IMPLEMENTED
route_controller    = NOT_IMPLEMENTED
autoscaling         = BLOCKED_UNTIL_G1_G2_G3_AND_NEW_BASELINE
status              = READY_FOR_RT2_WAVE1_ICR
```

`base_commit` 永远指 integration 代码基线 `29ed7be`，不是本 Baseline 分支后续只含文档的
checkpoint commit。Wave 1 的所有功能 Agent 必须从这个精确 SHA 分叉。

## 2. Integration truth

核验时：

- local integration HEAD = remote integration HEAD = `29ed7be`；
- tracked/untracked status 均为空；
- Route Foundation merge `10b80d1` 是 ancestor；
- 标题 `Merge Route Foundation MVP into integration` 的 merge commit 精确为 1 个；
- 最新 integration 还包含 `87df609` Hero/Theft/Perf merge 和 `29ed7be` isolated H5 smoke origin 修正；
- Baseline 未复用 integration 主工作树，也未开发运行时代码。

## 3. Baseline Gate

以下命令均在未修改的 `29ed7be` 上真实 Exit 0：

```text
go test -buildvcs=false ./internal/authoritycomposition/... -count=1                 PASS
go test -race -buildvcs=false ./internal/authoritycomposition/... -count=1           PASS
go test -buildvcs=false ./internal/logic/actor/authority/... -count=1                 PASS
go test -buildvcs=false ./internal/actorrouting/... -count=1                          PASS
go test -race -buildvcs=false ./internal/actorrouting/... \
  ./internal/logic/actor/authority/... -count=1                                      PASS
go test -buildvcs=false -timeout=600s ./... -count=1                                 PASS
go vet ./internal/authoritycomposition/... ./internal/actorrouting/...                PASS
go vet ./...                                                                          PASS
go build -buildvcs=false ./cmd/...                                                    PASS
git diff --check                                                                      PASS
```

公司 Proxy Redis composed+static process smoke：

```text
AUTHORITY_MEMBERSHIP_MODE=true
ACTOR_ASSIGNMENT_MODE=static
Q2R2_LIVE=1

go run -buildvcs=false ./cmd/q2r2b1-smoke
```

结果：

- S1 player+farm zero-shard Ready、composed、无 legacy renewal：PASS；
- S2A player one-shard real renew 至少 2 轮：PASS；
- S2B farm one-shard real renew 至少 2 轮：PASS；
- S3 mismatch/illegal mode fail closed，zero-route boot mismatch 保持合法：PASS；
- S4 SIGTERM 有界退出、无 Fatal、精确 key cleanup：PASS；
- overall：`Q2R2B1 PASS`。

Secret 只从 ignored local config 读取，没有写入命令证据或 Git；无 KEYS/SCAN/SCRIPT FLUSH。

## 4. Contract/source audit

I0 已完整阅读 Agent 工作包手册、Foundation Handoff，并审计 RouteSnapshot、Provider、
RouteAssignmentManager、Membership Runtime、Lease Scheduler、Redis Authority 和四个 command root。
现状：

- 本地完整 Snapshot model、validation、dynamic local provider、SnapshotApplier 已存在；
- Manager Claim/Transfer/Unassign/Renew/Drain/recovery/tombstone memory seam 已存在；
- Membership 与 Scheduler 已能消费本地 DynamicAssignmentProvider；
- 尚无 durable snapshot Store、远程 source/pump、Redis transition adapter、Controller/Holder/routectl；
- command roots 仍只从静态文件启动，尚无 `ACTOR_ASSIGNMENT_MODE=remote-watch` 接线。

## 5. Luna Max audit

只读 Review 使用：

```text
CLI       = OpenAI Codex
provider  = openai
model     = gpt-5.6-luna
reasoning = max
sandbox   = read-only
base      = 29ed7be2f7ddc6df2e837830c165ad50ca62e28f
```

本机 Codex 默认配置曾指向非官方 provider；I0 发现后立即终止并丢弃这些会话结果。最终采用的
focused reviews 日志明确显示 `provider: openai`，两条均 Exit 0。

首轮 Finding：

- P0：ASSIGNING 是否可以作为 durable full Snapshot 的合同解释不清；
- P1：Store CAS/bootstrap/checksum/replay 未冻结；
- P1：Remote Pump 缺独立 installer capability 与 authenticated-source contract；
- P1：single-controller 是部署假设还是 fencing 未明确；
- P1：A/B/Q write set 与 provider/recovery seam 可能重叠；
- P2：revision jump、capability rejection、corrupt snapshot、Evidence manifest 需补矩阵。

I0 未忽略 Finding，已形成：

`docs/verification/route-control-shared-contract-rc-scb1-20260808.md`

其中冻结：

- ASSIGNING 是可持久化/传播但不可服务的 Handoff 投影；ACTIVE 仍只在 Target prepare 后发布；
- snapshot envelope、canonical JSON、SHA-256、topology fingerprint、CAS/replay；
- RT2A 只实现 existing recovery/tombstone seam 的 durable adapter；
- RT2B 使用独立 `RemoteAssignmentInstaller` capability；
- source authentication 属于 RT2C transport adapter，Pump 仍二次校验；
- remote absent-holder 与 static boot mismatch 使用不同显式构造路径；
- MVP single-controller 的非 HA 边界；
- A/B/Q-0 精确逐路径 write lock；
- Q Final 必须新建 worktree；
- E1/E2/E3/E4 evidence 等级。

## 6. Closure result

最终 closure 使用 official OpenAI Codex `gpt-5.6-luna`、reasoning `max`、read-only，Exit 0：

```text
P0 = 0
P1 = 0
NO BLOCKING FINDINGS
verdict = READY
```

保留 P2 guard：RT2A ICR 必须明确显式 bootstrap API 与测试；RT2C 在 G2/G3 前必须落地
single-controller startup/deployment proof。两者不阻塞 Wave 1 ICR。

## 7. Next decision

```text
提交 Baseline 文档 checkpoint
→ 冻结 CP-ROUTE-DYNAMIC-BASE（code base 仍是 29ed7be）
→ 从 29ed7be 创建 RT2A / RT2B / RT2Q-0 三个独立 worktree
→ 每个 Agent 先只交 ICR
→ I0 确认 READY 后才允许对应 Writer 开始代码
```
