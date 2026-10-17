# 74 · 农场项目多 Agent 控制面架构

版本：`v1.1-bounded-mvp`（历史基线；多模型目标见 `74-agent-control-plane-multi-model-v1.2.md`）
日期：`2026-08-04`
范围：Linux 开发机上的 Codex Agent 调度、上下文、隔离、验证、恢复与合并；不定义游戏业务合同。

> **演进指针（2026-08-05）**：多模型控制面 CP1～CP6 已完成并进入统一集成基线。
> 当前实现以 `tools/agentctl/agentctl.py`、`tools/agentctl/profiles.json` 和
> `docs/design/74-agent-control-plane-multi-model-v1.2.md` 为准，不依赖任何固定开发 worktree。
> 本文保留为 AC1 单 Codex Runner 基线说明。

> **AC1 当前实施边界（2026-08-04 决策）**：本轮只交付可立即用于下一轮农场开发的单机 MVP。保留上一轮单文件 `agentctl.py`、原子 JSON 状态和 Linux `flock`；最多并发 3 个任务，第四个排队。SQLite、Hooks、SDK/App Server、Web UI、systemd、分布式 Supervisor、自动 commit/push/merge 和 premerge/merge queue 均留在后续设计，不是本轮的完成条件。本文后续章节描述目标架构；实际命令和当前证据以 `docs/verification/agent-controller-handoff.md` 为准。

## 1. 结论

农场项目采用一个轻量、项目专用的 `agentctl` 控制面。它不是替代 Codex 的新 Agent，也不负责理解或重新解释 26～37 号业务合同；它负责把已经确认的合同安全、可恢复地交给不同 Codex Thread 执行。

当前唯一实施形态：

```text
┌──────────────────────────────────────────────────────────┐
│ Human Control Plane                                      │
│ Codex App Remote SSH / agentctl status|show|resume       │
└──────────────────────────┬───────────────────────────────┘
                           │ Unix domain socket / CLI
┌──────────────────────────▼───────────────────────────────┐
│ agentctl Controller（单机唯一上层控制者、JSON/flock 写者） │
│                                                          │
│ Task/Run State      Context Builder     Lock Manager      │
│ Scheduler           Staleness Detector  Capacity Policy  │
│ ExecJson Runner     Gate Runner         Evidence Index    │
│ Review Pipeline     Candidate Builder   Merge Queue       │
└──────────┬──────────────────┬──────────────────┬─────────┘
           │                  │                  │
           ▼                  ▼                  ▼
   原子 JSON + JSONL      codex exec --json   Git Worktrees
   控制事实与原始事件     Thread/Turn/Event    Branch/Commit
           │                  │                  │
           └──────────────────┴──────────────────┘
                              ▼
                  Tests / Premerge / Handoff

辅助防线：
Codex Hooks + workspace-write Sandbox + Git 权限隔离
```

核心原则：

1. `agentctl` 决定任务能否启动、继续、验收和合并；Codex Agent 只完成分配给它的工程工作。
2. 每项任务一个 worktree、一个分支、一条主 Writer Thread；修复继续原 Writer Thread，不反复重建上下文。
3. Agent 不直接合并 integration，也不直接 push；Git 提交、候选构建和合并由控制器执行。
4. 合同真值仍是仓库文档和固定 Commit；Context Pack 只保存精确引用与 Hash，不复制第二份“简化合同”。
5. Hooks 是快速防误操作的 Guardrail，不是唯一安全边界。
6. 六段式是任务流水线，不是固定启动六个 Agent。

## 2. 为什么需要单一 Supervisor

仅靠上层人工轮询多个终端存在四个问题：

- Agent 是否运行、排队、等待容量、停滞或失败只能从长日志猜测；
- SSH 断开、控制脚本退出后，任务状态和后续动作容易失去统一所有者；
- 多个 Agent 可能同时修改同一语义边界，worktree 只能隔离文件，不能识别语义冲突；
- 测试、复核和合并依赖人工记忆，容易反复做全仓审查。

Supervisor 只做控制工作，不长期持有游戏代码状态。它的主要循环是：

```text
读取可运行任务
→ 检查依赖、Context、Base、锁与并发额度
→ 创建或恢复 Codex Turn
→ 消费结构化事件
→ 更新运行事实
→ 运行确定性 Gate
→ 进入下一阶段或等待人工
```

首版把 Supervisor 放入固定 tmux Session 中运行，使 SSH 断开不影响任务。`agentctl` CLI 把控制请求以“临时文件 + 原子重命名”写入请求目录，Supervisor 消费后统一修改 SQLite。`status/show` 只读 SQLite。不先建设 Unix Socket、Web UI、systemd 服务或分布式控制集群。

## 3. Runner 选择

### 3.1 主 Runner：`codex exec --json`

当前公司 Linux 环境已经真实验证 Luna Max、Sol Extra High、Thread Resume 和 `codex exec --json`。因此首版只采用这一条 Runner：

- `stdout` 是结构化 JSONL，可直接识别 Thread、Turn、Tool、FileChange、完成和错误事件；
- Thread ID 持久化后使用原 Thread Resume；
- Writer 使用 `workspace-write`，Reviewer 使用 `read-only`；
- `14003` 等容量错误由控制器分类为 `WAITING_CAPACITY`；
- 不依赖终端文本正则，不因 SSH 断开丢任务；
- 与现有公司模型和认证环境兼容，不引入新的 SDK 版本风险。

必须显式固定当前公司环境可用的 Codex CLI 路径，并在 Supervisor 启动时完成版本、模型、Hook 和 Sandbox Preflight。

### 3.2 tmux 的定位

每个活跃 Agent 仍可有独立 tmux Session，用于进程保活和人工救援，但：

- 状态只从 JSONL、进程、Git 和测试证据派生；
- 不通过 Pane 文本判断完成或停滞；
- 正常恢复由 `agentctl` 使用原 Thread ID 进行。

### 3.3 暂不使用 SDK 或 App Server

不直接实现 App Server JSON-RPC Client，也不在首版迁移 Python/TypeScript SDK。SDK 确实更适合长期自动化集成，但目前不会解决比“可靠状态、写集、恢复和门禁”更紧迫的问题。等首个 Plant Pilot 跑稳后，如果 `exec --json` 在主动控制、多 Turn 并发或事件语义上成为明确瓶颈，再在不改变上层 Task/Run 模型的前提下替换 Runner。

## 4. 四类真值

控制器不能把所有事实塞进一个 JSON 文件。

| 真值 | 载体 | 作用 |
|---|---|---|
| 控制真值（目标） | SQLite | Task、Run、Thread、锁、阶段、测试、评审、候选与合并状态 |
| 控制真值（当前 MVP） | 每任务原子 JSON + `flock` | 并发额度、运行摘要、Context Pack、审查证据和恢复事实 |
| 原始事件 | 每 Run 追加式 JSONL | Codex 事件、控制动作、错误与时间线，可审计、可重放 |
| 代码真值 | Git Worktree / Commit | 实际代码、差异、候选提交和集成结果 |
| 合同真值 | 26～37、Manifest 与固定 Commit | Agent 应遵守的业务与技术合同 |

面向人的 `status.json`、控制台表格和 Handoff 都由上述事实生成，不能反向作为真值写回。

## 5. SQLite 物理约束（后续迁移，不属于当前 MVP）

数据库位于本机 `/data` 的 ext4 文件系统，不放到 NFS。首版由 Supervisor 单写，优先使用普通回滚日志模式，避免当前旧 SQLite 的 WAL 并发缺陷：

```text
PRAGMA journal_mode=DELETE
PRAGMA foreign_keys=ON
PRAGMA busy_timeout=5000
```

但采用更严格规则：

- Supervisor 是 SQLite 唯一写者；CLI 的控制动作进入原子请求目录，状态命令只读；
- Runner 与 Hook 不直接更新数据库，而是写追加式事件/请求，由 Supervisor 消费；
- JSONL 先追加，Supervisor 再在事务内更新索引状态；
- 启动时检查 SQLite 版本。

当前 Linux 的 Python SQLite 是 `3.42.0`。SQLite 官方说明 WAL 在多连接并发写/Checkpoint 下存在一个已修复缺陷，修复版本包括 `3.44.6`、`3.50.7`、`3.51.3+`。首版没有高吞吐数据库需求，因此不为 WAL 冒险；以后升级 SQLite 并有并发读性能证据后再开启 WAL。

建议表：

```text
tasks          任务静态定义、当前 phase
runs           每次模型 Turn/验证/评审运行
threads        Writer/Reviewer Thread ID 与模型
events         JSONL 事件索引和关键结构化事件
locks          文件写集锁、语义锁、Lease
artifacts      Context Pack、Diff、Handoff、候选 Commit
tests          命令、结果、耗时、日志引用
reviews        结构化 Findings 与修复状态
checkpoints    Agent 语义检查点
merge_queue    候选、Integration Base、队列状态
```

## 6. 状态模型：阶段与运行状态分离

不使用一个包含几十种状态、允许任意跳转的大 FSM。保存两组正交信息，再派生用户看到的状态。

### 6.1 Phase

```text
ICR
→ IMPLEMENT
→ FOCUSED_REVIEW
→ FIX（需要时）
→ VERIFY
→ PREMERGE
→ MERGE
→ DONE
```

### 6.2 Run Status

```text
QUEUED
RUNNING
WAITING_CAPACITY
SUSPECTED_STALL
INTERRUPTED
SUCCEEDED
FAILED
CANCELLED
```

### 6.3 Guard 状态

```text
CONTEXT_STALE
BASE_STALE
DEPENDENCY_STALE
LOCK_CONFLICT
POLICY_VIOLATION
NEEDS_HUMAN
```

例如“正在实现但模型满载”等于：

```text
phase = IMPLEMENT
run_status = WAITING_CAPACITY
```

不会为了容量不足把业务阶段改成 FAILED。

`SUSPECTED_STALL` 只能表示一段时间没有结构化进展，不能自动杀死进程。控制器先检查进程、最后事件、工具是否仍在运行，再决定提醒、Steer、Interrupt 或人工接管。

## 7. Context Builder

每个 Task Manifest 至少声明：

```text
task_id / title / risk
repo / integration_ref / expected_base_commit
authoritative_inputs[]
  - path
  - section heading 或稳定锚点
  - document_sha256
  - optional section_sha256
dependencies[]
owned_write_set[]
semantic_locks[]
acceptance_commands[]
review_scope[]
model_policy
```

Context Pack 只生成短索引：

- 本任务目标与明确非目标；
- 权威文件的绝对路径、章节、Commit 和 Hash；
- 允许修改的路径；
- 依赖任务的候选 Commit；
- 验收命令；
- 上一 Turn 的语义 Checkpoint。

Agent 自己按清单读取所需章节。禁止把几万行合同全文塞进 Prompt，也禁止复制一份长期漂移的“迷你合同”。

启动与每次 Resume 前执行：

```text
verify_context_hash
verify_base_commit
verify_dependency_commit
verify_write_set_and_semantic_locks
```

任意一项漂移都停止新 Turn，进入对应 Guard 状态，不让 Agent 按旧前提继续实现。

## 8. Hooks 的职责（后续，不属于当前 MVP）

Hooks 与控制器形成两层防线。

### 8.1 目标 Hook（当前 MVP 不安装）

`SessionStart(startup|resume|compact)`：

- 从 `AGENTCTL_TASK_ID` 获取不超过约 500 Token 的任务摘要；
- 告知当前写集、合同引用、Phase 和禁止事项；
- Context/Base 已过期时阻止继续。

`PreToolUse(Bash|apply_patch|Edit|Write)`：

- 阻止明显越出 owned write set 的编辑；
- 阻止 Agent 自行 `git commit/push/merge/rebase`；
- 阻止写 integration worktree、权威合同或其他任务 worktree；
- 阻止明显破坏性命令。

`PostToolUse`：

- 记录工具事实；
- 重新检查真实 Git Diff 是否越界；
- 越界时标记 `POLICY_VIOLATION`。它不能撤销已经发生的副作用。

`Stop`：

- 只检查本 Turn 是否提交了结构化 Checkpoint；
- 第一次缺失时可以自动继续一次；
- `stop_hook_active=true` 后仍缺失则允许退出并标记 `NEEDS_HUMAN`，避免无限续跑和浪费 Token。

### 8.2 不放进 Hook 的内容

- 不在 Hook 内跑全量测试；
- 不在 Hook 内合并、提交或修复代码；
- 不在 Hook 内调度其他 Agent；
- 不解析不稳定的完整 Transcript；
- 不把长日志作为 additional context 返回给模型。

Codex 官方明确说明部分工具路径可能绕开 Hook，因此 Agent 仍运行在 `workspace_write` Sandbox 中，Git 与合并权限由控制器掌握，并在 Gate 中再次检查真实 Diff。

### 8.3 Hook 信任

项目 Hook 必须经过 Codex 的精确定义 Hash 审查。首版使用稳定、版本化的用户级 Hook 路径，人工通过 `/hooks` 信任一次；控制器启动前校验 Hook 脚本 Hash。Hook 变化时任务必须停止并重新审查，不默认使用 `--dangerously-bypass-hook-trust`。

## 9. Write Set 与 Semantic Lock

Worktree 解决“文件内容互不覆盖”，不能解决“两个任务虽然改不同文件，但同时改变同一协议语义”。

因此每个任务同时持有：

- `owned_write_set`：允许修改的文件/目录 Glob；
- `semantic_locks`：例如 `workflow:plant`、`proto:public-command`、`redis:friend-authority`、`actor:farm-participant`。

锁由 Supervisor 租约管理。任务、Supervisor 或机器异常后，必须先确认原 Runner 不再活跃，才能回收锁；不能仅凭超时直接让两个 Writer 同时运行。

## 10. 风险自适应六段式流水线

六段式表示工程阶段，不等于六个 Agent：

```text
1. ICR：读取合同、列出改动面、冲突和测试
2. Writer：同一 Thread 实现
3. Focused Review：独立 Read-only Thread，只审声明范围
4. Fixer：恢复原 Writer Thread 修复明确 Findings
5. Verify/Premerge：确定性命令 + 最新 integration 临时 worktree
6. Merge/Handoff：控制器提交、合并、推送并生成证据
```

风险策略：

| 风险 | 流程 |
|---|---|
| Low | ICR → Writer → Targeted Verify → Premerge |
| Medium | ICR → Writer → Focused Review → Fixer → Verify → Premerge |
| High | ICR → Writer → Domain Review → Fixer → Independent Verify → Premerge |

复核禁止重新发起无边界全仓审计。Reviewer 只检查 Manifest 中的合同、Diff、原始 Findings 和验收项；Fix Verification 只确认原 Findings 是否闭合。

## 11. Git 与合并所有权

Agent 的 Writer/Reviewer Turn 使用：

```text
Writer   = workspace_write
Reviewer = read_only
```

Agent 不持有 Git Common Dir 写权。标准过程：

```text
Writer 修改 task worktree
→ agentctl 校验真实 Diff 与写集
→ 运行 Targeted Tests
→ agentctl 创建候选 Commit
→ Reviewer 审候选 Diff
→ 原 Writer Thread 修复
→ agentctl 重建候选 Commit
→ 从最新 integration 创建临时 premerge worktree
→ cherry-pick 候选并运行 Premerge Gate
→ 单线程 Merge Queue 更新 integration
→ Push + Evidence + Handoff
```

任何 Agent 都不能直接修改 integration 分支。这也避免 Agent 因 Sandbox 无法写 `.git/index.lock` 后自行发明临时 Git Object Store 绕过权限。

## 12. 失败与恢复

### 12.1 模型容量不足

`14003` 或 SDK 可识别 overload：

```text
保留 worktree、Thread ID、Phase 和锁
→ WAITING_CAPACITY
→ 指数退避 + jitter
→ 恢复同一 Thread 继续
```

容量不足不是代码失败，不重新审计仓库。

### 12.2 Supervisor 重启

启动后 Reconcile：

```text
SQLite RUNNING Run
→ 检查子进程/Turn 是否仍存在
→ 检查 Git Diff 与最后 JSONL 事件
→ 可恢复则 thread_resume
→ 不确定则 INTERRUPTED + NEEDS_HUMAN
```

Supervisor 不能因“长时间没有 Agent 文本”就认定失败；应以结构化 Turn/Tool 事件和进程状态判断。

### 12.3 同一问题反复失败

- Luna Max 默认；
- 容量排队只重试，不算技术失败；
- 同一可复现技术问题经过两个修复 Turn 仍未闭合，恢复同一 Writer Thread 切换到 GPT-5.6 Sol Extra High；
- 不新建 worktree，不丢失已有修改；
- 仍失败则 `NEEDS_HUMAN`，附最小复现和证据。

## 13. 长期首版目标与当前 MVP

长期架构必须有：

1. `agentctl supervise|status|show|start|resume|continue|stop`；
2. SQLite Schema、Migration、单写 Supervisor 与原子请求目录；
3. Manifest 校验和 Context/Base/Dependency Stale；
4. File Write Set 与 Semantic Lock；
5. 单一 `ExecJsonRunner`，支持新 Thread、原 Thread Resume 和结构化 JSONL；
6. JSONL 事件落盘与结构化状态派生；
7. `WAITING_CAPACITY`、`SUSPECTED_STALL`、`INTERRUPTED` 恢复；
8. Targeted Verify、Focused Review、Candidate/Premerge Worktree、Merge Queue；
9. 最小 Hooks 和 Hook Trust/Hash Preflight；
10. 单元测试和 Fake Runner 集成测试。

当前 MVP 已交付/补强：

1. 原子 JSON + `flock` 的单文件 Controller；
2. `max_concurrency=3`、第四任务 `QUEUED` 和 `dispatch-once`；
3. Context Pack 引用/hash/base/依赖复核与 stale guard；
4. JSONL 短摘要、Thread/last message/command/测试/14003/exit 提取；
5. `SUSPECTED_STALL` 不杀进程、14003 原 Thread/worktree resume、失败计数和 Luna→Sol 手动建议；
6. 真实 Git changed-files 写集 Gate、Reviewer read-only sandbox、重复 Review 拒绝和 Writer Fix resume；
7. Fake tmux/Codex 测试证据。

当前明确暂缓：

1. SQLite 单写 Supervisor 与 request inbox；
2. Web UI；
3. 多机分布式 Supervisor；
4. 自动解决代码冲突、candidate/premerge/merge queue；
5. Hooks 及 Hook trust/hash preflight；
6. 自动修改权威合同；
7. 无人值守直接合并主分支；
8. 根据成本自动选择任意模型。

## 14. Pilot

控制器完成后，不立即调度所有剩余工作。使用 `Plant Workflow Coordinator` 作为首个 Pilot，因为它同时覆盖：

- Farm/Player 双 Actor；
- Workflow/Receipt/Unknown 语义；
- 多份权威合同的 Context Pack；
- 文件写集与语义锁；
- Writer → Reviewer → Fixer → Premerge；
- 有价值但范围可控的端到端测试。

Pilot 通过后，再并行打开好友、任务/邮件和 H5 经营链路，最大并发先保持 3。

## 15. 当前 AC1 基线如何演进

当前 AC1 已具备 Manifest、Hash、worktree、tmux/JSONL、基础状态和测试；本轮保留单文件实现并做有界增量，以下仍是后续演进顺序：

1. 把单个大状态 JSON 迁移为 SQLite + 只读 Snapshot；
2. 将大枚举 FSM 拆成 Phase、RunStatus、Guard；
3. 固定 `ExecJsonRunner`，tmux 只负责进程保活；
4. 增加单一 Supervisor 和原子请求目录；
5. Agent 移除 Git 提交和 Push 职责；
6. 增加 Reconcile、Hooks 与双重 Write Set 校验；
7. 增加 Candidate/Premerge/Merge Queue。

如果这些改动导致继续维护现有 1900 行单文件的成本高于拆分，应把代码拆为：

```text
agentctl/
├── cli.py
├── supervisor.py
├── db.py
├── model.py
├── context.py
├── locks.py
├── scheduler.py
├── runner.py
├── gates.py
├── gitops.py
├── hooks/
└── tests/
```

不要求保留旧单文件结构的兼容性；只保留已经验证有效的行为与测试意图。

## 16. 参考依据

- OpenAI Codex SDK：<https://developers.openai.com/codex/sdk>
- OpenAI Codex Python SDK：<https://github.com/openai/codex/tree/main/sdk/python>
- OpenAI Codex App Server：<https://developers.openai.com/codex/app-server>
- OpenAI Codex Hooks：<https://developers.openai.com/codex/hooks>
- SQLite WAL：<https://sqlite.org/wal.html>
- Git Worktree：<https://git-scm.com/docs/git-worktree>
