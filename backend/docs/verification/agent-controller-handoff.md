# AC1 Agent Controller 交接（bounded MVP）

这是下一轮农场开发使用的单机 Linux 控制器交接。实现继续是
`tools/agentctl/agentctl.py` 单文件、Python 3 标准库；它不是游戏运行时，
不修改 Actor、协议、Redis、Gateway 或 26～37 号合同。

本轮有意保留原子 JSON 状态和 Linux `flock`。SQLite、Hook、Codex SDK/App
Server、Web UI、systemd、分布式 Supervisor、自动 commit/push/merge 和
premerge/merge queue 都没有实现，也没有修改 `.codex/**`。

## 安装与自检

不需要 pip：

```bash
chmod +x tools/agentctl/agentctl
tools/agentctl/agentctl --help
tools/agentctl/agentctl doctor
```

状态和日志默认位于 `~/.local/state/ac1-agentctl`，生产使用建议显式隔离：

```bash
export AGENTCTL_STATE_DIR=/data/home/developer_026/.local/state/ac1-agentctl
export AGENTCTL_CODEX=/data/home/developer_026/.nvm/versions/node/v22.23.1/bin/codex
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" doctor
```

`doctor` 独立识别 Python、Git、tmux、Codex 和 `codex exec --json`。模型默认
来自 Manifest；人工恢复时才显式切换 `--model Sol --reasoning-effort xhigh`。

## Manifest 与 Context Pack

Schema 和示例：

- `tools/agentctl/schema/manifest.schema.json`
- `tools/agentctl/examples/manifest.example.json`

Writer Manifest 的必填字段是 `task_id`、`repo`、`worktree`、`branch`、
`base_commit`、`model`、`reasoning_effort`、`prompt_file`、
`owned_write_set`、`forbidden_write_set`、`authoritative_inputs`、
`acceptance_commands`、`review_scope`。可选字段包括 `semantic_locks`、
`risk`、`dependencies`。

Reviewer Manifest 使用 `role: "reviewer"`、`review_of: "<writer-task>"`，
`owned_write_set: []`；控制器会以 Codex `--sandbox read-only` 启动，并在退出
后比较真实 Diff。Reviewer 可使用 Writer worktree 或单独的只读 worktree；
Reviewer 不取得写集。

`register` 在写入状态前检查：

1. task id、相对写集模式、repo/worktree、branch 和精确 base commit；
2. prompt 与权威输入存在；声明的 sha256 必须匹配，未声明的 hash 会被冻结；
3. dependencies 的 Git HEAD；
4. 活跃任务的 owned/forbidden write set 与 semantic lock；冲突 fail closed。

状态目录下每个任务保存一个原子 JSON 状态、追加式 `codex.jsonl`、普通日志、
last message 和 exit code。Context Pack 只保存：

- prompt 和权威文件的精确路径、section、sha256；
- 当前 base commit 与 dependency commit；
- 冻结摘要、写集、semantic locks、risk、验收命令和 review scope。

它不复制权威正文。每次 `start`/`resume` 都重新检查 Manifest、prompt、权威
输入、base、依赖、锁和真实 Diff；漂移会设置 `guard=CONTEXT_STALE`、
`BASE_STALE`、`DEPENDENCY_STALE`、`LOCK_CONFLICT` 或 `POLICY_VIOLATION`，
并阻止继续运行。

## 并发与生命周期

默认 `max_concurrency=3`。任务使用可预测的 `ac1-<task_id>` tmux session，
pane 设为 `remain-on-exit`；tmux 只负责保活和人工救援，状态不从 pane 文本
推导。

```bash
# 登记四个已准备好的 worktree
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" register task-1.json
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" register task-2.json
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" register task-3.json
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" register task-4.json

# 一次性填满三个槽位；task-4 保持 QUEUED
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" dispatch-once
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" status

# 一项结束后只调度一次，不启动常驻无限循环
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" watch-once task-1
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" dispatch-once
```

也可以人工 `start TASK_ID`；满额时它会原子地转为 `QUEUED`。`watch-once` 在
发现一次结束后会最多调用一次 dispatch，外层仍可按需再次调用。

常用命令：

```bash
agentctl start TASK_ID
agentctl status [TASK_ID]
agentctl list
agentctl capture TASK_ID --lines 120
agentctl checkpoint TASK_ID --phase FOCUSED_REVIEW --note 'scoped review ready' \
  --test 'python3 -m unittest discover -s tools/agentctl/tests -v'
agentctl stop TASK_ID
```

阶段只表示任务流程：

```text
ICR -> IMPLEMENT -> FOCUSED_REVIEW -> FIX -> VERIFY -> PREMERGE -> DONE
```

`IMPLEMENTING`/`FIXING` 仍接受为上一轮 Manifest/checkpoint 的兼容别名。进程
状态独立显示为 `REGISTERED`、`READY`、`QUEUED`、`RUNNING`、
`READY_FOR_REVIEW`、`WAITING_CAPACITY`、`SUSPECTED_STALL`、`FAILED`、
`CONTRACT_CONFLICT` 或 `STOPPED`。退出 0 不会自动变成 DONE。

## JSONL 短摘要与恢复

Runner 只构造参数数组执行 `codex exec --json`，每次 run 追加原始 JSONL。
`status` 默认只展示短摘要，不 tail 长日志，包括：

- session/thread id；
- 最后 Agent message、当前 command、最后日志增长时间；
- 结构化测试事件、14003/capacity 信号和 exit code；
- Git HEAD、changed files、branch 和 remote ahead/behind。

14003、`full capacity`、`resource limit`：

```text
watch-once -> WAITING_CAPACITY
resume     -> codex exec resume <原 session/thread id>
```

控制器保留原 worktree、branch、日志和 Thread，不创建新 worktree。人工恢复
并切换模型的例子：

```bash
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" resume task-1 \
  --model Sol --reasoning-effort xhigh
```

普通长时间无 JSONL 增长只标 `SUSPECTED_STALL` 并输出 warning，绝不 kill。
非零退出标 `FAILED`；同一任务重复技术失败达到第二次时只给出手动
“可切 Sol xhigh”建议，不自动换模型、不无限重试。

## Write Set、Reviewer 与 Fixer

任务退出后，控制器读取真实 `git status --porcelain`：任何 forbidden 或不在
owned write set 内的文件都会设置 `POLICY_VIOLATION` 并 fail closed。控制器
不替 Agent commit、push、merge、rebase。

Reviewer 是独立任务/Thread，但必须声明 `review_of`，只检查该 Writer 的
Manifest review scope、真实 Diff 和验收合同。相同 Diff、合同和证据没有变化
时，第二次 Reviewer 会被拒绝，避免重复全仓复核。需要修复时使用：

```bash
tools/agentctl/agentctl fix writer-task-id \
  --model Luna\
  --reasoning-effort high
```

`fix` 调用原 Writer 的 `codex exec resume`，保留原 session/thread 和 worktree，
并把 phase 推进到 `FIX`；它不会创建第二个 Writer Thread。

## 停止与数据保留

`stop` 只停止该任务 tmux session，保留 worktree、branch、状态、JSONL、普通
日志、last message 和 Git 数据。SSH 断开不会让 pane 因 CLI 退出而被清理。

本轮不提供真实 Hook 安装命令；上层若未来启用 Hook，应在 Agent 退出后单独
安装并完成 trust/hash preflight，不能把 Hook 当作本控制器的状态真值或测试替代。

## 测试证据

本地 Fake 测试不调用真实模型，Fake tmux/Codex 覆盖：

- 三并发、第四排队、结束后一次性调度；
- 14003 到 WAITING_CAPACITY，再用同一 Thread/worktree resume；
- stall 不 kill；
- write-set conflict、Context stale；
- JSONL session/message/command/test 短摘要；
- Reviewer read-only、重复审查拒绝、Fixer 原 Thread resume；
- 原有原子状态、hash、注入防护、stop 保留 Git 数据意图。

验收命令：

```bash
python3 -m unittest discover -s tools/agentctl/tests -v
python3 tools/agentctl/agentctl.py --help
tools/agentctl/agentctl doctor
python3 -m py_compile tools/agentctl/agentctl.py
git diff --check
```

## 剩余限制

- 这是单机单上层控制者，不是 SQLite Supervisor 或分布式控制面；多个独立
  上层进程同时调度同一 state dir 不在支持范围内。
- 状态是每任务原子 JSON，查询会刷新运行摘要；后续若需要多读者/单写者
  服务，再迁移 SQLite，但本轮不混入该风险。
- `watch-once`/`dispatch-once` 是有界动作，不是常驻 watchdog；外层负责
  周期调用和人工容量恢复。
- 依赖检查要求 Manifest 指定的 dependency 路径是 Git root；不主动 fetch
  远端。
- JSONL schema 随 Codex CLI 版本变化；控制器支持常见 snake/camel case
  session/thread 和常见 agent message/command 事件，未知事件仍保留原文。
