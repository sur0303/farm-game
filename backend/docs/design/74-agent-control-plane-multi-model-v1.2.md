# 74 · 农场项目多模型 Agent 控制面架构

版本：`v1.2-sol-controller-luna-grok-repair-loop`
日期：`2026-08-05`
状态：`IMPLEMENTED / MULTI_RUNNER_PILOT_VERIFIED / INTEGRATED`

> 规范实现位于 `tools/agentctl/`，已经通过 Grok Fix + Luna Review 真实 Pilot。
> 旧 CP1～CP6 worktree 仅为历史开发现场，不再是运行依赖。

范围：Linux 开发机上的多 Provider Agent 调度、上下文隔离、确定性验证、修复升级与恢复；不定义游戏业务合同（26～37 号仍由仓库文档与固定 Commit 承载）。

本文在 `74-agent-control-plane-architecture.md`（v1.1-bounded-mvp）基础上演进：保留 Context Pack、worktree、写集、语义锁、风险自适应六段式与 AC1 单文件 `agentctl.py` 的已验证行为；将「单 Codex 模型 + 单 Thread 内 Luna→Sol 切换」升级为「单 Task 下多 Runner、多 Run 接力」。它不引入 SQLite Supervisor、Web UI 或自动 merge queue 作为本轮完成条件。

---

## 0. 一页结论

### 0.1 架构总览

```text
┌─────────────────────────────────────────────────────────────────────┐
│ External Sol Control Agent（外部控制面，Sol xhigh 人工/会话控制）      │
│ 任务拆分 · 风险 · ICR 审批 · 升级决策 · 合同冲突 · 最终交接授权        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ 读取 controller-brief.json / .md
                                │ 下发 Manifest · 批准 next 动作
┌───────────────────────────────▼─────────────────────────────────────┐
│ agentctl Controller（项目专用控制面，JSON + flock 唯一写者）            │
│ Task/Run 编排 · Profile 解析 · Verify Gate · Repair 计数 · 产物索引   │
│ Write-set / Semantic Lock · Context Staleness · Capacity 分类        │
└───────┬─────────────────┬─────────────────────┬─────────────────────┘
        │                 │                     │
        ▼                 ▼                     ▼
 Codex CLI Runner    Cursor CLI Runner    CodeBuddy CLI Runner
 (Luna / Sol)        (Grok 4.5 HF)        (Kimi K3 backup)
        │                 │                     │
        └─────────────────┴─────────────────────┘
                          ▼
              Git Worktree + 结构化产物目录
              Tests / Handoff / controller-brief
```

**控制关系**：外部 Sol 控制 Agent 不替代 `agentctl` 写状态；`agentctl` 不替代外部 Sol 做任务级战略决策。二者通过短摘要 `controller-brief` 衔接。

### 0.2 默认多模型流水线

对 Medium/High 风险任务的推荐默认环：

```text
LUNA_IMPLEMENT（Codex Luna Max，Writer，可 Resume 同一 Session）
    → VERIFY（agentctl 确定性 acceptance_commands）
        ├─ CODE_FAILURE ──→ GROK_FIX（Cursor Grok，定向 Bug Pack）
        │                       → VERIFY（重复，最多 max_fix_cycles_before_diagnosis 轮）
        └─ PASS ──────────→ LUNA_REVIEW（Codex Luna，新建 read-only Session）
                                ├─ PASS ──→ READY_FOR_HANDOFF
                                └─ REQUIRED_FIX ──→ GROK_FIX → VERIFY → …
两轮同签名 CODE_FAILURE 仍未闭合
    → SOL_DIAGNOSE（Codex Sol xhigh，read-only，输出 diagnosis.json）
    → GROK_FIX_AFTER_DIAGNOSIS（一次升级修复，max_post_diagnosis_fix_cycles=1）
    → FINAL_VERIFY
    → LUNA_FINAL_REVIEW
仍失败
    → 外部 Sol 决定：KIMI_CHALLENGE / KIMI_BACKUP_WRITER / SPLIT / ROLLBACK / NEEDS_HUMAN
```

**关键顺序修正**：测试/编译失败时**不要**先调用 Luna Review。确定性失败证据应直接交给 Grok 定向修复；Luna Review 仅在 Verify PASS 后检查测试难以覆盖的语义、越界与合同边界。

### 0.3 八条核心约束

1. **控制分层**：外部 Sol 控制 Agent 负责任务拆分、风险、ICR、升级与交接授权；`agentctl` 负责 Run 编排、Gate 与状态真值；执行 Agent 只做被分配角色的工程工作。
2. **单 Writer 单 worktree**：每个 Task 固定一个 worktree、一个分支；同一时刻只有一个 Writer 持有 workspace-write；Fixer 在 Grok/Kimi Profile 下接力，但不并行两个 Writer。
3. **Git 所有权**：Agent 不自行 `commit/push/merge/rebase`；候选 Diff、写集校验与 Handoff 由 `agentctl`/人工控制面执行。
4. **合同真值不外复制**：26～37 号与 Manifest 引用仍是唯一合同源；Context Pack 只保存路径、章节、Commit 与 Hash，禁止塞入「简化版合同」。
5. **结构化产物交接**：模型之间通过 `.agentctl/tasks/<task_id>/` 下 JSON/Markdown 产物交互，禁止复制完整聊天记录作为下一 Run 的主输入。
6. **Verify 先于 Review**：编译/单测/接口失败走 Grok Fix 环；Luna Review 仅对 Verify PASS 的 Diff 做 focused review。
7. **Sol 诊断只读**：内部 Sol Diagnoser 不写代码；若 `contract_conflict=true` 立即停止并交外部 Sol 与人工确认，禁止 Grok 猜测合同缺口。
8. **可计数的升级**：只有「Bug Pack → Fix → Verify（→ Review）」构成有效 `repair_cycle`；Provider 容量、环境、认证问题不计入技术修复失败；Profile 声明与 Runner 实际选中模型不一致时进入 `MODEL_MISMATCH` Guard，禁止静默换模。

### 0.4 与 v1.1 的继承关系

| v1.1 已验证能力 | v1.2 变化 |
|---|---|
| 原子 JSON + `flock`，`max_concurrency=3` | 保留；Task 状态扩展 Run 索引 |
| Context Pack hash / base / dependency stale | 保留 |
| `codex exec --json` + tmux 保活 | 扩展为多 RunnerAdapter；Codex 仍为 Luna/Sol 主路径 |
| Writer / Reviewer 角色 | Reviewer 强制 fresh Session + Findings 文件协议 |
| `failure_count` + Luna→Sol 手动建议 | 拆分为 repair_cycle / failure_signature / post_diagnosis 计数 |
| 写集 Gate、semantic lock | 保留；Fixer/Diagnoser 受 Manifest Bug Pack 约束 |

---

## 1. 角色边界

多模型流水线不是「六个 Agent 同时在线」，而是**按 Phase 激活的单角色 Run**。每个角色有明确的输入、输出、写权限与禁止事项。

### 1.1 External Sol Control Agent（外部控制 Sol）

**身份**：当前人工控制会话中的 Sol xhigh（或等效高推理控制 Agent），**不由 `agentctl` 自动调用**。

**职责**：

- 从实施计划/Manifest 池中选择下一任务包与风险等级
- 审批或拒绝子 Agent 的 ICR（改动面、冲突、测试计划）
- 决定何时启动 `diagnose`、`fallback`、`next` 中的非默认分支
- 处理 `contract_conflict`、跨任务语义冲突与缩小范围/回滚
- 最终判定是否 `READY_FOR_HANDOFF` 并允许合并/integration 继续

**输入**：`controller-brief.json`、Handoff 摘要、关键 verify/findings/diagnosis 产物（非 raw JSONL 全文）。

**输出**：新/修订 Manifest、明确指令（如「启动 SOL_DIAGNOSE」「批准 Kimi backup 一次」）、NEEDS_HUMAN 决策。

**禁止**：直接修改 task worktree 代码；绕过 `agentctl` 写状态；代替 Reviewer 输出 Findings。

### 1.2 Luna Writer + Independent Luna Reviewer

#### Luna Writer

- **Profile**：`codex-luna-primary`
- **Runner**：Codex CLI，`workspace-write`
- **Session**：ICR + IMPLEMENT 阶段可 Resume **同一 Writer Session/Thread**，保留实现上下文
- **输入**：Manifest、Context Pack、Prompt、依赖 Commit 引用
- **输出**：`runs/run-*-luna-writer/implementation-checkpoint.json`、代码 Diff
- **职责**：理解合同、列出改动面、完成主体实现、提交结构化 Checkpoint

#### Luna Reviewer（必须与 Writer Session 隔离）

- **Profile**：`codex-luna-review`
- **Runner**：Codex CLI，`read-only`，**`fresh_session: true`（禁止 Resume Writer Session）**
- **输入**：Manifest、合同引用、当前 Diff、Verify 证据、Writer Checkpoint（不含 Writer 全量 transcript）
- **输出**：`findings/findings-NNN.json`
- **审查重点**（测试不易覆盖）：
  - 幂等与失败语义是否正确
  - 是否修改了不属于任务的代码
  - 是否绕过 Workflow/Authority 主链
  - 是否仅让测试「看起来通过」
  - 是否遗漏合同中的竞态与恢复边界

**禁止**：Writer Session Resume 后自审；Reviewer 写任何文件。

### 1.3 Grok Fixer（Cursor Grok 4.5 High Fast）

- **Profile**：`cursor-grok45-fixer`
- **Runner**：Cursor CLI，`workspace-write`，**fresh Session**
- **输入**：**定向 Bug Pack**（见 §4.3），来源于 `verify-*.json` 和/或 `findings-*.json` 中的 Required 项
- **输出**：`runs/run-*-grok-fixer/fix-checkpoint.json`、最小 Diff
- **职责**：根据失败签名或 Required Finding 做快速定向修复，不重规划整个业务

**禁止**：接收 26～37 号全文；扩大写集；自行决定合同解释。

### 1.4 Sol Diagnoser（内部诊断 Sol，只读）

- **Profile**：`codex-sol-xhigh-diagnose`
- **Runner**：Codex CLI，`read-only`，fresh Session
- **触发**：同一 `failure_signature` 下 `repair_cycle_count >= max_fix_cycles_before_diagnosis`（默认 2）
- **输入**：两轮 Verify 结果、Findings 历史、Diff 摘要、Fix Checkpoint、Bug Pack 历史
- **输出**：`diagnosis/diagnosis-NNN.json`
- **职责**：收敛根因、解释前两轮修复为何失败、给出 `minimal_fix_plan`；判断是否存在 `contract_conflict`

**禁止**：直接修改代码；在未输出 diagnosis 前启动下一轮 Grok Fix。

### 1.5 Kimi Backup / Challenger（CodeBuddy Kimi K3 High）

- **Profile**：`codebuddy-kimi-k3-backup`
- **Runner**：CodeBuddy CLI；默认 **不进入固定流水线**
- **两种模式**：
  1. **Provider 后备 Fixer**：Grok/Cursor 长时间不可用时，接收相同 Bug Pack 在同一 worktree 继续修复
  2. **独立挑战者**：Sol 诊断后仍失败时，只读分析单一失败点，质疑根因是否遗漏；是否升级为 Writer 由外部 Sol 决定

**禁止**：与 Grok 并行 Writer；共享跨 Provider Session；未经 Provider Handoff 直接接力。

---

## 2. Task / Run / Session 三层模型

### 2.1 Task（任务层）

Task 是**长期存在的工程单元**，绑定：

- 唯一 `task_id`
- 唯一 worktree + branch
- Manifest（合同引用、写集、验收命令、execution_policy）
- 跨 Run 共享的 `state.json`（Phase、Guard、Repair 计数）
- 产物根目录：`.agentctl/tasks/<task_id>/`

Task 生命周期可能跨越多个 Run、多个 Provider、多个 Session，但**始终只有一个活跃 Writer**。

### 2.2 Run（运行层）

Run 是**一次具体的 Runner  invocation**，例如：

- `run-001-luna-writer`：ICR + 首轮实现
- `run-002-grok-fixer`：第一次定向修复
- `run-003-luna-reviewer`：独立 Review
- `run-004-sol-diagnoser`：根因诊断

每个 Run 记录：

| 字段 | 含义 |
|---|---|
| `run_id` | 单调递增或时间戳 ID |
| `profile` | 使用的逻辑 Profile |
| `runner` | `codex-cli` / `cursor-cli` / `codebuddy-cli` |
| `role` | writer / fixer / reviewer / diagnoser / backup |
| `session_id` | Provider 侧 Session/Thread 标识 |
| `fresh_session` | 是否禁止 Resume 前一 Run |
| `started_at` / `ended_at` | 时间界 |
| `run_status` | QUEUED / RUNNING / SUCCEEDED / FAILED / … |
| `artifacts` | raw-events.jsonl、checkpoint、last-message |

**Run 之间**通过结构化 checkpoint/verify/findings/diagnosis 传递信息，不传递完整 chat history。

### 2.3 Session（会话层）

Session 是 Provider 内部的对话上下文：

- **Writer Session** 可在 IMPLEMENT/FIX（同 Profile 同 Runner）间 Resume
- **Reviewer / Diagnoser / Fixer（Grok）** 默认 **fresh Session**
- **跨 Provider 绝不共享 Session**；只共享 worktree 与结构化 handoff

```text
Task (care-finalize-v1)
├── state.json          ← Phase, Guard, repair counters
├── runs/
│   ├── run-001 …       ← Session A (Luna Writer, resumable)
│   ├── run-002 …       ← Session B (Grok, fresh)
│   └── run-003 …       ← Session C (Luna Reviewer, fresh, read-only)
└── verify/ findings/ diagnosis/
```

---

## 3. Runner Profile

Profile 是**逻辑别名**，把 Manifest 与具体 CLI 模型字符串解耦。项目级定义位于 `tools/agentctl/profiles.json`（运行时也可通过 `AGENTCTL_PROFILES` 覆盖）。

### 3.1 profiles.json 示例

```json
{
  "codex-luna-primary": {
    "runner": "codex-cli",
    "model": "GPT-5.6 Luna Max",
    "reasoning_effort": "high",
    "role": "writer",
    "write_mode": "workspace-write",
    "fresh_session": false
  },
  "codex-luna-review": {
    "runner": "codex-cli",
    "model": "GPT-5.6 Luna Max",
    "reasoning_effort": "high",
    "role": "reviewer",
    "write_mode": "read-only",
    "fresh_session": true
  },
  "cursor-grok45-fixer": {
    "runner": "cursor-cli",
    "model": "Grok 4.5 High Fast",
    "role": "fixer",
    "write_mode": "workspace-write",
    "fresh_session": true
  },
  "codex-sol-xhigh-diagnose": {
    "runner": "codex-cli",
    "model": "GPT-5.6 Sol",
    "reasoning_effort": "xhigh",
    "role": "diagnoser",
    "write_mode": "read-only",
    "fresh_session": true
  },
  "codebuddy-kimi-k3-backup": {
    "runner": "codebuddy-cli",
    "model": "Kimi K3 High",
    "role": "backup",
    "write_mode": "workspace-write",
    "fresh_session": true
  }
}
```

### 3.2 Profile 字段语义

| 字段 | 说明 |
|---|---|
| `runner` | 适配器类型，决定 CLI  argv 与事件 JSONL 解析 |
| `model` | 期望模型显示名；启动后由 doctor/runner 回读校验 |
| `reasoning_effort` | Codex 专用；Cursor/CodeBuddy 可忽略 |
| `role` | 控制 Prompt 模板、写模式与可用命令 |
| `write_mode` | `workspace-write` 或 `read-only` |
| `fresh_session` | `true` 时禁止继承同 Task 上一 Run 的 session_id |

### 3.3 MODEL_MISMATCH Guard

CLI 实际选中模型可能与 Profile 声明略有差异（版本字符串、大小写、别名）。**允许 Preflight 容忍的规则由 `agentctl doctor` 定义**；若启动事件/report 中的模型不在容忍集合内：

```text
guard = MODEL_MISMATCH
run_status = FAILED（或 INTERRUPTED，取决于是否已产生副作用）
phase 保持不变
```

处理：

1. 停止当前 Run，不写入 repair_cycle
2. 记录 expected vs actual 到 `state.json` 与 controller-brief
3. 外部 Sol 决定：修正 Profile、修正 CLI 配置、或 NEEDS_HUMAN

**禁止**静默改用其他模型继续流水线。

### 3.4 legacy-codex-profile 映射

旧 Manifest 仅含 `model` + `reasoning_effort` 时，CP1 起自动映射：

```text
initial_writer_profile = legacy-codex-profile(model, reasoning_effort)
fixer_profile          = cursor-grok45-fixer        （若未显式禁用多 Runner）
reviewer_profile       = codex-luna-review          （同 model Family 时）
diagnoser_profile      = codex-sol-xhigh-diagnose
```

映射只在 `execution_policy` 缺失时生效；显式 `execution_policy` 优先。

---

## 4. 默认多模型流水线

### 4.1 Phase 与流水线对应

| Phase | 激活 Profile | 说明 |
|---|---|---|
| `CONTROL_APPROVED` | — | 外部 Sol 已批准 Manifest |
| `LUNA_IMPLEMENT` | codex-luna-primary | ICR + 实现 |
| `VERIFY` | — | agentctl 确定性命令 |
| `GROK_FIX` | cursor-grok45-fixer | 定向修复 |
| `LUNA_REVIEW` | codex-luna-review | Focused Review |
| `SOL_DIAGNOSE` | codex-sol-xhigh-diagnose | 根因诊断 |
| `GROK_FIX_AFTER_DIAGNOSIS` | cursor-grok45-fixer | 诊断后升级修复 |
| `FINAL_VERIFY` | — | 全量 acceptance_commands |
| `LUNA_FINAL_REVIEW` | codex-luna-review | 最终 Review |
| `READY_FOR_HANDOFF` | — | 见 §13 |
| `DONE` / `NEEDS_HUMAN` | — | 终止态 |

Low 风险任务可跳过 Review 或 Diagnose，但必须在 Manifest `risk` 中声明并经外部 Sol 批准。

### 4.2 状态机（ASCII）

```text
CONTROL_APPROVED
       │
       ▼
LUNA_IMPLEMENT
       │
       ▼
VERIFY ─────────────── PASS ──────────────┐
  │                                       │
  │ CODE_FAILURE                          ▼
  ▼                                  LUNA_REVIEW
GROK_FIX                                  │
  │                              PASS ────┴──→ READY_FOR_HANDOFF
  ▼                                      │
VERIFY                                    │ REQUIRED_FIX
  │                                      ▼
  └───────────────→ GROK_FIX ←────────────┘
                       │
                       ▼
                    VERIFY
                       │
             两轮同签名仍未闭合
                       ▼
                  SOL_DIAGNOSE
                       │
                       ▼
            GROK_FIX_AFTER_DIAGNOSIS
                       │
                       ▼
                  FINAL_VERIFY
                       │
                       ▼
               LUNA_FINAL_REVIEW
                       │
             PASS ─────┴───── FAIL
              │                 │
            DONE       KIMI / HUMAN / SPLIT
```

### 4.3 Verify 分类

每次 `agentctl verify` 生成 `verify/verify-NNN.json`：

```json
{
  "verify_id": "verify-002",
  "task_id": "care-finalize-v1",
  "phase_before": "GROK_FIX",
  "started_at": "2026-08-05T08:12:04Z",
  "finished_at": "2026-08-05T08:14:31Z",
  "classification": "CODE_FAILURE",
  "overall": "FAIL",
  "commands": [
    {
      "command": "go test ./internal/player/actor/... -run CareReward -count=1",
      "exit_code": 1,
      "duration_ms": 8421,
      "log_ref": "verify/verify-002-care-reward.log"
    }
  ],
  "failure_signatures": [
    "TestCareRewardReplayDoesNotDoubleGrant"
  ],
  "failure_signature_hash": "sha256:ab12…",
  "environment_ok": true,
  "provider_ok": true,
  "diff_hash": "sha256:cd34…",
  "changed_files": [
    "internal/player/actor/care_reward.go"
  ],
  "suggested_next_phase": "GROK_FIX"
}
```

**classification 枚举**：

| 值 | 含义 | 下一动作 |
|---|---|---|
| `PASS` | 所有 acceptance_commands 成功 | `LUNA_REVIEW` 或 `READY_FOR_HANDOFF`（视 Phase） |
| `CODE_FAILURE` | 编译/测试/lint/写集违规 | `GROK_FIX`；累计 repair_cycle |
| `ENV_FAILURE` | Redis/端口/Docker/依赖服务不可用 | 重试 Verify；**不计** repair_cycle |
| `PROVIDER_FAILURE` | 14003、认证、CLI 崩溃 | `WAITING_CAPACITY` 或 `fallback`；**不计** repair_cycle |
| `NO_CHANGE` | Fix Run 结束但 Diff 为空 | 记录 Guard；不计有效 repair_cycle |
| `CONTRACT_CONFLICT` | 实现与权威合同不可调和 | 停止；`controller-brief` 标记 NEEDS_HUMAN |

### 4.4 Bug Pack（Fixer 输入）

Grok/Kimi Fixer **只接收 Bug Pack**，由 `agentctl fix --profile …` 或 `next` 在启动 Run 前组装为 `current-handoff.md` + 内联 JSON：

```json
{
  "bug_pack_id": "bp-verify-002",
  "task_id": "care-finalize-v1",
  "source": ["verify-002.json"],
  "goal": "修复 CareRewardOrder 重试时重复增加经验的问题",
  "non_goals": [
    "不修改 Proto/FarmActor/Workflow Runtime/Redis 公共脚本"
  ],
  "failure_signatures": [
    "TestCareRewardReplayDoesNotDoubleGrant"
  ],
  "failed_commands": [
    "go test ./internal/player/actor/... -run CareRewardReplayDoesNotDoubleGrant -count=1"
  ],
  "error_excerpt_ref": "verify/verify-002-care-reward.log#lines=120-180",
  "invariants": [
    "相同 participant_invocation_id + 相同 hash 返回原 Receipt",
    "不得再次增加经验和任务进度"
  ],
  "contract_refs": [
    {
      "path": "docs/design/31-care-reward-contract.md",
      "section": "§3.2 Idempotent replay",
      "sha256": "…"
    }
  ],
  "diff_summary_ref": "runs/run-002-grok-fixer/diff-summary.md",
  "files_allowed": [
    "internal/player/actor/care_reward.go",
    "internal/player/actor/care_reward_test.go"
  ],
  "files_forbidden": [
    "proto/**",
    "internal/workflow/**",
    "internal/storage/redis/**"
  ],
  "required_findings": [],
  "prior_fix_attempts": 1,
  "diagnosis_ref": null
}
```

当 Fix 来自 Review 时，`source` 包含 `findings-001.json`，`required_findings` 非空。

### 4.5 Findings JSON（Reviewer 输出）

```json
{
  "findings_id": "findings-001",
  "task_id": "care-finalize-v1",
  "reviewer_profile": "codex-luna-review",
  "run_id": "run-003-luna-reviewer",
  "reviewed_diff_hash": "sha256:ef56…",
  "reviewed_verify_id": "verify-003",
  "verdict": "REQUIRED_FIX",
  "summary": "Care 重试路径未检查 receipt 已存在，可能 double grant",
  "items": [
    {
      "finding_id": "F-CARE-04",
      "severity": "required",
      "category": "idempotency",
      "title": "Replay 未短路已存在 Receipt",
      "evidence": [
        "internal/player/actor/care_reward.go:HandleReplay L142-L168 未比较 stored receipt hash"
      ],
      "contract_refs": [
        "docs/design/31-care-reward-contract.md#32-idempotent-replay"
      ],
      "suggested_fix": "在 mutation 前查询 receipt store，hash 相等则返回原 Receipt",
      "blocks_handoff": true
    },
    {
      "finding_id": "F-CARE-NIT-01",
      "severity": "nit",
      "category": "style",
      "title": "日志字段命名不一致",
      "blocks_handoff": false
    }
  ],
  "required_finding_ids": ["F-CARE-04"],
  "passes_without_required": false
}
```

**verdict 枚举**：`PASS` | `REQUIRED_FIX` | `CONTRACT_CONFLICT`

仅 `severity=required` 且 `blocks_handoff=true` 的项进入 Bug Pack 的 `required_findings`。

### 4.6 Checkpoint JSON

**implementation-checkpoint.json**（Luna Writer）：

```json
{
  "checkpoint_type": "implementation",
  "task_id": "care-finalize-v1",
  "run_id": "run-001-luna-writer",
  "phase": "LUNA_IMPLEMENT",
  "commit_hint": "未提交；worktree HEAD abc1234",
  "diff_hash": "sha256:…",
  "changed_files": ["internal/player/actor/care_reward.go"],
  "claims": [
    "已实现 CareReward replay 短路",
    "已添加 TestCareRewardReplayDoesNotDoubleGrant"
  ],
  "open_risks": [
    "未验证 Redis fault 注入路径"
  ],
  "requested_next": "VERIFY"
}
```

**fix-checkpoint.json**（Grok Fixer）：

```json
{
  "checkpoint_type": "fix",
  "task_id": "care-finalize-v1",
  "run_id": "run-002-grok-fixer",
  "bug_pack_id": "bp-verify-002",
  "addressed_signatures": ["TestCareRewardReplayDoesNotDoubleGrant"],
  "addressed_findings": [],
  "diff_hash": "sha256:…",
  "changed_files": ["internal/player/actor/care_reward.go"],
  "claims": [
    "在 mutation 前增加 receipt hash 比较"
  ],
  "requested_next": "VERIFY"
}
```

---

## 5. 修复周期与升级条件

### 5.1 计数器定义

| 计数器 | 何时 +1 | 何时不计 |
|---|---|---|
| `repair_cycle_count` | Bug Pack → Fix Run 结束 → Verify 执行（无论 PASS/FAIL） | ENV/PROVIDER/NO_CHANGE |
| `same_failure_signature_count` | Verify FAIL 且 `failure_signature_hash` 与上一轮相同 | 签名变化 |
| `review_round_count` | 每次 LUNA_REVIEW / LUNA_FINAL_REVIEW 完成 | — |
| `post_diagnosis_fix_count` | GROK_FIX_AFTER_DIAGNOSIS 完成一次 | — |
| `diagnosis_count` | SOL_DIAGNOSE 完成并写出 diagnosis.json | — |

**有效修复周期**定义：

```text
明确 Bug Pack / Required Findings
  → Grok（或 Kimi backup）修改
  → agentctl verify
  → （若 PASS）Luna Review
```

### 5.2 升级至 Sol Diagnose

当满足以下全部条件时，`suggested_next_phase = SOL_DIAGNOSE`：

```text
repair_cycle_count >= max_fix_cycles_before_diagnosis   （默认 2）
AND same_failure_signature_count >= 2
AND classification 最近一次为 CODE_FAILURE
AND diagnosis_count == 0   （同一签名未诊断过）
```

外部 Sol 可通过 controller-brief 提前批准或推迟诊断。

### 5.3 diagnosis.json

```json
{
  "diagnosis_id": "diagnosis-001",
  "task_id": "care-finalize-v1",
  "run_id": "run-004-sol-diagnoser",
  "failure_signatures": ["TestCareRewardReplayDoesNotDoubleGrant"],
  "root_cause": "Player actor 在 replay 分支仍调用 ApplyProgress，未区分 duplicate invocation",
  "why_previous_fixes_failed": [
    "第一轮只在 HTTP handler 层短路，workflow 内仍重复 emit",
    "第二轮补丁检查了 receipt id 但未比较 payload hash"
  ],
  "minimal_fix_plan": [
    "在 care_reward.go Apply 入口比较 invocation hash",
    "Extract 纯函数 IsDuplicateReceipt 并补 table-driven test",
    "禁止在 duplicate 路径 touch task progress store"
  ],
  "files_allowed": [
    "internal/player/actor/care_reward.go",
    "internal/player/actor/care_reward_test.go"
  ],
  "files_forbidden": [
    "proto/**",
    "internal/workflow/**"
  ],
  "new_tests_required": [
    "TestCareRewardReplayDoesNotDoubleGrant",
    "TestCareRewardReplayHashMismatchStillMutates"
  ],
  "contract_conflict": false,
  "confidence": "high",
  "recommended_fixer_profile": "cursor-grok45-fixer"
}
```

若 `contract_conflict: true`：

- Phase → `NEEDS_HUMAN`
- 禁止自动 GROK_FIX_AFTER_DIAGNOSIS
- controller-brief 必须列出冲突合同章节

### 5.4 诊断后修复（严格一轮）

```text
max_post_diagnosis_fix_cycles = 1   （默认）
```

流程：

```text
SOL_DIAGNOSE → diagnosis-001.json
  → GROK_FIX_AFTER_DIAGNOSIS（Bug Pack 必须 embed diagnosis_ref）
  → FINAL_VERIFY
  → LUNA_FINAL_REVIEW
```

仍失败则**不**进入无限 Grok↔Luna↔Sol 环；controller-brief `suggested_next_actions` 仅允许：

- `KIMI_CHALLENGE_READONLY`
- `KIMI_BACKUP_WRITER_ONCE`
- `SPLIT_TASK`
- `ROLLBACK_CHECKPOINT`
- `NEEDS_HUMAN`

### 5.5 execution_policy 默认阈值

```json
{
  "max_fix_cycles_before_diagnosis": 2,
  "max_post_diagnosis_fix_cycles": 1,
  "require_fresh_review_session": true,
  "automatic_provider_switch": false
}
```

`automatic_provider_switch: false` 表示 Provider 后备必须经 `agentctl fallback` 或外部 Sol 显式批准。

---

## 6. CodeBuddy / Kimi 后备策略

Kimi **不建议**排在每个任务的固定流水线末尾；仅在需要时启用。

### 6.1 Provider 后备（Fixer 替换）

**触发条件**（任一）：

- Cursor Grok 长时间不可用（CLI 错误、账号容量、区域限制）
- Cursor CLI 本身故障
- `PROVIDER_FAILURE` 连续 N 次（默认 N=3，可配置）

**切换步骤**（必须顺序执行）：

1. `agentctl stop` 或等价方式停止 Grok Run
2. 确认无测试/工具子进程仍在 worktree 运行
3. 记录当前 `diff_hash` 到 `state.json`
4. 生成 `provider-handoff.json`（与 Bug Pack 同构 + `from_profile` / `to_profile`）
5. `agentctl fallback --profile codebuddy-kimi-k3-backup`
6. 新 Session 启动；**不**继承 Grok session_id

### 6.2 独立挑战者（只读）

Sol 诊断后升级修复仍失败时，优先：

```text
agentctl fallback --profile codebuddy-kimi-k3-backup --read-only --challenge diagnosis-001
```

Prompt 约束：

- 只分析指定 `failure_signature` / 单测
- 判断 Sol `root_cause` 是否遗漏
- 输出 `challenge-report.json`，**不**默认改代码

外部 Sol 阅读 challenge 报告后，决定是否授予 **一次** `KIMI_BACKUP_WRITER` 写权限。

### 6.3 provider-handoff.json 示例

```json
{
  "handoff_id": "ph-001",
  "task_id": "care-finalize-v1",
  "from_profile": "cursor-grok45-fixer",
  "to_profile": "codebuddy-kimi-k3-backup",
  "reason": "CURSOR_CAPACITY_EXhaustED",
  "diff_hash_at_handoff": "sha256:…",
  "bug_pack_ref": "bp-verify-002",
  "active_processes_cleared": true,
  "timestamp": "2026-08-05T10:22:00Z"
}
```

---

## 7. Agent 交互：结构化产物目录

### 7.1 目录布局

```text
.agentctl/tasks/<task_id>/
├── task.json                 # Manifest 快照 + 注册 hash
├── brief.md                  # 人类可读任务摘要（≤500 tokens 级）
├── refs.md                   # 权威输入索引
├── state.json                # Phase / Guard / Repair 计数 / 当前 Run 指针
├── controller-brief.json     # 给外部 Sol 的短摘要（CP6 自动生成）
├── controller-brief.md
├── current-handoff.md        # 当前 Run 启动材料（Bug Pack 渲染）
│
├── runs/
│   ├── run-001-luna-writer/
│   │   ├── raw-events.jsonl
│   │   ├── last-message.md
│   │   ├── implementation-checkpoint.json
│   │   └── runner.meta.json      # profile, session_id, model_actual
│   ├── run-002-grok-fixer/
│   │   ├── raw-events.jsonl
│   │   ├── fix-checkpoint.json
│   │   └── diff-summary.md
│   ├── run-003-luna-reviewer/
│   └── run-004-sol-diagnoser/
│
├── verify/
│   ├── verify-001.json
│   └── verify-001-*.log
│
├── findings/
│   └── findings-001.json
│
├── diagnosis/
│   └── diagnosis-001.json
│
└── handoffs/
    └── provider-handoff-001.json
```

全局状态目录（并发锁、注册表）仍可在 `~/.local/state/ac1-agentctl/` 或 `AGENTCTL_STATE_DIR`；**任务级产物**统一落在 worktree 或 repo 内 `.agentctl/tasks/` 以便 Review 与 Handoff 同库审计。

### 7.2 交互关系

```text
Luna Writer     → implementation-checkpoint.json
agentctl verify → verify-NNN.json
Grok Fixer      ← Bug Pack (+ diagnosis_ref 若 post-diagnosis)
                → fix-checkpoint.json
Luna Reviewer   ← Diff + verify + checkpoint
                → findings-NNN.json
Sol Diagnoser   ← verify×2 + findings + fix history
                → diagnosis-NNN.json
agentctl        → controller-brief.json（汇总）
External Sol    ← controller-brief
                → 批准 next / 修订 Manifest
```

### 7.3 controller-brief.json 示例

```json
{
  "task_id": "CARE-FINALIZE",
  "phase": "VERIFY_FAILED",
  "run_status": "SUCCEEDED",
  "guard": null,
  "current_profile": "cursor-grok45-fixer",
  "current_run_id": "run-002-grok-fixer",
  "repair_cycle_count": 2,
  "same_failure_signature_count": 2,
  "review_round_count": 0,
  "post_diagnosis_fix_count": 0,
  "diagnosis_count": 0,
  "failed_commands": [
    "go test ./internal/player/actor/... -run CareReward"
  ],
  "failure_signatures": [
    "TestCareRewardReplayDoesNotDoubleGrant"
  ],
  "required_findings": [],
  "last_verify_id": "verify-002",
  "last_classification": "CODE_FAILURE",
  "diff_hash": "sha256:cd34…",
  "worktree": "/data/home/developer_026/CodeBuddy/20260519141303/farm-game-care-finalize",
  "branch": "codex/care-finalize-20260805",
  "suggested_next_actions": [
    "SOL_DIAGNOSE",
    "NEEDS_HUMAN"
  ],
  "generated_at": "2026-08-05T09:45:12Z"
}
```

外部 Sol **只需阅读此文件与必要产物引用**，无需复制 raw JSONL。

---

## 8. 状态模型

v1.2 延续 v1.1 的**正交状态**设计：Phase（业务阶段）、Run Status（运行态）、Guard（阻塞原因）、Repair State（计数器）分开存储。

### 8.1 Phase（扩展）

在 v1.1 `ICR → IMPLEMENT → FOCUSED_REVIEW → FIX → VERIFY → PREMERGE → MERGE → DONE` 基础上，多模型 Phase 增加细粒度别名，可与 legacy Phase 映射：

| v1.2 Phase | 映射 legacy | 说明 |
|---|---|---|
| `CONTROL_APPROVED` | `READY` | 已注册待启动 |
| `LUNA_IMPLEMENT` | `IMPLEMENT` / `ICR` | Writer 实现 |
| `GROK_FIX` | `FIX` | Grok 定向修复 |
| `LUNA_REVIEW` | `FOCUSED_REVIEW` | 独立 Review |
| `SOL_DIAGNOSE` | `FIX`（子状态） | 只读诊断 |
| `GROK_FIX_AFTER_DIAGNOSIS` | `FIX` | 诊断后修复 |
| `FINAL_VERIFY` | `VERIFY` | 终验 |
| `LUNA_FINAL_REVIEW` | `FOCUSED_REVIEW` | 终审 |
| `READY_FOR_HANDOFF` | `PREMERGE` 前态 | 见 §13 |
| `VERIFY` | `VERIFY` / `VERIFYING` | 确定性验证 |
| `DONE` | `DONE` | 完成 |
| `NEEDS_HUMAN` | — | 需人工 |

### 8.2 Run Status

```text
QUEUED
RUNNING
WAITING_CAPACITY      # 14003 / Provider 限流
SUSPECTED_STALL       # 无结构化进展；不自动 kill
INTERRUPTED           # SSH/进程异常；可 reconcile
SUCCEEDED
FAILED
CANCELLED
```

Run Status 描述**当前 Run**；Task 级 `state.json` 另存 `active_run_id`。

### 8.3 Guard 状态

```text
CONTEXT_STALE
BASE_STALE
DEPENDENCY_STALE
LOCK_CONFLICT
POLICY_VIOLATION      # 写集/semantic lock 越界
MODEL_MISMATCH        # 实际模型 ≠ Profile 期望
NEEDS_HUMAN
CONTRACT_CONFLICT
```

Guard 与 Phase 正交。例如：

```text
phase = GROK_FIX
run_status = WAITING_CAPACITY
guard = null
```

容量等待**不**改变 Phase，也不增加 `repair_cycle_count`。

### 8.4 Repair State（state.json 片段）

```json
{
  "repair_cycle_count": 2,
  "same_failure_signature_count": 2,
  "last_failure_signature_hash": "sha256:ab12…",
  "review_round_count": 1,
  "post_diagnosis_fix_count": 0,
  "diagnosis_count": 0,
  "last_diagnosis_id": null,
  "failure_count_legacy": 2
}
```

`failure_count_legacy` 仅用于 CP1–CP3 间兼容旧 CLI 展示；新逻辑以 repair 计数为准。

---

## 9. Manifest 与 execution_policy

### 9.1 完整 Manifest 示例

```json
{
  "task_id": "care-finalize-v1",
  "repo": "/data/home/developer_026/CodeBuddy/20260519141303/farm-game",
  "worktree": "/data/home/developer_026/CodeBuddy/20260519141303/farm-game-care-finalize",
  "branch": "codex/care-finalize-20260805",
  "base_commit": "9adc81d21e3ede7abbd0fd5df884ab54c7217700",
  "execution_policy": {
    "initial_writer_profile": "codex-luna-primary",
    "fixer_profile": "cursor-grok45-fixer",
    "reviewer_profile": "codex-luna-review",
    "diagnoser_profile": "codex-sol-xhigh-diagnose",
    "backup_profile": "codebuddy-kimi-k3-backup",
    "max_fix_cycles_before_diagnosis": 2,
    "max_post_diagnosis_fix_cycles": 1,
    "require_fresh_review_session": true,
    "automatic_provider_switch": false
  },
  "prompt_file": "prompts/care-finalize-v1.md",
  "owned_write_set": [
    "internal/player/actor/care_reward.go",
    "internal/player/actor/care_reward_test.go"
  ],
  "forbidden_write_set": [
    "proto/**",
    "internal/workflow/**",
    "internal/storage/redis/**"
  ],
  "semantic_locks": ["workflow:care-reward"],
  "risk": "high",
  "dependencies": [],
  "authoritative_inputs": [
    {
      "path": "docs/design/31-care-reward-contract.md",
      "section": "§3 Idempotent replay"
    }
  ],
  "acceptance_commands": [
    "go test ./internal/player/actor/... -run CareReward -count=1",
    "git diff --check"
  ],
  "review_scope": [
    "internal/player/actor/care_reward.go",
    "internal/player/actor/care_reward_test.go"
  ]
}
```

Schema 规则（`manifest.schema.json`）：

- `model` + `reasoning_effort` **或** `execution_policy` 二选一必填
- 存在 `execution_policy` 时，Profile 名必须在 `profiles.json` 中存在
- CP1 起 register 时校验 Profile；Run 时校验 MODEL_MISMATCH

### 9.2 Legacy 映射规则

| Legacy 字段 | 映射 |
|---|---|
| `"model": "Luna Max", "reasoning_effort": "high"` | `initial_writer_profile = codex-luna-primary`（或动态 legacy 条目） |
| `"model": "Sol", "reasoning_effort": "xhigh"` | 仅 Writer 任务时使用；Diagnoser 仍走 `codex-sol-xhigh-diagnose` |
| 无 execution_policy | 单 Codex Writer 行为与 v1.1 一致；`fix`/`resume` 仍可用 |

动态 legacy Profile 命名：`legacy-codex-writer-<model-slug>-<effort>`，由 CP1 `agentctl register` 自动 materialize 到 task-local overlay。

---

## 10. agentctl 命令

以下命令在 v1.2 设计下扩展；**CP1 仅保证 register/start/resume/status 与 Profile 解析兼容**，其余命令按 CP2–CP6 递次落地。

### 10.1 命令一览

| 命令 | 作用 |
|---|---|
| `agentctl register <manifest>` | 校验 Manifest/Profile；写入 task.json；legacy 映射 |
| `agentctl start <task_id>` | 启动 initial_writer_profile（默认 LUNA_IMPLEMENT） |
| `agentctl verify <task_id>` | 运行 acceptance_commands；写 verify-NNN.json；更新 Phase/计数 |
| `agentctl fix <task_id> [--profile PROFILE] [--prompt]` | 组装 Bug Pack；启动 Fixer Run（默认 fixer_profile） |
| `agentctl review <task_id> [--profile]` | 启动 fresh Luna Reviewer；写 findings |
| `agentctl diagnose <task_id> [--profile]` | 启动 Sol Diagnoser；写 diagnosis.json |
| `agentctl fallback <task_id> --profile … [--read-only]` | Provider 后备 / Kimi challenge |
| `agentctl runs <task_id>` | 列出 runs/ 下所有 Run 摘要 |
| `agentctl show <task_id>` | 人类可读详情：Phase、Guard、计数、最近 verify/findings |
| `agentctl status [task_id]` | 轻量状态；无 task_id 时 list 活跃任务 |
| `agentctl resume <task_id>` | Resume **Writer** Session（同 Profile）；不用于 Reviewer |
| `agentctl stop <task_id>` | 停止 tmux/Runner 进程；保留 Git 与产物 |
| `agentctl next <task_id> [--action ACTION]` | 按 state 与 policy 建议启动下一 Phase（可需外部 Sol 批准位） |

保留 v1.1 命令：`doctor`、`list`、`capture`、`watch-once`、`dispatch-once`、`checkpoint`/`advance`（legacy Phase 兼容）。

### 10.2 verify

```bash
agentctl verify care-finalize-v1
agentctl verify care-finalize-v1 --json
```

行为：

1. 检查 ENV preflight（Redis/compose 可选探针）
2. 顺序执行 Manifest `acceptance_commands`
3. 写集 Gate：真实 `git diff` vs owned/forbidden
4. 分类结果写入 verify-NNN.json
5. 更新 `repair_cycle_count` / `same_failure_signature_count`（仅 CODE_FAILURE 路径）
6. 推导 `suggested_next_phase`；CP6 起刷新 controller-brief

### 10.3 fix --profile

```bash
agentctl fix care-finalize-v1
agentctl fix care-finalize-v1 --profile cursor-grok45-fixer
agentctl fix care-finalize-v1 --profile codebuddy-kimi-k3-backup
```

- 默认 `--profile` = Manifest `fixer_profile`
- 启动前写 `current-handoff.md`；Run 结束必须产出 fix-checkpoint.json 或标记 NO_CHANGE
- `--profile` 与 execution_policy 不一致时要求 `--force` + 外部 Sol 已在 brief 中批准

### 10.4 review

```bash
agentctl review care-finalize-v1
```

- 强制 `fresh_session=true`
- sandbox `read-only`
- 输入：最近 PASS 的 verify、Diff、Manifest review_scope
- 输出：findings-NNN.json；`review_round_count += 1`

### 10.5 diagnose

```bash
agentctl diagnose care-finalize-v1
```

- 前置：`repair_cycle_count >= max_fix_cycles_before_diagnosis`（或 `--force`）
- 只读；输出 diagnosis-NNN.json
- 若 `contract_conflict` → Phase=`NEEDS_HUMAN`

### 10.6 fallback

```bash
agentctl fallback care-finalize-v1 --profile codebuddy-kimi-k3-backup
agentctl fallback care-finalize-v1 --profile codebuddy-kimi-k3-backup --read-only --challenge diagnosis-001
```

执行 §6 Provider Handoff 步骤。

### 10.7 runs / show / status

```bash
agentctl runs care-finalize-v1 --json
agentctl show care-finalize-v1
agentctl status
agentctl status care-finalize-v1 --json
```

`show` 额外展示：最近 3 个 verify 分类、open required findings、diagnosis 摘要、controller-brief 路径。

### 10.8 resume / stop / next

```bash
agentctl resume care-finalize-v1          # 仅 Writer Profile
agentctl stop care-finalize-v1
agentctl next care-finalize-v1            # 按 suggested_next_phase 执行
agentctl next care-finalize-v1 --action SOL_DIAGNOSE
```

`next` 在 CP4 前可仅为 dry-run 建议；CP6 起与 controller-brief 联动。

---

## 11. Single Writer 与 Review 隔离

### 11.1 Single Writer 原则

- 同一 Task 同一时刻最多一个 `write_mode=workspace-write` 的活跃 Run
- Luna Writer 与 Grok Fixer **串行**；Fixer 完成后 Writer Session 可 Resume 继续同一 Profile 的后续 IMPLEMENT，但不得在 Grok 仍在运行时 Resume Luna Writer
- semantic lock 租约与 v1.1 相同：Fixer 运行期间仍持有 Task 级 Writer 租约

### 11.2 Review 隔离

| 要求 | 实现 |
|---|---|
| Fresh Session | `codex-luna-review.fresh_session=true`；`agentctl review` 拒绝传入 writer session_id |
| Read-only | Codex `--sandbox read-only`；Hook/Gate 双检 |
| 输入最小化 | Prompt 只含 Manifest、Diff、verify、checkpoint；**不含** Writer raw-events.jsonl |
| 输出结构化 | 必须写 findings-NNN.json；缺文件则 Run=FAILED |
| Fix 闭环 | REQUIRED_FIX 只通过 Bug Pack 进入 Grok；Reviewer 不得直接改代码 |

### 11.3 Review 重复拒绝（继承 v1.1）

同一 `reviewed_diff_hash` 且 open required findings 未变化时，拒绝重复 Review Run，避免 Token 浪费。Diff 变化或 Verify 更新后 hash 变化方可重新 Review。

---

## 12. 失败分类

所有失败必须归入以下四类之一，决定计数与恢复策略。

### 12.1 Provider / Capacity（PROVIDER_FAILURE）

- Codex `14003`、rate limit、overload
- Cursor 账号容量、Grok 不可用
- CodeBuddy 认证失败、token 过期
- CLI 崩溃、MODEL_MISMATCH

**策略**：`WAITING_CAPACITY` → 指数退避；可 `fallback`；**不**增加 repair_cycle；外部 Sol 可选换 Provider。

### 12.2 Environment（ENV_FAILURE）

- 测试 Redis / Tcaplus / compose 未就绪
- 端口占用、Docker 环境污染
- 磁盘满、worktree 权限
- 网络瞬断导致依赖 clone 失败

**策略**：重试 Verify；修复环境；**不**增加 repair_cycle；若长期失败 → NEEDS_HUMAN。

### 12.3 Code（CODE_FAILURE）

- 编译错误、单测失败、race 失败
- lint、写集 Gate 失败
- Review REQUIRED_FIX（进入 Bug Pack）
- NO_CHANGE（Fixer 未产生有效 Diff）

**策略**：GROK_FIX 环；累计 repair_cycle 与 failure_signature；达阈值 → SOL_DIAGNOSE。

### 12.4 Contract（CONTRACT_CONFLICT）

- 实现与 authoritative_inputs 不可调和
- Sol diagnosis 标记 `contract_conflict: true`
- Reviewer verdict=CONTRACT_CONFLICT

**策略**：立即停止自动修复；Phase=NEEDS_HUMAN；外部 Sol 决定是否修订合同、缩小任务或回滚。

### 12.5 分类矩阵

| 现象 | 分类 | repair_cycle | 典型下一动作 |
|---|---|---|---|
| 单测 assert 失败 | CODE_FAILURE | +1 | GROK_FIX |
| 14003 | PROVIDER_FAILURE | 0 | WAITING_CAPACITY / resume |
| Redis connection refused | ENV_FAILURE | 0 | 重试 verify |
| Reviewer 发现幂等漏洞 | CODE_FAILURE | +1（Fix 后） | GROK_FIX |
| Sol 认定合同缺字段 | CONTRACT | 0 | NEEDS_HUMAN |
| Grok 退出但 git diff 空 | NO_CHANGE / CODE | 0 | NEEDS_HUMAN 或换 Pack |

---

## 13. READY_FOR_HANDOFF 成功标准

Task 进入 `READY_FOR_HANDOFF` **必须**同时满足：

1. **Phase 路径完整**（High 风险）：`LUNA_IMPLEMENT → VERIFY(PASS) → LUNA_REVIEW(PASS) → …`；若曾 CODE_FAILURE，则诊断后路径 `SOL_DIAGNOSE → GROK_FIX_AFTER_DIAGNOSIS → FINAL_VERIFY(PASS) → LUNA_FINAL_REVIEW(PASS)` 已完成
2. **Verify**：最近一次 `FINAL_VERIFY` 或等价终验 `classification=PASS`；所有 `acceptance_commands` 成功
3. **Findings**：无 open `severity=required` 且 `blocks_handoff=true` 的项；NIT 可登记但不阻塞
4. **写集**：真实 Diff ⊆ owned_write_set；无 forbidden 路径；semantic lock 无冲突
5. **Context/Base/Dependency**：无 CONTEXT_STALE / BASE_STALE / DEPENDENCY_STALE Guard
6. **MODEL**：所有已完成 Run 无未解决的 MODEL_MISMATCH
7. **产物齐全**：`implementation-checkpoint.json`、最新 verify、最终 findings（若 Review 必须）、diagnosis（若曾触发）路径可在 Handoff 引用
8. **Git**：worktree 干净或仅含预期变更；**尚未**要求 agentctl 自动 commit（仍由人工/integration 流程提交）
9. **controller-brief**：`suggested_next_actions` 含 `READY_FOR_HANDOFF`；外部 Sol 已显式批准（CP6 前为人工确认）

Handoff 文档（项目 verification 模板）应链接：

- `task_id`、branch、worktree HEAD
- verify-NNN.json 摘要
- findings 最终 verdict
- 已知 open nits
- 未跑的非 acceptance E2E（若有）

---

## 14. 有界迁移 CP1–CP6（已完成）

改造**不需要重写** agentctl；在现有 `CodexAdapter`、`failure_count`、fix/resume、写集 Gate 上增量演进。

| 阶段 | 交付 | 兼容性 |
|---|---|---|
| **CP1** | `CodexAdapter` → `RunnerAdapter` 抽象；Task 状态字段 `runner_session_id` / `profile`；`profiles.json`；Manifest `execution_policy` + legacy 映射；register 校验 | 旧 Manifest 仍可 start；行为与 v1.1 一致 |
| **CP2** | `CursorCliRunner`；`agentctl fix --profile cursor-grok45-fixer`；Bug Pack 组装；tmux/JSONL 解析 | Codex-only 任务不受影响 |
| **CP3** | 单 Task 多 Run 目录；`runs/` 索引；fresh Luna Reviewer；`findings-*.json` | Review 仍可选；legacy 单 Run 状态迁移 |
| **CP4** | `failure_signature`；`repair_cycle_count`；`agentctl verify` 分类；`diagnose`；diagnosis.json | `failure_count_legacy` 并行展示 |
| **CP5** | `CodeBuddyCliRunner`；`fallback`；provider-handoff | 默认不启用 Kimi |
| **CP6** | 自动生成 `controller-brief`；`next` 半自动编排；与外部 Sol 工作流文档对齐 | 全自动 merge 仍不在范围 |

CP1～CP6 已在同一实现中完成；当前默认仍是人工批准跨 Provider 切换，不启用自动 fallback、自动提交或自动合并。

### 14.1 实现代码触点

- `tools/agentctl/agentctl.py`：`RunnerAdapter` 协议、`CodexCliRunner` 实现、Profile 加载
- `tools/agentctl/profiles.json`：逻辑 Profile 真值
- `tools/agentctl/schema/manifest.schema.json`：`execution_policy`
- `docs/design/74-agent-control-plane-multi-model-v1.2.md`：本文档

### 14.2 非目标（任一 CP 均不做）

- SQLite 单写 Supervisor
- 自动 commit/push/merge
- Web UI / 分布式调度
- 按成本自动选择任意模型
- 修改 26～37 号权威合同正文

---

## 15. Pilots

### 15.1 第一轮：Care Finalize — Reviewer / Fixer Pilot

**时机**：Care Writer 任务结束，**不中途切换**正在运行的旧 Agent。

**步骤**：

1. 新 Luna Session → `agentctl review`（Focused Review）
2. 若有 REQUIRED_FIX → 组装 Bug Pack → `agentctl fix`（Grok）
3. `agentctl verify`
4. 新 Luna Final Review

**证明目标**：

- Cursor Runner 可写且写集 Gate 有效
- Luna Reviewer 不改文件
- Findings 准确传入 Grok
- 不同 Session 的 raw-events 不混淆

### 15.2 第二轮：小 Bug — 两轮升级机制

选择可复现的局部失败（非 Theft 主链）：

```text
Luna 实现
  → Grok Fix 1 → VERIFY FAIL
  → Grok Fix 2 → VERIFY FAIL（同签名）
  → agentctl diagnose → diagnosis.json
  → Grok 升级修复 → FINAL_VERIFY
```

验证 `repair_cycle_count`、`same_failure_signature_count`、Phase 转换与 controller-brief 建议。

### 15.3 第三轮：Theft — 完整流水线

```text
Luna ICR + 实现
  → VERIFY
  → Grok Fix（按需）
  → Luna Review
  → 最多 2 轮 repair
  → Sol Diagnose（若仍失败）
  → Grok 升级修复
  → Luna Final Review
```

Kimi 仅在 Provider 故障或 Sol 后仍失败时**手动**启用。

### 15.4 不推荐

- 在 Care P1 活跃 Writer 上热切换 CP2+
- 用 Theft 主链「试控制器」而故意引入无关失败
- 无 Bug Pack 约束的 Grok 全仓重写

---

## 16. 评估指标

### 16.1 任务级指标

| 指标 | 定义 | 目标趋势 |
|---|---|---|
| `time_to_first_pass_verify` | 从 start 到首次 VERIFY PASS | 下降 |
| `repair_cycles_per_task` | 有效 repair_cycle 总数 | 可控（P50 ≤2） |
| `diagnosis_rate` | 触发 SOL_DIAGNOSE 的任务占比 | 低但非零 |
| `post_diagnosis_success_rate` | 诊断后一轮 Fix 终验 PASS 比例 | 高 |
| `review_required_fix_rate` | Review 产出 REQUIRED_FIX 比例 | 随阶段下降 |
| `handoff_rework_rate` | READY 后/integration 又被打回 | 下降 |

### 16.2 控制面指标

| 指标 | 定义 |
|---|---|
| `provider_failure_ratio` | PROVIDER_FAILURE / 全部 verify |
| `env_failure_ratio` | ENV_FAILURE / 全部 verify |
| `model_mismatch_count` | MODEL_MISMATCH Guard 次数 |
| `no_change_fix_rate` | Fix Run 无 Diff 比例 |
| `session_cross_contamination` | Reviewer 错误绑定 Writer session（应为 0） |
| `mean_controller_brief_tokens` | 外部 Sol 阅读成本 |

### 16.3 质量门禁

Pilot 通过阈值（建议）：

- Care Pilot：`findings → grok → verify` 闭环 **≥1 次**成功，且无写集越界
- 升级 Pilot：同签名 2 轮失败后 **100%** 触发 diagnose 建议
- Theft Pilot：完整流水线 **≥1 个** task READY_FOR_HANDOFF，integration 测试无回归

---

## 17. 最终原则

1. **控制面不写游戏逻辑**：agentctl 只编排工程 Run；业务合同仍在文档与代码审查中。
2. **确定性优先于模型直觉**：测试与 Gate 的 PASS/FAIL 高于 Agent 自述「已完成」。
3. **快修与深诊分离**：Grok 快修表象；Sol 只读深诊；禁止 Sol 直接改代码。
4. **Review 必须独立 Session**：同模型可以，同 Session 不行。
5. **Fix 必须定向**：Bug Pack 限制目标、写集与不变量；禁止全仓重规划。
6. **升级可计数、可审计**：repair_cycle 与 failure_signature 可回放；Provider/环境问题不污染技术失败计数。
7. **Provider 可替换、Session 不可共享**：Kimi 是后备与挑战者，不是默认第五棒。
8. **有界迁移**：CP1–CP6 增量交付；不破坏运行中 Care 任务；legacy Manifest 继续工作。
9. **人工 Sol 保留最终权威**：controller-brief 服务人做决策，而非无人值守无限循环。
10. **Handoff 可验证**：READY_FOR_HANDOFF 是证据状态，不是 Agent 自我声明。

---

## 附录 A：与 v1.1 文档对照

| 主题 | v1.1 | v1.2 本文 |
|---|---|---|
| Runner 数量 | 仅 Codex | Codex + Cursor + CodeBuddy |
| 模型切换 | 同 Thread Luna→Sol | 多 Run 多 Profile + Diagnose |
| Review | Codex read-only | 强制 fresh Session + Findings 文件 |
| Fix | resume writer | Grok Bug Pack Fixer |
| 状态 | Phase + Run Status + Guard | 增加 Repair 计数与 v1.2 Phase |
| 实施 | AC1 MVP 历史基线 | CP1–CP6 已完成并通过真实 Pilot |

## 附录 B：参考实现路径

```text
farm-game/
├── tools/agentctl/
│   ├── agentctl.py
│   ├── profiles.json
│   └── schema/manifest.schema.json
└── docs/design/
    └── 74-agent-control-plane-multi-model-v1.2.md
```

## 附录 C：相关文档

- `docs/design/74-agent-control-plane-architecture.md` — v1.1 现状架构
- `docs/verification/agent-controller-handoff.md` — AC1 已交付行为证据
- `docs/design/73-linux-multi-agent-dispatch-runbook.md` — 任务派发 runbook

---

*文档结束。*
