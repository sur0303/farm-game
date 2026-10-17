# Agentctl Multi-Model CP3–CP6 Handoff（2026-08-05）

分支：`codex/agentctl-multi-model-cp1-20260805`
工作树：`farm-game/.worktrees/agentctl-mm-cp1`
设计：`docs/design/74-agent-control-plane-multi-model-v1.2.md`
前置：CP1 / CP2 handoff（同目录）

## 本轮目标

把控制面从「verify + Cursor fixer」补齐到设计里的多模型闭环：

`verify → Luna fresh review → Grok fix（≤2）→ Sol diagnose → Grok post-diagnosis fix → 可选 Kimi backup`

并给外部 Sol 控制器可读的 brief / next 建议。**不迁移**正在跑的 Care Writer。

## 已交付

### CP3 · Multi-run + Luna Review

- 每次 launch 绑定 `tasks/<task>/runs/run-NNN-<profile-slug>/`
  - `raw-events.jsonl` / `runner.log` / `last-message.md` / `exit-code` / `runner.pid` / `meta.json`
  - 任务根目录保留 `codex.jsonl` 等兼容 symlink
- `agentctl review TASK [--profile codex-luna-review]`
  - 要求最新 verify=`PASS`
  - 强制 fresh session + read-only
  - 写 Review Pack，seed `findings/findings-NNN.json`
- `watch-once` harvest Findings；diff fingerprint 变化则 `INVALIDATED` / `POLICY_VIOLATION`
- CLI：`runs TASK` 列出 Run 产物

### CP4 · Repair counters + Sol Diagnose

- verify-after-fixer 路径更新分类计数：
  - `repair_cycle_count`
  - `same_failure_signature_count`（基于 failure signatures）
  - `post_diagnosis_fix_count`（有 diagnosis 后再 fix）
- `agentctl diagnose TASK [--profile codex-sol-xhigh-diagnose]`
  - 门禁：`repair_cycle_count >= max_fix_cycles_before_diagnosis`（默认 2）
  - read-only Sol xhigh；写 Diagnosis Pack
- 不再用裸 `failure_count` 驱动诊断建议（字段保留兼容）

### CP5 · CodeBuddy / Kimi backup

- `CodeBuddyCliRunner`（`codebuddy-cli`，`cli_model=kimi-k3-ioa`）
- `agentctl fallback TASK --reason "..." [--profile codebuddy-kimi-k3-backup]`
  - **仅手动**；`--reason` 必填；无静默 provider 切换
- `doctor` 检查 codebuddy；`IMPLEMENTED_RUNNERS` 含 `codex-cli` / `cursor-cli` / `codebuddy-cli`

### CP6 · Controller brief + next

- 自动写 `controller-brief.json` / `controller-brief.md`
- `agentctl next TASK` → `suggested_next_actions`，例如：
  - `SUGGEST_GROK_FIX` / `SUGGEST_LUNA_REVIEW` / `SUGGEST_SOL_DIAGNOSE`
  - `SUGGEST_CODEBUDDY_BACKUP` / `READY_FOR_HANDOFF` / `NEEDS_HUMAN` / `WAIT_CONTROLLER`

## 新增 / 关键 CLI

```text
verify / fix --profile / review / diagnose / fallback --reason / next / runs
```

## 验收

```bash
cd farm-game/.worktrees/agentctl-mm-cp1
python3 -m unittest discover -s tools/agentctl/tests -v
# 34 OK

tools/agentctl/agentctl --json doctor
# ok=true；profiles.declared_unimplemented=[]

# Pilot 仍故意失败（合成 bug 未修）：
python3 -m unittest discover -s tools/agentctl/pilot -p 'test_*.py' -v
```

关键单测覆盖：

- multi-run `current_run_id` slug
- review Findings harvest → `next` 建议 `SUGGEST_GROK_FIX`
- diagnose 门禁 + fallback 需要 reason
- verify + Cursor profile fix 修 pilot bug（测完恢复故意错误）

## 推荐端到端操作（合成 Pilot）

```bash
export AGENTCTL_STATE_DIR=/data/home/developer_026/.local/state/ac1-agentctl-mm-pilot
WT=/data/home/developer_026/CodeBuddy/20260519141303/farm-game/.worktrees/agentctl-mm-cp1
cd "$WT"
AC="tools/agentctl/agentctl --state-dir $AGENTCTL_STATE_DIR"

# 确认 pilot 仍坏
python3 -m unittest discover -s tools/agentctl/pilot -p 'test_*.py' -v

$AC register tools/agentctl/examples/manifest.pilot-quota.json
$AC verify mm-pilot-quota
$AC fix mm-pilot-quota --profile cursor-grok45-fixer
$AC watch-once mm-pilot-quota
$AC verify mm-pilot-quota
$AC review mm-pilot-quota --profile codex-luna-review
$AC watch-once mm-pilot-quota
$AC next mm-pilot-quota
$AC runs mm-pilot-quota
```

若 Grok 连修两次仍 `CODE_FAILURE`：

```bash
$AC diagnose mm-pilot-quota --profile codex-sol-xhigh-diagnose
$AC watch-once mm-pilot-quota
$AC fix mm-pilot-quota --profile cursor-grok45-fixer
# 仍失败且需要备份时（手动）：
$AC fallback mm-pilot-quota --reason "cursor unavailable / repeated CODE_FAILURE" \
  --profile codebuddy-kimi-k3-backup
```

## 明确未做 / 约束

- 没有自动 provider fallback；没有 daemon 调度循环
- 真实模型 id 以本机 `agent --list-models` / CodeBuddy 模型列表为准；未做强 `MODEL_MISMATCH` 门禁
- Care 业务 Writer **不要**迁到本分支；Care P1 继续在独立 worktree
- 未提交；需要时再 commit

## 真实 Pilot 结果（2026-08-05）

在干净 worktree `farm-game/.worktrees/agentctl-mm-pilot-run` 上跑通：

| 步骤 | 结果 |
|---|---|
| `verify` | `CODE_FAILURE`（off-by-one） |
| `fix --profile cursor-grok45-fixer` | Cursor `cursor-grok-4.5-high-fast` 修好 `quota.py` |
| `verify` | `PASS`（`verify-002`） |
| `review --profile codex-luna-review` | Codex `gpt-5.6-luna` Findings=`PASS` |
| `next` | `READY_FOR_HANDOFF` |

本机修正：

- Cursor Grok `cli_model`：`cursor-grok-4.5-high-fast`（不是 `grok-4.5-high-fast`）
- Codex Luna/Sol 增加 `cli_model`：`gpt-5.6-luna` / `gpt-5.6-sol`
- Codex reviewer 不用 `read-only` sandbox（否则写不了 state 目录 Findings）；靠 diff fingerprint 防改代码

开发 worktree 里的 `tools/agentctl/pilot/quota.py` **仍保留故意 bug**，供单测使用。

## 状态

`DESIGN_CANDIDATE / AGENTCTL_MVP_COMPATIBLE / MULTI_RUNNER_CP3-6_IMPLEMENTED / PILOT_GROK_LUNA_VERIFIED`
