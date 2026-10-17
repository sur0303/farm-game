# Agentctl Multi-Model CP2 Handoff（2026-08-05）

分支：`codex/agentctl-multi-model-cp1-20260805`
工作树：`farm-game/.worktrees/agentctl-mm-cp1`
设计：`docs/design/74-agent-control-plane-multi-model-v1.2.md`

## 本轮目标

在不碰游戏主链的前提下，用**合成 Pilot Bug**验证：

1. `agentctl verify` 产出确定性失败证据
2. `agentctl fix --profile cursor-grok45-fixer` 生成 Bug Pack 并启动 Cursor Runner
3. Fixer 只改允许路径中的小文件后，再次 verify 可通过

## 合成 Pilot Bug（方案二）

| 文件 | 作用 |
|---|---|
| `tools/agentctl/pilot/quota.py` | 故意 off-by-one：`limit - used + 1` |
| `tools/agentctl/pilot/test_quota_acceptance.py` | 验收失败（不在默认 `tests/` 发现路径） |
| `tools/agentctl/examples/manifest.pilot-quota.json` | Pilot Manifest |

默认 `python3 -m unittest discover -s tools/agentctl/tests` **保持绿灯**；只有 pilot acceptance 会失败。

## CP2 已交付

1. **`CursorCliRunner`**
   - `agent --print --output-format stream-json --model --workspace --trust`
   - writer：`--force --sandbox disabled`
   - reviewer/diagnoser：`--mode ask`
   - prompt 以 argv 传入（非 stdin）

2. **`agentctl verify TASK`**
   - 跑 `acceptance_commands`
   - 写 `~/.local/state/.../tasks/<task>/verify/verify-NNN.json`
   - 分类 `PASS` / `CODE_FAILURE`

3. **`agentctl fix TASK --profile cursor-grok45-fixer [--from verify-001]`**
   - 从 verify 构建 Bug Pack（`bugpacks/bugpack-NNN.md|json`）
   - 切换 `runner_kind=cursor-cli`，新建 Session（不 resume Codex）
   - legacy `fix`（无 `--profile`）仍 resume 原 Writer Thread

4. **doctor** 检查 Cursor agent CLI；profiles 中仅 CodeBuddy 仍 unimplemented

## 验收

```bash
cd farm-game/.worktrees/agentctl-mm-cp1
python3 -m unittest discover -s tools/agentctl/tests -v
# 32 OK，含 fake Cursor 修 pilot bug 的端到端单测

# Pilot 仍故意失败：
python3 -m unittest discover -s tools/agentctl/pilot -p 'test_*.py' -v
```

## 真实 Cursor/Grok Pilot 操作（可选）

```bash
export AGENTCTL_STATE_DIR=/data/home/developer_026/.local/state/ac1-agentctl-mm-pilot
WT=/data/home/developer_026/CodeBuddy/20260519141303/farm-game/.worktrees/agentctl-mm-cp1
cd "$WT"

# 确认 pilot 仍是坏的
python3 -m unittest discover -s tools/agentctl/pilot -p 'test_*.py' -v

# 登记（manifest 内 base_commit 已指向当前 HEAD）
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" register \
  tools/agentctl/examples/manifest.pilot-quota.json

tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" verify mm-pilot-quota
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" fix mm-pilot-quota \
  --profile cursor-grok45-fixer
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" watch-once mm-pilot-quota
tools/agentctl/agentctl --state-dir "$AGENTCTL_STATE_DIR" verify mm-pilot-quota
```

注意：真实 Grok 模型 id 以本机 `agent --list-models` 为准；Profile 里 `cli_model` 现为 `grok-4.5-high-fast`，不匹配时会进模型侧错误（后续可加 `MODEL_MISMATCH` 门禁）。

## 明确未做（已由 CP3–CP6 接手）

见 `docs/verification/agentctl-multi-model-cp3-6-handoff-20260805.md`：

- CP3：多 Run 目录 / Fresh Luna Reviewer / Findings
- CP4：failure signature + Sol Diagnose
- CP5：CodeBuddy/Kimi manual fallback
- CP6：controller-brief / `next`

## 下一步建议

1. 用真实 Cursor/Grok + Luna Review 跑 CP3–CP6 handoff 中的 Pilot 命令。
2. Care 收尾后再做业务 Pilot，不迁移正在跑的 Care Writer。
