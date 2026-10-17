# Agentctl Multi-Model CP1 Handoff（2026-08-05）

分支：`codex/agentctl-multi-model-cp1-20260805`
工作树：`farm-game/.worktrees/agentctl-mm-cp1`
基线：`3804c5d511674137facd3bdc24d8f814fb271725`
设计：`docs/design/74-agent-control-plane-multi-model-v1.2.md`

## 本轮目标

把「单 Codex 模型执行」升级为「一个 Task 下可声明多 Runner Profile」的 CP1 基础，**不中断**正在运行的 Care Writer，也不实现 Cursor/CodeBuddy 启动。

## 已交付（CP1）

1. **设计文档**
   - 新增多模型 v1.2 设计
   - v1.1 架构文增加演进指针

2. **Profile 配置**
   - `tools/agentctl/profiles.json`
   - `codex-luna-primary` / `codex-luna-review` / `cursor-grok45-fixer` / `codex-sol-xhigh-diagnose` / `codebuddy-kimi-k3-backup`

3. **Manifest**
   - schema 支持 `execution_policy`，或继续用 legacy `model` + `reasoning_effort`
   - legacy 自动映射为 `legacy-codex:<hash>` 临时 Profile
   - CP1 只允许 **启动** `codex-cli` Profile；fixer/backup 可声明但不可 launch

4. **Runner 抽象**
   - `RunnerAdapter` Protocol
   - `CodexCliRunner`（`CodexAdapter` 别名保留）
   - `runner_for_kind()`；非 Codex 在 CP1 fail-closed

5. **状态字段（schema_version=3）**
   - `execution_policy` / `active_profile` / `runner_kind` / `current_run_id`
   - `runner_session_id` / `runner_thread_id`（与 `codex_*` 双向镜像）
   - 分类计数器占位：`repair_cycle_count` / `same_failure_signature_count` / `review_round_count` / `post_diagnosis_fix_count`
   - 旧 `failure_count` 仍保留（升级逻辑仍在 CP4）

6. **doctor**
   - 增加 profiles 检查与 unimplemented runner 列表

## 验收

```bash
cd farm-game/.worktrees/agentctl-mm-cp1
python3 -m unittest discover -s tools/agentctl/tests -v
tools/agentctl/agentctl doctor
```

## 明确未做（后续 CP）

| CP | 内容 |
|---|---|
| CP2 | `CursorCliRunner`：doctor / start / resume / stop / 实际模型校验 |
| CP3 | Task 下多 Run 目录、`raw-events.jsonl`、Fresh Luna Reviewer、Findings |
| CP4 | failure signature、repair_cycle 升级、Sol Diagnose Pack |
| CP5 | CodeBuddy/Kimi Backup + manual fallback |
| CP6 | 自动 `controller-brief.json/md` |

## 使用约束

- **不要**把正在跑的 Care Writer 迁到本分支。
- Care 收尾后的 Pilot：新 Luna Review Session → 有 Finding 再启 Grok（需 CP2）。
- `automatic_provider_switch` 永远默认 `false`；跨 Provider 必须人工/`next` 建议。

## 推荐下一步

1. CP2：已继续落地，见 `agentctl-multi-model-cp2-handoff-20260805.md`。
2. CP3：`runs/<run_id>/` 产物布局 + fresh reviewer session。
3. Care finalize 时用新控制面做 Reviewer/Fixer Pilot，不切换旧 Writer。
