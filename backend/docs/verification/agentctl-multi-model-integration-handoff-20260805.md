# Agentctl 多模型控制器集成交接（2026-08-05）

状态：`INTEGRATED / UNIT_PASS / DOCTOR_PASS / PILOT_VERIFIED`

## 集成位置

- 规范入口：`tools/agentctl/agentctl`
- 核心实现：`tools/agentctl/agentctl.py`
- 模型配置：`tools/agentctl/profiles.json`
- Manifest Schema：`tools/agentctl/schema/manifest.schema.json`
- 设计：`docs/design/74-agent-control-plane-multi-model-v1.2.md`

控制器已移植到包含 Plant、Harvest、Care、Theft 的统一游戏基线，不再依赖
`agentctl-mm-cp1` 或 `agentctl-mm-pilot-run` 等历史 worktree。

## 已交付能力

1. 一个 Task 固定一个 worktree、分支和写集；同一时刻只允许一个 Writer。
2. Codex Luna 负责主体实现，确定性 Verify 先于语义 Review。
3. Verify 失败后生成结构化 Bug Pack，交 Cursor/Grok 做最小修复。
4. Verify 通过后启动 fresh Luna Reviewer，避免 Writer 自审上下文污染。
5. 同签名问题连续失败达到阈值后，允许 Sol 只读诊断并输出最小修复计划。
6. CodeBuddy/Kimi 仅作为人工批准的后备执行器，不自动切换 Provider。
7. 每次 Run 保存事件、日志、最后消息、退出码和结构化产物。
8. 自动生成 `controller-brief.json/md`，`next` 只给建议，不替人做合并决策。
9. 保留 Context Hash、写集 Gate、语义锁、并发上限和容量错误续跑。

## 确定性验证

```bash
python3 -m unittest discover -s tools/agentctl/tests -v
tools/agentctl/agentctl --json doctor
```

结果：

- 34 项单元测试全部通过。
- Codex CLI、Cursor Agent、CodeBuddy CLI、Git、tmux、Python 全部可用。
- 5 个 Profile 均可解析，三个 Runner 均已实现。
- 合成 Pilot 已真实跑通：
  `CODE_FAILURE → Grok Fix → PASS → Luna Review → READY_FOR_HANDOFF`。

## 推荐使用流程

```text
register
→ start
→ watch-once
→ verify
→ fix / review / diagnose / fallback
→ next
→ 人工确认交接与合并
```

日常不要把 26～37 号文档全文塞给每个 Agent。Manifest 只引用权威章节和固定 Commit，
Runner 之间只传 Verify、Findings、Diagnosis、Checkpoint 等结构化短产物。

## 当前有界限制

- 不提供常驻 Daemon；周期调度由外部控制会话调用 `dispatch-once/watch-once`。
- 不自动 commit、push、rebase 或 merge。
- 不自动切换 Provider，Kimi fallback 必须携带人工原因。
- `MODEL_MISMATCH` 已预留 Guard 名称，但真实模型回读强门禁仍待补齐。
- 单测存在少量测试子进程 `ResourceWarning`，当前不影响退出码与功能，后续可独立清理。
- Pilot quota 故意保留 off-by-one，只是隔离的控制器验收夹具，不属于游戏或生产逻辑。

## 替换结论

新版是旧 AC1 控制器的向后兼容替换：旧 Manifest 仍可映射为 legacy Profile，原有
`register/start/resume/status/watch-once/stop` 继续可用；新增多 Runner 和修复闭环不会
强制旧任务采用新模型。后续业务 Agent 应统一使用本版本，不再从历史 worktree 复制控制器。
