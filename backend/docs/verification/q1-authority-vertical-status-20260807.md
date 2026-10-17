# Q1 Authority Vertical — 当前交接状态（2026-08-07）

> 本文件是 CP-VERTICAL-A-RC1 冻结后的最新状态快照，供用户接手/审阅。
> 详细分析见 `q1-smoke-bot-fix-and-h5-flake-handoff-20260807.md`（§1-§8）。

## 一、候选冻结（CP-VERTICAL-A-RC1）

| 仓库 | worktree | 分支 | 冻结 commit | 状态 |
|---|---|---|---|---|
| farm-game | `farm-game-premerge-a-final-20260806` | `codex/premerge-a-backend-final-20260806` | `5fe0f2e` | 代码干净 |
| classic-farm-h5 | `classic-farm-h5-premerge-a-final-20260806` | `codex/premerge-a-h5-final-20260806` | `fe15152` | 干净 |

登记文件：`docs/verification/results/authority-vertical-20260807T041925Z-2131722/cp-vertical-a-rc1.json`

> 注意：冻结的是**代码 commit**（`5fe0f2e` / `fe15152`）。此后我又提交了 S2/S3 测试工具
> （backend `46cbfcd` + 交接 `464535f`）——这些是**测试代码/文档**，不是业务功能，不改变 RC1 的
> 业务代码真值。合并发布 CP-VERTICAL-A 时用 `5fe0f2e` + `fe15152` 即可（S2/S3 工具可一并带上或后置）。

## 二、验收进度

| 阶段 | 状态 | 证据 |
|---|---|---|
| 正常 Smoke（S0+S1A real H5+S1B bot） | ✅ run23/24/25 三轮 PASS | `results/authority-vertical-20260807T041708Z-2125231/` 等 |
| 故障 Smoke（S2/S3） | ✅ 连续两轮 4/4 PASS | `results/authority-fault-20260807T124457Z/`、`-124517Z/` |
| H5 单测/tsc/build | ✅ 129 tests 全绿 | — |
| backend go vet/build | ✅ | — |
| 静态 Gate / dirty 检查 | ✅（dirty 检测排除 evidence 产物） | — |

### 本轮核心修复摘要
1. **H5 预算分离**：BootstrapRecovery purpose 分流 + PlayerBootstrapRefresh 独立控制器
   （run20 验证 PASS，修复"PLANT refresh 被恢复预算饿死"）。
2. **问题 A**：闭合 `PROCESSING → GET_OPERATION_RESULT → COMPLETED → refresh` 链
   （run21/22 卡死根因，修复后 run23/24/25 稳定）。
3. **问题 B**：FarmChange 版本与 plot delta 原子应用（malformed 整体拒绝 → Fresh Join）。
4. **S2/S3**：新增 `authority-fault-bot` + `run-authority-fault-smoke`，4 场景全过。

## 三、当前工作区状态

- **backend worktree**：仅剩几个未跟踪的 fault-smoke evidence 目录（`results/authority-fault-20260807T1235xxZ/`
  是早期调试轮次，非最终 PASS 轮；最终 PASS 轮已提交）。`git status --branch` 显示的 `??` 目录
  属测试产物，脚本 dirty 检测已忽略，不影响验收。
- **H5 worktree**：干净（HEAD `fe15152`）。

## 四、下一步（等你决策）

1. **Review** 两个候选 commit（`5fe0f2e` + `fe15152`）。
2. 确认后**合并发布 CP-VERTICAL-A**：
   - 后端 `5fe0f2e` 合入目标分支；
   - H5 `fe15152` 合入目标分支；
   - 可选：同时带上 S2/S3 测试工具（`46cbfcd`）以保留回归能力。
3. 合并后如需，跑一轮合并后 Smoke 确认无回归。

## 五、已知未决（非阻塞）

- Operation 时间线 witness（T0-T19）未实现——如需可在 fault-bot/authority-bot 增强，非阻塞。
- S1A 连续 10 次定向跑（用户规格建议）尚未执行满；当前 3 轮完整 PASS + 2 轮故障 PASS。

## 六、遇到难题时的约定

按用户指示：若遇难以解决的问题，暂停并写交接文档（详细记录现象、根因猜测、已做尝试、建议方向），
交用户处理。当前无阻塞，进展正常。
