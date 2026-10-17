# CP-VERTICAL-B · H1 H5 交接

日期：2026-08-08
分支：`codex/cp-vertical-b-h1-friend-care-20260808`
基线：`e8f90ed`
Backend proto source：`../farm-game-cp-vertical-b-b1-20260808`

## 本轮交付

- 按冻结 Backend proto 重新生成 public/farm/player/logicrpc/task bindings；`player.ts` 对 `task.ts` 的 `TaskDomainState` 依赖使 task binding 必须和生成物一起保留。
- IR/adapter 增加严格 `PublicProfile`、`FarmSnapshot.ownerPublicProfile`、`feature_summary.tasks=31` 的 `task_id/instance_id/progress/target/state` 映射，以及完整 CARE fields 5–11。
- `GET_OPERATION_RESULT` 的 COMPLETED outcome 依据 `original_command_type` 解码 typed payload，并暴露 `completedResult`，CARE 快慢路径使用同一收敛入口。
- HTTPS 好友资料严格读取 flat `owner_id/display_name/avatar_id/level`；卡片使用 server avatar/level，share `inviter_label` 不再进入 profile；好友农场标题仅使用合法 Snapshot profile，缺失时显示“好友资料暂不可用”。
- FarmChange partial apply 保留 owner profile；旧 farm snapshot 不得覆盖新版本。
- CARE 在真实发送前记录 operation id、当前 `player_state_revision` 与 connection generation；`reward_obligation_created` 只进入 Reward Pending。Bootstrap refresh 最多 3 次，仅 revision 严格大于 baseline 才 settle，预算耗尽显示“农场变化已提交，奖励仍在结算/可重试”。旧 generation/operation 不可结束等待。
- CARE `SUCCEEDED/NO_CHANGE` 在 `reward_obligation_created` 缺失或为 false 时清理 wait 并回到 IDLE；只有字段明确为 true 才启动有界 Bootstrap refresh，`REJECTED` 同样清理。App shell 公开 current owner、farm version、profile source/identity、task version/count、CARE status/baseline/revision 与 farm title source；好友卡片公开服务端 owner/avatar/level。
- 任务面板改为 server task summary 的只读渲染，claim 按钮 disabled，App 不发送 `CLAIM_TASK`。
- 保留现有真实 WSS JOIN/CARE/THEFT 与 return-home navigation；测试 helper 增加稳定 profile/avatar/level/revision/task 数据集。

## 验证

通过：

```text
FARM_GAME_DIR=../farm-game-cp-vertical-b-b1-20260808 npm run gen:proto
npm test -- --run                 16 files / 142 tests passed（本轮新增 13 tests）
npx tsc --noEmit                  passed
npm run build:web                 passed
git diff --check                  passed
```

本轮新增真实单元测试覆盖：`care-reward-refresh.test.ts` 5 项；`final-protocol-adapter.test.ts` 6 项；`friend-roster.test.ts` 2 项。未运行 Playwright、未修改 Backend、未接通 CLAIM_TASK 发送路径。

`npm ci` 首次安装按要求执行，但 esbuild install script 在当前受限环境返回 EPERM；使用同一 lockfile 的 `npm ci --ignore-scripts` 恢复生成/构建工具后，proto generation 和 build 均通过。

指定 H5 Playwright 回归未启动：`npx playwright test --config playwright.web.config.ts src/test/e2e/web-real-vertical.spec.ts` 的 config webServer 以 exit code 1 退出，当前没有可用真实 backend/web server；本结果不视为最终真实双浏览器 hard-gate。

## commit / 风险

本 worktree 的 `.git/worktrees/.../index.lock` 所在 git metadata 位于只读文件系统，`git restore`/commit 无法更新 index；因此本轮已产生真实文件变化，但尚未能在此环境创建 commit。未 push、未 merge。建议 Q1 在可写 git metadata 环境提交以下建议 commit：

```text
proto: sync CP-B profile and task bindings
feat(h5): render authoritative friend and farm profiles
feat(h5): converge care rewards and task status
fix(h5): fresh join rejoin-required and preserve theft return-home
tests/handoff
```

剩余风险：真实 Backend 需要下发合法 owner profile，并在 CARE reward obligation 后推进 `player_state_revision`；未运行真实双浏览器链时无法证明“登录→买种子→种植→收获→分享加好友→好友农场→浇水→偷菜→返回自己农场”。Q1 需在 Backend 可用环境补跑并确认最终 hard-gate spec。

READY_FOR_Q1: YES
