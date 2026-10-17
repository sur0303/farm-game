# R1-T-AUTHCOMP-PANIC-TRIAGE 中文交接

## 结论

分类：`TEST_TIMING_CONFIRMED`。

这不是 R1-4 candidate 独有回归。baseline `dd7104a3c43cecdc85c8d7962014f9760bf28f2d` 的 C1 普通定向 100 次与 C2 race 定向 50 次都各自精确复现一次：

```text
fatal flag must be set after an entry panic
```

两次 `-race` 相关运行都没有 race detector 报告。candidate 与 baseline 的 `lease_scheduler.go`、`lease_scheduler_test.go` 提交态无 diff。

## 机械 happens-before 对照

原测试 helper 只等待 `EntryCount()==0`。生产执行顺序是：

1. `entry.run` 的 defer 捕获 renew panic。
2. `handleEntryPanic` 持 `s.mu` 停止并删除 entry，然后解锁。
3. 解锁后才 fence 对应 guard。
4. 最后 `notifyFatal` 才 CAS `fatal=true`、保存 `fatalErr`，并同步调用 `safeNotifyFatal`。
5. `safeNotifyFatal` 用 defer/recover 包住会 panic 的 callback。

因此测试在 `EntryCount` 上获取同一把 mutex，只能证明“删除 entry”在它之前；无法证明解锁之后的 fence/fatal 写入已发生。helper 可以在中间态返回，随后马上读到 `Fatal()==false`。baseline C1/C2 的精确失败和 candidate B1 中同构 sibling `TestLeaseSchedulerEntryPanicLeavesNoActiveEntry` 的失败共同验证了这个窗口。

修复只改测试：两个相关测试都让其独立 `OnFatal` callback 向 channel 发送事件。channel receive 发生在 callback send 之后，而生产代码保证 send 之前已经按程序顺序完成 entry 删除、guard fence、fatal CAS 和 `fatalErr` store。测试现在等待这个事件后再断言，没有增加 sleep，也没有扩大 timeout。

## 运行结果

| ID | worktree / commit | 结果 | seed | 关键事实 |
|---|---|---:|---|---|
| candidate A1 | candidate / `1ec3dcd…` | PASS | — | 目标 100/100 |
| candidate A2 | candidate / `1ec3dcd…` | PASS | — | race 目标 50/50；无 race 报告 |
| candidate B1 | candidate / `1ec3dcd…` | FAIL | `1786276506503064082` | sandbox listener 禁止；另复现同构 sibling 时序断言 |
| candidate B2 | candidate / `1ec3dcd…` | FAIL | `1786276601662227301` | sandbox listener 禁止；无 race 报告 |
| baseline C1 | primary / `dd7104a…` | FAIL | — | 目标 99 PASS / 1 精确 FAIL |
| baseline C2 | primary / `dd7104a…` | FAIL | — | race 目标 49 PASS / 1 精确 FAIL；无 race 报告 |
| baseline C3 | primary / `dd7104a…` | FAIL | `1786276738734200727` | sandbox listener 禁止 |
| baseline C4 | primary / `dd7104a…` | FAIL | `1786276811536054645` | sandbox listener 禁止；无 race 报告 |
| fixed V1 | primary + test fix | PASS | — | 目标与 sibling 各 100/100 |
| fixed V2 | primary + test fix | PASS | — | race 目标与 sibling 各 50/50；无 race 报告 |

B/C package/shuffle 的 FAIL 没有被复跑覆盖：当前 sandbox 禁止 `miniredis` 监听 `127.0.0.1:0`，原始日志分别保留了 840/420 次 `socket: operation not permitted`。这些环境失败不被写成 PASS。

D 未运行：D 的前提是大量重复仍未复现；本次精确目标已在 baseline C1 和 C2 复现并完成定因。

## 审计清单

- `OnFatal` 是 scheduler 实例字段，不是 package global；本路径没有可变 package global。
- callback 由 `notifyFatal -> safeNotifyFatal` 在 entry goroutine 内同步调用，不是另起异步 goroutine。
- callback 在 scheduler/guard 锁外调用，并由 `safeNotifyFatal` recover；panic 不逃出进程。
- 目标测试不修改 `postRenewHook`/`lostGenHook`，不存在 hook restore 遗漏。
- `Cancel` 在锁内按精确 generation 停止并删除 entry；panic handler 对自身 entry 做相同局部清理。
- `timeoutSchedulerFixture` 注册 `t.Cleanup(scheduler.Close)`；`Close` cancel context、停止剩余 entry，并等待 worker WaitGroup。目标测试后来创建的第二个 shard 也由该 cleanup 回收。
- 原 helper 的 1ms polling/2s deadline 不是 fatal 的事件屏障；新 channel receive 才是。未加 sleep、未扩大 timeout。
- 本注入路径 callback 立即 panic 并被 recover，修复后普通/race 重复均完成；没有本路径 goroutine 泄漏证据。

## Controller 动作

candidate worktree 全程只读并保持 clean。请在本分支单一 triage commit 生成后，由 Controller **另行 cherry-pick** 到 R1-4 candidate；不要直接修改 candidate worktree。本任务未 push/merge/tag。

原始输出、每轮 exit code、UTC/北京时间、commit、独立 `GOCACHE/GOTMPDIR` 路径和 shuffle seed 均在 `raw/`；机器结论见 `triage.json`，校验见 `SHA256SUMS`。

## Controller completion-barrier review

Controller 保留原 `fatalCh` happens-before，并进一步等待 `FatalErr` 出现精确的 `OnFatal callback panicked` 诊断，保证 `safeNotifyFatal` 的外层 recover 与 diagnostic publication 已完成，而不是仅证明 callback 已进入。三个同构测试普通模式各 100/100、race 模式各 50/50 PASS；原始输出见 `raw/controller-fixed-V3*` 与 `raw/controller-fixed-V4*`。全仓三连门只在该 test-only commit cherry-pick 到精确 R1-4 merge-queue candidate 后执行。
