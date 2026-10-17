# farm-game 源码快照

来自 Gongfeng `developer_026/farm-game` 的 `origin/master`（`280daa13`，2026-08-21）。

这是你自己写的实验工程，不是 UC 生产仓。存档时已去掉：

- `deploy/secrets` 和本地 Redis / kubeconfig
- `docs/verification` 里的压测 dump、pprof、原始 Grafana 导出（太大，面试用不上）

`docs/design/` 留着，方便对着代码看契约。

## dirty 实验怎么看

master 默认还是 `durable_cas`。`dirty_best_effort` 在当时没合进 master。

对照补丁：

- [patches/dirty-writeback-20260824.patch](patches/dirty-writeback-20260824.patch) — 相对 master 的 52 个文件
- [../notes/dirty/88-actor-dirty-writeback-experiment-plan.md](../notes/dirty/88-actor-dirty-writeback-experiment-plan.md)
- [../notes/dirty/actor-dirty-writeback-experiment-20260824.md](../notes/dirty/actor-dirty-writeback-experiment-20260824.md)

在这份快照上应用补丁：

```bash
cd farm-game
git apply patches/dirty-writeback-20260824.patch
```
