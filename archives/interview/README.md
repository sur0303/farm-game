# FarmGame 面试复习存档

个人复习用：简历稿、口述、对照笔记，以及你自己写的 **farm-game / H5 源码快照**（已去密钥）。

**没有放进去的东西：**

- 公司业务仓源码（`zonesvr` / `gatesvr` / `dirsvr` / `coordinator` / `xrpc`）
- 集群地址、账号、镜像仓库凭据、`deploy/secrets`、kubeconfig
- 压测原始 dump / pprof（代码仓里那 80 多 MB 对面试没有用）

公司仓不能拷到私人仓。离职后靠这份笔记讲 UC 思想即可，不要随身带着生产代码。

## 怎么复习

按这个顺序读，大约 3 个晚上：

1. [resume/project.md](resume/project.md) — 简历六条，先能背
2. [study/oral.md](study/oral.md) — 1 分钟 / 3 分钟口述
3. [notes/01-uc-architecture-and-farm-compare.md](notes/01-uc-architecture-and-farm-compare.md) — 有状态后台在想什么
4. [study/watermark.md](study/watermark.md) — `IsDirty` / version 水位（常被追问）
5. [notes/02-durable-cas-vs-dirty-and-perf.md](notes/02-durable-cas-vs-dirty-and-perf.md) — 两条数据面 + 数字
6. [evidence/hot-shard/ONE-PAGE-PITCH.md](evidence/hot-shard/ONE-PAGE-PITCH.md) + [screenshots](evidence/hot-shard/screenshots/) — 迁移那一页
7. [farm-game/](farm-game/) — 对着代码过 Actor / 路由 / CAS；dirty 看 [farm-game/SOURCE.md](farm-game/SOURCE.md)
8. [classic-farm-h5/](classic-farm-h5/) — H5：登录、WSS、协议、经营闭环；说明见 [classic-farm-h5/SOURCE.md](classic-farm-h5/SOURCE.md)

## 目录

| 路径 | 内容 |
|---|---|
| `resume/` | 可粘贴项目经历 |
| `notes/` | 架构对照、CAS vs Dirty、压测 |
| `notes/dirty/` | dirty 实验方案和报告 |
| `study/` | 口述稿、水位图解、复习清单 |
| `evidence/hot-shard/` | 3.15 秒迁移结论和标注图 |
| `farm-game/` | master 源码快照 + dirty 补丁 |
| `classic-farm-h5/` | H5 客户端源码快照 |
