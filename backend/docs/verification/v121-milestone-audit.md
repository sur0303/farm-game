# M0 后 M1–M6 验收映射

日期：2026-07-30

现有 `v121-m1-*` 到 `v121-m6-*` 文件最初按实现批次编号，不能直接等同于冻结
计划中的 M1–M6。后续以如下真实验收项判断完成度：

| 里程碑 | 冻结计划要求 | 当前结论 |
| --- | --- | --- |
| M1 | RecoveryBarrier、恢复 UI、多 Gate Drain | 已完成，见 `v121-m1-multigate-drain.md` |
| M2 | 三 Loop 与进程故障 E2E 重跑、冻结 | 已完成，见 `v121-m2-loop-and-process-freeze.md` |
| M3 | Task/Codex/Mail 任选一个最小 Stretch | 已实现植物图鉴，见 `v121-stretch-codex.md` |
| M4 | ActorAddress、ShardCalculator、ActorRouteClient 第一版 | 已完成静态第一版与 writer epoch fence，见 `v121-m5-actor-routing.md` |
| M5 | Redis 生产失败语义与 Tcaplus 迁移形状 | 应用 uncertain commit、HA 逐笔测量工具、稳态基线与迁移 descriptor 已验证；平台切主/恢复待维护窗口，见 `v121-m5-redis-semantics.md` |
| M6 | 混合负载与 3000 万 DAU、30 分钟报告 | 500 连接/100 Buy+Plant 的 30 分钟本地基线已完成且零错误；生产上限仍需 1k+、持续 QPS、热门 Farm、慢客户端和平台故障专项，见 `v121-m6-capacity-and-observability.md` |

任何一项只有设计或脚本、没有可复现运行证据时，都不标为完成。
