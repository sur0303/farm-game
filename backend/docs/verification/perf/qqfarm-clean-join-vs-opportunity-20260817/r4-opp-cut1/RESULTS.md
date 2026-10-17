# Clean Join vs opportunity 2026-08-17 r4 (friend opportunity first-cut)

Gate-first Skill clean reset（r4b）。相对 r3 只换 Player 镜像，Farm/Gate/Worker 不变：

- Player `farm-route-runtime@sha256:af14da7896da0f6f7e642c5dea9b54d040e903760eff66401316987e4ba04cd2`（nav pipeline + Index BatchLookup + Facts 复用）
- Farm 仍 `25b3a71e`，`DOWNLINK_LANE_WORKERS=4`，`DOWNLINK_BATCH_SIZE=32`
- Player `FRIEND_OPPORTUNITY_SCAN_BATCH_SIZE=64`
- Gate 仍 `244ab98e`（recovery pending-join）
- 无 Holder pprof

Farm uid `b1958862-36c2-4ad7-b30b-91340ff1d88e`。证据目录即本目录。

上一轮 r4 第一次 clean reset 在 claims 后 Holder 被调度挤掉（`Insufficient cpu`），按 Skill 未原地修复，整轮重来。

## Join / thick smoke

| 档 | run | ready | Ready P95 | 判定 |
|---|---|---|---|---|
| Join 1 | `dkr17s6ffi9e` | 1 | 40.8 ms | PASS |
| thick 1（无好友） | `dkr184ckcops` | 1 | 39.4 ms | PASS（NONE，friend P95 9.9 ms） |

`--require-ready` PASS。`farm_downlink_queue_full_total=0`。

## Opportunity-only thick-login 300 人阶梯

50 好友 / 密度 20%。测窗内 Gate 好友限流临时 `10000/20000`，测后已恢复 `20/40`。

| 档 | run | ready | FOUND | Ready P95 | friend startup P95 | 判定 |
|---|---|---|---|---|---|---|
| smoke 1（无好友） | `dkr184ckcops` | 1 | NONE | 39.4 ms | 9.9 ms | PASS |
| 100/s | `dkr1a746uxsr` | 300 | 300 | **43.4 ms** | **28.1 ms** | PASS（r3 65 / 52 ms） |
| 150/s | `dkr1c5e1gvr9` | 300 | 300 | **62.7 ms** | **42.5 ms** | PASS（r3 66 / 53 ms） |
| 200/s | `dkr1ctqiar5e` | 300 | 300 | **57.2 ms** | **36.2 ms** | PASS（r3 80 / 65 ms） |
| 250/s | `dkr1dhzsjr5x` | 300 | 300 | **48.5 ms** | **33.1 ms** | PASS（r3 舒适档 124 / 107 ms） |
| 300/s | `dkr1f2xtrtsf` | 300 | 300 | **55.9 ms** | **38.6 ms** | PASS（r3 拐点 241 / 207 ms） |
| 400/s | `dkr1fr9v2fhv` | 300 | 300 | **90.7 ms** | **65.5 ms** | **舒适 PASS** |
| 500/s | `dkr1gf7p94hp` | 300 | 300 | 215.4 ms | 158.5 ms | 功能过、延迟拐点 |

Opportunity 极限：舒适容量 **400/s**（r3 为 250/s）。500/s 仍 300/300 FOUND、0 个 503，但 Ready P95 > 200ms。300 连接在 ≥300/s 时注入窗口不到 1s，更高档不再代表持续 QPS。

100–400/s 的 PASS **不是** 生产 Gate `20/40` 下的有效容量；那个保护会先 429。JSON `qps≈10` 是 `ready/hold-duration`，忽略。

## 对照 r3

| 口径 | r3（4 lane + scan 64，串行 nav/Index/Facts） | r4（同拓扑 + Player first-cut） |
|---|---|---|
| 100/s friend P95 | 52 ms | **28 ms** |
| 250/s friend P95 | 107 ms（舒适上限） | **33 ms** |
| 300/s friend P95 | 207 ms（拐点） | **39 ms** |
| 舒适容量 | 250/s | **400/s** |
| 延迟拐点 | 300/s | **500/s** |

第一刀生效：冷路径少了串行 Redis RTT 和重复 Facts，好友弹窗 P95 在 r3 拐点档从 207ms 降到 39ms。
