# qqfarm 机会弹窗 100/s pprof 与服务端增量

- 日期：2026-08-17
- 集群：`uc-test-plus / developer_026-qqfarm`
- Runtime：`farm-route-runtime@sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026`
- 压测器：`farm-ws-stress@sha256:8e66feebce2b741c04c00744089dd47b09eb9c6ec3c2d1f30ba1e1351973008f`
- 账号：300 个全新 viewer，每人 50 好友，20% 机会密度
- 场景：机会-only `thick-login`，300 人、100/s、30s
- Run ID：`dkqvj3yzpuf6`

## 结果

功能结果为 300/300 Ready，机会 FOUND 300/300，所有错误、断线、Gate rejection 和 Farm downlink queue-full 都为 0。但是登录 Ready P95 为 1629.67ms，机会弹窗 P95 为 1473.48ms，已经进入明显延迟拐点。

同一批请求的服务端增量把瓶颈定位到 PlayerSvr 的机会编排层：

| 观测 | 次数 | 累计服务时间 | 平均 |
|---|---:|---:|---:|
| Player `GetNextFriendOpportunity` | 300 | 203.600s | 678.67ms |
| Farm `BatchReadFarmOpportunityHeader` | 600 | 1.515s | 2.52ms |
| Farm `BatchEvaluateFarmOpportunity` | 600 | 1.449s | 2.42ms |

Player 300 次请求中只有 2 次在 100ms 内完成；28 次落在 100–250ms、85 次落在 250–500ms、116 次落在 500–1000ms、69 次落在 1–2.5s。相反，600 次 Farm Header 和 600 次 Facts RPC 全部在 10ms 内完成，narrow unavailable、fallback、UNKNOWN 均为 0。

Gate 的 12 秒 CPU profile 总采样仅 0.92 CPU 秒（平均约 0.077 核）；机会 HTTP handler 累计 CPU 只有 10ms，没有 Gate 业务热函数。Prometheus 10 秒 rate 窗口峰值为 Player 2.236 核、Gate 0.288 核、Farm 0.204 核。它证明 Gate 和 Farm Header 不是本次机会弹窗延迟的首要瓶颈。

## 为什么不是“一次 Header 读取”

首次机会请求会在 PlayerSvr 中依次执行：导航状态 Load、好友名册 revision、50 条 FriendIndex 扫描与 canonical Pair 批量复验、两批 Header 粗筛、Top-K Facts 精筛、最终目标的 Pair/Profile/Facts 再验证，以及 CandidateWindow CAS Save。因此 narrow 只优化了 Farm Header 读取，未消除 PlayerSvr 的完整名册安全校验与状态持久化成本。

本次诊断后已恢复 Gate 默认好友 HTTP 限流 `20/40`、移除临时 `GATE_PPROF_ENABLED`，删除临时 fixture Pod，并再次通过 `prepare-qqfarm-loadtest --require-ready`；Gate、Player、Farm、两个 Worker、Controller 均 Ready 且 restart=0。

