# 好友互动机会 P0 优化交接

日期：2026-08-11
状态：独立分支验证完成，未合入 Integration。

## 1. 目标与结论

本轮优化覆盖“在线 Hint → 右下角弹窗”“登录/回到自己农场 → 下一座可互动好友”“打开好友列表”三条读链，并降低 `farm-worker` 空扫描造成的 Redis 压力。

最终方案仍然不引入异步 `FarmOpportunitySummary` 第二份写模型：

```text
Farm 权威 Header/Facts
  → 首次全局 Header 粗筛和排序
  → Top-K Facts 精确检查
  → 保存每玩家 CandidateWindow

后续同一轮查询
  → O(1) 读取 FriendIndex roster_revision
  → 版本未变则复用 CandidateWindow
  → 仅精查小窗口和最终目标
```

Hint 只表示“某座农场的机会版本变化”，不携带可操作结论。H5 收到 Hint 后清除该目标的旧推荐，按 `opportunity_revision` 去重，在 300ms 窗口内合并触发，然后只请求一次服务端导航；最终操作仍由 Pair 和 Farm 权威状态裁决。

## 2. 实现内容

### 2.1 服务端导航

- `GET_NEXT_FRIEND_OPPORTUNITY` 的 V3 开关实际传入 `player-svr`，避免只在 Gate 设置但远程 Player 服务仍走旧算法。
- 首次查询读取最多 300 个好友 Header，全局按 `actionable_at_ms` 排序，最多精查 16 个候选，持久保存 8 个 CandidateWindow 条目。
- 每个 CandidateWindow 绑定 `roster_revision`。后续查询只读取一次当前 revision；未变化时不再扫描完整好友表。
- 最终返回前仍执行 `FriendIndex.Lookup + canonical Pair + PublicProfile + Farm Facts` 校验。CandidateWindow 不是权限或业务真值。
- 玩家真正进入推荐农场并达到 `SUBSCRIPTION_READY` 后，服务端记录 visited，导航才推进到下一座农场。

### 2.2 H5 Hint 路径

- 同一 owner 的旧/重复 `opportunity_revision` 被丢弃。
- Hint 突发在 300ms 内合并；执行中的请求最多保留一轮尾随刷新。
- Hint 到达后不再由浏览器翻最多 20 页好友列表查找目标；改为调用一次服务端 V3 导航。
- 新 Hint 到达时，如果旧弹窗正指向同一农场，先隐藏旧 READY 结论，避免短暂展示已失效机会。
- 好友列表仍按页（20 人）读取并精确检查；打开列表和弹窗导航是两条不同成本的链路。

### 2.3 Worker 空扫描

- `ObserveDue` 结果缓存从固定 1 秒改为可配置，真实压测使用 10 秒。
- 完成一次完整空桶扫描后使用有界指数退避，真实压测最大 2 秒。
- 发现任务、发生错误或未完成整轮扫描时立即恢复基础扫描节奏。
- 增加空扫描退避和 Due 观察缓存测试。

### 2.4 真实链路测试夹具

- 使用公司 Redis、真实 `farm-gate + player-svr + farm-svr + farm-worker + Chromium`。
- 创建 300 个真实好友关系、双方索引、canonical Pair、公开资料和农场记录。
- 一次批量购买 5 颗种子，再在 5 块地依次种植，避免测试夹具反复打开商店产生 UI 抖动。
- 记录 HTTP 请求、Hint/弹窗时间点、进程指标和 Redis 命令分布。

## 3. 数据对比

测试条件：300 好友、5 块地、B1 纯 Hint、60 秒观测窗口、公司 Redis。

| 指标 | 优化前 Header/Facts | 优化后 |
|---|---:|---:|
| 浏览器好友相关 HTTP 请求 | 35 | 9 |
| 其中好友列表分页请求 | 31 | 3 |
| 机会导航请求 | 4 | 6 |
| 首次冷导航 | 394～421ms | 176ms |
| CandidateWindow 热导航 | 372～421ms | 11～13ms |
| HTTP 综合 P50 | 36ms | 13ms |
| HTTP 综合 P95 | 394ms | 176ms |
| Worker Redis 命令总数 | 450,202 | 71,362 |
| `ZCOUNT` | 360,806 | 24,814 |
| `ZRANGEBYSCORE` | 84,021 | 43,085 |

变化比例：

- 浏览器好友相关请求减少约 74%。
- Worker Redis 命令减少约 84%。
- `ZCOUNT` 减少约 93%。
- `ZRANGEBYSCORE` 减少约 49%。
- 冷启动仍需面对 300 好友全局排序，但同一轮后续查询已降到约 12ms。

实时体验观测：

- 首个浇水机会：到期后约 3ms 收到 Hint，约 482ms 后弹窗可操作；其中包含 300ms H5 合并窗口和一次 176ms 冷导航。
- 作物成熟机会：Hint 到弹窗约 316ms；此时 CandidateWindow 已热，服务端查询约 12ms，主要延迟来自 300ms 合并窗口。
- 全部响应为 2xx，真实浏览器场景通过。

这些数字是单机真实链路证据，不等同于 3000 万 DAU 容量证明；容量结论仍需多实例并发压测。

## 4. 正确性边界

- Header 只用于粗筛，Facts 和 TheftAttemptReceipt 决定当前玩家是否可操作。
- CandidateWindow 只保存导航候选，不保存最终权限。
- `roster_revision` 未变只表示可以复用候选顺序；最终目标仍重新校验 canonical Pair，因此删友/拉黑不会依赖窗口正确性。
- Hint 丢失由低频 fallback poll 和登录/回家主动 Pull 补偿。
- Hint 重复不会重复授权，也不会让旧 revision 覆盖新权威结果。
- Redis Worker 退避只影响后台发现时延，不改变作物由 `trusted_now >= mature_at` 派生的成熟事实。

## 5. 验证

```text
后端：go test ./...                         PASS
H5：  28 test files / 231 tests            PASS
H5：  npm run build                        PASS
E2E： 300 friends / company Redis / B1     PASS
```

最终证据运行 ID：`p0optv3b1r6-20260811`。

## 6. 尚未继续扩张的优化

以下内容不阻塞当前 P0：

1. 同一农场多块地在很短时间内到期时，Worker 仍可能产生多条不同 revision 的 Hint；当前 H5 已合并为少量 Pull，后续可在服务端增加按 `farm_id` 的 100～300ms Hint 合并窗口。
2. Worker 仍需扫描较多 DueIndex 分桶。下一阶段应引入“当前非空桶目录”，让空桶完全退出热扫描，而不是继续增加固定轮询间隔。
3. 首次 300 好友冷导航约 176ms。若真实压测证明不足，再评估独立只读 Summary；当前数据尚不支持承担额外写放大、重建和一致性成本。
4. 需要继续做多玩家并发、热点好友列表和 Redis Cluster 多节点压测，不能用单浏览器结果直接外推 3000 万 DAU。

## 7. 分支与 worktree

```text
后端分支：codex/friend-opportunity-p0-opt-r1
后端 worktree：farm-game-friend-opportunity-p0-opt-20260811

H5 分支：codex/h5-friend-opportunity-p0-opt-r2
H5 worktree：classic-farm-h5-friend-opportunity-p0-opt2-20260811
```

两条分支都不得直接合入 Integration；先 Review 本文、差异和真实证据，再决定合并顺序。
