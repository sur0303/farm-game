# 好友互动提醒真实链路 A/B 结果（2026-08-11）

## 1. 结论

当前推荐候选为：

```text
B2 + 1 秒 Scheduler 分片轮询

可行动 Hint：
- WATER：好友当前可以浇水
- STEALABLE：好友作物已经成熟且可能可偷

不发送 Hint：
- PLANT：播种只登记未来到期时间
- PEST：当前 H5 尚未提供除虫操作

安全兜底：
- 120 秒 Pull
```

B1 和 B2 的服务端 Scheduler、到期重算和 Hint 写路径完全相同，区别只在 H5 的兜底 Pull 周期。B2 在保留相同实时性的同时减少了无事件时的查询，因此优于 B1。

1 秒轮询相对于实验初始的 250ms 轮询，将 Scheduler 专属 Redis 往返减少约 73%，本次实测中浇水提醒仍约 1.5 秒、成熟提醒约 0.83 秒。

这不是 3000 万 DAU 容量证明。它证明的是：真实浏览器、真实服务进程和公司 Redis 上，这条功能链已经闭环，并能把固定扫描成本与 Hint 增量成本分别量出。

## 2. 实测链路

```text
H5 浏览器 A、B
  → farm-gate HTTPS/WSS
  → player-svr / farm-svr
  → 公司 Redis（每个 Variant 独立 namespace）
  → farm-worker 到期发现
  → 读取当前 Farm Header/Facts
  → 收集目标农场主最多 300 名好友
  → Redis Pub/Sub 发布 targeted Hint
  → Gate 只向本机在线连接下行
  → H5 收到 Hint 后重新精确查询
  → 展示“可浇水”或“可偷菜”弹窗
```

测试没有替换现有开发栈；每组使用随机回环端口和独立 Redis namespace。

## 3. 负载模型

| 项目 | 本次设置 |
|---|---:|
| 好友关系 | 300 人 |
| 在线浏览器 | 2 个（农场主和一名访问者） |
| 目标作物 | 1 茬 |
| 浇水边界 | 种植约 3 秒后 |
| 成熟边界 | 种植约 10 秒后 |
| 每组观察时间 | 130 秒 |
| FARM Route Shard | 1（隔离验收配置） |
| 真实发布 Hint | 2 个：WATER、STEALABLE |
| 到期边界 | 3 个：WATER、PEST、STEALABLE；PEST 只推进 Schedule，不发布 Hint |

300 名好友都写入真实 FriendRelation/FriendIndex，用于测量目标好友收集和 Hint payload；只有一个访问者在线，因此这不是“300 个同时在线客户端”的广播压测。

## 4. Variant 定义

| Variant | Scheduler/Hint | H5 兜底 |
|---|---|---|
| A | 关闭 | 30 秒 Pull |
| B1 | 开启 | 30 秒 Pull |
| B2 | 开启 | 120 秒 Pull |
| B2-1s | 开启，轮询从 250ms 调整为 1s | 120 秒 Pull |

## 5. 端到端结果

| Variant | WATER 到弹窗 | STEALABLE 到弹窗 | `/friends/opportunity` | 好友分页请求 | 好友 HTTP 总数 | HTTP p95 | 错误 |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 未单独测量 | 15001 ms | 4 | 4 | 8 | 413 ms | 0 |
| B1 / 250ms | 1472 ms | 345 ms | 5 | 21 | 26 | 417 ms | 0 |
| B2 / 250ms | 1425 ms | 137 ms | 2 | 18 | 20 | 375 ms | 0 |
| B2 / 1s | 1500 ms | 831 ms | 2 | 18 | 20 | 410 ms | 0 |

A 的 15001ms 是成熟时刻恰好落在下一次 30 秒轮询前约 15 秒的单次样本，不代表固定延迟 15 秒；A 的理论最坏等待仍接近 30 秒。

## 6. Scheduler 与 Hint 的 Redis 成本

指标通过 go-redis Hook 按 `component=opportunity_wakeup` 单独归因，不采集 Redis Key、值、密码、响应或业务 payload。

| Variant | 专属命令 | 专属请求字节 | 专属往返 | 固定 TIME + DueIndex 扫描命令 | 固定扫描字节 | 到期处理 + Hint 命令 | 到期处理 + Hint 字节 |
|---|---:|---:|---:|---:|---:|---:|---:|
| B1 / 250ms | 1075 | 136688 | 1060 | 1034 | 89441 | 41 | 47247 |
| B2 / 250ms | 1075 | 136688 | 1060 | 1034 | 89441 | 41 | 47247 |
| B2 / 1s | 301 | 71273 | 286 | 260 | 23530 | 41 | 47743 |

关键解释：

- B1 与 B2 的服务端成本相同，证明 120 秒兜底只减少客户端/查询流量，不增加 Hint 写入。
- 250ms 配置在 130 秒内执行 517 轮；1s 配置执行 130 轮。
- 250ms 下 1075 条专属命令中，1034 条只是每轮一次 Redis `TIME` 和一次 DueIndex 查询，占约 96.2%。
- 1s 后固定扫描命令从 1034 降至 260；业务边界处理仍是 3 次，发布 Hint 仍是 2 次。
- 两次 `PUBLISH` 的请求字节合计约 5072 字节。每条 Hint 携带最多 300 个 viewer id，编码 payload 约为 `32 + 8 × 300 = 2432` 字节，再加命令和 Channel 开销。
- 表中的 41 条“到期处理 + Hint”命令还包含 3 次 Schedule Claim、Farm Header/Facts 读取、一次 PEST 边界推进和最终 Complete，不能全部算作两次 Hint 的纯写成本。

### 6.1 播种时登记 Schedule 的额外写成本

另用真实浏览器链路在同一次 `Buy + Plant` 完成后立即抓取指标，避免后续 Scheduler、Hint 和 fallback Pull 干扰：

| Variant | farm-svr Redis 命令 | farm-svr Redis 往返 | farm-svr 请求字节 |
|---|---:|---:|---:|
| A：不附加 Schedule | 15 | 15 | 40751 |
| B2：同提交附加 Schedule | 15 | 15 | 41478 |

B2 没有增加 Redis 命令或往返，只让同一次 Farm Authority 提交增加 727 字节，约为本次 Farm 写窗口的 1.8%。原因是 Farm 状态与 `FarmOpportunityWakeup` 在同一 Authority Bucket/Slot 内由同一次原子脚本提交，而不是“先种植，再单独写一个提醒任务”。

这组数字会受到 ID 长度和序列化字段的少量影响，但足以说明当前主要成本不在播种写放大。

## 7. 已发现的性能问题

### 7.1 Hint 定向精查已在融合分支闭环

融合分支不再让 H5 沿好友分页寻找 Hint 目标。在线 Hint 在 300ms 窗口内按
`target_owner_id + opportunity_revision` 合并，单次最多携带 16 个目标：

```text
GET /api/v1/friends/opportunity?hint_target=<owner>:<revision>...
  → viewer 只取自 HttpOnly 登录会话
  → 校验 canonical FriendRelation / relation_epoch
  → 读取 PublicProfile 与目标 Farm Header/Facts
  → 读取 viewer 的 TheftAttemptReceipt
  → 得到当前玩家的精确 action_mask
  → 与 CandidateWindow 在同一次 Navigation CAS 中更新
  → 返回当前最新的一张推荐
```

Hint 只是失效提示，不是授权。若服务端权威 `opportunity_revision` 尚未追上 Hint，返回
`UNKNOWN`，保留旧窗口但不展示旧 READY，客户端有界重试。已访问或已失效目标会从窗口移除。

融合后真实 300 好友链路中，5 次定向 `/friends/opportunity?hint_target=...` 均为
10～12ms，只发生 1 次当前页好友列表请求；没有再为目标位置翻 15 页。浇水边界到弹窗
1230ms，成熟边界到偷菜弹窗 841ms。

### 7.2 多 Worker 不能重复扫描全部 Shard

本次验收只有 1 个 FARM Shard。当前实验装配会把配置里的全部 FARM Shard 交给一个 Worker；正式多 Worker 部署时必须由 Route Assignment/Lease 给各 Worker 分配互不重叠的 Shard 子集。

禁止：

```text
每个 farm-worker 都扫描全部 FARM Shard
```

推荐：

```text
FARM Shard Assignment
  → 每个 Worker 仅扫描自己持有的 Shard
  → 1 秒轮询
  → 记录 due lag / empty scan / claim conflict
```

### 7.3 Pub/Sub 仍会到达所有 Gate

当前一条 targeted Hint 带 300 个 viewer id，通过共享 Pub/Sub 到达所有 Gate，再由每个 Gate 只命中本地在线连接。实现简单，但 Gate 数量很大时存在复制放大。

演进方向是批量读取在线 SessionRoute，按 `gateway_id` 分组后只投递到目标 Gate。P0 可以先保留当前方案，但压测必须覆盖 Gate 数增长。

## 8. 推荐落地顺序

1. 保留 A/B 开关，异常时可切回 B1/A，无需迁移 Farm 权威数据。
2. 使用 B2 语义：WATER + STEALABLE Hint，120 秒 Navigator 兜底。
3. Scheduler 初始轮询设为 1 秒，并按 FARM Shard 独占分配。
4. 在线 Hint 使用已实现的定向精查；登录、回自己农场、页面恢复和 fallback 使用全局 Navigator V3。
5. 小流量观察 Hint 成功率、端到端 actionable latency、Due lag、每 Hint 好友数、UNKNOWN 比例和 Gate 下行队列。
6. Hint SLO 稳定后再扩大流量，并依据 Gate 数量决定是否从共享 Pub/Sub 演进到按 `gateway_id` 定向投递。

## 9. 证据位置

Linux 验证工作区：

```text
/data/home/developer_026/CodeBuddy/20260519141303/
  farm-game-friend-opportunity-real-e2e-20260811/
```

原始证据：

```text
docs/verification/evidence/friend-opportunity-real-ab/full300v3/
  a/result.json
  b1/result.json
  b2/result.json

docs/verification/evidence/friend-opportunity-real-ab/full300v3b2poll1s/
  b2/result.json

docs/verification/evidence/friend-opportunity-real-ab/full300v4plantcost/
  a/result.json
  b2/result.json
```

所有六次真实浏览器 E2E 均通过，HTTP 非 2xx 数均为 0。

融合分支追加验收：

```text
docs/verification/evidence/friend-opportunity-real-ab/fusion-20260811-1s-b2/
  b2/result.json
```

该次使用 300 名真实好友关系、2 块地、1 秒 Scheduler 和 B2 客户端。结果通过；测量期
6 次机会查询中 5 次带定向目标，定向请求耗时 10～12ms。另一次无 Hint 的全局查询返回
`503/UNKNOWN`，符合“不把不确定状态误报为 NONE/READY”的合同，并由 B2 恢复链负责重试。
