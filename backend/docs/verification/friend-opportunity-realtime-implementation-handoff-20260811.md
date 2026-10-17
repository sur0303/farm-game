# 好友互动机会实时提醒 A/B 实现交接

日期：2026-08-11
分支：`codex/friend-opportunity-realtime-ab-r1`
基线：`Integration@c395f3343d7006d32a834c307422e7b34df04a34`

## 1. 本分支解决什么问题

原有方案只依赖客户端约 30 秒一次的机会查询。作物在好友在线期间成熟，或护理机会到期时，右下角弹窗可能延迟到下一轮查询才出现。

本分支增加可恢复的“整座农场下一次机会唤醒”，到期后只发送失效提示（Hint），客户端仍通过原有精确查询获得当前玩家真实可执行的操作。Scheduler 不修改作物成熟状态，也不把 Hint 当成业务真值。

```text
FarmActor 权威提交
  ├─ Farm 状态 / Header / Facts
  └─ FarmOpportunityWakeup（同一次 Redis 原子提交）
                    ↓
Redis DueIndex（每座农场最多一个有效 Schedule）
                    ↓
farm-worker 到期领取并按 Redis TIME 重算
                    ↓
读取 owner FriendIndex 一次，最多 300 人
                    ↓
发布携带目标 viewer_ids 的 OpportunityChanged Hint
                    ↓
各 farm-gate 只向本机命中的在线连接投递
                    ↓
H5 合并 Hint，重新发起 viewer-exact 查询
```

## 2. 正确性边界

- 一座农场只有一个稳定 `schedule_id`，不会为每块地创建常驻计时器。
- `generation` 绑定 Farm 版本；旧唤醒只能成为 stale no-op，不能覆盖新计划。
- Farm 状态、机会 Header/Facts、Schedule 和必要结果在同一 Authority Bucket/Redis Slot 内原子提交。
- Worker 使用 Redis `TIME` 作为生产可信时间；本地 `Now` 仅用于测试。
- Worker 到期后重新读取紧凑 Header/Facts 并重新裁决，不激活 FarmActor。
- Scheduler 只负责唤醒和提示，不把作物写成 Mature；成熟仍由 `trusted_now >= mature_at` 派生。
- Hint 只表示“相关机会可能变化”，客户端必须重新精确查询；Hint 丢失仍由 fallback pull 补偿。
- Worker 崩溃、租约超时或 Hint 发布失败时，Schedule 可由 DueIndex 再发现、重新领取。

## 3. 主要代码位置

| 职责 | 位置 |
|---|---|
| 下一机会计算与 Schedule 语义 | `internal/farm/domain/opportunity_wakeup.go` |
| Schedule payload | `internal/farm/opportunitywakeup/` |
| FarmActor 原子附加 Schedule | `internal/farm/actor/opportunity_schedule.go` |
| Redis/内存 Store Schedule CAS、Lease、DueIndex | `internal/storage/authoritystore/`、`internal/storage/redis/authority/` |
| 目标好友 Hint 封装与发布 | `internal/friend/opportunity_targeted_hint_bus.go` |
| 到期发现、领取、重算、推进 | `internal/worker/opportunityrecovery/` |
| farm-worker 生产装配 | `cmd/farm-worker/opportunity_recovery.go`、`cmd/farm-worker/main.go` |
| A/B 合成模型与命令 | `internal/loadtest/friendopportunity/`、`cmd/friend-opportunity-bench/` |
| 矩阵脚本 | `scripts/run-friend-opportunity-realtime-ab.sh` |

## 4. A/B 开关

默认全部关闭，合入分支不会自动改变现有环境行为。

| 组别 | farm-svr | farm-worker | H5 |
|---|---|---|---|
| A：Pull 基线 | `FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED=false` | `FARM_OPPORTUNITY_WAKEUP_ENABLED=false` | `VITE_FRIEND_OPPORTUNITY_EXPERIMENT=A` |
| B1：Hint + 30s Pull | `true` | `true`，`SHADOW=false` | `B1` |
| B2：Hint + 120s Pull | `true` | `true`，`SHADOW=false` | `B2` |
| 影子验证 | `true` | `true`，`SHADOW=true` | A 或 B1 |

Worker 细粒度参数：

```text
FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL=250ms
FARM_OPPORTUNITY_WAKEUP_LEASE=5s
FARM_OPPORTUNITY_WAKEUP_CLAIM_LIMIT=64
FARM_OPPORTUNITY_WAKEUP_DISCOVERY_LIMIT=256
```

显式启用 Worker 但缺少 Redis 或 FARM 静态路由时，进程会 fail closed，不会伪装成已启用。

## 5. 已完成验证

### Go 测试

- `go test -count=1 ./...`：通过。
- Farm、Friend、Gate、Worker、AuthorityStore、Redis Store 的重点包回归：通过。
- 当前运行环境配置的真实 Redis 探针：条件建档、同 Slot Schedule 原子提交、Redis TIME 到期发现、Claim/Complete 全部通过。
- 真实 Redis 探针默认跳过，仅设置 `FARM_REDIS_LIVE_PROBE=1` 时运行；测试使用唯一命名空间，只删除自己派生出的固定 Key，不扫描或清空 Redis。

### 合成 A/B 矩阵

固定种子 79、10 session/s、5 分钟，完整矩阵共 216 个 cell。以下是三个代表点：

| 好友×地块×机会密度×在线率 | A 30s fallback | B1 30s fallback | B2 120s fallback | Scheduler upsert/cancel | Hint 目标数 | A/B1/B2 模型逻辑操作量 | p95 A/B1/B2 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 20×5×5%×1.9%，均匀 | 16500 | 16500 | 5400 | 4/1 | 2 | 676500 / 676527 / 221427 | 83/83/83ms |
| 100×12×20%×5%，突发 | 16500 | 16500 | 5400 | 192/48 | 892 | 3316500 / 3320184 / 1089084 | 94/5/5ms |
| 300×24×50%×10%，突发 | 16500 | 16500 | 5400 | 2880/720 | 86526 | 9916500 / 10191198 / 3520098 | 94/5/5ms |

结论仅适用于合成模型：B1 主要改善及时性；B2 在保留 Hint 及时性的同时减少 11100 次 fallback pull。它不能替代真实集群的 Redis、Gate 下行和 Worker 容量压测。

## 6. 当前有意保留的限制

- Targeted Hint envelope 当前仍经共享 Pub/Sub 到所有 Gate，再由每个 Gate 按本地连接过滤；它已经避免每个 Gate 重查 FriendIndex，但尚未演进成按 SessionRoute 直接投递到目标 Gate 的批次。
- 好友上限按 300 做有界读取；更高上限必须先重做扇出容量模型。
- B2 的收益依赖 Hint 可用率；Hint 丢失时最坏补偿时间为 120 秒，因此只应在实际 Hint SLO 达标后放量。
- 合成 benchmark 是决策辅助，不是 3000 万 DAU 的容量证明。正式结论需要记录真实 Redis CPU/延迟、DueIndex 积压、Hint fanout、Gate 队列和客户端 actionable latency。

## 7. 推荐上线顺序

1. 保持所有开关关闭部署代码。
2. 开启 `ATTACH_ENABLED=true` 与 Worker `SHADOW=true`，验证 Schedule 数量、过期积压、重试和 Redis 开销。
3. 小流量 B1，观察 Hint 到精确 Pull 的端到端时延与 Gate 队列。
4. B1 稳定后再试 B2；若 Hint 丢失率或 Worker lag 超标，立即回退 B1/A，无需迁移业务数据。
