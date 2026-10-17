# C5 · 好友机会调度专用 DueIndex

状态：实现候选，尚未合入 `Integration`。本文描述 C5 的物理合同、迁移顺序和证据边界。

## 1. 为什么要拆专用索引

旧实现把 `FRIEND_OPPORTUNITY` 调度项和普通 Workflow/Order 都写进同一个 `source-due` ZSET。Worker 为寻找少量机会唤醒项，可能反复扫过大量无关 `wr1_` 成员。C5 为每个 FARM Authority Bucket 增加同槽键：

```text
{authority-bucket}:due           # 既有共享索引
{authority-bucket}:schedule-due  # 仅 sr1_ 调度成员
```

专用索引只改变“如何发现到期工作”，不改变 ScheduleRecord、Actor 权威状态和 Claim/Advance/Complete 的 Fence 语义。

## 2. 四种运行模式

| 模式 | 写入 | 读取 | 用途 |
|---|---|---|---|
| `shared` | 只写共享索引 | 只读共享索引 | 默认兼容模式 |
| `dual_shadow` | 同槽原子双写 | 仍读共享索引 | 建立专用索引、不改变线上执行路径 |
| `dedicated_fallback` | 同槽原子双写 | 有界读取两侧并按 due_at 全局合并 | 迁移观察与兜底，不能作为最终性能模式 |
| `dedicated_only` | 只保留专用索引 | 只读专用索引 | 最终低开销模式 |

`dedicated_only` 必须同时设置 `FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY=true`。缺少显式就绪证明时进程拒绝启动，避免尚未重建完成便漏掉旧 Schedule。

## 3. 安全迁移顺序

```text
shared
  ↓ 发布全部新 Writer
dual_shadow
  ↓ bounded backfill，直到每个 FARM Shard 完整扫描一轮
  ↓ 确认旧 shared-only Writer 已全部 Drain
dedicated_fallback
  ↓ 对比发现结果、积压、延迟和错误；保持可回退
  ↓ 写入 READY 证明
dedicated_only
  ↓ 稳定观察后，另行清理 shared 中遗留 sr1_ 成员
```

禁止从 `shared` 直接跳到 `dedicated_only`。

回滚也必须有方向约束：到达 `dedicated_only` 后，安全的第一回滚目标只能是
`dedicated_fallback`，不能直接切回 `shared`。`dedicated_only` 不再维护完整共享索引，
直接回到 `shared` 可能漏发现只存在于专用索引中的 Schedule。若未来必须完全回到
`shared`，需要先提供“专用索引到共享索引”的反向重建工具并完成覆盖校验；C5 当前
没有实现这条低优先级退回路径。

## 4. Backfill 为什么安全

回填不是把旧 ZSET 的 score 原样复制过去。每个候选成员都要重新读取当前 ScheduleRecord，再由同槽 Lua 复核：

- schedule_id / generation / state / next_due_at 必须仍与读取结果一致；
- ACTIVE 时以当前 `next_due_at` 写入专用索引；
- COMPLETED、缺失或已换代时清理专用索引中的旧成员；
- 并发 Advance/Complete 先发生时，回填不会把旧 score 复活。

`BackfillScheduleDueIndexPage` 使用有界 `ZSCAN MATCH sr1_*`，按页可重入、可幂等重跑。命令行工具需要静态 RouteSnapshot，用它逐个枚举 FARM Shard 对应的 Authority Bucket。

示例：

```bash
SCHEDULE_DUE_REDIS_URL='redis://...' go run ./cmd/schedule-due-index-tool \
  -command backfill \
  -confirm-backfill-mode dual_shadow \
  -route-snapshot ./config/route-snapshot.json \
  -page-size 128 \
  -output ./backfill-evidence.json
```

Redis URL 只能通过环境或运行时参数注入，报告不会输出凭据、业务 Key、Actor ID 或 Schedule Payload。
`-confirm-backfill-mode` 是必填的运维确认，只接受 `dual_shadow` 或
`dedicated_fallback`；Store API 在 `shared` 和 `dedicated_only` 下同样拒绝回填。

## 5. 可重复 A/B 探针

```bash
scripts/perf/friend-opportunity-c5/run-schedule-due-index-c5 smoke
scripts/perf/friend-opportunity-c5/run-schedule-due-index-c5 full
```

脚本启动隔离的单节点 Redis 7.4 容器，将大量普通 `wr1_` 成员与一个到期 `sr1_` 成员放入共享索引，对比：

- `shared`；
- `dedicated_fallback`；
- `dedicated_only`。

记录每种模式的 Redis 命令数和 p50/p95/p99。该探针证明“专用索引能否消除无关成员扫描”，不能证明公司 Redis Cluster、多 Worker 或 3000 万 DAU 容量。

## 6. 验收门槛

- 所有 Commit/Claim/Advance/Complete 脚本保留 SHA Golden；
- `dedicated_fallback` 在两侧数据不一致时仍返回全局最早到期项；
- `dedicated_only` 面对至少 10 万普通共享成员时不读取共享索引；
- Backfill 可重复运行，并能修正旧 score、清理完成项；
- 错误模式、损坏的专用成员和未就绪 `dedicated_only` 都 Fail Closed；
- 多 Worker 只扫描 C4 分配给自己的 FARM Shard。

## 7. 尚未完成的生产证据

- 公司 Redis Cluster 上的跨节点 Pipeline、连接池和真实 RTT；
- 多 Worker Assignment Watch 与迁移期间的重复/遗漏验证；
- 生产规模 Bucket 数量、到期密度和 1 秒扫描周期容量；
- 与 Gate WSS Hint 下行共同运行时的端到端延迟与资源占用。

因此 C5 只能标记为“实现与单节点证据完成”，不能标记为“3000 万 DAU 已证明”。

## 8. 2026-08-12 隔离 Redis 实测

环境：单节点 Redis 7.4 容器，2 CPU / 2 GiB；共享索引放入 100,000 个更早到期的普通 `wr1_` 成员，查询 limit=1，重复 2 次。

| 模式 | 每次 Redis 命令 | 找到机会任务 | 单次延迟（约） |
|---|---:|---:|---:|
| `shared` | 65,536 | 0/2 | 8.55 s |
| `dedicated_fallback` | 65,537 | 2/2 | 8.54 s |
| `dedicated_only` | 1 | 2/2 | 0.15 ms |

这组数据说明：共享索引在大量无关成员前置时既有扫描放大，也可能在有界扫描上限内找不到 Schedule；`dedicated_fallback` 保住了迁移正确性，但不会消除旧侧扫描成本；`dedicated_only` 才是最终性能形态。原始证据位于 `docs/verification/perf/friend-opportunity-c5/results/schedule-due-c5-smoke-20260811T203751Z-1755493/`。

## 9. 100 万普通成员完整探针

同一隔离环境将普通 `wr1_` 成员扩大到 1,000,000，三种模式各执行 5 次：

| 模式 | 每次 Redis 命令 | 找到机会任务 | P50 | P95 / P99 |
|---|---:|---:|---:|---:|
| `shared` | 65,536 | 0/5 | 8.917 s | 9.212 s |
| `dedicated_fallback` | 65,537 | 5/5 | 8.677 s | 8.793 s |
| `dedicated_only` | 1 | 5/5 | 0.155 ms | 0.156 ms |

完整探针再次表明：`dedicated_fallback` 是迁移正确性模式，不是性能模式；只有
`dedicated_only` 才消除无关 Workflow/Order 的扫描放大。原始证据位于
`docs/verification/perf/friend-opportunity-c5/results/schedule-due-c5-full-20260811T211215Z-1922807/`。
该结果仍是单节点仓库级证据，`supports_30m_dau=false`。
