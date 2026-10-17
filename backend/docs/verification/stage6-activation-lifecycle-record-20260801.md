# 阶段 6：Actor Activation 完整生命周期实施与验收记录（2026-08-01）

版本：`2026-08-01-stage6-complete`

施工依据：`docs/verification/s2-handoff.md` §6（硬不变量、六个工作包、12 条验收）。
本文只记录阶段 6 增量；阶段 0～5 证据见 `pr0-pr2-implementation-record-20260801.md`。
所有改动仍在工作区 diff 中，未提交；提交边界由用户另行指示。

## 1. 实现的工作包

### 6.3.1 Executor 按 Key 退休基础（`internal/logic/actor/runtime/executor.go`）

- `mailbox` 增加 `generation`（Executor 级原子分配器）、`lastActivity`、
  `retired`（atomic）、`inFlight`（mailbox 锁内计数）、`exited`（goroutine 退出观察）。
- 新 API：
  - `MailboxSnapshot(key) (MailboxSnapshot, bool)`：generation、lastActivity、双队列深度、
    in-flight、retired 的一次性快照；
  - `RetireIfIdle(key, generation) RetireOutcome`：仅当 map 中仍是同一 generation、
    双队列皆空且无 in-flight 时退休；`RetireRetired / RetireNotFound / RetireBusy /
    RetireStaleGeneration` 四态可区分。
- 并发语义：
  - 锁序恒为 `Executor.mu → mailbox.mu`；enqueue（user/system）在 mailbox 锁内检查
    `retired`，退休在持双锁时摘除 map 并置位，因此不存在"投递到已关闭 channel"的窗口；
  - Submit 路径遇到 `errMailboxRetired` 自动经 `mailboxFor` 换取新 generation 重试，
    任务不丢、不重；
  - `Stop()` 保留全局关闭语义，不用于模拟普通 passivation。
- 指标：`farm_actor_mailbox_retired_total / mailbox_retire_busy_total /
  mailbox_retire_stale_total`（`actor_kind` label）。

### 6.3.2 PassivationBlockers 快照（`internal/farm/actor/registry.go`）

- blocker 判定边界实现于 `tryPassivateIdle`：`turn_running`（mailbox in-flight）、
  `user_queue_nonempty`、`system_queue_nonempty`、`resolving_unknown`（activation FSM）。
- `persistence_in_flight / dirty_unhanded / participant_result_unanchored` 以常量形式
  预留：当前 Redis CAS 在 Turn 内同步完成，已被 `turn_running` 覆盖；没有把持久化
  异步化。
- Presence 数量与持久化 `RETRY_WAIT` Workflow 不在 blocker 中（阶段 5 已拆耦）。

### 6.3.3 分桶 Idle Sweeper（新包 `internal/logic/actor/idle`）

- `Sweeper`：`buckets map[bucketIndex]set[key]` + `keyPos` 反查；`Touch` 只更新
  deadline/桶归属，无 per-Actor Timer。
- 每 tick（`SweepOnce`）只处理 `bucketIndex <= now` 的到期桶，`MaxPerTick` 封顶，
  溢出以最老未处理桶计 `lag_seconds`。
- 候选由 `RetireFunc` 二次校验（idle deadline、generation、blocker）：
  `Retired → 除名`；`NotIdle → 重新计 idle`；`Busy → 按 BlockedRetryDelay 放入后续桶
  并按 reason 计数`，不 busy loop。
- `Now` 可注入（fake clock）；`Run(ctx)` 后台循环与 `SweepOnce` 同步入口分离。
- 指标：`idle_sweeper_tracked_keys / buckets / sweeps_total / candidates_total /
  retired_total / not_idle_total / lag_seconds / blocked_total{reason}`。

### 6.3.4 FarmActor 两阶段 passivation（`internal/farm/actor/registry.go`）

```text
候选(ACTIVE + mailbox 快照无 blocker + lastActivity 超时)
  → BeginPassivate / PASSIVATING
  → 二次校验 mailbox generation 与队列不变(变了则 CancelPassivate 放弃)
  → SystemQueue 内:再查 PASSIVATING → 删 Registry Actor → FinishPassivate → ABSENT
    → Hub RetireFarmIfEmpty(不清 Presence)
  → Executor RetireIfIdle(退休 Mailbox/goroutine)
  → activation ForgetIfAbsent(回收 ABSENT entry)
```

- 新命令与 passivation 并发：`AdmitUserCommand` 在 PASSIVATING 且写权有效时取消回
  ACTIVE；若删除已完成，命令看到 ABSENT 走 `AdmitNeedsLoad` 重新激活。两种交错都不丢、
  不重、不 panic（race 测试覆盖）。
- mailbox 退休失败（Busy/Stale，即新命令已开始重新激活）不算失败：Actor 主体已释放，
  mailbox 留给新生命周期，下轮扫描再处理。
- Holder 失权（`NotifyLeaseLost`/`evictFenced`）、panic（`handlePanic`）、Unknown
  （`ResolveUnknown`）语义不变，不与普通 idle passivation 共用 `MarkAbsent`。
- `activation.Controller`：entry 增加 `passivated` tombstone，`BeginLoad` 据此计
  `ReactivatedTotal`；新 API `ForgetIfAbsent(key)` 只回收仍为 ABSENT 的 entry。

### 6.3.5 Player Executor Mailbox 回收（`internal/player/actor/registry.go`）

- Player 每 Turn 重新加载权威记录、无缓存 Actor，不引入 Farm 式 Activation FSM；
  只接同一 Sweeper 做 Mailbox/goroutine 空闲回收。
- 回收后下一条命令自动创建新 generation Mailbox 并重新加载权威记录（测试断言
  generation 变化与记录完整性）。

### 6.3.6 指标与接线

- `cmd/farm-svr/main.go`：`FARM_ACTOR_IDLE_*` env（默认 5m/30s/30s/256/5s），
  `StartIdleSweeper`，注册 runtime/activation/sweeper 全部新指标。
- `cmd/player-svr/main.go`：`PLAYER_MAILBOX_IDLE_*` env，同上。
- `deploy/docker-compose.local.yml`：上述 env 透传（默认生产值，E2E 用短超时覆盖）。

## 2. 验收证据

### 2.1 静态、单元与 Race（2026-08-01 复跑）

```text
go vet ./...                        PASS
go test ./...                       PASS
go test -race ./internal/...        PASS
go build ./...                      PASS
./scripts/verify-s2.sh              PASS(含 H5 Vitest 61/61、staging/web 构建)
```

新增定向测试：

- `runtime/executor_retire_test.go`：空闲退休释放 goroutine、in-flight/排队/系统队列
  阻止退休、stale generation 不误删、Submit 与退休并发 200 轮不丢不重不 panic、
  退休后 Stop 快速返回。
- `idle/sweeper_test.go`：到期候选、活跃 key 不扫描、blocked 按延迟重试不 busy loop、
  NotIdle 重新计时、MaxPerTick 与 lag、Forget、10k key 桶有界。
- `activation/activation_test.go`（追加）：ForgetIfAbsent 只回收 ABSENT entry、
  钝化后重新加载计 reactivation。
- `farm/actor/passivation_test.go`：idle 自动 ABSENT 并释放全部资源、Presence 保留、
  RETRY_WAIT 不钉住、running turn / 排队 user / RESOLVING_UNKNOWN 各自阻塞、
  100 轮 Submit×Sweep 并发、重新激活从权威 Store 恢复且 generation 更新、
  200 farm 大 Key 数回落、持续活跃不钝化。
- `player/actor/registry_retire_test.go`：mailbox idle 回收后命令正确（新 generation、
  权威记录重载）、300 player 大 Key 数回落、running turn 阻塞。

### 2.2 真实浏览器与公司 Redis（短 idle 配置验证回落）

以 `FARM_ACTOR_IDLE_TIMEOUT=30s`、bucket/sweep 10s、`PLAYER_MAILBOX_IDLE_TIMEOUT=30s`
重建四容器后：

```text
Web account E2E          1/1 PASS(注册/重登/分享加好友)
Staging Direct WS E2E    5/5 PASS(个人经济/重连、好友实时、顶号、好友农场返回、双端浇水偷菜)
```

阶段 6 指标前后对比（E2E 结束、浏览器关闭、等待约 75s）：

```text
                                    阶段6前(旧二进制)   阶段6后
farm_actor_active_keys{farm}            4                0
farm_actor_activation_tracked_keys      4                0
farm_actor_mailbox_retired_total        —(无指标)        4
farm_actor_activation_passivate_completed_total  —       4
farm_actor_activation_entry_forgotten_total      —       4
farm_actor_idle_sweeper_sweeps_total    —               17
farm_actor_idle_sweeper_candidates_total —               6(retired 4, not_idle 2)
farm_actor_active_keys{player}          3                0
farm_actor_mailbox_retired_total{player} —               6
farm_subscription_hub_lanes_retired_total 0(历史样本)     4
farm_subscription_hub_active_lanes      —                0
farm_subscription_hub_boot_fences       —                0
subscription_cleanup_actor_scan_total   0                0
```

- `active_keys` 与 `activation_tracked_keys` 在 idle 后回落到 0：资源按"当前活跃
  Actor"回落，不再按历史访问数增长。
- `farm_subscription_hub_lanes_retired_total=4`：Actor 自动 ABSENT、浏览器断连精确清理后，
  空 Hub Lane 在真实运行中退休（阶段 5 缺失的运行级证据，本次补齐）。
- `not_idle_total=2`：E2E 期间候选遇到新活动正确重新计时。
- `reactivated_total=0`：E2E 结束后浏览器未再访问，符合预期（tombstone 路径单测覆盖）。
- `subscription_cleanup_actor_scan_total=0` 全程保持（验收 12）。

验证结束后已用生产默认 env（5m/30s）重建四容器，全部 healthy，gate `/readyz` 200。

### 2.3 交接 12 条验收对照

| # | 项 | 证据 |
|---|---|---|
| 1 | idle 自动 ABSENT 并释放 Actor/Mailbox/goroutine/entry | 单测 + §2.2 指标 |
| 2 | Presence 存在仍可 passivate,Hub 保留 | 单测 |
| 3 | RETRY_WAIT Workflow 不钉住 Actor | 单测(blocker 边界不含 workflow) |
| 4 | running Turn / 队列 / Unknown / dirty 阻塞 | 单测三类 + turn_running 覆盖同步 CAS |
| 5 | 新命令与退休并发不丢不重不 panic | race 单测(executor 200 轮、registry 100 轮) |
| 6 | 重新激活权威恢复,旧 generation 不影响 | 单测(generation 变化 + 数据一致) |
| 7 | Player Mailbox 回收后再执行正确 | 单测 + §2.2 player 指标 |
| 8 | 大量 Key 后资源回落 | 单测(200 farm/300 player) + §2.2 |
| 9 | race 覆盖 Executor/Activation/FarmRegistry/PlayerRegistry | `go test -race ./internal/...` PASS |
| 10 | 公司 Redis 下 Web + Staging 复跑 | §2.2(非 Mock) |
| 11 | Actor ABSENT 后空 Hub Lane 真实退休 | `lanes_retired_total=4` |
| 12 | `subscription_cleanup_actor_scan_total` 保持 0 | §2.2 |

## 3. 明确未做（交接 §6.4 边界）

- 未改公网 v3 Proto field number；未迁移 Workflow Bucket 物理格式；未实现 Journal v2；
- 未删除 PendingJoin 兼容分支（`STRICT_PENDING_JOIN_FENCE=0` 不变）；
- 未把 Presence 权限化或做成 Actor blocker；未异步化 persistence；
- 未改 PlayerActor 生命周期模型；未混入 TCONND/产品功能。

## 4. 配置索引

| env | 默认 | 作用 |
|---|---|---|
| `FARM_ACTOR_IDLE_TIMEOUT` | 5m | Farm Actor 空闲到候选 |
| `FARM_ACTOR_IDLE_BUCKET_WIDTH` | 30s | Sweeper 桶宽 |
| `FARM_ACTOR_IDLE_SWEEP_INTERVAL` | 30s | 后台扫描周期 |
| `FARM_ACTOR_IDLE_MAX_PER_SWEEP` | 256 | 单 tick 候选上限 |
| `FARM_ACTOR_IDLE_BLOCKED_RETRY_DELAY` | 5s | 阻塞重试延迟 |
| `PLAYER_MAILBOX_IDLE_TIMEOUT` | 5m | Player Mailbox 空闲到候选 |
| `PLAYER_MAILBOX_IDLE_SWEEP_INTERVAL` | 30s | 后台扫描周期 |
| `PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP` | 256 | 单 tick 候选上限 |

## 5. 仍需后续观察（生产/发布级，非本次范围）

- 生产流量下 passivation blocked reason 分布与 reactivation 抖动（评估 idle timeout 是否过短）；
- 大 Key 数下 sweeper lag 表现与 `MaxPerSweep` 调优；
- 阶段 7（Journal v2 等）按交接 §9 顺序另行施工。
