# PR-0～阶段 5 实施与验收记录（2026-08-01）

## 1. 施工依据和边界

本轮以用户提供的“调整后的正式施工顺序”完整原文为施工依据。

- 26～29 号文档仍是修改稿，本轮不归档、不声明冻结；
- `docs/design/60-contract-alignment-plan.md` 只保留为历史阶段记录，不是当前主计划；
- 本轮允许修改内部 `logicrpc`，不修改公网 v3 Proto field number；
- 阶段 0～4 不引入新的 Redis Key/Hash Tag/Lua 原子边界；阶段 5 仅新增短 TTL、非业务数据的
  `GateInstanceLease` Key，前缀可配置且没有与业务 CAS 合并；
- 不修改 Workflow Bucket 物理格式；
- 不冻结最终 `StoredOperationOutcome` 结构。

双仓起始基线和草稿等待门见
`docs/verification/draft-dependency-matrix-20260801.md`。

## 2. 已实施发布单元

### PR-0：基线、验证入口和草稿依赖矩阵

- 记录两个仓库的起始分支、HEAD 和脏工作区事实；
- `scripts/verify-s2.sh` 固定 Go vet/test/race/build、H5 单测、staging build、web build；
- Web E2E 和会停启服务的故障 E2E 通过显式环境变量启用；
- 压测遗留的 pprof、block profile 和 SessionEpoch cache 改为显式 opt-in；
- `blobstore` 同 epoch Lua 优化保留为独立压测改动，不与本轮正确性提交混合。

### PR-1A：H5 Push-only Join

- JOIN `ACCEPTED` 始终按 `CommandResult` 解码，不再把 Result 内 Snapshot 当入场快照；
- H5 每条连接最多保留一个有界 `PendingJoin` 和一个候选 Snapshot；
- `ACCEPTED` 与 Snapshot 可乱序到达，但 subscription 必须相同；
- 两个锁存器都满足后才进入 `InFarm`；
- Join 私有状态与公共 Snapshot 合并，避免公共 Push 擦除背包/钱包；
- 新 Join 替换旧候选，Join timeout 与候选 Snapshot 共用 deadline；
- server connection sequence 必须严格连续，空洞直接进入断线恢复；
- Join 不进入 Durable Operation Journal；完整 WSS 重连暂时保持现有 operation ID 策略。

### PR-1B：farm-svr Result 瘦身和内部 Fence

- `JOIN_FARM` 强制要求 `operation_id`；
- Join Result 只保留 `subscription_id` 和短期私有状态，不携带 FarmSnapshot；
- Join Snapshot 的内部 `DownlinkItem` 增加 `join_operation_id`、`target_farm_id`；
- 新增内部 `CancelJoin(operation_id, optional subscription_id, full fence)` RPC；
- `FarmActor` Presence 记录 Join operation，取消时同时匹配 operation、可选 subscription 和完整连接 Fence；
- `CancelJoin` 通过目标 `FARM:farm_id` 的 SystemQueue O(1) 投递，不扫描全部 Actor；
- 迟到的旧 Fence 不能删除重连后同 operation 的新 Presence。

### PR-1C：Gate PendingJoin 兼容/观测模式

- Gate 与 ConnectionRegistry 都维护 `Pending → Canceling → Closed` 生命周期；
- PendingJoin 有 CreatedAt、共享 deadline 和 Closed tombstone；
- 同一 operation/farm 的网络重试复用原意图和 deadline，不重置计时器；
- 新 Join 先精确关闭旧 Join，再创建新意图；
- timeout 不后台偷偷重试，不调用 connection-wide Actor 扫描；
- timeout 先记录锁存器状态，再发送精确 `CancelJoin`，最后进入 Closed；
- Closed 后的 ACCEPTED、Snapshot 和错误 Fence 只计数并丢弃，不能复活订阅；
- `STRICT_PENDING_JOIN_FENCE=0` 为默认兼容/观测模式；旧 Snapshot 只允许绑定到已经存在的 PendingJoin；
- 严格模式代码和测试已具备，但未越过观察门删除兼容分支。

已暴露 Go/No-Go 指标：

- `farm_gate_pending_join_current`
- `farm_gate_pending_join_age_seconds`
- `farm_gate_join_pending_timeout_total`
- `farm_gate_join_accept_without_snapshot_total`
- `farm_gate_join_snapshot_without_accept_total`
- `farm_gate_join_subscription_mismatch_total`
- `farm_gate_join_stale_callback_total`
- `farm_gate_legacy_implicit_bind_total`

当前本地真实 Web E2E 后，这些计数器均为 0，PendingJoin gauge 为 0。这只是一轮本地证据，
还不是足以删除兼容分支的生产观察窗。

### PR-2：TIMEOUT_UNKNOWN 同 Turn 冻结和 v2 样板

- `FarmActor.save` 在不确定 CAS 时捕获 domain-specific `UnknownContext`：operation、写前版本、writer epoch、command type、activation epoch；
- `FarmRegistry` 在发生 Unknown 的同一个 Actor Turn 内同步进入 `RESOLVING_UNKNOWN`；
- 已排队的后续 User Task 在准入时得到 Busy，不能越过不确定点；
- resolver 只在 SystemQueue 中权威重读，receipt 证明 committed，成功权威读且无 receipt 才判定 not committed；
- 强写 `ErrTimeoutUnknown` 对外统一为 `PROCESSING + ERR_TIMEOUT`；
- Player 没有缓存 Actor，不做 Farm 式常驻冻结，但下一次 Turn 必须重新加载权威记录；
- 修复 Player `GET_OPERATION_RESULT`：查询原 operation receipt 时不拿查询 Envelope 的 hash 和原业务 payload hash 误比；原业务命令重放仍严格校验 hash。

新增 v2 故障样板覆盖：

1. Buy CAS 已提交但返回 timeout：首响 `PROCESSING`，随后查询得到 SUCCEEDED receipt；
2. 同一 Buy operation 重放不重复扣款或加种子；
3. Farm Plant CAS 的 committed / not-committed 两种 timeout 分支；
4. Farm Water CAS 已提交但返回 timeout，权威重读后只重放一次；
5. Unknown 同 Turn 冻结，排队 User Task 不执行；
6. 权威重读后 receipt 恢复原结果；
7. 同一 Plant operation 最终最多产生一个 CropCycle，Water 不重复推进 farm version。

### 阶段 3：ActiveSubscription 精确 Disconnect

- 新增内部 `CleanupFarmSubscription(target_farm_id, subscription_id, presence_epoch, full fence)` RPC；
- Join Snapshot 通过 Gate 的两锁存器屏障后，将 `presence_epoch` 一并提升到本地 ActiveSubscription；
- Disconnect 捕获 ActiveSubscription 和 PendingJoin 后再删除本地连接：
  - ActiveSubscription 路由到唯一的 `FARM:farm_id`，通过 SystemQueue 执行 `LeaveFenced`；
  - 未终态 PendingJoin 另行发送 `CancelJoin(operation_id, optional subscription_id, full fence)`；
- 精确清理不激活不存在的 FarmActor，可安全重放，旧 Presence/Fence 不能删除复用连接后的新 Presence；
- Routed 与 Sharded Logic client 都只把清理请求发送到 farm-svr；动态路由模式携带 `FARM:farm_id` 路由元数据；
- 旧 `CleanupConnection/CleanupConnectionFence` 全扫描函数暂时保留为兼容接口，但生产 `gatewayapp` 不挂载它们；
- 新增 `subscription_cleanup_actor_scan_total`：只要旧扫描函数被调用，即使当时没有 Actor，也会递增。

### 阶段 4：per-Gate Downlink 隔离

旧分发器只有一个进程级队列和一个发送协程。混合 Gate 批次进入 `RoutedGatewayClient` 后仍按 Gate 顺序发送，
并使用 `WaitForReady(true)`；一个失联 Gate 可以无限占住整个下行队列。

当前实现改为：

- `gateway_id → 独立 bounded lane → 独立 batch → 独立 RPC deadline`；
- 同一 Gate 的 Item 和 QueueAccepted/失败 callback 保持 FIFO，不会发生 Join continuation 越序；
- 不再使用 `WaitForReady`，默认每个批次 2 秒 fail-fast deadline；
- 不同 Gate Lane 可并发推进，坏 Gate 只阻塞自己的 Lane；
- 全局 `MaxConcurrentRPCs` 默认 16，防止 Gate 数增长时产生无界网络并发；
- 每 Gate 队列默认 256、batch 默认 32、Lane 总数默认最多 1024；
- 空闲 Lane 默认 1 分钟回收，避免动态 Gate 生命周期留下永久 goroutine/queue；
- route 缺失、Lane 超限、单 Lane 满都会 fail closed，由现有 Farm Resync 路径处理。

新增指标：

- `farm_downlink_lanes_current`
- `farm_downlink_lanes_created_total`
- `farm_downlink_lanes_retired_total`
- `farm_downlink_queue_full_total`
- `farm_downlink_rpc_failure_total`

### 阶段 5：SubscriptionHub、FarmEventLane 与 GateBoot 生命周期

#### 为什么不能继续让 Presence 跟着 FarmActor

原实现把 `Presence`、`presenceByPlayer` 和 Join 缓冲都放在 `FarmActor`。这会产生两个错误耦合：

1. 只要还有观看者，Actor 就不能真正钝化，否则订阅和 Join 屏障会随 Actor 指针一起消失；
2. Actor 的持久业务状态生命周期与 Gate 连接生命周期被迫相同，Actor 重载、失权和普通 idle
   passivation 无法表达不同清理语义。

当前实现由 `SubscriptionHub` 按 `farm_id` 持有运行态 Presence，Actor 激活时只借用该 farm 的视图：

- 普通 `FarmActor` passivation 只丢弃业务缓存，Presence 留在 Hub；
- Actor 再激活时重新挂接同一 Hub 状态；
- 精确 disconnect 即使 Actor 已 ABSENT，也直接进入 Hub System Lane，不激活 Actor；
- 当 Actor 已 ABSENT 且最后一个 Presence 被清除时，Hub 独立 retire 空 Lane 并结束其 goroutine；
- Holder 失权或 Actor panic 才执行 `DrainFarm + RESYNC_REQUIRED`，不把普通钝化误判为连接故障。

#### 为什么还需要 FarmEventLane

仅把 map 挪到 Hub 仍不能保证 Join 快照与增量顺序。`JoinPrepared(Snapshot V)` 与随后已经提交的
`FarmChange(V+1...)` 必须进入同一条 per-farm Lane；否则快照可能覆盖更新后的地块。

当前 `FarmEventLane` 提供：

- 每 farm 独立、默认 128 个待处理事件的有界业务队列；
- `Join`、Snapshot queued/flushing、已提交 `FarmChange` 使用同一 FIFO Lane；
- disconnect、CancelJoin、GateBoot 清理使用独立 System admission，业务队列满不能饿死清理；
- 已成功 CAS 后若事件 Lane 已满，不回滚业务真值、不把成功改报失败，而是清除该 farm 的不完整
  Presence 并生成 `RESYNC_REQUIRED(reason=farm_event_lane_full)`；
- 版本空洞和原有 Join buffer items/bytes/duration 超限仍走精确 Resync。

Presence 有 24 小时长 TTL 兜底，由低频 Reaper 扫描 Hub；该 TTL 在每次 Push、心跳或业务操作时
都不会续期。正常存活判断依赖一份 GateBoot Lease，而不是给每个 subscription 制造 Redis 高频写。

#### 为什么 GateBoot Lease 不能复用 SessionRoute Lease

`SessionRoute` 证明某个玩家会话的 Fence 仍有效；它不能证明承载几千连接的 Gate 进程本身仍活着。
若为每个 Presence 单独续租，会把连接数直接变成 Redis 写放大。

当前每次 Gate 进程启动生成新的 `(gateway_id, gateway_boot_id)`，并只续约一份 15 秒 Redis TTL：

- Redis Key 只保存诊断截止时间，Redis TTL 是存活权威；
- 续约失败不会延长本地 `trusted_until`；
- 越过本地可信截止时间后 `/readyz=503`，Direct WebSocket 在 HTTP Upgrade 前返回 503，
  gateproxy 兼容入口仍在新 `ClientHello` 处拒绝；
- 已建立会话的 owner-write 被拒绝，read-only 恢复查询仍允许；
- Redis 恢复并成功续约后可自动回到 Ready，不要求进程重启；
- Gate 与 farm-svr 使用同一个可配置前缀 `GATE_BOOT_LEASE_PREFIX`。

`GateBootReaper` 不枚举全局 Gate，只从本 Holder 的 Hub boot index 读取实际存在的 Fence：

```text
SubscriptionHub local boot index
  -> (gateway_id, gateway_boot_id)
  -> Redis EXISTS exact GateBoot Lease
  -> alive: retain
  -> absent: ReapBoot(exact subscription_id + presence_epoch + full boot fence)
  -> Redis error: retain and retry (authority uncertainty must not delete Presence)
```

新增指标：

- `farm_gate_boot_lease_trusted`
- `farm_gate_boot_lease_seconds_remaining`
- `farm_gate_boot_lease_renew_attempts_total`
- `farm_gate_boot_lease_renew_successes_total`
- `farm_gate_boot_lease_renew_failures_total`
- `farm_subscription_hub_active_lanes`
- `farm_subscription_hub_boot_fences`
- `farm_subscription_hub_lanes_retired_total`
- `farm_event_lane_full_total`
- `farm_event_lane_resync_total`
- `farm_gate_boot_reaper_*`
- `farm_subscription_fallback_expired_total`

## 3. 实施中发现并修复的问题

### 同 Join 重试的旧 Timer 竞态

原实现会用同 operation 覆盖 PendingJoin，却留下旧 timer。旧 timer 到期后可能提前取消新对象。
现改为同 operation/farm 复用原 PendingJoin、原锁存器和原 deadline。

### Player 轮询误报幂等冲突

`GET_OPERATION_RESULT` 与原 `BUY_OFFER` 的 Envelope hash 必然不同。查询路径现在只用 operation ID
定位 receipt；只有重放原业务命令时才比较原 payload hash。

### 本地 Compose 可能打包旧二进制

`Dockerfile.prebuilt` 只打包 `.local-build/*`。仅运行 `docker compose up --build` 不会重新编译 Go，
可能继续使用旧二进制。当前正确顺序是：

```bash
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build --force-recreate \
  player-svr farm-svr farm-gate farm-worker
```

本轮已按该顺序重建，四个服务均 healthy。

### 生产 Disconnect 原先没有真正清 Farm Presence

旧代码虽然存在 connection-wide cleaner 接口，但 `gatewayapp` 没有注入该接口，因此真实多进程部署断线时
只会清 Gate 本地连接、下行 Registry 和 Redis SessionRoute，不会清 farm-svr 内的 Presence。直接把旧接口
接上又会变成 O(当前进程全部 FarmActor) 扫描。本阶段没有启用扫描接口，而是新增 Gate → farm-svr 的
精确内部 RPC，一次只进入目标 FarmActor 的 SystemQueue。

滚动发布顺序必须是 farm-svr 先于 farm-gate。新 Gate 调旧 farm-svr 会收到 Unimplemented；生产路径不会
为了掩盖版本错误回退到全 Actor 扫描。

### `WaitForReady` 与 bounded queue 并不能共同提供隔离

旧注释认为“队列有界，所以 `WaitForReady(true)` 可以安全等待 Gate 恢复”。实际问题是有界只限制积压数量，
不限制第一批占用发送协程的时间；当这个协程同时服务所有 Gate 时，一个坏 Gate 会让健康 Gate 的 Item 也
排在后面。阶段 4 同时引入 per-Gate Lane 和 RPC deadline，而不是只给旧混合批次补一个 timeout。

### Alpine 镜像中的预编译 Go 二进制必须是静态链接

阶段 5 第一次真实重建时，四个镜像能成功 build，但 `player-svr` 以
`exec /usr/local/bin/farm-service: no such file or directory` 退出。文件实际存在；根因是宿主机默认
Go build 产生了依赖 glibc loader 的 ELF，而运行镜像是 Alpine/musl。

本次改为 `CGO_ENABLED=0 go build` 后，`file` 确认为 `statically linked`，四个服务全部 healthy。
这也说明 `Dockerfile.prebuilt` 的准备脚本必须固定静态编译，不能只检查二进制文件是否存在。

## 4. 验收证据

后端：

- `go vet ./...`：通过；
- `go test ./...`：通过；
- `go test -race ./internal/...`：通过；
- `go build ./...`：通过；
- Join/CancelJoin、PendingJoin 严格/兼容模式、Player/Farm Unknown 定向测试：通过。
- 精确 Disconnect、旧 Fence/连接 ID 复用、Absent Actor、SystemQueue 满载和 Routed/Sharded/gRPC 转发测试：通过。
- per-Gate 坏/好 Gate 隔离、全局 RPC 并发上限、callback 顺序、Lane queue bound/idle retirement 和指标测试：通过。
- SubscriptionHub 跨 Actor passivation、ABSENT Actor 精确清理、不重新激活测试：通过；
- FarmEventLane FIFO、queue bound、已提交变更遇到 Lane 满时保留业务版本并强制 Resync：通过；
- GateBoot initial-untrusted、续约恢复、过期后拒绝 owner-write/允许 read-only：通过；
- Direct WebSocket 的可恢复 admission guard 在 Upgrade 前返回 503，恢复后无需重启即可重新接入：通过；
- GateBootReaper 只检查本地 Hub Boot、精确删除 dead Boot、保留 alive Boot：通过；
- 上述阶段 5 相关包 `go test -race`：通过。

H5：

- Vitest：13 个测试文件、72 个测试通过；
- `npm run build:staging`：通过；
- `npm run build:web`：通过；
- Web account E2E：注册、再次登录、双向好友通过；
- Direct WebSocket staging E2E：个人经济闭环/重连、好友实时更新、顶号、好友农场返回、
  双端浇水和偷菜同步，共 5/5 通过；
- Web 故障 E2E：停 Worker/Player、恢复未决收获、全后端重启、Redis 恢复通过。

本次阶段 3 重建四个服务后，真实 Web account E2E 再次通过。该轮连接关闭产生了：

- `farm_rpc_calls_total{method="CleanupFarmSubscription",code="OK"} = 3`；
- `subscription_cleanup_actor_scan_total = 0`；
- `farm_gate_pending_join_current = 0`；
- Join timeout/mismatch/stale/legacy 计数均为 0。

这证明本地 Web 模式确实调用了精确清理 RPC，同时没有误入旧扫描路径；它仍不是 PR-1D 所需的生产观察窗。

阶段 4 重建 farm-svr/farm-gate/farm-worker 后，Web account E2E 再次通过；对应本地指标为：

- `farm_downlink_lanes_created_total = 1`、`farm_downlink_lanes_current = 1`（本地单 Gate）；
- `farm_downlink_queue_full_total = 0`；
- `farm_downlink_rpc_failure_total = 0`；
- `CleanupFarmSubscription OK = 3`；
- `subscription_cleanup_actor_scan_total = 0`。

阶段 5 使用公司 Redis、`GATE_TRANSPORT=websocket` 重建四个服务后均 healthy，Web account 1/1、
staging 5/5 通过。观察值：

- `farm_gate_boot_lease_trusted = 1`；
- `farm_gate_boot_lease_renew_attempts_total = 26`；
- `farm_gate_boot_lease_renew_successes_total = 26`；
- `farm_gate_boot_lease_renew_failures_total = 0`；
- `farm_gate_boot_lease_seconds_remaining ≈ 14.8`（15 秒 TTL 的一次采样）；
- `farm_event_lane_full_total = 0`、`farm_event_lane_resync_total = 0`；
- `farm_gate_boot_reaper_sweeps_total = 82`、`boots_checked_total = 8`；
- `dead_boots_total = 0`、`errors_total = 0`、`presences_removed_total = 0`；
- 浏览器退出后 `farm_subscription_hub_boot_fences = 0`，说明精确断连已清空运行态 Boot index。
- `CleanupFarmSubscription OK = 17`、`subscription_cleanup_actor_scan_total = 0`；
- `farm_subscription_hub_lanes_retired_total = 0`：本轮浏览器关闭只清 Presence，尚未触发 Actor idle
  passivation；Actor ABSENT 后 Hub 独立 retirement 已由单测覆盖，不能把本地未触发 idle timer 误写成生产观察证据。

## 5. 尚未完成/不能提前宣布完成

- PR-1D 的兼容分支删除尚未执行：必须先有真实观察窗证明
  `legacy_implicit_bind_total == 0`；
- 阶段 3 已实现，但跨版本上线必须遵守 farm-svr → farm-gate 顺序；
- 阶段 5 的 Hub/GateBoot Lease 已完成；完整 Activation 生命周期、Journal v2、Workflow
  物理迁移和 v3 灰度仍属于后续阶段；
- 公网 v3 Proto、Redis 物理结构、Workflow Bucket、最终 StoredOperationOutcome 仍在等待门内。
