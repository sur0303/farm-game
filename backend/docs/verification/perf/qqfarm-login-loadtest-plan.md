# QQFarm 登录与在线 E2E 压测计划

> 状态：执行基线（2026-08-18）
>
> 本计划用于区分冷登录、重连和在线连接容量。三者不得合并为一个“登录 QPS”结论。

## 1. 目标

回答以下三个独立问题：

1. 每秒可以接收多少个从未使用过的 Player ID？
2. 已有账号发生重连时，每秒可以处理多少次新 Session/WebSocket 建立？
3. 系统可以稳定维持多少个在线 WebSocket 和多少业务操作 QPS？

正式容量结论必须同时包含：

```text
冷登录吞吐 + 重连吞吐 + 在线连接容量
```

## 2. 三种测试模型

| 模型 | Player ID | HTTPS Login | WebSocket | Ready 后行为 | 测试目标 |
|---|---|---|---|---|---|
| 冷登录 | 每次全新、只用一次 | 每次执行 | 每次新建 | Ready 后关闭 | Actor 激活、建档、Redis 加载和新用户登录吞吐 |
| 重连 | 复用固定账号池 | 每次重新登录/续期 | 每次新建 | 按重连循环 | Session、Ticket、连接重建和热 Actor 路径 |
| 在线保持 | 复用固定账号池 | 只登录一次 | 长期复用同一条 | 保持在线并执行业务 | 并发连接、心跳、业务命令、长稳内存 |

当前工具对应关系：

- `cold-thick-login`：全新 ID，Ready 后关闭连接；不代表在线连接容量。
- `reconnect`：复用账号、重新建立连接；不代表全新 Actor 冷激活。
- `thick-login`/在线路径：建立连接后保持；用于在线连接验证。若要作为完整游戏 E2E，必须包含心跳和代表性业务操作，不能只保持空闲 WebSocket。

HTTP 登录使用共享 keep-alive Transport 可以复用 TCP/TLS 连接，但 WebSocket 认证连接不能跨账号复用。

## 3. 压测数据池

### 3.1 冷登录 ID 池

冷登录只需要唯一的 Player ID，不需要提前准备 Player/Farm 数据。当前平台 Token 登录会在登录链路中执行幂等建档，因此以下操作禁止发生在正式冷登录前：

- Login；
- Bootstrap；
- Join；
- 预热 Actor；
- 提前建立好友关系。

每个 ID 只能使用一次，并且必须与历史所有压测区间不重叠。每个 Job 使用独立区间；多个 Job 不得共享 `player-start` 范围。

本计划已登记初始区间：[qqfarm-cold-id-pool-manifest-20260818.csv](./qqfarm-cold-id-pool-manifest-20260818.csv)。这些区间当前状态为 `reserved`，正式启动某一档后才标记为 `consumed`；未完成或无效轮次中的 ID 也不得再次使用。

数量计算：

```text
冷 ID 数量 = 目标 launch rate × launch duration × 1.1～1.2
```

单档持续 5 分钟时的建议池：

| 目标总速率 | 原始 ID 数 | 建议准备量 |
|---:|---:|---:|
| 500/s | 150,000 | 165,000 |
| 1000/s | 300,000 | 330,000 |
| 1500/s | 450,000 | 495,000 |
| 2000/s | 600,000 | 660,000 |

四档全部连续执行且 ID 不复用时，约需要 1,650,000 个唯一 ID。两个发压 Job 必须将每档池拆成两个不重叠区间。

> 100,000 个 ID 在 2000/s 下只能支撑约 50 秒，不能作为 5 分钟 2000/s 的完整池。

### 3.2 热账号池

重连和在线保持使用固定账号池。该池需要在正式测量前完成一次数据准备，至少包含：

- PlayerRecord；
- FarmRecord；
- 默认资产、地块和用户状态。

好友、浇水、偷菜等业务还需要单独准备：

- 好友关系；
- 目标农场；
- 社交索引；
- 可操作的作物/地块状态。

数据准备过程不计入正式登录 QPS。热账号池数量按目标在线数计算：

```text
热账号池数量 = 目标并发在线数 × 1.1～1.2
```

当前 returning-player pool 仍受 `blobstore: writer epoch mismatch` 影响，在该问题修复并验证前，不得把它作为有效热账号池。

## 4. 测试阶段

### P0：基线与环境确认

只执行一次标准恢复流程：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

禁止运行：

- `apply-r93.sh`；
- 直接 apply staged YAML；
- 多个 reset 并发执行。

开始前确认：

- Gate、Player、Farm、Worker、Route Controller 全部 Ready；
- Redis 三个实例 Ready；
- 所有服务无 Pending Pod；
- reset 脚本没有残留进程；
- 发压 Job 已清理；
- Player/Farm 没有新的 OOM 或重启。

当前冷登录诊断配置：

- Gate：1 Pod，2 CPU，4Gi；
- Player：2 Pod，2 CPU，16Gi；
- Farm：2 Pod，2 CPU，16Gi；
- Worker：2 Pod；
- Redis：3 Pod；
- Shard：1024；
- `GATE_BACKEND_MAX_INFLIGHT=1024`；
- `GATE_BACKEND_ADMISSION_WAIT=25ms`；
- `GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT=256`；
- `PLAYER_MAILBOX_IDLE_TIMEOUT=2m`；
- `FARM_ACTOR_IDLE_TIMEOUT=2m`；
- Player/Farm idle bucket width：`5s`；
- Player/Farm sweep interval：`5s`；
- Player/Farm max per sweep：`2048`。

2 分钟 idle 是当前拟定的离线 Actor 策略；5 秒扫描和 2048/tick 用于让回收吞吐跟上冷登录创建速率，仍需通过回收压力和 Redis/RPC 指标验证后再作为生产容量结论。

### P1：冒烟 E2E

持续 3～5 分钟，使用少量全新 ID，确认：

```text
Login → WebSocket Hello → Ready → Close
```

检查协议正确性、ID 唯一性、Ready 统计和连接清理。冒烟通过后才进入容量测试。

### P2：冷登录分档容量

每档使用全新的 ID 区间，不 reset、不复用前一档 ID：

```text
500/s → 1000/s → 1500/s → 2000/s
```

建议每档：

```text
Ramp Up：1～2 分钟
目标速率：3～5 分钟
Drain：3～5 分钟
```

两个 Job 的速率之和为目标总速率。例如总速率 500/s 时：

```bash
# Job A
WS_STRESS_COLD_POOL_SIZE=165000 \
WS_STRESS_COLD_LAUNCH_DURATION=300s \
WS_STRESS_CPU=4 WS_STRESS_MEMORY=8Gi \
WS_STRESS_PRIORITY_CLASS=high-cpu-priority \
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  cold-thick-login 2000 250 360s <cold-range-a-start>

# Job B 使用另一个不重叠的 ID 区间，rate 同为 250。
```

`high-cpu-priority` 只用于发压 Pod，不用于后端服务；若产生抢占，记录被抢占的 Pod。发压端 CPU、内存、临时端口或网络错误会使该轮无效，不能归因于后端。

`conns` 应根据 Ready P95 调整，不能固定过小：

```text
conns >= 每 Job 速率 × 预计 Ready P95 秒数 × 1.5
```

### P3：在线连接稳态

使用热账号池：

```text
Ramp Up：5 分钟
Stable：20～30 分钟
Ramp Down：5 分钟
```

连接建立后保持同一条 WebSocket，并按真实用户比例执行：

- 心跳；
- 拉取农场；
- 种植；
- 好友列表；
- 访问好友农场；
- 浇水/偷菜；
- 任务或商店操作。

该阶段的主要指标是 Active WebSocket、业务 QPS、业务 P95/P99、内存斜率和连接稳定性，不是新用户 Login QPS。

### P4：重连稳定性

使用热账号池，复用账号但反复创建新的 WebSocket，持续 20～30 分钟。记录：

- 新 Session/连接速率；
- 重连成功率；
- Hello/Ready 延迟；
- Gate syscall/socket CPU；
- Actor 是否复用或重新激活；
- 客户端和服务端连接是否泄漏。

### P5：Soak 长稳

在 P3 通过的目标在线规模下运行 1～2 小时，用于发现：

- Heap/RSS 持续上涨；
- GC 恶化；
- Actor/mailbox 未回收；
- goroutine 泄漏；
- WebSocket 泄漏；
- Redis/cache 膨胀；
- P99 随时间持续增长。

### P6：峰值洪峰

在稳定基线通过后单独执行：

```text
0 → 20% → 50% → 100% → 120%～150% 目标峰值
```

每个峰值阶段保持 2～5 分钟，重点观察系统是否优雅退化，以及降压后 backlog 是否恢复。

## 5. 必采指标

### 发压端

- launch QPS；
- Login 成功 QPS；
- Ready completion QPS；
- Ready P50/P95/P99/max；
- Login、Hello、Join、Snapshot 分段延迟；
- 连接失败、超时、临时端口错误；
- 发压 Pod CPU、内存、网络和 goroutine。

### Gate

- CPU、RSS、heap、goroutine；
- Active/accepted WebSocket；
- backend inflight；
- admission wait 和 slots full；
- downlink queue；
- WebSocket handle latency；
- pprof CPU、heap、goroutine。

### Player/Farm

- CPU、RSS、heap、GC；
- Actor activation/retire；
- mailbox 数量、队列长度、等待时间；
- Redis P95/P99、pool wait；
- gRPC/RPC 延迟；
- OOM、重启、Ready 状态；
- pprof CPU、heap、goroutine。

### pprof 采集要求

pprof 必须在负载期间采集，而不是只在结束后采集：

- 稳态开始后采集 30～60 秒 CPU profile；
- 稳态开始、中点、结束各采集一次 heap；
- 采集 goroutine profile；
- 记录采集时的 QPS、Pod 名称和资源使用；
- 如果 Pod 在 profile 期间被替换，该轮 pprof 无效。

## 6. 有效轮次判定

### 后端必须满足

- 所有服务持续 Ready；
- 无 OOMKilled；
- 无新的后端重启；
- Gate Pod 不发生替换；
- Redis、Route Controller、Holder 无异常；
- downlink queue、backend slots、Actor activation 不持续堆积；
- `blobstore: writer epoch mismatch` 等依赖错误为零或已明确排除。

### 发压端必须满足

- 无 `cannot assign requested address`；
- 无大量 `network timeout`/`context deadline exceeded`；
- 无 CPU/内存 limit 压满；
- 无 Pending、Evicted、Preempted；
- 统计中的 launch QPS 与目标一致。

否则只能标记为“无效轮次”，不能称为后端容量不足。

### 容量报告至少包含

```text
目标 launch QPS
实际 launch QPS
Login success QPS
Ready completion QPS
Ready success rate
Ready P50/P95/P99
连接错误分类
Gate/Player/Farm CPU、RSS、heap
Actor/mailbox 指标
Redis 延迟
OOM/重启
pprof 结论
```

冷登录、重连和在线保持必须分别报告，不得合并成一个登录容量数字。

## 7. 当前执行顺序

1. 使用已登记的冷登录唯一 ID 区间；
2. 清理残留 Job，确认没有并发 reset；
3. 执行 P1 冒烟；
4. 执行 P2：500/s → 1000/s → 1500/s → 2000/s；
5. 每档完成 drain，并核对 Actor/内存是否回收；
6. P2 完成后再准备/修复热账号池；
7. 执行 P3 在线保持；
8. 执行 P4 重连；
9. 最后执行 P5 Soak 和 P6 峰值洪峰。

在冷登录容量阶段不重复 reset；只有出现 OOM、服务状态污染或无法恢复时，才使用唯一的 `reset-1024-initial-assignment.sh` 恢复环境。
