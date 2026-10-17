# 好友机会 3000 万 DAU 容量优化实施交接

日期：`2026-08-12`
分支：`codex/friend-opportunity-30m-capacity-r1`
状态：`C0-C6 本地实现完成；尚未合入 Integration；尚未推送；多 Pod 最终容量验收待执行`

## 1. 本轮交付结论

当前分支已经把 82 号计划中可以在单机和组件层完成的优化串成一条可运行基线：

```text
Farm Authority Commit
→ 专用 ScheduleDueIndex
→ Worker 只扫描 OwnedShards
→ 批量读取 Runtime SessionRoute
→ 按 Gate 分组 Direct DeliverBatch
→ Gate 每连接有界队列与 Hint 软配额
→ WSS Hint
→ H5 定向精查 / Pull 恢复
```

它解决的是明显的结构性放大和失控风险，不等于已经用 172 万并发连接证明
3000 万 DAU。最终结论仍需多 Gate、多 Worker、三个独立 Redis 集群和真实混合流量。

## 2. 分支提交

| 提交 | 内容 |
|---|---|
| `d8f9502` | 专用 ScheduleDueIndex、迁移模式、重建与探针 |
| `66a7318` | Redis Authority / Runtime / Projection 三域 Client 生命周期 |
| `1d5af0d` | farm-gate 与 player-svr Redis 分域注入 |
| `6b619d8` | farm-svr 与 farm-worker Redis 分域注入 |
| `0c822d2` | Friendship 双域注入和 Compose 三域配置 |
| `56f3b1c` | Gate 下行分类、Hint 软配额、慢连接 Harness、排空通知修复 |
| `71d54a0` | Gate 容量参数进入真实启动链与部署配置 |

分支还包含 C0-C4 的观测、CandidateWindow Top-K、SessionRoute 探针和 Worker
Assignment 实现，完整提交顺序以 `git log` 为准。

## 3. Redis 三域怎么落地

### 3.1 配置

```text
FARM_AUTHORITY_REDIS_URL
FARM_RUNTIME_REDIS_URL
FARM_PROJECTION_REDIS_URL
```

每个域创建独立 go-redis Client 和连接池。某个域未配置时，只回退到
`FARM_REDIS_URL`。因此：

- 本地仍可只配置一个公司 Redis；
- 容量环境可逐域切到不同 Redis 集群；
- 即使三个 URL 当前相同，也已经实现连接池和调用归属隔离；
- 三个 URL 相同不能提供故障域、CPU 和内存隔离，不能当作正式拆集群证据。

### 3.2 数据归属

| 域 | 当前数据 |
|---|---|
| Authority | AccountCredential、Player/Farm 强事实、Header/Facts、Schedule、canonical FriendRelation、Workflow |
| Runtime | SessionAuthority、SessionRoute、ConnectionTicket、登录限流、GateBootLease |
| Projection | FriendIndex、修复/导航/CandidateWindow、ResultDirectory、好友机会 Hint sidecar |

PublicProfile 继续走既有 PlayerReadAPI/Authority 读路径。本轮没有为了“看起来分域”
额外制造资料投影。

### 3.3 已验证内容

- 三域环境变量各自解析，缺省时兼容旧变量；
- 相同地址仍建立三个独立 Client/Pool；
- 部分域启动失败会回滚关闭已经打开的 Client；
- Gate/Player/Farm/Worker 的典型数据只写入其所属 miniredis；
- Compose 在旧单 URL 和三个独立 URL 两种模式下均能通过 `config --quiet`；
- 错误日志不输出包含密码的 Redis URL。

## 4. Gate 长连接与背压

### 4.1 生产配置

```text
GATE_DOWNLINK_WRITER_MAX_ITEMS=256
GATE_DOWNLINK_WRITER_MAX_BYTES=1048576
GATE_DOWNLINK_HINT_MAX_ITEMS=32
GATE_DOWNLINK_HINT_MAX_BYTES=65536
GATE_DOWNLINK_DRAIN_QUANTUM=64
```

这些值是首轮候选，不是最终定容值。启动时校验：

- 总 item/byte 必须大于 0；
- Hint 配额不能为负，也不能大于总上限；
- drain quantum 必须大于 0；
- 一旦 Gate 已接纳连接，不能再修改 Writer 容量。

### 4.2 语义

- Hint 在分配下行 sequence 前检查软配额，超额 Hint 可以丢弃；
- 被丢 Hint 不会形成 sequence 空洞，由登录、回家、页面恢复和 fallback Pull 补偿；
- FarmChange 与 CommandResult 不受 Hint 软配额限制，但仍受连接总上限保护；
- 每条连接有自己的 Writer，慢连接只填满自己的队列；
- WritePump 每次最多排空一个 quantum，然后回到 `select`，给同步结果和心跳调度机会；
- level-triggered notifier 保证队列仍有数据时会重新唤醒，避免 lost wakeup。

## 5. 已执行验证

```bash
go test -race -count=3 \
  ./internal/gateway/command \
  ./internal/gateway/downlink \
  ./internal/gateway/transport/websocket \
  ./internal/gatewayapp

go test ./...

docker compose -f deploy/docker-compose.yml config --quiet
docker compose -f deploy/docker-compose.local.yml config --quiet
```

结果：全部通过。C6B 慢连接 Harness 覆盖 `1% / 5% / 20%` 三组，验证：

- 健康连接只对 Hint 发生软拒绝，FarmChange/CommandResult 能进入保留容量；
- 慢连接队列满不会占用其他连接的队列；
- Hint 配额在传输排空后重新开放；
- 接受后的单连接 FIFO 不乱序；
- race detector 未发现数据竞争。

C5 单 Redis 证据位于：

```text
docs/verification/perf/friend-opportunity-c5/results/
schedule-due-c5-full-20260811T211215Z-1922807/
```

C6B 组件证据位于：

```text
docs/verification/perf/gate-c6b/c6b-harness-20260812/
```

## 6. 尚未完成的最终验收

以下事项不能用现有单机测试替代：

1. 三个真实独立 Redis 集群下的 CPU、带宽、连接池等待和故障隔离；
2. 多 Gate 下 1%、5%、20% 慢连接与真实 WebSocket 扇出；
3. 多 Worker Assignment 迁移、Drain、崩溃和 due lag；
4. 多 player-svr/farm-svr 的 Actor 路由与容量线性扩展；
5. 登录冷扫、好友分页、Hint、经营写混合流量；
6. 按峰值约 5730 新会话/秒、172 万同时在线估算 Gate 数量和安全余量。

## 7. 下一批建议

按以下顺序继续，避免同时改变语义和容量参数：

```text
1. 三 Redis 实例功能/故障探针
2. 多 Gate 真实 WSS 慢连接压测
3. 多 Worker OwnedShards 扩缩容与 Drain
4. R1-R4 混合流量矩阵
5. 根据 CPU/RSS/P99 计算实例数量
6. 评审后再决定是否合入 Integration
```

只有混合负载证明 SessionRoute 或 Authority Header 成为主瓶颈时，才重新评估
OnlineRouteBucket 或 FarmOpportunitySummary；当前不要提前引入第二套投影。
