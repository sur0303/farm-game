# A8 · 三主三从 Redis Cluster 适配与验证记录

日期：2026-08-14  
工作树：`farm-game-source-order-scaleplan-a8-20260814`  
分支：`codex/source-order-scaleplan-a8-20260814`  
范围：SourceOrder 合入扩缩容分支后的 Redis Cluster 适配审计。本文不改变 26～31 号业务与存储合同。

## 1. 当前平台假设

当前 Redis 平台为三主三从 Cluster。应用只需要配置至少三个可达 seed URL；客户端启动后通过
Cluster 拓扑发现其余主节点和副本节点。业务代码不把“主 1/主 2/主 3”或具体 Redis Slot
写入路由表，也不把 replica 当作业务 Holder。

```text
FARM_REDIS_CLUSTER_URLS=redis://master-1:6379,redis://master-2:6379,redis://master-3:6379
        │
        └─ redis.NewClusterClient
             ├─ 自动发现 3 个 Primary
             ├─ 自动发现各自 Replica
             └─ 根据 Hash Tag 将命令发往当前 Slot Owner
```

Cluster 模式使用 logical database `0`。不能通过 `SELECT db` 隔离业务；隔离依赖 namespace、
ACL、Key 前缀和不同 Cluster/连接池。单节点 URL 仍保留为本地和兼容回退配置。

配置优先级为：

```text
FARM_<DOMAIN>_REDIS_CLUSTER_URLS
→ FARM_REDIS_CLUSTER_URLS
→ FARM_<DOMAIN>_REDIS_URL
→ FARM_REDIS_URL
```

## 2. 已完成的代码适配

### 2.1 业务服务连接

`internal/redisdomains` 现在统一解析单节点 URL 和 Cluster seed 列表：

- 单节点使用 `redis.NewClient`；
- Cluster seed 使用 `redis.NewClusterClient`；
- 业务依赖统一使用 `redis.UniversalClient`；
- 保留密码、TLS、协议、超时、重试、连接池和身份配置；
- Cluster 拒绝非零 logical DB，避免配置看似成功但运行时无法 `SELECT`。

已接入的核心进程包括：

- `player-svr`；
- `farm-svr`；
- `farm-worker`；
- `farm-gate` 的账号、好友和投影访问；
- `route-controller`；
- `rt2q-route-bootstrap` 和 `schedule-due-index-tool` 等运行维护工具；
- 好友机会性能夹具和审计工具。

### 2.2 同槽原子操作

Redis Cluster 只允许同一 Slot 内执行 Lua/事务。代码继续由 KeyBuilder 生成完整 Hash Tag，
不允许业务 Payload 自己指定 Redis Key：

```text
同一个 Authority Bucket
→ ShardWriteAuthority
→ ActorRecord
→ Receipt / SourceOrder / DueIndex
→ 使用完全相同的 {hash-tag}
```

Player、Farm、Friend Pair、Mail、Workflow 和 RouteControl 各自有自己的逻辑 Bucket。不能为了
“减少一次 Redis hop”把 Player 与 Farm 强行放到同一 Slot；跨 Actor 结果仍通过 SourceOrder、
ResultDirectory 或 Projection 异步发布。

### 2.3 已修复的 Cluster 不兼容点

`sourcework.go` 原来把无 Key 的 `TIME` 与带 Key 的 `HMGET` 放在同一次事务 Pipeline 中。
Cluster 下这两个命令无法保证被路由到同一 Slot，因此现在改为：

```text
先单独读取 Redis TIME
→ 再按 Actor Authority Slot 执行 HMGET
```

`Redis TIME` 只作为诊断/读取时间来源；真正的 Authority Lua 仍在同槽内使用脚本合同规定的
时间与 Writer Fence。

生产探针 `cmd/redis-production-probe` 现在也支持 Cluster seed：

- 使用 `redisdomains.OpenEndpoint`；
- Lua 探针使用带 Key 的 `redis.NewScript`，避免 Cluster 下无 Key 脚本随机落到不同节点；
- `WAIT` 在 Cluster 模式标记为 `NOT_APPLICABLE`，不把它误报成跨主节点的持久化保证；
- 仍可用单节点 URL 检查传统 `WAIT` 返回的副本 ACK。

路由初始化和 DueIndex 维护工具也支持对应的 Cluster seed 环境变量，避免上线后因为工具仍
使用单节点客户端而无法访问 MOVED Slot。

## 3. 没有改变的业务语义

以下内容仍由 26～31 号文档负责，Redis 从单节点变成 Cluster 不改变它们：

- Actor → VirtualShard → Holder Pod 的业务路由；
- `writer_epoch`、Lease、Drain/Handoff 和旧 Holder Fence；
- `record_version`、幂等 Receipt 和 `TIMEOUT_UNKNOWN` Resolver；
- FarmActor 的 Plant/Harvest/Care/Theft 串行裁决；
- SourceOrder 的来源事实与 Order 同槽提交；
- Worker 通过 DueIndex 恢复，不通过 Redis Key 过期事件承担可靠任务；
- Projection 不是最终权限或资产真值。

Redis Cluster 的 Slot 迁移不等于 Actor Shard 迁移。Redis 客户端处理 Cluster 的 MOVED/ASK；
RouteControl 仍单独处理业务 Shard 的 Holder 迁移。两套迁移不能混为一谈。

## 4. 验证结果

在没有提供真实三主三从连接地址和凭证的前提下，已完成：

- `go test ./internal/redisdomains ./internal/storage/redis/authority ./internal/storage/redis/routecontrol ./internal/storage/redis/playermail ./internal/friend ./internal/workflow ./internal/storage/blobstore ./internal/gatewayapp ./cmd/player-svr ./cmd/farm-svr ./cmd/farm-worker ./cmd/route-controller -count=1`；
- `go test ./cmd/friend-opportunity-perf-fixture ./cmd/friend-opportunity-audit ./cmd/redis-production-probe -count=1`；
- `go test ./... -count=1`；
- Docker Compose 配置解析；
- K8s JSON 资源和 Secret 模板 JSON 解析；
- `scripts/company-redis-compose.sh` 的 shell 语法检查；
- 同槽 KeyBuilder/Lua/CROSSSLOT 相关单测。

以上证明的是代码和合同兼容性，不等于已经验证真实平台的主从切换、Slot 迁移或故障 RPO。

## 5. 真实三主三从上线前必须补的探针

拿到测试 Cluster seed 和最小权限账号后，按以下顺序执行：

1. 连接发现：确认三个 seed 能返回完整 Cluster 拓扑，六个节点均可达；
2. Slot 路由：写入不同 Hash Tag，确认命令被路由到对应 Primary；
3. 同槽 Lua：执行 Authority、Friend Pair、Mail、Workflow 的代表性脚本，确认无 `CROSSSLOT`；
4. MOVED/ASK：在平台批准的窗口记录客户端重定向与恢复延迟；
5. 主从切换：观察 `TIMEOUT_UNKNOWN`、旧 Writer Fence、重试和 Resolver 结果；
6. 副本与持久化：分别记录复制延迟、AOF/RDB、RPO/RTO，不能用 `WAIT` 结果替代 Cluster 级结论；
7. 压测：按 Player/Farm/Worker/Projection 连接池和 Slot 热点分别记录 p50/p95/p99、CPU、
   网络字节、命令失败率、CROSSSLOT 和连接重建。

## 6. 仍保留单节点语义的工具

`cmd/redis-ha-probe`、部分历史 smoke/fixture/harness 仍是单节点或单地址工具，不能直接用它们
证明三主三从 Cluster 的行为。它们不在核心服务热路径；在真实 Cluster 验收时应使用
`redis-production-probe` 或另行增加 Cluster 专用故障注入工具，避免把单节点 `WAIT`/地址语义
误用于 Cluster 结论。

## 7. 结论

当前 A8 已经把“业务服务支持三主三从 Cluster”补齐，且保持了原有 Actor、RouteControl、
SourceOrder 和同槽原子边界。当前尚未完成的不是代码接口，而是真实公司 Cluster 环境的拓扑、
主从切换、Slot 迁移、持久化和容量验收。未取得这些原始日志前，不宣称已经完成生产级容灾验证。
