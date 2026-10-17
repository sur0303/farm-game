# W31-A · Namespace、KeyBuilder 与公司 Redis Probe 交接

日期：2026-08-03
状态：`IMPLEMENTATION_PASS / PLATFORM_EVIDENCE_PARTIAL`

## 1. 本包完成内容

本包按照 31 号权威合同实现了无网络依赖的 Redis 物理定位基础：

- `Namespace(environment, zone)` 严格校验，拒绝 `{`、`}`、`:`、大小写漂移和用户输入注入；
- PLAYER/FARM 使用独立 RouteDomain 与独立 Authority Hash Tag；
- `AuthorityBucketRef`、`WorkflowBucketRef` 全部携带 LayoutVersion；
- BucketRef 字段不对包外开放：Actor Ref 由 `PhysicalProfile + AuthorityShardResolver + ActorAddress` 生成，
  `ActorRecord` 再次计算 Shard 并拒绝错误 Bucket；Workflow Ref 与 workflow_id 绑定并在构造 Work Key 时复算；
- Authority、Independent Workflow、Runtime、Projection 使用不同 KeyBuilder；
- `auth-v1` 中 Authority Bucket ID 直接采用已经由路由层算出的 VirtualShard ID，不运行第二套持久化 Hash；
- Redis Cluster Slot 使用 `CRC16-XMODEM(hash_tag) mod 16384`；
- `AssertSameSlot` 不只比较数字 Slot，还要求 Hash Tag 逐字相同，防止两个不同 Tag 偶然碰撞到同一 Slot 后被错误当成一个原子域；
- Workflow BucketCount 来自版本化 Profile，不能随 Worker Pod 数变化；
- 旧 Session/Ticket/Presence Key 保留兼容，但已明确禁止用于 Actor Authority。
- 新 Runtime Builder 为 ConnectionTicket 与 SessionAuthority 生成相同 Session Hash Tag；好友导航和
  Projection 仍不进入该原子域。当前 26 号热路径继续使用旧但同槽的兼容 Key，迁移时不能拆开二者。

权威 Key 示例：

```text
{classic-farm:prod:z1:auth:player:v1:s:42}:shard-authority
{classic-farm:prod:z1:auth:player:v1:s:42}:actor:10001
{classic-farm:prod:z1:auth:player:v1:s:42}:order:CARE_REWARD:<stable_order_id>
{classic-farm:prod:z1:auth:player:v1:s:42}:due
{classic-farm:prod:z1:auth:player:v1:s:42}:receipt-due
{classic-farm:prod:z1:auth:player:v1:s:42}:bucket-meta
{classic-farm:prod:z1:auth:player:v1:s:42}:scan-lease
```

同一个 actor_id 和 shard 数字在 FARM 域的 Tag 仍不同：

```text
{classic-farm:prod:z1:auth:farm:v1:s:42}:actor:10001
```

## 2. Golden 证据

固定样例：

```text
Namespace       = prod / z1
Domain          = PLAYER
LayoutVersion   = v1
VirtualShard    = 42
ActorID         = 10001

ActorKey = {classic-farm:prod:z1:auth:player:v1:s:42}:actor:10001
RedisSlot = 122
```

CRC 独立已知向量：

```text
CRC16-XMODEM("123456789") = 0x31C3
RedisSlot("123456789") = 12739
```

Workflow v1 当前实现候选样例：

```text
workflow_id = sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s
workflow_hash_version = 1
workflow_bucket_count = 256（探针候选，不是生产常量）
workflow_bucket = 65
```

31 号只冻结了“稳定、版本化、BucketCount 为 2 的幂”的语义，尚未在正文正式冻结 Workflow Hash 的
全部字节算法。因此 `SHA-256(workflow_id UTF-8) / first uint64 big-endian / mask` 与 Bucket 65 当前是
受测试保护的实现候选，不得在合同补充确认前宣称为跨语言最终物理合同。

## 3. 公司 Redis 非破坏性探针

执行入口：

```text
FARM_REDIS_CONFIG=<local-secret-config> ./scripts/probe-company-redis-production
```

探针不会输出 URL/密码，不会执行 `SCRIPT FLUSH`、配置修改、Slot 迁移或主动故障切换。原始脱敏结果：

```text
docs/verification/results/w31a-company-redis-probe-20260803.json
```

已得到的真实证据：

| 项目 | 结果 | 能证明什么 |
|---|---|---|
| Redis | 7.1.5 / master | 当前测试端点可用 |
| Redis TIME | PASS | Lease/DueIndex 可以使用服务端时间 |
| noeviction | PASS | 当前实例不会按 LRU 驱逐权威 Key |
| Replica / WAIT | 1 个副本，`WAIT 1` 返回 1 | 当前请求可等待一个副本确认 |
| Probe Key 安全 TTL | 120 秒候选 | 探针异常退出也不会留下永久垃圾；权威 Key 无 TTL 由后续 Repository 合同测试证明 |
| EVAL/EVALSHA | PASS | 固定小 Lua 脚本族可运行 |
| NOSCRIPT | PASS | 不执行 `SCRIPT FLUSH` 也能验证错误分类 |
| Lua Size | 1/8/32/64 KiB 全部 PASS | 至少 64 KiB Script 可 Load + EvalSha；不是推荐脚本大小 |
| ACL | `UNKNOWN_COMMAND` | 当前端点不能用 Redis ACL 证明最小权限 |
| Cluster | Standalone/Proxy | 当前端点无法产生真实 MOVED/ASK |
| Failover | 未执行 | 必须在运维批准窗口使用 `redis-ha-probe` 补证据 |

## 4. 不能宣称已经完成的内容

### 4.1 生产持久化仍未证明

当前标准 Redis 指标显示：

```text
appendonly = no
save = empty
connected_replicas = 1
WAIT 1 = 1
```

这能证明副本确认，不能证明平台重启、主从同时故障或灾备恢复后的资产 RPO。平台可能有 Redis 外部持久化能力，但在取得平台文档、备份恢复或故障注入证据前，禁止宣称公司 Redis 已经满足生产权威存储。

### 4.2 Route HashSeed 漂移

最新 27 号要求 `HashVersion + HashSeed + ShardCount + Routes` 原子安装。当前 `internal/actorrouting.Calculator` 仍是旧的无 HashSeed SHA-256 v1。W31-A 的 KeyBuilder 因此只接受已经计算好的 `AuthorityBucketRef`，没有把旧算法固化成新的持久化合同。

在任何 `auth-v1` 权威数据上线前，必须新增工作包修正：

```text
GAP-ROUTE-HASH-PROFILE
```

它需要冻结带 Seed 的规范字节编码、Hash 算法、Go Golden Sample、RouteSnapshot JSON 与旧 Snapshot 的拒绝/迁移策略。否则路由 Shard 与 Authority Bucket 可能在不同进程产生漂移。

### 4.3 Repository 尚未接入新 Key

本包只实现纯 Key/Slot/Profile 和平台 Probe。`ActorAuthorityStore`、Lua ScriptRegistry、Unknown Resolver、ShardWriteAuthority 与 PersistenceLane 分别属于后续 W8A-L、W9A-L、W8C-L、W31-B/C/D。旧开发 Workflow Key 也不能静默映射到新前缀；开发环境需清理旧 Namespace 或执行显式版本迁移。

当前 Builder 最终仍返回通用 `RedisKey`；W31-B 的 Repository/Script 参数必须进一步收敛成按脚本定义的
Authority/Workflow KeySet，不能让 ProjectionKey 仅凭字符串进入 Authority Lua。`AssertSameSlot` 是最后
一道运行时防线，不代替编译期 Repository API。

## 5. 验证命令

```text
go test ./internal/storage/redis/rediskey ./cmd/redis-production-probe -count=1
go test ./... -count=1
go vet ./...
go test -race ./internal/storage/redis/rediskey ./internal/workidentity ./internal/actorrouting -count=1
```

## 6. 下一步

代码主线可进入 W8A-L 的 `ActorAuthorityStore` 逻辑接口和 Fault Fake，因为它不依赖公司 Redis 已经通过生产持久化验收。并行必须关闭 `GAP-ROUTE-HASH-PROFILE`；平台侧继续补 Failover/RPO/RTO/ACL 或等价网络隔离证据。W31-B/C 在上述路由 Hash 合同冻结后，才能把 Repository 真正接入 `auth-v1` Key。
