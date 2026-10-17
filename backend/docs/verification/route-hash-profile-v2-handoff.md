# Route Hash Profile v2 交接与验证

状态：`IMPLEMENTATION_PASS / CONTROL_PLANE_AND_CAPACITY_PROBE_PENDING`

日期：2026-08-03

对应合同：`27 · 接入、路由、Actor 与 Workflow 冻结合同` 中 RouteSnapshot、VirtualShard、
`auth-v1` Persistence Bucket 与迁移边界。

## 1. 本轮关闭的问题

此前实现使用无 Seed 的 SHA-256 v1，`RouteSnapshot` 不携带 HashSeed，还允许旧 flat JSON 自动转换。
这会让不同进程可能用不同本地配置补 Seed，也无法证明 Redis `auth-v1` Bucket 与 Actor Route 使用同一
映射。ActorRouteClient 还维护 `actor_id -> Assignment` TTL 缓存，内存规模与活跃 Actor 数相关。

本轮冻结：

```text
HashProfile {
  Version = 2
  Seed    = RouteSnapshot.HashSeed
}

ActorAddress
→ seeded XXH64 v2
→ VirtualShard
→ RouteSnapshot.Domain.Routes[VirtualShard]
→ Holder
```

`HashVersion`、`HashSeed`、Realm、PLAYER/FARM ShardCount 都是持久布局的一部分。普通 Watch、Redirect、
滚动发布和 Holder 迁移只能改变 Assignment、Lease 与 WriterEpoch，不能改变上述布局。

## 2. v2 规范编码

Hash 算法：`github.com/cespare/xxhash/v2` 的 seeded XXH64。

Seed 作为 Digest 初始化参数，不重复写入消息。输入字节严格为：

```text
ASCII "CFRT"
+ big_endian(hash_version:u32)
+ big_endian(realm_utf8_byte_length:u16)
+ realm_utf8_bytes
+ domain:u8                 // PLAYER=1, FARM=2
+ big_endian(actor_id:u64)
```

计算：

```text
hash64 = XXH64(seed=HashSeed, canonical_bytes)
virtual_shard = hash64 % ShardCount
```

Realm 必须是合法 UTF-8、非空、无首尾空白且长度不超过 u16；ActorID 必须非零。ShardCount 可以不是
2 的幂，取模语义不变。

## 3. Golden Sample

测试 Profile：

```text
realm = classic-farm
version = 2
seed = 202608030001
actor_id = 10001
shard_count = 1024
```

PLAYER：

```text
canonical_hex = 4346525400000002000c636c61737369632d6661726d010000000000002711
hash64        = 11000288192871273299
virtual_shard = 851
```

FARM：

```text
canonical_hex = 4346525400000002000c636c61737369632d6661726d020000000000002711
hash64        = 1295869252430091328
virtual_shard = 64
```

任何字节、Raw Hash 或 Shard 变化都属于持久布局迁移，不能作为兼容代码上线。

## 4. Snapshot 安装与迁移边界

- `RouteSnapshot` 新增权威 `hash_seed`，校验 Version/Seed/Revision/ShardCount/Routes/HolderIndex 后整表
  原子替换；调用方不得从本地配置补 Seed。
- Resolver 与 Static Provider 存储并返回嵌套 Slice 的深拷贝，Redirect 无法提前修改当前生效表。
- `StaticResolver.Replace` 是 Snapshot Watch、Redirect 和手动 Reload 共用的唯一安装口；安装锁把
  Revision 校验、不可变表构造和 `atomic.Store` 串成一个线性化操作，旧 Revision 不能在并发下覆盖
  已安装的新 Revision。
- 同一个 `GlobalRevision` 只有内容完全相同才算 Duplicate；相同 Revision、不同 Payload 直接拒绝。
- Revision Gap 的 Full Reload 不得低于收到的 Push Revision，也不得回退当前 Revision。
- 已加载 Resolver 的普通 `Replace` 若改变 Realm、HashVersion、HashSeed 或任一 ShardCount，返回
  `ErrHashMigrationRequired`，旧表继续服务。
- 旧 `routing_version + assignments` flat JSON、无 Seed Snapshot、SHA-256 v1 都返回显式迁移错误。
  本轮没有提供在线双读，因为 `auth-v1` 尚未以旧 Profile 写入生产权威数据。
- Holder/Shard 状态只接受 `ACTIVE/DRAINING/ASSIGNING`；只有二者同时为 `ACTIVE` 才能 Resolve，
  迁移态返回 `ErrShardDraining`，不会进入 Actor Mailbox。

若未来确需改变 Profile，必须发布新 AuthorityLayoutVersion/Keyspace，离线或双写迁移 Actor Bucket，
完成全量校验后再切换 RouteSnapshot；不能原地重哈希。

## 5. 热路径与 3000 万 DAU 关系

Calculator 构造时预生成 PLAYER/FARM 规范前缀。每个命令只追加 8 字节 ActorID、执行一次 XXH64，随后
对连续 Shard Assignment 数组做一次索引。没有每请求 Redis/RouteManager 查询，也没有高基数
actor_id 路由缓存；内存规模由 ShardCount 与 Holder 数决定，而不是 DAU。

这能证明路由算法本身不会随 3000 万 DAU 线性占内存，但不能单独证明系统容量。仍需使用真实 Actor
ID 分布和目标 ShardCount 做离线分布、热点与多 Pod Assignment 探针；Actor Mailbox、存储、跨 Actor
Workflow 和 200 人广播仍是独立压测对象。

当前开发云主机（Linux/amd64，AMD EPYC 7K62，32 路并行）三轮微基准：

```text
BenchmarkCalculatorShardFor       46.35～47.44 ns/op   0 B/op   0 allocs/op
BenchmarkStaticResolverResolveActor 109.3～112.7 ns/op  0 B/op   0 allocs/op
```

该结果只证明本地热路径没有分配且量级合理，不替代跨 Pod gRPC、Route Watch、Actor Mailbox 和真实业务
容量测试。

## 6. AuthorityStore 对齐

`authoritystore.ShardResolver` 与 Redis `rediskey.AuthorityShardResolver` 都同时暴露 `ShardFor`、
`ShardCount` 和 `Profile`。MemoryStore 与 Authority KeyBuilder 初始化时校验 Profile，
ShardWriteAuthority 同时持久 `HashVersion + HashSeed`，Claim/Renew/Transfer/Unassign 和 Actor Commit
都会拒绝与当前 Calculator 不同的已存 Profile；测试或调用方不能再写死 v1，也不能在重启时用同
Version 的新 Seed 解释旧 Bucket。KeyBuilder 不实现第二套 Hash，只通过同一个 Calculator/Resolver
取得 VirtualShard；W31-B/C 再通过 BucketMeta 将该 Profile 与物理冷启动布局绑定。

## 7. 代码与测试

主要文件：

- `internal/actorrouting/routing.go`
- `internal/actorrouting/provider.go`
- `internal/actorrouting/hash_profile_test.go`
- `internal/actorrouting/routing_test.go`
- `internal/actorrouting/provider_test.go`
- `internal/storage/authoritystore/{types,memory,shard_authority}.go`
- `deploy/actor-route.static.example.json`
- `deploy/route-snapshot.player.json`

覆盖：Golden、Domain/Seed 分离、非 2 次幂 Shard、分布烟测、旧 Profile 拒绝、布局替换拒绝、同 Revision
冲突、Gap 回退拒绝、Snapshot 深拷贝、并发 Replace/Resolve、并发多 Writer 不回退、迁移状态拒绝、
Authority 与 Redis KeyBuilder HashProfile 对齐。

提交前执行：

```text
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

## 8. 未完成且不得夸大的证据

- RouteManager/Discovery Watch 的生产控制面尚未实现；当前只证明 Snapshot Provider/Applier 合同。
- 真实 3000 万 Actor ID 语料、目标 PLAYER/FARM ShardCount 与卡方/最大偏斜报告尚未生成。
- Redis Cluster/Tcaplus 正式 Authority Repository 尚未接入，Profile v2 只是解除 W31-B/C 的前置阻塞。
- W31-B/C 仍须让 `bucket-meta.hash_config_id` 绑定 Realm/Profile/ShardCount；当前每条 Shard Authority
  已校验 Version/Seed，但物理 Repository 还必须在首次写入空 Bucket 前阻止冷启动误配。
- 示例 Seed 是 JSON 安全的非秘密开发值；生产环境 Seed 应由配置发布流程生成、审计并永久冻结。
