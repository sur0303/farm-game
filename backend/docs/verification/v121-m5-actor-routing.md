# v1.21 M5 · Actor Virtual Shard 路由与 writer_epoch Fence

日期：2026-07-29

## 1. 问题

原实现只有：

```text
Gate → 固定 player-svr / farm-svr 地址 → 本进程 keyed mailbox
```

它能保证一个进程内同 Key 串行，但不能回答多 Pod 时哪个 Pod 是当前 Holder。
直接使用 `hash(id) % pod_count` 会在扩缩容时重映射大量 Actor；只做网络路由又
无法阻止迁移后的旧 Pod 继续用合法 CAS 版本写 Redis。

## 2. 本阶段选择

实现项目内最小、可替换的数据面合同：

```text
ActorAddress(realm, kind, id)
→ routing_version=1 的冻结 Hash
→ VirtualShard
→ 完整 Static ShardAssignment
→ ActorRouteClient 短 TTL cache
→ holder endpoint
→ holder 本地 keyed mailbox
→ Redis CAS + writer_epoch
```

没有引入 Proxyless Mesh 或独立 ShardCoordinator。当前静态快照足以验证 Actor
地址、Shard、Holder 和 Fence 的边界；控制面动态 watch/lease 只有在两 Pod
实验和容量证据需要时再增加。

## 3. 固定合同

`ActorAddress` 必须包含 `actor_kind`，因此相同数字 ID 的 PlayerActor 与
FarmActor 会进入独立路由域。v1 route key 使用：

```text
big_endian(routing_version)
+ realm length + realm
+ kind byte
+ big_endian(actor_id)
```

然后取 SHA-256 前 64 位模 `virtual_shard_count`。黄金测试冻结：

```text
classic-farm / PLAYER / 10001 / 1024 → shard 379
classic-farm / FARM   / 10001 / 1024 → shard 136
```

静态快照必须覆盖每个 Domain 的每个 Shard，缺一个、重复一个、revision/epoch
为零或 endpoint 为空都启动失败，避免请求静默落到默认 Pod。

## 4. 已接入路径

- farm-gate：按命令冲突键选择 PLAYER 或 FARM Route Domain；
- 登录 Bootstrap：按 `PLAYER:player_id` 选择建档 Holder；
- farm-svr Workflow：Reserve/Commit/Reward/GetPrivateState 按参与玩家路由；
- farm-worker：Farm inspection/completion 和所有 Player participant 均按 Actor
  地址路由；
- player-svr/farm-svr：配置静态快照和 `ACTOR_HOLDER_POD` 后，只承载分配给
  自己的 Actor。

调用方把 realm/kind/id/shard/revision/writer_epoch/holder 作为可信 gRPC
metadata 发送。真正的写权不依赖 metadata，而由 Holder 根据自己的 Assignment
解析 actor_id，并由存储 Fence 最终裁决。

## 5. writer_epoch

Redis/Memory BlobStore 增加强合同：

```text
Fence(id, new_epoch)
InsertEpoch(id, epoch, data)
CASEpoch(id, record_version, epoch, data)
```

新 Holder 激活已有 Actor 时先原子提升 `writer_epoch`。此后：

- 旧 epoch 的 CASEpoch 返回 `ErrWriterFenced`；
- legacy CAS 在记录已经进入 epoch 模式后也返回 `ErrWriterFenced`；
- 新 Holder 仍需匹配普通 record version，writer_epoch 不替代 CAS；
- 新旧 Holder 即使同时存活，旧 Holder 也不能提交权威状态。

`connection_generation`、`session_epoch` 和 `writer_epoch` 仍是三种不同 Fence。

## 6. 配置

示例见 `deploy/actor-route.static.example.json`。

```text
farm-gate / farm-worker:
  ACTOR_ROUTE_SNAPSHOT=/config/actor-route.json

player-svr / farm-svr:
  ACTOR_ROUTE_SNAPSHOT=/config/actor-route.json
  ACTOR_HOLDER_POD=<与快照 holder_pod 完全一致>
```

迁移一个 Shard 的安全顺序：

1. 生成更高 `shard_revision`、更高 `writer_epoch` 的新快照；
2. 让新 Holder 加载快照并在首次激活时执行存储 Fence；
3. 调用方更新快照/重启以刷新静态 Resolver；
4. 旧 Holder 的后续写由存储拒绝；
5. 确认旧 Holder 无 inflight 后摘除。

静态模式目前通过配置发布完成迁移，不支持在线 watch。

## 7. 验证

已通过：

```text
ActorAddress/Hash 黄金测试                         PASS
快照完整覆盖校验                                  PASS
RouteCache revision invalidate                    PASS
Gate PLAYER/FARM 冲突键路由                       PASS
writer_epoch 新 Holder 接管、旧 Holder/legacy CAS 拒绝 PASS
gateway/auth/farm/workflow/cmd 受影响包测试         PASS
```

未宣称完成：

- 动态 ShardCoordinator、Lease renew/watch；
- STALE_ROUTE 带新 revision 的在线 Redirect 重试；
- 跨 Pod Presence/SessionRoute 精确下行；
- 两个真实 Player/Farm Pod 的迁移压测。

前三项只有在静态双 Pod 实验暴露实际运维问题后才进入下一轮；精确下行与容量证据
在 M6/最终部署验证中继续收敛。
