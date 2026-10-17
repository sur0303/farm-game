# 87 · K8s Holder 宕机自动恢复合同

状态：`v1.0-a7-implemented-candidate`
日期：2026-08-14
分支：`codex/scaleplan-holder-failover-a7-20260814`
前置：26～37 业务合同、30/31 写权合同、85 K8s Discovery 迁移计划、86 自动 Rebalance。

## 1. 目标

当 `player-svr` 或 `farm-svr` 某个有状态 Pod 意外消失、容器重启并换代，或 HolderControl 验证结果不再包含旧进程代次时，系统应当：

1. 自动发现，不依赖人工指定故障 Shard；
2. 自动等待或证明旧 Writer 已失效；
3. 自动把受影响 Shard 接管到仍健康的 Pod；
4. 自动发布更高版本路由；
5. 依靠 `writer_epoch` 拒绝旧 Pod 的迟到写；
6. Actor 从 Redis 权威数据冷加载，客户端通过幂等重试或重新 Join 恢复；
7. PLAYER/FARM 两个路由域独立恢复，未受影响 Shard 继续服务。

本合同不承诺进程内、尚未按持久化等级落库的 Soft 状态零丢失；它遵守既有 RPO 合同。强事实、Receipt、Workflow 与关键资产仍必须先可靠提交。

## 2. 为什么不能“Pod 不见了就直接改路由表”

Kubernetes 只知道 Pod 的基础设施状态，不知道当前 Shard 的写权是否已经安全释放。若看到 Pod NotReady 就直接改 `Shard -> Pod`：

- 网络分区时旧 Pod 可能仍能访问 Redis；
- Informer 事件可能短暂延迟或乱序；
- 新旧进程可能同时认为自己是 Writer；
- 迟到的旧请求可能覆盖新状态。

因此自动恢复必须经过同一条权威路径：

```text
K8s Pod/Ready变化
→ HolderControl验证精确(holder_id, boot_id)
→ VerifiedHolderView变化
→ AutoFailoverController发现旧代次缺失
→ 读取并核对ShardAuthority
→ 持久CrashTakeover RoutePlan
→ Authority CAS推进writer_epoch
→ 发布新RouteSnapshot
```

Informer 负责发现与唤醒；HolderControl 负责证明游戏进程身份；RouteControl 才能授予写权。Holder 不需要再向第二套注册中心主动注册。

## 3. 两条故障接管路径

### 3.1 Pod/进程彻底消失，但没有新 Boot 证明

```text
t0  RouteSnapshot: Shard 27 → farm-svr-1 / boot-A / writer_epoch=8
t1  farm-svr-1消失，Informer视图不再包含boot-A
t2  旧Authority Lease仍有效
    → 不改写权
    → 指标记为waiting_lease
t3  Redis权威时间确认Lease到期
    → 读取ShardAuthority并核对完整旧Fence
    → 提交确定性CrashTakeover Plan
t4  TakeoverExpiredShardAuthority CAS成功
    → holder=farm-svr-0/boot-B
    → assignment_revision=old+1
    → writer_epoch=old+1
    → lease_epoch=old+1
t5  发布新RouteSnapshot
    → 新Holder按需冷加载Actor
```

这里必须等待 Authority Lease，而不能仅相信控制器本机时间。普通 `TransferShardAuthority` 不允许在旧 Lease 已到期后偷用原转移路径；自动宕机恢复使用独立的 `TakeoverExpiredShardAuthority` Lua CAS。

### 3.2 同一个 StatefulSet Pod 身份已经出现新 Boot

容器重启时 Pod UID 可能不变，但游戏进程 `boot_id` 必须变化：

```text
farm-svr-1 / boot-A  → 消失
farm-svr-1 / boot-B  → K8s Ready + HolderControl验证通过
```

同一 Pod 内旧容器与新容器不可能同时存活；新 Boot 是比“暂时 NotReady”更强的旧进程失效证据。此时控制器可以走 `HolderUnavailable` 接管，不必机械等待旧 Lease 到期。接管仍需完整旧 Fence、持久 RoutePlan 和更高 `writer_epoch`，不能直接覆写路由数组。

## 4. 自动控制器算法

每次 Reconcile：

1. 读取当前内存 `RouteSnapshot`；
2. 按 PLAYER、FARM 路由域分别读取 `VerifiedHolderView`；
3. 仅检查 `ACTIVE` Shard 的精确 `(holder_id, boot_id)` 是否仍存在；
4. Snapshot Lease 尚有效且没有新 Boot 证明时，直接等待，不对 Redis 做全量扇出；
5. 到期候选才读取对应 `ShardAuthority`，并核对 domain、shard、holder、boot、assignment revision、writer/lease epoch；
6. 从健康 Holder 中选择当前 Shard 数最少的目标，稳定排序打破并列；
7. 基于源 Fence 和目标代次生成确定性 `plan_id` / `transition_id`；
8. 提交并执行现有 `CrashTakeover RoutePlan`；
9. 一次 Pass 最多完成 32 个 Plan、最多读取 128 次 Authority；剩余故障在下一次 Pass 继续；
10. ScalePlan 自动扩缩容与 AutoFailover 共用一个控制锁，避免并行提交相互冲突的路由修改。

Informer 事件只负责快速更新 Pod 代次和唤醒控制器。RoutePlan 发布
`ASSIGNING` 新版本后，目标 Pod 的 `ObservedRouteRevision` 会变化，但 Pod 对象本身未必变化；
因此 RoutePlan 对目标 Holder 做精确 `Get` 时，会只对目标路由域主动执行一次
Kubernetes + HolderControl 重验。这样目标收敛不依赖 30 秒全局 resync，后者只作为周期安全网。

若没有健康目标，控制器保持原事实、暴露 `no_target` 指标并报警；不能伪造恢复成功。若整个 FARM 域所有 Pod 同时消失，恢复要等至少一个新 Holder 通过 K8s Ready 和 HolderControl 验证。

## 5. Redis 原子边界

`TakeoverExpiredShardAuthority` 只接受：

```text
state == ACTIVE
Redis TIME >= lease_until_ms
完整ExpectedFence完全相等
transition_id未被不同请求占用
```

一次 Lua 原子更新：

```text
holder_instance_id / holder_boot_id
assignment_revision + 1
writer_epoch + 1
lease_epoch + 1
new lease_until_ms
last_transition_id / digest / timestamp
```

相同 `transition_id + intent` 重试返回 `ALREADY_APPLIED`；相同 ID 不同 Intent 返回冲突。旧 Pod 后续的 Renew、Actor Commit 或 CAS 都携带旧 Fence，Redis 返回 mismatch。

## 6. 客户端表现

### Player Shard 故障

- 已进入但尚未开始 Actor Turn 的请求可能收到短暂 `UNAVAILABLE` / Redirect；
- 客户端用原 `operation_id` 有界重试；
- 已提交的操作通过 Receipt/OperationResult 返回原结果；
- 新 Holder 从 Redis 冷加载 PlayerActor。

### Farm Shard 故障

- 旧 Presence、JoiningBuffer、Hub 游标和连接队列不迁移；
- 旧 Subscription 受到 subscription/route revision Fence；
- 客户端收到 `REJOIN_REQUIRED`，或在下行中断后重新 Join；
- 新 Holder 加载 FarmActor，重新执行 Snapshot → Catch-up → Ready；
- 未受影响的 Farm Shard 和 PLAYER 域继续服务。

自动恢复的最坏时间近似：

```text
Informer发现延迟
+ 旧Authority Lease剩余时间（若没有新Boot证明）
+ 1秒Reconcile周期
+ RoutePlan执行与Snapshot发布
+ 客户端重试/重Join
```

因此 Lease TTL 是安全性和 RTO 的共同参数，后续必须通过故障演练校准，不能为了追求极短 RTO 取消 Fence。

新 Holder 收到 `ASSIGNING` 路由后，控制器会主动重验其最新
`ObservedRouteRevision`；这一步仍然是只读身份/就绪校验，不会授予写权。只有 Authority CAS
成功并发布 ACTIVE Snapshot 后，新 Holder 才成为 Writer。

## 7. 配置与监控

```text
SCALEPLAN_MODE=auto
HOLDER_DISCOVERY_MODE=k8s
ROUTE_FAILOVER_RECONCILE_INTERVAL=1s
ROUTE_FAILOVER_MAX_PLANS_PER_PASS=32
ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS=128
```

关键指标：

```text
route_holder_failover_pass_total{result=...}
route_holder_failover_plan_total
route_holder_failover_authority_read_total
route_holder_failover_missing_shards
route_holder_failover_waiting_lease_shards
route_holder_failover_no_target_shards
```

建议告警：

- `missing_shards > 0` 超过两倍 Lease TTL；
- `no_target_shards > 0` 立即告警；
- failover pass 持续 error；
- takeover 后旧 writer mismatch 突增；
- FARM 恢复后的重 Join 失败率或耗时升高。

## 8. 首版边界

已实现的是 Shard 粒度冷恢复，不是 Actor 内存热迁移，也不是热点 Actor 单独搬迁。自动恢复不会自动创建新 Pod；Pod 副本恢复由 StatefulSet/K8s 负责，RouteControl 负责在现存健康 Holder 之间恢复 Shard。若希望容量也自动补齐，应由 HPA/ScalePlan 在另一条控制链补副本，二者通过共享控制锁协调。
