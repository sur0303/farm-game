# RC-SCB-1 — Route Control Shared Contract

版本：`rc-scb1-20260808`
状态：`FROZEN / LUNA_MAX_CLOSURE_PASS / NO_P0_P1`
Integration base：`29ed7be2f7ddc6df2e837830c165ad50ca62e28f`
用途：冻结 RT2A/RT2B/RT2Q-0 并行施工前的共享语义；本文不授予生产写权，也不修改 26～37 的权威顺序。

## 1. Authority order

```text
Redis ShardWriteAuthority
  > RouteAssignmentManager
  > durable complete RouteSnapshot CAS
  > authenticated complete-snapshot transport
  > process-local immutable snapshot
  > HolderRegistry / RoutePlan / routectl views
```

- Manager 是 generation 转换唯一协调者；
- Registry 不能授予写权；
- Plan 不能替代 Authority 或 Snapshot；
- GlobalRevision 不是 Store writer fence；
- 不新增第二套 WriterEpoch、LeaseEpoch 或 AssignmentRevision 状态机。

## 2. ASSIGNING 的冻结解释

`ShardRoute.State=ASSIGNING` 可以持久化并通过 Watch 传播，但它只是 fail-closed 的控制面
Handoff 投影，不是 30/31 所称“新 Holder 可服务的 Assignment”。

Planned Drain 的合法顺序是：

```text
Source Drain 完成
→ Authority 原子切到 Target generation（Redis lease_state=ACTIVE）
→ 持久化/传播完整 ASSIGNING RouteSnapshot
→ Target PrepareShardGeneration
→ 持久化/传播完整 ACTIVE RouteSnapshot
```

必须同时满足：

1. ASSIGNING 与 ACTIVE 均是整张 Snapshot，不是 shard patch；
2. ASSIGNING 中 Holder/Boot/ShardRevision/WriterEpoch/LeaseEpoch 必须与已提交 Authority 一致；
3. Resolver 对 ASSIGNING 返回不可服务；Gate 不路由普通请求；
4. Membership 不为 ASSIGNING 开放普通业务准入；
5. Target prepare 不扫描 ActorRecord、不批量 Load Actor；
6. 只有 prepare 成功后才能发布同 generation ACTIVE；
7. ACTIVE 发布失败只允许恢复同 generation publication，不回退 Source 写权；
8. Redis ShardWriteAuthority `lease_state` 仍只有 UNASSIGNED/ACTIVE，不新增 ASSIGNING。

因此 30/31 的“新 Holder Ready 后发布 Assignment”指 ACTIVE 数据面 Assignment；
ASSIGNING 是同一合同明确允许的 RouteManager Handoff/Holder Runtime 状态。该解释与 integration
中现有 `RouteAssignmentManager.Drain`、`ValidateAuthoritySnapshotUpdate` 和 Resolver fail-closed
行为一致，不改变 Store Authority 语义。

## 3. Durable RouteSnapshot Store

### 3.1 Stored envelope

逻辑字段：

```text
schema_version       = route-snapshot-v1
realm
revision             = snapshot.GlobalRevision
canonical_json       = json.Marshal(validated RouteSnapshot)
checksum_sha256      = lowercase hex SHA-256(canonical_json)
topology_fingerprint = lowercase hex SHA-256(canonical topology bytes)
committed_at_ms      = Redis authority time or Store-confirmed time
```

Canonical topology bytes 使用版本化、长度前缀编码，内容仅为：schema、Realm、HashVersion、
HashSeed、Player.ShardCount、Farm.ShardCount。Holder、Endpoint、Revision、Lease deadline 不进入
TopologyFingerprint。

任何 schema/checksum/fingerprint/JSON/RouteSnapshot validation 不一致都必须 Fail Closed；不得返回
“空 Store”或旧内存 Snapshot 掩盖损坏。

### 3.2 CAS

```text
empty + expected=0 + next.revision=1       → COMMITTED
current=R + expected=R + next.revision=R+1 → COMMITTED
current=next.revision + exact same envelope → ALREADY_COMMITTED
current=next.revision + different bytes     → REVISION_CONFLICT
其他 expected/current/next 组合             → REVISION_CONFLICT 或 STALE_EXPECTATION
uint64 overflow                             → FAIL_CLOSED
```

- 同一 revision 不允许不同 payload；
- 不允许跳 revision；
- 不能先通知再持久化；
- `LoadCurrent` 只有物理 key 确认不存在时才返回 `found=false`；
- static→dynamic 首次导入必须显式 bootstrap；不根据本地文件猜测已提交状态。

### 3.3 DurableSnapshotPublisher

```text
validate/canonicalize
→ Store CAS
→ process-local publisher/watch notification
```

- Store committed、notify 失败：返回可分类 publish failure，保留 committed Store；
- 同一 Snapshot replay：Store 返回 ALREADY_COMMITTED，再重试 notify；
- 不重复推进 Authority 或 revision；
- `Publish` 使用普通更新校验；
- `PublishAuthoritySnapshot` 只供现有 Manager authority activation 路径；
- Publisher 不接受客户端 supplied caller role/capability。

### 3.4 Transition recovery/tombstone

RT2A 只实现现有 `authority.TransitionRecoveryStore` 与
`authority.TransitionTombstoneStore` 的 durable adapter：

- 不复制 manager recovery state machine；
- same transition_id+same fingerprint 幂等；different fingerprint Fail Closed；
- bounded retention/capacity，满时拒绝，禁止静默遗忘；
- 接口当前无 Context，adapter 每次 Redis 操作必须使用配置的有界内部 timeout；
- 只允许固定、同 slot 的精确 keys/Lua；不得 KEYS、全局 SCAN、SCRIPT FLUSH。

## 4. RemoteAssignmentPump

### 4.1 Trust boundary

Transport-neutral source port：

```go
type RemoteSnapshotSource interface {
    FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error)
    WatchFrom(context.Context, uint64) (<-chan actorrouting.RouteSnapshot, error)
}
```

该 Port 的前置合同是：具体 transport adapter 已验证内部 server identity、deadline/cancellation、
schema/checksum/topology envelope，并且只返回完整 Snapshot。RT2B 不实现 mTLS；RT2C 的 internal gRPC
adapter 必须完成该验证后才能实现此 Port。任何 public/H5 caller 都不能构造生产 Source。

Pump 仍必须独立执行 RouteSnapshot shape/layout/generation/revision 校验，不能只相信 transport。

### 4.2 Separate local capability

新增远程构造入口时必须返回两个不可互相派生的 handle：

```text
DynamicAssignmentProvider   # read/watch only
RemoteAssignmentInstaller   # Pump-only complete snapshot install
```

- Installer 不能从 Provider 公共 API 重新取得；
- 不把 Manager 的 `DynamicAssignmentPublisher` 交给数据面普通 caller；
- 普通 data-plane resolver/consumer 永远拿不到 installer；
- composition root 是 capability 唯一分发者；
- provider.go 的该接缝由 RT2B 单 Writer 修改，RT2A/RT2Q 禁止修改。

Installer 校验：

- full validated Snapshot；
- same topology；
- higher GlobalRevision；
- per-shard generation 不回滚；
- same revision exact duplicate 幂等，different payload conflict；
- 允许已认证完整 Snapshot 的 revision jump；
- 允许合法同 generation ASSIGNING→ACTIVE；
- 不允许 ACTIVE→ASSIGNING 同 generation；
- 不允许 holder/boot 改变而 generation 未完整前进；
- stale callback 不覆盖新 revision。

### 4.3 Pump state machine

```text
BOOTSTRAPPING → WATCHING → RESYNCING → DEGRADED → CLOSED
```

- 启动 Fetch 失败：启动 Fail Closed；
- R10 直接收到完整 R12：允许直接安装，不要求 R11 patch；
- duplicate 幂等、stale 忽略并计数；
- Watch close/error：保留当前本地 Snapshot，FetchFullSnapshot 校准，再 WatchFrom(applied revision)；
- topology 变化：terminal Fail Closed；
- Resolve 热路径远程调用数=0；
- Close 取消 blocked Fetch/Watch/backoff；
- 慢消费者由 transport/hub 断开，Pump resync，不阻塞 publisher。

## 5. Static 与 remote 模式隔离

```text
composed + static       → 保留当前 registered-holder 和 BootMismatch 启动失败语义
composed + remote-watch → initial authenticated Fetch；允许本地 Holder 暂未出现在 Snapshot，
                          0-shard Ready、CanServeShard 全 false，等待 Controller 分配
legacy + remote-watch   → 非法，启动失败
```

Remote 构造器/Options 必须显式；禁止在现有 static constructor 中用“缺 Holder 时继续”方式放宽检查。
RT2I 是最终 mode/composition 唯一 Writer。

## 6. Controller single-writer boundary

本期不实现 Leader Election、HA 或自动 failover。安全合同是：

1. Route Control Deployment `replicas=1` 且使用不重叠的 Recreate/人工确认启动策略；
2. Controller ID+Boot 非空且来自内部 composition，不相信 RPC payload caller_role；
3. mutating RPC 只接受受认证 routectl/control identity；
4. 一个进程内只有一个 RouteAssignmentManager 与 PlanExecutor；
5. expected Authority fence、transition fingerprint、durable Snapshot CAS 继续防 stale/replay；
6. 如果检测到第二实例、身份漂移或无法证明旧实例终止，新的 mutating readiness 必须关闭；
7. 本期不得宣称多 Controller split-brain 安全或 HA。

是否增加 startup-only exclusive lease 由 RT2C 的 ICR 决定；若增加，它只能阻止第二实例 Ready，不能被
宣传为 Leader Election，也不能替代 Authority expected fence。没有可复跑的 single-instance deployment
证据前，G2/G3 不得通过。

## 7. Exact Wave 1 write locks

### RT2A

Owned：

```text
internal/routecontrol/snapshot_store.go
internal/routecontrol/snapshot_codec.go
internal/routecontrol/durable_publisher.go
internal/routecontrol/*_test.go（仅上述组件）
internal/storage/redis/routecontrol/**
docs/verification/rt2a-route-snapshot-store-handoff.md
```

Forbidden：

```text
internal/actorrouting/provider.go
internal/actorrouting/authority/manager.go
internal/actorrouting/remote/**
internal/authoritycomposition/**
cmd/** deploy/** api/proto/**
```

### RT2B

Owned：

```text
internal/actorrouting/remote/**
internal/actorrouting/provider.go（仅 RemoteAssignmentInstaller 接缝）
internal/actorrouting/provider*_test.go（仅该接缝测试）
docs/verification/rt2b-dynamic-route-consumer-handoff.md
```

Forbidden：

```text
internal/routecontrol/**
internal/storage/redis/**
internal/actorrouting/authority/manager.go
internal/authoritycomposition/**
cmd/** deploy/** api/proto/**
```

### RT2Q-0

Owned：

```text
cmd/rt2q-route-control-harness/**（仅测试二进制）
internal/testkit/routecontrol/**
docs/verification/rt2q-route-control-harness-handoff.md
docs/verification/route-control-evidence-schema-20260808.md
```

Forbidden：所有生产 runtime/store/proto/composition、A/B owned files、现有 smoke/e2e 主线。
RT2Q Final 必须使用最终候选的新独立 worktree，不复用 Q-0 Writer worktree。

## 8. Evidence levels

```text
E1 UNIT/RACE
E2 COMPONENT_FAULT
E3 REAL_REDIS_MULTI_PROCESS
E4 MULTI_POD_BUSINESS_E2E
```

Manifest 必填：base/head/branch、run_id、start/end、命令及 exit、mode、schema、Snapshot/Authority/
Holder timeline、verdict、artifact hashes、secret scan。不得用 E1/E2 宣称 E3/E4。

## 9. Remaining phase boundaries

- RT2A/B/Q-0 可以在本合同 closure 后从相同 integration base 开工；
- RT2C 必须先于依赖其 Proto/identity 的 RT2D；不能把二者当无依赖并行；
- RT2I 独占 composition root；
- RT2M 只能调用 Manager/Control Service；
- Autoscaling 在 G1/G2/G3、CP-ROUTE-CONTROL-MVP 合入新 integration、新 Baseline 之前保持 BLOCKED。
