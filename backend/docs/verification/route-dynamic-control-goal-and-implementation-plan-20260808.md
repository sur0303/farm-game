# 经典农场动态 Shard 控制面与后续自动扩缩容实施计划

版本：v1.0-controller-frozen-candidate
保存时间：2026-08-08
控制器：I0
当前候选 integration：`codex/integration-20260803@29ed7be2f7ddc6df2e837830c165ad50ca62e28f`
最终 `base_commit`：由 `CP-ROUTE-DYNAMIC-BASE` Gate 通过后冻结；所有 Agent Prompt 必须写精确 SHA。

> 本文件是上下文压缩后的恢复入口。它保存目标、边界、阶段、Writer/Worktree 规则、
> 测试矩阵与停止线。实现时的合同优先级仍是：`docs/design/26`～`37`、Route
> Foundation 最终 Handoff、本计划、当前源码与测试。遇到合同冲突必须停止并报告，
> 不得用本计划覆盖既有权威合同。

---

## 0. 当前事实

最新 integration 已包含 Static Route Foundation：

- zero-shard process readiness；
- `ProcessReady` 与 `CanServeShard` 分离；
- 完整 `RouteSnapshot` 的 ADD/REMOVE/REPLACE Membership 收敛；
- Redis Authority proof 后安装本地 Guard；
- per-shard generation 独立 Renew、Deadline、Fence 与安全恢复；
- 旧 Holder/Boot/WriterEpoch/WriterClaim/RecordVersion 无法继续权威写；
- composed+static 启动、Static Boot Mismatch Fail Closed；
- Q2R-2A/B1/B2 与 RF-OBS H1/H2/H3 已通过 integration post-merge Gate。

尚未实现：

- durable RouteSnapshot Store / GlobalRevision CAS；
- 跨进程 Snapshot Get/Watch；
- RemoteAssignmentPump；
- Route Control Role；
- HolderRegistry / HolderAgent / HolderControlService；
- RoutePlan / routectl；
- planned Drain 跨进程迁移与人工 crash takeover；
- 自动故障发现、自动扩缩容与 Controller HA。

---

## 1. Worktree、分支与 Writer 硬规则

目录层级固定为：

```text
farm-game
  integration 主工作树；只作为集成真值，不直接开发

farm-game-route-dynamic-base-20260808
  I0 Baseline worktree；核验、Gate、冻结 base_commit；不开发正式功能

farm-game-rt2a-route-snapshot-store-20260808
farm-game-rt2b-dynamic-route-consumer-20260808
farm-game-rt2q-route-control-harness-20260808
  Wave 1 各 Agent 独立 worktree

farm-game-rt2c-route-control-service-20260808
farm-game-rt2d-holder-control-20260808
farm-game-rt2i-route-composition-20260808
farm-game-rt2m-routectl-20260808
  后续 Wave 各 Agent 独立 worktree

farm-game-route-control-premerge-20260808
  从届时最新 integration 创建的最终 Premerge
```

硬规则：

1. 一项任务一个 worktree、一个 branch、一条主 Writer Thread；
2. 所有首 Wave Agent 从同一个精确 `DYNAMIC_BASE_COMMIT` 分叉；
3. 不复用 integration 主工作树、Baseline worktree 或其他 Agent worktree；
4. Agent 不 push、不合并 integration；push/Candidate/Premerge/Merge Queue 由 I0 执行；
5. Reviewer 只读；Finding 由原 Writer 修复；
6. 不扩大 owned write set；跨包接口需要 I0 冻结；
7. Agent 开工后 integration 前进，不频繁 rebase；在最终 Premerge 集中合并；
8. 只有直接依赖接口发生破坏、任务无法继续时，I0 才决定重建工作树；
9. 所有子 Agent 仅用官方 Codex provider：`openai-codex/gpt-5.6-luna`，reasoning=`max`；
10. 禁止 Cursor、CodeBuddy Agent 或其他模型作为实现/Review Writer；
11. Secret、Cookie、Ticket、完整 WriterClaim、真实 Redis Endpoint 不得进 Git；
12. 公司共享 Redis 只允许测试 namespace 的精确 key `DEL`/`EXISTS`；禁止 `KEYS`、
    全局 `SCAN`、`SCRIPT FLUSH`。

---

## 2. 本期 Route Control MVP 必须交付

1. 最新完整 `RouteSnapshot` 的持久化与 GlobalRevision CAS；
2. Route Control Role 单写者；
3. `GetSnapshot` / `WatchFrom` 跨进程传播完整 Snapshot；
4. farm-gate、player-svr、farm-svr、farm-worker 动态消费同一 Snapshot；
5. Holder 注册、心跳、Ready/Draining/Drained 状态；
6. `routectl status/holders/move-shard/drain-holder/takeover-after-expiry/verify`；
7. planned A→B Drain；
8. Pod A 宕机后的人工、受证明约束的 LeaseExpired 接管；
9. 两个 farm-svr Pod 的真实迁移 E2E；
10. 多人偷菜迁移期间无重复扣减、无重复奖励、旧 Writer 成功数为 0；
11. Controller restart publication recovery；
12. Controller 不在业务请求热路径，停止时已有 Assignment 继续服务。

准确宣称：

> 系统具备跨进程动态完整 Snapshot、Holder 动态 Membership、手工计划迁移和人工
> 故障接管。迁移正确性由 ShardWriteAuthority、WriterEpoch、WriterClaimID、
> RecordVersion 与 Receipt 保证；客户端通过 Fresh Join 恢复。自动故障发现和自动
> 扩缩容属于通过 G1/G2/G3 后的下一层触发器。

---

## 3. Route Control MVP 明确非目标

- 自动检测 Pod 死亡并自动接管；
- 大规模自动恢复队列与迁移风暴处理；
- 多 Route Controller Leader Election / HA；
- Kubernetes 自动 Scale-out / Scale-in；
- 热点自动识别、预测式调度、自动独占 Pod；
- ActorRecord、Actor 内存、Mailbox、Presence、Subscription、Socket 搬迁；
- 无限 Revision 历史；
- 公网控制面 API；
- 每个业务请求查询 Controller 或 Redis 路由表；
- 重新实现第二套 Authority 或 WriterEpoch 状态机。

这些能力不得偷渡到 RT2A～RT2Q。自动扩缩容在第 18 节单独立项。

---

## 4. 四层真值

### 4.1 ShardWriteAuthority

最终写权真值。只有 Redis Authority Store 能裁决 Holder/Boot/WriterEpoch/LeaseEpoch。

### 4.2 RouteSnapshot

写权事实的路由投影。回答当前请求到哪个 Holder、当前 Revision。Authority 切换后，
由唯一 RouteAssignmentManager 发布完整 Snapshot。

### 4.3 HolderRegistry

候选 Pod 与运行状态。回答 Holder/Boot 是否在线、Ready、Draining、Serving Shard 数、
容量与 Endpoint。Registry 绝不能授予写权。

### 4.4 RoutePlan

运维进度、审计与 restart recovery。它不是 Authority，也不是另一份路由真值。

---

## 5. 不得反转的核心不变量

- I1：RouteAssignmentManager 是 Shard generation 转换的唯一协调者；
- I2：Authority 切换先于对应 ACTIVE Snapshot；
- I3：每次发布完整 Snapshot，禁止外部可见的局部 Shard patch；
- I4：请求热路径只读本地不可变 Snapshot；
- I5：Controller/Redis 路由不可用时不逐请求远程查路由；
- I6：迁移只改变 VirtualShard→Holder，不改变 Actor→VirtualShard；
- I7：不搬 ActorRecord、Actor 内存、Mailbox、Presence、Socket；
- I8：新 Holder 不扫描整个 Shard、不批量加载 Actor；
- I9：Planned Move 必须 Drain；直接 Transfer 只允许 CrashTakeoverProof；
- I10：相同 transition_id+相同输入幂等；不同输入冲突 Fail Closed；
- I11：Registry 失效不等于可抢权；接管必须有旧 LeaseExpired 或可信不可用证明；
- I12：旧 Holder 即使存活也必须被 Store Fence 拒绝；
- I13：H5 不持有 Actor Assignment；路由变化通过 Gate/Holder 与 Fresh Join 收敛；
- I14：TopologyFingerprint 改变 Fail Closed，不自动更换 HashSeed/ShardCount/Layout；
- I15：Snapshot 全量校准，Watch 只是变更通知/完整快照传输，不允许半张表；
- I16：legacy+remote-watch 非法；static 语义不得被 remote 模式削弱。

---

## 6. 目标架构

```text
routectl
   │ internal gRPC
   ▼
Route Control Role（单副本）
   ├─ HolderRegistry
   ├─ RoutePlanStore / PlanExecutor
   ├─ RouteAssignmentManager（复用现有）
   ├─ RedisRouteSnapshotStore
   ├─ TransitionRecovery/TombstoneStore
   ├─ SnapshotWatchHub
   └─ HolderControlClient
        │ GetSnapshot / WatchFrom
        ├─────────────► farm-gate RemoteAssignmentPump → local DynamicProvider
        ├─────────────► player/farm HolderAgent + ControlService + Membership Runtime
        └─────────────► farm-worker RemoteAssignmentPump → participant routing
```

部署：独立 `farm-route-control`，replicas=1。Controller 宕机时已有进程保留本地完整
Snapshot，Holder 继续直接 Renew，业务继续；只暂停新计划、迁移和接管。

---

## 7. 建议代码边界

```text
internal/actorrouting/remote/
  source.go consumer.go consumer_test.go

internal/routecontrol/
  control.go snapshot_store.go durable_publisher.go
  holder_registry.go plan.go plan_executor.go drain_controller.go verify.go testkit/

internal/storage/redis/routecontrol/
  keybuilder.go snapshot_store.go transition_store.go holder_store.go plan_store.go scripts/

internal/holdercontrol/
  agent.go service.go drain_runtime.go status.go

cmd/route-controller/
cmd/routectl/
```

不得破坏现有：

- `internal/actorrouting/authority.RouteAssignmentManager`；
- `internal/authoritycomposition` Membership/Lease Scheduler；
- `internal/storage/authoritystore` 与 Redis Authority fencing；
- 公网 Proto；
- 业务写路径的 WriterClaim/RecordVersion/Receipt 语义。

---

## 8. 核心接口（RC-SCB-1 冻结）

以下形状取代早期候选；若与其他章节有差异，以
`route-control-shared-contract-rc-scb1-20260808.md` 为准。

```go
type RouteSnapshotStore interface {
    // found=false 只能表示物理 snapshot key 不存在；损坏必须返回错误。
    LoadCurrent(ctx context.Context) (stored StoredRouteSnapshot, found bool, err error)
    CompareAndSwap(ctx context.Context, expectedRevision uint64,
        next actorrouting.RouteSnapshot) (SnapshotCASResult, error)
}

type DurableSnapshotPublisher struct {
    store RouteSnapshotStore
    hub   SnapshotNotifier
}

type RemoteSnapshotSource interface {
    // 具体 transport 在返回前已完成内部身份、schema/checksum/envelope 校验。
    FetchFullSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error)
    WatchFrom(ctx context.Context, revision uint64) (<-chan actorrouting.RouteSnapshot, error)
}

type RemoteAssignmentInstaller interface {
    InstallFullSnapshot(ctx context.Context, snapshot actorrouting.RouteSnapshot) error
}

type RemoteAssignmentPump struct {
    source    RemoteSnapshotSource
    installer RemoteAssignmentInstaller
}
```

`RemoteAssignmentInstaller` 是 Pump-only、不可从 read-only
`DynamicAssignmentProvider` 派生的独立 capability；严禁把 Manager 的
`DynamicAssignmentPublisher` 交给 Remote Pump。Current revision 只从 Pump 已成功安装的本地状态读取，
不是 `RemoteSnapshotSource` 的第三个 RPC。

Store 物理合同：

- 完整 blob + schema/checksum/topology fingerprint；
- GlobalRevision 严格单调 CAS；
- expected revision 不匹配返回可分类 CAS conflict；
- 不写局部 patch；
- transition recovery/tombstone 与 snapshot 分层；
- Controller restart 可加载 committed current；
- publisher notify 失败不回滚已提交 Store，而是 recovery/resync。

Consumer 状态机：

```text
BOOTSTRAPPING → WATCHING → RESYNCING → DEGRADED → CLOSED
```

行为：

- 启动先 FetchFullSnapshot、validate、publish local provider，再 WatchFrom；
- 当前 R10 直接收到完整 R12 可以应用；
- duplicate 幂等、stale callback 不能覆盖新 revision；
- 流断开保留本地 Snapshot，FetchFullSnapshot 校准后重连；
- topology fingerprint 改变 Fail Closed；
- Resolve 热路径远程调用数为 0；
- Close 必须取消 blocked Fetch/Watch。

---

## 9. Holder 模型

状态：

```text
REGISTERING → WARMING → READY_ZERO_SHARD → SERVING
                                 └────────→ DRAINING → DRAINED
```

Registration 至少包含：domain、holder_id、boot_id、internal endpoint、state、
process_ready、draining、serving_shard_count、snapshot_revision、started/heartbeat/
lease_until、capacity_weight、resource_summary。

HolderAgent：

1. 启动时生成 HolderBootID；
2. 启动内部 HolderControlService；
3. Register WARMING；
4. 启动 RemoteAssignmentPump；
5. 构造 Membership Runtime；
6. zero shard 时 READY_ZERO_SHARD，有服务 shard 时 SERVING；
7. 周期 Heartbeat；
8. 正常停机 DRAINED/退出。

目标 Holder 条件必须全部满足：domain、heartbeat 新鲜、process_ready、允许状态、
非 draining、boot 匹配、容量不低于阈值。Registry 通过后仍需 Manager+Authority。

模式差异：

- static：Snapshot 中缺本地 Holder仍是配置错误并启动失败；
- remote-watch：允许 WAITING_ASSIGNMENT/0-shard Ready，所有 CanServeShard=false；
- 差异必须由显式 mode 参数实现，不得放宽 static boot mismatch。

---

## 10. HolderControlService

内部 RPC：

- `StopNewActivationAndTurns`；
- `WaitCurrentTurns`；
- `ConfirmFactsCommitted`；
- `PrepareShardGeneration`；
- `GetShardStatus`。

Source：停止新 Activation/User Turn；已 admitted turn 可完成；等待 active turns=0 有明确
Timeout；确认 durability barrier 后才能切 Authority。其他 Shard 不受影响。

Target：验证 controller identity 与 Authority generation；只准备 Membership/Lease/Guard/
依赖；不扫描 Actor、不批量 load；直到 ACTIVE Snapshot 才开放准入。现有
`ColdLoadNewHolder` 物理实现必须收敛为“准备新 generation”，不得真的 cold-load 整个 shard。

---

## 11. RoutePlan

类型：MoveShard、DrainHolder、CrashTakeover。状态至少覆盖：Proposed、Validating、
DrainingSource、AuthoritySwitched、AssigningPublished、TargetPrepared、ActivePublished、
Verifying、Succeeded、Failed、NeedsHuman。

Plan 保存 plan_id、transition_id、domain/shard、source/target holder+boot、expected fence、
base/result revision、timestamps、last_error。它服务审计和 restart recovery，不得替代
Authority+Snapshot。

---

## 12. Planned A→B Drain

1. routectl 只调用 Route Control Service，不写 Redis Authority/Snapshot；
2. Controller 校验 current Snapshot、Source、Target Registry、current Authority；
3. 冻结 ExpectedFence，生成稳定 plan_id/transition_id，持久化 Plan；
4. 调用现有 `RouteAssignmentManager.Drain`：
   - StopNewActivationAndTurns(Source)
   - WaitCurrentTurns(Source)
   - ConfirmFactsCommitted(Source)
   - Authority Transfer 换代
   - 发布完整 ASSIGNING Snapshot
   - PrepareShardGeneration(Target)
   - 发布完整 ACTIVE Snapshot
   - Verify
5. ASSIGNING 期间 Gate 不把新请求送 Source/Target，返回恢复 RouteFeedback/Rejoin；
6. Target prepared 但不普通服务；
7. ACTIVE 后各进程 Watch 收敛：Gate 路由 B、B Membership 新 generation、A Remove old；
8. Subscription/Presence 不迁移；H5 收到 REJOIN_REQUIRED 后 Fresh Join。

允许短暂不可用，绝不允许双写。

---

## 13. 人工 Crash Takeover

`takeover-after-expiry` 必须验证：

- 旧 heartbeat 过期或 Holder 明确不可达；
- 旧 boot 与 ExpectedFence 一致；
- Authority Lease 已过期，或存在冻结合同认可的可信不可达证明；
- Target Ready；
- transition_id 稳定。

然后调用 Manager `Transfer(Mode=CrashTakeover, TakeoverProof=...)`。无 proof、Lease 未过期且
Holder 健康必须拒绝。旧 Pod 恢复后，旧 WriterEpoch/LeaseEpoch/Boot 写成功数必须为 0。

---

## 14. Controller 与 Watch 故障语义

- Controller 正常运行中宕机：已有 Snapshot/Lease 继续，停止新 Plan；
- Authority 已切换、Snapshot 未发布时宕机：restart 读取 TransitionRecovery+Authority，
  只补发同一 generation 完整 Snapshot，禁止再次推进 WriterEpoch；
- Watch 断线：保留 local Snapshot，重连时 GetSnapshot 全量校准，再 WatchFrom；
- Snapshot 不存在且未显式 bootstrap、checksum/schema/topology 错误、Authority Reader 不可用、
  TransitionRecovery 无法加载、transition fingerprint 冲突、controller identity 为空：启动失败。

---

## 15. Composition 与开关

两个独立维度：

```text
AUTHORITY_MEMBERSHIP_MODE = legacy | composed
ACTOR_ASSIGNMENT_MODE     = static | remote-watch
```

允许：legacy+static、composed+static、composed+remote-watch。禁止 legacy+remote-watch。

remote-watch 启动初次 Fetch 失败：进程启动 Fail Closed。运行中 Watch 断开：保留本地
Snapshot并重连/全量校准，不逐请求访问 Controller。单进程所有模块共享同一 Snapshot 与连接池。

Composition 文件唯一 Writer：RT2I。其他 Agent 不得修改：

```text
cmd/farm-gate/**
cmd/player-svr/**
cmd/farm-svr/**
cmd/farm-worker/**
deploy/**
```

---

## 16. routectl MVP

命令：status、snapshot、holders、shard、move-shard、drain-holder、
takeover-after-expiry、verify。

`verify` 对账：Snapshot tuple、Authority tuple、Source/Target local status、Gate resolve、
transition/plan。全部一致才 PASS。routectl 禁止直接写 Authority Redis 或 Snapshot blob。

---

## 17. 分阶段 Checkpoint 与 Agent 所有权

### CP0：CP-ROUTE-DYNAMIC-BASE

Baseline 只做：精确 SHA、Foundation present-once、静态/组合 runtime Gate、代码合同审计、
Agent prompt base 固定。输出 READY_FOR_RT2 或 BLOCKED。

### Wave 1

#### RT2A — Route Snapshot Store

分支：`codex/rt2a-route-snapshot-store-20260808`。唯一负责：

- RouteSnapshotStore interface；
- Redis CAS/checksum/schema/key builder；
- DurableSnapshotPublisher；
- TransitionRecovery/Tombstone Store。

禁止：cmd、RemoteAssignmentPump、Holder Runtime、RouteAssignmentManager 核心语义。
Checkpoint：CP-ROUTE-SNAPSHOT-STORE。

#### RT2B — Dynamic Route Consumer

分支：`codex/rt2b-dynamic-route-consumer-20260808`。唯一负责：

- transport-neutral FetchFullSnapshot / WatchFrom；
- RemoteAssignmentPump；
- revision/gap/duplicate/reconnect/resync/topology fail closed；
- DynamicAssignmentProvider 接缝。

禁止：Redis 物理 key、cmd、RouteAssignmentManager、authoritycomposition 核心。
Checkpoint：CP-ROUTE-REMOTE-CONSUMER。

#### RT2Q-0 — Evidence Harness Foundation

分支：`codex/rt2q-route-control-harness-20260808`。只写测试、工具、文档：

- 多进程启动工具；
- Revision/Authority/Holder timeline；
- 故障注入；
- Evidence schema/manifest；
- 不修改生产控制面代码。

### Wave 2（从 Wave 1 接口冻结后的新 checkpoint 建新独立 worktree）

#### RT2C — Route Control Transport

唯一负责内部 Control proto、GetSnapshot/GetCurrentRevision/WatchFrom、WatchHub、
`cmd/route-controller` transport shell。不得拥有 Holder runtime/composition。

#### RT2D — Holder Control

负责 HolderRegistry、HolderAgent、HolderControlService、RemoteDrainController。

### Wave 3

#### RT2I — Composition Root

唯一写 cmd/farm-gate、player-svr、farm-svr、farm-worker、deploy；负责 feature flags、
生命周期、remote zero-shard waiting 与 static 语义回归。

#### RT2M — routectl/manual migration

负责 RoutePlanStore、PlanExecutor、routectl 命令，复用 Manager Drain/Transfer；禁止第二套
WriterEpoch 状态机、禁止直接写 Authority Redis/Snapshot blob。

### RT2Q Final

只在最终候选运行 Watch、A→B Drain、Controller restart、Crash takeover、多人偷菜、
Fresh Join、权威对账；只提交 test/tool/evidence/handoff。

---

## 18. 自动扩缩容后续阶段（不得与 RT2 并行偷跑）

自动扩缩容目标保留，但只有 G1/G2/G3 全通过、CP-ROUTE-CONTROL-MVP 合入新的
integration 后，才从新的精确 integration SHA 建立另一套 Baseline/Agent/Premerge。

### AS0 — Capacity Metrics

- 低基数 Holder 容量、CPU/memory、serving shards、activation/turn pressure；
- 热点 TopK，不以 player/farm/shard 做普通 Prometheus label；
- capacity snapshot freshness 与 confidence；
- 不触发迁移。

### AS1 — Autoscaler Dry Run

- policy evaluator 只输出 proposal，不创建 RoutePlan、不扩 Pod；
- stability window、cooldown、hysteresis、max surge/move budget；
- replayable decision log；
- 与 routectl verify 对账。

### AS2 — Controlled Scale-out

- 通过平台 adapter 请求扩容；
- Holder READY_ZERO_SHARD 后才能进入候选；
- 只调用已验证 PlanExecutor planned Drain；
- bounded concurrency/canary/abort；
- 不直接改 Authority/Snapshot。

### AS3 — Drain-aware Scale-in

- 先标 Holder Draining，停止新分配；
- 全部 shard 用已验证 Drain 迁出并 verify；
- serving_shards=0、turns=0、facts committed、registry DRAINED 后才缩容；
- 支持 pause/resume/rollback-to-safe-stop；
- 无法证明安全时 NeedsHuman，不强杀。

G4 允许自动扩缩容的前提：G1/G2/G3、容量指标、routectl drain 可暂停/恢复、迁移阶段可观测。

---

## 19. Gate

### Baseline Gate

```text
go test ./internal/authoritycomposition/... -count=1
go test -race ./internal/authoritycomposition/... -count=1
go test ./internal/logic/actor/authority/... -count=1
go test ./internal/actorrouting/... -count=1
go test -buildvcs=false ./... -count=1
go vet ./internal/authoritycomposition/... ./internal/actorrouting/...
go build -buildvcs=false ./cmd/...
git diff --check
git status --short
```

加 composed+static 单 Pod smoke：runtime 启动、Static Snapshot 安装、Player/Farm 业务主链无回归。

### G1：Remote Watch

Store CAS、disconnect/reconnect、full resync、topology fail closed、hot-path remote calls=0、
Controller 停止后 existing assignment 继续服务。

### G2：Manual Planned Migration

A→B Drain 连续三轮、旧 Writer 成功=0、Source 最终 0 shard、Target 不批量加载 Actor、
Fresh Join、未受影响 shard 继续服务。

### G3：Manual Crash Takeover

无 proof 拒绝、LeaseExpired proof 通过、旧 Pod 恢复后旧写=0、transition replay 幂等。

### G4：Autoscale Entry

G1/G2/G3 全部通过；capacity metrics 与 freshness；drain pause/resume；stage observability。

---

## 20. 必测矩阵

Store：bootstrap；R1→R2 CAS；并发 CAS 单赢家；validation/checksum；Store committed+
notify failed；restart load；Authority committed/Snapshot pending recovery；transition same-input replay；
different-input conflict。

Watch：Get；R1→R2；R1直接完整R3；duplicate；slow consumer disconnect；stream断开全量校准；
stale callback；topology fail closed；hot-path 0 RPC；Close blocked watch；race。

Holder：WARMING；READY_ZERO_SHARD；Serving count；Boot replacement stale heartbeat reject；
draining/expired holder 不可选；Registry 不授权；remote absent holder wait；static absent holder fail。

Drain：normal A→B；wait active turn；drain timeout no authority switch；authority fail no snapshot；
ASSIGNING+prepare fail；checkpoint retry；ACTIVE publish recovery；old A Redis fence；no bulk load；
unaffected shard serving。

Crash：no proof；healthy/unexpired reject；LeaseExpired pass；old Pod recovered old writes=0；replay。

Vertical E2E：20 players hot farm；quantity invariant；migration operation either old completes or recovery；
Fresh Join；operation_id no duplicate settlement；inventory/farm decrement reconcile；farm_version monotonic；
old holder stale writer success=0；Controller stopped existing assignments serve。

---

## 21. Observability

低基数指标：snapshot revision/age/CAS conflict/watch clients/reconnect/resync/slow consumer；
plan total/inflight/duration/failure；holder counts/heartbeat age/serving shards/process ready；handoff
duration/failure；fence rejects；writer epoch mismatch；actor load/activation/fresh join。

迁移结构化日志：plan_id、transition_id、domain、virtual_shard、source/target holder+boot、
base/result revision、writer/lease epoch、stage、elapsed、result、error_class。不得记录完整 token/claim/secret。

---

## 22. 最终 Premerge

从届时最新远端 integration 新建 Premerge，按依赖顺序合入冻结 final heads：

```text
Snapshot Store → Dynamic Consumer → Route Control Transport
→ Holder Control → Composition → routectl/Plan → Evidence
```

总 Gate：全仓 test/race/vet/build/diff/fsck/large-object/secret；公司 Redis probe；多 Pod static；
跨进程 Watch；A→B migration；Controller restart；manual crash takeover；多人偷菜迁移/Fresh Join。

如果 integration 在开发期间前进，Premerge 从最新 integration 创建并集中处理冲突；不得把历史
候选和最终 head 重复合入。通过后才能由 I0 推 integration 并冻结 CP-ROUTE-CONTROL-MVP。

---

## 23. Agent ICR 与交付格式

每个 Agent 首先只做 ICR，输出 `READY`、`BLOCKED` 或 `CONTRACT_CONFLICT`。Prompt 必须包含：
精确 base_commit/task_id、权威顺序、owned/forbidden write set、禁止 push/merge、Secret 规则、
测试与交付格式。

完成交付：commit SHA、精确 diff、测试原始命令和结果、未完成项、风险、回滚方式、Handoff。
Reviewer 只读，Finding 按 P0/P1/P2 分类，由原 Writer 修复。

---

## 24. 当前恢复状态

```text
latest_integration_branch = codex/integration-20260803
candidate_base_commit      = 29ed7be2f7ddc6df2e837830c165ad50ca62e28f
local_remote_match         = YES
integration_tracked_clean  = YES
route_foundation_present   = YES_ONCE
CP-ROUTE-DYNAMIC-BASE      = READY_FOR_RT2_WAVE1_ICR
RC-SCB-1                   = FROZEN_LUNA_MAX_NO_P0_P1
Wave_1_worktrees           = NOT_CREATED
formal_feature_development = NOT_STARTED
baseline_code_commit       = 29ed7be2f7ddc6df2e837830c165ad50ca62e28f
autoscaling                = BLOCKED_UNTIL_G1_G2_G3_AND_NEW_BASELINE
```

Baseline Gate 已通过。RC-SCB-1 经 official OpenAI Codex `gpt-5.6-luna`、reasoning `max`
两轮 closure：首轮 3 个 P1 接口漂移已修正，最终 P0/P1 为 0，`NO BLOCKING FINDINGS`。
下一步提交 Baseline 文档 checkpoint，并从固定 code base `29ed7be` 创建 Wave 1 三个独立 worktree；
每个 Agent 必须先只提交 ICR，未获 I0 确认前不得开始运行时代码。
