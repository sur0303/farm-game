# 30 · Actor CAS、WriterEpoch 与权威存储合同

版本：`v1.0-root-scoped-attempt-fence-freeze-candidate`  
日期：2026-08-03  
定位：冻结 PlayerActor/FarmActor 的写权、激活、CAS、Unknown Outcome、钝化、持久化 Lane、
可靠 Order 以及 `ActorAuthorityStore` 的逻辑合同；供 Actor Runtime、Repository、Redis Lua、
Workflow/Worker 和故障测试实现使用。  
状态：`30-01～30-23` 已经逐项确认，属于稳定语义；公司 Redis 的 Key、Hash Tag、Lua、TTL、
Bucket 数、序列化和生产持久化能力仍由第 31 号物理合同与平台探针决定。

本版在 P0 单 Authority Group/单 Durable Lane、LoadAndClaim 独立 `CLAIM_UNKNOWN`
状态机、Harvest/Theft 唯一 Workflow 锚点、统一 Store 状态、内部幂等查询命名、Group 派生规则、
跨 Epoch Unknown 接管和 RouteManager/Holder/ShardWriteAuthority 更新权限基础上，补齐所有 Store 强写
统一携带的完整 ShardWriteFence、Execution-Accept 后确定性终态的持久幂等语义，并明确控制面
Handoff 状态与 Redis 数据面写权状态的归属；同时对齐第 32 号
`REJECTED + REFRESH_AUTHORITY_STATE` 的可恢复终态语义。
本版进一步按第 32 号 Q32-12 明确：Plant 的具体 Steal/Care 运行快照、Care 的奖励快照以及 Theft 的
GuardDecision 都属于来源 Authority Commit 的不可分割事实，不能在提交后重新读取热更新配置补算。
本版继续澄清 `FarmChange(...)` 是由同一 CandidateState 冻结、受 PostCommitDeliveryGuard 保护的
已提交运行时输出，不是 ActorRecord 中的无界事件历史；同时统一 CareOperationReceipt、PlotVersion 和
FarmOpportunityHeader 的 Authority Commit 边界。

本文成熟度分为三层：

- `STABLE_SEMANTIC`：本文编号合同是实现 Agent 必须遵守的语义，不得由代码自行改写；
- `CANDIDATE`：整份合同尚未通过真实 Redis、故障注入和多 Holder 测试，不能宣称已经生产验证；
- `PHYSICAL_PENDING`：Redis Key/Lua、Cluster Slot、Lease/重试参数、Bucket 数和记录布局待第 31 号冻结。

上位与相邻权威：

- [26-access-session-contract.md](./26-access-session-contract.md) 唯一负责 Session、Join、Subscription
  和实时下行；
- [27-access-routing-actor-workflow-freeze.md](./27-access-routing-actor-workflow-freeze.md) 负责
  命令到领域/权威的语义矩阵、Actor Route/Runtime 和跨 Actor Workflow/Order 的 L1 语义；
- [28-command-catalog-public-protocol.md](./28-command-catalog-public-protocol.md) 负责 CommandCatalog
  Schema、公网命令、结果和弱网恢复；
- [29-internal-rpc-trusted-context.md](./29-internal-rpc-trusted-context.md) 负责 Gate、Coordinator、
  Worker 与 Holder 之间的可信 RPC 接缝；
- 本文只负责“请求到达正确权威后，怎样取得写权并得到可证明的持久化结果”。

本文不以旧 [30-redis.md](./30-redis.md) 为依据；后者是早期“Tcaplus 权威、Redis 缓存”阶段的
历史 D5 材料，与当前公司 Redis P0 权威方案冲突时不得作为实现输入。

---

## 0. 一页结论

一次合法 Actor 写入必须同时满足：

```text
当前Shard WriteAuthority仍为ACTIVE且Lease有效
+ ShardWriteFence完整匹配（Holder/Boot/AssignmentRevision/WriterEpoch/LeaseEpoch）
+ expected_writer_claim_id匹配
+ expected_record_version匹配
+ 幂等Receipt不存在，或存在且Payload Hash相同
```

三个核心 Fence：

| Fence | 范围 | 变化时机 | 防止的问题 |
|---|---|---|---|
| `writer_epoch` | 一个 RouteDomain 的 VirtualShard | Holder 写权换代 | 旧 Pod/旧 Holder 迟到写 |
| `writer_claim_id` | 一个 Actor Authority Activation/Resolver | 每次可写激活或 Resolver 接管 | 旧 Actor 内存实例迟到写 |
| `record_version` | 一个权威持久化组 | 每次成功业务提交 | 基于旧状态覆盖新状态 |

最终 Actor 安全合同：

> 系统不保证同一逻辑 Actor 在全局物理内存中永远只有一个实例。正常迁移、进程暂停、网络分区和
> 恢复期间可以短暂存在多个实例。系统保证的是：同一 Actor Authority 的强写只有在当前
> ShardWriteAuthority、WriterEpoch、WriterClaimID、RecordVersion 和 Idempotency Receipt 全部原子
> 匹配时才能提交；旧 Holder、旧 Activation 和迟到请求即使仍然存活，也无法修改权威状态。

核心执行路径：

```text
RouteIngress校验Holder/本地Lease Deadline
→ ActorRegistry Singleflight LoadAndClaim
→ 严格非重入Actor Turn
→ IdempotencyPrecheck（Receipt/Result）
→ Validate / Decide
→ BuildCandidateState
→ AuthorityStore原子Commit：
   ShardWriteAuthority/WriterEpoch
   → WriterClaimID
   → Idempotency Receipt/Hash
   → RecordVersion
   → State + Result + 必要Order/Outbox
→ 明确COMMITTED后Actor才InstallState
→ TIMEOUT_UNKNOWN时C1→C2 FenceAndClaim并读取持久ResolutionReceipt
```

结果未知时：

```text
LoadAndClaim已发送但无明确响应
→ Registry进入CLAIM_UNKNOWN
→ 保留原ActivationEntry/Future/Claim身份
→ 不安装Actor、不开放UserMailbox

Persist已发送但无明确响应
→ Actor进入RESOLVING_UNKNOWN
→ FenceAndClaimActor(C1→C2)
→ 让旧Claim永久失效
→ 根据Fence切点的Receipt和权威状态判断COMMITTED/NOT_COMMITTED
```

P0 存储与异步选择：

```text
公司Redis
├── Actor权威状态
├── Receipt / OperationResult
├── Workflow / Order
└── 分桶DueIndex

无上位Workflow的来源状态 + 来源同槽Order + 分桶DueIndex
→ 同Slot Lua原子提交

DirectDispatcher
→ 正常路径低延迟

farm-worker
→ DueIndex扫描、Lease、重试和故障恢复

MQ
→ P0不引入
```

首版所有业务写使用 `R2_CRASH_DURABLE` 或 `R3_WORKFLOW_DURABLE`；`R1_ACTOR_SOFT` 只在故障测试和
压测通过后按命令白名单开启。

P0 `auth-v1` 的路由与持久化布局冻结为：

```text
VirtualShard与AuthorityPersistenceBucket语义分离
+ 同RouteDomain内AuthorityPersistenceBucketID = VirtualShardID
+ PLAYER/FARM使用独立Hash Tag命名空间
```

这样 Shard 写权、Actor 状态、Receipt 和来源同槽 Order 才能在同一 Redis 线性化域内校验；Holder
迁移只换 Assignment/Lease/WriterEpoch，不搬迁数据。

---

## 1. 范围与非目标

### 1.1 本文回答的问题

1. 一次 Actor 写入为什么合法；
2. WriterEpoch、WriterClaimID、RecordVersion 分别防什么；
3. Actor 第一次请求如何只加载一次；
4. CAS 明确成功、冲突、失权和超时未知时分别怎么办；
5. Shard Drain、滚动发布和崩溃接管怎样避免双写；
6. Actor 怎样安全 Passivate，旧回调为何不能删除新 Actor；
7. 业务状态、Receipt、Order 和 DueIndex 哪些必须原子提交；
8. SoftSave 如何避免低版本覆盖高版本 Barrier；
9. Repository 给 Actor 暴露哪些逻辑接口；
10. 什么测试通过后才允许正式 Freeze。

### 1.2 本文不冻结

- Redis Key 的最终字符串格式；
- Redis Hash Tag 的最终字符串、Cluster Slot 碰撞与 Key 前缀；`auth-v1` 的 BucketID 与
  VirtualShardID 一一映射已经冻结，不再由第 31 号重新选型；
- Lua 脚本编号、参数编码和返回码数值；
- Redis Cluster Slot、Bucket 数和 Worker 分片数；
- Lease TTL、续租周期、Backoff、Mailbox 和等待者上限；
- ActorRecord 使用 Protobuf Blob、Hash 还是平台记录；
- 公司 Redis 的复制、落盘、备份、容灾和 RPO/RTO；
- 未来是否迁移 TcaplusDB；
- 业务数值、作物配置、好友权限和偷菜规则。

这些属于第 31 号物理合同、场景合同或容量验证，不能反向改变本文的写权语义。

---

## 2. 术语与权威对象

```text
ActorAddress
├── route_domain          PLAYER / FARM
└── actor_id              player_id / farm_id

VirtualShard
→ ActorAddress经版本化ShardCalculator得到的稳定路由分片

Holder
→ 当前承载某VirtualShard并拥有有效Lease的Pod进程代次

AuthorityPersistenceGroup
→ 一次原子提交覆盖的一个或多个同域物理记录
→ P0由ActorAddress + authority_layout_version唯一派生，调用方不能自由选组

AuthorityPersistenceBucket
→ 一组必须处于同一Redis Cluster Slot的权威Key；auth-v1中BucketID等于VirtualShardID

ActorRecord
→ PlayerActor/FarmActor的长期权威状态

Receipt
→ 某个Operation或Participant步骤已经产生业务效果的持久证据

OperationRootRef
→ 本次业务意图实际修改或创建的根Authority；与subject、command、operation共同组成
  EffectiveIdempotencyKey，防止不同Farm、FriendPair或独立Workflow之间的Receipt互相污染

Order / Workflow
→ 跨Actor或延迟工作可恢复推进的持久锚点
```

必须区分四种版本：

| 字段 | 作用 |
|---|---|
| `assignment_revision` | 路由表快照演进；可以变化但不一定换写权 |
| `writer_epoch` | Shard Holder 写权代次 |
| `writer_claim_id` | 单 Actor 可写 Activation/Resolver 身份 |
| `record_version` | 权威业务状态版本 |

`activation_generation` 是第五个、仅进程内存在的生命周期 Fence；它不进入业务 CAS，不能代替
`writer_claim_id`。

P0 每个 Actor 只定义一个主要权威持久化组：

```text
PlayerActor(player_id) → PlayerAuthorityGroup
FarmActor(farm_id)     → FarmAuthorityGroup
```

“一个组”不要求把所有字段编码成一个巨大 Blob；它可以包含同一 Hash Tag 下的状态 Key、Receipt Key、
来源同槽 Order 和 DueIndex，但必须共享同一个 `ActorClaimAuthority`、RecordVersion/CAS 边界和
Persistence Lane。P0 不实现一个 Actor 横跨多个 Slot 的 ClaimSet。

---

## 3. 30-01：三层写入 Fence

### 3.1 合法提交条件

每个强写都必须携带并原子校验：

```text
ShardWriteFence
+ expected_writer_claim_id
+ expected_record_version
```

完整语义还包括：

```text
ShardWriteAuthority.state == ACTIVE
+ Shard Lease未过期
+ 调用方是受信Holder服务身份
```

三者不能互相替代：

- 只有 RecordVersion：旧 Holder 仍可能基于最新读取发起非法写；
- 只有 WriterEpoch：同 Holder 内旧 Activation 仍可能迟到写；
- 只有 WriterClaim：同 Activation 基于旧业务状态仍可能覆盖新状态。

`writer_claim_id` 也不是“只有单 Actor 独立迁移才需要”。Actor 钝化重激活、同一 Holder 内
Activation 换代、`TIMEOUT_UNKNOWN` 解析、迟到存储请求/回调以及热点 Actor 热迁移都会产生“Shard
Epoch 没变，但旧 Actor 实例仍可能写”的窗口，因此必须使用持久 Claim Fence。进程内
`activation_generation` 只能保护 Registry 和回调，不能阻止旧请求写共享存储。

### 3.2 WriterEpoch 不能只读取 ActorRecord 副本

ActorRecord 可以保存最近观察到的 Epoch 作为诊断信息，但 CAS 必须验证当前权威 Shard 写权；不能
只比较记录中遗留的旧数字：

```text
Shard已从epoch=8切到9
ActorRecord仍显示8
旧Holder请求携带8

如果只比较ActorRecord
→ 旧请求可能误成功

如果比较当前ShardWriteAuthority
→ WRITER_EPOCH_MISMATCH
```

第 31 号必须证明 Shard WriteAuthority 和 AuthorityCommit 之间存在同一线性化边界，不能使用一次
非原子的“先 GET Epoch、再写 ActorRecord”冒充。

### 3.3 `auth-v1` 的路由—持久化映射

P0 采用第 27 号冻结的 A′ 方案：

```text
VirtualShard
→ 逻辑执行分片与Shard写权单位

AuthorityPersistenceBucket
→ Redis同槽原子提交单位

auth-v1:
AuthorityPersistenceBucketID = VirtualShardID
```

这解决了一个具体物理问题：提交 Lua 必须同时读取当前 ShardWriteAuthority，并写 Actor 状态、Receipt
和来源 Order；如果两者由独立 Hash 得到不同 Slot，Redis Cluster 会拒绝跨 Slot 原子脚本。

二者仍不可在领域模型中混为同一对象：Holder Assignment 可以频繁变化，而 `auth-v1` Bucket 不能
随 Pod 变化。PLAYER/FARM 使用独立的版本化 Hash Tag 命名空间。ShardCount、HashVersion 或 HashSeed
投入生产后不得原地修改；未来布局变化必须使用 `auth-v2` 和显式迁移。

---

## 4. 30-02：WriterEpoch 的归属与推进

### 4.1 归属

```text
writer_epoch
→ VirtualShard级

writer_claim_id
→ Actor Activation/Resolver级

record_version
→ AuthorityPersistenceGroup级
```

示例：

```text
FARM Shard 27
Holder=Pod A
writer_epoch=8

FarmActor(1001)
writer_claim_id=claim-A
record_version=15
```

正常种植：

```text
epoch仍为8
claim仍为claim-A
record_version 15→16
```

Actor 空闲卸载后重新激活：

```text
epoch仍为8
claim-A→claim-B
record_version仍从16继续
```

Shard 迁移到 Pod B：

```text
epoch 8→9
新Activation获得claim-C
业务版本继续从16开始
```

### 4.2 唯一推进者

只有 RouteManager 协调的 `ShardWriteAuthority` 换代可以推进 WriterEpoch。当前 Holder 可以在完整
Fence 匹配时续租，但不能自行改变 Holder、Boot、WriterEpoch 或 AssignmentRevision。以下行为不得
自行增加 Epoch：

- 普通 Actor Turn；
- Actor Load/Passivation；
- 业务 RecordVersion 冲突；
- 客户端重连；
- FarmSubscription 切换；
- Worker 重试。

Holder 更换、Holder Boot 换代、计划内 Handoff 或故障接管才推进 WriterEpoch。

---

## 5. 30-03：Actor 激活与 Singleflight

### 5.1 激活主链

```text
命令到达当前Holder
→ ActorRegistry.Resolve(ActorKey)
→ ABSENT时创建LOADING Entry
→ 同ActorKey并发请求共享Activation Future
→ 生成activation_generation
→ 生成稳定claim_operation_id和new_writer_claim_id
→ LoadAndClaimActor
→ 明确成功后在当前Holder/Boot Registry中安装唯一Activation
→ 等待请求分别进入有界Mailbox
```

`Singleflight` 只合并加载，不合并业务命令。100 个请求同时访问同一 Actor 时只能发生一次
`LoadAndClaimActor`，但这个“唯一”只作用于当前 Holder/Boot 的 Registry；成功后仍有 100 条独立
Actor 消息。另一个隔离或暂停中的 Holder 可能还保留旧对象，但其旧 Epoch/Claim 无法提交。
不同 ActorKey 可以并行加载。

### 5.2 Registry 最小状态

```text
ActivationEntry {
  actor_key
  state                    # LOADING / CLAIM_UNKNOWN / ACTIVE / FENCED / RECONCILE_REQUIRED
  activation_generation
  activation_future
  bounded_waiters
  claim_operation_id
  target_writer_claim_id
  writer_epoch
  writer_claim_id
  record_version
}
```

### 5.3 统一 ShardWriteFence 与 LoadAndClaimActor

所有可能取得或使用 Actor 强写资格的 Store 请求必须携带同一逻辑 Fence：

```text
ShardWriteFence {
  expected_holder_id
  expected_holder_boot_id
  expected_assignment_revision
  expected_writer_epoch
  expected_lease_epoch
}
```

`route_domain/virtual_shard` 不接受业务调用方自由填写；Store 由 `ActorAddress + HashProfile +
authority_layout_version` 重新派生。`ShardWriteFence` 也不来自客户端或 Actor 业务 Payload：Holder
Repository Adapter 将 ActorRouteHeader 中的 Assignment 视图与本地已验证 Shard Lease 证明合并后注入。

Store 使用自身可信时间原子检查当前 `ShardWriteAuthority`：Holder、Boot、AssignmentRevision、
WriterEpoch、LeaseEpoch、`ACTIVE` 和尚未到期必须全部匹配。Holder 本地 Lease Deadline 用于热路径
Fail Closed；Store 检查是共享存储最终 Fence，二者不能互相替代。

逻辑输入：

```text
ActorAddress
ShardWriteFence
claim_operation_id
new_writer_claim_id
```

逻辑原子效果：

```text
验证当前Shard写权
→ 初始化或读取ActorRecord
→ 将旧WriterClaim原子换成new_writer_claim_id
→ 返回权威状态和record_version
```

同一 `claim_operation_id + new_writer_claim_id` 必须幂等。响应丢失后使用原身份重试，不能每次生成
新 Claim。

### 5.4 LoadAndClaim 的独立 CLAIM_UNKNOWN 状态机

LoadAndClaim Unknown 发生在 Actor 尚未安装时，不能复用业务 Commit 的
`ACTIVE → RESOLVING_UNKNOWN` 状态机。Registry 冻结为：

```text
ABSENT
→ LOADING
→ ACTIVE

LOADING
→ CLAIM_UNKNOWN
→ ACTIVE / ABSENT / FENCED / RECONCILE_REQUIRED
```

| LoadAndClaim 结果 | Registry 行为 |
|---|---|
| 明确未执行/不可用 | Entry 回到 `ABSENT`，完成等待者并按策略 Backoff |
| Epoch 不匹配 | Entry 进入 `FENCED`，等待者收到 RouteStale，不在本地安装 |
| `TIMEOUT_UNKNOWN` | 保持原 Entry，进入 `CLAIM_UNKNOWN`，启动唯一恢复者 |
| 加载数据损坏或 Claim 事实无法解释 | `RECONCILE_REQUIRED`，Fail Closed |

`CLAIM_UNKNOWN` 必须遵守：

1. 不安装半初始化 Actor，不开放 UserMailbox；
2. `ActivationEntry`、原 `activation_future`、`claim_operation_id` 和 `new_writer_claim_id` 保持不变；
3. 后续同 Actor 请求继续加入同一个有界 Future，不创建第二次 LoadAndClaim；
4. 恢复优先使用原 `claim_operation_id + new_writer_claim_id` 幂等重入或读取 Claim 结果，禁止为了
   “恢复”直接生成 C3；
5. 某个等待者 Deadline 到达只移除该等待者，不取消共享恢复；等待者、恢复 Deadline、Backoff、
   ActivationEntry 数和激活速率都必须有界。

恢复结果：

```text
当前Claim已经是目标C2，且claim_operation_id一致
→ 返回原LoadAndClaim结果
→ 只安装一次Activation

当前Claim仍是原C1
→ 在恢复Deadline内使用完全相同的Claim请求重试

WriterEpoch已经变化
→ 本地Entry进入FENCED
→ 等待者返回RouteStale，由新Holder重新激活

当前Claim是无法归因的第三方C3
→ RECONCILE_REQUIRED，或在能证明已失去路由权时返回RouteStale
```

物理 Store 必须保留足以幂等识别 Claim 操作的证据；具体 Key/Lua 由第 31 号实现，但不得改变上述
状态机。

---

## 6. 30-04：Authority CAS 与结果分类

### 6.1 Actor 先做幂等预检，再计算 Candidate

```text
构造EffectiveIdempotencyKey + canonical_payload_hash
→ IdempotencyPrecheck
   ├── 同Key + 同Hash已有终态：直接返回原OperationResult
   ├── 同Key + 不同Hash：IDEMPOTENCY_CONFLICT
   └── 不存在：继续
→ 基于Actor当前内存State重新校验业务规则
→ 生成CandidateState
→ 调用AuthorityStore.Commit
→ COMMITTED后才InstallState
```

幂等预检必须发生在业务校验之前。原因是一次操作可能已经成功，但客户端因弱网没有收到结果；重试
时土地、库存或作物周期已经变化。如果先按“当前状态”校验，服务端可能错误返回“种子不足”或
“地块非空”，而不是返回第一次已成功的种植结果。

预检优先读取 Actor 激活时加载的有界 Receipt/Result 索引；若目标已从热索引整理出去，则调用
`LookupIdempotencyOutcome` 查询可证明的终态或归档证据。预检只是快速路径，Store 的原子 Commit 仍必须
再次检查 Receipt，防止两个并发请求同时通过预检。

Repository 不计算成熟、产量、宠物拦截或任务规则；它只验证 Fence、Version、Receipt 并原子保存
Actor 已经计算好的候选结果。

### 6.2 AuthorityCommitStatus

Store 提交只使用以下统一状态；第 27 号不再定义第二套名称：

| 结果 | 含义 | Actor 行为 |
|---|---|---|
| `COMMITTED` | 状态、Receipt、持久终态与必要 Intent 已提交 | 安装返回的权威状态，返回本次业务终态 |
| `ALREADY_COMMITTED` | 同幂等键、同 Hash 已存在持久终态 | 返回原成功/拒绝/无变化结果，不重复执行 |
| `IDEMPOTENCY_CONFLICT` | 同幂等键、不同 Hash | 拒绝并告警 |
| `RECORD_VERSION_CONFLICT` | RecordVersion 已过期 | 暂停该 Actor 写入，重载权威状态后最多重算一次 |
| `WRITER_EPOCH_MISMATCH` | 当前 Holder 失去 Shard 写权 | Actor Fenced，重新路由 |
| `WRITER_CLAIM_MISMATCH` | 当前 Activation 已失效 | 停止 Actor，不继续 Mailbox |
| `UNAVAILABLE_NOT_SENT` | 能证明请求尚未进入存储执行边界 | 丢弃 Candidate，按 Deadline 有界重试 |
| `TIMEOUT_UNKNOWN` | 请求已发送，无法证明是否执行 | 进入持久 Unknown 解析，禁止隐藏重试 |

`LoadAndClaimStatus`、`AuthorityCommitStatus`、`FenceResolutionStatus` 和 `WorkClaimStatus` 必须是
不同逻辑枚举，不能把 `COMMITTED`、`NOT_COMMITTED`、`CLAIM_UNKNOWN`、`LEASED` 等塞进一个万能
状态。`APPLIED` 可以继续用于 `GUARD_APPLIED` 等领域 Participant 结果，不再表示 Store 提交成功。

同一 WriterClaim 下第一次出现 `RECORD_VERSION_CONFLICT` 已经说明内存与权威存储发生了非预期偏差，必须记录
高严重度指标并执行一次“重载 → 幂等预检 → 业务重验 → 新 Candidate”。若同一 Claim 下再次冲突，
进入 `RECONCILE_REQUIRED`，不得无限 CAS 重试。若冲突来自 WriterEpoch/Claim 变化，则按失权处理，
不能走普通重载。

#### 6.2.1 确定性终态的持久幂等

第 28 号 `TerminalOutcomePolicy=DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT` 的直接 Actor 写命令，
一旦越过 Execution-Accept，以下确定性终态都必须由同一次 Authority Commit 保存：

```text
SUCCEEDED
REJECTED + RetryDirective.NONE
REJECTED + RetryDirective.REFRESH_AUTHORITY_STATE
NO_CHANGE
```

`REJECTED/NO_CHANGE` 可以不改变领域 Model，但仍需原子写入 Idempotency Receipt 与
StoredOperationOutcome，并推进承载这些事实的 `record_version`。这样同一个 operation_id 在玩家资产、
作物或配置后来变化后，仍返回原终态，不会把旧业务意图重新解释成一次新操作。

`REFRESH_AUTHORITY_STATE` 表示旧 operation 已经确定终止，刷新后若玩家仍要操作必须创建新
operation；因此它可以是持久终态。StoredOperationOutcome 保存稳定 ErrorCode，不保存旧连接当时的
RetryDirective。GetOperationResult 按第 28/32 号 ErrorRecoveryPolicy 在查询时重建当前
`completed_retry_directive`。

下列结果不按业务终态保存：

- ADMITTED/Execution-Accept 之前的协议、Session、路由和容量拒绝；
- Redirect、临时不可用和 `RETRY_SAME_OPERATION`；
- `PROCESSING`、`TIMEOUT_UNKNOWN` 与尚未完成的 Workflow/Order。

它们分别按接入重试、Workflow 恢复或 Unknown Resolver 处理。独立 Workflow 的最终结果由其唯一
WorkflowRecord/结果发布链保存，不允许再在 Actor 中创建第二份全局终态锚点。

### 6.3 原子校验顺序

```text
RPC入口验证受信服务身份
→ Holder Runtime验证本地Shard Lease单调Deadline
→ Store原子验证持久ShardWriteAuthority(holder/boot/state/epoch)
→ WriterEpoch
→ WriterClaimID
→ EffectiveIdempotencyKey / Receipt
   ├── 相同Hash：ALREADY_COMMITTED
   └── 不同Hash：IDEMPOTENCY_CONFLICT
→ RecordVersion
→ 原子提交State + Receipt + Result + 必要Intent
```

Redis Lua 本身不负责验证 mTLS；服务身份由第 29 号 RPC Ingress 验证。它也不能只相信 Holder 的本地
Lease 缓存：本地 Lease 用于热路径 Fail Closed，Store 仍以同一线性化域中的持久 Holder/Boot/Epoch
令牌阻止旧 Holder 写入。

Receipt 必须在 RecordVersion 前检查。第一次提交成功但响应丢失后，重试仍携带旧版本；先检查
RecordVersion 会误报冲突，先检查 Receipt 才能返回原结果。

Epoch/Claim 必须先于 Receipt。旧 Holder 或旧 Activation 即使能访问共享 Redis，也不能越过当前
写权 Fence 代表新权威返回结果；它应 Redirect/停止，由当前 Holder 使用同 operation_id 查询。

### 6.4 调用侧结果

| 结果 | 判定条件 | 行为 |
|---|---|---|
| `UNAVAILABLE_NOT_SENT` | 能证明请求尚未进入存储 | 丢弃 Candidate，可使用相同请求有界重试 |
| `TIMEOUT_UNKNOWN` | 请求已经发送，无法证明是否执行 | 立即进入 `RESOLVING_UNKNOWN` |

Repository Adapter 禁止把已发送超时偷偷当成普通 Unavailable 自动重试，也禁止生成新 operation、
Claim 或 Participant 身份。

### 6.5 结果确定性分类

Store/Runtime 必须先把结果分成“能否证明”，再由第 32 号场景合同映射成公网 CommandStatus：

| 分类 | 典型结果 | 语义 |
|---|---|---|
| `DEFINITELY_COMMITTED` | `COMMITTED`、`ALREADY_COMMITTED` | 业务效果已发生且有 Receipt |
| `DEFINITELY_NOT_COMMITTED` | `UNAVAILABLE_NOT_SENT`、明确的 Epoch/Claim/Version 拒绝 | 本 Candidate 未提交；是否重新计算由上层决定 |
| `BUSINESS_CONFLICT` | `IDEMPOTENCY_CONFLICT`、业务校验拒绝 | 不应透明重试 |
| `UNKNOWN_OR_RECONCILE` | `TIMEOUT_UNKNOWN`、`RECONCILE_REQUIRED` | 不能对客户端谎报普通失败 |

`RECORD_VERSION_CONFLICT` 只能证明“这一份 Candidate 没有提交”，不代表整个 operation 从未被其他尝试提交，
因此重载后仍必须先查 Receipt。

本文不使用 `ADMITTED/ACCEPTED` 代指 Store 执行边界。根据第 29 号唯一合同：

```text
Execution-Accept之前
→ 可以返回RouteRejection
→ 调用方刷新Assignment并使用原operation_id有界重试

Execution-Accept之后
→ 不得要求Gate重跑完整业务命令
→ FirstHop内部处理Participant重路由
→ 或返回最终BusinessCommandOutcome
→ 已有可靠锚点/结果未知时返回PROCESSING + POLL_OPERATION_RESULT
```

`REJOIN_REQUIRED` 是第 26/28/29 号定义的 Subscription 实时视图恢复 Push，不是
ActorAuthorityStore 状态。旧农场命令的 CommandResult 继续回答原 operation；是否 Fresh Join 由独立
实时失效通道决定。具体 ErrorCode 和公网业务结果由第 32 号按场景冻结。

---

## 7. 30-05：TIMEOUT_UNKNOWN 与 FenceAndClaimActor

### 7.1 为什么一次 GET 不够

```text
原CAS已发出
→ Actor等待超时
→ 此刻GET不到Receipt
→ 原CAS可能仍在网络或Redis输入队列
→ 稍后仍可能提交
```

所以“现在没有 Receipt”不等于“以后不会提交”。必须先建立一个让旧请求永久失效的线性化切点。

### 7.2 Fence 的含义

当前 Actor 写身份为 C1：

```text
FenceAndClaimActor(C1→C2)
```

原子把合法 WriterClaim 换成 C2。所有携带 C1 的迟到请求从这个切点以后都会返回
`WRITER_CLAIM_MISMATCH`。

### 7.3 两种合法顺序

原 CAS 记为 R，Fence 记为 F；二者必须位于同一个线性化域。

```text
R→F
```

意味着：

```text
原CAS使用C1提交State+Receipt
→ Fence再把C1换成C2
→ Fence切点能看到Receipt
→ COMMITTED
```

```text
F→R
```

意味着：

```text
Fence先把C1换成C2
→ 切点上没有Receipt且业务状态仍为before
→ 迟到CAS携带C1被拒绝
→ NOT_COMMITTED
```

不存在“Fence 已经确认未提交，但旧 CAS 之后又成功”的第三种结果。

### 7.4 请求与幂等

```text
FenceAndClaimRequest {
  ActorAddress
  ShardWriteFence
  old_writer_claim_id
  new_writer_claim_id
  resolve_operation_id
  original_idempotency_key
  canonical_payload_hash
  before_record_version
  before_state_digest
}
```

`resolve_operation_id + new_writer_claim_id` 必须稳定。Fence 响应丢失后：

- 当前 Claim 已是同一 C2：读取持久 `ResolutionReceipt` 并返回原 Fence 结果；
- 仍是 C1：在 Deadline 内使用同一请求重试；
- 出现第三方 Claim：停止当前 Resolver，重新路由或进入对账。

仅仅看到“当前 Claim 已是 C2”不够，因为 C2 之后还可能发生新的业务提交；如果再次根据当前状态
重算 Fence 结果，原来的线性化切点就会漂移。因此 `FenceAndClaimActor` 必须在与 C1→C2 换代相同
的原子提交中创建：

```text
ResolutionReceipt {
  resolve_operation_id
  actor_address
  purpose                       # RESOLVE_UNKNOWN
  old_writer_claim_id
  new_writer_claim_id

  original_idempotency_key
  canonical_payload_hash
  before_record_version
  before_business_version_or_digest

  cut_record_version
  cut_business_version_or_digest
  resolution_status             # COMMITTED / NOT_COMMITTED / RECONCILE_REQUIRED
  original_result_ref
}
```

同一 `resolve_operation_id + new_writer_claim_id` 的重试必须返回这份持久结果，即使 C2 随后又完成了
其他写入也不能改判。`ResolutionReceipt` 受本文第 13.6 节的有界与归档合同约束，但在 Unknown 恢复窗口
结束前不得被静默淘汰。

### 7.5 解析结果

| 切点结果 | 结论 |
|---|---|
| 原 Operation Receipt 存在且 Hash 相同 | 持久写入 `ResolutionReceipt(COMMITTED)`，返回原结果 |
| Receipt 不存在，业务状态/版本仍等于 before | 持久写入 `ResolutionReceipt(NOT_COMMITTED)`，可用原幂等身份重试 |
| Receipt 不存在，但出现无法解释的业务变化 | 持久写入 `ResolutionReceipt(RECONCILE_REQUIRED)`，禁止自动重放 |
| Fence 自身 `TIMEOUT_UNKNOWN` | 保持隔离，使用同 Resolver 身份重试 |
| WriterEpoch 变化 | 当前 Holder 停止，由新 Holder 接管解析 |

WriterClaim 的元数据换代不得被误判为业务状态变化。若底层数据库对任意字段变化自动推进物理行
版本，第 31 号 Adapter 必须额外暴露可比较的业务版本或 Fence 前状态摘要。

### 7.6 Unknown 隔离范围

- P0 每个 PlayerActor/FarmActor 只有一个主要 AuthorityPersistenceGroup；该组 Unknown 时保守阻塞
  此 Actor 的全部业务写；
- 未来若把一个 Actor 拆成多个独立 Persistence Group，必须先冻结命令到 Group 的依赖矩阵、
  ClaimSet/版本组合和跨 Group Intent；不能仅因性能考虑自行并发写多个 Slot；
- 读取 Unknown 状态的查询不得返回基于旧内存的权威结论；
- Resolver 得到终态并安装权威状态后，才能恢复业务命令。

### 7.7 WriterEpoch 换代后的 Unknown 接管

同一 Epoch 内使用 WriterClaim C1→C2 建立切点；WriterEpoch 已经从 E8 换到 E9 时，Epoch 换代本身
是更外层 Fence。前提是 ShardWriteAuthority 换代与旧 Epoch Actor Commit 位于同一线性化域：

```text
旧E8 Commit先提交
→ E9换代后仍能读到同EffectiveIdempotencyKey/Hash的Receipt
→ COMMITTED

E9换代先提交
→ 所有迟到E8 Commit被WRITER_EPOCH_MISMATCH拒绝
→ 在恢复证据窗口完整时，没有Receipt即可证明旧Candidate未提交
```

新 Holder 接管原 Unknown 时必须携带原 `EffectiveIdempotencyKey + canonical_payload_hash`，并按以下规则恢复：

1. Receipt/ResolutionReceipt 存在且 Hash 相同：返回原结果；
2. Receipt 不存在，Epoch 切点可证明旧写已永久失效，且 Receipt/Tombstone/ExpiredEvidence/Workflow 等恢复证据仍在
   完整保留窗口内：持久化 `NOT_COMMITTED` 解析结论；场景允许重试时继续使用原业务幂等身份，
   但重新计算的新 Candidate 必须使用新的 `commit_request_id`；
3. Hash 冲突：`IDEMPOTENCY_CONFLICT`，禁止重放；
4. 证据已经超过在线保留窗口或状态无法解释：`RECOVERY_OVERDUE/RECONCILE_REQUIRED`，公网保持
   `PROCESSING` 或进入对账，不能把它当作 `NOT_FOUND/EXPIRED` 创建新业务意图。

新 Holder 不得发明新的玩家 operation，也不得仅凭“当前查不到 Receipt”立即重放。具体 Epoch 换代
Lua 和证据保留水位是第 31 号必须验证的物理测试项。

---

## 8. 30-06：Shard Lease、Handoff 与故障接管

### 8.1 逻辑 ShardWriteAuthority

```text
ShardWriteAuthority {
  route_domain
  virtual_shard
  holder_pod_id
  holder_boot_id
  writer_epoch
  assignment_revision
  lease_epoch
  lease_state
  lease_expire_at
}
```

跨文档 Revision 映射冻结为：

```text
ShardWriteAuthority.assignment_revision
= RouteSnapshot中该Shard的ShardRoute.ShardRevision
= ActorRouteHeader.shard_revision

RouteSnapshot.GlobalRevision
= ActorRouteHeader.source_snapshot_revision
```

RouteManager 每次改变 Holder/Boot/WriterEpoch 或 Assignment 状态时，必须先原子提交完整
`ShardWriteAuthority` 并推进 `assignment_revision`，再发布包含同一 Holder/Boot/Revision/Epoch 元组的
RouteSnapshot。不得把 `GlobalRevision` 当成 Store 写 Fence，也不得从不同记录版本拼装元组。

`holder_boot_id` 区分同一个 Pod 名称的不同进程生命。新进程不能继承旧进程内存 Lease。

Redis `ShardWriteAuthority` 是数据面最终写权 Fence；RouteManager 是 Holder/Boot/WriterEpoch/迁移
状态换代的唯一协调者。当前 Holder 只允许在 holder/boot/writer_epoch/lease_epoch 等完整 Fence
匹配时续租，不允许自行改变 Holder、Boot、WriterEpoch 或 AssignmentRevision。

`DRAINING/ASSIGNING` 只属于 RouteManager 控制面 Handoff 记录和 Holder Runtime；它们不进入
`ShardWriteAuthority.lease_state`。数据面只保存 `UNASSIGNED/ACTIVE`：Transfer 之前为
`ACTIVE(A)`，原子线性化之后为 `ACTIVE(B)`，显式卸载时可为 `UNASSIGNED`。控制面状态不能被
Store 当作新的可写状态，且不得先发布 B 的路由再完成该线性化切换。

Shard Lease Claim/Renew 只接收 `requested_ttl_ms`。Lease Authority 使用自身可信时间裁决并返回：

```text
lease_epoch
writer_epoch
assignment_revision
accepted_ttl_ms
```

Holder 在发送请求前记录本地单调时钟起点，并使用
`renew_started_monotonic + accepted_ttl_ms - safety_margin` 建立保守 Deadline；不得直接比较本机 Unix
时间与 Store 的 `lease_expire_at`。控制面短暂不可用时可以服务到 Deadline；Deadline 到达仍未获得
有效续租确认时必须 Self-Fence：

```text
停止新命令Admission
→ 不开始新的写Turn
→ 当前Activation进入FENCED/STOPPING
→ 返回ROUTE_UNAVAILABLE或让调用方重试
```

若仍保留独立 AssignmentStore，它只保存迁移意图、Handoff 进度和审计；Assignment Snapshot 是已提交
ShardWriteAuthority 的路由投影，不是第二套写权真值。

### 8.2 计划内 Handoff

```text
RouteManager标记Shard DRAINING
→ A停止新写
→ A完成已经开始的有界Turn
→ 强写结果明确，Soft状态按合同ForceFlush
→ A确认Drain
→ RouteManager原子切换A/epoch=8到B/epoch=9
→ B取得ACTIVE Lease
→ 发布与B/boot/epoch/revision完全一致的Assignment Snapshot
→ B按请求懒加载Actor
```

禁止先发布新 Assignment 再切换 Store 写权。若 ShardWriteAuthority 已切到 B 但 Snapshot 发布失败，
只能暂时不可用：旧 A 的写被 Store 拒绝，RouteManager 恢复后重发 Snapshot；不得回退为 A 继续写。

不要求迁移整个 Shard 的 Actor 内存。热点 Actor Snapshot Handoff 可以作为以后优化，但不是 P0
正确性基础。

### 8.3 意外崩溃

```text
A崩溃或Lease失效
→ 故障确认
→ RouteManager将Shard分配给B并推进Epoch
→ B取得ACTIVE Lease
→ Actor按需LoadAndClaim
→ 客户端用原operation_id重试
```

### 8.4 Epoch 竞争的唯一顺序

```text
A的CAS先于Epoch切换
→ 可以提交
→ B加载时必须看到它

Epoch切换先于A的CAS
→ A返回WRITER_EPOCH_MISMATCH
→ 不得提交
```

本地 Assignment Snapshot 是性能缓存，不是写权。缓存过期最多造成一次 Redirect；最终是否允许写
由 Holder Ingress 和 AuthorityStore Fence 决定。

---

## 9. 30-07：Actor 生命周期与 Passivation

### 9.1 状态机

```text
ABSENT
→ LOADING
→ ACTIVE
→ QUIESCING
→ PASSIVATING
→ ABSENT

ACTIVE
├→ RESOLVING_UNKNOWN
├→ FENCED
└→ STOPPING
```

`QUIESCING` 是短暂可撤销阶段。新消息到达时可以取消空闲卸载并回到 ACTIVE；进入 PASSIVATING
以后不再恢复旧 Activation，新请求等待释放后触发新一轮 Singleflight 激活。

### 9.2 Passivation 准入

必须同时满足：

```text
Mailbox为空
+ 没有正在执行的Turn
+ 没有Persistence请求in-flight
+ 不处于RESOLVING_UNKNOWN
+ 没有必须等待的系统消息
+ 达到idle_timeout
+ Dirty Soft状态已ForceFlush成功
```

ForceFlush 返回 `TIMEOUT_UNKNOWN` 时进入 Resolver，不能删除 Actor。

### 9.3 不主动清空持久 Claim

正常卸载时保留 ActorRecord 中的旧 Claim。下次 `LoadAndClaimActor` 原子换成新 Claim；主动清空会
增加一次没有业务价值的写入及新的 Unknown 窗口。

### 9.4 ActivationGeneration

所有异步回调和释放操作携带：

```text
actor_key
activation_generation
writer_claim_id
request_id
```

Registry 删除必须使用 Compare-and-Release：

```text
ReleaseActivation(actor_key, expected_generation)
```

旧 generation=100 的迟到回调不能删除已经安装的 generation=101。

### 9.5 资源释放

PASSIVATING 完成后必须释放：

- Actor 对象；
- UserMailbox/SystemQueue；
- 等待者/Future；
- run loop/goroutine；
- Actor 定时器；
- Registry Entry；
- Actor 级统计对象。

---

## 10. 30-08：权威提交原子边界

### 10.1 P0 单一主要 AuthorityPersistenceGroup

每个 Actor 的主要状态、幂等 Receipt、OperationResult、来源同槽 Order 和必要 Outbox 可以是多个
Redis Key，但必须：

```text
位于同一个auth-v1 Hash Tag
+ 受同一个ActorClaimAuthority约束
+ 共享一个Group RecordVersion/CAS边界
+ 由同一条Persistence Lane排序
+ 在命令要求的Lua原子边界中共同提交
```

这既避免一个无限增长的大 Blob，也避免首版引入多 Claim、多 Slot 和跨组 Unknown。未来拆组必须由
容量证据驱动，并新增 ClaimSet、命令依赖矩阵和跨组 Intent 合同。

### 10.2 必须同提交

```text
Fence/Version校验
+ 权威业务状态
+ RecordVersion推进
+ OperationReceipt或ParticipantReceipt
+ 必要OperationResult
+ 必要Order/PendingOutbox
```

业务效果与 Receipt 不能分两次写；来源事实与不可丢的后续 Intent 也不能分两次写。

### 10.3 不能强行同事务

以下内容不进入来源 Actor 的权威原子组：

- 另一个 PlayerActor/FarmActor 的状态；
- Gate 下行、客户端动画与 QueueAccepted；
- MQ 发布；
- 查询投影、红点缓存、排行榜；
- 日志、Metric 和 TLog 导出。

来源 Actor 只需原子提交业务事实和可靠 Intent；外部 Participant、投影与通知最终完成。

### 10.4 场景原子边界

本节代码块中的 `FarmChange(X)` 表示：Actor 在发起 Store 强提交前，已经根据同一 CandidateState 和
目标 `farm_version` 构造不可变 `CommittedFarmChange` 候选并安装 PostCommitDeliveryGuard。Authority
Store 原子保存业务状态、版本、Receipt、Decision/Header 与必要 Order，不保存无界 FarmChange 历史；
确认 `COMMITTED` 后必须把原候选交给当前 delivery generation，或将该 Farm 实时流标记失效并要求
Subscription Fresh Join。不得在提交后重新读取热配置或当前状态拼装另一份 Change。

#### Buy

```text
Wallet扣金币
+ Inventory加种子
+ ShopCounter
+ OperationReceipt
+ Player RecordVersion
```

#### Plant / ReserveSeed

```text
PlayerActor：
available seed→reserved
+ SeedReservation
+ ParticipantReceipt(RESERVE_SEED)
+ RecordVersion

FarmActor：
Plot创建CropCycle
+ CropRuleSnapshot版本/Hash与作物运行字段
+ StealPlanSnapshot（整数总额度和槽位）
+ CarePlanSnapshot（确定IncidentID和due_at）
+ ParticipantReceipt(COMMIT_PLANT)
+ PlotVersion
+ FarmVersion
+ FarmOpportunityHeader / opportunity_dirty_generation
+ 构造候选CommittedFarmChange(Plant)
+ RecordVersion
```

两个 Participant 提交不组成分布式事务，由 PlantWorkflow 恢复。
PlantWorkflow 必须在 ReserveSeed 前保存完整 CropRuleSnapshot；Farm Commit 后即使当前配置已经更新或
旧配置从进程内 ConfigRegistry 下线，也只能从 Workflow/CropCycle 恢复原规则。相同规则版本却出现
不同 Canonical Hash 时拒绝提交并进入配置完整性告警。

#### Harvest

`HarvestWorkflow` 必须已经由 farm-svr Coordinator `CreateIfAbsent`，再调用 Farm Participant。
FarmActor 原子提交：

```text
CropCycle结束
+ owner_claimable标记已领取，并冻结最终数量
+ HarvestDecision {
    workflow_id
    crop_cycle_id
    reward_item_id
    reward_quantity
    rule_version / config_version
    decision_hash
  }
+ ParticipantReceipt(COMMIT_HARVEST)
+ PlotVersion
+ FarmVersion
+ FarmOpportunityHeader / opportunity_dirty_generation
+ 构造候选CommittedFarmChange(Harvest)
+ RecordVersion
```

`HarvestWorkflow` 随后使用稳定 `participant_invocation_id` 调用
`PlayerActor.GrantHarvestReward`，由 PlayerActor 原子增加 Inventory 并写 Participant/RewardReceipt。
Farm Participant 响应丢失时，同一调用命中 Receipt 并返回原 `HarvestDecision`。不再创建
`HarvestRewardOrder`、第二套 DueIndex 或其他能独立推进发奖的恢复锚点。

#### Care

```text
CareIncident未解决且trusted_now_ms >= due_at_ms
+ resolved_by_player_id / resolved_at_ms
+ CareResolutionSnapshot {
    care_reward_rule_version
    reward_exp
    friend_interaction_eligible
    resolution_hash
  }
+ CareRewardOrder
+ CareOperationReceipt
+ PlotVersion
+ FarmVersion
+ FarmOpportunityHeader / opportunity_dirty_generation
+ 构造候选CommittedFarmChange(CareResolved)
+ RecordVersion
```

CarePlan 的 `due_at` 来自 Plant 已冻结事实；奖励规则在首次 RESOLVED Turn 冻结。CareRewardOrder 和
后续 Player Participant 只使用 CareResolutionSnapshot，不得按投递时的最新配置重新计算奖励。
Care 的 StoredOperationOutcome 只保存稳定 `care_reward_order_id + reward_obligation_created`；
Order 当前 `PENDING/APPLIED` 状态属于查询视图，不能在后台投递完成后改写原终态 Outcome。

#### Theft

`TheftWorkflow` 必须已经由目标 farm-svr Coordinator `CreateIfAbsent`，再调用 Farm Participant。
FarmActor 首次裁决必须原子提交：

```text
stolen_amount增加
+ owner_claimable减少
+ TheftAttemptReceipt
+ TheftTargetDecision {
    workflow_id / settlement_id
    theft_attempt_id
    crop_cycle_id
    visitor_player_id
    guard_decision_snapshot {
      guard_snapshot_version / guard_rule_version
      random_algorithm_version / random_key_version / random_draw_bp
      guard_triggered / intercept_policy
      gross / withheld / net
      crop_unit_coin_value / owner_guard_coin_amount
    }
    visitor_reward_item_id
    visitor_net_crop_quantity
    owner_guard_coin_amount
    crop_rule_version
    decision_hash
  }
+ ParticipantReceipt(FARM_DECIDE)
+ FarmActivity
+ PlotVersion
+ FarmVersion
+ FarmOpportunityHeader / opportunity_dirty_generation
+ 构造候选CommittedFarmChange(Theft)
+ RecordVersion
```

完整 `TheftSettlement` 的 `FARM_DECIDED → VISITOR_SETTLED → OWNER_SETTLED → COMPLETED` 进度属于
Actor 外的 TheftWorkflow。FarmActor 不保存这套跨 Participant 可变进度，不创建访问者/主人奖励
Order、重复 Intent 或第二套 DueIndex。Workflow 使用稳定 Participant 身份分别调用访问者 PlayerActor
发放净作物、主人 PlayerActor 发放宠物金币。

宠物结果在 FarmActor 首次裁决时固定并写入 `TheftTargetDecision`；Participant 重试命中 Receipt 后
返回原 Decision，不能重新随机，也不能从偷菜者钱包扣钱。GuardSnapshot 使用裁决时 FarmActor 已经
生效的版本，而不是 Plant 时的版本；其后宠物喂养、切换或规则热更新只影响新的 TheftAttempt。

---

## 11. 30-08A：Order 可恢复发现

### 11.1 P0 冻结选择

P0 使用：

```text
没有上位独立Workflow的单向可靠交付（Care / GuardSync）
→ 来源状态
+ 来源同槽Order
+ 分桶DueIndex
→ 公司Redis同Slot Lua原子提交
```

不使用单一全局 DueIndex；也不把所有 PendingOrder 永久内嵌进 Actor 大记录。

逻辑布局示例，具体 Key 留给第 31 号：

```text
{farm-auth-37}:farm:1001
{farm-auth-37}:order:care-reward-abc
{farm-auth-37}:due
```

Authority Bucket 必须稳定，不随 Holder Pod 迁移。Bucket 数、Hash Tag 以及它与 VirtualShard 的物理
布局细节由第 31 号证明；但 `auth-v1` 的语义映射已经冻结为
`AuthorityPersistenceBucketID = VirtualShardID`，第 31 号不得重新引入第二套独立 Hash。

### 11.2 DirectDispatcher 与 Worker

```text
权威Lua明确提交
→ DirectDispatcher放入本地有界队列
→ 快速调用Participant
```

DirectDispatcher 丢失不影响正确性：

```text
farm-worker取得Bucket Lease
→ 读取due_at<=now的Order
→ 原子Claim
→ 调用Participant
→ Receipt防重
→ COMPLETED并移出DueIndex
或更新next_attempt_at重试
```

P0 不需要 MQ。未来引入 MQ 时，它也只能是 Order 提交后的分发通道，不能取代来源事务和 Receipt。

### 11.3 独立 Workflow

Plant、Harvest、Theft 等独立 Workflow 按稳定 WorkflowBucket 保存：

```text
{workflow-12}:workflow:plant-xyz
{workflow-12}:due
```

来源同槽 Order 跟随 AuthorityPersistenceBucket；独立 Workflow 跟随 WorkflowBucket。两者都不得随
Holder/Pod 变化重新 Hash。

### 11.4 Work Lease 与可信时间

Work Lease 只解决“多个 worker 谁来推进这一条恢复任务”，不授予 PlayerActor/FarmActor 写权，也
不能替代 Participant Receipt。它与 Shard Lease 是两种不同热路径：

```text
Shard Lease
→ Holder持有
→ 控制VirtualShard写入准入

Work Lease
→ farm-worker持有
→ 控制Workflow/Order恢复调度
```

Claim/Renew 只能传 `requested_ttl_ms`，不能传 worker 本机计算的绝对 `lease_until`。Repository 使用
Redis `TIME`（或平台等价可信时间）原子计算权威到期时间，并返回：

```text
owner_token
lease_epoch
work_version
accepted_ttl_ms
```

Worker 以“请求开始时的本地单调时间 + accepted_ttl_ms - safety_margin”建立保守 Deadline；超过 Deadline
不再发起新 Participant Step。WorkerRecoveryIngress 在消息进入 Actor Mailbox 前再次验证
`owner_token + lease_epoch + work_version`。Lease 过期可能造成重复调度，最终业务防重仍由
Participant Receipt 保证。完整内部 RPC 字段以第 29 号为唯一权威。

---

## 12. 30-09：Persistence Lane 与 Durability Barrier

### 12.1 为什么 Mailbox 串行仍不够

Actor Turn 可以串行，但异步存储请求可能乱序完成：

```text
SoftSave(v10)正在执行
→ 内存推进到v11
→ Barrier准备可靠提交v12
→ 如果v12先完成、v10后覆盖
→ 数据回退
```

P0 第一阶段关闭 SoftSave，最小 Lane 状态为：

```text
IDLE
DURABLE_SAVE_INFLIGHT
RESOLVING_UNKNOWN
DRAINING
```

只有在 R1 Feature Toggle 开启后才增加 `SOFT_SAVE_INFLIGHT`，并实现完整的版本水位、ForceFlush 和
Durability Barrier。无论是否开启 Soft，同 Lane 的 DurableSave、Claim 和 Unknown Resolve 都必须
串行。P0 每个 Actor 只有一个主要物理组，因此不能绕过 Lane 并发写同一 Actor 的其他 Key。

### 12.2 四级持久化

| 等级 | 成功语义 | 示例 |
|---|---|---|
| `R0_RUNTIME_ONLY` | 丢失后重建 | FarmSubscriptionHub 的 Presence、JoiningBuffer、Pending/Active Subscription、post-ready buffer |
| `R1_ACTOR_SOFT` | 内存先成功，允许已声明 RPO | 白名单内普通低风险玩家状态 |
| `R2_CRASH_DURABLE` | 存储明确提交后才成功 | Farm、Receipt、永久解锁、奖励领取 |
| `R3_WORKFLOW_DURABLE` | 持久 Workflow 锚点 + 各 Participant 的 R2 提交，可中断恢复 | Plant、Harvest 发奖、Theft、护理奖励 |

P0 第一阶段全部业务写使用 R2/R3。R1 通过 Feature Toggle 按命令白名单逐项启用，不能仅因为某个
字段叫“普通金币”就自动走 Soft。

R0 只说明这些运行态允许丢失后通过 Fresh Join 重建。其唯一语义权威是第 26 号文档；它们不属于
FarmActor、不会进入 ActorAuthorityStore，也不受 Actor RecordVersion/WriterClaim 的持久化管理。

R3 不是比 R2 更强的跨 Actor 原子事务。它表示：先可靠创建 Workflow/Order 锚点，每个 Participant
分别以 R2 原子提交“本地状态 + ParticipantReceipt”，再由 Coordinator/worker 推进到终态。中途
失败允许看到 `PROCESSING`，但最终可恢复，不能承诺多个 Actor 同时提交。

### 12.3 Barrier

```text
EnsureDurable {
  idempotency_key
  required_current_version
  current_dirty_state
  durable_mutation
  receipt
  optional_orders/outbox
}
```

Barrier 到达时：

```text
等待较早SoftSave结果明确
→ 若Unknown先解析
→ 捕获当前最新内存状态
→ 合并本次关键变化、Receipt和Intent
→ 一次可靠提交
```

例如 Soft Buy 后立即 Plant，Plant 的 ReserveSeed Barrier 必须把未落库购买结果、Reservation 和
ParticipantReceipt 一起提交，不能只保存 Reservation。

### 12.4 版本水位

Persistence Lane 至少跟踪：

```text
memory_state_version
persisted_through_version
durable_through_version
save_inflight_version
dirty_since
```

SoftSave(v10) 完成时，如果内存已到 v11，只能推进 `persisted_through_version=10` 并保持 Dirty；
不能把 v10 状态重新安装到 Actor 内存。

---

## 13. 30-10：ActorAuthorityStore 逻辑接口

### 13.1 接口

```go
type ActorAuthorityStore interface {
    LoadAndClaim(ctx context.Context, req LoadAndClaimRequest) (LoadAndClaimResult, error)
    Commit(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error)
    FenceAndClaim(ctx context.Context, req FenceAndClaimRequest) (FenceAndClaimResult, error)
    LookupIdempotencyOutcome(ctx context.Context, req IdempotencyLookupRequest) (IdempotencyLookupResult, error)
}
```

上层不得直接依赖 Redis Key、Lua 编号、连接对象或 Tcaplus API。

### 13.2 幂等预检读取

```text
IdempotencyLookupRequest {
  ActorAddress
  EffectiveIdempotencyKey
  canonical_payload_hash
}

IdempotencyLookupResult {
  status = FOUND_MATCH | NOT_FOUND | HASH_CONFLICT | RECOVERY_OVERDUE
  optional operation_result
  optional receipt_or_archive_ref
}
```

`FOUND_MATCH` 直接返回原结果；`HASH_CONFLICT` 拒绝；只有 `NOT_FOUND` 才允许重新做业务校验。
`RECOVERY_OVERDUE` 表示恢复证据超出在线窗口但不能证明“从未执行”，必须进入人工/后台对账，不能
当作 `NOT_FOUND` 自动重放。Store 内 Commit 仍再次按同 Key/Hash 检查 Receipt。

这是 Actor/Participant 执行前的内部幂等查询，不是第 28 号公网 `GET_OPERATION_RESULT`。映射冻结为：

| 内部事实 | 公网 OperationLookupStatus |
|---|---|
| 可证明终态存在 | `COMPLETED` |
| Workflow/Order/Unknown 正在恢复 | `PROCESSING` |
| `RECOVERY_OVERDUE` 但仍有恢复证据 | `PROCESSING`，同时告警/对账 |
| 当前没找到且无法证明已经过期 | `NOT_FOUND` |
| Tombstone/ExpiredEvidence 能证明结果不可恢复，且不存在未决 Work/Unknown/Receipt | `EXPIRED` |

因此 `RECOVERY_OVERDUE != EXPIRED`；恢复变慢不能成为客户端换 operation_id 重做业务的理由。

### 13.3 Commit 逻辑结构

```text
AuthorityCommitRequest {
  commit_request_id
  ActorAddress
  authority_layout_version
  optional asserted_authority_group

  ShardWriteFence
  expected_writer_claim_id
  expected_record_version

  EffectiveIdempotencyKey
  canonical_payload_hash

  candidate_state
  receipt
  operation_result
  attached_orders[]
  pending_outbox[]
}
```

Store 必须根据 `ActorAddress + authority_layout_version` 唯一派生 AuthorityPersistenceGroup 和物理
Bucket。`asserted_authority_group` 只用于防错断言：一致才继续，不一致返回
`AUTHORITY_ADDRESS_MISMATCH`，且不得访问调用方指定的错误 Slot。上层业务代码不能自由拼接 Group、
Bucket 或 Redis Hash Tag。

`candidate_state` 必须是 Actor 基于受信输入和当前权威状态计算的完整候选；Store 不重新解释公网
Payload，不调用另一个 Actor，也不发送 Gate 下行。

`commit_request_id` 标识一份不可变 AuthorityCommitRequest 的具体 Store 投递，用于日志、回调和
“明确未发送/已经发送”的边界。它不替代业务 `operation_id`/EffectiveIdempotencyKey，也不能用于业务防重。

复用规则：

| 情况 | commit_request_id |
|---|---|
| 完全相同且能证明尚未发送的请求做传输重试 | 保持原值 |
| 请求已经发送但响应未知 | 不重发 Commit，进入 Resolver |
| `RECORD_VERSION_CONFLICT` 后重载并生成新 Candidate | 生成新值 |
| candidate_state、expected version、Receipt、Order/Outbox 任一变化 | 生成新值 |
| 业务 operation_id / EffectiveIdempotencyKey | 始终保持原值 |

换言之，只有逻辑内容完全相同的 Store 请求才可复用 `commit_request_id`；任何重新业务校验、重算
Candidate 或改变预期版本的提交都必须使用新值。

### 13.4 重试边界

Adapter 允许在“明确尚未发送”时使用同一请求做有界重试。请求已经发送但结果未知时必须返回
`TIMEOUT_UNKNOWN`，禁止隐藏重试、生成新幂等键或切换 Claim。

读取可以按只读策略重试；写入不能使用只读重试规则。

### 13.5 回调 Fence

异步结果投递 Actor SystemQueue 时携带：

```text
actor_key
activation_generation
writer_claim_id
commit_request_id
```

Actor 必须在安装状态或完成 Future 前重新比较当前 Activation。旧回调只允许释放自己的资源，不能
完成新请求、安装状态或删除新 Actor。

### 13.6 有界记录与提交载荷

以下对象必须分别配置 item、byte 和 retention 上限：

```text
Actor权威状态
热Receipt / OperationResult索引
ResolutionReceipt
attached_orders / pending_outbox
单次AuthorityCommitRequest及其candidate_state
```

超过单次提交上限必须在进入 Store 前明确拒绝，不能依赖 Redis/Lua 超时来限流。终态 Receipt 可以按
第 27 号文档第 7.10 节整理或归档；未决 Workflow、Unknown Resolution 和恢复所需证据不能因普通 LRU/TTL 静默
消失。具体字节数、冷热布局和归档 Key 由第 31/32 号及压测冻结。

---

## 14. 30-11：失败矩阵与验收

### 14.1 核心不变量

```text
I1  任一VirtualShard同一时刻最多一个有效Holder写代次
I2  任一Actor Authority同一时刻最多一个有效WriterClaim
I3  低RecordVersion不能覆盖高RecordVersion
I4  业务效果与对应Receipt不能只成功一半
I5  来源事实与必要Order不能只成功一半
I6  TIMEOUT_UNKNOWN解析前，相关后续写不能越过
I7  旧Activation回调不能修改或删除新Activation
I8  旧Holder迟到请求不能在新WriterEpoch下提交
I9  已提交操作的重试必须先命中Receipt，不能被当前业务状态改判
I10 auth-v1的Shard写权、Actor状态及来源同槽Order位于同一Redis线性化域
I11 FenceAndClaim的结论由持久ResolutionReceipt固定，不能随后续写漂移
I12 任一强写必须完整匹配Holder/Boot/AssignmentRevision/WriterEpoch/LeaseEpoch且Lease有效
```

### 14.2 激活

1. 同一 Holder/Boot 内 100 个并发请求首次访问同一 Actor，只产生一次 LoadAndClaim 和一个本地
   Activation；其他 Holder 上的旧对象由 Epoch/Claim Fence 隔离；
2. 不同 ActorKey 可以并行加载；
3. Load 明确失败不留下半初始化 Entry；
4. Load Unknown 保留原 ActivationEntry/Future/claim_operation_id/new_writer_claim_id，不安装 Actor、
   不开放 UserMailbox；
5. Claim 响应丢失后使用原身份恢复，只安装一次 Activation，不生成 C3；
6. Epoch 换代使原 Entry Fenced，等待者 RouteStale，由新 Holder 重新激活；
7. 单个等待者超时不取消共享 Load；
8. Waiter、并发 Load、恢复 Deadline、Backoff 和加载速率上限生效。

### 14.3 CAS 与幂等

1. 同 operation + 同 Hash 重复提交，只产生一次效果并返回 `ALREADY_COMMITTED`；
2. 同 operation + 不同 Hash 返回 `IDEMPOTENCY_CONFLICT`；
3. 旧 RecordVersion 返回冲突且不盲目提交旧 Candidate；
4. 旧 WriterEpoch/Claim 均不能写；
5. Lease 过期但 Epoch 数字尚未变化时仍 Fail Closed；
6. 首次 Plant 已提交但响应丢失，重试时即使地块已非空，仍先返回原成功结果；
7. 同一 Claim 首次 RECORD_VERSION_CONFLICT 只允许重载并重算一次；第二次进入 RECONCILE_REQUIRED；
8. 并发请求同时通过 Actor 预检时，Store Receipt 再检查仍只允许一次业务效果；
9. 完全相同且明确未发送的请求可复用 commit_request_id；重算 Candidate/修改版本或附带工作后必须
   生成新值，但业务 EffectiveIdempotencyKey 保持不变；
10. AuthorityPersistenceGroup 断言与 ActorAddress 派生结果不一致时返回
    AUTHORITY_ADDRESS_MISMATCH，且不会访问错误 Slot；
11. 分别篡改 Holder、Boot、AssignmentRevision、WriterEpoch、LeaseEpoch，Store 都必须在任何状态、
    Receipt、Order 写入前拒绝；
12. 确定性 REJECTED/NO_CHANGE 提交只写 Receipt/Outcome 也必须幂等推进记录版本；业务条件随后变化时，
    同 operation_id 仍返回原终态，新 operation_id 才能重新裁决。

### 14.4 Unknown 顺序测试

必须可控制存储命令执行顺序：

```text
CAS先提交、响应丢失、Fence后执行
→ COMMITTED

Fence先执行、旧CAS迟到
→ NOT_COMMITTED + WRITER_CLAIM_MISMATCH

Fence响应也丢失
→ 使用同resolve_operation_id/C2读取持久ResolutionReceipt得到幂等结果

Fence完成后C2又提交其他业务
→ 重试原Fence仍返回原ResolutionReceipt，不按新状态改判

无Receipt但状态异常变化
→ RECONCILE_REQUIRED

E8旧写未知，E9换代先完成
→ 旧E8写永久被拒绝
→ 证据窗口完整且无Receipt时持久解析NOT_COMMITTED

E8旧写先提交，E9随后换代
→ 新Holder按原EffectiveIdempotencyKey/Hash读取Receipt并返回COMMITTED

跨Epoch恢复证据已过期
→ RECOVERY_OVERDUE / RECONCILE_REQUIRED，不映射公网EXPIRED
```

### 14.5 Handoff 与崩溃

1. Drain 期间不开始新写 Turn；
2. Epoch 切换前提交的写能被新 Holder 加载；
3. Epoch 切换后的旧写全部失败；
4. 当前 Holder 只能在完整 Fence 匹配时续租，不能自行改变 Holder/Boot/WriterEpoch/Revision；
5. 先切换 ShardWriteAuthority、待新 Holder Ready 后才发布 Assignment；反序发布测试必须失败；
6. Authority 已切换但 Snapshot 发布失败时只暂时不可用，不回退旧写权；
7. 旧 Assignment 只产生 Redirect，不产生双写；
8. Holder Lease 到期后本地 Self-Fence。

### 14.6 Passivation 资源收敛

重复 Activate/Passivate 后以下指标必须收敛：

- Actor 数；
- Mailbox/SystemQueue 数；
- Registry Entry 数；
- goroutine/run loop 数；
- Timer 和等待 Future 数。

旧 generation 的 Release 不能删除新 Activation；Dirty Flush Unknown 不能直接卸载。

### 14.7 Persistence Lane

构造：

```text
SoftSave(v10)延迟
→ 内存v11
→ Barrier提交v12
```

最终存储只能是 v12 或更高。SoftSave(v10) Unknown 时 Barrier 必须等待解析。

### 14.8 Workflow/Order 唯一锚点与故障注入

Plant/Harvest/Theft 必须先可靠创建唯一 Workflow，再调用 Participant。测试必须断言：

- Farm Participant 只保存本地 Decision、ParticipantReceipt 和 workflow_id 引用；
- Farm Participant 响应丢失后，相同 `participant_invocation_id` 返回原 Decision；
- 不存在同一 operation 的 HarvestRewardOrder、第二套 TheftSettlement 进度、重复奖励 Order 或重复
  DueIndex；
- Workflow/Participant 任意步骤重试都不会重新计算 Harvest 奖励或宠物随机结果。

Care/GuardSync 的来源同槽 Order 在以下切点杀进程：

- 来源事务执行前；
- Lua 原子事务中；
- Lua 成功但响应丢失；
- Lua 成功但 DirectDispatcher 投递前；
- Participant 成功但响应丢失；
- Order 标记完成前。

最终必须满足：

```text
来源事实不存在
→ Order不存在

来源事实存在
→ Order存在且DueIndex可发现

Participant重复调用
→ Receipt保证只生效一次
```

### 14.9 `auth-v1` 原子域与迁移

1. 任意 Actor 重算必须满足 `AuthorityPersistenceBucketID == VirtualShardID`；
2. PLAYER/FARM 相同数字 Shard 不共享 Hash Tag；
3. Lua 可在同一 Slot 原子校验 ShardWriteAuthority，并提交 Actor 状态、Receipt、Order 和 DueIndex；
4. Holder Pod 变化不改变 Key 或 Bucket；
5. 修改 HashVersion/HashSeed/ShardCount 的测试必须被拒绝，除非使用新持久化版本和迁移流程；
6. 第 31 号真实 Redis Cluster 探针不得出现 `CROSSSLOT`。

### 14.10 Work Lease、单组与容量

1. Worker 本机时钟前跳/后跳不改变 Repository 的 Work Lease 裁决；
2. `accepted_ttl_ms` 到达本地保守 Deadline 后不再开始新 Participant Step；
3. WorkerRecoveryIngress 拒绝旧 owner_token、lease_epoch 或 work_version；
4. P0 每个 Actor 的写均经过同一主要 Persistence Lane；
5. Receipt、ResolutionReceipt、Order/Outbox 和单次 Commit 超过 item/byte 上限时明确失败并告警；
6. 未决恢复锚点不会被普通 TTL/LRU 静默淘汰。

---

## 15. 可观测性

至少提供：

```text
actor_activation_inflight
actor_activation_waiters
actor_load_singleflight_join_total
actor_load_failure_total
actor_claim_unknown_count
actor_claim_unknown_duration
actor_claim_recovery_total{result}
actor_state_count{state}
actor_mailbox_depth

authority_commit_total{result}
authority_commit_latency
writer_epoch_mismatch_total
writer_claim_mismatch_total
record_version_conflict_total
record_version_reconcile_required_total
idempotency_precheck_total{result}

unknown_resolving_count
unknown_resolving_duration
unknown_reconcile_required_total
resolution_receipt_total{status}

persistence_lane_depth{lane}
soft_save_lag_versions
durability_barrier_latency

order_pending_count{bucket,type}
order_due_lag
order_retry_total
worker_claim_conflict_total
work_lease_expired_before_dispatch_total

shard_lease_renew_failure_total
shard_self_fence_total
shard_handoff_duration
shard_authority_assignment_revision_gap

authority_record_bytes{domain}
authority_hot_receipt_count{type}
authority_commit_payload_bytes{domain}
```

日志必须携带 ActorAddress、VirtualShard、WriterEpoch、WriterClaimID（可截断/Hash）、RecordVersion、
operation/workflow/order ID、ActivationGeneration 和 TraceID，禁止记录密码、Ticket、Cookie 或完整
玩家私密数据。

---

## 16. 稳定决策登记

```text
30-01  强写同时匹配完整ShardWriteFence、WriterClaimID、RecordVersion。

30-02  WriterEpoch属于VirtualShard，只在Holder写权换代时推进；存储必须验证当前Shard权威。

30-03  同一Holder/Boot内，同ActorKey并发首次请求由Singleflight合并LoadAndClaim，只产生一个本地
       Activation；这不是全局对象唯一性保证。

30-04  Actor在业务重验前执行IdempotencyPrecheck，Store在Commit内再次检查Receipt；明确区分
       COMMITTED、ALREADY_COMMITTED、IDEMPOTENCY_CONFLICT、RECORD_VERSION_CONFLICT、
       WRITER_EPOCH_MISMATCH、WRITER_CLAIM_MISMATCH、UNAVAILABLE_NOT_SENT和TIMEOUT_UNKNOWN。
       同Claim的RECORD_VERSION_CONFLICT最多重载重算一次。

30-05  TIMEOUT_UNKNOWN通过同线性化域FenceAndClaimActor解析，并原子持久化ResolutionReceipt；
       不能用一次GET、固定等待或随后状态重新计算原切点结果。

30-06  Redis ShardWriteAuthority是最终数据面写权Fence；RouteManager唯一协调Holder/Boot/Epoch/
       迁移状态换代，当前Holder只能在完整Fence匹配时续租。Lease到期Fail Closed，Assignment只在
       Authority切换和新Holder Ready后发布。DRAINING/ASSIGNING只属于控制面与Holder Runtime；
       Redis lease_state只允许UNASSIGNED/ACTIVE，且不存在两个可写ACTIVE Holder。

30-07  Passivation必须无Turn/无inflight/无Unknown/Dirty已可靠处理，并用ActivationGeneration释放。

30-08  权威状态、Receipt、必要Order/Outbox按来源合同原子提交；跨Actor状态不做分布式事务。一项
       可靠流程只有一个全局恢复锚点：Plant/Harvest/Theft使用预创建Workflow，Actor只提交本地
       Decision+ParticipantReceipt；Care/GuardSync使用来源同槽Order。

30-08A P0使用公司Redis同Slot Lua原子提交来源状态、独立Order和分桶DueIndex；DirectDispatcher
       加速，farm-worker恢复，不引入MQ。

30-09  P0每个Actor只有一个主要AuthorityPersistenceGroup、一个Actor WriterClaim、一个Group
       RecordVersion/CAS边界和一条Durable Persistence Lane；允许同槽多Key。Barrier不能被较低
       版本SoftSave覆盖；首版关闭Soft，启用R1后才增加SOFT_SAVE_INFLIGHT。

30-10  Actor仅依赖可替换ActorAuthorityStore；Adapter不得隐藏Unknown或生成新幂等身份。

30-11  正式Freeze前必须通过激活、CAS、Unknown、Handoff、Passivation、Lane和Order故障测试。

30-12  VirtualShard与AuthorityPersistenceBucket概念分离；P0 auth-v1冻结为同RouteDomain内
       BucketID=VirtualShardID。Holder迁移不搬数据，Hash布局变化必须新版本显式迁移。

30-13  Authority状态、热Receipt/Result、ResolutionReceipt、Order/Outbox与Commit Payload必须有界；
       未决恢复证据不得被普通TTL/LRU静默淘汰。

30-14  Work Lease使用Repository可信时间与accepted_ttl_ms，Worker使用本地单调保守Deadline；Lease
       只减少重复调度，Participant Receipt仍是最终业务防重。

30-15  不追求同一逻辑Actor全局物理对象唯一；迁移、暂停、分区和恢复时允许短暂多实例，但只有
       同时匹配ShardWriteAuthority、WriterEpoch、WriterClaimID、RecordVersion和Idempotency
       Receipt的强写可以提交。writer_claim_id适用于钝化重激活、同Holder换代、Unknown解析和
       迟到请求，不是热迁移专属机制。

30-16  LoadAndClaim TIMEOUT_UNKNOWN使用独立CLAIM_UNKNOWN激活状态：保留原ActivationEntry/Future/
       claim_operation_id/new_writer_claim_id，不安装Actor、不开放UserMailbox、不生成C3，直到原
       Claim幂等恢复为ACTIVE/ABSENT/FENCED/RECONCILE_REQUIRED。

30-17  R0 Presence/JoiningBuffer/Subscription Buffer属于FarmSubscriptionHub运行态，不属于FarmActor
       或ActorAuthorityStore；唯一语义权威是第26号文档。

30-18  AuthorityPersistenceGroup由ActorAddress+authority_layout_version唯一派生；内部幂等查询使用
       LookupIdempotencyOutcome并与公网GetOperationResult分离；commit_request_id仅在完全相同的
       Store请求传输重试中复用，新Candidate保留业务幂等键但生成新commit_request_id。

30-19  LoadAndClaim、Commit和FenceAndClaim统一携带Repository注入的ShardWriteFence；Store按可信时间
       原子校验Holder、Boot、AssignmentRevision、WriterEpoch、LeaseEpoch、ACTIVE与Lease有效性。
       RouteDomain/VirtualShard由ActorAddress和版本化Hash规则派生，客户端或业务Payload不能指定写槽。

30-20  IdempotencyPolicy=REQUIRED的直接Actor写在Execution-Accept后，确定性的SUCCEEDED、
       REJECTED+NONE、REJECTED+REFRESH_AUTHORITY_STATE和NO_CHANGE都保存Receipt与
       StoredOperationOutcome；临时错误、PROCESSING和TIMEOUT_UNKNOWN不得伪装成业务终态。
       同operation_id始终返回原终态，恢复Directive由服务端注册表重建而不持久化旧连接指令。

30-21  Plant的Crop/Steal/Care运行快照、Care的奖励快照和Theft的GuardDecision分别在其来源事实首次
       生效时与Authority状态、Receipt、必要Order、版本和FarmChange原子提交。恢复只读取已提交快照，
       不按当前配置补算成熟时间、槽位、护理奖励或宠物随机结果。

30-22  Friendship Pair、Quota、ShareIssuer是非Actor Authority，不使用Actor Lease、Mailbox、WriterClaim
       或ActorAuthorityStore接口；但其每次强写仍必须满足目标化版本/Fence、Root作用域幂等Receipt、
       来源Order同槽提交以及精确CommitAttempt级TIMEOUT_UNKNOWN解析，不得把“无Actor”解释为普通
       无条件Redis写。Resolver不得写operation级永久Tombstone阻止同一业务意图后续合法新Attempt。

30-23  持久业务幂等使用EffectiveIdempotencyKey并显式包含OperationRootRef。非Actor CommitAttemptID
       由Authority、CommitIdentity摘要和attempt ordinal确定性派生；AttemptCursor必须持久可发现，
       进程重启不得创建无法关联原Unknown的新随机Attempt。
```

---

## 16A. Friendship 非 Actor Authority 的等价 CAS/Unknown 合同

第 33 号的 Pair、Quota 和 ShareIssuer 不属于本文件的 `ActorAuthorityStore`，因此不取得
`ShardWriteFence/WriterClaimID`。它们仍需满足等价的强事实合同，但并不要求所有命令都用整条记录
版本制造无关冲突：

```text
FriendshipAuthorityCommit
条件：
  EffectiveIdempotencyKey的OperationRootRef等于当前Authority
  AuthorityReceipt不存在，或已存在且payload_hash相同
  本次CommitAttemptCursor仍指向同一OPEN attempt_id/candidate_hash
  完整记录替换：expected_record_version匹配
  目标化集合修改：对应relation_epoch/reservation_epoch/generation等领域Fence匹配

同一次原子提交：
  Authority事实
  record_version++
  AuthorityReceipt / StoredOperationOutcome
  CommitAttemptCursor = COMMITTED
  必要SOURCE_PERSISTENCE_BUCKET Work
  同桶DueIndex
```

### 16A.1 Business Operation 与 CommitAttempt 分离

一个业务 operation 可以因 `RECORD_VERSION_CONFLICT` 或明确未提交而重新加载、重验并产生新的 Store
Candidate。Unknown Resolver 必须只裁决“哪一次物理提交”，不能永久封死整个业务 operation：

```text
CommitIdentity
├── PUBLIC_OPERATION: EffectiveIdempotencyKey + payload_hash
└── WORKFLOW_PARTICIPANT: WorkAddress + participant_step + invocation_id + step_payload_hash

CommitAttemptCursor {
  commit_identity_digest
  current_attempt_ordinal
  current_attempt_id
  candidate_hash
  state                 // OPEN | COMMITTED | NOT_COMMITTED | RECONCILE_REQUIRED
  resolution_receipt_id?
}
```

`candidate_hash` 至少绑定 expected record/domain fences、规范 Mutation、Receipt/Outcome 和待创建
Order/DueIndex 身份。`attempt_id` 必须确定性派生：

```text
StableOpaqueID(
  "friend-commit-attempt-id-v1",
  source_authority_kind,
  canonical_source_authority_id,
  commit_identity_digest,
  attempt_ordinal)
```

相同 Candidate 的传输重试复用同一 attempt_id；只有上一个 Attempt 已明确 `NOT_COMMITTED`，调用方才可
递增 ordinal、重新业务校验并创建新 Candidate。Cursor 必须与 Authority 同槽持久化，使进程崩溃后仍能
通过 CommitIdentity 找回当前 Attempt；随机内存 ID 不满足恢复合同。

### 16A.2 精确 Attempt 的 Unknown Fence

非 Actor Authority 的 `TIMEOUT_UNKNOWN` 不能仅执行一次 GET 后宣布未提交，因为旧连接上的原 Lua 可能
迟到。Resolver 必须在同一 Authority Bucket 中执行线性化
`FenceAndResolveFriendCommitAttempt(current_attempt_id, candidate_hash)`：

```text
原Commit先执行
→ Authority事实、Receipt和Attempt=COMMITTED已原子提交
→ Resolver读取同一Receipt
→ 持久ResolutionReceipt(COMMITTED)

Resolver先执行
→ 确认Cursor仍指向该OPEN attempt且Authority仍满足before证据
→ 原子把该Attempt改为NOT_COMMITTED并写ResolutionReceipt
→ 迟到原Commit因Attempt已关闭而拒绝
→ 同一business operation仍可在重新验证后创建下一ordinal的新Attempt
```

若 Receipt 不存在但 Authority 已出现无法由 before 证据解释的变化，Resolver 固定
`RECONCILE_REQUIRED`，不得猜成未提交。`ResolutionReceipt` 绑定 attempt_id 和 candidate_hash；同一
resolve 请求重试必须返回原结论。

该 Fence 只针对一个精确 CommitAttempt 的 Unknown 解析，不是 Actor WriterEpoch，也不授予某 Pod
长期写权。Pair/Quota/ShareIssuer 的 Redis Hash Tag、Lua、AttemptCursor、ResolutionReceipt、TTL 和 GC
由好友权威物理合同定义。

## 17. 第 31 号文档的输入

第 31 号必须把本文逻辑合同映射为公司 Redis 的可执行物理设计，至少回答：

1. ShardWriteAuthority 与 ActorRecord 如何位于可原子校验的线性化域；
2. 按 `auth-v1: AuthorityPersistenceBucketID = VirtualShardID` 实现 PLAYER/FARM 独立 Hash Tag，
   并证明 ShardWriteAuthority、ActorRecord、Receipt、来源 Order/DueIndex 可在同一 Slot 原子处理；
3. Player/Farm/Workflow/Order/Receipt/DueIndex 的 Key 与 Slot；
4. `LoadAndClaim`、`Commit`、`FenceAndClaim`、Order Claim/Complete 的 Lua；
5. Redis TIME、Lease、TTL 和本地单调 Deadline；
6. Blob/Hash、RecordVersion、业务版本和物理行版本如何区分；
7. Lua 超时、连接断开、MOVED/ASK、主从切换如何映射到明确失败或 Unknown；
8. Bucket 数、ZSET 热点、记录大小、脚本时延与 3000 万 DAU 容量模型；
9. 公司 Redis 当前是否提供满足题目目标的持久化、复制、备份和容灾；
10. 如何从旧 `30-redis.md` 的缓存型 Key 平滑迁移，不复用其旧权威假设；
11. ResolutionReceipt 如何与 Claim 换代同提交、怎样按 resolve_operation_id 幂等读取；
12. Actor/Receipt/Order/Outbox/Commit Payload 的 item、byte、retention 与归档上限；
13. Work Lease 如何使用 Redis TIME、`accepted_ttl_ms` 和本地单调 Deadline。
14. 用同一 Lua 线性化域证明“对象可短暂多实例、有效写者唯一”：旧 Holder/Claim/Version 请求均
    无法越过 ShardWriteAuthority + WriterEpoch + WriterClaimID + RecordVersion + Receipt Fence。
15. `CLAIM_UNKNOWN` 如何使用原 `claim_operation_id + new_writer_claim_id` 幂等恢复，且不生成半 Actor
    或第三个 Claim。
16. RouteManager/当前 Holder 的字段更新权限，以及“先切 Authority、再 Ready、后发 Snapshot”的
    Assignment 发布协议。
17. Store 如何由 `ActorAddress + authority_layout_version` 派生 Group/Hash Tag，并拒绝错误断言。

在第 31 号完成前，实现可以编写内存 Fake 和接口级测试，但不能把临时 Redis Key/Lua 当作正式
生产合同。

---

## 18. 正式 Freeze 退出条件

本文保持 `CANDIDATE`，直到以下证据全部可重复：

1. 内存 Fake Store 单元测试；
2. 公司 Redis 原子脚本集成测试；
3. 多连接乱序、超时和响应丢失故障注入；
4. 双 Holder/WriterEpoch 竞争测试；
5. FenceAndClaim 顺序测试；
6. Activate/Passivate 资源收敛测试；
7. Persistence Lane 低版本覆盖测试；
8. Order + DueIndex 原子性与 Worker 恢复测试；
9. 路由、存储和 Actor 关键指标接入；
10. 幂等预检顺序、RECORD_VERSION_CONFLICT 有界重载和持久 ResolutionReceipt 测试；
11. `auth-v1` 同槽/CROSSSLOT、Holder 迁移不搬数据和版本化迁移测试；
12. Work Lease 可信时间、记录大小上限与未决证据保留测试；
13. 第 31 号物理合同通过评审。
14. Plant 的 Crop/Steal/Care 快照与 CropCycle、Receipt、版本和 FarmChange 原子提交测试；
15. CareResolutionSnapshot + CareRewardOrder + DueIndex 同槽原子性，以及奖励配置热更新后恢复测试；
16. Theft 目标扣减 + AttemptReceipt + GuardDecisionSnapshot + Decision + FarmChange 原子性，并验证
    TIMEOUT_UNKNOWN、WriterClaim换代和重放不会重新抽取宠物结果。

这些条件通过后，才允许将版本从 `freeze-candidate` 提升为正式 Freeze。
