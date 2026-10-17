# 34 · 任务系统合同

版本：`v1.3-pet-unlock-state-version-freeze`  
日期：2026-08-03  
状态：`STABLE_INPUT`  
定位：在第 26～33 号已冻结的 Session、Actor、Workflow、Redis 与核心场景之上，定义 P0
简化任务系统的任务分类、进度权威、触发时机、每日周期、领奖幂等和红点边界。本文不重新
定义 Plant、Harvest、Care、Theft 或 Join 的成功语义，只消费它们已确认的业务事实。

## 0. 上位合同与不重复定义

- [26 · 接入与会话](./26-access-session-contract.md)：登录、Join、SubscriptionReady 与在线状态；
- [27 · 路由、Actor 与 Workflow](./27-access-routing-actor-workflow-freeze.md)：PlayerActor、Order、
  DirectDispatcher 与 farm-worker；
- [28 · CommandCatalog 与公网协议](./28-command-catalog-public-protocol.md)：`GET_TASK_LIST`、
  `CLAIM_TASK`、`CONFIRM_FRIEND_VISIT` 的 Envelope、CommandSpec、结果与重试语义；
- [29 · 内部 RPC](./29-internal-rpc-trusted-context.md)：Participant 身份、InvocationID 与 Receipt；
- [30 · Actor 权威 Store](./30-actor-authority-store-contract.md)：Authority Commit、幂等、Unknown 与
  Persistence Lane；
- [31 · Redis 物理合同](./31-actor-authority-redis-physical-contract.md)：Authority/Runtime/Projection 分类、
  DueIndex、红点查询副本和容量约束；
- [32 · 核心经营场景](./32-core-business-scenario-contract.md)：Harvest、Care、Theft 等任务事件的
  真实成功边界；
- [33 · 好友、分享与权限](./33-friendship-share-access-contract.md)：好友访问和好友互动的权限
  前置条件。

冲突时服从上位合同。任务系统不能为了推进进度，把已被核心场景拒绝或 `NO_CHANGE`
的操作重新解释成成功。

---

## 1. 已确认产品输入

### 1.1 每日任务

每天 0 点进入新周期。P0 至少包含：

1. 当日登录一次；
2. 当日访问好友农场一次；
3. 当日与好友农场有效互动一次，偷菜或护理任一成功即可。

三项均为当日首次达成后可领奖，不要求继续累计多次。“访问”和“互动”是两条独立任务；
仅进入好友农场不能完成互动任务。

### 1.2 成长任务

P0 保留简单的累计/里程碑任务，例如：

- 达到指定等级；
- 累计成功收获指定次数。

成长任务不在每日 0 点清空。P0 只配置一条线性成长任务链，界面同时只展示当前一个成长任务；
领取后推进到下一步，并使用永久累计指标立即判断下一任务是否已经满足，不提前物化和展示整条链。

### 1.3 临时活动任务

保留配置化的活动任务扩展点，但 P0 不实现复杂任务链、任务前置图、通行证或全服实时
排名。活动任务的开始、结束和配置版本必须使用可信服务端时间。

---

## 2. 已继承的领域边界

第 27、32 号已经给出不得反转的上层边界：

```text
player-svr
├── PlayerActor(player_id)
│   └── TaskDomain              # 任务进度、周期、领取Receipt权威
├── TaskConfigRepository           # 版本化只读配置
└── TaskReadAPI                    # 完整列表进入PlayerActor；入口摘要可读查询副本

farm-worker
└── 只恢复既有Harvest/Care/Theft Workflow或Order；P0没有任务专用Consumer
```

P0 不创建 `TaskActor`、`task-svr` 或任务专用 MQ。任务是 PlayerActor 内的一个领域模块，不是
第五个独立业务 Deployment。红点是从任务权威状态产生的可重建查询副本，不是任务完成的
真值。

### 2.1 Q34-01 冻结：任务权威、配置与部署边界

任务进度按 `player_id` 归属 `PlayerActor.TaskDomain`。它至少拥有：

- 每日任务当前周期与进度；
- 成长任务累计进度；
- 任务完成、可领取和已领取状态；
- 防止重复推进的任务进度 Receipt；
- 防止重复领奖的 Claim Receipt。

任务配置由 `player-svr` 内的 `TaskConfigRepository` 以只读、版本化方式提供；配置不是
`PlayerActor` 的可变权威状态。完整任务列表和详情由 `TaskReadAPI` 进入 PlayerActor 后返回；只有入口
`TaskSummary/claimable_count` 与红点允许使用可重建查询副本。查询副本不能参与任务完成、周期切换
或领奖裁决。

任务事实按来源分为三条路径：

```text
PlayerActor 内已经可靠成立的业务事实
→ 在同一个 Actor Turn 更新 TaskDomain
→ 与玩家权威状态一同提交

其他 Actor 内已经可靠成立的 Harvest/Care/Theft 事实
→ 复用已有 Workflow Participant 或 CareRewardOrder
→ 既有 Dispatcher/Worker 继续推进原 Participant
→ PlayerActor 在奖励/结算同一次提交中幂等更新 TaskDomain

好友访问事实
→ H5 收到 SUBSCRIPTION_READY 后自动发送 CONFIRM_FRIEND_VISIT
→ Gate 从当前 ActiveSubscription 生成好友访问可信证明
→ PlayerActor 原子确认本日访问任务
```

`farm-worker` 只恢复既有业务 Workflow/Order，不拥有任务进度，也不能绕过 PlayerActor 直接改任务
权威数据。P0 不增加通用 `TaskProgressIntent/TaskProgressOrder`、任务专用 DueIndex 或任务 MQ；
未来只有当新增事件无法复用现有可靠 Participant，且事件量或发布节奏需要独立扩缩容时，才评估
增加 Typed Task Work 或拆出 TaskConsumer，但任务进度和领奖的最终提交仍回到 PlayerActor。

### 2.2 Q34-02 冻结：每日周期与惰性换期

每日任务使用服务端可信时间和固定游戏时区派生周期：

```text
game_timezone = Asia/Shanghai
daily_period_id = YYYYMMDD(trusted_now_ms, game_timezone)
```

服务端不在每天 0 点扫描和改写全部玩家。`PlayerActor` 在处理登录任务事件、任务查询、任务
进度事件或领奖前统一执行 `EnsureDailyPeriod`：

```text
current_period_id = DailyPeriod(trusted_now_ms)

current_period_id == TaskDomain.daily_period_id
→ 继续处理当前周期

current_period_id != TaskDomain.daily_period_id
→ 上一周期未领取的每日奖励过期
→ 丢弃上一周期每日进度和可领取状态
→ 按当前配置初始化 current_period_id
→ 再处理本次请求或事件
```

这是一种惰性换期：玩家几天未登录时直接进入当前周期，不为中间日期补建任务；玩家一直在线
跨过 0 点时，也不要求重新登录，下一次任务查询、任务事件或领奖同样会触发换期。服务端可以在
任务响应中返回 `next_reset_at_ms`，H5 到点后主动刷新任务界面，但客户端时间只用于 UI 调度，
不能决定任务周期。

跨零点延迟送达的既有 Participant 任务事实必须携带来源权威提交时捕获的 `occurred_at_ms`。事件只
属于其真实发生周期，不能因为投递延迟而计入第二天；若送达时该周期已经过期，则幂等确认后忽略，
不污染新周期。P0 不提供跨日补领宽限期。成长任务不参与每日换期。

零点削峰必须遵守以下合同：

1. 不创建全局“当日任务”热点 Key，不在所有玩家 Key 上设置相同的零点过期时间；
2. 新周期初始化与本次首个任务进度合并为一次 PlayerActor Authority Commit，禁止先重置再单独写进度；
3. 不在零点向全部在线玩家广播任务重置；任务响应返回 `next_reset_at_ms`，H5 后台自动刷新使用
   `hash(player_id, next_period_id) % refresh_jitter_window_ms` 的稳定抖动，P0 默认窗口为 60 秒；
4. 玩家主动打开任务页面时立即查询，不受刷新抖动限制；
5. 任务列表和红点加载不是登录、建连或进入自己农场的成功屏障，失败时局部显示 Loading 并重试；
6. `TaskConfigRepository` 使用进程内版本化缓存，禁止每名玩家换期时回源读取配置存储。

上述设计只消除系统主动制造的零点洪峰；真实用户同时登录产生的自然峰值仍需通过 Player Route
Domain、Redis 分片、预扩容和专项压测承载。

### 2.3 Q34-03 冻结：任务事件的权威成功点

任务进度只能消费已经成立的业务事实，不能在收到客户端点击或业务请求时提前推进：

| 任务事件 | 权威成功点 | 不推进的情况 |
|---|---|---|
| 每日登录 | 本周期首次进入受信 PlayerActor Turn（如 PlayerBootstrap、任务查询或玩家业务命令），PlayerActor 原子提交登录标记 | 仅完成 HTTP 密码校验、仅签发 Ticket、仅建立 WSS/ClientHello |
| 访问好友农场 | `SUBSCRIPTION_READY` 后自动发送的 `CONFIRM_FRIEND_VISIT` 被 PlayerActor 可靠提交；可信上下文证明当前是已授权好友 Subscription | Join/Ready 失败、确认命令未接纳、自己农场或缺少好友关系 Fence |
| 好友护理 | FarmActor 权威提交使一个有效护理事件发生真实状态变化 | 重复护理、`NO_CHANGE`、无权限、事件已结束 |
| 好友偷菜 | FarmActor 接受合法偷菜尝试并提交 `TheftAttemptReceipt` | 未成熟、已经偷过、无可偷份额、非好友或其他拒绝 |
| 成功收获 | 一个 `CropCycle` 被 FarmActor 成功结束 | 未成熟、重复收获、失败或 `NO_CHANGE` |
| 达到等级 | PlayerActor 可靠提交后的 `level_after` 达到配置门槛 | 客户端动画或尚未提交的经验变化 |

访问好友和好友互动是两条独立任务。Ready 后 H5 立即后台确认访问，不要求玩家点击额外按钮；之后
成功护理或合法偷菜才完成互动。好友 Care/Theft Participant 已经证明发生过合法访问，因此当本日
访问任务尚未满足时，可以在同一次 PlayerActor 提交中同时满足“访问”和“互动”，但两项仍各自
领奖。玩家持续在线跨零点时，本周期第一次受信 PlayerActor Turn 可以产生登录标记，不要求重新
输入密码或重新建连。拒绝在 Gate/FirstHop 之前的请求不能产生登录标记。

收获累计按成功结束的 `CropCycle` 数量计算，而不是按客户端命令数计算；批量收获成功结束五个
周期时进度增加五。等级任务是状态型任务：配置出现时玩家已经满足等级门槛，也应根据当前权威
等级完成，不要求再次升级。

宠物即使截留本次全部可得作物，只要 FarmActor 已经接受合法偷菜尝试、提交 Attempt Receipt 且
消耗了本茬机会，仍算完成一次好友互动；宠物只影响最终净获得数量，不否定互动事实。

### 2.4 Q34-04 冻结：直接更新与跨 Actor 进度分界

任务进度优先复用已经存在的 PlayerActor Turn 或跨 Actor Participant，不为每种业务事实重复创建
一条任务专用消息：

```text
PlayerBootstrap、GET_TASK_LIST、CLAIM_TASK等受信PlayerActor Turn，以及等级变化
→ PlayerActor 同一 Turn 直接更新 TaskDomain

HarvestWorkflow.GRANT_HARVEST_REWARD
→ PlayerActor 同一次Authority Commit提交库存/经验、HarvestCounter、累计收获进度和ParticipantReceipt

CareRewardOrder.APPLY_CARE_REWARD
→ care_context=FRIEND时，同一次Authority Commit提交奖励、好友访问/互动进度和ParticipantReceipt
→ care_context=OWNER时不推进好友访问或好友互动任务

TheftWorkflow.SETTLE_THEFT_VISITOR
→ PlayerActor 同一次Authority Commit提交净偷取作物、好友访问/互动进度、TheftVisitorReceipt和ParticipantReceipt

FriendVisit
→ H5收到SUBSCRIPTION_READY后持久化并自动发送CONFIRM_FRIEND_VISIT
→ Gate校验当前ActiveSubscription、目标农场和authorization relation fence
→ PlayerActor以Actor Direct命令可靠提交TaskDomain与StoredOperationOutcome
```

宠物截留全部作物时，`SETTLE_THEFT_VISITOR` 仍携带 `valid_interaction=true` 和
`net_stolen_quantity=0`，因此互动任务可以完成而仓库不增加作物。

来源服务、FarmActor、FarmSubscriptionHub、Gate 和 farm-worker 都不能直接改 `TaskDomain`。Hub 只
建立 Subscription；Gate 只从当前受信 Session 生成不可由 H5 伪造的访问上下文；最终进度必须进入
PlayerActor 邮箱并在其单一写权下裁决。`CONFIRM_FRIEND_VISIT` 不阻塞 Join Ready，也不建立独立
Task Work；一旦命令进入 PlayerActor Mailbox，结果按普通可恢复写命令保存。

### 2.5 Q34-05 冻结：防重、乱序、重放与写放大

P0 不保存全量玩家行为流水。任务系统只消费必要的权威事实，并只在任务状态真实变化时更新：

```text
任务状态没有变化
→ 不增加 task_state_version
→ 不产生新的任务红点 generation
→ 除 REQUIRED公网命令自身必须保存的OperationOutcome外，不为任务额外写Redis

任务首次完成或累计进度真实增加
→ 才更新 TaskDomain
→ 与来源 ParticipantReceipt/TaskReceipt 一次 Authority Commit
```

防重优先复用现有来源 Receipt：Harvest 使用 `GRANT_HARVEST_REWARD` 的 Participant/Reward Receipt，
Care 使用 `ParticipantReceipt(APPLY_CARE_REWARD)` 与 `CareRewardReceipt`，Theft 使用
`TheftVisitorReceipt`。同一 `InvocationID/source_receipt_id`
重放时返回原结果，不能重复修改资产或任务进度。`event_id` 相同但 `payload_hash` 不同必须返回
`IDEMPOTENCY_CONFLICT` 并告警。

必须区分两种防重：

```text
来源事件幂等
→ 同一个operation_id或Participant Invocation重放时，payload hash必须一致
→ 不一致是IDEMPOTENCY_CONFLICT

一次性任务满足
→ satisfaction_key = player_id + task_instance_id
→ 第一个合法witness使IN_PROGRESS变为CLAIMABLE
→ 后续来自另一好友、Care或Theft的合法witness返回NO_CHANGE/ALREADY_SATISFIED
→ witness不同不是IDEMPOTENCY_CONFLICT
```

登录任务复用本来就会进入 PlayerActor 的受信 Turn；当前周期已完成时只做内存判断。Care/Theft 的
互动标记放进原有 Reward/Settlement Participant，不建立任务消息。`CONFIRM_FRIEND_VISIT` 使用
Envelope 中普通 `operation_id` 防重，不创建按日确定性 Order；同日第二次合法确认使用新的
operation_id 时返回持久 `NO_CHANGE/ALREADY_SATISFIED`，TaskDomain 不变但该新 operation 的终态仍
保存，以便响应丢失后查询。

H5 收到某周期最新 `TaskSummary` 已表明好友访问任务满足后，该周期后续 Ready 不再发送确认；若
Summary 尚未加载或可能过期，则允许发送一次，由 PlayerActor 最终裁决。该客户端/Session 提示只
用于削减无效命令，不能替代 TaskDomain 真值。

不同任务采用单调更新：一次性任务只允许 `false → true`，累计任务按唯一来源增量，等级任务使用
`max(progress, level_after)`。因此乱序不能回退状态。跨零点延迟事件按每个任务分别判断：过期的
每日部分记录为 `IGNORED_EXPIRED` 后不计入新一天，累计收获等非周期进度仍正常应用。忽略结果也
必须被幂等确认，使 Dispatcher/Worker 可以结束重试。

一次 PlayerActor 提交必须原子包含任务状态变化与所复用的 Receipt。Receipt 只保留到覆盖
Workflow/Order 最长重试与 Unknown 解析窗口，不无限增长；具体压缩和墓碑规则继承第 30、31 号。

### 2.6 Q34-06 冻结：领奖与资产原子提交

P0 公网命令一次只领取一个任务：

```text
ClaimTaskRequest {
    task_instance_id
}
```

`operation_id` 只存在于第 28 号 `ClientCommandEnvelope`，不得在 Typed Payload 中重复。Claim 的
Canonical Payload Hash 只覆盖稳定 `task_instance_id`；幂等键是
`{session.player_id, CLAIM_TASK, envelope.operation_id}`。

客户端不提交奖励内容。服务端根据任务实例绑定的配置版本读取冻结的 `RewardBundle`。每日任务实例
至少包含 `task_id + daily_period_id + config_version`；成长任务实例包含
`task_id + activation/config_version`，旧周期实例不能领取新周期奖励。

P0 任务奖励中的宠物解锁使用规范化结构：

```text
PetUnlockGrant {
    pet_id
    pet_config_version
    grant_definition_version
}
```

TaskConfig 发布时必须验证宠物定义存在、允许任务解锁、整个 PetCatalog 不超过 32 项，且 RewardBundle
序列化大小有界；不能等玩家领奖时才发现 grant 指向无效配置。

`ClaimTask` 全部在 PlayerActor 一个 Turn 内完成，不创建跨 Actor Workflow：

```text
EnsureDailyPeriod
→ 校验任务实例、完成条件和未领取状态
→ 发放普通金币/种子/普通道具/经验/PetUnlockGrant
→ TaskState = CLAIMED
→ 写 ClaimReceipt 与 StoredOperationOutcome
→ 一次 crash-durable Authority Commit
→ 提交确认后返回 SUCCEEDED
```

PlayerActor 在 Turn 开始时只捕获一次 `trusted_now_ms`，并用该时间完成换期、过期与领取窗口校验。
若命令在截止前通过校验并越过 Execution-Accept，后续 Redis 提交稍晚于 0 点或活动截止时间，不得
重新按新时间把同一操作改判为过期；重试只读取原 Receipt/Outcome。

任务状态、Wallet、Inventory、Experience/Level、PetState、ClaimReceipt 和 OperationResult 必须处于
同一个 Player 权威原子提交中。`PetUnlockGrant` 只把宠物加入有界已解锁集合；实际新增时
`PetState.pet_state_version++`，但不推进 `guard_version`、不自动产生守护效果，也不创建
GuardSyncOrder。重复解锁是集合 NoChange，不推进宠物版本，任务本身仍可正常领取。相同
`operation_id + task_instance_id` 返回原结果；相同 operation_id 搭配
不同任务实例返回 `IDEMPOTENCY_CONFLICT`；不同 operation_id 重领同一实例返回
`NO_CHANGE/TASK_ALREADY_CLAIMED`，不得再次发奖。提交超时未知时返回 `PROCESSING`，客户端只能用
原 operation_id 查询或重试。

若运行时发现 PetUnlockGrant 配置缺失、Hash 不匹配或集合容量合同被破坏，必须在任何金币、道具、经验、
PetState 或 TaskState 写入前 Fail Closed 并告警；禁止部分发放其余奖励后单独跳过宠物。

P0 奖励只包含 PlayerActor 权威资产和 PetState 宠物解锁，不直接修改 FarmActor 地块或土地解锁状态。以后需要土地类奖励时
优先发放仓库中的解锁券，再通过独立使用命令处理。经验奖励导致升级时，可以在同一 Turn 更新等级
里程碑任务为可领取，但不递归自动领奖。P0 不实现一键领取全部；未来批量领取仍必须在单个
PlayerActor 内设置任务数量和奖励总量上限。

领奖终态映射冻结为：

| 条件 | CommandStatus | RetryDirective | 是否保存当前 operation 终态 |
|---|---|---|---:|
| 成功领取 | `SUCCEEDED` | `NONE` | 是 |
| 不同 operation 重领已领取实例 | `NO_CHANGE/TASK_ALREADY_CLAIMED` | `NONE` | 是 |
| 任务尚未完成 | `REJECTED/TASK_NOT_COMPLETED` | `NONE` | 是 |
| 每日实例已跨期或活动领取期已过 | `REJECTED/TASK_INSTANCE_EXPIRED` | `REFRESH_AUTHORITY_STATE` | 是 |
| 实例不存在或不属于当前玩家 | `REJECTED/TASK_INSTANCE_NOT_FOUND` | `NONE` | 是 |
| 同 operation 改换 task_instance_id | `REJECTED/IDEMPOTENCY_CONFLICT` | `NONE` | 原 operation 既有证据为准 |
| Authority Commit 结果未知 | `PROCESSING` | `POLL_OPERATION_RESULT` | 等 Unknown 解析 |
| Mailbox 尚未接纳 | `REJECTED` | `RETRY_SAME_OPERATION` | 否，仍在 Execution-Accept 前 |

### 2.7 Q34-07 冻结：配置版本、在途任务与活动过期

任务实例创建后必须绑定不可变配置版本，目标、事件类型和奖励不能在进行途中被最新配置覆盖。

每日任务按日期选择唯一任务集，配置发布端必须先创建全局不可变映射：

```text
DailyTaskSetAssignment {
    period_id
    config_version
    effective_at_ms
    config_payload_hash
}
```

Assignment 是配置发布面的不可变全局记录，由 `TaskConfigRepository` 批量加载并做进程内版本化缓存；
它不是每玩家 Redis Key，也不需要 TaskActor。具体使用公司配置中心、版本化文件还是配置表属于部署
适配，但必须满足同一 `period_id` 只有一个已发布 hash 的语义。

同一 `period_id` 的所有玩家使用相同版本；当天发布的新每日配置默认从指定的下一
`effective_period_id` 生效，禁止按“玩家首次登录时的最新版本”选择，以免同一天出现不同规则。
Pod 读取 Assignment 失败但已缓存该 period 的相同映射时可继续；从未见过当前 period 的 Pod 必须让
任务查询/进度局部 Fail Closed 或 Loading，不能自行选择“本机最新版本”。PlayerActor 第一次创建本日
实例时持久保存 `daily_period_id + daily_config_version`，本周期内不漂移。

成长任务的 `{task_id, definition_version}` 一经发布不可原地修改；变更目标或奖励必须发布新版本。
已激活实例继续使用旧版本，新实例使用新版本。PlayerActor 维护 `total_harvest_count`、
`highest_level` 等少量永久累计指标，因此新成长任务发布时，已满足门槛的玩家可以在惰性装载时直接
变为可领取，不要求重新累计。

临时活动任务使用：

```text
start_at_ms
progress_end_at_ms
claim_end_at_ms
config_version
```

开始前与进度截止后的新行为不计入；活动窗口内发生但延迟送达的可靠事件按来源
`occurred_at_ms` 计算。完成奖励只可在 `claim_end_at_ms` 前领取，之后变为 `EXPIRED`。P0 默认
`claim_end_at_ms = progress_end_at_ms`，暂不提供活动结束后的补领宽限期，但保留独立字段。

活动开始和结束都不得扫描全部玩家。PlayerActor 在登录、打开任务页或收到相关事件时惰性创建、
完成或过期任务实例。`TaskConfigRepository` 以整份快照原子加载，校验失败继续使用上一份有效配置，
一次请求不得混用版本；旧配置至少保留到全部关联实例的领取期限和 Receipt 查询期结束。任务实例
保存 `task_id`、`definition_version`、`reward_hash`、进度和状态，领奖严格读取其绑定版本。

### 2.8 Q34-08 冻结：任务列表、连续成长任务、红点与降级验收

任务实例状态机为：

```text
IN_PROGRESS → CLAIMABLE → CLAIMED
IN_PROGRESS / CLAIMABLE → EXPIRED
```

`CLAIMED` 不可回退。每日任务跨零点生成新周期实例；活动任务超过领取截止时间后过期；成长任务
默认不失效。

P0 任务界面固定为：

```text
3 个每日任务
+
1 个当前成长任务
```

成长任务使用线性 `growth_cursor`，不实现任务 DAG，也不同时物化全部后续任务：

```text
领取当前成长任务
→ 同一 PlayerActor Turn 发奖并推进 growth_cursor
→ 激活下一任务
→ 使用 total_harvest_count / highest_level 等永久指标立即求值
→ 已满足则下一任务立即为 CLAIMABLE
→ 返回 next_growth_task 与最新 TaskSummary
```

下一任务即使已经满足，也不自动递归领奖；客户端展示新的领取按钮，玩家使用新的 operation_id 再次
领取。成长链结束后不再展示成长任务。临时活动任务仅保留扩展合同，不进入 P0 的四项固定展示区。

完整列表始终通过 `player-svr/TaskReadAPI` 读取 PlayerActor 权威状态：

```text
H5 → farm-gate → player-svr/TaskReadAPI → PlayerActor
    → EnsureDailyPeriod → 对齐配置版本 → TaskListView
```

查询只要求有效认证与恢复完成，不要求当前 Farm Subscription。`TaskListView` 至少返回
`daily_period_id`、`next_reset_at_ms`、`task_state_version`、`claimable_count`、三个每日任务和当前
成长任务。因为 P0 最多四项，不分页。登录 Bootstrap 仅并行读取轻量 `TaskSummary`；完整任务列表
在玩家打开入口时加载，失败时局部显示 Loading，不阻塞登录或进入农场。

P0 逻辑状态冻结为：

```text
TaskDomainState {
  daily_period_id
  daily_config_version
  daily_tasks[3]
  growth_cursor
  current_growth_task?
  total_harvest_count
  highest_level
  task_state_version
  claimable_count
}

TaskInstance {
  task_instance_id
  task_id
  category
  definition_version
  period_id? / activity_id?
  target
  progress
  state
  reward_hash
  progress_end_at_ms? / claim_end_at_ms?
  first_witness_id?
}
```

同一个 Authority Commit 无论改变一个还是多个实例，`task_state_version` 只增加一次；
`claimable_count` 必须等于提交后实际 `CLAIMABLE` 数量。Receipt 使用第 30/31 号统一的有界
Operation/Participant Receipt，不在 `model:task` 内复制第二份无界事件历史。

红点权威定义为 `claimable_count > 0`，真值位于 PlayerActor.TaskDomain。Projection Redis 只保存：

```text
TaskRedPointProjection {
    player_id
    daily_period_id
    task_state_version
    claimable_count
}
```

只有 TaskDomain 状态真实变化时才推进 `task_state_version`。任务红点属于可重建 Projection，P0 不为
每次任务变化创建必达 `TaskSummaryChangedOrder`，也不让投影容量反压领奖或任务进度提交。提交后由
进程内 Projection Adapter 最佳努力按版本 CAS 更新；`GET_TASK_LIST`、PlayerBootstrap 或后续
PlayerActor Turn 发现投影落后时可再次刷新。在线 Push 是最佳努力，Pull 可以修复；投影缺失、跨周期
或版本落后不能参与完成/领奖裁决。任务查询和领奖响应直接携带最新 TaskSummary，使 H5 立即更新
红点，不等待投影。

故障降级规则：配置读取失败时，只有最后有效快照包含当前 `DailyTaskSetAssignment` 才能继续使用；
本 Pod 从未见过当前 period 时局部 Loading/Fail Closed。红点投影不可用时暂不显示红点；任务列表失败时局部
Loading/重试；跨 Actor 投递延迟时由 Worker 恢复；ClaimTask 超时未知返回 `PROCESSING`；PlayerActor
权威状态不可用时禁止根据投影发奖。

P0 验收至少覆盖：首次登录与重复登录、严格零点惰性换期、Ready 后访问确认的 Subscription/Relation
Fence、同日多好友确认与 Care/Theft 多 witness 竞争、有效护理/偷菜与
`NO_CHANGE` 分界、宠物全截留、批量收获累计、Worker 重放、领奖 Unknown/重复、成长链连续激活、
配置 Assignment 跨 Pod 不漂移、红点 Push/Pull 修复，以及零点无统一 TTL、全局 Key、全服扫描或
集中初始化。

---

## 3. 冻结决策索引

| 编号 | 问题 | 状态 |
|---|---|---|
| Q34-01 | TaskDomain 权威、配置和部署边界 | **已冻结**：PlayerActor 内 TaskDomain；不拆 TaskActor/task-svr |
| Q34-02 | 每日 0 点的 period_id、时区与惰性换期 | **已冻结**：Asia/Shanghai、严格零点、惰性换期、不跨日补领 |
| Q34-03 | 登录、好友访问、好友互动、收获和等级事件的精确成功点 | **已冻结**：只消费权威成功事实；好友访问以Ready后的可靠确认提交为准 |
| Q34-04 | 同 Actor 直接更新与跨 Actor 进度分界 | **已冻结**：同 Turn优先、复用既有Participant；不建任务专用Work |
| Q34-05 | 任务进度幂等、Receipt、乱序与重放 | **已冻结**：来源防重与一次性任务满足分离、无行为流水 |
| Q34-06 | ClaimTask 的资产发放、领奖Receipt与重复领取 | **已冻结**：单任务、Player内原子发奖、超时查原操作 |
| Q34-07 | 配置版本、在途任务和活动过期 | **已冻结**：DailyTaskSetAssignment统一周期版本、实例绑版本、活动惰性生命周期 |
| Q34-08 | 任务列表、红点、失败降级与最小验收 | **已冻结**：3每日+1成长、权威列表、无必达投影Order的可重建红点 |

Q34-01～Q34-08 已全部冻结。本文成为任务系统的现行稳定输入；第 28、29、31 号已经分别登记对应
公网命令、可信访问证明与 Redis 物理边界。最终 Proto 字段号和压测阈值仍不得由实现 Agent 从本文
自行补全，应由唯一 Proto 生成源和实现探针确定。
