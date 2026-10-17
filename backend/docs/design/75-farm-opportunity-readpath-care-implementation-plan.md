# 75 · 好友互动机会 Header、护理与偷菜 Receipt 实施及迁移计划

版本：`v1.4-i3a-implemented`

状态：`STABLE_INPUT / I0-I3A_IMPLEMENTED / I3B-I5_PENDING`

日期：2026-08-08

定位：第 31～33 号合同的聚焦实施附录和迁移清单。本文不重新定义接入、路由、Actor、Workflow、
ActionPermit 或公开协议；如果与第 26～33 号完成迁移后的正文冲突，以完成迁移后的上位合同为准。

## 0. 结论

P0 采用下面这条最小、准确且可演进的链路：

```text
一座农场一份 FarmOpportunityHeader（Authority Redis）
→ 只保存最早可偷/浇水/除虫时间，不保存 candidate_mask
→ 查询时用 trusted_now_ms 计算粗筛结果
→ FarmReadAPI.BatchEvaluate 读取紧凑权威切片并计算 viewer 专属 action_mask
→ 最终操作仍由 ActionPermit + FarmActor 串行裁决
```

P0 不实现 `FarmOpportunitySummary`、Summary Refresh Order、到点改 Mask 的 Scheduler，也不为每名好友写
离线通知 Inbox。只有真实压测证明 Authority Header 读流量明显抬高权威写 P99，才启用独立 Projection
Redis 中的 Summary Provider。

护理和偷菜的数据边界冻结为：

```text
Care：一茬最多一个浇水 Incident + 一个虫害 Incident
      每个 Incident 只记录唯一成功解决者，不保存尝试者集合

Theft：每名好友每茬最多一次合法 Attempt
       使用 (crop_cycle_id, visitor_player_id) 唯一 Receipt
       P0 一茬最多十个合法 Attempt，因此 Receipt 数量天然有界
```

## 1. 不改变的上位边界

本次修改不得改变：

1. 第 26 号的登录、Session、Join、Subscription 和实时下行顺序；
2. 第 27 号的 PLAYER/FARM Route Domain、VirtualShard、Actor Mailbox 和 Actor 外 Workflow；
3. 第 28 号的 `GET_FRIEND_LIST`、`GET_NEXT_FRIEND_OPPORTUNITY`、`CARE`、`THEFT` 公网命令身份；
4. 第 29 号的 TrustedContext、ActionAdmission、Participant 和 Worker 恢复身份；
5. canonical FriendRelation 是好友真值，FriendIndex 只是查询索引；
6. FarmActor 是地块、CropCycle、CareIncident、TheftAttemptReceipt 的最终裁判；
7. Farm 的地块、护理、偷菜和 FarmVersion 使用可靠 Authority Commit，不走普通 SoftSave；
8. 成熟与护理到期由时间派生，Scheduler 不修改 `MATURE/ACTIVE` 业务状态。

## 1.1 2026-08-08 Linux 真实实现核验

核验基线：

```text
仓库：/data/home/developer_026/CodeBuddy/20260519141303/farm-game
好友/护理最新集成分支：codex/cp-vertical-b-integration-20260808
基线提交：c766e3996bf16488655cc4884be8957a62e5de2d
```

基线测试已通过：

```text
go test ./internal/farm/domain ./internal/farm/actor \
        ./internal/farm/ingress ./internal/friend
```

真实代码现状：

1. 新 CARE Authority 链路已经使用 `CareIncident.resolved_by_player_id` 和 `CareRewardOrder`，并且
   `internal/farm/ingress/care_authority_test.go` 已证明它不写 legacy `RecentCareAttempts`；
2. `FarmRecord.recent_care_attempts` 和 `FarmActor.PrepareCare` 仍是旧兼容路径，迁移期只能停止新写、保留旧记录读取，
   不能直接删 Proto 字段；
3. `FarmOpportunityHeader` 当前仍保存 `CandidateMask/DirtyGeneration`，并被编码进多种 operation/participant
   proof；它没有独立的物理读字段；
4. Redis Authority Store 已支持同一 Actor Hash 中多个 `model:<name>` 字段，并在同一次 Commit Lua 中原子替换；
   但 Farm Binding 当前只提交 `model:record`；
5. `internal/friend/opportunity.go` 目前只有 `SummaryReader + ExactEvaluator` 的领域端口与单测，尚未接真实
   Redis Summary Provider；HTTP `/api/v1/friends/opportunity` 也仍是占位实现；
6. 因而现在正适合在真正接线前替换为 Header ReadPath，不存在迁移已上线 Summary 数据的问题。

### 1.2 Linux 并行任务隔离结论

已存在但尚未合入的新偷菜工作树：

```text
farm-game-theft-p99-fastpath-20260808
farm-game-theft-p99-recovery-20260808
```

当前核验时两棵工作树均无未提交修改、也没有运行中的 Codex 进程，但仍按其原任务所有权保留冲突边界：

- FastPath 可能修改 `internal/theftonline/**`、`internal/farm/ingress/theft_admission_test.go`、
  `cmd/farm-svr/main.go`；
- Recovery 可能修改 `internal/worker/theftrecovery/**`、部分 `internal/friend/**`、`cmd/farm-worker/main.go`。

已从 CP-B 最新集成提交建立隔离工作树：

```text
工作树：/data/home/developer_026/CodeBuddy/20260519141303/farm-game-friend-opportunity-header-v2-20260808
分支：codex/friend-opportunity-header-v2-20260808
基线：c766e3996bf16488655cc4884be8957a62e5de2d
```

当前可以安全实施 I0/I1，但在偷菜任务完成或明确释放前，禁止该工作树修改：

```text
internal/friend/**
internal/theftonline/**
internal/worker/theftrecovery/**
cmd/farm-svr/main.go
cmd/farm-worker/main.go
```

因此好友查询接线 I3 暂不启动；先完成 Farm Header/Facts 权威地基，随后把偷菜提交合入共同基线，再由
单一 Owner 接 `internal/friend/**`，可以把合并冲突控制在接口适配层。

### 1.3 I0/I1 实施检查点

2026-08-08 已在隔离分支完成第一阶段实现：

```text
分支：codex/friend-opportunity-header-v2-20260808
提交：e64e1f9
```

已完成：

1. `FarmOpportunityHeaderV2` 与 `FarmOpportunityFactsV1` 的确定性派生、定长/有界二进制编码；
2. Header 不保存时间相关 Mask 和 DirtyGeneration；Facts 保存当前 Active Cycle、剩余偷菜槽位、未解决护理事件及
   每茬最多十名合法偷菜访客；
3. Farm Authority 初始建档和后续写入均使用同一个 Actor Hash 内的 `record + opportunity_header +
   opportunity_facts` 三模型字段；
4. 三字段由同一 WriterFence、RecordVersion CAS、Receipt 和 Lua Commit 原子提交；
5. legacy 记录缺少 Header/Facts 时返回 UNKNOWN/fallback，不解释为“无机会”；
6. 物理容量检查已计入三模型字段，相关定向测试与 `go test ./...` 均通过。

### 1.4 I2 实施检查点

2026-08-08 已继续在同一隔离分支完成权威只读链路：

```text
提交：158cff8
```

已完成：

1. `ModelFieldBatchReader` 成为 Authority Store 的只读能力，不获取 WriterClaim、也不激活 Actor；
2. Memory 与 Redis 实现都保持输入 farm_id 顺序，Redis 使用有界 Pipeline；在 ClusterClient 下由 go-redis
   按节点/Slot 拆批，业务调用者不感知物理 Bucket；
3. 单次 `FarmReadAPI` 批次冻结为最多 64 座农场，好友上限 300 时由调用者分成最多五批；
4. `BatchReadHeaders` 读取 `model:opportunity_header` 做时间粗筛；
5. `BatchEvaluate` 同批读取 Header/Facts，校验 `source_farm_version` 一致后计算 viewer 专属
   `STEAL/WATER/REMOVE_PEST`；
6. 旧记录缺字段、字段损坏或版本不一致均返回 `UNKNOWN`，不会被误认为无互动机会；
7. Redis 批读、精确 viewer 判定、legacy fallback 定向测试及 `go test ./...` 全部通过。

### 1.5 I3A 实施检查点

2026-08-08 已完成好友查询核心的 Provider 接缝，但尚未宣称公网命令接线完成：

1. `Navigator` 不再依赖 `SummaryReader`，改为可替换的 `CandidateReader`；
2. P0 `AuthorityOpportunityProvider` 同时实现 Header 粗筛和 Facts 精查，复用 I2 的不激活 Actor 读链；
3. 可选 `SummaryCandidateReader` 只作为未来 A/B Provider，不能改变精确判断；
4. Header/Summary 缺失或损坏统一作为 `UNKNOWN` 进入精查，修复了旧实现“缺 Summary 就跳过好友”的假阴性；
5. 候选在每个有界扫描批次内按最新机会时间排序，窗口最多 8 个；
6. `ActionUnknown` 在连续导航中保持可重试，不会被当作 `NONE` 丢弃；
7. `internal/friend`、`internal/farm/readpath` 全量和 race 测试通过。

尚未完成：I3B 将 `GET_FRIEND_LIST / GET_NEXT_FRIEND_OPPORTUNITY` 的 FirstHop Handler、FriendIndex 分页、
Runtime Navigation Redis 与该 Provider 组合起来；I4 H5 行为；I5 真实 Redis + 服务级 A/B/C 压测。第 76 号
现已具备可执行的本地预基准，但它只用于比较命令形态，不能替代 I5。

## 2. 冻结后的领域模型

### 2.1 FarmOpportunityHeader V2

每座农场只有一份 Header。它位于 FARM Authority Bucket，与 Farm CandidateState 在同一次提交中更新：

```text
FarmOpportunityHeaderV2 {
  schema_version = 2
  source_farm_version

  next_stealable_at_ms   // 仍有可偷槽位的 Active Crop 中最早 mature_at
  next_water_at_ms       // 未解决 WATER Incident 中最早 due_at
  next_pest_at_ms        // 未解决 PEST Incident 中最早 due_at
  next_opportunity_at_ms // 上述非空时间的最小值
}
```

约定：

- `0` 表示该类型当前没有任何候选；
- 已经到期但尚未处理的机会仍保留原时间，不在查询后写回；
- Header 不保存 `candidate_mask/opportunity_mask`；
- Header 不保存 viewer 维度结论；
- `next_opportunity_at_ms` 是读取优化字段，必须等于三个分项非零值的最小值；
- `source_farm_version` 用于调试、缓存 Fence 和未来 Projection 构建，不取代 FarmVersion。

#### 2.1.1 Redis Authority 物理落点

Linux 当前一个 Actor Hash 可以保存多个原子模型字段。因此 P0 不把 Header 继续藏在 operation proof，
也不只把它追加进完整 `FarmRecord` Blob。冻结为：

```text
同一个 FARM Actor Redis Hash / 同一个 Cluster Slot
├── model:record               // 完整 FarmRecord，Actor 激活与业务裁决
├── model:opportunity_header   // 固定大小的 Header V2，好友批量粗筛
└── model:opportunity_facts    // 紧凑精确事实，候选 BatchEvaluate
```

`model:opportunity_facts` 只包含计算 viewer `action_mask` 所需的信息：

```text
FarmOpportunityFactsV1 {
  source_farm_version
  active_cycles[] {
    plot_id
    crop_cycle_id
    mature_at_ms
    remaining_steal_slots
    unresolved_water_due_at_ms
    unresolved_pest_due_at_ms
    theft_attempt_visitor_ids[] // 每茬最多十个
  }
}
```

三者必须由同一份 CandidateState 重建，并通过同一次 Authority Commit Lua/CAS 提交。禁止：

```text
先提交 model:record
→ 再异步写 Header/Facts
```

Header/Facts 是同一权威记录里的读优化字段，不是 Projection Redis，也不需要 Worker 刷新。

旧记录兼容：

- 旧 Actor Hash 只有 `model:record` 时，Actor 激活后从完整事实重建 Header/Facts，下一次业务写原子回填；
- 未激活旧记录被好友查询命中时，Provider 将其标记为 `UNKNOWN/MISSING_READ_MODEL`，再有界读取完整
  `model:record` 计算；不得把缺少 Header 当成 `NONE`；
- operation/participant proof 的旧 Header 解码必须保留；新 proof 升版本后写 V2，禁止原地改变旧版本字节含义。

### 2.2 CareIncident

P0 每个 CropCycle 最多创建两个 Incident：`WATER` 和 `PEST`。`PENDING/ACTIVE` 是时间派生展示阶段，
不需要到点写状态：

```text
CareIncident {
  incident_id
  crop_cycle_id
  plot_id
  care_type              // WATER | PEST
  due_at_ms

  resolution? {
    resolved_by_player_id
    resolved_at_ms
    resolve_operation_id
    care_reward_order_id
  }
}
```

阶段计算：

```text
CropCycle已结束                         → CLOSED
resolution存在                         → RESOLVED
trusted_now_ms < due_at_ms             → PENDING
trusted_now_ms >= due_at_ms            → ACTIVE
```

不保存：

```text
watered_player_ids[]
care_attempt_players[]
每位失败护理者一份长期Incident Receipt
```

首次解决者仍必须写入 `resolved_by_player_id`，用于活动展示、护理奖励、好友互动任务和审计。
`CareOperationReceipt` 是 operation 幂等和稳定结果，不是“该好友是否有一次护理额度”的集合。

### 2.3 TheftAttemptReceipt

```text
TheftAttemptReceipt {
  theft_attempt_id
  crop_cycle_id
  visitor_player_id
  consumed_slot_index
  gross_stolen_quantity
  guard_lost_quantity
  net_received_quantity
  decision_hash
  committed_at_ms
}
```

唯一业务身份：

```text
(crop_cycle_id, visitor_player_id)
```

只有通过关系、成熟、周期和剩余槽位前置检查，并被 FarmActor 接受的合法 Attempt 才创建该 Receipt。
宠物完整截留仍属于合法 Attempt，消耗当前槽位和该好友本茬机会。无剩余槽位、未成熟、非好友等前置
拒绝不创建长期 TheftAttemptReceipt；对应 operation 的短期结果仍服从统一幂等合同。

P0 每茬十个 2% 槽位，所以一个 CropCycle 最多十份合法 TheftAttemptReceipt，不会增长到全部 300 个好友。
CropCycle 结束后，Receipt 按第 30/31 号的业务防重保留期归档或清理，不能在仍可能重放时提前删除。

## 3. Header 的确定性重算

FarmActor 每次从当前 CandidateState 扫描最多 24 块土地，重算 Header。复杂度固定为 `O(24)`，不维护
第二套容易漂移的增量 Mask。

```text
RebuildOpportunityHeader(farm, trusted_now_ms_not_required):
  stealable = []
  water = []
  pest = []

  for each plot in farm.plots:
    cycle = plot.active_crop
    if cycle不存在:
      continue

    if cycle.next_steal_slot_index < len(cycle.steal_plan.slot_quantity):
      stealable.append(cycle.mature_at_ms)

    for incident in cycle.care_incidents:
      if incident.resolution不存在:
        if incident.type == WATER: water.append(incident.due_at_ms)
        if incident.type == PEST:  pest.append(incident.due_at_ms)

  next_stealable_at_ms = min_or_zero(stealable)
  next_water_at_ms = min_or_zero(water)
  next_pest_at_ms = min_or_zero(pest)
  next_opportunity_at_ms = min_non_zero(三者)
```

重算不使用当前时间，因此不会因为墙钟前进产生写放大。时间只在查询和最终 FarmActor Turn 中读取。

### 3.1 必须触发重算的提交

| 权威操作 | Header 变化 |
|---|---|
| Plant | 加入 `mature_at`、WATER/Pest `due_at` |
| Care 首次成功 | 移除对应未解决 Incident 的候选时间 |
| Theft 合法 Attempt | 若仍有槽位则保持该周期 `mature_at`；槽位耗尽后移除 |
| Harvest/铲除 | 移除该 CropCycle 的成熟和护理时间 |
| 配置热更新 | 不改变在途 CropCycle，不重算其冻结时间 |
| Snapshot/好友列表查询 | 只读，不更新 Header |

Header 必须由与业务事实相同的 CandidateState 产生，并在同一次 Authority Commit 中写入。禁止先提交
CropCycle/CareIncident，再通过内存队列补 Header。

## 4. 好友列表与连续导航读路径

### 4.1 当前页好友列表

```text
H5 GET_FRIEND_LIST
→ FriendIndex分页得到候选friend_id
→ BatchValidatePair过滤删除/拉黑/旧epoch
→ BatchGet PublicProfile
→ BatchGet FarmOpportunityHeaderV2
→ 使用同一次trusted_now_ms计算effective_candidate_mask
→ FarmReadAPI.BatchEvaluate当前页有效好友
→ 返回精确action_mask或UNKNOWN
```

查询时临时计算：

```text
if 0 < next_stealable_at_ms <= trusted_now_ms: HAS_STEALABLE_CANDIDATE
if 0 < next_water_at_ms <= trusted_now_ms:     NEED_WATER_CANDIDATE
if 0 < next_pest_at_ms <= trusted_now_ms:      HAS_PEST_CANDIDATE
```

这只是粗筛。`BatchEvaluate(viewer, farm_ids)` 必须从紧凑 Authority ReadPath 检查：

- 当前 CropCycle 是否仍存在且已成熟；
- 可偷槽位是否仍有剩余；
- `(crop_cycle_id, viewer_player_id)` TheftAttemptReceipt 是否存在；
- WATER/PEST Incident 是否到期且未解决；
- 目标版本是否完整可读。

好友关系不在 BatchEvaluate 中重复扫描 FriendRelation；上游 BatchValidatePair/ActionPermit 分别承担页面候选
和执行授权。页面查询结果不具备授权能力，点击操作仍重新走 ActionAdmission。

### 4.2 Redis 读取方式

P0 不逐好友激活 FarmActor。由于 FARM 写均可靠落入 Authority Redis，BatchEvaluate 可读取一个提交前或
提交后的完整版本，不能读取半份 CandidateState。

读取必须：

1. 按 Redis Cluster 节点/Slot 分组；
2. 使用 Pipeline/MGET/HMGET 等平台允许的批量方式；
3. 设置总并发、每节点并发、item、byte 和 Deadline 上限；
4. 单卡超时返回 `UNKNOWN/Loading`，不能伪装成 `NONE`；
5. 不在查询路径写回 Header、Mask 或 Seen 状态。

粗筛只批读 `model:opportunity_header`；只有粗筛命中的候选才批读
`model:opportunity_facts`。普通好友卡片不下载完整 `model:record`。这正是拆成两个 Authority Model Field，
而不是把 Header 放入 FarmRecord Blob 的原因。

### 4.3 连续机会导航

`FriendOpportunityNavigation` 继续保存在 Runtime Redis。首次查询按 32～64 个好友为一批读取 Header，
只对到期候选执行有界 BatchEvaluate，保存最多 8 个精确候选；回到自己农场后先复核窗口中的下一项，
不够再继续扫描。

P0 只保证：

- 返回的当前候选经过精确检查；
- 已进入并 Ready 的推荐农场在本轮标记 visited；
- 当前窗口内按 `latest_actionable_at_ms` 排序；
- 不承诺对尚未扫描的全部 300 名好友形成严格全局时间排序。

若产品以后要求“300 名好友中严格最新机会一定排第一”，必须通过压测后选择：全量紧凑索引扫描、
独立 Summary/时间索引 Projection，或调整产品排序合同，不能让 P0 查询暗中扫描全部 FarmRecord。

## 5. Summary 的演进位置

P0：

```text
OpportunityCandidateProvider = AuthorityHeaderProvider
```

保留可替换接口：

```go
type OpportunityCandidateProvider interface {
    BatchGet(ctx context.Context, farmIDs []uint64) ([]OpportunityCandidate, error)
}
```

以后压测触发时才增加：

```text
OpportunityCandidateProvider = ProjectionSummaryProvider
```

Summary 只能转移读取负载和提供查询排序，不能替代 BatchEvaluate 或 FarmActor。启用条件至少满足一项：

1. Header 批读使 Authority Redis 写 P99 相比隔离基线增加超过 10%；
2. 热点 Slot 的 CPU、网络或连接池长期超过安全水位；
3. 首次导航在平均/上限好友模型下无法满足产品 SLO；
4. 产品要求严格全好友机会排序，而紧凑 Authority ReadPath 成本不可接受。

5%～10% 为首轮决策观察区，不是最终生产承诺。最终阈值由真实公司 Redis 压测冻结。

## 6. Care 权威提交与奖励

### 6.1 首次成功

```text
FarmActor.ResolveCareIncident
→ 校验CropCycle/Incident/Permit/due_at
→ resolution不存在
→ 写唯一resolution
→ 写CareOperationReceipt
→ 创建CareRewardOrder(PENDING)
→ 重算FarmOpportunityHeaderV2
→ PlotVersion++ / FarmVersion++
→ 同一次Authority Commit
→ PostCommitDeliveryGuard发布一份FarmChange
```

`CareRewardOrder` 通过 DirectDispatcher 快速投递，失败由 farm-worker/DueIndex 恢复。Actor Turn 不等待
PlayerActor。只有首次解决者获得经验；好友护理时才推进一次好友互动任务。

### 6.2 并发和重试

```text
A和B同时浇水
→ FarmActor Mailbox唯一顺序
→ A先提交resolution
→ B看到resolution已存在
→ B返回NO_CHANGE
```

- A 的同 operation 重试返回原 CareOperationReceipt；
- B 不创建第二个 CareRewardOrder；
- 不保存 `B曾经尝试浇水` 的长期 Incident 记录；
- 如果 B 的 NO_CHANGE 响应丢失，只由统一 OperationResult 保留必要的短期稳定结果。

## 7. 逐文档修改计划

修改必须按顺序执行，避免下游 Agent 同时依据两套模型开工。

### M0：建立修改锁

在 Manifest 将 31、32、33、40、61、71～73 和 Casebook 标记为 `ACTIVE_EDIT/NEEDS_REBASE`；26～30 只做
术语审计，不改变其主合同。M0 完成前暂停派发 Opportunity/Summary/Care 集成任务。

### M1：修改主合同

| 文件 | 必须修改 |
|---|---|
| 31 | Header 改为 V2 时间索引；删除持久 candidate_mask；Summary 改为压测后可选 Provider；重写好友读路径、容量与验收 |
| 32 | Plant/Care/Theft/Harvest 的 Header 更新改为显式字段重算；Care 明确只保存唯一解决者；Theft 明确每茬最多十份合法 Receipt |
| 33 | 好友列表与连续导航默认读取 AuthorityHeaderProvider；Summary 只作为可替换加速源；Permit/Pair 语义不变 |
| 33A | 仅审计：Pair/Quota/Permit Key/Lua 不因本次方案改变；删除把 Summary 误写成授权来源的措辞 |

### M2：同步跨合同接缝

| 文件 | 修改范围 |
|---|---|
| 26 | Bootstrap/登录弹窗只引用 `FriendOpportunityRead`，不绑定 Summary 实现 |
| 27 | 路由表保留 SERVICE_LB + FARM ReadPath；Projection Summary 改成可选演进 |
| 28 | CommandType/Payload 不变；Result 注释改为精确 action_mask 来自 BatchEvaluate |
| 29 | Trusted RPC 不变；候选 Provider 名称泛化，Worker 不再承担 P0 Summary 刷新 |
| 30 | 删除 P0 必须维护 opportunity dirty generation 的要求；保留通用可重建 Projection 机制 |
| 40 | 协议总控登记“无公网协议变化、P0读路径变化” |
| 60 | 增加 Header 无假阴性、Care 唯一解决者、查询 UNKNOWN 降级测试 |
| 61 | 容量模型从 Summary 逻辑读改为 Header item/byte 与 Authority 写 P99 竞争测试 |

### M3：同步案例和实施编排

| 文件 | 修改范围 |
|---|---|
| core-scenario-casebook | 重写 C07：Header 粗筛、BatchEvaluate 精筛、护理唯一解决者、数据位置与失败表现 |
| 71 | 删除“P0 必须先完成 Summary”依赖；增加 Header/ReadPath/Care 单写者实施波次 |
| 72 | DF1、SO1/H1、R1/R2 工作包改成 Provider 接口和 Header V2；禁止并行修改 FarmActor |
| 73 | 更新 Linux Agent Prompt、owned_write_set、验收命令和暂停/恢复条件 |
| 架构图 | 不立即重画；下一次总体图更新时把 Summary 标成“可选 Projection”，主链画 Header + BatchEvaluate |

### M4：治理收尾

全文搜索并分类处理：

```text
candidate_mask
opportunity_mask
FarmOpportunitySummary
opportunity_dirty_generation
OpportunitySummaryRefreshOrder
Summary只作候选
```

legacy 和历史报告不回改；现行合同、实施计划、案例册和图源必须不存在“Summary 是 P0 必经链”的残留。
更新版本号、Manifest 日期和 README 阅读顺序后再解除写锁。

## 8. 代码实施计划

### I0：合同与测试向量先行

交付：

- Header V2 字段与空值语义；
- 24 地块重算黄金向量；
- CareIncident 首次解决/重复请求向量；
- Theft 十槽与 Receipt 上界向量；
- Header 粗筛允许假阳性、禁止假阴性的测试矩阵。

退出条件：第 31/32/33 号完成 M1，公共 DTO/领域接口不再漂移。

### I1：Farm 纯领域与 Actor 接线

单一 Owner：`DF1-FARM-AUTHORITY-INTEGRATION`。

```text
owned_write_set:
  internal/farm/domain/**
  internal/farm/actor/**
  internal/logic/actor/authority/**（仅多模型 CandidateState 接口）
  internal/storage/authoritystore/**（仅通用多模型接口/测试）
  Farm CandidateState/mapper/tests

forbidden:
  Friend Pair Repository
  公网Proto字段号
  Redis公共KeyBuilder/Lua框架
  H5
```

任务：

1. 实现 Header V2 与 OpportunityFacts V1 的确定性全量重算；
2. 将 Farm Authority CandidateState 从单一 `model:record` 扩展为 record/header/facts 三字段；
3. Plant/Care/Theft/Harvest 候选状态全部调用同一重算函数；
4. CareIncident 只保存可选 resolution；停止新 CARE Authority 路径写 legacy CareAttemptReceipt；
5. 首次 Care 同提交 Receipt + CareRewardOrder + Header/Facts + FarmVersion；
6. TheftAttemptReceipt 按 cycle+visitor 唯一，并验证最多十份合法 Receipt；
7. proof 使用显式版本升级兼容旧 Header，禁止修改已有 proof 版本的字节布局；
8. Actor 重载、重试、并发后结果一致。

### I2：Authority 紧凑读路径

单一 Owner：`R2-FARM-OPPORTUNITY-READPATH`。

任务：

1. 复用第 31 号 KeyBuilder/Repository，不手拼 Redis Key；
2. 提供 `BatchGetOpportunityHeader`，只读 `model:opportunity_header`；
3. 提供 `BatchGetOpportunityFacts(viewer, farm_ids)`，只读 `model:opportunity_facts`；
4. 按 Redis 节点/Slot 分组并行、Pipeline、有界 item/byte/deadline；
5. 证明读到的是一次完整 Authority Commit 的旧版或新版，而不是半提交；
6. 输出 Header 和精确切片实际序列化大小、Redis 命令数与延迟报告。

按 Linux 当前实现，I1 必须先完成三模型字段的 Authority Commit 接缝，I2 才能接真实批读；两者不再并行修改
Authority Binding/Store。I2 可提前只写不落地的 Provider 接口、Fake 和黄金测试，但不得自行定义第二套 Header/Facts。

### I3：好友查询与连续导航

单一 Owner：`H1-FRIEND-OPPORTUNITY-NAVIGATION`。

任务：

1. 将现有 Summary 依赖收敛到 `OpportunityCandidateProvider`；
2. P0 注入 `AuthorityHeaderProvider`；
3. `GET_FRIEND_LIST` 当前页执行 Pair 校验、Profile 批读、Header 粗筛和 BatchEvaluate；
4. `GET_NEXT_FRIEND_OPPORTUNITY` 每批 32～64 人、候选窗口最多 8 人；
5. Ready 后标记 visited，返回自己农场后复核并给下一项；
6. Header/精确读超时显示 Loading/Retry，不返回伪造 NONE；
7. 不激活 20/300 个 FarmActor，不逐好友 RPC/Redis 命令。

### I4：Dispatcher/Worker 奖励恢复

复用现有 WF1，不创建新 Worker 类型：

```text
CareRewardOrder
→ DirectDispatcher最佳努力
→ PlayerActor.ApplyCareReward
→ 失败由DueIndex + farm-worker恢复
```

验证 Farm 已解决、PlayerActor 暂不可用、Worker 重启和投递响应丢失时，护理经验与任务只到账一次。

### I5：H5 接入

1. 好友卡片只消费服务端 `action_mask/UNKNOWN`；
2. UNKNOWN 显示 Loading/Retry；
3. 在线 Hint 只触发重新 Pull，不直接点亮可护理/可偷按钮；
4. 回自己农场后请求下一候选；
5. `NO_CHANGE` 显示“已经被其他好友处理”，随后刷新当前农场/导航候选。

### I6：容量与演进决策

至少覆盖：

- 好友平均 100、上限 300；
- 每人每天登录 5～6 次、单次不超过 5 分钟；
- 20 人当前页/32～64 扫描批次/8 人候选窗口；
- 200 人同场护理并发；
- 同一 Incident 200 请求竞争；
- 24 地块全部种植、全部带 WATER/PEST；
- Authority Redis 同时承受 Farm 写与 Header/精确读；
- 读超时、Redis 节点抖动、Actor 迁移和 Worker 重启。

对比三组：

```text
A：仅业务写，无好友机会查询
B：Authority Header + BatchEvaluate
C：独立Projection Summary + BatchEvaluate（仅压测原型）
```

记录 Gate/ReadAPI QPS、Redis command/item/byte、CPU、网络、连接池等待、Farm Commit P95/P99、查询
P95/P99、假阳性率和 UNKNOWN 比例。没有真实数据前不得宣称 Summary 一定更快或 Authority 读一定足够。

## 9. 推荐 Agent 派发与依赖

```text
M0～M4 文档迁移（单写者）
          │
          ▼
I0 合同/黄金向量（共享基线）
          │
          ▼
I1 Farm + Authority多模型提交单写者
          │
          ▼
I2 Redis Header/Facts ReadPath
          │
          ▼
I3 Friend Query/Navigation
     ┌────┴──────────┐
     ▼               ▼
I4 Worker恢复      I5 H5
     └────┬──────────┘
          ▼
I6 E2E/容量/故障注入
```

禁止把 I1 拆给“Header Agent”和“Care Agent”同时修改 FarmActor。可以并行开发纯函数和测试，但最终 Actor
接线、CandidateState 和 Authority Commit 必须由一个 Owner 完成。

## 10. 分阶段验收

### 10.1 领域正确性

- 24 地块任意组合下 Header 三个分项和总最小值正确；
- 时间经过不需要 Redis 写，也能在查询时出现机会；
- 两人并发护理只有一个 resolution 和一个 CareRewardOrder；
- 护理不保存玩家尝试集合；
- 同一好友同一 CropCycle 只能有一份 TheftAttemptReceipt；
- 一茬最多十份合法 TheftAttemptReceipt；
- Harvest 后该周期不再出现在 Header 和 BatchEvaluate 中。

### 10.2 查询正确性

- Header 到期候选不会被漏掉；
- Header 假阳性会被 BatchEvaluate 过滤；
- viewer 已偷过某茬时不再显示该茬可偷，但其他地块仍可显示；
- 护理被别人完成后当前页刷新为无该护理机会；
- Pair 删除/拉黑后旧 FriendIndex 不会产生可操作按钮；
- 部分 Redis 超时显示 UNKNOWN，不显示错误的 NONE。

### 10.3 可靠性

- Care Authority Commit 后进程退出，奖励 Order 仍可发现；
- 奖励提交成功但响应丢失，不重复加经验/任务；
- Header 与 Farm 事实不存在半提交；
- Actor Passivation/迁移后 Header、Incident、AttemptReceipt 可恢复；
- Projection Redis 全部关闭时，P0 好友机会仍能通过 Authority Header 正常工作。

### 10.4 性能

- Header 序列化大小有硬上限；
- 批量读按 node/slot 聚合，不产生 N+1；
- 20/100/300 好友模型都有 item、byte、命令数和延迟证据；
- 开启好友机会读后 Authority 写 P99 的变化有明确报告；
- 达到触发阈值才进入 Summary 物理实现，不根据感觉提前建设投影刷新链。

## 11. 完成定义

只有同时满足以下条件，才能把本计划标为“实现完成”：

1. M0～M4 文档迁移完成，现行文档不存在 P0 Summary 必经链残留；
2. I1～I5 进入 Linux 主线且提供 commit、测试和 dirty 状态；
3. Care/ Theft/好友列表/连续导航纵向 E2E 通过；
4. Redis 真实环境至少完成 Header 批读、Farm Commit 竞争和超时降级探针；
5. 200 人同场护理只产生一次权威变化，慢连接不阻塞 FarmActor；
6. I6 报告明确决定继续 AuthorityHeaderProvider，还是启用 ProjectionSummaryProvider。
