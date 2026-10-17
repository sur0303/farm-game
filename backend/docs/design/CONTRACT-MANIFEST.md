# 经典农场设计合同 Manifest

更新时间：2026-08-08
用途：记录现行文档的唯一职责、协作状态和依赖关系。它是导航和并发写入锁，不重新定义任何技术语义。

## 状态定义

| 状态 | 含义 |
|---|---|
| `STABLE_INPUT` | 已完成本轮讨论，可作为下游设计输入；仍需实现和生产验证 |
| `PLATFORM_PARAMETERS_PENDING` | 语义已冻结，但平台能力、容量阈值或最终物理参数仍须探针/压测；只能作为 `STABLE_INPUT` 的限定语 |
| `IMPLEMENTATION_VALIDATION_PENDING` | 语义和合同链已冻结，但代码完成度与生产能力仍须独立验证证据；只能作为 `STABLE_INPUT` 的限定语 |
| `BASELINE_COMMIT_PENDING` | 实施计划内容已与现行合同对齐，但尚未登记唯一合同 Baseline Commit，不能正式派发并行施工 |
| `ACTIVE_EDIT` | 正在由一条独立写入线更新；其他 Agent 不得修改或据此重写依赖文档 |
| `NEEDS_REBASE` | 自身内容存在，但上游刚变化，必须重新对齐后才能作为总控/计划依据 |
| `PAUSED_BY_USER` | 用户明确暂停；不继续设计且不作为当前实现输入，恢复时先审计上游变化 |
| `PLANNED_PHYSICAL` | 逻辑上游已冻结，但对应物理实现合同尚未创建；不得据此自行发明 Key/Lua |
| `ACCEPTANCE_ONLY` | 只定义测试和验收，不拥有业务语义 |
| `EXPLANATION_ONLY` | 只解释案例，不拥有规范语义 |
| `REFERENCE_REVIEW` | 暂时保留、等待分类；不能覆盖现行合同 |

## 当前总账

| ID | 文件 | 当前版本 | 状态 | 唯一职责 | 直接上游 |
|---|---|---|---|---|---|
| 26 | [接入与会话](./26-access-session-contract.md) | v1.3 | `STABLE_INPUT` | Auth、Ticket、Session、Join、Subscription、200 人下行与首屏查询接缝 | 用户决定 |
| 27 | [路由/Actor/Workflow](./27-access-routing-actor-workflow-freeze.md) | v4.2 | `STABLE_INPUT` | 首跳、VirtualShard、Actor Runtime、Workflow/Order、Bootstrap与好友机会路由 | 26、32～37 |
| 28 | [公网协议](./28-command-catalog-public-protocol.md) | v2.8 | `STABLE_INPUT` | FarmFrame、33类CommandSpec、不可变Result/Push、Root作用域结果查询 | 26、27、32～37 |
| 29 | [内部 RPC](./29-internal-rpc-trusted-context.md) | v2.11 | `STABLE_INPUT` | 三类可信调用、Join握手、Ready后Navigation接缝与Actor/非Actor权威 | 26～28、32～37 |
| 30 | [Actor 权威 Store](./30-actor-authority-store-contract.md) | v1.0 | `STABLE_INPUT` | Writer Fence、CAS、Unknown、Persistence Lane及非Actor等价边界 | 27、29 |
| 31 | [Redis 物理合同](./31-actor-authority-redis-physical-contract.md) | v1.4 | `STABLE_INPUT` | Actor Key/Slot/Lua/DueIndex/ResultDirectory、Navigation、GuardSync、图鉴Model与容量 | 30、34～37 |
| 32 | [核心经营场景](./32-core-business-scenario-contract.md) | v0.29 | `STABLE_INPUT` | Buy/Sell/Unlock/Plant/Mature/Harvest/Care/Theft与现行支持系统接缝 | 26～31、33～37 |
| 33 | [好友、分享与权限](./33-friendship-share-access-contract.md) | v1.2 | `STABLE_INPUT` | Relation、Share、Delete/Block、ReactivationBarrier、Permit与连续导航语义 | 26～32 |
| 33A | [好友权威 Redis 物理合同](./33A-friendship-authority-redis-physical-contract.md) | v0.3 | `STABLE_INPUT` | Pair/Quota/ShareIssuer Key、Lua、Attempt Fence、GC、ACL与容量 | 29～33 |
| 34 | [任务系统](./34-task-system-contract.md) | v1.3 | `STABLE_INPUT` | 任务周期、进度、领奖、红点与版本化PetUnlockGrant | 26～33、36 |
| 35 | [系统邮件与附件领取](./35-mail-system-contract.md) | v1.2 | `STABLE_INPUT` | MailEntry、六封分页、可信材料、附件领取；P0排除宠物解锁 | 26～34、36 |
| 35A | [PlayerMail Redis 物理合同](./35A-player-mail-redis-physical-contract.md) | v1.0 | `STABLE_INPUT` | 非Actor邮件Key、同槽Lua、分页、Unknown、GC、ACL与容量；平台参数待验证 | 31、35 |
| 36 | [简化宠物守护系统](./36-simple-pet-guard-contract.md) | v1.1 | `STABLE_INPUT` | 宠物解锁/喂食/切换、状态版本、GuardSync水位与偷菜守护 | 26～35 |
| 37 | [植物图鉴与活动档案](./37-plant-compendium-contract.md) | v1.1 | `STABLE_INPUT` | Plant冻结解锁资格、Harvest原始Delta、活动档案、Seen Fence与容量 | 26～36 |
| 40 | [协议与技术收口总控](./40-core-technical-closure.md) | v1.1 | `STABLE_INPUT` | 26～37现行合同导航、33类协议收口、实现准入与待验证边界 | 26～37 |
| 60 | [接入会话验收](./60-access-session-test-acceptance-freeze.md) | v1.5 | `ACCEPTANCE_ONLY` | 证明 26 范围是否实现正确 | 26、28、29 |
| 61 | [性能、弱网恢复与平滑更新统一方案](./61-performance-resilience-smooth-update-plan.md) | v0.2-capacity-vector-release-guard-rollback-alignment-review | `ACTIVE_EDIT / PLATFORM_PARAMETERS_PENDING / IMPLEMENTATION_VALIDATION_PENDING` | 综合容量向量、连续体验、发布/回滚节奏和证据边界；不拥有业务、协议或验收语义 | 26～31、60、71 |
| 71 | [多 Agent 实施编排](./71-multi-agent-implementation-plan-draft.md) | v1.8 | `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING` | CP2后的长期施工依赖、写集、合并列车与验收等级；合同内容基线a78196a，不拥有业务语义 | 26～40、60 |
| 72 | [Agent 工作包手册](./72-agent-work-package-playbook-draft.md) | v1.7 | `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING` | 完整角色卡、写集、测试和交接模板；Round 0/1与MA0历史卡已退休，不拥有业务语义 | 71 |
| 73 | [Linux Agent 调度与 Prompt](./73-linux-multi-agent-dispatch-runbook.md) | v1.5 | `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING` | CP2完成基线下AR1/WF1/H1/Q1短分支派发、检查点与并发槽；不拥有业务语义 | 71、72、Linux审计/W5B/MA0证据 |
| 75 | [好友机会 Header、护理与偷菜 Receipt 实施及迁移计划](./75-farm-opportunity-readpath-care-implementation-plan.md) | v1.4-i3a-implemented | `STABLE_INPUT / I0-I3A_IMPLEMENTED / I3B-I5_PENDING` | I0/I1完成Authority record/header/facts原子提交，I2完成不激活Actor的Redis批读和精确判断，I3A完成CandidateReader与P0 Authority Provider并修复缺Summary假阴性；FirstHop/Runtime导航、公网H5和服务级压测仍待实施；不覆盖上位合同 | 26～33、60、71～73、用户2026-08-08确认 |
| 76 | [好友互动机会 Summary 与 Authority Header A/B 压测方案](./76-friend-opportunity-summary-ab-benchmark-plan.md) | v1.1-preliminary-benchmark-ready | `STABLE_INPUT / PRELIMINARY_BENCH_READY / CAPACITY_PENDING` | 已提供统一矩阵、证据规则和可执行本地预基准，比较 Authority Header 无 Summary、Projection Summary 与业务写隔离基线；预基准不得替代真实Redis、Farm写P99、投影滞后与故障降级的服务级证据 | 31～33、60、61、75、用户2026-08-08确认 |
| Casebook | [核心场景案例册](./core-scenario-casebook.md) | v1.1 | `EXPLANATION_ONLY` | C01～C07 时序、在线机会 Push/Pull、失败恢复与 Review 检查项 | 26～37 |
| D5 | [早期 Redis Key/TTL 材料](./30-redis.md) | 无现行合同版本 | `REFERENCE_REVIEW` | 等待确认是否仍有需提炼的探针/会话内容 | 30、31 优先 |

## 当前协作锁

```text
33-friendship-share-access-contract.md  → STABLE_INPUT，Q33-01～Q33-20已冻结；删除立即撤权，重加受Barrier+Rotate防护
33A-friendship-authority-redis-physical-contract.md → STABLE_INPUT，v0.3；已映射Redis物理边界，平台参数/安全选型仍待探针
34-task-system-contract.md              → STABLE_INPUT，v1.3；RewardBundle的PetUnlockGrant已绑定配置与pet_state_version
26/27/28/29/31/33跨领域接缝               → STABLE_INPUT，已同步Bootstrap、33类命令、好友机会Ready后visited及支持系统边界
35-mail-system-contract.md              → STABLE_INPUT，v1.2；P0邮件RewardBundle明确排除PetUnlockGrant
35A-player-mail-redis-physical-contract.md → STABLE_INPUT，v1.0；35A-01～35A-10已冻结，平台参数仍待Redis探针
36-simple-pet-guard-contract.md         → STABLE_INPUT，Q36-01～Q36-08已冻结；宠物协议已由28号 v2.8登记
37-plant-compendium-contract.md         → STABLE_INPUT，v1.1；Q37-01～Q37-08已冻结，协议/Participant/Model已同步27～32
40-core-technical-closure.md            → STABLE_INPUT，v1.1；26～37协议总控与33类命令已对齐，代码完成度由独立验证记录证明
71/72/73实施编排与工作包                 → STABLE_INPUT，CP2=5e31c2d/cp2-20260804；MA0已关闭，Round 2按AR1/WF1/H1/Q1写锁派发
75好友机会Header/Care迁移计划             → STABLE_INPUT；代码派发前先完成M0～M4，P0不再以Summary刷新链为前置
61-performance-resilience-smooth-update-plan.md → ACTIVE_EDIT，当前只允许本轮综合方案写入线修改；确认后再转STABLE_INPUT
30-redis.md                             → 暂留顶层，等待用户最终分类
其他现行合同                            → 本轮文档治理不修改语义
```

第 34 号现行冻结边界：

1. 只消费 26～33 已冻结的成功事实，不重定义 Join/Care/Theft/Harvest；
2. P0 固定展示三个每日任务和一个线性成长任务；
3. 任务权威位于 PlayerActor，不拆 TaskActor/task-svr，不引入任务 MQ；
4. Redis Key/Lua、具体 Proto 字段号和压测阈值留给后续物理合同或实现探针。

第 35 号现行冻结边界：

1. 仅系统邮件，无玩家互发；MailEntry 独立分页，附件领取进入 PlayerActor；
2. P0 每页六封、最多六十封、默认三十天且保护未领取附件；
3. 不新增 MailActor、mail-svr 或邮件专用 MQ；全服 Campaign 与未来可见邮件只保留 P1 边界；
4. Claim Payload只含mail_id；领取使用服务端TrustedMailClaimMaterial，同时受operation和mail_id防重；
5. 资产、MailClaimReceipt、OperationResult和投影dirty generation在PlayerActor一次可靠提交；
6. Entry/已读/分页的Redis物理实现由35A负责，claimed/红点投影不得替代领取权威。

第 35A 号现行冻结边界：

1. `35A-01～35A-10` 的非 Actor 权威、分桶、同槽原子、分页、Unknown、投影和 GC 语义已冻结；
2. PlayerMail 不使用 Actor VirtualShard Holder/WriterEpoch，也不新增 MailActor/mail-svr；
3. Entry、DeliveryDedup、Index、Summary 与 DueIndex 的物理接缝由 35A 唯一拥有；
4. Redis 平台能力、容量阈值、最终 Lua SHA/Key 前缀/Proto 字段仍为 `PLATFORM_PARAMETERS_PENDING`；
5. 待验证参数阻塞生产容量证明，但不阻塞 Fake Repository、脚本合同测试和纵向链路实现。

第 36 号当前冻结边界：

1. P0 只实现宠物拥有、切换守护宠物、喂狗粮延长守护和偷菜截留；
2. PetState 位于 PlayerActor，GuardSnapshot 位于 FarmActor，不新增 PetActor/pet-svr；
3. 喂食/切换与 GuardSyncOrder 在 Player Authority 中可靠提交，Dispatcher 快速投递、worker 恢复；
4. 宠物通过普通金币购买或任务 RewardBundle 解锁；未喂狗粮时守护不生效；
5. 狗粮每份增加8小时，可叠加但最多预存24小时；超过上限整次拒绝且不扣道具；
6. 切换已解锁宠物保留剩余守护时间，不额外消耗狗粮；
7. PetState最多32只并使用独立pet_state_version；GuardSync携带完整规则快照；
8. GuardSyncOrder使用统一来源Order ID，APPLY_GUARD_SNAPSHOT为Typed Participant，固定Watermark不依赖Order永久存在；
9. 历史宠物命令Outcome不可变；当前APPLIED/PENDING由GET_PET_STATE动态读取，不反写历史结果；
10. Feed只改Farm record_version；真正Switch才产生一次公开FarmMetaDelta；到期不写版本；
11. 宠物页不新增查询投影或专属红点；公共FarmView不暴露守护截止时间、概率和规则版本；
12. 下架只停止新购买，既有玩家仍可使用；配置、失败、并发和25项验收已冻结；
13. 宠物不扣偷菜者金币，被截留作物按第 32 号合同折价补偿农场主。

第 37 号当前冻结边界：

1. P0 唯一图鉴身份为 `crop_definition_id`；展示资源或活动实例不参与永久解锁；
2. Plant 冻结 `CompendiumUnlockSnapshot` 并随 CropCycle 保存，Harvest 不重新读取当前 Catalog 决定资格；
3. PlayerActor 将库存、经验、计数、永久成员、原始 UnlockDelta、Receipt 与 StoredOutcome 一次提交；
4. `GET_COMPENDIUM` 由 CompendiumReadIngress 在 Actor Turn 外合并本地不可变 Catalog，不新增图鉴服务或投影；
5. `ACK_COMPENDIUM_SEEN(observed_revision)` 使用单调 Fence，GET 不隐式清红点；
6. 活动归档和资源保持不可变引用；遗留种子按配置继续种植，缺失 Catalog 仅占位告警、不删除解锁事实；
7. 图鉴权威 Model 与 Player 共享 WriterClaim、RecordVersion 和 PersistenceLane；配置发布和页面分页受
   `CompendiumLimitProfile` 约束，具体编码与阈值仍待探针。
