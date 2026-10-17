# Friend Opportunity Realtime R0 真实代码审计

审计日期：2026-08-10（Asia/Shanghai）

审计范围：

- 后端 worktree：/data/home/developer_026/CodeBuddy/20260519141303/farm-game-friend-opportunity-realtime-ab-20260810
- 后端分支：codex/friend-opportunity-realtime-ab-r1
- 后端基线及审计时 HEAD：c395f3343d7006d32a834c307422e7b34df04a34
- H5 worktree：/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-opportunity-realtime-ab-20260810
- H5 分支：codex/h5-opportunity-realtime-ab-r1
- H5 基线及审计时 HEAD：f22dcb447e154e424656b12409e041c32dfb4c5c

本次直接审阅上述真实 worktree 的代码、测试和协议；未依赖 Windows 参考计划。开始审计时两个 worktree 均为 clean，未 checkout、提交或合入其他分支。本次唯一写入是本文件。

## 结论摘要

当前实现已经形成“Farm Authority Header/Facts + pull 查询 + Redis pull-only hint + Gate 本地下发”的基本闭环，读路径不会激活 Farm actor，且机会判断仍由 Farm 权威数据完成。H5 也有 30 秒兜底轮询、事件触发刷新、GET_NEXT request fence 和按成熟时间刷新。

但它还不是完整的可靠实时机会系统：

1. 30 秒不是后端机会查询定时器，而是 H5 在可见、已 Ready、无并发请求时的 fallback poll。后端的主动触发主要来自 Farm 变更后的 hint，hint 是 Redis Pub/Sub 的 best-effort pull invalidation，不是持久事件。
2. 当前 GET_NEXT 的生产路径仍是 FriendList 分页扫描；虽然 friend.Navigator 已实现候选窗口、粗读和 visited 逻辑，但 internal/friendshipservice/service.go 的 GetNextFriendOpportunity 没有使用它。
3. Header/Facts 与 FarmRecord 在同一 Farm model-state CAS 中落盘，Header/Facts 评估具备版本校验和逐行 UNKNOWN 降级；这是当前最可靠的 authority 接入点。
4. SessionRoute 已有按 Gate 的批量续租和 Redis pipeline，但没有发现权威的 SessionRoute 批读 API。Per-Gate DeliverBatch 已存在且带连接代际、Join/Target、Subscription fence。
5. H5 的主要一致性风险是 refreshReadyFriendAuthority 缺少与 FriendOpportunityFlow 同级的请求代际 fence：某个 Ready 刷新慢返回时，可能在较新的 Hint/Ready 触发本地撤销之后重新写回旧的 READY action/targets。

因此，R0 可以验证现有 pull、hint、Gate delivery 和 fail-closed 行为，但后续实现前需要明确：是否把 GET_NEXT 切到 Navigator/Authority 候选读、是否为成熟时间建立 level-triggered durable source、以及如何补齐 H5 Ready refresh 的 fence。

## 1. 后端机会查询与协议链路

### 1.1 真实 HTTP / internal RPC 路径

当前用户侧入口在 internal/gateway/auth/share.go：

- handleListFriends：GET /api/v1/friends，转发 friendship client 的 GetFriendList。
- handleFriendOpportunity：GET /api/v1/friends/opportunity，转发 GetNextFriendOpportunity；FOUND 返回候选，NONE 返回明确无候选，UNKNOWN 转为 503 FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE。

friendship client/server 的校验在 internal/friendshipservice/grpc_client.go、grpc_server.go 和 friendship_service.proto。internal/friendshipservice/service.go 的 GetFriendList 进入 internal/friend/read.go 的 ReadAPI.GetFriendList。

ReadAPI.GetFriendList 的当前性质：

- FriendIndex 只是可重建、可丢失的投影；canonical FriendPair 才是关系真相。internal/friend/roster.go 的 ListRoster 会做 revision fence、Pair revalidation 和 bounded refill。
- profile 使用一次 BatchGet；机会使用一次 BatchEvaluateFriendOpportunity；响应严格校验 cardinality/order。
- opportunity 行缺失、UNKNOWN 或 theft target/mask 不一致时按卡片降级为 UNKNOWN，只保留 CAN_VISIT，不把不确定数据当成可操作机会。
- cursor 绑定 viewer、sort 和 roster revision；revision 漂移会被拒绝/要求重新取页。

internal/friendshipservice/service.go 的 GetNextFriendOpportunity（约 346-384 行）目前会：

1. 读取 NavigationVisited 的当前集合；
2. 反复调用 ReadAPI.GetFriendList，单页上限为 friend.MaxPageSize（50）；
3. 跳过已 visited 的卡片；遇到 OpportunityReadUnknown 记下 unknown 并继续扫描；
4. 返回第一个 Actionable 卡片；扫描结束时若曾遇到 unknown 返回 UNKNOWN，否则返回 NONE。

这条路径没有调用 internal/friend/opportunity.go 的 Navigator.Next，没有使用 AuthorityOpportunityProvider 的批量 Header candidate 读，也没有为 public result 填充有实际语义的 navigation_generation。Navigator.Next 自身已经有候选窗口上限 8、扫描批量上限 32/最大 64、UNKNOWN 保留和按 ActionableAtMs 排序，但目前是“已实现但未接入生产 GET_NEXT”的旁路能力。

### 1.2 Header/Facts authority

核心模型在 internal/farm/domain/opportunity_read_models.go：

- FarmOpportunityHeaderV2：schema、SourceFarmVersion、NextStealableAtMs、NextWaterAtMs、NextPestAtMs、NextOpportunityAtMs；不携带 viewer 个性化结果。
- FarmOpportunityFactsV2：按 PlotID/CycleID 保存 MatureAtMs、RemainingTheftSlots、Water/Pest incident 到期时间、TheftVisitorIDs。
- DeriveFarmOpportunityReadModels 从 durable FarmRecord 和 typed receipts 确定性重建 Header/Facts；过期 cycle 跳过，active cycle、theft 数量、visitor 上限和 incident 约束均校验。
- EvaluateFarmOpportunityFacts 在 trusted now 下计算 viewer mask。成熟且仍有 theft slot 且 viewer 未在 TheftVisitorIDs 中才给 CAN_STEAL；Water 只在未成熟且 incident 到期时给 CAN_WATER；Pest 依据 incident 到期时间计算。
- opportunity revision 使用 Facts SourceFarmVersion，故机会版本可与 Farm authority 版本对齐。

写入和恢复在 internal/farm/actor/opportunity_models.go、actor.go、registry.go：

- deriveFarmOpportunityModels 直接从 durable record/receipts 重建，不信任可变缓存。
- saveWithCandidateAndOutcomeAndOrders 通过 CASEpochModelStateWithOutcomeAndOrders 将 FarmRecord、Header/Facts 和订单候选绑定在一次 CAS 流程中；没有“Farm CAS 成功后再补写 opportunity model”的弱窗口。
- V1 到 V2 的转换有 writer-fenced CAS；V2 partial/mixed 或 source version 不一致会 fail closed。缺失 legacy model 不是“无机会”，而是 unavailable/unknown 语义。

只读服务在 internal/farm/readpath/authority.go：

- BatchReadHeaders 一次读取最多 64 个 farm 的 opportunity_header，检查 exact cardinality、schema 和 V2 decode。
- BatchEvaluate 一次读取 Header + Facts，检查 Header/Facts source version 相等，然后调用 domain 的纯 EvaluateFarmOpportunityFacts；store error、缺失、坏数据均按行给 Unknown/NotFound，批次结构错误才整体失败。
- internal/farm/ingress/friend_opportunity.go 的 BatchEvaluateFarmOpportunity 通过 service-LB RPC 暴露该能力，明确 never activates actor；每行结果保持输入顺序。

internal/friend/opportunity_authority.go 和 grpc_opportunity.go 将 authority provider 接入 Friend ReadAPI。grpc 层对 theft target 的 plot 范围、cycle 不透明性、重复项和 mask/target 一致性做 decode 校验，坏行降为 UNKNOWN。

### 1.3 GET_NEXT 的语义和当前缺口

协议已经区分：

- public/commands.proto：FOUND/NONE、candidate、candidate action mask/status/revision/validated_at_ms 和 navigation_generation。
- friendshiprpc/friendship_service.proto：internal FOUND/NONE/UNKNOWN；FOUND 必须带 READY 且 Actionable card，NONE/UNKNOWN 不能带 card。

当前服务正确保留了 UNKNOWN 与 NONE 的区别，并且 internal/gateway/auth/share.go 将 UNKNOWN 变成暂时不可用，而不是错误地显示“没有机会”。缺口是查询效率和实现分层：GET_NEXT 逐页调用完整 FriendList，不能利用已经写好的 coarse Header candidate reader、候选窗口和按批 exact evaluation。若后续接入 Navigator，仍必须在最终返回前保留 Pair canonical revalidation、profile batch、visited fence 和 action target 校验，不能把投影候选直接当作授权结果。

## 2. Hint producer / subscriber / 下发

### 2.1 Producer

internal/farm/ingress/service.go 的 EmitChange 在成功变更路径上调用 normal emitChange；FarmVersion 大于 0 时再以约 100ms context 调用 FriendOpportunityNotifier.PublishFriendOpportunityHint。public action 的 CARE、THEFT、WATER、PEST 等变更会经过该路径；participant_server.go 只观察 APPLIED、非 replay 且带 Change 的 action，并覆盖 plot unlock、plant、harvest、theft、guard snapshot 等相关变更。

internal/friend/opportunity_hint_bus.go 的 PublishFriendOpportunityHint 只发布 owner、SourceFarmVersion、OpportunityRevision。消息不携带 viewer action mask，消费方必须再次 pull authority。FanoutFriendOpportunityHint 根据 owner 的 bounded FriendIndex 找 viewer，过期/缺失 index 只意味着额外 pull，不改变 authority。

当前没有看到“仅因 trusted time 到达 MatureAtMs/incident DueAtMs 而产生 hint”的 producer。Farm 变更 hint 只能覆盖有持久 Farm action 的变化；成熟时间本身不产生 FarmVersion 变化，H5 依靠 maturity timer 和 30 秒轮询补偿。

### 2.2 Subscriber 和多 Gate

internal/gatewayapp/friendship.go 创建 Redis hint bus，并为每个 Gate 启动 RunOpportunityHintSubscriber。Redis channel 为 classic-farm:<env>:<zone>:friend-opportunity-hint-v1；Subscribe 对坏 payload 忽略并支持重连，FanoutFriendOpportunityHint 再调用 gateway/downlink/service.go 的 ConnectionRegistry.NotifyFriendOpportunityChanged。

NotifyFriendOpportunityChanged 只向当前 Gate 上该 player 的本地连接入队 player push；没有集中式 SessionRoute 查询。因为每个 Gate 都订阅 Redis、只扫描自己的本地连接，所以多 Gate 的 fanout 依赖“每 Gate 都消费同一 hint”，而不是单点路由。

gateway/commandcatalog/pushspec.go 已注册 FRIEND_OPPORTUNITY_CHANGED。internal/farm/ingress/friend_opportunity_test.go、internal/friend/opportunity_hint_bus_test.go 和 internal/gateway/downlink/service_test.go 覆盖 payload、bounded friend index fanout、target player 和不携带 action mask。

### 2.3 Hint 的可靠性边界

这是 pull invalidation，不是可靠队列：

- Redis Pub/Sub 丢消息不会进入 durable retry；EmitChange 忽略 publish error，hint 失败不阻塞 Farm Ready/动作结果。
- subscriber 没有 owner/farm 的 last-seen revision 去重或单调性检查；同一 hint 重复消费主要由 H5 再 pull 来收敛，乱序旧 revision 仍可能触发多余刷新。
- 没有离线玩家的持久 fanout；玩家回到 H5 后由 bootstrap、打开好友面板、可见性恢复和 poll 补偿。

若 R0 目标只要求“hint 触发一次 pull，权威结果 fail closed”，现状满足。若目标要求不丢事件、成熟时自动唤醒或跨 Gate 可证明送达，现状不满足。

## 3. DueIndex / Order / Claim / Lease / Backoff 和可复用组件

通用工作流在 internal/workflow：

- workstore.go 定义 WorkRecord、WorkVersion、DueIndex、LeaseOwner/Epoch/Token、ClaimDue、RenewLease、PendingDue 和原子 Advance contract。RedisWorkRepository 用 Lua + Redis TIME 在 record 与 due index 之间做 CAS，claim 时把 due score 移到 lease expiry。
- backoff.go 提供 250ms 起、30s 上限、可选 jitter 的指数退避。
- recoverer.go 的 RecoveryLoop 按 typed work type 有界 claim，记录 claim timing，按 accepted lease TTL 减 safety margin 运行 Resume；不在恢复阶段创造新的业务意图。
- dispatcher.go 是 durable WorkRecord/DueIndex 之后的 bounded fast path；队列满是 backpressure，不能替代 durable source。

internal/worker/theftrecovery 是可参考的完整模板：PairDispatchRepository 从 Pair DueIndex 做有界 discover；DispatchOrder 固化 source order/payload hash/permit/admission/deadline；WorkLeaseRepository 做 exact source proof、lease fence、renew、complete/reschedule；receipt-first 处理响应丢失和重试。cmd/farm-worker/main.go 还表明 Plant/Unlock/Harvest 共用 generic Work DueIndex，而 theft worker 使用独立的 friend-pair family。

不应直接复用 theftrecovery 的 FRIEND_PAIR DueIndex、DispatchOrder 或 WorkLease key 来做 opportunity hint/maturity notification。前者是不可变动作 dispatch/settlement order，后者是读模型失效和 pull 触发；混用会把 action order、permit、claim 语义污染到机会通知。若后续需要持久化机会事件，应新建明确的 work family/key/source order，或在 generic workflow 上定义专门的 opportunity notification work。

相关验证路径：

- internal/workflow/workstore_test.go、rediswork_test.go：原子 create/advance、CAS、claim/reclaim、renew、typed claim。
- internal/workflow/recoverer_test.go、dispatcher_test.go 及 crash/race 测试：恢复、backoff、bounded dispatch。
- internal/worker/theftrecovery/friendpair_test.go、recovery_test.go、discovery_test.go：Pair DueIndex、旧 lease、receipt-first、重启 reclaim、坏 source 和分页/bucket。
- internal/farm/actor/model_field_v2_test.go、registry_model_v2_runtime_test.go：V1/V2、partial/mixed 数据、迁移、crash/restart、deterministic encoding。

## 4. SessionRoute 与 Per-Gate DeliverBatch

### 4.1 SessionRoute 现状

internal/gateway/session/authority.go 的 RouteFence 是 GatewayID、GatewayBootID、ConnectionID、ConnectionGeneration；ActiveRoute 还带 PlayerID、SessionID、SessionEpoch、RouteRevision、LeaseExpiresAt。Route lease 默认 TTL 为 30 秒。

当前 authority interface 只有 RenewRoutes(ctx, routes, leaseTTL) 形式的批量续租，没有发现 BatchRead/GetRoute/BatchReadSessionRoute 的权威批读 API。RedisAuthority.RenewRoutes 在 authority_redis.go 中用 Pipeline 把每条 route 的 Lua renew 放进一次网络往返，并逐项解析混合结果；这属于批量续租/写入，不是批量读当前路由。

internal/gateway/command/gate.go 的 renewSessionLeaseBatch 由 leasebatch.Planner 驱动：每个 Gate/boot holder 隔离、按 item/bytes 有界、校验完整 route fence、处理 mixed renewal、transient backoff 和 lease deadline。leasebatch/planner.go 提供稳定 phase/bucket/jitter 和 O(log N) Upsert/Remove。它解决的是 Gate 本地续租调度，不提供跨 Gate 的 SessionRoute resolve。

若后续需求是“按 player/session 批读 route 再决定投递 Gate”，当前接入点不是 RenewRoutes；需要在 session authority 新增带 route fence/epoch/revision 的只读批量 API，并明确 expired、missing、transient 的逐项语义。

### 4.2 DeliverBatch 现状

已有两层 batch：

1. internal/logic/downlink/router.go 的 RoutedGatewayClient.DeliverBatch 按 item.GatewayId 分组，每个 Gate 一次 RPC，再按原请求顺序映射结果；Gate 缺失/不可用逐项返回 UNAVAILABLE。
2. internal/logic/downlink/dispatcher.go 为每个 Gate 建独立 lane，含普通和 reserved control 队列，按 32 items、512 KiB 等边界收集，限并发、有限次 Unavailable retry 和总预算；business item status 不重试。

Gate 侧 internal/gateway/downlink/service.go 的 ConnectionRegistry.DeliverBatch 对每项检查 item ID、connection/generation、Join/Target fence、subscription fence、dedup 和 writer capacity，返回 QUEUE_ACCEPTED、STALE、QUEUE_FULL 或 UNAVAILABLE。相关测试覆盖 Gate 分组、坏 Gate 不阻塞健康 Gate、FIFO callback、reserved control lane、transient retry、旧 generation、dedup 和 push fence。

这套能力可以直接承载 FRIEND_OPPORTUNITY_CHANGED 的 per-Gate delivery；现有 hint 路径实际使用的是本地 ConnectionRegistry.Enqueue，未发现需要另造一套机会专用下发通道。

## 5. H5 Popup、轮询、Hint、request fence、maturity refresh

### 5.1 Popup 与机会来源

主要实现位于 src/app/App.ts、src/friends/game-friend-service.ts、src/friends/opportunity-presentation.ts：

- App template 中有隐藏的 .friend-opportunity-card，按 friend owner 显示推荐；点击进入 visitOwner，不在点击时重复发起 GET_NEXT。
- loadFriendOpportunity 调用 GET /api/v1/friends/opportunity；FOUND 必须是 READY 且包含 Theft/Water/Pest 任一服务端 action bit；NONE 和 UNKNOWN 分开处理。
- loadFriendRoster 的好友页 authority 是 fallback：当 GET_NEXT 没有可展示推荐时，从已取到的好友页选择第一个 H5 支持的 exact opportunity。
- opportunity-presentation.ts 当前 H5 只支持 Theft、Water；PEST 服务端能力存在，但因为没有 remove-pest UI/command，故有意不广告。UNKNOWN 永远不会被合成为 action gain。
- game-friend-service.ts 的 parseFriendSummary 校验 owner/profile/relation、mask 范围、status、validated time、theft target 形状、plot/cycle 唯一性和 theft bit/target 一致性；HTTP status 不会被吞掉。

### 5.2 30 秒和触发刷新

src/app/friend-roster-refresh-policy.ts：

- FRIEND_ROSTER_FALLBACK_POLL_MS = 30_000；
- 仅在 service 存在、好友数大于 0、WS 状态为 InFarm、requestsInFlight 为 0 且页面可见时允许 poll；
- poll 的定位是补偿 missed hint，不是 authority 的唯一来源。

App.ts 的触发点包括 bootstrap/login、好友面板/可见性恢复、friend-roster-changed、FRIEND_OPPORTUNITY_CHANGED，以及 subscription-ready。Hint 处理先用目标 owner 的本地状态撤销 action bits、清空 targets/recommendation，再 loadFriendRoster，成功后按当前代际再 loadFriendOpportunity；这部分是 fail-closed 的正确方向。

### 5.3 request fence

friend-opportunity-flow.ts 用 requestGeneration：

- beginRequest/isCurrentRequest/invalidateRequests/reset；
- GET_NEXT 的 stale response 被丢弃；
- explicit navigation、reset 和新 hint 会使旧推荐失效；
- friend owner 只有在 SUBSCRIPTION_READY 后才 MarkVisited；own farm Ready 会消费一次“刚访问好友” marker 触发下一次机会查询。

App.ts 的普通分页 loadFriendRoster 还有 friendRosterRequest counter 和 revision fence；roster-page-refresh.ts 的策略确保同 revision 的 silent refresh 保留后续页，不把 cursor rewind 成第一页。

明确缺口：refreshReadyFriendAuthority 在 App.ts（约 2812-2847 行）内部按 cursor/revision 扫描好友页，但没有接收/捕获 FriendOpportunityFlow requestGeneration，也没有自己的 authority refresh generation。它可能在较新的 FRIEND_OPPORTUNITY_CHANGED 或另一轮 subscription-ready 已将 owner 降级为 UNKNOWN 后，仍以慢返回的旧 READY 数据写回 action/targets。当前 error 分支会保持 UNKNOWN，但成功的旧响应存在重新启用旧数据的窗口。

另一个边界是 hint revision 目前主要用于触发和本地元数据；subscriber/H5 没有通用的 per-owner monotonic last-seen 去重。旧 hint 通常只导致额外 pull，正式 pull 失败时 action bits 仍保持撤销，但 revision 元数据可能回退。

### 5.4 maturity refresh

App.ts 的 scheduleMaturityRecompute 使用 serverNow 计算下一次 matureAt/incident 到期，timer 到点时：

- 若当前可见 owner 是好友，则 invalidateAndRefreshFriendAuthority，再读当前好友的权威结果；
- 否则只重绘本地投影；
- scheduleFarmClockProjection 每秒只负责倒计时/时钟展示，不改变 authority。

这补偿了成熟时间没有 FarmChange/hint 的事实。friend-roster-refresh-policy.test.ts 也明确只对当前好友安排 maturity refresh；不对所有离线好友 fanout。

## 6. 已实现、缺口和潜在冲突清单

| 领域 | 已实现 | 缺口/冲突 |
| --- | --- | --- |
| Authority | Header/Facts 同 Farm model-state CAS；trusted-now 纯评估；版本和坏数据 fail closed | matureAt/DueAt 到时不自动产生后端 hint |
| Friend list | Pair canonical revalidation；profile/opportunity batch；cursor/revision fence | GET_NEXT 没接 Navigator/粗 Header candidate，仍逐页完整扫描 |
| GET_NEXT | FOUND/NONE/UNKNOWN 严格区分；visited 在 SUBSCRIPTION_READY 后写入 | navigation_generation 没有形成实际 end-to-end fence |
| Hint | Redis producer/subscriber；pull-only payload；每 Gate 本地 fanout | Pub/Sub 非 durable；无重放、持久 retry、单调去重 |
| Poll | H5 30s 可见/Ready/无并发请求兜底 | 轮询是补偿机制，不能证明实时或不丢事件 |
| H5 fence | GET_NEXT Flow、普通 roster counter、HTTP/shape 校验 | refreshReadyFriendAuthority 成功旧响应可越过新触发重写状态 |
| Maturity | 当前可见好友按 serverNow timer refresh | 非当前好友只等 poll/后续 bootstrap；没有后端 level-triggered maturity event |
| SessionRoute | 30s lease、按 Gate planner、Redis pipeline renew | 未发现 SessionRoute 权威 batch read/resolve |
| Downlink | RoutedGatewayClient 分 Gate；Dispatcher lane/retry；Gate DeliverBatch fences | 机会 hint 当前不需要新增专用 delivery；若要中央路由需先补 route read |
| Worker | generic Work DueIndex/lease/backoff/recovery；theft 完整模板 | theft pair order/lease 不适合机会读失效，不能直接复用 |
| H5 action surface | Theft/Water 展示与 exact target 逻辑 | PEST 是 server-ready 但 H5 intentionally unsupported，产品接入前不能误显示 |

## 7. 后续实现的精确接入点

按风险和复用边界，建议后续实现从以下位置进入：

1. GET_NEXT 优化：以 internal/friendshipservice/service.go:GetNextFriendOpportunity 为切换边界，接入 internal/friend/opportunity.go:Navigator.Next 和 internal/friend/opportunity_authority.go:AuthorityOpportunityProvider。Navigator 输出只能作为候选；最终仍要走 FriendPair canonical truth、profile batch、visited fence、BatchEvaluate target 校验，并保留 UNKNOWN 不等于 NONE。
2. 机会变更 producer：保持 internal/farm/ingress/service.go:EmitChange 只发布 owner/version 的 pull hint；若要覆盖成熟时间，新增独立的 level-triggered source/worker，在 trusted now 到达后按 Farm header/version 产生可重试通知。不要把成熟时刻伪造成客户端本地 action mask。
3. Hint 顺序与去重：在 internal/friend/opportunity_hint_bus.go:Subscribe 或其 handler 入口增加 owner/farm/revision 的 last-seen/monotonic 规则；旧消息可以丢弃，最新消息只触发一次 pull。若要求跨离线可靠性，应先选新的 durable Work family，再接 workflow/recoverer。
4. H5 Ready refresh fence：以 App.ts:refreshReadyFriendAuthority 为最小修改点，为每次 owner authority refresh 捕获 generation，并在每个 page response/apply 前校验 generation、owner、expected roster revision；Hint、subscription-ready、explicit navigation、reset 都必须使该 generation 失效。不要只依赖 requestsInFlight，因为它没有“哪一轮拥有写回权”的语义。
5. H5 maturity：继续复用 scheduleMaturityRecompute/invalidateAndRefreshFriendAuthority；若后端增加成熟 hint，仍先本地撤销再 pull，不能直接根据 hint 恢复 action。
6. 多 Gate delivery：继续复用 internal/logic/downlink/router.go:RoutedGatewayClient.DeliverBatch、dispatcher.go 的 per-Gate lane 和 internal/gateway/downlink/service.go:ConnectionRegistry.DeliverBatch。只有在需要按玩家查找非本地 Gate 时，才在 internal/gateway/session authority 新增 BatchRead/Resolve API，并沿用 RouteFence/SessionEpoch/RouteRevision。
7. 持久异步机会通知：若确认需要 durable claim/lease/backoff，应新建 opportunity notification 的 WorkType、source order 和 due key，复用 internal/workflow 的原子 WorkRecord/DueIndex/ClaimDue/RenewLease/RecoveryLoop contract；不要引用 worker/theftrecovery 的 FRIEND_PAIR dispatch identity。

## 8. 测试与验证记录

已执行的只读、短时现有测试（均通过，未启动长期服务）：

1. 后端机会/authority/读路径：

    go test ./internal/friend ./internal/farm/domain ./internal/farm/readpath ./internal/farm/ingress ./internal/friendshipservice -run 'Test(EvaluatePageExactForAllCards|Navigation|OpportunitySummary|FriendRead|AuthorityOpportunity|FriendOpportunityRPC|OpportunityHintBusPayload|EmitChangePublishes|BatchEvaluateFarmOpportunity|FarmOpportunityReadModels|ProviderDecodes|ProviderReturns|AllFriendshipRPCMethodMappings|FriendshipRPCPreservesUnknown)' -count=1

    结果：internal/friend、internal/farm/domain、internal/farm/readpath、internal/farm/ingress、internal/friendshipservice 全部 ok。

2. SessionRoute、lease planner、Gate/Downlink：

    go test ./internal/gateway/session ./internal/gateway/leasebatch ./internal/gateway/command ./internal/gateway/downlink ./internal/logic/downlink -run 'Test(BeginRaisesEpoch|KickInvalidatesOldPermit|ActivationAndRenew|ExpiredRouteLease|Planner|RenewSessionLeases|RoutedGatewayClient|Dispatcher|DeliverBatch|ConnectionRegistry)' -count=1

    结果：上述五个 package 全部 ok。

3. Farm model V2、workflow、theft recovery：

    go test ./internal/farm/actor ./internal/workflow ./internal/worker/theftrecovery -run 'Test(FarmV1ToV2|FarmV2|FarmCommandCarrierDirtyMatrix|FarmAuthorityRuntime|SameInputFarmV1V2|MemoryWorkRepository|RedisWorkRepository|RecoveryLoop|WorkVersionMismatch|DirectDispatcher|TheftRecovery|RedisPairDiscovery|FriendPairRecovery|PairDueObservation)' -count=1

    结果：上述三个 package 全部 ok。

H5 测试未执行：远端 H5 worktree 的 package.json 使用 Vitest（test: vitest run，build: tsc --noEmit && vite build），但服务器无 Node/npm（node --version 返回 bash: node: command not found）。因此未启动 e2e，也未把未运行误报为通过。应在有 Node 的环境执行 src/test/friend-opportunity-flow.test.ts、friend-roster-refresh-policy.test.ts、friend-roster.test.ts、roster-page-refresh.test.ts、opportunity-presentation.test.ts、final-protocol-adapter.test.ts，以及 src/test/e2e/candidate-opportunity-theft.spec.ts。

本次未修改 Go、TypeScript、Proto、配置、其他文档；未提交、未合入 Integration。
