# SO0-FRIEND-AUTHORITY 交接

- contract_baseline：docs/design @ 4e58793（26 v1.3 / 27 v4.2 / 28 v2.8 / 29 v2.11 / 30 v1.0 / 31 v1.4 / 32 v0.29 / 33 v1.2 / 33A v0.3 / 40 v1.1；CONTRACT-MANIFEST 2026-08-03）
- code_baseline：4e587936df183dd7e6e7725200f98dec38e69d5d（codex/so0-friend-authority-20260803，开工时 `git status` clean；基线 `go test ./...` 全绿、`go vet ./...` 无输出）
- owned_write_set：internal/friend/** 中 Friend Pair/Quota/ShareIssuer 纯权威逻辑、Repository port/内存实现、Permit/Opportunity 查询核心与测试；docs/verification/so0-friend-authority-handoff.md
- forbidden_write_set：PlayerActor、FarmActor、farm/subscription、Proto/generated、公共 Redis KeyBuilder/Lua 框架、Mail、cmd、H5、docs/design（本分支零越权：未触碰任何一项）
- dependencies：SCB-1（`internal/workidentity` StableOpaqueID、`internal/contracts`，只读消费）；R1 公共 Redis 接口未合入，本任务只冻结领域模型 + Repository port + 内存/合同测试
- status = COMPLETED（DOMAIN_LOGIC_PASS；Redis/多进程项 Pending，见下文）

## 1. 现状审计（复用/缺失/冲突矩阵）

| 类别 | 内容 |
|---|---|
| 复用 | `workidentity.StableOpaqueID`（33A §2.7 六条黄金向量全部逐字通过，ids_test.go）；`CanonicalPair/ValidPair`；既有 `Store`/`MemoryStore`/`RedisStore`/`PermitService` 全部保留未改，gateway/auth、farm/ingress、gatewayapp、cmd 四个消费方零改动 |
| 缺失（本任务补齐） | canonical Pair 权威（Relation+双向Block+ReactivationBarrier 共置，pair.go）；Quota Reservation/Finalize/ReleaseFence（quota.go）；ShareIssuer 状态机与 ValidationReceipt/RotateReceipt（share_issuer.go）；AddFriend 兑换编排与 TerminationOrder 领域执行器（service.go）；VisitPermit/ActionPermit 分离（permit_core.go）；FriendIndex 分页与 roster revision cursor（roster.go）；Summary/BatchEvaluate/连续导航 visited 核心（opportunity.go）；三类 Repository port 与内存适配器（repository.go、memory_authority.go） |
| 冲突（记 ICR，不在本分支重写） | 既有 `internal/friend/redis.go`：单 hash tag `{friend}` 把 relation/index/receipt 混在同一 Slot、无 Pair/Quota/ShareIssuer 三类 Authority 拆分、无 ReactivationBarrier、无 relation_epoch 绑定的 Quota、无 ValidationReceipt/RotateReceipt、无 CommitAttempt Fence、无 SourceOrder/DueIndex，不满足 33A §2/§3/§4/§5/§6。见 §5 ICR-SO0-01 |

## 2. 已冻结的领域语义（对照任务必须完成项）

1. **canonical FriendRelation 唯一真值**：`PairRecord` 共置 Relation + 双向 Block + Barrier；`FriendIndex`/`FarmOpportunitySummary` 均为可重建副本（`RebuildFromCanonical` 测试证明）；Permit 只经 `GetPair` 线性化读签发，绝不依赖索引（roster_test/opportunity_test）。
2. **Relation 状态机**：邀请（RedeemShare）/接受（Activate）/删除/拉黑/解除拉黑/重加 barrier 全覆盖；无 TTL 自动失效；`relation_epoch` 单调递增；旧 epoch Permit、迟到 Activate、迟到 TerminationOrder 全部 fail closed（pair_test、permit_core_test、concurrency_test）。
3. **分享链接多人使用**：同一 Token 100 并发兑换 = 100 个独立 pair 全部成功、无全局次数计数；同一兑换者重复 operation 幂等重放；300 上限并发 Reserve 绝不出现第 301 个；自加/重复/拉黑/并发兑换均为确定终态。
4. **VisitPermit 与 ActionPermit 分离**：`AuthorizeVisit` 只做 Join 预检（绑定 join_operation/attempt、epoch、TTL 10s）；`CommitFriendActionAdmission` 逐 operation 原子准入并冻结 `ActionPermitSnapshot`；恢复时只校验 `execution_accepted_at_authority_ms` 窗口，不按当前墙钟重新过期。
5. **Summary 只粗筛**：`EvaluatePage` 对当前页全部卡片做 BatchEvaluate；Summary 误报被精确检查排除；超时卡片返回 UNKNOWN 不伪造 NONE；`next_opportunity_at` 补偿 Summary 滞后。
6. **连续导航**：`Navigation` 维护本轮 visited（有界 512）、scan cursor、≤8 候选窗口；`MarkVisited` 只接受带 SubscriptionID/session_epoch 的服务端 fence（SubscriptionReady 即视为访问，不要求操作成功）；回自己农场后 `Next` 排除已访问并继续扫描；roster revision 变化重置扫描但保留 visited。
7. **在线 Hint 只加速**：核心无任何按"农场成熟 → 全部离线好友"的持久扇出结构；个性化 TheftAttempt 不进入 FriendRelation（由 Farm 权威持有，本包不出现）。
8. **有界/可分页/可重建**：FriendIndex 默认 20/硬上限 50、cursor 绑定 roster revision、有界 refill budget（3×page）；导航窗口 ≤8、扫描批 32~64；不新增 FriendActor（DomainService 无状态，权威在 Repository）。

## 3. DOMAIN_LOGIC_PASS 清单

| 项 | 证据 |
|---|---|
| 33A §2.7 StableOpaqueID 黄金向量 ×5 + CommitIdentity kind 防碰撞 | ids_test.go |
| Pair 生命周期表驱动（删除/拉黑/解除/自加/重复/陈旧 epoch/barrier 全周期 PENDING→READY→CONSUMED、proof 只在 READY 发放） | pair_test.go |
| Quota Reserve/Finalize/ReleaseFence/迟到 Finalize/旧 epoch 释放不删新 epoch/硬上限/grandfathered overflow/64 并发×500 请求不超 300 | quota_test.go |
| ShareIssuer 生成复用/Rotate/Revoke/过期/多人复用/自加拒绝/幂等重放/RotateForPairReactivation 绑定 barrier_revision、普通 Rotate 不满足 proof | share_issuer_test.go |
| RedeemShare 编排：happy path 双方名额 epoch=1、重放点重放、ALREADY_FRIENDS、第三人独立兑换、payload 冲突、删除→PENDING 拒绝→TerminationOrder 回填 floor 释放双方名额→旧 token 拒绝→显式 Rotate→重加 epoch=3 且 barrier CONSUMED、拉黑立即拒绝 Admission、第二方满员释放第一方 reservation | service_test.go |
| VisitPermit 签发/绑定/TTL 过期/删除后陈旧 epoch fail closed；ActionAdmission 提交/重放/GetFriendActionAdmission 解析 Unknown/冲突/删除后旧 operation 可恢复完成但新 operation 拒绝/畸形 Permit 拒绝 | permit_core_test.go |
| 分页稳定性（45 人三页无重无漏、稳定排序）、revision 变化旧 cursor 失败、跨 viewer cursor 拒绝、3 条陈旧索引过滤+补位、可重建、旧 termination 事件不删新条目、cursor opaque 往返 | roster_test.go |
| Summary 误报被 BatchEvaluate 排除、当前页全部精确检查、UNKNOWN 不隐藏、visited 连续导航+fence 幂等+H5 无法伪造 fence、roster 变化重置扫描保留 visited、时间补偿 | opportunity_test.go |
| 100 并发兑换同一 Token、双方并发加删终态一致（quota 与 canonical 对账）、拉黑与激活竞态 20 轮、旧 epoch Permit 在重加后仍 fail closed、公开 mutation 重放/冲突/NO_CHANGE | concurrency_test.go |

测试原始结果：

```text
$ go test -race -count=1 -timeout=120s ./internal/friend/...
ok  farmgame/internal/friend  1.069s
$ go test -count=1 -timeout=180s ./...
全部 ok（EXIT=0，无失败输出）
$ go vet ./...
（无输出）
$ git diff --check
（无输出）
```

## 4. Pending（R1/多进程项，本任务明确未做）

| 项 | 阻塞依赖 | 后续工作包 |
|---|---|---|
| 三类 Authority 的生产 Redis Adapter（Key/Hash Tag/Lua/ScriptRegistry/EVALSHA/NOSCRIPT） | R1 公共 Redis 接口未合入 | R1 稳定后由 SO0 后续或 SO1 按 33A §4/§5/§6 实现，只换 Adapter，不改本分支 port 语义 |
| CommitAttemptCursor / FenceAndResolve Unknown 物理 Fence | 同上（33A §3.3/3.4） | 随 Redis Adapter 一并落地；本分支已在领域层冻结"重放同 CommitIdentity/不同 payload 冲突"的决策语义 |
| FriendTerminationOrder 的 Hub Subscription 撤销步骤（AUTHORIZING/JOINING/…/IN_FARM 按 relation_id+captured_epoch 精确撤销） | farm/subscription 属 forbidden_write_set | SO1 + FI1 接缝；本分支 `ExecuteTerminationOrder` 只覆盖 floor 回填 + 双方 quota 释放 + 订单完成 |
| FriendActionDispatchOrder 的 DirectDispatcher/farm-worker 恢复、ResultPublishOrder → ResultDirectory | Workflow/worker 框架与 Projection 写方 | WF1/SO1；本分支已冻结 DispatchOrderID 派生（33A §4.8）与内嵌不可变 Permit 语义 |
| Token Envelope 密码学封装（AEAD/KMS/Keyring）、日志脱敏采集 | SECURITY_PLATFORM_PENDING（33A §6.2/§17.3） | 安全评审后由 SO1 接入；本分支命令输入按"已认证 envelope claims"建模 |
| AddFriendWorkflow 持久化（INDEPENDENT_WORK_BUCKET）与崩溃后续跑 | 通用 Workflow Store（WF1） | WF1；本分支 DomainService 终态 outcome 存储仅建模 Root 作用域 StoredOperationOutcome，进程内实现 |
| Activate DOMAIN_EPOCH_CONFLICT 后按原 Workflow 重读重试（当前返回确定 REJECTED 由客户端换 operation 重试） | WF1 恢复语义 | SO1；33 §5.2 允许"拒绝/重读并补偿"，本分支选择了拒绝+补偿的确定终态 |
| Receipt 生命周期 Full→Tombstone→Expired Evidence、bucket 容量计数、ACL/慢日志脱敏 | R1 物理层 | 随 Redis Adapter 落地 |

## 5. ICR 登记

**ICR-SO0-01：既有 `internal/friend/redis.go` 不满足 33A，需整体替换为 R1 适配器**

- 问题：现有 RedisStore 用单一 `{friend}` hash tag 混部 relation/index/receipt 三类 Key；无 Pair/Quota/ShareIssuer 分桶；无 Barrier/ReleaseFence/ValidationReceipt/RotateReceipt；无 CommitAttempt Fence；好友上限用 SCARD 裸计数（无法按 epoch 幂等释放）；index 与关系同 Lua 写（33 要求索引只是可重建投影，删除不能等索引）。
- 合同依据：33 §2/§3/§4/§5、33A §2/§4/§5/§6/§9、33A-14。
- 影响：生产 Redis 路径在 R1 适配器落地前不满足合同；本分支未改 redis.go（ forbidden 自创第二套公共原语），新增领域代码与旧 Store 并存且互不依赖。
- 建议：R1 合入后新增 `internal/friend/redis_authority.go` 实现本分支 `PairRepository/QuotaRepository/ShareIssuerRepository`，旧 `RedisStore`/`Store` 由 SO1 评估下线或保留为 legacy 兼容层；33A §9.5 ScriptRegistry 按 Redis Node 维护加载状态。
- 兼容性/测试向量：本分支内存适配器 + 合同测试即 Adapter 的验收基线；33A §2.7 黄金向量已锁定。

## 6. R1/AR1 后续接线接口

- R1 提供：通用 KeyBuilder/Slot 断言、ScriptRegistry、CommitAttempt Cursor/Resolution 原语、DueIndex/Receipt GC 原语。
- SO0 提供（本分支已冻结，R1/AR1 直接消费）：
  - `PairRepository / QuotaRepository / ShareIssuerRepository`（repository.go）：每个 mutating 方法 = 一个 33A Lua 的逻辑等价物，R1 Adapter 逐方法映射即可；
  - `DomainService.RedeemShare/DeleteFriend/BlockPlayer/UnblockPlayer/ExecuteTerminationOrder`（service.go）：SO1 的 AddFriendWorkflow/TerminationOrder worker 的步骤编排蓝本；
  - `VisitAuthorizer.AuthorizeVisit/BindCheck`（permit_core.go）：SO1 好友 Join AUTHORIZING 接缝的授权/绑定检查；
  - `ListRoster/EvaluatePage/Navigator`（roster.go/opportunity.go）：FriendReadAPI 与 GET_NEXT_FRIEND_OPPORTUNITY 的查询核心，farm 侧 SummaryReader/ExactEvaluator port 已由本分支定义。
- AR1 注意：好友三类 Authority 是非 Actor 权威，不接 WriterClaim/Holder；AR1 的 Actor 主链接线不应把 FriendRelation 并入 PlayerActor。

## 7. 提交信息

- commit：9501ad87422600c6fda5531795e5aa00e1bca63d（本文件回填 hash 见后续小提交）
- push：origin/codex/so0-friend-authority-20260803
- 回滚方式：`git revert <commit>`；新增文件与既有代码零耦合，revert 不影响其他包。
