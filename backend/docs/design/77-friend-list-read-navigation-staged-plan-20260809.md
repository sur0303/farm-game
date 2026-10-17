# 77 — 好友列表读路径与连续互动导航分阶段实施计划（2026-08-09）

Status: REVISED / STAGED; only F1, F2 and SCB-79 user-visible correctness work are in the current candidate.

> 编号说明：本计划保留用户指定的 #77。原先误占 #77 的精确偷菜目标 SCB
> 已改编号为 `79-friend-exact-theft-target-scb-20260809.md`；物理批读 SCB
> 继续使用 #78。

## 1. 已验证的物理现状

发布基线 `808b4a8` 的好友页由三条读链组成：

1. PublicProfile：按 Player Holder 合并为 Batch RPC，但 Holder 内最多八路
   `Registry.Read`，读取完整 PlayerRecord，并进入每玩家串行执行器；
2. Pair：ReadAPI 逻辑批量，Redis 实际仍为最多八路 `GetPair`，每条执行
   canonical record 与 bucket envelope 的多次 Redis 读取；
3. FarmOpportunity：一次批量 RPC，底层已用 Redis Pipeline 读取与 FarmRecord
   同 CAS 的 Header/Facts，逐 farm 的缺失、损坏或超时保留为 UNKNOWN。

FriendIndex 只负责候选投影。Pair/Block/epoch、Permit 与 FarmActor 始终是访问和
写操作权威。Hint 只是失效通知，不携带 action mask、精确目标或授权。

## 2. 当前候选范围

### F1 — Player PublicProfile 紧凑模型（当前候选）

- 每次 Authority Player 创建和变更都在同一 RecordVersion CAS 写入
  `model:record + model:public_profile + terminal outcome`；
- Player Holder 对新记录只执行一次 `BatchReadModelFields(public_profile)`，不进入
  Registry mailbox；
- 旧记录缺少投影时，第二次物理批读直接读取 `model:record` 并派生同一公开视图，
  同样不进入 Registry mailbox。下一次 Player 变更自然补写紧凑投影；
- 正式“紧凑字节量”性能验收仍需先完成存量回填或证明旧记录占比已降至门限；
- 运行时回滚：`FARM_FRIEND_PUBLIC_PROFILE_MODEL_READ_ENABLED=0` 恢复旧
  Registry 兼容路径，不改变数据格式。

### F2 — Redis Pair 真 Pipeline（当前候选）

- `RedisPairRepository.BatchGetPairs` 在一次 Pipeline 中读取全部 canonical Pair；
- 同一物理 bucket 的 meta、receipt/attempt/pending cardinality 只读一次；
- TYPE、固定 manifest、HLEN、bucket identity/counters、Pair identity、Relation、
  Block 和 termination/reactivation 校验与 `GetPair` 等价，任一损坏使整页失败；
- 运行时回滚：`FARM_FRIEND_PAIR_BATCH_ENABLED=0` 隐藏 PairBatchReader，恢复
  有界并发 `GetPair`。

### 用户可见正确性 Hotfix（SCB-79，当前候选）

- `trusted_now_ms >= mature_at_ms` 时 WATER 永远消失；
- Authority 返回当前 viewer 精确可偷的 `(plot_id, crop_cycle_id)`；
- H5 只使用精确目标与当前 Ready Snapshot 的交集；
- UNKNOWN、无目标和本茬已偷过均 fail closed；
- 成功、已偷过、其他 REJECTED 与 UNAVAILABLE 都替换“请求已提交”文案，资产
  只在终态和 PlayerBootstrap 刷新后显示。

F1/F2 与 SCB-79 可以一起发布，但验收证据必须分别报告正确性、物理往返和 UI
终态，不能用一个浏览器 PASS 替代读路径证明。

## 3. 后续阶段（不得无证据并入本 hotfix）

### F0 — 基线与容量门

在继续改导航前采集 20/50 好友、稀疏/密集机会两组数据：RPC 数、Redis commands
与 round trips、返回字节、Player mailbox activation、p50/p95/p99、Gateway/Player/
Farm WSS、UNKNOWN 比例和 Hint 合并率。没有 F0 不允许宣称 Facts-only 或
NavigationV2 更快。

### F3 — 普通列表 Facts-only A/B

当前生产链已经是 Header/Facts Pipeline。所谓“Facts-only 普通页”只能作为 A/B：
对整页直接批读 Facts，省掉 Header 字段，但可能在机会稀疏时增加读取字节。必须在
F0 两种密度下证明总字节、CPU 和 p99 同时不退化，且保持：

- 一个可信 `trusted_now_ms`；
- 每 farm 独立 UNKNOWN，不把缺失解释为 NONE；
- THEFT bit 与精确目标集合一致；
- 不激活 FarmActor。

建议开关：`FARM_FRIEND_FACTS_ONLY_LIST_ENABLED`，默认关闭直至 A/B 通过。

### F4 — NavigationV2 CandidateWindow

连续互动导航使用服务端候选窗口（上限 8），先以 Header 做廉价候选，再对窗口执行
viewer-specific Facts 和 canonical Pair 复核。返回的是候选顺序，不是 Permit；Join
和写操作仍需重新判权。不得把普通列表首屏 cursor 当作导航 cursor。

建议开关：`FARM_FRIEND_NAVIGATION_V2_ENABLED`。超时或依赖错误返回显式 UNKNOWN/
UNAVAILABLE，不静默回退到客户端 mature-plot 扫描。

### F5 — visited generation

visited 只在真实 ActiveSubscription `SubscriptionReady` 后由服务端记录，键包含
viewer、navigation generation 和 target owner。开始新一轮、好友关系 epoch 变化或
Home reset 时推进 generation；Snapshot 到达、Join accepted 或客户端点击都不能提前
标记 visited。

### F6 — Hint 去噪

Hint 仍只携带 target owner 与单调 revision。Gateway 可按 `(viewer,target)` 合并未消费
revision；H5 收到 Hint 先撤权再 Pull。禁止在 Hint 中加入 action mask、精确 target、
Pair 状态或 Permit。需要分别测试乱序、重复、丢失、重连和 revision 回退。

### F7 — 局部降级、WSS 与收口

- FarmOpportunity 单行故障继续显示该卡 UNKNOWN；
- Pair/Profile 的 identity、corruption 或批次 cardinality 错误仍整页 fail closed；
- 分页刷新不能用第一页结果清除后续页 baseline；
- Player/Farm mailbox activation、Holder WSS 和 Redis 连接/缓冲 WSS 必须进入门禁；
- staging 故障注入需覆盖一个 Holder 超时、单 farm facts 损坏、Hint storm 和 Redis
  Pipeline transport error。

## 4. 发布与回滚门

当前候选发布前必须全部满足：

1. Backend `go test ./...`、`go vet ./...`、`go test -race ./...`；
2. Pair batch/single parity：存在、缺失、共享 bucket、manifest 损坏、WRONGTYPE、
   transport error，并证明一批仅一个 Pipeline round trip；
3. PublicProfile：初始化/变更同 CAS、损坏 fail closed、旧记录直接物理回源且不进入
   serial executor；
4. H5 全量测试、TypeScript、web/staging/wechat audit build；
5. 双浏览器真实 Authority E2E，证明成熟 WATER 消失、精确偷菜目标消费、库存不乐观、
   terminal 文案和 pending=0；
6. 先灰度 F1/F2；异常时分别关闭上述两个开关。协议字段 5 为 append-only，不能通过
   把 UNKNOWN 降为 NONE 或重新启用客户端推断来回滚。

NavigationV2、Facts-only、visited generation 和 Hint 去噪须使用独立 checkpoint、
独立证据与独立回滚开关。
