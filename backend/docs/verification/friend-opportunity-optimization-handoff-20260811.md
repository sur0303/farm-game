# 好友互动机会实时优化实施交接

日期：2026-08-11
状态：独立分支已实现并完成单机真实链路验收；未合入 `Integration`。
后端分支：`codex/friend-opportunity-optimized-r1`
H5 配套分支：`codex/h5-friend-opportunity-fusion-r1`，提交 `787dc00`

## 1. 本轮采用的最终方案

本轮没有引入 `FarmOpportunitySummary` 第二份异步数据，也没有使用 Redis Pub/Sub 作为生产主链。采用的主干是：

```text
Farm 权威写入
→ 同一权威提交更新紧凑 OpportunityHeader / Facts
→ 同一农场只登记一个最早 next_opportunity_at 的 Wakeup
→ farm-worker 每秒有界扫描自己负责的 FARM Shard DueIndex
→ 到期后重新读取 Header/Facts 并按 trusted_now 精确判断
→ 批量读取好友 SessionRoute，只保留在线且 Fence 完整的连接
→ 按 gateway_id 分组，通过现有 DeliverBatch 直投 Gate
→ WSS 推送 FRIEND_OPPORTUNITY_CHANGED Hint
→ H5 在 300ms 窗口内合并 Hint，并按 target_owner_id/revision 定向精查
→ 更新 CandidateWindow，显示当前最新的一张互动卡片
```

登录、回到自己农场、页面重新可见和低频兜底仍走 `GET_NEXT_FRIEND_OPPORTUNITY`。Hint 负责及时，Pull 负责丢消息恢复；最终能否偷菜、浇水、除虫始终由好友关系权威和 Farm Facts 精确裁决。

## 2. 已实现内容

### 2.1 Header 窄读

`FriendOpportunityHeaderReader` 支持：

- `full`：读取完整有界 Farm Model，作为兼容基线。
- `narrow-shadow`：主结果仍使用 full，同时旁路执行 narrow 并比较结果。
- `narrow`：只读取 `_meta:record_version`、`_meta:model_layout_version` 和 `model:opportunity_header`；最终候选仍读取 Facts/AttemptReceipt 精确检查。

防护条件：单批最多 64 座农场；窄读缺字段、版本不支持或结果未知时 fail closed，并允许最多 8 个目标回退 full，不把 UNKNOWN 伪装成 NONE。

公司 Redis 的 300 好友确定性基准：

| 模式 | 延迟 | Redis 命令 | RTT | 分配量 |
|---|---:|---:|---:|---:|
| full | 约 13.1ms | 600 | 5 | 约 1.24MB |
| narrow | 约 6.2ms | 300 | 5 | 约 0.59MB |

`narrow` 将该阶段延迟约降低 53%，并显著减少传输与解码成本。

### 2.2 在线 Hint 直投

新增可配置传输模式：

- `pubsub`：保留兼容模式，仍是默认值。
- `shadow-direct`：实际仍由原链路服务用户，同时执行 Direct 旁路统计。
- `direct`：正式使用 SessionRoute + Gate DeliverBatch。

Direct 路径不是只按 `connection_id` 裸投递。每条下行携带并在 Gate 校验完整 `DownlinkTargetFence`：

- `target_player_id`
- `gateway_id` / `gateway_boot_id`
- `connection_id` / `connection_generation`
- `session_epoch`
- `session_route_revision`

`FRIEND_OPPORTUNITY_CHANGED` 被明确归类为 Player/Session Push，不复用 Farm PresenceEpoch。旧连接、旧 Gate 启动代次、旧 SessionRoute revision 均会被拒绝，不能误投给新会话。

### 2.3 Worker 有界扫描与公平性

Opportunity recovery 不再让每个 Worker 无界扫描所有分桶：

- 只扫描 `OwnedShardsProvider` 返回的 FARM Shard。
- 单轮有 `ClaimLimit=64`、`DiscoveryLimit=256`、`MaxConcurrent=4` 和 `RoundTimeBudget=800ms`。
- 维护轮转起始游标；某轮预算耗尽后，下一轮从后续 Shard 开始，避免固定前缀长期饿死。
- 输出 owned shards、扫描数、空分桶、backlog、最大 due lag、budget exhausted、失败与 stale generation 指标。

单 Worker 环境默认拥有全部 Shard；多 Worker 环境必须由部署/路由控制面提供不重叠的 owned shard 集合。当前静态 Provider 是 P0 实现，动态 Assignment 属于下一阶段。

### 2.4 H5 定向刷新与恢复

H5 配套提交 `787dc00` 已收敛：

- 收到 Hint 后按 `target_owner_id + opportunity_revision` 去重。
- 300ms 合并窗口内只发一次定向刷新，避免同一农场短时间连续变化造成查询风暴。
- 服务端精查后同步 Upsert/Discard CandidateWindow，避免“弹窗正确、连续导航仍读旧窗口”。
- 登录、回家、页面重新可见和 fallback 走完整 Navigator V3，而不是只刷新好友列表第一页。
- 点击弹窗进入目标农场，返回自己农场后继续拉取下一座未访问且当前可操作的农场。

### 2.5 审阅后补强

独立并行审阅又补齐了两类边界：

- `FRIEND_OPPORTUNITY_CHANGED` 现在强制要求完整 `TargetFence`，并校验 PushContext 必须是目标玩家一致的 Player context；不再允许仅凭 Farm PresenceEpoch 走兼容路径。
- KICK、SessionRoute lease 终止和本地过期会撤销 Gate Downlink target。撤销后即使 DirectDispatcher 持有旧路由，Gate 也返回 `STALE_CONNECTION`；本地兼容通知同样跳过 revoked 连接。
- 共享 DueIndex 的 schedule discovery 改为有界分页，可跳过最多 65536 条非 schedule 前缀，避免 `limit` 较小时被旧 Order 永久遮挡。
- 静态 Staging 在没有动态 Shard Assignment 前收敛为单 Worker 副本，避免两个副本默认同时扫描全部 FARM Shard。多 Worker 扩展必须先接入明确且不重叠的 OwnedShards。

## 3. 真实链路验收

环境：公司 Redis；真实 `farm-gate + player-svr + farm-svr + farm-worker + 浏览器`；300 个好友；5 块测试土地；`narrow` Header；`direct` Hint；1 秒 Worker 扫描。

审阅强化后的最终 B2 证据：

- 通过：`pass=true`
- 好友列表首批 20 人：48ms
- 在线 Hint 定向精查：9～11ms
- 浇水边界到弹窗：约 1.38s
- 成熟边界到偷菜弹窗：约 0.82s
- 好友 HTTP：7 次，P50 11ms，P95 337ms；其中一次无当前机会的全局恢复查询明确返回 503/UNKNOWN，定向 Hint 查询全部为 200
- Worker：10 次 claim，8 次有效 Hint，0 failed，0 stale generation，0 lease lost
- Gate 日志：没有 stale fence 或错误投递

证据目录：

```text
docs/verification/evidence/friend-opportunity-real-ab/fofinalb2fence/b2/
├── result.json
├── viewer.png
├── gate.log
├── player.log
├── farm.log
├── worker.log
└── config.txt
```

最终结果不是“每次弹窗查询 300 个好友”：冷启动/回家时使用一次全局恢复；在线阶段由 Hint 指向明确目标，只精查该农场并更新窗口。

## 4. 被验证但未采用的方案

### 4.1 Summary

300 好友的轻量 Summary 微基准约 5.2ms，读取略快于 narrow Header；但它需要额外写、延迟一致性、重建和第二份语义维护。本轮 narrow Header 已达到 6.2ms，且仍直接来自 Authority Model，因此 P0 不引入 Summary。后续只有在真实混合读写压测证明 Authority 读取成为主要瓶颈时再打开对照实现。

### 4.2 SessionRoute HMGET

尝试把紧凑 SessionRoute Hash 的 Pipeline `HGETALL` 改为 9 字段 `HMGET`。实测命令结构从约 1506 次 HGETALL 转为大量 HMGET，但 HMGET 需要重复发送字段名，请求字节反而明显增加；Session Hash 本身只有约 12 个字段。因此已回退，保留 Pipeline HGETALL。若未来 Session Hash 变大，应以响应字节、Redis CPU 和端到端延迟重新验证，而不是仅按命令名判断。

### 4.3 Redis Pub/Sub

Pub/Sub 仍保留为兼容/对照模式，但不作为当前生产主链。Direct 模式复用现有 SessionRoute、Gateway ConnectionRegistry、下行队列和 Fence，故障时由登录/回家/fallback Pull 恢复，不需要额外维护订阅拓扑。

## 5. 验证命令

```bash
go test ./...

go test -race \
  ./internal/friend \
  ./internal/gateway/session \
  ./internal/gateway/downlink \
  ./internal/logic/downlink \
  ./internal/worker/opportunityrecovery \
  ./cmd/farm-worker

FARM_REDIS_URL=redis://127.0.0.1:6379 \
WSS_ENDPOINT=wss://farm.test/ws \
PERMIT_SECRET=test-permit-secret \
SESSION_TICKET_HMAC_SECRET=test-ticket-secret \
ACCOUNT_COOKIE_SECRET=test-cookie-secret \
docker compose -f deploy/docker-compose.yml config -q
```

上述全量测试、Race 检查和 Compose 配置检查均已通过。

## 6. 尚未证明的边界

这次验收证明单机真实链路和 300 好友产品上限下的正确性与延迟，不等于已经证明 3000 万 DAU：

- 当前是真实 Redis + 单 Viewer/单测试农场，不是峰值混合读写容量压测。
- 多 Worker 的动态 Shard Assignment、再均衡和滚动发布仍需云端验证。
- Schedule 与 Order 当前仍共享 DueIndex；已通过有界分页消除普通前缀饥饿，但若单一分桶积压超过 65536 条更老的非 Schedule 成员，仍应演进为同 Slot 的独立 ScheduleDueIndex。
- Direct Hint 目前会批量读取最多 300 份紧凑 SessionRoute；若在线通知扇出成为 Redis 热点，可再评估短 TTL OnlineRouteIndex，但必须保留 Gate 完整 Fence。
- `narrow` 的真实链路 fixture 当前主要覆盖 V1 布局；V2 布局需增加独立真实数据夹具。
- 生产参数候选为 1 秒扫描，不应把测试环境 250ms 直接带入生产。

## 7. 后续 Agent 的工作顺序

1. 从本分支补充 V2 Header fixture 和 full/narrow shadow mismatch 指标告警。
2. 在多 Worker 环境接入 FARM Route Assignment，验证 OwnedShards 不重叠、Drain 后游标重新归一化。
3. 做混合负载：登录/好友列表/Hint/Plant/Care/Theft 同时运行，关注 Redis CPU、Authority Slot P99、due lag 和 Gate 队列。
4. 只有指标证明 Direct SessionRoute 批读成为瓶颈时，才设计可恢复的 OnlineRouteIndex；不能牺牲完整 Session Fence。
5. 通过后再分别把后端与 H5 分支提 MR；本交接不授权直接合入 `Integration`。
