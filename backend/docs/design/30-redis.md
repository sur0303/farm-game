# 历史 D5 参考材料 · Redis Key / TTL / 故障恢复（非当前第 30 号权威）

> 本文是早期“Tcaplus 业务权威、Redis 仅缓存”阶段的历史材料，保留用于代码迁移与真库探针追溯。
> 当前 Actor 写权、CAS、Unknown Outcome 和 Redis P0 权威语义以
> [30-actor-authority-store-contract.md](./30-actor-authority-store-contract.md) 为准；Redis Key、Hash
> Tag、Lua、TTL 和生产持久化将在第 31 号物理合同中重新冻结。本文不得覆盖 26～30 的当前合同。

对应交接文档任务四。key 定义:`internal/storage/redis/rediskey/rediskey.go`。早期曾在内部测试 Redis
`<internal-redis-host>:<port>` 验证读写与类型（`live_test.go`）；真实环境地址不得写入设计文档。

前提见 [00-overview](./legacy/00-overview.md) §3 冻结结论 7(缓存不是权威)。

---

## 1. Key 规范

格式:`系统模块名:业务信息:参数:值类型`,值类型 ∈ {string,list,set,zset,hash}。

**Hash tag**：需要同一实体的多个 key 落同一 Cluster slot 时，用 `{player:<player_id>}` 或 `{farm:<farm_id>}` 包裹参与 slot 计算的部分。同农场的 `farm:online` 使用 `{farm:<farm_id>}`，不能只用裸数字导致 Player/Farm 语义混淆。

## 2. Key 目录

| 类别 | Key 示例 | 类型 | TTL | 丢失后行为 |
|---|---|---|---|---|
| 会话 | `session:current:{owner}:hash` | hash | 会话时长(如 2h,活跃续期) | 提升 authority epoch,强制重登 |
| 会话权威代次缓存 | `session:authority.epoch:global:string` | string | 无 | 回源不会随同一 Redis 全量丢失而回退的持久/协调来源 |
| 一次性连接票据 | `connection:ticket:{sha256}:hash` | hash | 默认 30s | 重新登录获取新票据 |
| 路由提示 | `route:owner.node:{owner}:string` | string | 短(如 10min) | 重新定位 |
| 连接绑定 | `gate:conn.binding:{owner}:string` | string | 会话时长 | 跨节点推送失效,重连重建 |
| 限流(玩家) | `ratelimit:player:{owner}:hash` | hash | 滑窗(如 60s) | 用保守默认重建 |
| 限流(IP) | `ratelimit:ip:{ip}:hash` | hash | 滑窗 | 同上 |
| 热结果缓存 | `opresult:cache:{owner}.{op}:string` | string | 短(如 10min) | 回源 TcaplusDB |
| 好友摘要（B2-2B 已退役） | — | — | — | 关系只读 FriendPair；列表由可重建 FriendIndex 加速 |
| 公开农场摘要 | `farm:public.summary:{owner}:hash` | hash | 中 | 回源/异步重建 |
| 重连状态 | `reconnect:token:{token}:string` | string | 短(如 5min) | 完整登录并重新 Join |
| 在场集合 | `farm:online:{target}:set` | set | 会话时长 | 客户端重新 Join 重建 |
| 分布式锁 | `lock:owner:{owner}:string` | string | 锁超时(如 10s) | 锁自动释放 |

## 3. 绝不能只存于 Redis(交接文档 §9.5)

以下权威事实必须以 TcaplusDB 为唯一权威,Redis 至多做可重建缓存:

- Wallet / Inventory / Farm(Plot / CropCycle)
- Friendship
- TheftSettlement / PendingOutbox
- operation_id 防重事实
- 唯一奖励领取事实(LootReceipt)/ 唯一任务进度

Redis 丢失可影响体验/命中率/限流精度,但不能造成金币丢失、地块回退、已偷产量恢复、关系永久错误、幂等记录消失后重复发奖。

## 4. Connection ticket

- 服务端生成 32 个密码学安全随机字节，给 H5 的值为无 padding base64url string；
- Redis Key 使用原始 ticket 的 SHA256，不保存原文；
- Value 绑定 `player_id/session_id/session_authority_epoch/active_session_epoch/issued_at/expires_at`；
- 默认 TTL 30 秒，只允许 GETDEL 或 Lua 原子读取并删除，禁止先 GET 再 DELETE；
- 票据不绑定 IP、User-Agent 或指定 Gateway，允许负载均衡到任意健康 Gateway；
- 先校验 FarmFrame.protocol_version、连接序号和 ClientHello.client_build，再消耗票据；
- 核销成功后即使连接随即断开，原票据也不得恢复；客户端重新登录获取新票据；
- Redis 不可用时 fail-closed，不得相信 H5 自报 player_id。

## 5. Redis 全量故障恢复(fail-closed)

流程(交接文档 §8.6 / §13.2):

```
Redis 全量丢失
→ 暂停签发写 Permit(fail-closed,不假成功)
→ 从独立持久/协调来源提升全局 session_authority_epoch
→ Redis 重建该 epoch 的缓存
→ Gateway/Actor 只接受新 authority epoch,旧 SessionPermit 整体失效
→ 玩家重新登录
→ 热缓存与路由重新构建
```

关键：`session_authority_epoch` 不能只存在于将被整体清空的同一 Redis 中，也**不能**把 `active_session_epoch` 重置为 1 后立即开放写入(会让旧凭证复活)。S2 只预留 Permit 字段，不实现灾难切换；Owner 业务事实不受 Redis 故障影响(权威在 Tcaplus)。

## 6. 客户端库与连接

- 库:`github.com/redis/go-redis/v9`。
- 连接:`redis.ParseURL(uri)`;参考现网 ms-go 惯例——`DialTimeout≈2s`、`MinIdleConns≈10`、不设 `MaxIdleConns`(避免大量 TIME_WAIT)、起保活 Ping。
- URL 形式：`redis://:<password>@<internal-redis-host>:<port>/<db>`（真实凭据只通过 Secret 注入，不入库）。

## 7. 验收

- 离线:`TestKeyFormats` / `TestHashTagColocation`(同 owner 多 key 同 hash tag)通过。
- connection_ticket 并发核销测试：同一个 ticket 只能一个请求成功，Redis 中不出现原始 ticket。
- **真库**：`FARM_REDIS_URL=... go test -run Live` 在受控内部测试实例上验证 session hash 读写、
  `farm:online` set、TTL；具体地址和凭据不记录在本文。
