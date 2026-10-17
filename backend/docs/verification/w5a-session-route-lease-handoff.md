# W5A · SessionRoute Lease 单调 Deadline 实施交接

日期：2026-08-03  
基线：`codex/recovery-admission-20260803` / `b00edbd06460c8d398160e9009df7399af50e398`  
实现分支：`codex/w5a-session-route-lease-20260803`  
状态：`IMPLEMENTATION_PASS / REAL_DUAL_GATE_EVIDENCE_PENDING`

## 1. 本次边界

本包只关闭第 26 号合同 §7.5 的 SessionRoute Lease 物理差距，不冒充完成
Subscription Lease、双 Gate Recovery E2E、Actor Redirect 或 200 人同场验收。

冻结链路为：

```text
Gate 记录 renew_started_monotonic
→ SessionAuthority.RenewRoutes(完整 Route Fence)
→ Redis Lua 使用 Redis TIME 写权威 lease_expire_at_ms
→ 每条匹配结果返回 accepted_ttl
→ Gate 建立 renew_started_monotonic + accepted_ttl - safety_margin
→ Deadline 前允许准入
→ Deadline 到达后该 connection generation 终止准入，不可续租复活
```

`ActiveRoute.LeaseExpiresAt` 继续作为 Redis 权威记录和诊断数据，但 Gate 业务准入不再
比较该跨机器绝对时间。

## 2. 实现内容

### 2.1 Authority 返回接受后的 TTL

`session.ActivationResult` 和 `session.RouteRenewal` 新增：

```go
AcceptedLeaseTTL time.Duration
```

内存 Authority、Redis Authority 与 Redis Ticket Activation 均返回实际接受的 TTL。
同一 Fence 回放已经核销的 Ticket 只返回 Redis 权威 Lease 的**剩余 TTL**，不会因
重复 `ClientHello` 在 Gate 本地重新获得一整段 TTL。

### 2.2 Redis 使用权威时间

Session 激活和 Route 续租 Lua 使用：

```lua
local redis_time = redis.call('TIME')
local redis_now_ms = redis_time[1] * 1000 + floor(redis_time[2] / 1000)
local lease_expire = redis_now_ms + accepted_ttl
```

Gate 不再把本机 `time.Now().UnixMilli()` 算出的绝对到期写入 Redis。
续租 Lua 在完整 Fence 匹配后仍会用 Redis TIME 检查原 Lease：原 Lease 已到期时返回
`LeaseExpired`，不允许相同 Fence 把权威 Route 复活。

### 2.3 Gate 本地保守 Deadline

每条连接新增仅在 Gate 内使用的 `routeLeaseDeadline`。初次 ClientHello 激活和每次续租
都从请求开始时刻计算 Deadline，使 RPC 往返和 Authority 处理耗时消耗 TTL，而不是延长信任。

Safety Margin 由 `GATE_ROUTE_LEASE_SAFETY_MARGIN` 配置；默认取 TTL 的 10%，最多 1 秒。
Margin 必须小于 Route Lease TTL，否则 Gate 启动失败。

### 2.4 过期是终态

`laneRouteLeaseExpired` 是当前连接代次的终止状态：

- 过期后不再进入续租批次；
- Redis 权威 Lease 已过期时，即使完整 Fence 仍匹配也不能续租复活；
- 续租 RPC 返回期间若旧 Deadline 已越过，不接受迟到结果；
- 已经进入 FirstHop 的 ADMITTED 命令不被 Lease 到期取消；
- 新命令返回 `ROUTE_LEASE_EXPIRED`；
- 新连接代次通过 Cookie 免密换票和 ClientHello 获得新 Lease。

显式完整 Fence 不匹配仍返回 `STALE_SESSION`，与纯 Lease 到期保持互斥。

## 3. 验证证据

目标单测覆盖：

- Authority 激活和续租显式返回 Accepted TTL；
- 已核销 Ticket 的同 Fence 回放只返回正数且小于原值的剩余 TTL；
- Redis 绝对时间即使人为设置为过去，也不影响 Gate 本地准入；
- 本地 Deadline 到期即使 Redis 绝对时间在未来仍 Fail Closed；
- 过期连接不能被后续续租复活；
- Redis 权威到期后完整 Fence 仍匹配也不能复活；
- 续租响应必须逐项回显完整 Route Fence，重叠续租轮次被串行化；
- RPC 延迟从 Accepted TTL 中扣除；
- 已 ADMITTED 命令允许在 Lease 到期后完成；
- KICK/STale 与 LeaseExpired 错误及指标互斥。

执行：

```text
go test ./internal/gateway/session ./internal/gateway/auth/... \
  ./internal/gateway/command ./internal/gatewayapp -count=1
```

公司 Redis 真脚本探针：

```text
go test ./internal/gateway/auth/ticket \
  -run TestLiveRedisTicketActivation -count=1 -v
```

探针已经证明 Redis Ticket Activation 和 Route Renew Lua 能在公司 Redis 执行，且返回的
权威到期落在 Redis TIME 前后采样窗口加 Accepted TTL 内。

最终验证已执行：

```text
go test ./... -count=1
go vet ./...
go test -race ./internal/gateway/session ./internal/gateway/auth/... \
  ./internal/gateway/command ./internal/gatewayapp -count=1
git diff --check
```

结果：全部 PASS。`go test ./...` 覆盖全仓；Race 覆盖 Session、Auth/Ticket、Gate 与
Gateway App 的本次并发边界；未使用窄测试代替全仓结论。

## 4. 明确未完成

以下不能由本提交宣称完成：

1. `BatchRenewSubscriptions`、Hub 完整 Fence 和 Subscription 到期释放名额；
2. Route Lease 到期后的最佳努力 Reconnect Push 与物理连接有序 Drain；
3. 当前 Recovery Admission 下的双 Gate 顶号、Drain、重连真实 E2E；
4. GateControl mTLS/服务身份；
5. 动态 Actor Redirect 在线接入；
6. 1+199 同场和 Renew QPS 压测；
7. 混合版本滚动发布门禁。

这些分别进入 W5B 与后续 Evidence Bundle，不能沿用旧提交的 PASS。
