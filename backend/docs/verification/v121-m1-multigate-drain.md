# M1 多 Gate Drain 与 RecoveryBarrier 验证

日期：2026-07-30

## 原问题

单 Gate 已能在 SIGTERM 时：

- `/readyz` 进入 503；
- 拒绝新 Upgrade；
- 在既有 writer 中发送 `DrainNotice`；
- 用 1012 关闭连接；
- H5 重连后执行 RecoveryBarrier。

但原 farm-svr 只有一个进程级 `GATEWAY_DOWNLINK_ADDR`，FarmActor Presence 只记录
`connection_id`。两个 Gate 可以启动，不代表 Logic 知道连接属于哪一个 Gate：
客户端若落到 Gate B，Snapshot 和 FarmChange 仍可能被发往 Gate A。因此原实现
不能宣称多 Gate 完成。

## 修正

1. `TrustedCommandContext.gateway_id` 在 Join 时进入 FarmActor Presence。
2. `FarmSettlement` 不受影响；这是连接级临时路由，不进入业务持久状态。
3. `DownlinkItem` 增加 `gateway_id`。
4. `RoutedGatewayClient` 按 `gateway_id` 分批调用对应
   `GatewayDownlinkService`。
5. 未知、缺失或不可用的 route 返回 `UNAVAILABLE`，不猜测其他 Gate。
6. 单 Gate 的 `GATEWAY_DOWNLINK_ADDR` 保持兼容；多 Gate 使用：

```text
GATEWAY_DOWNLINK_ROUTES=
  gate-a=127.0.0.1:19392,
  gate-b=127.0.0.1:19492
```

## 真实验证

命令：

```bash
./scripts/test-multigate-drain
```

测试临时启动：

```text
H5 :5175
  -> readiness-aware proxy :18580
      -> gate-a :18380 / downlink :19392
      -> gate-b :18480 / downlink :19492
  -> farm-svr :19300
  -> player-svr :19100
  -> company Redis
```

时序：

1. 浏览器经代理登录 Gate A，完成 ServerHello、Join 和私有 Snapshot。
2. 指标确认 Gate A active=1、Gate B active=0。
3. 测试向 Gate A 发送 SIGTERM。
4. Gate A 立即 not-ready，发送 DrainNotice 和 1012。
5. H5 进入重连；代理按 readiness 把续期和新 WebSocket 交给 Gate B。
6. Gate B 用共享 Redis 核销新的一次性 ticket。
7. farm-svr 按 Presence 中的 `gateway_id=gate-b` 将 Snapshot 发往 Gate B。
8. H5 恢复私有状态、目标农场和未决查询，完成 RecoveryBarrier 回到 `InFarm`。
9. 指标确认 Gate B active=1，好友花名册状态为 `ready`。

2026-07-30 当前工作区复跑结果：Playwright 1/1 通过，用例 1.4 秒，
完整 Playwright 2.4 秒；harness 正常清理临时 Gate/Farm/Proxy 进程。

本地 `multigate-proxy` 只用于模拟 Kubernetes Service 对 readiness 的摘流行为，
不作为生产流量入口交付。生产仍由 Service/Ingress/LB 完成实例选择。
