# M2 三 Loop 与进程故障验证冻结

日期：2026-07-30

## 验收栈

```text
Playwright H5
  -> Vite 同源代理
  -> farm-gate (Direct WebSocket)
  -> player-svr / farm-svr
  -> farm-worker
  -> 公司持久 Redis
```

四个业务服务运行在独立 Docker 容器中，公司 Redis 不在 Compose 生命周期内。

## 三 Loop 结果

`npm run e2e:staging`：4/4 通过，31.5 秒（2026-07-30 当前工作区复跑）。

覆盖：

1. 个人与经济 Loop：购买、种植、成熟、收获、出售、断线恢复。
2. 好友 Loop：首次登录主动加载花名册、分享接受、双方
   `FRIEND_ROSTER_CHANGED` 下行、好友拜访、返回自己农场。
3. 多人互动 Loop：好友浇水、偷菜、公开 FarmChange 广播、断线恢复和双浏览器一致性。
4. M3 附加证据：首次收获图鉴解锁在断线重连后仍存在。

## 本轮补充的边界合同

### 成熟作物不能再浇水

该约束不是只由 H5 隐藏按钮：

- `FarmActor.Water`（本人浇水）与 `FarmActor.PrepareCare`（好友护理）均在
  权威状态写入前判断 `now >= mature_at`；
- 成熟后统一返回 `ERR_CROP_ALREADY_MATURE`，不增加 `farm_version`，
  不修改 `watered`，也不创建护理 Settlement；
- H5 同时不再把成熟地块标记为 `can-water`，Mock Server 与真实服务保持同一语义；
- `TestMatureCropRejectsOwnerWaterAndFriendCare` 覆盖本人和好友两条服务端路径。

### 分享接受后双方好友列表实时收敛

接受分享成功时，服务端只在关系真实发生变化后，分别向邀请方与接受方发送
`FRIEND_ROSTER_CHANGED`。H5 收到下行后重新读取权威花名册。好友/分享 HTTP
响应统一带 `Cache-Control: no-store`，前端请求也显式使用 `cache: no-store`，
避免浏览器用旧的成功响应满足下行触发的刷新。

双浏览器用例先解除已有关系，再接受同一分享，并在不点击刷新按钮的情况下验证：

- 接受方能够进入邀请方农场；
- 邀请方好友计数变为 1；
- 邀请方收到“已与玩家成为好友”提示；
- 双方好友列表均包含对方。

## 进程故障结果

`npm run e2e:workflow`：1/1 通过，用例 37.6 秒、完整 Playwright 38.3 秒
（2026-07-30 当前工作区复跑）。

故障时序：

1. player-b 完成购买、种植并等待作物成熟。
2. 停止 `farm-worker` 和 `player-svr`。
3. 提交 Harvest；FarmActor 先持久化 Settlement 并清空地块，客户端保留
   `PROCESSING` 未决操作，玩家物品尚未增加。
4. 恢复 `player-svr` 和 `farm-worker`；worker claim 原 Workflow，执行幂等
   `ApplyReward` 并完成 Farm Receipt。
5. H5 重连、查询原 operation，未决项归零且胡萝卜只增加一次。
6. 再停止四个业务进程并全部恢复；同一浏览器重新进入 `InFarm`，Redis 中的
   玩家资产、空地块和终态 Receipt 均被重建，没有回到进程内初始值。

测试 `finally` 会恢复被停止的服务。运行结束后的四个容器均为 `healthy`。

## 结论

冻结计划中“重跑/冻结三个 Loop + process-fault E2E”的验收口径已有可重复证据。
该结论不外推为 Redis 主从切换、uncertain commit 或多 Gate Drain 已完成；这些
分别属于 M5 和 M1。
