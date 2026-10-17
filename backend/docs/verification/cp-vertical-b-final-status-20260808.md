# CP-VERTICAL-B Final Status — Friend / Care / Theft Real Browser Loop

日期：2026-08-08
状态：**PASS / READY_TO_PUBLISH**

## 1. 候选真值

```text
Backend branch             = codex/cp-vertical-b-integration-20260808
Backend verified code head = c6fa6dac0a42e30ff0dab661e4bdefb431f30069
H5 branch                  = codex/cp-vertical-b-integration-20260808
H5 verified code head      = 81caee13ff4da86347ed40506c2dccf24b9db050
Backend base               = 1ae95ce
H5 base                    = e8f90ed
```

本文档提交只登记验收结果；上述两个 code head 是两轮真实栈验收时由 runner 冻结并写入
`run-manifest.json` 的候选。

## 2. 已闭合产品合同

- Public Proto 仅 append-only：
  - `PublicProfileView.avatar_id = 3`
  - `PublicProfileView.level = 4`
  - `FarmSnapshot.owner_public_profile = 5`
  - `PlayerFeatureSummaryView.tasks = 31`
- Player authority 持久化用户名/头像，并提供窄 `GetPublicProfile` RPC。
- HTTPS `/api/v1/share/*`、`/api/v1/friends*` 返回服务端权威昵称、头像和等级。
- HTTPS 分享接受同时提交 `FRIEND_SHARE_ISSUER`、`FRIEND_QUOTA`、`FRIEND_PAIR`；旧 Friend Store
  仅作 roster projection，不能单独授权 CARE/THEFT。
- 好友农场标题和 owner 资料来自 `FarmSnapshot.owner_public_profile`，缺失/畸形时 fail closed。
- Bootstrap 投影 TaskDomain 摘要；`CLAIM_TASK` 仍禁用。
- WATER CARE 提交 FarmChange、Care receipt、reward obligation；奖励以 Player revision 前进和
  Bootstrap 私有状态收敛为准。
- CARE `PROCESSING` 查询不再返回 `ERR_COMMAND_UNIMPLEMENTED`。目录未命中时返回诚实
  `NOT_FOUND`，H5 在仍有效的订阅上重放同一 `operation_id`，由 FarmActor receipt 防重。
- THEFT 后双方 FarmChange、访问者库存和 Player revision 均收敛。
- 分享导航只消费一次；显式返回自己农场不会在 WSS 重连后被旧 share intent 抢回好友农场。
- 商店数量输入跨 500ms 权威状态重绘保持稳定。

冻结不变：friend/share WSS CommandCatalog 不启用；PEST 延期；客户端不伪造 PEST incident；
`CLAIM_TASK` 不启用。

## 3. 不可拆分真实浏览器硬门

固定流程：

```text
登录 → 买种子 → 种植 → 收获 → 分享加好友 → 进入好友农场
→ 浇水 → 偷菜 → 返回自己农场
```

环境：headed Chromium（Xvfb）、两个独立 BrowserContext/网页账号、真实 farm-gate、
player-svr、farm-svr、farm-worker、HTTP/WSS、真实 Redis Authority；无 Mock、无前端资料/资产伪造。

额外硬断言：

- 好友卡 avatar/level/display name 来自服务端 profile；好友农场 owner profile 来自 Snapshot。
- WATER 后两个浏览器看到相同递增 farm version，访问者 Care reward `SETTLED` 且 Player revision 前进。
- THEFT 后 owner/visitor 的 `stolen_total` 同值增加、`harvestable_count` 下降，访问者库存/revision 前进。
- 返回自己农场后任务摘要可见且 claim 按钮禁用，pending operation 为 0。
- 浏览器 hard refresh 后农场、资产、好友和任务恢复。
- 固定函数 hook 对 farm-gate 做一次真实优雅重启；两个浏览器建立更高 connection generation，
  恢复各自明确选择的农场和权威资产。
- Redis ShardWriteAuthority 保持 ACTIVE，无 legacy fallback；独立 Go WSS protocol bot PASS。

### 连续两轮 PASS

1. `.agentctl-cp-route-state/evidence/cp-vertical-b-20260808T094201Z/`
   - S1A headed Playwright：PASS（53.8s）
   - S1B protocol bot：PASS（19 frames）
   - Authority audit：PASS
   - 视频：`videos/*.webm`
   - trace：`playwright-artifacts/**/trace.zip`
   - milestones：`screenshots/{friend-profile,water-theft,return-home,restart-recovered}-*.png`
2. `.agentctl-cp-route-state/evidence/cp-vertical-b-repeat-20260808T095801Z/`
   - S1A headed Playwright：PASS（51.9s）
   - S1B protocol bot：PASS
   - Authority audit：PASS

重启窗口中的 WSS 503 是进程 listener 关闭到 replacement `/readyz` 成功之间的预期恢复证据；
它没有被当作业务成功，最终 generation、Join、Snapshot、资产恢复断言均通过。普通 404 为浏览器
静态资源请求噪声，不参与业务断言。

## 4. 静态验收

Backend（109 packages）：

```bash
go test -buildvcs=false -count=1 ./...       # PASS
go test -buildvcs=false -race -count=1 ./... # PASS
go vet -buildvcs=false ./...                 # PASS
git diff --check                             # PASS
```

H5：

```bash
npm test -- --run     # 16 files / 143 tests PASS
npm run build         # PASS
npm run audit:build   # web/staging/wechat real-backend boundary PASS
npx tsc --noEmit      # PASS
git diff --check      # PASS
```

`npm run validate:handoff` 需要实际 staging/production URL 参数；本地 Authority 硬门不伪造部署 URL，
因此该部署交接检查不作为本候选的本地 PASS 声明。

## 5. 发布结论与诚实边界

CP-VERTICAL-B 的服务端权威好友资料、好友农场、WATER Care 奖励/任务收敛，以及固定流程所需
THEFT/返回主页/刷新与 farm-gate 重启恢复均已闭合，可以发布独立 checkpoint。

本证据是 headed Playwright 自动操作与录屏，不冒充用户自由手工点击；PEST、Pet、Mail、
UnlockPlot 和 CLAIM_TASK 不在本 checkpoint 的完成声明内。
