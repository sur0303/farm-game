# CP-VERTICAL-B H5 Final Status

日期：2026-08-08
状态：**PASS / READY_TO_PUBLISH**

## 候选

```text
H5 branch             = codex/cp-vertical-b-integration-20260808
H5 verified code head = 81caee13ff4da86347ed40506c2dccf24b9db050
Backend verified head = c6fa6dac0a42e30ff0dab661e4bdefb431f30069
H5 base               = e8f90ed
Backend base          = 1ae95ce
```

## H5 闭合项

- 好友 roster 显示服务端权威 display name/avatar/level；不再从 owner id 或昵称首字伪造资料。
- 好友农场标题和资料只接受 `FarmSnapshot.owner_public_profile`。
- 解码完整 CareResult、Task summary 与 OperationResult。
- WATER 奖励以 baseline Player revision 后的有界 Bootstrap 刷新收敛；no-obligation 正确清理。
- CARE `PROCESSING` 查询未命中时，在当前有效订阅上重放同一 `operation_id`，避免冲突 Fresh Join。
- THEFT 后刷新访问者库存/revision，并展示双方权威 `harvestable_count` / `stolen_total`。
- `CLAIM_TASK` UI 保持禁用。
- 商店购买数量跨 500ms render tick 保持稳定。
- accepted share 只作为一次性本地导航意图；显式 Home 可覆盖旧恢复目标，重连不再返回旧好友农场。
- hard refresh 与真实 farm-gate 优雅重启后主动恢复 Join、Snapshot、好友、任务和资产。

PEST 延期；H5 不生成 PEST incident id。friend/share 继续只使用 HTTPS。

## 真实浏览器硬门

固定单测试、两个独立 BrowserContext/网页账号、headed Chromium、真实 HTTP/WSS/Redis Authority：

```text
登录 → 买种子 → 种植 → 收获 → 分享加好友 → 进入好友农场
→ 浇水 → 偷菜 → 返回自己农场
```

连续两轮通过：

- `.agentctl-cp-route-state/evidence/cp-vertical-b-20260808T094201Z/`
  - Playwright PASS（53.8s）
  - Protocol bot PASS
  - Authority audit PASS
  - 双页面视频、trace、里程碑截图齐全
- `.agentctl-cp-route-state/evidence/cp-vertical-b-repeat-20260808T095801Z/`
  - Playwright PASS（51.9s）
  - Protocol bot PASS
  - Authority audit PASS

验收还覆盖：WATER 双方 FarmChange + reward/revision、THEFT 双方计数 + 访问者库存/revision、
返回主页 pending=0、hard refresh 恢复，以及 farm-gate restart 后更高 connection generation 和权威状态恢复。

## 静态门

```text
vitest:       16 files / 143 tests PASS
tsc:          PASS
vite build:   PASS
audit:build:  web / staging / wechat PASS
diff check:   PASS
```

`validate:handoff` 需要实际 staging/production URL；未用虚构部署地址制造本地 PASS。

以上是 headed 自动化与录屏证据，不等同于用户自由手工点击。
