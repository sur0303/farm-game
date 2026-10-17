# 好友互动机会融合分支交接（2026-08-11）

## 1. 分支与基线

- 后端分支：`codex/friend-opportunity-fusion-r1`，基于 `bef0330`。
- H5 分支：`codex/h5-friend-opportunity-fusion-r1`，基于 `ac82629`。
- 两个分支均为独立 worktree，未合入 `Integration`。
- 脏实验工作区没有被整体合并；只吸收经过重新实现和测试的语义。

## 2. 最终链路

```text
Farm opportunity revision R 变化
→ Scheduler/Worker 到期重算并发 Hint(owner_id, R)
→ Gate 向在线好友连接下行
→ H5 300ms 合并 Hint，最多 16 个目标
→ GET /api/v1/friends/opportunity?hint_target=owner:R...
→ player-svr 校验好友关系、公开资料与 Farm Header/Facts/Receipt
→ CandidateWindow 与返回推荐在同一次 Navigation CAS 中提交
→ H5 展示最新的一张推荐卡
```

恢复链：

```text
登录 / 回自己农场 / 页面重新可见 / 120 秒 fallback
→ 不携带 Hint 目标
→ 调用 Navigator V3
→ 冷查询粗筛全好友，热查询复用 CandidateWindow
```

好友列表保持独立：只对当前页 20 人执行精确批查，不依赖 CandidateWindow。

## 3. 本次融合内容

### 后端

- 公网 Proto 和 Friendship RPC 增加有界 `hinted_targets`。
- Gate 解析重复的 `hint_target=owner:revision`；viewer 仍只来自登录会话。
- 最多 16 个目标，服务端再次归一化、去重并保留最大 revision。
- Hint 目标执行 canonical FriendRelation、PublicProfile、Farm Facts 与 TheftAttemptReceipt 精查。
- 有效目标 Upsert 到 CandidateWindow；已访问、已失效目标移除。
- 权威 revision 落后时返回 `UNKNOWN`，保留窗口但不展示旧 READY。
- CandidateWindow 变更与本次返回结果共用同一次 Navigation Repository CAS。
- Scheduler 默认生产候选从 250ms 调整为 1s；实验仍可显式配置 250ms。

### H5

- Hint 批次携带目标农场与 revision，不再通过翻好友分页寻找目标。
- 相同 owner 保留最高 revision；单请求最多 16 个目标，溢出目标进入后续重试。
- `visibility` 和 B2 `fallback-poll` 都会调用全局下一机会查询。
- `.env.web`、`.env.staging` 默认 B2，兜底周期 120 秒。

## 4. 验证结果

- 后端：`go test ./...` 全部通过。
- H5：28 个测试文件、234 个测试全部通过。
- H5：`tsc --noEmit` 与 Vite web 构建通过。
- 真实链路：浏览器 + farm-gate + player-svr + farm-svr + farm-worker + 公司 Redis，300 好友，B2，1 秒 Scheduler，通过。
- 真实定向机会查询：5 次，单次 10～12ms。
- 浇水边界到弹窗：1230ms。
- 成熟边界到偷菜弹窗：841ms。
- 真实链路只产生 1 次当前页好友列表请求，没有为 Hint 目标翻 15 页。

## 5. 保留问题

- 一次无 Hint 的全局 Navigator 查询返回 `503/UNKNOWN`。这是保守语义，不影响 Hint 主链，但应继续观测 UNKNOWN 比例和重试收敛。
- 当前 Pub/Sub Hint 会到达所有 Gate，再由 Gate 过滤本机连接；Gate 数量扩大后评估按 `gateway_id` 分组投递。
- 多 Worker 必须按 FARM Shard 分配互不重叠的 DueIndex 扫描范围。
- 本次是单机真实链路与 300 好友功能验收，不是 3000 万 DAU 容量证明。
