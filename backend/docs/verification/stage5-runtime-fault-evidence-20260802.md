# 阶段 5 运行级故障证据（2026-08-02）

版本：`2026-08-02-stage5-runtime-evidence`

对应 `docs/verification/s2-handoff.md` §7 剩余四项运行级证据，全部通过。
测试脚本：`scripts/test-stage5-fault-evidence`（21 项断言，运行结果 `RESULT: PASS (21 项全部通过)`）。

## 1. 测试方法

全程受控故障注入，无浏览器，安全边界：

- 拓扑为独立本地进程（player-svr:19800、farm-svr:19700、gate-a:18700、gate-b:18701），
  不动运行中的 docker 栈；
- 所有业务 key 使用 per-run 前缀（`farm:stage5e:<ts>:*`），退出时精确清理；
  session/ticket 为固定前缀 key，按 per-run 玩家 ID 精确删除且有 TTL；
- **Redis 失联通过本机 socat TCP 代理断开模拟**（farm-svr 经 16410、gate-b 经 16411），
  公司共享 Redis 本身不受任何影响；
- 玩家三角：A=农场主（短连接 hot 驱动写），C1=gate-a 长连接观察者，C2=gate-b 长连接观察者。
  好友关系经真实 HTTP 链路建立（curl 登录 + `/api/v1/share/create` + `/api/v1/share/accept`），
  双 Gate  Presence 经 `ws-stress`（load token + ClientHello + JoinFarm）建立。

关键实现细节：

- 冻结 Gate 用 `kill -STOP`（暂停续约，进程不退出，恢复用 `kill -CONT`）；
- socat `fork` 模式下必须 `pkill -f` 杀整个进程族，否则已建立的连接池连接仍然可用，
  模拟不出失联（首轮踩坑记录）；
- 观察者使用独立玩家 ID，避免与 A 的短连接互踢（同玩家重复登录触发 session fencing，
  首轮踩坑记录）。

## 2. 证据与断言

### E1 受控 Gate Boot Lease 过期 → Reaper 精确清理 Presence

冻结 gate-b（SIGSTOP，续约停止）26s 后：

| 断言 | 结果 |
|---|---|
| E1.1 `farm_gate_boot_reaper_dead_boots_total` 0 → 1 | PASS |
| E1.2 `farm_gate_boot_reaper_presences_removed_total` 0 → 1 | PASS |
| E1.3 `farm_subscription_hub_boot_fences` 2 → 1（仅剩 gate-a，存活 Presence 保留） | PASS |
| E1.4 gate-a 客户端连接不受 gate-b 冻结影响 | PASS |
| E1.5 SIGCONT 后 gate-b 恢复续约重新 trusted（readyz 200） | PASS |

### E2 受控 Redis 失联超过 trusted deadline → Gate 降级与恢复

断开 gate-b 的 Redis 代理：

| 断言 | 结果 |
|---|---|
| E2.1 trusted 窗口内 readyz 仍 200（短暂失联不误伤） | PASS |
| E2.2 超过 trusted deadline（15s）后 readyz=503 | PASS |
| E2.3 失联期间新登录被拒绝（http=401，owner-write 入口关闭） | PASS |
| E2.4 失联期间 WS 升级被拒绝（http=503） | PASS |
| E2.5 代理恢复后 gate-b 不重启进程自行转 Ready（readyz 200） | PASS |

### E3 多 Gate 坏 Lane 不阻塞健康 Lane

C2 重新加入后冻结 gate-b（其 downlink gRPC 无响应，命中 2s RPC deadline），
同时 A 经 gate-a 对农场 A 执行 hot 写循环（plant + 6 次 water）：

| 断言 | 结果 |
|---|---|
| E3.1 gate-a 写操作全部成功（`hot_water_ok=6`, `hot_water_err=0`, `join_errors=0`） | PASS |
| E3.2 gate-a 推送链路健康（`scenario_succeeded=1`） | PASS |
| E3.3 `farm_downlink_rpc_failure_total` 0 → 2（坏 Lane 失败独立计数） | PASS |
| E3.4 gate-a 长连接观察者在故障期间保持连接 | PASS |

### E4 Redis authority 不确定时 Presence 保留重试不误删

断开 farm-svr 的 Redis 代理 12s（≥2 个 reaper tick）：

| 断言 | 结果 |
|---|---|
| E4.1 `farm_gate_boot_reaper_errors_total` 0 → 2（查询失败被计数） | PASS |
| E4.2 `presences_removed_total` 保持不变（authority 不确定不误删） | PASS |
| E4.3 Hub `boot_fences` 保持不变（1） | PASS |
| E4.4 代理恢复后业务正常（ready=1, join_errors=0，无卡死） | PASS |

## 3. 交接 §7 清单关闭状态

至此 §7 五项运行级证据全部关闭（第三项"阶段 6 自动 passivation 后观察真实
Hub empty-lane retirement"已于 2026-08-01 在阶段 6 验证中取得：
`farm_subscription_hub_lanes_retired_total=4`，见
`stage6-activation-lifecycle-record-20260801.md`）。

## 4. 复现

```bash
# 需要 deploy/secrets/redis.conf.local(公司 Redis 凭据,git-ignored)
bash scripts/test-stage5-fault-evidence
# 期望末尾输出: RESULT: PASS (21 项全部通过)
```
