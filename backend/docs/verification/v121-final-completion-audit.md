# v1.21 M1–M6 当前工作区完成度审计

日期：2026-07-30

## 审计原则

本文件不以“有代码”“有报告”或“以前跑通过”为完成证据。每个里程碑分别核对：

1. 冻结计划要求；
2. 当前代码中的权威边界；
3. 当前工作区重新执行的测试；
4. 原始运行产物；
5. 仍然缺失、间接或需要外部状态变更的证据。

当前里程碑名称以 `v121-milestone-audit.md` 的冻结映射为准；早期按实现批次命名的
`v121-m1-*` 到 `v121-m6-*` 文件不能覆盖该映射。

## M1：RecoveryBarrier 与多 Gate Drain

要求：

- 客户端重连时不能在只有 Socket/ServerHello 的情况下提前恢复写操作；
- Gate Drain 必须停止接新连接并通知已有连接；
- 真实浏览器从 Gate A 摘流到 Gate B；
- Logic 下行必须按 `gateway_id` 回到连接实际所在 Gate。

当前证据：

```text
./scripts/test-multigate-drain
Playwright 1/1 PASS
Gate A active=1 → SIGTERM/DrainNotice/1012
Gate B active=1 → RecoveryBarrier → InFarm
好友花名册 ready
```

结论：**完成**。本地 readiness proxy 只模拟生产 LB 摘流，不冒充生产 LB 自身验收。

## M2：三条 Loop 与进程故障恢复

要求：

- 个人/经济 Loop 可重复闭合；
- 分享、好友列表、好友拜访和返回自己农场闭合；
- 好友 Care/Theft 与同场 FarmChange 实时闭合；
- 在线协调中断后由原 Workflow/Receipt 恢复；
- 全部业务进程重启后从公司 Redis 重建，不能回到进程内初始状态。

当前证据：

```text
npm run e2e:staging
4/4 PASS，31.5s

npm run e2e:workflow
1/1 PASS，用例 37.6s

故障注入后：
farm-gate / player-svr / farm-svr / farm-worker 全部 healthy
```

补充回归覆盖成熟作物拒绝本人/好友浇水，以及接受分享后邀请方无手动刷新实时看到
好友变化。

结论：**完成**。

## M3：最小 Stretch

要求：从 Task/Codex/Mail 中选择一个最小但服务端权威、可持久恢复的链路。

当前实现选择 Codex：

```text
Harvest Settlement.crop_definition_id
→ PlayerActor.ApplyReward
→ PlayerRecord 同一次 CAS 写入资产、Receipt、CodexEntry
→ PrivateState 下发
→ H5 植物图鉴
```

当前证据：

- Go 全量测试通过；
- H5 13 个文件、59 个测试通过；
- staging build 通过；
-真实浏览器首次 Harvest 解锁，断线重连后仍为 unlocked；
- M2 Worker/Player 故障恢复使用同一可靠 Harvest 链路。

结论：**完成最小 Stretch**。多作物、图鉴奖励、完成度红点不在本里程碑范围。

## M4：ActorAddress、Virtual Shard 与静态 ActorRouteClient

要求：

- `ActorAddress(realm, kind, id)`；
- 冻结、可测试的 `ShardCalculator`；
- 完整静态 `ShardAssignment`；
- `ActorRouteClient` 缓存/Revision 失效；
- 写权不能只靠路由，必须有存储侧 `writer_epoch` Fence。

当前重新执行：

```text
TestCalculatorHasFrozenKindSeparatedGolden        PASS
TestStaticResolverRequiresCompleteAssignments     PASS
TestActorRouteClientCachesAndInvalidatesByRevision PASS
TestWriterEpochFencesOldHolderAndLegacyCAS        PASS
Gate PLAYER/FARM conflict-key routing tests       PASS
```

结论：**完成冻结计划要求的静态第一版**。动态 Coordinator/Watch/Lease、在线 Redirect
和真实双 Holder 迁移是下一阶段分布式控制面，不伪装成本里程碑已实现。

## M5：Redis 生产失败语义与 Tcaplus 迁移形状

已经证明：

- Redis 7.1.5、master、1 replica、`noeviction`；
- 隔离 key Insert/Get/CAS、16 路唯一 winner、uncertain marker 读回；
- Redis writer epoch、Workflow Lease、Friend relation epoch、Share MULTI_ACCEPT；
- HA 探针稳态 48/48 写与 48/48 同连接 `WAIT 1`，独立读回零丢失；
- Tcaplus 五表候选 schema 可生成，primary/index descriptor 合同测试通过；
- 已建 `OwnerCoreRecord` 真表 Insert/Get/RecordVersion CAS/旧版本冲突再次通过。

明确反证/缺口：

- Redis 自报 `appendonly=no`、`save=""`；
- 当前 HA 基线 `role_changes=0`，没有发生平台切主；
- 没有把平台备份恢复到隔离实例；
- 因此没有真实 RPO/RTO、ACK 后主故障和恢复数据完整性证据。

结论：**未全部完成**。应用侧和测量工具已完成，实际切主与备份恢复必须由资源
所有者在维护窗口执行，步骤见 `v121-m5-platform-window-runbook.md`。

## M6：容量模型、可观测性与 30 分钟本地基线

原始产物重新核对：

```text
Join-only：400 attempted / 400 ready
Buy+Plant：100 attempted / 100 ready / 100 scenario succeeded
duration：1800s
login/connect/hello/join/scenario errors：0
unexpected disconnects：0
Join P95：7.463831ms
Plant scenario P95：6.88527ms
runtime samples：150
sample span：04:44:46Z → 05:14:42Z
Gate reject / slow-client kick / Actor mailbox reject：全程 0
```

容量报告明确区分 3000 万 DAU 模型输入与本地实测，没有把 500 连接当成生产上限。

结论：**完成 M6 第一份 30 分钟本地容量与可观测性基线**。1k+ 长连接、持续业务
QPS、热门 Farm、慢客户端和平台故障属于后续生产容量爬坡。

## 最终状态

| 里程碑 | 当前状态 |
| --- | --- |
| M1 | 完成 |
| M2 | 完成 |
| M3 | 完成 |
| M4 | 完成 |
| M5 | 应用与工具完成；平台切主/备份恢复未完成 |
| M6 | 完成第一份冻结要求的 30 分钟基线 |

因此当前不能把整个 M1–M6 目标标为完成。唯一剩余验收动作是 M5 的外部平台维护
窗口；它不能由代码测试、普通进程重启或未发生角色变化的稳态探针替代。
