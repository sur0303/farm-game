# AR2 · 生产 Authority 切换与路由控制面决策

- 状态：`FROZEN_IMPLEMENTATION_DECISION`
- 日期：`2026-08-04`
- 基线：`cp3-20260804`
- 适用：`player-svr`、`farm-svr`、`farm-gate`、后续 RouteManager/AssignmentProvider 实现

## 1. 审计结论

CP3 已具备以下可复用组件：

- `ActorAuthorityStore` 的 Redis/Lua 实现；
- `PlayerActor`、`FarmActor` 的 `NewAuthorityRegistry` 接缝；
- `HolderLeaseGuard`、完整 `ShardWriteFence` 与 Unknown 解析；
- 静态 `AssignmentProvider`、本地路由快照、Redirect 基础组件；
- 公网 CommandCatalog、FirstHop Trusted Context 与 Participant IDL。

但是当前生产装配仍然是：

```text
cmd/player-svr → legacy blobstore.NewRedisStore
cmd/farm-svr   → legacy blobstore.NewRedisStore + legacy workflow.Store
cmd/farm-worker→ legacy workflow.Runner
```

而且当前没有生产实现负责：

```text
Claim/Renew/Transfer ShardWriteAuthority
→ 安装 HolderLeaseGuard
→ 发布与 Authority 完全一致的 Assignment Snapshot
→ Gate/Worker/Holder Watch 并原子替换路由表
→ FirstHop 校验 Holder/Boot/Revision/WriterEpoch/Lease
```

因此，`AR1 完成`只表示 Authority Registry 与 Store 组件已经可用，不表示生产 `cmd/*` 已经切换。

## 2. 冻结决策

### 2.1 禁止伪生产切换

以下做法禁止进入生产主链：

- 把测试用 `StaticFenceSource` 直接接入 `cmd/player-svr` 或 `cmd/farm-svr`；
- 让 Holder 自行增加 `writer_epoch` 或伪装 RouteManager；
- 复用上一次进程的 `holder_boot_id`；
- 只更新 Holder 本地 Epoch，却让 Gate 继续永久使用旧静态 Snapshot；
- 为了通过单机 Demo 而跳过 `HolderLeaseGuard`、FirstHop Route Fence 或 Store Fence。

原因是这些方案在进程重启后会出现静态路由与 Redis 写权不一致，无法同时满足第 27、29、30、31 号合同。

### 2.2 当前主线

在 RT1 完成之前：

```text
生产/多进程 Demo composition root
→ 继续显式使用 legacy store 模式

Player/Farm Participant、Workflow、业务规则开发
→ 必须同时提供 NewAuthorityRegistry 下的组件级/故障注入测试
→ 不得依赖 legacy store 特有行为
```

这不是放弃 Authority，而是把“领域实现”和“生产写权控制面切换”拆成两个可验证里程碑。

### 2.3 RT1 最小闭环

生产 Authority 切换前必须完成 `RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL`：

1. RouteAssignmentManager 是 Holder/Boot/WriterEpoch/AssignmentRevision 换代的唯一协调者；
2. 当前 Holder 只做 `RenewShardAuthority`，不能自行 Claim 新代次；
3. RouteManager 先提交 Redis `ShardWriteAuthority`，再发布同一元组的 Snapshot；
4. Gate、player-svr、farm-svr、farm-worker 使用可 Watch 的 AssignmentProvider；
5. Holder 以本地单调 Deadline 驱动 `HolderLeaseGuard`，到期立即 Fail Closed；
6. FirstHop 在 Execution-Accept 前校验完整 RouteHeader 与本地 Lease；
7. `NewAuthorityRegistry` 使用由 LeaseGuard 提供的动态 `FenceSource`；
8. 重启、Lease 到期、Redirect、Drain 与原 `operation_id` 重试形成 E2E 证据；
9. Authority 模式通过显式配置启用，legacy 保留为短期回滚开关，禁止双写。

RT1 首版可以只有一个 PLAYER Holder 和一个 FARM Holder，但必须走完整控制面语义；不要求首版实现热点迁移、自动均衡或 UC Proxyless Provider。

## 3. 对 Round 3 的影响

RT1 不阻塞以下短工作包：

- `PI1-A`：Player Participant 生产 Adapter；
- `FI1-A`：Farm Participant 生产 Adapter；
- `SC1-A`：Plant Workflow Definition/Coordinator；
- `SO1-A`：Friend Relation/Permit Redis Adapter。

这些工作包只导出构造器、Handler、Definition 和测试，不修改 `cmd/*` composition root。

RT1 阻塞：

- 把 `cmd/player-svr` / `cmd/farm-svr` 默认切换到 AuthorityStore；
- 宣称 Actor Authority 生产切换完成；
- 基于新 Authority 主链进行多 Pod/故障/容量结论。

## 4. 验收门槛

RT1 与最终 composition 切换至少要证明：

```text
旧 Holder Lease 有效时，新 Holder 不能强抢
旧 Holder Lease 到期后不能开始新 Turn
Store 拒绝旧 Fence 的迟到 Commit
新 Boot 取得新 WriterEpoch/AssignmentRevision
Gate 刷新 Assignment 后以原 operation_id 重试
Player/Farm Actor 冷加载恢复
不存在 legacy + authority 双写
```

只有上述证据完成，才能把生产模式从 `legacy` 改为 `authority`。
