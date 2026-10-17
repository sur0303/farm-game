# SourceOrder 切流与 Redis IO 瘦身交接

日期：2026-08-14  
仓库：`developer_001@example.invalid:developer_026/farm-game.git`  
工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-source-order-migration-20260813`  
分支：`codex/source-order-migration-20260813`  
提交：本交接对应 `4f16fcf`；WSS CARE/THEFT 与 THEFT dest 证明在后续提交。  
基点：`af8f11d fix:batch-subscription-lease-renewals-by-physical-holder`  
远端：已 push，upstream = `origin/codex/source-order-migration-20260813`

---

## 0. 先读这一节

### 0.1 这份文档要解决什么

接手的人最容易做三件错事：

1. 以为 Plant/Harvest 还在走 `{work-v1}` WorkRecord FirstHop，去改已经删掉的双路径开关。
2. 为了省一次 Redis  hop，去改 `ShardFor` / hash tag，把 Player 和 Farm  silently 同槽。
3. 改 Lua 源码却不升版本名，把 `golden_test.go` 里冻结的 SHA 改掉交差。

相对压测 `af8f11d` 的提交清单、以及移植进来的图鉴/任务/宠物/邮件/商店，见 `docs/verification/source-order-redis-io-scenarios-20260814.md`。

### 0.2 工作树边界

只改本工作树。禁止编辑压测树：

```text
farm-game-friend-opportunity-convergence-20260812
```

本树是从压测树 HEAD `af8f11d` 拉出的独立 worktree / 分支，同一分支不能检出两次。压测还在旧树上跑。

本树的 `.git` 是 gitfile，真正的 gitdir 在：

```text
farm-game/.git/worktrees/farm-game-source-order-migration-20260813
```

### 0.3 一句话现状

公网跨 actor 命令（自己种、解锁、收获、浇水、偷菜源端）已经只走 SourceOrder。旧 WorkRecord FirstHop、`sourceorderflag`、farm-worker 的 `{work-v1}` Plant/Unlock/Harvest loop、以及 `LEGACY_*` 排空 loop 都已删。相关单测已绿。同槽 / 联合提交明确不做，留给上线之后有数字再议。

---

## 1. 分支与验证

| 项 | 值 |
|---|---|
| 相对基点 | 1 个提交，`+7070 / −1939`，70 文件 |
| 是否已推 | 是 |
| MR | 未开。创建页：https://git.internal.example.invalid/redacted-reference |

本轮跑过且通过的包（`go test -count=1`）：

```text
./internal/farm/ingress/
./internal/theftonline/
./internal/storage/redis/authority/
./internal/storage/authoritystore/
./internal/farm/actor/
./internal/player/participant/
./internal/player/actor/
./internal/worker/carerecovery/
./internal/worker/sourceorder/
./internal/worker/theftrecovery/
./internal/worker/plantrecovery/
./cmd/farm-svr/
./cmd/farm-worker/
```

**已做的真实进程 WSS smoke**（2026-08-14，本工作树）：

```text
PO1_E2E_MODE=SOURCE_ORDER_WSS go run ./cmd/po1-plant-e2e
```

结果 `SOURCE_ORDER_WSS E2E PASS`。真实 `player-svr` / `farm-svr` / `farm-worker` / `farm-gate` + Docker Redis。同一条胡萝卜：

1. 主人 JOIN → BUY → PLANT（Player `RESERVE_SEED` APPLIED）
2. 好友先 JOIN 自家农场发 `ClientRecoveryComplete`，再切主人农场（围观时发 recovery 会被 gate 踢）
3. CARE WATER：`CARE_REWARD` COMPLETED，好友 exp `0→1`
4. THEFT：`THEFT_VISITOR` COMPLETED，`SETTLE_THEFT_VISITOR` APPLIED，产物落到好友
5. 主人 HARVEST：`GRANT_HARVEST_REWARD` APPLIED，DueIndex 空

成熟必须 10s（目录胡萝卜 `WaterOffset=3s`）。THEFT dest 的 Spec 仍是 `THEFT`，地址已是 SourceOrder `SOURCE_PERSISTENCE_BUCKET`；`isAuthoritySourceOrderCall` 必须认这条，否则 dest 卡在 `REWARD_DISPATCHING` 找 generic WorkRecord。

**没做的验证**：浏览器 H5、进程重启故障注入、多 worker 抢占、容量压测。

---

## 2. 生产路径现在怎么走

Ingress 入口在 `internal/farm/ingress/service.go`。没有开关，没有 fallback。

| 公网命令 | Ingress | 谁是 source | 第二跳 |
|---|---|---|---|
| PLANT | `plant` → `plantSourceOrder` | Player：扣种子 + 写 PLANT 订单 | Farm Direct：`DispatchOnce` 下种 |
| UNLOCK_PLOT | `unlockPlot` → `unlockPlotSourceOrder` | Player：扣金币 + 写 UNLOCK 订单 | Farm Direct：解锁地块 |
| HARVEST | `harvestOnlineCommand` → `harvestSourceOrder` | Farm：清空地块 + GRANT 订单 | Player Direct：入包 |
| CARE | `care` → `careAuthority`（`AuthorityEnabled()`） | Farm Incident + CARE 奖励订单 | Player Direct：给浇水人经验 |
| THEFT 源端 | `admitTheft` → `CommitTheftSource` | Farm：DecideTheft + VISITOR/GUARD 订单 | fire-and-forget enqueue |
| SELL / BUY | player-svr 单 actor | 不走 SourceOrder | 无 |

已删除、不要再接回来：

- `sourceorderflag` 整包（永远 true 的双路径开关）
- `plantWorkRecord` / `unlockPlotWorkRecord` / harvest WorkRecord 分支
- `SetPlantOnline` / `SetHarvestOnline` / `SetUnlockPlotOnline`
- `buildPlantCoordinator` / `buildUnlockPlotCoordinator` / `buildHarvestCoordinator`
- farm-worker 的 `{work-v1}` PlantLoop / UnlockLoop / HarvestLoop
- farm-worker 的 `LegacyLoop`（`LEGACY_THEFT` / `LEGACY_CARE` / `LEGACY_HARVEST` 排空）

### 2.1 Plant

`internal/farm/ingress/plant_source.go`：

1. 校验只能种自己的农场（`RouteKey == session.player_id`）。
2. 组 `ReserveSeedPayload`，经 Player coordinator `ExecuteStep`。
3. Player 一次 Lua：扣种子、写 participant receipt、挂 PLANT SourceOrder + DueIndex。
4. `plantOrderDispatcher` → `Direct.DispatchOnce` 打 Farm dest。
5. 成功后 `plantSourcePublicResult` 读 Farm snapshot + Player private state。

幂等冲突映射在 `playerCoordinatorFailure`（unlock 共用）：

- 消息含 `PARTICIPANT_IDEMPOTENCY_CONFLICT` → `ERR_IDEMPOTENCY_CONFLICT` → 公网 **REJECTED**
- `DeadlineExceeded` / `Canceled` / `Unavailable` → 返回 nil，调用方打 **PROCESSING**
- 其它 `FailedPrecondition` / `InvalidArgument` / `PermissionDenied` 等 → **REJECTED**
- 不要再把 FailedPrecondition 全吞成 PROCESSING

### 2.2 Harvest

Farm 是 source。`FarmActor.HarvestOwner` 一次 Lua：地块清空、receipt、GRANT 订单、DueIndex。然后 `harvestOrderEnqueuer` Direct 给 Player 发胡萝卜。

产量是 `cropcatalog.SnapshotBaseYield = 500`，不是 legacy `BaseYield = 10`。卖 1 根后：coin 105、库存 499。Codex `SourceOperationId` 是 dest CAS id（如 `cas_9_2`），不是公网 `harvest-1`。

### 2.3 CARE

`care()` 见 `registry.AuthorityEnabled()` 后走 `careAuthority`。`SetLegacyCareEnabled` 对 Authority registry **无效**。

好友 CARE dest 只给浇水人 `RewardExp = 1`（`careRewardExpBase`），owner 不加 CARE 经验。无 admission 时 fail-closed：`REJECTED` + `ERR_FRIEND_PERMIT_INVALID`，不是 UNAVAILABLE。

CARE 不再写 `{work-v1}` WorkflowRecord。

### 2.4 THEFT

FirstHop **必须**有 `SourceCommit`。`theftonline.NewDispatch` 缺它会拒构图。`DispatchTheft` 不再 fallback `coord.Start`。

`CommitTheftSource`（`internal/farm/ingress/theft_source.go`）在 Farm 一次 Lua 写 DecideTheft + THEFT_VISITOR / THEFT_GUARD 订单。

dest 执行就是这两张 SourceOrder（Direct + farm-worker `THEFT_VISITOR`/`THEFT_GUARD`）。Pair DueIndex 恢复做 SourceCommit，**不再 CreateIfAbsent dest WorkRecord**。旧 `{work-v1}` THEFT 遗留单忽略。明细见 `source-order-redis-io-scenarios-20260814.md`。

### 2.5 farm-worker

READY 挂的是 SourceOrder recovery：

- FamilyPlant / FamilyUnlock：`buildCareRecovery(..., "PLANT" / "UNLOCK_PLOT")`，扫 PLAYER bucket
- FamilyHarvest：`"HARVEST"`，扫 FARM bucket
- FamilyCare：CareRewardOrder + Pair CARE
- THEFT_VISITOR / THEFT_GUARD：SourceOrder goroutine
- FamilyTheft：Pair DueIndex → SourceCommit THEFT_VISITOR/GUARD，不建 dest WorkRecord

旧 `{work-v1}` compatibility loop 已不启动，也不再打开 `workflow.NewRedisStore` / `workflow.NewRunner` / `workflow.NewRedisWorkRepository`。

---

## 3. Redis / Lua 本轮改了什么

脚本源一旦改，**必须升版本名**。`scripts.go` 注释：Sources are immutable。`TestScriptSHAGoldens` 钉 SHA-1，改 body 不升版本名会红。

| 当前脚本名 | SHA-1 | 做什么 |
|---|---|---|
| `CommitActorMutationWithOrdersV10` | `486185a058ca255a0a6a50d8ba87bb194f5c0c11` | ARGV 只传 upsert field，不再带全量 CandidateState |
| `ClaimWorkV4` | `3e653e1c51eb270dc1f3d04b8a9ae2c58dacc041` | 缺 lease 字段当 0 / 空闲 |
| `CompleteWorkV3` | `6b77fbf525538ccfa4b87223c679cdfc7a35399c` | Complete 后 `HDEL` 空闲 lease 字段，不再 HSET 空串 |

V1/V2 冻结不动。Schedule 创建 / Complete **没改**，仍写满 lease 字段（避免动 `ClaimScheduledWork*`）。

### 3.1 Sparse ARGV（V10）

HSET 本来就是 sparse upsert。以前 EVAL ARGV 把整个 CandidateState 再传一遍，IO 最大、又不动分片。V10 只传变更 field。单测：`TestCommitSparseArgvOmitsUnchangedModelValues`。

### 3.2 PENDING 订单不再写空闲 lease

创建 PENDING SourceOrder 时 **不写**：

```text
lease_holder_id / lease_holder_boot_id / lease_epoch
lease_token_hash / lease_expire_at_ms
current_step / last_error_class
completed_at_ms / reconcile_required_at_ms
```

仍写：身份、payload、state、work_version、retry_count、deadline、max_retry、created/updated。

读路径必须把缺字段当空闲：

- `GetSourceOrder`：`lease_epoch` / `lease_expire_at_ms` / `completed_at_ms` 用 `parseUint(..., false)`
- `ReadSourceWork`：`sourceWorkIdleDefault` 把 HMGET nil 收成 `""` 或 `"0"`，否则 `errMalformedBlob`

`TestCareSourceOrderAndDueIndexAreCreatedByOneCommit` 断言 PENDING HASH 没有上述 idle 字段。

### 3.3 Complete 后 HDEL

`CompleteWorkV3` HSET 只写 `state=COMPLETED`、`current_step`、`next_due_at_ms=0`、`completed_at_ms`、`updated_at_ms`，然后：

```text
HDEL lease_holder_id lease_holder_boot_id lease_epoch
     lease_token_hash lease_expire_at_ms
```

`ALREADY_COMPLETED` 在 lease fence 之前判断，删字段不影响幂等 Complete。Claim 对 COMPLETED 走 `NOT_CLAIMABLE`。

`TestWorkLeaseClaimRenewCompleteLifecycle` 断言完成后这些字段不在 HASH 里。

Direct 路径：GetSourceOrder（缺字段=空闲）→ ExecuteStep → **先 Claim 再 Complete**（Complete 仍要 lease fence）。

---

## 4. 单测夹具注意

`newTestStack` 已换成 `newSourceOrderTestStack`（`internal/farm/ingress/authority_stack_test.go`）：

- miniredis + Redis authority Farm/Player registry，shard count = 1
- in-process Player/Farm participant + `Direct.DispatchOnce`
- 玩家 9、10
- **Participant `Now` 必须是 `time.Now`（budget 用墙钟）**；业务时间用 `NowMs = current.UnixMilli()`
- 测试钟推进时要 `server.SetTime(*current)`，否则 CARE dest `dueScore > TrustedNowMs` 会 FailedPrecondition
- `SetFriendActionAdmission(friendActionAdmissionStub{})`
- `SetCareOrderEnqueuer` → `WorkTypeCareReward` + `direct.DispatchOnce`
- `NewRedisCareDependencyVerifier` 接到 player server

已删：`plant_online_test.go`、`TestHarvestWorkflowRecoversAfterFarmServiceRestart`。

`TestHolderRebuiltEnqueuesRejoinRequiredItem` 仍用 Memory blobstore。`actor.Plant` 的 `cropCycleID` 必须是 `workidentity` 稳定 opaque id，否则 save/fence 前被 `ValidateStableOpaqueID` 拒掉，发不出 `REJOIN_REASON_HOLDER_REBUILT`。

`delivery_guard_test.go` 的 operation id 不能有空格。

专门的 SourceOrder e2e：

- `TestPlantSourceOrderPublicCommandCompletesFarmAndDue`
- `TestHarvestSourceOrderPublicCommandGrantsAndClearsDue`

---

## 5. 同槽与联合提交：结论已经拍板

### 5.1 上线前不要做同槽

不是方向错，是不该挡开服。要换 `HashVersion`、迁存量 HASH/订单/authority。路由答辩里 Planned Shard Migration 还没资格化。只同槽、还保持两个 actor 两次 turn，plant 仍是两跳两张 Lua 一张订单，付了 rehash 的价，热路径几乎不变。

ARGV 瘦身和 Complete `HDEL` 才是本轮该收的 IO。

### 5.2 两个 actor 必须留着

Player / Farm 是两个地址、两套 mailbox、两套 writer epoch。不要收成「同一条 mailbox」。

同槽 ≠ 单个 actor。同槽只是让自己的两条 key 共用一个 Redis hash tag：

```text
现在（不同槽）
{classic-farm:prod:z1:auth:player:v1:s:42}:actor:10001
{classic-farm:prod:z1:auth:farm:v1:s:17}:actor:10001

同槽后（未做，仅说明）
{classic-farm:prod:z1:auth:owner:v3:s:N}:player:actor:10001
{classic-farm:prod:z1:auth:owner:v3:s:N}:farm:actor:10001
```

Cluster 槽只看 `{...}`。domain 从 tag 挪到后缀，两条 key 仍是两个 HASH，不是一个 HASH 的两个 field。

现有单测钉死 Player/Farm 不能 `AssertSameSlot`，`player/farm must not share holder`。`ShardFor` 的 hash 输入带 domain 字节（PLAYER=1，FARM=2），tag 里也有 `auth:player` / `auth:farm`。

### 5.3 同槽本身不够砍订单

| | 只同槽（两次 turn） | 同槽 + plant 联合提交 |
|---|---|---|
| Lua 次数 | 仍 2 | 1 |
| PLANT 订单 | 还要（两次提交中间会崩） | 自己种可以不要 |
| receipt | 两边都要 | 留一份幂等，不能零份 |
| 谁能写谁 | 各写各的 | 一个进程同时带两边 writer epoch |
| gRPC hop | 仍 2 | 可以收到 1 |

receipt 挡的是公网 `operation_id` 重试，和同不同槽无关。

偷菜 / 帮浇对端是别人，同槽也消不掉订单。

若以后真做：同槽和联合提交同一季度落地。只同槽不联合提交不划算。`FARM_WORKER` 今天跟 Farm 共用 farm hash，新版本要单独定义。

---

## 6. 故意没动的东西

- CARE dest 引擎本体（只换了 source 订单和 recovery 扫描）
- Schedule 创建 / Complete 的 lease 字段
- Player/Farm 同槽、HashVersion、存量 rehash
- 压测树、`farm-game-support-economy-*`、`farm-game-r2-route-integration-*`
- 把两个 actor 合成一个

`internal/workflow` 里的 `LegacyRepository` / `LegacyHandler`，以及 `theftonline.Coordinator` / `WorkerAdvancer`，代码还在、生产不接线。旧 `{work-v1}` THEFT dest 单不再恢复。

---

## 7. 关键文件

| 路径 | 角色 |
|---|---|
| `internal/farm/ingress/plant_source.go` | Plant FirstHop |
| `internal/farm/ingress/unlock_source.go` | Unlock FirstHop |
| `internal/farm/ingress/service.go` | harvest / care 入口；`plant`/`unlockPlot` 只转 SourceOrder |
| `internal/farm/ingress/theft_source.go` | THEFT source Lua |
| `internal/farm/ingress/authority_stack_test.go` | SourceOrder 测试栈 |
| `internal/player/participant/plant_source.go` | Player ReserveSeed + 挂单 |
| `internal/farm/actor/harvest_order.go` | Farm harvest source 订单 |
| `internal/farm/actor/theft_order.go` | Farm theft source 订单 |
| `internal/worker/sourceorder/` | 订单 id / WorkAddress / proof |
| `internal/worker/carerecovery/` | Plant/Unlock/Harvest/CARE/THEFT_* SourceOrder recovery |
| `internal/theftonline/composition.go` | `SourceCommit` 必填 |
| `internal/storage/redis/authority/scripts.go` | V10 / V4 / V3 Lua |
| `internal/storage/redis/authority/golden_test.go` | SHA 冻结 |
| `cmd/farm-svr/main.go` | 生产接线；THEFT `SourceCommit` |
| `cmd/farm-worker/main.go` | SourceOrder recovery；无 legacy loop |
| `internal/actorrouting/routing.go` | `ShardFor` 带 domain，不要 silently 改 |

---

## 8. 改 Lua 的规矩

1. 新拷一份源，常量名升版本（`CompleteWorkV2` → `CompleteWorkV3`）。
2. 旧 `*V1` / `*V2` 源码和 SHA 不动。
3. 先跑 `go test -run TestScriptSHAGoldens ./internal/storage/redis/authority/`，把 got SHA 写进 `golden_test.go`。
4. 读路径要能消化「字段不存在」。
5. 不要改 Schedule 脚本，除非单独排并测 `ClaimScheduledWork*`。

---

## 9. 建议的下一步（按优先级）

1. **开 MR**，对 `codex/friend-opportunity-convergence-20260812` 或当前集成基线。本分支从 `af8f11d` 长出，压测树若又往前走，先看是否要 rebase。
2. **跑一次真实 WSS / authority vertical smoke**（`cmd/po1-plant-e2e` 或仓库自带 smoke），确认种/收/浇/偷在真 Redis + 分进程下仍通。
3. 上线后用 profile 看自己种/收的第二次 hop 是不是热路径。是，并且愿意改「各写各的」契约，再把同槽 + 联合提交同一季度做。
4. 上线后用 Redis 指标看 V10 ARGV / ClaimAndComplete / THEFT 不再写 `{work-v1}` 是否落在热路径上。同槽仍按第 5 节另排。

不要做：为了好看把 Player/Farm 合成一个 actor；为了省 CROSSSLOT 热改 v2 hash。
