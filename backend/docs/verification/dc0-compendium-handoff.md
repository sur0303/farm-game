# DC0-COMPENDIUM-DOMAIN 交接文档

- task_id: `DC0-COMPENDIUM-DOMAIN`
- branch: `codex/dc0-compendium-domain-20260803`
- worktree: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-dc0-compendium-domain`
- base_commit: `4e587936df183dd7e6e7725200f98dec38e69d5d`
- contract_baseline: `a78196a48d044a350a3b761c1d73b404f44a18c1`（docs/design/72 登记）
- 日期： 2026-08-03
- 状态： `COMPLETED`（纯领域层 DOMAIN_LOGIC_PASS；PlayerActor/Redis/Proto 接线为后续依赖，未越权实现）

## 1. 范围与权威来源

实现第 37 号合同（`v1.1-unlock-snapshot-seen-fence-capacity-freeze-candidate`）的**纯领域层**：
有界 `PlayerCompendiumState`、首次自种自收获解锁、原始 `UnlockDelta`、单调 Seen Fence、
版本化 `PlantCatalogSnapshot`/`ActivityArchiveDefinition`、确定性 `PageView` 与 fail-closed 容量闸门。

权威阅读顺序已执行：37（全文） → 32（§3.3 Plant 冻结 `CompendiumUnlockSnapshot` 随 CropCycle
持久化；§4.4 PlayerActor 一次提交 Inventory/Exp/Counter/图鉴/Receipt/StoredOutcome；§8.4 图鉴
接缝） → 28（§5.9 `GET_COMPENDIUM`/`ACK_COMPENDIUM_SEEN`、错误表
`COMPENDIUM_SEEN_ALREADY_ADVANCED`、`WORK_CAPACITY_EXCEEDED`） → 31（`model:compendium_meta`
+ `model:compendium_membership` 与 Player 共享 WriterClaim/RecordVersion/Durable Lane；
`CompendiumLimitProfile` 发布闸门） → CONTRACT-MANIFEST（37 STABLE_INPUT，Q37-01～08 已冻结） →
71/72/73（Round 1 DC0 卡：Compendium 活动档案与 Seen 纯领域缺口，DP1 §8.5.6 步骤） → 当前源码。

明确未做（符合任务边界）：不创建 CompendiumActor/compendium-svr、不接 PlayerActor/Ingress、
不改 FarmActor/Proto/Redis/Workflow/H5/cmd、不写 docs/design、不持久化、不发 RPC。

## 2. 开始前基线核对

| 项 | 结果 |
|---|---|
| branch | `codex/dc0-compendium-domain-20260803` ✓ |
| HEAD == base_commit | `4e58793` ✓ |
| 工作区 | 干净 ✓ |
| `go test ./...`（基线） | exit 0 ✓ |
| `go vet ./...`（基线） | exit 0 ✓ |

## 3. 现状审计（复用/缺失/冲突矩阵）

施工前 Harvest/Farm 域已有**最小图鉴行为**：`internal/farm/domain` 的
`CompendiumUnlockSnapshot`（types.go:126）在 Plant 时随 CropCycle 冻结（plant.go:101），
Harvest 原样透传进 `HarvestDecision.Compendium`（harvest.go:80、decision.go:189）。
Player 侧**不存在任何图鉴权威状态**：`internal/player/` 仅 `actor/`、`ingress/`、`pet/`、
`task/`，全仓 grep 无 compendium 玩家态实现。

| 维度 | 结论 |
|---|---|
| 复用 | Farm 域 Plant 冻结/Harvest 透传保持不变，直接作为本包输入来源（不删除、不平行复制）；包风格对齐 `internal/player/pet`（不可变 State + Clone-on-write + 排序去重有界集合 + ActionStatus/ErrorCode 词汇） |
| 缺失（本次补齐） | Player 侧永久解锁权威 State/UnlockFact、revision/seen_revision、DecideUnlock/AckSeen 转移、PlantCatalog/ActivityArchive 配置模型与发布校验、确定性 PageView（普通/活动中/活动档案/灰剪影/缺配置占位）、LimitProfile fail-closed 闸门、档案游标分页 |
| 冲突 | 无。本包不 import `internal/farm/domain`；Wire 侧 Snapshot→领域输入的字段级映射留给 M3 Player 集成方，避免玩家域反向依赖农场域 |

## 4. 交付物

```
internal/player/compendium/
├── doc.go        # 包定位与边界
├── types.go      # EntryCategory/ActivityState/DisplayState/ActionStatus
├── errors.go     # ErrorCode（28 号登记词汇）+ Error
├── limits.go     # LimitProfile + Validate + 确定性字节估算（状态/页面）
├── catalog.go    # PlantCatalogEntry/ActivityArchiveDefinition/CatalogSnapshot + ValidateCatalog
├── state.go      # State（排序去重 Facts、Revision、SeenRevision）+ 派生 HasNewUnlock
├── unlock.go     # UnlockSnapshot/UnlockCommand/UnlockDelta + DecideUnlock
├── seen.go       # AckSeen 单调 Fence
├── view.go       # PageView/PageCategory/PageEntry + BuildPageView + 档案游标分页
└── *_test.go     # 表驱动 + race 测试
docs/verification/dc0-compendium-handoff.md  # 本文
```

## 5. 关键决策与合同映射

1. **唯一解锁条件**（37 §3.1）：`DecideUnlock` 只在四条件同时满足时新增永久成员——
   `unlock_eligible && snapshot.crop==decision.crop && planted_by==owner && owner==actor.player`。
   偷菜/好友护理/邮件/仅持种子全部走 `NO_CHANGE` 且 `newly_unlocked=false`（SourceMatrix 表驱动），
   收获奖励本身在包外结算、不受影响。
2. **原始 Delta 不重算**（37 §3.2）：`UnlockDelta{newly_unlocked, crop_definition_id?,
   revision_after}` 在首次提交时一次成型；重复收获返回 `NO_CHANGE`、revision 不增长，
   Delta 仍原样输出供 StoredOperationOutcome 持久化，重试不得按当前集合重算。
3. **Seen Fence**（37 §5.2，28 §5.9）：`seen_after = max(seen, min(observed, revision))`；
   旧/重复 Ack 返回 `NO_CHANGE / COMPENDIUM_SEEN_ALREADY_ADVANCED`；GET（BuildPageView）是
   纯读，绝不触碰水位；红点只由 `revision > seen_revision` 派生，不落存储字段。
4. **活动档案四种组合**（37 §1.1/§4.2）：展示分组完全由版本化 Catalog 决定——活动中组
   （按 catalog 首现序）、档案组（按 `archive_order + activity_id` 全序，ValidateCatalog
   拒绝重复 order 破坏游标全序）；条目 DisplayState 只由权威 State 决定。同一
   `crop_definition_id` 被多活动复用时一个永久事实在各档案同时显示已解锁（37 §2）。
5. **缺 Catalog 不删事实**（37 §4.2）：永久成员在当前 Catalog 缺失时进入
   `CATALOG_MISSING` 占位组（`UNLOCKED_FULL` + `catalog_missing_placeholder`），并把排序后的
   `CatalogMissingCropIDs` 交给集成方告警；永不降级为未解锁、不静默隐藏。
6. **容量 fail-closed**（37 §5.4，31）：`LimitProfile` 六参数全部 >0 才可用；发布期
   `ValidateCatalog` 强制 entries/archives/collectible 上限；运行期首次解锁先查
   `max_unlocked_entries` 与状态字节估算，超限 `REJECTED / WORK_CAPACITY_EXCEEDED`
   （28 号登记码，RETRY_SAME_OPERATION 语义，集成方据此让 HarvestWorkflow 停
   `PROCESSING/RECONCILE_REQUIRED`，绝不先发奖励再静默跳解锁）。页面侧普通/活动中/占位组
   必须整组放得下（放不下即拒绝，属发布闸门失效），档案按剩余 item/byte 预算以稳定游标
   分页；单个档案永远放不下时拒绝而不是给出无法推进的游标。**无静默截断、无无界集合**。
7. **字节估算是版本化逻辑合同**：物理编码（有界排序数组 vs ordinal Bitmap）仍为
   PHYSICAL_PENDING（37 §5.4），本包用固定常量（state 32+64/条、page 64+128/组+160/条）
   让发布闸门与运行期 Fence 共用同一份估算，物理定型后只需换常量。
8. **领域状态与展示配置分离**：State 不含任何展示资源；Catalog 不含任何玩家事实；
   `display_definition_id` 永不参与解锁身份（37 §2 Q37-06）。
9. **遗留种子**（37 §4.2）：活动归档后 Plant 是否允许由 `legacy_seed_plantable` 决定
   （包外 Farm/Workflow 职责）；本包只保证冻结 Snapshot 的 `unlock_eligible=true` 在归档后
   依然按普通规则解锁（端到端测试覆盖）。
10. **错误码只用 28 号登记词汇**：`COMPENDIUM_SEEN_ALREADY_ADVANCED`(NO_CHANGE)、
    `WORK_CAPACITY_EXCEEDED`(REJECTED/临时)、`INVALID_ARGUMENT`(包内输入校验)；未自造新码。

## 6. 测试覆盖 → 验收/任务要求映射

| 测试 | 覆盖点 |
|---|---|
| `TestDecideUnlock_FirstUnlockProducesDelta` / `..._RepeatHarvestNoChange` | 首次解锁一次 Delta、revision=1；重复收获 NO_CHANGE 不增长 revision（任务 2；验收 1/4） |
| `TestDecideUnlock_SourceMatrix` | 偷菜、他人收获、eligible=false（邮件/护理/仓库）、snapshot/decision 不匹配均不解锁（任务：偷菜不解锁；验收 3） |
| `TestDecideUnlock_LegacySeedAfterArchive` + `TestBuildPageView_LegacySeedEndToEnd` | 归档后遗留种子按冻结 Snapshot 解锁并在档案完整展示（任务：遗留种子；验收 6/13/14） |
| `TestBuildPageView_ActivityLifecycleMatrix` | 活动结束四种组合：活动中±解锁、档案±解锁（任务：四种组合；验收 5） |
| `TestBuildPageView_CatalogMissingKeepsFacts` | 配置下架保留 UNLOCKED 事实 + 占位 + `CatalogMissingCropIDs` 告警清单（任务 4；验收 7/15） |
| `TestBuildPageView_CropReusedAcrossArchives` | 同一 crop 两活动复用均显示已解锁（验收 16） |
| `TestDecideUnlock_ConcurrentSamePlant` | 并发同植物串行化后恰一个 Delta=true（任务：并发同植物；验收 10/11） |
| `TestAckSeen_*`（6 个） | 首推进、重复/乱序/旧 Ack NO_CHANGE、迟到 Ack(5) 不清 revision=6 红点、钳制不超过 revision、seen<=revision 不变量（任务 3；验收 12） |
| `TestDecideUnlock_CapacityFailClosed` / `TestValidateCatalog_CapacityGates` / `TestLimitProfile_ZeroBoundsRejected` | 状态条目/字节上限、Catalog 发布闸门、零上限拒绝（任务 6；验收 17） |
| `TestBuildPageView_ArchivePagination` / `..._ByteBudgetPagination` / `..._ArchiveOrderIsStable` / `..._PaginationFailClosed` | 档案游标分页不截断不去重、字节预算分页、全序稳定、坏/陈旧游标与超限 fail-closed（任务 6；验收 18） |
| `TestDecideUnlock_ReplayDeterminism` / `TestBuildPageView_Deterministic` / `TestDecideUnlock_FactsStaySorted` | 同输入 100 次字节级一致；GET 纯读不动水位；集合排序去重（任务：deterministic view；验收 2/19 领域前提） |
| `TestConcurrentDomainCallsRaceFree`（-race） | 领域零共享可变状态，串行点只有 PlayerActor Mailbox |

Actor/Redis/Workflow 层验收项（重放 Receipt、RECONCILE_REQUIRED、E2E 等）依赖后续集成包，
不在本包可证范围。

## 7. 与旧最小图鉴代码的迁移接缝（本任务未接 PlayerActor）

现有最小行为（Farm 域 Plant 冻结 + Harvest 透传）**保留不动**。M3 接入时的接缝：

1. **Player 集成方**（`internal/player/actor` Owner）：在 `GRANT_HARVEST_REWARD` Participant
   内把 `farmdomain.HarvestDecision.Compendium`（`internal/farm/domain/types.go:126`）
   **字段级映射**为本包 `UnlockSnapshot`（六字段同名同义），调 `DecideUnlock`；`SUCCEEDED`
   时把 Candidate 与 Inventory/Exp/Counter/Receipt/StoredOperationOutcome **同一次
   Authority Commit** 提交（32 §4.4），并把 `UnlockDelta` 原样嵌入
   ParticipantReceipt/StoredOutcome（37 §3.2）。`WORK_CAPACITY_EXCEEDED` 时让
   HarvestWorkflow 停 `PROCESSING/RECONCILE_REQUIRED` 告警（37 §5.4），不得先奖后跳。
2. **持久化**（R1/31）：`model:compendium_meta` 存 count/revision/seen_revision，
   `model:compendium_membership` 存有界 Facts；与 Wallet/Inventory 共享 WriterClaim、
   RecordVersion、Durable Lane，不开第二条 Persistence Lane。
3. **读链**（Player Ingress Owner）：`CompendiumReadIngress` 在 Actor Turn 外捕获本地不可变
   CatalogSnapshot（先 `ValidateCatalog`）、短 Turn 读 State、Turn 外 `BuildPageView`；
   Catalog 暂不可用时查询失败允许重试，不阻塞其他写命令（37 §5.1）。
4. **协议**（P1）：`GET_COMPENDIUM`/`ACK_COMPENDIUM_SEEN` 与 `HarvestResult` 的
   newly_unlocked/crop_id/revision 字段号由唯一 Proto 源分配（PHYSICAL_PENDING）。
5. **配置 Owner**：PlantCatalog 发布管线接入 `ValidateCatalog` + `LimitProfile` 闸门；
   Archive 资源版本化保留，不做玩家扫描式回收（37 §4.1）。

## 8. 验证证据

```
gofmt -l internal/player/compendium/                          → 无输出
go test -race -count=1 -timeout=90s ./internal/player/compendium/...
   → ok  farmgame/internal/player/compendium  1.020s（26 个测试函数全通过）
go test -count=1 -timeout=180s ./...                          → exit 0（全仓无回归）
go vet ./...                                                  → exit 0
git diff --check                                              → exit 0
```

## 9. owned/forbidden 写集合合规

- 实际写入：`internal/player/compendium/**`、`docs/verification/dc0-compendium-handoff.md`。✓
- 未触碰：PlayerActor/Ingress、FarmActor、Proto/generated、Redis、Workflow、H5、cmd、
  docs/design、其他 Player 子领域、`internal/farm/domain`（只读复用）。✓

## 10. 未完成项与回滚

- 无未完成项（纯领域范围全部闭合）。
- 回滚方式：`git revert` 本分支提交即可；无共享文件改动，无数据迁移。
