# DF0 · Farm 最终规则纯领域包交接

日期：2026-08-03
任务 ID：DF0-FARM-DOMAIN-RULES
分支：`codex/df0-farm-domain-20260803`
基线 commit：`6b78d2f5b7793afe8636d919280c85c7849d6200`
状态：`IMPLEMENTATION_VALIDATION_PENDING`（纯领域包与单测完成；未接 Actor/存储/RPC）

权威依据顺序：docs/design/26～37 → CONTRACT-MANIFEST → 71/72/73 → Linux 实现审计 → 当前源码。
重点阅读：32（全文）、36（全文）；交叉阅读 27 §4.2（FarmOpportunityHeader/CandidateState）、30（权威提交边界）。

## 1. 交付范围

只交付第 32/36 号尚缺的 Farm **纯领域规则**、版本化配置模型和 mapper-free 单元测试。
所有导出函数为确定性 Transition/Decision：输入旧状态、命令、`trusted_now_ms`、冻结规则，输出
`CandidateState/Decision/*Error`；不接 Actor、Redis、RPC、Workflow 或进程入口。

新包：`internal/farm/domain`（清晰新名；未复用 `domain2/runtime2` 式重复主链命名，未改动
`internal/farm/actor|ingress|subscription|subscriptionreaper` 任何文件）。

| 文件 | 内容 |
|---|---|
| `internal/farm/domain/doc.go` | 包边界与权威引用 |
| `internal/farm/domain/types.go` | Plot/CropCycle/CareIncident/GuardSnapshot/FarmState 与 Clone |
| `internal/farm/domain/config.go` | PlotUnlockRule/CropRule/StealPlan/CarePlan/GuardRule 版本化配置与发布校验 |
| `internal/farm/domain/decision.go` | Status/ErrorCode/各场景 Decision 结构 |
| `internal/farm/domain/transition.go` | CandidateState、NewFarmState、DecidePlotUnlock、CheckUnlockEligibility、确定性 incident ID |
| `internal/farm/domain/plant.go` | DecidePlant（冻结 CropRuleSnapshot/StealPlan/CarePlan/Compendium） |
| `internal/farm/domain/care.go` | DecideCare（全局首次成功，重复 NO_CHANGE） |
| `internal/farm/domain/theft.go` | DecideTheft + DecideGuardInterception |
| `internal/farm/domain/harvest.go` | DecideHarvest（reward = base_yield − stolen） |
| `internal/farm/domain/guard.go` | ApplyGuardSnapshot 单调版本 + Canonical Hash |
| `internal/farm/domain/opportunity.go` | FarmOpportunityHeader 派生 |
| 8 个 `*_test.go` | 表驱动测试（见 §5） |

## 2. 复用/缺失/冲突 差异矩阵

审计对象：`internal/farm/actor/actor.go`、`internal/cropcatalog`、`api/gen/farm`、Linux 实现审计 §6.6/§6.7。

| 主题 | 现有实现（演示版，未改动） | DF0 纯领域包 | 处置 |
|---|---|---|---|
| 土地规模 | 新农场直接 12 块可用地，无锁定概念 | 24 稳定槽、初始 5 块免费解锁、6～24 固定顺序付费解锁 | **缺失→新建** |
| 解锁规则 | 无价格/等级/顺序/UnlockPlot | 版本化 `PlotUnlockRule` + `DecidePlotUnlock`（CandidateState，不扣 Player 资产）+ `CheckUnlockEligibility`（Player 侧纯校验） | **缺失→新建** |
| CropCycle 冻结 | proto `CropCycle` 仅 plant/mature/base_yield/stealable_limit（整数 3/2/2） | 冻结 rule_version、base_yield、stealable_limit、unit_coin_value、harvest_exp、StealPlanSnapshot、CarePlanSnapshot、CompendiumUnlockSnapshot | **冲突→新包按 32 冻结；旧 proto 模型留给后续 Actor 迁移任务替换** |
| 成熟 | 读时按 `now >= MatureAt` 派生（方向一致） | 同样时间派生；显式测试证明无 WITHER、到点不写状态 | **复用语义**，纯函数化 |
| Care | 单 `Watered` bool；无虫害、无 due_at、无全局首次成功语义 | WATER/REMOVE_PEST 独立 Incident，FIXED_OFFSET due_at 冻结于 Plant，全局首次成功、后续 NO_CHANGE，未处理不影响成熟/产量，随 cycle 结束 | **冲突→新建**（旧 `Watered` 语义不进入新包） |
| Theft | 每次固定偷 1、上限整数、无槽位曲线、无宠物 | 冻结 20% 总帽 + 10×2% 槽位、Q32-12 精确整数（base_yield 须为 50 的倍数）、每访客每茬一次、份额耗尽 NO_CHANGE | **冲突→新建** |
| Guard | 无宠物；主人固定 +1 coin | GuardSnapshot 版本化副本、P0 二元截留、截留按冻结单价补主人金币、不扣访客钱包、随机抽取作为输入注入 | **缺失→新建** |
| GuardSync 应用 | 无 | ApplyGuardSnapshot：高版本 APPLIED / 同版同 Hash 幂等 / 低版本 SUPERSEDED / 同版异 Hash INVARIANT_VIOLATION；AppearanceChanged 区分 Feed（隐）与 Switch（公开） | **缺失→新建** |
| OpportunityHeader | 无 | 固定大小派生头（source_farm_version/dirty_generation/next_opportunity_at_ms/candidate_mask），随 CandidateState 重算，不替代 FarmActor 裁决 | **缺失→新建** |
| Receipt/幂等/Unknown | actor 内 proto Receipt + blobstore CAS | **不实现**：属 30/31 权威存储边界；领域内的业务防重（Theft Attempt、Care 首次成功）以领域事实表达 | 明确边界，不重复造 |
| `internal/cropcatalog` | 演示数值（10/15/20 秒成熟、3/2/2 上限） | 不改动、不引用；新 CropRule 为独立版本化模型，后续由接入任务统一数据源 | 避免双主链依赖 |

## 3. 关键规则选择（与合同逐条对齐）

1. **CandidateState 不落权威内存**：所有 Decide 函数先 `Clone()` 再改候选；输入状态零副作用（有专门测试断言输入未被修改）。对应 27 号 BuildCandidateState 语义。
2. **初始 5 块免费解锁不产生购买记录**：`UnlockWorkflowID/UnlockRuleVersion/UnlockedAtMs` 均为零值；付费地块在 Commit 时冻结规则版本与 workflow_id。
3. **顺序校验**：`DecidePlotUnlock` 要求 `previous plot UNLOCKED`，否则 `PREVIOUS_PLOT_LOCKED`；已被其他 operation 解锁返回 `NO_CHANGE/ALREADY_UNLOCKED`（供 Coordinator 释放金币预留）。
4. **成熟/守护到期均为单次 `trusted_now_ms` 派生**：无 MATURE/EXPIRED 持久状态；`now == guard_until` 已失效；成熟后永不枯萎。
5. **Care**：incident_id = `cycleID#TYPE#1` 确定性派生，旧茬 incident 无法作用新茬（B3）；`now < due_at` 拒绝（防伪造）；全局首次成功唯一奖励义务（`RewardObligationCreated`）。
6. **Theft 整数规则**：`slot_quantity[i] = base_yield × slot_bp[i] / 10000`，发布校验要求精确整除且 > 0，总槽比 == total_cap ≤ 2000bp；运行时不四舍五入。10 槽全耗后第 11 位好友 `NO_CHANGE/NO_STEALABLE_QUOTA`；`stolen_amount` 按 gross 增加，保证 `owner + Σgross == base_yield`。
7. **GuardDecision**：P0 二元（lost ∈ {0, gross}）；`PARTIALLY_BLOCKED` 仅作预留枚举；随机抽取 `random_draw_bp` 作为函数输入（每裁决 Turn 捕获一次），重试/热更新/重载不重抽；访客钱包不动，主人补偿 = lost × 冻结单价。
8. **OpportunityHeader**：任何机会变化的提交从同一 CandidateState 重算；仅时间经过不写状态，`next_opportunity_at_ms` 暴露下次可能变化时间；mask 非 viewer 个性化（HARVEST_READY / CARE_ACTIVE / THEFT_CANDIDATE）。
9. **NO_CHANGE 与 REJECTED 区分**：`Error.Status` 显式携带，映射 32 §10.6 的终态分类。
10. **越权防控**：包内无 coin/seed/inventory/level 字段；等级与金币校验独立为纯函数 `CheckUnlockEligibility` 供 PlayerActor 侧使用。

## 4. 测试原始结果

命令与原始输出（本 worktree，Go 1.26.3）：

```text
$ go test -race -count=1 ./internal/farm/domain/
ok  	farmgame/internal/farm/domain	1.024s

31 个顶层测试全部 PASS（含表驱动子用例 80+）：
  TestNewFarmStateGenesis / TestUnlockPlotFullSweep5To24 / TestUnlockPlotRejections(6)
  TestCheckUnlockEligibility(5) / TestDecidePlotUnlockDeterministic
  TestDecidePlantFreezesRuleSnapshot / TestMaturityIsTimeDerived(3) / TestDecidePlantRejections(8)
  TestDecidePlantTimeOverflow / TestCarePlanDeterministicAcrossReplay
  TestDecideCareFirstSuccessThenNoChange / TestDecideCareWaterAndPestAreIndependent
  TestDecideCareRejections(5) / TestUnresolvedCareDoesNotAffectYield
  TestDecideTheftRejectsImmatureAndKeepsAttempt / TestDecideTheftOneAttemptPerVisitorPerCycle
  TestDecideTheftTenSlotsThenNoQuota / TestHarvestTheftOrdering(2)
  TestTheftAttemptDoesNotLeakAcrossCycles / TestDecideTheftRejections(3)
  TestDecideTheftDeterministic / TestGuardEffectivenessBoundaries(4)
  TestDecideGuardInterception(8) / TestTheftGuardSettlement(3)
  TestApplyGuardSnapshot(6) / TestCanonicalGuardSnapshotHashDeterministic
  TestDecideHarvestHappyPath / TestDecideHarvestRejections(4) / TestMatureCycleNeverWithers
  TestValidatePlotUnlockRule(6) / TestValidateCropRuleStealPlan(9)
  TestStealQuantitiesIntegerBoundaries(5) / TestValidateGuardRule(5)
  TestOpportunityHeaderLifecycle / TestOpportunityHeaderTracksUnlock

$ go test -count=1 ./...     # 全仓 PASS（与基线一致，无回归）
$ go vet ./...               # PASS
$ gofmt -l internal/farm/domain/   # 无输出
```

覆盖任务要求的矩阵：5→24 全量解锁、旧 crop_cycle（Care/Theft/Harvest 三处）、未成熟 Theft 不消耗
Attempt、Harvest/Theft 双向顺序、重复 Care（同/不同处理者、owner）、10 槽耗尽、整数边界
（50/100/250/5000 精确、49/51 拒绝、0 比例槽拒绝、cap>20% 拒绝）、宠物全拦/不拦/到期/边界、
部分拦仅预留不产生、确定性（Unlock/Theft 双跑一致 + 输入不可变）。

## 5. 证据级别

- `UNIT_PASS (-race)`：本包全部表驱动测试。
- `REPO_REGRESSION_PASS`：全仓 `go test ./...`、`go vet ./...` 在基线与交付态均绿。
- 未声明：进程级、Actor 接线、Redis、E2E 或容量证据（本任务明确不涉及）。

## 6. 未执行项（显式）

- 未改 `internal/farm/actor|ingress|subscription|subscriptionreaper`、Proto/gen、Redis Key/Lua、
  Workflow/Worker、cmd、Player/Task/Pet 领域、docs/design 语义。
- 未接 FarmActor：现有演示版规则（12 地、整数偷取、单 Watered、无宠物）仍在旧主链运行；替换属于
  后续 Actor/Workflow 接入任务。
- 未实现 StableOpaqueID/Workflow 身份（crop_cycle_id 由调用方注入；领域只校验非空与唯一）。
- 未实现 `DETERMINISTIC_RANDOM_WINDOW` 护理调度（预留枚举与字段）。
- GuardSnapshot 的 `snapshot_hash` 为包内 Canonical 构造函数；与第 29/31 号最终 Canonical 编码的
  字节级对齐需在 SCB-1 共享合同基线合入后复核。

## 7. 风险与 ICR

- **ICR-DF0-1（共享 Canonical Hash）**：`CanonicalGuardSnapshotHash` 目前是领域包私有实现
  （SHA-256 over 显式字段串）。第 36 号要求与 PetState/GuardSyncOrder 字节级一致，需待 SCB-1
  共享 Canonical 编码落地后切换到唯一生成源。当前不阻塞纯领域语义与测试。
- **风险：双规则并存期**。旧演示规则仍在生产主链。接入任务必须整体替换 Plot/Care/Theft 裁决，
  禁止新旧规则按命令混用，否则 OpportunityHeader 与 20% 曲线语义会被旧写路径破坏。
- **风险：base_yield 配置门槛**。Q32-12 要求 50 的倍数；运营发布工具必须调用
  `ValidateCropRule`，否则 `DecidePlant` 会 fail-closed 拒绝种植。
- Theft 的 `relation/permit` 准入、Care 的好友任务推进、Harvest 的图鉴解锁提交均在
  PlayerActor/Pair Authority 边界外，本包不裁决；接入时不得把这些检查下沉进领域包绕过 Permit。

## 8. 回滚

纯新增目录 `internal/farm/domain/` 与本文件；无对既有文件的修改。回滚 = 删除该目录并还原本次
提交（单 commit），不影响任何现有主链。

## 9. 下一依赖

1. P1A/SCB-1 共享 Canonical Hash 与 StableOpaqueID 基线合入后，对齐 `snapshot_hash` 生成源
  （处理 ICR-DF0-1）。
2. FarmActor/Workflow 接入任务：以本包替换 actor.go 内演示规则，接 UnlockPlot/Plant/Harvest/
  Care/Theft/GuardSync 主链与 Receipt/Order 边界。
3. DT0/DP0 纯领域包与本包并行，接入时注意 Player 侧 `CheckUnlockEligibility` 的复用点。
