# DP0-PET-DOMAIN 交接文档

- task_id: `DP0-PET-DOMAIN`
- branch: `codex/dp0-pet-domain-20260803`
- worktree: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-dp0-pet-domain`
- base_commit: `6b78d2f5b7793afe8636d919280c85c7849d6200`
- 日期： 2026-08-03
- 状态： `COMPLETED`（纯领域层；跨 Actor/Redis/Proto 接线为后续依赖，未越权实现）

## 1. 范围与权威来源

实现第 36 号合同（v1.1-pet-state-version-guard-sync-hardening-freeze）的**纯领域层**：
简化 `PetState`、版本化/配置化守护规则、不可变 `GuardSnapshot`/`GuardSyncInput`。

权威阅读顺序已执行：36（完整） → 32（Theft/Guard 决策：FROZEN_32 二元截留、
GuardDecisionSnapshot 在 Attempt 时冻结）→ 30/31（Participant/Receipt 边界：Receipt/Outcome
走有界 Operation 机制，Order/DueIndex 同槽原子创建）→ CONTRACT-MANIFEST（36 STABLE_INPUT，
协议已由 28 v2.8 登记）→ 71 A3（纯领域包纪律）→ Linux implementation audit
（20260803，确认运行时无任何 PetState/Feed/Switch 实现）→ 当前源码。

明确未做（符合任务边界）：不创建 PetActor/pet-svr、不接 Redis/RPC/Workflow、不改 Proto、
不动 PlayerActor/FarmActor/Ingress、不写其他 Player 子领域、不改 26～37 语义。

## 2. 开始前基线核对

| 项 | 结果 |
|---|---|
| branch | `codex/dp0-pet-domain-20260803` ✓ |
| HEAD == base_commit | `6b78d2f` ✓ |
| 工作区 | 干净（仅未跟踪 `.codebuddy/`）✓ |
| `go test ./...`（基线） | exit 0 ✓ |
| `go vet ./...`（基线） | exit 0 ✓ |

## 3. internal/player/pet 现状审计（复用/缺失/冲突矩阵）

施工前 `internal/player/` 仅有 `actor/`（registry/state）与 `ingress/`（participant/service），
**不存在 pet 包**；audit 文档第 375 行确认"当前 Go/Proto/CommandCatalog 中没有可运行的
PetState/Feed/Switch"。

| 维度 | 结论 |
|---|---|
| 复用 | 无现存宠物代码可复用；包风格对齐 `internal/cropcatalog`（纯数据 + 查询函数、无外部依赖）；checked arithmetic 按 36 §3.4 自研最小实现 |
| 缺失（本次补齐） | PetState、PetCatalog/FeedRule/Definition 配置模型与发布校验、Unlock/Feed/Switch 状态转换、GuardRuleSnapshot/GuardSnapshot/GuardSyncInput、Canonical Hash |
| 冲突 | 无。未触碰 `internal/player/actor`、`internal/player/ingress` 及其他任何包；错误码全部采用 28 号已登记词汇，未自造 |

## 4. 交付物

```
internal/player/pet/
├── config.go     # CatalogSnapshot/FeedRule/Definition/GuardRule + ValidateCatalog + checked arithmetic
├── state.go      # State（PetState）、Clone/Owns/GuardEffective、GuardRuleSnapshot
├── snapshot.go   # GuardSnapshot + Canonical Hash（sha256 over 固定序大端二进制）+ GuardSyncInput
├── unlock.go     # UnlockByCoin（NO_CHANGE 幂等、费用需求输出）、ApplyTaskUnlockGrant（集合幂等）
├── feed.go       # Feed（trusted_now、8h/24h、整单拒绝、消耗/Receipt/Sync 输出）
├── switch.go     # Switch（保留 guard_until、NO_CHANGE 不推进版本、产出 Sync 输入）
├── errors.go     # ActionStatus + 28 号登记错误码
└── *_test.go     # 表驱动 + 并发 race 测试
docs/verification/dp0-pet-domain-handoff.md  # 本文
```

## 5. 关键决策与合同映射

1. **纯函数领域**：`UnlockByCoin/ApplyTaskUnlockGrant/Feed/Switch` 均为
   `(State, Catalog, Request, trusted_now?, 余额) -> Result{Status, ErrorCode, Candidate, ...}`。
   输入 State 永不被修改（Clone-on-write），Candidate 只在 `SUCCEEDED` 时提交。费用
   （`CoinFee`）、狗粮消耗（`ItemConsumption`）、Receipt 载荷、`GuardSyncInput` 全部是
   **输出需求**，由 PlayerActor Authority 层在一次 Commit 中落实——领域层不扣钱包、不写
   Inventory、不写 Farm、不建 Workflow（36 §3.2/§4.3/§5.3，任务第 5 条）。
2. **8h/24h 语义**（36 §4.1）：`base = max(trusted_now, old_until)`，
   `candidate = base + q*duration`（checked add/mul，溢出拒绝不环绕），要求
   `candidate <= now + horizon`，越界**整单** `REJECTED/PET_FEED_LIMIT_EXCEEDED`、零部分消耗。
   到期后从 now 重起、不补空档（§4.5）。`quantity<=0` 或 `>max_quantity_per_command` 拒绝；
   服务端按剩余时长重算实际允许量，不只校验 `q<=3`（§4.2）。
3. **错误码**：全部取自 28 号登记表：`PET_ALREADY_UNLOCKED/PET_ALREADY_ACTIVE(NO_CHANGE)`、
   `PET_NOT_FOUND/PET_NOT_UNLOCKED/PET_NOT_AVAILABLE/PET_FEED_LIMIT_EXCEEDED/
   PET_PRICE_CHANGED/PET_CONFIG_VERSION_CHANGED/PET_FEED_RULE_CHANGED/PET_GUARD_RULE_CHANGED/
   INSUFFICIENT_COIN/INSUFFICIENT_DOG_FOOD(REJECTED)`。36 §4.1 行文中的
   `PET_GUARD_LIMIT_EXCEEDED` 以 28 号登记的 `PET_FEED_LIMIT_EXCEEDED` 为准。
4. **双版本**（36 §6.1）：`pet_state_version` 随解锁/喂食/真实切换 +1；`guard_version` 仅随
   喂食/真实切换 +1；`NO_CHANGE/REJECTED` 两者均不推进。解锁不同步 Farm。
5. **切换保留守护槽**（36 §5.1）：真实切换 `guard_until_ms` 原样保留、不耗狗粮，规则快照从
   本次确认的 PetDefinition 重新冻结；同目标 `NO_CHANGE` 不建 Sync。
6. **GuardSnapshot 不可变与 Hash**（36 §6.2）：字段仅
   `schema_version/guard_version/active_guard_pet_id/guard_until_ms/rule`——足够 Farm 在 Theft
   时确定性裁决，不含 owner（farm_id 已标识）、不含钱包/库存/拥有集合等可变 PlayerState。
   Hash 覆盖上述全部字段的固定序大端 Canonical 编码，不覆盖自身；不 Hash JSON/默认 Protobuf。
   `BuildGuardSyncInput` 在决策同一时刻计算 Hash，保证 PetState 规则快照与 GuardSyncOrder
   载荷字节级一致（验收 24）。
7. **配置发布校验**（36 §8.1）：`ValidateCatalog` 强制 ≤32 只（第 33 只发布期拒绝）、pet_id
   唯一且为正、价格非负、bp∈[0,10000]、intercept 仅 FULL_GROSS、`duration>0`、
   `horizon>=duration`、`max_quantity>0`、乘加 checked。
8. **下架语义**（36 §8.1）：`coin_unlock_enabled=false` 只拒绝新金币购买；已拥有者可切换
   （`TestSwitch_Matrix/delisted but owned`）、可喂食、可被任务奖励幂等授予；定义不删除。
9. **守护生效**（36 §3.1/§4.5）：`GuardEffective(now) = 已选择 && now < guard_until`，严格
   小于，`now == until` 已失效；惰性计算，无 Timer、无 EXPIRED 写入。

## 6. 测试覆盖 → 验收矩阵映射（36 §8.4 中领域层可证部分）

| 测试 | 覆盖点 |
|---|---|
| `TestValidateCatalog_TableDriven` / `...RejectsThirtyThirdPet` | 32 上限（验收 15）、全部 §8.1 校验 |
| `TestUnlockByCoin_Success/...ReplayedOperationChargesOnce/...RejectMatrix/...OwnedSetCap` | 购买解锁、100 次重放只扣一次（验收 1）、NO_CHANGE 幂等、32 容量防线 |
| `TestApplyTaskUnlockGrant` | 任务奖励解锁、集合幂等 NoChange、金币/任务重叠不重复（验收：并发矩阵行 2）、下架仍可授予 |
| `TestSwitch_SuccessPreservesGuardUntil/..._Matrix` | 切换保留剩余时间、不耗狗粮、版本推进、未拥有拒绝、同目标 NO_CHANGE、下架可切换 |
| `TestFeed_DurationMatrix` | 8h/24h 边界（精确等于上限接受、超 1ms 拒绝）、过期不补档、`now==until` 已过期、整单拒绝零消耗 |
| `TestFeed_ValidationMatrix/...CheckedArithmeticOverflow` | 无激活宠物、三类版本过时、狗粮不足、溢出拒绝不环绕 |
| `TestFeed_SequentialCommandsSerialize` | 24h 边界串行重算、绝不部分扣道具（验收 4） |
| `TestFeed_ReplayDeterminism` | 同 （状态，请求，now) 100 次字节级一致（验收 2 的领域层前提） |
| `TestGuardEffective_Boundaries` | 未喂食/恰到期/已过期均无守护（验收 5） |
| `TestGuardSnapshotHash/...BuildGuardSyncInput/...CandidateStateImmutability` | Hash 稳定、同版本异载荷异 Hash（验收 7 前提）、解锁不产生 Sync、State↔Order Hash 一致（验收 24）、Snapshot 不可变 |
| `TestConcurrentDomainCallsRaceFree`（-race） | 领域层零共享可变状态，串行点只有 PlayerActor Mailbox |

Actor/Redis 层验收项（3、6~10、13、14、16~23、25）依赖后续集成包，不在本包可证范围。

## 7. 验证证据

```
gofmt -l internal/player/pet/        → 无输出
go vet ./internal/player/pet/        → exit 0
go test -race -count=1 ./internal/player/pet/  → ok（19 个测试函数全通过）
go test ./...                        → exit 0（全仓无回归）
go vet ./...                         → exit 0
```

## 8. owned/forbidden 写集合合规

- 实际写入：`internal/player/pet/**`、`docs/verification/dp0-pet-domain-handoff.md`。✓
- 未触碰：PlayerActor/FarmActor/Ingress、Proto/API 生成代码、Redis Key/Lua/Adapter、
  Workflow/Worker、cmd/**、其他 Player 子领域、docs/design/26～37。✓

## 9. 下一依赖（仅登记，不在本包实现）

1. **Player Integrator**（`internal/player/actor` Owner）：将本包接入 PlayerActor——
   PetState 持久化（`model:pet`）、有界 PetUnlock/Feed/Switch Receipt 与不可变
   StoredOperationOutcome（30/31）、同一 Authority Commit 落实 CoinFee/ItemConsumption/
   GuardSyncOrder(PENDING)+DueIndex（容量不足整单 fail-closed）、GuardSyncWatermark 同槽 Key。
2. **Protocol/Shared Contract Owner**：`GET_PET_STATE/PET_UNLOCK_BY_COIN/PET_FEED/PET_SWITCH`
   与 `APPLY_GUARD_SNAPSHOT` 的 Proto 字段号分配与生成；PetPageView 组装
   （PetState + GuardSyncWatermark 动态 guard_sync_status + 只读 Catalog 合并）。
3. **Farm Integrator**：`ApplyGuardSnapshot` Participant（版本/Hash Fence、
   SUPERSEDED/INVARIANT_VIOLATION）、Feed 只推进 record_version、真实 Switch 推进
   farm_version 并产生单份 FarmMetaDelta。
4. **Workflow Owner**：GuardSyncOrder 恢复（DueIndex 续投、order_id 复用 27/31 公式），
   旧 Order 的 SUPERSEDED 回写不得回退更高 APPLIED 水位。
5. **配置 Owner**：PetCatalog 发布管线接入 `ValidateCatalog`，并避免同一只宠物付费金币
   入口与必达任务奖励重叠（36 §8.3 末段）。
