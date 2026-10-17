# MA0-PLAYER-MAIL-ENTRY-STORE 交接文档（R2：I0 修复轮）

- 任务：MA0-PLAYER-MAIL-ENTRY-STORE（第 71 号 CP2A 后唯一立即派发项）
- base_tag：`cp2a-20260804`，base_commit：`f63cf8935b203196fe13cd0334b059bd777e2e6e`
- branch：`codex/ma0-player-mail-20260804`
- worktree：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-ma0-player-mail`
- session_id：`round15-ma0-player-mail-20260804`
- 日期：2026-08-04（R1 初版 + R2 I0 NEEDS_FIX 修复）

## 0. 证据等级声明

| 等级 | 状态 |
|---|---|
| UNIT_PASS（含黄金样例） | PASS |
| RACE_PASS（miniredis + `-race`） | PASS |
| NOSCRIPT / Timeout Unknown（before/after apply） | PASS（受控故障注入） |
| REAL_REDIS_PASS | **PENDING**：本环境无真实 Redis；`FARM_REDIS_URL` 门控的 live smoke 已实现但未执行 |
| Failover / 3000 万 DAU 容量 | **PENDING**：PLATFORM_PARAMETERS_PENDING，未宣称 |
| PM4（receipt item/byte 有界） | **PENDING**：见 §7 ICR-1，35A 布局缺口，未宣称完成 |

本交付只实现第 35/35A 号的“非 Actor PlayerMail Authority”。PlayerActor 附件发奖、
MailClaimState/MailClaimReceipt 权威提交、proto/CommandCatalog、cmd 接线均不在本次范围。

## 1. 提交序列

| commit | 内容 |
|---|---|
| `ca2c39c` | R1：`internal/player/mail` 领域模型、Codec、Port、Memory Fake |
| `6748d94` | R1：`internal/storage/redis/playermail` KeyBuilder、六个 Lua、Store |
| `08d62b8` | R1：分页/维护/Unknown/并发测试 |
| `5fa4f53` | R1：容量编码尺寸探针 |
| `25a2d26`+`7062aee` | R1：handoff 初版 |
| （R2 修复，见 git log） | R2-1 领域 FIX-1/2/3 + 同步 MemoryStore 漂移 |
| （R2 修复，见 git log） | R2-2 Redis V2 脚本族 + FIX-1/2/3/4 + 回归 |
| （本文档） | R2-3 ICR-1 + 本 handoff |

完整 hash 以远端 `origin/codex/ma0-player-mail-20260804` HEAD 与 `git log` 为准；
本文不内嵌会随提交失效的“最终 hash”。

## 2. 文件清单

允许写集内全部文件：

- `internal/player/mail/`：doc、config（LimitsV1 + ScanLease clamp）、types、lp、reward、
  identity、codec（5 种有界 LP blob）、sortmember、dueref、repository（三个 Port +
  DeleteAuthorization/ClaimProof）、delivery、claimmaterial、memory（参考实现+故障注入）
- `internal/storage/redis/playermail/`：doc、keybuilder、scripts（V1 冻结 + V2 现行共
  11 个版本化 Lua）、registry、classify、store、read、maintenance
- 测试：golden/codec/memory/pagination/fixregress（domain）；golden/keybuilder/store/
  read/maintenance/unknown/concurrency/capacity/fixregress/live（redis adapter）
- `docs/verification/ma0-player-mail-handoff.md`（本文件）
- `docs/verification/ma0-player-mail-icr.md`（ICR-1：receipt 有界性缺口）

未触碰：PlayerActor、PlayerIngress、FarmActor、Workflow、api/proto、cmd、R1 公共
KeyBuilder/ScriptRegistry、docs/design、integration 工作树。

## 3. R2 修复清单（I0 NEEDS_FIX）

| 编号 | 问题 | 修复 | 回归测试 |
|---|---|---|---|
| FIX-1 | Expire worker 延迟窗口内 Delete/MarkRead/Projection 因 `is_visible` 比较 `expire_at>now` 而跳过计数转移，后续 expire 又 NO_CHANGE/STALE，Summary 永久漂移 | Lua `is_counted(sf)` 物化计数谓词（deleted_at=0、auto_evicted=0、expired_at=0），wall-clock 比较只留读路径；MemoryStore 同语义 | `TestFix1*InExpireDelayWindow`（domain+redis 各 3 个：Delete/MarkRead/Projection + STALE→重登记→NO_CHANGE 不二次扣减） |
| FIX-2 | DeleteMailV1 仅凭字符串 `ALLOW_WITH_CLAIM_PROOF` 放行，未绑定 mail_id/claim_material_hash | `DeleteAuthorization`+`ClaimProof{MailID, Claimed, ClaimMaterialHash, ReceiptRef}` 强类型 Port；Store 结构校验；DeleteMailV2 对 immutable Entry 防御复核，失败返回 CLAIM_PROOF_INVALID（Go 合同错误，绝不删除） | `TestFix2*`：A 邮件 proof 删 B、hash 不同、receipt 为空、plain ALLOW 带 proof 全部拒绝；直接伪造脚本调用被拒；正确 proof 成功 |
| FIX-3 | DeliveryDedup 保留 90d 但 67d 后无 DueRef 能找到它，tombstone 永久占位 → 256 次后 CAPACITY_EXCEEDED | GcMailV2 同槽读 dedup，`effective_purge=max(entry.purge, dedup.retain_until)`；NOT_DUE 返回 effective 分值；到期同次删除 Entry/State/Dedup；dedup 缺失/畸形 fail closed（RECONCILE_REQUIRED） | 30d/37d/90d 组合：首 GC NOT_DUE@90d → 重登记 → 到期 PURGED、DeliveryRecords--；保留窗内重复 delivery 回原 mail_id；dedup 缺失拒绝 GC |
| FIX-4 | ListPage 先 HMGET 后记账，单次响应可超 PageScanBytes | 每批候选数 ≤ 剩余 byte budget / 最坏单候选编码（MaxMailEntryBytes+MaxMailStateBytes+SortMember）；末页向前探 1 个 member，与 Fake 精确对齐 | `TestFix4RedisByteBudgetPaging`（最大 Entry/State + 紧 byte budget 多短页完整遍历） |
| 附带 | contract violation（CROSS_SLOT/RECONCILE_REQUIRED/CLAIM_PROOF_INVALID）被吞进 typed status | 全部改为 Go error 上抛 | `TestFix2LuaDefenseDirectly`、`TestFix3RedisGcMissingDedupFailsClosed` |
| 附带 | Memory/Redis 漂移：scan lease TTL clamp、insert 容量检查/淘汰顺序、末页 cursor | 共享 `mail.ClampScanLeaseTTL`；MemoryStore 容量检查先于淘汰、迭代有界；末页探针对齐 | 现有 scan lease/eviction/pagination 测试 |

脚本版本纪律：V1 六个脚本源字节未动（黄金 SHA 不变）；V2 四个脚本新名新黄金。

## 4. 合同条款 → 实现/测试矩阵

| 条款 | 实现 | 测试 |
|---|---|---|
| 35 Q35-03 mail_id 确定性派生 | `mail.MailID`（StableOpaqueID farm-mail-id-v1） | `TestGoldenStableIdentities` |
| 35 M5 三类 Hash 分责 | `DeliveryPayloadHash`/`ClaimMaterialHash`（LP+SHA256，purge_at 不入 Hash） | `TestHashDutySeparation` |
| 35A §2.1 XXH64(seed=0)+LP 分桶 | `Profile.BucketFor`，4096 为 Pending 参数 | `TestGoldenBuckets` |
| 35A PM1 同槽 Hash Tag | KeyBuilder 逐字 tag + `AssertSameSlot` + Lua CROSS_SLOT 自检 | `TestGoldenKeys` |
| 35A §3 SortMember/排他游标 | `MailSortMemberCodecV1` | `TestGoldenSortMember`、`TestSortMemberOrdering` |
| 35A §7.1 DueRef | `mdr1_` LP+Base64URL 可逆编码 | `TestGoldenDueRef`、`TestDueRefValidation` |
| 35 Q35-04/35A §4.1 Insert 原子边界 | `InsertMailIfAbsentV1`：Entry+State+Dedup+Index+Summary+expire/gc Due 同 Lua | insert 全套 |
| 35A §4.1 惰性安全淘汰/满保护 | safe-evict ZSET + 原子 auto_evicted + 有界循环 | `Test*LazyEvictionAndFullProtected` |
| 35A PM4（Entry/State/Dedup/索引/扫描） | retained count/bytes、delivery records、entry/state 字节写前 Fail Closed | `Test*RetainedCapacityFailClosed`、`TestRedisByteCapacityFailClosed` |
| 35A PM4（**mutation/projection Receipt**） | **PENDING — ICR-1**：receipt 无清理路径，35A 布局缺口，未宣称完成 | — |
| 35A §4.2 MarkRead | MarkMailReadV2（FIX-1 计数谓词） | `TestRedisMarkReadFlows`、`TestFix1RedisMarkRead*` |
| 35A §4.3 Delete 资格 | `DecideDeleteEligibility` + DeleteAuthorization + DeleteMailV2 防御复核（FIX-2） | `TestFix2*` |
| 35A §4.4/PM3 投影单调 | ApplyMailClaimProjectionV2：claimed false→true，不清 deleted_at、不回 Index | `Test*ProjectionMonotonic*`、`TestFix1RedisProjection*` |
| 35A §4.5/4.6 Expire/GC | ExpireMailVisibilityV1（Redis TIME 复核 + versioned DueRef）；GcMailV2（FIX-3 effective purge） | `Test*ExpireAndGcWindows`、`TestRedisGcPreservesUnclaimedAttachment` |
| 35A §5/PM5 Unknown 重入 | 稳定 delivery/mutation/work 身份 + 持久 Receipt；transport 分类 NOT_SENT/UNKNOWN | `unknown_test.go`（before/after/重复重入/同身份不同 payload=DELIVERY_ID_CONFLICT） |
| 35 Q35-05/35A §3 分页 | 每页≤6、排他游标、有界多取、scan item/byte 上限（FIX-4 实际 HMGET 有界） | `read_test.go`/`pagination_test.go`/`TestFix4RedisByteBudgetPaging` |
| 35A PM7 禁 KEYS/HGETALL | 全部显式 Field HMGET/HSET、ZRANGEBYLEX/ZRANGEBYSCORE LIMIT；Lua 无 HGETALL/KEYS | 代码审查 + golden |
| 35 Q35-06 TrustedMailClaimMaterial | `ClaimMaterialIssuer.Issue`（重算 Hash、固定 audience、TTL≤15min） | `TestClaimMaterialIssue` |
| 35A §7.2 Scan Lease | `ClaimMailScanLeaseV1` + 共享 clamp | `Test*ScanLease` |
| 35 §2.3 立即可见 | visible_at>Redis TIME → NOT_VISIBLE_YET | insert 测试 |
| 35 Q35-07 60 封全保护 | MAILBOX_FULL_PROTECTED | eviction 测试 |

## 5. 数据布局（35A §2.3 逐字实现）

```text
{classic-farm:<env>:<zone>:player-mail:v1:b:<bucket>}:box:<player_id>    HASH
{...}:index:<player_id>        ZSET score=0 member=SortMemberV1
{...}:safe-evict:<player_id>   ZSET score=0
{...}:expire-due               ZSET score=expire_at_ms member=mdr1_...
{...}:gc-due                   ZSET score=physical_purge_at_ms
{...}:bucket-meta              HASH（布局/水位，预留）
{...}:scan-lease               HASH
```

Mailbox HASH 字段：`_meta:schema_version/mailbox_revision/visible_count/retained_count/
retained_bytes/unread_count/claimable_count/delivery_tombstone_count`，
`entry:<mail_id>`、`state:<mail_id>`、`delivery:<digest>`、`mutation:<digest>`、
`projection:<digest>`。所有 Value 是 schema 标记开头的 LP 序列，Lua 只按下标读标量。
（`receipt-due` 与 `_meta:receipt_count` 属 ICR-1 候选 A 的 35A 追加项，未实现。）

## 6. Unknown 语义

- 脚本永远返回确定性 status 数组；UNAVAILABLE_NOT_SENT/TIMEOUT_UNKNOWN 只由 Go 从
  transport 错误分类（与 R1 同一白名单）；CROSS_SLOT/RECONCILE_REQUIRED/
  CLAIM_PROOF_INVALID 作为 Go 合同错误上抛，不进入 typed status。
- Insert 重入 = 原 delivery identity；MarkRead/Delete 重入 = mutation digest；
  Projection 重入 = work digest；Expire/GC 重入 = DueRef + 版本复核（旧版本 =
  STALE_REF/NO_CHANGE，绝不误删新版本）。
- Socket timeout 从不虚报成功，从不映射为普通业务失败。
- **已知限制（不声称 exactly-once）**：旧 DueRef 的“脚本 ZREM → worker 用
  `RegisterDueRef` 重登记”之间存在崩溃窗口；worker 在窗口内崩溃会丢失该 due 成员，
  依赖 35A §7.3 Reconcile 抽样重建兜底（见 ICR-1 §5）。

## 7. 已知限制与 ICR

1. **ICR-1（OPEN）**：mutation/projection receipt 无清理路径、未计水位，PM4 对
   receipt 未闭环；候选 A（receipt-due 索引 + 脚本族演进，推荐）/候选 B（折叠进
   per-mail 结构，不推荐）见 `ma0-player-mail-icr.md`。在 35A 修订前本行保持 PENDING。
2. DueRef 重登记崩溃窗口（§6）。
3. FIX-3 代价：Entry/State 物理保留随 dedup 保留期延长（默认 67d→90d worst case），
   保留字节水位占用时间变长；这是 P0 保守方案的有意取舍，保持六个小型脚本族。
4. Redis ListPage 与 MemoryStore 在“满页+恰好耗尽”边界已通过 1-member 探针对齐；
   除此之外未发现残留语义漂移；如后续发现，以 Redis 适配器 + 黄金样例为准更新 Fake。

## 8. 初步容量/热 Key 估算（编码实测，非生产证据）

实测编码（`TestCapacityEncodingSizes`）：

| 样例 | Entry | State | SortMember | 每封合计（含双索引） |
|---|---|---|---|---|
| 最小纯文本 | 287B | 119B | 57B | ≈520B |
| 典型带附件 | 458B | 183B | 57B | ≈755B |
| 上限边界 | 4968B | 183B | 57B | ≈5.2KB |

- 最坏可见负载：3000 万 × 60 × 755B ≈ **13.6 TB**（纯 Value；未含 Key、Hash 对象头、
  复制、备份、碎片）。**Redis 全 Entry 仍是探针方案，Tcaplus Body + Redis Meta/Index
  对比待生产评审。**
- 单玩家 Mailbox HASH：60 × ≈755B ≈ 45KB；96 封保留 + 256 delivery 记录 << 768KiB。
  receipt 字段增长见 ICR-1（未闭环）。
- FIX-3 后 Entry/State 物理保留 worst case 90 天（dedup 保留期），保留水位占用窗口
  相应拉长；receipt-due 落地后需重估。
- 桶共享 DueIndex：4096 桶 ⇒ 平均 ~7.3k 玩家/桶；DueRef member ≈130B。
  桶倾斜、Lua p99、failover 全部待真实环境验证。

## 9. 平台 Pending 项

1. 真实公司 Redis：Cluster Hash Tag/Lua/ACL/noeviction/持久化/failover（live smoke 就绪）；
2. 4096 桶的 Slot/Key 分布实测；
3. 物理参数 96/768KiB/256/24/64KiB 按真实分布修订；
4. Redis 全 Entry vs Tcaplus Body 生产成本对比；
5. 最终脚本 SHA、Key 前缀、`.proto` 字段号冻结；
6. ICR-1 决议后的 receipt-due 布局与水位参数。

## 10. 后续接线 Port

- **PlayerMailClaimIngress（PlayerActor 接线，后续任务）**：
  `MailReadRepository.GetEntry` 强读 → `mail.ClaimMaterialIssuer{Issuer}.Issue(entry, now)`
  生成 `TrustedMailClaimMaterial`（audience=PLAYER_AUTHORITY，TTL≤15min，Hash 重算复核）。
  删除资格精确回源需要 `model:mail_claim_state` 读取 Port 提供 `mail.ClaimProof`
  （FIX-2 强类型：mail id + claim_material_hash + 非空 receipt ref + claimed），
  再调 `mail.DecideDeleteEligibility` 并用 `AllowDelete()/AllowDeleteWithProof(...)` 调
  `DeleteMail`。
- **MailClaimProjection（Dispatcher/worker）**：`ApplyMailClaimProjection(
  ClaimProjectionOrder)`；Order 丢失时由列表精确回源或 Reconcile 修复。
- **farm-worker**：`ClaimScanLease/ListExpireDue/ListGcDue/ExpireVisibility/GcMail/
  RegisterDueRef`；STALE_REF/NOT_DUE 后用 `RegisterDueRef` 重登记当前版本与
  effective 分值；批次继续受 max_due_items/bytes/script_calls/time_budget 约束。
- **Reconcile**：只允许用上述 Port 修复 Index/Summary/Projection/DueRef 缺项；
  禁止改写 RewardBundle 或 Player Claim 真值。
- **投递方**：冻结 `mail.DeliveryParams` 快照 → `mail.BuildMailEntry` →
  `InsertMailIfAbsent`；超时用原 delivery identity 重入。

## 11. 回滚方式

本任务全部是新增文件，不修改任何既有文件：回滚 = revert 上述提交序列
（或删除 `internal/player/mail`、`internal/storage/redis/playermail`、
`docs/verification/ma0-player-mail-*.md`），base `f63cf89` 的其他链路不受影响。
线上数据尚未存在，无数据迁移问题。V1/V2 脚本并存，回滚 R2 修复只需让 Store 切回
V1 名字（但 R1 已知缺陷随之恢复，不推荐）。

## 12. 验证原始结果（R2）

```text
$ go test -race -count=1 -timeout=180s ./internal/player/mail/... ./internal/storage/redis/playermail/...
ok  farmgame/internal/player/mail                1.056s
ok  farmgame/internal/storage/redis/playermail   3.625s

$ go test -count=1 -timeout=300s ./...
（全部 ok / [no test files]，full_exit=0）

$ go vet ./... && go build -buildvcs=false ./... && git diff --check
VERIFY_OK
```
