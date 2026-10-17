# MA0-PLAYER-MAIL ICR-1：Mutation/Projection Receipt 的 PM4 有界性缺口

- 日期：2026-08-04
- 分支：`codex/ma0-player-mail-20260804`
- 状态：**OPEN，等待 35A 合同修订决议**；本 ICR 不改 docs/design，只记录缺口、证据与候选。
- 触发：I0 独立审查（NEEDS_FIX 轮）确认 `mutation:<digest>` / `projection:<digest>`
  HASH 字段写入后没有任何清理路径，也未计入 item/byte 水位，违反 35A PM4。

## 1. 条款与代码证据

35A §2.4 冻结 Mailbox HASH 字段族：

```text
mutation:<mutation_digest>          # MarkRead/Delete稳定结果
projection:<work_digest>            # ClaimProjection幂等Receipt
```

35A §5 冻结这些 Receipt 用于 Unknown 重入；35A PM4 要求"所有列表、Lua、DueIndex、
Receipt、正文和扫描都同时受 item+byte 上限约束"。但 35A §4（六个脚本）与 §6（Receipt、
保留与容量）没有冻结任何 receipt 过期发现索引、清理脚本或 receipt 专属水位参数；
`_meta:delivery_tombstone_count` 只统计 `delivery:<digest>`。

代码证据（本分支实现）：

- `internal/storage/redis/playermail/scripts.go`：MarkMailReadV2 / DeleteMailV2 /
  ApplyMailClaimProjectionV2 在 box HASH 写入 receipt 字段；无任何读取方会删除它们。
- Receipt blob 带 `retain_until_ms`（LimitsV1：30 天），但该时间只被编码，无人消费。
- 每个新 mutation_id / work_id 产生一个新字段；玩家长期累计的 MarkRead/Delete/
  Projection 操作数不受任何上界约束 → box HASH 字段数无界增长。

## 2. 在现有冻结布局内能否合规收口：不能

已逐一排除不新增物理 Key/DueKind/脚本的方案：

| 方案 | 结论 |
|---|---|
| HSCAN box HASH 找过期 receipt | 违反 35A PM7/§2.4（禁止整 HASH 扫描） |
| GC 时按 mail_id 反推 receipt 字段名 | 不可行：mutation digest 绑定 mutation_id（operation 维度），GC 不知道历史 mutation_id 集合 |
| meta 计数 + 写入时 Fail Closed | 无清理路径时邮箱在达到上限后永久拒绝 MarkRead/Delete/Projection，比现状更糟，且 35A 未授权该失败语义 |
| 进程内定时器/惰性读时清理 | 违反"不依赖进程内定时器"；读路径无 receipt 全集视图 |

因此该缺口需要 35A 增加物理元素（Key/DueKind/脚本或字段模型变化），超出实现 Agent
权限，按纪律冻结为 ICR。

## 3. 候选处理

### 候选 A（推荐）：receipt-due 发现索引 + 现有脚本族小幅演进

- 新增同槽 ZSET `<mail_tag>:receipt-due`（复用现有 bucket Hash Tag，无新 Slot 域）；
- MarkMailRead/Delete/ApplyClaimProjection 脚本在写 receipt 的同一次 Lua 里
  `ZADD receipt-due retain_until receipt_due_ref`（DueKind 新增 `RECEIPT_EXPIRY`，
  member 复用 mdr1_ 编码，附带 field 名）；
- farm-worker 复用现有 scan-lease + 有界 ListDue 模式；清理动作为
  GcMailV2 的兄弟版本（如 `GcMailReceiptsV1`）或对 GcMailV2 的版本化扩展：
  复核 Redis TIME >= retain_until 后 HDEL receipt 字段并修正 receipt 计数水位；
- meta 增加 `_meta:receipt_count`（及可选 receipt_bytes），写入时 Fail Closed，
  清理时递减，PM4 闭环。
- 影响面：35A §2.3 Key 总表 + §2.4 meta 字段 + §4 脚本族 + §6.1 参数，均为追加式修订；
  不改变业务语义、权威划分或公网协议。

### 候选 B：把 receipt 折叠进有界 per-mail 结构

- 不再按 mutation digest 独立存字段，只在 state blob 内保留"最近一次 mutation 结果"
  或每 mail 一个小型 receipt 子记录（随 Entry/State 同生命周期，GcMail 一并清理）。
- 代价：同一 mail 上 MarkRead 与 Delete 的 receipt 互相覆盖，Unknown 分辨能力变弱；
  改变 35A §2.4 字段模型与 §5 的"读 mutation Receipt 或原身份重入"语义；
  对并发多端 mutation 的幂等保证需要重新论证。
- 不推荐：削弱了 35A §5 已经冻结的稳定身份语义。

## 4. 当前风险与缓解

- 风险：高频 MarkRead（每次打开详情即写 receipt）+ 多 mutation_id 场景下，单玩家
  box HASH 字段数随时间线性增长；达到 Redis HASH 性能拐点前不会有正确性问题，
  但违反 PM4 的硬性有界承诺。
- 现有缓解：receipt blob 单体 ≤1KB（编码强制）；mutation/projection 操作频率受
  60 封可见上限间接约束（Delete/Projection 随邮件生命周期有限，MarkRead 是主要
  增长源）。**这不构成合规收口，只是风险量级说明。**
- handoff 的 PM4 行已降级为 PENDING（receipt 项），不宣称 PM4 完成。

## 5. 关联已知限制（非本 ICR 阻塞项，记录在案）

- 旧 DueRef 的"脚本 ZREM → worker 用 `RegisterDueRef` 重登记"之间存在崩溃窗口：
  若 worker 在两者之间崩溃，该 due 成员丢失，需要 35A §7.3 Reconcile（抽样重建
  DueRef）兜底。当前实现不声称该链路 exactly-once；P1 可考虑在 mutation 脚本内
  原子重登记（需要脚本内构造 due member 或同 Lua 附带 ZADD，属另一 35A 演进）。
