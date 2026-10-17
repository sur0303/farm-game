# ScalePlan A1 Redis repository verification

本文件记录 A1 在基线 `91a1f92` 上的实现边界和本地证据。实现没有修改 core model、executor、state machine、route manager、cmd、deploy 或 `go.mod`。

## 已实现

- `internal/scaleplan/codec` 使用独立 DTO 固定 `scale-plan-record-v1` 的 JSON 字段顺序，拒绝未知字段、尾随数据和非 canonical JSON；checksum 为 `scale-plan-record-checksum-v1 || canonical_json`。
- codec 显式持久化 `PhaseReleasingExclusion`、可选 `CleanupIntent` 的 `FailureCode`/`ScaleResolution`/`ReleaseOutcome`，以及 `BlockedQuiescenceProof` 的 `ScaleResolution`/`ExclusionReleaseOutcome`；字段变化绑定 canonical bytes 和 checksum。
- `internal/storage/redis/scaleplan` 使用同一 realm hash-tag 的 KeyBuilder；动态 PlanID 只进入 lowercase hex member/key，idempotency/workload 使用 SHA-256 digest。
- Redis round-trip 使用真实 canonical JSON/checksum 加载，并通过新 Backend 实例验证重启后 A0.6 字段不丢失；apply-then-unknown 后继续 CAS 也保留字段。
- CreateOrGet、Load、CompareAndSwap、ListRecoverable 和 sealed blocked termination 均验证固定 hash shape、索引、accounting、lease/fence 与 corruption latch。
- ApplyPlan Lua 在首次写入前验证 manifest；Create/CAS/blocked 使用同一原子写集，并在同一脚本内最多清理默认 64 个已到期 terminal record。另提供 bounded `CleanupTerminal`，返回 `MORE_DUE`。
- lease、fence、mutation timestamp 和 blocked proof expiry 在 Lua 内使用 Redis `TIME`；未授权/伪造 blocked seal 在任何 Redis 访问前拒绝。
- `internal/testkit/scaleplanstore` 只启动 miniredis，不连接公司 Redis。

## 聚焦验证

```text
go test ./internal/scaleplan/codec ./internal/storage/redis/scaleplan ./internal/testkit/scaleplanstore -count=1
```

覆盖 A0.6 codec 的正向 round-trip、canonical/checksum 绑定、非法 outcome、旧 schema/缺失 proof 字段/损坏 JSON fail-closed，Redis A0.6 重启 round-trip、apply-then-unknown 字段保留，以及 Redis TIME in EVAL、Create/Load/CAS/List、CAS expected revision、幂等 replay/conflict、单 workload active、lease takeover/fence、terminal active release/retention、bounded cleanup、容量前 cleanup、corruption latch zero-write 和 forged seal rejection。

静态检查要求 A1 目录不出现 `SCAN` 或 Redis `KEYS` discovery path；所有 Lua 动态记录均来自显式 bounded `KEYS` manifest。

## A0.6 codec 集成护栏

当前 worktree 已合入 A0.6 core 字段，codec 仍保持显式 DTO 和现有外层版本策略：

1. `PhaseReleasingExclusion` 通过现有 `phase` 字符串字段持久化；`CleanupIntent` 作为 optional nested DTO 显式映射三个字段。
2. `BlockedQuiescenceProof` 显式映射 `scale_resolution` 和 `exclusion_release_outcome`；proof DTO 保持必填字段，旧 proof 缺字段在 canonical 重建时 fail-closed。
3. 外层 Redis `RecordSchema` 不变为 `scale-plan-record-v1`；core `schema_version=scale-plan-v2` 继续负责旧 core schema 的 fail-closed，普通旧记录（无 cleanup）保持兼容。
4. codec fixture 覆盖 release outcome 的 checksum 绑定、损坏/旧 schema/缺失 proof 字段拒绝；Redis fixture 覆盖 backend reopen 和 apply-then-unknown 后 exact Load。

## 未覆盖风险

验证使用 miniredis；尚未在目标 Redis 7.2 Cluster 上完成 Lua 执行时延、slot、ACL 和故障注入证据。生产接入前仍需 A5/A4 的真实 Redis/conformance gate，以及 A0.6 字段合并后的 codec 测试矩阵。
