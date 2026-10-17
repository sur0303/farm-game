# 当前施工基线与草稿依赖矩阵（2026-08-01）

> **状态更新（2026-08-02）**：合同 26/27/28/29/30 已冻结并入库 `docs/design/`
> （提交 `09dd1a4`），不再是草稿；60 号测试验收权威与 70 号 W0 施工主线已入库。
> 本文保留为 PR-1/PR-2 时期的历史施工记录。当前等待门只剩：
> **31 号**（Redis Key/Hash Tag/Lua/Slot/TTL/容量物理布局，编写中）与
> **32 号评审**（完整 Typed Payload/Result 迁移）；公网 Join 核心 Wire 已随 70 号计划
> W11A 先行冻结。历史正文如下。

这是一份临时施工记录，不是 26～29 号合同的归档，也不声明这些草稿已经冻结。
后续施工以用户提供的“调整后的正式施工顺序”原文为准；
`docs/design/60-contract-alignment-plan.md` 仅作为历史阶段记录，不再是当前主计划。

## 双仓基线

| 仓库 | 分支 | 本轮开始时 HEAD | 工作区状态 |
| --- | --- | --- | --- |
| `farm-game` | `codex/v03-access-session-hardening-20260730` | `6460b9971e8b10863c177e29fca08bf8b9de399e` | 含未提交的压测/观测性改动及 PR-1/PR-2 施工改动 |
| `classic-farm-h5` | `codex/v03-access-session-hardening-20260730` | `25f5e403de3f3ed0aa1ec952229fc28002c99bbd` | 含未提交的 Push-only Join 改动 |

详细文件清单以 `git status --short` 和最终分批提交为准，不把工作区未提交内容误写成上述 HEAD 已包含。

## 当前允许实施的稳定不变量

| 改动 | 依赖的草稿结论 | 当前发布单元 |
| --- | --- | --- |
| H5 必须同时收到匹配的 Join Result 与 Snapshot 才进入 `InFarm` | Join 双通道及 Fence | PR-1A |
| Join Result 不携带 FarmSnapshot；Snapshot 只走 Push | Join Push-only | PR-1A / PR-1B |
| Gate PendingJoin 有界、带 Deadline 和终态，迟到回调不能复活 | PendingJoin 生命周期 | PR-1C / PR-1D |
| CancelJoin 可凭 operation_id + 完整连接 Fence 清理 | 精确 Join 清理 | PR-1B |
| `TIMEOUT_UNKNOWN` 在同一 Actor Turn 进入 `RESOLVING_UNKNOWN` | Unknown 冻结 | PR-2 |
| 强写 Unknown 返回 `PROCESSING`，客户端轮询原 operation_id | Unknown 对外语义 | PR-2 |

## 等待门：本轮不得实施

除非用户明确确认相应草稿稳定，本轮不得修改或冻结：

- 公网 v3 Proto 的 field number / oneof 结构；
- Redis Key、Hash Tag 及新的 Lua 原子事务边界；
- Workflow Bucket 的物理布局与迁移状态机格式；
- `StoredOperationOutcome` 最终持久化结构。

内部 `logicrpc.DownlinkItem` 的 Join Fence 和内部 `CancelJoin` RPC 属于 PR-1B，
不等同于公网 v3 Proto 冻结。

现有 `blobstore` Fence Lua 的“同 epoch 不重复 HSET”来自此前压测工作区，
属于独立性能改动；在草稿等待门解除前不得与 PR-1/PR-2 混成同一正确性提交。

## 固定验证入口

`scripts/verify-s2.sh` 固定执行 Go vet/test/race/build、H5 单测、staging build 和 web build。
真实 Web E2E 需要已启动后端，使用：

```bash
FARM_VERIFY_WEB_E2E=1 ./scripts/verify-s2.sh
```

包含服务停启的故障恢复 Web E2E 必须显式开启：

```bash
FARM_VERIFY_WEB_FAULT_E2E=1 ./scripts/verify-s2.sh
```
