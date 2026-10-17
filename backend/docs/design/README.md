# 经典农场后台设计文档

本目录已经完成第一轮文档治理。顶层主要保留现行合同、技术总控、验收合同和案例册；冻结前的方案已
完整转入 `legacy/`，不再要求同步，也不得作为实现权威。`30-redis.md` 暂按用户要求留在顶层等待
最终分类，其正文仍是早期 Tcaplus 权威阶段的材料，当前不得覆盖第 30/31 号合同。

> 当前协作状态：第 33～37 号好友、任务、邮件、简化宠物守护和图鉴合同已冻结，任务/邮件/宠物/图鉴接缝已同步到第
> 27/28/29/31 号；第 33A 与 35A 号非 Actor 权威 Redis 物理合同均已成为 `STABLE_INPUT`，平台参数与
> 安全选型仍待探针；第 40 号已经按 26～37 现行合同和 33 类公开命令完成收口。W5B 已在 Linux
> `a949761` 完成双 Gate/Drain/Fresh Join、200 人续租和 GateBoot 证据；MA0 PlayerMail 已由 `5e31c2d`
> 合入并发布 `cp2-20260804`，独立审查无 P0/P1。第 71/72/73 号现以 Round 2 为当前轮次；合同内容
> 基线仍为 `a78196a`，I0 从 `cp2-round2-20260804` 按独立 worktree、写锁和 handoff 门禁派发。
> 精确状态见
> [CONTRACT-MANIFEST](./CONTRACT-MANIFEST.md)。

## 唯一阅读顺序

1. [CONTRACT-MANIFEST](./CONTRACT-MANIFEST.md)：先确认版本、状态和唯一职责；
2. [26 · 接入与会话](./26-access-session-contract.md)；
3. [27 · 路由、Actor 与 Workflow](./27-access-routing-actor-workflow-freeze.md)；
4. [28 · 公网协议与 CommandCatalog](./28-command-catalog-public-protocol.md)；
5. [29 · 内部 RPC 与可信上下文](./29-internal-rpc-trusted-context.md)；
6. [30 · Actor 权威 Store](./30-actor-authority-store-contract.md)；
7. [31 · Redis 物理与容量](./31-actor-authority-redis-physical-contract.md)；
8. [32 · 核心经营场景](./32-core-business-scenario-contract.md)；
9. [33 · 好友、分享与权限](./33-friendship-share-access-contract.md)，业务语义与WorkPlacement已冻结；
10. [33A · 好友权威 Redis 物理合同](./33A-friendship-authority-redis-physical-contract.md)，当前为
    `STABLE_INPUT / PLATFORM_PARAMETERS_PENDING`，只负责 Pair/Quota/ShareIssuer 的 Key/Lua/GC/ACL；
11. [34 · 任务系统](./34-task-system-contract.md)，Q34-01～Q34-08 已冻结；任务公网命令、访问确认和红点物理边界已跨合同登记；
12. [35 · 系统邮件与附件领取](./35-mail-system-contract.md)，v1.2，Q35-01～Q35-08 已冻结，P0 邮件奖励明确排除宠物解锁；
13. [35A · PlayerMail 非 Actor 权威 Redis 物理合同](./35A-player-mail-redis-physical-contract.md)，当前为
    `STABLE_INPUT / PLATFORM_PARAMETERS_PENDING`，35A-01～35A-10 已冻结，只负责 Entry/State/Index/Lua/GC/容量；
14. [36 · 简化宠物守护系统](./36-simple-pet-guard-contract.md)，v1.1，Q36-01～Q36-08 已冻结，包含状态版本、不可变结果与 GuardSync 水位；
15. [37 · 植物图鉴与活动档案](./37-plant-compendium-contract.md)，Q37-01～Q37-08 已冻结；
16. [40 · 协议与技术收口总控](./40-core-technical-closure.md)，当前 `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING`；
17. [60 · 接入与会话验收](./60-access-session-test-acceptance-freeze.md)；
18. [61 · 性能、弱网恢复与平滑更新统一方案](./61-performance-resilience-smooth-update-plan.md)，当前为
    `ACTIVE_EDIT / PLATFORM_PARAMETERS_PENDING / IMPLEMENTATION_VALIDATION_PENDING`，只组合现行合同，不覆盖第 26～31、60 号；
19. [71 · 多 Agent 实施编排](./71-multi-agent-implementation-plan-draft.md)，当前为 `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING`；
20. [72 · Agent 工作包手册](./72-agent-work-package-playbook-draft.md)，当前为 `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING`；
21. [73 · Linux 多 Agent 施工调度与 Prompt 手册](./73-linux-multi-agent-dispatch-runbook.md)，当前为
    `STABLE_INPUT / IMPLEMENTATION_VALIDATION_PENDING`，只负责当前 Linux 基线的实际派发；
22. [75 · 好友互动机会 Header、护理与偷菜 Receipt 实施及迁移计划](./75-farm-opportunity-readpath-care-implementation-plan.md)，
    当前为 `STABLE_INPUT / I0-I3A_IMPLEMENTED / I3B-I5_PENDING`；Header/Facts 权威地基、不激活 Actor 的批量精确读链和好友查询 Provider 接缝已完成，FirstHop/Runtime导航接线与服务级A/B压测仍待实施；
23. [76 · 好友互动机会 Summary 与 Authority Header A/B 压测方案](./76-friend-opportunity-summary-ab-benchmark-plan.md)，
    当前为 `STABLE_INPUT / PRELIMINARY_BENCH_READY / CAPACITY_PENDING`；本地命令形态预基准已可执行，最终仍以同一服务级流量轨迹比较无 Summary、Summary 与业务写隔离基线；
24. [核心场景案例册](./core-scenario-casebook.md)，只用于解释，不覆盖合同。

## 目录分层

| 目录 | 性质 | 维护规则 |
|---|---|---|
| 顶层 26～40 | 现行合同与总控 | 按唯一职责更新；版本与 Manifest 同步 |
| 顶层 60 | 测试与验收权威 | 不重新定义业务或协议 |
| 顶层 61 | 性能与连续体验综合方案 | 只组合 26～31、60 的语义与验收；物理参数由探针冻结 |
| 顶层 71～73 | 正式实施编排 | 只转译合同为施工任务；实现与生产能力仍须独立验证 |
| 顶层 75 | 好友机会与护理聚焦迁移计划 | 先完成合同迁移，再按单写者边界派发代码；不覆盖 26～33 |
| `core-scenario-casebook.md` | 案例说明 | 冲突时服从 26～37 |
| `30-redis.md` | 待分类参考 | 暂留顶层；与 30/31 冲突时服从 30/31 |
| [`backlog/`](./backlog/README.md) | 后续领域输入 | 不是当前实现合同，提炼后再进入新编号合同 |
| [`reports/`](./reports/README.md) | 特定日期汇报快照 | 保留原貌，不持续同步 |
| [`legacy/`](./legacy/README.md) | 冻结前历史方案 | 不同步、不修正文义、不作为施工依据 |
| `assets/` | 图和源文件 | 使用前必须确认它对应的合同版本 |

## 文档修改规则

1. 一个主题只能有一份唯一权威合同；
2. 修改前在 Manifest 将文档标为 `ACTIVE_EDIT`，同一文档只允许一条写入线；
3. 修改完成后更新版本、状态和 Manifest；受影响的下游总控/实施计划只在其非
   `PAUSED_BY_USER` 时同步，暂停期间只登记“恢复后需 Rebase”；
4. 不回改 legacy 来“保持一致”，历史材料保持历史原貌；
5. 实施记录和测试结果放入 `docs/verification/`，不能反向覆盖合同；
6. Linux 源码实现状态必须带 branch、commit、dirty 状态和验证证据。
