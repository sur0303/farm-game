# 71 · 经典农场多 Agent 实施编排基线

- 版本：`v2.2-core-gameplay-integrated`
- 日期：2026-08-05
- 状态：`ACTIVE_IMPLEMENTATION / CORE_GAMEPLAY_INTEGRATED / PRODUCT_CLOSURE_NEXT`
- 合同内容基线：`a78196a48d044a350a3b761c1d73b404f44a18c1`
- CP2A 代码合并点：`94155566b08bccfb453b344fdee046918f199953`
- CP2A 稳定名称：`cp2a-20260804`
- CP2 代码合并点：`5e31c2dea65136fa3c0486c6d77765dd5815588e`
- CP2 代码检查点：`cp2-20260804`
- Round 2 最终派发名称：`cp2-round2-20260804`（并行依赖澄清后创建）
- Round 2 后端合并点：`0564a5194d0c1df88b50d899db6fa8ecf8208bf8`
- Round 2 H5 合并点：`6e582d4dea3674b0f58b6f1d65d91cf07c09f308`
- P1B-CONSUMERS-2 派发名称：`round2-p1b-consumers2-20260804`
- P1B-CONSUMERS-2 最终候选：`a22dca9814680bd7d5d874ad7fc42ec6e4eb3015`
- P1B-CONSUMERS-2 后端合并点：`74de91925409efdfa9a5e53e6d551b394d4942ab`
- CP3 稳定名称：`cp3-20260804`

> 2026-08-05 当前进度覆盖说明：本文后续 CP3/Round 3 内容保留为历史施工记录；当前实现已经推进到
> `4e8f9d7`，Plant、Harvest、Care、Theft 与 200 人热点 Theft 证据已进入 integration。下一轮不再派发
> PI1/FI1/SC1 旧任务卡，以
> [2026-08-05 Linux 工作区真实实现进度审计](../verification/linux-workspace-implementation-audit-20260805.md)
> 的完成矩阵、剩余项和 Hero Scenario 次序为准。第 26～37 号仍是业务语义权威。

逐 Agent 的可派发任务卡见：
[72 · 多 Agent 工作包执行手册](./72-agent-work-package-playbook-draft.md)。

针对 2026-08-03 Linux 真实基线、可直接复制的派发 Prompt、检查点和当前轮次，见：
[73 · Linux 多 Agent 施工调度与 Prompt 手册](./73-linux-multi-agent-dispatch-runbook.md)。第 71 号继续
负责长期依赖和合并原则；第 73 号只负责当前基线下的具体派发，不拥有业务语义。

Linux 当前实现、提交、dirty 状态和实测证据的详细真源见：
[Linux 工作区真实实现进度审计（2026-08-05）](../verification/linux-workspace-implementation-audit-20260805.md)。
旧的 [2026-08-03 审计](../verification/linux-workspace-implementation-audit-20260803.md) 只保留作历史点时证据。

## 0. 文档定位

本文把第 26～37 号合同转换为可实施、可并行、可验收的工程工作包，供后续在 Linux 工作区派发多个
Agent 使用。第 26～37 号的产品、权威、失败与主要物理语义已经闭合；本文**不修改也不覆盖这些
合同**，只负责施工依赖、写集、合并列车和验收等级。G1（W5B）、Round 0、Round 1 与 MA0 均已
完成；G0 也已将 Windows 权威目录精确同步到 Linux，并以 `a78196a` 登记合同内容基线。MA0 已完成
两轮实现、独立只读审查和组合验证并进入 CP2。Round 2 的 AR1、WF1、H1 已完成、独立审查并分别
合入后端 `0564a51` 与 H5 `6e582d4`。P1B-CONSUMERS-2 也已完成 FirstHop/Ingress 最终消费者迁移；
主审发现的 TargetFarmPolicy P1 由原 writer session 修复，第二轮独立只读复审给出 PASS、无 P0/P1，
并以 `74de919` 合入后端。CP3 已发布，当前没有编码 Agent 占用写锁；下一步先从 CP3 做真实代码缺口
审计，再把 PI1/FI1 拆成互不冲突的短工作包，禁止按旧任务卡重复派发。
每个 Agent 开工前仍必须由 I0 分配独立 worktree、写集和 handoff，且不得自行补齐合同缺口。

权威顺序固定为：

```text
26～37 现行合同
→ 已登记 Baseline Commit 的 CONTRACT-MANIFEST
→ 本实施编排
→ Agent 工作包说明
→ 代码与测试证据
```

冲突时，上游合同优先。Agent 不得通过修改本文来改变业务语义，也不得根据旧版总体架构、旧 40/70
号计划或历史代码反向改写合同。`30-redis.md` 仅是迁移参考；当前 P0 在线权威服从第 30/31 号 Redis
Authority Store 合同。

---

## 1. 当前代码基线与真实完成度

### 1.1 Linux 集成基线

```text
backend:
  path   = /data/home/developer_026/CodeBuddy/20260519141303/farm-game
  branch = codex/integration-20260803
  code_merge_commit = 74de91925409efdfa9a5e53e6d551b394d4942ab
  checkpoint_tag    = cp3-20260804
  round2_dispatch_tag = cp2-round2-20260804
  p1b_dispatch_tag  = round2-p1b-consumers2-20260804
  next_dispatch_tag = cp3-20260804
  note   = P1B-CONSUMERS-2 已合入并完成二次独立复审与整仓验证；当前无活动 writer

H5:
  path   = /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
  branch = codex/integration-20260803
  commit = 6e582d4dea3674b0f58b6f1d65d91cf07c09f308
```

当前后端与 H5 集成分支均已推送且干净。Round 1、MA0、AR1、WF1、H1 的候选及最终 integration
合并点均已经通过与风险相称的独立审查和组合验证：

- `bash scripts/gen-proto.sh` 重生成后零差异；
- 后端整仓 `go test -race -count=1 -timeout=600s ./...`；
- 后端 `go vet ./...` 与 `go build -buildvcs=false ./...`；
- `git diff --check` 与工作树 clean 检查；
- H5 74/74 tests、`build:web`、`build:staging` 与三环境 `audit:build`。

后端 Round 2 合并候选额外执行了整仓 `go test -race -count=1 -timeout=600s ./...`、vet、build、diff
和 clean 检查；H1 在独立审查 worktree 复核 wire/journal/reducer 后给出 `PASS / no P0-P1`。这些仍不
等于真实公司 Redis Cluster、Failover、ACL 或生产参数已经验证。

H5 在当前 Linux Node `v22.23.1` 下已复核：74/74 单测、Web/Staging 构建和 Web/Staging/Wechat
产物审计全部通过。构建前发现的 root 归属属于环境残留，修正为 `developer_026:users` 后构建成功，未修改源码。

这些结果证明四条基础轨能够共同构建和通过竞态测试，不等于第 26～37 号全部合同已实现，也不等于
R1 已获得公司 Redis 持久化/Failover 证据。

### 1.2 W5B 已完成并清理临时工作区

```text
feature_commit          = c86ce39
merge_commit            = 4c97191
runtime_evidence_chain  = f90b11b -> b2f65e2 -> 22b84e9 -> dc235ef
w5b_completion_commit   = a949761d360bcbda9bbf0190c9d3c3f99bc1b60d
status                  = COMPLETED
```

W5B 已完成实现、真实多进程证据和退役开关：

- 双 Gate/Fresh Join：A 激活、B 顶号、旧连接拒绝、Holder 重启触发
  `REJOIN_REQUIRED(LEASE_LOST)`、同连接 Fresh Join 后恢复经营；
- Drain：真实 H5 浏览器在 Gate A `SIGTERM` 后收到 Drain/1012，经代理连接 Gate B 并回到 `InFarm`；
- 200 人 Subscription Lease：200/200 Join，稳态 20 批次共 4000 次续租，P99 ≤ 5ms，零过期、
  零 fail-closed、零 REJOIN，并覆盖 1600 个 `FARM_CHANGE`；
- GateBoot Reaper 已提供 `enabled|shadow|disabled` 三态，`disabled` 模式完整 E2E 通过。

历史 W5B worktree 与本地/远端功能分支已经删除，内容全部进入 integration。后续 Agent 不得再次执行
W5B 或从历史 `9756bf1/c86ce39` 分叉。证据真源是 Linux 的
`docs/verification/w5b-subscription-lease-handoff.md` 与 `docs/verification/s7-handoff.md`。S7 中复制的旧合同
版本和旧 70 号施工顺序不具备语义权威；设计与施工以本地同步后的 26～37、Manifest、71～73 为准。

### 1.3 Round 0 已完成并发布 CP1

Round 0 的五条互不争用写集已经完成、组合验证并合入同一集成分支：

| 工作包 | 集成提交 | 已交付边界 |
|---|---|---|
| P1A / SCB-1 | `77aa0a5` + `4456762` | 共享身份、Participant IDL、稳定 Hash/ID、Fake 与并发/校验加固 |
| DF0 | `208263e` | `internal/farm/domain` 纯领域规则，不接 Actor/Redis/RPC |
| DP0 | `439f034` | `internal/player/pet` 简化宠物与 Guard 输入纯领域规则 |
| Q0 | `bb3ed36` | 可控测试工具、故障注入接缝、Load Harness 与合同测试矩阵 |
| DT0 | `f075f5b` | `internal/player/task` 每日/成长任务纯领域规则；幂等 Receipt 留在 PlayerAuthority 外层 |

`f075f5b99af416bab5253e142cbcc884cd16f088` 是历史 CP1 代码检查点，不再作为新任务分叉点。
本轮 71～73 与锁表同步并发布后，后续 Agent 必须从稳定 Tag `cp2a-20260804` 分叉；该 Tag 指向
`origin/codex/integration-20260803` 的最终干净 HEAD。SCB-1 的最终共享合同检查点为
`44567622d18f65e41f63fe46d164363e4d1aa8e1`。
上述纯领域包只证明规则计算和边界测试完成，
不代表已经接入 PlayerActor/FarmActor、权威 Redis、公开 33 命令或产品 E2E。

该轮次已经结束，不能再按上述四张旧任务卡重复派发。它们的完成事实见下一节。

### 1.4 Round 1 已完成并形成 CP2A

Round 1 四个工作包都从 `4e587936df183dd7e6e7725200f98dec38e69d5d` 分叉，在独立 worktree 中完成，
由 I0 逐轨复验后合入候选 `e1bf29ae3db472e52c7014f16bb4bd2f0571f0f3`，最后通过显式 merge commit
`94155566b08bccfb453b344fdee046918f199953` 进入 integration：

| 工作包 | 功能分支交付 | integration 中的主要提交 | 证据与真实剩余项 |
|---|---|---|---|
| R1 | `faa5dcf`、`d8ef450`、`a0c0992`、`b2e7ffe` | `d0289a4`～`930a072` | Adapter/Lua、Writer Fence、Unknown、WorkLease、DueIndex、ResultDirectory v2 的 unit/race/full/vet/build 通过；公司 Redis、CompactReceipt/GC 和 AR1 主链接线仍未完成 |
| P1B | `2829283` + `561d662` | `c3adf06` + `e1bf29a` | 33/33 唯一 Catalog、FarmFrame v4 oneof、uint64 sequence、Typed Payload/Result 与 Golden 完成；7 条命令已接线，26 条明确 feature-disabled；旧 FirstHop Envelope Hash/兼容字段由 `P1B-CONSUMERS-2` 收口 |
| SO0 | `9501ad8` + `09b5852` | `9ecea40` + `e826786` | Friend Pair/Quota/Share/Permit/Opportunity 纯逻辑与 Repository Port 完成；真实 Redis Adapter、Join/Admission 接线留给后续 |
| DC0 | `95e8e61` | `abcb4b6` | 活动档案、灰图、Seen/Revision 纯领域完成；PlayerActor/Proto/H5 接线留给后续 |

组合候选和 integration merge 都独立执行了 Proto 重生成零漂移、整仓 `go test -race ./...`、
`go vet ./...`、`go build -buildvcs=false ./...` 和 `git diff --check`。因此 CP2A 的证据等级是
`COMBINED_RACE_PASS`，不是 `REAL_REDIS_PASS` 或产品 E2E。

CP2A 后解锁的 **MA0 PlayerMail EntryStore** 已完成并进入 CP2。本段保留为 Round 1 历史证据；
`P1B-CONSUMERS-2` 已在 CP3 前关闭兼容接缝，不得再次派发；PI1/FI1 必须基于 CP3 的真实缺口重新拆包。

### 1.5 MA0 已完成并发布 CP2

MA0 从 `cp2a-20260804` 分叉，最终候选为 `f4e543eb3f7d2ac0b5fd1576894e6f972a618921`，由
`5e31c2dea65136fa3c0486c6d77765dd5815588e` 显式合入 integration，并发布 `cp2-20260804`：

- 交付 `internal/player/mail` 领域/Repository 与 `internal/storage/redis/playermail` Redis Adapter；
- 完成每页 6 封、投递防重、已读/删除、过期、GC、Unknown、稳定分页与容量边界；
- 第一轮审查发现的计数漂移、ClaimProof 绑定、Dedup/GC 生命周期和 HMGET 预算问题已在 V2 Lua 中修复；
- 第二轮独立只读审查给出 `PASS / no P0-P1`；I0 又在 integration 上执行整仓 race、vet、build 与 diff 检查；
- 邮件附件资产仍由后续 PlayerActor Participant 领取，MA0 没有新增 mail-svr、MailActor 或 MQ。

尚未关闭但已显式登记的风险：Receipt 尚无有界清理、DueRef 重登记存在崩溃窗口、真实公司 Redis
Cluster/ACL/Failover 与 96/768KiB/256 等参数仍待 Probe；分页尾随空页、safe-evict 迟到成员和内部字段长度
校验属于 P2，不阻塞 CP2，但必须在生产接线前关闭或形成平台证据。

### 1.6 按合同划分的当前完成度

| 合同 | 当前已有基础 | 仍缺的主要实现 |
|---|---|---|
| 26 接入与会话 | Join/Ready 屏障、切农场、H5 JoinPhase、SessionRoute Lease；W5B 的双 Gate/Drain/Fresh Join、200 人续租与 GateBoot 证据已完成 | 完整产品主链下的 200 人广播/慢连接、TCONND、Soak 与最终验收 |
| 27 路由/Actor/Workflow | Route Hash v2、Redirect、Actor Runtime、四类 Redis Workflow；R1 已交付新 AuthorityStore Redis Adapter 组件 | AR1 把新 Store/WriterClaim 接入 Runtime；动态 Assignment 控制面仍后置 |
| 28 公网协议 | P1B 已完成 FarmFrame v4 顶层 oneof、uint64 sequence、33/33 唯一 CommandCatalog、Typed Payload/Result 和黄金向量 | `P1B-CONSUMERS-2` 重接 FirstHop/Ingress/旧 Hash；其余 26 条命令按业务里程碑逐条启用，禁止伪造成功 |
| 29 内部 RPC | Trusted Context、Route Header、Join RPC；SCB-1 已补 Player/Farm/Friendship Participant IDL、Fake 与调用描述符校验 | 生产 Participant/Worker/ActionAdmission Adapter 与真实调用链 |
| 30 权威 Store | 接口、Fake、Fence/CAS/Unknown 逻辑证明；R1 Redis Adapter 与故障/竞态测试已完成 | AR1 Runtime 接线、公司 Redis Probe、CompactReceipt/GC 与生产耐久性证据 |
| 31 Redis 物理合同 | R1 已实现 Key/Slot Golden、ScriptRegistry、Repository/Lua、WorkLease、DueIndex、ResultDirectory v2 | 真实公司 Redis 的 ACL/持久化/Failover/延迟 Probe，并替换旧主链 |
| 32 核心经营 | 旧 Buy/Sell/Plant/Harvest/Water/Theft 主链可运行；DF0 已实现 5→24、Care/Pest、20%/2% Theft 等纯领域规则 | 将 DF0 接入 FarmActor/Workflow，补批量命令、可靠提交与纵向 E2E |
| 33/33A 好友 | SO0 已补 canonical Pair、Quota、ShareIssuer、Permit、撤权与 Opportunity 查询核心 | Redis Adapter、Join/ActionAdmission/连续导航生产接线与场景 E2E |
| 34 任务 | DT0 已实现三条每日任务、惰性 Period 换代、单成长链、领奖状态机与红点权威 Summary 的纯领域层 | PlayerActor Receipt/奖励同提交、查询投影、公开命令和 H5 闭环 |
| 35/35A 邮件 | MA0 已交付 PlayerMail EntryStore、Redis V1/V2 Lua、六封分页、已读/删除、投递防重、过期/GC/Unknown 与可信领取材料 | PlayerActor 附件领取、Receipt 有界清理、DueRef 崩溃窗修复及真实公司 Redis/容量 Probe |
| 36 宠物 | DP0 已实现解锁/购买、喂食、切换、版本化 Guard 输入纯领域层 | PlayerActor 接线、GuardSyncOrder、Farm GuardSnapshot 与 Theft E2E |
| 37 图鉴 | 旧主链已有首次自收获解锁；DC0 已补活动档案、完整灰图、Seen/Revision 纯领域 | PlayerActor/Proto/H5/红点接线与持久恢复 E2E |

任何“存在旧代码骨架”的项目都不能直接标记完成。必须按现行合同补齐幂等、失败语义、权威提交和验收证据。

### 1.6 已有代码骨架的收敛策略

Linux 基线已经存在下列可复用骨架：

```text
internal/logic/actor/{activation,persistence,runtime}
internal/storage/{authoritystore,blobstore,redis/rediskey}
internal/workflow
internal/player/{actor,ingress}
internal/farm/{actor,ingress,subscription}
internal/gateway/commandcatalog
cmd/{farm-gate,player-svr,farm-svr,farm-worker}
```

后续工作包必须先做“现有能力→合同条款”的差异矩阵，然后在这些包上收敛；禁止为了
规避改造而新建 `runtime2`、`workflow2`、第二套 Actor Registry 或第二套 Redis Keyspace。

CommandCatalog 已完成 **12 → 33** 的唯一协议迁移。`BUY_OFFER` 等旧符号只作为同编号源码编译窗口，
运行时没有第二套 Catalog；`LEAVE_FARM/GET_FARM_SNAPSHOT` 未注册。当前仍保留的 deprecated Envelope
字段只是尚未重接线 FirstHop 的服务端兼容接缝，客户端值不受信，必须由 `P1B-CONSUMERS-2` 删除，
不能把兼容窗口误写成长期双主链。

---

## 2. 实施总原则

### 2.1 先纵向闭环，再横向铺满

优先顺序不是按文档编号机械施工，而是按风险逐层证明：

```text
接入/订阅稳定
→ 权威存储可证明
→ 公网/内部协议唯一
→ 通用 Workflow 可恢复
→ 单 Player 经营闭环
→ 双 Actor 经营闭环
→ 好友同场与跨玩家结算
→ 任务/邮件/宠物/图鉴
→ 200 人热点与 3000 万 DAU 容量证据
```

### 2.2 不把领域模块直接等同于独立服务

P0 仍维持当前部署边界：

```text
farm-gate
player-svr
farm-svr
farm-worker
```

任务、邮件、宠物、图鉴是清晰领域模块，但首版不拆 TaskActor、MailActor、PetActor、CompendiumActor，
也不增加对应独立服务。好友关系是非 Actor Authority，可先与 player-svr 共部署。以后是否拆 Deployment，
以独立扩缩容、故障隔离、团队所有权或负载证据为准。

### 2.3 Actor 外协调，Actor 内裁决

- PlayerActor 只串行裁决玩家资产、任务、宠物、图鉴和领取 Receipt；
- FarmActor 只串行裁决地块、作物、护理、偷菜、GuardSnapshot 和 FarmVersion；
- 跨 Actor 的 Plant/Unlock/Harvest/Theft 使用 Actor 外 Coordinator + 持久 Workflow；
- 单向可靠副作用使用来源同槽 Order + DirectDispatcher + farm-worker；
- Actor Turn 不等待远程 RPC，不在 Mailbox 内跑网络重试。

### 2.4 Wire、领域模型与存储模型分离

```text
公网 Protobuf DTO
≠ Actor 内领域状态
≠ Redis Record/Lua 输入
```

协议 Agent、领域 Agent、存储 Agent 各自只能修改自己的表达层。转换函数需要显式测试，不能让生成的 Proto
对象直接成为 Actor 长期状态，也不能让 Redis Key/Hash Tag 泄漏到业务命令。

H5 只需要消费公网 Proto、字符串 ID、公开版本和必要的 Canonical Payload Hash 测试向量；
`VirtualShard`、`ActorAddress`、`WorkRef`、Redis Hash Tag 与 Redis Key 都是后端内部合同，不得要求
TypeScript 生成或理解这些值。后端 Golden Test 可以由 Go、Lua 合同测试和独立测试工具共同消费。

### 2.5 P0 默认可靠写，性能优化必须后置

第一轮所有资产、地块、Receipt 和可靠 Work 均按 `CRASH_DURABLE` 或 `WORKFLOW_DURABLE` 实现。
`ACTOR_SOFT`、批量 SoftSave、MQ 正确性依赖、完整 UC Proxyless、透明 Socket 迁移和 Actor 内存热迁移
都不进入首轮主链。Persistence Lane 与 Barrier 接口需要先正确存在，但 SoftSave 功能开关保持关闭，
只有压测证明同步可靠提交是主要瓶颈后才进入独立优化工作包。

### 2.6 新工程无存量客户端负担，协议迁移不做双主链

当前 H5 和后端都由本项目同步开发，没有已公开发布的旧客户端存量。因此 P1 应采用一次
受控升版：保留被废弃的 field number/enum number 为 `reserved`，保留离线迁移 Golden，但不在
生产命令路径同时维护 12 条旧命令和 33 条新命令。若之后真正存在灰度客户端，再根据
第 28 号物理参数门槛增加有时限的兼容窗口。

### 2.7 配置是版本化输入，不是第五个业务服务

P0 不新增 `config-svr`。Shop/Crop/Care/Theft/Task/Pet/Compendium 各自拥有强类型配置模型和
校验器，通过进程内 `ConfigRegistry` 原子替换不可变快照。配置可以先来自版本化文件，
之后换成公司配置中心 Watch；Actor Turn 不得等待远程配置 RPC。各纯领域 Agent 同时交付它
所属配置的解析、版本/Hash、容量上限、无效快照 Fail Closed 和旧版保留测试，不各自新建
通用配置框架。

---

## 3. 并行施工前的两个 Gate

### G0：最终合同基线 Gate（已关闭）

第 26～37 号主体语义、版本、命令清单和下游导航已经完成内容对齐。Windows 权威 `docs/design` 已
逐文件 SHA-256 校验后同步到 Linux，旧 70 号计划随镜像删除，合同内容基线为：

```text
contract_baseline_commit = a78196a48d044a350a3b761c1d73b404f44a18c1
26～37版本
CONTRACT-MANIFEST状态
仍为平台参数候选的内容
禁止实现Agent自行决定的内容
```

G0 已再次验证两个贯通第 26～33 号的公开协议接缝：

```text
GET_PLAYER_BOOTSTRAP
→ 26要求的独立玩家私有首屏读取；不能塞进FarmSnapshot

GET_NEXT_FRIEND_OPPORTUNITY
→ 26/31/33要求的连续机会导航读取；不能用GET_FRIEND_LIST含糊替代
```

最终方案已固定为两条显式、可安全重查的公开命令，P0 公开 Command Inventory 因此是 **33 类**；
第 28、40 号和 Manifest 已统一该数量，不再允许实现 Agent 选择 Push 或合并查询作为第三种方案。

本轮已经完成版本总账、35A/60/Q37 导航、任务接缝、第 33 号引用以及 W5B/H5 实际状态的机械对齐。
G0 已关闭，W5B/E0/H0 均不再占用前置槽位；P1A、DF0、DT0、DP0、Q0 也已经在独立 worktree
完成并合入 CP1。后续不得重复派发这些卡，除非其上游合同或写集发生明确变更。

### G1：W5B 已关闭

W5B 已通过 MR !6 和后续四个证据提交闭合，完成记录见 `a949761`。双 Gate、Drain、Fresh Join、
200 人批续租和 GateBoot 退役均已有公司 Redis/真实进程证据，因此 E0 不再派发，也不存在需要保护的
W5B 脏工作树。这里的 `COMPLETED` 只代表 W5B Subscription Lease 工作包，不等于整个项目已通过
TCONND、全玩法、Soak 或 3000 万 DAU 容量验收；这些仍由后续 Q0/Q2 单独证明。

所有新 Agent 都必须从登记后的干净 Integration/Contract Baseline 建独立 worktree，不得从
`9756bf1`、`c86ce39` 或已删除的 W5B 分支分叉。

---

## 4. 单写者区域

以下区域同时只允许一个 Agent 拥有写权限：

| 单写者区域 | 唯一 Owner | 其他 Agent 如何协作 |
|---|---|---|
| `docs/design/26～37`、Manifest | 合同 Owner（非实现 Agent） | 实现 Agent 只读引用；歧义写入独立 review/handoff 并停止扩张 |
| `api/proto/**`、`api/gen/**`、`internal/contracts/**`、`internal/identity/**` | Protocol/Shared Contract Owner | 领域 Agent 提供接缝需求，不自行改字段号、稳定身份算法或生成代码 |
| `internal/gateway/commandcatalog/**` | Protocol/Catalog Owner | 场景 Agent只提交测试向量或需求清单 |
| `internal/storage/redis/**`、公共 ScriptRegistry/KeyBuilder | Redis Owner | 好友/邮件 Agent 调用 Repository 接口，不手拼 Key |
| `internal/player/actor/**`、`internal/player/ingress/**`、`cmd/player-svr/**` | Player Integrator | 子领域 Agent只写独立纯领域包 |
| `internal/farm/actor/**`、`internal/farm/ingress/**`、`cmd/farm-svr/**` | Farm Integrator | 子领域 Agent只写独立规则/状态转换包 |
| `internal/workflow/**`、`internal/worker/**` | Workflow Owner | 场景 Agent提供具体 Workflow 定义，不改通用调度器 |
| H5 协议解码、全局 Store、连接状态机 | H5 Integration Owner | 页面 Agent只写独立组件和领域 reducer 测试 |
| `go.mod/go.sum`、`cmd/farm-gate`、`cmd/player-svr`、`cmd/farm-svr`、`cmd/farm-worker` 的 composition root | Integration Owner | 各模块导出构造器/注册项，依赖与最终装配由 Owner 统一修改 |

这张表比“每个 Agent 一个服务”更重要。真正造成并发冲突的通常不是业务概念，而是共享 Proto、Actor 包、
Redis KeyBuilder 和启动接线。

---

## 5. 推荐施工波次

### Wave 0：基线收口

| 包 | 内容 | 并行性 |
|---|---|---|
| B0（已完成） | Windows 权威 `docs/design` 已精确同步并登记为 `a78196a` | 后续只由合同 Owner 更新正文 |
| E0（历史） | W5B 双 Gate/Fresh Join/Drain/200 人续租/GateBoot 证据 | `a949761` 已完成，不再派发 |
| V0 | 只读复核当前集成基线与 W5B Evidence Bundle | 可并行，只写独立 verification 文件 |

Wave 0 的合同与代码基线已经登记。当前轮次的精确 Agent 数量、任务切片和 Prompt 以第 73 号为准；
每个 Agent 的实际分支起点仍由 I0 在派发当下复核并记录。

### Wave 1：基础轨并行（CP2A 已完成）

SCB-1、Farm/Task/Pet 纯领域包和 Q0 Harness 已在 CP1 完成；A1/R1、A2/P1B、好友 SO0 与图鉴 DC0
已在 CP2A 完成并通过组合竞态验证。以下小节保留为已完成工作包的验收规格，不得按旧 Prompt 重复派发。

#### A1 · Redis Authority 基础

范围：第 30/31 号。

```text
internal/storage/redis/**
internal/storage/authoritystore/redis*（新文件或明确目录）
cmd/redis-*-probe/**
对应测试与 verification/A1-*.md
```

已交付：小 Lua 脚本族、Repository、EVALSHA/NOSCRIPT、Fence/CAS/Unknown Resolver、DueIndex/Receipt、
WorkLease 与 ResultDirectory v2 基础原语。不得修改 Actor、业务命令和 Proto；真实公司 Redis 与 AR1
接线仍属于后续证据/集成工作。

#### A2 · 共享实现合同、公网协议与 CommandCatalog

范围：第 28/29/31 号共享 Wire/身份合同，以及第 32～37 号已登记的完整 33 类公开 Command Inventory。
实现不得删减、合并或用 Push 替代两条已冻结的首屏查询。

```text
api/proto/**
api/gen/**
internal/gateway/commandcatalog/**
internal/protocoltest/**
协议黄金向量
```

本包已经按两个阶段交付并合入：

1. `SCB-1`（Shared Contract Baseline）：先提交公共 Envelope/错误语义、可信上下文、
   `EffectiveIdempotencyKey`、`WorkAddress/SourceAuthorityRef`、`WorkType/ParticipantStep`、
   `StableOpaqueID/CanonicalPayloadHash`、Player/Farm/Friendship Participant IDL、跨领域不可变 Snapshot、
   Fake Client/Server 和注册完整性/黄金向量测试；不包含真正业务裁决；
2. `P1-FULL`：再完成唯一 command_type、33 类 Typed Payload/Result、RetryDirective、Root/RouteKey、
   最大尺寸和兼容测试。当前旧命令名称必须显式迁移或 reserved，不能留下两条并行语义主链。

`SCB-1` 中的 Protobuf 只是跨进程 Wire Contract。Player/Farm Actor 的内存状态仍使用领域类型，通过显式
Mapper 与合同测试转换，不能把生成的 Proto 直接当 Authority State。

#### A3 · 纯领域模型与状态转换

只创建不依赖网络、Redis、Actor Mailbox 的独立包；建议按最终仓库风格选择路径，例如：

```text
internal/player/economy/**
internal/player/task/**
internal/player/pet/**
internal/player/compendium/**
internal/player/mail/**
internal/farm/domain/**
```

交付：不可变输入、CandidateState、规则版本检查、checked arithmetic、状态转换和表驱动测试。
不得修改 `internal/player/actor`、`internal/farm/actor`、Proto 或 Redis。

如果 A3 过大，可拆成两个不交叉 Agent：

- A3P：Player 侧 economy/task/pet/compendium/mail；
- A3F：Farm 侧 plot/crop/care/theft/guard/opportunity。

若需要更多并行度，只允许继续拆“纯领域包贡献者”，例如 Economy/Reservation、Task/Pet/Compendium、
Plot/Crop/Harvest、Care/Theft/Guard；这些 Agent 不得修改 PlayerActor/FarmActor。Player Integrator 与
Farm Integrator 始终各自只有一个。

#### A4 · 验收工具与故障注入骨架

范围：`internal/testkit/**`、`loadtest/**`、独立 `cmd/*-probe` 和 verification 模板。

交付：可控时钟、RPC 闸门、Redis 超时注入、Actor 崩溃点、慢连接、批量 200 订阅测试骨架。只搭工具，
不复制业务规则，不用 Mock 结果宣称场景通过。

#### Wave 1 合并检查点 · SCB-1

纯领域模型可以在 `SCB-1` 合入前并行编写，但以下工作必须等待 `SCB-1` 合入并从新 checkpoint rebase：

- Player/Farm Participant Server 与生产 gRPC Adapter；
- Friendship ActionAdmission 接到 Farm Participant；
- Plant/Harvest/Theft Coordinator；
- CareRewardOrder 接到 `ApplyCareReward`；
- farm-worker Work Registry 和四个 Deployment 的 composition root。

领域包可以在自己的消费侧定义小 Port Interface 和 Fake；不得定义一个跨 Player/Farm/Friendship 的
`EverythingService`，也不得在业务分支中偷偷扩展公共 Proto。接缝变化统一走 ICR（Interface Change
Request），由 Shared Contract Owner 合入增量合同后，消费者再 rebase。

### Wave 2：共享基础接线与尾部消费者收口已完成

MA0、AR1、WF1 与 H1 已完成并进入 integration。P1B-CONSUMERS-2 随后以单一 FirstHop/Ingress writer
完成可信校验、完整 TargetFarmPolicy、权威 Canonical Payload Hash 与 deprecated 兼容窗口收口，最终
候选 `a22dca9` 经第二轮独立复审无 P0/P1，并以 `74de919` 合入。CP3 已发布；动态 Assignment 仍不阻塞
玩法主链，下一轮先核验 PI1/FI1 的真实代码差距，再登记新写锁。

#### B1 · Actor Runtime + Persistence Lane

独占 Player/Farm Actor Runtime 接线区，实现 LoadAndClaim、Singleflight 激活、Mailbox、Authority Commit、
Drain/Passivation、ForceFlush 和失权自 Fence。先接 Fake/Repository 接口，再接真实 Redis Adapter。

#### B2 · Workflow/Order/Dispatcher/Worker

独占通用 Workflow 区，实现：

```text
CreateIfAbsent
Work Lease
Participant Receipt
来源同槽 Order
DirectDispatcher 快路径
farm-worker DueIndex 恢复
```

本阶段只用 `TestWorkflow` 证明框架，不宣称 Plant/Theft 已完成。

#### B3 · 好友非 Actor Authority

SO0 纯逻辑与 Repository Port 已在 CP2A 完成。真实 Redis Adapter、Join Hub、ActionAdmission 和
FarmActor 接线仍由后续 SO1/场景里程碑完成，不得重建第二套好友核心。

#### B4 · PlayerMail EntryStore（MA0 已完成）

范围：第 35/35A 号。MA0 已实现 MailEntry、分页、已读、删除、投递防重、过期、GC、Unknown 与
可信领取材料，候选 `f4e543e` 已由 merge `5e31c2d` 进入 CP2。附件领取资产仍未接 PlayerActor；
Receipt 清理、DueRef 崩溃窗口和真实 Redis 容量/Failover 证据作为明确后续项，不得伪报生产完成。

#### B5 · H5 Wire/Reducer 基础

等待 A2 的 Proto/黄金向量合入后开始。实现最终 FarmFrame、CommandResult/ServerPush 分离、Operation Journal、
RetryDirective、Farm/Player 私有状态分层和版本 Gap 处理。本包不同时大改所有页面。

### Wave 3：按纵向闭环集成

本阶段不建议把每个玩法直接交给不同 Agent 同时修改 Actor。正确方式是“领域包并行，Actor 接线单写者”。

跨领域场景按“来源负责冻结事实，目标权威负责应用事实”分工：

| 场景接缝 | 来源/协调 Owner | Player 权威 Owner | Farm 权威 Owner | 恢复/装配 Owner |
|---|---|---|---|---|
| Plant | SC1 创建并推进 `PlantWorkflow` | DP1 实现 Reserve/Finalize/Release Seed | DF1 实现 `CommitPlant` | WF1 恢复，I0 装配 |
| Friend Care | SO1 提交 ActionAdmission 与不可变 Permit | DP1 实现 `ApplyCareReward` | DF1 实现 `ResolveCareIncident` 并冻结 `CareResolutionSnapshot` | WF1 投递 Order，I0 装配 |
| Harvest | SC1 推进 Workflow | DP1 原子应用资产/经验/任务/图鉴 | DF1 冻结并提交 Harvest 决策 | WF1 恢复，I0 装配 |
| Theft | SO1 提交 ActionAdmission；SC1 推进 Workflow | DP1 分别实现访客与主人结算 Participant | DF1 冻结目标扣减/宠物截留决策 | WF1 恢复，I0 装配 |

这里的 Owner 是代码写权，不是说 Coordinator 可以修改 Participant 内部状态。所有跨边界调用只通过
`SCB-1` 合同；DP1 不编辑 FarmActor，DF1/SO1/SC1 也不编辑 PlayerActor。

所有会改变公开农场状态的纵向链，在开始 Plant/Harvest 前必须先具备最小实时交接：

```text
Farm Authority Commit
→ PostCommitDeliveryGuard
→ FarmEventLane/FarmSubscriptionHub
→ FARM_CHANGE
→ H5按farm_version应用
```

CommandResult 只表达操作结果，不能代替 FarmChange 更新页面。共享 VersionRing、Opportunity Projection
和 200 人热点优化可以后置，但这条最小正确性链不能放到 Plant/Harvest 之后补。

#### C1 · Buy/Sell：第一条 Player-only 写链

证明：

```text
H5 → Gate → PLAYER Route → PlayerActor
→ wallet/inventory/receipt/outcome 一次 Authority Commit
→ CommandResult / OperationResult
```

它用于先验证 Actor、Redis 和幂等，不引入 Workflow。

#### C2 · UnlockPlot/Plant：第一条双 Actor Workflow

启动条件：最小 FarmChange 交付链已在一个测试 Farm 上证明 `Commit V → FARM_CHANGE(V) → H5 应用 V`。
业务顺序：先 UnlockPlot，再使用初始已解锁地块验证 Plant。

```text
farm-svr Coordinator
→ CreateIfAbsent Workflow
→ PlayerActor ReserveCoin/Seed
→ FarmActor CommitUnlock/CommitPlant
→ PlayerActor Finalize
→ Complete
```

完成后才能宣称通用 Workflow 已经被真实业务验证。

#### C3 · Harvest + Task + Compendium

Harvest 的 Player Participant 一次提交：

```text
Inventory
Exp/Level
Task progress
Compendium first-own-harvest unlock
Receipts/Outcome
```

这三个功能必须由同一个 Player Integrator 接线，不能让 Task Agent、Compendium Agent 和 Harvest Agent
并发修改 PlayerActor。

#### C4 · 好友建交、Join 和 ActionAdmission

先完成 Share/Relation/Delete/Block，再接 AUTHORIZING Join、VisitPermit 和 ActionAdmission。好友索引和
OpportunitySummary 只是候选，不能替代 Pair Authority 或 FarmActor 最终裁决。

#### C5 · Care + 实时广播 + 任务互动

先 Owner Care，再 Friend Care。验证唯一 Incident、`NO_CHANGE`、CareRewardOrder、FarmChange、200 人广播
和“好友互动一次”任务事实。

#### C6 · Pet + GuardSync

先完成购买/任务解锁、喂食、切换，再把版本化 GuardSnapshot 可靠同步到 FarmActor。宠物操作成功后
GuardSync 可以 Pending，但不得重复扣道具。

#### C7 · Theft + Pet 裁决 + 双方结算

依赖 C4 和 C6。验证每好友每茬一次、20% 总上限/单次 2%、宠物截留作物而非扣访客钱包、主人金币补偿、
访客库存到账、ResultDirectory 和 Workflow 恢复。

#### C8 · Mail Claim 与任务领奖

MailEntry 查询是非 Actor 路径；附件领取和 TaskClaim 进入 PlayerActor，资产 + Receipt + Outcome 原子提交。

### Wave 4：H5 产品闭环

H5 按后端已经稳定的纵向链逐批接入：

1. Buy/Sell/Unlock/Plant/Harvest；
2. 好友列表、Join、Care/Theft 和连续机会导航；
3. Task/Mail/Pet/Compendium 页面及红点；
4. 弱网、PROCESSING、Fresh Join、Loading/Retry 和局部降级。

同一时刻只允许一个 H5 Integration Owner 修改全局 Store、WebSocket 客户端和 Operation Journal。页面组件
可以并行，但必须通过稳定 selector/action 接口接入。

### Wave 5：容量、稳定性与生产证据

- 200 人同场：一份共享 Snapshot/VersionRing，每订阅只保留 Fence/游标/有界追赶状态；
- 按 Gate 聚合下行，不让 FarmActor 等待 199 个连接写出；
- 热点 Farm Mailbox、Join 风暴、慢客户端、VersionRing 覆盖和 Fresh Join；
- Redis Lua P99、Unknown、NOSCRIPT、主从/代理故障、RPO/RTO；
- Shard Drain、Holder 崩溃、Writer Fence、旧 Holder 迟到写；
- 3000 万 DAU 的 DAU→PCU→命令 QPS→Redis/下行容量推导；
- 全链路 Trace/Metric/Structured Log 和告警；
- 最终 Evidence Bundle，不用设计估算替代实测。

本波次增加一个非业务的 Platform Owner，独占 `Dockerfile*`、`deploy/**` 和生产启动脚本：

```text
四个Deployment的独立镜像/进程入口
本地Docker Compose多进程E2E
公司Redis/配置/密钥外置
readiness/liveness/graceful drain
云上K8s资源、滚动发布与HPA候选
```

Platform Owner 不修改 Actor、Workflow 或业务规则；只用已稳定的 composition root 组装环境。

---

## 6. 推荐并发规模与工作包拓扑

不建议一次启动十几个编码 Agent。当前仓库共享接线较多，**4～5 个并发 Agent 是更稳妥的上限**。

推荐拓扑：

```text
当前 CP3 稳定点：
  P1B-CONSUMERS-2 与两轮独立审查均已关闭
  当前无活动编码 Agent、无共享写锁
  I0 先做 post-CP3 真实缺口审计，再登记 PI1/FI1 短包
  未登记 owned_write_set 前禁止直接启动长期 Player/Farm 大分支

历史 CP1 Checkpoint（已完成）：
  Integration Owner合入SCB-1
  运行合同/黄金向量/全仓测试
  发布shared_contract_baseline_commit
  所有跨领域消费者从该Commit rebase

Round 3（从 CP3 分叉）：
  Agent-A  Player Integrator（单写者）
  Agent-B  Farm Integrator（单写者）
  Agent-C  Friendship Adapter/Admission
  Agent-D  Scenario Coordinator
  Agent-E  H5 Feature + Q2 纵向验收

CP3 后真实代码审计补充（2026-08-04）：

```text
AR1 已完成 Authority Registry/Store 组件接缝
≠ cmd/player-svr、cmd/farm-svr 已切换生产 Authority 主链

生产切换前新增 RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL：
RouteManager提交ShardWriteAuthority
→ Assignment Watch
→ HolderLeaseGuard
→ FirstHop完整Route Fence
→ NewAuthorityRegistry composition

PI1-A / FI1-A / SC1-A 可以并行推进组件与业务闭环；
RT1 完成前禁止把测试 StaticFenceSource 接入生产 cmd，也禁止宣称 Authority 生产切换完成。
```

详细决策见 `docs/verification/ar2-production-authority-cutover-decision.md`。

Wave 3：
  Player Integrator（单写者）
  Farm Integrator（单写者）
  Scenario Workflow Agent
  H5 Feature Agent
  Verification Agent

Wave 5：
  Verification/Capacity Agent
  Observability Agent
  Platform/Deployment Agent
  Integration Owner
```

Player Integrator 与 Farm Integrator 可以并行；但任意两个 Player 子功能不能同时直接编辑 PlayerActor，任意
两个 Farm 子功能也不能同时直接编辑 FarmActor。

---

## 7. 分支、Worktree 与合并列车

### 7.1 分支规则

所有 Agent 从同一个已记录集成 Commit 创建分支：

```text
codex/a1-redis-authority-<date>
codex/a2-public-protocol-<date>
codex/a3p-player-domain-<date>
codex/a3f-farm-domain-<date>
...
```

每个 Agent 的首条交接必须记录：

```text
base_commit
owned_write_set
forbidden_write_set
contract_sections
dependencies
definition_of_done
```

### 7.2 合并顺序

```text
合同基线
→ W5B
→ CP1：SCB-1共享实现合同 + Farm/Task/Pet纯领域 + Q0
→ CP2A：R1 Redis Adapter + P1B 33命令 + SO0 + DC0（已完成）
→ CP2：MA0 + Round1/MA0组合验证（已完成，`5e31c2d` / `cp2-20260804`）
→ AR1 Actor Runtime + WF1 Workflow Runtime + H1 H5 Wire（已完成并独立审查）
→ P1B-CONSUMERS-2（已完成，候选 `a22dca9`，合入 `74de919`）
→ CP3（已发布：`cp3-20260804`）
→ Buy/Sell
→ 最小 FarmChange 交付链
→ Unlock/Plant
→ Harvest/Task/Compendium
→ Friendship Join/Admission
→ Care
→ Pet/GuardSync
→ Theft
→ Mail/其余P0d
→ H5全量闭环
→ Capacity/Soak/Production Evidence
```

同一波次的分支可以并行开发，但必须按上述 Merge Train 合入。每次合入后重新跑全仓测试，再让下一批从
新的集成 Commit 分叉；不要让后续 Agent 长期基于过时基线开发。

### 7.3 禁止的协作方式

- 多个 Agent 共用一个脏 worktree；
- 多个 Agent 同时生成 Proto；
- 业务 Agent 手工修改生成文件；
- 为减少冲突而复制第二套 PlayerActor/FarmActor；
- 在场景分支直接手拼 Redis Key 或自行新增 Lua；
- 合并冲突时选择 “ours/theirs” 整文件覆盖；
- 未跑测试就把独立分支状态写成“已集成完成”。

---

## 8. 每个 Agent 的完成定义

每个工作包至少交付：

1. 对应合同条款列表；
2. 改动文件与写集证明；
3. 单元测试、表驱动边界和并发测试；
4. 幂等、Timeout Unknown、旧 Fence、重复/乱序测试；
5. `go test`、`go vet`，共享状态包补 `go test -race`；
6. 若改 H5，补 Vitest、TypeScript 检查和 build；
7. 若改 Redis，补真实公司 Redis Probe 与未执行平台项；
8. 独立 `docs/verification/<work-package>-handoff.md`；
9. Commit、回滚方式、剩余风险；
10. 明确区分 `LOGIC_PASS`、`INTEGRATION_PASS`、`E2E_PASS`、`CAPACITY_PASS`。

Agent 不得只以“代码能编译”作为完成，也不得因单元测试通过宣称 3000 万 DAU 或 200 人同场已经验证。

---

## 9. 可直接复用的 Agent 任务模板

```text
目标：实现 <工作包名称>。

唯一语义输入：
- 合同 <编号与章节>
- contract_baseline_commit = <commit>
- integration_base_commit = <commit>
- shared_contract_baseline_commit = <SCB-1或更新Commit；纯领域包可写N/A>

允许修改：
- <明确目录/文件模式>

禁止修改：
- docs/design/26～37
- <其他单写者区域>

前置依赖：
- <已合入Commit>

必须完成：
1. <功能条目>
2. <故障/幂等条目>
3. <指标/日志条目>

必须执行：
- go test <packages>
- go test -race <shared-state packages>
- go vet <packages or ./...>
- <probe/e2e>

交付：
- 一个可独立审查的Commit
- docs/verification/<package>-handoff.md
- 原始测试结果、未执行项、风险和回滚方式

遇到合同歧义：停止该分支的语义扩张，把问题列入handoff，不自行决定。
```

---

## 10. 正式派发 Gate 与基线登记

业务语义已经结束发散。以下 1～9 已完成；第 10 项仍是 I0 对每个实际工作包的派发门禁：

1. `[工作区已对齐]` `GET_PLAYER_BOOTSTRAP` 与 `GET_NEXT_FRIEND_OPPORTUNITY` 已在第 26/28 号形成唯一公开表达；
2. `[工作区已对齐]` 第 28 号 Command Inventory 数量、名称、RouteKey、结果和错误与第 32～37 号逐项一致；
3. `[工作区已对齐]` Manifest、README 与正文中的 35A、60、Q37-01～Q37-08 状态一致；
4. `[工作区已对齐]` 第 32 号只保留现行 Participant/Order 和第 33～37 号下游合同引用；
5. `[工作区已对齐]` 第 40 号已同步 26 v1.3、27 v4.2、28 v2.8/33类、29 v2.11、31 v1.4、
   32 v0.29、33 v1.2、40 v1.1 与 60 v1.5；
   不存在另一份 70 号实施计划与 71/72/73 并行授权；
6. `[已关闭]` W5B 已在 `a949761` 完成实现、双 Gate/Drain/Fresh Join、200 人续租和 GateBoot 证据；
7. 后端与 H5 集成分支均干净，并登记最终 Commit：

```text
contract_baseline_commit = a78196a48d044a350a3b761c1d73b404f44a18c1
shared_contract_baseline_commit = 44567622d18f65e41f63fe46d164363e4d1aa8e1
round1_dispatch_base = 4e587936df183dd7e6e7725200f98dec38e69d5d
round1_candidate_commit = e1bf29ae3db472e52c7014f16bb4bd2f0571f0f3
cp2a_code_merge_commit = 94155566b08bccfb453b344fdee046918f199953
cp2a_checkpoint = cp2a-20260804
ma0_candidate_commit = f4e543eb3f7d2ac0b5fd1576894e6f972a618921
cp2_code_merge_commit = 5e31c2dea65136fa3c0486c6d77765dd5815588e
cp2_checkpoint = cp2-20260804
round2_dispatch_checkpoint = cp2-round2-20260804
round2_backend_merge_commit = 0564a5194d0c1df88b50d899db6fa8ecf8208bf8
p1b_consumers2_dispatch_checkpoint = round2-p1b-consumers2-20260804
p1b_consumers2_candidate_commit = a22dca9814680bd7d5d874ad7fc42ec6e4eb3015
p1b_consumers2_merge_commit = 74de91925409efdfa9a5e53e6d551b394d4942ab
cp3_checkpoint = cp3-20260804
h5_integration_commit = 6e582d4dea3674b0f58b6f1d65d91cf07c09f308
w5b_completion_commit = a949761d360bcbda9bbf0190c9d3c3f99bc1b60d
```

8. `[Round 1 已完成]` R1/P1B/SO0/DC0 均有独立分支、worktree、handoff、复验与组合检查点；旧锁已关闭；
9. `[Round 2 已验证]` AR1、WF1、H1 均完成独立审查；后端 `0564a51` 通过整仓 race/vet/build/diff，
   H5 `6e582d4` 通过 tests 与多环境 build/audit；旧写锁关闭；
10. `[P1B/CP3 已关闭]` P1B-CONSUMERS-2 已经二次独立复审、整仓验证并合入；`cp3-20260804` 是下一轮唯一后端分叉点；
11. `[MA0 已关闭]` 原 branch/worktree 只供审计，不得继续占用 Mail 写锁或重复派发；
12. 所有实现 Agent 都明确：平台参数 Pending 不等于可以自行选择默认生产值。

本文已升级为正式施工输入。第 71 号只管理波次和依赖，第 72 号只管理具体任务卡；I0 必须在每次派发
前完成第 8～12 项。两份文档都不构成实现 Agent 自行补充业务语义的授权。
