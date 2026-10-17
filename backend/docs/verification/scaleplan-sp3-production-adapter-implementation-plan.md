# ScalePlan SP3 Production Adapter 架构、威胁模型与实施计划（A0）

> Checkpoint：`CP-SCALEPLAN-SP3-ADAPTER-PLAN`
>
> 本文只冻结 Production Adapter 的架构、扩展接缝、物理合同、威胁模型、阶段依赖和验收门槛；不授权或实现 Adapter，不连接真实 Redis/Kubernetes，不部署，不运行真实扩缩容，不采样性能。

---

## 0. 文档身份、基线与授权

```text
schema_version=scaleplan-sp3-production-adapter-plan-v1
worktree=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-sp3-adapter-plan-20260813
branch=codex/scaleplan-sp3-adapter-plan-20260813
base=0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
reviewed_protocol_code_identity=c06ae6a552bfb3c27f8d5283dc3ae7111c1799f4
protocol_status_docs_tip=5082719feed98a6b107da52a65e9b81aec00ceb8
g21_runtime_identity=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361
sp3_0_marker=SP3_0_PASS_READY_TO_PLAN_ADAPTERS
```

当前授权边界：

```text
A0_plan_authorized=true
A0_5_implementation_authorized=false
A1_implementation_authorized=false
A2_implementation_authorized=false
A3_implementation_authorized=false
A4_implementation_authorized=false
A5_real_environment_authorized=false
A6_performance_authorized=false
adapter_implementation_authorized=false
deployment_authorized=false
runtime_activation_authorized=false
ready_for_sp3=false
supports_30m_dau=false
```

A0 唯一允许写集：

```text
docs/verification/scaleplan-sp3-production-adapter-implementation-plan.md
docs/verification/scaleplan-sp3-production-adapter-plan-status.json
```

明确禁止：

- 修改任何 Go/Proto/Lua/部署文件；
- 创建 A0.5/A1/A2/A3/A4 实现分支；
- 连接外部 Redis、Kubernetes 或网络服务；
- 读取公司 secret、KUBECONFIG 或 ServiceAccount token；
- 执行 `redis-cli`、`kubectl`、`helm` 或部署命令；
- 执行真实 RoutePlan、Shard Migration、Pod 扩缩容；
- 运行性能、容量、压力或 Cleanup；
- Push、Merge、Rebase Integration、Tag。

A0 Gate：

```text
round1_independent_review=FAIL  P0/P1/P2=1/8/2
round2_independent_review=FAIL  P0/P1/P2=1/4/1
round2_5_source_audit=SELF_FOUND  X1/X2（见 §11.3、§8.3）
round3_independent_review=FAIL  P0/P1/P2=1/14/5
round4_independent_review=FAIL  P0/P1/P2=1/7/3
round5_independent_review=FAIL  P0/P1/P2=0/8/6   # P0 首次归零
round6_independent_review=FAIL  P0/P1/P2=0/5/5   # P0 连续两轮归零
required_verdict=PASS
required_P0=0                                     # 未放宽
required_P1/P2=见下述 carry-forward 条款           # 已放宽
runtime_touch=false
```

**出口判据在 round-6 之后被显式放宽，理由与代价如下（本条是治理决定，不是技术结论）。**

原判据是 `0/0/0`。六轮下来它没有收敛：finding 计数为 11 → 6 → 20 → 11 → 14 → 10，其中 round-6 的 **5 条 P1 全部由 round-5 的修订自己引入**，没有一条是遗留缺陷。这是 S3 早已记录、但一直只在文字上承认的现象——修订产生 finding 的速率与它关闭 finding 的速率接近 1:1，因为每关一条都要冻结新规格面，而新规格面又是对着未经编译验证的源码写的。

真正促成放宽的是 round-6 五条 P1 的**性质**，而不是它们的数量：

```text
R6-01  枚举漏了三条编译边  -> go build ./... 一条命令给出真集合
R6-02  DEGRADED 无处持久化 -> 打开 model.go 看一眼字段列表即知
R6-03  两张表行数不一致    -> 行键比对，可写成 gate
R6-04  矩阵某一列判定过宽  -> 逐行拆开即可
R6-05  Redis 少一个 key    -> 写 Lua 的第一分钟就会撞上
```

五条都不是"设计不安全"，而是"规格文本与代码事实之间的对齐误差"，且**五条全部能被实现阶段在分钟级证伪**。继续在纯文档层追平它们，是用最慢的手段做最容易的事，并且每追平一次就有很大概率再引入一条同类的。

因此改为：

```text
A0 出口 = P0 = 0
        + 全部未关闭的 P1/P2 逐条进入 §12.9 carry-forward 清单
        + 每条都指定：owner 阶段、判据、以及"由什么机械动作证伪"
        + 清单中【安全类】条目为零

安全类的定义（三者之一即算，必须回到 0）：
  会让某个 Plan 在不安全状态下 terminal
  会让 fence / exclusion / barrier 的安全属性失效
  会让 fail-closed 退化为 fail-open
```

按此判据核对 round-6 的十条：R6-04 属安全类（ownership 列过宽会让 scale operation 未决时释放 ownership），已在本轮**关闭**；R6-05 与 R6-08 触及 corrupt-latch 的 fail-open 面，但通道 A（mutation lease）独立成立且未被削弱，通道 B 缺失只是**少一条检出路径**，不构成既有属性失效，因此归为功能缺口而非安全类。其余七条均为对齐误差。安全类清零。

**代价必须写清楚，不能只写好处。** 这个放宽把三件事从"复审保证"降级为"实施阶段保证"：编译边的完整性、DEGRADED 状态的可持久化、通道 B 的底座。如果 A0.5 的第一个动作没有按 §12.9 执行，这三件事就没有任何人保证——`0/0/0` 至少还有一个外部复审在兜底，carry-forward 清单只有执行纪律在兜底。因此 §12.9 的每一条都绑定了具体的 gate，A0.5 gate 不通过即整体回退，这是唯一的补偿机制。

### 本版本（A0.5 授权申请版）的主线是把剩余问题交给能证伪它们的阶段

round-3 的 20 条 finding 里，约一半是 round-2 修订**自己引入**的（P0-1、P1-6～P1-13 中的多数），而不是原有缺陷。成因很清楚：每一轮修订都在冻结新机制，而新机制是对着没有完全核实的源码写的，于是修订产生 finding 的速度接近它关闭 finding 的速度。最典型的是 round-2.5 的自查 X1——它基于"exclusion 记录随每轮扩缩容无界增长"这个**错误**的源码结论，设计了 `created_at_ms` + bounded ZSET + `PruneExpiredExclusions` + 启用前置计数 gate 一整套保留机制，而这套机制自己又生出了 round-3 的 P1-9、P1-10、P1-11。

因此第一原则是：**能删的不改，能不冻结的不冻结**。round-4 候选最大的一笔修订是删除整套 exclusion 保留机制（§8.3），一次关闭四条 finding，并让 A1R 的 additive-only 主张重新成立。round-5 候选在此基础上删掉了 `RELEASED_BY_HOLDER_ABSENT`（round-4 P0 的来源），round-6 候选继续删掉那条建立在它之上的错误活性论证。

**本版本（round-6 之后）承认这条路线已到收益拐点。** round-6 的 5 条 P1 **全部**由 round-5 的修订引入，且全部可被 `go build` / 打开一个结构体 / 写一行 Lua 在分钟级证伪。第二原则因此加入：**能由实现阶段证伪的，不在文档层追平**。具体兑现是 §0 A0 Gate 的出口判据放宽与 §12.8 的 carry-forward 清单。这不是降低标准——P0 判据一格未松，安全类 finding 必须清零——而是换一种更快也更可靠的手段去关闭那些本来就该由编译器回答的问题。

structural change 累计六项：

```text
S1 写集全覆盖审计：§11.2 的分区表由源码 mutation 入口反向推导，并断言全覆盖与不重叠
S2 Preflight 化：所有依赖平台/库行为的断言降级为 §12.7 具名 preflight gate + fail-closed marker
S3 减法优先：新增机制前必须先证明"不加这个机制会具体坏在哪"；
   自查 finding 与复审 finding 一样需要逐条回源码核对，
   "源码核对确认"这个标签本身不构成证据
S4 范围收窄：反复产生 finding 的机制降级为"只冻结安全属性 + fail-closed 默认"，
   算法细节下放到能被测试证伪的阶段
S5 跨阶段编译依赖显式化：接口变更与其实现者的编译依赖必须【测量】而非枚举
   （round-6 更正：枚举单位是编译单元不是文件，手工枚举连续两轮不完整）；
   A0.5 对被打断的实现文件持有【受限、临时、可机械核验】的 conformance 编辑权，
   而不是用一句"两半同时落地"绕过阶段边界。见 §8.1
S6 判据分层（本轮新增）：把"必须由文档证明"与"只能由实现证伪"分开。
   前者在 A0 清零，后者进入 §12.8 carry-forward 并绑定到实施阶段的 gate。
   分层线是【安全类】的定义，不是难易或工作量。见 §0 A0 Gate 与 §12.8
```

S1 的动机：round-2 的 P1-A、P1-D 与 round-2.5 自查的 X1、X2 是同一个缺陷的四个实例。round-3 证明第一版 S1 仍漏两处，round-5 又漏两处（`memory_repository.go`、`transport/watch_handshake_test.go`），方法层缺陷与修订见 §11.1。S2 的动机：P1-C、P2-A 与 P0-A 的一部分都是把只能靠编译/运行证实的平台事实当成冻结断言写死；round-3 的 P0-1 是同一类错误的再次发作。S5 的动机最直白：跨阶段编译依赖是 `go build` 五分钟就会撞上、在纯文档层却要到第五轮才被挖出来的东西——它本身就是"某些问题只能由实现阶段证伪"这一论点的证据。

---

## 1. 已冻结的安全属性

Adapter 必须实现协议，不能通过修改协议来迁就基础设施。

### 1.1 命令式 Saga 边界

ScalePlan 只执行显式提交的 workload、replica 目标和 shard move manifest；CPU/QPS/P99 阈值、扩缩容高低水位、持续窗口和 cooldown 属于外部 autoscaling policy，不属于 ScalePlan。

### 1.2 Scale-in 固定顺序

```text
SERVING
-> durable PlacementExcluded
-> victim 继续 Serving/Renew/作为 RoutePlan Source
-> 逐 shard 迁空
-> READY_ZERO_SHARD
-> Registry + Placement 双 Zero-Shard Proof
-> StatefulSet replicas 减一
```

上述为成功路径；victim Pod 最终被删除并由新 boot 取代，其 exclusion 因 boot 不匹配而变为永久惰性，不需解除。**非成功路径**（中止、失败 terminal、BLOCKED 安全终止）下 victim 进程仍然存活且 boot 未变，必须先 owner-scoped 解除 exclusion 才允许进入 terminal，否则会留下一个已迁空、又不能接收新 placement 的死容量，并永久阻塞对该 Holder boot 的重试。

两个关键约束：解除必须**排在** scale 写结果被 exact 解析为 NOT_COMMITTED 之后（round-3），否则可能把 shard 迁入一个 Kubernetes 正准备删除的 Pod；而"无需解除"只由**正面证明**构成——当前唯一被接受的是 `current_holder=BOOT_MISMATCH`（round-4 收紧）。"Holder 记录已消失"**不**属于无需解除：Holder 记录有 3 分钟 TTL 而 exclusion 记录无 TTL，同 boot 可在 TTL 失效后重新注册并让 exclusion 恢复生效，因此记录缺失只是证据缺失，必须保持 BLOCKED 并重试。完整生命周期、四条释放结局与释放时机见 §8.3。

禁止：

- 将 PlacementExclusion 映射成 `DRAINING`；
- 非空 victim manifest 时提前接受 `READY_ZERO_SHARD`；
- 在迁空前删除 Pod；
- 仅凭 Pod readiness 判断可缩容；
- 绕过 RoutePlan Production Verification；
- 在 victim 仍存活且 boot 未变的情况下带着未解除的 exclusion 进入 terminal；
- 在 scale 写结果仍为 unknown 时解除 exclusion。

### 1.3 精确运行身份

动态 Holder 身份：

```text
Domain / HolderID / BootID / PodUID / WorkloadUID / Ordinal
```

语法：visible ASCII `0x21..0x7e`、无空白、每个 opaque identity 最多 128 bytes、ordinal 为 canonical non-negative int32。BootID 是进程 generation，必须每次进程启动随机生成并在 Authority、HolderControl、HolderAgent 间共享；不得从可复用配置注入。

### 1.4 Fail closed

以下任一情况必须保持原 durable ownership，不产生新的不安全副作用：

- partial/incomplete observation；
- UID/Boot/ordinal/owner-reference drift；
- resourceVersion/CAS conflict；
- RoutePlan evidence missing/malformed/regressed；
- placement snapshot/token drift；
- Manager transition/recovery unknown；
- proof 过期；
- fence 丢失；
- corruption/checksum mismatch；
- unknown outcome 无法 exact re-read；
- Controller overlap 未被 fencing token 排除。

---

## 2. 阶段、依赖与并行边界

修订后的阶段图：

```text
A0  Architecture + Threat Model（本文，只写 docs）
 |
 v
A0.5 Shared Production Extension Seams（单一 Writer，需另行授权）
 |\
 | +--> A1  ScalePlan Durable Repository（Miniredis only，全新 keyspace）
 | +--> A1R Production Control-Store Deltas（RoutePlan/RouteControl 持久层，additive only）
 | +--> A2  Kubernetes Adapter（Fake API only）
 | +--> A3  Control/RoutePlan/Quiescence（Fake/In-process，只依赖 A0.5 冻结的合同）
 |                |
 +----------------+--> A4 Composition（off by default，不部署）
                          |
                          v
                    A1/A1R/A2/A3/A4 Compatibility Review
                          |
                   单独请求 A5 授权
                          v
                    A5 isolated 1 -> 2 -> 1
                          |
                   单独请求 A6 授权
                          v
                    A6a execution capacity
                    A6b autoscaling policy（独立工作流）
```

A1/A1R/A2/A3 只有在 A0.5 冻结共享接缝后才能并行。每个阶段必须独立 worktree、独立分支、唯一主 Writer、互不重叠的 Owned Write Set。A4 只消费各阶段 reviewed commits；不得由多个 Writer 同时修改 composition、Manager、RoutePlan Executor、Holder lifecycle 或 fence 语义。

A3 只依赖 A0.5 冻结的 durable-stop / snapshot-bootstrap 合同，不依赖 A1R 的实现，因此可与 A1R 并行；A4 必须同时消费 reviewed 的 A1R 实现。

A1R 与 A1 的风险等级不同，必须分开对待：

```text
A1  写全新的 ScalePlan keyspace，当前无生产数据，schema 自由
A1R 修改 G21 运行时正在使用的持久层，必须 additive-only、向后兼容、不破坏既有 schema，
    每个 delta 必须对应 §11.2 中一条具名合同，且不得改变现有 G21 identity 与既有脚本语义
```

---

## 3. 总体组件图

```text
Authenticated manual operator / routectl
                  |
                  v
       ScalePlan manual API (A4, mode=manual)
                  |
                  v
        scaleplan.Executor + Recovery
          |         |          |
          |         |          +--> RoutePlanPort / Verification
          |         +-------------> Trusted Holder + Placement + Exclusion
          +-----------------------> Kubernetes WorkloadScaler
                  |
                  v
      core-owned DurableRepositoryFacade
          |                  |
          |                  +--> repository-owned distributed mutation lease
          v
 RedisScalePlanBackend (A1, same-slot atomic Lua)

Blocked safe termination:

core BlockedTerminationOwner
 -> repository mutation lease
 -> ControlMutationCoordinator exclusive fence (A3)
 -> durably stop every owned child / close recovery
 -> complete K8s + Holder + Placement + Manager capture
 -> final revalidate
 -> core-owned sealed terminal mutation
 -> Redis atomic Plan+Receipt+Proof+ownership release
 -> bounded fence release/recovery
```

核心原则：

1. 外部 package 只能实现低层 backend/reader 接口，不能实现或伪造 core 的安全终止 owner；
2. 所有生产写者必须使用同一 durable mutation/fence coordinator；
3. 进程内 `sync.Mutex`、channel、map 只能作本地优化，不能作为生产 fencing 证明；
4. Kubernetes mutation 不是 Redis 事务的一部分，必须通过 durable active-operation registry 与 RBAC/admission threat boundary 关闭竞态；
5. 完整 placement snapshot 不能由逐 shard read 拼接。

---

## 4. 威胁模型

## 4.1 需要保护的资产

- 一个 workload 同时最多一个 active ScalePlan；
- StatefulSet UID、replicas 与 Pod generation；
- Holder exact process identity；
- shard placement 的唯一 active writer；
- PlacementExclusion 的 owner/revision；
- RoutePlan/Manager transition 的可恢复性；
- blocked plan 的 workload ownership；
- durable Plan、Receipt、Proof 与 audit retention；
- G21 与现有 Route/Capacity evidence 身份隔离。

## 4.2 可信组件

- reviewed ScalePlan Core；
- core-owned durable repository facade；
- A1 Redis backend 与限定 ACL user；
- A2 Kubernetes adapter 与最小权限 ServiceAccount；
- A3 control coordinator、RoutePlan/Manager adapters；
- Kubernetes API server 与 Redis server；
- mTLS 验证后的 manual operator identity。

## 4.3 不可信输入与故障

- operator manifest；
- Holder Registry 自报 claims；
- stale watch/list/cache；
- Redis/Kubernetes timeout、response loss；
- Controller crash/restart；
- 两个 Controller overlap；
- stale lease holder；
- malformed/corrupt durable bytes；
- Pod/StatefulSet delete/recreate；
- 第三方 replicas mutation；
- delayed RoutePlan/Manager recovery；
- clock skew；
- partial network partition。

## 4.4 Threat model 外但必须显式记录

拥有 cluster-admin、Redis admin 或主机 root 的恶意操作者可以绕过普通 RBAC/ACL。A5 资格仅在以下条件成立时有效：

- 独立 Namespace 无 HPA、GitOps reconciler 或其他 scale writer；
- 只有 ScalePlan ServiceAccount 有 `/scale` 写权限；
- audit 未发现越权 writer；
- Redis admin/host root 未干预；
- **route snapshot 的写者集合同样有界**（round-3 P1-2）：除受 gate 管控的 `DurableSnapshotPublisher.Bootstrap` 外，只存在 `cmd/cap-static-route-init` 一个直连写者，且它未在生产 realm 的 namespace 内运行过。前一版本的单 writer 前提只讨论了 Kubernetes `/scale`，完全没有覆盖持久快照，这是一处前提缺口而不只是文字遗漏。

### 已接受风险：`statefulsets: patch` 的越权面（round-3 P2-5）

§7.2 的 RV 取消屏障需要在 `statefulsets` 上增加 `patch`。若集群不支持 `ValidatingAdmissionPolicy`（§12.7 `K8S_VALIDATING_ADMISSION_POLICY`），该权限**无法**被限制到字段级。风险的准确表述是：

```text
patch 允许修改 spec.template.spec.serviceAccountName 与 volumes
-> 可把该工作负载改成以 Namespace 内任意 ServiceAccount 运行
-> 即 Namespace 内的权限提升
并且任何 spec.template 变更都会触发全量滚动重启
-> 即一次全域可用性事件
```

前一版本把它表述为"拥有 patch 全字段能力，由 audit 证明其从未修改 spec"，这不足以让安全 owner 做出知情接受——audit 是事后的，而上述后果是即时的。因此该风险必须由安全 owner 按"Namespace 内权限提升 + 全量滚动重启"这一表述书面接受；不接受则 `ValidatingAdmissionPolicy` 从"推荐"升为 A5 的硬前置条件。

若出现越权写入或无法证明 writer 集合，证据无效并 fail closed；不得通过 Adapter 猜测修复。

### 已接受缺口：深度审计发现的 corruption 无法阻断写入（CF-3，2026-08-13）

删除 corrupt-latch 通道 B 的代价，必须按其真实形状陈述，而不是按"少一条冗余通道"陈述：

```text
逐操作校验的范围是【有界】的：只验本次 operation 的 KEYS / meta / lease /
候选 / exact direct references / bounded cleanup manifest，不扫全 realm。
AuditPage 存在的全部意义就是覆盖这个范围之外的深度损坏。
删除通道 B 之后，AuditPage 在 touched-key 之外发现的 corruption：
  - 只能产出一份只读报告
  - 无法置位 latch
  - 后续 mutation 不会重新发现它（范围有界）
=> 一个已知损坏的 realm 会继续被写入，直到有人读到那份报告并人工介入
```

**接受它的理由不是缺口不存在，而是缺口当前空转**：系统里没有审计者这个角色，`AuditPage` 没有任何调用方，ACL 里也不存在只读审计 principal。一条无人走的路径上的缺口，不产生实际风险。

**造价侧**：通道 B 要正确实现需要 coordinator 签发的短时效 capability、发放/撤销/限流三套机制、Lua 侧对"已签发未撤销"集合的 CAS（只查撤销等于接受从未签发过的凭据）、以及一条独立的误置位申诉判据。§6.5 冻结的 keyspace 里没有对应 key，§11.2 里没有合同拥有它。

**重新决定的触发条件（不是建议，是条件）**：`AuditPage` 获得第一个真实运行者时。在那之前它是空转缺口，在那之后它是 fail-open。A5 前置说明与运维 runbook 必须包含本段。

## 4.5 主要攻击/事故面与控制

| 风险 | 控制 |
|---|---|
| 直接伪造 safe terminal | core sealed mutation；普通 repository 无 terminal API |
| 两 Controller 同时写 | Redis fencing epoch + active operation registry + write-script check |
| 旧 Controller lease 过期后继续写 | 每次 durable commit 校验 fencing token；丢 lease 取消 context |
| K8s API response loss | exact UID/RV/replicas re-read/adopt |
| Pod claims 伪造 | Kubernetes API owner-reference 与 Registry 独立 join |
| 假完整 placement | 单 snapshot identity、全 scope cardinality、end revalidate |
| child 在 fence 后恢复 | durable terminal stop + restart index removal + recovery gate check |
| proof 到 terminal CAS 竞态 | fence 覆盖至 repository linearization；repository time expiry check |
| Redis corruption | exact shape/bounds/checksum/reference validation，零写入 |
| unbounded recovery | bounded ZSET index；无 SCAN/KEYS |
| off 模式副作用 | 不构造 client/worker/API/goroutine，不读 ScalePlan keys |

---

## 5. A0.5：Core-owned Production Extension Seams

## 5.1 为什么 A0.5 是硬依赖

当前 `internal/scaleplan` 使用 package-private 接口：

```text
commitBlockedTermination(...)
enterPlanMutation(...)
repositoryIdentity()
```

这能阻止普通调用者伪造 terminal commit，但 Go 的其他 package（例如 `internal/storage/redis/scaleplan`）也无法直接实现这些未导出方法。因此不能让 A1 Redis store 直接冒充 `blockedTerminationRepository`。

A0.5 必须在单一 Writer 下新增**生产可扩展但仍 sealed**的接缝，且专项独立复审 `0/0/0`。不得让 Adapter 直接公开 `CommitBlockedTermination`。

## 5.2 冻结的 paired production runtime/facade 形态

概念 API（名称可在 A0.5 review 中微调，语义不可削弱）：

```go
// internal/scaleplan，exported only for backend implementation.
type DurableRepositoryBackend interface {
    AcquireMutationLease(ctx, scope, owner) (BackendMutationLease, error)
    RenewMutationLease(ctx, lease) (BackendMutationLease, error)
    ReleaseMutationLease(ctx, lease) error

    CreateOrGet(ctx, lease, command DurableCreateCommand) (CreateResult, error)
    Load(ctx, planID string) (PlanRecord, bool, error)
    CompareAndSwap(ctx, lease, command DurableCASCommand) (CASResult, error)
    ListRecoverable(ctx, cursor RecoverableCursor) (RecoverablePage, error)

    ApplySealedBlockedTermination(ctx, lease,
        command SealedBlockedTerminationMutation) (BlockedTerminationResult, error)
}

type ProductionRuntimeOptions struct {
    RepositoryBackend DurableRepositoryBackend
    QuiescenceBackend QuiescenceBackend
    // Controller identity, realm scope, lease/proof bounds.
}

type ProductionRuntime struct { /* unexported capabilities */ }

func NewProductionRuntime(options ProductionRuntimeOptions) (*ProductionRuntime, error)
func (r *ProductionRuntime) Repository() ScalePlanRepository
func (r *ProductionRuntime) NewExecutor(options ExecutorDependencyOptions) (*Executor, error)
```

`NewProductionRuntime` 在 core 内一次性配对构造未导出的 repository facade、唯一 `BlockedTerminationOwner` 与 approved quiescence coordinator。它不导出 owner，也不允许调用者把任意 public `BlockedQuiescencePort` 注入另一个 owner。现有 `NewBlockedTerminationOwner` 仅保留 memory/test construction；A0.5 必须使其拒绝 production facade（private production capability mismatch），并增加编译/运行测试证明外部 package 无法为 production repository 创建 alternate owner。

Production runtime/facade：

- 实现普通 `ScalePlanRepository`；
- 实现 package-private `blockedTerminationRepository`；
- 持有非零 private `repositoryIdentity`；
- 拥有 local serialization 和 durable mutation lease；
- 是唯一可 mint `SealedBlockedTerminationMutation` 的组件；seal 绑定 private owner capability、repository identity、mutation lease/fence、quiescence fence epoch、canonical evidence digest 与 exact blocked checkpoint；
- 验证 backend 返回的 Plan/Receipt/Proof；
- 不向 Executor 或 operator 暴露 backend。

`SealedBlockedTerminationMutation` 是 exported opaque value，但所有字段与 seal 私有；backend 仅通过只读 accessor 获取 canonical bytes、expectation、proof digest、lease/fence values。零值、手工字面量、alternate owner、错误 facade/repository/quiescence epoch 的 command 必须 `Authorized()==false`，A1 backend 必须在任何 Redis 调用前拒绝。Seal 不是安全性的唯一来源：backend Lua 仍独立验证 durable current Plan、fence、proof、expiry 和 ownership。

禁止方案：

- 导出 `CommitBlockedTermination(expectation, receipt)`；
- 让 Redis store 直接实现 package-private owner；
- 仅靠注释约定“不要调用”；
- 将 seal 变成可配置字符串；
- Adapter 自行构造 terminal candidate 或 workload unlock。

## 5.3 Repository mutation lease

**现状（round-4 更正；round-3 P1-3 的"更正"本身是错的）。** 本节经历了两次相反的修订，这里给出经 round-4 独立复算确认的版本。

round-2 版本称"现有 Executor 在七个 mutation 入口周围进入 repository-owned barrier"。round-3 复审据 `rg -n 'enterPlanMutation' internal/ cmd/` 只有三处命中、`executor.go` 零命中，判定该说法为假，本文档上一版据此冻结了"七个入口一个都没进 barrier"。**这个检索是大小写敏感的**，只匹配到未导出的 `enterPlanMutation`（它确实只有一个调用者），漏掉了 Executor 实际调用的**导出包装** `EnterPlanMutation`。round-2 的原始描述是对的。

exact 现状（round-3 P2-4 同时更正了两个方法名，round-2 版本写作 `Reconcile`/`Terminate`）：

```text
入口方法                                    barrier 进入点        scope 实参
executor.go:52    Submit                    executor.go:56       ""（PlanID 尚不存在）
executor.go:94    Execute                   executor.go:98       planID
executor.go:161   Step                      executor.go:165      planID
executor.go:956   Pause                     executor.go:960      planID
executor.go:985   Resume                    executor.go:989      planID
executor.go:1022  ReconcileBlocked          executor.go:1026     planID     （不是 Reconcile）
executor.go:1136  OperatorFailBlocked       间接：blocked_termination.go:74  （不是 Terminate）
                    -> BlockedTerminationOwner.TerminalizeBlocked -> EnterPlanMutation

internal/scaleplan/blocked_termination.go:60  导出包装 EnterPlanMutation(ctx, _ string)
                                              **scope 实参被显式丢弃**
internal/scaleplan/blocked_termination.go:64  转发 repository.enterPlanMutation(ctx)（无 scope）
internal/scaleplan/memory_repository.go:49    mutation: make(chan struct{}, 1)
internal/scaleplan/memory_repository.go:54-65 单个全局 channel，无 per-plan 维度
```

七个入口全部已在 barrier 内：六个直接、一个经 `TerminalizeBlocked` 间接。因此 A0.5 的工作**是改造已有接缝**，不是新引入租约边界。工作量按此重估——比上一版描述的小，但多出两项上一版没有识别的工作：

```text
1. 让被丢弃的 scope 参数产生意义。现役 EnterPlanMutation 接收 scope 却直接丢弃，
   转发的 repository 方法根本没有 scope 形参。接上 durable lease 时这条参数链
   必须整体打通，否则 scope 会继续是装饰性的。
2. 收紧导出面。EnterPlanMutation 是 BlockedTerminationOwner 上的【导出】方法，
   barrier 目前对任何持有 owner 引用的 package 都可达。这与 §5.2「不导出 owner
   能力」的封装意图存在张力：durable lease 接上之后，任意 caller 都能取得
   realm 级互斥。A0.5 必须一并处理——或改为未导出并由 Executor 内部转发，
   或保留导出但要求携带 guard 凭据。
```

风险评估相应下调：不触及"为运行中的主路径新增互斥"，而是在既有互斥点上替换实现，before/after 行为等价测试仍然必须逐入口做，但基线行为是已存在的。

**realm-global 而非 per-plan 的理由。** round-3 判定"`Submit` 在 PlanID 生成前已经进 barrier"这条前提不成立并要求重给理由；按上文，该前提**成立**，本轮恢复它作为第一条理由。四条独立理由，任何一条单独成立即足够：

```text
1. Submit 在 PlanID 生成之前就已经进 barrier——executor.go:56 传入空串进入，
   executor.go:67 才 BuildInitialPlan 生成 Plan。此时没有 plan_id 可作为 lease
   scope，只能用 realm。这是源码事实，不是设计选择。
2. 现役 barrier 已经是 realm-global 的。memory_repository.go:49 是【单个】容量
   为 1 的全局 channel，enterPlanMutation 连 scope 形参都没有，全 repository
   互斥。改成 per-plan 是【放宽】现役语义，需要独立论证，而不是相反。
3. §8.6 的 quiescence gate 本身是 realm 范围的（EXCLUSIVE 覆盖整个 realm 的
   Manager/RoutePlan/Publisher writer 集合）。若 mutation lease 是 per-plan 而
   gate 是 realm-global，§8.8 的锁顺序会出现两个不可比较的粒度。
4. 功能版本的 max_routeplans_inflight=1、max_shards_per_wave=1 已经把同 realm
   的扩缩容串行化，per-plan lease 带不来任何实际并行度。
```

不得为了并行度弱化正确性；若将来要放宽到 per-plan，属于 A0 amendment。

生产 facade 的映射：

```text
scope = Namespace + Realm
owner = ControllerID / ControllerBootID / random operation nonce
fence = monotonic canonical decimal counter allocated by Redis script
```

**§8.8 锁顺序的第一环同步更正（round-4 二次更正）**：该环在当前源码上**已经存在**且已经是 realm 范围的（七个 Executor 入口全部经 `EnterPlanMutation` 进入同一个全局 barrier）。上一版称"它是 A0.5 要新建的"，源于 round-3 的大小写敏感检索，本轮撤回。A0.5 的工作是把这个已存在的内存 barrier 换成 durable lease，锁顺序本身不变。

A0.5 必须改变当前只返回 `release func` 的 mutation seam。冻结形态：

```go
type PlanMutationGuard interface {
    Context() context.Context       // lease-bound derived context
    Lease() DurableMutationLease    // opaque owner/fence/expiry
    Release(context.Context) error  // joins renewer shutdown
}

func (r *ProductionRuntime) EnterPlanMutation(ctx context.Context, scope MutationScope) (PlanMutationGuard, error)
```

Executor 的全部受保护 read/effect/commit 必须使用 `guard.Context()`，而不是原始 caller context；renew loss 会 cancel derived context。Release 必须等待 renewal goroutine 退出，再 compare-and-release durable lease。Memory repository（`memory_repository.go`，该接缝的唯一实现者）可用同接口的无 renew guard。

**导出面的定夺（round-5 P2）。** 前一版本一边要求"收紧 `EnterPlanMutation` 的导出面"，一边把冻结形态写成导出方法，两句话没有同时成立的读法。定夺的关键是**它们是两个不同类型上的两个方法**，方向相反：

| 方法 | 定夺 | 依据 |
|---|---|---|
| `BlockedTerminationOwner.EnterPlanMutation` | **改为未导出** `enterPlanMutation` | §5.2 的封装意图针对的正是这一个——任何持有 owner 的 package 都能进 barrier。全部调用者已枚举且**全部在包内**：`executor.go` 六处 + `blocked_termination.go` 一处，收紧不破坏任何跨包调用者 |
| `ProductionRuntime.EnterPlanMutation` | **保持导出**，即上方冻结形态 | 它是 §5.2 sealed facade 的唯一 sanctioned 入口。§12.2 的 `reference_adapter_exercises_every_seam` 要求 `internal/testkit/scaleplan/**`（**另一个 package**）跑通"guard context 取消"，跨包拿 guard 必须有导出入口。把它也收紧会让该 gate 无法满足 |

因此 §12.2 的 `enter_plan_mutation_export_surface_tightened=true` 精确指前一行，不指后一行。若 A0.5 发现 `ProductionRuntime.EnterPlanMutation` 之外还需要第二个跨包 barrier 入口，属 §17 停止条件。

Lease 要求：

- acquire 使用 Redis `TIME` 与原子 fencing counter；counter 以 canonical decimal string 存储，其域上限、解析与格式化规则统一遵循 §6.13；达到上限永久 fail closed `FENCE_EXHAUSTED`，不得 wrap/reset；
- local channel/mutex 防止同 facade 重入；
- heartbeat 间隔 5s，lease TTL 20s（可配置但上限 60s）；
- renew 丢失后立即取消受保护 operation context；
- 所有 A1 write Lua 在 linearization 时校验 owner nonce、fence、expiry；
- stale owner 即使继续运行也不能提交 durable write；
- release 必须 compare owner+fence，response unknown 时由 exact lease read/recovery 处理；
- Controller restart 不复用 BootID/nonce；
- no-force-unlock；只有 expiry/recovery 或 exact owner release；
- acquire/renew/release 的 apply-then-response-loss 均通过 exact owner+nonce+fence read 分类；
- active operation registration/completion 同样有 deterministic operation ID 与 exact replay。

外部副作用在 lease 丢失后仍可能产生 unknown outcome，因此下一 owner 必须使用既有 write-ahead intent exact re-read/adopt；不得盲目重发。

## 5.4 其他必须由 A0.5 冻结的共享 seam

1. `CompletePlacementCapture`：明确 realm/domain/workload scope、global revision、checksum、fingerprint、total cardinality、完整 canonical placements 与 revalidate token；
2. `RoutePlanDurableStop`：按 owner ScalePlanID、child PlanID、payload exact terminalize/pause，并原子移出 restart index；
3. `ControlMutationCoordinator`：所有 Manager/RoutePlan/Kubernetes control writer 进入 shared gate；
4. `KubernetesIdentitySnapshot`：A2 输出、A3 消费的 Pod/StatefulSet authoritative join DTO；
5. `NestedControlOperationToken`：可沿 RoutePlan -> Manager -> Publisher 调用链继承，不重复获取非重入 gate；
6. fence token/accessor：不能由 provider 仅返回可伪造 bool；
7. common failure classification：Conflict/Pending/OutcomeUnknown/Dependency/Corrupt 的唯一映射。

A0.5 若发现必须改变已冻结 ScalePlan/RoutePlan/Manager 语义，应停止并回到专项协议 Review；不能在 Adapter package 中暗改。

---

## 6. A1：Durable ScalePlan Repository 设计

## 6.1 Owned/Forbidden Write Set

提议 Owned Write Set（实现需另行授权）：

```text
internal/storage/redis/scaleplan/**
internal/scaleplan/codec/**
internal/testkit/scaleplanstore/**
docs/verification/scaleplan-a1-*
```

A1 禁止修改：

```text
internal/scaleplan/model.go
internal/scaleplan/executor.go
internal/scaleplan/state_machine.go
internal/actorrouting/authority/**
internal/routeplan/**
cmd/route-controller/**
deploy/**
```

共享 seam 只能由 A0.5 Writer 修改。

## 6.2 Repository instance 与 Redis Cluster hash slot

一个 production repository instance 固定：

```text
redis Namespace(Environment, Zone)
+ exact Realm
+ schema layout v1
```

Environment/Zone 先通过现有 `rediskey.Namespace` grammar（首字符 `[a-z]`，其余 `[a-z0-9_-]`，最大 32 bytes），再增加 ScalePlan ACL-safe 限制，拒绝 `{`、`}`、`*`、`?`、`[`、`]`、`\\`、`:` 与控制字符。Realm 使用 visible ASCII no-whitespace max128 后只将 lowercase SHA-256 放入 hash tag。ACL pattern 与 Redis keys 必须由同一个 validated KeyBuilder 生成，禁止手写 interpolation。

所有原子相关 key 必须使用同一 hash tag：

```text
{classic-farm:<environment>:<zone>:scale-plan:v1:<realm_sha256>}:
```

`realm_sha256` 是 canonical realm bytes 的 lowercase SHA-256；meta 中同时保存原始 realm 与 digest，防止错误配置/碰撞被静默接受。A1 KeyBuilder 是唯一 key 来源，并提供 `AssertSameSlot` 测试。

禁止按 PlanID 使用不同 Redis slot；否则 CreateOrGet 与 terminal ownership release 无法原子化。

## 6.3 Keyspace

```text
...:meta                              HASH
...:lease                             HASH (realm mutation lease)
...:lease-fence                       STRING canonical decimal counter (§6.13)
...:corrupt-latch                     HASH (realm fail-closed latch, §6.4)
...:idempotency:<sha256(key)>         HASH
...:active:<sha256(workload-key)>     HASH
...:plan:<hex(plan-id)>               HASH
...:recoverable                       ZSET
...:terminal                          ZSET
...:accounting                        HASH (plan member -> canonical bytes)
```

所有动态 suffix 只接受 canonical lowercase digest/hex；raw ID/key 保存在 record 内并 exact compare。不存在通过 SCAN/KEYS 发现计划的路径。

## 6.4 Record schemas

### Plan record `scale-plan-record-v1`

固定字段和固定 field count：

```text
schema
realm
plan_id
revision
status
updated_at_ms
completed_at_ms
recovery_retain_until_ms
terminal_retain_until_ms
canonical_json
checksum_sha256
canonical_bytes
```

- canonical JSON 由 `internal/scaleplan/codec` 显式 DTO 生成，字段顺序冻结；
- 不直接依赖 Go 默认 JSON field name/order；
- checksum scope：`scale-plan-record-checksum-v1 || canonical_json`；
- `Plan.Validate()`、PlanID、payload、receipt、proof 在 read 后全部重算；
- maximum record bytes 初始 1 MiB，配置上限不可超过 8 MiB；
- oversized record 在进入 Lua 前拒绝。

### Idempotency record `scale-plan-idempotency-v1`

```text
schema / realm / key_digest / raw_key / payload_digest / plan_id / retain_until_ms
```

### Active workload record `scale-plan-active-workload-v1`

```text
schema / realm / workload_digest / canonical_workload_key / plan_id / plan_revision
```

### Meta `scale-plan-meta-v1`

```text
schema / realm / realm_digest / count / bytes / active_count
recoverable_count / recoverable_bytes / terminal_count / terminal_bytes
store_epoch
```

`store_epoch` 是 canonical decimal string，域上限与算术规则见 §6.13；每个改变 Plan/reference/index 的原子 mutation 增加一次。

meta **只维护可精确增量维护的完整性量**：`store_epoch` 与各索引的 cardinality/bytes。原先的 `recoverable_index_checksum` / `terminal_index_checksum` 字段已删除，理由是有序流的 SHA-256 在数学上不可能由 old digest 加 delta 推出，任何声称"增量更新全流摘要"的实现要么是错的，要么实际在每次 mutation 里全量重读索引（违反 §6.4 的 bounded validation scope）。不引入 Merkle 替代，因为 bucket 函数、leaf canonical 编码、fanout、更新键集与证明边界都需要另行冻结，其新增的规范面大于它在本规模下带来的收益（默认 1024 项、硬上限 1,048,576 项）。

深度完整性由 §6.4 的 `AuditPage` 只读协议提供：bounded 遍历重算 cardinality/bytes、reference 一致性与 accounting，可在 bounded realm gate 下做全量重算比对。

### Durable corruption latch

```text
...:corrupt-latch  HASH  scale-plan-corrupt-latch-v1
  schema / realm / detected_at_ms / store_epoch_at_detection / class / evidence_digest / detector_identity
```

规则：

- 任何 mutation 或 audit 检测到 corruption 时，以 `store_epoch` 做 CAS 原子写入 latch，与该次检测的零写入语义一起提交；
- **latch 写入自身是一次 durable write，因此必须被授权**（round-3 P1-12）。理由：latch 一旦置位就永久阻断整个 realm 且只能人工解除，若不做任何校验，§4.3 明列的不可信故障"stale lease holder"——一个已丢租约、ACL 凭据仍有效、可能因自身 bug 或读到瞬时不一致而误判 corruption 的旧 Controller——就可以单方面把整个 realm 打成不可用。这与 §5.3 的绝对承诺"stale owner 即使继续运行也不能提交 durable write"直接冲突；
- **授权只有一条通道**（CF-3 处置，2026-08-13；取代 round-4 的双通道方案）：

```text
通道 A：有效 mutation lease（owner nonce + fence + expiry）
        linearization 时与其他 A1 write Lua 用完全相同的规则校验
        适用于：mutation 自身的逐操作校验发现 corruption
```

原 round-4 方案还有一条通道 B（一次性 audit capability），供合法持只读 ACL、按授权跑 `AuditPage`、按定义不申请 mutation lease 的审计者置位 latch。它已被整体删除，代价记入 §4.4。

- **删除通道 B 恢复了 round-3 的那个缺口，这次是被知情接受而不是被忽略**。缺口的准确形状：

```text
"后续 mutation 会读到 latch" 只有在 latch 已被置位时才是保护，
而唯一发现该 corruption 的一方恰好被禁止置位。
后续 mutation 也不会自己重新发现它——本节的 bounded validation scope
明确只验证本次 operation 的 KEYS / meta / lease / 候选 / exact direct
references / bounded cleanup manifest，【不】扫描全 realm。
AuditPage 存在的全部意义就是覆盖逐操作校验看不到的深度范围。
=> AuditPage 在 touched-key 集合之外发现的 corruption 无法置位 latch，
   明知已损坏的 realm 继续被写入。
```

  round-4 判定这是 fail-open 因而补了通道 B；CF-3 判定在 v1 不建这条通道，理由不是缺口不存在，而是**当前没有审计者这个角色，`AuditPage` 没有任何调用方，因此缺口是空转的**。缺口在 `AuditPage` 获得第一个真实运行者的那一刻起变为实际风险，届时必须重新决定（§4.4）。

- **删除同时消掉了通道 B 自带的 DoS 面**：持有有效 audit capability 者本可以使一个 realm 永久 fail-closed。这一条不算删除的收益——它是 latch 的设计意图而非缺陷——但它连同发放、撤销、限流、以及一条独立的误置位申诉路径，构成了这条通道的真实造价，而这正是在没有审计者的前提下不建它的理由；
- **每个 mutation 脚本第一阶段的验证必须读取 latch 并要求 absent**；latch 存在则零写入返回 `REALM_CORRUPT_LATCHED`；这是 latch 相对只读告警的唯一价值——只读 `AuditPage` 无法阻止另一个 Controller 继续写入；
- latch 只能由离线、授权、完整修复流程清除，Adapter 不自动 repair、不设 TTL、不自动过期；
- **误置位的申诉路径**（round-5 P1，CF-3 后简化为单通道）。只剩通道 A 后不再需要按 `detector_identity` 分叉，但**分叉存在的理由必须留档**：round-5 之前的统一判据要求"证明 detector 当时持有有效 fence"，而通道 B 的检测者按定义从不持有 fence，于是通道 B 置位的 latch 结构上不可清除、realm 永久锁死且无出路。任何将来重新引入非 lease 置位通道的方案，必须同时给出该通道的等价清除判据，否则就是把这个死锁重新造出来。

```text
前置（任一不可核验即不得清除）：
  store_epoch_at_detection / detector_identity / evidence_digest 三者可核验
  且 evidence_digest 对应的证据经离线复算【不成立】（即确属误判）

额外证明：
  detector 在 detected_at_ms 持有有效 fence

不满足 -> 不得清除，升级为完整数据核查
```

- `AuditPage` 必须 pin start `store_epoch`，每 page 与结束时重校验；epoch 漂移返回 conflict，不得报告 clean。

显式接受的爆炸半径：latch 是 realm 级 fail-closed，触发后会阻断该 realm 内**全部** mutation，包括与损坏记录无关的健康 Plan 的 recovery，且只能人工解除。对 v1 的 manual-only、单 inflight plan 场景这是可接受的取舍；必须写入 A5 前置说明与运维 runbook，不得在 Adapter 里用"跳过损坏项继续"来软化。

每个 read/mutation 均验证 exact shape、canonical integer、bounds、reference consistency 和 accounting。

### Bounded validation scope

“corruption zero-write”精确定义为：每个 script 在首个 mutation 前，必须完整验证 corrupt latch absent、该 operation 的全部 `KEYS`、meta/lease、候选、exact direct references、以及本次 bounded cleanup/page manifest；不宣称每次 operation 都扫描并验证 realm 内最多 1,048,576 records。动态 keys 由显式 bounded manifest 作为 `KEYS` 传入，ARGV/KEYS 总 bytes 默认上限 4 MiB、hard 16 MiB，单 script touched plan 默认 64、hard 256；超过返回 `BOUND_EXCEEDED`，零写入。

另设 `AuditPage(cursor, limit<=256)` 只读 full-audit protocol，按 recoverable/terminal indexes bounded 遍历并重算 meta/accounting/索引 cardinality 与 bytes；发现 corruption 时按本节 corrupt latch 以 epoch-CAS 阻断后续 mutation，但不自动修复。实现 benchmark 必须证明目标 Redis 7.2 的 Lua max execution budget（初始 gate 50ms local p99）与参数上限；不满足则降低 bounds并复审，不能放宽 script timeout。

## 6.5 Terminal retention

Production ScalePlan terminal Plan、Receipt、Proof 与 idempotency record 固定物理保留：

```text
38 days after CompletedAtMs
```

理由：覆盖最长 `7d plan TTL + 30d recovery retention = 37d`，并留 1 天安全余量；与 RoutePlan production retention 对齐。

非 terminal（包括 BLOCKED）不得 TTL 自动消失。Terminal cleanup 只通过显式 bounded `terminal` ZSET 进行，不依赖 native key TTL 先行删除，以避免 dangling index。每次 mutating operation 最多清理配置上限（默认 64）个已到期 terminal records；另可由 A4 bounded maintenance 调用相同脚本。到期项超过单次上限时返回 `MORE_DUE`，调用方继续 bounded batch；容量不足时 fail closed。

Receipt/Proof、Plan、idempotency record 必须同批删除；不得提前删除 proof。Terminal record 不保留 active workload index。

## 6.6 Recoverable index

- `recoverable` ZSET member 为 encoded PlanID；score 为 `UpdatedAtMs`；
- 只含 nonterminal Plan；
- maximum active/recoverable items 默认 1024、硬上限 1,048,576；
- `ListRecoverable` 必须显式 page size，默认/硬上限 256；
- 先以 Lua 验证 ZCARD、member syntax、score、meta/accounting；
- Go 构造 exact Plan keys，再调用 bounded batch-load Lua；
- batch-load 前后比较 store epoch 与索引 cardinality；变化则 retry bounded 次或返回 conflict；
- 任一 member dangling、duplicate、record corrupt 或 status terminal 均使整页失败，不返回 partial list；
- 不做自动“修复”未知 corruption。

## 6.7 Redis authoritative time

所有会改变 audit/expiry/lease 的脚本必须在 Lua 内调用：

```lua
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
```

Controller 提供的时间只能用于预校验，不能成为 authoritative commit time。A1 支持矩阵先冻结 Redis 7.2.x（最终版本由部署环境 owner 在实现授权前确认）。

Miniredis Gate：A1 第一项测试必须证明目标 miniredis 版本在 EVAL 中支持 `TIME` 且返回受控 server time。若不支持，A1 状态必须为 `BLOCKED_MINIREDIS_TIME_SEMANTICS`；不得改用 ARGV `now_ms` 冒充生产线性化时间。可增加纯 backend clock fake 测 facade，但不能把它当 Lua authoritative-time 证据。

ScalePlan repository 与 control fence 应使用同一 Redis deployment。若 Redis Cluster 不同节点执行 `TIME`，A0.5/A3 必须冻结最大时钟偏差（建议 250ms）和至少 5s safety margin；无法证明偏差时 proof fail closed。

## 6.8 原子 Lua operations

### CreateOrGet

单一脚本原子完成：

```text
validate lease/fence/time
validate meta/index/accounting exact state
validate idempotency record absent/same payload/conflict
validate active workload absent/same plan/busy
validate candidate initial Plan
validate complete bounded cleanup manifest and every cleanup record/reference
-- only now may the first mutation occur
apply bounded terminal cleanup
write Plan
write idempotency
write active workload
add recoverable
update accounting/meta
return CREATED / EXISTING_SAME_PAYLOAD / IDEMPOTENCY_CONFLICT / WORKLOAD_BUSY
```

### CompareAndSwap

```text
validate lease/fence/time
load exact current record
validate current checksum/Plan
validate expected revision and candidate revision+1
reject OPERATOR_SAFE_TERMINATION/Receipt/Proof
validate state-machine update
replace Plan/accounting atomically
update recoverable or terminal index
if ordinary terminal: release active workload atomically
```

普通成功/失败 terminal 也必须与 active index release 原子化。

### Sealed Blocked Termination

一次脚本完成：

```text
validate opaque core seal in Go before Redis call
validate distributed lease/fence
Redis TIME at linearization
load exact BLOCKED plan
validate Plan/revision/reason/payload/checkpoint
validate proof digest/capture/expiry/evidence retention
validate candidate receipt/proof/terminal timestamp
write terminal Plan + Receipt + full Proof
remove recoverable
add terminal retention score
remove exact active workload ownership
update accounting/meta
return COMMITTED / ALREADY_COMMITTED / REVISION_CONFLICT / PROOF_EXPIRED / CORRUPT
```

绝对禁止拆成“写 terminal”与“删 active”两个 Redis 操作。

### Load

Load 脚本同时验证 Plan record 与所有必须存在的 reference：

- nonterminal 必须在 recoverable；
- terminal 必须在 terminal；
- active nonterminal 必须被 exact workload active record 指向；
- terminal 不得被 active record 指向；
- checksum、accounting、score/retention 一致。

合法 absent 返回 NotFound；dangling reference 返回 Corrupt。

## 6.9 Apply-then-unknown

所有 mutation 接受 deterministic operation identity：

```text
Create: idempotency key + payload digest
CAS: planID + expected revision + candidate checksum
Blocked terminal: TerminationID + expected checkpoint + proof digest
Lease: owner ControllerID/BootID/nonce/fence
```

response loss 后只允许 exact Load/replay：

- exact candidate 已存在 -> `ALREADY_COMMITTED`；
- current 仍是 expected -> 可按同 operation identity 重试；
- revision/identity 不同 -> conflict；
- read 不完整 -> outcome unknown，保留 ownership。

## 6.10 Corruption 与零写入

所有 Lua 按上述 operation validation scope，先验证全部 touched keys/manifest，再执行首个 mutation；cleanup 也属于 mutation，必须排在所有 validation 后。负向测试至少覆盖：

- wrong key type；
- extra/missing hash field；
- malformed canonical integer；
- invalid UTF-8/identity bytes；
- checksum mismatch；
- PlanID/payload mismatch；
- dangling idempotency/active/recoverable/terminal/accounting；
- duplicate index member/reference；
- over max items/bytes；
- mixed schema；
- stale lease/fence；
- terminal proof corruption；
- corrupt latch present（每个 mutation 脚本都必须零写入返回 `REALM_CORRUPT_LATCHED`）；
- latch 写入自身的 epoch-CAS 竞态（两个检测者并发写 latch，只有一个提交，另一个观察到 already-latched 而不是覆盖）；
- **无有效 mutation lease 者一律不得置位 latch**（负向用例；CF-3 删除通道 B 后主语可以写宽）：lease 已过期、fence 落后、或根本不持 lease 的一方提交 latch 写，必须零写入被拒，realm 保持可用。round-4 到 round-6 期间本用例的主语被**故意收窄**为"持 stale fence 的旧 Controller"，因为当时通道 B 允许合法审计者不持 lease 置位，写宽会把 fail-open 固化成测试断言。通道 B 删除后该顾虑消失，但反过来必须记住：**任何将来重新引入非 lease 置位通道的改动，都会让这条用例变红**，这正是它作为护栏的意义；
- **只读 `AuditPage` 在 touched-key 之外发现 corruption 时只能报告，不能阻断**（正向用例，CF-3 后语义反转）：审计者发现损坏后返回报告，latch **保持 absent**，后续 mutation 继续被允许。这条用例断言的是一个**已知缺口**而非一个期望属性——它存在的意义是让缺口在测试里可见，而不是让它藏在文档某一段里。缺口的完整陈述见 §6.4 与 §4.4；
- audit 期间 store epoch 漂移（必须返回 conflict，不得报告 clean）；
- §6.13 的域上限边界。**round-3 P2-2 更正**：前一版本写作"上限+1 语法拒绝"，这是错的——`9007199254740992` 是 16 位、无前导零的 canonical decimal，它先通过语法检查，再被**数值**检查拒绝。正确的三条边界是"上限-1 成功、上限 fail closed `<FIELD>_EXHAUSTED`、上限+1 通过语法但被数值上限拒绝"，另需一条真正的语法负例（前导零、非数字、超长）。

每个测试在失败前后比较所有 exact keys，证明零写入。

## 6.11 Schema migration

当前没有 production ScalePlan durable data，首个生产 schema 为 v1：

```text
legacy_production_scaleplan_schema=NONE
new_schema=scale-plan-record-v1
```

未知 schema 一律 fail closed。未来 v2 必须单独设计：

1. bounded index discovery（禁止 SCAN/KEYS）；
2. 读 v1、完整 validate；
3. canonical encode v2；
4. 单 record + indexes 原子迁移；
5. mixed-state reader 在明确窗口内双读；
6. corruption 零写入；
7. migration completion marker；
8. 独立 review。

A1 不预埋“任意 JSON 向后兼容”。

## 6.12 ACL

独立 Redis user 仅允许 ScalePlan hash tag：

```text
~{classic-farm:<environment>:<zone>:scale-plan:v1:<realm_sha256>}:*
```

最小命令集按脚本实际调用冻结，候选：

```text
+EVAL +EVALSHA +PING
+TYPE +EXISTS +DEL
+GET +SET
+HGET +HMGET +HSET +HDEL +HEXISTS +HLEN +HSTRLEN
+ZADD +ZREM +ZSCORE +ZCARD +ZRANGE
+TIME
```

不得授予：

```text
FLUSHALL FLUSHDB SCRIPT FLUSH
SCAN KEYS
CONFIG MODULE DEBUG SHUTDOWN MIGRATE RESTORE
@all @admin @dangerous
```

Go store 不使用 `SCRIPT FLUSH`；允许 EVALSHA miss 后 exact EVAL fallback。最终 ACL 由 A1 脚本命令静态扫描测试生成，未使用命令不授权。

## 6.13 Lua 整数域冻结

Redis 7.x 的 Lua 5.1 只有 double 数值类型，超过 `2^53-1` 的整数无法精确表示、比较与格式化，`tostring` 还会以 `%.14g` 破坏数值。因此**所有在 Lua 内比较或自增的整数字段**统一冻结同一个域上限：

```text
MAX_LUA_SAFE_INTEGER = 9007199254740991      // 2^53-1
```

适用字段（穷举，新增字段必须加入本表并同步 Go/codec 边界测试）：

```text
Plan.revision                  ScalePlan 自有 keyspace，本节规则全适用
lease fence counter            同上
store_epoch                    同上
active operation sequence      同上
control fence epoch            同上
```

**`placement exclusion revision` 不适用本节规则（round-3 P2-2 更正）。** 它是 G21 现役 `placement-exclusion-v1` 记录的字段，由现役 Lua 递增，而现役实现用的正是本节禁止的 `tostring(...)`（`internal/storage/redis/routecontrol/holder_scripts.go:1147/1201`）。§8.3 的 owner-scoped 释放要让 A1R 对该字段 +1，而 A1R 受 additive-only 约束、不得改变现役脚本语义——两条约束不能同时满足，因此在此明确表态：**该字段沿用现役实现的格式化与比较方式，不纳入本节规则**。现役做法在安全性上并不更弱，其等长字典序比较（`holder_scripts.go:64-65`、`:125-126`）反而比依赖 `tonumber` 舍入行为更稳。本节规则只约束 ScalePlan 自有 keyspace 内新建的字段。

规则：

- **解析**：先按 canonical decimal digit-string 语法校验（无符号、无前导零、长度 ≤16 位、仅 `0-9`），再 `tonumber`；语法不合法或数值超过上限，零写入返回 `<FIELD>_EXHAUSTED` 或 `<FIELD>_MALFORMED`；
- **格式化**：只使用 `string.format('%d', v)`；禁止 `tostring(v)`、禁止数字与字符串用 `..` 拼接、禁止用 `cjson` 编码这些整数；
- **递增**：`v + 1` 在上限内是精确的；达到上限永久 fail closed，不得 wrap/reset；
- Go 侧与 codec 侧冻结同一上限，边界测试见 §6.10 最后一条（注意上限+1 是**数值**拒绝而非语法拒绝）。

这取代了此前"按 digit carry 做 uint64 字符串算术"的方案。理由：上述字段全部是每次 mutation 才 +1 的单调计数器，任何现实生命周期内都到不了 `2^53`；用一个显式的、低于 uint64 的 domain bound 换掉一整套手写大数算术，消除的实现缺陷类远多于它放弃的取值范围。这是 domain bound，不是 wrap——语义仍然是到顶即永久 fail closed。

若某字段确实需要承载来自外部系统的完整 uint64（例如 Kubernetes resourceVersion），则该字段**不得**参与 Lua 内的算术或大小比较，只能作为 opaque canonical string 做相等性比较。

---

## 7. A2：Kubernetes Workload/Identity Adapter 设计

## 7.1 Owned/Forbidden Write Set

提议 Owned：

```text
internal/platform/kubernetes/scaleplan/**
internal/testkit/kubernetes/scaleplan/**
docs/verification/scaleplan-a2-*
```

禁止修改 ScalePlan state machine、Manager、RoutePlan、composition、deploy。A2 只使用 client-go fake/reactor，不读取真实 KUBECONFIG，不调用 kubectl/kind/k3s。

## 7.2 API 调用合同

第一版支持矩阵冻结：

```text
Kubernetes=v1.36 API semantics
local conformance target=k3s v1.36.2+k3s1
client-go module target=k8s.io/client-go v0.36.x (exact patch pinned in A2 go.mod/go.sum review)
apps/v1 StatefulSet
pod ordinal source=apps.kubernetes.io/pod-index label
fallback=NONE
```

Pod 必须存在 stable `apps.kubernetes.io/pod-index` label，值为 canonical non-negative int32，并同时验证 Pod name 等于 `<statefulset-name>-<ordinal>`；label absent/malformed/name mismatch fail closed，禁止只从名称猜 ordinal。

A2 fake tests不资格化真实 apiserver。进入 A5 前新增 `K8S_SCALE_API_CONFORMANCE` 独立授权 gate，在隔离 Namespace 只验证：`GET/UpdateScale` metadata UID/RV、stale RV 409、generation increment、observedGeneration behavior、Pod index label、pagination/watch RV 和 TokenReview bound token；失败则 A5 BLOCKED，不修改 production workload。

第一版仅支持 namespaced `apps/v1 StatefulSet`。

### Get

```text
GET StatefulSet
LIST all selector-matching Pods with pagination
validate each Pod controller OwnerReference
GET StatefulSet end bracket
```

要求：

- start/end StatefulSet UID、resourceVersion、generation、spec.replicas 一致；
- Pod owner ref 必须 `controller=true`、kind StatefulSet、apiVersion apps/v1、UID exact；
- Pod UID、name、ordinal、HolderID 唯一；
- ordinal 从 canonical StatefulSet Pod identity 解析并与 owner/name exact join，不只做字符串后缀猜测；
- page continue token 和 list resourceVersion 一致；Expired/partial page fail closed；
- canonical ordinal sort；
- delete/recreate（同名不同 UID）返回 conflict；
- readiness 只作为 WorkloadState observation，不作为 shard safety proof。

### Scale expected -> desired

冻结调用形态：

1. exact GET StatefulSet，比较 UID、resourceVersion、expected replicas；
2. `GET /scale` 必须返回 metadata UID == StatefulSet UID、非空 resourceVersion、replicas；缺任一字段即 `UNSUPPORTED_SCALE_METADATA`，不得继续；
3. `UpdateScale` 携带 exact resourceVersion 与 desired replicas；
4. exact GET StatefulSet re-read；
5. 比较 UID 未变、spec.replicas、generation；
6. 后续 Get/Executor 等待 observedGeneration 与 Pod inventory 收敛。

不使用无 precondition 的 merge patch。A5 conformance 若证明目标 v1.36 `/scale` 不保留上述 UID/RV/CAS 语义，状态为 `BLOCKED_KUBERNETES_SCALE_CONTRACT`；JSON Patch 或主资源方案属于 A0 plan amendment + independent review，不能在 A2 临时切换或降低 CAS。

### Unknown outcome

对 timeout、connection reset、watch close、response loss：

- exact re-read same name；
- UID 不同 -> conflict；
- replicas==desired 且 generation/RV 前进 -> adopted committed；
- replicas==expected 且 RV 未发生第三方变化 -> **仅在 operation 仍注册时**可用同一 operation identity、同一 RV 前置条件重试；**不得**据此判定"未提交"并关闭 operation；
- 其他 replicas/RV -> conflict；
- re-read 失败/partial -> outcome unknown。

禁止因客户端错误直接重发 Scale。

重试之所以安全，是因为重试请求与可能仍在途的原请求携带相同的 RV 前置条件与相同的 desired replicas：apiserver 至多让其中一个提交，另一个必然 409，最终状态确定为 desired。关闭 operation 则不同——见下节。

### 未决 scale operation 的取消屏障（关闭 P0-A）

问题：`UpdateScale` 一旦被 apiserver 接受授权，它可能仍排队在 transport、priority-and-fairness 队列或 admission 链中。此时 exact re-read 观察到 `UID/RV/replicas` 全部未变，**不能**证明该请求不会在未来提交。若据此移除 active operation、取得 EXCLUSIVE、terminalize 并释放 workload ownership，这个迟到请求仍可能以当时仍然有效的 resourceVersion 成功，而那时已经没有任何 owner 在观察它。

冻结解法：**resourceVersion 取消屏障写**。recovery 在关闭任何未决 scale operation 之前，必须先提交一次让该 StatefulSet 的 resourceVersion 前进的写，**并 exact 证明它确实前进了**：

```text
1. exact GET StatefulSet -> (UID=U, RV=V0, replicas=R, generation=G)
2. 生成屏障 attempt nonce，先写入 durable operation record（见下"nonce 为什么必须唯一"）
3. 屏障写：以 RV=V0 作为前置条件 patch StatefulSet，仅修改 metadata.annotations：
     scaleplan.farm/barrier-operation = <deterministic operation ID>
     scaleplan.farm/barrier-fence     = <control fence epoch>
     scaleplan.farm/barrier-nonce     = <本次 attempt nonce，每次尝试必不相同>
   不修改 spec，不修改 spec.template（因此 generation 不变、不触发滚动、不影响 Pod）
4. 屏障写返回 200 -> **必须对响应对象 exact 校验三项，按下表分类**。
   校验顺序固定为 (b) -> (a) -> (c)，因为 UID 不符会使另外两项失去意义：

     (b) metadata.uid != U
         -> workload 已被重建，屏障对新对象无意义 -> conflict / BLOCKED

     (a) UID 相符，但 metadata.resourceVersion == V0
         -> 屏障【未】成立（见 F58a）-> 换新 nonce 重试，operation 保持未决

     (c) UID 相符且 RV != V0 -> 屏障已成立，任何以 V0 为前置条件的在途
         UpdateScale 此后必然 409。此时按响应对象的 spec.replicas 分类：
           == R（expected，未提交的那个值）
                       -> scale 确未提交 -> operation 关闭为 NOT_COMMITTED
           == desired  -> scale【已经】提交（见下"为什么必须复核 replicas"）
                       -> 按 adopted committed 分类，不得关闭为 NOT_COMMITTED
           其他值      -> 第三方改写（§4.3 / F43），既非 expected 也非 desired
                       -> 【不得】归入上面任何一类 -> unknown，conflict / BLOCKED

   三项分类【穷尽】：(b) 覆盖 UID 变化，(a) 覆盖 RV 未前进，(c) 的三个子分支
   按 spec.replicas 的取值三分，不存在第四种取值类别。

   但【互斥不是无条件的】（round-6 R6-09）：desired == R 时前两个子分支
   同时命中，结果取决于求值顺序，而两者的处置相反。这不能靠"约定先判
   哪一个"来掩盖，必须在【发出 scale 之前】就排除该输入：
     提交前置条件 desired != R 由 §6.2 的 submission 校验强制，
     desired == R 的 scale operation 根本不允许被注册（no-op scale
     不需要屏障，也不需要 operation record）。
   A2 必须为此写一条断言：构造 desired == R 的 submission，断言它在
   注册 operation 之前就被拒绝，而不是走到步骤 4 才发现歧义。
5. 屏障写返回 409 -> 已有其他写入抢先提交 -> exact re-read 重新分类：
     replicas==desired -> adopted committed
     replicas==R       -> 第三方改了别的字段 -> 重新 GET 取新 V0，回步骤 2 重试
     其他               -> conflict / BLOCKED
6. 屏障写 outcome unknown -> operation 保持未决，不得关闭，不得进入 terminal
```

**为什么必须复核 replicas（round-4 P1）。** 只比 RV 就宣布 `NOT_COMMITTED`，这个结论隐含了一条平台假设：屏障 patch 真的执行了 `metadata.resourceVersion=V0` 前置条件。若该前置条件被强制，200 就意味着提交发生在对象仍处于 V0 时，在途的 `UpdateScale(RV=V0)` 必然尚未提交，结论正确。但若某种 patch 类型不强制前置条件，下面这条序列会让实现宣布 `NOT_COMMITTED` 而 scale 其实**已经**提交：

```text
在途 UpdateScale(RV=V0) 先提交 -> RV 变为 V1、replicas 变为 desired
  -> 屏障 patch 随后成功        -> RV 变为 V2，V2 != V0，(a) 通过
  -> 若只比 RV，判定 NOT_COMMITTED（错误）
  -> §8.3 据此解除 exclusion，把 shard 迁入一个 Kubernetes 正在删除的 Pod
```

这条决定安全性的平台事实登记为 `K8S_ANNOTATION_PATCH_HONORS_RV_PRECONDITION`（§12.7）。但登记不等于安全：屏障写的 200 响应体本来就是完整的 StatefulSet 对象，多比两个字段的成本为零，因此 (b)(c) 两项复核是**无条件**要求，不因该 gate 在 A5 通过而可省略。这与 §8.3 释放前置条件里的 `spec.replicas == expected` 是同一道检查，区别在于 `NOT_COMMITTED` 这个判定本身是在这里铸造的，检查必须前移到铸造点。

**为什么 `spec.replicas` 必须三分而不是二分（round-5 P1）。** round-4 版本把 (c) 写成"`!= expected` 即判定已提交"。这是一个二分，而 §4.3 的威胁模型里 `spec.replicas` 有**三**类取值：`expected`、`desired`、以及第三方改写成的任意第三值（F43 正是为此建模的）。把第三值判成 adopted committed 会让释放路径走 §8.3 的 COMMITTED 行——"不解除 exclusion"——而 victim 此刻仍以同一 boot 存活。按 round-5 收紧后的规则只有 owner plan 能解除，于是这条 exclusion 永久不可解除：与 round-4 P0 同一危害类别，只是换了个门进来。第三值必须归入 unknown，走 fail-closed。

**屏障 operation 自身如何 resolve（round-4 P1）。** 屏障写也要注册 durable active operation（见下"resolution-only operation"），而 EXCLUSIVE 的前置条件是 active set 严格为空，因此屏障 operation 自己必须有终止性的关闭判据。若沿用通则"关闭前先发一次屏障写"，每个屏障 operation 都需要一个新的 resolution-only operation 来解析，active set 永不为空，BLOCKED plan 永远无法安全终止——正是本节要消除的那个落地结果换了个位置发生。冻结如下判据，它只读、不写、必然终止：

每个分支都必须**同时**给出两件事的处置：屏障 operation 自身、以及被它屏障的那个 scale operation。round-4 版本只写了后者，导致第三分支下屏障 operation 既不关闭也不重试，永远留在 active set——恰好在唯一用来证明终止性的那条分支上构成活锁（round-5 P1）。冻结如下，**每一行的第二列都不为空**：

```text
resolve 屏障 operation：exact GET StatefulSet，比较 annotation
  scaleplan.farm/barrier-nonce 与 operation record 中持久化的 nonce

  观察                        | 屏障 operation 的处置      | scale operation 的处置    | ownership
  ---------------------------|---------------------------|--------------------------|----------
  nonce 相等                  | 关闭为 COMMITTED           | 按步骤 4 的 (b)(a)(c) 定性 | 可释放，当且仅当
                              |                           |                          | 步骤 4 未落在
                              |                           |                          | F58d/F58e/F58f
  nonce 不等，RV == 该 attempt| 关闭为 NOT_COMMITTED，并   | 保持未决，换新 nonce 重发  | 保留
    的 V0                     | 【立即注册新的屏障 op】     | 屏障（回步骤 2）          |
  nonce 不等，RV != V0        | 关闭为 SUPERSEDED          | 回步骤 5 重新分类，仍未决  | 保留
  GET 失败 / partial          | 保持未决                   | 保持未决                  | 保留
```

**ownership 列必须逐行给，不能整表给（round-6 R6-04）。** 前一版本在失败矩阵里把"前三行 released"写成整表结论，但第二、三行**都**让 scale operation 留在未决。workload ownership 的释放条件是 active set 为空（§8.6），而 active set 里同时装着屏障 operation 与 scale operation；只关闭前者不足以清空。第二行更强：它在关闭旧屏障 op 的同一步注册了新的屏障 op，active set 根本没有收缩。因此只有第一行、且步骤 4 未落入任何冲突分支时，才可能走到释放。失败矩阵已按 F58c.1～F58c.4 逐行重写。

`SUPERSEDED` 这一行需要单独论证，因为"我们的 nonce 不在对象上、RV 却变了"有两种成因：屏障根本没提交而第三方推进了 RV；或者屏障提交过、随后被第三方把 annotation 覆盖掉。**两种成因不需要区分**，因为屏障要建立的安全属性只有一条——「以 V0 为前置条件的在途请求此后不可能提交」——而它只取决于 `RV != V0`，与是谁推进的无关。这依赖同一对象的 resourceVersion **不会回到曾出现过的值**（etcd modRevision 对单对象严格递增），该性质登记在 §12.7 的 `K8S_RV_BARRIER_SEMANTICS` 之下，且只用到"不复现"而非可比大小，与 §6.13 对 RV 的 opaque 约束不冲突。

**终止性的准确表述（round-5 P1）。** 前一版本写"只读、必然终止"，这句话过强。准确的分解是：

```text
递减度量只在两行上成立，必须分开说（round-6 R6-04 的同源更正）：

  第 1 行 nonce 相等   -> 关闭屏障 op，不产生新 op -> active set 收缩
  第 3 行 nonce≠,RV≠V0 -> 关闭屏障 op，不产生新 op -> active set 收缩
  第 2 行 nonce≠,RV==V0-> 关闭旧屏障 op【同时注册新的】-> active set 不变
  第 4 行 GET 失败      -> 什么都不关闭 -> active set 不变

因此"只读判据 => 必然终止"只覆盖第 1、3 行。
第 2 行的重发循环与第 4 行的失败循环都【不】由递减度量约束，
  二者共用 barrier_resolve_escalation_attempts 这一个有界计数器，
  用尽后同样进入 DEGRADED_BARRIER_UNRESOLVED（非终态）。
真正被保证的是：不存在【无声】的无限循环，
  而不是【一定会结束】。

GET 持续失败时，没有任何过程能终止，这不是本节能解决的问题。
  正确姿态是 fail closed 而非硬造一个终态：
    屏障 operation 保持未决 -> active set 不为空 -> 不得进入 EXCLUSIVE
    -> Plan 保持 BLOCKED，workload ownership 保留（F58）
  连续 barrier_resolve_escalation_attempts 次 GET 失败后，
    进入 operator-visible 的 DEGRADED_BARRIER_UNRESOLVED，
    性质与 §8.3 的 DEGRADED_EXCLUSION_UNRELEASED 完全一致：
    非终态、不改变任何判据、只改变可见性。
    不得因为"重试够多次了"就允许关闭 operation
```

这条判据的基础是屏障效果**幂等且可观测**：nonce 唯一，annotation 落库即证明该次 attempt 已提交，不需要再发任何写。它是 resolution-only 类的**唯一**终止基础；任何"靠再写一次来解析"的实现都不满足上述递减度量，属于 §17 停止条件。失败矩阵见 F58c/F58f。

**nonce 为什么必须唯一（round-3 P0）。** Kubernetes 的 registry 层对"结果对象与当前对象逐字节相同"的更新**不写 etcd、不递增 resourceVersion**，并以 200 返回原对象。若屏障 payload 只由 operation ID 与 fence epoch 构成，二者在同一 gate 周期内都是确定性且可重放的（§6.9/§7.2/§8.6 都要求未决 operation 用同一 deterministic identity 重放），于是存在这条完全合法的序列：屏障(Op1,E) 提交、RV=V1、Op1 关闭、同一 Op1 重新发起 `UpdateScale(RV=V1)`、响应丢失、recovery 再次写屏障(Op1,E) —— 此时对象内容不变、**RV 停在 V1**、apiserver 返回成功；若据"屏障写提交成功"就关闭 operation、取得 EXCLUSIVE、terminalize 并释放 workload ownership，那个仍排队的 `UpdateScale(RV=V1)` 随后成功提交，而此时已无任何 owner 观察它——正是本节要消除的原始故障模式。因此：nonce 每次 attempt 必须不同，且第 4 步的 RV≠V0 校验是**无条件**的，不因 nonce 唯一而可省略（nonce 唯一使 RV 前进成为预期，RV 校验使它成为已证明）。

nonce 必须在屏障写**之前**持久化进 operation record，否则 recovery 无法判断上一次屏障用的是哪个 nonce。崩溃在持久化之前是安全的：生成新 nonce 重发即可，旧屏障即使已提交也只是多推进一次 RV，不改变 spec，无副作用（F58b）。

正确性依据：§7.2 已强制每次 `UpdateScale` 携带 exact resourceVersion；apiserver 对携带 RV 前置条件的更新在 etcd 上执行事务性 modRevision 比较，因此屏障写与在途请求由 apiserver 全序化。**已被 exact 证明前进过 RV** 的屏障写一旦提交，任何以旧 RV 为前置条件的授权请求都不可能再提交。这满足 round-2 finding 要求的"具备同等 API-server-visible atomic stale-request rejection 的机制"，且不引入 admission webhook——不需要 cluster 级 `ValidatingWebhookConfiguration`、TLS 服务与证书轮转，也不会因为 `failurePolicy: Fail` 把每一次扩缩容耦合到 webhook 与 Redis 的可用性上，更不会引入 webhook 自身缓存陈旧这一新的攻击面。

**resourceVersion 只做相等性判断。** Kubernetes 明确 resourceVersion 是 opaque、不保证可比大小（§6.13 已冻结这一点）。上文与 unknown-outcome 分类中一律使用"等于/不等于 V0"，不得写成 `<=` 或 `>`；adopted 判定由 `generation` 的单调性承担，不由 RV 承担。

代价与硬要求：

- RBAC 需要在 `statefulsets` 上增加 `patch`（§7.6）；
- RBAC 无法把 `patch` 限制到字段级，因此**推荐**再加一条 `ValidatingAdmissionPolicy`（apiserver 进程内 CEL，非 webhook，无外部可用性依赖），限制 ScalePlan SA 对 `statefulsets` 的 `patch` 只能修改 `metadata.annotations` 中 `scaleplan.farm/` 前缀的键；
- 若目标集群不支持 `ValidatingAdmissionPolicy`，必须把该风险作为已接受风险显式记入 §4.4。**风险的准确表述不是"可能改 spec"**：`patch` 允许修改 `spec.template.spec.serviceAccountName` 与 `volumes`，即把该工作负载改成以 Namespace 内**任意** ServiceAccount 运行，并触发全量滚动重启。这是一次 Namespace 内的权限提升加可用性事件，必须由安全 owner 按此表述知情接受，而不是按"未改 spec 由 audit 证明"轻描淡写地接受；
- 屏障写本身也是一次 Kubernetes mutation，必须同样注册 durable active operation。它属于下述 **resolution-only operation**。

**resolution-only operation（round-3 finding）。** §8.6 的 Exclusive acquire 序列是「close gate（OPEN→CLOSING）→ 拒绝新 writer → resolve 全部 active operation → active set 为空 → 取得 EXCLUSIVE」。但要 resolve 一个未决 scale operation，按上文必须先提交一次**新的** Kubernetes mutation（屏障写）并为它注册 durable active operation——这一步恰好落在"拒绝新 writer"之后、"active set 为空"之前。若不给出例外，落地时只有两种结果：屏障永远发不出去（未决 operation 永不可关闭，BLOCKED plan 永远无法安全终止），或者实现悄悄允许 CLOSING 期间注册新 writer（fence 被削弱）。因此冻结第三类 operation：

```text
resolution-only operation
  - 只能由当前 gate owner 在 CLOSING 状态下注册，携带 gate 的当前 fence epoch
  - 只能是「解析某个已存在的未决 operation」所必需的写，当前仅 RV 屏障写一种
  - 必须计入 active set，并且必须在进入 EXCLUSIVE 之前 exact resolve
  - 不得创建新的 Plan 级副作用，不得修改 spec
  - 非 gate owner 在 CLOSING 下注册 resolution-only operation 一律拒绝
```

gate 状态机与 F51/F52 按此扩展：CLOSING 下 active set 允许因 resolution-only operation 而短暂增长，但其元素必须全部来自"解析既有未决 operation"，且 EXCLUSIVE 的前置条件仍是 active set 严格为空。

Preflight gate：`K8S_RV_BARRIER_SEMANTICS`、`K8S_NOOP_WRITE_DOES_NOT_ADVANCE_RV` 与 `K8S_ANNOTATION_PATCH_HONORS_RV_PRECONDITION`（§12.7）。A2 必须在 fake 层同时建模正例（唯一 payload 前进 RV、不改 generation）与两条反例（相同 payload 不前进 RV 且返回 200；patch 不强制 RV 前置条件时的提交顺序）；A5 conformance gate 必须在真实 apiserver 上证明"stale RV 的 `UpdateScale` 返回 409"、"带唯一 nonce 的 annotation patch 前进 RV 但不改变 generation"、"同 payload 连写两次第二次 RV 不变"、以及"stale RV 的 annotation patch 返回 409"。任一不成立则 `BLOCKED_KUBERNETES_RV_BARRIER`，未决 operation 永久保持未决，不得 terminalize。

#### §7.2 在 A0 冻结什么、不冻结什么（S4 范围收窄）

与 §8.3 同理：RV 屏障在 round-2、round-3、round-4 连续三轮产生 P0/P1，每一轮的缺陷都是"机制依赖一条未经证实的 apiserver 行为"，而这类事实在文档层只能论证、不能证伪（round-3 P0 是"同内容写不前进 RV"，round-4 P1 是"annotation patch 是否强制 RV 前置条件"）。A0 收窄为**只冻结安全属性与 fail-closed 默认**：

```text
A0 冻结（不可在后续阶段放宽）：
  SP-BAR-1  关闭一个未决 scale operation 为 NOT_COMMITTED，必须先取得
            "此后任何以 V0 为前置条件的写都不可能提交"的【正面证明】
  SP-BAR-2  该证明的成立条件是步骤 4 的 (b)(a)(c) 三项 exact 校验全部通过，
            且 (c) 落在 `spec.replicas == expected` 这一个子分支上；
            其余任何组合都不是 NOT_COMMITTED
  SP-BAR-3  (b)(c) 两项复核无条件执行，不因任何 preflight gate 通过而省略
  SP-BAR-4  屏障 operation 自身只能靠【只读】判据关闭（F58c），
            靠再发一次写来解析的实现不满足终止性；
            F58c 的每一行必须【同时】处置屏障 operation 与 scale operation
  SP-BAR-5  证明不可得时 operation 永久保持未决，不得 terminalize。
            有界重试后只允许升级为 operator-visible 的 degraded 状态，
            不允许因重试次数用尽而关闭 operation（round-5）

A0 不冻结（下放到能被测试证伪的阶段）：
  - patch 类型的选择（strategic-merge / merge / apply）  -> A2 + A5 实测后回填
  - nonce 的生成方式与长度                               -> A2
  - 重试节奏与退避                                       -> A2 + A5
  - 是否存在比 annotation patch 更廉价的 RV 推进手段      -> A5 可提 amendment
```

这里的收窄不是放松：SP-BAR-3 恰恰把一项原本"由 gate 保证"的检查改成了无条件执行，因为**廉价的兜底优于精确的假设**。

### stale Controller 的 Kubernetes 写（与 P0-A 分离的独立威胁）

必须与 P0-A 区分：一个已经丢失 Redis mutation lease、但仍持有有效 Kubernetes token 的旧 Controller，可以读到**新的** RV 再发起 scale 写。RV 屏障对它无效，因为它的请求并不早于屏障。

v1 冻结决定：**接受"仅检测"，不引入 fencing admission**。依据链：

```text
§4.4 单 writer 前提（只有 ScalePlan SA 拥有 /scale 写权限，Namespace 内无 HPA/GitOps reconciler）
+ §8.6 EXCLUSIVE gate（新 writer 无法注册 active operation）
+ F43 第三方 replicas mutation 检测
+ §8.6 Revalidate 中的 Kubernetes UID/RV/replicas 未变检查
+ §6.8 sealed terminal 在 Redis 线性化点再次检查 proof expiry
```

**残留窗口的准确界定（round-3 修正）。** round-2.5 版本把该窗口界定为"final revalidate 与 sealed terminal linearization 之间"，并要求它落在 §8.9 的 proof safety margin（建议 5s）内。这与本节自己的立论矛盾：本节的整个出发点就是"已被授权的请求可能仍排队在 transport / priority-and-fairness / admission 链中，排队时长没有上限"。同一前提套到 stale Controller 上——它完全可以在 final revalidate **之前**读取 RV 并发出请求，而该请求在 terminal 提交、ownership 释放**之后**才落库。此时 F43 的第三方 mutation 检测（依赖 revalidate 时刻的观察）与 sealed terminal 的 proof expiry 检查都已无从触发。因此真实暴露窗口是：

```text
[stale Controller 最后一次可能读取 RV 的时刻,
 其在途请求在 apiserver 侧的最大存活时间]
```

它由「mutation lease 过期被检测到的延迟」加「apiserver 请求排队与执行的最大时长」决定，**与 proof safety margin 无关**，5s margin 不构成对它的任何约束。

因此 v1 的可接受性依赖于以下两条中至少一条，且必须在 A5 evidence 中给出实测值：

```text
(a) 为 ScalePlan SA 的全部 /scale 与 patch 请求设置 apiserver 侧请求超时上界
    （客户端 timeoutSeconds + 服务端 --request-timeout），使在途请求存活时间有上界；
    残留窗口 = lease 过期检测延迟 + 该上界
(b) sealed terminal 之后再做一次延迟 exact re-read（延迟 ≥ (a) 的上界），
    作为 A5 的检测证据；检测到第三方写即判定本次 run 不合格
```

命名 finding：`STALE_CONTROLLER_KUBERNETES_WRITE_DETECTION_ONLY`。A5 必须实测的是上述窗口，不是 proof safety margin。

若安全 owner 不接受该残留窗口，则必须引入按 operation ID + fence epoch 校验每个 `/scale` 请求的 admission 机制。那属于 A0 plan amendment 加独立复审，不得在 A2/A3 阶段临时加入。

## 7.3 client-go fake 的限制与补偿

client-go fake 不自然模拟 API server 的 RV/CAS、generation、subresource、429、watch RV 和 RBAC。A2 测试必须增加自定义 reactor/fake transport：

- monotonic resourceVersion；
- UpdateScale exact RV conflict；
- generation increment；
- independently controlled observedGeneration；
- 429 + Retry-After；
- timeout before commit / apply then timeout；
- NotFound；
- watch close/relist；
- paginated partial list；
- Pod delete/recreate；
- StatefulSet delete/recreate；
- third-party replicas mutation。

Fake PASS 只表示 code-only contract，不声称真实 API server 已资格化。

## 7.4 Victim selection ownership

A2 返回完整 canonical Pod inventory；最高 ordinal victim 的选择仍由 ScalePlan Core 完成。Adapter 不实现第二套 victim policy。

Scale-in mutation必须证明 Core 冻结的 victim 是当前最高 ordinal，且 scale-down 后 desired replicas 会删除该 ordinal；不直接 DELETE Pod。

## 7.5 Trusted Kubernetes identity snapshot

A2 通过 A0.5 seam 输出：

```text
StatefulSet namespace/name/UID/RV/generation/observedGeneration/replicas
Pod name/UID/ordinal/ready/owner UID
list resourceVersion + capture checksum
```

A3 再与 Holder Registry 独立读取并 exact join：

```text
PodUID
WorkloadUID (= StatefulSet UID)
Ordinal
HolderID
HolderBootID
PodName
Logic/HolderControl/TLS endpoints
```

Registry claims 不得反向证明 Kubernetes UID。任何 owner ref 缺失、多 controller owner、UID drift、duplicate ordinal 或 endpoint mismatch 都 fail closed。

### Pod-bound process trust anchor

仅比较自报 PodUID/WorkloadUID 不构成可信绑定。A2/A3 冻结使用 **kubelet 投射的 Pod-bound ServiceAccount token + TokenReview + mTLS session**：

```text
每个 StatefulSet 一个共享的最小权限 workload ServiceAccount（不是 per-Pod SA）
Pod template 挂载 projected serviceAccountToken volume：
  audience          = classic-farm-holder-registration
  expirationSeconds = 600
  由 kubelet 签发，token 天然绑定到本 Pod（bound object = 该 Pod）
```

为什么不是 per-Pod ServiceAccount：普通 StatefulSet 的 Pod template 只有一个 `serviceAccountName`，无法为每个 ordinal 指定不同 SA；也不能假设每个 SA 默认有权为自己调用 TokenRequest API。共享 SA 是可接受的，因为可信区分并不来自 SA 身份，而来自 token 里绑定的 Pod UID 与一次性 BootID challenge。

流程：

1. Holder 从投射卷读取 token；**每次使用前重读文件**，不得缓存首次读取值（kubelet 在约 80% 生命周期处原地轮换该文件）；
2. HolderRegistry register/heartbeat transport 将 token 作为敏感 credential，仅在内存中传递，不写 durable record/log/evidence；
3. Route-controller 通过 Kubernetes TokenReview 验证 audience、namespace、ServiceAccount，并从 TokenReview 响应的 `status.user.extra` 取 `authentication.kubernetes.io/pod-uid` 与 `pod-name`；
4. 以该 pod-uid 独立 GET Pod/StatefulSet owner chain 做 exact join，不接受请求方自报的 PodUID；
5. verified principal 绑定 mTLS connection identity、PodUID、StatefulSet UID、ordinal、HolderID naming policy 与一次性 BootID registration challenge；
6. heartbeat 只接受同一 mTLS principal/PodUID/BootID；
7. Registry durable `DeploymentIdentity` 仍是 claim，但 A3 只在同时存在 fresh `VerifiedPodPrincipal` 时采用；
8. TokenReview/K8s API unavailable、credential replay 到其他 Pod、principal/Pod/Registry mismatch 均 fail closed。

`expirationSeconds` 只能取 `600`：Kubernetes 对 projected serviceAccountToken 的该字段有下限校验（不得小于 10 分钟），因此 round-2 之前写的 `<=600` 实际上只有 600 一个合法取值。

Holder 不调用 TokenRequest API，不需要 per-Pod SA，也不授予 tokenreviews 权限。A5 RBAC 只需额外允许 route-controller 对 tokenreviews `create`（cluster-scoped 最小 verb）。

Preflight gate：`K8S_BOUND_TOKEN_POD_IDENTITY`（§12.7）。必须在 A5 conformance gate 上证明目标 v1.36 的 TokenReview 响应确实携带 `pod-uid` / `pod-name` extra；不成立则 `BLOCKED_BOUND_TOKEN_POD_IDENTITY`。此时只能选择并专项复审 SPIFFE/SPIRE 或 admission-issued Pod certificate，不得退回 self-claim join。

Endpoint 采用 deterministic derivation，不信任 Registry 任意字符串：StatefulSet Pod DNS name + frozen service/ports 生成 expected Logic/HolderControl endpoint，Registry 与 RouteSnapshot 必须等于 derivation；TLS DNS identity 必须由 verified certificate SAN 覆盖。

### Domain-to-workload ownership

第一版 production manual ScalePlan 冻结每个 `(realm, route domain)` 恰好一个 configured StatefulSet UID；配置记录 namespace/name/UID/domain 并纳入 config hash。Complete placement proof 是 domain-global，要求每个 active placement holder都 join到该唯一 StatefulSet 的 contiguous Pod inventory。若同一 domain 需要多 StatefulSet，必须 protocol vNext 引入 workload partition key；A1～A5 v1 不支持。

## 7.6 最小 RBAC

A5 候选 namespaced Role（业务对象）仅限独立 Namespace：

```yaml
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "watch", "patch"]      # patch 仅用于 §7.2 的 RV 取消屏障写
- apiGroups: ["apps"]
  resources: ["statefulsets/scale"]
  verbs: ["get", "update"]
```

`statefulsets: patch` 是为 RV 取消屏障写新增的最小必要权限，取代了 admission webhook 方案。RBAC 无法做字段级限制，因此推荐同时部署一条 `ValidatingAdmissionPolicy`（apiserver 进程内 CEL，无外部服务与可用性依赖），限制该 SA 的 `statefulsets` patch 只能修改 `metadata.annotations` 中 `scaleplan.farm/` 前缀的键；集群不支持时按 §7.2 记为已接受风险并由 audit 证明从未修改 spec。

若最终实现的 scale 写使用 patch，则只将 `patch` 加到 `statefulsets/scale`；不同时保留未使用的 update/patch。禁止 cluster-admin、Secret 访问、其他 Namespace、任意 Pod delete、route-controller 的其他 workload 修改权限。

另需单独最小 ClusterRole 仅授予 `authentication.k8s.io/tokenreviews` 的 `create` 给 route-controller SA，用于 Pod-bound token 验证；不得授予其他 authentication/authorization API。Holder 的共享 workload SA 无任何 API 权限——它的 token 由 kubelet 通过投射卷签发，不经过 TokenRequest API，因此不需要也不得授予 Pod/StatefulSet/tokenreviews 权限。

A4 `off` 模式不创建 Kubernetes client，因此也不读取 token 或发起 API discovery。

---

## 8. A3：Control/RoutePlan/Quiescence Adapter 设计

## 8.1 Owned/Forbidden Write Set

共享语义改动**全部在 A0.5 串行完成**；A3 不得再次修改它们。

A0.5 exact Owned Write Set（round-3 P2-4 补齐三个文件；round-4 补齐四处，并把 `internal/actorrouting/authority/` 由具名文件改为通配——这是唯一一处用具名文件而其余同类目录都用通配的地方，也正是漏掉 `placement_permit_test.go` 的原因）：

```text
internal/scaleplan/repository.go
internal/scaleplan/blocked_termination.go
internal/scaleplan/executor.go
internal/scaleplan/ports.go
internal/scaleplan/memory_repository.go              # C01/C02：ScalePlanRepository 与
                                                     #   blockedTerminationRepository 的【唯一】
                                                     #   实现者，接缝一改必改（round-5 P1）
internal/scaleplan/*_test.go
internal/routeplan/executor.go
internal/routeplan/model.go
internal/routeplan/store.go
internal/routeplan/service.go                        # C07：CF-1 实测补入（第十三处缺口）。
                                                     #   持有 RouteSnapshotStore 并调用其方法，
                                                     #   fence 形参一加即断
internal/routeplan/*_test.go
internal/actorrouting/authority/manager.go
internal/actorrouting/authority/*_test.go            # round-4 P2：含 placement_permit_test.go
                                                     #   与 lease_projection_test.go，
                                                     #   前者是 Controller 的第五个实现者
internal/routecontrol/durable_publisher.go
internal/routecontrol/durable_publisher_test.go
internal/routecontrol/snapshot_store.go              # C07：RouteSnapshotBootstrapper 接口本体
internal/routecontrol/placement_exclusion.go         # C09：exclusion 接口
internal/routecontrol/holder_registry.go             # C09：生产实现者（round-3 P1-1）
internal/routecontrol/watch_hub.go                   # C07：CF-1 实测补入（第十二处缺口）。
                                                     #   同样持有 RouteSnapshotStore；此前本清单
                                                     #   在该目录用具名文件列了四个，漏了它
internal/routecontrol/*_test.go
internal/routecontrol/transport/*_test.go            # C07：handshakeRaceStore 按方法集实现
                                                     #   RouteSnapshotStore，且上一行的通配
                                                     #   【不递归】进子目录（round-5 P1）
internal/testkit/capstaticroute/*.go                 # C09/C14：改用通配（round-6 R6-01）。
internal/testkit/capstaticroute/*_test.go            #   此前具名列了 placement_permit.go 与
                                                     #   initializer_test.go，漏掉同目录的
                                                     #   liveness_integration_test.go:27（第七条编译边）。
                                                     #   被断言【不修改】的 initializer.go 仍在 §11.4，
                                                     #   落在写集内不等于必须改
cmd/cap-redis/*_test.go                              # C14：同上，此前断言无处可写（round-4 P1）
cmd/route-controller/plan_runtime.go
cmd/route-controller/plan_runtime_test.go
internal/testkit/scaleplan/**
docs/verification/scaleplan-a0-5-*

# 以下两条由 CF-1 不动点实测补入并经核准（写集修订 W1，见本节末）：
internal/controlfence/**                             # token 的新家，叶子包，无内部依赖
internal/routecontrol/gateverification/verification_test.go  # C18 的 ctx 边

# 以下文件 A0.5 只持有 conformance 编辑权（见下节 S5），
# 权利在 seam commit 处终止，之后归各自的正常 owner：
internal/storage/redis/routecontrol/snapshot_store.go     # 边 1  -> A1R
cmd/cap-static-route-init/main.go                         # 边 2  -> A1R
internal/storage/redis/routecontrol/holder_store.go       # 边 3  -> A1R
internal/storage/redis/routecontrol/transition_store.go   # 边 4  -> A1R
cmd/routectl/main_test.go                                 # 边 5  -> A4
internal/storage/redis/routecontrol/routecontrol_test.go  # 边 6  -> A1R
internal/storage/redis/routecontrol/holder_store_test.go  # 边 6b -> A1R

# W1 核准：以下五条【只持 conformance 编辑权】，不计入 owned。
# 全部同源于 authority.SnapshotPublisher 这条从未被枚举的接口边。
internal/actorrouting/provider.go                    # §6.1 中属 A1 禁止写集；
                                                     #   禁令意图同样约束 A0.5，
                                                     #   故降为只改签名 -> 原 owner
internal/actorrouting/provider_test.go               # 同上目录
internal/actorrouting/redirect_test.go               # 同上目录
internal/authoritycomposition/composition_test.go    # 六轮复审中从未出现
internal/authoritycomposition/endpoint_pool_test.go  # 同上
internal/authoritycomposition/runtime_membership_test.go  # 同上
```

上一版把边 5、边 6 写进了 S5 的表却**没有**同步到本清单，CF-1 实测把它们连同边 6b 一起找了出来。这正是本文档反复出现的那类缺陷（表改了、清单没改），也说明"先验表"与"写集"必须由同一次机械动作产出，而不是人工誊抄。

**通配符不递归**是本清单的一条硬约定（round-5 P1 的成因）：`internal/routecontrol/*_test.go` 不覆盖 `internal/routecontrol/transport/`。任何新增目录都必须显式列出，不得指望上层通配兜底。

### 写集修订 W1（CF-1 不动点实测的产物，**需与 §6.1 一并核准**）

CF-1 跑到不动点后，真集合有八条落在原写集之外。它们不是"又漏了几个文件"，而是一条接口边的完整代价，其中一条直接撞上另一个阶段的禁令：

| 条目 | 来源 | 与既有条款的冲突 |
|---|---|---|
| `internal/controlfence/**` | token 的新家（用户已裁定） | 新包，无冲突 |
| `internal/actorrouting/provider.go` 及同目录两个测试 | `SnapshotPublisher` 边 | **`internal/actorrouting/**` 在 §6.1 属 A1 禁止写集** |
| `internal/authoritycomposition/` 三个测试 | 同上 | 六轮复审中从未出现，无既有归属 |
| `gateverification/verification_test.go` | C18 的 ctx 边 | 无既有归属 |

`internal/actorrouting/**` 这一条需要显式处理：§6.1 禁止的是 **A1**，而现在要动它的是 **A0.5**，二者不是同一个阶段，所以字面上不冲突；但禁令的**意图**是"数据面的路由核心不得被 adapter 阶段改动"，A0.5 同样落在这个意图内。冻结的处置是**把它按 S5 的 conformance 编辑权对待而非 owned**：A0.5 只允许改签名、不允许改任何行为，权利在 seam commit 处终止。实测的 diff 已符合这一约束——`provider.go` 的两处改动都只是加一个被忽略的形参。

`internal/authoritycomposition/` 三个测试同理，只有 token 实参的机械插入，无行为改动。

### 跨阶段编译依赖与 conformance 编辑权（round-5 P1，S5）

round-4 版本只发现了 C18 一处跨阶段编译依赖，并用一句"两半必须在同一次集成中落地"打发过去。这个说法**在本文档自己的规则下不可执行**，而且远不止一处。

先说不可执行。三条规则构成一个环：

```text
(1) §12.2 的 A0.5 gate 要求 production_seam_compiles=true
      -> 被复审的那个 A0.5 commit 必须能编译
(2) 能编译要求 A1R 侧的实现同步满足新接口
(3) §2 要求 A1R 在【reviewed】A0.5 之后才能开始，
      §11.6 要求"发现 write set 重叠立即停止"
      -> A0.5 的 Writer 不得碰 A1R 的文件

任取一条出路都违反另外两条中的至少一条。
```

再说范围。**枚举单位必须是编译单元，不是文件（round-6 R6-01）。** 上一版按"非测试文件 + 接口断言 + 直接调用点"手工枚举出四条边，实测漏了三条，全部来自 `_test.go`——Go 把 `package p` 与它的 `_test.go` 编成同一个测试编译单元，`go build ./...` 看不见它们，`go test ./...` 一定会红。而 §12.2 的 A0.5 gate 要求 `go test ./...` 通过，所以这些边和非测试边同样是硬的。

已知七条（前四条来自上一版，后三条是本轮实测补入）：

| # | A0.5 侧改动 | 被打断的编译单元 | 断言/调用点 | 归属 |
|---|---|---|---|---|
| 1 | C07/C08：`RouteSnapshotStore`/`RouteSnapshotBootstrapper` 加 fence 形参 | `internal/storage/redis/routecontrol/snapshot_store.go` | `:30`、`:31` 两条 `var _ core.…` | A1R |
| 2 | C07/C08 同上 | `cmd/cap-static-route-init/main.go` | `:245` `NewSnapshotStore`、`:269` `CompareAndSwap`、`:271` `Bootstrap` | C17 / A1R |
| 3 | C09/C10：exclusion 接口加 `ReleasePlacementExclusionForOwner` | `internal/storage/redis/routecontrol/holder_store.go` | `:275` `var _ core.PlacementExclusionStore` | A1R |
| 4 | C18：两个 transition store 接口加 `ctx` | `internal/storage/redis/routecontrol/transition_store.go` | `:38`/`:39`/`:40` 三条 `var _ authority.…` | A1R |
| 5 | C07/C08 同上 | `cmd/routectl/main_test.go` | `:342` `snapshotStore.Bootstrap` | **A4**，不在 A0.5 conformance 权内 |
| 6 | C07/C08 同上 | `internal/storage/redis/routecontrol/routecontrol_test.go` | `:156`/`:217`/`:299`/`:303`/`:313`/`:344` 六处 `Bootstrap` | **A1R**，在 conformance 权内 |
| 7 | C07/C08 同上 | `internal/testkit/capstaticroute/liveness_integration_test.go` | `:27` `durableStore.Bootstrap` | **无人拥有**（见下） |

第 7 条是第十一处写集缺口，成因值得记下来：本轮刚把"通配符不递归"写成硬约定，同一次修订却在 `internal/testkit/capstaticroute/` 下用**具名文件**列了 `placement_permit.go` 与 `initializer_test.go`，于是隔壁的 `liveness_integration_test.go` 再次漏网。处置：该目录改用 `internal/testkit/capstaticroute/*.go` + `*_test.go` 通配，纳入 C09 的 A0.5 写集。第 5 条跨到 A4，conformance 编辑权必须相应扩展到 `cmd/routectl/main_test.go`。

**枚举方法本身改为机械的。** 手工枚举已经连续两轮被证明不可靠，不再作为依据：

**该方法本身已被执行过一次（CF-1 预跑），并因此被修正了两处。** 下面写的是修正后的版本；两处缺陷记在末尾，因为它们决定了为什么方法要长成这样。

```text
A0.5 的【第一个动作】，先于任何设计工作：

  1. 只做接口签名与方法集改动（C07/C08/C09/C10/C18），不写任何实现
  2. 循环，直到全绿：
       go build -buildvcs=false ./...
       go vet   -buildvcs=false ./...
       go test  -buildvcs=false ./... -run <不匹配任何用例的名字>
       收集本轮报错 -> 记入【break set】
       在【报错所指类型的声明处】补 fail-closed 哨兵方法或形参
         -> 记入【edit set】
       回到循环开头
  3. 两个集合都要留档，且【不是同一个集合】
  4. 与七行先验表比对，差异逐条给出归属

必须一次到不动点，不能只看第一轮。
```

两个集合的区别是这次预跑最重要的产出：

```text
break set = 编译器报错的位置 = 【使用点】
  例：initializer.go:59 把 *allowAllPlacementExclusions 赋给接口
  这些包会让 A0.5 gate 变红，但它们【不需要被修改】

edit set = 修复落点 = 【类型声明处】
  例：placement_permit.go 里给该类型补上方法
  这才是要进 Owned Write Set 的集合

两者互不包含：
  initializer.go 在 break set、不在 edit set
    （它还是 §11.4 的"显式不修改"条目，把它塞进写集是错的）
  placement_exclusion.go 在 edit set、不在 break set
    （改接口的那一刻它自己是对的）
```

**缺陷一：级联遮蔽。** 预跑时只改了 C07/C08 的 fence 形参，`go build ./...` 只报出 `internal/routecontrol` 一个包；直接构建已知下游 `internal/storage/redis/routecontrol` 得到的仍是上游的错误——**它根本没被类型检查**。一个包编译失败会把它整个依赖锥藏起来。上一版写的"收集全部编译错误的包路径去重集合 = 真集合"因此是错的，必须迭代到不动点。C09/C10 实测两轮收敛，第一轮 5 个文件、第二轮 0 个。

**缺陷二：`-buildvcs`。** 不加 `-buildvcs=false` 时，`cmd/**` 下全部 `.test` 合成包会以 VCS stamping 失败告终（本 worktree 的 git 状态触发 `exit status 128`），120 个包直接不被类型检查。预跑时正是这一项让 `internal/testkit/capstaticroute/placement_permit.go` 与 `internal/routeplan/placement_permit_test.go` 在第一次测量中完全消失。

上一版在此处还写着"若出现**第五条**编译边即属 §17 停止条件"。这条停止条件**撤销**：它把"手工枚举不完整"错当成了"设计有漏"。替代物是上述第 4 步的比对留档——出现表外的包不是停止条件，**不做这次比对**才是。

### CF-1 正式执行的结果（A0.5 第一个动作，**十七轮迭代跑到不动点**）

预跑之后，A0.5 获授权并按上述修正版流程正式执行了 CF-1。第八轮撞上停止条件（token 无处安放，见发现一），回报后由用户裁定新建叶子包 `internal/controlfence`，随后继续跑到**不动点**。

**终态（第 17 轮）：`go build` 全绿、`go vet` 全绿、160 个测试单元全部编译通过、0 个 build failed、gofmt 干净。** 基线为 159 个测试单元，新增的 1 个即 `internal/controlfence`。

```text
真集合（edit set）      34 个文件 / 11 个包
  §8.1 owned 写集内     19
  conformance 编辑权内    8
  两者之外                7      <- 需要写集修订，见下
  新增包                  1      internal/controlfence
```

十七轮的 break set 逐轮为 1 / 5 / 6 / 2 / 10 / 8 / 2 / 8 / 13 / 3 / 2 / 5 / 4 / 2 / 1 / 1 / 0 个文件。**它完全不单调**：第 4 轮降到 2、第 5 轮弹到 10，第 8 轮降到 2、第 9 轮又弹到 13。原因是每修好一个包，编译器才开始类型检查它的依赖锥。这从正面印证了"缺陷一：级联遮蔽"——**只看第一轮会把真集合低估成 1/34**，而上一版文档的做法正是只看第一轮。

**七个越界文件，六个同一根因。** 全部来自发现一那条从未被枚举的 publisher 边：

```text
internal/actorrouting/provider.go                        A1 明令禁止写集
internal/actorrouting/provider_test.go                   同上
internal/actorrouting/redirect_test.go                   同上
internal/authoritycomposition/composition_test.go        从未出现在任何枚举中
internal/authoritycomposition/endpoint_pool_test.go      同上
internal/authoritycomposition/runtime_membership_test.go 同上
internal/routecontrol/gateverification/verification_test.go   C18 的 ctx 边
```

`internal/actorrouting/**` 在 §6.1 里是 **A1 的禁止写集**。一条漏枚举的接口边，把 A0.5 直接推到了另一个阶段的禁区门口——这是"漏一条边"的真实代价，不是"多改三个文件"。

**发现六（本轮新增）：按方法名打补丁分不开两个同名接口。** `CompareAndSwap` 同时属于 snapshot store 和 RoutePlan store；`Publish` 同时属于四个不相关的 func 类型（`AuthorityReaderFunc`、`PendingProjectionTargetValidator`、`PendingDrainControllerFactory`、局部 helper `newOptions`）。本轮的批量正则先后误伤了这五处，每次都由类型检查器抓回来。这对 §11.1 的审计方法是直接的反证：**那套 `rg` 命令按名字枚举实现者与调用点，它分不开的东西和我的正则分不开的是同一批。**

**发现一（阻断，新）：`authority.SnapshotPublisher` 是从未被枚举过的第四条接口。** 七行先验表只列了 C07/C08、C09/C10、C18 三组。实测显示 `DurableSnapshotPublisher.Publish` 的签名一改，`internal/actorrouting/authority/manager.go:57` 的 `SnapshotPublisher` 接口即不再被满足——它是**另一个包里的独立接口**，靠结构一致性而非显式断言绑定，任何按接口名检索都看不到这条边。它的实现者之一 `internal/actorrouting/provider.go` 的 `DynamicAssignmentPublisher` **不在任何阶段的任何写集内**（`initializer.go:60` 把它当 `SnapshotPublisher` 用，实测报错）。

更硬的是：给它加 token 会成环，这一点由编译器直接判定，不是推测：

```text
package farmgame/internal/actorrouting
	imports farmgame/internal/routecontrol from provider.go
	imports farmgame/internal/actorrouting from durable_publisher.go: import cycle not allowed
```

结论：**`NestedControlOperationToken` 不能住在 `internal/routecontrol`。** §8.1 S5 此前只为**哨兵**预留了"若成环改放叶子包"的应急条款，从未考虑过 **token 本身**有同样的约束，而 token 出现在每一条被改的签名里，约束比哨兵强得多。

**已裁定（用户，2026-08-13）：新建叶子包 `internal/controlfence`。** 它只声明 token，无任何内部依赖，两层各自 import 它、谁也不 import 谁：

```text
        internal/controlfence          无内部依赖的叶子
           ▲              ▲
           │              │
   routecontrol ────> actorrouting
```

`internal/routecontrol` 保留 `type NestedControlOperationToken = controlfence.NestedControlOperationToken` **类型别名**（不是新类型），因此已按 `routecontrol.` 限定符写好的调用点无需返工，两个名字指向同一个类型。此裁定的理由是分层：token 是纯控制面概念，塞进数据面的 `actorrouting` 会让后来人反复追问它为什么在那里；多一个只有一个文件的包，比这个便宜。

被否决的第三个选项是"快照发布不加 token"——快照发布正是两个 Controller 最容易撞车的位置，§8.6 把它列为 fence 检查点有其理由，为省两行而摘掉它等于自拆六轮复审要守的东西。

**发现二（新）：C18 的十个调用点里有七个作用域内根本没有 context。** 分别是构造函数 `NewRouteAssignmentManager` 与五个 `*Locked` 辅助方法（`expireMemoLocked`、`transitionTombstoneLocked`、`reserveRecoveryLocked`、`commitRecoveryLocked`、`markAuthorityAttemptedLocked`、`deleteRecoveryLocked`）。只有 `RecoverPendingProjections` 有真 ctx。给构造函数加 ctx 会波及 **41 个调用点、6 个文件、4 个包**，其中包含 `internal/testkit/capstaticroute/initializer.go:59`——§11.4 冻结为"显式不修改"的条目。CF-1 不替这件事做决定：本轮用具名的 `transitionStoreStartupContext()` 占位（8 处，可 grep），归属留给 A1R。

**发现三（推翻上一轮预跑）：`watch_hub.go` 与 `routeplan/service.go` 不是编译边。** 预跑的类型探针把它们报成"第十二、十三处写集缺口"并据此补进了 §8.1。八轮实测中它们**一次都没进过 break set**：二者只是把 `RouteSnapshotStore` 当**字段类型持有**，从不调用被改的方法，持有接口类型不产生编译边。这是类型探针相对编译器的**假阳性**，也说明"机械测量"里"机械"两字必须落到编译器上——探针也是一种枚举。两条写集条目本身无害（在写集内不等于必须改），但那两条 finding 应当撤销。

**发现四（实证确认 §11.4）：`initializer.go` 只在 break set、不在 edit set。** 第 5 轮它因 `allowAllPlacementExclusions` 缺方法而报错，第 6 轮补完 `placement_permit.go` 的声明后它自动出局，全程未被修改。§11.4 的"显式不修改"断言成立。

**发现五：`MemoryRouteSnapshotStore` 没有编译期接口断言**，所以它在自己的声明处不报错，只在被赋给接口的地方报（`transport_test.go`、`review_test.go`）。这正是 break set 与 edit set 互不包含的机制本身。A0.5 已为它补上 `var _ RouteSnapshotStore` / `var _ RouteSnapshotBootstrapper` 两条断言，让后续任何签名漂移在声明处即刻暴露。

**与七行先验表的比对（gate `compile_edge_set_measured_not_enumerated` 要求的留档）：**

| 先验表 | 实测判定 |
|---|---|
| 边 1 `redis/snapshot_store.go` | 成立，conformance 编辑已落 |
| 边 2 `cmd/cap-static-route-init/main.go` | **未达**，迭代在到达前中止 |
| 边 3 `redis/holder_store.go` | 成立，conformance 编辑已落 |
| 边 4 `redis/transition_store.go` | 成立，conformance 编辑已落 |
| 边 5 `cmd/routectl/main_test.go` | 成立 |
| 边 6 `redis/routecontrol_test.go` | 成立（第 8 轮进入 break set），**未达** edit |
| 边 6b `redis/holder_store_test.go` | **未达** |
| 边 7 `capstaticroute/liveness_integration_test.go` | 成立 |
| 预跑新增 `watch_hub.go` | **假阳性，应撤销** |
| 预跑新增 `routeplan/service.go` | **假阳性，应撤销** |
| — | **表外新增**：`authority.SnapshotPublisher` + `actorrouting/provider.go`（阻断项） |
| — | **表外新增**：`gateverification/verification_test.go`（第 8 轮进入 break set，未达 edit） |

**上一版预跑的实测结果（clean tree，`0036d17b`，build/vet 全绿、159 个测试单元编译通过为基线）：**

| 合同 | 受影响包 | edit set 文件数 | 其中仅测试文件 |
|---|---|---|---|
| C07/C08 | 7 | 18 | `cmd/routectl`、`internal/routecontrol/transport`、`internal/testkit/capstaticroute` 三个包为纯测试边 |
| C09/C10 | 6 | 9 | — |
| C18 | 2 | 4 | — |
| 合计（去重） | **8** | **20** | |

C09/C10 做了完整的实证闭环：真加方法、迭代到不动点、再整树还原，编译器给出的 edit set 与类型探针的预测**逐文件一致**，8 个实现者与 §11.2 C09 表的"八个"吻合。这条路径可以认为已经证实。

比对七行先验表，**四处差异，两处全新**：

```text
新发现（此前任何一轮都没出现过）：
  internal/routecontrol/watch_hub.go   第十二处写集缺口
  internal/routeplan/service.go        第十三处写集缺口
    两者都持有 RouteSnapshotStore 并调用其方法，fence 形参一加即断；
    此前这两个目录都用【具名文件】列写集，又一次漏在同一种形态上

已在 S5 表中列出、却没有同步到 §8.1 写集：
  cmd/routectl/main_test.go                                 边 5
  internal/storage/redis/routecontrol/routecontrol_test.go  边 6
  （外加实测新增的 holder_store_test.go，记为边 6b）
```

第二类差异的性质值得单独指出：它不是"没想到"，而是**表改了、清单没改**——与 R6-03（§8.3 改了、§16 没改）、R5-08（P0 改了、§16/§17 没改）是同一个失效模式的第三次出现。因此 §12.8 CF-1 的绑定 gate 增加一条要求：先验表与 §8.1 写集必须由**同一次**机械动作产出并比对，不接受人工誊抄。

冻结解法是给 A0.5 一项**受限、临时、可机械核验**的 conformance 编辑权，而不是搬动阶段边界：

```text
conformance 编辑的定义（四条全部满足才算）：
  C1 只做两件事之一：把已有签名改成匹配新接口，或补上新接口要求的方法
  C2 对【全部既有调用路径】行为逐字节等价。新增形参（如 ctx）在 A0.5
     阶段被接受且【忽略】，由 A1R 负责让它真正生效
  C3 新增方法的函数体只能是 `return <zero>, ErrNotImplementedUntilA1R`。
     不允许"顺手实现一点"，不允许返回 nil error
  C4 不改任何 Lua 脚本、不加任何 key、不动任何 schema

C3 引用的哨兵【本身】不是 conformance 编辑（round-6 R6-07）：
  声明一个 var 既不是改签名也不是加方法，C1 容不下它。
  处置：哨兵在 A0.5 【自己的】写集里声明一次，作为普通 A0.5 编辑，
  与 conformance 权无关。声明位置由第一个动作的 go build 确定——
  它必须能被上表全部被编辑单元 import 且不成环。默认位置
  internal/routecontrol/conformance.go（C07 的 A0.5 写集）；
  若 go build 报出 import cycle，改放一个无内部依赖的叶子包，
  该事实同样在 A0.5 gate 留档。
  A1R 的 no_conformance_sentinel_remains 因此有两件事要清：
  全部哨兵【调用点】与哨兵【声明】本身。

权属是【时序移交】而不是并发共有：
  A0.5 对上表被列出的文件、以及第一个动作实测补出的文件持有 conformance
  编辑权，该权利在 A0.5 seam commit 处终止；此后由各自的正常 owner
  （A1R / A4）完整拥有。§2/§11.6 的"互不重叠"约束的是
  【同一时刻的两个 Writer】，本条不构成并发重叠
```

两侧各加一道 gate，保证这项权利不被滥用、也不会遗留到生产：

- A0.5：`conformance_edits_are_behavior_preserving=true`。核验方式是机械的——被 conformance 编辑的文件其 diff 内不得出现 Lua 字符串、不得出现新 key 常量，且每个新增方法体必须**逐字**是 C3 的哨兵形式；
- A0.5：`compile_edge_set_measured_not_enumerated=true`。第一个动作的四条命令输出与七行先验表的比对结论必须留档，二者差异逐条给出归属；
- A1R：`no_conformance_sentinel_remains=true`。全仓 `ErrNotImplementedUntilA1R` 命中数必须为零，**声明与调用点都算**。哨兵在两阶段之间是 fail-closed 的（调用方按 §8.3 的 unknown 处置转 BLOCKED），但它**不得**存活到 A5。

哨兵在窗口期不会进入生产：A0.5 是 seam commit，A4 composition 默认 off，A5 才是第一次真实运行。即便被调用，返回 error 也走 fail-closed 路径。

本清单与 §11.2 的 A0.5 条目必须逐条一致；§11.5 的并集条款只是兜底，不能替代本清单的完整性。

A0.5 只加入 shared production seams/token propagation/durable-stop interface 与 tests，不实现 Redis/K8s adapter。完成后独立 review 并形成 immutable seam commit。

**A0.5 的工作量已按 round-4 二次重估**（round-3 的重估方向是反的）：`executor.go` 的改动**是**"改造已有接缝"——七个 mutation 入口全部已在 `EnterPlanMutation` barrier 内（见 §5.3）。真实工作是把内存 barrier 换成 durable lease、让当前被丢弃的 scope 形参产生意义、并收紧 `EnterPlanMutation` 的导出面。风险低于 round-3 的描述，但 before/after 行为等价测试仍须逐入口做。

A3 exact Owned Write Set：

```text
internal/scaleplan/adapters/control/**
internal/testkit/scaleplancontrol/**
docs/verification/scaleplan-a3-*
```

A3 Forbidden：

```text
internal/scaleplan/{repository.go,blocked_termination.go,executor.go,ports.go}
internal/routeplan/**
internal/actorrouting/authority/**
internal/routecontrol/**          # 含 holder_registry.go、placement_exclusion.go、snapshot_store.go
cmd/route-controller/**
cmd/cap-static-route-init/**      # C17 归 A1R，A3 同样禁止
```

注意 `internal/routecontrol/**` 对 A3 全域禁止与 C09/C07 把其中若干 exact 文件分配给 A0.5 并不冲突：禁止的对象是 A3，拥有者是 A0.5。

若 A3 发现 seam 不足，立即停止并开新的 A0.5 amendment，由唯一 Shared Seam Writer 修改、重跑 A0.5 与 A1/A2 compatibility review；不得让 A3 与 A0.5 overlap。A1/A2/A3 只能从同一个 reviewed A0.5 commit 创建。

## 8.2 Complete placement capture

禁止逐 shard 调用后拼接。生产流程：

```text
capture Manager/route snapshot global revision
read complete realm+domain scope from one canonical snapshot
read exact Authority fences
read exact Holder Registry snapshot
join every active placement to exact Holder process and endpoint
compute total active cardinality + canonical checksum + topology fingerprint
end-read global revision/checksum
unchanged -> KnownFresh=true + opaque token
changed/partial -> conflict
```

`CaptureShard`、`CaptureShards`、`CaptureHolderShards` 都从一次完整 snapshot 派生；不能各自使用不同 revision。Blocked proof 返回完整 domain inventory，并证明 submitted move keys 全部存在，同时验证 unrelated active placements。

Token：

```text
scope realm/domain-global；附带 authoritative workload UID 仅用于 exact join，不缩小 placement completeness
snapshot global revision
snapshot checksum
topology fingerprint
total active cardinality
Holder registry revision/checksum
Authority capture revision
authority lease epoch（每 shard 聚合）
assignment revision
fencing epoch
```

`Revalidate` 重复 end bracket；任一值变化即失效。

Token 中**不得**包含 authority lease deadline 或任何会被 same-generation renew 推进的量。理由见 §8.6 的 liveness 豁免：holder-side renew 持续发生且不进 gate，若把 deadline 纳入 token，每一次 renew 都会让 capture 失效，扩缩容将永远取不到完整证据。lease epoch 与 assignment revision 则**不会**被 same-generation renew 改变，因此既能覆盖真正的 placement 变化，又对 renew 免疫——这正是豁免成立所依赖的分界。

## 8.3 PlacementExclusionPort

复用已 reviewed durable Manager-wide exclusion：

- exact HolderID+BootID+PodUID+WorkloadUID+Ordinal；
- owner ScalePlanID；
- monotonic revision CAS；
- Manager restart 可恢复；
- target admission/commit/publication/retry/recovery 全路径检查；
- excluded victim 仍可 Serving/Renew/source；
- Adapter 只做 exact mapping，不创建旁路 exclusion store。

### Exclusion 生命周期（round-2.5 自查 finding X1）

round-1 与 round-2 都没有覆盖这一点：本文档此前 18 处提到 exclusion，全部是"如何加上"，**没有任何一处定义它何时被解除**。§1.2 的 scale-in 序列停在"StatefulSet replicas 减一"，§6.8 的 sealed terminal 脚本只动 ScalePlan 自己 keyspace 内的 Plan/Receipt/Proof/索引/active workload，物理上也不可能解除另一个系统里的 exclusion。

#### 现状：单向闩锁，且记录的**可达性**由 Holder 记录决定

round-3 复审推翻了本节 round-2.5 版本的一条关键结论，修订后的源码事实如下。

接口侧确为单向闩锁：

```text
internal/routecontrol/placement_exclusion.go:47-59
  PlacementExclusionStore / PlacementExclusionController 只有
  GetPlacementExclusion / ExcludePlacement / ValidatePlacementTarget，没有解除方法
internal/routecontrol/placement_exclusion.go:88-96
  validatePlacementExclusionPrecondition 拒绝 ExpectedExcluded=true
  错误文本即 "only exact unexcluded CAS is supported"，即只支持 false -> true
internal/routecontrol/placement_exclusion.go:78-85
  Excluded=true 必须有 owner；Excluded=false 必须无 owner
```

存储侧的形状与前一版描述不同，这决定了整节的设计：

```text
internal/storage/redis/routecontrol/holder_store.go:219-227
  PlacementRecord(domain, holderID) -> "placement:<hex domain>:<hex holderID>"
  key **不含 bootID**；bootID 只是记录内的一个字段
internal/storage/redis/routecontrol/holder_scripts.go:1119-1128
  current_holder：Holder 记录不存在 -> NOT_FOUND
                  Holder 记录 boot ≠ 请求 boot -> BOOT_MISMATCH
internal/storage/redis/routecontrol/holder_scripts.go:1162-1164 / 1185-1187
  getPlacementExclusion 与 excludePlacement 都先过 current_holder，
  非 OK 直接返回，**根本到不了 placement 记录**
internal/storage/redis/routecontrol/holder_scripts.go:1144-1147
  placement 记录 boot == 请求 boot -> 返回真实状态
  否则返回合成基线 ('0', revision+1, '')，即「未排除、无 owner」
```

由此得到三条准确结论，替换 round-2.5 版本：

1. **记录数有上界，不需要保留期机制。** key 只含 `(domain, holderID)`，同一 holderID 永远只有一条记录，后续 exclude 原地覆盖。上界是历史出现过的不同 holderID 数（≈ 最大 ordinal 数），不是「每轮扩缩容 +1」。round-2.5 据此设计的 `created_at_ms` + bounded ZSET + `PruneExpiredExclusions` + 启用前置计数 gate 全部基于错误前提，**本轮整体删除**；
2. **exclusion 的效果是 boot 范围的，尽管 key 是 holder 范围的。** boot 一旦变更，`load_placement` 对新 boot 返回合成的未排除基线，旧记录对任何调用者都表现为未排除，因此它**永久惰性**，既不占用 placement 合格性，也不会被新 Pod 误继承。原「随每轮扩缩容无界增长的死数据」判断不成立；
3. **但"Holder 记录不存在"不属于第 2 条，记录不可达是可逆的**（round-4 P0，修正 round-3 版本）。Holder 记录有 3 分钟 TTL（`holder_store.go:26`），exclusion 记录**没有任何 TTL**——`holder_scripts.go:675` 的 `expire_keys` 只在 `:968`/`:1094` 两个 Holder 记录脚本里调用，exclusion 脚本从不调用。进程只要有一次超过 TTL 的心跳中断（Redis 抖动、长 GC、网络分区），Holder 记录就会消失而 **BootID 不变**（BootID 是进程 generation，只在进程重启时重新生成）；注册脚本 `:911` 判 `EXISTS` 为假即 `existing = false`，直接落到 `:956` 的无条件 `HSET`，而 `FRESH_BOOT_CONFLICT`（`:942`）只在记录**仍存在**且 boot 不同时才触发，因此**同一 boot 可以在 TTL 失效后重新注册**，`load_placement` 随即因 `values[4] == boot`（`:1144`）恢复返回真实的排除状态；
4. **真正的危害只有一条**：scale-in 中止或 BLOCKED 安全终止时，victim 进程仍在运行且 boot 未变，它已被迁空又不能接收新 shard，成为纯粹的死容量；且因 CAS 前置条件必须是 `ExpectedExcluded=false`，同一 boot 内的重试被永久阻塞。这与 §1.4 的 fail-closed 定义冲突——fail closed 要求"保持原 durable ownership，不产生新的不安全副作用"，而这是一个持久的容量副作用。

#### §8.3 在 A0 冻结什么、不冻结什么（S4 范围收窄）

这条机制在 round-2.5、round-3、round-4 连续三轮产生 finding，每一轮的修法都是新增一条分支或约束，而每一轮新增的分支又成为下一轮的缺陷来源（round-3 新增的 `RELEASED_BY_HOLDER_ABSENT` 即 round-4 的 P0）。根因是它的正确性完全取决于 Redis 记录的 TTL 与重注册语义，这类事实在文档层只能论证、不能证伪。因此 A0 收窄为**只冻结安全属性与 fail-closed 默认**：

```text
A0 冻结（不可在后续阶段放宽）：
  SP-EXCL-1  证据缺失不得当作"无需解除"的证明。只有正面证明"该 exclusion
             记录此后不可能再对任何调用者生效"才允许跳过解除
  SP-EXCL-2  当前唯一被接受的正面证明是 current_holder=BOOT_MISMATCH
  SP-EXCL-3  任何其他 capture 结果（含 NOT_FOUND、unknown）一律保持 BLOCKED
             并重试，不得 terminal
  SP-EXCL-4  释放必须 owner-scoped、exact 匹配 owner 与 revision
  SP-EXCL-5  释放必须排在 scale 写 exact 解析为 NOT_COMMITTED 之后
  SP-EXCL-6  停留在非终态的释放路径必须在有界次数后变为 operator-visible
             的 degraded 状态。degraded 是【可见性】变化，不得改变
             SP-EXCL-1~3 的任何判据，不得新增允许 terminal 的分支

A0 不冻结（下放到能被测试证伪的阶段）：
  - 重试节奏、退避与 BLOCKED 停留时长上界      -> A1R + A5
  - exclusion_release_escalation_attempts 与
    exclusion_release_escalation_min_wall_ms   -> A5 实测后冻结；
                                                  A0 只冻结"必须有界"
  - 是否新增第二种正面证明（例如 Kubernetes 侧
    证明 Pod 对象已不存在且 UID 已变）          -> 需 A0 plan amendment，
                                                  且必须先有 A1R 集成测试
  - Holder 记录 TTL 与重注册的精确边界行为      -> A1R 集成测试实测后回填 §12.7
```

新增正面证明属于 amendment 而不是实现细节，因为放宽 SP-EXCL-2 就是在放宽 SP-EXCL-1。

#### 冻结解法：owner-scoped 释放，单一 inert 终态分支

```text
ReleasePlacementExclusionForOwner(domain, holderID, bootID, ownerScalePlanID, expectedRevision)
```

- 只允许 true -> false，必须 exact 匹配 `ownerScalePlanID` 与 `expectedRevision`；
- revision 单调 +1，域上限与算术规则见 §6.13；
- 解除后必须清空 owner，与现有 `validatePlacementExclusion` 的"unexcluded state has owner"约束一致；
- 幂等可重放：owner 匹配、当前已 unexcluded、revision 一致 -> `ALREADY_RELEASED`；
- 解除是 Placement writer，必须在 coordinator token 下进行（§8.6）。

**owner 独占性的准确范围。** 独占只在单个 `(holderID, bootID)` 代内成立。boot 一旦滚动，`load_placement` 对新 boot 返回合成的未排除基线，另一个 ScalePlan 可以合法地对同一 holderID 重新 exclude 并成为新 owner——这条路径走的是既有 `ExcludePlacement`，新增的 owner-scoped 释放拦不住它。这**不是**缺陷：旧 boot 的 exclusion 此刻已经惰性，覆盖它不会撤销任何仍在生效的排除。但由此推出释放路径必须有 inert 终态分支，否则原 owner 会卡死。

**释放的结局枚举。** 释放前必须先做一次 exact Holder capture。枚举的依据不是"设计上想到几种"，而是 `getPlacementExclusionScript`（`holder_scripts.go:1151-1169`）**在源码上能返回的全部值**，逐个给出处置。缺省规则是：除 `FOUND`+CAS 成功与 `BOOT_MISMATCH` 外，一切结果都不允许 terminal。

| 脚本返回 | 产生位置 | 含义 | 释放结局 | 是否允许 terminal |
|---|---|---|---|---|
| `FOUND`，且 placement 记录 boot == 该 boot | `:1168` | exclusion 仍在生效 | 执行 owner CAS 解除 | CAS 成功才允许 |
| `BOOT_MISMATCH` | `current_holder :1127` | victim 已换 boot，旧 exclusion **永久**惰性 | `RELEASED_BY_BOOT_ROLLOVER`，不写 | 允许（**唯一**的 inert 终态） |
| `NOT_FOUND` | `current_holder :1121` | **证据缺失**，不构成 victim 已消失的证明 | 保持未决 | **不允许**，BLOCKED + 退避重试，有界后进 `DEGRADED_EXCLUSION_UNRELEASED` |
| `MALFORMED` | `:1160` 参数校验、`current_holder :1124/:1126`、`load_placement` 返回 nil（`:1166`） | 记录损坏或参数不合法 | 保持未决 | **不允许**，同 `NOT_FOUND`，并按 §6.4 触发 corruption 判定 |
| `CROSS_SLOT` | `:1152` | key 未落在同一 slot | 保持未决 | **不允许**，同 `NOT_FOUND`；属部署配置错误 |
| `REVISION_EXHAUSTED` | `:1167`（经 `load_placement :1146`） | revision 抵达 §6.13 域上限 | 保持未决 | **不允许** |
| 传输层失败 / 超时 / 连接中断 | 脚本之外 | 无法判定 | 保持未决 | **不允许**，BLOCKED |

`REVISION_EXHAUSTED` 在**释放路径上不可达**，理由必须写明而不是省略：释放总是以当初 exclude 时的那个 bootID 发起 capture。`load_placement` 只在 placement 记录的 `values[4] ~= boot` 时才走 revision+1 的合成分支（`:1144-1147`），而那要求 boot 已经滚动；boot 一旦滚动，`current_holder` 会**先**返回 `BOOT_MISMATCH`（`:1127`）并在 `:1164` 直接短路返回，根本到不了 `load_placement`。列出它是因为脚本能返回它，A1R 若观察到该值即说明上述推理有误，应触发 `REQUIRED_FINDING`。

**为什么只有 `BOOT_MISMATCH` 是 inert 终态（round-4 P0）。** round-3 版本把 `NOT_FOUND` 也列为 inert 终态 `RELEASED_BY_HOLDER_ABSENT`，理由是"victim 已消失、记录不可达且惰性"。按上文结论 3，这个推理不成立，落地后会主动制造本节要消除的那个后果：

```text
victim 心跳中断超过 3 分钟 -> Holder 记录过期
  -> 释放路径 capture 到 NOT_FOUND -> 判定 inert -> 不写、允许 terminal
  -> ScalePlan 终止，释放 workload ownership，exclusion 从未解除
  -> victim 心跳恢复，以【同一 boot】重新注册成功
  -> load_placement 因 values[4] == boot 返回真实状态 excluded=1, owner=<已终止的 Plan>
  -> exclusion 重新生效，但 owner 已不存在：
       owner-scoped 释放要求 exact 匹配 owner  -> 无人能解除
       ExcludePlacement 前置条件强制 ExpectedExcluded=false -> 无人能重新 exclude
  => 死容量 + 该 boot 上的重试被永久阻塞，即结论 4 描述的危害
```

两条判据的区别是**证明的方向**。`BOOT_MISMATCH` 是**正面证明**：新 generation 已经接管，`load_placement` 对新 boot 返回合成的未排除基线（`:1144-1147`），旧记录此后对任何调用者都不可能再生效，覆盖它不会撤销任何仍在生效的排除。`NOT_FOUND` 只是**证据缺失**：它同时兼容"进程已永久消失"和"进程活着但心跳断过一次"，而这两种情况需要相反的处理。

同节内部还有一处矛盾佐证了这一点：本分支的前置条件（下文"与 scale 写结果的定序"）要求 scale 写已 exact 解析为 `NOT_COMMITTED` **且 `spec.replicas == expected`**——也就是 Kubernetes 明确**没有**减少副本、**不会**删除 victim Pod。在已经证明 Pod 不会被删除的状态下，从一条 Redis 注册记录的缺失推断出"victim 已消失"是自相矛盾的。

**`NOT_FOUND` 的活性出口（round-5 更正）。** 前一版本论证「Pod 总会回来，因此 `NOT_FOUND` 是瞬态」。这条论证**不成立**，本节据此更正。

StatefulSet 语义保证的是 **Pod 对象**被重建，而 `current_holder` 读的是 Holder 记录，后者只有 Holder **进程注册成功**才会产生。两者之间隔着一整个"Pod 能否真的跑起来"的问题。以下状态都让 Pod 对象存在、却无限期不注册：`CrashLoopBackOff`、`ImagePullBackOff`、不可调度（资源不足、亲和性无解）、PVC 未绑定、节点 `NotReady` 导致 Pod 卡在 `Terminating`。本计划自己的 A5 要求还**放大**了这个概率而不是缩小：§14.1 强制"应用 ResourceQuota、低优先级 PriorityClass"，一个被抢占或被配额挡住的低优先级 Pod 会一直 `Pending`。因此 `NOT_FOUND` 可以是稳定状态，需要真正的出口。

先分清两种危害，它们的紧迫性不同：

```text
exclusion 记录本身在 NOT_FOUND 期间【无害】：
  没有 Holder 记录 -> current_holder 先行拒绝 -> load_placement 根本到不了
  -> 没有任何 shard 能被放置到该 holderID，exclusion 生效与否都一样
  真正的危害在 victim 以【同一 boot】回来的那一刻才出现（即结论 4）

真正在流逝的是【Plan 的 BLOCKED 状态】：
  它持有 workload ownership，阻塞该 realm 上的其他 ScalePlan
  这是一个活性问题，不是安全问题
```

因此出口只需要解决后者，且**不得**用放宽 SP-EXCL-1～3 的方式解决：

```text
capture 连续 exclusion_release_escalation_attempts 次为 NOT_FOUND/unknown
  且累计停留超过 exclusion_release_escalation_min_wall_ms
-> 进入 DEGRADED_EXCLUSION_UNRELEASED

DEGRADED_EXCLUSION_UNRELEASED 的性质：
  - 是【非终态】。Plan 仍然 BLOCKED，workload ownership 仍然保留
  - 不改变上表任何一行的判据，不新增允许 terminal 的分支
  - 唯一的变化是【可见性】：置位 operator-visible 状态与告警，
    并在 Plan 记录中持久化已尝试次数、首次进入时间、最近一次 capture 结果
  - capture 一旦转为 OK 或 BOOT_MISMATCH，自动离开该状态，走上表原路径

操作员可用的处置（按代价排序，均不放宽安全属性）：
  1. 修复 Pod 无法运行的根因（配额、调度、镜像、PVC、节点）
     -> 进程注册 -> capture 转 OK 或 BOOT_MISMATCH -> 自动出清
  2. 删除 victim Pod。若它此后能起来，必然带【新 BootID】
     -> capture 转 BOOT_MISMATCH -> inert 终态
  3. Pod 永久无法运行（例如配额被永久回收）时，唯一出路是
     §8.3「A0 不冻结」里那条【第二种正面证明】：由 Kubernetes 侧
     exact 证明 Pod 对象已不存在、或其 UID 已不同于 exclusion 时记录的 UID。
     这蕴含绑定该 boot 的进程已消失、该 boot 永不可能重新注册，
     因此旧记录永久惰性——这是与 BOOT_MISMATCH 同等强度的正面证明。
     但它需要 A0 plan amendment 且必须先有 A1R 集成测试，不在本版本启用
```

手段 3 的存在说明本节没有把自己关死，但它**不是**本版本可用的逃生门：在 amendment 落地前，`DEGRADED_EXCLUSION_UNRELEASED` 的正确读法是"这个 realm 需要人来看一眼"，而不是"可以强制终止了"。§9.4 的 `TerminateBlockedScalePlan` 对该状态**仍然被禁止**——否则等于用一个人工按钮把 round-4 的 P0 原样放回来。

这条分支正是 round-3 finding「释放结构上不可达却又被要求必须成功」的出口：它把「无法解除」与「无需解除」区分开，只有前者才阻塞终止。round-4 的修正把"无需解除"收紧为只由正面证明构成。

**与 scale 写结果的定序（round-3 finding）。** 「victim Pod 是否仍存在」在 scale 写未决时不是稳定谓词——replicas 可能已提交而 Pod 尚未被 StatefulSet controller 删除。此时解除 exclusion 会让 victim 立刻重新成为 placement 合格目标，Manager 可能把 shard 迁入一个 Kubernetes 正准备删除的 Pod。因此释放的前置条件是：

```text
该 scale operation 已 exact 解析为 NOT_COMMITTED（按 §7.2 的取消屏障）
  且 Kubernetes capture 证明 workload UID 未变、spec.replicas == expected
-> 才允许进入上表
outcome unknown -> 保留 exclusion，保持 BLOCKED，不得 terminal
```

释放时机冻结：

| ScalePlan 终态 | scale 写结论 | exclusion 处理 |
|---|---|---|
| scale-in 成功（replicas 已减、Pod 将被删除） | COMMITTED | 不解除。**理由是 boot 滚动，不是记录消失**：该 ordinal 若再次上线必然带新 BootID，`load_placement` 对新 boot 合成未排除基线（`:1131-1148`），旧记录永久惰性。与 §1.2、F61b 同一条判据 |
| scale-in 失败/中止，Plan 进入普通 terminal | NOT_COMMITTED（exact） | 必须走上表七行之一；仅 `FOUND`+CAS 成功与 `BOOT_MISMATCH` 两行允许 terminal |
| BLOCKED 安全终止 | NOT_COMMITTED（exact） | 同上，与 child durable-stop 同级 |
| 任一终态 | UNKNOWN | 保留 exclusion，不得 terminal，保持 BLOCKED |
| Plan Pause | 任意 | 保留 exclusion，Resume 后继续 |

**没有 retention 机制。** 不新增 `created_at_ms`，不新增索引，不新增 prune，不新增启用前置计数 gate。理由见上文结论 1、2：记录数有界、失效记录惰性。这同时保证了 A1R 对 `placement-exclusion-v1` 记录的 **additive-only 主张成立**——现役 Lua 对该记录做精确形状校验（`holder_scripts.go:1101-1104` 恰好 8 个字段、`:1134` 的 `HLEN ~= #PLACEMENT_FIELDS` 即判 MALFORMED、`:1141` 的 integrity 覆盖 6 个字段），任何加字段的改动都会让滚动发布期间的旧脚本把全部记录读成损坏，从而中断整个 domain 的 placement admission。本版本不加字段。

失败矩阵见 F59～F61（§10）。写集归属见 §11.2：接口、Manager 与 `HolderRegistry` 侧属 A0.5，Redis 脚本侧属 A1R。

## 8.4 RoutePlanPort

- `EnsureMove`：完整 expected request，idempotent；
- `GetMove`：调用 exact `GetPlan(plan_id)`，比较所有 immutable fields；
- terminal evidence 在 38d retention 内读取；
- `Verify`：只接受 `ProductionVerification`；
- missing/mismatch/malformed/regression 均 fail closed；
- owner retain-until 不得早于 ScalePlan recovery retention。

## 8.5 Durable child stop

Blocked safe termination 要求每个 child 当前 terminal 且 `DurablyStopped=true`。A3 不能自行填 bool。

必须新增 core-owned RoutePlan stop primitive。阶段归属冻结为：**A0.5 冻结合同与 core primitive，A1R 实现 Redis 持久层，A3 只做 exact mapping**（关闭 round-2 finding P1-A：此前 A0.5 明确不实现 Redis adapter，A3 又禁止写 `internal/routeplan/**`，导致 `internal/storage/redis/routeplan/**` 无人拥有）。

```text
StopOwnedPlanForScaleTermination(
  ownerScalePlanID,
  exact child PlanID/payload,
  expected current revision,
  termination fence token,
)
```

原子操作：

- exact load child；
- 若已 SUCCEEDED/FAILED/PAUSED，validate/replay；
- 若 nonterminal，仅在 quiescence gate closed 且没有 unresolved Manager effect 时，terminalize 为 `PAUSED`；
- error code 固定 `SCALEPLAN_OPERATOR_SAFE_STOP`；
- 从 RoutePlan restart index 移除；
- 写 terminal retention；
- 持久化 owner/fence audit；
- response unknown 可 exact replay。

`DurablyStopped=true` 只能由以下事实派生：terminal record 完整、restart index 不含 child、Manager transition/recovery 已 quiescent（证据必须是全称的枚举加基数断言，`Manager.ObserveTransition` 只是对已知 child 的补充证据，见 §8.6）、fence epoch 当前有效。RoutePlan Controller restart 不得恢复 terminal PAUSED。

若 child effect/Manager outcome 未知，停止操作返回 unsafe，父 Plan 保持 BLOCKED 与 workload lock。

持久层实现归属（A1R，exact 文件见 §11.2）：

```text
internal/storage/redis/routeplan/plan_store.go     新增 StopOwnedPlanForScaleTermination 入口
internal/storage/redis/routeplan/scripts.go        新增单脚本原子：终态写入 + restart index 移除 + retention
```

该脚本必须在一次原子执行内完成"写 terminal 阶段 + 从 restart index 移除 + 写 retention + 持久化 owner/fence audit"，不得拆成多次 Redis 调用；否则 Controller 在两次调用之间崩溃会留下一个 terminal 但仍在 restart index 里的 child，restart 后被复活。既有 `plan_store.go` 只有 `Load` / `CompareAndSwap` / `ListIncomplete`，没有任何终态原子入口，因此这是必需的新增而非映射。A1R 必须遵守 §2 的 additive-only 约束：不改变既有脚本语义、不破坏既有 schema、不改变 G21 identity。

## 8.6 ControlMutationCoordinator 与 quiescence fence

### Writer 集合与 actual call graph

Coordinator 采用**可继承 operation token**，不是让每层重复 acquire 非重入 gate。Top-level writer acquire 一次，nested call 必须传递同一 token；Manager/Publisher 验证 token scope/epoch但不重新登记 active operation。

Frozen coverage table：

| Top-level / nested path | Acquire/verify | 最小 scope | durable fence check points |
|---|---|---|---|
| ScalePlan Submit/Execute/Recover/Terminate | acquire Plan mutation guard；external effects另 register control op | realm-global | before intent CAS、before/after each external effect、Plan CAS |
| Kubernetes Workload Scale | inherited ScalePlan token或独立 manual top-level token | realm + StatefulSet UID | operation register、API request前、exact reread后、completion |
| RoutePlan Submit/Execute/Recover/Stop | top-level acquire，若由 ScalePlan 调用则 inherit | realm/domain；owned child附 workload UID | RoutePlan CAS、drain callback、Manager call、terminal/restart index commit |
| Manager `Claim`/`Transfer`/`TransferValidated`/`Unassign`/`Drain`/`DrainValidated` | verify inherited token；独立调用先 acquire | **realm/domain-global**（请求未必有可信 workload UID） | authority store mutation前后、recovery tombstone、snapshot publication |
| Manager `Renew` | **liveness 豁免，不进 gate**（round-3 P1-4 更正；理由见下） | 不适用 | 由 §8.2 capture token 的 end revalidate 覆盖 |
| Manager `ProjectFreshLeases` | top-level acquire realm-global shared writer token | realm-global | wave capture、pending publication retry、publish commit |
| Manager `RecoverPendingProjections` | top-level acquire exclusive recovery token | realm-global | each recovery load/effect/commit/publish |
| Placement `Exclude` / `ReleaseForOwner` / admission / commit / publication | inherited Manager/Scale token | realm/domain-global | exclusion CAS、release CAS、target choice、authority commit、publish |
| RouteSnapshot publisher `Publish`/`PublishAuthoritySnapshot` | **verify inherited token only** | inherited | durable snapshot CAS 与 notification publication |
| RouteSnapshot publisher `Bootstrap` | **top-level acquire 独立 startup/recovery token** | realm-global | bootstrap CAS 前后、notification publication |
| Holder-side same-generation lease renew | **liveness 豁免，不进 gate**（见下） | 不适用 | 由 §8.2 capture token 的 end revalidate 覆盖 |
| Holder Register/Heartbeat | 不阻断 liveness；只能更新 immutable identity+liveness，不能作为 placement writer | realm/domain | Holder store CAS；placement eligibility/exclusion由 Manager gate控制 |
| Transition quiescence 枚举（只读证据入口） | 在 gate 下 capture token，不登记 writer | realm/domain | 见下"quiescence 的证据必须是全称的" |

禁止无 token 的 production Manager/RoutePlan/Publisher mutation。Read-only Get/List 可在 gate 下 capture token，end revalidate；不登记 writer。

**`SCALEPLAN_MODE=off` 下的默认语义（round-3 P1-14）。** 上一句与 §9.2 的"off 模式不构造 quiescence coordinator"存在互斥：off 是默认模式，而 Manager/RoutePlan/Publisher 在 off 下照常运行并持续 mutation，此时 coordinator 不存在、token 无从获取。两种朴素实现都不可接受——强制校验会让 G21 在默认模式下全线报错；"token 缺省即放行"则意味着只要 coordinator 未装配（配置错误、部分回滚、被恶意关闭），fence 就静默失效，而 fence 正是 §4.5"两 Controller 同时写"的唯一控制。因此冻结显式三态：

```text
coordinator = absent   仅当 SCALEPLAN_MODE=off 时合法；
                       此时 Manager/RoutePlan/Publisher 走与 G21 完全一致的无 token 路径，
                       配置 hash 与 G21 identity 不变
coordinator = present  mode=manual/auto 必需；缺失则**进程启动失败**，不得降级放行
非法组合            mode≠off 且 coordinator=absent -> 启动期 fail closed
                       mode=off  且 coordinator=present -> 启动期 fail closed（避免"off 却有副作用"）
```

关键点：token 的缺省行为不是"放行"，而是"该进程根本不该处于这个状态"，判定发生在**启动期**而不是每次 mutation。这样既不给 G21 增加运行时分支，也不存在静默失效的路径。该断言必须同时进入 §12.6 的 off-mode 零副作用测试与 §12.2 的 A0.5 gate。

### quiescence 的证据必须是全称的（round-3 P1-5）

`DurablyStopped=true` 与 blocked 安全终止都要求证明 Manager 侧 transition/recovery 已 quiescent。前一版本把 `Manager.ObserveTransition` 冻结为"唯一证据来源"，这是错的：该 API 的入参是**单个** `TransitionObservationRequest`（含具体 transitionID），返回该 transition 的 `CompleteRecovery`/`Tombstone` 分类，且在证据缺失或与 memo 不符时直接报错（`internal/actorrouting/authority/manager.go:1428-1497`）。它只能回答"我已知的这一个 transition 是否已完结"，无法回答"realm/domain 内不存在任何在途 transition"——而后者才是 quiescence。这是证明义务与所选 API 的语义不匹配。

冻结修正：

```text
quiescence 证据 = 枚举原语 + 基数断言 + end revalidate
  枚举原语：LoadTransitionRecoveries / LoadTransitionTombstones
            （internal/storage/redis/routecontrol/transition_store.go:96/299 已存在）
  基数断言：在 gate EXCLUSIVE 下，realm/domain 内未完结 transition 集合为空
  end revalidate：capture 结束时重新枚举，集合仍为空且 assignment revision 未变
Manager.ObserveTransition 降级为"对已知 child 的补充证据"，不再是唯一来源
```

该枚举表面是否足以支撑全称断言，登记为 preflight gate `TRANSITION_QUIESCENCE_ENUMERATION`（§12.7），其**反例**（在途但尚未可枚举的时间窗）登记为 `TRANSITION_ENUMERATION_BLIND_WINDOW`；任一不成立则 `BLOCKED_QUIESCENCE_EVIDENCE`，不得进入 EXCLUSIVE。

**exact 路径归属为新增的 C18（round-4 P1 更正，此前误写为 C12）。** C12 的路径列只有 `manager.go` + test，而 `transition_store.go` 此前不在 §11.2 任何一行、不在 §11.4、也未被 C13 preflight 悬置。两个枚举原语当前签名不接受 `context.Context`，与 §5.3 的 `guard.Context()` 约束冲突，A0.5 必须改签名——详见 §11.2 C18。

### `Bootstrap` 为什么必须是独立 top-level token

`DurableSnapshotPublisher.Bootstrap`（`internal/routecontrol/durable_publisher.go:117`）是一条独立的持久快照 mutation 入口，其实现明确"never falls back to ordinary CAS"，随后还会 `notify`。它发生在启动路径上，正是两个 Controller 最可能重叠的时刻。因此它必须持有自己的 startup/recovery top-level token、durable operation record 与 publication fence，不得无 token 写。对应的持久层为 `internal/storage/redis/routecontrol/snapshot_store.go:179`，归属见 §11.2。

### Holder-side lease renew 的 liveness 豁免（关闭 P1-D）

round-2 finding 指出存在绕过 `Manager.Renew` 的直接 `RenewShardAuthority` 调用者，并给出"纳入 coordinator 或作为 liveness 豁免"两个选项。源码审计后**只有豁免一条路可走**，因此在此冻结，不留选择。

已核实的全部直接调用点（全部 `Caller.Kind = ShardAuthorityCallerHolder`）。round-3 P1-4 补入第七、第八处——前两轮的检索命令写成了 `rg -n 'RenewShardAuthority' internal/`，把 `cmd/` 整体排除在外，正是 §11.1 缺陷 2：

```text
运行在 Holder 数据面进程：
internal/logic/actor/authority/dynamic_fence.go:850    HolderLeaseRuntime.Renew
internal/logic/actor/authority/lease_renew.go:116      HolderLeaseRuntime.RenewWithFence
internal/logic/actor/authority/lease_renew.go:176      HolderLeaseRuntime.PrepareRenew
internal/authoritycomposition/membership_adapter.go:125
internal/authoritycomposition/composition.go:655
internal/authoritycomposition/composition.go:739

不在 Holder 数据面进程，但 Caller 仍为 Holder（round-3 补入，见 §11.4）：
cmd/cap-redis/scenarios_runtime.go:175                 隔离场景工具，TTL 4h
internal/testkit/capstaticroute/initializer.go:111     经 Manager.Renew 的 holder 续期
```

把它们纳入 coordinator 会造成自伤性故障：gate 进入 CLOSING 时新 writer 被拒绝，而 gate CLOSING 正是每一次扩缩容的必经阶段——这会在扩缩容窗口内停止全域 Holder 的租约续期，导致租约批量过期、authority 大面积丢失。这与 §8.6 已有的"Holder Register/Heartbeat 不阻断 liveness"是同一条理由。

**`Manager.Renew` 同属豁免，不是控制面入口（round-3 P1-4）。** 前一版本的 coverage table 把 `Manager.Renew` 标为"控制面发起"并纳入 gate，与源码相反：

```text
internal/actorrouting/authority/manager.go:1513-1519
  入口处强制 Caller.Kind == ShardAuthorityCallerHolder，
  且 fence 的 holder/boot 必须等于 caller 自身，否则返回 ErrHolderRenewOnly
  代码注释："A holder may renew only its own current tuple"
```

它在结构上**只可能**是 holder 侧同代续期，不存在"控制面发起"的形态。按前一版本把它放进 gate，恰好制造出本节论证豁免时所说的那个自伤性故障——豁免与门禁在同一节里互相矛盾。因此它与上述八处调用点同类，一并归入 liveness 豁免，并受同一组证明义务约束。

豁免不是免证明。A0.5 必须完成以下证明义务，缺一不可：

1. **不变式显式化**：same-generation renew 只延长本地单调 deadline，不能改变 holder、writer epoch、lease epoch 或 assignment revision。代码里目前只有注释与 `renewalMatchesRequest` 的隐式校验（`dynamic_fence.go:830-832`、`:870`），A0.5 必须把它变成被测试覆盖的显式不变式；
2. **capture token 的取值必须避开被 renew 推进的量**：§8.2 的 token 必须包含 authority lease **epoch** 与 assignment revision，**不得**包含 lease deadline；否则持续 renew 会让每一次 complete capture 都失效，扩缩容永远无法取得证据；
3. **对抗测试**：capture 期间持续 renew，capture 必须仍然成立；而 `Claim`/`Transfer`/`Unassign`/`Drain` 中任意一个发生，capture 必须失效；
4. **豁免失效条件**：若将来任何 renew 路径获得改变 holder/generation/placement 的能力，豁免立即失效，必须回到 A0 amendment，不得在 Adapter 层打补丁。

上述六个文件本身**不进入任何阶段的 Owned Write Set**（无需修改），豁免由 A0.5 写集内的测试断言；见 §11.2 的"显式不修改"分区。

完整受控 writer：

```text
ScalePlan Executor
Kubernetes Workload Scale（含 §7.2 的 RV 取消屏障写，屏障写为 resolution-only operation）
RoutePlan Submit / Execute / Recover / Stop
Manager Claim / Transfer / TransferValidated / Unassign / Drain / DrainValidated
Manager ProjectFreshLeases / RecoverPendingProjections
Placement Exclude / ReleaseForOwner / Admission / Authority Commit
RouteSnapshot Publish / PublishAuthoritySnapshot / Bootstrap
```

明确不在受控 writer 集合内、且已论证其安全性的：Holder-side same-generation lease renew（本节，含 `Manager.Renew`）、Holder Register/Heartbeat（liveness）、transition quiescence 枚举与 `Manager.ObserveTransition`（只读）。

**`Bootstrap` 写者集合的完整枚举（round-3 P1-2）。** 上表的 `RouteSnapshot ... Bootstrap` 此前被理解为"`DurableSnapshotPublisher.Bootstrap` 一处"。源码中还有第二个二进制**绕过 publisher 直接写持久快照**：`cmd/cap-static-route-init/main.go:245` 自行 `redisroutecontrol.NewSnapshotStore`，`:269` 调 `CompareAndSwap`、`:271` 调 `Bootstrap`。它就是 §17 停止条件里所说的"第二个未加 fence 的 Controller publisher"，而 §4.4 的单 writer 前提此前只讨论了 Kubernetes `/scale`，完全没有覆盖 route snapshot 的写者集合。本轮冻结：

```text
写者 1：DurableSnapshotPublisher.Bootstrap        -> 受 gate 管控，独立 startup/recovery token
写者 2：cmd/cap-static-route-init                 -> 隔离环境专用初始化工具
        约束：只允许在与生产 realm 不同的 namespace 内运行；
              禁止与任何 ScalePlan 同时运行；
              A1R 给 SnapshotStore.Bootstrap/CompareAndSwap 加 fence 参数时必须同步改造它
              （写集归属 C17）；
              §4.4 的单 writer 前提必须显式包含这条约束，A5 evidence 必须证明它未在
              生产 realm 上运行过
```

### Durable gate data

```text
scope realm-global 或 realm/domain-global；仅可信 K8s scale 子操作附带 workload UID
mode OPEN / CLOSING / EXCLUSIVE
fence epoch
owner ControllerID/BootID/TerminationID
acquired/expires Redis TIME
active operation records
last heartbeat
```

### 普通 writer

1. Top-level writer 在 gate OPEN 时注册 durable active operation（deterministic operation ID、类型、scope、identity、fence epoch）；nested writer只验证/继承 token；
2. 执行外部/Manager/RoutePlan/Kubernetes effect；
3. exact resolve outcome；
4. 提交 durable result；
5. 删除 active operation。

Controller crash 时 operation record 保留到 recovery；不能仅凭 TTL 删除 unresolved K8s/Manager outcome。Registration、completion/removal apply-then-response-loss 通过 operation ID exact read 重放。Recovery exact re-read 后才能 close。

### Exclusive acquire

```text
close gate (OPEN -> CLOSING, new epoch)
reject new writers
  例外：gate owner 可注册 resolution-only operation（§7.2），当前仅 RV 屏障写一种；
        它必须计入 active set，并在下一步之前 exact resolve
recover/resolve all active operations（含本轮新注册的 resolution-only operation）
require active set empty              // 严格为空，resolution-only 不例外
acquire EXCLUSIVE owner lease
terminalize/durably stop owned children where safe
prove Manager transitions/recovery quiescent   // 全称证据，见上文 quiescence 一节
capture complete K8s/Holder/Placement/Exclusion evidence
return fence + manifest
```

### Revalidate

- gate 仍 EXCLUSIVE；
- owner/epoch/expiry exact；
- active operations 仍为零；
- child/restart/Manager facts不变；
- Kubernetes UID/RV/replicas 未变；
- complete placement token 未变；
- exclusion owner/revision 未变。

### Release

- compare owner+epoch；
- terminal commit 已 exact known 后才 release；
- response loss通过 exact gate read；
- Controller crash 后 lease expiry只打开“recovery required”，不得直接 OPEN；
- recovery 先检查 ScalePlan terminal record与所有 active operations，再决定 OPEN/BLOCKED。

## 8.7 Kubernetes writer 的特殊竞态

Redis fence不能物理阻止 cluster-admin 直接 patch StatefulSet。生产控制依赖：

- Namespace Role 只授予 ScalePlan SA scale write；
- 不部署 HPA/其他 reconciler；
- 所有授权 Scale writer 使用 coordinator active-operation registry；
- admission/audit 检查拒绝或标记非授权 writer；
- fence acquire 等待所有 registered operations exact resolved；
- final revalidate StatefulSet UID/RV/replicas。

Cluster-admin/root 绕过属于 threat model 外；发现即使 A5 evidence invalid。A0.5 review 必须判断是否需要 ValidatingAdmissionPolicy/Webhook；若目标集群无法证明单 writer，A5 不得执行。

## 8.8 锁顺序

唯一允许顺序：

```text
ScalePlan repository mutation lease
-> ControlMutationCoordinator gate/fence
-> RoutePlan/Manager durable-stop/recovery barriers
-> Kubernetes/Holder/Placement observations
-> final revalidate
-> ScalePlan Redis terminal linearization
-> Control fence release/recovery
-> repository mutation lease release
```

禁止 Manager/RoutePlan callback 反向获取 ScalePlan repository lease。发现反向依赖立即停止 A3 并回 A0 review。

## 8.9 Clock/expiry

Control fence 与 ScalePlan repository 使用同一 Redis deployment time authority。Proof expiry 取以下最早值并减 safety margin：

```text
control fence expiry
Kubernetes capture freshness deadline
Holder lease minimum
placement lease minimum
child/Manager evidence deadline
```

建议 proof 最长 60s，提交 safety margin 至少 5s，加 Redis node 最大 clock skew。repository Lua 在最终线性化点再次检查；任何 deadline 不足保持 BLOCKED。

---

## 9. A4：Route-controller Composition（默认完全关闭）

## 9.1 模式

```text
SCALEPLAN_MODE=off     # default
SCALEPLAN_MODE=manual
```

本版本不提供 `observe` 或 `auto`。

## 9.2 off 模式硬要求

`off` 时：

- 不构造 ScalePlan repository/backend；
- 不读取任何 ScalePlan Redis key；
- 不构造 Kubernetes client，不读 token/kubeconfig，不做 discovery；
- 不构造 quiescence coordinator。此时 Manager/RoutePlan/Publisher 走与 G21 完全一致的无 token 路径，**这不是"token 缺省即放行"**：`coordinator=absent` 与 `mode=off` 是启动期强制绑定的一对，任一不匹配则进程启动失败，语义见 §8.6"`SCALEPLAN_MODE=off` 下的默认语义"；
- 不注册 ScalePlan gRPC/HTTP mutation API；
- 不注册 recovery worker、ticker、watch 或 goroutine；
- 不创建 RoutePlan；
- 不修改 Holder/Placement/StatefulSet；
- 不增加 DNS/network dial；
- 不改变现有 RoutePlan/Holder runtime、配置 hash 和 G21 identity；
- Close/health 路径无 ScalePlan 对象或泄漏。

必须有 compose/unit tests 对 client constructors、Redis key read、goroutine count 与 fake call count 做零调用断言。

## 9.3 manual 模式

A4 组合：

```text
A1  repository facade/backend
A1R RoutePlan durable-stop / snapshot Bootstrap fence / exclusion release 持久层
A2  WorkloadScaler/KubernetesIdentitySnapshot
A3  Holder/Placement/Exclusion/RoutePlan/Quiescence
ScalePlan Executor + startup recovery
manual API + routectl
metrics/audit
```

Manual operator 要求：

- mTLS exact client identity；
- realm/workload authorization allowlist；
- explicit manifest 与 idempotency key；
- blocked termination 需 explicit TerminationID + exact checkpoint；
- routectl 显示 immutable plan digest并要求显式确认参数；
- 无自动 threshold/autoscaling；
- audit 不记录 secret；
- API max message/plan/move bounds；
- recovery 在接受新 mutation 前完成或 fail closed。

## 9.4 API 候选

```text
SubmitScalePlan
GetScalePlan
ListRecoverableScalePlans (operator bounded)
PauseScalePlan
ResumeScalePlan
TerminateBlockedScalePlan
```

不得公开 generic Plan CAS、force unlock、force delete、skip verification。

`TerminateBlockedScalePlan` 的前置条件必须在**本节实现处**强制，不能只写在 §8.3——round-5 的 R5-08 正是"修法只改了论证处、没改实现处"的实例：

```text
拒绝条件（任一成立即拒绝，零副作用返回）：
  该 Plan 的 exclusion 释放路径未走到 §8.3 结局表中允许 terminal 的两行
    （FOUND + CAS 成功、或 BOOT_MISMATCH）
  该 Plan 处于 DEGRADED_EXCLUSION_UNRELEASED 或 DEGRADED_BARRIER_UNRESOLVED
  该 Plan 的 active operation set 非空（含 resolution-only）

这两个 DEGRADED 状态是【非终态】，它们的存在只是为了让人看见，
不是为了给出一个"重试够久就可以强制终止"的入口。
把它们做成可 terminate 的，等于用一个人工按钮把 round-4 的 P0 放回来。
```

## 9.5 Metrics

无高基数 PlanID label：

```text
scaleplan_active{realm,domain,operation}
scaleplan_phase_transition_total{phase,result}
scaleplan_repository_operation_total{op,result}
scaleplan_recovery_total{result}
scaleplan_external_effect_total{type,result}
scaleplan_quiescence_total{result}
scaleplan_fence_held_seconds
scaleplan_blocked_total{reason_class}
```

## 9.6 A4 允许/禁止

允许（formal lock absent）：全仓 test、focused race/vet/build、proto generation、secret/diff gate、Compose/K8s render。

禁止：kubectl apply、真实 ScalePlan submit、真实 Pod 扩缩、外部 Redis、镜像发布/导入、Runtime Activation。

---

## 10. Failure / Restart / Unknown-outcome Matrix

| ID | 位置/故障 | 检测 | 必须行为 | workload ownership |
|---|---|---|---|---|
| F01 | Create timeout before commit | exact Load/idempotency absent | same operation retry | retained/uncreated |
| F02 | Create response loss after commit | exact idempotency+Plan match | ALREADY_COMMITTED | retained |
| F03 | idempotency same key/different payload | index exact compare | conflict | unchanged |
| F04 | active workload exists | active index | WORKLOAD_BUSY | retained by current Plan |
| F05 | CAS response loss | candidate revision/checksum read | replay/adopt exact only | retained |
| F06 | stale mutation lease | Lua fence mismatch | reject write/cancel operation | retained |
| F07 | lease release response loss | exact owner/fence read | recovery, no force delete | retained |
| F08 | Redis record checksum corrupt | read validation | ErrCorrupt, zero write | retained |
| F09 | dangling index/reference | Lua validation | ErrCorrupt, zero write | retained |
| F10 | recoverable index over bound | ZCARD/meta | capacity/corrupt fail closed | retained |
| F11 | terminal cleanup partial | bounded batch | MORE_DUE, continue later | unaffected |
| F12 | K8s timeout before scale commit | exact UID/RV/replicas read | retry only if unchanged | retained |
| F13 | K8s apply then response loss | desired replicas exact read | adopted committed | retained |
| F14 | K8s 409 conflict | RV/UID re-read | conflict/BLOCKED | retained |
| F15 | K8s 429 | Retry-After bounded | pending, no duplicate | retained |
| F16 | StatefulSet NotFound | exact GET | conflict/BLOCKED | retained |
| F17 | StatefulSet delete/recreate | UID mismatch | conflict/BLOCKED | retained |
| F18 | Pod list partial/continue expired | list metadata | pending/BLOCKED, no effect | retained |
| F19 | watch closed | exact relist | pending | retained |
| F20 | owner-reference invalid | K8s join | conflict/BLOCKED | retained |
| F21 | PodUID/ordinal duplicate | canonical inventory | conflict/BLOCKED | retained |
| F22 | Holder Boot/UID drift | independent join | conflict/BLOCKED | retained |
| F23 | placement snapshot changes | end token revalidate | pending/BLOCKED | retained |
| F24 | unrelated active placement invalid | full inventory validation | unsafe/BLOCKED | retained |
| F25 | exclusion CAS conflict | exact Get | adopt exact or BLOCKED | retained |
| F26 | exclusion response loss | exact owner/revision read | adopt exact or pending | retained |
| F27 | child Ensure before commit timeout | exact GetPlan absent | same request retry | retained |
| F28 | child Ensure apply-then-unknown | exact GetPlan match | adopt | retained |
| F29 | child evidence missing/malformed | exact Get/validate | BLOCKED | retained |
| F30 | child nonterminal at safe termination | current child read | unsafe | retained |
| F31 | child stop response loss | terminal record+restart index | exact replay | retained |
| F32 | child recovery active | coordinator active record | unsafe | retained |
| F33 | Manager transition race | transition/recovery fence | unsafe/BLOCKED | retained |
| F34 | quiescence acquire returns fence+error | bounded WithoutCancel release | unsafe | retained |
| F35 | two Controllers acquire | fencing epoch | one winner, stale rejected | retained |
| F36 | Controller crash with active K8s op | durable operation record | exact outcome recovery | retained |
| F37 | fence expires during proof | revalidate/repository TIME | PROOF_EXPIRED | retained |
| F38 | proof/receipt corruption | recompute/reconstruct | ErrCorrupt | retained |
| F39 | terminal commit before execution timeout | exact Plan remains BLOCKED | retry after fresh proof | retained |
| F40 | terminal apply then response loss | exact TerminationID/checkpoint/proof | ALREADY_COMMITTED | released exactly once |
| F41 | terminal revision conflict | current Plan read | no rebase | retained |
| F42 | fence release response loss after commit | exact terminal+fence read | recovery release | already released in repo |
| F43 | third-party replicas mutation | RV/audit/final revalidate | evidence invalid/BLOCKED | retained |
| F44 | off mode constructor called | zero-call test | fail test/reject release | N/A |
| F45 | perf/runtime concurrent start | common interlock + both lock protocols | one actor starts；other exits 75 | retained |
| F46 | mutation lease acquire apply then response loss | exact lease owner/nonce read | adopt same fence or conflict | retained |
| F47 | mutation lease renew apply then response loss | exact lease expiry/fence read | continue only exact renewed lease | retained |
| F48 | active-operation register response loss | deterministic operation ID read | adopt registration before effect | retained |
| F49 | operation completion/removal response loss | exact operation terminal/result read | replay removal/recover | retained |
| F50 | gate/active record corruption | exact schema/checksum | unsafe，zero-write | retained |
| F51 | crash after OPEN->CLOSING | gate recovery state | resolve active set，never auto OPEN | retained |
| F52 | crash after active set empty before EXCLUSIVE | exact gate owner/epoch read | resume acquire or conflict | retained |
| F53 | crash while EXCLUSIVE | expiry -> RECOVERY_REQUIRED | inspect Plan/children/ops before OPEN | retained/released only if terminal |
| F54 | RoutePlan stop CAS conflict | exact child/restart/Manager read | unsafe，no fabricated stop | retained |
| F55 | admission/audit control unavailable | health/preflight | A5 blocked；no qualification | retained |
| F56 | bare formal lock protocol violation mid-mutation | periodic detection | stop new effects，resolve current unknown，invalidate run | retained |
| F57 | 已授权 scale 请求延迟/排队，recovery 读到旧 UID/RV/replicas | 未决 operation record | 必须先提交带唯一 nonce 的 RV 取消屏障写，**且 exact 校验返回 RV≠V0**，才可关闭 operation | retained |
| F58 | RV 屏障写 outcome unknown | exact GET 重读 RV/replicas | operation 保持未决，不得 terminalize | retained |
| F58a | 屏障写返回成功但 RV **未**前进（重放同内容 payload，apiserver 跳过 etcd 写） | 比较响应 RV 与 V0 | 视同 outcome unknown：operation 保持未决，换新 nonce 重试；**绝不**据此关闭 operation | retained |
| F58b | 屏障 nonce 在 durable operation record 写入前进程崩溃 | recovery 读 operation record 无 nonce | 生成新 nonce 重发屏障；旧屏障即使已提交也只是多推进一次 RV，无副作用 | retained |
| F58c.1 | 屏障 operation 未决；exact GET 读到 annotation nonce **等于**持久化 nonce | 只读 GET | 屏障 operation 关闭为 COMMITTED；scale operation 由同一次 GET 的 UID/RV/`spec.replicas` 按步骤 4 分类。两者**都**解析后 active set 才可能为空 | released **仅当**步骤 4 未落在 F58d/F58e/F58f 任一冲突分支；否则 retained |
| F58c.2 | 屏障 operation 未决；nonce **不等**且当前 RV **==** V0 | 只读 GET | 屏障 operation 关闭为 NOT_COMMITTED，并**立即注册一个新的**屏障 operation（新 nonce）重发。scale operation **仍未决** | **retained**。active set 非空（旧屏障关闭的同时新屏障入集），不得进入 EXCLUSIVE |
| F58c.3 | 屏障 operation 未决；nonce **不等**且当前 RV **≠** V0 | 只读 GET | 屏障 operation 关闭为 `SUPERSEDED`（安全属性只取决于 RV≠V0，与谁推进无关）；scale operation 回步骤 5 重新分类，**仍未决** | **retained**。scale operation 未解析前 active set 不空 |
| F58c.4 | 屏障 operation 未决；exact GET 失败 | 传输层/apiserver 错误 | 屏障 operation 与 scale operation **均**保持未决。只读判据，不得靠再发一次写来解析（违者属 §17 停止条件） | retained |
| F58d | 屏障写返回 200、UID 相符、RV≠V0，但响应对象 `spec.replicas` 已等于 desired | 步骤 4 的 (c) 项复核 | scale **已提交**，按 adopted committed 分类；**不得**关闭为 `NOT_COMMITTED`，否则 §8.3 会据此解除 exclusion 并把 shard 迁入正被删除的 Pod | retained |
| F58e | 屏障写返回 200、UID 与 U 不同 | 步骤 4 的 (b) 项复核，**先于** (a)(c) | workload 已被重建，conflict / BLOCKED；屏障对新对象无意义 | retained |
| F58f | 屏障写返回 200、UID 相符、RV≠V0，但 `spec.replicas` 既非 expected 也非 desired | 步骤 4 的 (c) 项第三子分支 | 第三方改写（§4.3 / F43）。**不得**归入 adopted committed，否则 exclusion 会被永久锁死（与 F61b 同一危害类别）；归入 unknown，conflict / BLOCKED | retained |
| F58g | `resolve 屏障 operation` 的 exact GET 连续失败 | 连续失败计数达 `barrier_resolve_escalation_attempts` | 屏障 operation 保持未决，active set 不为空，**不得**进入 EXCLUSIVE；置位 operator-visible `DEGRADED_BARRIER_UNRESOLVED`（非终态，不改变任何判据）。不得因重试次数用尽而关闭 operation | retained |
| F59 | exclusion release CAS 冲突或非 owner 调用（同一 boot 代内） | exact owner/revision 比较 | 拒绝，不得解除他人 exclusion；Plan 转 BLOCKED | retained |
| F60 | exclusion release response loss | exact owner/revision read | `ALREADY_RELEASED` 或重放；不得盲目重发 | retained |
| F61 | victim 仍存活、同 boot，Plan 准备 terminal 且 exclusion 未解除 | terminal 前置检查 | 禁止 terminal；先解除，解除 unknown 才保持 BLOCKED | retained |
| F61a | 释放时 `current_holder` 返回 `BOOT_MISMATCH`（victim 已换 boot） | exact Holder capture | `RELEASED_BY_BOOT_ROLLOVER`，不写、允许 terminal；旧 exclusion 已惰性 | released |
| F61b | 释放时 `current_holder` 返回 `NOT_FOUND`（含 Holder 记录 TTL 失效） | exact Holder capture | **禁止 terminal**；保持 BLOCKED 并按退避重试 capture。记录缺失是证据缺失而非 victim 消失的证明：Holder 记录有 3 分钟 TTL、exclusion 记录无 TTL，同 boot 可在 TTL 失效后重新注册并让 exclusion 恢复生效（round-4 P0） | retained |
| F61c | scale 写结果未决时尝试解除 exclusion | §7.2 屏障未给出 exact NOT_COMMITTED | 禁止解除；保留 exclusion，保持 BLOCKED，避免把 shard 迁入正被删除的 Pod | retained |
| F61d | boot 滚动后另一 ScalePlan 对同一 holderID 合法 exclude，原 owner 记录被覆盖 | 原 owner capture 得到 `BOOT_MISMATCH` | 走 F61a，不与新 owner 争抢；owner 独占性只在单个 boot 代内成立 | released |
| F61e | victim 心跳中断使 Holder 记录过期，随后以**同一 boot** 重新注册 | 重试 capture 由 `NOT_FOUND` 转为 `OK` | 走正常 owner CAS 解除；F61b 期间未产生任何写，无需补偿。这是 F61b 的**若干**出口之一，**不是**保证——Pod 可能因 CrashLoopBackOff、不可调度、配额被抢占而无限期不注册（§8.3 活性段） | released |
| F61f | `NOT_FOUND`/`MALFORMED`/`CROSS_SLOT`/unknown 持续，Pod 无限期不注册 | 连续次数达 `exclusion_release_escalation_attempts` 且累计超 `exclusion_release_escalation_min_wall_ms` | 置位 operator-visible `DEGRADED_EXCLUSION_UNRELEASED`。**非终态**：Plan 仍 BLOCKED，ownership 仍保留，SP-EXCL-1~3 判据不变。§9.4 `TerminateBlockedScalePlan` 对该状态**仍然禁止** | retained |
| F61g | 释放 capture 返回 `MALFORMED` 或 `CROSS_SLOT` | 脚本返回码 exact 匹配 | 同 `NOT_FOUND` 处置：禁止 terminal、BLOCKED + 重试。`MALFORMED` 另按 §6.4 触发 corruption 判定 | retained |
| F62 | corrupt latch 已置位 | 每个 mutation 第一阶段读 latch | 全部 mutation 零写入返回 `REALM_CORRUPT_LATCHED` | retained |
| F62a | 只读 `AuditPage` 在 touched-key 集合**之外**发现 corruption，检测者不持 mutation lease | 无——CF-3 删除通道 B 后该路径不存在 | **只能返回报告，latch 保持 absent，后续 mutation 继续被允许**。这是一条**知情接受的缺口**而非受控威胁：逐操作校验的范围有界、不扫全 realm，因此这类 corruption 也不会被后续 mutation 重新发现。缺口在 `AuditPage` 获得第一个真实运行者时变为实际风险，届时必须重新决定（§4.4） | **accepted gap** |
| F62b | 任何不持有效 mutation lease 者尝试置位 latch（含持 stale fence 的旧 Controller） | 通道 A 的 lease 校验失败 | 零写入拒绝，realm 保持可用。CF-3 后主语可以写宽——通道 B 存在时不能这样写，否则会把 fail-open 固化成断言 | retained |
| F63 | audit 期间 store epoch 漂移 | pin start epoch + 每 page 重校验 | 返回 conflict，不得报告 clean，必须重跑 | retained |
| F64 | §6.13 整数字段达到域上限 | 解析期上限检查 | 零写入 `<FIELD>_EXHAUSTED`，不得 wrap/reset | retained |
| F65 | 两个 Controller 同时执行 snapshot `Bootstrap` | 独立 startup token + durable operation record | 一个胜出，另一个 conflict；不得无 token bootstrap | retained |
| F66 | complete capture 期间持续发生 holder-side same-generation renew | lease epoch / assignment revision 未变 | capture 仍成立；若 epoch/revision 变化则失效 | retained |

每个 A0.5/A1/A1R/A2/A3/A4 finding 必须映射到至少一个 deterministic test；不允许只写人工 checklist。

---

## 11. Writer、Worktree 与 Write Set 计划

## 11.1 写集推导方法（S1）

round-2 的 P1-A、P1-D 与 round-2.5 自查的 X1、X2 是同一个缺陷的四个实例：写集是按架构意图自顶向下列出的，不是从代码里实际存在的 mutation 入口自底向上推导的。

round-3 复审证明 round-2.5 的第一版 S1 漏了两处（`internal/routecontrol/holder_registry.go`、`cmd/cap-static-route-init/main.go`）；round-4 复审证明 round-3 的第二版 **仍然漏了两处**（`internal/testkit/capstaticroute/placement_permit.go`、`internal/storage/redis/routecontrol/transition_store.go`），并且据一条有缺陷的检索把 §5.3 改成了错的。四轮下来根因始终是方法本身，不是执行时的疏忽：

```text
缺陷 1（round-3 发现）：只 rg 了「接口/方法名」，没有枚举「接口的全部实现者」。
缺陷 2（round-3 发现）：检索命令一律限定 internal/，把 cmd/ 整体排除在外。
缺陷 3（round-4 发现）：按【接口名】检索找实现者，在 Go 里必然漏。
        Go 的接口满足是隐式的，实现者文件可以完全不出现接口名。
        internal/testkit/capstaticroute/placement_permit.go 实现了
        PlacementExclusionController 的全部三个方法，而该标识符在文件里
        一次都没有出现——round-3 加了「枚举全部实现者」这句话，但执行方式
        仍是 rg 接口名，所以这一步在文字上有、在执行上没有。
缺陷 4（round-4 发现）：检索大小写敏感，漏掉导出/未导出包装与转发层。
        rg 'enterPlanMutation' 只命中未导出方法（确实只有一个调用者），
        漏掉 Executor 实际调用的导出包装 EnterPlanMutation，
        于是得出「七个入口一个都没进 barrier」这个与源码相反的结论，
        并据此废弃了一条其实成立的 realm-global 理由、反向重估了 A0.5 工作量。
```

因此本节的推导步骤按 round-4 修订如下，第 3 步被实质改写，第 4 步扩大作用域：

```text
1. 枚举本文档冻结的每一条合同
2. 对每条合同 rg 出它要触及的接口/类型/方法，记录 exact 文件与行号
3. 【round-4 改写】实现者与调用者的枚举必须是【结构化】的，不得依赖名称检索：
   3a. 对每个接口，取其【方法集】，对每个方法名在全仓检索同名 receiver 方法
       （rg -n 'func \([^)]+\) <Method>\('），再逐个比对签名是否满足该接口；
       不得只检索接口类型名
   3b. 对每个方法，枚举全部调用者，且检索必须【大小写不敏感或显式并列】
       导出与未导出两种拼写，并沿转发层继续向上枚举（A 调 B、B 调目标，
       则 A 也是调用者）
   3c. 有条件时以编译期断言复核：为每个接口在其定义包写
       var _ Iface = (*Impl)(nil)，编译不过即枚举不全
4. 【round-3 新增，round-4 扩大】检索作用域必须是整个仓库：internal/、cmd/、
   以及非 _test.go 的 testkit；【且 _test.go 也必须纳入实现者枚举】——
   接口新增方法会让测试文件里的实现者一同编译失败，它们同样需要归属
5. 断言每个 exact 路径恰好属于一个阶段的 Owned Write Set（不重叠）
6. 断言没有任何被合同触及的路径处于"无人拥有"状态（全覆盖）
7. 对确定不修改的路径，进入 §11.4 显式不修改清单，并写明它为什么不需要修改；
   【round-4 新增】若该清单条目的断言方式写的是"某阶段测试"，则该测试文件
   所在路径必须真的在那个阶段的写集里，否则断言无处可写
```

本次审计使用的检索（在 worktree 内可复算，全部为全仓作用域）：

```text
# 方法集枚举（缺陷 3 的修法：按方法名找 receiver，不按接口名）
rg -n 'func \([^)]+\) (GetPlacementExclusion|ExcludePlacement|ValidatePlacementTarget)\(' internal/ cmd/
rg -n 'func \([^)]+\) (Bootstrap|CompareAndSwap|Load|ListIncomplete)\(' internal/ cmd/
rg -n 'func \([^)]+\) (LoadTransitionRecoveries|LoadTransitionTombstones)\(' internal/ cmd/

# 调用者枚举（缺陷 4 的修法：大小写不敏感 + 沿转发层向上）
rg -ni 'enterPlanMutation' internal/ cmd/
rg -n 'RenewShardAuthority|ShardAuthorityCallerHolder' internal/ cmd/
rg -n 'NewSnapshotStore|\.Bootstrap\(|\.CompareAndSwap\(' internal/ cmd/

# 方法面枚举
rg -n 'func \(m \*RouteAssignmentManager\) [A-Z]' internal/actorrouting/authority/manager.go
rg -n 'func \(p \*DurableSnapshotPublisher\) [A-Z]' internal/routecontrol/durable_publisher.go
rg -n 'func \(s \*PlanStore\) [A-Z]' internal/storage/redis/routeplan/
rg -n 'func \(s \*(SnapshotStore|TransitionStore)\) [A-Z]' internal/storage/redis/routecontrol/
```

注意 `--glob '!*_test.go'` 已从实现者枚举中移除（第 4 步）：round-4 的 `placement_permit_test.go` 与 round-3 的 `placement_permit.go` 是同一个缺陷在测试文件与非测试文件上的两次发作。任何新增合同都必须同时新增覆盖其自身包的检索。

## 11.2 合同 → exact 路径 → 阶段（审计结果）

| # | 合同 | 需要改动的 exact 路径 | 阶段 | 源码核对依据 |
|---|---|---|---|---|
| C01 | sealed blocked-termination + ProductionRuntime facade（§5.2） | `internal/scaleplan/{repository,blocked_termination,executor,ports,memory_repository}.go` + `*_test.go` | A0.5 | 五个文件均存在，现为 package-private 接缝。`memory_repository.go` 是 `ScalePlanRepository`（`repository.go:49`）与 `blockedTerminationRepository`（`blocked_termination.go:25`）的**唯一**实现者：`repositoryIdentity`:52、`enterPlanMutation`:54、`commitBlockedTermination`:150、`ListRecoverable`:211（round-5 P1 补入） |
| C02 | PlanMutationGuard / lease-bound derived context（§5.3） | `internal/scaleplan/{executor,repository,memory_repository}.go` | A0.5 | 同上。接缝签名一改，唯一实现者必改 |
| C03 | ScalePlan durable repository（§6） | `internal/storage/redis/scaleplan/**`、`internal/scaleplan/codec/**`、`internal/testkit/scaleplanstore/**` | A1 | 三者均为新建，当前不存在 |
| C04 | Kubernetes scaler / identity snapshot / RV 取消屏障（§7） | `internal/platform/kubernetes/scaleplan/**`、`internal/testkit/kubernetes/scaleplan/**` | A2 | `internal/platform/` 当前不存在，全部新建 |
| C05 | RoutePlan durable-stop primitive 与合同（§8.5） | `internal/routeplan/{executor,model,store}.go` + `*_test.go` | A0.5 | 存在；`store.go` 现只有 Load/CAS/ListIncomplete |
| C06 | RoutePlan durable-stop 持久层（§8.5） | `internal/storage/redis/routeplan/{plan_store,scripts}.go` + test | **A1R** | `plan_store.go:128/226/325` 只有 Load/CAS/ListIncomplete，无终态原子入口 |
| C07 | snapshot `Bootstrap` fence + operation record（§8.6） | `internal/routecontrol/{durable_publisher,snapshot_store}.go` + test + `internal/routecontrol/transport/*_test.go` | A0.5 | `durable_publisher.go:117` Bootstrap；`snapshot_store.go:42` `RouteSnapshotBootstrapper` 接口。按方法集枚举出第三个实现者 `handshakeRaceStore`（`transport/watch_handshake_test.go:164/174` 的 `LoadCurrent`/`CompareAndSwap`），该类型全文未出现接口名，任何按名检索都会漏（round-5 P1） |
| C08 | snapshot `Bootstrap` 持久层（§8.6） | `internal/storage/redis/routecontrol/snapshot_store.go` + test | **A1R** | `snapshot_store.go:179` Bootstrap 实现 |
| C09 | placement exclusion 释放 — 接口/全部实现者/Manager（§8.3） | 见下方 C09 实现者全表 | A0.5 | `placement_exclusion.go:47-50` 定义 `PlacementExclusionStore`（2 方法）、`:55-59` 定义 `PlacementExclusionController`（3 方法）；`manager.go:121` 消费 Controller |
| C10 | placement exclusion 释放 — 持久层（§8.3） | `internal/storage/redis/routecontrol/{holder_store,holder_scripts}.go` + test | **A1R** | `ScriptGetPlacementExclusion` 位于 holder store；本版本**不改记录形状**（§8.3），只新增 release 脚本 |
| C11 | ControlMutationCoordinator / nested token（§8.6） | seam：`internal/scaleplan/ports.go`（A0.5）；实现：`internal/scaleplan/adapters/control/**`（A3，新建） | A0.5 + A3 | `adapters/` 当前不存在 |
| C12 | Manager writer token 校验（§8.6） | `internal/actorrouting/authority/manager.go` + test | A0.5 | `manager.go:1003/1102/1360/1370/1378/1399/1502/1513/2923/2933` 为全部 mutating 入口 |
| C13 | complete capture 的 lease epoch / assignment revision 读取表面（§8.2） | **判定 SUFFICIENT，无 A1R read-surface delta**：`internal/storage/redis/authority/{reader,model_read}.go`、`internal/storage/authoritystore/**` 保持不变 | A0.5 已判定 | 两趟不重叠全量读 bracket 每 shard 单调 `AssignmentRevision` / `WriterEpoch` / `LeaseEpoch`，并比较 `ActiveCardinality`；`TestAuthorityReadSurfaceExposesTheComparedQuantities` 与 `TestAuthorityEpochsAreMonotonicAcrossUnassign` 固定前提 |
| C14 | holder-side renew liveness 豁免（§8.6） | 断言测试位于 A0.5 写集：`internal/testkit/capstaticroute/initializer_test.go`、`cmd/cap-redis/*_test.go`（round-4 P1 补入，此前断言无处可写） | A0.5（仅测试） | **八个**调用点、六个文件，见 §8.6，全部 `ShardAuthorityCallerHolder`（round-4 P2 更正，此前写作"六个调用点"） |
| C15 | off/manual composition（§9） | seam：`cmd/route-controller/plan_runtime.go`（A0.5）；其余 `cmd/route-controller/**`、routectl、mode config、render（A4） | A0.5 + A4 | `cmd/route-controller/` 现有 main.go / plan_runtime.go / lease_projection.go / compose_test.go / main_test.go / lease_projection_test.go（共六个，round-3 P2-4 更正） |
| C17 | snapshot `Bootstrap` 的第二个写者（§8.6） | `cmd/cap-static-route-init/main.go` + test | **A1R** | round-3 P1-2：`main.go:245` 自行 `NewSnapshotStore`、`:269` `CompareAndSwap`、`:271` `Bootstrap`，完全绕过 `DurableSnapshotPublisher`。C08 给 `SnapshotStore.Bootstrap`/`CompareAndSwap` 加 fence 参数必然改坏此文件，故必须与 C08 同阶段同 writer |
| C16 | 各阶段 testkit | `internal/testkit/scaleplan/**`（A0.5，已存在 9 个 fake）、`internal/testkit/scaleplanstore/**`（A1）、`internal/testkit/kubernetes/scaleplan/**`（A2）、`internal/testkit/scaleplancontrol/**`（A3） | 各自阶段 | 除 `testkit/scaleplan/` 外均为新建 |
| C18 | transition quiescence 枚举原语（§8.6） | 接口侧 `internal/actorrouting/authority/manager.go` + `manager_test.go`（A0.5）；Redis 实现 `internal/storage/redis/routecontrol/transition_store.go` + test（**A1R**，A0.5 持 conformance 编辑权） | A0.5 + A1R | round-4 P1：§8.6 把 `LoadTransitionRecoveries`/`LoadTransitionTombstones` 冻结为全称 quiescence 的证据源，但该文件此前不在任何一行。实现者三处：`transition_store.go:96/299`（A1R）、`manager.go:313/363` `MemoryTransitionTombstoneStore`（A0.5）、`manager_test.go:1528/1543` `fullTombstoneStore`（A0.5）。**两个原语都是 store-global、无 scope 形参**（round-5），因此基数断言的作用域是整个 store 而非单个 realm/domain；若 A1R 判定需要 scope，属接口变更，回 A0 amendment |

**C09 实现者全表（round-4 按 §11.1 第 3 步结构化枚举重算）。** 上一版称 `holder_registry.go` 是"唯一实现者"，该断言为假——按接口**名**检索在 Go 里必然漏掉隐式实现者。按**方法集**在全仓枚举 `GetPlacementExclusion` / `ExcludePlacement` / `ValidatePlacementTarget` 的 receiver，结果如下：

| 实现者 exact 路径 | 类型 | 满足的接口 | 阶段 | 备注 |
|---|---|---|---|---|
| `internal/routecontrol/holder_registry.go:593/622/664` | `HolderRegistry` | Controller（3/3） | A0.5 | 生产实现，round-3 P1-1 补入 |
| `internal/routecontrol/holder_registry_test.go:89/109` | `memoryHolderStore` | Store（2/2） | A0.5 | |
| `internal/actorrouting/authority/manager.go:1387/1399` | `RouteAssignmentManager` | Store（2/2），转发给 Controller | A0.5 | 同时是消费者（`:121`） |
| `internal/actorrouting/authority/manager_test.go:71/78/91` | `fakePlacementExclusions` | Controller（3/3） | A0.5 | |
| `internal/actorrouting/authority/placement_permit_test.go:23/29/41` | `allowAllPlacementExclusions` | Controller（3/3） | A0.5 | **round-4 P2 补入**，§8.1 此前按文件名只列了 `manager*.go` |
| `internal/routeplan/placement_permit_test.go:15/18/22` | `routePlanTestPlacementExclusions` | Controller（3/3） | A0.5 | 由 C05 的 `internal/routeplan/*_test.go` 覆盖 |
| `internal/testkit/capstaticroute/placement_permit.go:23/28/38` | `allowAllPlacementExclusions` | Controller（3/3） | A0.5 | **round-4 P1 补入**。非 `_test.go`，被 `initializer.go:59` 用于构造真实 `RouteAssignmentManager`，不是死代码；文件内**一次都没出现过接口名** |
| `internal/storage/redis/routecontrol/holder_store.go:472/504` | `HolderStore` | Store（2/2） | **A1R** | 由 C10 覆盖 |

给 `PlacementExclusionController` 新增 `ReleasePlacementExclusionForOwner` 会让上表除 A1R 一行外的**全部** Controller 实现者编译失败，因此它们必须全部在 A0.5 写集内。C09 的 exact 路径列即为上表除最后一行外的并集。

C13 已于 A0.5 判定为 **SUFFICIENT**，无 A1R read-surface delta。这里不是声称 Redis 提供跨 shard 原子快照，而是使用两趟严格串行、互不重叠的全量读取做 bracket：每个 shard 的 `AssignmentRevision` / `WriterEpoch` / `LeaseEpoch` 前后必须一致，`ActiveCardinality` 也必须一致。上述 epoch 单调，且 unassign 递增而不删除权威记录，因此两趟读取之间存在一个可线性化的公共稳定区间。实现不得把逐 shard epoch 换成求和，也不得省略 active cardinality；若 unassign 将来改为删除记录，C13 必须重开。`internal/storage/redis/authority/**` 与 `internal/storage/authoritystore/**` 在 A0.5 保持未修改。

**C18 的 context 形参问题（round-4 P1 的第二半）。** `LoadTransitionRecoveries` / `LoadTransitionTombstones` 当前签名**不接受** `context.Context`（`transition_store.go:96`、`:299` 内部用自带的 `operationContext()`；`manager.go:167`、`:230` 的接口方法同样无 ctx）。这与 §5.3 的绝对约束冲突——"Executor 的全部受保护 read/effect/commit 必须使用 `guard.Context()`"。在当前签名下，quiescence 证据读取**结构上无法**接受 guard context，mutation lease 丢失时不会被取消，于是一个已丢租约的 Controller 仍可读出"集合为空"并据此进入 EXCLUSIVE。冻结决定：**A0.5 必须为这两个方法加 `ctx context.Context` 首参并改造三个实现者**，不接受"这条路径豁免 §5.3"。豁免会让 §8.6 的 quiescence 证据成为 §5.3 唯一的例外，而它恰好是最需要 fence 的那次读取。

## 11.3 覆盖性与不重叠断言

```text
partition_disjoint=true      // C01..C18 中没有任何 exact 路径出现在两个阶段
partition_complete=true      // 没有被合同触及但无人拥有的路径
unowned_paths_closed_round2_5=4
unowned_paths_closed_round3=2
unowned_paths_closed_round4=2
unmodified_paths_added_round3=2
unmodified_paths_promoted_to_writeset_round4=2   // §11.4 两条断言此前无处可写
```

round-2.5 关闭的四处：

```text
P1-A  internal/storage/redis/routeplan/**                     -> C06 / A1R
X2-a  internal/storage/redis/routecontrol/snapshot_store.go   -> C08 / A1R
X2-b  internal/storage/redis/routecontrol/holder_store.go     -> C10 / A1R
X2-c  internal/routecontrol/snapshot_store.go                 -> C07 / A0.5
```

round-3 复审发现并在本轮关闭的第五、第六处：

```text
P1-1  internal/routecontrol/holder_registry.go   -> C09 / A0.5
      唯一实现 PlacementExclusionController 的类型。给接口加 ReleaseForOwner
      会直接编译失败，除非同时改它；而它此前既不在任何写集，也不在 §11.4，
      还被 §8.1 列入 A3 Forbidden。
P1-2  cmd/cap-static-route-init/main.go          -> C17 / A1R
      第二个直连 SnapshotStore.Bootstrap / CompareAndSwap 的写者。
```

**A3 Forbidden 与 C09 的关系已澄清**：`internal/routecontrol/**` 对 A3 仍然全域禁止；C09 把其中两个 exact 文件（`placement_exclusion.go`、`holder_registry.go`）分配给 A0.5，二者不冲突——禁止的是 A3，拥有的是 A0.5。

两处缺口的共同根因是 §11.1 缺陷 1 与缺陷 2（未枚举接口实现者、检索作用域限定 `internal/`），方法层修订见 §11.1。

round-4 复审发现并在本轮关闭的第七、第八处：

```text
R4-03  internal/testkit/capstaticroute/placement_permit.go  -> C09 / A0.5
       PlacementExclusionController 的第二个非 _test.go 实现者，被
       initializer.go:59 用于构造真实 RouteAssignmentManager。文件内一次都
       没出现过接口名，因此按接口名检索必然漏——与 P1-1 是同一缺陷换了目录。
R4-04  internal/storage/redis/routecontrol/transition_store.go -> C18 / A1R
       §8.6 冻结的 quiescence 证据源，此前不在 §11.2、不在 §11.4、
       也未被 C13 preflight 悬置。
```

第七处的根因是 §11.1 缺陷 3（按接口名而非方法集找实现者），第八处的根因是新增合同未同步新增覆盖自身包的检索。方法层修订见 §11.1 第 3、4 步。

**§11.4 两条断言的可执行性（round-4 P1）。** `cmd/cap-redis/scenarios_runtime.go` 与 `internal/testkit/capstaticroute/initializer.go` 两行的断言方式写的是"A0.5 测试"，但这两个包下此前没有任何路径属于任何阶段的写集，断言无处可写，A0.5 一动手就会撞上 §17 的停止条件。本轮把两个**测试文件**位置纳入 C14 的 A0.5 写集（被断言的两个源文件本身仍然不修改），使 §12.1 的 `explicit_unchanged_list_justified=true` 可满足。

`internal/storage/redis/authority/**` 与 `internal/storage/authoritystore/**` 未被判定为缺口，但也未被判定为不需修改，它们由 C13 的 preflight 决定；在该 preflight 出结论前，任何阶段都不得修改它们。

## 11.4 显式不修改清单

以下路径被合同**触及但确定不修改**，理由必须写明，且每条都要有断言测试：

| 路径 | 为什么不需要修改 | 断言方式 |
|---|---|---|
| `internal/logic/actor/authority/{dynamic_fence,lease_renew}.go` | holder-side same-generation renew 属 liveness 豁免（§8.6），不进 gate | A0.5 测试：capture 期间持续 renew 不使 capture 失效 |
| `internal/authoritycomposition/{membership_adapter,composition}.go` | 同上，同为 holder-side renew 调用点 | 同上 |
| `internal/routeplan/service.go` | coordinator token 边界在 executor，不在 gRPC 层；service 为纯转发 | A0.5 测试：不存在绕过 executor 的 mutation 路径 |
| `internal/routeplan/{codec,production_verification}.go` | durable-stop 不改变 verification 与编码语义 | A0.5 diff 断言 |
| `internal/storage/redis/authority/**` | 待 C13 preflight 判定；判定前禁止修改 | preflight 结论写入 A0.5 handoff |
| `cmd/cap-redis/scenarios_runtime.go`（`:175` 直接 `RenewShardAuthority`） | round-3 P1-4 补入。第七处直接 renew 调用点，Caller 为 Holder、TTL 4h，属隔离场景工具而非生产控制面；按 §8.6 的 holder-side renew 豁免不进 gate | A0.5 测试，**测试文件位于 `cmd/cap-redis/*_test.go`，已由 C14 纳入 A0.5 写集**（round-4 P1）：该调用不携带 coordinator token 仍可成立，且不改变 lease epoch / assignment revision |
| `internal/testkit/capstaticroute/initializer.go`（`:111` 经 `Manager.Renew`） | round-3 P1-4 补入。以 Holder caller 走 `Manager.Renew`，同属 liveness 豁免；虽在 testkit 下但非 `_test.go`，按 §11.1 第 4 步必须显式登记 | 同上，测试文件为 `internal/testkit/capstaticroute/initializer_test.go`（已存在），由 C14 纳入 A0.5 写集 |

注意同目录的 `internal/testkit/capstaticroute/placement_permit.go` **不在**本清单——它是 `PlacementExclusionController` 的实现者，接口新增方法后必须修改，已由 C09 归入 A0.5 写集。同一个包里"不修改的文件"与"必须修改的文件"并存是正常的，但两者都必须被显式登记。

## 11.5 阶段、分支与 Writer

所有分支从各自明确 reviewed base 创建；以下名称只是实施授权后的候选。

| 阶段 | 候选分支 | 主 Writer | Owned Write Set | 依赖 |
|---|---|---|---|---|
| A0.5 | `codex/scaleplan-sp3-adapter-seams-20260813` | Shared Seam Writer | §8.1 + §11.2 中标记 A0.5 的 exact files + tests/docs + §8.1 S5 列出的四个文件的 **conformance 编辑权**（seam commit 处终止） | A0 PASS + 用户授权 |
| A1 | `codex/scaleplan-sp3-repository-a1-20260813` | Repository Writer | C03 | reviewed A0.5 |
| A1R | `codex/scaleplan-sp3-controlstore-a1r-20260813` | Control-Store Writer | C06 + C08 + C10 + C17（+ C13 delta，若 preflight 判定需要） | reviewed A0.5 |
| A2 | `codex/scaleplan-sp3-kubernetes-a2-20260813` | Kubernetes Writer | C04 | reviewed A0.5 |
| A3 | `codex/scaleplan-sp3-control-a3-20260813` | Control Writer | C11 实现部分 + `internal/testkit/scaleplancontrol/**` + A3 docs；禁止 shared packages | reviewed A0.5 |
| A4 | `codex/scaleplan-sp3-composition-a4-20260813` | Integration Writer | C15 非 seam 部分、API/routectl、mode config、render/docs | reviewed A1/A1R/A2/A3 |

## 11.6 规则

- A1/A1R/A2/A3 不直接共享 writer；
- RouteAssignmentManager、RoutePlan Executor、Holder lifecycle、quiescence fence、composition 任一时刻只有一个 Writer；
- "互不重叠"约束的是**同一时刻的两个 Writer**，不是"同一文件在整个项目生命周期内只能被一个阶段碰过"。§8.1 S5 的 conformance 编辑权是时序移交：A0.5 seam commit 之前归 A0.5、之后归 A1R，任一时刻仍只有一个 Writer，不触发本条；
- A1R 修改的是 G21 运行时正在使用的持久层，必须 additive-only、不破坏既有 schema、不改变既有脚本语义与 G21 identity；
- 发现 write set 重叠立即停止，不通过 cherry-pick 手工拼接未复审语义；
- 任何阶段若发现需要修改 §11.2 未分配给自己的路径，立即停止并回到 A0 amendment 重跑 §11.1 审计，不得就地扩张写集；
- 不 Push/Merge/Rebase Integration/Tag，除非用户另行授权；
- 每个 handoff 记录 base、implementation commit、reviewed diff SHA、checks 与未执行动作。

---

## 12. 每阶段 Gate

## 12.1 A0

```text
only_two_docs_changed=true
architecture_plan=PASS
threat_model=PASS
sealed_extension_seam_defined=true
failure_matrix_complete=true
write_set_partition_complete=true          # §11.3 全覆盖
write_set_partition_disjoint=true          # §11.3 不重叠
explicit_unchanged_list_justified=true     # §11.4 每条都有理由与断言方式
preflight_registry_complete=true           # §12.7 无"未经证实的平台事实断言"
independent_review_P0=0                    # 六轮，最后两轮均为 0
carry_forward_list_complete=true           # §12.8：未关闭项逐条列出，各有 owner/判据/机械动作
carry_forward_contains_no_safety_class=true # §0 的三条安全类定义，逐条核对为零
runtime_touch=false
checkpoint=CP-SCALEPLAN-SP3-ADAPTER-PLAN
```

`independent_review=P0_0_P1_0_P2_0` 这一格在 round-6 后被上述三格取代，理由与代价见 §0 与 §20.9。**P0 判据一格未松**；放宽的只是 P1/P2 的处置方式——由"必须在文档层关闭"改为"必须进入 §12.8 并绑定实施阶段的 gate"。

A0 PASS 仍不授权代码。

## 12.2 A0.5

```text
production_seam_compiles=true
zero_value_sealed_command_rejected=true
external_backend_cannot_mint_terminal=true
repository_identity_binding=true
distributed_lease_contract=true
shared_writer_seams_frozen=true
reference_adapter_exercises_every_seam=true
renew_liveness_exemption_proven=true        # §8.6 证明义务，含 Manager.Renew 与第七/八处调用点
capture_token_excludes_lease_deadline=true  # §8.2
authority_capture_read_surface_decided=true # §12.7 C13 preflight
executor_seven_entrypoints_lease_bound=true # §5.3：七个入口逐个 before/after 行为等价。
                                            #   注意是【改造已有接缝】：七个入口本就在
                                            #   EnterPlanMutation barrier 内（round-4 更正）
mutation_scope_param_is_meaningful=true     # §5.3：当前被丢弃的 scope 形参必须打通到 lease
enter_plan_mutation_export_surface_tightened=true # §5.3：仅指 BlockedTerminationOwner 上的
                                            #   那一个（改未导出，七个调用者全在包内）。
                                            #   ProductionRuntime 上的 facade 入口保持导出，
                                            #   否则跨包 reference adapter 拿不到 guard
transition_quiescence_universal_evidence=true # §8.6：枚举+基数，非单 transition 观察
transition_enumeration_accepts_guard_context=true # §11.2 C18：两个枚举原语必须加 ctx 首参，
                                            #   否则 quiescence 证据读取无法被 lease 丢失取消
coordinator_mode_binding_enforced_at_startup=true # §8.6 三态；mode≠off 且无 coordinator 必须启动失败
compile_edge_set_measured_not_enumerated=true # §12.8 CF-1，【A0.5 的第一个动作】：
                                            #   go build/vet/test 的错误包路径去重集合
                                            #   与 §8.1 S5 七行先验表比对，差异逐条归属。
                                            #   取代旧的 cross_stage_compile_edges_enumerated——
                                            #   手工枚举连续两轮不完整，不再作为依据
conformance_edits_are_behavior_preserving=true # §8.1 S5 的 C1~C4，机械核验：
                                            #   被编辑文件的 diff 内无 Lua 字符串、无新 key 常量，
                                            #   新增方法体逐字为 ErrNotImplementedUntilA1R 哨兵。
                                            #   哨兵【声明】本身是普通 A0.5 编辑，不走 conformance 权
degraded_state_persistence_owner_assigned=true # §12.8 CF-2：model.go/state_machine.go 必须同属
                                            #   一个阶段并写入 §11.2；三种形状择一并给落选理由
auditor_principal_decided=true              # 【已满足】随通道 B 一并删除；§6.12 的 ACL 不新增
                                            #   只读审计者 principal，鉴权面不扩大
focused_50x/race/vet=true
review=P0=0，且未关闭项全部落入 §12.8 carry-forward
```

`reference_adapter_exercises_every_seam` 的含义与理由：A0.5 的写集覆盖整个路由控制面的核心，一次性改完并"永久冻结"，而此时没有任何 adapter 存在可以验证这个接缝——这是在给一个还没有消费者的接口做定稿。§8.1 自己写了失败代价：A3 发现接缝不足就要重做 A0.5 并重跑 A1/A2 兼容复审。因此 A0.5 必须在自己的写集内（`internal/testkit/scaleplan/**`）提供一个**一次性的 in-tree reference adapter**（fake/memory），按 A1/A1R/A2/A3 将来的调用方式把每条接缝都跑通：sealed terminal 路径、guard context 取消、nested token 继承、durable-stop primitive、exclusion release、Bootstrap fence。这不扩大写集，也不改变冻结纪律——`external_backend_cannot_mint_terminal` 本来就必须有第二个 package 才能证明——只是把"看起来对"变成"编得过、测得过"。

## 12.3 A1

```text
CreateOrGet/Load/CAS/ListRecoverable=PASS
terminal_retention=PASS
active_single_flight=PASS
blocked_terminal_atomic_commit=PASS
Redis_TIME_linearization=PASS
Lua_zero_write_corruption=PASS
apply_then_unknown=PASS
no_scan_keys=PASS
miniredis_only=true
lua_integer_domain_bound=PASS
corrupt_latch_blocks_every_mutation=PASS
channel_b_has_substrate_or_is_removed=true  # 【已满足，2026-08-13】通道 B 整体删除，代价记入
                                            #   §4.4「已接受缺口」，执行范围见 §21.5。
                                            #   A1 不得实现任何 audit capability 置位路径；
                                            #   latch 授权只有通道 A（有效 mutation lease）
review=0/0/0
```

## 12.3R A1R（Production Control-Store Deltas）

```text
additive_only=true                          # 不破坏既有 schema/脚本语义/G21 identity
existing_g21_tests_unchanged_and_green=true
routeplan_stop_single_script_atomic=PASS    # 终态+restart index 移除+retention 一次原子
snapshot_bootstrap_fence_persisted=PASS
exclusion_release_owner_cas=PASS
exclusion_record_shape_unchanged=PASS       # 8 字段与 integrity 输入均不变，见 §8.3
exclusion_inert_branch_is_boot_mismatch_only=PASS  # 唯一 inert 终态；见 §8.3 SP-EXCL-2
exclusion_not_found_stays_blocked=PASS      # NOT_FOUND 不得 terminal，须重试（round-4 P0）；
                                            #   有界后升级为 DEGRADED_EXCLUSION_UNRELEASED，
                                            #   该状态仍不得 terminal（round-5 R5-07）
exclusion_same_boot_reregister_restores_state=PASS  # TTL 失效后同 boot 重注册，
                                            #   GetPlacementExclusion 重新返回 excluded=1
exclusion_capture_outcomes_exhaustive=PASS  # §8.3 七行结局表逐行有测试；
                                            #   出现表外结局即 REQUIRED_FINDING
exclusion_outcome_rows_match_section_8_3=true # §16（round-6 R6-03）：抽取 §8.3 与 §16 两表
                                            #   第一列的行键序列逐字比较，不等即失败。
                                            #   双向保护：任一侧增行而另一侧未同步都会红
exclusion_malformed_and_cross_slot_stay_blocked=PASS # 与 NOT_FOUND 同一处置（F61g）
no_conformance_sentinel_remains=true        # §8.1 S5：全仓 ErrNotImplementedUntilA1R
                                            #   命中数必须为零——【声明与调用点都算】——
                                            #   哨兵不得存活到 A5
no_new_exclusion_index_or_prune=true
no_scan_keys=true
miniredis_only=true
review=0/0/0
```

## 12.4 A2

```text
fake_client_only=true
UID_RV_replicas_CAS=PASS
owner_reference_join=PASS
unknown_outcome=PASS
429/conflict/notfound/partial/watch=PASS
minimal_RBAC_review=PASS
review=0/0/0
```

## 12.5 A3

```text
complete_snapshot=PASS
exclusion_exact_mapping=PASS
routeplan_exact_get_verify=PASS
durable_child_stop=PASS
manager_recovery_fence=PASS
two_controller_simulation=PASS
fence_expiry/release_unknown=PASS
review=0/0/0
```

## 12.6 A4

```text
SCALEPLAN_MODE_off_default=true
off_zero_side_effect=PASS
off_requires_absent_coordinator=PASS        # mode=off 且 coordinator=present -> 启动失败
manual_requires_present_coordinator=PASS    # mode=manual 且 coordinator=absent -> 启动失败
manual_auth/audit=PASS
startup_recovery=PASS
restart_matrix=PASS
full_test/race/vet/build/render=PASS
external_runtime=NOT_TOUCHED
A1_A1R_A2_A3_A4_compatibility_review=0/0/0
checkpoint=CP-SCALEPLAN-SP3-CODE
```

A4 PASS 后仍需用户单独授权 A5。

## 12.7 跨阶段 Preflight Gate 登记表（S2）

round-2 的 P1-C（projected token 语义）、P2-A（Lua 数值精度）与 P0-A 的一部分（apiserver CAS 行为）有一个共同成因：把只能靠编译或运行才能证实的平台/库行为，当作冻结断言写进 A0。这类错误在写代码时几十分钟就会撞上，在文档层做对抗性评审却要好几轮才能挖出来。

本文档此前已经为这个问题发明过两次正确解法（§6.7 的 `BLOCKED_MINIREDIS_TIME_SEMANTICS`、§7.2 的 `BLOCKED_KUBERNETES_SCALE_CONTRACT`）。S2 把它一致化：**A0 不再对任何平台事实下断言，只登记"这里有一个未经证实的外部行为假设、由哪个阶段用什么方法证实、证实不了时 fail closed 成什么"**。

**round-3 P0-1 暴露了 S2 第一版的一个漏洞，规则据此加强。** 上一版只登记了 `K8S_RV_BARRIER_SEMANTICS`（"annotation patch 前进 RV"）这条**正例**，而整个屏障机制的安全性实际取决于一条**反例**——"内容相同的写不前进 RV"。正例成立并不蕴含反例不存在，于是一条决定安全性的平台事实以"未登记"的状态存活到第三轮。因此新增规则：

```text
当某个机制的安全性依赖"某操作一定会产生某效果"时，
必须同时登记"该操作在什么条件下不会产生该效果"，
并说明设计如何在那种条件下仍然 fail closed。
只登记正例视为未登记。
```

| Gate | 假设内容 | 负责阶段 | 验证方法 | 不成立时 |
|---|---|---|---|---|
| `REDIS_VERSION_TARGET` | 目标 Redis 版本为 7.2.x | A1 | 部署环境 owner 在实现授权前书面确认 | 阻塞 A1 启动 |
| `REDIS_SAME_SLOT_ATOMICITY` | 同 hash tag 的 key 保证同 slot 原子 | A1 | `AssertSameSlot` 测试 | §17 停止条件 |
| `MINIREDIS_EVAL_TIME` | miniredis 在 EVAL 内支持 `TIME` 且返回受控 server time | A1 | A1 第一项测试 | `BLOCKED_MINIREDIS_TIME_SEMANTICS` |
| `REDIS_LUA_EXECUTION_BUDGET` | 冻结上限下 Lua 执行时间满足 local p99 50ms | A1 | benchmark | 降低 bounds 并复审，不放宽 script timeout |
| `LUA_INTEGER_DOMAIN` | Lua 5.1 double 精确表示 ≤2^53-1，`string.format('%d')` 精确 | A1 | §6.10 边界测试 | 回 A0 amendment 重选算术方案 |
| `K8S_SCALE_CAS_SEMANTICS` | `/scale` 返回 UID/RV 且 `UpdateScale` 遵守 RV 前置条件 | A2 fake + A5 conformance | 自定义 reactor + 真实 apiserver | `BLOCKED_KUBERNETES_SCALE_CONTRACT` |
| `K8S_RV_BARRIER_SEMANTICS` | stale RV 的 update 返回 409；带唯一 nonce 的 annotation patch 前进 RV 但不改 generation | A2 fake + A5 conformance | 同上 | `BLOCKED_KUBERNETES_RV_BARRIER`，未决 operation 永久未决 |
| `K8S_NOOP_WRITE_DOES_NOT_ADVANCE_RV` | 结果对象与当前对象逐字节相同的写**不**递增 resourceVersion，且以 200 返回原对象 | A2 fake + A5 conformance | fake 必须建模此反例；A5 用相同 payload 连写两次，断言第二次 RV 不变 | `BLOCKED_KUBERNETES_RV_BARRIER`；本条不成立不代表可以省略 §7.2 的 RV≠V0 校验，校验在任何情况下都必须执行 |
| `K8S_POD_INDEX_LABEL` | Pod 稳定携带 `apps.kubernetes.io/pod-index` 且为 canonical int32 | A2 + A5 | 同上 | fail closed，禁止从名称猜 ordinal |
| `K8S_BOUND_TOKEN_POD_IDENTITY` | TokenReview 响应 `status.user.extra` 携带 pod-uid / pod-name | A5 conformance | 真实 TokenReview | `BLOCKED_BOUND_TOKEN_POD_IDENTITY`，改走 SPIFFE/admission cert 专项复审 |
| `K8S_PROJECTED_TOKEN_MIN_EXPIRATION` | projected token `expirationSeconds` 下限为 600 | A2 render + A5 | manifest 校验 | 按实际下限修正 §7.5 并复审 |
| `K8S_VALIDATING_ADMISSION_POLICY` | 集群支持 VAP，可把 SA 的 statefulsets patch 限制到 annotations | A5（可选加固） | 策略部署验证 | 按 §4.4 的"Namespace 内权限提升 + 全量滚动重启"表述记为已接受风险，须安全 owner 书面确认；不确认则本 gate 升为 A5 硬前置 |
| `CLIENT_GO_FAKE_CAPABILITY` | fake + reactor 可建模 RV/CAS/generation/429/watch/分页 | A2 | A2 测试 | §17 停止条件 |
| `AUTHORITY_CAPTURE_READ_SURFACE` | 现有 authority 只读表面足以取得 lease epoch + assignment revision 的一致快照 | A0.5 | 源码判定 | delta 归 A1R，并在 A1R 复审前完成 |
| `REDIS_TIME_CLOCK_SKEW` | Redis 节点间最大时钟偏差可证明且 ≤250ms | A0.5 / A3 | 部署环境证据 | proof fail closed |
| `K8S_ANNOTATION_PATCH_HONORS_RV_PRECONDITION` | 对主资源的 annotation patch 携带 `metadata.resourceVersion` 时，apiserver **强制**该前置条件，stale 即 409 | A2 fake + A5 conformance | A5：先以 V0 读取，第三方写入推进到 V1，再以 V0 为前置条件发 annotation patch，断言 409 | `BLOCKED_KUBERNETES_RV_BARRIER`。**反例**：若某 patch 类型（strategic-merge / merge / apply）不强制前置条件，则"在途 UpdateScale(V0) 先提交、屏障 patch 随后成功"会让 RV≠V0 校验通过而 scale 实际已提交；§7.2 步骤 4 的 replicas + UID 复核是针对该反例的 fail-closed 兜底，必须实现，不得因本 gate 通过而省略 |
| `EXCLUSION_RECORD_REACHABILITY` | exclusion 记录仅在 Holder 记录存在且 boot 匹配时可读写；否则 `current_holder` 先行拒绝 | A0.5 源码判定 + A1R Miniredis | 对 §8.3 结局表的**每一行**各写一条测试，不止 NOT_FOUND / BOOT_MISMATCH 两条：另含 `MALFORMED`（`:1160` 参数、`current_holder :1124/:1126`、`load_placement` nil）、`CROSS_SLOT`（`:1152`）、`REVISION_EXHAUSTED`（`:1167`，预期**不可达**，观察到即 `REQUIRED_FINDING`）、传输层失败（round-5 P2） | §8.3 的 inert 终态分支不成立，释放路径回 A0 amendment |
| `EXCLUSION_RECORD_UNREACHABILITY_IS_NOT_TERMINAL` | **反例（round-4 P0）**：`current_holder` 拒绝**不**蕴含该记录此后永远不可读写。Holder 记录 3 分钟 TTL 且 exclusion 记录无 TTL，TTL 失效后同一 boot 可重新注册并让 exclusion 恢复生效 | A1R Miniredis 集成测试 | 让 Holder 记录过期 -> 断言 `current_holder=NOT_FOUND` -> 以**同一 boot** 重新注册 -> 断言 `GetPlacementExclusion` 重新返回 `excluded=1` 与原 owner | 若实测**不能**同 boot 重注册，§8.3 可提交 amendment 把 `NOT_FOUND` 重列为 inert；在此之前 SP-EXCL-2 只接受 `BOOT_MISMATCH`。设计侧的 fail closed 已就位：`NOT_FOUND` 保持 BLOCKED 并重试 |
| `TRANSITION_QUIESCENCE_ENUMERATION` | 存在可枚举全部在途 transition 的只读原语，足以证明全称 quiescence | A0.5 源码判定 | 枚举 + 基数 + end revalidate | `BLOCKED_QUIESCENCE_EVIDENCE`，不得进入 EXCLUSIVE |
| `TRANSITION_ENUMERATION_BLIND_WINDOW` | **反例**：一个 transition 已在途、但尚未出现在 `LoadTransitionRecoveries` 返回里的时间窗。源码判定该窗口为空——`manager.go:2636-2644` 的 `reserveRecoveryLocked` 与 `:2664-2687` 的 `markAuthorityAttemptedLocked` 是**写前置**意图，落在唯一一次 AuthorityStore mutation 之前。**该判定是有条件的**（round-5 P2）：两者都经 `recoveryStoreLocked()`（`:2628-2634`），它在 `m.tombstones == nil` **或** `m.tombstones` 不满足 `TransitionRecoveryStore` 类型断言时返回 nil，此时两个函数**不写任何持久记录**即返回。`markAuthorityAttemptedLocked` 的 nil 分支更尖锐：`:2670` 只在内存 memo 上置 `authorityAttempted = true`，进程内观察者无法区分"已记录"与"未记录" | A0.5 源码判定 + A1R 故障注入 | (1) 在 `reserveRecoveryLocked` 之后、AuthorityStore mutation 之前注入崩溃，断言枚举仍能看到该 transition；(2) **构造 `recoveryStoreLocked()` 返回 nil 的 Manager**，断言 EXCLUSIVE 获取被拒而不是读出空集合通过 | 若存在非空盲窗，枚举不足以证明全称 quiescence，`BLOCKED_QUIESCENCE_EVIDENCE`。**fail-closed marker（round-5 新增）**：构造期必须强制 `tombstones != nil` 且满足 `TransitionRecoveryStore`（现役 `manager.go:641/644` 已强制，A0.5 不得放宽）；quiescence 读取路径必须**显式**检查 `recoveryStoreLocked() != nil`，为 nil 即返回 `BLOCKED_QUIESCENCE_EVIDENCE`，**不得**把 nil store 的空返回当作"集合为空"。设计侧兜底：§8.6 的 end revalidate 覆盖"枚举之后新起"的 transition，但**不**覆盖"枚举之前已在途却不可见"，故本条不成立即无兜底 |

规则：

- A0 只对本表的**完整性**负责，不对任何一格的**结论**负责；
- 任何阶段若发现新的平台事实依赖，必须先把它登记进本表并给出 fail-closed marker，才能继续实现；
- 禁止用"实现时再看"代替登记；未登记即视为 §17 的停止条件。

---

## 12.8 A0 出口的 carry-forward 清单

本节是 §0 A0 Gate 放宽条款的**唯一**兑现物。清单之外不存在"心照不宣的待办"；任何未列入本节的未关闭 finding 视同未放宽，仍然阻塞 A0 出口。

三条 carry-forward，全部来自 round-6，全部**非安全类**：

### CF-1（源 R6-01）编译边的真集合未测量 —— **已预跑，本条降级为收尾**

- **原判据**：§8.1 S5 的七行先验表是手工产物，已连续两轮被证明不完整。真集合从未被测量过。
- **本轮已完成的部分**：按用户单独授权做了一次 CF-1 预跑（只改签名、跑编译、不写实现、整树还原，回归验证 build/vet 全绿、159 个测试单元照常编译）。结果与两处方法缺陷见 §8.1 S5。真集合为 **8 个包、20 个文件**；比对先验表得 **4 处差异**，其中 `internal/routecontrol/watch_hub.go` 与 `internal/routeplan/service.go` 为全新缺口，已补入 §8.1；另两处是 S5 表已列而 §8.1 未同步的边 5、边 6（外加实测新增的边 6b），也已补入。
- **仍未做的部分**：预跑只对 **C09/C10** 做了完整的实证闭环（真加方法 → 迭代到不动点 → 还原），C07/C08 与 C18 的 edit set 来自类型探针的预测，未做同样的闭环。差异风险不高（C09/C10 的预测与实证逐文件一致），但没有被证实。
- **owner**：A0.5，仍是**第一个动作**。
- **机械动作**：§8.1 S5 修正后的迭代式循环，覆盖三组接口，跑到不动点。
- **绑定 gate**：`compile_edge_set_measured_not_enumerated=true`，本轮追加两项要求——(a) 必须区分并分别留档 break set 与 edit set；(b) 先验表与 §8.1 写集必须由**同一次**机械动作产出并比对，不接受人工誊抄。
- **不做会怎样**：A0.5 声称"seam commit 编译通过"，但从未验证测试单元；A1R 接手时才发现被打断的包不在自己写集里，退回到 round-5 的死循环。

### CF-2（源 R6-02）DEGRADED 状态无持久化落点

- **判据**：`DEGRADED_EXCLUSION_UNRELEASED`（§8.3）与 `DEGRADED_BARRIER_UNRESOLVED`（§7.2）都要求记录"连续失败次数"与"首次进入时刻"，但源码事实是：`internal/scaleplan/model.go:365-395` 的 `Plan` 结构体**没有**任何计数字段；`internal/scaleplan/state_machine.go:87-91` 禁止在 BLOCKED 期间改 `BlockedReason`，白名单里**只有一条**例外（`CHILD_ROUTEPLAN_PAUSED` → `RECOVERY_RETENTION_EXCEEDED`）。两个文件都在 §5.2 的 **A1 禁止修改**列表里，且不属于任何阶段的 Owned Write Set。
- **注意这不是"没想到"，而是设计与既有冻结冲突**：现役状态机把 BLOCKED 建模为"原因一旦确定就不再变"，而 DEGRADED 要求它变。白名单里那条既有例外说明这类改动**有先例、且是逐条白名单式的**，这是 CF-2 的解法形状。
- **owner**：A0.5（决定形状与归属）+ A1（落 codec）。
- **机械动作**：先在 §11.2 给 `model.go` / `state_machine.go` 一个明确 owner（二者必须同属一个阶段，否则字段与校验会分裂到两个 Writer）；再在三种形状中选定并论证：(a) `Plan` 新增两个计数字段——动记录形状，牵连 §6.2 与 A1 codec 的 golden；(b) 扩展 `blockedExpired` 白名单，把 DEGRADED 建模为 `BlockedReason` 的合法转移——不动结构体，但需要独立的计数载体；(c) 计数**不持久化**，只存在于 Controller 内存，DEGRADED 仅作为 metric/日志暴露——最省，但重启即丢失，需要论证这是否可接受（它只影响可见性，不影响任何安全判据，因此**很可能**可接受）。
- **绑定 gate**：`degraded_state_persistence_owner_assigned=true`（A0.5）。三种形状必须择一并写明落选理由；若选 (c)，必须显式断言"DEGRADED 不参与任何判据"这一点在 §7.2/§8.3 中已成立。
- **不做会怎样**：A1R 实现到升级路径时无处写计数，要么擅自改 A1 禁改文件（撞 §17 停止条件），要么静默去掉 DEGRADED，让 §8.3/§7.2 的有界升级论证落空。

### CF-3（源 R6-05、R6-08）corrupt-latch 通道 B 缺底座

- **判据**：§6.4 的一次性 audit capability 需要一个"已签发未撤销"集合，但 §6.5 冻结的 keyspace 里**没有**对应 key，§11.2 里没有合同拥有它，也没有定义 retention 与容量上限；脚本侧目前只描述了"查撤销"，没有"查签发"，而只查撤销意味着任何未在撤销集合里的 capability 都被接受——包括从未签发过的。§6.12 冻结的 ACL 里也**不存在**只读审计者这一 principal，而通道 B 的整个前提是"持只读 ACL 的审计者执行一次写"。
- **owner**：A1（keyspace + 脚本）；ACL principal 由 A0.5 在 §6.12 定夺。
- **机械动作**：要么补齐（新增 key、指定合同、定义 retention 与上限、脚本改为签发集合 CAS、§6.12 增加 `auditor` principal 及其 `+@read` 加单条写权限），要么**整体删除通道 B**并把代价显式记入 §4.4——通道 A 独立成立，删除通道 B 只是失去一条检出路径，不削弱任何既有属性。
- **绑定 gate**：`channel_b_has_substrate_or_is_removed=true`（A1）。二选一，不允许保留"设计上有、实现上没有"的中间态。
- **不做会怎样**：这是本清单里唯一有 fail-open 气味的一条——文档写着有两条 latch 通道，实际只有一条能用，运维按文档预期审计者能阻断写入，而实际不能。**因此它的默认处置是删除而不是补齐**：保留一条能用的通道，好过声称有两条。

---

## 12.9 carry-forward 的纪律

三条约束，防止本清单变成"永远在清单上"：

- 清单**只减不增**。A0.5 若发现新的同类问题，走 §17 停止条件回到复审，不得往本清单追加；
- 每条都必须在其 owner 阶段的 gate 上有一个**布尔量**，不是"已考虑"这种描述；
- CF-1 必须在 A0.5 的**第一个动作**完成。它是另外两条的前提——CF-2 的 owner 归属、CF-3 的合同归属，都要等真集合出来才能定稿。

---

## 13. Formal Performance 与 Runtime Mutation 机械锁

## 13.1 Formal Perf Lock

```text
/data/farm-locks/formal-perf.lock
```

只由 Perf Agent 创建/删除。每次 Go test/race/build、docker/image、Runtime/Kubernetes/Redis 命令前检查；存在则 exit 75。

存在时允许：读文档、编辑代码、git status/diff/grep、写设计、只读 Review。

存在时禁止：Go heavy command、docker/containerd、kubectl/helm/kustomize apply、redis-cli、真实 RoutePlan、部署、Runtime mutation。

## 13.2 K3s Runtime Mutation Lock

候选：

```text
/data/farm-locks/k3s-runtime-mutation.lock
```

使用 `flock -n` 持有 file descriptor，而不是普通 `touch`。持锁期间文件记录 machine-readable JSON：

```json
{
  "schema_version": "k3s-runtime-mutation-lock-v1",
  "owner_agent": "...",
  "pi_session_id": "...",
  "pid": 0,
  "run_id": "...",
  "workspace": "...",
  "namespace": "...",
  "operation": "...",
  "acquired_at": "...",
  "expires_at": "..."
}
```

另增共同 interlock：

```text
/data/farm-locks/runtime-perf-interlock.lock
```

Perf Agent 与 runtime actor 都必须先 `flock -x` 此 interlock。唯一顺序：

```text
interlock -> formal-perf state/mutation-lock decision -> long-lived owner lock
```

Runtime 获取协议：

1. `flock -x` interlock；
2. 检查 formal lock absent；
3. open + nonblocking `flock` runtime mutation lock；
4. 再检查 formal lock absent；
5. 写 metadata；
6. 释放 interlock，但整个 runtime mutation 期间保持 mutation fd；
7. 每个可取消阶段检查 formal lock；若后来出现 formal lock，只停止尚未开始的后续 mutation并安全收敛当前 unknown outcome，不能宣称回滚已发生 mutation；
8. trap 关闭 mutation fd；非 owner 不删除 file。

Perf Agent 协议必须同步修改：

1. `flock -x` interlock；
2. 尝试 nonblocking acquire runtime mutation flock；失败则不得创建 formal lock/开始窗口；
3. 在持有 interlock + runtime flock 时原子写 formal lock metadata；
4. 释放 runtime probe flock和interlock；
5. 性能窗口期间 formal lock 持有独立 flock/owner metadata；
6. 删除 formal lock也必须重新 acquire interlock，并确认 owner identity。

因此 formal lock 不会在已开始的 runtime mutation 后凭空建立。F45 的正确语义改为“interlock 阻止窗口与 runtime mutation 同时开始；若外部违规创建裸文件，停止新 mutation并将当前结果标记 invalid/unknown”，不承诺不可能的已执行副作用回滚。

A0～A4 不获取此锁，因为禁止真实 runtime mutation。

---

## 14. A5 隔离功能验证（待单独授权）

## 14.1 隔离身份

```text
namespace=farm-scaleplan-func-<run_id>
redis_environment=scaleplan-<run_id>
redis_zone=sp
realm=scaleplan-<run_id>
evidence=/data/farm-scaleplan/evidence/<run_id>/
image=registry/...@sha256:<exact-digest>
```

禁止使用 `/data/farm-l2-r2/**`、`/data/farm-perf-*/**`、当前压测 Namespace、压测 Redis identity、`:latest` 或既有 mutable tag。

优先独立 Redis 实例；若只能共享进程，必须完全独立 hash tag、ACL user 和 keyspace，并与正式性能窗口互斥。

ControllerID/BootID、HolderID/BootID、PodUID、TLS CA/cert、nonce 全部带独立 RunID。应用 ResourceQuota、低优先级 PriorityClass、CPU/memory/Pod/PVC 限制；这不使同机性能采样可信，因此仍必须在性能窗口外执行。

## 14.2 第一轮固定功能路径

```text
FARM 1 -> 2 -> 1
one shard per wave
one child RoutePlan inflight
low business traffic
```

Scale-out：

1. StatefulSet 1→2；
2. 新 Pod/UID/owner ref exact；
3. 新 Holder `READY_ZERO_SHARD`；
4. placement eligible；
5. 一个演示 FARM shard；
6. child SUCCEEDED；
7. Production Verify consistent；
8. Gate 命中新 Holder；
9. old writer success=0。

Scale-in：

1. 最高 ordinal victim；
2. durable exclusion；
3. victim 保持 SERVING/renew/source；
4. shard 迁空；
5. Registry + Placement zero shard；
6. victim `READY_ZERO_SHARD`；
7. StatefulSet 2→1；
8. victim Pod 消失；
9. no duplicate child/generation regression。

故障场景：scale response loss、每 phase Controller restart、child apply-then-unknown、victim Boot drift、workload UID replacement、third-party replicas mutation、blocked safe termination、fence release response loss。

所有时间数据：

```text
FUNCTIONAL_OBSERVATION_ONLY
NOT_CAPACITY_QUALIFIED
```

Checkpoint：`CP-MANUAL-SCALEPLAN-FUNCTIONAL`。

---

## 15. A6 拆分（待再次授权）

### A6a ScalePlan execution capacity

测量：单 wave latency、RoutePlan execution cost、repository/fence overhead、under-load correctness。当前协议仍冻结：

```text
MaxRoutePlansInflight=1
MaxShardsPerWave=1
```

A6a 只能提出候选值；改变它们需要 ScalePlan protocol vNext、兼容性 Review、实现与新的功能证据。

### A6b Autoscaling policy

扩容高水位、缩容低水位、持续窗口、cooldown 属于外部 policy controller，必须独立设计/授权，不进入命令式 ScalePlan Saga。

性能 Agent 必须在新 exact commit、image digest、config hash、RunID 上重跑：

```text
POST_SCALEPLAN_NO_ACTIVE_PLAN_BASELINE
SCALE_OUT_UNDER_LOAD
SCALE_IN_UNDER_LOAD
```

旧 PRE_SCALEPLAN_BASELINE 不能用于声称新代码无回归。

---

## 16. 测试策略

A0 不运行 Runtime/外部测试。A0.5～A4（含 A1R）在授权后仅 code-only：

```text
formal lock absent check
go test ./...
focused affected packages -count=50
go test -race affected packages
go vet affected packages
git diff --check
secret scan
Compose/K8s render (A4 only, no apply)
```

A1：in-process Miniredis、Lua atomicity、codec golden、corruption zero-write、corrupt latch、§6.13 域边界、unknown outcome。

A1R：in-process Miniredis；durable-stop 单脚本原子性、Bootstrap fence（含 `cmd/cap-static-route-init` 的同步改造）、exclusion release owner-CAS。

exclusion capture 的结局必须**逐个**建模。round-6 R6-03 记录了一次真实的漂移：本表曾停留在四行，而 §8.3 已扩为七行，两处都由同一次修订产出，且本段当时已经写着"逐字一致"。因此本轮不再依赖人工同步，改为**行键派生**——本表的行键集合定义为 §8.3 结局表第一列的**逐字复制**，本表只增加测试维度的两列：

| capture 结局（行键取自 §8.3，不得增删改） | A1R 必须断言的行为 | 不得断言 |
|---|---|---|
| `FOUND`，且 placement 记录 boot == 该 boot | 执行 owner-scoped CAS；**仅 CAS 成功**才允许 terminal；CAS 失败按 unknown 处置 | 不得在未收到 CAS 成功应答时允许 terminal |
| `BOOT_MISMATCH` | **唯一**的 inert 终态：不写、允许 terminal | — |
| `NOT_FOUND` | BLOCKED + 重试；达到 `exclusion_release_escalation_attempts` 后进入 `DEGRADED_EXCLUSION_UNRELEASED`（§8.3），仍**不**允许 terminal | 不得断言其为 inert、不得允许 terminal、不得据此释放 workload ownership |
| `MALFORMED` | unknown：BLOCKED + 重试，与 `NOT_FOUND` 同一处置；并按 §6.4 触发 corruption 判定 | 同上；不得把 corruption 判定降级为只读告警 |
| `CROSS_SLOT` | unknown：BLOCKED + 重试；测试须覆盖它是部署配置错误而非瞬态 | 同上；不得与传输层失败合并为同一断言 |
| `REVISION_EXHAUSTED` | §8.3 论证其在释放路径上不可达；A1R 必须断言**观察不到**该值，一旦观察到即 `REQUIRED_FINDING` | 不得为它写"正常处置"分支从而掩盖推理失效 |
| 传输层失败 / 超时 / 连接中断 | unknown：BLOCKED + 重试 | 同上 |

派生关系必须**机械可查**，否则它和上一版的"逐字一致"没有区别。A1R gate 增加 `exclusion_outcome_rows_match_section_8_3`：抽取两张表第一列的行键序列，逐字比较，不等即 gate 失败。这条检查同时保护反方向——§8.3 日后再增行时，本表不同步也会失败。

必须存在一条**反例测试**：构造「Holder 记录 TTL 过期但进程存活、随后以同一 BootID 重新注册」的序列，断言 ScalePlan 在该序列中**没有**terminal、exclusion 最终被 owner-CAS 解除。此外必须断言 `placement-exclusion-v1` 记录形状与 integrity 输入**逐字节不变**；并且必须重跑既有 G21 相关包的全部测试，证明 additive-only 未改变既有语义。

A2：client-go fake + reactors/fake transport；不使用真实 API server；必须建模 RV 取消屏障的正例与反例（stale RV 409、带唯一 nonce 的 annotation patch 前进 RV 不改 generation、**相同 payload 重写不前进 RV 且返回 200**）。

屏障步骤 4 的分类与 F58c 的解析同样必须**逐分支**建模，与 §7.2 逐字一致：

| 场景 | 必须断言 |
|---|---|
| UID 变化（F58e） | 先于 (a)(c) 判定，conflict / BLOCKED |
| RV == V0（F58a） | 换新 nonce 重试，**不得**关闭 operation |
| RV≠V0 且 `replicas == expected` | 关闭为 `NOT_COMMITTED` |
| RV≠V0 且 `replicas == desired`（F58d） | adopted committed，**不得**关闭为 `NOT_COMMITTED` |
| RV≠V0 且 `replicas` 为第三值（F58f） | unknown / BLOCKED，**不得**归入 adopted committed |
| F58c.1～F58c.4 | 每行**同时**断言屏障 operation 与 scale operation 的处置，**外加该行的 ownership 判定**；F58c.3 须断言屏障关闭为 `SUPERSEDED`；F58c.2/F58c.3 须断言 ownership **保留**（scale operation 仍未决），这是 round-6 R6-04 的修法所在 |
| `desired == R` 的 submission | 在注册 operation **之前**被拒（round-6 R6-09），不得走到步骤 4 才发现 (c) 前两个子分支同时命中 |
| F58c GET 持续失败（F58g） | active set 不空、不得进 EXCLUSIVE、置位 `DEGRADED_BARRIER_UNRESOLVED`，且**不得**因重试用尽而关闭 operation |

A3：fake/in-process Manager/RoutePlan/coordinator；每个 effect 前后 restart、two-controller、fence expiry、child recovery、capture drift。

A4：off-mode zero effect、manual auth、composition/recovery、goroutine leak、full repository tests。

每阶段独立 reviewer 使用当前可用最高强度模型；PASS 只接受 `P0/P1/P2=0/0/0`。

---

## 17. 停止条件

任一出现立即停止、记录 `REQUIRED_FINDING`，回 A0/SP3 Review：

```text
formal-perf.lock exists before a forbidden command
write outside Owned Write Set
need to write a path not assigned to this stage in §11.2
new platform-fact dependency discovered but not registered in §12.7
need to modify frozen protocol semantics
need to export forgeable terminal API
cannot prove Redis same-slot atomicity
cannot use authoritative Redis TIME
requires SCAN/KEYS discovery
requires external Redis secret
Kubernetes UID/owner/Boot join incomplete
cannot commit the RV cancellation barrier（缺 patch 权限或 apiserver 语义不成立）
cannot exact-verify that the barrier write advanced resourceVersion（RV≠V0 无法判定）
client-go fake cannot represent required CAS and no reactor design exists
complete placement snapshot cannot be captured
placement exclusion cannot be released owner-scoped，且 §8.3 唯一的 inert 分支（`BOOT_MISMATCH`）也不成立
exclusion capture 出现 §8.3 七行结局表之外的返回值（该表由 `getPlacementExclusionScript` 源码穷尽导出，出现表外值即说明导出有漏）
transition quiescence 无法给出全称证据（枚举原语不足）
any of the renew-exemption proof obligations in §8.6 fails
A1R cannot stay additive-only（必须破坏既有 schema 或既有脚本语义）
RoutePlan child cannot be durably stopped across restart
Manager/RoutePlan writer bypasses coordinator
Kubernetes writer set cannot be bounded
route snapshot writer set cannot be bounded（发现第三个绕过 publisher 的持久快照写者）
need to bypass Production Verification
mutable image tag required
access to performance Namespace/Redis/evidence
```

`found two unfenced Controller publishers` 已从停止条件中移除：round-3 P1-2 确认第二个写者就是 `cmd/cap-static-route-init`，它已在 §8.6 具名登记、在 §11.2 C17 分配给 A1R、并附带"隔离 namespace、禁止与 ScalePlan 同时运行"的约束。它不再是未知量。停止条件相应上移为"发现**第三个**"。

禁止在 Adapter 中“临时兼容”上述问题。

---

## 18. 计划交付物与请求授权条件

A0 只提交：

```text
docs/verification/scaleplan-sp3-production-adapter-implementation-plan.md
docs/verification/scaleplan-sp3-production-adapter-plan-status.json
```

独立 Review PASS 后状态：

```text
checkpoint=CP-SCALEPLAN-SP3-ADAPTER-PLAN
architecture_plan=PASS
threat_model=PASS
review=P0_0_P1_0_P2_0
runtime_touch=false
adapter_implementation_authorized=false
```

随后只向用户请求：

```text
A0.5 shared seam implementation authorization
A1 / A1R / A2 / A3 / A4 code-only implementation authorization
```

请求不包含 A5/A6、部署、外部 Redis/Kubernetes、镜像发布或性能窗口。

---

## 19. 最终不变声明

```text
protocol_complete=true
all_five_blockers_pass=true
sp3_0_compatibility_pass=true
adapter_plan_authored=true
adapter_plan_reviewed=false
adapter_implementation_authorized=false
production_adapter_implemented=false
external_runtime_touched=false
real_scale_out_evidence=false
real_scale_in_evidence=false
performance_sampling=false
ready_for_sp3=false
supports_30m_dau=false
```

A0 文档 PASS 仅表示生产 Adapter 已具备可实现、可审查的路线；不表示 Pod 扩缩容已实现，更不表示生产或容量资格。

---

## 20. Round-5 候选：修订记录与复审交接（2026-08-13）

### 20.1 当前结论

本节记录第四轮复审失败之后的完整修订。当前**仍不是 A0 PASS**，而是等待第五轮独立复审的候选版本：

```text
stage=A0_REVIEW_REMEDIATION
status=ROUND5_PENDING
round1=FAIL  P0/P1/P2=1/8/2
round2=FAIL  P0/P1/P2=1/4/1
round2_5_source_audit=SELF_FOUND X1/X2
round3=FAIL  P0/P1/P2=1/14/5
round4=FAIL  P0/P1/P2=1/7/3
round5=PENDING
adapter_implementation_authorized=false
runtime_touch=false
```

本轮策略是**减法加范围收窄**，不是继续加机制：删除一条 round-3 新增的 inert 分支（P0 的来源）、撤回一次基于错误 finding 的修订、把两个反复出问题的机制从"冻结完整算法"降级为"冻结安全属性与 fail-closed 默认"。净效果是 §7.2 与 §8.3 的可放宽面减少、无条件检查增加。

到目前为止只编写了 A0 架构/威胁模型计划；没有实现 A0.5/A1/A1R/A2/A3/A4，没有构建或部署，没有访问外部 Redis/Kubernetes，没有真实 `1→2→1`，没有性能采样。本轮的全部工作是修改本文档与配套 status JSON，外加只读源码审计。

### 20.2 精确身份

```text
worktree=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-sp3-adapter-plan-20260813
branch=codex/scaleplan-sp3-adapter-plan-20260813
HEAD/base=0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
reviewed_protocol_code_identity=c06ae6a552bfb3c27f8d5283dc3ae7111c1799f4
G21_runtime_identity=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361
formal_perf_lock=ABSENT
```

Git 状态是**恰好两份授权文档 untracked**：

```text
?? docs/verification/scaleplan-sp3-production-adapter-implementation-plan.md
?? docs/verification/scaleplan-sp3-production-adapter-plan-status.json
```

未执行 push、merge、rebase integration 或 tag，也尚未 commit A0 文档。接手者不得把 untracked 误判为丢失工作，也不得从其他 worktree 覆盖。

### 20.3 前两轮背景

A0 初稿覆盖 Redis repository、Kubernetes scaler、trusted identity join、placement completeness、RoutePlan durable stop、distributed quiescence、off/manual composition、A5/A6 隔离与锁协议。

针对 round-1 已写入的主要修订：

1. paired core `ProductionRuntime`，绑定唯一 production facade、BlockedTermination owner 与 approved quiescence backend；
2. mutation guard 返回 lease-bound derived context，renew loss 会 cancel operation；
3. nested control token、writer call graph 与 realm/domain-global scope；
4. Redis canonical decimal fence、store epoch、bounded touched-key validation；
5. Kubernetes v1.36/k3s v1.36.2 合同、Pod index label、pre-A5 conformance；
6. Pod-bound TokenReview 方向、domain 到单 StatefulSet mapping；
7. A0.5 与 A3 exact non-overlap write sets；
8. runtime/performance common interlock；
9. coordinator unknown-outcome/restart failure matrix 扩展。

round-2 确认上述方面有实质改进，但仍给出 FAIL：

```text
round2=FAIL
P0/P1/P2=1/4/1
reviewed_plan_sha256=e1691527c6199daed4aeaae49696a3558230e70c31f0de52c945d95de88d25d6
reviewed_status_sha256=56e51623e323a36f3122104cb0519baa42daef3cd7eec62b44b7c51eb5b85ee9
marker=CP_SCALEPLAN_SP3_ADAPTER_PLAN_FAIL
```

上述哈希是 round-2 reviewer 实际读取的文档身份。

round-3 reviewer 实际读取的文档身份：

```text
round3=FAIL
P0/P1/P2=1/14/5
reviewed_plan_sha256=cf66ea44a7eecde073ab8e1d780e773a83c1a1b24f543a956517fd3e85f45b09
reviewed_status_sha256=10f532bc9f0096436cfdf2085e48f5f81946c3e368d70e7a3155ff25a665b8f1
HEAD=0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
marker=CP_SCALEPLAN_SP3_ADAPTER_PLAN_FAIL
```

round-4 reviewer 实际读取的文档身份：

```text
round4=FAIL
P0/P1/P2=1/7/3
reviewed_plan_sha256=196f0c89ecc8df0e35838be2fe1d2910413e4b0a865afcc3d2bea0ce84c67c81
reviewed_status_sha256=05435c742f74e5f936fe94f189e81d0fe04c16dc3f3b11088cb09974338a1c35
HEAD=0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
plan_lines=2636
marker=CP_SCALEPLAN_SP3_ADAPTER_PLAN_FAIL
```

六轮计数走势与来源分布：

| 轮次 | P0 | P1 | P2 | 合计 | 缺陷的来源 |
|---|---|---|---|---|---|
| round-1 | 1 | 8 | 2 | 11 | 初稿既有缺陷 |
| round-2 | 1 | 4 | 1 | 6 | 初稿既有缺陷（P0-A） |
| round-3 | 1 | 14 | 5 | 20 | P0 由 **round-2 修法引入**（RV 屏障不校验 RV 前进） |
| round-4 | 1 | 7 | 3 | 11 | P0 由 **round-3 修法引入**（`RELEASED_BY_HOLDER_ABSENT`） |
| round-5 | 0 | 8 | 6 | 14 | P0 首次归零 |
| round-6 | 0 | 5 | 5 | 10 | P0 连续两轮归零；**5 条 P1 全部由 round-5 修法引入**，无一遗留 |

两条走势要分开读。**P0 已经收敛**：连续两轮为零，且 round-5、round-6 两位复审者都独立攻击过 §8.3 的 P0 修法（TTL 失效 + 同 boot 重注册）而未能推翻。**P1 没有收敛**：合计数在 6 到 20 之间震荡，六轮下来没有单调下降的趋势，因为每关一条都要冻结新的规格面，而新规格面又是对着未经编译验证的源码写的。

round-6 提供了判断"该不该继续"的决定性信息：它的 5 条 P1 **全部**是 round-5 修订的产物，且逐条看都是对齐误差而非设计缺陷——漏枚举编译边（`go build` 可测）、状态无处持久化（打开结构体可见）、两表行数不一致（行键比对可查）、矩阵一列过宽（拆行即可）、Redis 少一个 key（写 Lua 立刻撞上）。继续在文档层追平它们，是用最慢的手段做最容易的事。

这是 §20.8 新增 **S6（判据分层）** 的直接依据，也是 §0 A0 Gate 由 `0/0/0` 改为"P0=0 + carry-forward 清单 + 安全类清零"的全部理由。分层线画在**安全类**上，不画在难易上：round-6 的 R6-04 属安全类，本轮已关闭；其余九条进入 §12.8 或已随机械修订关闭。

本版本是 **A0.5 授权申请版**，不再是复审候选版。

### 20.4 round-2 finding 关闭记录

| finding | 关闭方式 | 落点 | 与 round-2 建议的差异 |
|---|---|---|---|
| P0-A 延迟 scale 请求越过 recovery 与 terminal release | recovery 关闭任何未决 scale operation 前必须提交 **RV 取消屏障写**；屏障提交后早于它的在途请求必然 409 | §7.2、F57/F58、§7.6 RBAC、§12.7 | round-2 推荐 admission webhook；本轮改用 apiserver 既有的乐观并发原语。屏障写与在途请求由 apiserver 在 etcd 上全序化，同样满足"API-server-visible atomic stale-request rejection"，但不引入 cluster 级 webhook、TLS 证书轮转、`failurePolicy: Fail` 的可用性耦合与 webhook 自身缓存陈旧这一新攻击面。代价是 `statefulsets: patch` 一个 verb |
| P0-A 派生 | 把"stale Controller 用**新** RV 写"拆成独立命名威胁 `STALE_CONTROLLER_KUBERNETES_WRITE_DETECTION_ONLY` | §7.2 | round-2 把两类竞态混在一起，因而推导出"必须上 webhook"。RV 屏障挡不住 stale controller，但那是另一个问题，v1 冻结为"仅检测"并写明残留窗口 |
| P1-A 无阶段拥有 Redis RoutePlan durable-stop | 新增 **A1R** 阶段，并把范围扩大到全部生产控制面持久层 delta | §2、§8.5、§11.2 C06/C08/C10、§11.5、§12.3R | round-2 只要求补 `internal/storage/redis/routeplan/**`；审计发现同类缺口共四处，故按类解决而非按例解决 |
| P1-B index checksum 与 corruption latch | 删除增量全流 SHA-256 字段；meta 只保留可精确增量维护的 `store_epoch` 与各索引 cardinality/bytes；深度校验交给 bounded `AuditPage`；新增 epoch-CAS `corrupt-latch`，每个 mutation 第一阶段必须读取并要求 absent | §6.3、§6.4、§6.6、§6.10、F62/F63 | round-2 给了"bucketed Merkle root"或"删掉 root 全量重算"两个选项，本轮选后者：在默认 1024、硬上限 1,048,576 的规模下，Merkle 需要另行冻结 bucket 函数、leaf 编码、fanout、更新键集与证明边界，新增的规范面大于收益 |
| P1-C per-Pod SA 与 StatefulSet 不兼容 | 改为每 StatefulSet 一个共享最小权限 SA + kubelet 投射卷，`expirationSeconds=600` | §7.5、§7.6、§12.7 | 补充精确值：Kubernetes 对该字段有 600 秒下限校验，round-2 写的 `<=600` 实际只有 600 一个合法取值 |
| P1-D 遗漏直接 authority renew writer 与 Bootstrap | 冻结为 **liveness 豁免**（不再是二选一），列出 exact 调用点与证明义务；`Bootstrap` 升为独立 top-level token | §8.6、§8.2、§11.4、F65/F66 | round-2 给了"豁免"或"纳入 coordinator"两个选项。源码审计显示调用点全部是 `ShardAuthorityCallerHolder`：纳入 coordinator 会在每次扩缩容 gate CLOSING 时停止全域租约续期，造成自伤性故障。因此只有豁免可行，故冻结为一并要求证明。**round-3 补充**：调用点由六处增至八处，`Manager.Renew` 亦归入豁免，Bootstrap 写者由一处增至两处 |
| P2-A Plan revision 的 Lua 算术 | 所有 Lua 内比较/自增的整数统一冻结域上限 `2^53-1`，规定解析/格式化规则，禁止 `tostring` 与数字拼接 | §6.13、§5.3、§6.4、F64 | round-2 要求做 uint64 canonical digit-string 算术。这些字段都是每次 mutation 才 +1 的单调计数器，现实生命周期内到不了 2^53；用一个显式低域上限换掉整套手写大数算术，消除的实现缺陷类远多于放弃的取值范围 |

### 20.5 round-2.5 源码自查发现的 finding

前两轮独立复审均未覆盖，由 §11.1 的机械审计发现：

| 编号 | 内容 | 证据 | 落点 |
|---|---|---|---|
| X1 | PlacementExclusion 只有"加"没有"减"：全文此前 18 处提到 exclusion 全是创建与校验，没有任何一处定义解除；源码侧接口无解除方法，CAS 前置条件显式拒绝 `ExpectedExcluded=true`。后果是失败的 scale-in 会把 victim 永久排除出 placement、永久阻塞对该 boot 的重试 | `internal/routecontrol/placement_exclusion.go:47-59`、`:88-96`、`:78-85` | §8.3、F59～F61d、§11.2 C09/C10 |
| X2 | 写集缺口不止 RoutePlan 一处，共四处无人拥有的路径 | 见 §11.3 | §11.2、§11.3 |

X1 与 §1.4 的 fail-closed 定义直接冲突：fail closed 要求"不产生新的不安全副作用"，而遗留 exclusion 产生的正是一个持久的容量副作用。

**X1 的第三条后果已被 round-3 P2-1 推翻并删除。** 前一版本还断言"记录无保留期无界增长"，并据此设计了 `created_at_ms` + bounded ZSET + `PruneExpiredExclusions` + 启用前置计数 gate。该断言与源码 key 结构不符（`holder_store.go:219-227`，key 为 `(domain, holderID)`，同一 holder 只有一条记录、原地覆盖），整套保留机制在本轮全部删除，连带关闭 round-3 的 P1-9、P1-10、P1-11。教训记入 §11.1：自查 finding 与复审 finding 一样需要逐条回到源码核对，"源码核对确认"这个标签本身不构成证据。

**X2 的四处并不完整**：round-3 复审发现第五、第六处（`internal/routecontrol/holder_registry.go`、`cmd/cap-static-route-init/main.go`），根因是 S1 方法本身的两个缺陷，已在 §11.1 修订。

### 20.6 round-3 finding 关闭记录

round-3 判定 FAIL，P0=1 / P1=14 / P2=5。按来源分类：**八条由 round-2 修订自身引入**（P0-1、P1-6～P1-13 中的多数），五条是前两轮未查出的既有缺陷（P1-1、P1-2、P1-3、P1-5、P1-14），两条是对修订的事实性纠正（P1-4、P1-12）。这个分布本身就是 S3 减法优先原则的依据。

| finding | 关闭方式 | 落点 |
|---|---|---|
| **P0-1** 屏障不校验 RV 是否真的前进 | 屏障 payload 加每次唯一的 attempt nonce（先持久化进 operation record）；屏障返回 200 后**无条件** exact 比较 RV≠V0，相等一律按 unknown 保持未决；反例登记为具名 gate | §7.2、F57/F58a/F58b、§12.7 `K8S_NOOP_WRITE_DOES_NOT_ADVANCE_RV` |
| P1-1 `holder_registry.go` 无人拥有 | 归入 C09 / A0.5；同时澄清"A3 全域禁止 `internal/routecontrol/**`"与"A0.5 拥有其中若干 exact 文件"并不冲突 | §11.2 C09、§11.3、§8.1 |
| P1-2 第二个无 fence 的 snapshot 写者 | 新增 C17 / A1R；§8.6 具名登记两个 Bootstrap 写者并给出隔离约束；§4.4 单 writer 前提补入 route snapshot 写者集合 | §8.6、§11.2 C17、§4.4、§17 |
| ~~P1-3 §5.3 源码前提错误~~ **round-4 判定该 finding 本身为假，本轮整体撤回** | round-3 据大小写敏感的 `rg 'enterPlanMutation'` 判定 Executor 未进 barrier，漏掉了导出包装 `EnterPlanMutation`；实际七个入口全部已在 barrier 内。round-3 版本的"更正"因此把 §5.3 改成了错的，并废弃了一条成立的 realm-global 理由、反向夸大了 A0.5 风险。本轮恢复 round-2 的现状描述，详见 R4-02 | §5.3、§8.1、§8.8、§11.1 缺陷 4 |
| P1-4 `Manager.Renew` 误标为控制面发起 + 调用点枚举不全 | 改归 liveness 豁免（源码强制 `ErrHolderRenewOnly`）；补入第七、第八处调用点；审计命令改全仓 | §8.6、§11.4、§11.1 |
| P1-5 `ObserveTransition` 无法证明全称 quiescence | 冻结"枚举 + 基数 + end revalidate"的全称证据；`ObserveTransition` 降级为补充证据；新增 preflight gate | §8.6、§8.5、§12.7 |
| P1-6 屏障写与 CLOSING 门禁互斥 | 冻结 `resolution-only operation` 类：仅 gate owner 可在 CLOSING 下注册，必须计入 active set 并在 EXCLUSIVE 前 exact resolve | §7.2、§8.6 |
| P1-7 释放未与 scale 结果定序 | 释放前置条件加入"scale operation 已 exact 解析为 NOT_COMMITTED 且 UID/replicas 未变"；unknown 保留 exclusion 并保持 BLOCKED | §8.3、F61c |
| P1-8 boot 漂移下 owner 可被劫持、释放不可达 | 更正 owner 独占性范围（只在单个 boot 代内成立）；新增 `RELEASED_BY_BOOT_ROLLOVER` inert 终态分支，区分"无法解除"与"无需解除"。**round-4 撤回了同时新增的 `RELEASED_BY_HOLDER_ABSENT`**：Holder 记录 TTL 失效不蕴含 victim 消失，该分支会跳过本应执行的解除并制造永久不可解除的 exclusion（round-4 P0） | §8.3、F61a/F61b/F61d/F61e |
| **P1-9 / P1-10 / P1-11 / P2-1** | **整套 exclusion 保留机制删除**。P2-1 证明"无界增长"的前提是错的（key 为 `(domain, holderID)`，同一 holder 一条记录、原地覆盖），机制失去动机；删除后 prune 判据自相矛盾（P1-9）、additive-only 不成立（P1-10）、`PREEXISTING_EXCLUSION_COUNT_ZERO` 不可证明（P1-11）三条同时消失 | §8.3、§12.3R、§12.7 |
| P1-12 latch 写入授权未定义 | **【双通道部分已被 CF-3 取代，2026-08-13；"latch 写必须被授权"仍然成立】** latch 写必须被授权；新增 stale fence 负向用例与误置位申诉路径。**round-4 修正了"只读检测者不得置位"这一条**：单通道方案让 `AuditPage` 在逐操作校验范围之外发现的 corruption 永远无法阻断后续写入（fail open），改为双通道——有效 mutation lease，**或**绑定 pinned `store_epoch` 的一次性 audit capability | §6.4、§6.10、F62a/F62b |
| P1-13 stale Controller 残留窗口界定错误 | 窗口重述为"最后可能读 RV 的时刻 → 在途请求在 apiserver 的最大存活时间"，与 proof safety margin 无关；给出两条可真正约束它的手段 | §7.2 |
| P1-14 off 模式与 token 强制的互斥 | 冻结显式三态，判定发生在**启动期**：`mode≠off` 且无 coordinator 必须启动失败，不存在"缺省即放行" | §8.6、§9.2、§12.2、§12.6 |
| P2-2 域上限边界与字段注记 | 更正"上限+1 语法拒绝"为数值拒绝；明确 `placement exclusion revision` 不适用 §6.13 规则，沿用现役实现 | §6.10、§6.13 |
| P2-3 RV 用序关系推理 | 全部改为相等性判断；adopted 判定改由 `generation` 单调性承担 | §7.2 |
| P2-4 §8.1 清单过时与方法名错误 | 补入三个遗漏文件使 exact 名副其实；更正 `ReconcileBlocked`/`OperatorFailBlocked`；更正 `cmd/route-controller/` 文件数为六 | §8.1、§5.3、§11.2 C15 |
| P2-5 `statefulsets: patch` 越权面被低估 | 按"Namespace 内权限提升 + 全量滚动重启"重新表述，要求安全 owner 按此表述书面接受 | §4.4、§7.2 |

round-3 明确**确认关闭**的：P1-B（index checksum / corruption latch，仅余 latch 授权即 P1-12）、P1-C（projected token，完全关闭）、P2-A（Lua 算术，实质关闭）。reviewer 专门尝试推翻 F66（holder-side renew 对 capture 的免疫）并**未能推翻**——`MemoryStore.RenewShardAuthority` 只在 `requestedExpiry > LeaseUntilMs` 时推进过期时间，Redis 的 `RenewShardAuthorityV2` 只 `HSET lease_expire_at_ms`，完全不动 writer_epoch / lease_epoch / assignment_revision。

### 20.7 round-4 finding 关闭记录

round-4 判定 FAIL，P0=1 / P1=7 / P2=3。P1 由 14 降至 7，写集分区在 17 条合同中的 15 条上经受住独立复算，§8.3 的十余处 Lua 行号引用无一偏移。但 **P0 连续四轮都是 1，且第三、第四轮的 P0 都由前一轮的修法引入**——这个分布是本轮范围收窄（S4）的直接依据。

| finding | 关闭方式 | 落点 |
|---|---|---|
| **R4-01（P0）** `RELEASED_BY_HOLDER_ABSENT` 把 Holder 记录 TTL 失效误读为 victim 消失 | **删除该分支**。Holder 记录 3 分钟 TTL、exclusion 记录无 TTL、同 boot 可在 TTL 失效后重注册，因此记录缺失是**可逆的证据缺失**而非终态。`NOT_FOUND` 改为保持 BLOCKED 并按退避重试；唯一 inert 终态是 `BOOT_MISMATCH`，因为只有它正面证明了新 generation 已接管。**round-5 复审确认该修法本身经得起攻击，但其活性论证"Pod 总会回来"为假并已在本轮删除**（见 R5-07） | §8.3、§1.2、F61b/F61e/F61f、§12.7 新增反例 gate |
| R4-02（P1）§5.3 的"更正"本身为假 | **撤回 round-3 P1-3 的全部修订**。七个 Executor 入口全部已在 barrier 内（六个直接调 `EnterPlanMutation`，`OperatorFailBlocked` 经 `TerminalizeBlocked` 间接）；恢复"`Submit` 在 PlanID 生成前已进 realm 范围 barrier"作为 realm-global 的第一条理由，并新增第二条（现役 barrier 本就是单个全局 channel）；A0.5 工作量按"改造已有接缝"重估，同时补入两项此前未识别的工作：让被丢弃的 scope 形参生效、收紧 `EnterPlanMutation` 的导出面 | §5.3、§8.1、§8.8、§11.1 缺陷 4 |
| R4-03（P1）`PlacementExclusionController` 的第七处无人拥有的实现者 | 按方法集重新枚举全部实现者，形成 §11.2 的 C09 实现者全表（8 个）；`internal/testkit/capstaticroute/placement_permit.go` 归入 C09 / A0.5；删除"唯一实现者"措辞；§11.4 两条断言的测试文件位置纳入 C14 的 A0.5 写集，使断言可执行 | §11.2 C09/C14、§11.3、§11.4、§8.1 |
| R4-04（P1）quiescence 证据源不属于任何阶段 | 新增合同 **C18**：接口侧归 A0.5、Redis 实现侧归 A1R；同时冻结"A0.5 必须为两个枚举原语加 `ctx` 首参"，不接受该路径豁免 §5.3 的 `guard.Context()` 约束 | §11.2 C18、§8.6、§8.1 |
| R4-05（P1）`NOT_COMMITTED` 的铸造缺 replicas 复核 | 屏障步骤 4 由"只比 RV"改为**三项 exact 校验**（RV≠V0、UID 未变、`spec.replicas == expected`），缺一按 unknown；新增 gate `K8S_ANNOTATION_PATCH_HONORS_RV_PRECONDITION` 并登记其反例；明确 (b)(c) 两项是**无条件**兜底，不因该 gate 通过而省略 | §7.2、F58d/F58e、§12.7 |
| R4-06（P1）屏障 operation 自身无解析路径 | 冻结 F58c：以 exact GET 比较 annotation nonce 关闭屏障 operation。该判据**只读、必然终止**，是 resolution-only 类的唯一终止基础；任何"靠再发一次写来解析"的实现列为 §17 停止条件 | §7.2、F58c |
| R4-07（P1）要求 lease 才能 latch 导致审计发现的 corruption fail open | **【已被 CF-3 取代，2026-08-13：通道 B 整体删除，此行仅为历史】** latch 授权改为**双通道**：有效 mutation lease，**或**绑定 detector identity + pinned `store_epoch` + evidence digest 的一次性 audit capability。区分点由"有没有 lease"改为"能否证明看到的是当前状态"，对 stale 检测者的排斥力不变；§6.10 负向用例主语由"任何无 lease 者"收紧为"stale fence 的旧 Controller"，并新增一条正向用例 | §6.4、§6.10、F62a/F62b |
| R4-08（P1）两条新 gate 仍只登记正例 | 补 `EXCLUSION_RECORD_UNREACHABILITY_IS_NOT_TERMINAL` 与 `TRANSITION_ENUMERATION_BLIND_WINDOW` 两条反例 gate，各自写明设计如何在反例成立时仍然 fail closed | §12.7 |
| R4-P2-1 C14 调用点计数陈旧 | 六 → 八个调用点、六个文件 | §11.2 C14 |
| R4-P2-2 §11.3 整段逐字重复 | 删去其一 | §11.3 |
| R4-P2-3 §8.1 对 authority 目录用具名文件而非通配 | 改为 `internal/actorrouting/authority/*_test.go`，纳入第五个 Controller 实现者 `placement_permit_test.go` | §8.1 |

round-4 明确**确认关闭**且 reviewer 尝试推翻未果的：P1-9/P1-10/P1-11/P2-1 的整体减法（独立验证了 key 结构、原地覆盖、以及 holderID 在生产路径上即 Pod 名因而受 ordinal 数上界约束，并专门去找累积路径未果）；P0-1 的屏障核心逻辑（从 nonce 唯一性、崩溃窗口、RV==V0 误判、第三方 reconciler 四个方向攻击均未推翻）；P1-2、P1-7、P1-13、P1-14、P2-2、P2-3、P2-5。

### 20.7R round-5 finding 关闭记录

round-5 判定 FAIL，P0=0 / P1=8 / P2=6。**P0 首次归零**：reviewer 从三个方向攻击 §8.3 的 P0 修法（TTL 常量、`expire_keys` 的调用点、同 boot 重注册路径）均未推翻，并独立复算确认 §5.3 的整体撤回是对的。P1 由 7 升至 8、P2 由 3 升至 6，但性质变了——八条里七条是局部遗漏或自相矛盾，只有一条（R5-03）指向分阶段模型本身。

| finding | 关闭方式 | 落点 |
|---|---|---|
| **R5-08（P1，后果最重）** P0 修法未传播到 §16/§17 与 status JSON | §16 由一句话展开为**逐结局的断言表**，并强制一条反例测试（TTL 过期 + 同 boot 重注册序列下 Plan 不得 terminal）；§17 停止条件改为唯一 inert 分支并新增"出现表外第五类结局"；status JSON 的 `resolution` 同步 | §16、§17、status JSON |
| R5-01（P1）`memory_repository.go` 无人拥有 | 归入 C01/C02 的 A0.5 写集。它是 `ScalePlanRepository` 与 `blockedTerminationRepository` 的唯一实现者，§5.3 改接缝必改它。**本轮 §5.3 三次引用该文件作证据却没问它归谁**，是 S1 方法在"引用即枚举"上的盲区 | §8.1、§11.2 C01/C02 |
| R5-02（P1）`transport/watch_handshake_test.go` 无人拥有 | 归入 C07 的 A0.5 写集，并把"**通配符不递归**"写成 §8.1 的硬约定。`handshakeRaceStore` 按方法集实现 `RouteSnapshotStore` 且全文不出现接口名——正是 §11.1 缺陷 3 要防的形态，本轮采纳了方法却没对 C07 重跑 | §8.1、§11.2 C07 |
| **R5-03（P1，唯一结构性）** 跨阶段编译依赖不可执行 | 新增 **S5**。穷尽枚举出**四条**编译边（C07/C08 两条、C09/C10 一条、C18 一条），冻结受限的 conformance 编辑权（C1~C4）+ 时序移交 + 两侧 gate（`conformance_edits_are_behavior_preserving` / `no_conformance_sentinel_remains`）；§11.6 澄清"互不重叠"约束的是同一时刻的两个 Writer | §8.1、§11.5、§11.6、§12.2、§12.3R |
| R5-04（P1）步骤 4 表头与分支矛盾、(c) 过度归类 | 表头改为"按下表分类"并固定校验顺序 (b)→(a)→(c)；(c) 由二分改**三分**：`== expected` → NOT_COMMITTED、`== desired` → adopted committed、第三值 → unknown（F43 第三方改写）。新增 F58f | §7.2、F58d/F58e/F58f |
| R5-05（P1）F58c 第三分支未处置屏障 operation | 改为**每行同时处置屏障 operation 与 scale operation** 的四行表；第三分支关闭为 `SUPERSEDED`（安全属性只取决于 RV≠V0，与谁推进无关）；"必然终止"改为给出递减度量（active set 单调收缩），GET 持续失败改为有界升级到 `DEGRADED_BARRIER_UNRESOLVED`。新增 F58g | §7.2、F58c/F58g |
| R5-06（P1）通道 B 置位的 latch 结构上不可清除 | **【已被 CF-3 取代，2026-08-13：通道 B 整体删除，此行仅为历史】** 申诉路径按 `detector_identity` **分叉**：通道 A 证明 fence，通道 B 证明 capability 未撤销且绑定 epoch 一致（等强度）；补齐 capability 的发放者/有效期/一次性/撤销/限流五项；把 DoS 面显式承认为设计意图并说明缓解在发放侧；更正"两通道排斥力相同"为**仅陈旧性维度**成立。新增 F62c/F62d | §6.4、§6.10、F62b/F62c/F62d |
| R5-07（P1）F61e 活性论证为假且无操作员出口 | 删掉"Pod 总会回来"。区分**exclusion 记录在 `NOT_FOUND` 期间无害**与**BLOCKED 占用 ownership 才是真正流逝的代价**；新增 SP-EXCL-6 与非终态的 `DEGRADED_EXCLUSION_UNRELEASED`（有界升级、只改可见性、不动任何判据）；列出三条操作员处置，其中第三条即已登记的 amendment 路径。§9.4 的 `TerminateBlockedScalePlan` 对该状态仍然禁止。新增 F61f | §8.3、F61e/F61f |
| R5-P2-1 结局集不穷尽 | 由"四种结局（穷尽）"改为按 `getPlacementExclusionScript` 的**真实返回码**七行全枚举，含 `MALFORMED`/`CROSS_SLOT`/`REVISION_EXHAUSTED`/传输层失败；并写明 `REVISION_EXHAUSTED` 在释放路径上不可达的理由（boot 一滚先被 `current_holder` 短路），观察到即 `REQUIRED_FINDING`。新增 F61g | §8.3、F61g、§12.7 |
| R5-P2-2 COMMITTED 行沿用已被推翻的推理 | 理由由"记录随 Holder 消失而惰性"改为"该 ordinal 再上线必带新 BootID，`load_placement` 合成未排除基线"，与 §1.2、F61b 同一判据 | §8.3 |
| R5-P2-3 盲窗 gate 漏掉 nil store 分支 | 登记该判定的**前置条件**（`recoveryStoreLocked()` 在 `tombstones == nil` 或类型断言失败时返回 nil，且 `markAuthorityAttemptedLocked` 的 nil 分支只写内存 memo），并给出 fail-closed marker：构造期强制 + 读取路径显式检查 nil，不得把 nil store 的空返回当作"集合为空" | §12.7 |
| R5-P2-4 轮次标签与结构性修改计数陈旧 | §0 的 A0 Gate 补齐 round-4/5 结论与 round-6 PENDING；标题改 round-6 候选；结构性修改由三项改为**五项**并写入 §0 | §0 |
| R5-P2-5 gate 计数与实际不符 | §12.7 实为 **20** 格；round-5 候选新增三格（其中两条反例），本轮候选未新增、只修订两格 | §20.9 |
| R5-P2-6 冻结形态与导出面收紧自相矛盾 | 分开定夺：`BlockedTerminationOwner.EnterPlanMutation` **改未导出**（七个调用者全在包内）；`ProductionRuntime.EnterPlanMutation` **保持导出**，因为 §12.2 的 `reference_adapter_exercises_every_seam` 要求跨包 reference adapter 能拿到 guard | §5.3、§12.2 |

round-5 明确**确认关闭**且 reviewer 尝试推翻未果的：§8.3 的 P0 修法本身、§5.3 的整体撤回（独立按大小写不敏感重算了七个入口）、四条 realm-global 理由、A0.5 的"改造已有接缝"重估、§11.2 十八条合同中的十六条、`cmd/cap-redis/*_test.go` 与 A1R 无重叠、§6.13 域上限、两个 Bootstrap 写者、A1R additive-only、off 模式三态，以及 §8.3 全部 Lua 行号引用无漂移。

### 20.7S round-6 finding 处置记录

round-6 判定 FAIL，P0=0 / P1=5 / P2=5。**P0 连续两轮归零**，且第二位独立复审者同样未能推翻 §8.3 的 P0 修法。5 条 P1 全部由 round-5 的修订引入，这一事实本身构成了 §0 出口判据放宽（S6）的依据。

七条本轮**关闭**：

| finding | 关闭方式 | 落点 |
|---|---|---|
| **R6-04（P1，唯一安全类）** F58c 的 ownership 列在 scale operation 未决的行上写 released | 拆为 F58c.1～F58c.4，**每行各自携带 ownership 判定**。只有第 1 行、且步骤 4 未落入 F58d/F58e/F58f 时才可释放；第 2 行在关闭旧屏障 op 的同一步注册新的，active set 根本没收缩。同时更正"递减度量"的适用范围——它只覆盖第 1、3 行，第 2、4 行由 `barrier_resolve_escalation_attempts` 有界，真正被保证的是"不存在**无声**的无限循环"而非"一定会结束" | §7.2、F58c.1～.4、§16 |
| **R6-03（P1）** §16 断言表停在四行而 §8.3 已七行 | 这是 R5-08 一字不差的重演，且发生在写下传播纪律的同一次修订里。因此不再依赖人工同步：§16 的行键定义为 §8.3 第一列的**逐字复制**，并新增 A1R gate `exclusion_outcome_rows_match_section_8_3` 做双向行键比对 | §16、§12.3R |
| R6-06（P2）§17 仍写"第五类结局" | 改为"七行结局表之外的返回值"，并写明该表由源码穷尽导出 | §17 |
| R6-07（P2）`ErrNotImplementedUntilA1R` 在 C1 下无处声明 | 明确哨兵**声明本身不是 conformance 编辑**，走 A0.5 自己的写集（默认 `internal/routecontrol/conformance.go`）；若 `go build` 报 import cycle 则改放叶子包并留档。A1R 的清理义务扩展为"声明与调用点都算" | §8.1 S5、§12.3R |
| R6-09（P2）步骤 4(c) 的互斥被无条件断言 | `desired == R` 时前两个子分支同时命中且处置相反。不靠约定求值顺序掩盖，改为在**注册 operation 之前**由 §6.2 submission 校验排除该输入，并要求 A2 为此写断言 | §7.2、§16 |
| R6-10（P2）§8.3 引 `holder_scripts.go:1169` 而 `FOUND` 在 1168 | 改为 `:1168` | §8.3 |
| R6-01 的一半（P1）编译边枚举单位错误 | 枚举单位由**文件**改为**编译单元**，补入三条测试边（`cmd/routectl/main_test.go`、`routecontrol_test.go`、`liveness_integration_test.go`）；`internal/testkit/capstaticroute/` 改用通配（第十一处写集缺口，成因是同一次修订里刚立的"通配不递归"约定被具名文件绕过）；撤销"出现第五条即停止"这条误置的停止条件 | §8.1 S5、§11.2 C09/C14 |

三条进入 §12.8 **carry-forward**：

| finding | 为什么不在文档层关闭 | carry-forward 编号 |
|---|---|---|
| R6-01 的另一半（P1）真集合从未测量 | 手工枚举连续两轮不完整。正确做法不是枚举第三遍，而是改成测量：`go build`/`vet`/`test` 的错误包路径去重集合就是真集合。这是**事实**不是判断，不需要复审确认 | CF-1，A0.5 第一个动作 |
| R6-02（P1）DEGRADED 无持久化落点 | 需要在三种形状里择一（新增 Plan 字段 / 扩展 `blockedExpired` 白名单 / 干脆不持久化只做 metric），且都要先给 `model.go`、`state_machine.go` 定 owner。第三种很可能就够——DEGRADED 不参与任何安全判据，只改可见性 | CF-2，A0.5 + A1 |
| R6-05 + R6-08（P1+P2）通道 B 缺 key、缺合同、缺签发检查、缺 auditor principal | 通道 A 独立成立，通道 B 缺失只是少一条检出路径，不构成既有属性失效。**默认处置是删除而非补齐**——保留一条能用的通道，好过声称有两条 | CF-3，A1（默认删除） |

### 20.8 结构性修改（S1 / S2 / S3 / S4 / S5 / S6）

本文档除逐条修订外，累计改掉了产生这些 finding 的五个机制：

```text
S1 §11.1-11.4  写集由源码 mutation 入口反向推导，并断言全覆盖与不重叠；
               审计检索命令写入文档，结论可复算；显式不修改清单必须给理由与断言方式
               round-3 修订：必须枚举接口的全部实现者与方法的全部调用者；
                            检索作用域必须是全仓，至少覆盖 internal/、cmd/、非 test testkit
               round-4 修订：实现者枚举必须【结构化】——按接口的方法集在全仓找同签名
                            receiver，不得按接口名检索（Go 的隐式接口满足使实现者文件
                            可以完全不出现接口名）；调用者枚举必须大小写不敏感并沿
                            转发层向上；_test.go 同样纳入实现者枚举；§11.4 条目若以
                            "某阶段测试"作为断言方式，该测试文件必须真在那个阶段的写集里
S2 §12.7       所有依赖平台/库行为的断言降级为具名 preflight gate + fail-closed marker；
               A0 只对登记表的完整性负责，不对任何一格的结论负责
               round-3 修订：登记正例不等于登记完毕，决定安全性的反例必须单独登记
S3 全文        减法优先：新增机制前必须先证明"不加这个机制会具体坏在哪"；
               自查 finding 与复审 finding 一样需要逐条回源码核对
               round-4 修订：复审 finding 本身也必须回源码核对。验证 finding 引用的
                            【证据】为真，不等于验证它推出的【结论】为真——round-3 的
                            P1-3 引用的 rg 输出完全属实，据此推出的结论却与源码相反
S4 §7.2 §8.3   范围收窄：正确性完全取决于平台/存储精确行为的机制，A0 只冻结【安全属性
               与 fail-closed 默认】，算法细节下放到能被测试证伪的阶段。
               依据：这两节各自连续三轮产生 P0/P1，每轮的修法都新增一条分支或约束，
               而新增的分支又成为下一轮的缺陷来源。两次 P0 都是二十行测试几分钟能钉死
               的事实（"同内容写不前进 RV"、"Holder 记录 TTL 失效不等于进程消失"），
               却各自在文档层活了整整一轮。
               推论：廉价的无条件兜底优于精确的平台假设——SP-BAR-3 把一项原本由 gate
               保证的检查改成无条件执行，正是这条推论的应用。
               round-5 修订：范围收窄只免除【算法】的冻结义务，不免除【传播】义务。
                            R5-08 证明：安全属性改对了，而照着实现的那一节（§16）
                            没改，等于没改。今后任何 P0 修法必须全文 grep 该机制的
                            全部计数与枚举，并逐处同步，把它写进修订自查清单
S5 §8.1 §11.5  跨阶段编译依赖显式化：接口变更与其全部实现者之间的编译边必须列出；
S5 §11.6 §12   被打断的实现文件由 A0.5 持【受限、临时、可机械核验】的 conformance
               编辑权（只改签名/补方法、行为等价、新方法体只能是 fail-closed 哨兵、
               不碰 Lua/key/schema），权利在 seam commit 处终止后移交正常 owner。
               依据：round-4 用一句"两半必须在同一次集成中落地"打发了 C18，而该说法
               在本文档自己的三条规则下构成环（gate 要求可编译 / 编译要求 A1R 那一半 /
               A1R 不得早于 reviewed A0.5 且写集不重叠）。
               round-6 修订：枚举单位是【编译单元】不是文件——Go 把 package 与它的
                            _test.go 编成同一个测试单元，go build 看不见、go test 必红，
                            而 A0.5 gate 要求 go test 通过，故这些边同样是硬的。
                            更根本的修订是【放弃手工枚举】：连续两轮枚举不完整，
                            改为由 go build/vet/test 的错误包路径去重集合来【测量】，
                            七行先验表只用于估算范围与暴露归属缺口。
               推论：这条边是 `go build` 五分钟能撞上、纯文档层却活了六轮的东西，
               它本身就是"某些问题只能由实现阶段证伪"的证据。S6 是把这条推论
               从个案上升为判据。
S6 §0 §12.8    判据分层：把"必须由文档证明"与"只能由实现证伪"分开。A0 出口由 0/0/0
               改为【P0=0 + 未关闭项逐条进入 carry-forward + 安全类清零】。
               分层线画在【安全类】上（会让 Plan 不安全 terminal / 会让 fence、
               exclusion、barrier 的安全属性失效 / 会让 fail-closed 退化为 fail-open），
               不画在难易或工作量上。
               依据：六轮 finding 合计 11/6/20/11/14/10，P0 已收敛而 P1 从未收敛；
               round-6 的 5 条 P1【全部】由 round-5 修法引入，且逐条都能被
               go build、打开一个结构体、写一行 Lua 在分钟级证伪。
               代价（必须一并记住）：编译边完整性、DEGRADED 可持久化、通道 B 底座
               这三件事由"复审保证"降级为"实施纪律保证"。补偿机制只有一个——
               §12.8 每条都绑定实施阶段的布尔 gate，不通过即整体回退。
```

归类依据：round-2 的 P1-A、P1-D，round-2.5 的 X1、X2，round-3 的 P1-1、P1-2，round-4 的 R4-03、R4-04，round-5 的 R5-01、R5-02 是"写集自顶向下写而非自底向上推导"的十个实例；P1-C、P2-A、P0-A 的一部分、round-3 的 P0-1、round-4 的 R4-01、R4-05 是"把只能靠编译/运行证实的平台事实当成冻结断言"的六个实例；round-3 的 P1-9～P1-11、P2-1 是"基于未核实的源码结论新增机制"的四个实例；round-4 的 R4-02 是"未核实复审 finding 的结论"的第一个实例；round-5 的 R5-08 是"修法未传播到照着实现的那一节"的第一个实例，也是本轮唯一一条会直接重建 P0 的 finding。

### 20.9 A0.5 授权申请 —— **已批准（2026-08-13）**

用户于 CF-1 预跑结果回报后批准本申请。批准的是下述**原样范围**，未作扩大。A0.5 的执行 worktree 与分支：

```text
worktree = farm-game-scaleplan-a0-5-20260813
branch   = codex/scaleplan-a0-5-20260813
base     = 0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
契约     = 本文档与 status JSON 的 A0.5 授权版，随 worktree 冻结带入
```

本节替代原先的"下一轮复审攻击面"。经六轮独立对抗复审，本文档申请进入 A0.5，理由、判据与代价如下。

**申请依据（三条，全部可核对）：**

```text
1. P0 连续两轮归零，且两位【不同的】复审者独立攻击 §8.3 的 P0 修法
   （TTL 失效 + 同 boot 重注册）均未推翻。安全主线已稳定
2. round-6 的 5 条 P1 全部由 round-5 修订引入，无一遗留缺陷。
   这说明剩余问题不在设计里，在【文档与代码的对齐】上
3. 五条逐条都能被实现阶段分钟级证伪，其中三条已在本轮关闭，
   三条进入 §12.8 carry-forward 并各自绑定实施阶段的布尔 gate
```

**申请的具体内容：**

```text
授权范围 = A0.5 一个阶段
        + §8.1 冻结的 exact Owned Write Set
        + §8.1 S5 冻结的 conformance 编辑权（受限、临时、可机械核验）
不申请  = A1 / A1R / A2 / A3 / A4，更不申请 A5 / A6
不申请  = 任何真实 Redis / Kubernetes 连接
不申请  = 任何 runtime mutation、压测或采样
```

**A0.5 的第一个动作是固定的，且先于任何设计工作：** 执行 §12.8 CF-1 的四条命令，用编译器测出跨阶段编译边的真集合，与 §8.1 S5 的七行先验表比对并留档。这一步产出的是事实不是判断，它同时是 CF-2（`model.go` / `state_machine.go` 的归属）与 CF-3（通道 B 的合同归属）定稿的前提。

**A0.5 的退出判据在 §12.2**，其中四格是本轮新增或改写的：`compile_edge_set_measured_not_enumerated`、`conformance_edits_are_behavior_preserving`、`degraded_state_persistence_owner_assigned`、`auditor_principal_decided`。任何一格不通过即整体回退，不允许"带着未决项进入 A1"。

**必须一并接受的代价：** 编译边完整性、DEGRADED 状态可持久化、通道 B 底座这三件事，从"由外部复审保证"降级为"由实施纪律保证"。`0/0/0` 判据至少有一个独立复审在兜底；carry-forward 清单只有 §12.2 的四格 gate 在兜底。**如果 A0.5 的第一个动作被跳过，这三件事就没有任何人保证。**

**若用户选择继续复审而不是授权**，round-7 的火力应集中在两处，其余前六轮已反复攻过：

```text
【最高优先】§0 的出口判据放宽本身是否成立：
         "安全类"的三条定义是否穷尽——有没有一种危害既不让 Plan
             不安全 terminal、也不让 fence/exclusion/barrier 失效、
             也不让 fail-closed 退化，却仍然是安全问题；
         R6-05/R6-08 被归为【非安全类】的论证是否成立——
             通道 B 缺失真的只是"少一条检出路径"吗
【最高优先】§12.8 三条 carry-forward 的 gate 是否真的能兜住：
         CF-1 的四条命令是否真能测出全部编译边（cgo、build tag、
             //go:build ignore、generate 产物会不会漏）；
         CF-2 的三种形状是否穷尽，形状 (c) 的"不影响任何判据"是否为真；
         CF-3 的"默认删除"会不会在实施中被悄悄改成"默认保留"
```

### 20.10 精确身份与禁止事项

```text
worktree=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-sp3-adapter-plan-20260813
branch=codex/scaleplan-sp3-adapter-plan-20260813
HEAD/base=0036d17b6d4add1b1a3092e3e5e0051f75bb5fd4
reviewed_protocol_code_identity=c06ae6a552bfb3c27f8d5283dc3ae7111c1799f4
G21_runtime_identity=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361
formal_perf_lock=ABSENT
worktree_state=TWO_UNTRACKED_AUTHORIZED_DOCS_ONLY
```

**A0.5 已获用户批准（2026-08-13）。** 批准范围严格限于 §20.9 所列：A0.5 一个阶段、§8.1 的 exact Owned Write Set、S5 的 conformance 编辑权。A1/A1R/A2/A3/A4 需在 A0.5 gate 通过后另行申请；A5/A6 不在范围内。以下禁止清单**除 A0.5 接缝改动外一项未减**：

```text
Adapter/code/proto/Lua/deploy implementation
真实 Redis/Kubernetes connection
kubectl/helm/docker build/runtime mutation
1->2->1
performance/capacity sampling
读取公司 Redis secret
SCAN/KEYS/cleanup
push/merge/rebase integration/tag
```

禁止命令保持：

```text
FLUSHALL
FLUSHDB
SCRIPT FLUSH
```

本文档**申请**A0.5（见 §20.9），但在用户批准前不自行授予；A1/A1R/A2/A3/A4 需在 A0.5 gate 通过后另行申请；A5/A6 一概不在本文档范围内。

---

## 21. A0.5 执行结束记录（2026-08-13）

### 21.1 结论

A0.5 的六条接缝已冻结、已实现、且**已被第二个包真实驱动过**。14 项 exit gate 全部 PASS。stage 从 `A0_5_SEAM_IN_PROGRESS` 推进到 `A0_5_SEAM_COMPLETE`，状态 `A0_5_EXIT_GATE_PASSED_PENDING_REVIEW` —— 等复审，不等实现。

提交序列：

| commit | 内容 |
| --- | --- |
| `c3752fa` | CF-1 编译边测量跑到不动点 |
| `ce06d5c` | sealed runtime、mutation guard、durable stop、exclusion release |
| `2c093c5` | gofmt |
| `8c0807c` | reference adapter：六条接缝全跑通 |

### 21.2 为什么必须有 in-tree reference adapter

A0.5 在没有任何调用方的情况下冻结接口，这正是"接口看着对、行为是错的"的标准成因。`internal/testkit/scaleplan/reference_backend.go` 就是那个调用方：它按 A1/A1R/A2/A3 将来的用法驱动每一条接缝。

它还承担一件**在别处无法完成**的证明。包内测试能够到未导出字段，所以"外部后端铸不出终态"这句话在 `package scaleplan` 里是自证的。只有第二个包能证伪它。`TestReferenceBackendReceivesSealItCannotMint` 位于 `package scaleplan_test`，零值 seal 报告未授权，并且**在触碰任何存储之前**被拒 —— 顺序是被断言的，因为 A1 的等价实现必须在开 Redis 连接之前就拒掉，而只看返回值的测试发现不了这个区别。

### 21.3 两条接缝原本是空转的

写下来是因为这类缺陷会重复发生：

**Bootstrap / Publish 的 control token。** CF-1 把 token 穿进了签名，然后没有任何一处校验它 —— 参数存在，行为为零。和当初 `MutationScope` 那个形参一模一样的毛病。现在校验绑定在装配上：`RequireControlToken()` 由装配 coordinator 的同一个 composition root 调用，两者不可能各说各话。有 coordinator 的进程拒绝未签发 token；没有的进程走与 G21 完全一致的路径，mode=off 因此是真正的 no-op，而不是"另一种配置的写入者"。

**心跳下限。** 原本 1 秒，是拍出来的。真正的安全性质是 `heartbeat*3 <= TTL`：一个租约必须能扛过至少两次失败续约才过期，否则一个丢包就会取消一次本来正常的 mutation。下限降到 10ms，比值约束保留，测试从 5 秒降到 1 秒。

### 21.4 CF-2：DEGRADED 计数取形态 (c)

**决定**：升级计数留在 controller 内存，不进 Plan 记录。

**依据**：DEGRADED 不参与任何安全判据。所有 DEGRADED 路径都让计划停在 BLOCKED，而 BLOCKED 本身已经是 fail-closed。因此重启丢掉计数买到的是"多重试几次"，不是"做了不安全的动作"。形态 (a) 和 (b) 都要改 `model.go` 或 `state_machine.go` —— 两个文件在 A1 禁改列表里；(b) 还要给一个"reason 一旦判定就固定"的状态机加第二条 BlockedReason 变更路径，代价高于这点可见性。

**承认的代价**：controller 重启后，运维可见的 DEGRADED 信号归零，一个跨重启持续失败的计划可能永远不浮出水面。这是监控缺口，不是正确性缺口 —— 也正因为它是真实损失，才要记下来而不是假装没有。

**护栏**：`TestBlockedReasonHasExactlyOneWhitelistedTransition` 钉住唯一那条白名单转移。将来谁加 DEGRADED 边，这个测试会挂，CF-2 被迫重开，而不是被悄悄绕过。

**`model.go` / `state_machine.go` 归属**：A0.5，且 A0.5 选择不修改二者。今后任何修改需求都是 A0 修正案，不是阶段内决定。

### 21.5 CF-3：删除通道 B（已执行，2026-08-13）

**先更正本节的初稿。** 初稿把通道 B 写成"没有任何消费方要求的第二观测通道"，把删除说成消掉了冗余。这个表述是错的，而且错在关键处：通道 B 不是冗余，它是**某一类 corruption 的唯一检出与阻断路径**。逐操作校验的范围有界，`AuditPage` 覆盖的正是它看不见的深度范围，因此 AuditPage 在 touched-key 之外发现的损坏，删掉通道 B 之后就再也没有第二条路能阻断写入。

准确的处置陈述是：**接受一个缺口，因为它当前空转**——系统里没有审计者角色，`AuditPage` 没有调用方，ACL 里也没有对应 principal。完整代价、造价侧明细、以及重新决定的触发条件（`AuditPage` 获得第一个真实运行者）见 §4.4。

**已执行的删除范围**：

| 位置 | 动作 |
| --- | --- |
| §6.4 授权通道 | 双通道改单通道；保留缺口的完整形状陈述 |
| §6.4 申诉路径 | 取消按 `detector_identity` 分叉；保留分叉存在的理由，防止将来重造死锁 |
| §16 用例 | 删 5 条通道 B 用例；主语写宽为"任何不持有效 lease 者"；新增一条断言缺口本身的用例 |
| F62a | 结论从"必须成功置位"反转为"只能报告"，标记 `accepted gap` |
| F62b | 主语写宽 |
| F62c / F62d | 删除 |
| §4.4 | 新增已接受缺口段 |

保留为历史记录、不予删除的：§0 的 round-6 分类、§12.8 CF-3 条目、§20.7R / §20.7S 的 finding 关闭记录。它们记录的是"当时为什么这么判"，删掉就失去了审计链。

### 21.6 Gate 结果

14 项全 PASS，逐项证据见 status JSON 的 `a0_5_exit_gate`。三项值得单独说：

- **focused 50x**：`internal/scaleplan`、`internal/testkit/scaleplan`、`internal/routeplan` 各 50 遍，干净。
- **race 全树**：`go test -race ./...` 干净。此前有一次 `internal/workflow` 失败，只在全树负载下出现，重跑不复现，且该包本分支一行未改 —— 归为既有 flake，不归 A0.5。
- **gofmt**：所有 owned 文件干净。树里剩下的未格式化文件早于本分支。

### 21.7 A0.5 未做、也不该做的事

接缝冻结不等于后端存在。以下仍然全空，且**本来就不在 A0.5 范围内**：

- ScalePlan 的 Redis Store（A1）—— 现在只有内存实现
- Lua 脚本与 codec（A1R）
- K8s 适配器（A2）—— 真正动 pod 副本数的那一层
- Manager / Publisher 的生产装配（A3）

换句话说：ScalePlan 的**核心**现在是硬的，**四肢**还没有。Pod 扩缩容依赖 A2，而 A2 依赖 A1 落地，所以扩缩容目前是 0。

### 21.8 A0.5 最终门禁收口候选（2026-08-13）

上一轮“14 项全 PASS”采用了实现者自拟的 14 项清单，不是 §12.2 冻结的 21 项清单，因此不能作为 A0.5 最终结论。按 §12.2 原文逐项重算后，本候选状态为：

```text
verified=20/21
pending=shared_writer_seams_frozen
independent_review=pending
```

本轮关闭五项：

- `repository_identity_binding`：`BackendMutationLease` 增加稳定 `RepositoryID`。Seal 的稳定写权绑定为 `RepositoryID + Scope + Owner + Fence`；续租只更新 `ExpiresAtMs`，因此 deadline 不参与相等判断。包内测试证明 private repository identity / sealer 不可串用；跨包测试故意让两个独立 backend 的 Scope、Owner、Fence 相撞，证明另一仓库仍会在 store access 前拒绝真实 Seal。
- `renew_liveness_exemption_proven`：四条证明义务均有行为测试，持续 renew 不破坏 Capture，真实 placement 变化必使 Capture 失效。
- `capture_token_excludes_lease_deadline`：反射测试拒绝时间、deadline、expiry、TTL 字段，同时要求 lease epoch、assignment revision 与 active cardinality 在场。
- `authority_capture_read_surface_decided`：C13 判定 `SUFFICIENT`，按 §11.2 的 bracket 约束实施，不新增 A1R read surface。
- `transition_quiescence_universal_evidence`：枚举标志、未完成基数、domain、revision 与时间锚点都成为 fail-closed 的机器断言。

`shared_writer_seams_frozen` 的机械证据已经具备：从接缝冻结提交 `d4d808b` 到本候选，Manager、RoutePlan、Publisher、Kubernetes scale 与 Placement exclusion 的共享 writer 接缝没有变化；后续提交仅涉及 Capture/Quiescence 证据以及 ScalePlan 自身 Seal/Lease 合同。由于“冻结后无人改变共享 writer”是流程断言，最终 PASS 与 `review=P0=0` 必须由独立复审者给出，当前实现者不自评。
