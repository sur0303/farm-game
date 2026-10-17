# Local kind Staging 范围变更评估与交接

日期：2026-08-10

文档性质：范围变更评估、列车边界交接、后续实施输入

当前状态：`PROPOSAL_ONLY_NOT_AUTHORIZED`

适用项目：Q1/R1 容量列车、第二批结构改造、后续 R2 与本地 Kubernetes 验收

> 本文不批准范围变更，不修改任何现有 PASS/Blocked 状态，不把本地 kind 结果冒充正式生产
> Staging、真实多物理节点容量或 30M DAU 证据。本文只把“用单台 Linux + Docker + kind 替代外部
> Kubernetes 资源”的分析整理成可审阅、可决策、可交接的工程合同草案。

---

## 1. 执行摘要

待评估方案的实质不是单纯选择一种 Kubernetes 工具，而是提出一次验收范围重基线：

```text
原外部依赖阻塞
公司 Kubernetes / Registry / DNS / CLB / 正式 TLS / 正式 H5 URL

拟替代为
单台 Linux + Docker + kind + 本地不可变镜像 + 本地入口 + 可选本地 TLS/mTLS
```

该方案在技术上可以验证：

- Kubernetes Deployment/StatefulSet/Service 等对象能否正确装配；
- Pod startup/readiness/liveness、preStop、SIGTERM 与 RollingUpdate；
- 多 Pod 应用级路由、重连、恢复和正确性；
- 在动态路由前置条件完成后，验证 Pod 级 Shard A→B 迁移和旧 Holder Fence；
- 本地 H5、Go Protocol Bot、少量 Playwright witness 与 Authority Oracle 的联动。

该方案不能验证：

- 多物理节点容量或机器级容灾；
- 多可用区、云负载均衡、生产 DNS、生产证书或真实公网路径；
- 生产 Redis 网络、配额和故障域；
- 生产级 Kubernetes 容量、生产 SLO、3M/5M PCU 或 30M DAU；
- 未完成的 Worker readiness、动态 Route Runtime 或 Model Field V2 正确性。

因此，正确分类应为：

```text
LOCAL_KIND_ACCEPTANCE = PROPOSED_NOT_AUTHORIZED
FORMAL_K8S_STAGING     = 保留原状态，不静默删除或改名
PRODUCTION_CLAIM       = NONE
MULTI_PHYSICAL_NODE    = NOT_PROVEN
```

只有验收负责人批准明确的范围变更记录后，才可以将外部基础设施项目登记为
`OUT_OF_SCOPE_BY_APPROVED_DECISION`。执行者不能单方面把它们改成 `NOT_REQUIRED`。

---

## 2. 当前冻结事实

### 2.1 Train A：R1 对照列车

当前 R1 单 Pod合同冻结为：

```text
Runtime Evidence = d1899ecab8d71248062ffd70eb4e42048e394a38
Business         = 93a66ce76d509d86e8cac8e1d87563d084eba821
Backend Image    = sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7
H5 Commit        = 525c4d324100b17a891f02797c4880fa9a02f70a
H5 Image         = sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd
Model Layout     = R1 authority/workflow/friend v1; Model Field V2 disabled
```

列车顺序：

```text
CAP-REDIS-L1-R1
→ CAP-SINGLE-POD-L2-R1
→ 条件满足后 CAP-MIXED-B0-R1
```

本地 kind 方案不得改变、替换或重新构建上述 R1 Runtime。特别禁止：

- 把“最新 integration”或 B2 镜像装入 L2-R1；
- 在 R1 中启用 `authority-v2`、Model Field V2 或 Friendship 唯一切流；
- 用本地 kind 多 Pod结果替代单 Pod L2-R1；
- 用本地 kind 结果提前解除 Mixed B0 阻断；
- 将本地 kind 的 200-WSS 数据作为 R1 单 Pod安全常数。

### 2.2 Train B：第二批结构改造

冻结顺序为：

```text
FriendshipDomain
→ legacy roster 删除
→ Model Field V2
→ Worker readiness
→ Static K8s Staging
```

本地 kind 只能作为最后一个 `Static K8s Staging` 的候选承载环境。它不能证明前四项已经完成，
也不能替代各项自己的 Review、Premerge、integration binding 和正确性门。

### 2.3 R2 重跑义务

第二批完成后仍必须使用同一合同重跑：

```text
CAP-REDIS-L1-R2
CAP-SINGLE-POD-L2-R2
CAP-MIXED-B1-R2
```

只有通过 R2 的数据可以进入后续 3M/5M PCU 模型。本地 kind 验收本身不是 R2 容量重跑，不能跳过
L1/L2/Mixed，也不能自行创建会与既有 R2 语义冲突的 Checkpoint 名称。

### 2.4 integration 身份待确认

截至本文生成时，已核验的远端分支仍为：

```text
origin/codex/integration-20260803
= 5ede235d85d59fbc66abb15ea920b70d493c213a
```

该 Head 只包含 L2-R1 Design integration binding。虽然已有“第二批已经合入 integration”的口头信息，
但在该远端分支上尚未看到对应新 Head。此差异不影响冻结 R1，但会阻断未来本地 kind 的 B2/R2 镜像
身份确认。开始 B2 staging 前必须取得实际分支、merge commit 和不可变镜像身份。

---

## 3. 本方案真正改变的内容

待评估分析提出了四类变化：

1. **范围变化**：外部集群、Registry、DNS、CLB 和正式 URL 从阻塞项变成范围外；
2. **环境替换**：使用 kind Node 容器、NodePort、SSH tunnel 和本地 TLS；
3. **验收重写**：创建本地 K0～K9 流程和 `LOCAL_ACCEPTANCE_PASS`；
4. **结论降级**：保留应用级多 Pod结论，但明确不声明生产或多物理节点能力。

其中第 2 项是技术决策；第 1、3、4 项是验收治理决策。只有技术执行权限不足以批准后三项。

“Formal”也不必然等于“公司云集群”。Formal 可以表示身份、步骤、证据和 Review 严格受控。若原合同
未要求物理多机，一个本地 kind 环境也可以被正式执行，但必须在结论中准确写成
`single-linux-kind`，不能借“formal”暗示生产等价性。

---

## 4. 建议的批准状态模型

### 4.1 批准前

```text
scope_change_decision       = NOT_APPROVED
local_kind_acceptance       = PROPOSED_NOT_AUTHORIZED
formal_k8s_staging          = 保持原状态
external_k8s_cluster        = UNRESOLVED_OR_OUT_OF_SCOPE_PROPOSAL
external_registry           = UNRESOLVED_OR_OUT_OF_SCOPE_PROPOSAL
public_dns                  = UNRESOLVED_OR_OUT_OF_SCOPE_PROPOSAL
public_tls                  = UNRESOLVED_OR_OUT_OF_SCOPE_PROPOSAL
formal_h5_url               = UNRESOLVED_OR_OUT_OF_SCOPE_PROPOSAL
production_capacity_claim   = NONE
multi_physical_node_claim   = NONE
```

### 4.2 明确批准范围变更后

必须记录批准人、Decision/ADR ID、批准时间、替代验收项和不可声明项，之后才允许写：

```text
external_k8s_cluster = OUT_OF_SCOPE_BY_APPROVED_DECISION(<decision-id>)
external_registry    = OUT_OF_SCOPE_BY_APPROVED_DECISION(<decision-id>)
public_dns           = OUT_OF_SCOPE_BY_APPROVED_DECISION(<decision-id>)
public_tls           = OUT_OF_SCOPE_BY_APPROVED_DECISION(<decision-id>)
formal_h5_url        = OUT_OF_SCOPE_BY_APPROVED_DECISION(<decision-id>)

local_kind_cluster   = REQUIRED
local_image_identity = REQUIRED
local_browser_origin = REQUIRED
local_multi_pod_e2e  = REQUIRED
```

不得把 `OUT_OF_SCOPE` 写成 `PASS`。被取消的生产要求与本地替代项必须分别记录。

---

## 5. 建议的环境边界

### 5.1 逻辑拓扑

若范围变更获批，建议的目标拓扑为：

```text
Linux Host
├── Docker Engine
├── kind control-plane container
├── kind worker-a container
├── kind worker-b container
│   ├── farm-gate Pods
│   ├── player-svr Pods
│   ├── farm-svr Pods
│   ├── farm-worker Pods
│   ├── local edge/H5 Pod（可选）
│   └── route-controller Pod（仅 RT2 前置完成后）
├── Driver cgroup / loadgen Pod
├── Oracle / Evidence collector
└── Redis（按第 8 节批准的隔离方案）
```

kind 的三个 Node 都是同一台 Linux 的 Docker 容器。Node label、taint 和 K8s resource limit 可以验证
调度和逻辑隔离，但不构成物理资源隔离证据。

### 5.2 服务工作负载类型

不要仅为了 Pod 名字稳定或报告好看就选择 StatefulSet。

- 无持久网络身份要求的 Gate/Worker：优先 Deployment；
- Player/Farm 是否需要 StatefulSet：由 Holder identity、Route Runtime 和存储合同决定；
- Pod 名称不得成为权威 Actor/Holder 身份；
- 如果服务只依赖 Route Assignment 和 writer epoch，优先保留可替换 Pod 语义；
- StatefulSet、Headless Service 或 PVC 必须有明确产品理由和独立 Review。

### 5.3 探针和生命周期

本地 K8s 仍必须验证：

```text
startupProbe
readinessProbe
livenessProbe
preStop
terminationGracePeriodSeconds
resources.requests
resources.limits
RollingUpdate strategy
PodDisruption/termination behavior（适用时）
```

但不得凭空假定 `/healthz` 或 `/readyz` 已满足业务语义。特别是 Worker readiness 属于 B2 Writer 的生产
责任，Platform Writer 只能消费并验证，不能在 staging 分支用假探针绕过。

Readiness 至少需要证明：

- 配置和冻结身份已加载；
- 必要 Redis/Authority 能力可用；
- 静态 Route snapshot 可接受且未过期；
- 需要的内部 RPC listener 已就绪；
- Worker 不会在恢复/重放未准备好时接收 Due；
- DRAINING/迁移中的 Holder 不被重新加入新流量。

---

## 6. 本地入口与 H5

### 6.1 推荐入口

最终本地验收建议使用同一 Origin：

```text
http://127.0.0.1:<approved-port>/
```

或者本地 TLS：

```text
https://farm.test:<approved-port>/
```

反向代理按实际 Route 合同映射：

```text
/           → H5 static bundle
<API path>  → farm-gate HTTP
<WS path>   → farm-gate WebSocket
```

实际 API/WS path 必须从冻结产品配置读取，不得根据示例擅自改 H5 业务代码。

### 6.2 SSH tunnel

远程 Linux 可通过 SSH local forward 暴露到浏览器本机，以避免开放服务器防火墙。Evidence 只记录入口
配置 Hash、端口类别和 tunnel 模式，不记录用户、主机名、内部 IP 或凭据。

### 6.3 HTTP/WS 与 TLS/WSS

- 早期装配诊断可在明确的本地开发模式使用 HTTP/WS；
- 不得为了本地测试永久降低生产 Cookie、SameSite 或 Secure 默认值；
- 如果验收要声明 Secure Cookie、HTTPS 换票、WSS 重连和旧连接 Fence，应使用本地测试 CA；
- 推荐保留域 `.test`，避免 `.local` 的 mDNS 冲突；
- 本地 CA 必须只用于测试，不进入产品镜像和 Git；
- 浏览器信任安装步骤和证书 SAN Hash 应进入环境证据，私钥不得进入 Evidence。

### 6.4 NodePort 与 port-forward

- `kubectl port-forward` 只用于快速诊断；
- RollingUpdate、双 Gate 切换和长连接恢复的正式本地证据，应使用 Service + NodePort/kind port mapping；
- port-forward 进程自身终止不能冒充 Gate 故障或正常 Service 切流；
- Edge 代理也是被测拓扑的一部分，必须记录资源、错误、重启和连接行为。

---

## 7. 镜像身份合同

不需要公司 Registry 不等于不需要不可变镜像身份。

必须记录：

```text
source_commit
review_head
premerge_head
integration_head
Dockerfile_sha256
build_context_hash
OCI image ID/digest
kind node imported image identity
Pod spec image reference
running container image identity
manifest_sha256
```

要求：

- 只从 clean、reviewed、premerge 通过的精确 Head 构建；
- 禁止从脏 worktree 构建最终证据镜像；
- 禁止 `latest`；
- 即使使用 commit Tag，也必须核验并记录内容寻址 digest/image ID；
- 使用 `IfNotPresent` 时必须防止同 Tag 旧镜像残留；更安全的是唯一 Tag 加运行前后 digest 核对；
- `kind load docker-image` 完成后，要从每个相关 Node 和运行容器反向核验实际镜像；
- 本地 Docker Image ID 只证明该机器上的内容身份，不证明可复现构建。

分析中出现的示例 commit、Tag 或端口均为未验证占位符，不能直接进入正式 Manifest。

---

## 8. Redis 隔离决策

### 8.1 与 Train A 的硬边界

CAP-REDIS-L1-R1 和 CAP-SINGLE-POD-L2-R1 继续遵守已冻结的唯一、loopback、临时 Redis 和精确销毁
合同。公司 Redis 不得进入 R1 Runtime，也不得用于补写或替换 R1 Evidence。

### 8.2 本地 K8s 可选方案

本地 K8s 必须在执行前选择并批准一种模式，不能运行中切换：

#### 模式 A：本机/集群内独占临时 Redis

适合正确性、生命周期和可重复验收：

- 每次运行一个独占实例；
- 固定版本和配置 Hash；
- 独占 namespace；
- 禁止 `FLUSHALL`、`SCRIPT FLUSH`；
- 使用固定点位清理和残留检查；
- 运行结束精确销毁实例和 namespace。

#### 模式 B：授权公司 Redis

只适合明确授权的集成连通或后续 staging，不继承 R1 资格：

- 必须有环境 Owner、配额、测试时窗和压力授权；
- 使用唯一 namespace 和统一 layout/profile/hash version；
- namespace 隔离不能证明 CPU、内存、带宽和 latency 隔离；
- 共享 Redis 先满时结果只能是 `INCONCLUSIVE_SHARED_REDIS`；
- 禁止全库命令、全局脚本操作和非授权扫描；
- Secret 通过临时文件或 Secret 注入，Git/Evidence 不保存 URL、密码或原始 Secret；
- 运行前后执行有界、固定点残留检查；
- 公司 Redis 结果不得输入单 Pod R1 或生产容量模型。

### 8.3 Probe Job

Probe 必须使用与业务相同的 Go Redis Client 和 TLS/Auth 配置，只执行授权的有界动作：

```text
connect/auth
PING
Redis TIME
加载或校验固定 Lua
创建唯一测试 Key
读取并校验
删除该精确 Key
EXISTS == 0
```

禁止 `KEYS`、无界 `SCAN`、`FLUSHALL`、`SCRIPT FLUSH` 和访问其他运行 namespace。

---

## 9. Static Staging 与动态迁移必须拆开

本地分析把静态多 Pod和 Shard 迁移放进同一条最终演示路径，但两者的前置条件不同。

### 9.1 Static Local K8s

可以在静态 Route snapshot 下验证：

- 多 Gate/Player/Farm/Worker Pod 装配；
- Service/Readiness；
- H5 F1/F2/F3；
- 200-WSS 和 slow-reader 功能正确性；
- Gate RollingUpdate 与 Fresh Join；
- Worker SIGTERM 与 Due 恢复；
- Player/Farm Pod 重建后的静态重新装配。

### 9.2 Dynamic Route / RT2Q

只有既有动态控制面前置门关闭后，才允许验证：

- Route Watch reconnect；
- HolderControl mTLS peer identity 与 ACL；
- Drain/Prepare；
- Authority switch；
- writer epoch 换代；
- Shard A→B；
- 旧 Holder 存储拒写；
- Controller restart/crash takeover；
- VerificationPort 与最终一致性。

部署了 route-controller Pod 不等于动态 Route 资格通过。Static Staging 不得提前宣称 Shard migration PASS。

---

## 10. Load Generator 与容量边界

### 10.1 Driver

本地多 Pod负载仍应由 Go Protocol Bot/open-loop scheduler 产生：

```text
planned
launched
scheduled-but-unsent
completed
rejected
timeout
arrival lag
```

必须分开记录，禁止 catch-up burst。Playwright 只保留少量页面作为正确性 witness。

### 10.2 单机局限

即使把 loadgen Pod 调度到另一个 kind worker，它仍与所有服务共享同一台 Linux。必须记录：

- Host CPU、memory、steal、IO 和 network；
- 每个 kind Node container cgroup；
- 每个业务 Pod cgroup；
- Driver cgroup；
- Edge、Oracle 和 collector 资源；
- Host background workload。

Driver、Docker bridge、kind Node 或 Host 先满时，只能判为 `INCONCLUSIVE_ENVIRONMENT` 或
`INCONCLUSIVE_LOAD_GENERATOR`，不能形成服务容量结论。

200-WSS 在该阶段主要证明功能、连接生命周期和正确性；如果没有独立资源资格，不得把它发布为生产并发
常数。

---

## 11. 候选验收序列

以下名称是待审批的本地验收草案，不是已经存在的 PASS Checkpoint。

### K0：环境与装配

- kind config、Kubernetes/Docker/kernel 版本冻结；
- Node Ready；
- Namespace、Service、ConfigMap、Secret 引用正确；
- 不可变镜像身份一致；
- Redis Probe PASS；
- 无旧 namespace、Pod、Job 或测试残留；
- 各 Pod startup/readiness PASS。

### K1：第二批布局确认

- FriendshipDomain 唯一 Authority 状态符合已审合同；
- legacy roster 已按批准计划删除且无双真值；
- Model Field V2 exact codec/layout 已冻结；
- 100% V2 的定义、读写开关、回滚门和残留检查明确；
- Worker readiness 生产实现已由 B2 Writer 提供。

任何 v1/v2 双读歧义、未知布局或残留均立即停止。

### K2：H5 F1/F2/F3 与 Authority Oracle

- 固定 H5 bundle；
- Browser + Protocol Bot + Oracle；
- 登录、Bootstrap、Join、Buy/Plant/Harvest/Water/Theft/Home；
- 两次点击 Theft 合同；
- 断线恢复和 pending journal；
- hard correctness gates 全零。

### K3：200-WSS 基线

- 200 distinct users 定义固定；
- ready、terminal、disconnect、FarmVersion、Receipt 和 conservation 证据完整；
- planned/actual/unsent 分离；
- 只形成本地功能基线，不形成生产容量常数。

### K4：Slow Reader

- 一个 non-reader、多个 slow reader 和随机 read pause；
- 只允许影响各自 ConnectionWriter；
- FarmActor、Hub 和健康连接无全局阻塞；
- bounded eviction/reconnect。

### K5：Gate RollingUpdate

- Service/NodePort 入口保持；
- 旧 Gate 停止新 admission；
- preStop/drain/termination budget 有证据；
- H5 重连、换票、Fresh Join；
- 原 Operation 仍可查询；
- 不能把 port-forward 中断冒充 Gate rollout。

### K6：Worker SIGTERM 与恢复

- Worker readiness/drain；
- SIGTERM 后不丢 Due；
- takeover/retry 有界；
- oldest Due 最终回落；
- 无 duplicate side effect、StepUnknown 或错误 Scope。

### K7：Player/Farm Pod 重建

- 静态 Route 或批准的 Route 模式明确；
- Actor reload/passivation 行为正确；
- Receipt/Order/Workflow 不重复；
- FarmVersion 连续；
- readiness 前不接收权威写。

### K8：动态 Shard A→B

仅在 RT2 前置关闭后执行：

- Drain；
- Authority switch；
- writer epoch 更新；
- Gate 路由收敛；
- 旧 Holder 拒写；
- Fresh Join；
- 最终业务与 Authority 对账。

### K9：Controller restart / Verify

- Watch 重连；
- Controller restart；
- 切换关键点 crash/takeover；
- VerificationPort；
- 无 split-brain、stale commit、双奖励或版本断层。

---

## 12. 硬停止条件

任何本地 K8s 阶段出现以下情况都必须停止新压力并保留原始 Evidence：

```text
duplicate_grant_total > 0
duplicate_debit_total > 0
stale_holder_commit_success_total > 0
farm_version_gap_total > 0
workflow_step_unknown_residual > 0
due_residual > 0
h5_pending_journal_residual > 0
authority_client_mismatch_total > 0
silent_drop_total > 0
unexpected_lease_expiry_total > 0
friend_pair_index_unrepaired > 0
```

其他自动分类建议：

```text
FAIL_CORRECTNESS
FAIL_K8S_LIFECYCLE
INCONCLUSIVE_ENVIRONMENT
INCONCLUSIVE_LOAD_GENERATOR
INCONCLUSIVE_SHARED_REDIS
INCONCLUSIVE_OBSERVABILITY
INCONCLUSIVE_DEPENDENCY
```

失败、过载和 superseded 运行不得删除、覆盖或改名。修复后的运行使用新 Run ID，并引用旧运行原因。

---

## 13. Evidence 最小集合

每次候选运行至少归档：

```text
scope-decision-reference.json
run-manifest.json
environment-manifest.json
kind-config.yaml.sha256
cluster-version.json
node-cgroup-inventory.json
source-and-image-identity.json
workload-manifest-hashes.json
configmap-hashes.json
secret-reference-hashes.json
redis-profile-and-endpoint-hash.json
pod-inventory-before.json
pod-inventory-after.json
service-endpoint-inventory.json
rollout-history.json
kubernetes-events.ndjson
planned-vs-actual.json
resource-samples.ndjson
correctness-verdict.json
authority-oracle.json
browser-witness-verdict.json
protocol-verdict.json
cleanup-verdict.json
run-report.md
evidence-hash-index.json
```

要求：

- 不保存 Secret、Redis URL、Cookie、Ticket、证书私钥和原始 Operation ID；
- Endpoint、身份和 Operation 使用域分离 Hash；
- Kubernetes events/logs 必须清理环境变量和 Secret value；
- Image Tag 不能代替 digest；
- Manifest Hash 不能代替运行中实际容器身份；
- Evidence Index 在所有附件完成后生成，并检测修改；
- Reviewer 只审 exact Evidence Head，不在证据目录原地修复。

---

## 14. 最终可声明与不可声明内容

### 14.1 本地验收全部通过后可以声明

```text
platform                         = single-linux-kind
local_kubernetes_assembly        = PASS
immutable_local_image_identity   = PASS
local_multi_pod_application_e2e  = PASS
pod_lifecycle                    = PASS
service_readiness_routing        = PASS
local_browser_origin             = PASS
local_200_wss_functional         = PASS（若对应门通过）
local_slow_reader_isolation      = PASS（若对应门通过）
local_gate_rolling_update        = PASS（若对应门通过）
local_worker_recovery            = PASS（若对应门通过）
local_dynamic_shard_migration    = PASS（仅 RT2 前置和 K8/K9 通过）
```

### 14.2 必须继续声明为未证明

```text
production_kubernetes            = NOT_PROVEN
production_capacity              = NOT_CLAIMED
multi_physical_node              = NOT_PROVEN
multi_az                         = NOT_PROVEN
production_load_balancer         = NOT_PROVEN
production_dns_tls               = NOT_PROVEN
production_redis_path            = NOT_PROVEN
3m_5m_pcu_model                  = BLOCKED_UNTIL_R2
supports_30m_dau                 = false
```

不得使用模糊的 `K8S_PASS` 同时覆盖本地编排和生产能力。

---

## 15. Writer 与 Reviewer 责任分配

### 15.1 Controller / 验收负责人

- 决定项目是否正式缩小为本地答辩范围；
- 批准 Decision/ADR；
- 指定哪些外部项是范围外而不是失败；
- 批准最终可声明和不可声明内容；
- 解决 `R2`、`S1`、`RT2Q` 等名称冲突。

### 15.2 B2 Product Writers

- FriendshipDomain 唯一 Authority；
- legacy roster 删除；
- Model Field V2 codec/layout/read-write/rollback；
- Worker readiness；
- 所有生产指标和探针语义。

Local K8s Writer 不得替这些 Owner 修改生产逻辑。

### 15.3 Local K8s / Platform Writer

建议在独立分支和 worktree 中负责：

- kind config；
- 本地不可变镜像构建/侧载脚本；
- Kubernetes manifests；
- Secret 引用，不接触明文；
- Service/NodePort/Edge；
- lifecycle 和 rollout orchestration；
- 环境/Evidence collector；
- cleanup。

不得改 Train A L2 Harness 或冻结 R1 Runtime。

### 15.4 Performance Writer

- exact Go Driver；
- open-loop planned/actual；
- Driver cgroup；
- 资源和 backlog 采集；
- 只发布合同允许的本地结论；
- 第二批后执行完整 L1-R2/L2-R2/Mixed-B1-R2。

### 15.5 Independent Reviewer

- 审范围 Decision 是否真实批准；
- 核 exact source/image/runtime 身份；
- 核共享单机和共享 Redis 限制是否被诚实披露；
- 核失败运行是否保留；
- 核动态迁移是否满足 RT2 前置；
- 核最终报告没有生产、多物理节点和 30M DAU 越界声明。

---

## 16. 开始实施前必须回答的问题

1. 谁批准用 local kind 替代原外部 Staging，Decision/ADR ID 是什么？
2. 原合同中的 `Formal K8s Staging` 是否真的要求公司集群，还是只要求正式 Evidence？
3. “第二批已合入”的实际 integration branch、merge commit 和镜像 digest 是什么？
4. Local kind 运行属于 Train B Static Staging，还是独立的 Local Acceptance Train？
5. Redis 使用独占临时实例还是授权公司 Redis？是否允许压力？
6. Worker readiness 的生产 Owner 和 exact metric/probe 合同是什么？
7. Player/Farm 需要 Deployment 还是 StatefulSet？稳定 Pod identity 是否参与 Holder 合同？
8. Static Route 和 Dynamic Route 的切换点是什么？K8/K9 何时获得授权？
9. H5 使用 HTTP/WS 还是本地 TLS/WSS？是否需要 Secure Cookie 证据？
10. Edge 采用 NodePort 直连、Ingress 还是独立 Nginx？谁负责资源和日志？
11. Load Generator 是否与服务共享物理 Host？若共享，结论如何降级？
12. 最终答辩允许声明哪些能力，明确禁止声明哪些能力？

这些问题未关闭前，只允许做无侵入环境盘点和设计，不允许发布 `LOCAL_ACCEPTANCE_PASS`。

---

## 17. 推荐执行顺序

```text
D0  范围 Decision/ADR 批准
D1  取得 B2 exact integration/image identity
D2  Train A 完成冻结 R1 对照，不受 local kind 影响
D3  B2 四项产品门分别关闭
D4  Local kind Design + independent review
D5  Harness/collector/oracle 负控
D6  K0 环境装配
D7  K1～K7 Static Local K8s
D8  RT2 前置关闭后再执行 K8/K9
D9  Local Evidence independent review
D10 使用同一容量合同执行 L1-R2/L2-R2/Mixed-B1-R2
D11 只有 R2 数据进入 3M/5M PCU 模型
```

任何 Local kind 工作都不得成为延迟或污染 R1 对照的理由。

---

## 18. 当前交接状态

```text
local_kind_scope_analysis       = DOCUMENTED
scope_change_decision           = NOT_APPROVED
local_kind_design_review        = NOT_RUN
local_kind_harness              = NOT_IMPLEMENTED
local_kind_cluster              = NOT_CREATED
local_kind_smoke                = NOT_RUN
local_kind_200_wss              = NOT_RUN
local_kind_rolling_update       = NOT_RUN
local_kind_worker_recovery      = NOT_RUN
local_kind_shard_migration      = BLOCKED_BY_RT2_PREREQUISITES

CAP-SINGLE-POD-L2-R1 design     = PASS_INDEPENDENTLY_REVIEWED
CAP-SINGLE-POD-L2-R1 harness    = IMPLEMENTATION_IN_PROGRESS
observability_qualification     = NOT_RUN
load_generator_qualification    = NOT_RUN
smoke                           = NOT_RUN
CAP-MIXED-B0-R1                 = BLOCKED
baseline_eligible               = false
capacity_claim                  = NONE
supports_30m_dau                = false
```

本文只完成分析交接。下一位 Owner 应先取得范围决策和第二批 exact integration 身份，再创建独立的 Local
K8s Design/Writer 工作包；不得从本文直接推导任何 PASS。
