# R2 本地 K8s 容量实验室实施接手文档 — 2026-08-10

状态：`TAKEOVER_REQUIRED_IMPLEMENTATION_INCOMPLETE_FAIL_CLOSED`

本文档用于交接尚未完成的 R2 本地容量实验室实施工作，记录 Git 身份、主机实时状态、已完成实现、已知错误、准确阻塞项和后续执行顺序。

本文档不是资格结果，不是 Active Binding，也不构成 30M DAU 支撑声明。

---

## 1. 执行摘要

本地基础设施已经运行，但 Farm 产品栈尚未部署。

```text
k3s                         RUNNING
本地 loopback Registry      RUNNING
本地专用 Redis              RUNNING / FUNCTIONAL_ONLY
本地 TLS Secret             MATERIALIZED
本地运行时 Secret           MATERIALIZED
Backend/H5 冻结镜像         PUBLISHED TO LOCAL REGISTRY
Functional Kustomize        RENDERED AND IMAGE-BOUND
Route Authority             NOT INITIALIZED
Farm 产品工作负载           NOT DEPLOYED
Prometheus/Collector        NOT DEPLOYED
四服务合法 Driver Adapter   NOT IMPLEMENTED
公司 Redis                  NOT CONTACTED
L2/Formal/Mixed/Soak        NOT RUN
```

当前治理事实：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
LOCAL_LAB_30M_DAU_MODEL_READY=false
```

当前已绑定镜像的 Render 被明确标记为不可激活：

```text
status=IMAGES_BOUND_ROUTE_NOT_AUTHORITY_INITIALIZED
activatable=false
cluster_applied=false
route_status=PLACEMENT_INTENT_NOT_AUTHORITY_INITIALIZED
```

不得把 `LAB_BOOTSTRAPPED_NOT_QUALIFIED` 解释成 Functional PASS 或容量资格 PASS。

---

## 2. 冻结授权状态

除非后续存在新的、机器可读的 Owner/Controller 授权收据，否则以下状态继续生效：

```text
K3S_INSTALL_AUTHORIZED=true
DRIVER_IMPLEMENTATION_AUTHORIZED=true
STATIC_INIT_IMPLEMENTATION_AUTHORIZED=true
LOCAL_REGISTRY_AUTHORIZED=true
LOCAL_FUNCTIONAL_K8S_AUTHORIZED=true

LOW_RATE_REDIS_PROBE_AUTHORIZED=PENDING_REDIS_OWNER
ACTIVE_BINDING_AUTHORIZED=false
L2_STAIRCASE_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
MIXED_AUTHORIZED=false
SOAK_AUTHORIZED=false
LOCAL_LAB_30M_DAU_MODEL_READY=false
```

### 2.1 公司 Redis 规则

公司 Redis 从未被访问。当前没有执行过以下任何行为：

- `PING`；
- 建连预检；
- 启动健康检查；
- Fixture；
- SCAN；
- Staircase；
- Formal；
- Fault；
- Mixed；
- Cleanup 请求。

Redis Owner 授权之前，不得加载或使用：

```text
deploy/secrets/redis.conf.local
```

仅仅用公司 Redis URL 启动产品 Pod，也可能产生网络连接或初始化请求，因此在当前授权状态下同样禁止。

永久禁止：

```text
FLUSHALL
FLUSHDB
SCRIPT FLUSH
```

当前运行的 Redis 是独立的本地 Profile B：

```text
PROFILE_B_LOCAL_DEDICATED_REDIS_KNEE
service=farm-local-redis.farm-l2-r2-deps.svc.cluster.local:6379
environment_class=LOCAL_LINUX_K8S_WITH_LOCAL_REDIS
capacity_constant_eligible=false
```

不得把它改名为公司 Redis，也不得把它的 Evidence 与未来 Profile A 的公司 Redis Evidence 合并。

---

## 3. Git 与身份状态

### 3.1 仓库状态

```text
workspace=/data/home/developer_026/CodeBuddy/20260519141303/farm-game
branch=codex/integration-20260803
HEAD=c395f3343d7006d32a834c307422e7b34df04a34
upstream=origin/codex/integration-20260803
ahead/behind=0/0（编写本文档前）
worktree=DIRTY
```

本文档涉及的实施改动目前都尚未提交。

严禁执行：

```text
git reset --hard
git clean -fd
```

也不得在未保存当前工作前切换分支或丢弃 Untracked 文件，因为大部分新增实现仍是 Untracked 状态。

之前已经推送的计划提交：

```text
初版规格提交=170ec42c790e4dcb6e8837750b921060b9b00a09
条件批准修订=c395f3343d7006d32a834c307422e7b34df04a34
```

### 3.2 产品与资格身份

以下身份必须继续分别登记，不得互相继承：

```text
Backend Business Head=5ce43715363eea4f18e0925b6992429785053261
Qualified Backend Evidence Head=c781775a804af0988380a9cbb0dbd758ff20f6cd
H5 Commit=f22dcb447e154e424656b12409e041c32dfb4c5c
CAP-REDIS-L1-R2 Final Head=1df5305add7df195349482546a26df93ebe80d28

Harness Reviewed Head=8f3a22e2ca355aa89096334a41abb188787e220f
Harness Review Result=7ea9dd2af2652a7ab93a675071b4baf8ec3d2d29
Harness Binary SHA256=7d1fce85aa4b462db80158ddb04ce1a3a73111937dc9f51e027ce115acee5fcb
```

当前本地镜像：

```text
Backend 本地 Image ID
sha256:672cf6862dcf06a4a5bfbb6146263b98bf04de930b113f7eb8fd8e8efb3493e9
label cap.redis.business-head=5ce43715363eea4f18e0925b6992429785053261

Backend 本地 Registry Ref
127.0.0.1:5001/farm/backend@sha256:51f9d6b527bbec16cc8bf1a61ad44a60fe524a3487f26ba7e41b30d2973f5014

H5 本地 Image ID
sha256:eb1bb441ab6c46ef647bb07e360d9f068f9728e07a82f18450ef85b633f96f80
label cap.redis.h5-head=f22dcb447e154e424656b12409e041c32dfb4c5c

H5 本地 Registry Ref
127.0.0.1:5001/farm/h5@sha256:5256ef7d2c74e2f1a38ecd3db665e8cee2f7127161d99dd5748fae7310669a13
```

当前身份缺口：

`frozen-images-local-registry.json` 只记录了 Backend Business Head，尚未把以下身份绑定到同一份经 Review 的 Local Functional Binding：

- Qualified Backend Evidence Head；
- CAP Result；
- Harness Identity；
- Verification Commit；
- Route Receipt；
- Server Configuration Hash。

不得把镜像发布收据当作完整 Binding。

---

## 4. 主机与集群实时状态

接手文档编写时观测到：

```text
主机 Logical CPU=32
主机 Physical Core=16
RAM≈62.3 GiB
cgroup=v2
Docker=29.3.1
sudo -n=available

/data free≈301.5 GB
根文件系统 free≈18.0 GB
```

SMT Sibling 配对：

```text
(0,1), (2,3), ... (30,31)
```

### 4.1 k3s

```text
version=v1.36.2+k3s1
binary SHA256=65a55ec56c24eab44383086166ec620a491952b7e23941a49ddca6e8a4c4b4de
context=farm-l2-r2
node=farm-l2-r2
node status=Ready
data-dir=/data/farm-l2-r2/k3s
kubeconfig=/data/farm-l2-r2/runtime/kubeconfig
reserved-cpus=0-3,8-15,28-31
```

Systemd Unit：

```text
/etc/systemd/system/k3s.service
```

配置 Hash：

```text
/etc/rancher/k3s/config.yaml
SHA256=f2f6f4ce5097d1f018c2664dbb1fdd1df533e11d974de66acf0ebf0b14c897dd

/etc/rancher/k3s/registries.yaml
SHA256=6c564d64cc638ee856ab2c8028b5ff8db8c0d5c58889c8a1f791fafc229abe53
```

不得把 kubeconfig 提交到 Git 或复制到 Evidence。

### 4.2 Namespace 与工作负载

```text
farm-l2-r2
  Secret/farm-local-tls
  Secret/farm-staging-secrets
  无 Gate/Player/Farm/Worker/H5 工作负载

farm-l2-r2-deps
  Deployment/farm-local-redis 1/1 Ready
  Service/farm-local-redis
```

`farm-l2-r2` 产品 Namespace 当前只有 Secret，没有应用产品 ConfigMap 或工作负载。

### 4.3 Registry

```text
container=farm-l2-r2-registry
listen=127.0.0.1:5001 only
image=registry@sha256:46faa9a1ae6813194b53921a370f2f4f8c5e1aae228a89bceafef5847a6a3278
data=/data/farm-l2-r2/registry
delete_enabled=false
GC during profile=false
```

Measurement Window 内不得执行 Registry GC、Retag、Rebuild 或 Push。

### 4.4 数据目录

```text
/data/farm-l2-r2/k3s        ≈252 MB
/data/farm-l2-r2/registry   ≈56 MB
/data/farm-l2-r2/prometheus 当前为空，尚未部署
/data/farm-l2-r2/evidence   当前为空，尚无容量运行
/data/farm-l2-r2/receipts   运行时收据
/data/farm-l2-r2/runtime    kubeconfig、TLS 材料和已绑定 Render Tree
```

TLS 私钥只存在于：

```text
/data/farm-l2-r2/runtime/tls/**
```

以及 Kubernetes TLS Secret 中。它们不是 Evidence，不得提交到 Git。

---

## 5. 运行时收据

收据位于 Git 之外：

```text
/data/farm-l2-r2/receipts
```

当前文件 Hash：

| Receipt | SHA256 | 状态 |
|---|---|---|
| `k3s-bootstrap.json` | `616cd7f9a2a9e4b7a97f6a1bc44ed75c96a621ed4193ecd687e2bf83d0ecd583` | `INSTALLED_NOT_QUALIFIED` |
| `local-registry.json` | `7f1e03c59d8bb2f21f3d3856486b05fa9fba7cc09578ef6d648167660e033464` | `RUNNING_NOT_QUALIFIED` |
| `cpu-allocation-profile-probe.json` | `1837854e2fd4376326c92c971a0d913b29bc854c88d6e26be625d5ad070a57a1` | 仅 CPU Probe PASS |
| `local-dedicated-redis.json` | `1b3e8dc81aabc2c0b02e214e924c055e27f2e36dae8be8c01f4d59dea8ae359c` | `RUNNING_FUNCTIONAL_ONLY_NOT_CAPACITY_QUALIFIED` |
| `local-tls-functional-g1.json` | `fcc90872859b3403b02abe1ea979e2907a7462893843e91dec24212218d2ac35` | `MATERIALIZED_FUNCTIONAL_ONLY` |
| `functional-secrets.json` | `92085dba5e914586fed468bded192aab887adff856358ff7b06a7c058578284c` | `MATERIALIZED_FUNCTIONAL_ONLY` |
| `frozen-images-local-registry.json` | `74158aefc30c24a70af5e1c30577bb3fbf3f264cb182944a372c788ef350fc04` | `PUBLISHED_NOT_ACTIVATED` |
| `functional-render-binding.json` | `1048dd1f1810bcbbca15767ab2371a4e1618cfa9032eec9af0cbb4309642e042` | `IMAGES_BOUND_ROUTE_NOT_AUTHORITY_INITIALIZED` |

Materializer 重新运行后，Receipt 会发生变化。每次获授权变更后都必须重新计算并登记文件 Hash。

不得在运行代次变化后继续引用旧 Receipt Hash。

---

## 6. CPU 分配：已证明 Profile 与当前实时分配

CPU Manager Probe 在先创建 Target Probe Pod、后创建 Dependency Probe Pod 的顺序下证明了：

```text
Target request=4 CPU
Target container cpuset=4-7
Dependency request=10 CPU
Dependency container cpuset=16-25
Driver reserved=8-15
Observer reserved=28-31
Unallocated exclusive safety=26-27
```

这只是 Probe 结果：

- 不是当前 Redis 的实际分配；
- 不是 Formal CPU Isolation Receipt。

### 6.1 当前关键冲突

Probe Pod 删除后才启动了长期运行的本地 Redis Pod。

当前 Redis Container 的实际分配是：

```text
cpuset.cpus.effective=4-5
```

因此，本地 Redis 当前占用了原计划留给 Target 的物理核组。

不得因为之前 Probe 通过，就宣称当前 Dependency 位于 `16-25`。

进入任何容量 Profile 之前必须修正启动顺序：

1. 停止新 Arrivals，并确认当前不在 Measurement Window；
2. 保存并 Hash 相关 Receipt；
3. 仅在获授权的 Profile Teardown Window 内删除本地 Redis Pod/Deployment；
4. 先创建 4-CPU Target Pod，使 CPU Manager 分配 `4-7`；
5. 再创建 Dependency Pod，使它们从 `16-25` 开始分配；
6. 保存每个工作负载的 Container 级 cgroup cpuset；
7. 如 SMT Sibling 被拆分，或 Target/Driver 重叠，立即拒绝。

禁止用以下方式掩盖问题：

- 共享物理核；
- Target 使用小数 CPU；
- 接受任意 cpuset；
- 用 Functional Profile 的 CPU 结果作为 L2 常数。

当前主机的 4C Target 正式扩展曲线仅支持 1/2 Pod。

```text
4×4C=NOT_RUN_INSUFFICIENT_HOST
```

只有切换更大主机，或创建并独立 Review 的 2C Scale Profile，才能改变该结论。

---

## 7. 已实现源码改动

### 7.1 执行规格与假设

已修改：

```text
docs/verification/perf/local-k8s-r2/R2-LOCAL-K8S-CAPACITY-LAB-IMPLEMENTATION-PLAN-20260810.md
docs/verification/perf/local-k8s-r2/R2-LOCAL-K8S-LOAD-EXECUTION-SPEC-30M-DAU-20260810.md
```

已新增：

```text
deploy/load/30m-dau-assumption-manifest-v0.1.json
```

重要修订包括：

- Normal Design Join = `62,500/s`；
- Normal Stress Join = `125,000/s`；
- Fault Reconnect 与 Normal Join 分开建模；
- 5 分钟平均会话时长继续标记为 `ASSUMED`；
- 七命令 Redis 表改名为 `business_write_redis_lower_bound`；
- Redis 总 ops/bytes 增加 Session、Join、Friendship、Lease、Worker、Projection、Reconcile；
- 增加 `READ_RECOVERY` Mixed Profile；
- 增加 Player Activation、Friendship、Worker Backlog Pod 公式；
- Gate G2 Hold 改为 2–5 秒，并受 G1 Connection Candidate 硬上限约束；
- Fixture 拆为 F0/F1/F2，F2 需要单独批准；
- 当前主机正式 Scale 只允许 1/2 Pod；
- 修正 Allocator、Primary/Replica 和敏感性措辞；
- 所有 30M 输入继续标记为假设，不冒充实测常数；
- 继续明确 `supports_30m_dau=false`。

### 7.2 Redis Namespace/Cluster Slot 黄金测试

已修改：

```text
internal/storage/redis/rediskey/keyspace_test.go
```

新增：

```text
TestRunNamespacePreservesBucketSlotIsolation
```

该测试证明：

- 同一 Authority Bucket 内的 Key 继续同槽；
- 不同业务 Bucket 不会退化为一个 Slot；
- 普通 Run Namespace 仍位于 Key 前缀；
- Run Namespace 不会替换现有业务 Bucket Hash Tag；
- Namespace 变化不会破坏 Bucket 分布。

不得引入：

```text
cap:{r2-local:<run-id>}:*
```

否则整轮数据会被压入同一个 Redis Cluster Slot。

### 7.3 本地 K8s 工具

已在 `scripts/local-k8s/` 下新增：

| 工具 | 用途 | 重要行为 |
|---|---|---|
| `bootstrap-k3s` | 安装固定版本 k3s | 校验 Binary SHA；可能重启/变更集群，不得随意重跑 |
| `start-registry` | 启动本地 Registry | 只监听 Loopback，并生成 Receipt |
| `lab-status` | 输出安全状态 | 不输出 Secret；状态始终不冒充资格 PASS |
| `probe-cpu-manager` | CPU Manager 精确 Probe | 创建并删除 Probe Pod，不是 Formal Receipt |
| `render-route-intent.py` | 生成带 Boot 代次的 Placement Intent | 不访问 Redis；Authority Generation 全为零；永不可直接激活 |
| `validate-functional-render` | 校验 Git 中 Render-only Overlay | 要求 4/4 Inert Route 和 Zero Image Digest |
| `apply-local-redis` | 部署 Profile B Redis | 本地、Ephemeral EmptyDir、无容量资格 |
| `materialize-local-tls` | 创建本地 CA/Server TLS | 重跑会签发新 Server Material，活跃 Session 期间禁止执行 |
| `materialize-functional-secrets` | 创建产品运行时 Secret | 重跑会旋转全部 Key，活跃 Profile 期间禁止执行 |
| `publish-frozen-images` | 推送精确本地 Image ID | 生成 Immutable Registry Ref，但不是 Active Binding |
| `bind-functional-render` | 在 `/data` 副本绑定 Image Digest | Route 仍未初始化，所以仍不可激活 |
| `test-tools` | Shell/Python/工具测试 | 不证明产品 Functional 行为 |
| `test_route_intent.py` | Route Intent 单元测试 | 校验 Generation、Nonce 和 Inert Output |

生成的 Python `__pycache__` 已在编写本文档前删除，不得提交。

### 7.4 Functional Overlay

已新增：

```text
deploy/k8s/static-staging/overlays/local-capacity-r2/dependencies/local-redis.yaml
deploy/k8s/static-staging/overlays/local-capacity-r2/functional/**
```

Git 中的 Functional Overlay 被故意设置为：

```text
image digests=全零占位
route shard count=4/4
route global_revision=0
route shard_revision=0
writer_epoch=0
lease_epoch=0
lease_expire_at_ms=0
annotation=farm.io/activation-status: RENDER_ONLY_NOT_AUTHORITY_INITIALIZED
```

为了保证 Offline Render 测试可复现，Git 中使用了固定 Nonce：

```text
a1b2c3d4e5f60718293a4b5c
```

不得把该 Nonce 用于真实运行。真实运行必须生成新的 Controller Nonce，并确保 Workload Environment 与 Route HolderBootID 精确一致。

`bind-functional-render` 已生成：

```text
/data/farm-l2-r2/runtime/functional-bound-render.yaml
SHA256=2faf2c964894a78ebd352735d1c7851f77c19903d291f7c0798b38ec968dd569
```

该文件已经绑定真实镜像 Digest，但 Route 仍为 Inert Route，因此不得应用。

零 Generation Route 会在 RouteSnapshot 校验或 Authority 启动阶段失败，它不是部署候选。

### 7.5 Driver 框架

已新增：

```text
internal/testkit/capdriver/server.go
internal/testkit/capdriver/server_test.go
cmd/cap-driver/main.go
cmd/cap-driver/main_test.go
deploy/load/cap-driver-config.schema.json
deploy/load/cap-driver-{gate,player,farm,worker}.blocked.json
```

已实现：

- Loopback-only HTTP Server；
- `GET /health` 输出 Binary/Config SHA256、Service/Profile Identity 和 Inflight；
- `POST /operation` 严格 JSON 解码和 Service/Profile Identity 校验；
- 有界 Inflight Admission；
- Allowlisted Result/Error Contract；
- Response 不暴露原始 Player/Session/Operation Identity；
- Fail-closed Blocker State。

尚未实现：

- 合法 Gate Protocol Adapter；
- 合法 Player Adapter；
- 合法 Farm Adapter；
- 包含完整 WorkRecord/Source/DueIndex 的 Worker Fixture Adapter；
- Open-loop Launcher 集成；
- 60 个成功样本的 Driver Qualification；
- `1.5×` Headroom 证明。

四份配置当前全部使用：

```text
adapter=design_blocker
ready=false
blocker=DESIGN_BLOCKER
```

已经执行过一次 Blocked Driver Negative-control，并按预期返回 HTTP 503。

该测试没有产生业务压力，不能称为 Driver Qualification。

---

## 8. 产品工作负载尚未部署的准确原因

这不是 k3s 调度失败，也不是因为必须先使用公司 Redis。

当前部署输入被故意设置为不可激活。

Git 中的 Route 文件只是 Placement Intent。虽然 Route Row 的 `state` 是 `ACTIVE`，但以下字段全部为零：

```text
revision
writer_epoch
lease_epoch
lease deadline
```

它也没有在 Redis 中 Claim 任何 ShardWriteAuthority。

直接应用会导致：

- RouteSnapshot Validation 失败；
- Authority Runtime 启动失败；
- Pod CrashLoop 或 Readiness 失败；
- 或诱使后续实现者错误放宽校验。

合法激活要求每个 Shard 都有来自 Authority Store Transition Result 的准确值：

```text
holder_id
holder_boot_id
assignment_revision
writer_epoch
lease_epoch
lease_until_ms
```

这些字段必须来自 Authority Store 返回值，禁止由 Renderer 猜测。

Holder Identity 格式必须是：

```text
holder_id      = <statefulset-pod-name>
holder_boot_id = <pod-name>-<profile-id>-g<generation>-<controller-nonce>
```

以下三处必须精确一致：

- Workload `ACTOR_HOLDER_BOOT_ID`；
- RouteSnapshot HolderRef；
- Redis ShardWriteAuthority。

---

## 9. 关键 Route 活性阻塞

即使完成初始 Static Init，仍然存在第二个产品级阻塞。

`internal/actorrouting.StaticResolver.resolve` 会在不可变 `LeaseUntilMs` 到期后拒绝 Route：

```text
if assignment.LeaseUntilMs <= 0 || now >= assignment.LeaseUntilMs:
    return ErrLeaseExpired
```

Player/Farm Holder 进程具有本地 Authority Composition 和逐 Shard Lease Renewal 机制。

但 Gate 和 Worker 当前通过 `LoadStaticResolver` 只加载一次 ConfigMap 文件，尚未消费续租后的最新中央 Lease 投影。

后果：

1. Static Init 可以生成最初有效的 30–60 秒 RouteSnapshot；
2. Holder 进程可能在 Repository 中成功续租；
3. Gate/Worker 仍保留最初的 Lease Deadline；
4. 原 Deadline 到期后，即使 Holder 已续租，外部 Routing 仍可能返回 `ErrLeaseExpired`；
5. 定期重启 Gate/Worker 或写入远期 Lease 只是在掩盖缺陷。

在最新 Lease Liveness 到达所有 Routing Consumer 之前，正确状态必须是：

```text
PRODUCT_ROUTE_LIVENESS_BLOCKER
```

当前实现无法通过要求的：

```text
>=3× Shard Authority Lease TTL
```

Route Liveness 观测门。

合格的修复必须提供经过 Review 的、单调递增的 Whole-snapshot Projection，把 Authority Lease 状态传播给所有 Consumer；或者提供另一套经过 Review 的产品机制，在不削弱 Fence 的前提下证明最新活性。

修复必须拒绝：

- Revision Gap；
- Generation Rollback；
- Boot Mismatch；
- Unknown Commit Outcome；
- Stale Projection。

禁止以下捷径：

- 写入远期 `lease_expire_at_ms`；
- 沿用 Base Manifest 中的 2100 年 Lease；
- 忽略 `ErrLeaseExpired`；
- 每次到期前重启 Pod；
- 把单个 Holder 的本地视图冒充全局真相；
- Route Liveness Unknown 时仍把 Readiness 设置为 True。

该修复可能需要修改产品 Routing Composition，因此应进入独立产品变更列车和独立 Review，而不是在 CAP/Harness 分支中未 Review 地修改产品语义。

---

## 10. 必须实现的 Static Init

计划路径：

```text
cmd/cap-static-route-init/**
```

现有参考实现：

```text
cmd/smoke-authority-init/main.go
```

该命令只支持 1/1 Smoke Fixture，不能满足 1024/1024。

### 10.1 输入合同

Static Init 必须消费经 Review 的输入：

- Profile ID；
- Run ID；
- Generation Number；
- 新生成的 Controller Nonce；
- 精确的 1024/1024 Placement Intent；
- Route Realm/Hash Version/Hash Seed；
- 通过 Runtime Secret 或受保护输入提供的本地 Redis Profile B URL；
- 保留业务 Bucket Hash Tag 的普通 Redis Namespace Components；
- Route Manager Identity/Boot Identity；
- TTL 与 Deadline Budget；
- 四个 Holder Endpoint 与 Boot ID；
- 目标 Image/Binding Identity。

Git 中固定的 4/4 Intent 只能用于测试。

激活必须使用：

```text
player.shard_count=1024
farm.shard_count=1024
```

### 10.2 Transition 要求

对全部 2048 个 Shard：

1. 按冻结 Placement 计算精确 Holder；
2. 使用确定性的 Run-scoped Transition ID 执行 `ClaimShardAuthority`；
3. 只接受 `APPLIED` 或内容精确一致的 `ALREADY_APPLIED`；
4. 遇到 Timeout-unknown、Conflict、Fence Mismatch、Unauthorized 或 Incomplete Output 必须拒绝；
5. 校验返回的 Domain/Shard/Hash/Layout/Holder/Boot；
6. 记录 Assignment Revision、Writer Epoch、Lease Epoch、Accepted TTL 和 Lease Deadline；
7. Final RouteSnapshot 只能由已校验的返回值构造。

必须使用：

- 有界并发；
- 足以完成 2048 次本地 Redis Transition 的 Context；
- 原子输出；
- 完整度校验。

Authority Store 默认 Shard Lease 最大 TTL 是 1 分钟。

必须测量并限制以下总耗时，确保第一个 Claim 的 Shard 不会在四个 Holder Pod 启动并续租前过期：

- 2048 Shard Claim；
- Snapshot Materialization；
- Kubernetes Apply；
- Image Pull；
- Pod Startup；
- 首次 Holder Renewal。

任何 Shard 未闭合时：

```text
activatable=false
不发布 Snapshot
停止新 Arrivals
保留 Partial Ledger
```

禁止发布 Partial Snapshot。

禁止使用 FLUSH Cleanup。只能使用精确 Run Ledger Cleanup，或者保留失败 Receipt 并放弃该 Namespace。

### 10.3 Receipt 要求

初始化 Receipt 至少包含：

```text
schema/status
run_id/profile_id/generation/controller_nonce_hash
business/verification/image identities
namespace hash（不能记录原始公司 Redis URL）
realm/hash version/hash seed
player/farm shard counts
holder IDs and boot IDs
requested/accepted TTL
按精确 Status 分类的 Transition Total
最小/最大 Initial Lease Deadline
RouteSnapshot SHA256
Server Configuration SHA256
complete=true|false
activatable=true 仅允许在完整精确成功时出现
Profile B 下 company_redis_contacted=false
formal_l2_authorized=false
```

不得把以下内容写入 Git：

- 原始 Credential；
- Secret Value；
- Kubeconfig；
- Private Key；
- 敏感 Endpoint；
- 原始 Container ID；
- 原始 cgroup Path。

### 10.4 测试要求

至少增加：

- Deterministic Placement Unit Test；
- Holder Boot Generation Test；
- 4/4 本地 Integration Test；
- 1024/1024 Completeness Test；
- 使用精确 Transition ID 的 Idempotent Rerun Test；
- Partial Failure 不发布 Snapshot Test；
- Timeout-unknown Fail-closed Test；
- Holder/Boot Mismatch Rejection Test；
- Snapshot Hash/Revision Validation Test；
- 使用现有 Builder 的 Namespace/Cluster Slot Test；
- Expired Initial Lease Rejection Test；
- 默认不访问公司 Redis Test。

---

## 11. 产品部署接续步骤

### 11.1 当前禁止应用的文件

现在不得运行：

```text
kubectl apply -k deploy/k8s/static-staging/overlays/local-capacity-r2/functional
kubectl apply -f /data/farm-l2-r2/runtime/functional-bound-render.yaml
```

原因：

- 第一份仍是 Zero Image Digest 和 Inert Route；
- 第二份有真实 Image Digest，但 Route 仍未初始化。

### 11.2 必须生成的 Activated Render

Static Init 完成后，需要在以下位置创建新的 Runtime Render Tree：

```text
/data/farm-l2-r2/runtime/**
```

它必须：

- 用已校验的 1024/1024 Snapshot 替换 ConfigMap Route 文件；
- 在 StatefulSet Environment 中设置四个精确 HolderBootID；
- 使用 Immutable Backend/H5 Registry Digest；
- 只引用 Runtime Secret；
- 把 Activation Annotation 从 Render-only 改为明确的 Local Provisional 状态；
- 不包含原始 Redis URL 或 Credential；
- 保存完整 Render SHA256；
- 继续标记 `LOCAL_FUNCTIONAL_ONLY`，不得冒充生产 Active。

`ACTIVE_BINDING_AUTHORIZED=false` 禁止把它叫作生产 Active Binding，但不禁止创建准确标记的本地 Provisional Functional Binding。

### 11.3 部署顺序

建议初始顺序：

1. 校验 Lab Context 与全部 Receipt；
2. 校验本地 Redis 和 Registry 健康；
3. 生成完整 Static Init Output；
4. 创建并 Review Local Provisional Binding；
5. 应用 Route ConfigMap 和 Platform Configuration；
6. 使用精确 Boot ID 部署 Player/Farm Holder；
7. 等待 Holder Readiness 和精确本地 Lease Admission；
8. 部署 Gate/Worker/H5；
9. 校验所有 Image ID、Config、cgroup 和 Route Hash；
10. 只运行有界 Local Functional Smoke；
11. Correctness、Route、Oracle 或 Evidence 任一不确定时立即停止。

由于 Route Liveness Projection 尚未闭合，在解决该问题前的任何初始 Smoke 都必须命名为：

```text
DEV_FUNCTIONAL_NOT_QUALIFICATION
```

它不能成为：

```text
CP-LOCAL-K8S-FUNCTIONAL-R2 PASS
```

也不得让 Arrivals 持续超过已知的 Stale Projection Window。

### 11.4 Functional 检查项

至少包括：

- 每个 Pod 的 `/healthz` 和 `/readyz`；
- Login → ClientHello → JoinFarm → SubscriptionReady；
- Player Bootstrap/Read；
- Farm Snapshot 和合法 Command Path；
- Friendship `remote_player` 路径；
- Model V2 无 Fallback；
- 完整 WorkRecord 与 Recovery Path；
- Duplicate Effect=0；
- Stale Writer/Boot Rejection；
- Boot Generation Rollover；
- Holder Lease Renewal/Loss；
- Gate/Worker Fresh Route Liveness；
- Residual 和 Correctness Counter 全部为 0。

禁止：

- 隐藏性能接口；
- Direct Actor Invocation；
- Raw Redis 业务写入；
- 绕过 LoadAndClaim；
- 绕过 WriterFence。

---

## 12. Driver 接续工作

通用 Server 当前只是合同外壳。Adapter 必须走合法产品入口。

### 12.1 Gate

参考：

```text
cmd/ws-stress/main.go
```

它已经执行真实公开路径：

```text
HTTP Login → WebSocket → ClientHello → JoinFarm → FarmSnapshot/SubscriptionReady
```

不得使用 `cmd/ws-driver` 作为产品 Driver。该命令明确只是 TCONND Dumb Probe，不会讲 Farm Application Protocol。

Gate Qualification 必须拆分：

- G1 Connection/Handshake；
- G2 2–5 秒 Connection Hold，并受 G1 Hard Cap 限制；
- Join 独立 Launch Family；
- Fault Reconnect 与 Normal Join 分开。

### 12.2 Player

Player Load 仍必须通过合法的 Public Gate/Session Route。

Direct Player gRPC 或 Actor Registry 调用会绕过 Session Admission，不能形成有效容量 Evidence。

应选择主要工作由 Player 承担的公开 Command，并保留端到端 Correctness Gate。

### 12.3 Farm

Farm Load 必须通过 Gate 使用公开 Join 和 Farm Command Envelope，保留：

- Subscription；
- WriterFence；
- Participant；
- Result Semantic。

不得新增隐藏 Farm Performance RPC。

### 12.4 Worker

Worker Fixture 必须包含完整合同：

```text
WorkRecord
Source Authority Fact/Order
DueIndex
WorkType
Stable Work Identity
Expected Version/Fence
Lease Owner Token/Epoch
Participant Step
Participant Invocation Identity
```

必须执行正常 `LoadAndClaim`/Recovery Ingress。

以下方式无效：

- Raw Redis 插入不完整 Work Item；
- 没有可信 Worker Context 时直接调用 `ResumeStep`。

### 12.5 Driver Qualification

每个 Driver Binary/Profile 都必须满足：

- Immutable Binary SHA256；
- Exact Config SHA256；
- Open-loop Monotonic Scheduler；
- 分离 Planned/Launched/Unsent/Completed/Rejected/Timeout；
- 禁止 Catch-up Burst；
- 至少 60 个成功样本；
- 至少 `1.5×` Target Headroom；
- Driver CPU 与 Target Sibling 隔离；
- Driver 先满只能判定 `INCONCLUSIVE_LOAD_GENERATOR`；
- 使用前完成独立 Review。

---

## 13. Observability 与 Harness 缺口

尚未实现或部署：

```text
Prometheus
Service Scrape Configuration
Collector Adapter
Oracle Adapter
Driver Qualification Collector
本地 cap-single-pod Run Plan/Binding
以 /data/farm-l2-r2/evidence 为根的 Evidence Writer
```

在以下条件满足前不得开始 Measurement Window：

- 所有 Required Metric 都映射到真实 Series；
- Oracle I/O 已证明；
- Evidence I/O 已证明；
- Unknown Correctness 能立即停止 Arrivals。

Formal 规则继续保持：

- Target 使用 Guaranteed QoS；
- Integer CPU 且 Request==Limit；
- 不拆分 SMT Sibling；
- Measurement 中禁止 Build/Push/Rollout/Config Change/GC；
- 当前主机只执行 1/2 Pod Scale；
- Driver 满足 60 Samples 和 `1.5×` Headroom；
- 所有 Correctness/Residual/Unknown Counter 为 0；
- 至少 `3×` Authority Lease TTL 的 Route Liveness 观测；
- Profile A 使用公司 Redis 时必须另有预算授权。

---

## 14. 已执行测试

以下测试已在当前 Dirty Worktree 上通过：

```text
./scripts/local-k8s/test-tools

go test ./internal/testkit/capdriver \
        ./cmd/cap-driver \
        ./internal/storage/redis/rediskey

go vet ./internal/testkit/capdriver \
       ./cmd/cap-driver \
       ./internal/storage/redis/rediskey

go build ./cmd/cap-driver

git diff --check
```

此外通过：

- Route Intent Python Tests；
- Functional Render-only Validation；
- 30M Spec Finding Consistency Assertions；
- CPU Manager Allocation Probe；
- Local Redis Deployment Readiness；
- Blocked Driver HTTP Negative-control；
- `lab-status` Cluster/Registry/Company-Redis Assertions。

尚未运行：

```text
go test ./...
完整仓库 Vet
产品 Pod Startup
CP-LOCAL-K8S-FUNCTIONAL-R2
Boot Rollover
3×TTL Route Liveness
Driver Qualification
L2 Staircase/Formal/Mixed/Soak
公司 Redis Probe
```

不得把有限 Package Test 冒充 Repository-wide Validation。

---

## 15. 安全接手命令

首先只执行 Read-only Status：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
export KUBECONFIG=/data/farm-l2-r2/runtime/kubeconfig

git status -sb
git rev-parse HEAD
./scripts/local-k8s/lab-status
kubectl get nodes -o wide
kubectl get all,configmap,secret -n farm-l2-r2
kubectl get all -n farm-l2-r2-deps -o wide
docker ps --filter name=farm-l2-r2-registry
```

执行 Implementation Test：

```bash
./scripts/local-k8s/test-tools
go test ./internal/testkit/capdriver ./cmd/cap-driver ./internal/storage/redis/rediskey
go vet ./internal/testkit/capdriver ./cmd/cap-driver ./internal/storage/redis/rediskey
git diff --check
```

不得输出 Secret Data。

日志或 Evidence 中不得运行：

```text
kubectl get secret ... -o yaml
kubectl get secret ... -o json
```

如有需要，只查询 Key Name/Count，不查询 Value。

不得为了“看看效果”而重跑 Bootstrap、Materialization 或 Publish Script。必须先阅读脚本。

多个 `--apply`/`--publish` 模式会修改实时状态或旋转身份。

---

## 16. Cleanup 与状态保留规则

当前没有 Formal Measurement Window，但仍必须为接手保留状态。

禁止：

- 删除 `/data/farm-l2-r2`；
- 在保存 Receipt 和决定 Profile 顺序前删除本地 Redis Deployment；
- 随意删除或旋转 Runtime Secret；
- 执行 Registry GC；
- Prune 冻结 Backend/H5 Image；
- 执行 Global Docker Prune；
- 删除 Untracked Git Implementation；
- 把 Evidence 写入根分区；
- 把 TLS Private Key 或 Kubeconfig 复制进 Git。

三层 Teardown 尚未完整实现，这是当前 M2 缺口。

任何破坏性 Cleanup 之前，都必须实现或记录：

```text
Run Teardown
Profile Teardown
Lab Teardown
```

发生 RAM/Disk Pressure 时：

1. 停止新 Arrivals；
2. 保存并 Hash Evidence/Receipt；
3. 进入明确的 Cleanup Window；
4. 只删除 Allowlisted、Unreferenced Cache/Image/Evidence；
5. 不得删除 Selected Evidence 或当前已绑定镜像。

根文件系统空闲空间已从之前约 22 GiB 降到约 18 GiB。

后续必须继续把以下数据放在 `/data`：

- k3s；
- Registry；
- Prometheus；
- Evidence。

继续 Build Image 前先检查 Build Cache。Measurement 期间不得 Cleanup。

---

## 17. 推荐接续顺序

下一位实施者必须按以下顺序继续，不应再扩展无关旁支。

### P0 — 保留与 Review

1. 校验 Git Status 和 Live Receipt；
2. Review 所有 Untracked Implementation；
3. 发现 Receipt/文档 Hash 错误时按运行事实修正；
4. 继续保持公司 Redis 零访问。

### P1 — 闭合 Static Init

1. 实现 `cmd/cap-static-route-init/**`；
2. 增加完整 Unit Test 和 Local Redis Integration Test；
3. 使用新 Nonce 生成 1024/1024 Functional Intent；
4. 初始化精确的 Local Profile B Authority Record；
5. 生成不包含 Secret 的完整 Receipt；
6. 任一 Shard 不完整时不得 Apply。

### P2 — 闭合产品 Route Liveness

1. 设计 Gate/Worker/所有 Consumer 的 Fresh Lease Projection；
2. 在正确的产品变更列车中实现；
3. 独立 Review Monotonic Revision/Generation/Lease 行为；
4. 不使用远期 Deadline 或重启手段，证明 `>=3×TTL`；
5. 未闭合时继续输出 `PRODUCT_ROUTE_LIVENESS_BLOCKER`。

### P3 — Local Provisional Binding 与部署

1. 绑定全部 Business/Verification/Image/Harness/Route/Config Identity；
2. 生成 Activated Local Runtime Render；
3. 修正 CPU Admission Order/Profile；
4. 先部署 Player/Farm，再部署 Gate/Worker/H5；
5. 校验 Readiness 和 Exact cgroup；
6. 先运行有界 `DEV_FUNCTIONAL_NOT_QUALIFICATION`；
7. 全部 Functional Gate PASS 后才允许晋级。

### P4 — 合法 Driver 与 Observability

1. 从 `cmd/ws-stress` 提取合法 Gate Protocol Client；
2. 实现 Player/Farm Public-path Adapter；
3. 实现完整 Worker Fixture/Claim Path；
4. 部署 Prometheus/Collector/Oracle；
5. 每个 Driver 完成 60 Samples 和 `1.5×` Headroom；
6. 完成独立 Review。

### P5 — 受授权控制的阶段

只有存在机器可读授权后，才能依次进入：

1. 公司 Redis 低速兼容探针；
2. Active Binding 批准；
3. L2 Staircase 批准；
4. Formal 批准；
5. Mixed/Fault/Soak 批准；
6. 可复算 30M Model。

---

## 18. 本接手文档的退出条件

只有后续已签名或已提交的状态同时记录以下全部事实时，才能废止本文档：

```text
Static Init 1024/1024 完成
精确 Boot Generation 已绑定
全部 2048 Authority Record 已校验
Fresh Route Liveness 到达所有 Consumer
3×TTL Proof PASS
Local Provisional Binding Review PASS
产品工作负载已部署且 Ready
Functional Correctness PASS
四个合法 Driver Qualification PASS
Prometheus/Collector/Oracle 完整
公司 Redis 授权状态明确
Git 改动已经 Review、Commit、Push
```

在此之前必须继续保持：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
company_redis_contacted=false
```
