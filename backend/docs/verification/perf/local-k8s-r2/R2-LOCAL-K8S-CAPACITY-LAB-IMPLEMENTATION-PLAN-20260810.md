# R2 本地 Linux Kubernetes 容量实验室实施计划

日期：2026-08-10

```text
PLAN_REVIEW=CONDITIONALLY_APPROVED
PLAN_STATUS=APPROVED_FOR_IMPLEMENTATION

DRIVER_IMPLEMENTATION_AUTHORIZED=true
LOCAL_K3S_INSTALL_AUTHORIZED=true
STATIC_INIT_IMPLEMENTATION_AUTHORIZED=true
DRY_RUN_RENDER_UNIT_TEST_AUTHORIZED=true
LOCAL_FUNCTIONAL_K8S_AUTHORIZED=true
LOW_RATE_REDIS_PROBE_AUTHORIZED=PENDING_REDIS_OWNER_CONFIRMATION

COMPANY_REDIS_FUNCTIONAL_QUALIFICATION_AUTHORIZED=false
FORMAL_REDIS_PRESSURE_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
MIXED_AUTHORIZED=false
PRODUCTION_CAPACITY_CLAIM_AUTHORIZED=false
```

目标环境：`LOCAL_K8S_CAPACITY_LAB`

Redis：公司 Redis，作为外部受控依赖

路由：静态 Placement/Generation RouteSnapshot；不部署 Claim/Transfer/Rebalance Controller。Lease 活性必须由现有 Holder 续租链路和本地动态证明维持，不能一直相信启动文件中的固定到期时间。

> 当前授权立即开始 M1 四服务 Driver、M2 固定版本 k3s/Registry/Static Init 工具实现，以及 dry-run、render、单元测试。公司 Redis 在 Owner 确认前仅可准备低速探针，不得实际运行；Functional Qualification、正式 L2、Mixed 和容量声明继续阻断。

数值压测矩阵、业务 Mix、Fault/Soak 和 3000 万 DAU 换算模型见：
`R2-LOCAL-K8S-LOAD-EXECUTION-SPEC-30M-DAU-20260810.md`。两份文件必须一起审核，部署计划不得替代数值执行规格。

### 审核 P1 状态

| P1 | 计划修订 | 实现/Evidence 状态 | 正式门 |
|---|---|---|---|
| Holder Boot generation | 已改为 Controller generation nonce | OPEN | Boot rollover PASS |
| 静态 Route 长时 Lease | 已增加 3×TTL 和动态本地证明门 | OPEN；必要时进入产品级 Route Liveness Review | Functional/L2 blocker |
| CPU 隔离 | 已增加 static CPU manager/cpuset/SMT 规则 | OPEN | Resource Profile receipt |
| Redis 压力预算 | 已定义机器可执行授权 Schema | BLOCKED_BY_REDIS_OWNER | Formal blocker |
| Disk/Evidence | 已冻结 `/data`、容量和保留候选 | OPEN | Disk Profile receipt |
| Cleanup lifecycle | 已拆为 Run/Profile/Lab 三层 | 计划闭合，negative control 待实现 | Teardown review |

---

## 1. 目标与声明边界

### 1.1 最终交付

在当前 Linux 主机建立可重复创建和销毁的本地 Kubernetes 容量实验室，完成：

```text
CP-LOCAL-K8S-FUNCTIONAL-R2
CP-LOCAL-PERF-R2-SINGLE-POD
CP-LOCAL-PERF-R2-MIXED
LOCAL_LAB_CAPACITY_BASELINE
```

实验室用于：

- 验证 R2 在 Kubernetes 中的部署、Service、Readiness 和生命周期；
- 测量一个 Gate/Player/Farm/Worker Pod 的本机安全容量；
- 测量本地 Mixed 组合容量和第一瓶颈；
- 比较优化前后变化并形成初步 Pod 资源模型；
- 保留完整 Evidence、身份、负控和清理证明。

### 1.2 永久禁止的等价替换

```text
本地 K8s PASS
≠ 已上线
≠ 生产 Kubernetes 容量
≠ 公司 Redis 总容量
≠ 生产跨可用区、LB、TLS 或 Failover 证明
≠ 支持 3000 万 DAU
```

在 L2/Mixed 完成前继续保持：

```text
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
```

生产 `release` overlay 保持 `REJECTED_UNBOUND_INPUTS`；不得为了本地实验降低其门槛。

---

## 2. 当前冻结输入

### 2.1 产品和 Evidence

```text
Backend Business Head       = 5ce43715363eea4f18e0925b6992429785053261
Backend Qualified Evidence  = c781775a804af0988380a9cbb0dbd758ff20f6cd
Backend Verification Head   = b69f532b4c2f9a1eba0321f7a123e43d18630798
H5 Integration              = f22dcb447e154e424656b12409e041c32dfb4c5c
Backend local image ID      = sha256:672cf6862dcf06a4a5bfbb6146263b98bf04de930b113f7eb8fd8e8efb3493e9
H5 local image ID           = sha256:eb1bb441ab6c46ef647bb07e360d9f068f9728e07a82f18450ef85b633f96f80
Redis L1                    = CAP-REDIS-L1-R2 PASS
```

Business、Verification、Evidence 和 Runtime Image 分别登记；不得把文档/Harness 提交冒充重新构建过的产品身份。

### 2.2 Generic Harness

```text
Reviewed Harness Head       = 8f3a22e2ca355aa89096334a41abb188787e220f
Focused Review Result       = 7ea9dd2af2652a7ab93a675071b4baf8ec3d2d29
Harness Binary SHA256       = 7d1fce85aa4b462db80158ddb04ce1a3a73111937dc9f51e027ce115acee5fcb
Review P0/P1/P2             = 0/0/0
READY_FOR_CONTROLLER_BINDING= true
```

新增 Driver config identity、本地环境字段或 R2 Binding trust root 后，必须重新构建 Harness 并做窄范围 Exact-Head Review。

### 2.3 公司 Redis

```text
Local config path   = deploy/secrets/redis.conf.local
Git state           = ignored
Filesystem mode     = 0600
Required fields     = present
Endpoint class      = public/DNS-resolvable
Endpoint hash       = 3a0b20c3fd6fb9a4765f3dba814bb0124e479debe6fa98e1c842d61df23bd5ab
Database            = default
```

不在 Git、日志、Evidence 或本计划中保存地址、密码或 raw URL。

### 2.4 主机能力

```text
Kernel       = 6.6.119-49.20.tl4.x86_64
CPU          = 32 logical CPUs / 16 physical cores / 1 socket / 1 NUMA node
SMT mapping  = adjacent pairs: (0,1), (2,3), ... (30,31)
Memory       = 65312416 KiB (~62.3 GiB)
cgroup       = v2
Docker       = 29.3.1
Root disk    = /dev/vda1 xfs, ~22 GiB free, 79% used
Data disk    = /dev/vdb ext4 mounted /data, ~285 GiB free, 40% used
k3s          = not installed
```

上述磁盘和 CPU 数据是计划修订时的瞬时检查，不是运行身份；bootstrap/preflight 必须重新实测并写 Hash receipt。主机有多用户和其他任务。Formal 前必须进入安静窗口；Host background CPU、steal、clock step、IRQ/softirq 或其他压力超过合同门立即 `INCONCLUSIVE_ENVIRONMENT`。根分区不得承载 k3s、Registry、Prometheus 或 Evidence 主数据。

---

## 3. 总体架构

### 3.1 运行平面

```text
Linux Host
├── /data/farm-l2-r2/{k3s,registry,prometheus,evidence}
├── Docker local registry 127.0.0.1:5001
├── k3s single-node cluster: farm-l2-r2
│   ├── namespace farm-l2-r2
│   │   ├── farm-gate
│   │   ├── player-svr StatefulSet
│   │   ├── farm-svr StatefulSet
│   │   ├── farm-worker
│   │   ├── farm-h5
│   │   ├── static Route ConfigMap
│   │   └── Prometheus / ServiceMonitor
│   └── company Redis egress
└── isolated host-network Docker test tools
    ├── cap-single-pod Harness
    ├── Gate/Player/Farm/Worker Driver
    ├── Collector
    ├── Oracle
    └── loopback metrics proxies
```

业务 Pod 在 k3s；压测工具使用独立 Docker cgroup 和 host network。这样保持 Harness 的 credential-free loopback HTTP 合同，同时可以从宿主机只读采集 k3s/containerd Pod cgroup，并避免把 Driver CPU 算进目标 Pod。

所有 Driver/Harness/Oracle 容器必须 non-root、read-only rootfs、`cap-drop=ALL`、`no-new-privileges`，固定 pids/memory/cpuset/nofile；不得挂载 docker.sock/containerd.sock。Collector 只读挂载 `/proc` 与 `/sys/fs/cgroup`，不 privileged、不修改 cgroup。所有工具只监听 `127.0.0.1`。Controller 分配冻结端口段并持有 `PORT_RANGE_LOCK`；锁冲突时不得启动第二套工具。

### 3.2 为什么正式本地容量使用 k3s

```text
kind = Manifest/功能预演
k3s = LOCAL_K8S_CAPACITY_LAB 正式本地 L2/Mixed
```

k3s 直接使用宿主机 cgroup/containerd，避免 kind 的 Docker-node 嵌套层干扰 CPU、RSS、FD、network 和 cgroup 解释。若安装 k3s 不获批准，允许 fallback 到新建 kind，但结果必须降级为：

```text
environment_class=LOCAL_KIND_RELATIVE_BASELINE
```

不得称为最终本地单 Pod 安全容量。

---

## 4. 仓库布局与写集

### 4.1 Local Lab

```text
deploy/k8s/static-staging/overlays/local-capacity-r2/**
scripts/local-k8s/**
deploy/load/cap-single-pod-local-r2-*
docs/verification/perf/local-k8s-r2/**
```

生产 `overlays/release/**` 只读。

### 4.2 四服务 Driver

```text
cmd/cap-driver/**
internal/testkit/capdriver/**
deploy/load/cap-driver-*
docs/verification/perf/cap-single-pod/drivers/**
```

### 4.3 Static Authority Init

建议新增独立 test-only 工具：

```text
cmd/cap-static-route-init/**
internal/testkit/caprouteinit/**
```

该 test-only 写集已获条件授权；只能使用现有 Repository/RouteManager seam，并通过 Boot/Lease Independent Review。不能修改产品 Route Runtime 或 Redis Lua；若 Lease 活性需要产品改动，必须停止并另开产品级任务。

### 4.4 禁止写集

```text
业务 Handler
Actor 正确性语义
Workflow 状态机
Friendship 产品实现
Redis 业务 Lua
H5 产品业务代码
生产公网协议
生产 release overlay 门槛
```

若 Observability Qualification 证明生产指标缺失，登记独立产品 Observability 任务，不在测试工具中伪造指标。

---

## 5. Local k3s Cluster 实现

### 5.1 Cluster bootstrap

`scripts/local-k8s/bootstrap-k3s` 必须：

1. 使用固定 k3s 版本、来源 URL 和 SHA256，不执行未固定的 `curl | sh`；
2. 拒绝复用 `kind-farm-perf` 或未知 context；
3. 创建 context `farm-l2-r2`；
4. 固定 `data-dir=/data/farm-l2-r2/k3s`，拒绝落入根分区默认目录；
5. 配置 kubelet `cpu-manager-policy=static`、`full-pcpus-only`（版本支持时）、`reserved-cpus`、`system-reserved`、`kube-reserved`；
6. 记录 kubeconfig SHA、Kubernetes/k3s/containerd/kernel/cgroup/CNI/CPU/RAM；
7. 记录 `lscpu -e=CPU,CORE,SOCKET,NODE`、CPU Manager checkpoint、Pod effective cpuset、IRQ/softirq 分布和 CFS throttling；
8. 记录运行用户、时间窗口和 Host background baseline；
9. 默认不删除其他 context/cluster；
10. 支持精确 teardown，只删除本任务创建的资源。

输出：

```text
cluster-identity.json
cluster-config.sha256
host-profile.json
cpu-topology.json
cpu-allocation-receipt.json
disk-profile.json
bootstrap-verdict.json
```

### 5.1.1 L2 CPU 隔离 Profile

`L2_RESOURCE_PROFILE_V1` 候选按完整物理核的 SMT sibling pair 划分：

```text
CPU 0-3    cores 0-1   Host/k3s/Registry/IRQ
CPU 4-7    cores 2-3   当前唯一 Target Pod（4 logical CPU）
CPU 8-15   cores 4-7   当前唯一活跃 Driver（总计 8 logical CPU）
CPU 16-25  cores 8-12  Dependency Pods（总计 10 logical CPU）
CPU 26-27  core 13     未分配 exclusive 安全余量
CPU 28-31  cores 14-15 Observer/Prometheus
```

这是基于当前拓扑的候选，bootstrap 必须重新生成实际 CPU ID，不得盲写。当前候选 kubelet `reserved-cpus=0-3,8-15,28-31`，Target/Dependency 只允许使用 4-7、16-25，26-27 保持未分配；k3s/Registry/IRQ 约束在 0-3，Driver 在 8-15，Observer 在 28-31。任何 physical core 的两个 SMT siblings 只能属于同一资源组；Target 与 Driver 不得共享 sibling。Kubelet reserved CPU、systemd affinity、IRQ affinity、Docker `--cpuset-cpus` 与 Pod effective cpuset 必须互不重叠且与 receipt 精确一致。若 CPU Manager 没有把 Target 分配到完整 4-thread/2-core 目标组，Preflight 拒绝，不靠 Pod 启动顺序猜测。

内存候选预算：

```text
Host/k3s/Registry            8 GiB
Target Pod                   4 GiB
当前唯一 Active Driver       8 GiB
Dependency Pods（10 CPU）   24 GiB aggregate
Observer/Prometheus          8 GiB
未分配安全余量              >=10 GiB
```

Target Pod 使用 Guaranteed QoS、整数 CPU、request==limit；同一 L2 时只能有一个 8C/8GiB Active Driver，不能解释为四个 Driver 各占 8C。Driver 同时固定 `--cpus`、`--memory=8g`、`--pids-limit` 和 `--ulimit nofile`。Prometheus、Registry、Collector、support services 都必须进入已登记 CPU/RAM 组；实际 cgroup limit 及 working set 进入 receipt。

Mixed 不复用本 Profile；必须生成单独 `MIXED_RESOURCE_PROFILE_V1`，冻结各服务、聚合 Driver、Observer 和 system CPU/RAM/cpuset。该 Profile 未 Review 前 `MIXED_AUTHORIZED=false`。

当前主机的4C Target扩展曲线只授权1/2 Pod；4×4C 在计入8C Driver、system和dependencies后超出32 logical CPU，固定为 `NOT_RUN_INSUFFICIENT_HOST`。不得共享物理核硬跑4 Pod。完整1/2/4曲线需更大/第二台主机；另建2C Scale Profile只能形成独立趋势。

### 5.2 Local Registry

Docker 启动固定版本 Registry，监听 `127.0.0.1:5001`，数据目录固定为 `/data/farm-l2-r2/registry`。k3s containerd 使用受控 mirror 配置。Formal 三轮和同一 Profile 期间禁止 Registry GC、重建或 tag 变更；Profile 前后保存 catalog/digest receipt。

镜像必须引用 digest：

```text
127.0.0.1:5001/farm/backend@sha256:<manifest-digest>
127.0.0.1:5001/farm/h5@sha256:<manifest-digest>
127.0.0.1:5001/farm/gate-driver@sha256:<manifest-digest>
...
```

禁止 `latest/r2/test` 作为 Evidence identity。必须区分本地 Docker Image ID 与 Registry Manifest Digest。

### 5.3 Local DNS/TLS

M1/M2 开发、单元测试和 dry-run 可以使用 HTTP/WS；`CP-LOCAL-K8S-FUNCTIONAL-R2` 最终门和 Gate L2 必须使用：

```text
farm.l2.local
api.farm.l2.local
ws.farm.l2.local
local CA + HTTPS/WSS
```

本地 CA 和证书仅运行时生成，写入 K8s Secret，不提交 Git。必须真实验证 Secure Cookie、Origin、WSS Upgrade、Ingress 转发、H5 同源行为和 Gate 握手；不声明公网 TLS 能力。Player/Farm/Worker 内部 gRPC 本批保持当前传输，不顺带修改。

---

## 6. 公司 Redis 接入

### 6.1 Secret 注入

`scripts/local-k8s/materialize-secrets` 从 ignored `redis.conf.local` 读取配置，在内存中构造连接信息，通过 stdin/0600 临时文件创建 K8s Secret；不得把值放入命令行、shell trace 或 YAML Evidence。

Evidence 只记录：

```text
Secret name hash
Endpoint hash
Redis profile/config hash
ACL/TLS mode classification
Connectivity verdict
```

### 6.2 Connectivity/Profile Job

只有 Redis Owner 确认 `LOW_RATE_REDIS_PROBE_AUTHORIZED=true` 后，才执行非破坏性 Job：

- DNS/TCP/Auth/PING；
- Redis version/role/profile；
- ACL 能否执行本任务所需命令；
- unique namespace 的单 key write/read/delete；
- 低速 client/command latency；
- 禁止命令静态审计；
- 不打印地址或错误原文中的凭据。

Owner 确认前只允许 render Job 和 Secret 静态检查，不发网络请求。低速 probe/Static Init/少量 Functional 也不得继承为 Formal pressure authorization。

### 6.3 Namespace 隔离

每个尝试使用唯一 `rediskey.Namespace` 普通组件：

```text
REDIS_NAMESPACE_ENVIRONMENT=l2r2<run-id-hash>  # lower segment <=32
REDIS_NAMESPACE_ZONE=z1
```

禁止把 `{run-id}` 包在所有业务 Key 外层，否则 Redis Cluster 会把整轮数据压进一个 Slot。Namespace 必须进入现有 Authority/Workflow Bucket Hash Tag；Static Init 实现必须增加同 Bucket同 Slot、不同Bucket分散、namespace前后分布不退化、全部Key属于本run的黄金测试。黄金测试未 PASS 前不得向公司 Redis写 Fixture。

启动前必须证明 residual=0。运行时维护本 namespace exact key ledger；结束后优先按 ledger exact UNLINK/DEL。只有授权 Receipt 的 `allow_scan_cleanup=true` 时才允许以 namespace-bound bounded SCAN 做完整性复核。若为 false，只有 Redis Owner 提供等价的 exact namespace accounting，才能形成 Formal residual=0；仅有 ledger/known-index 不能排除未登记意外 key，只允许低速 Functional 或判 `INCONCLUSIVE_DEPENDENCY`。`max_namespace_keys/bytes` 同时是 Fixture 和 cleanup 的硬上限。

永久禁止：

```text
FLUSHALL
FLUSHDB
SCRIPT FLUSH
删除其他 namespace
复用 Formal namespace/cache/Due backlog
```

### 6.4 机器可执行压力授权

正式压力前必须生成 runtime-only、0600、gitignored 的授权 Receipt，并通过 reviewed Schema：

```yaml
redis_pressure_authorization:
  approval_id: "<owner-issued-id>"
  owner_identity_hash: "<sha256>"
  allowed_window:
    timezone: "<iana-timezone>"
    start: "<rfc3339>"
    end: "<rfc3339>"
  max_connections: <positive-int>
  max_ops_per_second: <positive-number>
  max_evalsha_per_second: <positive-number>
  max_request_bytes_per_second: <positive-number>
  max_response_bytes_per_second: <positive-number>
  max_namespace_keys: <positive-int>
  max_namespace_bytes: <positive-int>
  max_fixture_write_ops_per_second: <positive-number>
  max_cleanup_ops_per_second: <positive-number>
  max_cleanup_duration_seconds: <positive-int>
  max_single_key_bytes: <positive-int>
  max_single_run_minutes: <positive-int>
  allow_info_commands: true|false
  allow_scan_cleanup: true|false
```

授权 Receipt SHA 和 Schema SHA 进入 Binding/Evidence；不保存 Owner 原始账号或 Redis endpoint。字段缺失、窗口未开始/已结束、Receipt hash drift 均在发流量前拒绝。

Harness 使用 CAP-REDIS-L1-R2 Operation-bound 放大表和本轮精确 command rates 预计算：

```text
expected_redis_ops_s
  = Σ(command_arrival_rate × redis_ops_per_command)

expected_evalsha_s
  = Σ(command_arrival_rate × evalsha_per_command)

expected_request_bytes_s
  = Σ(command_arrival_rate × redis_request_bytes_per_command)

expected_response_bytes_s
  = Σ(command_arrival_rate × redis_response_bytes_per_command)
```

还必须加入 Holder Lease renew、Static Init、Fixture、Oracle/INFO 和 cleanup 的独立预算；不能只算业务命令。任一候选 tier 预计超过任一授权上限，Preflight 直接 `REJECTED_REDIS_BUDGET`。运行时按独立计数器实测，达到上限立即停止新 arrivals并判 `INCONCLUSIVE_DEPENDENCY`。

### 6.5 当前 Redis 授权边界

在 Owner Receipt 到位前：

```text
允许（仍需 Owner 低速确认）：DNS/Auth/PING、唯一 Namespace 单 key write/read/delete、
                             Static Init 低速 Fixture、少量 Functional
禁止：Staircase、Formal、Mixed、公司 Redis Fault
```

实现时固定双 Profile：

```text
PROFILE_A_COMPANY_REDIS_COMPAT
  预算内验证真实网络/ACL/延迟；预算先满为INCONCLUSIVE_DEPENDENCY
PROFILE_B_LOCAL_DEDICATED_REDIS_KNEE
  本地独占依赖，用于寻找Target Pod Knee
```

如果公司 Redis 不提供正式预算，Driver、K8s Functional 和 Harness 使用 Profile B；公司 Redis只保留低速兼容性探针。两者 Binding/Evidence/容量常数完全隔离，Profile B 环境身份为 `LOCAL_LINUX_K8S_WITH_LOCAL_REDIS`，不能与公司 Redis baseline 合并。

Collector 仅在授权允许时采集 INFO；始终采集客户端 RTT、错误、timeout、pool wait、request/response bytes 和 ops。公司 Redis或网络先满时为 `INCONCLUSIVE_DEPENDENCY`。不把共享 Redis 总 CPU/内存写成实验室 Redis 容量；CAP-REDIS fault gates 不在公司 Redis 重放。

---

## 7. 静态路由实现

### 7.1 Holder 与 Boot generation

Gate、Player、Farm、Worker 启动时读取同一份 reviewed Placement/Generation Snapshot。Player/Farm 的稳定 Holder 身份和运行代次严格分开：

```text
holder_id      = <statefulset-pod-name>
holder_boot_id = <pod-name>-<profile-id>-g<generation>-<controller-nonce>
```

`controller-nonce` 至少 96 bit、非秘密、DNS-safe，由 Controller 在 Profile activation 前生成；不能由 Pod 名称、固定字符串、启动时间秒值或 mutable tag 推导。Pod Template annotation、`ACTOR_HOLDER_BOOT_ID`、RouteSnapshot Holder row、Redis ShardWriteAuthority 和 Binding 必须 exact-match。

每个新 Holder 部署代次必须原子产生：

```text
new holder_boot_id
→ new Pod template generation annotation
→ new RouteSnapshot SHA
→ new Authority Init Receipt
→ new Redis namespace/profile run generation
→ new Binding/Activation Receipt
```

Controller 记录 Pod UID/container restart count。Pod UID 改变但 Boot generation 未改变时 readiness 必须失败，禁止让重建 Pod 继承旧 Boot。

两类测试不得合并：

```text
SAME_CONTAINER_RESTART_NEGATIVE_CONTROL
  = Pod UID不变但容器进程生命改变；K8s原环境中的旧Boot不得被新进程继承为写权。
    在没有新Boot投影前必须保持NotReady/FailClosed，不能作为自动恢复能力。

HOLDER_BOOT_ROLLOVER
  = Controller生成新Boot ID + 新Snapshot + 新Authority Init + 新Namespace/Generation；
    这是恢复路径，旧Boot的renew/commit必须被Fence拒绝。
```

在两项各自 Evidence 闭合前，不宣称 Same-Holder Restart 或 Boot rollover 已验证。

### 7.2 Static Authority Init Job

新工具读取 reviewed Route Profile 和 Controller 生成的 Boot identities，针对每个 Player/Farm virtual shard：

1. 验证 namespace/运行代次为空且授权 key/byte budget 足够；
2. 只能通过现有 Repository/`RouteAssignmentManager` trusted seam Claim，禁止 Raw Redis 写 Authority；
3. 验证 holder/boot/address/layout/hash/shard/revision/writer epoch/lease epoch 完整返回；
4. TIMEOUT_UNKNOWN 通过权威读/FenceAndClaim 解析；未解析不能成功；
5. 幂等 replay 只接受精确 ALREADY_APPLIED；
6. 把 Init/Lease Scheduler 预计 Redis ops/bytes 纳入预算；
7. 输出无 Secret 的 Authority/Route hash receipt；
8. 任一不一致阻止 Holder 启动。

K8s apply 顺序：Redis authorization/probe → Boot generation → Init Job PASS → projected Snapshot PASS → Player/Farm。工具只属于 test/control plane，不进入产品 Runtime image。

### 7.3 静态 Placement 与长期 Lease 活性

静态 Snapshot 只冻结：

```text
Realm / AuthorityLayout / Hash Profile / ShardCount
Holder placement / HolderBoot generation
AssignmentRevision / WriterEpoch / LeaseEpoch
```

Lease 活性不能靠 `lease_expire_at_ms=4102444800000` 或把 TTL 人为调大。启动 Snapshot 的 deadline 必须来自 Init 的真实 accepted TTL；Player/Farm 使用现有 `AUTHORITY_MEMBERSHIP_MODE=composed`、`NewMembershipRuntime` 和 `PerShardLeaseScheduler` 续租当前完整 generation。Claim/Transfer/Unassign/Rebalance 仍不属于本地数据面。

正式 Functional 前必须证明：Holder 续租后的本地单调 Deadline 被写热路径使用；Gate/Worker/Coordinator 的 route resolve 也读取同一 generation 的最新活性投影，而不是继续使用启动文件的固定 Unix 到期值。若当前 Resolver/Watch 只能看到启动 deadline，立即输出：

```text
PRODUCT_ROUTE_LIVENESS_BLOCKER
FORMAL_L2_AUTHORIZED=false
```

随后进入独立产品级 Route Liveness 设计/Review；禁止在 test-only 工具中伪装指标、冒充 Holder、循环改 ConfigMap 或把 Lease 改成一小时/2100年掩盖问题。

`CP-LOCAL-K8S-FUNCTIONAL-R2` 增加 `LEASE-LONG-RUN`，持续时间：

```text
duration >= 3 × ACTOR_AUTHORITY_LEASE_TTL
```

全程断言：

```text
holder_lease_expired_total = 0
membership_degraded_shards = 0
serving_shard_count = expected_per_holder
renewal_deadline_remaining_ms > lease_safety_margin_ms
route_resolve_lease_expired_total = 0
worker_participant_route_failure_total = 0
```

若现有 Prometheus 名称不同，Observability Adapter 必须一对一映射并绑定 source；缺少 source 时为 `INCONCLUSIVE_OBSERVABILITY`，不能用日志人工判 PASS。

### 7.4 冻结 Route Profile

本地实际 Functional Qualification、四服务 L2 和 Mixed 统一使用同一物理 ShardCount，避免把不同布局混成同一 `AuthorityLayoutVersion=v1`：

```text
profile_id                  = L2_STATIC_ROUTE_PROFILE_V1
authority_layout_version    = v1
realm                       = classic-farm-local-l2-r2
hash_version                = 2
hash_seed                   = 202608030001
player_shard_count          = 1024
farm_shard_count            = 1024
lease_ttl                   = 30s
renew_interval              = 10s
safety_margin               = 5s
membership_mode             = composed
```

1024/1024 是本地列车冻结值，不是生产最终 ShardCount。每个 Profile 的 ServingShardCount：

| Profile | Player holders × shards | Farm holders × shards |
|---|---|---|
| functional | 2 × 512 | 2 × 512 |
| l2-gate | 2 × 512 | 2 × 512 |
| l2-player | target 1 × 1024 | dependency 2 × 512 |
| l2-farm | dependency 2 × 512 | target 1 × 1024 |
| l2-worker | dependency 2 × 512 | dependency 2 × 512 |
| mixed | 2 × 512 | 2 × 512 |

Shard 分配必须由 hash/holder-index renderer 确定，不允许手写漏项。每个 Profile 有独立 RouteSnapshot SHA、Boot generations、Authority Init Receipt、Redis namespace 和 Binding。Formal 期间 Snapshot 的 Placement/Generation 不变；续租只推进活性 deadline，不改变 generation。

4/4 小拓扑只允许离线 render/unit test，不运行 Redis/业务，也不进入 Functional Evidence。若未来改变任一 ShardCount/HashSeed/HashVersion，必须使用新 Route Profile、Namespace 和显式布局版本方案；当前产品只接受 frozen `AuthorityLayoutVersion=v1`，若必须并存不同物理布局则先进入产品迁移 Review，不能由测试工具私造版本。

### 7.5 静态路由限制

- 禁止 HPA；
- 禁止在同一 Placement/Generation Snapshot 下随意 scale Player/Farm；
- 不测试动态 Claim/Transfer/Rebalance；
- SAME_CONTAINER_RESTART_NEGATIVE_CONTROL 与 HOLDER_BOOT_ROLLOVER 分开；
- 单节点多副本 anti-affinity 为 `preferred`，L2 target 单副本；
- Gate/Worker 可按预审 topology 固定副本，Formal 中不可变；
- Lease Scheduler/Guard/Renewal 的 CPU 和 Redis ops 必须计入 target/dependency 指标及 Redis budget。

---

## 8. 四服务 Driver

### 8.1 统一 HTTP 合同

Driver 是 test-only 独立进程。Harness 当前合同需做窄修改，增加 config identity：

```json
GET /health
{
  "schema_version": "cap-l2-driver-health.v1",
  "binary_sha256": "<sha256>",
  "config_sha256": "<sha256>",
  "service": "gate|player|farm|worker",
  "profile": "<exact-profile>",
  "maximum_inflight": 1000
}
```

```text
POST /operation
request:  sequence / scheduled_at / service / profile / phase
response: sequence / COMPLETED|REJECTED|TIMEOUT|FAILED / allowlisted error / queue / correctness gates
```

Driver 不把 raw Operation/Session/Receipt/Workflow/Work ID 返回 Harness 或写 Evidence。

Harness amendment 必须：

- Run Plan 增加 `driver_config_sha256`；
- health exact-match binary/config/service/profile/max inflight；
- Manifest/Formal identity 增加 Driver config SHA；
- unknown field/identity drift fail closed；
- 独立窄 Review 后再进入 Binding。

### 8.2 Gate Driver

复用真实 FarmFrame 协议，不发送 malformed bytes。支持：

```text
OPEN_SESSION
JOIN_FARM
SEND_PING_FRAME
SEND_COMMAND
HOLD_CONNECTION
CLOSE_CONNECTION
SIMULATE_SLOW_READER
RECONNECT_AFTER_DRAIN
```

路径：HTTPS Login → ConnectionTicket → WSS → ClientHello → Bootstrap/Join/Ready。Session pool、frame encode/decode、socket errors、event backlog、network bytes 全部可观测。

### 8.3 Player Driver

通过现有正式 typed ingress/internal gRPC、Route Header、LoadAndClaim、Actor turn 和 Authority commit；不得直接调用 Actor 内部对象或绕过 fence。覆盖：

```text
BUY
SELL
CLAIM_TASK
GET_PLAYER_BOOTSTRAP
Player Participant steps
uniform/cold/single-hot profiles
```

存在合法内部 Ingress 和受信 Caller 时才使用。若现有内部 API 不能在不绕过产品边界的情况下独立加载 Player，停止并输出 `DESIGN_BLOCKER`，禁止隐藏性能接口、Actor 对象直调、跳过 LoadAndClaim/WriterFence 或 Raw Redis 改状态。允许的替代方案是多个高余量 Gate 经正式 FirstHop 驱动单目标 Player；Gate 作为 Dependency 单独监测。

### 8.4 Farm Driver

通过正式 FarmIngress/Coordinator/Participant/Subscription 路径：

```text
JOIN
PLANT
HARVEST
WATER
THEFT
FARM_CHANGE_FANOUT
uniform/cold/single-hot/multi-hot profiles
```

Farm 同样只能使用合法 Ingress/受信 Caller；禁止隐藏性能接口、Actor 直调、绕过 LoadAndClaim/WriterFence 或 Raw Redis 写 Actor。无法隔离时允许多个高余量 Gate 经正式 FirstHop 驱动单目标 Farm。Gate 作为支持路径时必须保持高余量并作为 Dependency 监测；不能让 Gate 先满后声称 Farm 容量。

### 8.5 Worker Driver

Worker 使用 backlog/arrival 模型：

```text
SEED_BACKLOG（测量前）
QUERY_BACKLOG
START_ARRIVALS
STOP_ARRIVALS
WAIT_DRAIN
```

按 Plant/Harvest/Care/Theft/Friend Action/Result Publish 分离。Fixture 必须通过现有 Repository/Testkit 创建完整合法事实：

```text
完整 WorkRecord
合法 SourceAuthority/来源 Fixture 事实
DueIndex
正确 WorkType/Family
稳定 Work Identity
Participant 所需完整 Fence
```

禁止只向 DueIndex 塞一个 member、Raw Redis 修改业务状态或省略来源事实；否则测到的是 InvalidOrder，不是 Worker drain capacity。

Redis/Participant delay 属于独立 Fault Controller，不混入普通 Driver。`cmd/cap-driver/**` 和 Static Init 二进制必须通过构建清单 negative check，证明未 COPY/链接进 Backend/H5 product runtime images。

---

## 9. Collector / Oracle / Prometheus

### 9.1 Collector

Host-network Collector 使用只读 host cgroup/proc 接缝，解析 k3s/containerd target Pod、Driver Docker container、Observer 和已知 dependency cgroup。采集：

- target/driver CPU、RSS、FD、network；
- Go runtime、GC、goroutine；
- primary queue、oldest age、latency；
- Redis client hook/INFO delta；
- cgroup limit/identity drift；
- host background/steal/clock/restart；
- planned/launched/unsent/completed/rejected/timeout。

原始 cgroup path、PID、container ID 只做 hash，不进入 Evidence。

### 9.2 Metrics loopback proxy

Prometheus 在 k3s 内抓取 ServiceMonitor。Controller 建立只读 port-forward/proxy 到 host loopback固定端口；Collector/Oracle 配置只引用 credential-free loopback URL。Proxy config/hash 纳入 Server Config identity。

### 9.3 Oracle

Oracle 独立采集 15 个 correctness gates 及新增 R2 gates。COUNTER_DELTA 在运行前 arm baseline；GAUGE_CURRENT 读取残留。Missing/extra/reset/negative/source error 立即：

```text
INCONCLUSIVE_OBSERVABILITY
停止新 arrivals
```

任一 correctness 非零：

```text
FAIL_CORRECTNESS
停止新 arrivals
```

必须执行负控证明 Oracle 真能停止 Harness；负控结果不能混入正式容量样本。

---

## 10. K8s Overlays

新增且只新增本地路径：

```text
deploy/k8s/static-staging/overlays/local-capacity-r2/
├── functional/
├── l2-gate/
├── l2-player/
├── l2-farm/
├── l2-worker/
└── mixed/
```

共同 patch：

- namespace `farm-l2-r2`；
- local registry digest；
- `FRIENDSHIP_SERVICE_MODE=remote_player`；
- local host/TLS；
- company Redis Secret reference 和 runtime egress patch；
- `L2_RESOURCE_PROFILE_V1`：Target 4C/4GiB Guaranteed QoS，整数 CPU，requests==limits；
- support/dependency/observer 分别绑定 CPU/RAM/cpuset receipt；
- HPA absent；
- fixed replicas；
- immutable Placement/Generation Route ConfigMap + reviewed Lease Profile；
- no Demo/fault product switches；
- Prometheus/ServiceMonitor，TSDB=`/data/farm-l2-r2/prometheus`；
- local CA + HTTPS/WSS（Functional Qualification/Gate L2 required）；
- production release resources不变。

Mixed overlay 必须引用独立 `MIXED_RESOURCE_PROFILE_V1`；不存在或未 Review 时 render 可成功但 activation status 必须为 `REJECTED_UNBOUND_MIXED_RESOURCE_PROFILE`。

Secret、公司 CIDR、raw endpoint 和本机证书通过 runtime generated patch 注入，生成文件 `0600`、gitignored、结束后删除；Evidence 只保存 Hash。

---

## 11. Controller Binding

Driver 与 Lab 分别 Review PASS 后先生成 R2 本地 provisional Binding；未闭合 Formal Gate 时不得标 ACTIVE：

```text
binding_id=R2
environment_class=LOCAL_LINUX_K8S_WITH_COMPANY_REDIS
claim_scope=LOCAL_LAB_ONLY
network_path_class=HOST_LOCAL_TO_K3S
load_balancer_class=LOCAL_INGRESS
redis_path_class=HOST_TO_COMPANY_REDIS
status=BLOCKED_CONDITIONAL_APPROVAL
```

只有 Phase 1.5 PASS 后，新的 immutable Activation Receipt 才把同一 exact Binding 激活为 `ACTIVE`。

至少绑定：

- Business/Qualified Evidence/H5 identity；
- Backend/H5 Registry Manifest Digest；
- Gate/Player/Farm/Worker Driver binary+config SHA；
- Collector/Oracle/Harness binary+config SHA；
- K8s context/kubeconfig/cluster config hash；
- CPU topology/allocation、L2 Resource Profile、Disk/Evidence Profile hash；
- Route Snapshot、Boot generation、Authority Init、Lease Liveness、Server Config hash；
- Company Redis endpoint/profile/config hash 和 Redis Owner authorization receipt hash；
- network/load-balancer/redis path class；
- fixed port range/PORT_RANGE_LOCK receipt；
- Profile/Required Metrics/Stop/Verdict/Environment hash；
- Review Result Heads。

Binding SHA 写入 Harness compile-time trust root。Harness Binary 与 Binding 相互引用会产生循环，因此 Controller 另生成 immutable activation receipt：

```text
Binding SHA ↔ Harness Binary SHA ↔ Review Head ↔ Runtime Config Hashes
```

随后执行一次窄范围 Exact-Head Review。只有 Boot rollover、3×TTL Lease、CPU isolation、Redis authorization、Disk Profile 和三层 Cleanup 全部有 Evidence 时，目标才允许为：

```text
READY_FOR_R2_LOCAL_L2=true
FORMAL_L2_AUTHORIZED=true
```

缺任一项时 Binding 必须是 `BLOCKED_CONDITIONAL_APPROVAL`，不能仅凭 Driver/Lab 代码 Review 设为 ACTIVE。

---

## 12. 分阶段执行与验收

### 12.1 并行实施与串行测量边界

M1 Driver 与 M2 k3s/Registry/Static Init 可以由两个 Agent 在隔离写集/worktree 中并行实现；共享端口、Registry、cluster context 或 Redis namespace 前必须经 Controller 和锁协调。

运行时采用严格窗口：

```text
DEPLOY_WINDOW
  image build/push → manifest apply → rollout → identity/hash capture

STABILIZE_WINDOW
  all readiness/lease/metrics gates → host quiet check → frozen topology

MEASUREMENT_WINDOW
  Precheck/Warmup/Staircase/Candidate/Formal；禁止部署变更

CLEANUP_WINDOW
  Evidence hash/export → Run Cleanup
```

Functional 可以在部署完成后立即做低速检查，但不能一边 rollout 一边计量。正式 Measurement Window 内永久禁止：镜像 build/push、Pod rollout/redeploy、ConfigMap/Secret/Route 变更、Registry GC、Prometheus 重配置、container/image prune、Evidence GC、其他 Profile 启停和无关编译任务。任何 deployment revision、Pod UID、container restart、image digest 或配置 Hash 漂移都立即 `INCONCLUSIVE_ENVIRONMENT`。

Mixed 的含义是 Gate/Player/Farm/Worker 容器先全部部署并稳定后同时承载业务负载，不是“边部署边压测”。

### Phase 0：已授权的实现 Review

- Driver adapters deterministic/race/full tests；
- Local Lab scripts、固定 k3s install、Registry、CPU/data-dir 配置；
- dry-run、render、Secret scan、三层 teardown negative control；
- Static Init 默认只做 Memory/Local Redis unit/integration；公司 Redis低速唯一 namespace fixture 必须等待 Owner 确认；
- Boot generation/Pod UID drift negative tests；
- Lease 3×TTL 测试工具与指标映射实现；
- 两条分支独立 Review；
- 不运行正式压力。

### Phase 0.5：Functional Activation Gate

必须全部闭合：

```text
BOOT_GENERATION_IMPLEMENTATION_REVIEW=PASS
LEASE_LIVENESS_IMPLEMENTATION_REVIEW=PASS
CPU_ISOLATION_PROFILE=PASS
LOW_RATE_REDIS_AUTHORIZATION=PASS
DISK_EVIDENCE_PROFILE=PASS
DRIVER_AND_LAB_INDEPENDENT_REVIEW=PASS
PROVISIONAL_R2_BINDING_REVIEW=PASS
```

通过后只允许设置 `COMPANY_REDIS_FUNCTIONAL_QUALIFICATION_AUTHORIZED=true`。`FORMAL_L2_AUTHORIZED` 仍为 false。本地专用 Redis 的 Functional 已授权，但必须使用 `LOCAL_LINUX_K8S_WITH_LOCAL_REDIS` 独立身份，不能继承为公司 Redis qualification。

### Phase 1：CP-LOCAL-K8S-FUNCTIONAL-R2

只有 Phase 0.5 PASS 后运行，且最终门必须使用 local CA + HTTPS/WSS：

```text
Cluster/Registry/Secret/authorized Redis probe
→ Boot generation + Authority Init
→ Player/Farm Ready
→ LEASE-LONG-RUN >=3×TTL
→ Worker/Gate/H5 Ready
→ HTTPS Login/WSS Join
→ BUY/SELL/CLAIM_TASK/PLANT/HARVEST/WATER/THEFT
→ Friendship remote_player
→ Worker recovery
```

故障：Gate RollingUpdate/Drain、Worker Kill/Replacement、NetworkPolicy、Readiness。`SAME_CONTAINER_RESTART_NEGATIVE_CONTROL` 必须保持 fail closed；`HOLDER_BOOT_ROLLOVER` 使用新 Snapshot/Authority/Namespace 恢复。静态路由不声明跨 Holder takeover。

### Phase 1.5：Formal L2 Activation Gate

Functional 结束后必须全部闭合：

```text
CP_LOCAL_K8S_FUNCTIONAL_R2=PASS
HOLDER_BOOT_ROLLOVER=PASS
LEASE_LIVENESS_3X_TTL=PASS
ROUTE_LIVENESS_PRODUCT_BLOCKER=ABSENT
REDIS_PRESSURE_AUTHORIZATION=PASS
DISK_AND_CLEANUP_RUNTIME_EVIDENCE=PASS
R2_LOCAL_BINDING_EXACT_HEAD_REVIEW=PASS
```

通过后 Activation Receipt 才能设置：

```text
FORMAL_REDIS_PRESSURE_AUTHORIZED=true
FORMAL_L2_AUTHORIZED=true
```

### Phase 2：Observability Qualification

- Required Metrics exact set；
- Oracle exact gate set；
- cgroup/FD/GC/network 可采集；
- Redis dependency metrics；
- counter reset 和 >5s gap negative control；
- Driver/Collector/Oracle/Harness SHA 一致。

缺失即停止，不运行 Smoke。

### Phase 3：Load Generator Runtime Qualification

每个 Driver 对候选最大初始档证明至少 1.5×：

```text
actual/planned >=98%
arrival lag P99 <=50ms
60-sample slope observation complete
arrival lag/backlog/socket error slope=stable
Driver CPU <=70%
Driver RSS/FD/network 有余量
```

Driver 先满只能是 `INCONCLUSIVE_LOAD_GENERATOR`。

### Phase 4：CP-LOCAL-PERF-R2-SINGLE-POD

仅当 machine-readable Redis authorization 在有效窗口内，且 `FORMAL_L2_AUTHORIZED=true` 时运行。顺序：

```text
L2-GATE-R2
L2-PLAYER-R2
L2-FARM-R2
L2-WORKER-R2
```

每项：

```text
PRECHECK → SMOKE → WARMUP → STAIRCASE → CANDIDATE
→ FORMAL_1 → COOLDOWN → FORMAL_2 → COOLDOWN → FORMAL_3
→ INDEPENDENT REVIEW
```

### Phase 5：CP-LOCAL-PERF-R2-MIXED

四服务 L2、`MIXED_RESOURCE_PROFILE_V1` 和独立 Mixed authorization 全部 PASS 后，Controller 才能设置 `MIXED_AUTHORIZED=true`。运行固定 Mixed profile，包含登录、Join、经营、任务、好友、恢复。Driver/Dependency/Target bottleneck 分离，不能把单 Pod 常数直接相加。

### Phase 6：本地模型

只根据精确 L1/L2/Mixed 结果形成 `LOCAL_LAB_CAPACITY_BASELINE` 和扩容趋势。3M/5M 仅能作为假设模型并明确列出本地/生产差异；`supports_30m_dau` 仍由后续独立治理决定。

---

## 13. Disk、Evidence 与三层清理

### 13.1 数据目录和空间门

```text
k3s data-dir   = /data/farm-l2-r2/k3s
registry data  = /data/farm-l2-r2/registry
prometheus     = /data/farm-l2-r2/prometheus
evidence root  = /data/farm-l2-r2/evidence
```

运行前 `/data` 必须 `free >=50 GiB`。运行中任一条件触发立即停止新 arrivals并判 `INCONCLUSIVE_ENVIRONMENT`：

```text
free <30 GiB
filesystem free <10%
本轮 raw cap exceeded
活动列车 active cap exceeded
Evidence write/fsync/hash failure
```

容量上限：

```text
Functional/L2 single run raw cap = 2 GiB
Mixed single run raw cap         = 5 GiB
entire active train cap          = 50 GiB
```

根分区只保存系统和少量 0600 runtime config；preflight 检测到 k3s/containerd/Registry/TSDB/Evidence 写入根分区即拒绝。

### 13.2 Evidence 内容和保留

每轮保存 Git/镜像/Binding/Profile/Config、Cluster/host/cgroup/CPU/network path、Route/Boot/Authority/Redis authorization/namespace hashes、planned/actual、latency/resource/queue、correctness/environment/dependency verdict、Hash Index、Secret scan 和 cleanup receipt。

Git 永久保留：

```text
Manifest / Verdict / Summary / Hash Index / Cleanup Receipt / Review Result
```

原始 artifact 保留：

```text
Selected Formal/Mixed       90天
FAIL_CORRECTNESS             至少30天，或缺陷关闭后7天，取更长
INCONCLUSIVE_ENVIRONMENT     14天
普通非选中 PASS              checkpoint后14天
```

Evidence 完成 raw hash 后才允许压缩；压缩后同时保存 raw hash、compressed artifact hash、compression identity。失败 Evidence 原样保留，禁止因空间不足删除后用后续重跑冒充第一次运行。达到保留期后的删除必须生成 GC ledger/receipt，不覆盖原 Hash Index。

### 13.3 每轮 Run Cleanup

```text
停止Driver
→ 停止新arrivals并有限回收inflight
→ 导出/Hash Evidence
→ 清理本轮Redis exact namespace
→ residual audit
→ 删除本轮runtime Secret/Job/临时patch
```

不删除 k3s cluster、Registry、基础 Prometheus、固定 Profile RouteSnapshot。

### 13.4 每个 Profile Cleanup

三轮 Formal 和 Independent Review 完成后：删除该 Profile 工作负载和临时 Route/Boot Secret；复核所有 run namespace residual=0；保留 Cluster、Registry、基础 Prometheus 和已导出 Evidence。Profile cleanup 不能改变已保存运行身份。

### 13.5 整个 Lab Teardown

只有 Gate/Player/Farm/Worker L2、Mixed、Independent Review 全部完成，或用户明确取消整个列车后，才允许删除 `farm-l2-r2` cluster、local Registry、Prometheus runtime data 和 runtime certificates。先导出 Selected Evidence/Hash，再按 create ledger 精确删除；不得影响其他 context、Docker 容器、目录或 Redis namespace。每一层 cleanup 都必须有幂等和“拒绝删除非本任务资源”negative control。

### 13.6 内存/磁盘不足时的受控回收

删除文件不能直接解决进程 RAM 不足，必须先分类：

```text
RAM pressure  → 停止非活动开发容器/编译任务、卸载非当前Profile工作负载
Disk pressure → 仅按白名单删除未引用或已过保留期数据
```

允许在 Deploy/Cleanup Window 回收：

- 本任务未运行的旧 DEV Driver/Harness/临时容器；
- create ledger 中已确认非活动 Profile 的工作负载；
- 不被任何冻结 Manifest Digest 引用的本地 build cache/镜像层；
- 已超过第 13.2 节保留期且已生成 GC receipt 的非选中 artifacts；
- 本任务遗留且经 create ledger 证明不属于活动 cluster 的临时目录。

禁止删除：当前/Selected/未过期失败 Evidence、当前 Registry digest/layer、活动 Prometheus TSDB、当前 Profile namespace/Route/Authority、其他项目/用户数据。禁止在 Measurement Window 执行 `docker system prune`、Registry GC、Evidence GC、Linux `drop_caches`、动态调小资源或杀死 Target/Dependency 来“救”本轮。

若 Measurement Window 内发生 memory pressure、kubelet eviction、OOM、disk threshold 或 Evidence I/O 压力：立即停止新 arrivals，有限回收 inflight，导出并 Hash 当轮 Evidence，判 `INCONCLUSIVE_ENVIRONMENT`；只在随后 Cleanup Window 执行白名单回收，再从 Precheck 重新开始。

---

## 14. 主要风险与停止条件

| 风险 | 处理 |
|---|---|
| 公司 Redis 是共享实例 | Owner machine-readable授权；预计/实测越界即拒绝或 INCONCLUSIVE |
| Holder Boot 被 Pod 名替代 | Controller nonce + Pod UID drift gate + Boot rollover Evidence |
| 启动 Snapshot Lease 到期 | 3×TTL长跑；续租活性投影缺失即 PRODUCT_ROUTE_LIVENESS_BLOCKER |
| k3s 需要 root | 已批准固定版本安装；超出 `/data`/CPU 配置范围需再次批准；kind 仅 relative baseline |
| Target/Driver/SMT 干扰 | static CPU manager + full physical core sibling + cpuset receipt |
| 32 CPU 在 Mixed 接近资源上限 | 独立 Mixed Profile；不足则新 Profile/机器，不改 Evidence |
| `/` 仅约22GiB空闲 | 所有主数据落 `/data`；50/30GiB空间门 |
| 多用户主机噪声 | 安静窗口 + host/IRQ/steal background gate |
| cgroup/containerd 路径漂移 | Preflight 动态解析并 hash，失败即环境不确定 |
| Static Route 与 Redis Authority 不一致 | Init Job exact compare，Holder 不启动 |
| Profile间ShardCount混用 | qualification统一1024/1024；变化需新布局迁移Review |
| Run cleanup误删Cluster | Run/Profile/Lab三层create ledger与negative control |
| Player/Farm Driver 找不到合法内部入口 | 停止并交设计 Review，禁止后门/Actor 直调 |
| Required metrics 不存在 | Observability Qualification 失败，登记产品指标任务 |
| 公司 Redis 数据残留 | unique namespace + bounded cleanup + residual=0 |
| Evidence 出现 Secret/raw ID | 立即失败并隔离，不发布结果 |

---

## 15. 里程碑

```text
M0  Plan conditionally approved；M1/M2 implementation authorized
M1  Four-service Driver candidate
M2  Local k3s Lab + Boot-aware Static Init candidate
M3  Driver/Lab Independent Reviews PASS
M4  Boot/Lease implementation + CPU + low-rate Redis + Disk gates PASS
M5  Provisional Binding Review；Functional authorized
M6  CP-LOCAL-K8S-FUNCTIONAL-R2 + Boot rollover + 3×TTL PASS
M7  Redis formal budget + Active Binding Exact-Head Review；Formal L2 authorized
M8  Observability Qualification PASS
M9  Load Generator Qualification PASS
M10 Four Single-Pod L2 PASS
M11 Mixed Resource Profile + Local Mixed PASS
M12 LOCAL_LAB_CAPACITY_BASELINE published
```

任一里程碑失败，不跳过、不手工提升后续状态。

---

## 16. 已审核决策与剩余授权

| 决策 | 结论 | 冻结条件 |
|---|---|---|
| 单节点 k3s | 批准 | 仅 `LOCAL_LAB_ONLY`；kind只预演 |
| 固定版本 k3s | 批准 | 固定 binary/checksum；data-dir `/data` |
| Registry 127.0.0.1:5001 | 批准 | loopback、digest、Formal不GC/重建 |
| `cmd/cap-driver/**` 写集 | 批准 | test-only、不进产品镜像、独立Review |
| Static Authority Init | 条件批准 | 现有Repository/RouteManager seam；Boot/Lease闭合 |
| 公司 Redis压力 | 未批准 Formal | Owner机器可执行预算；当前仅待确认低速 probe |
| 本地 HTTPS/WSS | 要求 | dry-run可HTTP；Functional/Gate L2必须HTTPS/WSS |
| Target 4C/4GiB、Driver总计8C/8GiB | 条件批准 | `L2_RESOURCE_PROFILE_V1` + CPU pinning |
| host-network Docker工具 | 条件批准 | non-root/read-only/cap-drop/no socket/cpuset/PORT_RANGE_LOCK |
| Evidence策略 | 批准候选 | `/data`、数值cap/retention、正式前生成冻结Profile |

### 16.1 当前可以执行

```text
CAP-L2-R2-FOUR-SERVICE-DRIVER-ADAPTERS
R2-LOCAL-K3S-CAPACITY-LAB
固定k3s/Registry/CPU/data-dir实现
Boot-aware Static Init实现
render/dry-run/unit/race/local-Redis integration
Independent Review准备
```

### 16.2 当前继续阻断

```text
公司Redis网络探针（直到Owner确认低速权限）
CP-LOCAL-K8S-FUNCTIONAL-R2 qualification
公司Redis Staircase/Formal
四服务正式L2
Mixed
任何生产或3000万DAU声明
```

### 16.3 Formal 授权切换

Controller 分两次切换：Phase 0.5 七项实现门 PASS 后，仅设置：

```text
COMPANY_REDIS_FUNCTIONAL_QUALIFICATION_AUTHORIZED=true
```

Phase 1.5 的 Functional/Boot/3×TTL/Redis Budget/Binding 门 PASS 后，才设置：

```text
FORMAL_REDIS_PRESSURE_AUTHORIZED=true
FORMAL_L2_AUTHORIZED=true
```

`MIXED_AUTHORIZED` 仍需四服务 L2 和 Mixed Resource Profile 后单独切换。状态字段不能手工编辑或从计划批准推导。当前不产生任何容量 PASS。
