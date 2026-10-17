# CP-LOCAL-ROUTE-LIVENESS-R2 G21 交接文档

日期：2026-08-11  
范围：本地 k3s / Redis Profile B / Route Controller / Player Holder / Farm Holder  
里程碑：`CP-LOCAL-ROUTE-LIVENESS-R2`  
候选名称：`CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE`  
结论：`READY_FOR_INDEPENDENT_REVIEW`

> 本文只交接本地 Holder Route Control Liveness。它不是 Active Binding、完整产品 Functional、L2 容量结果、生产 K8s 结论或 3000 万 DAU 证明。

---

## 1. 冻结身份

### 1.1 Runtime / Verification Identity

```text
branch=codex/r2-local-route-integration-20260811
verification_identity=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361
runtime_image=127.0.0.1:5001/farm/route-runtime@sha256:cc1e6d155e33948bad1b994cee568dcb5ef35822953d4d40968019ea487e2cea
worktree_at_candidate_generation=clean
```

G21 Runtime Image Receipt 中保留独立 Business Identity：

```text
business_identity=5ce43715363eea4f18e0925b6992429785053261
```

任何本文之后的纯文档提交都不得被当作 G21 Runtime Verification Identity。G21 只绑定上述 `2f16f7c...` 和精确镜像 Digest。

### 1.2 Run Identity

```text
run_id=capr2-ab-g21-20260811
profile_id=functional
generation=21
controller_nonce=78c5062d653f3cb53caffbb0
redis_namespace_environment=l2r2abg21
redis_namespace_zone=z1
```

Holder BootID：

```text
player-svr-0-functional-g21-78c5062d653f3cb53caffbb0
player-svr-1-functional-g21-78c5062d653f3cb53caffbb0
farm-svr-0-functional-g21-78c5062d653f3cb53caffbb0
farm-svr-1-functional-g21-78c5062d653f3cb53caffbb0
```

Route Controller Boot：

```text
route-controller-functional-g21-78c5062d653f3cb53caffbb0
```

该 Controller Boot 是本次本地控制面 Generation 的恢复身份；Route Controller Deployment 使用 `replicas=1`、`strategy=Recreate`，禁止两个 Publisher 重叠。

---

## 2. 当前资格矩阵

```text
route_authority_init             = PASS
static_init_2048                 = PASS
durable_snapshot_revision_1     = PASS
fresh_lease_projection          = PASS
route_control_plane_liveness    = PASS
holder_membership_liveness      = PASS
three_ttl                       = PASS
route_controller_recovery       = PENDING
gate_route_consumer_liveness    = NOT_RUN
worker_route_consumer_liveness  = NOT_RUN
full_public_command_path        = NOT_RUN
full_product_functional         = NOT_RUN
capacity_qualification          = NOT_RUN
```

治理状态必须继续保持：

```text
activatable=false
local_provisional_binding_eligible=false
active_binding_authorized=false
formal_l2_authorized=false
capacity_constant_eligible=false
supports_30m_dau=false
company_redis_contacted=false
```

---

## 3. G21 实测结果

### 3.1 Static Init / Durable Snapshot

```text
PLAYER shards                  = 1024
FARM shards                    = 1024
verified total                 = 2048/2048
first durable GlobalRevision   = 1
static receipt schema          = farm-local-static-route-init-receipt.v2
complete                       = true
bootstrap_materializable       = true
activatable                    = false
static_ms                      = 9169
render_ms                      = 218
apply_ms                       = 19515
post-apply static lease budget = 35552 ms
```

Static Init 完成：

```text
ShardWriteAuthority claim/renew
→ Exact Ledger
→ 2048 Authority read-back
→ Route Snapshot
→ Durable Envelope
→ Durable read-back
→ Checksum
→ Topology Fingerprint
→ Receipt commit
```

任何 Partial、Unknown、Checksum/Topology mismatch 都不得提交成功 Receipt。

### 3.2 Runtime Topology

```text
Route Controller = 1/1 Ready
Player Holder    = 2/2 Ready
Farm Holder      = 2/2 Ready
Pod restart      = 0
```

每个 Holder：

```text
serving_shard_count      = 512
lease_scheduler_entries  = 512
installed_memberships    = 512
pending_actions          = 0
degraded_shards          = 0
inflight_actions         = 0
fatal_error_present      = false
assignment_healthy       = true
```

### 3.3 3×TTL Formal Window

```text
lease_ttl_ms                     = 60000
required_duration_ms             = 180000
elapsed_ms                       = 180199
successful_samples               = 91
first_revision                   = 4
last_revision                    = 36
minimum_observed_remaining_ms    = 33116
status                           = HOLDER_ROUTE_CONTROL_LIVENESS_LOCAL_PASS
three_ttl_proven                 = true
```

正式 Candidate 只冻结 `180199 ms`。Window 之后继续运行的时间只能登记为：

```text
INFORMATIVE_ONLY
informative_post_window_observation_included=false
```

不得让同一 Evidence SHA 同时代表 180 秒和更长运行时长。

---

## 4. Candidate 与 Evidence

### 4.1 Candidate

```text
path=/data/farm-l2-r2/receipts/cp-local-route-runtime-r2-candidate-capr2-ab-g21-20260811.json
sha256=6e5f3fe6126555398fdf58846c241d0d9e3b441f76dbce7dc698ae2a6d3dcd2d
schema=farm-cp-local-route-runtime-r2-candidate.v2
checkpoint=CP-LOCAL-ROUTE-LIVENESS-R2
milestone=CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE
status=READY_FOR_INDEPENDENT_REVIEW
review_status=PENDING_INDEPENDENT_A_B_INTEGRATION_REVIEW
```

### 4.2 核心 Receipt / Artifact

| 项目 | 路径 | SHA-256 |
|---|---|---|
| Static Init Receipt | `/data/farm-l2-r2/receipts/static-init/capr2-ab-g21-20260811/static-init-receipt.json` | `85b98191593bdc875694372e383956b48cf79df7e004b7480d30dbb3abe8b6c2` |
| Static Route Snapshot | `/data/farm-l2-r2/runtime/static-init/capr2-ab-g21-20260811/route-snapshot.json` | `8e705d498413961e1d25a827cfee059408b5358e3a3d7a66efd32dcd4023554f` |
| Bootstrap Render | `/data/farm-l2-r2/runtime/route-bootstrap-capr2-ab-g21-20260811/bootstrap-render.yaml` | `87c31e8163db623951db783801a6fbcf02f4ac15df7dabaf95949ae98ba0f751` |
| Bootstrap Render Receipt | `/data/farm-l2-r2/receipts/route-bootstrap-render-capr2-ab-g21-20260811.json` | `535ef77891077e82f3c0c3ab056692d8298ba03c8d9331f0a5da724ccc361e57` |
| Bootstrap Apply Receipt | `/data/farm-l2-r2/receipts/route-bootstrap-apply-capr2-ab-g21-20260811.json` | `4dd5a3e295443a15aa6a13396e9b9e69d2fb6caef821052a77aad8b734dbd36d` |
| Runtime Image Receipt | `/data/farm-l2-r2/receipts/route-runtime-image-capr2-ab-g21-20260811.json` | `d914d6244a44066795ea49196b1f8d3fe69824e938922f68f2caa2c88ed33be9` |
| Route TLS Receipt | `/data/farm-l2-r2/receipts/route-control-tls-capr2-ab-g21-20260811.json` | `7f062140bcbcbdcd2bcc2665027efb480fc26b2dcb6d4fee96adfff158a338a1` |
| 3×TTL Liveness Receipt | `/data/farm-l2-r2/receipts/route-liveness-capr2-ab-g21-20260811.json` | `a837c81eb98437cc435dad84eb164d2f4685301c1a190f0755c186089d660c15` |
| First Dynamic Snapshot | `/data/farm-l2-r2/evidence/route-liveness-capr2-ab-g21-20260811/first-route-snapshot.json` | `a93ab00c87c1b4276b8de530da8f0f0d87e88d1bc9a54b0a78bad096712cf6b5` |
| Last Dynamic Snapshot | `/data/farm-l2-r2/evidence/route-liveness-capr2-ab-g21-20260811/last-route-snapshot.json` | `fe8809d229d940a6a04e93d2fd0a84f57c1959d2633fd3c89ef06cdfe48d9949` |

Post-window 诊断材料：

```text
/data/farm-l2-r2/evidence/route-bootstrap-capr2-ab-g21-20260811/post-liveness-pass
```

这些 Post-window 文件不进入 180 秒 Formal SHA，不得反向改写 Candidate。

---

## 5. 五项 Evidence Review 结果

### 5.1 Evidence Identity：PASS

Candidate Generator 已机械验证：

```text
源码 HEAD
Runtime Image Receipt
5 个 Pod imageID
Rendered Manifest SHA
Route Snapshot SHA
Static Init Receipt SHA
TLS Receipt SHA
Run/Generation/Nonce
```

5 个 Runtime Pod 的 imageID 全部精确等于：

```text
127.0.0.1:5001/farm/route-runtime@sha256:cc1e6d155e33948bad1b994cee568dcb5ef35822953d4d40968019ea487e2cea
```

### 5.2 Deadline-only Projection：PASS

Revision 4 与 Revision 36 的完整 Snapshot 已内容寻址冻结。比较结果：

允许变化并实际变化：

```text
GlobalRevision
2048 个 LeaseUntilMs
Snapshot SHA
```

保持不变：

```text
Realm
HashVersion
HashSeed
HolderID
HolderBootID
Holder Endpoint/State/Weight
HolderIndex
Shard State
AssignmentRevision
WriterEpoch
LeaseEpoch
```

结果：

```text
changed_deadlines=2048
generation_fingerprint=01ae57fa66feaedb30d783b5cdbbf5cbe54b0a84a481d52a1a08e5e840d02586
holder_identity_fingerprint=5b686fa29e339f7bea3502fe9b490b3ea4f3e7a2299b0fddcc304a1b7931b3d7
```

每个 Holder 首尾计数保持：

```text
ActionTokenSequence  = 512 → 512
SuccessfulAdds       = 512 → 512
SuccessfulReplaces   = 0 → 0
SuccessfulRemoves    = 0 → 0
SchedulerEntries     = 512 → 512
ServingShards        = 512 → 512
```

因此 Deadline-only 投影没有制造 Membership REPLACE、Guard/Scheduler churn 或伪换代。

代码中没有 `RegistrationToken` 字段；对应不变量由 HolderBootID、ActionTokenSequence、Authority Epoch 和 Scheduler Entry 共同证明。

### 5.3 单一 Publisher：PASS

```text
route_controller_publisher_count = 1
projection_attempts               = 41
projection_successes              = 41
duplicate_publisher_detected      = false
stale_callback_rejections         = 0
stale_callback_commits            = 0
partial_snapshot_publishes        = 0
pending_unknown_unresolved        = 0
deadline_rollbacks                = 0
```

该结论只覆盖 G21 Formal Window，不是多副本 HA 证明。

### 5.4 Lease Budget：PASS

实测预算：

```text
Projection Interval                 = 5000 ms
Maximum Successful Projection Wave  = 469 ms
Holder Observation Budget           = 5000 ms
Evidence Sample Interval             = 2000 ms
Scheduler Margin                     = 5000 ms
Observed Pipeline Budget             = 17469 ms
Minimum Remaining Lease              = 33116 ms
```

```text
17469 ms < 33116 ms
```

`Maximum Successful Projection Wave` 已包含本次实际 Authority 全量读取与 Durable Publish；Holder Observation Budget 覆盖 Watch 应用与 Holder Heartbeat 可观测窗口。该比较是 G21 实测预算，不替代 Fault/Soak 中的长尾分析。

### 5.5 Formal / Informative 时长分离：PASS

```text
formal_frozen_observation_ms=180199
informative_post_window_observation_included=false
```

如需登记更长运行时长，必须创建新的独立 Evidence，不得覆盖 G21 Liveness Receipt。

---

## 6. Cursor Review Finding 闭环

Cursor Grok 初次只读 Review 结论：`READY_FOR_FIX_AND_RERUN`。

### 6.1 HIGH：READY_ZERO_SHARD 绕过 Static Verifier

已修复。配置 Static Verifier 的 Registry 在首次进入 `SERVING` 时覆盖：

```text
WARMING → SERVING
READY_ZERO_SHARD → SERVING
```

验证执行两次：

```text
pre-commit verifier
→ durable heartbeat commit
→ post-commit verifier
```

动态无 Static Verifier 的 Holder 仍可使用普通：

```text
READY_ZERO_SHARD → SERVING
```

### 6.2 Verifier / Commit TOCTOU

本地 Guard 与 Redis 无法组成分布式原子事务。当前缓解：

- Commit 前验证真实 Snapshot/Guard/Gate；
- Commit 后再次验证；
- Post-commit 失败返回明确错误；
- Holder Agent 对已提交 Candidate 做 Exact Get Reconciliation；
- Start/Heartbeat 进入 Fail-closed Drain；
- Registry 永远不能授予 Actor Authority；真正准入仍由 Owned ACTIVE Guard 决定。

残余竞态应在 Fault 阶段通过 Fence/Commit 注入继续验证，但不阻塞本地 Route Liveness Checkpoint。

### 6.3 Gate 独立 WS Readiness

已对齐主 Readiness，检查：

```text
Draining
BootLeaseTrusted
Route Projection Freshness
Logic / Downstream Connectivity
```

Gate 仍未部署，因此只算代码闭环，不算 Gate Consumer Liveness PASS。

### 6.4 Static `activatable` 语义

已升级为：

```text
receipt/ledger schema=v2
bootstrap_materializable=true
activatable=false
```

### 6.5 Manager Renew 注释

已明确：Renew 不发布 Generation Change，但可发布同 Generation 的 Deadline Projection。

---

## 7. 代码验证

在 `2f16f7c6...` 上通过：

```text
go test ./...
go test -race ./internal/authoritycomposition \
  ./internal/holdercontrol \
  ./internal/routecontrol \
  ./internal/storage/redis/routecontrol \
  ./internal/gatewayapp \
  ./internal/testkit/capstaticroute \
  ./cmd/cap-static-route-init \
  ./cmd/player-svr \
  ./cmd/farm-svr \
  ./cmd/farm-gate \
  ./cmd/farm-worker \
  ./cmd/route-controller
go vet ./...
go build ./...
./scripts/local-k8s/test-tools
git diff --check
```

注：本工作区是 Git worktree，外层目录存在非仓库 `.git` 目录；精确 `go build ./...` 使用当前 worktree 的 `GIT_DIR` / `GIT_WORK_TREE` 完成 VCS stamping。`go build -buildvcs=false ./...` 同样通过，但不是上述最终记录。

---

## 8. Route Controller Restart Recovery 计划

### 8.1 目的

G21 只证明单 Controller 稳态持续投影；`restart_count=0` 不证明 Durable Restart Recovery。

待验证链：

```text
删除唯一 Route Controller Pod
→ Holder Authority Scheduler 继续续租 Redis
→ 新 Controller 从 Durable Snapshot 恢复
→ Manager 强读完整 2048 Authority
→ 第一次 Fresh Projection 成功
→ Consumer Watch 自动重连
→ KnownFresh 恢复
→ Revision 继续向前
```

这不是 HA、Leader Election、Redis Failover、容量测试或生产故障认证。

### 8.2 前置条件

1. 不修改 G21 Candidate、Liveness Receipt 或首尾 Snapshot。
2. 生成独立 Recovery Addendum：

```text
/data/farm-l2-r2/receipts/route-controller-recovery-capr2-ab-g21-20260811.json
```

3. 测试前记录：
   - Controller Pod UID；
   - Controller Revision / KnownFresh / Minimum Lease；
   - 4 个 Holder Pod UID；
   - 4 个 HolderBootID；
   - 4 个 Holder MembershipEvidenceStats；
   - First recovery Snapshot Generation Fingerprint；
   - 当前 Publisher/Failure Counter。
4. 计算删除时允许的最大 Outage Budget；不得在低 Lease Budget 时开始。
5. 公司 Redis 继续零访问，只允许本地 Profile B。

### 8.3 故障动作

唯一允许的故障动作：

```bash
KUBECONFIG=/data/farm-l2-r2/runtime/kubeconfig \
  kubectl -n farm-l2-r2 delete pod \
  -l app.kubernetes.io/name=route-controller \
  --wait=true
```

执行器必须在动作前机械确认：

```text
context=farm-l2-r2
namespace=farm-l2-r2
route-controller pods=1
Deployment replicas=1
strategy=Recreate
```

禁止：

- 修改 Deployment、ConfigMap、Secret、镜像、资源或环境变量；
- rollout restart Player/Farm；
- 删除 Holder Pod；
- 清理 Redis；
- `FLUSHALL` / `FLUSHDB` / `SCRIPT FLUSH`；
- 运行 Registry/Evidence GC。

### 8.4 成功标准

必须全部满足：

```text
old_controller_pod_uid != new_controller_pod_uid
controller_pod_count always <= 1
new_controller_ready = true
known_fresh re-established = true
post_recovery_revision > pre_delete_revision
observed_shards = 2048
minimum_remaining_lease >= 15000 ms
controller_outage_ms < pre_delete_minimum_remaining_lease_ms
projection_attempts continue
projection_successes continue
publish_pending_failures = 0
partial_snapshot_publishes = 0
stale_callback_commits = 0
deadline_rollbacks = 0
```

Holder 必须保持：

```text
Player/Farm Pod UID unchanged
HolderID unchanged
HolderBootID unchanged
AssignmentRevision unchanged
WriterEpoch unchanged
LeaseEpoch unchanged
HashVersion/HashSeed unchanged
Topology unchanged
ServingShards 512 each
SchedulerEntries 512 each
ActionTokenSequence unchanged
SuccessfulReplaces 0
SuccessfulRemoves 0
Pending/Degraded/Inflight 0
Holder restart 0
```

Controller 允许变化：

```text
Controller Pod UID
Controller process start time
GlobalRevision
Lease Deadline
Snapshot SHA
```

### 8.5 失败和停止条件

以下任一出现立即停止并保存 Evidence：

```text
Controller overlap > 1
Controller 无法读取 Durable Snapshot
Revision rollback/reset to 1
KnownFresh 未在 Lease Budget 内恢复
Holder Ready=false
Holder restart > 0
Holder Registry DRAINING/DRAINED
Membership Replace/Remove > 0
ActionTokenSequence 改变
SchedulerEntries != 512
Partial Snapshot
Pending Unknown 未解决
Deadline rollback
Pod/Image identity 非预期变化
```

失败后：

1. 保存 Controller previous/current log；
2. 保存 Holder log、Events、Workloads、首尾 Evidence；
3. 回滚/停止产品工作负载；
4. 后续重跑必须使用全新 Namespace、Generation、Nonce、TLS 和 HolderBootID；
5. 不得复用 G21 作为失败后的重跑身份。

### 8.6 Recovery Receipt 建议字段

```json
{
  "schema_version": "farm-local-route-controller-recovery.v1",
  "base_candidate_sha256": "6e5f3fe6126555398fdf58846c241d0d9e3b441f76dbce7dc698ae2a6d3dcd2d",
  "verification_identity": "2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361",
  "status": "PASS_OR_BLOCKED",
  "old_controller_pod_uid": "...",
  "new_controller_pod_uid": "...",
  "pre_delete_revision": 0,
  "post_recovery_revision": 0,
  "outage_ms": 0,
  "known_fresh_recovery_ms": 0,
  "pre_delete_minimum_remaining_lease_ms": 0,
  "minimum_observed_recovery_lease_ms": 0,
  "holder_identity_unchanged": false,
  "membership_generation_unchanged": false,
  "membership_churn_detected": false,
  "publisher_overlap_detected": false,
  "activatable": false,
  "active_binding_authorized": false,
  "supports_30m_dau": false,
  "company_redis_contacted": false,
  "evidence": {}
}
```

Recovery PASS 只把：

```text
route_controller_recovery=PENDING
```

升级为：

```text
route_controller_recovery=PASS
```

不得自动打开任何 Binding 或 L2 权限。

---

## 9. Recovery 后的执行顺序

```text
1. Route Controller Restart Recovery
2. 独立 A/B/Integration Review
3. 单 Worker Route Consumer 接入
4. 单 Gate Route Consumer 接入
5. H5 正式静态 Build 接入
6. Full Product Functional
7. F1/F2/F3 + Authority Oracle
8. Agent C Prometheus/Collector/Oracle/Evidence Writer
9. 四服务合法 Driver
10. CPU Capacity Profile 隔离
11. Single-Pod L2
12. 1/2 Pod Scale Curve
13. Mixed/Fault/Soak
14. 本地 30M DAU 可复算模型
```

### 9.1 Worker 下一门

必须证明：

```text
Route Fresh → 可以 Claim 新 Work
Route KnownFresh=false → 停止 Claim 新 Due/Lease
已持有 Work → 有界完成或释放
Route 恢复 → 恢复 Claim
3×TTL 后无 actor route: holder lease expired
```

### 9.2 单 Gate 下一门

必须证明：

```text
Gate 观察 Manager Revision
Gate Deadline Fresh
命令路由到正确 Player/Farm Holder
Route stale 时停止新 Actor 命令准入
主 HTTP / 独立 WS Readiness 同时 Fail Closed
```

### 9.3 H5 / Full Functional

至少覆盖：

```text
注册/登录
Cookie/Ticket
WSS
ClientHello
Bootstrap/Join
Snapshot/SubscriptionReady
BUY/PLANT/HARVEST
好友访问
WATER/THEFT
返回自己农场
Hard Refresh
```

并要求：

```text
InvalidOrder=0
StepUnknown=0
Due residual=0
Route lease expired=0
Unexpected REJOIN=0
```

---

## 10. 环境约束

当前主机正式容量范围仍只有：

```text
1 Pod
2 Pod
```

```text
4×4C=NOT_RUN_INSUFFICIENT_HOST
```

本地 Redis 当前 CPU 布局仍不是最终容量 Profile；L2 前必须重新执行 Target CPU Anchor 和 Dependency CPU 重排。

公司 Redis 继续等待 Owner 机器可读授权。授权前禁止：

- 读取 `deploy/secrets/redis.conf.local`；
- 建连、PING、探针、Fixture、SCAN、压力或 Cleanup；
- `FLUSHALL`、`FLUSHDB`、`SCRIPT FLUSH`。

---

## 11. 最终结论

G21 已完成并冻结：

```text
Redis Authority 2048/2048
Durable Snapshot Revision 1
唯一 Route Manager Fresh Lease Projection
Player/Farm 4 Holder Membership Liveness
Deadline-only Projection 无伪换代
单 Publisher 稳态
3×TTL Formal Window
完整 Evidence Identity Binding
```

当前可以准确称为：

```text
CP-LOCAL-ROUTE-LIVENESS-R2=PASS
CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE=READY_FOR_INDEPENDENT_REVIEW
```

尚不能称为：

```text
FULL_PRODUCT_ROUTE_LIVENESS_LOCAL_PASS
CP-LOCAL-K8S-FUNCTIONAL-R2
R2_LOCAL_L2_ACTIVE_BINDING
supports_30m_dau=true
```

下一执行项：`Route Controller Restart Recovery`。
