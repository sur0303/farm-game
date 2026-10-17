# qqfarm batch=64 部署与压测交接

- 日期：2026-08-17
- 集群 / Namespace：`uc-test-plus` / `developer_026-qqfarm`
- 工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816`
- 分支：`codex/scaleplan-local-kind-qqfarm-r1`
- HEAD：`9b970a00b600b58a9dca64d196f3715ca5b80dc1`
- 当前状态：`DEPLOYED / BATCH64_2500_CONTROL_FAIL / LADDER_STOPPED`

## 1. 接手后的第一条结论

2026-08-17 已按更新后的 Skill 完成 clean reset、claim 前双 Worker 预热、12 claims、Join smoke。环境当前可用，但 **batch=64 的 2500/s 控制档未达到无损 PASS**，按停止条件未上探 3000/s。

当前现场是：

```text
Farm Gate          1/1 Ready，restart=0
PlayerSvr          1/1 Ready，4 Shard，restart=0
FarmSvr            1/1 Ready，4 Shard，DOWNLINK_BATCH_SIZE=64，restart=0
Farm Worker A/B    1/1 Ready，restart=0
Route Controller   1/1 Ready，restart=0
Redis              3 主 3 从
```

2500/s 控制档（run `dkqwrtrupqtd`，player-start `1786938500000`）：

```text
attempted=ready=10000
queue-full Δ=0
unexpected_disconnects=24
Ready P95=222.35 ms（门槛 200ms；batch=32 对照 81.80 ms）
peak_active=9976
测后 1-player smoke PASS，--require-ready PASS
判定：FAIL（断线 + P95）
```

因此不能宣称 batch=64 抬高了无损拐点。3000/3500/4000 未跑。

上一轮失败现场（Worker A `stale local route`）已用 Skill 冷部署清掉：两个 Worker 在 revision 1、initial Controller 仍运行时预热 Ready，再停 Controller 并立即提交 12 claims（`ALREADY_COMMITTED`，revision 13）。不要再复用旧 revision，也不要用 `apply-r93.sh`。

## 2. 可以直接交给下一个 Agent 的提示词

```text
使用 $prepare-qqfarm-loadtest。

工作树：
/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816

只操作 Kubernetes context=uc-test-plus、namespace=developer_026-qqfarm。
当前环境无效，不要修复 Redis 或续跑；从 dedicated Redis clean reset 重新部署。
保持 1 Gate、1 PlayerSvr、1 FarmSvr、2 Farm Worker、1 Controller、4/4/4 Shard。
FarmSvr 使用 DOWNLINK_BATCH_SIZE=64。
严格按更新后的 Skill：两个 Worker 必须在 revision 1、initial Controller 仍运行时预热并 Ready；
然后停 Controller，确认 Holder/Worker Pod UID、boot ID、restart 未变化，立即提交 12 claims；
最后启动 final Controller 和 Gate。

先完成 full Join 1-player smoke 和 --require-ready，再跑 10000 玩家 Join 的
2500/s、3000/s、3500/s、4000/s 阶梯。每档使用不重叠 player-start，保存原始 JSON、
queue-full 前后值、Pod CPU/RSS/restart/readiness。任何业务错误、非预期断线、Pod 重启、
readiness 翻转都停止；queue-full 非零的档位不算无损容量。

把结果写入：
docs/verification/perf/qqfarm-downlink-batch64-20260817/
并更新本交接文档和 qqfarm-login-join-and-connection-capacity-20260816.md。
```

## 3. Skill 在哪里、包含什么

必须使用的 Skill：

```text
/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/
```

内容：

| 文件 | 作用 |
|---|---|
| `SKILL.md` | 固定 scope、两阶段 bootstrap、停止条件、压测交付项 |
| `references/runbook.md` | clean reset、精确身份、12 claims、final startup 的详细顺序 |
| `scripts/preflight.sh` | 核验 context、namespace、Redis 主从与 slots、工作树、Worker boot；`--require-ready` 额外检查完整拓扑 |
| `agents/openai.yaml` | Skill 的 Codex UI 入口与默认提示 |

本次已将以下经验补入 Skill：

1. 两个 Worker 必须在 revision 1、initial Controller 仍运行时预热到 Ready；
2. Worker 的最终 boot ID 必须在 claim 前固定；
3. 停 initial Controller 后重新核对 Holder/Worker Pod UID、boot ID、Ready、restart；
4. 任一 Pod 被 `Preempted` 或 UID 变化，禁止复用 revision 1，必须 clean reset；
5. claim 结果验证以 `revision=13 + 12 个 domain/shard tuple + epoch=1` 为准；当前 bootstrap JSON 的 claim `status` 会序列化为 `\u0001`，不要错误要求可打印字符串；
6. Worker 首次启动不得放在 claim 之后，因为 Authority 最大 TTL 为 60 秒。

Skill 已执行：

```text
quick_validate.py prepare-qqfarm-loadtest
→ Skill is valid!
```

这只证明 Skill 结构合法。新加入的“claim 前预热两个 Worker”顺序尚未完成本轮端到端 forward pass，下一位 Agent 必须用真实部署和 smoke 证明。

## 4. 工作树与版本边界

### 4.1 当前分支不是干净提交

当前 tracked 修改包括：

```text
cmd/route-controller/holder_discovery_runtime.go
cmd/ws-stress/main.go
cmd/ws-stress/main_test.go
deploy/k8s/uc-test-plus/developer_026-qqfarm/20-config.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/26-route-controller.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/40-farm-svr.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh
docs/verification/perf/qqfarm-login-join-and-connection-capacity-20260816.md
internal/logic/downlink/dispatcher_test.go
internal/scaleplan/executor.go
```

另有未跟踪本地目录和证据。它们不是本轮 batch=64 的全部内容，不得把整个 dirty tree 当成一个单变量提交。

### 4.2 batch=64 的实际改动

文件：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/40-farm-svr.yaml
```

候选配置：

```yaml
- name: DOWNLINK_BATCH_SIZE
  value: "64"
```

队列边界仍为 256，单 Gate lane 数仍为 1。本次只把一次 drain 的 item 上限从 32 调到 64，没有扩大队列，也没有增加 lane。

定向测试：

```text
go test ./internal/logic/downlink ./cmd/farm-svr ./cmd/ws-stress    PASS
kubectl apply --dry-run=client                                    PASS
git diff --check                                                  PASS
```

`internal/logic/downlink/dispatcher_test.go` 新增了 64-item collect 测试。Gate 的服务端批量上限是 256 items / 1 MiB，因此 64 未越过接收合同。

### 4.3 镜像

当前业务 Runtime：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@
sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026
```

batch=64 是环境变量，不需要重建 Runtime 镜像。

当前压力工具默认镜像：

```text
mirrors.tencent.com/uc-backend/farm-ws-stress@
sha256:8e66feebce2b741c04c00744089dd47b09eb9c6ec3c2d1f30ba1e1351973008f
```

该镜像可用于旧 `join` 路径的 batch=64 A/B。工作树中最新的 opportunity-only `thick-login` Ready 解耦修改尚未确认发布到该 digest；不得用该镜像声称最新 thick-login 口径已经上线。

## 5. 为什么必须冷部署

`DOWNLINK_BATCH_SIZE` 是 FarmSvr 启动时读取的配置。FarmSvr 当前持有 4 个 Shard，直接滚动或删除 Farm Pod 会改变 Pod UID / boot ID，并触发精确 Holder 身份与 Authority 恢复路径。

本次测试只想改变 batch，不应同时引入一次 Holder 故障迁移。因此正确方式是：

```text
所有消费者归零
→ Redis clean reset
→ FarmSvr 为 0 副本时写入 batch=64
→ 两阶段 bootstrap
→ full Join smoke
→ QPS 阶梯
```

不要为了“省时间”原地重启已分配 Farm Holder。

## 6. 稳定部署流程

### 6.1 固定变量

```bash
export QQ_REPO=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
export QQ_SKILL=/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest
export QQ_CONTEXT=uc-test-plus
export QQ_NAMESPACE=developer_026-qqfarm
```

不要把这些变量替换成 `HOME`、通配符或其他 namespace。

### 6.2 第一遍 preflight

```bash
bash "$QQ_SKILL/scripts/preflight.sh" --repo "$QQ_REPO"
```

它应证明：

- context / cluster 都是 `uc-test-plus`；
- namespace 是 `developer_026-qqfarm`；
- Redis `cluster_state=ok`、16384 slots；
- `farm-redis-0/1/2` 都是 master；
- `SCALEPLAN_MODE=off`；
- 两个 Worker Deployment 已渲染稳定 boot ID。

当前 `--require-ready` 必然失败，这是正确结果。

### 6.3 构建 bootstrap 工具

```bash
cd "$QQ_REPO"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -buildvcs=false -trimpath \
  -o /tmp/rt2q-route-bootstrap-qqfarm-b64 \
  ./cmd/rt2q-route-bootstrap
sha256sum /tmp/rt2q-route-bootstrap-qqfarm-b64
```

不要使用旧 `/tmp` 二进制，也不要使用 `deploy/.../apply-r93.sh`。旧脚本包含旧 Redis environment 和 2 Holder 拓扑，不适用于当前 qqfarm。

### 6.4 停止消费者

```bash
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" scale \
  deployment/farm-gate deployment/farm-worker-a deployment/farm-worker-b \
  deployment/route-controller --replicas=0

kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" scale \
  statefulset/player-svr statefulset/farm-svr --replicas=0
```

等待 Gate、Player、Farm、Worker、Controller Pod 全部消失。Redis、Prometheus、Grafana 可以保留。

删除旧 helper：

```bash
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" \
  delete pod farm-route-bootstrap --ignore-not-found --wait=true
```

### 6.5 只清空三个已验证 master

先逐个证明：

```text
farm-redis-0 role=master
farm-redis-1 role=master
farm-redis-2 role=master
cluster_state=ok
cluster_slots_assigned=16384
```

然后仅对这三个明确 Pod 执行：

```bash
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" exec farm-redis-0 -- redis-cli FLUSHALL
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" exec farm-redis-1 -- redis-cli FLUSHALL
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" exec farm-redis-2 -- redis-cli FLUSHALL
```

要求三个 `DBSIZE=0`。禁止使用 glob，禁止清其他 namespace。

### 6.6 在 Farm 为 0 副本时固定 batch=64

仓库 manifest 已写入 64，但 `30-player-svr.yaml` / `40-farm-svr.yaml` 包含 `PENDING_STAGED_APPLY` workload UID 占位符，不能直接原样 apply 到存量 StatefulSet。

在副本为 0 时对现有 StatefulSet 设置：

```bash
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" \
  set env statefulset/farm-svr DOWNLINK_BATCH_SIZE=64
```

保留 StatefulSet 当前真实 `ACTOR_HOLDER_WORKLOAD_UID`。

### 6.7 创建 Holder Pod、提交 revision 1

只启动：

```text
player-svr=1
farm-svr=1
Controller=0
Workers=0
Gate=0
```

等待 Pod 对象出现，记录：

```text
Player Pod UID / StatefulSet UID
Farm Pod UID / StatefulSet UID
Worker A / B 的渲染 boot ID
```

预期 Holder boot：

```text
player-svr-0-<player-pod-uid>
farm-svr-0-<farm-pod-uid>
```

创建 `redis:7-alpine` helper，复制刚构建的 bootstrap 二进制。生成同一批次共用的 32 位 lowercase hex challenge，以及 initial / assign / final 三个不同 Controller boot。

bootstrap 参数固定为：

```text
realm=classic-farm-dynamic-uc
controller-id=route-controller
redis-environment=uctestscaleplanqqfarm20260816
redis-zone=z1
shard-count=4
```

单 Player / 单 Farm 拓扑只传 `player-a` 和 `farm-a`，不要伪造 B。两 Worker 都传入。

第一阶段要求：

```text
--bootstrap-only
bootstrap_status=COMMITTED
initial_revision=1
final_revision=1
claims=null
```

### 6.8 initial Controller、Holder 和 Worker 预热屏障

启动 initial Controller。要求 Player/Farm：

```text
status=ready
assignment_state=WATCHING
assignment_healthy=true
process_ready=true
serving_shard_count=0
snapshot_revision=1
degraded_shards=0
fatal_error_present=false
restart=0
```

此时启动 Worker A 和 Worker B，保持 Gate=0。要求两个 Worker：

```text
Ready=true
restart=0
live WORKER_BOOT_ID == bootstrap 参数中的 boot ID
```

这一屏障的目的不是让 Worker 提前持有 Shard，而是提前完成调度、镜像拉取、进程启动和 route watch，避免 60 秒 claim lease 被首次启动耗尽。

如果任何事件出现 `Preempted`，或任何 Holder / Worker Pod UID 变化，立即停止并从 Redis clean reset 开始。不要重新生成参数后继续 revision 1。

### 6.9 精确 12 claims

记录四个 Pod 的 UID、boot ID、restart 和 Ready；然后停止 initial Controller，并立即再次核对完全相同。

使用同一 challenge、`--resume-existing`，对 Player A / Farm A 传入：

```text
Pod UID
StatefulSet UID
ordinal=0
HolderControl endpoint
TLS identity=holder-control.developer_026-qqfarm.svc.cluster.local
claim boot=当前精确 boot
```

要求：

```text
bootstrap_status=ALREADY_COMMITTED
initial_revision=1
final_revision=13
claims=12
PLAYER shard 0..3 各一条
FARM shard 0..3 各一条
FARM_WORKER shard 0..3 各一条
assignment_revision=1
writer_epoch=1
lease_epoch=1
```

当前工具的 `claim.status` 可能是 JSON `\u0001`。不要只因它不是字符串 `COMMITTED` 就判失败；必须以上述 revision、tuple 和 fence 对账。

### 6.10 final Controller、Gate 与最终门槛

12 claims 完成后立即启动 final Controller。两个 Worker 保持运行。先等：

```text
Player 4 Shard Ready
Farm 4 Shard Ready
Worker A/B Ready、restart=0
Controller Ready、restart=0
```

再启动 Gate，要求 Gate Ready、restart=0。

确认 Farm 进程实际读取了 64：

```bash
kubectl --context "$QQ_CONTEXT" -n "$QQ_NAMESPACE" \
  exec farm-svr-0 -- printenv DOWNLINK_BATCH_SIZE
```

输出必须是 `64`。

删除 helper，运行：

```bash
bash "$QQ_SKILL/scripts/preflight.sh" \
  --repo "$QQ_REPO" --require-ready
```

只有输出 `preflight=PASS require_ready=1` 才进入 smoke。

## 7. 压测前 smoke

### 7.1 Full Join smoke

使用未使用过的玩家 ID：

```bash
cd "$QQ_REPO"
KUBE_CONTEXT=uc-test-plus bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  join 1 1 5s <unused-player-start>
```

必须解析最后 JSON，要求：

```text
attempted=1
ready=1
login_errors=0
connect_errors=0
hello_errors=0
join_errors=0
unexpected_disconnects=0
```

`join-accepted` 不够，因为它不证明 Farm Actor 激活和 Snapshot 到达。

### 7.2 再跑 require-ready

Smoke 后再次执行：

```bash
bash "$QQ_SKILL/scripts/preflight.sh" \
  --repo "$QQ_REPO" --require-ready
```

然后记录基线 `farm_downlink_queue_full_total`。计数器是累计值，每档只看增量。

## 8. batch=64 的 Join QPS 测试

### 8.1 batch=32 对照数据

相同单实例业务拓扑、10000 玩家、15 秒旧 `join` 口径：

| 注入速率 | Ready | Ready P95 | queue-full 增量 | 判定 |
|---:|---:|---:|---:|---|
| 1500/s | 10000/10000 | 35.02 ms | 0 | PASS |
| 2500/s | 10000/10000 | 81.80 ms | 0 | 已验证无损上界 |
| 3000/s | 10000/10000 | 153.51 ms | 30 | 非无损 |
| 4000/s | 10000/10000 | 1790.48 ms | 19587 | 过载 |

这组数据来自 [qqfarm-login-join-and-connection-capacity-20260816.md](qqfarm-login-join-and-connection-capacity-20260816.md)。

### 8.2 batch=64 假设

batch=64 可能减少 FarmSvr 对单 Gate lane 的 RPC 次数，让一次出队携带更多 Snapshot/downlink item。它不改变：

- 单 lane 串行结构；
- 队列 256；
- Gate writer 能力；
- 每条消息大小；
- Player/Farm Actor 激活成本。

因此它可能提高 2500～4000/s 区间的拐点，但不能预先宣称吞吐翻倍。

### 8.3 立即执行的阶梯

每档使用新的、互不重叠的 `player-start`：

```bash
run-ws-stress.sh join 10000 2500 15s <range-1>
run-ws-stress.sh join 10000 3000 15s <range-2>
run-ws-stress.sh join 10000 3500 15s <range-3>
run-ws-stress.sh join 10000 4000 15s <range-4>
```

执行策略：

1. 2500/s 是同口径控制档；
2. 3000/s 检查原先 30 次 queue-full 是否消失；
3. 只有 3000/s 无业务错误、无 queue-full、P95 未恶化、Pod 全绿，才跑 3500/s；
4. 只有 3500/s 仍无损，才跑 4000/s；
5. 首个失败后停止上探，在通过档与失败档之间二分，不反复轰炸坏现场。

### 8.4 每档必须保存

建议证据目录：

```text
docs/verification/perf/qqfarm-downlink-batch64-20260817/
├── README.md
├── environment-before.txt
├── bootstrap-initial.json
├── bootstrap-assign.json
├── smoke-join-1.json
├── join-10000-rate2500.json
├── join-10000-rate3000.json
├── join-10000-rate3500.json
├── join-10000-rate4000.json
├── metrics-rate2500-before.txt
├── metrics-rate2500-after.txt
├── ...
└── environment-after.txt
```

每档记录：

- 压测原始 JSON 和 run ID；
- player ID 范围；
- `attempted / ready / peak_active`；
- Ready P50/P95/P99/max；
- login/connect/hello/join/snapshot/断线错误；
- queue-full 前值、后值、增量；
- Gate/Player/Farm/Worker/Controller Pod 名、UID、Node、restart、Ready；
- `kubectl top pod`；
- Gate/Player/Farm process CPU、RSS、goroutine；
- Gate writer queue / Farm downlink lane depth 与 reject；
- Redis master CPU、连接、超时、MOVED/ASK；
- 测试后 1-player Join smoke 和 `--require-ready`。

`run-ws-stress.sh` 即使业务 JSON 有错误也可能进程退出 0，必须解析 JSON，不能以 shell exit code 作为 PASS。

### 8.5 无损判定与停止条件

一档只有同时满足以下条件才算 PASS：

```text
attempted == ready == 10000
所有业务错误 == 0
非预期断线 == 0
queue-full 增量 == 0
Pod restart 增量 == 0
所有 workload Ready
P95 <= 200ms（本轮探索门槛）
测试结束后队列回到 0
最终 1-player smoke 通过
```

以下任一发生立即停止：

- Pod `CrashLoopBackOff`、OOM、Preempted 或 UID 变化；
- readiness 翻转；
- queue-full 明显增长；
- Join / Snapshot 错误；
- Redis timeout / cluster state 异常；
- 压测客户端 Pod 自身端口或资源成为瓶颈。

## 9. Thick Login 与机会弹窗不要混入 batch A/B

batch=64 要回答的是旧完整 Join/Snapshot 注入的下行瓶颈，不要同时改变 thick-login 行为或好友数据集。

当前 opportunity-only 证据：

- 300 个全新 viewer；
- 每人 50 好友；
- 100/s；
- 300/300 Ready，机会 FOUND 300/300，错误 0；
- Ready P95 `1629.67 ms`；
- 机会 P95 `1473.48 ms`。

服务端定位：

| 路径 | 次数 | 平均服务时间 |
|---|---:|---:|
| Player `GetNextFriendOpportunity` | 300 | 678.67 ms |
| Farm Header | 600 | 2.52 ms |
| Farm Facts | 600 | 2.42 ms |

证据在：

- [qqfarm-opportunity-pprof-20260817/README.md](qqfarm-opportunity-pprof-20260817/README.md)
- [qqfarm-opportunity-only-20260817-r2.json](qqfarm-opportunity-only-20260817-r2.json)

结论：机会弹窗的当前瓶颈是 PlayerSvr 的名册 revision、50 关系 canonical 校验、Header 粗筛、Top-K Facts 精筛、最终关系/Profile/Facts 复验和 CandidateWindow CAS，不是 Farm Header 单 RPC，也不是 Gate CPU。

机会弹窗应独立统计，不能阻塞旧 Join Ready 的延迟口径。完成 batch=64 Join A/B 后，再单独发布最新版 ws-stress，并重新建立 opportunity-only thick-login 容量基线。

## 10. 后续完整压测计划

用户提供的 `91 · 单 Pod 容量、横向扩展与核心业务压测方案` 推荐顺序为：

```text
P0  固定版本 + smoke
P1  Gate：Thick Login、持续连接、Downlink Lane
P2  Player/Farm/Worker 单 Pod 容量向量
P3  1/2/4 Pod 横向扩展效率
P4  Plant/Harvest/Care/Theft 正确性与吞吐
P5  好友列表、Navigator、Hint 与到期洪峰
P6  200 人单热点与多热点
P7  混合负载 + 2h/24h Soak
P8  50%～60% 负载下扩缩容、滚动发布、故障注入
P9  容量模型、Pod 规划、答辩证据
```

当前接手优先级只到 P1 的 batch=64 Join A/B。不要在环境未稳定时直接进入扩缩容、滚动更新或故障注入。

## 11. 已知陷阱

1. `apply-r93.sh` 是旧环境脚本，Redis environment 和 Holder 数都不匹配；不要运行。
2. `30-player-svr.yaml` / `40-farm-svr.yaml` 的 workload UID 是 staged placeholder；不能对存量 STS 原样 apply。
3. `60-farm-worker.yaml` 包含 `REPLACE_AT_APPLY` boot placeholder；不得原样 apply。
4. Controller 停止后的 exact identity 窗口很短；先预热 Worker，再停 Controller，再立即 claim。
5. Pod Ready condition 可能短暂保留旧值；必须轮询 Holder `/readyz` 的 `serving_shard_count` 和 snapshot revision。
6. `kubectl rollout status sts/...` 对 `OnDelete` StatefulSet 不适用；使用 Pod condition 加 `/readyz`。
7. UC 当前节点资源紧张，事件中真实出现过 `Preempted` 和 `Insufficient cpu`。这不是可以忽略的噪声；Pod UID 一变就必须 clean reset。
8. claim JSON 的 `status=\u0001` 是当前工具序列化表现；以 12 tuple、revision 13、epoch 1 对账。
9. Authority 最大 TTL 是 60 秒；不能通过“调大到 5 分钟”绕开 Worker 预热。
10. queue-full 是队列满后的拒绝，不是普通排队；任何增量都意味着该档不是无损容量。
11. 累计 Prometheus counter 不会自动清零；必须使用 before/after 增量。
12. 压测玩家 ID 不能复用，否则旧 Actor、Mail、Task、Receipt、CandidateWindow 会污染冷启动口径。

## 12. 当前未完成事项

- [x] 使用更新后的 Skill 完成一次 2 Worker、claim 前预热的 clean deploy；
- [x] full Join smoke（`dkqwr2u7qpwu`）；
- [x] `--require-ready`；
- [x] 保存 batch=64 环境基线；
- [x] 2500/s 控制档（FAIL：24 断线，P95 222ms）；
- [ ] 3000/s 对比档（因控制档未过而停止）；
- [ ] 条件通过后 3500/s、4000/s；
- [ ] 二分新的无损边界；
- [x] 测后 smoke（`dkqwsbmy61jo`）与 `--require-ready`；
- [x] 更新主容量报告；
- [ ] 将 dirty tree 收敛为可复现提交和镜像 digest。

## 13. 结果表模板

| batch | rate | players | Ready | P95 | P99 | queue-full Δ | errors | restarts | verdict | run ID |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 32 | 2500/s | 10000 | 10000 | 81.80 ms | 102.73 ms | 0 | 0 | 0 | PASS | `dkqffkors2ts` |
| 32 | 3000/s | 10000 | 10000 | 153.51 ms | 192.92 ms | 30 | 0 | 0 | FAIL no-loss | `dkqfhpnnvx35` |
| 32 | 4000/s | 10000 | 10000 | 1790.48 ms | 2223.82 ms | 19587 | 0 | 0 | OVERLOAD | `dkqfg4szdu43` |
| 64 | 2500/s | 10000 | 10000 | 222.35 ms | 252.89 ms | 0 | 24 disconnects | 0 | FAIL | `dkqwrtrupqtd` |
| 64 | 3000/s | 10000 | 未跑 | — | — | — | — | — | SKIP after 2500 FAIL | — |
| 64 | 3500/s | 10000 | 未跑 | — | — | — | — | — | SKIP | — |
| 64 | 4000/s | 10000 | 未跑 | — | — | — | — | — | SKIP | — |

最终报告必须区分：

```text
功能通过
无损容量
性能拐点
过载可达值
生产安全容量
```

不能因为所有玩家最终 Ready，就把存在 queue-full、秒级 P95 或连接丢失的档位写成容量 PASS。
