# QQFarm 3000/s 双 Gate 优化版压测记录

日期：2026-08-19  
环境：`uc-test-plus / developer_026-qqfarm`  
口径：cold-thick-login，跳过好友机会请求，仅测登录、Bootstrap、摘要加载和 Join/Ready。

## 1. 结论

本轮使用优化后的 Runtime、2 Gate、Player/Farm `4 CPU / 16Gi`，完成：

```text
12 Job × 250/s ≈ 3000/s
attempted = 539,994
ready     = 539,994
login_errors = 0
join_errors  = 0
connect_errors = 0
```

每个 Job 的 Ready P95 为 `51.9–55.7ms`，平均 `53.7ms`；Ready P99 为 `62.1–66.5ms`，平均 `64.0ms`。

最终指标没有观察到：

- Gate WebSocket reject
- Gate pending join timeout
- Downlink queue full
- Player/Farm mailbox reject
- Actor activation pending/throttled/load failure
- 压测期间新增 OOM 或业务 Pod 重启

这证明本配置下 3000/s 是一个稳定通过点，但不是单 Gate 极限结论。

随后仅对 Gate 使用的 Redis Cluster client 调整了 pool：go-redis 默认值为 `10 × GOMAXPROCS`，本配置每个 Gate 为 `20`；Gate 专用 URL 改为 `pool_size=64`。Player/Farm 未扩大 pool。

pool=64 后的直接复测表明：

```text
3100/s：557,995 attempted，557,976 Ready（99.9966%），19 connect errors，Gate 未失 ready
3200/s：575,991 attempted，575,991 Ready（100%），login/connect/hello/join errors 全为 0
```

因此当前证据不是“极限等于 3100/s”，而是：**3200/s 已能短时稳定完成，但延迟已经明显进入拐点；3000/s 是舒适稳定点，3200/s 是当前 2 CPU Gate 的可用上沿。**

## 2. 精确版本和镜像

- Backend 优化代码：`36f43b0` (`perf: use shard route lookup for participant proof`)
- 当前分支：`codex/scaleplan-local-kind-qqfarm-r1`
- 当前 reset pin commit：`c259e4f`
- Runtime：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:63a8dad6217017645d30ae15bed18a3d1965092ce9e9896db03dedf27a06d92c
```

- Load image：

```text
mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62
```

- H5：

```text
mirrors.tencent.com/uc-backend/farm-h5@sha256:044e35944ef042d9672086f74e17890ac77f22f0396cfefcc703bc0c57c67092
```

## 3. 固定部署拓扑

```text
Gate       2
Player     2 × 4 CPU / 16Gi
Farm       2 × 4 CPU / 16Gi
Worker     2 × 4 CPU / 8Gi
Shards     1024
Redis      3
Worker     static split，A/B 各 512 shards
```

Mailbox 配置：

```text
PLAYER_MAILBOX_INITIAL_SIZE=16
PLAYER_MAILBOX_MAX_SIZE=64
FARM_MAILBOX_INITIAL_SIZE=16
FARM_MAILBOX_MAX_SIZE=64
idle timeout=2m
idle sweep interval=5s
```

## 4. 唯一部署方法

后端只能通过以下 reset 入口部署；不能裸 `kubectl apply`、手工 Claim Route，也不能并行运行其他部署脚本。

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816

K=(kubectl --context uc-test-plus -n developer_026-qqfarm)
for job in $("${K[@]}" get jobs -l app.kubernetes.io/name=ws-stress \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
  "${K[@]}" delete job "$job" --ignore-not-found --wait=true
 done

QQFARM_GATE_REPLICAS=2 \
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

reset 成功必须看到：

```text
UC_1024_INITIAL_ASSIGNMENT_OK ... shards=1024 holders=2+2 workers=512+512 downlink=ready
```

启动压测前必须确认：

- 两个 Worker `/readyz` 为 `READY`
- `PLANT/HARVEST/CARE/THEFT` 均为 `READY`
- 每个 Worker `farm_opportunity_wakeup_owned_shards=512`
- 两个 Gate Ready
- 四个 Player/Farm Holder Ready
- Farm 到两个 Gate 的 downlink 连通
- 压测期间禁止 reset

## 5. 压测方法

### 5.1 单 Job 只用于 smoke

单 Job 不能作为高 QPS 正式压测。此前单进程高并发会遇到客户端临时端口耗尽；正式压测必须拆成多个 Job。

正确 smoke 必须给 cold 场景配置 cold pool：

```bash
WS_STRESS_IMAGE='mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62' \
WS_STRESS_PRIORITY_CLASS=high-cpu-priority \
WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true \
WS_STRESS_COLD_POOL_SIZE=51 \
WS_STRESS_COLD_LAUNCH_DURATION=10s \
WS_STRESS_WAIT=420s \
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  cold-thick-login 51 10 10s 1006000000
```

Smoke 预期：`51/51 Ready`，Login/Join error 为 0。Smoke 必须使用与正式压测不同的 ID 段。

### 5.2 正式 3000/s

固定使用 12 个并行 Job：

```text
12 × 250/s = 3000/s
每 Job conns=2000
每 Job cold pool=165000
cold launch duration=3m
总 duration=4m
所有 Job priorityClassName=high-cpu-priority
```

Player ID 起点必须不重叠：

```bash
start=$((1007000000 + (i - 1) * 165000))
```

正式启动模板：

```bash
set -u
set -o pipefail
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816

OUT=/tmp/qqfarm-cold-3000-2gate-4cpu16g-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
WS_STRESS_IMAGE='mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62'
PIDS=()

for i in $(seq 1 12); do
  start=$((1007000000 + (i - 1) * 165000))
  (
    WS_STRESS_IMAGE="$WS_STRESS_IMAGE" \
    WS_STRESS_PRIORITY_CLASS=high-cpu-priority \
    WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true \
    WS_STRESS_COLD_POOL_SIZE=165000 \
    WS_STRESS_COLD_LAUNCH_DURATION=3m \
    WS_STRESS_WAIT=900s \
    bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
      cold-thick-login 2000 250 4m0s "$start" \
      > "$OUT/job${i}.log" 2>&1
  ) &
  PIDS+=("$!")
done

rc=0
for pid in "${PIDS[@]}"; do wait "$pid" || rc=1; done
printf 'OUT=%s RC=%s\n' "$OUT" "$rc"
exit "$rc"
```

注意：`duration=4m` 不是 4 分钟持续发射新 ID；本轮 `cold-launch-duration=3m`，所以实际请求量约为：

```text
3000 × 180s = 540,000
```

### 5.3 优先级验证

压测 Job 必须验证：

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm get pods \
  -l app.kubernetes.io/name=ws-stress \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.priorityClassName}{"\n"}{end}'
```

本轮所有压测 Pod 均为：

```text
high-cpu-priority
```

## 6. Actor 数量和登录数据量

正式压测结果：

```text
Player Actor keys：539,994
Farm Actor keys：539,994
逻辑 Actor keys 合计：1,079,988
```

摘要加载计数：

```text
profiles_loaded          = 539,994
task_summaries_loaded     = 539,994
mail_summaries_loaded     = 539,994
compendium_summaries      = 539,994
pet_summaries_loaded      = 539,994
daily_task_rows_loaded    = 1,619,982
task_claimable_total      = 539,994
friend_opportunity_loaded = 0
```

由于本轮 smoke 曾误用正式 Job 1 的同一 ID 起点，严格的全新 cold ID 数量应扣除 smoke 的 51 个 ID，约为 `539,943`。以后 smoke 必须使用独立 ID 段；本记录保留该数据质量限制，不能把本轮宣称为完全无重叠的 cold A/B。

## 7. retained heap 和 mailbox

本轮压测结束后采集的 heap pprof 位于：

```text
/tmp/qqfarm-cold-3000-2gate-4cpu16g-20260819T055105Z/pprof/
```

采集时间约为压测结束后 3 分钟，因此是 drain 后 retained heap，不是峰值内存。

### 7.1 retained heap

`retained heap` 表示采样时仍被 Go 对象引用、尚未释放的堆内存；不是累计分配量，也不等同于 RSS。

### 7.2 mailbox 观察值

pprof 中 `Executor.mailboxFor` 的 retained heap：

```text
Player-0：约 0.90GiB
Player-1：约 0.91GiB
Farm-0：   约 1.28GiB
Farm-1：   约 1.30GiB
合计：     约 4.4GiB
```

该值包含 mailbox 对象、channel、队列和相关运行时分配的归因，不是纯粹的业务 payload。

按本轮约 1,079,988 个 Actor key 粗略平均约 `4KiB / Actor generation`，仅作为观测平均值，不能视为固定单 Actor 大小。

Mailbox 限制是：

```text
User queue initial = 16 items
User queue max     = 64 items
System queue       = 32 items
User queued-byte budget ≈ 64KiB/mailbox
```

### 7.3 主要 retained heap 热点

Player：

- mailbox：约 62–65%
- `cloneAuthorityModelState`：约 13%

Farm：

- mailbox：约 38–40%
- `reflect.unsafe_New`：约 19%
- join/idempotency、Model Decode、FarmEventLane

Gate retained heap 约 10MiB，不是主要内存压力来源。

## 8. `cloneRouteSnapshot` 结论

本轮累计分配中 `cloneRouteSnapshot` 约占：

```text
Player：约 1.8–2.9%
Farm：   约 2.8–3.0%
```

调用链主要是：

```text
AssignmentComposition.Healthy
  -> DynamicAssignmentProvider.CurrentSnapshot/GetSnapshot
  -> cloneRouteSnapshot
```

以及 `runMembershipObserver`、`StaticResolver.Snapshot` 和 Holder health/status 路径。

没有证据表明它是本轮登录压力的主要来源；`snapshotResolver.Resolve` 的 `LookupShard` 快路径没有产生完整快照复制。当前主要压力仍是 Actor mailbox、Actor 激活、Model V2 编解码、Redis `LoadAndClaim/Commit` 和 gRPC/protobuf 分配。

## 9. 和历史 3000/s 结果对比

| 版本 | Player/Farm 资源 | Ready P95 | Ready P99 | 结果 |
|---|---|---:|---:|---|
| 旧 lazy mailbox | 2 CPU / 16Gi | ~1447ms | ~1984ms | 全 Ready，但延迟过高 |
| 交接文档历史版 | 4 CPU / 32Gi | ~68ms | ~129ms | 通过 |
| 本轮优化版 | 4 CPU / 16Gi | **53.7ms** | **64.0ms** | 通过 |

本轮相较历史 4 CPU/32Gi 结果更好，但不是严格单变量 A/B；Runtime、reset 生命周期、ID 数据和压测镜像存在差异。因此可以确认本轮配置性能更好，不能把全部收益单独归因于 `LookupShard`。

## 10. 4000/s 双 Gate 压力边界

本轮没有 reset，直接复用上一轮双 Gate、4 CPU/16Gi 部署。使用 16 个 Job × 250/s，ID 使用正式 3000/s 后的全新区间。所有压测 Pod 均为 `high-cpu-priority`。

结果目录：

```text
/tmp/qqfarm-cold-4000-2gate-4cpu16g-20260819T061145Z
```

16/16 Kubernetes Job 实际完成；外层启动器最后打印 `RC=1` 是本地 shell 在 `wait` 处理 CPU pprof 后重复等待已被回收的子进程造成的 bookkeeping 错误，不是 Job 失败。

```text
attempted = 598,770
ready     = 284,001
ready rate = 47.43%
ready P95 ≈ 11.1s
ready P99 ≈ 16.5s
```

主要失败：

```text
login: context deadline exceeded       178,811
connect: websocket bad handshake       110,149
hello: EOF/network timeout              25,354
```

这不是干净的 4000/s 通过点；它证明当前配置在 3000/s 与 4000/s 之间已经进入 Gate 过载区。

### 10.1 Gate 是当前主要瓶颈

压测中途 `kubectl top pods`：

```text
farm-gate-0  1970m CPU / 1312Mi
farm-gate-1  1907m CPU / 1073Mi
player-0     2421m CPU
player-1     2082m CPU
farm-0       2123m CPU
farm-1       2228m CPU
```

Gate 已达到各自 2 CPU limit；Player/Farm 仍低于 4 CPU limit。Gate CPU pprof 在负载期间采集，热点包括：

- `auth.Service.Login` 累计约 20%
- `RedisAuthority.Reserve` / Redis EvalSha 累计约 8–11%
- `ShardedBootstrapper.EnsureOwnerInitialized` 累计约 7–8%
- WebSocket ServeHTTP、gRPC 写出和 `Syscall6`
- `mallocgc`、GC assist、stack scan 等运行时开销

因此当前不是 `cloneRouteSnapshot` 或 Worker 瓶颈，而是 Gate 登录认证、Redis session reserve、Bootstrap RPC、WebSocket/gRPC 网络写出和 GC 共同把 Gate 推到 2 CPU 上限。

### 10.2 Gate admission 也开始拒绝

中途 metrics：

```text
farm-gate-0:
  farmgate_ws_conn_rejected_total = 22,335
  farmgate_ws_conn_total = 384,840

farm-gate-1:
  farmgate_ws_conn_rejected_total = 0
  farmgate_ws_conn_active = 6,377
  farm_gate_preauth_connections = 3,931
  farm_gate_pending_join_current = 36
  BACKEND_SLOTS_FULL = 1
```

当前配置为：

```text
GATE_PREAUTH_MAX_CONNECTIONS = 8192
GATE_PREAUTH_HELLO_TIMEOUT   = 10s
GATE_BACKEND_MAX_INFLIGHT    = 1024
GATE_WS_RATE_PER_SEC         = 0 (disabled)
```

`farmgate_ws_conn_rejected_total` 没有 reason label，代码会合并 pre-auth capacity、connection limit、rate-limit 等原因；本轮 WS rate limit 是关闭的，Gate-0 的 22,335 次拒绝与 pre-auth/连接水位最吻合。`BACKEND_SLOTS_FULL` 只有 1 次，且 Gate-1 中途 backend in-flight 仅 12，因此不是 1024 backend slot 的主瓶颈。

### 10.3 4000/s 负载生成器也受并发上限影响

当前每 Job `conns=2000`。当 Ready 延迟上升到约 11 秒时，单 Job 的并发连接上限会限制实际发射量，因此本轮实际 attempted 只有 598,770，没有达到理想的 `4000 × 180 = 720,000`。

所以本轮结论是：

```text
3000/s：通过
4000/s：当前 Gate 配置下失败，且 Gate CPU/pre-auth admission 是首要边界
```

不能把本轮的 `47.43% Ready` 当作后端 Holder 极限；它首先触发了 Gate 2 CPU 和 pre-auth 水位。

## 11. 3500/s 双 Gate 继续压测

本轮同样没有 reset，直接复用现有双 Gate、4 CPU/16Gi 配置；清理的只是上一轮已完成的 ws-stress Job。使用 14 Job × 250/s，并采用 4000/s 之后的新 ID 段（offset `28..41`）。

结果目录：

```text
/tmp/qqfarm-cold-3500-2gate-4cpu16g-20260819T062254Z
```

```text
14/14 Job Complete
attempted = 563,600
ready     = 332,173
ready rate = 58.94%
ready P95 ≈ 11.28s
ready P99 ≈ 17.35s
```

主要失败：

```text
login: context deadline exceeded       109,277
connect: websocket bad handshake        87,652
hello: EOF/network timeout              34,009
```

压测中途 CPU：

```text
farm-gate-0  2011m
farm-gate-1  1940m
player-0     2443m
player-1     1975m
farm-0       1817m
farm-1       1974m
```

Gate 两个实例都达到 2 CPU 上限；Player/Farm 仍未达到 4 CPU 上限。Gate CPU pprof 仍然集中在：

- Login / WebSocket ServeHTTP
- Redis EvalSha、Session Reserve
- Bootstrap RPC
- Syscall、gRPC/network write
- `mallocgc` 和 GC scan

中途 Gate admission 还出现：

```text
Gate-0 ws rejected = 65,334
Gate-1 ws rejected = 55,777
RPC_UNAVAILABLE = 11
BACKEND_SLOTS_FULL = 1
```

Player activation load failure 为 `90 + 40`，Farm activation load failure 为 0；这属于 Gate/Redis 过载后的次生错误，不是当前第一瓶颈。

需要注意：本轮每个 Job 仍为 `conns=2000`。当 Ready 延迟达到约 11 秒时，压测器连接上限使实际 attempted 低于理想的 `3500 × 180 = 630,000`。因此这是“配置目标 3500/s 下的过载证据”，不是完全无客户端约束的精确 3500/s 服务容量。

当前可以确定：

```text
3000/s：稳定通过
3500/s：已经严重过载，Gate 是第一瓶颈
4000/s：同样严重过载
```

当前 2 CPU/4Gi Gate 的稳定容量点仍记录为 3000/s；失败边界位于 3000/s 以上、3500/s（实际 offered load 约 3130/s）以内。若要继续缩小边界，需要使用更大的每 Job `conns`（例如 3000）避免压测器先达到并发上限，并继续保持多个 Job、非重叠 ID 和高优先级；这将是新的压力实验，不能与本轮结果混为一谈。

## 12. 3400/s 双 Gate（conns=3000）

本轮仍未 reset，直接复用现有双 Gate、4 CPU/16Gi 配置；使用 17 Job × 200/s，并将每 Job `conns` 从 2000 提高到 3000。使用新的 ID 段（offset `42..58`）。

结果目录：

```text
/tmp/qqfarm-cold-3400-2gate-4cpu16g-conns3000-20260819T063250Z
```

```text
17/17 Job Complete
attempted = 611,695
ready     = 145,344
ready rate = 23.76%
actual attempted rate ≈ 3,398/s
ready P95 ≈ 11.04s
ready P99 ≈ 16.32s
```

这次 `conns=3000` 已基本消除压测器并发上限影响，实际 attempted rate 接近目标 3400/s，因此比前一轮 3500/s（实际 offered 约 3130/s）更适合判断 Gate 边界。

主要失败：

```text
login: context deadline exceeded       276,225
connect: websocket bad handshake       173,416
hello: EOF/network timeout              14,921
```

压测中途 CPU：

```text
farm-gate-0  2010m
farm-gate-1  1716m
player-0     2073m
player-1     1994m
farm-0       1766m
farm-1       1999m
```

Gate-0 已达到 2 CPU；Gate-1 负载不均但仍较高。Gate-0 CPU pprof 主要为：

- `auth.Service.Login` 累计约 27%
- `RedisAuthority.Reserve` / Redis EvalSha 累计约 12%
- `ShardedBootstrapper.EnsureOwnerInitialized` 累计约 10%
- WebSocket/gRPC、Redis 网络写出和 `Syscall6` 约 20%
- `mallocgc`、GC 和调度开销

Gate admission 累计拒绝：

```text
Gate-0 ws rejected = 118,829
Gate-1 ws rejected = 118,574
RPC_UNAVAILABLE = 19
BACKEND_SLOTS_FULL = 1
```

在这个更接近真实 3400/s 的负载下，后端也出现次生压力：

```text
Player activation load failures = 1,354 + 1,493
Player activation throttled      = 565 + 806
Player mailbox rejected          = 1 + 4
Farm downlink queue full         = 1,651 + 1,474
```

但 Farm mailbox queued/rejected、Farm activation pending/load failure 仍为 0；Gate CPU/pre-auth admission 仍是第一瓶颈，Player activation 和 Farm downlink 是后续被带出的压力点。

Gate-1 的 in-load CPU pprof 因 Kubernetes exec 在采集结束时出现 websocket EOF，文件不完整；Gate-0 pprof、全套 metrics 和 `kubectl top` 采集有效。

本轮结论（当时 Gate Redis pool 尚为默认值）：

```text
3000/s：稳定通过
3400/s（实际 offered ≈ 3398/s）：失败
```

这次结果当时支持“稳定容量点为 3000/s”的判断；但后续 Gate pool=64 后的 3100/3200 复测已经改变了该边界，最新结论见第 15、16 节。后续复测仍保持 `conns=3000`，避免压测器并发上限扭曲结果。

## 13. 3200/s 结果无效：Route Control 先失效

本轮使用 16 Job × 200/s、每 Job `conns=3000`，实际 attempted rate 为 `3200.08/s`，但不能作为 Gate 容量结果。

结果目录：

```text
/tmp/qqfarm-cold-3200-2gate-4cpu16g-conns3000-20260819T064537Z
```

表面结果为：

```text
attempted = 576,015
ready     = 318,345
ready rate = 55.27%
ready P95 ≈ 61ms
```

但失败原因不是正常过载：

```text
login 401 ERR_UNAUTHENTICATED = 249,084
```

Gate 日志实际原因是：

```text
actor route: holder lease expired
```

同时 route-controller 日志出现：

```text
scaleplan control adapter: token expired
route-controller independent deadline refresh ... verification failed
RoutePlan runtime recovery ... control mutation gate
placement snapshot drift: authority/snapshot projection mismatch
```

采样 metrics 还显示：

```text
farm_gate_boot_lease_seconds_remaining = 0
GATE_BOOT_LEASE_EXPIRED = 8
```

因此这轮的 55.27% Ready 是 Route Control/Holder lease 失效后的结果，不是 3200/s 的 Gate 性能结果。虽然 `kubectl get pods` 仍显示所有 Pod Running/Ready，但控制面 token 已过期，不能只依据 Pod readiness 继续压测。

本轮 CPU 也不能用于容量判断：Gate 约 1.86–1.88 CPU，但大量登录在 Holder lease 校验处快速失败；Player/Farm 的高 CPU 和 `ERR_STORE_UNAVAILABLE` / subscription stale join 是控制面失效后的次生噪声。

下一轮**干净 cold 基线**容量测试前，必须先通过唯一的 `reset-1024-initial-assignment.sh` 恢复 Route Control token、Holder lease、placement projection，并重新确认 ownership、family readiness、Holder readiness、Gate boot lease 和 downlink 全部正常；否则不应把结果宣称为新的 reset cold 基线。第 15、16 节是用户明确要求“不 reset”的 Gate pool 直接验证，因此单独标注为非 reset 结果。

## 14. 后续固定规则

1. 后端部署只执行 `reset-1024-initial-assignment.sh`。
2. 不在压测中途 reset。
3. 单 Job 只做低量 smoke；正式高 QPS 必须拆成多个 Job。
4. 所有压测 Job 设置并验证 `WS_STRESS_PRIORITY_CLASS=high-cpu-priority`。
5. 每个正式 Job 使用不重叠 Player ID；smoke 也必须使用独立 ID 段。
6. cold 基线使用 `WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true`；好友机会必须另用 returning-player pool 验证。
7. CPU pprof 必须在负载进行中采集；heap/allocs 可在结束后采集。
8. mailbox queued、activation pending、sweeper lag 等指标必须在负载期间周期采样，不能只看结束后的 0 值。
9. 不使用 `git add .`，只单独添加明确的文档或源码文件。

## 15. Gate Redis pool=64 后的 3100/s

本轮在 Gate pool 调整并重新部署后进行；未再次 reset，使用全新的 Player ID 段。正式负载为 15 Job × 200/s + 1 Job × 100/s，每 Job `conns=3000`、cold pool `165000`、cold launch `3m`。结果目录：

```text
/tmp/qqfarm-cold-3100-2gate-4cpu16g-pool64-rerun-20260819T073346Z
```

结果：

```text
16/16 Job Complete
attempted       = 557,995
ready           = 557,976
ready rate      = 99.9966%
login_errors    = 0
hello_errors    = 0
join_errors     = 0
connect_errors  = 19
```

各 Job 的延迟范围：

```text
Login P95 = 45.1–49.0ms
Ready P95 = 79.9–164.5ms
```

19 次失败均为 connect 阶段的 `context deadline exceeded` / `network timeout`，没有出现 Login 认证错误、Redis dependency error、Gate readiness 丢失或 Gate Pod 重启。负载期间采集了 Gate/Player/Farm CPU pprof；压测结束后两 Gate 均保持 Ready、Restart Count 为 0。

这说明 pool=64 后 3100/s 不再复现旧 pool 下的 Redis connection pool timeout，且仍有明显余量；但这不是完全无错误的 3100/s 结果，因此应记录为“稳定通过、存在极少量连接超时”。

## 16. Gate Redis pool=64 后的 3200/s

本轮按要求未 reset，使用 16 Job × 200/s、每 Job `conns=3000`、不重叠的新 Player ID 段。结果目录：

```text
/tmp/qqfarm-cold-3200-2gate-4cpu16g-pool64-direct-20260819T074335Z
```

结果：

```text
16/16 Job Complete
attempted       = 575,991
ready           = 575,991
ready rate      = 100%
login_errors    = 0
connect_errors  = 0
hello_errors    = 0
join_errors     = 0
```

Watchdog 共完成 56 轮 Gate readiness 检查，全部为：

```json
{"status":"ready"}
```

中途 metrics：

```text
farm_redis_dependency_error_percent{domain="authority"}  = 0
farm_redis_dependency_error_percent{domain="runtime"}    = 0
farm_redis_dependency_error_percent{domain="projection"} = 0
```

但延迟相较 3100/s 出现明显非线性上升：

```text
Login P95 = 318.3–327.9ms
Ready P95 = 409.4–421.0ms
```

Gate CPU pprof 在负载期间采集，两个 Gate 的 `Syscall6` 均约占 20% flat samples，并伴随网络写出、分配和 GC 运行时开销。没有观察到旧 3200/s 测试中的 readiness 503、Redis pool timeout、OOM 或 Pod 重启。

需要区分两类结论：

```text
可用性容量：3200/s 短时通过
延迟容量：3200/s 已接近 Gate 2 CPU 的边界，不是舒适容量
```

旧的 3200/s 失败发生在 pool=64 调整之前，Gate readiness 503 与 `redis: connection pool timeout` 是当时的主要现象；因此不能用旧结果否定本轮 3200/s。当前更准确的容量判断为：

```text
3000/s：舒适、低延迟稳定点
3100/s：稳定通过，存在极少量连接超时
3200/s：短时全量 Ready，但 Login/Ready 延迟显著升高，是当前可用上沿
>3200/s：尚未在 pool=64 配置下验证，不应提前宣称稳定容量
```

本轮未 reset，因此 3200/s 结果是 Gate pool 调整后的直接验证，不是新的干净 reset cold 基线；Player ID 使用了独立区间，未与前轮正式负载重叠。

## 17. 3100/3200 CPU pprof 瓶颈

两轮均在负载期间采集 15 秒 CPU pprof，分析命令为：

```bash
go tool pprof -top -cum <OUT>/pprof/cpu-gate-0.pb.gz
go tool pprof -top -cum <OUT>/pprof/cpu-gate-1.pb.gz
```

对应目录：

```text
/tmp/qqfarm-cold-3100-2gate-4cpu16g-pool64-rerun-20260819T073346Z/pprof/
/tmp/qqfarm-cold-3200-2gate-4cpu16g-pool64-direct-20260819T074335Z/pprof/
```

### 17.1 Gate：第一瓶颈是网络/协议处理叠加登录固定成本

3100/s 和 3200/s 的 Gate pprof 形状基本一致，没有出现一个新的 Redis 函数突然独占 CPU。两轮主要累计调用链范围如下（累计百分比存在调用链重叠，不能直接相加）：

```text
net/http.(*conn).serve                         ≈ 37–38%
WebSocket Server.ServeHTTP                     ≈ 18–20%
Gate.HandleBinary                              ≈ 15–17%
syscall / net.Conn.Write / FD.Write           ≈ 11–15%
PooledClientConn.Invoke / gRPC downstream      ≈ 12–14%
auth.Handler.ServeHTTP                         ≈ 11–13%
auth.Service.Login                             ≈ 8–9%
Redis Script.EvalSha / ClusterClient.Process   ≈ 8–9%
runtime.mallocgc cumulative path              ≈ 12–14%
```

Flat samples 中 `internal/runtime/syscall/linux.Syscall6` 约占 `20–22%`；3200/s 时 `mallocgc`、protobuf eager unmarshal、gRPC loopy writer 和 WebSocket write pump 仍然明显存在。这说明 Gate 的 CPU 主要消耗在：

1. HTTP/WebSocket 连接和 FarmFrame 处理；
2. 向 Player/Farm 的 gRPC 调用及返回写出；
3. 每次 cold login 的认证、Redis Reserve/EvalSha 和 Bootstrap 固定成本；
4. 请求/响应 protobuf、JSON、WebSocket buffer 的分配与 GC。

pool=64 后 Redis pool wait 不再是主要可见故障，3200/s 中 Redis dependency error 仍为 0；Redis client 的 EvalSha/Process 仍约占 Gate 累计 CPU 的 8–9%，但它是登录固定成本的一部分，不是单独的 Redis server saturation 证据。

### 17.2 Player：Actor Executor、Authority LoadAndClaim 和 Redis

Player 两轮 pprof 的主链保持稳定：

```text
Executor.run / execJob                       ≈ 40–43% cumulative
submitAuthorityVersioned                     ≈ 23–25%
ensureAuthority                              ≈ 17–19%
AuthorityStore.LoadAndClaim                  ≈ 16–16.5%
Redis ClusterClient.Process                  ≈ 13–14%
syscall                                     ≈ 14–16% flat
```

这些比例说明 Player 主要在处理登录触发的 Player Actor authority 绑定、Redis `LoadAndClaim`、gRPC 服务流和状态提交；不是 Route Snapshot clone 或 Worker Claim 在消耗主要 CPU。Player 是 Gate 登录链路的下游成本，但在本轮没有观察到独立的 Redis pool failure。

### 17.3 Farm：Actor authority/load-and-claim 和 downlink gRPC

Farm 两轮 pprof 的主链为：

```text
Executor.run / execJob                       ≈ 24–26% cumulative
Farm Registry.Submit                         ≈ 24–26%
actorAuthority / loadAndClaim                ≈ 21–23%
gRPC server / processUnaryRPC                ≈ 15%
gRPC loopyWriter / network write             ≈ 11–13%
syscall                                     ≈ 14–16% flat
```

Farm 主要处理 Farm Actor authority/load-and-claim、Holder gRPC 和 downlink 写出；本轮没有出现 Farm mailbox 或 activation failure 作为第一瓶颈。它会放大 Gate 的下游固定成本，但从 pprof 形状看，当前首要优化面仍然是 Gate 登录入口和 Gate 网络/协议路径。

### 17.4 pprof 对容量结论的解释

3100/s 到 3200/s 时，Gate pprof 的热点比例没有根本改变，但 Login P95 从约 `45–49ms` 升到 `318–328ms`，Ready P95 从约 `80ms` 升到 `409–421ms`。因此 3200/s 的非线性主要是同一组 Gate 固定工作在 2 CPU 附近进入排队，而不是后端突然出现新的 Actor、Route 或 Redis 数据瓶颈。

后续优化优先级应为：

1. 降低 Gate 每次 Login 的固定调用数和序列化/反序列化成本；
2. 将 Login 的 Reserve、Bootstrap route resolve、EnsureOwnerInitialized、Ticket Issue、JSON response、WebSocket Activate 分阶段打 histogram；
3. 继续观测 Redis pool `PoolSize/InUse/Idle/WaitCount/Timeouts`，确认 pool 扩大后的真实利用率和等待量；
4. 再评估 Gate admission、Gate CPU/副本扩容；
5. 不把 `LookupShard`、Worker Claim 或 mailbox 优化误判为本轮第一瓶颈。

## 18. 3000/3100/3200 延迟横向对比

以下数据来自每个 ws-stress Job 结束时输出的 JSON。`avg (min–max)` 表示各 Job 百分位的算术平均，以及 Job 之间的最小/最大值；不是把所有请求原始样本合并后重新计算的全局百分位。压测工具只输出 Login/Hello 的 P50/P95，不输出它们的 P99；Ready 同时输出 P50/P95/P99/max。

| 目标速率 | Job / 实际速率 | attempted / ready | Ready rate | Login P50 / P95 | Hello P50 / P95 |
|---:|---:|---:|---:|---:|---:|
| 3000/s | 12 / 2999.97/s | 539,994 / 539,994 | 100% | 15.9 / 24.5ms | 7.4 / 12.6ms |
| 3100/s | 16 / 3099.97/s | 557,995 / 557,976 | 99.9966% | 15.7 / 47.2ms | 7.2 / 12.6ms |
| 3200/s | 16 / 3199.95/s | 575,991 / 575,991 | 100% | 24.8 / 322.7ms | 8.7 / 18.8ms |

Ready 延迟：

| 目标速率 | Ready P50 | Ready P95 | Ready P99 | Ready max |
|---:|---:|---:|---:|---:|
| 3000/s | 37.8ms (35.4–39.9) | **53.7ms** (51.9–55.7) | **64.0ms** (62.1–66.5) | 139.4ms (120.2–150.9) |
| 3100/s | 36.1ms (35.2–37.5) | 87.1ms (79.9–164.5) | 490.3ms (282.0–3462.6) | 846.6ms (531.7–5242.4) |
| 3200/s | 56.4ms (55.6–57.8) | **415.6ms** (409.4–421.0) | **654.3ms** (644.2–676.5) | 939.2ms (912.5–962.7) |

### 18.1 延迟变化

- **3000/s**：P95/P99 分别约 `53.7/64.0ms`，尾延迟很短，是当前舒适稳定点。
- **3100/s**：Login P95 仍只有约 `47ms`，但 Ready P99 平均升到 `490ms`，最大 Job 尾延迟达到 `3.46s`；同时出现 19 次 connect timeout，说明已经开始进入排队拐点。
- **3200/s**：Login P95 约 `323ms`，Ready P95/P99 约 `416/654ms`，但本轮仍 100% Ready、无业务错误；说明 pool=64 消除了旧的 Redis pool failure，但没有消除 Gate CPU/网络路径的排队成本。

因此不能只看 Ready rate：3200/s 的可用性仍为 100%，但 P95/P99 已经明显高于 3000/s；若业务 SLA 关注亚百毫秒 Ready，3000/s 更适合作为容量承诺值，3200/s 应记录为短时可用上沿而非舒适容量。3000/s 是 pool 调整前的优化基线，3100/3200 是 pool=64 后的直接复测，不能视为严格单变量延迟 A/B。
