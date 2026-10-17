# QQFarm UC Test Plus 1024-Shard 压测交接文档

> 交接对象：后续需要合并本分支改动、继续验证 QQFarm 冷登录容量的开发者
>
> 最后更新：2026-08-18
>
> 本文只描述 `uc-test-plus/developer_026-qqfarm`，不适用于其他 Namespace、其他 Redis tuple 或其他部署脚本。

---

## 1. 当前结论先看

### 1.1 已完成

1. 完成 1024 shard 的双 Player、双 Farm 初始分配链路。
2. 完成稳定多 Gate 身份：StatefulSet ordinal、headless DNS、Redis route identity。
3. 完成冷登录、好友入口、Actor/Redis、WebSocket 和 Gate admission 相关优化。
4. 完成 lazy mailbox：
   - 首次创建只分配 16 个 User mailbox channel 槽位；
   - 逻辑最大队列为 64；
   - 超过初始 channel 容量后使用 overflow slice；
   - System mailbox 与 User mailbox 隔离。
5. idle sweeper 调整为 2 分钟 idle timeout、5 秒 bucket/sweep、每轮最多回收 16384 个 Actor。
6. 已构建并发布包含 lazy mailbox 的 runtime 镜像：

   ```text
   mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:ad872edef5806a9a49a26d0e88a2266118c1752fb31fd1285e2cea41c39fa0f6
   ```

7. 已验证 `2 CPU / 16Gi + mailbox initial=16/max=64` 在 3000/s 下不 OOM，所有请求 Ready，但延迟较高。
8. 当前源码和 reset 脚本已经改为目标配置：

   ```text
   Player/Farm: 4 CPU / 16Gi
   mailbox initial: 16
   mailbox max: 64
   ```

### 1.2 尚未完成

`4 CPU / 16Gi + mailbox 16/64` 的最终 3000/s A/B 尚未在本轮重新部署后验证。

注意：最近一次真实集群压测是在修改资源配置之前完成的，集群最后已知实际运行配置仍是 `2 CPU / 16Gi`。源码、YAML 和 reset 脚本现在是 `4 CPU / 16Gi`，需要再次执行唯一 reset 脚本后才会真正部署到集群。

---

## 2. Git、工作区和分支

仓库：

```text
/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
```

远端：

```text
developer_001@example.invalid:developer_026/farm-game.git
```

当前分支：

```text
codex/scaleplan-local-kind-qqfarm-r1
```

已推送 commit：

```text
f46e4f5 perf: reduce qqfarm actor mailbox footprint
```

合并到其他分支：

```bash
git fetch origin
git checkout <目标分支>
git merge --no-ff origin/codex/scaleplan-local-kind-qqfarm-r1
```

也可以只 cherry-pick 当前提交：

```bash
git cherry-pick f46e4f5
```

本分支工作区还留有若干未跟踪的压测产物、预编译二进制和历史实验目录。它们没有提交，不要使用 `git add .` 把这些大文件加入提交。交接文档本身在本文路径；如果需要把本文也放入远端，单独执行：

```bash
git add docs/verification/perf/qqfarm-1024-mailbox-handoff-20260818.md
git commit -m "docs: add qqfarm 1024 mailbox handoff"
git push origin codex/scaleplan-local-kind-qqfarm-r1
```

---

## 3. 绝对不能违反的部署约束

### 3.1 唯一允许的部署入口

只能使用：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

执行示例：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
QQFARM_GATE_REPLICAS=2 bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

### 3.2 禁止事项

禁止：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/apply-r93.sh
任何 raw manifest apply
压测稳态期间 reset
直接 patch 已经承载 shard 的 Holder Pod
```

reset 脚本会执行干净重置、清理专用三 Redis master、重建 route bootstrap、重新完成 1024 shard 初始分配，并在所有 Holder 启动前写入资源、镜像、mailbox、pprof 和 Gate admission 配置。

这是破坏性操作，只允许在专用的 `developer_026-qqfarm` 测试 Namespace 执行。执行前确认没有其他人正在使用该 Namespace。

### 3.3 固定测试拓扑

```text
Kubernetes context: uc-test-plus
Namespace:          developer_026-qqfarm
Player replicas:    2
Farm replicas:      2
Farm Worker:        2 Deployments
Gate:               2 StatefulSet replicas for the validated multi-Gate run
Redis:              farm-redis-0/1/2
Shard count:        1024
Player:             512 + 512
Farm:               512 + 512
```

reset 默认 `QQFARM_GATE_REPLICAS=1`。本次容量结果和后续正式 A/B 应使用：

```bash
QQFARM_GATE_REPLICAS=2 bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

---

## 4. 当前镜像和关键配置

### 4.1 Runtime 镜像

Player、Farm、Farm Worker、Farm Gate、Route Controller 使用同一个 runtime digest：

```text
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:ad872edef5806a9a49a26d0e88a2266118c1752fb31fd1285e2cea41c39fa0f6
```

reset 脚本会使用 `kubectl set image` 重新固定所有 workload 的 runtime image，因此不要依赖集群中已有的 tag。

### 4.2 ws-stress 镜像

本次冷登录压测必须显式指定支持完整参数的镜像：

```text
mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62
```

旧镜像不支持 `-skip-friend-opportunity`，不能使用 `run-ws-stress.sh` 的历史默认 digest。执行时总是设置：

```bash
WS_STRESS_IMAGE='mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62'
```

### 4.3 mailbox 和 sweeper

reset 会设置：

```text
PLAYER_MAILBOX_INITIAL_SIZE=16
PLAYER_MAILBOX_MAX_SIZE=64
FARM_MAILBOX_INITIAL_SIZE=16
FARM_MAILBOX_MAX_SIZE=64
PLAYER_MAILBOX_IDLE_TIMEOUT=2m
FARM_ACTOR_IDLE_TIMEOUT=2m
PLAYER_MAILBOX_IDLE_BUCKET_WIDTH=5s
FARM_ACTOR_IDLE_BUCKET_WIDTH=5s
PLAYER_MAILBOX_IDLE_SWEEP_INTERVAL=5s
FARM_ACTOR_IDLE_SWEEP_INTERVAL=5s
PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP=16384
FARM_ACTOR_IDLE_MAX_PER_SWEEP=16384
```

`20-config.yaml` 也固定了这些值，但 UC 的实际 reset 以 `kubectl set env` 和 ConfigMap patch 为准。

### 4.4 Gate admission

不要为了追求 QPS 随意提高以下值：

```text
GATE_BACKEND_MAX_INFLIGHT=1024
GATE_BACKEND_ADMISSION_WAIT=25ms
GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT=256
```

历史验证表明提高 Gate backend admission 可能造成 Downlink cascade 和 `farm_downlink_queue_full_total`。

---

## 5. 部署步骤

### 5.1 部署前检查

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816

git checkout codex/scaleplan-local-kind-qqfarm-r1
git status --short
bash -n deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

确认 reset 脚本中的资源为：

```bash
rg -n 'requests=cpu|limits=cpu' \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
```

预期：Player 和 Farm 都是 `cpu=4,memory=16Gi`。

确认集群上下文：

```bash
kubectl config current-context
kubectl --context uc-test-plus -n developer_026-qqfarm get pods
```

### 5.2 清理旧压测 Job

reset 不负责删除已经完成的 ws-stress Job。开始新一轮前清理旧 Job，避免混淆统计：

```bash
K=(kubectl --context uc-test-plus -n developer_026-qqfarm)
for job in $("${K[@]}" get jobs \
  -l app.kubernetes.io/name=ws-stress \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\\n"}{end}'); do
  "${K[@]}" delete job "$job" --ignore-not-found --wait=true
done
```

### 5.3 执行唯一 reset

```bash
QQFARM_GATE_REPLICAS=2 \
  bash deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh \
  2>&1 | tee /tmp/qqfarm-reset-4cpu16g-$(date -u +%Y%m%dT%H%M%SZ).log
```

脚本完成时应输出：

```text
UC_1024_INITIAL_ASSIGNMENT_OK challenge=... shards=1024 holders=2+2 downlink=ready
```

reset 的主要阶段：

1. 停止 Gate、Worker、Route Controller、Player、Farm。
2. 只清理专用三 Redis master。
3. 设置 StatefulSet、headless DNS、stable gate identity。
4. 写入 runtime image、资源、mailbox、sweeper 和 pprof 配置。
5. 创建临时 `farm-route-bootstrap`，从当前 worktree 编译 `cmd/rt2q-route-bootstrap`。
6. 先启动 legacy Controller 释放 Holder init barrier。
7. 切换到 `k8s-canary` discovery 和 Controller-owned sealed initial assignment。
8. 完成 1024 shard 的 512+512 分配。
9. 最后启动两个 Worker 和两个 Gate，并检查 Farm→Gate `:9092` downlink。

### 5.4 部署后检查

资源和镜像：

```bash
K=(kubectl --context uc-test-plus -n developer_026-qqfarm)
"${K[@]}" get sts farm-svr player-svr \
  -o custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image,CPU_REQ:.spec.template.spec.containers[0].resources.requests.cpu,CPU_LIMIT:.spec.template.spec.containers[0].resources.limits.cpu,MEM_REQ:.spec.template.spec.containers[0].resources.requests.memory,MEM_LIMIT:.spec.template.spec.containers[0].resources.limits.memory
```

预期：

```text
farm-svr   ...  4  4  16Gi  16Gi
player-svr ...  4  4  16Gi  16Gi
```

Pod 和重启次数：

```bash
"${K[@]}" get pods -o wide
"${K[@]}" get pods \
  -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount,PHASE:.status.phase
```

四个 Holder 的 mailbox 环境：

```bash
for p in farm-svr-0 farm-svr-1 player-svr-0 player-svr-1; do
  c=farm-svr; case "$p" in player-*) c=player-svr;; esac
  echo "=== $p"
  "${K[@]}" exec "$p" -c "$c" -- env | \
    grep -E 'MAILBOX_(INITIAL|MAX)_SIZE|IDLE_MAX_PER_SWEEP' | sort
 done
```

四个 Holder `/readyz`：

```bash
for p in farm-svr-0 farm-svr-1 player-svr-0 player-svr-1; do
  c=farm-svr; port=9091
  case "$p" in player-*) c=player-svr; port=9101;; esac
  echo "=== $p"
  "${K[@]}" exec "$p" -c "$c" -- \
    wget -q -T 10 -O- "http://127.0.0.1:${port}/readyz"
 done
```

每个 Holder 必须满足：

```text
status=ready
assignment_healthy=true
fatal_error_present=false
serving_shard_count=512
installed_memberships=512
pending_actions=0
degraded_shards=0
```

Gate 和 downlink：

```bash
"${K[@]}" get sts farm-gate
"${K[@]}" get pods -l app.kubernetes.io/name=farm-gate
for farm in farm-svr-0 farm-svr-1; do
  for gate in 0 1; do
    "${K[@]}" exec "$farm" -c farm-svr -- \
      nc -z -w 2 "farm-gate-${gate}.farm-gate-headless.developer_026-qqfarm.svc.cluster.local" 9092
  done
done
```

只有在四个 Holder readiness、Gate readiness 和两个 Farm 到两个 Gate 的 downlink 都通过后，才可以启动压测。

---

## 6. 如何写和启动 3000/s 压测脚本

### 6.1 单个 Job 的脚本接口

入口：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh
```

调用格式：

```text
run-ws-stress.sh [scenario] [conns] [rate] [duration] [player-start]
```

例如：

```bash
WS_STRESS_IMAGE="$WS_STRESS_IMAGE" \
WS_STRESS_PRIORITY_CLASS=high-cpu-priority \
WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true \
WS_STRESS_COLD_POOL_SIZE=165000 \
WS_STRESS_COLD_LAUNCH_DURATION=3m \
WS_STRESS_WAIT=420s \
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  cold-thick-login 2000 250 4m0s 1007000000
```

含义：

```text
scenario: cold-thick-login
conns:    2000       # Job 内最大连接并发，不是总 QPS
rate:     250/s
持续:     4m
player-start: 唯一 ID 起点
```

### 6.2 为什么使用 12 个 Job

本轮固定使用：

```text
12 个 Job × 250/s = 3000/s
每个 cold pool = 165000
每个 Job 使用不重叠的 Player ID 区间
```

Player 起点分配：

```text
Job  1: 1007000000 + 0×165000
Job  2: 1007000000 + 1×165000
...
Job 12: 1007000000 + 11×165000
```

不要让多个 Gate 或多个 Job 共享相同的冷 ID。多 Gate 只使用稳定 Gate identity，冷登录使用唯一 Player ID。

### 6.3 正式 3000/s 启动脚本

下面脚本是推荐的 12 Job 启动方式。启动前确认 backend 已完成 readiness；启动后不要 reset。

```bash
#!/usr/bin/env bash
set -u
set -o pipefail

cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816

OUT=/tmp/qqfarm-cold-3000-2gate-4cpu16g-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
printf 'load_started=%s\\n' "$(date -u +%FT%TZ)" > "$OUT/meta.txt"

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
    WS_STRESS_WAIT=420s \
    bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
      cold-thick-login 2000 250 4m0s "$start" \
      > "$OUT/job${i}.log" 2>&1
  ) &
  PIDS+=("$!")
done

printf 'jobs_started=%s\\n' "$(date -u +%FT%TZ)" >> "$OUT/meta.txt"
rc=0
for pid in "${PIDS[@]}"; do
  wait "$pid" || rc=1
done
printf 'jobs_finished=%s rc=%s\\n' "$(date -u +%FT%TZ)" "$rc" >> "$OUT/meta.txt"
exit "$rc"
```

`WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true` 只用于本轮冷登录/Actor mailbox 基线，目的是跳过冷 ID 没有好友关系和可行动农场时必然失败的 Opportunity。生产默认不要关闭好友弹窗；需要验证真实 Opportunity 时必须使用已经 seed 好好友关系和农场数据的 returning-player pool。

### 6.4 结果汇总

```bash
OUT=/tmp/qqfarm-cold-3000-2gate-4cpu16g-<timestamp>
python3 - "$OUT" <<'PY'
import glob
import json
import sys

rows = []
for path in glob.glob(sys.argv[1] + "/job*.log"):
    text = open(path, encoding="utf-8").read()
    start = text.find("{\n")
    if start < 0:
        continue
    try:
        rows.append(json.loads(text[start:]))
    except Exception:
        pass

print("jobs", len(rows))
print("attempted", sum(x["attempted"] for x in rows))
print("ready", sum(x["ready"] for x in rows))
print("login_errors", sum(x["login_errors"] for x in rows))
print("join_errors", sum(x["join_errors"] for x in rows))
for key in ("qps", "ready_p95_ms", "ready_p99_ms", "ready_max_ms"):
    values = [x[key] for x in rows]
    print(key, "avg", sum(values) / len(values), "min", min(values), "max", max(values))
PY
```

通过标准：

```text
12/12 Job 成功
attempted == ready
login_errors == 0
join_errors == 0
Farm/Player 无 OOMKilled
Gate 无重启
mailbox_rejected_total 不增长
```

延迟要单独记录，不要只看 Job 成功。3000/s 在 2 CPU 下虽然全部 Ready，但 P95 约 1.45s；4 CPU/32Gi 的历史结果约 P95 68ms、P99 129ms。下一轮 4 CPU/16Gi 的目的就是确认低延迟能否在不增加内存的情况下恢复。

---

## 7. 压测期间如何采集 metrics、CPU、heap 和 allocs

### 7.1 不要在压测结束后才采 CPU profile

上一次 2 CPU/16Gi 压测中，pprof 是 Job 完成后才采的，因此 CPU profile 主要反映 GC/scavenger 和 idle 状态，不能作为稳态 CPU 热点结论。

正确做法：

1. reset 完成并确认 readiness；
2. 启动 12 个 Job；
3. Job 运行 30–90 秒后并行采集 CPU profile；
4. 压测结束后再采 heap、allocs 和最终 metrics。

### 7.2 端口

```text
farm-gate:  8080
player-svr: 9101
farm-svr:   9091
```

pprof：

```text
http://127.0.0.1:8080/debug/pprof/
http://127.0.0.1:9101/debug/pprof/
http://127.0.0.1:9091/debug/pprof/
```

### 7.3 正确保存 pprof 到本地

`kubectl exec` 中不能把文件写到本地；要使用 `-O-`，再由本地 shell 重定向：

```bash
OUT=/tmp/qqfarm-cold-3000-2gate-4cpu16g-<timestamp>
mkdir -p "$OUT/pprof"
K=(kubectl --context uc-test-plus -n developer_026-qqfarm)

# CPU：必须在压测进行中执行
for spec in \
  'gate-0 farm-gate-0 farm-gate 8080' \
  'gate-1 farm-gate-1 farm-gate 8080' \
  'player-0 player-svr-0 player-svr 9101' \
  'player-1 player-svr-1 player-svr 9101' \
  'farm-0 farm-svr-0 farm-svr 9091' \
  'farm-1 farm-svr-1 farm-svr 9091'; do
  read -r label pod container port <<< "$spec"
  "${K[@]}" exec "$pod" -c "$container" -- \
    wget -q -T 45 -O- \
    "http://127.0.0.1:${port}/debug/pprof/profile?seconds=30" \
    > "$OUT/pprof/cpu-${label}.pb.gz" &
done
wait

# heap/allocs：压测结束后也可以采，分别看 retained heap 和累计分配
for spec in \
  'gate-0 farm-gate-0 farm-gate 8080' \
  'gate-1 farm-gate-1 farm-gate 8080' \
  'player-0 player-svr-0 player-svr 9101' \
  'player-1 player-svr-1 player-svr 9101' \
  'farm-0 farm-svr-0 farm-svr 9091' \
  'farm-1 farm-svr-1 farm-svr 9091'; do
  read -r label pod container port <<< "$spec"
  "${K[@]}" exec "$pod" -c "$container" -- \
    wget -q -T 90 -O- "http://127.0.0.1:${port}/debug/pprof/heap" \
    > "$OUT/pprof/heap-${label}.pb.gz" &
  "${K[@]}" exec "$pod" -c "$container" -- \
    wget -q -T 90 -O- "http://127.0.0.1:${port}/debug/pprof/allocs" \
    > "$OUT/pprof/allocs-${label}.pb.gz" &
done
wait
```

分析：

```bash
go tool pprof -top -nodecount=40 \
  "$OUT/pprof/heap-farm-0.pb.gz"

go tool pprof -top -nodecount=40 -sample_index=alloc_space \
  "$OUT/pprof/allocs-farm-0.pb.gz"

go tool pprof -top -nodecount=30 \
  "$OUT/pprof/cpu-farm-0.pb.gz"
```

优先关注：

```text
internal/logic/actor/runtime.(*Executor).mailboxFor
internal/farm/actor.(*Registry).loadAndClaimActor
internal/farm/actormodelv2.Decode
internal/farm/actor.(*FarmActor).rememberJoinIdempotency
internal/farm/actor.(*FarmActor).joinWithMode
internal/farm/subscription.newFarmEventLane
internal/storage/redis/authority.(*Store).LoadAndClaim
```

### 7.4 metrics 采集

```bash
mkdir -p "$OUT/metrics"
for spec in \
  'farm-0 farm-svr-0 farm-svr 9091' \
  'farm-1 farm-svr-1 farm-svr 9091' \
  'player-0 player-svr-0 player-svr 9101' \
  'player-1 player-svr-1 player-svr 9101' \
  'gate-0 farm-gate-0 farm-gate 8080' \
  'gate-1 farm-gate-1 farm-gate 8080'; do
  read -r label pod container port <<< "$spec"
  "${K[@]}" exec "$pod" -c "$container" -- \
    wget -q -T 15 -O- "http://127.0.0.1:${port}/metrics" \
    > "$OUT/metrics/${label}.txt"
done
```

重点记录：

```text
process_resident_memory_bytes
go_memstats_heap_inuse_bytes
go_memstats_alloc_bytes_total
farm_actor_active_keys
farm_actor_activation_tracked_keys
farm_actor_activation_concurrent_loads
farm_actor_activation_pending_loads
farm_actor_activation_throttled_total
farm_actor_activation_load_fail_total
farm_actor_mailbox_queued
farm_actor_mailbox_rejected_total
farm_actor_mailbox_retired_total
farm_actor_idle_sweeper_lag_seconds
farm_actor_idle_sweeper_retired_total
farm_actor_turn_latency_seconds
farm_downlink_queue_items
farm_downlink_queue_capacity
farm_gate_command_backend_in_flight
farm_gate_command_backend_rejected_total
```

注意：压测结束后 `mailbox_queued` 和 activation pending 可能已经归零，因此要在压测过程中周期性保存 metrics，不能只看最后一次。

同时采集：

```bash
"${K[@]}" top pods
for p in farm-svr-0 farm-svr-1 player-svr-0 player-svr-1; do
  c=farm-svr; case "$p" in player-*) c=player-svr;; esac
  echo "=== $p"
  "${K[@]}" exec "$p" -c "$c" -- cat /sys/fs/cgroup/cpu.stat
  "${K[@]}" exec "$p" -c "$c" -- cat /sys/fs/cgroup/memory.events
 done
```

---

## 8. 已完成压测结果和解释

### 8.1 4 CPU / 32Gi 历史成功结果

```text
12 个 Job
总请求：539863
Ready：539863
实际 QPS：约 2998.6/s
平均 P95：约 68ms
平均 P99：约 129ms
Login errors：0
Join errors：0
Farm：无 OOM
```

该结果证明 3000/s 在 4 CPU/32Gi 下可以通过，但它和后续版本不是严格单变量 A/B，因为 mailbox、镜像、资源和 reset 生命周期存在差异。

### 8.2 2 CPU / 16Gi + lazy mailbox 结果

结果目录：

```text
/tmp/qqfarm-cold-3000-2gate-mailbox16-max64-2cpu16g-20260818
```

```text
12/12 Job 成功
attempted=539970
ready=539970
login_errors=0
join_errors=0
平均单 Job QPS：约 249.53/s
平均 P95：约 1447ms
平均 P99：约 1984ms
```

运行结束时：

```text
Farm RSS：约 9.3–10.3Gi
Player RSS：约 4.7–5.0Gi
Farm mailbox rejected：0
Player mailbox rejected：0
Farm sweeper lag：0
Farm activation pending：0
Farm OOMKilled：无
```

Actor key 和 retirement 示例：

```text
Farm active keys：约 185k / 199k
Farm retired mailbox：约 84k / 71k
Player active keys：约 22k / 22k
Player retired mailbox：约 247k / 248k
```

### 8.3 延迟升高的判断

本轮没有出现业务失败，问题是后端处理和排队能力：

1. 2 CPU 下 Player/Farm 每个 Actor 的串行 turn 处理能力下降。
2. cold login 会造成大量 Player/Farm Actor activation、`LoadAndClaim`、模型 Decode 和 Redis authority 操作。
3. mailbox `max=64` 会保留有限的排队能力，避免立即 `ErrMailboxFull`，但在 2 CPU 处理不过来时表现为请求等待。
4. 4 CPU/32Gi 版本的低延迟主要来自额外 CPU 处理能力，不是 Gate 单独变快。
5. mailbox lazy 优化降低了 retained heap，但不等于增加 CPU 或 Redis 吞吐。

因此下一轮应使用 `4 CPU / 16Gi` 做单变量验证：如果 P95 回到几十毫秒，说明主要瓶颈是 CPU；如果仍然很高，再重点分析 `LoadAndClaim`、Redis pool wait/RTT、Actor activation 和模型 Decode。

---

## 9. 代码改动摘要

### 9.1 Actor runtime mailbox

主要文件：

```text
internal/logic/actor/runtime/executor.go
internal/logic/actor/runtime/executor_test.go
```

核心改动：

```text
Options.UserMailboxInitialSize
mailbox.userOverflow []job
```

逻辑：

- `UserMailboxSize` 仍是逻辑最大队列容量；
- `UserMailboxInitialSize` 控制首次 channel 预分配容量；
- 初始 channel 满后进入 `userOverflow`；
- 超过逻辑最大容量返回 `ErrMailboxFull`；
- `MailboxSnapshot`、`Stats`、`RetireIfIdle`、`dropQueued` 同时统计 channel 和 overflow；
- SystemQueue 仍使用独立 mailbox，不受 User mailbox 缩小影响。

### 9.2 Farm/Player Registry

主要文件：

```text
internal/farm/actor/registry.go
internal/player/actor/registry.go
cmd/farm-svr/main.go
cmd/player-svr/main.go
```

生产环境通过以下变量控制：

```text
FARM_MAILBOX_INITIAL_SIZE
FARM_MAILBOX_MAX_SIZE
PLAYER_MAILBOX_INITIAL_SIZE
PLAYER_MAILBOX_MAX_SIZE
```

### 9.3 多 Gate 和路由

主要文件：

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/50-farm-gate.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/20-config.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
internal/gateway/...
internal/actorrouting/...
```

必须保持：

```text
GATEWAY_ID=farm-gate-0 / farm-gate-1
headless endpoint 使用 ordinal DNS
GATEWAY_DOWNLINK_ROUTES 使用稳定 gate_id 映射
GATE_CONTROL_TARGETS 使用稳定 control endpoint
```

不能使用两个副本共享同一个随机或静态 Gate identity。

### 9.4 WebSocket

主要文件：

```text
internal/gateway/transport/websocket/websocket.go
```

已加入 `WriteBufferPool` 并降低正常连接日志噪声；`SetWriteDeadline` 没有删除。当前 profile 仍显示 `serveConnection`、`bufio.NewReaderSize`/`NewWriterSize` 等分配，需要后续单独优化，不能把 WriteBufferPool 宣称为主要内存收益来源。

### 9.5 Friend/Opportunity

已完成 shard lookup、Top-K friend navigation、O(1) activation metrics、FriendIndex Lua 合并读取等优化。

冷 ID 没有好友关系和可行动农场，所以冷登录基线应显式跳过 Opportunity；真实 Opportunity 需要另外 seed returning-player pool。

---

## 10. 常见错误和排查顺序

### `ERR_STORE_UNAVAILABLE` / `OOMKilled`

先看：

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm get pods
kubectl --context uc-test-plus -n developer_026-qqfarm describe pod farm-svr-0
kubectl --context uc-test-plus -n developer_026-qqfarm top pods
```

Farm OOM 时 Gate 可能完全不重启，但这不代表压测通过。必须检查 Farm/Player 的 restartCount、OOMKilled 和 Ready 结果。

### `ERR_OPPORTUNITY_BUSY`

通常是 Opportunity 后台扫描/弹窗压力，不应混入 cold login mailbox 基线。确认是否设置：

```text
WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true
```

### `actor route: stale route` / `REJOIN_REQUIRED`

检查：

1. 是否同时运行了两个部署脚本；
2. Gate 是否有唯一稳定身份；
3. 是否 reset 后仍有旧 Job；
4. 是否在初始 assignment 完成前启动了 Gate/Worker；
5. Redis route snapshot 是否为当前 reset 的 challenge/boot。

### `BACKEND_SLOTS_FULL` / Downlink queue full

不要直接提高 Gate admission。先确认：

```text
GATE_BACKEND_MAX_INFLIGHT=1024
GATE_BACKEND_ADMISSION_WAIT=25ms
```

并查看 Farm downlink queue、Gate backend in-flight 和 rejected metrics。

### `cannot assign requested address`

通常是压测器同时创建过多连接、Job/Pod 复用或 ID/endpoint 生命周期混乱。清理旧 ws-stress Job，确保所有 Job 同时 Running 后再开始统计；不要在已有压测期间 reset。

### `blobstore: writer epoch mismatch`

这是 returning-player bot pool 的已知问题，不是 cold unique ID 基线的判定依据。若要验证真实返回玩家链路，应先修复或重新 seed bot pool。

---

## 11. 后续推荐执行顺序

1. 从远端合并 `f46e4f5`，保留本文和 reset 脚本。
2. 清理旧 ws-stress Job。
3. 用 `QQFARM_GATE_REPLICAS=2` 执行 reset，真正部署 `4 CPU / 16Gi`。
4. 按本文的四 Holder readiness 和 downlink 检查确认 `512+512`、`degraded_shards=0`、`pending_actions=0`。
5. 先做单 Job smoke：

   ```bash
   WS_STRESS_IMAGE='mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:e57fe6ed26118e0b21678d6071fbed3822d0f6b87dc30c0381ddba225da67c62' \
   WS_STRESS_SKIP_FRIEND_OPPORTUNITY=true \
   bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
     cold-thick-login 51 10 10s 1007000000
   ```

   预期 `51/51 Ready`、无 Login/Join error。

6. 启动 12×250/s 的 3000/s 正式压测。
7. 压测中采 CPU profile 和周期 metrics，压测后采 heap/allocs。
8. 对比：
   - `ready_p95_ms` / `ready_p99_ms`；
   - `mailboxFor` retained heap；
   - `farm_actor_active_keys`；
   - mailbox rejected、queued、retired；
   - sweeper lag；
   - activation pending/throttled；
   - Redis LoadAndClaim latency/pool wait。
9. 如果 4 CPU/16Gi 低延迟且无 OOM，再评估 3500/s。3500/s 在旧 16Gi 配置下曾 OOM，当前不能直接宣称通过。
10. 测试结束后如需恢复干净状态，再执行一次 reset；不要在稳态压测中途执行。

---

## 12. 本文引用的关键文件

```text
deploy/k8s/uc-test-plus/developer_026-qqfarm/reset-1024-initial-assignment.sh
deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh
deploy/k8s/uc-test-plus/developer_026-qqfarm/20-config.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/30-player-svr.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/40-farm-svr.yaml
deploy/k8s/uc-test-plus/developer_026-qqfarm/50-farm-gate.yaml
internal/logic/actor/runtime/executor.go
internal/logic/actor/runtime/executor_test.go
internal/farm/actor/registry.go
internal/player/actor/registry.go
internal/gateway/transport/websocket/websocket.go
```
