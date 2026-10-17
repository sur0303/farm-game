# qqfarm thick-login 首页 Ready 达到 Join-only QPS 交接

- 日期：2026-08-17
- 用户：苏鹏煜
- 集群 / Namespace：`uc-test-plus` / `developer_026-qqfarm`
- 工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816`
- 分支：`codex/scaleplan-local-kind-qqfarm-r1`
- HEAD：`9b970a00b600b58a9dca64d196f3715ca5b80dc1`（工作树脏；**未经用户要求不要 commit**）
- 当前状态：`R14_DEPLOYED / THICK_LOGIN_1000_FAIL / JOIN_ONLY_1000_PASS / CODE_CHANGE_NOT_STARTED`
- 已确认目标：保持 **1 Gate / 1 Player / 1 Farm**；首页 Ready 必须包含 Join + 用户信息/红点；机会弹窗异步、可重试，不阻塞 Ready

## 1. 接手后的第一条结论

r14 环境已经部署成功，**不要修 Redis、不要原地滚 Holder**。readonly Bootstrap + `MAX_CONCURRENT_ACTOR_LOADS=2000` 已经上线并测过：

| 场景 | 档 | Ready | 判定 |
|---|---|---:|---|
| thick-login 3000×60s | 200/s `dkr8kdf7vrxm` | 3000/3000 | PASS，P95 46.9ms |
| thick-login 3000×60s | 1000/s `dkr8n1gwuyjr` | **887/3000** | FAIL |
| join-only 3000×60s | 1000/s `dkr8up6uwnoe` | **3000/3000** | PASS，P95 40.9ms |
| join-only 10000×15s 热现场 | 2500/3000/3500 | 10000 / 10000 / 9999 | 功能上限约 3000/s；热现场 P95 已差 |

**1000/s 挂的是厚登录附加逻辑，不是「登进去」。** 纯 Join 同规格全过。下一任不要再调大超时或 `MAX_CONCURRENT_ACTOR_LOADS` 来宣称容量提升。

已确认的产品口径：

```text
首页 Ready = HTTP 登录 + 用户信息/任务/邮件/图鉴/宠物红点 + WS Hello + 自己农场 Join/Snapshot
机会弹窗 = Ready 之后异步；限流/超时只能重试；禁止伪造 NONE
拓扑     = 继续 1 Gate / 1 Player / 1 Farm / 2 Worker / 1 Controller / 4/4/4
```

代码改造 **还没开始写**。计划已定，见 §6。现场 r14 仍是旧 thick-login 客户端：Hello 后并行发 `GET_PLAYER_BOOTSTRAP` + `JOIN_FARM` + HTTP 机会弹窗。

## 2. 可以直接交给下一个 Agent 的提示词

```text
使用 $prepare-qqfarm-loadtest。先完整读：
/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/SKILL.md
/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/references/runbook.md
以及本交接：
docs/verification/perf/qqfarm-thick-login-home-ready-handoff-20260817.md

工作树：
/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
分支：codex/scaleplan-local-kind-qqfarm-r1
只操作 context=uc-test-plus、namespace=developer_026-qqfarm。
未经用户要求不要 git commit。

目标：单实例下首页 Ready 达到 Join-only 的 3000@1000/s。
不要靠加大 GATE_BACKEND_MAX_INFLIGHT / FRIENDSHIP_RPC_TIMEOUT / MAX_CONCURRENT_ACTOR_LOADS 宣称成功。

按计划实现（顺序固定）：
1. 补齐 Gate backend / Player activation / Friendship RPC 分类指标
2. EnsureOwnerInitialized 用已有 rec/version 生成 GetPlayerBootstrapResult，登录 HTTP 返回 home_bootstrap；不得二次 registry.Read / 不得新增 CAS
3. 新 thick-login：HTTP 有 home_bootstrap 时只发 JOIN_FARM；保留 GET_PLAYER_BOOTSTRAP 给旧客户端、Renew、跨日刷新
4. 机会弹窗独立限流，Ready 后再请求，429+Retry-After，禁止 NONE 伪造
5. 单元测试通过后，按 Skill clean reset 部署新 Player（及需要的 Gate/ws-stress 镜像），再跑压测

压测顺序：
- Join 1-player smoke（未用过的 178799* 段）
- thick-login 1-player smoke（未用过的 947* 段，不要用 bot pool）
- preflight --require-ready
- 如 Redis 被 FLUSHALL：重种 5000 bot pool（950000000 / fake 8000000000）
- Join-only 3000@1000/s 基线（未用过的 178800* 段）
- 新 home-ready thick-login 3000@1000/s（bot pool 内未重叠段，建议 950003000）
- 机会异步 30s 排空统计
- 200/s 控制档（bot pool 内另一段）

验收：ready=3000/3000；login/connect/hello/join/bootstrap 错误=0；Gate backend reject 与 Player activation throttle 增量=0；Ready P95≤200ms；3000 个机会请求在 Ready 后 30s 内全部 FOUND/NONE。

把证据写入：
docs/verification/perf/qqfarm-clean-join-vs-opportunity-20260817/r15-home-ready/
并更新本交接文档。
```

## 3. Skill、工作树、禁止事项

必须使用：

```text
/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/
```

| 文件 | 作用 |
|---|---|
| `SKILL.md` | scope、两阶段 bootstrap、停止条件、压测交付 |
| `references/runbook.md` | clean reset 逐步顺序 |
| `scripts/preflight.sh` | context/namespace/Redis/Worker boot |
| `scripts/probe-farm-gate-downlink.sh` | Farm→Gate `:9092` TCP，Gate HTTP `/readyz` 不够 |
| `scripts/seed-returning-bot-pool.sh` | 5000 回归玩家 + 60 假好友 |

硬禁止：

- 不要跑 `apply-r93.sh`
- 不要 raw-apply 仍含 `PENDING_STAGED_APPLY` / `REPLACE_AT_APPLY` 的 YAML
- 不要 `kubectl set env` 改已 assigned 的 Gate/Player（4/4 之后改 env 会重启，Farm downlink 可能中毒）
- 不要 `kubectl set image sts/player-svr *=NEW`；只在 0 replica 时改 `player-svr=<digest>`
- 不要改 Farm digest，除非用户明确要求。当前 Farm 保持 `@sha256:25b3a71ea4c51e21fc720cb6eeb8dabc9f0b92edb6e54a04d3ca7f3f9f47ce57`
- 不要重 apply worker YAML。Worker boot 固定为：
  - `farm-worker-a-60b45f25aecdd662fb511b74512c6dd7`
  - `farm-worker-b-6f895d79c68356098c66682f6b47e236`
- 不要在 Controller=0 窗口等 Ready；`/readyz` 503 是预期，立刻交 12 claims
- 不要复用 Join / 首次 Ensure 的 `player-start`
- 不要 HSET viewer farms
- 不要把 ws-stress 进程 exit 0 当 PASS；解析完整 `{"run_id": ...}`
- 不要 commit，除非用户明确说

`run-ws-stress.sh` 没有可执行位，必须 `bash` 调用。

## 4. 当前集群现场（r14，2026-08-17 21:30 左右）

拓扑全绿，4/4 serving。Gate 曾因 CPU 不足 Pending，后来调度成功，**当前 Gate 是新 Pod**（`farm-gate-5b5b595d85-956sl`），UID 已不是 r14 部署瞬间的 `48qgt`。Player/Farm Holder UID 仍是 r14 捕获值。

```text
context                 uc-test-plus
namespace               developer_026-qqfarm
challenge               d3188d437d62366e92eed26c884ef7ac
Player                  player-svr-0  UID=95d4344d-c610-41ad-b8a2-8bb6e2e92095  restart=0  serving=4
Farm                    farm-svr-0    UID=11b0e98a-2c09-4b00-831e-4c23544fbafd  restart=0  serving=4
Gate                    farm-gate-5b5b595d85-956sl  1/1  restart=0
Worker A/B              Ready restart=0，boot 见上
Controller              route-controller-77dd868794-bhwzn  1/1
helper                  farm-route-bootstrap 仍在
Redis                   6 节点；3 主 farm-redis-0/1/2
route snapshot          观测到 revision 182（最终 Controller 会持续 reconcile，不要假设停在 13）
```

镜像（live，以 STS/Deploy 为准，不要信 git YAML 里的旧 digest）：

| 组件 | digest |
|---|---|
| Player | `farm-route-runtime@sha256:20bce078a8d7f57c447fb7b301b84313aa9ffe498c3cd68133fac69ca4060a64`（bootstrap-readonly-20260817，`-tags timetzdata`） |
| Farm | `@sha256:25b3a71ea4c51e21fc720cb6eeb8dabc9f0b92edb6e54a04d3ca7f3f9f47ce57` |
| Gate | `@sha256:244ab98e60699b8bdcde102c1fdb266003806a517b4b8462422554fdc22fc741` |
| Worker / Controller | `@sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026` |
| ws-stress Job | `@sha256:8e66feebce2b741c04c00744089dd47b09eb9c6ec3c2d1f30ba1e1351973008f`（opportunity-only；`friend_rosters_loaded` 可缺） |

关键环境：

```text
MAX_CONCURRENT_ACTOR_LOADS=2000
MAX_PENDING_ACTOR_LOADS=4096
ACTIVATION_RATE_PER_SEC=0
GATE_BACKEND_MAX_INFLIGHT=1024
GATE_BACKEND_RPC_TIMEOUT=3s
FRIENDSHIP_RPC_TIMEOUT=2s
GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=10000
GATE_FRIENDSHIP_HTTP_RATE_BURST=20000
FARM_FRIEND_NAVIGATION_V3_ENABLED=1
FRIEND_NAVIGATION_WINDOW_MODE=top-k
FRIEND_NAVIGATION_WINDOW_SIZE=10
```

**改 Player/Gate 代码后必须 clean reset 再部署新镜像。** 当前现场可以继续做「旧代码」对照压测，但不能用来验证新 home-ready 行为。

## 5. r14 已经证明了什么

证据目录：

```text
docs/verification/perf/qqfarm-clean-join-vs-opportunity-20260817/r14-bootstrap-readonly/
```

### 5.1 代码已落地（在工作树，未单独成 commit）

- `GET_PLAYER_BOOTSTRAP` 改为 `registry.Read` + `projectPlayerBootstrapView`，无 CAS、无 welcome mail SourceOrder
- 每日登录仍在 HTTP `EnsureOwnerInitialized` 的 `commitOwnerLoginFacts`
- 跨日任务周期 persist 移到 `GET_TASK_LIST`
- 默认 `MAX_CONCURRENT_ACTOR_LOADS=2000`

核心路径：`internal/player/ingress/service.go` `executePlayerBootstrap`。

### 5.2 thick-login 1000/s 失败分布（`dkr8n1gwuyjr`，player-start `950002000`）

```text
1158  friend opportunity 503  {"error":"friend opportunity unavailable"}
 587  bootstrap ERR_TIMEOUT
 155  bootstrap ERR_SERVER_BUSY
 129  join actor busy
  48  network timeout
  24  join stale route
  12  REJOIN_REQUIRED
```

`profiles_loaded=2045`，`ready=887`。Bootstrap 读通的人多于完整 Ready。Farm `queue-full=0`。相对 r12：Bootstrap 错误 2371→742，Ready 304→887，好友 503 从 325 涨到 1158（Bootstrap 变快后更多人挤到弹窗）。

200/s 同池 `950000000`：3000/3000，`mail_red_point_players=0`（welcome 已不挂首页）。

### 5.3 为什么 thick-login 一直差、Join-only 不差

每个 thick-login 玩家同时走三条腿：

```text
HTTP EnsureOwnerInitialized（Join-only 也有）
WSS GET_PLAYER_BOOTSTRAP     ← 额外 Player mailbox Read
WSS JOIN_FARM                ← Gate 单连接 worker 会等 Bootstrap RPC 完成才开始 Join
HTTP GET /friends/opportunity ← 独立 2s Friendship RPC，不计入 ready，但会计 friend_startup_errors
```

Gate 单连接命令 worker **串行**。客户端虽并行 write，服务端仍是 `bootstrap_rpc + join_rpc`。1024 inflight × 3s 超时，1000/s 两条命令会把槽位打满 → `ERR_SERVER_BUSY` / TIMEOUT。

好友机会：Gate `FRIENDSHIP_RPC_TIMEOUT=2s`，成功 P95 已到 ~1943ms，超时被统一打成 HTTP 503 `friend opportunity unavailable`。这不是 429（限流已放 10000/20000）。

Join-only 没有 Bootstrap 帧、没有机会 HTTP，所以 3000@1000/s P95 40.9ms。

超时怎么算：有效预算是 **Gate→Player 这一跳 2s**；Player→Farm 的 `FRIENDSHIP_FARM_RPC_TIMEOUT` 默认也是 2s，但父 ctx 更早截止，不会变成 4s。ws-stress HTTP client 15s 更松，先撞服务端 2s。

## 6. 下一任要实现的改造（尚未写代码）

计划文件（只作参考，不要改计划文件本身）：

```text
/data/home/developer_026/.cursor/plans/thick_login_qps_2e70a806.plan.md
```

验收：`3000 players @ 1000/s`，`ready=3000`，login/connect/hello/join/bootstrap 错误 0，Gate backend reject 与 Player activation throttle 增量 0，Ready P95 ≤ 200ms；3000 个机会在 Ready 后 30s 内全部终态 FOUND/NONE。

### 6.1 观测

- Player `ActivationStats` 接到 `farm_actor_activation_*{actor_kind="player"}`（Farm 已有同类注册）
- Gate：backend inflight、拒绝、按 command type 的 RPC 延迟
- Friendship：把 deadline / resource-exhausted / unavailable 从同一个 503 拆开

### 6.2 Bootstrap 合并进 HTTP Ensure

- proto：`EnsureOwnerInitializedResponse` 增加可选 `public.GetPlayerBootstrapResult player_bootstrap = 2`
- `EnsureOwnerInitialized` 在 login facts 之后用手上的 `rec/version` 调 `projectPlayerBootstrapView`；**禁止**再 `registry.Read`、禁止新增 CAS
- 邮件仍 50ms fail-soft；投影/邮件失败则省略字段，登录仍成功，客户端回退 WSS 命令
- Gate `Bootstrapper` 返回该投影，写入 `LoginResult.home_bootstrap`（JSON 名）
- `Renew` 不带投影
- 保留 WSS `GET_PLAYER_BOOTSTRAP` 作为 RecoveryRequery（旧客户端、重连、跨日）
- 不要把私有首页塞进 `ServerHello` / FarmSnapshot / Push
- 更新 26/28 契约文档：HTTP 登录可附带同一 Result

关键文件：

```text
api/proto/logicrpc/logic_service.proto
internal/player/ingress/service.go
internal/gateway/auth/login.go
internal/gateway/auth/grpc_bootstrap.go
internal/gateway/auth/sharded_bootstrap.go
```

### 6.3 thick-login 只剩 JOIN_FARM

- `internal/testkit/gateclient/client.go` 解析 `home_bootstrap`
- `cmd/ws-stress/main.go`：HTTP 投影通过 `validateThickBootstrap` 后 **不发** 首个 WSS Bootstrap，只发 Join；Ready 仍等 Snapshot
- 旧响应缺字段才走旧命令
- 测完必须发新 ws-stress 镜像；当前集群 Job 镜像不含该逻辑

### 6.4 机会弹窗移出洪峰

- `/api/v1/friends/opportunity` 独立限流，过载 `429 + Retry-After`
- ws-stress：`recordReady` 之后再拉机会；遵循 Retry-After + 有上限抖动；单独统计 `opportunity_attempted/completed/retries/terminal_errors/drain_ms`；不得改变 Ready
- 保持 V3 CAS 与「未知不可转 NONE」
- 本轮不加 2s/3s 超时
- 若 200/s 削峰后 30s 仍排不完：第二阶段再做 Header/Facts singleflight 或 per-viewer 物化投影，不要和登录关键路径混一次实验

### 6.5 明确不采用

加大 inflight、加大超时、加大 pending、把机会重新绑回 Ready、删除 WSS Bootstrap 命令、HSET 农场、横向加 Pod。

## 7. 怎么部署（改代码后）

只改 Player 时：在 0 replica 换 Player digest，Farm 保持现 digest。Gate/ws-stress 有代码改动才各自发镜像。

Player 构建（必须 `-tags timetzdata`）：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
# 从当前 live Player 镜像抽出非 player-svr 二进制，只重编 player-svr
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags timetzdata -buildvcs=false -trimpath \
  -o .local-build/player-svr ./cmd/player-svr
# 推 mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:<new>
```

然后 **完整 Skill clean reset**（FLUSHALL 会删 bot pool，测完必须重种）。顺序摘要：

1. `preflight.sh --repo <worktree>`，先记下两个 Worker `WORKER_BOOT_ID`
2. scale Gate/Worker/Controller/Player/Farm 到 0；删 `farm-route-bootstrap`
3. 确认 `farm-redis-0/1/2` 是 master 且 16384 slots，再 `FLUSHALL`，`DBSIZE=0`
4. 0 replica 时 `set image sts/player-svr player-svr=<new-digest>`；Gate 好友限流 10000/20000（仍是 0 replica）
5. Holder scale 1，等 Pod 对象存在，捕获 UID / STS UID / boot=`<pod>-<uid>`
6. 起 helper `redis:7-alpine`，拷 `/tmp/rt2q-route-bootstrap-qqfarm` 与 fixture
7. bootstrap-only，challenge 新生成，`--shard-count 4`，要求 `COMMITTED` rev 1、`claims=null`
8. `ROUTE_CONTROLLER_BOOT=$initial`，Controller=1，等 Player/Farm `READY_ZERO_SHARD`（serving=0，rev=1）
9. Worker 再 Gate，rev 1 期间预热；Worker live boot 必须等于捕获值；`probe-farm-gate-downlink.sh` PASS
10. **只** Controller=0；Pod 消失后核对 UID/boot/restart=0（phase=Running 即可）；立即 12 claims `--resume-existing`，要求 `ALREADY_COMMITTED` 1→13、12 claims
11. `ROUTE_CONTROLLER_BOOT=$final`，Controller=1；Player **和** Farm serving=4；再 probe
12. Join smoke → unused thick-login smoke → `--require-ready` → seed 5000

Redis helper：

```text
RT2Q_BOOTSTRAP_REDIS_URL=redis://farm-redis-0.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0
RT2Q_BOOTSTRAP_REDIS_CLUSTER_URLS=redis://farm-redis-0...:6379/0,redis://farm-redis-1...:6379/0,redis://farm-redis-2...:6379/0
```

端点：

```text
Player  player-svr-0.player-svr-headless.developer_026-qqfarm.svc.cluster.local:9100
        control :19100
Farm    farm-svr-0.farm-svr-headless.developer_026-qqfarm.svc.cluster.local:9090
        control :19101
Worker  farm-worker-a|b.developer_026-qqfarm.svc.cluster.local:9201
TLS     holder-control.developer_026-qqfarm.svc.cluster.local
```

exec 必须带 `-c player-svr|farm-svr|farm-worker|farm-gate`，wget 必须 `-q`。

## 8. 怎么压测

脚本：

```bash
KUBE_CONTEXT=uc-test-plus bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  <scenario> <conns> <rate> <duration> <player-start>
```

当前 Job 镜像是旧 thick-login。**§6.3/6.4 改完必须发新 ws-stress 镜像**，并改 `run-ws-stress.sh` 的 `WS_STRESS_IMAGE`，或导出环境变量覆盖。

解析 JSON：从日志里取完整 `{"run_id": ...}` 对象。`wait --for=complete` 可能误报超时，Job 实际已 Complete；用 `kubectl logs job/...` 补日志。

### 8.1 已占用的 player-start（不要复用）

| 用途 | 范围 | 说明 |
|---|---|---|
| Join smoke r14 | `1787930000000` | 1 人 |
| thick smoke r14 | `946000000` | 1 人，非 bot pool |
| thick 3000@200 | `950000000–950002999` | bot pool |
| thick 3000@1000 | `950002000–950004999` | 与 200 档后半重叠，不要再当冷回归 |
| join-only 3000@1000 | `1787940000000–1787942999` | 首次 Join |
| join ladder 10k | `1787950000000` / `1787960000000` / `1787970000000` 各 10000 | 热现场 |
| bot pool 全体 | `950000000–950004999` | Ensure 过；fake `8000000000–8000001019` |

建议下一轮：

```text
Join smoke              1787990000000
thick smoke（非池）      947000000
Join-only 3000@1000     1788000000000
home-ready 3000@1000    950000000     （若未 FLUSHALL 可复用池；FLUSHALL 后重种同一布局）
机会排空观察            同上一次 thick 的同一 run
200/s 控制              950003000     （若 1000 档用了 950000000）
```

Join / 首次 Ensure 永不复用。thick 回归在 Redis 未 flush 时必须用 bot pool。

### 8.2 命令模板

```bash
QQ_REPO=/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816
QQ_SKILL=/data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest
EVIDENCE=$QQ_REPO/docs/verification/perf/qqfarm-clean-join-vs-opportunity-20260817/r15-home-ready
mkdir -p "$EVIDENCE"
cd "$QQ_REPO"

# smoke
KUBE_CONTEXT=uc-test-plus WS_STRESS_WAIT=180s bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh join 1 1 5s 1787990000000 \
  | tee "$EVIDENCE/join-smoke.log"

KUBE_CONTEXT=uc-test-plus WS_STRESS_WAIT=180s bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 1 1 10s 947000000 \
  | tee "$EVIDENCE/thick-smoke.log"

bash "$QQ_SKILL/scripts/preflight.sh" --repo "$QQ_REPO" --require-ready

# 仅 FLUSHALL 后
bash "$QQ_SKILL/scripts/seed-returning-bot-pool.sh" \
  --out-dir "$EVIDENCE" --viewer-start 950000000 --total 5000 --fake-base 8000000000

# 基线：纯 Join
KUBE_CONTEXT=uc-test-plus WS_STRESS_WAIT=180s bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh join 3000 1000 60s 1788000000000 \
  | tee "$EVIDENCE/join-3000-r1000.log"

# 目标：新 home-ready
KUBE_CONTEXT=uc-test-plus WS_STRESS_WAIT=180s bash \
  deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 3000 1000 60s 950000000 \
  | tee "$EVIDENCE/thick-3000-r1000.log"
```

每档前后抓：

```text
kubectl top pod
Player/Farm/Gate /metrics：go_goroutines、process_cpu、RSS、farm_downlink_queue_full_total
新指标：Gate backend reject/inflight、Player activation_throttled_total、Friendship RPC 分类
Pod Ready / restart / serving_shard_count
```

### 8.3 PASS / FAIL

**home-ready thick-login 1000/s：**

- `attempted == ready == 3000`
- `login_errors=connect_errors=hello_errors=join_errors=0`
- `thick_login.bootstrap_errors=0`
- `profiles_loaded=task/mail/compendium/pet_summaries_loaded=3000`
- Ready P95 ≤ 200ms
- Gate backend reject Δ=0，Player `activation_throttled_total` Δ=0
- `farm_downlink_queue_full_total` Δ=0
- Pod restart=0，仍 4/4

**机会弹窗（不进 Ready）：**

- `friend_opportunity_found + friend_opportunity_none == 3000`（在 drain 窗口内）
- 不允许把 UNKNOWN/503 记成 NONE
- 429 必须重试；30s 内排空
- `friend_startup_errors` 在新口径下应为终态失败，不含已重试成功的 429

**Join-only 基线：** 3000/3000，错误全 0。若基线已 FAIL，不要解释 thick 结果。

停止条件：业务错误、Pod 重启、readiness 翻转、Worker stale route、Join `REJOIN_REQUIRED`+Farm `connection refused`。后者必须 clean reset，禁止在同一 Farm Pod 上重试 Join。

## 9. 建议单测

- Ensure 返回的 Result.revision == 最终 Ensure version；随后 WSS Bootstrap 无 CAS
- 同日 Ensure 重放不 CAS
- 跨日：Ensure persist period；WSS Bootstrap 仍不 persist（现有 `TestGetPlayerBootstrapCrossDayDoesNotPersistPeriod`）
- welcome 仍不挂 Ensure/Bootstrap
- `taskDomain == nil`：登录成功、无 `player_bootstrap`
- Gate JSON round-trip；`Renew` 无投影
- ws-stress：有 `home_bootstrap` 时不发 WSS Bootstrap；无字段时回退
- 机会 429 不变成 NONE；UNKNOWN 保持 503/`temporarily unavailable`

## 10. 工作树脏文件说明

HEAD `9b970a00` 上已有大量未提交改动，包括 r14 Bootstrap readonly、好友 V3、downlink、ws-stress opportunity-only、config 2000 等。下一任改 §6 时叠在这些文件上。**不要 reset，不要无关 commit。**

本轮新改主要还会动：

```text
api/proto/logicrpc/logic_service.proto + 生成代码
internal/player/ingress/service.go
internal/player/actor/registry.go          # ActivationStats
cmd/player-svr/main.go                    # 注册 player activation metrics
internal/gateway/command/gate.go
internal/gateway/auth/login.go / grpc_bootstrap.go / sharded_bootstrap.go / share.go
internal/gatewayapp/app.go                # 独立机会限流
internal/friendshipservice/grpc_client.go
internal/testkit/gateclient/client.go
cmd/ws-stress/main.go / main_test.go
deploy/.../run-ws-stress.sh               # 新 ws-stress digest
```

## 11. 相关证据路径

```text
r14 结果
docs/verification/perf/qqfarm-clean-join-vs-opportunity-20260817/r14-bootstrap-readonly/

r14 Join 阶梯（热现场，P95 不可当冷基线）
.../r14-bootstrap-readonly/join-ladder/

r12 welcome-on-bootstrap 对照
.../r12-welcome-bootstrap/

batch=64 Join 旧交接（另一条线，不要和 thick-login 混）
docs/verification/perf/qqfarm-batch64-loadtest-handoff-20260817.md
```

## 12. 若现场已坏，从哪重来

出现以下任一情况，不要修 Redis / 不要重启 assigned Holder，从 §7 clean reset 重来：

- Worker `stale local route`
- Holder restart 或 UID 变化
- Farm `DeliverBatch connection refused` / Join `REJOIN_REQUIRED`
- Gate 被 `set env` 重启
- claims 不是 12 条或 revision 不是 13
- Redis master 角色不对

Controller=0 时全 0/1 Ready **不是**崩溃。
