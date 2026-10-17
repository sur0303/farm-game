# Q1-PERF-BASELINE-V1 详细交接文档

日期：2026-08-09  
任务：`Q1-PERF-BASELINE-V1`  
状态：**工具链已交付；首轮正式运行在正确性硬门停止；尚未形成可用于性能前后对比的 `BASELINE_RECORDED`**

> **后续诊断更新（同日）：** 已通过 Wire Tap 和只读 Redis Dump 确认，原 F3 超时是测试仍按一次点击、而固定 H5 已要求两次 Theft 确认造成的 `CONTRACT_CONFLICT / H5_WIRE_THEFT_MISSING`。Test-only overlay 对齐后获得完整 `F3_CHAIN_PASS`。同时确认反复出现的两个 Invalid Candidate 属于 F3 之前同一 F1 Care Operation 的 Dispatch/Result Order，机器分类为 `PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG`，这才是当前产品正确性阻断。详见 `q1-f3-invalid-order-followup-20260809.md`。本文其余部分保留首轮运行的历史视角。

---

## 1. 一页结论

本任务已经完成以下工作：

1. 按指定 Backend/H5 精确 Commit 创建了两个独立 worktree 和 Q1 分支；
2. 建立了可重复运行的 Scenario Coordinator、开放到达调度、资源采样、Evidence Reporter 和统一报告格式；
3. 复用了现有 Go Protocol Bot、双 Playwright H5、Fault Controller、Authority Probe 和 Client State Oracle；
4. 冻结了完整、首轮保守、工具 Smoke 三套压测 Profile；
5. 实现了 planned/actual load、CPU/RSS/goroutine/fd/GC/network、正确性不变量和四态领域判定；
6. 完成了工具 Smoke，并执行了一次选定的正式 Baseline V1 运行；
7. 正式运行在产品正确性锚点触发 `FAIL_CORRECTNESS`，按停止条件没有继续施加连接、热点和混合压力；
8. 保存了机器可读核心证据以及约 294 MiB 原始浏览器/服务/代理证据；
9. 没有修改任何禁止写集，没有 push，没有 merge integration，也没有声称支持 3000 万 DAU。

选定运行：

```text
run_id   = q1perf-b1-20260809100947-1793411
verdict  = FAIL_CORRECTNESS
class    = FAIL
```

失败边界：

- 独立 Go Protocol Bot：`PASS`；
- 双真实 Playwright H5：在 F3 等待 Fault Controller 观察到 `FAIL_FARM_CHANGE` 时，30 秒超时；
- Fault Proxy 原始事件中没有该 `FAIL_FARM_CHANGE`，也没有对应 `REJOIN_REQUIRED`；
- Worker 同时重复报告 Theft 恢复候选 `InvalidOrders:2`，最后错误为 `theft online: target binding unavailable: not found`；
- Authority business oracle 因 Browser Verdict 非 PASS 而为 `INCONCLUSIVE`；
- 因正确性停止条件触发，200/500 连接阶梯、200-WSS baseline/slow-reader 和混合业务阶梯均未执行。

因此当前不能写：

```text
BASELINE_RECORDED
PERF_REGRESSION_PASS
CP-PERF-BASELINE-V1 已成功冻结
SUPPORTS_30M = true
```

---

## 2. 精确基线与 worktree

### 2.1 Backend

```text
源仓库：    farm-game
worktree：  farm-game-q1-perf-baseline-v1
分支：      codex/q1-perf-baseline-v1
生产基线：  260babcd93cfb6a4a58a481cc3c7f8c1f95e455a
证据归档头：b5b34fbeed0dc1e956b854798a4e62f2ad31fa57
```

`backend_commit` 与 `backend_checkout_commit` 被刻意分开记录：

- `backend_commit` 是被测生产代码基线 `260bab...`；
- `backend_checkout_commit` 是该基线上仅增加 Q1 测试/报告文件的 Q1 Commit；
- Manifest 列出两者之间的全部路径；只要出现 Q1 Owned Write Set 之外的路径，Manifest 初始化就会失败。

### 2.2 H5

```text
源仓库：    classic-farm-h5
worktree：  classic-farm-h5-q1-perf-baseline-v1
分支：      codex/q1-perf-baseline-v1
HEAD：      df379b5fcc5eb710fc1efa5406c752ba18c3ab30
```

H5 worktree 没有业务代码修改，Git 状态干净。

本机为了避免在测量窗口安装依赖，将该 H5 worktree 的 `node_modules` 链接到 package-lock 完全相同的主 H5 worktree。两个 `package-lock.json` 的 SHA-256 均为：

```text
e4263209c9712aaaff3856bddbc9cbdca308910b0a5d18bc6962c437cad2f059
```

此链接是 ignored 的本机准备项，不属于提交证据。换机器执行时，应在测量窗口外运行精确 lockfile 的 `npm ci`。

### 2.3 Checkpoint Tag 异常

任务开始时检查到两个仓库均存在：

```text
cp-mvp-before-hardening-20260809
```

但该 Tag 在 worktree 创建过程中被其他并行工作移动，最终不再指向用户给定的两个精确 Integration Commit。当前观察值为：

```text
Backend Tag -> 2c46f855b1eb2fd9ace596989e80b3c28ec0a140
H5 Tag      -> e39e6d7457867dd28e5b55f15fdb165cfede5819
```

Q1 的处理是：

1. 不移动、不重建、不强推该 Tag；
2. 将两个 Q1 worktree 强制固定到用户给定的精确 Commit；
3. Manifest 继续记录精确 SHA，而不是记录移动中的 Tag 名。

---

## 3. 目标完成度

| 原目标 | 当前状态 | 说明 |
|---|---|---|
| 记录第一批改造前的真实性能 | **BLOCKED_BY_CORRECTNESS** | 正式运行在产品正确性锚点停止，未进入压力阶段 |
| 建立可重复运行的压测工具和报告格式 | **完成** | Coordinator、Profile、Sampler、Reporter、核心输出均已实现并通过 Smoke |
| 对第一批每项修改做前后对比 | **工具已具备，尚不可执行有效比较** | 候选重放和 metrics comparison 已实现，但当前没有有效 `BASELINE_RECORDED` |
| 发现明显瓶颈和观测缺口 | **完成第一轮** | 发现 F3/Theft 路径阻断，并生成统一 observability gaps |
| 不宣称支持 3000 万 DAU | **完成** | Manifest、Verdict、报告均显式写入 false/禁止声明 |

---

## 4. 实现的正式测试体系

当前实现遵循：

```text
Scenario Coordinator
├── Go Protocol Bots
├── 2 个 Playwright H5
├── Fault Controller
├── Authority Probe
├── Client State Oracle
└── Evidence Reporter
```

### 4.1 Scenario Coordinator

入口：

```text
scripts/perf/run-baseline
```

负责：

- 校验 Backend/H5 精确 Commit；
- 校验 Q1 分支相对 Backend 基线是否只修改允许路径；
- 生成唯一 `run_id` 和 Redis namespace；
- 创建临时本地 Redis；
- 编译被测生产二进制和冻结的 Load Tool；
- 顺序执行正确性、热点、连接和混合 Profile；
- 触发停止条件；
- 汇总五个标准输出；
- 在最终判定前销毁精确 Redis 容器。

### 4.2 Go Protocol Bots

复用和补充如下：

- `cmd/authority-smoke-bot`：产品正确性锚点中的独立真实协议见证；
- `cmd/ws-stress`：Gate 连接阶梯和 Join 压力；
- `cmd/ws-stress -scenario theft-hot`：既有 200-WSS 热点 Theft、慢读者和 FarmVersion Oracle；
- `cmd/perf-baseline`：位于 Q1 允许写集的混合业务 Journey Bot。

`cmd/perf-baseline` 是既有 `authority-smoke-bot` 状态机的 Q1 隔离分支，增加 `--stop-after-plant`，用于开放到达的：

```text
Register
→ Login
→ ClientHello
→ Join/Snapshot/Ready
→ Bootstrap
→ ClientRecoveryComplete
→ Buy
→ authority-confirmed Plant
```

没有修改既有 `cmd/ws-stress` 或 `cmd/authority-smoke-bot` 的核心语义。

### 4.3 Playwright H5

产品锚点运行：

- 2 个真实 BrowserContext；
- headed Chromium + 独立 Xvfb；
- 每轮动态 H5 端口，避免并行 Agent 占用固定 5194；
- H5 始终来自精确 Commit `df379b...`；
- Playwright 不承担主压力。

### 4.4 Fault Controller

复用：

```text
cmd/gate-vertical-e2e proxy
```

产品锚点覆盖：

- F1：丢失 CARE CommandResult；
- F2：真实 Gate restart/drain 恢复；
- F3：FarmChange 故障和 REJOIN 恢复；
- CLAIM_TASK 丢失结果恢复。

选定运行中 F1、F2 已完成；F3 未观察到计划中的 `FAIL_FARM_CHANGE`。

### 4.5 Authority Probe

复用：

- `cmd/authority-smoke-fixture`；
- `cmd/theft-hot-audit`。

只读 Authority/业务审计，不作为 H5 正式同步路径，不修改权威业务数据。

### 4.6 Client State Oracle

既有 Theft Hot Bot 会对每个健康连接记录：

- Snapshot 基线版本；
- 每个 FarmChange 版本；
- 版本 Gap；
- 版本回退/重复；
- REJOIN_REQUIRED；
- 健康连接完成数。

选定正式运行因更早的产品正确性硬门失败，没有进入 Theft Hot Profile。

### 4.7 Evidence Reporter

入口：

```text
tools/perf/q1perf.py
```

支持子命令：

```text
manifest
sample
run-bot-arrivals
ingest-ws
ingest-correctness
ingest-theft
record-status
endpoint-hash
list-stages
finalize
```

---

## 5. 已新增文件

### 5.1 Load Bot

```text
cmd/perf-baseline/main.go
```

作用：开放到达混合业务中的真实 Go Protocol Journey。

### 5.2 Coordinator 与栈编排

```text
scripts/perf/run-baseline
scripts/perf/lib-stack.sh
scripts/perf/run-product-anchor-profile
```

`lib-stack.sh` 提供：

- 动态端口；
- 精确临时 Redis 容器；
- Backend-under-test 二进制构建；
- Load Tool 构建；
- Authority fixture；
- Player/Farm/Worker/Gate 独立进程启动；
- readiness；
- 精确 PID/容器清理。

`run-product-anchor-profile` 是 Q1 自有的产品锚点编排副本，只修复测试编排问题：

- 动态 H5/Xvfb；
- Gate/Fault Proxy/Player/Farm gRPC 边界 readiness；
- Gate dependency `/readyz`；
- 不修改生产代码和业务裁决。

### 5.3 Profile

```text
deploy/load/q1-perf-baseline-v1.json
deploy/load/q1-perf-baseline-v1-first-run.json
deploy/load/q1-perf-baseline-v1-smoke.json
```

三者分别是：

- Full：完整 200/500/1000/2000/5000 Gate 阶梯；
- First Run：保守首轮，最多 500 连接；
- Smoke：仅验证工具，`baseline_eligible=false`，禁止发布为正式基线。

### 5.4 Reporter 与测试

```text
tools/perf/q1perf.py
tools/perf/test_q1perf.py
tools/perf/.gitignore
```

### 5.5 文档和选定证据

```text
docs/verification/perf/README.md
docs/verification/perf/performance-baseline-report-template.md
docs/verification/perf/baseline-v1-20260809/**
docs/verification/perf/results/.gitkeep
```

---

## 6. 冻结的 Profile

## 6.1 Profile 1：产品正确性锚点

首轮配置：

```text
2 Playwright H5
1 independent Go Protocol Bot
Hero
Task Claim
F1
F2
F3
Authority Oracle
```

这是所有性能压力之前的硬门。

## 6.2 Profile 2：Gate 连接阶梯

Full：

```text
200
500
1,000
2,000
5,000
```

First Run：

```text
200 @ 100 arrivals/s, 20s
500 @ 200 arrivals/s, 20s
```

每个 Stage 使用：

- 新进程栈；
- 新 Redis 容器；
- 新 namespace；
- 新玩家 ID 区间。

这样避免前一档 Actor、Record、连接和 GC 状态污染下一档增长曲线。

## 6.3 Profile 3：热点农场

冻结为现有：

```text
200-WSS Theft baseline
200-WSS Theft slow-reader
```

证据包括：

- Ready barrier；
- 10 SUCCEEDED / 189 terminal no-effect-or-rejected 目标；
- idempotent retry；
- FarmVersion 连续；
- Workflow/Receipt/资产守恒审计；
- 慢读者隔离。

选定正式运行未执行此 Profile。

## 6.4 Profile 4：混合业务小阶梯

当前开放到达压力支持：

```text
REGISTER
LOGIN
JOIN
GET_PLAYER_BOOTSTRAP
BUY
PLANT
```

First Run：

```text
mix-low:    Join 60@6/s   + Journey 12@2/s
mix-medium: Join 120@12/s + Journey 24@3/s
mix-high:   Join 240@24/s + Journey 48@6/s
```

以下操作目前由 Hero/热点正确性锚点覆盖，但还没有进入开放到达 Mixer：

```text
SELL
UNLOCK_PLOT
WATER
THEFT
CLAIM_TASK
FRIEND_LIST
OPERATION_RESULT
```

该边界已在 `observability-gaps.json` 标为 `PARTIAL`，没有猜测吞吐。

---

## 7. 开放到达和 planned/actual

连接压力继续使用 `ws-stress` 的固定 ticker 到达率，并记录：

```text
configured_connections
connect_rate_per_second
attempted
ready
scenario_succeeded
scenario_errors
unexpected_disconnects
```

Q1 自有 Journey Scheduler 额外记录：

```text
scheduled_arrivals
launched_arrivals
dropped_arrivals
arrival_lag_p50_ms
arrival_lag_p95_ms
arrival_lag_p99_ms
arrival_lag_max_ms
arrival_lag_violation_count
completed
failed
timed_out
```

判定纪律：

- `actual < planned`：`INCONCLUSIVE_ENVIRONMENT`；
- arrival lag 超预算：`INCONCLUSIVE_ENVIRONMENT`；
- Load Generator 达到冻结 CPU 水位：`INCONCLUSIVE_ENVIRONMENT`；
- Go Bot 协议/业务错误：失败，不能只报告成功请求延迟。

现有 `ws-stress` 自身仍不输出每条到达的 scheduler lag，因此连接阶梯的 `open_arrival_lag_ms` 仍标记为 `PENDING_OBSERVABILITY`；不能用 Journey Scheduler 的 lag 冒充 ws-stress 的 lag。

---

## 8. 资源采样

`q1perf.py sample` 每 0.5 秒采样：

### 8.1 每进程

```text
alive
CPU seconds
CPU one-core scale
CPU machine percent
RSS
virtual memory
threads
fd
/proc IO bytes/chars
```

进程角色：

```text
load-generator
gate
farm
player
worker
```

### 8.2 Go/Prometheus

抓取包括：

```text
go_goroutines
go_memstats_*
go_gc_duration_seconds_*
process_resident_memory_bytes
process_open_fds
FarmGate WSS
Gate command/lease
Actor activation/mailbox
Farm subscription/event lane
Workflow backlog/claim
Theft recovery
Route/grpc metrics
```

### 8.3 网络

记录 `/proc/net/dev` 的开始/结束差值，明确标记：

```text
network_scope = host_network_namespace
```

它不是 per-process 网络归因，报告没有伪装成每进程网络字节。

---

## 9. Manifest 和标准输出

每轮要求的五个文件均已实现：

```text
baseline-run-manifest.json
baseline-verdict.json
baseline-metrics.json
observability-gaps.json
performance-baseline-report.md
```

Manifest 记录：

```text
backend_commit
backend_checkout_commit
backend_q1_only_diff_paths
h5_commit
h5_checkout_commit
load_tool_commit
host/machine_spec_hash
Redis mode/endpoint hash/namespace
config_path/config_hash
test window
planned_load
actual_load
cleanup_completed
capacity_claim
```

Manifest 明确包含：

```text
capacity_claim = NOT_A_30M_DAU_CAPACITY_PROOF
```

Verdict 明确包含：

```text
supports_30m_dau = false
```

---

## 10. 判定模型

机器分类：

```text
PASS
FAIL
INCONCLUSIVE
```

领域状态：

```text
BASELINE_RECORDED
PERF_REGRESSION_PASS
FAIL_CORRECTNESS
INCONCLUSIVE_ENVIRONMENT
```

优先级：

1. 任一硬正确性不变量 > 0 → `FAIL_CORRECTNESS`；
2. 环境、实际负载、Load Generator、证据不完整 → `INCONCLUSIVE_ENVIRONMENT`；
3. Baseline 所有配置 Profile 完整、正确性为 0 → `BASELINE_RECORDED`；
4. Candidate 在同 Profile 下正确性通过且可比目标不显著退化 → `PERF_REGRESSION_PASS`。

当前 Candidate 比较器以冻结阈值比较可比 latency 指标；如果没有可比指标，结果为 Inconclusive，不会假通过。

注意：当前选定运行没有 `BASELINE_RECORDED`，因此不能将它的空压力 metrics 用作第一批候选的有效性能比较基准。

---

## 11. Redis 安全

本实现首轮只允许：

```text
ephemeral_local
```

行为：

- Redis 绑定 loopback 动态端口；
- 每个 Stage 新建精确容器；
- 每轮唯一 namespace；
- Evidence 只记录 `SHA-256(host:port/db)`；
- 不记录 Redis password；
- 不执行全库清理；
- Cleanup 只删除 Q1 自己创建的精确容器；
- 删除后通过 `docker inspect` 不存在，才写 `cleanup_completed=true`。

选定运行：

```text
redis.mode              = ephemeral_local
redis.secret_recorded   = false
redis.cleanup_completed = true
```

共享公司 Redis 适配器被有意禁用。若后续必须使用共享 Redis，需要单独评审：

- endpoint hash；
- unique namespace；
- exact namespace-only unlink；
- 外部流量基线；
- 首轮最大压力；
- 禁止影响其他环境。

---

## 12. 工具校准中发现并修复的问题

以下是 Q1 工具问题，不是正式 Baseline 结论：

| 问题 | 处理 |
|---|---|
| Bash readiness 函数在 `set -u` 下同一 local 声明引用未赋值变量 | 拆分声明和 deadline 计算 |
| stack-up 在 command substitution 中提前失败可能遗留进程/Redis | 增加函数 RETURN trap，精确清理部分栈 |
| B0 的 `ws-stress -scenario plant` 缺少当前 Plant 必填上下文，返回 `invalid input: required fields` | 不修改既有 Bot；在允许写集新增 `cmd/perf-baseline` Journey |
| 完整 authority bot 并发 Journey 包含 premature Harvest，适合作正确性锚点但不适合作轻量开放到达 | Q1 Journey 增加 `--stop-after-plant`，保留到权威 Plant 的真实链路 |
| 固定 H5 端口 5194 被其他并行 Agent 使用 | 每轮动态 H5 端口和动态 allowed origin |
| Player/Farm/Proxy/Gate listener 启动窗口造成首个协议 Bot 偶发失败 | Q1 编排增加确定性 TCP 和 dependency readiness，不修改生产代码 |
| 报告重放会移动原 test window | Reporter 改为首次 finalize 冻结，后续格式重放保留窗口 |
| 正确性硬门失败时报告措辞仍像已记录基线 | 报告改为明确“baseline performance was not recorded” |

工具 Smoke 最终通过了：

- 20 连接开放到达；
- Join + Buy/Plant 混合 Journey；
- planned = actual；
- Resource/Prometheus 采集；
- 报告五件套；
- 精确 Redis/进程清理。

Smoke Profile 已设置：

```text
baseline_eligible = false
```

因此后续不得将 Smoke PASS 发布为 Baseline V1。

---

## 13. 正式选定运行

### 13.1 身份

```text
run_id              q1perf-b1-20260809100947-1793411
backend_commit      260babcd93cfb6a4a58a481cc3c7f8c1f95e455a
h5_commit           df379b5fcc5eb710fc1efa5406c752ba18c3ab30
load_tool_commit    63a74786fb6c1eb836a7a3e89e6da4d398cece3e
config_hash         sha256:a4ceef4f2d10f850b183f3ecab0cbe6fdb15da4399389a774d86c8c8917cee17
machine_spec_hash   sha256:e2601c8b7b361b034d2ed44621ad4b461ed9625242069f78057c47a9cdcd41b5
window              2026-08-09T10:09:48.039037Z
                    → 2026-08-09T10:11:14.247790Z
```

机器：

```text
AMD EPYC 7K62 48-Core Processor
32 logical CPUs
66,879,913,984 bytes RAM
Linux 6.6.119-49.20.tl4.x86_64
nofile soft/hard = 524288/524288
Docker 29.3.1
Go 1.26.3-X:nodwarf5
Node 22.23.1
```

### 13.2 判定

```text
verdict = FAIL_CORRECTNESS
class   = FAIL
hard_correctness_invariant_count = 2
```

两个计数是同一产品锚点的两个机器信号：

```text
browser_witness_failure = 1
hero_or_fault_failure   = 1
```

不是两次独立发奖或两个 FarmVersion Gap。

### 13.3 已通过部分

独立 Go Protocol Bot：

```text
register            ok
login               ok
hello               ok
join                ok
bootstrap           ok
recovery_complete   ok
buy                 ok
plant               ok
premature_reject    ok
grow                ok
harvest             ok
bootstrap_refresh   ok
recovery_once       ok
```

Browser 在失败前已完成：

```text
login
buy-and-plant
harvest
reverse-visit-b-to-a-ready
share-and-join-friend
f1-lost-care-result-recovered
friend-farm-simulated-disconnect-recovery
f2-gate-drain-restored-friend-target
water-cycle-cleared-on-harvest
```

### 13.4 阻断点

H5 Spec：

```text
classic-farm-h5-q1-perf-baseline-v1/
  src/test/e2e/cp-vertical-b.spec.ts:351
```

失败断言：

```text
等待 downlinkFaultEvents() 中出现：

event.type == "FAIL_FARM_CHANGE"

30 秒超时，Expected true，Received false。
```

选定 Proxy 事件：

```text
FAIL_FARM_CHANGE count = 0
REJOIN_REQUIRED count  = 0
```

同期 Worker 反复记录：

```text
Discovered:2
InvalidOrders:2
Errors:2
LastError: theft online: target binding unavailable: not found
```

这是一组需要 Theft/Worker/Fault 路径 Owner 继续定位的相关证据；Q1 不据此越权修改业务实现，也不在交接中宣称已经确定根因。

### 13.5 为什么没有继续压

任务停止条件明确包括：

```text
任一正确性不变量 > 0
```

所以没有继续运行：

```text
hot-farm Theft baseline
hot-farm Theft slow-reader
connections-200
connections-500
mix-low
mix-medium
mix-high
```

报告中的 pressure planned/actual 为 0 是真实停止结果，不是漏报，也不是 PASS。

---

## 14. Evidence 位置

### 14.1 Git 内核心证据

```text
docs/verification/perf/baseline-v1-20260809/
```

包含：

```text
README.md
authority-after.json
baseline-metrics.json
baseline-run-manifest.json
baseline-verdict.json
browser-verdict.json
observability-gaps.json
performance-baseline-report.md
product-correctness-anchor.json
protocol-verdict.json
```

### 14.2 本机原始证据

```text
docs/verification/perf/results/
  q1perf-b1-20260809100947-1793411/
```

大小约：

```text
294 MiB
```

包括：

- Playwright trace；
- 截图；
- 视频；
- Browser artifacts；
- Gate/Farm/Player/Worker log；
- Fault Proxy events；
- route snapshot；
- fixture；
- coordinator log；
- Redis log；
- 五件套报告。

`docs/verification/perf/results/*` 默认 Git ignored，避免把每次临时运行的 294 MiB 浏览器证据全部提交。删除 worktree 前必须先归档此目录到外部 Evidence 存储。

---

## 15. Observability Gaps

当前统一记录：

| Metric | 状态 | Owner/影响 |
|---|---|---|
| `actor_record_bytes` | `PENDING_OBSERVABILITY` | A3；不能比较 Actor Record P50/P95/P99 |
| `commit_bytes` | `PENDING_OBSERVABILITY` | A3；不能量化 Redis request/response 写放大 |
| `open_arrival_lag_ms` for ws-stress | `PENDING_OBSERVABILITY` | Q1；连接 Bot 没有逐到达 lag |
| `connection_writer_queue_depth` | `PENDING_OBSERVABILITY` | Gate；当前只有拒绝/踢除等侧面计数 |
| `redis_session_route_qps` | `PENDING_OBSERVABILITY` | Gate/A4；Redis 聚合 commandstats 不能精确归因 |
| `redis_subscription_lease_qps` | `PENDING_OBSERVABILITY` | A4；Lease counter 有，精确 Redis QPS 待补 |
| Mixed operation set | `PARTIAL` | 当前开放到达支持 Login/Join/Bootstrap/Buy/Plant |
| `safe_connections_per_gate` | `NOT_FROZEN` | 尚未完成正式阶梯 |
| `supports_30m_dau` | `PROHIBITED_CLAIM` | 本轮禁止得出该结论 |
| Playwright witness | `MISSING_OR_FAILED` | 选定正式运行未 PASS |

必须继续保留：

```text
actor_record_bytes = PENDING_OBSERVABILITY
commit_bytes       = PENDING_OBSERVABILITY
```

不得填猜测值。

---

## 16. 写集审计

Q1 实际提交只涉及：

```text
cmd/perf-baseline/**
tools/perf/**
scripts/perf/**
deploy/load/**
docs/verification/perf/**
```

相对 Backend 基线检查以下路径无任何变化：

```text
internal/**
api/proto/**
internal/gateway/**
internal/gatewayapp/**
internal/player/**
internal/farm/**
internal/workflow/**
internal/storage/authoritystore/**
```

H5 Git HEAD 保持精确 Commit，业务源码无修改。

---

## 17. Q1 Commit 序列

```text
e629780af0e3b70ac949d85902ad844fe0c3f071
  test(perf): add reproducible Q1 baseline harness

2d6be5be56e776f1f624792fd9b441cb1990207d
  test(perf): isolate product anchor orchestration

63a74786fb6c1eb836a7a3e89e6da4d398cece3e
  test(perf): gate product witness on dependency readiness

b86d4f9246403ce18896c78d2fbaaf1775d1097a
  docs(perf): report correctness-stop boundary

0323cd0c75cc9787d4203d668133882d9a392081
  test(perf): preserve measured window on report replay

b5b34fbeed0dc1e956b854798a4e62f2ad31fa57
  docs(perf): retain Q1 baseline correctness stop
```

选定运行使用的 Load Tool Commit 是 `63a7478...`。后续三个 Commit只改 Reporter 重放边界和保留选定证据，不改变该次实际施压代码。

---

## 18. 已执行验证

最终执行并通过：

```bash
python3 -m unittest -v tools/perf/test_q1perf.py

go test \
  ./cmd/perf-baseline \
  ./cmd/ws-stress \
  ./cmd/authority-smoke-bot

bash -n \
  scripts/perf/run-baseline \
  scripts/perf/lib-stack.sh \
  scripts/perf/run-product-anchor-profile

python3 -m json.tool deploy/load/*.json
git diff --check
```

Python 单测覆盖：

- Redis endpoint hash 不包含 credentials；
- Secret/credential URI 脱敏；
- planned/actual 记录；
- `BASELINE_RECORDED` 正路径；
- `FAIL_CORRECTNESS` 优先；
- 缺失 Scenario 为 Inconclusive；
- 不产生 30M 支持声明。

最终清理检查：

```text
Q1 Redis container leak = 0
Q1 process leak         = 0
Backend worktree clean
H5 worktree clean
```

---

## 19. 如何重跑同一 Baseline

在 F3 阻断被归属 Owner 解释或修复后，仍应使用同一 B0、H5 和 First Run Profile：

```bash
cd farm-game-q1-perf-baseline-v1

scripts/perf/run-baseline \
  --config deploy/load/q1-perf-baseline-v1-first-run.json \
  --backend-root "$PWD" \
  --backend-commit 260babcd93cfb6a4a58a481cc3c7f8c1f95e455a \
  --h5-root ../classic-farm-h5-q1-perf-baseline-v1 \
  --h5-commit df379b5fcc5eb710fc1efa5406c752ba18c3ab30 \
  --run-kind baseline \
  --profiles product,hot,connections,mix
```

只有得到：

```text
BASELINE_RECORDED
```

才可以把新的 `baseline-metrics.json` 用于 A1/A3/A4 或 R1 候选前后对比。

当前 `baseline-v1-20260809/baseline-metrics.json` 是正确性停止证据，不是有效性能基准。

---

## 20. 如何重放候选

工具已支持候选 worktree：

```bash
scripts/perf/run-baseline \
  --run-kind candidate \
  --backend-root /path/to/candidate-worktree \
  --backend-commit <candidate-exact-commit> \
  --h5-root ../classic-farm-h5-q1-perf-baseline-v1 \
  --h5-commit df379b5fcc5eb710fc1efa5406c752ba18c3ab30 \
  --config deploy/load/q1-perf-baseline-v1-first-run.json \
  --baseline-metrics /path/to/a-valid-baseline-metrics.json
```

约束：

- Candidate 必须基于同一 B0 构造；
- H5、Load Tool、Profile、机器和 Redis 模式应保持一致；
- 报告必须核对 config hash 和 machine spec hash；
- actual load 未达到 plan 时不能判 `PERF_REGRESSION_PASS`；
- 正确性失败优先于性能结果；
- 不得使用当前失败运行的空 metrics 作为有效 baseline。

---

## 21. 下一步建议与责任边界

### P0：先关闭正确性硬门

建议由对应生产 Owner/Fault Harness Owner 只读分析：

1. 为什么 H5 点击 Theft 后，已 arm 的 Fault Proxy 没有观察到 `FAIL_FARM_CHANGE`；
2. Worker 的两个 Theft 候选为什么持续为 `InvalidOrders`；
3. `target binding unavailable: not found` 是测试前置、路由绑定、Order 内容还是生产恢复路径问题；
4. F3 应判生产缺陷，还是 Fault Controller 未命中导致的环境 Inconclusive；
5. Browser、Worker DueIndex 和 Proxy plan/event 的 operation/cycle/target 是否指向同一 Theft。

重点原始证据：

```text
evidence/product-correctness-anchor/playwright.log
evidence/product-correctness-anchor/browser-verdict.json
evidence/product-correctness-anchor/protocol-events.ndjson
evidence/product-correctness-anchor/worker.log
evidence/product-correctness-anchor/farm.log
evidence/product-correctness-anchor/gate.log
evidence/product-correctness-anchor/authority-after.json
```

### Q1 不应做的事

Q1 不应为让基线变绿而修改：

```text
Theft Handler/Coordinator
Worker 生产恢复语义
Farm/Player Actor
AuthorityStore/Lua
Gate/Fault 生产语义
Proto
H5 业务代码
Route Runtime
```

生产修复应由对应单写者完成；Q1 只在固定 Profile 下重放并报告。

### P1：正确性通过后

依次执行：

1. First Run 完整 Profile；
2. 确认 `BASELINE_RECORDED`；
3. 归档 raw evidence；
4. 再运行 Full 连接阶梯；
5. 对 A1/A3/A4 指定候选 Commit 重放；
6. 第一批正式合并后，在精确 R1 Head 重放；
7. 只有 B0 与 R1 都使用同 Profile、同机器、同 H5/Load Tool，才做前后比较。

---

## 22. 不得误读的结论

本交付证明：

```text
Q1 工具可以重复编排真实协议 Bot、真实 H5、Fault、Authority 和 Evidence；
Commit/配置/机器/Redis/计划与实际负载可以进入统一报告；
正确性失败会阻止压力数字被包装成性能成功；
Redis 隔离和清理可验证；
候选重放入口已具备。
```

本交付没有证明：

```text
第一批改造前性能基线已经成功冻结；
Gate 安全连接数；
200/500/1000/2000/5000 阶梯已跑完；
200-WSS 在本精确 B0 已重跑通过；
混合业务吞吐 SLO；
3000 万 DAU；
300 万 PCU；
生产 Pod/Redis/Shard 数量。
```

---

## 23. Push / Merge / Checkpoint 状态

```text
push integration：未执行
merge integration：未执行
修改移动 Tag：未执行
创建成功型 CP-PERF-BASELINE-V1 Tag：未执行
```

没有创建 `CP-PERF-BASELINE-V1` 成功型 checkpoint 的原因是选定运行不是 `BASELINE_RECORDED`。在正确性硬门关闭前创建成功型 Tag 会误导后续候选比较。
