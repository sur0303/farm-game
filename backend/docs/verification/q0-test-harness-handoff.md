# Q0-TEST-HARNESS Handoff

- task_id: Q0-TEST-HARNESS
- branch: `codex/q0-test-harness-20260803`(独立 worktree,未新建分支,未合入 integration)
- base_commit: `6b78d2f5b7793afe8636d919280c85c7849d6200`
- 日期: 2026-08-03
- 状态: **COMPLETED**(验证记录见 §5,全部命令真实 exit 0)

## 1. 范围与纪律

目标:建立后续 Agent 共用的测试/故障注入骨架与合同追踪。**未修改任何业务实现**
(业务 Actor/Ingress/Proto/Redis 生产 Key·Lua·Adapter/Workflow 生产实现/cmd 生产入口/H5
均未触碰;`git diff` 仅含本 handoff 所列新增文件)。Mock/Fake 结果未冒充高等级证据,
本任务自身不申报任何 `CAPACITY_PASS`。

## 2. 交付物

### 2.1 internal/testkit(全部新建,含自测,确定性、无 sleep)

| 包 | 能力 | 关键 API |
|---|---|---|
| `testkit/clock` | 可控时钟,收敛三处重复 fakeClock;适配 `SetClock(func() time.Time)` 接缝 | `New/Now/NowFunc/Advance/Set/Deadline/Expired` |
| `testkit/fault` | RPC gate/延迟/断连/崩溃注入(任意一元接缝);blobstore 边界 timeout/response-lost(before/after apply,语义对齐 authoritystore.FaultStore) | `Injector`(FIFO 脚本,`HangEntered` 确定性握手,禁 sleep)/`Do[Req,Resp]`/`LogicClient` 适配器/`BlobStore`(BlobGet…BlobFence,Unavailable/TimeoutUnknown×2/Crash) |
| `testkit/slowconn` | 慢连接/有界队列工具,复用 transport.FakeTransport(唯一传输 Fake) | `FillOutgoing/FillIncoming/DrainOutgoing/DrainIncoming/SlowConsumer.Step` |
| `testkit/assert` | 五类复用断言,只观察公开表面(proto 状态/哨兵错误/lease 状态) | `SameOperationRetried/PayloadConflict/TimeoutUnknown(+Processing)/OldWriterFenced/StaleSubscriptionPush/StaleSubscriptionRenew` |

设计要点:
- `Hang` 用 `HangEntered(call)` channel 做"已进入阻塞点"握手——先登记 pending 再发信号,
  `ReleaseHang` 必然命中;测试不得用 sleep 猜时序(本任务曾因此死锁,已修复并回归)。
- `BlobStore` 的 BeforeApply/AfterApply 区分"未生效但不可知"与"已生效但响应丢失",
  与 doc 30 §30-05 的 Unknown 语义一一对应。
- 断言助手不依赖未导出类型,可在任意 Owner 的测试里直接使用。

### 2.2 loadtest(W5B 工具扩展接缝,含自测)

- `LeaseStressParams`/`WSStressParams`/`DrainScriptEnv`:类型化参数 → 既有二进制
  (`cmd/lease-stress`、`cmd/ws-stress`)命令行与既有脚本(`scripts/test-multigate-drain`)环境变量。
  **不是第二套 Harness**。
- preset:`BroadcastSlowConn`(200 订阅+慢连接背压)、`FullGameplay`、`Soak(2h/24h 档)`、`Drain`,
  每个携带 `EvidenceRule`(必需证据清单+判定标准)。未真实运行并采齐证据前,
  结论只能是 `INCONCLUSIVE`(doc 60 §2.3)。
- `LeaseStressParams.Validate` 钉死 199-viewer 合同上限(26 §HC-ACCESS-36)。

### 2.3 文档

- `q0-contract-test-matrix.md`:26~37 全部条款组 → 现有证据/等级/状态/缺口/Owner;
  含"现有工具→条款能力"重复能力清单(§0)与最高优先级缺口汇总(§12)。
- `q0-baseline-20260803.md`:base_commit 测试耗时(vet 1.834s / test 5.406s,44 ok/26 no-test/0 FAIL)、
  最慢包、12 条已注册 Command、Actor/Workflow/既有工具清单——后续 checkpoint 以此 diff。

## 3. ICR(需改生产代码的缺口,只记录不越权)

| ID | 缺口 | 精确接缝 | 建议 Owner |
|---|---|---|---|
| ICR-Q0-1 | 广播慢连接场景缺"per-conn 下行消费限速/慢连接比例"参数 | `cmd/ws-stress/main.go` flag 块与客户端读循环 | Q1/容量工具 Owner |
| ICR-Q0-2 | FakeTransport 不暴露队列长度,积压观测只能靠消费端计数或 Gate /metrics | `internal/gateway/transport/fake.go`(可加 `Len()` 只读方法,属测试 Fake 非生产逻辑) | 后续任一 Agent(如需) |
| ICR-Q0-3 | 29 §L2-82 统一 failpoint 枚举(BEFORE_PARTICIPANT_SEND…FORCE_SHARD_HANDOFF)无生产侧挂钩,当前注入分散在各测试 | workflow runner/participant 调用点 + authoritystore 适配层;testkit/fault.Injector 已提供挂接原语 | BIZ(workflow/存储 Owner) |

## 4. 后续 Owner 使用指引

- 时间相关测试:一律 `testkit/clock.New(固定时间)` + 生产 `SetClock` 接缝;逐步替换散落的
  私有 fakeClock(替换属各 Owner 写集)。
- RPC/存储故障:先查 `authoritystore.FaultStore`(authority 边界)与 `testkit/fault.BlobStore`
  (blobstore 边界);任意一元 RPC 用 `fault.Do` 包装;Hang 场景必须 `<-inj.HangEntered(call)`
  后再 `ReleaseHang`。
- 幂等/冲突/Unknown/Fence/旧订阅断言:直接用 `testkit/assert`,不要重写第四份判断逻辑。
- 容量场景:从 `loadtest` preset 出发生成命令;实跑后把原始输出+metrics 抓取存入
  `docs/verification/results/` 并标注证据等级;**禁止用 testkit(内存 Fake)结果申报
  REAL_REDIS_PASS/CAPACITY_PASS**。
- checkpoint diff:对照 `q0-baseline-20260803.md` 的耗时与 Command/Actor/Workflow 清单。

## 5. 验证记录(本任务提交点,全部真实 exit 0)

| 命令 | 结果 |
|---|---|
| `gofmt -l internal/testkit loadtest` | 无输出(干净) |
| `go test -count=1 -timeout=30s ./internal/testkit/...` | ok × 4(assert/clock/fault/slowconn),EXIT=0 |
| `go test -race -count=1 -timeout=60s ./internal/testkit/... ./loadtest/...` | ok × 5,EXIT=0 |
| `go test -count=1 -timeout=30s ./loadtest/...` | ok,EXIT=0 |
| `go test ./...` | PASS,exit 0(44+5 包 ok,0 FAIL) |
| `go vet ./...` | PASS,exit 0 |

基线(base_commit,不含本任务):`go vet ./...` 1.834s PASS;`go test ./...` 5.406s PASS。
测试执行纪律:全程 `set -o pipefail` 或直接读 exit code,不以管道末段退出码充当测试结果。

## 6. 证据等级自评

本任务交付物的自测证据等级为 `UNIT_PASS` + `RACE_PASS`(testkit/loadtest 自身)。
合同矩阵中所有等级判定沿用既有证据(W5B 实跑、stage5 故障证据、各包单测),
Q0 未新增任何业务证据,也未将任何既有证据升级。
