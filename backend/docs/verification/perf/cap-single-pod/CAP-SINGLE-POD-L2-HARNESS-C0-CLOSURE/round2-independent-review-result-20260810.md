# CAP-SINGLE-POD-L2 Harness Round 2 Independent Review Result

日期：2026-08-10

## Verdict

```text
REVIEWED_HEAD=eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb
P0=0
P1=1
P2=0
EXACT_HEAD_CLOSURE=FAIL
BINDING_TRUST_ROOT=PASS
EVIDENCE_FAIL_CLOSED=PASS
SECRET_RAW_ID_GUARD=PASS
ORACLE_RUNTIME_STOP=PASS
CORRECTNESS_LATCH=PASS
SCRAPE_GAP=PASS
LOAD_GENERATOR_QUALIFICATION=FAIL
FORMAL_IDENTITY=PASS
FULL_TEST_VET_BUILD=PASS
READY_FOR_R2_BINDING=false
OBSERVABILITY_QUALIFICATION=NOT_RUN
LOAD_GENERATOR_RUNTIME_QUALIFICATION=NOT_RUN
SMOKE=NOT_RUN
BASELINE_ELIGIBLE=false
CAPACITY_CLAIM=NONE
SUPPORTS_30M_DAU=false
```

结果：`REQUIRED_FINDINGS`。不得激活 R2 Binding。

## P1-HR2-1：Driver qualification 可在没有形成 60-sample slope observation 时 PASS

### 影响

`LOAD_GENERATOR_QUALIFICATION` 是 Smoke 及后续阶段的可信前置。如果该阶段在不足 60 个 runtime samples 时 PASS，以下三类 Driver slope 可能全部因为“尚无可计算窗口”而显示为零，而不是因为实际观测证明稳定：

```text
arrival_lag_positive_slope_windows_max
driver_event_backlog_positive_slope_windows_max
driver_socket_error_positive_slope_windows_max
```

这种 Evidence 可以被 `verifyLoadGeneratorQualification` 接受，并解锁 Smoke/Warmup/Staircase/Candidate/Formal。Driver 先满时可能被错误归因到服务容量，因此为 P1。

### 精确代码事实

1. `validatePhaseDuration` 没有为 `LOAD_GENERATOR_QUALIFICATION` 设置最小持续时间。
2. `evaluateRun` 对该阶段没有要求 `queueSlope(samples).Samples` 达到一个完整 60-sample observation window。
3. `verifyLoadGeneratorQualification` 只检查三项 `*_windows_max == 0`，没有检查 qualification 的 runtime sample/window 数。
4. 当前 `newQualificationFixture` 的成功路径中 `Result.QueueSlope.Samples` 为 Go 零值 `0`，但 `TestHeadroomFactorComputedNotSelfReported` 仍能获得成功的 qualification summary。这是现有测试内的直接反例。

### Required fix

至少同时满足：

- `LOAD_GENERATOR_QUALIFICATION` 计划时长不小于 60 秒；
- 该阶段不能在少于 60 个成功 runtime samples 时输出 `PASS_LOAD_GENERATOR_QUALIFICATION`；
- prior qualification verifier 必须拒绝 `QueueSlope.Samples < 60`，不能仅信任三项零值 slope counters；
- 新增不足 60 samples/过短 duration 的确定性拒绝测试；
- 保留 actual/planned、Driver CPU、lag P99、三项 slope、Evidence Index/Hash、时间顺序、identity 和逐 target ≥1.5× 的原有检查。

## Reviewed closure items

### Binding

- R1 Active Binding 命中编译期固定 SHA 与固定 basename；
- forged Active R1/R2 均被测试拒绝；
- R2 不在 trust root 中，外部 Binding/Run Plan 无法激活；
- Binding Evidence 只记录 basename 和 digest，不记录绝对路径。

### Observability / Oracle / Correctness

- Required Metrics 在 Preflight 和 runtime 使用 exact set；
- source failure、missing/extra metric、counter reset fail closed；
- Oracle incomplete/error/reset/missing/extra/negative value 立即停止新 arrivals；
- operation correctness 未知/负值变为固定 protocol violation；
- Gate 值使用 latched max，后续零值不能清除事故；
- Snapshot 1 秒采样下允许 5 次 transient failure，第 6 次即 `>5s` 停止。

### Evidence / Secret

- resource marshal/short-write/I/O/close failure 锁存 observability failure；
- incomplete Index 不能进入 Formal aggregator；
- Redis/HTTP raw URL、userinfo/query/fragment、Secret、raw ID 使用固定拒绝策略；
- 未知 metric/gate 不回显原始名称到 Evidence。

### Formal

- 非 Aggregate 路径不能输出 `PASS_SAFE_TIER`；
- Candidate 与 Formal×3 decision 被区分；
- Result/Manifest/correctness/environment/adapter identities 做交叉校验；
- business、server config、route、lease、Redis config、image、cgroup、fixture、Driver qualification 和 adapter drift 阻止聚合 PASS；
-输出继续固定 `publish_authorized=false`、`baseline_eligible=false`、`capacity_claim=NONE`、`supports_30m_dau=false`。

## Independent deterministic validation

在 exact `eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb` 执行：

```text
GOFLAGS=-buildvcs=false go test -race ./cmd/cap-single-pod -count=20 PASS
python3 scripts/perf/cap-single-pod/test_harness_tools.py              PASS 3/3
GOFLAGS=-buildvcs=false go test ./... -count=1                         PASS
GOFLAGS=-buildvcs=false go vet ./...                                  PASS
GOFLAGS=-buildvcs=false go build ./cmd/...                             PASS
git diff --check                                                       PASS
Train-A scope from 5ede235...                                          PASS
```

测试通过不覆盖 P1-HR2-1；该 finding 是成功 fixture 明确允许 `QueueSlope.Samples=0` 的语义缺口。

## Scope

`5ede235... → eabb83c...` 的 46 个 changed files 全部位于：

```text
cmd/cap-single-pod/**
scripts/perf/cap-single-pod/**
deploy/load/cap-single-pod-*
docs/verification/perf/cap-single-pod/**
```

没有产品代码变化，没有 Smoke/Warmup/Staircase/Formal 压力 Evidence。
