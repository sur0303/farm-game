# Harness Round 2 P1 Focused Review Result

日期：2026-08-10

## Verdict

```text
ORIGINAL_ROUND2_REVIEWED_HEAD=eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb
ORIGINAL_ROUND2_REVIEW_RESULT_HEAD=d66a8bf290c0927b8a8bdf4fe6ba7e8be0ac3137
ORIGINAL_ROUND2_RESULT=P0_0_P1_1_P2_0
P1_FIX_IMPLEMENTATION_HEAD=73da76b938fb0cea6e95940f3ea540dc3c5306c0
REVIEWED_HEAD=8f3a22e2ca355aa89096334a41abb188787e220f
HARNESS_BINARY_SHA256=7d1fce85aa4b462db80158ddb04ce1a3a73111937dc9f51e027ce115acee5fcb
P0=0
P1=0
P2=0
EXACT_HEAD_CLOSURE=PASS
GENERIC_HARNESS_REVIEW=PASS
BINDING_TRUST_ROOT=PASS
ORACLE_RUNTIME_STOP=PASS
EVIDENCE_FAIL_CLOSED=PASS
DRIVER_THREE_SLOPES=PASS
LOAD_GENERATOR_1_5X_QUALIFICATION=PASS
FORMAL_IDENTITY=PASS
SECRET_RAW_ID_GUARD=PASS
CORRECTNESS_LATCH=PASS
SCRAPE_GAP=PASS
FULL_TEST_VET_BUILD=PASS
READY_FOR_CONTROLLER_BINDING=true
OBSERVABILITY_QUALIFICATION=NOT_RUN
LOAD_GENERATOR_RUNTIME_QUALIFICATION=NOT_RUN
SMOKE=NOT_RUN
BASELINE_ELIGIBLE=false
CAPACITY_CLAIM=NONE
SUPPORTS_30M_DAU=false
```

`8f3a22e...` 是在原 exact `eabb83c...` Review 后只闭合 `P1-HR2-1` 的新候选；不能把原不可变 `eabb83c...` 的结果改写为 0/0/0。最终 Generic Harness PASS 绑定新候选及其相同可复现 binary。

## P1-HR2-1 closure

确认：

- `LOAD_GENERATOR_QUALIFICATION` duration `<60s` 在计划校验阶段拒绝；
- runtime 成功 samples `<60` 时为 `INCONCLUSIVE_OBSERVABILITY`；
- prior qualification Evidence 的 `Duration <60` 或 `QueueSlope.Samples <60` 均拒绝；
- 健康 60-sample qualification 可继续；
- arrival lag、Driver event backlog、socket error 三类 positive slope 的连续三窗口 runtime stop 保持；
- prior Evidence 仍重新读取 complete Index、Index SHA、phase result、时间顺序、Driver/Profile/Binding/Service/Frozen identity、actual/planned、Driver CPU、lag、三类 slope 和逐 target 实际 headroom；
- Manifest 自报 `headroom_factor_proven` 不能替代实际 `qualified/requested` 最小值 ≥1.5。

修复只涉及五个 `cmd/cap-single-pod` 文件及 test-only handoff/status，没有产品代码变化。

## Full focused matrix

### Binding trust root

R1 Active Binding 必须命中编译期 SHA 与固定 basename；forged Active R1/R2 和 Run Plan override 拒绝。R2 仍故意不在 trust root 中，当前 PASS 只允许 Controller 开始生成 R2 Binding，不代表 R2 已激活。

### Oracle / correctness / scrape

Oracle source error、incomplete、counter reset、missing/extra gate、negative value立即停止新 arrivals；Operation gate 使用 exact allowlist、non-negative protocol 和 latched max。Snapshot transient gap 在冻结 1 秒 sample interval 下第 1–5 次失败可恢复，第 6 次即 `>5s` stop。

### Evidence / secret

Resource marshal/short-write/I/O/close failure锁存 `INCONCLUSIVE_OBSERVABILITY`；incomplete Index 不能进入 Formal。Redis/HTTP raw URL、userinfo/query/fragment、Secret、raw Operation/Session/Receipt/Workflow/Work ID 和未知 metric/gate 回显均被拒绝或固定错误码化。

### Formal identity

非 Aggregate phase 不能输出 `PASS_SAFE_TIER`。Candidate 与 Formal×3 decision、Result/Manifest/Attachments、Frozen Business、Harness、Binding、Profile/Contract/Fixture、Machine、Driver qualification、server/route/lease、Redis config、image、cgroup 和 Collector/Oracle identity 全部一致后才可生成 Aggregate；Aggregate 仍固定 `publish_authorized=false`。

## Independent validation

在 clean exact `8f3a22e2ca355aa89096334a41abb188787e220f` 重跑：

```text
P1 focused adversarial tests                                         PASS count=20
GOFLAGS=-buildvcs=false go test -race ./cmd/cap-single-pod -count=20 PASS
python3 scripts/perf/cap-single-pod/test_harness_tools.py              PASS 3/3
GOFLAGS=-buildvcs=false go test ./... -count=1                         PASS
GOFLAGS=-buildvcs=false go vet ./...                                  PASS
GOFLAGS=-buildvcs=false go build ./cmd/...                             PASS
git diff --check                                                       PASS
Binary rebuild SHA256                                                  7d1fce85...
```

## Authorization boundary

本 Review 只返回：

```text
READY_FOR_CONTROLLER_BINDING=true
```

它不返回 R2 Active Binding，不运行 Static Staging/Observability/Load Generator Runtime Qualification/Smoke，也不授权 Local kind、手工试玩或容量声明。
