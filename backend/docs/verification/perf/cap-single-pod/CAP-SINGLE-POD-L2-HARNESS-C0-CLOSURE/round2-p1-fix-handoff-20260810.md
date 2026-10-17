# Harness Round 2 P1 Fix Handoff

日期：2026-08-10

## Identity

```text
ROUND2_REVIEWED_HEAD=eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb
ROUND2_REVIEW_RESULT_HEAD=d66a8bf290c0927b8a8bdf4fe6ba7e8be0ac3137
ROUND2_REVIEW_RESULT=P0_0_P1_1_P2_0
P1_FIX_IMPLEMENTATION_HEAD=73da76b938fb0cea6e95940f3ea540dc3c5306c0
P1_FIX_BINARY_SHA256=7d1fce85aa4b462db80158ddb04ce1a3a73111937dc9f51e027ce115acee5fcb
```

状态：`READY_FOR_NARROW_FOCUSED_REVIEW`，不是 R2 Binding 授权。

## Closed finding

`P1-HR2-1`：Load Generator qualification 原先可以在不足一个完整 60-sample slope observation 时，把三类 slope 尚未形成窗口产生的零值误当成稳定证据。

修复：

1. `LOAD_GENERATOR_QUALIFICATION` Run Plan duration 必须至少 60 秒；
2. runtime evaluation 在少于 60 个成功 samples 时返回 `INCONCLUSIVE_OBSERVABILITY`，不能输出 `PASS_LOAD_GENERATOR_QUALIFICATION`；
3. prior qualification Evidence verifier 同时要求 `Duration >= 60` 和 `QueueSlope.Samples >= 60`；
4. 保留三类 slope window max 必须为零、actual/planned ≥98%、lag P99 ≤50ms、Driver CPU ≤70%、完整 Evidence Index/Hash、时间/identity 和逐 target 实际 headroom ≥1.5×；
5. 新增计划过短、Evidence duration 过短、Evidence samples 不足和 runtime samples 不足的确定性拒绝测试。

`QueueSlope.Samples` 与三类 Driver slope 使用同一个成功 `collectionState.samples` 序列，因此 60 samples 是三类 OLS runtime observation 已实际形成的机械下界，不信任自报 headroom。

## Exact validation

在 clean exact implementation Head 执行：

```text
GOFLAGS=-buildvcs=false go test -race ./cmd/cap-single-pod -count=20 PASS
python3 scripts/perf/cap-single-pod/test_harness_tools.py              PASS 3/3
GOFLAGS=-buildvcs=false go test ./... -count=1                         PASS
GOFLAGS=-buildvcs=false go vet ./...                                  PASS
GOFLAGS=-buildvcs=false go build ./cmd/...                             PASS
git diff --check                                                       PASS
```

没有运行 Observability Qualification、Load Generator Runtime Qualification、Smoke、Warmup、Staircase 或 Formal；没有修改产品代码。

## Focused Reviewer target

Reviewer 必须精确审查最终 handoff Head，并确认：

```text
P0/P1/P2=0/0/0
EXACT_HEAD_CLOSURE=PASS
GENERIC_HARNESS_REVIEW=PASS
READY_FOR_CONTROLLER_BINDING=true
```

重点复核：不足 60 秒/60 samples 必须拒绝、60 samples 的健康 qualification 可继续、三类 positive slope 仍会在运行时停止、prior Evidence 的 duration/sample/window/Hash/headroom/identity 全部 fail closed。
