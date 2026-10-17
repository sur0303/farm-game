# CAP-SINGLE-POD-L2 Harness Review Findings Closure

日期：2026-08-10

原只读 Review Head：`ba934b9d126180a3f8e8c94f5000886087a76b91`

修复实现 Head：`02ebb2e5f0b9f81a5ab0a2c0f4d0184bda105f07`

修复实现 Binary SHA-256：`8ab45d58184f442461ce5bd58b461dba155d7b5f1053b2307ebae075cd49d90b`

状态：`READY_FOR_REVIEW_ROUND_2`

> 本文仅声明原 Review Findings 已有确定性修复和测试覆盖，不代替 Round 2 独立 Reviewer 的 PASS。

## Finding Closure

### Binding Trust Root

- R1 Active Binding SHA-256 编译期固定为 `e2c9b487f1077fbacd6ed08af8b315a11a37d3bed13ad1fb6c5d738c9bf8bfe5`。
- Active Binding 同时要求受信 digest 和固定 basename。
- R2 不在受信 Active Binding 表中；外部伪造 `ACTIVE R2` 继续拒绝。
- 普通 Run Plan 没有 `expected_binding_sha256` 覆盖字段。
- Evidence 只记录 basename、expected/actual SHA、binding ID/status，不记录绝对路径。

### Evidence Write Failure

- Resource writer 通过 test-only `evidenceSink` 注入。
- Marshal、short write、ENOSPC/I/O、close error 均锁存 `evidence_write_error_total`。
- 锁存后立即停止新 arrivals，最终 verdict 为 `INCONCLUSIVE_OBSERVABILITY`。
- Hash Index 写入 `complete=false` 和固定 `EVIDENCE_WRITE_ERROR`；Formal Aggregator 拒绝 incomplete index。
- 写盘继续失败时保留已有部分文件且不打印 PASS。

### Secret / Raw Identity

- Evidence 拒绝所有 `redis://`、`rediss://`、HTTP(S) raw endpoint、Redis URL、password/secret、Operation/Session/Receipt/Workflow/Work ID、Ticket、Authorization、Cookie 和 raw RESP 字段。
- Loopback URL 禁止 userinfo、query、fragment 和 opaque URI。
- Runtime Metrics 与 Oracle Gates 使用 exact allowlist；extra set 只留下固定错误码，不回显未知名称。

### Oracle / Correctness Stop

- Oracle incomplete、source error、counter reset、Gate set mismatch、未知/非法值均锁存 observability stop 并立即取消新 arrivals。
- 已发请求使用独立 bounded request context，在 Driver request timeout 内有限回收，不被 arrival cancellation 直接丢弃。
- Operation correctness 只接受 required Gate 和非负值，使用 latched max；后续零值不能清除事故。
- 未知 Gate/负值锁存 `harness_correctness_protocol_violation`，最终保持 `FAIL_CORRECTNESS`。

### Snapshot Scrape Gap

- HTTP/JSON fetch failure进入 transient gap，不立即伪造成 Missing Metric。
- 5 个一秒失败样本内恢复可继续；第 6 个样本（`>5s`）停止为 `INCONCLUSIVE_OBSERVABILITY`。
- Adapter 明确报告 required metric 缺失仍立即停止，不使用 transient gap 掩盖配置缺口。
- Oracle 观测采用更严格策略：incomplete 立即停止。

### Load Generator Qualification

- 实现 arrival lag、Driver event backlog、Driver socket error 三类 60-sample OLS positive-slope windows，连续 3 个窗口立即 `INCONCLUSIVE_LOAD_GENERATOR`。
- 同时闭合 Redis pool wait、support queue 和 target P99 nonlinear stop conditions。
- Post-qualification phase 必须绑定一个完整 prior qualification Evidence 目录。
- 重新校验 Evidence Index、附件 Hash、Index SHA、Driver SHA、Profile SHA、Binding、Service/Profile、时间顺序、actual/planned、Driver CPU、lag P99 和三类 slope。
- 对每个 requested target 计算 `qualified/requested`，最小实际 headroom 必须 ≥1.5；不信任 Manifest 自报 factor。
- Evidence 只记录 qualification run ID、Evidence SHA、qualified/requested tier 和实际 headroom，不记录绝对路径。

### Formal Identity

- Candidate 必须为 `PASS_SAFE_TIER_CANDIDATE`；三轮必须为 `PASS_FORMAL_ROUND`。
- Result 与自身 Manifest 交叉验证 run/service/profile/phase/tier/targets。
- correctness/environment attachments 必须完整 PASS 并与 Result adapter identity 一致。
- 四轮匹配 Frozen Business/Verification/H5 identity、Harness identity、Binding SHA、Profile/Contract/Fixture、Machine class、Driver qualification、server config、route snapshot、lease profile、Redis profile/config、Backend/H5/Redis/support image digests、cgroup limits、Collector/Oracle config identity。
- Queue slope、Candidate ratio、Formal ratio和三项跨轮 CV 继续 fail-closed。

## Deterministic Tests

新增/强化测试覆盖：

```text
Binding: forged Active R2/R1, Run Plan override, checked-in digest
Oracle: incomplete immediate stop, exact Gate set, invalid value
Correctness: unknown Gate, negative value, latched violation
Evidence: marshal, short write, closed-file I/O, incomplete index
Secret: passwordless Redis URL, rediss, raw Operation ID, endpoint query/fragment
Scrape: under-five recovery, over-five stop, real fetch failure
Loadgen: arrival lag/event backlog/socket error slopes, qualification hash/time/headroom/identity
Formal: wrong Candidate/Round decision, Result/Manifest mismatch, server/route/lease/Redis/image/business/adapter drift
```

## Exact Validation

在实现 Head 上执行：

```text
GOFLAGS=-buildvcs=false go test -race ./cmd/cap-single-pod -count=1   PASS
GOFLAGS=-buildvcs=false go test -race ./cmd/cap-single-pod -count=20  PASS
python3 scripts/perf/cap-single-pod/test_harness_tools.py              PASS 3/3
GOFLAGS=-buildvcs=false go test ./... -count=1                         PASS
GOFLAGS=-buildvcs=false go vet ./...                                  PASS
GOFLAGS=-buildvcs=false go build ./cmd/...                             PASS
git diff --check                                                       PASS
```

没有运行 Smoke、Warmup、Staircase、Formal 或任何正式压力；没有修改产品代码；R2 Binding 仍未激活。
