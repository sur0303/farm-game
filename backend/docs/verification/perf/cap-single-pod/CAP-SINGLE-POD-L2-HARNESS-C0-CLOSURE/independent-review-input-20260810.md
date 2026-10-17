# CAP-SINGLE-POD-L2-HARNESS-C0 Independent Review Input

Review target：最终 `HARNESS_CANDIDATE_HEAD`（由交付消息提供）

Core implementation：`b17f1b8bdf89cd1b92ee06c245cb6fb24a1bccf0`

Exact adapter identity amendment：`af1feae5bb471aba7385dbb7b69428c18c66b6ce`

Review 类型：Harness/测试工具独立 Review，不是性能 Evidence Review。

## 1. 必须确认的边界

- [ ] Diff 只进入授权 test-only 路径；
- [ ] 没有产品 Handler、Actor、Workflow、Redis Lua、Friendship、Route Runtime 或 H5 改动；
- [ ] R1 frozen identity 六项精确不变；
- [ ] R2 binding 默认 blocked，不能在没有 `R2_FINAL_CANDIDATE_HEAD` 时执行；
- [ ] 没有 Smoke、Staircase、Formal 或正式压力 Evidence；
- [ ] `baseline_eligible=false`、`capacity_claim=NONE`、`supports_30m_dau=false` 未改变。

## 2. Generic Core

### Scheduler

- [ ] monotonic open-loop，不按服务响应反馈降速；
- [ ] >50ms arrival 记 unsent，不 catch up；
- [ ] planned/launched/unsent/completed/rejected/timeout/failed 独立；
- [ ] bounded inflight 和 bounded histogram；
- [ ] correctness signal 能停止新 arrivals。

### Collector

- [ ] Preflight 和每个 runtime sample 都检查 required metric 全集；
- [ ] Harness-owned counters 不能被外部 Snapshot 伪造覆盖；
- [ ] counter reset fail-closed；
- [ ] Prometheus mapping、cgroup v2、procfs FD、identity/cgroup drift 是真实读取；
- [ ] measured service、Driver、Observer、Dependencies 分开；
- [ ] endpoint/listener/cgroup root 约束不能逃逸；
- [ ] Collector/Oracle `/health` 的 binary/config SHA 必须由 Run Preflight 精确复核；
- [ ] adapter identity 只以 Hash 进入 Evidence，不泄漏 raw endpoint/config path；
- [ ] scrape error 不泄漏 URL 或 Secret。

### Oracle

- [ ] 15 gates 在 Preflight/runtime 都要求完整；
- [ ] COUNTER_DELTA 在启动时 arm baseline；
- [ ] GAUGE_CURRENT 不错误做累加；
- [ ] reset、missing、invalid value、source error 都不可能 PASS；
- [ ] 非零 correctness 的 precedence 最高。

### Verdict

- [ ] Smoke/Warmup/Qualification/Staircase/Candidate/单轮 Formal 都不能输出 `PASS_SAFE_TIER`；
- [ ] 只有 candidate + Formal×3 聚合器可以输出 `PASS_SAFE_TIER`；
- [ ] 聚合器验证每个输入 Evidence Hash Index；
- [ ] identity/profile/tier/fixture 和跨轮 CV 门不能跳过；
- [ ] 聚合 PASS 仍要求 Independent Review，且不形成全局容量 Claim。

### Stop Conditions

- [ ] Stop/Verdict/Environment 文件不仅被 Hash，也被核心验证 schema/precedence/threshold；
- [ ] contract 漂移无法静默沿用旧常量；
- [ ] correctness、environment、observability、driver、dependency、target precedence 精确；
- [ ] sustained 和 immediate 条件没有互相冒充。

### Evidence/Schema

- [ ] Run Plan 与 Manifest 都先经过 Draft 2020-12 runner；
- [ ] Manifest Schema SHA 被 Active Binding 固定；
- [ ] exact binary SHA、contract hashes 和 image digest 被 Go Preflight 复核；
- [ ] actual start/end 在 Hash Index 前落盘；
- [ ] raw endpoint、Secret、原始 Operation ID 不进入 Evidence；
- [ ] 旧输出目录不能覆盖。

## 3. 建议重点攻击的负控

1. 删除一个 runtime metric，但把 `missing_metrics` 留空；
2. 删除一个 correctness gate，但保持 `observation_complete=true`；
3. Oracle counter 在运行中重置；
4. 把 R2 blocked binding 传给 Run Plan；
5. 修改 R1 binding 的 Manifest Schema path/hash；
6. 给 Smoke 构造全部健康指标，检查是否错误得到 `PASS_SAFE_TIER`；
7. 修改 Formal 2 tier/fixture/profile 或输入 Evidence attachment；
8. 让 Driver CPU/arrival lag/actual ratio 持续越界；
9. 让 Dependency 先满；
10. 把 collector source 改成非 loopback、把 cgroup path 逃逸 approved root；
11. 给 JSON 增加 `password` 或未知字段，检查错误输出是否回显 value；
12. 重用已有 Evidence 输出目录。

## 4. Reviewer 必须返回

```text
REVIEWED_HEAD=<sha>
P0=<n>
P1=<n>
P2=<n>
TRAIN_A_SCOPE=PASS|FAIL
R1_IDENTITY_FREEZE=PASS|FAIL
R2_BINDING_ISOLATION=PASS|FAIL
REQUIRED_METRICS_MAINLINE=PASS|FAIL
ORACLE_COMPLETENESS=PASS|FAIL
PHASE_VERDICT=PASS|FAIL
FORMAL_AGGREGATION=PASS|FAIL
SCHEMA_AND_EVIDENCE=PASS|FAIL
COLLECTOR_ORACLE_ADAPTERS=PASS|FAIL
FULL_TEST_VET_BUILD=PASS|FAIL
HARNESS_C0=PASS|BLOCKED
READY_FOR_R2_BINDING=YES|NO
OBSERVABILITY_QUALIFICATION=NOT_RUN
LOAD_GENERATOR_QUALIFICATION=NOT_RUN
SMOKE=NOT_RUN
BASELINE_ELIGIBLE=false
CAPACITY_CLAIM=NONE
SUPPORTS_30M_DAU=false
```

任何 P0/P1 阻断 Harness C0。Reviewer 不得在候选分支原地修复 Evidence 或把未运行资格项改成 PASS。
