# CAP-SINGLE-POD-L2 Harness Round 2 Independent Review Input

Review target：最终 `HARNESS_FIX_HEAD`（由交付消息提供）

修复实现 Parent：`02ebb2e5f0b9f81a5ab0a2c0f4d0184bda105f07`

原 Review：`ba934b9d126180a3f8e8c94f5000886087a76b91` → `REQUIRED_FINDINGS`

Review 方式：只读，不在候选分支修复。

## 必查项

- [ ] Active R1 Binding 必须命中编译期 SHA trust root；伪造 Active R1/R2 均拒绝。
- [ ] R2 没有受信 digest 时无法通过 Run Plan 或外部 Binding 激活。
- [ ] Binding Evidence 不含绝对路径。
- [ ] Required Metrics 在 Preflight 和每次 Runtime Snapshot 都使用 exact set。
- [ ] Counter reset、Evidence write error、unknown metric 立即 fail-closed。
- [ ] Oracle incomplete/error/reset/missing/extra/invalid value 立即停止新 arrivals。
- [ ] Operation correctness 使用 latched max，未知/负值不可能抵消。
- [ ] Snapshot transient gap 恢复与 `>5s` stop 边界精确。
- [ ] Evidence write failure 不可能生成 complete index 或 PASS phase result。
- [ ] Redis/HTTP raw URL、query/fragment、Secret、raw Operation/Session/Receipt/Workflow ID 不能进入 Evidence。
- [ ] 三类 Driver slope 在运行中自动停止，不只在阶段结束检查。
- [ ] Qualification Evidence 附件和 Hash 被实际读取；headroom 由所有 target 计算而不是自报。
- [ ] Qualification run 必须早于 Smoke，Driver/Profile/Binding/Service identity 一致。
- [ ] 非 Formal Aggregate 路径不能输出 `PASS_SAFE_TIER`。
- [ ] Candidate + Formal×3 Result/Manifest/Attachments 和全部配置身份一致。
- [ ] 任一 server/route/lease/Redis/image/business/adapter drift 阻止聚合 PASS。
- [ ] `baseline_eligible=false`、`capacity_claim=NONE`、`supports_30m_dau=false` 保持。
- [ ] Diff 仍严格位于 Train-A/test-only 写集；没有产品改动和正式压力 Evidence。

## Reviewer 输出

```text
REVIEWED_HEAD=<HARNESS_FIX_HEAD>
P0=<n>
P1=<n>
P2=<n>
EXACT_HEAD_CLOSURE=PASS|FAIL
BINDING_TRUST_ROOT=PASS|FAIL
EVIDENCE_FAIL_CLOSED=PASS|FAIL
SECRET_RAW_ID_GUARD=PASS|FAIL
ORACLE_RUNTIME_STOP=PASS|FAIL
CORRECTNESS_LATCH=PASS|FAIL
SCRAPE_GAP=PASS|FAIL
LOAD_GENERATOR_QUALIFICATION=PASS|FAIL
FORMAL_IDENTITY=PASS|FAIL
FULL_TEST_VET_BUILD=PASS|FAIL
READY_FOR_R2_BINDING=true|false
OBSERVABILITY_QUALIFICATION=NOT_RUN
LOAD_GENERATOR_RUNTIME_QUALIFICATION=NOT_RUN
SMOKE=NOT_RUN
BASELINE_ELIGIBLE=false
CAPACITY_CLAIM=NONE
SUPPORTS_30M_DAU=false
```

只有 `P0=0/P1=0/P2=0` 且 `EXACT_HEAD_CLOSURE=PASS` 才能返回 `READY_FOR_R2_BINDING=true`。
