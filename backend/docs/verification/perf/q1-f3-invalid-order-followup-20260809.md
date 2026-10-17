# Q1 F3 / InvalidOrder 确定性诊断与二次交接（2026-08-09）

> 本文是 `q1-perf-baseline-v1-handoff-20260809.md` 的后续诊断，不覆盖首轮正式运行的历史事实。
>
> **最新结论：F3 本身已在同一 B0 上形成完整 PASS 证据链；当前真正阻断 Baseline 的是一个与 F3 无关的 Care Pair DueIndex / Theft Recovery Scope 产品正确性问题。性能基线仍未记录。**

---

## 1. 当前状态

```text
Q1-PERF-BASELINE-V1

tooling_status                = READY
f3_fault_chain                = PASS (1 次带最终分类器的确定性诊断)
full_browser_anchor           = PASS
independent_go_protocol_bot   = PASS
invalid_order_classification  = PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG
correctness_anchor            = BLOCKED_BY_PRODUCT_CORRECTNESS
performance_baseline          = NOT_RECORDED
capacity_claim                = NONE
supports_30m_dau              = false
```

这仍然不是性能基线，也没有产生连接容量、Redis QPS、P99 或 3000 万 DAU 结论。

---

## 2. 固定版本与诊断运行

### 2.1 被测业务版本

```text
Backend = 260babcd93cfb6a4a58a481cc3c7f8c1f95e455a
H5      = df379b5fcc5eb710fc1efa5406c752ba18c3ab30
```

### 2.2 最终选定诊断运行

```text
Run ID          = q1perf-f3diag3-0809122638-2569920
Q1/Load Tool    = 3b5af2eb0758c708edd3a4293093030be9076f94
Profile          = deploy/load/q1-perf-baseline-v1-f3-triage.json
Config Hash      = sha256:6f714f96b1a3969381791a5cc89182bdef924114ee6d5ab1277bcdd8e84296eb
Window           = 2026-08-09T12:26:39.093403Z -> 2026-08-09T12:27:53.137799Z
Redis Mode       = ephemeral_local
Redis Cleanup    = true
Redis Secret     = not recorded
FLUSHALL          = prohibited/not used
```

该 Profile 明确设置 `baseline_eligible=false`，只用于确定性诊断。

核心证据归档：

```text
docs/verification/perf/followup-f3diag3-20260809/
```

完整大体积原始证据仍在 ignored 目录：

```text
docs/verification/perf/results/q1perf-f3diag3-0809122638-2569920/
```

---

## 3. 对首轮 F3 失败的重新分类

首轮正式运行只知道：

```text
FAIL_FARM_CHANGE = 0
REJOIN_REQUIRED  = 0
browser wait     = timeout
```

当时不能证明 Fault 是否真正作用于目标 Theft。

后续加入 H5 Wire Tap 后，第一轮诊断 `q1perf-f3diag-0809121421-2454380` 证明：

1. Fault Plan 已被 Proxy 接收并可通过 `GET /admin/plan` 读回；
2. F3 之前的 Downlink 流量确实经过同一个 Proxy；
3. Fault Plan arm 后，H5 没有发出任何 Wire `THEFT` Command；
4. 因此 Authority、FarmChange、Proxy Matcher 和 REJOIN 链均没有进入。

该失败的确定分类是：

```text
classification  = CONTRACT_CONFLICT
contract_detail = H5_WIRE_THEFT_MISSING
```

它不是 `PRODUCT_FARM_CHANGE_MISSING`，也不是 `MATCHER_BUG`。

---

## 4. 为什么 H5 没有发出 Theft

这是一个测试契约滞后，不是 Fault Controller 随机失效。

固定 H5 业务代码的 `beginTheft()` 实现两次确认：

```text
第一次点击“偷菜”
-> 只设置 pendingTheftConfirmation
-> UI 文案变为“确认偷菜 #x”
-> 明确不发送请求

第二次点击
-> 才执行 commandService.execute('THEFT', ...)
```

对应业务代码位于：

```text
classic-farm-h5/src/app/App.ts
beginTheft()
```

但固定 Commit 内的 `cp-vertical-b.spec.ts` 仍按一次点击编排：

```text
点击目标地块
-> 点击一次 data-theft
-> 立即等待 FAIL_FARM_CHANGE
```

所以旧测试实际等待的是一个从未发送的命令。

### 4.1 修复方式

Q1 没有修改 H5 业务代码，也没有修改 H5 tracked 文件。

运行时在 H5 ignored 的 `test-results/` 下生成 test-only overlay，只做一处精确替换：

```text
第一次点击 data-theft
-> 断言按钮出现“确认偷菜”
-> 第二次点击 data-theft
-> 再等待 FAIL_FARM_CHANGE
```

Overlay 每次记录：

- 原始 Spec Hash；
- 生成 Spec Hash；
- 原始 Config Hash；
- 生成 Config Hash；
- 精确改动说明；
- `tracked_h5_modified=false`。

证据：

```text
followup-f3diag3-20260809/evidence/h5-test-overlay.json
```

这不是绕过 F3，而是让测试按固定 H5 已存在的真实产品确认语义发出命令。

---

## 5. 最终 F3 完整证据链

最终诊断运行的机器分类：

```text
classification = F3_CHAIN_PASS
baseline_state = CORRECTNESS_ANCHOR_PASS
```

### T0：Fault Plan 已 arm

```text
observed_at = 2026-08-09T12:27:25.756349Z
GET /admin/plan -> fail_farm_change_allow_rejoin=true
```

B0 Fault Controller 没有 `plan_id` 字段，因此当前以 UTC read-back 和目标 operation/cycle/subscription 组合完成绑定。

### T1：真实 H5 Wire Theft

```text
operation_id   = f8b1e82e-f0ab-4991-b32d-7f0170b38ac3
target_farm_id = 1000002
crop_cycle_id  = sid1_t1XVhIywRpXletGRjSi03pox2MLA5TJPkXqHHuxNDkY
command_type   = THEFT
```

该事件来自透明 WSS Tap；Tap 不记录 Connection Ticket、Cookie、密码或原始 Payload。

### T2：Authority Commit

只读 Redis Dump 找到同一 `operation_id` 的 durable Pair Dispatch Order 和 Target Binding。

```text
matching_order_count = 1
```

### T3/T4：FarmChange 生成并构造 Downlink Item

```text
unique_item_count = 1
item_id = sub-aa27def50e1a0afc737725c1c88c0673:change:5
```

### T5/T6：Proxy 命中并注入

```text
FAIL_FARM_CHANGE injection attempts = 4
```

这里的 `4` 是同一 Downlink Item 的重试/注入次数，不是四次业务提交。

### T8：REJOIN_REQUIRED

```text
target-bound REJOIN_REQUIRED = observed
reason = REJOIN_REASON_DELIVERY_FAILED
```

Proxy 原始事件中 owner/visitor 可各有 REJOIN；分类器只要求与目标 H5 Subscription 绑定的事件。

### T9/T10：Fresh Join 与权威事实收敛

Browser 完成：

```text
f3-farm-change-failure-rejoined
bFarmVersionAfterTheft = 5
bTheftPlotIndex        = 0
bStolenTotal           = 10
```

随后继续完成：

```text
task-list-visit-interact-growth-and-claims
return-home
final-authority-stable
```

Browser Verdict 和独立 Go Protocol Bot 均为 `PASS`。

因此，原 F3 阻断已经被确定为旧 E2E 编排与 H5 两次确认语义不一致，而不是 B0 的 F3 Recovery 产品缺陷。

---

## 6. InvalidOrders 的确定性分类

### 6.1 计数语义纠正

`InvalidOrders:2` 不是“已经证明存在两个独立业务事故”。

它是每轮 Worker Recovery 的处理计数。

最终诊断中：

```text
rounds_with_invalid_orders          = 11
invalid_processing_observations     = 22
max_invalid_orders_in_one_round     = 2
unique_due_candidate_count          = 2
```

只有在 Redis 销毁前完成只读 Dump 后，才确定确实存在两个唯一 DueIndex Candidate。

### 6.2 两个 Candidate 是什么

两个 Candidate 是同一 F1 Care Operation 的一对 Source Order：

```text
Care operation_id = 20b39d68-dd51-47de-97f8-ba08931c95dc
```

1. `FRIEND_ACTION_DISPATCH_ORDER`
   - state = PENDING
   - source_operation_id = 上述 Care Operation
   - visitor = 1000003
   - owner/target = 1000002
   - due_score = 0

2. `RESULT_PUBLISH_ORDER`
   - state = PENDING
   - dispatch_order_id 指向第一个 Order
   - due_score = 0

两者：

```text
created_at_ms = 1786278431671
first worker invalid observation = 2026-08-09T12:27:12Z
F3 plan armed                 = 2026-08-09T12:27:25.756349Z
```

它们明确早于 F3，且 Wire Tap 把 Source Operation 解析为 `CARE`，不是 `THEFT`。

### 6.3 为什么 Worker 报 target binding unavailable

只读代码审计显示：

- Theft Recovery 从通用 Pair DueIndex 发现 Candidate；
- `FRIEND_ACTION_DISPATCH_ORDER` 的 Work Type 对 Care/Theft 都相同；
- Theft decoder 在按 Action Scope 排除非 Theft 之前就查找 Theft Target Binding；
- Care 不应拥有 Theft Target Binding；
- Result Order 也被 Theft Result Processor 消费；
- 因而同一对 Care Order 每轮被重复计为 Invalid。

机器分类：

```text
PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG
```

这与 F3 Theft Operation 无关。最终 F3 Theft 自己的 Dispatch/Result Order 均已终态，且不在 DueIndex 中。

### 6.4 正确性影响

尽管 H5 的 F1 Care 用户可见流程已恢复并显示成功，这两个 Pair Order 仍保持 Pending/Due：

- Worker 持续错误轮询；
- DueIndex 残留；
- Recovery Scope 不正确；
- 不能把它当成仅日志噪声；
- 当前 B0 存在独立产品正确性问题。

因此最终诊断 Verdict 正确变为：

```text
verdict                         = FAIL_CORRECTNESS
hard_correctness_incident_count = 1
persistent_pair_due_residue     = 2
```

`2` 是同一产品 Scope Bug 下的两个关联 Source Order Signal；Incident Count 仍为 `1`。

---

## 7. 为什么没有继续跑性能阶段

虽然 F3 已 PASS，但独立的 Worker Recovery Scope 产品正确性问题触发硬门。

因此仍然不应继续：

- 200-WSS Theft baseline；
- 200-WSS slow-reader；
- connections-200/500；
- mix-low/medium/high。

不能为了获得性能数字忽略 Pending Pair DueIndex Residue。

---

## 8. B0 与当前 Integration 的祖先关系

已机械确认：

```text
Backend:
260babcd93cfb6a4a58a481cc3c7f8c1f95e455a
  is ancestor of
  dd7104a3c43cecdc85c8d7962014f9760bf28f2d

H5:
df379b5fcc5eb710fc1efa5406c752ba18c3ab30
  is ancestor of
  fdb9849c0aad613b9a1c45ab8bf33a692c5ee967
```

命令均返回 `0`。

这证明当前 Integration 从该 B0 向前演进，但后续仍要对每个独立 R1 Candidate 做同样检查。

---

## 9. Checkpoint Tag 状态

Q1 继续以精确 SHA 为真值，没有移动、强制覆盖或 push Tag。

诊断时观察到同名 Tag 当前为：

### Backend Repository

```text
tag_name        = cp-mvp-before-hardening-20260809
tag_object_sha  = 2c46f855b1eb2fd9ace596989e80b3c28ec0a140
target_commit   = 6d7ccc35a72a5dd5a95350c7c716dbca39bab0dd
```

### H5 Repository

```text
tag_name        = cp-mvp-before-hardening-20260809
tag_object_sha  = e39e6d7457867dd28e5b55f15fdb165cfede5819
target_commit   = 48991e6a9ca724941b54202603605948b169eed1
```

这些都不是本次 B0 精确 SHA，因此旧 Tag 应登记为 `INVALIDATED_MOVED_TAG`，不能用于机械比较。

新 annotated Tag 的创建和远端保护属于 Integration/Release Governance Owner；Q1 本轮未创建或推送替代 Tag。

---

## 10. 责任边界与下一步

### 10.1 产品 Owner：修复 Care/Theft Recovery Scope

建议 Owner 最小修复并验证：

1. Theft Recovery 在读取 Target Binding 前识别并排除非 Theft Action Scope；
2. Theft Result Processor 不消费非 Theft Result Order；
3. Care Recovery 的合法 Owner 能完成或清理自己的 Dispatch/Result Order；
4. 已成功 Care 后 Pair DueIndex 无遗留；
5. Worker 不再对 Care 记录 `target binding unavailable`；
6. 不通过删除 DueIndex、屏蔽日志或伪造 Target Binding 绕过。

Q1 不修改生产 Worker/Workflow/Friend/Authority 代码。

### 10.2 基线版本选择

由于 B0 已证明存在产品正确性问题，不应直接把错误 B0 建为性能基线。

推荐：

```text
B0
+ 最小 Care/Theft Recovery Scope 正确性修复
= B0.1-CORRECTNESS-FIX
```

然后确认所有第一批 Candidate/Integration 都包含同一修复，以 B0.1 建立可比较基线。

### 10.3 修复后的验证顺序

```text
1. Care lost-result + Pair DueIndex cleanup：10/10
2. F3 定向：10/10
3. 完整 Browser/Go/Authority Anchor：3/3
4. 200-WSS baseline
5. 200-WSS slow-reader
6. connections-200
7. connections-500
8. mix-low
9. mix-medium
10. mix-high
```

当前 test-only overlay 对齐后的完整 Browser Anchor 已连续通过两轮；其中最终分类器版本有一轮完整 `F3_CHAIN_PASS`。这不等于要求的 10/10 或 3/3 已完成。

---

## 11. 最终边界

本轮后续工作把原来的“F3 未分类失败”拆成了两个确定结论：

```text
原 F3 超时
= 测试只点击一次，但固定 H5 要求两次确认
= CONTRACT_CONFLICT / H5_WIRE_THEFT_MISSING
= 已通过 test-only overlay 对齐并获得 F3_CHAIN_PASS

Worker InvalidOrders
= 两个唯一 Pending/Due Care Source Order
= 早于 F3
= 与 F3 Theft operation_id 不同
= PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG
= 当前真正的正确性阻断
```

因此当前最诚实的发布状态是：

```text
Q1 工具链                  READY
F3 Fault/Recovery Chain    PASS
产品正确性锚点             FAIL（Care Pair Due Recovery Scope）
性能基线                   NOT_RECORDED
容量结论                   NONE
30M DAU 声明               PROHIBITED
```
