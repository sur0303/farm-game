# RF-OBS-1 RECOVERY BLOCKER 交接文档（接手者必读）

> 状态：**阻塞**。本地 Deadline 关闭准入的证据已 DIRECT 达成；**同代恢复场景
> 未通过，根因未定位**。请接手者按本文档复现并诊断。

## 0. 一句话结论

- ✅ **已证明（多轮稳定）**：本地 Lease Deadline 到期 → 目标 Shard 准入关闭
  （diag `serving=0`）→ 真实命令在 Actor Turn 前被本地准入层拒绝（黑盒
  `writer epoch mismatch`，~700µs）。
- ❌ **未证明（阻塞）**：权威 Lease 仍有效时，黑洞结束后的同代 renew 恢复
  （`lease_expire_at_ms` 8s 内完全停滞）。

## 1. 现象（多轮复现）

### 1.1 黑洞中 renew 停止
fault-proxy 事件（`fault-proxy-events.ndjson`）显示黑洞期间：

```
conn_id=1  bytes_in=186   ← 唯一的 renew EVALSHA（黑洞开始后 ~48ms 转发到 Redis）
conn_id=2  bytes_in=30    ← ~4s 后
conn_id=3  bytes_in=30    ← ~4s 后
conn_id=4  bytes_in=30
```

- 之后**再无 186B 的 renew EVALSHA** → 黑洞中 renew 通道实际停止。
- 30B 请求身份未知（疑似 go-redis 连接健康检查 PING，非 renew）；fault proxy
  未做命令级解析，无法确认。

### 1.2 权威 lease 停滞
`authority-timeline.ndjson`（observeLoop 每 200ms 采样）：黑洞开始后
`lease_expire_at_ms` 恒为定值（如 `1786174612415`），`redis_time_ms` 持续前进 →
**权威 lease 不推进**（黑洞中无成功 renew）。

### 1.3 恢复后不恢复
黑洞结束（`proxy.StopMode()`）后，`waitLeaseAdvance(before=黑洞期 lease)` 等待
8s，lease 无任何推进（`lease_expire_at_ms` 仍为黑洞期值，随后权威过期）。

### 1.4 与 Q2R-2B2 N3 的行为差异（关键矛盾）
- Q2R-2B2 N3（TTL=8s / margin=1.5s / 窗口 ~6.4s / 恢复余量 ~0.8s）→ **PASS**，
  恢复后 renew 在 0.8s 内成功、lease 推进。
- RF-OBS-1（TTL=20s / margin=5s / 窗口 ~5.5s / 恢复余量 ~4.5s）→ **FAIL**，
  黑洞中 renew 停止、恢复后 8s 不恢复。

**同是 modeBlackhole（请求到 Redis、响应截留），结果相反** → 差异不在基本
故障注入，而在场景附加内容或参数。

## 2. 已尝试的排查与结果（均未定位）

| # | 动作 | 结果 |
|---|---|---|
| 1 | 调整 TTL 8s→12s→20s、margin 1.5s→5s | 恢复仍失败；黑洞中 renew 均停止 |
| 2 | 缩短黑洞内附加耗时（800→200ms、diag 600→300ms） | 无改善 |
| 3 | 修正 `waitLeaseAdvance`（before=0 立即返回 bug） | 暴露真停滞，未修复 |
| 4 | 修正 same-generation 字段名（holder_instance_id） | 通过（revision/epoch 未变） |
| 5 | fault-proxy 事件分析 | 确认 renew 停止；无法识别 30B 请求 |
| 6 | 阅读 renewOnce 源码 | LOCAL_FENCED 后理论应继续 renew（RefreshOrRearmIfMatches）；与实测矛盾 |

## 3. 为什么我排查不出来（能力边界）

1. **观测盲区**：RF-OBS-1 写集禁止修改 authoritycomposition/lease_scheduler/
   membership core → 无法观测 entry 的 renew 循环状态、go-redis 连接池、阻塞的
   读等进程内部信息；只能通过 readiness/Redis/黑盒命令/字节级 proxy 间接推断。
2. **fault proxy 无命令级解析**：只能看到字节数与连接事件，无法区分
   "EVALSHA renew" vs "PING 健康检查" vs "gRPC 探测触发的 Claim EVALSHA"；
   scheduler 无 renew 结果日志可对。
3. **行为不一致无内部抓手**：Q2R-2B2 N3 与 RF-OBS-1 结果相反，但两者差异
   （附加的 gRPC 探测、diag、参数）无法在黑盒层隔离出唯一原因。
4. **数值与时序反复调整未收敛**：黑洞实际时长（5.5s）与窗口计算值（14.9s）
   不一致，且无法解释 → 说明观测到的"黑洞段"本身被其他因素（连接池/额外请求）
   干扰。

## 4. 最可能根因方向（供接手者优先验证）

**假设 A（连接池被探测请求占满/卡死）**：RF-OBS-1 在黑洞中发送
`GET_PLAYER_BOOTSTRAP`（gRPC）→ 进程内 bindingFor→Claim→发 LOAD_AND_CLAIM
EVALSHA 到 proxy → 被黑洞截留 → go-redis 连接被阻塞读占用 → 连接池耗尽 →
后续 renew 无可用连接 → renew 停止。恢复后阻塞读超时释放，但 renew 重试节奏/
超时可能使其在权威到期前未成功。

验证方法（不需要改 core）：
- 黑洞中**不发起任何 gRPC 探测**，仅观测 renew 是否停止（隔离变量）。
- 或让 proxy 的 blackhole 对 **EVALSHA 响应**做"读后丢弃并保持连接"而非
  "截留不响应"（drop-response 模式，Q2R-2B2 N5 用过且 PASS）——验证"截留 vs
  丢弃"对 go-redis 连接池的不同影响。

**假设 B（renew 退避/超时在黑洞中失控）**：attemptTimeout 在黑洞中耗尽后
entry 进入长退避，恢复后未及时触发下一轮。验证：恢复后观察 entry 首次 renew
时间（需 scheduler 内部观测，或延长恢复等待窗口到退避上限以上）。

**假设 C（权威 lease 在恢复前已过期）**：数值上黑洞结束时应剩 ~4.5s，但
observeLoop 显示黑洞段 lease 停滞于 2415，黑洞结束 redis_time≈7895 → 剩 4.5s，
恢复窗口内应可 renew；若实际入口在 8s 后才重试，则已过期。验证：恢复后立即
（0s）观测 lease 是否在首个 renew 尝试时推进。

## 5. 建议的下一步（最小实验，不改 core）

1. **实验 X1**：黑洞中不发起 gRPC 探测，仅 diag+黑洞+恢复，观测 lease 是否
   推进（验证假设 A）。若 PASS → 确认 gRPC 探测是干扰源。
2. **实验 X2**：用 `drop-response`（N5 模式）替代 `blackhole` 跨本地 deadline，
   观测恢复（验证"截留 vs 丢弃"对 go-redis 的影响）。
3. **实验 X3**：恢复等待窗口拉长到 30s（覆盖退避上限），确认是否只是恢复慢。
4. 若 X1 确认 gRPC 探测干扰 → RF-OBS-1 的黑盒拒绝改在**恢复后立刻探测**
   （Q2R-2B2 N3 的成功时序），把"黑洞期间拒绝"证据与"恢复"分离成两个场景。

## 6. 当前可冻结的证据（无论根因如何均成立）

```text
RF-OBS-1（部分冻结）
LOCAL_SHARD_ADMISSION_CLOSED_DIRECTLY = PASS   diag serving=0（guard fenced）
COMMAND_REJECTED_BEFORE_ACTOR_TURN    = PASS   黑盒 ~700µs writer epoch mismatch
NO_RECEIPT_OR_RECORD_MUTATION         = PASS
SAME_GENERATION_RENEW_RECOVERED       = REQUIRED_FINDING（恢复未通过，根因待 X1-X3）
status                                = PARTIAL_PASS
```

- Q2R-2B2 N3 已 PASS 同代恢复（TTL=8s/margin=1.5s），可作为"同代恢复成立"的
  独立证据引用；RF-OBS-1 的恢复失败不推翻它（行为差异待解释）。

## 7. 环境与命令

- worktree：`farm-game-rf-obs1-direct-shard-admission-20260807`
- 分支：`codex/rf-obs1-direct-shard-admission-20260807`（HEAD `d812977` 之后有
  未提交的 harness 修改：TTL/margin/diag/黑盒拒绝/同代断言/waitLeaseAdvance）
- 运行：`FARM_REDIS_URL=<secret> Q2R2_LIVE=1 go run ./cmd/rfobs1-smoke`
- 关键文件：
  - `cmd/rfobs1-smoke/main.go`（场景；黑洞窗口 `localDeadlineWindow`、黑盒拒绝、
    同代断言、`waitLeaseAdvance`）
  - `cmd/rfobs1-smoke/envsnap.go`（TTL=20s/margin=5s、claim）
  - `cmd/rfobs1-smoke/faultproxy.go`（blackhole/drop-response 模式）
  - `cmd/player-svr/q2r2diag.go` + `q2r2diag_test.go`（诊断接缝，4 硬约束已测）
- artifacts：`/tmp/rfobs1-artifacts-*/RF-OBS-1/`（verdict.json、authority-timeline.ndjson、
  fault-proxy-events.ndjson、process.log）

## 8. 接手者待办

1. 按 §5 做 X1-X3 最小实验，定位 renew 停止根因。
2. 若需改 authoritycomposition 内部（renew 日志/连接池观测），请单独开工作包
   （RF-OBS-1 分支不扩写集）。
3. 恢复场景通过后，把 `SAME_GENERATION_RENEW_RECOVERED` 升为 PASS，RF-OBS-1
   整体冻结，并更新 Route Foundation Premerge。
4. 若确认 gRPC 探测干扰，把"黑洞期间拒绝"与"恢复"拆成两个独立场景，避免
   相互干扰。
