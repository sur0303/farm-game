# Q2R-2B2 REAL-PROCESS NETWORK-FAULT SMOKE Handoff

- 分支：`codex/q2r2b2-network-fault-smoke-20260807`
- worktree：`farm-game-q2r2b2-network-fault-smoke-20260807`（独立目录）
- base：`7990af2`（RT1B-3C-C final，CP-ROUTE-STATIC-BOOT-MISMATCH-CODE）
- 目标：真实 player-svr / farm-svr + 公司 Proxy Redis + **每进程独立测试专用 TCP
  Fault Proxy**，验证逐 Shard Lease 在真实断连/黑洞/响应丢弃/恢复下的进程语义。
- 范围：Static Assignment、`AUTHORITY_MEMBERSHIP_MODE=true`、Remote Watch OFF。
  未修改 authoritycomposition 核心 / lease_scheduler / membership* / Redis Lua /
  KeyBuilder / RouteManager / Actor Registry / 公网 Proto / H5。

## 1. 架构

```text
player-svr / farm-svr (真实二进制, env FARM_REDIS_URL=proxy)
        │
        ▼
本机独立 TCP Fault Proxy (每进程一个, 字节透传 + 确定性故障注入)
        │
        ▼
公司 Proxy Redis (AUTH 由客户端经 proxy 透明转发)
```

Fault 模式（`cmd/q2r2b2-smoke/faultproxy.go`）：
- `down`：关闭 listener + 全部连接 → 新 dial ECONNREFUSED（明确未发送）。
- `blackhole`：请求转发到 Redis、响应截留（请求已达，客户端超时）。
- `blackhole-no-forward`：请求不转发（Redis 无操作，权威 lease 老化）。
- `drop-response`：读完整 RESP 响应帧后丢弃并关闭连接（post-execution 响应丢失）。

## 2. Results（公司 Proxy Redis 实测）

| 场景 | 操作 | 结果 |
|---|---|---|
| N1 connect-refused | proxy Down（未发送）→ 恢复 | **PASS**：进程保持 ready（Transient 非 LostGeneration），恢复后同代 renew 推进 |
| N2 short blackhole | 2s 黑洞 < 本地安全窗口（6.5s） | **PASS**：ready 保持，恢复后 lease 继续推进 |
| N3 cross local deadline | 黑洞跨本地 deadline（按 Redis TIME+lease−margin 定窗），权威 lease 仍有效 | **PASS**（跨窗口 + 恢复后同代 renew 重新 Admit，lease 推进）；`N3_local_fail_closed_direct_observation = PENDING_OBSERVABILITY`（本地 Guard Fence 为 INFERRED，非直接观测） |
| N4 authority expiry | blackhole-no-forward 直到权威 lease 真过期 | **PASS**：恢复后 lease 不再推进（迟到 renew 不可复活），进程 0-shard ready（单 Shard LostGeneration 不 Fatal） |
| N5 response drop | 确定性丢弃 renew 响应帧（连接关闭） | **PASS**（`N5_renew_response_drop`）；`N5_shard_authority_unknown_resolution = PASS_Q2R2A`（引用真库 ALREADY_COMMITTED）；`N5_actor_commit_process_response_drop = PROBE_PENDING`（进程内无业务 commit RPC 注入点） |
| N6 SIGTERM blocked | 黑洞中 SIGTERM，PLAYER + FARM 各一次 | **PASS**：有界退出 exit 0、无 fatal（Closed 而非 Fatal）、精确 key 清理 |

## 3. 观测与限制（诚实登记）

- 可观测信号：`/readyz`（Runtime 全局状态）、Redis `lease_expire_at_ms` +
  Redis TIME（Renew 行为）、进程日志、进程退出码。
- 进程内部状态（`ServingShardCount`、`Scheduler EntryCount`、Guard `Admitted`）
  无运行期日志/metrics，外部不可直接观测 → N3 的“本地 Fail Closed”通过
  “跨本地安全窗口 + 恢复后同代 renew 重新 Admit”推断；每场景产出
  `authority-timeline.ndjson` 原始时间线可复核。test-only 观测接缝需改
  player-svr/farm-svr（超出写集），未做。
- 共享 Redis 全局副作用：无（禁止 SCRIPT FLUSH / KEYS / SCAN / 全局 iptables）。

## 4. Artifacts（每场景目录）

`run-manifest.json`、`fault-plan.json`（场景时序）、`fault-proxy-events.ndjson`、
`route-snapshot.json`、`authority-timeline.ndjson`（readyz/Redis TIME/lease/exited）、
`process.log`、`process-exit.json`、`verdict.json`。

## 5. Files

| 文件 | 内容 |
|---|---|
| `cmd/q2r2b2-smoke/faultproxy.go` | 每进程独立 TCP Fault Proxy（RESP 分帧 + 4 种确定性故障） |
| `cmd/q2r2b2-smoke/envsnap.go` | env/快照/权威 claim 辅助 |
| `cmd/q2r2b2-smoke/proc.go` | 进程生命周期/readiness/Redis 观测/时间线/verdict |
| `cmd/q2r2b2-smoke/scenarios.go` | N1–N6 场景 |
| `cmd/q2r2b2-smoke/main.go` | 编排 + artifacts |
| `docs/verification/q2r2b2-network-fault-smoke-handoff.md` | 本文档 |

## 6. Verification

```text
公司 Proxy Redis：
FARM_REDIS_URL=<secret> Q2R2_LIVE=1 go run ./cmd/q2r2b2-smoke
→ N1..N6 PASS（含 N6 player+farm），Q2R2B2 PASS（~70s）

无环境回归：
go test -buildvcs=false ./... -count=1    PASS（全仓）
go vet ./cmd/q2r2b2-smoke/...             PASS
go build -buildvcs=false ./cmd/...        PASS
git diff --check                          PASS
```

公司 Redis 清理：`tracked_exact_keys_created == deleted`、
`tracked_exact_keys_remaining = 0`（管理性验证 `classic-farm:q2r2b2*` 残留
`*0`）；`cleanup_method = exact DEL + per-key EXISTS`。

## 7. 冻结登记（PASS_WITH_EVIDENCE_LIMITS）

```text
CP-Q2R2B2-REAL-PROCESS-NETWORK-FAULT-SMOKE

base                                  = 7990af2
final_head                            = <提交后>

N1_connect_refused                    = PASS
N2_short_blackhole                    = PASS

N3_cross_local_deadline               = PASS
N3_same_generation_recovery           = PASS
N3_local_fail_closed_direct_observation = PENDING_OBSERVABILITY
   (ServingShardCount/EntryCount/Guard Admitted 无运行期日志/metrics；
    本地 Fail Closed 由“跨本地安全窗口 + 权威 lease 有效时同代 renew 重新
    Admit”推断 = INFERRED，非直接观测；方式 B 真实 Shard 准入注入留待
    Route Foundation Premerge 补或明确限制结论)

N4_authority_expiry_no_revival        = PASS
N4_single_shard_lost_generation       = PASS

N5_renew_response_drop                = PASS
N5_shard_authority_unknown_resolution = PASS_Q2R2A
N5_actor_commit_process_response_drop = PROBE_PENDING
   (进程内无业务 Authority Commit RPC 可注入；Q2R-2A 真库
    ALREADY_COMMITTED 重放证据引用，非进程级证据)

N6_player_sigterm_blocked             = PASS
N6_farm_sigterm_blocked               = PASS

company_proxy_profile                 = PASS
static_assignment                     = PASS
remote_watch                          = OFF
direct_cluster_profile                = PROBE_PENDING
proxy_backend_failover                = PROBE_PENDING
capacity_latency_cost                 = PROBE_PENDING

status                                = PASS_WITH_EVIDENCE_LIMITS
```

### 7.1 Base 映射（机械复核）

- Q2R-2B2 base = `7990af2`（3C-C 最终，amend 后占位符已替换）。
- `1069dbb`（3C-C C2 amend 前，含 `<C2 HEAD>` 占位符）**不是** `7990af2` 祖先
  （amend 兄弟，共 parent `6ac12e4`）；完整 tree 不同（仅 handoff 文档差异）。
- **硬化代码 blob 完全一致**（机械验证）：`composition.go` = `ee7c1261`、
  `boot_mismatch_test.go` = `8cf10c1`（1069dbb 与 7990af2 两边相同）→
  `510c09c`（base 7990af2）包含完整 Static Boot mismatch 硬化。
- 映射记录：`1069dbb → 7990af2`（amend 后 clean 等价，代码等价）。

## 8. 剩余

- Q2R-2A + Q2R-2B1 + Q2R-2B2 三层证据齐备后 → Route Foundation Premerge
  （从最新 integration 合入干净 Route 候选 + 全仓 Gate）。
- RT1A Remote Assignment Publisher / Watch（routectl 手动迁移、Pod Crash 自动
  接管）为后续任务。
- N5 的 commit 级 ALREADY_COMMITTED 重放依赖 Q2R-2A 真库证据（进程内无业务
  commit RPC 注入点），保持 PROBE_PENDING。
