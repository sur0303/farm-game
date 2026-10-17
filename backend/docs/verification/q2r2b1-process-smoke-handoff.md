# Q2R-2B1 REAL PROCESS STARTUP / SHUTDOWN SMOKE Handoff

- **clean_branch** = `codex/q2r2b1-real-process-smoke-20260807`（已 push，远端指向 eacc9c8）
- **clean_worktree** = `farm-game-q2r2b1-smoke-clean-20260807`
- **final_head** = `eacc9c8`（从 base `a18a99d` 单 commit 重建；tree hash 与旧 HEAD
  完全一致，运行证据机械绑定）
- **base** = `a18a99d`（Q2R-2A final，CP-Q2R2A-COMPANY-PROXY-REDIS-EVIDENCE）
- **abandoned_branch** = `codex/q2r2b1-process-smoke-20260807`（**DO_NOT_MERGE /
  DO_NOT_RESUME**）
- **abandoned_commits** = `cee7d44 -> 4402d47`
- **abandoned_reason** = 中间历史含 22MB 编译产物 blob；push_forbidden = true
- 工具：`cmd/q2r2b1-smoke`（Q2R2_LIVE=1 + FARM_REDIS_URL 双门控，无环境即拒绝运行）
- 目标：启动**真实 player-svr / farm-svr 二进制** + **公司 Proxy Redis** +
  `AUTHORITY_MEMBERSHIP_MODE=true` + **Static Assignment** + Remote Watch OFF，
  验证 S1–S4 进程级语义。
- 环境：`redis_profile=company-proxy`；Endpoint/密码不在文档（受控 Evidence/CI Secret）。

## 1. Scenarios

| 场景 | 断言 | 结果 |
|---|---|---|
| S1 0-Shard | player+farm 0-shard 启动：healthz=200、readyz=200、日志含 `membership=composed`、无 legacy renewal、ServingShardCount=0 | **PASS** |
| S2A 1-Shard PLAYER | 预置 Redis Authority（Claim G1, PLAYER 域）→ snapshot 分 Shard → readyz=200、日志 `membership=composed domain=PLAYER holder=… snapshot_revision=`、**真实 Renew ≥2 轮**（`lease_expire_at_ms` 单调推进）、无 legacy renewal | **PASS**（`1786094320430 -> 1786094321843`） |
| S2B 1-Shard FARM | 同上但 FARM 域 composition root：claim(FARM) + snapshot FARM route + **farm-svr 真实 Renew ≥2 轮**、readyz=200、无 legacy renewal | **PASS**（`1786094322005 -> 1786094323418`） |
| S3 No-Fallback | 3a holder 未注册 / 3c authority mismatch / 3d 非法 membership mode → 进程**非 0 退出**、readiness 从未开放、无 legacy 回退；3b zero-route boot mismatch 与 3e **assigned-shard boot mismatch** 均为 composed 合同的**安全 0-shard ready**；无遗留 Guard/测试 Key | **PASS** |
| S4 SIGTERM | **player-svr 与 farm-svr 各一次**：SIGTERM → 有界退出（≤10s）、exit code=0、无 fatal（Runtime 走 Closed 而非 Fatal）、测试创建 Key 精确清理 | **PASS** |

## 2. 关键证据

- **S2A/S2B 真实 Renew（两种 Domain）**：分别读公司 Redis PLAYER / FARM shard
  hash 字段 `lease_expire_at_ms`（claim/renew Lua 写入），跨 ≥3 个 renew interval
  后单调推进 → 证明 PerShardLeaseScheduler 在**两个 composition root**（player-svr、
  farm-svr）+ 公司 Proxy Redis 上都是唯一 Renew Owner。
- **S3 失败不回退**：`NewRuntimeForMode` 失败 → cmd `log.Fatal` → 非 0 退出，
  无 legacy runRenewal 启动（日志无 `legacy`/`runRenewal`）。
- **S3 3b/3e boot mismatch（区分两种）**：
  - 3b zero-route：holder 已注册、快照未分配任何 shard 给该 holder+boot → 合法
    0-shard ready。
  - 3e **assigned-shard**：快照把 shard 分配给 holder-id，但 route 的
    holder_boot_id ≠ 进程 boot → composed 合同（`desiredLocalShards` 按
    holder+boot 过滤）下该 route 不成为 desired → 进程**安全降级为 0-shard
    ready**（无服务资格、无双写），**不是启动失败**。这是实测真实行为，与
    legacy `installLocalGrants` 的严格失败路径不同；若产品要求“已分配 shard 但
    boot 不符必须启动失败”，需在 RT1B-3C 系列另立硬化项（不在本轮改
    authoritycomposition）。
- **S3c authority mismatch**：snapshot generation 与 Redis 权威不一致 → 首轮
  reconcile 无法收敛 → 启动失败（非 0 退出）。
- **S4 SIGTERM（双二进制）**：`<-stop` → GracefulStop → health.Shutdown →
  `run()` 返回 → `authorityRuntime.Close()`（state=Closed，非 Fatal）→ exit 0；
  player 与 farm 各验证一次。
- **清理**：每个场景隔离 namespace（`q2r2b1s1/…/s4f-<nano>`）+ 精确 key `DEL`
  （S2A/S2B/S3c/S3e/S4 的 shard authority key），**cleanup_method = exact DEL +
  per-key EXISTS**，无 KEYS/SCAN、无 SCRIPT FLUSH；
  结束后 `tracked_exact_keys_created == tracked_exact_keys_deleted`，
  `tracked_exact_keys_remaining = 0`。

## 2.1 小遗留（不阻塞）

- farm-svr 启动日志仍为 3C-B 旧格式（`membership=%s` 结尾，**未含
  `domain=/snapshot_revision=`**，player-svr 已含）；S2B 的 FARM 域证据由
  claim(FARM)+snapshot FARM route+真实 renew 证明，日志字段留待后续对齐
  （一行日志改动，不属本轮写集）。

## 3. Files

| 文件 | 内容 |
|---|---|
| `cmd/q2r2b1-smoke/main.go` | 新增：build 真实二进制 + 公司 Proxy Redis 连接 + 4 场景进程级 smoke |
| `docs/verification/q2r2b1-process-smoke-handoff.md` | 本文档 |

未触碰：`cmd/**` 其余命令、`api/proto/**`、authoritycomposition 内核、
`membership*.go`/`lease_scheduler.go`/`composition.go`、Redis Lua/KeyBuilder、H5。

## 4. Verification

```text
公司 proxy Redis：
FARM_REDIS_URL=<secret> Q2R2_LIVE=1 \
  go run ./cmd/q2r2b1-smoke
  → S1 / S2A / S2B / S3 / S4 PASS / Q2R2B1 PASS（~20s）

无环境回归：
go test -buildvcs=false ./... -count=1       PASS（全仓）
go vet ./cmd/q2r2b1-smoke/                   PASS
go build -buildvcs=false ./cmd/...           PASS
git diff --check                             PASS
```

## 5. 冻结登记

```text
CP-Q2R2B1-REAL-PROCESS-MEMBERSHIP-SMOKE

base                              = a18a99d
clean_final_head                  = eacc9c8

dirty_history_feature             = cee7d44
dirty_history_cleanup             = 4402d47
dirty_history_status              = ABANDONED_NOT_PUSHED (DO_NOT_MERGE/DO_NOT_RESUME)

zero_shard_player                 = PASS
zero_shard_farm                   = PASS
player_one_shard_real_renew       = PASS
farm_one_shard_real_renew         = PASS
holder_unregistered_fail_closed   = PASS
authority_mismatch_fail_closed    = PASS
invalid_mode_fail_closed          = PASS
player_sigterm                    = PASS
farm_sigterm                      = PASS
exact_key_cleanup                 = PASS

assigned_boot_mismatch_observed   = SAFE_ZERO_SHARD_READY
assigned_boot_mismatch_policy     = REQUIRED_HARDENING_BEFORE_ROUTE_FOUNDATION
                                    (RT1B-3C-C-STATIC-BOOT-MISMATCH-HARDENING)

status                            = PASS_WITH_ROUTE_POLICY_FINDING
```

## 6. 边界与剩余

- 证明的是 **Static Assignment 启动 + Holder 本地 Membership 生命周期**；Remote
  Watch 关闭，**未证明**运行中跨进程动态 0→1→G2→0（属后续 Remote Watch 任务）。
- **S3e assigned-shard boot mismatch**：当前实测为安全 0-shard ready（无双写）。
  Route Foundation 前需由 RT1B-3C-C 硬化：Static 模式存在本 holder 本地 ACTIVE
  route 但 boot 不匹配 → `ErrLocalRouteBootMismatch` → 启动 Fail Closed（防
  Static 模式永久黑洞）；Dynamic Watch 模式保留"新 Pod 先 Ready、再领取 Shard"
  的 Degraded 语义（先登记合同，随 Remote Watch 实现落代码）。
- `real_process_network_fault`（DNS/TCP-RST/half-write/代理无响应/响应丢失/池刷新）
  仍为 Q2R-2B PENDING（本轮只证明明确未发送的 fault-injection 分类，见 Q2R-2A）。
- 平台项（公司 Proxy 后端 failover / 持久化 RPO / 容量延迟成本 / direct-cluster）
  保持 PROBE_PENDING（第 31 号）。
- farm-svr 启动日志缺 `domain=/snapshot_revision=`（P2）：独立收口
  `RT1B-3C-B-OBSERVABILITY-ALIGNMENT`，不在 Q2R-2B1 Evidence 分支改生产 cmd。
