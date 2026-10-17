# RF-OBS-1 DIRECT SHARD ADMISSION EVIDENCE Handoff

- 分支：`codex/rf-obs1-direct-shard-admission-20260807`
- worktree：`farm-game-rf-obs1-direct-shard-admission-20260807`（独立目录）
- base：`387f7f5`（Route Foundation Premerge final，CP-ROUTE-FOUNDATION-STATIC-AUTHORITY CANDIDATE）
- 目标：**直接证明**本地 Lease Deadline 到期后目标 Shard 停止接受新 Actor Turn，
  以及权威 Lease 仍有效时同 generation Renew 后恢复准入（把 Q2R-2B2 N3 的
  INFERRED 证据升级为 DIRECT）。

## 1. 观测接缝（test-only）

- `cmd/player-svr/main.go`：`Q2R2_DIAGNOSTICS=1` 时在 `127.0.0.1` 启动只读 HTTP
  `/q2r2z/diag`，暴露 `serving_shard_count` / `can_serve_shard`（`Runtime` 现有
  只读方法）。非公网管理 API；不注册 Proto；不暴露到 CLB；只读不改状态。
- 说明：黑洞期间经故障代理的业务 Redis 不可达，但 diagnostics 是进程本地
  127.0.0.1 端点、不受 Redis 故障影响，因此黑洞中可直接观测本地准入状态
  （无黑盒竞态）。

## 2. 结果（公司 Proxy Redis 实测，8/8 PASS）

```text
BASELINE_ADMISSION_ALLOWED            = PASS  bootstrap SUCCEEDED（准入打开）
AUTHORITY_LEASE_STILL_VALID           = PASS  now=1786171541293 lease=1786171549016
LOCAL_DEADLINE_CROSSED                = PASS  window_ms=5023；跨本地 deadline（lease−margin）且权威 lease 仍有效
COMMAND_REJECTED_BEFORE_ACTOR_TURN    = PASS  diag serving=0 can_serve=false  ← DIRECT（黑洞期间）
NO_RECEIPT_OR_RECORD_MUTATION         = PASS  （准入在 Actor Turn 前拒绝；无 SUCCEEDED mutation）
SAME_GENERATION_RENEW_RECOVERED       = PASS  lease_expire_at_ms 恢复后推进
POST_RECOVERY_ADMISSION_ALLOWED       = PASS  diag serving=1 can_serve=true（重新 Admit）
POST_RECOVERY_BOOTSTRAP_SUCCEEDED     = PASS  bootstrap 再次 SUCCEEDED
```

时序：真实 player-svr + 每进程 Fault Proxy + 公司 Proxy Redis + Static
Assignment；确认 ≥2 轮真实 renew；黑洞跨本地安全窗口（Redis TIME + lease −
margin 定窗）；黑洞期间 diag 直接 `serving=0/can_serve=false`；恢复网络后同代
renew 重新 Admit（diag 翻转 `serving=1/can_serve=true`）→ bootstrap SUCCEEDED。

## 3. 关键意义

Q2R-2B2 N3 的 `N3_local_fail_closed_direct_observation` 从
`PENDING_OBSERVABILITY(INFERRED)` 升级为 **DIRECT**：真实进程中本地 Deadline
到期后目标 Shard 的准入确实被关闭（serving=0 + can_serve=false），不再只是
由时间窗口 + 后续 renew 推断。

## 4. Files

| 文件 | 内容 |
|---|---|
| `cmd/player-svr/main.go` | test-only diagnostics 端点（`Q2R2_DIAGNOSTICS=1`，127.0.0.1 只读） |
| `cmd/rfobs1-smoke/*` | RF-OBS-1 smoke：fault proxy + gRPC bootstrap 探测 + diag 直接观测 |
| `docs/verification/rf-obs1-direct-shard-admission-handoff.md` | 本文档 |

未触碰：`lease_scheduler.go`、membership core/adapter、Redis Lua/KeyBuilder、
RouteManager、Remote Watch、Actor 业务语义、公网 Proto/H5。

## 5. Verification

```text
公司 Proxy Redis：FARM_REDIS_URL=<secret> Q2R2_LIVE=1 go run ./cmd/rfobs1-smoke
  → RF-OBS-1 PASS（8/8 checks，~8s）

go test -buildvcs=false ./... -count=1   PASS（全仓）
go vet ./internal/authoritycomposition/... ./internal/player/actor/... ./cmd/rfobs1-smoke/...  PASS
go build -buildvcs=false ./cmd/...       PASS
git diff --check                         PASS
```

公司 Redis 清理：exact-key `DEL`+`EXISTS`（残留 `*0`）；无 KEYS/SCAN/SCRIPT FLUSH。

## 6. 冻结登记（候选）

```text
CP-RF-OBS1-DIRECT-SHARD-ADMISSION

LOCAL_DEADLINE_CROSSED               = true
AUTHORITY_LEASE_STILL_VALID          = true
COMMAND_REJECTED_BEFORE_ACTOR_TURN   = true  (serving=0 can_serve=false, DIRECT)
NO_RECEIPT_OR_RECORD_MUTATION        = true
SAME_GENERATION_RENEW_RECOVERED      = true
POST_RECOVERY_ADMISSION_ALLOWED      = true
POST_RECOVERY_BOOTSTRAP_SUCCEEDED    = true
```

## 7. 后续

- RF-OBS-1 证据并入 Route Foundation Premerge → `CP-ROUTE-FOUNDATION-STATIC-AUTHORITY`
  `real_process_network_fault` 升级为 PASS（N3 direct 证据）。
- RT1A（跨进程 Snapshot Store/Publisher/Watch）从该稳定基线开始。
