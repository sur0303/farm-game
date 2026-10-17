# RT1B-3C-C STATIC BOOT MISMATCH HARDENING Handoff

- 分支：`codex/rt1b-3c-c-static-boot-mismatch-20260807`
- worktree：`farm-game-rt1b-3c-c-static-boot-mismatch-20260807`（独立目录）
- base：`eacc9c8`（Q2R-2B1 final）
- 目标：Static 模式下，本地 holder 存在 ACTIVE route 但期望 boot ≠ 进程 boot
  时启动 Fail Closed（`ErrLocalRouteBootMismatch`），防止“Shard 已分配但无人
  服务”被 0-shard Ready 长期掩盖（Remote Watch 未启用，错误拓扑无法运行期
  自动修正）。不做动态迁移/跨进程 Watch/RouteManager/自动接管。

## 1. Frozen Semantics（Static 模式）

| 拓扑 | 语义 |
|---|---|
| registered holder + 无任何本地 route | **合法 0-shard Ready**（无服务资格，安全） |
| foreign holder route | 当前 Runtime **忽略**（非本地，不影响本进程） |
| local holder + ACTIVE route + boot 匹配 | 正常继续构造 |
| local holder + ACTIVE route + boot 不匹配 | **启动 Fail Closed**（`ErrLocalRouteBootMismatch`，非 0 退出、readiness 从未开放、无 legacy 回退） |
| 匹配 route 与错误 boot route 同时存在 | **仍失败**（只要存在一个本 holder 的 ACTIVE route 且 boot 不匹配，静态拓扑即不可信） |

Future Dynamic Watch（Remote Watch ON）：新 boot 可先 0-shard Ready 并暴露
`membership_topology_degraded`，由 RouteManager 换代重分配。该合同**仅登记**
（`TestDynamicMembershipRuntimeLocalHolderOldBootMayStartDegraded` SKIP），
随 RT1A 实现落代码；当前不得写成已实现。

## 2. Changes

| 文件 | 改动 |
|---|---|
| `internal/authoritycomposition/composition.go` | 新增 `ErrLocalRouteBootMismatch` + `checkLocalRouteBootMismatch`；在 `buildCompositionCommon` 的 holder 注册检查后调用（按 `opts.Domain` 限定） |
| `internal/authoritycomposition/boot_mismatch_test.go` | 5 个核心测试 + 1 个 Dynamic 合同登记（SKIP） |
| `cmd/q2r2b1-smoke/main.go` | 3e 扩展为 PLAYER+FARM 双域 assigned boot mismatch：非 0 退出 + readiness 从未开放（`runToExitNoReady`）+ 无 legacy renewal 回退 |
| `docs/verification/rt1b-3c-c-static-boot-mismatch-handoff.md` | 本文档 |

未触碰：`lease_scheduler.go`、`membership*.go`、`adapter.go`、Redis Lua/
KeyBuilder、`cmd/**` 其余命令、Actor Registry、H5。

## 3. 真实进程证据（公司 Proxy Redis，Q2R-2B1 完整复跑）

```
S1  PASS  player+farm 0-shard ready (zero-route 不受影响)
S2A PASS  PLAYER one-shard real renew (deadline 推进 ≥2 轮)
S2B PASS  FARM one-shard real renew (deadline 推进 ≥2 轮)
S3  PASS  holder-unregistered / authority-mismatch / illegal-mode /
          assigned-boot-mismatch (PLAYER+FARM) 均非 0 退出、
          readiness 从未开放、无 legacy renewal 回退；
          zero-route boot mismatch 仍为合法 0-shard ready
S4  PASS  player+farm 双二进制 SIGTERM 有界退出 exit 0、无 fatal、exact-key 清理
```

复跑环境：`Q2R2_LIVE=1` + `FARM_REDIS_URL=<secret>`；唯一 namespace、
`cleanup_method = exact DEL + per-key EXISTS`（无 KEYS/SCAN/SCRIPT FLUSH）；
结束后 `tracked_exact_keys_created == tracked_exact_keys_deleted` 且
`tracked_exact_keys_remaining = 0`。

## 4. Verification

```text
go test ./internal/authoritycomposition/... -count=1          PASS
go test -race ./internal/authoritycomposition/... -count=1     PASS
go vet ./internal/authoritycomposition/...                     PASS
go test -buildvcs=false ./... -count=1                         PASS（全仓）
go build -buildvcs=false ./cmd/...                             PASS
go vet ./cmd/q2r2b1-smoke/...                                  PASS
git diff --check                                               PASS
```

## 5. 冻结登记

```text
CP-ROUTE-STATIC-BOOT-MISMATCH-CODE

base                            = eacc9c8
implementation                  = 6ac12e4  (composition.go + boot_mismatch_test.go)
real_process_evidence           = 1069dbb  (smoke main.go + handoff)
final_head                      = 1069dbb

static_zero_route               = READY
static_foreign_holder_route     = IGNORED
static_local_boot_mismatch      = FAIL_CLOSED
dynamic_old_boot_policy         = PENDING_RT1A

status                          = PASS
```

Q2R-2B1 状态升级：`PASS_WITH_ROUTE_POLICY_FINDING` → **PASS**
（S3e 政策已由本工作包收口）。

## 6. 剩余

- Dynamic Watch 的 degraded 合同（RT1A）尚未实现，仅登记。
- Q2R-2B2（真实进程网络故障：Toxiproxy 短时断连 / Deadline 过期 / 恢复后重新
  Admit / 权威 Lease 真过期不可复活）从本分支硬化后的 final HEAD 起建。
