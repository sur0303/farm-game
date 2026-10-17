# RT1B-3C-B / I0-ROUTE-WIRE Handoff

- 分支：`codex/rt1b-3c-b-route-wire-20260807`
- worktree：`farm-game-rt1b-3c-b-route-wire-20260807`（独立目录）
- base：`cb70b3c`；3C-A final_head `8ee3688`（CP-ROUTE-MEMBERSHIP-RUNTIME-CODE，
  READY_FOR_ROUTE_PREMERGE-CODE，已 push）
- 范围：**只改 cmd 接线 + compose/env 接缝 + 接线测试**；不接 Remote Watch、
  不改 RouteManager/Actor Registry/Redis 布局、不改 authoritycomposition 既有
  membership 内核（`membership*.go`、`lease_scheduler.go`、`composition.go` 主逻辑
  均未触碰）。

## 1. Frozen Mode Contract（冻结模式契约）

`AUTHORITY_MEMBERSHIP_MODE` 二选一，由 `authoritycomposition.MembershipModeFromEnvironment`
解析，`NewRuntimeForMode` 组装：

| 值 | 组装路径 | 禁止 |
|---|---|---|
| `true`（/1/on/yes/composed/membership） | `NewMembershipRuntime`（RealMembershipAdapter + ShardMembershipReconciler + PerShardLeaseScheduler） | legacy `runRenewal` **绝不启动**；legacy lease 表保持空 |
| `false`（/0/off/no/legacy/空） | 旧 `New`（全局 `runRenewal`） | 新 Scheduler **绝不启动**（`scheduler == nil`） |

硬性禁止：
1. 两套 renew 同时运行（模式互斥 + `TestNewRuntimeForMode{Composed,Legacy}...` 双断言）。
2. Authority 初始化失败后静默回退 legacy（`NewRuntimeForMode` 失败返回 `(nil, err)`，
   cmd 一律 `log.Fatal`，`TestNewRuntimeForModeComposedFailureNeverFallsBackToLegacy`）。
3. player-svr 与 farm-svr Ready 语义不一致（两端 ready 探针都基于
   `authorityRuntime.Ready()` = `ProcessReady()` = `membershipState == Ready`）。

## 2. Changes

| 文件 | 改动 |
|---|---|
| `internal/authoritycomposition/membership_mode.go` | 新增：`MembershipMode`（composed/legacy）+ `MembershipModeFromEnvironment`（未知值 error，空默认 legacy）+ `NewRuntimeForMode`（composed→NewMembershipRuntime，legacy→New，无回退） |
| `cmd/player-svr/main.go` | 解析 `AUTHORITY_MEMBERSHIP_MODE`；ModeAuthority 分支改用 `NewRuntimeForMode`；**重构为 `run() error`**（构造成功后任何错误都经 return 触发 `defer authorityRuntime.Close()`，`log.Fatal` 只留 openStore 等构造前 helper）；启动日志含 membership/domain/snapshot_revision |
| `cmd/farm-svr/main.go` | 同上（DomainFarm）；另把 `openGatewayDownlinks` 改为返回 error（失败时关闭已建立连接），消除 helper 内 `log.Fatal` 绕过 Close |
| `deploy/docker-compose.local.yml` | player-svr/farm-svr 加 `AUTHORITY_MEMBERSHIP_MODE`（默认 false 保持本地 legacy，可覆盖） |
| `deploy/docker-compose.yml` | player-svr/farm-svr 加 `AUTHORITY_MEMBERSHIP_MODE`（**基础 compose 默认 false**；C1/C2 拆分，不把默认 true 作为生产开关） |
| `deploy/docker-compose.membership.yml` | **新增**：membership enablement override（`AUTHORITY_MEMBERSHIP_MODE=true` 显式启用；生产清单要求显式传值，空值静默 legacy 不可发生） |
| `internal/authoritycomposition/membership_mode_test.go` | 接线测试（见 §4） |
| `docs/verification/rt1b-3c-route-wire-handoff.md` | 本文档 |

未触碰：`cmd/**` 其余命令、`api/proto/**`、Actor Registry、Redis Lua/KeyBuilder、
`membership*.go`/`lease_scheduler.go`/`composition.go`（3C-A 冻结物）、H5。

## 2.1 Focused Review 修复（commit 7c30860 之后）

| Review 发现 | 修复 |
|---|---|
| `log.Fatal`/`log.Fatalf` 绕过 `defer authorityRuntime.Close()`（player 84/92/170/192 行、farm 构造后 10+ 处） | 两个 main 重构为 `func run() error`；`main` 只 `log.Print(err); os.Exit(1)`。任何构造/运行错误经 `return` 触发全部 `defer`（`Close` 先于退出）。构造前 helper（`openStore`/`openStores`/`configureActorRoute`）保留 `log.Fatal`（无 runtime 可泄漏） |
| `openGatewayDownlinks` 内部 `log.Fatalf` 绕过 Close | 签名加 `error` 返回；失败时关闭已建立连接再 return |
| 基础 compose 默认 true 过早 | C1/C2 拆分：基础 compose 默认 false，`docker-compose.membership.yml` 显式 true；生产清单必须显式传值 |
| 空值默认 legacy 的生产风险 | `MembershipModeFromEnvironment` 空→legacy 仅作本地兼容；启动日志输出 `membership=composed/legacy domain=... holder=... boot=... snapshot=... snapshot_revision=N` 使模式可观测；生产 manifest 必须显式 `AUTHORITY_MEMBERSHIP_MODE=true` |

## 3. Wiring Shape

```text
player-svr / farm-svr
  └─ AUTHORITY_MEMBERSHIP_MODE=true
       └─ authoritycomposition.NewRuntimeForMode → NewMembershipRuntime
            ├─ RealMembershipAdapter   (repository-proved install)
            ├─ ShardMembershipReconciler (snapshot-driven, action barrier Close)
            └─ PerShardLeaseScheduler  (SOLE renew owner; runRenewal 不启动)
       readyz ← authorityRuntime.Ready() == ProcessReady == state==Ready
```

`OptionsFromEnvironment` 保持零值 per-shard tuning（`NewMembershipRuntime` 回落
默认），无需扩展 env 面。

## 4. Wiring Tests（均 PASS，含 -race）

| 要求 | 测试 |
|---|---|
| `AUTHORITY_MEMBERSHIP_MODE` 解析（true/false/空/大小写/非法→fail） | `TestMembershipModeFromEnvironment`（13 子项表驱动） |
| 每 Shard 唯一 Renew Owner；legacy 续租调用次数 0 | `TestNewRuntimeForModeComposedNeverStartsLegacyRenewal`（composed：scheduler!=nil、serving=1、entries=1、`len(runtime.leases)==0`） |
| 旧模式新 Scheduler 绝不启动 | `TestNewRuntimeForModeLegacyNeverStartsPerShardScheduler`（legacy：scheduler==nil、legacy lease=1） |
| 未知模式不静默回退 | `TestNewRuntimeForModeUnknownModeFails` |
| player-svr registered + 0 shard 启动 Ready | `TestMembershipRuntimeStartupZeroShardProcessReady`（3C-A 既有） |
| farm-svr registered + 0 shard 启动 Ready | `TestMembershipRuntimeFarmDomainZeroShardProcessReady`（新增） |
| 启动 Snapshot 分配 Shard → ServingShardCount/Scheduler Entry 正确 | `TestMembershipRuntimeStartupAdmitsAssignedShard`（player，3C-A 既有）+ `TestMembershipRuntimeFarmDomainAdmitsAssignedShard`（farm，新增） |
| 正常 LostGeneration → ProcessReady 保持 true | `TestMembershipRuntimeLostGenerationKeepsProcessReady`（3C-A 既有） |
| 无法 Fence → ProcessReady=false | `TestMembershipRuntimeFenceFailureLowersProcessReady`（3C-A 既有） |
| SIGTERM → Closed 而非 Fatal；Guards/Entries 清零 | `TestMembershipRuntimeCloseFailClosedAndIdempotent`、`TestRuntimeCloseIsNotFatal`（3C-A 既有；cmd 的 SIGTERM 路径 `defer authorityRuntime.Close()`） |
| Authority 初始化失败 → 进程启动失败，不回落 Legacy | `TestNewRuntimeForModeComposedFailureNeverFallsBackToLegacy`（`(nil, err)` + `ErrLocalHolderNotRegistered`；cmd 端 `log.Fatal`） |

## 5. Verification

```text
go test ./internal/authoritycomposition/... -count=1            PASS
go test -race ./internal/authoritycomposition/... -count=1       PASS
go vet ./internal/authoritycomposition/... ./cmd/player-svr/... \
     ./cmd/farm-svr/...                                          PASS
go build -buildvcs=false ./cmd/...                               PASS
go test -buildvcs=false ./... -count=1                           PASS（全仓）
git diff --check                                                 PASS
```

进程级验证（真实二进制 + 真实 Redis + 真实 snapshot + /readyz + SIGTERM）
属 S1-S4 Route Foundation Smoke，Q2R-2B 阶段执行（Remote Watch 仍关闭）。

## 6. Remaining Work

- **Remote Watch 仍关闭**（I0 只做静态接线；动态 assignment watch 属后续任务）。
- cmd 切换上线后，`New`/`runRenewal` 的彻底退役发生在 legacy 模式移除时
  （当前保留 `AUTHORITY_MEMBERSHIP_MODE=false` 回滚路径）。
- 生产 snapshot/holder env（ACTOR_ROUTE_SNAPSHOT / ACTOR_HOLDER_POD /
  ACTOR_HOLDER_BOOT_ID 等）由部署层注入；compose 已提供接缝。
- 运行期 degraded（REMOVE fence 失败）与 OnFatal→ProcessReady 探针联动
  （RT1B-3C-B 之后任务；当前 readyz 已随 `state==Ready` 自动翻转）。
