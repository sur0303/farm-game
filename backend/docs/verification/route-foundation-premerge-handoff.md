# ROUTE FOUNDATION PREMERGE Handoff

- 分支：`premerge/route-foundation-20260807`
- worktree：`farm-game-route-foundation-premerge-20260807`（独立目录）
- base：`1ae95ceb`（最新 integration，codex/integration-20260803）
- merge：`5ab3b86` = merge `ea1a17c`（Q2R-2B2 final，含累计 Route 候选）into integration
- 累计 Route 候选链：`935b51e → … → 8ee3688(3C-A) → 425ab7c(3C-B) → a18a99d(Q2R-2A) →
  eacc9c8/9565e9f(Q2R-2B1) → 7990af2(3C-C) → ea1a17c(Q2R-2B2)`

## 1. Gate

```text
go test -buildvcs=false ./... -count=1        PASS（全仓）
go test -race ./internal/authoritycomposition/... ./internal/storage/redis/authority/...  PASS
go vet ./internal/authoritycomposition/... ./cmd/player-svr/... ./cmd/farm-svr/...       PASS
go build -buildvcs=false ./cmd/...            PASS
git diff --check                              PASS
大对象 Gate（1ae95ceb..HEAD 无 >5MB blob）     PASS
Secret/Endpoint 扫描（ZmtwT / 21.6.179 / redis://:..@） PASS（无匹配）
```

## 2. 三层复跑（Premerge HEAD，公司 Proxy Redis）

```text
Q2R-2A  Repository/Lua probe         PASS（7.4s；9 语义 + cluster SKIP=PROBE_PENDING）
Q2R-2B1 Real-process startup/shutdown PASS（S1/S2A/S2B/S3/S4 全 PASS，双 Domain renew + SIGTERM）
Q2R-2B2 Real-process network-fault    PASS（N1..N6 全 PASS，N5 commit 级 PROBE_PENDING）
```

公司 Redis 残留：`exact_key_cleanup = PASS`（各 probe/smoke 记录精确 key →
`DEL` → 逐 key `EXISTS==0`，`remaining=0`）；`global_namespace_scan =
NOT_REQUIRED_ON_SHARED_REDIS`（第 31 号禁止 KEYS/全局 SCAN；不因“管理性全库
确认”对共享实例执行无界扫描；仅当平台方提供专用隔离实例或安全管理面索引时
才额外验证）。

## 3. 冻结登记（CANDIDATE）

```text
CP-ROUTE-FOUNDATION-STATIC-AUTHORITY

membership_runtime_code      = PASS   (3C-A)
route_wire_code              = PASS   (3C-B)
static_boot_fence            = PASS   (3C-C)
company_proxy_redis          = PASS   (Q2R-2A)
real_process_startup         = PASS   (Q2R-2B1)
real_process_network_fault   = PASS_WITH_EVIDENCE_LIMITS (Q2R-2B2:
                               N3 local fail-closed = INFERRED,
                               N5 actor-commit = PROBE_PENDING)

static_assignment            = PASS
remote_assignment_watch      = NOT_IMPLEMENTED
pod_crash_auto_takeover      = NOT_IMPLEMENTED

status                       = CANDIDATE_READY_FOR_FINAL_VERIFICATION

剩余核心证据（RF-OBS-1-DIRECT-SHARD-ADMISSION-EVIDENCE）：
  直接向越过本地 Deadline 的目标 Shard 发送受控请求，证明在 Actor Turn /
  LoadAndClaim 之前被拒绝（本地准入确实关闭，而非仅由时间+后续 Renew 推断）；
  权威 Lease 仍有效时同 generation Renew 后恢复准入。完成前维持
  PASS_WITH_EVIDENCE_LIMITS 的措辞，不得宣称“已直接证明 Deadline 后所有新
  Actor Turn 均被拒绝”。
```

## 4. 说明

- 本 Premerge 只合入累计 Route 候选到最新 integration；未实现 Remote Watch /
  RouteManager / 自动迁移（RT1A 后续）。
- N3 本地 Fail Closed 为间接证据（进程内部状态无运行期观测）；最终宣称
  “本地 Deadline 后目标 Shard 停止所有新 Actor Turn”需在 Premerge 后补直接
  观测（方式 B 真实 Shard 准入注入）或明确限制结论。
- 未触碰：Redis Lua/KeyBuilder、公网 Proto、Actor Registry、H5。
