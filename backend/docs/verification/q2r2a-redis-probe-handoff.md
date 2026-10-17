# Q2R-2A REAL REDIS REPOSITORY / LUA PROBE Handoff

- 分支：`codex/q2r2-redis-probe-20260807`
- worktree：`farm-game-q2r2-redis-probe-20260807`（独立目录）
- base：`425ab7c`（RT1B-3C-B final，CP-ROUTE-WIRE-CODE）
- 目标：证明 MemoryStore/FaultStore（miniredis）中成立的 authority 语义在**公司
  Redis** 上也成立；公司特有项（direct-cluster 路由/持久化/容灾/成本）按第 31
  号保持 PROBE_PENDING。
- 结果合同：只用 PASS / FAIL / INCONCLUSIVE / PROBE_PENDING 登记；MemoryStore
  PASS 不得写成“真 Redis 已验证”。

## 0. 修复记录（Q2R-2A-FIX-LIVE-PROBE-CLASSIFICATION-SAFETY，base 24ec6d3）

| Finding | 修复 |
|---|---|
| 场景 2 把 FENCE_MISMATCH 表述成“不虚报 LostGeneration”，与冻结合同冲突 | 拆成两个冻结测试：transport → `UNAVAILABLE_NOT_SENT`（`LeaseRenewTransient`，Deadline 前 Guard 保持 Admitted）；stale-generation renew → `FENCE_MISMATCH`（`LeaseRenewLostGeneration`，精确 Fence 目标 generation，当前 generation 不受影响）。`TestLiveQ2R2TransientTransportFailureBeforeDeadlineKeepsAdmission` / `TestLiveQ2R2FenceMismatchIsLostGeneration` |
| 共享实例上默认 `SCRIPT FLUSH` | 移除默认 FLUSH；用**从未加载过的唯一脚本 SHA**（随机注释）自然触发 NOSCRIPT → registry 加载内嵌源码 → 重试成功（`TestLiveQ2R2NoScriptReloadWithoutGlobalFlush`）。不再对共享实例做全局副作用 |
| 用 `KEYS`/全局 `SCAN` 清理/验残留 | 改为记录 probe 创建的**精确 Key 集合**，清理 `DEL` 每个 key 并逐 key `EXISTS==0`（`TestLiveQ2R2CleanupUsesExactKeys`，`q2r2Keys` helper） |
| Handoff/日志含真实 Endpoint/密码/URL | 脱敏：`redis_profile=company-proxy`、`auth_enabled=true`、`cluster_commands_exposed=false`；真实 Endpoint 存受控 Evidence/CI Secret，不进普通 Git 文档 |
| Proxy / Direct-Cluster 混淆 | 拆分登记：company-proxy profile = **PASS**；direct-cluster MOVED/ASK/Primary 路由 = **PROBE_PENDING** |

## 1. Environment（脱敏）

- `redis_profile = company-proxy`（代理型 Redis；AUTH 实测 OK、Redis TIME 可用）。
- `auth_enabled = true`，`cluster_commands_exposed = false`（`CLUSTER` 命令被代理隐藏）。
- `RedisClientProfile = COMPANY_PROXY_V1`，`application_receives_moved_ask = false`
  （应用只连代理 Endpoint；不宣称支持直连 Cluster Profile）。
- 真实 Endpoint 不在本文档与日志中；由受控 Evidence 存储/CI Secret 提供。
- Probe 运行要求 `Q2R2_LIVE=1` **且** `FARM_REDIS_URL` 已设置；缺任一即 Skip，
  普通开发环境不会意外访问公司 Redis。
- Probe 数据：隔离 namespace `liveq2r2-<unix nano>` + 短 TTL；清理只 `DEL`
  probe 自己创建的精确 key 并 `EXISTS==0` 验证，**不用 KEYS/SCAN**。
- 本修复版不再对共享实例执行 `SCRIPT FLUSH`（唯一脚本 SHA 自然触发 NOSCRIPT）。

## 2. Probe Results

| # | 场景 | 结果 | 证据 |
|---|---|---|---|
| 1 | Claim G1 → Renew G1：Redis TIME 返回 AcceptedTTL，本地单调 Deadline 刷新 | **PASS** | `Q2R2-1 OK`；LeaseUntil 跟踪 Redis TIME（delta<1.5s），Renew 后单调前进 |
| T | transport 错误（未发送 ECONNREFUSED）→ `UNAVAILABLE_NOT_SENT`（LeaseRenewTransient）；Deadline 前 Guard 保持 Admitted；权威 lease 不受影响 | **PASS** | `Q2R2-TRANSIENT OK status=UNAVAILABLE_NOT_SENT then_live_until=…`；非 FenceMismatch/LostGeneration，后续正确 Renew APPLIED |
| L | stale-generation Renew → `FENCE_MISMATCH`（LeaseRenewLostGeneration），**绝非 Transient**；精确 Fence 该 generation；当前 generation 不受影响 | **PASS** | `Q2R2-LOSTGEN OK stale=FENCE_MISMATCH current_until=…` |
| 3 | 本地安全 Deadline 到期（权威 lease 仍有效）→ 同 generation Renew 重新 Admit | **PASS** | `Q2R2-3 OK` |
| 4 | 权威 Lease 真正过期 → 迟到 Renew 不能复活 | **PASS** | `Q2R2-4 OK late_renew_status=LEASE_EXPIRED` |
| 5 | Transfer G1→G2→G3：G1/G2 迟到 fence 不影响 G3 | **PASS** | `Q2R2-5 OK`；g3 推进成功，stale fences 被拒 |
| N | NOSCRIPT（从未加载的唯一 SHA）→ 自动 reload 后恢复，无全局 SCRIPT FLUSH | **PASS** | `Q2R2-NOSCRIPT OK unique_sha=… reloaded without global flush` |
| 7 | Lua 已提交、响应丢失 → 相同身份重放解析 ALREADY_COMMITTED，不安装未证明 generation | **PASS** | `Q2R2-7 OK`；真实 Redis 幂等重放 + lookup FOUND_MATCH |
| C | exact-key 清理：DEL 精确 key + EXISTS==0，无 KEYS/SCAN | **PASS** | `Q2R2-CLEANUP OK exact-key DEL + EXISTS == 0` |
| 8 | Direct Cluster 行为：MOVED / ASK / 目标 Primary Script Load / Failover | **PROBE_PENDING** | company-proxy 隐藏 CLUSTER 命令；直连集群实例未就绪（第 31 号 PENDING 保留） |

## 3. 最终登记（冻结候选）

证据范围精确化：本次验证的是 **Shard Authority Lease 脚本族**（Claim/Renew/
Transfer/fence/expiry/NOSCRIPT/Unknown 重放），**不等于**第 31 号完整 Actor 强
提交链（`LoadAndClaimActor`、ActorRecord+Receipt、RecordVersion CAS、
SourceOrder+DueIndex、容量上限、Commit TIMEOUT_UNKNOWN Resolver 等需
R1/Actor Commit 专门真库 Probe）。

```text
CP-Q2R2A-COMPANY-PROXY-REDIS-EVIDENCE

company_proxy_shard_authority_lease_family = PASS
claim_renew_ttl                 = PASS
transient_not_sent_fault_injection = PASS  (UNAVAILABLE_NOT_SENT -> LeaseRenewTransient)
fence_mismatch_lost_generation  = PASS     (FENCE_MISMATCH -> LeaseRenewLostGeneration)
local_deadline_recovery         = PASS
authority_expiry_no_revival     = PASS
g1_g2_g3_isolation              = PASS
noscript_safe_reload            = PASS     (unique SHA, no global flush)
timeout_unknown_resolution      = PASS     (ALREADY_COMMITTED replay)
exact_key_cleanup               = PASS     (DEL + EXISTS, no KEYS/SCAN)

platform (PROBE_PENDING):
real_process_network_fault      = PENDING_Q2R2B  (DNS/TCP-RST/half-write/proxy无响应/响应丢失/池刷新)
company_proxy_backend_failover  = PROBE_PENDING  (后端 Primary 真实切换)
company_proxy_persistence_rpo   = PROBE_PENDING
company_proxy_capacity_latency  = PROBE_PENDING  (长时 Lua P99 / 吞吐 / 连接池预算 / 成本)
direct_cluster_profile          = PROBE_PENDING  (MOVED/ASK, 目标 Primary Script Load, Failover)

proxy_profile                   = PASS
status                          = PASS_WITH_PLATFORM_PENDING
```

transient 证据边界：`transient_not_sent_fault_injection = PASS` 证明的是
“Repository/Scheduler 收到**明确未发送**错误后分类为 Transient 且不误判
LostGeneration”（EVALSHA 接缝注入 `ECONNREFUSED` → fateNotSent）。真实 OS 网络
路径（DNS 失败、已连接后 RST、请求写出一半、代理无响应、响应包丢失、连接池
拓扑刷新）不在本轮范围，归 Q2R-2B `PENDING_Q2R2B`。

## 4. Files

| 文件 | 内容 |
|---|---|
| `internal/storage/redis/authority/q2r2_probe_test.go` | Q2R-2A live probe（`FARM_REDIS_URL` 门控）；TRANSIENT/LOST_GENERATION 拆分、唯一脚本 NOSCRIPT、exact-key 清理、无 KEYS/SCAN、无默认 SCRIPT FLUSH |
| `docs/verification/q2r2a-redis-probe-handoff.md` | 本文档 |

未触碰：`cmd/**`、`api/proto/**`、业务 Repository/Lua 主体、RT1B-3C 冻结物、
`membership*.go`/`lease_scheduler.go`/`composition.go`。

## 5. Verification

```text
公司 proxy Redis：
FARM_REDIS_URL=<secret endpoint> Q2R2_LIVE=1 \
  go test ./internal/storage/redis/authority/ -run Q2R2 -v -count=1
  → 9 PASS + 1 SKIP(PROBE_PENDING)

缺省（无 Q2R2_LIVE=1 或 FARM_REDIS_URL）→ 全部 Skip，绝不触网。

无环境回归（probe 无 FARM_REDIS_URL 自动 Skip）：
go test ./internal/authoritycomposition/... -count=1          PASS
go test -race ./internal/authoritycomposition/... -count=1     PASS
go test -buildvcs=false ./... -count=1                         PASS（全仓）
go vet ./internal/authoritycomposition/...                     PASS
git diff --check                                               PASS
```

## 6. 剩余 PENDING（不因本次 Probe 关闭）

- **direct-cluster**（MOVED/ASK、目标 Primary Script Load、Failover、连接池拓扑
  刷新）：需公司直连集群实例，保持 **PROBE_PENDING**（第 31 号）。
- 公司 Redis 持久化/容灾/Lua 延迟/成本：第 31 号 PROBE_PENDING。
- Q2R-2B（真实进程 Membership Smoke S1–S4）与 Q2R-2A 一起支撑进程级证据；
  本轮完成 Repository/Lua 层（Q2R-2B 期间不得运行全局 SCRIPT FLUSH）。
