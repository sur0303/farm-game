# S7 最终合同同步与 W5B 后施工交接

版本：`2026-08-03-w5b-completed`

日期：2026-08-03

Linux 根目录：`/data/home/developer_026/CodeBuddy/20260519141303`

> 本文只记录当前实现事实和接手入口。语义以 26～33 为准，好友 Redis 物理实现受 33A 门禁，验收以
> 60 为准，施工顺序以 70 为准。本文不得复制或覆盖另一套合同。

## 1. 当前结论

当前实现基线（两仓均已推送远端）：

```text
farm-game:        codex/integration-20260803 @ a949761（唯一现行 HEAD,主 worktree 直接施工）
classic-farm-h5:  codex/integration-20260803 @ 6737438
```

**W5B Subscription Lease 已完成（COMPLETED）**：实现 + 双 Gate/Drain/Fresh Join E2E +
200 人续租容量 + GateBoot 退役翻转证据齐备，详见
`docs/verification/w5b-subscription-lease-handoff.md` §4。

worktree 生命周期记录：W5B 在独立 worktree `farm-game-w5b`
（`codex/w5b-subscription-lease-20260803`）开发，经 MR !6（`c86ce39`）合入 integration 后，
该 worktree 与本地/远端分支均已删除（2026-08-03）。后续工作包如用 worktree 隔离，
合入后按同一纪律清理。

H5 当前基线：

```text
path: /data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5
head: 6737438（W11A/W3A/W7D/recovery-barrier 已在 integration）
```

下一批施工项（70 v3.3 关键路径，不交叉写集可并行）：**W31-B/C/D**（生产 Redis Lua、
Repository、Unknown Resolver、Persistence Lane）、**W11B-S/W32-A/W11B-W**（17 类
CommandCatalog/Typed Wire/经营闭环）、**W6D**（Participant IDL）。

## 2. 合同真源

本机最终 26～33 已同步到 Linux，精确版本与 SHA-256 见
`docs/design/CONTRACT-MANIFEST.md`：

```text
26 v1.2   27 v3.4   28 v1.9   29 v2.4
30 v0.9   31 v0.7   32 v0.25  33 v0.9
```

第 33 号新增的好友非 Actor Authority 物理前置已同步为 33A v0.1，但状态仍是 `ACTIVE_EDIT`。在 33A
提升为稳定输入前，不得让多个 Agent 并行实现 FRIEND_PAIR/QUOTA/SHARE_ISSUER 的真实 Redis Key/Lua。

60 已重审为 v1.5，修复了旧 Snapshot-first 兼容、HomeReady 降级、旧容量模型、Join 重试范围和
Session/Subscription Lease 混淆，并增加 AUTHORIZING、ActionAdmission、ResultDirectory、Unknown 与
200 人精确验收。旧 S01～S06 独立执行计划已经删除，测试规则只在 60 维护。

70 已重排为 v3.3，当前关键路径是（W5B 已完成并从路径移除）：

```text
W31-B/C/D 与 W11B-S/W32-A/W11B-W（不交叉写集可并行）
→ W6D + W31-E
→ W32-B~G
→ 33A Freeze + W33-1~6
→ P0d 最小闭环
→ W31-F/G/H 与全量验收
```

## 3. 已进入当前线性基线的实现

| 工作包 | 提交 | 当前证明级别 |
|---|---|---|
| W2A Join Core | `06d8b10` / handoff `fe75902` | `LOGIC_PASS` |
| W2B Switch/Cleanup | `3443a28` | `LOGIC_PASS` |
| W3B PlayerBootstrap | `9c6580d` | `CURRENT_WIRE_PASS` |
| Recovery Admission | `b00edbd` | `LOGIC_PASS / REAL_E2E_PENDING` |
| Route Hash v2 | `0d1a405` | `LOGIC_PASS` |
| W7D Stable IDs | `a9fb2bf` | `IMPLEMENTATION_PASS` |
| W31-A KeyBuilder/Probe | `c7ddc90` | `IMPLEMENTATION_PASS / PLATFORM_PARTIAL` |
| W8A-L Authority Store | `c231f64` | `LOGIC_PASS / REDIS_ADAPTER_PENDING` |
| W9A-L Fence Resolution | `22429c6` | `FAKE_PROOF / REDIS_LUA_PENDING` |
| W8C Shard Write Authority | `768a8f0` | `LOGIC_PASS / RUNTIME_WIRING_PENDING` |
| W5A SessionRoute Lease | `72de661` | `IMPLEMENTATION_PASS`（双 Gate 证据已由 W5B 证据提交覆盖） |
| W5B Subscription Lease | `c86ce39`(MR !6) + 证据 `f90b11b`/`b2f65e2`/`22b84e9`/`dc235ef` | `COMPLETED` |

“已在分支实现”“已进入当前 worktree”“真实平台通过”“容量通过”是不同结论。上述状态不得升级，除非
对应 Evidence Bundle 绑定实际 Commit、配置、Redis 目标和原始日志。

## 4. W5A 已完成内容

W5A 实现了：

- SessionAuthority Redis TIME；
- Authority 返回 accepted TTL；
- Gate 从 renew-start 建立本地 monotonic deadline，并扣 Safety Margin；
- 已过期 Route/connection generation 不可续租复活；
- 续租响应完整 Fence 校验和重叠轮次串行；
- Deadline 后停止新连接级准入，之前已经 ADMITTED 的命令可完成；
- 公司 Redis Lua 探针、定向 test/race/vet 和全仓测试。

证据：`docs/verification/w5a-session-route-lease-handoff.md`。
双 Gate/Drain 运行证据已由 W5B 的证据提交（`f90b11b`/`b2f65e2`）一并覆盖。

## 5. W5B 完成摘要

W5B 按原目标全部落地，状态 `COMPLETED`：

- `BatchRenewSubscriptions` 完整 Fence 批量续租、Hub 惰性过期不可复活、
  Gate 本地单调 Deadline（renew-start + accepted TTL - safety margin）、
  越过 Deadline 后 REJOIN_REQUIRED → 精确终止 → H5 Fresh Join；
- Gate/Hub 双侧指标（有界标签）与 fail-closed 纪律；
- 运行证据（handoff §4）：双 Gate/Fresh Join（`f90b11b`）、Drain（`b2f65e2`）、
  200 人续租 4000 次/p99≤5ms/零过期零 REJOIN（`22b84e9`）、
  GateBoot disabled 退役翻转（`dc235ef`）；
- GateBoot Reaper 现为三态开关（enabled/shadow/disabled），disabled 模式
  完整 E2E 全绿——订阅生命周期已由租约独立承担。

详细边界与证据：`docs/verification/w5b-subscription-lease-handoff.md`。

## 6. 当前已知后续门槛

1. W31-B/C/D：生产 Redis Lua、Repository、Unknown Resolver 和 Persistence Lane 尚未接线；
2. W11B-S/W：最终 17 类 CommandCatalog、Typed Wire、FarmFrame sequence、容量错误码尚未完成；
3. W31-E：Workflow/Order/DirectDispatcher/Worker 必须等待存储与 Participant IDL；
4. W32：核心经营需按最终合同重新接线和故障证明，不能直接继承旧 v1.21 完成声明；
5. 33A：好友非 Actor Authority 物理合同尚未冻结；
6. W33：旧好友实现不满足 Token Generation、Quota Epoch、AUTHORIZING Join、ActionAdmission 和 Result Anchor；
7. 200 人、TCONND、Failover、Soak 和 3000 万 DAU 仍未取得正式证据（60 号范围）。

## 7. 仓库治理

- 过时 `access-session-s01-s06-execution-plan.md` 已删除，场景验收只引用 60；
- 已清理误提交的根目录和 `deploy/*/` ELF；
- `.gitignore` 已覆盖 `/farm-gate`、`/player-svr`、`/farm-svr`、`/farm-worker`、`/tcaplus-verify`、
  `/bin`、`/.bin`、`/.local-build` 及已知 deploy 临时二进制；
- 构建必须使用临时输出目录或 Docker build context，不能再次把 ELF 写回受跟踪路径；
- W5B 的 `farm-game-w5b` worktree 及本地/远端分支已清理（内容全部并入 integration）。

## 8. 验证与版本控制纪律

每个工作包独立 Commit，交接至少记录：合同条款、改动文件、兼容影响、执行命令、原始结果、未执行项、
故障注入、指标、回滚方式和后续依赖。

最低验证按改动面选择：

```bash
go test ./... -count=1
go vet ./...
go test -race <affected packages> -count=1
./scripts/gen-proto.sh && git diff --exit-code -- api/gen
H5 npm test / build:web（公开协议或客户端变化时）
真实 Redis / 多进程 / 双 Gate E2E（对应合同需要时）
```

任何未执行项必须明确标为 `PENDING/NOT_RUN`，不得写成通过。正式场景证据按 60 的 Run Manifest 绑定
backend/h5 commit、dirty 状态、镜像、配置、transport、Redis target、run_id 和原始日志。

## 9. 接手阅读顺序

1. `docs/design/CONTRACT-MANIFEST.md`；
2. 当前工作包涉及的 26～33/33A；
3. `docs/design/70-w0-contract-alignment-implementation-plan.md`；
4. `docs/design/60-access-session-test-acceptance-freeze.md`；
5. 本文；
6. 当前工作包 verification handoff 与源码测试；
7. 需要启动环境时再读 `s2-handoff.md`，其历史状态不覆盖现行合同。
