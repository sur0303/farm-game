# ROUTE FOUNDATION MVP — INTEGRATION POST-MERGE HANDOFF

## 1. Merge

```text
integration ref     = refs/heads/codex/integration-20260803
old integration     = 1ae95ceba0040f7ed788537f312d2125ce209498
premerge head       = 1e2f6bce50b91f0b90e3a6075b5225885d56ad6f
tested merge commit = 10b80d1b844edc22c8b6acb7254900012979e82a
merge parent 1      = 1ae95ceba0040f7ed788537f312d2125ce209498
merge parent 2      = 1e2f6bce50b91f0b90e3a6075b5225885d56ad6f
final head          = resolve refs/heads/codex/integration-20260803
```

用户已明确授权合入。控制器从远端 integration 基线创建独立 merge worktree，使用
`--no-ff` 合入冻结 Premerge；合并无冲突。主 integration worktree 中既有无关
`tools/agentctl/__pycache__/` 未被读取、暂存或提交。

## 2. Static post-merge Gate

以下命令在 `10b80d1` merge commit 上真实 Exit 0：

```text
go test -buildvcs=false -count=1 -timeout=600s ./...                 PASS
go test -race -buildvcs=false -count=1 -timeout=600s \
  ./internal/authoritycomposition/... \
  ./internal/storage/redis/authority/... \
  ./internal/logic/actor/authority/... ./cmd/player-svr ./cmd/rfobs1-smoke  PASS
go vet ./...                                                        PASS
go build -buildvcs=false ./cmd/...                                  PASS
git diff --check                                                    PASS
git fsck --no-dangling                                             PASS
```

附加 Gate：

- 新增 Git blob `>1 MiB = 0`；
- Secret/Endpoint 扫描无 credential、private key 或真实 Endpoint；
- merge parents 与远端授权候选精确一致；
- post-merge evidence 提交仅改 `docs/verification/**`，运行时代码无漂移。

## 3. Company Proxy Redis post-merge Gate

所有工具均使用双环境门、唯一 namespace、精确 key DEL + per-key EXISTS；无
KEYS/全局 SCAN，无 SCRIPT FLUSH，Endpoint/secret 不写入 Git。

| Gate | 结果 |
|---|---|
| Q2R-2A Repository/Lua | PASS：9 个语义测试；CLUSTER profile 因 company-proxy 隐藏命令按冻结语义 SKIP/PROBE_PENDING |
| Q2R-2B1 Real Process | PASS：S1、S2A、S2B、S3、S4 全通过 |
| Q2R-2B2 Network Fault | PASS：N1–N6 全通过，无 Fatal |
| RF-OBS H1 | PASS：丢 4 个 Renew reply；直接 Local Fence；余量 4595ms；约 1808ms 后同 generation 恢复 |
| RF-OBS H2 | PASS：直接 Local Fence；余量 4595ms；约 1808ms 后同 generation 恢复 |
| RF-OBS H3 | PASS：余量 1052ms，晚于 latest safe；保持 LocalDeadline Fail Closed；无 Fatal |

Recovery budget 为 `2163ms`。H1/H2 满足 latest-safe 前同代恢复；H3 使用
latest-safe 后安全 Fail Closed 承诺。命令证据仅声明 direct local fence 期间无成功
业务 mutation，不声明 ingress-before-mailbox rejection。

## 4. Evidence

```text
docs/verification/results/route-foundation-integration-20260808T132300Z/
├── run-manifest.json
├── q2r2b2/{run-manifest.json,verdict.json}
└── rfobs1h/
    ├── H1-selective-renew-loss/{verdict.json,recovery-budget.json}
    ├── H2-all-response-early-restore/{verdict.json,recovery-budget.json}
    └── H3-no-forward-late-restore/{verdict.json,recovery-budget.json}
```

Premerge 的完整功能、Review、边界和证据索引见：
`docs/verification/route-foundation-mvp-premerge-20260808-handoff.md`。

## 5. Result and frozen boundary

```text
CP-ROUTE-FOUNDATION-MVP

merge                         = PASS_NO_CONFLICT
full_repository_test          = PASS
authority_race                = PASS
go_vet                        = PASS
cmd_build                     = PASS
q2r2a                         = PASS_WITH_CLUSTER_PROFILE_SKIP
q2r2b1                        = PASS
q2r2b2                        = PASS
rf_obs_h1_h2_h3              = PASS
large_object_gate             = PASS
secret_endpoint_gate          = PASS
status                        = INTEGRATION_POSTMERGE_PASS
```

本次合入仍只交付 Static Route Foundation。AssignmentStore、Remote Watch、
RouteManager 控制面、routectl、人工迁移、Kubernetes/Autoscaler 和自动扩缩容仍未
实现；后续开发必须从本次新 integration head 建立新分支，不得继续修改冻结 Premerge。
