# RT1-R0-BASELINE-RECONCILIATION（只读核验报告）

- 任务 ID：RT1-R0-BASELINE-RECONCILIATION
- 模式：READ_ONLY（核验期未修改任何文件；本报告由 I0 随 CP-ROUTE-FOUNDATION 落盘）
- current integration：`1ae95ce`（branch `codex/integration-20260803`，tag `cp-vertical-a-20260807`）
- 核验方法：git ancestry / patch-id / merge-tree 预检 + 克隆顺序 cherry-pick 模拟 + 全量构建测试 + 旧分支干净性与定向测试

## 1. 旧 Route branch / head

| 任务链 | 分支 | head |
|---|---|---|
| RT1B-1 | `codex/rt1b-1-zero-shard-readiness-20260806` | `d20144c` |
| RT1B-2A/2B | `codex/rt1b-2a-shard-membership-core-20260806` | `9819880` |
| RT1B-3A(+FIX) | `codex/rt1b-3a-real-membership-adapter-20260806` | `478b60c` |

旧分支 worktree 干净性：`farm-game-rt1b-3a-real-membership-adapter-20260806` `git status` 无脏文件，head=478b60c；定向测试（authoritycomposition + logic/actor/authority）PASS。

## 2. Candidate commit chain（严格线性，merge-base=935b51e）

```
935b51e (= 7ed8161^ = merge-base with 1ae95ce)
└─ 7ed8161  RT1B-1   split ProcessReady/ShardReady, zero-shard holder startup
   └─ d20144c  RT1B-1 F1  gate static startup on local holder registration
      └─ f024de4  RT1B-2A  local shard-membership reconciler core
         └─ 425f740  RT1B-2B C1  fence membership topology + generation state
            └─ 9819880  RT1B-2B C2  execute membership ports outside state lock
               └─ 296c721  RT1B-3A  real local membership adapter, generation-fenced ports
                  └─ e7072bc  RT1B-3A FIX C1  two-phase install (Prepare/Commit)
                     └─ 478b60c  RT1B-3A FIX C2  exact-generation deadline refresh + hash fence
```

`git cherry -v 1ae95ce 425ab7c`：34 个 RT1B commit 全部 `+`，无一被 1ae95ce 吸收。

## 3. 每个 commit 修改文件

| commit | 文件 | 规模 |
|---|---|---|
| 7ed8161 | docs/verification/rt1b-1-zero-shard-handoff.md(新)、internal/authoritycomposition/composition.go、composition_test.go | +345/-17 |
| d20144c | 同上三文件 | +225/-5 |
| f024de4 | rt1b-2a-membership-handoff.md(新)、composition.go(+6)、membership.go(新533)、membership_diff.go(新131)、membership_internal_test.go(新300)、membership_test.go(新430)、testkit/ports.go(新183) | +1704 |
| 425f740 | composition.go、membership.go、membership_diff.go、membership_internal_test.go、membership_test.go | +357/-40 |
| 9819880 | rt1b-2a handoff、membership.go、membership_internal_test.go、membership_test.go | +477/-20 |
| 296c721 | rt1b-3a handoff(新)、membership_adapter.go(新226)、membership_adapter_test.go(新420)、internal/logic/actor/authority/generation_fence.go(新97) | +883 |
| e7072bc | rt1b-3a handoff、membership.go、membership_adapter.go、membership_adapter_test.go、membership_internal_test.go、membership_test.go、testkit/ports.go | +540/-131 |
| 478b60c | rt1b-3a handoff、membership_adapter_test.go、generation_fence.go、generation_fence_test.go(新122) | +308/-5 |

## 4. 与 1ae95ce 的差异事实（决定零冲突的前提）

- `internal/authoritycomposition/`（composition.go + composition_test.go）在 935b51e 与 1ae95ce 之间**零差异**。
- `internal/logic/actor/authority/` 在 935b51e..1ae95ce 主线**零改动**；`LeaseGuardRegistry`（dynamic_fence.go）主线未动。
- membership*.go / membership_adapter*.go / testkit/ports.go / generation_fence*.go 在 1ae95ce **不存在**（纯新增）。
- 1ae95ce 全树无 `ShardMembership|MembershipReconciler|ServingShardCount|CanServeShard` 痕迹；docs/verification 无 rt1b-1/2a/3a 文档。

## 5. keep / drop / reimplement 表

| commit | 判定 | 理由 |
|---|---|---|
| 7ed8161 | keep | 上下文在 1ae95ce 完整保留，顺序 cherry-pick 干净 |
| d20144c | keep | 依赖 7ed8161，顺序重放干净 |
| f024de4 | keep | 5 个新文件纯新增 + composition.go 窄接缝 |
| 425f740 | keep | 修改前序新文件，顺序重放干净 |
| 9819880 | keep | 同上 |
| 296c721 | keep | 4 个新文件纯新增 |
| e7072bc | keep | 修改前序新文件，顺序重放干净 |
| 478b60c | keep | 同上 |

**drop：无。已被吸收：无。需重做：无（in-scope 8/8 全部原样保留）。**

## 6. 冲突矩阵（顺序 cherry-pick 实测）

git merge-tree 独立 3-way 预检在 d20144c/425f740/9819880/e7072bc/478b60c 报 modify/delete "冲突"——全部是"文件在 base 存在、在 1ae95ce 不存在"的假象；**顺序 cherry-pick（按 parent→commit diff 应用）实测 8/8 零冲突、零手工解决**。冲突预案：若未来基线变动导致 composition.go 上下文漂移，仅 d20144c/425f740 可能需手工三方合并（这两处与 RT1B-1 触点重叠），其余均为纯新增，天然无冲突。

## 7. proposed CP-ROUTE-FOUNDATION head

- worktree：`farm-game-cp-route-foundation-20260807`（git worktree，branch `codex/cp-route-foundation-20260807`）
- 基线：1ae95ce；重放：`git cherry-pick -x 7ed8161 d20144c f024de4 425f740 9819880 296c721 e7072bc 478b60c`
- 重放后 head：见分支顶端（8 个新 hash，均带 `cherry picked from commit` 溯源行）

## 8. required test commands（验收，全部 PASS 后发布）

```bash
go test ./internal/authoritycomposition/... -count=1
go test -race ./internal/authoritycomposition/... -count=1
go test ./internal/logic/actor/authority/... -count=1
go test ./internal/actorrouting/... -count=1
go test -buildvcs=false ./... -count=1
go build -buildvcs=false ./cmd/...
git diff --check
```

## 9. 下一阶段缺口（R0 核验确认，原样保留待补）

旧链未实现：跨进程 Watch、cmd 接线、自动迁移、Actor Registry 接入。CP-ROUTE-FOUNDATION 不包含这些内容，属后续批次（RT1W/RT1I/RT1M/RT1F）。
