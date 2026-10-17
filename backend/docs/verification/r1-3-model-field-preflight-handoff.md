# R1-3 Actor Model Field V2 Preflight Handoff

日期：2026-08-09  
冻结 Backend 基线：`ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c`  
阶段：M0 observation/schema；没有启用或实现 v2 数据迁移。

## 交付结论

当前 Player/Farm 的每次变化 Commit 都提交完整 ModelState：Player 为 `record + public_profile`，Farm 为
`record + opportunity_header + opportunity_facts`。内嵌业务/Participant Receipt 位于完整 record 内，外层
Authority Receipt 另行追加；CARE 成功还原子创建一条 SourceOrder。这是当前命令写放大的主要来源。

已经交付三类可验证物：

1. 生产 authority 模式下的非裁决 Prometheus 指标；
2. 独立、只读、失败可审计的 `actor-layout-inspect`；
3. 基于真实 Go mutation 的 Model Map/命令矩阵，以及不产生双真值的 v1→v2 后续方案。

没有变更 Authority Redis key/layout、Lua、Proto、Gateway、Friendship、Route、H5 或 Actor 业务结果；没有
Claim/Commit 型迁移工具、双读双写、SoftSave 或 blob 删除。

## 文件与证据

- 设计：[`actor-model-field-v2-schema.md`](../design/actor-model-field-v2-schema.md)
- 命令矩阵：[`command-model-mutation-matrix.json`](../design/command-model-mutation-matrix.json)
- 本地 fixture baseline：[`actor-layout-v1-baseline.json`](actor-layout-v1-baseline.json)
- 指标：`internal/observability/actorlayout/metrics.go`
- 非裁决 Store 装饰：`internal/storage/authoritystore/observation.go`
- 检查器：`cmd/actor-layout-inspect/**`
- 真实编码锁定：Player/Farm `model_layout_preflight_test.go`

命令矩阵覆盖所有 `commandcatalog` 当前 Enabled public command，并重点列出 BUY、SELL、CLAIM_TASK、
UNLOCK_PLOT、PLANT、HARVEST、CARE/WATER、THEFT；另外列出 legacy `FarmActor.Water` 和产品 Participant
`APPLY_GUARD_SNAPSHOT`。Pet/Mail 明确 deferred。Hero/Task 的组合仍由 Controller 门决定，本 Writer 没有
替代该裁决。

## 指标实际实现范围

Player/Farm server 在 authority mode 中把同一个 observer 安装到 Authority Commit seam 和 go-redis Hook。
observer 不修改请求，不返回准入结论，Store 的原始 `CommitResult` 与 error 原样透传；可选 physical
capability/projector 接口集合也精确保留。

| 指标 | 已实现范围/降级 |
|---|---|
| `actor_record_serialized_bytes{domain}` | 候选 `model:record` value bytes |
| `authority_commit_request_bytes{domain,command}` | Store 已有 bounded logical commit accounting |
| `authority_commit_response_bytes{domain}` | storage-independent logical CommitResult accounting |
| `receipt_bytes{domain}` | Redis projector 可用时为 exact physical outer receipt LP bytes；否则 logical |
| `order_bytes{domain}` | 每条 AttachedOrder logical envelope；落库 Hash 总字节由 inspector 测 |
| `model_state_bytes{domain}` | field-name + serialized value bytes |
| `serialization_duration_ms{domain}` | request/response sizing + physical receipt projection；不冒充上游 protobuf marshal 时间 |
| `redis_round_trips{domain}` | 只计同一 Commit context 的实际 Process/Pipeline dispatch；共享 client 的其他命令不计 |

`command` 只有 `AR1_RECORD_COMMIT`、`AR1_INITIAL_INSTALL`、`OTHER`。Store 看不到可信 BUY/SELL 等业务名，
所以没有从 payload/operation id 猜标签。没有 operation/player/actor id 标签。

## Inspector 操作合同

```bash
go run ./cmd/actor-layout-inspect \
  --redis-url "$ACTOR_INSPECT_REDIS_URL" \
  --namespace '<environment>:<zone>' \
  --sample-players 1000 \
  --sample-farms 1000 \
  --out actor-layout-v1-sample.json
```

- 连接探测为 `PING`；采样 Store spy 只允许 `SCAN`、`HGETALL`；
- 不构造 Actor Registry，不调用 Claim/Fence/Commit/EVAL，不修复、激活或迁移 Actor；
- Redis URL 不进报告，逐项错误和连接错误再次清除 URL/password；不输出 record/receipt/order payload；
- `0` = 请求样本完整，`2` = partial 且报告已写、失败逐项列出，`1` = 参数/连接/输出 fatal；
- 每个选中 authority bucket 最多扫描 10,000 个 SourceOrder，超限是 partial；
- 样本选择是 bounded SCAN 首批 key，`representative=false`；Redis Cluster 情况只覆盖 `--redis-url`
  指向节点可见的 key，需要按授权 topology 分节点执行和合并，不能宣称全群体；
- HGETALL 是只读但会读取选中 Hash 的全部字段，运行方需按实例负载设置小样本、分批执行。

任何单项采样失败只影响这个独立 CLI 的 partial 报告，不进入 Player/Farm 服务路径。

## Baseline 限制

工作区没有获授权的 Redis endpoint；沙箱还禁止创建 loopback listener。因此 checked-in baseline 不是 live
sample，而是各 1 个由生产 model builder 生成的 deterministic initial fixture：

| Domain | samples | `record` value | ModelState logical | 其他 model value |
|---|---:|---:|---:|---|
| Player | 1 | 81 B | 119 B | `public_profile=18 B` |
| Farm | 1 | 249 B | 358 B | `opportunity_header=48 B`, `opportunity_facts=20 B` |

它只能捕获 codec/fixture drift，不能用于生产容量、P95/P99、Receipt/Order 或命令写放大结论。上线任何 v2
开关之前，必须用 inspector 获取授权生产样本并归档原始 JSON。

## 验证记录

通过：

```text
go test ./internal/storage/authoritystore ./internal/observability/actorlayout ./cmd/actor-layout-inspect
go test ./internal/player/actor ./internal/farm/actor -run 'Test(Player|Farm)V1InitialFixtureSerializedBytes$'
go test -run '^$' ./...
go test -race ./internal/storage/authoritystore ./internal/logic/actor/authority ./internal/observability/actorlayout ./cmd/actor-layout-inspect
go test -race ./internal/player/actor ./internal/farm/actor -run 'Test(Player|Farm)V1InitialFixtureSerializedBytes$'
go test -race -run '^$' ./internal/storage/redis/authority ./internal/authoritycomposition ./cmd/player-svr ./cmd/farm-svr
go vet ./...
python3 -m json.tool docs/design/command-model-mutation-matrix.json
python3 -m json.tool docs/verification/actor-layout-v1-baseline.json
git diff --check
```

`go test ./...` 已实际执行，但沙箱返回非零：216 个 top-level tests（14 个 package）在启动 miniredis、
loopback HTTP/WebSocket/gRPC listener 时得到 `listen tcp 127.0.0.1:0: socket: operation not permitted`；日志中
共有 261 个该环境拒绝。全包 compile-only、无 socket 的目标测试、authority 相关 race 和 vet 均通过。
需要在允许 loopback socket 的 Linux runner 上重新执行 full `go test ./...`，才能给出全量 PASS。

本 Writer 环境还把 worktree 的 Git metadata 挂载为只读；`git add` 在创建
`.git/worktrees/.../index.lock` 时返回 `Read-only file system`。因此本环境不能真实创建 commit 或清理
working tree；不得把临时仓库 hash 冒充本分支提交。源码交付和上述验证结果保留在当前 working tree。

## 后续门禁

1. Controller 确认 Hero/Task 组合与 Task legacy carrier 去留；Pet/Mail 继续 deferred；
2. 取得生产 Player/Farm/Receipt/Order 样本和指标 P50/P95/P99；
3. 冻结 V2 codec、manifest、转换/回滚 golden fixtures；
4. 单独审阅未来 sparse mutation Lua、`TIMEOUT_UNKNOWN` 和灰度/回滚实现；
5. 只有上述通过后，才讨论 dual-version reader 与 `write-v2`；M0 不携带该开关。
