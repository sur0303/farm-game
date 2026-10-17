# Theft 最终组合验证记录（2026-08-05）

状态：`COMBINED_PASS / NOT_YET_MERGED`

验证分支：`codex/theft-integration-20260805`

组合 HEAD：`ba4234c`

## 本轮合并的收口修复

- `f8cee3b`：关闭 `RESULT_PUBLISH_ORDER` 生命周期，恢复全部终态的公开查询，并精确释放 Pair pending count/bytes/DueIndex。
- `558e0cb`：将 GuardSnapshot 通过来源证明、路由与 writer fence 提交到 Farm 权威记录，并在提交成功后立即更新热点 FarmActor。
- `ba4234c`：生产/Authority 模式缺失、过短或已知开发 Permit Secret 时启动失败；Gate 与 farm-svr 使用同一校验合同。

## 组合代码门禁

以下命令均在组合分支通过：

```text
go test -count=1 ./...
go test -race -count=1 \
  ./internal/friend ./internal/worker/theftrecovery ./internal/theftonline \
  ./internal/player/ingress ./internal/farm/actor ./internal/farm/ingress \
  ./internal/workflow/participantproof ./internal/storage/redis/authority \
  ./internal/scenario/theft ./internal/permitsecret ./internal/gateway/session \
  ./internal/gateway/auth ./internal/gatewayapp ./internal/gateway/command
go vet ./...
git diff --check
```

## 真实 Redis 结果生命周期回归

Redis：`redis:7.4-alpine`，独立临时实例。

```text
TestPairMixedTerminalLifecycleOver64RealRedis
REAL_REDIS_PASS: 80 mixed terminal Pair operations released all source-order count, bytes, and due members
PASS
```

该回归跨过单 Pair 默认 64 个 pending work 容量，混合覆盖 `SUCCEEDED / NO_CHANGE / REJECTED`，每次均验证：

- `pending_work_count == 0`
- `pending_work_bytes == 0`
- DueIndex 为空

## 200 WSS 热点农场组合复验

证据 Stamp：`20260805T122000Z`。

- 真实进程：`farm-gate / farm-svr / player-svr / farm-worker`
- Redis：独立 `redis:7.4-alpine`
- 公网路径：HTTPS 登录 + WSS + ClientHello + Join + SubscriptionReady
- 连接：200/200 Ready，峰值 200，Join 错误 0
- 并发 Theft：199 个好友同时操作同一成熟作物
- 终态：199/199 可查询，`10 SUCCEEDED + 84 NO_CHANGE + 105 REJECTED`
- 产量守恒：`base_yield=500`，可偷上限 100，恰好 10 次 × 10，最终偷走 20%
- 访客资产：仓库净增加 100；10 份 Farm Receipt 与 10 份 Player Receipt 均存在
- 实时同步：200 个连接共收到 2000 个 FarmChange；每个连接版本 `2..11` 连续，无缺口、无回退、无 RejoinRequired
- Workflow：199 个全部终态，零逾期、零活动 Settlement、同 operation_id 重放未产生重复结算
- 热 Join：200 人 Ready 屏障 44.07ms，约 4537.73/s；Join p99 43.44ms
- Theft 终态 p99：10.86s；该延迟仍是后续性能优化目标，不影响本轮正确性结论

核心证据：

- `results/theft-200-wss-baseline-20260805T122000Z-driver.json`
- `results/theft-200-wss-baseline-20260805T122000Z-audit.json`
- `results/theft-200-wss-baseline-20260805T122000Z-authority.json`
- `results/theft-200-wss-baseline-20260805T122000Z-environment.json`

## 结论与边界

本轮已经证明 Theft 的权威串行裁决、跨 Actor 最终结算、公开结果恢复、同场广播、热点连接容量和热点 Actor 守护热更新可以在同一代码组合中成立。

尚未把以下内容夸大为已证明：

- 最终组合未重跑“强制写满队列并踢掉慢连接”；已有证据只证明 10 次变化下健康连接不受一个慢读连接影响。
- Guard 热更新组合探针使用生产 Redis Adapter + miniredis；200 WSS 基线是无守护配置。宠物拦截的公司 Redis 多进程专项仍可作为后续证据补充。
- 当前 199 并发操作的终态 p99 约 10.86s，需要后续围绕 Worker 调度、Redis 争用和查询轮询优化。

在上述边界下，Theft 已达到“可以进入最终窄审与 integration 合入判断”的状态。
