# Care P1 修复 Handoff（2026-08-05）

分支：`codex/care-p1-fix-20260805`
基线：`3984f8851fe545e078b30f0c7f13bd471c53a406`
工作树：`farm-game/.worktrees/care-p1-fix`（原 `farm-game-care-integration-20260805` 对当前用户不可写，故从同提交新建可写分支）

## 已修复（独立审查 9 P1 中的优先项 + 可安全落地项）

1. **好友 ActionAdmission**
   Authority 好友 CARE 改为 `PairRepository.CommitFriendActionAdmission`；FarmActor 校验冻结 `ActionPermitSnapshot` 绑定，不再依赖 HMAC 短 TTL permit。`farm-svr` Authority 模式装配 Redis Pair Authority。

2. **完整订阅 Fence**
   FirstHop 对 `RequiresCurrentSubscription` 命令强制：`admitted_farm_id`、非空 `admitted_subscription_id`、完整 SessionPermit（epoch/gateway/connection/generation/route revision）。

3. **Authority 默认模式**
   `ACTOR_AUTHORITY_MODE` 空值默认 `authority`；仅显式 `legacy`/`rollback` 进入 Legacy。

4. **公网 CareResult**
   扩展 `public.CareResult`（32 §5.4 字段），Authority CARE 出口序列化该类型，不再回传内部 `FarmCommandResult`。

5. **Claim deadline / retry fence**
   新增 `ClaimWorkV3`（deadline + max_retry → `RECONCILE_REQUIRED`）；保留并注册冻结的 `ClaimWorkV1/V2`、`CompleteWorkV1`。

6. **DueIndex Care 饥饿**
   Care 扫描预算提高到 65536；malformed member 在 Care 扫描中安全 ZREM。

7. **farm-worker Care 恢复 fail-closed**
   Authority 模式下缺路由/Recovery client/构造失败时启动 Fatal。

8. **Worker deferClaim attempt**
   坏 payload 路径使用 `claim.RetryCount+1`，不再写死 `1`。

## E2E 期间额外修复（真实多进程暴露）

9. **plantonline RouteFence 识别 APPLY_CARE_REWARD**
   `routeDomain` 未包含 Care，player-svr fence 拒绝 Care Participant。

10. **Care 不走 generic WorkRepository proof**
    `ExecuteStep`/`ResumeStep` 对 Care 跳过 WorkVersion/WorkLease；`participantproof` 的 WorkAddress/IdentityBinding 对 Care 也不再要求不存在的 `WorkRecord CARE_REWARD/*`。来源证明只走 `CareDependencyVerifier`（Authority SourceOrder）。

## E2E 期间额外修复（好友多进程）

11. **ActionAdmission record_version 刷新**
    `CommitFriendActionAdmission` 改为 `commitAllowRecordVersionRefresh` + 有界重读 Pair（对齐 quota），避免同 Pair 并发准入把 version conflict 升级成幂等冲突。

12. **CareRewardOrder subject = carer**
    `AttachedOrder.SubjectPlayerID` + `CommitActorMutationWithOrdersV5` 按订单写入 `subject_player_id`；好友 CARE 不再把 Farm ActorID 错写成 SourceOrder subject（否则 worker `subject player mismatch` → RECONCILE_REQUIRED）。

## 仍未闭合

- **P1-5 Care 长期证明 / 7 天 receipt GC / tombstone**：改动面大，未在本轮落地。
- 公司 Redis 当前为 **STANDALONE_OR_PROXY**（master + 1 replica，`cluster_enabled=0`），不是 Redis Cluster；MOVED/ASK 与真集群槽位证据仍缺。

## 验收

单元/组合：

```text
go test -count=1 ./internal/authoritycomposition ./internal/logic/trusted \
  ./internal/farm/actor ./internal/farm/ingress ./internal/storage/redis/authority \
  ./internal/worker/carerecovery ./internal/player/participant ./internal/player/task \
  ./internal/plantonline ./internal/workflow/participantproof \
  ./cmd/farm-svr ./cmd/player-svr
```

真实 Redis（公司 `198.18.0.14:6380`，密码不入库；通过 `FARM_REDIS_CONFIG` / `FARM_REDIS_URL`）：

```text
# 生产探针：TIME / EVAL / EVALSHA / NOSCRIPT / WAIT
docs/verification/results/care-redis-production-probe-20260805.jsonl
# cluster_mode=STANDALONE_OR_PROXY；eval*_assessment=PASS

# Authority Claim + Care SourceOrder Claim/Complete live
FARM_REDIS_URL=... go test -count=1 -v ./internal/storage/redis/authority/ -run 'Live'
# 归档：docs/verification/results/care-worklease-live-20260805.log

# 多进程好友 Care E2E（8 好友抢同一 WATER Incident）
PO1_E2E_MODE=CARE FARM_REDIS_URL=... go run ./cmd/po1-plant-e2e
# 归档：docs/verification/results/care-friend-multiprocess-e2e-20260805.log
# PASS: Pair ActionAdmission ×8 → 1 SUCCEEDED + 7 NO_CHANGE → CareRewardOrder COMPLETED；winner exp 0→1
```
