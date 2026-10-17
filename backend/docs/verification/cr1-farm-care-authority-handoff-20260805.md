# CR1：CARE FarmActor 权威路径 handoff

基线：`3804c5d511674137facd3bdc24d8f814fb271725`

本包把 Authority Registry 下的 CARE 从 `Watered/FarmSettlement/PROCESSING` 迁移到
`FarmActor.ResolveCareIncident`。非 Authority Registry 仍保留原 CARE 适配，仅作为兼容窗口；Authority
生产入口不会创建 legacy settlement，也不调用 Player 奖励。

## 实现要点

- FarmActor 复用 `internal/farm/domain.DecideCare`，接通 WATER 与 REMOVE_PEST；owner 绑定和有效好友
  permit/RelationEpoch 分支均在同一 mailbox turn 内复核。
- Incident 首次成功写 RESOLVED 与奖励义务；同 Incident 后续请求写终态 NO_CHANGE，不推进 FarmVersion、
  不生成 FarmChange、不给第二个 CareRewardOrder。due 只由 `trusted_now_ms` 判断；成熟不拦截，Harvest
  后的 ended cycle 拒绝。
- 通过通用 `CASEpochWithOutcomeAndOrders` 接缝一次提交 FarmRecord、确定性 CARE proof（承载
  CareOperationReceipt/Participant 业务字段）、StoredOperationOutcome、Plot/Farm 版本、Opportunity
  header/dirty generation、CareRewardOrder(PENDING) 及 DueIndex。Order payload 保存完整
  CareResolutionSnapshot 与 resolution hash，顶层成功不等待 Player。
- AttachedOrders 的 canonical hash 包含订单/终态，覆盖原子提交、精确重放、订单冲突、TIMEOUT_UNKNOWN、
  旧 writer/record version；旧 Plant/Harvest 的无订单 CASEpoch 行为保持原路径。
- 提交前安装既有 PostCommitDeliveryGuard；成功只消费一个候选 FarmChange。投影发送和 Player 奖励不在
  本包实现。
- CARE proof 使用现有冻结 Proto 的 `FarmOperationReceipt.ResultPayload` 载体，并校验内外层
  operation/hash/status、snapshot 字段和 resolution hash；没有 Proto/generated 文件改动。

## 验证

新增/覆盖：

- 同 Incident 20 并发：恰好一个 SUCCEEDED、19 个 NO_CHANGE、一个奖励订单；owner/friend、WATER/REMOVE_PEST、
  成熟后护理；未到期和 Harvest 后拒绝。
- AttachedOrders 原子性、精确重放、订单冲突、TIMEOUT_UNKNOWN、旧 record version 拒绝。
- Authority CARE ingress 不写 `ActiveSettlements`/`RecentCareAttempts`，并返回单一候选变化与订单信息。
- response-loss 重放返回原 SUCCEEDED，不增加第二份订单。

最终验证命令均通过：

```text
go test -count=1 -timeout=600s ./...
go test -race -count=1 -timeout=600s ./...
go vet ./...
git diff --check
```

全量 race 中曾有一次既有 `internal/friend/TestConcurrentAddBothDirectionsAndDelete` 的偶发并发断言失败；
该测试单独 `-race -count=3` 通过，随后顺序全量 race 通过。该问题不涉及本包改动。

明确不包含：Player 奖励落账、Worker/MQ、新服务、Proto 或投影发送改动。

最终提交：以当前分支 `git rev-parse HEAD` 为准（提交标题：`feat(farm): cut care over to authority actor path`）。
