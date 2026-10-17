# TH1 · FarmActor Theft Authority Handoff

日期：2026-08-05  
范围：Farm Participant / FarmActor / 现有 Authority Commit；不包含 Workflow、Player 入库/金币、好友服务或 H5。

## 已实现证据

- `internal/farm/ingress/participant_server.go`
  - 在现有 Participant ingress kernel 后接入 `DECIDE_THEFT`，复用同一 receipt resolver、capacity admission、FarmActor mailbox 和 observer 线路。
  - 增加 `DecideTheftDependencyVerifier`：Farm 侧在入 mailbox 前要求上游证明 Work/Route/subject、Friend Action Admission、permit 与 Execution-Accept 绑定；缺失证明 fail closed。
  - replay 先于 Work/lease/dependency 重验；成功重放不再次发布 FarmChange。

- `internal/farm/actor/participant.go`
  - `ValidateDecideTheftPayload` 校验 Farm/owner/visitor/plot/cycle、TheftAttemptID、SettlementID、ActionPermitSnapshot canonical hash、Permit 字段和 typed payload hash。payload 没有数量、槽位、宠物结果或单价字段。
  - `FarmActor.DecideTheftWithAction` 在单 mailbox turn 内执行成熟度、ACTIVE cycle、owner 未收获、visitor/cycle Attempt、唯一下一个正槽位和 stealable limit 重检；合法首尝试一次提交 gross 扣减、public stolen_total、TheftAttemptReceipt、typed TheftTargetDecision、GuardDecisionSnapshot、ParticipantReceipt、Opportunity header、版本和 candidate FarmChange。
  - `FARM-DECIDE-THEFT-RESULT-PROOF-V1` 持久化完整 typed result/hash、Decision hash、Guard draw/rule/key/algorithm、gross/lost/net/unit coin/guard coin 和版本；resolver 只返回该 proof，不重新裁决。
  - FarmChange 只通过既有 `PlotView` 暴露地块事实；不携带 visitor、random draw、owner coin 或 visitor inventory；FarmActor 不调用 Player/Workflow。
  - P0 Plant snapshot 复用已有十个 `200bp` 槽及 `base_yield % 50 == 0` 验证；Theft 使用槽位顺序，Harvest 通过 gross/owner claimable 守恒继续兼容。
  - `TheftPRF`/`HMACTheftPRF` 是服务端注入点；无有效宠物时不截留，P0 有效宠物只能 0 或 full gross。guard coin 乘法增加溢出检查。
  - Participant proof 与活跃 CropCycle 不受旧 7 天 GC 删除；legacy TheftAttemptReceipt 在活跃茬次上保留。不同 operation_id 的同 visitor/cycle 仍由 Attempt proof 拒绝。

- `internal/farm/domain/theft.go`
  - 增加 stolen/limit/cycle 不变量、槽位 ratio 边界和 Guard coin overflow 检查；完整截留仍是成功并消耗 Attempt。

## 核心测试

已运行：

```text
go test ./internal/farm/domain ./internal/farm/actor
go test ./internal/farm/actor -run 'TestParticipantDecideTheft|TestValidateDecideTheft' -count=1
go test ./...
```

新增/覆盖：

- 未成熟拒绝不消耗 Attempt；同 visitor 换 operation_id 仍拒绝。
- 多 visitor 并发消费十个 2% 槽，base_yield=250 时 gross=50、FarmVersion/Attempt 数量守恒。
- 无 Guard 与 full-gross Guard；full interception 是 `APPLIED`、net=0、仍消耗机会。
- receipt-first 响应重放、proof hash 固定、actor passivation/reload 后 replay。
- Theft 后 Harvest 得到 245，旧 Farm projection 不会把 gross 归还主人作物之外；Permit hash 伪造被拒绝。
- 既有 domain 表驱动覆盖 immature、同 visitor、十槽 20%、Harvest/Theft 顺序、旧 cycle 和 Guard 二值结果。

## 冻结合同与当前物理模型的差异

- 当前 `FarmRecord/FarmCore` 只有 `stolen_total`，没有独立 `owner_claimable` 字段；实现以 `base_yield - stolen_total` 作为同一 Farm authority 内的派生 claimable，未引入第二份存储语义。
- 当前 Farm proto 没有独立 `FarmActivity` 或 OpportunityHeader 字段；Opportunity header 已按现有 Participant proof/legacy mutation receipt 机制持久化，FarmChange 仍只发公共 PlotView。
- 当前 Farm bucket 没有为 Theft 建立 Workflow settlement/source order；本提交只保存 `settlement_id` 作为冻结 Decision identity，不创建 ActiveSettlement、订单或下游进度，符合 Farm Authority Commit 边界。

## 待集成事项

1. 组合层需要实现 `DecideTheftDependencyVerifier`，接入真实 Friend Action Admission、WorkRecord、Route/Work bucket 和 Execution-Accept proof；本 Farm 包不重新实现好友服务。
2. 组合层需要把 GuardSync 的有效 snapshot 单调安装到 `SetParticipantGuardSnapshot` 边界，并向 Registry 注入部署密钥对应的 `HMACTheftPRF`。当前默认 PRF 仅用于兼容本地 fixture；GuardSync 的 durable physical field 不在现有 FarmRecord proto 中。
3. 下游 `SettleTheftVisitor`/`CreditGuardIncome`、Player inventory/coin 和 Workflow orchestration 留给后续 Participant/Workflow worktree；Farm 只提供冻结 Decision proof。
4. 生产 capacity 仍由现有 ParticipantPhysicalCapacity/Attached Order guard 管理；Participant proof 达到固定容量时 fail closed，尚未实现合同所述 archive/compact 后台作业。

本交接未修改 `docs/design/**`。
