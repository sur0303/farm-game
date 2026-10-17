# 13 · 核心 Loop 服务联动图

本文只画主干关系；字段、失败语义和状态机见 [双 Actor 设计](./16-player-farm-actor-split.md)、[路由迁移设计](./17-uc-routing-migration-adaptation.md) 与 [关键时序](./41-sequences.md)。

---

## 1. S2 系统骨架

```mermaid
flowchart LR
    H5["H5"]
    T["TCONND BY_WS/WSS"]
    GP["gateproxy"]
    G["farm-gate<br/>Auth / Session / Frame / Backpressure"]
    I["farm-logic Ingress<br/>Admission"]
    R["ActorRouteAdapter<br/>routing v2 / 双 Region"]
    P["PlayerActor(self)<br/>空 PlayerCore"]
    F["FarmActor(self farm)<br/>空 FarmCore + Presence"]
    PS[("Tcaplus PlayerCore")]
    FS[("Tcaplus FarmCore")]
    B[("AccountBootstrap")]

    H5 --> T --> GP --> G --> I --> R
    R --> P --> PS
    R --> F --> FS
    I --> B
    F -->|"Snapshot/FarmChange"| G
```

S2 先证明：登录、双记录建档、连接、Session、ActorKey 路由、两类 Actor 激活、Join、Snapshot 和重连成立。

## 2. 个人成长 Loop

```mermaid
flowchart LR
    BUY["购买种子"]
    PLANT["播种"]
    WAIT["等待 mature_at"]
    HARVEST["收获"]
    REWARD["库存 / 经验"]
    LEVEL["升级 / 解锁"]

    BUY --> PLANT --> WAIT --> HARVEST --> REWARD --> LEVEL --> BUY

    PA["PlayerActor<br/>Wallet / Inventory / Progression"]
    FA["FarmActor<br/>Plot / CropCycle / FarmVersion"]
    WF["Worker<br/>跨 Actor 恢复"]

    BUY -.内存原子 + 异步回写.-> PA
    PLANT -.Player→Farm→Player.-> WF
    WAIT -.时间派生.-> FA
    HARVEST -.Farm→Player.-> WF
    REWARD -.权威.-> PA
```

核心变化：

- Buy 是 PlayerActor 内 EconomyTxn 原子修改，随后 DirtyModel 异步增量回写；
- Plant 是 Player 预留、Farm 种下、Player 确认；
- Mature 不产生定时状态写；
- Harvest 是 Farm 结束作物、Player 到账。

## 3. Plant 联动

```mermaid
flowchart LR
    C["H5 Plant"]
    G["farm-gate<br/>CommandRouteDirectory"]
    PC["farm-svr<br/>FarmOperationCoordinator"]
    WF["CreateIfAbsent<br/>PlantWorkflow"]
    P1["PlayerActor<br/>Reserve seed + PlantIntent"]
    F["FarmActor<br/>ApplyPlant + PlantReceipt"]
    P2["PlayerActor<br/>Consume or Release"]
    W["farm-worker<br/>timeout recovery"]

    C --> G -->|"FARM:farm_id"| PC --> WF --> P1 --> F --> P2
    P1 -.PendingOutbox.-> W
    F -.OutcomeOutbox.-> W
    W --> WF
    W --> F
    W --> P2
```

`FarmOperationCoordinator` 只推进正常路径，不直接修改玩家种子或农场地块。它必须先可靠创建 `PlantWorkflow`，再调用 PlayerActor/FarmActor Participant；超时后由 farm-worker Claim 同一 Workflow 继续。种子属于 PlayerActor，只决定 `ReserveSeed` 的裁决权，不决定 Coordinator 必须部署在 player-svr。永久地块冲突释放预留，暂时失败保持原 workflow 重试。

## 4. 经济 Loop

```mermaid
flowchart LR
    SEED["种子"]
    LAND["FarmActor 土地生产"]
    CROP["收获物"]
    SELL["出售/订单"]
    COIN["金币"]
    BUY["购买种子"]

    SEED --> LAND --> CROP --> SELL --> COIN --> BUY --> SEED

    PA["PlayerActor<br/>种子/收获物/金币"]
    FA["FarmActor<br/>地块/作物"]
    WF["Harvest Settlement"]

    SEED -.权威.-> PA
    LAND -.权威.-> FA
    CROP -.Farm→Player.-> WF
    SELL -.内存原子 + 异步回写.-> PA
    COIN -.权威.-> PA
```

防刷要点：

- 客户端不提交产量和价格；
- CropCycle 在 FarmActor 冻结规则；
- HarvestReceipt 防止重复到账；
- Buy/Sell 使用 operation_id 防重；
- PROCESSING 操作不能换 operation_id 重做。

## 5. 社交 Loop

```mermaid
flowchart LR
    FRIEND["好友列表/摘要"]
    JOIN["Join 好友农场"]
    OBSERVE["查看成熟与事件"]
    ACTION["偷菜 / 浇水 / 除虫"]
    FEEDBACK["实时 FarmChange / 奖励"]
    RETURN["回自己农场经营"]

    FRIEND --> JOIN --> OBSERVE --> ACTION --> FEEDBACK --> RETURN --> FRIEND

    AUTH["FriendshipAuthority"]
    FA["目标 FarmActor<br/>Presence + 农场裁决"]
    PA["访客 PlayerActor<br/>奖励到账"]

    JOIN -.权限.-> AUTH
    JOIN -.同场.-> FA
    ACTION -.目标事实.-> FA
    ACTION -.跨 Actor 奖励.-> PA
```

好友摘要只是候选展示，不能代替 FarmActor 对当前作物、版本和配额的最终裁决。

## 6. Join 与实时同步

```mermaid
flowchart TD
    J["JoinFarm(target_farm_id)"]
    A["FarmActor 创建 Joining<br/>记录 SnapshotVersion=V"]
    S["Snapshot(V) 进入订阅 FIFO"]
    B["JoiningBuffer 收集 V+1..."]
    Q{"整批 QueueAccepted?"}
    I["InFarm<br/>继续实时 FarmChange"]
    X["订阅失效<br/>Resync / 重连"]

    J --> A --> S
    A --> B
    S --> Q
    B --> Q
    Q -->|是| I
    Q -->|否/超限/超时| X
```

Presence 在 FarmActor 里是因为 Join、业务变化和广播版本必须对齐；它不是 Tcaplus 长期事实。

## 7. 护理奖励

```mermaid
flowchart LR
    B["好友 B"]
    FA["FarmActor(A)<br/>解决 Incident"]
    FC[("FarmCore<br/>CareRewardOutbox")]
    W["farm-worker"]
    PB["PlayerActor(B)<br/>经验/任务 + Receipt"]
    PC[("PlayerCore(B)")]

    B --> FA --> FC
    FC --> W --> PB --> PC
```

第一位有效护理者改变 Farm；其他人得到 NO_CHANGE。Farm 提交不等待奖励到账。

## 8. 偷菜结算

```mermaid
flowchart LR
    B["访客 B"]
    FA["FarmActor(A)<br/>成熟/机会/保底/宠物裁决"]
    FC[("FarmCore(A)<br/>扣减 + Settlement + Outbox")]
    W["farm-worker"]
    PB["PlayerActor(B)<br/>Inventory + LootReceipt"]
    PC[("PlayerCore(B)")]
    DONE["FarmActor(A)<br/>Settlement COMPLETED"]

    B --> FA --> FC --> W --> PB --> PC
    PB --> W --> DONE
```

未成熟直接拒绝。目标已扣减但访客暂未到账时显示 PROCESSING/奖励到账中。

## 9. 任务与邮件

```mermaid
flowchart LR
    LOCAL["PlayerActor 本地事件<br/>购买/出售"]
    REMOTE["FarmActor 事件<br/>收获/护理"]
    RECEIPT["PlayerActor Receipt"]
    TASK["TaskProgress"]
    CLAIM["TaskClaim / MailClaim"]
    ASSET["Wallet / Inventory / Progression"]

    LOCAL --> TASK
    REMOTE --> RECEIPT --> TASK
    TASK --> CLAIM --> ASSET
```

Player 本地 P-ASYNC Buy/Sell 在经济快照保存成功后，通过 post-save effect 推进 TaskModel，丢失窗口由近期 PurchaseReceipt/Reconcile 修复；唯一领奖必须强制持久化。Farm 触发必须用 Receipt 可靠防重，不能跨 Actor 直接改 PlayerCore。

## 10. 演进边界

```text
S2  双 Actor 空骨架 + Join
↓
S3a Buy（Player EconomyTxn + DirtyModel）
↓
S3b Plant（Player→Farm→Player）
↓
S3c Mature + Harvest（Farm→Player）
↓
S4  好友 Join + FarmChange + Care
↓
S5  CareReward + Theft
```

这条顺序先用最小业务验证每一种一致性形态，再扩玩法，避免同时铺开多条半成品链路。
