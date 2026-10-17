# QQFarm333 在线热 Shard 迁移 —— PPT 讲稿

> 配套图片：`screenshots/*-annotated.png`（红色竖线 = Forward 20:22:19 / Move-back 20:26:22）。
> 本讲稿只讲**单分片迁移 + 2,000 在线真实玩家**这一个场景。
> 主证据：presence 流量双向零错误（3.15s / 3.15s）。
> 补充证据：完整业务动作（Journey）单分片 Forward 零错误。
> Pod 4→5→4 全量不在 PPT 中展示，被追问时按末尾"边界"一节如实回答。

---

## P1 开场

> 我们验证了一件事：**一个承载 2 万玩家数据、其中 2 千人在线的热点分片，能否在不掉线、零错误的前提下，在两个 Holder 之间来回迁移。**
> 结论：可以。单分片迁移约 3 秒，迁移窗口内约 14.7 万次真实命令全部成功，零失败。

---

## P2 场景与条件

> 构造了一个单点密集场景：
>
> - 20,000 个真实 Player 记录，通过真实登录/加入流程创建，**全部命中同一个虚拟分片 PLAYER shard 0**；
> - 其中 2,000 个通过真实 Login + 主页读取 + 加入自己的农场，保持持续在线；
> - 剩余 18,000 个离线，作为密集持久化数据存在；
> - 把 shard 0 从 `player-svr-0` 迁到 `player-svr-1`，再迁回。

（配图可省略，用数字页即可）

---

## P3 迁移正确性 —— Shard 分布

> 这是迁移最直接的证据。
> 迁移前两个 Holder 各 256 个分片；Forward 后变成 255 / 257——shard 0 精确地从 `player-svr-0` 转移到了 `player-svr-1`；Move-back 后回到 256 / 256。
>
> 全程 1024 个分片总和不变，Holder membership 无 pending、无 degraded。
> 这说明：**迁移是"权威归属"的精确转移，不是数据的复制。**

（图：`shard-distribution-annotated.png`）

---

## P4 在线连续性 —— 分片内 ACTIVE 按 Holder 拆分

> 这张图按 Holder 拆开 Player ACTIVE。
>
> 迁移时：
> - 蓝色线（`player-svr-0`）从 2000 降到 0 —— source 停止服务；
> - 同时另一条线（`player-svr-1`）从 0 升到约 1900 —— target 接管并重新激活；
> - Move-back 时反向再来一次。
>
> 注意：这里展示的是"连接连续 + 状态按需重建"。**迁移不搬运内存里的 Actor 数据**——source 被 fence，target 从持久化状态按请求重新激活，所以 ACTIVE 是"先降后升"，不是瞬时切换。

（图：`player-active-per-holder-annotated.png`）

**如果被追问"为什么过渡期总和超过 2000"：**

> 迁移后 source 的旧 Actor 已失去写权，但还留在内存中等待空闲回收（约 2 分钟 idle），同时 target 的新 Actor 已激活，所以短暂出现"两边都有"。**写权自转移时刻起只属于 target**，旧 Actor 不再接收新命令、写请求也会被 writer_epoch 拒绝。这不是双写。

---

## P5 路由收敛 —— RouteSnapshot Revision

> 迁移期间 RouteSnapshot 的 revision 快速推进，Gate 与所有 Holder 在毫秒级收敛到同一 revision，没有分叉。
> 这说明路由公告与权威归属是一致的。

（图：`route-revisions-annotated.png`）

---

## P6 命令正确性 —— 迁移窗口 14.7 万次命令零失败

> 迁移窗口内（10 分钟观测窗，含两次迁移），在线玩家的持续流量产生了：
>
> - `GET_PLAYER_BOOTSTRAP` 成功：73,696 次
> - `JOIN_FARM` 接受：73,916 次
> - 合计约 14.76 万次命令，**失败、超时、unavailable 全为 0**
>
> 命令正确率面板显示 100%，失败面板为 0，结果矩阵里只有 SUCCEEDED / ACCEPTED 两行。

（图：`command-correctness-rate-annotated.png`、`command-failures-annotated.png`、`command-results-trend-annotated.png`、`command-result-matrix-annotated.png`）

---

## P7 零错误 —— 迁移错误面板

> 这张图是迁移窗口内 Gate 的 backend 拒绝、mailbox 拒绝、degraded shard 计数，**全部为 0**。
> 我们特意保留了空图作为证据：不是没采集，而是确实没有错误。
>
> 少数在迁移瞬间到达的命令，通过"精确的 pre-mailbox 拒绝证明"安全等待并按原请求重试，最终全部成功（指标里的 RESUMED）。

（图：`migration-errors-annotated.png`）

---

## P8 性能 —— 从 23 秒到 3 秒

> 同一场景优化前需要 **23.35 秒**；
> 现在 Forward **3.15 秒**、Move-back **3.15 秒**，约 **7.4 倍提升**。
>
> 核心优化：
> 1. Holder 身份安全缓存——去掉每分片重复的 Kubernetes 发现；
> 2. 在线 Actor 的事实确认并行化（上限 64）——drain 不再逐个串行；
> 3. 单分片手动迁移使用"立即发布"，跳过聚合器等待；
> 4. Gate 在迁移窗口内做有界的、带精确证明的安全等待与重试。

（图可选：`hot-shard-dashboard-annotated.png`，或纯数字表）

---

## P9 原理 —— 写权转移的正确顺序

> 我们的迁移顺序是：
>
> 1. **停止新命令**（StopNewActivationAndTurns）
> 2. **等待已接受的命令执行完**（WaitCurrentTurns）
> 3. **确认所有事实已持久化**（ConfirmFactsCommitted）
> 4. **转移写权**（Authority CAS，writer_epoch/lease_epoch 原子换代）
> 5. 发布 ASSIGNING → target 准备 → 发布 ACTIVE
>
> 关键点：**写权在 source 跑完、持久化确认之后才转移**，不会提前。
> 转移后 target 才拥有新写权；source 的旧 Actor 已不能写，只等待空闲回收。
> 这就是"不掉线、零失败"的结构性原因。

---

## P10 补充证据 —— 完整业务动作（Journey）迁移

> 前面是登录/Join 类流量。我们还用**完整业务动作**（BUY / PLANT / HARVEST / SELL）的 2,000 在线 Bot 执行了一次同样的单分片 Forward 迁移：
>
> - 迁移耗时：5.66 秒（比登录类慢，因为要 drain 真实业务 turn）；
> - 迁移窗口 `command unavailable`：**0**；
> - 迁移后 target 恢复 2000 ACTIVE，source 归 0。
>
> 说明零错误结论对**完整业务命令**同样成立，不只是读/Join。
> 目前仅完成 Forward 单程验证；双向回迁以登录类数据（3.15 秒双向零错误）为准。

（补充证据，若时间紧可跳过）

---

## 边界（被追问时如实回答）

> 1. 本结论是**单分片**迁移。并发 204 分片的 Pod 4→5→4 我们同样验证过，分片分布最终都正确收敛，但迁移窗口仍有少量请求失败，**因此尚未宣称全量 Pod 扩缩容通过**，相关工作在进行中。
> 2. 2,000 在线是真实并发上限；更大的在线规模（如 5 万、10 万）依赖后续实验。
> 3. 迁移约 3 秒是指 Authority/路由状态机时间；旧 Actor 的完整空闲回收约 2 分钟，但不阻塞服务。

---

## 数据来源

全部数字来自：

```text
docs/verification/perf/qqfarm333-hot-shard-20260820/
  RESULTS.md
  artifacts/routeplans-summary-r6.json
  artifacts/farm-gate-0-final.prom / farm-gate-1-final.prom
  artifacts/warm-20k-r6.json
  screenshots/*-annotated.png
```

Prometheus 原始查询和 Pod 状态均在 artifacts 中。
