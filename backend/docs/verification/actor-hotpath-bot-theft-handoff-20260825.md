# Actor 热路径与好友偷菜机器人交接

日期：`2026-08-25`

分支：`codex/actor-hotpath-bot-theft-r1`

状态：已推送独立分支，**未合入 Integration**。

## 1. 本轮目标与结论

本轮完成两项相互独立但可一起用于下一轮压测的能力：

1. 减少 `dirty_best_effort` Actor 热 Turn 中对完整 Player/Farm `ModelState` 的重复克隆；
2. 让真实机器人在初始化时建立好友关系，并能够进入好友农场执行偷菜。

验证结论：

- Actor 热路径改造已通过 Linux 全量单元测试和目标包 `-race`；
- 好友初始化、买种、种植、成熟等待、Join 好友农场和偷菜已在 `uc-test-plus / developer_026-qqfarm` 通过真实四服务与公司 Redis 链路；
- 4 名机器人共建立 4 份 canonical 好友关系、8 条双向索引和 8 个好友额度槽；
- 4 名机器人买种并种植成功，随后分别偷取好友环中下一位玩家的成熟作物，4/4 成功，零拒绝、零超时；
- 当前线上 Runtime Pod 尚未替换为本分支服务端镜像，因此本轮真实链路只证明机器人能力，不把它当成 Actor 优化后的 QPS A/B 结果。

## 2. 提交清单

| Commit | 内容 |
|---|---|
| `b1b4a4e9` | 减少 Dirty Actor 状态克隆 |
| `0439ddf1` | 好友环 Fixture、偷菜目标映射与测试 |
| `afef10f4` | 将 `live-bot-fixture` 纳入 Load 工具镜像 |
| `c7ac582e` | 增加预编译 Live Bot 镜像入口 |

## 3. Actor 热路径优化

### 3.1 原路径的重复工作

一次普通 Dirty Actor Turn 原来可能连续发生以下完整状态复制：

```text
CurrentModelSnapshot
→ 防御性复制完整 ModelState
→ PlayerActor 立即反序列化

构建新的 Candidate ModelState
→ StageSoftModelState 再复制一次

PrepareSoftFlush 冻结一份
→ 构建 Store Request 再复制一份
→ Flush 成功安装 persisted baseline 再复制一份
```

这些复制不增加业务正确性，却会在玩家/Farm 状态字段变多、在线 Actor 增多时产生额外 CPU 与内存分配。

### 3.2 新路径

新增两个边界明确的内部接口：

```text
VisitCurrentModelSnapshot(visitor)
```

- 只允许在已串行化的 Actor Turn 内借用当前状态；
- callback 返回后不能保留或修改 map/byte slice；
- 公共读取仍使用会防御性复制的 `CurrentModelSnapshot`。

```text
StageSoftModelStateOwned(actor_id, fresh_candidate, schedules)
```

- Player/Farm Candidate Builder 每次都创建全新的 `ModelState`；
- Actor 将这份新对象的所有权直接转交给 Binding；
- 一般调用方仍使用 `StageSoftModelState`，其防御性复制语义没有改变。

Soft Flush 现在只在冻结当前版本时复制一次；Store Request 只读复用冻结状态，成功后再把其所有权转给 persisted baseline，不再重复复制。

### 3.3 不变量

- Borrowed Snapshot 只能在 Actor 串行域中使用；
- Owned Candidate 转交后，调用方不能再次读取或修改；
- Flush 冻结的版本与更新的内存版本仍通过 `soft_generation` 隔离；
- 旧 Flush 完成不能清除或覆盖更新的 Dirty Turn；
- Writer Epoch、Lease、CAS 和 Drain 语义没有放宽；
- 原 `StageSoftModelState` 和 `CurrentModelSnapshot` 继续保持安全的防御性复制。

## 4. 好友初始化与偷菜机器人

### 4.1 好友环

Fixture 新增：

```text
--friend-ring
--friend-ring-offset=1
```

若玩家为 `P0 ... P(N-1)`，Offset 为 1，则：

```text
P0 → P1
P1 → P2
...
P(N-1) → P0
```

关系落库采用 canonical pair：

```text
low_player_id  = min(A, B)
high_player_id = max(A, B)
```

Fixture 为每条边建立：

```text
1 份 FriendRelation Pair 权威
2 条 FriendIndex（A→B、B→A）
2 个好友额度槽（A、B 各一份）
```

Pair、Index、Quota 使用确定性 operation/workflow 标识，重复执行会幂等修复。多个玩家可并行初始化；同一玩家的额度更新保持串行，避免对同一 Quota 记录并发 CAS。

### 4.2 偷菜目标

`live-farm-bot` 同样新增好友环参数。Bot `i` 的目标农场为：

```text
target_index = (i + offset) % player_count
target_farm  = player_start + target_index
```

`theft` 场景现在拒绝误偷自己的农场，必须显式配置目标农场或好友环。机器人仍走真实路径：

```text
HTTP Login
→ WSS / ClientHello
→ Join 自己农场完成恢复屏障
→ Join 好友农场
→ Snapshot / Catch-up / SubscriptionReady
→ 选择成熟且可偷的 CropCycle
→ THEFT
→ 等待命令终态
```

Fixture 是压测前置工具，直接使用正式 Friendship Repository 建立真值与索引；它不是模拟用户在 H5 上逐步发送“申请/接受好友”的产品流程。

## 5. 验证

### 5.1 代码验证

Linux 全量测试：

```text
go test ./...
```

结果：通过。

目标包竞态测试：

```text
go test -race \
  ./internal/logic/actor/authority \
  ./internal/player/actor \
  ./internal/farm/actor \
  ./internal/farmbotclient \
  ./internal/livefarmbot \
  ./cmd/live-bot-fixture \
  ./cmd/live-farm-bot
```

结果：通过。

同时修复了一个原有的跨平台测试问题：粗粒度时钟下合法的 `0ms` admission 样本不再被丢弃。

### 5.2 真实链路冒烟

环境：

```text
Kubernetes context: uc-test-plus
Namespace:          developer_026-qqfarm
Redis:              公司 Redis Cluster
Player ID:          998250825000 起，共 4 人
Job:                actor-hotpath-friend-theft-smoke-0825-r3
```

机器人镜像：

```text
mirrors.tencent.com/uc-backend/farm-live-bot@
sha256:d3ae9cb0c9083f448195b37d4c16189f9bf4fbf7dd5052ff1686530cb287aad1
```

Fixture 结果：

```json
{
  "players": 4,
  "initialized": 4,
  "friend_pairs": 4,
  "friend_index_rows": 8,
  "friend_quota_slots": 8
}
```

Owner 阶段：

```text
ready=4, succeeded=4, failed=0
actions=8（4 Buy + 4 Plant）
Plant latency: min=5ms, p95=9ms, max=9ms
```

等待 12 秒（环境作物成熟时间为 10 秒）后执行好友偷菜：

```text
ready=4, succeeded=4, failed=0
theft actions=4
rejected=0, timeout=0, pending=0
Theft latency: min=7ms, p50=8ms, p95=13ms, max=13ms
```

Job 最终状态：`Complete 1/1`，总时长约 15 秒。

前两次 Job 分别因测试保护参数缺少 `--allow-non-loopback`、监听端口错误设置为 `0` 而提前退出；均发生在业务阶段之前。修正编排参数后的 `r3` 完整通过，不涉及业务代码补丁。

## 6. 使用方式

初始化玩家、资产和好友环：

```text
live-bot-fixture \
  --player-start=<start> \
  --players=<N> \
  --coins=50000 \
  --level=19 \
  --friend-ring \
  --friend-ring-offset=1
```

让所有 Owner 买种并种植：

```text
live-farm-bot \
  --scenario=journey \
  --players=<N> \
  --player-start=<start> \
  --session-actions=1
```

成熟后让每个 Bot 偷下一位好友：

```text
live-farm-bot \
  --scenario=theft \
  --players=<N> \
  --player-start=<start> \
  --friend-ring \
  --friend-ring-offset=1
```

单轮 `theft` 每名 Bot 选择一块合法成熟作物；持续压测可结合 Session 时长和动作次数重复执行。

## 7. 尚未完成的验证

本轮没有替换当前压测环境的 PlayerSvr/FarmSvr Runtime 镜像，因此还不能给出热路径优化后的真实 QPS 增幅。下一步应在独立压测窗口做同配置 A/B：

```text
A：4715edaf（优化前）
B：c7ac582e（本分支）

固定：
Bot 数、到达率、思考时间、Shard、Pod 数、Redis、Gate/Downlink 参数

比较：
actions/s、P95/P99、CPU、alloc_space、GC pause、
cloneAuthorityModelState、oldest_dirty_age、flush throughput
```

只有这组 A/B 完成后，才能把“减少代码级克隆”进一步升级为“全链路容量提升”的结论。

