# Actor Dirty Writeback 实验验证报告

日期：`2026-08-24`

分支：`codex/actor-dirty-writeback-experiment-r1`

基线：`origin/master@280daa13bc004560e3c5d5665db492de429c54c6`

工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-actor-dirty-writeback-20260824`

## 1. 验证结论

实验路径已具备可运行、可切换、可回退到默认可靠模式的代码基础：

- 默认 `durable_cas` 行为保持不变；
- `dirty_best_effort` 的普通 Actor Turn 不执行 Redis 命令；
- 多个 Turn 可合并为一次 writer-fenced Redis Flush；
- Flush Redis I/O 在 Actor Mailbox 之外执行；
- Flush 期间出现的新版本不会被旧完成回调误清除；
- 正常 Stop/Shard Drain 会强制写回；
- 硬崩溃仍会丢失最近未 Flush 的内存状态；
- 跨 Actor 业务不再由 SourceOrder/Worker 可靠恢复。
- Player/Farm `/metrics` 已暴露 Dirty marks、合并量、Flush 成败、队列深度、in-flight 和最老脏龄。

这是一条性能与弱一致性对照实验，不是生产替换结论。

## 2. 自动化验证

已通过：

```text
git diff --check
go test ./...
go test -race ./internal/logic/actor/dirtywriteback \
  ./internal/logic/actor/authority \
  ./internal/player/actor \
  ./internal/farm/actor
```

`go vet ./...` 仍报告 4 个基线告警：两个测试复制含锁的 protobuf、一个测试 fake 按值传递 `sync.Once`，以及 `cmd/route-controller` 的 context cancel 路径。对应文件相对 `origin/master` 无改动，本实验没有扩大范围去修改它们；因此本报告只声明测试和关键 race 检测通过，不声明全仓 vet 已清零。

重点覆盖：

| 测试方向 | 证明内容 |
|---|---|
| unique queue | 同一 Actor 多次 Mark 只占一个条目 |
| queue bound | 容量按唯一 Actor 数限制 |
| in-flight remark | Flush 期间再次修改会重新入队 |
| failed flush | 保存失败后仍保留 Dirty 责任 |
| FlushNow / Close | 生命周期等待正在执行的 Flush，并补写更新版本 |
| Player hot path | 第二个 Dirty Turn 使用内存新状态，不回退 Redis |
| Farm hot path | 连续业务 Turn 只推进内存版本 |
| mailbox isolation | 阻塞 Redis Store 时，Actor 仍能处理后续 Turn |
| frozen version | V2 Flush 时产生 V3，V2 成功后 V3 仍 Dirty |
| restart | 从 Redis 恢复独立 `actor_memory_version` |
| drain | Player/Farm Shard Facts Barrier 先 Flush 再退休 |
| writer fence | 旧 Holder 的迟到 Flush 被拒绝 |

## 3. 公司 Redis Cluster A/B

### 3.1 环境

- namespace：`developer_026-qqfarm` 中现有公司 Redis 连接配置；
- Redis 形态：`cluster`，三主三从；
- 测试命令：`cmd/actor-writeback-ab`；
- 测试数据：每次创建独立 Redis namespace 和两个独立 Player ID；
- 清理：测试结束遍历各 Master，只删除该次隔离 namespace；
- 场景：单 PlayerActor 连续执行 500 次 `coin += 1`，最后读取 Redis 校验结果。

该工具不会输出 Redis 地址或凭据。

### 3.2 500 Turn 结果

| 指标 | durable_cas | dirty_best_effort |
|---|---:|---:|
| Turn 数 | 500 | 500 |
| 总耗时 | 268 ms | 5 ms |
| 吞吐 | 1,859.944 turns/s | 97,479.359 turns/s |
| p50 | 482 μs | 9 μs |
| p95 | 737 μs | 12 μs |
| p99 | 2,265 μs | 52 μs |
| 最大值 | 2,788 μs | 81 μs |
| 热路径 Redis 命令 | 500 | 0 |
| 热路径命令类型 | 500 × EVALSHA | 无 |
| 最终 Flush Redis 命令 | 0 | 1 |
| 最终 Flush 耗时 | 31 μs（无业务 Flush） | 1,884 μs |
| 最终 Redis RecordVersion | 501 | 2 |
| 最终 actor_memory_version | 0（旧模式未使用） | 501 |
| 持久金币增量校验 | +500 | +500 |

解释：

- reliable 模式的 500 次逻辑修改产生 500 次同步 Lua/CAS；初始建档后物理版本到 501。
- Dirty 模式的 500 次逻辑修改没有热路径 Redis 命令，最终合并成 1 次 Flush；物理版本只到 2，但逻辑 Actor 版本为 501。
- 两组最终金币都精确增加 500，说明正常退出 Flush 能保存合并后的最新状态。

### 3.3 不能从这组数据推出什么

这是单 Actor、顺序钱包修改的存储微基准，不包含：

- H5、HTTPS/WSS、Gate；
- Actor 路由网络跳转；
- Plant/Harvest/Theft 跨 Actor RPC；
- 200 人同场广播；
- 多 Pod 扩缩容；
- Redis 故障和 Pod 硬 Kill；
- TLog 成本。

因此这组数据只证明“移除每 Turn Redis CAS 后的理论热路径收益”和“最终合并写回正确”，不能直接换算成整服 QPS 或 3000 万 DAU 容量。

## 4. 已修复的关键缺陷

### 4.1 Player 第二 Turn 覆盖内存新状态

初版 Dirty 实现因物理容量视图无效而重新 Load Redis，第二条命令可能从旧快照开始，覆盖第一条未 Flush 修改。现已冻结规则：只要 Binding 已激活，Redis 是恢复基线，普通 Turn 必须使用 Binding 的最新内存模型。

### 4.2 每 Turn 仍查询 Shard Fence

初版虽然不写 Redis，但 `ValidateCurrentGeneration` 仍读取路由/Fence，热路径并未真正脱离 Redis。实验模式现改为本地 generation 校验；显式 Drain 退休 Binding，最终 Flush 用 writer fence 拒绝旧 Holder。

### 4.3 Flush 期间的新变更被误清 Dirty

现以冻结 `softGeneration` 和逻辑 memory version 判断完成边界。V2 Flush 成功只确认 V2；如果当前已是 V3，Actor 继续 Dirty 并重新入队。

### 4.4 Drain 与后台 Flush 死锁

Shard Facts Barrier 先在 Drain Mailbox 外等待/触发 Flush，再进入最终事实确认与驱逐，避免生命周期调用和正在执行的 Flush Completion 相互等待。

## 5. 风险与建议

| 风险 | 当前行为 | 上线前要求 |
|---|---|---|
| Pod 硬崩溃 | 丢失最近未 Flush 窗口 | 故障注入量化 RPO |
| 跨 Actor RPC 失败 | 允许部分结果，不由 Worker 恢复 | 产品明确接受或缩小 Dirty 范围 |
| 生命周期通知丢失 | 旧 Actor 可能先返回，Flush 后被 Fence 拒绝 | 多 Pod Handoff 压测与告警 |
| Redis 长时间不可用 | 进程内重试，队列/脏龄增长 | 熔断、最大脏龄和内存保护 |
| Opportunity Schedule 延迟 | 最多额外等待一个 Flush 窗口写入 | 真实好友弹窗 E2E 验收 |
| 审计/恢复 | 尚无完整 TLog 重放 | 不得声称日志可恢复业务 |

最终建议：保留该分支作为 A/B 实验线。若真实全链路数据证明收益显著，可优先把 Dirty 用于低价值、可重建、允许少量回退的数据；金币、库存和跨 Actor 经营结果是否采用，必须由故障实验和产品语义共同决定。

## 6. 5 万 Bot 真实链路补充验证

后续已在 `developer_026-qqfarm` 完成三轮 5 万 Bot 的 HTTP 登录、WSS、Join、Snapshot 与持续经营压测。接入与 Join 达到 5 万在线，但当前 2 Gate + 2 PlayerSvr + 2 FarmSvr 尚未通过 5 万持续经营的严格验收；最佳一轮约为 6,215 动作/秒、47,162 个 Bot 无失败，同时暴露 Downlink、Gate backend、Redis 等级联瓶颈以及最老 Dirty 状态超过 3 分钟的问题。

完整数据、参数和证据路径见：

- [Actor Dirty Writeback：5 万 Bot 真实链路压测报告](actor-dirty-writeback-real-chain-50k-20260824.md)
