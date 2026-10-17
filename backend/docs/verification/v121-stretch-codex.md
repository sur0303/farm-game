# M3 最小 Stretch：植物图鉴

日期：2026-07-30

## 1. 为什么选择图鉴

冻结计划要求在 Task、Codex、Mail 中选择一个最小 Stretch。当前三个可靠
Loop 已经包含 Harvest Workflow，而“第一次收获某种作物”天然会产生一个
永久玩家事实。因此先实现植物图鉴可以复用现有工作流恢复能力，不需要为了
展示功能再引入第二套定时任务、邮件领取或红点状态机。

如果只在 H5 本地标记“已解锁”，刷新、换设备和断线恢复都会丢失，而且客户端
可以伪造；如果由 FarmActor 直接写玩家图鉴，则会破坏 FarmActor 与 PlayerActor
的权威边界。当前实现选择：

```text
Harvest Workflow
  -> FarmActor 持久化包含 crop_definition_id 的 Settlement
  -> PlayerActor.ApplyReward
  -> 同一个 PlayerRecord CAS 同时提交物品、经验和 CodexEntry
  -> FarmActor 完成原 Workflow
```

在线协调失败时，`farm-worker` 从原 Settlement 读取同一个
`codex_crop_definition_id` 并继续 `ApplyReward`。同一
`operation_id + payload_hash` 的重放返回原 Receipt，不会重复增加物品、经验或
图鉴条目。

## 2. 数据合同

- `player.PlayerRecord.codex_entries`：永久事实，按 `crop_definition_id` 唯一。
- `player.PrivateState.codex_entries`：只下发给当前认证玩家。
- `farm.FarmSettlementParticipant.codex_crop_definition_id`：恢复所需的可靠输入。
- `logicrpc.ApplyRewardRequest.codex_crop_definition_id`：内部可信 Participant 参数。

`CodexEntry` 记录：

- `crop_definition_id`
- `first_harvested_at_ms`
- `source_operation_id`

旧 Redis 记录没有该字段时按空数组解释，protobuf 字段增加保持向后兼容。

## 3. 客户端表现

仓库面板显示“植物图鉴 · 胡萝卜”：

- 首次收获前：锁定，提示首次收获后解锁。
- 首次收获成功后：显示已解锁及首次收获时间。
- 重连并重新获取 PrivateState 后：仍然显示已解锁。

该状态只来自服务端 `PrivateState`，客户端不根据按钮点击或本地计时推导。

## 4. 验证结果

2026-07-30 本地真实四服务栈（公司持久 Redis）：

- `go test ./...`：通过。
- `go vet ./...`：通过。
- H5 Vitest：13 个文件，59 个测试通过。
- H5 Staging build：通过。
- Playwright Staging：4/4 通过，31.5 秒。

其中新增证据覆盖：

1. PlayerActor 首次奖励原子解锁图鉴。
2. 同 operation 重放不创建第二个条目。
3. farm-worker 接管 Harvest 时保留 crop definition。
4. 浏览器首次收获后显示解锁。
5. 模拟断线、RecoveryBarrier 完成后仍显示解锁。
6. 首次登录好友列表进入 `ready`，分享接受通过下行主动刷新双方列表。

## 5. 当前边界

本阶段只实现胡萝卜的一条最小永久事实链路。未实现图鉴奖励、多个作物配置、
完成度红点或图鉴排行榜；这些不应阻挡 M3 最小 Stretch 验收。
