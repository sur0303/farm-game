# M5 Redis 生产语义探测

日期：2026-07-30

## 非破坏性真实结果

命令：

```bash
./scripts/probe-company-redis-production
```

公司 Redis 返回：

| 项目 | 结果 | 判断 |
| --- | --- | --- |
| Redis | 7.1.5，role=master | 可识别 |
| replica | 1 个 | 有复制副本 |
| `WAIT 1 1000` | 1 个 ACK | 本次写入已被一个副本接收 |
| maxmemory | 8 GiB | 需结合容量报告 |
| maxmemory-policy | `noeviction` | 符合权威 key 不被淘汰要求 |
| 权威测试 key TTL | 无 TTL | 符合 Player/Farm 强事实合同 |
| AOF | disabled | Redis 自报没有 AOF |
| RDB save 配置 | 空 | Redis 自报没有周期 save |
| loading | false | 当前可服务 |
| failover state | no-failover | 本次没有切主 |

`noeviction`、单副本 ACK 是正向证据，但 **不能单独证明持久性**。当前实例通过
`INFO/CONFIG` 自报 AOF 关闭且 `save` 为空。如果 DevCloud 的“持久 Redis”依赖
Redis 进程之外的定制复制、云盘快照或平台备份，需要平台 SLA/控制台配置或恢复
演练作为证据，不能从客户端名称推断。

## 数据结构 Live 结果

隔离临时 key 全部通过：

- BlobStore Insert/Get、16 路并发 CAS 只有一个 winner。
- 模拟“脚本已提交但响应丢失”：按权威版本和 operation marker 读回可判定成功；
  旧 expected version 重试被拒绝。
- writer_epoch 新 Holder 成功、旧 Holder被 fence。
- Workflow CreateOrLoad、payload conflict、claim lease、terminal remove due。
- Friend canonical relation、好友上限、撤销/重建 relation epoch。
- Share token 的原子多次接受合同。

## HA 测量工具与稳态基线

已增加：

```text
cmd/redis-ha-probe
scripts/probe-company-redis-ha
docs/verification/v121-m5-platform-window-runbook.md
```

探针不会主动切主。它使用隔离随机 key 持续写入幂等 marker，将每次
`ACKNOWLEDGED/UNKNOWN`、Redis sequence、同连接 `WAIT` 副本 ACK、role/replid
逐笔 fsync 到不含凭据的 JSONL。切主或恢复后，`verify` 会把结果分成：

- 客户端已确认但丢失；
- 副本已确认但丢失；
- 超时但实际提交；
- 超时且未提交；
- payload 不一致或核对读取失败。

2026-07-30 稳态基线：

| 项目 | 结果 |
| --- | ---: |
| 写入尝试 | 48 |
| 客户端确认 | 48 |
| `WAIT 1` 确认 | 48 |
| 客户端确认后缺失 | 0 |
| 副本确认后缺失 | 0 |
| Unknown | 0 |
| 角色变化 | 0 |
| 最大观察不可用 | 0 ms |

独立新连接复核结果相同，之后已删除本轮两个隔离 key。原始清单位于
`results/redis-ha-baseline-20260730T0530Z.jsonl`。这只证明工具稳态可用，
不把 `role_changes=0` 的正常运行冒充成切主通过。

## Tcaplus 迁移形状复核

`api/proto/tcaplus/farm_tables.proto` 已再次完整生成。新增
`TestTcaplusMigrationSchemaContract` 从实际生成 descriptor 校验：

- 只有冻结的五张候选表；
- 每张表的 primary key 与预期完全一致；
- Workflow due index、Friend 双向 roster index 与预期一致；
- 所有 option 引用字段真实存在、非 repeated，且类型可作为 key/index。

真实已建 `OwnerCoreRecord` 也于 2026-07-30 再次通过专用 owner 的
Insert/Get/RecordVersion CAS/旧版本冲突并自动清理。当前表格组仍只有
`OwnerCoreRecord` 与 `owner_probe`；新五张表只是经过生成和 descriptor 校验的
迁移候选形状，尚未创建、backfill 或接流量。

## 仍需平台窗口

本探针有意不执行以下破坏性动作：

1. 主节点 kill/切主期间的成功、超时、连接重置语义；
2. 已收到 replica ACK 后主节点丢盘时的数据结果；
3. 备份恢复到隔离实例并核对 Player/Farm/Workflow；
4. RPO/RTO 和误删保护；
5. 复制积压耗尽、网络分区和副本落后。

因此 M5 当前状态是“应用层、可观测配置、HA 测量工具与 Tcaplus 迁移形状已验证；
平台持久/切主/恢复语义待维护窗口”，不能标为全部完成。维护窗口执行方法见
`v121-m5-platform-window-runbook.md`。
