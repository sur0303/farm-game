# M5 Redis 平台切主与备份恢复演练手册

日期：2026-07-30

## 1. 目的与边界

应用单测、临时 key CAS 和进程重启不能回答以下平台问题：

- 已经向客户端返回成功的写，在主从切换后是否仍存在；
- `WAIT 1` 已确认的写，在原主故障后是否仍存在；
- 超时/连接重置的写究竟已提交还是未提交；
- 客户端不可用时长和平台恢复时长；
- 备份能否恢复到隔离实例，以及恢复数据的实际时间点。

`cmd/redis-ha-probe` 只测量这些语义，**不会触发切主、暂停 Redis、修改配置或删除
业务数据**。每轮使用随机 hash tag：

```text
farm:test:ha:{random_namespace}:attempts
farm:test:ha:{random_namespace}:sequence
```

每次写入使用稳定 `attempt_id + payload_hash`。观察结果逐条 `fsync` 到本地 JSONL，
凭据不会写入清单。`WAIT` 与写命令固定使用同一条 Redis 连接，避免把其他连接的
复制进度误认为本次写的副本 ACK。

平台切主和备份恢复属于外部状态变更，只能在明确维护窗口、由 Redis 资源所有者
授权后执行。不要用生产业务流量代替隔离探针。

## 2. 切主演练

### 2.1 前置条件

1. 记录 Redis 实例、区域、当前主/副本和平台事件单号；
2. 冻结本轮 RTO/RPO 验收阈值；
3. 确认 `deploy/secrets/redis.conf.local` 指向待验证实例；
4. 保留一个独立终端运行探针，另一个终端由资源所有者执行托管切主；
5. 切主前后都不要运行 `cleanup`。

### 2.2 执行

```bash
./scripts/probe-company-redis-ha \
  -mode run \
  -manifest docs/verification/results/redis-ha-failover-<UTC>.jsonl \
  -duration 10m \
  -interval 100ms \
  -command-timeout 750ms \
  -wait-replicas 1 \
  -wait-timeout 1s
```

建议时序：

```text
T+0m   启动探针
T+2m   平台执行一次受控主从切换
T+5m   确认新主可服务，但不停止探针
T+10m  探针结束并自动执行第一次读回核对
```

随后使用全新 Redis 连接独立核对：

```bash
./scripts/probe-company-redis-ha \
  -mode verify \
  -manifest docs/verification/results/redis-ha-failover-<UTC>.jsonl \
  -endpoint-purpose post-failover-primary
```

### 2.3 判定

硬失败：

- `acknowledged_missing > 0`；
- `replica_acknowledged_lost > 0`；
- `payload_mismatch > 0`；
- `read_errors > 0` 导致结果无法判断。

必须记录但不直接伪装成失败或成功：

- `unknown_committed`：客户端看到未知，但权威 marker 已提交；
- `unknown_not_committed`：客户端看到未知，权威 marker 未提交；
- `max_observed_unavailable_ms`：以连续写探针观察到的客户端不可用窗口；
- 平台控制台的切主开始/结束时间、事件 ID 和主节点变化。

只有满足业务约定的 RTO/RPO，且不存在 ACK 丢失，才能把切主语义标为通过。

## 3. 备份恢复演练

为了避免把“备份时间点之后的正常缺失”误报成数据丢失，本轮使用静止 marker：

1. 运行一轮 1 分钟探针并保留 key；
2. 探针结束后独立 `verify`，确认源实例全部 marker 存在；
3. 之后才创建一次 on-demand 备份，记录备份 ID、开始和完成时间；
4. 把该备份恢复到隔离实例，记录恢复开始、可连接时间与恢复实例 ID；
5. 使用指向恢复实例的单独本地配置执行 `verify`；
6. 源实例与恢复实例都验证完成后再分别执行 `cleanup`。

源实例：

```bash
./scripts/probe-company-redis-ha \
  -mode run \
  -manifest docs/verification/results/redis-ha-backup-<UTC>.jsonl \
  -duration 1m \
  -interval 100ms
```

恢复实例（明确指定另一份未提交配置）：

```bash
FARM_REDIS_CONFIG=deploy/secrets/redis-restored.conf.local \
./scripts/probe-company-redis-ha \
  -mode verify \
  -manifest docs/verification/results/redis-ha-backup-<UTC>.jsonl \
  -endpoint-purpose isolated-restored-backup
```

恢复验收要求：

- 所有探针结束前已确认的 marker 都存在；
- `acknowledged_missing=0`、`replica_acknowledged_lost=0`；
- payload hash 全部一致；
- Player/Farm/Workflow 各选一个隔离测试实体做 protobuf 解码与版本核对；
- 记录备份点 RPO、恢复 RTO 和平台恢复实例是否隔离；
- 恢复实例绝不接业务 Gate。

本步骤证明“指定备份可恢复”；最坏 RPO 还必须结合平台备份周期/SLA，以及在备份
周期边界附近的专门演练。

## 4. 清理

清理命令只从清单读取随机 namespace，并只删除对应两个测试 key：

```bash
./scripts/probe-company-redis-ha \
  -mode cleanup \
  -manifest docs/verification/results/redis-ha-<scenario>-<UTC>.jsonl
```

JSONL 清单、平台事件截图/ID、备份 ID 和汇总结论作为验收证据保留。不要把
Redis URL、密码或控制台临时凭据提交到仓库。

## 5. 当前基线

2026-07-30 在没有切主的平台稳态下运行 5 秒：

```text
attempts                  48
acknowledged              48
replica_acknowledged      48
acknowledged_missing       0
replica_acknowledged_lost  0
unknown                    0
role_changes               0
max_unavailable_ms         0
```

清单：

```text
docs/verification/results/redis-ha-baseline-20260730T0530Z.jsonl
```

该结果只证明探针本身、同连接 `WAIT` 与独立读回核对在稳态可工作，不是切主或备份
恢复通过的证据。
