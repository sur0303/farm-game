# 简历条目（直接粘贴）

**FarmGame（高并发实时农场游戏后台）** 2026.07 – 2026.08  
**项目背景**：面向高并发在线的实时农场经营后台，支持种植、收获、护理、偷菜、好友互动，以及服务在线扩缩与分片迁移。  
**涉及技术**：Go、gRPC、Protobuf、WebSocket、Redis Cluster、Lua、Kubernetes、Prometheus

- **Actor 模型**：采用 PlayerActor / FarmActor 双权威，同一 Actor 通过 mailbox 串行裁决、不同 Actor 并行执行；玩家资产命令与共享农场写互不阻塞，保证同一农场内地块操作顺序唯一
- **Shard 路由**：基于稳定哈希将 Actor 映射到 1024 个 VirtualShard，PLAYER / FARM 两套路由域独立放置；Gate 使用本地路由快照完成首跳，路由变更后按原操作有界重试
- **在线迁移**：自研 ScalePlan 实现分片在线迁移，按停收、排空、落盘、写权换代转移所有权，避免双写；单热点分片 2 万玩家 / 2 千在线，迁移约 3.15 秒，窗口内约 14.7 万次命令零失败、不掉线
- **权威存储**：使用 Redis Cluster 作为权威库，通过 Lua 在同槽内完成写权校验与版本提交，命令成功落库后再返回；跨 Actor 种植/收获用持久凭证记录进度，由独立 Worker 扫描恢复
- **延迟落库**：实现可切换的内存先行路径，普通命令只改内存并按 Actor 合并脏标记，后台一次性带写权刷盘；单 Actor 连续 500 次修改，热路径从 268ms / 500 次 Redis 降到 5ms / 1 次刷盘，迁移与停机前强制刷盘
- **接入与容量**：Gate 负责 Session、协议与下行背压，不持有业务写权；公司 K8s 上冷登录压测 3000/s 稳定通过，Ready P95 约 54ms

专业素质补一句：

> 掌握 Go 并发（goroutine、channel）、gRPC / Protobuf，以及 Redis Cluster、Kubernetes 与 Prometheus。

被问「两个存储为什么都写」：

> 默认是每笔生意先和 Redis 对版本再返回，崩溃能回到上一笔。延迟落库是可切换的加速路径，热路径不写库，刷盘时仍校验写权；金币和跨农场结算默认不走这条。
