# v1.18 三 Loop 第一、二阶段实现与验证记录

日期：2026-07-29  
状态：第一、二阶段纵切完成并切到公司持久 Redis；多节点运行时、Redis 容灾、容量与发布验收未完成

## 1. 本轮结论

本轮形成了一条不依赖 TCONND/gateproxy 的本地可执行主链：

```text
classic-farm-h5
  → HTTP 登录 / 分享
  → Direct FarmGate WebSocket / FarmFrame v0.2
  → farm-gate
      ├─ 经济命令 → player-svr → PlayerActor(player_id)
      └─ 农场命令 → farm-svr   → FarmActor(farm_id)
                               ↘ player-svr participant
  → Redis WorkflowStore ← farm-worker → player-svr / farm-svr
```

三个优先 Loop 的当前覆盖如下：

| Loop | 已实现行为 | 验证 |
|---|---|---|
| 个人经营 | Join、12 地块、购买种子、播种、按服务端时间成熟、收获、断线恢复 | 真实 Chromium + Docker 多进程 |
| 经济 | 初始金币、购买扣款、收获入包、出售增币、幂等 receipt | 真实 Chromium + 服务集成测试 |
| 实时社交 | 双账号分享接受、好友列表自动刷新、好友 Join、浇水、偷菜、双方实时版本广播 | 两个独立 Chromium Context + 服务集成测试 |

这是一条开发/演示纵切，不等价于 3000 万 DAU 生产完成。

## 2. 第一阶段：双账号、HTTP Session 与分享闭环

- Staging 明确提供两个独立身份：`player-a / 10001`、`player-b / 10002`；浏览器 Profile 不再被错误地当作账号。
- 登录同时签发 HMAC 保护的 `HttpOnly farm_session` Cookie，并绑定当前活动 `session_epoch`；同账号再次登录后，旧 HTTP Session 立即失效。
- 创建分享、接受分享、好友列表和解除好友均从 Session 推导操作者，不信任请求体中的 `player_id/viewer_id`。
- 分享 token 使用 128 bit 随机值，Redis TTL 默认 24 小时；接受通过 Lua 原子消费，重复接受、过期、自邀、无 Session 和好友上限均有测试。
- 好友列表在页面聚焦和每 5 秒自动刷新；邀请方无需手动刷新即可看到新好友。
- 好友农场只开放护理操作；播种、收获仍由本人农场裁决。

真实浏览器测试使用两个隔离 Context 完成：

```text
A 登录 → 创建分享
  → B 打开分享并用另一账号登录
  → 服务端接受并建立双向好友关系
  → A 自动看到 B
  → B 进入 A 农场浇水
  → A/B 收到同一 FarmChange
  → B 成熟后偷菜
  → B 断线重连并恢复好友列表
```

## 3. 第二阶段：可恢复 Workflow 与 farm-worker

`WorkflowRecord` 只保存可恢复的调度状态；业务权威事实仍是 FarmRecord 的 operation
receipt/settlement 与 PlayerRecord 的 reservation/grant receipt。

状态机：

```text
PENDING
  → FARM_COMMITTED
      → COMPLETED
      → RETRY_WAIT → FARM_COMMITTED / COMPLETED
  → COMPENSATED
```

- `farm-svr` 在跨权威操作开始前创建 Workflow，并在 Farm 提交后记录
  `FARM_COMMITTED`；同步 participant 路径有 5 秒优先完成窗口。
- `farm-worker` 独立进程从 Redis 有序集合原子 claim 到期记录，以 lease 防止多个
  worker 同时接管，并采用 250ms 起步、最多 30s 的指数退避。
- Worker 每次执行前先读取 Farm 权威 receipt：播种继续 commit/release reservation，
  收获和偷菜继续幂等 GrantItem，然后调用 Farm 完成同一 operation。
- Worker 或任一服务重启不会重复发物品；终态由原 `operation_id` 查询恢复，H5
  IndexedDB journal 保留 `PROCESSING/Unknown` 操作并在重连后查询。
- 本地 Compose 已加入 `farm-worker` 与独立 health 端口 `19201`。

真实进程故障测试执行：

```text
B 播种并等待成熟
  → stop farm-worker
  → stop player-svr
  → 收获：FarmRecord 提交，客户端得到 PROCESSING，物品尚未到账
  → start player-svr + farm-worker
  → worker 从 Redis 接管、幂等发奖并完成 Farm receipt
  → H5 重连查询原 operation_id
  → 未决操作清除，胡萝卜恰好增加 10
```

## 4. 服务与权威边界

- `player-svr` 持有独立 `PlayerRecord`，裁决钱包、背包、购买/出售 receipt、物品预留和发放。
- `farm-svr` 持有独立 `FarmRecord`，裁决地块、作物周期、护理、偷菜、Presence 和农场版本。
- `farm-gate` 只做登录票据、HTTP Session、协议、命令路由、连接序号和下行背压，不修改资产或农场事实。
- `farm-worker` 不直接编辑 Player/Farm blob，只调用 participant/workflow gRPC 接口。
- `OwnerCoreRecord` 没有继续扩张；旧表和旧 Actor 仅作为历史迁移/真库探针基线。

## 5. 当前公司持久 Redis 主库

按最新授权，Player/Farm/Friend/Share/Workflow 数据直接使用公司改造的持久 Redis，
并用独立 namespace 与其他环境隔离：

- `farm:v118:primary:player:<player_id>`
- `farm:v118:primary:farm:<farm_id>`
- `farm:v118:primary:friend:*`
- `farm:v118:primary:share:token:<token>`（24 小时 TTL）
- `farm:v118:primary:workflow:record:<operation_id>`
- `farm:v118:primary:workflow:due`

Player/Farm 数据以确定性 protobuf blob + version 存储，Lua CAS 保证版本条件写。
Workflow 使用持久 record、due zset 和 lease；业务服务启动时连接失败会 fail-closed。
详细边界见 `docs/design/31-persistent-redis-authority.md`。

## 6. 可重复运行

Docker 构建环境无法解析内部 `git.internal.example.invalid` 时，先在宿主机编译，再把二进制放入最小镜像：

```sh
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh up -d --build
./scripts/company-redis-compose.sh ps
```

默认端口：

| 组件 | 地址 |
|---|---|
| FarmGate HTTP/WS | `http://127.0.0.1:8080` / `ws://127.0.0.1:8080/ws` |
| FarmGate downlink | `127.0.0.1:9092` |
| farm-svr gRPC/health | `127.0.0.1:19000` / `127.0.0.1:19001` |
| player-svr gRPC/health | `127.0.0.1:19100` / `127.0.0.1:19101` |
| farm-worker health | `http://127.0.0.1:19201/readyz` |
| 业务数据库 | 公司持久 Redis（凭据由本地 ignored 文件注入） |

H5 验收：

```sh
cd ../classic-farm-h5
npm run e2e:staging
npm run e2e:workflow
```

`e2e:workflow` 会短暂停止并自动恢复本地 `player-svr/farm-worker`，不要在共享环境运行。

## 7. 已执行验证

```text
go test ./...                                             PASS
go test -race ./internal/workflow ./internal/farm/ingress PASS
go vet ./...                                              PASS
docker compose config + local binary preparation          PASS
npm test                                                  54 PASS
npm run build:staging                                     PASS
双浏览器分享/好友/实时同步 E2E                              PASS
停进程/恢复/恰好一次发奖 E2E                               PASS
全业务进程重启后 Redis 状态恢复                              PASS
```

服务端测试还覆盖：

- 买种子 → 播种 → 成熟 → 收获 → 出售；
- 好友 Join → 浇水 → 两连接收到同一递增 `farm_version`；
- 偷菜结算、访客到账和相同 `operation_id` 重放；
- Workflow lease、播种补偿、发奖失败退避和服务重启恢复；
- Redis 票据一次性核销、HTTP Session 代次失效和 WebSocket 出站序号合并。

公司 Redis 实测还覆盖：停止 `farm-gate/farm-svr/player-svr/farm-worker` 全部业务
进程、保持 Redis 独立运行，再启动后，原浏览器页面无需刷新或重新登录即可获取新
Ticket、重建 WebSocket Session、重新 JoinFarm，并恢复 Player/Farm/Workflow 记录
和终态 receipt。测试同时验证：结果查询早于 Worker 完成时，H5 会对 journal 中的
未决 operation_id 退避轮询，最终从 `PROCESSING` 收敛到终态。

## 8. 仍未完成的生产合同

- 公司 Redis 的持久确认点、主从切换、备份恢复、RPO/RTO 和禁止 eviction 验收；
- UC ShardedService/Region、多节点 Actor 迁移、passivation、加载风暴和 writer epoch；
- 正式平台登录、HTTPS Cookie/SameSite/CSRF 策略、域名与密钥托管；
- Worker 积压指标、死信/人工对账、批量扫描和多副本压测；
- 弱网矩阵、多实例 Session handoff、滚动发布和跨可用区故障注入；
- 3000 万 DAU 容量模型、压测报告和生产告警阈值；
- 任务、邮件、宠物、图鉴等三个主 Loop 之外的外围系统。

下一步应完成公司 Redis 容灾语义、多实例、弱网和容量验收；Tcaplus 保留为未来可选迁移路径，当前不再是前置项。
