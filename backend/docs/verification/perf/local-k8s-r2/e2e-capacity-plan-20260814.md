# 本地 r2 e2e 容量压测计划（2026-08-14）

状态：探索性计划。不是 v1.1 控制板 PASS，不支持 30M DAU 结论。  
公司 Redis：不用。只打本地 `farm-local-redis`。  
回执目录：`/data/farm-l2-r2/receipts/`。

## 0. 先恢复集群，再压

当前 `dyn-friend-r77-cap-20260813` 已不可用：`player-svr-0` OOM 后 PLAYER 分片 stall，gate/holder/worker `/readyz=false`。  
Holder boot id 绑 pod UID。不要删光 pod。恢复方式：新 `ROUTE_RUN_ID` 全量部署。

未恢复前，本计划所有场景都不跑。

## 1. 测什么、不测什么

对外报的容量数字，只允许来自 **玩家可感知的 e2e 路径**。单接口 QPS 只用来解释「谁先满」，不能写成登录/进游戏容量。

| 优先级 | 场景 | 玩家路径 | 用来回答 |
| --- | --- | --- | --- |
| P0 | 进游戏 | HTTP 登录拿票 → WSS → ClientHello → Join 自家农场 → Snapshot（地块/资产） | 同时能进游戏的人数、到达率 |
| P0 | 在线存量 | 进游戏成功后心跳/续租，保持 60–90s | 单 gate 能挂住多少已进游戏的连接 |
| P0 | 种植 | 进游戏成功后 Buy → Plant（带 snapshot 的 `ExpectedPlotVersion`）→ 终态 SUCCEEDED 且有 `crop_cycle_id` | 分散经营写路径 |
| P1 | 经营闭环 | 种植成功后等成熟 → Harvest → Sell | 个人农场主链是否在容量点上对账 |
| P1 | 热点围观 | 1 个农场主进自己农场并种活作物 + 199 访客 Join 该农场（先 seed 好友关系） | 单农场扇出，不是进游戏容量 |
| P1 | 浇水/访问 | 热点就绪后 owner Water；可选访客 Water（拒绝也算观测） | Care 写放大和权限 |
| P2 | 分段诊断 | 只 HTTP 登录 / 只 WSS Hello / 只 Join | 某个 e2e 折点之后才跑，用来拆层 |
| P2 | Crash Takeover | holder OOM 后过期分片是否被接管 | 今早集群就是这个洞；有容量数字后再补 |

明确不做（本轮）：

- T0 压测器资格包、控制板登记、G1/G2/G3 复跑
- 把「只 HTTP 登录」写成登录容量（今早 30000 就是这个坑）
- 公司 Redis、30M DAU 外推
- Cookie 换票、账密登录、Gate Restart 重连风暴（先保证主链）
- Planned Migration、Controller Restart 带载、Slow Reader、Theft 完整结算（P0/P1 有数字后再排）

旧计划 `classic-farm-3000w-dau-loadtest-plan-v1.1.md` 只作场景词典。本文件覆盖本轮执行顺序和判定。

## 2. 已有数字（仅作阶梯起点，不当本轮 PASS）

环境：1× farm-gate 4c/8Gi，player/farm-svr 各 2 副本 4Gi，本地 Redis。工具：`ws-stress`。

| 路径 | 可用（P95 还能看） | 硬折点 | 谁先满 |
| --- | --- | --- | --- |
| 只 HTTP 登录 | 30000 / 3.3ms | 40000，`player-svr-0` OOM | holder 4Gi，**不是进游戏容量** |
| Join+Snapshot（分散自家农场） | ~10000 / 30–67ms | 16000 lease lost | gate subscription lease + activation throttle；gate RSS 峰值 1.77Gi，8Gi 没碰到 |
| 种植 | 健康集群 1000 成功 | 16000 打坏后 200 都挂 | 写路径依赖健康 lease；损坏后不自动恢复 |

本轮进游戏 e2e 从 **2000 → 5000 → 8000 → 10000** 起爬，不再从 HTTP 3 万起。

## 3. 工具与判定

- 1 连接 = 1 个合成玩家。player id 区间每次换新的，避免脏档。
- 驱动：仓库 `.bin/ws-stress`。secret 只从 `secret/farm-dynamic-secrets` 的 `load-test-token-secret` 注入，不写盘、不进 argv。
- 进游戏成功 = Join 拿到自家农场 Snapshot（有地块版本），不是拿到 HTTP ticket。
- 种植成功 = Plant 终态 SUCCEEDED 且 `crop_cycle_id` 非空。PROCESSING 不算成功。
- 驱动必须有硬超时（`duration + 90s`），禁止 `readSuccessfulResult` 无限等。
- 折点（任一即停该阶梯）：
  - 任一 holder/gate 重启或 OOM
  - `subscription lease lost > 0`
  - ready < 95%
  - join/login 错误 > 2%
  - 场景错误 > 5%（种植/收获）
  - P95(进游戏) > 200ms 记为 **软墙**，继续爬硬墙，但对外可用容量停在软墙前一档
- 每档记录：attempted/ready、P50/P95、错误码、gate RSS、holder RSS、lease lost、pod restart。写 JSON 回执。
- `company_redis_contacted: false`。不宣称 30M DAU。

## 4. 场景定义

### S0 冒烟（每轮部署后必跑）

200 人走完整进游戏路径。失败则停，不爬阶梯。

### S1 进游戏 e2e（本轮主数字）

路径：HTTP 登录 → WSS → ClientHello → Join 自家农场 → Snapshot。  
到达：开放到达，rate 随 N 升高（约 200–800/s）。  
阶梯：2000、5000、8000、10000，过了再 12000、14000。  
报告两行：

- **可用**：最后一档 P95 ≤ 200ms 且无硬折点
- **硬墙**：lease lost / OOM / ready&lt;95%

Burst 附档（P0 后半）：固定 5000 在线，再打 1s/5s 进游戏洪峰，看 Join 排队和 throttle。不要和存量阶梯混成一个数字。

### S2 在线存量

S1 成功后保持连接 60–90s，心跳/续租。  
阶梯跟 S1 可用档和 +1 档。这是连接成本，不是进游戏 QPS。

### S3 种植 e2e

每人先走完 S1，再 Buy+Plant。分散到不同农场。  
阶梯：200、500、1000、2000，过了再 4000。  
必须在健康集群上跑。禁止在 lease lost 之后接着种。

### S4 经营闭环（P1）

S3 成功子集上 Mature → Harvest → Sell。先 200/500，再考虑 1000。对账失败即停。

### S5 热点 1+199（P1）

HTTP share 在 test profile 上是 503，继续用 Redis `ActivateFriendRelation` pair-only seed。  
199 访客 Join 同一农场，200/200 Snapshot ready 后再浇水。  
owner Plant 必须拿到 `crop_cycle_id`；清掉 WS read deadline。  
只报热点扇出，不和 S1 进游戏容量混报。

### S6 分段诊断（P2，按需）

仅当 S1/S3 碰到折点且看不出哪一层时：

1. 只 HTTP 登录（会建档，注意 player-svr 内存）
2. HTTP + WSS Hello，不 Join
3. 完整 S1

用来回答「票、握手、Join/Snapshot 谁先死」，不单独对外报。

## 5. 执行顺序

1. 新 `ROUTE_RUN_ID` 部署，pods Ready，`/readyz` 全绿。
2. S0 冒烟。
3. S1 进游戏阶梯 → 写出可用/硬墙。
4. S2 在可用档和硬墙前一档做存量。
5. S3 种植阶梯。
6. 有余量再 S4、S5。
7. 折点原因不清时才 S6。
8. 每场景一份回执，最后一份 summary：可用容量、硬墙、谁先满、集群是否仍 Ready。

一档打坏 lease 或 OOM 后：**停写路径，不要用坏集群继续种/收**。需要写路径时重新部署。

## 6. 规模与资源（本轮不改，除非盒子先满）

| 组件 | 本轮 | 备注 |
| --- | --- | --- |
| farm-gate | 4 CPU / 8Gi | 今早峰值 ~1.8Gi，不是第一墙 |
| player-svr / farm-svr | 2 CPU / 4Gi ×2 | HTTP 建档和 actor 常驻会先打满 4Gi |
| worker | 保持 1 CPU / 768Mi | 种植/收获看 backlog，不先加盒 |
| Redis | 本地 farm-local-redis | 禁止公司 Redis |

主机约 32c/64G，禁止 9 个 pod 全部 4c8g。

动态 profile 物化时默认带上单机压测源 IP 准入（写进 `farm-staging-platform`，不要每次 apply 后再手工改 ConfigMap）：

- `GATE_WS_RATE_PER_SEC=5000`
- `GATE_WS_RATE_BURST=20000`
- `GATE_PREAUTH_MAX_CONNECTIONS_PER_IP=16384`
- `GATE_PREAUTH_MAX_BYTES_PER_IP=1073741824`（1GiB）

Pod 级 `GATE_PREAUTH_MAX_CONNECTIONS` / `GATE_PREAUTH_MAX_BYTES_POD` 保持产品值，那是真饱和信号。代码默认 10/s burst 20 仍是产品防滥用，不改。

测 pod 打满（CPU ~90%、进游戏 QPS）时必须打开 farm-svr 激活风暴门，否则 12k 先撞 1000/s token bucket，测的不是 2c/4Gi。容量跑通过物化 env 打开，不改 Go 默认：

- `ACTIVATION_RATE_PER_SEC=0`（0 = 不限速）
- `MAX_CONCURRENT_ACTOR_LOADS=1024`（不能设 0，二进制会回退 64）
- `MAX_PENDING_ACTOR_LOADS=4096`

## 7. 回执要写清的三句话

1. 测的是哪条玩家路径（必须能复述 HTTP→WS→Join→Snapshot / 再 +Plant）。
2. 可用档 vs 硬墙档（P95 和错误率分开）。
3. 谁先满（gate lease、holder 4Gi、worker backlog、Redis），以及是否仍可用或需要新 `ROUTE_RUN_ID`。
