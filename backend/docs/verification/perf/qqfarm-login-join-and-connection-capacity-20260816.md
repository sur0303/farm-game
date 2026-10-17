# developer_026-qqfarm 登录、Join 与持续连接容量实测

- 日期：2026-08-16
- 集群 / Namespace：`uc-test-plus` / `developer_026-qqfarm`
- 状态：登录与 Join 注入阶梯、持续连接容量阶梯均已完成
- 用途：记录本次实际测了什么、如何判定结果，以及可复现的容量边界

## 1. 测试对象与版本

本轮只测试一套固定单实例业务拓扑：

| 组件 | 实例与资源 | 镜像 |
|---|---:|---|
| Farm Gate | 1 Pod，4 CPU / 8 GiB | `farm-route-runtime@sha256:0d533888...9ec026` |
| PlayerSvr | 1 Pod，4 CPU / 8 GiB | `farm-route-runtime@sha256:0d533888...9ec026` |
| FarmSvr | 1 Pod，4 CPU / 8 GiB | `farm-route-runtime@sha256:0d533888...9ec026` |
| Farm Worker | 2 Pod，各 4 CPU / 8 GiB | `farm-route-runtime@sha256:0d533888...9ec026` |
| Route Controller | 1 Pod，1 CPU / 1 GiB limit | `farm-route-runtime@sha256:0d533888...9ec026` |
| Redis Cluster | 6 Pod | 专用于该 Namespace |

代码基线为 `codex/scaleplan-local-kind-qqfarm-r1@5c516034b3f56df1c8bc2e1900b0ec18bced8867`。本次发布包含尚未提交的 Gate 连接索引、加厚登录和 Player Bootstrap 摘要改动；发布后 tracked diff 指纹为 `sha256:6abed6f7faf678f8864f8341e54869dfab9cb7ec218f9fe92e3eb44ee24573b0`。该指纹用于复现本轮结果，不能把结果归到纯净基线提交。

加厚登录压测器使用 `farm-ws-stress@sha256:d59aaf49...40bac6`。所有测试前后均执行 `prepare-qqfarm-loadtest` 的 `--require-ready` 预检；Gate、PlayerSvr、FarmSvr、Worker 与 Controller 必须全部 Ready。

## 2. 实际业务链路

每个虚拟玩家都独立完成以下完整链路，不复用其他玩家的 WSS：

```text
HTTP 登录
  -> 建立一条独立 WebSocket
  -> ClientHello 身份激活
  -> Join 自己的农场
  -> Player Actor 路由/加载
  -> Farm Actor 路由/加载
  -> 收到自己的 Farm Snapshot
  -> Ready
  -> 持续保持该 WebSocket，直到全局测试时长结束
```

因此，本报告中的 `Ready` 不是“TCP 已连接”，而是玩家已登录并成功进入自己的农场。连接测试也不是空闲裸 WebSocket 测试，而是先完成上述业务链路后再保持连接。

### 2.1 加厚登录场景（待重新建立容量基线）

原 `join` 场景没有主动请求玩家私有 Bootstrap、好友列表和好友互动机会，属于“进入农场链路”，不等同于 H5 首页全部可用。新增 `thick-login` 场景按 `codex/integration-20260803` H5 的首页启动边界执行：

```text
HTTP 登录并取得 farm_session Cookie + WSS ticket
  -> WSS ServerHello，并精确校验 login player_id/session_id
  -> 三条启动腿并行推进
       1. WSS GET_PLAYER_BOOTSTRAP
          - 个人 profile/display_name
          - progression / wallet / inventory
          - feature_summary.tasks（任务红点摘要）
          - feature_summary.mail/compendium/pet（后端有返回时分别计数）
       2. WSS Join 自己的农场
          - Join ACCEPTED
          - FarmSnapshot
          - SubscriptionReady
       3. HTTP 好友互动机会弹窗
          - GET /api/v1/friends/opportunity
  -> Bootstrap 与 Join 屏障都完成后发送 ClientRecoveryComplete
  -> 好友机会得到权威 FOUND 或 NONE
  -> Ready
  -> 保持该玩家自己的 WSS 连接
```

严格校验项包括：Bootstrap revision 非零；个人 ID 必须等于登录身份；昵称非空；成长、钱包、库存存在；任务摘要版本、日周期和任务行存在；可信服务端时间存在。好友列表为空以及机会为 `NONE` 都是合法权威结果；`UNKNOWN` / HTTP 503 不能记作成功。

`GET_TASK_LIST` 不属于登录屏障。真实 H5 使用 Bootstrap 的 `claimable_count` 渲染任务红点，只有用户打开任务面板时才拉完整任务列表。因此本场景没有为了“加厚”而虚构额外的任务面板操作。新生成的压测玩家也没有浏览器 IndexedDB 中的未决操作，H5 的 pending-operation recovery 在本场景中等价为空集合。

运行方式：

```bash
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 1 1 10s <unused-player-start>
```

第五个参数必须使用本轮未使用过的玩家 ID 区间。复用历史压测玩家会把旧私有状态、邮件或未决工作带入登录基线，既不代表新用户登录，也可能让跨版本遗留冲突污染结果。

结果 JSON 的 `thick_login` 会单独给出 Bootstrap / 机会弹窗错误、个人资料和各类红点摘要加载数、机会接口完成数、FOUND/NONE 数量，以及 Bootstrap、机会弹窗 P50/P95。只有 `attempted == ready == profiles_loaded == task_summaries_loaded == friend_opportunity_loaded == found + none` 且所有错误为 0，才算完整加厚登录成功。好友列表首屏不属于 `thick-login`，由独立 `friend-list` 场景测量。

本报告后续章节中的既有 2500/s 和 30000 稳定连接结论仍是旧 `join` 口径，不能外推为 `thick-login` 容量。加厚场景需要从 1 玩家 smoke 开始重新执行注入速率与持续连接阶梯。

首个 1 玩家探索 smoke（运行 ID `dkqglpf246aq`）完成了个人资料、3 条日任务（其中 1 条可领取）、欢迎邮件红点、空好友列表和权威机会结果 `NONE`；当次 Ready 44.33 ms，Bootstrap 13.03 ms，好友首页刷新 25.23 ms。但它同时观测到图鉴/宠物摘要为 0，因此按最终严格口径不算完整 PASS。

旧 PlayerSvr 镜像的 Bootstrap 尚未填充 `compendium` / `pet` 摘要。全新玩家严格 smoke `dkqgoyapu3ss` 因而得到 `ready=0`，证明严格检查没有放水。2026-08-16 23:39 已将同一 PlayerActor 记录生成图鉴/宠物红点摘要的补丁随运行时 digest `0d533888...9ec026` 发布；发布后的结果见下一节。

### 2.2 新运行时发布与严格 smoke

本次发布严格使用 `prepare-qqfarm-loadtest` 的 clean-reset 和两阶段 bootstrap：三个 Redis master 均先核验角色与 16384 slots，再清理专用 keyspace；bootstrap-only 得到 `COMMITTED revision=1`，随后 12 条 exact claim 得到 `ALREADY_COMMITTED final_revision=13`。Player/Farm 的 Pod UID、StatefulSet UID、ordinal、HolderControl endpoint、TLS identity 与投影 Token 均保留精确校验。

最终 Controller 启动后，先等待 Player/Farm 观察到 revision 15 的 Holder lease 恢复屏障，再启动 Workers 和 Gate。最终 1 Gate、1 PlayerSvr、1 FarmSvr、2 Workers、1 Controller 全部 Ready、restart=0；Player/Farm 各服务 4 个 Shard，后续观测 revision 为 26；Redis 仍为 3 master、16384 slots，`farm_downlink_queue_full_total=0`。发布后的 CPU 瞬时基线为 Gate 14m、Player 3m、Farm 3m、Worker A/B 26m/41m、Controller 6m。

完整 Join smoke：

- run ID：`dkqhbehv7r6y`；player start：`910200000`；
- `attempted=ready=1`，登录/连接/Hello/Join/意外断线错误全部为 0；
- Ready 49.11 ms，其中 Login 26.31 ms、Join Accepted 10.06 ms、Snapshot 6.86 ms；
- 独立 WSS 在成功进入自己农场后保持 5 秒。

严格 thick-login smoke：

- run ID：`dkqhbmpdbbc6`；player start：`910210000`；
- `attempted=ready=1`，Bootstrap/好友启动及所有通用错误全部为 0；
- profile、task、mail、compendium、pet、friend opportunity 各成功加载 1；
- 3 条日任务、1 条可领取任务、邮件红点均被观测；
- Ready 34.46 ms，Bootstrap 4.71 ms，好友启动 24.51 ms。

该新玩家没有预造好友，因此权威互动机会结果为 `NONE`。这证明机会接口能正确完成，不证明好友列表或大好友量成本。压测器另有 `friend-list` / `next-opportunity` / `hint` 场景，fixture 支持 `friend-count=20/100/300`；好友列表容量必须使用独立 `friend-list` 场景测量，不能把机会弹窗 smoke 外推为好友列表容量。

### 2.3 每个账号 50 个好友的数据集与实测

为测量好友数据对登录厚度的影响，新增 `friend-opportunity-perf-fixture` 的固定范围模式。它在专用 qqfarm Redis namespace 中通过生产好友 Pair/FriendIndex 仓储建立关系，并通过 PlayerSvr 初始化好友账号、写入受控 Farm Opportunity Header/Facts；每个测试账号最终逐页校验为 **50 个好友**。50 个测试账号可由多个 viewer 共享，单个好友最多被 300 个 viewer 引用，仍在产品好友上限内。好友农场的 `crop_cycle_id` 使用正式 `sid1_` 稳定 opaque ID，fixture 还会用同一生产协议解码器验证 Header、Facts 和 FarmSvr RPC，避免把 UNKNOWN 当成 FOUND。

单账号严格 smoke：

- viewer：`920000020`；fixture：`qqfarmfriends50smoke`；压测 run：`dkqhq0flyvl8`；
- `verified_friend_count=50`，关系 50 条、双向索引 100 行，10 个好友为可互动候选；
- `attempted=ready=1`，登录、Hello、Join、Bootstrap、好友启动及意外断线错误均为 0；
- 好友首屏返回 20 张卡片（接口分页上限），互动机会权威结果为 `FOUND`；
- Ready 104.44 ms，好友启动 90.39 ms。

200 账号探索档（每人 50 个好友，共 10,000 条 viewer→friend 关系）先以 100 登录/秒运行，185 个账号的好友请求收到 Gate 的 HTTP 429。原因是当前配置的 `GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=20`、`GATE_FRIENDSHIP_HTTP_RATE_BURST=40`，而每个 thick-login 同时发起好友列表和机会两条请求；这是明确的保护性限流，不是 Pod 宕机，也没有关闭该保护。

在限流预算内复测：

- fixture：`qqfarmfriends50x200r2`，viewer `922000000–922000199`，每人 50 个好友；
- 压测 run：`dkqhryopeyhe`，200 个独立登录/WSS/Join，注入 5/s，保持 50 秒；
- `attempted=ready=200`，所有通用错误和好友错误均为 0，意外断线为 0；
- profile/task/mail/compendium/pet 各 200 次成功，好友列表 200 次成功、首屏好友卡片 4,000 张、互动机会 `FOUND=200`；
- Ready P50/P95/P99 为 95.10/112.27/125.56 ms；好友启动 P50/P95 为 82.54/96.28 ms；
- 业务 Pod 全部 Ready、restart=0，Farm `farm_downlink_queue_full_total` 仍为 0。

因此，当前已经证明“每个压测用户有 50 个好友，并完成登录到自己农场、加载好友首页和互动机会”这条链路可运行；200 人档的安全吞吐受 Gate 好友 HTTP 限流约束，不能把 100/s 的 429 结果解释为 Gate/Player/Farm 的计算容量上限。

### 2.4 50 好友 Thick Login G2 探索阶梯

为继续执行压测方案的 G2 阶梯，使用 300 个不重叠 viewer、每人 50 个好友、20% 机会密度、30 秒 Job。默认 Gate 好友保护限流 `20/40` 先临时提高到 `10000/20000`，避免把保护性 429 混入容量结果；全部档位结束后已恢复 `20/40`，并重新通过 `prepare-qqfarm-loadtest --require-ready`。完整 JSON、fixture 和命令见 [`qqfarm-friend-50-20260817/`](qqfarm-friend-50-20260817/)。

| 注入速率 | Ready | Ready P95 | 好友启动 P95 | 主要结果 | Run ID |
|---:|---:|---:|---:|---|---|
| 20/s | 300/300 | 118.48 ms | 99.35 ms | 功能通过 | `dkqiafugcsh6` |
| 50/s | 300/300 | 170.08 ms | 148.56 ms | 功能通过 | `dkqibb35379p` |
| 75/s | 300/300 | 2522.81 ms | 2302.66 ms | 功能通过但已进入延迟拐点 | `dkqic7p6pohx` |
| 85/s | 256/300 | 3166.83 ms | 2880.90 ms | 44 个好友启动 503 | `dkqidmuwkk6d` |
| 100/s | 177/300 | 3722.71 ms | 3061.64 ms | 123 个好友启动 503 | `dkqi9c47mn2c` |
| 500/s | 59/300 | 4100.70 ms | 3710.95 ms | 503、Bootstrap/网络超时 | `dkqi6r1hlq6t` |

这些是 300 人短批探索，不是方案要求的 10 分钟持续档，不能称为生产安全容量。失败档的 HTTP 结果是好友服务 503 和少量 Bootstrap `ERR_TIMEOUT`，不是 Gate 429；Gate 连接拒绝、Farm 下行 `queue-full`、Actor mailbox reject 均为 0。当前可复现的 50 好友厚登录结论是：20/50/s 功能通过，75/s 出现明显延迟拐点，85/s 开始失败；`FRIENDSHIP_RPC_TIMEOUT=2s` 下，冷 Player/Farm Actor 激活和好友读取是先暴露的瓶颈。

### 2.5 机会接口-only 部署复验（2026-08-17）

按 `prepare-qqfarm-loadtest` 重新执行专用 Redis clean-reset、revision 1 bootstrap、12 条 exact claim（revision 13）和最终 Controller/Worker/Gate 启动。新版压测器镜像为
`farm-ws-stress@sha256:8e66feebce2b741c04c00744089dd47b09eb9c6ec3c2d1f30ba1e1351973008f`，该版本的 thick-login 只调用 `GET /api/v1/friends/opportunity`，不读取好友列表首页；完整原始结果保存在 [`qqfarm-opportunity-only-20260817-r2.json`](qqfarm-opportunity-only-20260817-r2.json)。

部署复验通过：`preflight --require-ready` 为 PASS；Player/Farm 均 `status=ready`、`serving_shard_count=4`、`installed_memberships=4`、`pending_actions=0`、`degraded_shards=0`，Gate `/readyz` 为 ready，Redis 为 3 master、16384 slots、`cluster_state=ok`。

新版机会-only smoke：run `dkquzuvvgsxh`，玩家区间从 `930120000` 开始，`attempted=ready=1`，登录、连接、Hello、Join、Bootstrap、机会启动和意外断线错误全部为 0；个人资料、任务、邮件、图鉴、宠物摘要各加载 1，任务 3 行/1 条可领取，机会接口权威结果为 `NONE`，Bootstrap P50/P95 为 12.64/12.64 ms，机会启动 P50/P95 为 15.18/15.18 ms。

本次当前 Pod 仍可用，但两个 Worker 在首次 authority lease 收敛期间各有 3 次容器重启；后续已持续 Ready，Player/Farm/Gate/Controller 重启数为 0。因此本轮是“功能与就绪通过、Worker 非零重启已记录”，不是严格的全 workload 零重启认证。

### 2.6 机会接口-only 100/s pprof 定位

在重新创建两个 Worker Pod、使全部业务 workload restart=0 后，使用 300 个全新 viewer、每人 50 好友、20% 机会密度复现机会-only `thick-login` 100/s。Run `dkqvj3yzpuf6` 为 300/300 Ready、FOUND 300、所有错误与断线为 0，但机会 P95 为 1473.48ms，登录 Ready P95 为 1629.67ms。

同批服务端增量显示，Player `GetNextFriendOpportunity` 300 次累计 203.60s，平均 678.67ms；Farm narrow Header 600 次平均 2.52ms，Facts 精筛 600 次平均 2.42ms且全部在 10ms 内。Gate 12s CPU pprof 仅采样 0.92 CPU-s，机会 handler 累计 CPU 10ms。瓶颈因此不在 Gate 或 Farm Header，而在 PlayerSvr 首次机会导航的完整名册/Pair 复验、导航 Load/CAS Save 和多阶段编排。完整证据见 [`qqfarm-opportunity-pprof-20260817/`](qqfarm-opportunity-pprof-20260817/)。

基于该结果，后续压测口径调整为：机会请求仍与 Bootstrap/Join 并行发起，但登录 Ready P95 只计算登录、Hello、Bootstrap 与 Join/Snapshot；机会 attempted/completed/FOUND/NONE/error 和 P50/P95/P99/Max 独立统计，机会失败不能篡改登录 Ready，也不能从结果中隐藏。

## 3. Gate 热点优化与同口径对比

优化前，`Gate.handleHello` 在全局互斥锁内遍历全部连接来查同一玩家，连接爬升时形成 O(N²) 扫描。10000 玩家、1000 玩家/秒的 pprof 中，该扫描累计约 3.22 CPU 秒。

本轮改为 O(1) 的 `playerID -> connectionID + generation` 二级索引，同时保留精确 generation 与 authority 顺序校验。验证包括全仓测试、`go test -race ./internal/gateway/command` 和全仓 vet。

| 10000 玩家，1000/s | Ready | P50 | P95 | P99 | Gate CPU 增量 | 意外断线 |
|---|---:|---:|---:|---:|---:|---:|
| 优化前 | 10000 | 33.53 ms | 169.39 ms | 247.44 ms | 约 25.85 CPU-s | 251 |
| 优化后（新玩家区间） | 10000 | 31.06 ms | 43.46 ms | 65.58 ms | 约 21.86 CPU-s | 11 |

优化后的登录、连接、Hello、Join 错误均为 0；11 次断线出现在全局结束边界。对应运行 ID 为 `dkqf8iv8reh4`。

另一次优化后试跑为 9998/10000：一条在 Controller Snapshot revision 切换瞬间收到 `ERR_STORE_UNAVAILABLE`，一条为 Join barrier 网络超时。该次结果没有被覆盖或伪装成成功，随后用全新玩家区间完成了上表的全通过试验。

## 4. 登录到 Join 的注入速率阶梯

每档都是 10000 个独立玩家、全局时长 15 秒。`注入速率` 表示每秒新启动多少条完整登录 + WSS + Join 流程，不等同于 Farm 业务消息吞吐。

| 注入速率 | Ready | P50 | P95 | P99 | 全链路错误 | Farm 下行 queue-full 增量 | 运行 ID |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1500/s | 10000 | 27.51 ms | 35.02 ms | 44.19 ms | 0 | 0 | `dkqff40ret2h` |
| 2500/s | 10000 | 33.31 ms | 81.80 ms | 102.73 ms | 0 | 0 | `dkqffkors2ts` |
| 3000/s | 10000 | 23.41 ms | 153.51 ms | 192.92 ms | 0 | 30 | `dkqfhpnnvx35` |
| 4000/s | 10000 | 329.85 ms | 1790.48 ms | 2223.82 ms | 0 | 19587 | `dkqfg4szdu43` |

结论：

- 当前已验证的“无 queue-full”最高注入速率是 2500 个完整玩家/秒。
- 饱和起点在 2500/s 到 3000/s 之间；3000/s 已出现少量拒绝，不能记为无损容量。
- 4000/s 虽然最终 10000 人都 Ready，但 P95 已到 1.79 秒，并出现大量下行队列拒绝，属于过载结果。
- 2500/s 档位的粗略平均 CPU 为 Gate 约 1.17 核、PlayerSvr 约 0.48 核、FarmSvr 约 0.50 核；Gate 是 CPU 最忙的业务组件，但没有先碰到 4 核上限。
- 首个明确硬瓶颈是 FarmSvr 到单个 Gate 的 Snapshot/downlink lane：默认单 Gate 一条串行 lane，队列 256、批大小 32。`queue-full` 表示队列已满后拒绝，不是普通排队。
- PlayerSvr 不是本轮最先达到的瓶颈。

4000/s 后已确认 lane、Gate writer 队列和积压全部归零；随后 1 玩家完整链路 smoke 通过，组件仍为 Ready、0 重启。累积 counter 不会自行归零，后续测试只比较增量。

### 4.1 Farm downlink batch=64 同口径控制档（2026-08-17）

按 `prepare-qqfarm-loadtest` 冷部署：Worker 在 revision 1、initial Controller 仍运行时预热 Ready，再提交 12 claims 到 revision 13。FarmSvr `DOWNLINK_BATCH_SIZE=64`，队列仍 256、Gate lane 仍 1。证据目录：[`qqfarm-downlink-batch64-20260817/`](qqfarm-downlink-batch64-20260817/)。

| batch | 注入速率 | Ready | P50 | P95 | P99 | queue-full Δ | 意外断线 | 判定 | Run ID |
|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 32 | 2500/s | 10000 | 33.31 ms | 81.80 ms | 102.73 ms | 0 | 0 | PASS | `dkqffkors2ts` |
| 64 | 2500/s | 10000 | 55.08 ms | 222.35 ms | 252.89 ms | 0 | 24 | FAIL | `dkqwrtrupqtd` |

batch=64 控制档功能上 10000 人都 Ready，且没有 queue-full，但 P95 超过本轮 200ms 探索门槛，并有 24 次非预期断线（`peak_active=9976`）。按交接停止条件未上探 3000/s。测后 1-player smoke `dkqwsbmy61jo` 与 `--require-ready` 通过，业务 Pod restart=0。

这不能证明 batch=64 抬高了无损拐点，也不能单独归因于 batch：该档是冷部署后的第一档大流量，P95 相对 batch=32 基线变差。3000/3500/4000 仍待在稳定现场复测。

## 5. 持续连接容量测试方法

目标是找出单 Gate 在玩家已进入自己农场后的最大稳定连接数。每档采用互不重叠的玩家区间，并用多个压测 Pod 分摊连接：单 Pod 的 `ip_local_port_range` 为 `32768-60999`，只有约 28232 个源端口，不能让这个客户端限制冒充 Gate 限制。

实际执行了 20000、30000、32000、35000 和三次 40000 档位。每个压测 Pod 最多维持 10000 条连接，累计注入速率不超过 1000/s，低于已经验证无损的 2500/s；120 秒档位在约 40 秒爬升结束后保留约 80 秒全量稳态观察。

每档必须同时记录：

- `attempted / ready / peak_active / unexpected_disconnects`；
- 登录、Connect、Hello、Join 和 Snapshot 各类错误；
- Gate 的 `farmgate_ws_conn_active`、`process_open_fds`、RSS、CPU、goroutine；
- PlayerSvr、FarmSvr 的 CPU、RSS、重启与 readiness；
- Farm 下行 `queue-full` 增量和 Gate writer queue；
- 压测 Pod 数、每 Pod 连接数与玩家 ID 区间。

稳定通过口径：全部目标玩家 Ready；稳态保持期间连接数不持续下降；非结束边界的意外断线为 0；业务组件无重启、无 OOM、readiness 不翻转；queue-full 不增长；P95 不超过 200 ms。第一档不满足时停止上探，保留证据并缩小边界。

## 6. 持续连接实测结果

| 目标连接 | 压测 Pod | Ready | 峰值活跃 | 意外断线 | queue-full 增量 | P95 范围 | 判定 |
|---:|---:|---:|---:|---:|---:|---:|---|
| 20000 | 2×10000 | 20000 | 20000 | 0 | 0 | 32.52–38.52 ms | PASS |
| 30000 | 3×10000 | 30000 | 30000 | 0 | 0 | 32.14–32.74 ms | PASS |
| 32000 | 4×8000 | 32000 | 31972 | 28 | 0 | 28.16–36.62 ms | FAIL |
| 35000 | 3×10000 + 1×5000 | 35000 | 34962 | 38 | 0 | 27.92–44.59 ms | FAIL |
| 40000，首次 | 4×10000 | 39996 | 约 39987 | 12 | 280 | 70.71–121.10 ms | FAIL |
| 40000，受控复测 1 | 4×10000 | 40000 | 39877 | 123 | 0 | 71.12–78.00 ms | FAIL |
| 40000，受控复测 2 | 4×10000 | 40000 | 39971 | 29 | 678 | 92.52–124.30 ms | FAIL |

受控 40000 复测把四个压测 Pod 强制分散到不同节点，并排除曾两次集中出现断线的 `198.18.0.34`。复测仍然出现非零意外断线，因此 40000 的不稳定可复现；断线数量从 123 波动到 29，说明严重程度有随机性，但不能把失败归为单次偶发。

本轮可交付容量结论：

- 单个 4 CPU / 8 GiB Gate 已严格验证可稳定维持 **30000 条**“登录 + 独立 WSS + Join 自己农场 + 收到 Snapshot”后的连接。
- 32000 是首个严格失败档，真实零损边界位于 **30000 到 32000** 之间；31000 未测，不能宣称精确理论上限就是 30000。
- 40000 个玩家都能完成 Ready，但连接保持阶段会丢 29–123 条，并可能再次触发 Farm 下行 queue-full，所以只能称为可达峰值，不能称为稳定容量。
- 30000 稳态观测值：Gate 约 30090 FD、3.51 GB process RSS，采样 CPU 约 1.07 核；PlayerSvr / FarmSvr 仍 Ready、0 重启，queue-full 增量为 0。
- 40000 复测时 Gate 约 40063 FD、4.39 GB process RSS，仍未碰到 4 CPU / 8 GiB 硬上限；Gate rejection 和 slow-client kick 均为 0。因此当前首个失败表现是未分类的连接中断及偶发 Farm 下行背压，而不是明确的 Gate admission 拒绝、OOM 或 CPU 打满。

最终压测后完整 1 玩家 smoke（运行 ID `dkqg6te361ww`）为 1/1 Ready，全部错误与意外断线为 0；`prepare-qqfarm-loadtest --require-ready` 再次通过。

## 7. 结果使用边界

本报告是 `uc-test-plus` 单实例拓扑的探索性容量验证，不是 3000 万 DAU 容量结论，也不是生产 SLO 认证。正式容量结论仍需固定干净提交、镜像 digest、原始 JSON/Prometheus 证据，并进行更长时间稳定性与故障注入测试。
