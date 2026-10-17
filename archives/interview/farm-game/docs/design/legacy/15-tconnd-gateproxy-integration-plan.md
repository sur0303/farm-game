# 15 · TCONND + gateproxy 平台候选适配计划

版本：v0.1

更新时间：2026-07-26

状态：候选验证；不阻塞 Direct WebSocket P0 主链，未完成真实 BY_WS/WSS 探针前不得成为
默认生产入口。

> 2026-07-29 决策更新：P0 默认使用
> `公网 DNS → CLB/TLS → farm-gate Direct WebSocket`。本文仅保留 TCONND + gateproxy 的平台候选验证、
> 兼容性探针和未来切换条件；原文中“禁止 farm-gate 自建生产 WSS”的结论已经废止。两种
> Transport 复用同一个 Gate Core，同一环境只显式启用一种，不做自动回退。详见
> [21-direct-websocket-tlog-freeze](./21-direct-websocket-tlog-freeze.md)。

本文是 [S2 主计划](./14-s2-implementation-plan.md) 中 TCONND 接入工作包的专项实施计划。它回答四个问题：

1. `mfcn/gatesvr`、`mfcn/gateproxy` 和新农场分别使用到什么边界；
2. 在 Linux 云服务器上按什么顺序完成真实接入；
3. `farm-gate` 应实现哪些代码，不应复制哪些代码；
4. BY_WS 或 gateproxy 不兼容时如何决策和回退。

本文不改变 PlayerActor/FarmActor、TcaplusDB CAS、Join/JoiningBuffer 等既有业务合同。

---

## 1. 方案结论

S2 首选链路冻结为：

```text
H5
→ WSS / binary FarmFrame
→ TCONND(BY_WS)
→ TCONNAPI / TFRAMEHEAD / TBUS
→ gateproxy
→ Unix Socket gRPC GateStream
→ farm-gate
→ LogicIngress
→ ActorRouteAdapter
→ PlayerActor / FarmActor
```

各部分的使用方式如下：

| 组件 | 处理方式 | 原因 |
|---|---|---|
| `mfcn/gatesvr` | 只作为部署和接入参考，不作为农场运行依赖 | 它同时依赖 Coordinator、GID、用户服务、Pulsar、Maple 等 MFCN 基础设施，直接使用会引入大量无关耦合 |
| TCONND | 使用批准的内部镜像或官方发布包 | 它拥有公网 Socket、BY_WS/WSS、连接生命周期和底层流控 |
| `mfcn/gateproxy` | 优先作为平台接入组件直接部署，并使用其公开 Go client/Proto | 它已把 TCONNAPI/TBUS 转换为 Unix gRPC GateStream，可避免新项目重复实现 C++/TBUS 适配 |
| `farm-gate` | 新农场独立实现 | 它拥有 FarmFrame、ticket、Session、限流、连接代次、ConnectionWriter 和 Logic RPC，但不拥有农场业务事实 |
| `farm-tconnd-adapter` | 暂不新建 | 只有真实探针证明现有 gateproxy 无法支持 BY_WS，且没有可用平台版本时，才重新评审是否实现最薄兼容层 |

这里的“使用 gateproxy”是把它当作平台依赖和传输边界，不复制 `mfcn/gatesvr` 的 Session、业务协议、路由、ACK、缓存或游戏逻辑。

---

## 2. 已掌握证据与尚未确认事实

### 2.1 已掌握证据

- `mfcn/gatesvr` 的 Go 进程通过默认地址 `/data/unix-socket/gateproxy.sock` 连接 gateproxy；
- gateproxy 对外提供双向流式 `ConnectorService.GateStream`；
- `mfcn/gatesvr` Helm 中，一个接入 Pod 同时包含 TCONND、gateproxy 和 Go gatesvr；
- TCONND 与 gateproxy 使用 TBUS，gateproxy 与 Go 进程使用共享卷中的 Unix Socket；
- `mfcn/gatesvr` 引用了 `git.internal.example.invalid/mfcn/gateproxy/go-client v0.0.9`；
- `mfcn/gateproxy` 仓库包含 TCONND 3.4.36 的二进制、头文件、库和插件；
- TFRAMEHEAD 头文件中存在 `WEBSTART/WEBINPROC/WEBSTOP/WEBRESP`；
- TCONND 内部镜像线索为 `mirrors.tencent.com/mfcn/tconnd:0.0.7`。

### 2.2 尚未确认事实

- `tconnd:0.0.7` 内实际 TCONND 版本及是否启用 BY_WS；
- BY_WS 的真实配置模板、WSS 证书字段、监听和心跳参数；
- 当前 gateproxy 镜像的准确兼容 tag；
- TCONND 在 BY_WS 模式下是否把 Web 命令归一化为普通 `START/INPROC/STOP`；
- 如果仍产生 `WEBSTART/WEBINPROC`，当前 gateproxy 是否支持；
- gateproxy GateStream 下行写成功的精确确认点；
- 最大消息、慢连接、空闲连接和容器重启的真实行为。

因此当前状态是“平台资产已定位，但 BY_WS 链路尚未验收”，不能写成“完全缺少官方包”，也不能写成“已经可以直接生产使用”。

---

## 3. 两个强制决策门

### 3.1 决策门 A：gateproxy 能否直接承接 BY_WS

先运行最小探针，不先修改 farm-gate 主体。

```text
浏览器连接 TCONND BY_WS
→ gateproxy GateStream
→ probe 打印收到的命令类型与连接头
```

结果分三种：

| 实测结果 | 决策 |
|---|---|
| gateproxy 收到普通 `START/INPROC/STOP`，消息边界正确 | 直接使用 gateproxy，删除自研 Adapter 进程计划 |
| gateproxy 能直接输出等价 Web 事件，Go client 可识别 | 在 farm-gate 的 transport mapper 中归一化，不增加新进程 |
| gateproxy 丢弃或无法识别 `WEBSTART/WEBINPROC` | 先向平台仓库确认支持 BY_WS 的版本；没有可用版本时，再评审最薄兼容层 |

禁止在实测前修改整个 `mfcn/gatesvr`，也禁止为了绕过问题让 farm-gate 自建第二个生产 WSS 入口。

### 3.2 决策门 B：gateproxy 下行是否提供逐消息结果

当前 GateStream 是双向消息流，不能预设它提供“浏览器已收到”的 ACK。

S2 的 `QUEUE_ACCEPTED` 始终定义为：

> 下行 item 已进入 farm-gate 当前 connection generation 的有界 ConnectionWriter。

本地可以明确判定：

- `QUEUE_ACCEPTED`：进入本地有界队列；
- `QUEUE_FULL`：本地队列达到 item/byte 上限；
- `CONNECTION_NOT_FOUND`：本地 ConnectionRegistry 无此连接；
- `STALE_CONNECTION`：连接代次不匹配；
- `STALE_SUBSCRIPTION`：订阅代次不匹配；
- `UNAVAILABLE`：gateproxy stream 不可用。

消息写入 GateStream 之后，如果平台没有逐条回执，不得虚构更强确认。Stream 失败时使连接/订阅失效，客户端通过重新认证和 Join 恢复。

---

## 4. 代码与依赖边界

### 4.1 farm-game 目标目录

```text
farm-game/
├── cmd/
│   ├── farm-gate/
│   ├── farm-logic/
│   └── tconnd-probe/
├── internal/gateway/
│   ├── transport/
│   │   ├── transport.go
│   │   ├── fake.go
│   │   └── gateproxy/
│   │       ├── client.go
│   │       ├── stream.go
│   │       ├── mapper.go
│   │       ├── generation.go
│   │       └── metrics.go
│   ├── command/
│   ├── session/
│   └── downlink/
├── deploy/
│   ├── compose/
│   │   ├── compose.core.yaml
│   │   └── compose.tconnd-probe.yaml
│   └── tconnd/
│       ├── README.md
│       ├── config.example/
│       └── secrets.example/
├── scripts/
│   └── probe-tconnd
└── docs/verification/
    └── tconnd-probe-report.md
```

### 4.2 gateproxy client 接入原则

- 优先依赖批准的 `git.internal.example.invalid/mfcn/gateproxy/go-client` 固定版本；
- 在 `go.mod/go.sum` 中固定版本，不跟随浮动 master；
- 记录对应 gateproxy 镜像 tag、仓库 commit 和 TCONND 镜像 digest；
- 不把 `mfcn/gatesvr` 源码复制进农场仓库；
- 不把第三方仓库中的密钥、发布配置或环境地址复制进农场仓库；
- 如果 CI 无法获取私有 Go module，先解决内部 Go Proxy/SSH 权限，不手工复制生成代码制造第二份真值。

### 4.3 农场 Transport 抽象

farm-gate 只依赖本项目定义的窄接口：

```go
type Event struct {
    Kind       EventKind
    Connection ConnectionHandle
    Payload    []byte
    Reason     string
}

type ConnectionHandle struct {
    ConnectionID string
    Generation   uint64
}

type Transport interface {
    Events(ctx context.Context) <-chan Event
    Send(ctx context.Context, handle ConnectionHandle, payload []byte) error
    Close(ctx context.Context, handle ConnectionHandle, reason string) error
    Ready() bool
}
```

实际字段以最终实现为准，但边界必须保持：

- `FakeTransport` 和 gateproxy transport 使用同一接口；
- gateproxy 的 `id/conn_idx/extend` 只存在于 transport 实现内部；
- FarmFrame body 不由 gateproxy transport 解析；
- `connection_id + generation` 防止底层连接索引复用后误发旧数据；
- Transport 不访问 Redis、Tcaplus、PlayerActor 或 FarmActor。

---

## 5. Linux 云服务器实施顺序

### 阶段 TP0：只读资产确认

目标：确认“拿得到什么”，不启动服务。

任务：

1. 通过 SSH 在 Linux 服务器验证 `mfcn/gatesvr` 和 `mfcn/gateproxy` 的只读权限；
2. 记录两个仓库当前使用的 commit，不把它们放进 farm-game 工作树；
3. 验证内部镜像仓库登录权限；
4. 查看 `tconnd:0.0.7` manifest、架构和 digest；
5. 找到可用 gateproxy tag，并确认与 Go client `v0.0.9` 的兼容关系；
6. 找到批准的 BY_WS/WSS 示例配置和证书申请方式；
7. 确认平台组件在新项目中直接依赖/部署的授权边界。

退出条件：

- 镜像、版本、配置来源和权限全部有记录；
- 没有把环境密钥写入仓库；
- 仍缺失的输入有明确责任人和获取方式。

### 阶段 TP1：构建最小 gateproxy probe

目标：只验证连接事件，不引入农场 Session、Actor 或 Tcaplus。

probe 负责：

- 连接 Unix Socket；
- 建立 `GateStream`；
- 记录 Start/Inproc/Stop/Heartbeat 及原始命令类型；
- 对每个底层连接分配本地 generation；
- 原样回显业务 bytes；
- 输出结构化日志和计数指标；
- 在 stream 断开时退出 NotReady。

退出条件：

- probe 可构建、可单测；
- Fake GateStream 能覆盖连接、数据、断开、乱序和 stream 失败；
- 日志中不打印 ticket 或用户隐私。

### 阶段 TP2：启动 TCONND + gateproxy

目标：先完成 T7，再做其他探针。

建议拓扑：

```text
同一 Compose project / 未来同一 Pod

tconnd
  ├── 对外监听 WSS
  └── 与 gateproxy 共享 TBUS 所需 IPC

gateproxy
  ├── 连接 TCONND/TBUS
  └── 在共享卷创建 /data/unix-socket/gateproxy.sock

tconnd-probe
  └── 只挂载 gateproxy Unix Socket，不接触 TBUS
```

Docker Compose 初始假设：

- TCONND 与 gateproxy 共享 IPC namespace；
- gateproxy 与 probe/farm-gate 共享 Unix Socket volume；
- WSS 证书和平台配置只读挂载；
- 镜像固定 digest；
- restart policy 不代替 readiness；
- 如果官方运行方式不支持 Compose，则按原生进程或同 Pod 方式部署，不强行套容器。

退出条件：

- TCONND、gateproxy、probe 均 Ready；
- WSS 端口可从浏览器访问；
- 重启后残留 IPC 能被正确清理或被检测为 NotReady。

### 阶段 TP3：执行 T1～T8

执行顺序固定为：

```text
T7 启动
→ T1 消息边界
→ T2 正常下行
→ T3 断线/慢读/队列
→ T8 心跳与半开
→ T4 顺序
→ T5 最大帧
→ T6 重启与清理
```

每项必须保存：

- 镜像 digest、配置版本和服务器规格；
- 执行命令或自动化脚本；
- 客户端、probe、gateproxy、TCONND 的原始时间关联日志；
- 实际返回值和错误码；
- 结论、未覆盖项和对 FarmFrame/ServerHello 的影响。

没有原始证据的项目保持“未验证”，不以 FakeTransport 结果代替。

### 阶段 TP4：接入 farm-gate

在 TP3 的 A/B 决策门通过后才执行。

任务：

1. 把 probe 的 GateStream client 下沉为 `internal/gateway/transport/gateproxy`；
2. Connected 时创建不可预测 `connection_id + generation`；
3. Data 只把 opaque bytes 投给 FarmFrame decoder；
4. Disconnect 原子移除 ConnectionRegistry，并通知 Session/Presence 过期；
5. ConnectionWriter 先做本地 generation、subscription、item/byte 上限检查；
6. 独立发送协程写 GateStream，不让业务 Actor 等待网络；
7. Stream 中断后使 Access NotReady，并使受影响连接重新 Join；
8. 接入现有 ClientHello、Session、Logic RPC 和 Join 状态机。

退出条件：

- FakeTransport 与 gateproxy transport 通过同一合同测试；
- 同一 connection generation 内消息保持 FIFO；
- 旧 generation 下行必定被拒绝；
- 慢连接不会阻塞 Actor；
- gateproxy/TCONND 重启不会产生业务假成功。

### 阶段 TP5：H5 和真实端到端验收

客户端流程：

```text
HTTPS TestLogin
→ 获得 wss_endpoint + connection_ticket
→ 连接 TCONND
→ ClientHello(ticket)
→ ServerHello
→ JoinFarm
→ ACCEPTED，客户端保持 Joining
→ 收到并应用 FarmSnapshot
→ 客户端进入 InFarm
```

必须测试：

- ticket 只能核销一次；
- 新登录顶掉旧连接后，旧连接不能继续发命令；
- Snapshot 与 JoiningBuffer 无版本空洞；
- TCONND、gateproxy、farm-gate 任一重启后，H5 能重新认证和 Join；
- 浏览器前后台切换、断网恢复和半开连接符合 T8 结论；
- Staging 构建不包含 FakeTransport/MockAuth 回退。

---

## 6. T1～T8 验收表

| 编号 | 要回答的问题 | 最小测试 | 影响的设计 |
|---|---|---|---|
| T1 | 一个 WebSocket binary message 是否对应一个完整上行业务消息；命令是普通还是 WEB 类型 | 0B、1B、1KB、64KB 和边界帧；记录 GateStream | 是否需要分片重组；gateproxy 是否可直接使用 |
| T2 | 下行写入 GateStream 的成功点是什么 | 正常回显并关联浏览器收到时间 | 只定义本地 QueueAccepted，还是能记录更外层接受 |
| T3 | 连接不存在、慢读、队列满、TCONND 停止如何表现 | 断线后发送、暂停读取、限小队列、强杀 | 错误映射和断开/重连策略 |
| T4 | 同连接的消息顺序是否保持 | 双向连续序号和并发发送 | FIFO 保证和 Resync 条件 |
| T5 | WSS、TCONND、TBUS、gRPC、farm-gate 的共同最大帧 | 分档递增直到失败 | `max_frame_bytes` |
| T6 | IPC、Unix Socket、容器重启和 Ready 如何恢复 | 分别重启三个组件并检查残留 | Compose/Pod 与启动门 |
| T7 | 镜像、权限、证书、BY_WS 配置是否真实可运行 | 从空环境部署 | 是否具备真实平台验收资格 |
| T8 | Ping/Pong、空闲超时、前后台切换、半开连接由谁处理 | 静默、切后台、断网、防火墙丢包 | 是否需要应用层 Heartbeat |

---

## 7. 故障语义

| 故障 | 客户端/服务端行为 | 业务强事实 |
|---|---|---|
| TCONND 退出 | Socket 断开；重新登录或刷新 ticket 后 Join | 不回退 |
| gateproxy 退出 | farm-gate transport NotReady；当前连接失效 | 不回退 |
| GateStream 中断 | 停止接受新连接和下行；关闭/失效受影响连接 | 不回退 |
| farm-gate 退出 | Session、ConnectionWriter 丢失；客户端重连 | 不回退 |
| 本地下行 QueueFull | 订阅 stale；Resync 或断开重连 | 不回退 |
| 旧 connection generation 下行 | 拒绝 `STALE_CONNECTION` | 不影响 |
| gateproxy 接受后浏览器未收到 | 客户端版本缺口或超时后重新 Join | 不回滚已经 CAS 的事实 |

网络发送不是金币、库存、作物或 OperationResult 的提交点。

---

## 8. 可观测性与安全

最低指标：

- 当前连接数、连接建立/断开速率；
- GateStream 状态和重连次数；
- 上下行 frame 数、bytes 和非法 frame 数；
- 每连接 ConnectionWriter item/byte 深度；
- QueueFull、StaleConnection、Unavailable 数量；
- ClientHello 成功/失败和 ticket 重放；
- Join 用时、Snapshot 排队用时和重连次数；
- 按组件区分的 CPU、内存、FD、网络和 IPC 使用。

最低关联字段：

```text
trace_id
connection_id
connection_generation
player_id（日志脱敏）
request_id
subscription_id
farm_version
```

安全要求：

- WSS 证书、TCONND 配置密钥、Redis/Tcaplus 凭据不入库；
- 日志不输出 connection_ticket、完整 Cookie 或鉴权 Token；
- 测试阶段即使 TCONND `NoAuth=1`，ClientHello ticket 仍然 fail-closed；
- probe 端口和管理接口只对测试网络开放；
- `mfcn/gatesvr`、`mfcn/gateproxy` 的只读参考目录不进入农场镜像构建上下文。

---

## 9. 初始性能验证

TCONND 探针先验证语义，再逐级加压：

```text
1 个连接功能验证
→ 100 个连接稳定性
→ 1,000 个连接与重连
→ 根据 FD/CPU/内存结果提升到单机安全上限
```

第一轮至少记录：

- 建连和 ClientHello P50/P95/P99；
- 小帧 echo P50/P95/P99；
- 每连接内存和 FD；
- GateStream 批量大小和 CPU；
- 慢客户端比例为 1% 时对健康连接的影响；
- 重启时重连风暴的峰值。

不能直接把 `mfcn/gatesvr` 的历史单机连接数作为农场容量结论。最终单实例连接上限必须使用农场 FarmFrame、BY_WS/WSS、当前证书和云服务器规格压测。

---

## 10. Go / No-Go

### 允许开始 farm-gate 真实接入

必须全部满足：

- T7 真实启动通过；
- T1 证明 gateproxy 能向 Go 侧交付 BY_WS 连接和完整消息；
- T2/T3 已明确发送与错误语义；
- 可用的 TCONND、gateproxy 版本和镜像 digest 已冻结；
- BY_WS/WSS 配置和 Secret 获取方式已确认；
- `tconnd-probe-report.md` 有原始证据链接或日志位置。

### 允许宣布 S2 真实接入完成

必须全部满足：

- T1～T8 完成；
- H5 经真实 TCONND/gateproxy 完成 ClientHello/ServerHello；
- Join/Snapshot/JoiningBuffer/重连通过；
- TCONND、gateproxy、farm-gate 故障注入通过；
- 最大帧、心跳和 Access Ready 已写入配置；
- 没有部署第二套绕过 TCONND 的生产 WebSocket Gateway。

---

## 11. 当前立即执行清单

按顺序只做以下七件事：

1. 在 Linux 云服务器只读验证两个 MFCN 仓库和内部镜像权限；
2. 找到并记录 gateproxy 可用 tag、TCONND 镜像 digest 和 BY_WS 配置来源；
3. 新建 `cmd/tconnd-probe`，只实现 GateStream 事件记录和回显；
4. 建立最小 TCONND + gateproxy + probe 部署；
5. 先完成 T7、T1、T2、T3、T8；
6. 根据结果决定“直接 gateproxy”还是“需要平台兼容版本/最薄适配层”；
7. 决策通过后，才把 GateStream client 接入 farm-gate。

这七项完成前，不开始购买、种植、偷菜，也不为了接入问题改变已冻结的 Player/Farm Actor 路由。
