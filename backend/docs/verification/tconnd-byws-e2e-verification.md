# 农场游戏 WebSocket 接入 TCONND：端到端验证与操作手册

> 状态：**已端到端跑通（本地 Docker）**
> 结论：农场游戏可以用 WebSocket 直连 TCONND（BY_WS 模式），**但必须使用完整实现 BY_WS→TBUS 路由的 TCONND 3.4.x（本地 `gf-3.4.36`），不能用 UC 生产的 `mfcn/tconnd:0.0.13`**。
> 参考文档：KM 317872《TCONND Websocket 模式说明》（thomaspan，2017 发布 / 2021 更新）。

---

## 0. TL;DR（给赶时间的人）

1. **能不能用 WS 接 TCONND？** —— 能。BY_WS 是 TCONND 官方模式，端到端链路已在本地实测跑通。
2. **用哪个 TCONND？** —— `tconnd 3.4.x`（本地镜像 `local/tconnd:gf-3.4.36`）。UC 生产用的 `mfcn/tconnd:0.0.13` 的 BY_WS 是半成品：只做 WebSocket 101 升级握手，**不把连接路由进 TBUS**，客户端会被静默丢弃（0 帧）。
3. **完整链路：**
   ```
   H5(ws://) → TCONND(BY_WS) → TBUS → gateproxy → GameSvr(farm-gate)
        ↑ {"tws_notify_type":"pass"} ← START-ack(分配 id) ←────────────┘
   ```
4. **修了一个真实 bug：** farm-gate 的 `internal/gateway/transport/gateproxy/gateproxy.go` 在回 START 应答时**原样回显了 TCONND 传来的哨兵 id（-1）**，导致 TCONND 收不到有效应答、超时关闭客户端、永远不下发 `pass`。已按 UC 生产模型修正（分配真实 id + 连接键改用稳定的 ConnIdx）。
5. **本地开发无需** UC 测试域名 / SSL 证书 / WSS / CLB，用 `ws://IP:端口` 即可；准生产验收再开 `UseSSL=1`。

---

## 1. 背景与问题

农场游戏（H5 + Go 后端）需要让浏览器通过 WebSocket 接入。UC 的接入层是 `TCONND + gateproxy + 业务容器` 同 Pod 模式，TCONND 支持多种运行模式，其中 `LenParsertype=BY_WS` 是专门为 H5/WebSocket 设计的模式。

问题：**农场游戏能不能直接复用这套，用 WebSocket 接 TCONND？** 之前一直卡在"WebSocket 101 升级成功，但 gateproxy 收不到 START/INPROC，业务拿不到连接事件"，怀疑是 TCONND 版本或配置问题，需要验证。

---

## 2. KM 317872 给出的官方设计（要点）

- TCONND 靠 `LenParsertype` 区分模式：`TDR`（二进制透传）/ `QQ`（端游）/ `GCP`（手游）/ **`BY_WS`（H5/WebSocket）**。
- BY_WS 模式提供鉴权、账号映射、排队、流控，**不做加密**（加密交给 wss/TLS）。
- 与 GameSvr 交互和 GCP 模式基本一致：走 TBUS，用 START / STOP 包管理连接生命周期。
- 与客户端有 **4 条文本控制协议**：上行鉴权请求（参数塞 URL query）、下行排队通知 `queue`、关闭通知 `close`、连接成功通知 `pass`。
- 正确配置结构（嵌套 `WSParser` 子块）：
  ```xml
  <PDUs type="PDU">
    <LenParsertype>BY_WS</LenParsertype>
    <LenParser type="PDULenParser">
      <WSParser type="PDULenWSParser">
        <NoAuth>1</NoAuth>              <!-- 非 0 = 开无鉴权开发模式 -->
        <IsIDMappingOn>0</IsIDMappingOn>
        <EnableTransparent>0</EnableTransparent> <!-- 0 = 标准 WS 模式（会下发 pass 等通知）-->
        <IsAuthRefreshOn>0</IsAuthRefreshOn>
      </WSParser>
    </LenParser>
  </PDUs>
  ```
  > 注意：`WSParser` 用字段 `<NoAuth>`；GCP 模式的 `GCPParser` 用的是 `<EnableNoAuth>`，两者不要混用。
- wss 需 `<UseSSL>1</UseSSL>` + `<SSLConfig>` 配证书/私钥。**本地开发不需要**，用 `ws://` 即可。
- 集群化：登录时用 `server_id` 或 `type_id+zone_id` 选登录点；连接建立后消息固定发往登录点，要换点需 GameSvr 发 `SetRoute`。**注意区分：TCONND route 决定进哪个接入点，Actor Router 决定进哪个 PlayerActor/FarmActor，二者不要混。**

---

## 3. 从"不行"到"跑通"的完整过程与思考

验证装置（本地 Docker，`farm-game/deploy/docker-compose.probe-only.yml`）：
- `tconnd`：BY_WS 模式，监听 `ws://0.0.0.0:9980`
- `gateproxy`：通过 TBUS 连 tconnd，对 GameSvr 暴露 GateStream（unix socket）
- `tconnd-probe`：纯观测端，只记录 gateproxy→GameSvr 的 START/INPROC/STOP，**不回应**
- `ws-driver`（新增）：真正的 WebSocket 客户端，连 tconnd、打印收到的帧
- `tconnd-responder`（新增）：最小 GameSvr，收到 START 就回 ack（用来触发 `pass`）

### 实验一：生产同款 `mfcn/tconnd:0.0.13`
- ws-driver：`101 upgrade OK` → **0 帧** → 约 2.7s 后 EOF（被静默丢弃）。
- probe：只有 `connection:-1 / auth_type:65535` 的哨兵 START，固定 4s 一次，与客户端连接/断开无关。
- **判定：0.0.13 的 BY_WS 是半成品，WebSocket 会话根本不进 TBUS，配置怎么调都没用。**

### 实验二：完整版 `local/tconnd:gf-3.4.36`（无鉴权参数）
- ws-driver 收到真实文本帧：
  ```json
  {"tws_notify_type":"close","reason":34,"excode":0,"detail":"auth param invalid or lost in client request"}
  ```
- **判定：gf-3.4.36 真正实现了 WS 控制协议**（会按文章下发 `close` 文本通知）。它拒绝我们只是因为 URL 没带鉴权参数（3.4.x 即使 `NoAuth=1` 也要求 URL 带鉴权字段，schema 与 0.0.13/文章有差异）。

### 实验三：`gf-3.4.36` + URL 鉴权参数
- ws-driver 收到：
  ```json
  {"tws_notify_type":"close","reason":27,"excode":0,"detail":"connection closed for wait start/relay pkg from svr timeout"}
  ```
- reason 从 34 变 27：**鉴权已过，TCONND 向 GameSvr 发了 START 并在等应答，但超时**（因为 probe 是被动观测端，不回 start-ack）。
- probe 侧记录到一条 START，其 `client_type=1` 正是我们 URL 里传的值 —— **证明 WS 会话已端到端路由到 gateproxy。**

### 实验四：接上最小 GameSvr `tconnd-responder`（第一次，失败）
- responder 收到 START（`conn_idx=4303, client_type=1`）并回了 ack，但**原样回显了 `head.id=-1`**。
- 结果 ws-driver 仍拿到 `close reason=27`。
- **顿悟点**：查 UC 权威实现 `gatesvr/internal/pkg/gate/gate_handler.go` 的 `handleStartMsg`，发现回 START 应答时**不是原样回显 head**，而是：
  ```go
  Head: &proxypb.TconndHead{Id: s.ID(), ConnIdx: head.ConnIdx, Extend: 0}
  ```
  即 **GameSvr 必须分配一个自己的 Id**（`s.ID()`），回显 `ConnIdx`，`Extend` 置 0。TCONND 收到 `-1` 无法据此路由回该连接，所以一直超时。

### 实验五：responder 改为分配真实 id（成功）
- ws-driver 收到：
  ```json
  {"tws_notify_type":"pass"}
  ```
- responder 侧：
  ```
  START  head{id=-1 conn_idx=4303} client_type=1  -> ack 分配 id=1
  INPROC head{id=1  conn_idx=4303} 4 bytes         ← 客户端发的二进制帧透传上来
  STOP   head{id=1  conn_idx=4303}                 ← 断开干净收尾
  ```
- **关键观察**：ack 之后，INPROC/STOP 的 `head.id` 都变成了我们分配的 `1`，而 `conn_idx=4303` 全程不变。说明 **Id 由 GameSvr 分配、之后作为路由标识；ConnIdx 才是连接的稳定标识**。

---

## 4. 由此修复的 farm-gate 真实 bug

`farm-game/internal/gateway/transport/gateproxy/gateproxy.go` 原实现有两处会导致接真 TCONND 时彻底不可用：

1. **START 应答原样回显 head（含 id=-1）** → TCONND 收不到有效应答 → 永远不发 `pass` → 所有连接超时断开。
2. **连接状态用含 Id 的 key**（`tconnd:{id}:{connIdx}:{extend}`）→ START(id=-1) 与后续 INPROC/STOP(id=分配值) 的 key 不一致 → 状态错乱、帧找不到连接、断连计数错误。

修复（已提交，含单测）：
- START 时用分配器 `nextConnID` 分配正整数 id，回 `{Id: assigned, ConnIdx: 回显, Extend: 0}`，并把该 head 持久化用于下行帧。
- `connectionKey` 改为**只用 ConnIdx**（对齐 UC 的 `GetSessionByConnIdx`）。
- 新增单测 `TestTransportHandlesSentinelStartIdFromRealTconnd`：复现 START(id=-1) → INPROC/STOP(id=1) 的真实 wire 行为，断言帧仍能到达 gate client 且只断连一次。

---

## 5. WebSocket 接 TCONND —— 详细操作步骤

### 5.1 TCONND 配置（本地开发）
文件 `farm-game/deploy/tconnd/tconnd_gcp.xml` 的 PDU 段：
```xml
<LenParsertype>BY_WS</LenParsertype>
<LenParser type="PDULenParser">
  <WSParser type="PDULenWSParser">
    <NoAuth>1</NoAuth>
    <IsIDMappingOn>0</IsIDMappingOn>
    <EnableTransparent>0</EnableTransparent>  <!-- 必须 0：透传模式不下发 pass -->
    <IsAuthRefreshOn>0</IsAuthRefreshOn>
  </WSParser>
</LenParser>
```
监听：`<Url>tcp://0.0.0.0:9980?reuse=1</Url>`。本地**不配** `UseSSL`（即 `ws://`）。

### 5.2 镜像
- TCONND：**`local/tconnd:gf-3.4.36`（3.4.x 完整版）**，不要用 `mfcn/tconnd:0.0.13`。
- gateproxy：`local/gateproxy:gf-v0.0.34`（与 gf tconnd 配套）。

### 5.3 GameSvr（farm-gate）侧对 START 的正确应答（重要）
收到 START 后必须回一个带**自分配 id** 的 START 应答，否则 TCONND 不发 `pass`：
```go
Head: &proxypb.TconndHead{Id: <自分配正整数>, ConnIdx: head.ConnIdx, Extend: 0}
Msg:  &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}}
```
后续 INPROC/STOP 会带上这个分配的 id；连接状态请按 **ConnIdx** 索引。

### 5.4 H5 客户端契约（EnableTransparent=0 时）
- 连接：`new WebSocket("ws://host:9980/?account_value=...&account_type=...&appid=...&token=...")`（本地 NoAuth 亦需带这些字段，值可为占位）。
- `onmessage` **必须先分流文本控制帧**：
  ```ts
  function onMessage(e: MessageEvent) {
    if (typeof e.data === "string") { handleTconndControl(e.data); return; } // queue/close/pass
    handleBinaryFarmFrame(e.data);
  }
  ```
- 状态机建议：`Disconnected → ConnectingSocket → WaitingTconndPass → (Queued?) → TconndReady → SendingClientHello → FarmAuthenticated → InFarm`。
- **业务 `ClientHello(connection_ticket)` 必须在收到 `{"tws_notify_type":"pass"}` 之后再发**，否则可能在 TCONND 尚未完成 START 握手时过早发业务帧。

### 5.5 鉴权分两层（不可互相替代）
- **平台/接入鉴权**（TCONND）：URL 里的 `account_value/token/appid` 等；本地 `NoAuth=1` 可绕过。
- **应用 Session 鉴权**（farm-gate）：`ClientHello(connection_ticket)`；**任何环境都不能绕过**——没有 ticket 必须拒绝进入 Actor。

### 5.6 一键复跑本地验证
```bash
cd farm-game

# 1. 构建观测/驱动工具镜像
bash deploy/tconnd-probe/build-image.sh
bash deploy/ws-driver/build-image.sh
bash deploy/tconnd-responder/build-image.sh

# 2. 起 tconnd + gateproxy（gf 完整版）
cd deploy
export TCONND_IMAGE=local/tconnd:gf-3.4.36 GATEPROXY_IMAGE=local/gateproxy:gf-v0.0.34
rm -f /tmp/farm-socket/*
docker compose -f docker-compose.probe-only.yml up -d tconnd gateproxy

# 3. 起最小 GameSvr（回 START ack）
docker run -d --name farm-responder \
  --network farm-game-tconnd-probe_default \
  -v /tmp/farm-socket:/data/unix-socket \
  -e RESPONDER_DURATION=40s farm-game-tconnd-responder:local

# 4. 发起带鉴权的 WS 连接，应收到 {"tws_notify_type":"pass"}
docker run --rm --network farm-game-tconnd-probe_default \
  -e WS_RUN_DURATION=10s -e WS_SEND_HELLO=1 -e WS_SEND_DELAY=3s \
  -e "WS_URL=ws://tconnd:9980/?account_value=devuser&account_type=1&appid=0&token=devtoken&account_format=3&client_type=1" \
  farm-game-ws-driver:local

# 5. 查看 GameSvr 侧 START/INPROC/STOP
docker logs farm-responder | grep -v heartbeat

# 6. 清理
docker rm -f farm-responder
docker compose -f docker-compose.probe-only.yml down
```

---

## 6. 遗留问题 / 后续

1. **gf-3.4.36 的 NoAuth 语义**：为何 `NoAuth=1` 仍要求 URL 带鉴权字段？需确认 3.4.x 的 WSParser schema，明确本地真正免鉴权的配法。
2. **WSS 验收**：准生产需开 `UseSSL=1` + 证书，验证 `wss://` 链路。
3. **多 tconnd 键**：当前 `connectionKey` 只用 ConnIdx，适用于单 Access Pod 内单 tconnd；若一个 gateproxy 后接多个 tconnd 实例，需要 (tconnd 实例, ConnIdx) 组合键。
4. **生产镜像来源**：确认 MF 提供的、可用于生产的 BY_WS TCONND 正式 tag（gf-3.4.36 目前是本地镜像）。
5. **farm-gate 真实联调**：用真实 farm-gate（而非 tconnd-responder）跑通 `pass → ClientHello → 业务` 全流程。

---

## 附录：本次新增/改动文件

- 新增 `farm-game/cmd/ws-driver/`（最小 WebSocket 驱动）+ `deploy/ws-driver/`
- 新增 `farm-game/cmd/tconnd-responder/`（最小 GameSvr，回 START ack）+ `deploy/tconnd-responder/`
- 修改 `farm-game/deploy/docker-compose.probe-only.yml`（新增 `ws-driver` 服务）
- 修改 `farm-game/deploy/tconnd/tconnd_gcp.xml`（`EnableTransparent` 1→0）
- 修复 `farm-game/internal/gateway/transport/gateproxy/gateproxy.go`（START 分配真实 id + ConnIdx 键）
- 新增/更新 `farm-game/internal/gateway/transport/gateproxy/gateproxy_test.go`（含真实 wire 行为测试）
