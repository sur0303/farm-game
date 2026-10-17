# 11 · FarmFrame 编码规格

版本：v0.2  
状态：目标合同已冻结；现有 `frame.proto`、生成代码和 codec 仍是 v0.1，实现 S2 时统一替换。

本文定义 **TCONND BY_WS 透传业务字节中的农场应用帧**。TCONND 和 gateproxy transport 只把这段数据当作 opaque bytes，`farm-gate` 与 H5 才解析 FarmFrame。

---

## 1. 冻结结论

P0 不再使用：

```text
total_size | meta_size | FrameMeta(pb) | Body
```

统一改为一个 Protobuf：

```proto
enum FrameKind {
  FRAME_KIND_UNSPECIFIED = 0;
  FRAME_KIND_CLIENT_HELLO = 1;
  FRAME_KIND_SERVER_HELLO = 2;
  FRAME_KIND_CLIENT_COMMAND = 3;
  FRAME_KIND_COMMAND_RESULT = 4;
  FRAME_KIND_SERVER_PUSH = 5;
  FRAME_KIND_CONNECTION_ERROR = 6;
}

message FarmFrame {
  uint32 protocol_version = 1;
  FrameKind kind = 2;
  uint32 connection_seq = 3;

  reserved 4, 5, 6, 7;
  reserved "server_seq", "operation_id", "error_code", "timeout_ms";

  bytes body = 15;
}
```

每个 WebSocket binary message 恰好承载一个序列化后的 `FarmFrame`。

真实 TCONND 联调必须验证 BY_WS 经 gateproxy 后是否保持这一消息边界。如果平台把单个 WebSocket message 分段交付，farm-gate transport 应依据官方长度/分段语义重组后再产生一个 `Data` 事件；不能因此恢复农场应用层的 `total_size/meta_size`。

字段含义：

| 字段 | 含义 | 是否参与业务正确性 |
|---|---|---|
| `protocol_version` | 农场外部协议版本；首帧即校验，连接内不得改变 | 只负责兼容性 |
| `kind` | 决定 `body` 应按哪一种控制消息或 Envelope 解码 | 负责协议分发 |
| `connection_seq` | 当前连接、当前发送方向的观测序号 | 只做链路完整性与排错，不做业务幂等 |
| `body` | ClientHello、ServerHello 或三大 Envelope 等序列化字节 | 由对应消息定义语义 |

`request_id`、`operation_id`、`error_code`、`farm_version` 不进入 FarmFrame，仍分别以业务 Envelope 为唯一真值。

## 2. 为什么删除 total_size、meta_size 和 FrameMeta

当前真实链路已经有三层消息边界：

```text
浏览器 ↔ TCONND：WebSocket message
TCONND ↔ gateproxy：TFRAMEHEAD 中的数据长度
gateproxy ↔ farm-gate：GateStream Inproc body / gRPC message
```

因此：

- `total_size` 不再提供新的截断保护，接收方直接在 Protobuf 反序列化前检查 `len(bytes)`；
- `meta_size` 只服务于旧的 Meta/Body 拼接布局，单一 FarmFrame 不再需要；
- 继续保留自定义 8 字节头，会让 H5、压测器和 farm-gate 多维护一套大端长度协议；
- “裸 TCP 压测方便”不能反向污染生产应用协议。若压测工具使用裸 TCP，由该测试 Transport 自己增加 length-prefix，不把它定义成 FarmFrame 的一部分。

删除这些字段不会削弱 TCONND/WSS 下的可靠性。

## 3. kind 与 body 的唯一映射

| kind | 方向 | body 类型 | 允许状态 |
|---|---|---|---|
| `CLIENT_HELLO` | H5 → farm-gate | `ClientHello` | 仅未认证，且必须是第一帧 |
| `SERVER_HELLO` | farm-gate → H5 | `ServerHello` | ClientHello 核销成功后 |
| `CLIENT_COMMAND` | H5 → farm-gate | `ClientCommandEnvelope` | 已认证 |
| `COMMAND_RESULT` | farm-gate → H5 | `CommandResultEnvelope` | 已认证 |
| `SERVER_PUSH` | farm-gate → H5 | `ServerPushEnvelope` | 已认证 |
| `CONNECTION_ERROR` | farm-gate → H5 | `ConnectionError` | 可安全回包时；随后关闭连接 |

连接级错误使用：

```proto
message ConnectionError {
  ErrorCode error_code = 1;
  string error_message = 2;
}
```

它只表达无法归属某个 `request_id` 的建连、版本或帧错误。正常业务拒绝仍使用 `CommandResultEnvelope`，不能用 ConnectionError 替代。

客户端不得发送服务端方向的 kind；未认证连接不得发送 `CLIENT_COMMAND`。方向或状态不合法时，farm-gate 不解析领域 body，直接记录、尽量返回 ConnectionError 并关闭。

## 4. protocol_version

- 初始实现只接受一个配置化版本；
- `FarmFrame.protocol_version` 是外部协议版本的唯一真值；
- `ClientHello` 不再重复保存 protocol_version，旧字段号 2 需要 `reserved`；
- farm-gate 在解析具体 body 前先检查版本；
- 同一 connection generation 内版本发生变化，按协议违规关闭；
- `ServerHello` 表示 farm-gate 已接受该版本，不需要再重复返回另一份版本字段。

版本不兼容时，如果 FarmFrame 本身仍可安全解析，返回 `ConnectionError(ERR_UNSUPPORTED_VERSION)` 后关闭；否则直接关闭。

## 5. connection_seq

原来的 `client_seq` 与 `server_seq` 合并为一个字段，是因为方向已经由连接端点和 kind 确定：

```text
H5 → farm-gate：H5 维护自己的 connection_seq
farm-gate → H5：farm-gate 维护自己的 connection_seq
```

规则：

- 每个方向都从 1 开始严格递增；
- 新 connection generation 重新从 1 开始；
- `0` 非法；
- 达到 `uint32` 上限前主动重连，不允许回绕；
- 它不作为 ACK，不支持按序号补发，也不代替 request_id、operation_id 或 farm_version；
- farm-gate 发现上行缺口、重复或倒退时，不执行该帧并关闭连接；
- H5 发现下行缺口时停止应用后续业务帧，重新认证并 Join；不确定强写继续按原 operation_id 查询。

TCONND/TCP 理论上已保证有序；序号的价值是发现 gateproxy、队列或客户端实现错误，而不是重新实现 TCP。

## 6. timeout 与 deadline

删除 `FrameMeta.timeout_hint_ms`：

- H5 的页面等待超时是客户端本地策略；
- farm-gate/Logic 根据命令类别配置服务端 deadline，并受全局上限约束；
- 强写超时后由 operation_id 恢复；
- Join 超时后重新认证并使用新的 request_id Join；
- 客户端不能通过一个传输字段延长服务端资源占用。

若未来某类只读请求确实需要客户端预算提示，应将它加入该命令合同，而不是恢复全局 FrameMeta。

## 7. 大小限制与压缩

接收顺序：

```text
gateproxy/TCONND 长度与队列限制
→ farm-gate 在 Unmarshal 前检查 len(bytes)
→ FarmFrame Unmarshal
→ kind / 状态 / seq / body 长度检查
→ 对应 body Unmarshal
```

大小策略：

- 未认证阶段使用独立的小上限，建议 8 KiB，足够容纳 ClientHello；
- 认证后的有效上限由 `ServerHello.max_frame_bytes` 告知 H5；
- 有效值必须是 TCONND、gateproxy、farm-gate 三者上限的最小值；
- farm-gate 保留 1 MiB 硬上限作为安全边界，但生产配置必须经过 TCONND T5 实测，不能把 1 MiB 当作容量结论；
- P0 的 FarmSnapshot 必须在单帧上限内，不设计通用分片；
- P0 不增加应用层 compression/flags。是否启用 WebSocket/TCONND 压缩由真实 CPU、带宽和弱网测试决定。

## 8. 错误处理

| 错误 | 处理 |
|---|---|
| 字节数超过当前阶段上限 | 不反序列化；尽量返回 `ERR_MESSAGE_TOO_LARGE` 后关闭 |
| FarmFrame 反序列化失败 | 记录采样日志并关闭，不回显原始 bytes |
| 不支持的 protocol_version | `ConnectionError(ERR_UNSUPPORTED_VERSION)` 后关闭 |
| kind 未知、方向错误或状态错误 | 连接级协议错误后关闭 |
| body 与 kind 不匹配或反序列化失败 | `ConnectionError(ERR_DECODE)` 后关闭 |
| connection_seq 缺口/重复/倒退 | 不处理当前帧；关闭并重连 |
| 下行 ConnectionWriter 溢出 | 尽量发送 ResyncRequired；无法排队则关闭 |

错误日志不得记录完整 connection_ticket、SessionPermit 或未经裁剪的 body。

## 9. 与 TCONND 和 gateproxy 的边界

```text
H5 ws.send(proto.Marshal(FarmFrame))
→ TCONND BY_WS 解 WebSocket
→ TFRAMEHEAD(INPROC) + opaque FarmFrame bytes
→ gateproxy GateStream Inproc
→ farm-gate 校验并解析 FarmFrame
```

TFRAMEHEAD START/STOP（或 BY_WS 等价事件）经 gateproxy 成为连接生命周期事件，不封装成 FarmFrame 发给 H5。TCONND 和 gateproxy 均不检查 FarmFrame kind、operation_id 或 farm_version。

## 10. S2 实现迁移与验收

进入 S2 编码时一次性完成：

1. `frame.proto` 删除 `FrameMeta`，新增 `FrameKind/FarmFrame`；
2. `control.proto` 新增 ClientHello、ServerHello、ConnectionError；
3. ClientHello 的旧 protocol_version 字段号 2 设为 reserved；
4. ClientCommandEnvelope 删除重复 protocol_version/client_seq 并 reserved；
5. ServerPushEnvelope 删除重复 server_seq 并 reserved；
6. 删除 `total_size/meta_size` codec，改成单 FarmFrame Protobuf 编解码；
7. 测试覆盖往返、未知 kind、方向/状态错误、版本错误、序号缺口、超限、畸形 protobuf 和敏感日志；
8. FakeTransport 与真实 gateproxy transport 使用同一组 FarmFrame 测试向量；
9. `go test ./...` 与 H5↔TCONND 最小联调通过。

现有 v0.1 Proto、生成代码和 codec 在实施上述迁移前仍可能通过测试，但不能据此反推最终协议。
