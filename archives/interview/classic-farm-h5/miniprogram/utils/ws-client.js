// Direct FarmGate WebSocket client for WeChat Mini-Program (protobuf v2).
// Sends ClientHello immediately after the WebSocket 101 upgrade.

const config = require('./config');
const $root = require('./proto/farm_pb.js');

const common = $root.common;

/**
 * Build the direct FarmGate WebSocket URL.
 */
function buildWsUrl() {
  // 直连模式:使用配置的 wsPath(默认 /ws),不再拼 TCONND query 参数。
  const path = config.wsPath || '/ws';
  return `${config.wsBaseUrl}${path}`;
}

// FrameKind enum mapping (from frame.proto)
const FrameKind = common.FrameKind;

/**
 * FarmGateWSClient wraps wx.connectSocket for the canonical FarmFrame protocol.
 *
 * Wire format (per WebSocket binary message): one serialized common.FarmFrame.
 *   - protocol_version: 2
 *   - kind: FrameKind enum
 *   - connection_seq: increments from 1
 *   - body: protobuf-encoded inner message (ClientHello / ClientCommandEnvelope / ...)
 */
class FarmGateWSClient {
  constructor() {
    this.socket = null;
    this.state = 'disconnected';
    this.listeners = { open: [], data: [], hello: [], close: [], error: [] };
    this.connectionTicket = null;
    this.sessionId = null;
    this.connectionSeq = 0;
    this._pingTimer = null;
  }

  connect(connectionTicket, sessionId) {
    this.connectionTicket = connectionTicket;
    this.sessionId = sessionId;
    this.connectionSeq = 0;
    const url = buildWsUrl();
    console.log('[FarmGateWS] connecting to', url);

    this.socket = wx.connectSocket({
      url,
      header: { 'content-type': 'application/x-protobuf' },
      success: () => console.log('[FarmGateWS] socket created'),
      fail: (err) => {
        console.error('[FarmGateWS] connect failed', err);
        this.emit('error', err);
      },
    });

    this.socket.onOpen(() => {
      console.log('[FarmGateWS] socket opened');
      // Direct contract: a successful 101 upgrade is the ClientHello send point.
      this.state = 'authenticating';
      this.emit('open');
      this.sendClientHello();
    });

    this.socket.onMessage((res) => this.handleMessage(res.data));

    this.socket.onClose(() => {
      console.log('[FarmGateWS] socket closed');
      this.state = 'disconnected';
      this.stopPing();
      this.emit('close');
    });

    this.socket.onError((err) => {
      console.error('[FarmGateWS] socket error', err);
      this.state = 'disconnected';
      this.stopPing();
      this.emit('error', err);
    });
  }

  handleMessage(data) {
    if (typeof data === 'string') {
      // Canonical FarmGate only sends binary FarmFrame messages.
      console.error('[FarmGateWS] unexpected text frame:', data.slice(0, 100));
      this.emit('error', new Error('FarmGate protocol violation: text frame'));
      return;
    }
    // Binary frame: protobuf FarmFrame
    let bytes;
    if (data instanceof ArrayBuffer) {
      bytes = new Uint8Array(data);
    } else if (ArrayBuffer.isView(data)) {
      bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    } else {
      console.warn('[FarmGateWS] unknown binary data type', typeof data);
      return;
    }
    this.handleFarmFrame(bytes);
  }

  /**
   * Encode a FarmFrame and send as one binary WS message.
   */
  sendFrame(kind, bodyBytes) {
    this.connectionSeq += 1;
    const frame = common.FarmFrame.create({
      protocolVersion: 2,
      kind,
      connectionSeq: this.connectionSeq,
      body: bodyBytes,
    });
    const raw = common.FarmFrame.encode(frame).finish();
    // wx.connectSocket.send 要求 ArrayBuffer
    const buf = raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
    this.socket.send({
      data: buf,
      success: () => console.log('[FarmGateWS] frame sent kind=', kind, 'seq=', this.connectionSeq),
      fail: (err) => {
        console.error('[FarmGateWS] frame send failed', err);
        this.emit('error', err);
      },
    });
  }

  sendClientHello() {
    if (!this.connectionTicket) {
      console.error('[FarmGateWS] no connection ticket');
      return;
    }
    const hello = common.ClientHello.create({
      connectionTicket: this.connectionTicket,
      clientBuild: config.clientBuild || 'farm-wechat/0.1',
    });
    const body = common.ClientHello.encode(hello).finish();
    console.log('[FarmGateWS] sending ClientHello, ticket=', this.connectionTicket.slice(0, 12) + '...');
    this.sendFrame(FrameKind.FRAME_KIND_CLIENT_HELLO, body);
    this.state = 'authenticated';
    this.startPing();
  }

  handleFarmFrame(bytes) {
    let frame;
    try {
      frame = common.FarmFrame.decode(bytes);
    } catch (e) {
      console.error('[FarmGateWS] FarmFrame decode failed:', e.message);
      return;
    }
    const kindName = Object.keys(FrameKind).find((k) => FrameKind[k] === frame.kind) || frame.kind;
    console.log('[FarmGateWS] recv frame kind=', kindName, 'seq=', frame.connectionSeq, 'body.len=', frame.body.length);

    switch (frame.kind) {
      case FrameKind.FRAME_KIND_SERVER_HELLO: {
        const hello = common.ServerHello.decode(frame.body);
        console.log('[FarmGateWS] ServerHello sessionId=', hello.sessionId);
        this.emit('hello', hello);
        break;
      }
      case FrameKind.FRAME_KIND_COMMAND_RESULT: {
        const result = common.CommandResultEnvelope.decode(frame.body);
        const statusName = Object.keys(common.CommandStatus).find((k) => common.CommandStatus[k] === result.status) || result.status;
        console.log('[FarmGateWS] CommandResult op=', result.operationId, 'status=', statusName,
          'errCode=', result.errorCode, 'errMsg=', result.errorMessage,
          'farmVersion=', result.farmVersion && result.farmVersion.toString(),
          'payload.len=', result.payload.length);
        this.emit('data', { kind: 'command_result', result });
        break;
      }
      case FrameKind.FRAME_KIND_SERVER_PUSH: {
        const push = common.ServerPushEnvelope.decode(frame.body);
        const pushTypeName = Object.keys(common.PushType).find((k) => common.PushType[k] === push.pushType) || push.pushType;
        console.log('[FarmGateWS] ServerPush type=', pushTypeName, 'farmVersion=', push.farmVersion && push.farmVersion.toString(), 'payload.len=', push.payload.length);
        this.emit('data', { kind: 'server_push', push });
        break;
      }
      case FrameKind.FRAME_KIND_CONNECTION_ERROR: {
        const cerr = common.ConnectionError.decode(frame.body);
        console.error('[FarmGateWS] ConnectionError:', cerr.errorCode, cerr.errorMessage);
        this.emit('error', new Error(cerr.errorMessage || 'server connection error'));
        break;
      }
      default:
        console.warn('[FarmGateWS] unhandled frame kind', frame.kind);
    }
  }

  /**
   * Send a farm command (after authentication).
   * @param {number} commandType - common.CommandType enum value
   * @param {Uint8Array} payload - protobuf-encoded command body
   * @param {object} opts - { operationId, targetOwnerId, expectedFarmVersion, ... }
   */
  sendCommandEnvelope(commandType, payload, opts = {}) {
    if (this.state !== 'authenticated') {
      console.error('[FarmGateWS] not authenticated');
      return false;
    }
    const uniq = `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
    // Coerce targetOwnerId to a number/Long — proto uint64 rejects strings.
    let ownerId = opts.targetOwnerId;
    if (typeof ownerId === 'string') ownerId = parseInt(ownerId, 10) || 0;
    if (ownerId == null || isNaN(ownerId)) ownerId = 0;
    const env = common.ClientCommandEnvelope.create({
      operationId: opts.operationId || `op-${uniq}`,
      commandType,
      targetOwnerId: ownerId,
      expectedFarmVersion: opts.expectedFarmVersion || 0,
      expectedPlotVersion: opts.expectedPlotVersion || 0,
      expectedCropCycleId: opts.expectedCropCycleId || '',
      requestId: opts.requestId || `req-${uniq}`,
      payload: payload || new Uint8Array(0),
    });
    const body = common.ClientCommandEnvelope.encode(env).finish();
    this.sendFrame(FrameKind.FRAME_KIND_CLIENT_COMMAND, body);
    return true;
  }

  /**
   * Convenience: send a command by enum NAME with empty payload.
   * For commands with parameters, use sendCommandEnvelope directly.
   */
  sendCommand(commandName, params = {}) {
    const cmdType = common.CommandType[commandName];
    if (cmdType === undefined) {
      console.error('[FarmGateWS] unknown command:', commandName);
      return false;
    }
    return this.sendCommandEnvelope(cmdType, new Uint8Array(0), {
      targetOwnerId: params.target_owner_id,
      operationId: params.operation_id,
      requestId: params.request_id,
    });
  }

  startPing() {
    // FarmFrame-level ping is not defined in proto; rely on TCP keepalive.
    // No-op for now.
  }

  stopPing() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  disconnect() {
    this.stopPing();
    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }
    this.state = 'disconnected';
  }

  on(event, callback) {
    if (this.listeners[event]) this.listeners[event].push(callback);
  }

  emit(event, data) {
    (this.listeners[event] || []).forEach((cb) => cb(data));
  }
}

module.exports = { FarmGateWSClient, buildWsUrl, common, farm: $root.farm };
