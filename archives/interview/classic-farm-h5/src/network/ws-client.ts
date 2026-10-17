import type { ClientCommand, ClientFrame, FarmDeltaBatch, FarmSnapshot, PrivateState, QueryOperation, ServerFrame, SubscriptionReady } from '../protocol/types';
import type { FarmProtocolAdapter } from '../protocol/protocol-adapter';
import type { AuthProvider, ConnectionTicket } from '../auth/types';
import type { BinaryCloseEvent, BinaryTransport } from './transport';
import { createUuid } from '../platform/random-id';
import { parseUint64 } from '../protocol/uint64';

export type ConnectionStatus = 'Disconnected' | 'Connecting' | 'Authenticating' | 'Authenticated' | 'Joining' | 'Recovering' | 'InFarm' | 'Reconnecting';
export type JoinPhase = 'NO_JOIN' | 'PENDING_JOIN' | 'WAITING_SNAPSHOT' | 'CATCHING_UP' | 'READY';

export interface RecoveryState {
  phase: ConnectionStatus;
  attempt: number;
  nextRetryMs?: number;
  offline: boolean;
}

export interface WsClientEvents {
  status: (status: ConnectionStatus) => void;
  frame: (frame: ServerFrame) => void;
  error: (error: Error) => void;
  recovery?: (state: RecoveryState) => void;
}

export interface WsClientOptions {
  ticketTimeoutMs?: number;
  openTimeoutMs?: number;
  helloTimeoutMs?: number;
  joinTimeoutMs?: number;
  readyApplyTimeoutMs?: number;
  stableReadyMs?: number;
  onlineWakeDelayMs?: number;
  reconnectBaseDelayMs?: number;
  reconnectMaxDelayMs?: number;
  random?: () => number;
  // When enabled, a Snapshot only opens the RecoveryBarrier. The application
  // must call completeRecovery after private/desired farm state and pending
  // operation queries have all been restored.
  manualRecoveryBarrier?: boolean;
}

type TransportFactory = (ticket: ConnectionTicket) => BinaryTransport;

interface PendingJoin {
  ownerId: string;
  operationId: string;
  acceptedSubscriptionId?: string;
  acceptedJoinAttemptId?: string;
  privateState?: PrivateState;
  appliedVersion?: bigint;
  pendingReady?: SubscriptionReady;
}

interface ActiveSubscription {
  ownerId: string;
  subscriptionId: string;
  joinAttemptId: string;
  appliedVersion: bigint;
}

const defaultOptions: Required<WsClientOptions> = {
  ticketTimeoutMs: 8_000,
  openTimeoutMs: 10_000,
  helloTimeoutMs: 5_000,
  joinTimeoutMs: 10_000,
  readyApplyTimeoutMs: 2_000,
  stableReadyMs: 10_000,
  onlineWakeDelayMs: 250,
  reconnectBaseDelayMs: 500,
  reconnectMaxDelayMs: 10_000,
  random: Math.random,
  manualRecoveryBarrier: false,
};

const fatalCloseCodes = new Set([1002, 1003, 1007, 1008, 1009]);

const asArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const copy = new Uint8Array(bytes.length);
  copy.set(bytes);
  return copy.buffer;
};

const asError = (error: unknown, fallback: string): Error => (
  error instanceof Error ? error : new Error(fallback)
);

const closeError = (event: BinaryCloseEvent): Error => {
  const reason = event.reason ? `: ${event.reason}` : '';
  return new Error(`WebSocket closed (${event.code}${reason})`);
};

export class WsClient {
  private transport: BinaryTransport | null = null;
  private ticket: ConnectionTicket | null = null;
  private statusValue: ConnectionStatus = 'Disconnected';
  private wantsConnection = false;
  private recoveryEnabled = false;
  private reconnecting = false;
  private reconnectTimer: number | undefined;
  private helloTimer: number | undefined;
  private joinTimer: number | undefined;
  private readyApplyTimer: number | undefined;
  private stableReadyTimer: number | undefined;
  private connectPromise: Promise<void> | null = null;
  private reconnectAttempt = 0;
  private serverRetryAfterMs: number | undefined;
  // Connection, Recovery and Join are independent contract dimensions. The
  // legacy ConnectionStatus is retained for UI compatibility; JoinPhase is
  // the source of truth for the public Join barrier.
  private joinPhaseValue: JoinPhase = 'NO_JOIN';
  private recoveryBarrierRequired = false;
  private recoveryBarrierComplete = true;
  private activeSubscription: ActiveSubscription | null = null;
  private attemptGeneration = 0;
  private handledFailureGeneration = -1;
  private attemptAbort: AbortController | null = null;
  // Retained across recoverable reconnects so the same Join intent reuses its
  // operation_id. It is replaced by explicit navigation and cleared on
  // terminal disconnect; it is not an unbounded history.
  private activeJoinIntent: { ownerId: string; operationId: string } | null = null;
  // At most one Join can be pending on a connection. The final protocol requires
  // both acceptance and the exact subscription Snapshot; the buffered value
  // is bounded to one already frame-size-checked Snapshot and shares the Join
  // deadline.
  private pendingJoin: PendingJoin | null = null;
  private readonly options: Required<WsClientOptions>;

  private readonly handleOnline = (): void => {
    this.wakeRecovery();
  };

  private readonly handleVisibilityChange = (): void => {
    if (typeof document !== 'undefined' && !document.hidden) this.wakeRecovery();
  };

  constructor(
    private readonly auth: AuthProvider,
    private readonly createTransport: TransportFactory,
    private readonly events: WsClientEvents,
    private readonly protocolAdapter: FarmProtocolAdapter,
    options: WsClientOptions = {},
  ) {
    this.options = { ...defaultOptions, ...options };
    if (typeof window !== 'undefined') {
      window.addEventListener?.('online', this.handleOnline);
    }
    if (typeof document !== 'undefined') {
      document.addEventListener?.('visibilitychange', this.handleVisibilityChange);
    }
  }

  get status(): ConnectionStatus {
    return this.statusValue;
  }

  get joinPhase(): JoinPhase {
    return this.joinPhaseValue;
  }

  /**
   * 当前活跃 Join 意图的 operation_id(重连轮换 attempt 时沿用同一 operation_id;
   * 只有切换农场/显式 Fresh Join 才轮换)。App 用它把 JOIN_FARM 的 REJECTED
   * 结果归因到具体导航,避免失败后整条应用卡死在未入园状态。
   */
  get activeJoinOperationId(): string | undefined {
    return this.activeJoinIntent?.operationId;
  }

  getConnectionTicket(): ConnectionTicket | null {
    return this.ticket;
  }

  async connect(): Promise<void> {
    this.wantsConnection = true;
    this.clearReconnectTimer();
    return this.startAttempt();
  }

  private startAttempt(): Promise<void> {
    if (this.connectPromise) return this.connectPromise;

    const generation = ++this.attemptGeneration;
    this.handledFailureGeneration = -1;
    const promise = this.connectInternal(generation).catch((error: unknown) => {
      const normalized = asError(error, 'connection attempt failed');
      if (this.isCurrentAttempt(generation) && this.reconnecting) {
        this.handleRecoverableFailure(generation, normalized);
      }
      throw normalized;
    });
    this.connectPromise = promise;
    void promise.finally(() => {
      if (this.connectPromise === promise) this.connectPromise = null;
    }).catch(() => undefined);
    return promise;
  }

  private async connectInternal(generation: number): Promise<void> {
    this.clearReconnectTimer();
    this.clearPhaseTimers();
    this.attemptAbort?.abort();
    const controller = new AbortController();
    this.attemptAbort = controller;
    this.setStatus(this.reconnecting ? 'Reconnecting' : 'Connecting');

    const ticketPromise = this.auth.acquireConnectionTicket(controller.signal);
    const ticket = await this.withDeadline(
      ticketPromise,
      this.options.ticketTimeoutMs,
      'connection ticket',
      () => controller.abort(),
    );
    if (!this.isCurrentAttempt(generation)) throw new Error('connection attempt superseded');

    this.ticket = ticket;
    const transport = this.createTransport(ticket);
    this.transport = transport;
    this.protocolAdapter.resetConnection?.();
    transport.binaryType = 'arraybuffer';

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const openTimer = window.setTimeout(() => {
        const error = new Error(`WebSocket open timed out after ${this.options.openTimeoutMs}ms`);
        failBeforeOpen(error);
        transport.close(4000, 'open timeout');
      }, this.options.openTimeoutMs);

      const clearOpenTimer = (): void => window.clearTimeout(openTimer);
      const failBeforeOpen = (error: Error): void => {
        if (settled) return;
        settled = true;
        clearOpenTimer();
        reject(error);
      };

      transport.onopen = () => {
        if (!this.isCurrentAttempt(generation)) {
          transport.close(4000, 'superseded');
          return;
        }
        if (settled) return;
        settled = true;
        clearOpenTimer();
        this.setStatus('Authenticating');
        try {
          // Direct FarmGate contract: send ClientHello immediately after the
          // WebSocket 101 upgrade. No timing heuristic or admission pass frame.
          this.send({ kind: 'hello', value: { connectionTicket: ticket.value } });
          this.recoveryEnabled = true;
          this.startHelloDeadline(generation);
          resolve();
        } catch (error) {
          reject(asError(error, 'failed to send ClientHello'));
        }
      };
      transport.onmessage = (event) => this.onMessage(generation, event.data);
      transport.onerror = (error) => {
        failBeforeOpen(asError(error, 'WebSocket transport error'));
      };
      transport.onclose = (event) => {
        if (this.transport === transport) this.transport = null;
        if (!settled) failBeforeOpen(closeError(event));
        this.onClose(generation, event);
      };
      transport.open?.();
      if (transport.readyState === 1 && !settled) transport.onopen?.();
    });
  }

  joinFarm(ownerId: string): void {
    if (this.statusValue !== 'Authenticated' && this.statusValue !== 'Recovering' && this.statusValue !== 'InFarm') throw new Error('cannot join before WSS authentication');
    const recoveringSameIntent = this.reconnecting
      && this.activeJoinIntent?.ownerId === ownerId;
    this.beginJoin(ownerId, recoveringSameIntent);
  }

  retryJoinFarm(ownerId: string): void {
    if (this.statusValue !== 'Authenticated' && this.statusValue !== 'Recovering' && this.statusValue !== 'InFarm') throw new Error('cannot join before WSS authentication');
    this.beginJoin(ownerId, this.activeJoinIntent?.ownerId === ownerId);
  }

  get activeJoinOwnerId(): string | undefined {
    return this.activeJoinIntent?.ownerId;
  }

  private beginJoin(ownerId: string, reuseIntent: boolean, recoveryRequiredOverride?: boolean): void {
    if (!reuseIntent) {
      this.activeJoinIntent = { ownerId, operationId: createUuid() };
    }
    this.recoveryBarrierRequired = recoveryRequiredOverride
      ?? (this.options.manualRecoveryBarrier && this.statusValue !== 'InFarm');
    this.recoveryBarrierComplete = !this.recoveryBarrierRequired;
    this.activeSubscription = null;
    this.joinPhaseValue = 'PENDING_JOIN';
    this.setStatus('Joining');
    const operationId = this.activeJoinIntent?.operationId;
    if (!operationId) throw new Error('Join intent is missing operation_id');
    this.pendingJoin = { ownerId, operationId };
    this.send({
      kind: 'command',
      value: {
        operationId,
        requestId: createUuid(),
        payload: { type: 'JOIN_FARM', targetFarmId: ownerId },
      },
    });
    this.startJoinDeadline(this.attemptGeneration);
  }

  sendCommand(command: ClientCommand): void {
    if (this.statusValue !== 'InFarm') throw new Error('cannot send business command before JoinFarm');
    this.send({ kind: 'command', value: command });
  }

  requestPlayerBootstrap(playerId: string): string {
    if (
      this.statusValue !== 'Authenticated'
      && this.statusValue !== 'Joining'
      && this.statusValue !== 'Recovering'
      && this.statusValue !== 'InFarm'
    ) throw new Error('cannot request PlayerBootstrap before WSS authentication');
    const operationId = createUuid();
    this.send({
      kind: 'command',
      value: {
        operationId,
        requestId: createUuid(),
        payload: { type: 'GET_PLAYER_BOOTSTRAP' },
      },
    });
    return operationId;
  }

  queryOperation(query: Omit<QueryOperation, 'requestId'>): void {
    if (
      this.statusValue !== 'Authenticated'
      && this.statusValue !== 'Joining'
      && this.statusValue !== 'Recovering'
      && this.statusValue !== 'InFarm'
    ) return;
    this.send({ kind: 'query', value: { ...query, requestId: createUuid() } });
  }

  completeRecovery(): void {
    if (!this.recoveryBarrierRequired || this.recoveryBarrierComplete) return;
    // The marker shares the WebSocket FIFO with subsequent business commands.
    // Only open the local barrier after send succeeds; a closed transport must
    // leave the client in Recovering so writes fail closed and reconnect begins.
    this.send({ kind: 'recovery-complete', value: {} });
    this.recoveryBarrierComplete = true;
    this.tryOpenFarmInteractive(this.attemptGeneration);
  }

  prepareForDrain(retryAfterMs: number): void {
    if (!Number.isFinite(retryAfterMs) || retryAfterMs < 0) return;
    this.serverRetryAfterMs = Math.min(this.options.reconnectMaxDelayMs, Math.round(retryAfterMs));
  }

  disconnect(): void {
    this.wantsConnection = false;
    this.recoveryEnabled = false;
    this.reconnecting = false;
    this.reconnectAttempt = 0;
    this.handledFailureGeneration = -1;
    this.activeJoinIntent = null;
    this.pendingJoin = null;
    this.activeSubscription = null;
    this.joinPhaseValue = 'NO_JOIN';
    this.recoveryBarrierRequired = false;
    this.recoveryBarrierComplete = true;
    this.attemptGeneration += 1;
    this.attemptAbort?.abort();
    this.attemptAbort = null;
    this.clearReconnectTimer();
    this.clearPhaseTimers();
    const transport = this.transport;
    this.transport = null;
    transport?.close(1000, 'client disconnect');
    this.setStatus('Disconnected');
  }

  dispose(): void {
    this.disconnect();
    if (typeof window !== 'undefined') {
      window.removeEventListener?.('online', this.handleOnline);
    }
    if (typeof document !== 'undefined') {
      document.removeEventListener?.('visibilitychange', this.handleVisibilityChange);
    }
  }

  retryNow(): void {
    this.wantsConnection = true;
    this.recoveryEnabled = true;
    this.reconnecting = true;
    this.clearReconnectTimer();
    this.setStatus('Reconnecting');
    if (!this.connectPromise) this.armReconnect(0);
  }

  simulateDisconnect(): void {
    if (this.transport && this.transport.readyState < 2) {
      // A browser/proxy may surface an intentionally closed socket as 1006 or
      // delay the close callback indefinitely. Arm the normal bounded recovery
      // state machine before closing so this diagnostic control never depends
      // on receiving that callback. The generation fence makes a later close
      // from the old transport harmless.
      const generation = this.attemptGeneration;
      const transport = this.transport;
      this.handleRecoverableFailure(generation, new Error('simulated recoverable disconnect'), false);
      transport.close(4001, 'simulated recoverable disconnect');
      return;
    }
    this.retryNow();
  }

  private send(frame: ClientFrame): void {
    if (!this.transport || this.transport.readyState !== 1) throw new Error('WSS is not open');
    this.transport.send(asArrayBuffer(this.protocolAdapter.encodeClientFrame(frame)));
  }

  private onMessage(generation: number, data: ArrayBuffer): void {
    if (!this.isCurrentAttempt(generation)) return;
    try {
      const frame = this.protocolAdapter.decodeServerFrame(new Uint8Array(data));
      if (frame.kind === 'hello') {
        this.clearHelloTimer();
        // v0.2 ServerHello intentionally carries only session epochs; identity
        // comes from the HTTPS ticket already bound to this connection.
        if (!frame.value.playerId && this.ticket) frame.value.playerId = this.ticket.playerId;
        this.setStatus('Authenticated');
      }
      if (frame.kind === 'result') {
        this.recordJoinAcceptance(frame.value);
        this.events.frame(frame);
        return;
      }
      if (frame.kind === 'snapshot') {
        const snapshot = this.recordJoinSnapshot(frame.value);
        if (!snapshot) return;
        if (this.protocolAdapter.joinBarrier === 'snapshot-only') {
          this.finishSnapshotOnlyJoin(generation, snapshot);
        }
        this.events.frame({ kind: 'snapshot', value: snapshot });
        return;
      }
      if (frame.kind === 'delta-batch') {
        this.applyJoinDeltaBatch(generation, frame.value);
        return;
      }
      if (frame.kind === 'subscription-ready') {
        if (this.recordSubscriptionReady(generation, frame.value)) {
          this.events.frame(frame);
        }
        return;
      }
      if (frame.kind === 'change') {
        if (!this.acceptLiveChange(frame.value)) return;
        this.events.frame(frame);
        return;
      }
      if (frame.kind === 'rejoin-required') {
        this.handleRejoinRequired(frame.value.ownerId, frame.value.subscriptionId);
      }
      // A KICK is an intentional ownership transfer, not a weak-network
      // failure. Stop before emitting it so the old page cannot automatically
      // log in again and steal the session back from the replacement page.
      if (frame.kind === 'kick') {
        this.disconnect();
      }
      // control.connection_error 是连接级协议/会话错误(28 §2 方向守卫唯一
      // 服务端控制帧)。会话类错误不可自动恢复:停止重连,交回登录流程;
      // 其余按可恢复连接失败处理(新 WSS + Cookie 免密换票)。
      if (frame.kind === 'connection-error') {
        const sessionFatal = new Set([2001, 2002, 2003, 2004, 2005]).has(frame.value.errorCodeNumber);
        if (sessionFatal) {
          this.wantsConnection = false;
          this.recoveryEnabled = false;
          this.reconnecting = false;
          this.setStatus('Disconnected');
        } else {
          this.handleRecoverableFailure(generation, new Error(`connection error ${frame.value.errorCode}: ${frame.value.message}`));
          this.transport?.close(4002, 'connection error resync');
        }
      }
      this.events.frame(frame);
    } catch (error) {
      const normalized = asError(error, 'invalid server frame');
      if (normalized.message.startsWith('join barrier violation')) {
        this.events.error(normalized);
        this.freshJoinCurrentOwner();
        return;
      }
      if (normalized.message.startsWith('server connection sequence gap')) {
        this.handleRecoverableFailure(generation, normalized);
        this.transport?.close(4002, 'protocol resync required');
        return;
      }
      this.events.error(normalized);
    }
  }

  private recordJoinAcceptance(result: Extract<ServerFrame, { kind: 'result' }>['value']): void {
    if (this.protocolAdapter.joinBarrier !== 'accepted-snapshot-ready') return;
    const pending = this.pendingJoin;
    if (!pending || result.operationId !== pending.operationId) return;
    if (result.status !== 'ACCEPTED') {
      this.clearJoinTimer();
      this.pendingJoin = null;
      this.joinPhaseValue = 'NO_JOIN';
      this.setStatus('Authenticated');
      return;
    }
    if (!result.subscriptionId || !result.joinAttemptId || !result.targetFarmId) {
      throw new Error('join barrier violation: ACCEPTED did not carry the complete JoinResultContext');
    }
    if (result.targetFarmId !== pending.ownerId) {
      throw new Error('join barrier violation: ACCEPTED target farm does not match the Join intent');
    }
    if (
      (pending.acceptedSubscriptionId && pending.acceptedSubscriptionId !== result.subscriptionId)
      || (pending.acceptedJoinAttemptId && pending.acceptedJoinAttemptId !== result.joinAttemptId)
    ) {
      throw new Error('join barrier violation: ACCEPTED changed its subscription fence');
    }
    pending.acceptedSubscriptionId = result.subscriptionId;
    pending.acceptedJoinAttemptId = result.joinAttemptId;
    pending.privateState = result.privateState;
    this.joinPhaseValue = 'WAITING_SNAPSHOT';
  }

  private recordJoinSnapshot(snapshot: FarmSnapshot): FarmSnapshot | undefined {
    const pending = this.pendingJoin;
    if (!pending || snapshot.ownerId !== pending.ownerId) return undefined;
    if (this.protocolAdapter.joinBarrier === 'snapshot-only') {
      return this.mergeJoinPrivateState(snapshot, pending.privateState);
    }
    if (this.protocolAdapter.joinBarrier !== 'accepted-snapshot-ready') return undefined;
    if (!snapshot.subscriptionId || !snapshot.joinAttemptId) {
      throw new Error('join barrier violation: FARM_SNAPSHOT did not carry its complete fence');
    }
    if (this.joinPhaseValue !== 'WAITING_SNAPSHOT') {
      throw new Error('join barrier violation: FARM_SNAPSHOT arrived before matching ACCEPTED');
    }
    if (
      snapshot.subscriptionId !== pending.acceptedSubscriptionId
      || snapshot.joinAttemptId !== pending.acceptedJoinAttemptId
    ) {
      // A terminal callback from a cancelled attempt is expected during farm
      // switches. It is stale data, not a reason to poison the new Join.
      return undefined;
    }
    pending.appliedVersion = parseUint64(snapshot.farmVersion, 'snapshot.farm_version');
    this.joinPhaseValue = 'CATCHING_UP';
    this.setStatus(this.recoveryBarrierRequired ? 'Recovering' : 'Joining');
    return this.mergeJoinPrivateState(snapshot, pending.privateState);
  }

  private mergeJoinPrivateState(snapshot: FarmSnapshot, privateState?: PrivateState): FarmSnapshot {
    if (!privateState) return snapshot;
    return {
      ...snapshot,
      privateStateIncluded: true,
      state: {
        ...snapshot.state,
        ...privateState,
        inventory: { ...privateState.inventory },
      },
    };
  }

  private finishSnapshotOnlyJoin(generation: number, snapshot: FarmSnapshot): void {
    const pending = this.pendingJoin;
    if (!pending) return;
    const version = parseUint64(snapshot.farmVersion, 'snapshot.farm_version');
    this.activeSubscription = {
      ownerId: snapshot.ownerId,
      subscriptionId: snapshot.subscriptionId ?? `test:${pending.operationId}`,
      joinAttemptId: snapshot.joinAttemptId ?? `test:${pending.operationId}`,
      appliedVersion: version,
    };
    this.pendingJoin = null;
    this.joinPhaseValue = 'READY';
    this.clearJoinTimer();
    this.tryOpenFarmInteractive(generation);
  }

  private applyJoinDeltaBatch(generation: number, batch: FarmDeltaBatch): void {
    const pending = this.pendingJoin;
    if (!pending || this.joinPhaseValue !== 'CATCHING_UP') return;
    if (
      batch.ownerId !== pending.ownerId
      || batch.subscriptionId !== pending.acceptedSubscriptionId
      || batch.joinAttemptId !== pending.acceptedJoinAttemptId
    ) return;
    if (pending.appliedVersion === undefined || batch.deltas.length === 0) {
      throw new Error('join barrier violation: FARM_DELTA_BATCH is empty or has no Snapshot baseline');
    }
    const from = parseUint64(batch.fromVersion, 'delta.from_version');
    const through = parseUint64(batch.throughVersion, 'delta.through_version');
    if (through < from || from !== pending.appliedVersion + 1n) {
      throw new Error('join barrier violation: FARM_DELTA_BATCH does not continue the local version');
    }
    let expected = from;
    for (const delta of batch.deltas) {
      const version = parseUint64(delta.farmVersion, 'delta.farm_version');
      if (
        delta.ownerId !== pending.ownerId
        || delta.subscriptionId !== pending.acceptedSubscriptionId
        || version !== expected
      ) {
        throw new Error('join barrier violation: FARM_DELTA_BATCH contains a gap or wrong fence');
      }
      this.events.frame({ kind: 'change', value: { ...delta, source: 'catch-up' } });
      pending.appliedVersion = version;
      expected += 1n;
    }
    if (pending.appliedVersion !== through) {
      throw new Error('join barrier violation: FARM_DELTA_BATCH through_version is inconsistent');
    }
    this.tryFinishPendingReady(generation);
  }

  private recordSubscriptionReady(generation: number, ready: SubscriptionReady): boolean {
    const pending = this.pendingJoin;
    if (!pending || this.joinPhaseValue !== 'CATCHING_UP') return false;
    if (
      ready.ownerId !== pending.ownerId
      || ready.subscriptionId !== pending.acceptedSubscriptionId
      || ready.joinAttemptId !== pending.acceptedJoinAttemptId
    ) return false;
    if (pending.appliedVersion === undefined) {
      throw new Error('join barrier violation: SUBSCRIPTION_READY arrived without a Snapshot baseline');
    }
    const readyVersion = parseUint64(ready.readyThroughVersion, 'ready.ready_through_version');
    if (pending.appliedVersion > readyVersion) {
      throw new Error('join barrier violation: SUBSCRIPTION_READY is behind the applied version');
    }
    pending.pendingReady = ready;
    if (pending.appliedVersion < readyVersion) {
      this.startReadyApplyDeadline(generation);
      return false;
    }
    this.promoteReadySubscription(generation, pending);
    return true;
  }

  private tryFinishPendingReady(generation: number): void {
    const pending = this.pendingJoin;
    if (!pending || !pending.pendingReady || pending.appliedVersion === undefined) return;
    const readyVersion = parseUint64(pending.pendingReady.readyThroughVersion, 'ready.ready_through_version');
    if (pending.appliedVersion > readyVersion) {
      throw new Error('join barrier violation: catch-up advanced beyond ready_through_version');
    }
    if (pending.appliedVersion === readyVersion) {
      const ready = pending.pendingReady;
      this.promoteReadySubscription(generation, pending);
      this.events.frame({ kind: 'subscription-ready', value: ready });
    }
  }

  private promoteReadySubscription(generation: number, pending: PendingJoin): void {
    if (!pending.acceptedSubscriptionId || !pending.acceptedJoinAttemptId || pending.appliedVersion === undefined) {
      throw new Error('join barrier violation: Ready promotion is missing a fence');
    }
    this.clearJoinTimer();
    this.clearReadyApplyTimer();
    this.activeSubscription = {
      ownerId: pending.ownerId,
      subscriptionId: pending.acceptedSubscriptionId,
      joinAttemptId: pending.acceptedJoinAttemptId,
      appliedVersion: pending.appliedVersion,
    };
    this.pendingJoin = null;
    this.joinPhaseValue = 'READY';
    this.tryOpenFarmInteractive(generation);
  }

  private tryOpenFarmInteractive(generation: number): void {
    if (this.joinPhaseValue !== 'READY') return;
    if (this.recoveryBarrierRequired && !this.recoveryBarrierComplete) {
      this.setStatus('Recovering');
      return;
    }
    this.reconnecting = false;
    this.clearReconnectTimer();
    this.setStatus('InFarm');
    this.startStableReadyTimer(generation);
  }

  private acceptLiveChange(change: Extract<ServerFrame, { kind: 'change' }>['value']): boolean {
    const active = this.activeSubscription;
    if (!active) {
      const pending = this.pendingJoin;
      if (
        pending
        && change.ownerId === pending.ownerId
        && change.subscriptionId === pending.acceptedSubscriptionId
      ) {
        throw new Error('join barrier violation: FARM_CHANGE arrived before SUBSCRIPTION_READY');
      }
      return false;
    }
    if (change.ownerId !== active.ownerId || change.subscriptionId !== active.subscriptionId) return false;
    if (this.joinPhaseValue !== 'READY') {
      throw new Error('join barrier violation: FARM_CHANGE arrived before SUBSCRIPTION_READY');
    }
    const version = parseUint64(change.farmVersion, 'change.farm_version');
    if (version !== active.appliedVersion + 1n) {
      throw new Error('join barrier violation: FARM_CHANGE is not the next continuous version');
    }
    active.appliedVersion = version;
    return true;
  }

  private onClose(generation: number, event: BinaryCloseEvent): void {
    if (!this.isCurrentAttempt(generation) || !this.wantsConnection) return;
    this.clearPhaseTimers();

    const error = closeError(event);
    if (fatalCloseCodes.has(event.code)) {
      this.wantsConnection = false;
      this.recoveryEnabled = false;
      this.reconnecting = false;
      this.setStatus('Disconnected');
      this.events.error(error);
      return;
    }
    if (!this.recoveryEnabled && !this.reconnecting) return;
    this.handleRecoverableFailure(generation, error, event.code !== 1000);
  }

  private handleRejoinRequired(ownerId: string, subscriptionId: string): void {
    const currentOwner = this.activeJoinIntent?.ownerId;
    if (!currentOwner || currentOwner !== ownerId) return;
    const knownSubscription = this.activeSubscription?.subscriptionId
      ?? this.pendingJoin?.acceptedSubscriptionId;
    if (knownSubscription && knownSubscription !== subscriptionId) return;
    this.freshJoinCurrentOwner();
  }

  private freshJoinCurrentOwner(): void {
    const ownerId = this.activeJoinIntent?.ownerId;
    if (!ownerId) return;
    this.clearJoinTimer();
    this.clearReadyApplyTimer();
    this.pendingJoin = null;
    this.activeSubscription = null;
    this.joinPhaseValue = 'NO_JOIN';
    this.reconnecting = false;
    // A stream rebuild does not imply that private bootstrap recovery must be
    // repeated. Snapshot + Ready form the new farm barrier on this connection.
    this.setStatus('Authenticated');
    this.beginJoin(ownerId, false, false);
  }

  private handleRecoverableFailure(generation: number, error: Error, reportError = true): void {
    if (!this.isCurrentAttempt(generation) || !this.wantsConnection) return;
    if (this.handledFailureGeneration === generation) return;
    this.handledFailureGeneration = generation;
    this.pendingJoin = null;
    this.activeSubscription = null;
    this.joinPhaseValue = 'NO_JOIN';
    this.recoveryBarrierRequired = this.options.manualRecoveryBarrier;
    this.recoveryBarrierComplete = !this.recoveryBarrierRequired;
    this.reconnecting = true;
    this.clearPhaseTimers();
    this.setStatus('Reconnecting');
    this.reconnectAttempt += 1;

    if (!this.isOffline()) {
      const delay = this.nextReconnectDelay();
      this.events.recovery?.({ phase: 'Reconnecting', attempt: this.reconnectAttempt, nextRetryMs: delay, offline: false });
      this.armReconnect(delay);
    } else {
      this.events.recovery?.({ phase: 'Reconnecting', attempt: this.reconnectAttempt, offline: true });
    }
    if (reportError) this.events.error(error);
  }

  private nextReconnectDelay(): number {
    if (this.serverRetryAfterMs !== undefined) {
      const delay = this.serverRetryAfterMs;
      this.serverRetryAfterMs = undefined;
      return delay;
    }
    const exponent = Math.min(this.reconnectAttempt - 1, 20);
    const ceiling = Math.min(
      this.options.reconnectMaxDelayMs,
      this.options.reconnectBaseDelayMs * (2 ** exponent),
    );
    return Math.max(0, Math.round(this.options.random() * ceiling));
  }

  private wakeRecovery(): void {
    if (!this.wantsConnection || !this.reconnecting || this.isOffline() || this.connectPromise) return;
    const delay = Math.round(this.options.random() * this.options.onlineWakeDelayMs);
    this.armReconnect(delay);
  }

  private armReconnect(delayMs: number): void {
    this.clearReconnectTimer();
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.wantsConnection || this.isOffline()) return;
      void this.startAttempt().catch(() => undefined);
    }, Math.max(0, delayMs));
  }

  private startHelloDeadline(generation: number): void {
    this.clearHelloTimer();
    this.helloTimer = window.setTimeout(() => {
      if (!this.isCurrentAttempt(generation) || this.statusValue !== 'Authenticating') return;
      const error = new Error(`ServerHello timed out after ${this.options.helloTimeoutMs}ms`);
      this.handleRecoverableFailure(generation, error);
      this.transport?.close(4000, 'hello timeout');
    }, this.options.helloTimeoutMs);
  }

  private startJoinDeadline(generation: number): void {
    this.clearJoinTimer();
    this.joinTimer = window.setTimeout(() => {
      if (
        !this.isCurrentAttempt(generation)
        || this.joinPhaseValue === 'NO_JOIN'
        || this.joinPhaseValue === 'READY'
      ) return;
      const error = new Error(`Join barrier timed out after ${this.options.joinTimeoutMs}ms`);
      this.pendingJoin = null;
      this.handleRecoverableFailure(generation, error);
      this.transport?.close(4000, 'join timeout');
    }, this.options.joinTimeoutMs);
  }

  private startReadyApplyDeadline(generation: number): void {
    this.clearReadyApplyTimer();
    this.readyApplyTimer = window.setTimeout(() => {
      if (!this.isCurrentAttempt(generation) || this.joinPhaseValue !== 'CATCHING_UP') return;
      this.events.error(new Error(`SUBSCRIPTION_READY apply timed out after ${this.options.readyApplyTimeoutMs}ms`));
      this.freshJoinCurrentOwner();
    }, this.options.readyApplyTimeoutMs);
  }

  private startStableReadyTimer(generation: number): void {
    this.clearStableReadyTimer();
    this.stableReadyTimer = window.setTimeout(() => {
      if (!this.isCurrentAttempt(generation) || this.statusValue !== 'InFarm') return;
      this.reconnectAttempt = 0;
      this.stableReadyTimer = undefined;
    }, this.options.stableReadyMs);
  }

  private async withDeadline<T>(
    promise: Promise<T>,
    timeoutMs: number,
    label: string,
    onTimeout: () => void,
  ): Promise<T> {
    let timer: number | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = window.setTimeout(() => {
        reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        onTimeout();
      }, timeoutMs);
    });
    try {
      return await Promise.race([promise, timeout]);
    } finally {
      if (timer !== undefined) window.clearTimeout(timer);
    }
  }

  private isCurrentAttempt(generation: number): boolean {
    return this.attemptGeneration === generation;
  }

  private isOffline(): boolean {
    return typeof navigator !== 'undefined' && navigator.onLine === false;
  }

  private setStatus(status: ConnectionStatus): void {
    if (this.statusValue === status) return;
    this.statusValue = status;
    this.events.status(status);
    this.events.recovery?.({
      phase: status,
      attempt: this.reconnectAttempt,
      offline: this.isOffline(),
    });
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== undefined) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
  }

  private clearHelloTimer(): void {
    if (this.helloTimer !== undefined) window.clearTimeout(this.helloTimer);
    this.helloTimer = undefined;
  }

  private clearJoinTimer(): void {
    if (this.joinTimer !== undefined) window.clearTimeout(this.joinTimer);
    this.joinTimer = undefined;
  }

  private clearReadyApplyTimer(): void {
    if (this.readyApplyTimer !== undefined) window.clearTimeout(this.readyApplyTimer);
    this.readyApplyTimer = undefined;
  }

  private clearStableReadyTimer(): void {
    if (this.stableReadyTimer !== undefined) window.clearTimeout(this.stableReadyTimer);
    this.stableReadyTimer = undefined;
  }

  private clearPhaseTimers(): void {
    this.clearHelloTimer();
    this.clearJoinTimer();
    this.clearReadyApplyTimer();
    this.clearStableReadyTimer();
  }
}
