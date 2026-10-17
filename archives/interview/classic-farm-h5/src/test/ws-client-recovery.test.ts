import { describe, expect, it, vi } from 'vitest';
import type { AuthProvider, ConnectionTicket } from '../auth/types';
import { TestFarmServer } from './helpers/test-farm-server';
import { WsClient, type WsClientOptions } from '../network/ws-client';
import type { BinaryTransport } from '../network/transport';
import type { FarmProtocolAdapter } from '../protocol/protocol-adapter';
import type { ClientFrame, ServerFrame } from '../protocol/types';
import type { FarmProtocolAdapter as AdapterSeam } from '../protocol/protocol-adapter';
import { FinalProtocolAdapter } from '../protocol/final-protocol-adapter';
import { CommandType as ProtoCommandType } from '../protocol/generated/common/envelope';
import { createTestTransportPair } from './helpers/test-transport';

/** 用生产 adapter 包一层记录/改写,不再是第二线协议。 */
const wrapAdapter = (
  real: FinalProtocolAdapter,
  onEncode?: (frame: ClientFrame) => void,
  onDecode?: (frame: ServerFrame) => ServerFrame | undefined,
): AdapterSeam => ({
  joinBarrier: real.joinBarrier,
  resetConnection: () => real.resetConnection(),
  encodeClientFrame: (frame) => {
    onEncode?.(frame);
    return real.encodeClientFrame(frame);
  },
  decodeServerFrame: (bytes) => {
    const decoded = real.decodeServerFrame(bytes);
    return onDecode?.(decoded) ?? decoded;
  },
});

const browserEvents = new EventTarget();
const networkState = { onLine: true };

vi.stubGlobal('window', {
  setTimeout,
  clearTimeout,
  addEventListener: browserEvents.addEventListener.bind(browserEvents),
  removeEventListener: browserEvents.removeEventListener.bind(browserEvents),
});
vi.stubGlobal('navigator', networkState);

const ticket = (issued: number): ConnectionTicket => ({
  value: `test-ticket-player-a-${issued}`,
  playerId: 'player-a',
  sessionEpoch: `epoch-${issued}`,
  environment: 'test',
});

const fastRecovery: WsClientOptions = {
  ticketTimeoutMs: 20,
  openTimeoutMs: 50,
  helloTimeoutMs: 50,
  joinTimeoutMs: 50,
  stableReadyMs: 1,
  onlineWakeDelayMs: 1,
  reconnectBaseDelayMs: 1,
  reconnectMaxDelayMs: 1,
  random: () => 0,
};

function createSnapshotWaiter(expected: number): {
  seen: () => number;
  wait: Promise<void>;
  onSnapshot: () => void;
} {
  let count = 0;
  let resolve!: () => void;
  const wait = new Promise<void>((done) => {
    resolve = done;
  });
  return {
    seen: () => count,
    wait,
    onSnapshot: () => {
      count += 1;
      if (count === expected) resolve();
    },
  };
}

describe('WsClient recovery coordinator', () => {
  it('opens canonical Join only after matching ACCEPTED, Snapshot and Ready', async () => {
    let server!: BinaryTransport;
    let joinOperationId = '';
    const decoded = new Map<number, () => ServerFrame>();
    const adapter: FarmProtocolAdapter = {
      joinBarrier: 'accepted-snapshot-ready',
      resetConnection: () => undefined,
      encodeClientFrame: (frame) => {
        if (frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM') {
          joinOperationId = frame.value.operationId ?? '';
        }
        return new Uint8Array([frame.kind === 'hello' ? 1 : 2]);
      },
      decodeServerFrame: (bytes) => {
        const factory = decoded.get(bytes[0]);
        if (!factory) throw new Error(`unexpected test frame ${bytes[0]}`);
        return factory();
      },
    };
    const frames: ServerFrame[] = [];
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => {
        const [client, peer] = createTestTransportPair();
        server = peer;
        peer.open?.();
        return client;
      },
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          frames.push(frame);
          if (frame.kind === 'hello') ws.joinFarm('player-a');
        },
      },
      adapter,
      { ...fastRecovery, joinTimeoutMs: 500, manualRecoveryBarrier: true },
    );
    const send = (token: number): void => server.send(new Uint8Array([token]).buffer);
    decoded.set(10, () => ({ kind: 'hello', value: { playerId: 'player-a', sessionEpoch: '1', serverTimeMs: 1 } }));
    decoded.set(11, () => ({
      kind: 'result',
      value: {
        operationId: joinOperationId,
        status: 'ACCEPTED',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
        targetFarmId: 'player-a',
        privateState: { wallet: 77, inventory: { seed: 3 } },
      },
    }));
    decoded.set(12, () => ({
      kind: 'snapshot',
      value: {
        ownerId: 'player-a',
        farmVersion: '5',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
        privateStateIncluded: false,
        state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
      },
    }));
    decoded.set(13, () => ({
      kind: 'delta-batch',
      value: {
        ownerId: 'player-a',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
        fromVersion: '6',
        throughVersion: '7',
        deltas: [6, 7].map((farmVersion) => ({
          ownerId: 'player-a',
          subscriptionId: 'sub-a',
          farmVersion: String(farmVersion),
          source: 'catch-up' as const,
          partial: true,
          state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
        })),
      },
    }));
    decoded.set(14, () => ({
      kind: 'subscription-ready',
      value: {
        ownerId: 'player-a',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
        readyThroughVersion: '7',
      },
    }));

    try {
      await ws.connect();
      send(10);
      await vi.waitFor(() => expect(ws.status).toBe('Joining'));

      send(11);
      await vi.waitFor(() => expect(ws.joinPhase).toBe('WAITING_SNAPSHOT'));
      send(12);
      await vi.waitFor(() => expect(ws.joinPhase).toBe('CATCHING_UP'));
      expect(ws.status).toBe('Recovering');
      expect(frames.filter((frame) => frame.kind === 'snapshot')).toHaveLength(1);
      expect(() => ws.sendCommand({ payload: { type: 'BUY', offerId: 1, count: 1 } }))
        .toThrow('cannot send business command before JoinFarm');
      ws.completeRecovery();
      expect(ws.status).toBe('Recovering');

      send(13);
      await vi.waitFor(() => expect(frames.filter((frame) => frame.kind === 'change')).toHaveLength(2));
      expect(ws.status).toBe('Recovering');
      send(14);
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));
      const snapshots = frames.filter((frame): frame is Extract<ServerFrame, { kind: 'snapshot' }> => frame.kind === 'snapshot');
      expect(snapshots).toHaveLength(1);
      expect(snapshots[0].value.state.wallet).toBe(77);
      expect(snapshots[0].value.privateStateIncluded).toBe(true);
      expect(frames.filter((frame) => frame.kind === 'change').map((frame) => (
        frame.kind === 'change' ? frame.value.farmVersion : ''
      ))).toEqual(['6', '7']);
    } finally {
      ws.dispose();
    }
  });

  it('ignores stale subscription catch-up and holds Ready until the missing version is applied', async () => {
    let server!: BinaryTransport;
    let joinOperationId = '';
    const decoded = new Map<number, () => ServerFrame>();
    const adapter: FarmProtocolAdapter = {
      joinBarrier: 'accepted-snapshot-ready',
      resetConnection: () => undefined,
      encodeClientFrame: (frame) => {
        if (frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM') {
          joinOperationId = frame.value.operationId ?? '';
        }
        return new Uint8Array([frame.kind === 'hello' ? 1 : 2]);
      },
      decodeServerFrame: (bytes) => {
        const factory = decoded.get(bytes[0]);
        if (!factory) throw new Error(`unexpected test frame ${bytes[0]}`);
        return factory();
      },
    };
    const changes: string[] = [];
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => {
        const [client, peer] = createTestTransportPair();
        server = peer;
        peer.open?.();
        return client;
      },
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('player-a');
          if (frame.kind === 'change') changes.push(String(frame.value.farmVersion));
        },
      },
      adapter,
      { ...fastRecovery, joinTimeoutMs: 500, readyApplyTimeoutMs: 500 },
    );
    const change = (subscriptionId: string): ServerFrame => ({
      kind: 'delta-batch',
      value: {
        ownerId: 'player-a',
        subscriptionId,
        joinAttemptId: 'attempt-a',
        fromVersion: '6',
        throughVersion: '6',
        deltas: [{
          ownerId: 'player-a',
          subscriptionId,
          farmVersion: '6',
          source: 'catch-up',
          partial: true,
          state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
        }],
      },
    });
    decoded.set(20, () => ({ kind: 'hello', value: { playerId: 'player-a', sessionEpoch: '1', serverTimeMs: 1 } }));
    decoded.set(21, () => ({
      kind: 'result',
      value: {
        operationId: joinOperationId,
        status: 'ACCEPTED',
        targetFarmId: 'player-a',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
      },
    }));
    decoded.set(22, () => ({
      kind: 'snapshot',
      value: {
        ownerId: 'player-a',
        subscriptionId: 'sub-a',
        joinAttemptId: 'attempt-a',
        farmVersion: '5',
        state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
      },
    }));
    decoded.set(23, () => change('old-sub'));
    decoded.set(24, () => ({
      kind: 'subscription-ready',
      value: {
        ownerId: 'player-a', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a', readyThroughVersion: '6',
      },
    }));
    decoded.set(25, () => change('sub-a'));

    const send = (token: number): void => server.send(new Uint8Array([token]).buffer);
    try {
      await ws.connect();
      send(20);
      await vi.waitFor(() => expect(ws.joinPhase).toBe('PENDING_JOIN'));
      send(21);
      send(22);
      await vi.waitFor(() => expect(ws.joinPhase).toBe('CATCHING_UP'));
      send(23);
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(changes).toEqual([]);
      send(24);
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(ws.joinPhase).toBe('CATCHING_UP');
      expect(ws.status).toBe('Joining');
      send(25);
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));
      expect(changes).toEqual(['6']);
    } finally {
      ws.dispose();
    }
  });

  it('Fresh Joins with a new operation_id when catch-up contains a version hole', async () => {
    let server!: BinaryTransport;
    const joins: string[] = [];
    const decoded = new Map<number, () => ServerFrame>();
    const adapter: FarmProtocolAdapter = {
      joinBarrier: 'accepted-snapshot-ready',
      resetConnection: () => undefined,
      encodeClientFrame: (frame) => {
        if (frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM') {
          joins.push(frame.value.operationId ?? '');
        }
        return new Uint8Array([frame.kind === 'hello' ? 1 : 2]);
      },
      decodeServerFrame: (bytes) => {
        const factory = decoded.get(bytes[0]);
        if (!factory) throw new Error(`unexpected test frame ${bytes[0]}`);
        return factory();
      },
    };
    const errors: Error[] = [];
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => {
        const [client, peer] = createTestTransportPair();
        server = peer;
        peer.open?.();
        return client;
      },
      {
        status: () => undefined,
        error: (error) => errors.push(error),
        frame: (frame) => { if (frame.kind === 'hello') ws.joinFarm('player-a'); },
      },
      adapter,
      { ...fastRecovery, joinTimeoutMs: 500 },
    );
    decoded.set(30, () => ({ kind: 'hello', value: { playerId: 'player-a', sessionEpoch: '1', serverTimeMs: 1 } }));
    decoded.set(31, () => ({
      kind: 'result',
      value: {
        operationId: joins[0], status: 'ACCEPTED', targetFarmId: 'player-a', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a',
      },
    }));
    decoded.set(32, () => ({
      kind: 'snapshot',
      value: {
        ownerId: 'player-a', farmVersion: '5', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a',
        state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
      },
    }));
    decoded.set(33, () => ({
      kind: 'delta-batch',
      value: {
        ownerId: 'player-a', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a', fromVersion: '7', throughVersion: '7',
        deltas: [{
          ownerId: 'player-a', subscriptionId: 'sub-a', farmVersion: '7', source: 'catch-up', partial: true,
          state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
        }],
      },
    }));
    const send = (token: number): void => server.send(new Uint8Array([token]).buffer);

    try {
      await ws.connect();
      send(30);
      await vi.waitFor(() => expect(joins).toHaveLength(1));
      send(31);
      send(32);
      await vi.waitFor(() => expect(ws.joinPhase).toBe('CATCHING_UP'));
      send(33);
      await vi.waitFor(() => expect(joins).toHaveLength(2));
      expect(joins[1]).not.toBe(joins[0]);
      expect(ws.joinPhase).toBe('PENDING_JOIN');
      expect(ws.status).toBe('Joining');
      expect(errors.some((error) => error.message.includes('does not continue the local version'))).toBe(true);
    } finally {
      ws.dispose();
    }
  });

  it('reuses Join operation_id across recovery and rotates it when switching farms', async () => {
    const server = new TestFarmServer();
    const sent: ClientFrame[] = [];
    const adapter = wrapAdapter(new FinalProtocolAdapter(), (frame) => sent.push(frame));
    const snapshots = createSnapshotWaiter(3);
    let calls = 0;
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(++calls) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('10001');
          if (frame.kind === 'snapshot') snapshots.onSnapshot();
        },
      },
      adapter,
      fastRecovery,
    );

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(1));
      server.disconnect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(2));
      ws.joinFarm('10002');
      await snapshots.wait;

      const joins = sent
        .filter((frame): frame is Extract<ClientFrame, { kind: 'command' }> => (
          frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM'
        ))
        .map((frame) => frame.value);
      expect(joins).toHaveLength(3);
      expect(joins[0].operationId).toBeTruthy();
      expect(joins[1].operationId).toBe(joins[0].operationId);
      expect(joins[1].requestId).not.toBe(joins[0].requestId);
      expect(joins[2].operationId).not.toBe(joins[1].operationId);
      expect(joins[2].requestId).not.toBe(joins[1].requestId);
    } finally {
      ws.dispose();
    }
  });

  it('keeps business writes closed until the application completes RecoveryBarrier', async () => {
    const server = new TestFarmServer();
    const sent: ClientFrame[] = [];
    const adapter = wrapAdapter(new FinalProtocolAdapter(), (frame) => sent.push(frame));
    let resolveSnapshot!: () => void;
    const snapshot = new Promise<void>((resolve) => { resolveSnapshot = resolve; });
    let ws!: WsClient;
    ws = new WsClient({ acquireConnectionTicket: async () => ticket(1) }, () => server.createClientTransport(), {
      status: () => undefined,
      error: () => undefined,
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind === 'snapshot') resolveSnapshot();
      },
    }, adapter, { ...fastRecovery, manualRecoveryBarrier: true });

    try {
      await ws.connect();
      await snapshot;
      expect(ws.status).toBe('Recovering');
      expect(() => ws.sendCommand({
        operationId: 'op-before-recovery',
        payload: { type: 'BUY', offerId: 1, count: 1 },
      })).toThrow('cannot send business command before JoinFarm');

      ws.completeRecovery();
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));

      ws.completeRecovery();
      ws.sendCommand({
        operationId: 'op-after-recovery',
        payload: { type: 'BUY', offerId: 1, count: 1 },
      });
      const markers = sent.filter((frame) => frame.kind === 'recovery-complete');
      expect(markers).toHaveLength(1);
      const markerIndex = sent.findIndex((frame) => frame.kind === 'recovery-complete');
      const writeIndex = sent.findIndex((frame) => (
        frame.kind === 'command' && frame.value.operationId === 'op-after-recovery'
      ));
      expect(markerIndex).toBeGreaterThan(-1);
      expect(writeIndex).toBeGreaterThan(markerIndex);
    } finally {
      ws.dispose();
    }
  });

  it('fails closed when the RecoveryComplete control frame cannot be sent', async () => {
    const server = new TestFarmServer();
    const adapter = wrapAdapter(new FinalProtocolAdapter(), (frame) => {
      if (frame.kind === 'recovery-complete') throw new Error('control send failed');
    });
    let resolveSnapshot!: () => void;
    const snapshot = new Promise<void>((resolve) => { resolveSnapshot = resolve; });
    let ws!: WsClient;
    ws = new WsClient({ acquireConnectionTicket: async () => ticket(1) }, () => server.createClientTransport(), {
      status: () => undefined,
      error: () => undefined,
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind === 'snapshot') resolveSnapshot();
      },
    }, adapter, { ...fastRecovery, manualRecoveryBarrier: true });

    try {
      await ws.connect();
      await snapshot;
      expect(ws.status).toBe('Recovering');
      expect(() => ws.completeRecovery()).toThrow('control send failed');
      expect(ws.status).toBe('Recovering');
      expect(() => ws.sendCommand({
        operationId: 'must-stay-closed',
        payload: { type: 'BUY', offerId: 1, count: 1 },
      })).toThrow('cannot send business command before JoinFarm');
    } finally {
      ws.dispose();
    }
  });

  it('Fresh Joins with a new operation_id when joinFarm is called while InFarm (client version-gap path)', async () => {
    const server = new TestFarmServer();
    const sent: ClientFrame[] = [];
    const adapter = wrapAdapter(new FinalProtocolAdapter(), (frame) => sent.push(frame));
    let snapshots = 0;
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('10001');
          if (frame.kind === 'snapshot') snapshots += 1;
        },
      },
      adapter,
      fastRecovery,
    );

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots).toBe(1));
      expect(ws.status).toBe('InFarm');
      ws.joinFarm('10001');
      await vi.waitFor(() => expect(snapshots).toBe(2));

      const joins = sent
        .filter((frame): frame is Extract<ClientFrame, { kind: 'command' }> => (
          frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM'
        ))
        .map((frame) => frame.value);
      expect(joins).toHaveLength(2);
      expect(joins[1].operationId).not.toBe(joins[0].operationId);
      expect(joins[1].requestId).not.toBe(joins[0].requestId);
    } finally {
      ws.dispose();
    }
  });

  it('Fresh Joins with a new operation_id after REJOIN_REQUIRED without replaying writes', async () => {
    const server = new TestFarmServer();
    const sent: ClientFrame[] = [];
    let injected = false;
    const adapter = wrapAdapter(new FinalProtocolAdapter(), (frame) => sent.push(frame), (frame) => {
      if (!injected && frame.kind === 'snapshot') {
        injected = true;
        // 终止当前 Subscription(同一 fence),客户端必须 Fresh Join。
        return {
          kind: 'rejoin-required' as const,
          value: {
            ownerId: frame.value.ownerId,
            subscriptionId: frame.value.subscriptionId ?? '',
            reason: 'HOLDER_REBUILT' as const,
            invalidFromVersion: 3,
          },
        };
      }
      return undefined;
    });
    let snapshots = 0;
    let writes = 0;
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('10001');
          if (frame.kind === 'snapshot') snapshots += 1;
          if (frame.kind === 'result' && frame.value.operationId === 'write-1') writes += 1;
        },
      },
      adapter,
      fastRecovery,
    );

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots).toBe(1));
      const joins = sent
        .filter((frame): frame is Extract<ClientFrame, { kind: 'command' }> => (
          frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM'
        ))
        .map((frame) => frame.value);
      expect(joins).toHaveLength(2);
      expect(joins[1].operationId).not.toBe(joins[0].operationId);
      expect(joins[1].requestId).not.toBe(joins[0].requestId);
      expect(writes).toBe(0);
    } finally {
      ws.dispose();
    }
  });

  it('does not reopen RecoveryBarrier for ordinary in-session farm navigation', async () => {
    const server = new TestFarmServer(() => true);
    let snapshotCount = 0;
    let resolveFriendSnapshot!: () => void;
    const friendSnapshot = new Promise<void>((resolve) => { resolveFriendSnapshot = resolve; });
    let ws!: WsClient;
    ws = new WsClient({ acquireConnectionTicket: async () => ticket(1) }, () => server.createClientTransport(), {
      status: () => undefined,
      error: () => undefined,
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind !== 'snapshot') return;
        snapshotCount += 1;
        if (snapshotCount === 1) {
          expect(ws.status).toBe('Recovering');
          ws.completeRecovery();
          // Ready 与 Snapshot 背靠背到达;InFarm 在 Ready 提升后成立。
          vi.waitFor(() => expect(ws.status).toBe('InFarm')).then(() => ws.joinFarm('10002'));
          return;
        }
        resolveFriendSnapshot();
      },
    }, new FinalProtocolAdapter(), { ...fastRecovery, manualRecoveryBarrier: true });

    try {
      await ws.connect();
      await friendSnapshot;
      expect(snapshotCount).toBe(2);
      expect(ws.status).toBe('InFarm');

      expect(() => ws.sendCommand({
        operationId: 'care-friend-after-navigation',
        payload: { type: 'CARE', plotId: 1, targetFarmId: '10002', cropCycleId: 'friend-crop-1' },
      })).not.toThrow();
    } finally {
      ws.dispose();
    }
  });

  it('arms simulated recovery without depending on an old transport close callback', async () => {
    const server = new TestFarmServer();
    const snapshots = createSnapshotWaiter(2);
    let ticketCalls = 0;
    let transportCalls = 0;
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(++ticketCalls) },
      () => {
        const transport = server.createClientTransport();
        transportCalls += 1;
        if (transportCalls === 1) {
          const close = transport.close.bind(transport);
          transport.close = (code, reason) => {
            // Reproduce a browser/proxy that never reports the old socket's
            // close callback after the diagnostic disconnect request.
            if (reason === 'simulated recoverable disconnect') return;
            close(code, reason);
          };
        }
        return transport;
      },
      {
        status: () => undefined,
        error: () => undefined,
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('10001');
          if (frame.kind === 'snapshot') snapshots.onSnapshot();
        },
      },
      new FinalProtocolAdapter(),
      fastRecovery,
    );

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(1));
      ws.simulateDisconnect();
      await snapshots.wait;
      expect(ticketCalls).toBe(2);
      expect(transportCalls).toBe(2);
      expect(ws.status).toBe('InFarm');
    } finally {
      ws.dispose();
    }
  });

  it('keeps retrying when ticket acquisition fails after an established session drops', async () => {
    const server = new TestFarmServer();
    const snapshots = createSnapshotWaiter(2);
    const errors: Error[] = [];
    let calls = 0;
    const auth: AuthProvider = {
      acquireConnectionTicket: async () => {
        calls += 1;
        if (calls === 2) throw new Error('gate unavailable during rolling restart');
        return ticket(calls);
      },
    };
    let ws!: WsClient;
    ws = new WsClient(auth, () => server.createClientTransport(), {
      status: () => undefined,
      error: (error) => errors.push(error),
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind === 'snapshot') snapshots.onSnapshot();
      },
    }, new FinalProtocolAdapter(), fastRecovery);

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(1));

      ws.simulateDisconnect();
      await snapshots.wait;

      expect(calls).toBe(3);
      expect(errors.map((error) => error.message)).toContain('gate unavailable during rolling restart');
      expect(ws.status).toBe('InFarm');
    } finally {
      ws.dispose();
    }
  });

  it('abandons a stuck ticket request at its deadline and continues recovery', async () => {
    const server = new TestFarmServer();
    const snapshots = createSnapshotWaiter(2);
    let calls = 0;
    let timedOutRequestWasAborted = false;
    const auth: AuthProvider = {
      acquireConnectionTicket: async (signal) => {
        calls += 1;
        if (calls !== 2) return ticket(calls);
        return new Promise<ConnectionTicket>((_resolve, reject) => {
          signal?.addEventListener('abort', () => {
            timedOutRequestWasAborted = true;
            reject(new DOMException('aborted', 'AbortError'));
          }, { once: true });
        });
      },
    };
    let ws!: WsClient;
    ws = new WsClient(auth, () => server.createClientTransport(), {
      status: () => undefined,
      error: () => undefined,
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind === 'snapshot') snapshots.onSnapshot();
      },
    }, new FinalProtocolAdapter(), fastRecovery);

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(1));

      ws.simulateDisconnect();
      await snapshots.wait;

      expect(timedOutRequestWasAborted).toBe(true);
      expect(calls).toBe(3);
      expect(ws.status).toBe('InFarm');
    } finally {
      ws.dispose();
    }
  });

  it('pauses retries while offline and wakes immediately on the online event', async () => {
    const server = new TestFarmServer();
    const snapshots = createSnapshotWaiter(2);
    let calls = 0;
    const auth: AuthProvider = {
      acquireConnectionTicket: async () => {
        calls += 1;
        return ticket(calls);
      },
    };
    let ws!: WsClient;
    ws = new WsClient(auth, () => server.createClientTransport(), {
      status: () => undefined,
      error: () => undefined,
      frame: (frame) => {
        if (frame.kind === 'hello') ws.joinFarm('10001');
        if (frame.kind === 'snapshot') snapshots.onSnapshot();
      },
    }, new FinalProtocolAdapter(), fastRecovery);

    try {
      await ws.connect();
      await vi.waitFor(() => expect(snapshots.seen()).toBe(1));

      networkState.onLine = false;
      ws.simulateDisconnect();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(calls).toBe(1);
      expect(ws.status).toBe('Reconnecting');

      networkState.onLine = true;
      browserEvents.dispatchEvent(new Event('online'));
      await snapshots.wait;

      expect(calls).toBe(2);
      expect(ws.status).toBe('InFarm');
    } finally {
      networkState.onLine = true;
      ws.dispose();
    }
  });

  it('delivers GET_PLAYER_BOOTSTRAP private state as a command result frame', async () => {
    const server = new TestFarmServer();
    const frames: ServerFrame[] = [];
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          frames.push(frame);
          if (frame.kind === 'hello' && !frames.some((f) => f.kind === 'result')) {
            ws.requestPlayerBootstrap('10001');
          }
        },
      },
      new FinalProtocolAdapter(),
      fastRecovery,
    );
    try {
      await ws.connect();
      await vi.waitFor(() => {
        const result = frames.find((frame) => frame.kind === 'result' && frame.value.commandType === 'GET_PLAYER_BOOTSTRAP');
        expect(result).toBeDefined();
        if (result?.kind !== 'result') return;
        expect(result.value.status).toBe('SUCCEEDED');
        expect(result.value.operationId).toBeTruthy();
        expect(result.value.requestId).toBeTruthy();
        expect(result.value.privateState).toBeDefined();
        expect(result.value.privateState?.wallet).toBe(100);
      });
    } finally {
      ws.dispose();
    }
  });

  it('surfaces a feature-disabled GET_PLAYER_BOOTSTRAP with the echoed operation_id', async () => {
    const server = new TestFarmServer(undefined, [ProtoCommandType.GET_PLAYER_BOOTSTRAP]);
    const frames: ServerFrame[] = [];
    let ws!: WsClient;
    let bootstrapOperationId = '';
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          frames.push(frame);
          if (frame.kind === 'hello' && !bootstrapOperationId) {
            bootstrapOperationId = ws.requestPlayerBootstrap('10001');
          }
        },
      },
      new FinalProtocolAdapter(),
      fastRecovery,
    );
    try {
      await ws.connect();
      await vi.waitFor(() => {
        const result = frames.find((frame) => frame.kind === 'result' && frame.value.operationId === bootstrapOperationId);
        expect(result).toBeDefined();
        if (result?.kind !== 'result') return;
        expect(result.value.commandType).toBe('GET_PLAYER_BOOTSTRAP');
        expect(result.value.status).toBe('REJECTED');
        expect(result.value.errorCode).toBe('ERR_COMMAND_UNIMPLEMENTED');
        expect(result.value.featureUnavailable).toBe(true);
        expect(result.value.requestId).toBeTruthy();
      });
    } finally {
      ws.dispose();
    }
  });

  it('reconciles an admitted THEFT result after the subscription switches back home', async () => {
    const server = new TestFarmServer(() => true);
    const frames: ServerFrame[] = [];
    let ws!: WsClient;
    let completedRecovery = false;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          frames.push(frame);
          if (frame.kind === 'hello') ws.joinFarm('10001');
          if (frame.kind === 'subscription-ready' && frame.value.ownerId === '10001' && !completedRecovery) {
            completedRecovery = true;
            ws.completeRecovery();
          }
        },
      },
      new FinalProtocolAdapter(),
      { ...fastRecovery, manualRecoveryBarrier: true, joinTimeoutMs: 5_000 },
    );
    try {
      await ws.connect();
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));

      // 切到好友农场(允许访问),发起 THEFT,然后立刻切回自己农场。
      ws.joinFarm('10002');
      await vi.waitFor(() => expect(frames.some(
        (frame) => frame.kind === 'subscription-ready' && frame.value.ownerId === '10002',
      )).toBe(true));
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));
      ws.sendCommand({
        operationId: 'theft-1',
        requestId: 'req-theft-1',
        payload: { type: 'THEFT', plotId: 2, targetFarmId: '10002', cropCycleId: 'friend-crop-2' },
      });
      ws.joinFarm('10001');

      const statusesOf = () => frames
        .filter((frame) => frame.kind === 'result' && frame.value.operationId === 'theft-1')
        .map((frame) => (frame.kind === 'result' ? frame.value.status : ''));
      await vi.waitFor(() => expect(statusesOf()).toContain('PROCESSING'));
      // 已 admitted 的写命令结果不因 Subscription 切换而丢弃(26/28 号合同)。
      await vi.waitFor(() => expect(statusesOf()).toContain('SUCCEEDED'), { timeout: 3_000 });
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));
    } finally {
      ws.dispose();
    }
  });

  it('keeps the client in Authenticated with a diagnosable result when a friend Join is rejected', async () => {
    const server = new TestFarmServer(() => false);
    const frames: ServerFrame[] = [];
    let ws!: WsClient;
    let joinOperationId = '';
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(1) },
      () => server.createClientTransport(),
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          frames.push(frame);
          if (frame.kind === 'hello' && !joinOperationId) {
            ws.joinFarm('10002');
            joinOperationId = ws.activeJoinOperationId ?? '';
          }
        },
      },
      new FinalProtocolAdapter(),
      { ...fastRecovery, joinTimeoutMs: 5_000 },
    );
    try {
      await ws.connect();
      await vi.waitFor(() => {
        const result = frames.find((frame) => frame.kind === 'result' && frame.value.operationId === joinOperationId);
        expect(result).toBeDefined();
        if (result?.kind !== 'result') return;
        expect(result.value.commandType).toBe('JOIN_FARM');
        expect(result.value.status).toBe('REJECTED');
        expect(result.value.errorCode).toBe('ERR_NOT_FRIEND');
        expect(result.value.requestId).toBeTruthy();
        expect(joinOperationId).toBeTruthy();
      });
      await vi.waitFor(() => expect(ws.status).toBe('Authenticated'));
      expect(ws.joinPhase).toBe('NO_JOIN');
    } finally {
      ws.dispose();
    }
  });

  it('drops live pushes from a stale subscription without forcing a Fresh Join', async () => {
    let server!: BinaryTransport;
    let joinOperationId = '';
    const decoded = new Map<number, () => ServerFrame>();
    const adapter: FarmProtocolAdapter = {
      joinBarrier: 'accepted-snapshot-ready',
      resetConnection: () => undefined,
      encodeClientFrame: (frame) => {
        if (frame.kind === 'command' && frame.value.payload.type === 'JOIN_FARM') {
          joinOperationId = frame.value.operationId ?? '';
        }
        return new Uint8Array([frame.kind === 'hello' ? 1 : 2]);
      },
      decodeServerFrame: (bytes) => {
        const factory = decoded.get(bytes[0]);
        if (!factory) throw new Error(`unexpected test frame ${bytes[0]}`);
        return factory();
      },
    };
    const changes: string[] = [];
    let ws!: WsClient;
    ws = new WsClient(
      { acquireConnectionTicket: async () => ticket(60) },
      () => {
        const [client, peer] = createTestTransportPair();
        server = peer;
        peer.open?.();
        return client;
      },
      {
        status: () => undefined,
        error: (error) => { throw error; },
        frame: (frame) => {
          if (frame.kind === 'hello') ws.joinFarm('player-a');
          if (frame.kind === 'change') changes.push(String(frame.value.farmVersion));
        },
      },
      adapter,
      fastRecovery,
    );
    const send = (token: number): void => server.send(new Uint8Array([token]).buffer);
    const liveChange = (subscriptionId: string, farmVersion: string, wallet: number): ServerFrame => ({
      kind: 'change',
      value: {
        ownerId: 'player-a',
        subscriptionId,
        farmVersion,
        source: 'live',
        partial: true,
        state: { ownerId: 'player-a', wallet, inventory: {}, plots: [] },
      },
    });
    decoded.set(60, () => ({ kind: 'hello', value: { playerId: 'player-a', sessionEpoch: '1', serverTimeMs: 1 } }));
    decoded.set(61, () => ({
      kind: 'result',
      value: { operationId: joinOperationId, status: 'ACCEPTED', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a', targetFarmId: 'player-a' },
    }));
    decoded.set(62, () => ({
      kind: 'snapshot',
      value: {
        ownerId: 'player-a', farmVersion: '5', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a',
        privateStateIncluded: false,
        state: { ownerId: 'player-a', wallet: 0, inventory: {}, plots: [] },
      },
    }));
    decoded.set(63, () => ({
      kind: 'subscription-ready',
      value: { ownerId: 'player-a', subscriptionId: 'sub-a', joinAttemptId: 'attempt-a', readyThroughVersion: '5' },
    }));

    try {
      await ws.connect();
      send(60);
      await vi.waitFor(() => expect(ws.status).toBe('Joining'));
      send(61);
      send(62);
      send(63);
      await vi.waitFor(() => expect(ws.status).toBe('InFarm'));

      // 旧 subscription 的 live push(26/28 号合同):必须丢弃,且不得触发 Fresh Join。
      decoded.set(64, () => liveChange('stale-sub', '6', 999));
      send(64);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(changes).toEqual([]);
      expect(ws.status).toBe('InFarm');

      // 当前 subscription 的 live push 正常收敛。
      decoded.set(65, () => liveChange('sub-a', '6', 101));
      send(65);
      await vi.waitFor(() => expect(changes).toEqual(['6']));
      expect(ws.status).toBe('InFarm');
    } finally {
      ws.dispose();
    }
  });
});
