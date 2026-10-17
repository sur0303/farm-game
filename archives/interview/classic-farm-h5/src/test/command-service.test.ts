import { describe, expect, it, vi } from 'vitest';
import { TestAuthProvider } from './helpers/test-auth-provider';
import { CommandService } from '../domain/command-service';
import {
  BoundedOperationJournal,
  JournalCapacityError,
  JOURNAL_PAYLOAD_SCHEMA_VERSION,
  type OperationJournal,
  type PendingOperationRecord,
} from '../persistence/operation-journal';
import { FarmStore } from '../state/farm-store';
import type { WsClient } from '../network/ws-client';
import { encodeCommandPayload } from '../protocol/final-protocol-adapter';
import { UnlockPlotRequest } from '../protocol/generated/public/commands';
import type { CommandPayload, QueryOperation } from '../protocol/types';

vi.stubGlobal('window', { setTimeout, clearTimeout });

export class MemoryJournal implements OperationJournal {
  readonly records = new Map<string, PendingOperationRecord>();

  async put(record: PendingOperationRecord): Promise<void> {
    this.records.set(record.operationId, record);
  }

  async remove(scope: string, operationId: string): Promise<void> {
    const record = this.records.get(operationId);
    if (record && `${record.environment}:${record.playerId}` === scope) this.records.delete(operationId);
  }

  async list(scope: string): Promise<PendingOperationRecord[]> {
    return [...this.records.values()].filter((record) => `${record.environment}:${record.playerId}` === scope);
  }
}

const makeService = (ws: Partial<WsClient>, journal: OperationJournal, options = {}) => {
  const service = new CommandService(ws as WsClient, new FarmStore(), journal, 'test', {
    resultPollBaseDelayMs: 1,
    resultPollMaxDelayMs: 1,
    random: () => 0.5,
    ...options,
  });
  return service;
};

const plantRecord = (overrides: Partial<PendingOperationRecord> = {}): PendingOperationRecord => ({
  environment: 'test',
  playerId: 'player-a',
  operationId: 'op-recovery',
  commandType: 'PLANT',
  targetFarmId: '10001',
  operationRootPolicy: 'FARM',
  payloadSchemaVersion: JOURNAL_PAYLOAD_SCHEMA_VERSION,
  payloadEncoding: 'protobuf-base64',
  originalTypedPayload: 'CAE=',
  payloadIr: JSON.stringify({ type: 'PLANT', plotId: 1, cropDefinitionId: 1, targetFarmId: '10001', expectedPlotVersion: '1', expectedCropRuleVersion: '0' }),
  canonicalPayloadHash: '00',
  createdAtMs: 1,
  lastStatus: 'AwaitingResult',
  retryCount: 0,
  ...overrides,
});

const encodeBase64 = (bytes: Uint8Array): string => {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

describe('CommandService', () => {
  it('journals a recoverable write before sending and clears it on terminal results', async () => {
    const journal = new MemoryJournal();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({ type: 'SELL', itemId: 2001, count: 1 });
    expect(ws.sendCommand).toHaveBeenCalledWith(expect.objectContaining({
      operationId,
      payload: { type: 'SELL', itemId: 2001, count: 1 },
    }));
    const records = await journal.list('test:player-a');
    expect(records).toHaveLength(1);
    expect(records[0].originalTypedPayload.length).toBeGreaterThan(0);
    expect(records[0].canonicalPayloadHash).toMatch(/^[0-9a-f]{64}$/);
    expect(records[0].operationRootPolicy).toBe('PLAYER');

    await service.handleResult({ operationId, commandType: 'SELL', status: 'PROCESSING', message: '结算处理中' });
    expect(service.getPending()[0]?.status).toBe('Processing');
    expect(await journal.list('test:player-a')).toHaveLength(1);

    await service.handleResult({ operationId, commandType: 'SELL', status: 'REJECTED', message: '没有可出售的胡萝卜' });
    expect(service.getPending()[0]?.status).toBe('Resolved');
    expect(await journal.list('test:player-a')).toHaveLength(0);
  });

  it('does not resurrect a journal record when a terminal result races the send path', async () => {
    const journal = new MemoryJournal();
    let service!: CommandService;
    let terminalResult: Promise<void> | undefined;
    const ws = {
      status: 'InFarm',
      queryOperation: vi.fn(),
      sendCommand: vi.fn((command: { operationId: string }) => {
        // The durable transition must already be complete at the exact send
        // edge; then reproduce a same-turn terminal response.
        expect(journal.records.get(command.operationId)?.lastStatus).toBe('AwaitingResult');
        terminalResult = service.handleResult({
          operationId: command.operationId,
          commandType: 'BUY',
          status: 'SUCCEEDED',
        });
      }),
    } as unknown as WsClient;
    service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    await service.execute({ type: 'BUY', offerId: 1, count: 1 });
    await terminalResult;
    expect(await journal.list('test:player-a')).toHaveLength(0);
  });

  it('waits for the business terminal result instead of treating enqueue as settled', async () => {
    const journal = new MemoryJournal();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({
      type: 'THEFT',
      plotId: 1,
      targetFarmId: '10002',
      cropCycleId: 'theft-cycle-1',
    });
    const waiting = service.waitForTerminal(operationId, 1_000);
    await service.handleResult({ operationId, commandType: 'THEFT', status: 'PROCESSING' });
    await service.handleResult({ operationId, commandType: 'THEFT', status: 'SUCCEEDED' });

    await expect(waiting).resolves.toEqual({
      operationId,
      outcome: 'terminal',
      status: 'SUCCEEDED',
    });
    service.dispose();
  });

  it('turns a timed-out batch operation Unknown and retains its Journal record', async () => {
    const journal = new MemoryJournal();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({
      type: 'THEFT',
      plotId: 1,
      targetFarmId: '10002',
      cropCycleId: 'theft-cycle-timeout',
    });
    const result = await service.waitForTerminal(operationId, 5);

    expect(result).toEqual({ operationId, outcome: 'timeout' });
    expect(service.getPending()).toEqual([
      expect.objectContaining({ operationId, status: 'Unknown' }),
    ]);
    expect(await journal.list('test:player-a')).toHaveLength(1);
    service.dispose();
  });

  it('journals and replays UNLOCK_PLOT with the exact server-fenced payload', async () => {
    const journal = new MemoryJournal();
    const sendCommand = vi.fn();
    const ws = { status: 'InFarm', sendCommand, queryOperation: vi.fn() } as unknown as WsClient;
    const store = new FarmStore();
    store.applyPrivateState({ economyVersion: '1', wallet: 10_000, inventory: {} });
    const service = new CommandService(ws, store, journal, 'test', {
      resultPollBaseDelayMs: 1,
      resultPollMaxDelayMs: 1,
      random: () => 0.5,
    });
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);
    const payload: CommandPayload = {
      type: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      expectedPlotVersion: '11',
      expectedUnlockRuleVersion: '9',
    };

    const operationId = await service.execute(payload);
    const firstSend = sendCommand.mock.calls[0][0] as { operationId: string; payload: CommandPayload };
    expect(firstSend.operationId).toBe(operationId);
    expect(firstSend.payload).toEqual(payload);
    const record = (await journal.list('test:player-a'))[0];
    expect(record).toMatchObject({
      commandType: 'UNLOCK_PLOT',
      operationRootPolicy: 'FARM',
      targetFarmId: '10001',
      plotId: 6,
    });
    const encoded = Uint8Array.from(atob(record.originalTypedPayload), (character) => character.charCodeAt(0));
    expect(UnlockPlotRequest.decode(encoded)).toEqual({
      targetFarmId: 10001n,
      plotId: 6,
      expectedPlotVersion: 11n,
      expectedUnlockRuleVersion: 9n,
    });

    await service.handleResult({ operationId, commandType: 'UNLOCK_PLOT', status: 'PROCESSING' });
    await service.replayOperation(operationId);
    const replay = sendCommand.mock.calls[1][0] as { operationId: string; payload: CommandPayload };
    expect(replay.operationId).toBe(operationId);
    expect(replay.payload).toEqual(firstSend.payload);
    expect(JSON.stringify(replay.payload)).not.toContain('requiredLevel');
    expect(JSON.stringify(replay.payload)).not.toContain('normalCoinPrice');

    await service.handleResult({
      operationId,
      commandType: 'UNLOCK_PLOT',
      status: 'SUCCEEDED',
      paidNormalCoin: '1200',
      normalCoinBalance: '8800',
      committedPlotVersion: '12',
      committedFarmVersion: '42',
    });
    // normal_coin_balance is a reservation-lineage receipt, not a local wallet
    // mutation. Bootstrap/revision is the only private-state refresh authority.
    expect(store.getPrivateState().wallet).toBe(10_000);
    expect(await journal.list('test:player-a')).toHaveLength(0);
    service.dispose();
  });

  it('replays a disconnected UNLOCK_PLOT record with the same operation and bytes', async () => {
    const journal = new MemoryJournal();
    const unlockPayload: CommandPayload = {
      type: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      expectedPlotVersion: '11',
      expectedUnlockRuleVersion: '9',
    };
    await journal.put({
      environment: 'test',
      playerId: 'player-a',
      operationId: 'op-unlock-recovery',
      commandType: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      operationRootPolicy: 'FARM',
      payloadSchemaVersion: JOURNAL_PAYLOAD_SCHEMA_VERSION,
      payloadEncoding: 'protobuf-base64',
      originalTypedPayload: encodeBase64(encodeCommandPayload(unlockPayload)),
      payloadIr: JSON.stringify(unlockPayload),
      canonicalPayloadHash: '00',
      createdAtMs: 1,
      lastStatus: 'Created',
      retryCount: 0,
    });
    const sendCommand = vi.fn();
    const ws = { status: 'Authenticated', sendCommand, queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);
    await service.recoverPending();
    (ws as { status: string }).status = 'InFarm';
    await vi.waitFor(() => expect(sendCommand).toHaveBeenCalledTimes(1));
    const firstReplay = sendCommand.mock.calls[0][0] as { operationId: string; payload: CommandPayload };
    await service.replayOperation('op-unlock-recovery');
    const secondReplay = sendCommand.mock.calls[1][0] as { operationId: string; payload: CommandPayload };
    expect(secondReplay.operationId).toBe(firstReplay.operationId);
    expect(secondReplay.payload).toEqual(firstReplay.payload);
    expect(secondReplay.payload).toEqual(unlockPayload);
    service.dispose();
  });

  it('polls GET_OPERATION_RESULT with the original identity while PROCESSING', async () => {
    const journal = new MemoryJournal();
    const queryOperation = vi.fn();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({
      type: 'HARVEST',
      plotId: 1,
      targetFarmId: '10001',
      cropCycleId: 'cc-1',
      expectedPlotVersion: '2',
    });
    await service.handleResult({
      operationId,
      commandType: 'HARVEST',
      status: 'PROCESSING',
      serverOperationLocator: 'loc-1',
    });
    await vi.waitFor(() => expect(queryOperation).toHaveBeenCalled());
    const query = queryOperation.mock.calls[0][0] as QueryOperation;
    expect(query.originalOperationId).toBe(operationId);
    expect(query.originalCommandType).toBe('HARVEST');
    expect(query.targetFarmId).toBe('10001');
    expect(query.serverOperationLocator).toBe('loc-1');

    await service.handleResult({
      operationId: '',
      commandType: 'GET_OPERATION_RESULT',
      status: 'SUCCEEDED',
      operationLookup: {
        originalOperationId: operationId,
        originalCommandType: 'HARVEST',
        lookupStatus: 'COMPLETED',
        completedStatus: 'SUCCEEDED',
        completedRetryDirective: 'NONE',
      },
    });
    expect(service.getPending()[0]?.status).toBe('Resolved');
    service.dispose();
    expect(await journal.list('test:player-a')).toHaveLength(0);
  });

  it('settles failed Work lookup as UNAVAILABLE and refreshes authority without endless polling', async () => {
    const journal = new MemoryJournal();
    const requestPlayerBootstrap = vi.fn();
    const retryJoinFarm = vi.fn();
    const ws = {
      status: 'InFarm', activeJoinOwnerId: '10002', sendCommand: vi.fn(), queryOperation: vi.fn(),
      requestPlayerBootstrap, retryJoinFarm,
    } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);
    const operationId = await service.execute({
      type: 'THEFT', plotId: 1, targetFarmId: '10002', cropCycleId: 'cycle-failed',
    });
    await service.handleResult({ operationId, commandType: 'THEFT', status: 'PROCESSING' });
    await service.handleResult({
      operationId: 'query-failed', commandType: 'GET_OPERATION_RESULT', status: 'SUCCEEDED',
      operationLookup: {
        originalOperationId: operationId, originalCommandType: 'THEFT', lookupStatus: 'COMPLETED',
        completedStatus: 'UNAVAILABLE', completedErrorCode: 'ERR_INTERNAL',
        completedRetryDirective: 'REFRESH_AUTHORITY_STATE',
      },
    });
    expect(service.getPending()[0]).toMatchObject({ status: 'Resolved', terminalStatus: 'UNAVAILABLE' });
    expect(requestPlayerBootstrap).toHaveBeenCalledWith('player-a');
    expect(retryJoinFarm).toHaveBeenCalledWith('10002');
    expect(await journal.list('test:player-a')).toHaveLength(0);
    service.dispose();
  });

  it('replays CARE with the same operation id on a query miss without a conflicting Fresh Join', async () => {
    const journal = new MemoryJournal();
    const sendCommand = vi.fn();
    const retryJoinFarm = vi.fn();
    const ws = { status: 'InFarm', sendCommand, queryOperation: vi.fn(), retryJoinFarm } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({
      type: 'CARE',
      plotId: 1,
      targetFarmId: '10002',
      cropCycleId: 'care-cycle-1',
      careType: 'WATER',
    });
    await service.handleResult({ operationId, commandType: 'CARE', status: 'PROCESSING' });
    await service.handleResult({
      operationId: '',
      commandType: 'GET_OPERATION_RESULT',
      status: 'SUCCEEDED',
      operationLookup: {
        originalOperationId: operationId,
        originalCommandType: 'CARE',
        lookupStatus: 'NOT_FOUND',
      },
    });

    await vi.waitFor(() => expect(sendCommand).toHaveBeenCalledTimes(2));
    expect(sendCommand.mock.calls[1][0]).toEqual(expect.objectContaining({ operationId }));
    expect(retryJoinFarm).not.toHaveBeenCalled();
    service.dispose();
  });

  it('journals CLAIM_TASK and Ready-fenced visit confirmation under server-owned roots', async () => {
    const journal = new MemoryJournal();
    const sendCommand = vi.fn();
    const ws = { status: 'InFarm', sendCommand, queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    service.setConnectionTicket(await new TestAuthProvider('player-a').acquireConnectionTicket());

    const claimId = await service.execute({ type: 'CLAIM_TASK', taskInstanceId: 'daily:2026-08-08:login' });
    const visitId = await service.execute({ type: 'CONFIRM_FRIEND_VISIT', targetFarmId: '10002' });
    expect(journal.records.get(claimId)).toMatchObject({
      commandType: 'CLAIM_TASK', operationRootPolicy: 'PLAYER', targetFarmId: undefined,
    });
    expect(journal.records.get(visitId)).toMatchObject({
      commandType: 'CONFIRM_FRIEND_VISIT', operationRootPolicy: 'FRIEND_PAIR', targetFarmId: '10002',
    });
    expect(sendCommand).toHaveBeenCalledWith(expect.objectContaining({
      operationId: claimId, payload: { type: 'CLAIM_TASK', taskInstanceId: 'daily:2026-08-08:login' },
    }));
    expect(sendCommand).toHaveBeenCalledWith(expect.objectContaining({
      operationId: visitId, payload: { type: 'CONFIRM_FRIEND_VISIT', targetFarmId: '10002' },
    }));

    await service.handleResult({ operationId: claimId, commandType: 'CLAIM_TASK', status: 'SUCCEEDED' });
    await service.handleResult({ operationId: visitId, commandType: 'CONFIRM_FRIEND_VISIT', status: 'NO_CHANGE' });
    expect(await journal.list('test:player-a')).toEqual([]);
  });

  it('marks feature-disabled commands resolved without retry loops', async () => {
    const journal = new MemoryJournal();
    const queryOperation = vi.fn();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({ type: 'CLAIM_TASK', taskInstanceId: 'task-1' });
    await service.handleResult({
      operationId,
      commandType: 'CLAIM_TASK',
      status: 'REJECTED',
      errorCode: 'ERR_COMMAND_UNIMPLEMENTED',
      errorCodeNumber: 1007,
      featureUnavailable: true,
      message: '命令已登记但尚未开放',
    });
    const operation = service.getPending().find((candidate) => candidate.operationId === operationId);
    expect(operation?.status).toBe('Resolved');
    expect(operation?.featureUnavailable).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(queryOperation).not.toHaveBeenCalled();
    expect(await journal.list('test:player-a')).toHaveLength(0);
  });

  it('stops auto-polling and keeps records when the result directory is feature-disabled', async () => {
    const journal = new MemoryJournal();
    const queryOperation = vi.fn();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({ type: 'SELL', itemId: 2001, count: 1 });
    await service.handleResult({ operationId, commandType: 'SELL', status: 'PROCESSING' });
    await vi.waitFor(() => expect(queryOperation).toHaveBeenCalled());
    await service.handleResult({
      operationId: '',
      commandType: 'GET_OPERATION_RESULT',
      status: 'REJECTED',
      errorCode: 'ERR_COMMAND_UNIMPLEMENTED',
      errorCodeNumber: 1007,
      featureUnavailable: true,
    });
    const callsAfterDisabled = queryOperation.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(queryOperation.mock.calls.length).toBe(callsAfterDisabled);
    expect(service.getPending()[0]?.status).toBe('Unknown');
    expect(await journal.list('test:player-a')).toHaveLength(1);
    service.dispose();
  });

  it('fails closed when the first pending-operation recovery query cannot be queued', async () => {
    const journal = new MemoryJournal();
    await journal.put(plantRecord());
    const ws = {
      status: 'Authenticated',
      sendCommand: vi.fn(),
      queryOperation: vi.fn(() => { throw new Error('writer unavailable'); }),
    } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    await expect(service.recoverPending()).rejects.toThrow('保持恢复屏障并重试');
    expect(service.getPending()).toEqual([
      expect.objectContaining({ operationId: 'op-recovery', status: 'Unknown' }),
    ]);
    expect(await journal.list('test:player-a')).toHaveLength(1);
    service.dispose();
  });

  it('replays only never-sent records with the same operation_id after InFarm opens', async () => {
    const journal = new MemoryJournal();
    await journal.put(plantRecord({ lastStatus: 'Created' }));
    await journal.put(plantRecord({ operationId: 'op-admitted', lastStatus: 'AwaitingResult' }));
    const sendCommand = vi.fn();
    const queryOperation = vi.fn();
    const ws = { status: 'Authenticated', sendCommand, queryOperation, retryJoinFarm: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    await service.recoverPending();
    // 从未上线的记录进入 InFarm 等待队列;已可能 ADMITTED 的记录只查询。
    expect(sendCommand).not.toHaveBeenCalled();
    expect(queryOperation).toHaveBeenCalledWith(expect.objectContaining({ originalOperationId: 'op-admitted' }));

    (ws as { status: string }).status = 'InFarm';
    await vi.waitFor(() => expect(sendCommand).toHaveBeenCalled());
    const replay = sendCommand.mock.calls[0][0] as { operationId: string; payload: { type: string } };
    expect(replay.operationId).toBe('op-recovery');
    expect(replay.payload.type).toBe('PLANT');
    service.dispose();
  });

  it('refuses to replay records with an unknown payload schema version', async () => {
    const journal = new MemoryJournal();
    await journal.put(plantRecord({ payloadSchemaVersion: 'legacy-v0', payloadIr: undefined }));
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    await expect(service.replayOperation('op-recovery')).rejects.toThrow('fail-closed');
    service.dispose();
  });
});

describe('BoundedOperationJournal', () => {
  it('rejects oversized entries before first send and blocks new writes at capacity', async () => {
    const inner = new MemoryJournal();
    const journal = new BoundedOperationJournal(inner, {
      maxPendingEntriesPerPlayer: 1,
      maxEntryBytes: 512,
      maxTotalBytesPerPlayer: 1024,
      maxRetentionMs: 1000,
    });
    const base = plantRecord();
    await journal.put(base);
    // 已达条目上限:新写被 Fail Closed 拒绝;既有记录的状态更新仍允许。
    await expect(journal.put(plantRecord({ operationId: 'op-second' }))).rejects.toThrow(JournalCapacityError);
    await expect(journal.put({ ...base, lastStatus: 'Processing' })).resolves.toBeUndefined();
    const metrics = await journal.metrics('test:player-a');
    expect(metrics.capacityState).toBe('JOURNAL_CAPACITY_BLOCKED');
    expect(metrics.pendingEntries).toBe(1);

    const huge = plantRecord({ operationId: 'op-huge', originalTypedPayload: 'x'.repeat(4096) });
    const loose = new BoundedOperationJournal(new MemoryJournal(), {
      maxPendingEntriesPerPlayer: 8,
      maxEntryBytes: 128,
      maxTotalBytesPerPlayer: 4096,
      maxRetentionMs: 1000,
    });
    await expect(loose.put(huge)).rejects.toThrow(JournalCapacityError);
  });

  it('flags recovery-overdue without deleting pending records', async () => {
    const inner = new MemoryJournal();
    const journal = new BoundedOperationJournal(inner, {
      maxPendingEntriesPerPlayer: 8,
      maxEntryBytes: 4096,
      maxTotalBytesPerPlayer: 65536,
      maxRetentionMs: 10,
    });
    await journal.put(plantRecord({ createdAtMs: Date.now() - 60_000 }));
    const metrics = await journal.metrics('test:player-a');
    expect(metrics.capacityState).toBe('JOURNAL_RECOVERY_OVERDUE');
    expect(await inner.list('test:player-a')).toHaveLength(1);
  });
});

describe('CommandService · cross-subscription reconciliation', () => {
  it('reconciles an admitted write result after the active subscription switches farms', async () => {
    const journal = new MemoryJournal();
    const ws = { status: 'InFarm', sendCommand: vi.fn(), queryOperation: vi.fn() } as unknown as WsClient;
    const service = makeService(ws, journal);
    const ticket = await new TestAuthProvider('player-a').acquireConnectionTicket();
    service.setConnectionTicket(ticket);

    const operationId = await service.execute({
      type: 'PLANT',
      plotId: 1,
      cropDefinitionId: 1,
      targetFarmId: '10001',
      expectedPlotVersion: 0,
      expectedCropRuleVersion: 0,
    });
    await service.handleResult({ operationId, commandType: 'PLANT', status: 'ACCEPTED', targetFarmId: '10001' });
    // 订阅面切到好友农场后,已 admitted 的写结果仍按 operationId 收敛(26 号合同)。
    await service.handleResult({
      operationId,
      commandType: 'PLANT',
      status: 'SUCCEEDED',
      message: 'ok',
      targetFarmId: '10001',
    });

    expect(service.getPending().find((operation) => operation.operationId === operationId)?.status).toBe('Resolved');
    expect(await journal.list('test:player-a')).toHaveLength(0);
  });
});
