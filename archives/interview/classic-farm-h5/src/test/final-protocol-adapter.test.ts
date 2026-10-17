import { describe, expect, it } from 'vitest';
import { FinalProtocolAdapter } from '../protocol/final-protocol-adapter';
import { ClientHello } from '../protocol/generated/common/control';
import {
  ClientCommandEnvelope,
  CommandResultEnvelope,
  CommandType as ProtoCommandType,
  PushType,
  RetryDirective as ProtoRetryDirective,
  ServerPushEnvelope,
} from '../protocol/generated/common/envelope';
import { FarmFrame } from '../protocol/generated/common/frame';
import { CommandStatus, ErrorCode } from '../protocol/generated/common/enums';
import { CropPhase, PlotLockState } from '../protocol/generated/farm/farm';
import { FriendOpportunityChanged } from '../protocol/generated/friend/friend';
import {
  FarmChangePush,
  FarmSnapshotPush,
  SubscriptionReadyPush,
  type PlotView,
} from '../protocol/generated/farm/farm_view';
import {
  BuyRequest,
  BuyResult,
  CareOutcome,
  ClaimTaskRequest,
  ClaimTaskResult,
  ConfirmFriendVisitRequest,
  ConfirmFriendVisitResult,
  CareRequest,
  CareResult as ProtoCareResult,
  CareType,
  GetOperationResultRequest,
  GetOperationResultResult,
  GetPlayerBootstrapResult,
  GetTaskListResult,
  JoinFarmRequest,
  OperationLookupStatus,
  PlayerTaskProgressState,
  SellRequest,
  UnlockPlotRequest,
  UnlockPlotResult,
} from '../protocol/generated/public/commands';
import { PrivateState } from '../protocol/generated/player/player';
import type { CommandPayload } from '../protocol/types';

const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');

const fromHex = (hex: string): Uint8Array => {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i += 1) bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return bytes;
};

const serverFrame = (_adapter: FinalProtocolAdapter, sequence: bigint, body: Partial<FarmFrame>): Uint8Array =>
  FarmFrame.encode({ protocolVersion: 4, kind: 0, connectionSeq: 0, sequence, body: new Uint8Array(), ...body } as FarmFrame).finish();

const plotView = (overrides: Partial<PlotView>): PlotView => ({
  plotIndex: 0,
  plotVersion: 1n,
  cropCycleId: '',
  cropDefinitionId: 0,
  cropPhase: CropPhase.CROP_PHASE_UNSPECIFIED,
  plantAtMs: 0n,
  matureAtMs: 0n,
  harvestableCount: 0,
  stolenTotal: 0,
  watered: false,
  lockState: PlotLockState.PLOT_LOCK_STATE_UNSPECIFIED,
  unlockOrder: 0,
  requiredLevel: 0,
  normalCoinPrice: 0n,
  unlockCandidate: false,
  unlockRuleVersion: 1n,
  ...overrides,
});

// 与 farm-game/internal/protocoltest/golden_test.go 逐字节共享的帧黄金。
describe('final protocol adapter · frame goldens', () => {
  it('encodes ClientHello byte-identical to the Go golden', () => {
    const adapter = new FinalProtocolAdapter();
    const bytes = adapter.encodeClientFrame({ kind: 'hello', value: { connectionTicket: 't' } });
    expect(toHex(bytes)).toBe('0804400152100a01741a0b6661726d2d68352f302e32');
  });

  it('round-trips the Go ClientCommand golden semantically', () => {
    // 两端编码器字段发射顺序不同(同为合法 protobuf):Go 按字段号、
    // ts-proto 按声明顺序;做字段级一致 + 往返一致,不做逐字节相等。
    const golden = FarmFrame.decode(fromHex('08044002621112036f70312002520272317a0408011002'));
    expect(golden.protocolVersion).toBe(4);
    expect(golden.sequence).toBe(2n);
    expect(golden.clientCommand?.operationId).toBe('op1');
    expect(golden.clientCommand?.requestId).toBe('r1');
    expect(golden.clientCommand?.commandType).toBe(ProtoCommandType.BUY);
    expect(toHex(golden.clientCommand?.payload ?? new Uint8Array())).toBe('08011002');

    const adapter = new FinalProtocolAdapter();
    adapter.encodeClientFrame({ kind: 'hello', value: { connectionTicket: 't' } });
    const bytes = adapter.encodeClientFrame({
      kind: 'command',
      value: {
        operationId: 'op1',
        requestId: 'r1',
        payload: { type: 'BUY', offerId: 1, count: 2 },
      },
    });
    const own = FarmFrame.decode(bytes);
    expect(own.clientCommand?.operationId).toBe(golden.clientCommand?.operationId);
    expect(own.clientCommand?.requestId).toBe(golden.clientCommand?.requestId);
    expect(own.clientCommand?.commandType).toBe(golden.clientCommand?.commandType);
    expect(toHex(own.clientCommand?.payload ?? new Uint8Array())).toBe(toHex(golden.clientCommand?.payload ?? new Uint8Array()));
    expect(Reflect.has(own.clientCommand ?? {}, 'targetOwnerId')).toBe(false);
    const roundTrip = ClientCommandEnvelope.decode(ClientCommandEnvelope.encode(own.clientCommand!).finish());
    expect(roundTrip.payload).toEqual(golden.clientCommand?.payload);
  });

  it('decodes the Go CommandResult/ServerPush separation goldens', () => {
    // 黄金字节层面:CommandResult=oneof field 13,ServerPush=field 14,逐字节可区分。
    const resultFrame = FarmFrame.decode(fromHex('080440036a0610013a027231'));
    expect(resultFrame.commandResult).toBeDefined();
    expect(resultFrame.serverPush).toBeUndefined();
    expect(resultFrame.commandResult?.status).toBe(CommandStatus.SUCCEEDED);
    expect(resultFrame.commandResult?.requestId).toBe('r1');
    const pushFrame = FarmFrame.decode(fromHex('0804400472023015'));
    expect(pushFrame.serverPush).toBeDefined();
    expect(pushFrame.commandResult).toBeUndefined();
    expect(pushFrame.serverPush?.pushType).toBe(PushType.SUBSCRIPTION_READY);

    // 同一内容经适配器(重排 sequence)按语义解码。
    const adapter = new FinalProtocolAdapter();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, { commandResult: resultFrame.commandResult }));
    expect(result.kind).toBe('result');
    if (result.kind !== 'result') throw new Error('unreachable');
    expect(result.value.status).toBe('SUCCEEDED');
    expect(result.value.requestId).toBe('r1');
  });

  it('decodes the shared W11A envelope goldens', () => {
    const adapter = new FinalProtocolAdapter();
    // Join ACCEPTED(JoinResultContext)——frame 包装:field 13 = command_result。
    const resultBytes = fromHex('0a046f702d3110063a057265712d31b80106ca011208914e12077375622d6162631a046a612d31');
    const frame = serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.decode(resultBytes),
    });
    const decoded = adapter.decodeServerFrame(frame);
    if (decoded.kind !== 'result') throw new Error('expected command result');
    expect(decoded.value.status).toBe('ACCEPTED');
    expect(decoded.value.commandType).toBe('JOIN_FARM');
    expect(decoded.value.subscriptionId).toBe('sub-abc');
    expect(decoded.value.joinAttemptId).toBe('ja-1');
    expect(decoded.value.targetFarmId).toBe('10001');

    const errorBytes = fromHex('0a046f702d3210043a057265712d32a20112088927120d706c6f74206f63637570696564b80104');
    const errorFrame = serverFrame(adapter, 2n, {
      commandResult: CommandResultEnvelope.decode(errorBytes),
    });
    const errorResult = adapter.decodeServerFrame(errorFrame);
    if (errorResult.kind !== 'result') throw new Error('expected error result');
    expect(errorResult.value.status).toBe('REJECTED');
    expect(errorResult.value.errorCode).toBe('ERR_PLOT_NOT_EMPTY');
    expect(errorResult.value.message).toBe('plot occupied');
  });
});

describe('final protocol adapter · encode discipline', () => {
  it('omits retired frame/envelope fields', () => {
    const adapter = new FinalProtocolAdapter();
    adapter.encodeClientFrame({ kind: 'hello', value: { connectionTicket: 't' } });
    const commandBytes = adapter.encodeClientFrame({
      kind: 'command',
      value: {
        operationId: 'op-plant',
        requestId: 'req-plant',
        payload: {
          type: 'PLANT',
          plotId: 3,
          cropDefinitionId: 1,
          targetFarmId: '10001',
          expectedPlotVersion: '9',
          expectedCropRuleVersion: '0',
        },
      },
    });
    const frame = FarmFrame.decode(commandBytes);
    expect(Reflect.has(frame, 'kind')).toBe(false);
    expect(Reflect.has(frame, 'connectionSeq')).toBe(false);
    expect(Reflect.has(frame, 'body')).toBe(false);
    expect(frame.sequence).toBe(2n);
    const envelope = frame.clientCommand!;
    expect(Reflect.has(envelope, 'targetOwnerId')).toBe(false);
    expect(Reflect.has(envelope, 'expectedFarmVersion')).toBe(false);
    expect(Reflect.has(envelope, 'expectedPlotVersion')).toBe(false);
    expect(Reflect.has(envelope, 'expectedCropCycleId')).toBe(false);
    expect(envelope.commandType).toBe(ProtoCommandType.PLANT);
    expect(envelope.requestId).toBe('req-plant');
    expect(envelope.operationId).toBe('op-plant');
  });

  it('encodes JoinFarm with target_farm_id inside the typed payload', () => {
    const adapter = new FinalProtocolAdapter();
    const bytes = adapter.encodeClientFrame({
      kind: 'command',
      value: { operationId: 'op-join', requestId: 'req-join', payload: { type: 'JOIN_FARM', targetFarmId: '10001' } },
    });
    const frame = FarmFrame.decode(bytes);
    expect(frame.clientCommand?.commandType).toBe(ProtoCommandType.JOIN_FARM);
    const payload = JoinFarmRequest.decode(frame.clientCommand!.payload);
    expect(payload.targetFarmId).toBe(10001n);
  });

  it('encodes GET_OPERATION_RESULT with original identity, target and locator', () => {
    const adapter = new FinalProtocolAdapter();
    const bytes = adapter.encodeClientFrame({
      kind: 'query',
      value: {
        requestId: 'req-q1',
        originalCommandType: 'THEFT',
        originalOperationId: 'op-theft',
        targetFarmId: '10002',
        serverOperationLocator: 'loc-1',
      },
    });
    const frame = FarmFrame.decode(bytes);
    expect(frame.clientCommand?.commandType).toBe(ProtoCommandType.GET_OPERATION_RESULT);
    expect(frame.clientCommand?.operationId).toBe('');
    const request = GetOperationResultRequest.decode(frame.clientCommand!.payload);
    expect(request.originalCommandType).toBe(ProtoCommandType.THEFT);
    expect(request.originalOperationId).toBe('op-theft');
    expect(request.originalTarget?.targetFarmId).toBe(10002n);
    expect(request.serverOperationLocator).toBe('loc-1');
  });

  it('encodes the recovery barrier as a client control frame', () => {
    const adapter = new FinalProtocolAdapter();
    const bytes = adapter.encodeClientFrame({ kind: 'recovery-complete', value: {} });
    const frame = FarmFrame.decode(bytes);
    expect(frame.control?.clientRecoveryComplete).toBeDefined();
  });
});

describe('final protocol adapter · decode fences', () => {
  const snapshotBody = (adapter: FinalProtocolAdapter, sequence: bigint, overrides: { ownerId?: bigint; subscriptionId?: string; joinAttemptId?: string; snapshotVersion?: bigint } = {}) => serverFrame(adapter, sequence, {
    serverPush: ServerPushEnvelope.create({
      pushType: PushType.FARM_SNAPSHOT,
      farmSubscription: {
        targetPlayerId: 10001n,
        targetFarmId: overrides.ownerId ?? 10001n,
        subscriptionId: overrides.subscriptionId ?? 'sub-1',
      },
      payload: FarmSnapshotPush.encode({
        joinAttemptId: overrides.joinAttemptId ?? 'ja-1',
        snapshotVersion: overrides.snapshotVersion ?? 5n,
        snapshot: { ownerId: overrides.ownerId ?? 10001n, snapshotTimeMs: 1n, farmVersion: overrides.snapshotVersion ?? 5n, plots: [] },
      }).finish(),
    }),
  });

  it('enforces strict per-direction sequence continuity', () => {
    const adapter = new FinalProtocolAdapter();
    adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({ operationId: 'op-1', status: CommandStatus.SUCCEEDED }),
    }));
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 3n, {
      commandResult: CommandResultEnvelope.create({ operationId: 'op-2', status: CommandStatus.SUCCEEDED }),
    }))).toThrow('server connection sequence gap');
  });

  it('fails closed on legacy kind/body frames and wrong protocol versions', () => {
    const adapter = new FinalProtocolAdapter();
    expect(() => adapter.decodeServerFrame(fromHex('0801180328013a02080e'))).toThrow();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({ status: CommandStatus.SUCCEEDED }),
    }).map((byte, index) => (index === 1 ? 3 : byte)))).toThrow('unsupported FarmFrame protocol version 3');
  });

  it('rejects server frames in the client direction', () => {
    expect(() => new FinalProtocolAdapter().decodeServerFrame(serverFrame(new FinalProtocolAdapter(), 1n, {
      clientHello: ClientHello.create({ connectionTicket: 'x' }),
    }))).toThrow('direction violation');
    expect(() => new FinalProtocolAdapter().decodeServerFrame(serverFrame(new FinalProtocolAdapter(), 1n, {
      clientCommand: ClientCommandEnvelope.create({ commandType: ProtoCommandType.BUY }),
    }))).toThrow('direction violation');
  });

  it('decodes FARM_SNAPSHOT/SUBSCRIPTION_READY with fences', () => {
    const adapter = new FinalProtocolAdapter();
    const snapshot = adapter.decodeServerFrame(snapshotBody(adapter, 1n));
    if (snapshot.kind !== 'snapshot') throw new Error('expected snapshot');
    expect(snapshot.value.ownerId).toBe('10001');
    expect(snapshot.value.farmVersion).toBe('5');
    expect(snapshot.value.subscriptionId).toBe('sub-1');
    expect(snapshot.value.joinAttemptId).toBe('ja-1');

    const ready = adapter.decodeServerFrame(serverFrame(adapter, 2n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.SUBSCRIPTION_READY,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: SubscriptionReadyPush.encode({ joinAttemptId: 'ja-1', readyThroughVersion: 5n }).finish(),
      }),
    }));
    if (ready.kind !== 'subscription-ready') throw new Error('expected ready');
    expect(ready.value.readyThroughVersion).toBe('5');
  });

  it('decodes a valid owner_public_profile and preserves its authoritative fields', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_SNAPSHOT,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmSnapshotPush.encode({
          joinAttemptId: 'ja-1',
          snapshotVersion: 5n,
          snapshot: {
            ownerId: 10001n,
            snapshotTimeMs: 1n,
            farmVersion: 5n,
            plots: [{
              plotIndex: 0,
              plotVersion: 3n,
              cropCycleId: 'cycle-1',
              cropDefinitionId: 1,
              cropPhase: CropPhase.GROWING,
              plantAtMs: 1n,
              matureAtMs: 10n,
              harvestableCount: 8,
              stolenTotal: 2,
              watered: false,
              lockState: 1,
              unlockOrder: 1,
              requiredLevel: 1,
              normalCoinPrice: 0n,
              unlockCandidate: false,
              unlockRuleVersion: 1n,
            }],
            ownerPublicProfile: { playerId: 10001n, displayName: 'Alice', avatarId: 7, level: 12 },
          },
        }).finish(),
      }),
    }));
    if (decoded.kind !== 'snapshot') throw new Error('expected snapshot');
    expect(decoded.value.ownerPublicProfile).toEqual({
      playerId: '10001', displayName: 'Alice', avatarId: 7, level: 12,
    });
    expect(decoded.value.state.plots[0]).toMatchObject({
      harvestableCount: 8,
      stolenTotal: 2,
    });
  });

  it('preserves LOCKED, UNLOCKED, and UNSPECIFIED PlotView lock/rule fields', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_SNAPSHOT,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmSnapshotPush.encode({
          joinAttemptId: 'ja-1',
          snapshotVersion: 5n,
          snapshot: {
            ownerId: 10001n,
            snapshotTimeMs: 1n,
            farmVersion: 5n,
            plots: [
              plotView({
                plotIndex: 0,
                plotVersion: 10n,
                cropPhase: CropPhase.CROP_PHASE_UNSPECIFIED,
                lockState: PlotLockState.PLOT_LOCK_STATE_UNLOCKED,
                unlockOrder: 0,
                requiredLevel: 0,
                normalCoinPrice: 0n,
                unlockCandidate: false,
                unlockRuleVersion: 3n,
              }),
              plotView({
                plotIndex: 6,
                plotVersion: 11n,
                cropPhase: CropPhase.CROP_PHASE_UNSPECIFIED,
                lockState: PlotLockState.PLOT_LOCK_STATE_LOCKED,
                unlockOrder: 7,
                requiredLevel: 4,
                normalCoinPrice: 1200n,
                unlockCandidate: true,
                unlockRuleVersion: 9n,
              }),
              plotView({
                plotIndex: 7,
                plotVersion: 12n,
                cropPhase: CropPhase.CROP_PHASE_UNSPECIFIED,
                lockState: PlotLockState.PLOT_LOCK_STATE_UNSPECIFIED,
                unlockOrder: 8,
                requiredLevel: 5,
                normalCoinPrice: 2400n,
                unlockCandidate: false,
                unlockRuleVersion: 9n,
              }),
            ],
          },
        }).finish(),
      }),
    }));
    if (decoded.kind !== 'snapshot') throw new Error('expected snapshot');
    expect(decoded.value.state.plots).toMatchObject([
      {
        plotIndex: 0, plotId: 'plot-0', lockState: 'UNLOCKED', unlockOrder: 0,
        requiredLevel: 0, normalCoinPrice: '0', unlockCandidate: false, unlockRuleVersion: '3',
      },
      {
        plotIndex: 6, plotId: 'plot-6', lockState: 'LOCKED', unlockOrder: 7,
        requiredLevel: 4, normalCoinPrice: '1200', unlockCandidate: true, unlockRuleVersion: '9',
      },
      {
        plotIndex: 7, plotId: 'plot-7', lockState: 'UNSPECIFIED', unlockOrder: 8,
        requiredLevel: 5, normalCoinPrice: '2400', unlockCandidate: false, unlockRuleVersion: '9',
      },
    ]);
  });

  it('preserves lock/rule fields on a FARM_CHANGE delta', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_CHANGE,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmChangePush.encode({
          change: {
            ownerId: 10001n,
            farmVersion: 6n,
            changedPlots: [plotView({
              plotIndex: 6,
              plotVersion: 12n,
              cropPhase: CropPhase.CROP_PHASE_UNSPECIFIED,
              lockState: PlotLockState.PLOT_LOCK_STATE_UNLOCKED,
              unlockOrder: 7,
              requiredLevel: 4,
              normalCoinPrice: 1200n,
              unlockCandidate: false,
              unlockRuleVersion: 9n,
            })],
          },
        }).finish(),
      }),
    }));
    if (decoded.kind !== 'change') throw new Error('expected change');
    expect(decoded.value.state.plots[0]).toMatchObject({
      plotIndex: 6,
      plotId: 'plot-6',
      lockState: 'UNLOCKED',
      unlockOrder: 7,
      requiredLevel: 4,
      normalCoinPrice: '1200',
      unlockCandidate: false,
      unlockRuleVersion: '9',
    });
  });

  it('fails closed when owner_public_profile player_id mismatches owner_id', () => {
    const adapter = new FinalProtocolAdapter();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_SNAPSHOT,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmSnapshotPush.encode({
          joinAttemptId: 'ja-1',
          snapshotVersion: 5n,
          snapshot: {
            ownerId: 10001n,
            snapshotTimeMs: 1n,
            farmVersion: 5n,
            plots: [],
            ownerPublicProfile: { playerId: 10002n, displayName: 'Wrong owner', avatarId: 7, level: 12 },
          },
        }).finish(),
      }),
    }))).toThrow(/does not match owner_id/);
  });

  it('keeps a missing owner_public_profile unavailable instead of inventing one', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(snapshotBody(adapter, 1n));
    if (decoded.kind !== 'snapshot') throw new Error('expected snapshot');
    expect(decoded.value.ownerPublicProfile).toBeUndefined();
  });

  it('rejects FARM_SNAPSHOT with inconsistent snapshot_version or missing fence', () => {
    const adapter = new FinalProtocolAdapter();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_SNAPSHOT,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmSnapshotPush.encode({
          joinAttemptId: 'ja-1',
          snapshotVersion: 6n,
          snapshot: { ownerId: 10001n, snapshotTimeMs: 1n, farmVersion: 5n, plots: [] },
        }).finish(),
      }),
    }))).toThrow('does not match');
  });

  it('decodes typed BUY result with private state and deltas', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = BuyResult.encode({
      privateState: PrivateState.create({ economyVersion: 7n, coin: 90n, inventory: [{ itemId: 1001, count: 3n }], level: 1, exp: 0n }),
      coinDelta: -10n,
      itemId: 1001,
      itemDelta: 1n,
    }).finish();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-buy',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.BUY,
        payload,
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value.commandType).toBe('BUY');
    expect(decoded.value.privateState?.wallet).toBe(90);
    expect(decoded.value.privateState?.economyVersion).toBe('7');
    expect(decoded.value.privateState?.inventory.seed).toBe(3);
    expect(decoded.value.coinDelta).toBe(-10);
  });

  it('maps ERR_COMMAND_UNIMPLEMENTED to an explicit feature-unavailable result', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-task',
        status: CommandStatus.REJECTED,
        commandType: ProtoCommandType.CLAIM_TASK,
        error: { errorCode: ErrorCode.ERR_COMMAND_UNIMPLEMENTED, errorMessage: '命令已登记但尚未开放' },
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value.featureUnavailable).toBe(true);
    expect(decoded.value.errorCode).toBe('ERR_COMMAND_UNIMPLEMENTED');
    expect(decoded.value.retryDirective).toBeUndefined();
  });

  it('decodes unlock error codes without collapsing them to ERR_INTERNAL', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-unlock',
        status: CommandStatus.REJECTED,
        commandType: ProtoCommandType.UNLOCK_PLOT,
        error: { errorCode: ErrorCode.ERR_LEVEL_REQUIREMENT_NOT_MET, errorMessage: 'level required' },
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value.commandType).toBe('UNLOCK_PLOT');
    expect(decoded.value.errorCode).toBe('ERR_LEVEL_REQUIREMENT_NOT_MET');
    expect(decoded.value.errorCodeNumber).toBe(5007);
  });

  it('rejects a JOIN_FARM result that carries a forbidden payload', () => {
    const adapter = new FinalProtocolAdapter();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-join',
        status: CommandStatus.ACCEPTED,
        commandType: ProtoCommandType.JOIN_FARM,
        payload: new Uint8Array([1, 2, 3]),
        join: { targetFarmId: 10001n, subscriptionId: 'sub-1', joinAttemptId: 'ja-1' },
      }),
    }))).toThrow('forbidden result payload');
  });

  it('decodes FARM_CHANGE only from the matching subscription context', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FARM_CHANGE,
        farmSubscription: { targetPlayerId: 10001n, targetFarmId: 10001n, subscriptionId: 'sub-1' },
        payload: FarmChangePush.encode({
          change: { ownerId: 10001n, farmVersion: 6n, changedPlots: [] },
        }).finish(),
      }),
    }));
    if (decoded.kind !== 'change') throw new Error('expected change');
    expect(decoded.value.farmVersion).toBe('6');
    expect(decoded.value.subscriptionId).toBe('sub-1');
    expect(decoded.value.source).toBe('live');
  });
});

describe('final protocol adapter · gameplay payload encoders', () => {
  const encodePayload = (payload: CommandPayload): Uint8Array => {
    const adapter = new FinalProtocolAdapter();
    const bytes = adapter.encodeClientFrame({
      kind: 'command',
      value: { operationId: 'op-encode', requestId: 'req-encode', payload },
    });
    const frame = FarmFrame.decode(bytes);
    expect(frame.clientCommand).toBeDefined();
    return frame.clientCommand!.payload;
  };

  it('encodes BUY with a caller-specified quantity', () => {
    const request = BuyRequest.decode(encodePayload({ type: 'BUY', offerId: 1, count: 3 }));
    expect(request.offerId).toBe(1);
    expect(request.count).toBe(3);
  });

  it('encodes SELL with a caller-specified quantity', () => {
    const request = SellRequest.decode(encodePayload({ type: 'SELL', itemId: 2001, count: 5 }));
    expect(request.itemId).toBe(2001);
    expect(request.count).toBe(5n);
  });

  it('encodes task writes with only the authority identifiers', () => {
    const claim = ClaimTaskRequest.decode(encodePayload({ type: 'CLAIM_TASK', taskInstanceId: 'daily:2026-08-08:login' }));
    expect(claim).toEqual({ taskInstanceId: 'daily:2026-08-08:login' });
    expect(Reflect.has(claim, 'reward')).toBe(false);
    expect(Reflect.has(claim, 'progress')).toBe(false);

    const confirm = ConfirmFriendVisitRequest.decode(encodePayload({ type: 'CONFIRM_FRIEND_VISIT', targetFarmId: '10002' }));
    expect(confirm).toEqual({ targetFarmId: 10002n });
    expect(Reflect.has(confirm, 'subscriptionId')).toBe(false);
  });

  it('decodes CLAIM_TASK as one authoritative task/economy reward receipt', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = ClaimTaskResult.encode(ClaimTaskResult.fromPartial({
      privateState: { economyVersion: 8n, coin: 100n, exp: 25n, level: 2, inventory: [] },
      summary: {
        taskStateVersion: 9n,
        claimableCount: 0,
        dailyPeriodId: '2026-08-08',
        dailyTasks: [{
          instanceId: 'daily:2026-08-08:login', taskId: 'daily.login.v1', progress: 1n, target: 1n,
          state: PlayerTaskProgressState.PLAYER_TASK_PROGRESS_CLAIMED,
        }],
      },
      grantedReward: { exp: 25n },
      nextGrowthTask: {
        instanceId: 'growth:1', taskId: 'growth.harvest.v2', progress: 0n, target: 3n,
        state: PlayerTaskProgressState.PLAYER_TASK_PROGRESS_ACTIVE,
      },
      claimedTaskInstanceId: 'daily:2026-08-08:login',
      trustedServerNowMs: 1234n,
    })).finish();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-claim', status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.CLAIM_TASK, payload,
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value.privateState).toMatchObject({ economyVersion: '8', wallet: 100, progression: { level: 2, exp: '25' } });
    expect(decoded.value.taskSummary).toMatchObject({ taskStateVersion: '9', claimableCount: 0 });
    expect(decoded.value.taskGrantedReward).toEqual({ coin: 0, exp: 25, items: [], petUnlock: undefined });
    expect(decoded.value.taskInstanceId).toBe('daily:2026-08-08:login');
    expect(decoded.value.nextGrowthTask).toMatchObject({ instanceId: 'growth:1', target: 3, state: 'ACTIVE' });
  });

  it('decodes the frozen task list detail/reward and fails closed on incomplete write results', () => {
    const adapter = new FinalProtocolAdapter();
    const dailyTasks = [0, 1, 2].map((index) => ({
      progress: {
        instanceId: `daily:${index}`, taskId: `daily.${index}`, progress: BigInt(index === 0 ? 1 : 0), target: 1n,
        state: index === 0
          ? PlayerTaskProgressState.PLAYER_TASK_PROGRESS_CLAIMABLE
          : PlayerTaskProgressState.PLAYER_TASK_PROGRESS_ACTIVE,
      },
      category: 1,
      kind: index + 1,
      definitionVersion: 3n,
      periodId: '2026-08-08',
      configVersion: 7n,
      reward: { exp: 10n },
    }));
    const listPayload = GetTaskListResult.encode(GetTaskListResult.fromPartial({
      summary: {
        taskStateVersion: 4n, claimableCount: 1, dailyPeriodId: '2026-08-08',
        dailyTasks: dailyTasks.map((detail) => detail.progress),
      },
      nextResetAtMs: 2_000_000_000_000n,
      dailyConfigVersion: 7n,
      dailyTasks,
    })).finish();
    const list = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        status: CommandStatus.SUCCEEDED, commandType: ProtoCommandType.GET_TASK_LIST, payload: listPayload,
      }),
    }));
    if (list.kind !== 'result') throw new Error('expected result');
    expect(list.value.taskList?.dailyTasks).toHaveLength(3);
    expect(list.value.taskList?.dailyTasks[0]).toMatchObject({
      progress: { instanceId: 'daily:0', state: 'CLAIMABLE' },
      definitionVersion: '3', configVersion: '7', reward: { exp: 10 },
    });

    const malformedClaim = ClaimTaskResult.encode(ClaimTaskResult.fromPartial({
      claimedTaskInstanceId: 'daily:0',
    })).finish();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 2n, {
      commandResult: CommandResultEnvelope.create({
        status: CommandStatus.SUCCEEDED, commandType: ProtoCommandType.CLAIM_TASK, payload: malformedClaim,
      }),
    }))).toThrow('missing authority state');

    const malformedConfirm = ConfirmFriendVisitResult.encode(ConfirmFriendVisitResult.fromPartial({
      confirmedTaskInstanceId: 'daily:visit',
    })).finish();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 3n, {
      commandResult: CommandResultEnvelope.create({
        status: CommandStatus.SUCCEEDED, commandType: ProtoCommandType.CONFIRM_FRIEND_VISIT, payload: malformedConfirm,
      }),
    }))).toThrow('missing authority summary');
  });

  it('encodes the canonical UNLOCK_PLOT payload with only server fences', () => {
    const request = UnlockPlotRequest.decode(encodePayload({
      type: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      expectedPlotVersion: '11',
      expectedUnlockRuleVersion: '9',
    }));
    expect(request).toEqual({
      targetFarmId: 10001n,
      plotId: 6,
      expectedPlotVersion: 11n,
      expectedUnlockRuleVersion: 9n,
    });
    expect(Reflect.has(request, 'requiredLevel')).toBe(false);
    expect(Reflect.has(request, 'normalCoinPrice')).toBe(false);
  });

  it('decodes all seven fields of the UNLOCK_PLOT result', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = UnlockPlotResult.encode({
      farmId: 10001n,
      plotId: 6,
      appliedUnlockRuleVersion: 9n,
      paidNormalCoin: 1200n,
      normalCoinBalance: 8800n,
      committedPlotVersion: 12n,
      committedFarmVersion: 42n,
    }).finish();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-unlock',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.UNLOCK_PLOT,
        payload,
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value).toMatchObject({
      commandType: 'UNLOCK_PLOT',
      farmId: '10001',
      targetFarmId: '10001',
      plotId: 6,
      appliedUnlockRuleVersion: '9',
      paidNormalCoin: '1200',
      normalCoinBalance: '8800',
      committedPlotVersion: '12',
      committedFarmVersion: '42',
    });
  });

  it('encodes CARE WATER with an empty incident fence', () => {
    const request = CareRequest.decode(encodePayload({
      type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1', careType: 'WATER',
    }));
    expect(request.careType).toBe(CareType.CARE_TYPE_WATER);
    expect(request.incidentId).toBe('');
    expect(request.cropCycleId).toBe('cycle-1');
  });

  it('encodes CARE PEST with the incident fence', () => {
    const request = CareRequest.decode(encodePayload({
      type: 'CARE', plotId: 2, targetFarmId: '10001', cropCycleId: 'cycle-2', careType: 'PEST', incidentId: 'inc-9',
    }));
    expect(request.careType).toBe(CareType.CARE_TYPE_PEST);
    expect(request.incidentId).toBe('inc-9');
  });

  it('fails closed when PEST lacks the incident fence and rejects unknown care types', () => {
    const adapter = new FinalProtocolAdapter();
    expect(() => adapter.encodeClientFrame({
      kind: 'command',
      value: { operationId: 'op-1', payload: { type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1', careType: 'PEST' } },
    })).toThrow(/incident_id/);
    expect(() => adapter.encodeClientFrame({
      kind: 'command',
      value: { operationId: 'op-2', payload: { type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1', careType: 'FERTILIZE' as 'WATER' } },
    })).toThrow(/unsupported care type/);
  });

  it('decodes a GET_PLAYER_BOOTSTRAP success into private state', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = GetPlayerBootstrapResult.encode(GetPlayerBootstrapResult.fromPartial({
      playerStateRevision: 9n,
      trustedServerNowMs: 456n,
      wallet: { coin: 88n },
      inventory: { items: [{ itemId: 2001, count: 2n }] },
    })).finish();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-bootstrap',
        requestId: 'req-bootstrap',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
        payload,
      }),
    }));
    expect(result.kind).toBe('result');
    if (result.kind !== 'result') return;
    expect(result.value.commandType).toBe('GET_PLAYER_BOOTSTRAP');
    expect(result.value.status).toBe('SUCCEEDED');
    expect(result.value.playerStateRevision).toBe('9');
    expect(result.value.trustedServerNowMs).toBe(456);
    expect(result.value.privateState).toBeDefined();
    expect(result.value.privateState!.wallet).toBe(88);
    expect(Object.values(result.value.privateState!.inventory)).toContain(2);
  });

  it('strictly decodes all seven GET_PLAYER_BOOTSTRAP fields', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = GetPlayerBootstrapResult.encode(GetPlayerBootstrapResult.fromPartial({
      playerStateRevision: 12n,
      trustedServerNowMs: 789n,
      profile: { playerId: 10001n, displayName: 'Alice' },
      progression: { level: 3, exp: 120n },
      wallet: { coin: 320n },
      inventory: { items: [{ itemId: 2001, count: 4n }, { itemId: 2002, count: 1n }] },
      featureSummary: {},
    })).finish();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-bootstrap',
        requestId: 'req-bootstrap',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
        payload,
      }),
    }));
    expect(result.kind).toBe('result');
    if (result.kind !== 'result') return;
    expect(result.value.playerStateRevision).toBe('12');
    expect(result.value.trustedServerNowMs).toBe(789);
    expect(result.value.privateState?.profile).toEqual({ playerId: '10001', displayName: 'Alice' });
    expect(result.value.privateState?.progression).toEqual({ level: 3, exp: '120' });
    expect(result.value.privateState?.featureSummary).toEqual({});
    expect(result.value.privateState?.wallet).toBe(320);
    expect(result.value.privateState?.inventory).toEqual({ carrot: 4, '番茄': 1 });
  });

  it('decodes feature_summary.tasks at protobuf field 31', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = GetPlayerBootstrapResult.encode(GetPlayerBootstrapResult.fromPartial({
      playerStateRevision: 31n,
      trustedServerNowMs: 789n,
      wallet: { coin: 320n },
      inventory: { items: [] },
      featureSummary: {
        tasks: {
          taskStateVersion: 17n,
          claimableCount: 2,
          dailyPeriodId: '2026-08-08',
          dailyTasks: [{
            instanceId: 'daily-1',
            taskId: 'care-once',
            progress: 1n,
            target: 1n,
            state: PlayerTaskProgressState.PLAYER_TASK_PROGRESS_CLAIMABLE,
          }],
          currentGrowth: {
            instanceId: 'growth-1',
            taskId: 'harvest-once',
            progress: 0n,
            target: 1n,
            state: PlayerTaskProgressState.PLAYER_TASK_PROGRESS_ACTIVE,
          },
        },
      },
    })).finish();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-bootstrap-tasks',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
        payload,
      }),
    }));
    if (result.kind !== 'result') throw new Error('expected result');
    expect(result.value.privateState?.featureSummary?.tasks).toEqual({
      taskStateVersion: '17',
      claimableCount: 2,
      dailyPeriodId: '2026-08-08',
      dailyTasks: [{ instanceId: 'daily-1', taskId: 'care-once', progress: 1, target: 1, state: 'CLAIMABLE' }],
      currentGrowth: { instanceId: 'growth-1', taskId: 'harvest-once', progress: 0, target: 1, state: 'ACTIVE' },
    });
  });

  it('decodes all stable CARE result fields 5..11', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = ProtoCareResult.encode({
      targetFarmId: 10002n,
      plotId: 5,
      cropCycleId: 'cycle-care-5',
      committedFarmVersion: 11n,
      incidentId: 'incident-6',
      careType: CareType.CARE_TYPE_PEST,
      outcome: CareOutcome.CARE_OUTCOME_ALREADY_RESOLVED,
      resolvedByPlayerId: 10001n,
      committedPlotVersion: 12n,
      careRewardOrderId: 'reward-order-10',
      rewardObligationCreated: true,
    }).finish();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-care-fields',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.CARE,
        payload,
      }),
    }));
    if (result.kind !== 'result') throw new Error('expected result');
    expect(result.value.careResult).toEqual({
      targetFarmId: '10002',
      plotId: 5,
      cropCycleId: 'cycle-care-5',
      committedFarmVersion: '11',
      incidentId: 'incident-6',
      careType: 'PEST',
      outcome: 'ALREADY_RESOLVED',
      resolvedByPlayerId: '10001',
      committedPlotVersion: '12',
      careRewardOrderId: 'reward-order-10',
      rewardObligationCreated: true,
    });
  });

  it('decodes a completed CARE payload carried by GET_OPERATION_RESULT', () => {
    const adapter = new FinalProtocolAdapter();
    const carePayload = ProtoCareResult.encode({
      targetFarmId: 10002n,
      plotId: 2,
      cropCycleId: 'cycle-care-completed',
      committedFarmVersion: 8n,
      incidentId: '',
      careType: CareType.CARE_TYPE_WATER,
      outcome: CareOutcome.CARE_OUTCOME_RESOLVED,
      resolvedByPlayerId: 10001n,
      committedPlotVersion: 9n,
      careRewardOrderId: 'reward-completed',
      rewardObligationCreated: true,
    }).finish();
    const lookupPayload = GetOperationResultResult.encode({
      originalCommandType: ProtoCommandType.CARE,
      originalOperationId: 'op-care-completed',
      lookupStatus: OperationLookupStatus.OPERATION_LOOKUP_COMPLETED,
      retryAfterMs: 0,
      completedRetryDirective: 0,
      completedOutcome: {
        originalCommandType: ProtoCommandType.CARE,
        originalOperationId: 'op-care-completed',
        status: CommandStatus.SUCCEEDED,
        resultPayload: carePayload,
      },
    }).finish();
    const result = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-query-care',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_OPERATION_RESULT,
        payload: lookupPayload,
      }),
    }));
    if (result.kind !== 'result') throw new Error('expected result');
    expect(result.value.operationLookup?.lookupStatus).toBe('COMPLETED');
    expect(result.value.operationLookup?.originalCommandType).toBe('CARE');
    expect(result.value.operationLookup?.completedResult?.operationId).toBe('op-care-completed');
    expect(result.value.operationLookup?.completedResult?.careResult?.careRewardOrderId).toBe('reward-completed');
    expect(result.value.careResult?.rewardObligationCreated).toBe(true);
  });

  it('preserves an honest UNAVAILABLE failed-work terminal from operation lookup', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = GetOperationResultResult.encode({
      originalCommandType: ProtoCommandType.THEFT,
      originalOperationId: 'op-theft-failed',
      lookupStatus: OperationLookupStatus.OPERATION_LOOKUP_COMPLETED,
      retryAfterMs: 0,
      completedRetryDirective: ProtoRetryDirective.REFRESH_AUTHORITY_STATE,
      completedOutcome: {
        originalCommandType: ProtoCommandType.THEFT,
        originalOperationId: 'op-theft-failed',
        status: CommandStatus.UNAVAILABLE,
        error: { errorCode: ErrorCode.ERR_INTERNAL, errorMessage: 'refresh authority state' },
        farmContext: { targetFarmId: 10002n },
        resultPayload: new Uint8Array(),
      },
    }).finish();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-query-failed', commandType: ProtoCommandType.GET_OPERATION_RESULT,
        status: CommandStatus.SUCCEEDED, payload,
      }),
    }));
    if (decoded.kind !== 'result') throw new Error('expected result');
    expect(decoded.value.operationLookup).toMatchObject({
      originalOperationId: 'op-theft-failed',
      originalCommandType: 'THEFT',
      lookupStatus: 'COMPLETED',
      completedStatus: 'UNAVAILABLE',
      completedErrorCode: 'ERR_INTERNAL',
      completedTargetFarmId: '10002',
    });
  });

  it('decodes FRIEND_OPPORTUNITY_CHANGED strictly as a pull hint', () => {
    const adapter = new FinalProtocolAdapter();
    const decoded = adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      serverPush: ServerPushEnvelope.create({
        pushType: PushType.FRIEND_OPPORTUNITY_CHANGED,
        player: { targetPlayerId: 10001n },
        payload: FriendOpportunityChanged.encode({
          targetOwnerId: 10002n,
          sourceFarmVersion: 9n,
          opportunityRevision: 10n,
        }).finish(),
      }),
    }));
    expect(decoded).toEqual({ kind: 'friend-opportunity-changed', value: {
      targetOwnerId: '10002', sourceFarmVersion: '9', opportunityRevision: '10',
    } });
  });

  it('fails closed on a malformed GET_PLAYER_BOOTSTRAP success missing wallet/inventory', () => {
    const adapter = new FinalProtocolAdapter();
    const payload = GetPlayerBootstrapResult.encode(GetPlayerBootstrapResult.fromPartial({
      playerStateRevision: 9n,
      trustedServerNowMs: 456n,
    })).finish();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 1n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-bootstrap',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
        payload,
      }),
    }))).toThrow(/malformed success/);

    // 缺失 inventory 同理:绝不把缺失库存解释为 0。
    const noInventory = GetPlayerBootstrapResult.encode(GetPlayerBootstrapResult.fromPartial({
      playerStateRevision: 9n,
      trustedServerNowMs: 456n,
      wallet: { coin: 88n },
    })).finish();
    expect(() => adapter.decodeServerFrame(serverFrame(adapter, 2n, {
      commandResult: CommandResultEnvelope.create({
        operationId: 'op-bootstrap-2',
        status: CommandStatus.SUCCEEDED,
        commandType: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
        payload: noInventory,
      }),
    }))).toThrow(/malformed success/);
  });
});
