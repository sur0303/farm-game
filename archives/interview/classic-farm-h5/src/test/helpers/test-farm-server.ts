/**
 * 最终公网协议(protocol_version=4)进程内假服务器,仅供单元测试。
 *
 * 与生产 Gate 同一形状:FarmFrame oneof + 严格 sequence、
 * ACCEPTED→FARM_SNAPSHOT→SUBSCRIPTION_READY 唯一 Join 下行、
 * ClientRecoveryComplete 写准入栅栏、Typed Payload/Result、
 * GET_OPERATION_RESULT 结果目录。业务裁决是玩具实现,不证明服务端行为。
 */
import type { BinaryTransport } from '../../network/transport';
import { createTestTransportPair } from './test-transport';
import { ClientHello, ServerHello } from '../../protocol/generated/common/control';
import {
  ClientCommandEnvelope,
  CommandResultEnvelope,
  CommandType as ProtoCommandType,
  PushType,
  RetryDirective as ProtoRetryDirective,
  ServerPushEnvelope,
} from '../../protocol/generated/common/envelope';
import { FarmFrame } from '../../protocol/generated/common/frame';
import { CommandStatus, ErrorCode } from '../../protocol/generated/common/enums';
import {
  BuyRequest,
  BuyResult,
  CareRequest,
  CareResult,
  GetOperationResultRequest,
  GetOperationResultResult,
  GetPlayerBootstrapResult,
  HarvestRequest,
  HarvestResult,
  JoinFarmRequest,
  OperationLookupStatus,
  PublicProfileView,
  PlantRequest,
  PlantResult,
  SellRequest,
  SellResult,
  TheftRequest,
  TheftResult,
} from '../../protocol/generated/public/commands';
import {
  FarmChange,
  FarmChangePush,
  FarmSnapshot,
  FarmSnapshotPush,
  SubscriptionReadyPush,
  type PlotView,
} from '../../protocol/generated/farm/farm_view';
import { CropPhase, PlotLockState } from '../../protocol/generated/farm/farm';
import { PrivateState } from '../../protocol/generated/player/player';
import {
  cropById,
  cropByOfferId,
  cropByProduceItemId,
} from '../../domain/crop-catalog';

export const TEST_PLAYER_FARM_IDS: Record<string, bigint> = {
  'player-a': 10001n,
  'player-b': 10002n,
};

export const testFarmIdOf = (playerId: string): bigint => TEST_PLAYER_FARM_IDS[playerId] ?? 10001n;
export const testPlayerIdOf = (farmId: bigint): string =>
  Object.entries(TEST_PLAYER_FARM_IDS).find(([, id]) => id === farmId)?.[0] ?? `player-${farmId}`;

interface FakePlot {
  plotIndex: number;
  plotVersion: bigint;
  cropCycleId: string;
  cropDefinitionId: number;
  plantAtMs: bigint;
  matureAtMs: bigint;
  watered: boolean;
}

interface FakeFarm {
  farmId: bigint;
  farmVersion: bigint;
  plots: FakePlot[];
}

interface FakePlayer {
  playerId: string;
  economyVersion: bigint;
  wallet: bigint;
  inventory: Record<string, number>;
}

interface StoredOutcome {
  commandType: number;
  status: CommandStatus;
  errorCode: ErrorCode;
  message: string;
  resultPayload: Uint8Array;
  targetFarmId?: bigint;
}

interface ConnectionState {
  playerId: string;
  serverSeq: bigint;
  lastClientSeq: bigint;
  recoveryComplete: boolean;
  subscription: { farmId: bigint; subscriptionId: string; joinAttemptId: string } | null;
}

type FriendshipAuthorizer = (viewerId: string, targetOwnerId: string) => boolean;

const asArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const copy = new Uint8Array(bytes.length);
  copy.set(bytes);
  return copy.buffer;
};

const privateStateOf = (player: FakePlayer): PrivateState => ({
  economyVersion: player.economyVersion,
  coin: player.wallet,
  inventory: Object.entries(player.inventory).map(([key, count]) => {
    const itemId = Number(key);
    return { itemId, count: BigInt(count) };
  }),
  level: 1,
  exp: 0n,
  codexEntries: [],
  tasks: [],
});

export class TestFarmServer {
  private readonly usedTickets = new Set<string>();
  private readonly outcomes = new Map<string, StoredOutcome>();
  private readonly players = new Map<string, FakePlayer>();
  private readonly farms = new Map<bigint, FakeFarm>();
  private readonly connections = new Map<BinaryTransport, ConnectionState>();
  private activeTransport: BinaryTransport | null = null;
  private attemptCounter = 0;
  /** 与后端基线一致:合同已注册但未接线的命令 → REJECTED + 1007。 */
  private readonly featureDisabled = new Set<number>([ProtoCommandType.CLAIM_TASK]);

  constructor(
    private readonly canAccessFriendFarm: FriendshipAuthorizer = (viewerId, targetOwnerId) => (
      viewerId === 'player-a' && targetOwnerId === 'player-b'
    ),
    extraFeatureDisabled: readonly number[] = [],
  ) {
    // 默认 CLAIM_TASK 未接线;调用方可追加(如 GET_PLAYER_BOOTSTRAP)以模拟
    // Gate 准入拒绝 REJECTED + ERR_COMMAND_UNIMPLEMENTED(1007) 且回显
    // operation_id/request_id 的合同行为。
    for (const commandType of extraFeatureDisabled) this.featureDisabled.add(commandType);
    this.ensurePlayer('player-a');
    const friend = this.ensurePlayer('player-b');
    friend.wallet = 65n;
    friend.inventory = { '1001': 1, '2001': 2, '1002': 1, '2002': 1 };
    const friendFarm = this.farms.get(testFarmIdOf('player-b'))!;
    friendFarm.plots[0] = {
      plotIndex: 1,
      plotVersion: 2n,
      cropCycleId: 'friend-crop-1',
      cropDefinitionId: 2,
      plantAtMs: BigInt(Date.now()),
      matureAtMs: BigInt(Date.now() + 5000),
      watered: false,
    };
    friendFarm.plots[1] = {
      plotIndex: 2,
      plotVersion: 2n,
      cropCycleId: 'friend-crop-2',
      cropDefinitionId: 1,
      plantAtMs: BigInt(Date.now() - 10000),
      matureAtMs: BigInt(Date.now() - 1),
      watered: false,
    };
  }

  private ensurePlayer(playerId: string): FakePlayer {
    let player = this.players.get(playerId);
    if (!player) {
      player = {
        playerId,
        economyVersion: 1n,
        wallet: 100n,
        inventory: { '1001': 2, '2001': 0 },
      };
      this.players.set(playerId, player);
      const farmId = testFarmIdOf(playerId);
      this.farms.set(farmId, {
        farmId,
        farmVersion: 1n,
        plots: Array.from({ length: 6 }, (_, index) => ({
          plotIndex: index + 1,
          plotVersion: 1n,
          cropCycleId: '',
          cropDefinitionId: 0,
          plantAtMs: 0n,
          matureAtMs: 0n,
          watered: false,
        })),
      });
    }
    return player;
  }

  createClientTransport(): BinaryTransport {
    const [client, server] = createTestTransportPair();
    server.onmessage = (event) => this.handle(server, new Uint8Array(event.data));
    server.onclose = () => {
      this.connections.delete(server);
      if (this.activeTransport === server) this.activeTransport = null;
    };
    this.activeTransport = server;
    server.open?.();
    return client;
  }

  disconnect(): void {
    this.activeTransport?.close();
  }

  private handle(transport: BinaryTransport, bytes: Uint8Array): void {
    try {
      const frame = FarmFrame.decode(bytes);
      if (frame.protocolVersion !== 4) {
        transport.close(4002, 'unsupported protocol version');
        return;
      }
      const connection = this.connections.get(transport);
      if (connection) {
        if (frame.sequence !== connection.lastClientSeq + 1n) {
          transport.close(4002, 'client sequence gap');
          return;
        }
        connection.lastClientSeq = frame.sequence;
      }
      if (frame.clientHello) {
        this.authenticate(transport, frame.clientHello, frame.sequence);
        return;
      }
      if (!connection) {
        transport.close(4001, 'not authenticated');
        return;
      }
      if (frame.control?.clientRecoveryComplete) {
        // 与 Gate 一致:空 marker 不能绕过自己农场 ActiveSubscription 前置。
        if (connection.subscription?.farmId === testFarmIdOf(connection.playerId)) {
          connection.recoveryComplete = true;
        }
        return;
      }
      if (frame.clientCommand) this.handleCommand(transport, connection, frame.clientCommand);
    } catch (error) {
      transport.onerror?.(error);
    }
  }

  private authenticate(transport: BinaryTransport, hello: ClientHello, sequence: bigint): void {
    const ticket = hello.connectionTicket;
    if (this.usedTickets.has(ticket) || !ticket.startsWith('test-ticket-')) {
      transport.close(4001, 'ticket rejected');
      return;
    }
    this.usedTickets.add(ticket);
    const playerId = ticket.split('-').slice(2, -1).join('-') || 'player-a';
    this.ensurePlayer(playerId);
    this.connections.set(transport, {
      playerId,
      serverSeq: 0n,
      lastClientSeq: sequence,
      recoveryComplete: false,
      subscription: null,
    });
    this.send(transport, {
      serverHello: ServerHello.create({
        sessionId: `test-session-${Date.now()}`,
        activeSessionEpoch: 1n,
        sessionAuthorityEpoch: 1n,
        serverTimeMs: BigInt(Date.now()),
        maxFrameBytes: 1 << 20,
        playerId: testFarmIdOf(playerId),
      }),
    });
  }

  private handleCommand(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope): void {
    const resultBase = {
      operationId: command.operationId,
      requestId: command.requestId,
      commandType: command.commandType,
    };
    const reject = (status: CommandStatus, errorCode: ErrorCode, message: string): void => {
      this.sendResult(transport, resultBase, status, errorCode, message);
    };
    if (this.featureDisabled.has(command.commandType)) {
      reject(CommandStatus.REJECTED, ErrorCode.ERR_COMMAND_UNIMPLEMENTED, '命令已登记但尚未开放');
      return;
    }
    if (command.commandType === ProtoCommandType.JOIN_FARM) {
      this.handleJoin(transport, connection, command);
      return;
    }
    if (command.commandType === ProtoCommandType.GET_PLAYER_BOOTSTRAP) {
      const player = this.players.get(connection.playerId)!;
      const payload = GetPlayerBootstrapResult.encode({
        playerStateRevision: player.economyVersion,
        profile: { playerId: testFarmIdOf(connection.playerId), displayName: connection.playerId },
        progression: { level: 1, exp: 0n },
        wallet: { coin: player.wallet },
        inventory: {
          items: Object.entries(player.inventory).map(([key, count]) => ({ itemId: Number(key), count: BigInt(count) })),
        },
        featureSummary: {
          tasks: {
            taskStateVersion: player.economyVersion,
            claimableCount: 0,
            dailyPeriodId: 'test-daily',
            dailyTasks: [{
              instanceId: `daily:${connection.playerId}:buy-seed`,
              taskId: 'buy-seed',
              progress: 0n,
              target: 1n,
              state: 1,
            }],
            currentGrowth: {
              instanceId: `growth:${connection.playerId}:harvest`,
              taskId: 'harvest-first-crop',
              progress: 0n,
              target: 1n,
              state: 1,
            },
          },
        },
        trustedServerNowMs: BigInt(Date.now()),
      }).finish();
      this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
      return;
    }
    if (command.commandType === ProtoCommandType.GET_OPERATION_RESULT) {
      this.handleLookup(transport, connection, command);
      return;
    }
    // 写命令:恢复屏障 + 当前订阅栅栏。
    if (!connection.recoveryComplete) {
      reject(CommandStatus.REJECTED, ErrorCode.ERR_RECOVERY_INCOMPLETE, '客户端恢复屏障尚未完成');
      return;
    }
    const existing = this.outcomes.get(command.operationId);
    if (existing) {
      this.sendResult(transport, resultBase, existing.status, existing.errorCode, existing.message, existing.resultPayload);
      return;
    }
    switch (command.commandType) {
      case ProtoCommandType.BUY:
        this.handleBuy(transport, connection, command, resultBase);
        return;
      case ProtoCommandType.SELL:
        this.handleSell(transport, connection, command, resultBase);
        return;
      case ProtoCommandType.PLANT:
        this.handlePlant(transport, connection, command, resultBase);
        return;
      case ProtoCommandType.HARVEST:
        this.handleHarvest(transport, connection, command, resultBase);
        return;
      case ProtoCommandType.CARE:
        this.handleCare(transport, connection, command, resultBase);
        return;
      case ProtoCommandType.THEFT:
        this.handleTheft(transport, connection, command, resultBase);
        return;
      default:
        reject(CommandStatus.REJECTED, ErrorCode.ERR_UNKNOWN_COMMAND, '暂不支持该命令');
    }
  }

  private requireFarmSubscription(
    connection: ConnectionState,
    targetFarmId: bigint,
    resultBase: { operationId: string; requestId: string; commandType: number },
    transport: BinaryTransport,
  ): FakeFarm | null {
    const farm = this.farms.get(targetFarmId);
    if (!farm || connection.subscription?.farmId !== targetFarmId) {
      this.send(transport, {
        commandResult: CommandResultEnvelope.create({
          ...resultBase,
          status: CommandStatus.REJECTED,
          error: { errorCode: ErrorCode.ERR_NOT_IN_FARM, errorMessage: '当前不在该农场' },
          retryDirective: ProtoRetryDirective.FRESH_JOIN,
        }),
      });
      return null;
    }
    return farm;
  }

  private handleJoin(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope): void {
    const request = JoinFarmRequest.decode(command.payload);
    const ownerId = testPlayerIdOf(request.targetFarmId);
    const resultBase = {
      operationId: command.operationId,
      requestId: command.requestId,
      commandType: command.commandType,
    };
    if (ownerId !== connection.playerId && !this.canAccessFriendFarm(connection.playerId, ownerId)) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_NOT_FRIEND, '未建立好友关系，不能访问该农场');
      return;
    }
    const farm = this.farms.get(request.targetFarmId);
    if (!farm) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_FOUND, '目标农场不存在');
      return;
    }
    this.attemptCounter += 1;
    const subscriptionId = `sub-${this.attemptCounter}`;
    const joinAttemptId = `ja-${this.attemptCounter}`;
    connection.subscription = { farmId: farm.farmId, subscriptionId, joinAttemptId };
    // 唯一 Join 下行:ACCEPTED → FARM_SNAPSHOT → SUBSCRIPTION_READY。
    this.sendResult(transport, {
      ...resultBase,
      join: {
        targetFarmId: farm.farmId,
        subscriptionId,
        joinAttemptId,
      },
    }, CommandStatus.ACCEPTED, ErrorCode.OK, '');
    this.sendPush(transport, PushType.FARM_SNAPSHOT, farm, FarmSnapshotPush.encode({
      joinAttemptId,
      snapshotVersion: farm.farmVersion,
      snapshot: this.snapshotOf(farm),
    }).finish());
    this.sendPush(transport, PushType.SUBSCRIPTION_READY, farm, SubscriptionReadyPush.encode({
      joinAttemptId,
      readyThroughVersion: farm.farmVersion,
    }).finish());
  }

  private handleLookup(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope): void {
    const request = GetOperationResultRequest.decode(command.payload);
    const stored = this.outcomes.get(request.originalOperationId);
    const resultBase = {
      operationId: command.operationId,
      requestId: command.requestId,
      commandType: command.commandType,
    };
    const payload = GetOperationResultResult.encode(stored ? {
      originalCommandType: request.originalCommandType,
      originalOperationId: request.originalOperationId,
      lookupStatus: OperationLookupStatus.OPERATION_LOOKUP_COMPLETED,
      retryAfterMs: 0,
      completedOutcome: {
        originalCommandType: stored.commandType,
        originalOperationId: request.originalOperationId,
        status: stored.status,
        error: stored.errorCode === ErrorCode.OK ? undefined : { errorCode: stored.errorCode, errorMessage: stored.message },
        farmContext: stored.targetFarmId ? { targetFarmId: stored.targetFarmId } : undefined,
        resultPayload: stored.resultPayload,
      },
      completedRetryDirective: ProtoRetryDirective.NONE,
    } : {
      originalCommandType: request.originalCommandType,
      originalOperationId: request.originalOperationId,
      lookupStatus: OperationLookupStatus.OPERATION_LOOKUP_NOT_FOUND,
      retryAfterMs: 0,
      completedRetryDirective: ProtoRetryDirective.NONE,
    }).finish();
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handleBuy(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = BuyRequest.decode(command.payload);
    const player = this.players.get(connection.playerId)!;
    const crop = cropByOfferId(request.offerId);
    if (!crop) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_OFFER_NOT_FOUND, '商品不存在');
      return;
    }
    const price = BigInt(crop.buyPrice) * BigInt(request.count || 1);
    if (player.wallet < price) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_INSUFFICIENT_COIN, '金币不足');
      return;
    }
    player.wallet -= price;
    player.economyVersion += 1n;
    const key = String(crop.seedItemId);
    player.inventory[key] = (player.inventory[key] ?? 0) + (request.count || 1);
    const payload = BuyResult.encode({
      privateState: privateStateOf(player),
      coinDelta: -price,
      itemId: crop.seedItemId,
      itemDelta: BigInt(request.count || 1),
    }).finish();
    this.outcomes.set(command.operationId, {
      commandType: command.commandType,
      status: CommandStatus.SUCCEEDED,
      errorCode: ErrorCode.OK,
      message: '',
      resultPayload: payload,
    });
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handleSell(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = SellRequest.decode(command.payload);
    const player = this.players.get(connection.playerId)!;
    const crop = cropByProduceItemId(request.itemId);
    const key = String(request.itemId);
    if (!crop || (player.inventory[key] ?? 0) <= 0) {
      this.sendResult(transport, resultBase, CommandStatus.NO_CHANGE, ErrorCode.ERR_INSUFFICIENT_ITEM, '没有可出售的作物');
      return;
    }
    player.inventory[key] -= 1;
    player.wallet += BigInt(crop.sellPrice);
    player.economyVersion += 1n;
    const payload = SellResult.encode({
      privateState: privateStateOf(player),
      coinDelta: BigInt(crop.sellPrice),
      itemId: crop.produceItemId,
      itemDelta: -1n,
    }).finish();
    this.outcomes.set(command.operationId, {
      commandType: command.commandType,
      status: CommandStatus.SUCCEEDED,
      errorCode: ErrorCode.OK,
      message: '',
      resultPayload: payload,
    });
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handlePlant(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = PlantRequest.decode(command.payload);
    const farm = this.requireFarmSubscription(connection, request.targetFarmId, resultBase, transport);
    if (!farm) return;
    const player = this.players.get(connection.playerId)!;
    const plot = farm.plots.find((candidate) => candidate.plotIndex === request.plotId);
    if (!plot) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_FOUND, '地块不存在');
      return;
    }
    if (plot.cropCycleId) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_EMPTY, '地块非空，不能播种');
      return;
    }
    const crop = cropById(request.cropDefinitionId || 1);
    const seedKey = String(crop.seedItemId);
    if ((player.inventory[seedKey] ?? 0) <= 0) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_INSUFFICIENT_ITEM, `${crop.name}种子不足`);
      return;
    }
    player.inventory[seedKey] -= 1;
    player.economyVersion += 1n;
    plot.plotVersion += 1n;
    plot.cropCycleId = `${command.operationId}-crop`;
    plot.cropDefinitionId = crop.id;
    plot.plantAtMs = BigInt(Date.now());
    plot.matureAtMs = BigInt(Date.now() + crop.matureAfterMs);
    plot.watered = false;
    const payload = PlantResult.encode({
      targetFarmId: farm.farmId,
      plotId: plot.plotIndex,
      cropCycleId: plot.cropCycleId,
      committedFarmVersion: farm.farmVersion + 1n,
    }).finish();
    this.commitFarmChange(transport, connection, farm, plot);
    this.outcomes.set(command.operationId, {
      commandType: command.commandType,
      status: CommandStatus.SUCCEEDED,
      errorCode: ErrorCode.OK,
      message: '',
      resultPayload: payload,
      targetFarmId: farm.farmId,
    });
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handleHarvest(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = HarvestRequest.decode(command.payload);
    const farm = this.requireFarmSubscription(connection, request.targetFarmId, resultBase, transport);
    if (!farm) return;
    const player = this.players.get(connection.playerId)!;
    const plot = farm.plots.find((candidate) => candidate.plotIndex === request.plotId);
    if (!plot) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_FOUND, '地块不存在');
      return;
    }
    if (!plot.cropCycleId) {
      this.sendResult(transport, resultBase, CommandStatus.NO_CHANGE, ErrorCode.ERR_PLOT_EMPTY, '地块已经是空的');
      return;
    }
    if (plot.matureAtMs > BigInt(Date.now())) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_CROP_NOT_MATURE, '作物还未成熟');
      return;
    }
    const crop = cropById(plot.cropDefinitionId || 1);
    const cycleId = plot.cropCycleId;
    plot.cropCycleId = '';
    plot.cropDefinitionId = 0;
    plot.plantAtMs = 0n;
    plot.matureAtMs = 0n;
    plot.watered = false;
    plot.plotVersion += 1n;
    const produceKey = String(crop.produceItemId);
    player.inventory[produceKey] = (player.inventory[produceKey] ?? 0) + 1;
    player.economyVersion += 1n;
    const payload = HarvestResult.encode({
      targetFarmId: farm.farmId,
      plotId: plot.plotIndex,
      cropCycleId: cycleId,
      committedFarmVersion: farm.farmVersion + 1n,
      harvestedQuantity: 1,
      compendiumNewlyUnlocked: false,
      compendiumRevision: 0n,
    }).finish();
    this.commitFarmChange(transport, connection, farm, plot);
    this.outcomes.set(command.operationId, {
      commandType: command.commandType,
      status: CommandStatus.SUCCEEDED,
      errorCode: ErrorCode.OK,
      message: '',
      resultPayload: payload,
      targetFarmId: farm.farmId,
    });
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handleCare(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = CareRequest.decode(command.payload);
    const farm = this.requireFarmSubscription(connection, request.targetFarmId, resultBase, transport);
    if (!farm) return;
    const plot = farm.plots.find((candidate) => candidate.plotIndex === request.plotId);
    if (!plot) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_FOUND, '地块不存在');
      return;
    }
    if (!plot.cropCycleId || plot.matureAtMs <= BigInt(Date.now())) {
      this.sendResult(transport, resultBase, CommandStatus.NO_CHANGE, ErrorCode.ERR_CROP_ALREADY_MATURE, '当前没有可浇水的作物');
      return;
    }
    if (plot.watered) {
      this.sendResult(transport, resultBase, CommandStatus.NO_CHANGE, ErrorCode.OK, '这块地今天已经浇过水了');
      return;
    }
    plot.watered = true;
    plot.plotVersion += 1n;
    const payload = CareResult.encode({
      targetFarmId: farm.farmId,
      plotId: plot.plotIndex,
      cropCycleId: plot.cropCycleId,
      committedFarmVersion: farm.farmVersion + 1n,
      incidentId: '',
      careType: request.careType,
      outcome: 1,
      resolvedByPlayerId: BigInt(TEST_PLAYER_FARM_IDS[connection.playerId] ?? 10001n),
      committedPlotVersion: plot.plotVersion,
      careRewardOrderId: `care:${command.operationId}`,
      rewardObligationCreated: false,
    }).finish();
    this.commitFarmChange(transport, connection, farm, plot);
    this.outcomes.set(command.operationId, {
      commandType: command.commandType,
      status: CommandStatus.SUCCEEDED,
      errorCode: ErrorCode.OK,
      message: '',
      resultPayload: payload,
      targetFarmId: farm.farmId,
    });
    this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
  }

  private handleTheft(transport: BinaryTransport, connection: ConnectionState, command: ClientCommandEnvelope, resultBase: { operationId: string; requestId: string; commandType: number }): void {
    const request = TheftRequest.decode(command.payload);
    const farm = this.requireFarmSubscription(connection, request.targetFarmId, resultBase, transport);
    if (!farm) return;
    if (request.targetFarmId === testFarmIdOf(connection.playerId)) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PERMISSION_DENIED, '不能从自己的农场偷菜');
      return;
    }
    const plot = farm.plots.find((candidate) => candidate.plotIndex === request.plotId);
    if (!plot) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_PLOT_NOT_FOUND, '地块不存在');
      return;
    }
    if (!plot.cropCycleId || plot.matureAtMs > BigInt(Date.now())) {
      this.sendResult(transport, resultBase, CommandStatus.REJECTED, ErrorCode.ERR_CROP_NOT_MATURE, '目标作物尚未成熟');
      return;
    }
    const crop = cropById(plot.cropDefinitionId || 1);
    const cycleId = plot.cropCycleId;
    plot.cropCycleId = '';
    plot.cropDefinitionId = 0;
    plot.plantAtMs = 0n;
    plot.matureAtMs = 0n;
    plot.watered = false;
    plot.plotVersion += 1n;
    const settlementId = `settlement-${command.operationId}`;
    this.commitFarmChange(transport, connection, farm, plot);
    // PROCESSING 不携带 Typed Result;终态经结果目录/查询收敛。
    this.sendResult(transport, resultBase, CommandStatus.PROCESSING, ErrorCode.OK, '偷菜处理中');
    window.setTimeout(() => {
      const visitor = this.players.get(connection.playerId);
      if (!visitor) return;
      const produceKey = String(crop.produceItemId);
      visitor.inventory[produceKey] = (visitor.inventory[produceKey] ?? 0) + 1;
      visitor.economyVersion += 1n;
      const payload = TheftResult.encode({
        targetFarmId: farm.farmId,
        plotId: plot.plotIndex,
        cropCycleId: cycleId,
        committedFarmVersion: farm.farmVersion,
        settlementId,
      }).finish();
      this.outcomes.set(command.operationId, {
        commandType: command.commandType,
        status: CommandStatus.SUCCEEDED,
        errorCode: ErrorCode.OK,
        message: '',
        resultPayload: payload,
        targetFarmId: farm.farmId,
      });
      if (transport.readyState === 1) {
        this.sendResult(transport, resultBase, CommandStatus.SUCCEEDED, ErrorCode.OK, '', payload);
      }
    }, 250);
  }

  private snapshotOf(farm: FakeFarm): FarmSnapshot {
    return {
      ownerId: farm.farmId,
      snapshotTimeMs: BigInt(Date.now()),
      farmVersion: farm.farmVersion,
      ownerPublicProfile: {
        playerId: farm.farmId,
        displayName: testPlayerIdOf(farm.farmId),
        avatarId: farm.farmId === testFarmIdOf('player-b') ? 2 : 1,
        level: 1,
      } satisfies PublicProfileView,
      plots: farm.plots.map((plot) => this.plotViewOf(plot)),
    };
  }

  private plotViewOf(plot: FakePlot): PlotView {
    return {
      plotIndex: plot.plotIndex,
      plotVersion: plot.plotVersion,
      cropCycleId: plot.cropCycleId,
      cropDefinitionId: plot.cropDefinitionId,
      cropPhase: !plot.cropCycleId
        ? CropPhase.CROP_PHASE_UNSPECIFIED
        : plot.matureAtMs <= BigInt(Date.now()) ? CropPhase.MATURE : CropPhase.GROWING,
      plantAtMs: plot.plantAtMs,
      matureAtMs: plot.matureAtMs,
      harvestableCount: plot.cropCycleId ? 1 : 0,
      stolenTotal: 0,
      watered: plot.watered,
      lockState: PlotLockState.PLOT_LOCK_STATE_UNLOCKED,
      unlockOrder: plot.plotIndex,
      requiredLevel: 1,
      normalCoinPrice: 0n,
      unlockCandidate: false,
      unlockRuleVersion: 1n,
    };
  }

  private commitFarmChange(transport: BinaryTransport, connection: ConnectionState, farm: FakeFarm, plot: FakePlot): void {
    farm.farmVersion += 1n;
    if (connection.subscription?.farmId !== farm.farmId) return;
    this.sendPush(transport, PushType.FARM_CHANGE, farm, FarmChangePush.encode({
      change: FarmChange.create({
        ownerId: farm.farmId,
        farmVersion: farm.farmVersion,
        changedPlots: [this.plotViewOf(plot)],
      }),
    }).finish());
  }

  private sendResult(
    transport: BinaryTransport,
    base: { operationId: string; requestId: string; commandType: number } & {
      join?: { targetFarmId: bigint; subscriptionId: string; joinAttemptId: string };
    },
    status: CommandStatus,
    errorCode: ErrorCode,
    message: string,
    payload?: Uint8Array,
  ): void {
    this.send(transport, {
      commandResult: CommandResultEnvelope.create({
        operationId: base.operationId,
        requestId: base.requestId,
        commandType: base.commandType,
        status,
        ...(errorCode !== ErrorCode.OK ? { error: { errorCode, errorMessage: message } } : {}),
        ...(base.join ? { join: base.join } : {}),
        ...(payload && payload.length > 0 ? { payload } : {}),
      }),
    });
  }

  private sendPush(transport: BinaryTransport, pushType: PushType, farm: FakeFarm, payload: Uint8Array): void {
    const connection = this.connections.get(transport);
    if (!connection?.subscription) return;
    this.send(transport, {
      serverPush: ServerPushEnvelope.create({
        pushType,
        farmSubscription: {
          targetPlayerId: testFarmIdOf(connection.playerId),
          targetFarmId: farm.farmId,
          subscriptionId: connection.subscription.subscriptionId,
        },
        payload,
      }),
    });
  }

  private send(transport: BinaryTransport, body: Pick<FarmFrame, 'serverHello' | 'commandResult' | 'serverPush' | 'control'>): void {
    const connection = this.connections.get(transport);
    if (!connection) return;
    connection.serverSeq += 1n;
    transport.send(asArrayBuffer(FarmFrame.encode({
      protocolVersion: 4,
      sequence: connection.serverSeq,
      ...body,
    }).finish()));
  }
}
