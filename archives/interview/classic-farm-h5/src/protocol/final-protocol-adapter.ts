/**
 * 最终公网协议适配器(P1B freeze,protocol_version=4)。
 *
 * 唯一生产命令主链。与 farm-game @ cp2-round2-20260804 对齐:
 * - FarmFrame:protocol_version=4、sequence=8(uint64,每连接每方向从 1 严格 +1)、
 *   frame_body oneof;deprecated kind/connection_seq/body 不发送也不消费。
 * - ClientCommandEnvelope 只携带 request_id/operation_id/command_type/bytes payload;
 *   deprecated target_owner_id/expected_* 永远不填(Gate 不读)。
 * - Typed Payload/Result 全部来自 generated/public/commands.proto。
 * - uint64 实体 ID 与版本在本接缝全部归一为十进制 string;sequence 用 bigint 计数。
 */
import {
  ClientHello,
  ConnectionError,
  ControlFrame,
  DrainNotice,
  ServerHello,
} from './generated/common/control';
import {
  ClientCommandEnvelope,
  CommandResultEnvelope,
  CommandType as ProtoCommandType,
  ServerPushEnvelope,
  PushType,
  RetryDirective as ProtoRetryDirective,
} from './generated/common/envelope';
import { FarmFrame } from './generated/common/frame';
import { CommandStatus, ErrorCode } from './generated/common/enums';
import {
  FarmChange as ProtoFarmChange,
  FarmChangePush,
  FarmDeltaBatchPush,
  FarmSnapshot as ProtoFarmSnapshot,
  FarmSnapshotPush,
  RejoinReason,
  RejoinRequiredPush,
  SubscriptionReadyPush,
  type PlotView,
} from './generated/farm/farm_view';
import { CropPhase, PlotLockState } from './generated/farm/farm';
import { FriendMutationKind, FriendOpportunityChanged, FriendRosterChanged } from './generated/friend/friend';
import { PrivateState as ProtoPrivateState } from './generated/player/player';
import { TaskState as ProtoTaskState } from './generated/task/task';
import {
  BuyRequest,
  BuyResult,
  CareOutcome,
  CareRequest,
  CareResult,
  CareType,
  ClaimTaskRequest,
  ClaimTaskResult,
  ConfirmFriendVisitRequest,
  ConfirmFriendVisitResult,
  GetOperationResultRequest,
  GetOperationResultResult,
  GetPlayerBootstrapRequest,
  GetPlayerBootstrapResult,
  GetTaskListRequest,
  GetTaskListResult,
  HarvestRequest,
  HarvestResult,
  JoinFarmRequest,
  OperationLookupStatus,
  PlayerTaskProgressState,
  PlantRequest,
  PlantResult,
  SellRequest,
  SellResult,
  TheftRequest,
  TheftResult,
  UnlockPlotRequest,
  UnlockPlotResult,
} from './generated/public/commands';
import { BinaryReader } from '@bufbuild/protobuf/wire';
import type {
  ClientCommand,
  ClientFrame,
  CommandPayload,
  CommandResult,
  CommandType,
  FarmChange,
  FarmSnapshot,
  PublicProfile,
  PlotLockState as PlotLockStateIR,
  PlotState,
  CareResult as CareResultIR,
  OperationLookup,
  PrivateState,
  RetryDirective,
  ServerFrame,
  TaskProgressEntry,
  TaskDetailView,
  TaskListView,
  TaskRewardView,
  TaskSummary,
} from './types';
import type { FarmProtocolAdapter } from './protocol-adapter';
import { parseUint64, uint64String } from './uint64';
import { commandTypeNumber } from './canonical-payload';
import { inventoryKeyForItem } from '../domain/crop-catalog';

export const MAX_CANONICAL_FRAME_SIZE = 1 << 20;
export const CANONICAL_PROTOCOL_VERSION = 4;
const CLIENT_BUILD = 'farm-h5/0.2';

const ERROR_CODE_NAMES: Record<number, string> = {
  [ErrorCode.ERR_BAD_FRAME]: 'ERR_BAD_FRAME',
  [ErrorCode.ERR_DECODE]: 'ERR_DECODE',
  [ErrorCode.ERR_UNSUPPORTED_VERSION]: 'ERR_UNSUPPORTED_VERSION',
  [ErrorCode.ERR_MESSAGE_TOO_LARGE]: 'ERR_MESSAGE_TOO_LARGE',
  [ErrorCode.ERR_UNKNOWN_COMMAND]: 'ERR_UNKNOWN_COMMAND',
  [ErrorCode.ERR_RATE_LIMITED]: 'ERR_RATE_LIMITED',
  [ErrorCode.ERR_COMMAND_UNIMPLEMENTED]: 'ERR_COMMAND_UNIMPLEMENTED',
  [ErrorCode.ERR_UNAUTHENTICATED]: 'ERR_UNAUTHENTICATED',
  [ErrorCode.ERR_SESSION_EXPIRED]: 'ERR_SESSION_EXPIRED',
  [ErrorCode.ERR_SESSION_PERMIT_INVALID]: 'ERR_SESSION_PERMIT_INVALID',
  [ErrorCode.ERR_LOGIN_ELSEWHERE]: 'ERR_LOGIN_ELSEWHERE',
  [ErrorCode.ERR_CONNECTION_TICKET_INVALID]: 'ERR_CONNECTION_TICKET_INVALID',
  [ErrorCode.ERR_RECOVERY_INCOMPLETE]: 'ERR_RECOVERY_INCOMPLETE',
  [ErrorCode.ERR_NOT_FRIEND]: 'ERR_NOT_FRIEND',
  [ErrorCode.ERR_FRIEND_PERMIT_INVALID]: 'ERR_FRIEND_PERMIT_INVALID',
  [ErrorCode.ERR_FRIEND_LIMIT]: 'ERR_FRIEND_LIMIT',
  [ErrorCode.ERR_SHARE_LINK_INVALID]: 'ERR_SHARE_LINK_INVALID',
  [ErrorCode.ERR_PERMISSION_DENIED]: 'ERR_PERMISSION_DENIED',
  [ErrorCode.ERR_INSUFFICIENT_COIN]: 'ERR_INSUFFICIENT_COIN',
  [ErrorCode.ERR_INSUFFICIENT_ITEM]: 'ERR_INSUFFICIENT_ITEM',
  [ErrorCode.ERR_OFFER_NOT_FOUND]: 'ERR_OFFER_NOT_FOUND',
  [ErrorCode.ERR_RULE_VERSION_MISMATCH]: 'ERR_RULE_VERSION_MISMATCH',
  [ErrorCode.ERR_LEVEL_REQUIREMENT_NOT_MET]: 'ERR_LEVEL_REQUIREMENT_NOT_MET',
  [ErrorCode.ERR_PLOT_LOCKED]: 'ERR_PLOT_LOCKED',
  [ErrorCode.ERR_PREVIOUS_PLOT_LOCKED]: 'ERR_PREVIOUS_PLOT_LOCKED',
  [ErrorCode.ERR_TASK_NOT_CLAIMABLE]: 'ERR_TASK_NOT_CLAIMABLE',
  [ErrorCode.ERR_TASK_ALREADY_CLAIMED]: 'ERR_TASK_ALREADY_CLAIMED',
  [ErrorCode.ERR_TASK_INSTANCE_EXPIRED]: 'ERR_TASK_INSTANCE_EXPIRED',
  [ErrorCode.ERR_TASK_INSTANCE_NOT_FOUND]: 'ERR_TASK_INSTANCE_NOT_FOUND',
  [ErrorCode.ERR_PLOT_NOT_EMPTY]: 'ERR_PLOT_NOT_EMPTY',
  [ErrorCode.ERR_PLOT_EMPTY]: 'ERR_PLOT_EMPTY',
  [ErrorCode.ERR_CROP_NOT_MATURE]: 'ERR_CROP_NOT_MATURE',
  [ErrorCode.ERR_VERSION_CONFLICT]: 'ERR_VERSION_CONFLICT',
  [ErrorCode.ERR_PLOT_NOT_FOUND]: 'ERR_PLOT_NOT_FOUND',
  [ErrorCode.ERR_CROP_ALREADY_MATURE]: 'ERR_CROP_ALREADY_MATURE',
  [ErrorCode.ERR_FARM_FULL]: 'ERR_FARM_FULL',
  [ErrorCode.ERR_NOT_IN_FARM]: 'ERR_NOT_IN_FARM',
  [ErrorCode.ERR_ACTOR_REDIRECT]: 'ERR_ACTOR_REDIRECT',
  [ErrorCode.ERR_DOWNLINK_OVERFLOW]: 'ERR_DOWNLINK_OVERFLOW',
  [ErrorCode.ERR_STEAL_ALREADY_DONE]: 'ERR_STEAL_ALREADY_DONE',
  [ErrorCode.ERR_STEAL_QUOTA_EXHAUSTED]: 'ERR_STEAL_QUOTA_EXHAUSTED',
  [ErrorCode.ERR_STEAL_BLOCKED_BY_PET]: 'ERR_STEAL_BLOCKED_BY_PET',
  [ErrorCode.ERR_SETTLEMENT_NOT_FOUND]: 'ERR_SETTLEMENT_NOT_FOUND',
  [ErrorCode.ERR_INTERNAL]: 'ERR_INTERNAL',
  [ErrorCode.ERR_STORE_CAS_CONFLICT]: 'ERR_STORE_CAS_CONFLICT',
  [ErrorCode.ERR_STORE_UNAVAILABLE]: 'ERR_STORE_UNAVAILABLE',
  [ErrorCode.ERR_TIMEOUT]: 'ERR_TIMEOUT',
  [ErrorCode.ERR_IDEMPOTENCY_CONFLICT]: 'ERR_IDEMPOTENCY_CONFLICT',
  [ErrorCode.ERR_SERVER_BUSY]: 'ERR_SERVER_BUSY',
};

const errorCodeName = (code: number): string => ERROR_CODE_NAMES[code] ?? `ERROR_CODE_${code}`;

const STATUS_TEXT: Record<number, CommandResult['status']> = {
  [CommandStatus.SUCCEEDED]: 'SUCCEEDED',
  [CommandStatus.PROCESSING]: 'PROCESSING',
  [CommandStatus.NO_CHANGE]: 'NO_CHANGE',
  [CommandStatus.REJECTED]: 'REJECTED',
  [CommandStatus.UNAVAILABLE]: 'UNAVAILABLE',
  [CommandStatus.ACCEPTED]: 'ACCEPTED',
};

const RETRY_DIRECTIVE_TEXT: Record<number, RetryDirective> = {
  [ProtoRetryDirective.NONE]: 'NONE',
  [ProtoRetryDirective.RETRY_SAME_OPERATION]: 'RETRY_SAME_OPERATION',
  [ProtoRetryDirective.POLL_OPERATION_RESULT]: 'POLL_OPERATION_RESULT',
  [ProtoRetryDirective.FRESH_JOIN]: 'FRESH_JOIN',
  [ProtoRetryDirective.RECONNECT]: 'RECONNECT',
  [ProtoRetryDirective.REAUTHENTICATE]: 'REAUTHENTICATE',
  [ProtoRetryDirective.FRESH_JOIN_THEN_RETRY]: 'FRESH_JOIN_THEN_RETRY',
  [ProtoRetryDirective.REFRESH_AUTHORITY_STATE]: 'REFRESH_AUTHORITY_STATE',
};

const COMMAND_TYPE_BY_NUMBER = new Map<number, CommandType>([
  [ProtoCommandType.JOIN_FARM, 'JOIN_FARM'],
  [ProtoCommandType.UNLOCK_PLOT, 'UNLOCK_PLOT'],
  [ProtoCommandType.BUY, 'BUY'],
  [ProtoCommandType.SELL, 'SELL'],
  [ProtoCommandType.PLANT, 'PLANT'],
  [ProtoCommandType.HARVEST, 'HARVEST'],
  [ProtoCommandType.CARE, 'CARE'],
  [ProtoCommandType.THEFT, 'THEFT'],
  [ProtoCommandType.GET_PLAYER_BOOTSTRAP, 'GET_PLAYER_BOOTSTRAP'],
  [ProtoCommandType.GET_OPERATION_RESULT, 'GET_OPERATION_RESULT'],
  [ProtoCommandType.GET_TASK_LIST, 'GET_TASK_LIST'],
  [ProtoCommandType.CLAIM_TASK, 'CLAIM_TASK'],
  [ProtoCommandType.CONFIRM_FRIEND_VISIT, 'CONFIRM_FRIEND_VISIT'],
]);

const exactNumber = (value: bigint, field: string): number => {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error(`${field} exceeds JavaScript safe integer range`);
  return number;
};

const validDisplayName = (value: string, field: string): string => {
  const displayName = value.trim();
  if (displayName.length === 0 || displayName.length > 64) {
    throw new Error(`${field} must be a non-empty display name`);
  }
  return displayName;
};

const publicProfileToIR = (
  profile: { playerId: bigint; displayName: string; avatarId: number; level: number },
  expectedPlayerId: string,
  field: string,
): PublicProfile => {
  const playerId = uint64String(profile.playerId, `${field}.player_id`);
  if (playerId !== expectedPlayerId) throw new Error(`${field}.player_id does not match owner_id`);
  if (!Number.isInteger(profile.avatarId) || profile.avatarId < 0) throw new Error(`${field}.avatar_id is invalid`);
  if (!Number.isInteger(profile.level) || profile.level < 1) throw new Error(`${field}.level is invalid`);
  return {
    playerId,
    displayName: validDisplayName(profile.displayName, `${field}.display_name`),
    avatarId: profile.avatarId,
    level: profile.level,
  };
};

const taskProgressToIR = (task: {
  instanceId: string;
  taskId: string;
  progress: bigint;
  target: bigint;
  state: PlayerTaskProgressState;
}): TaskProgressEntry => {
  if (!task.instanceId || !task.taskId) throw new Error('task entry is missing instance_id/task_id');
  const progress = exactNumber(task.progress, 'task.progress');
  const target = exactNumber(task.target, 'task.target');
  if (progress < 0 || target < 0 || progress > target) throw new Error('task progress is invalid');
  const state = task.state === PlayerTaskProgressState.PLAYER_TASK_PROGRESS_CLAIMABLE ? 'CLAIMABLE'
    : task.state === PlayerTaskProgressState.PLAYER_TASK_PROGRESS_CLAIMED ? 'CLAIMED'
      : task.state === PlayerTaskProgressState.PLAYER_TASK_PROGRESS_ACTIVE ? 'ACTIVE' : null;
  if (!state) throw new Error('task state is unspecified');
  return { instanceId: task.instanceId, taskId: task.taskId, progress, target, state };
};

const taskSummaryToIR = (summary: {
  taskStateVersion: bigint;
  claimableCount: number;
  dailyPeriodId: string;
  dailyTasks: Array<{ instanceId: string; taskId: string; progress: bigint; target: bigint; state: PlayerTaskProgressState }>;
  currentGrowth?: { instanceId: string; taskId: string; progress: bigint; target: bigint; state: PlayerTaskProgressState };
}): TaskSummary => {
  if (!Number.isInteger(summary.claimableCount) || summary.claimableCount < 0) throw new Error('task claimable_count is invalid');
  if (!summary.dailyPeriodId) throw new Error('task daily_period_id is missing');
  return {
    taskStateVersion: uint64String(summary.taskStateVersion, 'task_state_version'),
    claimableCount: summary.claimableCount,
    dailyPeriodId: summary.dailyPeriodId,
    dailyTasks: summary.dailyTasks.map(taskProgressToIR),
    currentGrowth: summary.currentGrowth ? taskProgressToIR(summary.currentGrowth) : undefined,
  };
};

const taskRewardToIR = (reward: import('./generated/public/commands').PlayerTaskRewardView | undefined): TaskRewardView => ({
  coin: exactNumber(reward?.coin ?? 0n, 'task.reward.coin'),
  exp: exactNumber(reward?.exp ?? 0n, 'task.reward.exp'),
  items: (reward?.items ?? []).map((item) => ({ itemId: item.itemId, count: exactNumber(item.count, 'task.reward.item.count') })),
  petUnlock: reward?.petUnlock ? {
    petId: reward.petUnlock.petId,
    petConfigVersion: uint64String(reward.petUnlock.petConfigVersion, 'task.reward.pet_config_version'),
    grantDefinitionVersion: uint64String(reward.petUnlock.grantDefinitionVersion, 'task.reward.grant_definition_version'),
  } : undefined,
});

const taskDetailToIR = (detail: import('./generated/public/commands').PlayerTaskDetailView): TaskDetailView => {
  if (!detail.progress) throw new Error('task detail is missing progress');
  return {
    progress: taskProgressToIR(detail.progress),
    category: detail.category,
    kind: detail.kind,
    definitionVersion: uint64String(detail.definitionVersion, 'task.definition_version'),
    periodId: detail.periodId,
    configVersion: uint64String(detail.configVersion, 'task.config_version'),
    reward: taskRewardToIR(detail.reward),
  };
};

const taskListToIR = (list: GetTaskListResult): TaskListView => {
  if (!list.summary) throw new Error('task list is missing summary');
  const nextResetAtMs = exactNumber(list.nextResetAtMs, 'task.next_reset_at_ms');
  if (nextResetAtMs <= 0 || list.dailyTasks.length !== 3) throw new Error('task list shape is invalid');
  return {
    summary: taskSummaryToIR(list.summary),
    nextResetAtMs,
    dailyConfigVersion: uint64String(list.dailyConfigVersion, 'task.daily_config_version'),
    dailyTasks: list.dailyTasks.map(taskDetailToIR),
    currentGrowth: list.currentGrowth ? taskDetailToIR(list.currentGrowth) : undefined,
  };
};

const featureSummaryToIR = (summary: import('./generated/public/commands').PlayerFeatureSummaryView | undefined) => (
  summary ? { ...(summary.tasks ? { tasks: taskSummaryToIR(summary.tasks) } : {}) } : undefined
);

const privateStateToIR = (state?: ProtoPrivateState): PrivateState | undefined => {
  if (!state) return undefined;
  const inventory: Record<string, number> = {};
  for (const item of state.inventory) {
    const key = inventoryKeyForItem(item.itemId);
    inventory[key] = exactNumber(item.count, `inventory.${key}`);
  }
  return {
    economyVersion: uint64String(state.economyVersion, 'economy_version'),
    wallet: exactNumber(state.coin, 'coin'),
    inventory,
    progression: { level: state.level, exp: uint64String(state.exp, 'exp') },
    codex: state.codexEntries.map((entry) => ({
      cropDefinitionId: entry.cropDefinitionId,
      firstHarvestedAt: exactNumber(entry.firstHarvestedAtMs, 'codex.first_harvested_at_ms'),
      sourceOperationId: entry.sourceOperationId,
    })),
    tasks: state.tasks.map((task) => ({
      taskDefinitionId: task.taskDefinitionId,
      progress: exactNumber(task.progress, 'task.progress'),
      target: exactNumber(task.target, 'task.target'),
      state: task.state === ProtoTaskState.CLAIMED ? 'CLAIMED' as const
        : task.state === ProtoTaskState.CLAIMABLE ? 'CLAIMABLE' as const : 'ACTIVE' as const,
      updatedAt: exactNumber(task.updatedAtMs, 'task.updated_at_ms'),
    })),
  };
};

const plotLockStateToIR = (lockState: PlotLockState): PlotLockStateIR => {
  switch (lockState) {
    case PlotLockState.PLOT_LOCK_STATE_UNLOCKED: return 'UNLOCKED';
    case PlotLockState.PLOT_LOCK_STATE_LOCKED: return 'LOCKED';
    case PlotLockState.PLOT_LOCK_STATE_UNSPECIFIED: return 'UNSPECIFIED';
    default: return 'UNSPECIFIED';
  }
};

const plotToIR = (plot: PlotView): PlotState => ({
  plotIndex: plot.plotIndex,
  plotId: `plot-${plot.plotIndex}`,
  plotVersion: uint64String(plot.plotVersion, 'plot_version'),
  lockState: plotLockStateToIR(plot.lockState),
  unlockOrder: plot.unlockOrder,
  requiredLevel: plot.requiredLevel,
  normalCoinPrice: uint64String(plot.normalCoinPrice, 'normal_coin_price'),
  unlockCandidate: plot.unlockCandidate,
  unlockRuleVersion: uint64String(plot.unlockRuleVersion, 'unlock_rule_version'),
  status: !plot.cropCycleId ? 'EMPTY' as const
    : plot.cropPhase === CropPhase.MATURE ? 'MATURE' as const : 'GROWING' as const,
  ...(plot.cropCycleId ? {
    cropCycleId: plot.cropCycleId,
    cropDefinitionId: plot.cropDefinitionId,
    matureAt: exactNumber(plot.matureAtMs, 'mature_at_ms'),
    harvestableCount: plot.harvestableCount,
    stolenTotal: plot.stolenTotal,
    ...(plot.watered ? { wateredAt: exactNumber(plot.plantAtMs, 'watered_at_ms') } : {}),
  } : {}),
});

const snapshotToIR = (snapshot: ProtoFarmSnapshot): FarmSnapshot => {
  const ownerId = uint64String(snapshot.ownerId, 'owner_id');
  return {
  ownerId,
  farmVersion: uint64String(snapshot.farmVersion, 'farm_version'),
  ownerPublicProfile: snapshot.ownerPublicProfile
    ? publicProfileToIR(snapshot.ownerPublicProfile, ownerId, 'owner_public_profile')
    : undefined,
  privateStateIncluded: false,
  state: {
    ownerId,
    wallet: 0,
    inventory: {},
    codex: [],
    tasks: [],
    plots: snapshot.plots.map(plotToIR),
  },
  };
};

const changeToIR = (change: ProtoFarmChange): FarmChange => ({
  ownerId: uint64String(change.ownerId, 'owner_id'),
  farmVersion: uint64String(change.farmVersion, 'farm_version'),
  partial: true,
  state: {
    ownerId: uint64String(change.ownerId, 'owner_id'),
    wallet: 0,
    inventory: {},
    plots: change.changedPlots.map(plotToIR),
  },
});

const rejoinReasonText = (reason: RejoinReason): Extract<ServerFrame, { kind: 'rejoin-required' }>['value']['reason'] => {
  switch (reason) {
    case RejoinReason.REJOIN_REASON_STREAM_GAP: return 'STREAM_GAP';
    case RejoinReason.REJOIN_REASON_DELIVERY_FAILED: return 'DELIVERY_FAILED';
    case RejoinReason.REJOIN_REASON_HOLDER_REBUILT: return 'HOLDER_REBUILT';
    case RejoinReason.REJOIN_REASON_SUBSCRIPTION_LEASE_LOST: return 'SUBSCRIPTION_LEASE_LOST';
    default: return 'UNSPECIFIED';
  }
};

/** Typed Payload 编码(command-service Journal 复用,保证存发一致)。 */
export const encodeCommandPayload = (payload: CommandPayload): Uint8Array => {
  switch (payload.type) {
    case 'JOIN_FARM':
      return JoinFarmRequest.encode({
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
      }).finish();
    case 'UNLOCK_PLOT':
      return UnlockPlotRequest.encode({
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
        plotId: payload.plotId,
        expectedPlotVersion: parseUint64(payload.expectedPlotVersion, 'expected_plot_version'),
        expectedUnlockRuleVersion: parseUint64(payload.expectedUnlockRuleVersion, 'expected_unlock_rule_version'),
      }).finish();
    case 'BUY':
      return BuyRequest.encode({
        offerId: payload.offerId,
        count: payload.count,
        expectedOfferVersion: parseUint64(payload.expectedOfferVersion, 'expected_offer_version'),
      }).finish();
    case 'SELL':
      return SellRequest.encode({
        itemId: payload.itemId,
        count: BigInt.asIntN(64, parseUint64(payload.count, 'count')),
      }).finish();
    case 'PLANT':
      return PlantRequest.encode({
        plotId: payload.plotId,
        cropDefinitionId: payload.cropDefinitionId,
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
        expectedPlotVersion: parseUint64(payload.expectedPlotVersion, 'expected_plot_version'),
        expectedCropRuleVersion: parseUint64(payload.expectedCropRuleVersion, 'expected_crop_rule_version'),
      }).finish();
    case 'HARVEST':
      return HarvestRequest.encode({
        plotId: payload.plotId,
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
        cropCycleId: payload.cropCycleId,
        expectedPlotVersion: parseUint64(payload.expectedPlotVersion, 'expected_plot_version'),
      }).finish();
    case 'CARE': {
      // 第 32 号合同:WATER 走 plot/cropCycle fence;PEST(REMOVE_PEST)额外要求
      // incident_id fence(Gate validator 已放行)。incident 视图未下发前 UI 只发
      // WATER;此处保留完整 wire 编码路径,对缺 fence 的 PEST fail fast(不伪造)。
      const careType = payload.careType ?? 'WATER';
      if (careType !== 'WATER' && careType !== 'PEST') {
        throw new Error(`unsupported care type ${String(careType)}; wire admits WATER or PEST`);
      }
      if (careType === 'PEST' && !payload.incidentId) {
        throw new Error('CARE_TYPE_PEST requires the crop cycle incident_id fence');
      }
      return CareRequest.encode({
        plotId: payload.plotId,
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
        cropCycleId: payload.cropCycleId,
        incidentId: payload.incidentId ?? '',
        careType: careType === 'PEST' ? CareType.CARE_TYPE_PEST : CareType.CARE_TYPE_WATER,
      }).finish();
    }
    case 'THEFT':
      return TheftRequest.encode({
        plotId: payload.plotId,
        targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id'),
        cropCycleId: payload.cropCycleId,
      }).finish();
    case 'GET_PLAYER_BOOTSTRAP':
      return GetPlayerBootstrapRequest.encode({}).finish();
    case 'GET_TASK_LIST':
      return GetTaskListRequest.encode({}).finish();
    case 'CLAIM_TASK':
      return ClaimTaskRequest.encode({ taskInstanceId: payload.taskInstanceId }).finish();
    case 'CONFIRM_FRIEND_VISIT':
      return ConfirmFriendVisitRequest.encode({ targetFarmId: parseUint64(payload.targetFarmId, 'target_farm_id') }).finish();
  }
};

const encodeFrame = (sequence: bigint, frameBody: Pick<FarmFrame, 'clientHello' | 'clientCommand' | 'control'>): Uint8Array =>
  FarmFrame.encode({
    protocolVersion: CANONICAL_PROTOCOL_VERSION,
    sequence,
    ...frameBody,
  }).finish();

const lookupText = (status: OperationLookupStatus): OperationLookup['lookupStatus'] => {
  switch (status) {
    case OperationLookupStatus.OPERATION_LOOKUP_NOT_FOUND: return 'NOT_FOUND';
    case OperationLookupStatus.OPERATION_LOOKUP_PROCESSING: return 'PROCESSING';
    case OperationLookupStatus.OPERATION_LOOKUP_COMPLETED: return 'COMPLETED';
    case OperationLookupStatus.OPERATION_LOOKUP_EXPIRED: return 'EXPIRED';
    default: throw new Error(`unspecified operation lookup status ${status}`);
  }
};

export class FinalProtocolAdapter implements FarmProtocolAdapter {
  readonly joinBarrier = 'accepted-snapshot-ready' as const;
  private sequence = 0n;
  private lastServerSequence = 0n;
  /** 兜底关联:信封 command_type 为 UNSPECIFIED 的旧实现才使用。 */
  private readonly pendingCommandTypes = new Map<string, CommandType>();

  resetConnection(): void {
    this.sequence = 0n;
    this.lastServerSequence = 0n;
    this.pendingCommandTypes.clear();
  }

  private nextSequence(): bigint {
    this.sequence += 1n;
    return this.sequence;
  }

  encodeClientFrame(frame: ClientFrame): Uint8Array {
    if (frame.kind === 'hello') {
      this.lastServerSequence = 0n;
      return encodeFrame(this.nextSequence(), {
        clientHello: ClientHello.create({
          connectionTicket: frame.value.connectionTicket,
          clientBuild: CLIENT_BUILD,
        }),
      });
    }
    if (frame.kind === 'recovery-complete') {
      return encodeFrame(this.nextSequence(), {
        control: ControlFrame.create({ clientRecoveryComplete: {} }),
      });
    }
    if (frame.kind === 'query') {
      const query = frame.value;
      const originalCommandType = commandTypeNumber(query.originalCommandType);
      const payload = GetOperationResultRequest.encode({
        originalCommandType,
        originalOperationId: query.originalOperationId,
        originalTarget: {
          targetFarmId: query.targetFarmId !== undefined
            ? parseUint64(query.targetFarmId, 'original_target.target_farm_id')
            : undefined,
          peerPlayerId: query.peerPlayerId !== undefined
            ? parseUint64(query.peerPlayerId, 'original_target.peer_player_id')
            : undefined,
          mailId: query.mailId,
        },
        serverOperationLocator: query.serverOperationLocator,
      }).finish();
      const sequence = this.nextSequence();
      return encodeFrame(sequence, {
        clientCommand: ClientCommandEnvelope.create({
          requestId: query.requestId,
          operationId: '',
          commandType: ProtoCommandType.GET_OPERATION_RESULT,
          payload,
        }),
      });
    }
    const command: ClientCommand = frame.value;
    const operationId = command.operationId ?? '';
    if (operationId) this.pendingCommandTypes.set(operationId, command.payload.type);
    return encodeFrame(this.nextSequence(), {
      clientCommand: ClientCommandEnvelope.create({
        requestId: command.requestId ?? operationId,
        operationId,
        commandType: commandTypeNumber(command.payload.type),
        payload: encodeCommandPayload(command.payload),
      }),
    });
  }

  decodeServerFrame(bytes: Uint8Array): ServerFrame {
    if (bytes.length === 0 || bytes.length > MAX_CANONICAL_FRAME_SIZE) throw new Error('FarmFrame exceeds max size');
    const frame = FarmFrame.decode(new BinaryReader(bytes));
    if (frame.protocolVersion !== CANONICAL_PROTOCOL_VERSION) {
      throw new Error(`unsupported FarmFrame protocol version ${frame.protocolVersion}`);
    }
    if (frame.sequence === 0n) throw new Error('invalid FarmFrame sequence');
    const expectedSequence = this.lastServerSequence + 1n;
    if (frame.sequence !== expectedSequence) {
      throw new Error(`server connection sequence gap: expected ${expectedSequence}, got ${frame.sequence}`);
    }
    this.lastServerSequence = frame.sequence;
    if (frame.clientHello || frame.clientCommand) {
      throw new Error('protocol direction violation: server sent a client-scoped frame');
    }
    if (frame.serverHello) return this.decodeServerHello(frame.serverHello);
    if (frame.commandResult) return this.decodeCommandResult(frame.commandResult);
    if (frame.serverPush) return this.decodeServerPush(frame.serverPush);
    if (frame.control) {
      const error = frame.control.connectionError;
      if (!error) throw new Error('protocol direction violation: server sent a client-scoped control');
      return {
        kind: 'connection-error',
        value: {
          errorCode: errorCodeName(error.errorCode),
          errorCodeNumber: error.errorCode,
          message: error.errorMessage,
        },
      };
    }
    throw new Error('FarmFrame has no frame_body');
  }

  private decodeServerHello(hello: ServerHello): ServerFrame {
    return {
      kind: 'hello',
      value: {
        playerId: uint64String(hello.playerId, 'player_id'),
        sessionEpoch: uint64String(hello.activeSessionEpoch, 'active_session_epoch'),
        serverTimeMs: exactNumber(hello.serverTimeMs, 'server_time_ms'),
        maxFrameBytes: hello.maxFrameBytes || undefined,
      },
    };
  }

  private decodeCommandResult(result: CommandResultEnvelope): ServerFrame {
    // 28 §2.2:信封 command_type 是权威;仅在 UNSPECIFIED 时兜底本地关联。
    const commandType = COMMAND_TYPE_BY_NUMBER.get(result.commandType)
      ?? this.pendingCommandTypes.get(result.operationId);
    const value: CommandResult = {
      operationId: result.operationId,
      requestId: result.requestId || undefined,
      commandType,
      status: STATUS_TEXT[result.status] ?? 'UNAVAILABLE',
      errorCode: !result.error || result.error.errorCode === ErrorCode.OK
        ? undefined
        : errorCodeName(result.error.errorCode),
      errorCodeNumber: !result.error || result.error.errorCode === ErrorCode.OK
        ? undefined
        : result.error.errorCode,
      message: result.error?.errorMessage || undefined,
      retryDirective: RETRY_DIRECTIVE_TEXT[result.retryDirective],
      retryAfterMs: result.retryAfterMs || undefined,
      serverOperationLocator: result.serverOperationLocator || undefined,
      featureUnavailable: result.error?.errorCode === ErrorCode.ERR_COMMAND_UNIMPLEMENTED || undefined,
    };
    if (result.status !== CommandStatus.PROCESSING && result.operationId) {
      this.pendingCommandTypes.delete(result.operationId);
    }
    if (result.join) {
      value.subscriptionId = result.join.subscriptionId || undefined;
      value.joinAttemptId = result.join.joinAttemptId || undefined;
      value.targetFarmId = uint64String(result.join.targetFarmId, 'join.target_farm_id');
    } else if (result.farm) {
      value.targetFarmId = uint64String(result.farm.targetFarmId, 'farm.target_farm_id');
      value.subscriptionId = result.farm.subscriptionId;
    }
    if (result.payload.length === 0) return { kind: 'result', value };
    // L2-PROTO-27:Join 的 ACCEPTED 只经 JoinResultContext 携带元信息;
    // result_payload 必须为空,携带即协议错误(fail closed)。
    if (commandType === 'JOIN_FARM') {
      throw new Error('JOIN_FARM ACCEPTED carried a forbidden result payload');
    }
    this.decodeTypedResultPayload(commandType, result.payload, value);
    return { kind: 'result', value };
  }

  private decodeTypedResultPayload(commandType: CommandType | undefined, payload: Uint8Array, value: CommandResult): void {
    switch (commandType) {
      case 'BUY': {
        const decoded = BuyResult.decode(payload);
        value.privateState = privateStateToIR(decoded.privateState);
        value.coinDelta = exactNumber(decoded.coinDelta, 'coin_delta');
        value.itemId = decoded.itemId;
        value.itemDelta = exactNumber(decoded.itemDelta, 'item_delta');
        return;
      }
      case 'SELL': {
        const decoded = SellResult.decode(payload);
        value.privateState = privateStateToIR(decoded.privateState);
        value.coinDelta = exactNumber(decoded.coinDelta, 'coin_delta');
        value.itemId = decoded.itemId;
        value.itemDelta = exactNumber(decoded.itemDelta, 'item_delta');
        return;
      }
      case 'PLANT': {
        const decoded = PlantResult.decode(payload);
        value.targetFarmId = uint64String(decoded.targetFarmId, 'target_farm_id');
        value.plotId = decoded.plotId;
        value.cropCycleId = decoded.cropCycleId || undefined;
        value.committedFarmVersion = uint64String(decoded.committedFarmVersion, 'committed_farm_version');
        return;
      }
      case 'UNLOCK_PLOT': {
        const decoded = UnlockPlotResult.decode(payload);
        value.farmId = uint64String(decoded.farmId, 'farm_id');
        value.targetFarmId = value.farmId;
        value.plotId = decoded.plotId;
        value.appliedUnlockRuleVersion = uint64String(
          decoded.appliedUnlockRuleVersion,
          'applied_unlock_rule_version',
        );
        value.paidNormalCoin = uint64String(decoded.paidNormalCoin, 'paid_normal_coin');
        value.normalCoinBalance = uint64String(decoded.normalCoinBalance, 'normal_coin_balance');
        value.committedPlotVersion = uint64String(decoded.committedPlotVersion, 'committed_plot_version');
        value.committedFarmVersion = uint64String(decoded.committedFarmVersion, 'committed_farm_version');
        return;
      }
      case 'HARVEST': {
        const decoded = HarvestResult.decode(payload);
        value.targetFarmId = uint64String(decoded.targetFarmId, 'target_farm_id');
        value.plotId = decoded.plotId;
        value.cropCycleId = decoded.cropCycleId || undefined;
        value.committedFarmVersion = uint64String(decoded.committedFarmVersion, 'committed_farm_version');
        value.harvestedQuantity = decoded.harvestedQuantity;
        value.compendiumNewlyUnlocked = decoded.compendiumNewlyUnlocked || undefined;
        value.unlockedCropDefinitionId = decoded.unlockedCropDefinitionId;
        value.compendiumRevision = uint64String(decoded.compendiumRevision, 'compendium_revision');
        return;
      }
      case 'CARE': {
        const decoded = CareResult.decode(payload);
        const targetFarmId = uint64String(decoded.targetFarmId, 'target_farm_id');
        const careType = decoded.careType === CareType.CARE_TYPE_WATER ? 'WATER'
          : decoded.careType === CareType.CARE_TYPE_PEST ? 'PEST' : null;
        const outcome = decoded.outcome === CareOutcome.CARE_OUTCOME_RESOLVED ? 'RESOLVED'
          : decoded.outcome === CareOutcome.CARE_OUTCOME_ALREADY_RESOLVED ? 'ALREADY_RESOLVED' : null;
        if (!careType || !outcome) throw new Error('CARE result has an unspecified care type/outcome');
        if (!decoded.cropCycleId || !decoded.careRewardOrderId || decoded.resolvedByPlayerId === 0n) {
          throw new Error('CARE result is missing frozen reward/profile fields');
        }
        const careResult: CareResultIR = {
          targetFarmId,
          plotId: decoded.plotId,
          cropCycleId: decoded.cropCycleId,
          committedFarmVersion: uint64String(decoded.committedFarmVersion, 'committed_farm_version'),
          incidentId: decoded.incidentId,
          careType,
          outcome,
          resolvedByPlayerId: uint64String(decoded.resolvedByPlayerId, 'resolved_by_player_id'),
          committedPlotVersion: uint64String(decoded.committedPlotVersion, 'committed_plot_version'),
          careRewardOrderId: decoded.careRewardOrderId,
          rewardObligationCreated: decoded.rewardObligationCreated,
        };
        value.targetFarmId = targetFarmId;
        value.plotId = decoded.plotId;
        value.cropCycleId = decoded.cropCycleId || undefined;
        value.committedFarmVersion = careResult.committedFarmVersion;
        value.careResult = careResult;
        return;
      }
      case 'THEFT': {
        const decoded = TheftResult.decode(payload);
        value.targetFarmId = uint64String(decoded.targetFarmId, 'target_farm_id');
        value.plotId = decoded.plotId;
        value.cropCycleId = decoded.cropCycleId || undefined;
        value.committedFarmVersion = uint64String(decoded.committedFarmVersion, 'committed_farm_version');
        value.settlementId = decoded.settlementId || undefined;
        return;
      }
      case 'GET_PLAYER_BOOTSTRAP': {
        const decoded = GetPlayerBootstrapResult.decode(payload);
        // 28 §3.7 严格解码(H5-VERTICAL-CORRECTNESS-A1 §3):SUCCEEDED 的
        // Bootstrap 必须携带全部私有经济字段,缺失 wallet/inventory 即
        // malformed success —— fail closed,绝不把缺失钱包/库存解释为 0。
        if (decoded.wallet === undefined || decoded.inventory === undefined) {
          throw new Error('GET_PLAYER_BOOTSTRAP SUCCEEDED without wallet/inventory (malformed success)');
        }
        value.playerStateRevision = uint64String(decoded.playerStateRevision, 'player_state_revision');
        value.trustedServerNowMs = exactNumber(decoded.trustedServerNowMs, 'trusted_server_now_ms');
        const privateState: PrivateState = {
          playerStateRevision: value.playerStateRevision,
          wallet: exactNumber(decoded.wallet.coin, 'wallet.coin'),
          inventory: Object.fromEntries(decoded.inventory.items.map((item) => [
            inventoryKeyForItem(item.itemId),
            exactNumber(item.count, 'inventory.item'),
          ])),
        };
        if (decoded.profile) {
          privateState.profile = {
            playerId: uint64String(decoded.profile.playerId, 'profile.player_id'),
            displayName: validDisplayName(decoded.profile.displayName, 'profile.display_name'),
          };
        }
        if (decoded.progression) {
          privateState.progression = {
            level: decoded.progression.level,
            exp: uint64String(decoded.progression.exp, 'progression.exp'),
          };
        }
        if (decoded.featureSummary) {
          privateState.featureSummary = featureSummaryToIR(decoded.featureSummary);
        }
        value.privateState = privateState;
        return;
      }
      case 'GET_OPERATION_RESULT': {
        const decoded = GetOperationResultResult.decode(payload);
        const lookup: OperationLookup = {
          originalCommandType: COMMAND_TYPE_BY_NUMBER.get(decoded.originalCommandType),
          originalOperationId: decoded.originalOperationId,
          lookupStatus: lookupText(decoded.lookupStatus),
          retryAfterMs: decoded.retryAfterMs || undefined,
          completedRetryDirective: decoded.completedRetryDirective
            ? RETRY_DIRECTIVE_TEXT[decoded.completedRetryDirective]
            : undefined,
        };
        if (decoded.completedOutcome) {
          const outcome = decoded.completedOutcome;
          const outcomeCommandType = COMMAND_TYPE_BY_NUMBER.get(outcome.originalCommandType);
          if (!outcomeCommandType || (COMMAND_TYPE_BY_NUMBER.get(decoded.originalCommandType) !== outcomeCommandType)) {
            throw new Error('GET_OPERATION_RESULT completed outcome command identity mismatch');
          }
          lookup.completedStatus = outcome.status === CommandStatus.SUCCEEDED ? 'SUCCEEDED'
            : outcome.status === CommandStatus.NO_CHANGE ? 'NO_CHANGE'
              : outcome.status === CommandStatus.REJECTED ? 'REJECTED'
                : outcome.status === CommandStatus.UNAVAILABLE ? 'UNAVAILABLE'
                  : undefined;
          if (!lookup.completedStatus) throw new Error('GET_OPERATION_RESULT completed outcome status is invalid');
          lookup.completedErrorCode = outcome.error && outcome.error.errorCode !== ErrorCode.OK
            ? errorCodeName(outcome.error.errorCode) : undefined;
          lookup.completedErrorMessage = outcome.error?.errorMessage || undefined;
          lookup.completedResultPayload = outcome.resultPayload.length > 0 ? outcome.resultPayload : undefined;
          lookup.completedTargetFarmId = outcome.farmContext
            ? uint64String(outcome.farmContext.targetFarmId, 'farm_context.target_farm_id')
            : undefined;
          lookup.originalCommandType = outcomeCommandType;
          if (lookup.completedResultPayload && lookup.originalCommandType) {
            const completedResult: CommandResult = {
              operationId: lookup.originalOperationId,
              commandType: lookup.originalCommandType,
              status: lookup.completedStatus,
              errorCode: lookup.completedErrorCode,
              message: lookup.completedErrorMessage,
              targetFarmId: lookup.completedTargetFarmId,
            };
            this.decodeTypedResultPayload(lookup.originalCommandType, lookup.completedResultPayload, completedResult);
            lookup.completedResult = completedResult;
            // Keep the CARE projection available without changing the lookup
            // envelope's own operation_id/command identity.
            if (completedResult.careResult) value.careResult = completedResult.careResult;
          }
        }
        value.operationLookup = lookup;
        return;
      }
      case 'GET_TASK_LIST': {
        const decoded = GetTaskListResult.decode(payload);
        value.taskList = taskListToIR(decoded);
        value.taskSummary = value.taskList.summary;
        return;
      }
      case 'CLAIM_TASK': {
        const decoded = ClaimTaskResult.decode(payload);
        if (!decoded.summary || !decoded.privateState || !decoded.claimedTaskInstanceId) {
          throw new Error('CLAIM_TASK result is missing authority state');
        }
        const summary = taskSummaryToIR(decoded.summary);
        const privateState = privateStateToIR(decoded.privateState);
        if (!privateState) throw new Error('CLAIM_TASK private state is malformed');
        privateState.featureSummary = { tasks: summary };
        value.privateState = privateState;
        value.taskSummary = summary;
        value.taskGrantedReward = taskRewardToIR(decoded.grantedReward);
        value.taskInstanceId = decoded.claimedTaskInstanceId;
        value.nextGrowthTask = decoded.nextGrowthTask ? taskProgressToIR(decoded.nextGrowthTask) : undefined;
        value.trustedServerNowMs = exactNumber(decoded.trustedServerNowMs, 'trusted_server_now_ms');
        return;
      }
      case 'CONFIRM_FRIEND_VISIT': {
        const decoded = ConfirmFriendVisitResult.decode(payload);
        if (!decoded.summary || !decoded.confirmedTaskInstanceId) {
          throw new Error('CONFIRM_FRIEND_VISIT result is missing authority summary');
        }
        value.taskSummary = taskSummaryToIR(decoded.summary);
        value.taskInstanceId = decoded.confirmedTaskInstanceId;
        value.trustedServerNowMs = exactNumber(decoded.trustedServerNowMs, 'trusted_server_now_ms');
        return;
      }
      default:
        // 未接线命令的占位 Result(全部 reserved):无可解析字段,忽略。
        return;
    }
  }

  private decodeServerPush(push: ServerPushEnvelope): ServerFrame {
    const farmContext = (): { ownerId: string; subscriptionId: string } => {
      const context = push.farmSubscription;
      if (!context || context.targetFarmId === 0n || !context.subscriptionId) {
        throw new Error('farm subscription push is missing its fence context');
      }
      return {
        ownerId: uint64String(context.targetFarmId, 'push.target_farm_id'),
        subscriptionId: context.subscriptionId,
      };
    };
    if (push.pushType === PushType.FARM_SNAPSHOT) {
      const context = farmContext();
      const snapshotPush = FarmSnapshotPush.decode(push.payload);
      if (!snapshotPush.joinAttemptId || !snapshotPush.snapshot) {
        throw new Error('FARM_SNAPSHOT is missing join attempt or snapshot');
      }
      if (snapshotPush.snapshotVersion !== snapshotPush.snapshot.farmVersion) {
        throw new Error('FARM_SNAPSHOT version does not match its payload');
      }
      const snapshot = snapshotToIR(snapshotPush.snapshot);
      if (snapshot.ownerId !== context.ownerId) {
        throw new Error('FARM_SNAPSHOT owner does not match push context');
      }
      return {
        kind: 'snapshot',
        value: {
          ...snapshot,
          subscriptionId: context.subscriptionId,
          joinAttemptId: snapshotPush.joinAttemptId,
        },
      };
    }
    if (push.pushType === PushType.FARM_DELTA_BATCH) {
      const context = farmContext();
      const batch = FarmDeltaBatchPush.decode(push.payload);
      if (!batch.joinAttemptId) throw new Error('FARM_DELTA_BATCH is missing join_attempt_id');
      return {
        kind: 'delta-batch',
        value: {
          ownerId: context.ownerId,
          subscriptionId: context.subscriptionId,
          joinAttemptId: batch.joinAttemptId,
          fromVersion: uint64String(batch.fromVersion, 'from_version'),
          throughVersion: uint64String(batch.throughVersion, 'through_version'),
          deltas: batch.deltas.map((delta) => ({
            ...changeToIR(delta),
            subscriptionId: context.subscriptionId,
            source: 'catch-up' as const,
          })),
        },
      };
    }
    if (push.pushType === PushType.SUBSCRIPTION_READY) {
      const context = farmContext();
      const ready = SubscriptionReadyPush.decode(push.payload);
      if (!ready.joinAttemptId) throw new Error('SUBSCRIPTION_READY is missing join_attempt_id');
      return {
        kind: 'subscription-ready',
        value: {
          ownerId: context.ownerId,
          subscriptionId: context.subscriptionId,
          joinAttemptId: ready.joinAttemptId,
          readyThroughVersion: uint64String(ready.readyThroughVersion, 'ready_through_version'),
        },
      };
    }
    if (push.pushType === PushType.FARM_CHANGE) {
      const context = farmContext();
      const payload = FarmChangePush.decode(push.payload);
      if (!payload.change) throw new Error('FARM_CHANGE payload is missing change');
      const change = changeToIR(payload.change);
      if (change.ownerId !== context.ownerId) {
        throw new Error('FARM_CHANGE owner does not match push context');
      }
      return {
        kind: 'change',
        value: { ...change, subscriptionId: context.subscriptionId, source: 'live' },
      };
    }
    if (push.pushType === PushType.FRIEND_ROSTER_CHANGED) {
      const changed = FriendRosterChanged.decode(push.payload);
      if (
        changed.mutation !== FriendMutationKind.ACCEPT_SHARE
        && changed.mutation !== FriendMutationKind.REVOKE_RELATION
      ) {
        throw new Error('invalid friend roster mutation');
      }
      return {
        kind: 'friend-roster-changed',
        value: {
          counterpartOwnerId: uint64String(changed.counterpartPlayerId, 'counterpart_player_id'),
          relationEpoch: uint64String(changed.relationEpoch, 'relation_epoch'),
          mutation: changed.mutation === FriendMutationKind.ACCEPT_SHARE ? 'ADDED' : 'REVOKED',
        },
      };
    }
    if (push.pushType === PushType.FRIEND_OPPORTUNITY_CHANGED) {
      const changed = FriendOpportunityChanged.decode(push.payload);
      return {
        kind: 'friend-opportunity-changed',
        value: {
          targetOwnerId: uint64String(changed.targetOwnerId, 'target_owner_id'),
          sourceFarmVersion: uint64String(changed.sourceFarmVersion, 'source_farm_version'),
          opportunityRevision: uint64String(changed.opportunityRevision, 'opportunity_revision'),
        },
      };
    }
    if (push.pushType === PushType.KICK) {
      // PushSpec:KICK → common.ConnectionError + SessionPushContext。
      const kick = push.payload.length > 0 ? ConnectionError.decode(push.payload) : undefined;
      return {
        kind: 'kick',
        value: {
          errorCode: kick ? errorCodeName(kick.errorCode) : 'ERR_SESSION_EXPIRED',
          message: kick?.errorMessage || '账号已在其他页面登录',
        },
      };
    }
    if (push.pushType === PushType.DRAIN_NOTICE) {
      const notice = DrainNotice.decode(push.payload);
      return {
        kind: 'drain',
        value: {
          drainId: notice.drainId,
          reconnectAfterMs: notice.reconnectAfterMs,
          deadlineMs: exactNumber(notice.deadlineMs, 'deadline_ms'),
        },
      };
    }
    if (push.pushType === PushType.REJOIN_REQUIRED) {
      const context = farmContext();
      const required = RejoinRequiredPush.decode(push.payload);
      return {
        kind: 'rejoin-required',
        value: {
          ownerId: context.ownerId,
          subscriptionId: context.subscriptionId,
          reason: rejoinReasonText(required.reason),
          ...(required.invalidFromVersion === undefined ? {} : {
            invalidFromVersion: uint64String(required.invalidFromVersion, 'invalid_from_version'),
          }),
        },
      };
    }
    throw new Error(`unsupported server push type ${push.pushType}`);
  }
}

export const finalProtocolAdapter: FarmProtocolAdapter = new FinalProtocolAdapter();
