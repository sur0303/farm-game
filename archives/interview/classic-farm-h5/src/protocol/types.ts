/**
 * H5 内部 IR(展示/状态层)。线网形状的唯一权威是 farm-game/api/proto
 * (经 generated/ 绑定消费);本文件只描述 WsClient/Store/App 之间传递的
 * 已解码语义,不定义任何线网字段号。
 *
 * 纪律:uint64 实体 ID 与单调版本在 JS 边界一律为十进制 string(Uint64Value
 * 输入允许 number/bigint,适配器统一归一);不透明 ID 是有界 string,只做相等
 * 性、长度与格式校验。
 */

export type Uint64Value = string | number | bigint;

/** 客户端可发送的公网命令(28 §6.1 33 目录中本客户端接线的子集)。 */
export type CommandType =
  | 'JOIN_FARM'
  | 'UNLOCK_PLOT'
  | 'BUY'
  | 'SELL'
  | 'PLANT'
  | 'HARVEST'
  | 'CARE'
  | 'THEFT'
  | 'GET_PLAYER_BOOTSTRAP'
  | 'GET_OPERATION_RESULT'
  | 'GET_TASK_LIST'
  | 'CLAIM_TASK'
  | 'CONFIRM_FRIEND_VISIT';

export type ResultStatus =
  | 'SUCCEEDED'
  | 'PROCESSING'
  | 'NO_CHANGE'
  | 'REJECTED'
  | 'UNAVAILABLE'
  | 'ACCEPTED';

export type RetryDirective =
  | 'NONE'
  | 'RETRY_SAME_OPERATION'
  | 'POLL_OPERATION_RESULT'
  | 'FRESH_JOIN'
  | 'RECONNECT'
  | 'REAUTHENTICATE'
  | 'FRESH_JOIN_THEN_RETRY'
  | 'REFRESH_AUTHORITY_STATE';

/** Typed Payload 判别联合:target/expected 版本全部进入各自 Payload(28 §3)。 */
export type CommandPayload =
  | { type: 'JOIN_FARM'; targetFarmId: Uint64Value }
  | {
      type: 'UNLOCK_PLOT';
      targetFarmId: Uint64Value;
      plotId: number;
      expectedPlotVersion: Uint64Value;
      expectedUnlockRuleVersion: Uint64Value;
    }
  | { type: 'BUY'; offerId: number; count: number; expectedOfferVersion: Uint64Value }
  | { type: 'SELL'; itemId: number; count: Uint64Value }
  | {
      type: 'PLANT';
      plotId: number;
      cropDefinitionId: number;
      targetFarmId: Uint64Value;
      expectedPlotVersion: Uint64Value;
      expectedCropRuleVersion: Uint64Value;
    }
  | {
      type: 'HARVEST';
      plotId: number;
      targetFarmId: Uint64Value;
      cropCycleId: string;
      expectedPlotVersion: Uint64Value;
    }
  | {
      type: 'CARE';
      plotId: number;
      targetFarmId: Uint64Value;
      cropCycleId: string;
      /**
       * 公网护理类型。WATER 已端到端接线;PEST(REMOVE_PEST)的 wire 已随第 32 号
       * 合同冻结且 Gate validator 已放行,但 FarmView 尚未下发 Incident 视图(view
       * 侧缺口),客户端无从获得 incident_id fence,因此 UI 目前只产生 WATER;拿到
       * 权威 incident 数据前不得伪造。
       */
      careType?: 'WATER' | 'PEST';
      /** 事件类护理的目标 incident_id fence;WATER 恒为空串。 */
      incidentId?: string;
    }
  | { type: 'THEFT'; plotId: number; targetFarmId: Uint64Value; cropCycleId: string }
  | { type: 'GET_PLAYER_BOOTSTRAP' }
  | { type: 'GET_TASK_LIST' }
  | { type: 'CLAIM_TASK'; taskInstanceId: string }
  | { type: 'CONFIRM_FRIEND_VISIT'; targetFarmId: Uint64Value };

export const commandTypeOfPayload = (payload: CommandPayload): CommandType => payload.type;

export interface ClientHello {
  connectionTicket: string;
}

export interface ClientCommand {
  operationId?: string;
  requestId?: string;
  payload: CommandPayload;
}

/** GET_OPERATION_RESULT 查询(28 §3.8):原命令身份 + 原业务目标/Locator。 */
export interface QueryOperation {
  requestId: string;
  originalCommandType: Exclude<CommandType, 'GET_OPERATION_RESULT'>;
  originalOperationId: string;
  targetFarmId?: Uint64Value;
  peerPlayerId?: Uint64Value;
  mailId?: string;
  serverOperationLocator?: string;
}

export type ClientFrame =
  | { kind: 'hello'; value: ClientHello }
  | { kind: 'recovery-complete'; value: Record<string, never> }
  | { kind: 'command'; value: ClientCommand }
  | { kind: 'query'; value: QueryOperation };

export interface ServerHello {
  playerId: string;
  sessionEpoch: string;
  serverTimeMs: number;
  maxFrameBytes?: number;
}

/** GET_OPERATION_RESULT 的下行结构(28 §3.8)。 */
export interface OperationLookup {
  /** 被查询的原命令身份(来自 GetOperationResultResult,非信封)。 */
  originalCommandType?: CommandType;
  originalOperationId: string;
  lookupStatus: 'NOT_FOUND' | 'PROCESSING' | 'COMPLETED' | 'EXPIRED';
  /** Business terminals plus honest control-terminal UNAVAILABLE. */
  completedStatus?: 'SUCCEEDED' | 'REJECTED' | 'NO_CHANGE' | 'UNAVAILABLE';
  completedErrorCode?: string;
  completedErrorMessage?: string;
  completedRetryDirective?: RetryDirective;
  /** 服务端重建指令;H5 不自行把 ErrorCode 映射成恢复动作。 */
  retryAfterMs?: number;
  /** StoredOperationOutcome.result_payload 原 bytes,由 Journal 按原命令类型处置。 */
  completedResultPayload?: Uint8Array;
  completedTargetFarmId?: string;
  /** The original typed result decoded with the original command identity. */
  completedResult?: CommandResult;
}

export interface CommandResult {
  operationId: string;
  requestId?: string;
  commandType?: CommandType;
  status: ResultStatus;
  errorCode?: string;
  errorCodeNumber?: number;
  message?: string;
  retryDirective?: RetryDirective;
  retryAfterMs?: number;
  /** 服务端签发的不透明定位提示;客户端不解析,查询时原样回带。 */
  serverOperationLocator?: string;
  /** ERR_COMMAND_UNIMPLEMENTED:合同已注册但业务 Handler 尚未接线。 */
  featureUnavailable?: boolean;
  privateState?: PrivateState;
  playerStateRevision?: Uint64Value;
  trustedServerNowMs?: number;
  /** ── 农场写命令最小 Typed Result(28 §2.2 约束 7)── */
  targetFarmId?: string;
  /** UNLOCK_PLOT carries the server's farm_id explicitly in its typed result. */
  farmId?: string;
  plotId?: number;
  cropCycleId?: string;
  committedFarmVersion?: Uint64Value;
  appliedUnlockRuleVersion?: Uint64Value;
  paidNormalCoin?: Uint64Value;
  /** Linearized reservation balance; never apply this locally to PrivateState. */
  normalCoinBalance?: Uint64Value;
  committedPlotVersion?: Uint64Value;
  harvestedQuantity?: number;
  settlementId?: string;
  compendiumNewlyUnlocked?: boolean;
  unlockedCropDefinitionId?: number;
  compendiumRevision?: Uint64Value;
  coinDelta?: number;
  itemId?: number;
  itemDelta?: number;
  /** ── Join 元信息(仅 JoinResultContext,唯一来源)── */
  subscriptionId?: string;
  joinAttemptId?: string;
  /** ── GET_OPERATION_RESULT ── */
  operationLookup?: OperationLookup;
  /** CARE's complete typed result, including settlement/reward state. */
  careResult?: CareResult;
  taskList?: TaskListView;
  taskSummary?: TaskSummary;
  taskGrantedReward?: TaskRewardView;
  taskInstanceId?: string;
  nextGrowthTask?: TaskProgressEntry;
}

export interface PublicProfile {
  playerId: string;
  displayName: string;
  avatarId: number;
  level: number;
}

export interface FarmSnapshot {
  ownerId: string;
  farmVersion: Uint64Value;
  state: FarmState;
  /** Subscription fence from the FARM_SNAPSHOT push envelope. */
  subscriptionId?: string;
  /** Gate-issued Join attempt fence from FarmSnapshotPush. */
  joinAttemptId?: string;
  /** False for public Farm pushes that intentionally omit private economy state. */
  privateStateIncluded?: boolean;
  /** Backend authoritative profile; never derive this from ownerId. */
  ownerPublicProfile?: PublicProfile;
}

export interface FarmChange {
  ownerId: string;
  farmVersion: Uint64Value;
  state: FarmState;
  /** Canonical FarmChange carries only changed_plots, not a full snapshot. */
  partial?: boolean;
  /** Subscription fence from the FARM_CHANGE push envelope. */
  subscriptionId?: string;
  /** Catch-up changes suppress historical animation and toast side effects. */
  source?: 'catch-up' | 'live';
}

export interface FarmDeltaBatch {
  ownerId: string;
  subscriptionId: string;
  joinAttemptId: string;
  fromVersion: Uint64Value;
  throughVersion: Uint64Value;
  deltas: FarmChange[];
}

export interface SubscriptionReady {
  ownerId: string;
  subscriptionId: string;
  joinAttemptId: string;
  readyThroughVersion: Uint64Value;
}

export interface DrainNotice {
  drainId: string;
  reconnectAfterMs: number;
  deadlineMs: number;
}

export interface RejoinRequired {
  ownerId: string;
  subscriptionId: string;
  reason: 'STREAM_GAP' | 'DELIVERY_FAILED' | 'HOLDER_REBUILT' | 'SUBSCRIPTION_LEASE_LOST' | 'UNSPECIFIED';
  invalidFromVersion?: Uint64Value;
}

export interface FriendRosterChange {
  counterpartOwnerId: string;
  relationEpoch: string;
  mutation: 'ADDED' | 'REVOKED';
}

export interface FriendOpportunityHint {
  targetOwnerId: string;
  sourceFarmVersion: string;
  opportunityRevision: string;
}

export interface SessionKick {
  errorCode: string;
  message: string;
}

/** 连接级协议错误(control.connection_error,S→C 唯一服务端控制帧)。 */
export interface ConnectionError {
  errorCode: string;
  errorCodeNumber: number;
  message: string;
}

export type ServerFrame =
  | { kind: 'hello'; value: ServerHello }
  | { kind: 'result'; value: CommandResult }
  | { kind: 'snapshot'; value: FarmSnapshot }
  | { kind: 'change'; value: FarmChange }
  | { kind: 'delta-batch'; value: FarmDeltaBatch }
  | { kind: 'subscription-ready'; value: SubscriptionReady }
  | { kind: 'friend-roster-changed'; value: FriendRosterChange }
  | { kind: 'friend-opportunity-changed'; value: FriendOpportunityHint }
  | { kind: 'kick'; value: SessionKick }
  | { kind: 'drain'; value: DrainNotice }
  | { kind: 'rejoin-required'; value: RejoinRequired }
  | { kind: 'connection-error'; value: ConnectionError };

export interface PlotState {
  /** Exact 0-based PlotView index echoed by the server; never inferred from order. */
  plotIndex: number;
  /** Server-authoritative lock fact. Missing/unknown values are never plantable. */
  lockState: PlotLockState;
  /** Server-provided unlock offer/rule projection; never derive these locally. */
  unlockOrder: number;
  requiredLevel: number;
  normalCoinPrice: Uint64Value;
  unlockCandidate: boolean;
  unlockRuleVersion: Uint64Value;
  /** Stable DOM key derived only by the protocol adapter from plot_index. */
  plotId: string;
  plotVersion: Uint64Value;
  /** 服务端 CropPhase 直读:不再由客户端按成熟时间推导。 */
  status: 'EMPTY' | 'GROWING' | 'MATURE';
  cropCycleId?: string;
  cropDefinitionId?: number;
  matureAt?: number;
  wateredAt?: number;
  /** Server-authoritative remaining produce and cumulative theft counters. */
  harvestableCount?: number;
  stolenTotal?: number;
}

export type PlotLockState = 'UNSPECIFIED' | 'UNLOCKED' | 'LOCKED';

/** GET_PLAYER_BOOTSTRAP 的玩家私有资料(28 §3.7 profile)。 */
export interface BootstrapProfile {
  playerId: string;
  displayName: string;
}

export type TaskProgressState = 'ACTIVE' | 'CLAIMABLE' | 'CLAIMED';

export interface TaskProgressEntry {
  instanceId: string;
  taskId: string;
  progress: number;
  target: number;
  state: TaskProgressState;
}

export interface TaskSummary {
  taskStateVersion: Uint64Value;
  claimableCount: number;
  dailyPeriodId: string;
  dailyTasks: TaskProgressEntry[];
  currentGrowth?: TaskProgressEntry;
}

export interface TaskRewardView {
  coin: number;
  exp: number;
  items: Array<{ itemId: number; count: number }>;
  petUnlock?: { petId: number; petConfigVersion: Uint64Value; grantDefinitionVersion: Uint64Value };
}

export interface TaskDetailView {
  progress: TaskProgressEntry;
  category: number;
  kind: number;
  definitionVersion: Uint64Value;
  periodId: string;
  configVersion: Uint64Value;
  reward: TaskRewardView;
}

export interface TaskListView {
  summary: TaskSummary;
  nextResetAtMs: number;
  dailyConfigVersion: Uint64Value;
  dailyTasks: TaskDetailView[];
  currentGrowth?: TaskDetailView;
}

/** GET_PLAYER_BOOTSTRAP 的成长摘要(28 §3.7 progression)。 */
export interface BootstrapProgression {
  level: number;
  exp: Uint64Value;
}

export interface PrivateState {
  /** Monotonic PlayerActor economy version used to reject stale recovery data. */
  economyVersion?: Uint64Value;
  /** GET_PLAYER_BOOTSTRAP 的 player_state_revision(28 §3.7 约束 4 单调应用)。 */
  playerStateRevision?: Uint64Value;
  wallet: number;
  inventory: Record<string, number>;
  codex?: CodexUnlock[];
  tasks?: TaskState[];
  /** GET_PLAYER_BOOTSTRAP 私有资料(28 §3.7):顶部昵称的权威来源。 */
  profile?: BootstrapProfile;
  /** GET_PLAYER_BOOTSTRAP 成长摘要(28 §3.7):等级的权威来源。 */
  progression?: BootstrapProgression;
  /** GET_PLAYER_BOOTSTRAP feature_summary.tasks (field 31). */
  featureSummary?: { tasks?: TaskSummary };
}

export interface CodexUnlock {
  cropDefinitionId: number;
  firstHarvestedAt: number;
  sourceOperationId: string;
}

export interface TaskState {
  taskDefinitionId: number;
  progress: number;
  target: number;
  state: 'ACTIVE' | 'CLAIMABLE' | 'CLAIMED';
  updatedAt: number;
  name?: string;
  description?: string;
  rewardCoin?: number;
  rewardExp?: number;
}

export interface FarmState extends PrivateState {
  ownerId: string;
  plots: PlotState[];
}

/** Complete CARE result, fields 5..11 included. */
export interface CareResult {
  targetFarmId: string;
  plotId: number;
  cropCycleId: string;
  committedFarmVersion: string;
  incidentId: string;
  careType: 'WATER' | 'PEST';
  outcome: 'RESOLVED' | 'ALREADY_RESOLVED';
  resolvedByPlayerId: string;
  committedPlotVersion: string;
  careRewardOrderId: string;
  rewardObligationCreated: boolean;
}
