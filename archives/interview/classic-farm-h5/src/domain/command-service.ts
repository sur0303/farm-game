import type { ConnectionTicket } from '../auth/types';
import type {
  OperationRootPolicy,
  PendingOperationRecord,
  OperationJournal,
} from '../persistence/operation-journal';
import { JOURNAL_PAYLOAD_SCHEMA_VERSION } from '../persistence/operation-journal';
import type { CommandPayload, CommandResult, RetryDirective } from '../protocol/types';
import { canonicalPayloadHash } from '../protocol/canonical-payload';
import { encodeCommandPayload } from '../protocol/final-protocol-adapter';
import { FarmStore } from '../state/farm-store';
import { WsClient } from '../network/ws-client';
import { createUuid } from '../platform/random-id';

export type PendingStatus = 'Created' | 'Sending' | 'AwaitingResult' | 'Processing' | 'Resolved' | 'Unknown';
export type TerminalOperationStatus = 'SUCCEEDED' | 'NO_CHANGE' | 'REJECTED' | 'UNAVAILABLE';

export type WaitForTerminalOutcome = 'terminal' | 'unknown' | 'timeout' | 'disconnected';

export interface WaitForTerminalResult {
  operationId: string;
  outcome: WaitForTerminalOutcome;
  status?: TerminalOperationStatus;
}

export interface PendingOperation {
  operationId: string;
  commandType: CommandPayload['type'];
  targetFarmId?: string;
  plotId?: number;
  status: PendingStatus;
  terminalStatus?: TerminalOperationStatus;
  message?: string;
  /** 合同已注册但后端尚未接线(ERR_COMMAND_UNIMPLEMENTED)。 */
  featureUnavailable?: boolean;
}

export interface CommandServiceEvents {
  /** REAUTHENTICATE:当前 Cookie/Session 不可再用,必须回登录流程。 */
  reauthenticate?: (message: string) => void;
  /** Called after journaling and immediately before a real command is sent. */
  beforeSend?: (operationId: string, payload: CommandPayload) => void;
}

export interface CommandServiceOptions {
  resultPollBaseDelayMs?: number;
  resultPollMaxDelayMs?: number;
  /** 同一 operation 自动重试(RETRY_SAME_OPERATION)上限,超出转 Unknown。 */
  maxSameOperationRetries?: number;
  random?: () => number;
  events?: CommandServiceEvents;
}

const defaultOptions: Required<Omit<CommandServiceOptions, 'events'>> = {
  resultPollBaseDelayMs: 500,
  resultPollMaxDelayMs: 5_000,
  maxSameOperationRetries: 3,
  random: Math.random,
};

const base64Encode = (bytes: Uint8Array): string => {
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
};

/** 命令 → Journal Root Policy 与原业务目标(28 §6.1 OperationRootPolicy)。 */
const rootOf = (payload: CommandPayload): {
  policy: OperationRootPolicy;
  targetFarmId?: string;
  plotId?: number;
  peerPlayerId?: string;
  mailId?: string;
} => {
  switch (payload.type) {
    case 'UNLOCK_PLOT':
    case 'PLANT':
    case 'HARVEST':
      return { policy: 'FARM', targetFarmId: String(payload.targetFarmId), plotId: payload.plotId };
    case 'CARE':
    case 'THEFT':
    case 'CONFIRM_FRIEND_VISIT':
      return { policy: 'FRIEND_PAIR', targetFarmId: String(payload.targetFarmId) };
    default:
      return { policy: 'PLAYER' };
  }
};

/** 可恢复写命令(IdempotencyPolicy=REQUIRED);查询/Join 不进入 Journal。 */
const isRecoverableWrite = (payload: CommandPayload): boolean =>
  payload.type === 'BUY' || payload.type === 'SELL' || payload.type === 'UNLOCK_PLOT'
  || payload.type === 'PLANT' || payload.type === 'HARVEST'
  || payload.type === 'CARE' || payload.type === 'THEFT'
  || payload.type === 'CLAIM_TASK' || payload.type === 'CONFIRM_FRIEND_VISIT';

export class CommandService {
  private readonly pending = new Map<string, PendingOperation>();
  private readonly payloads = new Map<string, CommandPayload>();
  private readonly listeners = new Set<() => void>();
  private readonly resultPollTimers = new Map<string, number>();
  private readonly resultPollAttempts = new Map<string, number>();
  private readonly sameOperationRetries = new Map<string, number>();
  private readonly replayTimers = new Map<string, number>();
  private readonly options: Required<Omit<CommandServiceOptions, 'events'>>;
  private readonly events: CommandServiceEvents;
  private ticket: ConnectionTicket | null = null;

  constructor(
    private readonly ws: WsClient,
    private readonly store: FarmStore,
    private readonly journal: OperationJournal,
    private readonly environment: string,
    options: CommandServiceOptions = {},
  ) {
    const { events, ...rest } = options;
    this.options = { ...defaultOptions, ...rest };
    this.events = events ?? {};
  }

  setConnectionTicket(ticket: ConnectionTicket): void {
    this.ticket = ticket;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getPending(): PendingOperation[] {
    return [...this.pending.values()];
  }

  /**
   * Wait for this exact operation to reach a durable terminal state. This is
   * deliberately separate from execute(): enqueueing a frame is not a
   * business outcome. Timeout/disconnect stops result polling for this caller,
   * marks the operation Unknown, and leaves its Journal record intact.
   */
  async waitForTerminal(operationId: string, timeoutMs: number): Promise<WaitForTerminalResult> {
    const operation = this.pending.get(operationId);
    if (!operation) return { operationId, outcome: 'unknown' };
    if (operation.status === 'Resolved') {
      return {
        operationId,
        outcome: 'terminal',
        status: operation.terminalStatus ?? 'UNAVAILABLE',
      };
    }
    if (operation.status === 'Unknown') {
      await this.stopUnresolved(operationId, '操作结果未知，批量操作已停止');
      return { operationId, outcome: 'unknown' };
    }
    if (this.ws.status !== 'InFarm') {
      await this.stopUnresolved(operationId, '连接已离开农场，批量操作已停止');
      return { operationId, outcome: 'disconnected' };
    }

    const boundedTimeout = Number.isFinite(timeoutMs) ? Math.max(1, Math.round(timeoutMs)) : 1;
    return new Promise<WaitForTerminalResult>((resolve) => {
      let finished = false;
      let pollTimer: number | undefined;
      let timeoutTimer: number | undefined;
      const finish = (result: WaitForTerminalResult): void => {
        if (finished) return;
        finished = true;
        if (pollTimer !== undefined) window.clearTimeout(pollTimer);
        if (timeoutTimer !== undefined) window.clearTimeout(timeoutTimer);
        resolve(result);
      };
      const check = (): void => {
        if (finished) return;
        const current = this.pending.get(operationId);
        if (!current) {
          finish({ operationId, outcome: 'unknown' });
          return;
        }
        if (current.status === 'Resolved') {
          finish({
            operationId,
            outcome: 'terminal',
            status: current.terminalStatus ?? 'UNAVAILABLE',
          });
          return;
        }
        if (current.status === 'Unknown') {
          void this.stopUnresolved(operationId, '操作结果未知，批量操作已停止').catch(() => undefined);
          finish({ operationId, outcome: 'unknown' });
          return;
        }
        if (this.ws.status !== 'InFarm') {
          void this.stopUnresolved(operationId, '连接已离开农场，批量操作已停止').catch(() => undefined);
          finish({ operationId, outcome: 'disconnected' });
          return;
        }
        pollTimer = window.setTimeout(check, 25);
      };
      timeoutTimer = window.setTimeout(() => {
        void this.stopUnresolved(operationId, '操作终态等待超时，批量操作已停止').catch(() => undefined);
        finish({ operationId, outcome: 'timeout' });
      }, boundedTimeout);
      pollTimer = window.setTimeout(check, 0);
    });
  }

  /**
   * 执行业务命令。可恢复写命令先落 Journal(含原 Typed Payload 与 Canonical
   * Hash)再上线;Journal 容量 Fail Closed 时拒绝发送(不先发网络再记录)。
   */
  async execute(payload: CommandPayload): Promise<string> {
    if (!this.ticket) throw new Error('cannot execute before authentication context is available');
    const operationId = createUuid();
    const recoverable = isRecoverableWrite(payload);
    let record: PendingOperationRecord | undefined;
    if (recoverable) {
      const encoded = encodeCommandPayload(payload);
      const root = rootOf(payload);
      record = {
        environment: this.environment,
        playerId: this.ticket.playerId,
        operationId,
        commandType: payload.type,
        operationRootPolicy: root.policy,
        targetFarmId: root.targetFarmId,
        plotId: root.plotId,
        peerPlayerId: root.peerPlayerId,
        mailId: root.mailId,
        payloadSchemaVersion: JOURNAL_PAYLOAD_SCHEMA_VERSION,
        payloadEncoding: 'protobuf-base64',
        originalTypedPayload: base64Encode(encoded),
        payloadIr: JSON.stringify(payload, uint64Replacer),
        canonicalPayloadHash: await canonicalPayloadHash(payload),
        createdAtMs: Date.now(),
        lastStatus: 'Created',
        retryCount: 0,
      };
      // JournalCapacityError 在此抛出:命令从未上线,可安全改参数后重试。
      await this.journal.put(record);
    }
    this.pending.set(operationId, {
      operationId,
      commandType: payload.type,
      targetFarmId: rootOf(payload).targetFarmId,
      plotId: rootOf(payload).plotId,
      status: 'Created',
    });
    this.payloads.set(operationId, payload);
    this.notify();
    try {
      this.pending.get(operationId)!.status = 'Sending';
      this.notify();
      // Persist the conservative "possibly admitted" state before sending.
      // BoundedOperationJournal.put() performs an async capacity read; doing
      // this after send lets a fast terminal result remove the record first,
      // then the delayed put resurrect a completed operation on reconnect.
      // A crash between this write and send is safe: recovery queries the same
      // operation_id and only replays it under the existing idempotency policy.
      if (record) {
        record = { ...record, lastStatus: 'AwaitingResult' };
        await this.journal.put(record);
      }
      this.events.beforeSend?.(operationId, payload);
      this.ws.sendCommand({ operationId, requestId: createUuid(), payload });
      this.pending.get(operationId)!.status = 'AwaitingResult';
      this.notify();
    } catch (error) {
      this.pending.get(operationId)!.status = 'Unknown';
      if (record) await this.journal.put({ ...record, lastStatus: 'Unknown', retryCount: 1 });
      this.notify();
      throw error;
    }
    return operationId;
  }

  /**
   * 恢复未决操作(28 §3.9 恢复矩阵):
   * - 从未上线(lastStatus=Created)的记录:同 operation_id + 原 Typed Payload
   *   重放(可证明未 Accepted;服务端幂等保证并发重发只生效一次);
   * - 已可能 ADMITTED 的记录:只按原 operation_id 查询,不换 ID、不盲放;
   * - payloadSchemaVersion 未知:Fail Closed,只查询不重放;
   * - 查询命令后端未接线(1007):记录保留,首轮视为未建立,保持恢复屏障。
   */
  async recoverPending(): Promise<void> {
    if (!this.ticket) return;
    const scope = `${this.environment}:${this.ticket.playerId}`;
    const records = await this.journal.list(scope);
    const failed: string[] = [];
    for (const record of records) {
      const operation: PendingOperation = {
        operationId: record.operationId,
        commandType: record.commandType as CommandPayload['type'],
        targetFarmId: record.targetFarmId,
        plotId: record.plotId ?? plotIdFromJournal(record),
        status: 'Unknown',
      };
      this.pending.set(record.operationId, operation);
      this.clearResultPoll(record.operationId);
      if (record.lastStatus === 'Created' && record.payloadSchemaVersion === JOURNAL_PAYLOAD_SCHEMA_VERSION && record.payloadIr) {
        // 从未上线:InFarm 打开后重放(恢复屏障先于写准入,不能此刻发送)。
        this.scheduleReplay(record.operationId);
        continue;
      }
      if (!this.queryAndContinuePolling(operation, record)) {
        failed.push(record.operationId);
      }
    }
    this.notify();
    if (failed.length > 0) {
      throw new Error(`未决操作首轮查询未能入队(${failed.length} 条),保持恢复屏障并重试`);
    }
  }

  async handleResult(result: CommandResult): Promise<void> {
    const operation = this.pending.get(result.operationId);
    if (result.serverOperationLocator) {
      await this.updateJournal(result.operationId, { serverOperationLocator: result.serverOperationLocator });
    }
    // GET_OPERATION_RESULT 的下行不是业务命令结果,按 Journal 恢复矩阵处置。
    if (result.commandType === 'GET_OPERATION_RESULT' || result.operationLookup) {
      await this.handleLookupResult(result);
      return;
    }
    if (!operation) return;
    operation.message = result.message;
    if (result.featureUnavailable) {
      // 合同已注册但后端未接线:明确不可用,终态清理,不自动重试。
      operation.status = 'Resolved';
      operation.terminalStatus = 'UNAVAILABLE';
      operation.featureUnavailable = true;
      operation.message = result.message ?? '该功能尚未开放,请稍后再试';
      this.clearResultPoll(result.operationId);
      await this.removeJournal(result.operationId);
      this.deferPendingRemoval(result.operationId);
      this.notify();
      return;
    }
    if (result.privateState) this.store.applyPrivateState(result.privateState);
    const directive: RetryDirective = result.retryDirective ?? 'NONE';
    if (result.status === 'PROCESSING') {
      operation.status = 'Processing';
      await this.updateJournal(result.operationId, { lastStatus: 'Processing' });
      this.scheduleResultPoll(operation);
    } else if (result.status === 'SUCCEEDED' || result.status === 'NO_CHANGE') {
      await this.resolveTerminal(result.operationId, result.status);
    } else if (result.status === 'REJECTED' && directive === 'RETRY_SAME_OPERATION') {
      // 确认未进入业务执行边界:同 operation_id + 原 payload 有界重试。
      await this.retrySameOperation(result.operationId);
    } else if (result.status === 'REJECTED' && directive === 'FRESH_JOIN_THEN_RETRY') {
      this.freshJoinThenRetry(result.operationId, result.targetFarmId ?? operation.targetFarmId);
    } else if (result.status === 'REJECTED' && directive === 'FRESH_JOIN') {
      const ownerId = result.targetFarmId ?? operation.targetFarmId ?? this.ws.activeJoinOwnerId;
      if (ownerId) this.ws.retryJoinFarm(ownerId);
      operation.status = 'Unknown';
      await this.updateJournal(result.operationId, { lastStatus: 'Unknown' });
    } else if (result.status === 'REJECTED') {
      // REJECTED + NONE / REFRESH_AUTHORITY_STATE:确定性终态,不自动重试。
      if (directive === 'REFRESH_AUTHORITY_STATE') this.refreshAuthorityState();
      await this.resolveTerminal(result.operationId, 'REJECTED');
    } else {
      // UNAVAILABLE / ACCEPTED(非 Join)/ 其他:结果未知,按原 operation 查询。
      if (directive === 'RECONNECT') this.ws.retryNow();
      else if (directive === 'REAUTHENTICATE') this.events.reauthenticate?.(result.message ?? '登录状态已失效,请重新登录');
      else if (directive === 'REFRESH_AUTHORITY_STATE') this.refreshAuthorityState();
      operation.status = 'Unknown';
      await this.updateJournal(result.operationId, { lastStatus: 'Unknown' });
      this.scheduleResultPoll(operation, result.retryAfterMs);
    }
    this.notify();
  }

  private async handleLookupResult(result: CommandResult): Promise<void> {
    const lookup = result.operationLookup;
    // 查询命令本身被拒(典型:1007 feature-disabled):查询通道不可用是服务端
    // 明确终态。停止所有结果轮询(不再制造无效流量),未决记录全部保留为
    // Unknown,不淘汰、不盲放;人工 replayOperation 仍是唯一重放接缝。
    if (!lookup) {
      if (result.featureUnavailable) {
        for (const [operationId, operation] of this.pending) {
          if (operation.status === 'Resolved') continue;
          this.clearResultPoll(operationId);
          operation.status = 'Unknown';
          operation.message = '服务端结果查询尚未开放;未决操作已保留,可在观察农场状态后手动重试';
        }
        this.notify();
      }
      return;
    }
    // 查询下行按 typed result 的 original_operation_id 关联未决记录(信封
    // operation_id 属于查询命令自身)。
    const operation = this.pending.get(lookup.originalOperationId);
    if (!operation) return;
    const operationId = lookup.originalOperationId;
    switch (lookup.lookupStatus) {
      case 'COMPLETED': {
        const directive = lookup.completedRetryDirective ?? 'NONE';
        if (directive === 'REFRESH_AUTHORITY_STATE') this.refreshAuthorityState();
        await this.updateJournal(operationId, { lastLookupStatus: 'COMPLETED' });
        await this.resolveTerminal(operationId, lookup.completedStatus ?? 'UNAVAILABLE');
        break;
      }
      case 'PROCESSING':
        operation.status = 'Processing';
        await this.updateJournal(operationId, { lastStatus: 'Processing', lastLookupStatus: 'PROCESSING' });
        this.scheduleResultPoll(operation, lookup.retryAfterMs);
        break;
      case 'NOT_FOUND': {
        // NOT_FOUND 不证明原请求未到达;按 RecoveryPolicy 恢复原 operation。
        await this.updateJournal(operationId, { lastLookupStatus: 'NOT_FOUND' });
        const record = await this.findJournalRecord(operationId);
        if (operation.commandType === 'CARE') {
          // CARE is an authority-local FarmActor commit, not a generic
          // WorkRecord. A query projection miss is not proof of failure, but
          // replaying the same operation_id on the still-active subscription
          // is safe: the Farm receipt returns the terminal CareResult without
          // duplicating FarmChange/reward. Fresh-Joining here would conflict
          // with the live same-session presence fence.
          await this.retrySameOperation(operationId);
        } else if (record?.targetFarmId) this.freshJoinThenRetry(operationId, record.targetFarmId);
        else await this.retrySameOperation(operationId);
        break;
      }
      case 'EXPIRED':
        // 结果已过保留期:停止自动重试,刷新权威状态,保留人工处置入口。
        operation.status = 'Unknown';
        operation.message = '操作结果已超过服务端保留期,请核对农场状态后重试';
        this.refreshAuthorityState();
        await this.updateJournal(operationId, { lastLookupStatus: 'EXPIRED' });
        break;
    }
    this.notify();
  }

  /**
   * 手动重放接缝:同 operation_id + 原 Typed Payload(服务端按幂等键只生效
   * 一次)。仅当 Journal 仍持有该 operation 的重放材料时可用。
   */
  async replayOperation(operationId: string): Promise<void> {
    const record = await this.findJournalRecord(operationId);
    if (!record) throw new Error('operation journal record is missing');
    if (record.payloadSchemaVersion !== JOURNAL_PAYLOAD_SCHEMA_VERSION || !record.payloadIr) {
      throw new Error('operation journal record has an unknown payload schema; replay is fail-closed');
    }
    // Durable journal material is the replay source of truth. Do not reuse a
    // mutable in-memory object: reconnect/replay must keep the original
    // expected plot/rule versions and never acquire UI-only qualification
    // fields.
    const payload = JSON.parse(record.payloadIr) as CommandPayload;
    if (payload.type !== record.commandType) {
      throw new Error('operation journal payload command identity mismatch');
    }
    this.ws.sendCommand({ operationId, requestId: createUuid(), payload });
    const operation = this.pending.get(operationId);
    if (operation) {
      operation.status = 'AwaitingResult';
      this.notify();
    }
  }

  dispose(): void {
    for (const timer of this.resultPollTimers.values()) window.clearTimeout(timer);
    for (const timer of this.replayTimers.values()) window.clearTimeout(timer);
    this.resultPollTimers.clear();
    this.resultPollAttempts.clear();
    this.replayTimers.clear();
    this.listeners.clear();
  }

  private async retrySameOperation(operationId: string): Promise<void> {
    const attempts = (this.sameOperationRetries.get(operationId) ?? 0) + 1;
    this.sameOperationRetries.set(operationId, attempts);
    const operation = this.pending.get(operationId);
    if (attempts > this.options.maxSameOperationRetries) {
      if (operation) {
        operation.status = 'Unknown';
        this.scheduleResultPoll(operation);
      }
      await this.updateJournal(operationId, { lastStatus: 'Unknown' });
      return;
    }
    if (operation) operation.status = 'Sending';
    await this.updateJournal(operationId, { lastStatus: 'AwaitingResult', retryCount: attempts });
    try {
      await this.replayOperation(operationId);
    } catch {
      // 写准入尚未开放(恢复屏障/Join 未完成):排队到 InFarm 后重放。
      this.scheduleReplay(operationId);
    }
  }

  private freshJoinThenRetry(operationId: string, targetFarmId: string | undefined): void {
    const ownerId = targetFarmId ?? this.ws.activeJoinOwnerId;
    if (!ownerId) return;
    this.ws.retryJoinFarm(ownerId);
    this.scheduleReplay(operationId);
  }

  private scheduleReplay(operationId: string): void {
    if (this.replayTimers.has(operationId)) return;
    const timer = window.setTimeout(() => {
      this.replayTimers.delete(operationId);
      if (this.ws.status !== 'InFarm') {
        // 恢复屏障/Join 未完成:下一轮再试(有界,与结果轮询共享退避)。
        this.scheduleReplay(operationId);
        return;
      }
      void this.replayOperation(operationId).catch(() => {
        this.scheduleReplay(operationId);
      });
    }, this.options.resultPollBaseDelayMs);
    this.replayTimers.set(operationId, timer);
  }

  private refreshAuthorityState(): void {
    // 不自动重放旧写命令;重新读取 Farm/Player 权威状态。
    if (this.ticket) this.ws.requestPlayerBootstrap(this.ticket.playerId);
    const ownerId = this.ws.activeJoinOwnerId;
    if (ownerId) this.ws.retryJoinFarm(ownerId);
  }

  private queryAndContinuePolling(operation: PendingOperation, record?: PendingOperationRecord): boolean {
    if (
      this.ws.status !== 'InFarm'
      && this.ws.status !== 'Recovering'
      && this.ws.status !== 'Joining'
      && this.ws.status !== 'Authenticated'
    ) return false;
    let queued = true;
    try {
      this.sendLookup(operation.operationId, record);
    } catch {
      queued = false;
      operation.status = 'Unknown';
      this.notify();
    }
    this.scheduleResultPoll(operation);
    return queued;
  }

  private sendLookup(operationId: string, record?: PendingOperationRecord): void {
    const operation = this.pending.get(operationId);
    this.ws.queryOperation({
      originalCommandType: (record?.commandType ?? operation?.commandType ?? 'BUY') as Exclude<CommandPayload['type'], 'GET_OPERATION_RESULT'>,
      originalOperationId: operationId,
      targetFarmId: record?.targetFarmId ?? operation?.targetFarmId,
      peerPlayerId: record?.peerPlayerId,
      mailId: record?.mailId,
      serverOperationLocator: record?.serverOperationLocator,
    });
  }

  private scheduleResultPoll(operation: PendingOperation, delayOverrideMs?: number): void {
    if (this.resultPollTimers.has(operation.operationId) || operation.status === 'Resolved') return;
    const attempt = this.resultPollAttempts.get(operation.operationId) ?? 0;
    const exponent = Math.min(attempt, 20);
    const ceiling = Math.min(
      this.options.resultPollMaxDelayMs,
      this.options.resultPollBaseDelayMs * (2 ** exponent),
    );
    const jitter = 0.8 + this.options.random() * 0.4;
    const delay = Math.max(1, Math.round(delayOverrideMs ?? ceiling * jitter));
    const timer = window.setTimeout(() => {
      this.resultPollTimers.delete(operation.operationId);
      const current = this.pending.get(operation.operationId);
      if (!current || current.status === 'Resolved') return;
      if (
        this.ws.status !== 'InFarm'
        && this.ws.status !== 'Recovering'
        && this.ws.status !== 'Joining'
        && this.ws.status !== 'Authenticated'
      ) return;
      this.resultPollAttempts.set(operation.operationId, attempt + 1);
      void this.findJournalRecord(operation.operationId).then((record) => {
        this.queryAndContinuePolling(current, record ?? undefined);
      });
    }, delay);
    this.resultPollTimers.set(operation.operationId, timer);
  }

  private clearResultPoll(operationId: string): void {
    const timer = this.resultPollTimers.get(operationId);
    if (timer !== undefined) window.clearTimeout(timer);
    this.resultPollTimers.delete(operationId);
    this.resultPollAttempts.delete(operationId);
    this.sameOperationRetries.delete(operationId);
    const replayTimer = this.replayTimers.get(operationId);
    if (replayTimer !== undefined) window.clearTimeout(replayTimer);
    this.replayTimers.delete(operationId);
  }

  private async resolveTerminal(operationId: string, terminalStatus: TerminalOperationStatus): Promise<void> {
    const operation = this.pending.get(operationId);
    if (operation) {
      operation.status = 'Resolved';
      operation.terminalStatus = terminalStatus;
    }
    this.clearResultPoll(operationId);
    await this.removeJournal(operationId);
    this.deferPendingRemoval(operationId);
  }

  private async stopUnresolved(operationId: string, message: string): Promise<void> {
    const operation = this.pending.get(operationId);
    if (!operation || operation.status === 'Resolved') return;
    operation.status = 'Unknown';
    operation.message = message;
    this.clearResultPoll(operationId);
    await this.updateJournal(operationId, { lastStatus: 'Unknown' });
    this.notify();
  }

  private deferPendingRemoval(operationId: string): void {
    window.setTimeout(() => {
      this.pending.delete(operationId);
      this.payloads.delete(operationId);
      this.notify();
    }, 800);
  }

  private async findJournalRecord(operationId: string): Promise<PendingOperationRecord | null> {
    if (!this.ticket) return null;
    const scope = `${this.environment}:${this.ticket.playerId}`;
    const records = await this.journal.list(scope);
    return records.find((candidate) => candidate.operationId === operationId) ?? null;
  }

  private async updateJournal(operationId: string, patch: Partial<PendingOperationRecord>): Promise<void> {
    const record = await this.findJournalRecord(operationId);
    if (record) await this.journal.put({ ...record, ...patch, operationId });
  }

  private async removeJournal(operationId: string): Promise<void> {
    if (!this.ticket) return;
    await this.journal.remove(`${this.environment}:${this.ticket.playerId}`, operationId);
  }

  private notify(): void {
    for (const listener of this.listeners) listener();
  }
}

const uint64Replacer = (_key: string, value: unknown): unknown =>
  typeof value === 'bigint' ? value.toString(10) : value;

const plotIdFromJournal = (record: PendingOperationRecord): number | undefined => {
  if (!record.payloadIr) return undefined;
  try {
    const payload = JSON.parse(record.payloadIr) as { plotId?: unknown };
    return typeof payload.plotId === 'number' && Number.isInteger(payload.plotId) ? payload.plotId : undefined;
  } catch {
    return undefined;
  }
};
