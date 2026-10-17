/**
 * PlayerBootstrapRefresh:经营命令(GET_PLAYER_BOOTSTRAP 之后)资产收敛的
 * 独立 refresh 控制器(Q1 预算分离)。
 *
 * 它不发网络,只管理确定性状态:
 * - dirty sequence:资产命令每次 SUCCEEDED 递增,表示"本地私有状态可能过期";
 * - applied sequence:一次 refresh 成功应用后收敛到该 dirty 时刻;
 * - 同时最多一个 in-flight;期间又有资产变化时,本次成功后恰好触发一次 follow-up;
 * - 独立重试预算(maxAttempts),与首屏恢复的 MAX_BOOTSTRAP_ATTEMPTS 完全无关。
 *
 * 错误语义:refresh 失败/被拒绝不把已 applied 的 RecoveryBarrier 改回
 * pending/unreachable;只保留 dirty 并按独立 backoff 重试。
 */
export interface PlayerBootstrapRefreshAttempt {
  operationId: string;
  connectionGeneration: number;
  dirtySequence: number;
  attempt: number;
}

export class PlayerBootstrapRefresh {
  private generation = 0;
  private dirtySequence = 0;
  private appliedSequence = 0;
  private attempts = 0;
  private inFlight: PlayerBootstrapRefreshAttempt | null = null;

  constructor(private readonly maxAttempts = 3) {}

  reset(generation: number): void {
    this.generation = generation;
    this.dirtySequence = 0;
    this.appliedSequence = 0;
    this.attempts = 0;
    this.inFlight = null;
  }

  /** 资产终态成功后标记一次新的业务事实。 */
  markDirty(): void {
    this.dirtySequence += 1;
    // 上一轮重试预算耗尽,但出现了新的业务事实,允许启动新一轮有界查询。
    if (this.inFlight === null && this.attempts >= this.maxAttempts) {
      this.attempts = 0;
    }
  }

  canStart(generation: number): boolean {
    return generation === this.generation
      && this.inFlight === null
      && this.dirtySequence > this.appliedSequence
      && this.attempts < this.maxAttempts;
  }

  begin(operationId: string, generation: number): PlayerBootstrapRefreshAttempt {
    if (!this.canStart(generation)) throw new Error('player bootstrap refresh cannot start');
    const attempt: PlayerBootstrapRefreshAttempt = {
      operationId,
      connectionGeneration: generation,
      dirtySequence: this.dirtySequence,
      attempt: ++this.attempts,
    };
    this.inFlight = attempt;
    return attempt;
  }

  claim(operationId: string, generation: number): PlayerBootstrapRefreshAttempt | null {
    const current = this.inFlight;
    if (
      current === null
      || current.operationId !== operationId
      || current.connectionGeneration !== generation
    ) {
      return null;
    }
    this.inFlight = null;
    return current;
  }

  /** 成功应用:收敛 dirty;期间又有新资产事实时 needsAnotherRequest=true。 */
  succeed(attempt: PlayerBootstrapRefreshAttempt): void {
    this.appliedSequence = Math.max(this.appliedSequence, attempt.dirtySequence);
    this.attempts = 0;
  }

  /** 失败/超时:保留 dirty,调用方按独立 backoff 再次 pump。 */
  fail(): void {
    // dirtySequence 保留;attempts 已在 begin 时递增,backoff 后再次 pump。
  }

  /** 在应用后是否仍需再发一笔(期间又有资产命令)。 */
  get needsAnotherRequest(): boolean {
    return this.dirtySequence > this.appliedSequence;
  }

  get pending(): boolean {
    return this.inFlight !== null;
  }

  /** 诊断:已标记的脏序列号。 */
  get dirtySeq(): number {
    return this.dirtySequence;
  }

  /** 诊断:已收敛的序列号。 */
  get appliedSeq(): number {
    return this.appliedSequence;
  }

  /** 诊断:当前轮已尝试次数。 */
  get retries(): number {
    return this.attempts;
  }
}
