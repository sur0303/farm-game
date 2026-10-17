/**
 * GET_PLAYER_BOOTSTRAP 请求的目的(PlayerRefresh / Q1 预算分离)。
 *
 * 首屏/重连的 mandatory Bootstrap 是 RecoveryBarrier 的准入门,共享每轮
 * MAX_BOOTSTRAP_ATTEMPTS 的恢复预算;经营命令后的资产 refresh 是显式查询,
 * 无长期 OperationResult,应独立重试,不得消耗恢复预算(否则 Recovery 抖动
 * 重试一次就会饿死 PLANT 后的刷新 —— Q1 run16 根因)。
 */
export type BootstrapRequestPurpose = 'recovery' | 'refresh';

interface TrackedBootstrapRequest {
  generation: number;
  purpose: BootstrapRequestPurpose;
}

/** 资产相关命令:终态成功后应安全重查 Bootstrap 刷新私有状态(PlayerRefresh §4)。 */
const ASSET_AFFECTING_COMMANDS = new Set(['BUY', 'SELL', 'PLANT', 'HARVEST', 'THEFT', 'UNLOCK_PLOT', 'CLAIM_TASK']);

/** 纯谓词:该命令终态成功会改变 wallet/inventory,需要重查 Bootstrap。 */
export const isAssetAffectingCommand = (commandType: string | undefined): boolean =>
  commandType !== undefined && ASSET_AFFECTING_COMMANDS.has(commandType);

/**
 * Recovery 会话内 GET_PLAYER_BOOTSTRAP 请求的相关性跟踪
 * (H5-VERTICAL-CORRECTNESS-A1 §2 + Q1 预算分离)。
 *
 * 恢复与刷新请求都按 operationId 记录所属 generation,响应按
 * `claim(operationId, generation)` 认领(收到终态即消费,防止集合随会话
 * 无限增长)。旧 generation / 未知 operationId 一律返回 null(不消费)。
 * 乱序晚到的成功仍是当前 generation 的合法应答(A1 §2);新 ServerHello 开启
 * 新 generation 时 reset() 隔离旧 generation 的响应。
 */
export class BootstrapRecovery {
  private generationValue = -1;
  private recoverySentCountValue = 0;
  private readonly requests = new Map<string, TrackedBootstrapRequest>();

  constructor(private readonly maxAttempts: number) {}

  reset(generation: number): void {
    this.generationValue = generation;
    this.recoverySentCountValue = 0;
    this.requests.clear();
  }

  get generation(): number {
    return this.generationValue;
  }

  /** 本 generation 已发送的 mandatory Recovery 请求数(退避重试会递增)。 */
  get recoverySentCount(): number {
    return this.recoverySentCountValue;
  }

  /** mandatory Recovery 的发送预算:仅恢复重试计数,refresh 不占用。 */
  hasBudget(): boolean {
    return this.recoverySentCountValue < this.maxAttempts;
  }

  /** 记录一次 mandatory Recovery 发送;预算耗尽或 generation 不匹配时拒绝。 */
  trackRecovery(operationId: string, generation: number): boolean {
    if (operationId === '' || generation !== this.generationValue || !this.hasBudget()) return false;
    this.recoverySentCountValue += 1;
    this.requests.set(operationId, { generation, purpose: 'recovery' });
    return true;
  }

  /** 记录一次资产 refresh 发送:不消耗恢复预算,只做相关性跟踪。 */
  trackRefresh(operationId: string, generation: number): boolean {
    if (operationId === '' || generation !== this.generationValue) return false;
    this.requests.set(operationId, { generation, purpose: 'refresh' });
    return true;
  }

  /** 认领响应:返回该请求的用途并立即消费(从集合移除),防无限增长。 */
  claim(operationId: string | undefined, generation: number): BootstrapRequestPurpose | null {
    if (operationId === undefined) return null;
    const request = this.requests.get(operationId);
    if (request === undefined || request.generation !== generation) return null;
    this.requests.delete(operationId);
    return request.purpose;
  }

  /** 请求超时/失败时遗忘,避免集合与 in-flight 残留。 */
  forget(operationId: string, generation: number): void {
    const request = this.requests.get(operationId);
    if (request?.generation === generation) {
      this.requests.delete(operationId);
    }
  }
}
