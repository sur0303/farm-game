import type { CommandResult } from '../protocol/types';

/**
 * Bootstrap + Recovery 屏障的纯判定逻辑(26/28 号合同)。
 *
 * 从 App 抽出的无副作用谓词,便于单元测试;App 只负责喂入实时事实并执行
 * 副作用(completeRecovery / 消息 / dataset)。
 */

/** GET_PLAYER_BOOTSTRAP 在本轮 Recovery 中的终态。 */
export type BootstrapOutcome =
  | 'pending'           // 尚未拿到权威终态
  | 'applied'           // SUCCEEDED/NO_CHANGE:私有基线已应用
  | 'feature-disabled'  // 后端明确 REJECTED ERR_COMMAND_UNIMPLEMENTED(1007)
  | 'unreachable';      // 重试预算耗尽仍无权威终态(老后端静默丢弃/链路异常)

export interface RecoveryBarrierFacts {
  /** ws.status === 'Recovering'(Join Ready 但屏障未放开)。 */
  readonly wsRecovering: boolean;
  /** ws.joinPhase === 'READY'(当前 Join 的公开 Ready 条件已满足)。 */
  readonly joinReady: boolean;
  /** 本玩家 farm snapshot 已应用。 */
  readonly ownSnapshotReady: boolean;
  /** Bootstrap 终态。 */
  readonly bootstrapOutcome: BootstrapOutcome;
  /** Journal 未决命令首遍对账完成。 */
  readonly pendingFirstPassComplete: boolean;
  /** currentOwnerId === ownOwnerId(Gate 只接受自家 ActiveSubscription 上的完成标记)。 */
  readonly onOwnFarm: boolean;
}

/**
 * Bootstrap 条件满足 = 权威终态成功(applied)。
 * (H5-VERTICAL-CORRECTNESS-A1 §1)feature-disabled/unreachable 不得打开
 * RecoveryBarrier:后端尚未接线(1007)或重试预算耗尽仍无权威应答时,资产
 * 读数必须保持未知,屏障保持关闭(fail closed),绝不把"不可用"当成可进入
 * 农场的通行证;后续首个权威 private_state 回执仍会被单调应用(26 号合同)。
 */
export const bootstrapConditionMet = (outcome: BootstrapOutcome): boolean => outcome === 'applied';

/** 屏障放开(允许 completeRecovery)的全部必要条件。 */
export const recoveryBarrierSatisfied = (facts: RecoveryBarrierFacts): boolean =>
  facts.wsRecovering
  && facts.joinReady
  && facts.ownSnapshotReady
  && bootstrapConditionMet(facts.bootstrapOutcome)
  && facts.pendingFirstPassComplete
  && facts.onOwnFarm;

/**
 * 分类 GET_PLAYER_BOOTSTRAP 的 CommandResult。
 * 返回 null 表示非终态(中间态或可重试的拒绝),由 App 的重试预算继续处理。
 */
export const classifyBootstrapResult = (
  result: Pick<CommandResult, 'status' | 'errorCode' | 'featureUnavailable'>,
): Exclude<BootstrapOutcome, 'pending'> | null => {
  if (result.status === 'SUCCEEDED' || result.status === 'NO_CHANGE') return 'applied';
  if (result.status === 'REJECTED') {
    if (result.featureUnavailable || result.errorCode === 'ERR_COMMAND_UNIMPLEMENTED') {
      return 'feature-disabled';
    }
    return null;
  }
  return null;
};
