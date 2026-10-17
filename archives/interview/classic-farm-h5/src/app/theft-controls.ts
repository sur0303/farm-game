import type { PlotState } from '../protocol/types';
import type { FriendOpportunityStatus, FriendTheftTarget } from '../friends/friend-roster';
import { effectiveCropPhase } from './plot-guards';

export const isExactTheftPlotEligible = (
  plot: PlotState,
  status: FriendOpportunityStatus,
  targets: readonly FriendTheftTarget[],
  trustedNowMs: number,
  alreadyConfirmed: boolean,
): boolean => status === 'READY'
  && !alreadyConfirmed
  && plot.lockState === 'UNLOCKED'
  && effectiveCropPhase(plot, trustedNowMs) === 'MATURE'
  && targets.some((target) => target.plotId === plot.plotIndex + 1 && target.cropCycleId === plot.cropCycleId);

export interface TheftConfirmation {
  ownerId: string;
  plotId: string;
  cropCycleId: string;
}

export const matchesTheftConfirmation = (
  armed: TheftConfirmation | null,
  ownerId: string,
  plotId: string,
  cropCycleId: string | undefined,
): boolean => Boolean(armed
  && cropCycleId
  && armed.ownerId === ownerId
  && armed.plotId === plotId
  && armed.cropCycleId === cropCycleId);

export interface TheftTerminalPresentation {
  message: string;
  error: boolean;
  consumesTarget: boolean;
}

export const theftTerminalPresentation = (
  status: 'SUCCEEDED' | 'NO_CHANGE' | 'REJECTED' | 'UNAVAILABLE',
  errorCode: string | undefined,
  errorMessage: string | undefined,
  plotLabel: string,
): TheftTerminalPresentation => {
  if (status === 'SUCCEEDED' || status === 'NO_CHANGE') {
    return {
      message: `偷菜成功${plotLabel}，服务端已完成权威结算，正在刷新库存…`,
      error: false,
      consumesTarget: true,
    };
  }
  if (status === 'REJECTED' && errorCode === 'ERR_STEAL_ALREADY_DONE') {
    return {
      message: `该地块本茬已经偷过${plotLabel}，本次没有重复获得作物`,
      error: true,
      consumesTarget: true,
    };
  }
  if (status === 'REJECTED') {
    return {
      message: `偷菜未成功${plotLabel}：${errorMessage ?? errorCode ?? '服务端拒绝了本次操作'}`,
      error: true,
      consumesTarget: false,
    };
  }
  return {
    message: `偷菜结果不可用${plotLabel}，已刷新权威状态；请以当前库存和好友机会为准`,
    error: true,
    consumesTarget: false,
  };
};
