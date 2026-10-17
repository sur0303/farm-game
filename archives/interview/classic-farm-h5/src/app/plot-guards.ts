import type { CommandPayload, PlotState } from '../protocol/types';

/**
 * 地块展示阶段谓词(Q1 CONTRACT_MISMATCH 修正)。
 *
 * 合同(32 号)规定:成熟是时间派生视图,不是持久状态迁移。服务端在 Plant 时
 * 一次性保存 mature_at,作物成熟时**不追加状态写、不推进 FarmVersion**;只有
 * Harvest/Theft/Care 等真正改变业务事实时才推进 FarmVersion。
 *
 * 因此把两个概念分开:
 * - wire phase:最近一次 Snapshot/FarmChange 里服务端下发的 PlotState.status;
 * - effective display phase:由"受信服务端时钟 + mature_at"派生的当前展示阶段,
 *   仅用于 UI 投影,绝不对 FarmStore.plot.status 做写入(不伪造 FarmChange)。
 *
 * 使用 trustedNowMs(服务端时钟下界)而非浏览器 Date.now():墙钟可能被修改/
 * NTP 跳变;下界估计最多让成熟显示稍晚,不会提前给出操作提示。
 */

/** 展示阶段:EMPTY / GROWING / MATURE。 */
export type EffectiveCropPhase = 'EMPTY' | 'GROWING' | 'MATURE';

/** Only a positively identified server-unlocked plot is plantable. */
export const isPlantablePlot = (plot: PlotState): boolean =>
  plot.lockState === 'UNLOCKED' && plot.status === 'EMPTY';

/** Owner-only unlock affordance; level/coin qualification stays server-side. */
export const canUnlockPlot = (
  plot: PlotState,
  viewingOwnFarm: boolean,
  writesReady: boolean,
  pending: boolean,
): boolean =>
  viewingOwnFarm
  && writesReady
  && !pending
  && plot.lockState === 'LOCKED'
  && plot.unlockCandidate;

/** Build the one canonical unlock intent from the latest PlotView only. */
export const unlockPlotPayload = (
  plot: PlotState,
  targetFarmId: string,
  viewingOwnFarm: boolean,
): Extract<CommandPayload, { type: 'UNLOCK_PLOT' }> | undefined => {
  if (!viewingOwnFarm || plot.lockState !== 'LOCKED' || !plot.unlockCandidate) return undefined;
  return {
    type: 'UNLOCK_PLOT',
    targetFarmId,
    plotId: plot.plotIndex,
    expectedPlotVersion: plot.plotVersion,
    expectedUnlockRuleVersion: plot.unlockRuleVersion,
  };
};

/** Unknown/malformed lock states are intentionally not treated as unlocked. */
export const isKnownPlotLockState = (plot: PlotState): boolean =>
  plot.lockState === 'UNSPECIFIED' || plot.lockState === 'UNLOCKED' || plot.lockState === 'LOCKED';

/**
 * 派生展示阶段(纯函数)。
 * trustedNowMs 为 null(尚未收到任何受信服务端时钟)时,不会把 GROWING
 * 提升为 MATURE——保持 fail-closed。
 */
export const effectiveCropPhase = (
  plot: PlotState,
  trustedNowMs: number | null,
): EffectiveCropPhase => {
  if (plot.status === 'EMPTY') return 'EMPTY';
  if (plot.status === 'MATURE') return 'MATURE';
  if (
    plot.status === 'GROWING'
    && trustedNowMs !== null
    && plot.matureAt !== undefined
    && trustedNowMs >= plot.matureAt
  ) {
    return 'MATURE';
  }
  return 'GROWING';
};

/** 服务端下发的 wire 权威状态是否为 MATURE(不派生;用于诊断/断言)。 */
export const isMaturePlot = (plot: PlotState): boolean => plot.status === 'MATURE';

/**
 * Single-theft UX preserves an explicitly selected mature target, otherwise it
 * deterministically picks the first authority-visible mature plot. This only
 * chooses a UI intent; FarmActor still performs the final adjudication.
 */
export const selectTheftPlot = (
  plots: PlotState[],
  selectedPlotId: string | null,
  trustedNowMs: number | null,
): PlotState | undefined => {
  const eligible = (plot: PlotState): boolean => plot.lockState === 'UNLOCKED'
    && effectiveCropPhase(plot, trustedNowMs) === 'MATURE';
  return plots.find((plot) => plot.plotId === selectedPlotId && eligible(plot)) ?? plots.find(eligible);
};

/** 可浇水判定:服务端仍为 GROWING 且本地尚无 wateredAt 浇灌标记。 */
export const isWaterablePlot = (plot: PlotState, trustedNowMs: number | null = null): boolean =>
  plot.lockState === 'UNLOCKED'
  && effectiveCropPhase(plot, trustedNowMs) === 'GROWING'
  && !plot.wateredAt;
