export type GrowingCropStage = 'stage-sprout' | 'stage-leaf' | 'stage-ripen';

export interface CropTimeProjection {
  secondsLeft: number;
  stage: GrowingCropStage;
}

/**
 * Pure UI projection for a growing crop. It never decides whether a crop may
 * be harvested; that decision remains based on trusted server time and the
 * authoritative Farm view.
 */
export const cropTimeProjection = (
  matureAtMs: number,
  nowMs: number,
  configuredDurationMs: number,
): CropTimeProjection => {
  const durationMs = Math.max(1, configuredDurationMs);
  const secondsLeft = Math.max(0, Math.ceil((matureAtMs - nowMs) / 1_000));
  const plantedAtMs = matureAtMs - durationMs;
  const grownMs = Math.min(Math.max(nowMs - plantedAtMs, 0), durationMs);
  const stage: GrowingCropStage = grownMs < durationMs / 3
    ? 'stage-sprout'
    : grownMs < (durationMs * 2) / 3
      ? 'stage-leaf'
      : 'stage-ripen';

  return { secondsLeft, stage };
};
