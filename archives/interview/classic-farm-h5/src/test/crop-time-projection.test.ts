import { describe, expect, it } from 'vitest';
import { cropTimeProjection } from '../app/crop-time-projection';

describe('cropTimeProjection', () => {
  it('uses the configured crop duration instead of a fixed ten-second clock', () => {
    const matureAt = 40_000;
    expect(cropTimeProjection(matureAt, 10_000, 30_000)).toEqual({
      secondsLeft: 30,
      stage: 'stage-sprout',
    });
    expect(cropTimeProjection(matureAt, 25_000, 30_000).stage).toBe('stage-leaf');
    expect(cropTimeProjection(matureAt, 35_000, 30_000).stage).toBe('stage-ripen');
  });

  it('clamps countdown and growth at their legal display bounds', () => {
    expect(cropTimeProjection(10_000, 12_000, 10_000)).toEqual({
      secondsLeft: 0,
      stage: 'stage-ripen',
    });
    expect(cropTimeProjection(10_000, -5_000, 10_000)).toEqual({
      secondsLeft: 15,
      stage: 'stage-sprout',
    });
  });
});
