import { describe, expect, it } from 'vitest';

import type { PlotState } from '../protocol/types';
import {
  canUnlockPlot,
  effectiveCropPhase,
  isMaturePlot,
  isPlantablePlot,
  isWaterablePlot,
  selectTheftPlot,
  unlockPlotPayload,
} from '../app/plot-guards';

const plot = (partial: Partial<PlotState>): PlotState => ({
  plotId: 'plot-1',
  plotIndex: 1,
  plotVersion: 1,
  lockState: 'UNLOCKED',
  unlockOrder: 1,
  requiredLevel: 1,
  normalCoinPrice: '0',
  unlockCandidate: false,
  unlockRuleVersion: '1',
  status: 'EMPTY',
  cropDefinitionId: 1,
  ...partial,
});

describe('isMaturePlot', () => {
  it('accepts only the authoritative wire MATURE status, never local matureAt clock skew', () => {
    const now = Date.now();
    // 服务端 wire 仍是 GROWING:不得把本地成熟时间当作权威状态。
    expect(isMaturePlot(plot({ status: 'GROWING', matureAt: now - 5_000 }))).toBe(false);
    // 服务端权威 MATURE,即使本地时钟显示还没到成熟时间,也视为成熟。
    expect(isMaturePlot(plot({ status: 'MATURE', matureAt: now + 60_000 }))).toBe(true);
    expect(isMaturePlot(plot({ status: 'MATURE', matureAt: now - 1 }))).toBe(true);
    expect(isMaturePlot(plot({ status: 'MATURE' }))).toBe(true);
    expect(isMaturePlot(plot({ status: 'GROWING' }))).toBe(false);
    expect(isMaturePlot(plot({ status: 'GROWING', matureAt: now + 10_000 }))).toBe(false);
    expect(isMaturePlot(plot({ status: 'EMPTY' }))).toBe(false);
  });
});

describe('isPlantablePlot', () => {
  it('requires a positively identified UNLOCKED EMPTY plot', () => {
    expect(isPlantablePlot(plot({ lockState: 'UNLOCKED', status: 'EMPTY' }))).toBe(true);
    expect(isPlantablePlot(plot({ lockState: 'LOCKED', status: 'EMPTY' }))).toBe(false);
    expect(isPlantablePlot(plot({ lockState: 'UNSPECIFIED', status: 'EMPTY' }))).toBe(false);
    expect(isPlantablePlot(plot({ lockState: 'UNLOCKED', status: 'GROWING' }))).toBe(false);
    expect(isPlantablePlot(plot({ lockState: 'UNLOCKED', status: 'MATURE' }))).toBe(false);
    // A malformed runtime value is not an unlock proof.
    expect(isPlantablePlot(plot({ lockState: 'MALFORMED' as never, status: 'EMPTY' }))).toBe(false);
  });
});

describe('unlock affordance', () => {
  it('keeps friend locks read-only and builds exact owner payload from PlotView', () => {
    const candidate = plot({
      plotIndex: 6,
      plotId: 'plot-6',
      lockState: 'LOCKED',
      unlockCandidate: true,
      plotVersion: '11',
      unlockRuleVersion: '9',
      requiredLevel: 4,
      normalCoinPrice: '1200',
    });
    expect(canUnlockPlot(candidate, false, true, false)).toBe(false);
    expect(unlockPlotPayload(candidate, '10001', false)).toBeUndefined();
    expect(canUnlockPlot(candidate, true, true, false)).toBe(true);
    expect(unlockPlotPayload(candidate, '10001', true)).toEqual({
      type: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      expectedPlotVersion: '11',
      expectedUnlockRuleVersion: '9',
    });
    expect(unlockPlotPayload(candidate, '10001', true)).not.toHaveProperty('requiredLevel');
    expect(unlockPlotPayload(candidate, '10001', true)).not.toHaveProperty('normalCoinPrice');
    expect(canUnlockPlot({ ...candidate, unlockCandidate: false }, true, true, false)).toBe(false);
    expect(canUnlockPlot(candidate, true, true, true)).toBe(false);
  });
});

describe('effectiveCropPhase (Q1 trusted-clock display derivation)', () => {
  it('derives MATURE from trusted server time, not browser wall clock', () => {
    // 受信服务端时钟已过 mature_at:展示阶段提升为 MATURE。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: 1_000 }), 1_100)).toBe('MATURE');
    // 受信服务端时钟未到 mature_at:保持 GROWING(下界估计只会偏晚,不会提前)。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: 1_000 }), 900)).toBe('GROWING');
    // 恰好相等:视为成熟。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: 1_000 }), 1_000)).toBe('MATURE');
  });

  it('fails closed without a trusted clock or without mature_at', () => {
    // 无受信时钟(尚未收到任何服务端时间):不得把 GROWING 提升为 MATURE。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: 1_000 }), null)).toBe('GROWING');
    // 无 mature_at 的服务端数据:保持 GROWING。
    expect(effectiveCropPhase(plot({ status: 'GROWING' }), 5_000)).toBe('GROWING');
  });

  it('keeps wire EMPTY/MATURE authoritative', () => {
    expect(effectiveCropPhase(plot({ status: 'EMPTY', matureAt: 1 }), 9_999)).toBe('EMPTY');
    expect(effectiveCropPhase(plot({ status: 'MATURE', matureAt: 9_999 }), 1)).toBe('MATURE');
    // wire MATURE 优先于受信时钟判断。
    expect(effectiveCropPhase(plot({ status: 'MATURE', matureAt: 9_999 }), null)).toBe('MATURE');
  });

  it('browser Date.now running 10min ahead cannot force maturity early', () => {
    // 受信服务端时钟(下界估计)才是唯一输入:即使墙钟被改快 10 分钟,调用方
    // 只要传受信时钟值(而非 Date.now),effective 就按该值判定,不受墙钟影响。
    const serverNow = 100_000;
    // 受信时钟未到 mature_at:仍 GROWING(此时墙钟可能已远超该值,但无关)。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: serverNow + 5_000 }), serverNow)).toBe('GROWING');
    // 受信时钟到点:才 MATURE。effective 只看 trustedNowMs,天然不受墙钟快慢影响。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: serverNow + 5_000 }), serverNow + 5_000)).toBe('MATURE');
    // 未同步受信时钟(null):即使墙钟已远超 mature_at,也不得成熟(fail-closed)。
    expect(effectiveCropPhase(plot({ status: 'GROWING', matureAt: serverNow + 5_000 }), null)).toBe('GROWING');
  });
});

describe('selectTheftPlot', () => {
  const first = plot({ plotId: 'plot-1', plotIndex: 1, status: 'MATURE' });
  const second = plot({ plotId: 'plot-2', plotIndex: 2, status: 'GROWING', matureAt: 1_000 });

  it('preserves an eligible selection and otherwise chooses the first mature target', () => {
    expect(selectTheftPlot([first, second], 'plot-2', 1_000)?.plotId).toBe('plot-2');
    expect(selectTheftPlot([first, second], null, 1_000)?.plotId).toBe('plot-1');
    expect(selectTheftPlot([first, second], 'missing', 1_000)?.plotId).toBe('plot-1');
  });

  it('fails closed for locked or not-yet-mature plots', () => {
    expect(selectTheftPlot([{ ...first, lockState: 'LOCKED' }, second], null, 999)).toBeUndefined();
    expect(selectTheftPlot([second], null, null)).toBeUndefined();
  });
});

describe('isWaterablePlot', () => {
  it('requires GROWING plus no local water mark, independent of matureAt', () => {
    const now = Date.now();
    expect(isWaterablePlot(plot({ status: 'GROWING' }))).toBe(true);
    expect(isWaterablePlot(plot({ lockState: 'LOCKED', status: 'GROWING' }))).toBe(false);
    expect(isWaterablePlot(plot({ lockState: 'UNSPECIFIED', status: 'GROWING' }))).toBe(false);
    expect(isWaterablePlot(plot({ status: 'GROWING', matureAt: now + 10_000 }), now)).toBe(true);
    expect(isWaterablePlot(plot({ status: 'GROWING', matureAt: now - 1 }), now)).toBe(false);
    expect(isWaterablePlot(plot({ status: 'GROWING', wateredAt: now }))).toBe(false);
    expect(isWaterablePlot(plot({ status: 'MATURE' }))).toBe(false);
    expect(isWaterablePlot(plot({ status: 'EMPTY' }))).toBe(false);
  });
});
