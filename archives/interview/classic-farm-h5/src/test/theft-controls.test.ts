import { describe, expect, it } from 'vitest';
import type { PlotState } from '../protocol/types';
import { isExactTheftPlotEligible, matchesTheftConfirmation, theftTerminalPresentation } from '../app/theft-controls';

const cycleId = 'sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s';
const plot = (overrides: Partial<PlotState> = {}): PlotState => ({
  plotId: 'plot-3',
  plotIndex: 3,
  lockState: 'UNLOCKED',
  unlockOrder: 4,
  unlockCandidate: false,
  requiredLevel: 1,
  normalCoinPrice: '0',
  unlockRuleVersion: '1',
  status: 'GROWING',
  cropCycleId: cycleId,
  cropDefinitionId: 1,
  matureAt: 1_000,
  plotVersion: 2,
  stolenTotal: 0,
  ...overrides,
});

describe('authority theft controls', () => {
  it('requires READY authority and an exact 1-based plot/cycle intersection', () => {
    const targets = [{ plotId: 4, cropCycleId: cycleId }];
    expect(isExactTheftPlotEligible(plot(), 'READY', targets, 1_000, false)).toBe(true);
    expect(isExactTheftPlotEligible(plot(), 'UNKNOWN', targets, 1_000, false)).toBe(false);
    expect(isExactTheftPlotEligible(plot(), 'READY', [{ plotId: 3, cropCycleId: cycleId }], 1_000, false)).toBe(false);
    expect(isExactTheftPlotEligible(plot(), 'READY', [{ plotId: 4, cropCycleId: `${cycleId}x` }], 1_000, false)).toBe(false);
    expect(isExactTheftPlotEligible(plot(), 'READY', targets, 999, false)).toBe(false);
    expect(isExactTheftPlotEligible(plot(), 'READY', targets, 1_000, true)).toBe(false);
  });

  it('requires a Theft-button-owned exact confirmation instead of any generic plot selection', () => {
    expect(matchesTheftConfirmation(null, 'owner-2', 'plot-3', cycleId)).toBe(false);
    expect(matchesTheftConfirmation({ ownerId: 'owner-2', plotId: 'plot-3', cropCycleId: cycleId }, 'owner-2', 'plot-3', cycleId)).toBe(true);
    expect(matchesTheftConfirmation({ ownerId: 'owner-2', plotId: 'plot-3', cropCycleId: cycleId }, 'owner-2', 'plot-4', cycleId)).toBe(false);
    expect(matchesTheftConfirmation({ ownerId: 'owner-2', plotId: 'plot-3', cropCycleId: cycleId }, 'owner-3', 'plot-3', cycleId)).toBe(false);
  });

  it('presents every terminal honestly and consumes only success/already-done targets', () => {
    expect(theftTerminalPresentation('SUCCEEDED', undefined, undefined, '（地块 #3）')).toMatchObject({
      error: false, consumesTarget: true, message: expect.stringContaining('偷菜成功'),
    });
    expect(theftTerminalPresentation('NO_CHANGE', undefined, undefined, '')).toMatchObject({ error: false, consumesTarget: true });
    expect(theftTerminalPresentation('REJECTED', 'ERR_STEAL_ALREADY_DONE', undefined, '')).toMatchObject({
      error: true, consumesTarget: true, message: expect.stringContaining('本茬已经偷过'),
    });
    expect(theftTerminalPresentation('REJECTED', 'ERR_NOT_FRIEND', '好友关系无效', '')).toMatchObject({
      error: true, consumesTarget: false, message: expect.stringContaining('好友关系无效'),
    });
    expect(theftTerminalPresentation('UNAVAILABLE', undefined, undefined, '')).toMatchObject({
      error: true, consumesTarget: false, message: expect.stringContaining('结果不可用'),
    });
  });
});
