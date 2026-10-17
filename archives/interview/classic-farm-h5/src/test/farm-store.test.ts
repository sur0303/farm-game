import { describe, expect, it } from 'vitest';
import { FarmStore, validPlotDeltas } from '../state/farm-store';
import type { PlotState } from '../protocol/types';

const plotState = (partial: Partial<PlotState> = {}): PlotState => ({
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
  ...partial,
});

const snapshot = (version: number) => ({
  ownerId: 'player-a',
  farmVersion: version,
  state: {
    ownerId: 'player-a',
    wallet: 100,
    inventory: { seed: 2 },
    plots: [plotState()],
  },
});

describe('FarmStore', () => {
  it('rejects a farm version gap instead of guessing missing changes', () => {
    const store = new FarmStore();
    store.applySnapshot(snapshot(1));
    expect(store.applyChange({ ...snapshot(3), state: { ...snapshot(3).state, plots: [] } })).toBe('gap');
    expect(store.onFarm('player-a')?.farmVersion).toBe(1);
  });

  it('rejects a malformed Snapshot instead of treating missing lock state as unlocked', () => {
    const store = new FarmStore();
    store.applySnapshot(snapshot(1));
    store.applySnapshot({
      ...snapshot(2),
      state: {
        ...snapshot(2).state,
        plots: [plotState({ lockState: undefined as never })],
      },
    });
    expect(store.onFarm('player-a')?.farmVersion).toBe(1);
    expect(store.onFarm('player-a')?.state.plots[0].lockState).toBe('UNLOCKED');
  });

  it('deduplicates an old farm change', () => {
    const store = new FarmStore();
    store.applySnapshot(snapshot(2));
    expect(store.applyChange({ ...snapshot(2), state: { ...snapshot(2).state, plots: [] } })).toBe('duplicate');
  });

  it('requires a snapshot before applying a farm change', () => {
    const store = new FarmStore();
    expect(store.applyChange(snapshot(1))).toBe('gap');
    expect(store.onFarm('player-a')).toBeUndefined();
  });

  it('never lets an older PlayerBootstrap overwrite a newer operation result', () => {
    const store = new FarmStore();
    store.applyPrivateState({ economyVersion: '8', wallet: 80, inventory: { carrot: 2 } });
    store.applyPrivateState({ economyVersion: '7', wallet: 100, inventory: {} });
    expect(store.getPrivateState()).toMatchObject({
      economyVersion: '8', wallet: 80, inventory: { carrot: 2 },
    });

    store.applyPrivateState({ economyVersion: '9', wallet: 95, inventory: { carrot: 1 } });
    expect(store.getPrivateState()).toMatchObject({
      economyVersion: '9', wallet: 95, inventory: { carrot: 1 },
    });
  });

  it('applies task summaries monotonically without changing economy state', () => {
    const store = new FarmStore();
    store.applyPrivateState({ economyVersion: '8', wallet: 80, inventory: { carrot: 2 } });
    store.applyTaskSummary({
      taskStateVersion: '5', claimableCount: 1, dailyPeriodId: '2026-08-08',
      dailyTasks: [{ instanceId: 'daily:login', taskId: 'daily.login.v1', progress: 1, target: 1, state: 'CLAIMABLE' }],
    });
    store.applyTaskSummary({
      taskStateVersion: '4', claimableCount: 0, dailyPeriodId: '2026-08-07', dailyTasks: [],
    });
    expect(store.getPrivateState()).toMatchObject({
      economyVersion: '8', wallet: 80, inventory: { carrot: 2 },
      featureSummary: { tasks: { taskStateVersion: '5', claimableCount: 1, dailyPeriodId: '2026-08-08' } },
    });
  });

  it('keeps bootstrap revision/profile when CLAIM_TASK returns a partial private state', () => {
    const store = new FarmStore();
    expect(store.applyPlayerBootstrap({
      playerStateRevision: '12', economyVersion: '7', wallet: 50, inventory: {},
      profile: { playerId: '10001', displayName: 'Alice' },
    })).toBe(true);
    store.applyPrivateState({
      economyVersion: '8', wallet: 50, inventory: {}, progression: { level: 2, exp: '25' },
      featureSummary: {
        tasks: { taskStateVersion: '9', claimableCount: 0, dailyPeriodId: '2026-08-08', dailyTasks: [] },
      },
    });
    expect(store.getPrivateState()).toMatchObject({
      playerStateRevision: '12', economyVersion: '8',
      profile: { playerId: '10001', displayName: 'Alice' },
      progression: { level: 2, exp: '25' },
      featureSummary: { tasks: { taskStateVersion: '9', claimableCount: 0 } },
    });
  });

  it('keeps Snapshot fence metadata through a partial FarmChange', () => {
    const store = new FarmStore();
    store.applySnapshot({
      ...snapshot(1),
      subscriptionId: 'sub-1',
      joinAttemptId: 'join-1',
      privateStateIncluded: false,
    });
    const result = store.applyChange({
      ...snapshot(2),
      partial: true,
      subscriptionId: 'sub-1',
      state: {
        ...snapshot(2).state,
        plots: [plotState({ plotVersion: 2, status: 'GROWING' })],
      },
    });
    expect(result).toBe('applied');
    const farm = store.onFarm('player-a');
    expect(farm?.farmVersion).toBe(2);
    // Snapshot 订阅元数据(fence)在 applyChange 后保留,不被 FarmChange 丢弃。
    expect(farm?.subscriptionId).toBe('sub-1');
    expect(farm?.joinAttemptId).toBe('join-1');
    expect(farm?.privateStateIncluded).toBe(false);
  });

  it('keeps every lock/rule offer field when a partial delta updates the plot', () => {
    const store = new FarmStore();
    store.applySnapshot({
      ...snapshot(10),
      state: {
        ...snapshot(10).state,
        plots: [plotState({
          plotIndex: 6,
          plotId: 'plot-6',
          lockState: 'LOCKED',
          unlockOrder: 7,
          requiredLevel: 4,
          normalCoinPrice: '1200',
          unlockCandidate: true,
          unlockRuleVersion: '9',
        })],
      },
    });
    const result = store.applyChange({
      ...snapshot(11),
      partial: true,
      state: {
        ...snapshot(11).state,
        plots: [plotState({
          plotIndex: 6,
          plotId: 'plot-6',
          plotVersion: 2,
          lockState: 'UNLOCKED',
          unlockOrder: 7,
          requiredLevel: 4,
          normalCoinPrice: '1200',
          unlockCandidate: false,
          unlockRuleVersion: '9',
          status: 'EMPTY',
        })],
      },
    });
    expect(result).toBe('applied');
    expect(store.onFarm('player-a')?.state.plots).toEqual([
      expect.objectContaining({
        plotIndex: 6,
        plotId: 'plot-6',
        plotVersion: 2,
        lockState: 'UNLOCKED',
        unlockOrder: 7,
        requiredLevel: 4,
        normalCoinPrice: '1200',
        unlockCandidate: false,
        unlockRuleVersion: '9',
      }),
    ]);
  });

  it('clears crop-cycle-scoped watering and crop fields when Harvest projects an empty plot', () => {
    const store = new FarmStore();
    store.applySnapshot({
      ...snapshot(20),
      state: {
        ...snapshot(20).state,
        plots: [plotState({
          plotVersion: 4,
          status: 'MATURE',
          cropCycleId: 'cycle-watered-1',
          cropDefinitionId: 1,
          matureAt: 1_800_000_010_000,
          wateredAt: 1_800_000_000_000,
          harvestableCount: 240,
          stolenTotal: 10,
        })],
      },
    });
    const result = store.applyChange({
      ...snapshot(21),
      partial: true,
      state: {
        ...snapshot(21).state,
        plots: [plotState({ plotVersion: 5, status: 'EMPTY' })],
      },
    });
    expect(result).toBe('applied');
    const plot = store.onFarm('player-a')?.state.plots[0];
    expect(plot).toMatchObject({ status: 'EMPTY', plotVersion: 5 });
    expect(plot).not.toHaveProperty('cropCycleId');
    expect(plot).not.toHaveProperty('cropDefinitionId');
    expect(plot).not.toHaveProperty('matureAt');
    expect(plot).not.toHaveProperty('wateredAt');
    expect(plot).not.toHaveProperty('harvestableCount');
    expect(plot).not.toHaveProperty('stolenTotal');
  });

  it('rejects a same-revision PlayerBootstrap with different authoritative content', () => {
    const store = new FarmStore();
    store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 } });
    // 相同 revision 但内容不同:不得静默覆盖(保留已应用内容)。
    store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 999, inventory: {} });
    expect(store.getPrivateState()).toMatchObject({
      playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 },
    });
    // 相同 revision 且内容一致:no-op,不影响已应用内容。
    store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 } });
    expect(store.getPrivateState()).toMatchObject({
      playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 },
    });
  });

  it('reports whether a PlayerBootstrap was actually applied', () => {
    const store = new FarmStore();
    // 缺失 privateState:未应用。
    expect(store.applyPlayerBootstrap(undefined)).toBe(false);
    // 新 revision:写入成功。
    expect(store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 } })).toBe(true);
    // 旧 revision:拒绝。
    expect(store.applyPlayerBootstrap({ playerStateRevision: '4', wallet: 1, inventory: {} })).toBe(false);
    // 相同 revision 不同内容:拒绝(不覆盖)。
    expect(store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 999, inventory: {} })).toBe(false);
    // 相同 revision 相同内容:幂等一致,接受。
    expect(store.applyPlayerBootstrap({ playerStateRevision: '5', wallet: 100, inventory: { carrot: 2 } })).toBe(true);
    // 新 revision 刷新(资产操作后重查):写入成功。
    expect(store.applyPlayerBootstrap({ playerStateRevision: '6', wallet: 60, inventory: { carrot: 1 } })).toBe(true);
    expect(store.getPrivateState()).toMatchObject({
      playerStateRevision: '6', wallet: 60, inventory: { carrot: 1 },
    });
  });

  it('applies a Plant FarmChange atomically: version and plot delta land together', () => {
    // Q1 问题 B 快路径:current FarmVersion=0,收到 Plant FarmChange
    // (from=0 through=1, plot-1 GROWING plotVersion=2 cropCycleId=cycle-1)。
    const store = new FarmStore();
    store.applySnapshot(snapshot(0));
    const result = store.applyChange({
      ...snapshot(1),
      partial: true,
      state: {
        ...snapshot(1).state,
        plots: [
          plotState({ plotVersion: 2, status: 'GROWING', cropCycleId: 'cycle-1' }),
        ],
      },
    });
    expect(result).toBe('applied');
    const farm = store.onFarm('player-a');
    expect(farm?.farmVersion).toBe(1);
    expect(farm?.state.plots).toEqual([
      expect.objectContaining({
        plotId: 'plot-1',
        plotVersion: 2,
        status: 'GROWING',
        cropCycleId: 'cycle-1',
      }),
    ]);
    // Snapshot 上的其他地块不得被 partial delta 丢弃(fence 保留)。
    expect(farm?.state.inventory).toEqual({ seed: 2 });
  });

  it('rejects a FarmChange whose PlotDelta is malformed, keeping farmVersion untouched', () => {
    // Q1 问题 B fail closed:malformed PlotDelta(缺 plotId)→ 整体拒绝,
    // FarmVersion 仍为 0,由调用方触发 Fresh Join / Resync。
    const store = new FarmStore();
    store.applySnapshot(snapshot(0));
    const result = store.applyChange({
      ...snapshot(1),
      partial: true,
      state: {
        ...snapshot(1).state,
        plots: [
          plotState({ plotId: '', plotVersion: 2, status: 'GROWING' }),
        ],
      },
    });
    expect(result).toBe('gap');
    expect(store.onFarm('player-a')?.farmVersion).toBe(0);
    expect(store.onFarm('player-a')?.state.plots[0]).toMatchObject({ status: 'EMPTY', plotVersion: 1 });
  });

  it('validPlotDeltas rejects malformed lock state and offer/rule fields', () => {
    expect(validPlotDeltas([plotState({ plotVersion: 2, status: 'GROWING' })])).toBe(true);
    expect(validPlotDeltas([plotState({ plotId: '', plotVersion: 2, status: 'GROWING' })])).toBe(false);
    expect(validPlotDeltas([plotState({ lockState: undefined as never })])).toBe(false);
    expect(validPlotDeltas([plotState({ lockState: 'BROKEN' as never })])).toBe(false);
    expect(validPlotDeltas([plotState({ unlockCandidate: undefined as never })])).toBe(false);
    expect(validPlotDeltas([plotState({ normalCoinPrice: undefined as never })])).toBe(false);
    expect(validPlotDeltas([plotState({ unlockRuleVersion: 'not-a-number' })])).toBe(false);
    expect(validPlotDeltas([plotState({ plotVersion: undefined as never, status: 'GROWING' })])).toBe(false);
    expect(validPlotDeltas([plotState({ plotVersion: 2, status: 'HARVESTED' as never })])).toBe(false);
    expect(validPlotDeltas([])).toBe(true);
  });

  it('keeps Bootstrap profile/progression as authoritative display data', () => {
    const store = new FarmStore();
    store.applyPlayerBootstrap({
      playerStateRevision: '7',
      wallet: 30,
      inventory: {},
      profile: { playerId: '10001', displayName: 'Alice' },
      progression: { level: 3, exp: '120' },
    });
    expect(store.getPrivateState()).toMatchObject({
      profile: { playerId: '10001', displayName: 'Alice' },
      progression: { level: 3, exp: '120' },
    });
  });
});
