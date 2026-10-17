import { describe, expect, it } from 'vitest';
import type { PlotState } from '../protocol/types';
import { freezeBatchTargets, runPlotBatch } from '../app/batch-command-runner';

const plot = (plotIndex: number, cropCycleId: string, plotVersion = '1'): PlotState => ({
  plotIndex,
  lockState: 'UNLOCKED',
  unlockOrder: 0,
  requiredLevel: 0,
  normalCoinPrice: '0',
  unlockCandidate: false,
  unlockRuleVersion: '1',
  plotId: `plot-${plotIndex}`,
  plotVersion,
  status: 'MATURE',
  cropCycleId,
  cropDefinitionId: 1,
  harvestableCount: 1,
  stolenTotal: 0,
});

const terminal = (operationId: string) => ({
  operationId,
  outcome: 'terminal' as const,
  status: 'SUCCEEDED' as const,
});

describe('plot batch command runner', () => {
  it('runs 24 persistent-MATURE THEFT targets once with max in-flight one', async () => {
    const plots = Array.from({ length: 24 }, (_, index) => plot(index, `cycle-${index}`));
    const targets = freezeBatchTargets(plots, () => true);
    const sent: Array<{ plotIndex: number; cropCycleId: string }> = [];
    let inFlight = 0;
    let maxInFlight = 0;
    let nextOperation = 0;

    const result = await runPlotBatch({
      targets,
      readLatest: (target) => plots.find((candidate) => candidate.plotIndex === target.plotIndex),
      isOperable: () => true,
      isConnectionReady: () => true,
      send: async (target, current) => {
        inFlight += 1;
        maxInFlight = Math.max(maxInFlight, inFlight);
        sent.push({ plotIndex: current.plotIndex, cropCycleId: target.cropCycleId });
        await Promise.resolve();
        inFlight -= 1;
        return { kind: 'sent', operationId: `op-${nextOperation++}` };
      },
      waitForTerminal: async (operationId) => terminal(operationId),
      operationTimeoutMs: 1_000,
    });

    expect(result).toMatchObject({ targetCount: 24, settled: 24, skipped: 0 });
    expect(result.stopReason).toBeUndefined();
    expect(maxInFlight).toBe(1);
    expect(sent).toHaveLength(24);
    expect(new Set(sent.map((target) => `${target.plotIndex}\u0000${target.cropCycleId}`)).size).toBe(24);
  });

  it('deduplicates the same plot and crop cycle even when the initial view repeats it', () => {
    const targets = freezeBatchTargets([
      plot(2, 'cycle-same', '4'),
      plot(2, 'cycle-same', '5'),
      plot(2, 'cycle-next', '6'),
    ], () => true);

    expect(targets).toEqual([
      { plotIndex: 2, cropCycleId: 'cycle-same' },
      { plotIndex: 2, cropCycleId: 'cycle-next' },
    ]);
  });

  it.each([
    ['unknown', 'unresolved'],
    ['timeout', 'unresolved'],
    ['disconnected', 'connection'],
  ] as const)('stops after the first %s and does not send a later target', async (waitOutcome, expectedReason) => {
    const plots = [plot(0, 'cycle-0'), plot(1, 'cycle-1'), plot(2, 'cycle-2')];
    const targets = freezeBatchTargets(plots, () => true);
    const sent: string[] = [];
    const result = await runPlotBatch({
      targets,
      readLatest: (target) => plots.find((candidate) => candidate.plotIndex === target.plotIndex),
      isOperable: () => true,
      isConnectionReady: () => true,
      send: async (target) => {
        sent.push(target.cropCycleId);
        return { kind: 'sent', operationId: `op-${sent.length}` };
      },
      waitForTerminal: async (operationId) => ({ operationId, outcome: waitOutcome }),
      operationTimeoutMs: 1,
    });

    expect(sent).toEqual(['cycle-0']);
    expect(result.settled).toBe(0);
    expect(result.stopReason).toBe(expectedReason);
  });

  it('reads the latest FarmChange plot version before sending the next target', async () => {
    const initial = [plot(0, 'cycle-0', '1'), plot(1, 'cycle-1', '1')];
    const current = new Map(initial.map((candidate) => [candidate.plotIndex, candidate]));
    const targets = freezeBatchTargets(initial, () => true);
    const sentVersions: string[] = [];

    const result = await runPlotBatch({
      targets,
      readLatest: (target) => current.get(target.plotIndex),
      isOperable: () => true,
      isConnectionReady: () => true,
      send: async (_target, latest) => {
        sentVersions.push(String(latest.plotVersion));
        return { kind: 'sent', operationId: `op-${sentVersions.length}` };
      },
      waitForTerminal: async (operationId) => {
        if (operationId === 'op-1') current.set(1, plot(1, 'cycle-1', '9'));
        return terminal(operationId);
      },
      operationTimeoutMs: 1_000,
    });

    expect(result.settled).toBe(2);
    expect(sentVersions).toEqual(['1', '9']);
  });
});
