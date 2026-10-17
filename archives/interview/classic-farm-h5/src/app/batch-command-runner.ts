import type { PlotState } from '../protocol/types';
import type { WaitForTerminalResult } from '../domain/command-service';

/** A batch target is frozen at click time and never re-discovered mid-run. */
export interface BatchTarget {
  plotIndex: number;
  cropCycleId: string;
}

export type BatchSendResult =
  | { kind: 'sent'; operationId: string }
  | { kind: 'skipped' }
  | { kind: 'failed' };

export type BatchStopReason = 'connection' | 'unresolved';

export interface BatchRunResult {
  targetCount: number;
  settled: number;
  skipped: number;
  stopReason?: BatchStopReason;
}

export interface PlotBatchRunnerOptions {
  targets: readonly BatchTarget[];
  readLatest: (target: BatchTarget) => PlotState | undefined;
  isOperable: (target: BatchTarget, plot: PlotState) => boolean;
  isConnectionReady: () => boolean;
  send: (target: BatchTarget, plot: PlotState) => Promise<BatchSendResult>;
  waitForTerminal: (operationId: string, timeoutMs: number) => Promise<WaitForTerminalResult>;
  operationTimeoutMs: number;
}

/**
 * Freeze one unique target per (plotIndex, cropCycleId). The caller supplies
 * the initial authoritative PlotView eligibility; no later farm scan can add
 * another operation to this batch.
 */
export const freezeBatchTargets = (
  plots: readonly PlotState[],
  isEligible: (plot: PlotState) => boolean,
): BatchTarget[] => {
  const seen = new Set<string>();
  const targets: BatchTarget[] = [];
  for (const plot of plots) {
    if (!isEligible(plot) || typeof plot.cropCycleId !== 'string' || plot.cropCycleId.length === 0) continue;
    const key = `${plot.plotIndex}\u0000${plot.cropCycleId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    targets.push({ plotIndex: plot.plotIndex, cropCycleId: plot.cropCycleId });
  }
  return targets;
};

/**
 * Run a frozen plot batch with exactly one unresolved business operation at a
 * time. A missing/stale target is skipped; an unknown, timed-out, failed, or
 * disconnected operation stops the batch and leaves recovery to the Journal.
 */
export const runPlotBatch = async (options: PlotBatchRunnerOptions): Promise<BatchRunResult> => {
  let settled = 0;
  let skipped = 0;

  for (const target of options.targets) {
    if (!options.isConnectionReady()) {
      return { targetCount: options.targets.length, settled, skipped, stopReason: 'connection' };
    }

    const latest = options.readLatest(target);
    if (!latest || latest.plotIndex !== target.plotIndex || latest.cropCycleId !== target.cropCycleId) {
      skipped += 1;
      continue;
    }
    if (!options.isOperable(target, latest)) {
      skipped += 1;
      continue;
    }

    let sent: BatchSendResult;
    try {
      sent = await options.send(target, latest);
    } catch {
      return { targetCount: options.targets.length, settled, skipped, stopReason: 'unresolved' };
    }
    if (sent.kind === 'skipped') {
      skipped += 1;
      continue;
    }
    if (sent.kind !== 'sent') {
      return { targetCount: options.targets.length, settled, skipped, stopReason: 'unresolved' };
    }

    let terminal: WaitForTerminalResult;
    try {
      terminal = await options.waitForTerminal(sent.operationId, options.operationTimeoutMs);
    } catch {
      return { targetCount: options.targets.length, settled, skipped, stopReason: 'unresolved' };
    }
    if (terminal.outcome !== 'terminal') {
      return {
        targetCount: options.targets.length,
        settled,
        skipped,
        stopReason: terminal.outcome === 'disconnected' ? 'connection' : 'unresolved',
      };
    }
    settled += 1;
  }

  return { targetCount: options.targets.length, settled, skipped };
};
