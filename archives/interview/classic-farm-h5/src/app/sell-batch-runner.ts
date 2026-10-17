import type { TerminalOperationStatus, WaitForTerminalResult } from '../domain/command-service';

/** Public SELL accepts one positive count no larger than this server boundary. */
export const MAX_SELL_COUNT = 999;

export interface SellStock {
  itemId: number;
  quantity: number;
}

/** One immutable SELL command planned from the authoritative inventory at click time. */
export interface SellBatchTarget {
  itemId: number;
  count: number;
}

export type SellBatchStopReason = 'connection' | 'unresolved' | 'terminal-rejection';

export interface SellBatchResult {
  targetCount: number;
  itemCount: number;
  settledTargets: number;
  settledItems: number;
  stopReason?: SellBatchStopReason;
  terminalStatus?: TerminalOperationStatus;
}

export interface SellBatchRunnerOptions {
  targets: readonly SellBatchTarget[];
  isConnectionReady: () => boolean;
  send: (target: SellBatchTarget) => Promise<string | undefined>;
  waitForTerminal: (operationId: string, timeoutMs: number) => Promise<WaitForTerminalResult>;
  operationTimeoutMs: number;
}

/**
 * Freeze the exact inventory quantity visible at click time and split it into
 * server-valid commands. Invalid or empty stacks are ignored fail closed.
 */
export const freezeSellBatchTargets = (
  stock: readonly SellStock[],
  maxPerCommand = MAX_SELL_COUNT,
): SellBatchTarget[] => {
  if (!Number.isSafeInteger(maxPerCommand) || maxPerCommand < 1) {
    throw new Error('maxPerCommand must be a positive safe integer');
  }
  const targets: SellBatchTarget[] = [];
  const seenItems = new Set<number>();
  for (const entry of stock) {
    if (
      !Number.isInteger(entry.itemId)
      || entry.itemId <= 0
      || !Number.isSafeInteger(entry.quantity)
      || entry.quantity <= 0
      || seenItems.has(entry.itemId)
    ) continue;
    seenItems.add(entry.itemId);
    let remaining = entry.quantity;
    while (remaining > 0) {
      const count = Math.min(remaining, maxPerCommand);
      targets.push({ itemId: entry.itemId, count });
      remaining -= count;
    }
  }
  return targets;
};

/**
 * Submit at most one unresolved SELL at a time. Every next chunk waits for a
 * successful business terminal; a rejection, unknown result, timeout, or
 * disconnect stops the frozen batch and leaves Journal recovery authoritative.
 */
export const runSellBatch = async (options: SellBatchRunnerOptions): Promise<SellBatchResult> => {
  const itemCount = options.targets.reduce((sum, target) => sum + target.count, 0);
  let settledTargets = 0;
  let settledItems = 0;

  for (const target of options.targets) {
    if (!options.isConnectionReady()) {
      return { targetCount: options.targets.length, itemCount, settledTargets, settledItems, stopReason: 'connection' };
    }

    let operationId: string | undefined;
    try {
      operationId = await options.send(target);
    } catch {
      return { targetCount: options.targets.length, itemCount, settledTargets, settledItems, stopReason: 'unresolved' };
    }
    if (!operationId) {
      return { targetCount: options.targets.length, itemCount, settledTargets, settledItems, stopReason: 'unresolved' };
    }

    let terminal: WaitForTerminalResult;
    try {
      terminal = await options.waitForTerminal(operationId, options.operationTimeoutMs);
    } catch {
      return { targetCount: options.targets.length, itemCount, settledTargets, settledItems, stopReason: 'unresolved' };
    }
    if (terminal.outcome !== 'terminal') {
      return {
        targetCount: options.targets.length,
        itemCount,
        settledTargets,
        settledItems,
        stopReason: terminal.outcome === 'disconnected' ? 'connection' : 'unresolved',
      };
    }
    if (terminal.status !== 'SUCCEEDED') {
      return {
        targetCount: options.targets.length,
        itemCount,
        settledTargets,
        settledItems,
        stopReason: 'terminal-rejection',
        terminalStatus: terminal.status,
      };
    }
    settledTargets += 1;
    settledItems += target.count;
  }

  return { targetCount: options.targets.length, itemCount, settledTargets, settledItems };
};
