import { describe, expect, it } from 'vitest';
import {
  freezeSellBatchTargets,
  MAX_SELL_COUNT,
  runSellBatch,
} from '../app/sell-batch-runner';

const succeeded = (operationId: string) => ({
  operationId,
  outcome: 'terminal' as const,
  status: 'SUCCEEDED' as const,
});

describe('sell batch runner', () => {
  it('freezes a stack above the public boundary into exact bounded chunks', () => {
    expect(freezeSellBatchTargets([
      { itemId: 2001, quantity: 2_001 },
      { itemId: 2002, quantity: 999 },
    ])).toEqual([
      { itemId: 2001, count: MAX_SELL_COUNT },
      { itemId: 2001, count: MAX_SELL_COUNT },
      { itemId: 2001, count: 3 },
      { itemId: 2002, count: MAX_SELL_COUNT },
    ]);
  });

  it('ignores malformed and duplicate inventory stacks fail closed', () => {
    expect(freezeSellBatchTargets([
      { itemId: 0, quantity: 3 },
      { itemId: 2001, quantity: -1 },
      { itemId: 2001, quantity: 4 },
      { itemId: 2001, quantity: 9 },
      { itemId: 2002, quantity: Number.MAX_SAFE_INTEGER + 1 },
    ])).toEqual([{ itemId: 2001, count: 4 }]);
    expect(() => freezeSellBatchTargets([], 0)).toThrow('maxPerCommand');
  });

  it('waits for each successful business terminal with max in-flight one', async () => {
    const targets = freezeSellBatchTargets([{ itemId: 2001, quantity: 2_001 }]);
    const sent: Array<{ itemId: number; count: number }> = [];
    let unresolved = 0;
    let maxUnresolved = 0;

    const result = await runSellBatch({
      targets,
      isConnectionReady: () => true,
      send: async (target) => {
        unresolved += 1;
        maxUnresolved = Math.max(maxUnresolved, unresolved);
        sent.push(target);
        return `op-${sent.length}`;
      },
      waitForTerminal: async (operationId) => {
        unresolved -= 1;
        return succeeded(operationId);
      },
      operationTimeoutMs: 1_000,
    });

    expect(maxUnresolved).toBe(1);
    expect(sent).toEqual(targets);
    expect(result).toEqual({
      targetCount: 3,
      itemCount: 2_001,
      settledTargets: 3,
      settledItems: 2_001,
    });
  });

  it.each([
    ['unknown', 'unresolved'],
    ['timeout', 'unresolved'],
    ['disconnected', 'connection'],
  ] as const)('stops after a %s result without sending another chunk', async (outcome, stopReason) => {
    let sends = 0;
    const result = await runSellBatch({
      targets: freezeSellBatchTargets([{ itemId: 2001, quantity: 1_001 }]),
      isConnectionReady: () => true,
      send: async () => `op-${++sends}`,
      waitForTerminal: async (operationId) => ({ operationId, outcome }),
      operationTimeoutMs: 1,
    });

    expect(sends).toBe(1);
    expect(result).toMatchObject({ settledTargets: 0, settledItems: 0, stopReason });
  });

  it.each(['REJECTED', 'NO_CHANGE', 'UNAVAILABLE'] as const)(
    'stops after terminal %s and reports the authoritative status',
    async (status) => {
      let sends = 0;
      const result = await runSellBatch({
        targets: freezeSellBatchTargets([{ itemId: 2001, quantity: 1_001 }]),
        isConnectionReady: () => true,
        send: async () => `op-${++sends}`,
        waitForTerminal: async (operationId) => ({ operationId, outcome: 'terminal', status }),
        operationTimeoutMs: 1_000,
      });

      expect(sends).toBe(1);
      expect(result).toMatchObject({
        settledTargets: 0,
        settledItems: 0,
        stopReason: 'terminal-rejection',
        terminalStatus: status,
      });
    },
  );

  it('does not send after the connection leaves InFarm between chunks', async () => {
    let ready = true;
    let sends = 0;
    const result = await runSellBatch({
      targets: freezeSellBatchTargets([{ itemId: 2001, quantity: 1_001 }]),
      isConnectionReady: () => ready,
      send: async () => `op-${++sends}`,
      waitForTerminal: async (operationId) => {
        ready = false;
        return succeeded(operationId);
      },
      operationTimeoutMs: 1_000,
    });

    expect(sends).toBe(1);
    expect(result).toMatchObject({ settledTargets: 1, settledItems: 999, stopReason: 'connection' });
  });
});
