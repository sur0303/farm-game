import { describe, expect, it } from 'vitest';
import {
  careRewardRefreshAction,
  CareRewardRefresh,
} from '../app/care-reward-refresh';

describe('CARE reward refresh fencing', () => {
  it('does not settle on an equal revision', () => {
    const refresh = new CareRewardRefresh(3);
    refresh.begin('care-1', '10', 4, true);

    expect(refresh.settledByRevision('10', 4)).toBe(false);
    expect(refresh.pending).toBe(true);
    expect(refresh.current?.baselineRevision).toBe('10');
  });

  it('settles and clears the wait on a higher revision', () => {
    const refresh = new CareRewardRefresh(3);
    refresh.begin('care-1', '10', 4, true);

    expect(refresh.settledByRevision('11', 4)).toBe(true);
    expect(refresh.pending).toBe(false);
  });

  it('fences old generations and operation ids', () => {
    const refresh = new CareRewardRefresh(3);
    refresh.begin('care-1', '10', 4, true);

    expect(refresh.matches('care-old', 4)).toBe(false);
    expect(refresh.matches('care-1', 3)).toBe(false);
    expect(refresh.startBootstrap('bootstrap-1', 3)).toBe(false);
    expect(refresh.startBootstrap('bootstrap-old', 4)).toBe(true);
    expect(refresh.claimBootstrap('bootstrap-wrong', 4)).toBeNull();
    expect(refresh.claimBootstrap('bootstrap-1', 3)).toBeNull();
    expect(refresh.current?.inFlightBootstrapOperationId).toBe('bootstrap-old');
    expect(refresh.claimBootstrap('bootstrap-old', 4)?.operationId).toBe('care-1');
  });

  it('rebinds the original baseline after a lost result reconnect', () => {
    const refresh = new CareRewardRefresh(3);
    refresh.begin('care-lost', '10', 4, false);
    expect(refresh.startBootstrap('stale-bootstrap', 4)).toBe(false);
    expect(refresh.rebindConnectionGeneration(5)).toBe(true);
    expect(refresh.matches('care-lost', 5)).toBe(true);
    expect(refresh.current?.baselineRevision).toBe('10');
    refresh.setRewardPending('care-lost', 5, true);
    expect(refresh.settledByRevision('11', 5)).toBe(true);
  });

  it('exhausts exactly the configured bootstrap attempt budget', () => {
    const refresh = new CareRewardRefresh(3);
    refresh.begin('care-1', '10', 4, true);

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      expect(refresh.startBootstrap(`bootstrap-${attempt}`, 4)).toBe(true);
      expect(refresh.claimBootstrap(`bootstrap-${attempt}`, 4)?.attempts).toBe(attempt);
    }
    expect(refresh.canStartBootstrap(4)).toBe(false);
    expect(refresh.exhaust(4)).toBe(true);
    expect(refresh.pending).toBe(false);
  });

  it('clears legal no-obligation and rejected CARE outcomes without polling', () => {
    expect(careRewardRefreshAction('SUCCEEDED', false)).toBe('CLEAR');
    expect(careRewardRefreshAction('NO_CHANGE', undefined)).toBe('CLEAR');
    expect(careRewardRefreshAction('REJECTED', true)).toBe('CLEAR');
    expect(careRewardRefreshAction('SUCCEEDED', true)).toBe('POLL');
    expect(careRewardRefreshAction('NO_CHANGE', true)).toBe('POLL');
  });
});
