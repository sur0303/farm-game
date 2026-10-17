import { describe, expect, it } from 'vitest';
import { canNavigateAcceptedShareNow, type AcceptedShareNavigationState } from '../app/share-navigation';

const ready: AcceptedShareNavigationState = {
  connectionStatus: 'InFarm',
  joinPhase: 'READY',
  ownOwnerId: '10001',
  currentOwnerId: '10001',
  pendingOwnerId: '10002',
  friendKnown: true,
};

describe('accepted share navigation fence', () => {
  it('joins only after the own subscription is fully interactive', () => {
    expect(canNavigateAcceptedShareNow(ready)).toBe(true);
    expect(canNavigateAcceptedShareNow({ ...ready, connectionStatus: 'Recovering' })).toBe(false);
    expect(canNavigateAcceptedShareNow({ ...ready, connectionStatus: 'Joining', joinPhase: 'CATCHING_UP' })).toBe(false);
    expect(canNavigateAcceptedShareNow({ ...ready, connectionStatus: 'InFarm', joinPhase: 'CATCHING_UP' })).toBe(false);
  });

  it('rejects stale, self, unknown, and non-home share targets', () => {
    expect(canNavigateAcceptedShareNow({ ...ready, pendingOwnerId: null })).toBe(false);
    expect(canNavigateAcceptedShareNow({ ...ready, pendingOwnerId: '10001' })).toBe(false);
    expect(canNavigateAcceptedShareNow({ ...ready, friendKnown: false })).toBe(false);
    expect(canNavigateAcceptedShareNow({ ...ready, currentOwnerId: '10003' })).toBe(false);
  });
});
