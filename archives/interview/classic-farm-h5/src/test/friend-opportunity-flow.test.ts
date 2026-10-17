import { describe, expect, it } from 'vitest';
import { FriendOpportunityFlow } from '../app/friend-opportunity-flow';

describe('FriendOpportunityFlow', () => {
  it('rejects stale opportunity responses after a newer request starts', () => {
    const flow = new FriendOpportunityFlow();
    const oldRequest = flow.beginRequest();
    const currentRequest = flow.beginRequest();

    expect(flow.isCurrentRequest(oldRequest)).toBe(false);
    expect(flow.isCurrentRequest(currentRequest)).toBe(true);
  });

  it('lets an explicit navigation invalidate an in-flight recommendation', () => {
    const flow = new FriendOpportunityFlow();
    const request = flow.beginRequest();
    flow.invalidateRequests();

    expect(flow.isCurrentRequest(request)).toBe(false);
  });

  it('counts a friend visit only after SubscriptionReady', () => {
    const flow = new FriendOpportunityFlow();

    expect(flow.onSubscriptionReady('player-a', 'player-a')).toBe(false);
    expect(flow.onSubscriptionReady('player-b', 'player-a')).toBe(false);
    expect(flow.onSubscriptionReady('player-a', 'player-a')).toBe(true);
    expect(flow.onSubscriptionReady('player-a', 'player-a')).toBe(false);
  });

  it('invalidates old requests and visit markers on session reset', () => {
    const flow = new FriendOpportunityFlow();
    const oldRequest = flow.beginRequest();
    flow.onSubscriptionReady('player-b', 'player-a');
    flow.reset();

    expect(flow.isCurrentRequest(oldRequest)).toBe(false);
    expect(flow.onSubscriptionReady('player-a', 'player-a')).toBe(false);
  });
});
