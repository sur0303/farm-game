import { describe, expect, it } from 'vitest';
import {
  FRIEND_ROSTER_FALLBACK_POLL_MS,
  shouldRefreshFriendRoster,
} from '../app/friend-roster-refresh-policy';

const readyContext = {
  hasFriendService: true,
  hasOwner: true,
  friendCount: 20,
  connectionReady: true,
  requestsInFlight: 0,
  pageVisible: true,
};

describe('friend roster refresh policy', () => {
  it('uses a low-frequency fallback instead of the former five-second poll', () => {
    expect(FRIEND_ROSTER_FALLBACK_POLL_MS).toBe(30_000);
  });

  it('refreshes only for a visible ready session with known friends', () => {
    expect(shouldRefreshFriendRoster(readyContext)).toBe(true);
    expect(shouldRefreshFriendRoster({ ...readyContext, friendCount: 0 })).toBe(false);
    expect(shouldRefreshFriendRoster({ ...readyContext, connectionReady: false })).toBe(false);
    expect(shouldRefreshFriendRoster({ ...readyContext, pageVisible: false })).toBe(false);
  });

  it('does not overlap an existing roster request', () => {
    expect(shouldRefreshFriendRoster({ ...readyContext, requestsInFlight: 1 })).toBe(false);
  });
});
