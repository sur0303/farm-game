import { describe, expect, it } from 'vitest';
import { FriendActionMask } from '../friends/friend-roster';
import { gainedFriendOpportunities, presentFriendOpportunity } from '../friends/opportunity-presentation';

describe('friend opportunity presentation', () => {
  it('shows explicit product action labels instead of one aggregate hint', () => {
    expect(presentFriendOpportunity('READY', FriendActionMask.THEFT)).toEqual({
      label: '可偷菜', canTheft: true, canWater: false,
    });
    expect(presentFriendOpportunity('READY', FriendActionMask.THEFT | FriendActionMask.WATER).label).toBe('可偷菜 · 可浇水');
    expect(presentFriendOpportunity('READY', FriendActionMask.WATER).label).toBe('可浇水');
    expect(presentFriendOpportunity('READY', FriendActionMask.VISIT).label).toBe('暂无互动机会');
    expect(presentFriendOpportunity('UNKNOWN', FriendActionMask.THEFT).label).toBe('互动机会查询中，可重试');
  });

  it('notifies only newly gained authority actions after a prior Ready baseline', () => {
    expect(gainedFriendOpportunities(undefined, 'READY', FriendActionMask.THEFT)).toEqual([]);
    expect(gainedFriendOpportunities(0, 'READY', FriendActionMask.THEFT)).toEqual(['THEFT']);
    expect(gainedFriendOpportunities(FriendActionMask.THEFT, 'READY', FriendActionMask.THEFT)).toEqual([]);
    expect(gainedFriendOpportunities(FriendActionMask.THEFT, 'READY', FriendActionMask.THEFT | FriendActionMask.WATER)).toEqual(['WATER']);
    expect(gainedFriendOpportunities(0, 'UNKNOWN', FriendActionMask.THEFT)).toEqual([]);
  });
});
