import { FriendActionMask, type FriendOpportunityStatus } from './friend-roster';

export interface FriendOpportunityPresentation {
  label: string;
  canTheft: boolean;
  canWater: boolean;
}

/** Render only product actions that the current H5 can actually perform. */
export const presentFriendOpportunity = (
  status: FriendOpportunityStatus | undefined,
  actionMask: number,
): FriendOpportunityPresentation => {
  if (status !== 'READY') {
    return { label: '互动机会查询中，可重试', canTheft: false, canWater: false };
  }
  const canTheft = (actionMask & FriendActionMask.THEFT) !== 0;
  const canWater = (actionMask & FriendActionMask.WATER) !== 0;
  const labels: string[] = [];
  if (canTheft) labels.push('可偷菜');
  if (canWater) labels.push('可浇水');
  if (labels.length === 0 && (actionMask & FriendActionMask.PEST) !== 0) labels.push('有其他互动');
  return {
    label: labels.length > 0 ? labels.join(' · ') : '暂无互动机会',
    canTheft,
    canWater,
  };
};

export type FriendOpportunityGain = 'THEFT' | 'WATER';

/** Compare two authority pulls; UNKNOWN transitions never manufacture gains. */
export const gainedFriendOpportunities = (
  previousReadyMask: number | undefined,
  nextStatus: FriendOpportunityStatus,
  nextMask: number,
): FriendOpportunityGain[] => {
  if (previousReadyMask === undefined || nextStatus !== 'READY') return [];
  const gained = nextMask & ~previousReadyMask;
  const result: FriendOpportunityGain[] = [];
  if ((gained & FriendActionMask.THEFT) !== 0) result.push('THEFT');
  if ((gained & FriendActionMask.WATER) !== 0) result.push('WATER');
  return result;
};
