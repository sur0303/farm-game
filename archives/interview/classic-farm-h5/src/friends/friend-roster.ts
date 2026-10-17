export const FriendActionMask = {
  THEFT: 1,
  WATER: 2,
  PEST: 4,
  VISIT: 8,
} as const;

export type FriendOpportunityStatus = 'READY' | 'UNKNOWN';

export interface FriendTheftTarget {
  plotId: number;
  cropCycleId: string;
}

export interface FriendSummary {
  ownerId: string;
  displayName: string;
  avatarId: number;
  level: number;
  relationId: string;
  relationEpoch: bigint;
  actionMask: number;
  opportunityStatus: FriendOpportunityStatus;
  opportunityRevision: bigint;
  theftEligibleTargets: FriendTheftTarget[];
  validatedAtMs: number;
}

export interface FriendRosterPage {
  friends: FriendSummary[];
  nextCursor: string;
  rosterRevision: bigint;
  validatedAtMs: number;
}

/**
 * The UI only consumes confirmed game-friend summaries.
 * The production implementation must read the server's friendship projection;
 * it must not derive access from a URL or from a WeChat contact list.
 */
export interface FriendRosterProvider {
  list(viewerId: string): Promise<FriendSummary[]>;
  listPage(viewerId: string, cursor?: string, pageSize?: number): Promise<FriendRosterPage>;
}
