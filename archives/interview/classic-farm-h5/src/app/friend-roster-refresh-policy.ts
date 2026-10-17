export const FRIEND_ROSTER_FALLBACK_POLL_MS = 30_000;

export interface FriendRosterRefreshContext {
  hasFriendService: boolean;
  hasOwner: boolean;
  friendCount: number;
  connectionReady: boolean;
  requestsInFlight: number;
  pageVisible: boolean;
}

/**
 * The fallback poll only reconciles missed push hints. Primary refresh edges
 * are login Bootstrap, opening the friend sheet, visibility resume and
 * FriendOpportunityChanged.
 */
export const shouldRefreshFriendRoster = (context: FriendRosterRefreshContext): boolean =>
  context.hasFriendService
  && context.hasOwner
  && context.friendCount > 0
  && context.connectionReady
  && context.requestsInFlight === 0
  && context.pageVisible;
