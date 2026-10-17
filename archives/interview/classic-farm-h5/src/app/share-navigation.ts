import type { ConnectionStatus, JoinPhase } from '../network/ws-client';

export interface AcceptedShareNavigationState {
  connectionStatus: ConnectionStatus;
  joinPhase: JoinPhase;
  ownOwnerId: string;
  currentOwnerId: string;
  pendingOwnerId: string | null;
  friendKnown: boolean;
}

/**
 * An accepted share may finish while the mandatory own-farm recovery barrier
 * is still open. A friend Join is safe only after the own subscription is
 * fully Ready and WsClient has promoted the connection to InFarm.
 */
export const canNavigateAcceptedShareNow = (state: AcceptedShareNavigationState): boolean =>
  state.connectionStatus === 'InFarm'
  && state.joinPhase === 'READY'
  && state.ownOwnerId !== ''
  && state.currentOwnerId === state.ownOwnerId
  && state.pendingOwnerId !== null
  && state.pendingOwnerId !== state.ownOwnerId
  && state.friendKnown;
