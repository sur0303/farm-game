import { compareUint64 } from '../protocol/uint64';

export type CareTerminalStatus = 'SUCCEEDED' | 'NO_CHANGE' | 'REJECTED';
export type CareRewardRefreshAction = 'POLL' | 'CLEAR';

/**
 * A CARE terminal result only authorizes reward convergence when the backend
 * explicitly created an obligation. Missing is intentionally equivalent to
 * false: a stable SUCCEEDED/NO_CHANGE without that field has nothing to wait
 * for, and REJECTED must always clear the fence.
 */
export const careRewardRefreshAction = (
  status: CareTerminalStatus,
  rewardObligationCreated: boolean | undefined,
): CareRewardRefreshAction => (
  status !== 'REJECTED' && rewardObligationCreated === true ? 'POLL' : 'CLEAR'
);

export interface CareRewardWait {
  operationId: string;
  baselineRevision: string;
  connectionGeneration: number;
  attempts: number;
  inFlightBootstrapOperationId?: string;
  rewardPending: boolean;
}

/**
 * Bounded CARE reward/task convergence. A CARE result is a business fact, but
 * reward_obligation_created is not proof that the player balance settled.
 * This helper keeps the operation and connection fences together so an old
 * query cannot finish a newer wait.
 */
export class CareRewardRefresh {
  private waitState: CareRewardWait | null = null;

  constructor(private readonly maxAttempts = 3) {}

  begin(operationId: string, baselineRevision: string, connectionGeneration: number, rewardPending: boolean): void {
    this.waitState = {
      operationId,
      baselineRevision,
      connectionGeneration,
      attempts: 0,
      rewardPending,
    };
  }

  reset(): void {
    this.waitState = null;
  }

  /** Preserve the original operation/baseline across a transport recovery. */
  rebindConnectionGeneration(connectionGeneration: number): boolean {
    if (!this.waitState) return false;
    this.waitState = { ...this.waitState, connectionGeneration, inFlightBootstrapOperationId: undefined };
    return true;
  }

  get current(): CareRewardWait | null {
    return this.waitState ? { ...this.waitState } : null;
  }

  matches(operationId: string, connectionGeneration: number): boolean {
    return this.waitState?.operationId === operationId
      && this.waitState.connectionGeneration === connectionGeneration;
  }

  canStartBootstrap(connectionGeneration: number): boolean {
    const wait = this.waitState;
    return Boolean(
      wait
      && wait.connectionGeneration === connectionGeneration
      && wait.rewardPending
      && !wait.inFlightBootstrapOperationId
      && wait.attempts < this.maxAttempts,
    );
  }

  startBootstrap(operationId: string, connectionGeneration: number): boolean {
    if (!this.canStartBootstrap(connectionGeneration) || !this.waitState) return false;
    this.waitState = {
      ...this.waitState,
      attempts: this.waitState.attempts + 1,
      inFlightBootstrapOperationId: operationId,
    };
    return true;
  }

  claimBootstrap(operationId: string, connectionGeneration: number): CareRewardWait | null {
    const wait = this.waitState;
    if (!wait || wait.connectionGeneration !== connectionGeneration || wait.inFlightBootstrapOperationId !== operationId) return null;
    this.waitState = { ...wait, inFlightBootstrapOperationId: undefined };
    return { ...this.waitState };
  }

  setRewardPending(operationId: string, connectionGeneration: number, rewardPending: boolean): void {
    if (!this.matches(operationId, connectionGeneration) || !this.waitState) return;
    this.waitState = { ...this.waitState, rewardPending };
  }

  settledByRevision(revision: string, connectionGeneration: number): boolean {
    const wait = this.waitState;
    if (!wait || wait.connectionGeneration !== connectionGeneration || !wait.rewardPending) return false;
    if (compareUint64(revision, wait.baselineRevision) <= 0) return false;
    this.waitState = null;
    return true;
  }

  exhaust(connectionGeneration: number): boolean {
    if (!this.waitState || this.waitState.connectionGeneration !== connectionGeneration) return false;
    this.waitState = null;
    return true;
  }

  get pending(): boolean {
    return this.waitState !== null;
  }
}
