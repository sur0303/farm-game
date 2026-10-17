/**
 * Coordinates UI-only friend-opportunity requests and visit navigation.
 * Public API payloads and server-side opportunity adjudication remain outside
 * this class.
 */
export class FriendOpportunityFlow {
  private requestGeneration = 0;
  private visitedFriendSinceHome = false;

  beginRequest(): number {
    this.requestGeneration += 1;
    return this.requestGeneration;
  }

  isCurrentRequest(generation: number): boolean {
    return generation === this.requestGeneration;
  }

  invalidateRequests(): void {
    this.requestGeneration += 1;
  }

  /** Invalidates old responses without reusing a previous generation. */
  reset(): void {
    this.invalidateRequests();
    this.visitedFriendSinceHome = false;
  }

  /**
   * A visit counts only after the target subscription reaches Ready. Returning
   * home consumes that visit marker exactly once and asks the caller to pull
   * the next recommendation.
   */
  onSubscriptionReady(ownerId: string, ownOwnerId: string): boolean {
    if (ownerId !== ownOwnerId) {
      this.visitedFriendSinceHome = true;
      return false;
    }
    if (!this.visitedFriendSinceHome) return false;
    this.visitedFriendSinceHome = false;
    return true;
  }
}
