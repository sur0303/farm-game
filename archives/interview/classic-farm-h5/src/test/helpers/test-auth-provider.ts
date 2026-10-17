import type { AuthProvider, ConnectionTicket } from '../../auth/types';

/** Deterministic AuthProvider test double; it is never imported by an H5 build. */
export class TestAuthProvider implements AuthProvider {
  private issued = 0;

  constructor(private readonly playerId = 'player-a') {}

  async acquireConnectionTicket(): Promise<ConnectionTicket> {
    this.issued += 1;
    return {
      value: `test-ticket-${this.playerId}-${this.issued}`,
      playerId: this.playerId,
      sessionEpoch: `${this.issued}`,
      environment: 'test',
    };
  }
}
