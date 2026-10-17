import { parseConnectionTicket, type AuthProvider, type ConnectionTicket } from './types';

export interface LaunchTicketSource {
  getLaunchTicket(): string | Promise<string>;
  consumeLaunchTicket?(): void;
}

export class QueryLaunchTicketSource implements LaunchTicketSource {
  constructor(private readonly href = window.location.href) {}

  getLaunchTicket(): string {
    const ticket = new URL(this.href).searchParams.get('launch_ticket');
    if (!ticket) throw new Error('微信小程序未交接 launch_ticket');
    return ticket;
  }

  consumeLaunchTicket(): void {
    if (typeof window !== 'undefined' && window.location.href === this.href && window.history?.replaceState) {
      const url = new URL(this.href);
      url.searchParams.delete('launch_ticket');
      window.history.replaceState(window.history.state, document.title, url.toString());
    }
  }
}

export class WeChatAuthProvider implements AuthProvider {
  private exchanged = false;
  private exchangeAttempted = false;
  private freshTicketUnavailable = false;
  private inFlight: Promise<ConnectionTicket> | null = null;

  constructor(
    private readonly endpoint = '/api/v1/auth/exchange',
    private readonly source: LaunchTicketSource = new QueryLaunchTicketSource(),
    private readonly renewEndpoint = '/api/v1/auth/renew',
  ) {}

  get requiresFreshLaunchTicket(): boolean {
    return !this.exchanged && (this.exchangeAttempted || this.freshTicketUnavailable);
  }

  async acquireConnectionTicket(signal?: AbortSignal): Promise<ConnectionTicket> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.acquireConnectionTicketOnce(signal);
    try {
      return await this.inFlight;
    } finally {
      this.inFlight = null;
    }
  }

  private async acquireConnectionTicketOnce(signal?: AbortSignal): Promise<ConnectionTicket> {
    if (this.exchanged || this.exchangeAttempted) return this.renewConnectionTicket(signal);

    let launchTicket: string;
    try {
      launchTicket = await this.source.getLaunchTicket();
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes('launch_ticket')) throw error;
      this.freshTicketUnavailable = true;
      try {
        const connectionTicket = await this.renewConnectionTicket(signal);
        this.exchanged = true;
        return connectionTicket;
      } catch {
        throw error;
      }
    }

    // launch_ticket is one-time. Once exchange starts, never replay it after
    // a network or response-parsing failure; a later call can only use renew.
    this.exchangeAttempted = true;
    this.freshTicketUnavailable = false;
    this.source.consumeLaunchTicket?.();
    const response = await fetch(this.endpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ launchTicket }),
    });
    if (!response.ok) throw new Error(`微信登录交接失败: ${response.status}`);
    const connectionTicket = parseConnectionTicket(await response.json());
    this.exchanged = true;
    return connectionTicket;
  }

  private async renewConnectionTicket(signal?: AbortSignal): Promise<ConnectionTicket> {
    const response = await fetch(this.renewEndpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    if (!response.ok) throw new Error(`微信连接票据续发失败: ${response.status}`);
    return parseConnectionTicket(await response.json());
  }
}
