import { describe, expect, it, vi } from 'vitest';
import { parseConnectionTicket } from '../auth/types';
import { StagingAuthError, StagingAuthProvider } from '../auth/staging-auth-provider';
import { QueryLaunchTicketSource, WeChatAuthProvider } from '../auth/wechat-auth-provider';

const ticket = {
  value: 'one-time-ticket',
  playerId: 'player-a',
  sessionEpoch: 'epoch-1',
  environment: 'staging',
};

describe('auth providers', () => {
  it('normalizes the canonical login response and preserves the server WSS endpoint', () => {
    expect(parseConnectionTicket({
      connection_ticket: 'canonical-ticket',
      player_id: 'player-a',
      active_session_epoch: 'epoch-7',
      wss_endpoint: 'wss://farm.example/game',
    })).toEqual({
      value: 'canonical-ticket',
      playerId: 'player-a',
      sessionEpoch: 'epoch-7',
      environment: 'web',
      wsUrl: 'wss://farm.example/game',
    });
  });

  it('validates and returns a staging connection ticket', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ({ ok: true, json: async () => ticket }));
    vi.stubGlobal('fetch', fetchMock);
    await expect(new StagingAuthProvider('/api/session', 'demo-platform-token').acquireConnectionTicket()).resolves.toEqual(ticket);
    expect(JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string)).toMatchObject({client_build: 'farm-h5/0.2'});
  });

  it('does not invent a staging Demo Token when no test token is supplied', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    await expect(new StagingAuthProvider('/api/session').acquireConnectionTicket()).rejects.toThrow('platform token is not configured');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('maps a staging network failure to a stable authentication error', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw new TypeError('network unreachable');
    }));

    const request = new StagingAuthProvider('/api/session', 'demo-platform-token').acquireConnectionTicket();
    await expect(request).rejects.toBeInstanceOf(StagingAuthError);
    await expect(request).rejects.toMatchObject({
      name: 'StagingAuthError',
      kind: 'network',
      message: 'staging login request failed',
    });
  });

  it('maps an invalid staging JSON response without leaking a parser exception', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true,
      json: async () => {
        throw new SyntaxError('unexpected token');
      },
    })));

    const request = new StagingAuthProvider('/api/session', 'demo-platform-token').acquireConnectionTicket();
    await expect(request).rejects.toBeInstanceOf(StagingAuthError);
    await expect(request).rejects.toMatchObject({
      name: 'StagingAuthError',
      kind: 'invalid_response',
      message: 'staging login returned invalid JSON',
    });
  });

  it('exchanges a WeChat launch ticket without exposing platform secrets', async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => ({ ...ticket, environment: 'wechat' }) }));
    vi.stubGlobal('fetch', fetchMock);
    const consumeLaunchTicket = vi.fn();
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => 'launch-ticket-1', consumeLaunchTicket });
    await expect(provider.acquireConnectionTicket()).resolves.toMatchObject({ environment: 'wechat' });
    expect(provider.requiresFreshLaunchTicket).toBe(false);
    expect(fetchMock).toHaveBeenCalledWith('/api/exchange', expect.objectContaining({
      body: JSON.stringify({ launchTicket: 'launch-ticket-1' }),
    }));
    expect(consumeLaunchTicket).toHaveBeenCalledTimes(1);
  });

  it('consumes the WeChat launch ticket before an exchange failure so it cannot be replayed', async () => {
    const consumeLaunchTicket = vi.fn();
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: false, status: 503 })));
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => 'launch-ticket-1', consumeLaunchTicket });
    await expect(provider.acquireConnectionTicket()).rejects.toThrow('503');
    expect(consumeLaunchTicket).toHaveBeenCalledTimes(1);
    expect(provider.requiresFreshLaunchTicket).toBe(true);
  });

  it('renews a connection ticket after the initial WeChat exchange', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ ...ticket, environment: 'wechat' }) })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ ...ticket, value: 'renewed-ticket', environment: 'wechat' }) });
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => 'launch-ticket-1' }, '/api/renew');

    await expect(provider.acquireConnectionTicket()).resolves.toMatchObject({ value: 'one-time-ticket' });
    await expect(provider.acquireConnectionTicket()).resolves.toMatchObject({ value: 'renewed-ticket' });
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/renew');
  });

  it('renews after a page refresh when the launch ticket was already removed', async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => ({ ...ticket, value: 'renewed-after-refresh', environment: 'wechat' }) }));
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => { throw new Error('寰俊灏忕▼搴忔湭浜ゆ帴 launch_ticket'); } }, '/api/renew');

    await expect(provider.acquireConnectionTicket()).resolves.toMatchObject({ value: 'renewed-after-refresh' });
    expect(fetchMock).toHaveBeenCalledWith('/api/renew', expect.objectContaining({ credentials: 'include' }));
  });

  it('shares one in-flight exchange between concurrent callers', async () => {
    let resolveResponse: ((value: { ok: boolean; json: () => Promise<typeof ticket> }) => void) | undefined;
    const fetchMock = vi.fn(() => new Promise<{ ok: boolean; json: () => Promise<typeof ticket> }>((resolve) => {
      resolveResponse = resolve;
    }));
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => 'launch-ticket-1' });

    const first = provider.acquireConnectionTicket();
    const second = provider.acquireConnectionTicket();
    await Promise.resolve();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    resolveResponse?.({ ok: true, json: async () => ({ ...ticket, environment: 'wechat' }) });
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
  });

  it('does not replay a launch ticket after a malformed successful exchange response', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ value: 'invalid' }) })
      .mockResolvedValueOnce({ ok: false, status: 401 });
    vi.stubGlobal('fetch', fetchMock);
    const consumeLaunchTicket = vi.fn();
    const provider = new WeChatAuthProvider('/api/exchange', { getLaunchTicket: () => 'launch-ticket-1', consumeLaunchTicket }, '/api/renew');

    await expect(provider.acquireConnectionTicket()).rejects.toThrow('playerId');
    await expect(provider.acquireConnectionTicket()).rejects.toThrow('401');
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(['/api/exchange', '/api/renew']);
    expect(consumeLaunchTicket).toHaveBeenCalledTimes(1);
  });

  it('rejects a malformed ticket response', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, json: async () => ({ value: 'missing-player' }) })));
    await expect(new StagingAuthProvider('/api/session', 'demo-platform-token').acquireConnectionTicket()).rejects.toThrow('playerId');
  });

  it('reads a launch ticket from an injected WebView URL', () => {
    const source = new QueryLaunchTicketSource('https://farm.example.com/game?launch_ticket=one-time&scene=friend');
    expect(source.getLaunchTicket()).toBe('one-time');
  });
});

