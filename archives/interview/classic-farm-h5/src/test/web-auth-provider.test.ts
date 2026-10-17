import { describe, expect, it, vi } from 'vitest';
import { WebAuthProvider, type StorageLike } from '../auth/web-auth-provider';

class MemoryStorage implements StorageLike {
  private readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

const ticketResponse = (ticket = 'web-ticket') => ({
  ok: true,
  status: 200,
  json: async () => ({
    connection_ticket: ticket,
    player_id: '1000001',
    active_session_epoch: '1',
    wss_endpoint: 'wss://farm.example/game',
  }),
}) as Response;

describe('web auth provider', () => {
  it('registers with explicit credentials and never writes the password to storage', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ticketResponse());
    vi.stubGlobal('fetch', fetchMock);
    const storage = new MemoryStorage();
    const provider = new WebAuthProvider('/api/v1/session/login', {
      registerEndpoint: '/api/v1/accounts/register',
      renewEndpoint: '/api/v1/auth/renew',
      storage,
      deviceId: 'web-device-1',
    });
    provider.prepareCredentials('register', 'Farmer_One', 'correct horse battery');

    await expect(provider.acquireConnectionTicket()).resolves.toMatchObject({
      value: 'web-ticket',
      playerId: '1000001',
      wsUrl: 'wss://farm.example/game',
    });
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/accounts/register', expect.objectContaining({
      credentials: 'include',
    }));
    const body = JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string) as Record<string, string>;
    expect(body).toEqual({
      username: 'Farmer_One',
      password: 'REPLACE_WITH_LOCAL_SECRET',
      bootstrap_id: 'web-device-1',
      device_id: 'web-device-1',
      client_build: 'farm-h5/0.2',
    });
    expect(storage.getItem('correct horse battery')).toBeNull();
  });

  it('uses the HttpOnly session renew endpoint for reconnect', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ticketResponse(`ticket-${fetchMock.mock.calls.length}`));
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WebAuthProvider('/login', {
      registerEndpoint: '/register',
      renewEndpoint: '/renew',
      deviceId: 'web-device-1',
    });
    provider.prepareCredentials('login', 'farmer_one', 'correct horse battery');

    await provider.acquireConnectionTicket();
    await provider.acquireConnectionTicket();

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/login');
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/renew');
    expect(fetchMock.mock.calls[1]?.[1]?.body).toBe('{}');
  });

  it('forgets the password after a failed primary request', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ({
      ok: false,
      status: 401,
      json: async () => ({ error: 'invalid username or password' }),
    }) as Response);
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WebAuthProvider('/login', { deviceId: 'web-device-1' });
    provider.prepareCredentials('login', 'farmer_one', 'wrong password');

    await expect(provider.acquireConnectionTicket()).rejects.toThrow('invalid username or password');
    await expect(provider.acquireConnectionTicket()).rejects.toThrow('请输入账号和密码');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('persists and reuses a generated device id across explicit logins', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ticketResponse());
    vi.stubGlobal('fetch', fetchMock);
    const storage = new MemoryStorage();
    const first = new WebAuthProvider('/login', { storage });
    first.prepareCredentials('login', 'farmer_one', 'correct horse battery');
    await first.acquireConnectionTicket();

    const second = new WebAuthProvider('/login', { storage });
    second.prepareCredentials('login', 'farmer_one', 'correct horse battery');
    await second.acquireConnectionTicket();

    const firstBody = JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string) as {
      bootstrap_id: string;
      device_id: string;
    };
    const secondBody = JSON.parse(fetchMock.mock.calls[1]?.[1]?.body as string) as {
      bootstrap_id: string;
      device_id: string;
    };
    expect(firstBody.bootstrap_id).toMatch(/^web-/);
    expect(secondBody.bootstrap_id).toBe(firstBody.bootstrap_id);
    expect(firstBody.device_id).toBe(firstBody.bootstrap_id);
    expect(secondBody.device_id).toBe(secondBody.bootstrap_id);
  });

  it('changes password and forgets local credential state', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ({
      ok: true,
      status: 200,
      json: async () => ({ status: 'password_changed' }),
    }) as Response);
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WebAuthProvider('/login', {
      changePasswordEndpoint: '/change-password',
      deviceId: 'web-device-1',
    });
    provider.prepareCredentials('login', 'farmer_one', 'old password value');

    await provider.changePassword('farmer_one', 'old password value', 'new password value');

    expect(fetchMock).toHaveBeenCalledWith('/change-password', expect.objectContaining({
      method: 'POST',
      credentials: 'include',
    }));
    const body = JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string) as Record<string, string>;
    expect(body).toEqual({
      username: 'farmer_one',
      current_password: 'REPLACE_WITH_LOCAL_SECRET',
      new_password: 'REPLACE_WITH_LOCAL_SECRET',
      bootstrap_id: 'web-device-1',
      device_id: 'web-device-1',
    });
    expect(provider.lastUsername()).toBe('farmer_one');
    // Password was never kept for reconnect; next ticket requires an explicit login.
    provider.prepareCredentials('login', 'farmer_one', '');
    await expect(provider.acquireConnectionTicket()).rejects.toThrow('请输入账号和密码');
  });

  it('logs out every device through the revoke-all endpoint', async () => {
    const fetchMock = vi.fn(async (..._args: Parameters<typeof fetch>) => ({
      ok: true,
      status: 200,
      json: async () => ({ status: 'revoked' }),
    }) as Response);
    vi.stubGlobal('fetch', fetchMock);
    const provider = new WebAuthProvider('/login', {
      logoutAllEndpoint: '/logout-all',
      deviceId: 'web-device-1',
    });

    await provider.logoutAll();

    expect(fetchMock).toHaveBeenCalledWith('/logout-all', expect.objectContaining({
      method: 'POST',
      credentials: 'include',
      body: '{}',
    }));
  });

  it('shares one in-flight primary request', async () => {
    let resolveResponse: ((value: Response) => void) | undefined;
    vi.stubGlobal('fetch', vi.fn(() => new Promise((resolve) => {
      resolveResponse = resolve;
    })));
    const provider = new WebAuthProvider('/login', { deviceId: 'web-device-1' });
    provider.prepareCredentials('login', 'farmer_one', 'correct horse battery');

    const first = provider.acquireConnectionTicket();
    const second = provider.acquireConnectionTicket();
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(1);
    resolveResponse?.(ticketResponse());
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
  });
});
