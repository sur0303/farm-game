import { parseConnectionTicket, type AuthProvider, type ConnectionTicket } from './types';

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export type WebAccountMode = 'login' | 'register';

export interface WebAuthOptions {
  registerEndpoint?: string;
  renewEndpoint?: string;
  changePasswordEndpoint?: string;
  logoutAllEndpoint?: string;
  clientVersion?: string;
  deviceId?: string;
  storage?: StorageLike;
}

const deviceStorageKey = 'classic-farm.web-device-id';

const browserStorage = (): StorageLike | null => {
  try {
    return typeof window !== 'undefined' ? window.localStorage : null;
  } catch {
    return null;
  }
};

const createDeviceId = (): string => {
  const random = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : String(Date.now()) + '-' + Math.random().toString(16).slice(2);
  return 'web-' + random;
};

const responseError = async (response: Response, fallback: string): Promise<Error> => {
  const payload = await response.json().catch(() => ({})) as { error?: unknown };
  const detail = typeof payload.error === 'string' ? payload.error : fallback;
  return new Error(detail);
};

/**
 * Standalone Web account authentication.
 *
 * Username/password is used only for an explicit primary login or register
 * action. Weak-network reconnects exchange the HttpOnly farm_session cookie at
 * /auth/renew, so reconnect never keeps a password or advances the account's
 * active session epoch unnecessarily.
 */
export class WebAuthProvider implements AuthProvider {
  private readonly storage: StorageLike | null;
  private readonly registerEndpoint: string;
  private readonly renewEndpoint: string;
  private readonly changePasswordEndpoint: string;
  private readonly logoutAllEndpoint: string;
  private readonly clientVersion: string;
  private deviceId: string | null = null;
  private inFlight: Promise<ConnectionTicket> | null = null;
  private primaryMode: WebAccountMode | null = null;
  private username = '';
  private password = '';

  constructor(
    private readonly loginEndpoint = '/api/v1/session/login',
    options: WebAuthOptions = {},
  ) {
    this.registerEndpoint = options.registerEndpoint ?? '/api/v1/accounts/register';
    this.renewEndpoint = options.renewEndpoint ?? '/api/v1/auth/renew';
    this.changePasswordEndpoint = options.changePasswordEndpoint ?? '/api/v1/accounts/change-password';
    this.logoutAllEndpoint = options.logoutAllEndpoint ?? '/api/v1/auth/logout-all';
    this.clientVersion = options.clientVersion ?? 'farm-h5/0.2';
    this.storage = options.storage ?? browserStorage();
    this.deviceId = options.deviceId ?? null;
  }

  prepareCredentials(mode: WebAccountMode, username: string, password: string): void {
    this.primaryMode = mode;
    this.username = username.trim();
    this.password = password;
  }

  lastUsername(): string {
    return this.username;
  }

  /**
   * Changes the account password and revokes every HTTP/WSS session generation.
   * Callers must disconnect and force an explicit re-login afterward.
   */
  async changePassword(username: string, currentPassword: string, newPassword: string, signal?: AbortSignal): Promise<void> {
    const normalized = username.trim();
    if (!normalized || !currentPassword || !newPassword) {
      throw new Error('请填写账号、当前密码和新密码');
    }
    const deviceId = this.getDeviceId();
    const response = await fetch(this.changePasswordEndpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        username: normalized,
        current_password: currentPassword,
        new_password: newPassword,
        bootstrap_id: deviceId,
        device_id: deviceId,
      }),
    });
    if (!response.ok) throw await responseError(response, `修改密码失败 (${response.status})`);
    this.primaryMode = null;
    this.password = '';
    this.username = normalized;
  }

  /**
   * Revokes every device session for the current Cookie subject and clears it.
   * Callers must disconnect and show the login screen afterward.
   */
  async logoutAll(signal?: AbortSignal): Promise<void> {
    const response = await fetch(this.logoutAllEndpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    if (!response.ok) throw await responseError(response, `退出全部设备失败 (${response.status})`);
    this.primaryMode = null;
    this.password = '';
  }

  async acquireConnectionTicket(signal?: AbortSignal): Promise<ConnectionTicket> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.primaryMode
      ? this.authenticateWithCredentials(this.primaryMode, signal)
      : this.renew(signal);
    try {
      return await this.inFlight;
    } finally {
      this.inFlight = null;
    }
  }

  private async authenticateWithCredentials(mode: WebAccountMode, signal?: AbortSignal): Promise<ConnectionTicket> {
    if (!this.username || !this.password) throw new Error('请输入账号和密码');
    const endpoint = mode === 'register' ? this.registerEndpoint : this.loginEndpoint;
    const deviceId = this.getDeviceId();
    const password = this.password;
    // Retain the password only long enough to build this one HTTPS request.
    // Both success and failure paths require the user to supply it again.
    this.password = '';
    const response = await fetch(endpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        username: this.username,
        password,
        bootstrap_id: deviceId,
        // A stable browser risk signal for server-side rate limiting. It is
        // client controlled and never acts as an authentication credential.
        device_id: deviceId,
        client_build: this.clientVersion,
      }),
    });
    if (!response.ok) throw await responseError(response, `${mode === 'register' ? '注册' : '登录'}失败 (${response.status})`);
    const ticket = parseConnectionTicket(await response.json());
    this.primaryMode = null;
    // Keep the last successful username for the in-session account panel only.
    // The password was already cleared before the network call returned.
    return ticket;
  }

  private async renew(signal?: AbortSignal): Promise<ConnectionTicket> {
    const response = await fetch(this.renewEndpoint, {
      method: 'POST',
      credentials: 'include',
      signal,
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    if (!response.ok) throw await responseError(response, '登录状态已失效，请重新输入账号密码');
    return parseConnectionTicket(await response.json());
  }

  private getDeviceId(): string {
    if (this.deviceId) return this.deviceId;
    try {
      const existing = this.storage?.getItem(deviceStorageKey);
      if (existing) {
        this.deviceId = existing;
        return existing;
      }
    } catch {
      // Private browsing or blocked storage should not prevent registration.
    }
    const created = createDeviceId();
    this.deviceId = created;
    try {
      this.storage?.setItem(deviceStorageKey, created);
    } catch {
      // The generated id remains valid for this page session.
    }
    return created;
  }
}
