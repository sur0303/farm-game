import { parseConnectionTicket, type AuthProvider, type ConnectionTicket } from './types';

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export type StagingAuthFailureKind = 'network' | 'http' | 'invalid_response';

export class StagingAuthError extends Error {
  constructor(
    public readonly kind: StagingAuthFailureKind,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'StagingAuthError';
  }
}

const browserStorage = (): StorageLike | null => {
  try {
    return typeof window !== 'undefined' ? window.localStorage : null;
  } catch {
    return null;
  }
};

const createBootstrapId = (): string => {
  const random = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : String(Date.now()) + '-' + Math.random().toString(16).slice(2);
  return 'bootstrap-' + random;
};

const BS_KEY = 'farm:bootstrap_id';

/** Read cached bootstrap_id from localStorage, or create + persist a new one. */
function loadOrCreateBootstrapId(storage: StorageLike | null): string {
  try {
    const cached = storage?.getItem(BS_KEY);
    if (cached) return cached;
    const id = createBootstrapId();
    storage?.setItem(BS_KEY, id);
    return id;
  } catch {
    return createBootstrapId();
  }
}

/**
 * StagingAuthProvider 对接 farm-gate 的隔离测试登录接口。
 *
 * 流程：POST /v1/auth/login → 平台票据校验 → 会话代次提升 → 建档 → 签发一次性 connection_ticket。
 *
 * 请求体：{platform_token, bootstrap_id, client_build}
 * 响应体：{connection_ticket, player_id, active_session_epoch, wss_endpoint}
 */
export class StagingAuthProvider implements AuthProvider {
  private readonly storage: StorageLike | null;
  private bootstrapId: string;

  constructor(
    private readonly endpoint = '/api/v1/auth/login',
    private readonly platformToken = '',
    storage?: StorageLike,
  ) {
    this.storage = storage ?? browserStorage();
    this.bootstrapId = loadOrCreateBootstrapId(this.storage);
  }

  async acquireConnectionTicket(signal?: AbortSignal): Promise<ConnectionTicket> {
    if (!this.platformToken) {
      throw new StagingAuthError('invalid_response', 'staging platform token is not configured');
    }
    let response: Response;
    try {
      response = await fetch(this.endpoint, {
        method: 'POST',
        credentials: 'include',
        signal,
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          platform_token: this.platformToken,
          // Stable per-browser bootstrap_id, cached in localStorage.
          // Survives Ctrl+Shift+R and farm-gate restarts.
          bootstrap_id: this.bootstrapId,
          client_build: 'farm-h5/0.2',
        }),
      });
    } catch (error) {
      throw new StagingAuthError('network', 'staging login request failed', { cause: error });
    }

    if (!response.ok) {
      let body = '';
      try { body = (await response.text()).slice(0, 256); } catch { /* ok */ }
      throw new StagingAuthError('http', `staging login failed: ${response.status}${body ? ` ${body}` : ''}`);
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch (error) {
      throw new StagingAuthError('invalid_response', 'staging login returned invalid JSON', { cause: error });
    }

    try {
      return parseConnectionTicket(payload);
    } catch (error) {
      const detail = error instanceof Error ? `: ${error.message}` : '';
      throw new StagingAuthError('invalid_response', `staging login bad payload${detail}`);
    }
  }
}
