export type FarmEnvironment = 'staging' | 'wechat' | 'web';
export type ProtocolProfile = 'canonical-final';
export type FarmAppProfile = 'local' | 'test' | 'showcase';

export interface ClientEnvironment {
  name: FarmEnvironment;
  profile: FarmAppProfile;
  protocolProfile: ProtocolProfile;
  apiBase: string;
  /** Explicit local/operator override. Server-issued wss_endpoint wins otherwise. */
  wsUrl?: string;
  demoPlatformToken?: string;
  demoSecondaryPlatformToken?: string;
}

export const parseFarmAppProfile = (raw: string | undefined, fallback: FarmAppProfile): FarmAppProfile => {
  const value = (raw ?? fallback).trim().toLowerCase();
  if (value === 'local' || value === 'test' || value === 'showcase') return value;
  throw new Error(`unsupported FARM_APP_PROFILE: ${value}`);
};

const rawEnvironment = (import.meta.env.VITE_FARM_ENV ?? 'web').trim().toLowerCase();

if (rawEnvironment !== 'staging' && rawEnvironment !== 'wechat' && rawEnvironment !== 'web') {
  throw new Error(`unsupported VITE_FARM_ENV: ${rawEnvironment}`);
}

const rawProtocolProfile = (import.meta.env.VITE_FARM_PROTOCOL ?? 'canonical-final').trim().toLowerCase();

if (rawProtocolProfile !== 'canonical-final') {
  throw new Error(`unsupported VITE_FARM_PROTOCOL: ${rawProtocolProfile}`);
}

const defaultProfile: FarmAppProfile = rawEnvironment === 'staging'
  ? 'test'
  : rawEnvironment === 'wechat'
    ? 'local'
    : 'showcase';
const rawProfile = (import.meta.env.VITE_FARM_PROFILE ?? defaultProfile).trim().toLowerCase();
const profile = parseFarmAppProfile(rawProfile, defaultProfile);

const readEnv = (key: string): string => {
  const value = (import.meta.env as Record<string, unknown>)[key];
  return typeof value === 'string' ? value.trim() : '';
};

const isMockProvider = (value: string): boolean =>
  value === 'mock' || value === 'fake' || value === 'stub' || value === 'fixture' || value === 'demo' || value === 'test';

if (profile === 'showcase') {
  if (rawEnvironment !== 'web') {
    throw new Error('FARM_APP_PROFILE=showcase requires VITE_FARM_ENV=web');
  }
  for (const key of [
    'VITE_DEMO_PLATFORM_TOKEN',
    'VITE_DEMO_SECONDARY_PLATFORM_TOKEN',
    'VITE_STAGING_ACCOUNT',
    'VITE_DEMO_ACCOUNT',
  ]) {
    if (readEnv(key)) throw new Error(`${key} is not allowed in showcase H5 builds`);
  }
  for (const key of ['VITE_AUTH_PROVIDER', 'VITE_FRIEND_PROVIDER', 'VITE_FARM_AUTH_PROVIDER', 'VITE_FARM_FRIEND_PROVIDER']) {
    if (isMockProvider(readEnv(key).toLowerCase())) {
      throw new Error(`${key}=mock is not allowed in showcase H5 builds`);
    }
  }
  for (const key of ['VITE_MOCK_AUTH', 'VITE_MOCK_FRIEND', 'VITE_USE_FIXTURES']) {
    if (['1', 'true', 'yes', 'on'].includes(readEnv(key).toLowerCase())) {
      throw new Error(`${key} is not allowed in showcase H5 builds`);
    }
  }
}

export const resolveWebSocketUrl = (value: string): string => {
  if (/^wss?:\/\//i.test(value)) return value;
  const url = new URL(value || '/ws', window.location.href);
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
};

export const clientEnvironment: ClientEnvironment = {
  name: rawEnvironment,
  profile,
  protocolProfile: rawProtocolProfile,
  apiBase: import.meta.env.VITE_API_BASE ?? '/api',
  ...(import.meta.env.VITE_WS_URL
    ? { wsUrl: resolveWebSocketUrl(import.meta.env.VITE_WS_URL) }
    : {}),
  ...(import.meta.env.VITE_DEMO_PLATFORM_TOKEN
    ? { demoPlatformToken: import.meta.env.VITE_DEMO_PLATFORM_TOKEN }
    : {}),
  ...(import.meta.env.VITE_DEMO_SECONDARY_PLATFORM_TOKEN
    ? { demoSecondaryPlatformToken: import.meta.env.VITE_DEMO_SECONDARY_PLATFORM_TOKEN }
    : {}),
};

