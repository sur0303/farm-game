import { defineConfig, loadEnv, type ProxyOptions } from 'vite';

/**
 * Derive the /ws dev-server proxy target from the client-facing VITE_WS_URL.
 * This is a local-dev convenience: .env.staging documents VITE_WS_URL as the
 * address developers set for the client, so without this fallback the /ws
 * proxy would silently stay off and ws://localhost:5173/ws would fail with
 * ERR_CONNECTION_REFUSED. An explicit VITE_WS_PROXY_TARGET still wins.
 */
const deriveWsProxyTarget = (wsUrl: string | undefined): string | undefined => {
  if (!wsUrl) return undefined;
  const match = /^wss?:\/\/([^/]+)/.exec(wsUrl);
  return match ? `ws://${match[1]}` : undefined;
};

export default defineConfig(({ mode }) => {
  // loadEnv third arg is the prefix filter. Pass 'VITE_' (or any non-empty
  // string) so vars from .env.staging.local actually reach the config object.
  // An empty string was returning an empty object on this Vite version, which
  // silently disabled the /api proxy and caused ERR_CONNECTION_REFUSED.
  const env = loadEnv(mode, process.cwd(), 'VITE_');
  const defaultProfile = mode === 'staging' ? 'test' : mode === 'wechat' ? 'local' : 'showcase';
  const appProfile = (process.env.FARM_APP_PROFILE ?? env.VITE_FARM_PROFILE ?? defaultProfile).trim().toLowerCase();
  if (!['local', 'test', 'showcase'].includes(appProfile)) {
    throw new Error(`unsupported FARM_APP_PROFILE: ${appProfile}`);
  }
  if (appProfile === 'showcase') {
    const farmEnvironment = (env.VITE_FARM_ENV ?? 'web').trim().toLowerCase();
    if (farmEnvironment !== 'web') {
      throw new Error('FARM_APP_PROFILE=showcase requires VITE_FARM_ENV=web');
    }
    const readBuildEnv = (key: string): string => (process.env[key] ?? env[key] ?? '').trim();
    for (const key of ['VITE_DEMO_PLATFORM_TOKEN', 'VITE_DEMO_SECONDARY_PLATFORM_TOKEN', 'VITE_STAGING_ACCOUNT', 'VITE_DEMO_ACCOUNT']) {
      if (readBuildEnv(key)) throw new Error(`${key} is not allowed in showcase H5 builds`);
    }
    for (const key of ['VITE_AUTH_PROVIDER', 'VITE_FRIEND_PROVIDER', 'VITE_FARM_AUTH_PROVIDER', 'VITE_FARM_FRIEND_PROVIDER']) {
      if (['mock', 'fake', 'stub', 'fixture', 'demo', 'test'].includes(readBuildEnv(key).toLowerCase())) {
        throw new Error(`${key}=mock is not allowed in showcase H5 builds`);
      }
    }
    for (const key of ['VITE_MOCK_AUTH', 'VITE_MOCK_FRIEND', 'VITE_USE_FIXTURES']) {
      if (['1', 'true', 'yes', 'on'].includes(readBuildEnv(key).toLowerCase())) {
        throw new Error(`${key} is not allowed in showcase H5 builds`);
      }
    }
  }
  // Explicit process env wins over local files so CI/E2E can select an
  // isolated farm-gate without being shadowed by a developer override.
  const apiProxyTarget = process.env.VITE_API_PROXY_TARGET ?? env.VITE_API_PROXY_TARGET;
  const wsProxyTarget = process.env.VITE_WS_PROXY_TARGET
    ?? env.VITE_WS_PROXY_TARGET
    ?? deriveWsProxyTarget(env.VITE_WS_URL);
  console.log(`[vite.config] mode=${mode} profile=${appProfile} apiProxyTarget=${apiProxyTarget} wsProxyTarget=${wsProxyTarget}`);
  const proxy: Record<string, string | ProxyOptions> = {};

  // Vite's proxy exists only for local development. Staging and production
  // deployments use the public same-origin /api route configured by Nginx/LB.
  if (apiProxyTarget) {
    proxy['/api'] = {
      target: apiProxyTarget,
      changeOrigin: true,
    };
  }

  // Direct WebSocket development path. Production normally uses the
  // server-issued wss_endpoint through CLB/Ingress.
  if (wsProxyTarget) {
    proxy['/ws'] = {
      target: wsProxyTarget,
      changeOrigin: true,
      ws: true,
    };
  }

  return {
    define: {
      '__FARM_SHOWCASE__': JSON.stringify(appProfile === 'showcase'),
      'import.meta.env.VITE_FARM_PROFILE': JSON.stringify(appProfile),
    },
    server: {
      port: 5173,
      ...(Object.keys(proxy).length ? { proxy } : {}),
    },
  };
});
