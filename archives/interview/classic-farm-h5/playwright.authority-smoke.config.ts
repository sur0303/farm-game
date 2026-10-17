import { defineConfig } from '@playwright/test';

const api = process.env.FARM_E2E_API_BASE_URL ?? 'http://127.0.0.1:8080';
// Q1: the client talks to the same-origin /ws, so the Vite dev server /ws proxy
// is genuinely exercised. VITE_WS_PROXY_TARGET points the /ws proxy at farm-gate.
const wsProxy = process.env.FARM_E2E_WS_PROXY_TARGET ?? 'ws://127.0.0.1:8080';
const ws = process.env.FARM_E2E_WSS_URL ?? '/ws';
const h5Port = 5194;

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: 'authority-vertical-smoke.spec.ts',
  workers: 1,
  retries: 0,
  timeout: 90_000,
  reporter: [['line']],
  use: {
    baseURL: `http://127.0.0.1:${h5Port}`,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  webServer: {
    // API requests go through the vite /api proxy; WebSocket traffic goes to
    // same-origin /ws and is proxied to farm-gate by Vite (VITE_WS_URL=/ws +
    // VITE_WS_PROXY_TARGET). This is the true "vite proxy" path, not a direct
    // ws:// connection to the Gate.
    command: `VITE_API_PROXY_TARGET=${api} VITE_WS_PROXY_TARGET=${wsProxy} VITE_WS_URL=${ws} npm run dev:web -- --host 127.0.0.1 --port ${h5Port}`,
    url: `http://127.0.0.1:${h5Port}`,
    reuseExistingServer: false,
  },
});
