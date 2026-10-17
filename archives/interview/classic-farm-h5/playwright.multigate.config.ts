import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: 'multigate-drain.spec.ts',
  workers: 1,
  use: {
    baseURL: 'http://127.0.0.1:5175',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'VITE_API_PROXY_TARGET=http://127.0.0.1:18580 VITE_WS_URL=ws://127.0.0.1:18580/ws npm run dev:staging -- --host 127.0.0.1 --port 5175',
    url: 'http://127.0.0.1:5175',
    reuseExistingServer: false,
  },
});
