import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: 'workflow-recovery.spec.ts',
  workers: 1,
  use: {
    baseURL: 'http://127.0.0.1:5174',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'VITE_API_PROXY_TARGET=http://127.0.0.1:8080 VITE_WS_URL=ws://127.0.0.1:8080/ws npm run dev:staging -- --host 127.0.0.1 --port 5174',
    url: 'http://127.0.0.1:5174',
    reuseExistingServer: true,
  },
});
