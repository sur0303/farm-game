import { defineConfig } from '@playwright/test';

// 真实环境目标可由环境变量覆盖(供 web-real-vertical 等真实链路用例):
//   FARM_E2E_API_BASE_URL  Web API 地址(默认本地 farm-gate 8080)
//   FARM_E2E_WSS_URL       WSS 地址(默认 ws://127.0.0.1:8080/ws)
const apiProxyTarget = process.env.FARM_E2E_API_BASE_URL ?? 'http://127.0.0.1:8080';
const wsUrl = process.env.FARM_E2E_WSS_URL ?? 'ws://127.0.0.1:8080/ws';

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: 'web-*.spec.ts',
  use: {
    baseURL: 'http://127.0.0.1:5175',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: `VITE_API_PROXY_TARGET=${apiProxyTarget} VITE_WS_URL=${wsUrl} npm run dev:web -- --host 127.0.0.1 --port 5175`,
    url: 'http://127.0.0.1:5175',
    reuseExistingServer: false,
  },
});
