import { defineConfig } from '@playwright/test';

const api = process.env.FARM_E2E_API_BASE_URL ?? 'http://127.0.0.1:8080';
const wsProxy = process.env.FARM_E2E_WS_PROXY_TARGET ?? 'ws://127.0.0.1:8080';
const ws = process.env.FARM_E2E_WSS_URL ?? '/ws';
const evidenceRoot = process.env.FARM_E2E_EVIDENCE_ROOT ?? 'test-results/cp-vertical-b';
const h5Port = Number.parseInt(process.env.FARM_E2E_H5_PORT ?? '5194', 10);

if (!Number.isInteger(h5Port) || h5Port < 1 || h5Port > 65_535) {
  throw new Error(`invalid FARM_E2E_H5_PORT: ${process.env.FARM_E2E_H5_PORT}`);
}

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: ['cp-vertical-b.spec.ts', 'cp-unlock-sequential.spec.ts', 'cp-social-navigation.spec.ts', 'cp-sell-authoritative.spec.ts'],
  outputDir: `${evidenceRoot}/playwright-artifacts`,
  preserveOutput: 'always',
  workers: 1,
  retries: 0,
  timeout: 240_000,
  reporter: [['line']],
  use: {
    baseURL: `http://127.0.0.1:${h5Port}`,
    headless: false,
    viewport: { width: 1280, height: 720 },
    launchOptions: { slowMo: 40 },
    trace: 'on',
    screenshot: 'on',
    video: { mode: 'on', size: { width: 1280, height: 720 } },
  },
  webServer: {
    command: `VITE_FARM_ENV=web VITE_FARM_PROTOCOL=canonical-final VITE_API_PROXY_TARGET=${api} VITE_WS_PROXY_TARGET=${wsProxy} VITE_WS_URL=${ws} npm run dev:web -- --host 127.0.0.1 --port ${h5Port}`,
    url: `http://127.0.0.1:${h5Port}`,
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
