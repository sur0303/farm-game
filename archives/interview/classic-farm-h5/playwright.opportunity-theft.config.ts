import { defineConfig } from '@playwright/test';

const h5Port = Number.parseInt(process.env.FARM_E2E_H5_PORT ?? '5195', 10);
const api = process.env.FARM_E2E_API_BASE_URL ?? 'http://127.0.0.1:8080';
const wsProxy = process.env.FARM_E2E_WS_PROXY_TARGET ?? api.replace(/^http/, 'ws');
const ws = process.env.FARM_E2E_WSS_URL ?? '/ws';
const evidenceRoot = process.env.FARM_E2E_EVIDENCE_ROOT ?? 'test-results/candidate-opportunity-theft';

if (!Number.isInteger(h5Port) || h5Port < 1 || h5Port > 65_535) {
  throw new Error(`invalid FARM_E2E_H5_PORT: ${process.env.FARM_E2E_H5_PORT}`);
}

export default defineConfig({
  testDir: './src/test/e2e',
  testMatch: ['candidate-opportunity-theft.spec.ts'],
  outputDir: `${evidenceRoot}/playwright-artifacts`,
  preserveOutput: 'always',
  workers: 1,
  retries: 0,
  timeout: 180_000,
  reporter: [['line']],
  use: {
    baseURL: `http://127.0.0.1:${h5Port}`,
    headless: process.env.FARM_E2E_HEADLESS !== '0',
    viewport: { width: 1280, height: 720 },
    trace: 'on',
    screenshot: 'on',
    video: { mode: 'on', size: { width: 1280, height: 720 } },
  },
  webServer: {
    command: `VITE_FARM_ENV=web VITE_FARM_PROTOCOL=canonical-final VITE_API_PROXY_TARGET=${api} VITE_WS_PROXY_TARGET=${wsProxy} VITE_WS_URL=${ws} npm run dev:web -- --host 127.0.0.1 --port ${h5Port}`,
    url: `http://127.0.0.1:${h5Port}`,
    reuseExistingServer: true,
    timeout: 30_000,
  },
});
