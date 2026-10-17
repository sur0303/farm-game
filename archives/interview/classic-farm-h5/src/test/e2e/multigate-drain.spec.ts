import { expect, test } from '@playwright/test';

const gateAPid = Number(process.env.FARM_GATE_A_PID);

async function metricValue(endpoint: string, name: string): Promise<number> {
  const response = await fetch(endpoint);
  if (!response.ok) return Number.NaN;
  const body = await response.text();
  const match = body.match(new RegExp(`^${name}\\s+([0-9.]+)$`, 'm'));
  return match ? Number(match[1]) : Number.NaN;
}

test('RecoveryBarrier moves an existing browser from draining Gate A to ready Gate B', async ({ page }) => {
  test.skip(!Number.isInteger(gateAPid) || gateAPid <= 0, 'multi-Gate process harness is required');
  test.setTimeout(60_000);

  await page.goto('/');
  await page.getByRole('button', { name: '使用 Staging 账号登录' }).click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
  await expect.poll(() => metricValue('http://127.0.0.1:18380/metrics', 'farmgate_ws_conn_active')).toBe(1);
  await expect.poll(() => metricValue('http://127.0.0.1:18480/metrics', 'farmgate_ws_conn_active')).toBe(0);

  process.kill(gateAPid, 'SIGTERM');
  await expect(page.locator('[data-message]')).toContainText('滚动更新', { timeout: 10_000 });
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });

  // Reaching InFarm requires ServerHello, own PrivateState Snapshot, pending
  // operation queries and RecoveryBarrier completion through Gate B.
  await expect.poll(() => metricValue('http://127.0.0.1:18480/metrics', 'farmgate_ws_conn_active'), {
    timeout: 20_000,
  }).toBe(1);
  await expect(page.locator('[data-farm-title]')).toHaveText('我的农场');
  await expect(page.locator('[data-friends]')).toHaveAttribute('data-state', 'ready');
});
