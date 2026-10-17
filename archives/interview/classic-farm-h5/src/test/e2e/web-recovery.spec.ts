import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { expect, test, type Page } from '@playwright/test';

const farmGameRoot = path.resolve(process.cwd(), process.env.FARM_GAME_ROOT ?? '../farm-game');
const composeScript = path.join(farmGameRoot, 'scripts/company-redis-compose.sh');

function compose(...args: string[]): void {
  execFileSync(composeScript, args, { cwd: farmGameRoot, stdio: 'pipe' });
}

async function inventoryCount(page: Page, label: string): Promise<number> {
  if (label === '金币') return Number(await page.locator('[data-private]').getAttribute('data-wallet-count'));
  if (label === '种子') return Number(await page.locator('[data-private]').getAttribute('data-seed-count'));
  return produceCount(page, label);
}

async function produceCount(page: Page, cropName: string): Promise<number> {
  const sheet = page.locator('[data-utility-sheet]');
  const wasHidden = await sheet.isHidden();
  const cropsTab = page.locator('[data-inventory-tab="crops"]');
  const showingCrops =
    !wasHidden && (await cropsTab.count()) > 0 && (await cropsTab.getAttribute('aria-selected')) === 'true';
  if (!showingCrops) {
    await openUtility(page, 'inventory');
    if ((await cropsTab.count()) > 0) await cropsTab.click();
  }
  const item = page.locator('[data-utility-sheet] .crop-item').filter({ hasText: cropName }).first();
  await expect(item).toBeVisible();
  const qty = Number((await item.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
  if (wasHidden) await closeUtility(page);
  return qty;
}

async function openUtility(page: Page, mode: 'inventory' | 'shop' | 'more'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
}

const webPassword = 'farm-test-password-2026';
async function submitWebAccount(page: Page, mode: 'login' | 'register', username: string): Promise<void> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(webPassword);
  await page.locator(mode === 'register' ? '[data-register]' : '[data-login]').click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
}

test('persistent Redis restores a web-account harvest across worker and full backend restarts', async ({ page, baseURL }) => {
  test.skip(process.env.FARM_WORKFLOW_E2E !== '1', 'destructive process-fault check is opt-in');
  test.setTimeout(120_000);

  const username = `recovery_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  await page.goto(baseURL!);
  await submitWebAccount(page, 'register', username);

  // GET_PLAYER_BOOTSTRAP 在当前后端基线 feature-disabled:首屏资产为未知(…),
  // 首笔 BUY 的权威 private_state 才让金币/种子读数变为数值。
  await openUtility(page, 'shop');
  await page.locator('[data-buy]').click();
  await expect.poll(() => inventoryCount(page, '金币')).toBeGreaterThanOrEqual(0);
  await expect.poll(() => inventoryCount(page, '种子')).toBeGreaterThan(0);
  await page.locator('[data-close-utility]').click();

  const plot = page.locator('.plot.empty').first();
  const plotID = await plot.getAttribute('data-plot-id');
  expect(plotID).toBeTruthy();
  await page.locator('[data-tool="plant"]').click();
  await plot.click();

  const plantedPlot = page.locator(`[data-plot-id="${plotID}"]`);
  await expect(plantedPlot).toContainText('已成熟', { timeout: 15_000 });
  const carrotsBefore = await inventoryCount(page, '胡萝卜');

  try {
    compose('stop', 'farm-worker');
    compose('stop', 'player-svr');
    await page.locator('[data-tool="harvest"]').click();
    await plantedPlot.click();
    await expect(plantedPlot).toHaveClass(/empty/, { timeout: 10_000 });
    await expect(page.locator('[data-pending]')).toContainText('Processing', { timeout: 10_000 });
    expect(await inventoryCount(page, '胡萝卜')).toBe(carrotsBefore);

    compose('up', '-d', 'player-svr');
    compose('up', '-d', 'farm-worker');
    await expect.poll(async () => {
      try {
        return (await fetch('http://127.0.0.1:19201/readyz')).ok;
      } catch {
        return false;
      }
    }, { timeout: 30_000 }).toBe(true);

    // Reconnect: the worker re-applies the +10 reward; the late Join private snapshot
    // must NOT overwrite the recovered private inventory (a282cc5).
    await openUtility(page, 'more');
    await page.locator('[data-reconnect]').click();
    await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
    await expect(page.locator('[data-pending]')).toContainText('暂无未决操作', { timeout: 15_000 });
    await expect.poll(() => inventoryCount(page, '胡萝卜'), { timeout: 15_000 }).toBe(carrotsBefore + 10);

    compose('stop', 'farm-worker', 'farm-gate', 'farm-svr', 'player-svr');
    compose('up', '-d', 'player-svr', 'farm-svr', 'farm-gate', 'farm-worker');
    await expect.poll(async () => {
      try {
        const [gate, worker] = await Promise.all([
          fetch('http://127.0.0.1:8080/readyz'),
          fetch('http://127.0.0.1:19201/readyz'),
        ]);
        return gate.ok && worker.ok;
      } catch {
        return false;
      }
    }, { timeout: 45_000 }).toBe(true);

    await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
    await expect.poll(() => inventoryCount(page, '胡萝卜')).toBe(carrotsBefore + 10);
    await expect(page.locator(`[data-plot-id="${plotID}"]`)).toHaveClass(/empty/);
  } finally {
    compose('up', '-d', 'player-svr', 'farm-svr', 'farm-gate', 'farm-worker');
  }
});
