import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { expect, test, type Page } from '@playwright/test';

const farmGameRoot = path.resolve(process.cwd(), '../farm-game');
const composeScript = path.join(farmGameRoot, 'scripts/company-redis-compose.sh');

function compose(...args: string[]): void {
  execFileSync(composeScript, args, {
    cwd: farmGameRoot,
    stdio: 'pipe',
  });
}

/** HUD exposes wallet/seeds; mature produce lives only in the inventory sheet. */
async function inventoryCount(page: Page, label: string): Promise<number> {
  if (label === '金币') {
    return Number(await page.locator('[data-private]').getAttribute('data-wallet-count'));
  }
  if (label === '种子') {
    return Number(await page.locator('[data-private]').getAttribute('data-seed-count'));
  }
  return produceCount(page, label);
}

async function produceCount(page: Page, cropName: string): Promise<number> {
  const sheet = page.locator('[data-utility-sheet]');
  const wasHidden = await sheet.isHidden();
  const cropsTab = page.locator('[data-inventory-tab="crops"]');
  // openUtility toggles the same mode closed; only (re)open when not on crops.
  const showingCrops = !wasHidden
    && await cropsTab.count() > 0
    && (await cropsTab.getAttribute('aria-selected')) === 'true';
  if (!showingCrops) {
    await openUtility(page, 'inventory');
    if (await cropsTab.count()) await cropsTab.click();
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

test('persistent Redis restores a harvest across worker and full backend restarts', async ({ page, baseURL }) => {
  test.skip(process.env.FARM_WORKFLOW_E2E !== '1', 'destructive process-fault check is opt-in');
  test.setTimeout(120_000);

  await page.goto(`${baseURL}/?demo_account=player-b`);
  await page.getByRole('button', { name: '使用 Staging 账号登录' }).click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });

  // Bootstrap feature-disabled 时首屏资产为未知(NaN):首笔 BUY 回包后才可见,
  // 只有明确知道资金不足才卖胡萝卜补仓。
  const initialWallet = await inventoryCount(page, '金币');
  if (!Number.isNaN(initialWallet) && initialWallet < 10) {
    expect(await inventoryCount(page, '胡萝卜'), 'fault-test account needs a carrot to replenish funds').toBeGreaterThan(0);
    await openUtility(page, 'inventory');
    await page.locator('[data-sell-item]:not([disabled])').first().click();
    await expect.poll(() => inventoryCount(page, '金币')).toBeGreaterThanOrEqual(10);
    await closeUtility(page);
  }
  await openUtility(page, 'shop');
  await page.locator('[data-buy]').click();
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
    // Keep FarmActor and FarmRecord available, but remove both the synchronous
    // Player participant and the asynchronous recovery executor.
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

    // The browser journal owns uncertainty recovery. Reconnecting causes it to
    // query GET_OPERATION_RESULT and apply the worker's terminal private state.
    await openUtility(page, 'more');
    await page.locator('[data-reconnect]').click();
    await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
    await expect(page.locator('[data-pending]')).toContainText('暂无未决操作', { timeout: 15_000 });
    // Worker claims FARM_COMMITTED workflows after InitialRecoveryDelay (5s);
    // allow that window plus result application after reconnect.
    await expect.poll(() => inventoryCount(page, '胡萝卜'), { timeout: 15_000 }).toBe(carrotsBefore + 10);

    // Restart every business process while leaving the company Redis outside
    // Compose. The same terminal receipt, farm and private inventory must be
    // reconstructed rather than reinitialized from process memory.
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

    // The same page must survive a complete Gate outage. Recovery keeps
    // retrying across failed ticket requests, obtains a fresh session
    // generation after startup, and reconstructs state from persistent Redis.
    await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
    await expect.poll(() => inventoryCount(page, '胡萝卜')).toBe(carrotsBefore + 10);
    await expect(page.locator(`[data-plot-id="${plotID}"]`)).toHaveClass(/empty/);
  } finally {
    // Leave the shared local development stack usable even when an assertion
    // or browser action fails midway through the fault injection.
    compose('up', '-d', 'player-svr');
    compose('up', '-d', 'farm-worker');
  }
});
