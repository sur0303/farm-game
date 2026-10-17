import { expect, test, type Page } from '@playwright/test';

const username = process.env.FARM_E2E_USER_A ?? '';
const password = process.env.FARM_E2E_PASSWORD ?? '';
const shell = (page: Page) => page.locator('[data-app-shell]');

async function waitReady(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
  await expect(shell(page)).toHaveAttribute('data-join-phase', 'READY');
  await expect(shell(page)).toHaveAttribute('data-recovery-barrier', 'open');
}

async function openUtility(page: Page, mode: 'inventory' | 'shop'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function produceCount(page: Page): Promise<number> {
  await openUtility(page, 'inventory');
  const crops = page.locator('[data-inventory-tab="crops"]');
  if (await crops.count()) await crops.click();
  const row = page.locator('.crop-item').filter({ hasText: '胡萝卜' }).first();
  await expect(row).toBeVisible();
  const value = Number((await row.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
  await page.locator('[data-close-utility]').click();
  return value;
}

async function seedCount(page: Page): Promise<number> {
  return Number(await page.locator('[data-private]').getAttribute('data-seed-count') ?? '0');
}

async function wallet(page: Page): Promise<number> {
  return Number(await page.locator('[data-private]').getAttribute('data-wallet-count') ?? '0');
}

test('SELL 2500 is authoritative and serialized as 999 + 999 + 502', async ({ page, baseURL }) => {
  test.skip(!username || !password || !baseURL, 'real account credentials are required');
  test.setTimeout(180_000);
  await page.goto(baseURL!);
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  await page.locator('[data-register]').click();
  await waitReady(page);

  for (let i = 1; i <= 5; i += 1) {
    await openUtility(page, 'shop');
    await page.locator('[data-buy-offer="1"]').click();
    await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(i);
    await page.locator('[data-close-utility]').click();
  }
  expect(await wallet(page)).toBe(50);

  const planted: string[] = [];
  for (let remaining = 4; remaining >= 0; remaining -= 1) {
    await page.locator('[data-quick-seed="1"]').click();
    const empty = page.locator('.plot.empty[data-plot-id]').first();
    const plotID = await empty.getAttribute('data-plot-id');
    if (!plotID) throw new Error('empty plot missing id');
    await empty.click();
    const plot = page.locator(`[data-plot-id="${plotID}"]`);
    await expect(plot).toHaveAttribute('data-effective-crop-phase', 'growing', { timeout: 30_000 });
    await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(remaining);
    planted.push(plotID);
  }

  for (const plotID of planted) {
    await expect(page.locator(`[data-plot-id="${plotID}"]`)).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
  }
  for (let harvested = 1; harvested <= planted.length; harvested += 1) {
    await page.locator('[data-tool="harvest"]').click();
    const plot = page.locator(`[data-plot-id="${planted[harvested - 1]}"]`);
    await plot.click();
    await expect(plot).toHaveClass(/\bempty\b/, { timeout: 30_000 });
    await expect.poll(() => produceCount(page), { timeout: 30_000 }).toBe(harvested * 500);
  }

  expect(await produceCount(page)).toBe(2500);
  const walletBefore = await wallet(page);
  await openUtility(page, 'inventory');
  await page.locator('[data-sell-all]').click();
  await expect(page.locator('[data-message]')).toContainText('已出售 2500 个作物（3 笔权威终态）', { timeout: 60_000 });
  await expect.poll(() => produceCount(page), { timeout: 30_000 }).toBe(0);
  await expect.poll(() => wallet(page), { timeout: 30_000 }).toBe(walletBefore + 2500 * 15);
  await expect(shell(page)).toHaveAttribute('data-pending-ops', '0');
});
