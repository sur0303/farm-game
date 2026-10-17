import { expect, test, type Page } from '@playwright/test';

const user = process.env.FARM_E2E_USER_A ?? '';
const password = process.env.FARM_E2E_PASSWORD ?? '';
const shell = (page: Page) => page.locator('[data-app-shell]');
const asBigInt = (value: string | null): bigint => BigInt(value || '0');
const seedCount = async (page: Page): Promise<bigint> =>
  asBigInt(await page.locator('[data-private]').getAttribute('data-seed-count'));
const walletCount = async (page: Page): Promise<bigint> =>
  asBigInt(await page.locator('[data-private]').getAttribute('data-wallet-count'));

async function waitConnected(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
  await expect(shell(page)).toHaveAttribute('data-bootstrap-state', 'applied', { timeout: 30_000 });
  await expect(shell(page)).toHaveAttribute('data-recovery-barrier', 'open');
}

async function register(page: Page): Promise<void> {
  await page.locator('[data-web-username]').fill(user);
  await page.locator('[data-web-password]').fill(password);
  const response = page.waitForResponse((candidate) => candidate.url().includes('/api/v1/accounts/register'));
  await page.locator('[data-register]').click();
  expect((await response).ok()).toBe(true);
  await waitConnected(page);
}

async function openUtility(page: Page, mode: 'inventory' | 'shop'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  if (await page.locator('[data-utility-sheet]').isVisible()) {
    await page.locator('[data-close-utility]').click();
  }
}

async function buySeeds(page: Page, count: number): Promise<void> {
  const before = await seedCount(page);
  await openUtility(page, 'shop');
  await page.locator('[data-shop-count]').fill(String(count));
  await page.locator('[data-buy-offer="1"]').click();
  await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(before + BigInt(count));
  await closeUtility(page);
}

async function plantFirstEmpty(page: Page): Promise<string> {
  const before = await seedCount(page);
  await page.locator('[data-quick-seed="1"]').click();
  const plot = page.locator('.plot.empty[data-plot-lock-state="UNLOCKED"]').first();
  const plotId = await plot.getAttribute('data-plot-id');
  if (!plotId) throw new Error('no unlocked empty plot');
  await plot.click();
  await expect(page.locator(`[data-plot-id="${plotId}"]`)).not.toHaveClass(/\bempty\b/, { timeout: 30_000 });
  await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(before - 1n);
  return plotId;
}

async function waitMature(page: Page, plotId: string): Promise<void> {
  await expect(page.locator(`[data-plot-id="${plotId}"]`))
    .toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
}

async function sellProduce(page: Page, count: number): Promise<void> {
  const before = await walletCount(page);
  await openUtility(page, 'inventory');
  const row = page.locator('[data-utility-sheet] .crop-item').filter({ hasText: '胡萝卜' }).first();
  await expect(row).toBeVisible();
  await row.locator('[data-sell-count]').fill(String(count));
  await row.locator('[data-sell-item]').click();
  await expect.poll(() => walletCount(page), { timeout: 30_000 }).toBeGreaterThan(before);
  await closeUtility(page);
}

test('sequential unlock publishes each next candidate without relogin', async ({ page, baseURL }) => {
  test.skip(!user || !password || !baseURL, 'real credentials/baseURL are required');
  test.setTimeout(180_000);
  await page.goto(baseURL!);
  await register(page);
  await buySeeds(page, 2);
  const plots = [await plantFirstEmpty(page), await plantFirstEmpty(page)];
  await Promise.all(plots.map((plotId) => waitMature(page, plotId)));
  for (const plotId of plots) {
    await page.locator('[data-tool="harvest"]').click();
    await page.locator(`[data-plot-id="${plotId}"]`).click();
    await expect(page.locator(`[data-plot-id="${plotId}"]`)).toHaveClass(/\bempty\b/, { timeout: 30_000 });
  }
  await sellProduce(page, 30);

  const plot6 = page.locator('[data-plot-id="plot-5"]');
  const plot7 = page.locator('[data-plot-id="plot-6"]');
  const plot8 = page.locator('[data-plot-id="plot-7"]');
  await expect(plot6).toHaveAttribute('data-plot-unlock-candidate', 'true');
  const walletBefore6 = await walletCount(page);
  await plot6.locator('[data-unlock-plot]').click();
  await expect(plot6).toHaveAttribute('data-plot-lock-state', 'UNLOCKED', { timeout: 45_000 });
  await expect.poll(() => walletCount(page), { timeout: 30_000 }).toBe(walletBefore6 - 100n);
  await expect(page.locator('[data-message]')).toContainText('可继续解锁第 7 块土地', { timeout: 30_000 });
  await expect(plot7).toHaveAttribute('data-plot-unlock-candidate', 'true');
  await expect(plot7.locator('[data-unlock-plot]')).toBeVisible();

  const walletBefore7 = await walletCount(page);
  await plot7.locator('[data-unlock-plot]').click();
  await expect(plot7).toHaveAttribute('data-plot-lock-state', 'UNLOCKED', { timeout: 45_000 });
  await expect.poll(() => walletCount(page), { timeout: 30_000 }).toBe(walletBefore7 - 200n);
  await expect(page.locator('[data-message]')).toContainText('可继续解锁第 8 块土地', { timeout: 30_000 });
  await expect(plot8).toHaveAttribute('data-plot-unlock-candidate', 'true');
  await expect(shell(page)).toHaveAttribute('data-pending-ops', '0', { timeout: 30_000 });
});
