import { expect, test, type Page } from '@playwright/test';

const users = [
  process.env.FARM_E2E_USER_A ?? '',
  process.env.FARM_E2E_USER_B ?? '',
  process.env.FARM_E2E_USER_C ?? '',
];
const password = process.env.FARM_E2E_PASSWORD ?? '';
const shell = (page: Page) => page.locator('[data-app-shell]');

async function waitReady(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
  await expect(shell(page)).toHaveAttribute('data-join-phase', 'READY');
  await expect(shell(page)).toHaveAttribute('data-recovery-barrier', 'open');
}

async function account(page: Page, mode: 'register' | 'login', username: string): Promise<string> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  await page.locator(mode === 'register' ? '[data-register]' : '[data-login]').click();
  await waitReady(page);
  const owner = await shell(page).getAttribute('data-own-owner-id');
  if (!owner) throw new Error(`${username} has no owner id`);
  return owner;
}

async function buyPlantAndMature(page: Page): Promise<void> {
  await page.locator('[data-open-utility="shop"]').first().click();
  await page.locator('[data-buy-offer="1"]').click();
  await expect.poll(async () => BigInt(await page.locator('[data-private]').getAttribute('data-seed-count') ?? '0')).toBeGreaterThan(0n);
  await page.locator('[data-close-utility]').click();
  await page.locator('[data-quick-seed="1"]').click();
  const empty = page.locator('.plot.empty[data-plot-id]').first();
  const plotID = await empty.getAttribute('data-plot-id');
  if (!plotID) throw new Error('empty plot missing id');
  await empty.click();
  const plot = page.locator(`[data-plot-id="${plotID}"]`);
  await expect(plot).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
}

async function acceptShare(page: Page, url: string, username: string, inviterOwner: string): Promise<void> {
  await page.goto(url);
  await account(page, 'login', username);
  await expect(shell(page)).toHaveAttribute('data-current-owner-id', inviterOwner, { timeout: 30_000 });
  await page.locator('[data-my-farm]').first().click();
  await waitReady(page);
}

test('server-owned A → B → C navigation and explicit NONE', async ({ browser, baseURL }) => {
  test.skip(users.some((user) => !user) || !password || !baseURL, 'three real social users are required');
  test.setTimeout(180_000);
  const contexts = await Promise.all([browser.newContext(), browser.newContext(), browser.newContext()]);
  try {
    const [pageA, pageB, pageC] = await Promise.all(contexts.map((context) => context.newPage()));
    await Promise.all([pageA.goto(baseURL!), pageB.goto(baseURL!), pageC.goto(baseURL!)]);
    const [ownerA, ownerB, ownerC] = await Promise.all([
      account(pageA, 'register', users[0]), account(pageB, 'register', users[1]), account(pageC, 'register', users[2]),
    ]);
    await Promise.all([buyPlantAndMature(pageB), buyPlantAndMature(pageC)]);

    await pageA.locator('[data-share]').first().click();
    await expect(pageA.locator('[data-message]')).toContainText('分享链接已生成');
    const shareURL = (await pageA.locator('[data-message]').textContent())?.match(/https?:\/\/[^\s]+/)?.[0];
    if (!shareURL) throw new Error('share URL missing');
    await acceptShare(pageB, shareURL, users[1], ownerA);
    await acceptShare(pageC, shareURL, users[2], ownerA);
    await expect(pageA.locator('[data-friend-count]')).toHaveText('2', { timeout: 30_000 });

    await pageA.locator('[data-open-friends]').first().click();
    await expect(pageA.locator('article.friend-row')).toHaveCount(2);
    await pageA.locator('[data-visit-friend]').click();
    await waitReady(pageA);
    const first = await shell(pageA).getAttribute('data-current-owner-id');
    expect(new Set([ownerB, ownerC]).has(first ?? '')).toBe(true);

    await pageA.locator('[data-open-friends]').first().click();
    await pageA.locator('[data-visit-friend]').click();
    await waitReady(pageA);
    const second = await shell(pageA).getAttribute('data-current-owner-id');
    expect(new Set([ownerB, ownerC]).has(second ?? '')).toBe(true);
    expect(second).not.toBe(first);

    await pageA.locator('[data-open-friends]').first().click();
    await pageA.locator('[data-visit-friend]').click();
    await expect(pageA.locator('[data-message]')).toContainText('本轮暂无未拜访', { timeout: 30_000 });
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', second!);

    await pageA.locator('[data-previous-friend]').click();
    await waitReady(pageA);
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', first!);
  } finally {
    await Promise.all(contexts.map((context) => context.close()));
  }
});
