import { expect, test, type Page } from '@playwright/test';

test.describe.configure({ mode: 'serial' });

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

async function expectCodexUnlocked(page: Page, cropDefinitionId = 1): Promise<void> {
  await openUtility(page, 'codex');
  await expect(page.locator(`[data-codex-crop="${cropDefinitionId}"]`)).toHaveClass(/unlocked/);
  await closeUtility(page);
}

async function openUtility(page: Page, mode: 'inventory' | 'shop' | 'codex' | 'more'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  // App toggles the same mode closed; always close first when switching.
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
}

async function ensurePurchaseFunds(page: Page): Promise<void> {
  let wallet = await inventoryCount(page, '金币');
  if (Number.isNaN(wallet)) {
    // Bootstrap feature-disabled:首屏资产未知,首笔 BUY 回包后读数才可用。
    await openUtility(page, 'shop');
    await page.locator('[data-buy]').click();
    await expect.poll(() => inventoryCount(page, '金币'), { timeout: 10_000 }).not.toBeNaN();
    await closeUtility(page);
    wallet = await inventoryCount(page, '金币');
  }
  if (wallet >= 10) return;
  const carrots = await inventoryCount(page, '胡萝卜');
  expect(carrots, 'the persistent staging account needs one carrot to replenish purchase funds').toBeGreaterThan(0);
  await openUtility(page, 'inventory');
  await page.locator('[data-sell-item]:not([disabled])').first().click();
  await expect.poll(() => inventoryCount(page, '金币')).toBeGreaterThanOrEqual(10);
  await closeUtility(page);
}

test('real Direct WSS personal and economy loop survives reconnect', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: '进入经典农场' })).toBeVisible();
  await page.getByRole('button', { name: '使用 Staging 账号登录' }).click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
  await expect(page.locator('[data-farm-title]')).toHaveText('我的农场');
  await expect(page.locator('[data-version]')).toContainText('farm_version');
  await ensurePurchaseFunds(page);
  const coinBeforeBuy = await inventoryCount(page, '金币');
  const seedsBeforeBuy = await inventoryCount(page, '种子');
  await openUtility(page, 'shop');
  await page.locator('[data-buy]').click();
  await expect.poll(() => inventoryCount(page, '金币')).toBe(coinBeforeBuy - 10);
  await expect.poll(() => inventoryCount(page, '种子')).toBe(seedsBeforeBuy + 1);
  await closeUtility(page);

  const emptyPlot = page.locator('.plot.empty').first();
  const plotID = await emptyPlot.getAttribute('data-plot-id');
  expect(plotID).toBeTruthy();
  const firstPlot = page.locator(`[data-plot-id="${plotID}"]`);
  await page.locator('[data-tool="plant"]').click();
  await emptyPlot.click();
  await expect(firstPlot).toHaveClass(/growing/);
  await expect(firstPlot).toContainText('已成熟', { timeout: 15_000 });

  const carrotsBeforeHarvest = await inventoryCount(page, '胡萝卜');
  await page.locator('[data-tool="harvest"]').click();
  await firstPlot.click();
  await expect(firstPlot).toHaveClass(/empty/);
  await expect.poll(() => inventoryCount(page, '胡萝卜')).toBe(carrotsBeforeHarvest + 10);
  await expectCodexUnlocked(page);

  await openUtility(page, 'inventory');
  await page.locator('[data-sell-item]:not([disabled])').first().click();
  await expect.poll(() => inventoryCount(page, '金币')).toBe(coinBeforeBuy + 5);
  await expect.poll(() => inventoryCount(page, '胡萝卜')).toBe(carrotsBeforeHarvest + 9);
  await closeUtility(page);

  await openUtility(page, 'more');
  await page.locator('[data-reconnect]').click();
  await expect(page.locator('[data-status]')).not.toHaveText('InFarm');
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
  await expect(page.locator('[data-farm-title]')).toHaveText('我的农场');
  await closeUtility(page);
  await expectCodexUnlocked(page);
});

test('friend roster loads on login and updates through the authenticated downlink', async ({ browser, baseURL }) => {
  test.setTimeout(45_000);
  const ownerContext = await browser.newContext();
  const friendContext = await browser.newContext();
  const owner = await ownerContext.newPage();
  const visitor = await friendContext.newPage();
  try {
    await owner.goto(`${baseURL}/?demo_account=player-a`);
    await owner.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(owner.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
    await expect(owner.locator('[data-friends]')).toHaveAttribute('data-state', 'ready');

    await owner.locator('[data-open-friends]').first().click();
    const existingFriend = owner.locator('[data-friend-owner="10002"]');
    if (await existingFriend.isVisible()) {
      await owner.locator('[data-revoke-owner="10002"]').click();
      await expect(existingFriend).toBeHidden();
    }
    await owner.locator('[data-close-friends]').last().click();

    await owner.locator('[data-share]').first().click();
    await expect(owner.locator('[data-message]')).toContainText('分享链接已生成');
    const message = await owner.locator('[data-message]').textContent();
    const shareURL = message?.match(/https?:\/\/\S+/)?.[0];
    expect(shareURL).toBeTruthy();

    const recipientURL = new URL(shareURL!);
    recipientURL.searchParams.set('demo_account', 'player-b');
    await visitor.goto(recipientURL.toString());
    await visitor.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
    await expect(visitor.locator('[data-friends]')).toHaveAttribute('data-state', 'ready');
    await expect(owner.locator('[data-friend-count]')).toHaveText('1', { timeout: 10_000 });
    await expect(visitor.locator('[data-friend-count]')).toHaveText('1', { timeout: 10_000 });
    await expect(owner.locator('[data-toast]')).toContainText('已与玩家10002成为好友');
    await owner.locator('[data-open-friends]').first().click();
    await visitor.locator('[data-open-friends]').first().click();
    await expect(owner.locator('[data-friend-owner="10002"]')).toBeVisible();
    await expect(visitor.locator('[data-friend-owner="10001"]')).toBeVisible();
  } finally {
    await ownerContext.close();
    await friendContext.close();
  }
});

test('a replacement login stops the old page instead of leaving stale InFarm state', async ({ browser, baseURL }) => {
  const oldContext = await browser.newContext();
  const replacementContext = await browser.newContext();
  const oldPage = await oldContext.newPage();
  const replacementPage = await replacementContext.newPage();
  try {
    await oldPage.goto(`${baseURL}/?demo_account=player-a`);
    await oldPage.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(oldPage.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });

    await replacementPage.goto(`${baseURL}/?demo_account=player-a`);
    await replacementPage.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(replacementPage.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });

    await expect(oldPage.locator('[data-auth-screen]')).toBeVisible({ timeout: 10_000 });
    await expect(oldPage.locator('[data-auth-message]')).toContainText('账号已在其他页面登录');
    await expect(oldPage.locator('[data-app-shell]')).toBeHidden();
  } finally {
    await oldContext.close();
    await replacementContext.close();
  }
});

test('friend navigation returns to the authenticated owner after reconnect', async ({ page }) => {
  await page.goto('/?demo_account=player-b');
  await page.getByRole('button', { name: '使用 Staging 账号登录' }).click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
  await expect(page.locator('[data-app-shell]')).toHaveAttribute('data-own-owner-id', '10002');
  await page.locator('[data-open-friends]').first().click();
  await page.locator('[data-friend-owner="10001"]').click();
  await expect(page.locator('[data-farm-title]')).toHaveText('好友农场 · 10001');

  await openUtility(page, 'more');
  await page.locator('[data-reconnect]').click();
  await expect(page.locator('[data-status]')).not.toHaveText('InFarm');
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
  await page.locator('[data-my-farm]').first().click();
  await expect(page.locator('[data-app-shell]')).toHaveAttribute('data-last-join-target', '10002');
  await expect(page.locator('[data-app-shell]')).toHaveAttribute('data-last-join-status', 'InFarm');
  await expect(page.locator('[data-farm-title]')).toHaveText('我的农场');
});

test('two authenticated browser profiles accept a share and receive the same farm changes', async ({ browser, baseURL }) => {
  test.setTimeout(90_000);
  const ownerContext = await browser.newContext();
  const friendContext = await browser.newContext();
  const owner = await ownerContext.newPage();
  const visitor = await friendContext.newPage();
  try {
    await owner.goto(`${baseURL}/?demo_account=player-a`);
    await owner.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(owner.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
    await expect(owner.locator('[data-friends]')).toHaveAttribute('data-state', 'ready');

    // Start from no relationship so the later assertion proves the
    // FRIEND_ROSTER_CHANGED downlink, rather than a pre-existing Redis row.
    await owner.locator('[data-open-friends]').first().click();
    const existingFriend = owner.locator('[data-friend-owner="10002"]');
    if (await existingFriend.isVisible()) {
      await owner.locator('[data-revoke-owner="10002"]').click();
      await expect(existingFriend).toBeHidden();
    }
    await owner.locator('[data-close-friends]').last().click();

    // Ensure there is a fresh crop for the visitor's care/theft checks. This
    // test runs after the personal loop in a clean Compose environment.
    await ensurePurchaseFunds(owner);
    await openUtility(owner, 'shop');
    await owner.locator('[data-buy]').click();
    await closeUtility(owner);
    const ownerPlot = owner.locator('.plot.empty').first();
    const plotID = await ownerPlot.getAttribute('data-plot-id');
    expect(plotID).toBeTruthy();
    await owner.locator('[data-tool="plant"]').click();
    await ownerPlot.click();
    await expect(owner.locator(`[data-plot-id="${plotID}"]`)).toHaveClass(/growing/);

    await owner.locator('[data-share]').first().click();
    await expect(owner.locator('[data-message]')).toContainText('分享链接已生成', { timeout: 10_000 });
    const message = await owner.locator('[data-message]').textContent();
    const shareURL = message?.match(/https?:\/\/\S+/)?.[0];
    expect(shareURL).toBeTruthy();

    const recipientURL = new URL(shareURL!);
    recipientURL.searchParams.set('demo_account', 'player-b');
    await visitor.goto(recipientURL.toString());
    await expect(visitor.locator('[data-staging-account]')).toHaveValue('player-b');
    await visitor.getByRole('button', { name: '使用 Staging 账号登录' }).click();
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
    await expect(visitor.locator('[data-farm-title]')).toHaveText('好友农场 · 10001', { timeout: 20_000 });
    await expect(owner.locator('[data-friend-count]')).toHaveText('1', { timeout: 10_000 });
    await expect(owner.locator('[data-toast]')).toContainText('已与玩家10002成为好友');
    await owner.locator('[data-open-friends]').first().click();
    await expect(owner.locator('[data-friend-owner="10002"]')).toBeVisible();
    await owner.locator('[data-close-friends]').last().click();

    const visitorPlot = visitor.locator(`[data-plot-id="${plotID}"]`);
    await visitor.locator('[data-tool="water"]').click();
    await visitorPlot.click();
    await expect(visitorPlot).toHaveClass(/watered/);
    await expect(owner.locator(`[data-plot-id="${plotID}"]`)).toHaveClass(/watered/);

    const beforeTheftVersion = await owner.locator('[data-version]').textContent();
    await expect(visitorPlot).toContainText('已成熟', { timeout: 15_000 });
    await visitorPlot.click();
    await visitor.locator('[data-theft]').click();
    await expect(owner.locator('[data-version]')).not.toHaveText(beforeTheftVersion ?? '', { timeout: 10_000 });

    // Restore the selected plot to empty so this scenario remains repeatable
    // against the persistent local Redis development adapter.
    await owner.locator('[data-tool="harvest"]').click();
    await owner.locator(`[data-plot-id="${plotID}"]`).click();
    await expect(owner.locator(`[data-plot-id="${plotID}"]`)).toHaveClass(/empty/);

    await openUtility(visitor, 'more');
    await visitor.locator('[data-reconnect]').click();
    await expect(visitor.locator('[data-status]')).not.toHaveText('InFarm');
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
    await expect(visitor.locator('[data-app-shell]')).toHaveAttribute('data-own-owner-id', '10002');
    await visitor.locator('[data-open-friends]').first().click();
    await expect(visitor.locator('[data-friend-owner="10001"]')).toBeVisible({ timeout: 10_000 });

    // Ordinary farm navigation must not reopen the connection RecoveryBarrier.
    await visitor.locator('[data-close-friends]').last().click();
    await visitor.locator('[data-my-farm]').first().click();
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm');
    await expect(visitor.locator('[data-farm-title]')).toHaveText('我的农场');
    await visitor.locator('[data-open-friends]').first().click();
    await visitor.locator('[data-friend-owner="10001"]').click();
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm');
    await expect(visitor.locator('[data-farm-title]')).toHaveText('好友农场 · 10001');

    // Reopening the roster while already visiting this farm must not issue a
    // second JoinFarm. The UI disables the action and the application guard
    // remains idempotent if stale DOM bypasses that presentation state.
    await visitor.locator('[data-open-friends]').first().click();
    const currentFriend = visitor.locator('[data-friend-owner="10001"]');
    await expect(currentFriend).toBeDisabled();
    await expect(currentFriend).toHaveText('当前农场');
    await expect(currentFriend.locator('xpath=ancestor::article').locator('.friend-copy small')).toHaveText('正在拜访');
    await visitor.locator('[data-app-shell]').evaluate((element) => { element.dataset.lastJoinTarget = 'same-farm-sentinel'; });
    await currentFriend.evaluate((button) => {
      button.removeAttribute('disabled');
      (button as HTMLButtonElement).click();
    });
    await expect(visitor.getByRole('dialog', { name: '好友' })).toBeHidden();
    await expect(visitor.locator('[data-app-shell]')).toHaveAttribute('data-last-join-target', 'same-farm-sentinel');
    await expect(visitor.locator('[data-status]')).toHaveText('InFarm');
  } finally {
    await ownerContext.close();
    await friendContext.close();
  }
});
