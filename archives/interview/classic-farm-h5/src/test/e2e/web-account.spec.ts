import { expect, test, type Page } from '@playwright/test';

const password = 'farm-test-password-2026';

async function submitAccount(page: Page, mode: 'login' | 'register', username: string): Promise<void> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  await page.locator(mode === 'register' ? '[data-register]' : '[data-login]').click();
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
}

test('Redis-backed Web accounts register, log in again, and become friends', async ({ browser, baseURL }) => {
  test.setTimeout(60_000);
  const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  const firstUsername = `farmer_${suffix}_a`;
  const secondUsername = `farmer_${suffix}_b`;
  const firstContext = await browser.newContext();
  const secondContext = await browser.newContext();
  const first = await firstContext.newPage();
  const second = await secondContext.newPage();
  try {
    await first.goto(baseURL!);
    await submitAccount(first, 'register', firstUsername);
    const firstPlayerID = await first.locator('[data-app-shell]').getAttribute('data-own-owner-id');
    expect(Number(firstPlayerID)).toBeGreaterThan(1_000_000);

    await first.locator('[data-share]').first().click();
    await expect(first.locator('[data-message]')).toContainText('分享链接已生成');
    const shareMessage = await first.locator('[data-message]').textContent();
    const shareURL = shareMessage?.match(/https?:\/\/\S+/)?.[0];
    expect(shareURL).toBeTruthy();

    await second.goto(shareURL!);
    await submitAccount(second, 'register', secondUsername);
    const secondPlayerID = await second.locator('[data-app-shell]').getAttribute('data-own-owner-id');
    expect(Number(secondPlayerID)).toBeGreaterThan(1_000_000);
    expect(secondPlayerID).not.toBe(firstPlayerID);
    await expect(first.locator('[data-friend-count]')).toHaveText('1', { timeout: 10_000 });
    await expect(second.locator('[data-friend-count]')).toHaveText('1', { timeout: 10_000 });

    // A fresh browser context proves the credential record and player mapping
    // survive beyond the registering page and return the same player_id.
    await firstContext.close();
    const reloginContext = await browser.newContext();
    const relogin = await reloginContext.newPage();
    try {
      await relogin.goto(baseURL!);
      await submitAccount(relogin, 'login', firstUsername);
      await expect(relogin.locator('[data-app-shell]')).toHaveAttribute('data-own-owner-id', firstPlayerID!);
      await expect(relogin.locator('[data-friend-count]')).toHaveText('1');
    } finally {
      await reloginContext.close();
    }
  } finally {
    await firstContext.close().catch(() => undefined);
    await secondContext.close();
  }
});
