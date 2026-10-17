import { expect, test, type Page } from '@playwright/test';

/**
 * 真实 Web 纵向用例骨架(非 mock):
 *   账号登录/免密换票 → WSS ClientHello → ServerHello → Bootstrap/RecoverPending
 *   → JoinOwnFarm → Snapshot/Ready → Buy → Plant → 切换好友农场 → (Care/Theft)
 *   → 返回自己农场。
 *
 * 运行前提(全部由环境变量注入,不写死凭证):
 *   FARM_E2E_API_BASE_URL  真实 Web API 地址(如 http://127.0.0.1:8080)
 *   FARM_E2E_WSS_URL       真实 WSS 地址(如 ws://127.0.0.1:8080/ws)
 *   FARM_E2E_USER_A / FARM_E2E_USER_B  两个测试账号(不存在则自动注册)
 *   FARM_E2E_PASSWORD      测试账号密码
 *
 * 示例:
 *   FARM_E2E_API_BASE_URL=http://127.0.0.1:8080 \
 *   FARM_E2E_WSS_URL=ws://127.0.0.1:8080/ws \
 *   FARM_E2E_USER_A=e2e_farmer_a FARM_E2E_USER_B=e2e_farmer_b \
 *   FARM_E2E_PASSWORD='…' \
 *   npx playwright test --config=playwright.web.config.ts web-real-vertical
 *
 * (playwright.web.config.ts 会把 FARM_E2E_API_BASE_URL / FARM_E2E_WSS_URL
 * 透传给它启动的 dev server 作为 API 代理目标与 WSS 地址;缺任一 FARM_E2E_*
 * 变量时本文件全部用例显式 skip,不以 mock 冒充 E2E。)
 */

const REQUIRED_ENV = [
  'FARM_E2E_API_BASE_URL',
  'FARM_E2E_WSS_URL',
  'FARM_E2E_USER_A',
  'FARM_E2E_USER_B',
  'FARM_E2E_PASSWORD',
] as const;

const missingEnv = REQUIRED_ENV.filter((name) => !process.env[name]);

const env = {
  userA: process.env.FARM_E2E_USER_A ?? '',
  userB: process.env.FARM_E2E_USER_B ?? '',
  password: process.env.FARM_E2E_PASSWORD ?? '',
};

// ---------------------------------------------------------------------------
// 可重用步骤
// ---------------------------------------------------------------------------

/** 登录;若账号不存在(登录失败提示)则注册后进入农场。 */
async function ensureAccount(page: Page, username: string): Promise<void> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(env.password);
  await page.locator('[data-login]').click();
  const status = page.locator('[data-status]');
  const authError = page.locator('[data-auth-message]');
  const outcome = await Promise.race([
    status.waitFor({ state: 'visible', timeout: 20_000 }).then(() => 'in' as const),
    authError.waitFor({ state: 'visible', timeout: 20_000 }).then(() => 'err' as const),
  ]).catch(() => 'timeout' as const);
  if (outcome === 'in') {
    await expect(status).toHaveText('InFarm');
    return;
  }
  if (outcome === 'timeout') throw new Error(`账号 ${username} 登录/注册均无响应`);
  await page.locator('[data-register]').click();
  await expect(status).toHaveText('InFarm', { timeout: 20_000 });
}

/** 屏障放开且资产就绪(真实后端下 Bootstrap 可能 feature-disabled,资产以回执为准)。 */
async function waitInFarm(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
  await expect(page.locator('[data-app-shell]')).toHaveAttribute('data-recovery-barrier', 'open');
}

/** 购买指定作物种子 count 份(BuyRequest.count 走共享数量输入)。 */
async function buySeeds(page: Page, offerId: number, count: number): Promise<void> {
  await page.locator('[data-open-utility]').click();
  await page.locator('[data-shop-count]').fill(String(count));
  await page.locator(`[data-buy-offer="${offerId}"]`).click();
  await expect(page.locator('[data-message]')).toContainText('购买成功', { timeout: 10_000 });
  await page.locator('[data-close-utility]').click();
}

/** 在首块空地播种指定作物;返回 plotId 供后续 harvest/care 步骤复用。 */
async function plantFirstEmptyPlot(page: Page, cropId: number): Promise<string> {
  await page.locator(`[data-quick-seed="${cropId}"]`).click();
  const emptyPlot = page.locator('.plot.empty[data-plot-id]').first();
  await expect(emptyPlot).toBeVisible();
  const plotId = await emptyPlot.getAttribute('data-plot-id');
  await emptyPlot.click();
  await expect(page.locator(`[data-plot-id="${plotId}"]`)).not.toHaveClass(/\bempty\b/, { timeout: 10_000 });
  return plotId!;
}

/** 打开好友面板并进入好友农场(通过 JOIN_FARM 切换 Subscription)。 */
async function visitFriendFarm(page: Page): Promise<string> {
  const ownOwner = await page.locator('[data-app-shell]').getAttribute('data-own-owner-id');
  await page.locator('[data-open-friends]').click();
  const entry = page.locator('[data-visit-friend]').first();
  await expect(entry).toBeVisible({ timeout: 10_000 });
  await entry.click();
  await expect(page.locator('[data-app-shell]')).not.toHaveAttribute('data-current-owner-id', ownOwner!, { timeout: 15_000 });
  return (await page.locator('[data-app-shell]').getAttribute('data-current-owner-id'))!;
}

/** 返回自己农场;成功后应重新拉取好友机会。 */
async function returnHome(page: Page, ownOwnerId: string): Promise<void> {
  await page.locator('[data-my-farm]').click();
  await expect(page.locator('[data-app-shell]')).toHaveAttribute('data-current-owner-id', ownOwnerId, { timeout: 15_000 });
  await expect(page.locator('[data-status]')).toHaveText('InFarm');
}

/** 成熟则收获(夹具前置不满足时返回 false,由调用方决定是否 skip)。 */
async function harvestIfMature(page: Page, plotId: string): Promise<boolean> {
  const plot = page.locator(`[data-plot-id="${plotId}"]`);
  if (!(await plot.evaluate((el) => el.classList.contains('mature')))) return false;
  await page.locator('[data-tool="harvest"]').click();
  await plot.click();
  await expect(page.locator('[data-message]')).toContainText('收获', { timeout: 10_000 });
  return true;
}

/** 在好友农场对首块未浇水生长中地块浇水(CARE WATER)。 */
async function waterFirstThirstyPlot(page: Page): Promise<boolean> {
  const target = page.locator('.plot:not(.empty):not(.mature):not(.watered)[data-plot-id]').first();
  if ((await target.count()) === 0) return false;
  await page.locator('[data-tool="water"]').click();
  await target.click();
  await expect(page.locator('[data-message]')).toContainText('浇水', { timeout: 10_000 });
  return true;
}

/** 好友农场有成熟作物则偷取(THEFT);无则返回 false。 */
async function stealIfAvailable(page: Page): Promise<boolean> {
  const target = page.locator('.plot.mature[data-plot-id]').first();
  if ((await target.count()) === 0) return false;
  await target.click();
  await expect(page.locator('[data-message]')).toContainText('收获', { timeout: 10_000 });
  return true;
}

// ---------------------------------------------------------------------------

test.describe('real web vertical', () => {
  test.skip(missingEnv.length > 0, `缺少真实环境变量: ${missingEnv.join(', ')}(不设 mock,显式 skip)`);

  test('login → bootstrap/join → buy/plant → friend visit → return home', async ({ browser, baseURL }) => {
    test.setTimeout(120_000);
    const contextA = await browser.newContext();
    const contextB = await browser.newContext();
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();
    try {
      // 1) 登录/注册两个账号(B 用于建立好友关系)。
      await pageA.goto(baseURL!);
      await ensureAccount(pageA, env.userA);
      await waitInFarm(pageA);
      const ownerA = (await pageA.locator('[data-app-shell]').getAttribute('data-own-owner-id'))!;

      await pageB.goto(baseURL!);
      await ensureAccount(pageB, env.userB);
      await waitInFarm(pageB);

      // 2) 通过分享链接建立好友关系(若已是好友则直接进入下一步)。
      await pageA.locator('[data-share]').first().click();
      await expect(pageA.locator('[data-message]')).toContainText('分享链接已生成');
      const shareURL = (await pageA.locator('[data-message]').textContent())?.match(/https?:\/\/\S+/)?.[0];
      expect(shareURL).toBeTruthy();
      await pageB.goto(shareURL!);
      await pageB.locator('[data-web-username]').fill(env.userB);
      await pageB.locator('[data-web-password]').fill(env.password);
      await pageB.locator('[data-login]').click();
      await expect(pageB.locator('[data-status]')).toHaveText('InFarm', { timeout: 20_000 });
      await expect(pageA.locator('[data-friend-count]')).not.toHaveText('0', { timeout: 15_000 });

      // 3) 自己农场:购买 2 份胡萝卜种子并播种。
      await buySeeds(pageA, 1, 2);
      const plotId = await plantFirstEmptyPlot(pageA, 1);

      // 4) 切到好友农场(JoinFarm 切换 Subscription),尝试浇水/偷菜。
      const friendOwner = await visitFriendFarm(pageA);
      expect(friendOwner).not.toBe(ownerA);
      const watered = await waterFirstThirstyPlot(pageA);
      const stolen = await stealIfAvailable(pageA);
      if (!watered && !stolen) {
        test.info().annotations.push({
          type: 'fixture-gap',
          description: '好友农场暂无可浇水/可偷取地块(需测试夹具准备生长中/成熟作物)',
        });
      }

      // 5) 返回自己农场;若作物已成熟则收获(成熟依赖后端生长计时夹具)。
      await returnHome(pageA, ownerA);
      const harvested = await harvestIfMature(pageA, plotId);
      if (!harvested) {
        test.info().annotations.push({
          type: 'fixture-gap',
          description: '作物尚未成熟(需成熟条件夹具或等待生长周期)',
        });
      }

      // 6) 错误可观测性:整个链路不得产生公开错误面。
      const lastError = await pageA.locator('[data-app-shell]').getAttribute('data-last-error-code');
      expect(lastError ?? '').toBe('');
    } finally {
      await contextA.close().catch(() => undefined);
      await contextB.close().catch(() => undefined);
    }
  });

  // 好友 Join 被拒(ERR_NOT_FRIEND)的真实链路需要"非好友目标农场"夹具与
  // 可导航入口;页面当前只对好友可见,故此处显式 skip 并记录缺口,不以
  // mock/脚本注入冒充真实 E2E。该路径已有单元级覆盖:
  // ws-client-recovery.test.ts「friend Join is rejected」。
  test('friend join rejection is diagnosable and never bounces to login', async () => {
    test.skip(true, '需要非好友目标农场夹具与可导航入口(单元测试已覆盖归因/回退语义)');
  });
});
