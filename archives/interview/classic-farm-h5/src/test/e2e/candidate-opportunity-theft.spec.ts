import { expect, test, type Page } from '@playwright/test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const evidenceRoot = process.env.FARM_E2E_EVIDENCE_ROOT ?? 'test-results/candidate-opportunity-theft';
const password = 'REPLACE_WITH_LOCAL_SECRET';
const suffix = `${Date.now()}`.slice(-9);
const userA = `canda${suffix}`;
const userB = `candb${suffix}`;
const shell = (page: Page) => page.locator('[data-app-shell]');
const seedCount = async (page: Page): Promise<bigint> => BigInt(await page.locator('[data-private]').getAttribute('data-seed-count') ?? '0');

async function waitConnected(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
  await expect(shell(page)).toHaveAttribute('data-bootstrap-state', 'applied', { timeout: 30_000 });
  await expect(shell(page)).toHaveAttribute('data-recovery-barrier', 'open');
  await expect(shell(page)).toHaveAttribute('data-join-phase', 'READY');
}

async function register(page: Page, username: string): Promise<string> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  await page.locator('[data-register]').click();
  await waitConnected(page);
  const owner = await shell(page).getAttribute('data-own-owner-id');
  if (!owner) throw new Error('registered account has no owner id');
  return owner;
}

async function login(page: Page, username: string): Promise<void> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  await page.locator('[data-login]').click();
  await waitConnected(page);
}

async function openUtility(page: Page, mode: 'inventory' | 'shop'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  if (await page.locator('[data-utility-sheet]').isVisible()) await page.locator('[data-close-utility]').click();
}

async function buySeed(page: Page): Promise<void> {
  const before = await seedCount(page);
  await openUtility(page, 'shop');
  await page.locator('[data-shop-count]').fill('1');
  await page.locator('[data-buy-offer="1"]').click();
  await expect.poll(() => seedCount(page), { timeout: 20_000 }).toBe(before + 1n);
  await closeUtility(page);
}

async function plant(page: Page): Promise<string> {
  await page.locator('[data-quick-seed="1"]').click();
  const empty = page.locator('.plot.empty[data-plot-id]').first();
  const plotId = await empty.getAttribute('data-plot-id');
  if (!plotId) throw new Error('no empty plot');
  await empty.click();
  await expect(page.locator(`[data-plot-id="${plotId}"]`)).not.toHaveClass(/\bempty\b/, { timeout: 20_000 });
  return plotId;
}

async function produceCount(page: Page): Promise<number> {
  await openUtility(page, 'inventory');
  const crops = page.locator('[data-inventory-tab="crops"]');
  if (await crops.count()) await crops.click();
  const row = page.locator('[data-utility-sheet] .crop-item').filter({ hasText: '胡萝卜' }).first();
  await expect(row).toBeVisible();
  const count = Number((await row.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
  await closeUtility(page);
  return count;
}

async function rosterCard(page: Page, ownerId: string): Promise<{ action_mask: number; theft_eligible_targets: Array<{plot_id: number; crop_cycle_id: string}> }> {
  return await page.evaluate(async (owner) => {
    const response = await fetch('/api/v1/friends?page_size=20', { credentials: 'include' });
    if (!response.ok) throw new Error(`friend roster HTTP ${response.status}`);
    const body = await response.json() as { friends: Array<{ owner_id: string; action_mask: number; theft_eligible_targets: Array<{plot_id: number; crop_cycle_id: string}> }> };
    const card = body.friends.find((candidate) => candidate.owner_id === owner);
    if (!card) throw new Error('friend card missing');
    return card;
  }, ownerId);
}

test('mature WATER disappears and exact Theft controls consume authority terminal', async ({ browser, baseURL }) => {
  test.setTimeout(180_000);
  mkdirSync(evidenceRoot, { recursive: true });
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();
  const facts: Record<string, unknown> = {};
  try {
    await Promise.all([pageA.goto(baseURL!), pageB.goto(baseURL!)]);
    const [ownerA, ownerB] = await Promise.all([register(pageA, userA), register(pageB, userB)]);
    facts.ownerA = ownerA; facts.ownerB = ownerB;

    await buySeed(pageB);
    const plotId = await plant(pageB);
    facts.plotId = plotId;

    await pageA.locator('[data-share]').first().click();
    await expect(pageA.locator('[data-message]')).toContainText('分享链接已生成', { timeout: 20_000 });
    const shareText = await pageA.locator('[data-message]').textContent();
    const shareURL = shareText?.match(/https?:\/\/[^\s]+/)?.[0];
    if (!shareURL) throw new Error(`share URL missing: ${shareText}`);
    await pageB.goto(shareURL);
    await login(pageB, userB);
    await expect(pageA.locator('[data-friend-count]')).toHaveText('1', { timeout: 30_000 });
    await pageB.locator('[data-my-farm]').first().click();
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });

    await expect(pageB.locator(`[data-plot-id="${plotId}"]`)).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
    await pageA.locator('[data-open-friends]').first().click();
    const card = pageA.locator(`article.friend-row[data-friend-owner="${ownerB}"]`);
    await expect(card).toBeVisible();
    await expect.poll(async () => {
      const mask = Number(await card.getAttribute('data-friend-action-mask'));
      return (mask & 1) !== 0 && (mask & 2) === 0;
    }, { timeout: 30_000 }).toBe(true);
    const beforeCard = await rosterCard(pageA, ownerB);
    expect(beforeCard.action_mask & 1).toBe(1);
    expect(beforeCard.action_mask & 2).toBe(0);
    expect(beforeCard.theft_eligible_targets).toHaveLength(1);
    facts.targetBefore = beforeCard.theft_eligible_targets[0];

    await card.locator('button[data-friend-owner]').click();
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });
    await expect(shell(pageA)).toHaveAttribute('data-join-phase', 'READY');
    const pending = pageA.locator('[data-pending]');
    await pageA.locator('[data-tool="water"]').click();
    await pageA.locator(`[data-plot-id="${plotId}"]`).click();
    await expect(pageA.locator('[data-message]')).toContainText('该地块不需要浇水');
    await expect(pending).toContainText('暂无未决操作');
    await expect(pageA.locator('[data-theft]')).toBeEnabled();

    const produceBefore = await produceCount(pageA);
    await expect(pending).toContainText('暂无未决操作');
    await pageA.locator('[data-theft]').click();
    await expect(pageA.locator('[data-message]')).toContainText('本次尚未提交请求');
    await expect(pending).toContainText('暂无未决操作');
    await pageA.locator('[data-theft]').click();
    await expect(pageA.locator('[data-message]')).toContainText('偷菜成功', { timeout: 45_000 });
    await expect.poll(() => produceCount(pageA), { timeout: 45_000 }).toBe(produceBefore + 10);
    await expect(pending).toContainText('暂无未决操作', { timeout: 30_000 });

    await expect(pageA.locator('[data-theft]')).toHaveAttribute('aria-disabled', 'true', { timeout: 30_000 });
    await expect(pageA.locator('[data-theft]')).toContainText('本茬已偷过');
    await expect(pageA.locator('[data-theft]')).toHaveAttribute('title', /本农场当前没有可偷作物/);
    await expect(pageA.locator('[data-steal-all]')).toHaveAttribute('aria-disabled', 'true');
    await expect(pageA.locator('[data-steal-all]')).toContainText('本茬已偷过');
    await expect.poll(async () => (await rosterCard(pageA, ownerB)).theft_eligible_targets.length, { timeout: 30_000 }).toBe(0);
    const afterCard = await rosterCard(pageA, ownerB);
    facts.produceBefore = produceBefore;
    facts.produceAfter = await produceCount(pageA);
    facts.pending = await pending.textContent();
    facts.rosterAfter = afterCard;

    await Promise.all([
      pageA.screenshot({ path: path.join(evidenceRoot, 'visitor-after-theft.png'), fullPage: true }),
      pageB.screenshot({ path: path.join(evidenceRoot, 'owner-after-theft.png'), fullPage: true }),
    ]);
    writeFileSync(path.join(evidenceRoot, 'candidate-verdict.json'), JSON.stringify({ pass: true, ...facts }, null, 2));
  } catch (error) {
    writeFileSync(path.join(evidenceRoot, 'candidate-verdict.json'), JSON.stringify({ pass: false, error: error instanceof Error ? error.stack : String(error), ...facts }, null, 2));
    throw error;
  } finally {
    await contextA.close();
    await contextB.close();
  }
});
