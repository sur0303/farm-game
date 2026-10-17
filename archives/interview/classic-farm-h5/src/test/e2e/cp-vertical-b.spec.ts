import { expect, test, type Browser, type BrowserContext, type Page } from '@playwright/test';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const userA = process.env.FARM_E2E_USER_A ?? '';
const userB = process.env.FARM_E2E_USER_B ?? '';
const password = process.env.FARM_E2E_PASSWORD ?? '';
const controlDir = process.env.FARM_E2E_CONTROL_DIR ?? '';
const apiBaseURL = process.env.FARM_E2E_API_BASE_URL ?? '';
const faultAdminURL = process.env.FARM_E2E_FAULT_ADMIN_URL ?? '';
const evidenceRoot = process.env.FARM_E2E_EVIDENCE_ROOT ?? 'test-results/cp-vertical-b';
const screensDir = path.join(evidenceRoot, 'screenshots');
const videosDir = path.join(evidenceRoot, 'videos');

const shell = (page: Page) => page.locator('[data-app-shell]');
const asBigInt = (value: string | null): bigint => BigInt(value || '0');
const shellBigInt = async (page: Page, attribute: string): Promise<bigint> =>
  asBigInt(await shell(page).getAttribute(attribute));
const seedCount = async (page: Page): Promise<bigint> =>
  asBigInt(await page.locator('[data-private]').getAttribute('data-seed-count'));

async function waitConnected(page: Page): Promise<void> {
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 45_000 });
  await expect(shell(page)).toHaveAttribute('data-bootstrap-state', 'applied', { timeout: 30_000 });
  await expect(shell(page)).toHaveAttribute('data-private-ready', 'true');
  await expect(shell(page)).toHaveAttribute('data-recovery-barrier', 'open');
  await expect(shell(page)).toHaveAttribute('data-join-phase', 'READY');
}

async function submitAccount(page: Page, mode: 'register' | 'login', username: string, expectOwnFarm = true): Promise<string> {
  await page.locator('[data-web-username]').fill(username);
  await page.locator('[data-web-password]').fill(password);
  const response = page.waitForResponse((candidate) => candidate.url().includes(
    mode === 'register' ? '/api/v1/accounts/register' : '/api/v1/session/login',
  ), { timeout: 30_000 });
  await page.locator(mode === 'register' ? '[data-register]' : '[data-login]').click();
  const result = await response;
  if (!result.ok()) throw new Error(`${mode} ${username} failed: HTTP ${result.status()} ${await result.text()}`);
  await waitConnected(page);
  const ownerId = await shell(page).getAttribute('data-own-owner-id');
  if (!ownerId) throw new Error(`${username} connected without own owner id`);
  if (expectOwnFarm) {
    await expect(shell(page)).toHaveAttribute('data-current-owner-id', ownerId);
    await expect(shell(page)).toHaveAttribute('data-profile-player-id', ownerId);
    await expect(shell(page)).toHaveAttribute('data-owner-profile-source', 'snapshot-owner-public-profile');
  }
  return ownerId;
}

async function openUtility(page: Page, mode: 'inventory' | 'shop' | 'tasks' | 'more'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
}

async function produceCount(page: Page, cropName = '胡萝卜'): Promise<number> {
  // A recovery can leave the diagnostic "more" sheet open. Always select the
  // inventory mode explicitly instead of treating any visible utility sheet as
  // an inventory sheet.
  await openUtility(page, 'inventory');
  const sheet = page.locator('[data-utility-sheet]');
  const cropsTab = page.locator('[data-inventory-tab="crops"]');
  if (await cropsTab.count()) await cropsTab.click();
  const row = sheet.locator('.crop-item').filter({ hasText: cropName }).first();
  await expect(row).toBeVisible();
  const value = Number((await row.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
  await closeUtility(page);
  return value;
}

async function buySeeds(page: Page, count: number): Promise<void> {
  const before = await seedCount(page);
  await openUtility(page, 'shop');
  await page.locator('[data-shop-count]').fill(String(count));
  await page.locator('[data-buy-offer="1"]').click();
  await expect.poll(() => seedCount(page), { timeout: 20_000 }).toBe(before + BigInt(count));
  await closeUtility(page);
}

async function plantFirstEmpty(page: Page): Promise<string> {
  const before = await seedCount(page);
  await page.locator('[data-quick-seed="1"]').click();
  const empty = page.locator('.plot.empty[data-plot-id]').first();
  const plotId = await empty.getAttribute('data-plot-id');
  if (!plotId) throw new Error('no empty plot available');
  await empty.click();
  const planted = page.locator(`[data-plot-id="${plotId}"]`);
  await expect(planted).not.toHaveClass(/\bempty\b/, { timeout: 20_000 });
  await expect.poll(() => seedCount(page), { timeout: 20_000 }).toBe(before - 1n);
  return plotId;
}

async function waitMature(page: Page, plotId: string, source: 'trusted-time' | 'trusted-time-or-wire' = 'trusted-time'): Promise<void> {
  const plot = page.locator(`[data-plot-id="${plotId}"]`);
  await expect(plot).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
  if (source === 'trusted-time') {
    await expect(plot).toHaveAttribute('data-crop-phase-source', 'trusted-time');
  } else {
    await expect.poll(() => plot.getAttribute('data-crop-phase-source')).toMatch(/^(trusted-time|wire)$/);
  }
}

async function saveMilestone(name: string, owner: Page, helper: Page): Promise<void> {
  mkdirSync(screensDir, { recursive: true });
  await Promise.all([
    owner.screenshot({ path: path.join(screensDir, `${name}-a.png`), fullPage: true }),
    helper.screenshot({ path: path.join(screensDir, `${name}-b.png`), fullPage: true }),
  ]);
}

async function startEvidenceContext(browser: Browser, _name: string): Promise<BrowserContext> {
  mkdirSync(videosDir, { recursive: true });
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    recordVideo: { dir: videosDir, size: { width: 1280, height: 720 } },
  });
  return context;
}

async function stopEvidenceContext(context: BrowserContext | undefined, _name: string): Promise<void> {
  if (!context) return;
  await context.close();
}

async function postJSON(url: string, body: Record<string, unknown>): Promise<void> {
  const response = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(`fault controller POST ${url}: HTTP ${response.status} ${await response.text()}`);
}

async function gateResultFaultFired(): Promise<boolean> {
  const response = await fetch(`${apiBaseURL}/api/v1/e2e/fault/drop-command-result`);
  return response.ok && Boolean((await response.json() as { fired?: boolean }).fired);
}

async function downlinkFaultEvents(): Promise<Array<{ type?: string; item_type?: string }>> {
  const response = await fetch(`${faultAdminURL}/admin/events`);
  if (!response.ok) throw new Error(`fault events HTTP ${response.status}`);
  return await response.json() as Array<{ type?: string; item_type?: string }>;
}

test('one real browser loop: login → buy → plant → harvest → share → friend farm → water → theft → home', async ({ browser, baseURL }) => {
  test.skip(!userA || !userB || !password || !baseURL, 'real CP-B credentials/baseURL are required');
  test.setTimeout(240_000);

  mkdirSync(evidenceRoot, { recursive: true });
  const steps: Array<{ step: string; detail?: Record<string, string | number | boolean> }> = [];
  const consoleErrors: string[] = [];
  let contextA: BrowserContext | undefined;
  let contextB: BrowserContext | undefined;
  let failure = '';
  const details: Record<string, string | number | boolean> = {};

  try {
    const activeContextA = await startEvidenceContext(browser, 'A');
    const activeContextB = await startEvidenceContext(browser, 'B');
    contextA = activeContextA;
    contextB = activeContextB;
    const pageA = await activeContextA.newPage();
    const pageB = await activeContextB.newPage();
    for (const [name, page] of [['A', pageA], ['B', pageB]] as const) {
      page.on('console', (message) => {
        if (message.type() === 'error') consoleErrors.push(`${name}: ${message.text().slice(0, 500)}`);
      });
    }

    await Promise.all([pageA.goto(baseURL!), pageB.goto(baseURL!)]);
    const [ownerA, ownerB] = await Promise.all([
      submitAccount(pageA, 'register', userA),
      submitAccount(pageB, 'register', userB),
    ]);
    expect(ownerA).not.toBe(ownerB);
    details.ownerA = ownerA;
    details.ownerB = ownerB;
    steps.push({ step: 'login', detail: { ownerA, ownerB } });

    // A's required personal loop and B's real-UI target preparation share the
    // same wall-clock maturity window. No test backdoor mutates crop state.
    await Promise.all([buySeeds(pageA, 1), buySeeds(pageB, 1)]);
    // B needs a second seed for the reverse-direction crop later. Keep the
    // authority writes sequential so this fault-heavy scenario never has two
    // unresolved BUY operations for the same PlayerActor.
    await buySeeds(pageB, 1);
    const [aHarvestPlot, bTheftPlot] = await Promise.all([
      plantFirstEmpty(pageA),
      plantFirstEmpty(pageB),
    ]);
    steps.push({ step: 'buy-and-plant', detail: { aHarvestPlot, bTheftPlot } });
    await Promise.all([waitMature(pageA, aHarvestPlot), waitMature(pageB, bTheftPlot)]);

    const aProduceBeforeHarvest = await produceCount(pageA);
    const aRevisionBeforeHarvest = await shellBigInt(pageA, 'data-player-state-revision');
    await pageA.locator('[data-tool="harvest"]').click();
    await pageA.locator(`[data-plot-id="${aHarvestPlot}"]`).click();
    await expect(pageA.locator(`[data-plot-id="${aHarvestPlot}"]`)).toHaveClass(/\bempty\b/, { timeout: 30_000 });
    await expect.poll(() => produceCount(pageA), { timeout: 30_000 }).toBeGreaterThan(aProduceBeforeHarvest);
    await expect.poll(() => shellBigInt(pageA, 'data-player-state-revision'), { timeout: 30_000 }).toBeGreaterThan(aRevisionBeforeHarvest);
    steps.push({ step: 'harvest' });

    // B now plants a separate fresh WATER target while the first target stays mature.
    const bWaterPlot = await plantFirstEmpty(pageB);
    await expect(pageB.locator(`[data-plot-id="${bWaterPlot}"]`)).toHaveAttribute('data-effective-crop-phase', 'growing');
    expect(bWaterPlot).not.toBe(bTheftPlot);

    const taskVersionBeforeCare = await shellBigInt(pageA, 'data-task-state-version');
    await pageA.locator('[data-share]').first().click();
    await expect(pageA.locator('[data-message]')).toContainText('分享链接已生成', { timeout: 20_000 });
    const shareText = await pageA.locator('[data-message]').textContent();
    const shareURL = shareText?.match(/https?:\/\/[^\s]+/)?.[0];
    if (!shareURL) throw new Error(`share URL missing from message: ${shareText}`);

    // B accepts through the real HTTP share intent after a real account login.
    await pageB.goto(shareURL);
    await submitAccount(pageB, 'login', userB, false);
    await expect(pageB.locator('[data-friend-count]')).toHaveText('1', { timeout: 30_000 });
    await expect(pageA.locator('[data-friend-count]')).toHaveText('1', { timeout: 30_000 });
    // Pending share navigation performs the real reverse-direction B→A Visit
    // through canonical VisitPermit + Ready before B returns home.
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerA, { timeout: 30_000 });
    await expect(shell(pageB)).toHaveAttribute('data-join-phase', 'READY');
    steps.push({ step: 'reverse-visit-b-to-a-ready' });
    // B returns home so owner and visitor contexts both subscribe to B before
    // FarmChange assertions.
    await pageB.locator('[data-my-farm]').first().click();
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });

    await pageA.locator('[data-open-friends]').first().click();
    const friendCard = pageA.locator(`article.friend-row[data-friend-owner="${ownerB}"]`);
    await expect(friendCard).toBeVisible();
    await expect(friendCard).toHaveAttribute('data-friend-avatar', '1');
    await expect(friendCard).toHaveAttribute('data-friend-level', '1');
    await expect(friendCard.locator('.friend-copy strong')).toHaveText(userB.toLowerCase());
    await expect(friendCard).toHaveAttribute('data-friend-opportunity', 'READY');
    details.relationEpoch = await friendCard.getAttribute('data-friend-relation-epoch') ?? '';
    details.friendActionMask = Number(await friendCard.getAttribute('data-friend-action-mask'));
    await expect(friendCard.locator('button[data-friend-owner]')).toBeEnabled();
    // Next opportunity is server-owned: H5 sends neither roster cursor nor a
    // visited set. B becomes skipped only after this Join reaches Ready.
    await pageA.locator('[data-visit-friend]').click();
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });
    await expect(shell(pageA)).toHaveAttribute('data-owner-profile-source', 'snapshot-owner-public-profile');
    await expect(shell(pageA)).toHaveAttribute('data-profile-player-id', ownerB);
    await expect(pageA.locator('[data-farm-title]')).toHaveText(`好友农场 · ${userB.toLowerCase()}`);
    steps.push({ step: 'share-and-join-friend' });
    await saveMilestone('friend-profile', pageA, pageB);

    // WATER: both independent browser contexts must apply the same B FarmChange.
    const waterVersionBefore = await shellBigInt(pageA, 'data-farm-version');
    expect(await shellBigInt(pageB, 'data-farm-version')).toBe(waterVersionBefore);
    const revisionBeforeCare = await shellBigInt(pageA, 'data-player-state-revision');
    // The authoritative carrot care plan opens WATER at plant_at + 3s. The
    // CP-B FarmSnapshot deliberately has no incident-timing projection, so
    // wait real time instead of treating an early ERR_VERSION_CONFLICT as a
    // successful care action.
    await pageA.waitForTimeout(3_500);
    if (!apiBaseURL) throw new Error('FARM_E2E_API_BASE_URL is required for F1');
    await postJSON(`${apiBaseURL}/api/v1/e2e/fault/drop-command-result`, { command_type: 'CARE' });
    const generationBeforeLostCareResult = await shellBigInt(pageA, 'data-connection-generation');
    await pageA.locator('[data-tool="water"]').click();
    await pageA.locator(`[data-plot-id="${bWaterPlot}"]`).click();
    await expect.poll(gateResultFaultFired, { timeout: 30_000 }).toBe(true);
    await expect.poll(() => shellBigInt(pageA, 'data-connection-generation'), { timeout: 60_000 }).toBeGreaterThan(generationBeforeLostCareResult);
    await waitConnected(pageA);
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB);
    await expect(pageA.locator(`[data-plot-id="${bWaterPlot}"]`)).toHaveClass(/\bwatered\b/, { timeout: 30_000 });
    await expect(pageB.locator(`[data-plot-id="${bWaterPlot}"]`)).toHaveClass(/\bwatered\b/, { timeout: 30_000 });
    await expect.poll(() => shellBigInt(pageA, 'data-farm-version'), { timeout: 30_000 }).toBeGreaterThan(waterVersionBefore);
    await expect.poll(() => shellBigInt(pageB, 'data-farm-version'), { timeout: 30_000 }).toBeGreaterThan(waterVersionBefore);
    const waterVersionAfterA = await shellBigInt(pageA, 'data-farm-version');
    expect(await shellBigInt(pageB, 'data-farm-version')).toBe(waterVersionAfterA);
    await expect(shell(pageA)).toHaveAttribute('data-care-reward-status', 'SETTLED', { timeout: 30_000 });
    await expect(shell(pageA)).toHaveAttribute('data-pending-ops', '0', { timeout: 30_000 });
    await expect.poll(() => shellBigInt(pageA, 'data-player-state-revision'), { timeout: 30_000 }).toBeGreaterThan(revisionBeforeCare);
    const revisionAfterCare = await shellBigInt(pageA, 'data-player-state-revision');
    steps.push({ step: 'f1-lost-care-result-recovered', detail: {
      farmVersion: waterVersionAfterA.toString(), revision: revisionAfterCare.toString(), generationBefore: generationBeforeLostCareResult.toString(),
    } });

    // A simulated transport loss while actively visiting B must recover the
    // same friend target. The diagnostic control arms recovery before closing,
    // so a proxy-reported 1006 cannot strand the application outside InFarm.
    const generationBeforeFriendReconnect = await shellBigInt(pageA, 'data-connection-generation');
    await openUtility(pageA, 'more');
    await pageA.locator('[data-reconnect]').click();
    // Reconnect can complete between two paint frames; generation is the
    // durable transport edge, unlike a transient non-InFarm label.
    await expect.poll(() => shellBigInt(pageA, 'data-connection-generation'), { timeout: 45_000 }).toBeGreaterThan(generationBeforeFriendReconnect);
    await waitConnected(pageA);
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB);
    await expect(pageA.locator('[data-farm-title]')).toHaveText(`好友农场 · ${userB.toLowerCase()}`);
    steps.push({ step: 'friend-farm-simulated-disconnect-recovery' });

    // F2: real Gate drain/restart while A is still visiting B. Recovery must
    // linearize on A's own subscription and then restore the original friend.
    if (!controlDir) throw new Error('FARM_E2E_CONTROL_DIR is required for F2');
    mkdirSync(controlDir, { recursive: true });
    const f2RequestPath = path.join(controlDir, 'restart-gate.request');
    const f2AckPath = path.join(controlDir, 'restart-gate.ack.json');
    rmSync(f2AckPath, { force: true });
    const f2GenerationA = await shellBigInt(pageA, 'data-connection-generation');
    const f2GenerationB = await shellBigInt(pageB, 'data-connection-generation');
    writeFileSync(f2RequestPath, JSON.stringify({ action: 'restart-gate' }));
    await expect.poll(() => {
      try { return JSON.parse(readFileSync(f2AckPath, 'utf8')).status as string; } catch { return 'WAITING'; }
    }, { timeout: 45_000 }).toBe('READY');
    await expect.poll(() => shellBigInt(pageA, 'data-connection-generation'), { timeout: 60_000 }).toBeGreaterThan(f2GenerationA);
    await expect.poll(() => shellBigInt(pageB, 'data-connection-generation'), { timeout: 60_000 }).toBeGreaterThan(f2GenerationB);
    await Promise.all([waitConnected(pageA), waitConnected(pageB)]);
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB);
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerB);
    steps.push({ step: 'f2-gate-drain-restored-friend-target' });

    // WATER is a fact of this crop_cycle_id, not of the plot forever. Once B
    // harvests the watered cycle, both owner and visitor must receive a complete
    // empty PlotView that clears the watering badge and all old crop fields.
    // A post-plant authoritative FarmChange (for example WATER) may already
    // carry MATURE on wire; otherwise the trusted clock derives it locally.
    await waitMature(pageB, bWaterPlot, 'trusted-time-or-wire');
    await pageB.locator('[data-tool="harvest"]').click();
    await pageB.locator(`[data-plot-id="${bWaterPlot}"]`).click();
    const waterPlotAAfterHarvest = pageA.locator(`[data-plot-id="${bWaterPlot}"]`);
    const waterPlotBAfterHarvest = pageB.locator(`[data-plot-id="${bWaterPlot}"]`);
    await expect(waterPlotBAfterHarvest).toHaveClass(/\bempty\b/, { timeout: 30_000 });
    await expect(waterPlotAAfterHarvest).toHaveClass(/\bempty\b/, { timeout: 30_000 });
    await expect(waterPlotBAfterHarvest).not.toHaveClass(/\bwatered\b/);
    await expect(waterPlotAAfterHarvest).not.toHaveClass(/\bwatered\b/);
    await expect(waterPlotBAfterHarvest).toHaveAttribute('data-harvestable-count', '');
    await expect(waterPlotAAfterHarvest).toHaveAttribute('data-harvestable-count', '');
    steps.push({ step: 'water-cycle-cleared-on-harvest' });

    // THEFT uses the distinct mature plot and proves the same changed counters
    // on owner and visitor, plus helper inventory/revision settlement.
    const theftPlotA = pageA.locator(`[data-plot-id="${bTheftPlot}"]`);
    const theftPlotB = pageB.locator(`[data-plot-id="${bTheftPlot}"]`);
    await expect(theftPlotA).toHaveAttribute('data-effective-crop-phase', 'mature');
    const stolenBefore = Number(await theftPlotA.getAttribute('data-stolen-total'));
    const harvestableBefore = Number(await theftPlotA.getAttribute('data-harvestable-count'));
    expect(harvestableBefore).toBeGreaterThan(0);
    const produceBeforeTheft = await produceCount(pageA);
    const theftVersionBefore = await shellBigInt(pageA, 'data-farm-version');
    if (!faultAdminURL) throw new Error('FARM_E2E_FAULT_ADMIN_URL is required for F3');
    await postJSON(`${faultAdminURL}/admin/plan`, { fail_farm_change_allow_rejoin: true });
    await theftPlotA.click();
    await expect(pageA.locator('[data-theft]')).toBeEnabled();
    await pageA.locator('[data-theft]').click();
    await expect(pageA.locator('[data-message]')).toContainText('本次尚未提交请求');
    await pageA.locator('[data-theft]').click();
    await expect.poll(async () => (await downlinkFaultEvents()).some((event) => event.type === 'FAIL_FARM_CHANGE'), { timeout: 30_000 }).toBe(true);
    await expect.poll(async () => (await downlinkFaultEvents()).some((event) => event.item_type === 'REJOIN_REQUIRED'), { timeout: 30_000 }).toBe(true);
    await postJSON(`${faultAdminURL}/admin/plan`, {});
    await expect.poll(async () => Number(await theftPlotA.getAttribute('data-stolen-total')), { timeout: 45_000 }).toBeGreaterThan(stolenBefore);
    await expect.poll(async () => Number(await theftPlotB.getAttribute('data-stolen-total')), { timeout: 45_000 }).toBeGreaterThan(stolenBefore);
    const stolenAfter = Number(await theftPlotA.getAttribute('data-stolen-total'));
    expect(Number(await theftPlotB.getAttribute('data-stolen-total'))).toBe(stolenAfter);
    expect(Number(await theftPlotA.getAttribute('data-harvestable-count'))).toBeLessThan(harvestableBefore);
    await expect.poll(() => shellBigInt(pageA, 'data-farm-version'), { timeout: 45_000 }).toBeGreaterThan(theftVersionBefore);
    await expect.poll(() => shellBigInt(pageB, 'data-farm-version'), { timeout: 45_000 }).toBeGreaterThan(theftVersionBefore);
    expect(await shellBigInt(pageB, 'data-farm-version')).toBe(await shellBigInt(pageA, 'data-farm-version'));
    await expect.poll(() => produceCount(pageA), { timeout: 45_000 }).toBeGreaterThan(produceBeforeTheft);
    await expect.poll(() => shellBigInt(pageA, 'data-player-state-revision'), { timeout: 45_000 }).toBeGreaterThan(revisionAfterCare);
    const bFarmVersionAfterTheft = await shellBigInt(pageA, 'data-farm-version');
    details.bFarmVersionAfterTheft = bFarmVersionAfterTheft.toString();
    details.bTheftPlotIndex = Number(bTheftPlot.replace(/^plot-/, ''));
    details.bStolenTotal = stolenAfter;
    steps.push({ step: 'f3-farm-change-failure-rejoined', detail: { stolenBefore, stolenAfter, bFarmVersionAfterTheft: bFarmVersionAfterTheft.toString() } });
    await saveMilestone('water-theft', pageA, pageB);

    // Return to A and verify authoritative own farm/private/task convergence.
    await pageA.locator('[data-my-farm]').first().click();
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerA, { timeout: 30_000 });
    await expect(pageA.locator('[data-farm-title]')).toHaveText('我的农场');
    await expect(shell(pageA)).toHaveAttribute('data-profile-player-id', ownerA);
    await expect(pageA.locator(`[data-plot-id="${aHarvestPlot}"]`)).toHaveClass(/\bempty\b/);
    await expect.poll(() => shellBigInt(pageA, 'data-task-state-version'), { timeout: 30_000 }).toBeGreaterThan(taskVersionBeforeCare);
    await openUtility(pageA, 'tasks');
    await postJSON(`${apiBaseURL}/api/v1/e2e/fault/drop-command-result`, { command_type: 'CLAIM_TASK' });
    const generationBeforeLostClaimResult = await shellBigInt(pageA, 'data-connection-generation');
    for (const taskId of ['daily.login.v1', 'daily.friend_visit.v1', 'daily.friend_interact.v1']) {
      const task = pageA.locator(`[data-task-id="${taskId}"]`);
      await expect(task).toBeVisible();
      await expect(task).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
      await task.locator('[data-claim-task]').click();
      await expect(task).toHaveAttribute('data-task-state', 'CLAIMED', { timeout: 60_000 });
      await expect(shell(pageA)).toHaveAttribute('data-pending-ops', '0', { timeout: 30_000 });
      if (taskId === 'daily.login.v1') {
        await expect.poll(() => shellBigInt(pageA, 'data-connection-generation'), { timeout: 60_000 })
          .toBeGreaterThan(generationBeforeLostClaimResult);
      }
    }
    const growthTask = pageA.locator('[data-task-id="growth.harvest.v1"]');
    await expect(growthTask).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
    await growthTask.locator('[data-claim-task]').click();
    await expect(growthTask).toHaveCount(0, { timeout: 30_000 });
    await expect(pageA.locator('[data-message]')).toContainText('任务奖励已由服务端结算');
    steps.push({ step: 'task-list-visit-interact-growth-and-claims', detail: {
      lostClaimResultRecovered: true,
      generationBeforeLostClaimResult: generationBeforeLostClaimResult.toString(),
    } });
    await closeUtility(pageA);
    await expect(shell(pageA)).toHaveAttribute('data-pending-ops', '0', { timeout: 30_000 });
    const finalProduce = await produceCount(pageA);
    details.aFinalProduce = finalProduce;
    details.aPlayerStateRevision = (await shellBigInt(pageA, 'data-player-state-revision')).toString();
    steps.push({ step: 'return-home', detail: { finalProduce } });
    await saveMilestone('return-home', pageA, pageB);

    // F2 already performed the one specified real Gate drain and the lost
    // CLAIM_TASK result performed another reconnect/replay. Finish on those
    // recovered authoritative pages; a fresh-login presence lease is outside
    // this Task checkpoint and is already covered by the published Social gate.
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerB);
    await expect(shell(pageA)).toHaveAttribute('data-pending-ops', '0');
    steps.push({ step: 'final-authority-stable' });
    await saveMilestone('final-authority-stable', pageA, pageB);
  } catch (error) {
    failure = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    throw error;
  } finally {
    if (faultAdminURL) await postJSON(`${faultAdminURL}/admin/plan`, {}).catch(() => undefined);
    writeFileSync(path.join(evidenceRoot, 'browser-verdict.json'), JSON.stringify({
      verdict: failure ? 'FAIL' : 'PASS',
      flow: 'login->buy->plant->harvest->share-friend->friend-farm->water->theft->home',
      steps,
      details,
      console_errors: consoleErrors,
      failure: failure || undefined,
      finished_at: new Date().toISOString(),
    }, null, 2));
    await stopEvidenceContext(contextA, 'A');
    await stopEvidenceContext(contextB, 'B');
  }
});
