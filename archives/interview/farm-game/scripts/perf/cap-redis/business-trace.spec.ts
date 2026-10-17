import { expect, test, type Browser, type BrowserContext, type Page } from '@playwright/test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { CommandType as ProtoCommandType } from '../../protocol/generated/common/envelope';
import { FarmFrame } from '../../protocol/generated/common/frame';

const userA = process.env.FARM_E2E_USER_A ?? '';
const userB = process.env.FARM_E2E_USER_B ?? '';
const password = process.env.FARM_E2E_PASSWORD ?? '';
const traceControl = process.env.CAP_BUSINESS_TRACE_CONTROL_URL ?? '';
const evidenceRoot = process.env.FARM_E2E_EVIDENCE_ROOT ?? 'test-results/cap-business-trace';

const shell = (page: Page) => page.locator('[data-app-shell]');
const asBigInt = (value: string | null): bigint => BigInt(value || '0');
const shellBigInt = async (page: Page, attribute: string): Promise<bigint> => asBigInt(await shell(page).getAttribute(attribute));
const seedCount = async (page: Page): Promise<bigint> => asBigInt(await page.locator('[data-private]').getAttribute('data-seed-count'));
const wallet = async (page: Page): Promise<number> => Number(await page.locator('[data-private]').getAttribute('data-wallet-count') ?? '0');

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
  if (!result.ok()) throw new Error(`${mode} failed: HTTP ${result.status()}`);
  await waitConnected(page);
  const ownerID = await shell(page).getAttribute('data-own-owner-id');
  if (!ownerID) throw new Error('connected without own owner id');
  if (expectOwnFarm) await expect(shell(page)).toHaveAttribute('data-current-owner-id', ownerID);
  return ownerID;
}

async function openUtility(page: Page, mode: 'inventory' | 'shop' | 'tasks'): Promise<void> {
  const sheet = page.locator('[data-utility-sheet]');
  if (await sheet.isVisible()) await page.locator('[data-close-utility]').click();
  await page.locator(`[data-open-utility="${mode}"]`).first().click();
  await expect(sheet).toBeVisible();
}

async function closeUtility(page: Page): Promise<void> {
  if (await page.locator('[data-utility-sheet]').isVisible()) await page.locator('[data-close-utility]').click();
}

async function produceCount(page: Page): Promise<number> {
  await openUtility(page, 'inventory');
  const cropsTab = page.locator('[data-inventory-tab="crops"]');
  if (await cropsTab.count()) await cropsTab.click();
  const row = page.locator('[data-utility-sheet] .crop-item').filter({ hasText: '胡萝卜' }).first();
  await expect(row).toBeVisible();
  const count = Number((await row.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
  await closeUtility(page);
  return count;
}

async function postTrace(endpoint: 'begin' | 'bind' | 'end', command: string, extra: Record<string, string> = {}): Promise<void> {
  const response = await fetch(`${traceControl}/${endpoint}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ command, ...extra }),
  });
  if (!response.ok) throw new Error(`trace ${endpoint} ${command}: HTTP ${response.status()}`);
}

const protoCommand = (command: string): ProtoCommandType => ({
  BUY: ProtoCommandType.BUY, SELL: ProtoCommandType.SELL, CLAIM_TASK: ProtoCommandType.CLAIM_TASK,
  PLANT: ProtoCommandType.PLANT, HARVEST: ProtoCommandType.HARVEST,
  WATER: ProtoCommandType.CARE, THEFT: ProtoCommandType.THEFT,
} as Record<string, ProtoCommandType>)[command];

function operationBinder(page: Page): { expect: (command: string) => Promise<void> } {
  let pending: { command: string; resolve: () => void; reject: (error: Error) => void } | undefined;
  page.on('websocket', (socket) => {
    socket.on('framesent', ({ payload }) => {
      if (!pending) return;
      try {
        const frame = FarmFrame.decode(typeof payload === 'string' ? Buffer.from(payload) : new Uint8Array(payload));
        const envelope = frame.clientCommand;
        if (!envelope || envelope.commandType !== protoCommand(pending.command)) return;
        if (!envelope.operationId) throw new Error('wire command has no operation identity');
        const current = pending;
        pending = undefined;
        void postTrace('bind', current.command, { operation_id: envelope.operationId })
          .then(current.resolve, (error) => current.reject(error instanceof Error ? error : new Error(String(error))));
      } catch (error) {
        if (pending) {
          const current = pending;
          pending = undefined;
          current.reject(error instanceof Error ? error : new Error(String(error)));
        }
      }
    });
  });
  return {
    expect: (command: string) => {
      if (pending) return Promise.reject(new Error('operation binder already has a pending command'));
      return new Promise<void>((resolve, reject) => { pending = { command, resolve, reject }; });
    },
  };
}

async function traceCommand(page: Page, binder: ReturnType<typeof operationBinder>, command: string, action: () => Promise<void>): Promise<void> {
  await expect(shell(page)).toHaveAttribute('data-pending-ops', '0', { timeout: 30_000 });
  await page.waitForTimeout(250);
  const bound = binder.expect(command);
  await postTrace('begin', command);
  try {
    const result = action();
    await bound;
    await result;
    await expect(shell(page)).toHaveAttribute('data-pending-ops', '0', { timeout: 45_000 });
    await page.waitForTimeout(250);
    await postTrace('end', command, { status: 'PASS' });
  } catch (error) {
    await postTrace('end', command, { status: 'FAIL' }).catch(() => undefined);
    throw error;
  }
}

async function buySeeds(page: Page, binder: ReturnType<typeof operationBinder> | undefined, count: number, traced = false): Promise<void> {
  const before = await seedCount(page);
  await openUtility(page, 'shop');
  await page.locator('[data-shop-count]').fill(String(count));
  const action = async () => {
    await page.locator('[data-buy-offer="1"]').click();
    await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(before + BigInt(count));
  };
  if (traced && binder) await traceCommand(page, binder, 'BUY', action); else await action();
  await closeUtility(page);
}

async function plantFirstEmpty(page: Page, binder: ReturnType<typeof operationBinder> | undefined, traced = false): Promise<string> {
  const before = await seedCount(page);
  await page.locator('[data-quick-seed="1"]').click();
  const empty = page.locator('.plot.empty[data-plot-id]').first();
  const plotID = await empty.getAttribute('data-plot-id');
  if (!plotID) throw new Error('no empty plot');
  const action = async () => {
    await empty.click();
    await expect(page.locator(`[data-plot-id="${plotID}"]`)).not.toHaveClass(/\bempty\b/, { timeout: 30_000 });
    await expect.poll(() => seedCount(page), { timeout: 30_000 }).toBe(before - 1n);
  };
  if (traced && binder) await traceCommand(page, binder, 'PLANT', action); else await action();
  return plotID;
}

async function waitMature(page: Page, plotID: string): Promise<void> {
  await expect(page.locator(`[data-plot-id="${plotID}"]`)).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 60_000 });
}

async function evidenceContext(browser: Browser): Promise<BrowserContext> {
  return browser.newContext({ viewport: { width: 1280, height: 720 } });
}

test('real H5 end-to-end business amplification trace', async ({ browser, baseURL }) => {
  test.skip(!userA || !userB || !password || !baseURL || !traceControl, 'trace environment is required');
  test.setTimeout(240_000);
  mkdirSync(evidenceRoot, { recursive: true });
  const tracedCommands: string[] = [];
  const details: Record<string, string | number | boolean> = {};
  let contextA: BrowserContext | undefined;
  let contextB: BrowserContext | undefined;
  let failure = '';
  try {
    // The independent Go protocol witness starts concurrently in the parent
    // harness. Let it finish before opening any measured command window.
    await new Promise((resolve) => setTimeout(resolve, 15_000));
    contextA = await evidenceContext(browser);
    contextB = await evidenceContext(browser);
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();
    const binderA = operationBinder(pageA);
    await Promise.all([pageA.goto(baseURL!), pageB.goto(baseURL!)]);
    const [ownerA, ownerB] = await Promise.all([
      submitAccount(pageA, 'register', userA), submitAccount(pageB, 'register', userB),
    ]);
    expect(ownerA).not.toBe(ownerB);
    details.ownerA = ownerA;
    details.ownerB = ownerB;

    await buySeeds(pageA, binderA, 1, true); tracedCommands.push('BUY');
    await buySeeds(pageB, undefined, 2);
    const aPlot = await plantFirstEmpty(pageA, binderA, true); tracedCommands.push('PLANT');
    const bTheftPlot = await plantFirstEmpty(pageB, undefined);
    await Promise.all([waitMature(pageA, aPlot), waitMature(pageB, bTheftPlot)]);

    const produceBeforeHarvest = await produceCount(pageA);
    const revisionBeforeHarvest = await shellBigInt(pageA, 'data-player-state-revision');
    await pageA.locator('[data-tool="harvest"]').click();
    await traceCommand(pageA, binderA, 'HARVEST', async () => {
      await pageA.locator(`[data-plot-id="${aPlot}"]`).click();
      await expect(pageA.locator(`[data-plot-id="${aPlot}"]`)).toHaveClass(/\bempty\b/, { timeout: 30_000 });
      await expect.poll(() => produceCount(pageA), { timeout: 30_000 }).toBeGreaterThan(produceBeforeHarvest);
      await expect.poll(() => shellBigInt(pageA, 'data-player-state-revision'), { timeout: 30_000 }).toBeGreaterThan(revisionBeforeHarvest);
    });
    tracedCommands.push('HARVEST');

    await openUtility(pageA, 'inventory');
    const cropsTab = pageA.locator('[data-inventory-tab="crops"]');
    if (await cropsTab.count()) await cropsTab.click();
    const sellRow = pageA.locator('[data-utility-sheet] .crop-item').filter({ hasText: '胡萝卜' }).first();
    const produceBeforeSell = Number((await sellRow.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0');
    const walletBeforeSell = await wallet(pageA);
    expect(produceBeforeSell).toBeGreaterThan(0);
    await traceCommand(pageA, binderA, 'SELL', async () => {
      await sellRow.locator('[data-sell-count]').fill('1');
      await sellRow.locator('[data-sell-item]').click();
      await expect.poll(async () => Number((await sellRow.locator('b').textContent())?.replace(/[^\d]/g, '') ?? '0'), { timeout: 30_000 }).toBe(produceBeforeSell - 1);
      await expect.poll(() => wallet(pageA), { timeout: 30_000 }).toBeGreaterThan(walletBeforeSell);
    });
    tracedCommands.push('SELL');
    await closeUtility(pageA);

    const bWaterPlot = await plantFirstEmpty(pageB, undefined);
    expect(bWaterPlot).not.toBe(bTheftPlot);
    await pageA.locator('[data-share]').first().click();
    await expect(pageA.locator('[data-message]')).toContainText('分享链接已生成', { timeout: 20_000 });
    const shareText = await pageA.locator('[data-message]').textContent();
    const shareURL = shareText?.match(/https?:\/\/[^\s]+/)?.[0];
    if (!shareURL) throw new Error('share URL missing');
    await pageB.goto(shareURL);
    await submitAccount(pageB, 'login', userB, false);
    await expect(pageB.locator('[data-friend-count]')).toHaveText('1', { timeout: 30_000 });
    await expect(pageA.locator('[data-friend-count]')).toHaveText('1', { timeout: 30_000 });
    await pageB.locator('[data-my-farm]').first().click();
    await expect(shell(pageB)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });
    await pageA.locator('[data-open-friends]').first().click();
    const friendCard = pageA.locator(`article.friend-row[data-friend-owner="${ownerB}"]`);
    await expect(friendCard).toBeVisible();
    details.relationEpoch = await friendCard.getAttribute('data-friend-relation-epoch') ?? '';
    details.friendActionMask = Number(await friendCard.getAttribute('data-friend-action-mask'));
    await pageA.locator('[data-visit-friend]').click();
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerB, { timeout: 30_000 });

    await pageA.waitForTimeout(3_500);
    const farmVersionBeforeWater = await shellBigInt(pageA, 'data-farm-version');
    const playerRevisionBeforeWater = await shellBigInt(pageA, 'data-player-state-revision');
    await pageA.locator('[data-tool="water"]').click();
    await traceCommand(pageA, binderA, 'WATER', async () => {
      await pageA.locator(`[data-plot-id="${bWaterPlot}"]`).click();
      await expect(pageA.locator(`[data-plot-id="${bWaterPlot}"]`)).toHaveClass(/\bwatered\b/, { timeout: 45_000 });
      await expect(pageB.locator(`[data-plot-id="${bWaterPlot}"]`)).toHaveClass(/\bwatered\b/, { timeout: 45_000 });
      await expect.poll(() => shellBigInt(pageA, 'data-farm-version'), { timeout: 45_000 }).toBeGreaterThan(farmVersionBeforeWater);
      await expect.poll(() => shellBigInt(pageA, 'data-player-state-revision'), { timeout: 45_000 }).toBeGreaterThan(playerRevisionBeforeWater);
      await expect(shell(pageA)).toHaveAttribute('data-care-reward-status', 'SETTLED', { timeout: 45_000 });
    });
    tracedCommands.push('WATER');

    const theftPlotA = pageA.locator(`[data-plot-id="${bTheftPlot}"]`);
    const theftPlotB = pageB.locator(`[data-plot-id="${bTheftPlot}"]`);
    await expect(theftPlotA).toHaveAttribute('data-effective-crop-phase', 'mature');
    const stolenBefore = Number(await theftPlotA.getAttribute('data-stolen-total'));
    const produceBeforeTheft = await produceCount(pageA);
    await theftPlotA.click();
    const theftButton = pageA.locator('[data-theft]');
    await theftButton.click();
    await expect(theftButton).toContainText('确认偷菜');
    await expect(shell(pageA)).toHaveAttribute('data-pending-ops', '0');
    await traceCommand(pageA, binderA, 'THEFT', async () => {
      await theftButton.click();
      await expect.poll(async () => Number(await theftPlotA.getAttribute('data-stolen-total')), { timeout: 45_000 }).toBeGreaterThan(stolenBefore);
      await expect.poll(async () => Number(await theftPlotB.getAttribute('data-stolen-total')), { timeout: 45_000 }).toBeGreaterThan(stolenBefore);
      await expect.poll(() => produceCount(pageA), { timeout: 45_000 }).toBeGreaterThan(produceBeforeTheft);
    });
    tracedCommands.push('THEFT');
    details.bFarmVersionAfterTheft = (await shellBigInt(pageA, 'data-farm-version')).toString();
    details.bTheftPlotIndex = Number(bTheftPlot.replace(/^plot-/, ''));
    details.bStolenTotal = Number(await theftPlotA.getAttribute('data-stolen-total'));

    await pageA.locator('[data-my-farm]').first().click();
    await expect(shell(pageA)).toHaveAttribute('data-current-owner-id', ownerA, { timeout: 30_000 });
    await openUtility(pageA, 'tasks');
    // Warm the task command's lazy script path outside every measured window,
    // exactly like the Primitive train's mandatory warmup round. The measured
    // row remains a real, distinct CLAIM_TASK against a claimable task.
    const dailyLogin = pageA.locator('[data-task-id="daily.login.v1"]');
    await expect(dailyLogin).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
    await dailyLogin.locator('[data-claim-task]').click();
    await expect(dailyLogin).toHaveAttribute('data-task-state', 'CLAIMED', { timeout: 45_000 });
    const measuredClaim = pageA.locator('[data-task-id="daily.friend_visit.v1"]');
    await expect(measuredClaim).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
    await traceCommand(pageA, binderA, 'CLAIM_TASK', async () => {
      await measuredClaim.locator('[data-claim-task]').click();
      await expect(measuredClaim).toHaveAttribute('data-task-state', 'CLAIMED', { timeout: 45_000 });
    });
    tracedCommands.push('CLAIM_TASK');
    const remainingDaily = pageA.locator('[data-task-id="daily.friend_interact.v1"]');
    await expect(remainingDaily).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
    await remainingDaily.locator('[data-claim-task]').click();
    await expect(remainingDaily).toHaveAttribute('data-task-state', 'CLAIMED', { timeout: 45_000 });
    const growthTask = pageA.locator('[data-task-id="growth.harvest.v1"]');
    await expect(growthTask).toHaveAttribute('data-task-state', 'CLAIMABLE', { timeout: 30_000 });
    await growthTask.locator('[data-claim-task]').click();
    await expect(growthTask).toHaveCount(0, { timeout: 30_000 });
    await closeUtility(pageA);

    details.aFinalProduce = await produceCount(pageA);
    details.aPlayerStateRevision = (await shellBigInt(pageA, 'data-player-state-revision')).toString();
    expect([...tracedCommands].sort()).toEqual(['BUY', 'CLAIM_TASK', 'HARVEST', 'PLANT', 'SELL', 'THEFT', 'WATER']);
  } catch (error) {
    failure = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    throw error;
  } finally {
    writeFileSync(path.join(evidenceRoot, 'browser-verdict.json'), JSON.stringify({
      verdict: failure ? 'FAIL' : 'PASS', flow: 'real-h5-seven-command-business-trace', tracedCommands, details,
      error_code: failure ? 'BROWSER_ASSERTION_FAILED' : '',
    }, null, 2));
    await Promise.allSettled([contextA?.close(), contextB?.close()]);
  }
});
