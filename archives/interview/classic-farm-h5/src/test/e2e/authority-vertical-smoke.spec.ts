import { expect, test, type Page } from '@playwright/test';

const user = process.env.FARM_E2E_USER_A ?? '';
const password = process.env.FARM_E2E_PASSWORD ?? '';

async function registerAndConnect(page: Page): Promise<void> {
  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      console.log(`[BROWSER:${msg.type()}] ${msg.text().slice(0, 300)}`);
    }
  });

  await page.locator('[data-web-username]').fill(user);
  await page.locator('[data-web-password]').fill(password);
  await page.locator('[data-register]').click();

  const registerResponse = await page.waitForResponse(
    (resp) => resp.url().includes('/api/v1/accounts/register'),
    { timeout: 20_000 },
  );
  if (!registerResponse.ok()) {
    const body = await registerResponse.json().catch(() => ({}));
    throw new Error(`注册失败 (${registerResponse.status()}): ${JSON.stringify(body)}`);
  }
  await expect(page.locator('[data-status]')).toHaveText('InFarm', { timeout: 30_000 });
}

test('Authority Bootstrap → Join → Buy → Plant → MATURE → Harvest', async ({ page, baseURL }) => {
  test.skip(!user || !password, 'real Authority credentials are required');

  await page.goto(baseURL!);
  await registerAndConnect(page);

  const shell = page.locator('[data-app-shell]');

  // A2: Bootstrap must have been applied.
  await expect(shell).toHaveAttribute('data-bootstrap-state', 'applied');
  // A1: Recovery barrier must be open after recovery completes.
  await expect(shell).toHaveAttribute('data-recovery-barrier', 'open');
  // PlayerRefresh §1: private state must be ready (strict bootstrap applied).
  await expect(shell).toHaveAttribute('data-private-ready', 'true');
  // Join must have reached READY (subscription active, recovery complete).
  await expect(shell).toHaveAttribute('data-join-phase', 'READY');

  const privateState = page.locator('[data-private]');
  const wallet = async () => BigInt((await privateState.getAttribute('data-wallet-count')) ?? '0');
  const seeds = async () => BigInt((await privateState.getAttribute('data-seed-count')) ?? '0');
  const refreshSent = async () => Number((await shell.getAttribute('data-bootstrap-refresh-sent')) ?? '0');
  const refreshApplied = async () => Number((await shell.getAttribute('data-bootstrap-refresh-applied')) ?? '0');
  const farmVersion = async () => {
    const text = await page.locator('[data-version]').textContent() ?? '0';
    return BigInt(text.replace(/\D/g, '') || '0');
  };

  const walletBefore = await wallet();
  const seedsBefore = await seeds();
  const farmVersionBefore = await farmVersion();

  // --- Buy ---
  await page.locator('[data-open-utility="shop"]').first().click();
  await page.locator('[data-shop-count]').fill('1');
  const buyButton = page.locator('[data-buy-offer="1"]');
  const isDisabled = await buyButton.isDisabled();
  if (isDisabled) {
    const msg = await page.locator('[data-message]').textContent();
    throw new Error(`Buy button disabled. wallet=${await wallet()} status=${await page.locator('[data-status]').textContent()} msg=${msg}`);
  }
  await buyButton.click();

  // Wait for seed count to increase (proves BUY command executed and committed).
  await expect.poll(seeds, { timeout: 15_000 }).toBe(seedsBefore + 1n);
  await page.locator('[data-close-utility]').click();

  // --- Plant ---
  // 基准必须在 plant 提交(plot.click)之前读取,否则 plant 的 refresh 可能已
  // 发出,读到的 baseline 已是 sent+1,后续 poll "> baseline" 就会因 plant 只发
  // 一笔而永远等不到再次增长(Q1 run19 误判)。
  const refreshSentBeforePlant = await refreshSent();
  const refreshAppliedBeforePlant = await refreshApplied();
  await page.locator('[data-quick-seed="1"]').click();
  const plot = page.locator('.plot.empty[data-plot-id]').first();
  const plotId = await plot.getAttribute('data-plot-id');
  await plot.click();
  const planted = page.locator(`[data-plot-id="${plotId}"]`);
  await expect(planted).not.toHaveClass(/\bempty\b/);

  // Plant commits asynchronously via WSS and has TWO legal completion paths:
  //  fast: workflow completes within the RPC budget → SUCCEEDED directly;
  //  slow: PROCESSING → H5 polls GET_OPERATION_RESULT (retry_after_ms) →
  //        COMPLETED → same terminal handling.
  // PLANT has no privateState, so EITHER path must drive the independent asset
  // refresh (dirty → sent → applied) that converges seed 1 → 0. The diagnostic
  // counters moved BEFORE the seed deduction so a regression distinguishes
  // "refresh not sent" vs "sent but not applied" vs "applied but store not
  // converged". FarmChange may arrive before the terminal result (slow path),
  // so farmVersion uses poll() too.
  await expect.poll(refreshSent, { timeout: 15_000 }).toBeGreaterThan(refreshSentBeforePlant);
  await expect.poll(refreshApplied, { timeout: 15_000 }).toBeGreaterThan(refreshAppliedBeforePlant);
  await expect.poll(seeds, { timeout: 15_000 }).toBe(seedsBefore);
  await expect.poll(farmVersion, { timeout: 10_000 }).toBeGreaterThan(farmVersionBefore);
  const farmVersionAfterPlant = await farmVersion();

  // --- Mature (Q1: maturity is a time-derived view, not a wire state change) ---
  // wire 仍是 GROWING(服务端按合同不下发 MATURE push),但 H5 用受信服务端
  // 时钟(trusted_server_now_ms 锚点)派生成熟展示;成熟不推进 FarmVersion。
  await expect(planted).toHaveAttribute('data-wire-crop-phase', 'growing');
  await expect(planted).toHaveAttribute('data-effective-crop-phase', 'mature', { timeout: 15_000 });
  await expect(planted).toHaveAttribute('data-crop-phase-source', 'trusted-time');
  await expect(planted).toHaveClass(/\bmature\b/);

  // 成熟展示是时间派生视图:期间 FarmVersion 不得变化(无伪造成熟 FarmChange)。
  const farmVersionAtMature = await farmVersion();
  expect(farmVersionAtMature).toBe(farmVersionAfterPlant);

  // --- Harvest: FarmActor 按 trusted_now_ms 裁决, 真实 FarmVersion 恰好 +1 ---
  await page.locator('[data-tool="harvest"]').click();
  await planted.click();
  await expect(planted).toHaveClass(/\bempty\b/, { timeout: 15_000 });
  await expect.poll(farmVersion, { timeout: 10_000 }).toBe(farmVersionAtMature + 1n);

  const walletAfter = await wallet();
  expect(walletAfter).toBeLessThanOrEqual(walletBefore);

  // Operation Journal 最终清理:所有终态操作 resolveTerminal(removeJournal +
  // defer pending 移除)后,页面无残留 pending operation。data-pending-ops 来自
  // commandService.getPending();poll 等待其归 0(覆盖 800ms defer)。
  await expect
    .poll(
      async () => Number((await shell.getAttribute('data-pending-ops')) ?? '-1'),
      { timeout: 15_000 },
    )
    .toBe(0);

  const lastError = await shell.getAttribute('data-last-error-code');
  expect(lastError ?? '').toBe('');
});
