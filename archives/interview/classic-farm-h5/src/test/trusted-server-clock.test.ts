import { describe, expect, it } from 'vitest';

import { TrustedServerClock } from '../app/trusted-server-clock';

describe('TrustedServerClock (A1B trusted server time)', () => {
  it('is unsynchronized and fail-closed until a server time is received', () => {
    const clock = new TrustedServerClock();
    expect(clock.hasTrustedTime()).toBe(false);
    expect(clock.trustedNow(0)).toBeNull();
  });

  it('never lets browser Date.now decide maturity: unsynced clock stays null even after wall clock advances', () => {
    const clock = new TrustedServerClock();
    // 浏览器墙钟已走 10 分钟,但从未收到受信服务端时间:必须保持 null(不提前)。
    const wallAdvanceMs = 10 * 60 * 1000;
    expect(clock.trustedNow(wallAdvanceMs)).toBeNull();
  });

  it('derives a lower-bound trusted now from the server anchor + monotonic elapsed', () => {
    const clock = new TrustedServerClock();
    clock.sync(1_000_000, 100);
    // 距锚点经过 2500ms 单调时间:受信服务端时间 = anchor + elapsed。
    expect(clock.trustedNow(100 + 2_500)).toBe(1_002_500);
    expect(clock.hasTrustedTime()).toBe(true);
  });

  it('can be re-anchored by a newer trusted_server_now_ms (same revision is fine)', () => {
    const clock = new TrustedServerClock();
    clock.sync(1_000_000, 100);
    // 新 Bootstrap 携带更新的 server now(同 player_state_revision 也可刷新时钟)。
    clock.sync(1_100_000, 1_000);
    expect(clock.trustedNow(1_000 + 100)).toBe(1_100_100);
  });

  it('never advances faster than monotonic time between syncs (lower bound)', () => {
    const clock = new TrustedServerClock();
    clock.sync(2_000_000, 50);
    // 若 performance.now 只走了 10ms(网络/时钟回拨),受信时间不得虚增。
    expect(clock.trustedNow(60)).toBe(2_000_010);
    expect(clock.trustedNow(50)).toBe(2_000_000);
  });

  it('rejects a re-anchor that would make the trusted clock go backwards (out-of-order stale response)', () => {
    const clock = new TrustedServerClock();
    clock.sync(1_000_000, 100);
    // 新锚点比当前推算值更小(乱序晚到的旧响应):必须拒绝,受信时间不得倒退。
    clock.sync(999_900, 200);
    expect(clock.trustedNow(200)).toBeGreaterThanOrEqual(1_000_000);
    expect(clock.trustedNow(200)).not.toBeLessThan(1_000_000);
    // 新锚点确实更大:允许前进。
    clock.sync(1_100_000, 300);
    expect(clock.trustedNow(300)).toBe(1_100_000);
  });
});
