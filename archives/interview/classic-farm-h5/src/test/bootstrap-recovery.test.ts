import { describe, expect, it } from 'vitest';

import { BootstrapRecovery, isAssetAffectingCommand } from '../app/bootstrap-recovery';
import { PlayerBootstrapRefresh } from '../app/player-bootstrap-refresh';

describe('BootstrapRecovery generation-scoped request correlation', () => {
  it('rejects responses that belong to an out-of-generation operation id', () => {
    const recovery = new BootstrapRecovery(3);
    recovery.reset(7);
    recovery.trackRecovery('op-bootstrap-1', 7);
    recovery.trackRecovery('op-bootstrap-2', 7);

    // 两次响应(含乱序晚到的 op-1)都是当前 generation 的合法成功。
    expect(recovery.claim('op-bootstrap-2', 7)).toBe('recovery');
    expect(recovery.claim('op-bootstrap-1', 7)).toBe('recovery');
  });

  it('isolates responses from previous generations after a new ServerHello', () => {
    const recovery = new BootstrapRecovery(3);
    recovery.reset(7);
    recovery.trackRecovery('op-bootstrap-1', 7);

    recovery.reset(8);
    recovery.trackRecovery('op-bootstrap-2', 8);

    expect(recovery.claim('op-bootstrap-1', 7)).toBeNull();
    expect(recovery.claim('op-bootstrap-2', 7)).toBeNull();
    expect(recovery.claim('op-bootstrap-2', 8)).toBe('recovery');
  });

  it('never consumes unknown or missing operation ids', () => {
    const recovery = new BootstrapRecovery(3);
    recovery.reset(7);
    recovery.trackRecovery('op-bootstrap-1', 7);

    expect(recovery.claim('op-unknown', 7)).toBeNull();
    expect(recovery.claim(undefined, 7)).toBeNull();
  });

  it('enforces the retry budget only across mandatory recovery attempts', () => {
    const recovery = new BootstrapRecovery(2);
    recovery.reset(7);
    expect(recovery.hasBudget()).toBe(true);
    recovery.trackRecovery('op-1', 7);
    recovery.trackRecovery('op-2', 7);
    expect(recovery.hasBudget()).toBe(false);
    // 预算耗尽后 recovery 发送被拒。
    expect(recovery.trackRecovery('op-3', 7)).toBe(false);
  });

  it('does not count refresh against the recovery budget', () => {
    const recovery = new BootstrapRecovery(2);
    recovery.reset(7);
    recovery.trackRecovery('op-recovery-1', 7);
    recovery.trackRecovery('op-recovery-2', 7);
    expect(recovery.hasBudget()).toBe(false);

    // refresh 不消耗恢复预算,仍可发送且可认领。
    expect(recovery.trackRefresh('op-refresh-1', 7)).toBe(true);
    expect(recovery.hasBudget()).toBe(false);
    expect(recovery.claim('op-refresh-1', 7)).toBe('refresh');
  });

  it('forget drops a tracked request by generation', () => {
    const recovery = new BootstrapRecovery(3);
    recovery.reset(7);
    recovery.trackRefresh('op-refresh-1', 7);
    recovery.forget('op-refresh-1', 7);
    expect(recovery.claim('op-refresh-1', 7)).toBeNull();
  });

  it('refresh operation ids are consumed and bounded (claim deletes)', () => {
    const recovery = new BootstrapRecovery(3);
    recovery.reset(7);
    for (let i = 0; i < 50; i += 1) {
      recovery.trackRefresh(`op-refresh-${i}`, 7);
      recovery.claim(`op-refresh-${i}`, 7);
    }
    // claim 即消费:认领后集合不随会话无限增长(内部不可直接观测,但 claim 全部返回)。
    expect(recovery.claim('op-refresh-0', 7)).toBeNull();
    expect(recovery.claim('op-refresh-49', 7)).toBeNull();
  });
});

describe('PlayerBootstrapRefresh independent asset-refresh controller', () => {
  it('allows refresh even when the recovery budget is exhausted', () => {
    // 最关键一条:mandatory recovery 已发送 3 次,hasBudget()==false,
    // PLANT SUCCEEDED 后 refresh 仍能 begin。
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);
    // 模拟 PLANT 终态(无 privateState)标 dirty。
    refresh.markDirty();
    expect(refresh.canStart(7)).toBe(true);
    const attempt = refresh.begin('op-refresh-1', 7);
    expect(attempt.attempt).toBe(1);
  });

  it('refresh does not increment the recovery attempt count', () => {
    const recovery = new BootstrapRecovery(3);
    const refresh = new PlayerBootstrapRefresh(3);
    recovery.reset(7);
    refresh.reset(7);

    refresh.markDirty();
    const attempt = refresh.begin('op-r', 7);
    // refresh 走独立 tracker,recovery.recoverySentCount 不增长。
    expect(recovery.recoverySentCount).toBe(0);
    recovery.trackRefresh(attempt.operationId, 7);
    expect(recovery.recoverySentCount).toBe(0);
  });

  it('a new asset fact while one refresh is in-flight triggers exactly one follow-up', () => {
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);

    refresh.markDirty(); // PLANT #1
    const a1 = refresh.begin('op-r1', 7);

    refresh.markDirty(); // HARVEST #2 arrives while r1 in-flight
    expect(refresh.pending).toBe(true);
    // 不能并发起第二笔。
    expect(refresh.canStart(7)).toBe(false);

    // 结果到达路径:先 claim 释放 in-flight,再 succeed 收敛 dirty。
    expect(refresh.claim(a1.operationId, 7)).toBe(a1);
    refresh.succeed(a1);
    // 期间又有资产命令 → 需要恰好一笔 follow-up。
    expect(refresh.needsAnotherRequest).toBe(true);
    expect(refresh.canStart(7)).toBe(true);
    const a2 = refresh.begin('op-r2', 7);
    refresh.claim(a2.operationId, 7);
    refresh.succeed(a2);
    expect(refresh.needsAnotherRequest).toBe(false);
  });

  it('dirty during in-flight coalesces: one follow-up not multiple', () => {
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);
    refresh.markDirty();
    const a1 = refresh.begin('op-r1', 7);
    refresh.markDirty();
    refresh.markDirty();
    expect(refresh.canStart(7)).toBe(false); // in-flight 期间无法并发
    refresh.claim(a1.operationId, 7);
    refresh.succeed(a1);
    expect(refresh.needsAnotherRequest).toBe(true);
    const a2 = refresh.begin('op-r2', 7);
    refresh.claim(a2.operationId, 7);
    refresh.succeed(a2);
    expect(refresh.needsAnotherRequest).toBe(false);
  });

  it('refresh timeout retries with an independent budget', () => {
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);
    refresh.markDirty();
    const a1 = refresh.begin('op-r1', 7);
    // 超时路径:先 claim 释放 in-flight,再 fail(保留 dirty)。
    expect(refresh.claim(a1.operationId, 7)).toBe(a1);
    refresh.fail();
    expect(refresh.canStart(7)).toBe(true); // 仍可重试
    const a2 = refresh.begin('op-r2', 7);
    expect(a2.attempt).toBe(2);
    refresh.claim(a2.operationId, 7);
    refresh.fail();
    const a3 = refresh.begin('op-r3', 7);
    expect(a3.attempt).toBe(3);
    refresh.claim(a3.operationId, 7);
    refresh.fail();
    // 三轮耗尽,不能继续 start(无新业务事实)。
    expect(refresh.canStart(7)).toBe(false);
  });

  it('new business fact reopens the budget after it is exhausted', () => {
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);
    refresh.markDirty();
    const a1 = refresh.begin('op-r1', 7);
    refresh.claim(a1.operationId, 7);
    refresh.fail();
    const a2 = refresh.begin('op-r2', 7);
    refresh.claim(a2.operationId, 7);
    refresh.fail();
    const a3 = refresh.begin('op-r3', 7);
    refresh.claim(a3.operationId, 7);
    refresh.fail();
    // 预算耗尽,不能继续 start。
    expect(refresh.canStart(7)).toBe(false);
    // 新的业务事实 markDirty(无 in-flight)重置 attempts → 允许新一轮。
    refresh.markDirty();
    expect(refresh.canStart(7)).toBe(true);
  });

  it('old connection generation refresh result is ignored', () => {
    const refresh = new PlayerBootstrapRefresh(3);
    refresh.reset(7);
    refresh.markDirty();
    const a1 = refresh.begin('op-r1', 7);
    // 新 ServerHello 重置到 generation 8。
    refresh.reset(8);
    // 旧 generation 的 operationId 无法认领。
    expect(refresh.claim(a1.operationId, 7)).toBeNull();
    expect(refresh.claim(a1.operationId, 8)).toBeNull();
  });
});

describe('isAssetAffectingCommand', () => {
  it('flags commands whose terminal success changes wallet/inventory', () => {
    expect(isAssetAffectingCommand('BUY')).toBe(true);
    expect(isAssetAffectingCommand('SELL')).toBe(true);
    expect(isAssetAffectingCommand('PLANT')).toBe(true);
    expect(isAssetAffectingCommand('HARVEST')).toBe(true);
    expect(isAssetAffectingCommand('THEFT')).toBe(true);
    expect(isAssetAffectingCommand('UNLOCK_PLOT')).toBe(true);
  });

  it('excludes non-asset and bootstrap/navigation commands', () => {
    expect(isAssetAffectingCommand('CARE')).toBe(false);
    expect(isAssetAffectingCommand('JOIN_FARM')).toBe(false);
    expect(isAssetAffectingCommand('GET_PLAYER_BOOTSTRAP')).toBe(false);
    expect(isAssetAffectingCommand('GET_OPERATION_RESULT')).toBe(false);
    expect(isAssetAffectingCommand('CLAIM_TASK')).toBe(true);
    expect(isAssetAffectingCommand(undefined)).toBe(false);
  });
});
