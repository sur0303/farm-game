import { describe, expect, it } from 'vitest';

import {
  formatCommandError,
  formatUnlockCommandError,
  unlockCommandPresentation,
} from '../app/error-surface';
import {
  bootstrapConditionMet,
  classifyBootstrapResult,
  recoveryBarrierSatisfied,
  type RecoveryBarrierFacts,
} from '../app/recovery-barrier';

const readyFacts: RecoveryBarrierFacts = {
  wsRecovering: true,
  joinReady: true,
  ownSnapshotReady: true,
  bootstrapOutcome: 'applied',
  pendingFirstPassComplete: true,
  onOwnFarm: true,
};

describe('recovery barrier predicate', () => {
  it('opens only when bootstrap, pending recovery and the public Join Ready all hold', () => {
    expect(recoveryBarrierSatisfied(readyFacts)).toBe(true);
    expect(recoveryBarrierSatisfied({ ...readyFacts, wsRecovering: false })).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, joinReady: false })).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, ownSnapshotReady: false })).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, pendingFirstPassComplete: false })).toBe(false);
    // Gate accepts ClientRecoveryComplete only while the authenticated owner's
    // subscription is active. A restored friend Snapshot cannot open it.
    expect(recoveryBarrierSatisfied({ ...readyFacts, onOwnFarm: false })).toBe(false);
  });

  it('opens the barrier only on an applied bootstrap; feature-disabled/unreachable stay closed', () => {
    expect(bootstrapConditionMet('pending')).toBe(false);
    expect(bootstrapConditionMet('applied')).toBe(true);
    // H5-VERTICAL-CORRECTNESS-A1 §1:feature-disabled/unreachable 不得打开屏障。
    expect(bootstrapConditionMet('feature-disabled')).toBe(false);
    expect(bootstrapConditionMet('unreachable')).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, bootstrapOutcome: 'pending' })).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, bootstrapOutcome: 'feature-disabled' })).toBe(false);
    expect(recoveryBarrierSatisfied({ ...readyFacts, bootstrapOutcome: 'unreachable' })).toBe(false);
  });
});

describe('classifyBootstrapResult', () => {
  it('maps terminal results to bootstrap outcomes and ignores interim ones', () => {
    expect(classifyBootstrapResult({ status: 'SUCCEEDED' })).toBe('applied');
    expect(classifyBootstrapResult({ status: 'NO_CHANGE' })).toBe('applied');
    expect(classifyBootstrapResult({
      status: 'REJECTED',
      errorCode: 'ERR_COMMAND_UNIMPLEMENTED',
      featureUnavailable: true,
    })).toBe('feature-disabled');
    expect(classifyBootstrapResult({ status: 'REJECTED', errorCode: 'ERR_RATE_LIMITED' })).toBeNull();
    expect(classifyBootstrapResult({ status: 'PROCESSING' })).toBeNull();
    expect(classifyBootstrapResult({ status: 'ACCEPTED' })).toBeNull();
  });
});

describe('formatCommandError', () => {
  it('keeps the public error code and request/operation ids visible', () => {
    expect(formatCommandError({
      message: '未建立好友关系，不能访问该农场',
      errorCode: 'ERR_NOT_FRIEND',
      errorCodeNumber: 2101,
      operationId: 'op-1',
      requestId: 'req-1',
    })).toBe('未建立好友关系，不能访问该农场 [ERR_NOT_FRIEND op=op-1 req=req-1]');
  });

  it('falls back gracefully when ids are missing', () => {
    expect(formatCommandError({ message: '失败' })).toBe('失败 [UNKNOWN_ERROR]');
    expect(formatCommandError({ message: '', errorCodeNumber: 1007 })).toBe('操作失败 [CODE_1007]');
  });
});

describe('formatUnlockCommandError', () => {
  it('uses Chinese business messages and never exposes raw ERR_INTERNAL', () => {
    expect(formatUnlockCommandError({
      errorCode: 'ERR_LEVEL_REQUIREMENT_NOT_MET', operationId: 'op-level',
    })).toContain('等级不足');
    expect(formatUnlockCommandError({ errorCode: 'ERR_INSUFFICIENT_COIN' })).toContain('金币不足');
    expect(formatUnlockCommandError({ errorCode: 'ERR_PREVIOUS_PLOT_LOCKED' })).toContain('前一块');
    const internal = formatUnlockCommandError({ errorCode: 'ERR_INTERNAL', operationId: 'op-internal' });
    expect(internal).toContain('土地解锁暂时失败');
    expect(internal).not.toContain('ERR_INTERNAL');
  });

  it('renders honest processing/terminal copy for every unlock outcome', () => {
    expect(unlockCommandPresentation('PROCESSING', {})).toEqual({ message: '土地解锁处理中', error: false });
    expect(unlockCommandPresentation('SUCCEEDED', {})).toEqual({
      message: '土地已解锁，正在同步权威农场状态', error: false,
    });
    expect(unlockCommandPresentation('NO_CHANGE', {})).toEqual({
      message: '土地无需重复解锁，正在同步权威农场状态', error: false,
    });
    expect(unlockCommandPresentation('REJECTED', {
      errorCode: 'ERR_INSUFFICIENT_COIN', operationId: 'op-coin',
    })).toMatchObject({ message: expect.stringContaining('金币不足'), error: true });
    expect(unlockCommandPresentation('REJECTED', {
      errorCode: 'ERR_PLOT_LOCKED',
    })).toMatchObject({ message: expect.stringContaining('锁定状态'), error: true });
    expect(unlockCommandPresentation('REJECTED', {
      errorCode: 'ERR_RULE_VERSION_MISMATCH',
    })).toMatchObject({ message: expect.stringContaining('规则已更新'), error: true });
    expect(unlockCommandPresentation('REJECTED', {
      errorCode: 'ERR_VERSION_CONFLICT',
    })).toMatchObject({ message: expect.stringContaining('状态已更新'), error: true });
  });
});
