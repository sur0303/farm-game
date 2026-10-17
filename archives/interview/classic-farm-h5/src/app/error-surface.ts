import type { CommandResult, ResultStatus } from '../protocol/types';

/**
 * 面向集成排查的公开错误面:在页面上展示公开 error code 与
 * request_id/operation_id,使 Playwright 用例与人工排查都能直接读取。
 */

type ErrorCarrier = Partial<Pick<
  CommandResult,
  'message' | 'errorCode' | 'errorCodeNumber' | 'operationId' | 'requestId'
>>;

/** 把 CommandResult 渲染成可诊断的一行错误文案。 */
export const formatCommandError = (result: ErrorCarrier): string => {
  const code = result.errorCode
    ?? (result.errorCodeNumber ? `CODE_${result.errorCodeNumber}` : 'UNKNOWN_ERROR');
  const base = result.message || '操作失败';
  const op = result.operationId ? ` op=${result.operationId}` : '';
  const req = result.requestId ? ` req=${result.requestId}` : '';
  return `${base} [${code}${op}${req}]`;
};

const UNLOCK_ERROR_MESSAGES: Record<string, string> = {
  ERR_LEVEL_REQUIREMENT_NOT_MET: '等级不足，暂不能解锁这块土地',
  ERR_INSUFFICIENT_COIN: '金币不足，无法解锁这块土地',
  ERR_PLOT_LOCKED: '这块土地仍处于锁定状态，请刷新后重试',
  ERR_PREVIOUS_PLOT_LOCKED: '请先解锁前一块土地',
  ERR_RULE_VERSION_MISMATCH: '解锁规则已更新，请刷新农场后重试',
  ERR_VERSION_CONFLICT: '土地状态已更新，请刷新农场后重试',
  ERR_INTERNAL: '土地解锁暂时失败，请稍后重试',
};

/** Business-facing unlock errors deliberately omit raw ERR_INTERNAL/code text. */
export const formatUnlockCommandError = (result: ErrorCarrier): string => {
  const code = result.errorCode ?? (result.errorCodeNumber ? `CODE_${result.errorCodeNumber}` : '');
  const base = UNLOCK_ERROR_MESSAGES[code] ?? '土地解锁未完成，请稍后重试';
  return result.operationId ? `${base}（操作 ${result.operationId}）` : base;
};

/** Single status-to-copy table used by the page and its protocol/UI tests. */
export const unlockCommandPresentation = (
  status: ResultStatus,
  result: ErrorCarrier,
): { message: string; error: boolean } | undefined => {
  switch (status) {
    case 'PROCESSING': return { message: '土地解锁处理中', error: false };
    case 'ACCEPTED': return { message: '土地解锁请求已受理，等待服务端裁决', error: false };
    case 'SUCCEEDED': return { message: '土地已解锁，正在同步权威农场状态', error: false };
    case 'NO_CHANGE': return { message: '土地无需重复解锁，正在同步权威农场状态', error: false };
    case 'REJECTED':
    case 'UNAVAILABLE': return { message: formatUnlockCommandError(result), error: true };
    default: return undefined;
  }
};
