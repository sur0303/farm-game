/**
 * TrustedServerClock — H5 受信服务端时钟(A1B-H5-TRUSTED-TIME-MATURITY)。
 *
 * 成熟展示由服务端时钟派生(第 32 号合同:成熟是时间派生视图,不是持久状态
 * 迁移)。本模块维护一个受信服务端时钟锚点,并基于 performance.now()
 * 单调推算当前服务端时间下界:
 *
 *   trustedNowMs = serverNowMs + (performance.now() - receivedMonotonicMs)
 *
 * 禁止使用 Date.now():浏览器墙上时钟可能被用户修改 / NTP 跳变 / 测试注入。
 * 锚点算法是"下界"估计——网络传输已耗时,结果最多让成熟显示稍晚,绝不提前。
 *
 * 锚点可从 ServerHello.server_time_ms 或 Bootstrap.trusted_server_now_ms
 * 刷新。trusted_server_now_ms 不属于 Player Authority 内容 Hash:即使
 * player_state_revision 未变化,新 Bootstrap 也能刷新时钟锚点,且不得因时间
 * 不同触发"同 revision 不同权威内容"拒绝(store 只比较 wallet 等权威内容)。
 */
export interface TrustedClockAnchor {
  /** 服务端时钟(ms),响应到达时捕获。 */
  serverNowMs: number;
  /** 同一时刻的 performance.now() 单调时间(ms)。 */
  receivedMonotonicMs: number;
}

export class TrustedServerClock {
  private serverNowMs: number;
  private receivedMonotonicMs: number;
  private synchronized = false;

  constructor() {
    this.serverNowMs = 0;
    this.receivedMonotonicMs = 0;
  }

  /** 用服务端权威时间刷新锚点(ServerHello / Bootstrap.trusted_server_now_ms)。 */
  sync(serverNowMs: number, nowMonotonicMs: number = performance.now()): void {
    // 单调保护(Q1-MATURITY-FINAL-FOCUSED-REVIEW P1-2):同一 connection 内乱序
    // 晚到的旧响应不得让 trustedNow 倒退。新锚点推算值低于当前推算值时拒绝。
    if (this.synchronized) {
      const currentEstimate = this.serverNowMs + (nowMonotonicMs - this.receivedMonotonicMs);
      if (serverNowMs < currentEstimate) return;
    }
    this.serverNowMs = serverNowMs;
    this.receivedMonotonicMs = nowMonotonicMs;
    this.synchronized = true;
  }

  /** 是否已收到过任何受信服务端时间(未同步时不得派生成熟)。 */
  hasTrustedTime(): boolean {
    return this.synchronized;
  }

  /**
   * 当前受信服务端时间下界(ms)。未同步时返回 null,调用方必须 fail-closed。
   */
  trustedNow(nowMonotonicMs: number = performance.now()): number | null {
    if (!this.synchronized) return null;
    return this.serverNowMs + (nowMonotonicMs - this.receivedMonotonicMs);
  }

  /** 调试/诊断:当前锚点(供页面 data-server-clock-ms 暴露)。 */
  snapshot(): { serverNowMs: number; synchronized: boolean } {
    return { serverNowMs: this.serverNowMs, synchronized: this.synchronized };
  }
}
