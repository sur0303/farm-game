import type { FarmChange, FarmSnapshot, FarmState, PlotState, PrivateState } from '../protocol/types';
import { compareUint64, isNextUint64, parseUint64 } from '../protocol/uint64';

export type FarmApplyResult = 'applied' | 'duplicate' | 'gap';

const VALID_PLOT_STATUSES = new Set(['EMPTY', 'GROWING', 'MATURE']);
const VALID_PLOT_LOCK_STATES = new Set(['UNSPECIFIED', 'UNLOCKED', 'LOCKED']);

/**
 * Q1 问题 B:PlotDelta 结构校验。任何 delta 缺 plotId / 非法 status / 空
 * plotVersion 都视为 malformed。校验失败必须整体拒绝 FarmChange(fail
 * closed),绝不把版本与地块拆开应用。
 */
export const validPlotDeltas = (plots: PlotState[]): boolean => {
  if (!Array.isArray(plots)) return false;
  const seenIndices = new Set<number>();
  return plots.every((plot) => {
    if (!plot || typeof plot !== 'object') return false;
    if (
      typeof plot.plotId !== 'string'
      || plot.plotId !== `plot-${plot.plotIndex}`
      || !Number.isInteger(plot.plotIndex)
      || plot.plotIndex < 0
      || seenIndices.has(plot.plotIndex)
      || plot.plotVersion === undefined
      || !VALID_PLOT_STATUSES.has(plot.status)
      // Lock state and every server-owned offer/rule field are part of the
      // PlotView contract. Missing or malformed values reject the whole delta;
      // they must never be interpreted as an unlocked plot.
      || !VALID_PLOT_LOCK_STATES.has(plot.lockState)
      || !Number.isInteger(plot.unlockOrder)
      || plot.unlockOrder < 0
      || !Number.isInteger(plot.requiredLevel)
      || plot.requiredLevel < 0
      || typeof plot.unlockCandidate !== 'boolean'
      || plot.normalCoinPrice === undefined
      || plot.unlockRuleVersion === undefined
    ) return false;
    try {
      parseUint64(plot.plotVersion, 'plot_version');
      parseUint64(plot.normalCoinPrice, 'normal_coin_price');
      parseUint64(plot.unlockRuleVersion, 'unlock_rule_version');
    } catch {
      return false;
    }
    seenIndices.add(plot.plotIndex);
    return true;
  });
  };

/**
 * Farm/Player reducer:只消费服务端 Snapshot/FarmChange Push 与权威
 * Receipt(Buy/Sell private_state、Bootstrap)。不本地计算金币、产量、
 * 成熟或任何随机结果;成熟状态来自服务端 CropPhase(见 adapter)。
 */
export class FarmStore {
  private privateState: PrivateState = { wallet: 0, inventory: {}, codex: [] };
  /** 私有数据是否已有任一服务端权威来源;false 时 UI 必须显示未知而非 0。 */
  private privateStateAvailable = false;
  private playerStateRevision: string | undefined;
  private readonly farms = new Map<string, FarmSnapshot>();
  private readonly listeners = new Set<() => void>();

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  hasPrivateState(): boolean {
    return this.privateStateAvailable;
  }

  getPrivateState(): PrivateState {
    return {
      economyVersion: this.privateState.economyVersion,
      playerStateRevision: this.privateState.playerStateRevision,
      wallet: this.privateState.wallet,
      inventory: { ...this.privateState.inventory },
      codex: structuredClone(this.privateState.codex ?? []),
      tasks: structuredClone(this.privateState.tasks ?? []),
      profile: this.privateState.profile ? { ...this.privateState.profile } : undefined,
      progression: this.privateState.progression
        ? { level: this.privateState.progression.level, exp: this.privateState.progression.exp }
        : undefined,
      featureSummary: this.privateState.featureSummary,
    };
  }

  getFarm(ownerId: string): FarmSnapshot | undefined {
    const snapshot = this.farms.get(ownerId);
    return snapshot ? structuredClone(snapshot) : undefined;
  }

  applySnapshot(snapshot: FarmSnapshot): void {
    if (!snapshot?.state || !validPlotDeltas(snapshot.state.plots)) return;
    const current = this.farms.get(snapshot.ownerId);
    // A late snapshot from an old Join must not roll back plots or replace a
    // newer owner profile. The active WsClient fence filters most of these;
    // this reducer guard is the final stale-data boundary.
    if (current) {
      const versionComparison = compareUint64(snapshot.farmVersion, current.farmVersion);
      if (versionComparison < 0) return;
      if (versionComparison === 0) {
        // A same-version rejoin may be the first legal delivery containing
        // owner_public_profile. Merge that identity only; never replace the
        // already-applied plots with an equal-version stale snapshot.
        if (!current.ownerPublicProfile && snapshot.ownerPublicProfile) {
          this.farms.set(snapshot.ownerId, {
            ...structuredClone(current),
            ownerPublicProfile: structuredClone(snapshot.ownerPublicProfile),
          });
          this.notify();
        }
        return;
      }
    }
    this.farms.set(snapshot.ownerId, structuredClone(snapshot));
    this.notify();
  }

  applyChange(change: FarmChange): FarmApplyResult {
    if (!change || typeof change !== 'object') return 'gap';
    const current = this.farms.get(change.ownerId);
    if (current && isNextUint64(current.farmVersion, change.farmVersion)) {
      // Q1 问题 B / H5-VERTICAL-CORRECTNESS §4:先完整验证、再整体提交。
      // 任何 PlotDelta 非法(缺 plotId / 非法 status)都整体拒绝,绝不留下
      // "farmVersion 已推进但 plot 未应用" 的不可能状态;返回 gap 由调用方
      // 触发 Fresh Join / Resync(fail closed)。
      if (!change?.state || !validPlotDeltas(change.state.plots)) return 'gap';
      // 以当前 Snapshot 为基底(H5-VERTICAL-CORRECTNESS-A1 §4):FarmChange 只
      // 携带 changed_plots + subscription fence,不得丢弃 Snapshot 上的
      // join_attempt_id / private_state_included 等订阅元数据(fence 保留)。
      const base = structuredClone(current);
      const next: FarmSnapshot = {
        ...base,
        farmVersion: change.farmVersion,
        subscriptionId: change.subscriptionId ?? base.subscriptionId,
      };
      if (change.partial) {
        const plots = new Map(base.state.plots.map((plot) => [plot.plotId, plot]));
        for (const plot of change.state.plots) {
          // changed_plots contains complete authoritative PlotViews, not field
          // patches. Replace the plot as a unit so lifecycle-scoped optional
          // fields (cropCycleId/wateredAt/theft counters) disappear when the
          // server projects an empty plot after Harvest. Lock/rule fields are
          // mandatory under validPlotDeltas, so replacement remains fail closed.
          plots.set(plot.plotId, structuredClone(plot));
        }
        next.state = {
          ...base.state,
          plots: [...plots.values()],
        };
      } else {
        next.state = change.state;
      }
      this.farms.set(change.ownerId, next);
      this.notify();
      return 'applied';
    }
    if (!current) return 'gap';
    if (compareUint64(change.farmVersion, current.farmVersion) <= 0) return 'duplicate';
    return 'gap';
  }

  /** 权威 Receipt(Buy/Sell 等)携带的 PrivateState:按 economy_version 单调应用。 */
  applyPrivateState(privateState: PrivateState | undefined): void {
    if (!privateState) return;
    if (
      privateState.economyVersion === undefined
      && this.privateState.economyVersion !== undefined
    ) return;
    if (
      privateState.economyVersion !== undefined
      && this.privateState.economyVersion !== undefined
      && compareUint64(privateState.economyVersion, this.privateState.economyVersion) < 0
    ) return;
    this.privateState = structuredClone({
      ...privateState,
      playerStateRevision: privateState.playerStateRevision ?? this.privateState.playerStateRevision,
      profile: privateState.profile ?? this.privateState.profile,
      featureSummary: privateState.featureSummary ?? this.privateState.featureSummary,
    });
    this.privateStateAvailable = true;
    this.notify();
  }

  /** Apply a direct TaskRead/Confirm authority summary without replacing economy. */
  applyTaskSummary(summary: NonNullable<PrivateState['featureSummary']>['tasks']): void {
    if (!summary) return;
    const current = this.privateState.featureSummary?.tasks;
    if (current && compareUint64(summary.taskStateVersion, current.taskStateVersion) < 0) return;
    this.privateState = structuredClone({
      ...this.privateState,
      featureSummary: { ...(this.privateState.featureSummary ?? {}), tasks: summary },
    });
    this.privateStateAvailable = true;
    this.notify();
  }

  /**
   * GET_PLAYER_BOOTSTRAP:按 player_state_revision 单调应用(28 §3.7 约束 4)。
   * (H5-VERTICAL-CORRECTNESS-A1 §4)相同 revision 不允许不同权威内容静默覆盖:
   * revision 相同但经济内容不同说明权威链路不一致,保留已应用内容。
   *
   * 返回是否"成功应用"或"幂等一致接受"(H5-BOOTSTRAP-CORRELATION-PLAYER-REFRESH):
   * true = 已写入或与已应用内容一致;false = 缺失/旧 revision/同 revision 不同
   * 内容,调用方不得据此标记 applied。
   */
  applyPlayerBootstrap(privateState: PrivateState | undefined): boolean {
    if (!privateState) return false;
    const revision = privateState.playerStateRevision;
    if (revision !== undefined && this.playerStateRevision !== undefined) {
      const comparison = compareUint64(revision, this.playerStateRevision);
      if (comparison < 0) return false;
      if (comparison === 0) {
        if (!samePrivateStateContent(this.privateState, privateState)) return false;
        return true;
      }
    }
    if (revision !== undefined) this.playerStateRevision = String(revision);
    this.privateState = structuredClone({
      ...privateState,
      economyVersion: this.privateState.economyVersion,
    });
    this.privateStateAvailable = true;
    this.notify();
    return true;
  }

  setInitialState(state: FarmState): void {
    this.applyPrivateState({
      economyVersion: state.economyVersion,
      wallet: state.wallet,
      inventory: { ...state.inventory },
      codex: structuredClone(state.codex ?? []),
      tasks: structuredClone(state.tasks ?? []),
    });
  }

  onFarm(ownerId: string): FarmSnapshot | undefined {
    return this.getFarm(ownerId);
  }

  private notify(): void {
    for (const listener of this.listeners) listener();
  }
}

/** 比较两份私有状态的权威内容(wallet/inventory/profile/progression)。 */
const samePrivateStateContent = (a: PrivateState, b: PrivateState): boolean => {
  if (a.wallet !== b.wallet) return false;
  const aInventory = a.inventory ?? {};
  const bInventory = b.inventory ?? {};
  const aKeys = Object.keys(aInventory);
  if (aKeys.length !== Object.keys(bInventory).length) return false;
  for (const key of aKeys) {
    if (aInventory[key] !== bInventory[key]) return false;
  }
  if (JSON.stringify(a.profile ?? null) !== JSON.stringify(b.profile ?? null)) return false;
  if (JSON.stringify(a.progression ?? null) !== JSON.stringify(b.progression ?? null)) return false;
  return true;
};
