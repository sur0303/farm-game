import type { AuthProvider } from '../auth/types';
import { CommandService } from '../domain/command-service';
import { BoundedOperationJournal, IndexedDbOperationJournal } from '../persistence/operation-journal';
import type { CommandResult, FarmSnapshot, PlotState, PrivateState, ServerFrame, TaskListView, Uint64Value } from '../protocol/types';
import { FarmStore } from '../state/farm-store';
import { BrowserWebSocketTransport } from '../network/transport';
import { WsClient } from '../network/ws-client';
import { finalProtocolAdapter } from '../protocol/protocol-adapter';
import { clientEnvironment, resolveWebSocketUrl } from './environment';
import {
  type ShareIntentResolver,
  type ShareLinkCreator,
} from '../share/share-intent';
import { consumeShareTokenFromUrl, readShareToken } from '../share/share-url';
import {
  HttpFriendRequestError,
  HttpGameFriendService,
  HttpShareLinkCreator,
  type GameFriendService,
} from '../friends/game-friend-service';
import { FriendActionMask, type FriendOpportunityStatus, type FriendSummary, type FriendTheftTarget } from '../friends/friend-roster';
import { WebAuthProvider, type WebAccountMode } from '../auth/web-auth-provider';
import {
  CROP_CATALOG,
  cropById,
  cropByOfferId,
  produceInventoryKey,
  seedInventoryKey,
} from '../domain/crop-catalog';
import {
  classifyBootstrapResult,
  recoveryBarrierSatisfied,
  type BootstrapOutcome,
} from './recovery-barrier';
import { BootstrapRecovery, isAssetAffectingCommand } from './bootstrap-recovery';
import { PlayerBootstrapRefresh } from './player-bootstrap-refresh';
import { careRewardRefreshAction, CareRewardRefresh } from './care-reward-refresh';
import {
  freezeBatchTargets,
  runPlotBatch,
  type BatchTarget,
} from './batch-command-runner';
import { freezeSellBatchTargets, runSellBatch } from './sell-batch-runner';
import {
  canUnlockPlot,
  effectiveCropPhase,
  isPlantablePlot,
  isWaterablePlot,
  selectTheftPlot,
  unlockPlotPayload,
} from './plot-guards';
import { TrustedServerClock } from './trusted-server-clock';
import { formatCommandError, unlockCommandPresentation } from './error-surface';
import { compareUint64 } from '../protocol/uint64';
import { createUuid } from '../platform/random-id';
import { canNavigateAcceptedShareNow } from './share-navigation';
import { gainedFriendOpportunities, presentFriendOpportunity } from '../friends/opportunity-presentation';
import { preservePagedRosterBaseline, refreshedRosterNextCursor } from '../friends/roster-page-refresh';
import { isExactTheftPlotEligible, matchesTheftConfirmation, theftTerminalPresentation, type TheftConfirmation } from './theft-controls';
import { cropTimeProjection } from './crop-time-projection';
import { FriendOpportunityFlow } from './friend-opportunity-flow';
import {
  FRIEND_ROSTER_FALLBACK_POLL_MS,
  shouldRefreshFriendRoster,
} from './friend-roster-refresh-policy';

import './styles.css';

const escapeHtml = (value: string): string => value.replace(/[&<>"']/g, (character) => ({
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
}[character] ?? character));

const avatarIconForId = (avatarId: number): string => {
  const avatars = ['🧑‍🌾', '👩‍🌾', '🧑‍🍳', '🧙‍♀️', '🦊', '🐼', '🐰', '🐯'];
  return avatars[(Math.max(1, avatarId) - 1) % avatars.length] ?? avatars[0];
};

type ToolMode = 'plant' | 'water' | 'harvest';
type UtilityMode = 'inventory' | 'shop' | 'codex' | 'tasks' | 'more' | null;
type InventoryTab = 'crops' | 'tools';

const TOOL_HINTS: Record<ToolMode, string> = {
  plant: '🥕 选中种子工具 — 点击一块已解锁空地播种',
  water: '💧 选中水桶工具 — 点击一个生长期作物浇水',
  harvest: '👋 选中收获工具 — 点击一个成熟作物收获',
};

const WEB_ACTIVE_PLAYER_STORAGE_KEY = 'classic-farm.web-active-player-id';

/** GET_PLAYER_BOOTSTRAP 每轮 Recovery 的发送预算(退避 750ms 倍增、5s 封顶)。 */
const MAX_BOOTSTRAP_ATTEMPTS = 3;
const BATCH_OPERATION_TIMEOUT_MS = 15_000;

export class FarmApp {
  private auth!: AuthProvider;
  private readonly store = new FarmStore();
  private readonly journal = new BoundedOperationJournal(new IndexedDbOperationJournal());
  private ws!: WsClient;
  private commandService: CommandService | null = null;
  private readonly root: HTMLElement;
  private readonly statusElement: HTMLElement;
  private readonly farmElement: HTMLElement;
  private readonly privateElement: HTMLElement;
  private readonly pendingElement: HTMLElement;
  private readonly messageElement: HTMLElement;
  private readonly toastElement: HTMLElement;
  private readonly farmTitleElement: HTMLElement;
  private readonly farmSubtitleElement: HTMLElement;
  private readonly harvestAllButton: HTMLButtonElement;
  private readonly careAllButton: HTMLButtonElement;
  private readonly stealAllButton: HTMLButtonElement;
  private readonly theftButton: HTMLButtonElement;
  private readonly authScreen: HTMLElement;
  private readonly authMessageElement: HTMLElement;
  private readonly loginButton: HTMLButtonElement;
  private readonly registerButton: HTMLButtonElement | null;
  private readonly webUsernameInput: HTMLInputElement | null;
  private readonly webPasswordInput: HTMLInputElement | null;
  private readonly stagingAccountSelect: HTMLSelectElement | null;
  private readonly shareButtons: HTMLButtonElement[];
  private pendingShareToken: string | null;
  private shareResolver: ShareIntentResolver | null = null;
  private shareCreator: ShareLinkCreator | null = null;
  private pendingShareOwnerId: string | null = null;
  private ownOwnerId = 'player-a';
  private currentOwnerId = 'player-a';
  private selectedPlotId: string | null = null;
  private activeTool: ToolMode | null = null;
  private activeCropDefinitionId = 1;
  private batchSweepRunning = false;
  private batchSweepCommandType: 'HARVEST' | 'CARE' | 'THEFT' | null = null;
  /** 受信服务端时钟(trusted_server_now_ms + performance.now 锚点,禁 Date.now)。 */
  private readonly trustedClock = new TrustedServerClock();
  /** 展示成熟重算用的单 Timer(取农场内最小 mature_at,到期重算 render)。 */
  private maturityTimer: number | undefined;
  /** 每秒只刷新地块倒计时/生长图标，不重建整个应用 DOM。 */
  private farmClockProjectionTimer: number | undefined;
  /** visibilitychange 恢复重算监听器(存引用以便登出时移除)。 */
  private readonly onVisibilityVisible = (): void => {
    if (document.visibilityState !== 'visible') return;
    this.render();
    this.pollFriendRosterOpportunities();
  };
  private readonly friendVisitButton: HTMLButtonElement;
  private readonly friendsElement: HTMLElement;
  private readonly friendsModal: HTMLElement;
  private readonly friendSearchInput: HTMLInputElement;
  private readonly utilitySheet: HTMLElement;
  private readonly utilityTitleElement: HTMLElement;
  private readonly utilityContentElement: HTMLElement;
  private readonly playerNameElement: HTMLElement;
  private readonly friendIds = new Set<string>();
  private readonly friendLabels = new Map<string, string>();
  private readonly friendProfiles = new Map<string, { avatarId: number; level: number }>();
  private readonly friendAuthority = new Map<string, { actionMask: number; status: FriendOpportunityStatus; relationEpoch: bigint; opportunityRevision: bigint; theftEligibleTargets: FriendTheftTarget[] }>();
  /** Cursor of the loaded page containing each friend; used for post-Ready exact refresh. */
  private readonly friendPageCursorByOwner = new Map<string, string>();
  /** Last successful READY pull, retained across transient UNKNOWN hint states. */
  private readonly friendReadyActionMasks = new Map<string, number>();
  /** Authority-confirmed successful/already-consumed Theft cycles for this browser session. */
  private readonly confirmedTheftCycles = new Set<string>();
  private readonly theftOperationTargets = new Map<string, { ownerId: string; plotId: number; cropCycleId: string }>();
  /** A Theft confirmation is armed only by the first Theft-button activation. */
  private pendingTheftConfirmation: TheftConfirmation | null = null;
  private friendRosterRequestsInFlight = 0;
  private friendRosterNextCursor = '';
  private friendRosterRevision = 0n;
  private readonly friendVisitHistory: string[] = [];
  private friendVisitHistoryIndex = -1;
  private friendRosterState: 'idle' | 'loading' | 'ready' | 'error' = 'idle';
  private friendRosterRequest = 0;
  private friendRosterError = '';
  private friendOpportunityState: 'idle' | 'loading' | 'ready' | 'error' = 'idle';
  private readonly friendOpportunityFlow = new FriendOpportunityFlow();
  private homeReady = false;
  private toastTimer: number | null = null;
  private friendService: GameFriendService | null = null;
  private recoveryTargetOwnerId = '';
  /** Preserve the original navigation intent across every transport attempt in one recovery cycle. */
  private recoveryCycleActive = false;
  private recoveryOwnSnapshotReady = false;
  /** Bootstrap 终态;仅 'applied' 满足 RecoveryBarrier Bootstrap 条件(A1 §1)。 */
  private bootstrapOutcome: BootstrapOutcome = 'pending';
  /** 当前 generation 内已发送 Bootstrap 请求的相关性跟踪(A1 §2 乱序不丢)。 */
  private readonly bootstrapRecovery = new BootstrapRecovery(MAX_BOOTSTRAP_ATTEMPTS);
  private recoveryPlayerBootstrapTimer: number | undefined;
  /** 经营命令后资产收敛的独立 refresh 控制器(Q1 预算分离,不与恢复共享预算)。 */
  private readonly playerBootstrapRefresh = new PlayerBootstrapRefresh(3);
  private readonly careRewardRefresh = new CareRewardRefresh(3);
  private careRewardStatus: 'IDLE' | 'PENDING' | 'SETTLED' | 'EXHAUSTED' = 'IDLE';
  private careRewardRefreshTimer: number | undefined;
  private playerBootstrapRefreshTimer: number | undefined;
  /** 诊断:本 generation 资产 refresh 已发送/已应用/已重试次数。 */
  private bootstrapRefreshSent = 0;
  private bootstrapRefreshApplied = 0;
  /** 进行中的农场导航(用于把 JOIN_FARM REJECTED 归因并自动回退)。 */
  private pendingNavigation: { targetOwnerId: string; previousOwnerId: string; operationId: string } | null = null;
  /** The journal scan succeeded and every recovered operation's first query was queued. */
  private recoveryPendingInitialPassComplete = false;
  private recoveryPendingStarted = false;
  /** After a command result applies private inventory, ignore late Join snapshots. */
  private allowPrivateSnapshot = true;
  private recoveryBootstrapGeneration = 0;
  /** Receipt fence awaiting the matching authoritative Farm projection. */
  private pendingUnlockProjection: { ownerId: string; plotIndex: number; farmVersion: Uint64Value } | null = null;
  private utilityMode: UtilityMode = null;
  /** Preserve user input across the 500ms authoritative-state render tick. */
  private shopCount = 1;
  private inventoryTab: InventoryTab = 'crops';
  private bulkSelling = false;
  private taskList: TaskListView | null = null;
  private taskListLoading = false;
  private confirmingFriendVisit = false;
  private friendSearch = '';
  // Native HTML drag-and-drop is inconsistent across desktop WebViews and is
  // unavailable in several mobile WebViews. Keep the drag state ourselves so
  // mouse, touch and pen all follow the same planting path.
  private seedPointerId: number | null = null;
  private seedPointerStart: { x: number; y: number } | null = null;
  private seedDragging = false;
  private seedDropTarget: HTMLElement | null = null;
  private ignoreNextSeedClick = false;
  private dragGhost: HTMLElement | null = null;
  private readonly handleWebIdentityChange = (event: StorageEvent): void => {
    if (
      clientEnvironment.name !== 'web'
      || event.key !== WEB_ACTIVE_PLAYER_STORAGE_KEY
      || !event.newValue
      || !this.commandService
      || event.newValue === this.ownOwnerId
    ) return;
    this.ws.disconnect();
    this.returnToAuth('同一浏览器配置已切换到另一个账号。每个浏览器配置在当前域名只能保持一个登录账号；多用户联调请使用隐私窗口或独立浏览器配置。');
  };

  constructor(root: HTMLElement) {
    this.root = root;
   this.pendingShareToken = readShareToken();
   consumeShareTokenFromUrl();
    const authDescription = clientEnvironment.name === 'staging'
        ? '当前使用 Staging 测试登录，将连接测试环境服务端。'
      : clientEnvironment.name === 'wechat'
          ? '当前等待微信小程序交接一次性登录票据。'
          : '注册一个网页账号，农场资产和好友关系会保存在服务端。';
    const authAccountLabel = clientEnvironment.name === 'wechat' ? '小程序交接' : clientEnvironment.name === 'web' ? '网页账号' : '测试账号';
    const stagingAccount = clientEnvironment.name === 'staging' ? this.initialStagingAccount() : 'player-a';
    const authAccountControl = clientEnvironment.name === 'staging'
      ? `<select data-staging-account aria-label="Staging 测试账号">
          <option value="player-a"${stagingAccount === 'player-a' ? ' selected' : ''}>player-a · 10001</option>
          <option value="player-b"${stagingAccount === 'player-b' ? ' selected' : ''}>player-b · 10002</option>
        </select>`
      : clientEnvironment.name === 'web'
        ? `<div class="web-account-fields">
            <label><span>账号</span><input data-web-username name="username" autocomplete="username" minlength="3" maxlength="24" placeholder="3–24 位字母、数字或下划线"></label>
            <label><span>密码</span><input data-web-password name="password" type="password" autocomplete="current-password" minlength="8" maxlength="128" placeholder="至少 8 个字符"></label>
          </div>`
      : `<strong>${clientEnvironment.name === 'wechat' ? 'launch_ticket' : 'player-a'}</strong>`;
    const authButtonLabel = clientEnvironment.name === 'staging'
        ? '使用 Staging 账号登录'
      : clientEnvironment.name === 'wechat'
          ? '使用微信身份登录'
          : '登录';
    this.root.innerHTML = `
      <section class="auth-screen" data-auth-screen>
        <div class="auth-card">
          <p class="eyebrow">CLASSIC FARM · H5 DEV</p>
          <h1>进入经典农场</h1>
          <p class="auth-description">${authDescription}</p>
          <div class="auth-account"><span>${authAccountLabel}</span>${authAccountControl}</div>
          <div class="auth-actions${clientEnvironment.name === 'web' ? ' split' : ''}">
            <button data-login>${authButtonLabel}</button>
            ${clientEnvironment.name === 'web' ? '<button type="button" class="secondary-auth" data-register>注册并进入</button>' : ''}
          </div>
          <p class="auth-message" data-auth-message>登录后将获取一次性测试 connection_ticket。</p>
        </div>
      </section>
      <div class="app-shell" data-app-shell hidden>
        <div class="farm-landscape">
          <header class="game-hud">
            <button type="button" class="player-chip" data-my-farm aria-label="回到我的农场">
              <span class="player-avatar" aria-hidden="true">🧑‍🌾</span>
              <span class="player-copy"><small>CLASSIC FARM</small><strong data-player-name>player-a</strong><em>我的农场</em></span>
            </button>
            <div class="hud-resources" data-private aria-label="我的资产"></div>
            <div class="connection" aria-live="polite"><span class="status-dot"></span><span data-status>Disconnected</span></div>
          </header>

          <main class="farm-stage">
            <div class="scenery scenery-cloud cloud-one" aria-hidden="true"></div>
            <div class="scenery scenery-cloud cloud-two" aria-hidden="true"></div>
            <div class="scenery scenery-hill hill-back" aria-hidden="true"></div>
            <div class="scenery scenery-hill hill-front" aria-hidden="true"></div>

            <aside class="action-rail action-rail-left" aria-label="快捷功能">
              <button type="button" class="round-action" data-share aria-label="分享农场"><span>↗</span><small>分享</small></button>
              <button type="button" class="round-action" data-open-utility="more" aria-label="更多功能"><span>☰</span><small>更多</small></button>
            </aside>

            <section class="farm-area" data-farm-area aria-label="农场地块">
              <div class="farm-scene" aria-hidden="true">
                <span class="scene-sun"></span>
                <span class="scene-cottage"><i>🏠</i></span>
                <span class="scene-tree tree-one"></span><span class="scene-tree tree-two"></span>
                <span class="scene-flower flower-one"></span><span class="scene-flower flower-two"></span><span class="scene-flower flower-three"></span>
                <span class="scene-path"></span>
                <span class="scene-fence fence-back"></span>
              </div>
              <div class="farm-sign-wrap">
                <div class="farm-sign" data-farm-sign>
                  <div class="farm-sign-copy">
                    <p class="eyebrow">FARM VIEW</p>
                    <h1 data-farm-title>我的农场</h1>
                    <span class="farm-sign-sub" data-farm-subtitle>播种 · 浇水 · 收获你的胡萝卜</span>
                  </div>
                  <span class="version" data-version>farm_version —</span>
                </div>
              </div>
              <div class="field-frame"><div class="plot-grid" data-farm></div></div>
              <div class="farm-batch-actions" data-batch-actions>
                <button type="button" class="batch-action harvest-all" data-harvest-all hidden><span>🧺</span><b>一键收获</b></button>
                <button type="button" class="batch-action care-all" data-care-all hidden><span>💧</span><b>一键护理</b></button>
                <button type="button" class="batch-action steal-all" data-steal-all hidden><span>🦝</span><b>一键偷菜</b></button>
              </div>
              <div class="farm-footer"><span data-selection>点击一个地块查看操作目标</span><span class="hint">选择工具后点地块操作</span></div>
            </section>

            <aside class="action-rail action-rail-right" aria-label="农场功能">
              <button type="button" class="round-action shop-action" data-open-utility="shop" aria-label="打开商店"><span>🏪</span><small>商店</small></button>
              <button type="button" class="round-action" data-open-utility="inventory" aria-label="打开仓库"><span>🧺</span><small>仓库</small></button>
            </aside>

            <section class="tool-ribbon" aria-label="农场工具">
              <p class="tool-hint" data-tool-hint>点击工具，或拖动种子到一块绿地</p>
              <div class="seed-quick-bar" data-seed-quick role="group" aria-label="选择种子"></div>
              <div class="tools">
                <button type="button" class="tool-btn seed-drag" data-tool="plant" data-seed-drag aria-label="选择种子工具" title="按住并拖到已解锁绿地播种"><span class="tool-icon" data-selected-seed-icon>🥕</span><span data-selected-seed-label>种子</span></button>
                <button type="button" class="tool-btn" data-tool="water" data-water aria-label="选择浇水工具"><span class="tool-icon">💧</span><span>浇水</span></button>
                <button type="button" class="tool-btn" data-tool="harvest" data-harvest aria-label="选择收获工具"><span class="tool-icon">🧤</span><span>收获</span></button>
                <button type="button" class="tool-btn theft-tool" data-theft aria-label="偷取好友成熟作物"><span class="tool-icon">🦝</span><span>偷菜</span></button>
              </div>
            </section>
          </main>

          <nav class="bottom-dock" aria-label="游戏主导航">
            <button type="button" data-my-farm><span>⌂</span><small>农场</small></button>
            <button type="button" data-open-utility="inventory"><span>🧺</span><small>仓库</small></button>
            <button type="button" data-open-utility="shop"><span>🏪</span><small>商店</small></button>
            <button type="button" class="friend-dock" data-open-friends aria-expanded="false"><span>👥</span><small>好友</small><i data-friend-count hidden>0</i></button>
            <button type="button" data-open-utility="codex" aria-label="打开植物图鉴"><span>📖</span><small>图鉴</small></button>
            <button type="button" data-open-utility="tasks" aria-label="打开任务"><span>📋</span><small>任务</small></button>
          </nav>

          <section class="utility-sheet" data-utility-sheet hidden aria-live="polite">
            <header><p class="eyebrow">FARM SERVICE</p><h2 data-utility-title>农场服务</h2><button type="button" class="close-button" data-close-utility aria-label="关闭">×</button></header>
            <div data-utility-content></div>
            <div data-pending hidden>暂无未决操作</div>
          </section>
          <p class="message" data-message>等待连接…</p>
        </div>

        <div class="friends-modal" data-friends-modal hidden>
          <div class="friends-scrim" data-close-friends></div>
          <section class="friends-panel" role="dialog" aria-modal="true" aria-labelledby="friends-title">
            <header class="friends-panel-header"><div><p class="eyebrow">SOCIAL GARDEN</p><h2 id="friends-title">好友</h2></div><button type="button" class="close-button" data-close-friends aria-label="关闭好友面板">×</button></header>
            <div class="friend-tabs" role="tablist"><button type="button" class="active" role="tab" aria-selected="true">同玩好友</button><button type="button" role="tab" disabled title="当前服务端尚未提供微信通讯录关系">邀请好友</button></div>
            <label class="friend-search"><span aria-hidden="true">⌕</span><input data-friend-search type="search" placeholder="搜索已添加好友" autocomplete="off"></label>
            <div class="friend-summary"><span>已确认的游戏好友</span><strong data-friend-summary>0 位</strong></div>
            <div data-friends class="friend-list"><p class="empty-state">登录后加载好友列表</p></div>
            <div class="friend-panel-footer"><button type="button" class="secondary" data-previous-friend>上一位</button><button type="button" class="secondary" data-visit-friend>下一位机会</button><button type="button" class="invite-button" data-share>邀请好友加入</button></div>
          </section>
        </div>
        <div class="toast" data-toast role="status" aria-live="polite" hidden></div>
      </div>`;
    this.statusElement = this.root.querySelector('[data-status]')!;
    this.farmElement = this.root.querySelector('[data-farm]')!;
    this.privateElement = this.root.querySelector('[data-private]')!;
    this.pendingElement = this.root.querySelector('[data-pending]')!;
    this.messageElement = this.root.querySelector('[data-message]')!;
    this.toastElement = this.root.querySelector('[data-toast]')!;
    this.farmTitleElement = this.root.querySelector('[data-farm-title]')!;
    this.farmSubtitleElement = this.root.querySelector('[data-farm-subtitle]')!;
    this.harvestAllButton = this.root.querySelector('[data-harvest-all]')!;
    this.careAllButton = this.root.querySelector('[data-care-all]')!;
    this.stealAllButton = this.root.querySelector('[data-steal-all]')!;
    this.harvestAllButton.addEventListener('click', () => void this.harvestAll());
    this.careAllButton.addEventListener('click', () => void this.careAll());
    this.stealAllButton.addEventListener('click', () => void this.stealAll());
    this.theftButton = this.root.querySelector<HTMLButtonElement>('[data-theft]')!;
    this.authScreen = this.root.querySelector('[data-auth-screen]')!;
    this.authMessageElement = this.root.querySelector('[data-auth-message]')!;
    this.loginButton = this.root.querySelector<HTMLButtonElement>('[data-login]')!;
    this.registerButton = this.root.querySelector<HTMLButtonElement>('[data-register]');
    this.webUsernameInput = this.root.querySelector<HTMLInputElement>('[data-web-username]');
    this.webPasswordInput = this.root.querySelector<HTMLInputElement>('[data-web-password]');
    this.stagingAccountSelect = this.root.querySelector<HTMLSelectElement>('[data-staging-account]');
    this.shareButtons = [...this.root.querySelectorAll<HTMLButtonElement>('[data-share]')];
    this.friendVisitButton = this.root.querySelector<HTMLButtonElement>('[data-visit-friend]')!;
    this.friendsElement = this.root.querySelector<HTMLElement>('[data-friends]')!;
    this.friendsModal = this.root.querySelector<HTMLElement>('[data-friends-modal]')!;
    this.friendSearchInput = this.root.querySelector<HTMLInputElement>('[data-friend-search]')!;
    this.utilitySheet = this.root.querySelector<HTMLElement>('[data-utility-sheet]')!;
    this.utilityTitleElement = this.root.querySelector<HTMLElement>('[data-utility-title]')!;
    this.utilityContentElement = this.root.querySelector<HTMLElement>('[data-utility-content]')!;
    this.playerNameElement = this.root.querySelector<HTMLElement>('[data-player-name]')!;
    this.stagingAccountSelect?.addEventListener('change', () => this.persistStagingAccount());

    this.store.subscribe(() => this.render());
    window.addEventListener('focus', () => void this.refreshFriendRosterSilently());

    // Tool selection: click a tool button to activate that mode, then click a plot.
    this.root.querySelectorAll<HTMLButtonElement>('[data-tool]').forEach((btn) => {
      btn.addEventListener('click', (event) => {
        // A successful pointer drag still produces a click on the seed button
        // in some browsers. Do not let that click immediately toggle plant
        // mode back off after planting.
        if (btn.dataset.seedDrag !== undefined && this.ignoreNextSeedClick) {
          event.preventDefault();
          this.ignoreNextSeedClick = false;
          return;
        }
        this.closeUtility();
        this.pendingTheftConfirmation = null;
        const tool = btn.dataset.tool as ToolMode;
        if (this.activeTool === tool) {
          this.activeTool = null;
        } else {
          this.activeTool = tool;
        }
        this.root.querySelectorAll<HTMLButtonElement>('[data-tool]').forEach((b) => b.classList.toggle('active', b.dataset.tool === this.activeTool));
        const hint = this.root.querySelector<HTMLElement>('[data-tool-hint]');
        if (hint) hint.textContent = this.activeTool ? TOOL_HINTS[this.activeTool] : '选择工具后点击地块操作';
        this.render();
      });
    });
    this.root.querySelector('[data-seed-quick]')?.addEventListener('click', (event) => {
      const target = (event.target as HTMLElement).closest<HTMLButtonElement>('[data-quick-seed]');
      if (!target) return;
      const crop = cropById(Number(target.dataset.quickSeed));
      this.activeCropDefinitionId = crop.id;
      this.activeTool = 'plant';
      this.closeUtility();
      this.showMessage(`已选择${crop.name}种子，点击或拖到已解锁空地播种`);
      this.render();
    });
    const seedDragButton = this.root.querySelector<HTMLButtonElement>('[data-seed-drag]');
    seedDragButton?.addEventListener('pointerdown', (event) => {
      if (event.button !== 0 || !this.ensurePersonalFarm()) return;
      this.seedPointerId = event.pointerId;
      this.seedPointerStart = { x: event.clientX, y: event.clientY };
      this.seedDragging = false;
      this.clearSeedDropTarget();
      seedDragButton.setPointerCapture?.(event.pointerId);
    });
    window.addEventListener('pointermove', (event) => this.moveSeedDrag(event), { passive: false });
    window.addEventListener('pointerup', (event) => this.finishSeedDrag(event));
    window.addEventListener('pointercancel', (event) => this.finishSeedDrag(event, true));

    this.theftButton.addEventListener('click', () => {
      this.closeUtility();
      void this.beginTheft();
    });
    this.root.querySelectorAll<HTMLElement>('[data-open-utility]').forEach((button) => {
      button.addEventListener('click', () => this.toggleUtility(button.dataset.openUtility as Exclude<UtilityMode, null>));
    });
    this.root.querySelector('[data-close-utility]')?.addEventListener('click', () => this.closeUtility());
    this.root.querySelectorAll<HTMLElement>('[data-open-friends]').forEach((button) => {
      button.addEventListener('click', () => this.openFriends());
    });
    this.friendsModal.querySelectorAll<HTMLElement>('[data-close-friends]').forEach((button) => {
      button.addEventListener('click', () => this.closeFriends());
    });
    this.friendSearchInput.addEventListener('input', () => {
      this.friendSearch = this.friendSearchInput.value.trim().toLocaleLowerCase();
      this.renderFriends();
    });
    this.utilityContentElement.addEventListener('input', (event) => {
      const input = (event.target as HTMLElement).closest<HTMLInputElement>('[data-shop-count]');
      if (!input) return;
      const count = Number.parseInt(input.value, 10);
      if (Number.isFinite(count) && count >= 1) this.shopCount = Math.min(count, 99);
    });
    this.utilityContentElement.addEventListener('click', (event) => {
      const target = event.target as HTMLElement;
      const buy = target.closest<HTMLElement>('[data-buy-offer]');
      if (buy?.dataset.buyOffer) {
        const crop = cropByOfferId(Number(buy.dataset.buyOffer));
        if (crop) {
          this.activeCropDefinitionId = crop.id;
          // BuyRequest.count 由服务端原子裁决;数量为商店面板共享输入(≥1)。
          const count = this.readCountInput(this.utilityContentElement, '[data-shop-count]', 99);
          this.shopCount = count;
          void this.execute('BUY', { offerId: crop.offerId, count, expectedOfferVersion: crop.offerVersion });
        }
      }
      const sell = target.closest<HTMLElement>('[data-sell-item]');
      if (sell?.dataset.sellItem) {
        // SellRequest.count 取该行数量输入,钳制在 [1, 持有量]。
        const row = sell.closest<HTMLElement>('.inventory-item');
        const count = this.readCountInput(row ?? this.utilityContentElement, '[data-sell-count]', 999);
        void this.execute('SELL', { itemId: Number(sell.dataset.sellItem), count });
      }
      if (target.closest('[data-sell-all]')) void this.sellAllCrops();
      const selectedCrop = target.closest<HTMLElement>('[data-select-crop]');
      if (selectedCrop?.dataset.selectCrop) {
        const crop = cropById(Number(selectedCrop.dataset.selectCrop));
        this.activeCropDefinitionId = crop.id;
        this.activeTool = 'plant';
        this.closeUtility();
        this.showMessage(`已选择${crop.name}种子，点击已解锁空地播种`);
        this.render();
      }
      const tab = target.closest<HTMLElement>('[data-inventory-tab]')?.dataset.inventoryTab;
      if (tab === 'crops' || tab === 'tools') {
        this.inventoryTab = tab;
        this.render();
      }
      if (target.closest('[data-reconnect]')) this.ws.simulateDisconnect();
      if (target.closest('[data-change-password]')) void this.changeWebPassword();
      if (target.closest('[data-logout-all]')) void this.logoutAllWebSessions();
      const claim = target.closest<HTMLButtonElement>('[data-claim-task]');
      if (claim?.dataset.claimTask) void this.claimTask(claim.dataset.claimTask);
    });
    this.root.querySelectorAll('[data-my-farm]').forEach((button) => button.addEventListener('click', () => this.visitOwner(this.ownOwnerId)));
    this.root.querySelector('[data-visit-friend]')?.addEventListener('click', () => void this.visitNextFriendOpportunity());
    this.root.querySelector('[data-previous-friend]')?.addEventListener('click', () => this.visitPreviousFriend());
    this.friendsElement.addEventListener('click', (event) => {
      const target = event.target as HTMLElement;
      const revoke = target.closest<HTMLButtonElement>('[data-revoke-owner]');
      if (revoke?.dataset.revokeOwner) {
        void this.revokeFriend(revoke.dataset.revokeOwner);
        return;
      }
      const friend = target.closest<HTMLButtonElement>('button[data-friend-owner]');
      if (friend?.dataset.friendOwner) this.visitOwner(friend.dataset.friendOwner);
      const retry = target.closest<HTMLButtonElement>('[data-retry-friends]');
      if (retry) void this.loadFriendRoster();
      const more = target.closest<HTMLButtonElement>('[data-load-more-friends]');
      if (more) void this.loadFriendRoster(false, true);
    });
    this.shareButtons.forEach((button) => button.addEventListener('click', () => void this.shareFarm()));
    window.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') return;
      this.closeFriends();
      this.closeUtility();
    });

    // Plot click: with active tool, execute that action on the clicked plot.
    this.farmElement.addEventListener('click', (event) => {
      const unlockButton = (event.target as HTMLElement).closest<HTMLButtonElement>('[data-unlock-plot]');
      if (unlockButton?.dataset.unlockPlot) {
        event.preventDefault();
        event.stopPropagation();
        this.selectedPlotId = unlockButton.dataset.unlockPlot;
        void this.unlockPlotOnPlot(unlockButton.dataset.unlockPlot);
        return;
      }
      const plot = (event.target as HTMLElement).closest<HTMLElement>('[data-plot-id]');
      if (!plot?.dataset.plotId) return;
      const plotId = plot.dataset.plotId;
      this.selectedPlotId = plotId;
      this.render();

      if (this.activeTool === 'plant') void this.plantOnPlot(plotId);
      else if (this.activeTool === 'water') void this.waterOnPlot(plotId);
      else if (this.activeTool === 'harvest') void this.harvestOnPlot(plotId);
    });

    this.dragGhost = document.createElement('div');
    this.dragGhost.className = 'drag-ghost';
    this.dragGhost.setAttribute('hidden', '');
    document.body.appendChild(this.dragGhost);

    this.loginButton.addEventListener('click', () => void this.login('login'));
    this.registerButton?.addEventListener('click', () => void this.login('register'));
    this.webPasswordInput?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') void this.login('login');
    });
    if (clientEnvironment.name === 'web') {
      window.addEventListener('storage', this.handleWebIdentityChange);
    }
    // Farm maturity is trusted-time derived and does not create a mutation
    // push. A bounded authority pull lets a visible client discover newly
    // mature friend crops without deriving eligibility locally.
    window.setInterval(
      () => this.pollFriendRosterOpportunities(),
      FRIEND_ROSTER_FALLBACK_POLL_MS,
    );
    // Q1:浏览器从后台恢复时立即重算展示阶段(后台 Timer 可能被节流)。
    document.addEventListener('visibilitychange', this.onVisibilityVisible);
  }

  async start(): Promise<void> {
    this.setAuthMessage(this.pendingShareToken
      ? '检测到好友分享入口，登录后会由服务端验证分享权限。'
      : '登录后将获取一次性测试 connection_ticket。');
  }

  private moveSeedDrag(event: PointerEvent): void {
    if (event.pointerId !== this.seedPointerId || !this.seedPointerStart) return;

    if (!this.seedDragging) {
      const moved = Math.hypot(event.clientX - this.seedPointerStart.x, event.clientY - this.seedPointerStart.y);
      // Keep a normal tap available for the existing “select seed tool” flow.
      if (moved < 8) return;
      this.seedDragging = true;
      this.closeUtility();
      this.activeTool = 'plant';
      this.root.querySelectorAll<HTMLButtonElement>('[data-tool]').forEach((button) => {
        button.classList.toggle('active', button.dataset.tool === 'plant');
      });
      this.root.querySelector<HTMLElement>('[data-seed-drag]')?.classList.add('dragging');
      const hint = this.root.querySelector<HTMLElement>('[data-tool-hint]');
      if (hint) hint.textContent = '把种子放到一块已解锁空闲绿地后松开播种';
      this.showDragGhost('plant', event.clientX, event.clientY);
    }

    event.preventDefault();
    this.moveDragGhost(event.clientX, event.clientY);
    this.setSeedDropTarget(this.emptyPlotAt(event.clientX, event.clientY));
  }

  private finishSeedDrag(event: PointerEvent, cancelled = false): void {
    if (event.pointerId !== this.seedPointerId) return;

    const wasDragging = this.seedDragging;
    const target = this.seedDropTarget;
    const seedButton = this.root.querySelector<HTMLButtonElement>('[data-seed-drag]');
    if (seedButton?.hasPointerCapture?.(event.pointerId)) seedButton.releasePointerCapture(event.pointerId);
    this.seedPointerId = null;
    this.seedPointerStart = null;
    this.seedDragging = false;
    seedButton?.classList.remove('dragging');
    this.clearSeedDropTarget();
    this.hideDragGhost();

    if (!wasDragging) return;
    // Some engines emit a click on the source button after pointerup. Consume
    // only that synthetic click, then retain the usual click-to-select path.
    this.ignoreNextSeedClick = true;
    window.setTimeout(() => { this.ignoreNextSeedClick = false; }, 0);

    if (cancelled) return;
    const plotId = target?.dataset.plotId;
    if (!plotId) {
      this.showMessage('请把种子放到一块空闲绿地后再松开', true);
      return;
    }
    this.selectedPlotId = plotId;
    void this.plantOnPlot(plotId);
  }

  private emptyPlotAt(clientX: number, clientY: number): HTMLElement | null {
    const element = document.elementFromPoint(clientX, clientY);
    const plot = element?.closest<HTMLElement>('[data-plot-id]');
    return plot?.dataset.plantable === 'true' ? plot : null;
  }

  private showDragGhost(tool: ToolMode, x: number, y: number): void {
    if (!this.dragGhost) return;
    this.dragGhost.dataset.tool = tool;
    this.dragGhost.innerHTML = tool === 'plant' ? '<span>🥕</span>' : tool === 'water' ? '<span>💧</span>' : '<span>🧤</span>';
    this.dragGhost.removeAttribute('hidden');
    this.moveDragGhost(x, y);
  }

  private moveDragGhost(x: number, y: number): void {
    if (!this.dragGhost) return;
    this.dragGhost.style.transform = `translate(${x + 12}px, ${y + 12}px)`;
  }

  private hideDragGhost(): void {
    this.dragGhost?.setAttribute('hidden', '');
  }

  private setSeedDropTarget(target: HTMLElement | null): void {
    if (this.seedDropTarget === target) return;
    this.clearSeedDropTarget();
    this.seedDropTarget = target;
    target?.classList.add('drop-target');
  }

  private clearSeedDropTarget(): void {
    this.seedDropTarget?.classList.remove('drop-target');
    this.seedDropTarget = null;
  }

  private async login(webMode: WebAccountMode = 'login'): Promise<void> {
    if (clientEnvironment.name === 'web') {
      const username = this.webUsernameInput?.value.trim() ?? '';
      const password = this.webPasswordInput?.value ?? '';
      if (!/^[a-zA-Z0-9_]{3,24}$/.test(username)) {
        this.setAuthMessage('账号需要使用 3–24 位字母、数字或下划线。', true);
        this.webUsernameInput?.focus();
        return;
      }
      if ([...password].length < 8 || [...password].length > 128) {
        this.setAuthMessage('密码长度需要为 8–128 个字符。', true);
        this.webPasswordInput?.focus();
        return;
      }
    }
    this.loginButton.disabled = true;
    if (this.registerButton) this.registerButton.disabled = true;
    this.loginButton.textContent = webMode === 'register' ? '注册中…' : '登录中…';
    this.setAuthMessage(webMode === 'register' ? '正在创建账号并初始化农场…' : '正在验证身份并建立连接…');
    this.authScreen.setAttribute('hidden', '');
    this.root.querySelector<HTMLElement>('[data-app-shell]')?.removeAttribute('hidden');

    try {
      await this.initializeRuntime();
      if (clientEnvironment.name === 'web') {
        (this.auth as WebAuthProvider).prepareCredentials(
          webMode,
          this.webUsernameInput?.value ?? '',
          this.webPasswordInput?.value ?? '',
        );
      }
      await this.ws.connect();
    } catch (error) {
      this.root.querySelector<HTMLElement>('[data-app-shell]')?.setAttribute('hidden', '');
      this.authScreen.removeAttribute('hidden');
      const requiresFreshLaunchTicket = clientEnvironment.name === 'wechat'
        && (this.auth as { requiresFreshLaunchTicket?: boolean } | undefined)?.requiresFreshLaunchTicket === true;
      this.loginButton.disabled = requiresFreshLaunchTicket;
      if (this.registerButton) this.registerButton.disabled = false;
      this.loginButton.textContent = requiresFreshLaunchTicket ? '返回小程序重新进入' : '重新登录';
      const detail = error instanceof Error ? error.message : '登录失败，请重试.';
      const userFacingDetail = requiresFreshLaunchTicket
        ? '当前页面是微信承载模式，但没有收到小程序交接的 launch_ticket。请从微信小程序的 web-view 重新进入；电脑联调请使用 Web 或 Staging 模式。'
        : detail;
      this.setAuthMessage(
        userFacingDetail,
        true,
      );
    }
  }

  private async initializeRuntime(): Promise<void> {
    if (this.commandService) return;

    this.friendService = new HttpGameFriendService(clientEnvironment.apiBase);
    this.shareResolver = this.friendService;
    this.shareCreator = new HttpShareLinkCreator(clientEnvironment.apiBase);
    if (!__FARM_SHOWCASE__ && clientEnvironment.name === 'staging') {
      const { StagingAuthProvider } = await import('../auth/staging-auth-provider');
      const account = this.stagingAccountSelect?.value === 'player-b' ? 'player-b' : 'player-a';
      const platformToken = account === 'player-b'
        ? clientEnvironment.demoSecondaryPlatformToken
        : clientEnvironment.demoPlatformToken;
      if (!platformToken) throw new Error(`Staging 测试账号 ${account} 尚未配置平台票据`);
      this.persistStagingAccount();
      this.auth = new StagingAuthProvider(
        `${clientEnvironment.apiBase}/v1/auth/login`,
        platformToken,
      );
    } else if (clientEnvironment.name === 'wechat') {
      const { WeChatAuthProvider } = await import('../auth/wechat-auth-provider');
      this.auth = new WeChatAuthProvider(
        `${clientEnvironment.apiBase}/v1/auth/exchange`,
        undefined,
        `${clientEnvironment.apiBase}/v1/auth/renew`,
      );
    } else if (clientEnvironment.name === 'web') {
      this.auth = new WebAuthProvider(`${clientEnvironment.apiBase}/v1/session/login`, {
        registerEndpoint: `${clientEnvironment.apiBase}/v1/accounts/register`,
        renewEndpoint: `${clientEnvironment.apiBase}/v1/auth/renew`,
        changePasswordEndpoint: `${clientEnvironment.apiBase}/v1/accounts/change-password`,
        logoutAllEndpoint: `${clientEnvironment.apiBase}/v1/auth/logout-all`,
      });
    }

    this.ws = new WsClient(this.auth, (ticket) => {
      // Explicit local override is useful for SSH/port-forward development.
      // Deployed builds normally use farm-gate's server-issued endpoint.
      return new BrowserWebSocketTransport(resolveWebSocketUrl(
        clientEnvironment.wsUrl ?? ticket.wsUrl ?? '/ws',
      ));
    }, {
      status: (status) => {
        this.statusElement.textContent = status;
        this.root.querySelector('.status-dot')?.classList.toggle('ready', status === 'InFarm');
        if (status === 'Reconnecting') {
          // Only the first transport loss captures navigation intent. During a
          // failed recovery attempt currentOwnerId temporarily becomes home;
          // overwriting here would lose the original friend-farm destination.
          if (!this.recoveryCycleActive) {
            this.recoveryTargetOwnerId = this.currentOwnerId;
            this.recoveryCycleActive = true;
          }
          this.recoveryOwnSnapshotReady = false;
          this.recoveryPendingInitialPassComplete = false;
          this.homeReady = false;
          this.root.dataset.homeReady = 'false';
        }
        this.render();
      },
      frame: (frame) => void this.handleFrame(frame),
      error: (error) => this.showMessage(error.message, true),
      recovery: (state) => {
        if (state.phase !== 'Reconnecting') return;
        this.statusElement.textContent = state.offline
          ? `Reconnecting · 等待网络恢复`
          : `Reconnecting · 第 ${state.attempt} 次${state.nextRetryMs === undefined ? '' : `，${state.nextRetryMs}ms 后重试`}`;
      },
    }, finalProtocolAdapter, {
      manualRecoveryBarrier: true,
    });
    this.commandService = new CommandService(this.ws, this.store, this.journal, clientEnvironment.name, {
      events: {
        reauthenticate: (message) => this.returnToAuth(message),
        beforeSend: (operationId, payload) => {
          if (payload.type === 'THEFT') {
            this.theftOperationTargets.set(operationId, {
              ownerId: String(payload.targetFarmId), plotId: payload.plotId, cropCycleId: payload.cropCycleId,
            });
          }
          if (payload.type !== 'CARE') return;
          const baselineRevision = String(this.store.getPrivateState().playerStateRevision ?? '0');
          this.careRewardRefresh.begin(operationId, baselineRevision, this.recoveryBootstrapGeneration, false);
          this.careRewardStatus = 'PENDING';
          this.render();
        },
      },
    });
    this.commandService.subscribe(() => this.render());
  }

  private async handleFrame(frame: ServerFrame): Promise<void> {
    if (frame.kind === 'hello') {
      const bootstrapGeneration = ++this.recoveryBootstrapGeneration;
      const previousTarget = this.recoveryTargetOwnerId || this.currentOwnerId;
      this.ownOwnerId = frame.value.playerId;
      if (clientEnvironment.name === 'web') {
        try {
          window.localStorage.setItem(WEB_ACTIVE_PLAYER_STORAGE_KEY, this.ownOwnerId);
        } catch {
          // HttpOnly Cookie remains authoritative when storage is unavailable.
        }
      }
      // 受信服务端时钟:以 ServerHello 的 server_time_ms 建锚点
      // (Q1 CONTRACT_MISMATCH 修正:成熟展示用 trusted clock 派生,不用 Date.now)。
      this.trustedClock.sync(frame.value.serverTimeMs);
      const ticket = this.ws.getConnectionTicket();
      if (ticket) this.commandService?.setConnectionTicket(ticket);
      this.recoveryPendingInitialPassComplete = false;
      this.recoveryPendingStarted = false;
      this.bootstrapOutcome = 'pending';
      this.bootstrapRecovery.reset(bootstrapGeneration);
      this.playerBootstrapRefresh.reset(bootstrapGeneration);
      const recoveringCare = this.careRewardRefresh.rebindConnectionGeneration(bootstrapGeneration);
      if (!recoveringCare) {
        this.careRewardRefresh.reset();
        this.careRewardStatus = 'IDLE';
      } else {
        this.careRewardStatus = 'PENDING';
      }
      this.clearCareRewardRefreshTimer();
      this.bootstrapRefreshSent = 0;
      this.bootstrapRefreshApplied = 0;
      this.clearPlayerBootstrapTimer();
      this.clearPlayerBootstrapRefreshTimer();
      this.pendingNavigation = null;
      this.friendOpportunityFlow.reset();
      this.allowPrivateSnapshot = true;
      this.homeReady = false;
      this.root.dataset.homeReady = 'false';
      this.friendOpportunityState = 'loading';
      this.recoveryTargetOwnerId =
        previousTarget !== this.ownOwnerId && this.friendIds.has(previousTarget)
          ? previousTarget
          : this.ownOwnerId;
      this.recoveryOwnSnapshotReady = false;
      // Contract 26 starts private Bootstrap, pending-result recovery and the
      // own-farm Join independently. PrivateState carries economy_version, so
      // a late Bootstrap cannot overwrite a newer operation result.
      this.requestPlayerBootstrap(bootstrapGeneration);
      this.startPendingRecovery(bootstrapGeneration);
      this.ws.joinFarm(this.ownOwnerId);
      void this.loadFriendRoster();
      void this.loadFriendOpportunity(bootstrapGeneration);
      void this.preparePendingShare().then(() => {
        if (bootstrapGeneration !== this.recoveryBootstrapGeneration || this.ws.status === 'Disconnected') return;
        if (this.pendingShareOwnerId && this.friendIds.has(this.pendingShareOwnerId)) {
          this.recoveryTargetOwnerId = this.pendingShareOwnerId;
          // Accept may finish after Snapshot but before the own subscription
          // reaches Ready/ClientRecoveryComplete. Never bypass that barrier.
          // If it is not open yet, later own SubscriptionReady will retry.
          if (!this.tryNavigatePendingShare()) this.tryCompleteRecovery();
        }
      });
      return;
    }
    if (frame.kind === 'snapshot') {
      // Only switch the viewed farm when the snapshot matches the latest join
      // intent. Stale/subscribed friend-farm snapshots must not yank the user
      // back after they navigated home (title stuck on "好友农场 · …").
      if (frame.value.ownerId === this.ws.activeJoinOwnerId) {
        this.currentOwnerId = frame.value.ownerId;
      }
      this.selectedPlotId = null;
      this.pendingTheftConfirmation = null;
      this.store.applySnapshot(frame.value);
      this.tryCompleteUnlockProjectionSync();
      if (frame.value.ownerId === this.ownOwnerId) {
        if (frame.value.privateStateIncluded !== false && this.allowPrivateSnapshot) {
          // Test-only snapshot adapters may still include private state. The
          // production canonical protocol obtains it from PlayerBootstrap.
          this.store.setInitialState(frame.value.state);
        }
        this.recoveryOwnSnapshotReady = true;
      }
      this.showMessage(`${frame.value.ownerId === this.ownOwnerId ? '我的农场' : '好友农场'}已加载，当前状态来自服务端 Snapshot`);
      // Gate accepts ClientRecoveryComplete only while the authenticated
      // owner's subscription is active. Target-farm restoration happens after
      // that marker, so every Snapshot may advance the local predicate but a
      // friend Snapshot can never satisfy its onOwnFarm condition.
      this.tryCompleteRecovery();
      return;
    }
    if (frame.kind === 'subscription-ready') {
      const refreshOpportunityAfterHome = this.friendOpportunityFlow.onSubscriptionReady(
        frame.value.ownerId,
        this.ownOwnerId,
      );
      if (frame.value.ownerId !== this.ownOwnerId) {
        const currentHistory = this.friendVisitHistory[this.friendVisitHistoryIndex];
        if (currentHistory !== frame.value.ownerId) {
          this.friendVisitHistory.splice(this.friendVisitHistoryIndex + 1);
          this.friendVisitHistory.push(frame.value.ownerId);
          this.friendVisitHistoryIndex = this.friendVisitHistory.length - 1;
        }
      }
      // Snapshot is only the baseline. Recovery navigation and writes remain
      // fenced until Ready proves that all catch-up versions were applied.
      // The own-farm Ready is only one input to the recovery barrier. Do not
      // Join the remembered friend yet: Gate requires ClientRecoveryComplete
      // to linearize while the own-farm subscription is still current.
      if (frame.value.ownerId !== this.ownOwnerId) {
        void this.confirmReadyFriendVisit(frame.value.ownerId);
        // Exact per-viewer Theft targets are roster authority data, not part of
        // FarmSnapshot. Revoke stale targets immediately, then refresh the page
        // that actually contains this owner after the legal Ready fence.
        const authority = this.friendAuthority.get(frame.value.ownerId);
        if (authority) {
          this.friendAuthority.set(frame.value.ownerId, {
            ...authority, actionMask: authority.actionMask & FriendActionMask.VISIT,
            status: 'UNKNOWN', theftEligibleTargets: [],
          });
          this.render();
        }
        void this.refreshReadyFriendAuthority(frame.value.ownerId);
      }
      if (
        this.recoveryCycleActive
        && frame.value.ownerId === this.recoveryTargetOwnerId
        && this.ws.status === 'InFarm'
      ) {
        this.recoveryCycleActive = false;
        this.showMessage(`${frame.value.ownerId === this.ownOwnerId ? '我的农场' : '好友农场'}断线恢复完成`);
      }
      // 拜访好友后回到自己农场(Ready 已确认 catch-up 消费完):重新拉取下
      // 一好友机会,而不是停留在登录时的旧机会上。
      if (
        frame.value.ownerId === this.ownOwnerId
        && refreshOpportunityAfterHome
        && this.ws.status === 'InFarm'
      ) {
        void this.loadFriendOpportunity(this.recoveryBootstrapGeneration, 0);
      }
      // Initial share acceptance can complete while the own-farm Join is
      // still catching up. This Ready edge is the first safe direct-nav point;
      // recovery cycles remain owned by tryCompleteRecovery below.
      if (frame.value.ownerId === this.ownOwnerId) this.tryNavigatePendingShare();
      this.tryCompleteRecovery();
      return;
    }
    if (frame.kind === 'change') {
      const result = this.store.applyChange(frame.value);
      if (result === 'applied' || result === 'duplicate') this.tryCompleteUnlockProjectionSync();
      if (result === 'gap') {
        // Fresh Join only for the farm we intentionally joined. A gap on a
        // just-left friend farm must not reset activeJoinIntent before the
        // home-farm snapshot arrives (race with visitOwner(own)).
        const joinOwner = this.ws.activeJoinOwnerId ?? this.currentOwnerId;
        if (frame.value.ownerId === joinOwner) {
          this.showMessage('检测到 farm_version 缺口，正在 Fresh Join…', true);
          this.ws.joinFarm(frame.value.ownerId);
        }
      }
      return;
    }
    if (frame.kind === 'rejoin-required') {
      this.showMessage(`服务端要求重新同步（${frame.value.reason}），正在 Fresh Join…`, true);
      return;
    }
    if (frame.kind === 'friend-roster-changed') {
      await this.loadFriendRoster(true);
      if (frame.value.mutation === 'REVOKED' && this.currentOwnerId === frame.value.counterpartOwnerId) {
        this.visitOwner(this.ownOwnerId);
      }
      this.showToast(frame.value.mutation === 'ADDED'
        ? `已与玩家${frame.value.counterpartOwnerId}成为好友`
        : `与玩家${frame.value.counterpartOwnerId}的好友关系已解除`);
      return;
    }
    if (frame.kind === 'friend-opportunity-changed') {
      const opportunityRequestGeneration = this.friendOpportunityFlow.beginRequest();
      // Pull hint only: immediately revoke locally displayed interaction bits,
      // then refresh the formal page before enabling anything again.
      const current = this.friendAuthority.get(frame.value.targetOwnerId);
      if (current) {
        this.friendAuthority.set(frame.value.targetOwnerId, {
          ...current,
          actionMask: current.actionMask & FriendActionMask.VISIT,
          status: 'UNKNOWN',
          opportunityRevision: BigInt(frame.value.opportunityRevision),
          theftEligibleTargets: [],
        });
      }
      this.friendOpportunityState = 'loading';
      this.render();
      await this.loadFriendRoster(true, false, true);
      if (this.friendOpportunityFlow.isCurrentRequest(opportunityRequestGeneration)) {
        void this.loadFriendOpportunity(
          this.recoveryBootstrapGeneration,
          0,
          opportunityRequestGeneration,
        );
      }
      return;
    }
    if (frame.kind === 'kick') {
      this.returnToAuth(`${frame.value.message}。当前页面已停止自动重连；如需在这里继续，请手动重新登录。`);
      return;
    }
    if (frame.kind === 'drain') {
      this.ws.prepareForDrain(frame.value.reconnectAfterMs);
      this.showMessage(`服务正在滚动更新，将在约 ${frame.value.reconnectAfterMs}ms 后自动恢复连接`);
      return;
    }
    if (frame.kind !== 'result') return;
    if (this.handleCareBootstrapResult(frame.value)) return;
    if (this.handlePlayerBootstrapResult(frame.value)) return;
    if (this.handleNavigationResult(frame.value)) return;
    this.handleTaskResult(frame.value);
    this.handleUnlockCommandResult(frame.value);
    this.handleCareCommandResult(frame.value);
    this.handleTheftAuthorityResult(frame.value);
    await this.commandService?.handleResult(frame.value);
    if (frame.value.privateState) {
      // JOIN_FARM is intentionally not part of the durable CommandService
      // journal, so its accepted private bootstrap has no pending operation to
      // apply it. Applying here is also idempotent for ordinary write results.
      this.store.applyPrivateState(frame.value.privateState);
      this.allowPrivateSnapshot = false;
    }
    // PlayerRefresh §4/§1:资产相关终态成功后安全重查 Bootstrap 刷新权威私有状态。
    // BUY/SELL 结果已携带并成功应用完整 privateState(上方 applyPrivateState),不
    // 再额外制造一次查询;PLANT/HARVEST/THEFT 无完整 privateState,标 dirty 走
    // 独立 refresh 控制器(不与恢复共享预算)。Q1 预算分离后,恢复多重试也不会饿死
    // PLANT 后的刷新。
    if (
      (frame.value.status === 'SUCCEEDED'
        || (frame.value.commandType === 'UNLOCK_PLOT' && frame.value.status === 'NO_CHANGE'))
      && isAssetAffectingCommand(frame.value.commandType)
      && (!frame.value.privateState || frame.value.commandType === 'CLAIM_TASK')
    ) {
      this.playerBootstrapRefresh.markDirty();
      this.pumpPlayerBootstrapRefresh();
    }
    // 慢路径(PROCESSING → GET_OPERATION_RESULT 轮询 → COMPLETED):合同保证
    // PROCESSING 对应可查询的 operation,原请求之后不再主动推终态。COMPLETED
    // 时原命令终态已恢复,资产相关命令必须与快路径 SUCCEEDED 相同的 refresh
    // 闭环,否则 refreshSent 恒 0、seed 永不收敛(Q1 run21/22 问题 A)。
    const lookup = frame.value.operationLookup;
    if (
      lookup?.lookupStatus === 'COMPLETED'
      && (lookup.completedStatus === 'SUCCEEDED'
        || (lookup.originalCommandType === 'UNLOCK_PLOT' && lookup.completedStatus === 'NO_CHANGE'))
      && isAssetAffectingCommand(lookup.originalCommandType)
    ) {
      this.playerBootstrapRefresh.markDirty();
      this.pumpPlayerBootstrapRefresh();
    }
    // 公开错误面:REJECTED/UNAVAILABLE 展示 error code + request/operation id,
    // 并写入 dataset 供集成排查与 Playwright 断言。
    const isUnlockResult = frame.value.commandType === 'UNLOCK_PLOT'
      || frame.value.operationLookup?.originalCommandType === 'UNLOCK_PLOT';
    const isTheftResult = frame.value.commandType === 'THEFT'
      || frame.value.operationLookup?.originalCommandType === 'THEFT';
    if (frame.value.status === 'REJECTED' || frame.value.status === 'UNAVAILABLE') {
      if (!isUnlockResult && !isTheftResult) this.noteCommandError(frame.value);
      return;
    }
    if (frame.value.message && !isUnlockResult && !isTheftResult) this.showMessage(frame.value.message, false);
  }

  /** Render UNLOCK_PLOT lifecycle without treating a receipt as local state. */
  private handleUnlockCommandResult(result: CommandResult): void {
    let status = result.status;
    let operationId = result.operationId;
    let errorCode = result.errorCode;
    let errorCodeNumber = result.errorCodeNumber;
    let requestId = result.requestId;
    let message = result.message;
    let authoritativeResult: CommandResult | undefined = result.commandType === 'UNLOCK_PLOT' ? result : undefined;
    if (result.commandType !== 'UNLOCK_PLOT') {
      const lookup = result.operationLookup;
      if (lookup?.originalCommandType !== 'UNLOCK_PLOT') return;
      operationId = lookup.originalOperationId;
      requestId = result.requestId;
      if (lookup.lookupStatus === 'PROCESSING') {
        status = 'PROCESSING';
      } else if (lookup.lookupStatus === 'COMPLETED' && lookup.completedStatus) {
        status = lookup.completedStatus;
        errorCode = lookup.completedErrorCode;
        message = lookup.completedErrorMessage;
        authoritativeResult = lookup.completedResult;
      } else {
        return;
      }
    }
    const presentation = unlockCommandPresentation(status, {
      operationId, requestId, errorCode, errorCodeNumber, message,
    });
    if (!presentation) return;
    if (presentation.error) {
      const appShell = this.root.querySelector<HTMLElement>('[data-app-shell]');
      if (appShell) {
        appShell.dataset.lastErrorCode = errorCode ?? (errorCodeNumber ? String(errorCodeNumber) : '');
        appShell.dataset.lastErrorOp = operationId ?? '';
        appShell.dataset.lastErrorRequest = requestId ?? '';
      }
    }
    this.showMessage(presentation.message, presentation.error);
    if (
      !presentation.error
      && (status === 'SUCCEEDED' || status === 'NO_CHANGE')
      && authoritativeResult?.farmId
      && authoritativeResult.plotId !== undefined
      && authoritativeResult.committedFarmVersion !== undefined
    ) {
      this.pendingUnlockProjection = {
        ownerId: authoritativeResult.farmId,
        plotIndex: authoritativeResult.plotId,
        farmVersion: authoritativeResult.committedFarmVersion,
      };
      this.tryCompleteUnlockProjectionSync();
    }
  }

  private tryCompleteUnlockProjectionSync(): void {
    const pending = this.pendingUnlockProjection;
    if (!pending) return;
    const farm = this.store.onFarm(pending.ownerId);
    if (!farm || compareUint64(farm.farmVersion, pending.farmVersion) < 0) return;
    const purchased = farm.state.plots.find((plot) => plot.plotIndex === pending.plotIndex);
    if (!purchased || purchased.lockState !== 'UNLOCKED') return;
    const next = farm.state.plots.find((plot) => plot.lockState === 'LOCKED' && plot.unlockCandidate);
    this.pendingUnlockProjection = null;
    this.showMessage(next
      ? `权威农场状态已同步，可继续解锁第 ${next.unlockOrder} 块土地`
      : '权威农场状态已同步，土地已解锁');
  }

  private handleCareCommandResult(result: CommandResult): void {
    let operationId: string | undefined;
    let status: 'SUCCEEDED' | 'NO_CHANGE' | 'REJECTED' | undefined;
    let careResult = result.careResult;
    if (result.commandType === 'CARE') {
      operationId = result.operationId;
      status = result.status === 'SUCCEEDED' || result.status === 'NO_CHANGE' || result.status === 'REJECTED'
        ? result.status : undefined;
    } else if (result.operationLookup?.originalCommandType === 'CARE') {
      operationId = result.operationLookup.originalOperationId;
      const completed = result.operationLookup.completedStatus;
      status = completed === 'SUCCEEDED' || completed === 'NO_CHANGE' || completed === 'REJECTED' ? completed : undefined;
      careResult = result.operationLookup.completedResult?.careResult ?? careResult;
    }
    if (!operationId || !status || !this.careRewardRefresh.matches(operationId, this.recoveryBootstrapGeneration)) return;
    if (careRewardRefreshAction(status, careResult?.rewardObligationCreated) === 'CLEAR') {
      this.careRewardRefresh.reset();
      this.careRewardStatus = 'IDLE';
      this.clearCareRewardRefreshTimer();
      this.render();
      return;
    }
    this.careRewardRefresh.setRewardPending(operationId, this.recoveryBootstrapGeneration, true);
    this.careRewardStatus = 'PENDING';
    this.showMessage('护理已提交，奖励正在结算中（尚未到账）');
    if (status === 'SUCCEEDED' || status === 'NO_CHANGE') this.pumpCareRewardRefresh(0);
  }

  private handleCareBootstrapResult(result: CommandResult): boolean {
    const wait = this.careRewardRefresh.current;
    if (!wait?.inFlightBootstrapOperationId || result.operationId !== wait.inFlightBootstrapOperationId) return false;
    const claimed = this.careRewardRefresh.claimBootstrap(result.operationId, this.recoveryBootstrapGeneration);
    if (!claimed) return true;
    const revision = result.privateState?.playerStateRevision;
    if ((result.status === 'SUCCEEDED' || result.status === 'NO_CHANGE') && revision !== undefined) {
      // Apply the full authoritative progression/task summary first. Equal or
      // older revisions are deliberately not success: they remain in Pending.
      this.store.applyPlayerBootstrap(result.privateState);
      if (this.careRewardRefresh.settledByRevision(String(revision), this.recoveryBootstrapGeneration)) {
        this.careRewardStatus = 'SETTLED';
        this.clearCareRewardRefreshTimer();
        this.showMessage('护理状态已同步，奖励结算完成');
        this.render();
        return true;
      }
    }
    this.scheduleCareRewardRefresh(this.recoveryBootstrapGeneration);
    return true;
  }

  private pumpCareRewardRefresh(delayMs: number): void {
    const generation = this.recoveryBootstrapGeneration;
    if (!this.careRewardRefresh.pending) return;
    this.clearCareRewardRefreshTimer();
    if (delayMs > 0) {
      this.careRewardRefreshTimer = window.setTimeout(() => {
        this.careRewardRefreshTimer = undefined;
        this.pumpCareRewardRefresh(0);
      }, delayMs);
      return;
    }
    if (!this.careRewardRefresh.canStartBootstrap(generation)) {
      if (this.careRewardRefresh.exhaust(generation)) {
        this.careRewardStatus = 'EXHAUSTED';
        this.showMessage('农场变化已提交，奖励仍在结算/可重试', true);
        this.render();
      }
      return;
    }
    try {
      const bootstrapOperationId = this.ws.requestPlayerBootstrap(this.ownOwnerId);
      this.careRewardRefresh.startBootstrap(bootstrapOperationId, generation);
      this.render();
      const attempt = this.careRewardRefresh.current?.attempts ?? 1;
      this.careRewardRefreshTimer = window.setTimeout(() => {
        this.careRewardRefreshTimer = undefined;
        const current = this.careRewardRefresh.current;
        if (current?.inFlightBootstrapOperationId === bootstrapOperationId) {
          this.careRewardRefresh.claimBootstrap(bootstrapOperationId, generation);
          this.scheduleCareRewardRefresh(generation);
        }
      }, Math.min(1_500, 250 * 2 ** Math.max(0, attempt - 1)));
    } catch {
      this.scheduleCareRewardRefresh(generation);
    }
  }

  private scheduleCareRewardRefresh(generation: number): void {
    if (generation !== this.recoveryBootstrapGeneration || !this.careRewardRefresh.pending) return;
    const attempt = this.careRewardRefresh.current?.attempts ?? 0;
    this.pumpCareRewardRefresh(Math.min(1_500, 250 * 2 ** attempt));
  }

  private clearCareRewardRefreshTimer(): void {
    if (this.careRewardRefreshTimer !== undefined) {
      window.clearTimeout(this.careRewardRefreshTimer);
      this.careRewardRefreshTimer = undefined;
    }
  }

  /**
   * 把 JOIN_FARM 的 REJECTED 归因到发起时的导航意图:展示可诊断错误并自动
   * Join 回上一农场;不把整个应用退回登录,也不让用户卡在"未入园"状态。
   */
  private handleNavigationResult(result: CommandResult): boolean {
    const navigation = this.pendingNavigation;
    if (!navigation || !navigation.operationId || result.operationId !== navigation.operationId) return false;
    if (result.status === 'ACCEPTED' || result.status === 'PROCESSING') {
      // 准入成功(或旧链路中间态):导航意图已由 Join 生命周期接管。
      this.pendingNavigation = null;
      return false;
    }
    if (result.status !== 'REJECTED') return false;
    this.pendingNavigation = null;
    this.noteCommandError(result);
    // 回退链终点(自己农场也被拒)只保留错误面,不再自动重试,避免拒绝循环。
    if (navigation.targetOwnerId === this.ownOwnerId && navigation.previousOwnerId === this.ownOwnerId) {
      return true;
    }
    this.recoveryTargetOwnerId = navigation.previousOwnerId;
    this.showMessage(`无法进入目标农场：${formatCommandError(result)}；正在返回上一农场`, true);
    this.fallbackJoin(navigation.previousOwnerId);
    return true;
  }

  /** Join 回上一农场;若再失败,最终退回自己农场(错误面保持可诊断)。 */
  private fallbackJoin(ownerId: string): void {
    try {
      this.ws.joinFarm(ownerId);
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '返回上一农场失败', true);
      return;
    }
    this.pendingNavigation = {
      targetOwnerId: ownerId,
      previousOwnerId: this.ownOwnerId,
      operationId: this.ws.activeJoinOperationId ?? '',
    };
  }

  /** 公开错误面:页面消息 + data-app-shell dataset,供集成排查与测试断言。 */
  private noteCommandError(result: CommandResult): void {
    const appShell = this.root.querySelector<HTMLElement>('[data-app-shell]');
    if (appShell) {
      appShell.dataset.lastErrorCode = result.errorCode ?? (result.errorCodeNumber ? String(result.errorCodeNumber) : '');
      appShell.dataset.lastErrorOp = result.operationId ?? '';
      appShell.dataset.lastErrorRequest = result.requestId ?? '';
    }
    this.showMessage(formatCommandError(result), true);
  }

  private startPendingRecovery(bootstrapGeneration: number): void {
    if (this.recoveryPendingStarted) return;
    this.recoveryPendingStarted = true;
    const pendingRecovery = this.commandService?.recoverPending()
      ?? Promise.reject(new Error('未决操作恢复服务尚未初始化'));
    void pendingRecovery.then(() => {
      if (bootstrapGeneration !== this.recoveryBootstrapGeneration) return;
      this.recoveryPendingInitialPassComplete = true;
      this.tryCompleteRecovery();
    }).catch((error: unknown) => {
      if (bootstrapGeneration !== this.recoveryBootstrapGeneration) return;
      // Recovery is a write-admission fence. Loading or sending the first
      // reconciliation pass failed, so never fail open into InFarm.
      this.recoveryPendingInitialPassComplete = false;
      this.recoveryPendingStarted = false;
      this.showMessage(error instanceof Error ? error.message : '未决操作恢复暂时不可用', true);
      window.setTimeout(() => {
        if (
          bootstrapGeneration === this.recoveryBootstrapGeneration
          && this.ws.status !== 'Disconnected'
        ) this.startPendingRecovery(bootstrapGeneration);
      }, 1_000);
    });
  }

  private requestPlayerBootstrap(bootstrapGeneration: number): void {
    if (
      bootstrapGeneration !== this.recoveryBootstrapGeneration
      || this.bootstrapOutcome !== 'pending'
      || this.ws.status === 'Disconnected'
    ) return;
    if (!this.bootstrapRecovery.hasBudget()) {
      // 预算耗尽仍无权威终态(典型:旧后端把未注册命令静默丢弃或链路异常):
      // 记 unreachable 且不打开 RecoveryBarrier(A1 §1 fail closed),绝不伪造
      // 私有数据;资产读数保持未知,以首个权威 Buy/Sell private_state 回执为准。
      this.markBootstrapUnavailable(
        'unreachable',
        bootstrapGeneration,
        'GET_PLAYER_BOOTSTRAP 未获得权威应答(已达重试上限)，资产以经营回包为准',
      );
      return;
    }
    let operationId: string;
    try {
      operationId = this.ws.requestPlayerBootstrap(this.ownOwnerId);
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '玩家私有状态加载失败', true);
      this.markBootstrapUnavailable('unreachable', bootstrapGeneration, null);
      return;
    }
    this.bootstrapRecovery.trackRecovery(operationId, bootstrapGeneration);
    this.clearPlayerBootstrapTimer();
    const attempt = this.bootstrapRecovery.recoverySentCount;
    const delay = Math.min(5_000, 750 * (2 ** Math.min(attempt, 3)));
    this.recoveryPlayerBootstrapTimer = window.setTimeout(() => {
      this.recoveryPlayerBootstrapTimer = undefined;
      this.requestPlayerBootstrap(bootstrapGeneration);
    }, delay);
  }

  /** Bootstrap 被明确判定不可用:记终态、停重试(不打开 RecoveryBarrier)。 */
  private markBootstrapUnavailable(
    outcome: 'feature-disabled' | 'unreachable',
    bootstrapGeneration: number,
    message: string | null,
  ): void {
    if (bootstrapGeneration !== this.recoveryBootstrapGeneration || this.bootstrapOutcome !== 'pending') return;
    this.bootstrapOutcome = outcome;
    this.clearPlayerBootstrapTimer();
    if (message) this.showMessage(message, true);
    this.render();
    this.tryCompleteRecovery();
  }

  private handlePlayerBootstrapResult(result: CommandResult): boolean {
    // PlayerRefresh §2-3:按当前 recovery generation 认领 Bootstrap 响应并按用途
    // 分流。claim 即消费:收到终态后立即从集合移除,防 Set/Map 随会话无限增长。
    // 新 generation(新 ServerHello)的响应被 reset 后的集合隔离,旧 generation
    // 结果绝不能完成新恢复/更新资产。
    const purpose = this.bootstrapRecovery.claim(result.operationId, this.recoveryBootstrapGeneration);
    if (purpose === null) return false;
    if (purpose === 'refresh') {
      return this.handlePlayerBootstrapRefreshResult(result);
    }
    return this.handleMandatoryBootstrapResult(result);
  }

  /**
   * 首屏/重连 mandatory Bootstrap 结果。维持既有语义:严格 privateState 应用
   * 后才标 applied、打开 RecoveryBarrier;预算耗尽/fail closed 行为不变。
   */
  private handleMandatoryBootstrapResult(result: CommandResult): boolean {
    const outcome = classifyBootstrapResult(result);
    if (outcome === 'applied') {
      // PlayerRefresh §1:只有严格 privateState 存在且 store 成功应用(新 revision
      // 写入或幂等一致)后才标 applied;adapter 已挡 malformed success,缺失时保持
      // 屏障关闭(fail closed),绝不把无内容/被拒的响应当成私有状态可用。
      if (!result.privateState) return true;
      const applied = this.store.applyPlayerBootstrap(result.privateState);
      // Q1 / REVIEW P1-1:strict 检查与 store 应用判定通过后,才用 Bootstrap 的
      // trusted_server_now_ms 刷新受信时钟锚点(不用 Date.now)。malformed/旧
      // generation 响应(claim 已挡)不得更新时间锚点。trusted_server_now_ms
      // 不属于 Player Authority 内容 Hash:同 revision 但时间不同不得触发"同
      // revision 不同权威内容"拒绝(store 只比较 wallet 等权威内容)。
      if (result.trustedServerNowMs !== undefined) {
        this.trustedClock.sync(result.trustedServerNowMs);
      }
      this.clearPlayerBootstrapTimer();
      if (applied && this.bootstrapOutcome !== 'applied') {
        this.bootstrapOutcome = 'applied';
        this.render();
        this.tryCompleteRecovery();
      } else if (applied) {
        this.render();
      }
      return true;
    }
    if (this.bootstrapOutcome !== 'pending') return true;
    if (outcome === 'feature-disabled') {
      // GET_PLAYER_BOOTSTRAP 在当前后端基线 feature-disabled(1007):不伪造
      // 私有数据;屏障保持关闭(A1 §1),资产显示保持未知,
      // 以首个权威 Buy/Sell private_state 为准。
      this.markBootstrapUnavailable(
        'feature-disabled',
        this.recoveryBootstrapGeneration,
        '玩家资产查询尚未开放，资产以经营回包为准',
      );
      return true;
    }
    // 非终态(中间态或其他 REJECTED):交给退避重试,预算耗尽后按 unreachable 记终态。
    this.showMessage(result.message || '玩家私有状态暂时不可用，正在重试', true);
    return true;
  }

  /**
   * 资产 refresh 结果(PlayerRefresh §5)。成功:严格解码 → 按 revision 单调应用
   * → 刷新受信时钟 → succeed(期间又有资产命令则恰好再发一笔)。失败:绝不得把
   * 已 applied 的恢复屏障改回 pending/unreachable;保留 dirty 走独立重试。
   */
  private handlePlayerBootstrapRefreshResult(result: CommandResult): boolean {
    const outcome = classifyBootstrapResult(result);
    if (outcome === 'applied') {
      if (!result.privateState) {
        // 缺失私有状态:当作一次无害失败,保留 dirty 走重试,不关屏障。
        return true;
      }
      const applied = this.store.applyPlayerBootstrap(result.privateState);
      if (applied) {
        if (result.trustedServerNowMs !== undefined) {
          this.trustedClock.sync(result.trustedServerNowMs);
        }
        this.bootstrapRefreshApplied += 1;
        // 认领本次 in-flight 并成功收敛;期间又有资产命令则再发恰好一笔。
        const attempt = this.playerBootstrapRefresh.claim(
          result.operationId ?? '',
          this.recoveryBootstrapGeneration,
        );
        if (attempt) this.playerBootstrapRefresh.succeed(attempt);
        this.clearPlayerBootstrapRefreshTimer();
        this.render();
        if (this.playerBootstrapRefresh.needsAnotherRequest) {
          this.pumpPlayerBootstrapRefresh();
        }
      }
      return true;
    }
    // 非终态/被拒:refresh 失败无害——不关屏障、保留 dirty,按独立退避重试。
    this.showMessage(result.message || '资产状态刷新失败，稍后自动重试', true);
    if (this.playerBootstrapRefresh.pending) {
      const attempt = this.playerBootstrapRefresh.claim(
        result.operationId ?? '',
        this.recoveryBootstrapGeneration,
      );
      if (attempt) this.playerBootstrapRefresh.fail();
      this.bootstrapRecovery.forget(result.operationId, this.recoveryBootstrapGeneration);
    }
    this.pumpPlayerBootstrapRefresh();
    return true;
  }

  /**
   * 资产 refresh 控制器(PlayerRefresh §2/§4):不检查 recovery hasBudget,
   * 使用独立 PlayerBootstrapRefresh 的单飞 + dirty sequence + 有界重试。
   * GET_PLAYER_BOOTSTRAP 不要求 FarmSubscription 或 Recovery 完成,只要 WSS 已
   * 完成应用层认证(requestPlayerBootstrap 内部已挡 Disconnected)。
   */
  private pumpPlayerBootstrapRefresh(): void {
    const generation = this.recoveryBootstrapGeneration;
    if (!this.playerBootstrapRefresh.canStart(generation)) return;
    if (this.ws.status === 'Disconnected') return; // dirty 保留,状态恢复后再次 pump
    let operationId: string;
    try {
      operationId = this.ws.requestPlayerBootstrap(this.ownOwnerId);
    } catch {
      return; // 连接竞态:忽略,当前状态不受影响
    }
    if (!this.bootstrapRecovery.trackRefresh(operationId, generation)) return;
    let attempt;
    try {
      attempt = this.playerBootstrapRefresh.begin(operationId, generation);
    } catch {
      this.bootstrapRecovery.forget(operationId, generation);
      return;
    }
    this.bootstrapRefreshSent += 1;
    this.armPlayerBootstrapRefreshTimeout(attempt);
    // 诊断属性在 render 时写入 dataset;sent 递增后必须立即 render,否则
    // data-bootstrap-refresh-sent 停留在旧值(Playwright 断言读到的是 send 前
    // 的计数),掩盖"refresh 已发出"这一事实。
    this.render();
  }

  /** 单次 refresh 超时 3 秒;独立最多 3 次;退避 250ms / 750ms / 1500ms。 */
  private armPlayerBootstrapRefreshTimeout(attempt: import('./player-bootstrap-refresh').PlayerBootstrapRefreshAttempt): void {
    this.clearPlayerBootstrapRefreshTimer();
    const delay = Math.min(1_500, 250 * (2 ** Math.min(attempt.attempt - 1, 3)));
    this.playerBootstrapRefreshTimer = window.setTimeout(() => {
      this.playerBootstrapRefreshTimer = undefined;
      if (this.playerBootstrapRefresh.claim(attempt.operationId, attempt.connectionGeneration)) {
        this.playerBootstrapRefresh.fail();
      }
      this.bootstrapRecovery.forget(attempt.operationId, attempt.connectionGeneration);
      if (this.playerBootstrapRefresh.needsAnotherRequest) {
        this.pumpPlayerBootstrapRefresh();
      }
    }, 3_000 + delay);
  }

  private clearPlayerBootstrapRefreshTimer(): void {
    if (this.playerBootstrapRefreshTimer !== undefined) {
      window.clearTimeout(this.playerBootstrapRefreshTimer);
      this.playerBootstrapRefreshTimer = undefined;
    }
  }

  private clearPlayerBootstrapTimer(): void {
    if (this.recoveryPlayerBootstrapTimer !== undefined) {
      window.clearTimeout(this.recoveryPlayerBootstrapTimer);
      this.recoveryPlayerBootstrapTimer = undefined;
    }
  }

  private tryCompleteRecovery(): void {
    if (
      !recoveryBarrierSatisfied({
        wsRecovering: this.ws.status === 'Recovering',
        joinReady: this.ws.joinPhase === 'READY',
        ownSnapshotReady: this.recoveryOwnSnapshotReady,
        bootstrapOutcome: this.bootstrapOutcome,
        pendingFirstPassComplete: this.recoveryPendingInitialPassComplete,
        onOwnFarm: this.currentOwnerId === this.ownOwnerId,
      })
    ) return;
    const targetOwnerId = this.recoveryTargetOwnerId || this.ownOwnerId;
    try {
      this.ws.completeRecovery();
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '恢复完成标记发送失败，等待重连', true);
      return;
    }
    if (targetOwnerId !== this.ownOwnerId) {
      // completeRecovery synchronously opens InFarm. Only now is an ordinary
      // friend Join admissible: the Gate marker has already linearized on the
      // own-farm ActiveSubscription, while the desired target remains intact.
      this.pendingShareOwnerId = null;
      this.showMessage('私有状态与未决操作已恢复，正在返回断线前的好友农场…');
      try {
        this.ws.joinFarm(targetOwnerId);
        this.pendingNavigation = {
          targetOwnerId,
          previousOwnerId: this.ownOwnerId,
          operationId: this.ws.activeJoinOperationId ?? '',
        };
      } catch (error) {
        this.recoveryTargetOwnerId = this.ownOwnerId;
        this.recoveryCycleActive = false;
        this.showMessage(error instanceof Error ? error.message : '恢复目标农场失败', true);
      }
      return;
    }
    this.recoveryTargetOwnerId = this.ownOwnerId;
    this.recoveryCycleActive = false;
    this.showMessage('恢复完成：PlayerBootstrap、自家农场和未决操作首轮恢复均已就绪');
    this.tryCompleteHomeReady();
  }

  private async plantFirstEmpty(): Promise<void> {
    if (!this.ensurePersonalFarm()) return;
    const farm = this.store.onFarm(this.currentOwnerId);
    const plot = farm?.state.plots.find((candidate) => candidate.plotId === this.selectedPlotId && isPlantablePlot(candidate))
      ?? farm?.state.plots.find((candidate) => isPlantablePlot(candidate));
    if (!plot) return this.showMessage('没有空闲地块', true);
    await this.execute('PLANT', { plotId: plot.plotIndex }, plot.plotVersion);
  }

  private async harvestFirstMature(): Promise<void> {
    if (!this.ensurePersonalFarm()) return;
    const farm = this.store.onFarm(this.currentOwnerId);
    const trustedNow = this.serverNow();
    const matureCandidate = (candidate: PlotState): boolean =>
      candidate.lockState === 'UNLOCKED' && effectiveCropPhase(candidate, trustedNow) === 'MATURE';
    const plot = farm?.state.plots.find((candidate) => candidate.plotId === this.selectedPlotId && matureCandidate(candidate))
      ?? farm?.state.plots.find((candidate) => matureCandidate(candidate));
    if (!plot) return this.showMessage('暂时没有成熟作物', true);
    await this.execute('HARVEST', { plotId: plot.plotIndex }, plot.plotVersion, plot.cropCycleId);
  }

  private async waterFirstGrowing(): Promise<void> {
    const farm = this.store.onFarm(this.currentOwnerId);
    const trustedNow = this.serverNow();
    const plot = farm?.state.plots.find((candidate) => candidate.plotId === this.selectedPlotId && isWaterablePlot(candidate, trustedNow))
      ?? farm?.state.plots.find((candidate) => isWaterablePlot(candidate, trustedNow));
    if (!plot) return this.showMessage('当前没有需要浇水的作物', true);
    await this.execute('CARE', { plotId: plot.plotIndex }, plot.plotVersion, plot.cropCycleId);
  }

  // ---- Plot-level tool interactions ----

  private async plantOnPlot(plotId: string): Promise<void> {
    if (!this.ensurePersonalFarm()) return;
    const farm = this.store.onFarm(this.currentOwnerId);
    const plot = farm?.state.plots.find((p) => p.plotId === plotId);
    if (!plot) return this.showMessage('该地块不可播种', true);
    if (plot.lockState === 'UNSPECIFIED') return this.showMessage('土地状态未确认，暂不能播种', true);
    if (plot.lockState === 'LOCKED') return this.showMessage('这块土地已锁定，请先购买解锁', true);
    if (!isPlantablePlot(plot)) return this.showMessage('该地块不可播种', true);
    await this.execute('PLANT', { plotId: plot.plotIndex, cropDefinitionId: this.activeCropDefinitionId }, plot.plotVersion);
    this.activeTool = null;
    this.root.querySelectorAll('[data-tool]').forEach((b) => b.classList.remove('active'));
  }

  private async unlockPlotOnPlot(plotId: string): Promise<void> {
    if (!this.ensurePersonalFarm()) return;
    const farm = this.store.onFarm(this.currentOwnerId);
    const plot = farm?.state.plots.find((candidate) => candidate.plotId === plotId);
    if (!plot || plot.lockState !== 'LOCKED') {
      return this.showMessage('土地锁定状态已变化，请刷新农场后重试', true);
    }
    if (!plot.unlockCandidate) {
      return this.showMessage('请先解锁前一块土地', true);
    }
    const pending = (this.commandService?.getPending() ?? []).some((operation) =>
      operation.commandType === 'UNLOCK_PLOT'
      && operation.targetFarmId === this.currentOwnerId
      && operation.plotId === plot.plotIndex);
    if (pending) {
      return this.showMessage('土地解锁处理中', false);
    }
    const unlockPayload = unlockPlotPayload(plot, this.currentOwnerId, true);
    if (!unlockPayload || !canUnlockPlot(plot, true, this.ws.status === 'InFarm', pending)) {
      return this.showMessage('土地锁定状态已变化，请刷新农场后重试', true);
    }
    // Both expected versions are copied from the latest server PlotView. The
    // client deliberately does not submit level, price, or any qualification.
    await this.execute(
      'UNLOCK_PLOT',
      { plotId: unlockPayload.plotId },
      unlockPayload.expectedPlotVersion,
      undefined,
      unlockPayload.expectedUnlockRuleVersion,
    );
  }

  private async waterOnPlot(plotId: string): Promise<void> {
    const farm = this.store.onFarm(this.currentOwnerId);
    const plot = farm?.state.plots.find((p) => p.plotId === plotId && isWaterablePlot(p, this.serverNow()));
    if (!plot) return this.showMessage('该地块不需要浇水', true);
    await this.execute('CARE', { plotId: plot.plotIndex }, plot.plotVersion, plot.cropCycleId);
    this.activeTool = null;
    this.root.querySelectorAll('[data-tool]').forEach((b) => b.classList.remove('active'));
  }

  private async harvestOnPlot(plotId: string): Promise<void> {
    if (!this.ensurePersonalFarm()) return;
    const farm = this.store.onFarm(this.currentOwnerId);
    const plot = farm?.state.plots.find((p) => p.plotId === plotId
      && p.lockState === 'UNLOCKED'
      && effectiveCropPhase(p, this.serverNow()) === 'MATURE');
    if (!plot) return this.showMessage('该地块没有成熟作物', true);
    await this.execute('HARVEST', { plotId: plot.plotIndex }, plot.plotVersion, plot.cropCycleId);
    this.activeTool = null;
    this.root.querySelectorAll('[data-tool]').forEach((b) => b.classList.remove('active'));
  }

  private async sweepAll(commandType: 'HARVEST' | 'CARE' | 'THEFT', ownFarmOnly: boolean, emptyHint: string, doneHint: string): Promise<void> {
    if (ownFarmOnly && !this.ensurePersonalFarm()) return;
    if (!ownFarmOnly && this.currentOwnerId === this.ownOwnerId) return this.showMessage('请先进入好友农场', true);
    if (this.batchSweepRunning) return;

    // The farm owner and target set are both frozen at click time. A later
    // FarmChange may make a target stale, but it can never add another target
    // or cause THEFT to revisit a crop that remains MATURE.
    const batchOwnerId = this.currentOwnerId;
    const farm = this.store.onFarm(batchOwnerId);
    const isInitiallyOperable = (plot: PlotState): boolean => commandType === 'CARE'
      ? isWaterablePlot(plot, this.serverNow())
      : plot.lockState === 'UNLOCKED' && effectiveCropPhase(plot, this.serverNow()) === 'MATURE'
        && (commandType !== 'THEFT' || this.theftTargetEligible(batchOwnerId, plot));
    const targets = freezeBatchTargets(farm?.state.plots ?? [], isInitiallyOperable);
    if (targets.length === 0) return this.showMessage(emptyHint, true);

    this.batchSweepRunning = true;
    this.batchSweepCommandType = commandType;
    this.render();
    const batchLabel = commandType === 'THEFT' ? '一键偷菜' : commandType === 'CARE' ? '一键护理' : '一键收获';
    this.showMessage(`${batchLabel}处理中，共 ${targets.length} 个冻结目标，将逐个等待服务端终态…`);
    let result: Awaited<ReturnType<typeof runPlotBatch>>;
    try {
      result = await runPlotBatch({
        targets,
        readLatest: (target) => this.store.onFarm(batchOwnerId)?.state.plots
          .find((plot) => plot.plotIndex === target.plotIndex),
        isOperable: (target, plot) => this.isBatchPlotOperable(commandType, target, plot),
        isConnectionReady: () => this.ws.status === 'InFarm' && this.currentOwnerId === batchOwnerId,
        send: async (target, plot) => {
          // Re-read immediately before the real send. This second fence closes
          // the small race between runner qualification and CommandService
          // journaling, and ensures the expected version is never copied from
          // the frozen initial snapshot.
          const latest = this.store.onFarm(batchOwnerId)?.state.plots
            .find((candidate) => candidate.plotIndex === target.plotIndex);
          if (!latest || !this.isBatchPlotOperable(commandType, target, latest)) return { kind: 'skipped' };
          const operationId = await this.execute(
            commandType,
            { plotId: latest.plotIndex },
            latest.plotVersion,
            latest.cropCycleId,
            undefined,
            batchOwnerId,
          );
          return operationId ? { kind: 'sent', operationId } : { kind: 'failed' };
        },
        waitForTerminal: (operationId, timeoutMs) => this.commandService!.waitForTerminal(operationId, timeoutMs),
        operationTimeoutMs: BATCH_OPERATION_TIMEOUT_MS,
      });
    } finally {
      this.batchSweepRunning = false;
      this.batchSweepCommandType = null;
      this.render();
    }

    if (result.stopReason === 'connection') {
      this.showMessage(`${commandType === 'THEFT' ? '一键偷菜' : commandType === 'CARE' ? '一键护理' : '一键收获'}因连接中断停止，已确定终态 ${result.settled} 个目标；未决操作已保留`, true);
      return;
    }
    if (result.stopReason === 'unresolved') {
      this.showMessage(`${commandType === 'THEFT' ? '一键偷菜' : commandType === 'CARE' ? '一键护理' : '一键收获'}因存在未决操作停止，已确定终态 ${result.settled} 个目标；Journal 已保留`, true);
      return;
    }
    this.showMessage(doneHint.replace('{n}', String(result.settled)));
  }

  private isBatchPlotOperable(commandType: 'HARVEST' | 'CARE' | 'THEFT', target: BatchTarget, plot: PlotState): boolean {
    if (plot.plotIndex !== target.plotIndex || plot.cropCycleId !== target.cropCycleId) return false;
    if (commandType === 'CARE') return isWaterablePlot(plot, this.serverNow());
    return plot.lockState === 'UNLOCKED' && effectiveCropPhase(plot, this.serverNow()) === 'MATURE'
      && (commandType !== 'THEFT' || this.theftTargetEligible(this.currentOwnerId, plot));
  }

  private async harvestAll(): Promise<void> {
    await this.sweepAll('HARVEST', true, '暂时没有可收获的成熟作物', '一键收获完成，共确定终态 {n} 个目标');
  }

  private async careAll(): Promise<void> {
    await this.sweepAll('CARE', false, '当前没有需要浇水的作物', '一键护理完成，共确定终态 {n} 个目标');
  }

  private async stealAll(): Promise<void> {
    this.pendingTheftConfirmation = null;
    const authority = this.friendAuthority.get(this.currentOwnerId);
    if (this.currentOwnerId !== this.ownOwnerId && (!authority || authority.status !== 'READY')) {
      void this.loadFriendRoster(true, false, false);
      return this.showMessage('偷菜机会正在向服务端确认，请稍后再试', true);
    }
    await this.sweepAll('THEFT', false, '本农场当前没有可偷作物（可能本茬已经偷过）', '一键偷菜完成，共确定终态 {n} 个目标');
  }

  private async beginTheft(): Promise<void> {
    if (this.currentOwnerId === this.ownOwnerId) return this.showMessage('请先进入好友农场', true);
    const authority = this.friendAuthority.get(this.currentOwnerId);
    if (!authority || authority.status !== 'READY') {
      void this.loadFriendRoster(true, false, false);
      return this.showMessage('偷菜机会正在向服务端确认，请稍后再试', true);
    }
    const farm = this.store.onFarm(this.currentOwnerId);
    const trustedNow = this.serverNow();
    const eligiblePlots = (farm?.state.plots ?? []).filter((plot) =>
      plot.lockState === 'UNLOCKED' && effectiveCropPhase(plot, trustedNow) === 'MATURE'
      && this.theftTargetEligible(this.currentOwnerId, plot));
    const candidate = eligiblePlots.find((plot) => plot.plotId === this.selectedPlotId)
      ?? selectTheftPlot(eligiblePlots, null, trustedNow);
    if (!candidate) {
      this.pendingTheftConfirmation = null;
      return this.showMessage('本农场当前没有可偷作物（可能本茬已经偷过）', true);
    }
    const armed = this.pendingTheftConfirmation;
    if (!matchesTheftConfirmation(armed, this.currentOwnerId, candidate.plotId, candidate.cropCycleId)) {
      this.selectedPlotId = candidate.plotId;
      this.pendingTheftConfirmation = {
        ownerId: this.currentOwnerId, plotId: candidate.plotId, cropCycleId: candidate.cropCycleId ?? '',
      };
      this.render();
      return this.showMessage(`已选择可偷地块 ${candidate.plotId.replace('plot-', '#')}；请再次点击“确认偷菜”，本次尚未提交请求`);
    }
    this.pendingTheftConfirmation = null;
    const operationId = await this.execute('THEFT', { plotId: candidate.plotIndex }, candidate.plotVersion, candidate.cropCycleId);
    const pending = operationId && this.commandService?.getPending().find((operation) => operation.operationId === operationId);
    if (pending && pending.status !== 'Resolved') {
      this.showMessage(`偷菜请求已提交（地块 ${candidate.plotId.replace('plot-', '#')}），正在等待服务端权威结算；此提示不代表已经成功`);
    }
  }

  // H1 编译兼容适配:旧调用点统一经本助手映射到最终公网 Typed Payload
  // (target_farm_id/expected_* 进入 Payload;详见 h1-wire-reducer-handoff §7)。
  private async execute(commandType: 'BUY' | 'SELL' | 'UNLOCK_PLOT' | 'PLANT' | 'HARVEST' | 'CARE' | 'THEFT', payload?: Record<string, unknown>, expectedPlotVersion?: Uint64Value, expectedCropCycleId?: string, expectedUnlockRuleVersion?: Uint64Value, targetFarmId = this.currentOwnerId): Promise<string | undefined> {
    if (!this.commandService) {
      this.showMessage('游戏连接尚未初始化', true);
      return undefined;
    }
    if (this.ws.status !== 'InFarm') {
      this.showMessage('连接正在恢复，操作将在状态恢复完成后开放', true);
      return undefined;
    }
    // Every plot command receives the exact 0-based index carried by the
    // authoritative PlotView. Never derive a public plot_id from a DOM id.
    const plotIndex = (raw: unknown): number => {
      if (typeof raw !== 'number' || !Number.isInteger(raw) || raw < 0) {
        throw new Error('plot index is missing from the authoritative PlotView');
      }
      return raw;
    };
    try {
      switch (commandType) {
        case 'BUY':
          return await this.commandService.execute({
            type: 'BUY',
            offerId: Number(payload?.offerId ?? 1),
            count: Number(payload?.count ?? 1),
            expectedOfferVersion: Number(payload?.expectedOfferVersion ?? 1),
          });
        case 'SELL':
          return await this.commandService.execute({ type: 'SELL', itemId: Number(payload?.itemId ?? 2001), count: Number(payload?.count ?? 1) });
        case 'UNLOCK_PLOT':
          if (expectedPlotVersion === undefined || expectedUnlockRuleVersion === undefined) {
            throw new Error('unlock plot requires server plot/rule versions');
          }
          return await this.commandService.execute({
            type: 'UNLOCK_PLOT',
            plotId: plotIndex(payload?.plotId),
            targetFarmId,
            expectedPlotVersion,
            expectedUnlockRuleVersion,
          });
        case 'PLANT': {
          if (targetFarmId !== this.ownOwnerId) {
            this.showMessage('好友农场当前为只读，先回到我的农场再操作', true);
            return undefined;
          }
          // Last-mile guard for every current and future Plant entry point:
          // only the latest authoritative PlotView can authorize the send.
          const requestedPlotIndex = plotIndex(payload?.plotId);
          const authoritativePlot = this.store.onFarm(targetFarmId)?.state.plots
            .find((plot) => plot.plotIndex === requestedPlotIndex);
          if (!authoritativePlot) {
            this.showMessage('该地块状态未同步，暂不能播种', true);
            return undefined;
          }
          if (authoritativePlot.lockState === 'UNSPECIFIED') {
            this.showMessage('土地状态未确认，暂不能播种', true);
            return undefined;
          }
          if (authoritativePlot.lockState === 'LOCKED') {
            this.showMessage('这块土地已锁定，请先购买解锁', true);
            return undefined;
          }
          if (!isPlantablePlot(authoritativePlot)) {
            this.showMessage('该地块不可播种', true);
            return undefined;
          }
          return await this.commandService.execute({
            type: 'PLANT',
            plotId: requestedPlotIndex,
            cropDefinitionId: Number(payload?.cropDefinitionId ?? this.activeCropDefinitionId),
            targetFarmId,
            expectedPlotVersion: authoritativePlot.plotVersion,
            // 作物规则版本由 32 号合同冻结后下发;当前客户端无权威来源,发 0
            // (服务端在 32 接线前不裁决该字段,见后端 p1b-handoff §9.3)。
            expectedCropRuleVersion: 0,
          });
        }
        case 'HARVEST': {
          const requestedPlotIndex = plotIndex(payload?.plotId);
          const authoritativePlot = this.store.onFarm(targetFarmId)?.state.plots
            .find((plot) => plot.plotIndex === requestedPlotIndex);
          const currentCropCycleId = authoritativePlot?.cropCycleId;
          if (!authoritativePlot || currentCropCycleId === undefined || expectedCropCycleId === undefined
            || !this.isBatchPlotOperable('HARVEST', { plotIndex: requestedPlotIndex, cropCycleId: expectedCropCycleId }, authoritativePlot)) {
            this.showMessage('该地块状态已变化，收获已跳过', true);
            return undefined;
          }
          return await this.commandService.execute({
            type: 'HARVEST',
            plotId: requestedPlotIndex,
            targetFarmId,
            cropCycleId: currentCropCycleId,
            expectedPlotVersion: authoritativePlot.plotVersion,
          });
        }
        case 'CARE': {
          const requestedPlotIndex = plotIndex(payload?.plotId);
          const authoritativePlot = this.store.onFarm(targetFarmId)?.state.plots
            .find((plot) => plot.plotIndex === requestedPlotIndex);
          const currentCropCycleId = authoritativePlot?.cropCycleId;
          if (!authoritativePlot || currentCropCycleId === undefined || expectedCropCycleId === undefined
            || !this.isBatchPlotOperable('CARE', { plotIndex: requestedPlotIndex, cropCycleId: expectedCropCycleId }, authoritativePlot)) {
            this.showMessage('该地块状态已变化，护理已跳过', true);
            return undefined;
          }
          return await this.commandService.execute({
            type: 'CARE',
            plotId: requestedPlotIndex,
            targetFarmId,
            cropCycleId: currentCropCycleId,
            careType: 'WATER',
          });
        }
        case 'THEFT': {
          const requestedPlotIndex = plotIndex(payload?.plotId);
          const authoritativePlot = this.store.onFarm(targetFarmId)?.state.plots
            .find((plot) => plot.plotIndex === requestedPlotIndex);
          const currentCropCycleId = authoritativePlot?.cropCycleId;
          if (!authoritativePlot || currentCropCycleId === undefined || expectedCropCycleId === undefined
            || !this.isBatchPlotOperable('THEFT', { plotIndex: requestedPlotIndex, cropCycleId: expectedCropCycleId }, authoritativePlot)) {
            this.showMessage('该地块状态已变化，偷菜已跳过', true);
            return undefined;
          }
          return await this.commandService.execute({
            type: 'THEFT',
            plotId: requestedPlotIndex,
            targetFarmId,
            cropCycleId: currentCropCycleId,
          });
        }
      }
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '命令发送失败', true);
      return undefined;
    }
    return undefined;
  }

  private async sellAllCrops(): Promise<void> {
    if (this.bulkSelling) return;
    if (!this.ensurePersonalFarm()) return;
    if (this.ws.status !== 'InFarm') return this.showMessage('连接正在恢复，暂不能批量出售', true);
    const privateState = this.store.getPrivateState();
    const stock = CROP_CATALOG.map((crop) => ({
      itemId: crop.produceItemId,
      quantity: privateState.inventory[produceInventoryKey(crop)] ?? 0,
    })).filter((entry) => entry.quantity > 0);
    const targets = freezeSellBatchTargets(stock);
    const total = targets.reduce((sum, target) => sum + target.count, 0);
    if (total <= 0) return this.showMessage('仓库没有可出售的成熟作物', true);

    this.bulkSelling = true;
    this.render();
    try {
      // Freeze the authoritative click-time quantity, split every stack to the
      // public SELL count boundary, and admit at most one unresolved operation.
      // Inventory is never decremented optimistically; each following chunk is
      // sent only after the prior exact operation reaches SUCCEEDED.
      const result = await runSellBatch({
        targets,
        isConnectionReady: () => this.ws.status === 'InFarm' && this.currentOwnerId === this.ownOwnerId,
        send: (target) => this.execute('SELL', { itemId: target.itemId, count: target.count }),
        waitForTerminal: (operationId, timeoutMs) => this.commandService!.waitForTerminal(operationId, timeoutMs),
        operationTimeoutMs: BATCH_OPERATION_TIMEOUT_MS,
      });
      if (result.stopReason) {
        const reason = result.stopReason === 'connection' ? '连接已变化'
          : result.stopReason === 'terminal-rejection' ? `服务端终态 ${result.terminalStatus ?? 'REJECTED'}`
            : '操作结果尚未确认';
        this.showMessage(`批量出售已停止：${reason}；已确认出售 ${result.settledItems}/${result.itemCount} 个`, true);
      } else {
        this.showMessage(`已出售 ${result.settledItems} 个作物（${result.settledTargets} 笔权威终态）`);
      }
    } finally {
      this.bulkSelling = false;
      this.render();
    }
  }

  /** 读取数量输入并钳制到 [1, max];缺失/非法时回退 1(服务端仍按 fence 裁决)。 */
  private readCountInput(scope: ParentNode, selector: string, max: number): number {
    const input = scope.querySelector<HTMLInputElement>(selector);
    const raw = input ? Number.parseInt(input.value, 10) : Number.NaN;
    if (!Number.isFinite(raw) || raw < 1) return 1;
    return Math.min(raw, max);
  }

  /** 受信服务端时间下界;未同步时返回 null(fail-closed)。 */
  private serverNow(): number | null {
    return this.trustedClock.trustedNow();
  }

  private cropIcon(stage: string, watered: boolean, cropDefinitionId = 1): string {
    const drop = watered ? '<i class="crop-badge">💧</i>' : '';
    const species = cropById(cropDefinitionId).icon;
    switch (stage) {
      case 'stage-empty':
        return '<span class="crop-empty"><svg viewBox="0 0 48 48" aria-hidden="true"><circle cx="24" cy="24" r="16" fill="none" stroke="rgba(255,248,222,.65)" stroke-width="3" stroke-dasharray="4 6"/><path d="M24 16v16M16 24h16" stroke="rgba(255,248,222,.75)" stroke-width="4" stroke-linecap="round"/></svg></span>';
      case 'stage-sprout':
        return `<span class="crop-sprout"><svg viewBox="0 0 48 48" aria-hidden="true"><path d="M24 40V28" stroke="#6b8f3f" stroke-width="4" stroke-linecap="round"/><path d="M24 28c-6 0-10-4-10-9 4-1 8 1 10 5 2-4 6-6 10-5 0 5-4 9-10 9z" fill="#6fbf4a"/><path d="M24 28c-2-3-2-7 0-10 2 3 2 7 0 10z" fill="#8fd96a"/></svg><i class="crop-species">${species}</i>${drop}</span>`;
      case 'stage-leaf':
        return `<span class="crop-leaf"><svg viewBox="0 0 48 48" aria-hidden="true"><path d="M24 42V26" stroke="#5d7f36" stroke-width="4" stroke-linecap="round"/><path d="M24 26c-8 1-14-4-15-12 5-1 11 2 14 8 3-6 9-9 14-8-1 8-7 13-13 12z" fill="#5fb84a"/><path d="M24 26c-2-5-1-11 2-15 3 4 4 10-2 15z" fill="#7dd45e"/><path d="M20 34c-4 2-7 5-8 9h24c-1-4-4-7-8-9" fill="#c98f5a"/></svg><i class="crop-species">${species}</i>${drop}</span>`;
      case 'stage-ripen':
        return `<span class="crop-ripen"><svg viewBox="0 0 48 48" aria-hidden="true"><path d="M24 6c-3 4-5 7-5 11h10c0-4-2-7-5-11z" fill="#63b14d"/><path d="M19 17c-7 2-10 9-8 17 2 7 7 10 13 10s11-3 13-10c2-8-1-15-8-17" fill="#e8913a"/><path d="M19 22c-4 1-6 5-5 10" stroke="#f5b968" stroke-width="2" fill="none"/><path d="M24 44c-1-3-2-6-2-9" stroke="#c9762b" stroke-width="2"/></svg><i class="crop-species">${species}</i>${drop}</span>`;
      case 'stage-mature':
      default:
        return `<span class="crop-mature"><svg viewBox="0 0 48 48" aria-hidden="true"><path d="M24 4c-3 4-5 7-5 11h10c0-4-2-7-5-11z" fill="#5fae4a"/><path d="M18 15c-8 2-11 10-9 19 2 7 8 11 15 11s13-4 15-11c2-9-1-17-9-19" fill="#ef9b3f"/><path d="M18 21c-5 1-8 6-7 12" stroke="#ffc069" stroke-width="2" fill="none"/><path d="M24 45c-1-4-2-7-2-10" stroke="#d07a2d" stroke-width="2"/><path d="M30 20c3 1 5 4 5 8" stroke="#e0812c" stroke-width="2" fill="none"/></svg><i class="crop-species">${species}</i><i class="crop-ready-badge">✨</i>`;
    }
  }

  private render(): void {
    const farm = this.store.onFarm(this.currentOwnerId);
    const privateState = this.store.getPrivateState();
    const viewingOwnFarm = this.currentOwnerId === this.ownOwnerId;
    const writesReady = this.ws?.status === 'InFarm';
    const selectedPlot = farm?.state.plots.find((plot) => plot.plotId === this.selectedPlotId);
    // Q1:展示成熟用受信服务端时钟派生(不用 Date.now),wire status 保持权威原样。
    // trustedNow 可 null:effectiveCropPhase 遇 null fail-closed(不提前成熟);
    // 倒计时/进度条数学用本地时钟兜底(仅影响剩余秒显示,绝不影响成熟判定)。
    const trustedNow = this.serverNow();
    const countdownNow = trustedNow ?? Date.now();
    const selectedMature = Boolean(selectedPlot
      && selectedPlot.lockState === 'UNLOCKED'
      && effectiveCropPhase(selectedPlot, trustedNow) === 'MATURE');
    // Profile identity is an authority projection, never an owner-id-derived
    // fallback. Bootstrap profile is valid only for the authenticated owner;
    // FarmSnapshot is the only public profile source for a friend farm.
    const ownerProfile = farm?.ownerPublicProfile;
    const authenticatedProfile = privateState.profile?.playerId === this.ownOwnerId
      ? privateState.profile
      : undefined;
    const ownBootstrapProfile = viewingOwnFarm ? authenticatedProfile : undefined;
    const ownSnapshotProfile = this.store.onFarm(this.ownOwnerId)?.ownerPublicProfile;
    const ownerProfileSource: 'snapshot-owner-public-profile' | 'own-bootstrap' | 'unavailable' = ownerProfile
      ? 'snapshot-owner-public-profile'
      : ownBootstrapProfile
        ? 'own-bootstrap'
        : 'unavailable';
    const farmVersion = farm?.farmVersion === undefined ? '' : String(farm.farmVersion);
    const profilePlayerId = ownerProfile?.playerId ?? ownBootstrapProfile?.playerId ?? '';
    // GET_PLAYER_BOOTSTRAP's private profile has no avatar_id. Keep it empty
    // in that source instead of inventing an avatar from player identity.
    const profileAvatarId = ownerProfile ? String(ownerProfile.avatarId) : '';
    const profileLevel = ownerProfile?.level ?? (ownBootstrapProfile ? privateState.progression?.level : undefined);
    const taskSummary = viewingOwnFarm ? privateState.featureSummary?.tasks : undefined;
    const careWait = this.careRewardRefresh.current;
    const careRevision = privateState.playerStateRevision === undefined
      ? ''
      : String(privateState.playerStateRevision);
    const appShell = this.root.querySelector<HTMLElement>('[data-app-shell]');
    if (appShell) {
      appShell.dataset.ownOwnerId = this.ownOwnerId;
      appShell.dataset.currentOwnerId = this.currentOwnerId;
      appShell.dataset.connectionGeneration = String(this.recoveryBootstrapGeneration);
      appShell.dataset.farmVersion = farmVersion;
      appShell.dataset.ownerProfileSource = ownerProfileSource;
      appShell.dataset.profilePlayerId = profilePlayerId;
      appShell.dataset.profileAvatarId = profileAvatarId;
      appShell.dataset.profileLevel = profileLevel === undefined ? '' : String(profileLevel);
      appShell.dataset.taskStateVersion = taskSummary ? String(taskSummary.taskStateVersion) : '';
      appShell.dataset.taskClaimableCount = taskSummary ? String(taskSummary.claimableCount) : '';
      appShell.dataset.activeTool = this.activeTool ?? '';
      // 可观测:B bootstrap 终态与恢复屏障开合,供集成排查与 Playwright 断言。
      appShell.dataset.bootstrapState = this.bootstrapOutcome;
      appShell.dataset.recoveryBarrier = this.ws?.status === 'Recovering' ? 'closed' : 'open';
      // PlayerRefresh §5:诊断属性——私有状态是否就绪、当前权威 revision、Join 阶段。
      appShell.dataset.privateReady = String(this.store.hasPrivateState());
      const diagnosticRevision = this.store.getPrivateState().playerStateRevision;
      appShell.dataset.playerStateRevision = diagnosticRevision === undefined ? '' : String(diagnosticRevision);
      appShell.dataset.joinPhase = this.ws?.joinPhase ?? '';
      // A1B:受信服务端时钟诊断(trusted_server_now_ms 锚点 + 单调推算下界)。
      const serverClockNow = this.serverNow();
      appShell.dataset.trustedClockSynced = String(serverClockNow !== null);
      appShell.dataset.serverClockMs = serverClockNow === null ? '' : String(serverClockNow);
      // PlayerRefresh §5:永久低成本诊断——资产 refresh 状态,不再靠猜。
      appShell.dataset.bootstrapRecoveryAttempts = String(this.bootstrapRecovery.recoverySentCount);
      appShell.dataset.bootstrapRefreshDirtySeq = String(this.playerBootstrapRefresh.dirtySeq);
      appShell.dataset.bootstrapRefreshSent = String(this.bootstrapRefreshSent);
      appShell.dataset.bootstrapRefreshApplied = String(this.bootstrapRefreshApplied);
      appShell.dataset.bootstrapRefreshInflight = String(this.playerBootstrapRefresh.pending);
      appShell.dataset.bootstrapRefreshRetries = String(this.playerBootstrapRefresh.retries);
      appShell.dataset.careRewardStatus = this.careRewardStatus;
      appShell.dataset.careRewardOperation = careWait?.operationId ?? '';
      appShell.dataset.careRewardBaselineRevision = careWait?.baselineRevision ?? '';
      appShell.dataset.careRewardRevision = careRevision;
      appShell.dataset.farmTitleSource = ownerProfileSource;
      // Journal 未决操作数:终态处理后(含 defer pending 移除)应归 0,证明
      // Operation Journal 已清理、无残留 pending operation(S1A 硬断言)。
      appShell.dataset.pendingOps = String(this.commandService?.getPending().length ?? 0);
    }
    // Friend farm identity is only the latest legal FarmSnapshot profile. Never
    // turn an owner id, inviter label, or nickname initial into a profile.
    this.farmTitleElement.textContent = viewingOwnFarm
      ? '我的农场'
      : ownerProfile ? `好友农场 · ${ownerProfile.displayName}` : '好友农场';
    this.farmSubtitleElement.textContent = viewingOwnFarm
      ? '播种 · 浇水 · 收获你的作物'
      : ownerProfile ? '正在拜访好友的农场，可以帮忙浇水，也可以摘取成熟作物' : '好友资料暂不可用，请重新进入好友农场';
    this.farmTitleElement.setAttribute('data-farm-title-source', ownerProfileSource);
    this.root.querySelector('[data-farm-sign]')?.classList.toggle('friend-mode', !viewingOwnFarm);
    this.root.querySelector<HTMLElement>('[data-version]')!.dataset.farmVersion = farmVersion;
    this.root.querySelector('[data-farm-area]')?.setAttribute('data-active-tool', this.activeTool ?? '');
    // 顶部自己的昵称/等级使用 Bootstrap(A1 §4):displayName 优先,ownerId 兜底。
    const ownDisplayName = authenticatedProfile?.displayName || ownSnapshotProfile?.displayName || '我的农场';
    const ownLevel = privateState.progression?.level;
    this.playerNameElement.textContent = ownDisplayName;
    const playerAvatar = this.root.querySelector<HTMLElement>('.player-avatar');
    if (playerAvatar) playerAvatar.textContent = ownSnapshotProfile ? avatarIconForId(ownSnapshotProfile.avatarId) : '？';
    const playerChipCopy = this.root.querySelector<HTMLElement>('.player-chip em');
    if (playerChipCopy) {
      playerChipCopy.textContent = `我的农场${ownLevel === undefined ? '' : ` · Lv.${ownLevel}`}`;
    }
    this.friendVisitButton.disabled = !writesReady || this.friendIds.size === 0 || this.friendOpportunityState === 'loading';
    const previousFriend = this.root.querySelector<HTMLButtonElement>('[data-previous-friend]');
    if (previousFriend) previousFriend.disabled = !writesReady || this.friendVisitHistoryIndex <= 0;
    this.renderFriends();

    const maturePlots = farm?.state.plots.filter((plot) =>
      plot.lockState === 'UNLOCKED' && effectiveCropPhase(plot, trustedNow) === 'MATURE') ?? [];
    const theftAuthority = viewingOwnFarm ? undefined : this.friendAuthority.get(this.currentOwnerId);
    const theftCandidatePlots = maturePlots.filter((plot) => this.theftTargetEligible(this.currentOwnerId, plot));
    const theftKnownUnavailable = !viewingOwnFarm && theftAuthority?.status === 'READY' && theftCandidatePlots.length === 0;
    const theftAuthorityPending = !viewingOwnFarm && theftAuthority?.status !== 'READY';
    this.theftButton.disabled = !writesReady || viewingOwnFarm || this.batchSweepRunning;
    this.theftButton.classList.toggle('action-unavailable', theftKnownUnavailable || theftAuthorityPending);
    this.theftButton.setAttribute('aria-disabled', String(theftKnownUnavailable || theftAuthorityPending));
    this.theftButton.title = theftAuthorityPending ? '偷菜机会正在向服务端确认' : theftKnownUnavailable
      ? '本农场当前没有可偷作物，可能本茬已经偷过' : '选择并确认偷菜';
    const theftLabel = this.theftButton.querySelector<HTMLElement>('span:last-child');
    const armedTheftPlotId = this.pendingTheftConfirmation?.ownerId === this.currentOwnerId
      ? this.pendingTheftConfirmation.plotId : null;
    if (theftLabel) theftLabel.textContent = armedTheftPlotId && theftCandidatePlots.some((plot) => plot.plotId === armedTheftPlotId)
      ? `确认偷菜 ${armedTheftPlotId.replace('plot-', '#')}`
      : theftAuthorityPending ? '机会查询中' : theftKnownUnavailable ? '本茬已偷过' : '偷菜';
    const waterablePlots = farm?.state.plots.filter((plot) => isWaterablePlot(plot, trustedNow)) ?? [];
    const batchReady = writesReady && !this.batchSweepRunning;
    const batchRunning = this.batchSweepRunning;
    const showHarvestBatch = viewingOwnFarm && (maturePlots.length > 0 || (batchRunning && this.batchSweepCommandType === 'HARVEST'));
    const showCareBatch = waterablePlots.length > 0 || (batchRunning && this.batchSweepCommandType === 'CARE');
    const showTheftBatch = !viewingOwnFarm || (batchRunning && this.batchSweepCommandType === 'THEFT');
    this.harvestAllButton.toggleAttribute('hidden', !showHarvestBatch);
    this.careAllButton.toggleAttribute('hidden', !showCareBatch);
    this.stealAllButton.toggleAttribute('hidden', !showTheftBatch);
    this.harvestAllButton.disabled = !batchReady;
    this.careAllButton.disabled = !batchReady;
    this.stealAllButton.disabled = !batchReady;
    this.stealAllButton.classList.toggle('action-unavailable', theftKnownUnavailable || theftAuthorityPending);
    this.stealAllButton.setAttribute('aria-disabled', String(theftKnownUnavailable || theftAuthorityPending));
    this.stealAllButton.title = this.theftButton.title;
    const stealAllLabel = this.stealAllButton.querySelector<HTMLElement>('b');
    if (stealAllLabel) stealAllLabel.textContent = batchRunning && this.batchSweepCommandType === 'THEFT'
      ? '偷菜处理中…' : theftAuthorityPending ? '机会查询中' : theftKnownUnavailable ? '本茬已偷过' : '一键偷菜';

    const wallet = privateState.wallet;
    // 私有数据尚无权威来源(Bootstrap 未接线且尚无经营回包)时显示未知,
    // 不把 0 当成真实资产(详见 h1-wire-reducer-handoff §7)。
    const privateReady = this.store.hasPrivateState();
    const selectedCrop = cropById(this.activeCropDefinitionId);
    const selectedSeeds = privateState.inventory[seedInventoryKey(selectedCrop)] ?? 0;
    const totalSeeds = CROP_CATALOG.reduce((sum, crop) => sum + (privateState.inventory[seedInventoryKey(crop)] ?? 0), 0);
    this.privateElement.dataset.walletCount = privateReady ? String(wallet) : 'unknown';
    this.privateElement.dataset.seedCount = String(totalSeeds);
    this.privateElement.innerHTML = `
      <span class="hud-resource"><i>🪙</i><strong>${privateReady ? wallet : '…'}</strong><small>金币</small></span>
      <span class="hud-resource"><i>${selectedCrop.icon}</i><strong>${privateReady ? selectedSeeds : '…'}</strong><small>${selectedCrop.name}种子</small></span>`;

    this.root.querySelector<HTMLElement>('[data-selected-seed-icon]')!.textContent = selectedCrop.icon;
    this.root.querySelector<HTMLElement>('[data-selected-seed-label]')!.textContent = `${selectedCrop.name}种子`;

    const seedQuickBar = this.root.querySelector<HTMLElement>('[data-seed-quick]');
    if (seedQuickBar) {
      const canPickSeeds = writesReady && viewingOwnFarm;
      seedQuickBar.innerHTML = CROP_CATALOG.map((crop) => {
        const count = privateState.inventory[seedInventoryKey(crop)] ?? 0;
        const active = crop.id === this.activeCropDefinitionId && this.activeTool === 'plant';
        return `<button type="button" class="seed-chip ${active ? 'active' : ''}" data-quick-seed="${crop.id}" ${canPickSeeds ? '' : 'disabled'} aria-label="选择${crop.name}种子" title="${crop.name} · ${crop.matureAfterMs / 1000}s 成熟"><span>${crop.icon}</span><b>${crop.name}</b><small>×${count}</small></button>`;
      }).join('');
    }

    // Tool button states
    this.root.querySelectorAll<HTMLButtonElement>('[data-tool]').forEach((btn) => {
      const tool = btn.dataset.tool as ToolMode;
      btn.classList.toggle('active', this.activeTool === tool);
      btn.disabled = !writesReady || (!viewingOwnFarm && tool !== 'water');
    });

    this.root.querySelector<HTMLElement>('[data-selection]')!.textContent = selectedPlot
      ? `已选择 ${selectedPlot.plotId.replace('plot-', '地块 #')} · ${selectedPlot.lockState === 'LOCKED' ? '已锁定' : selectedPlot.lockState === 'UNSPECIFIED' ? '土地状态未知' : selectedMature ? '已成熟' : selectedPlot.status === 'EMPTY' ? '已解锁空地' : selectedPlot.wateredAt ? '已浇水' : '生长中'}`
      : this.activeTool ? TOOL_HINTS[this.activeTool] : '点击一个地块查看操作目标';
    this.root.querySelector('[data-version]')!.textContent = `farm_version ${farm?.farmVersion ?? '—'}`;

    this.farmElement.innerHTML = farm ? farm.state.plots.map((plot) => {
      const wirePhase = plot.status.toLowerCase();
      const locked = plot.lockState === 'LOCKED';
      const unspecifiedLock = plot.lockState !== 'UNLOCKED' && plot.lockState !== 'LOCKED';
      // Q1:展示成熟 = effectiveCropPhase(受信服务端时钟派生),wire status 不改写。
      const effective = effectiveCropPhase(plot, trustedNow);
      const mature = effective === 'MATURE';
      const timeProjection = cropTimeProjection(
        plot.matureAt ?? countdownNow,
        countdownNow,
        cropById(plot.cropDefinitionId ?? 1).matureAfterMs,
      );
      const secondsLeft = timeProjection.secondsLeft;
      const label = locked
        ? `等级 ${plot.requiredLevel} · ${String(plot.normalCoinPrice)} 金币`
        : unspecifiedLock
          ? '土地状态未知'
          : plot.status === 'EMPTY' ? '已解锁空地' : mature ? '已成熟' : `${secondsLeft}s`;
      const selected = plot.plotId === this.selectedPlotId;
      const canPlant = isPlantablePlot(plot) && viewingOwnFarm && this.activeTool === 'plant';
      const canWater = isWaterablePlot(plot, trustedNow) && this.activeTool === 'water';
      const canHarvest = plot.lockState === 'UNLOCKED' && mature && viewingOwnFarm && this.activeTool === 'harvest';
      const canSteal = plot.lockState === 'UNLOCKED' && mature && !viewingOwnFarm && this.theftTargetEligible(this.currentOwnerId, plot);
      const pendingUnlock = (this.commandService?.getPending() ?? []).some((operation) =>
        operation.commandType === 'UNLOCK_PLOT'
        && operation.targetFarmId === this.currentOwnerId
        && operation.plotId === plot.plotIndex);
      const canUnlock = canUnlockPlot(plot, viewingOwnFarm, writesReady, pendingUnlock);
      const stage = plot.status === 'EMPTY'
        ? 'stage-empty'
        : mature
          ? 'stage-mature'
          : timeProjection.stage;
      const phaseSource = plot.status === 'MATURE' || plot.status === 'EMPTY' ? 'wire' : mature ? 'trusted-time' : 'wire';
      const lockPrompt = locked
        ? `<small class="plot-lock-order">顺序 ${plot.unlockOrder || '—'} · ${plot.unlockCandidate ? (viewingOwnFarm ? '当前可购买' : '好友农场只读') : '需先解锁前一块'}</small>${canUnlock || pendingUnlock ? `<button type="button" class="unlock-plot-button" data-unlock-plot="${escapeHtml(plot.plotId)}" ${pendingUnlock ? 'disabled' : ''}>${pendingUnlock ? '土地解锁处理中' : '购买解锁'}</button>` : ''}`
        : unspecifiedLock
          ? '<small class="plot-lock-order">🔒 未收到有效锁地状态，操作已停用</small>'
          : '';
      const plotVisual = locked || unspecifiedLock
        ? `<div class="locked-plot-visual" aria-hidden="true"><span class="plot-lock-icon">🔒</span><span>${locked ? '锁地' : '待确认'}</span></div>`
        : `<div class="crop" aria-hidden="true">${this.cropIcon(stage, plot.wateredAt !== undefined, plot.cropDefinitionId)}</div>`;
      return `<div class="plot ${wirePhase} ${locked ? 'locked' : ''} ${unspecifiedLock ? 'lock-unspecified' : ''} ${mature ? 'mature' : ''} ${plot.wateredAt ? 'watered' : ''} ${selected ? 'selected' : ''} ${canPlant ? 'can-plant' : ''} ${canWater ? 'can-water' : ''} ${canHarvest ? 'can-harvest' : ''} ${canSteal ? 'can-steal' : ''} ${this.activeTool ? 'tool-active' : ''} ${stage}"
        data-plot-id="${plot.plotId}"
        data-plot-lock-state="${plot.lockState}"
        data-plot-unlock-candidate="${plot.unlockCandidate}"
        data-plot-unlock-rule-version="${String(plot.unlockRuleVersion)}"
        data-plot-version="${String(plot.plotVersion)}"
        data-plantable="${canPlant}"
        data-wire-crop-phase="${wirePhase}"
        data-effective-crop-phase="${effective.toLowerCase()}"
        data-crop-phase-source="${phaseSource}"
        data-harvestable-count="${plot.harvestableCount ?? ''}"
        data-stolen-total="${plot.stolenTotal ?? ''}">
        <span class="plot-index">${escapeHtml(plot.plotId.replace('plot-', '#'))}</span>
        ${plotVisual}
        <div class="plot-label"><strong>${escapeHtml(label)}</strong>${lockPrompt}</div>
      </div>`;
    }).join('') : '<p class="empty-state">等待农场 Snapshot…</p>';
    const pending = this.commandService?.getPending() ?? [];
    this.pendingElement.innerHTML = pending.length ? pending.map((operation) => {
      const label = operation.commandType === 'UNLOCK_PLOT' ? '土地解锁' : operation.commandType;
      const status = operation.commandType === 'UNLOCK_PLOT' && operation.status === 'Processing'
        ? '土地解锁处理中'
        : operation.status;
      return `<div class="pending-item"><span>${label}</span><strong>${status}</strong></div>`;
    }).join('') : '暂无未决操作';
    // 私有数据未知(Bootstrap 未接线)时不做本地资产门控,由服务端裁决并
    // 返回权威错误;显示为未知而非 0。
    this.renderUtility(privateReady ? wallet : null, privateState, writesReady, viewingOwnFarm);
    // Q1:每次 render 后只保留一个展示成熟重算 Timer(最小 mature_at)。
    this.scheduleMaturityRecompute(farm?.state.plots ?? []);
    this.scheduleFarmClockProjection(farm?.state.plots ?? []);
  }

  /**
   * Q1:单 Timer 展示重算。取当前农场尚未展示成熟作物中的最小 mature_at,
   * 到期后重算 render 并再调度下一个。这仅是页面展示重算(UI 投影),不是业务
   * Scheduler,绝不写入 FarmStore 权威状态。
   */
  private scheduleMaturityRecompute(plots: PlotState[]): void {
    if (this.maturityTimer !== undefined) {
      window.clearTimeout(this.maturityTimer);
      this.maturityTimer = undefined;
    }
    // 成熟判定用受信时钟 trustedNow(可 null,effectiveCropPhase fail-closed);
    // 调度间隔数学用本地时钟兜底(仅影响到点重算时机,不决定成熟判定)。
    const trustedNow = this.serverNow();
    const nowForDelay = trustedNow ?? Date.now();
    let nextAt: number | null = null;
    for (const plot of plots) {
      if (plot.status !== 'GROWING' || plot.matureAt === undefined) continue;
      if (effectiveCropPhase(plot, trustedNow) === 'MATURE') continue;
      if (nextAt === null || plot.matureAt < nextAt) nextAt = plot.matureAt;
    }
    if (nextAt === null) return;
    const delay = Math.max(0, nextAt - nowForDelay) + 50;
    this.maturityTimer = window.setTimeout(() => {
      this.maturityTimer = undefined;
      this.render();
    }, delay);
  }

  /**
   * Keep the visible countdown and cosmetic growth stage moving without the
   * former 500ms whole-page render loop. Authority changes still arrive via
   * Store events, and the exact mature_at edge is handled by maturityTimer.
   */
  private scheduleFarmClockProjection(plots: PlotState[]): void {
    if (this.farmClockProjectionTimer !== undefined) {
      window.clearTimeout(this.farmClockProjectionTimer);
      this.farmClockProjectionTimer = undefined;
    }
    const trustedNow = this.serverNow();
    const hasGrowingCrop = plots.some((plot) =>
      plot.status === 'GROWING'
      && plot.matureAt !== undefined
      && effectiveCropPhase(plot, trustedNow) === 'GROWING');
    if (!hasGrowingCrop) return;

    this.farmClockProjectionTimer = window.setTimeout(() => {
      this.farmClockProjectionTimer = undefined;
      this.refreshFarmClockProjection();
      this.scheduleFarmClockProjection(this.store.onFarm(this.currentOwnerId)?.state.plots ?? []);
    }, 1_000);
  }

  private refreshFarmClockProjection(): void {
    const farm = this.store.onFarm(this.currentOwnerId);
    if (!farm) return;
    const trustedNow = this.serverNow();
    const countdownNow = trustedNow ?? Date.now();
    const plotsById = new Map(farm.state.plots.map((plot) => [plot.plotId, plot]));
    const stageClasses = ['stage-sprout', 'stage-leaf', 'stage-ripen'];

    this.farmElement.querySelectorAll<HTMLElement>('[data-plot-id]').forEach((element) => {
      const plot = plotsById.get(element.dataset.plotId ?? '');
      if (
        !plot
        || plot.status !== 'GROWING'
        || plot.matureAt === undefined
        || effectiveCropPhase(plot, trustedNow) !== 'GROWING'
      ) return;

      const projection = cropTimeProjection(
        plot.matureAt,
        countdownNow,
        cropById(plot.cropDefinitionId ?? 1).matureAfterMs,
      );
      element.classList.remove(...stageClasses);
      element.classList.add(projection.stage);
      const label = element.querySelector<HTMLElement>('.plot-label > strong');
      if (label) label.textContent = `${projection.secondsLeft}s`;
      const crop = element.querySelector<HTMLElement>('.crop');
      if (crop) crop.innerHTML = this.cropIcon(
        projection.stage,
        plot.wateredAt !== undefined,
        plot.cropDefinitionId,
      );
    });
  }

  private renderFriends(): void {
    this.friendsElement.dataset.state = this.friendRosterState;
    const summary = this.root.querySelector<HTMLElement>('[data-friend-summary]');
    const count = this.root.querySelector<HTMLElement>('[data-friend-count]');
    const friendIds = [...this.friendIds].filter((ownerId) => {
      if (!this.friendSearch) return true;
      const label = this.friendLabels.get(ownerId) ?? ownerId;
      return `${label} ${ownerId}`.toLocaleLowerCase().includes(this.friendSearch);
    });
    if (summary) summary.textContent = `${this.friendIds.size} 位`;
    if (count) {
      count.textContent = String(this.friendIds.size);
      count.toggleAttribute('hidden', this.friendIds.size === 0);
    }
    this.friendsElement.innerHTML = this.friendRosterState === 'loading'
      ? '<p class="empty-state">正在照看好友花名册…</p>'
      : this.friendRosterState === 'error'
        ? `<div class="friend-load-error"><p>${escapeHtml(this.friendRosterError || '好友列表加载失败')}</p><button type="button" class="secondary" data-retry-friends>重新加载</button></div>`
        : friendIds.length
          ? friendIds.map((ownerId, index) => {
            const label = this.friendLabels.get(ownerId);
            const profile = this.friendProfiles.get(ownerId);
            const escapedOwnerId = escapeHtml(ownerId);
            const visiting = this.ws.status === 'InFarm' && ownerId === this.currentOwnerId;
            const authority = this.friendAuthority.get(ownerId);
            const canVisit = authority !== undefined && (authority.actionMask & FriendActionMask.VISIT) !== 0;
            const opportunity = presentFriendOpportunity(authority?.status, authority?.actionMask ?? 0);
            return `<article class="friend-row" data-friend-owner="${escapedOwnerId}" data-friend-avatar="${profile ? profile.avatarId : ''}" data-friend-level="${profile ? profile.level : ''}" data-friend-opportunity="${authority?.status ?? 'UNKNOWN'}" data-friend-relation-epoch="${authority?.relationEpoch.toString() ?? ''}" data-friend-action-mask="${authority?.actionMask ?? 0}" data-friend-can-theft="${opportunity.canTheft}" data-friend-can-water="${opportunity.canWater}">
              <span class="friend-rank">${index + 1}</span><span class="friend-avatar">${profile ? avatarIconForId(profile.avatarId) : '？'}</span>
              <span class="friend-copy"><strong>${escapeHtml(label ?? '好友资料暂不可用')}</strong><small>${profile ? `Lv.${profile.level} · ${visiting ? '正在拜访' : opportunity.label}` : '好友资料暂不可用'}</small></span>
              <button type="button" class="visit-button" data-friend-owner="${escapedOwnerId}"${visiting || !canVisit ? ` disabled${visiting ? ' aria-current="true"' : ''}` : ''}>${visiting ? '当前农场' : canVisit ? '拜访' : '暂不可用'}</button>
              <button type="button" class="friend-more" data-revoke-owner="${escapedOwnerId}" aria-label="解除与 ${escapeHtml(label ?? '好友')} 的好友关系" title="解除好友">···</button>
            </article>`;
          }).join('') + (this.friendRosterNextCursor ? '<button type="button" class="secondary" data-load-more-friends>加载更多好友</button>' : '')
          : this.friendSearch
            ? '<p class="empty-state">没有找到这位好友</p>'
            : '<p class="empty-state">还没有同玩好友。发出邀请，一起照看农场吧。</p>';
  }

  private renderUtility(wallet: number | null, privateState: PrivateState, writesReady: boolean, viewingOwnFarm: boolean): void {
    if (!this.utilityMode) {
      this.utilitySheet.setAttribute('hidden', '');
      return;
    }
    this.utilitySheet.removeAttribute('hidden');
    if (this.utilityMode === 'inventory') {
      this.utilityTitleElement.textContent = '我的仓库';
      const cropsSelected = this.inventoryTab === 'crops';
      const tabBar = `<div class="inventory-tabs" role="tablist">
        <button type="button" data-inventory-tab="crops" role="tab" aria-selected="${cropsSelected}">作物</button>
        <button type="button" data-inventory-tab="tools" role="tab" aria-selected="${!cropsSelected}">道具</button>
      </div>`;
      const content = cropsSelected
        ? `<div class="inventory-list">
            ${CROP_CATALOG.map((crop) => {
              const count = privateState.inventory[produceInventoryKey(crop)] ?? 0;
              const canSell = writesReady && viewingOwnFarm && (wallet === null || count > 0);
              return `<article class="inventory-item crop-item"><span class="inventory-item-icon">${crop.icon}</span><span class="inventory-item-copy"><strong>${crop.name}</strong><small>成熟作物 · 售价 ${crop.sellPrice} 金币</small></span><b>×${count}</b><span class="inventory-item-actions"><input class="qty-input" type="number" min="1" max="${Math.max(count, 1)}" value="1" data-sell-count="${crop.produceItemId}" aria-label="出售数量"${canSell ? '' : ' disabled'}><button type="button" class="single-sell" data-sell-item="${crop.produceItemId}" ${canSell ? '' : 'disabled'}>出售</button></span></article>`;
            }).join('')}
            <div class="inventory-actions">
              <button type="button" class="batch-sell" data-sell-all ${writesReady && viewingOwnFarm && (wallet === null || CROP_CATALOG.some((crop) => (privateState.inventory[produceInventoryKey(crop)] ?? 0) > 0)) && !this.bulkSelling ? '' : 'disabled'}>${this.bulkSelling ? '提交中…' : '全部出售'}</button>
            </div>
            <p class="inventory-note">每种作物独立计数；单个出售即时提交，批量出售会逐笔提交，断线后的未完成尾项可恢复查询。</p>
          </div>`
        : `<div class="inventory-list">
            ${CROP_CATALOG.map((crop) => {
              const count = privateState.inventory[seedInventoryKey(crop)] ?? 0;
              const selected = crop.id === this.activeCropDefinitionId;
              return `<article class="inventory-item ${selected ? 'selected-crop' : ''}"><span class="inventory-item-icon">${crop.icon}</span><span class="inventory-item-copy"><strong>${crop.name}种子</strong><small>购买价 ${crop.buyPrice} 金币 · ${crop.matureAfterMs / 1000}s 成熟</small></span><b>×${count}</b><button type="button" class="secondary" data-select-crop="${crop.id}">选择</button></article>`;
            }).join('')}
            <article class="inventory-item"><span class="inventory-item-icon">💧</span><span class="inventory-item-copy"><strong>水壶</strong><small>农场工具 · 可重复使用</small></span><b>∞</b></article>
          </div>`;
      this.utilityContentElement.innerHTML = `<div class="inventory-sheet">
        <div class="inventory-wallet"><span>🪙</span><strong>${wallet === null ? '…' : wallet}</strong><small>金币</small></div>
        ${tabBar}${content}
      </div>`;
      return;
    }
    if (this.utilityMode === 'shop') {
      this.utilityTitleElement.textContent = '农场商店';
      this.utilityContentElement.innerHTML = `<div class="shop-sheet"><p>每种作物有独立的种子、成熟时间和出售价格；购买后可在仓库“道具”页选择播种。</p>
        <label class="shop-count">本次购买数量 <input class="qty-input" type="number" min="1" max="99" value="${this.shopCount}" data-shop-count aria-label="购买数量"></label>
        ${CROP_CATALOG.map((crop) => `<button type="button" data-buy-offer="${crop.offerId}" ${crop.id === 1 ? 'data-buy' : ''} ${!writesReady || !viewingOwnFarm || (wallet !== null && wallet < crop.buyPrice) ? 'disabled' : ''}><span>${crop.icon}</span><b>购买${crop.name}种子</b><small>${crop.buyPrice} 金币 / 份 · ${crop.matureAfterMs / 1000}s 成熟</small></button>`).join('')}
      </div>`;
      return;
    }
    if (this.utilityMode === 'tasks') {
      this.utilityTitleElement.textContent = '每日任务';
      const taskSummary = privateState.featureSummary?.tasks;
      const taskEntries = taskSummary
        ? [...taskSummary.dailyTasks, ...(taskSummary.currentGrowth ? [taskSummary.currentGrowth] : [])]
        : [];
      const details = new Map([
        ...(this.taskList?.dailyTasks ?? []),
        ...(this.taskList?.currentGrowth ? [this.taskList.currentGrowth] : []),
      ].map((detail) => [detail.progress.instanceId, detail]));
      this.utilityContentElement.innerHTML = `<div class="task-sheet">
        <p>${this.taskListLoading ? '正在读取任务权威列表…' : taskSummary ? `周期 ${escapeHtml(taskSummary.dailyPeriodId)} · 服务端版本 ${escapeHtml(String(taskSummary.taskStateVersion))}` : '任务摘要暂不可用'}</p>
        ${taskEntries.map((task) => {
          const progress = task.progress;
          const target = task.target;
          const state = task.state;
          const claimable = state === 'CLAIMABLE';
          const claimed = state === 'CLAIMED';
          const reward = details.get(task.instanceId)?.reward;
          const rewardText = reward ? `奖励：${reward.coin ? `${reward.coin} 金币 ` : ''}${reward.exp ? `${reward.exp} 经验 ` : ''}${reward.items.map((item) => `道具 ${item.itemId}×${item.count}`).join(' ')}`.trim() : '奖励以服务端实例为准';
          return `<article class="task-entry ${claimable ? 'claimable' : ''} ${claimed ? 'claimed' : ''}" data-task-instance="${escapeHtml(task.instanceId)}" data-task-id="${escapeHtml(task.taskId)}" data-task-state="${state}" data-task-progress="${progress}" data-task-target="${target}">
            <div class="task-entry-copy"><strong>${escapeHtml(task.taskId)}</strong><small>实例 ${escapeHtml(task.instanceId)}</small><span>${escapeHtml(rewardText)} · ${state === 'CLAIMED' ? '已完成' : state === 'CLAIMABLE' ? '已达成' : '进行中'}</span></div>
            <div class="task-entry-progress"><b>${progress}/${target}</b><i><em style="width:${target > 0 ? Math.round(progress / target * 100) : 0}%"></em></i></div>
            <button type="button" data-claim-task="${escapeHtml(task.instanceId)}" ${claimable && writesReady && viewingOwnFarm ? '' : 'disabled'}>${claimed ? '已结算' : claimable ? '领取奖励' : '进行中'}</button>
          </article>`;
        }).join('') || '<p class="empty-state">暂无服务端任务实例</p>'}
      </div>`;
      return;
    }
    if (this.utilityMode === 'codex') {
      this.utilityTitleElement.textContent = '植物图鉴';
      const unlocked = new Map((privateState.codex ?? []).map((entry) => [entry.cropDefinitionId, entry.firstHarvestedAt]));
      this.utilityContentElement.innerHTML = `<div class="codex-sheet">
        <p>图鉴记录首次收获过的作物，与仓库库存相互独立。</p>
        ${CROP_CATALOG.map((crop) => {
          const firstHarvestedAt = unlocked.get(crop.id);
          const found = firstHarvestedAt !== undefined;
          return `<article class="codex-entry ${found ? 'unlocked' : 'locked'}" data-codex-crop="${crop.id}"><span>${found ? crop.icon : '❔'}</span><div><strong>${crop.name}</strong><small>${found ? `已解锁 · ${new Date(firstHarvestedAt).toLocaleString('zh-CN')}` : `收获第一茬${crop.name}后解锁`}</small></div><em>${found ? '已发现' : '未发现'}</em></article>`;
        }).join('')}
      </div>`;
      return;
    }
    this.utilityTitleElement.textContent = '农场动态';
    const webAccountPanel = clientEnvironment.name === 'web'
      ? `<div class="account-security">
          <p><b>账号安全</b><br>修改密码或退出全部设备后，当前连接会立即失效，需要重新登录。</p>
          <label><span>账号</span><input data-change-username autocomplete="username" value="${escapeHtml((this.auth as WebAuthProvider).lastUsername())}" maxlength="24"></label>
          <label><span>当前密码</span><input data-change-current type="password" autocomplete="current-password" maxlength="128"></label>
          <label><span>新密码</span><input data-change-next type="password" autocomplete="new-password" maxlength="128"></label>
          <label><span>确认新密码</span><input data-change-confirm type="password" autocomplete="new-password" maxlength="128"></label>
          <button type="button" class="secondary" data-change-password>修改密码</button>
          <button type="button" class="danger" data-logout-all>退出全部设备</button>
        </div>`
      : '';
    this.utilityContentElement.innerHTML = `<div class="more-sheet"><p><b>${viewingOwnFarm ? '正在照看自己的农场' : '正在拜访好友农场'}</b><br>连接状态：${escapeHtml(this.statusElement.textContent ?? 'Disconnected')}</p>
      <div class="operation-status"><span>未决操作</span>${this.pendingElement.innerHTML}</div>
      ${webAccountPanel}
      <button type="button" class="secondary" data-reconnect>模拟网络断线</button>
    </div>`;
  }

  private loadTaskList(): void {
    if (this.taskListLoading || this.ws.status !== 'InFarm') return;
    this.taskListLoading = true;
    this.render();
    try {
      this.ws.sendCommand({ requestId: createUuid(), payload: { type: 'GET_TASK_LIST' } });
    } catch (error) {
      this.taskListLoading = false;
      this.showMessage(error instanceof Error ? error.message : '任务列表请求失败', true);
      this.render();
    }
  }

  private async claimTask(taskInstanceId: string): Promise<void> {
    if (!this.commandService || this.ws.status !== 'InFarm' || this.currentOwnerId !== this.ownOwnerId) return;
    const task = this.store.getPrivateState().featureSummary?.tasks?.dailyTasks
      .concat(this.store.getPrivateState().featureSummary?.tasks?.currentGrowth ?? [])
      .find((entry) => entry.instanceId === taskInstanceId);
    if (!task || task.state !== 'CLAIMABLE') return this.showMessage('任务状态已变化，请刷新任务列表', true);
    try {
      await this.commandService.execute({ type: 'CLAIM_TASK', taskInstanceId });
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '任务领取发送失败', true);
    }
  }

  private async confirmReadyFriendVisit(targetFarmId: string): Promise<void> {
    if (this.confirmingFriendVisit || !this.commandService || this.ws.status !== 'InFarm') return;
    const visit = this.store.getPrivateState().featureSummary?.tasks?.dailyTasks
      .find((task) => task.taskId.includes('friend_visit'));
    if (visit && visit.state !== 'ACTIVE') return;
    this.confirmingFriendVisit = true;
    try {
      await this.commandService.execute({ type: 'CONFIRM_FRIEND_VISIT', targetFarmId });
    } catch (error) {
      this.confirmingFriendVisit = false;
      this.showMessage(error instanceof Error ? error.message : '好友拜访任务确认失败', true);
    }
  }

  private handleTaskResult(result: CommandResult): void {
    if (result.commandType === 'GET_TASK_LIST') {
      this.taskListLoading = false;
      if (result.status === 'SUCCEEDED' && result.taskList) {
        this.taskList = result.taskList;
        this.store.applyTaskSummary(result.taskList.summary);
      }
      this.render();
      return;
    }
    if (result.commandType === 'CONFIRM_FRIEND_VISIT') this.confirmingFriendVisit = false;
    if (result.taskSummary) this.store.applyTaskSummary(result.taskSummary);
    if (result.commandType === 'CLAIM_TASK' && (result.status === 'SUCCEEDED' || result.status === 'NO_CHANGE')) {
      this.showMessage(result.status === 'SUCCEEDED' ? '任务奖励已由服务端结算' : '任务奖励已领取');
      this.loadTaskList();
    }
  }

  private toggleUtility(mode: Exclude<UtilityMode, null>): void {
    this.utilityMode = this.utilityMode === mode ? null : mode;
    this.render();
    if (this.utilityMode === 'tasks') this.loadTaskList();
  }

  private closeUtility(): void {
    if (!this.utilityMode) return;
    this.utilityMode = null;
    this.utilitySheet.setAttribute('hidden', '');
  }

  private openFriends(): void {
    this.closeUtility();
    this.friendsModal.removeAttribute('hidden');
    this.root.querySelectorAll<HTMLElement>('[data-open-friends]').forEach((button) => button.setAttribute('aria-expanded', 'true'));
    void this.loadFriendRoster();
    window.setTimeout(() => this.friendSearchInput.focus(), 0);
  }

  private closeFriends(): void {
    if (this.friendsModal.hasAttribute('hidden')) return;
    this.friendsModal.setAttribute('hidden', '');
    this.root.querySelectorAll<HTMLElement>('[data-open-friends]').forEach((button) => button.setAttribute('aria-expanded', 'false'));
  }

  private showMessage(message: string, error = false): void {
    this.messageElement.textContent = message;
    this.messageElement.classList.toggle('error', error);
  }

  private showToast(message: string): void {
    if (this.toastTimer !== null) window.clearTimeout(this.toastTimer);
    this.toastElement.textContent = message;
    this.toastElement.removeAttribute('hidden');
    this.toastTimer = window.setTimeout(() => {
      this.toastElement.setAttribute('hidden', '');
      this.toastTimer = null;
    }, 4_000);
  }

  private async visitNextFriendOpportunity(): Promise<void> {
    if (!this.friendService || !this.ownOwnerId) return this.showMessage('好友机会服务暂不可用', true);
    const requestGeneration = this.friendOpportunityFlow.beginRequest();
    this.friendOpportunityState = 'loading';
    this.render();
    try {
      const opportunity = await this.friendService.nextOpportunity(this.ownOwnerId);
      if (!this.friendOpportunityFlow.isCurrentRequest(requestGeneration)) return;
      if (opportunity.status === 'UNKNOWN') throw new Error('好友互动机会权威查询暂不可用');
      if (opportunity.status === 'NONE') {
        this.friendOpportunityState = 'ready';
        this.render();
        return this.showMessage('本轮暂无未拜访的好友互动机会');
      }
      const friend = opportunity.friend;
      this.applyFriendSummary(friend);
      this.friendOpportunityState = 'ready';
      this.visitOwner(friend.ownerId);
    } catch (error) {
      if (!this.friendOpportunityFlow.isCurrentRequest(requestGeneration)) return;
      this.friendOpportunityState = 'error';
      this.render();
      this.showMessage(error instanceof Error ? error.message : '好友互动机会暂不可用', true);
    }
  }

  private visitPreviousFriend(): void {
    if (this.friendVisitHistoryIndex <= 0) return this.showMessage('没有上一位已完成拜访的好友');
    this.friendVisitHistoryIndex--;
    this.visitOwner(this.friendVisitHistory[this.friendVisitHistoryIndex]);
  }

  private applyFriendSummary(friend: FriendSummary, pageCursor?: string): void {
    this.friendIds.add(friend.ownerId);
    this.friendLabels.set(friend.ownerId, friend.displayName);
    this.friendProfiles.set(friend.ownerId, { avatarId: friend.avatarId, level: friend.level });
    this.friendAuthority.set(friend.ownerId, {
      actionMask: friend.actionMask,
      status: friend.opportunityStatus,
      relationEpoch: friend.relationEpoch,
      opportunityRevision: friend.opportunityRevision,
      theftEligibleTargets: friend.theftEligibleTargets.map((target) => ({ ...target })),
    });
    if (pageCursor !== undefined) this.friendPageCursorByOwner.set(friend.ownerId, pageCursor);
    if (friend.opportunityStatus === 'READY') this.friendReadyActionMasks.set(friend.ownerId, friend.actionMask);
  }

  private async refreshReadyFriendAuthority(ownerId: string): Promise<void> {
    if (!this.friendService || !this.ownOwnerId || ownerId === this.ownOwnerId) return;
    this.friendRosterRequestsInFlight += 1;
    try {
      let cursor = this.friendPageCursorByOwner.get(ownerId) ?? '';
      let expectedRevision = this.friendRosterRevision;
      // Known page cursors finish in one request. The bounded scan is only for
      // a server-owned next-opportunity target not yet present in loaded pages.
      for (let pageIndex = 0; pageIndex < 20; pageIndex += 1) {
        const page = await this.friendService.listPage(this.ownOwnerId, cursor, 20);
        if (expectedRevision !== 0n && page.rosterRevision !== expectedRevision) {
          await this.loadFriendRoster(true, false, false);
          return;
        }
        expectedRevision = page.rosterRevision;
        const friend = page.friends.find((candidate) => candidate.ownerId === ownerId);
        if (friend) {
          this.applyFriendSummary(friend, cursor);
          this.friendRosterRevision = page.rosterRevision;
          this.friendRosterState = 'ready';
          this.render();
          return;
        }
        if (this.friendPageCursorByOwner.has(ownerId) || !page.nextCursor) break;
        cursor = page.nextCursor;
      }
      // Relation/projected-page drift: refresh page one. The owner remains
      // UNKNOWN, so controls stay fail closed until a later authoritative pull.
      await this.loadFriendRoster(true, false, false);
    } catch {
      // Keep the immediate UNKNOWN revocation. A transport failure cannot
      // restore stale targets; ordinary polling/hints will retry authority.
      this.render();
    } finally {
      this.friendRosterRequestsInFlight = Math.max(0, this.friendRosterRequestsInFlight - 1);
    }
  }

  private async loadFriendRoster(silent = false, append = false, announceGains = false): Promise<void> {
    if (!this.friendService || (append && !this.friendRosterNextCursor)) return;
    const request = ++this.friendRosterRequest;
    this.friendRosterRequestsInFlight += 1;
    const hadReadyRoster = this.friendRosterState === 'ready';
    if (!silent && !append) {
      this.friendRosterState = 'loading';
      this.friendRosterError = '';
      this.render();
    }
    try {
      const page = await this.friendService.listPage(this.ownOwnerId, append ? this.friendRosterNextCursor : '', 20);
      if (request !== this.friendRosterRequest) return;
      if (append && this.friendRosterRevision !== 0n && page.rosterRevision !== this.friendRosterRevision) {
        throw new Error('好友列表版本已变化，请重新加载');
      }
      const preservePagedBaseline = !append && preservePagedRosterBaseline(
        silent,
        hadReadyRoster,
        this.friendRosterRevision,
        page.rosterRevision,
      );
      const previousNextCursor = this.friendRosterNextCursor;
      if (!append && !preservePagedBaseline) {
        this.friendIds.clear();
        this.friendLabels.clear();
        this.friendProfiles.clear();
        this.friendAuthority.clear();
        this.friendPageCursorByOwner.clear();
      }
      const gainedNotices: string[] = [];
      const refreshedOwnerIds = new Set<string>();
      for (const friend of page.friends) {
        if (friend.ownerId === this.ownOwnerId) continue;
        refreshedOwnerIds.add(friend.ownerId);
        const previousReadyMask = this.friendReadyActionMasks.get(friend.ownerId);
        const gains = gainedFriendOpportunities(previousReadyMask, friend.opportunityStatus, friend.actionMask);
        if (announceGains && gains.length > 0) {
          const actions = gains.map((gain) => gain === 'THEFT' ? '可以偷菜了' : '需要浇水了');
          gainedNotices.push(`${friend.displayName} ${actions.join('、')}`);
        }
        this.applyFriendSummary(friend, append ? this.friendRosterNextCursor : '');
      }
      if (!append && !preservePagedBaseline) {
        for (const ownerId of this.friendReadyActionMasks.keys()) {
          if (!refreshedOwnerIds.has(ownerId)) this.friendReadyActionMasks.delete(ownerId);
        }
      }
      this.friendRosterNextCursor = append
        ? page.nextCursor
        : refreshedRosterNextCursor(preservePagedBaseline, previousNextCursor, page.nextCursor);
      this.friendRosterRevision = page.rosterRevision;
      this.friendRosterState = 'ready';
      this.friendRosterError = '';
      this.render();
      if (gainedNotices.length > 0) {
        const shown = gainedNotices.slice(0, 2).join('；');
        const remaining = gainedNotices.length > 2 ? `；另有 ${gainedNotices.length - 2} 位好友` : '';
        this.showToast(`好友机会：${shown}${remaining}`);
      }
    } catch (error) {
      if (request !== this.friendRosterRequest) return;
      // 好友流程失败只展示可诊断错误,不把整个应用退回登录:WS 会话失效有
      // 自己的 returnToAuth 路径,HTTP 401 常与 Cookie 域/端口混用有关,
      // 不能直接当作会话失效处理。
      const status = error instanceof HttpFriendRequestError ? error.status : undefined;
      const message = error instanceof Error ? error.message : '好友列表暂时不可用';
      const detail = status ? `${message}（HTTP ${status}）` : message;
      if (append && status === 409) {
        this.friendRosterNextCursor = '';
        this.friendRosterRevision = 0n;
        void this.loadFriendRoster(false, false);
        return;
      }
      if (!silent || !hadReadyRoster) {
        this.friendRosterState = 'error';
        this.friendRosterError = detail;
        this.render();
      }
      if (!silent) this.showMessage(`好友列表加载失败：${detail}`, true);
    } finally {
      this.friendRosterRequestsInFlight = Math.max(0, this.friendRosterRequestsInFlight - 1);
    }
  }

  private pollFriendRosterOpportunities(): void {
    if (!shouldRefreshFriendRoster({
      hasFriendService: this.friendService !== null,
      hasOwner: Boolean(this.ownOwnerId),
      friendCount: this.friendIds.size,
      connectionReady: this.ws.status === 'InFarm',
      requestsInFlight: this.friendRosterRequestsInFlight,
      pageVisible: typeof document === 'undefined' || document.visibilityState !== 'hidden',
    })) return;
    void this.loadFriendRoster(true, false, true);
  }

  private async loadFriendOpportunity(
    bootstrapGeneration: number,
    attempt = 0,
    requestGeneration?: number,
  ): Promise<void> {
    if (!this.friendService || !this.ownOwnerId) return;
    const activeRequestGeneration = requestGeneration ?? this.friendOpportunityFlow.beginRequest();
    if (!this.friendOpportunityFlow.isCurrentRequest(activeRequestGeneration)) return;
    try {
      const opportunity = await this.friendService.nextOpportunity(this.ownOwnerId);
      if (
        bootstrapGeneration !== this.recoveryBootstrapGeneration
        || !this.friendOpportunityFlow.isCurrentRequest(activeRequestGeneration)
      ) return;
      if (opportunity.status === 'UNKNOWN') {
        throw new Error('好友互动机会权威查询暂不可用');
      }
      if (opportunity.status === 'FOUND') {
        this.friendIds.add(opportunity.friend.ownerId);
        this.friendLabels.set(opportunity.friend.ownerId, opportunity.friend.displayName);
        this.friendProfiles.set(opportunity.friend.ownerId, {
          avatarId: opportunity.friend.avatarId,
          level: opportunity.friend.level,
        });
        this.friendAuthority.set(opportunity.friend.ownerId, {
          actionMask: opportunity.friend.actionMask,
          status: opportunity.friend.opportunityStatus,
          relationEpoch: opportunity.friend.relationEpoch,
          opportunityRevision: opportunity.friend.opportunityRevision,
          theftEligibleTargets: opportunity.friend.theftEligibleTargets.map((target) => ({ ...target })),
        });
      }
      this.friendOpportunityState = 'ready';
      this.tryCompleteHomeReady();
      this.render();
    } catch (error) {
      if (
        bootstrapGeneration !== this.recoveryBootstrapGeneration
        || !this.friendOpportunityFlow.isCurrentRequest(activeRequestGeneration)
      ) return;
      // 同上:好友机会加载失败是可诊断的局部错误,不退回登录。
      const status = error instanceof HttpFriendRequestError ? error.status : undefined;
      const message = error instanceof Error ? error.message : '好友机会暂时不可用';
      this.friendOpportunityState = 'error';
      this.root.dataset.friendOpportunity = 'temporarily-unavailable';
      this.showMessage(`好友机会加载失败：${status ? `${message}（HTTP ${status}）` : message}`, true);
      this.tryCompleteHomeReady();
      this.render();
      const retryMs = Math.min(1_000 * 2 ** attempt, 10_000);
      window.setTimeout(() => {
        if (
          bootstrapGeneration === this.recoveryBootstrapGeneration
          && this.friendOpportunityFlow.isCurrentRequest(activeRequestGeneration)
          && this.ws.status !== 'Disconnected'
          && this.friendOpportunityState !== 'ready'
        ) void this.loadFriendOpportunity(bootstrapGeneration, attempt + 1, activeRequestGeneration);
      }, retryMs);
    }
  }

  private tryCompleteHomeReady(): void {
    // 好友机会端点失败(error)不阻塞 homeReady:自家农场仍可经营,机会
    // 面板显示局部错误并继续退避重试。
    if (this.homeReady || this.ws.status !== 'InFarm') return;
    if (this.friendOpportunityState !== 'ready' && this.friendOpportunityState !== 'error') return;
    this.homeReady = true;
    this.root.dataset.homeReady = 'true';
    this.root.dataset.friendOpportunity = this.friendOpportunityState === 'ready' ? 'ready' : 'temporarily-unavailable';
  }

  private async refreshFriendRosterSilently(): Promise<void> {
    if (!this.commandService || this.ws.status !== 'InFarm' || !this.ownOwnerId) return;
    await this.loadFriendRoster(true);
  }

  private visitOwner(ownerId: string): void {
    this.pendingTheftConfirmation = null;
    // InFarm:常规导航。Recovering 且目标即本轮恢复目标:重连后自动回访。
    // Authenticated 且 Join 空闲:Join 被拒后的手动恢复入口。其余状态拒绝。
    const status = this.ws.status;
    const recoveryTargeting = status === 'Recovering' && ownerId === this.recoveryTargetOwnerId;
    const idleAuthenticated = status === 'Authenticated' && this.ws.joinPhase === 'NO_JOIN';
    if (status !== 'InFarm' && !recoveryTargeting && !idleAuthenticated) {
      return this.showMessage('连接尚未就绪', true);
    }
    // A newer explicit navigation always wins over an in-flight recommendation.
    this.friendOpportunityFlow.invalidateRequests();
    // Re-selecting the farm already displayed is navigation idempotency, not
    // a Fresh Join. Sending a new operation here would race the still-active
    // Presence with a second same-session fence and needlessly enter recovery.
    if (ownerId === this.currentOwnerId && status === 'InFarm') {
      // The visible Snapshot can already be home while a just-accepted share
      // still holds a stale auto-navigation recovery target. An explicit Home
      // click is therefore idempotent on the live subscription but must still
      // win for the next reconnect.
      this.recoveryTargetOwnerId = ownerId;
      this.pendingShareOwnerId = null;
      this.closeFriends();
      this.closeUtility();
      return;
    }
    if (ownerId !== this.ownOwnerId && !this.friendIds.has(ownerId)) {
      return this.showMessage('该农场尚未建立好友关系', true);
    }
    this.closeFriends();
    this.closeUtility();
    const previousOwnerId = this.currentOwnerId;
    // Intentional navigation wins over a stale recovery/share auto-rejoin target.
    this.recoveryTargetOwnerId = ownerId;
    this.pendingShareOwnerId = null;
    try {
      this.ws.joinFarm(ownerId);
    } catch (error) {
      this.recoveryTargetOwnerId = previousOwnerId;
      return this.showMessage(error instanceof Error ? error.message : '切换农场失败', true);
    }
    // 记录导航意图,JOIN_FARM REJECTED 时归因并自动回退到上一农场。
    this.pendingNavigation = {
      targetOwnerId: ownerId,
      previousOwnerId,
      operationId: this.ws.activeJoinOperationId ?? '',
    };
    const appShell = this.root.querySelector<HTMLElement>('[data-app-shell]');
    if (appShell) {
      appShell.dataset.lastJoinTarget = ownerId;
      appShell.dataset.lastJoinStatus = this.ws.status;
    }
    this.render();
  }

  private theftCycleKey(ownerId: string, cropCycleId: string | undefined): string {
    return cropCycleId ? `${ownerId}:${cropCycleId}` : '';
  }

  private theftCycleConfirmed(ownerId: string, cropCycleId: string | undefined): boolean {
    const key = this.theftCycleKey(ownerId, cropCycleId);
    return key !== '' && this.confirmedTheftCycles.has(key);
  }

  private theftTargetEligible(ownerId: string, plot: PlotState): boolean {
    const authority = this.friendAuthority.get(ownerId);
    const trustedNow = this.serverNow();
    if (!authority || trustedNow === null) return false;
    return isExactTheftPlotEligible(
      plot,
      authority.status,
      authority.theftEligibleTargets,
      trustedNow,
      this.theftCycleConfirmed(ownerId, plot.cropCycleId),
    );
  }

  private consumeTheftTarget(ownerId: string, cropCycleId: string | undefined): void {
    if (this.pendingTheftConfirmation?.ownerId === ownerId && this.pendingTheftConfirmation.cropCycleId === cropCycleId) {
      this.pendingTheftConfirmation = null;
    }
    const key = this.theftCycleKey(ownerId, cropCycleId);
    if (key) this.confirmedTheftCycles.add(key);
    const authority = this.friendAuthority.get(ownerId);
    if (!authority || !cropCycleId) return;
    const remaining = authority.theftEligibleTargets.filter((target) => target.cropCycleId !== cropCycleId);
    this.friendAuthority.set(ownerId, {
      ...authority,
      actionMask: remaining.length === 0 ? authority.actionMask & ~FriendActionMask.THEFT : authority.actionMask,
      theftEligibleTargets: remaining,
    });
  }

  private handleTheftAuthorityResult(result: CommandResult): void {
    const lookup = result.operationLookup?.originalCommandType === 'THEFT' ? result.operationLookup : undefined;
    const terminal = result.commandType === 'THEFT' ? result : lookup?.completedResult;
    const terminalStatus = terminal?.status ?? lookup?.completedStatus;
    if (!terminalStatus || terminalStatus === 'PROCESSING' || terminalStatus === 'ACCEPTED') return;
    const operationId = lookup?.originalOperationId ?? terminal?.operationId ?? result.operationId;
    const tracked = this.theftOperationTargets.get(operationId);
    const errorCode = terminal?.errorCode ?? lookup?.completedErrorCode;
    const errorMessage = terminal?.message ?? lookup?.completedErrorMessage;
    const ownerId = terminal?.targetFarmId ?? lookup?.completedTargetFarmId ?? tracked?.ownerId ?? this.currentOwnerId;
    const cropCycleId = terminal?.cropCycleId ?? tracked?.cropCycleId;
    const zeroBasedPlotId = tracked?.plotId ?? (terminal?.plotId === undefined ? undefined : Math.max(0, terminal.plotId - 1));
    const plotLabel = zeroBasedPlotId === undefined ? '' : `（地块 #${zeroBasedPlotId}）`;

    const presentation = theftTerminalPresentation(terminalStatus, errorCode, errorMessage, plotLabel);
    if (presentation.consumesTarget) this.consumeTheftTarget(ownerId, cropCycleId);
    this.showMessage(presentation.message, presentation.error);
    if (terminalStatus === 'REJECTED' || terminalStatus === 'UNAVAILABLE') {
      const appShell = this.root.querySelector<HTMLElement>('[data-app-shell]');
      if (appShell) {
        appShell.dataset.lastErrorCode = errorCode ?? '';
        appShell.dataset.lastErrorOp = operationId;
        appShell.dataset.lastErrorRequest = result.requestId ?? '';
      }
    }
    this.theftOperationTargets.delete(operationId);
    if (this.selectedPlotId === (zeroBasedPlotId === undefined ? null : `plot-${zeroBasedPlotId}`)) this.selectedPlotId = null;
    this.playerBootstrapRefresh.markDirty();
    this.pumpPlayerBootstrapRefresh();
    this.render();
    void this.loadFriendRoster(true, false, false);
  }

  private tryNavigatePendingShare(): boolean {
    const target = this.pendingShareOwnerId;
    if (target === null) return false;
    if (!canNavigateAcceptedShareNow({
      connectionStatus: this.ws.status,
      joinPhase: this.ws.joinPhase,
      ownOwnerId: this.ownOwnerId,
      currentOwnerId: this.currentOwnerId,
      pendingOwnerId: target,
      friendKnown: this.friendIds.has(target),
    })) return false;
    this.pendingShareOwnerId = null;
    this.visitOwner(target);
    return true;
  }

  private async preparePendingShare(): Promise<void> {
    if (!this.pendingShareToken) return;
    if (!this.shareResolver) {
      this.showMessage('检测到分享链接，但当前环境尚未接入服务端分享解析', true);
      return;
    }
    try {
      const preview = await this.shareResolver.resolve(this.pendingShareToken);
      this.showMessage(`分享链接已解析，邀请来自 ${preview.inviterLabel}，正在确认好友关系…`);
      const resolution = await this.shareResolver.accept(this.pendingShareToken, this.ownOwnerId);
      // A share URL is a one-shot navigation intent even though its server
      // token supports multi-accept across different recipients. Keeping it
      // locally after acceptance would replay auto-navigation on every WSS
      // reconnect and override an explicit return-home choice.
      this.pendingShareToken = null;
      this.pendingShareOwnerId = resolution.targetOwnerId;
      this.friendIds.add(resolution.targetOwnerId);
      // inviter_label is share UX only, never a friend profile. The next
      // roster response or FarmSnapshot must supply display/avatar/level.
      this.showMessage(`好友关系已确认，邀请来自 ${resolution.inviterLabel}`);
      await this.loadFriendRoster();
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '分享链接验证失败', true);
    }
  }

  private async revokeFriend(friendOwnerId: string): Promise<void> {
    if (!this.friendService) return this.showMessage('当前环境尚未接入好友关系服务', true);
    try {
      await this.friendService.revoke(this.ownOwnerId, friendOwnerId);
      this.friendIds.delete(friendOwnerId);
      this.friendLabels.delete(friendOwnerId);
      if (this.currentOwnerId === friendOwnerId) this.visitOwner(this.ownOwnerId);
      this.render();
      this.showMessage('好友关系已解除');
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '解除好友失败', true);
    }
  }

  private async shareFarm(): Promise<void> {
    if (this.currentOwnerId !== this.ownOwnerId) return this.showMessage('请先回到我的农场再分享', true);
    if (!this.shareCreator) return this.showMessage('当前环境尚未接入服务端分享创建接口', true);
    try {
      const { url } = await this.shareCreator.create(this.ownOwnerId, window.location.origin);
      try {
        await navigator.clipboard?.writeText(url);
      } catch {
        // The URL remains visible in the message when clipboard permission is unavailable.
      }
      this.showMessage(`分享链接已生成：${url}`);
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '分享链接生成失败', true);
    }
  }

  private initialStagingAccount(): 'player-a' | 'player-b' {
    const explicit = new URL(window.location.href).searchParams.get('demo_account');
    if (explicit === 'player-a' || explicit === 'player-b') return explicit;
    try {
      const stored = window.localStorage.getItem('farm:staging_account');
      if (stored === 'player-a' || stored === 'player-b') return stored;
    } catch {
      // Storage can be unavailable in hardened/private browser contexts.
    }
    // A fresh browser profile opening a share is the recipient in the normal
    // two-player staging flow. Existing profiles keep their explicit account.
    return this.pendingShareToken ? 'player-b' : 'player-a';
  }

  private persistStagingAccount(): void {
    if (!this.stagingAccountSelect) return;
    try {
      window.localStorage.setItem('farm:staging_account', this.stagingAccountSelect.value);
    } catch {
      // The selected token still applies to the current page load.
    }
  }

  private ensurePersonalFarm(): boolean {
    if (this.currentOwnerId === this.ownOwnerId) return true;
    this.showMessage('好友农场当前为只读，先回到我的农场再操作', true);
    return false;
  }

  private async changeWebPassword(): Promise<void> {
    if (clientEnvironment.name !== 'web' || !(this.auth instanceof WebAuthProvider)) return;
    const username = this.utilityContentElement.querySelector<HTMLInputElement>('[data-change-username]')?.value.trim() ?? '';
    const currentPassword = this.utilityContentElement.querySelector<HTMLInputElement>('[data-change-current]')?.value ?? '';
    const nextPassword = this.utilityContentElement.querySelector<HTMLInputElement>('[data-change-next]')?.value ?? '';
    const confirmPassword = this.utilityContentElement.querySelector<HTMLInputElement>('[data-change-confirm]')?.value ?? '';
    if (!/^[a-zA-Z0-9_]{3,24}$/.test(username)) {
      this.showMessage('账号需要使用 3–24 位字母、数字或下划线', true);
      return;
    }
    if ([...currentPassword].length < 8 || [...nextPassword].length < 8 || [...nextPassword].length > 128) {
      this.showMessage('密码长度需要为 8–128 个字符', true);
      return;
    }
    if (nextPassword !== confirmPassword) {
      this.showMessage('两次输入的新密码不一致', true);
      return;
    }
    if (currentPassword === nextPassword) {
      this.showMessage('新密码不能与当前密码相同', true);
      return;
    }
    try {
      await this.auth.changePassword(username, currentPassword, nextPassword);
      if (this.webUsernameInput) this.webUsernameInput.value = username;
      this.returnToAuth('密码已修改，全部设备会话已失效，请用新密码重新登录。');
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '修改密码失败', true);
    }
  }

  private async logoutAllWebSessions(): Promise<void> {
    if (clientEnvironment.name !== 'web' || !(this.auth instanceof WebAuthProvider)) return;
    if (!window.confirm('确认退出全部设备？当前和其他浏览器上的登录都会立刻失效。')) return;
    try {
      await this.auth.logoutAll();
      this.returnToAuth('已退出全部设备，请重新登录。');
    } catch (error) {
      this.showMessage(error instanceof Error ? error.message : '退出全部设备失败', true);
    }
  }

  private returnToAuth(message: string): void {
    this.ws?.disconnect();
    // REVIEW P2-4:登出/切账号/断线清理展示重算 timer,避免残留 timer 在未登录态
    // 触发 render 的成熟重算。visibilitychange listener 保留(App 单例唯一,未登录
    // 时 render 对隐藏 app-shell 无害;移除会破坏重新登录后的恢复重算)。
    if (this.maturityTimer !== undefined) {
      window.clearTimeout(this.maturityTimer);
      this.maturityTimer = undefined;
    }
    if (this.farmClockProjectionTimer !== undefined) {
      window.clearTimeout(this.farmClockProjectionTimer);
      this.farmClockProjectionTimer = undefined;
    }
    this.closeFriends();
    this.closeUtility();
    this.friendIds.clear();
    this.friendLabels.clear();
    this.friendProfiles.clear();
    this.friendAuthority.clear();
    this.friendRosterNextCursor = '';
    this.friendRosterRevision = 0n;
    this.friendVisitHistory.length = 0;
    this.friendVisitHistoryIndex = -1;
    this.friendRosterState = 'idle';
    this.friendOpportunityState = 'idle';
    this.friendOpportunityFlow.reset();
    this.recoveryCycleActive = false;
    this.recoveryTargetOwnerId = '';
    this.homeReady = false;
    this.root.dataset.homeReady = 'false';
    this.root.querySelector<HTMLElement>('[data-app-shell]')?.setAttribute('hidden', '');
    this.authScreen.removeAttribute('hidden');
    this.loginButton.disabled = false;
    if (this.registerButton) this.registerButton.disabled = false;
    this.loginButton.textContent = '重新登录';
    if (this.webPasswordInput) this.webPasswordInput.value = '';
    if (clientEnvironment.name === 'web' && this.auth instanceof WebAuthProvider && this.webUsernameInput) {
      const lastUsername = this.auth.lastUsername();
      if (lastUsername) this.webUsernameInput.value = lastUsername;
    }
    this.setAuthMessage(message, true);
    this.render();
  }

  private setAuthMessage(message: string, error = false): void {
    this.authMessageElement.textContent = message;
    this.authMessageElement.classList.toggle('error', error);
  }
}
