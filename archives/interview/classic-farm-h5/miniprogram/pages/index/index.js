// Classic Farm WeChat Mini-Program — Main farm page
// WebSocket connect → display farm → handle commands + share/friend features

const { FarmGateWSClient } = require('../../utils/ws-client');
const { saveSession, getSession, clearSession } = require('../../utils/auth');
const config = require('../../utils/config');

const TOOL_ICONS = { plant: '🥕', water: '💧', harvest: '👋' };

Page({
  data: {
    status: 'disconnected',
    statusText: '未连接',
    plots: [],
    wallet: 0,
    seeds: 0,
    carrots: 0,
    activeTool: null,
    selectedPlot: null,
    message: '等待连接...',
    isError: false,
    farmVersion: 0,
    friendList: [],          // [{playerId, label}]
    isVisitingFriend: false, // true when viewing a friend's farm
    currentOwnerId: '100001', // whose farm we're viewing
    ownPlayerId: '100001',
    inviterLabel: '',        // label of who shared to us (for accept page)
    showAcceptShare: false,  // show the "accept share" modal
    shareToken: '',          // pre-fetched share token for onShareAppMessage
  },

  onLoad() {
    const app = getApp();
    // Check if we came via a share link
    if (app.globalData.shareToken) {
      this.data.shareToken = app.globalData.shareToken;
      // Resolve the share to show who invited us
      this.resolveShare(app.globalData.shareToken);
      app.globalData.shareToken = null; // consume it
    }

    this.ws = new FarmGateWSClient();
    this.ws.on('open', () => this.onSocketOpen());
    this.ws.on('hello', (hello) => this.onServerHello(hello));
    this.ws.on('data', (data) => this.onData(data));
    this.ws.on('close', () => this.onClose());
    this.ws.on('error', (err) => this.onError(err));
    this.connectToFarm();
  },

  onUnload() {
    if (this.ws) this.ws.disconnect();
  },

  connectToFarm() {
    const session = getSession();
    if (!session || !session.connectionTicket) {
      wx.redirectTo({ url: '/pages/login/login' });
      return;
    }
    this.setData({ status: 'connecting', statusText: '连接中...' });
    this.ws.connect(session.connectionTicket, session.sessionId);
  },

  onSocketOpen() {
    this.setData({ status: 'authenticating', statusText: '认证中...' });
  },

  onServerHello(hello) {
    this.setData({
      status: 'authenticated',
      statusText: '已连接',
      message: `已连接 (session ${hello.sessionId || 'ok'})`,
    });
    // 进入自己的农场
    const session = getSession();
    const ownerIdStr = String((session && session.playerId) || this.data.ownPlayerId);
    const ownerIdNum = parseInt(ownerIdStr, 10) || 0;
    this.setData({
      ownPlayerId: ownerIdStr,
      currentOwnerId: ownerIdStr,
    });
    this.ws.sendCommand('JOIN_FARM', { target_owner_id: ownerIdNum });
    this.loadFriends();
    this.prepareShareToken();
  },

  /**
   * Pre-fetch a share token so onShareAppMessage can use it synchronously.
   * Tokens are tied to playerId and reusable across shares (server decides TTL).
   */
  prepareShareToken() {
    const session = getSession();
    if (!session || !session.playerId) return;
    wx.request({
      url: `${config.apiBaseUrl}/api/v1/share/create`,
      method: 'POST',
      header: { 'content-type': 'application/json' },
      data: { player_id: parseInt(session.playerId, 10) },
      success: (res) => {
        if (res.statusCode === 200 && res.data && res.data.share_token) {
          this.setData({ shareToken: res.data.share_token });
          console.log('[Share] token ready:', res.data.share_token);
        } else {
          console.warn('[Share] create failed:', res.statusCode, res.data);
        }
      },
      fail: (err) => console.warn('[Share] create error:', err),
    });
  },

  onData(msg) {
    // msg: { kind: 'command_result'|'server_push', result|push }
    if (msg.kind === 'server_push') {
      this.handleServerPush(msg.push);
    } else if (msg.kind === 'command_result') {
      this.handleCommandResult(msg.result);
    }
  },

  handleServerPush(push) {
    const { farm } = require('../../utils/ws-client');
    const PushType = { FARM_SNAPSHOT: 1, FARM_CHANGE: 2, KICK: 3, SYSTEM_NOTICE: 4, OPERATION_RESULT: 5, RESYNC_REQUIRED: 6 };
    if (push.pushType === PushType.FARM_SNAPSHOT) {
      try {
        const snap = farm.FarmSnapshot.decode(push.payload);
        this.renderSnapshot(snap);
      } catch (e) {
        console.error('[Farm] FarmSnapshot decode failed', e);
      }
    } else if (push.pushType === PushType.FARM_CHANGE) {
      try {
        const change = farm.FarmChange.decode(push.payload);
        this.renderSnapshot(change);
      } catch (e) {
        console.error('[Farm] FarmChange decode failed', e);
      }
    } else if (push.pushType === PushType.SYSTEM_NOTICE) {
      this.setData({ message: '系统通知' });
    }
  },

  handleCommandResult(result) {
    // result.status: 1=SUCCEEDED, 2=PROCESSING, 3=NO_CHANGE, 4=REJECTED, 5=UNAVAILABLE, 6=ACCEPTED
    const statusName = ['UNSPECIFIED', 'SUCCEEDED', 'PROCESSING', 'NO_CHANGE', 'REJECTED', 'UNAVAILABLE', 'ACCEPTED'][result.status] || result.status;
    this.setData({ message: `命令 ${statusName}` });

    // JOIN_FARM accepted → payload 内嵌 FarmSnapshot
    if ((result.status === 6 || result.status === 1) && result.payload && result.payload.length > 0) {
      try {
        const { farm } = require('../../utils/ws-client');
        console.log('[Farm] decoding payload, len=', result.payload.length, 'first bytes=', Array.from(result.payload.slice(0, 10)));
        const snap = farm.FarmSnapshot.decode(result.payload);
        console.log('[Farm] decoded snapshot:', JSON.stringify({
          ownerId: snap.ownerId && snap.ownerId.toString(),
          snapshotTimeMs: snap.snapshotTimeMs && snap.snapshotTimeMs.toString(),
          farmVersion: snap.farmVersion && snap.farmVersion.toString(),
          plotsIsArray: Array.isArray(snap.plots),
          plotsLen: (snap.plots || []).length,
          plots: (snap.plots || []).slice(0, 3).map(p => ({ plotIndex: p.plotIndex, plotVersion: p.plotVersion && p.plotVersion.toString() })),
        }));
        this.renderSnapshot(snap);
      } catch (e) {
        console.warn('[Farm] CommandResult payload is not FarmSnapshot:', e.message, e.stack);
      }
    }
  },

  renderSnapshot(snapshot) {
    const farm = snapshot.farm || snapshot;
    const plots = (farm.plots || []).map((p) => ({
      id: p.plotId !== undefined ? p.plotId : p.plot_id,
      status: p.status || 'EMPTY',
      cropType: p.cropType || p.crop_type || '',
      watered: !!(p.wateredAt || p.watered_at),
      mature: ((p.matureAt || p.mature_at) || 0) > 0 && Date.now() >= ((p.matureAt || p.mature_at) || 0),
    }));
    this.setData({
      plots,
      wallet: farm.wallet || 0,
      seeds: (farm.inventory || {}).seed || 0,
      carrots: (farm.inventory || {}).carrot || 0,
      farmVersion: farm.farmVersion || farm.farm_version || 0,
    });
  },

  handleAck(ack) {
    this.setData({ message: `命令确认: ${ack.commandType || ack.command_type}` });
    if (ack.result) this.renderSnapshot(ack.result);
  },

  handleNotify(notify) {
    this.setData({ message: notify.text || '收到通知' });
  },

  onClose() {
    this.setData({ status: 'disconnected', statusText: '已断开', isError: true, message: '连接已断开' });
  },

  onError(err) {
    this.setData({ status: 'error', statusText: '错误', isError: true, message: err.message || '连接错误' });
  },

  // ---- Share & Friend ----

  resolveShare(token) {
    wx.request({
      url: `${config.apiBaseUrl}/api/v1/share/resolve?token=${encodeURIComponent(token)}`,
      success: (res) => {
        if (res.statusCode === 200 && res.data) {
          this.setData({
            inviterLabel: res.data.inviter_label,
            showAcceptShare: true,
          });
        }
      },
    });
  },

  handleAcceptShare() {
    const session = getSession();
    if (!session || !session.connectionTicket) return;
    wx.request({
      url: `${config.apiBaseUrl}/api/v1/share/accept`,
      method: 'POST',
      header: { 'content-type': 'application/json' },
      data: {
        share_token: this.data.shareToken,
        viewer_id: parseInt(this.data.ownPlayerId, 10),
      },
      success: (res) => {
        if (res.statusCode === 200 && res.data) {
          this.setData({ showAcceptShare: false });
          this.showMsg(`已加为好友，欢迎访问 ${res.data.inviter_label} 的农场`);
          this.loadFriends();
          // Visit the friend's farm
          this.visitFarm(res.data.target_owner_id);
        }
      },
    });
  },

  handleCancelShare() {
    this.setData({ showAcceptShare: false });
  },

  loadFriends() {
    // For MVP: use hardcoded friends. Production: load from server.
    this.setData({
      friendList: [
        { playerId: '100002', label: '好友-玩家2' },
        { playerId: '100003', label: '好友-玩家3' },
      ],
    });
  },

  visitFriend(e) {
    const playerId = e.currentTarget.dataset.id;
    this.visitFarm(playerId);
  },

  visitFarm(ownerId) {
    this.setData({
      isVisitingFriend: ownerId !== this.data.ownPlayerId,
      currentOwnerId: ownerId,
      message: ownerId === this.data.ownPlayerId ? '返回我的农场' : `正在访问好友 ${ownerId} 的农场`,
    });
    // Request the target owner's farm snapshot
    this.ws.sendCommand('JOIN_FARM', { owner_id: ownerId });
  },

  backToMyFarm() {
    this.setData({ isVisitingFriend: false, currentOwnerId: this.data.ownPlayerId });
    this.ws.sendCommand('JOIN_FARM', {});
  },

  onShareAppMessage() {
    const session = getSession();
    if (!session || !session.playerId) {
      return {
        title: '经典农场',
        path: '/pages/login/login',
      };
    }
    const token = this.data.shareToken;
    if (!token) {
      // Token not ready yet — re-fetch and share a generic entry link
      this.prepareShareToken();
      return {
        title: '经典农场 — 来看看我的农场！',
        path: '/pages/login/login',
      };
    }
    return {
      title: `经典农场 — 来看看我的农场！`,
      path: `/pages/login/login?share_token=${encodeURIComponent(token)}`,
    };
  },

  // ---- Tool interactions ----

  selectTool(e) {
    const tool = e.currentTarget.dataset.tool;
    this.setData({ activeTool: this.data.activeTool === tool ? null : tool });
  },

  onPlotTap(e) {
    if (this.data.isVisitingFriend) {
      return this.showMsg('好友农场只能查看不能操作', true);
    }
    const plotId = e.currentTarget.dataset.id;
    this.setData({ selectedPlot: plotId });
    if (!this.data.activeTool) return;
    const plot = this.data.plots.find((p) => p.id === plotId);
    if (!plot) return;
    switch (this.data.activeTool) {
      case 'plant':
        if (plot.status !== 'EMPTY') return this.showMsg('这块地已有作物', true);
        this.ws.sendCommand('PLANT_CROP', { plot_id: plotId, crop_type: 'carrot' });
        break;
      case 'water':
        if (plot.status !== 'GROWING' || plot.watered) return this.showMsg('不需要浇水', true);
        this.ws.sendCommand('WATER_CROP', { plot_id: plotId });
        break;
      case 'harvest':
        if (!plot.mature) return this.showMsg('还没成熟', true);
        this.ws.sendCommand('HARVEST_CROP', { plot_id: plotId });
        break;
    }
    this.setData({ activeTool: null });
  },

  buySeed() { this.ws.sendCommand('BUY_OFFER', { offer_id: 'seed_pack' }); },
  sellCarrot() { this.ws.sendCommand('SELL_ITEM', { item: 'carrot', count: 1 }); },

  showMsg(text, isError) {
    this.setData({ message: text, isError: !!isError });
  },

  // ---- Plot helpers ----
  plotClass(plot) {
    let cls = 'plot';
    if (plot.status === 'GROWING') cls += ' growing';
    if (plot.status === 'EMPTY') cls += ' empty';
    if (plot.mature) cls += ' mature';
    if (plot.watered) cls += ' watered';
    if (this.data.selectedPlot === plot.id) cls += ' selected';
    if (this.data.activeTool === 'plant' && plot.status === 'EMPTY') cls += ' can-plant';
    if (this.data.activeTool === 'water' && plot.status === 'GROWING' && !plot.watered) cls += ' can-water';
    if (this.data.activeTool === 'harvest' && plot.mature) cls += ' can-harvest';
    return cls;
  },
  plotIcon(plot) {
    if (plot.status === 'EMPTY') return '＋';
    if (plot.mature) return '🥕';
    if (plot.watered) return '🌿';
    return '🌱';
  },
  plotLabel(plot) {
    if (plot.status === 'EMPTY') return '空地';
    if (plot.mature) return '已成熟';
    if (plot.watered) return '已浇水';
    return '生长中';
  },
});
