// Classic Farm WeChat Mini-Program entry
const config = require('./utils/config');

App({
  globalData: {
    config,
    playerId: null,
    sessionId: null,
    connectionTicket: null,
    wssEndpoint: null,
    shareToken: null,       // share token from opened link
    inviterPlayerId: null,  // who shared to us
  },

  onLaunch(options) {
    // Read share_token from opened share link
    if (options && options.query && options.query.share_token) {
      this.globalData.shareToken = options.query.share_token;
    }
    // Restore cached session
    const session = wx.getStorageSync('farm_session');
    if (session && session.connectionTicket) {
      this.globalData.playerId = session.playerId;
      this.globalData.sessionId = session.sessionId;
      this.globalData.connectionTicket = session.connectionTicket;
      this.globalData.wssEndpoint = session.wssEndpoint;
    }
  },

  onShow(options) {
    // Also check onShow in case app was backgrounded and re-opened via share
    if (options && options.query && options.query.share_token) {
      this.globalData.shareToken = options.query.share_token;
    }
  },
});
