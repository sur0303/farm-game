// Classic Farm WeChat Mini-Program — Login page
// wx.login() → farm-gate auth → get ticket → navigate to farm

const { wxLoginToTicket, saveSession } = require('../../utils/auth');

Page({
  data: {
    loading: false,
    errorMessage: '',
  },

  onLoad() {
    // Auto-login if we have a cached session
    const app = getApp();
    if (app.globalData.connectionTicket) {
      this.navigateToFarm();
    }
  },

  handleLogin() {
    if (this.data.loading) return;
    this.setData({ loading: true, errorMessage: '' });

    wxLoginToTicket()
      .then((session) => {
        saveSession(session);
        const app = getApp();
        app.globalData.playerId = session.playerId;
        app.globalData.sessionId = session.sessionId;
        app.globalData.connectionTicket = session.connectionTicket;
        app.globalData.wssEndpoint = session.wssEndpoint;
        this.setData({ loading: false });
        this.navigateToFarm();
      })
      .catch((err) => {
        this.setData({
          loading: false,
          errorMessage: err.message || '登录失败，请重试',
        });
      });
  },

  navigateToFarm() {
    wx.redirectTo({ url: '/pages/index/index' });
  },
});
