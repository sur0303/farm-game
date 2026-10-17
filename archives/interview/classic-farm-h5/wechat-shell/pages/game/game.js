const config = require('../../config');

Page({
  data: {
    h5Url: '',
    loading: true,
    errorMessage: '',
  },

  onLoad(options) {
    this.entryOptions = options || {};
    this.loginAndOpenH5();
  },

  retry() {
    this.setData({ loading: true, errorMessage: '' });
    this.loginAndOpenH5();
  },

  loginAndOpenH5() {
    wx.login({
      success: ({ code }) => {
        if (!code) {
          this.fail('微信没有返回登录 code');
          return;
        }
        wx.request({
          url: `${config.apiBaseUrl}/api/v1/auth/wx-miniprogram-login`,
          method: 'POST',
          header: { 'content-type': 'application/json' },
          data: { code },
          success: (response) => {
            const ticket = response.data && (response.data.launch_ticket || response.data.launchTicket);
            if (response.statusCode < 200 || response.statusCode >= 300 || !ticket) {
              this.fail('服务端没有返回有效的 launch_ticket');
              return;
            }
            const query = `launch_ticket=${encodeURIComponent(ticket)}`;
            const entryQuery = this.buildEntryQuery(this.entryOptions);
            const separator = config.h5BaseUrl.includes('?') ? '&' : '?';
            this.setData({
              h5Url: `${config.h5BaseUrl}${separator}${query}${entryQuery}`,
              loading: false,
              errorMessage: '',
            });
          },
          fail: () => this.fail('无法连接登录服务，请检查 HTTPS 域名和网络配置'),
        });
      },
      fail: () => this.fail('微信登录失败，请稍后重试'),
    });
  },

  buildEntryQuery(options) {
    if (!options || !options.share_token) return '';
    return `&share_token=${encodeURIComponent(options.share_token)}`;
  },

  onH5Load() {
    this.setData({ loading: false });
  },

  onH5Error() {
    this.fail('H5 页面加载失败，请检查 web-view 业务域名和 HTTPS 证书');
  },

  onShareAppMessage() {
    return {
      title: '来经典农场看看',
      path: '/pages/share/share',
    };
  },

  fail(message) {
    this.setData({ loading: false, errorMessage: message });
  },
});