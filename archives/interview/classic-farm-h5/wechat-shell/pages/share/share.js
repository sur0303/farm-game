Page({
  data: {
    shareToken: '',
  },

  onLoad(options) {
    this.shareToken = options && options.share_token ? options.share_token : '';
    this.setData({ shareToken: this.shareToken });
  },

  openGame() {
    const query = this.shareToken ? '?share_token=' + encodeURIComponent(this.shareToken) : '';
    wx.redirectTo({ url: '/pages/game/game' + query });
  },

  onShareAppMessage() {
    return {
      title: '来经典农场看看',
      path: this.shareToken
        ? '/pages/share/share?share_token=' + encodeURIComponent(this.shareToken)
        : '/pages/share/share',
    };
  },
});
