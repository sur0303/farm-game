// WeChat Mini-Program auth helper
// Flow: wx.login() → POST code to farm-gate → get connection_ticket

const config = require('./config');

/**
 * Perform WeChat login and get farm connection ticket.
 * @returns {Promise<{playerId: number, sessionId: string, connectionTicket: string, wssEndpoint: string}>}
 */
function wxLoginToTicket() {
  return new Promise((resolve, reject) => {
    wx.login({
      success: ({ code }) => {
        if (!code) {
          reject(new Error('wx.login() returned no code'));
          return;
        }
        exchangeCodeForTicket(code).then(resolve, reject);
      },
      fail: (err) => reject(new Error('wx.login failed: ' + JSON.stringify(err))),
    });
  });
}

/**
 * Exchange WeChat login code for farm connection ticket.
 * @param {string} code - wx.login() code
 * @returns {Promise<{playerId: number, sessionId: string, connectionTicket: string, wssEndpoint: string}>}
 */
function exchangeCodeForTicket(code) {
  return new Promise((resolve, reject) => {
    wx.request({
      url: `${config.apiBaseUrl}/api/v1/auth/wx-miniprogram-login`,
      method: 'POST',
      header: { 'content-type': 'application/json' },
      data: { code, client_build: config.clientBuild },
      success: (res) => {
        if (res.statusCode >= 200 && res.statusCode < 300 && res.data) {
          const d = res.data;
          resolve({
            playerId: d.player_id || d.playerId || 0,
            sessionId: d.session_id || d.sessionId || '',
            connectionTicket: d.connection_ticket || d.connectionTicket || '',
            wssEndpoint: d.wss_endpoint || d.wssEndpoint || '',
          });
        } else {
          reject(new Error(`farm-gate auth failed: ${res.statusCode} ${JSON.stringify(res.data)}`));
        }
      },
      fail: (err) => reject(new Error('cannot reach farm-gate: ' + JSON.stringify(err))),
    });
  });
}

/**
 * Save session to local storage for reuse across app launches.
 */
function saveSession(session) {
  wx.setStorageSync('farm_session', session);
}

/**
 * Get cached session.
 */
function getSession() {
  return wx.getStorageSync('farm_session');
}

/**
 * Clear cached session.
 */
function clearSession() {
  wx.removeStorageSync('farm_session');
}

module.exports = { wxLoginToTicket, saveSession, getSession, clearSession };
