// WeChat Mini-Program configuration
// 直连 WebSocket 模式:farm-gate 直接暴露 /ws 端点(无需 TCONND/gateproxy)。
// 通过 SSH 隧道访问内网服务:ssh -N -L 8082:localhost:8082 developer_026@198.18.0.6

module.exports = {
  // Server endpoints
  apiBaseUrl: 'http://localhost:8082',  // farm-gate HTTP port (登录/分享)
  wsBaseUrl: 'ws://localhost:8082',     // farm-gate WebSocket 直连
  wsPath: '/ws',                        // WS Upgrade 路径

  // Game config
  gameName: '经典农场',
  protocolVersion: 2,
  clientBuild: 'farm-wechat/0.1',

  // WeChat AppID (fill in for production)
  appId: '',  // TODO: fill in your WeChat AppID

  // Debug mode
  debug: true,
};
