// 优雅关闭(Graceful Shutdown)支持。
//
// 收到 SIGTERM/SIGINT 时:
//   1. 设置 draining 标志,新连接被拒绝(返回 503)
//   2. 给所有活跃连接发送 WS 标准 Close 帧(1012 Service Restart),告知客户端立即恢复
//   3. 等待 drainWait(默认 5s)让客户端有机会重连到其他实例
//   4. 强制关闭所有残留连接
//
// 客户端收到 Close(GoingAway) 后应主动断开并重连(CLB 会路由到其他健康实例)。
// 注意:Close 帧经 WriteControl 发送——这是 gorilla 明确支持与其他方法并发调用的
// 唯一写路径,不会与每连接 WritePump 的业务写竞争。

package websocket

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// gracefulState 是优雅关闭的全局状态。
type gracefulState struct {
	draining atomic.Bool
	conns    sync.Map // map[*websocket.Conn]struct{}
}

// noteConn 在连接建立时注册,关闭时注销。
func (g *gracefulState) noteConn(c *websocket.Conn) func() {
	g.conns.Store(c, struct{}{})
	return func() { g.conns.Delete(c) }
}

// drain 触发优雅关闭。
// drainWait 是等待客户端重连的时间。
// 返回实际关闭的连接数。
func (s *Server) drain(drainWait time.Duration) int {
	if !s.graceful.draining.CompareAndSwap(false, true) {
		return 0 // 已经在 drain
	}
	logger := s.cfg.Logger
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.DrainTotal.Inc()
		s.cfg.Metrics.Lifecycle.Set(0)
		s.cfg.Metrics.DrainSessions.Set(float64(s.ActiveConnections()))
	}
	logger.Printf("[WS] entering drain mode, waiting %s before closing all connections", drainWait)

	// The notice uses the normal bounded writer, preserving response/push
	// ordering. A short flush budget lets WritePump put it on the wire before
	// the protocol-level 1012 fallback.
	if notifier, ok := s.cfg.Downlink.(drainNotifier); ok {
		now := time.Now()
		drainID := fmt.Sprintf("%x", now.UnixNano())
		notices := notifier.EnqueueDrainNotice(drainID, 2*time.Second, now.Add(drainWait))
		if notices > 0 {
			logger.Printf("[WS] queued DrainNotice to %d connections", notices)
			time.Sleep(100 * time.Millisecond)
		}
	}

	// 1012 明确表示服务重启。客户端可安全进入现有的重新认证 +
	// RecoveryBarrier 流程；1001 无法区分用户离开和实例摘流。
	closed := 0
	s.graceful.conns.Range(func(key, _ any) bool {
		conn, ok := key.(*websocket.Conn)
		if !ok {
			return true
		}
		// 发送 Close frame,1001 Going Away。
		// WriteControl 是 gorilla 官方支持并发调用的写路径,不与 WritePump 竞争。
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseServiceRestart, "server draining; reconnect"),
			time.Now().Add(2*time.Second))
		closed++
		return true
	})
	logger.Printf("[WS] sent close(ServiceRestart) to %d connections", closed)

	// 等待客户端主动断开
	time.Sleep(drainWait)

	// 强制关闭剩余连接
	forceClosed := 0
	s.graceful.conns.Range(func(key, _ any) bool {
		conn, ok := key.(*websocket.Conn)
		if !ok {
			return true
		}
		_ = conn.Close()
		forceClosed++
		return true
	})
	logger.Printf("[WS] force-closed %d remaining connections", forceClosed)
	return closed + forceClosed
}

// isDraining 返回当前是否在 drain 模式。
func (s *Server) isDraining() bool {
	return s.graceful.draining.Load()
}
