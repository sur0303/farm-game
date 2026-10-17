// Package websocket 提供 FarmGate 的直连 WebSocket 接入层。
//
// 与 gateproxy transport 并列,通过 GATE_TRANSPORT=websocket 启用。
// 复用 Gate.HandleBinary 的传输中立接口。
//
// 并发模型(正确性优先):
//   - 每连接恰好一个 ReadPump(收帧 + HandleBinary)和一个 WritePump(独占该连接所有网络写)。
//     Gorilla WebSocket 要求单连接同时只有一个 Writer:业务帧与 Ping 全部经 WritePump 串行化;
//     优雅关闭的 Close 控制帧走 gorilla 明确支持并发调用的 WriteControl。
//   - 同步响应(HandleBinary 返回值)经每连接有界 sendCh 移交 WritePump。
//   - 异步推送(Logic DeliverBatch)经 downlink notify 信号驱动 WritePump 排空每连接队列。
//
// 背压(慢客户端只淘汰自己):
//   - 每连接 sendCh 满 → 仅关闭本连接。
//   - 每连接 downlink.ConnectionWriter 有 items/bytes 双上限,满则向 Logic 返回 QUEUE_FULL。
//
// 读路径使用对象池复用小帧缓冲,降低每帧分配。
package websocket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// maxPooledBuffer 是池化读缓冲的容量上限;超大帧的扩容缓冲直接交 GC,防止常驻池内。
const maxPooledBuffer = 64 << 10

// readBufPool 复用 4KB 起步的读缓冲,小帧可完全复用,避免每帧分配。
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

// writeBufPool lets Gorilla release the per-connection write buffer after each
// message instead of retaining one in every short-lived WebSocket connection.
// The pool starts empty; Gorilla stores its internal writePoolData wrapper back
// into it after the first write, so the zero-value sync.Pool is intentional.
var writeBufPool sync.Pool

var errFrameExceedsLimit = errors.New("websocket: frame exceeds pre-auth limit")

// FrameHandler 是 transport 中立的核心处理器,通常由 command.Gate 提供。
type FrameHandler interface {
	HandleBinary(ctx context.Context, connectionID string, generation uint64, raw []byte) ([]byte, error)
	Disconnect(connectionID string, generation uint64)
}

// PreAuthLifecycle is an optional transport-neutral fence implemented by
// command.Gate. Keeping it separate from FrameHandler preserves small test
// doubles while making Direct WebSocket and gateproxy enforce identical
// resource rules before ClientHello activates a session.
type PreAuthLifecycle interface {
	BeginPreAuth(connectionID string, generation uint64, clientIP string) error
	AdmitPreAuth(connectionID string, generation uint64, frameBytes int) error
	IsAuthenticated(connectionID string, generation uint64) bool
	TimeoutPreAuth(connectionID string, generation uint64) bool
	PreAuthMaxFrameBytes() int64
}

// DownlinkSource 是 transport 消费 Logic 异步推送的最小接口,
// 由 downlink.ConnectionRegistry 实现。
type DownlinkSource interface {
	// Dequeue 取出该连接的下一帧;无数据时 ok=false。
	Dequeue(connectionID string, generation uint64) ([]byte, bool)
	// Notify 返回该连接的"队列非空"信号通道(容量 1,需配合排空语义使用)。
	Notify(connectionID string, generation uint64) (<-chan struct{}, bool)
}

// PendingDownlinkSource is the optional race-safe re-notify seam. A source
// that implements it lets WritePump, after exhausting a drain quantum, verify
// whether the queue is still non-empty and re-arm the wait channel. Without
// this probe a single edge-triggered signal can strand frames that arrived
// before the pump drained, because the second enqueue's signal is lost.
type PendingDownlinkSource interface {
	DownlinkSource
	// Pending reports whether the connection's queue still has undrained
	// frames. It must be non-destructive and safe to call concurrently.
	Pending(connectionID string, generation uint64) bool
}

// TerminalDownlinkSource distinguishes writer teardown from an ordinary
// level-triggered wakeup. A level-triggered notifier closes its current
// channel to signal pending work, so the receive ok bit cannot carry this
// distinction.
type TerminalDownlinkSource interface {
	DownlinkSource
	Closed(connectionID string, generation uint64) bool
}

type drainNotifier interface {
	EnqueueDrainNotice(drainID string, maxReconnectDelay time.Duration, deadline time.Time) int
}

// Config 是 WebSocket transport 的运行参数。
type Config struct {
	ReadLimit        int64
	SendQueueSize    int // 每连接同步响应缓冲(帧数)
	CommandQueueSize int // 每连接待处理入站帧上限；单 Lane 串行执行
	MaxConns         int // 单实例最大并发连接数(水位),0 = 不限
	HandshakeTimeout time.Duration
	HelloTimeout     time.Duration
	PingPeriod       time.Duration
	PongWait         time.Duration
	WriteWait        time.Duration
	// RateLimitPerSec 是每 IP 每秒允许的新连接数,0 表示关闭限流。
	RateLimitPerSec float64
	// RateLimitBurst 是每 IP 突发允许的最大连接数。
	RateLimitBurst int
	// AllowedOrigins 是 Origin 白名单(host 或 host:port)。
	// 为空时退回 gorilla same-origin 安全默认;显式配置后,无 Origin 头的
	// 非浏览器客户端(小程序原生/压测工具)仍放行。
	AllowedOrigins []string
	// TrustedProxies 是可信代理 CIDR 列表;仅当对端 IP 命中时才采纳
	// X-Forwarded-For / X-Real-IP。空 = 不信任任何代理(直连部署的正确默认)。
	TrustedProxies []*net.IPNet
	// Downlink 可选;非 nil 时每连接 WritePump 消费 Logic 异步推送。
	Downlink DownlinkSource
	// DownlinkDrainQuantum bounds how many queued downlink frames one wakeup
	// may drain before WritePump returns to its select. It gives CommandResult
	// (sendCh) and ping scheduling opportunities while preserving the single
	// connection's sequence ordering (the outbound sequencer is unchanged).
	DownlinkDrainQuantum int
	// Metrics 可选;非 nil 时启用 Prometheus 指标。
	Metrics *Metrics
	// CheckOrigin 返回 true 时允许跨域;nil 时由 AllowedOrigins 或 gorilla 默认接管。
	CheckOrigin func(r *http.Request) bool
	Logger      *log.Logger
	// AdmissionReady is a reversible process-level guard checked before HTTP
	// Upgrade. It is used by GateBoot Lease recovery; unlike Drain it does not
	// close established connections and can return to ready after renewal.
	AdmissionReady func() bool
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.ReadLimit <= 0 {
		out.ReadLimit = 1 << 20
	}
	if out.SendQueueSize <= 0 {
		out.SendQueueSize = 256
	}
	if out.DownlinkDrainQuantum <= 0 {
		out.DownlinkDrainQuantum = 64
	}
	if out.CommandQueueSize <= 0 {
		out.CommandQueueSize = 16
	}
	if out.HandshakeTimeout <= 0 {
		out.HandshakeTimeout = 5 * time.Second
	}
	if out.HelloTimeout <= 0 {
		out.HelloTimeout = 10 * time.Second
	}
	if out.PingPeriod <= 0 {
		out.PingPeriod = 25 * time.Second
	}
	if out.PongWait <= 0 {
		out.PongWait = 30 * time.Second
	}
	if out.WriteWait <= 0 {
		out.WriteWait = 5 * time.Second
	}
	if out.RateLimitPerSec < 0 {
		out.RateLimitPerSec = 0
	}
	if out.RateLimitPerSec > 0 && out.RateLimitBurst <= 0 {
		out.RateLimitBurst = 20
	}
	if out.Logger == nil {
		out.Logger = log.Default()
	}
	return out
}

// Server 是 WebSocket 接入服务,实现 http.Handler。
type Server struct {
	cfg      Config
	upgrader websocket.Upgrader
	handler  FrameHandler
	limiter  *IPLimiter
	graceful gracefulState

	activeConns       atomic.Int64
	generationCounter atomic.Uint64
}

// NewServer 构造 WebSocket transport。
func NewServer(handler FrameHandler, cfg Config) *Server {
	cfg = cfg.withDefaults()
	checkOrigin := cfg.CheckOrigin
	if checkOrigin == nil && len(cfg.AllowedOrigins) > 0 {
		checkOrigin = originWhitelist(cfg.AllowedOrigins)
	}
	s := &Server{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			HandshakeTimeout: cfg.HandshakeTimeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
			WriteBufferPool:  &writeBufPool,
			// nil 时 gorilla 使用 same-origin 安全默认;EnableCompression 保持
			// 默认 false,禁用 permessage-deflate,防压缩攻击与内存放大。
			CheckOrigin: checkOrigin,
		},
		handler: handler,
	}
	if cfg.RateLimitPerSec > 0 {
		s.limiter = NewIPLimiter(cfg.RateLimitPerSec, cfg.RateLimitBurst)
	}
	return s
}

// originWhitelist 生成 Origin 白名单校验:匹配 host 或 hostname 任一。
// 无 Origin 头的请求(非浏览器客户端)直接放行。
func originWhitelist(origins []string) func(*http.Request) bool {
	allowed := make(map[string]struct{}, len(origins)*2)
	for _, o := range origins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		allowed[o] = struct{}{}
		if h, _, err := net.SplitHostPort(o); err == nil {
			allowed[h] = struct{}{}
		}
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		if _, ok := allowed[u.Host]; ok {
			return true
		}
		_, ok := allowed[u.Hostname()]
		return ok
	}
}

// Close 释放后台资源(限流 GC goroutine)。
func (s *Server) Close() {
	if s.limiter != nil {
		s.limiter.Close()
	}
}

// Drain 触发优雅关闭。drainWait 是等待客户端重连的时间。
// 返回累计连接数。幂等:多次调用只第一次生效。
func (s *Server) Drain(drainWait time.Duration) int {
	return s.drain(drainWait)
}

// IsDraining is the admission/readiness boundary used by the process health
// endpoint. Liveness remains healthy while readiness turns false.
func (s *Server) IsDraining() bool {
	return s.isDraining()
}

func (s *Server) ActiveConnections() int64 {
	return s.activeConns.Load()
}

// ServeHTTP 处理 WebSocket Upgrade 请求,托管连接生命周期。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 优雅关闭模式:拒绝新连接
	if s.isDraining() {
		http.Error(w, "server draining", http.StatusServiceUnavailable)
		return
	}
	if s.cfg.AdmissionReady != nil && !s.cfg.AdmissionReady() {
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.ConnRejected.Inc()
		}
		http.Error(w, "server recovering", http.StatusServiceUnavailable)
		return
	}
	if !websocket.IsWebSocketUpgrade(r) {
		http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
		return
	}
	// 连接水位:先占名额再检查,超限立即退还并拒绝,不超发。
	if n := s.activeConns.Add(1); s.cfg.MaxConns > 0 && n > int64(s.cfg.MaxConns) {
		s.activeConns.Add(-1)
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.ConnRejected.Inc()
		}
		http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
		return
	}
	// IP 限流(仅在可信代理后采纳转发头)
	if s.limiter != nil && !s.limiter.AllowRequest(r, s.cfg.TrustedProxies) {
		s.activeConns.Add(-1)
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.ConnRejected.Inc()
		}
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	connectionID := newConnectionID()
	generation := s.generationCounter.Add(1)
	if lifecycle, ok := s.handler.(PreAuthLifecycle); ok {
		if err := lifecycle.BeginPreAuth(connectionID, generation, ClientIP(r, s.cfg.TrustedProxies)); err != nil {
			s.activeConns.Add(-1)
			if s.cfg.Metrics != nil {
				s.cfg.Metrics.ConnRejected.Inc()
			}
			http.Error(w, "pre-auth capacity reached", http.StatusServiceUnavailable)
			return
		}
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.activeConns.Add(-1)
		// Upgrade failed after PRE_AUTH admission. Disconnect is also the
		// transport-neutral release path for an unbound connection.
		s.handler.Disconnect(connectionID, generation)
		s.cfg.Logger.Printf("websocket upgrade: %v", err)
		return
	}
	unregister := s.graceful.noteConn(conn)
	defer unregister()
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.ConnTotal.Inc()
		s.cfg.Metrics.ConnActive.Inc()
	}
	s.serveConnection(connectionID, generation, conn)
}

// serveConnection 托管单连接生命周期:当前 goroutine 跑 ReadPump,
// 另起一个 WritePump 独占该连接的所有网络写。
func (s *Server) serveConnection(connectionID string, generation uint64, conn *websocket.Conn) {
	logger := s.cfg.Logger
	metrics := s.cfg.Metrics
	writeDone := make(chan struct{})
	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	type frameJob struct {
		payload []byte
		recycle func()
	}
	commandQueue := make(chan frameJob, s.cfg.CommandQueueSize)
	commandWorkerDone := make(chan struct{})
	workerStarted := false
	defer func() {
		// Interrupt the socket first so a transport close is never delayed by a
		// backend RPC. Then cancel/drain the per-connection SessionLane before
		// deleting its Gate binding.
		_ = conn.Close()
		cancelConnection()
		if workerStarted {
			close(commandQueue)
			<-commandWorkerDone
		}
		close(writeDone) // ReadPump 退出 → WritePump 立即退出
		s.activeConns.Add(-1)
		s.handler.Disconnect(connectionID, generation)
		if metrics != nil {
			metrics.ConnActive.Dec()
			if s.isDraining() {
				metrics.DrainSessions.Set(float64(s.activeConns.Load()))
			}
		}
	}()
	// Do not log normal connection open/close at high churn: cold-login tests
	// create hundreds of thousands of short-lived sockets. Protocol and write
	// errors below remain logged with the connection identity.
	conn.SetReadLimit(s.cfg.ReadLimit)

	sendCh := make(chan []byte, s.cfg.SendQueueSize)
	// hello 完成(registry.Bind)之前拿不到 notify 通道;ReadPump 在每帧处理后补订阅,
	// 通过 notifyChCh 一次性移交给 WritePump。
	notifyChCh := make(chan (<-chan struct{}), 1)
	go s.writePump(connectionID, generation, conn, sendCh, notifyChCh, writeDone)

	// PRE_AUTH is intentionally synchronous and admits exactly one application
	// FarmFrame. This prevents a client from filling the normal command lane
	// while ClientHello is still waiting on Redis/backend work.
	lifecycle, hasLifecycle := s.handler.(PreAuthLifecycle)
	helloDeadline := time.Now().Add(s.cfg.HelloTimeout)
	_ = conn.SetReadDeadline(helloDeadline)
	messageType, rc, err := conn.NextReader()
	if err != nil {
		if hasLifecycle && (time.Now().After(helloDeadline) || isTimeout(err)) {
			lifecycle.TimeoutPreAuth(connectionID, generation)
		}
		return
	}
	if messageType != websocket.BinaryMessage {
		logger.Printf("[WS] %s non-binary ClientHello frame type=%d, closing", connectionID, messageType)
		return
	}
	preAuthLimit := s.cfg.ReadLimit
	if hasLifecycle {
		if configured := lifecycle.PreAuthMaxFrameBytes(); configured > 0 && configured < preAuthLimit {
			preAuthLimit = configured
		}
	}
	payload, recycle, err := readFrameWithLimit(rc, preAuthLimit)
	if err != nil {
		logger.Printf("[WS] %s read ClientHello: %v", connectionID, err)
		return
	}
	if metrics != nil {
		metrics.MsgReceived.Inc()
		metrics.BytesReceived.Add(float64(len(payload)))
		metrics.FrameSizeRecv.Observe(float64(len(payload)))
	}
	if hasLifecycle {
		if err := lifecycle.AdmitPreAuth(connectionID, generation, len(payload)); err != nil {
			recycle()
			logger.Printf("[WS] %s PRE_AUTH admission: %v", connectionID, err)
			return
		}
	}
	handleStart := time.Now()
	helloCtx, cancelHello := context.WithDeadline(connectionCtx, helloDeadline)
	response, err := s.handler.HandleBinary(helloCtx, connectionID, generation, payload)
	cancelHello()
	recycle()
	if metrics != nil {
		metrics.HandleLatency.Observe(time.Since(handleStart).Seconds())
	}
	if err != nil {
		if hasLifecycle && errors.Is(err, context.DeadlineExceeded) {
			lifecycle.TimeoutPreAuth(connectionID, generation)
		}
		logger.Printf("[WS] %s ClientHello: %v", connectionID, err)
		return
	}
	if hasLifecycle && !lifecycle.IsAuthenticated(connectionID, generation) {
		logger.Printf("[WS] %s first FarmFrame did not activate ClientHello", connectionID)
		return
	}
	subscribed := s.cfg.Downlink == nil
	if !subscribed {
		if ch, ok := s.cfg.Downlink.Notify(connectionID, generation); ok {
			subscribed = true
			notifyChCh <- ch
		}
	}
	if len(response) > 0 {
		sendCh <- response
	}

	// Only an authenticated connection enters the normal SessionLane and can
	// extend its liveness via Pong.
	_ = conn.SetReadDeadline(time.Now().Add(s.cfg.PongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(s.cfg.PongWait))
	})

	go func() {
		defer close(commandWorkerDone)
		defer func() {
			// If one frame failed, release every pooled buffer that was already
			// admitted but can no longer be processed.
			for job := range commandQueue {
				job.recycle()
			}
		}()
		for job := range commandQueue {
			handleStart := time.Now()
			response, err := s.handler.HandleBinary(connectionCtx, connectionID, generation, job.payload)
			job.recycle()
			if metrics != nil {
				metrics.HandleLatency.Observe(time.Since(handleStart).Seconds())
			}
			if err != nil {
				logger.Printf("[WS] %s handle: %v", connectionID, err)
				_ = conn.Close()
				return
			}
			if !subscribed {
				// handleHello has bound the local registry before returning.
				if ch, ok := s.cfg.Downlink.Notify(connectionID, generation); ok {
					subscribed = true
					select {
					case notifyChCh <- ch:
					case <-connectionCtx.Done():
						return
					}
				}
			}
			if len(response) == 0 {
				continue
			}
			select {
			case sendCh <- response:
			case <-connectionCtx.Done():
				return
			default:
				if metrics != nil {
					metrics.SlowClient.Inc()
				}
				logger.Printf("[WS] %s send queue full, closing slow client", connectionID)
				_ = conn.Close()
				return
			}
		}
	}()
	workerStarted = true

	// ReadPump:把每条 binary message 交给 HandleBinary,响应移交 WritePump。
	for {
		messageType, rc, err := conn.NextReader()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			// 协议仅允许二进制帧;其他帧视为协议违规,直接断开。
			logger.Printf("[WS] %s non-binary frame type=%d, closing", connectionID, messageType)
			return
		}
		payload, recycle, err := readFrame(rc)
		if err != nil {
			logger.Printf("[WS] %s read: %v", connectionID, err)
			return
		}
		if metrics != nil {
			metrics.MsgReceived.Inc()
			metrics.BytesReceived.Add(float64(len(payload)))
			metrics.FrameSizeRecv.Observe(float64(len(payload)))
		}

		// ReadPump only performs protocol framing and bounded admission. The
		// per-connection SessionLane owns HandleBinary serially, so a slow RPC
		// does not block socket reads, pings or any other connection.
		select {
		case commandQueue <- frameJob{payload: payload, recycle: recycle}:
		default:
			recycle()
			if metrics != nil {
				metrics.SlowClient.Inc()
			}
			logger.Printf("[WS] %s command queue full, closing overloaded client", connectionID)
			return
		}
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// writePump 是单连接的写循环,独占 conn 的所有写操作(业务帧 + Ping)。
// 退出前关闭 conn,使 ReadPump 的阻塞读立即报错返回。
func (s *Server) writePump(connectionID string, generation uint64, conn *websocket.Conn,
	sendCh <-chan []byte, notifyChCh <-chan (<-chan struct{}), done <-chan struct{}) {
	logger := s.cfg.Logger
	metrics := s.cfg.Metrics
	pingTicker := time.NewTicker(s.cfg.PingPeriod)
	defer pingTicker.Stop()
	defer func() { _ = conn.Close() }()

	var notifyCh <-chan struct{}
	sequencer := newOutboundSequencer()

	writeFrame := func(payload []byte) error {
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteWait))
		if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			return err
		}
		if metrics != nil {
			metrics.MsgSent.Inc()
			metrics.BytesSent.Add(float64(len(payload)))
			metrics.FrameSizeSent.Observe(float64(len(payload)))
		}
		return nil
	}
	writeSequenced := func(payload []byte) error {
		ready, err := sequencer.Enqueue(payload)
		if err != nil {
			return err
		}
		for _, frame := range ready {
			if err := writeFrame(frame); err != nil {
				return err
			}
		}
		return nil
	}

	// drainDownlink 排空该连接的异步推送队列(Logic DeliverBatch 经 notify 信号驱动)。
	// 每次唤醒最多排空 DownlinkDrainQuantum 帧后回到 select,给同步
	// CommandResult(sendCh)与 ping 调度机会;单连接 sequence 顺序仍由
	// outboundSequencer 保证,量子只限制单次唤醒的批量长度。当达到量子上限
	// 且队列仍非空时,通过 pending/re-notify 探测保证下一轮 wait 通道必然
	// 再次可读:剩余帧无需再次入队也会被排空,而 select 仍然每量子返回一次。
	drainDownlink := func() error {
		if s.cfg.Downlink == nil {
			return nil
		}
		quantum := s.cfg.DownlinkDrainQuantum
		if quantum <= 0 {
			quantum = 64
		}
		var n int
		for n < quantum {
			frame, ok := s.cfg.Downlink.Dequeue(connectionID, generation)
			if !ok {
				break
			}
			n++
			if err := writeSequenced(frame); err != nil {
				return err
			}
		}
		if n > 0 && metrics != nil {
			metrics.WriteBatchSz.Observe(float64(n))
		}
		return nil
	}
	pendingDownlink := func() bool {
		source, ok := s.cfg.Downlink.(PendingDownlinkSource)
		return ok && source.Pending(connectionID, generation)
	}

	for {
		// The wait channel is re-armed on every iteration: after a drain the
		// writer's level-triggered notifier holds a fresh channel, and if the
		// quantum left frames queued the pending probe already re-armed it so
		// this select wakes immediately. A consumed channel is never reused.
		if s.cfg.Downlink != nil {
			if ch, ok := s.cfg.Downlink.Notify(connectionID, generation); ok {
				notifyCh = ch
			}
		}
		select {
		case payload := <-sendCh:
			if err := writeSequenced(payload); err != nil {
				logger.Printf("[WS] %s write: %v", connectionID, err)
				return
			}
		case ch := <-notifyChCh:
			// 订阅成功;立即排空一次,覆盖订阅前已入队的帧。
			notifyCh = ch
			if err := drainDownlink(); err != nil {
				logger.Printf("[WS] %s downlink write: %v", connectionID, err)
				return
			}
			if pendingDownlink() {
				continue
			}
		case <-notifyCh:
			if source, ok := s.cfg.Downlink.(TerminalDownlinkSource); ok &&
				source.Closed(connectionID, generation) {
				// ConnectionWriter is the Gate-local terminal backpressure
				// fence. Closing it must tear down the physical connection so
				// a client whose REJOIN_REQUIRED could not be queued cannot
				// continue issuing writes on a stale subscription.
				return
			}
			if err := drainDownlink(); err != nil {
				logger.Printf("[WS] %s downlink write: %v", connectionID, err)
				return
			}
			// Level-triggered re-check: after the quantum drain, a remaining
			// queue must wake the next select without a second enqueue.
			if pendingDownlink() {
				continue
			}
		case <-pingTicker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.Printf("[WS] %s ping: %v", connectionID, err)
				return
			}
		case <-done:
			return
		}
	}
}

// outboundSequencer merges synchronous command responses and asynchronous
// downlinks before they reach the socket. Sequence numbers are allocated under
// ConnectionRegistry's mutex, but the two source channels can become readable
// in either order; buffering here preserves the one monotonic wire stream.
type outboundSequencer struct {
	next    uint64
	pending map[uint64][]byte
}

func newOutboundSequencer() *outboundSequencer {
	return &outboundSequencer{next: 1, pending: make(map[uint64][]byte)}
}

func (s *outboundSequencer) Enqueue(payload []byte) ([][]byte, error) {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(payload, &frame); err != nil || frame.Sequence == 0 {
		// Keep the transport usable with narrow test doubles. Production Gate
		// and DownlinkRegistry always produce a valid sequenced FarmFrame.
		return [][]byte{payload}, nil
	}
	sequence := frame.Sequence
	if sequence < s.next {
		return nil, fmt.Errorf("outbound sequence regressed: got %d, want >= %d", sequence, s.next)
	}
	if _, duplicate := s.pending[sequence]; duplicate {
		return nil, fmt.Errorf("outbound sequence duplicated: %d", sequence)
	}
	s.pending[sequence] = payload

	ready := make([][]byte, 0, 1)
	for {
		next, ok := s.pending[s.next]
		if !ok {
			break
		}
		delete(s.pending, s.next)
		ready = append(ready, next)
		s.next++
	}
	return ready, nil
}

// readFrame 从消息 reader 读完整帧;小帧缓冲从对象池复用,降低每帧分配。
// 返回的 recycle 必须在 payload 消费完毕后调用(HandleBinary 同步解码后即可)。
func readFrame(r io.Reader) (payload []byte, recycle func(), err error) {
	return readFrameWithLimit(r, 0)
}

func readFrameWithLimit(r io.Reader, limit int64) (payload []byte, recycle func(), err error) {
	bp := readBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	for {
		if len(buf) == cap(buf) {
			nextCap := 2 * cap(buf)
			if limit > 0 && int64(nextCap) > limit+1 {
				nextCap = int(limit + 1)
			}
			if nextCap <= cap(buf) {
				putBuffer(bp, buf)
				return nil, nil, errFrameExceedsLimit
			}
			grown := make([]byte, nextCap)
			copy(grown, buf)
			buf = grown[:len(buf)]
		}
		n, rerr := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if limit > 0 && int64(len(buf)) > limit {
			putBuffer(bp, buf)
			return nil, nil, errFrameExceedsLimit
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			putBuffer(bp, buf)
			return nil, nil, rerr
		}
	}
	return buf, func() { putBuffer(bp, buf) }, nil
}

func putBuffer(bp *[]byte, buf []byte) {
	if cap(buf) > maxPooledBuffer {
		return
	}
	*bp = buf[:0]
	readBufPool.Put(bp)
}

// newConnectionID 生成 16 字节随机 connection ID。
func newConnectionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "ws-" + hex.EncodeToString(b[:])
}

// Handler 返回 http.Handler,可挂到 mux。
func (s *Server) Handler() http.Handler {
	return s
}

var _ http.Handler = (*Server)(nil)
