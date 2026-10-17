// Package preauth owns bounded resource accounting between transport admission
// and successful ClientHello activation. It is transport neutral: Direct
// WebSocket and TCONND/gateproxy use the same controller.
package preauth

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type RejectReason string

const (
	ReasonPodConnections  RejectReason = "pod_connections"
	ReasonIPConnections   RejectReason = "ip_connections"
	ReasonFrames          RejectReason = "connection_frames"
	ReasonConnectionBytes RejectReason = "connection_bytes"
	ReasonIPBytes         RejectReason = "ip_bytes"
	ReasonPodBytes        RejectReason = "pod_bytes"
	ReasonUnknownFence    RejectReason = "unknown_fence"
)

type Rejection struct {
	Reason RejectReason
}

func (e *Rejection) Error() string {
	if e == nil {
		return "pre-auth rejected"
	}
	return "pre-auth rejected: " + string(e.Reason)
}

func ReasonOf(err error) RejectReason {
	var rejection *Rejection
	if errors.As(err, &rejection) {
		return rejection.Reason
	}
	return ""
}

type Config struct {
	MaxConnections         int
	MaxConnectionsPerIP    int
	MaxFramesPerConnection int
	MaxBytesPerConnection  int64
	MaxBytesPerIP          int64
	MaxBytesPod            int64
	Metrics                *Metrics
	Now                    func() time.Time
}

func (cfg Config) withDefaults() Config {
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 4096
	}
	if cfg.MaxConnectionsPerIP <= 0 {
		cfg.MaxConnectionsPerIP = 64
	}
	if cfg.MaxFramesPerConnection <= 0 {
		cfg.MaxFramesPerConnection = 1
	}
	if cfg.MaxBytesPerConnection <= 0 {
		cfg.MaxBytesPerConnection = 64 << 10
	}
	if cfg.MaxBytesPerIP <= 0 {
		cfg.MaxBytesPerIP = 1 << 20
	}
	if cfg.MaxBytesPod <= 0 {
		cfg.MaxBytesPod = 64 << 20
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return cfg
}

type fence struct {
	connectionID string
	generation   uint64
}

type connectionState struct {
	ip       string
	frames   int
	bytes    int64
	openedAt time.Time
}

type ipState struct {
	connections int
	bytes       int64
}

type Controller struct {
	cfg         Config
	mu          sync.Mutex
	connections map[fence]connectionState
	byIP        map[string]ipState
	podBytes    int64
}

type Snapshot struct {
	Connections   int
	PodBytes      int64
	IPConnections int
	IPBytes       int64
}

func New(cfg Config) *Controller {
	cfg = cfg.withDefaults()
	return &Controller{
		cfg:         cfg,
		connections: make(map[fence]connectionState),
		byIP:        make(map[string]ipState),
	}
}

func (c *Controller) MaxFrameBytes() int64 {
	if c == nil {
		return 0
	}
	return c.cfg.MaxBytesPerConnection
}

func (c *Controller) Open(connectionID string, generation uint64, clientIP string) error {
	if c == nil {
		return nil
	}
	if strings.TrimSpace(connectionID) == "" || generation == 0 {
		return c.reject(ReasonUnknownFence)
	}
	key := fence{connectionID: connectionID, generation: generation}
	ip := normalizeIP(clientIP)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.connections[key]; exists {
		return nil
	}
	if len(c.connections) >= c.cfg.MaxConnections {
		return c.rejectLocked(ReasonPodConnections)
	}
	if ip != "" && c.byIP[ip].connections >= c.cfg.MaxConnectionsPerIP {
		return c.rejectLocked(ReasonIPConnections)
	}
	c.connections[key] = connectionState{ip: ip, openedAt: c.cfg.Now()}
	if ip != "" {
		current := c.byIP[ip]
		current.connections++
		c.byIP[ip] = current
	}
	c.metricsOpenLocked()
	return nil
}

func (c *Controller) Admit(connectionID string, generation uint64, frameBytes int) error {
	if c == nil {
		return nil
	}
	if frameBytes < 0 {
		return c.reject(ReasonConnectionBytes)
	}
	key := fence{connectionID: connectionID, generation: generation}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, exists := c.connections[key]
	if !exists {
		return c.rejectLocked(ReasonUnknownFence)
	}
	nextFrames := state.frames + 1
	nextBytes := state.bytes + int64(frameBytes)
	if nextFrames > c.cfg.MaxFramesPerConnection {
		return c.rejectLocked(ReasonFrames)
	}
	if nextBytes > c.cfg.MaxBytesPerConnection {
		return c.rejectLocked(ReasonConnectionBytes)
	}
	if c.podBytes+int64(frameBytes) > c.cfg.MaxBytesPod {
		return c.rejectLocked(ReasonPodBytes)
	}
	if state.ip != "" && c.byIP[state.ip].bytes+int64(frameBytes) > c.cfg.MaxBytesPerIP {
		return c.rejectLocked(ReasonIPBytes)
	}
	state.frames = nextFrames
	state.bytes = nextBytes
	c.connections[key] = state
	c.podBytes += int64(frameBytes)
	if state.ip != "" {
		current := c.byIP[state.ip]
		current.bytes += int64(frameBytes)
		c.byIP[state.ip] = current
	}
	c.metricsBytesLocked()
	return nil
}

func (c *Controller) Authenticate(connectionID string, generation uint64) bool {
	return c.finish(connectionID, generation, "authenticated")
}

func (c *Controller) Close(connectionID string, generation uint64) bool {
	return c.finish(connectionID, generation, "closed")
}

func (c *Controller) Timeout(connectionID string, generation uint64) bool {
	return c.finish(connectionID, generation, "timeout")
}

func (c *Controller) IsPending(connectionID string, generation uint64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	_, exists := c.connections[fence{connectionID: connectionID, generation: generation}]
	c.mu.Unlock()
	return exists
}

func (c *Controller) Snapshot(clientIP string) Snapshot {
	if c == nil {
		return Snapshot{}
	}
	ip := normalizeIP(clientIP)
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.byIP[ip]
	return Snapshot{
		Connections: len(c.connections), PodBytes: c.podBytes,
		IPConnections: current.connections, IPBytes: current.bytes,
	}
}

func (c *Controller) finish(connectionID string, generation uint64, result string) bool {
	if c == nil {
		return false
	}
	key := fence{connectionID: connectionID, generation: generation}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, exists := c.connections[key]
	if !exists {
		return false
	}
	delete(c.connections, key)
	c.podBytes -= state.bytes
	if c.podBytes < 0 {
		c.podBytes = 0
	}
	if state.ip != "" {
		current := c.byIP[state.ip]
		current.connections--
		current.bytes -= state.bytes
		if current.connections <= 0 && current.bytes <= 0 {
			delete(c.byIP, state.ip)
		} else {
			if current.connections < 0 {
				current.connections = 0
			}
			if current.bytes < 0 {
				current.bytes = 0
			}
			c.byIP[state.ip] = current
		}
	}
	c.metricsFinishLocked(result, c.cfg.Now().Sub(state.openedAt))
	return true
}

func (c *Controller) reject(reason RejectReason) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rejectLocked(reason)
}

func (c *Controller) rejectLocked(reason RejectReason) error {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.Rejected.WithLabelValues(string(reason)).Inc()
	}
	return &Rejection{Reason: reason}
}

func (c *Controller) metricsOpenLocked() {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.Connections.Set(float64(len(c.connections)))
	}
}

func (c *Controller) metricsBytesLocked() {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.Bytes.Set(float64(c.podBytes))
	}
}

func (c *Controller) metricsFinishLocked(result string, duration time.Duration) {
	if c.cfg.Metrics == nil {
		return
	}
	c.cfg.Metrics.Connections.Set(float64(len(c.connections)))
	c.cfg.Metrics.Bytes.Set(float64(c.podBytes))
	c.cfg.Metrics.Transitions.WithLabelValues(result).Inc()
	c.cfg.Metrics.Duration.WithLabelValues(result).Observe(duration.Seconds())
}

func normalizeIP(value string) string {
	value = strings.TrimSpace(value)
	if parsed := net.ParseIP(value); parsed != nil {
		return parsed.String()
	}
	// Do not let an untrusted transport invent arbitrary per-IP bucket names.
	// A missing/malformed address still consumes the pod-wide budgets; it simply
	// cannot participate in the more specific IP bucket.
	return ""
}

func (s Snapshot) String() string {
	return fmt.Sprintf("connections=%d pod_bytes=%d ip_connections=%d ip_bytes=%d",
		s.Connections, s.PodBytes, s.IPConnections, s.IPBytes)
}
