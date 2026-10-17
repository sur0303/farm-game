package websocket

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// IPLimiter 是每 IP 建连限流器。
// 采用令牌桶算法:每 IP 初始 burst 个令牌,以 rate 速率恢复。
// 超过 1 分钟未使用的 IP 自动从 map 中淘汰,防止内存膨胀。
type IPLimiter struct {
	rate    float64 // 每秒恢复令牌数
	burst   int     // 桶容量
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	stopGc  chan struct{}
	gcOnce  sync.Once
}

type tokenBucket struct {
	tokens    float64
	lastRefil time.Time
	lastSeen  time.Time
}

// NewIPLimiter 构造每 IP 限流器。
// rate:每秒允许的新连接数;burst:突发允许的最大连接数。
func NewIPLimiter(rate float64, burst int) *IPLimiter {
	if rate <= 0 {
		rate = 5
	}
	if burst <= 0 {
		burst = 10
	}
	l := &IPLimiter{
		rate:    rate,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
		stopGc:  make(chan struct{}),
	}
	go l.gcLoop()
	return l
}

// Allow 检查指定 IP 是否允许建立新连接。
func (l *IPLimiter) Allow(ip string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[ip]
	if !ok {
		b = &tokenBucket{tokens: float64(l.burst), lastRefil: now, lastSeen: now}
		l.buckets[ip] = b
	}
	// 恢复令牌
	elapsed := now.Sub(b.lastRefil).Seconds()
	b.tokens += elapsed * l.rate
	if b.tokens > float64(l.burst) {
		b.tokens = float64(l.burst)
	}
	b.lastRefil = now
	b.lastSeen = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// AllowRequest 从 http.Request 提取客户端 IP 并检查限流。
// trusted 是可信代理 CIDR;仅当对端 IP 命中时才采纳 XFF/X-Real-IP,防伪造。
func (l *IPLimiter) AllowRequest(r *http.Request, trusted []*net.IPNet) bool {
	return l.Allow(ClientIP(r, trusted))
}

// Close 停止后台 GC goroutine。
func (l *IPLimiter) Close() {
	l.gcOnce.Do(func() { close(l.stopGc) })
}

func (l *IPLimiter) gcLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-l.stopGc:
			return
		case <-t.C:
			l.mu.Lock()
			cutoff := time.Now().Add(-time.Minute)
			for ip, b := range l.buckets {
				if b.lastSeen.Before(cutoff) {
					delete(l.buckets, ip)
				}
			}
			l.mu.Unlock()
		}
	}
}

// ClientIP 提取请求的真实客户端 IP。
// 只有当 RemoteAddr 命中 trusted 代理 CIDR 时,才信任其注入的
// X-Forwarded-For / X-Real-IP;否则直接使用 RemoteAddr,
// 避免客户端伪造转发头绕过限流。
func ClientIP(r *http.Request, trusted []*net.IPNet) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if parsed := net.ParseIP(strings.TrimSpace(host)); parsed != nil {
		host = parsed.String()
	}
	if !ipTrusted(host, trusted) {
		return host
	}
	// Walk XFF from the nearest hop to the farthest. Trusted proxies are
	// peeled off; the first valid non-trusted hop is the effective client.
	// This remains safe when an ingress appends to a client-supplied XFF
	// instead of replacing it: a forged left-most value cannot win.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		for index := len(parts) - 1; index >= 0; index-- {
			parsed := net.ParseIP(strings.TrimSpace(parts[index]))
			if parsed == nil {
				continue
			}
			candidate := parsed.String()
			if !ipTrusted(candidate, trusted) {
				return candidate
			}
		}
	}
	// 其次 X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if parsed := net.ParseIP(strings.TrimSpace(xri)); parsed != nil {
			return parsed.String()
		}
	}
	return host
}

func ipTrusted(ip string, trusted []*net.IPNet) bool {
	if len(trusted) == 0 {
		return false
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, cidr := range trusted {
		if cidr.Contains(parsed) {
			return true
		}
	}
	return false
}
