package routecontrol

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TCPFaultProxyEvent records one owned TCP forward/drop without payload bytes.
type TCPFaultProxyEvent struct {
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Dropped  bool   `json:"dropped,omitempty"`
	WallTime string `json:"wall_time"`
}

// TCPFaultProxy is a byte-forwarding loopback proxy that can refuse/drop
// connections for bounded renew/RPC fault injection.
type TCPFaultProxy struct {
	mu       sync.Mutex
	listener net.Listener
	upstream string
	events   []TCPFaultProxyEvent
	next     uint64
	drop     atomic.Bool
	closed   atomic.Bool
	conns    map[net.Conn]struct{}
}

func StartTCPFaultProxy(listenAddr, upstreamAddr string) (*TCPFaultProxy, error) {
	upstreamAddr = strings.TrimSpace(upstreamAddr)
	if upstreamAddr == "" {
		return nil, errors.New("tcp fault proxy requires upstream")
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	proxy := &TCPFaultProxy{
		listener: listener, upstream: upstreamAddr, conns: make(map[net.Conn]struct{}),
	}
	go proxy.serve()
	return proxy, nil
}

func (p *TCPFaultProxy) Address() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *TCPFaultProxy) ArmDrop() {
	p.drop.Store(true)
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	p.record(TCPFaultProxyEvent{Kind: "drop_armed", Dropped: true})
}

func (p *TCPFaultProxy) ClearDrop() {
	p.drop.Store(false)
	p.record(TCPFaultProxyEvent{Kind: "drop_cleared"})
}

func (p *TCPFaultProxy) Events() []TCPFaultProxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]TCPFaultProxyEvent, len(p.events))
	copy(out, p.events)
	return out
}

func (p *TCPFaultProxy) Close() error {
	p.closed.Store(true)
	p.ArmDrop()
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

func (p *TCPFaultProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if p.closed.Load() {
				return
			}
			continue
		}
		if p.drop.Load() {
			p.record(TCPFaultProxyEvent{Kind: "accept_dropped", Dropped: true})
			_ = conn.Close()
			continue
		}
		go p.handle(conn)
	}
}

func (p *TCPFaultProxy) handle(client net.Conn) {
	p.track(client, true)
	defer func() {
		p.track(client, false)
		_ = client.Close()
	}()
	if p.drop.Load() {
		p.record(TCPFaultProxyEvent{Kind: "accept_dropped", Dropped: true})
		return
	}
	upstream, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		p.record(TCPFaultProxyEvent{Kind: "dial_failed"})
		return
	}
	p.track(upstream, true)
	defer func() {
		p.track(upstream, false)
		_ = upstream.Close()
	}()
	p.record(TCPFaultProxyEvent{Kind: "forward_open"})
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
}

func (p *TCPFaultProxy) track(conn net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.conns[conn] = struct{}{}
	} else {
		delete(p.conns, conn)
	}
}

func (p *TCPFaultProxy) record(event TCPFaultProxyEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	event.Sequence = p.next
	event.WallTime = time.Now().UTC().Format(time.RFC3339Nano)
	p.events = append(p.events, event)
}
