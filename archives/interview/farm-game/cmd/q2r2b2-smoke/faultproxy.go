package main

// faultProxy is a test-only TCP proxy placed in front of the company Redis.
// Each business process (player-svr / farm-svr) gets its own proxy instance so
// faults are isolated per composition root.  It transparently forwards RESP
// bytes and supports three deterministic fault modes:
//
//	modeDown:         listener closed; new connections get ECONNREFUSED
//	                   (request provably never sent).
//	modeBlackhole:    connections accepted; requests are forwarded to Redis
//	                   but responses are withheld from the client (the request
//	                   DID reach Redis, the client sees a timeout).
//	modeBlackholeNF:  connections accepted but nothing is forwarded to Redis;
//	                   the client waits (request never reaches Redis).
//	modeDropResponse: the next N complete RESP responses are read from Redis,
//	                   discarded, and the client connection is closed (post
//	                   execution reply loss).
//
// Every injected fault is appended to fault-proxy-events.ndjson.  The control
// interface is in-process (used by the smoke scenarios); it is not a network
// API.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type faultMode int

const (
	modePassthrough faultMode = iota
	modeDown
	modeBlackhole
	modeBlackholeNF
	modeDropResponse
)

func (m faultMode) String() string {
	switch m {
	case modePassthrough:
		return "passthrough"
	case modeDown:
		return "down"
	case modeBlackhole:
		return "blackhole"
	case modeBlackholeNF:
		return "blackhole-no-forward"
	case modeDropResponse:
		return "drop-response"
	default:
		return "unknown"
	}
}

type faultEvent struct {
	TS      int64  `json:"ts_ms"`
	Mode    string `json:"mode"`
	Detail  string `json:"detail,omitempty"`
	ConnID  int    `json:"conn_id,omitempty"`
	BytesIn int    `json:"bytes_in,omitempty"`
}

type faultProxy struct {
	mu       sync.Mutex
	listener net.Listener
	addr     string // fixed host:port the proxy binds (re-bound after Down/Up)
	target   string
	mode     faultMode
	drops    int // remaining responses to drop in drop-response mode
	nextID   int
	conns    map[net.Conn]bool

	events []faultEvent
	eventsPath string
}

func newFaultProxy(target string, eventsPath string) *faultProxy {
	return &faultProxy{target: target, mode: modePassthrough, eventsPath: eventsPath, conns: make(map[net.Conn]bool)}
}

// Start binds the proxy to an ephemeral localhost port.
func (p *faultProxy) Start() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.addr = ln.Addr().String()
	p.listener = ln
	p.mu.Unlock()
	go p.acceptLoop()
	return nil
}

func (p *faultProxy) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}

func (p *faultProxy) logEvent(ev faultEvent) {
	ev.TS = time.Now().UnixMilli()
	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
}

// SetMode switches the fault mode; detail is recorded on the event log.
func (p *faultProxy) SetMode(m faultMode, detail string, drops int) {
	p.mu.Lock()
	old := p.mode
	p.mode = m
	p.drops = drops
	p.mu.Unlock()
	p.logEvent(faultEvent{Mode: m.String(), Detail: fmt.Sprintf("from=%s %s", old, detail)})
}

// StopMode returns to passthrough.
func (p *faultProxy) StopMode() {
	p.SetMode(modePassthrough, "restore", 0)
}

// Down closes the listener and every existing connection (modeDown implied):
// the next dial from the client hits ECONNREFUSED (provably not sent).
func (p *faultProxy) Down() {
	p.mu.Lock()
	old := p.mode
	p.mode = modeDown
	ln := p.listener
	p.listener = nil
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[net.Conn]bool)
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	p.logEvent(faultEvent{Mode: modeDown.String(), Detail: fmt.Sprintf("from=%s listener+%d conns closed", old, len(conns))})
}

// Up re-binds the listener on the SAME host:port after Down, so an already
// running client keeps dialing the same endpoint.
func (p *faultProxy) Up() error {
	p.mu.Lock()
	if p.listener != nil {
		p.mu.Unlock()
		return nil
	}
	addr := p.addr
	p.mu.Unlock()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.listener = ln
	p.mu.Unlock()
	go p.acceptLoop()
	p.StopMode()
	return nil
}

func (p *faultProxy) acceptLoop() {
	for {
		p.mu.Lock()
		ln := p.listener
		p.mu.Unlock()
		if ln == nil {
			return
		}
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.nextID++
		id := p.nextID
		p.mu.Unlock()
		go p.handleConn(id, conn)
	}
}

func (p *faultProxy) currentMode() (faultMode, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode, p.drops
}

func (p *faultProxy) handleConn(id int, client net.Conn) {
	defer client.Close()
	p.mu.Lock()
	p.conns[client] = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()
	mode, _ := p.currentMode()
	if mode == modeDown {
		p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, Detail: "refused (down)"})
		return
	}
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, Detail: "upstream dial failed: " + err.Error()})
		return
	}
	defer upstream.Close()

	done := make(chan struct{}, 2)
	// client -> upstream (requests)
	go func() {
		defer func() { done <- struct{}{} }()
		p.relayRequests(id, client, upstream)
	}()
	// upstream -> client (responses)
	go func() {
		defer func() { done <- struct{}{} }()
		p.relayResponses(id, upstream, client)
	}()
	<-done
	<-done
}

// relayRequests forwards client bytes to Redis, honoring blackhole-no-forward.
func (p *faultProxy) relayRequests(id int, src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			mode, _ := p.currentMode()
			if mode == modeBlackholeNF {
				// Swallow the request; never forward.  The client waits.
				p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, BytesIn: n})
				continue
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// relayResponses forwards Redis responses to the client, honoring blackhole
// (withhold) and drop-response (read frame, discard, close).
func (p *faultProxy) relayResponses(id int, src, dst net.Conn) {
	br := bufio.NewReader(src)
	for {
		frame, err := readRESPFrame(br)
		if err != nil {
			return
		}
		mode, drops := p.currentMode()
		switch mode {
		case modeBlackhole:
			// Withhold the response; keep the connection open so the client
			// waits until its own timeout.
			p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, BytesIn: len(frame)})
			continue
		case modeDropResponse:
			if drops > 0 {
				p.mu.Lock()
				if p.drops > 0 {
					p.drops--
				}
				p.mu.Unlock()
				p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, BytesIn: len(frame), Detail: "dropped reply"})
				// Close the client connection: the client sees a broken reply.
				return
			}
		}
		if _, werr := dst.Write(frame); werr != nil {
			return
		}
	}
}

// readRESPFrame reads one complete RESP response frame from the reader.
func readRESPFrame(r *bufio.Reader) ([]byte, error) {
	head, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteByte(head)
	switch head {
	case '+', '-', ':':
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		return buf.Bytes(), nil
	case '$':
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		var n int
		if _, err := fmt.Sscanf(stringsTrimCRLF(line), "%d", &n); err != nil {
			return nil, err
		}
		if n >= 0 {
			body := make([]byte, n+2)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, err
			}
			buf.Write(body)
		}
		return buf.Bytes(), nil
	case '*':
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		var n int
		if _, err := fmt.Sscanf(stringsTrimCRLF(line), "%d", &n); err != nil {
			return nil, err
		}
		if n < 0 {
			return buf.Bytes(), nil
		}
		for i := 0; i < n; i++ {
			sub, err := readRESPFrame(r)
			if err != nil {
				return nil, err
			}
			buf.Write(sub)
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unexpected RESP type byte %q", head)
	}
}

func stringsTrimCRLF(b []byte) string {
	s := string(b)
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// FlushEvents writes the fault event log to the artifact path.
func (p *faultProxy) FlushEvents() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.Create(p.eventsPath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, ev := range p.events {
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return nil
}
