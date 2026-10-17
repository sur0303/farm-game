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
	"strings"
	"sync"
	"time"
)

type faultMode int

const (
	modePassthrough faultMode = iota
	modeDown
	// modeAllResponseBlackhole pauses EVERY Redis response (HELLO/AUTH/CLIENT/
	// PING/EVALSHA): a one-way downlink blackhole where the Redis return path
	// is entirely unreachable, including connection handshakes.
	modeAllResponseBlackhole
	modeBlackholeNF
	modeDropResponse
	// modeRenewResponseLoss drops ONLY the response to the renew EVALSHA
	// (identified by its SHA) and closes that client connection; HELLO/AUTH/
	// CLIENT/PING and all other commands keep working.  This isolates "renew
	// result invisible" from "Redis connection handshake unavailable".
	modeRenewResponseLoss
)

func (m faultMode) String() string {
	switch m {
	case modePassthrough:
		return "passthrough"
	case modeDown:
		return "down"
	case modeAllResponseBlackhole:
		return "all-response-blackhole"
	case modeBlackholeNF:
		return "blackhole-no-forward"
	case modeDropResponse:
		return "drop-response"
	case modeRenewResponseLoss:
		return "renew-response-loss"
	default:
		return "unknown"
	}
}

type faultEvent struct {
	TS      int64  `json:"ts_ms"`
	Mode    string `json:"mode"`
	Detail  string `json:"detail,omitempty"`
	ConnID  int    `json:"conn_id,omitempty"`
	ReqSeq  int    `json:"request_seq,omitempty"`
	Command string `json:"command,omitempty"`
	Evalsha string `json:"evalsha,omitempty"`
	BytesIn int    `json:"bytes_in,omitempty"`
}

// classifyRequest extracts the top-level RESP command name and, for EVALSHA,
// the script SHA from a client request frame (a RESP array).
func classifyRequest(frame []byte) (command, evalsha string) {
	// *N\r\n$L\r\nCMD\r\n...
	i := 0
	// skip "*N\r\n"
	for i < len(frame) && frame[i] != '\n' {
		i++
	}
	i++ // past \n
	// expect "$L\r\n"
	j := i
	for j < len(frame) && frame[j] != '\n' {
		j++
	}
	if j >= len(frame) {
		return "", ""
	}
	length := 0
	fmt.Sscanf(stringsTrimCRLF(frame[i:j]), "$%d", &length)
	j++ // past \n
	if length <= 0 || j+length > len(frame) {
		return "", ""
	}
	// go-redis emits lower-case command names. Normalize before dispatch so
	// selective renew-response loss cannot silently degrade to passthrough.
	command = strings.ToUpper(string(frame[j : j+length]))
	if command == "EVALSHA" {
		// next element is the sha: skip "$L\r\n" then read sha
		k := j + length
		if k < len(frame) && frame[k] == '\r' {
			k += 2
		}
		m := k
		for m < len(frame) && frame[m] != '\n' {
			m++
		}
		shaLen := 0
		fmt.Sscanf(stringsTrimCRLF(frame[k:m]), "$%d", &shaLen)
		m++
		if shaLen > 0 && m+shaLen <= len(frame) {
			evalsha = string(frame[m : m+shaLen])
		}
	}
	return command, evalsha
}

type faultProxy struct {
	mu           sync.Mutex
	listener     net.Listener
	addr         string // fixed host:port the proxy binds (re-bound after Down/Up)
	target       string
	mode         faultMode
	drops        int // remaining responses to drop in drop-response mode
	nextID       int
	conns        map[net.Conn]bool
	renewSHA     string            // renew EVALSHA sha for renew-response-loss mode
	pendingRenew map[net.Conn]bool // per-connection in-flight renew awaiting its response

	events     []faultEvent
	eventsPath string
}

// SetRenewSHA records the renew EVALSHA sha used by renew-response-loss.
func (p *faultProxy) SetRenewSHA(sha string) {
	p.mu.Lock()
	p.renewSHA = sha
	p.mu.Unlock()
}

// markPendingRenew records that a renew EVALSHA was forwarded on this
// connection and its response is still pending.
func (p *faultProxy) markPendingRenew(conn net.Conn) {
	p.mu.Lock()
	p.pendingRenew[conn] = true
	p.mu.Unlock()
}

// consumePendingRenew reports whether the next response frame on this
// connection is the renew response (and clears the marker).
func (p *faultProxy) consumePendingRenew(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pendingRenew[conn] {
		delete(p.pendingRenew, conn)
		return true
	}
	return false
}

func newFaultProxy(target string, eventsPath string) *faultProxy {
	return &faultProxy{target: target, mode: modePassthrough, eventsPath: eventsPath,
		conns: make(map[net.Conn]bool), pendingRenew: make(map[net.Conn]bool)}
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

// relayRequests forwards client requests (read as complete RESP frames) to
// Redis, honoring blackhole-no-forward, and logs a per-request classification.
func (p *faultProxy) relayRequests(id int, src, dst net.Conn) {
	br := bufio.NewReader(src)
	seq := 0
	for {
		frame, err := readRESPFrame(br)
		if err != nil {
			return
		}
		seq++
		cmd, sha := classifyRequest(frame)
		mode, _ := p.currentMode()
		if mode == modeBlackholeNF {
			// Swallow the request; never forward.  The client waits.
			p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, ReqSeq: seq, Command: cmd, Evalsha: sha, BytesIn: len(frame)})
			continue
		}
		if _, werr := dst.Write(frame); werr != nil {
			return
		}
		// Correlate a forwarded renew EVALSHA with its pending response so
		// renew-response-loss can drop exactly that reply.
		if cmd == "EVALSHA" && sha != "" && p.isRenewSHA(sha) {
			p.markPendingRenew(src)
		}
		p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, ReqSeq: seq, Command: cmd, Evalsha: sha, BytesIn: len(frame)})
	}
}

func (p *faultProxy) isRenewSHA(sha string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.renewSHA != "" && sha == p.renewSHA
}

// relayResponses forwards Redis responses to the client, honoring the fault
// mode: all-response-blackhole withholds every response; renew-response-loss
// drops only the response to the renew EVALSHA (by SHA) and closes the client
// connection; drop-response drops the next N responses.
func (p *faultProxy) relayResponses(id int, src, dst net.Conn) {
	br := bufio.NewReader(src)
	for {
		frame, err := readRESPFrame(br)
		if err != nil {
			return
		}
		mode, drops := p.currentMode()
		switch mode {
		case modeAllResponseBlackhole:
			// Withhold every response (including HELLO/AUTH handshakes): a
			// one-way downlink blackhole.  Keep the connection open so the
			// client waits until its own timeout.
			p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, BytesIn: len(frame)})
			continue
		case modeRenewResponseLoss:
			// Drop only the response that answers the renew EVALSHA and close
			// the client connection; everything else (HELLO/AUTH/PING/other
			// EVALSHA) flows normally.
			if p.consumePendingRenew(dst) {
				p.logEvent(faultEvent{Mode: mode.String(), ConnID: id, BytesIn: len(frame), Detail: "dropped renew reply"})
				return
			}
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
