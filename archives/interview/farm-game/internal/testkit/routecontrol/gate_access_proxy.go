package routecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// GateAccessProxyEvent records one owned HTTP/WS forward observation without
// exposing tickets, secrets, or raw Redis values.
type GateAccessProxyEvent struct {
	Sequence            uint64 `json:"sequence"`
	Kind                string `json:"kind"`
	UpstreamOK          bool   `json:"upstream_ok,omitempty"`
	Dropped             bool   `json:"dropped,omitempty"`
	Committed           bool   `json:"committed,omitempty"`
	Status              string `json:"status,omitempty"`
	RequestIDSHA256     string `json:"request_id_sha256,omitempty"`
	OperationIDSHA256   string `json:"operation_id_sha256,omitempty"`
	UpstreamResponseSHA string `json:"upstream_response_sha256,omitempty"`
	WallTime            string `json:"wall_time"`
}

// GateAccessForwardProxy terminates client HTTP/WS in front of one production
// Gate and can drop exactly one CommandResult after the upstream commit.
type GateAccessForwardProxy struct {
	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	upstream string
	events   []GateAccessProxyEvent
	next     uint64
	dropNext bool
	closed   bool
}

// StartGateAccessForwardProxy listens on listenAddr and forwards to upstreamHost
// (host:port of a real Gate HTTP listener).
func StartGateAccessForwardProxy(listenAddr, upstreamHost string) (*GateAccessForwardProxy, error) {
	upstreamHost = strings.TrimSpace(upstreamHost)
	if upstreamHost == "" {
		return nil, errors.New("gate access proxy requires upstream host")
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	proxy := &GateAccessForwardProxy{listener: listener, upstream: upstreamHost}
	director := func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = upstreamHost
		req.Host = upstreamHost
	}
	reverse := &httputil.ReverseProxy{Director: director}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", proxy.handleWS)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		reverse.ServeHTTP(w, r)
		proxy.record(GateAccessProxyEvent{Kind: "http_forward", UpstreamOK: true})
	})
	proxy.server = &http.Server{Handler: mux}
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *GateAccessForwardProxy) Address() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *GateAccessForwardProxy) ArmDropNextCommandResult() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropNext = true
}

func (p *GateAccessForwardProxy) ClearDrop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropNext = false
}

func (p *GateAccessForwardProxy) Events() []GateAccessProxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]GateAccessProxyEvent, len(p.events))
	copy(out, p.events)
	return out
}

func (p *GateAccessForwardProxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	server := p.server
	p.mu.Unlock()
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
	if p.listener != nil {
		_ = p.listener.Close()
	}
	return nil
}

func (p *GateAccessForwardProxy) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	client, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		p.record(GateAccessProxyEvent{Kind: "ws_upgrade", UpstreamOK: false})
		return
	}
	defer client.Close()
	upstreamURL := url.URL{Scheme: "ws", Host: p.upstream, Path: "/ws"}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	upstream, resp, err := dialer.Dial(upstreamURL.String(), nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		p.record(GateAccessProxyEvent{Kind: "ws_upgrade", UpstreamOK: false})
		return
	}
	defer upstream.Close()
	p.record(GateAccessProxyEvent{Kind: "ws_upgrade", UpstreamOK: true})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = copyWS(client, upstream, nil)
		_ = upstream.Close()
	}()
	go func() {
		defer wg.Done()
		_ = copyWS(upstream, client, func(raw []byte) ([]byte, bool) {
			return p.maybeDropCommandResult(raw)
		})
		_ = client.Close()
	}()
	wg.Wait()
}

func copyWS(src, dst *websocket.Conn, filter func([]byte) ([]byte, bool)) error {
	for {
		messageType, raw, err := src.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.BinaryMessage {
			if err := dst.WriteMessage(messageType, raw); err != nil {
				return err
			}
			continue
		}
		if filter != nil {
			filtered, ok := filter(raw)
			if !ok {
				continue
			}
			raw = filtered
		}
		if err := dst.WriteMessage(websocket.BinaryMessage, raw); err != nil {
			return err
		}
	}
}

func (p *GateAccessForwardProxy) maybeDropCommandResult(raw []byte) ([]byte, bool) {
	p.mu.Lock()
	drop := p.dropNext
	p.mu.Unlock()
	if !drop {
		return raw, true
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		return raw, true
	}
	result := frame.GetCommandResult()
	if result == nil {
		return raw, true
	}
	status := result.GetStatus()
	committed := status == commonpb.CommandStatus_SUCCEEDED ||
		status == commonpb.CommandStatus_NO_CHANGE ||
		status == commonpb.CommandStatus_PROCESSING ||
		status == commonpb.CommandStatus_ACCEPTED
	digest := sha256.Sum256(raw)
	p.mu.Lock()
	p.dropNext = false
	p.mu.Unlock()
	p.record(GateAccessProxyEvent{
		Kind:                "command_result_dropped",
		UpstreamOK:          true,
		Dropped:             true,
		Committed:           committed,
		Status:              status.String(),
		RequestIDSHA256:     digestText(result.GetRequestId()),
		OperationIDSHA256:   digestText(result.GetOperationId()),
		UpstreamResponseSHA: hex.EncodeToString(digest[:]),
	})
	return nil, false
}

func (p *GateAccessForwardProxy) record(event GateAccessProxyEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	event.Sequence = p.next
	event.WallTime = time.Now().UTC().Format(time.RFC3339Nano)
	p.events = append(p.events, event)
}

func digestText(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
