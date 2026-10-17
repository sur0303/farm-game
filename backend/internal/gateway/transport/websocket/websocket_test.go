package websocket

import (
	"context"
	"errors"
	gateprotocol "farmgame/internal/gateway/protocol"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	gatedownlink "farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/preauth"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type testFrameHandler struct {
	calls atomic.Int64
	bind  func(string, uint64)
}

func (h *testFrameHandler) HandleBinary(_ context.Context, connectionID string, generation uint64, raw []byte) ([]byte, error) {
	h.calls.Add(1)
	if h.bind != nil {
		h.bind(connectionID, generation)
	}
	return append([]byte("response:"), raw...), nil
}

func (*testFrameHandler) Disconnect(string, uint64) {}

type blockingFrameHandler struct {
	calls   atomic.Int64
	started chan struct{}
	once    sync.Once
}

type preAuthFrameHandler struct {
	testFrameHandler
	budget        *preauth.Controller
	authenticated atomic.Bool
	openedIP      string
}

func newPreAuthFrameHandler() *preAuthFrameHandler {
	return &preAuthFrameHandler{budget: preauth.New(preauth.Config{
		MaxConnections: 2, MaxConnectionsPerIP: 1,
		MaxFramesPerConnection: 1, MaxBytesPerConnection: 16,
		MaxBytesPerIP: 16, MaxBytesPod: 32,
	})}
}

func (h *preAuthFrameHandler) BeginPreAuth(connectionID string, generation uint64, clientIP string) error {
	h.openedIP = clientIP
	return h.budget.Open(connectionID, generation, clientIP)
}

func (h *preAuthFrameHandler) AdmitPreAuth(connectionID string, generation uint64, frameBytes int) error {
	return h.budget.Admit(connectionID, generation, frameBytes)
}

func (h *preAuthFrameHandler) IsAuthenticated(string, uint64) bool {
	return h.authenticated.Load()
}

func (h *preAuthFrameHandler) TimeoutPreAuth(connectionID string, generation uint64) bool {
	return h.budget.Timeout(connectionID, generation)
}

func (h *preAuthFrameHandler) PreAuthMaxFrameBytes() int64 {
	return h.budget.MaxFrameBytes()
}

func (h *preAuthFrameHandler) HandleBinary(ctx context.Context, connectionID string, generation uint64, raw []byte) ([]byte, error) {
	response, err := h.testFrameHandler.HandleBinary(ctx, connectionID, generation, raw)
	if err == nil {
		h.authenticated.Store(true)
		h.budget.Authenticate(connectionID, generation)
	}
	return response, err
}

func (h *preAuthFrameHandler) Disconnect(connectionID string, generation uint64) {
	h.budget.Close(connectionID, generation)
}

func (h *blockingFrameHandler) HandleBinary(ctx context.Context, _ string, _ uint64, raw []byte) ([]byte, error) {
	h.calls.Add(1)
	if string(raw) == "slow" {
		h.once.Do(func() { close(h.started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return append([]byte("response:"), raw...), nil
}

func (*blockingFrameHandler) Disconnect(string, uint64) {}

type testDownlink struct {
	mu         sync.Mutex
	connection string
	generation uint64
	queue      [][]byte
	notify     chan struct{}
}

func newTestDownlink() *testDownlink {
	return &testDownlink{notify: make(chan struct{}, 1)}
}

func (d *testDownlink) bind(connectionID string, generation uint64) {
	d.mu.Lock()
	d.connection = connectionID
	d.generation = generation
	d.mu.Unlock()
}

func (d *testDownlink) enqueue(payload []byte) {
	d.mu.Lock()
	d.queue = append(d.queue, append([]byte(nil), payload...))
	d.mu.Unlock()
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *testDownlink) Dequeue(connectionID string, generation uint64) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connection != connectionID || d.generation != generation || len(d.queue) == 0 {
		return nil, false
	}
	payload := d.queue[0]
	d.queue = d.queue[1:]
	return payload, true
}

func (d *testDownlink) Notify(connectionID string, generation uint64) (<-chan struct{}, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connection != connectionID || d.generation != generation {
		return nil, false
	}
	return d.notify, true
}

// Pending implements the optional PendingDownlinkSource seam with the same
// level-triggered contract as ConnectionRegistry: it reports whether the queue
// still holds frames and re-arms the edge-triggered notify channel so the
// pump's next Notify wakes immediately.
func (d *testDownlink) Pending(connectionID string, generation uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connection != connectionID || d.generation != generation || len(d.queue) == 0 {
		return false
	}
	select {
	case d.notify <- struct{}{}:
	default:
	}
	return true
}

func startTestServer(t *testing.T, handler FrameHandler, cfg Config) (string, func()) {
	t.Helper()
	server := NewServer(handler, cfg)
	httpServer := httptest.NewServer(server)
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	return wsURL, func() {
		httpServer.Close()
		server.Close()
	}
}

func dialTestWebSocket(t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("dial websocket: %v (status=%d)", err, response.StatusCode)
		}
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
}

func TestServerWritesSynchronousResponseAndAsynchronousDownlink(t *testing.T) {
	downlink := newTestDownlink()
	handler := &testFrameHandler{bind: downlink.bind}
	wsURL, closeServer := startTestServer(t, handler, Config{
		Downlink:        downlink,
		HelloTimeout:    time.Second,
		PingPeriod:      time.Hour,
		RateLimitPerSec: 0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read synchronous response: %v", err)
	}
	if got, want := string(payload), "response:hello"; got != want {
		t.Fatalf("synchronous response=%q, want %q", got, want)
	}

	downlink.enqueue([]byte("async-push"))
	_, payload, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read asynchronous downlink: %v", err)
	}
	if got, want := string(payload), "async-push"; got != want {
		t.Fatalf("asynchronous downlink=%q, want %q", got, want)
	}
}

func TestServerDrainsClosedLevelTriggeredSignalWithoutTreatingItAsTerminal(t *testing.T) {
	registry := gatedownlink.NewConnectionRegistry()
	writerReady := make(chan *gatedownlink.ConnectionWriter, 1)
	var bindOnce sync.Once
	handler := &testFrameHandler{bind: func(connectionID string, generation uint64) {
		bindOnce.Do(func() {
			writer := gatedownlink.NewConnectionWriter(generation, 4, 1<<20)
			registry.Bind(connectionID, "", generation, writer)
			if err := writer.EnqueueKind(
				generation,
				gatedownlink.FrameKindCommandResult,
				[]byte("level-triggered-push"),
			); err != nil {
				t.Errorf("enqueue level-triggered push: %v", err)
			}
			writerReady <- writer
		})
	}}
	wsURL, closeServer := startTestServer(t, handler, Config{
		Downlink:        registry,
		HelloTimeout:    time.Second,
		PingPeriod:      time.Hour,
		RateLimitPerSec: 0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, 2)
	for range 2 {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read response and level-triggered push: %v", err)
		}
		got[string(payload)] = true
	}
	if !got["response:hello"] || !got["level-triggered-push"] {
		t.Fatalf("received frames=%v", got)
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("still-open")); err != nil {
		t.Fatalf("write after ordinary notification: %v", err)
	}
	if _, payload, err := conn.ReadMessage(); err != nil || string(payload) != "response:still-open" {
		t.Fatalf("response after ordinary notification=(%q, %v)", payload, err)
	}

	writer := <-writerReady
	writer.Close()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("terminal writer close must tear down the websocket")
	}
}

func TestServerCompletesSharedPreAuthBeforeNormalSessionLane(t *testing.T) {
	handler := newPreAuthFrameHandler()
	wsURL, closeServer := startTestServer(t, handler, Config{
		HelloTimeout: time.Second, PingPeriod: time.Hour, RateLimitPerSec: 0,
	})
	defer closeServer()
	conn := dialTestWebSocket(t, wsURL)
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, response, err := conn.ReadMessage(); err != nil || string(response) != "response:hello" {
		t.Fatalf("ClientHello response=(%q,%v)", response, err)
	}
	if !handler.authenticated.Load() {
		t.Fatal("connection entered normal lane without authentication")
	}
	if snapshot := handler.budget.Snapshot(handler.openedIP); snapshot != (preauth.Snapshot{}) {
		t.Fatalf("authenticated PRE_AUTH budget leaked: %s", snapshot)
	}
	_ = conn.Close()
}

func TestServerRejectsNonBinaryFirstFrameWithoutCallingHandler(t *testing.T) {
	handler := &testFrameHandler{}
	wsURL, closeServer := startTestServer(t, handler, Config{
		HelloTimeout:    time.Second,
		PingPeriod:      time.Hour,
		RateLimitPerSec: 0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("not-a-client-hello")); err != nil {
		t.Fatalf("write text frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected connection close after non-binary first frame")
	}
	if got := handler.calls.Load(); got != 0 {
		t.Fatalf("handler calls=%d, want 0", got)
	}
}

func TestServerEnforcesHelloTimeout(t *testing.T) {
	handler := &testFrameHandler{}
	wsURL, closeServer := startTestServer(t, handler, Config{
		HelloTimeout:    40 * time.Millisecond,
		PingPeriod:      time.Hour,
		RateLimitPerSec: 0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected connection close after hello timeout")
	}
	if got := handler.calls.Load(); got != 0 {
		t.Fatalf("handler calls=%d, want 0", got)
	}
}

func TestReadPumpUsesBoundedPerConnectionCommandLane(t *testing.T) {
	handler := &blockingFrameHandler{started: make(chan struct{})}
	wsURL, closeServer := startTestServer(t, handler, Config{
		CommandQueueSize: 1,
		HelloTimeout:     time.Second,
		PingPeriod:       time.Hour,
		RateLimitPerSec:  0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("hello response: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("slow")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("slow command was not admitted")
	}
	// The worker is occupied, one command fits the bounded lane, and the next
	// frame must be observed by ReadPump and close only this connection.
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("queued")); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("overflow")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected overloaded command lane to close the connection")
	}
	if got := handler.calls.Load(); got != 2 {
		t.Fatalf("handler calls=%d, queued work escaped bounded lane", got)
	}
}

func TestDrainRejectsAdmissionAndSignalsServiceRestart(t *testing.T) {
	handler := &testFrameHandler{}
	server := NewServer(handler, Config{
		HelloTimeout:    time.Second,
		PingPeriod:      time.Hour,
		RateLimitPerSec: 0,
	})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()

	if got := server.ActiveConnections(); got != 1 {
		t.Fatalf("active connections=%d, want 1", got)
	}
	server.Drain(0)
	if !server.IsDraining() {
		t.Fatal("server must expose not-ready drain state")
	}
	_, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("new admission during drain err=%v response=%v", err, response)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseServiceRestart {
		t.Fatalf("drain close=%v, want 1012 Service Restart", err)
	}
}

func TestAdmissionReadyRejectsBeforeUpgradeAndRecovers(t *testing.T) {
	var ready atomic.Bool
	server := NewServer(&testFrameHandler{}, Config{
		HelloTimeout: time.Second, PingPeriod: time.Hour,
		AdmissionReady: ready.Load,
	})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	_, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("recovering admission err=%v response=%v", err, response)
	}
	ready.Store(true)
	connection := dialTestWebSocket(t, wsURL)
	_ = connection.Close()
	if server.IsDraining() {
		t.Fatal("reversible admission guard must not enter permanent drain")
	}
}

func TestClientIPTrustsForwardedHeaderOnlyFromConfiguredProxy(t *testing.T) {
	_, trusted, err := net.ParseCIDR("198.18.0.196/8")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "http://example/ws", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.9")

	req.RemoteAddr = "198.51.100.4:1234"
	if got, want := ClientIP(req, []*net.IPNet{trusted}), "198.51.100.4"; got != want {
		t.Fatalf("untrusted proxy client IP=%q, want %q", got, want)
	}

	req.RemoteAddr = "198.18.0.197:1234"
	if got, want := ClientIP(req, []*net.IPNet{trusted}), "203.0.113.9"; got != want {
		t.Fatalf("trusted proxy client IP=%q, want %q", got, want)
	}

	req.Header.Set("X-Forwarded-For", "192.0.2.99, 203.0.113.9, 198.18.0.198")
	if got, want := ClientIP(req, []*net.IPNet{trusted}), "203.0.113.9"; got != want {
		t.Fatalf("appended spoofed XFF client IP=%q, want nearest untrusted %q", got, want)
	}

	req.Header.Set("X-Forwarded-For", "not-an-ip, 198.18.0.198")
	req.Header.Set("X-Real-IP", "also-not-an-ip")
	if got, want := ClientIP(req, []*net.IPNet{trusted}), "198.18.0.197"; got != want {
		t.Fatalf("invalid forwarded chain client IP=%q, want conservative peer %q", got, want)
	}
}

func TestReadFrameWithLimitStopsBeforeUnboundedGrowth(t *testing.T) {
	payload, recycle, err := readFrameWithLimit(strings.NewReader("12345678"), 8)
	if err != nil || string(payload) != "12345678" {
		t.Fatalf("exact limit payload=(%q,%v)", payload, err)
	}
	recycle()
	if _, _, err := readFrameWithLimit(strings.NewReader("123456789"), 8); !errors.Is(err, errFrameExceedsLimit) {
		t.Fatalf("oversized frame error=%v", err)
	}
}

func TestOutboundSequencerMergesResponseAndDownlinkOrder(t *testing.T) {
	sequencer := newOutboundSequencer()
	frame := func(sequence uint64) []byte {
		t.Helper()
		data, err := proto.Marshal(gateprotocol.NewCommandResultFrame(sequence,
			&commonpb.CommandResultEnvelope{RequestId: "r"}))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	sequenceOf := func(data []byte) uint64 {
		t.Helper()
		var decoded commonpb.FarmFrame
		if err := proto.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded.GetSequence()
	}

	first, err := sequencer.Enqueue(frame(1))
	if err != nil || len(first) != 1 || sequenceOf(first[0]) != 1 {
		t.Fatalf("first=%v err=%v", first, err)
	}
	third, err := sequencer.Enqueue(frame(3))
	if err != nil || len(third) != 0 {
		t.Fatalf("third must wait for gap: %v err=%v", third, err)
	}
	ready, err := sequencer.Enqueue(frame(2))
	if err != nil || len(ready) != 2 || sequenceOf(ready[0]) != 2 || sequenceOf(ready[1]) != 3 {
		t.Fatalf("ready order invalid: %v err=%v", ready, err)
	}
}

func TestWritePumpDrainQuantumYieldsToCommandResult(t *testing.T) {
	downlink := newTestDownlink()
	handler := &testFrameHandler{bind: downlink.bind}
	wsURL, closeServer := startTestServer(t, handler, Config{
		Downlink:             downlink,
		DownlinkDrainQuantum: 2,
		HelloTimeout:         time.Second,
		PingPeriod:           time.Hour,
		SendQueueSize:        4,
		RateLimitPerSec:      0,
	})
	defer closeServer()

	conn := dialTestWebSocket(t, wsURL)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read hello response: %v", err)
	}

	// Enqueue more downlink frames than one quantum, then a synchronous
	// command. The bounded quantum must not starve the sendCh response: the
	// client eventually observes every frame exactly once.
	for i := 0; i < 6; i++ {
		downlink.enqueue([]byte(fmt.Sprintf("push-%d", i)))
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("command")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	seen := make(map[string]int)
	for len(seen) < 7 {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v (seen=%v)", err, seen)
		}
		seen[string(payload)]++
	}
	for i := 0; i < 6; i++ {
		if seen[fmt.Sprintf("push-%d", i)] != 1 {
			t.Fatalf("push-%d count=%d, want 1 (seen=%v)", i, seen[fmt.Sprintf("push-%d", i)], seen)
		}
	}
	if seen["response:command"] != 1 {
		t.Fatalf("command response count=%d (seen=%v)", seen["response:command"], seen)
	}
}
