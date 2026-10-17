package gateproxy

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/gateway/preauth"

	"git.internal.example.invalid/mfcn/gateproxy/go-client/pkg/proto/proxypb"
)

type fakeGateClient struct {
	mu          sync.Mutex
	frames      []string
	connections []string
	disconnects []string
}

type pollingGateClient struct {
	fakeGateClient
	mu       sync.Mutex
	body     []byte
	polled   int
	returned bool
}

type lifecycleGateClient struct {
	fakeGateClient
	budget              *preauth.Controller
	authenticateOnFrame bool
	authenticated       atomic.Bool
	clientIP            string
	timedOut            chan struct{}
	timeoutOnce         sync.Once
}

func newLifecycleGateClient(authenticateOnFrame bool) *lifecycleGateClient {
	return &lifecycleGateClient{
		budget: preauth.New(preauth.Config{
			MaxConnections: 4, MaxConnectionsPerIP: 2,
			MaxFramesPerConnection: 1, MaxBytesPerConnection: 32,
			MaxBytesPerIP: 64, MaxBytesPod: 128,
		}),
		authenticateOnFrame: authenticateOnFrame,
		timedOut:            make(chan struct{}),
	}
}

func (f *lifecycleGateClient) BeginPreAuth(connectionID string, generation uint64, clientIP string) error {
	f.clientIP = clientIP
	return f.budget.Open(connectionID, generation, clientIP)
}

func (f *lifecycleGateClient) AdmitPreAuth(connectionID string, generation uint64, frameBytes int) error {
	return f.budget.Admit(connectionID, generation, frameBytes)
}

func (f *lifecycleGateClient) IsAuthenticated(string, uint64) bool {
	return f.authenticated.Load()
}

func (f *lifecycleGateClient) TimeoutPreAuth(connectionID string, generation uint64) bool {
	ok := f.budget.Timeout(connectionID, generation)
	if ok {
		f.timeoutOnce.Do(func() { close(f.timedOut) })
	}
	return ok
}

func (f *lifecycleGateClient) PreAuthMaxFrameBytes() int64 {
	return f.budget.MaxFrameBytes()
}

func (f *lifecycleGateClient) HandleFrame(ctx context.Context, connectionID string, generation uint64, body []byte) ([]byte, error) {
	response, err := f.fakeGateClient.HandleFrame(ctx, connectionID, generation, body)
	if err == nil && f.authenticateOnFrame {
		f.authenticated.Store(true)
		f.budget.Authenticate(connectionID, generation)
	}
	return response, err
}

func (f *lifecycleGateClient) Disconnect(ctx context.Context, connectionID string, generation uint64) error {
	f.budget.Close(connectionID, generation)
	return f.fakeGateClient.Disconnect(ctx, connectionID, generation)
}

func (f *pollingGateClient) PollDownlink(context.Context, string, uint64) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polled++
	if f.returned || len(f.body) == 0 {
		return nil, false, nil
	}
	f.returned = true
	return append([]byte(nil), f.body...), true, nil
}

type failingStream struct {
	*fakeStream
	err error
}

func (s *failingStream) Send(*proxypb.GateMsg) error { return s.err }

func (f *fakeGateClient) HandleFrame(_ context.Context, connectionID string, generation uint64, body []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connections = append(f.connections, connectionID+":"+itoa(generation))
	f.frames = append(f.frames, string(body))
	return append([]byte("response:"), body...), nil
}

func (f *fakeGateClient) PollDownlink(context.Context, string, uint64) ([]byte, bool, error) {
	return nil, false, nil
}

func (f *fakeGateClient) Disconnect(_ context.Context, connectionID string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnects = append(f.disconnects, connectionID+":"+itoa(generation))
	return nil
}

type fakeStream struct {
	ctx    context.Context
	input  chan *proxypb.GateMsg
	mu     sync.Mutex
	output []*proxypb.GateMsg
}

func newFakeStream() *fakeStream {
	return &fakeStream{ctx: context.Background(), input: make(chan *proxypb.GateMsg, 16)}
}

func (s *fakeStream) Send(msg *proxypb.GateMsg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.output = append(s.output, msg)
	return nil
}

func (s *fakeStream) Recv() (*proxypb.GateMsg, error) {
	msg, ok := <-s.input
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

func (s *fakeStream) Context() context.Context { return s.ctx }
func (s *fakeStream) CloseSend() error         { return nil }

func (s *fakeStream) outputs() []*proxypb.GateMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*proxypb.GateMsg(nil), s.output...)
}

func TestTransportMapsStartInprocAndStopWithoutBusinessLogic(t *testing.T) {
	client := &fakeGateClient{}
	stream := newFakeStream()
	runner := New(client, Config{DownlinkPollInterval: time.Hour, HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()

	head := &proxypb.TconndHead{Id: 7, ConnIdx: 3, Extend: 9}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{Openid: 99}},
	}}}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: head, Msg: &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("farm-frame")}},
	}}}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: head, Msg: &proxypb.TconndMsg_StopMsg{StopMsg: &proxypb.TconndStopMsg{}},
	}}}
	close(stream.input)

	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("Run error=%v, want ErrStreamClosed", err)
	}
	client.mu.Lock()
	if len(client.frames) != 1 || client.frames[0] != "farm-frame" {
		t.Fatalf("frames=%v", client.frames)
	}
	// Connection is keyed by ConnIdx (3) only, not the mutable Id/Extend.
	if len(client.connections) != 1 || client.connections[0] != "tconnd:3:1" {
		t.Fatalf("connections=%v", client.connections)
	}
	if len(client.disconnects) != 1 || client.disconnects[0] != "tconnd:3:1" {
		t.Fatalf("disconnects=%v", client.disconnects)
	}
	client.mu.Unlock()

	out := stream.outputs()
	if len(out) != 2 {
		t.Fatalf("outbound messages=%d, want start ack and response", len(out))
	}
	ack := out[0].GetTconndMsg()
	if ack.GetStartMsg() == nil {
		t.Fatalf("first outbound message=%v", out[0])
	}
	// START ack must carry a GameSvr-assigned Id (not the incoming Id=7),
	// echo ConnIdx, and zero Extend. Otherwise real TCONND never sends "pass".
	if ack.GetHead().GetId() != 1 || ack.GetHead().GetConnIdx() != 3 || ack.GetHead().GetExtend() != 0 {
		t.Fatalf("start ack head=%v, want {id:1 conn_idx:3 extend:0}", ack.GetHead())
	}
	if got := string(out[1].GetTconndMsg().GetInprocMsg().GetBody()); got != "response:farm-frame" {
		t.Fatalf("response body=%q", got)
	}
}

// TestTransportHandlesSentinelStartIdFromRealTconnd reproduces the real BY_WS
// wire behaviour verified end to end against tconnd gf-3.4.36: START arrives
// with a sentinel Id (-1), and every later INPROC/STOP carries the
// GameSvr-assigned Id while ConnIdx stays stable. The transport must key by
// ConnIdx so the frame still reaches the gate client and the connection is torn
// down exactly once.
func TestTransportHandlesSentinelStartIdFromRealTconnd(t *testing.T) {
	client := &fakeGateClient{}
	stream := newFakeStream()
	runner := New(client, Config{DownlinkPollInterval: time.Hour, HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()

	const connIdx int32 = 4303
	// START: sentinel id, as TCONND actually sends it.
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: &proxypb.TconndHead{Id: -1, ConnIdx: connIdx, Extend: 0}, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}},
	}}}
	// INPROC/STOP: assigned id (1), same ConnIdx.
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: &proxypb.TconndHead{Id: 1, ConnIdx: connIdx, Extend: 0}, Msg: &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("farm-frame")}},
	}}}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: &proxypb.TconndHead{Id: 1, ConnIdx: connIdx, Extend: 0}, Msg: &proxypb.TconndMsg_StopMsg{StopMsg: &proxypb.TconndStopMsg{}},
	}}}
	close(stream.input)

	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("Run error=%v, want ErrStreamClosed", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	// Despite Id changing from -1 to 1, the frame reaches exactly one connection
	// and it is disconnected once (no orphaned state from a key mismatch).
	if len(client.frames) != 1 || client.frames[0] != "farm-frame" {
		t.Fatalf("frames=%v", client.frames)
	}
	if len(client.connections) != 1 || client.connections[0] != "tconnd:4303:1" {
		t.Fatalf("connections=%v", client.connections)
	}
	if len(client.disconnects) != 1 || client.disconnects[0] != "tconnd:4303:1" {
		t.Fatalf("disconnects=%v", client.disconnects)
	}
}

func TestConnectionGenerationIncrementsWhenHeadIsReused(t *testing.T) {
	client := &fakeGateClient{}
	runner := New(client, Config{DownlinkPollInterval: time.Hour, HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	stream := newFakeStream()
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: 1, ConnIdx: 2}
	start := func() *proxypb.GateMsg {
		return &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}}}}}
	}
	inproc := func() *proxypb.GateMsg {
		return &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("x")}}}}}
	}
	stream.input <- start()
	stream.input <- inproc()
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_StopMsg{StopMsg: &proxypb.TconndStopMsg{}}}}}
	stream.input <- start()
	stream.input <- inproc()
	close(stream.input)
	<-result

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.connections) != 2 || client.connections[0] != "tconnd:2:1" || client.connections[1] != "tconnd:2:2" {
		t.Fatalf("connections=%v", client.connections)
	}
}

func TestTransportSharesPreAuthLifecycleAndUsesTconndIP(t *testing.T) {
	client := newLifecycleGateClient(true)
	stream := newFakeStream()
	runner := New(client, Config{
		HelloTimeout: time.Second, DownlinkPollInterval: time.Hour,
		HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour,
	})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: -1, ConnIdx: 51}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{Ip: "203.0.113.51"}},
	}}}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: &proxypb.TconndHead{Id: 1, ConnIdx: 51},
		Msg:  &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("hello")}},
	}}}
	close(stream.input)
	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("Run error=%v", err)
	}
	if client.clientIP != "203.0.113.51" {
		t.Fatalf("PRE_AUTH client IP=%q", client.clientIP)
	}
	if snapshot := client.budget.Snapshot("203.0.113.51"); snapshot != (preauth.Snapshot{}) {
		t.Fatalf("authenticated PRE_AUTH budget leaked: %s", snapshot)
	}
}

func TestTransportRejectsSecondPreAuthFarmFrameWithoutClosingGateStream(t *testing.T) {
	client := newLifecycleGateClient(false)
	stream := newFakeStream()
	runner := New(client, Config{
		HelloTimeout: time.Hour, DownlinkPollInterval: time.Hour,
		HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour,
	})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: -1, ConnIdx: 52}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{Ip: "203.0.113.52"}},
	}}}
	for _, body := range []string{"first", "second"} {
		stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
			Head: &proxypb.TconndHead{Id: 1, ConnIdx: 52},
			Msg:  &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte(body)}},
		}}}
	}
	close(stream.input)
	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("one rejected connection closed GateStream: %v", err)
	}
	client.mu.Lock()
	frames := append([]string(nil), client.frames...)
	client.mu.Unlock()
	if len(frames) != 1 || frames[0] != "first" {
		t.Fatalf("frames reaching Gate=%v, want only first", frames)
	}
	outputs := stream.outputs()
	if len(outputs) != 3 || outputs[2].GetTconndMsg().GetStopMsg() == nil {
		t.Fatalf("outputs=%d, want START ack, first response, isolated STOP", len(outputs))
	}
}

func TestTransportTimesOutStartWithoutClientHello(t *testing.T) {
	client := newLifecycleGateClient(false)
	stream := newFakeStream()
	runner := New(client, Config{
		HelloTimeout: 20 * time.Millisecond, DownlinkPollInterval: time.Hour,
		HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour,
	})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
		Head: &proxypb.TconndHead{Id: -1, ConnIdx: 53},
		Msg:  &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{Ip: "203.0.113.53"}},
	}}}
	select {
	case <-client.timedOut:
	case <-time.After(time.Second):
		t.Fatal("ClientHello timeout did not release PRE_AUTH")
	}
	deadline := time.Now().Add(time.Second)
	for len(stream.outputs()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stream.input)
	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("Run error=%v", err)
	}
	outputs := stream.outputs()
	if len(outputs) != 2 || outputs[1].GetTconndMsg().GetStopMsg() == nil {
		t.Fatalf("outputs=%d, want START ack then timeout STOP", len(outputs))
	}
	if snapshot := client.budget.Snapshot("203.0.113.53"); snapshot != (preauth.Snapshot{}) {
		t.Fatalf("timeout PRE_AUTH budget leaked: %s", snapshot)
	}
}

func TestTransportPollsDownlinkAfterInboundFrame(t *testing.T) {
	client := &pollingGateClient{body: []byte("server-push")}
	stream := newFakeStream()
	runner := New(client, Config{DownlinkPollInterval: time.Millisecond, HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: 8, ConnIdx: 1, Extend: 2}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}}}}}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("client-frame")}}}}}
	deadline := time.After(time.Second)
	for {
		out := stream.outputs()
		if len(out) >= 3 {
			if got := string(out[2].GetTconndMsg().GetInprocMsg().GetBody()); got != "server-push" {
				t.Fatalf("downlink body=%q", got)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("downlink was not polled, outputs=%d", len(out))
		case <-time.After(time.Millisecond):
		}
	}
	close(stream.input)
	if err := <-result; err != ErrStreamClosed {
		t.Fatalf("Run error=%v", err)
	}
	client.mu.Lock()
	polled := client.polled
	client.mu.Unlock()
	if polled == 0 {
		t.Fatal("expected at least one downlink poll")
	}
}

func TestTransportRejectsOversizedInboundFrame(t *testing.T) {
	client := &fakeGateClient{}
	stream := newFakeStream()
	runner := New(client, Config{MaxFrameBytes: 4, HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: 9, ConnIdx: 1}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_InprocMsg{InprocMsg: &proxypb.TconndInprocMsg{Body: []byte("too-large")}}}}}
	if err := <-result; !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

func TestTransportPropagatesGateStreamSendFailure(t *testing.T) {
	stream := &failingStream{fakeStream: newFakeStream(), err: errors.New("send failed")}
	client := &fakeGateClient{}
	runner := New(client, Config{HeartbeatInterval: time.Hour, HeartbeatTimeout: 2 * time.Hour})
	result := make(chan error, 1)
	go func() { result <- runner.Run(stream) }()
	head := &proxypb.TconndHead{Id: 10, ConnIdx: 1}
	stream.input <- &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{Head: head, Msg: &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}}}}}
	if err := <-result; err == nil || !strings.Contains(err.Error(), "send failed") {
		t.Fatalf("want send failure, got %v", err)
	}
}

func itoa(value uint64) string {
	if value == 0 {
		return "0"
	}
	return strconv.FormatUint(value, 10)
}
