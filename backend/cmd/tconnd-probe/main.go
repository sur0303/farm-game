// Command tconnd-probe observes the Gongfeng gateproxy GateStream boundary.
// It does not claim that a WebSocket reached TCONND by itself: T1/T2/T3/T8
// still require a real BY_WS/WSS client run and their raw output is recorded
// alongside this probe's event log.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"git.internal.example.invalid/mfcn/gateproxy/go-client/pkg/proto/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	socket           string
	output           string
	duration         time.Duration
	heartbeatEvery   time.Duration
	heartbeatTimeout time.Duration
}

type event struct {
	TimeUnixMs int64  `json:"time_unix_ms"`
	Kind       string `json:"kind"`
	Connection string `json:"connection,omitempty"`
	Generation string `json:"generation,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	LatencyMs  int64  `json:"latency_ms,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	conn, err := grpc.DialContext(ctx, "unix:"+cfg.socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial gateproxy socket: %v", err)
	}
	defer conn.Close()
	stream, err := proxypb.NewConnectorServiceClient(conn).GateStream(ctx)
	if err != nil {
		log.Fatalf("open GateStream: %v", err)
	}
	writer, closeWriter, err := openWriter(cfg.output)
	if err != nil {
		log.Fatalf("open probe output: %v", err)
	}
	defer closeWriter()
	var writeMu sync.Mutex
	write := func(e event) {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := json.NewEncoder(writer).Encode(e); err != nil {
			log.Printf("write event: %v", err)
		}
	}
	write(event{TimeUnixMs: time.Now().UnixMilli(), Kind: "stream_ready", Detail: "gateproxy GateStream connected; external WSS client still required"})

	lastHeartbeat := atomic.Int64{}
	lastHeartbeat.Store(time.Now().UnixMilli())
	tracker := newGenerationTracker()
	var sendMu sync.Mutex
	errCh := make(chan error, 2)
	go receiveLoop(ctx, stream, write, &lastHeartbeat, tracker, errCh)
	go heartbeatLoop(ctx, stream, cfg, write, &lastHeartbeat, &sendMu, errCh)
	select {
	case <-ctx.Done():
		write(event{TimeUnixMs: time.Now().UnixMilli(), Kind: "probe_done", Detail: ctx.Err().Error()})
	case err := <-errCh:
		write(event{TimeUnixMs: time.Now().UnixMilli(), Kind: "probe_error", Detail: err.Error()})
	}
	_ = stream.CloseSend()
}

func receiveLoop(ctx context.Context, stream proxypb.ConnectorService_GateStreamClient, write func(event), lastHeartbeat *atomic.Int64, tracker *generationTracker, errCh chan<- error) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				errCh <- io.EOF
				return
			}
			errCh <- err
			return
		}
		if hb := msg.GetHeartbeatMsg(); hb != nil {
			now := time.Now().UnixMilli()
			lastHeartbeat.Store(now)
			write(event{TimeUnixMs: now, Kind: "heartbeat_recv", LatencyMs: now - hb.GetTimeMs()})
			continue
		}
		if tm := msg.GetTconndMsg(); tm != nil {
			writeTconnd(tm, write, tracker)
			continue
		}
		if batch := msg.GetBatchTconndMsg(); batch != nil {
			for _, tm := range batch.GetMsgs() {
				writeTconnd(tm, write, tracker)
			}
		}
	}
}

func heartbeatLoop(ctx context.Context, stream proxypb.ConnectorService_GateStreamClient, cfg config, write func(event), lastHeartbeat *atomic.Int64, sendMu *sync.Mutex, errCh chan<- error) {
	ticker := time.NewTicker(cfg.heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if now.UnixMilli()-lastHeartbeat.Load() > cfg.heartbeatTimeout.Milliseconds() {
				errCh <- fmt.Errorf("gateproxy heartbeat timeout: last=%d", lastHeartbeat.Load())
				return
			}
			sendMu.Lock()
			err := stream.Send(&proxypb.GateMsg{Msg: &proxypb.GateMsg_HeartbeatMsg{HeartbeatMsg: &proxypb.HeartbeatMsg{TimeMs: now.UnixMilli()}}})
			sendMu.Unlock()
			if err != nil {
				errCh <- err
				return
			}
			write(event{TimeUnixMs: now.UnixMilli(), Kind: "heartbeat_send"})
		}
	}
}

type generationTracker struct {
	mu     sync.Mutex
	active map[string]uint64
	next   map[string]uint64
}

func newGenerationTracker() *generationTracker {
	return &generationTracker{active: make(map[string]uint64), next: make(map[string]uint64)}
}

func (t *generationTracker) generation(head *proxypb.TconndHead, kind string) uint64 {
	key := strconv.FormatInt(int64(head.GetId()), 10) + ":" + strconv.FormatInt(int64(head.GetConnIdx()), 10) + ":" + strconv.FormatUint(head.GetExtend(), 10)
	t.mu.Lock()
	defer t.mu.Unlock()
	if kind == "start" {
		t.next[key]++
		t.active[key] = t.next[key]
	} else if _, ok := t.active[key]; !ok {
		t.next[key]++
		t.active[key] = t.next[key]
	}
	gen := t.active[key]
	if kind == "stop" {
		delete(t.active, key)
	}
	return gen
}

func writeTconnd(msg *proxypb.TconndMsg, write func(event), tracker *generationTracker) {
	if msg == nil || msg.GetHead() == nil {
		write(event{TimeUnixMs: time.Now().UnixMilli(), Kind: "invalid_tconnd_message", Detail: "missing head"})
		return
	}
	head := msg.GetHead()
	base := event{TimeUnixMs: time.Now().UnixMilli(), Connection: strconv.FormatInt(int64(head.GetId()), 10)}
	switch {
	case msg.GetStartMsg() != nil:
		base.Kind = "start"
		base.Generation = strconv.FormatUint(tracker.generation(head, base.Kind), 10)
		base.Detail = fmt.Sprintf("auth_type=%d client_type=%d", msg.GetStartMsg().GetAuthType(), msg.GetStartMsg().GetClientType())
	case msg.GetStopMsg() != nil:
		base.Kind = "stop"
		base.Generation = strconv.FormatUint(tracker.generation(head, base.Kind), 10)
	case msg.GetInprocMsg() != nil:
		body := msg.GetInprocMsg().GetBody()
		hash := sha256.Sum256(body)
		base.Kind, base.Bytes, base.SHA256 = "inproc", len(body), hex.EncodeToString(hash[:])
		base.Generation = strconv.FormatUint(tracker.generation(head, base.Kind), 10)
	default:
		base.Kind = "unknown"
	}
	write(base)
}

func loadConfig() config {
	get := func(key, fallback string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return fallback
	}
	parse := func(key, fallback string) time.Duration {
		value, err := time.ParseDuration(get(key, fallback))
		if err != nil || value <= 0 {
			value, _ = time.ParseDuration(fallback)
		}
		return value
	}
	return config{
		socket:           get("GATEPROXY_SOCKET", "/data/unix-socket/gateproxy.sock"),
		output:           get("TCONND_PROBE_OUTPUT", "docs/verification/tconnd-probe-events.jsonl"),
		duration:         parse("TCONND_PROBE_DURATION", "30s"),
		heartbeatEvery:   parse("TCONND_PROBE_HEARTBEAT_INTERVAL", "1s"),
		heartbeatTimeout: parse("TCONND_PROBE_HEARTBEAT_TIMEOUT", "5s"),
	}
}

func openWriter(path string) (*os.File, func(), error) {
	if path == "-" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { _ = file.Close() }, nil
}
