// Command tconnd-responder is a throwaway minimal GameSvr used only to close
// the T1 loop: it connects to the gateproxy GateStream, and whenever TCONND
// forwards a START it immediately replies with a START ack (echoing the exact
// TconndHead). That ack is what makes TCONND send {"tws_notify_type":"pass"}
// to the WebSocket client, proving the full BY_WS -> TBUS -> GameSvr -> pass
// chain end to end.
//
// It is NOT farm-gate and speaks no application protocol. It never validates a
// connection_ticket; it exists purely to answer START so we can observe pass.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"git.internal.example.invalid/mfcn/gateproxy/go-client/pkg/proto/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	socket := getenv("GATEPROXY_SOCKET", "/data/unix-socket/gateproxy.sock")
	duration := parseDuration("RESPONDER_DURATION", "30s")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	conn, err := grpc.DialContext(ctx, "unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial gateproxy socket: %v", err)
	}
	defer conn.Close()
	stream, err := proxypb.NewConnectorServiceClient(conn).GateStream(ctx)
	if err != nil {
		log.Fatalf("open GateStream: %v", err)
	}
	logf("GateStream connected; acting as minimal GameSvr (acks every START)")

	var sendMu sync.Mutex
	send := func(m *proxypb.GateMsg) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}

	// GameSvr-assigned connection id. The incoming START head.Id is a sentinel
	// (-1); TCONND expects the reply to carry a real server-assigned Id and the
	// echoed ConnIdx (matches UC gatesvr connector: Id=s.ID(), ConnIdx=head, Extend=0).
	var nextID atomic.Int32

	// Heartbeat loop keeps gateproxy from tearing down the GameSvr stream.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				_ = send(&proxypb.GateMsg{Msg: &proxypb.GateMsg_HeartbeatMsg{HeartbeatMsg: &proxypb.HeartbeatMsg{TimeMs: now.UnixMilli()}}})
			}
		}
	}()

	handle := func(tm *proxypb.TconndMsg) {
		if tm == nil || tm.GetHead() == nil {
			return
		}
		head := tm.GetHead()
		switch {
		case tm.GetStartMsg() != nil:
			sm := tm.GetStartMsg()
			assigned := nextID.Add(1)
			logf("recv START head{id=%d conn_idx=%d extend=%d} auth_type=%d client_type=%d -> ack with assigned id=%d",
				head.GetId(), head.GetConnIdx(), head.GetExtend(), sm.GetAuthType(), sm.GetClientType(), assigned)
			// Ack: server-assigned Id, echoed ConnIdx, Extend=0, empty StartMsg.
			ack := &proxypb.GateMsg{Msg: &proxypb.GateMsg_TconndMsg{TconndMsg: &proxypb.TconndMsg{
				Head: &proxypb.TconndHead{Id: assigned, ConnIdx: head.GetConnIdx(), Extend: 0},
				Msg:  &proxypb.TconndMsg_StartMsg{StartMsg: &proxypb.TconndStartMsg{}},
			}}}
			if err := send(ack); err != nil {
				logf("send START ack error: %v", err)
			}
		case tm.GetInprocMsg() != nil:
			body := tm.GetInprocMsg().GetBody()
			sum := sha256.Sum256(body)
			logf("recv INPROC head{id=%d conn_idx=%d} %d bytes sha256=%s", head.GetId(), head.GetConnIdx(), len(body), hex.EncodeToString(sum[:8]))
		case tm.GetStopMsg() != nil:
			logf("recv STOP head{id=%d conn_idx=%d}", head.GetId(), head.GetConnIdx())
		}
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				logf("stream done: %v", err)
				return
			}
			logf("recv error: %v", err)
			return
		}
		if tm := msg.GetTconndMsg(); tm != nil {
			handle(tm)
			continue
		}
		if batch := msg.GetBatchTconndMsg(); batch != nil {
			for _, tm := range batch.GetMsgs() {
				handle(tm)
			}
		}
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseDuration(key, fallback string) time.Duration {
	v, err := time.ParseDuration(getenv(key, fallback))
	if err != nil || v <= 0 {
		v, _ = time.ParseDuration(fallback)
	}
	return v
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "[%d] responder: "+format+"\n", append([]any{time.Now().UnixMilli()}, args...)...)
}
