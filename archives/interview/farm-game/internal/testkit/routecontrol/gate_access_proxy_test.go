//go:build linux

package routecontrol

import (
	"net"
	"net/http"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func TestGateAccessProxyDropsOneCommandResult(t *testing.T) {
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamLn.Close()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, upgradeErr := upgrader.Upgrade(w, r, nil)
		if upgradeErr != nil {
			return
		}
		defer conn.Close()
		for {
			messageType, raw, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			if messageType != websocket.BinaryMessage {
				continue
			}
			var frame commonpb.FarmFrame
			if proto.Unmarshal(raw, &frame) != nil {
				continue
			}
			cmd := frame.GetClientCommand()
			if cmd == nil {
				continue
			}
			resultFrame := &commonpb.FarmFrame{
				ProtocolVersion: 4,
				Sequence:        frame.GetSequence() + 1,
				FrameBody: &commonpb.FarmFrame_CommandResult{CommandResult: &commonpb.CommandResultEnvelope{
					OperationId: cmd.GetOperationId(), RequestId: cmd.GetRequestId(),
					Status: commonpb.CommandStatus_SUCCEEDED,
				}},
			}
			out, _ := proto.MarshalOptions{Deterministic: true}.Marshal(resultFrame)
			_ = conn.WriteMessage(websocket.BinaryMessage, out)
		}
	})
	go func() { _ = http.Serve(upstreamLn, mux) }()

	proxy, err := StartGateAccessForwardProxy("127.0.0.1:0", upstreamLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	dial := func() *websocket.Conn {
		t.Helper()
		dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
		client, resp, dialErr := dialer.Dial("ws://"+proxy.Address()+"/ws", nil)
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		return client
	}
	writeBuy := func(client *websocket.Conn, op string) {
		t.Helper()
		cmdFrame := &commonpb.FarmFrame{
			ProtocolVersion: 4, Sequence: 1,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
				OperationId: op, RequestId: "request-" + op, CommandType: commonpb.CommandType_BUY,
			}},
		}
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(cmdFrame)
		if err := client.WriteMessage(websocket.BinaryMessage, raw); err != nil {
			t.Fatal(err)
		}
	}

	proxy.ArmDropNextCommandResult()
	dropClient := dial()
	defer dropClient.Close()
	writeBuy(dropClient, "op-drop")
	_ = dropClient.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := dropClient.ReadMessage(); err == nil {
		t.Fatal("expected dropped command result to produce a read timeout")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		dropped := false
		for _, event := range proxy.Events() {
			if event.Kind == "command_result_dropped" && event.Dropped && event.Committed && event.Status == "SUCCEEDED" {
				dropped = true
			}
		}
		if dropped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events=%+v", proxy.Events())
		}
		time.Sleep(20 * time.Millisecond)
	}

	_ = dropClient.Close()
	proxy.ClearDrop()

	passClient := dial()
	defer passClient.Close()
	writeBuy(passClient, "op-pass")
	_ = passClient.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := passClient.ReadMessage(); err != nil {
		t.Fatalf("unarmed result should forward: %v", err)
	}
}
