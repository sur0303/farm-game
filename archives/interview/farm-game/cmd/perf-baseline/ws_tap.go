package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// wsTap is a test-only transparent WebSocket witness. It records bounded,
// decoded protocol identity fields (never tickets, cookies or raw payloads)
// so an F3 browser click can be joined to Pair/Workflow evidence by operation
// id. It carries no load and is used only by the two-page correctness anchor.
type wsTap struct {
	upstream string
	writer   *os.File
	mu       sync.Mutex
	nextID   atomic.Uint64
}

func runWSTap(args []string) int {
	fs := flag.NewFlagSet("perf-baseline ws-tap", flag.ContinueOnError)
	listenAddress := fs.String("listen", "127.0.0.1:0", "loopback HTTP/WebSocket listen address")
	upstream := fs.String("upstream", "", "upstream farm-gate ws:// URL")
	eventsOut := fs.String("events-out", "", "decoded NDJSON witness path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *upstream == "" || *eventsOut == "" {
		fmt.Fprintln(os.Stderr, "ws-tap requires --upstream and --events-out")
		return 2
	}
	parsedHost, _, err := net.SplitHostPort(*listenAddress)
	if err != nil || net.ParseIP(parsedHost) == nil || !net.ParseIP(parsedHost).IsLoopback() {
		fmt.Fprintln(os.Stderr, "ws-tap listen address must be an explicit loopback IP and port")
		return 2
	}
	if !strings.HasPrefix(*upstream, "ws://127.0.0.1:") && !strings.HasPrefix(*upstream, "ws://[::1]:") {
		fmt.Fprintln(os.Stderr, "ws-tap upstream must be loopback ws://")
		return 2
	}
	file, err := os.OpenFile(*eventsOut, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create ws-tap evidence: %v\n", err)
		return 2
	}
	defer file.Close()
	tap := &wsTap{upstream: *upstream, writer: file}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ws-tap listen: %v\n", err)
		return 2
	}
	log.Printf("ws-tap listening on %s upstream=%s", listener.Addr(), *upstream)
	server := &http.Server{Handler: http.HandlerFunc(tap.handle), ReadHeaderTimeout: 5 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "ws-tap serve: %v\n", err)
		return 2
	}
	return 0
}

func (t *wsTap) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/ws" {
		http.NotFound(w, r)
		return
	}
	headers := http.Header{}
	if origin := r.Header.Get("Origin"); origin != "" {
		headers.Set("Origin", origin)
	}
	upstream, response, err := websocket.DefaultDialer.Dial(t.upstream, headers)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		http.Error(w, "diagnostic upstream unavailable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	client, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer client.Close()

	connectionID := fmt.Sprintf("tap-%06d", t.nextID.Add(1))
	t.append(map[string]any{"event": "CONNECTION_OPEN", "connection_id": connectionID})
	done := make(chan struct{}, 2)
	go t.copyFrames(connectionID, "CLIENT_TO_SERVER", client, upstream, done)
	go t.copyFrames(connectionID, "SERVER_TO_CLIENT", upstream, client, done)
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
	t.append(map[string]any{"event": "CONNECTION_CLOSED", "connection_id": connectionID})
}

func (t *wsTap) copyFrames(connectionID, direction string, source, target *websocket.Conn, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	for {
		messageType, data, err := source.ReadMessage()
		if err != nil {
			return
		}
		if messageType == websocket.BinaryMessage {
			t.append(decodeWSTapFrame(direction, connectionID, data))
		}
		if err := target.WriteMessage(messageType, data); err != nil {
			return
		}
	}
}

func (t *wsTap) append(event map[string]any) {
	event["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	t.mu.Lock()
	defer t.mu.Unlock()
	data, _ := json.Marshal(event)
	_, _ = t.writer.Write(append(data, '\n'))
}

func decodeWSTapFrame(direction, connectionID string, raw []byte) map[string]any {
	event := map[string]any{
		"event":         "FRAME",
		"direction":     direction,
		"connection_id": connectionID,
		"encoded_bytes": len(raw),
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		event["decode_error"] = true
		return event
	}
	event["sequence"] = frame.GetSequence()
	switch body := frame.GetFrameBody().(type) {
	case *commonpb.FarmFrame_ClientHello:
		// Deliberately do not retain the connection ticket.
		event["event"] = "CLIENT_HELLO"
	case *commonpb.FarmFrame_ClientCommand:
		command := body.ClientCommand
		event["event"] = "CLIENT_COMMAND"
		event["operation_id"] = command.GetOperationId()
		event["request_id"] = command.GetRequestId()
		event["command_type"] = command.GetCommandType().String()
		if command.GetCommandType() == commonpb.CommandType_THEFT {
			var theft publicpb.TheftRequest
			if err := proto.Unmarshal(command.GetPayload(), &theft); err == nil {
				event["target_farm_id"] = theft.GetTargetFarmId()
				event["plot_id"] = theft.GetPlotId()
				event["crop_cycle_id"] = theft.GetCropCycleId()
			} else {
				event["theft_payload_decode_error"] = true
			}
		}
	case *commonpb.FarmFrame_CommandResult:
		result := body.CommandResult
		event["event"] = "COMMAND_RESULT"
		event["operation_id"] = result.GetOperationId()
		event["request_id"] = result.GetRequestId()
		event["command_type"] = result.GetCommandType().String()
		event["status"] = result.GetStatus().String()
		event["retry_directive"] = result.GetRetryDirective().String()
		if result.GetFarm() != nil {
			event["target_farm_id"] = result.GetFarm().GetTargetFarmId()
			event["subscription_id"] = result.GetFarm().GetSubscriptionId()
		}
		if result.GetError() != nil {
			event["error_code"] = result.GetError().GetErrorCode().String()
		}
	case *commonpb.FarmFrame_ServerPush:
		push := body.ServerPush
		event["event"] = "SERVER_PUSH"
		event["push_type"] = push.GetPushType().String()
		if context := push.GetFarmSubscription(); context != nil {
			event["target_player_id"] = context.GetTargetPlayerId()
			event["target_farm_id"] = context.GetTargetFarmId()
			event["subscription_id"] = context.GetSubscriptionId()
		}
		switch push.GetPushType() {
		case commonpb.PushType_FARM_CHANGE:
			var changePush farmpb.FarmChangePush
			if err := proto.Unmarshal(push.GetPayload(), &changePush); err == nil && changePush.GetChange() != nil {
				change := changePush.GetChange()
				event["farm_version"] = change.GetFarmVersion()
				event["owner_id"] = change.GetOwnerId()
				if len(change.GetChangedPlots()) > 0 {
					event["crop_cycle_id"] = change.GetChangedPlots()[0].GetCropCycleId()
				}
			}
		case commonpb.PushType_REJOIN_REQUIRED:
			var rejoin farmpb.RejoinRequiredPush
			if err := proto.Unmarshal(push.GetPayload(), &rejoin); err == nil {
				event["rejoin_reason"] = rejoin.GetReason().String()
				event["invalid_from_version"] = rejoin.GetInvalidFromVersion()
			}
		}
	case *commonpb.FarmFrame_Control:
		event["event"] = "CLIENT_CONTROL"
	default:
		event["event"] = "OTHER_FRAME"
	}
	return event
}
