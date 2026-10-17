// Command rt2q-business-driver speaks the actual authenticated FarmFrame v4
// protocol and emits only redacted per-operation observations.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const secretEnv = "RT2Q_BUSINESS_SECRET"

type loginResult struct {
	PlayerID         string `json:"player_id"`
	ConnectionTicket string `json:"connection_ticket"`
}

type observation struct {
	Sequence        int    `json:"sequence"`
	GateIndex       int    `json:"gate_index"`
	PlayerIDHash    string `json:"player_id_sha256"`
	OperationSHA256 string `json:"operation_sha256"`
	OK              bool   `json:"ok"`
	FailureClass    string `json:"failure_class,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rt2q-business-driver failed:", classifyBusinessDriverError(err))
		os.Exit(1)
	}
}

func classifyBusinessDriverError(err error) string {
	if err == nil {
		return "none"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid business driver arguments"):
		return "invalid_args"
	case strings.Contains(msg, "matching Gate HTTP"):
		return "endpoint_mismatch"
	case strings.Contains(msg, "private business secret"):
		return "secret_missing"
	case strings.Contains(msg, "did not complete every"):
		return "incomplete_count"
	case strings.Contains(msg, "business resolves failed"):
		return "resolve_failed"
	case strings.Contains(msg, "fresh join after lease loss"):
		return "fresh_join_failed"
	case strings.Contains(msg, "T2 response was not lost"):
		return "response_loss_missed"
	case strings.Contains(msg, "unexpected rejoin reason"):
		return "rejoin_reason_mismatch"
	case strings.Contains(msg, "REJOIN_REQUIRED"):
		return "rejoin_during_join"
	case strings.Contains(msg, "Gate login was not successful status="):
		if idx := strings.LastIndex(msg, "status="); idx >= 0 {
			code := strings.TrimSpace(msg[idx+len("status="):])
			if code != "" && !strings.ContainsAny(code, " \t\r\n:/@") {
				return "login_status_" + code
			}
		}
		return "login_failed"
	case strings.Contains(msg, "Gate login"):
		return "login_failed"
	case strings.Contains(msg, "T2 replay login failed"):
		return "t2_replay_login_failed"
	case strings.Contains(msg, "T2 replay join failed"):
		return "t2_replay_join_failed"
	case strings.Contains(msg, "T2 replay failed"):
		if strings.Contains(msg, "timeout") || strings.Contains(msg, "i/o timeout") {
			return "t2_replay_timeout"
		}
		if idx := strings.Index(msg, "status="); idx >= 0 {
			detail := strings.TrimSpace(msg[idx+len("status="):])
			detail = strings.ReplaceAll(detail, " ", "_")
			if detail != "" && !strings.ContainsAny(detail, "/@") {
				return "t2_replay_" + detail
			}
		}
		return "t2_replay_failed"
	case strings.Contains(msg, "business command status="):
		if idx := strings.Index(msg, "status="); idx >= 0 {
			detail := strings.TrimSpace(msg[idx+len("status="):])
			detail = strings.ReplaceAll(detail, " ", "_")
			if detail != "" && !strings.ContainsAny(detail, "/@") {
				return "command_failed_" + detail
			}
		}
		return "command_failed"
	case strings.Contains(msg, "business command"):
		return "command_failed"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	default:
		return "other"
	}
}

func run(args []string, output io.Writer) error {
	fs := flag.NewFlagSet("rt2q-business-driver", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", "resolve", "resolve|s6-reconcile")
	httpBases := fs.String("http-bases", "", "comma-separated Gate HTTP bases")
	wsURLs := fs.String("ws-urls", "", "comma-separated Gate WebSocket URLs")
	challenge := fs.String("challenge", "", "fresh run challenge")
	count := fs.Int("count", 100, "authenticated business resolves")
	playerStart := fs.Uint64("player-start", 0, "first test player ID")
	concurrency := fs.Int("concurrency", 10, "bounded parallel clients")
	timeout := fs.Duration("timeout", 30*time.Second, "whole driver timeout")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || len(*challenge) != 32 || *playerStart == 0 {
		return errors.New("invalid business driver arguments")
	}
	httpEndpoints, websocketEndpoints := splitEndpoints(*httpBases), splitEndpoints(*wsURLs)
	if len(httpEndpoints) == 0 || len(httpEndpoints) != len(websocketEndpoints) {
		return errors.New("matching Gate HTTP and WebSocket endpoints are required")
	}
	secret := []byte(os.Getenv(secretEnv))
	if len(secret) < 32 {
		return errors.New("private business secret is required")
	}
	if *mode == "s6-reconcile" {
		if *timeout <= 0 || *timeout > 8*time.Minute || len(httpEndpoints) != 1 {
			return errors.New("invalid business driver arguments")
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		return runS6Reconcile(ctx, httpEndpoints[0], websocketEndpoints[0], *playerStart, *challenge, secret, output)
	}
	if *count <= 0 || *count > 1000 || *concurrency <= 0 || *concurrency > 64 || *timeout <= 0 || *timeout > 2*time.Minute {
		return errors.New("invalid business driver arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	type result struct {
		observation observation
		err         error
	}
	jobs := make(chan int)
	results := make(chan result, *count)
	var workers sync.WaitGroup
	workerCount := min(*concurrency, *count)
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				gate := sequence % len(httpEndpoints)
				playerID := *playerStart + uint64(sequence)
				operation := fmt.Sprintf("rt2q-%s-resolve-%d", *challenge, sequence)
				err := resolveOnce(ctx, httpEndpoints[gate], websocketEndpoints[gate], playerID, operation, secret)
				item := observation{
					Sequence: sequence + 1, GateIndex: gate, PlayerIDHash: digestText(*challenge + ":player:" + strconv.FormatUint(playerID, 10)),
					OperationSHA256: digestText(*challenge + ":operation:" + operation), OK: err == nil,
				}
				if err != nil {
					item.FailureClass = classifyBusinessDriverError(err)
				}
				results <- result{observation: item, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for sequence := 0; sequence < *count; sequence++ {
			select {
			case jobs <- sequence:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	collected := make([]result, 0, *count)
	for item := range results {
		collected = append(collected, item)
	}
	if len(collected) != *count {
		return errors.New("business driver did not complete every requested resolve")
	}
	sort.Slice(collected, func(i, j int) bool { return collected[i].observation.Sequence < collected[j].observation.Sequence })
	encoder := json.NewEncoder(output)
	for _, item := range collected {
		if err := encoder.Encode(item.observation); err != nil {
			return err
		}
		if item.err != nil {
			return errors.New("one or more production business resolves failed")
		}
	}
	return nil
}

func resolveOnce(ctx context.Context, httpBase, wsURL string, playerID uint64, operation string, secret []byte) error {
	login, err := acquireTicket(ctx, httpBase, playerID, operation, secret)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	connection, response, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return err
	}
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := writeHello(connection, 1, &commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "rt2q-business-driver/1"}); err != nil {
		return err
	}
	if err := waitServerHello(connection); err != nil {
		return err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.GetPlayerBootstrapRequest{})
	if err != nil {
		return err
	}
	requestID := "request-" + operation
	if err := writeCommand(connection, 2, &commonpb.ClientCommandEnvelope{
		OperationId: operation, RequestId: requestID, CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP, Payload: payload,
	}); err != nil {
		return err
	}
	for {
		frame, err := readFrame(connection)
		if err != nil {
			return err
		}
		result := frame.GetCommandResult()
		if result == nil || result.GetRequestId() != requestID {
			continue
		}
		if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED && result.GetStatus() != commonpb.CommandStatus_NO_CHANGE {
			return errors.New("production business command was not successful")
		}
		return nil
	}
}

func acquireTicket(ctx context.Context, httpBase string, playerID uint64, bootstrap string, secret []byte) (loginResult, error) {
	id := strconv.FormatUint(playerID, 10)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(id))
	payload, _ := json.Marshal(map[string]string{
		"platform_token": "load:" + id + ":" + hex.EncodeToString(mac.Sum(nil)),
		"bootstrap_id":   bootstrap, "client_build": "rt2q-business-driver/1",
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(httpBase, "/")+"/api/v1/auth/login", bytes.NewReader(payload))
	if err != nil {
		return loginResult{}, err
	}
	request.Header.Set("content-type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return loginResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return loginResult{}, fmt.Errorf("Gate login was not successful status=%d", response.StatusCode)
	}
	var result loginResult
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return loginResult{}, err
	}
	if result.ConnectionTicket == "" || result.PlayerID != id {
		return loginResult{}, errors.New("Gate login returned an invalid identity")
	}
	return result, nil
}

func writeHello(connection *websocket.Conn, sequence uint64, hello *commonpb.ClientHello) error {
	return writeFrame(connection, &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence, FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}})
}

func writeCommand(connection *websocket.Conn, sequence uint64, command *commonpb.ClientCommandEnvelope) error {
	return writeFrame(connection, &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence, FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: command}})
}

func writeFrame(connection *websocket.Conn, frame *commonpb.FarmFrame) error {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return err
	}
	return connection.WriteMessage(websocket.BinaryMessage, raw)
}

func readFrame(connection *websocket.Conn) (*commonpb.FarmFrame, error) {
	for {
		messageType, raw, err := connection.ReadMessage()
		if err != nil {
			return nil, err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(raw, &frame); err != nil {
			return nil, err
		}
		return &frame, nil
	}
}

func waitServerHello(connection *websocket.Conn) error {
	for {
		frame, err := readFrame(connection)
		if err != nil {
			return err
		}
		if frame.GetServerHello() != nil {
			return nil
		}
		if frame.GetControl().GetConnectionError() != nil {
			return errors.New("Gate rejected ClientHello")
		}
	}
}

func splitEndpoints(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
