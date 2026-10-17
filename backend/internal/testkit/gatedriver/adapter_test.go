package gatedriver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/testkit/capdriver"
	"farmgame/internal/testkit/gateclient"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const playerBase = uint64(900000000)

type gateStub struct {
	server      *httptest.Server
	loginStatus int
	// helloSession, when set, overrides the session id the ServerHello
	// carries, which lets a test simulate a borrowed session.
	helloSession string
	// abortBeforeHello drops the transport once the ClientHello has arrived,
	// which is the peer vanishing mid-handshake rather than answering.
	abortBeforeHello bool
	logins           []uint64
}

// newGateStub answers both the login and the WebSocket handshake the adapter
// drives, so the test exercises the real client end to end.
func newGateStub(t *testing.T) *gateStub {
	t.Helper()
	stub := &gateStub{loginStatus: http.StatusOK}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if stub.loginStatus != http.StatusOK {
			w.WriteHeader(stub.loginStatus)
			_, _ = w.Write([]byte("shed"))
			return
		}
		id := strings.TrimPrefix(payload["bootstrap_id"], "cap-gate-")
		stub.logins = append(stub.logins, parseID(t, id))
		http.SetCookie(w, &http.Cookie{Name: auth.HTTPSessionCookieName, Value: "cookie"})
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(gateclient.LoginResult{
			PlayerID:         id,
			SessionID:        "session-" + id,
			ConnectionTicket: "ticket-" + id,
		})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if stub.abortBeforeHello {
			_ = conn.UnderlyingConn().Close()
			return
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(data, &frame); err != nil {
			return
		}
		ticket := frame.GetClientHello().GetConnectionTicket()
		id := strings.TrimPrefix(ticket, "ticket-")
		session := "session-" + id
		if stub.helloSession != "" {
			session = stub.helloSession
		}
		reply, err := proto.Marshal(&commonpb.FarmFrame{
			ProtocolVersion: 4, Sequence: 1,
			FrameBody: &commonpb.FarmFrame_ServerHello{ServerHello: &commonpb.ServerHello{
				PlayerId: parseID(t, id), SessionId: session,
			}},
		})
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.BinaryMessage, reply)
		time.Sleep(20 * time.Millisecond)
	})
	stub.server = httptest.NewServer(mux)
	t.Cleanup(stub.server.Close)
	return stub
}

func parseID(t *testing.T, raw string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse player id %q: %v", raw, err)
	}
	return value
}

func (s *gateStub) config() Config {
	return Config{
		Gate: gateclient.Config{
			HTTPBaseURL:     s.server.URL,
			WSURL:           "ws" + strings.TrimPrefix(s.server.URL, "http") + "/ws",
			LoadSecret:      []byte("secret"),
			ClientBuild:     "cap-gate/1",
			BootstrapPrefix: "cap-gate",
		},
		PlayerBase: playerBase,
	}
}

func operation(sequence uint64) capdriver.OperationRequest {
	return capdriver.OperationRequest{
		SchemaVersion: capdriver.OperationSchema,
		Sequence:      sequence,
		Service:       capdriver.ServiceGate,
		Profile:       "gate-qualification",
		Phase:         "QUALIFICATION",
	}
}

func TestExecuteCompletesOneSessionPerArrival(t *testing.T) {
	stub := newGateStub(t)
	adapter, err := New(stub.config())
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		result := adapter.Execute(context.Background(), operation(sequence))
		if result.Status != capdriver.StatusCompleted || result.ErrorClass != "" {
			t.Fatalf("sequence %d result=%+v", sequence, result)
		}
		if result.Correctness[SessionIdentityMismatchGate] != 0 {
			t.Fatalf("sequence %d reported a correctness breach", sequence)
		}
	}
	if adapter.Sessions() != 3 {
		t.Fatalf("sessions=%d", adapter.Sessions())
	}
	// Every arrival must own a distinct identity, otherwise concurrent
	// sessions would evict each other and the measurement would be a
	// reconnect storm rather than an arrival rate.
	seen := map[uint64]bool{}
	for _, id := range stub.logins {
		if seen[id] {
			t.Fatalf("player %d logged in twice", id)
		}
		seen[id] = true
	}
	if len(seen) != 3 || !seen[playerBase+1] {
		t.Fatalf("logins=%v", stub.logins)
	}
}

func TestExecuteReportsBorrowedSessionAsCorrectnessStop(t *testing.T) {
	stub := newGateStub(t)
	stub.helloSession = "session-from-a-previous-login"
	adapter, err := New(stub.config())
	if err != nil {
		t.Fatal(err)
	}
	result := adapter.Execute(context.Background(), operation(1))
	if result.Status != capdriver.StatusFailed || result.ErrorClass != "CORRECTNESS_STOP" {
		t.Fatalf("result=%+v", result)
	}
	if result.Correctness[SessionIdentityMismatchGate] != 1 {
		t.Fatalf("correctness=%v", result.Correctness)
	}
}

func TestExecuteSeparatesCapacityRejectionFromUnavailability(t *testing.T) {
	for status, want := range map[int]struct {
		status     capdriver.Status
		errorClass string
	}{
		http.StatusTooManyRequests:    {capdriver.StatusRejected, "CAPACITY_REJECTED"},
		http.StatusServiceUnavailable: {capdriver.StatusRejected, "CAPACITY_REJECTED"},
		http.StatusInternalServerError: {
			capdriver.StatusFailed, "DEPENDENCY_UNAVAILABLE",
		},
	} {
		stub := newGateStub(t)
		stub.loginStatus = status
		adapter, err := New(stub.config())
		if err != nil {
			t.Fatal(err)
		}
		result := adapter.Execute(context.Background(), operation(1))
		if result.Status != want.status || result.ErrorClass != want.errorClass {
			t.Fatalf("login status %d produced %+v", status, result)
		}
		if result.Correctness[SessionIdentityMismatchGate] != 0 {
			t.Fatalf("login status %d must not report a correctness breach", status)
		}
	}
}

func TestExecuteReportsCancellationAsTimeout(t *testing.T) {
	stub := newGateStub(t)
	adapter, err := New(stub.config())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := adapter.Execute(ctx, operation(1))
	if result.Status != capdriver.StatusTimeout || result.ErrorClass != "REQUEST_TIMEOUT" {
		t.Fatalf("result=%+v", result)
	}
}

func TestResultsSatisfyTheDriverContract(t *testing.T) {
	stub := newGateStub(t)
	adapter, err := New(stub.config())
	if err != nil {
		t.Fatal(err)
	}
	server, err := capdriver.NewServer(capdriver.Config{
		BinarySHA256: strings.Repeat("a", 64), ConfigSHA256: strings.Repeat("b", 64),
		Service: capdriver.ServiceGate, Profile: "gate-qualification",
		MaximumInflight: 4, Ready: true,
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(capdriver.OperationRequest{
		SchemaVersion: capdriver.OperationSchema, Sequence: 1,
		ScheduledAt: time.Now().Format(time.RFC3339Nano),
		Service:     capdriver.ServiceGate, Profile: "gate-qualification", Phase: "LOAD_GENERATOR_QUALIFICATION",
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/operation", strings.NewReader(string(body)))
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response capdriver.OperationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != capdriver.StatusCompleted || response.Correctness == nil {
		t.Fatalf("response=%+v", response)
	}
}

func TestConfigRefusesNonLoopbackAndMissingSecret(t *testing.T) {
	stub := newGateStub(t)
	remote := stub.config()
	remote.Gate.HTTPBaseURL = "http://gate.example.com:8080"
	if _, err := New(remote); err == nil {
		t.Fatal("a non-loopback gate endpoint must be refused")
	}
	secretless := stub.config()
	secretless.Gate.LoadSecret = nil
	if _, err := New(secretless); err == nil {
		t.Fatal("a driver without a load secret must be refused")
	}
	baseless := stub.config()
	baseless.PlayerBase = 0
	if _, err := New(baseless); err == nil {
		t.Fatal("a driver without a player base must be refused")
	}
}

func TestLoadSecretFromEnvRejectsEmpty(t *testing.T) {
	t.Setenv("GATEDRIVER_TEST_SECRET", "  ")
	if _, err := LoadSecretFromEnv("GATEDRIVER_TEST_SECRET"); err == nil {
		t.Fatal("an empty secret must be refused")
	}
	t.Setenv("GATEDRIVER_TEST_SECRET", "value")
	secret, err := LoadSecretFromEnv("GATEDRIVER_TEST_SECRET")
	if err != nil || string(secret) != "value" {
		t.Fatalf("secret=%q err=%v", secret, err)
	}
}
