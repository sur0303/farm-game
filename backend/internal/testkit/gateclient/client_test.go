package gateclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/gateway/auth"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	testPlayerID  = uint64(900000001)
	testSessionID = "session-900000001"
	testTicket    = "ticket-900000001"
)

type loginRequest struct {
	path    string
	payload map[string]string
}

// loginServer answers the login path with the identity the caller expects and
// records exactly what the client sent.
func loginServer(t *testing.T, observed *loginRequest, mutate func(*LoginResult, http.ResponseWriter)) *httptest.Server {
	t.Helper()
	handler := func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode login payload: %v", err)
		}
		*observed = loginRequest{path: r.URL.Path, payload: payload}
		result := LoginResult{
			PlayerID:         "900000001",
			SessionID:        testSessionID,
			ConnectionTicket: testTicket,
		}
		http.SetCookie(w, &http.Cookie{Name: auth.HTTPSessionCookieName, Value: "cookie-value"})
		if mutate != nil {
			mutate(&result, w)
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server
}

func TestLoginSignsPlatformTokenWithoutAccount(t *testing.T) {
	var observed loginRequest
	server := loginServer(t, &observed, nil)
	config := Config{HTTPBaseURL: server.URL, WSURL: "ws://127.0.0.1:1/ws", LoadSecret: []byte("secret"), RunID: "r1"}
	login, err := config.Login(context.Background(), testPlayerID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if observed.path != "/api/v1/auth/login" {
		t.Fatalf("path=%s", observed.path)
	}
	if !strings.HasPrefix(observed.payload["platform_token"], "load:900000001:") {
		t.Fatalf("platform_token=%q", observed.payload["platform_token"])
	}
	if observed.payload["username"] != "" || observed.payload["password"] != "" {
		t.Fatalf("account credentials leaked into the token path: %+v", observed.payload)
	}
	if observed.payload["bootstrap_id"] != "gateclient-r1-900000001" {
		t.Fatalf("bootstrap_id=%q", observed.payload["bootstrap_id"])
	}
	if login.Cookie != "cookie-value" || login.SessionID != testSessionID {
		t.Fatalf("login=%+v", login)
	}
}

func TestLoginUsesAccountSessionPath(t *testing.T) {
	var observed loginRequest
	server := loginServer(t, &observed, nil)
	config := Config{HTTPBaseURL: server.URL, WSURL: "ws://127.0.0.1:1/ws", LoadSecret: []byte("secret")}
	account := AccountCredential{Username: "showcase", Password: "pass"}
	if _, err := config.Login(context.Background(), testPlayerID, &account); err != nil {
		t.Fatal(err)
	}
	if observed.path != "/api/v1/session/login" {
		t.Fatalf("path=%s", observed.path)
	}
	if observed.payload["username"] != "showcase" || observed.payload["password"] != "pass" {
		t.Fatalf("payload=%+v", observed.payload)
	}
	if observed.payload["platform_token"] != "" {
		t.Fatal("account login must not sign a load-test platform token")
	}
	if observed.payload["bootstrap_id"] != "gateclient-900000001" {
		t.Fatalf("bootstrap_id=%q", observed.payload["bootstrap_id"])
	}
}

func TestLoginFailsClosedOnIncompleteIdentity(t *testing.T) {
	cases := map[string]func(*LoginResult, http.ResponseWriter){
		"missing session": func(result *LoginResult, _ http.ResponseWriter) {
			result.SessionID = ""
		},
		"missing ticket": func(result *LoginResult, _ http.ResponseWriter) {
			result.ConnectionTicket = ""
		},
		"other player": func(result *LoginResult, _ http.ResponseWriter) {
			result.PlayerID = "900000002"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var observed loginRequest
			server := loginServer(t, &observed, mutate)
			config := Config{HTTPBaseURL: server.URL, WSURL: "ws://127.0.0.1:1/ws"}
			if _, err := config.Login(context.Background(), testPlayerID, nil); err == nil {
				t.Fatal("expected a closed failure")
			}
		})
	}
}

func TestLoginRequiresSessionCookie(t *testing.T) {
	handler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(LoginResult{
			PlayerID:         "900000001",
			SessionID:        testSessionID,
			ConnectionTicket: testTicket,
		})
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()
	config := Config{HTTPBaseURL: server.URL, WSURL: "ws://127.0.0.1:1/ws"}
	_, err := config.Login(context.Background(), testPlayerID, nil)
	if err == nil || !strings.Contains(err.Error(), "farm_session") {
		t.Fatalf("err=%v", err)
	}
}

// helloServer upgrades one connection, reads the ClientHello and answers with
// the identity the test wants to prove the client accepts or rejects.
func helloServer(t *testing.T, playerID uint64, sessionID string) (*httptest.Server, chan string) {
	t.Helper()
	tickets := make(chan string, 1)
	upgrader := websocket.Upgrader{}
	handler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(data, &frame); err != nil {
			t.Errorf("decode ClientHello frame: %v", err)
			return
		}
		tickets <- frame.GetClientHello().GetConnectionTicket()
		reply, err := proto.Marshal(&commonpb.FarmFrame{
			ProtocolVersion: 4,
			Sequence:        1,
			FrameBody: &commonpb.FarmFrame_ServerHello{ServerHello: &commonpb.ServerHello{
				PlayerId:  playerID,
				SessionId: sessionID,
			}},
		})
		if err != nil {
			t.Errorf("encode ServerHello: %v", err)
			return
		}
		_ = conn.WriteMessage(websocket.BinaryMessage, reply)
		time.Sleep(50 * time.Millisecond)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server, tickets
}

func TestConnectCompletesHandshakeWithExactIdentity(t *testing.T) {
	server, tickets := helloServer(t, testPlayerID, testSessionID)
	config := Config{
		HTTPBaseURL: "http://127.0.0.1:1",
		WSURL:       "ws" + strings.TrimPrefix(server.URL, "http") + "/ws",
	}
	login := LoginResult{PlayerID: "900000001", SessionID: testSessionID, ConnectionTicket: testTicket}
	conn, hello, err := config.Connect(context.Background(), login)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := <-tickets; got != testTicket {
		t.Fatalf("connection ticket=%q", got)
	}
	if hello.GetPlayerId() != testPlayerID || hello.GetSessionId() != testSessionID {
		t.Fatalf("hello=%+v", hello)
	}
}

func TestConnectRejectsBorrowedSession(t *testing.T) {
	server, _ := helloServer(t, testPlayerID, "session-from-a-previous-login")
	config := Config{
		HTTPBaseURL: "http://127.0.0.1:1",
		WSURL:       "ws" + strings.TrimPrefix(server.URL, "http") + "/ws",
	}
	login := LoginResult{PlayerID: "900000001", SessionID: testSessionID, ConnectionTicket: testTicket}
	conn, _, err := config.Connect(context.Background(), login)
	if err == nil {
		conn.Close()
		t.Fatal("a ServerHello carrying another session must not be accepted")
	}
	if !strings.Contains(err.Error(), "session_id") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateServerHelloIdentity(t *testing.T) {
	hello := &commonpb.ServerHello{PlayerId: testPlayerID, SessionId: testSessionID}
	if err := ValidateServerHelloIdentity(hello, testPlayerID, testSessionID); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"nil hello":     func() error { return ValidateServerHelloIdentity(nil, testPlayerID, testSessionID) },
		"zero player":   func() error { return ValidateServerHelloIdentity(hello, 0, testSessionID) },
		"empty session": func() error { return ValidateServerHelloIdentity(hello, testPlayerID, "  ") },
		"other player":  func() error { return ValidateServerHelloIdentity(hello, testPlayerID+1, testSessionID) },
		"stale session": func() error { return ValidateServerHelloIdentity(hello, testPlayerID, "stale") },
	} {
		if check() == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
}

func TestConfigValidateRequiresBothEndpoints(t *testing.T) {
	if (Config{WSURL: "ws://127.0.0.1:1/ws"}).Validate() == nil {
		t.Fatal("missing HTTP base URL must be rejected")
	}
	if (Config{HTTPBaseURL: "http://127.0.0.1:1"}).Validate() == nil {
		t.Fatal("missing WebSocket URL must be rejected")
	}
	if (Config{HTTPBaseURL: "http://127.0.0.1:1", WSURL: "ws://127.0.0.1:1/ws"}).Validate() != nil {
		t.Fatal("a complete config must validate")
	}
}

func TestAccountCredentialFromEnvRequiresBothHalves(t *testing.T) {
	t.Setenv("GATECLIENT_TEST_USER", "showcase")
	t.Setenv("GATECLIENT_TEST_PASSWORD", "")
	if _, _, err := AccountCredentialFromEnv("GATECLIENT_TEST_USER", "GATECLIENT_TEST_PASSWORD"); err == nil {
		t.Fatal("a half-configured credential must fail closed")
	}
	t.Setenv("GATECLIENT_TEST_PASSWORD", "pass")
	credential, configured, err := AccountCredentialFromEnv("GATECLIENT_TEST_USER", "GATECLIENT_TEST_PASSWORD")
	if err != nil || !configured || credential.Username != "showcase" || credential.Password != "pass" {
		t.Fatalf("credential=%+v configured=%v err=%v", credential, configured, err)
	}
	t.Setenv("GATECLIENT_TEST_USER", "")
	if _, configured, err := AccountCredentialFromEnv("GATECLIENT_TEST_USER", "GATECLIENT_TEST_PASSWORD"); err == nil || configured {
		t.Fatal("a half-configured credential must fail closed")
	}
}
