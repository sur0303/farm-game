// Package gateclient speaks the real farm-gate client protocol: the HTTPS
// login that issues the connection ticket and farm_session cookie, and the
// FarmFrame v4 WebSocket handshake. It is test-only infrastructure shared by
// the ws-stress tool and the capacity driver adapters; it never sends bytes
// that production would correctly reject as malformed.
package gateclient

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	defaultClientBuild     = "gateclient/1"
	defaultBootstrapPrefix = "gateclient"
	handshakeTimeout       = 5 * time.Second
	writeTimeout           = 5 * time.Second
	helloTimeout           = 10 * time.Second
)

// Config addresses one farm-gate deployment. The zero value is unusable; use
// Validate before driving load with it.
type Config struct {
	HTTPBaseURL string
	WSURL       string
	// TLSCAPath is required only for a wss:// endpoint with a private CA.
	TLSCAPath string
	// LoadSecret signs the load-test platform token. It is unused when the
	// caller supplies an account credential.
	LoadSecret []byte
	// ClientBuild and BootstrapPrefix identify the driver in login payloads
	// and in the bootstrap id, so evidence can be traced back to one tool.
	ClientBuild     string
	BootstrapPrefix string
	RunID           string
	// Frames, when set, receives one observation per frame encoded or decoded
	// by Connect. It is optional: ws-stress runs without instrumentation.
	Frames FrameCounter
	// HTTPClient, when set, carries the login request instead of the default
	// client, and NetDial, when set, opens the WebSocket socket. They are the
	// only points where a caller can reach the connection before the protocol
	// wraps it, which is what a load generator needs to measure its own wire
	// bytes and socket failures. Both are optional.
	HTTPClient *http.Client
	NetDial    func(ctx context.Context, network string, address string) (net.Conn, error)
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.HTTPBaseURL) == "" {
		return errors.New("gateclient: HTTP base URL is required")
	}
	if strings.TrimSpace(c.WSURL) == "" {
		return errors.New("gateclient: WebSocket URL is required")
	}
	return nil
}

func (c Config) clientBuild() string {
	if strings.TrimSpace(c.ClientBuild) == "" {
		return defaultClientBuild
	}
	return c.ClientBuild
}

func (c Config) bootstrapPrefix() string {
	if strings.TrimSpace(c.BootstrapPrefix) == "" {
		return defaultBootstrapPrefix
	}
	return c.BootstrapPrefix
}

func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// ErrIdentityMismatch marks a ServerHello that carried an identity other than
// the one the login issued. Callers classify it as a correctness breach rather
// than a transport failure.
var ErrIdentityMismatch = errors.New("gateclient: ServerHello identity mismatch")

// LoginStatusError carries the HTTP status a rejected login returned, so a
// caller can tell a deliberate capacity rejection from an unavailable
// dependency.
type LoginStatusError struct {
	StatusCode int
	Body       string
}

func (e *LoginStatusError) Error() string {
	return fmt.Sprintf("login status %d: %s", e.StatusCode, e.Body)
}

// AccountCredential drives the showcase account login path instead of the
// load-test platform token.
type AccountCredential struct {
	Username string
	Password string
}

// AccountCredentialFromEnv reads a username/password pair that must be
// configured together or not at all.
func AccountCredentialFromEnv(usernameEnv, passwordEnv string) (AccountCredential, bool, error) {
	username := strings.TrimSpace(os.Getenv(usernameEnv))
	password := os.Getenv(passwordEnv)
	if username == "" && password == "" {
		return AccountCredential{}, false, nil
	}
	if username == "" || password == "" {
		return AccountCredential{}, false, fmt.Errorf(
			"%s and %s must be configured together",
			usernameEnv,
			passwordEnv,
		)
	}
	return AccountCredential{Username: username, Password: password}, true, nil
}

// LoginResult is the exact identity a WSS connection must carry.
type LoginResult struct {
	PlayerID         string `json:"player_id"`
	SessionID        string `json:"session_id"`
	ConnectionTicket string `json:"connection_ticket"`
	Cookie           string `json:"cookie,omitempty"`
	// HomeBootstrap is the optional home projection attached to the login
	// response. A non-nil value that passes client validation means the client
	// must NOT resend GET_PLAYER_BOOTSTRAP; absence or invalid content falls
	// back to the RecoveryRequery command.
	HomeBootstrap *publicpb.GetPlayerBootstrapResult `json:"home_bootstrap,omitempty"`
}

// BootstrapID is the recovery identity a login presents. It stays stable for
// one player within one run so a reconnect resumes the same bootstrap.
func (c Config) BootstrapID(playerID uint64) string {
	id := strconv.FormatUint(playerID, 10)
	if runID := strings.TrimSpace(c.RunID); runID != "" {
		return c.bootstrapPrefix() + "-" + runID + "-" + id
	}
	return c.bootstrapPrefix() + "-" + id
}

// Login performs the HTTPS login. When account is non-nil the showcase session
// login path is used; otherwise the load-test platform token is signed with
// LoadSecret. The farm_session cookie is required either way, because the
// friend HTTP endpoints are unreachable without it.
func (c Config) Login(
	ctx context.Context,
	playerID uint64,
	account *AccountCredential,
) (LoginResult, error) {
	id := strconv.FormatUint(playerID, 10)
	path := "/api/v1/auth/login"
	payloadValues := map[string]string{
		"bootstrap_id": c.BootstrapID(playerID),
		"client_build": c.clientBuild(),
	}
	if account != nil {
		path = "/api/v1/session/login"
		payloadValues["username"] = account.Username
		payloadValues["password"] = account.Password
	} else {
		mac := hmac.New(sha256.New, c.LoadSecret)
		_, _ = mac.Write([]byte(id))
		payloadValues["platform_token"] = "load:" + id + ":" + hex.EncodeToString(mac.Sum(nil))
	}
	payload, _ := json.Marshal(payloadValues)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.HTTPBaseURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return LoginResult{}, err
	}
	request.Header.Set("content-type", "application/json")
	response, err := c.httpClient().Do(request)
	if err != nil {
		return LoginResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		return LoginResult{}, &LoginStatusError{StatusCode: response.StatusCode, Body: string(body)}
	}
	var result LoginResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return result, err
	}
	if result.ConnectionTicket == "" || result.PlayerID != id {
		return result, errors.New("login returned invalid identity/ticket")
	}
	if strings.TrimSpace(result.SessionID) == "" {
		return result, errors.New("login returned no session_id; WSS ServerHello identity cannot be validated")
	}
	cookie := ""
	for _, cookieValue := range response.Cookies() {
		if cookieValue.Name == auth.HTTPSessionCookieName {
			cookie = cookieValue.Value
			break
		}
	}
	if cookie == "" {
		return result, errors.New("login did not issue the farm_session http cookie; friend HTTP endpoints unavailable")
	}
	result.Cookie = cookie
	return result, nil
}

// Dialer builds a WebSocket dialer, trusting TLSCAPath when the endpoint is
// wss:// with a private CA.
func (c Config) Dialer() (websocket.Dialer, error) {
	dialer := websocket.Dialer{HandshakeTimeout: handshakeTimeout, NetDialContext: c.NetDial}
	if strings.TrimSpace(c.TLSCAPath) == "" {
		return dialer, nil
	}
	pem, err := os.ReadFile(c.TLSCAPath)
	if err != nil {
		return dialer, fmt.Errorf("read TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return dialer, errors.New("TLS CA contains no certificate")
	}
	dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	return dialer, nil
}

// Connect dials the stable WebSocket URL and completes the FarmFrame v4
// handshake, returning only after ServerHello proves the connection carries
// the exact identity the login issued. The caller owns the returned
// connection and must close it.
func (c Config) Connect(
	ctx context.Context,
	login LoginResult,
) (*websocket.Conn, *commonpb.ServerHello, error) {
	playerID, err := strconv.ParseUint(login.PlayerID, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("login player id %q: %w", login.PlayerID, err)
	}
	dialer, err := c.Dialer()
	if err != nil {
		return nil, nil, err
	}
	conn, response, err := dialer.DialContext(ctx, c.WSURL, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return nil, nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	if err := WriteFrame(conn, 1, FrameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      c.clientBuild(),
	}); err != nil {
		conn.Close()
		return nil, nil, err
	}
	c.countEncoded()
	hello, err := readServerHelloForPlayer(conn, playerID, login.SessionID, c.Frames)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, hello, nil
}

// FrameCounter records protocol work as it happens. A load generator that
// cannot report its own encode and decode rate cannot be told apart from the
// service it is measuring when throughput stops rising.
type FrameCounter interface {
	ObserveFrameEncoded()
	ObserveFrameDecoded()
}

func (c Config) countEncoded() {
	if c.Frames != nil {
		c.Frames.ObserveFrameEncoded()
	}
}

// FrameKind is a local enum for the FarmFrame top-level oneof classes
// (business commands do not use the oneof).
type FrameKind uint8

const (
	FrameKindClientHello FrameKind = iota + 1
	FrameKindServerHello
	FrameKindClientCommand
	FrameKindCommandResult
	FrameKindServerPush
	FrameKindConnectionError
	FrameKindControl
)

// WriteFrame encodes one client frame in the v4 wire format
// (protocol_version=4, uint64 sequence, frame_body oneof).
func WriteFrame(
	conn *websocket.Conn,
	sequence uint64,
	kind FrameKind,
	body proto.Message,
) error {
	var frame *commonpb.FarmFrame
	switch kind {
	case FrameKindClientHello:
		hello, ok := body.(*commonpb.ClientHello)
		if !ok {
			return errors.New("client hello frame requires *commonpb.ClientHello")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
	case FrameKindClientCommand:
		cmd, ok := body.(*commonpb.ClientCommandEnvelope)
		if !ok {
			return errors.New("client command frame requires *commonpb.ClientCommandEnvelope")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: cmd}}
	case FrameKindControl:
		control, ok := body.(*commonpb.ControlFrame)
		if !ok {
			return errors.New("control frame requires *commonpb.ControlFrame")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_Control{Control: control}}
	default:
		return fmt.Errorf("unsupported outbound frame kind %d", kind)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

// ReadFrame reads the next binary frame and resolves its top-level class. A
// server-scoped Control(ConnectionError) maps to the local ConnectionError
// class.
func ReadFrame(conn *websocket.Conn) (FrameKind, []byte, error) {
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return 0, nil, err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(data, &frame); err != nil {
			return 0, nil, err
		}
		marshal := func(msg proto.Message) ([]byte, error) {
			return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		}
		switch body := frame.GetFrameBody().(type) {
		case *commonpb.FarmFrame_ServerHello:
			raw, err := marshal(body.ServerHello)
			return FrameKindServerHello, raw, err
		case *commonpb.FarmFrame_CommandResult:
			raw, err := marshal(body.CommandResult)
			return FrameKindCommandResult, raw, err
		case *commonpb.FarmFrame_ServerPush:
			raw, err := marshal(body.ServerPush)
			return FrameKindServerPush, raw, err
		case *commonpb.FarmFrame_Control:
			if connErr := body.Control.GetConnectionError(); connErr != nil {
				raw, err := marshal(connErr)
				return FrameKindConnectionError, raw, err
			}
			return 0, nil, errors.New("unexpected client-scoped control frame from server")
		default:
			return 0, nil, fmt.Errorf("unexpected FarmFrame body %T", frame.GetFrameBody())
		}
	}
}

// ReadUntil discards frames until the requested class arrives.
func ReadUntil(conn *websocket.Conn, kind FrameKind) error {
	for {
		got, _, err := ReadFrame(conn)
		if err != nil {
			return err
		}
		if got == FrameKindConnectionError {
			return errors.New("server returned ConnectionError")
		}
		if got == kind {
			return nil
		}
	}
}

// ReadServerHelloForPlayer decodes ServerHello and requires player_id and
// session_id to match the exact HTTPS login identity, so a mismatched or
// borrowed session can never count as readiness.
func ReadServerHelloForPlayer(
	conn *websocket.Conn,
	wantPlayerID uint64,
	wantSessionID string,
) (*commonpb.ServerHello, error) {
	return readServerHelloForPlayer(conn, wantPlayerID, wantSessionID, nil)
}

func readServerHelloForPlayer(
	conn *websocket.Conn,
	wantPlayerID uint64,
	wantSessionID string,
	frames FrameCounter,
) (*commonpb.ServerHello, error) {
	for {
		got, body, err := ReadFrame(conn)
		if err != nil {
			return nil, err
		}
		if frames != nil {
			frames.ObserveFrameDecoded()
		}
		if got == FrameKindConnectionError {
			return nil, errors.New("server returned ConnectionError")
		}
		if got != FrameKindServerHello {
			continue
		}
		var hello commonpb.ServerHello
		if err := proto.Unmarshal(body, &hello); err != nil {
			return nil, fmt.Errorf("decode ServerHello: %w", err)
		}
		if err := ValidateServerHelloIdentity(&hello, wantPlayerID, wantSessionID); err != nil {
			return nil, err
		}
		return &hello, nil
	}
}

// ValidateServerHelloIdentity requires the exact login player and login
// session identity. A ServerHello that only shares the player id (for example
// a stale session from a previous login) is rejected: evidence must be bound
// to the exact HTTPS login session that issued the farm_session cookie.
func ValidateServerHelloIdentity(
	hello *commonpb.ServerHello,
	wantPlayerID uint64,
	wantSessionID string,
) error {
	if hello == nil {
		return errors.New("ServerHello is nil")
	}
	if wantPlayerID == 0 {
		return errors.New("expected ServerHello player_id is zero")
	}
	if strings.TrimSpace(wantSessionID) == "" {
		return errors.New("expected ServerHello session_id is empty; login session was not preserved")
	}
	if hello.GetPlayerId() != wantPlayerID {
		return fmt.Errorf("%w: ServerHello player_id=%d does not match expected identity %d",
			ErrIdentityMismatch, hello.GetPlayerId(), wantPlayerID)
	}
	if hello.GetSessionId() != wantSessionID {
		return fmt.Errorf("%w: ServerHello session_id=%q does not match the exact HTTPS login session %q",
			ErrIdentityMismatch, hello.GetSessionId(), wantSessionID)
	}
	return nil
}
