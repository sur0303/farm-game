package farmbotclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	wireprotocol "farmgame/internal/gateway/protocol"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed       = errors.New("farmbotclient: session is closed")
	ErrNotConnected = errors.New("farmbotclient: session is not connected")
	ErrProtocol     = errors.New("farmbotclient: protocol error")
)

// ProtocolError means the peer or transport violated the v4 wire contract.
// The connection is discarded when this error is produced.
type ProtocolError struct {
	Action string
	Cause  error
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ErrProtocol.Error()
	}
	if e.Action == "" {
		return fmt.Sprintf("%s: %v", ErrProtocol, e.Cause)
	}
	return fmt.Sprintf("%s (%s): %v", ErrProtocol, e.Action, e.Cause)
}

func (e *ProtocolError) Unwrap() error { return ErrProtocol }

// RemoteConnectionError is the structured server-side ConnectionError.
type RemoteConnectionError struct {
	Code    commonpb.ErrorCode
	Message string
}

func (e *RemoteConnectionError) Error() string {
	if e == nil {
		return "farmbotclient: remote connection error"
	}
	return fmt.Sprintf("farmbotclient: remote connection error code=%s", e.Code)
}

// RejectedError is returned together with the original CommandResult for a
// terminal REJECTED status. The result remains available to callers so the
// complete generated ErrorDetail and retry directive are preserved.
type RejectedError struct {
	OperationID    string
	RequestID      string
	Code           commonpb.ErrorCode
	Message        string
	RetryDirective commonpb.RetryDirective
}

func (e *RejectedError) Error() string {
	if e == nil {
		return "farmbotclient: command rejected"
	}
	if e.Message == "" {
		return fmt.Sprintf("farmbotclient: operation rejected code=%s", e.Code)
	}
	return fmt.Sprintf("farmbotclient: operation rejected code=%s message=%s", e.Code, e.Message)
}

// UnexpectedStatusError marks a terminal or intermediate status that is not
// valid for the operation currently being driven.
type UnexpectedStatusError struct {
	OperationID string
	Status      commonpb.CommandStatus
}

func (e *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("farmbotclient: operation %q returned unexpected status %s", e.OperationID, e.Status)
}

// OperationExpiredError means the server can no longer resolve a previously
// accepted operation. It is deliberately distinct from a transport timeout:
// retrying the same lookup cannot recover an expired durable outcome.
type OperationExpiredError struct {
	OperationID string
}

func (e *OperationExpiredError) Error() string {
	return fmt.Sprintf("farmbotclient: operation %q result expired", e.OperationID)
}

// OperationResolutionRequest identifies one original business operation for
// GET_OPERATION_RESULT. The locator is an opaque server hint; identity still
// comes from the authenticated player, command, operation and target farm.
type OperationResolutionRequest struct {
	CommandType            commonpb.CommandType
	OperationID            string
	RequestID              string
	TargetFarmID           uint64
	ServerOperationLocator string
}

// CommandResult is the generated v4 command-result envelope.
type CommandResult = commonpb.CommandResultEnvelope

// ExecuteRequest describes one typed command. Exactly one of Payload and
// PayloadBytes may be supplied; Payload is marshaled deterministically.
type ExecuteRequest struct {
	OperationID string
	RequestID   string
	CommandType commonpb.CommandType

	Payload      proto.Message
	PayloadBytes []byte
}

func (r ExecuteRequest) normalized() (ExecuteRequest, error) {
	if err := validateID(r.OperationID, "operation_id"); err != nil {
		return ExecuteRequest{}, err
	}
	if err := validateID(r.RequestID, "request_id"); err != nil {
		return ExecuteRequest{}, err
	}
	if r.CommandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
		return ExecuteRequest{}, errors.New("farmbotclient: command_type is unspecified")
	}
	if r.CommandType == commonpb.CommandType_JOIN_FARM {
		return ExecuteRequest{}, errors.New("farmbotclient: use JoinFarm for JOIN_FARM")
	}
	if r.Payload != nil && r.PayloadBytes != nil {
		return ExecuteRequest{}, errors.New("farmbotclient: both typed payload and payload bytes supplied")
	}
	if r.Payload != nil {
		payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r.Payload)
		if err != nil {
			return ExecuteRequest{}, fmt.Errorf("farmbotclient: marshal typed payload: %w", err)
		}
		r.PayloadBytes = payload
	}
	r.Payload = nil
	r.PayloadBytes = append([]byte(nil), r.PayloadBytes...)
	return r, nil
}

func validateID(value, field string) error {
	if len(value) == 0 || len(value) > 64 || strings.TrimSpace(value) == "" {
		return fmt.Errorf("farmbotclient: %s must be 1..64 non-blank bytes", field)
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return fmt.Errorf("farmbotclient: %s must be printable ASCII", field)
		}
	}
	return nil
}

func requestIDFor(operationID string) string {
	candidate := "request-" + operationID
	if len(candidate) <= 64 {
		return candidate
	}
	digest := sha256.Sum256([]byte(operationID))
	return "request-" + hex.EncodeToString(digest[:])
}

// Client owns the immutable transport configuration and authenticator.
type Client struct {
	config        Config
	authenticator Authenticator
}

// NewClient constructs a client. With no authenticator argument it uses the
// platform HMAC authenticator; one injected authenticator is useful for
// tests or another explicitly supported login provider.
func NewClient(config Config, authenticators ...Authenticator) (*Client, error) {
	if len(authenticators) > 1 {
		return nil, errors.New("farmbotclient: at most one authenticator may be supplied")
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	var authenticator Authenticator
	if len(authenticators) == 1 {
		if authenticators[0] == nil {
			return nil, errors.New("farmbotclient: authenticator is nil")
		}
		authenticator = authenticators[0]
	} else {
		authenticator, err = NewPlatformHMACAuthenticator(normalized)
		if err != nil {
			return nil, err
		}
	}
	return &Client{config: normalized, authenticator: authenticator}, nil
}

// Login delegates to the configured authenticator.
func (c *Client) Login(ctx context.Context, credentials Credentials) (LoginResult, error) {
	if c == nil || c.authenticator == nil {
		return LoginResult{}, errors.New("farmbotclient: client authenticator is nil")
	}
	return c.authenticator.Login(ctx, credentials)
}

// Authenticate is an explicit alias for Login.
func (c *Client) Authenticate(ctx context.Context, credentials Credentials) (LoginResult, error) {
	return c.Login(ctx, credentials)
}

// Connect dials WSS and completes ClientHello/ServerHello validation for an
// already authenticated LoginResult. Reconnect requires LoginAndConnect so
// the caller's Credentials are retained by the Session.
func (c *Client) Connect(ctx context.Context, login LoginResult) (*Session, error) {
	return c.connect(ctx, login, nil)
}

// LoginAndConnect performs HTTP HMAC login followed by the v4 WebSocket
// handshake and exact player/session identity check.
func (c *Client) LoginAndConnect(ctx context.Context, credentials Credentials) (*Session, error) {
	login, err := c.Login(ctx, credentials)
	if err != nil {
		return nil, err
	}
	return c.ConnectAuthenticated(ctx, login, credentials)
}

// ConnectAuthenticated completes the ClientHello/ServerHello phase after a
// caller-observed Login. Unlike Connect, it safely retains a copy of the
// credentials so the returned Session can perform a full Reconnect.
func (c *Client) ConnectAuthenticated(ctx context.Context, login LoginResult, credentials Credentials) (*Session, error) {
	if c == nil {
		return nil, errors.New("farmbotclient: client is nil")
	}
	if credentials.PlayerID == 0 {
		return nil, errors.New("farmbotclient: credentials player id must be non-zero")
	}
	if len(credentials.Secret) == 0 {
		return nil, errors.New("farmbotclient: platform HMAC secret must be non-empty")
	}
	if credentials.PlayerID != login.PlayerID {
		return nil, errors.New("farmbotclient: credentials player id does not match login player id")
	}
	retained := credentials.clone()
	return c.connect(ctx, login, &retained)
}

func (c *Client) connect(ctx context.Context, login LoginResult, credentials *Credentials) (*Session, error) {
	if c == nil {
		return nil, errors.New("farmbotclient: client is nil")
	}
	if err := validateLoginResult(login); err != nil {
		return nil, err
	}
	session := &Session{
		client: c, login: login, outboundSequence: 1,
	}
	if credentials != nil {
		copy := credentials.clone()
		session.credentials = &copy
	}
	if err := session.connect(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

// LoginAndJoin runs the complete login, hello and JoinFarm barrier. The
// supplied operation ID is sent unchanged; request ID is generated only when
// omitted.
func (c *Client) LoginAndJoin(ctx context.Context, credentials Credentials, targetFarmID uint64, operationID string, requestID ...string) (*Session, *JoinResult, error) {
	session, err := c.LoginAndConnect(ctx, credentials)
	if err != nil {
		return nil, nil, err
	}
	joined, err := session.JoinFarm(ctx, targetFarmID, operationID, requestID...)
	if err != nil {
		_ = session.Close()
		return nil, nil, err
	}
	return session, joined, nil
}

func validateLoginResult(login LoginResult) error {
	if login.PlayerID == 0 || strings.TrimSpace(login.SessionID) == "" || strings.TrimSpace(login.ConnectionTicket) == "" {
		return errors.New("farmbotclient: login result is missing player/session/ticket identity")
	}
	if strings.TrimSpace(login.CookieValue()) == "" {
		return errors.New("farmbotclient: login result is missing the farm_session cookie")
	}
	return nil
}

// CookieValue returns the farm_session value while accepting either exported
// spelling on a manually constructed LoginResult.
func (r LoginResult) CookieValue() string {
	if r.Cookie != "" {
		return r.Cookie
	}
	return r.FarmSessionCookie
}

// JoinResult is the validated three-part JoinFarm barrier result.
type JoinResult struct {
	TargetFarmID   uint64
	SubscriptionID string
	JoinAttemptID  string
	Snapshot       *farmpb.FarmSnapshot
}

// Session is a single authenticated WebSocket connection. A background reader
// owns the read side for the whole life of the connection, so the session
// answers server pings and drains broadcast pushes even while the bot is
// idle. Writes are serialized by mu; callers wait for their own operation
// outside the lock, so a slow command never blocks an unrelated one.
type Session struct {
	client      *Client
	login       LoginResult
	credentials *Credentials

	// opMu serializes whole barrier operations. Join and reconnect rebuild
	// session-wide state and must not interleave with each other.
	opMu sync.Mutex

	mu     sync.Mutex
	conn   *websocket.Conn
	pump   *framePump
	closed bool
	// inboundSequence only covers the handshake; the pump owns it afterwards.
	outboundSequence uint64
	inboundSequence  uint64
	joinedFarmID     uint64
	subscriptionID   string
	retiredStats     SessionStats
	phaseObserver    PhaseObserver

	// resultQueries is read by Stats while a resolution is in flight, so it is
	// atomic rather than guarded by mu.
	resultQueries atomic.Uint64

	// farmView is nil unless the caller asked to track the realtime stream.
	farmView *FarmView
}

// FarmView returns the client-side farm state maintained from the join
// snapshot and the realtime stream, or nil when tracking is disabled.
func (s *Session) FarmView() *FarmView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.farmView
}

// observeBroadcast keeps the farm view current from pushes the background
// reader drained. It runs on the reader goroutine.
func (s *Session) observeBroadcast(push *commonpb.ServerPushEnvelope) {
	view := s.FarmView()
	if view == nil || push == nil {
		return
	}
	switch push.GetPushType() {
	case commonpb.PushType_FARM_CHANGE:
		var change farmpb.FarmChangePush
		if err := proto.Unmarshal(push.GetPayload(), &change); err != nil {
			view.Invalidate()
			return
		}
		view.Apply(change.GetChange())
	case commonpb.PushType_REJOIN_REQUIRED, commonpb.PushType_KICK:
		view.Invalidate()
	}
}

// PhaseObserver receives bounded latency samples for the wire phases a
// caller cannot time from the outside. Implementations must be safe for
// concurrent use and must not block.
type PhaseObserver interface {
	ObservePhase(phase string, elapsed time.Duration)
}

// Wire phases reported to a PhaseObserver. Command admission is the time to
// the first server verdict; resolution is the extra time a PROCESSING
// operation spends reaching its durable outcome.
const (
	PhaseAdmission  = "admission"
	PhaseResolution = "resolution"
)

// SetPhaseObserver installs the phase sink for this session. Passing nil
// removes it.
func (s *Session) SetPhaseObserver(observer PhaseObserver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phaseObserver = observer
}

func (s *Session) observePhase(phase string, elapsed time.Duration) {
	s.mu.Lock()
	observer := s.phaseObserver
	s.mu.Unlock()
	if observer != nil && elapsed > 0 {
		observer.ObservePhase(phase, elapsed)
	}
}

// Stats reports what the background reader has drained on this session,
// including connections already replaced by Reconnect.
func (s *Session) Stats() SessionStats {
	s.mu.Lock()
	total := s.retiredStats
	pump := s.pump
	s.mu.Unlock()
	if pump != nil {
		total.Add(pump.snapshot())
	}
	// Result queries are sent by the session rather than seen by a pump, and
	// they survive a reconnect, so they are held here rather than per
	// connection.
	total.ResultQueries += s.resultQueries.Load()
	return total
}

func (s *Session) connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectLocked(ctx)
}

func (s *Session) connectLocked(ctx context.Context) error {
	if s.closed {
		return ErrClosed
	}
	if s.conn != nil {
		s.discardConnectionLocked()
	}
	tlsConfig, err := s.client.config.tlsConfig()
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: s.client.config.Timeout, TLSClientConfig: tlsConfig}
	header := make(http.Header)
	header.Set("Cookie", (&http.Cookie{Name: FarmSessionCookieName, Value: s.login.CookieValue()}).String())
	conn, response, err := dialer.DialContext(ctx, s.client.config.WSURL, header)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return fmt.Errorf("farmbotclient: websocket dial: %w", err)
	}
	s.conn = conn
	s.outboundSequence = 1
	s.inboundSequence = 0
	s.conn.SetReadLimit(int64(s.client.config.MaxFrameBytes))
	if err := s.writeFrameLocked(ctx, &commonpb.FarmFrame{
		FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: &commonpb.ClientHello{
			ConnectionTicket: s.login.ConnectionTicket,
			ClientBuild:      s.client.config.ClientBuild,
		}},
	}); err != nil {
		s.discardConnectionLocked()
		return err
	}
	frame, err := s.readFrameLocked(ctx)
	if err != nil {
		return err
	}
	hello := frame.GetServerHello()
	if hello == nil {
		s.discardConnectionLocked()
		return protocolFailure("hello", errors.New("expected ServerHello as first server frame"))
	}
	if hello.GetPlayerId() != s.login.PlayerID {
		s.discardConnectionLocked()
		return protocolFailure("hello identity", errors.New("ServerHello player_id does not match login"))
	}
	if strings.TrimSpace(hello.GetSessionId()) == "" || hello.GetSessionId() != s.login.SessionID {
		s.discardConnectionLocked()
		return protocolFailure("hello identity", errors.New("ServerHello session_id does not match login"))
	}
	// The handshake is the last synchronous read; the pump owns the read side
	// from here so the connection stays responsive while the bot is idle.
	var observePush func(*commonpb.ServerPushEnvelope)
	if s.client.config.TrackFarmView {
		if s.farmView == nil {
			s.farmView = NewFarmView()
		}
		observePush = s.observeBroadcast
	}
	s.pump = startFramePump(s.conn, s.client.config.MaxFrameBytes, s.client.config.IdleTimeout, s.inboundSequence, observePush)
	return nil
}

// Close permanently closes this Session. Reconnect must be called before an
// explicit Close; network failures leave the Session reconnectable.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.joinedFarmID = 0
	if s.conn == nil {
		s.mu.Unlock()
		return nil
	}
	pump := s.pump
	err := s.conn.Close()
	s.discardConnectionLocked()
	s.mu.Unlock()
	// Waiting outside the lock lets the reader goroutine finish any in-flight
	// pong or delivery without deadlocking against Close.
	if pump != nil {
		pump.wait()
	}
	return err
}

// IsConnected reports whether a live connection is currently available.
func (s *Session) IsConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.conn != nil
}

func (s *Session) LoginResult() LoginResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.login
}

func (s *Session) PlayerID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.login.PlayerID
}

func (s *Session) SubscriptionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptionID
}

func (s *Session) discardConnectionLocked() {
	if s.pump != nil {
		s.retiredStats.Add(s.pump.snapshot())
		s.pump.stop()
		s.pump = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = nil
	s.outboundSequence = 0
	s.inboundSequence = 0
	s.subscriptionID = ""
}

func (s *Session) discardConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discardConnectionLocked()
}

// activePump returns the reader for the current connection.
func (s *Session) activePump() (*framePump, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.conn == nil || s.pump == nil {
		return nil, ErrNotConnected
	}
	return s.pump, nil
}

func (s *Session) writeFrame(ctx context.Context, frame *commonpb.FarmFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.writeFrameLocked(ctx, frame)
}

// waitContext bounds one operation. Without it a caller that passes a
// context without a deadline would wait forever for a verdict that the
// server may never send.
func (s *Session) waitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.client.config.Timeout)
}

func (s *Session) writeFrameLocked(ctx context.Context, frame *commonpb.FarmFrame) error {
	if s.conn == nil {
		return ErrNotConnected
	}
	if frame == nil {
		return protocolFailure("write", errors.New("nil FarmFrame"))
	}
	frame.ProtocolVersion = ProtocolVersion
	frame.Sequence = s.outboundSequence
	raw, err := wireprotocol.Encode(frame, s.client.config.MaxFrameBytes)
	if err != nil {
		return protocolFailure("encode", err)
	}
	if err := s.setWriteDeadline(ctx); err != nil {
		return err
	}
	if err := s.conn.WriteMessage(websocket.BinaryMessage, raw); err != nil {
		s.discardConnectionLocked()
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("farmbotclient: websocket write: %w", err)
	}
	if s.outboundSequence == ^uint64(0) {
		s.discardConnectionLocked()
		return protocolFailure("write", errors.New("outbound sequence exhausted"))
	}
	s.outboundSequence++
	return nil
}

func (s *Session) readFrameLocked(ctx context.Context) (*commonpb.FarmFrame, error) {
	if s.conn == nil {
		return nil, ErrNotConnected
	}
	if err := s.setReadDeadline(ctx); err != nil {
		return nil, err
	}
	messageType, raw, err := s.conn.ReadMessage()
	if err != nil {
		s.discardConnectionLocked()
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("farmbotclient: websocket read: %w", err)
	}
	if messageType != websocket.BinaryMessage {
		s.discardConnectionLocked()
		return nil, protocolFailure("read", errors.New("only binary WebSocket messages are accepted"))
	}
	frame, err := wireprotocol.Decode(raw, s.client.config.MaxFrameBytes)
	if err != nil {
		s.discardConnectionLocked()
		return nil, protocolFailure("decode", err)
	}
	if err := wireprotocol.ValidateServerFrame(frame); err != nil {
		s.discardConnectionLocked()
		return nil, protocolFailure("server direction", err)
	}
	expected := uint64(1)
	if s.inboundSequence != 0 {
		if s.inboundSequence == ^uint64(0) {
			s.discardConnectionLocked()
			return nil, protocolFailure("read sequence", errors.New("inbound sequence exhausted"))
		}
		expected = s.inboundSequence + 1
	}
	if frame.GetSequence() != expected {
		s.discardConnectionLocked()
		return nil, protocolFailure("read sequence", fmt.Errorf("got %d, want %d", frame.GetSequence(), expected))
	}
	s.inboundSequence = frame.GetSequence()
	if control := frame.GetControl(); control != nil {
		if connectionError := control.GetConnectionError(); connectionError != nil {
			s.discardConnectionLocked()
			return nil, &RemoteConnectionError{Code: connectionError.GetErrorCode(), Message: connectionError.GetErrorMessage()}
		}
	}
	return frame, nil
}

func protocolFailure(action string, cause error) error {
	return &ProtocolError{Action: action, Cause: cause}
}

func (s *Session) deadline(ctx context.Context) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	deadline := time.Now().Add(s.client.config.Timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return deadline, nil
}

func (s *Session) setReadDeadline(ctx context.Context) error {
	deadline, err := s.deadline(ctx)
	if err != nil {
		return err
	}
	return s.conn.SetReadDeadline(deadline)
}

func (s *Session) setWriteDeadline(ctx context.Context) error {
	deadline, err := s.deadline(ctx)
	if err != nil {
		return err
	}
	return s.conn.SetWriteDeadline(deadline)
}

// Execute sends a typed command and returns the first matching result.
// PROCESSING is a successful admission result and is returned to the caller;
// it is not guaranteed that a later terminal result will arrive on this WSS.
// Recoverable typed helpers use ResolveOperation with the same operation_id.
// REJECTED returns the result plus a structured *RejectedError.
func (s *Session) Execute(ctx context.Context, request ExecuteRequest) (*CommandResult, error) {
	normalized, err := request.normalized()
	if err != nil {
		return nil, err
	}
	pump, err := s.activePump()
	if err != nil {
		return nil, err
	}
	waiter, err := pump.expect(normalized.OperationID)
	if err != nil {
		return nil, err
	}
	defer pump.release(normalized.OperationID)

	started := time.Now()
	if err := s.writeFrame(ctx, &commonpb.FarmFrame{
		FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
			OperationId: normalized.OperationID,
			RequestId:   normalized.RequestID,
			CommandType: normalized.CommandType,
			Payload:     normalized.PayloadBytes,
		}},
	}); err != nil {
		return nil, err
	}
	waitCtx, cancel := s.waitContext(ctx)
	defer cancel()
	result, err := pump.await(waitCtx, waiter)
	if err != nil {
		return nil, err
	}
	if normalized.CommandType != commonpb.CommandType_GET_OPERATION_RESULT {
		s.observePhase(PhaseAdmission, time.Since(started))
	}
	if result.GetRequestId() != normalized.RequestID {
		return nil, protocolFailure("result identity", errors.New("command result request_id does not match request"))
	}
	if result.GetCommandType() != commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED && result.GetCommandType() != normalized.CommandType {
		return nil, protocolFailure("result identity", errors.New("command result command_type does not match request"))
	}
	switch result.GetStatus() {
	case commonpb.CommandStatus_PROCESSING:
		return result, nil
	case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_NO_CHANGE:
		return result, nil
	case commonpb.CommandStatus_REJECTED:
		return result, rejectedError(result)
	default:
		return result, &UnexpectedStatusError{OperationID: normalized.OperationID, Status: result.GetStatus()}
	}
}

const (
	defaultOperationResultPoll = 250 * time.Millisecond
	minOperationResultPoll     = 100 * time.Millisecond
	maxOperationResultPoll     = 5 * time.Second
)

// ResolveOperation polls GET_OPERATION_RESULT until the original operation
// reaches a durable terminal outcome or ctx ends. It never changes the
// original operation_id and validates every returned identity before exposing
// the terminal result to the caller.
func (s *Session) ResolveOperation(ctx context.Context, request OperationResolutionRequest) (*CommandResult, error) {
	if err := validateID(request.OperationID, "operation_id"); err != nil {
		return nil, err
	}
	if err := validateID(request.RequestID, "request_id"); err != nil {
		return nil, err
	}
	if request.CommandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED || request.CommandType == commonpb.CommandType_GET_OPERATION_RESULT {
		return nil, errors.New("farmbotclient: original command_type is invalid for result resolution")
	}
	if request.TargetFarmID == 0 {
		return nil, errors.New("farmbotclient: target farm id is required for result resolution")
	}
	if len(request.ServerOperationLocator) > 4096 {
		return nil, errors.New("farmbotclient: server operation locator is too long")
	}

	started := time.Now()
	defer func() { s.observePhase(PhaseResolution, time.Since(started)) }()

	// Register before the first query. The server may settle the operation
	// while that query is still in flight, and a watch installed afterwards
	// would miss the notification and fall back to the retry timer.
	s.mu.Lock()
	pump := s.pump
	s.mu.Unlock()
	var settled <-chan struct{}
	if pump != nil {
		var release func()
		settled, release = pump.watchOperation(request.OperationID)
		defer release()
	}

	for attempt := uint64(1); ; attempt++ {
		queryOperationID, queryRequestID := operationQueryIDs(request.OperationID, attempt)
		lookupRequest := &publicpb.GetOperationResultRequest{
			OriginalCommandType: request.CommandType,
			OriginalOperationId: request.OperationID,
			OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_TargetFarmId{
				TargetFarmId: request.TargetFarmID,
			}},
		}
		if request.ServerOperationLocator != "" {
			locator := request.ServerOperationLocator
			lookupRequest.ServerOperationLocator = &locator
		}
		s.resultQueries.Add(1)
		result, err := s.Execute(ctx, ExecuteRequest{
			OperationID: queryOperationID,
			RequestID:   queryRequestID,
			CommandType: commonpb.CommandType_GET_OPERATION_RESULT,
			Payload:     lookupRequest,
		})
		if err != nil {
			return nil, err
		}
		if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
			if err := waitOperationResult(ctx, settled, s.retryAfter(result.GetRetryAfterMs(), attempt)); err != nil {
				return nil, err
			}
			continue
		}

		var lookup publicpb.GetOperationResultResult
		if err := proto.Unmarshal(result.GetPayload(), &lookup); err != nil {
			return nil, protocolFailure("decode operation result", err)
		}
		if lookup.GetOriginalCommandType() != request.CommandType || lookup.GetOriginalOperationId() != request.OperationID {
			return nil, protocolFailure("operation result identity", errors.New("lookup identity does not match original operation"))
		}
		switch lookup.GetLookupStatus() {
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND,
			publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING:
			if err := waitOperationResult(ctx, settled, s.retryAfter(lookup.GetRetryAfterMs(), attempt)); err != nil {
				return nil, err
			}
			continue
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_EXPIRED:
			return nil, &OperationExpiredError{OperationID: request.OperationID}
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED:
			return resolvedCommandResult(request, &lookup)
		default:
			return nil, protocolFailure("operation result status", errors.New("lookup returned unspecified status"))
		}
	}
}

func resolvedCommandResult(request OperationResolutionRequest, lookup *publicpb.GetOperationResultResult) (*CommandResult, error) {
	outcome := lookup.GetCompletedOutcome()
	if outcome == nil {
		return nil, protocolFailure("operation result outcome", errors.New("completed lookup has no outcome"))
	}
	if outcome.GetOriginalCommandType() != request.CommandType || outcome.GetOriginalOperationId() != request.OperationID {
		return nil, protocolFailure("operation outcome identity", errors.New("completed outcome does not match original operation"))
	}
	if farmContext := outcome.GetFarmContext(); farmContext != nil && farmContext.GetTargetFarmId() != request.TargetFarmID {
		return nil, protocolFailure("operation outcome target", errors.New("completed outcome target farm does not match request"))
	}
	result := &commonpb.CommandResultEnvelope{
		OperationId:    request.OperationID,
		RequestId:      request.RequestID,
		CommandType:    request.CommandType,
		Status:         outcome.GetStatus(),
		Payload:        append([]byte(nil), outcome.GetResultPayload()...),
		RetryDirective: lookup.GetCompletedRetryDirective(),
	}
	if outcome.GetError() != nil {
		result.Error = proto.Clone(outcome.GetError()).(*commonpb.ErrorDetail)
	}
	if farmContext := outcome.GetFarmContext(); farmContext != nil {
		result.ResultContext = &commonpb.CommandResultEnvelope_Farm{Farm: &commonpb.FarmResultContext{
			TargetFarmId: farmContext.GetTargetFarmId(),
		}}
	}
	switch result.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_NO_CHANGE:
		return result, nil
	case commonpb.CommandStatus_REJECTED:
		return result, rejectedError(result)
	default:
		return result, protocolFailure("operation outcome status", errors.New("completed outcome is not terminal"))
	}
}

func operationQueryIDs(originalOperationID string, attempt uint64) (string, string) {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", originalOperationID, attempt)))
	encoded := hex.EncodeToString(digest[:])
	return "opq-" + encoded[:48], "req-" + encoded[:48]
}

// retryAfter resolves how long to wait before asking again. A configured poll
// overrides the server's hint, which is what lets a measurement run see the
// settle time the hint would otherwise round up to.
//
// The wait doubles with each attempt. A fixed short poll is what a client
// wants for an operation that settles quickly, and exactly what it must not do
// when one settles slowly: the queries are themselves commands, so a server
// that has fallen behind gets asked about it more often the further behind it
// falls. Under a startup burst that ran away to nine million queries against
// a hundred thousand real commands, and the load being measured was the
// measurement. Doubling keeps the first few answers sharp, which is where the
// resolution matters, and makes a slow operation cost a logarithmic number of
// questions instead of a linear one.
func (s *Session) retryAfter(serverHintMs uint32, attempt uint64) time.Duration {
	base := s.client.config.OperationResultPoll
	if base <= 0 {
		base = time.Duration(serverHintMs) * time.Millisecond
		if base == 0 {
			base = defaultOperationResultPoll
		}
		if base < minOperationResultPoll {
			base = minOperationResultPoll
		}
	}
	delay := base
	for backoff := uint64(1); backoff < attempt && delay < maxOperationResultPoll; backoff++ {
		delay *= 2
	}
	if delay > maxOperationResultPoll {
		delay = maxOperationResultPoll
	}
	return delay
}

// waitOperationResult blocks until the server says the operation settled, or
// until the retry deadline, whichever is first.
//
// The timer alone would make every async command look at least one poll
// interval slow no matter how fast the server was, which hides the entire
// range a capacity test needs to see. The notification alone would hang if it
// were ever dropped. Waiting for both keeps the measurement honest and the
// client safe.
func waitOperationResult(ctx context.Context, settled <-chan struct{}, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-settled:
		return nil
	case <-timer.C:
		return nil
	}
}

func rejectedError(result *commonpb.CommandResultEnvelope) error {
	detail := result.GetError()
	var code commonpb.ErrorCode
	var message string
	if detail != nil {
		code, message = detail.GetErrorCode(), detail.GetErrorMessage()
	}
	return &RejectedError{
		OperationID: result.GetOperationId(), RequestID: result.GetRequestId(),
		Code: code, Message: message, RetryDirective: result.GetRetryDirective(),
	}
}

func commandStatusError(result *commonpb.CommandResultEnvelope) error {
	if result.GetStatus() == commonpb.CommandStatus_REJECTED {
		return rejectedError(result)
	}
	return &UnexpectedStatusError{OperationID: result.GetOperationId(), Status: result.GetStatus()}
}

// Reconnect closes the old connection, logs in again, performs a fresh hello
// and rejoins the previously joined farm. joinOperationID is sent unchanged;
// a retry of a caller's business operation can therefore keep its original
// operation ID after this method returns.
func (s *Session) Reconnect(ctx context.Context, joinOperationID string, requestID ...string) (*JoinResult, error) {
	if err := validateID(joinOperationID, "operation_id"); err != nil {
		return nil, err
	}
	if len(requestID) > 1 {
		return nil, errors.New("farmbotclient: at most one request_id may be supplied")
	}
	reqID := requestIDFor(joinOperationID)
	if len(requestID) == 1 {
		reqID = requestID[0]
	}
	if err := validateID(reqID, "request_id"); err != nil {
		return nil, err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if s.credentials == nil {
		s.mu.Unlock()
		return nil, errors.New("farmbotclient: reconnect requires credentials from LoginAndConnect")
	}
	targetFarmID := s.joinedFarmID
	if targetFarmID == 0 {
		s.mu.Unlock()
		return nil, errors.New("farmbotclient: reconnect requires a previously joined farm")
	}
	s.discardConnectionLocked()
	credentials := s.credentials.clone()
	s.mu.Unlock()

	login, err := s.client.Login(ctx, credentials)
	if err != nil {
		return nil, err
	}
	if err := validateLoginResult(login); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.login = login
	err = s.connectLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	joined, err := s.joinFarm(ctx, targetFarmID, joinOperationID, reqID)
	if err != nil {
		s.discardConnection()
		return nil, err
	}
	return joined, nil
}

// JoinFarm enforces ACCEPTED + FARM_SNAPSHOT + SUBSCRIPTION_READY. The
// ACCEPTED and FARM_SNAPSHOT messages may arrive in either order; ready is
// accepted only after both and all subscription/join-attempt fences agree.
func (s *Session) JoinFarm(ctx context.Context, targetFarmID uint64, operationID string, requestID ...string) (*JoinResult, error) {
	if targetFarmID == 0 {
		return nil, errors.New("farmbotclient: target farm id must be non-zero")
	}
	if err := validateID(operationID, "operation_id"); err != nil {
		return nil, err
	}
	if len(requestID) > 1 {
		return nil, errors.New("farmbotclient: at most one request_id may be supplied")
	}
	reqID := requestIDFor(operationID)
	if len(requestID) == 1 {
		reqID = requestID[0]
	}
	if err := validateID(reqID, "request_id"); err != nil {
		return nil, err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	return s.joinFarm(ctx, targetFarmID, operationID, reqID)
}

type joinBarrier struct {
	targetFarmID         uint64
	operationID          string
	requestID            string
	acceptedSubscription string
	acceptedAttempt      string
	snapshotSubscription string
	snapshotAttempt      string
	readySubscription    string
	readyAttempt         string
	snapshot             *farmpb.FarmSnapshot
	ready                bool
}

func (s *Session) joinFarm(ctx context.Context, targetFarmID uint64, operationID, requestID string) (*JoinResult, error) {
	pump, err := s.activePump()
	if err != nil {
		return nil, err
	}
	events, err := pump.beginJoin(operationID)
	if err != nil {
		return nil, err
	}
	defer pump.endJoin()

	if err := s.writeFrame(ctx, &commonpb.FarmFrame{
		FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{
			OperationId: operationID,
			RequestId:   requestID,
			CommandType: commonpb.CommandType_JOIN_FARM,
			Payload:     marshalPayload(&publicpb.JoinFarmRequest{TargetFarmId: targetFarmID}),
		}},
	}); err != nil {
		return nil, err
	}
	waitCtx, cancel := s.waitContext(ctx)
	defer cancel()
	barrier := joinBarrier{targetFarmID: targetFarmID, operationID: operationID, requestID: requestID}
	for {
		event, err := pump.awaitJoinEvent(waitCtx, events)
		if err != nil {
			return nil, err
		}
		if event.result != nil {
			if err := barrier.observeResult(event.result); err != nil {
				return nil, err
			}
		}
		if event.push != nil {
			if err := barrier.observePush(event.push); err != nil {
				return nil, err
			}
		}
		joined, complete, err := barrier.result()
		if err != nil {
			return nil, err
		}
		if !complete {
			continue
		}
		s.mu.Lock()
		ownFarm := targetFarmID == s.login.PlayerID
		s.mu.Unlock()
		if ownFarm {
			if err := s.writeFrame(ctx, &commonpb.FarmFrame{
				FrameBody: &commonpb.FarmFrame_Control{Control: &commonpb.ControlFrame{
					Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
				}},
			}); err != nil {
				return nil, err
			}
		}
		s.mu.Lock()
		s.joinedFarmID = targetFarmID
		s.subscriptionID = joined.SubscriptionID
		view := s.farmView
		s.mu.Unlock()
		// A fresh join is the only trustworthy seed for the realtime view.
		view.Reset(joined.Snapshot)
		return joined, nil
	}
}

func (b *joinBarrier) observeResult(result *commonpb.CommandResultEnvelope) error {
	if result.GetRequestId() != b.requestID {
		return protocolFailure("join result identity", errors.New("join result request_id does not match request"))
	}
	if result.GetCommandType() != commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED && result.GetCommandType() != commonpb.CommandType_JOIN_FARM {
		return protocolFailure("join result identity", errors.New("join result command_type does not match JOIN_FARM"))
	}
	if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
		return commandStatusError(result)
	}
	if len(result.GetPayload()) != 0 {
		return protocolFailure("join result", errors.New("ACCEPTED JoinFarm result must have empty payload"))
	}
	join := result.GetJoin()
	if join == nil || join.GetTargetFarmId() != b.targetFarmID || strings.TrimSpace(join.GetSubscriptionId()) == "" || strings.TrimSpace(join.GetJoinAttemptId()) == "" {
		return protocolFailure("join result", errors.New("ACCEPTED JoinFarm context is incomplete"))
	}
	if b.acceptedSubscription != "" && (b.acceptedSubscription != join.GetSubscriptionId() || b.acceptedAttempt != join.GetJoinAttemptId()) {
		return protocolFailure("join result", errors.New("duplicate ACCEPTED JoinFarm context changed"))
	}
	b.acceptedSubscription = join.GetSubscriptionId()
	b.acceptedAttempt = join.GetJoinAttemptId()
	return nil
}

func (b *joinBarrier) observePush(push *commonpb.ServerPushEnvelope) error {
	if push == nil {
		return protocolFailure("join push", errors.New("nil push"))
	}
	context := push.GetFarmSubscription()
	switch push.GetPushType() {
	case commonpb.PushType_FARM_SNAPSHOT:
		if context == nil || context.GetTargetFarmId() != b.targetFarmID || strings.TrimSpace(context.GetSubscriptionId()) == "" {
			return protocolFailure("join snapshot", errors.New("snapshot subscription context is incomplete"))
		}
		var snapshotPush farmpb.FarmSnapshotPush
		if err := proto.Unmarshal(push.GetPayload(), &snapshotPush); err != nil {
			return protocolFailure("join snapshot", err)
		}
		snapshot := snapshotPush.GetSnapshot()
		if snapshot == nil || snapshot.GetOwnerId() != b.targetFarmID || strings.TrimSpace(snapshotPush.GetJoinAttemptId()) == "" {
			return protocolFailure("join snapshot", errors.New("snapshot payload identity is incomplete"))
		}
		if b.acceptedAttempt != "" && b.acceptedAttempt != snapshotPush.GetJoinAttemptId() {
			return protocolFailure("join snapshot", errors.New("snapshot join_attempt_id does not match ACCEPTED"))
		}
		if b.snapshot != nil && (b.snapshotSubscription != context.GetSubscriptionId() || b.snapshotAttempt != snapshotPush.GetJoinAttemptId()) {
			return protocolFailure("join snapshot", errors.New("duplicate snapshot fence changed"))
		}
		b.snapshotSubscription = context.GetSubscriptionId()
		b.snapshotAttempt = snapshotPush.GetJoinAttemptId()
		b.snapshot = snapshot
	case commonpb.PushType_SUBSCRIPTION_READY:
		if b.acceptedSubscription == "" || b.snapshot == nil {
			return protocolFailure("join ready", errors.New("SUBSCRIPTION_READY arrived before ACCEPTED and FARM_SNAPSHOT"))
		}
		if context == nil || context.GetTargetFarmId() != b.targetFarmID || strings.TrimSpace(context.GetSubscriptionId()) == "" {
			return protocolFailure("join ready", errors.New("ready subscription context is incomplete"))
		}
		var readyPush farmpb.SubscriptionReadyPush
		if err := proto.Unmarshal(push.GetPayload(), &readyPush); err != nil {
			return protocolFailure("join ready", err)
		}
		if strings.TrimSpace(readyPush.GetJoinAttemptId()) == "" || readyPush.GetJoinAttemptId() != b.acceptedAttempt || readyPush.GetJoinAttemptId() != b.snapshotAttempt {
			return protocolFailure("join ready", errors.New("ready join_attempt_id does not match join barrier"))
		}
		if context.GetSubscriptionId() != b.acceptedSubscription || context.GetSubscriptionId() != b.snapshotSubscription {
			return protocolFailure("join ready", errors.New("subscription_id does not match join barrier"))
		}
		b.readySubscription = context.GetSubscriptionId()
		b.readyAttempt = readyPush.GetJoinAttemptId()
		b.ready = true
	case commonpb.PushType_REJOIN_REQUIRED:
		return errors.New("farmbotclient: server requested rejoin during JoinFarm")
	}
	return nil
}

func (b *joinBarrier) result() (*JoinResult, bool, error) {
	if b.acceptedSubscription != "" && b.snapshotSubscription != "" && b.acceptedSubscription != b.snapshotSubscription {
		return nil, false, protocolFailure("join barrier", errors.New("ACCEPTED and snapshot subscription_id differ"))
	}
	if b.acceptedSubscription == "" || b.snapshot == nil || !b.ready {
		return nil, false, nil
	}
	if b.readySubscription != b.acceptedSubscription || b.readyAttempt != b.acceptedAttempt {
		return nil, false, protocolFailure("join barrier", errors.New("ready fence does not match ACCEPTED fence"))
	}
	return &JoinResult{
		TargetFarmID: b.targetFarmID, SubscriptionID: b.acceptedSubscription,
		JoinAttemptID: b.acceptedAttempt, Snapshot: b.snapshot,
	}, true, nil
}

func marshalPayload(message proto.Message) []byte {
	if message == nil {
		return nil
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil
	}
	return payload
}
