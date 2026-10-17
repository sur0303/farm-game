package farmbotclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	settlementpb "farmgame/api/gen/settlement"
	wireprotocol "farmgame/internal/gateway/protocol"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const testSecret = "farmbot-test-secret"

type testLoginPayload struct {
	PlatformToken string `json:"platform_token"`
	BootstrapID   string `json:"bootstrap_id"`
	ClientBuild   string `json:"client_build"`
}

type testHarness struct {
	client     *Client
	server     *httptest.Server
	loginSeen  chan testLoginPayload
	scriptDone chan struct{}
	scriptErr  chan error
}

func newHarness(t *testing.T, script func(*websocket.Conn) error) *testHarness {
	t.Helper()
	h := &testHarness{
		loginSeen: make(chan testLoginPayload, 8), scriptDone: make(chan struct{}), scriptErr: make(chan error, 1),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.scriptErr <- err
			return
		}
		var payload testLoginPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			h.scriptErr <- err
			return
		}
		h.loginSeen <- payload
		w.Header().Set("Set-Cookie", "farm_session=test-cookie; Path=/")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"player_id":"42","session_id":"session-1","connection_ticket":"ticket-1","wss_endpoint":"`+r.Host+`/ws"}`)
	})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			h.scriptErr <- err
			return
		}
		go func() {
			defer close(h.scriptDone)
			defer conn.Close()
			if script != nil {
				if err := script(conn); err != nil {
					h.scriptErr <- err
				}
			}
		}()
	})
	h.server = httptest.NewServer(mux)
	wsURL := "ws" + strings.TrimPrefix(h.server.URL, "http") + "/ws"
	var err error
	h.client, err = NewClient(Config{
		HTTPBaseURL: h.server.URL, WSURL: wsURL, Timeout: time.Second,
		ClientBuild: "farmbotclient-test/1",
	})
	if err != nil {
		h.server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		h.server.Close()
	})
	return h
}

func (h *testHarness) wait(t *testing.T) {
	t.Helper()
	select {
	case <-h.scriptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket script did not finish")
	}
	select {
	case err := <-h.scriptErr:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
}

func readClientFrame(conn *websocket.Conn) (*commonpb.FarmFrame, error) {
	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if messageType != websocket.BinaryMessage {
		return nil, fmt.Errorf("client message type=%d", messageType)
	}
	frame, err := wireprotocol.Decode(raw, wireprotocol.DefaultMaxFrameSize)
	if err != nil {
		return nil, err
	}
	if err := wireprotocol.ValidateClientFrame(frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func writeServerFrame(conn *websocket.Conn, frame *commonpb.FarmFrame) error {
	raw, err := wireprotocol.Encode(frame, wireprotocol.DefaultMaxFrameSize)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

func serverHello(conn *websocket.Conn, playerID uint64, sessionID string) (*commonpb.FarmFrame, error) {
	frame, err := readClientFrame(conn)
	if err != nil {
		return nil, err
	}
	if frame.GetSequence() != 1 || frame.GetClientHello() == nil {
		return nil, errors.New("missing ClientHello")
	}
	if err := writeServerFrame(conn, wireprotocol.NewServerHelloFrame(1, &commonpb.ServerHello{
		PlayerId: playerID, SessionId: sessionID,
	})); err != nil {
		return nil, err
	}
	return frame, nil
}

func testCredentials() Credentials {
	return Credentials{PlayerID: 42, Secret: []byte(testSecret)}
}

func TestLoginPlatformHMACAndCookie(t *testing.T) {
	h := newHarness(t, nil)
	result, err := h.client.Login(context.Background(), Credentials{
		PlayerID: 42, Secret: []byte(testSecret), BootstrapID: "boot-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PlayerID != 42 || result.SessionID != "session-1" || result.ConnectionTicket != "ticket-1" || result.Cookie != "test-cookie" || result.FarmSessionCookie != "test-cookie" {
		t.Fatalf("unexpected login result: player=%d session=%q ticket=%q cookie=%q", result.PlayerID, result.SessionID, result.ConnectionTicket, result.Cookie)
	}
	seen := <-h.loginSeen
	mac := hmac.New(sha256.New, []byte(testSecret))
	_, _ = mac.Write([]byte("42"))
	want := "load:42:" + hex.EncodeToString(mac.Sum(nil))
	if seen.PlatformToken != want || seen.BootstrapID != "boot-test" || seen.ClientBuild != "farmbotclient-test/1" {
		t.Fatalf("unexpected login request fields: %+v", seen)
	}
}

func TestDecodeLoginResultOptionalHomeBootstrap(t *testing.T) {
	identity := `"player_id":"42","session_id":"session-1","connection_ticket":"ticket-1"`
	for _, testCase := range []struct {
		name        string
		body        string
		wantPresent bool
		wantRev     uint64
	}{
		{name: "omitted", body: `{` + identity + `}`},
		{name: "null", body: `{` + identity + `,"home_bootstrap":null}`},
		{name: "malformed", body: `{` + identity + `,"home_bootstrap":"not-an-object"}`},
		{
			name: "present", body: `{` + identity + `,"home_bootstrap":{"player_state_revision":9}}`,
			wantPresent: true, wantRev: 9,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := decodeLoginResult([]byte(testCase.body))
			if err != nil {
				t.Fatalf("decodeLoginResult: %v", err)
			}
			if result.PlayerID != 42 || result.SessionID != "session-1" || result.ConnectionTicket != "ticket-1" {
				t.Fatalf("identity lost: %+v", result)
			}
			if !testCase.wantPresent {
				if result.HomeBootstrap != nil {
					t.Fatalf("HomeBootstrap=%v, want omitted", result.HomeBootstrap)
				}
				return
			}
			if result.HomeBootstrap == nil {
				t.Fatal("HomeBootstrap is missing")
			}
			if result.HomeBootstrap.GetPlayerStateRevision() != testCase.wantRev {
				t.Fatalf("player_state_revision=%d, want %d", result.HomeBootstrap.GetPlayerStateRevision(), testCase.wantRev)
			}
		})
	}
}

func TestServerHelloIdentityMismatchFailsClosed(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		_, err := serverHello(conn, 99, "session-1")
		return err
	})
	_, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err == nil || !errors.Is(err, ErrProtocol) {
		t.Fatalf("identity mismatch error=%v", err)
	}
	h.wait(t)
}

func snapshotPush(subscriptionID, attemptID string, ownerID uint64) (*commonpb.FarmFrame, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmSnapshotPush{
		JoinAttemptId: attemptID, SnapshotVersion: 1,
		Snapshot: &farmpb.FarmSnapshot{OwnerId: ownerID},
	})
	if err != nil {
		return nil, err
	}
	return wireprotocol.NewServerPushFrame(2, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: ownerID, TargetFarmId: ownerID, SubscriptionId: subscriptionID,
		}}, Payload: payload,
	}), nil
}

func readyPush(subscriptionID, attemptID string, ownerID uint64, sequence uint64) (*commonpb.FarmFrame, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.SubscriptionReadyPush{
		JoinAttemptId: attemptID, ReadyThroughVersion: 1,
	})
	if err != nil {
		return nil, err
	}
	return wireprotocol.NewServerPushFrame(sequence, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: ownerID, TargetFarmId: ownerID, SubscriptionId: subscriptionID,
		}}, Payload: payload,
	}), nil
}

func acceptedJoin(operationID, requestID, subscriptionID, attemptID string) *commonpb.FarmFrame {
	return wireprotocol.NewCommandResultFrame(3, &commonpb.CommandResultEnvelope{
		OperationId: operationID, RequestId: requestID, Status: commonpb.CommandStatus_ACCEPTED,
		CommandType: commonpb.CommandType_JOIN_FARM,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 42, SubscriptionId: subscriptionID, JoinAttemptId: attemptID,
		}},
	})
}

func TestConnectAuthenticatedRetainsCredentialCopy(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		_, err := serverHello(conn, 42, "session-1")
		return err
	})
	credentials := testCredentials()
	login, err := h.client.Login(context.Background(), credentials)
	if err != nil {
		t.Fatal(err)
	}
	session, err := h.client.ConnectAuthenticated(context.Background(), login, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	credentials.Secret[0] ^= 0xff
	if session.credentials == nil || string(session.credentials.Secret) != testSecret {
		t.Fatal("Session did not retain an independent credential copy")
	}
	h.wait(t)
}

func TestConnectAuthenticatedRejectsInvalidCredentialsWithoutSecretLeak(t *testing.T) {
	h := newHarness(t, nil)
	secret := "connect-authenticated-secret-must-not-leak"
	login := LoginResult{
		PlayerID: 99, SessionID: "session-1", ConnectionTicket: "ticket-1", Cookie: "test-cookie",
	}
	_, err := h.client.ConnectAuthenticated(context.Background(), login, Credentials{PlayerID: 42, Secret: []byte(secret)})
	if err == nil || !strings.Contains(err.Error(), "does not match") || strings.Contains(err.Error(), secret) {
		t.Fatalf("identity mismatch error=%v", err)
	}

	login.PlayerID = 42
	_, err = h.client.ConnectAuthenticated(context.Background(), login, Credentials{PlayerID: 42})
	if err == nil || !strings.Contains(err.Error(), "secret must be non-empty") {
		t.Fatalf("empty secret error=%v", err)
	}
}

func TestJoinFarmAcceptsSnapshotBeforeAcceptedAndSendsRecovery(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		joinFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		join := joinFrame.GetClientCommand()
		if join == nil || join.GetCommandType() != commonpb.CommandType_JOIN_FARM {
			return errors.New("missing JoinFarm command")
		}
		snapshot, err := snapshotPush("sub-1", "attempt-1", 42)
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, snapshot); err != nil {
			return err
		}
		if err := writeServerFrame(conn, acceptedJoin(join.GetOperationId(), join.GetRequestId(), "sub-1", "attempt-1")); err != nil {
			return err
		}
		ready, err := readyPush("sub-1", "attempt-1", 42, 4)
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, ready); err != nil {
			return err
		}
		recovery, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		if recovery.GetControl() == nil || recovery.GetControl().GetClientRecoveryComplete() == nil || recovery.GetSequence() != 3 {
			return errors.New("missing ClientRecoveryComplete")
		}
		return nil
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	joined, err := session.JoinFarm(context.Background(), 42, "join-op", "join-req")
	if err != nil {
		t.Fatal(err)
	}
	if joined.SubscriptionID != "sub-1" || joined.JoinAttemptID != "attempt-1" || joined.Snapshot.GetOwnerId() != 42 {
		t.Fatalf("unexpected join result: %+v", joined)
	}
	h.wait(t)
}

func TestJoinFarmRejectsSubscriptionMismatch(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		joinFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		join := joinFrame.GetClientCommand()
		snapshot, err := snapshotPush("sub-snapshot", "attempt-1", 42)
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, snapshot); err != nil {
			return err
		}
		return writeServerFrame(conn, acceptedJoin(join.GetOperationId(), join.GetRequestId(), "sub-result", "attempt-1"))
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.JoinFarm(context.Background(), 42, "join-op", "join-req")
	if err == nil || !errors.Is(err, ErrProtocol) {
		t.Fatalf("subscription mismatch error=%v", err)
	}
	h.wait(t)
}

func TestExecuteReturnsProcessingWithoutWaitingForInlineTerminal(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		frame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		command := frame.GetClientCommand()
		if command == nil || command.GetCommandType() != commonpb.CommandType_BUY || len(command.GetPayload()) == 0 {
			return errors.New("typed Buy command missing")
		}
		var payload publicpb.BuyRequest
		if err := proto.Unmarshal(command.GetPayload(), &payload); err != nil || payload.GetOfferId() != 1 || payload.GetCount() != 2 {
			return errors.New("Buy payload did not use generated Proto fields")
		}
		if err := writeServerFrame(conn, wireprotocol.NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{
			OperationId: command.GetOperationId(), RequestId: command.GetRequestId(), Status: commonpb.CommandStatus_PROCESSING,
			CommandType: commonpb.CommandType_BUY,
		})); err != nil {
			return err
		}
		return nil
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Execute(context.Background(), ExecuteRequest{
		OperationID: "buy-op", RequestID: "buy-req", CommandType: commonpb.CommandType_BUY,
		Payload: &publicpb.BuyRequest{OfferId: 1, Count: 2},
	})
	if err != nil || result.GetStatus() != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("PROCESSING must be returned as admission: result=%v err=%v", result, err)
	}
	h.wait(t)
}

func TestPlantProcessingPollsDurableOperationResult(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		plantFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		plant := plantFrame.GetClientCommand()
		if plant == nil || plant.GetCommandType() != commonpb.CommandType_PLANT {
			return errors.New("missing Plant command")
		}
		locator := "opaque-server-locator"
		if err := writeServerFrame(conn, wireprotocol.NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{
			OperationId: plant.GetOperationId(), RequestId: plant.GetRequestId(),
			Status: commonpb.CommandStatus_PROCESSING, CommandType: commonpb.CommandType_PLANT,
			RetryDirective:         commonpb.RetryDirective_POLL_OPERATION_RESULT,
			ServerOperationLocator: &locator,
		})); err != nil {
			return err
		}

		lookupFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		lookupCommand := lookupFrame.GetClientCommand()
		if lookupCommand == nil || lookupCommand.GetCommandType() != commonpb.CommandType_GET_OPERATION_RESULT {
			return errors.New("missing GET_OPERATION_RESULT command")
		}
		var lookupRequest publicpb.GetOperationResultRequest
		if err := proto.Unmarshal(lookupCommand.GetPayload(), &lookupRequest); err != nil {
			return err
		}
		if lookupRequest.GetOriginalCommandType() != commonpb.CommandType_PLANT ||
			lookupRequest.GetOriginalOperationId() != plant.GetOperationId() ||
			lookupRequest.GetOriginalTarget().GetTargetFarmId() != 42 ||
			lookupRequest.GetServerOperationLocator() != locator {
			return errors.New("operation lookup did not preserve original identity")
		}
		lookupPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.GetOperationResultResult{
			OriginalCommandType: commonpb.CommandType_PLANT,
			OriginalOperationId: plant.GetOperationId(),
			LookupStatus:        publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED,
			CompletedOutcome: &publicpb.StoredOperationOutcome{
				OriginalCommandType: commonpb.CommandType_PLANT,
				OriginalOperationId: plant.GetOperationId(),
				Status:              commonpb.CommandStatus_SUCCEEDED,
				FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: 42},
				ResultPayload:       []byte("plant-terminal"),
			},
			CompletedRetryDirective: commonpb.RetryDirective_NONE,
		})
		if err != nil {
			return err
		}
		return writeServerFrame(conn, wireprotocol.NewCommandResultFrame(3, &commonpb.CommandResultEnvelope{
			OperationId: lookupCommand.GetOperationId(), RequestId: lookupCommand.GetRequestId(),
			Status: commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_GET_OPERATION_RESULT,
			Payload: lookupPayload,
		}))
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Plant(context.Background(), "plant-op", "plant-req", &publicpb.PlantRequest{
		PlotId: 1, CropDefinitionId: 1, TargetFarmId: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.GetOperationId() != "plant-op" || result.GetRequestId() != "plant-req" ||
		result.GetCommandType() != commonpb.CommandType_PLANT || result.GetStatus() != commonpb.CommandStatus_SUCCEEDED ||
		string(result.GetPayload()) != "plant-terminal" || result.GetFarm().GetTargetFarmId() != 42 {
		t.Fatalf("unexpected resolved result: %+v", result)
	}
	h.wait(t)
}

// A resolution that sleeps on the retry timer reports the timer, not the
// server: every async command looks at least one poll interval slow however
// fast the server was, which hides the whole range a capacity run needs to
// see. The server here asks for a five second retry and then notifies. If the
// client honours the notification the call returns immediately; if it ignores
// it, the call takes five seconds and this test fails on the clock.
func TestOperationNotificationResolvesWithoutWaitingForTheRetryTimer(t *testing.T) {
	const retryAfterMs = 5000
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		plantFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		plant := plantFrame.GetClientCommand()
		if plant == nil || plant.GetCommandType() != commonpb.CommandType_PLANT {
			return errors.New("missing Plant command")
		}
		if err := writeServerFrame(conn, wireprotocol.NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{
			OperationId: plant.GetOperationId(), RequestId: plant.GetRequestId(),
			Status: commonpb.CommandStatus_PROCESSING, CommandType: commonpb.CommandType_PLANT,
			RetryDirective: commonpb.RetryDirective_POLL_OPERATION_RESULT, RetryAfterMs: retryAfterMs,
		})); err != nil {
			return err
		}

		// The first lookup finds the operation still running, with a retry
		// hint far longer than this test is willing to wait.
		firstLookup, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		pending, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.GetOperationResultResult{
			OriginalCommandType: commonpb.CommandType_PLANT,
			OriginalOperationId: plant.GetOperationId(),
			LookupStatus:        publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING,
			RetryAfterMs:        retryAfterMs,
		})
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, wireprotocol.NewCommandResultFrame(3, &commonpb.CommandResultEnvelope{
			OperationId: firstLookup.GetClientCommand().GetOperationId(),
			RequestId:   firstLookup.GetClientCommand().GetRequestId(),
			Status:      commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_GET_OPERATION_RESULT,
			Payload: pending,
		})); err != nil {
			return err
		}

		notification, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&settlementpb.OperationResult{
			OperationId: plant.GetOperationId(),
			Status:      commonpb.CommandStatus_SUCCEEDED,
		})
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, wireprotocol.NewServerPushFrame(4, &commonpb.ServerPushEnvelope{
			PushType: commonpb.PushType_OPERATION_RESULT,
			PushContext: &commonpb.ServerPushEnvelope_Player{
				Player: &commonpb.PlayerPushContext{TargetPlayerId: 42},
			},
			Payload: notification,
		})); err != nil {
			return err
		}

		secondLookup, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		completed, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.GetOperationResultResult{
			OriginalCommandType: commonpb.CommandType_PLANT,
			OriginalOperationId: plant.GetOperationId(),
			LookupStatus:        publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED,
			CompletedOutcome: &publicpb.StoredOperationOutcome{
				OriginalCommandType: commonpb.CommandType_PLANT,
				OriginalOperationId: plant.GetOperationId(),
				Status:              commonpb.CommandStatus_SUCCEEDED,
				FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: 42},
			},
		})
		if err != nil {
			return err
		}
		return writeServerFrame(conn, wireprotocol.NewCommandResultFrame(5, &commonpb.CommandResultEnvelope{
			OperationId: secondLookup.GetClientCommand().GetOperationId(),
			RequestId:   secondLookup.GetClientCommand().GetRequestId(),
			Status:      commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_GET_OPERATION_RESULT,
			Payload: completed,
		}))
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := session.Plant(context.Background(), "plant-op", "plant-req", &publicpb.PlantRequest{
		PlotId: 1, CropDefinitionId: 1, TargetFarmId: 42,
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("unexpected resolved result: %+v", result)
	}
	if elapsed >= retryAfterMs*time.Millisecond/2 {
		t.Fatalf("resolution took %v, so it waited for the retry timer instead of the notification", elapsed)
	}
	if stats := session.Stats(); stats.OperationPushes != 1 || stats.UnroutedPushes != 0 {
		t.Fatalf("notification accounting = %+v", stats)
	}
	h.wait(t)
}

func TestExecuteRejectedReturnsStructuredError(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		frame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		command := frame.GetClientCommand()
		return writeServerFrame(conn, wireprotocol.NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{
			OperationId: command.GetOperationId(), RequestId: command.GetRequestId(), Status: commonpb.CommandStatus_REJECTED,
			CommandType: commonpb.CommandType_CARE,
			Error:       &commonpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_PERMISSION_DENIED, ErrorMessage: "not allowed"},
		}))
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Execute(context.Background(), ExecuteRequest{
		OperationID: "care-op", RequestID: "care-req", CommandType: commonpb.CommandType_CARE,
		Payload: &publicpb.CareRequest{PlotId: 1, TargetFarmId: 42, CareType: publicpb.CareType_CARE_TYPE_WATER},
	})
	var rejected *RejectedError
	if result == nil || result.GetStatus() != commonpb.CommandStatus_REJECTED || !errors.As(err, &rejected) || rejected.Code != commonpb.ErrorCode_ERR_PERMISSION_DENIED || rejected.Message != "not allowed" {
		t.Fatalf("expected structured rejection: result=%v err=%v rejected=%+v", result, err, rejected)
	}
	h.wait(t)
}

func farmChangePush(sequence uint64, ownerID uint64) *commonpb.FarmFrame {
	return wireprotocol.NewServerPushFrame(sequence, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_CHANGE,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: ownerID, TargetFarmId: ownerID, SubscriptionId: "sub-1",
		}},
	})
}

// A long-lived bot spends most of its time idle. The connection must keep
// draining broadcasts during that time, or the backlog turns into gate-side
// backpressure and the next command reads a stale frame.
func TestSessionDrainsBroadcastsWhileIdle(t *testing.T) {
	const broadcasts = 200
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		for sequence := uint64(2); sequence < 2+broadcasts; sequence++ {
			if err := writeServerFrame(conn, farmChangePush(sequence, 42)); err != nil {
				return err
			}
		}
		frame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		command := frame.GetClientCommand()
		if command == nil || command.GetCommandType() != commonpb.CommandType_CARE {
			return errors.New("missing Care command after broadcast backlog")
		}
		return writeServerFrame(conn, wireprotocol.NewCommandResultFrame(2+broadcasts, &commonpb.CommandResultEnvelope{
			OperationId: command.GetOperationId(), RequestId: command.GetRequestId(),
			Status: commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_CARE,
		}))
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	deadline := time.Now().Add(2 * time.Second)
	for session.Stats().Pushes < broadcasts && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stats := session.Stats()
	if stats.Pushes != broadcasts || stats.BroadcastsDrained != broadcasts {
		t.Fatalf("idle session did not drain broadcasts: %+v", stats)
	}

	result, err := session.Care(context.Background(), "care-op", "care-req", &publicpb.CareRequest{
		PlotId: 1, TargetFarmId: 42, CareType: publicpb.CareType_CARE_TYPE_WATER,
	})
	if err != nil || result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("command after broadcast backlog: result=%v err=%v", result, err)
	}
	h.wait(t)
}

func farmChangeFrame(t *testing.T, sequence, farmVersion uint64, ownerID uint64, plot *farmpb.PlotView) *commonpb.FarmFrame {
	t.Helper()
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmChangePush{
		Change: &farmpb.FarmChange{
			OwnerId: ownerID, FarmVersion: farmVersion, ChangedPlots: []*farmpb.PlotView{plot},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return wireprotocol.NewServerPushFrame(sequence, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_CHANGE,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: ownerID, TargetFarmId: ownerID, SubscriptionId: "sub-1",
		}}, Payload: payload,
	})
}

// A bot that keeps acting must follow the realtime stream, or every action
// after the first one would target a plot state the server has already moved
// past.
func TestFarmViewTracksRealtimeChangesAfterJoin(t *testing.T) {
	h := newHarness(t, func(conn *websocket.Conn) error {
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		joinFrame, err := readClientFrame(conn)
		if err != nil {
			return err
		}
		join := joinFrame.GetClientCommand()
		snapshot, err := snapshotPush("sub-1", "attempt-1", 42)
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, snapshot); err != nil {
			return err
		}
		if err := writeServerFrame(conn, acceptedJoin(join.GetOperationId(), join.GetRequestId(), "sub-1", "attempt-1")); err != nil {
			return err
		}
		ready, err := readyPush("sub-1", "attempt-1", 42, 4)
		if err != nil {
			return err
		}
		if err := writeServerFrame(conn, ready); err != nil {
			return err
		}
		if _, err := readClientFrame(conn); err != nil {
			return err
		}
		return writeServerFrame(conn, farmChangeFrame(t, 5, 1, 42, &farmpb.PlotView{
			PlotIndex: 3, PlotVersion: 9, CropCycleId: "cycle-3", CropPhase: farmpb.CropPhase_MATURE,
			HarvestableCount: 4,
		}))
	})
	client, err := NewClient(Config{
		HTTPBaseURL: h.server.URL, WSURL: "ws" + strings.TrimPrefix(h.server.URL, "http") + "/ws",
		Timeout: time.Second, ClientBuild: "farmbotclient-test/1", TrackFarmView: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, joined, err := client.LoginAndJoin(context.Background(), testCredentials(), 42, "join-op", "join-req")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if joined.Snapshot.GetOwnerId() != 42 {
		t.Fatalf("unexpected join snapshot: %+v", joined.Snapshot)
	}
	view := session.FarmView()
	if view == nil {
		t.Fatal("farm view tracking was requested but no view exists")
	}
	deadline := time.Now().Add(2 * time.Second)
	for view.Stats().Applied == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stats := view.Stats()
	if stats.Applied != 1 || stats.Gaps != 0 || stats.Stale || stats.FarmVersion != 1 {
		t.Fatalf("realtime change was not applied: %+v", stats)
	}
	plots := view.Snapshot().GetPlots()
	if len(plots) != 1 || plots[0].GetPlotIndex() != 3 || plots[0].GetHarvestableCount() != 4 {
		t.Fatalf("farm view does not expose the changed plot: %+v", plots)
	}
	h.wait(t)
}

// A skipped farm_version means the client can no longer prove its view, so it
// must go stale instead of silently acting on a patched state.
func TestFarmViewGapMarksViewStale(t *testing.T) {
	view := NewFarmView()
	view.Reset(&farmpb.FarmSnapshot{OwnerId: 42, FarmVersion: 1, Plots: []*farmpb.PlotView{{PlotIndex: 1}}})
	view.Apply(&farmpb.FarmChange{OwnerId: 42, FarmVersion: 2, ChangedPlots: []*farmpb.PlotView{{PlotIndex: 1, PlotVersion: 2}}})
	view.Apply(&farmpb.FarmChange{OwnerId: 42, FarmVersion: 2, ChangedPlots: []*farmpb.PlotView{{PlotIndex: 1, PlotVersion: 3}}})
	if stats := view.Stats(); stats.Applied != 1 || stats.Duplicates != 1 || stats.Stale {
		t.Fatalf("replayed version must be ignored without going stale: %+v", stats)
	}
	view.Apply(&farmpb.FarmChange{OwnerId: 42, FarmVersion: 4, ChangedPlots: []*farmpb.PlotView{{PlotIndex: 1, PlotVersion: 4}}})
	stats := view.Stats()
	if stats.Gaps != 1 || !stats.Stale || stats.FarmVersion != 2 {
		t.Fatalf("version gap must mark the view stale: %+v", stats)
	}
	if view.Usable() {
		t.Fatal("stale view must not be usable")
	}
	view.Reset(&farmpb.FarmSnapshot{OwnerId: 42, FarmVersion: 9})
	if !view.Usable() {
		t.Fatal("fresh join snapshot must restore the view")
	}
}

// The gate pings on a fixed period and closes sessions that stop answering,
// so an idle bot must keep replying without a command in flight.
func TestSessionAnswersServerPingWhileIdle(t *testing.T) {
	pongs := make(chan struct{}, 1)
	h := newHarness(t, func(conn *websocket.Conn) error {
		conn.SetPongHandler(func(string) error {
			select {
			case pongs <- struct{}{}:
			default:
			}
			return nil
		})
		if _, err := serverHello(conn, 42, "session-1"); err != nil {
			return err
		}
		if err := conn.WriteControl(websocket.PingMessage, []byte("gate"), time.Now().Add(time.Second)); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return nil
			}
		}
	})
	session, err := h.client.LoginAndConnect(context.Background(), testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	select {
	case <-pongs:
	case <-time.After(2 * time.Second):
		t.Fatal("idle session did not answer the server ping")
	}
}

func TestLoginErrorDoesNotEchoSecret(t *testing.T) {
	secret := "do-not-leak-this-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "upstream failure "+secret)
	}))
	defer server.Close()
	client, err := NewClient(Config{HTTPBaseURL: server.URL, WSURL: "ws://127.0.0.1:1/ws", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Login(context.Background(), Credentials{PlayerID: 42, Secret: []byte(secret)})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("login error leaked secret: %v", err)
	}
}

// A polling client is a load source, and one that polls at a fixed short
// interval asks a struggling server about its backlog more often the bigger
// the backlog gets. The wait has to grow with the number of unanswered
// questions, and stop growing at the cap.
func TestPollingBacksOffSoASlowOperationCostsFewerQueries(t *testing.T) {
	session := &Session{client: &Client{config: Config{OperationResultPoll: 20 * time.Millisecond}}}
	for _, testCase := range []struct {
		attempt uint64
		want    time.Duration
	}{
		{attempt: 1, want: 20 * time.Millisecond},
		{attempt: 2, want: 40 * time.Millisecond},
		{attempt: 5, want: 320 * time.Millisecond},
		{attempt: 9, want: maxOperationResultPoll},
		{attempt: 400, want: maxOperationResultPoll},
	} {
		if got := session.retryAfter(0, testCase.attempt); got != testCase.want {
			t.Errorf("attempt %d waited %v, want %v", testCase.attempt, got, testCase.want)
		}
	}

	// Without a configured poll the server's hint is the base, floored so a
	// server asking to be polled instantly cannot be taken at its word.
	hinted := &Session{client: &Client{config: Config{}}}
	if got := hinted.retryAfter(1, 1); got != minOperationResultPoll {
		t.Errorf("first wait on a 1ms hint = %v, want the %v floor", got, minOperationResultPoll)
	}
	if got := hinted.retryAfter(250, 3); got != time.Second {
		t.Errorf("third wait on a 250ms hint = %v, want 1s", got)
	}
}
