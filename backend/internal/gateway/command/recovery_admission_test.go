package command

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

type trustedCaptureLogic struct {
	testLogic
	mu       sync.Mutex
	requests []*logicrpcpb.ExecuteCommandRequest
}

func (l *trustedCaptureLogic) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	l.mu.Lock()
	l.requests = append(l.requests, proto.Clone(req).(*logicrpcpb.ExecuteCommandRequest))
	l.mu.Unlock()
	return l.testLogic.ExecuteCommand(ctx, req)
}

func (l *trustedCaptureLogic) snapshot() []*logicrpcpb.ExecuteCommandRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*logicrpcpb.ExecuteCommandRequest(nil), l.requests...)
}

func setRecoveryCompleteForTest(t *testing.T, gate *Gate, connectionID string, generation uint64, complete bool) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current, ok := gate.connections[connectionID]
	if !ok || current.generation != generation {
		t.Fatalf("connection %s generation %d not found", connectionID, generation)
	}
	current.recoveryComplete = complete
	gate.connections[connectionID] = current
}

func setRecoveryActiveOwnSubscriptionForTest(t *testing.T, gate *Gate, connectionID string, generation uint64) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current, ok := gate.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		t.Fatalf("connection %s generation %d not active", connectionID, generation)
	}
	current.currentFarmID = current.binding.PlayerID
	current.currentSubscriptionID = "recovery-active-subscription"
	current.currentJoinAttemptID = "recovery-active-attempt"
	current.currentPresenceEpoch = 1
	current.subscriptionContextGeneration++
	gate.connections[connectionID] = current
}

func encodeRecoveryCompleteFrame(t *testing.T, seq uint32) []byte {
	t.Helper()
	raw, err := protocol.Encode(protocol.NewControlFrame(uint64(seq), &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{
			ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{},
		},
	}), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func encodeBuyFrame(t *testing.T, seq uint32, operationID string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	return encodeTypedCommandFrame(t, seq, &commonpb.ClientCommandEnvelope{
		OperationId: operationID,
		RequestId:   "request-" + operationID,
		CommandType: commonpb.CommandType_BUY,
		Payload:     payload,
	})
}

func activateHelloGeneration(t *testing.T, gate *Gate, authSvc *auth.Service, connectionID string, generation uint64) {
	t.Helper()
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{
		PlatformToken: "p", BootstrapID: connectionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	hello, err := proto.Marshal(&commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      "farm-h5/0.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), connectionID, generation, raw); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryAdmissionControlAndTrustedSnapshot(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &trustedCaptureLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	activateHello(t, gate, authSvc, "recovery-conn")
	setRecoveryCompleteForTest(t, gate, "recovery-conn", 1, false)

	// Recovery-independent commands (28 §6.1:JOIN_FARM 不要求恢复完成)在恢复
	// 屏障完成前仍可用,且 TrustedCommandContext 显式携带 false。
	if _, err := gate.HandleBinary(context.Background(), "recovery-conn", 1,
		encodeTypedCommandFrame(t, 2, &commonpb.ClientCommandEnvelope{
			OperationId: "join-before-recovery", RequestId: "join-before-recovery",
			CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, 101),
		})); err != nil {
		t.Fatalf("join before recovery: %v", err)
	}
	requests := logic.snapshot()
	if len(requests) != 1 || requests[0].GetTrusted().GetRecoveryCompleteAtAdmission() {
		t.Fatalf("join TrustedCommandContext=%v", requests)
	}
	if requests[0].GetTrusted().GetJoinAttempt() == nil || requests[0].GetTrusted().GetFarmContext() != nil {
		t.Fatalf("join must carry JoinAdmissionContext without FarmContext: %v", requests[0].GetTrusted())
	}

	// A write is rejected at Gate and never reaches FirstHop.
	rejectedRaw, err := gate.HandleBinary(context.Background(), "recovery-conn", 1,
		encodeBuyFrame(t, 3, "buy-before-recovery"))
	if err != nil {
		t.Fatalf("write before recovery closed connection: %v", err)
	}
	rejectedFrame, err := protocol.Decode(rejectedRaw, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	rejected := rejectedFrame.GetCommandResult()
	if rejected == nil {
		t.Fatal("response is not a command result")
	}
	if rejected.Status != commonpb.CommandStatus_REJECTED || rejected.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_RECOVERY_INCOMPLETE {
		t.Fatalf("write before recovery result=%v", rejected)
	}
	if got := len(logic.snapshot()); got != 1 {
		t.Fatalf("FirstHop calls=%d after rejected write, want 1", got)
	}

	// The marker and subsequent command share the connection FIFO. Duplicate
	// markers are idempotent and do not create a second state transition.
	setRecoveryActiveOwnSubscriptionForTest(t, gate, "recovery-conn", 1)
	if response, err := gate.HandleBinary(context.Background(), "recovery-conn", 1,
		encodeRecoveryCompleteFrame(t, 4)); err != nil || len(response) != 0 {
		t.Fatalf("recovery marker response=%x err=%v", response, err)
	}
	if _, err := gate.HandleBinary(context.Background(), "recovery-conn", 1,
		encodeRecoveryCompleteFrame(t, 5)); err != nil {
		t.Fatalf("duplicate recovery marker: %v", err)
	}
	if _, err := gate.HandleBinary(context.Background(), "recovery-conn", 1,
		encodeBuyFrame(t, 6, "buy-after-recovery")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	requests = logic.snapshot()
	if len(requests) != 2 || !requests[1].GetTrusted().GetRecoveryCompleteAtAdmission() {
		t.Fatalf("write TrustedCommandContext=%v", requests)
	}
}

func TestRecoveryAdmissionMarkerRequiresActiveOwnSubscription(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHelloGeneration(t, gate, authSvc, "early-marker", 1)

	if _, err := gate.HandleBinary(context.Background(), "early-marker", 1,
		encodeRecoveryCompleteFrame(t, 2)); !errors.Is(err, ErrRecoveryIncomplete) {
		t.Fatalf("marker before ActiveSubscription err=%v, want ErrRecoveryIncomplete", err)
	}
	gate.mu.Lock()
	current := gate.connections["early-marker"]
	gate.mu.Unlock()
	if current.recoveryComplete {
		t.Fatal("early marker opened recovery admission")
	}
}

func TestRecoveryAdmissionMarkerAcceptsPendingOwnFarmJoin(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &trustedCaptureLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	activateHelloGeneration(t, gate, authSvc, "pending-marker", 1)
	setPendingOwnFarmJoinAcceptedForTest(t, gate, "pending-marker", 1)

	if response, err := gate.HandleBinary(context.Background(), "pending-marker", 1,
		encodeRecoveryCompleteFrame(t, 2)); err != nil || len(response) != 0 {
		t.Fatalf("marker during pending own-farm join response=%x err=%v", response, err)
	}
	gate.mu.Lock()
	current := gate.connections["pending-marker"]
	gate.mu.Unlock()
	if !current.recoveryComplete {
		t.Fatal("pending own-farm join marker did not open recovery admission")
	}
	if current.currentSubscriptionID != "pending-own-subscription" ||
		current.currentJoinAttemptID != "pending-own-attempt" ||
		current.currentFarmID != current.binding.PlayerID {
		t.Fatalf("pending marker did not publish ActiveSubscription: %+v", current)
	}

	if _, err := gate.HandleBinary(context.Background(), "pending-marker", 1,
		encodeBuyFrame(t, 3, "buy-after-pending-marker")); err != nil {
		t.Fatalf("write after pending-join marker: %v", err)
	}
	requests := logic.snapshot()
	if len(requests) != 1 || !requests[0].GetTrusted().GetRecoveryCompleteAtAdmission() {
		t.Fatalf("write TrustedCommandContext=%v", requests)
	}
}

func setPendingOwnFarmJoinAcceptedForTest(t *testing.T, gate *Gate, connectionID string, generation uint64) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current, ok := gate.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		t.Fatalf("connection %s generation %d not active", connectionID, generation)
	}
	current.pendingJoin = &pendingJoinState{
		operationID:    "pending-own-join",
		farmID:         current.binding.PlayerID,
		subscriptionID: "pending-own-subscription",
		attemptID:      "pending-own-attempt",
		accepted:       true,
		state:          pendingJoinPending,
		leaseDeadline:  time.Now().Add(time.Minute),
	}
	gate.connections[connectionID] = current
}

func TestRecoveryAdmissionResetsPerConnectionGeneration(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHelloGeneration(t, gate, authSvc, "generation-conn", 1)
	setRecoveryActiveOwnSubscriptionForTest(t, gate, "generation-conn", 1)
	if _, err := gate.HandleBinary(context.Background(), "generation-conn", 1,
		encodeRecoveryCompleteFrame(t, 2)); err != nil {
		t.Fatal(err)
	}

	activateHelloGeneration(t, gate, authSvc, "generation-conn", 2)
	gate.mu.Lock()
	current := gate.connections["generation-conn"]
	gate.mu.Unlock()
	if current.recoveryComplete {
		t.Fatal("new connection generation inherited recoveryComplete")
	}

	// A stale transport generation cannot mark the replacement connection.
	if _, err := gate.HandleBinary(context.Background(), "generation-conn", 1,
		encodeRecoveryCompleteFrame(t, 3)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("stale generation err=%v, want ErrUnauthenticated", err)
	}
	gate.mu.Lock()
	current = gate.connections["generation-conn"]
	gate.mu.Unlock()
	if current.recoveryComplete {
		t.Fatal("stale generation changed replacement recovery state")
	}
	rejectedRaw, err := gate.HandleBinary(context.Background(), "generation-conn", 2,
		encodeBuyFrame(t, 2, "buy-new-generation"))
	if err != nil {
		t.Fatalf("new generation write closed connection: %v", err)
	}
	rejectedFrame, err := protocol.Decode(rejectedRaw, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	rejected := rejectedFrame.GetCommandResult()
	if rejected == nil {
		t.Fatal("response is not a command result")
	}
	if rejected.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_RECOVERY_INCOMPLETE {
		t.Fatalf("new generation write result=%v", rejected)
	}
}
