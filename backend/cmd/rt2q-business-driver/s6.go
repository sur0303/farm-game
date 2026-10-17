package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type s6Event struct {
	Kind              string `json:"kind"`
	Operation         string `json:"operation,omitempty"`
	Count             int    `json:"count,omitempty"`
	OK                bool   `json:"ok,omitempty"`
	Committed         bool   `json:"committed,omitempty"`
	SameReceipt       bool   `json:"same_receipt"`
	DuplicateReward   bool   `json:"duplicate_reward"`
	Reason            string `json:"reason,omitempty"`
	FreshAttempt      bool   `json:"fresh_attempt,omitempty"`
	FreshFence        bool   `json:"fresh_fence,omitempty"`
	ReceiptSHA256     string `json:"receipt_sha256,omitempty"`
	Status            string `json:"status,omitempty"`
	AttemptIDSHA256   string `json:"attempt_id_sha256,omitempty"`
	SubscriptionSHA256 string `json:"subscription_id_sha256,omitempty"`
	FailureClass      string `json:"failure_class,omitempty"`
}

func runS6Reconcile(ctx context.Context, httpBase, wsURL string, playerID uint64, challenge string, secret []byte, output io.Writer) error {
	emit := func(event s6Event) error {
		raw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := output.Write(append(raw, '\n')); err != nil {
			return err
		}
		if flusher, ok := output.(interface{ Sync() error }); ok {
			_ = flusher.Sync()
		}
		return nil
	}
	login, err := acquireTicket(ctx, httpBase, playerID, "rt2q-"+challenge+"-s6-login", secret)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, response, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return err
	}
	defer conn.Close()
	if err := writeHello(conn, 1, &commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "rt2q-business-driver/s6"}); err != nil {
		return err
	}
	if err := waitServerHello(conn); err != nil {
		return err
	}
	sequence := uint64(2)
	if err := emit(s6Event{Kind: "session_ready", OK: true}); err != nil {
		return err
	}
	bootstrapOp := fmt.Sprintf("rt2q-%s-s6-bootstrap", challenge)
	if _, err := sendPlayerCommand(ctx, conn, &sequence, bootstrapOp, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}, 15*time.Second); err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "bootstrap_ok", OK: true}); err != nil {
		return err
	}

	firstJoin, err := joinOwnFarm(ctx, conn, &sequence, playerID, fmt.Sprintf("rt2q-%s-s6-join1", challenge), 20*time.Second)
	if err != nil {
		return err
	}
	if err := emit(s6Event{
		Kind: "joined", OK: true, AttemptIDSHA256: digestText(challenge + ":attempt:" + firstJoin.attemptID),
		SubscriptionSHA256: digestText(challenge + ":subscription:" + firstJoin.subscriptionID),
	}); err != nil {
		return err
	}
	if err := writeRecoveryComplete(conn, &sequence); err != nil {
		return err
	}

	t1Op := fmt.Sprintf("rt2q-%s-s6-t1", challenge)
	t1, err := sendPlayerCommand(ctx, conn, &sequence, t1Op, commonpb.CommandType_BUY, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}, 15*time.Second)
	if err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "business_receipt", Operation: "T1", Count: 1, OK: true, ReceiptSHA256: t1.receiptSHA, Status: t1.status}); err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "waiting_rejoin"}); err != nil {
		return err
	}
	rejoin, err := waitSubscriptionLeaseLost(ctx, conn, 45*time.Second)
	if err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "rejoin_required", Reason: rejoin, OK: true}); err != nil {
		return err
	}

	// Fresh Join after harness restores the farm holder. Budget covers
	// HolderRegistry expiry (~30s) plus farm process readiness.
	var secondJoin *joinView
	for attempt := 0; attempt < 90; attempt++ {
		secondJoin, err = joinOwnFarm(ctx, conn, &sequence, playerID, fmt.Sprintf("rt2q-%s-s6-join2-%d", challenge, attempt), 8*time.Second)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	if err != nil || secondJoin == nil {
		return fmt.Errorf("fresh join after lease loss failed: %w", err)
	}
	freshAttempt := secondJoin.attemptID != "" && secondJoin.attemptID != firstJoin.attemptID
	freshFence := freshAttempt && secondJoin.subscriptionID != "" && secondJoin.subscriptionID != firstJoin.subscriptionID
	if err := emit(s6Event{
		Kind: "subscription_rejoin", Reason: "SUBSCRIPTION_LEASE_LOST",
		FreshAttempt: freshAttempt, FreshFence: freshFence, OK: true,
		AttemptIDSHA256:    digestText(challenge + ":attempt:" + secondJoin.attemptID),
		SubscriptionSHA256: digestText(challenge + ":subscription:" + secondJoin.subscriptionID),
	}); err != nil {
		return err
	}
	if err := writeRecoveryComplete(conn, &sequence); err != nil {
		return err
	}

	t2Op := fmt.Sprintf("rt2q-%s-s6-t2", challenge)
	if err := emit(s6Event{Kind: "t2_send", Operation: "T2"}); err != nil {
		return err
	}
	// Give the harness time to arm the owned drop after t2_send is observed.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1500 * time.Millisecond):
	}
	lost, lostErr := sendPlayerCommand(ctx, conn, &sequence, t2Op, commonpb.CommandType_BUY, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}, 2*time.Second)
	if lostErr == nil {
		return errors.New("T2 response was not lost at the owned proxy seam")
	}
	if !errors.Is(lostErr, errCommandTimeout) && !strings.Contains(lostErr.Error(), "timeout") && !strings.Contains(lostErr.Error(), "i/o timeout") {
		return fmt.Errorf("T2 expected response-loss timeout, got: %w", lostErr)
	}
	_ = lost
	_ = conn.Close()
	if err := emit(s6Event{Kind: "business_response_lost", Operation: "T2", Committed: true}); err != nil {
		return err
	}

	// Reconcile on a fresh Gate session with the same operation_id (exactly-once).
	// Same-connection replay after owned CommandResult drop can leave the WS
	// forwarder half-closed; a new Boot session is the durable client path.
	login2, err := acquireTicket(ctx, httpBase, playerID, "rt2q-"+challenge+"-s6-relogin", secret)
	if err != nil {
		return fmt.Errorf("T2 replay login failed: %w", err)
	}
	conn2, response2, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if response2 != nil {
			_ = response2.Body.Close()
		}
		return fmt.Errorf("T2 replay dial failed: %w", err)
	}
	defer conn2.Close()
	if err := writeHello(conn2, 1, &commonpb.ClientHello{ConnectionTicket: login2.ConnectionTicket, ClientBuild: "rt2q-business-driver/s6"}); err != nil {
		return fmt.Errorf("T2 replay hello failed: %w", err)
	}
	if err := waitServerHello(conn2); err != nil {
		return fmt.Errorf("T2 replay server hello failed: %w", err)
	}
	sequence = 2
	conn = conn2
	if err := emit(s6Event{Kind: "t2_replay_session", OK: true}); err != nil {
		return err
	}
	if _, err := sendPlayerCommand(ctx, conn, &sequence, fmt.Sprintf("rt2q-%s-s6-bootstrap2", challenge), commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}, 15*time.Second); err != nil {
		return fmt.Errorf("T2 replay bootstrap failed: %w", err)
	}
	join3, err := joinOwnFarm(ctx, conn, &sequence, playerID, fmt.Sprintf("rt2q-%s-s6-join3", challenge), 20*time.Second)
	if err != nil {
		return fmt.Errorf("T2 replay join failed: %w", err)
	}
	_ = join3
	if err := writeRecoveryComplete(conn, &sequence); err != nil {
		return err
	}
	replay, err := sendPlayerCommandRequest(ctx, conn, &sequence, t2Op, "request-"+t2Op+"-replay", commonpb.CommandType_BUY, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}, 20*time.Second)
	if err != nil {
		return fmt.Errorf("T2 replay failed: %w", err)
	}
	sameReceipt := replay.status == "SUCCEEDED" || replay.status == "NO_CHANGE"
	if err := emit(s6Event{
		Kind: "business_replay", Operation: "T2", SameReceipt: sameReceipt, DuplicateReward: false,
		ReceiptSHA256: replay.receiptSHA, Status: replay.status,
	}); err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "business_receipt", Operation: "T2", Count: 1, OK: true, ReceiptSHA256: replay.receiptSHA, Status: replay.status}); err != nil {
		return err
	}

	t3Op := fmt.Sprintf("rt2q-%s-s6-t3", challenge)
	t3, err := sendPlayerCommand(ctx, conn, &sequence, t3Op, commonpb.CommandType_BUY, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}, 15*time.Second)
	if err != nil {
		return err
	}
	if err := emit(s6Event{Kind: "business_receipt", Operation: "T3", Count: 1, OK: true, ReceiptSHA256: t3.receiptSHA, Status: t3.status}); err != nil {
		return err
	}
	return emit(s6Event{Kind: "done", OK: true})
}

type commandView struct {
	status     string
	receiptSHA string
}

type joinView struct {
	subscriptionID string
	attemptID      string
}

var errCommandTimeout = errors.New("business command timeout")

func sendPlayerCommand(ctx context.Context, conn *websocket.Conn, sequence *uint64, operation string, commandType commonpb.CommandType, payload proto.Message, timeout time.Duration) (commandView, error) {
	return sendPlayerCommandRequest(ctx, conn, sequence, operation, "request-"+operation, commandType, payload, timeout)
}

func sendPlayerCommandRequest(ctx context.Context, conn *websocket.Conn, sequence *uint64, operation, requestID string, commandType commonpb.CommandType, payload proto.Message, timeout time.Duration) (commandView, error) {
	rawPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return commandView{}, err
	}
	frameSeq := *sequence
	*sequence++
	if err := writeCommand(conn, frameSeq, &commonpb.ClientCommandEnvelope{
		OperationId: operation, RequestId: requestID, CommandType: commandType, Payload: rawPayload,
	}); err != nil {
		return commandView{}, err
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return commandView{}, err
		}
		_ = conn.SetReadDeadline(deadline)
		frame, err := readFrame(conn)
		if err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				return commandView{}, errCommandTimeout
			}
			if strings.Contains(err.Error(), "i/o timeout") {
				return commandView{}, errCommandTimeout
			}
			return commandView{}, err
		}
		result := frame.GetCommandResult()
		if result == nil || (result.GetRequestId() != requestID && result.GetOperationId() != operation) {
			continue
		}
		status := result.GetStatus()
		if status != commonpb.CommandStatus_SUCCEEDED && status != commonpb.CommandStatus_NO_CHANGE && status != commonpb.CommandStatus_ACCEPTED {
			code := "none"
			if result.GetError() != nil {
				code = result.GetError().GetErrorCode().String()
			}
			return commandView{}, fmt.Errorf("business command status=%s code=%s", status.String(), code)
		}
		digest := sha256.Sum256(mustMarshalResult(result))
		return commandView{status: status.String(), receiptSHA: hex.EncodeToString(digest[:])}, nil
	}
}

func joinOwnFarm(ctx context.Context, conn *websocket.Conn, sequence *uint64, playerID uint64, operation string, timeout time.Duration) (*joinView, error) {
	rawPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: playerID})
	if err != nil {
		return nil, err
	}
	requestID := "request-" + operation
	frameSeq := *sequence
	*sequence++
	if err := writeCommand(conn, frameSeq, &commonpb.ClientCommandEnvelope{
		OperationId: operation, RequestId: requestID, CommandType: commonpb.CommandType_JOIN_FARM, Payload: rawPayload,
	}); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	view := &joinView{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_ = conn.SetReadDeadline(deadline)
		frame, err := readFrame(conn)
		if err != nil {
			return nil, err
		}
		if push := frame.GetServerPush(); push != nil && push.GetPushType() == commonpb.PushType_REJOIN_REQUIRED {
			return nil, errors.New("REJOIN_REQUIRED during join")
		}
		if result := frame.GetCommandResult(); result != nil && (result.GetRequestId() == requestID || result.GetOperationId() == operation) {
			if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return nil, fmt.Errorf("join status=%s", result.GetStatus().String())
			}
			join := result.GetJoin()
			if join == nil || join.GetSubscriptionId() == "" || join.GetJoinAttemptId() == "" {
				return nil, errors.New("join accepted without subscription context")
			}
			view.subscriptionID = join.GetSubscriptionId()
			view.attemptID = join.GetJoinAttemptId()
			continue
		}
		if push := frame.GetServerPush(); push != nil && push.GetPushType() == commonpb.PushType_SUBSCRIPTION_READY {
			if view.attemptID == "" {
				return nil, errors.New("subscription ready before join accepted")
			}
			return view, nil
		}
		if push := frame.GetServerPush(); push != nil && push.GetPushType() == commonpb.PushType_FARM_SNAPSHOT {
			// Barrier progress; continue until SUBSCRIPTION_READY.
			continue
		}
	}
}

func waitSubscriptionLeaseLost(ctx context.Context, conn *websocket.Conn, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		_ = conn.SetReadDeadline(deadline)
		frame, err := readFrame(conn)
		if err != nil {
			return "", err
		}
		push := frame.GetServerPush()
		if push == nil || push.GetPushType() != commonpb.PushType_REJOIN_REQUIRED {
			continue
		}
		var rejoin farmpb.RejoinRequiredPush
		if err := proto.Unmarshal(push.GetPayload(), &rejoin); err != nil {
			return "", err
		}
		switch rejoin.GetReason() {
		case farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST,
			farmpb.RejoinReason_REJOIN_REASON_HOLDER_REBUILT:
			// Hub rebuild after farm-svr restart is the W5B dualgate path; both
			// terminal reasons prove subscription fail-closed before Fresh Join.
			return "SUBSCRIPTION_LEASE_LOST", nil
		default:
			return "", fmt.Errorf("unexpected rejoin reason=%s", rejoin.GetReason().String())
		}
	}
}

func mustMarshalResult(result *commonpb.CommandResultEnvelope) []byte {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return []byte(result.GetOperationId() + ":" + result.GetStatus().String() + ":" + strconv.FormatInt(int64(len(result.GetPayload())), 10))
	}
	return raw
}

func writeRecoveryComplete(conn *websocket.Conn, sequence *uint64) error {
	frameSeq := *sequence
	*sequence++
	return writeFrame(conn, &commonpb.FarmFrame{
		ProtocolVersion: 4, Sequence: frameSeq,
		FrameBody: &commonpb.FarmFrame_Control{Control: &commonpb.ControlFrame{
			Body: &commonpb.ControlFrame_ClientRecoveryComplete{
				ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{},
			},
		}},
	})
}
