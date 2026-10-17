package livefarmbot

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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/farmbotclient"
	wireprotocol "farmgame/internal/gateway/protocol"
	"farmgame/internal/livebot"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	harnessPlayerID    uint64 = 42
	harnessSecret             = "livefarmbot-harness-secret"
	harnessSessionID          = "session-1"
	harnessTicket             = "ticket-1"
	harnessCookie             = "cookie-1"
	harnessClientBuild        = "livefarmbot-test/1"
)

type harnessLoginPayload struct {
	platformToken string
	bootstrapID   string
	clientBuild   string
}

type protocolHarness struct {
	client             *farmbotclient.Client
	server             *httptest.Server
	loginSeen          chan harnessLoginPayload
	scriptDone         chan struct{}
	scriptErr          chan error
	loginHomeBootstrap json.RawMessage
}

func newProtocolHarness(t *testing.T, playerID uint64, script func(*websocket.Conn) error, options ...func(*farmbotclient.Config)) *protocolHarness {
	t.Helper()

	harness := &protocolHarness{
		loginSeen:  make(chan harnessLoginPayload, 8),
		scriptDone: make(chan struct{}),
		scriptErr:  make(chan error, 1),
	}
	report := func(err error) {
		if err == nil {
			return
		}
		select {
		case harness.scriptErr <- err:
		default:
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			err := fmt.Errorf("login method=%s, want POST", r.Method)
			report(err)
			http.Error(w, "test harness login method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			report(fmt.Errorf("read login body: %w", err))
			http.Error(w, "test harness login body", http.StatusBadRequest)
			return
		}
		var wire map[string]string
		if err := json.Unmarshal(body, &wire); err != nil {
			report(fmt.Errorf("decode login body: %w", err))
			http.Error(w, "test harness login json", http.StatusBadRequest)
			return
		}
		harness.loginSeen <- harnessLoginPayload{
			platformToken: wire["platform_token"],
			bootstrapID:   wire["bootstrap_id"],
			clientBuild:   wire["client_build"],
		}
		w.Header().Set("Set-Cookie", "farm_session="+harnessCookie+"; Path=/")
		w.Header().Set("Content-Type", "application/json")
		loginResponse := fmt.Sprintf("{\"player_id\":\"%d\",\"session_id\":\"%s\",\"connection_ticket\":\"%s\",\"wss_endpoint\":\"ws://%s/ws\"", playerID, harnessSessionID, harnessTicket, r.Host)
		if len(harness.loginHomeBootstrap) > 0 {
			loginResponse += ",\"home_bootstrap\":" + string(harness.loginHomeBootstrap)
		}
		loginResponse += "}"
		_, _ = io.WriteString(w, loginResponse)
	})

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(farmbotclient.FarmSessionCookieName)
		if err != nil || cookie.Value != harnessCookie {
			if err == nil {
				err = fmt.Errorf("unexpected WebSocket cookie value %q", cookie.Value)
			} else {
				err = fmt.Errorf("WebSocket cookie: %w", err)
			}
			report(err)
			http.Error(w, "test harness cookie", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			report(fmt.Errorf("upgrade WebSocket: %w", err))
			return
		}
		go func() {
			defer close(harness.scriptDone)
			defer conn.Close()
			report(script(conn))
		}()
	})

	harness.server = httptest.NewServer(mux)
	wsURL := "ws" + strings.TrimPrefix(harness.server.URL, "http") + "/ws"
	clientConfig := farmbotclient.Config{
		HTTPBaseURL: harness.server.URL,
		WSURL:       wsURL,
		Timeout:     2 * time.Second,
		ClientBuild: harnessClientBuild,
	}
	for _, option := range options {
		option(&clientConfig)
	}
	var err error
	harness.client, err = farmbotclient.NewClient(clientConfig)
	if err != nil {
		harness.server.Close()
		t.Fatalf("new farmbotclient: %v", err)
	}
	t.Cleanup(harness.server.Close)
	return harness
}

func (harness *protocolHarness) wait(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	select {
	case err := <-harness.scriptErr:
		t.Fatalf("protocol harness: %v", err)
	case <-harness.scriptDone:
	case <-timer.C:
		t.Fatal("protocol harness WebSocket script did not finish")
	}
	select {
	case err := <-harness.scriptErr:
		t.Fatalf("protocol harness: %v", err)
	default:
	}
}

func (harness *protocolHarness) nextLogin(t *testing.T) harnessLoginPayload {
	t.Helper()
	select {
	case payload := <-harness.loginSeen:
		return payload
	case <-time.After(time.Second):
		t.Fatal("HTTP login request was not observed")
		return harnessLoginPayload{}
	}
}

func assertHarnessLogin(t *testing.T, harness *protocolHarness, playerID uint64) {
	t.Helper()
	payload := harness.nextLogin(t)
	playerIDText := strconv.FormatUint(playerID, 10)
	mac := hmac.New(sha256.New, []byte(harnessSecret))
	_, _ = mac.Write([]byte(playerIDText))
	wantToken := "load:" + playerIDText + ":" + hex.EncodeToString(mac.Sum(nil))
	if payload.platformToken != wantToken {
		t.Fatalf("login platform token=%q, want %q", payload.platformToken, wantToken)
	}
	if payload.bootstrapID != "live-farm-bot-"+playerIDText {
		t.Fatalf("login bootstrap_id=%q", payload.bootstrapID)
	}
	if payload.clientBuild != harnessClientBuild {
		t.Fatalf("login client_build=%q, want %q", payload.clientBuild, harnessClientBuild)
	}
}

func (harness *protocolHarness) withHomeBootstrap(raw string) *protocolHarness {
	harness.loginHomeBootstrap = json.RawMessage(raw)
	return harness
}

type recordingObserver struct {
	mu     sync.Mutex
	events []string
	// Phases stay out of events so the lifecycle assertions keep reading as
	// the lifecycle, not as a timing log.
	phases map[string]int
}

func (observer *recordingObserver) record(event string) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.events = append(observer.events, event)
}

func (observer *recordingObserver) snapshot() []string {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]string(nil), observer.events...)
}

func (observer *recordingObserver) Phase(phase string, _ time.Duration) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.phases == nil {
		observer.phases = make(map[string]int)
	}
	observer.phases[phase]++
}

func (observer *recordingObserver) phaseCounts() map[string]int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	counts := make(map[string]int, len(observer.phases))
	for phase, count := range observer.phases {
		counts[phase] = count
	}
	return counts
}

func (observer *recordingObserver) Observe(livebot.Observation) {}
func (observer *recordingObserver) Login()                      { observer.record("login") }
func (observer *recordingObserver) Hello()                      { observer.record("hello") }
func (observer *recordingObserver) Join()                       { observer.record("join") }
func (observer *recordingObserver) Ready()                      { observer.record("ready") }
func (observer *recordingObserver) Action(action string, _ ...time.Duration) {
	observer.record("action:" + action)
}
func (observer *recordingObserver) Reconnect(_ ...int) {
	observer.record("reconnect")
}
func (observer *recordingObserver) Terminal(livebot.BotResult) {}
func (observer *recordingObserver) OnLogin()                   {}
func (observer *recordingObserver) OnHello()                   {}
func (observer *recordingObserver) OnJoin()                    {}
func (observer *recordingObserver) OnReady()                   {}
func (observer *recordingObserver) OnAction(string, ...time.Duration) {
}
func (observer *recordingObserver) OnReconnect(...int)           {}
func (observer *recordingObserver) OnTerminal(livebot.BotResult) {}

func assertObserverEvents(t *testing.T, observer *recordingObserver, want ...string) {
	t.Helper()
	if got := observer.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("observer events=%v, want %v", got, want)
	}
}

func readHarnessClientFrame(conn *websocket.Conn) (*commonpb.FarmFrame, error) {
	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if messageType != websocket.BinaryMessage {
		return nil, fmt.Errorf("client WebSocket message type=%d, want binary", messageType)
	}
	frame, err := wireprotocol.Decode(raw, wireprotocol.DefaultMaxFrameSize)
	if err != nil {
		return nil, err
	}
	if frame.GetProtocolVersion() != 4 {
		return nil, fmt.Errorf("FarmFrame protocol_version=%d, want 4", frame.GetProtocolVersion())
	}
	if err := wireprotocol.ValidateClientFrame(frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func writeHarnessServerFrame(conn *websocket.Conn, frame *commonpb.FarmFrame) error {
	raw, err := wireprotocol.Encode(frame, wireprotocol.DefaultMaxFrameSize)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

func serveJoinBarrier(conn *websocket.Conn, playerID uint64, snapshot *farmpb.FarmSnapshot) (*commonpb.ClientCommandEnvelope, error) {
	helloFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read ClientHello: %w", err)
	}
	if helloFrame.GetSequence() != 1 {
		return nil, fmt.Errorf("ClientHello sequence=%d, want 1", helloFrame.GetSequence())
	}
	hello := helloFrame.GetClientHello()
	if hello == nil {
		return nil, errors.New("first client frame is not ClientHello")
	}
	if hello.GetConnectionTicket() != harnessTicket {
		return nil, fmt.Errorf("ClientHello ticket=%q", hello.GetConnectionTicket())
	}
	if hello.GetClientBuild() != harnessClientBuild {
		return nil, fmt.Errorf("ClientHello build=%q", hello.GetClientBuild())
	}
	if err := writeHarnessServerFrame(conn, wireprotocol.NewServerHelloFrame(1, &commonpb.ServerHello{
		PlayerId:  playerID,
		SessionId: harnessSessionID,
	})); err != nil {
		return nil, fmt.Errorf("write ServerHello: %w", err)
	}

	joinFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read JOIN_FARM: %w", err)
	}
	if joinFrame.GetSequence() != 2 {
		return nil, fmt.Errorf("JOIN_FARM sequence=%d, want 2", joinFrame.GetSequence())
	}
	join := joinFrame.GetClientCommand()
	if join == nil || join.GetCommandType() != commonpb.CommandType_JOIN_FARM {
		return nil, fmt.Errorf("JOIN_FARM command=%v", join)
	}
	if strings.TrimSpace(join.GetOperationId()) == "" || strings.TrimSpace(join.GetRequestId()) == "" {
		return nil, errors.New("JOIN_FARM IDs are empty")
	}
	if len(join.GetOperationId()) > 64 || len(join.GetRequestId()) > 64 {
		return nil, errors.New("JOIN_FARM IDs exceed 64 bytes")
	}
	var joinRequest publicpb.JoinFarmRequest
	if err := proto.Unmarshal(join.GetPayload(), &joinRequest); err != nil {
		return nil, fmt.Errorf("decode JoinFarm payload: %w", err)
	}
	if joinRequest.GetTargetFarmId() != playerID {
		return nil, fmt.Errorf("JoinFarm target_farm_id=%d, want %d", joinRequest.GetTargetFarmId(), playerID)
	}

	const subscriptionID = "subscription-1"
	const joinAttemptID = "join-attempt-1"
	if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{
		OperationId: join.GetOperationId(),
		RequestId:   join.GetRequestId(),
		Status:      commonpb.CommandStatus_ACCEPTED,
		CommandType: commonpb.CommandType_JOIN_FARM,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: playerID, SubscriptionId: subscriptionID, JoinAttemptId: joinAttemptID,
		}},
	})); err != nil {
		return nil, fmt.Errorf("write JOIN ACCEPTED: %w", err)
	}

	snapshotPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmSnapshotPush{
		JoinAttemptId:   joinAttemptID,
		SnapshotVersion: 1,
		Snapshot:        snapshot,
	})
	if err != nil {
		return nil, fmt.Errorf("encode FarmSnapshot: %w", err)
	}
	if err := writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(3, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: playerID, TargetFarmId: playerID, SubscriptionId: subscriptionID,
		}},
		Payload: snapshotPayload,
	})); err != nil {
		return nil, fmt.Errorf("write FarmSnapshot: %w", err)
	}

	readyPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.SubscriptionReadyPush{
		JoinAttemptId:       joinAttemptID,
		ReadyThroughVersion: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("encode SubscriptionReady: %w", err)
	}
	if err := writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(4, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: playerID, TargetFarmId: playerID, SubscriptionId: subscriptionID,
		}},
		Payload: readyPayload,
	})); err != nil {
		return nil, fmt.Errorf("write SubscriptionReady: %w", err)
	}

	recoveryFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read ClientRecoveryComplete: %w", err)
	}
	if recoveryFrame.GetSequence() != 3 || recoveryFrame.GetControl() == nil || recoveryFrame.GetControl().GetClientRecoveryComplete() == nil {
		return nil, fmt.Errorf("recovery frame=%v", recoveryFrame)
	}
	return join, nil
}

func writeHarnessCommandSuccess(conn *websocket.Conn, sequence uint64, command *commonpb.ClientCommandEnvelope) error {
	return writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(sequence, &commonpb.CommandResultEnvelope{
		OperationId: command.GetOperationId(),
		RequestId:   command.GetRequestId(),
		Status:      commonpb.CommandStatus_SUCCEEDED,
		CommandType: command.GetCommandType(),
	}))
}

func serveJoinAndPlant(conn *websocket.Conn, playerID uint64, snapshot *farmpb.FarmSnapshot, expectedOffer int32, expectedDefinition uint32, expectedPlot uint32, expectedVersion uint64) error {
	if _, err := serveJoinBarrier(conn, playerID, snapshot); err != nil {
		return err
	}

	buyFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return fmt.Errorf("read BUY: %w", err)
	}
	if buyFrame.GetSequence() != 4 {
		return fmt.Errorf("BUY sequence=%d, want 4", buyFrame.GetSequence())
	}
	buy := buyFrame.GetClientCommand()
	if buy == nil || buy.GetCommandType() != commonpb.CommandType_BUY {
		return fmt.Errorf("BUY command=%v", buy)
	}
	var buyRequest publicpb.BuyRequest
	if err := proto.Unmarshal(buy.GetPayload(), &buyRequest); err != nil {
		return fmt.Errorf("decode BUY payload: %w", err)
	}
	if buyRequest.GetOfferId() != expectedOffer || buyRequest.GetCount() != 1 {
		return fmt.Errorf("BUY payload offer_id=%d count=%d", buyRequest.GetOfferId(), buyRequest.GetCount())
	}
	if buyRequest.GetExpectedOfferVersion() == 0 {
		return fmt.Errorf("BUY omitted expected_offer_version; the server would refuse it")
	}
	if err := writeHarnessCommandSuccess(conn, 5, buy); err != nil {
		return fmt.Errorf("write BUY result: %w", err)
	}

	plantFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return fmt.Errorf("read PLANT: %w", err)
	}
	if plantFrame.GetSequence() != 5 {
		return fmt.Errorf("PLANT sequence=%d, want 5", plantFrame.GetSequence())
	}
	plant := plantFrame.GetClientCommand()
	if plant == nil || plant.GetCommandType() != commonpb.CommandType_PLANT {
		return fmt.Errorf("PLANT command=%v", plant)
	}
	var plantRequest publicpb.PlantRequest
	if err := proto.Unmarshal(plant.GetPayload(), &plantRequest); err != nil {
		return fmt.Errorf("decode PLANT payload: %w", err)
	}
	if plantRequest.GetTargetFarmId() == 0 || plantRequest.GetTargetFarmId() != playerID {
		return fmt.Errorf("PLANT target_farm_id=%d, want bot player_id=%d", plantRequest.GetTargetFarmId(), playerID)
	}
	if plantRequest.GetPlotId() != expectedPlot || plantRequest.GetCropDefinitionId() != expectedDefinition || plantRequest.GetExpectedPlotVersion() != expectedVersion {
		return fmt.Errorf("PLANT payload plot=%d definition=%d version=%d", plantRequest.GetPlotId(), plantRequest.GetCropDefinitionId(), plantRequest.GetExpectedPlotVersion())
	}
	if err := writeHarnessCommandSuccess(conn, 6, plant); err != nil {
		return fmt.Errorf("write PLANT result: %w", err)
	}
	return nil
}

func serveJoinThenPlant(conn *websocket.Conn, playerID uint64, snapshot *farmpb.FarmSnapshot, expectedDefinition uint32, expectedPlot uint32, expectedVersion uint64) error {
	if _, err := serveJoinBarrier(conn, playerID, snapshot); err != nil {
		return err
	}
	plantFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return fmt.Errorf("read PLANT: %w", err)
	}
	if plantFrame.GetSequence() != 4 {
		return fmt.Errorf("PLANT sequence=%d, want 4", plantFrame.GetSequence())
	}
	plant := plantFrame.GetClientCommand()
	if plant == nil || plant.GetCommandType() != commonpb.CommandType_PLANT {
		return fmt.Errorf("PLANT command=%v, want plant without a preceding buy", plant)
	}
	var plantRequest publicpb.PlantRequest
	if err := proto.Unmarshal(plant.GetPayload(), &plantRequest); err != nil {
		return fmt.Errorf("decode PLANT payload: %w", err)
	}
	if plantRequest.GetPlotId() != expectedPlot || plantRequest.GetCropDefinitionId() != expectedDefinition || plantRequest.GetExpectedPlotVersion() != expectedVersion {
		return fmt.Errorf("PLANT payload plot=%d definition=%d version=%d", plantRequest.GetPlotId(), plantRequest.GetCropDefinitionId(), plantRequest.GetExpectedPlotVersion())
	}
	return writeHarnessCommandSuccess(conn, 5, plant)
}

func serveOwnRecoveryThenFriendTheft(conn *websocket.Conn, viewerID uint64, targetFarmID uint64, ownSnapshot *farmpb.FarmSnapshot, targetSnapshot *farmpb.FarmSnapshot, expectedPlot uint32, expectedCropCycleID string) error {
	if _, err := serveJoinBarrier(conn, viewerID, ownSnapshot); err != nil {
		return err
	}

	joinFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return fmt.Errorf("read friend JOIN_FARM: %w", err)
	}
	if joinFrame.GetSequence() != 4 {
		return fmt.Errorf("friend JOIN_FARM sequence=%d, want 4", joinFrame.GetSequence())
	}
	join := joinFrame.GetClientCommand()
	if join == nil || join.GetCommandType() != commonpb.CommandType_JOIN_FARM {
		return fmt.Errorf("friend JOIN_FARM command=%v", join)
	}
	var joinRequest publicpb.JoinFarmRequest
	if err := proto.Unmarshal(join.GetPayload(), &joinRequest); err != nil {
		return fmt.Errorf("decode friend JoinFarm payload: %w", err)
	}
	if joinRequest.GetTargetFarmId() != targetFarmID {
		return fmt.Errorf("friend JoinFarm target_farm_id=%d, want %d", joinRequest.GetTargetFarmId(), targetFarmID)
	}

	const subscriptionID = "friend-subscription-1"
	const joinAttemptID = "friend-join-attempt-1"
	if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(5, &commonpb.CommandResultEnvelope{
		OperationId: join.GetOperationId(),
		RequestId:   join.GetRequestId(),
		Status:      commonpb.CommandStatus_ACCEPTED,
		CommandType: commonpb.CommandType_JOIN_FARM,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: targetFarmID, SubscriptionId: subscriptionID, JoinAttemptId: joinAttemptID,
		}},
	})); err != nil {
		return fmt.Errorf("write friend JOIN ACCEPTED: %w", err)
	}

	snapshotPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmSnapshotPush{
		JoinAttemptId:   joinAttemptID,
		SnapshotVersion: targetSnapshot.GetFarmVersion(),
		Snapshot:        targetSnapshot,
	})
	if err != nil {
		return fmt.Errorf("encode friend FarmSnapshot: %w", err)
	}
	if err := writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(6, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: viewerID, TargetFarmId: targetFarmID, SubscriptionId: subscriptionID,
		}},
		Payload: snapshotPayload,
	})); err != nil {
		return fmt.Errorf("write friend FarmSnapshot: %w", err)
	}

	readyPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.SubscriptionReadyPush{
		JoinAttemptId:       joinAttemptID,
		ReadyThroughVersion: targetSnapshot.GetFarmVersion(),
	})
	if err != nil {
		return fmt.Errorf("encode friend SubscriptionReady: %w", err)
	}
	if err := writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(7, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
			TargetPlayerId: viewerID, TargetFarmId: targetFarmID, SubscriptionId: subscriptionID,
		}},
		Payload: readyPayload,
	})); err != nil {
		return fmt.Errorf("write friend SubscriptionReady: %w", err)
	}

	theftFrame, err := readHarnessClientFrame(conn)
	if err != nil {
		return fmt.Errorf("read THEFT: %w", err)
	}
	if theftFrame.GetSequence() != 5 {
		return fmt.Errorf("THEFT sequence=%d, want 5", theftFrame.GetSequence())
	}
	theft := theftFrame.GetClientCommand()
	if theft == nil || theft.GetCommandType() != commonpb.CommandType_THEFT {
		return fmt.Errorf("THEFT command=%v", theft)
	}
	var theftRequest publicpb.TheftRequest
	if err := proto.Unmarshal(theft.GetPayload(), &theftRequest); err != nil {
		return fmt.Errorf("decode THEFT payload: %w", err)
	}
	if theftRequest.GetTargetFarmId() != targetFarmID || theftRequest.GetPlotId() != expectedPlot || theftRequest.GetCropCycleId() != expectedCropCycleID {
		return fmt.Errorf("THEFT payload farm=%d plot=%d cycle=%q", theftRequest.GetTargetFarmId(), theftRequest.GetPlotId(), theftRequest.GetCropCycleId())
	}
	if err := writeHarnessCommandSuccess(conn, 8, theft); err != nil {
		return fmt.Errorf("write THEFT result: %w", err)
	}
	return nil
}

func harnessSnapshot(playerID uint64, plots ...*farmpb.PlotView) *farmpb.FarmSnapshot {
	return &farmpb.FarmSnapshot{
		OwnerId:        playerID,
		SnapshotTimeMs: 123,
		FarmVersion:    7,
		Plots:          plots,
	}
}

func harnessExecutor(t *testing.T, harness *protocolHarness) *Executor {
	t.Helper()
	executor, err := NewExecutor(harness.client, SharedHMACCredentials([]byte(harnessSecret)))
	if err != nil {
		t.Fatalf("new Executor: %v", err)
	}
	return executor
}

func harnessSpec(playerID uint64, scenario string) livebot.BotSpec {
	return livebot.BotSpec{
		RunID:    "run-real-farmframe",
		BotID:    "bot-real-farmframe",
		PlayerID: playerID,
		Index:    0,
		Seed:     7,
		Scenario: scenario,
	}
}

func TestExecutorJoinRealFarmFrameLifecycle(t *testing.T) {
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		_, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
			PlotIndex:   1,
			PlotVersion: 4,
			LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
		}))
		return err
	})
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "join"), observer)

	harness.wait(t)
	assertHarnessLogin(t, harness, harnessPlayerID)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	if !result.Ready {
		t.Fatal("successful join result is not Ready")
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready")
}

func TestExecutorPlantSendsBuyAndPlantPayloads(t *testing.T) {
	const (
		plotID      uint32 = 9
		plotVersion uint64 = 12
	)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		return serveJoinAndPlant(
			conn,
			harnessPlayerID,
			harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
				PlotIndex:   plotID,
				PlotVersion: plotVersion,
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}),
			cropcatalog.CarrotOfferID,
			uint32(cropcatalog.CarrotDefinitionID),
			plotID,
			plotVersion,
		)
	})
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "plant"), observer)

	harness.wait(t)
	assertHarnessLogin(t, harness, harnessPlayerID)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	if !result.Ready {
		t.Fatal("successful plant result is not Ready")
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready", "action:buy", "action:plant")
}

// Journey on an empty farm is planting: buy a seed, then plant it. The
// sequential walk must not fail just because there was nothing to harvest
// or water first.
func TestExecutorJourneyEmptyFarmPlants(t *testing.T) {
	const (
		plotID      uint32 = 9
		plotVersion uint64 = 12
	)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		return serveJoinAndPlant(
			conn,
			harnessPlayerID,
			harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
				PlotIndex:   plotID,
				PlotVersion: plotVersion,
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}),
			cropcatalog.CarrotOfferID,
			uint32(cropcatalog.CarrotDefinitionID),
			plotID,
			plotVersion,
		)
	}).withHomeBootstrap(`{"player_state_revision":1}`)
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "journey"), observer)

	harness.wait(t)
	assertHarnessLogin(t, harness, harnessPlayerID)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	if !result.Ready {
		t.Fatal("successful journey result is not Ready")
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready", "action:buy", "action:plant")
}

// A player who already has a seed in the login projection does not buy
// another before planting. Always buying would spend coins the run already
// granted and add a command the farm did not need.
func TestExecutorJourneySkipsBuyWhenSeedIsAlreadyInBag(t *testing.T) {
	const (
		plotID      uint32 = 9
		plotVersion uint64 = 12
	)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		return serveJoinThenPlant(
			conn,
			harnessPlayerID,
			harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
				PlotIndex:   plotID,
				PlotVersion: plotVersion,
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}),
			uint32(cropcatalog.CarrotDefinitionID),
			plotID,
			plotVersion,
		)
	}).withHomeBootstrap(fmt.Sprintf(
		`{"player_state_revision":1,"inventory":{"items":[{"item_id":%d,"count":1}]}}`,
		cropcatalog.CarrotSeedItemID,
	))
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "journey"), observer)

	harness.wait(t)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready", "action:plant")
}

func TestExecutorJourneyFallsBackToWSSBootstrap(t *testing.T) {
	sawBootstrap := make(chan struct{}, 1)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		helloFrame, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read ClientHello: %w", err)
		}
		if helloFrame.GetClientHello() == nil {
			return errors.New("first client frame is not ClientHello")
		}
		if err := writeHarnessServerFrame(conn, wireprotocol.NewServerHelloFrame(1, &commonpb.ServerHello{
			PlayerId: harnessPlayerID, SessionId: harnessSessionID,
		})); err != nil {
			return err
		}
		frame, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read bootstrap: %w", err)
		}
		command := frame.GetClientCommand()
		if command == nil || command.GetCommandType() != commonpb.CommandType_GET_PLAYER_BOOTSTRAP {
			return fmt.Errorf("second command is %v, want GET_PLAYER_BOOTSTRAP", command)
		}
		sawBootstrap <- struct{}{}
		return writeHarnessCommandSuccess(conn, 2, command)
	})
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "journey"), &recordingObserver{})
	harness.wait(t)
	select {
	case <-sawBootstrap:
	default:
		t.Fatal("journey did not send GET_PLAYER_BOOTSTRAP after a login without home_bootstrap")
	}
	if result.Status == livebot.BotSucceeded {
		t.Fatal("this harness stops after bootstrap, so the bot must not report success")
	}
}

func TestSeedStockLoadsOnlyTheConfiguredSeed(t *testing.T) {
	stock := newSeedStock(cropcatalog.CarrotSeedItemID)
	stock.load(&publicpb.GetPlayerBootstrapResult{
		Inventory: &publicpb.InventoryView{Items: []*economypb.ItemStack{
			{ItemId: cropcatalog.TomatoSeedItemID, Count: 9},
			{ItemId: cropcatalog.CarrotSeedItemID, Count: 2},
		}},
	})
	if !stock.has() || stock.count != 2 {
		t.Fatalf("stock count=%d has=%v, want 2 carrot seeds", stock.count, stock.has())
	}
	stock.consume()
	stock.consume()
	if stock.has() {
		t.Fatal("bag should be empty after planting both seeds")
	}
}

func TestProduceStockRequiresVisibleHarvestSettlementBeforeSell(t *testing.T) {
	stock := newProduceStock(cropcatalog.CarrotItemID)
	empty := &publicpb.GetPlayerBootstrapResult{Inventory: &publicpb.InventoryView{}}
	stock.load(empty)
	stock.noteHarvest(0, 500)
	if _, ok := stock.sellTarget(); ok {
		t.Fatal("pending harvest must not be sellable before a Player refresh")
	}
	stock.refresh(empty)
	if _, ok := stock.sellTarget(); ok || !stock.needsRefresh() {
		t.Fatal("missing reward must remain pending for the next turn")
	}
	stock.refresh(&publicpb.GetPlayerBootstrapResult{Inventory: &publicpb.InventoryView{Items: []*economypb.ItemStack{{
		ItemId: cropcatalog.CarrotItemID, Count: 500,
	}}}})
	target, ok := stock.sellTarget()
	if !ok || target.sellCount != 500 || stock.needsRefresh() {
		t.Fatalf("settled stock target=%+v ok=%v refresh=%v, want 500 and no refresh", target, ok, stock.needsRefresh())
	}
	stock.sold(target.sellCount)
	if _, ok := stock.sellTarget(); ok {
		t.Fatal("sold produce must leave no local sell target")
	}
}

func TestJourneyBlocksPlantUntilHarvestRewardClearsPlayerOccupancy(t *testing.T) {
	executor := &Executor{options: normalizedTestOptions(t)}
	spec := harnessSpec(harnessPlayerID, "journey")
	snapshot := harnessSnapshot(harnessPlayerID,
		&farmpb.PlotView{PlotIndex: 0, PlotVersion: 3, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
		&farmpb.PlotView{PlotIndex: 1, PlotVersion: 1, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
	)
	stock := newProduceStock(cropcatalog.CarrotItemID)
	stock.load(&publicpb.GetPlayerBootstrapResult{Inventory: &publicpb.InventoryView{}})
	stock.noteHarvest(0, 1)
	if action, target, ok, err := executor.chooseOnlineAction(spec, 0, snapshot, harnessPlayerID, stock, nil); err != nil || !ok || action != "plant" || target.plotID != 1 {
		t.Fatalf("pending reward action=%q target=%+v ok=%v err=%v, want unblocked plot 1", action, target, ok, err)
	}
	stock.refresh(&publicpb.GetPlayerBootstrapResult{Inventory: &publicpb.InventoryView{Items: []*economypb.ItemStack{{
		ItemId: cropcatalog.CarrotItemID, Count: 1,
	}}}})
	action, target, ok, err := executor.chooseOnlineAction(spec, 1, snapshot, harnessPlayerID, stock, nil)
	if err != nil || !ok || action != "sell" || target.sellCount != 1 {
		t.Fatalf("settled reward action=%q target=%+v ok=%v err=%v", action, target, ok, err)
	}
	stock.sold(1)
	action, target, ok, err = executor.chooseOnlineAction(spec, 2, snapshot, harnessPlayerID, stock, nil)
	if err != nil || !ok || action != "plant" || target.plotID != 0 {
		t.Fatalf("cleared occupancy action=%q target=%+v ok=%v err=%v", action, target, ok, err)
	}
}

func TestProduceStockBacksOffAfterRefreshFailure(t *testing.T) {
	stock := newProduceStock(cropcatalog.CarrotItemID)
	started := time.Now()
	stock.notePendingHarvest(0)
	if stock.refreshDue(started.Add(9*time.Second)) || !stock.refreshDue(started.Add(11*time.Second)) {
		t.Fatalf("initial harvest projection wait deadline=%v", stock.nextRefreshAt)
	}
	// Isolate the failure-backoff sequence from the initial batching delay.
	stock.nextRefreshAt = time.Time{}
	now := time.Unix(100, 0)
	stock.refreshFailed(now)
	if stock.refreshDue(now) || stock.refreshDue(now.Add(4999*time.Millisecond)) || !stock.refreshDue(now.Add(5*time.Second)) {
		t.Fatalf("first refresh backoff deadline=%v", stock.nextRefreshAt)
	}
	stock.refreshFailed(now.Add(5 * time.Second))
	if stock.refreshDue(now.Add(14*time.Second)) || !stock.refreshDue(now.Add(15*time.Second)) {
		t.Fatalf("second refresh backoff deadline=%v", stock.nextRefreshAt)
	}
}

func TestPlantReservationsExcludeOldEmptyViewUntilProjectionAdvances(t *testing.T) {
	reservations := plantReservations{1: plantReservation{version: 7, processing: true}, 2: plantReservation{version: 9}}
	snapshot := harnessSnapshot(harnessPlayerID,
		&farmpb.PlotView{PlotIndex: 1, PlotVersion: 7, CropCycleId: "cycle-1", LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
		&farmpb.PlotView{PlotIndex: 2, PlotVersion: 9, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
		&farmpb.PlotView{PlotIndex: 3, PlotVersion: 1, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
	)
	reservations.reconcile(snapshot)
	if _, exists := reservations[1]; exists {
		t.Fatal("projected crop did not clear plot 1 reservation")
	}
	if _, exists := reservations[2]; !exists {
		t.Fatal("unchanged empty plot 2 lost its reservation")
	}
	target, ok := findPlantPlotExcluding(snapshot, reservations)
	if !ok || target.plotID != 3 {
		t.Fatalf("target=%+v ok=%v, want unreserved plot 3", target, ok)
	}
	reservations.reconcile(harnessSnapshot(harnessPlayerID,
		&farmpb.PlotView{PlotIndex: 2, PlotVersion: 10, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
	))
	if len(reservations) != 0 {
		t.Fatalf("newer empty projection did not release reservation: %v", reservations)
	}
}

func TestExecutorJourneyWithNoLegalPlotSucceeds(t *testing.T) {
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		_, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
			PlotIndex:   1,
			PlotVersion: 2,
			LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED,
		}))
		return err
	}).withHomeBootstrap(`{"player_state_revision":1}`)
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "journey"), observer)

	harness.wait(t)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q, want success on an empty farm", result.Status, result.Error)
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready")
}

// Maturity is a time transition with no event behind it, so no push ever says
// a crop became ready and a cached view keeps reporting GROWING. A bot that
// believes the cached phase plants and then waits forever, which reads as a
// server with nothing to do rather than a client that stopped looking.
func TestMaturityIsReadFromTheClockNotTheCachedPhase(t *testing.T) {
	ready := &farmpb.PlotView{
		PlotIndex: 2, PlotVersion: 3, CropCycleId: "cycle-ready",
		CropDefinitionId: cropcatalog.CarrotDefinitionID,
		// What the view still says, and what the server would say now.
		CropPhase:        farmpb.CropPhase_GROWING,
		MatureAtMs:       time.Now().Add(-time.Minute).UnixMilli(),
		HarvestableCount: 10,
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	target, found := findMaturePlot(harnessSnapshot(harnessPlayerID, ready))
	if !found || target.plotID != ready.PlotIndex {
		t.Fatalf("a crop past its maturity time must be harvestable: found=%v target=%+v", found, target)
	}

	stillGrowing := &farmpb.PlotView{
		PlotIndex: 3, PlotVersion: 1, CropCycleId: "cycle-growing",
		CropPhase:        farmpb.CropPhase_GROWING,
		MatureAtMs:       time.Now().Add(time.Minute).UnixMilli(),
		HarvestableCount: 10,
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	if _, found := findMaturePlot(harnessSnapshot(harnessPlayerID, stillGrowing)); found {
		t.Fatal("a crop before its maturity time must not be harvestable")
	}
	justMaturedLocally := &farmpb.PlotView{
		PlotIndex: 5, PlotVersion: 1, CropCycleId: "cycle-clock-skew",
		CropPhase: farmpb.CropPhase_GROWING, MatureAtMs: time.Now().Add(-100 * time.Millisecond).UnixMilli(),
		HarvestableCount: 10, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	if _, found := findMaturePlot(harnessSnapshot(harnessPlayerID, justMaturedLocally)); found {
		t.Fatal("a cached growing crop inside the server-clock safety margin must wait")
	}

	// A crop that has been stolen down to nothing is past its maturity time
	// and still not worth a command.
	emptied := &farmpb.PlotView{
		PlotIndex: 4, PlotVersion: 1, CropCycleId: "cycle-emptied",
		CropPhase:        farmpb.CropPhase_MATURE,
		MatureAtMs:       time.Now().Add(-time.Minute).UnixMilli(),
		HarvestableCount: 0,
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	if _, found := findMaturePlot(harnessSnapshot(harnessPlayerID, emptied)); found {
		t.Fatal("a crop with nothing left on it must not be harvestable")
	}
	if _, found := findCarePlot(harnessSnapshot(harnessPlayerID, ready)); found {
		t.Fatal("a crop past its maturity time must not be watered")
	}
	if _, found := findCarePlot(harnessSnapshot(harnessPlayerID, stillGrowing)); !found {
		t.Fatal("an unwatered growing crop must still be waterable")
	}
	if wait := timeUntilNextMature(harnessSnapshot(harnessPlayerID, stillGrowing)); wait <= 0 || wait > time.Minute+time.Second {
		t.Fatalf("timeUntilNextMature=%s, want a wait until the growing crop matures", wait)
	}
}

func TestCareWaitsForIncidentOffsetAndStopsAtMaturity(t *testing.T) {
	now := time.Now()
	beforeDue := &farmpb.PlotView{
		PlotIndex:        5,
		PlotVersion:      1,
		CropCycleId:      "cycle-before-care",
		CropDefinitionId: cropcatalog.CarrotDefinitionID,
		CropPhase:        farmpb.CropPhase_GROWING,
		PlantAtMs:        now.Add(-2 * time.Second).UnixMilli(),
		MatureAtMs:       now.Add(8 * time.Second).UnixMilli(),
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	if _, found := findCarePlot(harnessSnapshot(harnessPlayerID, beforeDue)); found {
		t.Fatal("CARE before the carrot's 3-second water offset must not be selected")
	}

	dueAndGrowing := proto.Clone(beforeDue).(*farmpb.PlotView)
	dueAndGrowing.CropCycleId = "cycle-care-due"
	dueAndGrowing.PlantAtMs = now.Add(-3500 * time.Millisecond).UnixMilli()
	if _, found := findCarePlot(harnessSnapshot(harnessPlayerID, dueAndGrowing)); !found {
		t.Fatal("CARE after the water offset and before maturity must be selected")
	}

	mature := proto.Clone(dueAndGrowing).(*farmpb.PlotView)
	mature.CropCycleId = "cycle-care-mature"
	mature.CropPhase = farmpb.CropPhase_GROWING
	mature.MatureAtMs = now.Add(-time.Second).UnixMilli()
	if _, found := findCarePlot(harnessSnapshot(harnessPlayerID, mature)); found {
		t.Fatal("CARE at or after maturity must not be selected")
	}
}

// A bot starts with enough coins for a handful of seeds. If it never turns
// produce back into coins it spends the rest of a long run being rejected,
// which quietly lowers the load the run applies while looking like a server
// refusing work. Selling exactly what the harvest returned is what keeps the
// cadence fundable, so the sell must carry the server's own quantity.
func TestExecutorHarvestSellsWhatItReaped(t *testing.T) {
	const (
		plotID    uint32 = 3
		reaped    uint32 = 17
		cycleName        = "cycle-harvest"
	)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		snapshot := harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
			PlotIndex:        plotID,
			PlotVersion:      5,
			CropCycleId:      cycleName,
			CropDefinitionId: cropcatalog.CarrotDefinitionID,
			CropPhase:        farmpb.CropPhase_MATURE,
			HarvestableCount: int32(reaped),
			LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
		})
		if _, err := serveJoinBarrier(conn, harnessPlayerID, snapshot); err != nil {
			return err
		}

		harvestFrame, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read HARVEST: %w", err)
		}
		harvest := harvestFrame.GetClientCommand()
		if harvest == nil || harvest.GetCommandType() != commonpb.CommandType_HARVEST {
			return fmt.Errorf("HARVEST command=%v", harvest)
		}
		outcome, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.HarvestResult{
			TargetFarmId: harnessPlayerID, PlotId: plotID, CropCycleId: cycleName,
			HarvestedQuantity: reaped,
		})
		if err != nil {
			return fmt.Errorf("encode HarvestResult: %w", err)
		}
		if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(5, &commonpb.CommandResultEnvelope{
			OperationId: harvest.GetOperationId(), RequestId: harvest.GetRequestId(),
			Status: commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_HARVEST,
			Payload: outcome,
		})); err != nil {
			return fmt.Errorf("write HARVEST result: %w", err)
		}

		bootstrapFrame, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read harvest settlement probe: %w", err)
		}
		bootstrap := bootstrapFrame.GetClientCommand()
		if bootstrap == nil || bootstrap.GetCommandType() != commonpb.CommandType_GET_PLAYER_BOOTSTRAP {
			return fmt.Errorf("settlement probe command=%v", bootstrap)
		}
		bootstrapPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&publicpb.GetPlayerBootstrapResult{
			Inventory: &publicpb.InventoryView{Items: []*economypb.ItemStack{{ItemId: cropcatalog.CarrotItemID, Count: int64(reaped)}}},
		})
		if err != nil {
			return fmt.Errorf("encode settlement probe result: %w", err)
		}
		if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(6, &commonpb.CommandResultEnvelope{
			OperationId: bootstrap.GetOperationId(), RequestId: bootstrap.GetRequestId(),
			Status: commonpb.CommandStatus_SUCCEEDED, CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
			Payload: bootstrapPayload,
		})); err != nil {
			return fmt.Errorf("write settlement probe result: %w", err)
		}

		sellFrame, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read SELL: %w", err)
		}
		sell := sellFrame.GetClientCommand()
		if sell == nil || sell.GetCommandType() != commonpb.CommandType_SELL {
			return fmt.Errorf("SELL command=%v", sell)
		}
		var sellRequest publicpb.SellRequest
		if err := proto.Unmarshal(sell.GetPayload(), &sellRequest); err != nil {
			return fmt.Errorf("decode SELL payload: %w", err)
		}
		if sellRequest.GetItemId() != cropcatalog.CarrotItemID || sellRequest.GetCount() != int64(reaped) {
			return fmt.Errorf("SELL payload item=%d count=%d, want item=%d count=%d",
				sellRequest.GetItemId(), sellRequest.GetCount(), cropcatalog.CarrotItemID, reaped)
		}
		if sell.GetOperationId() == harvest.GetOperationId() {
			return errors.New("SELL reused the harvest operation id")
		}
		return writeHarnessCommandSuccess(conn, 7, sell)
	})
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "harvest"), observer)

	harness.wait(t)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready", "action:harvest", "action:sell")
}

func TestExecutorPlantFailsWithoutSuitablePlot(t *testing.T) {
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		_, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID,
			&farmpb.PlotView{
				PlotIndex:   1,
				PlotVersion: 2,
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED,
			},
			&farmpb.PlotView{
				PlotIndex:   2,
				PlotVersion: 3,
				CropCycleId: "occupied",
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			},
		))
		return err
	})
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), harnessSpec(harnessPlayerID, "plant"), observer)

	harness.wait(t)
	assertHarnessLogin(t, harness, harnessPlayerID)
	if result.Status != livebot.BotFailed {
		t.Fatalf("result status=%s error=%q, want failed", result.Status, result.Error)
	}
	if !strings.Contains(result.Error, "no suitable target plot") {
		t.Fatalf("failure error=%q, want no-suitable-plot reason", result.Error)
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready")
}

func TestExecutorFriendTheftCompletesOwnFarmRecoveryFirst(t *testing.T) {
	const (
		viewerID     uint64 = harnessPlayerID
		targetFarmID uint64 = 84
		plotID       uint32 = 3
		cropCycleID         = "friend-cycle-1"
	)
	harness := newProtocolHarness(t, viewerID, func(conn *websocket.Conn) error {
		return serveOwnRecoveryThenFriendTheft(
			conn,
			viewerID,
			targetFarmID,
			harnessSnapshot(viewerID, &farmpb.PlotView{
				PlotIndex:   0,
				PlotVersion: 1,
				LockState:   farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}),
			harnessSnapshot(targetFarmID, &farmpb.PlotView{
				PlotIndex:        plotID,
				PlotVersion:      9,
				CropCycleId:      cropCycleID,
				CropDefinitionId: 1,
				CropPhase:        farmpb.CropPhase_MATURE,
				HarvestableCount: 10,
				LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}),
			plotID,
			cropCycleID,
		)
	})
	executor, err := NewExecutorWithOptions(harness.client, SharedHMACCredentials([]byte(harnessSecret)), Options{TargetFarmID: targetFarmID})
	if err != nil {
		t.Fatalf("new friend-farm Executor: %v", err)
	}
	observer := &recordingObserver{}
	result := executor.Run(context.Background(), harnessSpec(viewerID, "theft"), observer)

	harness.wait(t)
	assertHarnessLogin(t, harness, viewerID)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("result status=%s error=%q", result.Status, result.Error)
	}
	if !result.Ready {
		t.Fatal("successful theft result is not Ready")
	}
	assertObserverEvents(t, observer, "login", "hello", "join", "ready", "action:theft")
}

// A capacity test measures bots that stay online and keep acting, so the
// executor must hold one session across many actions, give each action its own
// operation identity, and follow the realtime stream well enough to stop
// targeting a plot it already changed.
func TestExecutorSustainedSessionActsRepeatedlyOnOneConnection(t *testing.T) {
	const actions = 3
	plots := make([]*farmpb.PlotView, 0, 4)
	for index := uint32(1); index <= 4; index++ {
		plots = append(plots, &farmpb.PlotView{
			PlotIndex:        index,
			PlotVersion:      1,
			CropCycleId:      fmt.Sprintf("cycle-%d", index),
			CropDefinitionId: 1,
			CropPhase:        farmpb.CropPhase_GROWING,
			LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
		})
	}
	type observedCare struct {
		plotID      uint32
		operationID string
	}
	cares := make(chan observedCare, actions)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		if _, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID, plots...)); err != nil {
			return err
		}
		sequence := uint64(4)
		for issued := 0; issued < actions; issued++ {
			frame, err := readHarnessClientFrame(conn)
			if err != nil {
				return fmt.Errorf("read CARE %d: %w", issued, err)
			}
			command := frame.GetClientCommand()
			if command == nil || command.GetCommandType() != commonpb.CommandType_CARE {
				return fmt.Errorf("online command %d is %v, want CARE", issued, command)
			}
			var request publicpb.CareRequest
			if err := proto.Unmarshal(command.GetPayload(), &request); err != nil {
				return fmt.Errorf("decode CARE %d: %w", issued, err)
			}
			cares <- observedCare{plotID: request.GetPlotId(), operationID: command.GetOperationId()}
			sequence++
			if err := writeHarnessCommandSuccess(conn, sequence, command); err != nil {
				return fmt.Errorf("write CARE result %d: %w", issued, err)
			}
			// Watering the plot is what removes it from the bot's candidate
			// set, so the next action can only differ if the bot is really
			// applying the broadcast.
			sequence++
			payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmChangePush{
				Change: &farmpb.FarmChange{
					OwnerId:     harnessPlayerID,
					FarmVersion: uint64(8 + issued),
					ChangedPlots: []*farmpb.PlotView{{
						PlotIndex: request.GetPlotId(), PlotVersion: 2, CropCycleId: request.GetCropCycleId(),
						CropDefinitionId: 1, CropPhase: farmpb.CropPhase_GROWING, Watered: true,
						LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
					}},
				},
			})
			if err != nil {
				return fmt.Errorf("encode FARM_CHANGE %d: %w", issued, err)
			}
			if err := writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(sequence, &commonpb.ServerPushEnvelope{
				PushType: commonpb.PushType_FARM_CHANGE,
				PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
					TargetPlayerId: harnessPlayerID, TargetFarmId: harnessPlayerID, SubscriptionId: "subscription-1",
				}},
				Payload: payload,
			})); err != nil {
				return fmt.Errorf("write FARM_CHANGE %d: %w", issued, err)
			}
		}
		return nil
	}, func(config *farmbotclient.Config) { config.TrackFarmView = true })

	spec := harnessSpec(harnessPlayerID, "care")
	spec.SessionDuration = 10 * time.Second
	spec.ThinkTime = 5 * time.Millisecond
	spec.SessionActions = actions
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), spec, observer)

	harness.wait(t)
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("sustained result status=%s error=%q", result.Status, result.Error)
	}
	if result.Online <= 0 {
		t.Fatal("a sustained bot must report the time it held its session")
	}
	if result.Rejected != 0 || result.Skipped != 0 {
		t.Fatalf("unexpected rejected=%d skipped=%d", result.Rejected, result.Skipped)
	}
	close(cares)
	seenPlots := make(map[uint32]bool)
	seenOperations := make(map[string]bool)
	for care := range cares {
		seenPlots[care.plotID] = true
		seenOperations[care.operationID] = true
	}
	if len(seenPlots) != actions {
		t.Fatalf("bot cared for %d distinct plots, want %d; it is not following the realtime stream", len(seenPlots), actions)
	}
	if len(seenOperations) != actions {
		t.Fatalf("bot reused operation ids across %d actions; repeats would be idempotent replays", actions-len(seenOperations)+1)
	}
	phases := observer.phaseCounts()
	if phases[livebot.PhaseActionPrefix+"care"] != actions {
		t.Fatalf("care phase samples=%d, want %d", phases[livebot.PhaseActionPrefix+"care"], actions)
	}
	if phases[livebot.PhaseAdmission] != actions {
		t.Fatalf("admission phase samples=%d, want %d", phases[livebot.PhaseAdmission], actions)
	}
	for _, phase := range []string{livebot.PhaseLogin, livebot.PhaseHello, livebot.PhaseJoin} {
		if phases[phase] != 1 {
			t.Fatalf("%s phase samples=%d, want 1", phase, phases[phase])
		}
	}
	if result.Broadcasts != 0 {
		t.Fatalf("farm changes must feed the view, not be discarded: broadcasts=%d", result.Broadcasts)
	}
}

func TestExecutorUnlockProcessingUsesProjectionWithoutResultPolling(t *testing.T) {
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		snapshot := harnessSnapshot(harnessPlayerID, &farmpb.PlotView{
			PlotIndex: 5, PlotVersion: 1, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED,
			UnlockCandidate: true, UnlockRuleVersion: 1,
		})
		if _, err := serveJoinBarrier(conn, harnessPlayerID, snapshot); err != nil {
			return err
		}
		frame, err := readHarnessClientFrame(conn)
		if err != nil {
			return err
		}
		command := frame.GetClientCommand()
		if command == nil || command.GetCommandType() != commonpb.CommandType_UNLOCK_PLOT {
			return fmt.Errorf("warmup command=%v, want UNLOCK_PLOT", command)
		}
		if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(5, &commonpb.CommandResultEnvelope{
			OperationId: command.GetOperationId(), RequestId: command.GetRequestId(),
			Status: commonpb.CommandStatus_PROCESSING, CommandType: commonpb.CommandType_UNLOCK_PLOT,
			RetryDirective: commonpb.RetryDirective_POLL_OPERATION_RESULT,
		})); err != nil {
			return err
		}
		payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&farmpb.FarmChangePush{Change: &farmpb.FarmChange{
			OwnerId: harnessPlayerID, FarmVersion: 8, ChangedPlots: []*farmpb.PlotView{{
				PlotIndex: 5, PlotVersion: 2, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
				UnlockRuleVersion: 1,
			}},
		}})
		if err != nil {
			return err
		}
		return writeHarnessServerFrame(conn, wireprotocol.NewServerPushFrame(6, &commonpb.ServerPushEnvelope{
			PushType: commonpb.PushType_FARM_CHANGE,
			PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: harnessPlayerID, TargetFarmId: harnessPlayerID, SubscriptionId: "subscription-1",
			}},
			Payload: payload,
		}))
	}, func(config *farmbotclient.Config) { config.TrackFarmView = true })
	executor, err := NewExecutorWithOptions(harness.client, SharedHMACCredentials([]byte(harnessSecret)), Options{
		UnlockAllPlots: true, UnlockPlotLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := harnessSpec(harnessPlayerID, "join")
	spec.SessionDuration = 20 * time.Millisecond
	spec.ThinkTime = 5 * time.Millisecond
	result := executor.Run(context.Background(), spec, &recordingObserver{})
	harness.wait(t)
	if result.Status != livebot.BotSucceeded || result.ResultQueries != 0 {
		t.Fatalf("unlock result=%+v, want success with no GET_OPERATION_RESULT", result)
	}
}

func TestExecutorSustainedPlantReservesProcessingPlotWithoutPollingOrProjection(t *testing.T) {
	plots := []*farmpb.PlotView{
		{PlotIndex: 0, PlotVersion: 1, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
		{PlotIndex: 1, PlotVersion: 1, LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED},
	}
	planted := make(chan uint32, len(plots))
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		if _, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID, plots...)); err != nil {
			return err
		}
		sequence := uint64(4)
		for issued := 0; issued < len(plots); issued++ {
			buyFrame, err := readHarnessClientFrame(conn)
			if err != nil {
				return fmt.Errorf("read BUY %d: %w", issued, err)
			}
			buy := buyFrame.GetClientCommand()
			if buy == nil || buy.GetCommandType() != commonpb.CommandType_BUY {
				return fmt.Errorf("online command before PLANT %d is %v, want BUY", issued, buy)
			}
			sequence++
			if err := writeHarnessCommandSuccess(conn, sequence, buy); err != nil {
				return err
			}

			plantFrame, err := readHarnessClientFrame(conn)
			if err != nil {
				return fmt.Errorf("read PLANT %d: %w", issued, err)
			}
			plant := plantFrame.GetClientCommand()
			if plant == nil || plant.GetCommandType() != commonpb.CommandType_PLANT {
				return fmt.Errorf("online command %d is %v, want PLANT", issued, plant)
			}
			var request publicpb.PlantRequest
			if err := proto.Unmarshal(plant.GetPayload(), &request); err != nil {
				return fmt.Errorf("decode PLANT %d: %w", issued, err)
			}
			planted <- request.GetPlotId()
			sequence++
			if issued == 0 {
				if err := writeHarnessServerFrame(conn, wireprotocol.NewCommandResultFrame(sequence, &commonpb.CommandResultEnvelope{
					OperationId: plant.GetOperationId(), RequestId: plant.GetRequestId(),
					Status: commonpb.CommandStatus_PROCESSING, CommandType: commonpb.CommandType_PLANT,
					RetryDirective: commonpb.RetryDirective_POLL_OPERATION_RESULT,
				})); err != nil {
					return err
				}
				// Deliberately send neither a terminal lookup outcome nor FarmChange.
				// The next command must target another empty plot from the same old
				// snapshot instead of polling forever or repeating plot 1.
			} else if err := writeHarnessCommandSuccess(conn, sequence, plant); err != nil {
				return err
			}
		}
		return nil
	}, func(config *farmbotclient.Config) { config.TrackFarmView = true })

	spec := harnessSpec(harnessPlayerID, "plant")
	spec.SessionDuration = 5 * time.Second
	spec.ThinkTime = 5 * time.Millisecond
	spec.SessionActions = len(plots)
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), spec, observer)

	harness.wait(t)
	if result.Status != livebot.BotSucceeded || result.Rejected != 0 || result.Timeouts != 0 {
		t.Fatalf("sustained plant result=%+v", result)
	}
	if result.Processing != 1 || result.Pending != 1 || result.ProjectionPending != 2 || result.ResultQueries != 0 {
		t.Fatalf("processing=%d pending=%d projection_pending=%d result_queries=%d, want 1/1/2/0", result.Processing, result.Pending, result.ProjectionPending, result.ResultQueries)
	}
	close(planted)
	var seen []uint32
	for plotID := range planted {
		seen = append(seen, plotID)
	}
	if !reflect.DeepEqual(seen, []uint32{0, 1}) {
		t.Fatalf("planted plots=%v, want [0 1]", seen)
	}
}

// A saturated server answers late. If that ended the bot, the run would show
// a client-side cliff exactly where the server started queueing, and the
// timeouts would be reported as bot failures instead of as load evidence.
func TestExecutorSustainedSessionSurvivesCommandTimeout(t *testing.T) {
	plots := []*farmpb.PlotView{
		{PlotIndex: 1, PlotVersion: 1, CropCycleId: "cycle-1", CropDefinitionId: 1, CropPhase: farmpb.CropPhase_GROWING},
		{PlotIndex: 2, PlotVersion: 1, CropCycleId: "cycle-2", CropDefinitionId: 1, CropPhase: farmpb.CropPhase_GROWING},
	}
	served := make(chan struct{}, 4)
	harness := newProtocolHarness(t, harnessPlayerID, func(conn *websocket.Conn) error {
		if _, err := serveJoinBarrier(conn, harnessPlayerID, harnessSnapshot(harnessPlayerID, plots...)); err != nil {
			return err
		}
		// Read the first command and never answer it, then answer the second
		// one normally: the session must still be usable afterwards.
		first, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read first CARE: %w", err)
		}
		if first.GetClientCommand().GetCommandType() != commonpb.CommandType_CARE {
			return errors.New("first online command is not CARE")
		}
		served <- struct{}{}
		second, err := readHarnessClientFrame(conn)
		if err != nil {
			return fmt.Errorf("read second CARE: %w", err)
		}
		served <- struct{}{}
		return writeHarnessCommandSuccess(conn, 5, second.GetClientCommand())
	}, func(config *farmbotclient.Config) {
		config.TrackFarmView = true
		config.Timeout = 150 * time.Millisecond
	})

	spec := harnessSpec(harnessPlayerID, "care")
	spec.SessionDuration = 5 * time.Second
	spec.ThinkTime = 5 * time.Millisecond
	spec.SessionActions = 2
	observer := &recordingObserver{}
	result := harnessExecutor(t, harness).Run(context.Background(), spec, observer)

	harness.wait(t)
	if len(served) != 2 {
		t.Fatalf("server saw %d commands, want 2", len(served))
	}
	if result.Status != livebot.BotSucceeded {
		t.Fatalf("a timed-out command must not fail the bot: status=%s error=%q", result.Status, result.Error)
	}
	if result.Timeouts != 1 {
		t.Fatalf("timeouts=%d, want 1", result.Timeouts)
	}
	if result.Planned != 2 {
		t.Fatalf("planned=%d, want the configured action budget of 2", result.Planned)
	}
}

// Planned is what the cadence asked for over the window the bot really had.
func TestPlannedActionsFollowsCadenceAndBudget(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		think   time.Duration
		budget  int
		online  time.Duration
		planned int
	}{
		{name: "cadence", think: time.Second, online: 10 * time.Second, planned: 11},
		{name: "budget caps cadence", think: time.Second, budget: 4, online: 10 * time.Second, planned: 4},
		{name: "truncated window", think: time.Second, online: 2 * time.Second, planned: 3},
		{name: "no cadence is planless", think: 0, online: 10 * time.Second, planned: 0},
		{name: "no cadence but budgeted", think: 0, budget: 7, online: 10 * time.Second, planned: 7},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			spec := harnessSpec(harnessPlayerID, "care")
			spec.ThinkTime = testCase.think
			spec.SessionActions = testCase.budget
			if got := plannedActions(spec, testCase.online); got != testCase.planned {
				t.Fatalf("plannedActions=%d, want %d", got, testCase.planned)
			}
		})
	}
}

func TestDeterministicIDsStableAndBounded(t *testing.T) {
	runID := strings.Repeat("run-", 40)
	botID := strings.Repeat("bot-", 40)
	action := strings.Repeat("plant-", 20)
	operationID, requestID := DeterministicIDs(runID, botID, action, 17)
	operationIDAgain, requestIDAgain := DeterministicIDs(runID, botID, action, 17)
	if operationID != operationIDAgain || requestID != requestIDAgain {
		t.Fatalf("DeterministicIDs is not stable: (%q, %q) != (%q, %q)", operationID, requestID, operationIDAgain, requestIDAgain)
	}
	for name, value := range map[string]string{"operation_id": operationID, "request_id": requestID} {
		if len(value) == 0 || len(value) > 64 {
			t.Fatalf("%s length=%d, want 1..64", name, len(value))
		}
		for index := 0; index < len(value); index++ {
			if value[index] < 0x20 || value[index] > 0x7e {
				t.Fatalf("%s contains non-printable byte at %d", name, index)
			}
		}
	}
}

// TestActionIsDrawnFromWhatTheFarmAllows pins the rule that keeps a load run
// honest: a mix naming actions the farm cannot currently serve must still
// produce a command, because a bot that skips is a bot applying no load.
func TestActionIsDrawnFromWhatTheFarmAllows(t *testing.T) {
	const playerID = harnessPlayerID
	executor := &Executor{options: normalizedTestOptions(t)}
	spec := harnessSpec(playerID, "mixed")
	// The mix asks overwhelmingly for harvesting, and the farm has nothing
	// ripe. Only the empty plot can be acted on.
	spec.ActionMix = map[string]float64{"harvest": 99, "plant": 1}
	snapshot := harnessSnapshot(playerID, &farmpb.PlotView{
		PlotIndex: 4, PlotVersion: 2,
		LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	})

	for ordinal := 0; ordinal < 32; ordinal++ {
		action, target, ok, err := executor.chooseAction(spec, ordinal, snapshot, playerID)
		if err != nil {
			t.Fatalf("chooseAction: %v", err)
		}
		if !ok {
			t.Fatalf("ordinal %d skipped although one plot was plantable", ordinal)
		}
		if action != "plant" {
			t.Fatalf("ordinal %d chose %q, but only planting had a target", ordinal, action)
		}
		if target.plotID != 4 {
			t.Fatalf("ordinal %d targeted plot %d, want 4", ordinal, target.plotID)
		}
	}
}

// TestNothingLegalStillSkips guards the other side of it: renormalising over
// legal actions must not invent a target when the farm truly offers none.
func TestNothingLegalStillSkips(t *testing.T) {
	const playerID = harnessPlayerID
	executor := &Executor{options: normalizedTestOptions(t)}
	spec := harnessSpec(playerID, "mixed")
	spec.ActionMix = map[string]float64{"harvest": 1, "care": 1}
	// One plot, planted, already watered and not yet ripe: nothing applies.
	snapshot := harnessSnapshot(playerID, &farmpb.PlotView{
		PlotIndex: 1, PlotVersion: 3, CropCycleId: "cycle-growing", Watered: true,
		CropPhase: farmpb.CropPhase_GROWING, MatureAtMs: time.Now().Add(time.Hour).UnixMilli(),
		LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	})
	if _, _, ok, err := executor.chooseAction(spec, 0, snapshot, playerID); err != nil || ok {
		t.Fatalf("chooseAction ok=%v err=%v, want a skip", ok, err)
	}
}

// TestFixedScenarioIsNotRenormalised keeps a single-scenario run literal: a
// theft run that finds nothing to steal must skip, never substitute.
func TestFixedScenarioIsNotRenormalised(t *testing.T) {
	const playerID = harnessPlayerID
	executor := &Executor{options: normalizedTestOptions(t)}
	spec := harnessSpec(playerID, "theft")
	spec.ActionMix = map[string]float64{"plant": 1}
	snapshot := harnessSnapshot(playerID, &farmpb.PlotView{
		PlotIndex: 4, PlotVersion: 2,
		LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	})
	// Stealing from one's own farm is never legal, and planting is not on
	// offer here however plantable that plot is.
	action, _, ok, err := executor.chooseAction(spec, 0, snapshot, playerID)
	if err != nil {
		t.Fatalf("chooseAction: %v", err)
	}
	if ok {
		t.Fatalf("a theft run acted as %q on its own farm", action)
	}
}

func TestNormalizeScenarioAcceptsJourney(t *testing.T) {
	got, err := normalizeScenario("journey")
	if err != nil {
		t.Fatalf("normalizeScenario(journey): %v", err)
	}
	if got != "journey" {
		t.Fatalf("normalizeScenario(journey)=%q, want journey", got)
	}
	if mixedScenario("journey") {
		t.Fatal("journey must not be treated as a random mix")
	}
}

// Journey is a priority walk over the current snapshot: harvest (or theft on
// a friend's farm) before care, care before plant. A just-planted carrot is
// still growing, so the same tick must not try to harvest it.
func TestJourneyPicksHighestPriorityLegalAction(t *testing.T) {
	const playerID = harnessPlayerID
	const friendFarmID uint64 = 84
	executor := &Executor{options: normalizedTestOptions(t)}
	empty := &farmpb.PlotView{
		PlotIndex: 1, PlotVersion: 2,
		LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	unwatered := &farmpb.PlotView{
		PlotIndex: 2, PlotVersion: 3, CropCycleId: "cycle-growing",
		CropDefinitionId: cropcatalog.CarrotDefinitionID,
		CropPhase:        farmpb.CropPhase_GROWING,
		MatureAtMs:       time.Now().Add(time.Minute).UnixMilli(),
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}
	mature := &farmpb.PlotView{
		PlotIndex: 3, PlotVersion: 4, CropCycleId: "cycle-ready",
		CropDefinitionId: cropcatalog.CarrotDefinitionID,
		CropPhase:        farmpb.CropPhase_MATURE,
		MatureAtMs:       time.Now().Add(-time.Minute).UnixMilli(),
		HarvestableCount: 10,
		LockState:        farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
	}

	for _, testCase := range []struct {
		name         string
		targetFarmID uint64
		plots        []*farmpb.PlotView
		wantAction   string
		wantPlot     uint32
		wantOK       bool
	}{
		{
			name: "harvest before care and plant", targetFarmID: playerID,
			plots:      []*farmpb.PlotView{empty, unwatered, mature},
			wantAction: "harvest", wantPlot: 3, wantOK: true,
		},
		{
			name: "care before plant when nothing is mature", targetFarmID: playerID,
			plots:      []*farmpb.PlotView{empty, unwatered},
			wantAction: "care", wantPlot: 2, wantOK: true,
		},
		{
			name: "plant when the farm is empty", targetFarmID: playerID,
			plots:      []*farmpb.PlotView{empty},
			wantAction: "plant", wantPlot: 1, wantOK: true,
		},
		{
			name: "friend farm prefers theft", targetFarmID: friendFarmID,
			plots:      []*farmpb.PlotView{unwatered, mature},
			wantAction: "theft", wantPlot: 3, wantOK: true,
		},
		{
			name: "friend farm cares when nothing is stealable", targetFarmID: friendFarmID,
			plots:      []*farmpb.PlotView{unwatered},
			wantAction: "care", wantPlot: 2, wantOK: true,
		},
		{
			name: "nothing legal still skips", targetFarmID: playerID,
			plots: []*farmpb.PlotView{{
				PlotIndex: 1, PlotVersion: 3, CropCycleId: "cycle-growing", Watered: true,
				CropPhase: farmpb.CropPhase_GROWING, MatureAtMs: time.Now().Add(time.Hour).UnixMilli(),
				LockState: farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED,
			}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			spec := harnessSpec(playerID, "journey")
			snapshot := harnessSnapshot(testCase.targetFarmID, testCase.plots...)
			action, target, ok, err := executor.chooseAction(spec, 0, snapshot, testCase.targetFarmID)
			if err != nil {
				t.Fatalf("chooseAction: %v", err)
			}
			if ok != testCase.wantOK {
				t.Fatalf("chooseAction ok=%v action=%q, want ok=%v", ok, action, testCase.wantOK)
			}
			if !testCase.wantOK {
				return
			}
			if action != testCase.wantAction {
				t.Fatalf("chose %q, want %q", action, testCase.wantAction)
			}
			if target.plotID != testCase.wantPlot {
				t.Fatalf("targeted plot %d, want %d", target.plotID, testCase.wantPlot)
			}
		})
	}
}

func normalizedTestOptions(t *testing.T) Options {
	t.Helper()
	options, err := normalizeOptions(Options{})
	if err != nil {
		t.Fatalf("normalizeOptions: %v", err)
	}
	return options
}

func TestFriendRingFarmMappingWrapsDeterministically(t *testing.T) {
	options, err := normalizeOptions(Options{FriendRingStart: 1000, FriendRingSize: 4, FriendRingOffset: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1001, 1002, 1003, 1000}
	for index := range want {
		got := options.farmFor(livebot.BotSpec{PlayerID: 1000 + uint64(index), Index: index, Scenario: "theft"})
		if got != want[index] {
			t.Fatalf("index %d target=%d, want %d", index, got, want[index])
		}
	}
	// Every load-generator process starts its local Index at zero. A shard in
	// the middle of the global ring must still target its PlayerID neighbour.
	if got := options.farmFor(livebot.BotSpec{PlayerID: 1002, Index: 0, Scenario: "theft"}); got != 1003 {
		t.Fatalf("sharded target=%d, want 1003", got)
	}
	if _, err := normalizeOptions(Options{TargetFarmID: 99, FriendRingStart: 1000, FriendRingSize: 4, FriendRingOffset: 1}); err == nil {
		t.Fatal("accepted friend ring with explicit target")
	}
}
