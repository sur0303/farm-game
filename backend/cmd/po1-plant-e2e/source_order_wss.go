package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/testkit/gateclient"
	"farmgame/internal/worker/sourceorder"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var errWSSFrameTimeout = errors.New("wss frame timeout")

type wssFrame struct {
	kind gateclient.FrameKind
	body []byte
	err  error
}

type wssPlanted struct {
	plotID      uint32
	cropCycleID string
	matureAtMs  int64
	farmVersion uint64
	plotVersion uint64
}

// wssSession is a single FarmFrame v4 WebSocket client. It never sets a
// connection read deadline: gorilla poisons the socket after any read timeout.
type wssSession struct {
	httpBase string
	wsURL    string
	client   *http.Client
	user     string
	password string

	playerID uint64
	ticket   string
	conn     *websocket.Conn
	sequence uint64
	frames   chan wssFrame

	serverTimeAtSync int64
	monotonicAtSync  time.Time
	seedItemID       int32
	wallet           int64
	exp              int64
	inventory        map[int32]int64
}

func (e *e2e) startGate() error {
	e.gateCmd = e.startProcess(e.gateBinary, e.gateEnvironment())
	if err := waitTCP(e.gateHTTPAddr); err != nil {
		return fmt.Errorf("farm-gate readiness: %w", err)
	}
	if err := waitTCP(e.gateDownlinkAddr); err != nil {
		return fmt.Errorf("farm-gate downlink: %w", err)
	}
	log.Printf("SOURCE_ORDER_WSS farm-gate ready http=%s ws=ws://%s/ws downlink=%s",
		e.gateHTTPAddr, e.gateHTTPAddr, e.gateDownlinkAddr)
	return nil
}

func (e *e2e) gateEnvironment() []string {
	httpBase := "http://" + e.gateHTTPAddr
	return e.environment(map[string]string{
		"GATEWAY_LISTEN":                e.gateHTTPAddr,
		"GATEWAY_ID":                    "po1-e2e-gate",
		"FARM_APP_PROFILE":              "test",
		"GATEWAY_DOWNLINK_LISTEN":       e.gateDownlinkAddr,
		"GATE_CONTROL_LISTEN":           e.gateControlAddr,
		"GATE_TRANSPORT":                "websocket",
		"GATE_WS_PATH":                  "/ws",
		"GATE_WS_ALLOWED_ORIGINS":       "127.0.0.1,localhost," + e.gateHTTPAddr,
		"PLAYER_ADDR":                   e.playerAddr,
		"FARM_ADDR":                     e.farmAddr,
		"LOGIC_HEALTH_ADDR":             "http://" + e.farmHealth,
		"FARM_REDIS_URL":                e.redisURL,
		"WEB_ACCOUNT_STORE_PREFIX":      "po1-e2e-" + e.runID + ":web-account",
		"WEB_ACCOUNT_PLAYER_ID_BASE":    "1000000",
		"WEB_LOGIN_RATE_LIMIT_PREFIX":   "po1-e2e-" + e.runID + ":web-login-limit",
		"DEMO_PLATFORM_TOKEN":           "demo-platform-token",
		"DEMO_PLAYER_ID":                "10001",
		"HTTP_SESSION_TTL":              "2h",
		"HTTP_SESSION_COOKIE_SECURE":    "0",
		"PERMIT_SECRET":                 "po1-e2e-permit-secret-20260805-" + e.runID,
		"GATEWAY_TICKET_TTL":            "60s",
		"GATE_ROUTE_LEASE_TTL":          "30s",
		"GATE_BOOT_LEASE_PREFIX":        "po1-e2e-" + e.runID + ":gate-boot-lease",
		"GATE_BOOT_LEASE_TTL":           "15s",
		"GATE_BOOT_LEASE_RENEW_TIMEOUT": "3s",
		"INITIAL_RULE_VERSION":          "1",
		"GATE_WS_RATE_PER_SEC":          "100",
		"GATE_WS_RATE_BURST":            "200",
		"WSS_ENDPOINT":                  "ws://" + e.gateHTTPAddr + "/ws",
		"HTTP_PUBLIC_BASE":              httpBase,
	})
}

func (e *e2e) sourceOrderWSSExercise() error {
	log.Printf("SOURCE_ORDER_WSS: backend=real Redis (%s) processes=player-svr/farm-svr/farm-worker/farm-gate flags=PLANT_SOURCE_ORDER+HARVEST_SOURCE_ORDER", e.redisAddr)
	if err := e.startWorker(); err != nil {
		return err
	}
	session := &wssSession{
		httpBase: "http://" + e.gateHTTPAddr,
		wsURL:    "ws://" + e.gateHTTPAddr + "/ws",
		client:   &http.Client{Timeout: 10 * time.Second},
		user:     "sowss" + strings.ReplaceAll(e.runID, "-", ""),
		password: "SowssPass" + strings.ReplaceAll(e.runID, "-", ""),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := session.register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if err := session.login(ctx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	e.owner = session.playerID
	e.farmID = session.playerID
	log.Printf("SOURCE_ORDER_WSS login player_id=%d", session.playerID)
	if err := session.connect(ctx); err != nil {
		return fmt.Errorf("wss hello: %w", err)
	}
	defer session.close()

	snapshot, err := session.joinWithRetry("so-wss-join-"+e.runID, session.playerID)
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	log.Printf("SOURCE_ORDER_WSS joined farm=%d plots=%d version=%d", snapshot.GetOwnerId(), len(snapshot.GetPlots()), snapshot.GetFarmVersion())
	if err := session.bootstrap(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := session.sendRecoveryComplete(); err != nil {
		return fmt.Errorf("recovery complete: %w", err)
	}
	if err := e.sourceOrderWSSSupportBeforePlant(session); err != nil {
		return err
	}
	if err := session.buy(); err != nil {
		return fmt.Errorf("buy seed: %w", err)
	}
	plantOp := "so-wss-plant-" + e.runID
	planted, err := session.plant(plantOp, snapshot)
	if err != nil {
		orderID := mustPlantSourceOrderID(session.playerID, plantOp)
		return fmt.Errorf("plant: %w (%s)", err, e.sourceOrderProbe(
			actorrouting.DomainPlayer,
			string(identity.WorkTypePlant),
			orderID,
			session.playerID,
		))
	}
	log.Printf("SOURCE_ORDER_WSS planted plot=%d cycle=%s mature_at_ms=%d farm_version=%d",
		planted.plotID, planted.cropCycleID, planted.matureAtMs, planted.farmVersion)
	if err := e.waitSourceOrderCompleted(
		actorrouting.DomainPlayer,
		string(identity.WorkTypePlant),
		mustPlantSourceOrderID(session.playerID, plantOp),
		session.playerID,
	); err != nil {
		return fmt.Errorf("plant source order: %w", err)
	}
	if err := e.waitDueEmpty(actorrouting.DomainPlayer, string(identity.WorkTypePlant), session.playerID); err != nil {
		return fmt.Errorf("plant due: %w", err)
	}
	if err := e.assertNoLegacyWorkRecord(string(identity.WorkTypePlant), mustPlantSourceOrderID(session.playerID, plantOp)); err != nil {
		return fmt.Errorf("plant leftover WorkRecord: %w", err)
	}
	if err := e.sourceOrderWSSSocial(ctx, session, &planted); err != nil {
		return err
	}

	if err := session.waitGrow(&planted); err != nil {
		return fmt.Errorf("grow: %w", err)
	}
	harvestOp := "so-wss-harvest-" + e.runID
	walletBefore := session.wallet
	if err := session.harvest(harvestOp, planted); err != nil {
		if current, ok := plotVersionFromConflict(err); ok && current != planted.plotVersion {
			log.Printf("SOURCE_ORDER_WSS harvest version retry expected=%d current=%d", planted.plotVersion, current)
			planted.plotVersion = current
			harvestOp = harvestOp + "-v"
			if retryErr := session.harvest(harvestOp, planted); retryErr != nil {
				return fmt.Errorf("harvest: %w", retryErr)
			}
		} else {
			return fmt.Errorf("harvest: %w", err)
		}
	}
	if err := e.waitSourceOrderCompleted(
		actorrouting.DomainFarm,
		string(identity.WorkTypeHarvest),
		mustHarvestSourceOrderID(session.playerID, harvestOp),
		session.playerID,
	); err != nil {
		return fmt.Errorf("harvest source order: %w", err)
	}
	if err := e.waitDueEmpty(actorrouting.DomainFarm, string(identity.WorkTypeHarvest), session.playerID); err != nil {
		return fmt.Errorf("harvest due: %w", err)
	}
	if err := session.waitProduceLanded(walletBefore); err != nil {
		return err
	}
	if err := e.assertNoLegacyWorkRecord(string(identity.WorkTypeHarvest), mustHarvestSourceOrderID(session.playerID, harvestOp)); err != nil {
		return fmt.Errorf("harvest leftover WorkRecord: %w", err)
	}
	if err := e.sourceOrderWSSSupportAfterHarvest(session); err != nil {
		return err
	}
	log.Printf("SOURCE_ORDER_WSS evidence: support+farm commands used SourceOrder/DurableCommand; PLANT/HARVEST/CARE/THEFT COMPLETED; DueIndex empty; player=%d", session.playerID)
	return nil
}

func (e *e2e) sourceOrderWSSSocial(ctx context.Context, owner *wssSession, planted *wssPlanted) error {
	friendSess := &wssSession{
		httpBase: "http://" + e.gateHTTPAddr,
		wsURL:    "ws://" + e.gateHTTPAddr + "/ws",
		client:   &http.Client{Timeout: 10 * time.Second},
		user:     "sofri" + strings.ReplaceAll(e.runID, "-", ""),
		password: "SofriPass" + strings.ReplaceAll(e.runID, "-", ""),
	}
	if err := friendSess.register(ctx); err != nil {
		return fmt.Errorf("friend register: %w", err)
	}
	if err := friendSess.login(ctx); err != nil {
		return fmt.Errorf("friend login: %w", err)
	}
	e.viewer = friendSess.playerID
	if err := e.activateWSSFriendship(owner.playerID, friendSess.playerID); err != nil {
		return err
	}
	if err := friendSess.connect(ctx); err != nil {
		return fmt.Errorf("friend hello: %w", err)
	}
	defer friendSess.close()

	// CARE/THEFT require ClientRecoveryComplete, but the marker is only
	// accepted on the visitor's own farm. Visiting the owner farm first
	// and sending the marker closes the socket (gate: recovery incomplete).
	if _, err := friendSess.joinWithRetry("so-wss-friend-own-"+e.runID, friendSess.playerID); err != nil {
		return fmt.Errorf("friend own join: %w", err)
	}
	if err := friendSess.sendRecoveryComplete(); err != nil {
		return fmt.Errorf("friend recovery complete: %w", err)
	}
	if err := friendSess.bootstrap(); err != nil {
		return fmt.Errorf("friend bootstrap: %w", err)
	}
	if _, err := friendSess.joinWithRetry("so-wss-friend-visit-"+e.runID, owner.playerID); err != nil {
		return fmt.Errorf("friend visit join: %w", err)
	}
	expBefore := friendSess.exp

	if err := friendSess.waitUntil(planted.matureAtMs-7_000+300, 8*time.Second); err != nil {
		return fmt.Errorf("wait water due: %w", err)
	}
	careOp := "so-wss-care-" + e.runID
	incidentID, err := identity.CareIncidentID(planted.cropCycleID, "WATER", 1)
	if err != nil {
		return fmt.Errorf("care incident: %w", err)
	}
	careResult, err := friendSess.care(careOp, owner.playerID, *planted, incidentID)
	if err != nil {
		return fmt.Errorf("care: %w", err)
	}
	if careResult.GetOutcome() != publicpb.CareOutcome_CARE_OUTCOME_RESOLVED ||
		careResult.GetResolvedByPlayerId() != friendSess.playerID ||
		careResult.GetCareRewardOrderId() == "" {
		return fmt.Errorf("care result mismatch: %+v", careResult)
	}
	if careResult.GetCommittedPlotVersion() != 0 {
		planted.plotVersion = careResult.GetCommittedPlotVersion()
	}
	if careResult.GetCommittedFarmVersion() != 0 {
		planted.farmVersion = careResult.GetCommittedFarmVersion()
	}
	owner.absorbPlotUpdates(planted, 2*time.Second)
	if err := e.waitSourceOrderCompleted(actorrouting.DomainFarm, string(identity.WorkTypeCareReward),
		careResult.GetCareRewardOrderId(), owner.playerID); err != nil {
		return fmt.Errorf("care reward order: %w", err)
	}
	if err := friendSess.bootstrap(); err != nil {
		return fmt.Errorf("friend exp refresh: %w", err)
	}
	if friendSess.exp <= expBefore {
		return fmt.Errorf("care exp did not increase before=%d after=%d", expBefore, friendSess.exp)
	}
	log.Printf("SOURCE_ORDER_WSS CARE RESOLVED friend=%d order=%s exp %d->%d",
		friendSess.playerID, careResult.GetCareRewardOrderId(), expBefore, friendSess.exp)

	if err := friendSess.waitUntil(planted.matureAtMs+300, 12*time.Second); err != nil {
		return fmt.Errorf("wait theft mature: %w", err)
	}
	theftOp := "so-wss-theft-" + e.runID
	if err := friendSess.theft(theftOp, owner.playerID, *planted); err != nil {
		return fmt.Errorf("theft: %w", err)
	}
	visitorOrder, err := sourceorder.TheftVisitorSourceOrderID(owner.playerID, theftOp)
	if err != nil {
		return fmt.Errorf("theft visitor order id: %w", err)
	}
	if err := e.waitSourceOrderCompleted(actorrouting.DomainFarm, string(identity.WorkTypeTheftVisitor),
		visitorOrder, owner.playerID); err != nil {
		return fmt.Errorf("theft visitor order: %w", err)
	}
	if err := e.waitDueEmpty(actorrouting.DomainFarm, string(identity.WorkTypeTheftVisitor), owner.playerID); err != nil {
		return fmt.Errorf("theft visitor due: %w", err)
	}
	produceBefore := friendSess.produceCount()
	if err := friendSess.waitProduceIncreased(produceBefore); err != nil {
		return fmt.Errorf("theft produce: %w", err)
	}
	owner.absorbPlotUpdates(planted, 3*time.Second)
	log.Printf("SOURCE_ORDER_WSS THEFT visitor order COMPLETED friend=%d farm=%d plot_version=%d",
		friendSess.playerID, owner.playerID, planted.plotVersion)
	return nil
}

func (e *e2e) activateWSSFriendship(ownerID, friendID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace, err := rediskey.NewNamespace(e.namespace, e2eZone)
	if err != nil {
		return fmt.Errorf("friend namespace: %w", err)
	}
	repos, err := friend.NewRedisFriendshipRepositoriesForNamespace(e.redis, namespace)
	if err != nil {
		return fmt.Errorf("friend repos: %w", err)
	}
	if err := repos.EnsureScriptsLoaded(ctx); err != nil {
		return fmt.Errorf("friend scripts: %w", err)
	}
	low, high := friend.CanonicalPair(ownerID, friendID)
	pairID, err := friend.RelationIDFor(low, high)
	if err != nil {
		return fmt.Errorf("friend pair id: %w", err)
	}
	payloadHash := sha256.Sum256([]byte("wss-e2e-activate-" + e.runID))
	outcome, err := repos.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
		InviterPlayerID: ownerID, RedeemerPlayerID: friendID,
		TargetRelationEpoch: 1, TokenID: "wss-e2e-token-" + e.runID,
		TokenGeneration: 1, OperationID: "wss-e2e-activate-" + e.runID,
		NowMs: time.Now().UnixMilli(),
	}, friend.CommitIdentity{
		WorkAddressID: pairID, ParticipantInvocationID: "wss-e2e-activate-" + e.runID,
		ParticipantStep: "ACTIVATE",
	}, payloadHash[:])
	if err != nil {
		return fmt.Errorf("ActivateFriendRelation: %w", err)
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Reason != friend.ReasonAlreadyFriends {
		return fmt.Errorf("ActivateFriendRelation status=%v reason=%v", outcome.Status, outcome.Reason)
	}
	log.Printf("SOURCE_ORDER_WSS friendship activated pair=%s owner=%d friend=%d", pairID, ownerID, friendID)
	return nil
}

func (e *e2e) sourceOrderProbe(domain actorrouting.RouteDomain, workType, orderID string, actorID uint64) string {
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, actorrouting.ActorAddress{
		Domain: domain, ActorID: actorID,
	})
	if err != nil {
		return "bucket=" + err.Error()
	}
	snapshot, getErr := e.authority.WorkLease().GetSourceOrder(context.Background(), bucket, workType, orderID)
	due, dueErr := e.authority.WorkLease().ListDueWork(context.Background(), bucket, 32)
	dueIDs := make([]string, 0, len(due))
	if dueErr == nil {
		for _, ref := range due {
			dueIDs = append(dueIDs, ref.Ref.WorkType+":"+ref.Ref.WorkID)
		}
	}
	if getErr != nil {
		return fmt.Sprintf("order_get=%v due=%v due_err=%v", getErr, dueIDs, dueErr)
	}
	return fmt.Sprintf("order_state=%s current_step=%s last_error=%s due=%v",
		snapshot.State, snapshot.CurrentStep, snapshot.LastErrorClass, dueIDs)
}

func mustPlantSourceOrderID(playerID uint64, operationID string) string {
	orderID, err := sourceorder.PlantSourceOrderID(playerID, operationID)
	if err != nil {
		log.Fatalf("plant source order id: %v", err)
	}
	return orderID
}

func mustHarvestSourceOrderID(farmID uint64, operationID string) string {
	orderID, err := farmactor.HarvestSourceOrderID(farmID, operationID)
	if err != nil {
		log.Fatalf("harvest source order id: %v", err)
	}
	return orderID
}

func (e *e2e) waitSourceOrderCompleted(domain actorrouting.RouteDomain, workType, orderID string, actorID uint64) error {
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, actorrouting.ActorAddress{
		Domain: domain, ActorID: actorID,
	})
	if err != nil {
		return fmt.Errorf("authority bucket: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		snapshot, getErr := e.authority.WorkLease().GetSourceOrder(context.Background(), bucket, workType, orderID)
		if getErr == nil {
			lastState = snapshot.State
			if snapshot.State == "COMPLETED" {
				return nil
			}
		} else if !errors.Is(getErr, redisauthority.ErrSourceOrderNotFound) {
			lastState = getErr.Error()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s %s did not COMPLETE within 30s last_state=%q", workType, orderID, lastState)
}

func (e *e2e) waitDueEmpty(domain actorrouting.RouteDomain, workType string, actorID uint64) error {
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, actorrouting.ActorAddress{
		Domain: domain, ActorID: actorID,
	})
	if err != nil {
		return fmt.Errorf("authority bucket: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var last []string
	for time.Now().Before(deadline) {
		refs, listErr := e.authority.WorkLease().ListDueWork(context.Background(), bucket, 32)
		if listErr != nil {
			return fmt.Errorf("list due: %w", listErr)
		}
		last = last[:0]
		for _, ref := range refs {
			if ref.Ref.WorkType == workType {
				last = append(last, ref.Ref.WorkType+":"+ref.Ref.WorkID)
			}
		}
		if len(last) == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s due still occupied: %v", workType, last)
}

func (s *wssSession) close() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *wssSession) accountBody() ([]byte, error) {
	return json.Marshal(map[string]string{
		"username":     s.user,
		"password":     s.password,
		"bootstrap_id": "so-wss-" + s.user,
		"device_id":    "so-wss-" + s.user,
		"client_build": "po1-source-order-wss/1",
	})
}

func (s *wssSession) register(ctx context.Context) error {
	payload, err := s.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.httpBase+"/api/v1/accounts/register", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("register status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s *wssSession) login(ctx context.Context) error {
	payload, err := s.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.httpBase+"/api/v1/session/login", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("login status %d: %s", resp.StatusCode, body)
	}
	var result struct {
		PlayerID         string `json:"player_id"`
		ConnectionTicket string `json:"connection_ticket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}
	if result.ConnectionTicket == "" || result.PlayerID == "" {
		return errors.New("login returned empty ticket/player")
	}
	playerID, err := strconv.ParseUint(result.PlayerID, 10, 64)
	if err != nil {
		return fmt.Errorf("login player_id %q: %w", result.PlayerID, err)
	}
	s.playerID = playerID
	s.ticket = result.ConnectionTicket
	return nil
}

func (s *wssSession) connect(ctx context.Context) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, s.wsURL, nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return fmt.Errorf("dial %s: %w", s.wsURL, err)
	}
	s.conn = conn
	s.frames = make(chan wssFrame, 64)
	go s.pump()
	if err := gateclient.WriteFrame(s.conn, 1, gateclient.FrameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: s.ticket,
		ClientBuild:      "po1-source-order-wss/1",
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	s.sequence = 1
	kind, body, err := s.nextFrame(15 * time.Second)
	if err != nil {
		return fmt.Errorf("read server hello: %w", err)
	}
	if kind != gateclient.FrameKindServerHello {
		return fmt.Errorf("expected ServerHello, got kind=%d", kind)
	}
	var hello commonpb.ServerHello
	if err := proto.Unmarshal(body, &hello); err != nil {
		return fmt.Errorf("decode server hello: %w", err)
	}
	s.serverTimeAtSync = hello.GetServerTimeMs()
	s.monotonicAtSync = time.Now()
	return nil
}

func (s *wssSession) pump() {
	defer close(s.frames)
	for {
		kind, body, err := gateclient.ReadFrame(s.conn)
		if err != nil {
			s.frames <- wssFrame{err: err}
			return
		}
		s.frames <- wssFrame{kind: kind, body: body}
	}
}

func (s *wssSession) nextFrame(timeout time.Duration) (gateclient.FrameKind, []byte, error) {
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-s.frames:
		if !ok {
			return 0, nil, errors.New("connection closed")
		}
		if msg.err != nil {
			return 0, nil, msg.err
		}
		return msg.kind, msg.body, nil
	case <-timer.C:
		return 0, nil, errWSSFrameTimeout
	}
}

func (s *wssSession) sendCommand(operationID, requestID string, commandType commonpb.CommandType, payload []byte) error {
	s.sequence++
	return gateclient.WriteFrame(s.conn, s.sequence, gateclient.FrameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID,
		RequestId:   requestID,
		CommandType: commandType,
		Payload:     payload,
	})
}

func (s *wssSession) sendRecoveryComplete() error {
	s.sequence++
	return gateclient.WriteFrame(s.conn, s.sequence, gateclient.FrameKindControl, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{
			ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{},
		},
	})
}

func (s *wssSession) joinWithRetry(opPrefix string, targetOwner uint64) (*farmpb.FarmSnapshot, error) {
	var snapshot *farmpb.FarmSnapshot
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		snapshot, err = s.joinBarrier(opPrefix+"-"+strconv.Itoa(attempt), targetOwner)
		if err == nil {
			return snapshot, nil
		}
		if !strings.Contains(err.Error(), "UNAVAILABLE") && !strings.Contains(err.Error(), "ERR_STORE_UNAVAILABLE") {
			return nil, err
		}
		log.Printf("SOURCE_ORDER_WSS join retry %d target=%d: %v", attempt+1, targetOwner, err)
		time.Sleep(500 * time.Millisecond)
	}
	return nil, err
}

func (s *wssSession) absorbPlotUpdates(planted *wssPlanted, timeout time.Duration) {
	if planted == nil {
		return
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			return
		}
		if kind != gateclient.FrameKindServerPush {
			continue
		}
		change, ok, parseErr := parseFarmChange(body)
		if parseErr != nil || !ok || change == nil {
			continue
		}
		if change.GetFarmVersion() != 0 {
			planted.farmVersion = change.GetFarmVersion()
		}
		for _, plot := range change.GetChangedPlots() {
			if plot.GetPlotVersion() == 0 {
				continue
			}
			if plot.GetPlotIndex() == planted.plotID || plot.GetCropCycleId() == planted.cropCycleID {
				planted.plotVersion = plot.GetPlotVersion()
			}
		}
	}
}

func (s *wssSession) joinBarrier(operationID string, targetOwner uint64) (*farmpb.FarmSnapshot, error) {
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		return nil, err
	}
	if err := s.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_JOIN_FARM, joinPayload); err != nil {
		return nil, err
	}
	var snapshot *farmpb.FarmSnapshot
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for !ready {
		if time.Now().After(deadline) {
			return nil, errors.New("join barrier timeout")
		}
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errWSSFrameTimeout) {
				continue
			}
			return nil, fmt.Errorf("join read: %w", err)
		}
		switch kind {
		case gateclient.FrameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return nil, fmt.Errorf("join rejected: status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
		case gateclient.FrameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				return nil, err
			}
			switch push.GetPushType() {
			case commonpb.PushType_FARM_SNAPSHOT:
				var snapPush farmpb.FarmSnapshotPush
				if err := proto.Unmarshal(push.GetPayload(), &snapPush); err != nil {
					return nil, fmt.Errorf("decode snapshot push: %w", err)
				}
				snapshot = snapPush.GetSnapshot()
				if snapshot == nil || snapshot.GetOwnerId() != targetOwner {
					return nil, errors.New("join snapshot invalid")
				}
			case commonpb.PushType_SUBSCRIPTION_READY:
				if snapshot == nil {
					return nil, errors.New("SUBSCRIPTION_READY before snapshot")
				}
				ready = true
			}
		case gateclient.FrameKindConnectionError:
			return nil, errors.New("server ConnectionError during join")
		}
	}
	return snapshot, nil
}

func (s *wssSession) bootstrap() error {
	op := fmt.Sprintf("so-wss-bootstrap-%d", time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.GetPlayerBootstrapRequest{})
	if err != nil {
		return err
	}
	if err := s.sendCommand(op, "request-"+op, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, raw); err != nil {
		return err
	}
	payload, err := s.waitTerminal(op, 20*time.Second)
	if err != nil {
		return err
	}
	var boot publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(payload, &boot); err != nil {
		return fmt.Errorf("decode bootstrap: %w", err)
	}
	if boot.GetWallet() == nil || boot.GetInventory() == nil {
		return errors.New("bootstrap missing wallet/inventory")
	}
	s.applyBootstrap(&boot)
	return nil
}

func (s *wssSession) applyBootstrap(boot *publicpb.GetPlayerBootstrapResult) {
	s.wallet = boot.GetWallet().GetCoin()
	if boot.GetProgression() != nil {
		s.exp = boot.GetProgression().GetExp()
	}
	s.inventory = make(map[int32]int64, len(boot.GetInventory().GetItems()))
	for _, item := range boot.GetInventory().GetItems() {
		s.inventory[item.GetItemId()] = item.GetCount()
	}
}

func (s *wssSession) buy() error {
	op := fmt.Sprintf("so-wss-buy-%d", time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{
		OfferId: cropcatalog.CarrotOfferID, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return err
	}
	if err := s.sendCommand(op, "request-"+op, commonpb.CommandType_BUY, raw); err != nil {
		return err
	}
	payload, err := s.waitTerminal(op, 20*time.Second)
	if err != nil {
		return err
	}
	var buyResult publicpb.BuyResult
	if err := proto.Unmarshal(payload, &buyResult); err != nil {
		return fmt.Errorf("decode buy: %w", err)
	}
	s.seedItemID = buyResult.GetItemId()
	if ps := buyResult.GetPrivateState(); ps != nil {
		s.wallet = ps.GetCoin()
	}
	if s.seedItemID == 0 {
		s.seedItemID = cropcatalog.CarrotSeedItemID
	}
	return nil
}

func (s *wssSession) plant(operationID string, snapshot *farmpb.FarmSnapshot, skipPlot ...uint32) (wssPlanted, error) {
	skip := map[uint32]struct{}{}
	for _, plotID := range skipPlot {
		skip[plotID] = struct{}{}
	}
	var plotID uint32
	var plotVersion uint64
	found := false
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || plot.GetCropCycleId() != "" {
			continue
		}
		if _, skipped := skip[plot.GetPlotIndex()]; skipped {
			continue
		}
		plotID = plot.GetPlotIndex()
		plotVersion = plot.GetPlotVersion()
		found = true
		break
	}
	if !found {
		return wssPlanted{}, fmt.Errorf("no empty plot in snapshot plots=%d", len(snapshot.GetPlots()))
	}
	log.Printf("SOURCE_ORDER_WSS planting plot=%d plot_version=%d farm=%d", plotID, plotVersion, s.playerID)
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: plotID, CropDefinitionId: uint32(cropcatalog.CarrotDefinitionID), TargetFarmId: s.playerID,
		ExpectedPlotVersion: plotVersion,
	})
	if err != nil {
		return wssPlanted{}, err
	}
	if err := s.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_PLANT, raw); err != nil {
		return wssPlanted{}, err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return wssPlanted{}, errors.New("plant timeout waiting plot change")
		}
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errWSSFrameTimeout) {
				continue
			}
			return wssPlanted{}, err
		}
		switch kind {
		case gateclient.FrameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return wssPlanted{}, err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				log.Printf("SOURCE_ORDER_WSS plant PROCESSING plot=%d", plotID)
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return wssPlanted{}, fmt.Errorf("plant status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			log.Printf("SOURCE_ORDER_WSS plant SUCCEEDED plot=%d, waiting FARM_CHANGE", plotID)
		case gateclient.FrameKindServerPush:
			outcome, done, err := observePlantChange(body, plotID)
			if err != nil {
				return wssPlanted{}, err
			}
			if done {
				return outcome, nil
			}
		case gateclient.FrameKindConnectionError:
			return wssPlanted{}, errors.New("connection error during plant")
		}
	}
}

func observePlantChange(body []byte, plotID uint32) (wssPlanted, bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return wssPlanted{}, false, err
	}
	if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return wssPlanted{}, false, nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return wssPlanted{}, false, err
	}
	change := changePush.GetChange()
	outcome := wssPlanted{farmVersion: change.GetFarmVersion()}
	for _, plot := range change.GetChangedPlots() {
		if plot.GetPlotIndex() != plotID {
			continue
		}
		outcome.plotID = plotID
		outcome.cropCycleID = plot.GetCropCycleId()
		outcome.matureAtMs = plot.GetMatureAtMs()
		outcome.plotVersion = plot.GetPlotVersion()
	}
	if outcome.cropCycleID != "" && outcome.matureAtMs > 0 {
		return outcome, true, nil
	}
	return outcome, false, nil
}

func (s *wssSession) waitGrow(planted *wssPlanted) error {
	remaining := planted.matureAtMs - s.trustedNow()
	delay := time.Duration(remaining)*time.Millisecond + 500*time.Millisecond
	if delay < 500*time.Millisecond {
		delay = 500 * time.Millisecond
	}
	s.absorbPlotUpdates(planted, delay)
	return nil
}

func plotVersionFromConflict(err error) (uint64, bool) {
	if err == nil {
		return 0, false
	}
	msg := err.Error()
	idx := strings.Index(msg, "version ")
	if idx < 0 || !strings.Contains(msg, "ERR_VERSION_CONFLICT") {
		return 0, false
	}
	var current, expected uint64
	if _, scanErr := fmt.Sscanf(msg[idx:], "version %d, expected %d", &current, &expected); scanErr != nil {
		return 0, false
	}
	return current, current > 0
}

func (s *wssSession) trustedNow() int64 {
	return s.serverTimeAtSync + time.Since(s.monotonicAtSync).Milliseconds()
}

func (s *wssSession) harvest(operationID string, planted wssPlanted) error {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.HarvestRequest{
		PlotId: planted.plotID, TargetFarmId: s.playerID, CropCycleId: planted.cropCycleID,
		ExpectedPlotVersion: planted.plotVersion,
	})
	if err != nil {
		return err
	}
	if err := s.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_HARVEST, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("harvest timeout waiting version advance")
		}
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errWSSFrameTimeout) {
				continue
			}
			return err
		}
		switch kind {
		case gateclient.FrameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return fmt.Errorf("harvest status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			return nil
		case gateclient.FrameKindServerPush:
		case gateclient.FrameKindConnectionError:
			return errors.New("connection error during harvest")
		}
	}
}

func parseFarmChange(body []byte) (*farmpb.FarmChange, bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return nil, false, err
	}
	if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return nil, false, nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return nil, false, err
	}
	return changePush.GetChange(), true, nil
}

func (s *wssSession) produceCount() int64 {
	var total int64
	for itemID, count := range s.inventory {
		if itemID != s.seedItemID {
			total += count
		}
	}
	return total
}

func (s *wssSession) waitProduceIncreased(before int64) error {
	var last string
	for attempt := 0; attempt < 10; attempt++ {
		if err := s.bootstrap(); err != nil {
			return fmt.Errorf("bootstrap refresh: %w", err)
		}
		if s.produceCount() > before {
			return nil
		}
		parts := make([]string, 0, len(s.inventory))
		for itemID, count := range s.inventory {
			parts = append(parts, fmt.Sprintf("%d:%d", itemID, count))
		}
		last = strings.Join(parts, ",")
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("produce did not increase before=%d inventory=%s", before, last)
}

func (s *wssSession) waitProduceLanded(walletBefore int64) error {
	var lastInventory string
	for attempt := 0; attempt < 10; attempt++ {
		if err := s.bootstrap(); err != nil {
			return fmt.Errorf("bootstrap refresh: %w", err)
		}
		if s.wallet != walletBefore {
			return fmt.Errorf("coin drifted without sell: %d -> %d", walletBefore, s.wallet)
		}
		landed := false
		parts := make([]string, 0, len(s.inventory))
		for itemID, count := range s.inventory {
			parts = append(parts, fmt.Sprintf("%d:%d", itemID, count))
			if itemID != s.seedItemID && count > 0 {
				landed = true
			}
		}
		lastInventory = strings.Join(parts, ",")
		if landed {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("harvest produce not landed inventory=%s seed_item=%d", lastInventory, s.seedItemID)
}

func (s *wssSession) waitUntil(targetMs int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if s.trustedNow() >= targetMs {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("wait until %d timeout trusted_now=%d", targetMs, s.trustedNow())
		}
		left := time.Until(deadline)
		if left > 200*time.Millisecond {
			left = 200 * time.Millisecond
		}
		kind, _, err := s.nextFrame(left)
		if err != nil && !errors.Is(err, errWSSFrameTimeout) {
			return err
		}
		if kind == gateclient.FrameKindConnectionError {
			return errors.New("connection error while waiting")
		}
	}
}

func (s *wssSession) care(operationID string, farmID uint64, planted wssPlanted, incidentID string) (*publicpb.CareResult, error) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.CareRequest{
		PlotId: planted.plotID, TargetFarmId: farmID, CropCycleId: planted.cropCycleID,
		IncidentId: incidentID, CareType: publicpb.CareType_CARE_TYPE_WATER,
	})
	if err != nil {
		return nil, err
	}
	if err := s.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_CARE, raw); err != nil {
		return nil, err
	}
	payload, err := s.waitTerminal(operationID, 20*time.Second)
	if err != nil {
		return nil, err
	}
	result := &publicpb.CareResult{}
	if err := proto.Unmarshal(payload, result); err != nil {
		return nil, fmt.Errorf("decode care: %w", err)
	}
	return result, nil
}

func (s *wssSession) theft(operationID string, farmID uint64, planted wssPlanted) error {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.TheftRequest{
		PlotId: planted.plotID, TargetFarmId: farmID, CropCycleId: planted.cropCycleID,
	})
	if err != nil {
		return err
	}
	if err := s.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_THEFT, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("theft timeout waiting admission")
		}
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errWSSFrameTimeout) {
				continue
			}
			return err
		}
		if kind != gateclient.FrameKindCommandResult {
			if kind == gateclient.FrameKindConnectionError {
				return errors.New("connection error during theft")
			}
			continue
		}
		var result commonpb.CommandResultEnvelope
		if err := proto.Unmarshal(body, &result); err != nil {
			return err
		}
		if result.GetOperationId() != operationID {
			continue
		}
		switch result.GetStatus() {
		case commonpb.CommandStatus_PROCESSING, commonpb.CommandStatus_SUCCEEDED:
			return nil
		default:
			return fmt.Errorf("theft status=%s code=%s msg=%s",
				result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
		}
	}
}

func (s *wssSession) waitTerminal(operationID string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting terminal for %s", operationID)
		}
		kind, body, err := s.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errWSSFrameTimeout) {
				continue
			}
			return nil, err
		}
		switch kind {
		case gateclient.FrameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return nil, fmt.Errorf("status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			return result.GetPayload(), nil
		case gateclient.FrameKindConnectionError:
			return nil, errors.New("server ConnectionError")
		}
	}
}

func waitTCP(address string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("tcp %s did not accept connections", address)
}
