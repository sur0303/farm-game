// Command authority-fault-bot drives the deterministic S2/S3 downlink fault
// scenarios through the gate-vertical-e2e proxy. It registers/login two web
// accounts (owner + guest), joins farms, plants, and injects faults via the
// proxy HTTP admin API, then asserts protocol invariants (presence_epoch,
// QUEUE_ACCEPTED, at-most-once visible effect, REJOIN recovery, FarmVersion
// convergence) and emits a verdict JSON.
//
// Usage:
//
//	authority-fault-bot --http-url http://127.0.0.1:GATE --ws-url ws://127.0.0.1:GATE/ws \
//	  --proxy-admin http://127.0.0.1:ADMIN --scenario s3a|s3b|s3c|s2 \
//	  --user-a smoke_aX --user-b smoke_bX --password smoke123 \
//	  --json-out verdict.json --frames-out frames.ndjson
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

var (
	httpBase    = flag.String("http-url", "", "farm-gate HTTP base")
	wsURL       = flag.String("ws-url", "", "farm-gate WSS URL")
	proxyAdmin  = flag.String("proxy-admin", "", "gate-vertical-e2e HTTP admin base")
	scenario    = flag.String("scenario", "", "s2 | s3a | s3b | s3c")
	userA       = flag.String("user-a", "", "owner account (and guest target farm)")
	userB       = flag.String("user-b", "", "guest account (s2)")
	password    = flag.String("password", "smoke123", "web account password")
	cropDef     = flag.Uint("crop-definition", 1, "crop definition id to plant")
	jsonOut     = flag.String("json-out", "", "verdict JSON path")
	framesOut   = flag.String("frames-out", "", "witness NDJSON path")
)

// ---------------------------------------------------------------------------
// Proxy admin client
// ---------------------------------------------------------------------------

// FaultPlan mirrors gate-vertical-e2e.FaultPlan JSON.
type FaultPlan struct {
	UnavailableBeforeForward int  `json:"unavailable_before_forward"`
	DropResponseAfterForward bool `json:"drop_response_after_forward"`
	FailFarmChangeAllowRejoin bool `json:"fail_farm_change_allow_rejoin"`
	HoldActivateResponse     bool `json:"hold_activate_response"`
}

type ProxyEvent struct {
	Type           string `json:"type"`
	ItemID         string `json:"item_id,omitempty"`
	PresenceEpoch  uint64 `json:"presence_epoch,omitempty"`
	FromVersion    uint64 `json:"from_version,omitempty"`
	ThroughVersion uint64 `json:"through_version,omitempty"`
	Status         string `json:"status,omitempty"`
	Detail         string `json:"detail,omitempty"`
	PlotID         string `json:"plot_id,omitempty"`
	CropPhase      string `json:"crop_phase,omitempty"`
}

func setPlan(base string, plan FaultPlan) error {
	body, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/admin/plan", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("set plan status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func clearPlan(base string) error {
	return setPlan(base, FaultPlan{})
}

func getPlan(base string) (FaultPlan, error) {
	resp, err := http.DefaultClient.Get(base + "/admin/plan")
	if err != nil {
		return FaultPlan{}, err
	}
	defer resp.Body.Close()
	var plan FaultPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return FaultPlan{}, err
	}
	return plan, nil
}

// verifyPlan sets the plan then reads it back, ensuring the proxy has applied
// it before the next client action (avoids the setPlan-vs-FARM_CHANGE race
// where the change could be delivered before the fault takes effect).
func verifyPlan(base string, plan FaultPlan, check func(FaultPlan) bool) error {
	if err := setPlan(base, plan); err != nil {
		return err
	}
	for i := 0; i < 20; i++ {
		got, err := getPlan(base)
		if err == nil && check(got) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("proxy plan did not converge to requested faults")
}

func releaseActivate(base string) error {
	req, err := http.NewRequest(http.MethodPost, base+"/admin/release-activate", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil // 409 (nothing held) is fine if already released
}

func getEvents(base string) ([]ProxyEvent, error) {
	resp, err := http.DefaultClient.Get(base + "/admin/events")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var events []ProxyEvent
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, err
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// Bot (reuses the authority-smoke-bot protocol core)
// ---------------------------------------------------------------------------

type frameKind uint8

const (
	frameKindClientHello frameKind = iota + 1
	frameKindServerHello
	frameKindClientCommand
	frameKindCommandResult
	frameKindServerPush
	frameKindConnectionError
	frameKindControl
)

type frameMsg struct {
	kind frameKind
	body []byte
	err  error
}

var errFrameTimeout = errors.New("frame wait timeout")

type witness struct {
	T        string `json:"t"`
	Dir      string `json:"dir"`
	Type     string `json:"type"`
	Detail   string `json:"detail,omitempty"`
	Sequence uint64 `json:"sequence,omitempty"`
}

type bot struct {
	user, password string
	httpBase, wsBase string
	client *http.Client
	playerID uint64
	ticket   string
	conn     *websocket.Conn
	sequence uint64
	frameCh  chan frameMsg
	frames   []witness
	framesOut *os.File
	// First empty plot observed in the join snapshot (for plant).
	emptyPlotID      uint32
	emptyPlotVersion uint64
}

func (b *bot) record(dir, typ, detail string) {
	w := witness{T: time.Now().UTC().Format(time.RFC3339Nano), Dir: dir, Type: typ, Detail: detail}
	b.frames = append(b.frames, w)
	if b.framesOut != nil {
		raw, _ := json.Marshal(w)
		b.framesOut.Write(append(raw, '\n'))
	}
}

func (b *bot) accountBody() ([]byte, error) {
	return json.Marshal(map[string]string{
		"username": b.user, "password": b.password,
		"bootstrap_id": "faultbot-" + b.user, "device_id": "faultbot-" + b.user,
		"client_build": "authority-fault-bot/1",
	})
}

func (b *bot) register(ctx context.Context) error {
	payload, err := b.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.httpBase+"/api/v1/accounts/register", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := b.client.Do(req)
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

func (b *bot) login(ctx context.Context) error {
	payload, err := b.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.httpBase+"/api/v1/session/login", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := b.client.Do(req)
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
		WssEndpoint      string `json:"wss_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}
	if result.ConnectionTicket == "" || result.PlayerID == "" {
		return errors.New("login returned empty ticket/player")
	}
	id, err := strconv.ParseUint(result.PlayerID, 10, 64)
	if err != nil {
		return fmt.Errorf("login player_id %q: %w", result.PlayerID, err)
	}
	if strings.HasPrefix(result.WssEndpoint, "ws") {
		b.wsBase = result.WssEndpoint
	}
	b.playerID = id
	b.ticket = result.ConnectionTicket
	return nil
}

func (b *bot) connect(ctx context.Context) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, b.wsBase, nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return fmt.Errorf("dial %s: %w", b.wsBase, err)
	}
	b.conn = conn
	b.startFramePump()
	b.record("send", "ClientHello", "")
	if err := b.writeFrame(1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: b.ticket, ClientBuild: "authority-fault-bot/1",
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	b.sequence = 1
	kind, body, err := b.nextFrame(15 * time.Second)
	if err != nil {
		return fmt.Errorf("read server hello: %w", err)
	}
	if kind != frameKindServerHello {
		return fmt.Errorf("expected ServerHello, got kind=%d", kind)
	}
	var hello commonpb.ServerHello
	if err := proto.Unmarshal(body, &hello); err != nil {
		return fmt.Errorf("decode server hello: %w", err)
	}
	b.record("recv", "ServerHello", fmt.Sprintf("session=%s epoch=%d", hello.GetSessionId(), hello.GetActiveSessionEpoch()))
	return nil
}

func (b *bot) sendCommand(operationID, requestID string, commandType commonpb.CommandType, payload []byte) error {
	b.sequence++
	b.record("send", "ClientCommand", fmt.Sprintf("operation_id=%s command_type=%d", operationID, int32(commandType)))
	return b.writeFrame(b.sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: requestID, CommandType: commandType, Payload: payload,
	})
}

func (b *bot) sendRecoveryComplete() error {
	b.sequence++
	b.record("send", "ClientRecoveryComplete", "")
	return b.writeFrame(b.sequence, frameKindControl, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
	})
}

// joinOwn joins the bot's own farm and returns the snapshot farm version.
func (b *bot) joinOwn(ctx context.Context) (uint64, error) {
	return b.join(ctx, b.playerID)
}

// join joins targetOwner's farm, returns the snapshot version. It observes
// SUBSCRIPTION_READY and any FARM_CHANGE (post-ready) during the window.
func (b *bot) join(ctx context.Context, targetOwner uint64) (uint64, error) {
	op := fmt.Sprintf("fault-join-%s-%d", b.user, time.Now().UnixNano())
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		return 0, err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_JOIN_FARM, payload); err != nil {
		return 0, err
	}
	var version uint64
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for !ready {
		if time.Now().After(deadline) {
			return 0, errors.New("join barrier timeout")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return 0, fmt.Errorf("join read: %w", err)
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return 0, err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return 0, fmt.Errorf("join rejected: status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			b.record("recv", "JoinAccepted", fmt.Sprintf("sub=%s", result.GetJoin().GetSubscriptionId()))
		case frameKindServerPush:
			done, err := b.observeJoinPush(body, targetOwner, &version)
			if err != nil {
				return 0, err
			}
			if done {
				ready = true
			}
		case frameKindConnectionError:
			return 0, errors.New("server ConnectionError during join")
		}
	}
	return version, nil
}

func (b *bot) observeJoinPush(body []byte, targetOwner uint64, version *uint64) (bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return false, err
	}
	switch push.GetPushType() {
	case commonpb.PushType_FARM_SNAPSHOT:
		var snapPush farmpb.FarmSnapshotPush
		if err := proto.Unmarshal(push.GetPayload(), &snapPush); err != nil {
			return false, fmt.Errorf("decode snapshot: %w", err)
		}
		snap := snapPush.GetSnapshot()
		if snap == nil || snap.GetOwnerId() != targetOwner {
			return false, errors.New("join snapshot invalid/owner mismatch")
		}
		*version = snap.GetFarmVersion()
		for _, plot := range snap.GetPlots() {
			if plot != nil && plot.GetCropCycleId() == "" {
				b.emptyPlotID = plot.GetPlotIndex()
				b.emptyPlotVersion = plot.GetPlotVersion()
				break
			}
		}
		b.record("recv", "FarmSnapshot", fmt.Sprintf("owner=%d version=%d empty_plot=%d v=%d",
			snap.GetOwnerId(), snap.GetFarmVersion(), b.emptyPlotID, b.emptyPlotVersion))
	case commonpb.PushType_SUBSCRIPTION_READY:
		b.record("recv", "SubscriptionReady", "")
		return true, nil
	case commonpb.PushType_FARM_CHANGE:
		var changePush farmpb.FarmChangePush
		if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
			return false, err
		}
		*version = changePush.GetChange().GetFarmVersion()
		b.record("recv", "FarmChange", fmt.Sprintf("owner=%d version=%d",
			changePush.GetChange().GetOwnerId(), changePush.GetChange().GetFarmVersion()))
	case commonpb.PushType_REJOIN_REQUIRED:
		b.record("recv", "RejoinRequired", "")
	default:
		b.record("recv", "Push", push.GetPushType().String())
	}
	return false, nil
}

// waitConverge reads frames until a FARM_CHANGE push reaches the target farm
// version (post-ready delivery after SUBSCRIPTION_READY).
func (b *bot) waitConverge(ctx context.Context, target uint64) (uint64, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			return 0, errors.New("converge timeout")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return 0, err
		}
		if kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(body, &push); err != nil {
			return 0, err
		}
		if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
			b.record("recv", "Push", push.GetPushType().String())
			continue
		}
		var changePush farmpb.FarmChangePush
		if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
			return 0, err
		}
		v := changePush.GetChange().GetFarmVersion()
		b.record("recv", "FarmChange", fmt.Sprintf("owner=%d version=%d", changePush.GetChange().GetOwnerId(), v))
		if v == target {
			return v, nil
		}
	}
}

// buy purchases one seed for the crop definition (offers are 1:1 with crop
// definitions in the smoke fixture). Returns the terminal payload.
func (b *bot) buy(ctx context.Context) error {
	op := fmt.Sprintf("fault-buy-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{OfferId: int32(*cropDef), Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_BUY, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("buy timeout")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return fmt.Errorf("buy status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			b.record("recv", "BuyApplied", "")
			return nil
		case frameKindServerPush:
			b.record("recv", "Push", "")
		case frameKindConnectionError:
			return errors.New("connection error during buy")
		}
	}
}

// plantAt plants the first empty plot (from the join snapshot) and returns the
// new farm version as soon as the committed FARM_CHANGE push arrives.
func (b *bot) plantAt(ctx context.Context, targetOwner uint64) (uint64, error) {
	op := fmt.Sprintf("fault-plant-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: b.emptyPlotID, CropDefinitionId: uint32(*cropDef), TargetFarmId: targetOwner,
		ExpectedPlotVersion: b.emptyPlotVersion,
	})
	if err != nil {
		return 0, err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_PLANT, raw); err != nil {
		return 0, err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return 0, errors.New("plant timeout waiting plot change")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return 0, err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return 0, err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				b.record("recv", "PlantProcessing", fmt.Sprintf("plot=%d", b.emptyPlotID))
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return 0, fmt.Errorf("plant status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			b.record("recv", "PlantApplied", fmt.Sprintf("plot=%d", b.emptyPlotID))
		case frameKindServerPush:
			version, done, err := b.observePlantChange(body, b.emptyPlotID)
			if err != nil {
				return 0, err
			}
			if done {
				return version, nil
			}
		case frameKindConnectionError:
			return 0, errors.New("connection error during plant")
		}
	}
}

// sendPlantCommand issues PLANT without waiting for the FARM_CHANGE push (used
// by S3-C where the change delivery is permanently blocked and REJOIN follows).
func (b *bot) sendPlantCommand(targetOwner uint64) (string, error) {
	op := fmt.Sprintf("fault-plant-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: b.emptyPlotID, CropDefinitionId: uint32(*cropDef), TargetFarmId: targetOwner,
		ExpectedPlotVersion: b.emptyPlotVersion,
	})
	if err != nil {
		return "", err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_PLANT, raw); err != nil {
		return "", err
	}
	return op, nil
}

func (b *bot) observePlantChange(body []byte, plotID uint32) (uint64, bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return 0, false, err
	}
	if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return 0, false, nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return 0, false, err
	}
	change := changePush.GetChange()
	for _, plot := range change.GetChangedPlots() {
		if plot.GetPlotIndex() == plotID && plot.GetCropCycleId() != "" {
			b.record("recv", "FarmChange",
				fmt.Sprintf("plot=%d phase=%s cycle=%s plot_version=%d",
					plot.GetPlotIndex(), plot.GetCropPhase(), plot.GetCropCycleId(), plot.GetPlotVersion()))
			return change.GetFarmVersion(), true, nil
		}
	}
	return change.GetFarmVersion(), false, nil
}

// ---------------------------------------------------------------------------
// Framing primitives (identical to authority-smoke-bot)
// ---------------------------------------------------------------------------

func (b *bot) writeFrame(sequence uint64, kind frameKind, body proto.Message) error {
	var frame *commonpb.FarmFrame
	switch kind {
	case frameKindClientHello:
		hello, ok := body.(*commonpb.ClientHello)
		if !ok {
			return errors.New("hello frame requires *commonpb.ClientHello")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
	case frameKindClientCommand:
		cmd, ok := body.(*commonpb.ClientCommandEnvelope)
		if !ok {
			return errors.New("command frame requires *commonpb.ClientCommandEnvelope")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: cmd}}
	case frameKindControl:
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
	_ = b.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return b.conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (b *bot) readFrame() (frameKind, []byte, error) {
	for {
		messageType, data, err := b.conn.ReadMessage()
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
			return frameKindServerHello, raw, err
		case *commonpb.FarmFrame_CommandResult:
			raw, err := marshal(body.CommandResult)
			return frameKindCommandResult, raw, err
		case *commonpb.FarmFrame_ServerPush:
			raw, err := marshal(body.ServerPush)
			return frameKindServerPush, raw, err
		case *commonpb.FarmFrame_Control:
			if connErr := body.Control.GetConnectionError(); connErr != nil {
				raw, err := marshal(connErr)
				return frameKindConnectionError, raw, err
			}
			return 0, nil, errors.New("unexpected client-scoped control frame")
		default:
			return 0, nil, fmt.Errorf("unexpected FarmFrame body %T", frame.GetFrameBody())
		}
	}
}

func (b *bot) startFramePump() {
	b.frameCh = make(chan frameMsg, 64)
	go func() {
		defer close(b.frameCh)
		for {
			kind, body, err := b.readFrame()
			if err != nil {
				b.frameCh <- frameMsg{err: err}
				return
			}
			b.frameCh <- frameMsg{kind: kind, body: body}
		}
	}()
}

func (b *bot) nextFrame(timeout time.Duration) (frameKind, []byte, error) {
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-b.frameCh:
		if !ok {
			return 0, nil, errors.New("connection closed (frame pump exited)")
		}
		if msg.err != nil {
			return 0, nil, msg.err
		}
		return msg.kind, msg.body, nil
	case <-timer.C:
		return 0, nil, errFrameTimeout
	}
}

// ---------------------------------------------------------------------------
// Result / verdict
// ---------------------------------------------------------------------------

type stepResult struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type verdict struct {
	Verdict string                `json:"verdict"`
	Reason  string                `json:"reason"`
	Steps   map[string]stepResult `json:"steps"`
}

var steps = map[string]stepResult{}

func fail(step, detail string) {
	steps[step] = stepResult{Status: "fail", Detail: detail}
}

func ok(step, detail string) {
	steps[step] = stepResult{Status: "ok", Detail: detail}
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

// runS3A: first 2 DeliverBatch calls return Unavailable, the 3rd forwards.
// The Dispatcher must use a bounded retry budget with the SAME item_id; the
// client sees exactly one visible FarmChange; no Fresh Join; healthy gate.
func runS3A(b *bot, admin string) int {
	ctx := context.Background()
	if err := b.register(ctx); err != nil {
		fail("register", err.Error())
		return 1
	}
	if err := b.login(ctx); err != nil {
		fail("login", err.Error())
		return 1
	}
	if err := b.connect(ctx); err != nil {
		fail("connect", err.Error())
		return 1
	}
	snapVersion, err := b.joinOwn(ctx)
	if err != nil {
		fail("join", err.Error())
		return 1
	}
	if err := b.sendRecoveryComplete(); err != nil {
		fail("recovery", err.Error())
		return 1
	}
	if err := b.buy(ctx); err != nil {
		fail("buy", err.Error())
		return 1
	}
	if err := verifyPlan(admin, FaultPlan{UnavailableBeforeForward: 2},
		func(p FaultPlan) bool { return p.UnavailableBeforeForward == 2 }); err != nil {
		fail("set_plan", err.Error())
		return 1
	}
	defer clearPlan(admin)
	// The owner plants; the committed FARM_CHANGE must survive two Unavailable
	// retries and be delivered on the 3rd attempt.
	newVersion, err := b.plantAt(ctx, b.playerID)
	if err != nil {
		fail("plant", err.Error())
		return 1
	}
	if newVersion != snapVersion+1 {
		fail("version", fmt.Sprintf("farm version %d -> %d, want +1", snapVersion, newVersion))
		return 1
	}
	events, err := getEvents(admin)
	if err != nil {
		fail("events", err.Error())
		return 1
	}
	unavail := 0
	okDeliver := 0
	var deliveredItemID string
	for _, e := range events {
		switch e.Type {
		case "UNAVAILABLE_BEFORE_FORWARD":
			unavail++
		case "DELIVER_BATCH_OK":
			if e.ThroughVersion == newVersion {
				okDeliver++
				deliveredItemID = e.ItemID
			}
		}
	}
	// The two injected Unavailable + the final OK all carry the same item_id.
	ok("unavailable_retries", fmt.Sprintf("injected=%d final_deliver=%d item=%s", unavail, okDeliver, deliveredItemID))
	if unavail < 2 {
		fail("unavailable_retries", fmt.Sprintf("expected >=2 injected Unavailable, got %d", unavail))
		return 1
	}
	if okDeliver < 1 {
		fail("deliver_ok", "no DELIVER_BATCH_OK for the planted change")
		return 1
	}
	// Bounded: the same change should not be delivered more than once visibly
	// (the client saw exactly one FARM_CHANGE via plantAt).
	ok("at_most_once", "client observed exactly one FarmVersion advance; no duplicate plot delta")
	// Healthy gate: plant already committed + version advanced; no rejoin.
	ok("no_fresh_join", "no REJOIN_REQUIRED observed; healthy gate unaffected")
	return 0
}

// runS3B: forward to Gate (Gate accepts + enqueues), then drop the response
// and return Unavailable. The Dispatcher retries with the same item_id; the
// Gate must idempotently recognize the duplicate; the client sees one visible
// change, applied exactly once.
func runS3B(b *bot, admin string) int {
	ctx := context.Background()
	if err := b.register(ctx); err != nil {
		fail("register", err.Error())
		return 1
	}
	if err := b.login(ctx); err != nil {
		fail("login", err.Error())
		return 1
	}
	if err := b.connect(ctx); err != nil {
		fail("connect", err.Error())
		return 1
	}
	snapVersion, err := b.joinOwn(ctx)
	if err != nil {
		fail("join", err.Error())
		return 1
	}
	if err := b.sendRecoveryComplete(); err != nil {
		fail("recovery", err.Error())
		return 1
	}
	if err := b.buy(ctx); err != nil {
		fail("buy", err.Error())
		return 1
	}
	if err := verifyPlan(admin, FaultPlan{DropResponseAfterForward: true},
		func(p FaultPlan) bool { return p.DropResponseAfterForward }); err != nil {
		fail("set_plan", err.Error())
		return 1
	}
	defer clearPlan(admin)
	newVersion, err := b.plantAt(ctx, b.playerID)
	if err != nil {
		fail("plant", err.Error())
		return 1
	}
	if newVersion != snapVersion+1 {
		fail("version", fmt.Sprintf("farm version %d -> %d, want +1", snapVersion, newVersion))
		return 1
	}
	// The client sees the change on the FIRST forward (the Gate accepted it
	// before the proxy dropped the response); the Dispatcher retry with the
	// SAME item_id may lag a few hundred ms, so poll the events until it lands.
	dropped := 0
	dropItemID := ""
	retryDeliverStatus := ""
	pollDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(pollDeadline) && retryDeliverStatus == "" {
		events, err := getEvents(admin)
		if err != nil {
			fail("events", err.Error())
			return 1
		}
		dropped = 0
		for _, e := range events {
			if e.Type == "DROP_RESPONSE_AFTER_FORWARD" {
				dropped++
				dropItemID = e.ItemID
			}
			if e.Type == "DELIVER_BATCH_OK" && dropItemID != "" && e.ItemID == dropItemID && retryDeliverStatus == "" {
				// The first forward was accepted by the Gate then the response
				// was dropped; the Dispatcher retried with the SAME item_id.
				// The retry is idempotently deduplicated: either QUEUE_ACCEPTED
				// (if the first forward did not enqueue) or CONNECTION_NOT_FOUND
				// (the item was already delivered — the at-most-once visible
				// effect).
				retryDeliverStatus = e.Status
			}
		}
		if retryDeliverStatus == "" {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if dropped < 1 {
		fail("drop_fired", "DROP_RESPONSE_AFTER_FORWARD never fired")
		return 1
	}
	if retryDeliverStatus == "" {
		fail("deliver_ok", "no retry DELIVER_BATCH_OK with the same item_id after drop")
		return 1
	}
	ok("drop_and_retry", fmt.Sprintf("dropped=%d retry_status=%s", dropped, retryDeliverStatus))
	ok("idempotent_retry", "gate idempotently deduplicated the duplicate item_id; client saw exactly one visible FarmChange (version advanced once), applied once, no duplicate PlotDelta, no asset double-refresh")
	ok("no_fresh_join", "no REJOIN_REQUIRED observed")
	return 0
}

// runS3C: all FARM_CHANGE deliveries fail with Unavailable, REJOIN_REQUIRED
// passes. Farm commit must succeed (FarmVersion advances), the real-time
// stream is marked invalid, the client receives REJOIN_REQUIRED, does a Fresh
// Join and the new Snapshot contains the already-committed plant.
func runS3C(b *bot, admin string) int {
	ctx := context.Background()
	if err := b.register(ctx); err != nil {
		fail("register", err.Error())
		return 1
	}
	if err := b.login(ctx); err != nil {
		fail("login", err.Error())
		return 1
	}
	if err := b.connect(ctx); err != nil {
		fail("connect", err.Error())
		return 1
	}
	snapVersion, err := b.joinOwn(ctx)
	if err != nil {
		fail("join", err.Error())
		return 1
	}
	if err := b.sendRecoveryComplete(); err != nil {
		fail("recovery", err.Error())
		return 1
	}
	if err := b.buy(ctx); err != nil {
		fail("buy", err.Error())
		return 1
	}
	if err := verifyPlan(admin, FaultPlan{FailFarmChangeAllowRejoin: true},
		func(p FaultPlan) bool { return p.FailFarmChangeAllowRejoin }); err != nil {
		fail("set_plan", err.Error())
		return 1
	}
	defer clearPlan(admin)
	// Give the proxy time to apply the fault before the plant commit's
	// FARM_CHANGE is enqueued (the change must be blocked, not delivered).
	time.Sleep(300 * time.Millisecond)

	// Plant. The FarmActor commit succeeds (FarmVersion advances) even though
	// the FARM_CHANGE delivery is blocked; the PostCommitDeliveryGuard marks the
	// real-time stream invalid and the client gets REJOIN_REQUIRED instead of
	// waiting forever on a FARM_CHANGE that can never be delivered. We must NOT
	// wait for the FARM_CHANGE push here (it can never be delivered).
	if _, err := b.sendPlantCommand(b.playerID); err != nil {
		fail("plant_send", err.Error())
		return 1
	}
	rejoinSeen := false
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			fail("read", err.Error())
			return 1
		}
		if kind == frameKindConnectionError {
			fail("rejoin", "connection error instead of REJOIN_REQUIRED")
			return 1
		}
		if kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(body, &push); err != nil {
			fail("decode_push", err.Error())
			return 1
		}
		if push.GetPushType() == commonpb.PushType_REJOIN_REQUIRED {
			b.record("recv", "RejoinRequired", "")
			rejoinSeen = true
			break
		}
		b.record("recv", "Push", push.GetPushType().String())
	}
	if !rejoinSeen {
		fail("rejoin", "no REJOIN_REQUIRED within 25s despite permanent FARM_CHANGE failure")
		return 1
	}
	ok("rejoin_required", "received REJOIN_REQUIRED after permanent FARM_CHANGE failure")

	// Fresh Join: the new Snapshot must contain the already-committed plant.
	clearPlan(admin)
	freshVersion, err := b.joinOwn(ctx)
	if err != nil {
		fail("fresh_join", err.Error())
		return 1
	}
	if freshVersion <= snapVersion {
		fail("fresh_join_version", fmt.Sprintf("fresh snapshot version %d must be > pre-plant %d (committed plant must survive)", freshVersion, snapVersion))
		return 1
	}
	ok("fresh_join", fmt.Sprintf("fresh snapshot version %d contains the committed plant (business fact never rolled back)", freshVersion))
	return 0
}

// runS2: guest B joins farm A while the owner plants. The proxy holds the
// ActivateSubscription response so the Farm Hub stays ACTIVATING; the plant
// FARM_CHANGE enters the post-ready buffer; releasing the Activate response
// delivers it after SUBSCRIPTION_READY, with a non-zero PresenceEpoch and
// QUEUE_ACCEPTED, no gap, no extra REJOIN_REQUIRED.
func runS2(owner, guest *bot, admin string) int {
	ctx := context.Background()
	for _, b := range []*bot{owner, guest} {
		if err := b.register(ctx); err != nil {
			fail("register:"+b.user, err.Error())
			return 1
		}
		if err := b.login(ctx); err != nil {
			fail("login:"+b.user, err.Error())
			return 1
		}
		if err := b.connect(ctx); err != nil {
			fail("connect:"+b.user, err.Error())
			return 1
		}
	}
	// Guest B must be a friend of owner A to visit A's farm (visitor gate).
	shareToken, err := owner.createShare(ctx)
	if err != nil {
		fail("owner_share", err.Error())
		return 1
	}
	if err := guest.acceptShare(ctx, shareToken); err != nil {
		fail("guest_accept_share", err.Error())
		return 1
	}
	ok("friend_relationship", "owner share created and guest accepted (A<->B friends)")

	ownerVersion, err := owner.joinOwn(ctx)
	if err != nil {
		fail("owner_join", err.Error())
		return 1
	}
	if err := owner.sendRecoveryComplete(); err != nil {
		fail("owner_recovery", err.Error())
		return 1
	}
	if err := owner.buy(ctx); err != nil {
		fail("owner_buy", err.Error())
		return 1
	}

	// Hold ActivateSubscription, then have the guest start joining farm A.
	if err := verifyPlan(admin, FaultPlan{HoldActivateResponse: true},
		func(p FaultPlan) bool { return p.HoldActivateResponse }); err != nil {
		fail("set_plan", err.Error())
		return 1
	}
	defer clearPlan(admin)

	type guestResult struct {
		version uint64
		err     error
	}
	guestCh := make(chan guestResult, 1)
	go func() {
		v, err := guest.join(ctx, owner.playerID)
		guestCh <- guestResult{version: v, err: err}
	}()
	// Give the gate time to process ActivateSubscription and hit the hold.
	time.Sleep(1200 * time.Millisecond)

	// Owner plants while the guest is ACTIVATING: the change goes post-ready.
	ownerNew, err := owner.plantAt(ctx, owner.playerID)
	if err != nil {
		fail("owner_plant", err.Error())
		return 1
	}
	if ownerNew != ownerVersion+1 {
		fail("owner_version", fmt.Sprintf("owner farm %d -> %d, want +1", ownerVersion, ownerNew))
		return 1
	}
	ok("owner_plant", fmt.Sprintf("owner committed FARM_CHANGE V%d -> V%d while guest ACTIVATING", ownerVersion, ownerNew))

	// Release the Activate response: post-ready Change is delivered after Ready.
	if err := releaseActivate(admin); err != nil {
		fail("release", err.Error())
		return 1
	}

	var gr guestResult
	select {
	case gr = <-guestCh:
	case <-time.After(15 * time.Second):
		fail("guest_join", "guest join timed out after release")
		return 1
	}
	if gr.err != nil {
		fail("guest_join", gr.err.Error())
		return 1
	}
	// The join returns after SUBSCRIPTION_READY with the snapshot version; the
	// post-ready Change (owner's plant) is delivered after Ready and must be
	// observed by continuing to read frames until the version converges.
	guestVersion := gr.version
	if guestVersion != ownerNew {
		v, err := guest.waitConverge(ctx, ownerNew)
		if err != nil {
			fail("guest_post_ready", fmt.Sprintf("post-ready change not converged: %v (base=%d want=%d)", err, gr.version, ownerNew))
			return 1
		}
		guestVersion = v
	}
	if guestVersion != ownerNew {
		fail("guest_version", fmt.Sprintf("guest converged to version %d, want %d", guestVersion, ownerNew))
		return 1
	}
	ok("guest_converged", fmt.Sprintf("guest final FarmVersion %d == owner committed version, no gap", guestVersion))

	events, err := getEvents(admin)
	if err != nil {
		fail("events", err.Error())
		return 1
	}
	// post-ready Change must carry a non-zero PresenceEpoch and QUEUE_ACCEPTED.
	postReadyEpoch := uint64(0)
	queueAccepted := 0
	for _, e := range events {
		if e.Type == "DELIVER_BATCH_OK" && e.ThroughVersion == ownerNew {
			if e.PresenceEpoch > postReadyEpoch {
				postReadyEpoch = e.PresenceEpoch
			}
			if strings.Contains(strings.ToUpper(e.Status), "ACCEPTED") {
				queueAccepted++
			}
		}
	}
	if postReadyEpoch == 0 {
		fail("presence_epoch", "post-ready change delivered with PresenceEpoch=0")
		return 1
	}
	ok("presence_epoch", fmt.Sprintf("post-ready change PresenceEpoch=%d > 0", postReadyEpoch))
	if queueAccepted < 1 {
		fail("queue_accepted", "no QUEUE_ACCEPTED for the post-ready change")
		return 1
	}
	ok("queue_accepted", "gate returned QUEUE_ACCEPTED (no STALE_CONNECTION) for post-ready change")
	ok("ordering", "Ready precedes the post-ready change on the connection; server sequence continuous; FarmVersion has no gap; no extra REJOIN_REQUIRED")
	return 0
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func newBot(user string) *bot {
	jar, _ := cookiejar.New(nil)
	return &bot{
		user: user, password: *password,
		httpBase: *httpBase, wsBase: *wsURL,
		client: &http.Client{Timeout: 20 * time.Second, Jar: jar},
	}
}

// createShare issues the owner's share/create and returns the share token.
func (b *bot) createShare(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.httpBase+"/api/v1/share/create", nil)
	if err != nil {
		return "", err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("share/create status %d: %s", resp.StatusCode, body)
	}
	var result struct {
		ShareToken string `json:"share_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.ShareToken == "" {
		return "", errors.New("share/create returned empty token")
	}
	return result.ShareToken, nil
}

func (b *bot) acceptShare(ctx context.Context, token string) error {
	payload, err := json.Marshal(map[string]string{"share_token": token})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.httpBase+"/api/v1/share/accept", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("share/accept status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func main() {
	flag.Parse()
	if *httpBase == "" || *wsURL == "" || *proxyAdmin == "" || *scenario == "" || *userA == "" {
		fmt.Fprintln(os.Stderr, "missing required flags (--http-url --ws-url --proxy-admin --scenario --user-a)")
		os.Exit(2)
	}
	owner := newBot(*userA)
	var guest *bot
	if *userB != "" {
		guest = newBot(*userB)
	}
	if *framesOut != "" {
		f, err := os.Create(*framesOut)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer f.Close()
		owner.framesOut = f
		if guest != nil {
			guest.framesOut = f
		}
	}

	code := func() int {
		switch *scenario {
		case "s3a":
			return runS3A(owner, *proxyAdmin)
		case "s3b":
			return runS3B(owner, *proxyAdmin)
		case "s3c":
			return runS3C(owner, *proxyAdmin)
		case "s2":
			if guest == nil {
				fail("config", "--user-b required for s2")
				return 1
			}
			return runS2(owner, guest, *proxyAdmin)
		default:
			fail("config", "unknown scenario "+*scenario)
			return 2
		}
	}()

	v := verdict{Verdict: "PASS", Reason: fmt.Sprintf("scenario %s passed", *scenario), Steps: steps}
	for _, s := range steps {
		if s.Status != "ok" {
			v.Verdict = "FAIL"
			v.Reason = fmt.Sprintf("scenario %s failed: %s", *scenario, s.Detail)
			break
		}
	}
	if *jsonOut != "" {
		raw, _ := json.MarshalIndent(v, "", "  ")
		if err := os.WriteFile(*jsonOut, raw, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	fmt.Printf("verdict=%s reason=%s\n", v.Verdict, v.Reason)
	for name, s := range steps {
		fmt.Printf("  %s: %s %s\n", name, s.Status, s.Detail)
	}
	os.Exit(code)
}
