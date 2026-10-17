// Command gate-vertical-e2e provides a Downlink fault-injection proxy for
// Authority Smoke tests. It sits between farm-svr and the real farm-gate,
// forwarding gRPC Downlink calls while injecting deterministic faults.
//
// Subcommands:
//
//	proxy  — start the fault-injection gRPC proxy
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: gate-vertical-e2e <proxy> [flags]\n")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "proxy":
		runProxy(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// Fault plan
// ---------------------------------------------------------------------------

// FaultPlan controls which faults the proxy injects. It is replaced atomically
// via the HTTP admin API.
type FaultPlan struct {
	// UnavailableBeforeForward: reject the first N DeliverBatch calls with
	// codes.Unavailable before forwarding to the real Gate.
	UnavailableBeforeForward int `json:"unavailable_before_forward"`

	// DropResponseAfterForward: forward to Gate, then discard the response
	// and return codes.Unavailable. Only fires once.
	DropResponseAfterForward bool `json:"drop_response_after_forward"`

	// FailFarmChangeAllowRejoin: return Unavailable for all DeliverBatch
	// calls containing FARM_CHANGE items, but allow REJOIN_REQUIRED through.
	FailFarmChangeAllowRejoin bool `json:"fail_farm_change_allow_rejoin"`

	// HoldActivateResponse: forward ActivateSubscription to Gate, then hold
	// the response until Release is called.
	HoldActivateResponse bool `json:"hold_activate_response"`
}

// ---------------------------------------------------------------------------
// Protocol event
// ---------------------------------------------------------------------------

type ProtocolEvent struct {
	Timestamp      string `json:"timestamp"`
	Type           string `json:"type"`
	Method         string `json:"method"`
	ItemID         string `json:"item_id,omitempty"`
	ItemType       string `json:"item_type,omitempty"`
	PresenceEpoch  uint64 `json:"presence_epoch,omitempty"`
	FromVersion    uint64 `json:"from_version,omitempty"`
	ThroughVersion uint64 `json:"through_version,omitempty"`
	Status         string `json:"status,omitempty"`
	Detail         string `json:"detail,omitempty"`
	// Q1 问题 B:FarmChange delta 内容 witness(解析 FARM_CHANGE frame)。
	DeltaCount  int    `json:"delta_count,omitempty"`
	PlotID      string `json:"plot_id,omitempty"`
	CropPhase   string `json:"crop_phase,omitempty"`
	PlotVersion uint64 `json:"plot_version,omitempty"`
	CropCycleID string `json:"crop_cycle_id,omitempty"`
}

// ---------------------------------------------------------------------------
// Proxy
// ---------------------------------------------------------------------------

type DownlinkProxy struct {
	logicrpcpb.UnimplementedGatewayDownlinkServiceServer

	mu       sync.Mutex
	plan     FaultPlan
	upstream logicrpcpb.GatewayDownlinkServiceClient
	conn     *grpc.ClientConn

	// Counters for deterministic one-shot faults.
	unavailableRemaining atomic.Int64
	dropResponseFired    atomic.Bool

	// Activate hold. activateReleaseCh is created/closed only while holding
	// activateMu (Q1: a release fired while an Activate handler is between the
	// plan read and the hold setup must never touch a nil channel, and the
	// handler must never Unlock a mutex it does not hold).
	activateMu        sync.Mutex
	activateReleaseCh chan struct{}
	activateHeld      atomic.Bool

	// Event log.
	eventsMu   sync.Mutex
	events     []ProtocolEvent
	eventsFile *os.File
}

func runProxy(args []string) {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "gRPC listen address")
	upstream := fs.String("upstream", "", "real Gate downlink address")
	adminListen := fs.String("admin-listen", "127.0.0.1:0", "HTTP admin listen address")
	eventsOut := fs.String("events-out", "", "NDJSON events output path")
	_ = fs.Parse(args)

	if *upstream == "" {
		log.Fatal("--upstream is required")
	}

	conn, err := grpc.NewClient(*upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial upstream %s: %v", *upstream, err)
	}

	var eventsFile *os.File
	if *eventsOut != "" {
		eventsFile, err = os.Create(*eventsOut)
		if err != nil {
			log.Fatalf("create events file: %v", err)
		}
	}

	proxy := &DownlinkProxy{
		upstream:   logicrpcpb.NewGatewayDownlinkServiceClient(conn),
		conn:       conn,
		eventsFile: eventsFile,
	}
	proxy.unavailableRemaining.Store(0)

	// Start gRPC server.
	grpcListener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen %s: %v", *listen, err)
	}
	grpcServer := grpc.NewServer()
	logicrpcpb.RegisterGatewayDownlinkServiceServer(grpcServer, proxy)

	// Start HTTP admin server.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/plan", proxy.handleSetPlan)
	mux.HandleFunc("GET /admin/plan", proxy.handleGetPlan)
	mux.HandleFunc("POST /admin/release-activate", proxy.handleReleaseActivate)
	mux.HandleFunc("GET /admin/events", proxy.handleGetEvents)
	adminListener, err := net.Listen("tcp", *adminListen)
	if err != nil {
		log.Fatalf("admin listen %s: %v", *adminListen, err)
	}

	log.Printf("proxy gRPC listening on %s, upstream=%s", grpcListener.Addr(), *upstream)
	log.Printf("proxy admin listening on %s", adminListener.Addr())

	go func() {
		if err := http.Serve(adminListener, mux); err != nil {
			log.Printf("admin server: %v", err)
		}
	}()

	if err := grpcServer.Serve(grpcListener); err != nil {
		log.Fatalf("gRPC server: %v", err)
	}
}

// ---------------------------------------------------------------------------
// gRPC handlers
// ---------------------------------------------------------------------------

func (p *DownlinkProxy) DeliverBatch(ctx context.Context, req *logicrpcpb.DeliverBatchRequest) (*logicrpcpb.DeliverBatchResponse, error) {
	p.mu.Lock()
	plan := p.plan
	p.mu.Unlock()

	// Fault: UnavailableBeforeForward
	if remaining := p.unavailableRemaining.Load(); remaining > 0 {
		p.unavailableRemaining.Add(-1)
		p.recordEvent("UNAVAILABLE_BEFORE_FORWARD", "DeliverBatch", req, "injected_unavailable")
		return nil, status.Error(codes.Unavailable, "injected: unavailable before forward")
	}

	// Fault: FailFarmChangeAllowRejoin
	if plan.FailFarmChangeAllowRejoin {
		if p.containsFarmChange(req) && !p.containsRejoinRequired(req) {
			p.recordEvent("FAIL_FARM_CHANGE", "DeliverBatch", req, "farm_change_rejected")
			return nil, status.Error(codes.Unavailable, "injected: farm change rejected")
		}
	}

	// Forward to real Gate.
	resp, err := p.upstream.DeliverBatch(ctx, req)
	if err != nil {
		p.recordEvent("UPSTREAM_ERROR", "DeliverBatch", req, err.Error())
		return nil, err
	}

	// Fault: DropResponseAfterForward (one-shot)
	if plan.DropResponseAfterForward && p.dropResponseFired.CompareAndSwap(false, true) {
		p.recordEvent("DROP_RESPONSE_AFTER_FORWARD", "DeliverBatch", req, "response_dropped")
		return nil, status.Error(codes.Unavailable, "injected: response dropped after forward")
	}

	p.recordResponseEvent("DELIVER_BATCH_OK", "DeliverBatch", req, resp)
	return resp, nil
}

func (p *DownlinkProxy) ActivateSubscription(ctx context.Context, req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	p.mu.Lock()
	plan := p.plan
	p.mu.Unlock()

	// Forward to real Gate first.
	resp, err := p.upstream.ActivateSubscription(ctx, req)
	if err != nil {
		return nil, err
	}

	// Fault: HoldActivateResponse
	if plan.HoldActivateResponse {
		// Install the channel before marking held: activateHeld==true must
		// imply the release channel is already readable under activateMu, so
		// an admin release can never observe a nil channel (deadlock window).
		ch := make(chan struct{})
		p.activateMu.Lock()
		p.activateReleaseCh = ch
		p.activateMu.Unlock()
		p.activateHeld.Store(true)
		p.recordEvent("HOLD_ACTIVATE_RESPONSE", "ActivateSubscription", nil, "holding")
		select {
		case <-ch:
		case <-ctx.Done():
			// A caller cancelling must not leave a stale channel behind: the
			// next hold creates a fresh channel and the admin release only
			// closes the one it read under activateMu.
			p.activateHeld.Store(false)
			return nil, ctx.Err()
		}
		p.activateHeld.Store(false)
		p.recordEvent("RELEASE_ACTIVATE_RESPONSE", "ActivateSubscription", nil, "released")
		return resp, nil
	}

	p.recordActivateEvent(resp)
	return resp, nil
}

// ---------------------------------------------------------------------------
// HTTP admin handlers
// ---------------------------------------------------------------------------

func (p *DownlinkProxy) handleSetPlan(w http.ResponseWriter, r *http.Request) {
	var plan FaultPlan
	if err := json.NewDecoder(r.Body).Decode(&plan); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	p.plan = plan
	p.mu.Unlock()
	p.unavailableRemaining.Store(int64(plan.UnavailableBeforeForward))
	p.dropResponseFired.Store(false)
	log.Printf("fault plan updated: %+v", plan)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (p *DownlinkProxy) handleGetPlan(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	plan := p.plan
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plan)
}

func (p *DownlinkProxy) handleReleaseActivate(w http.ResponseWriter, _ *http.Request) {
	if p.activateHeld.CompareAndSwap(true, false) {
		p.activateMu.Lock()
		ch := p.activateReleaseCh
		p.activateReleaseCh = nil
		p.activateMu.Unlock()
		if ch != nil {
			close(ch)
		}
		p.recordEvent("MANUAL_RELEASE_ACTIVATE", "ActivateSubscription", nil, "released_via_admin")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "released"})
		return
	}
	http.Error(w, "no activate response held", http.StatusConflict)
}

func (p *DownlinkProxy) handleGetEvents(w http.ResponseWriter, _ *http.Request) {
	p.eventsMu.Lock()
	events := make([]ProtocolEvent, len(p.events))
	copy(events, p.events)
	p.eventsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// ---------------------------------------------------------------------------
// Event recording
// ---------------------------------------------------------------------------

func (p *DownlinkProxy) recordEvent(eventType, method string, req *logicrpcpb.DeliverBatchRequest, detail string) {
	event := ProtocolEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Type:      eventType,
		Method:    method,
		Detail:    detail,
	}
	if req != nil && len(req.GetItems()) > 0 {
		item := req.GetItems()[0]
		event.ItemID = item.GetItemId()
		event.ItemType = itemPushType(item)
		event.PresenceEpoch = item.GetPresenceEpoch()
		event.FromVersion = item.GetFromVersion()
		event.ThroughVersion = item.GetThroughVersion()
		enrichFarmChange(&event, item)
	}
	p.appendEvent(event)
}

// enrichFarmChange 解析 DownlinkItem.frame 里的 FARM_CHANGE payload,把
// change 的 delta 内容(delta_count / plot_id / crop_phase / plot_version /
// crop_cycle_id)写入 ProtocolEvent。解析失败静默忽略(item 顶层版本字段已足够)。
func itemPushType(item *logicrpcpb.DownlinkItem) string {
	if item == nil || len(item.GetFrame()) == 0 {
		return ""
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil || frame.GetServerPush() == nil {
		return ""
	}
	return frame.GetServerPush().GetPushType().String()
}

func enrichFarmChange(event *ProtocolEvent, item *logicrpcpb.DownlinkItem) {
	if item == nil || len(item.GetFrame()) == 0 {
		return
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil {
		return
	}
	push := frame.GetServerPush()
	if push == nil || push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return
	}
	change := changePush.GetChange()
	if change == nil {
		return
	}
	event.ThroughVersion = change.GetFarmVersion()
	if event.FromVersion == 0 && change.GetFarmVersion() > 0 {
		// item 顶层 from 未填时按 change 严格 +1 推断;已填则以 item 为准。
		event.FromVersion = change.GetFarmVersion() - 1
	}
	event.DeltaCount = len(change.GetChangedPlots())
	if len(change.GetChangedPlots()) > 0 {
		plot := change.GetChangedPlots()[0]
		event.PlotID = fmt.Sprintf("%d", plot.GetPlotIndex())
		event.CropPhase = plot.GetCropPhase().String()
		event.PlotVersion = plot.GetPlotVersion()
		event.CropCycleID = plot.GetCropCycleId()
	}
}

func (p *DownlinkProxy) recordResponseEvent(eventType, method string, req *logicrpcpb.DeliverBatchRequest, resp *logicrpcpb.DeliverBatchResponse) {
	if req == nil || resp == nil {
		return
	}
	for i, item := range req.GetItems() {
		event := ProtocolEvent{
			Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
			Type:           eventType,
			Method:         method,
			ItemID:         item.GetItemId(),
			ItemType:       itemPushType(item),
			PresenceEpoch:  item.GetPresenceEpoch(),
			FromVersion:    item.GetFromVersion(),
			ThroughVersion: item.GetThroughVersion(),
		}
		enrichFarmChange(&event, item)
		if i < len(resp.GetResults()) {
			event.Status = resp.GetResults()[i].GetStatus().String()
		}
		p.appendEvent(event)
	}
}

func (p *DownlinkProxy) recordActivateEvent(resp *logicrpcpb.ActivateSubscriptionResponse) {
	event := ProtocolEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Type:      "ACTIVATE_SUBSCRIPTION",
		Method:    "ActivateSubscription",
	}
	if resp != nil {
		event.Status = resp.GetStatus().String()
		event.ThroughVersion = resp.GetReadyThroughVersion()
	}
	p.appendEvent(event)
}

func (p *DownlinkProxy) appendEvent(event ProtocolEvent) {
	p.eventsMu.Lock()
	p.events = append(p.events, event)
	p.eventsMu.Unlock()

	if p.eventsFile != nil {
		data, _ := json.Marshal(event)
		p.eventsFile.Write(append(data, '\n'))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// pushTypeOf parses item.frame to the server push type. It is authoritative
// because the item-level from/through_version fields are NOT always populated
// by the Gate (Q1 S3-C: a FARM_CHANGE was delivered despite FailFarmChange
// because containsFarmChange relied on item-level version fields that were 0).
func pushTypeOf(item *logicrpcpb.DownlinkItem) commonpb.PushType {
	if item == nil || len(item.GetFrame()) == 0 {
		return commonpb.PushType_PUSH_TYPE_UNSPECIFIED
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil {
		return commonpb.PushType_PUSH_TYPE_UNSPECIFIED
	}
	push := frame.GetServerPush()
	if push == nil {
		return commonpb.PushType_PUSH_TYPE_UNSPECIFIED
	}
	return push.GetPushType()
}

func farmChangeOf(item *logicrpcpb.DownlinkItem) *farmpb.FarmChange {
	if item == nil || len(item.GetFrame()) == 0 {
		return nil
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil {
		return nil
	}
	push := frame.GetServerPush()
	if push == nil || push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return nil
	}
	return changePush.GetChange()
}

func (p *DownlinkProxy) containsFarmChange(req *logicrpcpb.DeliverBatchRequest) bool {
	for _, item := range req.GetItems() {
		if farmChangeOf(item) != nil {
			return true
		}
	}
	return false
}

func (p *DownlinkProxy) containsRejoinRequired(req *logicrpcpb.DeliverBatchRequest) bool {
	for _, item := range req.GetItems() {
		if pushTypeOf(item) == commonpb.PushType_REJOIN_REQUIRED {
			return true
		}
	}
	return false
}

// Ensure proto import is used (for Clone in future extensions).
var _ = proto.Clone
