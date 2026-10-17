// Command lease-fault-proxy is an isolated test-only LogicService proxy for
// R1-Q1 real subscription-lease evidence. Normal calls are forwarded to a real
// farm-svr. Its loopback admin seam can (a) obtain real STALE_FENCE/CLOSED
// per-item results from SubscriptionHub and (b) make one Holder temporarily
// unavailable. No capability is added to farm-gate or showcase runtime.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type faultPlan struct {
	InjectPartialOnce bool `json:"inject_partial_once"`
	StaleEntries      int  `json:"stale_entries"`
	ClosedEntries     int  `json:"closed_entries"`
	ClosedDelayMS     int  `json:"closed_delay_ms"`
	UnavailableForMS  int  `json:"unavailable_for_ms"`
}

type renewEvent struct {
	AtMS                    int64          `json:"at_ms"`
	Outcome                 string         `json:"outcome"`
	Items                   int            `json:"items"`
	UniqueFarms             int            `json:"unique_farms"`
	EstimatedBytes          int            `json:"estimated_bytes"`
	Oversize                bool           `json:"oversize"`
	MinimumDeadlineRemainMS *int64         `json:"minimum_deadline_remaining_ms,omitempty"`
	Statuses                map[string]int `json:"statuses,omitempty"`
}

type proxyState struct {
	SchemaVersion              string       `json:"schema_version"`
	Holder                     string       `json:"holder"`
	RenewCalls                 int          `json:"renew_calls"`
	SuccessfulCalls            int          `json:"successful_calls"`
	UnavailableCalls           int          `json:"unavailable_calls"`
	MixedFarmCalls             int          `json:"mixed_farm_calls"`
	MaximumItems               int          `json:"maximum_items"`
	MaximumNonOversizeBytes    int          `json:"maximum_non_oversize_bytes"`
	OversizeCalls              int          `json:"oversize_calls"`
	OversizeDispatchedAlone    bool         `json:"oversize_dispatched_alone"`
	PartialInjected            bool         `json:"partial_injected"`
	InjectedStale              int          `json:"injected_stale"`
	InjectedClosed             int          `json:"injected_closed"`
	TerminalReappeared         int          `json:"terminal_reappeared"`
	MinimumDeadlineRemainingMS *int64       `json:"minimum_deadline_remaining_ms,omitempty"`
	RenewedIdentities          int          `json:"renewed_identities"`
	MinimumRenewalsPerIdentity int          `json:"minimum_renewals_per_identity"`
	MaximumRenewalsPerIdentity int          `json:"maximum_renewals_per_identity"`
	UnavailableWindowStartedMS int64        `json:"unavailable_window_started_ms,omitempty"`
	UnavailableWindowUntilMS   int64        `json:"unavailable_window_until_ms,omitempty"`
	FirstRecoverySuccessMS     int64        `json:"first_recovery_success_ms,omitempty"`
	Events                     []renewEvent `json:"events"`
}

type proxy struct {
	logicrpcpb.UnimplementedLogicServiceServer

	holder       string
	upstream     logicrpcpb.LogicServiceClient
	maxItems     int
	maxBytes     int
	maxDispatch  time.Duration
	safetyMargin time.Duration
	eventsFile   *os.File

	mu            sync.Mutex
	plan          faultPlan
	pendingStale  int
	pendingClosed int
	failUntil     time.Time
	state         proxyState
	terminal      map[[32]byte]struct{}
	deadlines     map[[32]byte]time.Time
	renewed       map[[32]byte]int
}

func main() {
	var listen, adminListen, upstream, holder, eventsOut string
	var maxItems, maxBytes int
	var maxDispatch, safetyMargin time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:0", "gRPC listen address")
	flag.StringVar(&adminListen, "admin-listen", "127.0.0.1:0", "loopback HTTP admin address")
	flag.StringVar(&upstream, "upstream", "", "real farm-svr address")
	flag.StringVar(&holder, "holder", "", "bounded evidence Holder name")
	flag.StringVar(&eventsOut, "events-out", "", "raw NDJSON batch events")
	flag.IntVar(&maxItems, "max-items", 64, "configured Gate subscription batch item cap")
	flag.IntVar(&maxBytes, "max-bytes", 65536, "configured Gate subscription batch estimate cap")
	flag.DurationVar(&maxDispatch, "max-dispatch", 400*time.Millisecond, "configured Gate dispatch cap")
	flag.DurationVar(&safetyMargin, "safety-margin", 500*time.Millisecond, "configured Gate subscription safety margin")
	flag.Parse()
	if upstream == "" || holder == "" || eventsOut == "" || maxItems <= 0 || maxBytes <= 0 || maxDispatch <= 0 || safetyMargin <= 0 {
		log.Fatal("--upstream, --holder, --events-out and positive limits are required")
	}

	conn, err := grpc.NewClient(upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial upstream: %v", err)
	}
	defer conn.Close()
	eventsFile, err := os.OpenFile(eventsOut, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		log.Fatalf("open events: %v", err)
	}
	defer eventsFile.Close()
	p := &proxy{
		holder: holder, upstream: logicrpcpb.NewLogicServiceClient(conn),
		maxItems: maxItems, maxBytes: maxBytes, maxDispatch: maxDispatch, safetyMargin: safetyMargin,
		eventsFile: eventsFile, terminal: make(map[[32]byte]struct{}), deadlines: make(map[[32]byte]time.Time), renewed: make(map[[32]byte]int),
		state: proxyState{SchemaVersion: "r1-q1-lease-fault-proxy/v1", Holder: holder, OversizeDispatchedAlone: true, Events: []renewEvent{}},
	}

	grpcListener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("gRPC listen: %v", err)
	}
	adminListener, err := net.Listen("tcp", adminListen)
	if err != nil {
		log.Fatalf("admin listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	logicrpcpb.RegisterLogicServiceServer(grpcServer, p)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /admin/state", p.handleState)
	mux.HandleFunc("POST /admin/plan", p.handlePlan)
	mux.HandleFunc("POST /admin/reset-window", p.handleResetWindow)
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}

	log.Printf("lease fault proxy holder=%s gRPC=%s admin=%s", holder, grpcListener.Addr(), adminListener.Addr())
	go func() {
		if serveErr := grpcServer.Serve(grpcListener); serveErr != nil {
			log.Printf("gRPC serve: %v", serveErr)
		}
	}()
	go func() {
		if serveErr := httpServer.Serve(adminListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("admin serve: %v", serveErr)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	grpcServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}

func (p *proxy) handlePlan(w http.ResponseWriter, r *http.Request) {
	var plan faultPlan
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&plan); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if plan.StaleEntries < 0 || plan.ClosedEntries < 0 || plan.ClosedDelayMS < 0 || plan.UnavailableForMS < 0 ||
		plan.StaleEntries+plan.ClosedEntries > p.maxItems {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded fault plan"})
		return
	}
	now := time.Now()
	p.mu.Lock()
	p.plan = plan
	if plan.InjectPartialOnce {
		p.pendingStale = plan.StaleEntries
		p.pendingClosed = plan.ClosedEntries
	}
	if plan.UnavailableForMS > 0 {
		p.failUntil = now.Add(time.Duration(plan.UnavailableForMS) * time.Millisecond)
		p.state.UnavailableWindowStartedMS = now.UnixMilli()
		p.state.UnavailableWindowUntilMS = p.failUntil.UnixMilli()
		p.state.FirstRecoverySuccessMS = 0
	}
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "applied"})
}

func (p *proxy) handleState(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	state := p.state
	state.Events = append([]renewEvent(nil), p.state.Events...)
	state.RenewedIdentities = len(p.renewed)
	for _, count := range p.renewed {
		if state.MinimumRenewalsPerIdentity == 0 || count < state.MinimumRenewalsPerIdentity {
			state.MinimumRenewalsPerIdentity = count
		}
		if count > state.MaximumRenewalsPerIdentity {
			state.MaximumRenewalsPerIdentity = count
		}
	}
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, state)
}

func (p *proxy) handleResetWindow(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	p.plan = faultPlan{}
	p.pendingStale = 0
	p.pendingClosed = 0
	p.failUntil = time.Time{}
	p.state = proxyState{
		SchemaVersion: "r1-q1-lease-fault-proxy/v1", Holder: p.holder,
		OversizeDispatchedAlone: true, Events: []renewEvent{},
	}
	p.terminal = make(map[[32]byte]struct{})
	p.renewed = make(map[[32]byte]int)
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func (p *proxy) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	received := time.Now()
	items, estimatedBytes, farms := batchShape(req)
	event := renewEvent{AtMS: received.UnixMilli(), Items: items, UniqueFarms: farms, EstimatedBytes: estimatedBytes}
	event.Oversize = estimatedBytes > p.maxBytes

	p.mu.Lock()
	p.state.RenewCalls++
	if items > p.state.MaximumItems {
		p.state.MaximumItems = items
	}
	if farms > 1 {
		p.state.MixedFarmCalls++
	}
	if event.Oversize {
		p.state.OversizeCalls++
		if items != 1 {
			p.state.OversizeDispatchedAlone = false
		}
	} else if estimatedBytes > p.state.MaximumNonOversizeBytes {
		p.state.MaximumNonOversizeBytes = estimatedBytes
	}
	for _, entry := range req.GetEntries() {
		key := entryKey(entry)
		if _, exists := p.terminal[key]; exists {
			p.state.TerminalReappeared++
		}
		if deadline, exists := p.deadlines[key]; exists {
			remaining := deadline.Sub(received).Milliseconds()
			if event.MinimumDeadlineRemainMS == nil || remaining < *event.MinimumDeadlineRemainMS {
				value := remaining
				event.MinimumDeadlineRemainMS = &value
			}
			if p.state.MinimumDeadlineRemainingMS == nil || remaining < *p.state.MinimumDeadlineRemainingMS {
				value := remaining
				p.state.MinimumDeadlineRemainingMS = &value
			}
		}
	}
	failing := received.Before(p.failUntil)
	staleNow := min(p.pendingStale, items)
	closedNow := min(p.pendingClosed, items-staleNow)
	partial := staleNow+closedNow > 0
	plan := p.plan
	if partial {
		p.pendingStale -= staleNow
		p.pendingClosed -= closedNow
		plan.StaleEntries = staleNow
		plan.ClosedEntries = closedNow
	}
	p.mu.Unlock()

	if items == 0 || items > p.maxItems || farms > 1 {
		event.Outcome = "INVALID_BATCH_SHAPE"
		p.record(event)
		return nil, status.Error(codes.InvalidArgument, "lease evidence proxy rejected invalid batch shape")
	}
	if failing {
		event.Outcome = "INJECTED_UNAVAILABLE"
		p.mu.Lock()
		p.state.UnavailableCalls++
		p.mu.Unlock()
		p.record(event)
		return nil, status.Error(codes.Unavailable, "injected temporary Holder failure")
	}

	var response *logicrpcpb.BatchRenewSubscriptionsResponse
	var err error
	if partial {
		response, err = p.realPartial(ctx, req, plan)
		event.Outcome = "REAL_PARTIAL"
	} else {
		response, err = p.upstream.BatchRenewSubscriptions(forwardContext(ctx), req)
		event.Outcome = "FORWARDED"
	}
	if err != nil {
		event.Outcome = "UPSTREAM_ERROR"
		p.record(event)
		return nil, err
	}
	event.Statuses = statusCounts(response)
	p.observeResponse(received, req, response, partial)
	p.record(event)
	return response, nil
}

func (p *proxy) realPartial(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest, plan faultPlan) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	need := plan.StaleEntries + plan.ClosedEntries
	if need <= 0 || len(req.GetEntries()) < need {
		return nil, status.Error(codes.FailedPrecondition, "partial plan has insufficient real entries")
	}
	result := &logicrpcpb.BatchRenewSubscriptionsResponse{Results: make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries))}
	position := 0
	for index := 0; index < plan.StaleEntries; index++ {
		entry := proto.Clone(req.Entries[position]).(*logicrpcpb.SubscriptionLeaseEntry)
		entry.Target.ConnectionGeneration++
		one, err := p.upstream.BatchRenewSubscriptions(forwardContext(ctx), oneEntryRequest(req, entry))
		if err != nil || len(one.GetResults()) != 1 {
			return nil, fmt.Errorf("real stale injection: results=%d err=%w", len(one.GetResults()), err)
		}
		result.Results[position] = one.Results[0]
		position++
	}
	for index := 0; index < plan.ClosedEntries; index++ {
		entry := proto.Clone(req.Entries[position]).(*logicrpcpb.SubscriptionLeaseEntry)
		short := proto.Clone(entry).(*logicrpcpb.SubscriptionLeaseEntry)
		short.RequestedTtlMs = 1
		shortened, err := p.upstream.BatchRenewSubscriptions(forwardContext(ctx), oneEntryRequest(req, short))
		if err != nil || len(shortened.GetResults()) != 1 ||
			shortened.Results[0].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED {
			return nil, fmt.Errorf("real closed injection could not shorten Hub lease: response=%v err=%w", shortened, err)
		}
		delay := time.Duration(plan.ClosedDelayMS) * time.Millisecond
		if delay < 5*time.Millisecond {
			delay = 5 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		closed, err := p.upstream.BatchRenewSubscriptions(forwardContext(ctx), oneEntryRequest(req, entry))
		if err != nil || len(closed.GetResults()) != 1 {
			return nil, fmt.Errorf("real closed injection: results=%d err=%w", len(closed.GetResults()), err)
		}
		result.Results[position] = closed.Results[0]
		position++
	}
	if position < len(req.Entries) {
		rest := &logicrpcpb.BatchRenewSubscriptionsRequest{
			SourceGatewayId: req.SourceGatewayId, SourceGatewayBootId: req.SourceGatewayBootId,
			Entries: req.Entries[position:],
		}
		forwarded, err := p.upstream.BatchRenewSubscriptions(forwardContext(ctx), rest)
		if err != nil || len(forwarded.GetResults()) != len(rest.Entries) {
			return nil, fmt.Errorf("real healthy siblings: results=%d want=%d err=%w", len(forwarded.GetResults()), len(rest.Entries), err)
		}
		copy(result.Results[position:], forwarded.Results)
	}
	return result, nil
}

func (p *proxy) observeResponse(received time.Time, req *logicrpcpb.BatchRenewSubscriptionsRequest, response *logicrpcpb.BatchRenewSubscriptionsResponse, partial bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.SuccessfulCalls++
	if p.state.UnavailableWindowUntilMS > 0 && received.UnixMilli() >= p.state.UnavailableWindowUntilMS && p.state.FirstRecoverySuccessMS == 0 {
		p.state.FirstRecoverySuccessMS = received.UnixMilli()
	}
	if partial {
		p.state.PartialInjected = true
	}
	for index, entry := range req.GetEntries() {
		if index >= len(response.GetResults()) || response.Results[index] == nil {
			continue
		}
		key := entryKey(entry)
		statusValue := response.Results[index].GetStatus()
		switch statusValue {
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED:
			accepted := time.Duration(response.Results[index].GetAcceptedTtlMs()) * time.Millisecond
			p.deadlines[key] = received.Add(-p.maxDispatch).Add(accepted - p.safetyMargin)
			p.renewed[key]++
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE:
			if partial {
				p.state.InjectedStale++
				p.terminal[key] = struct{}{}
				delete(p.deadlines, key)
			}
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED:
			if partial {
				p.state.InjectedClosed++
				p.terminal[key] = struct{}{}
				delete(p.deadlines, key)
			}
		}
	}
}

func (p *proxy) record(event renewEvent) {
	p.mu.Lock()
	p.state.Events = append(p.state.Events, event)
	if p.eventsFile != nil {
		raw, _ := json.Marshal(event)
		_, _ = p.eventsFile.Write(append(raw, '\n'))
		_ = p.eventsFile.Sync()
	}
	p.mu.Unlock()
}

func batchShape(req *logicrpcpb.BatchRenewSubscriptionsRequest) (items, estimatedBytes, uniqueFarms int) {
	if req == nil {
		return 0, 0, 0
	}
	farms := map[uint64]struct{}{}
	for _, entry := range req.GetEntries() {
		items++
		estimatedBytes += 128 + proto.Size(entry)
		if entry != nil {
			farms[entry.GetTargetFarmId()] = struct{}{}
		}
	}
	return items, estimatedBytes, len(farms)
}

func oneEntryRequest(original *logicrpcpb.BatchRenewSubscriptionsRequest, entry *logicrpcpb.SubscriptionLeaseEntry) *logicrpcpb.BatchRenewSubscriptionsRequest {
	return &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: original.GetSourceGatewayId(), SourceGatewayBootId: original.GetSourceGatewayBootId(),
		Entries: []*logicrpcpb.SubscriptionLeaseEntry{entry},
	}
}

func entryKey(entry *logicrpcpb.SubscriptionLeaseEntry) [32]byte {
	if entry == nil {
		return sha256.Sum256(nil)
	}
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&logicrpcpb.SubscriptionLeaseEntry{
		TargetFarmId: entry.GetTargetFarmId(), SubscriptionId: entry.GetSubscriptionId(), JoinAttemptId: entry.GetJoinAttemptId(),
	})
	return sha256.Sum256(raw)
}

func statusCounts(response *logicrpcpb.BatchRenewSubscriptionsResponse) map[string]int {
	counts := map[string]int{}
	for _, result := range response.GetResults() {
		counts[result.GetStatus().String()]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make(map[string]int, len(keys))
	for _, key := range keys {
		ordered[key] = counts[key]
	}
	return ordered
}

func forwardContext(ctx context.Context) context.Context {
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, incoming.Copy())
	}
	return ctx
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

// The non-faulted LogicService surface is a transparent typed forwarder.
func (p *proxy) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	return p.upstream.ExecuteCommand(forwardContext(ctx), req)
}
func (p *proxy) ArmSnapshot(ctx context.Context, req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	return p.upstream.ArmSnapshot(forwardContext(ctx), req)
}
func (p *proxy) SnapshotQueueAccepted(ctx context.Context, req *logicrpcpb.SnapshotQueueAcceptedRequest) (*logicrpcpb.SnapshotQueueAcceptedResponse, error) {
	return p.upstream.SnapshotQueueAccepted(forwardContext(ctx), req)
}
func (p *proxy) CatchupBatchQueueAccepted(ctx context.Context, req *logicrpcpb.CatchupBatchQueueAcceptedRequest) (*logicrpcpb.CatchupBatchQueueAcceptedResponse, error) {
	return p.upstream.CatchupBatchQueueAccepted(forwardContext(ctx), req)
}
func (p *proxy) CancelJoinAttempt(ctx context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	return p.upstream.CancelJoinAttempt(forwardContext(ctx), req)
}
func (p *proxy) CleanupSubscription(ctx context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	return p.upstream.CleanupSubscription(forwardContext(ctx), req)
}
func (p *proxy) BatchEvaluateFarmOpportunity(ctx context.Context, req *logicrpcpb.BatchEvaluateFarmOpportunityRequest) (*logicrpcpb.BatchEvaluateFarmOpportunityResponse, error) {
	return p.upstream.BatchEvaluateFarmOpportunity(forwardContext(ctx), req)
}
