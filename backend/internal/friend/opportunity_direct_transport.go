package friend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	commonpb "farmgame/api/gen/common"
	friendpb "farmgame/api/gen/friend"
	logicrpcpb "farmgame/api/gen/logicrpc"
	gateprotocol "farmgame/internal/gateway/protocol"
	"farmgame/internal/gateway/session"
	"farmgame/internal/logic/downlink"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// TargetedOpportunityHintTransport is the worker-facing transport seam. The
// transport receives only invalidation identity; it must never turn a Hint
// into an authorization decision.
type TargetedOpportunityHintTransport interface {
	PublishTargetedHint(context.Context, uint64, uint64, uint64, uint64) error
}

// PubSubOpportunityHintTransport adapts the existing targeted Redis Pub/Sub
// bus to the worker-facing transport seam.
type PubSubOpportunityHintTransport struct {
	bus       *RedisOpportunityHintBus
	index     FriendIndexRepository
	published atomic.Uint64
	errors    atomic.Uint64
}

// NewPubSubOpportunityHintTransport constructs the best-effort Pub/Sub
// publisher. The FriendIndex is read once to keep the existing bounded target
// collection semantics.
func NewPubSubOpportunityHintTransport(bus *RedisOpportunityHintBus, index FriendIndexRepository) (*PubSubOpportunityHintTransport, error) {
	if bus == nil || index == nil {
		return nil, errors.New("friend: pubsub opportunity hint transport dependencies are required")
	}
	return &PubSubOpportunityHintTransport{bus: bus, index: index}, nil
}

func (p *PubSubOpportunityHintTransport) PublishTargetedHint(ctx context.Context, ownerID, _,
	sourceFarmVersion, opportunityRevision uint64) error {
	if p == nil || p.bus == nil || p.index == nil {
		return errors.New("friend: pubsub opportunity hint transport unavailable")
	}
	viewers, err := CollectTargetedOpportunityViewers(ctx, p.index, ownerID)
	if err != nil {
		return err
	}
	if len(viewers) == 0 {
		return nil
	}
	err = p.bus.PublishTargetedOpportunityHint(ctx, TargetedOpportunityHint{
		TargetOwnerID: ownerID, SourceFarmVersion: sourceFarmVersion,
		OpportunityRevision: opportunityRevision, ViewerPlayerIDs: viewers,
	})
	if err != nil {
		p.errors.Add(1)
		return err
	}
	p.published.Add(1)
	return nil
}

type PubSubOpportunityHintStats struct {
	Published uint64
	Errors    uint64
}

func (p *PubSubOpportunityHintTransport) Stats() PubSubOpportunityHintStats {
	if p == nil {
		return PubSubOpportunityHintStats{}
	}
	return PubSubOpportunityHintStats{
		Published: p.published.Load(),
		Errors:    p.errors.Load(),
	}
}

// DirectOpportunityHintTransport resolves online viewers from the authoritative
// SessionRoute store and queues one player-level push per active connection.
// Dispatcher owns batching, per-gateway FIFO isolation, bounded queueing and
// DeliverBatch retry/deadline behavior.
type DirectOpportunityHintTransport struct {
	index      FriendIndexRepository
	routes     session.SessionRouteResolver
	dispatcher *downlink.Dispatcher
	counters   directOpportunityHintCounters
}

// NewDirectOpportunityHintTransport constructs a direct transport from the
// existing FriendIndex, read-only SessionRoute resolver and Dispatcher.
func NewDirectOpportunityHintTransport(index FriendIndexRepository,
	routes session.SessionRouteResolver, dispatcher *downlink.Dispatcher) (*DirectOpportunityHintTransport, error) {
	if index == nil || routes == nil || dispatcher == nil {
		return nil, errors.New("friend: direct opportunity hint transport dependencies are required")
	}
	return &DirectOpportunityHintTransport{index: index, routes: routes, dispatcher: dispatcher}, nil
}

type DirectOpportunityHintReport struct {
	Targets          uint64
	Routes           uint64
	GatewayGroups    uint64
	MissingRoutes    uint64
	Enqueued         uint64
	QueueFull        uint64
	RouteUnavailable uint64
	Suppressed       uint64
}

// PublishTargetedHint collects the bounded online-candidate set and performs
// best-effort direct delivery. Missing/expired routes and queue/delivery
// failures do not fail the whole hint when another target can proceed.
func (t *DirectOpportunityHintTransport) PublishTargetedHint(ctx context.Context, ownerID, _,
	sourceFarmVersion, opportunityRevision uint64) error {
	if t == nil || t.index == nil {
		return errors.New("friend: direct opportunity hint transport unavailable")
	}
	viewers, err := CollectTargetedOpportunityViewers(ctx, t.index, ownerID)
	if err != nil {
		return err
	}
	if len(viewers) == 0 {
		return nil
	}
	_, dispatchErr := t.dispatch(ctx, TargetedOpportunityHint{
		TargetOwnerID: ownerID, SourceFarmVersion: sourceFarmVersion,
		OpportunityRevision: opportunityRevision, ViewerPlayerIDs: viewers,
	}, false)
	return dispatchErr
}

// DispatchTargetedOpportunityHint is the testable direct-delivery seam for an
// already-collected, sorted TargetedOpportunityHint.
func (t *DirectOpportunityHintTransport) DispatchTargetedOpportunityHint(ctx context.Context,
	hint TargetedOpportunityHint) (DirectOpportunityHintReport, error) {
	return t.dispatch(ctx, hint, false)
}

func (t *DirectOpportunityHintTransport) dispatch(ctx context.Context, hint TargetedOpportunityHint,
	suppressDelivery bool) (DirectOpportunityHintReport, error) {
	var report DirectOpportunityHintReport
	if t == nil || t.routes == nil || t.dispatcher == nil {
		return report, errors.New("friend: direct opportunity hint transport unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := hint.validate(); err != nil {
		return report, err
	}
	report.Targets = uint64(len(hint.ViewerPlayerIDs))
	t.counters.hints.Add(1)
	t.counters.targets.Add(report.Targets)

	routes, routeErr := t.routes.BatchGetActiveRoutes(ctx, hint.ViewerPlayerIDs)
	if routeErr != nil {
		t.counters.routeReadErrors.Add(1)
	}
	groups := make(map[string][]*logicrpcpb.DownlinkItem)
	for _, viewerID := range hint.ViewerPlayerIDs {
		route, ok := routes[viewerID]
		if !ok || !usableDirectRoute(route, viewerID) {
			report.MissingRoutes++
			t.counters.missingRoutes.Add(1)
			continue
		}
		frame, err := encodeFriendOpportunityHintFrame(hint, viewerID)
		if err != nil {
			return report, err
		}
		report.Routes++
		t.counters.routes.Add(1)
		item := &logicrpcpb.DownlinkItem{
			ItemId:              directOpportunityHintItemID(hint, viewerID),
			ConnectionId:        route.ConnectionID,
			Frame:               frame,
			TransportGeneration: route.ConnectionGeneration,
			GatewayId:           route.GatewayID,
			TargetFence: &logicrpcpb.DownlinkTargetFence{
				TargetPlayerId:       viewerID,
				GatewayId:            route.GatewayID,
				GatewayBootId:        route.GatewayBootID,
				ConnectionId:         route.ConnectionID,
				ConnectionGeneration: route.ConnectionGeneration,
				SessionEpoch:         route.SessionEpoch,
				SessionRouteRevision: route.RouteRevision,
			},
		}
		groups[route.GatewayID] = append(groups[route.GatewayID], item)
	}
	if len(groups) == 0 {
		if routeErr != nil {
			return report, fmt.Errorf("friend: resolve active opportunity routes: %w", routeErr)
		}
		return report, nil
	}
	gatewayIDs := make([]string, 0, len(groups))
	for gatewayID := range groups {
		gatewayIDs = append(gatewayIDs, gatewayID)
	}
	sort.Strings(gatewayIDs)
	report.GatewayGroups = uint64(len(gatewayIDs))
	t.counters.gatewayGroups.Add(report.GatewayGroups)
	for _, gatewayID := range gatewayIDs {
		for _, item := range groups[gatewayID] {
			if suppressDelivery {
				report.Suppressed++
				t.counters.suppressed.Add(1)
				continue
			}
			enqueueErr := t.dispatcher.EnqueueWithResult(item, t.observeResult)
			switch {
			case enqueueErr == nil:
				report.Enqueued++
				t.counters.enqueued.Add(1)
			case errors.Is(enqueueErr, downlink.ErrDispatcherFull):
				report.QueueFull++
				t.counters.queueFull.Add(1)
			case errors.Is(enqueueErr, downlink.ErrGatewayRouteUnavailable),
				errors.Is(enqueueErr, downlink.ErrDispatcherClosed):
				report.RouteUnavailable++
				t.counters.routeUnavailable.Add(1)
				t.counters.unavailable.Add(1)
			default:
				report.RouteUnavailable++
				t.counters.routeUnavailable.Add(1)
				t.counters.unavailable.Add(1)
			}
		}
	}
	if routeErr != nil && report.Enqueued == 0 && report.Suppressed == 0 {
		return report, fmt.Errorf("friend: resolve active opportunity routes: %w", routeErr)
	}
	return report, nil
}

func usableDirectRoute(route session.ActiveRoute, playerID uint64) bool {
	return route.PlayerID == playerID &&
		strings.TrimSpace(route.GatewayID) != "" &&
		strings.TrimSpace(route.GatewayBootID) != "" &&
		strings.TrimSpace(route.ConnectionID) != "" &&
		route.ConnectionGeneration != 0 &&
		route.SessionEpoch != 0 &&
		route.RouteRevision != 0
}

func directOpportunityHintItemID(hint TargetedOpportunityHint, viewerID uint64) string {
	return fmt.Sprintf("friend-opportunity:%d:%d:%d:%d",
		viewerID, hint.TargetOwnerID, hint.SourceFarmVersion, hint.OpportunityRevision)
}

func encodeFriendOpportunityHintFrame(hint TargetedOpportunityHint, viewerID uint64) ([]byte, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&friendpb.FriendOpportunityChanged{
		TargetOwnerId: hint.TargetOwnerID, SourceFarmVersion: hint.SourceFarmVersion,
		OpportunityRevision: hint.OpportunityRevision,
	})
	if err != nil {
		return nil, err
	}
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
		PushContext: &commonpb.ServerPushEnvelope_Player{
			Player: &commonpb.PlayerPushContext{TargetPlayerId: viewerID},
		},
		Payload: payload,
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(gateprotocol.NewServerPushFrame(1, push))
}

func (t *DirectOpportunityHintTransport) observeResult(result *logicrpcpb.DeliverBatchItemResult) {
	if t == nil {
		return
	}
	if result == nil {
		t.counters.unavailable.Add(1)
		return
	}
	switch result.Status {
	case logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED:
		t.counters.accepted.Add(1)
	case logicrpcpb.DownlinkItemStatus_QUEUE_FULL:
		t.counters.queueFull.Add(1)
	case logicrpcpb.DownlinkItemStatus_CONNECTION_NOT_FOUND:
		t.counters.connectionNotFound.Add(1)
	case logicrpcpb.DownlinkItemStatus_UNAVAILABLE:
		t.counters.unavailable.Add(1)
	default:
		t.counters.unavailable.Add(1)
	}
}

type directOpportunityHintCounters struct {
	hints              atomic.Uint64
	targets            atomic.Uint64
	routes             atomic.Uint64
	gatewayGroups      atomic.Uint64
	missingRoutes      atomic.Uint64
	routeReadErrors    atomic.Uint64
	enqueued           atomic.Uint64
	queueFull          atomic.Uint64
	routeUnavailable   atomic.Uint64
	accepted           atomic.Uint64
	connectionNotFound atomic.Uint64
	unavailable        atomic.Uint64
	suppressed         atomic.Uint64
}

type DirectOpportunityHintStats struct {
	Hints              uint64
	Targets            uint64
	Routes             uint64
	GatewayGroups      uint64
	MissingRoutes      uint64
	RouteReadErrors    uint64
	Enqueued           uint64
	QueueFull          uint64
	RouteUnavailable   uint64
	Accepted           uint64
	ConnectionNotFound uint64
	Unavailable        uint64
	Suppressed         uint64
}

func (t *DirectOpportunityHintTransport) Stats() DirectOpportunityHintStats {
	if t == nil {
		return DirectOpportunityHintStats{}
	}
	return DirectOpportunityHintStats{
		Hints: t.counters.hints.Load(), Targets: t.counters.targets.Load(),
		Routes: t.counters.routes.Load(), GatewayGroups: t.counters.gatewayGroups.Load(),
		MissingRoutes: t.counters.missingRoutes.Load(), RouteReadErrors: t.counters.routeReadErrors.Load(),
		Enqueued: t.counters.enqueued.Load(), QueueFull: t.counters.queueFull.Load(),
		RouteUnavailable: t.counters.routeUnavailable.Load(), Accepted: t.counters.accepted.Load(),
		ConnectionNotFound: t.counters.connectionNotFound.Load(), Unavailable: t.counters.unavailable.Load(),
		Suppressed: t.counters.suppressed.Load(),
	}
}

// ShadowDirectOpportunityHintTransport preserves the Pub/Sub product path,
// then executes direct route lookup and grouping without network delivery.
type ShadowDirectOpportunityHintTransport struct {
	primary TargetedOpportunityHintTransport
	direct  *DirectOpportunityHintTransport
}

func NewShadowDirectOpportunityHintTransport(
	primary TargetedOpportunityHintTransport,
	direct *DirectOpportunityHintTransport,
) (*ShadowDirectOpportunityHintTransport, error) {
	if primary == nil || direct == nil {
		return nil, errors.New(
			"friend: shadow-direct opportunity hint transport requires primary and direct transports",
		)
	}
	return &ShadowDirectOpportunityHintTransport{
		primary: primary,
		direct:  direct,
	}, nil
}

func (t *ShadowDirectOpportunityHintTransport) PublishTargetedHint(ctx context.Context, ownerID, farmID,
	sourceFarmVersion, opportunityRevision uint64) error {
	if t == nil || t.primary == nil || t.direct == nil {
		return errors.New("friend: shadow-direct opportunity hint transport unavailable")
	}
	if err := t.primary.PublishTargetedHint(
		ctx,
		ownerID,
		farmID,
		sourceFarmVersion,
		opportunityRevision,
	); err != nil {
		return err
	}
	viewers, err := CollectTargetedOpportunityViewers(ctx, t.direct.index, ownerID)
	if err != nil {
		t.direct.counters.unavailable.Add(1)
		return nil
	}
	if len(viewers) == 0 {
		return nil
	}
	_, err = t.direct.dispatch(ctx, TargetedOpportunityHint{
		TargetOwnerID: ownerID, SourceFarmVersion: sourceFarmVersion,
		OpportunityRevision: opportunityRevision, ViewerPlayerIDs: viewers,
	}, true)
	if err != nil {
		t.direct.counters.unavailable.Add(1)
	}
	return nil
}

func (t *ShadowDirectOpportunityHintTransport) Stats() DirectOpportunityHintStats {
	if t == nil || t.direct == nil {
		return DirectOpportunityHintStats{}
	}
	return t.direct.Stats()
}

// OpportunityHintTransportMode is the explicit three-way transport switch.
// Pub/Sub remains the default assembly choice; direct and shadow-direct are
// opt-in until the process composition supplies their dependencies.
type OpportunityHintTransportMode string

const (
	OpportunityHintTransportPubSub       OpportunityHintTransportMode = "pubsub"
	OpportunityHintTransportDirect       OpportunityHintTransportMode = "direct"
	OpportunityHintTransportShadowDirect OpportunityHintTransportMode = "shadow-direct"
)

type SwitchableOpportunityHintTransport struct {
	pubsub TargetedOpportunityHintTransport
	shadow TargetedOpportunityHintTransport
	direct TargetedOpportunityHintTransport
	mode   atomic.Uint32
}

const (
	opportunityHintModeCodePubSub uint32 = iota + 1
	opportunityHintModeCodeShadowDirect
	opportunityHintModeCodeDirect
)

func NewSwitchableOpportunityHintTransport(
	pubsub TargetedOpportunityHintTransport,
	direct *DirectOpportunityHintTransport,
	initialMode OpportunityHintTransportMode,
) (*SwitchableOpportunityHintTransport, error) {
	shadow, err := NewShadowDirectOpportunityHintTransport(pubsub, direct)
	if err != nil {
		return nil, err
	}
	transport := &SwitchableOpportunityHintTransport{
		pubsub: pubsub,
		shadow: shadow,
		direct: direct,
	}
	if err := transport.SetMode(initialMode); err != nil {
		return nil, err
	}
	return transport, nil
}

func (t *SwitchableOpportunityHintTransport) SetMode(
	mode OpportunityHintTransportMode,
) error {
	if t == nil || t.pubsub == nil || t.shadow == nil || t.direct == nil {
		return errors.New("friend: switchable opportunity hint transport unavailable")
	}
	parsed, err := ParseOpportunityHintTransportMode(string(mode))
	if err != nil {
		return err
	}
	switch parsed {
	case OpportunityHintTransportPubSub:
		t.mode.Store(opportunityHintModeCodePubSub)
	case OpportunityHintTransportShadowDirect:
		t.mode.Store(opportunityHintModeCodeShadowDirect)
	case OpportunityHintTransportDirect:
		t.mode.Store(opportunityHintModeCodeDirect)
	default:
		return errors.New("friend: unsupported opportunity hint transport")
	}
	return nil
}

func (t *SwitchableOpportunityHintTransport) Mode() OpportunityHintTransportMode {
	if t == nil {
		return ""
	}
	switch t.mode.Load() {
	case opportunityHintModeCodePubSub:
		return OpportunityHintTransportPubSub
	case opportunityHintModeCodeShadowDirect:
		return OpportunityHintTransportShadowDirect
	case opportunityHintModeCodeDirect:
		return OpportunityHintTransportDirect
	default:
		return ""
	}
}

func (t *SwitchableOpportunityHintTransport) PublishTargetedHint(
	ctx context.Context,
	ownerID uint64,
	farmID uint64,
	sourceFarmVersion uint64,
	opportunityRevision uint64,
) error {
	var selected TargetedOpportunityHintTransport
	switch t.Mode() {
	case OpportunityHintTransportPubSub:
		selected = t.pubsub
	case OpportunityHintTransportShadowDirect:
		selected = t.shadow
	case OpportunityHintTransportDirect:
		selected = t.direct
	default:
		return errors.New("friend: switchable opportunity hint mode unavailable")
	}
	return selected.PublishTargetedHint(
		ctx,
		ownerID,
		farmID,
		sourceFarmVersion,
		opportunityRevision,
	)
}

func ParseOpportunityHintTransportMode(raw string) (OpportunityHintTransportMode, error) {
	mode := OpportunityHintTransportMode(strings.ToLower(strings.TrimSpace(raw)))
	switch mode {
	case OpportunityHintTransportPubSub, OpportunityHintTransportDirect, OpportunityHintTransportShadowDirect:
		return mode, nil
	default:
		return "", fmt.Errorf("friend: opportunity hint transport must be pubsub|direct|shadow-direct, got %q", raw)
	}
}

// SelectOpportunityHintTransport selects from pubsub, direct and optional
// shadow-direct publishers. Shadow-direct requires both the primary Pub/Sub
// transport and a concrete direct transport so the product path remains live.
func SelectOpportunityHintTransport(mode string, transports ...TargetedOpportunityHintTransport) (TargetedOpportunityHintTransport, error) {
	parsed, err := ParseOpportunityHintTransportMode(mode)
	if err != nil {
		return nil, err
	}
	switch parsed {
	case OpportunityHintTransportPubSub:
		if len(transports) < 1 || transports[0] == nil {
			return nil, errors.New("friend: pubsub opportunity hint transport is required")
		}
		return transports[0], nil
	case OpportunityHintTransportDirect:
		index := 1
		if len(transports) == 1 {
			index = 0
		}
		if len(transports) <= index || transports[index] == nil {
			return nil, errors.New("friend: direct opportunity hint transport is required")
		}
		return transports[index], nil
	case OpportunityHintTransportShadowDirect:
		if len(transports) >= 3 && transports[2] != nil {
			return transports[2], nil
		}
		if len(transports) < 2 || transports[0] == nil ||
			transports[1] == nil {
			return nil, errors.New(
				"friend: shadow-direct requires primary pubsub and direct transports",
			)
		}
		if direct, ok := transports[1].(*DirectOpportunityHintTransport); ok {
			return NewShadowDirectOpportunityHintTransport(
				transports[0],
				direct,
			)
		}
		return nil, errors.New(
			"friend: shadow-direct requires a concrete direct transport or explicit shadow publisher",
		)
	}
	return nil, errors.New("friend: unsupported opportunity hint transport")
}

func RegisterPubSubOpportunityHintMetrics(
	reg prometheus.Registerer,
	transport *PubSubOpportunityHintTransport,
) {
	if reg == nil || transport == nil {
		return
	}
	stats := func() PubSubOpportunityHintStats { return transport.Stats() }
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm",
			Subsystem: "friend_opportunity_hint",
			Name:      "pubsub_published_total",
			Help:      "Targeted opportunity Hints published through the Pub/Sub product path.",
		}, func() float64 { return float64(stats().Published) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm",
			Subsystem: "friend_opportunity_hint",
			Name:      "pubsub_errors_total",
			Help:      "Targeted opportunity Hint Pub/Sub publication errors.",
		}, func() float64 { return float64(stats().Errors) }),
	)
}

func RegisterDirectOpportunityHintMetrics(reg prometheus.Registerer, transport *DirectOpportunityHintTransport) {
	if reg == nil || transport == nil {
		return
	}
	stats := func() DirectOpportunityHintStats { return transport.Stats() }
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_targets_total",
			Help: "Targeted opportunity Hint viewer targets considered by direct transport.",
		}, func() float64 { return float64(stats().Targets) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_missing_routes_total",
			Help: "Online-candidate Hint targets omitted because their active SessionRoute was missing or fenced.",
		}, func() float64 { return float64(stats().MissingRoutes) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_queue_full_total",
			Help: "Direct opportunity Hint items rejected by a bounded queue.",
		}, func() float64 { return float64(stats().QueueFull) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_connection_not_found_total",
			Help: "Direct opportunity Hint items rejected because the connection was not found.",
		}, func() float64 { return float64(stats().ConnectionNotFound) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_unavailable_total",
			Help: "Direct opportunity Hint items that could not reach or enter a Gate.",
		}, func() float64 { return float64(stats().Unavailable) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_accepted_total",
			Help: "Direct opportunity Hint items accepted by the exact fenced Gate connection.",
		}, func() float64 { return float64(stats().Accepted) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_opportunity_hint", Name: "direct_suppressed_total",
			Help: "Direct opportunity Hint items resolved but intentionally suppressed in shadow mode.",
		}, func() float64 { return float64(stats().Suppressed) }),
	)
}

func RegisterSwitchableOpportunityHintMetrics(
	reg prometheus.Registerer,
	transport *SwitchableOpportunityHintTransport,
) {
	if reg == nil || transport == nil {
		return
	}
	for _, mode := range []OpportunityHintTransportMode{
		OpportunityHintTransportPubSub,
		OpportunityHintTransportShadowDirect,
		OpportunityHintTransportDirect,
	} {
		mode := mode
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm",
			Subsystem: "friend_opportunity_hint",
			Name:      "transport_mode",
			Help:      "Current opportunity Hint transport mode; exactly one mode is 1.",
			ConstLabels: prometheus.Labels{
				"mode": string(mode),
			},
		}, func() float64 {
			if transport.Mode() == mode {
				return 1
			}
			return 0
		}))
	}
}
