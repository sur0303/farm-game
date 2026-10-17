// Package command contains the S2 farm-gate binary-message boundary.
// Gate owns transport/session concerns and forwards only trusted commands to Logic.
package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gateway/commandcatalog"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/leasebatch"
	"farmgame/internal/gateway/preauth"
	"farmgame/internal/gateway/protocol"
	"farmgame/internal/gateway/session"

	"google.golang.org/protobuf/proto"
)

const ProtocolVersion uint32 = protocol.ProtocolVersion

var (
	ErrUnauthenticated          = errors.New("gate: unauthenticated")
	ErrBadMessage               = errors.New("gate: bad message")
	ErrRouteLeaseExpired        = errors.New("gate: route lease expired")
	ErrSubscriptionLeaseExpired = errors.New("gate: subscription lease expired")
	ErrGateBootLeaseExpired     = errors.New("gate: boot lease expired")
	ErrRecoveryIncomplete       = errors.New("gate: recovery incomplete")
	// ErrStaleSession covers kick, renew fence reject, and same-player
	// session supersession. It is distinct from ErrRouteLeaseExpired.
	ErrStaleSession = errors.New("gate: stale session")
)

// laneAdmission is the local SessionLane state after ClientHello. Route lease
// expiry is terminal for this connection generation: a later renew must not
// resurrect a lane after Gate has already failed closed.
type laneAdmission uint8

const (
	laneAdmissible laneAdmission = iota
	laneStale
	laneRouteLeaseExpired
)

const (
	maxClientBuildBytes         = 64
	defaultBackendMaxInflight   = 1024
	defaultBackendRPCTimeout    = 3 * time.Second
	defaultBackendAdmissionWait = 25 * time.Millisecond
	defaultCrossGateKickTimeout = 3 * time.Second
	defaultPendingJoinTimeout   = 10 * time.Second
	defaultSubscriptionLeaseTTL = 30 * time.Second
)

type LogicClient interface {
	ExecuteCommand(context.Context, *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error)
}

type JoinAttemptCanceler interface {
	CancelJoinAttempt(context.Context, *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error)
}

type JoinSnapshotArmer interface {
	ArmSnapshot(context.Context, *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error)
}

type SubscriptionCleaner interface {
	CleanupSubscription(context.Context, *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error)
}

type SubscriptionLeaseRenewer interface {
	BatchRenewSubscriptions(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error)
}

// SubscriptionLeasePartitioner performs physical Holder resolution and
// generation revalidation at dispatch time. Gate invokes this method only
// after releasing Gate.mu. Planner registration names the same physical
// Holder, also resolved off Gate.mu, so farms that share a Holder share a
// batch instead of each farm occupying its own dispatch slot.
type SubscriptionLeasePartitioner interface {
	BatchRenewSubscriptionsPartition(context.Context, string, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error)
}

// subscriptionLeasePartitionResolver names the physical Holder generation a
// farm currently sits on. Sharded clients implement this; static clients do
// not, and the planner then keeps one-farm isolation.
type subscriptionLeasePartitionResolver interface {
	SubscriptionLeasePartition(context.Context, uint64) (string, error)
}

type ConnectionCleaner interface {
	CleanupConnection(context.Context, uint64, string) error
}

type FencedConnectionCleaner interface {
	CleanupConnectionFence(context.Context, session.ActiveRoute) error
}

type CrossGateKicker interface {
	KickOldSession(context.Context, session.ActiveRoute) error
}

type connection struct {
	generation                    uint64
	binding                       *auth.SessionBinding
	lastSeq                       uint64
	serverSeq                     uint64
	writer                        *downlink.ConnectionWriter
	currentFarmID                 uint64
	currentSubscriptionID         string
	currentJoinAttemptID          string
	currentPresenceEpoch          uint64
	subscriptionContextGeneration uint64
	recoveryComplete              bool
	pendingJoin                   *pendingJoinState
	admission                     laneAdmission
	// routeLeaseDeadline is derived only from Gate's local monotonic clock and
	// the authority-returned accepted TTL. Route.LeaseExpiresAt is retained for
	// authority/debug data and must never be compared for local admission.
	routeLeaseDeadline time.Time
	// Subscription Lease is independent from SessionRoute Lease. Expiry only
	// fences the current farm subscription; it never tears down authentication
	// or extends/revives the SessionRoute.
	subscriptionLeaseDeadline time.Time
	subscriptionLeaseTerminal bool
}

// localConnectionFence identifies one exact in-process connection lane. The
// generation is part of the key so a delayed Disconnect from a reused
// connection ID cannot erase the replacement player's index entry.
type localConnectionFence struct {
	connectionID string
	generation   uint64
}

type pendingJoinState struct {
	operationID    string
	farmID         uint64
	subscriptionID string
	// attemptID 是 Gate 生成的运行时 Fence(26 HC-ACCESS-32),与 operation_id
	// 分离:同一 durable operation 的重连复用同一 intent,新 intent 生成新 attempt。
	attemptID     string
	accepted      bool
	state         pendingJoinLifecycle
	deadline      time.Time
	timer         *time.Timer
	leaseDeadline time.Time
	leaseTerminal bool
}

type pendingJoinLifecycle uint8

const (
	pendingJoinPending pendingJoinLifecycle = iota + 1
	pendingJoinCanceling
	pendingJoinClosed
)

type Gate struct {
	logicrpcpb.UnimplementedGateControlServiceServer
	auth                          *auth.Service
	logic                         LogicClient
	maxFrame                      int
	downlinkWriterCapacity        downlink.WriterCapacity
	mu                            sync.Mutex
	connections                   map[string]connection
	playerConnections             map[uint64]localConnectionFence
	downlink                      *downlink.ConnectionRegistry
	cleaner                       ConnectionCleaner
	crossGateKicker               CrossGateKicker
	crossGateKickTimeout          time.Duration
	backendSlots                  chan struct{}
	backendRPCTimeout             time.Duration
	backendAdmissionWait          time.Duration
	pendingJoinTimeout            time.Duration
	preAuth                       *preauth.Controller
	metrics                       *Metrics
	bootLease                     *bootlease.Lease
	routeLeaseSafetyMargin        time.Duration
	routeLeaseRenewMu             sync.Mutex
	routeLeasePlanner             *leasebatch.Planner[routeLeaseCandidate]
	routeLeaseConfig              LeaseRenewalLaneConfig
	subscriptionLeaseTTL          time.Duration
	subscriptionLeaseSafetyMargin time.Duration
	subscriptionLeaseRenewMu      sync.Mutex
	subscriptionLeasePlanner      *leasebatch.Planner[subscriptionLeaseCandidate]
	subscriptionLeaseBatchConfig  LeaseRenewalLaneConfig
	leaseClock                    func() time.Time
	leaseSchedulersDraining       bool
}

// SetBootLease installs the process-lifetime admission fence. A nil lease is
// retained only for isolated unit tests and legacy embedders; gatewayapp always
// installs one before opening a public transport.
func (g *Gate) SetBootLease(lease *bootlease.Lease) {
	g.mu.Lock()
	g.bootLease = lease
	g.mu.Unlock()
}

// BootLeaseTrusted is also used by /readyz. Once the last confirmed deadline
// passes, load balancers stop sending new connections and owner-write commands
// already in the process are rejected until Redis renew recovers.
func (g *Gate) BootLeaseTrusted() bool {
	g.mu.Lock()
	lease := g.bootLease
	g.mu.Unlock()
	return lease == nil || lease.Trusted(time.Now())
}

// SetPreAuthController attaches the transport-neutral budget. Transports call
// Begin/Admit at their earliest safe boundary; Gate owns the only successful
// completion point after ClientHello has atomically activated the session.
func (g *Gate) SetPreAuthController(controller *preauth.Controller) {
	g.mu.Lock()
	g.preAuth = controller
	g.mu.Unlock()
}

func (g *Gate) BeginPreAuth(connectionID string, generation uint64, clientIP string) error {
	g.mu.Lock()
	controller := g.preAuth
	g.mu.Unlock()
	if controller == nil {
		return nil
	}
	return controller.Open(connectionID, generation, clientIP)
}

func (g *Gate) AdmitPreAuth(connectionID string, generation uint64, frameBytes int) error {
	g.mu.Lock()
	current, authenticated := g.connections[connectionID]
	controller := g.preAuth
	authenticated = authenticated && current.generation == generation && current.binding != nil
	g.mu.Unlock()
	if controller == nil || authenticated {
		return nil
	}
	err := controller.Admit(connectionID, generation, frameBytes)
	if preauth.ReasonOf(err) == preauth.ReasonUnknownFence && g.IsAuthenticated(connectionID, generation) {
		// ClientHello may have completed between the Gate state check and the
		// budget call. In that case the next business frame is post-auth.
		return nil
	}
	return err
}

func (g *Gate) IsAuthenticated(connectionID string, generation uint64) bool {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	g.mu.Unlock()
	return ok && current.generation == generation && current.binding != nil
}

func (g *Gate) TimeoutPreAuth(connectionID string, generation uint64) bool {
	g.mu.Lock()
	controller := g.preAuth
	g.mu.Unlock()
	return controller != nil && controller.Timeout(connectionID, generation)
}

func (g *Gate) PreAuthMaxFrameBytes() int64 {
	g.mu.Lock()
	controller := g.preAuth
	g.mu.Unlock()
	if controller == nil {
		return 0
	}
	return controller.MaxFrameBytes()
}

func NewGate(authService *auth.Service, logic LogicClient, maxFrame int) *Gate {
	if maxFrame <= 0 {
		maxFrame = protocol.DefaultMaxFrameSize
	}
	leaseTTL := session.DefaultRouteLeaseTTL
	if authService != nil {
		leaseTTL = authService.RouteLeaseTTL()
	}
	g := &Gate{
		auth: authService, logic: logic, maxFrame: maxFrame,
		downlinkWriterCapacity:        downlink.WriterCapacity{MaxItems: 256, MaxBytes: maxFrame},
		connections:                   make(map[string]connection),
		playerConnections:             make(map[uint64]localConnectionFence),
		backendSlots:                  make(chan struct{}, defaultBackendMaxInflight),
		backendRPCTimeout:             defaultBackendRPCTimeout,
		pendingJoinTimeout:            defaultPendingJoinTimeout,
		crossGateKickTimeout:          defaultCrossGateKickTimeout,
		routeLeaseSafetyMargin:        defaultRouteLeaseSafetyMargin(leaseTTL),
		subscriptionLeaseTTL:          defaultSubscriptionLeaseTTL,
		subscriptionLeaseSafetyMargin: defaultRouteLeaseSafetyMargin(defaultSubscriptionLeaseTTL),
	}
	cfg := defaultLeaseRenewalConfig(leaseTTL, defaultSubscriptionLeaseTTL)
	g.routeLeaseConfig = cfg.SessionRoute
	g.subscriptionLeaseBatchConfig = cfg.Subscription
	g.routeLeasePlanner, _ = leasebatch.NewPlanner[routeLeaseCandidate](cfg.SessionRoute.Batch)
	g.subscriptionLeasePlanner, _ = leasebatch.NewPlanner[subscriptionLeaseCandidate](cfg.Subscription.Batch)
	return g
}

// SetDownlinkWriterCapacity configures the per-connection asynchronous
// downlink budget before the Gate starts admitting connections. Hint quotas
// are soft reservations: they shed recoverable hints before they can consume
// the capacity reserved for authoritative FarmChange and command results.
func (g *Gate) SetDownlinkWriterCapacity(capacity downlink.WriterCapacity) error {
	if capacity.MaxItems <= 0 || capacity.MaxBytes <= 0 {
		return errors.New("gate: downlink writer item and byte limits must be positive")
	}
	if capacity.Hint.MaxHintItems < 0 || capacity.Hint.MaxHintBytes < 0 {
		return errors.New("gate: downlink hint quotas must not be negative")
	}
	if capacity.Hint.MaxHintItems > capacity.MaxItems {
		return errors.New("gate: downlink hint item quota exceeds writer item limit")
	}
	if capacity.Hint.MaxHintBytes > capacity.MaxBytes {
		return errors.New("gate: downlink hint byte quota exceeds writer byte limit")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.connections) != 0 {
		return errors.New("gate: cannot change downlink writer capacity after admitting connections")
	}
	g.downlinkWriterCapacity = capacity
	return nil
}

func defaultRouteLeaseSafetyMargin(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	margin := ttl / 10
	if margin > time.Second {
		margin = time.Second
	}
	return margin
}

// SetRouteLeaseSafetyMargin configures the conservative local-deadline
// subtraction required by contract 26. It must remain smaller than the
// requested route lease TTL; invalid values fail startup instead of silently
// extending trust.
func (g *Gate) SetRouteLeaseSafetyMargin(margin time.Duration) error {
	if margin < 0 {
		return errors.New("gate: route lease safety margin must not be negative")
	}
	ttl := session.DefaultRouteLeaseTTL
	if g.auth != nil {
		ttl = g.auth.RouteLeaseTTL()
	}
	if ttl <= 0 || margin >= ttl {
		return fmt.Errorf("gate: route lease safety margin %s must be smaller than ttl %s", margin, ttl)
	}
	if err := g.routeLeaseConfig.Batch.ValidateSafety(ttl, margin); err != nil {
		return fmt.Errorf("gate: route lease safety margin violates batch window: %w", err)
	}
	g.mu.Lock()
	g.routeLeaseSafetyMargin = margin
	g.mu.Unlock()
	return nil
}

func conservativeRouteLeaseDeadline(start time.Time, acceptedTTL, safetyMargin time.Duration) (time.Time, bool) {
	if start.IsZero() || acceptedTTL <= 0 || safetyMargin < 0 || safetyMargin >= acceptedTTL {
		return time.Time{}, false
	}
	return start.Add(acceptedTTL - safetyMargin), true
}

// SetSubscriptionLeaseConfig freezes the TTL Gate requests from the current
// Farm Holder and the conservative subtraction used to derive a local
// monotonic deadline. The Holder may clamp the requested TTL further.
func (g *Gate) SetSubscriptionLeaseConfig(requestedTTL, safetyMargin time.Duration) error {
	if requestedTTL < time.Millisecond || requestedTTL > time.Duration(^uint32(0))*time.Millisecond {
		return fmt.Errorf("gate: invalid subscription lease ttl %s", requestedTTL)
	}
	if safetyMargin < 0 || safetyMargin >= requestedTTL {
		return fmt.Errorf("gate: subscription lease safety margin %s must be smaller than ttl %s", safetyMargin, requestedTTL)
	}
	// Preserve the legacy setter's standalone safety: derive a new safe default
	// planner for this TTL. gatewayapp immediately replaces it with explicit env
	// wiring before opening connections.
	defaultBatch := defaultLeaseRenewalLaneConfig(requestedTTL)
	if err := validateLeaseRenewalLaneConfig("subscription", defaultBatch, requestedTTL, safetyMargin); err != nil {
		return err
	}
	planner, err := leasebatch.NewPlanner[subscriptionLeaseCandidate](defaultBatch.Batch)
	if err != nil {
		return err
	}
	g.mu.Lock()
	if g.subscriptionLeasePlanner != nil && g.subscriptionLeasePlanner.Stats().Backlog != 0 {
		g.mu.Unlock()
		return errors.New("gate: cannot change subscription lease config with scheduled subscriptions")
	}
	if g.subscriptionLeasePlanner != nil {
		g.subscriptionLeasePlanner.Stop()
	}
	g.subscriptionLeaseTTL = requestedTTL
	g.subscriptionLeaseSafetyMargin = safetyMargin
	g.subscriptionLeaseBatchConfig = defaultBatch
	g.subscriptionLeasePlanner = planner
	g.mu.Unlock()
	return nil
}

func conservativeSubscriptionLeaseDeadline(start time.Time, acceptedTTL, safetyMargin time.Duration) (time.Time, bool) {
	return conservativeRouteLeaseDeadline(start, acceptedTTL, safetyMargin)
}

// SetPendingJoinTimeout configures the liveness deadline shared by the two
// Join barrier latches. It is primarily exposed for deterministic tests.
func (g *Gate) SetPendingJoinTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = defaultPendingJoinTimeout
	}
	g.mu.Lock()
	g.pendingJoinTimeout = timeout
	g.mu.Unlock()
}

// SetBackendPool configures the Gate-wide bounded admission point. A value <=0
// keeps the safe default. The slot is acquired only after SessionLane checks
// pass and is released when the backend RPC completes.
func (g *Gate) SetBackendPool(maxInflight int, rpcTimeout time.Duration) {
	if maxInflight <= 0 {
		maxInflight = defaultBackendMaxInflight
	}
	if rpcTimeout <= 0 {
		rpcTimeout = defaultBackendRPCTimeout
	}
	g.mu.Lock()
	g.backendSlots = make(chan struct{}, maxInflight)
	g.backendRPCTimeout = rpcTimeout
	g.mu.Unlock()
}

// SetBackendAdmissionWait absorbs a short burst while preserving the hard
// backend concurrency cap. A caller that cannot obtain a slot within this
// bounded wait still fails closed with ERR_SERVER_BUSY; it never creates an
// unbounded request queue.
func (g *Gate) SetBackendAdmissionWait(wait time.Duration) {
	if wait < 0 {
		wait = 0
	}
	g.mu.Lock()
	g.backendAdmissionWait = wait
	g.mu.Unlock()
}

func (g *Gate) acquireBackendSlot(ctx context.Context, slots chan struct{}) bool {
	if slots == nil {
		return false
	}
	g.mu.Lock()
	wait := g.backendAdmissionWait
	g.mu.Unlock()
	if wait <= 0 {
		select {
		case slots <- struct{}{}:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// SetDownlinkRegistry attaches the gateway-local asynchronous push index. It
// is optional for the HTTP/FakeTransport harness, but required when Logic is a
// separate process and sends GatewayDownlinkService.DeliverBatch callbacks.
func (g *Gate) SetDownlinkRegistry(registry *downlink.ConnectionRegistry) {
	g.mu.Lock()
	g.downlink = registry
	g.mu.Unlock()
	if registry != nil {
		registry.SetJoinPromotionHandler(g.promoteJoin)
		registry.SetSubscriptionTerminationHandler(g.terminateSubscription)
	}
}

// revokeDownlinkTargetLocked mirrors a terminal SessionLane transition into
// the asynchronous downlink registry. g.mu must be held. The registry method
// is local, bounded, and does not call back into Gate.
func (g *Gate) revokeDownlinkTargetLocked(current connection) {
	if g.downlink == nil || current.binding == nil {
		return
	}
	route := current.binding.Route
	g.downlink.RevokeSessionTarget(&logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: current.binding.PlayerID,
		GatewayId:      route.GatewayID, GatewayBootId: route.GatewayBootID,
		ConnectionId: route.ConnectionID, ConnectionGeneration: route.ConnectionGeneration,
		SessionEpoch: current.binding.ActiveSessionEpoch, SessionRouteRevision: route.RouteRevision,
	})
}

func (g *Gate) SetConnectionCleaner(cleaner ConnectionCleaner) {
	g.mu.Lock()
	g.cleaner = cleaner
	g.mu.Unlock()
}

func (g *Gate) SetCrossGateKicker(kicker CrossGateKicker) {
	g.mu.Lock()
	g.crossGateKicker = kicker
	g.mu.Unlock()
}

// SetMetrics attaches optional command-admission counters. Nil is allowed.
func (g *Gate) SetMetrics(metrics *Metrics) {
	g.mu.Lock()
	g.metrics = metrics
	g.mu.Unlock()
}

// SetCrossGateKickTimeout bounds the best-effort UX KICK. SessionAuthority is
// still checked while issuing every new Permit, so timeout cannot authorize a
// stale command; it only delays the old transport's notification/closure.
func (g *Gate) SetCrossGateKickTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = defaultCrossGateKickTimeout
	}
	g.mu.Lock()
	g.crossGateKickTimeout = timeout
	g.mu.Unlock()
}

// RenewSessionLeases performs one explicitly requested bounded batch. The app
// lifecycle uses RenewDueSessionLeases so stable bucket phases, rather than a
// global ticker edge, decide eligibility.
func (g *Gate) RenewSessionLeases(ctx context.Context) error {
	return g.renewSessionLeaseBatch(ctx, true)
}

// RenewDueSessionLeases performs at most one due, holder-isolated physical
// Redis pipeline. It never scans the connections map.
func (g *Gate) RenewDueSessionLeases(ctx context.Context) error {
	return g.renewSessionLeaseBatch(ctx, false)
}

func (g *Gate) renewSessionLeaseBatch(ctx context.Context, force bool) error {
	g.routeLeaseRenewMu.Lock()
	defer g.routeLeaseRenewMu.Unlock()
	if g.auth == nil || g.routeLeasePlanner == nil {
		return nil
	}
	renewStarted := g.leaseNow()
	batch := g.routeLeasePlanner.Plan(renewStarted, force)
	if len(batch.Items) == 0 {
		g.observeLeaseBacklog()
		return nil
	}
	g.observeLeasePlanned("session_route", len(batch.Items), batch.EstimatedBytes, batch.MaxDueLag, batch.Oversize)

	routes := make([]session.ActiveRoute, 0, len(batch.Items))
	items := make([]leasebatch.Item[routeLeaseCandidate], 0, len(batch.Items))
	prePartial := false
	g.mu.Lock()
	for _, item := range batch.Items {
		candidate := item.Payload
		current, ok := g.connections[candidate.route.ConnectionID]
		if !ok || current.binding == nil || !sameRouteRenewFence(current.binding.Route, candidate.route) ||
			current.admission != laneAdmissible || !current.routeLeaseDeadline.Equal(candidate.deadline) {
			g.observeLeaseStaleNonResurrectionLocked("session_route")
			prePartial = true
			continue
		}
		if candidate.deadline.IsZero() || !renewStarted.Before(candidate.deadline) {
			current.admission = laneRouteLeaseExpired
			g.revokeDownlinkTargetLocked(current)
			g.connections[candidate.route.ConnectionID] = current
			g.observeLeaseEventLocked("session_route", "lost", 1)
			prePartial = true
			continue
		}
		routes = append(routes, candidate.route)
		items = append(items, item)
	}
	g.mu.Unlock()
	if len(routes) == 0 {
		if prePartial {
			g.observeLeaseEvent("session_route", "partial", 1)
		}
		g.observeLeaseBacklog()
		return nil
	}

	dispatchCtx, cancel := context.WithTimeout(ctx, g.routeLeaseConfig.Batch.MaxDispatchDuration)
	dispatchStarted := g.leaseNow()
	g.observeLeaseEvent("session_route", "attempted", len(routes))
	settleInFlight := g.observeLeaseInFlight("session_route", len(routes))
	defer settleInFlight()
	results, dispatchErr := g.auth.RenewRoutes(dispatchCtx, routes)
	cancel()
	dispatchFinished := g.leaseNow()
	g.observeLeaseDispatch("session_route", len(routes), batch.EstimatedBytes, dispatchFinished.Sub(dispatchStarted))

	var firstErr error
	if dispatchErr != nil {
		firstErr = dispatchErr
	} else if len(results) != len(routes) {
		firstErr = fmt.Errorf("gate: route lease renewal result count=%d want=%d", len(results), len(routes))
	}
	partial := prePartial || dispatchErr != nil || len(results) != len(routes)
	g.mu.Lock()
	for index, item := range items {
		candidate := item.Payload
		current, ok := g.connections[candidate.route.ConnectionID]
		if g.leaseSchedulersDraining || !ok || current.binding == nil || !sameRouteRenewFence(current.binding.Route, candidate.route) ||
			current.admission != laneAdmissible || !current.routeLeaseDeadline.Equal(candidate.deadline) {
			g.observeLeaseStaleNonResurrectionLocked("session_route")
			partial = true
			continue
		}
		if !dispatchFinished.Before(candidate.deadline) {
			current.admission = laneRouteLeaseExpired
			g.revokeDownlinkTargetLocked(current)
			g.connections[candidate.route.ConnectionID] = current
			g.observeLeaseEventLocked("session_route", "lost", 1)
			partial = true
			continue
		}
		if index >= len(results) {
			g.rescheduleRouteTransientLocked(item, dispatchFinished)
			g.observeLeaseEventLocked("session_route", "transient", 1)
			continue
		}
		result := results[index]
		if result.Transient {
			g.rescheduleRouteTransientLocked(item, dispatchFinished)
			g.observeLeaseEventLocked("session_route", "transient", 1)
			partial = true
			continue
		}
		if !sameRouteRenewFence(result.Route, candidate.route) || (result.Renewed && result.LeaseExpired) {
			g.rescheduleRouteTransientLocked(item, dispatchFinished)
			g.observeLeaseEventLocked("session_route", "transient", 1)
			partial = true
			if firstErr == nil {
				firstErr = fmt.Errorf("gate: route lease renewal malformed at index %d", index)
			}
			continue
		}
		if !result.Renewed {
			if result.LeaseExpired {
				current.admission = laneRouteLeaseExpired
			} else {
				current.admission = laneStale
			}
			g.revokeDownlinkTargetLocked(current)
			g.connections[candidate.route.ConnectionID] = current
			g.observeLeaseEventLocked("session_route", "lost", 1)
			partial = true
			continue
		}
		deadline, valid := conservativeRouteLeaseDeadline(renewStarted,
			result.AcceptedLeaseTTL, g.routeLeaseSafetyMargin)
		if result.AcceptedLeaseTTL > g.auth.RouteLeaseTTL() || !valid || !dispatchFinished.Before(deadline) ||
			g.routeLeaseConfig.Batch.ValidateSafety(result.AcceptedLeaseTTL, g.routeLeaseSafetyMargin) != nil {
			g.rescheduleRouteTransientLocked(item, dispatchFinished)
			g.observeLeaseEventLocked("session_route", "transient", 1)
			partial = true
			if firstErr == nil {
				firstErr = fmt.Errorf("gate: route lease renewal invalid accepted ttl at index %d", index)
			}
			continue
		}
		current.binding.Route = result.Route
		current.routeLeaseDeadline = deadline
		g.connections[candidate.route.ConnectionID] = current
		g.scheduleRouteLeaseLocked(result.Route, deadline, dispatchFinished)
		g.observeLeaseEventLocked("session_route", "succeeded", 1)
	}
	g.mu.Unlock()
	if partial {
		g.observeLeaseEvent("session_route", "partial", 1)
	}
	g.observeLeaseBacklog()
	return firstErr
}

func (g *Gate) rescheduleRouteTransientLocked(item leasebatch.Item[routeLeaseCandidate], now time.Time) {
	candidate := item.Payload
	due := now.Add(g.routeLeaseConfig.TransientBackoff)
	latest := candidate.deadline.Add(-g.routeLeaseConfig.Batch.MaxDispatchDuration)
	if due.After(latest) {
		due = candidate.deadline
	}
	item.Due = due
	_ = g.routeLeasePlanner.Upsert(item)
}

func sameRouteRenewFence(a, b session.ActiveRoute) bool {
	return a.PlayerID == b.PlayerID &&
		a.GatewayID == b.GatewayID && a.GatewayBootID == b.GatewayBootID &&
		a.ConnectionID == b.ConnectionID && a.ConnectionGeneration == b.ConnectionGeneration &&
		a.SessionEpoch == b.SessionEpoch && a.RouteRevision == b.RouteRevision
}

// HandleBinary consumes one complete transport binary message and returns one
// complete response message. It is deliberately transport-neutral and is used by
// FakeTransport as well as the in-process gateproxy transport.
func (g *Gate) HandleBinary(ctx context.Context, connectionID string, generation uint64, raw []byte) ([]byte, error) {
	frame, err := protocol.Decode(raw, g.maxFrame)
	if err != nil {
		if errors.Is(err, protocol.ErrBadVersion) && g.metrics != nil {
			var probe commonpb.FarmFrame
			if proto.Unmarshal(raw, &probe) == nil {
				g.metrics.ProtocolVersionRejected.WithLabelValues(
					fmt.Sprintf("%d", probe.ProtocolVersion)).Inc()
			}
		}
		return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	// 28 §2:方向违规(ServerHello/CommandResult/ServerPush 或服务端 Control)
	// 是协议违规,不能仅因为 oneof 能解码就接受。
	if err := protocol.ValidateClientFrame(frame); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	g.mu.Lock()
	if frame.GetClientHello() != nil {
		if current, ok := g.connections[connectionID]; ok && current.generation == generation && current.binding != nil {
			g.mu.Unlock()
			return nil, fmt.Errorf("%w: duplicate ClientHello", ErrBadMessage)
		}
	}
	// Contract 28 L2-PROTO-11/30: after the first accepted frame on this
	// generation, every subsequent frame must be exactly lastSeq+1 (no skip,
	// no rewind). The initial ClientHello establishes lastSeq with no prior.
	if current, ok := g.connections[connectionID]; ok && current.generation == generation {
		want := current.lastSeq + 1
		if frame.Sequence != want {
			g.mu.Unlock()
			return nil, fmt.Errorf("%w: connection sequence must be last+1 (got %d, want %d)",
				ErrBadMessage, frame.Sequence, want)
		}
		current.lastSeq = frame.Sequence
		g.connections[connectionID] = current
	}
	g.mu.Unlock()
	switch body := frame.GetFrameBody().(type) {
	case *commonpb.FarmFrame_ClientHello:
		if !g.BootLeaseTrusted() {
			g.observeCommandRejected(ReasonGateBootLeaseExpired)
			return nil, ErrGateBootLeaseExpired
		}
		return g.handleHello(ctx, connectionID, generation, frame.Sequence, body.ClientHello)
	case *commonpb.FarmFrame_ClientCommand:
		return g.handleCommand(ctx, connectionID, generation, frame.Sequence, body.ClientCommand)
	case *commonpb.FarmFrame_Control:
		return g.handleControl(connectionID, generation, body.Control)
	default:
		return nil, fmt.Errorf("%w: frame category %T", ErrBadMessage, frame.GetFrameBody())
	}
}

// handleControl applies connection-scoped lifecycle controls in the same FIFO
// stream as ClientCommand. ClientRecoveryComplete therefore linearizes before
// every later write frame without an extra acknowledgement round trip.
func (g *Gate) handleControl(connectionID string, generation uint64, control *commonpb.ControlFrame) ([]byte, error) {
	if control == nil || control.GetClientRecoveryComplete() == nil {
		return nil, fmt.Errorf("%w: invalid control frame", ErrBadMessage)
	}

	// Admission check and state transition share g.mu so a concurrent KICK or
	// generation replacement has a single, deterministic order with the mark.
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		g.mu.Unlock()
		return nil, ErrUnauthenticated
	}
	if current.admission == laneStale {
		g.mu.Unlock()
		g.observeCommandRejected(ReasonStaleSession)
		return nil, ErrStaleSession
	}
	if current.admission == laneRouteLeaseExpired || current.routeLeaseDeadline.IsZero() ||
		!time.Now().Before(current.routeLeaseDeadline) {
		current.admission = laneRouteLeaseExpired
		g.removeRouteLeaseLocked(current.binding.Route)
		g.revokeDownlinkTargetLocked(current)
		g.connections[connectionID] = current
		g.mu.Unlock()
		g.observeCommandRejected(ReasonRouteLeaseExpired)
		return nil, ErrRouteLeaseExpired
	}
	// A client marker is only the final edge of the recovery handshake. Gate
	// must independently prove that the initial own-farm Join already exists;
	// otherwise a custom client could send the empty marker immediately after
	// ClientHello and bypass the server-side Join prerequisite.
	//
	// ActiveSubscription is the steady-state proof. Under load,
	// SUBSCRIPTION_READY is enqueued (and can reach the client) before
	// promoteJoin publishes currentSubscriptionID. A pending accepted own-farm
	// Join is the same server-side milestone; treating it as ineligible closes
	// the WebSocket and drops a player who already completed the Join barrier.
	if !ownFarmRecoveryEligible(current) {
		g.mu.Unlock()
		g.observeCommandRejected(ReasonRecoveryIncomplete)
		return nil, ErrRecoveryIncomplete
	}
	if current.currentSubscriptionID == "" {
		applyPendingOwnFarmJoin(&current)
	}
	current.recoveryComplete = true
	g.connections[connectionID] = current
	g.mu.Unlock()
	return nil, nil
}

func ownFarmRecoveryEligible(current connection) bool {
	if current.binding == nil {
		return false
	}
	playerID := current.binding.PlayerID
	if current.currentFarmID != 0 && current.currentFarmID == playerID &&
		current.currentSubscriptionID != "" && current.currentJoinAttemptID != "" {
		return true
	}
	pending := current.pendingJoin
	if pending == nil || pending.state != pendingJoinPending || !pending.accepted ||
		pending.leaseTerminal || pending.farmID != playerID ||
		pending.subscriptionID == "" || pending.attemptID == "" {
		return false
	}
	return !pending.leaseDeadline.IsZero() && time.Now().Before(pending.leaseDeadline)
}

func applyPendingOwnFarmJoin(current *connection) {
	pending := current.pendingJoin
	if pending == nil {
		return
	}
	current.currentFarmID = pending.farmID
	current.currentSubscriptionID = pending.subscriptionID
	current.currentJoinAttemptID = pending.attemptID
	current.subscriptionLeaseDeadline = pending.leaseDeadline
	current.subscriptionLeaseTerminal = false
}

func (g *Gate) handleHello(ctx context.Context, connectionID string, generation uint64, seq uint64, hello *commonpb.ClientHello) ([]byte, error) {
	if g.auth == nil {
		return nil, ErrUnauthenticated
	}
	if hello == nil || hello.ConnectionTicket == "" || !validClientBuild(hello.ClientBuild) {
		return nil, ErrUnauthenticated
	}
	activationStarted := time.Now()
	authentication, err := g.auth.AuthenticateConnection(ctx, hello.ConnectionTicket, session.RouteFence{
		GatewayID: g.auth.GatewayID(), GatewayBootID: g.auth.GatewayBootID(),
		ConnectionID: connectionID, ConnectionGeneration: generation,
	})
	if err != nil {
		return nil, ErrUnauthenticated
	}
	binding := authentication.Binding
	g.mu.Lock()
	routeLeaseDeadline, validLease := conservativeRouteLeaseDeadline(
		activationStarted, authentication.AcceptedRouteLeaseTTL, g.routeLeaseSafetyMargin,
	)
	if !validLease || authentication.AcceptedRouteLeaseTTL > g.auth.RouteLeaseTTL() ||
		g.routeLeaseConfig.Batch.ValidateSafety(authentication.AcceptedRouteLeaseTTL, g.routeLeaseSafetyMargin) != nil ||
		!leaseDeadlineHasSchedulingWindow(time.Now(), routeLeaseDeadline, g.routeLeaseConfig.Batch) {
		g.mu.Unlock()
		_, _ = g.auth.CompareAndClearRoute(ctx, binding)
		return nil, ErrUnauthenticated
	}
	if previous, ok := g.connections[connectionID]; ok && previous.generation != generation {
		if previous.binding != nil && previous.binding.PlayerID == binding.PlayerID &&
			!sessionBindingPrecedes(previous.binding, binding) {
			g.mu.Unlock()
			return nil, ErrUnauthenticated
		}
		g.removeConnectionLeaseSchedulesLocked(connectionID, previous)
		g.removePlayerConnectionLocked(connectionID, previous)
		if previous.writer != nil {
			previous.writer.Close()
		}
	}
	if !g.stalePlayerConnectionLocked(binding, connectionID, generation) {
		g.mu.Unlock()
		return nil, ErrUnauthenticated
	}
	writer := downlink.NewConnectionWriterWithCapacity(generation, g.downlinkWriterCapacity)
	g.connections[connectionID] = connection{
		generation: generation, binding: binding, lastSeq: seq, serverSeq: 2, writer: writer,
		routeLeaseDeadline: routeLeaseDeadline,
	}
	if g.playerConnections == nil {
		g.playerConnections = make(map[uint64]localConnectionFence)
	}
	g.playerConnections[binding.PlayerID] = localConnectionFence{
		connectionID: connectionID,
		generation:   generation,
	}
	g.scheduleRouteLeaseLocked(binding.Route, routeLeaseDeadline, activationStarted)
	registry := g.downlink
	preAuthController := g.preAuth
	g.mu.Unlock()
	if preAuthController != nil {
		preAuthController.Authenticate(connectionID, generation)
	}
	if registry != nil {
		// Bind the replacement before issuing the exact old-route KICK below.
		// The request carries the old connection fence, so it cannot target this
		// new writer even when connection IDs are later reused.
		registry.Bind(connectionID, "", generation, writer)
		registry.BindPlayer(connectionID, generation, binding.PlayerID)
		registry.BindSessionTarget(&logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: binding.PlayerID, GatewayId: binding.Route.GatewayID,
			GatewayBootId: binding.Route.GatewayBootID, ConnectionId: connectionID,
			ConnectionGeneration: generation, SessionEpoch: binding.ActiveSessionEpoch,
			SessionRouteRevision: binding.Route.RouteRevision,
		})
		registry.SetNextSequence(connectionID, generation, 2)
	}
	if old := authentication.OldRoute; old != nil {
		if old.GatewayID == binding.Route.GatewayID && old.GatewayBootID == binding.Route.GatewayBootID {
			_, _ = g.KickSession(ctx, kickRequest(old))
		} else {
			g.mu.Lock()
			kicker := g.crossGateKicker
			kickTimeout := g.crossGateKickTimeout
			g.mu.Unlock()
			if kicker != nil {
				oldRoute := *old
				go func() {
					if kickTimeout <= 0 {
						kickTimeout = defaultCrossGateKickTimeout
					}
					kickCtx, cancel := context.WithTimeout(context.Background(), kickTimeout)
					defer cancel()
					if err := kicker.KickOldSession(kickCtx, oldRoute); err != nil {
						log.Printf("cross-gate kick player=%d old_gate=%s old_conn=%s: %v",
							oldRoute.PlayerID, oldRoute.GatewayID, oldRoute.ConnectionID, err)
					}
				}()
			} else {
				log.Printf("cross-gate kick skipped, no kicker configured: player=%d old_gate=%s old_conn=%s",
					old.PlayerID, old.GatewayID, old.ConnectionID)
			}
		}
	}
	serverHello := &commonpb.ServerHello{SessionId: binding.SessionID, ActiveSessionEpoch: binding.ActiveSessionEpoch,
		SessionAuthorityEpoch: binding.SessionAuthorityEpoch, ServerTimeMs: time.Now().UnixMilli(),
		MaxFrameBytes: uint32(g.maxFrame), PlayerId: binding.PlayerID}
	return protocol.Encode(protocol.NewServerHelloFrame(1, serverHello), g.maxFrame)
}

// stalePlayerConnectionLocked fences the currently indexed lane for the
// incoming player without scanning every live connection. It returns false
// when a newer or equal authority result is already installed, which prevents
// authentication completion order from being reversed by mutex acquisition
// order. Callers must hold g.mu.
func (g *Gate) stalePlayerConnectionLocked(incoming *auth.SessionBinding, replacementID string, replacementGeneration uint64) bool {
	if incoming == nil {
		return false
	}
	if g.playerConnections == nil {
		return true
	}
	playerID := incoming.PlayerID
	fence, ok := g.playerConnections[playerID]
	if !ok || (fence.connectionID == replacementID && fence.generation == replacementGeneration) {
		return true
	}
	existing, ok := g.connections[fence.connectionID]
	if !ok || existing.generation != fence.generation || existing.binding == nil ||
		existing.binding.PlayerID != playerID {
		// The secondary index is only an acceleration structure. Drop a stale
		// pointer rather than applying a fence to an unproven connection.
		delete(g.playerConnections, playerID)
		return true
	}
	if !sessionBindingPrecedes(existing.binding, incoming) {
		return false
	}
	existing.admission = laneStale
	g.removeConnectionLeaseSchedulesLocked(fence.connectionID, existing)
	g.connections[fence.connectionID] = existing
	return true
}

func sessionBindingPrecedes(current, incoming *auth.SessionBinding) bool {
	if current == nil || incoming == nil || current.PlayerID != incoming.PlayerID {
		return false
	}
	if current.SessionAuthorityEpoch != incoming.SessionAuthorityEpoch {
		return current.SessionAuthorityEpoch < incoming.SessionAuthorityEpoch
	}
	if current.ActiveSessionEpoch != incoming.ActiveSessionEpoch {
		return current.ActiveSessionEpoch < incoming.ActiveSessionEpoch
	}
	return current.Route.RouteRevision < incoming.Route.RouteRevision
}

// removePlayerConnectionLocked removes the secondary index only when it still
// names this exact connection generation. Callers must hold g.mu.
func (g *Gate) removePlayerConnectionLocked(connectionID string, current connection) {
	if current.binding == nil || g.playerConnections == nil {
		return
	}
	fence, ok := g.playerConnections[current.binding.PlayerID]
	if ok && fence.connectionID == connectionID && fence.generation == current.generation {
		delete(g.playerConnections, current.binding.PlayerID)
	}
}

func kickRequest(route *session.ActiveRoute) *logicrpcpb.KickSessionRequest {
	if route == nil {
		return nil
	}
	return &logicrpcpb.KickSessionRequest{
		PlayerId: route.PlayerID, ActiveSessionEpoch: route.SessionEpoch,
		GatewayId: route.GatewayID, GatewayBootId: route.GatewayBootID,
		ConnectionId: route.ConnectionID, ConnectionGeneration: route.ConnectionGeneration,
		RouteRevision: route.RouteRevision,
	}
}

// KickSession implements the internal GateControlService. It marks the exact
// old SessionLane STALE_SESSION before best-effort UX notification; Redis
// active route activation remains the security and linearization boundary.
func (g *Gate) KickSession(_ context.Context, req *logicrpcpb.KickSessionRequest) (*logicrpcpb.KickSessionResponse, error) {
	if req == nil || req.PlayerId == 0 || req.ConnectionId == "" {
		return &logicrpcpb.KickSessionResponse{Status: logicrpcpb.GateKickStatus_STALE_TARGET}, nil
	}
	g.mu.Lock()
	current, ok := g.connections[req.ConnectionId]
	if !ok {
		g.mu.Unlock()
		return &logicrpcpb.KickSessionResponse{Status: logicrpcpb.GateKickStatus_TARGET_NOT_FOUND}, nil
	}
	binding := current.binding
	if current.generation != req.ConnectionGeneration || binding == nil ||
		binding.PlayerID != req.PlayerId ||
		binding.ActiveSessionEpoch != req.ActiveSessionEpoch ||
		binding.Route.GatewayID != req.GatewayId ||
		binding.Route.GatewayBootID != req.GatewayBootId ||
		binding.Route.RouteRevision != req.RouteRevision {
		g.mu.Unlock()
		return &logicrpcpb.KickSessionResponse{Status: logicrpcpb.GateKickStatus_STALE_TARGET}, nil
	}
	current.admission = laneStale
	g.removeConnectionLeaseSchedulesLocked(req.ConnectionId, current)
	g.connections[req.ConnectionId] = current
	registry := g.downlink
	g.mu.Unlock()
	notified := false
	if registry != nil {
		notified = registry.NotifySessionFence(req.PlayerId, req.ConnectionId, req.ConnectionGeneration)
	}
	log.Printf("KickSession KICKED player=%d conn=%s gen=%d reason=STALE_SESSION kick_push_enqueued=%v",
		req.PlayerId, req.ConnectionId, req.ConnectionGeneration, notified)
	return &logicrpcpb.KickSessionResponse{Status: logicrpcpb.GateKickStatus_KICKED}, nil
}

func validClientBuild(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && len(trimmed) <= maxClientBuildBytes
}

// rejectCommandAdmission distinguishes STALE_SESSION from ROUTE_LEASE_EXPIRED
// against the canonical connection state. Kick / renew fence reject wins over
// a concurrent local clock expiry, and crossing the local deadline permanently
// closes admission for this connection generation.
func (g *Gate) rejectCommandAdmission(connectionID string, generation uint64) error {
	now := time.Now()
	g.mu.Lock()
	c, ok := g.connections[connectionID]
	if !ok || c.generation != generation || c.binding == nil {
		g.mu.Unlock()
		return ErrUnauthenticated
	}
	if c.admission == laneStale {
		g.mu.Unlock()
		g.observeCommandRejected(ReasonStaleSession)
		log.Printf("command rejected reason=%s conn=%s gen=%d", ReasonStaleSession, connectionID, generation)
		return ErrStaleSession
	}
	if c.admission == laneRouteLeaseExpired || c.routeLeaseDeadline.IsZero() ||
		!now.Before(c.routeLeaseDeadline) {
		c.admission = laneRouteLeaseExpired
		g.removeRouteLeaseLocked(c.binding.Route)
		g.revokeDownlinkTargetLocked(c)
		g.connections[connectionID] = c
		g.mu.Unlock()
		g.observeCommandRejected(ReasonRouteLeaseExpired)
		log.Printf("command rejected reason=%s conn=%s gen=%d", ReasonRouteLeaseExpired, connectionID, generation)
		return ErrRouteLeaseExpired
	}
	g.mu.Unlock()
	return nil
}

func (g *Gate) rejectGateBootLeaseAdmission(connectionID string, generation uint64,
	spec commandcatalog.CommandSpec) error {
	if spec.PermitClass != commandcatalog.PermitOwnerWrite || g.BootLeaseTrusted() {
		return nil
	}
	g.observeCommandRejected(ReasonGateBootLeaseExpired)
	log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s",
		ReasonGateBootLeaseExpired, connectionID, generation, spec.CommandType)
	return ErrGateBootLeaseExpired
}

// rejectSessionTargetPolicy enforces SessionAdmissionPolicy target_farm_policy
// (28 §6.1 Session 准入与目标策略). Target comes only from the decoded Typed
// Payload;the operator identity always comes from the trusted Session.
func (g *Gate) rejectSessionTargetPolicy(connectionID string, generation uint64, c connection,
	spec commandcatalog.CommandSpec, decoded proto.Message) error {
	policy := spec.SessionAdmission.TargetFarmPolicy
	target, hasTarget := commandcatalog.TargetFarmIDFromPayload(decoded)
	subscriptionRequired := func() error {
		g.observeCommandRejected(ReasonSubscriptionRequired)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s",
			ReasonSubscriptionRequired, connectionID, generation, spec.CommandType)
		return fmt.Errorf("%w: %s requires current subscription", ErrBadMessage, policy)
	}
	switch policy {
	case commandcatalog.TargetFarmOwnCurrentFarm:
		if c.currentFarmID == 0 {
			return subscriptionRequired()
		}
		if !hasTarget || target != c.binding.PlayerID || target != c.currentFarmID {
			g.observeCommandRejected(ReasonOwnCurrentFarmMismatch)
			log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s target=%d player=%d current_farm=%d",
				ReasonOwnCurrentFarmMismatch, connectionID, generation, spec.CommandType,
				target, c.binding.PlayerID, c.currentFarmID)
			return fmt.Errorf("%w: own-current-farm target mismatch", ErrBadMessage)
		}
	case commandcatalog.TargetFarmCurrentFarm:
		if c.currentFarmID == 0 {
			return subscriptionRequired()
		}
		if !hasTarget || target != c.currentFarmID {
			g.observeCommandRejected(ReasonCurrentFarmMismatch)
			log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s target=%d current_farm=%d",
				ReasonCurrentFarmMismatch, connectionID, generation, spec.CommandType,
				target, c.currentFarmID)
			return fmt.Errorf("%w: current-farm target mismatch", ErrBadMessage)
		}
	case commandcatalog.TargetFarmCurrentFriendFarm:
		if c.currentFarmID == 0 {
			return subscriptionRequired()
		}
		if !hasTarget || target != c.currentFarmID || target == c.binding.PlayerID {
			g.observeCommandRejected(ReasonCurrentFarmMismatch)
			log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s target=%d player=%d current_farm=%d",
				ReasonCurrentFarmMismatch, connectionID, generation, spec.CommandType,
				target, c.binding.PlayerID, c.currentFarmID)
			return fmt.Errorf("%w: current-friend-farm target mismatch", ErrBadMessage)
		}
	default:
		// PAYLOAD_TARGET / SESSION_PLAYER / NONE / SUBJECT_ROOT_OPERATION /
		// TOKEN_INVITER / CALLER+PEER:目标约束由 FirstHop 复核;这里只执行
		// RequiresCurrentSubscription 位(如好友农场订阅类查询)。
		if spec.SessionAdmission.RequiresCurrentSubscription && c.currentFarmID == 0 {
			return subscriptionRequired()
		}
	}
	return nil
}

// backendRejectReason classifies a logic.ExecuteCommand failure into a bounded
// backend-boundary reject reason. Route/pool errors are kept separate from a
// genuine backend transport failure so a client-visible generic unavailable
// response does not erase the route convergence signal.
func backendRejectReason(err error) string {
	if err == nil {
		return ReasonBackendOutcomeInvalid
	}
	if errors.Is(err, actorrouting.ErrTransportGenerationTransition) {
		return ReasonBackendRouteGeneration
	}
	if errors.Is(err, actorrouting.ErrStaleRoute) {
		return ReasonBackendStaleRoute
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ReasonBackendRPCTimeout
	}
	return ReasonBackendRPCUnavail
}

func (g *Gate) observeCommandRejected(reason string) {
	g.mu.Lock()
	metrics := g.metrics
	g.mu.Unlock()
	if metrics == nil || metrics.CommandRejected == nil {
		return
	}
	metrics.CommandRejected.WithLabelValues(reason).Inc()
}

// observeCommandResult records the result visible to the client. Keep the
// status/error labels closed over generated protocol enums so a large bot run
// cannot create an unbounded Prometheus series from server messages.
func (g *Gate) observeCommandResult(result *commonpb.CommandResultEnvelope) {
	if result == nil {
		return
	}
	g.mu.Lock()
	metrics := g.metrics
	g.mu.Unlock()
	if metrics == nil || metrics.CommandResults == nil {
		return
	}
	errorCode := "NONE"
	if detail := result.GetError(); detail != nil && detail.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		errorCode = detail.GetErrorCode().String()
	}
	metrics.CommandResults.WithLabelValues(
		result.GetCommandType().String(), result.GetStatus().String(), errorCode,
	).Inc()
}

func (g *Gate) observeCommandAdmitted() {
	g.mu.Lock()
	metrics := g.metrics
	g.mu.Unlock()
	if metrics == nil || metrics.CommandAdmission == nil {
		return
	}
	metrics.CommandAdmission.WithLabelValues(StageAdmitted).Inc()
}

// observeBackendInFlight moves the backend RPC slot gauge by delta (+1 on
// acquire, -1 on release).
func (g *Gate) observeBackendInFlight(delta float64) {
	g.mu.Lock()
	metrics := g.metrics
	g.mu.Unlock()
	if metrics == nil || metrics.BackendInFlight == nil {
		return
	}
	metrics.BackendInFlight.Add(delta)
}

// observeBackendRejected counts a command that never reached the backend or
// failed at the backend boundary (slot-full busy, RPC timeout/unavailable,
// invalid outcome).
func (g *Gate) observeBackendRejected(reason string) {
	g.mu.Lock()
	metrics := g.metrics
	g.mu.Unlock()
	if metrics == nil || metrics.BackendRejected == nil {
		return
	}
	metrics.BackendRejected.WithLabelValues(reason).Inc()
}

func (g *Gate) handleCommand(ctx context.Context, connectionID string, generation uint64, seq uint64, cmd *commonpb.ClientCommandEnvelope) ([]byte, error) {
	g.mu.Lock()
	c, ok := g.connections[connectionID]
	g.mu.Unlock()
	if !ok || c.generation != generation || c.binding == nil {
		return nil, ErrUnauthenticated
	}
	if err := g.rejectCommandAdmission(connectionID, generation); err != nil {
		return nil, err
	}
	pipelineStart := time.Now()
	commandTypeName := cmd.CommandType.String()
	spec, ok := commandcatalog.Lookup(cmd.CommandType)
	if !ok {
		g.observeCommandRejected(ReasonUnknownCommand)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s",
			ReasonUnknownCommand, connectionID, generation, cmd.CommandType)
		return nil, fmt.Errorf("%w: unregistered command type %s", ErrBadMessage, cmd.CommandType)
	}
	// 28 §6.1 AvailabilityPolicy:已注册合同但业务 Handler 尚未接线的命令在
	// 准入阶段明确拒绝(不路由、不伪造业务成功)。
	if !spec.Availability.Enabled {
		g.observeCommandRejected(ReasonCommandUnimplemented)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s feature=%s",
			ReasonCommandUnimplemented, connectionID, generation, cmd.CommandType, spec.Availability.FeatureKey)
		return g.encodeCommandRejected(connectionID, generation, cmd,
			commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "命令已登记但尚未开放")
	}
	if err := g.rejectGateBootLeaseAdmission(connectionID, generation, spec); err != nil {
		return nil, err
	}
	if spec.Idempotency.RequiresOperationID() && strings.TrimSpace(cmd.OperationId) == "" {
		return nil, fmt.Errorf("%w: operation_id required for %s", ErrBadMessage, cmd.CommandType)
	}
	// 1.5: payload size before typed decode (RateLimitPolicy intentionally not
	// enforced — Spec field + TODO(32) only; no invented thresholds).
	if commandcatalog.PayloadExceedsLimit(spec.MaxPayloadBytes, len(cmd.Payload)) {
		g.observeCommandRejected(ReasonPayloadTooLarge)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s payload_bytes=%d max=%d",
			ReasonPayloadTooLarge, connectionID, generation, cmd.CommandType,
			len(cmd.Payload), spec.MaxPayloadBytes)
		return nil, fmt.Errorf("%w: payload too large", ErrBadMessage)
	}
	// 28 §3.6:Gate 按 CommandSpec 解码一次并保留原始 bytes;FirstHop 独立
	// 再解码。本层不重新编码 Typed Payload。
	decoded, err := spec.Decoder(cmd.Payload)
	if err != nil {
		g.observeCommandRejected(ReasonPayloadDecodeFail)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s: %v",
			ReasonPayloadDecodeFail, connectionID, generation, cmd.CommandType, err)
		return nil, fmt.Errorf("%w: typed payload: %v", ErrBadMessage, err)
	}
	// 28 §6 SharedCommandSchema.StructuralValidator:已注册但尚未接线的功能
	// 切片(如 CARE 的非 WATER Incident)fail closed,不得伪造业务成功。
	if spec.Validator != nil {
		if validateErr := spec.Validator(decoded); validateErr != nil {
			if errors.Is(validateErr, commandcatalog.ErrFeatureDisabled) {
				g.observeCommandRejected(ReasonCommandUnimplemented)
				return g.encodeCommandRejected(connectionID, generation, cmd,
					commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "该命令的此功能尚未开放")
			}
			g.observeCommandRejected(ReasonPayloadDecodeFail)
			return nil, fmt.Errorf("%w: payload validation: %v", ErrBadMessage, validateErr)
		}
	}
	if err := g.rejectSessionTargetPolicy(connectionID, generation, c, spec, decoded); err != nil {
		return nil, err
	}
	if g.logic == nil {
		return nil, errors.New("gate: logic unavailable")
	}
	g.mu.Lock()
	backendSlots := g.backendSlots
	rpcTimeout := g.backendRPCTimeout
	g.mu.Unlock()
	if !g.acquireBackendSlot(ctx, backendSlots) {
		g.observeBackendRejected(ReasonBackendSlotsFull)
		return g.encodeCommandUnavailable(connectionID, generation, cmd,
			commonpb.ErrorCode_ERR_SERVER_BUSY, "服务器繁忙，请稍后重试")
	}
	g.observeBackendInFlight(1)
	defer func() {
		<-backendSlots
		g.observeBackendInFlight(-1)
	}()
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	// 28 §3.6:Gate 按 Catalog 从 Typed Payload / 可信 Session 提取 RouteKey
	// 用于本地准入与 DispatchContext;Envelope 原样透传给 FirstHop,不做任何
	// 回填或重编码。FirstHop 独立重算同一 RouteKey 并复核(29 §5)。
	var routeKey uint64
	if spec.RouteMode == commandcatalog.RouteModeActorAffinity && spec.RouteKeyExtractor != nil {
		key, keyErr := spec.RouteKeyExtractor(decoded, c.binding.PlayerID)
		if keyErr != nil {
			g.observeCommandRejected(ReasonRouteKeyInvalid)
			log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s: %v",
				ReasonRouteKeyInvalid, connectionID, generation, cmd.CommandType, keyErr)
			return nil, fmt.Errorf("%w: route key: %v", ErrBadMessage, keyErr)
		}
		routeKey = key
	}
	if cmd.CommandType == commonpb.CommandType_JOIN_FARM {
		var err error
		if c, err = g.revokeCurrentSubscriptionForSwitch(connectionID, generation, routeKey); err != nil {
			code, message := backendUnavailable(rpcCtx, "切换农场失败，请稍后重试", false)
			return g.encodeCommandUnavailable(connectionID, generation, cmd, code, message)
		}
	}
	pendingJoinPrepared := false
	if cmd.CommandType == commonpb.CommandType_JOIN_FARM {
		if !g.prepareJoin(connectionID, generation, cmd.OperationId, routeKey) {
			return g.encodeCommandUnavailable(connectionID, generation, cmd,
				commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, "无法建立 Join 屏障")
		}
		pendingJoinPrepared = true
		// Close the narrow race where another Join promoted a different farm
		// after the first current-state read but before this PendingJoin was
		// installed. prepareJoin has already replaced that old attempt, so this
		// second exact local revoke cannot be followed by its stale promotion.
		var err error
		if c, err = g.revokeCurrentSubscriptionForSwitch(connectionID, generation, routeKey); err != nil {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
			code, message := backendUnavailable(rpcCtx, "切换农场失败，请稍后重试", false)
			return g.encodeCommandUnavailable(connectionID, generation, cmd, code, message)
		}
	}
	// The initial checks above intentionally do not hold g.mu across decode,
	// backend-slot admission, or switch preparation. Re-read the local fence at
	// the ADMITTED linearization point so a concurrent A -> B switch cannot let
	// an old A command enter Logic after A has already been revoked.
	var admissionErr error
	c, admissionErr = g.refreshAdmissionContext(connectionID, generation, c, spec, decoded)
	if admissionErr != nil {
		if pendingJoinPrepared {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
		}
		if errors.Is(admissionErr, ErrRecoveryIncomplete) {
			return g.encodeCommandRejected(connectionID, generation, cmd,
				commonpb.ErrorCode_ERR_RECOVERY_INCOMPLETE, "恢复尚未完成，请稍后重试")
		}
		if errors.Is(admissionErr, ErrSubscriptionLeaseExpired) {
			return g.encodeFreshJoinRequired(connectionID, generation, cmd,
				"农场订阅已失效，请重新进入农场")
		}
		return nil, admissionErr
	}
	// ADMITTED: all Gate admission checks passed; about to enter logic.ExecuteCommand.
	// 合同 29 §3.2:Gate 从本地受信 Session 生成 TrustedCommandContext;FirstHop 信任
	// 内部 RPC 服务身份,不再逐命令签发签名 Permit(短期签名 Internal Permit 是 29 §3.5
	// 的未来可选项)。
	g.observeCommandAdmitted()
	if g.metrics != nil {
		g.metrics.PrePermitSeconds.WithLabelValues(commandTypeName).Observe(time.Since(pipelineStart).Seconds())
	}
	rpcStart := time.Now()
	resp, err := g.logic.ExecuteCommand(rpcCtx, &logicrpcpb.ExecuteCommandRequest{
		Command:  cmd,
		Trusted:  g.trustedContextFor(connectionID, generation, c, cmd, spec, routeKey),
		Dispatch: dispatchForCommand(spec, c.binding.PlayerID, decoded),
		TraceId:  cmd.RequestId,
	})
	if err != nil {
		if pendingJoinPrepared {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
		}
		reason := backendRejectReason(err)
		g.observeBackendRejected(reason)
		if reason == ReasonBackendRouteGeneration || reason == ReasonBackendStaleRoute {
			// Keep the original bounded local route error in the log. The wire
			// contract remains ERR_STORE_UNAVAILABLE, but this is the evidence
			// needed to distinguish pool convergence from Redis/backend failure.
			log.Printf("gate: ExecuteCommand route failure conn=%s gen=%d command_type=%s reason=%s err=%v",
				connectionID, generation, commandTypeName, reason, err)
		}
		code, message := backendUnavailable(rpcCtx, "后端服务暂时不可用", true)
		return g.encodeCommandUnavailable(connectionID, generation, cmd, code, message)
	}
	result, outcomeErr := commandResultFromOutcome(cmd, resp)
	if outcomeErr != nil {
		if pendingJoinPrepared {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
		}
		g.observeBackendRejected(ReasonBackendOutcomeInvalid)
		log.Printf("gate: outcome invalid player=%d conn=%s gen=%d cmd=%s: %v", c.binding.PlayerID, connectionID, generation, commandTypeName, outcomeErr)
		return g.encodeCommandUnavailable(connectionID, generation, cmd,
			commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, "后端返回无效响应")
	}
	if g.metrics != nil {
		g.metrics.RPCServerSeconds.WithLabelValues(commandTypeName).Observe(time.Since(rpcStart).Seconds())
		if result.GetError().GetErrorCode() == commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
			g.metrics.ReceiptConflictLeaks.WithLabelValues(commandTypeName).Inc()
		}
	}
	postStart := time.Now()
	if cmd.CommandType == commonpb.CommandType_JOIN_FARM {
		accepted := result.Status == commonpb.CommandStatus_ACCEPTED ||
			result.Status == commonpb.CommandStatus_SUCCEEDED ||
			result.Status == commonpb.CommandStatus_NO_CHANGE
		if !accepted {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
			serverSeq := g.nextServerSequence(connectionID, generation)
			return g.encodeCommandResultResponse(serverSeq, result, g.maxFrame)
		}
		join := result.GetJoin()
		internalJoin := resp.GetBusiness().GetJoinAccepted()
		attemptID := g.pendingJoinAttemptID(connectionID, generation, cmd.OperationId, routeKey)
		if join == nil || join.TargetFarmId != routeKey || join.SubscriptionId == "" ||
			join.JoinAttemptId == "" || join.JoinAttemptId != attemptID || c.binding == nil ||
			internalJoin == nil || internalJoin.TargetFarmId != join.TargetFarmId ||
			internalJoin.SubscriptionId != join.SubscriptionId || internalJoin.JoinAttemptId != join.JoinAttemptId ||
			internalJoin.AcceptedSubscriptionTtlMs == 0 {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
			return g.encodeCommandUnavailable(connectionID, generation, cmd,
				commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, "Join ACCEPTED metadata mismatch")
		}
		g.mu.Lock()
		registry := g.downlink
		safetyMargin := g.subscriptionLeaseSafetyMargin
		g.mu.Unlock()
		leaseDeadline, validLease := conservativeSubscriptionLeaseDeadline(rpcStart,
			time.Duration(internalJoin.AcceptedSubscriptionTtlMs)*time.Millisecond, safetyMargin)
		if !validLease || !time.Now().Before(leaseDeadline) {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
			return g.encodeCommandUnavailable(connectionID, generation, cmd,
				commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, "Join subscription lease already expired")
		}
		if registry == nil || !registry.QueueJoinAccepted(connectionID, generation, cmd.OperationId,
			routeKey, attemptID, join.SubscriptionId, result) ||
			!g.recordJoinAccepted(connectionID, generation, cmd.OperationId, routeKey,
				attemptID, join.SubscriptionId, leaseDeadline) ||
			!registry.MarkSnapshotArmRequested(connectionID, generation, cmd.OperationId,
				routeKey, attemptID, join.SubscriptionId) {
			g.cancelJoin(connectionID, generation, cmd.OperationId, routeKey)
			return nil, nil
		}
		g.armJoinAsync(c.binding, generation, cmd.OperationId, &logicrpcpb.SubscriptionFence{
			FarmId: routeKey, SubscriptionId: join.SubscriptionId, JoinAttemptId: attemptID,
		})
		if g.metrics != nil {
			g.metrics.PostSeconds.WithLabelValues(commandTypeName).Observe(time.Since(postStart).Seconds())
		}
		// ACCEPTED is already in ConnectionWriter. Returning it on the transport
		// response path would create a second ordering domain.
		g.observeCommandResult(result)
		return nil, nil
	}
	serverSeq := g.nextServerSequence(connectionID, generation)
	if g.metrics != nil {
		g.metrics.PostSeconds.WithLabelValues(commandTypeName).Observe(time.Since(postStart).Seconds())
	}
	return g.encodeCommandResultResponse(serverSeq, result, g.maxFrame)
}

// trustedContextFor 按合同 29 §3.1 从 Gate 本地受信 Session 生成 ADMITTED 瞬间的
// 不可变上下文快照:JOIN_FARM 携带 JoinAdmissionContext(29 §3.4),要求当前
// Subscription 的农场命令携带 FarmAdmissionContext,玩家域命令两者都不携带。
func (g *Gate) trustedContextFor(connectionID string, generation uint64, c connection,
	cmd *commonpb.ClientCommandEnvelope, spec commandcatalog.CommandSpec, routeKey uint64) *logicrpcpb.TrustedCommandContext {
	ctx := &logicrpcpb.TrustedCommandContext{
		Session:                     sessionSnapshot(c.binding, connectionID, generation),
		RecoveryCompleteAtAdmission: c.recoveryComplete,
	}
	if cmd.CommandType == commonpb.CommandType_JOIN_FARM {
		ctx.JoinAttempt = &logicrpcpb.JoinAdmissionContext{
			JoinAttemptId: g.pendingJoinAttemptID(connectionID, generation, cmd.OperationId, routeKey),
			TargetFarmId:  routeKey,
			ForceFresh:    true,
		}
		return ctx
	}
	// 29 §3.4 + 28 §6.1:凡 RequiresCurrentSubscription 的命令都携带
	// FarmAdmissionContext(与 FirstHop 无关,如 CONFIRM_FRIEND_VISIT 虽是
	// PLAYER 路由仍需要好友农场 Fence);其余命令不附带 farm_context。
	if spec.SessionAdmission.RequiresCurrentSubscription {
		ctx.FarmContext = &logicrpcpb.FarmAdmissionContext{
			AdmittedFarmId:         c.currentFarmID,
			AdmittedSubscriptionId: c.currentSubscriptionID,
		}
	}
	return ctx
}

func (g *Gate) pendingJoinAttemptID(connectionID string, generation uint64, operationID string, farmID uint64) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.pendingJoin == nil ||
		current.pendingJoin.operationID != operationID || current.pendingJoin.farmID != farmID {
		return ""
	}
	return current.pendingJoin.attemptID
}

func sessionSnapshot(binding *auth.SessionBinding, connectionID string, generation uint64) *logicrpcpb.SessionPermitSnapshot {
	snapshot := &logicrpcpb.SessionPermitSnapshot{
		ConnectionId: connectionID, ConnectionGeneration: generation,
		AdmittedAtUnixMs: time.Now().UnixMilli(),
	}
	if binding != nil {
		snapshot.PlayerId = binding.PlayerID
		snapshot.ActiveSessionEpoch = binding.ActiveSessionEpoch
		snapshot.GatewayId = binding.Route.GatewayID
		snapshot.GatewayBootId = binding.Route.GatewayBootID
		snapshot.SessionRouteRevision = binding.Route.RouteRevision
	}
	return snapshot
}

// dispatchForCommand 按合同 29 §4 生成 DispatchContext。当前静态单 Holder 部署下
// expected_* 留空,Holder 本地 Lease/WriterEpoch 校验仍是最终裁决(29 §4.3)。
// dispatchForCommand 按 28 §6.1 的 RouteMode/RouteDomain/RouteKeyExtractor 生成
// DispatchContext(L2-PROTO-23):ACTOR_AFFINITY 必须携带 ActorRouteHeader,
// SERVICE_LB / NONE 不得伪造 Actor Route。RouteKey 只来自 Catalog 声明的
// PAYLOAD_FIELD 或 TRUSTED_CONTEXT_FIELD 来源。
func dispatchForCommand(spec commandcatalog.CommandSpec, sessionPlayerID uint64, decoded proto.Message) *logicrpcpb.DispatchContext {
	switch spec.RouteMode {
	case commandcatalog.RouteModeActorAffinity:
		actorID, err := spec.RouteKeyExtractor(decoded, sessionPlayerID)
		if err != nil {
			// 准入阶段已提取并校验过 RouteKey;此处 fail closed 为 NONE,
			// FirstHop 对无 ActorRouteHeader 的 ACTOR_AFFINITY 命令同样拒绝。
			return &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_NONE}
		}
		domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED
		switch spec.RouteDomain {
		case commandcatalog.RouteDomainPlayer:
			domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
		case commandcatalog.RouteDomainFarm:
			domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
		}
		return &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: domain, ActorId: actorID},
		}
	case commandcatalog.RouteModeServiceLB:
		return &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB}
	default:
		return &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_NONE}
	}
}

// commandResultFromOutcome 按合同 29 §9.1 用原请求的 request_id/operation_id 与内部
// 业务结果组装公网 CommandResultEnvelope;route_rejection 视为后端不可用。
func commandResultFromOutcome(cmd *commonpb.ClientCommandEnvelope, resp *logicrpcpb.ExecuteCommandResponse) (*commonpb.CommandResultEnvelope, error) {
	if resp == nil {
		return nil, errors.New("empty response")
	}
	if rejection := resp.GetRouteRejection(); rejection != nil {
		return nil, fmt.Errorf("route rejected: %s", rejection.Reason)
	}
	business := resp.GetBusiness()
	if business == nil {
		return nil, errors.New("response missing business outcome")
	}
	result := &commonpb.CommandResultEnvelope{
		OperationId:    cmd.OperationId,
		RequestId:      cmd.RequestId,
		Status:         business.Status,
		CommandType:    cmd.CommandType,
		Payload:        business.ResultPayload,
		RetryDirective: toPublicLogicRetryDirective(business.RetryDirective),
		RetryAfterMs:   business.RetryAfterMs,
	}
	if join := business.GetJoinAccepted(); join != nil {
		result.ResultContext = &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: join.TargetFarmId, SubscriptionId: join.SubscriptionId,
			JoinAttemptId: join.JoinAttemptId,
		}}
	}
	if business.Error != nil {
		result.Error = &commonpb.ErrorDetail{
			ErrorCode: business.Error.ErrorCode, ErrorMessage: business.Error.ErrorMessage,
		}
	}
	return result, nil
}

// toPublicRetryDirective 把 commandcatalog 的 Go 枚举映射为公网 28 §2.3 枚举。
func toPublicLogicRetryDirective(d logicrpcpb.RetryDirective) commonpb.RetryDirective {
	switch d {
	case logicrpcpb.RetryDirective_NONE:
		return commonpb.RetryDirective_NONE
	case logicrpcpb.RetryDirective_RETRY_SAME_OPERATION:
		return commonpb.RetryDirective_RETRY_SAME_OPERATION
	case logicrpcpb.RetryDirective_POLL_OPERATION_RESULT:
		return commonpb.RetryDirective_POLL_OPERATION_RESULT
	case logicrpcpb.RetryDirective_FRESH_JOIN:
		return commonpb.RetryDirective_FRESH_JOIN
	case logicrpcpb.RetryDirective_RECONNECT:
		return commonpb.RetryDirective_RECONNECT
	case logicrpcpb.RetryDirective_REAUTHENTICATE:
		return commonpb.RetryDirective_REAUTHENTICATE
	case logicrpcpb.RetryDirective_FRESH_JOIN_THEN_RETRY:
		return commonpb.RetryDirective_FRESH_JOIN_THEN_RETRY
	case logicrpcpb.RetryDirective_REFRESH_AUTHORITY_STATE:
		return commonpb.RetryDirective_REFRESH_AUTHORITY_STATE
	default:
		return commonpb.RetryDirective_NONE
	}
}

func toPublicRetryDirective(d commandcatalog.RetryDirective) commonpb.RetryDirective {
	switch d {
	case commandcatalog.RetryDirectiveFreshJoin:
		return commonpb.RetryDirective_FRESH_JOIN
	case commandcatalog.RetryDirectiveReconnect:
		return commonpb.RetryDirective_RECONNECT
	case commandcatalog.RetryDirectiveReauthenticate:
		return commonpb.RetryDirective_REAUTHENTICATE
	case commandcatalog.RetryDirectivePollOperationResult:
		return commonpb.RetryDirective_POLL_OPERATION_RESULT
	case commandcatalog.RetryDirectiveRetrySameOperation:
		return commonpb.RetryDirective_RETRY_SAME_OPERATION
	case commandcatalog.RetryDirectiveRefreshAuthorityState:
		return commonpb.RetryDirective_REFRESH_AUTHORITY_STATE
	default:
		return commonpb.RetryDirective_NONE
	}
}

func backendUnavailable(ctx context.Context, fallback string, submitted bool) (commonpb.ErrorCode, string) {
	if submitted && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return commonpb.ErrorCode_ERR_TIMEOUT, "请求超时，请使用原 operation_id 查询或重试"
	}
	return commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, fallback
}

// refreshAdmissionContext establishes the final local command-admission
// snapshot. subscriptionContextGeneration is a Gate-local fence: switch,
// leave, terminal rejoin, and successful Join promotion all advance it.
func (g *Gate) refreshAdmissionContext(connectionID string, generation uint64, expected connection,
	spec commandcatalog.CommandSpec, decoded proto.Message) (connection, error) {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	g.mu.Unlock()
	if !ok || current.generation != generation || current.binding == nil {
		return connection{}, ErrUnauthenticated
	}
	if err := g.rejectCommandAdmission(connectionID, generation); err != nil {
		return connection{}, err
	}
	if spec.SessionAdmission.RequiresRecoveryComplete && !current.recoveryComplete {
		g.observeCommandRejected(ReasonRecoveryIncomplete)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s",
			ReasonRecoveryIncomplete, connectionID, generation, spec.CommandType)
		return connection{}, ErrRecoveryIncomplete
	}
	if spec.SessionAdmission.RequiresCurrentSubscription &&
		(current.subscriptionLeaseTerminal || current.subscriptionLeaseDeadline.IsZero() ||
			!time.Now().Before(current.subscriptionLeaseDeadline)) {
		g.expireConnectionSubscriptionLeases(connectionID, generation, time.Now())
		return connection{}, ErrSubscriptionLeaseExpired
	}
	if spec.SessionAdmission.RequiresCurrentSubscription &&
		(current.subscriptionContextGeneration != expected.subscriptionContextGeneration ||
			current.currentFarmID != expected.currentFarmID ||
			current.currentSubscriptionID != expected.currentSubscriptionID ||
			current.currentJoinAttemptID != expected.currentJoinAttemptID ||
			current.currentPresenceEpoch != expected.currentPresenceEpoch) {
		g.observeCommandRejected(ReasonSubscriptionChanged)
		log.Printf("command rejected reason=%s conn=%s gen=%d command_type=%s",
			ReasonSubscriptionChanged, connectionID, generation, spec.CommandType)
		return connection{}, fmt.Errorf("%w: subscription changed before admission", ErrBadMessage)
	}
	if err := g.rejectSessionTargetPolicy(connectionID, generation, current, spec, decoded); err != nil {
		return connection{}, err
	}
	return current, nil
}

// revokeCurrentSubscriptionForSwitch is the SwitchFarm linearization point.
// It performs break-before-make locally and never waits for the old Farm
// Holder. Commands admitted before this point may finish; later commands using
// the old Farm fence are rejected. Exact remote cleanup is best effort and
// cannot delete the next subscription because it carries the old full fence.
func (g *Gate) revokeCurrentSubscriptionForSwitch(connectionID string, generation,
	targetFarmID uint64) (connection, error) {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		g.mu.Unlock()
		return connection{}, ErrUnauthenticated
	}
	if current.currentFarmID == 0 || current.currentFarmID == targetFarmID {
		g.mu.Unlock()
		return current, nil
	}
	oldFarmID := current.currentFarmID
	oldSubscriptionID := current.currentSubscriptionID
	oldJoinAttemptID := current.currentJoinAttemptID
	copyBinding := *current.binding
	registry := g.downlink
	if oldCandidate, candidateOK := subscriptionLeaseCandidateFromActive(connectionID, current); candidateOK {
		g.removeSubscriptionLeaseLocked(oldCandidate)
	}
	current.currentFarmID = 0
	current.currentSubscriptionID = ""
	current.currentJoinAttemptID = ""
	current.currentPresenceEpoch = 0
	current.subscriptionLeaseDeadline = time.Time{}
	current.subscriptionLeaseTerminal = false
	current.subscriptionContextGeneration++
	g.connections[connectionID] = current
	g.mu.Unlock()

	if registry != nil && oldSubscriptionID != "" {
		registry.EndSubscription(connectionID, generation, oldSubscriptionID)
	}
	if oldFarmID != 0 && oldSubscriptionID != "" && oldJoinAttemptID != "" {
		g.cleanupSubscriptionAsync(&copyBinding, generation, oldFarmID,
			oldSubscriptionID, oldJoinAttemptID, logicrpcpb.CleanupReason_CLEANUP_REASON_SWITCH_FARM)
	} else if oldSubscriptionID != "" {
		log.Printf("gate: switch revoked incomplete old subscription fence farm=%d subscription=%s conn=%s gen=%d",
			oldFarmID, oldSubscriptionID, connectionID, generation)
	}
	return current, nil
}

func (g *Gate) prepareJoin(connectionID string, generation uint64, operationID string, farmID uint64) bool {
	if operationID == "" || farmID == 0 {
		return false
	}
	// A new Join owns the connection's single PendingJoin slot. Close the old
	// intent first so its late callbacks can never promote after replacement.
	g.mu.Lock()
	old, hasOld := g.connections[connectionID]
	var oldOperation string
	var oldFarm uint64
	if hasOld && old.generation == generation && old.pendingJoin != nil && old.pendingJoin.state != pendingJoinClosed {
		// A retry of the same durable Join reuses the original two-latch
		// intent and deadline. Replacing it would leave the previous timer
		// alive and let that timer cancel the replacement prematurely.
		if old.pendingJoin.operationID == operationID && old.pendingJoin.farmID == farmID {
			g.mu.Unlock()
			return true
		}
		oldOperation, oldFarm = old.pendingJoin.operationID, old.pendingJoin.farmID
	}
	g.mu.Unlock()
	if oldOperation != "" {
		g.cancelJoin(connectionID, generation, oldOperation, oldFarm)
	}

	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		g.mu.Unlock()
		return false
	}
	timeout := g.pendingJoinTimeout
	if timeout <= 0 {
		timeout = defaultPendingJoinTimeout
	}
	attemptID := newTraceID()
	current.pendingJoin = &pendingJoinState{
		operationID: operationID, farmID: farmID, state: pendingJoinPending,
		attemptID: attemptID,
		deadline:  time.Now().Add(timeout),
	}
	g.connections[connectionID] = current
	registry := g.downlink
	g.mu.Unlock()
	if registry == nil {
		// Unit/FakeTransport compatibility. Production gatewayapp always
		// attaches ConnectionRegistry and therefore always uses the two-latch
		// barrier rather than this transport-only fallback.
		return true
	}
	if registry.PrepareJoinAttempt(connectionID, generation, operationID, farmID, attemptID) {
		timer := time.AfterFunc(timeout, func() { g.expireJoin(connectionID, generation, operationID, farmID) })
		g.mu.Lock()
		current, ok = g.connections[connectionID]
		if ok && current.generation == generation && current.pendingJoin != nil &&
			current.pendingJoin.operationID == operationID && current.pendingJoin.farmID == farmID {
			current.pendingJoin.timer = timer
			g.connections[connectionID] = current
		} else {
			timer.Stop()
		}
		g.mu.Unlock()
		return true
	}
	g.mu.Lock()
	current, ok = g.connections[connectionID]
	if ok && current.generation == generation && current.pendingJoin != nil &&
		current.pendingJoin.operationID == operationID && current.pendingJoin.farmID == farmID {
		current.pendingJoin = nil
		g.connections[connectionID] = current
	}
	g.mu.Unlock()
	return false
}

func (g *Gate) acceptJoin(connectionID string, generation uint64, operationID string, farmID uint64, subscriptionID string) bool {
	if subscriptionID == "" {
		return false
	}
	partition := g.resolveSubscriptionLeasePartition(farmID)
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.pendingJoin == nil ||
		current.pendingJoin.state != pendingJoinPending ||
		current.pendingJoin.operationID != operationID || current.pendingJoin.farmID != farmID {
		g.mu.Unlock()
		return false
	}
	current.pendingJoin.subscriptionID = subscriptionID
	current.pendingJoin.accepted = true
	leaseTTL := g.subscriptionLeaseTTL
	safetyMargin := g.subscriptionLeaseSafetyMargin
	if leaseTTL <= 0 {
		leaseTTL = defaultSubscriptionLeaseTTL
	}
	now := time.Now()
	current.pendingJoin.leaseDeadline, _ = conservativeSubscriptionLeaseDeadline(now, leaseTTL, safetyMargin)
	current.pendingJoin.leaseTerminal = false
	registry := g.downlink
	if registry == nil {
		if current.pendingJoin.timer != nil {
			current.pendingJoin.timer.Stop()
		}
		current.currentFarmID = farmID
		current.currentSubscriptionID = subscriptionID
		current.currentJoinAttemptID = current.pendingJoin.attemptID
		current.currentPresenceEpoch = 0
		current.subscriptionLeaseDeadline = current.pendingJoin.leaseDeadline
		current.subscriptionLeaseTerminal = false
		current.pendingJoin.state = pendingJoinClosed
		if candidate, candidateOK := subscriptionLeaseCandidateFromActive(connectionID, current); candidateOK {
			candidate.holderPartition = partition
			g.scheduleSubscriptionLeaseLocked(candidate, now)
		}
	} else if candidate, candidateOK := subscriptionLeaseCandidateFromPending(connectionID, current); candidateOK {
		candidate.holderPartition = partition
		g.scheduleSubscriptionLeaseLocked(candidate, now)
	}
	g.connections[connectionID] = current
	g.mu.Unlock()
	return registry == nil || registry.AcceptJoin(connectionID, generation, operationID, farmID, subscriptionID)
}

func (g *Gate) recordJoinAccepted(connectionID string, generation uint64, operationID string,
	farmID uint64, attemptID, subscriptionID string, leaseDeadline time.Time) bool {
	partition := g.resolveSubscriptionLeasePartition(farmID)
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.pendingJoin == nil ||
		current.pendingJoin.state != pendingJoinPending ||
		current.pendingJoin.operationID != operationID || current.pendingJoin.farmID != farmID ||
		current.pendingJoin.attemptID != attemptID ||
		(current.pendingJoin.subscriptionID != "" && current.pendingJoin.subscriptionID != subscriptionID) ||
		!leaseDeadlineHasSchedulingWindow(now, leaseDeadline, g.subscriptionLeaseBatchConfig.Batch) {
		return false
	}
	current.pendingJoin.subscriptionID = subscriptionID
	current.pendingJoin.accepted = true
	current.pendingJoin.leaseDeadline = leaseDeadline
	current.pendingJoin.leaseTerminal = false
	g.connections[connectionID] = current
	if candidate, candidateOK := subscriptionLeaseCandidateFromPending(connectionID, current); candidateOK {
		candidate.holderPartition = partition
		g.scheduleSubscriptionLeaseLocked(candidate, now)
	}
	return true
}

// armJoinAsync performs only the explicit Gate -> Hub ordering RPC. ACCEPTED
// has already entered ConnectionWriter, so transient UNAVAILABLE can be retried
// with the same fence without holding the public request goroutine or a backend
// admission slot.
func (g *Gate) armJoinAsync(binding *auth.SessionBinding, generation uint64, operationID string,
	subscription *logicrpcpb.SubscriptionFence) {
	if binding == nil || subscription == nil {
		return
	}
	copyBinding := *binding
	connectionID := copyBinding.Route.ConnectionID
	go func() {
		backoff := 25 * time.Millisecond
		for {
			g.mu.Lock()
			current, ok := g.connections[connectionID]
			logic := g.logic
			rpcTimeout := g.backendRPCTimeout
			valid := ok && current.generation == generation && current.pendingJoin != nil &&
				current.pendingJoin.state == pendingJoinPending &&
				!current.pendingJoin.leaseTerminal &&
				current.pendingJoin.operationID == operationID &&
				current.pendingJoin.farmID == subscription.FarmId &&
				current.pendingJoin.attemptID == subscription.JoinAttemptId &&
				current.pendingJoin.subscriptionID == subscription.SubscriptionId
			deadline := time.Time{}
			leaseDeadline := time.Time{}
			if valid {
				deadline = current.pendingJoin.deadline
				leaseDeadline = current.pendingJoin.leaseDeadline
			}
			g.mu.Unlock()
			if !valid {
				return
			}
			if leaseDeadline.IsZero() || !time.Now().Before(leaseDeadline) {
				g.expireConnectionSubscriptionLeases(connectionID, generation, time.Now())
				return
			}
			if !deadline.After(time.Now()) {
				g.expireJoin(connectionID, generation, operationID, subscription.FarmId)
				return
			}
			armer, ok := logic.(JoinSnapshotArmer)
			if !ok {
				g.cancelJoin(connectionID, generation, operationID, subscription.FarmId)
				return
			}
			if rpcTimeout <= 0 {
				rpcTimeout = defaultBackendRPCTimeout
			}
			if remaining := time.Until(deadline); remaining < rpcTimeout {
				rpcTimeout = remaining
			}
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			response, err := armer.ArmSnapshot(ctx, &logicrpcpb.ArmSnapshotRequest{
				Subscription: proto.Clone(subscription).(*logicrpcpb.SubscriptionFence),
				Target: &logicrpcpb.DownlinkTargetFence{
					TargetPlayerId: copyBinding.PlayerID, GatewayId: copyBinding.Route.GatewayID,
					GatewayBootId: copyBinding.Route.GatewayBootID, ConnectionId: connectionID,
					ConnectionGeneration: generation, SessionEpoch: copyBinding.ActiveSessionEpoch,
					SessionRouteRevision: copyBinding.Route.RouteRevision,
				},
			})
			cancel()
			if err == nil && response != nil {
				switch response.Status {
				case logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED,
					logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ALREADY_ARMED:
					return
				case logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_NOT_FOUND,
					logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_STALE_FENCE:
					g.cancelJoin(connectionID, generation, operationID, subscription.FarmId)
					return
				}
			}
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-time.After(time.Until(deadline)):
				if !timer.Stop() {
					<-timer.C
				}
				g.expireJoin(connectionID, generation, operationID, subscription.FarmId)
				return
			}
			if backoff < 250*time.Millisecond {
				backoff *= 2
			}
		}
	}()
}

func (g *Gate) promoteJoin(promotion downlink.JoinPromotion) {
	partition := g.resolveSubscriptionLeasePartition(promotion.FarmID)
	g.mu.Lock()
	current, ok := g.connections[promotion.ConnectionID]
	if !ok || current.generation != promotion.Generation || current.pendingJoin == nil ||
		current.pendingJoin.state != pendingJoinPending ||
		!current.pendingJoin.accepted ||
		current.pendingJoin.operationID != promotion.OperationID ||
		current.pendingJoin.farmID != promotion.FarmID ||
		current.pendingJoin.subscriptionID != promotion.SubscriptionID ||
		current.pendingJoin.attemptID != promotion.JoinAttemptID {
		g.mu.Unlock()
		return
	}
	if current.pendingJoin.leaseTerminal {
		g.mu.Unlock()
		return
	}
	if current.pendingJoin.leaseDeadline.IsZero() || !time.Now().Before(current.pendingJoin.leaseDeadline) {
		g.mu.Unlock()
		g.expireConnectionSubscriptionLeases(promotion.ConnectionID, promotion.Generation, time.Now())
		return
	}
	if current.pendingJoin.timer != nil {
		current.pendingJoin.timer.Stop()
	}
	if pendingCandidate, candidateOK := subscriptionLeaseCandidateFromPending(promotion.ConnectionID, current); candidateOK {
		g.removeSubscriptionLeaseLocked(pendingCandidate)
	}
	current.currentFarmID = promotion.FarmID
	current.currentSubscriptionID = promotion.SubscriptionID
	current.currentJoinAttemptID = promotion.JoinAttemptID
	current.currentPresenceEpoch = promotion.PresenceEpoch
	current.subscriptionLeaseDeadline = current.pendingJoin.leaseDeadline
	current.subscriptionLeaseTerminal = false
	current.subscriptionContextGeneration++
	current.pendingJoin.state = pendingJoinClosed
	g.connections[promotion.ConnectionID] = current
	if activeCandidate, candidateOK := subscriptionLeaseCandidateFromActive(promotion.ConnectionID, current); candidateOK {
		activeCandidate.holderPartition = partition
		g.scheduleSubscriptionLeaseLocked(activeCandidate, time.Now())
	}
	g.mu.Unlock()
}

// terminateSubscription consumes the exact old-subscription callback emitted
// only after REJOIN_REQUIRED is queued (or that writer is closed on queue
// overflow). The complete fence prevents a delayed callback from clearing a
// newer farm joined on the same physical connection.
func (g *Gate) terminateSubscription(termination downlink.SubscriptionTermination) {
	g.mu.Lock()
	defer g.mu.Unlock()
	current, ok := g.connections[termination.ConnectionID]
	if !ok || current.generation != termination.Generation {
		return
	}
	changed := false
	if current.pendingJoin != nil && current.pendingJoin.state != pendingJoinClosed &&
		current.pendingJoin.farmID == termination.FarmID &&
		current.pendingJoin.subscriptionID == termination.SubscriptionID &&
		current.pendingJoin.attemptID == termination.JoinAttemptID {
		if candidate, candidateOK := subscriptionLeaseCandidateFromPending(termination.ConnectionID, current); candidateOK {
			g.removeSubscriptionLeaseLocked(candidate)
		}
		if current.pendingJoin.timer != nil {
			current.pendingJoin.timer.Stop()
		}
		current.pendingJoin.leaseTerminal = true
		current.pendingJoin.state = pendingJoinClosed
		changed = true
	}
	activeMatches := current.currentFarmID == termination.FarmID &&
		current.currentSubscriptionID == termination.SubscriptionID &&
		current.currentJoinAttemptID == termination.JoinAttemptID &&
		(termination.PresenceEpoch == 0 || current.currentPresenceEpoch == termination.PresenceEpoch)
	if activeMatches {
		if candidate, candidateOK := subscriptionLeaseCandidateFromActive(termination.ConnectionID, current); candidateOK {
			g.removeSubscriptionLeaseLocked(candidate)
		}
		current.currentFarmID = 0
		current.currentSubscriptionID = ""
		current.currentJoinAttemptID = ""
		current.currentPresenceEpoch = 0
		current.subscriptionLeaseDeadline = time.Time{}
		current.subscriptionLeaseTerminal = false
		current.subscriptionContextGeneration++
		changed = true
	}
	if !changed {
		return
	}
	g.connections[termination.ConnectionID] = current
}

func (g *Gate) cancelJoin(connectionID string, generation uint64, operationID string, farmID uint64) {
	g.cancelJoinWithReason(connectionID, generation, operationID, farmID,
		logicrpcpb.CancelJoinReason_CANCEL_JOIN_REASON_ACCEPTED_UNKNOWN)
}

func (g *Gate) cancelJoinWithReason(connectionID string, generation uint64,
	operationID string, farmID uint64, reason logicrpcpb.CancelJoinReason) {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	registry := g.downlink
	var binding *auth.SessionBinding
	subscriptionID := ""
	attemptID := ""
	if ok && current.generation == generation && current.pendingJoin != nil &&
		current.pendingJoin.state != pendingJoinClosed &&
		current.pendingJoin.operationID == operationID && current.pendingJoin.farmID == farmID {
		if candidate, candidateOK := subscriptionLeaseCandidateFromPending(connectionID, current); candidateOK {
			g.removeSubscriptionLeaseLocked(candidate)
		}
		if current.pendingJoin.timer != nil {
			current.pendingJoin.timer.Stop()
		}
		current.pendingJoin.state = pendingJoinCanceling
		subscriptionID = current.pendingJoin.subscriptionID
		attemptID = current.pendingJoin.attemptID
		if current.binding != nil {
			copyBinding := *current.binding
			binding = &copyBinding
		}
		g.connections[connectionID] = current
	} else {
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()
	if registry != nil {
		registry.CancelJoin(connectionID, generation, operationID, farmID)
	}
	if binding == nil {
		g.finishPendingJoinCancel(connectionID, generation, operationID, farmID, attemptID)
		return
	}
	if subscriptionID != "" {
		g.cleanupSubscriptionAsync(binding, generation, farmID, subscriptionID, attemptID,
			logicrpcpb.CleanupReason_CLEANUP_REASON_JOIN_FAILED)
		g.finishPendingJoinCancel(connectionID, generation, operationID, farmID, attemptID)
		return
	}
	go g.resolvePendingJoinCancel(binding, generation, operationID, farmID, attemptID, reason)
}

func (g *Gate) finishPendingJoinCancel(connectionID string, generation uint64,
	operationID string, farmID uint64, attemptID string) {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if ok && current.generation == generation && current.pendingJoin != nil &&
		current.pendingJoin.operationID == operationID && current.pendingJoin.farmID == farmID &&
		current.pendingJoin.attemptID == attemptID &&
		current.pendingJoin.state == pendingJoinCanceling {
		current.pendingJoin.state = pendingJoinClosed
		g.connections[connectionID] = current
	}
	g.mu.Unlock()
}

func (g *Gate) resolvePendingJoinCancel(binding *auth.SessionBinding, generation uint64,
	operationID string, farmID uint64, attemptID string, reason logicrpcpb.CancelJoinReason) {
	response, err := g.invokeCancelJoinAttempt(binding, generation, operationID, farmID, attemptID, reason)
	if err == nil && response != nil &&
		response.Status == logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_ALREADY_PROGRESSED &&
		response.SubscriptionId != nil && *response.SubscriptionId != "" {
		g.mu.Lock()
		current, ok := g.connections[binding.Route.ConnectionID]
		stillCanceling := ok && current.generation == generation && current.pendingJoin != nil &&
			current.pendingJoin.state == pendingJoinCanceling &&
			current.pendingJoin.operationID == operationID && current.pendingJoin.farmID == farmID &&
			current.pendingJoin.attemptID == attemptID
		g.mu.Unlock()
		if stillCanceling {
			g.cleanupSubscriptionAsync(binding, generation, farmID, *response.SubscriptionId,
				attemptID, logicrpcpb.CleanupReason_CLEANUP_REASON_JOIN_FAILED)
		}
	}
	g.finishPendingJoinCancel(binding.Route.ConnectionID, generation, operationID, farmID, attemptID)
}

func (g *Gate) cancelRemoteJoinDetached(binding *auth.SessionBinding, generation uint64,
	operationID string, farmID uint64, attemptID string, reason logicrpcpb.CancelJoinReason) {
	if binding == nil {
		return
	}
	copyBinding := *binding
	go func() {
		response, err := g.invokeCancelJoinAttempt(&copyBinding, generation, operationID, farmID, attemptID, reason)
		if err != nil || response == nil ||
			response.Status != logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_ALREADY_PROGRESSED ||
			response.SubscriptionId == nil || *response.SubscriptionId == "" {
			return
		}
		g.cleanupSubscriptionAsync(&copyBinding, generation, farmID, *response.SubscriptionId,
			attemptID, logicrpcpb.CleanupReason_CLEANUP_REASON_DISCONNECT)
	}()
}

func (g *Gate) invokeCancelJoinAttempt(binding *auth.SessionBinding, generation uint64,
	operationID string, farmID uint64, attemptID string,
	reason logicrpcpb.CancelJoinReason) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	canceler, ok := g.logic.(JoinAttemptCanceler)
	if !ok || binding == nil || operationID == "" || farmID == 0 || attemptID == "" {
		return nil, errors.New("gate: incomplete CancelJoinAttempt capability or fence")
	}
	g.mu.Lock()
	timeout := g.backendRPCTimeout
	g.mu.Unlock()
	if timeout <= 0 {
		timeout = defaultBackendRPCTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return canceler.CancelJoinAttempt(ctx, &logicrpcpb.CancelJoinAttemptRequest{
		TargetFarmId: farmID, JoinAttemptId: attemptID, JoinOperationId: operationID,
		Target: &logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: binding.PlayerID, GatewayId: binding.Route.GatewayID,
			GatewayBootId: binding.Route.GatewayBootID, ConnectionId: binding.Route.ConnectionID,
			ConnectionGeneration: generation, SessionEpoch: binding.ActiveSessionEpoch,
			SessionRouteRevision: binding.Route.RouteRevision,
		},
		Reason: reason,
	})
}

func (g *Gate) expireJoin(connectionID string, generation uint64, operationID string, farmID uint64) {
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if !ok || current.generation != generation || current.pendingJoin == nil ||
		current.pendingJoin.state != pendingJoinPending ||
		current.pendingJoin.operationID != operationID || current.pendingJoin.farmID != farmID ||
		time.Now().Before(current.pendingJoin.deadline) {
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()

	// Timeout closes only this Join intent. It must not scan every FarmActor or
	// silently erase a still-valid previous ACTIVE subscription. H5 owns the
	// retry decision through its matching Join deadline/recovery path.
	g.mu.Lock()
	registry := g.downlink
	g.mu.Unlock()
	if registry != nil {
		registry.ObserveJoinTimeout(connectionID, generation, operationID, farmID)
	}
	g.cancelJoinWithReason(connectionID, generation, operationID, farmID,
		logicrpcpb.CancelJoinReason_CANCEL_JOIN_REASON_PENDING_TIMEOUT)
}

func newTraceID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func (g *Gate) encodeCommandUnavailable(connectionID string, generation uint64,
	cmd *commonpb.ClientCommandEnvelope, code commonpb.ErrorCode, message string) ([]byte, error) {
	// RetryDirective is Go-side only until Batch 3 wires the envelope field;
	// log for observability / future hang-point — do NOT stuff into result proto.
	directive := commandcatalog.DecideRetryDirective(commandcatalog.RetryDecisionInput{
		Status:    commonpb.CommandStatus_UNAVAILABLE,
		ErrorCode: code,
	})
	log.Printf("command unavailable conn=%s gen=%d command_type=%s code=%s retry_directive=%s msg=%s",
		connectionID, generation, cmd.GetCommandType(), code, directive, message)
	result := &commonpb.CommandResultEnvelope{
		RequestId: cmd.GetRequestId(), OperationId: cmd.GetOperationId(),
		Status: commonpb.CommandStatus_UNAVAILABLE, CommandType: cmd.GetCommandType(),
		Error:          &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: message},
		RetryDirective: toPublicRetryDirective(directive),
	}
	spec, hasSpec := commandcatalog.Lookup(cmd.GetCommandType())
	if code == commonpb.ErrorCode_ERR_TIMEOUT && cmd.GetOperationId() != "" && hasSpec && spec.Idempotency.RequiresOperationID() {
		result.Status = commonpb.CommandStatus_PROCESSING
	}
	return g.encodeCommandResultResponse(
		g.nextServerSequence(connectionID, generation), result, g.maxFrame)
}

func (g *Gate) encodeCommandRejected(connectionID string, generation uint64,
	cmd *commonpb.ClientCommandEnvelope, code commonpb.ErrorCode, message string) ([]byte, error) {
	result := &commonpb.CommandResultEnvelope{
		RequestId: cmd.GetRequestId(), OperationId: cmd.GetOperationId(),
		Status: commonpb.CommandStatus_REJECTED, CommandType: cmd.GetCommandType(),
		Error: &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: message},
	}
	return g.encodeCommandResultResponse(
		g.nextServerSequence(connectionID, generation), result, g.maxFrame)
}

func (g *Gate) encodeFreshJoinRequired(connectionID string, generation uint64,
	cmd *commonpb.ClientCommandEnvelope, message string) ([]byte, error) {
	result := &commonpb.CommandResultEnvelope{
		RequestId: cmd.GetRequestId(), OperationId: cmd.GetOperationId(),
		Status: commonpb.CommandStatus_REJECTED, CommandType: cmd.GetCommandType(),
		Error:          &commonpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_NOT_IN_FARM, ErrorMessage: message},
		RetryDirective: commonpb.RetryDirective_FRESH_JOIN,
	}
	return g.encodeCommandResultResponse(
		g.nextServerSequence(connectionID, generation), result, g.maxFrame)
}

func (g *Gate) nextServerSequence(connectionID string, generation uint64) uint64 {
	g.mu.Lock()
	registry := g.downlink
	g.mu.Unlock()
	if registry != nil {
		if seq := registry.NextSequence(connectionID, generation); seq != 0 {
			return seq
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.connections[connectionID]
	if !ok || c.generation != generation {
		return 1
	}
	seq := c.serverSeq
	if seq == 0 {
		seq = 1
	}
	c.serverSeq = seq + 1
	g.connections[connectionID] = c
	return seq
}

// encodeCommandResultResponse 是 CommandResult 下行帧的唯一构造路径:
// CommandResult 与 ServerPush 是 FarmFrame 顶层 oneof 的不同分支(28 §2.1)。
func (g *Gate) encodeCommandResultResponse(seq uint64, result *commonpb.CommandResultEnvelope, maxFrame int) ([]byte, error) {
	encoded, err := protocol.Encode(protocol.NewCommandResultFrame(seq, result), maxFrame)
	if err == nil {
		g.observeCommandResult(result)
	}
	return encoded, err
}

func (g *Gate) Disconnect(connectionID string, generation uint64) {
	g.mu.Lock()
	preAuthController := g.preAuth
	if c, ok := g.connections[connectionID]; ok && c.generation == generation {
		g.removeConnectionLeaseSchedulesLocked(connectionID, c)
		g.removePlayerConnectionLocked(connectionID, c)
		if c.writer != nil {
			c.writer.Close()
		}
		registry := g.downlink
		var binding *auth.SessionBinding
		if c.binding != nil {
			copyBinding := *c.binding
			binding = &copyBinding
		}
		var pending *pendingJoinState
		if c.pendingJoin != nil && c.pendingJoin.state != pendingJoinClosed {
			copyPending := *c.pendingJoin
			if c.pendingJoin.timer != nil {
				c.pendingJoin.timer.Stop()
				copyPending.timer = nil
			}
			pending = &copyPending
		}
		delete(g.connections, connectionID)
		g.mu.Unlock()
		if preAuthController != nil {
			preAuthController.Close(connectionID, generation)
		}
		if registry != nil {
			registry.Unbind(connectionID, generation)
		}
		if binding != nil && c.currentFarmID != 0 && c.currentSubscriptionID != "" &&
			c.currentJoinAttemptID != "" {
			g.cleanupSubscriptionAsync(binding, generation, c.currentFarmID,
				c.currentSubscriptionID, c.currentJoinAttemptID,
				logicrpcpb.CleanupReason_CLEANUP_REASON_DISCONNECT)
		}
		if pending != nil {
			if pending.subscriptionID != "" {
				g.cleanupSubscriptionAsync(binding, generation, pending.farmID,
					pending.subscriptionID, pending.attemptID,
					logicrpcpb.CleanupReason_CLEANUP_REASON_DISCONNECT)
			} else {
				g.cancelRemoteJoinDetached(binding, generation, pending.operationID,
					pending.farmID, pending.attemptID,
					logicrpcpb.CancelJoinReason_CANCEL_JOIN_REASON_CONNECTION_CLOSED)
			}
		}
		if c.binding != nil {
			clearCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = g.auth.CompareAndClearRoute(clearCtx, c.binding)
			cancel()
		}
		return
	}
	g.mu.Unlock()
	if preAuthController != nil {
		preAuthController.Close(connectionID, generation)
	}
}

func (g *Gate) cleanupSubscriptionAsync(binding *auth.SessionBinding, generation, farmID uint64,
	subscriptionID, joinAttemptID string, reason logicrpcpb.CleanupReason) {
	if binding == nil || farmID == 0 || subscriptionID == "" || joinAttemptID == "" {
		return
	}
	copyBinding := *binding
	go func() {
		if err := g.cleanupSubscription(&copyBinding, generation, farmID,
			subscriptionID, joinAttemptID, reason); err != nil {
			log.Printf("gate: CleanupSubscription failed farm=%d subscription=%s attempt=%s conn=%s gen=%d: %v",
				farmID, subscriptionID, joinAttemptID, copyBinding.Route.ConnectionID, generation, err)
		}
	}()
}

func (g *Gate) cleanupSubscription(binding *auth.SessionBinding, generation, farmID uint64,
	subscriptionID, joinAttemptID string, reason logicrpcpb.CleanupReason) error {
	cleaner, ok := g.logic.(SubscriptionCleaner)
	if !ok {
		return errors.New("gate: logic does not support CleanupSubscription")
	}
	if binding == nil || farmID == 0 || subscriptionID == "" || joinAttemptID == "" {
		return errors.New("gate: incomplete CleanupSubscription fence")
	}
	g.mu.Lock()
	timeout := g.backendRPCTimeout
	g.mu.Unlock()
	if timeout <= 0 {
		timeout = defaultBackendRPCTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := cleaner.CleanupSubscription(ctx, &logicrpcpb.CleanupSubscriptionRequest{
		Subscription: &logicrpcpb.SubscriptionFence{
			FarmId: farmID, SubscriptionId: subscriptionID, JoinAttemptId: joinAttemptID,
		},
		Target: &logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: binding.PlayerID, GatewayId: binding.Route.GatewayID,
			GatewayBootId: binding.Route.GatewayBootID, ConnectionId: binding.Route.ConnectionID,
			ConnectionGeneration: generation, SessionEpoch: binding.ActiveSessionEpoch,
			SessionRouteRevision: binding.Route.RouteRevision,
		},
		Reason: reason,
	})
	return err
}
