package command

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/leasebatch"
	"farmgame/internal/gateway/session"

	"google.golang.org/protobuf/proto"
)

const (
	defaultLeaseBatchMaxItems = 256
	defaultLeaseBatchMaxBytes = 512 << 10
	defaultLeaseBucketCount   = 64
)

// LeaseRenewalLaneConfig freezes the scheduling and physical-dispatch limits
// for one independent lease lane. TransientBackoff never extends trust; it
// only decides when an unknown result may be retried before the old deadline.
type LeaseRenewalLaneConfig struct {
	Batch            leasebatch.Config
	TransientBackoff time.Duration
}

// LeaseRenewalConfig keeps SessionRoute and Subscription tunables separate so
// neither lane can consume the other's batch or dispatch budget.
type LeaseRenewalConfig struct {
	SessionRoute LeaseRenewalLaneConfig
	Subscription LeaseRenewalLaneConfig
}

func defaultLeaseRenewalLaneConfig(ttl time.Duration) LeaseRenewalLaneConfig {
	interval := ttl / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}
	width := interval / defaultLeaseBucketCount
	dispatch := ttl / 4
	if dispatch > 3*time.Second {
		dispatch = 3 * time.Second
	}
	if dispatch < time.Millisecond {
		dispatch = time.Millisecond
	}
	return LeaseRenewalLaneConfig{
		Batch: leasebatch.Config{
			Limits: leasebatch.Limits{
				MaxItems: defaultLeaseBatchMaxItems, MaxBytes: defaultLeaseBatchMaxBytes,
				MaxDispatchDuration: dispatch,
			},
			Schedule: leasebatch.Schedule{
				Interval: interval, BucketCount: defaultLeaseBucketCount, MaxJitter: width / 2,
			},
		},
		TransientBackoff: 250 * time.Millisecond,
	}
}

func defaultLeaseRenewalConfig(routeTTL, subscriptionTTL time.Duration) LeaseRenewalConfig {
	return LeaseRenewalConfig{
		SessionRoute: defaultLeaseRenewalLaneConfig(routeTTL),
		Subscription: defaultLeaseRenewalLaneConfig(subscriptionTTL),
	}
}

func validateLeaseRenewalLaneConfig(name string, cfg LeaseRenewalLaneConfig, ttl, margin time.Duration) error {
	if err := cfg.Batch.ValidateSafety(ttl, margin); err != nil {
		return fmt.Errorf("gate: %s lease batch config: %w", name, err)
	}
	if cfg.TransientBackoff <= 0 || cfg.TransientBackoff >= ttl-margin {
		return fmt.Errorf("gate: %s transient backoff %s outside safe window", name, cfg.TransientBackoff)
	}
	return nil
}

type routeLeaseCandidate struct {
	route    session.ActiveRoute
	deadline time.Time
}

func stableLeaseIdentity(kind string, fields ...string) string {
	var identity strings.Builder
	identity.WriteString(kind)
	for _, field := range fields {
		identity.WriteByte('|')
		identity.WriteString(strconv.Itoa(len(field)))
		identity.WriteByte(':')
		identity.WriteString(field)
	}
	return identity.String()
}

func routeLeaseScheduleKey(route session.ActiveRoute) string {
	return stableLeaseIdentity("route", route.GatewayID, route.GatewayBootID, route.ConnectionID, route.SessionID,
		strconv.FormatUint(route.ConnectionGeneration, 10), strconv.FormatUint(route.PlayerID, 10),
		strconv.FormatUint(route.SessionEpoch, 10), strconv.FormatUint(route.RouteRevision, 10))
}

func routeLeaseIdentity(route session.ActiveRoute) string { return routeLeaseScheduleKey(route) }

func routeLeaseHolder(route session.ActiveRoute) string {
	return stableLeaseIdentity("gate-holder", route.GatewayID, route.GatewayBootID)
}

func estimateRouteLeaseBytes(route session.ActiveRoute) int {
	// RedisAuthority queues one EVALSHA containing the fixed 40-byte digest,
	// one session key and eight argv values. Keep a conservative RESP/framing
	// allowance rather than using the much larger Lua source or proto.Size.
	return 384 + len(route.GatewayID) + len(route.GatewayBootID) + len(route.ConnectionID) +
		len(route.SessionID) + len(strconv.FormatUint(route.PlayerID, 10)) + 8*20
}

func subscriptionLeaseScheduleKey(candidate subscriptionLeaseCandidate) string {
	phase := "active"
	if candidate.pending {
		phase = "pending"
	}
	return stableLeaseIdentity("subscription", phase, candidate.binding.Route.GatewayID,
		candidate.binding.Route.GatewayBootID, strconv.FormatUint(candidate.generation, 10),
		strconv.FormatUint(candidate.contextGeneration, 10), candidate.connectionID,
		candidate.subscriptionID, strconv.FormatUint(candidate.farmID, 10), candidate.joinAttemptID,
		strconv.FormatUint(candidate.presenceEpoch, 10))
}

func subscriptionLeaseIdentity(candidate subscriptionLeaseCandidate) string {
	return subscriptionLeaseScheduleKey(candidate)
}

func (g *Gate) subscriptionLeaseHolderLocked(candidate subscriptionLeaseCandidate) string {
	// Pure: every caller holds Gate.mu. The partition string itself is resolved
	// off the lock and carried on the candidate; this only concatenates it with
	// the local gateway identity so two farms on one Holder share a batch.
	partition := candidate.holderPartition
	if partition == "" {
		partition = subscriptionLeaseFarmPartition(candidate.farmID)
	}
	return subscriptionLeaseDispatchHolder(candidate.binding.Route.GatewayID,
		candidate.binding.Route.GatewayBootID, partition)
}

func (g *Gate) resolveSubscriptionLeasePartition(farmID uint64) string {
	if farmID == 0 {
		return subscriptionLeaseFarmPartition(farmID)
	}
	// g.logic is assigned at construction and never mutated afterwards, so this
	// may run off Gate.mu. The resolver itself is an in-process snapshot read.
	resolver, ok := g.logic.(subscriptionLeasePartitionResolver)
	if !ok {
		return subscriptionLeaseFarmPartition(farmID)
	}
	partition, err := resolver.SubscriptionLeasePartition(context.Background(), farmID)
	if err != nil || partition == "" {
		return subscriptionLeaseFarmPartition(farmID)
	}
	return partition
}

func subscriptionLeaseFarmPartition(farmID uint64) string {
	return stableLeaseIdentity("farm-isolation", strconv.FormatUint(farmID, 10))
}

func subscriptionLeaseDispatchHolder(gatewayID, gatewayBootID, partition string) string {
	return stableLeaseIdentity("gate-holder", gatewayID, gatewayBootID) + "|partition:" + partition
}

func estimateSubscriptionLeaseBytes(candidate subscriptionLeaseCandidate, requestedTTL time.Duration) int {
	return 128 + proto.Size(candidate.entry(requestedTTL))
}

func (g *Gate) leaseNow() time.Time {
	if g.leaseClock != nil {
		return g.leaseClock()
	}
	return time.Now()
}

func leaseDeadlineHasSchedulingWindow(now, deadline time.Time, cfg leasebatch.Config) bool {
	if deadline.IsZero() || !now.Before(deadline) {
		return false
	}
	// NextDue may legally consume one complete stable phase interval. Reserve a
	// complete dispatch budget after that point; accepting a shorter window
	// would make safety depend on the identity's lucky bucket.
	return now.Add(cfg.Interval + cfg.MaxDispatchDuration).Before(deadline)
}

// SetLeaseRenewalConfig installs both planners before connections are opened.
// Reconfiguration with live connections is rejected so no item silently moves
// between incompatible bucket/hash or safety regimes.
func (g *Gate) SetLeaseRenewalConfig(cfg LeaseRenewalConfig) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.connections) != 0 {
		return errors.New("gate: lease renewal config must be installed before connections")
	}
	routeTTL := session.DefaultRouteLeaseTTL
	if g.auth != nil {
		routeTTL = g.auth.RouteLeaseTTL()
	}
	if err := validateLeaseRenewalLaneConfig("session_route", cfg.SessionRoute,
		routeTTL, g.routeLeaseSafetyMargin); err != nil {
		return err
	}
	if err := validateLeaseRenewalLaneConfig("subscription", cfg.Subscription,
		g.subscriptionLeaseTTL, g.subscriptionLeaseSafetyMargin); err != nil {
		return err
	}
	routePlanner, err := leasebatch.NewPlanner[routeLeaseCandidate](cfg.SessionRoute.Batch)
	if err != nil {
		return err
	}
	subscriptionPlanner, err := leasebatch.NewPlanner[subscriptionLeaseCandidate](cfg.Subscription.Batch)
	if err != nil {
		return err
	}
	if g.routeLeasePlanner != nil {
		g.routeLeasePlanner.Stop()
	}
	if g.subscriptionLeasePlanner != nil {
		g.subscriptionLeasePlanner.Stop()
	}
	g.routeLeaseConfig = cfg.SessionRoute
	g.subscriptionLeaseBatchConfig = cfg.Subscription
	g.routeLeasePlanner = routePlanner
	g.subscriptionLeasePlanner = subscriptionPlanner
	return nil
}

func (g *Gate) scheduleRouteLeaseLocked(route session.ActiveRoute, deadline, now time.Time) {
	if g.leaseSchedulersDraining || g.routeLeasePlanner == nil || deadline.IsZero() {
		return
	}
	identity := routeLeaseIdentity(route)
	due, err := leasebatch.NextDue(now, identity, g.routeLeaseConfig.Batch.Schedule)
	if err != nil {
		return
	}
	latest := deadline.Add(-g.routeLeaseConfig.Batch.MaxDispatchDuration)
	if !due.Before(latest) {
		due = now
	}
	_ = g.routeLeasePlanner.Upsert(leasebatch.Item[routeLeaseCandidate]{
		Key: routeLeaseScheduleKey(route), Identity: identity, Holder: routeLeaseHolder(route),
		Due: due, Deadline: deadline, EstimatedBytes: estimateRouteLeaseBytes(route),
		Payload: routeLeaseCandidate{route: route, deadline: deadline},
	})
}

func (g *Gate) removeRouteLeaseLocked(route session.ActiveRoute) {
	if g.routeLeasePlanner != nil {
		g.routeLeasePlanner.Remove(routeLeaseScheduleKey(route))
	}
}

func (g *Gate) scheduleSubscriptionLeaseLocked(candidate subscriptionLeaseCandidate, now time.Time) {
	if g.leaseSchedulersDraining || g.subscriptionLeasePlanner == nil || candidate.deadline.IsZero() {
		return
	}
	identity := subscriptionLeaseIdentity(candidate)
	due, err := leasebatch.NextDue(now, identity, g.subscriptionLeaseBatchConfig.Batch.Schedule)
	if err != nil {
		return
	}
	latest := candidate.deadline.Add(-g.subscriptionLeaseBatchConfig.Batch.MaxDispatchDuration)
	if !due.Before(latest) {
		due = now
	}
	_ = g.subscriptionLeasePlanner.Upsert(leasebatch.Item[subscriptionLeaseCandidate]{
		Key: subscriptionLeaseScheduleKey(candidate), Identity: identity,
		Holder: g.subscriptionLeaseHolderLocked(candidate), Due: due, Deadline: candidate.deadline,
		EstimatedBytes: estimateSubscriptionLeaseBytes(candidate, g.subscriptionLeaseTTL), Payload: candidate,
	})
}

func (g *Gate) removeSubscriptionLeaseLocked(candidate subscriptionLeaseCandidate) {
	if g.subscriptionLeasePlanner != nil {
		g.subscriptionLeasePlanner.Remove(subscriptionLeaseScheduleKey(candidate))
	}
}

func subscriptionLeaseCandidateFromPending(connectionID string, current connection) (subscriptionLeaseCandidate, bool) {
	pending := current.pendingJoin
	if current.binding == nil || pending == nil || pending.state == pendingJoinClosed ||
		!pending.accepted || pending.farmID == 0 || pending.subscriptionID == "" || pending.attemptID == "" {
		return subscriptionLeaseCandidate{}, false
	}
	return subscriptionLeaseCandidate{
		connectionID: connectionID, generation: current.generation,
		contextGeneration: current.subscriptionContextGeneration, binding: *current.binding,
		farmID: pending.farmID, subscriptionID: pending.subscriptionID,
		joinAttemptID: pending.attemptID, deadline: pending.leaseDeadline, pending: true,
	}, true
}

func subscriptionLeaseCandidateFromActive(connectionID string, current connection) (subscriptionLeaseCandidate, bool) {
	if current.binding == nil || current.currentFarmID == 0 || current.currentSubscriptionID == "" ||
		current.currentJoinAttemptID == "" {
		return subscriptionLeaseCandidate{}, false
	}
	return subscriptionLeaseCandidate{
		connectionID: connectionID, generation: current.generation,
		contextGeneration: current.subscriptionContextGeneration, binding: *current.binding,
		farmID: current.currentFarmID, subscriptionID: current.currentSubscriptionID,
		joinAttemptID: current.currentJoinAttemptID, presenceEpoch: current.currentPresenceEpoch,
		deadline: current.subscriptionLeaseDeadline,
	}, true
}

func (g *Gate) removeConnectionLeaseSchedulesLocked(connectionID string, current connection) {
	if current.binding != nil {
		g.removeRouteLeaseLocked(current.binding.Route)
	}
	if candidate, ok := subscriptionLeaseCandidateFromPending(connectionID, current); ok {
		g.removeSubscriptionLeaseLocked(candidate)
	}
	if candidate, ok := subscriptionLeaseCandidateFromActive(connectionID, current); ok {
		g.removeSubscriptionLeaseLocked(candidate)
	}
}

// StopLeaseRenewalSchedulers is the drain linearization point. Registrations
// and late executor results hold Gate.mu before Upsert, so after this returns no
// pending work can be recreated and both heaps are empty.
func (g *Gate) StopLeaseRenewalSchedulers() (routeCleared, subscriptionCleared int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.leaseSchedulersDraining {
		return 0, 0
	}
	g.leaseSchedulersDraining = true
	if g.routeLeasePlanner != nil {
		routeCleared = g.routeLeasePlanner.Stop()
	}
	if g.subscriptionLeasePlanner != nil {
		subscriptionCleared = g.subscriptionLeasePlanner.Stop()
	}
	if g.metrics != nil {
		g.metrics.LeaseDrainTotal.WithLabelValues("session_route").Inc()
		g.metrics.LeaseDrainTotal.WithLabelValues("subscription").Inc()
		g.metrics.LeaseBacklog.WithLabelValues("session_route").Set(0)
		g.metrics.LeaseBacklog.WithLabelValues("subscription").Set(0)
	}
	return routeCleared, subscriptionCleared
}

func (g *Gate) leaseBatchBacklog() (int, int) {
	route, subscription := 0, 0
	if g.routeLeasePlanner != nil {
		route = g.routeLeasePlanner.Stats().Backlog
	}
	if g.subscriptionLeasePlanner != nil {
		subscription = g.subscriptionLeasePlanner.Stats().Backlog
	}
	return route, subscription
}

func resultOrUnavailable(entry *logicrpcpb.SubscriptionLeaseEntry) *logicrpcpb.SubscriptionLeaseResult {
	return &logicrpcpb.SubscriptionLeaseResult{
		TargetFarmId: entry.GetTargetFarmId(), SubscriptionId: entry.GetSubscriptionId(),
		JoinAttemptId: entry.GetJoinAttemptId(),
		Status:        logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE,
	}
}

func (g *Gate) observeLeaseEvent(lane, outcome string, count int) {
	if count <= 0 || g.metrics == nil || g.metrics.LeaseEventsTotal == nil {
		return
	}
	g.metrics.LeaseEventsTotal.WithLabelValues(lane, outcome).Add(float64(count))
}

func (g *Gate) observeLeaseEventLocked(lane, outcome string, count int) {
	g.observeLeaseEvent(lane, outcome, count)
}

func (g *Gate) observeLeaseInFlight(lane string, count int) func() {
	if count <= 0 || g.metrics == nil || g.metrics.LeaseInFlight == nil {
		return func() {}
	}
	g.metrics.LeaseInFlight.WithLabelValues(lane).Add(float64(count))
	return func() { g.metrics.LeaseInFlight.WithLabelValues(lane).Sub(float64(count)) }
}

func (g *Gate) observeLeasePlanned(lane string, items, estimatedBytes int, dueLag time.Duration, oversize bool) {
	g.observeLeaseEvent(lane, "planned", items)
	metrics := g.metrics
	if metrics == nil {
		return
	}
	metrics.LeaseBatchItems.WithLabelValues(lane).Observe(float64(items))
	metrics.LeaseBatchEstimatedBytes.WithLabelValues(lane).Observe(float64(estimatedBytes))
	metrics.LeaseDueLagSeconds.WithLabelValues(lane).Observe(dueLag.Seconds())
	metrics.LeaseHolderIsolationTotal.WithLabelValues(lane).Inc()
	if oversize {
		metrics.LeaseEventsTotal.WithLabelValues(lane, "oversize").Inc()
	}
}

func (g *Gate) observeLeaseDispatch(lane string, items, estimatedBytes int, duration time.Duration) {
	if g.metrics == nil {
		return
	}
	g.metrics.LeaseDispatchSeconds.WithLabelValues(lane).Observe(duration.Seconds())
}

func (g *Gate) observeLeaseBacklog() {
	if g.metrics == nil {
		return
	}
	route, subscription := g.leaseBatchBacklog()
	g.metrics.LeaseBacklog.WithLabelValues("session_route").Set(float64(route))
	g.metrics.LeaseBacklog.WithLabelValues("subscription").Set(float64(subscription))
}

func (g *Gate) observeLeaseStaleNonResurrectionLocked(lane string) {
	if g.metrics != nil {
		g.metrics.LeaseStaleNonResurrection.WithLabelValues(lane).Inc()
	}
}
