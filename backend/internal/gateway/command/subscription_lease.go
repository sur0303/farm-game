package command

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/leasebatch"
	gateprotocol "farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

type subscriptionLeaseCandidate struct {
	connectionID      string
	generation        uint64
	contextGeneration uint64
	binding           auth.SessionBinding
	farmID            uint64
	subscriptionID    string
	joinAttemptID     string
	presenceEpoch     uint64
	deadline          time.Time
	pending           bool
	// holderPartition is the physical Holder generation the planner isolates
	// on. Empty means farm isolation: the honest bound when the logic client
	// cannot name a Holder, and the fallback when resolution failed.
	holderPartition string
}

func (c subscriptionLeaseCandidate) entry(requestedTTL time.Duration) *logicrpcpb.SubscriptionLeaseEntry {
	return &logicrpcpb.SubscriptionLeaseEntry{
		TargetFarmId:   c.farmID,
		SubscriptionId: c.subscriptionID,
		JoinAttemptId:  c.joinAttemptID,
		Target:         c.targetFence(),
		RequestedTtlMs: uint32(requestedTTL / time.Millisecond),
	}
}

func (c subscriptionLeaseCandidate) targetFence() *logicrpcpb.DownlinkTargetFence {
	return &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId:       c.binding.PlayerID,
		GatewayId:            c.binding.Route.GatewayID,
		GatewayBootId:        c.binding.Route.GatewayBootID,
		ConnectionId:         c.connectionID,
		ConnectionGeneration: c.generation,
		SessionEpoch:         c.binding.ActiveSessionEpoch,
		SessionRouteRevision: c.binding.Route.RouteRevision,
	}
}

// RenewSubscriptionLeases performs one explicit, bounded maintenance batch.
// The app lifecycle calls RenewDueSubscriptionLeases so stable bucket phases
// decide eligibility independently from the scheduler tick.
func (g *Gate) RenewSubscriptionLeases(ctx context.Context) error {
	return g.renewSubscriptionLeaseBatch(ctx, true)
}

// RenewDueSubscriptionLeases performs at most one due, single-Holder dispatch
// and never scans the connections map.
func (g *Gate) RenewDueSubscriptionLeases(ctx context.Context) error {
	return g.renewSubscriptionLeaseBatch(ctx, false)
}

func (g *Gate) renewSubscriptionLeaseBatch(ctx context.Context, force bool) error {
	g.subscriptionLeaseRenewMu.Lock()
	defer g.subscriptionLeaseRenewMu.Unlock()
	if g.subscriptionLeasePlanner == nil {
		return nil
	}
	renewStarted := g.leaseNow()
	batch := g.subscriptionLeasePlanner.Plan(renewStarted, force)
	if len(batch.Items) == 0 {
		g.observeLeaseBacklog()
		return nil
	}
	g.observeLeasePlanned("subscription", len(batch.Items), batch.EstimatedBytes, batch.MaxDueLag, batch.Oversize)

	candidates := make([]subscriptionLeaseCandidate, 0, len(batch.Items))
	items := make([]leasebatch.Item[subscriptionLeaseCandidate], 0, len(batch.Items))
	expired := make([]subscriptionLeaseCandidate, 0)
	prePartial := false
	g.mu.Lock()
	logic := g.logic
	requestedTTL := g.subscriptionLeaseTTL
	safetyMargin := g.subscriptionLeaseSafetyMargin
	for _, item := range batch.Items {
		candidate := item.Payload
		current, exists := g.connections[candidate.connectionID]
		if !exists || !subscriptionLeaseCandidateMatches(current, candidate) ||
			subscriptionLeaseCandidateTerminal(current, candidate) || current.admission != laneAdmissible ||
			!subscriptionLeaseCandidateDeadline(current, candidate).Equal(candidate.deadline) {
			g.observeLeaseStaleNonResurrectionLocked("subscription")
			prePartial = true
			continue
		}
		if candidate.deadline.IsZero() || !renewStarted.Before(candidate.deadline) {
			if markSubscriptionLeaseTerminal(&current, candidate) {
				expired = append(expired, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
			g.connections[candidate.connectionID] = current
			prePartial = true
			continue
		}
		candidates = append(candidates, candidate)
		items = append(items, item)
	}
	g.mu.Unlock()
	g.dispatchExpiredSubscriptionLeases(expired)
	if len(candidates) == 0 {
		if prePartial {
			g.observeLeaseEvent("subscription", "partial", 1)
		}
		g.observeLeaseBacklog()
		return nil
	}
	if requestedTTL < time.Millisecond {
		requestedTTL = defaultSubscriptionLeaseTTL
	}
	renewer, ok := logic.(SubscriptionLeaseRenewer)
	if !ok || renewer == nil {
		g.observeSubscriptionLeaseFailClosed(g.metrics, LeaseFailClosedUnsupportedLogic)
		g.rescheduleSubscriptionTransients(items, g.leaseNow())
		g.observeLeaseEvent("subscription", "transient", len(items))
		g.observeLeaseEvent("subscription", "partial", 1)
		return errors.New("gate: logic does not support BatchRenewSubscriptions")
	}

	entries := make([]*logicrpcpb.SubscriptionLeaseEntry, len(candidates))
	for i, candidate := range candidates {
		entries[i] = candidate.entry(requestedTTL)
	}
	request := &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId:     candidates[0].binding.Route.GatewayID,
		SourceGatewayBootId: candidates[0].binding.Route.GatewayBootID,
		Entries:             entries,
	}
	if metrics := g.metrics; metrics != nil {
		metrics.SubscriptionLeaseRenewBatches.Inc()
		metrics.SubscriptionLeaseRenewEntries.Add(float64(len(entries)))
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, g.subscriptionLeaseBatchConfig.Batch.MaxDispatchDuration)
	rpcStarted := g.leaseNow()
	g.observeLeaseEvent("subscription", "attempted", len(entries))
	settleInFlight := g.observeLeaseInFlight("subscription", len(entries))
	defer settleInFlight()
	var response *logicrpcpb.BatchRenewSubscriptionsResponse
	var dispatchErr error
	if partitioner, partitioned := logic.(SubscriptionLeasePartitioner); partitioned {
		response, dispatchErr = partitioner.BatchRenewSubscriptionsPartition(dispatchCtx, batch.Holder, request)
	} else {
		response, dispatchErr = renewer.BatchRenewSubscriptions(dispatchCtx, request)
	}
	cancel()
	responseAt := g.leaseNow()
	if metrics := g.metrics; metrics != nil {
		metrics.SubscriptionLeaseRenewSeconds.Observe(responseAt.Sub(rpcStarted).Seconds())
	}
	g.observeLeaseDispatch("subscription", len(entries), batch.EstimatedBytes, responseAt.Sub(rpcStarted))
	var firstErr error
	if dispatchErr != nil {
		firstErr = dispatchErr
		g.observeSubscriptionLeaseFailClosed(g.metrics, LeaseFailClosedRPCError)
	}
	partial := prePartial || dispatchErr != nil || response == nil || len(response.GetResults()) != len(candidates)
	if dispatchErr == nil && (response == nil || len(response.GetResults()) != len(candidates)) {
		firstErr = fmt.Errorf("gate: subscription lease renewal result count=%d want=%d", len(response.GetResults()), len(candidates))
		g.observeSubscriptionLeaseFailClosed(g.metrics, LeaseFailClosedMalformedResponse)
	}
	if response == nil {
		response = &logicrpcpb.BatchRenewSubscriptionsResponse{}
	}
	terminal := make([]subscriptionLeaseCandidate, 0)
	stale := make([]leasebatch.Item[subscriptionLeaseCandidate], 0)
	g.mu.Lock()
	for i, item := range items {
		candidate := candidates[i]
		current, exists := g.connections[candidate.connectionID]
		if g.leaseSchedulersDraining || !exists || !subscriptionLeaseCandidateMatches(current, candidate) ||
			subscriptionLeaseCandidateTerminal(current, candidate) ||
			!subscriptionLeaseCandidateDeadline(current, candidate).Equal(candidate.deadline) {
			g.observeLeaseStaleNonResurrectionLocked("subscription")
			partial = true
			continue
		}
		if !responseAt.Before(candidate.deadline) {
			if markSubscriptionLeaseTerminal(&current, candidate) {
				terminal = append(terminal, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
			g.connections[candidate.connectionID] = current
			partial = true
			continue
		}
		if i >= len(response.Results) || response.Results[i] == nil {
			g.rescheduleSubscriptionTransientLocked(item, responseAt)
			g.observeLeaseEventLocked("subscription", "transient", 1)
			continue
		}
		result := response.Results[i]
		if metrics := g.metrics; metrics != nil {
			metrics.SubscriptionLeaseResults.WithLabelValues(SubscriptionLeaseStatusLabel(result.GetStatus())).Inc()
		}
		if !sameSubscriptionLeaseIdentity(entries[i], result) {
			g.rescheduleSubscriptionTransientLocked(item, responseAt)
			g.observeLeaseEventLocked("subscription", "transient", 1)
			g.observeSubscriptionLeaseFailClosed(g.metrics, LeaseFailClosedMalformedResponse)
			partial = true
			if firstErr == nil {
				firstErr = fmt.Errorf("gate: subscription lease renewal fence mismatch at index %d", i)
			}
			continue
		}
		makeTerminal := false
		switch result.Status {
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED:
			acceptedTTL := time.Duration(result.AcceptedTtlMs) * time.Millisecond
			newDeadline, valid := conservativeSubscriptionLeaseDeadline(renewStarted, acceptedTTL, safetyMargin)
			if result.AcceptedTtlMs == 0 || acceptedTTL > requestedTTL || !valid ||
				!responseAt.Before(newDeadline) ||
				g.subscriptionLeaseBatchConfig.Batch.ValidateSafety(acceptedTTL, safetyMargin) != nil {
				g.rescheduleSubscriptionTransientLocked(item, responseAt)
				g.observeLeaseEventLocked("subscription", "transient", 1)
				g.observeSubscriptionLeaseFailClosed(g.metrics, LeaseFailClosedMalformedResponse)
				partial = true
				if firstErr == nil {
					firstErr = fmt.Errorf("gate: invalid accepted subscription ttl at index %d", i)
				}
				continue
			} else if candidate.pending {
				current.pendingJoin.leaseDeadline = newDeadline
			} else {
				current.subscriptionLeaseDeadline = newDeadline
			}
			candidate.deadline = newDeadline
			g.scheduleSubscriptionLeaseLocked(candidate, responseAt)
			g.observeLeaseEventLocked("subscription", "succeeded", 1)
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_NOT_FOUND,
			logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE,
			logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED:
			makeTerminal = true
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE:
			// The planned Holder generation is gone. Re-resolve off Gate.mu so
			// the next due lands on the live Holder instead of retrying the
			// stale partition until the lease expires.
			stale = append(stale, item)
			g.observeLeaseEventLocked("subscription", "transient", 1)
			partial = true
		case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE,
			logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNSPECIFIED:
			g.rescheduleSubscriptionTransientLocked(item, responseAt)
			g.observeLeaseEventLocked("subscription", "transient", 1)
			partial = true
		default:
			g.rescheduleSubscriptionTransientLocked(item, responseAt)
			g.observeLeaseEventLocked("subscription", "transient", 1)
			partial = true
		}
		if makeTerminal {
			if markSubscriptionLeaseTerminal(&current, candidate) {
				terminal = append(terminal, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
			partial = true
		}
		g.connections[candidate.connectionID] = current
	}
	g.mu.Unlock()
	g.dispatchExpiredSubscriptionLeases(terminal)
	g.rescheduleSubscriptionWithResolvedPartition(stale, responseAt)
	if partial {
		g.observeLeaseEvent("subscription", "partial", 1)
	}
	g.observeLeaseBacklog()
	return firstErr
}

// ExpireSubscriptionLeases consumes at most one bounded due batch. It remains
// available to command/Join fail-closed edges but no longer scans all local
// connections after an RPC error or on a lifecycle tick.
func (g *Gate) ExpireSubscriptionLeases(now time.Time) int {
	if g.subscriptionLeasePlanner == nil {
		return 0
	}
	batch := g.subscriptionLeasePlanner.Plan(now, false)
	expired := make([]subscriptionLeaseCandidate, 0, len(batch.Items))
	g.mu.Lock()
	for _, item := range batch.Items {
		candidate := item.Payload
		current, ok := g.connections[candidate.connectionID]
		if !ok || !subscriptionLeaseCandidateMatches(current, candidate) ||
			subscriptionLeaseCandidateTerminal(current, candidate) {
			continue
		}
		canonicalDeadline := subscriptionLeaseCandidateDeadline(current, candidate)
		if canonicalDeadline.IsZero() || !now.Before(canonicalDeadline) {
			if markSubscriptionLeaseTerminal(&current, candidate) {
				expired = append(expired, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
			g.connections[candidate.connectionID] = current
			continue
		}
		candidate.deadline = canonicalDeadline
		item.Deadline = canonicalDeadline
		item.Payload = candidate
		_ = g.subscriptionLeasePlanner.Upsert(item)
	}
	g.mu.Unlock()
	g.dispatchExpiredSubscriptionLeases(expired)
	return len(expired)
}

// expireConnectionSubscriptionLeases checks only the two lease slots owned by
// one exact connection generation. Admission and Join callbacks use this path
// so an expired fence is closed immediately without a global or scheduler scan.
func (g *Gate) expireConnectionSubscriptionLeases(connectionID string, generation uint64, now time.Time) int {
	expired := make([]subscriptionLeaseCandidate, 0, 2)
	g.mu.Lock()
	current, ok := g.connections[connectionID]
	if ok && current.generation == generation {
		if candidate, candidateOK := subscriptionLeaseCandidateFromPending(connectionID, current); candidateOK &&
			(candidate.deadline.IsZero() || !now.Before(candidate.deadline)) {
			g.removeSubscriptionLeaseLocked(candidate)
			if markSubscriptionLeaseTerminal(&current, candidate) {
				expired = append(expired, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
		}
		if candidate, candidateOK := subscriptionLeaseCandidateFromActive(connectionID, current); candidateOK &&
			(candidate.deadline.IsZero() || !now.Before(candidate.deadline)) {
			g.removeSubscriptionLeaseLocked(candidate)
			if markSubscriptionLeaseTerminal(&current, candidate) {
				expired = append(expired, candidate)
				g.observeLeaseEventLocked("subscription", "lost", 1)
			}
		}
		g.connections[connectionID] = current
	}
	g.mu.Unlock()
	g.dispatchExpiredSubscriptionLeases(expired)
	return len(expired)
}

func subscriptionLeaseCandidateDeadline(current connection, candidate subscriptionLeaseCandidate) time.Time {
	if candidate.pending {
		if current.pendingJoin == nil {
			return time.Time{}
		}
		return current.pendingJoin.leaseDeadline
	}
	return current.subscriptionLeaseDeadline
}

func (g *Gate) rescheduleSubscriptionWithResolvedPartition(items []leasebatch.Item[subscriptionLeaseCandidate], now time.Time) {
	if len(items) == 0 {
		return
	}
	partitions := make(map[uint64]string, len(items))
	for _, item := range items {
		farmID := item.Payload.farmID
		if _, known := partitions[farmID]; known {
			continue
		}
		partitions[farmID] = g.resolveSubscriptionLeasePartition(farmID)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, item := range items {
		candidate := item.Payload
		current, ok := g.connections[candidate.connectionID]
		if g.leaseSchedulersDraining || !ok || !subscriptionLeaseCandidateMatches(current, candidate) ||
			subscriptionLeaseCandidateTerminal(current, candidate) || !now.Before(candidate.deadline) {
			continue
		}
		candidate.holderPartition = partitions[candidate.farmID]
		item.Payload = candidate
		g.rescheduleSubscriptionTransientLocked(item, now)
	}
}

func (g *Gate) rescheduleSubscriptionTransients(items []leasebatch.Item[subscriptionLeaseCandidate], now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, item := range items {
		candidate := item.Payload
		current, ok := g.connections[candidate.connectionID]
		if !g.leaseSchedulersDraining && ok && subscriptionLeaseCandidateMatches(current, candidate) &&
			!subscriptionLeaseCandidateTerminal(current, candidate) && now.Before(candidate.deadline) {
			g.rescheduleSubscriptionTransientLocked(item, now)
		}
	}
}

func (g *Gate) rescheduleSubscriptionTransientLocked(item leasebatch.Item[subscriptionLeaseCandidate], now time.Time) {
	candidate := item.Payload
	due := now.Add(g.subscriptionLeaseBatchConfig.TransientBackoff)
	latest := candidate.deadline.Add(-g.subscriptionLeaseBatchConfig.Batch.MaxDispatchDuration)
	if due.After(latest) {
		due = candidate.deadline
	}
	item.Due = due
	item.Holder = g.subscriptionLeaseHolderLocked(candidate)
	_ = g.subscriptionLeasePlanner.Upsert(item)
}

func subscriptionLeaseCandidateMatches(current connection, candidate subscriptionLeaseCandidate) bool {
	if current.generation != candidate.generation || current.binding == nil ||
		current.subscriptionContextGeneration != candidate.contextGeneration ||
		!sameRouteRenewFence(current.binding.Route, candidate.binding.Route) ||
		current.binding.PlayerID != candidate.binding.PlayerID ||
		current.binding.ActiveSessionEpoch != candidate.binding.ActiveSessionEpoch {
		return false
	}
	if candidate.pending {
		pending := current.pendingJoin
		return pending != nil && pending.state == pendingJoinPending && pending.accepted &&
			pending.farmID == candidate.farmID && pending.subscriptionID == candidate.subscriptionID &&
			pending.attemptID == candidate.joinAttemptID
	}
	return current.currentFarmID == candidate.farmID &&
		current.currentSubscriptionID == candidate.subscriptionID &&
		current.currentJoinAttemptID == candidate.joinAttemptID &&
		current.currentPresenceEpoch == candidate.presenceEpoch
}

func subscriptionLeaseCandidateTerminal(current connection, candidate subscriptionLeaseCandidate) bool {
	if candidate.pending {
		return current.pendingJoin == nil || current.pendingJoin.leaseTerminal
	}
	return current.subscriptionLeaseTerminal
}

func markSubscriptionLeaseTerminal(current *connection, candidate subscriptionLeaseCandidate) bool {
	if current == nil || !subscriptionLeaseCandidateMatches(*current, candidate) ||
		subscriptionLeaseCandidateTerminal(*current, candidate) {
		return false
	}
	if candidate.pending {
		current.pendingJoin.leaseTerminal = true
		if current.pendingJoin.timer != nil {
			current.pendingJoin.timer.Stop()
		}
		return true
	}
	current.subscriptionLeaseTerminal = true
	current.subscriptionContextGeneration++
	return true
}

func (g *Gate) observeSubscriptionLeaseFailClosed(metrics *Metrics, reason string) {
	if metrics == nil {
		return
	}
	metrics.SubscriptionLeaseFailClosed.WithLabelValues(reason).Inc()
}

func (g *Gate) dispatchExpiredSubscriptionLeases(candidates []subscriptionLeaseCandidate) {
	metrics := g.metrics
	for _, candidate := range candidates {
		if metrics != nil {
			phase := LeasePhaseActive
			if candidate.pending {
				phase = LeasePhasePending
			}
			metrics.SubscriptionLeaseLocalExpired.WithLabelValues(phase).Inc()
		}
		g.dispatchSubscriptionLeaseExpired(candidate)
	}
}

func (g *Gate) dispatchSubscriptionLeaseExpired(candidate subscriptionLeaseCandidate) {
	g.mu.Lock()
	registry := g.downlink
	g.mu.Unlock()
	queued := false
	if registry != nil {
		item, err := subscriptionLeaseExpiredItem(candidate)
		if err == nil {
			response, deliverErr := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
				Items: []*logicrpcpb.DownlinkItem{item},
			})
			queued = deliverErr == nil && response != nil && len(response.Results) == 1 &&
				(response.Results[0].Status == logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED ||
					response.Results[0].Status == logicrpcpb.DownlinkItemStatus_QUEUE_FULL)
		}
	}
	if !queued {
		g.terminateSubscription(downlink.SubscriptionTermination{
			ConnectionID: candidate.connectionID, Generation: candidate.generation,
			FarmID: candidate.farmID, SubscriptionID: candidate.subscriptionID,
			JoinAttemptID: candidate.joinAttemptID, PresenceEpoch: candidate.presenceEpoch,
		})
	}
	g.cleanupSubscriptionAsync(&candidate.binding, candidate.generation, candidate.farmID,
		candidate.subscriptionID, candidate.joinAttemptID,
		logicrpcpb.CleanupReason_CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED)
}

func subscriptionLeaseExpiredItem(candidate subscriptionLeaseCandidate) (*logicrpcpb.DownlinkItem, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&farmpb.RejoinRequiredPush{
		Reason: farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST,
	})
	if err != nil {
		return nil, err
	}
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_REJOIN_REQUIRED,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: candidate.binding.PlayerID,
				TargetFarmId:   candidate.farmID,
				SubscriptionId: candidate.subscriptionID,
			},
		},
		Payload: payload,
	}
	frame, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(1, push))
	if err != nil {
		return nil, err
	}
	target := candidate.targetFence()
	return &logicrpcpb.DownlinkItem{
		ItemId:              fmt.Sprintf("subscription-lease-expired:%d:%s:%s", candidate.farmID, candidate.subscriptionID, candidate.joinAttemptID),
		SubscriptionId:      candidate.subscriptionID,
		ConnectionId:        candidate.connectionID,
		PresenceEpoch:       candidate.presenceEpoch,
		Frame:               frame,
		TransportGeneration: candidate.generation,
		GatewayId:           target.GatewayId,
		JoinFence: &logicrpcpb.SubscriptionFence{
			FarmId:         candidate.farmID,
			SubscriptionId: candidate.subscriptionID,
			JoinAttemptId:  candidate.joinAttemptID,
		},
		TargetFence: target,
	}, nil
}
