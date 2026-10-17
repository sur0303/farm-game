package authority

import (
	"context"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// snapshotCoalesceWaiter is one Drain snapshot mutation waiting to share a
// single GlobalRevision+1 CAS. Cond.Wait releases Manager.mu so other shards
// that have already committed Authority can join the same-state cohort.
// GlobalRevision stays a cursor, not a fence. Waiters still in Stop/Wait/Facts
// are not in this queue and are not waited on.
type snapshotCoalesceWaiter struct {
	ctx                 context.Context
	memo                *transitionMemo
	state               actorrouting.ShardRouteState
	authority           authoritystore.ShardWriteAuthority
	endpoint            string
	authorityActivation bool
	projectionComplete  bool
	enqueuedAt          time.Time
	done                bool
	err                 error
}

type snapshotCohort struct {
	state   actorrouting.ShardRouteState
	waiters []*snapshotCoalesceWaiter
	first   time.Time
	last    time.Time
}

func (m *RouteAssignmentManager) shouldCoalesceDrainSnapshotLocked(memo *transitionMemo) bool {
	if m == nil || m.snapshotCoalesceWindow <= 0 || m.snapshotCoalesceCond == nil || memo == nil {
		return false
	}
	if !memo.request.internalDrain || memo.request.mode != TransferModeDrainHandoff {
		return false
	}
	if _, open := m.authorityWaveBatchMemberStage(memo.request.domain, memo.request.virtualShard); open {
		return false
	}
	return true
}

func (m *RouteAssignmentManager) coalesceDrainSnapshotLocked(
	ctx context.Context,
	memo *transitionMemo,
	state actorrouting.ShardRouteState,
	authority authoritystore.ShardWriteAuthority,
	authorityActivation bool,
	projectionComplete bool,
) error {
	waiter := &snapshotCoalesceWaiter{
		ctx:                 ctx,
		memo:                memo,
		state:               state,
		authority:           authority,
		endpoint:            memo.request.endpoint,
		authorityActivation: authorityActivation,
		projectionComplete:  projectionComplete,
		enqueuedAt:          time.Now(),
	}
	m.pendingSnapshotWaiters = append(m.pendingSnapshotWaiters, waiter)
	m.scheduleSnapshotFlushLocked()
	for !waiter.done {
		m.snapshotCoalesceCond.Wait()
	}
	return waiter.err
}

func (m *RouteAssignmentManager) scheduleSnapshotFlushLocked() {
	if m.snapshotFlushScheduled || m.snapshotFlushInFlight {
		m.signalSnapshotFlush()
		return
	}
	m.snapshotFlushScheduled = true
	go m.flushCoalescedSnapshots()
}

func (m *RouteAssignmentManager) signalSnapshotFlush() {
	if m == nil || m.snapshotFlushWake == nil {
		return
	}
	select {
	case m.snapshotFlushWake <- struct{}{}:
	default:
	}
}

func (m *RouteAssignmentManager) drainSnapshotFlushWake() {
	if m == nil || m.snapshotFlushWake == nil {
		return
	}
	select {
	case <-m.snapshotFlushWake:
	default:
	}
}

func (m *RouteAssignmentManager) finishCanceledSnapshotWaitersLocked() {
	if len(m.pendingSnapshotWaiters) == 0 {
		return
	}
	kept := m.pendingSnapshotWaiters[:0]
	canceled := false
	for _, waiter := range m.pendingSnapshotWaiters {
		if waiter == nil {
			continue
		}
		if waiter.ctx != nil && waiter.ctx.Err() != nil {
			waiter.err = waiter.ctx.Err()
			waiter.done = true
			canceled = true
			continue
		}
		kept = append(kept, waiter)
	}
	m.pendingSnapshotWaiters = kept
	if canceled {
		m.snapshotCoalesceCond.Broadcast()
	}
}

func (m *RouteAssignmentManager) snapshotCohortsLocked() []snapshotCohort {
	var assigning, active []*snapshotCoalesceWaiter
	for _, waiter := range m.pendingSnapshotWaiters {
		if waiter == nil {
			continue
		}
		if waiter.state == actorrouting.ShardStateActive {
			active = append(active, waiter)
			continue
		}
		assigning = append(assigning, waiter)
	}
	cohorts := make([]snapshotCohort, 0, 2)
	if cohort, ok := snapshotCohortFromWaiters(actorrouting.ShardStateAssigning, assigning); ok {
		cohorts = append(cohorts, cohort)
	}
	if cohort, ok := snapshotCohortFromWaiters(actorrouting.ShardStateActive, active); ok {
		cohorts = append(cohorts, cohort)
	}
	return cohorts
}

func snapshotCohortFromWaiters(state actorrouting.ShardRouteState, waiters []*snapshotCoalesceWaiter) (snapshotCohort, bool) {
	if len(waiters) == 0 {
		return snapshotCohort{}, false
	}
	cohort := snapshotCohort{state: state, waiters: waiters, first: waiters[0].enqueuedAt, last: waiters[0].enqueuedAt}
	for _, waiter := range waiters[1:] {
		if waiter.enqueuedAt.Before(cohort.first) {
			cohort.first = waiter.enqueuedAt
		}
		if waiter.enqueuedAt.After(cohort.last) {
			cohort.last = waiter.enqueuedAt
		}
	}
	return cohort, true
}

func (m *RouteAssignmentManager) snapshotCohortReadyLocked(cohort snapshotCohort, now time.Time) bool {
	if m.snapshotCoalesceBatch > 0 && len(cohort.waiters) >= m.snapshotCoalesceBatch {
		return true
	}
	if m.snapshotCoalesceMaxWait > 0 && !cohort.first.IsZero() && !now.Before(cohort.first.Add(m.snapshotCoalesceMaxWait)) {
		return true
	}
	return m.snapshotCoalesceWindow > 0 && !cohort.last.IsZero() && !now.Before(cohort.last.Add(m.snapshotCoalesceWindow))
}

func (m *RouteAssignmentManager) snapshotShouldFlushLocked() bool {
	now := time.Now()
	for _, cohort := range m.snapshotCohortsLocked() {
		if m.snapshotCohortReadyLocked(cohort, now) {
			return true
		}
	}
	return false
}

func (m *RouteAssignmentManager) snapshotFlushDeadlineLocked() time.Time {
	var deadline time.Time
	for _, cohort := range m.snapshotCohortsLocked() {
		if m.snapshotCoalesceBatch > 0 && len(cohort.waiters) >= m.snapshotCoalesceBatch {
			return time.Time{}
		}
		candidates := make([]time.Time, 0, 2)
		if m.snapshotCoalesceWindow > 0 && !cohort.last.IsZero() {
			candidates = append(candidates, cohort.last.Add(m.snapshotCoalesceWindow))
		}
		if m.snapshotCoalesceMaxWait > 0 && !cohort.first.IsZero() {
			candidates = append(candidates, cohort.first.Add(m.snapshotCoalesceMaxWait))
		}
		for _, candidate := range candidates {
			if deadline.IsZero() || candidate.Before(deadline) {
				deadline = candidate
			}
		}
	}
	return deadline
}

func (m *RouteAssignmentManager) takeFlushableSnapshotWaitersLocked() []*snapshotCoalesceWaiter {
	now := time.Now()
	cohorts := m.snapshotCohortsLocked()
	var chosen snapshotCohort
	for _, cohort := range cohorts {
		if m.snapshotCoalesceBatch > 0 && len(cohort.waiters) >= m.snapshotCoalesceBatch {
			chosen = cohort
			break
		}
	}
	if len(chosen.waiters) == 0 {
		for _, cohort := range cohorts {
			if !m.snapshotCohortReadyLocked(cohort, now) {
				continue
			}
			if len(chosen.waiters) == 0 || cohort.first.Before(chosen.first) {
				chosen = cohort
			}
		}
	}
	if len(chosen.waiters) == 0 {
		return nil
	}
	limit := len(chosen.waiters)
	if m.snapshotCoalesceBatch > 0 && limit > m.snapshotCoalesceBatch {
		limit = m.snapshotCoalesceBatch
	}
	taken := chosen.waiters[:limit]
	remaining := make([]*snapshotCoalesceWaiter, 0, len(m.pendingSnapshotWaiters)-len(taken))
	seen := make(map[*snapshotCoalesceWaiter]struct{}, len(taken))
	for _, waiter := range taken {
		seen[waiter] = struct{}{}
	}
	for _, waiter := range m.pendingSnapshotWaiters {
		if _, ok := seen[waiter]; ok {
			continue
		}
		remaining = append(remaining, waiter)
	}
	m.pendingSnapshotWaiters = remaining
	return taken
}

func (m *RouteAssignmentManager) waitUntilSnapshotFlush() {
	for {
		m.mu.Lock()
		m.finishCanceledSnapshotWaitersLocked()
		if len(m.pendingSnapshotWaiters) == 0 || m.snapshotShouldFlushLocked() {
			m.mu.Unlock()
			return
		}
		deadline := m.snapshotFlushDeadlineLocked()
		m.mu.Unlock()
		wait := time.Until(deadline)
		if deadline.IsZero() || wait <= 0 {
			return
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			return
		case <-m.snapshotFlushWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		m.drainSnapshotFlushWake()
	}
}

func (m *RouteAssignmentManager) flushCoalescedSnapshots() {
	for {
		m.waitUntilSnapshotFlush()
		m.mu.Lock()
		m.finishCanceledSnapshotWaitersLocked()
		waiters := m.takeFlushableSnapshotWaitersLocked()
		if len(waiters) == 0 {
			if len(m.pendingSnapshotWaiters) == 0 {
				m.snapshotFlushScheduled = false
				m.snapshotFlushInFlight = false
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
			continue
		}
		m.snapshotFlushInFlight = true
		m.snapshotFlushScheduled = false
		current := actorrouting.CloneRouteSnapshot(m.snapshot)
		m.mu.Unlock()

		next, authority, wave, mergeErr := m.mergeCoalescedSnapshots(current, waiters)
		var publishErr error
		if mergeErr != nil {
			publishErr = mergeErr
		} else {
			publishErr = m.publishSnapshotKind(m.snapshotPublishContext(waiters), next, authority, wave)
			if publishErr != nil {
				publishErr = fmt.Errorf("%w: coalesced snapshot: %v", ErrSnapshotPublish, publishErr)
			}
		}

		m.mu.Lock()
		if publishErr == nil {
			m.snapshot = next
			for _, waiter := range waiters {
				m.finishCoalescedWaiterLocked(waiter, next)
			}
		} else {
			for _, waiter := range waiters {
				if waiter == nil {
					continue
				}
				waiter.err = publishErr
				waiter.done = true
			}
		}
		m.snapshotFlushInFlight = false
		m.snapshotCoalesceCond.Broadcast()
		if len(m.pendingSnapshotWaiters) == 0 {
			m.snapshotFlushScheduled = false
			m.mu.Unlock()
			return
		}
		m.snapshotFlushScheduled = true
		m.mu.Unlock()
	}
}

func (m *RouteAssignmentManager) mergeCoalescedSnapshots(
	current actorrouting.RouteSnapshot,
	waiters []*snapshotCoalesceWaiter,
) (actorrouting.RouteSnapshot, bool, bool, error) {
	if len(waiters) == 0 {
		return actorrouting.RouteSnapshot{}, false, false, fmt.Errorf("%w: empty coalesced snapshot batch", ErrSnapshotPublish)
	}
	baseRevision := current.GlobalRevision
	next := current
	seen := make(map[authorityKey]struct{}, len(waiters))
	authority := false
	state := waiters[0].state
	for _, waiter := range waiters {
		if waiter == nil || waiter.memo == nil {
			return actorrouting.RouteSnapshot{}, false, false, fmt.Errorf("%w: missing coalesced snapshot waiter", ErrSnapshotPublish)
		}
		if waiter.state != state {
			return actorrouting.RouteSnapshot{}, false, false, fmt.Errorf("%w: coalesced snapshot mixed %s and %s", ErrSnapshotPublish, state, waiter.state)
		}
		key := authorityKey{domain: waiter.authority.Domain, shard: waiter.authority.VirtualShard}
		if _, dup := seen[key]; dup {
			return actorrouting.RouteSnapshot{}, false, false, fmt.Errorf("%w: coalesced snapshot repeated %s/%d", ErrSnapshotPublish, key.domain, key.shard)
		}
		seen[key] = struct{}{}
		var err error
		next, err = m.snapshotForAuthority(next, waiter.authority, waiter.state, waiter.endpoint)
		if err != nil {
			return actorrouting.RouteSnapshot{}, false, false, err
		}
		authority = authority || waiter.authorityActivation
	}
	if baseRevision == ^uint64(0) {
		return actorrouting.RouteSnapshot{}, false, false, fmt.Errorf("%w: global revision overflow", ErrInvalidManager)
	}
	next.GlobalRevision = baseRevision + 1
	wave := authority && len(waiters) > 1
	if authority {
		if err := actorrouting.ValidateAuthorityWaveSnapshotUpdate(current, next); err != nil {
			return actorrouting.RouteSnapshot{}, false, false, err
		}
	} else if err := actorrouting.ValidateRouteSnapshotUpdate(current, next); err != nil {
		return actorrouting.RouteSnapshot{}, false, false, err
	}
	return next, authority, wave, nil
}

func (m *RouteAssignmentManager) snapshotPublishContext(waiters []*snapshotCoalesceWaiter) context.Context {
	for _, waiter := range waiters {
		if waiter != nil && waiter.ctx != nil && waiter.ctx.Err() == nil {
			return waiter.ctx
		}
	}
	for _, waiter := range waiters {
		if waiter != nil && waiter.ctx != nil {
			return waiter.ctx
		}
	}
	return context.Background()
}

func (m *RouteAssignmentManager) finishCoalescedWaiterLocked(waiter *snapshotCoalesceWaiter, next actorrouting.RouteSnapshot) {
	if waiter == nil {
		return
	}
	memo := waiter.memo
	if memo == nil {
		waiter.err = fmt.Errorf("%w: missing coalesced snapshot memo", ErrSnapshotPublish)
		waiter.done = true
		return
	}
	previousState, previousRevision, previousPublished, previousPending, previousUsedAt := memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt
	memo.lastState = waiter.state
	if waiter.state == actorrouting.ShardStateAssigning && memo.publishedSnapshotRevision == 0 {
		memo.publishedSnapshotRevision = next.GlobalRevision
	}
	memo.published = true
	memo.projectionPending = !waiter.projectionComplete
	memo.lastUsedAt = m.now()
	if err := m.updateRecoveryLocked(memo); err != nil {
		memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt = previousState, previousRevision, previousPublished, previousPending, previousUsedAt
		waiter.err = fmt.Errorf("%w: transition recovery update: %v", ErrSnapshotPublish, err)
		waiter.done = true
		return
	}
	waiter.done = true
}
