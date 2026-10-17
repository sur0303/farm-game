package authoritystore

import (
	"fmt"
	"sync"
	"time"
)

type HolderLeaseGuardState uint8

const (
	HolderLeaseGuardEmpty HolderLeaseGuardState = iota
	HolderLeaseGuardActive
	HolderLeaseGuardFenced
)

// HolderLeaseGuard is the hot-path, process-local admission fence. The
// requestStarted value must be captured before the store call using time.Now;
// Go then preserves its monotonic component through Add/Before.
type HolderLeaseGuard struct {
	mu           sync.Mutex
	holderID     string
	holderBootID string
	safetyMargin time.Duration
	now          func() time.Time
	state        HolderLeaseGuardState
	revoked      bool
	authority    ShardWriteAuthority
	deadline     time.Time
}

func NewHolderLeaseGuard(holderID, holderBootID string, safetyMargin time.Duration, now func() time.Time) (*HolderLeaseGuard, error) {
	if err := validateHolder(holderID, holderBootID); err != nil {
		return nil, err
	}
	if safetyMargin < 0 {
		return nil, fmt.Errorf("%w: negative safety margin", ErrInvalidRequest)
	}
	if now == nil {
		now = time.Now
	}
	return &HolderLeaseGuard{holderID: holderID, holderBootID: holderBootID, safetyMargin: safetyMargin, now: now}, nil
}

func (g *HolderLeaseGuard) InstallGrant(requestStarted time.Time, result ShardAuthorityTransitionResult) error {
	if g == nil {
		return fmt.Errorf("%w: nil lease guard", ErrInvalidRequest)
	}
	if result.Status != ShardTransitionApplied && result.Status != ShardTransitionAlreadyApplied {
		return fmt.Errorf("%w: non-success lease result %s", ErrInvalidRequest, result.Status)
	}
	observedNow := g.now()
	if requestStarted.After(observedNow) {
		return fmt.Errorf("%w: request start is in the future", ErrInvalidRequest)
	}
	authority := result.Authority
	if authority.State != ShardAuthorityActive || authority.HolderID != g.holderID || authority.HolderBootID != g.holderBootID || authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 || result.AcceptedTTLms <= 0 {
		return fmt.Errorf("%w: lease grant", ErrInvalidRequest)
	}
	ttl := time.Duration(result.AcceptedTTLms) * time.Millisecond
	deadline := requestStarted
	if ttl > g.safetyMargin {
		deadline = requestStarted.Add(ttl - g.safetyMargin)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != HolderLeaseGuardEmpty {
		if authority.Domain != g.authority.Domain || authority.VirtualShard != g.authority.VirtualShard {
			return fmt.Errorf("%w: authority address changed", ErrInconsistentLeaseGrant)
		}
		if authority.AssignmentRevision < g.authority.AssignmentRevision {
			return ErrStaleLeaseGrant
		}
		if g.revoked && authority.AssignmentRevision <= g.authority.AssignmentRevision {
			return ErrStaleLeaseGrant
		}
		if authority.AssignmentRevision == g.authority.AssignmentRevision {
			if !sameAuthorityGeneration(authority, g.authority) {
				return ErrInconsistentLeaseGrant
			}
			if deadline.Before(g.deadline) {
				deadline = g.deadline
			}
		} else if authority.WriterEpoch <= g.authority.WriterEpoch || authority.LeaseEpoch <= g.authority.LeaseEpoch {
			return ErrInconsistentLeaseGrant
		}
	}
	g.authority = authority
	g.deadline = deadline
	if !observedNow.Before(deadline) {
		g.state = HolderLeaseGuardFenced
		return ErrLeaseDeadlineExpired
	}
	g.revoked = false
	g.state = HolderLeaseGuardActive
	return nil
}

// ObserveAuthority immediately fences this holder when a newer data-plane
// authority tuple moves the shard away or unassigns it. A stale observation is
// ignored; it can never resurrect a fenced guard.
func (g *HolderLeaseGuard) ObserveAuthority(authority ShardWriteAuthority) error {
	if g == nil {
		return fmt.Errorf("%w: nil lease guard", ErrInvalidRequest)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == HolderLeaseGuardEmpty {
		return fmt.Errorf("%w: lease guard is empty", ErrInvalidRequest)
	}
	if authority.Domain != g.authority.Domain || authority.VirtualShard != g.authority.VirtualShard {
		return fmt.Errorf("%w: authority address changed", ErrInconsistentLeaseGrant)
	}
	if authority.AssignmentRevision <= g.authority.AssignmentRevision {
		return nil
	}
	g.revoked = true
	g.state = HolderLeaseGuardFenced
	if authority.WriterEpoch <= g.authority.WriterEpoch || authority.LeaseEpoch <= g.authority.LeaseEpoch {
		return ErrInconsistentLeaseGrant
	}
	g.authority = authority
	return nil
}

func (g *HolderLeaseGuard) CanAdmitWrite() bool     { return g.allow() }
func (g *HolderLeaseGuard) CanActivateActor() bool  { return g.allow() }
func (g *HolderLeaseGuard) CanStartActorTurn() bool { return g.allow() }

func (g *HolderLeaseGuard) Fence() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.revoked = true
	g.state = HolderLeaseGuardFenced
	g.mu.Unlock()
}

func (g *HolderLeaseGuard) Snapshot() (HolderLeaseGuardState, ShardWriteAuthority, time.Time) {
	if g == nil {
		return HolderLeaseGuardFenced, ShardWriteAuthority{}, time.Time{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expireLocked()
	return g.state, g.authority, g.deadline
}

func (g *HolderLeaseGuard) allow() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expireLocked()
	return g.state == HolderLeaseGuardActive
}

func (g *HolderLeaseGuard) expireLocked() {
	if g.state == HolderLeaseGuardActive && !g.now().Before(g.deadline) {
		g.state = HolderLeaseGuardFenced
	}
}

func sameAuthorityGeneration(a, b ShardWriteAuthority) bool {
	return a.AuthorityLayoutVersion == b.AuthorityLayoutVersion &&
		a.HashVersion == b.HashVersion && a.HashSeed == b.HashSeed &&
		a.Domain == b.Domain && a.VirtualShard == b.VirtualShard &&
		a.HolderID == b.HolderID && a.HolderBootID == b.HolderBootID &&
		a.AssignmentRevision == b.AssignmentRevision && a.WriterEpoch == b.WriterEpoch && a.LeaseEpoch == b.LeaseEpoch
}
