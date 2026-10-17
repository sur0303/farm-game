// Package authority hosts the generation-fenced lease renewal primitive used
// by the RT1B-3B per-shard lease scheduler.  It is additive: the existing
// Renew keeps its fail-closed semantics; RenewWithFence adds the classification
// the scheduler needs (transient vs lost generation) without touching Renew.
package authority

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// LeaseFenceReason distinguishes why a local guard was fenced.  Only
// LeaseFenceLocalDeadlineExpired is recoverable by an exact-generation renewal;
// every other reason is terminal for that generation.
type LeaseFenceReason uint8

const (
	// LeaseFenceNone means the guard is not fenced.
	LeaseFenceNone LeaseFenceReason = iota
	// LeaseFenceLocalDeadlineExpired: the local safety deadline passed while the
	// authority lease may still be valid.  Writes are paused but an
	// exact-generation renewal may rearm the guard.
	LeaseFenceLocalDeadlineExpired
	// LeaseFenceMembershipRemoved: the assignment was removed or replaced.  The
	// old generation can never recover.
	LeaseFenceMembershipRemoved
	// LeaseFenceLostGeneration: the repository explicitly returned
	// FenceMismatch/Expired/Unauthorized etc.  Only a new assignment recovers.
	LeaseFenceLostGeneration
	// LeaseFenceRuntimeFatal: runtime shutdown or a fatal condition.
	LeaseFenceRuntimeFatal
)

// isRecoverableFenceReason reports whether an exact-generation renewal may
// rearm the guard.  Only an unreasoned or local-deadline-expired guard is
// recoverable; every terminal reason is sticky.
func isRecoverableFenceReason(reason LeaseFenceReason) bool {
	return reason == LeaseFenceNone || reason == LeaseFenceLocalDeadlineExpired
}

// isTerminalFenceReason reports whether the reason is terminal for the exact
// generation.  A terminal reason can never be downgraded by a later local
// deadline expiry and can never be rearmed by a same-generation renewal.
func isTerminalFenceReason(reason LeaseFenceReason) bool {
	switch reason {
	case LeaseFenceMembershipRemoved, LeaseFenceLostGeneration, LeaseFenceRuntimeFatal:
		return true
	default:
		return false
	}
}

// LeaseRenewOutcome classifies one lease renewal attempt for the scheduler.
type LeaseRenewOutcome uint8

const (
	// LeaseRenewSuccess: the store accepted the renewal and the local monotonic
	// deadline was refreshed from the accepted TTL.
	LeaseRenewSuccess LeaseRenewOutcome = iota
	// LeaseRenewTransient: the store was unreachable or the result is
	// indeterminate; the guard is NOT fenced and the shard keeps serving until
	// its local deadline (bounded retry with backoff).
	LeaseRenewTransient
	// LeaseRenewLostGeneration: the store proved the holder lost this
	// generation (fence mismatch / lease expired / not found / unauthorized /
	// reconcile required); the guard is fenced immediately and only this shard
	// is affected.
	LeaseRenewLostGeneration
	// LeaseRenewExpired: the local monotonic deadline has already passed; the
	// guard is fenced and no new actor turn may start.
	LeaseRenewExpired
)

var (
	// ErrRenewLostGeneration reports that the holder lost the shard generation.
	ErrRenewLostGeneration = errors.New("routeauthority: holder lost shard generation")
	// ErrRenewTransient reports an indeterminate or transport-level renewal
	// failure that must not fence the guard.
	ErrRenewTransient = errors.New("routeauthority: lease renewal transient failure")
)

// RenewWithFence renews the shard lease with generation-fenced failure
// semantics:
//
//   - transport errors and indeterminate statuses (UnavailableNotSent /
//     TimeoutUnknown) yield LeaseRenewTransient and never fence the guard, so
//     the shard keeps serving until its local monotonic deadline;
//   - authority statuses that prove the holder lost the generation
//     (FenceMismatch / LeaseExpired / NotFound / Unauthorized /
//     ReconcileRequired) yield LeaseRenewLostGeneration and fence the guard
//     immediately, locally, with no network round trip;
//   - success refreshes only the local monotonic deadline from the accepted
//     TTL; if the local deadline has already passed, InstallGrant rejects and
//     the guard stays fenced (LeaseRenewExpired).
func (r *HolderLeaseRuntime) RenewWithFence(ctx context.Context, store authoritystore.ShardWriteAuthorityStore, req authoritystore.RenewShardAuthorityRequest) (LeaseRenewOutcome, authoritystore.ShardAuthorityTransitionResult, error) {
	if r == nil || r.handle == nil || store == nil {
		return LeaseRenewLostGeneration, authoritystore.ShardAuthorityTransitionResult{}, ErrRenewLostGeneration
	}
	if req.Caller.Kind != authoritystore.ShardAuthorityCallerHolder || req.Caller.InstanceID == "" || req.Caller.BootID == "" {
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, authoritystore.ShardAuthorityTransitionResult{}, ErrRenewLostGeneration
	}
	snapshot := r.handle.Snapshot()
	authority := snapshot.Authority
	if !snapshot.Admitted || authority.HolderID != req.Caller.InstanceID || authority.HolderBootID != req.Caller.BootID ||
		authority.Domain != req.Domain || authority.VirtualShard != req.VirtualShard || !fenceEqualsAuthority(req.ExpectedFence, authority) {
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionFenceMismatch}, ErrRenewLostGeneration
	}
	requestStarted := r.localNow()
	result, err := store.RenewShardAuthority(ctx, req)
	if err != nil {
		// Transport error (or context cancellation): indeterminate, never fence.
		return LeaseRenewTransient, result, fmt.Errorf("%w: %v", ErrRenewTransient, err)
	}
	switch result.Status {
	case authoritystore.ShardTransitionApplied, authoritystore.ShardTransitionAlreadyApplied:
		if !renewalMatchesRequest(req, authority, result) {
			r.handle.FenceForReason(LeaseFenceLostGeneration)
			return LeaseRenewLostGeneration, result, ErrRenewGenerationChanged
		}
		if err := r.handle.InstallGrant(requestStarted, result); err != nil {
			r.handle.FenceForReason(LeaseFenceLostGeneration)
			return LeaseRenewExpired, result, err
		}
		return LeaseRenewSuccess, result, nil
	case authoritystore.ShardTransitionFenceMismatch, authoritystore.ShardTransitionLeaseExpired,
		authoritystore.ShardTransitionNotFound, authoritystore.ShardTransitionUnauthorized,
		authoritystore.ShardTransitionReconcileRequired:
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, result, fmt.Errorf("%w: status=%s", ErrRenewLostGeneration, result.Status)
	default: // UnavailableNotSent / TimeoutUnknown: indeterminate, never fence.
		return LeaseRenewTransient, result, fmt.Errorf("%w: status=%s", ErrRenewTransient, result.Status)
	}
}

// PreparedLeaseRenew is the outcome of a successful remote renewal that has not
// yet been applied locally.  The scheduler re-validates token/generation/
// installed/desired before calling RefreshOrRearmIfMatches.
type PreparedLeaseRenew struct {
	RequestStarted time.Time
	AcceptedTTLMs  int64
	Authority      authoritystore.ShardWriteAuthority
}

// PrepareRenew performs the remote proof even when the local guard is fenced by
// a LocalDeadlineExpired reason (the authority lease may still be valid), and
// returns a PreparedLeaseRenew without any local side effect.  A guard fenced
// by MembershipRemoved/LostGeneration/RuntimeFatal is terminal: PrepareRenew
// refuses without touching Redis.
func (r *HolderLeaseRuntime) PrepareRenew(ctx context.Context, store authoritystore.ShardWriteAuthorityStore, req authoritystore.RenewShardAuthorityRequest) (LeaseRenewOutcome, *PreparedLeaseRenew, error) {
	if r == nil || r.handle == nil || store == nil {
		return LeaseRenewLostGeneration, nil, ErrRenewLostGeneration
	}
	if req.Caller.Kind != authoritystore.ShardAuthorityCallerHolder || req.Caller.InstanceID == "" || req.Caller.BootID == "" {
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, nil, ErrRenewLostGeneration
	}
	snapshot := r.handle.Snapshot()
	authority := snapshot.Authority
	if reason := r.handle.FenceReason(); reason != LeaseFenceNone && reason != LeaseFenceLocalDeadlineExpired {
		// MembershipRemoved / LostGeneration / RuntimeFatal are terminal.
		return LeaseRenewLostGeneration, nil, ErrRenewLostGeneration
	}
	if authority.HolderID != req.Caller.InstanceID || authority.HolderBootID != req.Caller.BootID ||
		authority.Domain != req.Domain || authority.VirtualShard != req.VirtualShard || !fenceEqualsAuthority(req.ExpectedFence, authority) {
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, nil, ErrRenewLostGeneration
	}
	requestStarted := r.localNow()
	result, err := store.RenewShardAuthority(ctx, req)
	if err != nil {
		// Transport error or context cancellation: indeterminate, never fence.
		return LeaseRenewTransient, nil, fmt.Errorf("%w: %v", ErrRenewTransient, err)
	}
	switch result.Status {
	case authoritystore.ShardTransitionApplied, authoritystore.ShardTransitionAlreadyApplied:
		if !renewalMatchesRequest(req, authority, result) {
			r.handle.FenceForReason(LeaseFenceLostGeneration)
			return LeaseRenewLostGeneration, nil, ErrRenewGenerationChanged
		}
		if result.AcceptedTTLms <= 0 {
			r.handle.FenceForReason(LeaseFenceLostGeneration)
			return LeaseRenewLostGeneration, nil, ErrRenewLostGeneration
		}
		return LeaseRenewSuccess, &PreparedLeaseRenew{
			RequestStarted: requestStarted,
			AcceptedTTLMs:  result.AcceptedTTLms,
			Authority:      result.Authority,
		}, nil
	case authoritystore.ShardTransitionFenceMismatch, authoritystore.ShardTransitionLeaseExpired,
		authoritystore.ShardTransitionNotFound, authoritystore.ShardTransitionUnauthorized,
		authoritystore.ShardTransitionReconcileRequired:
		r.handle.FenceForReason(LeaseFenceLostGeneration)
		return LeaseRenewLostGeneration, nil, fmt.Errorf("%w: status=%s", ErrRenewLostGeneration, result.Status)
	default: // UnavailableNotSent / TimeoutUnknown: indeterminate, never fence.
		return LeaseRenewTransient, nil, fmt.Errorf("%w: status=%s", ErrRenewTransient, result.Status)
	}
}

// RenewWithFenceRequest builds a holder renewal request for one shard.  The
// expected generation is always the one the scheduler registered for the entry.
func RenewWithFenceRequest(layout string, domain actorrouting.RouteDomain, shard uint32, holderID, holderBootID string, assignment, writer, lease uint64, leaseTTLms int64) authoritystore.RenewShardAuthorityRequest {
	return authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerHolder,
			InstanceID: holderID,
			BootID:     holderBootID,
		},
		AuthorityLayoutVersion: layout,
		Domain:                 domain,
		VirtualShard:           shard,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID:           holderID,
			ExpectedHolderBootID:       holderBootID,
			ExpectedAssignmentRevision: assignment,
			ExpectedWriterEpoch:        writer,
			ExpectedLeaseEpoch:         lease,
		},
		RequestedTTLms: leaseTTLms,
	}
}
