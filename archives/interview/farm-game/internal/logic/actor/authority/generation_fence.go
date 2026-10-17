// Package authority hosts the generation-fenced narrow primitives used by the
// RT1B-3A real membership adapter.  They are additive: they never change the
// semantics of the existing Claim/Transfer/Drain/Renew/WriteFence paths, only
// expose compare-and-act guard operations to the local membership reconciler.
package authority

import (
	"errors"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// ErrGenerationMismatch reports that an installed guard's generation differs
// from the expected generation in a generation-fenced Port call.
var ErrGenerationMismatch = errors.New("routeauthority: installed guard generation mismatch")

// authorityGenerationEqual reports whether two authorities share the exact
// generation identity: layout, hash profile, domain, shard, holder, boot and
// the three epochs.  Hash profile is part of the exact identity (a different
// HashVersion/HashSeed means a different persistence layout, never a same
// generation); it is compared for equality only and never ordered.
func authorityGenerationEqual(a, b authoritystore.ShardWriteAuthority) bool {
	return a.AuthorityLayoutVersion == b.AuthorityLayoutVersion &&
		a.HashVersion == b.HashVersion && a.HashSeed == b.HashSeed &&
		a.Domain == b.Domain && a.VirtualShard == b.VirtualShard &&
		a.HolderID == b.HolderID && a.HolderBootID == b.HolderBootID &&
		a.AssignmentRevision == b.AssignmentRevision &&
		a.WriterEpoch == b.WriterEpoch && a.LeaseEpoch == b.LeaseEpoch
}

// InstallIfMatches admits the given grant when no guard is installed, when the
// installed guard already carries exactly the expected generation (idempotent
// re-install), or when the expected generation is strictly newer than the
// installed one (a legal upgrade).  A stale install whose expected generation
// is not strictly newer than the installed guard is rejected, so an old
// install can never replace a newer generation.  Lease refresh is the renewal
// loop's job, not re-install's.
func (r *LeaseGuardRegistry) InstallIfMatches(domain actorrouting.RouteDomain, shard uint32, expected authoritystore.ShardWriteAuthority, guard *authoritystore.HolderLeaseGuard, requestStarted time.Time, result authoritystore.ShardAuthorityTransitionResult, now func() time.Time) (*HolderLeaseRuntime, error) {
	if r == nil || guard == nil {
		return nil, fmt.Errorf("%w: guard registry and guard are required", ErrLeaseAdmission)
	}
	if handle, ok := r.handle(domain, shard); ok && handle != nil {
		snapshot := handle.Snapshot()
		if snapshot.Authority.AssignmentRevision != 0 {
			if authorityGenerationEqual(snapshot.Authority, expected) {
				// Same generation already admitted: reuse the existing runtime
				// and refresh its local monotonic deadline from this prepare's
				// accepted TTL.  Never create a second guard.  If the local
				// deadline has already passed, InstallGrant fences and rejects
				// the install so the shard stays fail-closed.
				if err := handle.InstallGrant(requestStarted, result); err != nil {
					return nil, err
				}
				return &HolderLeaseRuntime{handle: handle, authorityNow: now, localNow: now}, nil
			}
			if !generationStrictlyNewer(generationOf(expected), generationOf(snapshot.Authority)) {
				return nil, fmt.Errorf("%w: installed generation is not strictly older than expected", ErrGenerationMismatch)
			}
		}
	}
	return InstallHolderLeaseGrant(r, domain, shard, guard, requestStarted, result, now)
}

// SnapshotAll returns every installed guard's coherent snapshot.  Each
// snapshot's Authority carries Domain/VirtualShard so callers can reconstruct
// the shard keys; used by consistency tests and future schedulers.
func (r *LeaseGuardRegistry) SnapshotAll() []LeaseGenerationSnapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]LeaseGenerationSnapshot, 0, len(r.handles))
	for _, handle := range r.handles {
		if handle == nil {
			continue
		}
		out = append(out, handle.Snapshot())
	}
	return out
}

// FenceIfMatches fences the installed guard only when it carries exactly the
// expected generation.  A guard for a different generation is left untouched,
// so an old REMOVE can never fence the new generation.  Fencing is a purely
// local operation: it immediately flips CanServeShard to false without any
// network round trip.
func (r *LeaseGuardRegistry) FenceIfMatches(domain actorrouting.RouteDomain, shard uint32, expected authoritystore.ShardWriteAuthority) error {
	if r == nil {
		return fmt.Errorf("%w: guard registry required", ErrLeaseAdmission)
	}
	handle, ok := r.handle(domain, shard)
	if !ok || handle == nil {
		return fmt.Errorf("%w: no guard installed", ErrLeaseAdmission)
	}
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision != 0 && !authorityGenerationEqual(snapshot.Authority, expected) {
		return fmt.Errorf("%w: installed generation differs from expected", ErrGenerationMismatch)
	}
	handle.FenceForReason(LeaseFenceMembershipRemoved)
	return nil
}

// RemoveIfMatches removes the installed guard only when it carries exactly the
// expected generation.  A guard for a different generation is never removed,
// so an old REMOVE cannot delete a newer guard.
func (r *LeaseGuardRegistry) RemoveIfMatches(domain actorrouting.RouteDomain, shard uint32, expected authoritystore.ShardWriteAuthority) error {
	if r == nil {
		return fmt.Errorf("%w: guard registry required", ErrLeaseAdmission)
	}
	handle, ok := r.handle(domain, shard)
	if !ok || handle == nil {
		return fmt.Errorf("%w: no guard installed", ErrLeaseAdmission)
	}
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision != 0 && !authorityGenerationEqual(snapshot.Authority, expected) {
		return fmt.Errorf("%w: installed generation differs from expected", ErrGenerationMismatch)
	}
	handle.FenceForReason(LeaseFenceMembershipRemoved)
	r.Remove(domain, shard)
	return nil
}

// RefreshOrRearmIfMatches applies a successful prepared renewal locally, but
// only while the installed guard still carries exactly the expected generation
// and its fence reason is recoverable (None or LocalDeadlineExpired).  A guard
// removed, replaced, or fenced by a terminal reason rejects the rearm so a late
// renewal can never reopen a removed shard.  On success the local monotonic
// deadline is rebuilt from the accepted TTL and the guard is admitted again.
func (r *LeaseGuardRegistry) RefreshOrRearmIfMatches(domain actorrouting.RouteDomain, shard uint32, expected authoritystore.ShardWriteAuthority, prepared PreparedLeaseRenew) error {
	if r == nil {
		return fmt.Errorf("%w: guard registry required", ErrLeaseAdmission)
	}
	handle, ok := r.handle(domain, shard)
	if !ok || handle == nil {
		return fmt.Errorf("%w: no guard installed", ErrLeaseAdmission)
	}
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision == 0 || !authorityGenerationEqual(snapshot.Authority, expected) {
		return fmt.Errorf("%w: installed generation differs from expected", ErrGenerationMismatch)
	}
	// The reason gate and the guard layer's revoked fence are both enforced:
	// a terminal reason rejects the rearm here, and even a recoverable reason
	// cannot revive a guard the repository already revoked (ErrStaleLeaseGrant
	// inside InstallGrant).
	reason := handle.FenceReason()
	if !isRecoverableFenceReason(reason) {
		return fmt.Errorf("%w: guard fence reason %d is not recoverable", ErrGenerationMismatch, reason)
	}
	result := authoritystore.ShardAuthorityTransitionResult{
		Status:        authoritystore.ShardTransitionApplied,
		Authority:     prepared.Authority,
		AcceptedTTLms: prepared.AcceptedTTLMs,
	}
	if err := handle.InstallGrant(prepared.RequestStarted, result); err != nil {
		return err
	}
	return nil
}
