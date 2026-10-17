// Package authoritycomposition hosts the RT1B-3A real membership adapter: it
// implements the reconciler's MembershipInstaller/Fencer/Remover Ports against
// the real local authority seams (ShardWriteAuthority repository,
// HolderLeaseGuard, LeaseGuardRegistry and InstallHolderLeaseGrant).
//
// Install proves the full desired generation through the repository before it
// admits anything; Fence/Remove are generation-fenced compare-and-act local
// operations.  The adapter never traverses or loads actors and never depends
// on cross-process watch, cmd wiring or RouteManager changes.
package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

// RealMembershipAdapterOptions configures the adapter.  Store, Registry,
// layout, hash profile, holder identity and a positive LeaseTTL are required.
// HashVersion/HashSeed must equal the snapshot topology the reconciler was
// sealed with; the adapter uses them to build the compare-and-act generation
// handle for the guard layer.
type RealMembershipAdapterOptions struct {
	Store                  authoritystore.ShardWriteAuthorityStore
	Registry               *routeauthority.LeaseGuardRegistry
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	HolderID               string
	HolderBootID           string
	LeaseTTL               time.Duration
	SafetyMargin           time.Duration
	Now                    func() time.Time
}

// RealMembershipAdapter is the real local implementation of the reconciler
// membership Ports.
type RealMembershipAdapter struct {
	store        authoritystore.ShardWriteAuthorityStore
	registry     *routeauthority.LeaseGuardRegistry
	layout       string
	hashVersion  uint32
	hashSeed     uint64
	holderID     string
	holderBootID string
	leaseTTL     time.Duration
	safetyMargin time.Duration
	now          func() time.Time

	// commitPreparedHook is a TEST_ONLY injection point invoked (under the
	// reconciler state lock, via commitPrepared) immediately before the local
	// guard is installed.  It lets tests deterministically park an in-flight
	// commit that already passed the runtime gate but has not yet admitted
	// anything, proving the fatal cleanup linearizes after the last possible
	// install.  Production never sets it.
	commitPreparedHook func()
	// prepareMembershipHook is a TEST_ONLY context-aware network-phase seam.
	// Production never sets it; blocking tests use it to prove Runtime.Close
	// cancels and joins an observer that is inside a Port call.
	prepareMembershipHook func(context.Context) error
}

var _ MembershipInstaller = (*RealMembershipAdapter)(nil)
var _ MembershipFencer = (*RealMembershipAdapter)(nil)
var _ MembershipRemover = (*RealMembershipAdapter)(nil)

// NewRealMembershipAdapter validates the options and builds the adapter.
func NewRealMembershipAdapter(opts RealMembershipAdapterOptions) (*RealMembershipAdapter, error) {
	if opts.Store == nil {
		return nil, errors.New("authority composition: real membership adapter requires a ShardWriteAuthority store")
	}
	if opts.Registry == nil {
		return nil, errors.New("authority composition: real membership adapter requires a LeaseGuardRegistry")
	}
	if opts.AuthorityLayoutVersion == "" || opts.HolderID == "" || opts.HolderBootID == "" {
		return nil, errors.New("authority composition: real membership adapter requires layout and holder identity")
	}
	if opts.HashVersion == 0 || opts.HashSeed == 0 {
		return nil, errors.New("authority composition: real membership adapter requires the snapshot hash profile")
	}
	if opts.LeaseTTL <= 0 {
		return nil, errors.New("authority composition: real membership adapter requires a positive lease TTL")
	}
	if opts.SafetyMargin < 0 || opts.SafetyMargin >= opts.LeaseTTL {
		return nil, errors.New("authority composition: adapter safety margin must be non-negative and below the lease TTL")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &RealMembershipAdapter{
		store:        opts.Store,
		registry:     opts.Registry,
		layout:       opts.AuthorityLayoutVersion,
		hashVersion:  opts.HashVersion,
		hashSeed:     opts.HashSeed,
		holderID:     opts.HolderID,
		holderBootID: opts.HolderBootID,
		leaseTTL:     opts.LeaseTTL,
		safetyMargin: opts.SafetyMargin,
		now:          opts.Now,
	}, nil
}

// PrepareMembership implements the first phase of MembershipInstaller.  It
// proves the full desired generation through the repository (ACTIVE +
// domain/shard/holder/boot/assignment/writer/lease match) and returns a
// PreparedMembership.  It never modifies the LeaseGuardRegistry and never
// admits: a failure leaves the shard fail-closed with nothing installed.
func (a *RealMembershipAdapter) PrepareMembership(ctx context.Context, member ShardMembership) (*PreparedMembership, error) {
	if err := validateMembershipMember(member); err != nil {
		return nil, err
	}
	if hook := a.prepareMembershipHook; hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	desired := member.Generation
	requestStarted := a.now()
	result, err := a.store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerHolder,
			InstanceID: a.holderID,
			BootID:     a.holderBootID,
		},
		AuthorityLayoutVersion: a.layout,
		Domain:                 member.Key.Domain,
		VirtualShard:           member.Key.Shard,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID:           desired.HolderID,
			ExpectedHolderBootID:       desired.HolderBootID,
			ExpectedAssignmentRevision: desired.AssignmentRevision,
			ExpectedWriterEpoch:        desired.WriterEpoch,
			ExpectedLeaseEpoch:         desired.LeaseEpoch,
		},
		RequestedTTLms: a.leaseTTL.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("authority composition: membership prepare renew: %w", err)
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		return nil, fmt.Errorf("authority composition: membership prepare rejected with status %s", result.Status)
	}
	if err := validateInstalledAuthority(a.layout, member, desired, result.Authority); err != nil {
		return nil, err
	}
	if result.AcceptedTTLms <= 0 {
		return nil, errors.New("authority composition: membership prepare accepted zero lease TTL")
	}
	// Renew is the durable linearization point and may extend LeaseUntilMs
	// without changing the assignment/writer/lease epochs. Prepared state must
	// carry that exact returned deadline; retaining the request deadline would
	// make the Holder report a generation different from durable Authority.
	preparedGeneration := desired
	preparedGeneration.LeaseUntilMs = result.Authority.LeaseUntilMs
	return &PreparedMembership{
		Key:            member.Key,
		Generation:     preparedGeneration,
		RequestStarted: requestStarted,
		AcceptedTTLms:  result.AcceptedTTLms,
		Authority:      result.Authority,
	}, nil
}

// CommitPreparedMembership implements the second phase of MembershipInstaller.
// It is purely local and bounded (no network): it installs the prepared
// generation through LeaseGuardRegistry.InstallIfMatches, which admits only
// when no guard, an identical generation (refreshing the local deadline), or a
// strictly newer generation is installed.  It must only be called while the
// reconciler's action token still matches the current plan.
func (a *RealMembershipAdapter) CommitPreparedMembership(ctx context.Context, prepared PreparedMembership) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared.AcceptedTTLms <= 0 {
		return errors.New("authority composition: membership commit requires a positive accepted TTL")
	}
	if hook := a.commitPreparedHook; hook != nil {
		hook()
	}
	guard, err := authoritystore.NewHolderLeaseGuard(a.holderID, a.holderBootID, a.safetyMargin, a.now)
	if err != nil {
		return err
	}
	result := authoritystore.ShardAuthorityTransitionResult{
		Status:        authoritystore.ShardTransitionApplied,
		Authority:     prepared.Authority,
		AcceptedTTLms: prepared.AcceptedTTLms,
	}
	// This is the adapter's local admission linearization precheck. A caller
	// deadline that fired before InstallIfMatches must never install a guard;
	// cancellation after InstallIfMatches returns is compensated by the
	// reconciler's post-commit barrier.
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = a.registry.InstallIfMatches(
		prepared.Key.Domain, prepared.Key.Shard,
		expectedAuthorityFor(a.layout, a.hashVersion, a.hashSeed, prepared.Key, prepared.Generation),
		guard, prepared.RequestStarted, result, a.now,
	)
	return err
}

// FenceMembership implements MembershipFencer: a local compare-and-act that
// fences only the exact expected generation and flips CanServeShard false
// immediately, without any network round trip.
func (a *RealMembershipAdapter) FenceMembership(ctx context.Context, member ShardMembership) error {
	if err := validateMembershipMember(member); err != nil {
		return err
	}
	return a.registry.FenceIfMatches(member.Key.Domain, member.Key.Shard, expectedAuthorityFor(a.layout, a.hashVersion, a.hashSeed, member.Key, member.Generation))
}

// RemoveMembership implements MembershipRemover: a local compare-and-act that
// removes only the exact expected generation so an old REMOVE can never delete
// a newer guard.
func (a *RealMembershipAdapter) RemoveMembership(ctx context.Context, member ShardMembership) error {
	if err := validateMembershipMember(member); err != nil {
		return err
	}
	return a.registry.RemoveIfMatches(member.Key.Domain, member.Key.Shard, expectedAuthorityFor(a.layout, a.hashVersion, a.hashSeed, member.Key, member.Generation))
}

// expectedAuthorityFor builds the expected authority identity (compare-and-act
// handle) for a membership generation.  It carries the layout and hash profile
// so the guard layer's generation ordering can distinguish upgrades from
// regressions.
func expectedAuthorityFor(layout string, hashVersion uint32, hashSeed uint64, key ShardKey, gen MembershipGeneration) authoritystore.ShardWriteAuthority {
	return authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: layout,
		HashVersion:            hashVersion,
		HashSeed:               hashSeed,
		Domain:                 key.Domain,
		VirtualShard:           key.Shard,
		HolderID:               gen.HolderID,
		HolderBootID:           gen.HolderBootID,
		AssignmentRevision:     gen.AssignmentRevision,
		WriterEpoch:            gen.WriterEpoch,
		LeaseEpoch:             gen.LeaseEpoch,
		State:                  authoritystore.ShardAuthorityActive,
	}
}

// validateInstalledAuthority enforces the frozen invariant that Install may
// only admit an authority which is ACTIVE and matches every desired identity
// field.
func validateInstalledAuthority(layout string, member ShardMembership, desired MembershipGeneration, authority authoritystore.ShardWriteAuthority) error {
	if authority.State != authoritystore.ShardAuthorityActive {
		return fmt.Errorf("authority composition: membership install authority not ACTIVE (state %d)", authority.State)
	}
	if authority.AuthorityLayoutVersion != layout ||
		authority.Domain != member.Key.Domain ||
		authority.VirtualShard != member.Key.Shard ||
		authority.HolderID != desired.HolderID ||
		authority.HolderBootID != desired.HolderBootID ||
		authority.AssignmentRevision != desired.AssignmentRevision ||
		authority.WriterEpoch != desired.WriterEpoch ||
		authority.LeaseEpoch != desired.LeaseEpoch {
		return fmt.Errorf("authority composition: membership install authority does not match desired generation (domain=%v shard=%d)", member.Key.Domain, member.Key.Shard)
	}
	if authority.LeaseUntilMs <= 0 {
		return errors.New("authority composition: membership install authority carries no lease deadline")
	}
	return nil
}

func validateMembershipMember(member ShardMembership) error {
	if member.Key.Domain != actorrouting.DomainPlayer && member.Key.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("authority composition: invalid membership domain %v", member.Key.Domain)
	}
	if member.Generation.isZero() {
		return errors.New("authority composition: membership generation is empty")
	}
	return nil
}
