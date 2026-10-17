package authoritystore

import (
	"context"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
)

// LeaseLimits are adapter-independent safety bounds. A production Redis
// adapter may choose tighter values from configuration, but never accepts a
// non-positive lease.
type LeaseLimits struct {
	MinTTL time.Duration
	MaxTTL time.Duration
}

func DefaultLeaseLimits() LeaseLimits {
	return LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: time.Minute}
}

func (l LeaseLimits) validate() error {
	if l.MinTTL < time.Millisecond || l.MaxTTL < l.MinTTL {
		return fmt.Errorf("%w: lease limits", ErrInvalidRequest)
	}
	return nil
}

func (s *MemoryStore) ClaimShardAuthority(ctx context.Context, req ClaimShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	if err := contextError(ctx); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := s.validateShardTarget(req.AuthorityLayoutVersion, req.Domain); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if !isRouteManager(req.Caller) {
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnauthorized}, nil
	}
	if err := validateStableInternalID("transition_id", req.TransitionID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := validateHolder(req.NewHolderID, req.NewHolderBootID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	acceptedTTL, err := s.acceptTTL(req.RequestedTTLms)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	intent := shardTransitionIntent{
		kind: shardTransitionClaim, transitionID: req.TransitionID,
		layoutVersion: req.AuthorityLayoutVersion,
		domain:        string(req.Domain), shard: req.VirtualShard,
		newHolderID: req.NewHolderID, newHolderBootID: req.NewHolderBootID,
		requestedTTLms: req.RequestedTTLms,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := trustedNow(s.now).UnixMilli()
	key := shardKey{domain: string(req.Domain), shard: req.VirtualShard}
	record := s.shards[key]
	if record != nil {
		if err := s.validateStoredHashProfile(record.authority); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
	}
	if result, done := replayShardTransition(record, intent, nowMs); done {
		return result, nil
	}
	if record != nil && record.authority.State == ShardAuthorityActive && record.authority.LeaseUntilMs > nowMs {
		return transitionResult(ShardTransitionActiveLeaseConflict, record.authority, nowMs), nil
	}
	if record == nil {
		record = &shardAuthorityRecord{}
		s.shards[key] = record
	}
	authority := record.authority
	authority.AuthorityLayoutVersion = s.layoutVersion
	authority.HashVersion = s.hashProfile.Version
	authority.HashSeed = s.hashProfile.Seed
	authority.Domain = req.Domain
	authority.VirtualShard = req.VirtualShard
	authority.HolderID = req.NewHolderID
	authority.HolderBootID = req.NewHolderBootID
	authority.WriterEpoch++
	authority.LeaseEpoch++
	authority.AssignmentRevision++
	authority.State = ShardAuthorityActive
	authority.LeaseUntilMs = nowMs + acceptedTTL
	authority.LastTransitionID = req.TransitionID
	authority.LastTransitionAtMs = nowMs
	record.authority = authority
	record.lastTransition = intent
	return transitionResult(ShardTransitionApplied, authority, nowMs), nil
}

func (s *MemoryStore) RenewShardAuthority(ctx context.Context, req RenewShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	if err := contextError(ctx); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := s.validateShardTarget(req.AuthorityLayoutVersion, req.Domain); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if !isCurrentHolderCaller(req.Caller, req.ExpectedFence) {
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnauthorized}, nil
	}
	acceptedTTL, err := s.acceptTTL(req.RequestedTTLms)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := trustedNow(s.now).UnixMilli()
	record := s.shards[shardKey{domain: string(req.Domain), shard: req.VirtualShard}]
	if record == nil {
		return ShardAuthorityTransitionResult{Status: ShardTransitionNotFound}, nil
	}
	if err := s.validateStoredHashProfile(record.authority); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if record.authority.State != ShardAuthorityActive || record.authority.LeaseUntilMs <= nowMs {
		return transitionResult(ShardTransitionLeaseExpired, record.authority, nowMs), nil
	}
	if !fenceMatches(record.authority, req.ExpectedFence) {
		return transitionResult(ShardTransitionFenceMismatch, record.authority, nowMs), nil
	}
	requestedExpiry := nowMs + acceptedTTL
	if requestedExpiry > record.authority.LeaseUntilMs {
		record.authority.LeaseUntilMs = requestedExpiry
	}
	return transitionResult(ShardTransitionApplied, record.authority, nowMs), nil
}

func (s *MemoryStore) TransferShardAuthority(ctx context.Context, req TransferShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	if err := contextError(ctx); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := s.validateShardTarget(req.AuthorityLayoutVersion, req.Domain); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if !isRouteManager(req.Caller) {
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnauthorized}, nil
	}
	if err := validateStableInternalID("transition_id", req.TransitionID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := validateHolder(req.NewHolderID, req.NewHolderBootID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if req.NewHolderID == req.ExpectedFence.ExpectedHolderID && req.NewHolderBootID == req.ExpectedFence.ExpectedHolderBootID {
		return ShardAuthorityTransitionResult{}, fmt.Errorf("%w: transfer does not change holder generation", ErrInvalidRequest)
	}
	acceptedTTL, err := s.acceptTTL(req.RequestedTTLms)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	intent := shardTransitionIntent{
		kind: shardTransitionTransfer, transitionID: req.TransitionID,
		layoutVersion: req.AuthorityLayoutVersion,
		domain:        string(req.Domain), shard: req.VirtualShard, expectedFence: req.ExpectedFence,
		newHolderID: req.NewHolderID, newHolderBootID: req.NewHolderBootID,
		requestedTTLms: req.RequestedTTLms,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := trustedNow(s.now).UnixMilli()
	record := s.shards[shardKey{domain: string(req.Domain), shard: req.VirtualShard}]
	if record != nil {
		if err := s.validateStoredHashProfile(record.authority); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
	}
	if result, done := replayShardTransition(record, intent, nowMs); done {
		return result, nil
	}
	if record == nil {
		return ShardAuthorityTransitionResult{Status: ShardTransitionNotFound}, nil
	}
	if record.authority.State != ShardAuthorityActive || record.authority.LeaseUntilMs <= nowMs {
		return transitionResult(ShardTransitionLeaseExpired, record.authority, nowMs), nil
	}
	if !fenceMatches(record.authority, req.ExpectedFence) {
		return transitionResult(ShardTransitionFenceMismatch, record.authority, nowMs), nil
	}
	record.authority.HolderID = req.NewHolderID
	record.authority.HolderBootID = req.NewHolderBootID
	record.authority.WriterEpoch++
	record.authority.LeaseEpoch++
	record.authority.AssignmentRevision++
	record.authority.LeaseUntilMs = nowMs + acceptedTTL
	record.authority.LastTransitionID = req.TransitionID
	record.authority.LastTransitionAtMs = nowMs
	record.lastTransition = intent
	return transitionResult(ShardTransitionApplied, record.authority, nowMs), nil
}

// TakeoverExpiredShardAuthority atomically replaces one exact expired writer
// generation. It is deliberately separate from TransferShardAuthority: a
// planned handoff still requires a live lease, while crash recovery requires
// the old lease to have ended.
func (s *MemoryStore) TakeoverExpiredShardAuthority(ctx context.Context, req ExpiredTakeoverShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	if err := contextError(ctx); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := s.validateShardTarget(req.AuthorityLayoutVersion, req.Domain); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if !isRouteManager(req.Caller) {
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnauthorized}, nil
	}
	if err := validateStableInternalID("transition_id", req.TransitionID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := validateHolder(req.NewHolderID, req.NewHolderBootID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if req.NewHolderID == req.ExpectedFence.ExpectedHolderID && req.NewHolderBootID == req.ExpectedFence.ExpectedHolderBootID {
		return ShardAuthorityTransitionResult{}, fmt.Errorf("%w: takeover does not change holder generation", ErrInvalidRequest)
	}
	acceptedTTL, err := s.acceptTTL(req.RequestedTTLms)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	intent := shardTransitionIntent{
		kind: shardTransitionTransfer, transitionID: req.TransitionID,
		layoutVersion: req.AuthorityLayoutVersion,
		domain:        string(req.Domain), shard: req.VirtualShard, expectedFence: req.ExpectedFence,
		newHolderID: req.NewHolderID, newHolderBootID: req.NewHolderBootID,
		requestedTTLms: req.RequestedTTLms,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := trustedNow(s.now).UnixMilli()
	record := s.shards[shardKey{domain: string(req.Domain), shard: req.VirtualShard}]
	if record != nil {
		if err := s.validateStoredHashProfile(record.authority); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
	}
	if result, done := replayShardTransition(record, intent, nowMs); done {
		return result, nil
	}
	if record == nil {
		return ShardAuthorityTransitionResult{Status: ShardTransitionNotFound}, nil
	}
	if record.authority.State != ShardAuthorityActive {
		return transitionResult(ShardTransitionLeaseExpired, record.authority, nowMs), nil
	}
	if record.authority.LeaseUntilMs > nowMs {
		return transitionResult(ShardTransitionActiveLeaseConflict, record.authority, nowMs), nil
	}
	if !fenceMatches(record.authority, req.ExpectedFence) {
		return transitionResult(ShardTransitionFenceMismatch, record.authority, nowMs), nil
	}
	record.authority.HolderID = req.NewHolderID
	record.authority.HolderBootID = req.NewHolderBootID
	record.authority.WriterEpoch++
	record.authority.LeaseEpoch++
	record.authority.AssignmentRevision++
	record.authority.LeaseUntilMs = nowMs + acceptedTTL
	record.authority.LastTransitionID = req.TransitionID
	record.authority.LastTransitionAtMs = nowMs
	record.lastTransition = intent
	return transitionResult(ShardTransitionApplied, record.authority, nowMs), nil
}

func (s *MemoryStore) UnassignShardAuthority(ctx context.Context, req UnassignShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	if err := contextError(ctx); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if err := s.validateShardTarget(req.AuthorityLayoutVersion, req.Domain); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	if !isRouteManager(req.Caller) {
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnauthorized}, nil
	}
	if err := validateStableInternalID("transition_id", req.TransitionID); err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	intent := shardTransitionIntent{
		kind: shardTransitionUnassign, transitionID: req.TransitionID,
		layoutVersion: req.AuthorityLayoutVersion,
		domain:        string(req.Domain), shard: req.VirtualShard, expectedFence: req.ExpectedFence,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := trustedNow(s.now).UnixMilli()
	record := s.shards[shardKey{domain: string(req.Domain), shard: req.VirtualShard}]
	if record != nil {
		if err := s.validateStoredHashProfile(record.authority); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
	}
	if result, done := replayShardTransition(record, intent, nowMs); done {
		return result, nil
	}
	if record == nil {
		return ShardAuthorityTransitionResult{Status: ShardTransitionNotFound}, nil
	}
	if record.authority.State != ShardAuthorityActive {
		return transitionResult(ShardTransitionFenceMismatch, record.authority, nowMs), nil
	}
	if !fenceMatches(record.authority, req.ExpectedFence) {
		return transitionResult(ShardTransitionFenceMismatch, record.authority, nowMs), nil
	}
	record.authority.HolderID = ""
	record.authority.HolderBootID = ""
	record.authority.WriterEpoch++
	record.authority.LeaseEpoch++
	record.authority.AssignmentRevision++
	record.authority.State = ShardAuthorityUnassigned
	record.authority.LeaseUntilMs = 0
	record.authority.LastTransitionID = req.TransitionID
	record.authority.LastTransitionAtMs = nowMs
	record.lastTransition = intent
	return transitionResult(ShardTransitionApplied, record.authority, nowMs), nil
}

func (s *MemoryStore) InspectShardWriteAuthority(domain actorrouting.RouteDomain, shard uint32) (ShardWriteAuthority, bool) {
	if s == nil {
		return ShardWriteAuthority{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.shards[shardKey{domain: string(domain), shard: shard}]
	if record == nil {
		return ShardWriteAuthority{}, false
	}
	return record.authority, true
}

func (s *MemoryStore) validateStoredHashProfile(authority ShardWriteAuthority) error {
	if authority.AuthorityLayoutVersion != s.layoutVersion ||
		authority.HashVersion != s.hashProfile.Version ||
		authority.HashSeed != s.hashProfile.Seed {
		return fmt.Errorf("%w: stored shard hash profile", ErrAuthorityAddressMismatch)
	}
	return nil
}

func (s *MemoryStore) validateShardTarget(layout string, domain actorrouting.RouteDomain) error {
	if s == nil {
		return fmt.Errorf("%w: nil store", ErrInvalidRequest)
	}
	if layout != s.layoutVersion {
		return fmt.Errorf("%w: layout version", ErrAuthorityAddressMismatch)
	}
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return fmt.Errorf("%w: route domain", ErrInvalidRequest)
	}
	return nil
}

func (s *MemoryStore) acceptTTL(requestedMs int64) (int64, error) {
	if requestedMs <= 0 {
		return 0, fmt.Errorf("%w: requested_ttl_ms", ErrInvalidRequest)
	}
	minMs := s.leaseLimits.MinTTL.Milliseconds()
	maxMs := s.leaseLimits.MaxTTL.Milliseconds()
	if requestedMs < minMs {
		requestedMs = minMs
	}
	if requestedMs > maxMs {
		requestedMs = maxMs
	}
	return requestedMs, nil
}

func isRouteManager(caller TrustedShardAuthorityCaller) bool {
	return caller.Kind == ShardAuthorityCallerRouteManager && caller.InstanceID != ""
}

func isCurrentHolderCaller(caller TrustedShardAuthorityCaller, fence ShardWriteFence) bool {
	return caller.Kind == ShardAuthorityCallerHolder && caller.InstanceID == fence.ExpectedHolderID && caller.BootID == fence.ExpectedHolderBootID
}

func validateHolder(holderID, bootID string) error {
	if err := validateStableInternalID("holder_id", holderID); err != nil {
		return err
	}
	return validateStableInternalID("holder_boot_id", bootID)
}

func fenceMatches(authority ShardWriteAuthority, fence ShardWriteFence) bool {
	return authority.HolderID == fence.ExpectedHolderID &&
		authority.HolderBootID == fence.ExpectedHolderBootID &&
		authority.AssignmentRevision == fence.ExpectedAssignmentRevision &&
		authority.WriterEpoch == fence.ExpectedWriterEpoch &&
		authority.LeaseEpoch == fence.ExpectedLeaseEpoch
}

func replayShardTransition(record *shardAuthorityRecord, intent shardTransitionIntent, nowMs int64) (ShardAuthorityTransitionResult, bool) {
	if record == nil || record.authority.LastTransitionID != intent.transitionID {
		return ShardAuthorityTransitionResult{}, false
	}
	if record.lastTransition != intent {
		return transitionResult(ShardTransitionReconcileRequired, record.authority, nowMs), true
	}
	return transitionResult(ShardTransitionAlreadyApplied, record.authority, nowMs), true
}

func transitionResult(status ShardAuthorityTransitionStatus, authority ShardWriteAuthority, nowMs int64) ShardAuthorityTransitionResult {
	remaining := authority.LeaseUntilMs - nowMs
	if authority.State != ShardAuthorityActive || remaining < 0 {
		remaining = 0
	}
	return ShardAuthorityTransitionResult{Status: status, Authority: authority, AcceptedTTLms: remaining}
}

func fenceOf(authority ShardWriteAuthority) ShardWriteFence {
	return ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch, ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
}
