package authority

import (
	"context"
	"fmt"
	"strconv"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

var _ authoritystore.ShardWriteAuthorityStore = (*Store)(nil)
var _ authoritystore.ExpiredTakeoverShardAuthorityStore = (*Store)(nil)

func isRouteManagerCaller(caller authoritystore.TrustedShardAuthorityCaller) bool {
	return caller.Kind == authoritystore.ShardAuthorityCallerRouteManager && caller.InstanceID != ""
}

func isHolderCaller(caller authoritystore.TrustedShardAuthorityCaller, fence authoritystore.ShardWriteFence) bool {
	return caller.Kind == authoritystore.ShardAuthorityCallerHolder &&
		caller.InstanceID == fence.ExpectedHolderID && caller.BootID == fence.ExpectedHolderBootID
}

func (s *Store) bucketForShard(domain actorrouting.RouteDomain, shard uint32) (rediskey.AuthorityBucketRef, error) {
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: route domain", authoritystore.ErrInvalidRequest)
	}
	bucket, err := s.opts.Profile.AuthorityBucketForShard(s.opts.Resolver, domain, shard)
	if err != nil {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: derive bucket: %v", authoritystore.ErrAuthorityAddressMismatch, err)
	}
	return bucket, nil
}

func (s *Store) validateTransitionBase(layout string, transitionID string, requestedTTLms int64) error {
	if err := s.checkLayout(layout); err != nil {
		return err
	}
	if err := validateStableID("transition_id", transitionID); err != nil {
		return err
	}
	if requestedTTLms <= 0 {
		return fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest)
	}
	return nil
}

func shardTransitionNotSent(err error) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnavailableNotSent}, err
}

func (s *Store) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := s.validateTransitionBase(req.AuthorityLayoutVersion, req.TransitionID, req.RequestedTTLms); err != nil {
		return shardTransitionNotSent(err)
	}
	if !isRouteManagerCaller(req.Caller) {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, nil
	}
	if err := validateStableID("holder_id", req.NewHolderID); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := validateStableID("holder_boot_id", req.NewHolderBootID); err != nil {
		return shardTransitionNotSent(err)
	}
	bucket, err := s.bucketForShard(req.Domain, req.VirtualShard)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	key, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	profile := s.opts.Resolver.Profile()
	digest := transitionIntentDigest("claim", s.layout, string(req.Domain), req.VirtualShard,
		authoritystore.ShardWriteFence{}, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms)
	args := []interface{}{
		s.layout,
		strconv.FormatUint(uint64(profile.Version), 10),
		string(req.Domain),
		strconv.FormatUint(uint64(req.VirtualShard), 10),
		req.NewHolderID, req.NewHolderBootID,
		strconv.FormatInt(req.RequestedTTLms, 10),
		strconv.FormatInt(s.opts.LeaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(s.opts.LeaseLimits.MaxTTL.Milliseconds(), 10),
		req.TransitionID, digest,
	}
	reply, err := s.registry.RunNoRetry(ctx, ScriptClaimShardAuthority, []string{key.String()}, args...)
	if err != nil {
		return s.shardTransportError(err)
	}
	return s.decodeShardReply(req.Domain, req.VirtualShard, reply)
}

func (s *Store) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := s.checkLayout(req.AuthorityLayoutVersion); err != nil {
		return shardTransitionNotSent(err)
	}
	if !isHolderCaller(req.Caller, req.ExpectedFence) {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, nil
	}
	if req.RequestedTTLms <= 0 {
		return shardTransitionNotSent(fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest))
	}
	bucket, err := s.bucketForShard(req.Domain, req.VirtualShard)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	key, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	args := []interface{}{
		s.layout,
		req.ExpectedFence.ExpectedHolderID, req.ExpectedFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ExpectedFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedLeaseEpoch, 10),
		strconv.FormatInt(req.RequestedTTLms, 10),
		strconv.FormatInt(s.opts.LeaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(s.opts.LeaseLimits.MaxTTL.Milliseconds(), 10),
	}
	reply, err := s.registry.RunNoRetry(ctx, ScriptRenewShardAuthority, []string{key.String()}, args...)
	if err != nil {
		return s.shardTransportError(err)
	}
	return s.decodeShardReply(req.Domain, req.VirtualShard, reply)
}

func (s *Store) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := s.validateTransitionBase(req.AuthorityLayoutVersion, req.TransitionID, req.RequestedTTLms); err != nil {
		return shardTransitionNotSent(err)
	}
	if !isRouteManagerCaller(req.Caller) {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, nil
	}
	if err := validateStableID("holder_id", req.NewHolderID); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := validateStableID("holder_boot_id", req.NewHolderBootID); err != nil {
		return shardTransitionNotSent(err)
	}
	if req.NewHolderID == req.ExpectedFence.ExpectedHolderID && req.NewHolderBootID == req.ExpectedFence.ExpectedHolderBootID {
		return shardTransitionNotSent(fmt.Errorf("%w: transfer does not change holder generation", authoritystore.ErrInvalidRequest))
	}
	bucket, err := s.bucketForShard(req.Domain, req.VirtualShard)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	key, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	digest := transitionIntentDigest("transfer", s.layout, string(req.Domain), req.VirtualShard,
		req.ExpectedFence, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms)
	args := []interface{}{
		s.layout,
		req.ExpectedFence.ExpectedHolderID, req.ExpectedFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ExpectedFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedLeaseEpoch, 10),
		req.NewHolderID, req.NewHolderBootID,
		strconv.FormatInt(req.RequestedTTLms, 10),
		strconv.FormatInt(s.opts.LeaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(s.opts.LeaseLimits.MaxTTL.Milliseconds(), 10),
		req.TransitionID, digest,
	}
	reply, err := s.registry.RunNoRetry(ctx, ScriptTransferShardAuthority, []string{key.String()}, args...)
	if err != nil {
		return s.shardTransportError(err)
	}
	return s.decodeShardReply(req.Domain, req.VirtualShard, reply)
}

func (s *Store) TakeoverExpiredShardAuthority(ctx context.Context, req authoritystore.ExpiredTakeoverShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := s.validateTransitionBase(req.AuthorityLayoutVersion, req.TransitionID, req.RequestedTTLms); err != nil {
		return shardTransitionNotSent(err)
	}
	if !isRouteManagerCaller(req.Caller) {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, nil
	}
	if err := validateStableID("holder_id", req.NewHolderID); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := validateStableID("holder_boot_id", req.NewHolderBootID); err != nil {
		return shardTransitionNotSent(err)
	}
	if req.NewHolderID == req.ExpectedFence.ExpectedHolderID && req.NewHolderBootID == req.ExpectedFence.ExpectedHolderBootID {
		return shardTransitionNotSent(fmt.Errorf("%w: takeover does not change holder generation", authoritystore.ErrInvalidRequest))
	}
	bucket, err := s.bucketForShard(req.Domain, req.VirtualShard)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	key, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	digest := transitionIntentDigest("transfer", s.layout, string(req.Domain), req.VirtualShard,
		req.ExpectedFence, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms)
	args := []interface{}{
		s.layout,
		req.ExpectedFence.ExpectedHolderID, req.ExpectedFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ExpectedFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedLeaseEpoch, 10),
		req.NewHolderID, req.NewHolderBootID,
		strconv.FormatInt(req.RequestedTTLms, 10),
		strconv.FormatInt(s.opts.LeaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(s.opts.LeaseLimits.MaxTTL.Milliseconds(), 10),
		req.TransitionID, digest,
	}
	reply, err := s.registry.RunNoRetry(ctx, ScriptTakeoverExpiredAuthority, []string{key.String()}, args...)
	if err != nil {
		return s.shardTransportError(err)
	}
	return s.decodeShardReply(req.Domain, req.VirtualShard, reply)
}

func (s *Store) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return shardTransitionNotSent(err)
	}
	if err := s.checkLayout(req.AuthorityLayoutVersion); err != nil {
		return shardTransitionNotSent(err)
	}
	if !isRouteManagerCaller(req.Caller) {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, nil
	}
	if err := validateStableID("transition_id", req.TransitionID); err != nil {
		return shardTransitionNotSent(err)
	}
	bucket, err := s.bucketForShard(req.Domain, req.VirtualShard)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	key, err := s.keys.ShardAuthority(bucket)
	if err != nil {
		return shardTransitionNotSent(err)
	}
	digest := transitionIntentDigest("unassign", s.layout, string(req.Domain), req.VirtualShard,
		req.ExpectedFence, "", "", 0)
	args := []interface{}{
		s.layout,
		req.ExpectedFence.ExpectedHolderID, req.ExpectedFence.ExpectedHolderBootID,
		strconv.FormatUint(req.ExpectedFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(req.ExpectedFence.ExpectedLeaseEpoch, 10),
		req.TransitionID, digest,
	}
	reply, err := s.registry.RunNoRetry(ctx, ScriptUnassignShardAuthority, []string{key.String()}, args...)
	if err != nil {
		return s.shardTransportError(err)
	}
	return s.decodeShardReply(req.Domain, req.VirtualShard, reply)
}

func (s *Store) shardTransportError(err error) (authoritystore.ShardAuthorityTransitionResult, error) {
	if isContractViolation(err) {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: %v", errScriptContract, err)
	}
	if classifyWriteError(err) == fateNotSent {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnavailableNotSent}, nil
	}
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionTimeoutUnknown}, nil
}

// decodeShardReply decodes the shared shard script snapshot reply:
// {status, holder, boot, writer_epoch, lease_epoch, assignment_revision,
// lease_state, lease_expire_at_ms, last_transition_id, last_transition_at_ms,
// remaining_ttl_ms}.
func (s *Store) decodeShardReply(domain actorrouting.RouteDomain, shard uint32, reply []string) (authoritystore.ShardAuthorityTransitionResult, error) {
	if len(reply) == 0 {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	if reply[0] == "UNAVAILABLE_NOT_SENT" {
		if len(reply) != 2 || reply[1] != "LAYOUT_MISMATCH" {
			// A short/unknown reply cannot prove where execution stopped. Keep
			// the zero/UNKNOWN result so Manager retains its durable boundary.
			return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: malformed not-sent shard reply", errScriptContract)
		}
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnavailableNotSent}, authoritystore.ErrAuthorityAddressMismatch
	}
	status, ok := map[string]authoritystore.ShardAuthorityTransitionStatus{
		"APPLIED":               authoritystore.ShardTransitionApplied,
		"ALREADY_APPLIED":       authoritystore.ShardTransitionAlreadyApplied,
		"ACTIVE_LEASE_CONFLICT": authoritystore.ShardTransitionActiveLeaseConflict,
		"FENCE_MISMATCH":        authoritystore.ShardTransitionFenceMismatch,
		"LEASE_EXPIRED":         authoritystore.ShardTransitionLeaseExpired,
		"NOT_FOUND":             authoritystore.ShardTransitionNotFound,
		"RECONCILE_REQUIRED":    authoritystore.ShardTransitionReconcileRequired,
	}[reply[0]]
	if !ok {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: shard status %q", errScriptContract, reply[0])
	}
	result := authoritystore.ShardAuthorityTransitionResult{Status: status}
	if reply[0] == "NOT_FOUND" {
		return result, nil
	}
	if len(reply) < 11 {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: short shard reply", errScriptContract)
	}
	profile := s.opts.Resolver.Profile()
	writerEpoch, _ := strconv.ParseUint(reply[3], 10, 64)
	leaseEpoch, _ := strconv.ParseUint(reply[4], 10, 64)
	revision, _ := strconv.ParseUint(reply[5], 10, 64)
	expireAt, _ := strconv.ParseInt(reply[7], 10, 64)
	transitionAt, _ := strconv.ParseInt(reply[9], 10, 64)
	remaining, _ := strconv.ParseInt(reply[10], 10, 64)
	state := authoritystore.ShardAuthorityUnassigned
	if reply[6] == "ACTIVE" {
		state = authoritystore.ShardAuthorityActive
	}
	result.Authority = authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: s.layout,
		HashVersion:            profile.Version,
		HashSeed:               profile.Seed,
		Domain:                 domain,
		VirtualShard:           shard,
		HolderID:               reply[1],
		HolderBootID:           reply[2],
		AssignmentRevision:     revision,
		WriterEpoch:            writerEpoch,
		LeaseEpoch:             leaseEpoch,
		State:                  state,
		LeaseUntilMs:           expireAt,
		LastTransitionID:       reply[8],
		LastTransitionAtMs:     transitionAt,
	}
	result.AcceptedTTLms = remaining
	return result, nil
}
