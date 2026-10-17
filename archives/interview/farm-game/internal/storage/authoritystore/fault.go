package authoritystore

import (
	"context"
	"fmt"
	"sync"
)

type Method uint8

const (
	MethodUnknown Method = iota
	MethodLoadAndClaim
	MethodCommit
	MethodFenceAndClaim
	MethodLookupIdempotency
	MethodClaimShardAuthority
	MethodRenewShardAuthority
	MethodTransferShardAuthority
	MethodTakeoverExpiredShardAuthority
	MethodUnassignShardAuthority
)

type FaultMode uint8

const (
	FaultNone FaultMode = iota
	FaultUnavailableNotSent
	FaultTimeoutUnknownBeforeApply
	FaultTimeoutUnknownAfterApply
)

// FaultStore is a deterministic transport-boundary fake. "BeforeApply"
// models a request whose outcome is unknown to the caller but did not mutate
// this store; "AfterApply" hides a successful delegate response. Tests must
// exercise both because TIMEOUT_UNKNOWN alone cannot reveal which occurred.
type FaultStore struct {
	inner      ActorAuthorityStore
	mu         sync.Mutex
	queues     map[Method][]FaultMode
	calls      map[Method]uint64
	lastCommit *AuthorityCommitRequest
}

var _ ActorAuthorityStore = (*FaultStore)(nil)
var _ ShardWriteAuthorityStore = (*FaultStore)(nil)
var _ ExpiredTakeoverShardAuthorityStore = (*FaultStore)(nil)
var _ PhysicalCapabilitiesProvider = (*FaultStore)(nil)
var _ PhysicalCommitUsageProjector = (*FaultStore)(nil)
var _ ExactCommitter = (*FaultStore)(nil)

func NewFaultStore(inner ActorAuthorityStore) *FaultStore {
	return &FaultStore{inner: inner, queues: make(map[Method][]FaultMode), calls: make(map[Method]uint64)}
}

func (s *FaultStore) PhysicalCapabilities() (PhysicalCapabilities, error) {
	if s == nil || s.inner == nil {
		return PhysicalCapabilities{}, fmt.Errorf("%w: fault store has no delegate", ErrInvalidRequest)
	}
	provider, ok := s.inner.(PhysicalCapabilitiesProvider)
	if !ok {
		return PhysicalCapabilities{}, fmt.Errorf("%w: delegate has no physical capabilities", ErrInvalidRequest)
	}
	return provider.PhysicalCapabilities()
}

func (s *FaultStore) ProjectPhysicalCommit(req AuthorityCommitRequest) (PhysicalCommitUsage, error) {
	if s == nil || s.inner == nil {
		return PhysicalCommitUsage{}, fmt.Errorf("%w: fault store has no delegate", ErrInvalidRequest)
	}
	projector, ok := s.inner.(PhysicalCommitUsageProjector)
	if !ok {
		return PhysicalCommitUsage{}, fmt.Errorf("%w: delegate has no physical commit projector", ErrInvalidRequest)
	}
	return projector.ProjectPhysicalCommit(req)
}

func (s *FaultStore) Inject(method Method, modes ...FaultMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[method] = append(s.queues[method], modes...)
}

func (s *FaultStore) Calls(method Method) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[method]
}

// LastCommitRequest returns a defensive copy of the most recent attempted
// commit. It is a deterministic test observation seam; it never changes the
// delegate result or participates in production authority state.
func (s *FaultStore) LastCommitRequest() (AuthorityCommitRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastCommit == nil {
		return AuthorityCommitRequest{}, false
	}
	return cloneCommitRequest(*s.lastCommit), true
}

func (s *FaultStore) LoadAndClaim(ctx context.Context, req LoadAndClaimRequest) (LoadAndClaimResult, error) {
	mode, err := s.next(MethodLoadAndClaim)
	if err != nil {
		return LoadAndClaimResult{}, err
	}
	switch mode {
	case FaultUnavailableNotSent:
		return LoadAndClaimResult{Status: LoadAndClaimUnavailableNotSent}, nil
	case FaultTimeoutUnknownBeforeApply:
		return LoadAndClaimResult{Status: LoadAndClaimTimeoutUnknown}, nil
	case FaultTimeoutUnknownAfterApply:
		if _, err := s.inner.LoadAndClaim(ctx, req); err != nil {
			return LoadAndClaimResult{}, err
		}
		return LoadAndClaimResult{Status: LoadAndClaimTimeoutUnknown}, nil
	default:
		return s.inner.LoadAndClaim(ctx, req)
	}
}

func (s *FaultStore) Commit(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false)
}

// CommitExact preserves the explicit exact-output capability through the
// deterministic transport-fault wrapper. It never upgrades an ordinary Commit.
func (s *FaultStore) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, true)
}

func (s *FaultStore) commit(ctx context.Context, req AuthorityCommitRequest, exact bool) (CommitResult, error) {
	s.mu.Lock()
	cloned := cloneCommitRequest(req)
	s.lastCommit = &cloned
	s.mu.Unlock()
	mode, err := s.next(MethodCommit)
	if err != nil {
		return CommitResult{}, err
	}
	commit := s.inner.Commit
	if exact {
		if exactCommitter, ok := s.inner.(ExactCommitter); ok {
			commit = exactCommitter.CommitExact
		}
	}
	switch mode {
	case FaultUnavailableNotSent:
		return CommitResult{Status: CommitUnavailableNotSent}, nil
	case FaultTimeoutUnknownBeforeApply:
		return CommitResult{Status: CommitTimeoutUnknown}, nil
	case FaultTimeoutUnknownAfterApply:
		if _, err := commit(ctx, req); err != nil {
			return CommitResult{}, err
		}
		return CommitResult{Status: CommitTimeoutUnknown}, nil
	default:
		return commit(ctx, req)
	}
}

func cloneCommitRequest(in AuthorityCommitRequest) AuthorityCommitRequest {
	if in.AssertedAuthorityGroup != nil {
		group := *in.AssertedAuthorityGroup
		in.AssertedAuthorityGroup = &group
	}
	in.CandidateState = cloneModelState(in.CandidateState)
	if in.ModelMutation != nil {
		mutation := *in.ModelMutation
		mutation.ModelUpserts = cloneModelState(in.ModelMutation.ModelUpserts)
		mutation.ModelDeletes = append([]string(nil), in.ModelMutation.ModelDeletes...)
		mutation.ResultingManifest = append([]string(nil), in.ModelMutation.ResultingManifest...)
		in.ModelMutation = &mutation
	}
	in.Receipt.Outcome.Payload = append([]byte(nil), in.Receipt.Outcome.Payload...)
	in.OperationResult.Payload = append([]byte(nil), in.OperationResult.Payload...)
	in.AttachedOrders = append([]AttachedOrder(nil), in.AttachedOrders...)
	for index := range in.AttachedOrders {
		in.AttachedOrders[index].Payload = append([]byte(nil), in.AttachedOrders[index].Payload...)
	}
	in.AttachedSchedules = append([]AttachedSchedule(nil), in.AttachedSchedules...)
	for index := range in.AttachedSchedules {
		in.AttachedSchedules[index].Payload = append([]byte(nil), in.AttachedSchedules[index].Payload...)
	}
	in.PendingOutbox = append([]PendingOutbox(nil), in.PendingOutbox...)
	for index := range in.PendingOutbox {
		in.PendingOutbox[index].Payload = append([]byte(nil), in.PendingOutbox[index].Payload...)
	}
	return in
}

func (s *FaultStore) FenceAndClaim(ctx context.Context, req FenceAndClaimRequest) (FenceAndClaimResult, error) {
	mode, err := s.next(MethodFenceAndClaim)
	if err != nil {
		return FenceAndClaimResult{}, err
	}
	switch mode {
	case FaultUnavailableNotSent:
		return FenceAndClaimResult{Status: FenceResolutionUnavailableNotSent}, nil
	case FaultTimeoutUnknownBeforeApply:
		return FenceAndClaimResult{Status: FenceResolutionTimeoutUnknown}, nil
	case FaultTimeoutUnknownAfterApply:
		if _, err := s.inner.FenceAndClaim(ctx, req); err != nil {
			return FenceAndClaimResult{}, err
		}
		return FenceAndClaimResult{Status: FenceResolutionTimeoutUnknown}, nil
	default:
		return s.inner.FenceAndClaim(ctx, req)
	}
}

func (s *FaultStore) LookupIdempotencyOutcome(ctx context.Context, req IdempotencyLookupRequest) (IdempotencyLookupResult, error) {
	mode, err := s.next(MethodLookupIdempotency)
	if err != nil {
		return IdempotencyLookupResult{}, err
	}
	if mode != FaultNone {
		return IdempotencyLookupResult{}, fmt.Errorf("%w: lookup mode %d", ErrInjectedFault, mode)
	}
	return s.inner.LookupIdempotencyOutcome(ctx, req)
}

func (s *FaultStore) ClaimShardAuthority(ctx context.Context, req ClaimShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	return s.runShardTransition(ctx, MethodClaimShardAuthority, func(store ShardWriteAuthorityStore) (ShardAuthorityTransitionResult, error) {
		return store.ClaimShardAuthority(ctx, req)
	})
}

func (s *FaultStore) RenewShardAuthority(ctx context.Context, req RenewShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	return s.runShardTransition(ctx, MethodRenewShardAuthority, func(store ShardWriteAuthorityStore) (ShardAuthorityTransitionResult, error) {
		return store.RenewShardAuthority(ctx, req)
	})
}

func (s *FaultStore) TransferShardAuthority(ctx context.Context, req TransferShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	return s.runShardTransition(ctx, MethodTransferShardAuthority, func(store ShardWriteAuthorityStore) (ShardAuthorityTransitionResult, error) {
		return store.TransferShardAuthority(ctx, req)
	})
}

func (s *FaultStore) TakeoverExpiredShardAuthority(ctx context.Context, req ExpiredTakeoverShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	return s.runExpiredTakeover(ctx, MethodTakeoverExpiredShardAuthority, func(store ExpiredTakeoverShardAuthorityStore) (ShardAuthorityTransitionResult, error) {
		return store.TakeoverExpiredShardAuthority(ctx, req)
	})
}

func (s *FaultStore) runExpiredTakeover(ctx context.Context, method Method, apply func(ExpiredTakeoverShardAuthorityStore) (ShardAuthorityTransitionResult, error)) (ShardAuthorityTransitionResult, error) {
	mode, err := s.next(method)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	store, ok := s.inner.(ExpiredTakeoverShardAuthorityStore)
	if !ok {
		return ShardAuthorityTransitionResult{}, fmt.Errorf("%w: delegate has no expired takeover store", ErrInvalidRequest)
	}
	switch mode {
	case FaultUnavailableNotSent:
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnavailableNotSent}, nil
	case FaultTimeoutUnknownBeforeApply:
		return ShardAuthorityTransitionResult{Status: ShardTransitionTimeoutUnknown}, nil
	case FaultTimeoutUnknownAfterApply:
		if _, err := apply(store); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
		return ShardAuthorityTransitionResult{Status: ShardTransitionTimeoutUnknown}, nil
	default:
		return apply(store)
	}
}

func (s *FaultStore) UnassignShardAuthority(ctx context.Context, req UnassignShardAuthorityRequest) (ShardAuthorityTransitionResult, error) {
	return s.runShardTransition(ctx, MethodUnassignShardAuthority, func(store ShardWriteAuthorityStore) (ShardAuthorityTransitionResult, error) {
		return store.UnassignShardAuthority(ctx, req)
	})
}

func (s *FaultStore) runShardTransition(ctx context.Context, method Method, apply func(ShardWriteAuthorityStore) (ShardAuthorityTransitionResult, error)) (ShardAuthorityTransitionResult, error) {
	mode, err := s.next(method)
	if err != nil {
		return ShardAuthorityTransitionResult{}, err
	}
	store, ok := s.inner.(ShardWriteAuthorityStore)
	if !ok {
		return ShardAuthorityTransitionResult{}, fmt.Errorf("%w: delegate has no shard authority store", ErrInvalidRequest)
	}
	switch mode {
	case FaultUnavailableNotSent:
		return ShardAuthorityTransitionResult{Status: ShardTransitionUnavailableNotSent}, nil
	case FaultTimeoutUnknownBeforeApply:
		return ShardAuthorityTransitionResult{Status: ShardTransitionTimeoutUnknown}, nil
	case FaultTimeoutUnknownAfterApply:
		if _, err := apply(store); err != nil {
			return ShardAuthorityTransitionResult{}, err
		}
		return ShardAuthorityTransitionResult{Status: ShardTransitionTimeoutUnknown}, nil
	default:
		return apply(store)
	}
}

func (s *FaultStore) next(method Method) (FaultMode, error) {
	if s == nil || s.inner == nil {
		return FaultNone, fmt.Errorf("%w: fault store has no delegate", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[method]++
	queue := s.queues[method]
	if len(queue) == 0 {
		return FaultNone, nil
	}
	mode := queue[0]
	s.queues[method] = queue[1:]
	return mode, nil
}
