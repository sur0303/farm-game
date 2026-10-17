package routeplan

import (
	"context"
	"errors"
	"log"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/storage/authoritystore"
)

type ManagerTransitionObserver interface {
	ObserveTransition(context.Context, authority.TransitionObservationRequest) (authority.TransitionObservation, error)
}

type GateObservationFleet interface {
	ObserveShard(context.Context, string, actorrouting.RouteDomain, uint32) ([]gateverification.PeerObservation, error)
	Close() error
}

type ProductionVerificationOptions struct {
	Manager        ManagerTransitionObserver
	Gates          GateObservationFleet
	CallTimeout    time.Duration
	ExpectedPeers  int
	Now            func() time.Time
	LeaseCheckMode actorrouting.LeaseCheckMode
}

// ProductionVerification is the real read-only VerificationPort. Its private
// marker prevents command-root composition from accepting an arbitrary noop.
type ProductionVerification struct {
	manager        ManagerTransitionObserver
	gates          GateObservationFleet
	timeout        time.Duration
	peers          int
	now            func() time.Time
	leaseCheckMode actorrouting.LeaseCheckMode
}

func NewProductionVerification(options ProductionVerificationOptions) (*ProductionVerification, error) {
	if options.Manager == nil || options.Gates == nil || options.ExpectedPeers <= 0 || options.ExpectedPeers > gateverification.MaxPeers {
		return nil, ErrExecutorInvalid
	}
	timeout := options.CallTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if timeout > 30*time.Second {
		return nil, ErrExecutorInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	leaseCheckMode := options.LeaseCheckMode
	if leaseCheckMode == "" {
		leaseCheckMode = actorrouting.LeaseCheckSnapshot
	}
	if leaseCheckMode != actorrouting.LeaseCheckSnapshot && leaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return nil, ErrExecutorInvalid
	}
	return &ProductionVerification{manager: options.Manager, gates: options.Gates, timeout: timeout, peers: options.ExpectedPeers, now: now, leaseCheckMode: leaseCheckMode}, nil
}

func (*ProductionVerification) productionVerificationPort() {}

func IsProductionVerificationPort(port VerificationPort) bool {
	production, ok := port.(*ProductionVerification)
	return ok && production != nil && production.manager != nil && production.gates != nil && production.timeout > 0 && production.peers > 0 && production.now != nil && (production.leaseCheckMode == actorrouting.LeaseCheckSnapshot || production.leaseCheckMode == actorrouting.LeaseCheckAuthorityDelegated)
}

// ManagerTransitionRequestForPlan is the single shared Plan-to-Manager
// request builder used by both execution and verification.
func ManagerTransitionRequestForPlan(plan Plan) (authority.TransitionObservationRequest, error) {
	if err := plan.Validate(); err != nil {
		return authority.TransitionObservationRequest{}, err
	}
	request := authority.TransferRequest{
		TransitionID: plan.TransitionID, Domain: plan.Domain, VirtualShard: plan.VirtualShard,
		ExpectedFence: plan.ExpectedSourceFence, NewHolderID: plan.TargetHolderID,
		NewHolderBootID: plan.TargetBootID, SourcePodUID: plan.SourceDeploymentIdentity.PodUID,
		SourceWorkloadUID: plan.SourceDeploymentIdentity.WorkloadUID, SourceOrdinal: plan.SourceDeploymentIdentity.Ordinal,
		TargetPodUID: plan.TargetDeploymentIdentity.PodUID, TargetWorkloadUID: plan.TargetDeploymentIdentity.WorkloadUID,
		TargetOrdinal: plan.TargetDeploymentIdentity.Ordinal, DeploymentIdentityPresent: plan.Operation == OperationPlannedDrain && !plan.SourceDeploymentIdentity.IsZero(),
		Endpoint:     plan.TargetEndpoint,
		RequestedTTL: time.Duration(plan.RequestedTTLms) * time.Millisecond,
	}
	observation := authority.TransitionObservationRequest{Transfer: request}
	switch plan.Operation {
	case OperationPlannedDrain:
		observation.PlannedDrain = true
		if plan.Domain == actorrouting.DomainFarmWorker {
			observation.Transfer.Mode = authority.TransferModeWorkerHandoff
		}
	case OperationCrashTakeover:
		observation.Transfer.Mode = authority.TransferModeCrashTakeover
		observation.Transfer.TakeoverProof = authority.TransferTakeoverProof{
			ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable:    plan.TakeoverProof.HolderUnavailable,
			LeaseExpired:         plan.TakeoverProof.LeaseExpired,
			Evidence:             plan.TakeoverProof.Evidence,
		}
	default:
		return authority.TransitionObservationRequest{}, ErrInvalidPlan
	}
	return observation, nil
}

func (v *ProductionVerification) VerifyGateResolution(ctx context.Context, plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot) error {
	if v == nil || v.now == nil || !v.currentProjectionMatches(plan, value, snapshot, v.now()) {
		return ErrVerification
	}
	bounded, cancel, err := v.context(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	observations, err := v.gates.ObserveShard(bounded, snapshot.Realm, plan.Domain, plan.VirtualShard)
	if err != nil || len(observations) != v.peers {
		return ErrVerification
	}
	observedAt := v.now()
	route, _, routeErr := snapshotRoute(snapshot, plan.Domain, plan.VirtualShard)
	if routeErr != nil {
		return ErrVerification
	}
	for _, peer := range observations {
		observed := peer.Observation
		leaseCompatible := observed.LeaseUntilMs == route.LeaseUntilMs &&
			observed.LeaseUntilMs <= value.LeaseUntilMs && observed.LeaseUntilMs > observedAt.UnixMilli()
		if v.leaseCheckMode == actorrouting.LeaseCheckAuthorityDelegated {
			// Gate route observations carry topology/fence data. Their deadline
			// may legitimately lag the Redis Authority value because ordinary
			// renewal no longer republishes Assignment Snapshot.
			leaseCompatible = observed.LeaseUntilMs > 0 && observed.LeaseUntilMs <= value.LeaseUntilMs
		}
		if observed.Realm != snapshot.Realm || observed.Domain != plan.Domain || observed.VirtualShard != plan.VirtualShard ||
			observed.HolderID != plan.TargetHolderID || observed.HolderBootID != plan.TargetBootID ||
			observed.LogicEndpoint != plan.TargetEndpoint || observed.State != actorrouting.ShardStateActive ||
			observed.AssignmentRevision != value.AssignmentRevision || observed.WriterEpoch != value.WriterEpoch ||
			observed.LeaseEpoch != value.LeaseEpoch || !leaseCompatible ||
			observed.SnapshotRevision != snapshot.GlobalRevision {
			return ErrVerification
		}
	}
	return nil
}

func (v *ProductionVerification) VerifyTransition(ctx context.Context, plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot) error {
	observation, err := v.observeManager(ctx, plan)
	if err != nil {
		log.Printf("route verification transition observation failed: %v", err)
		return ErrVerification
	}
	if !v.observedTerminalMatches(plan, value, snapshot, observation) {
		return ErrVerification
	}
	return nil
}

func (v *ProductionVerification) VerifyRecovery(ctx context.Context, plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot) error {
	observation, err := v.observeManager(ctx, plan)
	if err != nil || !v.observedTerminalMatches(plan, value, snapshot, observation) || !recoveryOrReplayFloorMatches(plan, observation) {
		return ErrVerification
	}
	return nil
}

func (v *ProductionVerification) VerifyTombstone(ctx context.Context, plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot) error {
	observation, err := v.observeManager(ctx, plan)
	if err != nil || !v.observedTerminalMatches(plan, value, snapshot, observation) || !terminalReplayFloorMatches(plan, observation) {
		return ErrVerification
	}
	return nil
}

func (v *ProductionVerification) observedTerminalMatches(plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot, observation authority.TransitionObservation) bool {
	if v == nil || v.now == nil || !v.currentProjectionMatches(plan, value, snapshot, v.now()) || observation.Fingerprint == "" ||
		(observation.Result.Status != authoritystore.ShardTransitionApplied && observation.Result.Status != authoritystore.ShardTransitionAlreadyApplied) ||
		!authorityDeadlineRenewalCompatible(observation.Result.Authority, value) ||
		!acceptedTTLMatchesRequest(observation.Result.AcceptedTTLms, plan.RequestedTTLms) {
		return false
	}
	route, _, err := snapshotRoute(snapshot, plan.Domain, plan.VirtualShard)
	if err != nil {
		return false
	}
	var durableResult authoritystore.ShardAuthorityTransitionResult
	switch observation.Class {
	case authority.TransitionEvidenceCompleteRecovery:
		durableResult = observation.Recovery.Result
		if observation.Recovery.TransitionID != plan.TransitionID || observation.Recovery.Fingerprint != observation.Fingerprint ||
			observation.Tombstone != (authority.TransitionTombstone{}) {
			return false
		}
	case authority.TransitionEvidenceTombstone:
		durableResult = observation.Tombstone.Result
		if observation.Tombstone.TransitionID != plan.TransitionID || observation.Tombstone.Fingerprint != observation.Fingerprint ||
			observation.Recovery != (authority.TransitionRecovery{}) {
			return false
		}
	default:
		return false
	}
	return transitionResultDeadlineRenewalCompatible(durableResult, observation.Result) &&
		route.LeaseUntilMs >= durableResult.Authority.LeaseUntilMs &&
		route.LeaseUntilMs <= value.LeaseUntilMs &&
		routeMatchesAuthority(snapshot, plan, value)
}

func (v *ProductionVerification) currentProjectionMatches(plan Plan, value authoritystore.ShardWriteAuthority, snapshot actorrouting.RouteSnapshot, now time.Time) bool {
	if v == nil || v.now == nil || plan.Validate() != nil || value.LeaseUntilMs <= now.UnixMilli() ||
		value.AssignmentRevision == 0 || value.WriterEpoch == 0 || value.LeaseEpoch == 0 || value.LastTransitionAtMs <= 0 ||
		!routeMatchesAuthority(snapshot, plan, value) {
		return false
	}
	route, _, err := snapshotRoute(snapshot, plan.Domain, plan.VirtualShard)
	if err != nil {
		return false
	}
	return v.leaseCheckMode == actorrouting.LeaseCheckAuthorityDelegated || route.LeaseUntilMs > now.UnixMilli()
}

func recoveryOrReplayFloorMatches(plan Plan, observation authority.TransitionObservation) bool {
	switch observation.Class {
	case authority.TransitionEvidenceCompleteRecovery:
		return completeRecoveryMatches(plan, observation)
	case authority.TransitionEvidenceTombstone:
		return atomicTombstoneMatches(plan, observation)
	default:
		return false
	}
}

// terminalReplayFloorMatches is deliberately separate from the recovery
// predicate. Manager retains a fully validated CompleteRecovery as the normal
// durable replay floor and atomically replaces it with a Tombstone only when
// the corresponding memo is evicted. Either exact representation proves the
// same ICR terminal floor; partial, overlapping, or mismatched evidence does
// not.
func terminalReplayFloorMatches(plan Plan, observation authority.TransitionObservation) bool {
	switch observation.Class {
	case authority.TransitionEvidenceCompleteRecovery:
		return completeRecoveryMatches(plan, observation)
	case authority.TransitionEvidenceTombstone:
		return atomicTombstoneMatches(plan, observation)
	default:
		return false
	}
}

func completeRecoveryMatches(plan Plan, observation authority.TransitionObservation) bool {
	recovery := observation.Recovery
	plannedDrain := plan.Operation == OperationPlannedDrain
	if observation.Class != authority.TransitionEvidenceCompleteRecovery || observation.Tombstone != (authority.TransitionTombstone{}) ||
		recovery.TransitionID != plan.TransitionID || recovery.Fingerprint != observation.Fingerprint ||
		recovery.Domain != plan.Domain || recovery.VirtualShard != plan.VirtualShard || recovery.ExpectedFence != plan.ExpectedSourceFence ||
		recovery.NewHolderID != plan.TargetHolderID || recovery.NewHolderBootID != plan.TargetBootID || recovery.Endpoint != plan.TargetEndpoint ||
		recovery.RequestedTTLms != plan.RequestedTTLms || recovery.InternalDrain != plannedDrain ||
		!transitionResultDeadlineRenewalCompatible(recovery.Result, observation.Result) ||
		!recovery.Committed || !recovery.Published || !recovery.AuthorityAttempted || !recovery.AuthorityAttemptedPresent ||
		recovery.RecoveryWireVersion != authority.TransitionRecoveryWireAuthorityAttempted || recovery.LastState != actorrouting.ShardStateActive {
		return false
	}
	if plannedDrain {
		if plan.Domain == actorrouting.DomainFarmWorker {
			return recovery.Mode == authority.TransferModeWorkerHandoff &&
				recovery.TakeoverProof == (authority.TransferTakeoverProof{}) &&
				recovery.InternalDrain &&
				!recovery.ColdLoaded
		}
		return recovery.Mode == authority.TransferModeDrainHandoff &&
			recovery.TakeoverProof == (authority.TransferTakeoverProof{}) &&
			recovery.InternalDrain &&
			recovery.ColdLoaded
	}
	return recovery.Mode == authority.TransferModeCrashTakeover && !recovery.ColdLoaded &&
		recovery.TakeoverProof == (authority.TransferTakeoverProof{
			ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable:    plan.TakeoverProof.HolderUnavailable,
			LeaseExpired:         plan.TakeoverProof.LeaseExpired,
			Evidence:             plan.TakeoverProof.Evidence,
		})
}

func atomicTombstoneMatches(plan Plan, observation authority.TransitionObservation) bool {
	tombstone := observation.Tombstone
	return observation.Class == authority.TransitionEvidenceTombstone && observation.Recovery == (authority.TransitionRecovery{}) &&
		observation.Fingerprint != "" && tombstone.TransitionID == plan.TransitionID && tombstone.Fingerprint == observation.Fingerprint &&
		transitionResultDeadlineRenewalCompatible(tombstone.Result, observation.Result) && !tombstone.ExpiredAt.IsZero()
}

func transitionResultDeadlineRenewalCompatible(durable, current authoritystore.ShardAuthorityTransitionResult) bool {
	return durable.Status == current.Status && durable.AcceptedTTLms == current.AcceptedTTLms &&
		authorityDeadlineRenewalCompatible(durable.Authority, current.Authority)
}

// acceptedTTLMatchesRequest treats AcceptedTTLms as the remaining grant at
// commit, not a copy of the plan request. Authority may clamp to the shard
// lease ceiling; verification still rejects a zero grant or an over-grant.
func acceptedTTLMatchesRequest(accepted, requested int64) bool {
	return accepted > 0 && requested > 0 && accepted <= requested
}

func (v *ProductionVerification) observeManager(ctx context.Context, plan Plan) (authority.TransitionObservation, error) {
	request, err := ManagerTransitionRequestForPlan(plan)
	if err != nil {
		return authority.TransitionObservation{}, err
	}
	bounded, cancel, err := v.context(ctx)
	if err != nil {
		return authority.TransitionObservation{}, err
	}
	defer cancel()
	return v.manager.ObserveTransition(bounded, request)
}

func (v *ProductionVerification) context(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if v == nil || v.manager == nil || v.gates == nil || v.now == nil || ctx == nil {
		return nil, nil, ErrExecutorInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, v.timeout)
	return bounded, cancel, nil
}

func (v *ProductionVerification) Close() error {
	if v == nil || v.gates == nil {
		return nil
	}
	if err := v.gates.Close(); err != nil && !errors.Is(err, gateverification.ErrClosed) {
		return err
	}
	return nil
}
