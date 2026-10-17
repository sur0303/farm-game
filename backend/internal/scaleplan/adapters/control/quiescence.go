package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/routeplan"
	core "farmgame/internal/scaleplan"
)

var (
	ErrQuiescenceInvalid        = errors.New("scaleplan control adapter: invalid quiescence dependency")
	ErrQuiescenceEvidence       = errors.New("BLOCKED_QUIESCENCE_EVIDENCE")
	ErrPlacementReleaseRequired = errors.New("REQUIRED_FINDING: PlacementExclusionPort release is not available")
	ErrQuiescenceExpired        = errors.New("scaleplan control adapter: quiescence proof expired")
	ErrQuiescenceChanged        = errors.New("scaleplan control adapter: quiescence facts changed")
)

const (
	defaultBlockedProofTTL = 30 * time.Second
	maxBlockedProofTTL     = 5 * time.Minute
)

// ExclusiveControlTokenSource is the optional token-bearing extension
// implemented by the in-process exclusive fence. Durable child stop requires
// the same unforgeable issuer token as every other control mutation.
type ExclusiveControlTokenSource interface {
	ControlToken() core.ControlOperationToken
}

type BlockedQuiescenceOptions struct {
	Coordinator  ExclusiveControlCoordinator
	Scope        core.MutationScope
	Owner        core.MutationLeaseOwner
	Workloads    WorkloadStateReader
	Holders      core.HolderStateReader
	Placements   CompletePlacementReader
	Exclusions   core.PlacementExclusionPort
	RoutePlans   core.RoutePlanPort
	DurableStops routeplan.DurableStopStore
	Transitions  TransitionEnumeration
	Now          func() time.Time
	ProofTTL     time.Duration
}

type blockedQuiescencePort struct {
	coordinator  ExclusiveControlCoordinator
	scope        core.MutationScope
	owner        core.MutationLeaseOwner
	workloads    WorkloadStateReader
	holders      core.HolderStateReader
	placements   CompletePlacementReader
	exclusions   core.PlacementExclusionPort
	routePlans   core.RoutePlanPort
	durableStops routeplan.DurableStopStore
	transitions  TransitionEnumeration
	now          func() time.Time
	proofTTL     time.Duration
}

type blockedQuiescenceFence struct {
	port     *blockedQuiescencePort
	plan     core.Plan
	proof    core.BlockedQuiescenceProof
	manifest core.BlockedQuiescenceManifest
	token    core.ControlOperationToken

	exclusive ExclusiveControlFence
	mu        sync.Mutex
	released  bool
}

// NewBlockedQuiescencePort constructs the production blocked-quiescence
// adapter. It requires the complete placement extension so an evidence
// manifest cannot be made by stitching per-shard captures.
func NewBlockedQuiescencePort(options BlockedQuiescenceOptions) (core.BlockedQuiescencePort, error) {
	if options.Coordinator == nil || options.Workloads == nil || options.Holders == nil ||
		options.Placements == nil || options.Exclusions == nil || options.RoutePlans == nil ||
		options.DurableStops == nil || options.Transitions == nil {
		return nil, ErrQuiescenceInvalid
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if err := options.Owner.Validate(); err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.ProofTTL <= 0 {
		options.ProofTTL = defaultBlockedProofTTL
	}
	if options.ProofTTL > maxBlockedProofTTL {
		return nil, ErrQuiescenceInvalid
	}
	return &blockedQuiescencePort{
		coordinator: options.Coordinator, scope: options.Scope, owner: options.Owner,
		workloads: options.Workloads, holders: options.Holders, placements: options.Placements,
		exclusions: options.Exclusions, routePlans: options.RoutePlans, durableStops: options.DurableStops,
		transitions: options.Transitions, now: options.Now, proofTTL: options.ProofTTL,
	}, nil
}

func (p *blockedQuiescencePort) AcquireBlockedQuiescence(ctx context.Context, plan core.Plan) (core.BlockedQuiescenceManifest, core.BlockedQuiescenceFence, error) {
	if err := quiescenceContext(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	if p == nil {
		return core.BlockedQuiescenceManifest{}, nil, ErrQuiescenceInvalid
	}
	if err := plan.Validate(); err != nil {
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	fence, err := p.acquireExclusive(ctx, plan)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	source, ok := fence.(ExclusiveControlTokenSource)
	if !ok || !source.ControlToken().Issued() {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, fmt.Errorf("%w: exclusive token surface", ErrQuiescenceEvidence)
	}
	token := source.ControlToken()
	capturedAt := p.now().UnixMilli()
	if capturedAt <= 0 {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, ErrQuiescenceInvalid
	}
	expiresAt := capturedAt + p.proofTTL.Milliseconds()
	if expiresAt <= capturedAt {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, ErrQuiescenceInvalid
	}
	retainUntil := plan.RecoveryRetainUntilMs
	if expiresAt > retainUntil {
		retainUntil = expiresAt
	}

	initialTransitions, err := p.enumerateTransitions(ctx)
	if err != nil {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	captured, err := p.captureManifest(ctx, plan, token, capturedAt, expiresAt, retainUntil, initialTransitions, "")
	if err != nil {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	finalTransitions, err := p.enumerateTransitions(ctx)
	if err != nil {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	if err := compareTransitionEnumerations(initialTransitions, finalTransitions); err != nil {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	captured.ManagerTransitions.Universal.UnfinishedRecoveries = finalTransitions.unfinishedRecoveries
	captured.ManagerTransitions.Universal.UnfinishedTombstones = finalTransitions.unfinishedTombstones
	captured.ManagerTransitions.Universal.EnumeratedAtMs = capturedAt
	captured.ManagerTransitions.Universal.AssignmentRevision = finalTransitions.assignmentRevision
	proof, err := core.BuildBlockedQuiescenceProof(plan, captured)
	if err != nil {
		_ = fence.Release(context.WithoutCancel(ctx))
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	resultFence := &blockedQuiescenceFence{port: p, plan: plan.Clone(), proof: proof, manifest: cloneBlockedManifest(captured), token: token, exclusive: fence}
	return captured, resultFence, nil
}

func (p *blockedQuiescencePort) acquireExclusive(ctx context.Context, plan core.Plan) (ExclusiveControlFence, error) {
	request := core.ControlOperationRequest{
		OperationID: "sp-bq-" + shortOperationDigest(plan.PlanID, plan.Revision),
		Kind:        core.ControlOperationScalePlan,
		Scope:       p.scope,
		Owner:       p.owner,
	}
	return p.coordinator.AcquireExclusive(ctx, request)
}

func (p *blockedQuiescencePort) captureManifest(ctx context.Context, plan core.Plan, token core.ControlOperationToken,
	capturedAt, expiresAt, retainUntil int64, transitions transitionEnumeration, releaseOutcome core.PlacementReleaseOutcome) (core.BlockedQuiescenceManifest, error) {
	workload, err := p.workloads.Get(ctx, plan.Submission.Workload)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	holders, err := p.holders.ListDomainHolders(ctx, plan.Submission.Domain)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	holders = holdersForWorkload(workload, holders)
	applyPodNames(workload, holders)
	placement, err := p.placements.CaptureComplete(ctx)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if err := p.placements.Revalidate(ctx, placement.Token); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	children, managers, err := p.captureChildren(ctx, plan, token, retainUntil, transitions, placement.Placements)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	exclusion, err := p.captureExclusion(ctx, plan, releaseOutcome)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	manifest := core.BlockedQuiescenceManifest{
		Workload: core.BlockedWorkloadManifest{State: cloneWorkloadState(workload), Holders: cloneHolderStates(holders)},
		Placement: core.BlockedPlacementManifest{
			Complete: true, Domain: plan.Submission.Domain,
			SnapshotRevision:    placement.Token.SnapshotRevision,
			SnapshotChecksum:    placement.Token.SnapshotChecksum,
			TopologyFingerprint: placement.Token.TopologyFingerprint,
			TotalActive:         uint32(len(placement.Placements)),
			Placements:          clonePlacements(placement.Placements),
		},
		Exclusion: exclusion,
		Children:  core.BlockedChildInventoryManifest{Children: children},
		ManagerTransitions: core.BlockedManagerQuiescenceManifest{
			Records: managers,
			Universal: core.ManagerQuiescenceEnumeration{
				Domain: plan.Submission.Domain, Enumerated: true,
				UnfinishedRecoveries: transitions.unfinishedRecoveries,
				UnfinishedTombstones: transitions.unfinishedTombstones,
				AssignmentRevision:   placement.Token.AssignmentRevision,
				EnumeratedAtMs:       capturedAt,
			},
		},
		CapturedAtMs: capturedAt, ExpiresAtMs: expiresAt, EvidenceRetainUntilMs: retainUntil,
	}
	return manifest, nil
}

func (p *blockedQuiescencePort) captureExclusion(ctx context.Context, plan core.Plan,
	releaseOutcome core.PlacementReleaseOutcome) (core.BlockedExclusionManifest, error) {
	if plan.ExclusionIntent == nil {
		if releaseOutcome != "" {
			return core.BlockedExclusionManifest{}, fmt.Errorf("%w: release outcome without exclusion intent", ErrQuiescenceEvidence)
		}
		return core.BlockedExclusionManifest{}, nil
	}
	if p == nil || p.exclusions == nil {
		return core.BlockedExclusionManifest{}, ErrQuiescenceInvalid
	}
	state, found, err := p.exclusions.Get(ctx, plan.Victim)
	if err != nil {
		return core.BlockedExclusionManifest{}, err
	}
	intent := plan.ExclusionIntent
	if releaseOutcome == "" {
		if !found || state.Holder != plan.Victim || !state.Excluded ||
			state.OwnerPlanID != plan.PlanID || state.Revision != intent.ObservedRevision {
			return core.BlockedExclusionManifest{}, fmt.Errorf("%w: active exclusion evidence", ErrQuiescenceEvidence)
		}
		return core.BlockedExclusionManifest{Present: true, State: state}, nil
	}
	if !releaseOutcome.Valid() || !releaseOutcome.TerminalForScalePlan() {
		return core.BlockedExclusionManifest{}, fmt.Errorf("%w: unsafe release outcome %q", ErrQuiescenceEvidence, releaseOutcome)
	}
	if !found {
		if releaseOutcome != core.PlacementReleaseBootRollover {
			return core.BlockedExclusionManifest{}, fmt.Errorf("%w: released exclusion disappeared", ErrQuiescenceEvidence)
		}
		return core.BlockedExclusionManifest{ReleaseOutcome: releaseOutcome}, nil
	}
	if state.Holder != plan.Victim || state.Excluded || state.OwnerPlanID != "" ||
		state.Revision <= intent.ObservedRevision {
		return core.BlockedExclusionManifest{}, fmt.Errorf("%w: release projection", ErrQuiescenceEvidence)
	}
	return core.BlockedExclusionManifest{State: state, ReleaseOutcome: releaseOutcome}, nil
}

func (p *blockedQuiescencePort) captureChildren(ctx context.Context, plan core.Plan, token core.ControlOperationToken,
	retainUntil int64, transitions transitionEnumeration, placements []core.ShardPlacement) ([]core.BlockedChildManifest, []core.BlockedManagerQuiescenceRecord, error) {
	children := make([]core.BlockedChildManifest, len(plan.ResolvedMoves))
	managers := make([]core.BlockedManagerQuiescenceRecord, len(plan.ResolvedMoves))
	for index, move := range plan.ResolvedMoves {
		request := move.RoutePlanRequest(plan.Submission.RoutePlanTTLms, plan.PlanID, plan.RecoveryRetainUntilMs, plan.Submission.Limits, len(plan.Submission.Moves))
		view, found, err := p.routePlans.GetMove(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		if !found || view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest {
			return nil, nil, fmt.Errorf("%w: child routeplan evidence unavailable", ErrQuiescenceEvidence)
		}
		record, found, err := p.durableStops.Load(ctx, move.ChildPlanID)
		if err != nil {
			return nil, nil, err
		}
		if !found || record.Plan.OwnerScalePlanID != plan.PlanID ||
			record.Plan.TransitionID != move.ChildTransitionID ||
			record.Plan.Domain != move.Intent.Domain || record.Plan.VirtualShard != move.Intent.VirtualShard {
			return nil, nil, fmt.Errorf("%w: child immutable join", ErrQuiescenceEvidence)
		}
		stopResult := routeplan.DurableStopResult{Status: routeplan.DurableStopAlreadyTerminal, Record: record}
		if !record.Plan.Phase.Terminal() {
			stopResult, err = p.durableStops.StopOwnedPlanForScaleTermination(ctx, routeplan.DurableStopCommand{
				OwnerScalePlanID: plan.PlanID, PlanID: move.ChildPlanID,
				ExpectedRevision: record.Plan.Revision, ExpectedPhase: record.Plan.Phase,
				NowMs: p.now().UnixMilli(), Fence: token,
			})
			if err != nil {
				return nil, nil, err
			}
		}
		if !stopResult.DurablyStopped(plan.PlanID) {
			return nil, nil, fmt.Errorf("%w: child is not durably terminal", ErrQuiescenceEvidence)
		}
		record = stopResult.Record
		observedPhase := record.Plan.Phase.String()
		if view.Phase != observedPhase && observedRoutePlanTerminal(view.Phase) {
			return nil, nil, fmt.Errorf("%w: child view/record phase mismatch", ErrQuiescenceEvidence)
		}
		recoveryID := transitionEvidenceID(move, transitions)
		if recoveryID == "" {
			// A terminal child may have stopped before the Manager created any
			// transition evidence. Accept that case only when the complete
			// Authority+Snapshot placement capture still proves the exact frozen
			// source fence and endpoint. This is a positive no-effect proof, not
			// an absence-of-record shortcut.
			if (record.Plan.Phase == routeplan.PhasePaused || record.Plan.Phase == routeplan.PhaseFailed) &&
				childStillAtExactSource(move, placements) {
				recoveryID = "no-effect:" + move.ChildTransitionID
			} else {
				verification, verifyErr := p.routePlans.Verify(ctx, move.ChildPlanID)
				if verifyErr != nil || verification.PlanID != move.ChildPlanID || !verification.Consistent {
					return nil, nil, fmt.Errorf("%w: known child transition evidence", ErrQuiescenceEvidence)
				}
				recoveryID = "observed:" + move.ChildTransitionID
			}
		}
		children[index] = core.BlockedChildManifest{
			PlanID: move.ChildPlanID, TransitionID: move.ChildTransitionID, PayloadDigest: move.ChildPayloadDigest,
			ParentObservedPhase: move.ChildPhase, Phase: observedPhase, ErrorCode: record.Plan.ErrorCode,
			DurablyStopped: true, Source: move.Source, SourceEndpoint: move.SourceEndpoint,
			Target: move.Target, TargetEndpoint: move.TargetEndpoint,
			ExpectedSourceFence: move.ExpectedSourceFence,
			ManagerTransitionID: move.ChildTransitionID, ManagerRecoveryID: recoveryID,
			EvidenceRetainUntilMs: retainUntil,
		}
		managers[index] = core.BlockedManagerQuiescenceRecord{
			ChildPlanID: move.ChildPlanID, TransitionID: move.ChildTransitionID,
			RecoveryID: recoveryID, Quiescent: true, EvidenceRetainUntilMs: retainUntil,
		}
	}
	return children, managers, nil
}

func (f *blockedQuiescenceFence) Proof() core.BlockedQuiescenceProof {
	if f == nil {
		return core.BlockedQuiescenceProof{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proof
}

func (f *blockedQuiescenceFence) Revalidate(ctx context.Context) error {
	if err := quiescenceContext(ctx); err != nil {
		return err
	}
	if f == nil || f.port == nil {
		return ErrQuiescenceInvalid
	}
	f.mu.Lock()
	if f.released {
		f.mu.Unlock()
		return ErrQuiescenceChanged
	}
	plan, original, originalProof, p, exclusive, token := f.plan.Clone(), f.manifest, f.proof, f.port, f.exclusive, f.token
	f.mu.Unlock()
	if exclusive == nil || !token.Issued() {
		return ErrQuiescenceEvidence
	}
	if err := exclusive.Revalidate(ctx); err != nil {
		return err
	}
	now := p.now().UnixMilli()
	if now <= 0 || now >= originalProof.ExpiresAtMs {
		return ErrQuiescenceExpired
	}
	freshTransitions, err := p.enumerateTransitions(ctx)
	if err != nil {
		return err
	}
	if freshTransitions.unfinishedRecoveries != 0 || freshTransitions.unfinishedTombstones != 0 {
		return fmt.Errorf("%w: unfinished transition appeared", ErrQuiescenceEvidence)
	}
	fresh, err := p.captureManifest(ctx, plan, token, original.CapturedAtMs, original.ExpiresAtMs, original.EvidenceRetainUntilMs, freshTransitions, original.Exclusion.ReleaseOutcome)
	if err != nil {
		return err
	}
	if err := compareTransitionEnumerationsFromManifest(original, fresh); err != nil {
		return err
	}
	normalized := normalizeRenewableFacts(original, fresh)
	proof, err := core.BuildBlockedQuiescenceProof(plan, normalized)
	if err != nil {
		return err
	}
	if !sameQuiescenceProofEvidence(originalProof, proof) || fresh.Placement.SnapshotRevision != original.Placement.SnapshotRevision ||
		fresh.Placement.SnapshotChecksum != original.Placement.SnapshotChecksum ||
		fresh.Placement.TopologyFingerprint != original.Placement.TopologyFingerprint {
		return ErrQuiescenceChanged
	}
	return nil
}

func (p *blockedQuiescencePort) ReleaseExclusionForBlockedTermination(ctx context.Context, plan core.Plan,
	fence core.BlockedQuiescenceFence) (core.BlockedQuiescenceManifest, error) {
	if err := quiescenceContext(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if p == nil || p.exclusions == nil || fence == nil {
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceInvalid
	}
	if err := plan.Validate(); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if plan.ExclusionIntent == nil {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: exclusion intent required", ErrQuiescenceEvidence)
	}
	localFence, ok := fence.(*blockedQuiescenceFence)
	if !ok || localFence.port != p {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: foreign quiescence fence", ErrQuiescenceEvidence)
	}
	localFence.mu.Lock()
	if localFence.released {
		localFence.mu.Unlock()
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceChanged
	}
	fencePlan := localFence.plan.Clone()
	original := cloneBlockedManifest(localFence.manifest)
	exclusive, token := localFence.exclusive, localFence.token
	localFence.mu.Unlock()
	if plan.PlanID != fencePlan.PlanID || plan.Revision != fencePlan.Revision ||
		plan.PayloadDigest != fencePlan.PayloadDigest {
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceChanged
	}
	planCheckpoint, err := core.BlockedCheckpointDigest(plan)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	fenceCheckpoint, err := core.BlockedCheckpointDigest(fencePlan)
	if err != nil || planCheckpoint != fenceCheckpoint {
		if err == nil {
			err = ErrQuiescenceChanged
		}
		return core.BlockedQuiescenceManifest{}, err
	}
	intent := plan.ExclusionIntent
	if !original.Exclusion.Present || original.Exclusion.State.Holder != plan.Victim ||
		!original.Exclusion.State.Excluded || original.Exclusion.State.OwnerPlanID != plan.PlanID ||
		original.Exclusion.State.Revision != intent.ObservedRevision || original.Exclusion.ReleaseOutcome != "" {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: active exclusion fence evidence", ErrQuiescenceEvidence)
	}
	if exclusive == nil || !token.Issued() {
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceEvidence
	}
	source, ok := exclusive.(ExclusiveControlTokenSource)
	if !ok || source.ControlToken() != token {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: inherited token surface", ErrQuiescenceEvidence)
	}
	if err := exclusive.Revalidate(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	tokenAware, ok := p.exclusions.(TokenAwarePlacementExclusionPort)
	if !ok {
		return core.BlockedQuiescenceManifest{}, ErrPlacementReleaseRequired
	}
	result, err := tokenAware.ReleaseForOwnerWithToken(ctx, token, plan.Victim, core.PlacementExclusionRelease{
		OwnerPlanID: intent.OwnerPlanID, ExpectedRevision: intent.ObservedRevision,
	})
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if !result.Outcome.Valid() || !result.Outcome.TerminalForScalePlan() {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: release outcome=%s", ErrQuiescenceEvidence, result.Outcome)
	}
	capturedAt := p.now().UnixMilli()
	if capturedAt <= 0 || capturedAt >= original.ExpiresAtMs {
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceExpired
	}
	freshTransitions, err := p.enumerateTransitions(ctx)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if freshTransitions.unfinishedRecoveries != 0 || freshTransitions.unfinishedTombstones != 0 {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: unfinished transition appeared", ErrQuiescenceEvidence)
	}
	fresh, err := p.captureManifest(ctx, plan, token, capturedAt, original.ExpiresAtMs,
		original.EvidenceRetainUntilMs, freshTransitions, result.Outcome)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if fresh.Exclusion.Present || fresh.Exclusion.State.Excluded || fresh.Exclusion.State.OwnerPlanID != "" {
		return core.BlockedQuiescenceManifest{}, fmt.Errorf("%w: exclusion remained after release", ErrQuiescenceEvidence)
	}
	if err := exclusive.Revalidate(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	proof, err := core.BuildBlockedQuiescenceProof(plan, fresh)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	localFence.mu.Lock()
	if localFence.released {
		localFence.mu.Unlock()
		return core.BlockedQuiescenceManifest{}, ErrQuiescenceChanged
	}
	localFence.manifest = cloneBlockedManifest(fresh)
	localFence.proof = proof
	localFence.mu.Unlock()
	return fresh, nil
}

func (f *blockedQuiescenceFence) Release(ctx context.Context) error {
	if err := quiescenceContext(ctx); err != nil {
		return err
	}
	if f == nil || f.port == nil {
		return ErrQuiescenceInvalid
	}
	f.mu.Lock()
	if f.released {
		f.mu.Unlock()
		return nil
	}
	exclusive := f.exclusive
	err := exclusive.Release(ctx)
	if err == nil {
		f.released = true
	}
	f.mu.Unlock()
	return err
}

func (p *blockedQuiescencePort) coordinatorRelease(ctx context.Context, plan core.Plan) error {

	request := core.ControlOperationRequest{
		OperationID: "sp-bq-" + shortOperationDigest(plan.PlanID, plan.Revision),
		Kind:        core.ControlOperationScalePlan, Scope: p.scope, Owner: p.owner,
	}
	fence, err := p.coordinator.AcquireExclusive(ctx, request)
	if err != nil {
		return err
	}
	return fence.Release(ctx)
}
func childStillAtExactSource(move core.ResolvedMove, placements []core.ShardPlacement) bool {
	for _, placement := range placements {
		if placement.Domain != move.Intent.Domain || placement.VirtualShard != move.Intent.VirtualShard {
			continue
		}
		return placement.Active && placement.KnownFresh && placement.Holder == move.Source &&
			placement.Endpoint == move.SourceEndpoint && placement.ExpectedSourceFence == move.ExpectedSourceFence
	}
	return false
}

func observedRoutePlanTerminal(phase string) bool {
	return phase == core.RoutePlanPhaseSucceeded || phase == core.RoutePlanPhaseFailed || phase == core.RoutePlanPhasePaused
}

type transitionEnumeration struct {
	recoveries []authority.TransitionRecovery

	tombstones           []authority.TransitionTombstone
	unfinishedRecoveries uint32
	unfinishedTombstones uint32
	assignmentRevision   uint64
	tombstoneDigest      string
}

func (p *blockedQuiescencePort) enumerateTransitions(ctx context.Context) (transitionEnumeration, error) {
	recoveries, err := p.transitions.LoadTransitionRecoveries(ctx)
	if err != nil {
		return transitionEnumeration{}, err
	}
	tombstones, err := p.transitions.LoadTransitionTombstones(ctx)
	if err != nil {
		return transitionEnumeration{}, err
	}
	var unfinishedRecoveries uint32
	for _, recovery := range recoveries {
		if recovery.TransitionID == "" || recovery.Fingerprint == "" {
			return transitionEnumeration{}, fmt.Errorf("%w: malformed recovery", ErrQuiescenceEvidence)
		}
		// A retained ACTIVE recovery is terminal positive evidence for a
		// completed child, not unfinished Manager work. ASSIGNING, unpublished,
		// or uncommitted records still fail the universal quiescence barrier.
		if !recovery.Committed || !recovery.Published || recovery.LastState != actorrouting.ShardStateActive {
			unfinishedRecoveries++
		}
	}
	parts := make([]string, 0, len(tombstones))
	for _, tombstone := range tombstones {
		if tombstone.TransitionID == "" || tombstone.Fingerprint == "" || tombstone.ExpiredAt.IsZero() {
			return transitionEnumeration{}, fmt.Errorf("%w: malformed tombstone", ErrQuiescenceEvidence)
		}
		parts = append(parts, tombstone.TransitionID+"\x1f"+tombstone.Fingerprint)
	}
	sort.Strings(parts)
	result := transitionEnumeration{
		recoveries: recoveries, tombstones: tombstones,
		unfinishedRecoveries: unfinishedRecoveries, unfinishedTombstones: 0,
		tombstoneDigest: strings.Join(parts, "\x1e"),
	}
	if unfinishedRecoveries != 0 {
		return result, fmt.Errorf("%w: unfinished recoveries remain", ErrQuiescenceEvidence)
	}
	return result, nil
}

func compareTransitionEnumerations(before, after transitionEnumeration) error {
	if before.tombstoneDigest != after.tombstoneDigest ||
		before.unfinishedRecoveries != after.unfinishedRecoveries ||
		before.unfinishedTombstones != after.unfinishedTombstones {
		return fmt.Errorf("%w: transition enumeration changed", ErrQuiescenceChanged)
	}
	return nil
}

func compareTransitionEnumerationsFromManifest(before core.BlockedQuiescenceManifest, after core.BlockedQuiescenceManifest) error {
	left, right := before.ManagerTransitions.Universal, after.ManagerTransitions.Universal
	if left.Domain != right.Domain || left.AssignmentRevision != right.AssignmentRevision ||
		left.UnfinishedRecoveries != right.UnfinishedRecoveries || left.UnfinishedTombstones != right.UnfinishedTombstones {
		return fmt.Errorf("%w: manager universal evidence changed", ErrQuiescenceChanged)
	}
	return nil
}

func transitionEvidenceID(move core.ResolvedMove, transitions transitionEnumeration) string {
	for _, recovery := range transitions.recoveries {
		if recovery.TransitionID == move.ChildTransitionID && recovery.Domain == move.Intent.Domain &&
			recovery.VirtualShard == move.Intent.VirtualShard {
			return "recovery:" + recovery.Fingerprint
		}
	}
	for _, tombstone := range transitions.tombstones {
		if tombstone.TransitionID == move.ChildTransitionID {
			return "tombstone:" + tombstone.Fingerprint
		}
	}
	return ""
}

func holdersForWorkload(workload core.WorkloadState, holders []core.HolderState) []core.HolderState {
	out := make([]core.HolderState, 0, len(holders))
	for _, holder := range holders {
		if holder.Ref.WorkloadUID == workload.UID {
			out = append(out, holder)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Ordinal < out[j].Ref.Ordinal })
	return out
}

func applyPodNames(workload core.WorkloadState, holders []core.HolderState) {
	byUID := make(map[string]string, len(workload.Pods))
	for _, pod := range workload.Pods {
		if pod.PodUID != "" {
			byUID[pod.PodUID] = pod.PodName
		}
	}
	for index := range holders {
		if name, found := byUID[holders[index].Ref.PodUID]; found {
			holders[index].PodName = name
		}
	}
}

func cloneWorkloadState(value core.WorkloadState) core.WorkloadState {
	value.Pods = append([]core.WorkloadPodState(nil), value.Pods...)
	return value
}

func cloneHolderStates(value []core.HolderState) []core.HolderState {
	return append([]core.HolderState(nil), value...)
}

func clonePlacements(value []core.ShardPlacement) []core.ShardPlacement {
	return append([]core.ShardPlacement(nil), value...)
}

func cloneBlockedManifest(value core.BlockedQuiescenceManifest) core.BlockedQuiescenceManifest {
	value.Workload.State = cloneWorkloadState(value.Workload.State)
	value.Workload.Holders = cloneHolderStates(value.Workload.Holders)
	value.Placement.Placements = clonePlacements(value.Placement.Placements)
	value.Children.Children = append([]core.BlockedChildManifest(nil), value.Children.Children...)
	value.ManagerTransitions.Records = append([]core.BlockedManagerQuiescenceRecord(nil), value.ManagerTransitions.Records...)
	return value
}

func normalizeRenewableFacts(original, fresh core.BlockedQuiescenceManifest) core.BlockedQuiescenceManifest {
	out := cloneBlockedManifest(fresh)
	out.CapturedAtMs = original.CapturedAtMs
	out.ExpiresAtMs = original.ExpiresAtMs
	out.EvidenceRetainUntilMs = original.EvidenceRetainUntilMs
	out.ManagerTransitions.Universal.EnumeratedAtMs = original.ManagerTransitions.Universal.EnumeratedAtMs
	holderDeadlines := make(map[core.HolderRef]time.Time, len(original.Workload.Holders))
	for _, holder := range original.Workload.Holders {
		holderDeadlines[holder.Ref] = holder.LeaseUntil
	}
	for index := range out.Workload.Holders {
		if deadline, found := holderDeadlines[out.Workload.Holders[index].Ref]; found {
			out.Workload.Holders[index].LeaseUntil = deadline
		}
	}
	placementDeadlines := make(map[core.ShardKey]time.Time, len(original.Placement.Placements))
	for _, placement := range original.Placement.Placements {
		placementDeadlines[core.ShardKey{Domain: placement.Domain, VirtualShard: placement.VirtualShard}] = placement.LeaseUntil
	}
	for index := range out.Placement.Placements {
		key := core.ShardKey{Domain: out.Placement.Placements[index].Domain, VirtualShard: out.Placement.Placements[index].VirtualShard}
		if deadline, found := placementDeadlines[key]; found {
			out.Placement.Placements[index].LeaseUntil = deadline
		}
	}
	return out
}

func sameQuiescenceProofEvidence(left, right core.BlockedQuiescenceProof) bool {
	return left.ProtocolVersion == right.ProtocolVersion &&
		left.PlanID == right.PlanID &&
		left.ExpectedRevision == right.ExpectedRevision &&
		left.ExpectedBlockedReason == right.ExpectedBlockedReason &&
		left.ExpectedPayloadDigest == right.ExpectedPayloadDigest &&
		left.ExpectedCheckpointDigest == right.ExpectedCheckpointDigest &&
		left.EvidenceDigest == right.EvidenceDigest &&
		left.WorkloadDigest == right.WorkloadDigest &&
		left.PlacementDigest == right.PlacementDigest &&
		left.ExclusionDigest == right.ExclusionDigest &&
		left.ChildInventoryDigest == right.ChildInventoryDigest &&
		left.ManagerQuiescenceDigest == right.ManagerQuiescenceDigest &&
		left.CapturedAtMs == right.CapturedAtMs && left.ExpiresAtMs == right.ExpiresAtMs &&
		left.EvidenceRetainUntilMs == right.EvidenceRetainUntilMs &&
		left.IntentCount == right.IntentCount && left.ChildCount == right.ChildCount
}

func shortOperationDigest(planID string, revision uint64) string {
	value := fmt.Sprintf("%s:%d", planID, revision)
	return strings.TrimPrefix(digestOperation(value), "op-")
}

func digestOperation(value string) string {
	var hash [32]byte
	hash = sha256.Sum256([]byte(value))
	return "op-" + hex.EncodeToString(hash[:16])
}

func quiescenceContext(ctx context.Context) error {
	if ctx == nil {
		return ErrQuiescenceInvalid
	}
	return ctx.Err()
}

var _ core.BlockedQuiescencePort = (*blockedQuiescencePort)(nil)
var _ core.BlockedExclusionReleasePort = (*blockedQuiescencePort)(nil)
var _ core.BlockedQuiescenceFence = (*blockedQuiescenceFence)(nil)
var _ ExclusiveControlTokenSource = (*exclusiveFence)(nil)
