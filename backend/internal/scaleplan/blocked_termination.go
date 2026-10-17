package scaleplan

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultBlockedProofMaxAge = time.Minute
	maximumBlockedProofMaxAge = 5 * time.Minute
)

type BlockedTerminationOwnerOptions struct {
	Repository ScalePlanRepository
	Quiescence BlockedQuiescencePort
	// Scope is the realm-global serialization domain this owner may take the
	// mutation barrier for. It is construction-time rather than per-call
	// because Submit enters the barrier before a PlanID exists (§5.3).
	Scope       MutationScope
	Now         func() time.Time
	MaxProofAge time.Duration
}

// BlockedTerminationOwner is the one process-wide protocol owner supplied to
// all Executors sharing a repository. Its barrier spans proof-fence acquire,
// final revalidation, atomic terminal commit, and fence release.
type blockedTerminationRepository interface {
	ScalePlanRepository
	commitBlockedTermination(context.Context, BlockedTerminationRequest, BlockedQuiescenceProof) (BlockedTerminationResult, error)
	enterPlanMutation(context.Context, MutationScope) (PlanMutationGuard, error)
	repositoryIdentity() *repositoryIdentity
}

type BlockedTerminationOwner struct {
	repository  blockedTerminationRepository
	quiescence  BlockedQuiescencePort
	scope       MutationScope
	now         func() time.Time
	maxProofAge time.Duration
}

func NewBlockedTerminationOwner(options BlockedTerminationOwnerOptions) (*BlockedTerminationOwner, error) {
	if options.Repository == nil || options.Quiescence == nil {
		return nil, fmt.Errorf("%w: blocked termination repository/quiescence required", ErrInvalidPlan)
	}
	repository, ok := options.Repository.(blockedTerminationRepository)
	if !ok {
		return nil, fmt.Errorf("%w: proof-owned repository capability required", ErrInvalidPlan)
	}
	// A production facade must be paired by NewProductionRuntime, which owns
	// the only sanctioned owner. Letting this constructor accept one would
	// hand any caller an alternate owner over production state.
	if _, production := options.Repository.(*productionRepositoryFacade); production {
		return nil, fmt.Errorf("%w: production facade requires paired construction", ErrInvalidPlan)
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxProofAge == 0 {
		options.MaxProofAge = defaultBlockedProofMaxAge
	}
	if options.MaxProofAge <= 0 || options.MaxProofAge > maximumBlockedProofMaxAge {
		return nil, fmt.Errorf("%w: blocked proof maximum age", ErrInvalidPlan)
	}
	return &BlockedTerminationOwner{repository: repository, quiescence: options.Quiescence,
		scope: options.Scope, now: options.Now, maxProofAge: options.MaxProofAge}, nil
}

// newPairedBlockedTerminationOwner is the production path. It bypasses the
// facade rejection above precisely because it is the pairing constructor.
func newPairedBlockedTerminationOwner(repository blockedTerminationRepository, options BlockedTerminationOwnerOptions) (*BlockedTerminationOwner, error) {
	if repository == nil || options.Quiescence == nil {
		return nil, fmt.Errorf("%w: blocked termination repository/quiescence required", ErrInvalidPlan)
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxProofAge == 0 {
		options.MaxProofAge = defaultBlockedProofMaxAge
	}
	if options.MaxProofAge <= 0 || options.MaxProofAge > maximumBlockedProofMaxAge {
		return nil, fmt.Errorf("%w: blocked proof maximum age", ErrInvalidPlan)
	}
	return &BlockedTerminationOwner{repository: repository, quiescence: options.Quiescence,
		scope: options.Scope, now: options.Now, maxProofAge: options.MaxProofAge}, nil
}

// enterPlanMutation is unexported because §5.2's encapsulation is aimed at
// exactly this method: every caller is inside this package, so narrowing it
// breaks nothing, while leaving it exported would let another package take the
// barrier without going through the sealed facade. The facade's own
// ProductionRuntime.EnterPlanMutation stays exported for cross-package callers.
func (o *BlockedTerminationOwner) enterPlanMutation(ctx context.Context, scope MutationScope) (PlanMutationGuard, error) {
	if o == nil || ctx == nil {
		return nil, ErrInvalidPlan
	}
	if scope != o.scope {
		return nil, fmt.Errorf("%w: mutation scope outside owner realm", ErrInvalidPlan)
	}
	guard, err := o.repository.enterPlanMutation(ctx, scope)
	if err != nil {
		return nil, err
	}
	if guard == nil || !guard.Lease().Held() {
		return nil, fmt.Errorf("%w: mutation guard not held", ErrInvalidPlan)
	}
	return guard, nil
}

func (o *BlockedTerminationOwner) TerminalizeBlocked(ctx context.Context, request BlockedTerminationRequest) (result BlockedTerminationResult, err error) {
	if o == nil || ctx == nil {
		return result, ErrInvalidPlan
	}
	if err = validateBlockedTerminationRequest(request); err != nil {
		return result, err
	}
	guard, err := o.enterPlanMutation(ctx, o.scope)
	if err != nil {
		return result, err
	}
	defer releaseGuard(guard, ctx)
	// Everything below is protected work, so it runs on the lease-bound
	// context: losing renewal must abort the terminal commit, not merely be
	// noticed afterwards.
	ctx = guard.Context()

	record, found, err := o.repository.Load(ctx, request.PlanID)
	if err != nil {
		return result, err
	}
	if !found {
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict}, ErrPlanNotFound
	}
	if record.Plan.Status == StatusFailed && record.Plan.BlockedTermination != nil {
		if replayErr := validateTerminalReplay(record.Plan, request); replayErr == nil {
			return BlockedTerminationResult{Status: BlockedTerminationAlreadyCommitted, Record: record}, nil
		} else if !errors.Is(replayErr, ErrRevisionConflict) {
			return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, replayErr
		}
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict, Record: record}, nil
	}
	if record.Plan.Status != StatusBlocked {
		return BlockedTerminationResult{Status: BlockedTerminationNotBlocked, Record: record}, nil
	}
	checkpoint, err := BlockedCheckpointDigest(record.Plan)
	if err != nil {
		return BlockedTerminationResult{Record: record}, err
	}
	if record.Plan.Revision != request.ExpectedRevision || record.Plan.BlockedReason != request.ExpectedBlockedReason ||
		record.Plan.PayloadDigest != request.ExpectedPayloadDigest || checkpoint != request.ExpectedCheckpointDigest {
		return BlockedTerminationResult{Status: BlockedTerminationExpectationMismatch, Record: record}, nil
	}

	manifest, fence, err := o.quiescence.AcquireBlockedQuiescence(ctx, record.Plan.Clone())
	if fence != nil {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if releaseErr := fence.Release(cleanupCtx); releaseErr != nil && err == nil {
				err = releaseErr
			}
		}()
	}
	if err != nil || fence == nil {
		if err == nil {
			err = ErrVerification
		}
		return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, err
	}
	proof, err := BuildBlockedQuiescenceProof(record.Plan, manifest)
	if err != nil || proof != fence.Proof() {
		if err == nil {
			err = ErrVerification
		}
		return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, err
	}
	nowMs := o.now().UnixMilli()
	if err = o.validateProof(record.Plan, request, proof, nowMs); err != nil {
		status := BlockedTerminationUnsafe
		if proof.ExpiresAtMs > 0 && nowMs >= proof.ExpiresAtMs {
			status = BlockedTerminationProofExpired
		}
		return BlockedTerminationResult{Status: status, Record: record}, nil
	}
	if record.Plan.ExclusionIntent != nil && blockedScaleNotCommittedEvidence(record.Plan, manifest.Workload.State) {
		releasePort, ok := o.quiescence.(BlockedExclusionReleasePort)
		if !ok {
			return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, ErrVerification
		}
		finalManifest, releaseErr := releasePort.ReleaseExclusionForBlockedTermination(ctx, record.Plan.Clone(), fence)
		if releaseErr != nil {
			return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, releaseErr
		}
		if finalManifest.Exclusion.Present || finalManifest.Exclusion.State.Excluded ||
			finalManifest.Exclusion.State.OwnerPlanID != "" ||
			!finalManifest.Exclusion.ReleaseOutcome.TerminalForScalePlan() {
			return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, ErrVerification
		}
		proof, err = BuildBlockedQuiescenceProof(record.Plan, finalManifest)
		if err != nil || proof != fence.Proof() {
			if err == nil {
				err = ErrVerification
			}
			return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, err
		}
		nowMs = o.now().UnixMilli()
		if err = o.validateProof(record.Plan, request, proof, nowMs); err != nil {
			status := BlockedTerminationUnsafe
			if proof.ExpiresAtMs > 0 && nowMs >= proof.ExpiresAtMs {
				status = BlockedTerminationProofExpired
			}
			return BlockedTerminationResult{Status: status, Record: record}, nil
		}
	} else if record.Plan.ExclusionIntent != nil && !blockedScaleCommittedEvidence(record.Plan, manifest.Workload.State) {
		return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, ErrVerification
	}
	if err = fence.Revalidate(ctx); err != nil {
		return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: record}, err
	}
	result, err = o.repository.commitBlockedTermination(ctx, request, proof)
	return result, err
}

func validateTerminalReplay(plan Plan, request BlockedTerminationRequest) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	receipt := plan.BlockedTermination
	if receipt == nil || receipt.TerminationID != request.TerminationID || receipt.ExpectedBlockedRevision != request.ExpectedRevision ||
		receipt.ExpectedBlockedReason != request.ExpectedBlockedReason || receipt.ExpectedPayloadDigest != request.ExpectedPayloadDigest ||
		receipt.ExpectedCheckpointDigest != request.ExpectedCheckpointDigest {
		return ErrRevisionConflict
	}
	return nil
}

func validateBlockedTerminationRequest(request BlockedTerminationRequest) error {
	if err := validateOpaque("plan id", request.PlanID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if request.ExpectedRevision == 0 || request.ExpectedBlockedReason == "" || !canonicalSHA256(request.ExpectedPayloadDigest) ||
		!canonicalSHA256(request.ExpectedCheckpointDigest) {
		return fmt.Errorf("%w: blocked termination expectation", ErrInvalidPlan)
	}
	return validateOpaque("termination id", request.TerminationID, MaxOpaqueIDBytes)
}

func (o *BlockedTerminationOwner) validateProof(plan Plan, request BlockedTerminationRequest, proof BlockedQuiescenceProof, nowMs int64) error {
	if proof.ProtocolVersion != BlockedTerminationProtocolVersion || proof.PlanID != request.PlanID ||
		proof.ExpectedRevision != request.ExpectedRevision || proof.ExpectedBlockedReason != request.ExpectedBlockedReason ||
		proof.ExpectedPayloadDigest != request.ExpectedPayloadDigest || proof.ExpectedCheckpointDigest != request.ExpectedCheckpointDigest ||
		proof.IntentCount != blockedTerminationIntentCount(plan) || proof.ChildCount != uint32(len(plan.ResolvedMoves)) ||
		proof.CapturedAtMs < plan.UpdatedAtMs || proof.CapturedAtMs > nowMs || nowMs >= proof.ExpiresAtMs ||
		proof.ExpiresAtMs-proof.CapturedAtMs > o.maxProofAge.Milliseconds() ||
		proof.EvidenceRetainUntilMs < plan.RecoveryRetainUntilMs || proof.EvidenceRetainUntilMs < proof.ExpiresAtMs {
		return ErrVerification
	}
	_, err := BlockedQuiescenceProofDigest(proof)
	return err
}
