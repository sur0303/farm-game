package carerecovery

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workidentity"
)

func (d *DirectDispatcher) dispatchPlant(ctx, itemCtx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot) error {
	return d.dispatchFarmDest(ctx, itemCtx, address, string(identity.WorkTypePlant), workidentity.RouteDomainPlayer, committed,
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCommitPlantPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.PlantPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.PlantInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			return decoded.GetOwnerPlayerId(), decoded.GetFarmId(), invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLANT,
		string(identity.StepCommitPlant),
		typedProof("PLAYER_ACTOR", "PLAYER", string(identity.StepCommitPlant)),
	)
}

func (d *DirectDispatcher) dispatchUnlock(ctx, itemCtx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot) error {
	return d.dispatchFarmDest(ctx, itemCtx, address, string(identity.WorkTypeUnlockPlot), workidentity.RouteDomainPlayer, committed,
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCommitPlotUnlockPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.UnlockPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.UnlockInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			return decoded.GetOwnerPlayerId(), decoded.GetFarmId(), invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLOT_UNLOCK,
		string(identity.StepCommitPlotUnlock),
		typedProof("PLAYER_ACTOR", "PLAYER", string(identity.StepCommitPlotUnlock)),
	)
}

func (d *DirectDispatcher) dispatchTheftVisitor(ctx, itemCtx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot) error {
	return d.dispatchPlayerDest(ctx, itemCtx, address, string(identity.WorkTypeTheftVisitor), committed,
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeSettleTheftVisitorPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.TheftVisitorPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.TheftVisitorInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			farmID := uint64(0)
			if decoded.GetTheftTargetDecision() != nil {
				farmID = decoded.GetTheftTargetDecision().GetFarmId()
			}
			return decoded.GetVisitorPlayerId(), farmID, invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantStep_PARTICIPANT_STEP_SETTLE_THEFT_VISITOR,
		typedProof("FARM_ACTOR", "FARM", string(identity.StepSettleTheftVisitor)),
	)
}

func (d *DirectDispatcher) dispatchTheftGuard(ctx, itemCtx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot) error {
	return d.dispatchPlayerDest(ctx, itemCtx, address, string(identity.WorkTypeTheftGuard), committed,
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCreditGuardIncomePayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.TheftGuardPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.TheftGuardInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			farmID := uint64(0)
			if decoded.GetTheftTargetDecision() != nil {
				farmID = decoded.GetTheftTargetDecision().GetFarmId()
			}
			return decoded.GetOwnerPlayerId(), farmID, invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME,
		contractspb.ParticipantStep_PARTICIPANT_STEP_CREDIT_GUARD_INCOME,
		typedProof("FARM_ACTOR", "FARM", string(identity.StepCreditGuardIncome)),
	)
}

type migratedIdentityFn func(workID string, payload []byte) (subjectPlayerID, destActorID uint64, invocation string, payloadHash []byte, err error)
type migratedProofFn func(claim authority.WorkClaim, response *contractspb.ParticipantResponse, layout, canonicalID string) (authority.WorkCompletionProof, error)

func typedProof(sourceKind, actorDomain, step string) migratedProofFn {
	return func(claim authority.WorkClaim, response *contractspb.ParticipantResponse, layout, canonicalID string) (authority.WorkCompletionProof, error) {
		return buildTypedCompletionProof(claim, response, layout, sourceKind, actorDomain, step, canonicalID)
	}
}

func (d *DirectDispatcher) dispatchFarmDest(ctx, itemCtx context.Context, address workidentity.WorkAddress, family string, sourceDomain workidentity.RouteDomain, committed *authority.SourceOrderSnapshot, identityFn migratedIdentityFn, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, stepName string, proofFn migratedProofFn) error {
	d.dispatched.Add(1)
	if d.config.Farm == nil || d.config.FarmRoute == nil {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: Farm Coordinator dest is required", ErrInvalidConfig)
	}
	bucket, err := d.resolveBucket(address)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	var order authority.SourceOrderSnapshot
	if committed != nil {
		order = *committed
	} else {
		order, err = d.config.SourceOrders.GetSourceOrder(itemCtx, bucket, family, address.WorkID)
		if err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if order.State == "COMPLETED" {
		d.completed.Add(1)
		return nil
	}
	if order.State != "PENDING" || order.WorkType != family || order.WorkID != address.WorkID ||
		order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || order.SubjectPlayerID == 0 ||
		order.OperationID == "" || order.WorkVersion == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: %s source order identity", ErrPayloadInvalid, family)
	}
	subjectID, destActorID, invocation, payloadHash, err := identityFn(order.WorkID, order.TypedPayload)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if order.PayloadHash != hex.EncodeToString(payloadHash) || subjectID != order.SubjectPlayerID || destActorID == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: %s payload binding", ErrPayloadInvalid, family)
	}
	address, err = sourceorder.WorkAddress(bucket, family, order.WorkID, sourceDomain, subjectID)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	route, err := d.config.FarmRoute(itemCtx, destActorID)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	request, err := farmCoordinatorRequest(route, address, order, invocation, command, step, destActorID, d.config.ParticipantBudgetMs)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		if err := attachBestEffortSourceOrder(request, address, order); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	response, rpcErr := d.config.Farm.ExecuteStep(itemCtx, request)
	if rpcErr != nil {
		d.rescheduled.Add(1)
		return rpcErr
	}
	if err := validateTypedTerminalProof(response, command, invocation); err != nil {
		d.rescheduled.Add(1)
		return err
	}
	return d.completeDirect(ctx, bucket, family, order, proofFn, response)
}

func (d *DirectDispatcher) dispatchPlayerDest(ctx, itemCtx context.Context, address workidentity.WorkAddress, family string, committed *authority.SourceOrderSnapshot, identityFn migratedIdentityFn, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, proofFn migratedProofFn) error {
	d.dispatched.Add(1)
	bucket, err := d.resolveBucket(address)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	var order authority.SourceOrderSnapshot
	if committed != nil {
		order = *committed
	} else {
		order, err = d.config.SourceOrders.GetSourceOrder(itemCtx, bucket, family, address.WorkID)
		if err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if order.State == "COMPLETED" {
		d.completed.Add(1)
		return nil
	}
	if order.State != "PENDING" || order.WorkType != family || order.WorkID != address.WorkID ||
		order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || order.SubjectPlayerID == 0 ||
		order.OperationID == "" || order.WorkVersion == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: %s source order identity", ErrPayloadInvalid, family)
	}
	subjectID, sourceFarmID, invocation, payloadHash, err := identityFn(order.WorkID, order.TypedPayload)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if order.PayloadHash != hex.EncodeToString(payloadHash) || subjectID != order.SubjectPlayerID || sourceFarmID == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: %s payload binding", ErrPayloadInvalid, family)
	}
	address, err = sourceorder.WorkAddress(bucket, family, order.WorkID, workidentity.RouteDomainFarm, sourceFarmID)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	route, err := d.config.Route(itemCtx, subjectID)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	request, err := playerCoordinatorRequest(route, address, order, invocation, command, step, d.config.ParticipantBudgetMs)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		if err := attachBestEffortSourceOrder(request, address, order); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	response, rpcErr := d.config.Player.ExecuteStep(itemCtx, request)
	if rpcErr != nil {
		d.rescheduled.Add(1)
		return rpcErr
	}
	if err := validateTypedTerminalProof(response, command, invocation); err != nil {
		d.rescheduled.Add(1)
		return err
	}
	return d.completeDirect(ctx, bucket, family, order, proofFn, response)
}

func (d *DirectDispatcher) completeDirect(ctx context.Context, bucket rediskey.AuthorityBucketRef, family string, order authority.SourceOrderSnapshot, proofFn migratedProofFn, response *contractspb.ParticipantResponse) error {
	if d.config.BestEffortNoSourcePersistence {
		// validateTypedTerminalProof already established the destination ACK.
		// A completion proof would falsely imply that a durable source record
		// exists, so dirty mode stops before constructing one.
		d.completed.Add(1)
		return nil
	}
	claim := authority.WorkClaim{
		WorkID: order.WorkID, WorkType: order.WorkType, PayloadHash: order.PayloadHash,
		SubjectPlayerID: order.SubjectPlayerID, WorkVersion: order.WorkVersion, Payload: order.TypedPayload,
	}
	completionProof, proofErr := proofFn(claim, response, order.AuthorityLayoutVersion, order.CanonicalSourceAuthorityID)
	if proofErr != nil {
		d.rescheduled.Add(1)
		return proofErr
	}
	return d.finishDirect(ctx, bucket, family, order.WorkID, completionProof)
}

func (d *DirectDispatcher) finishDirect(ctx context.Context, bucket rediskey.AuthorityBucketRef, family, workID string, proof authority.WorkCompletionProof) error {
	completionCtx, completionCancel := context.WithTimeout(ctx, minDuration(d.config.ItemDeadline, time.Second))
	defer completionCancel()
	complete, completeErr := d.completeDirectProof(completionCtx, authority.WorkClaimAndCompleteRequest{
		Bucket: bucket, WorkType: family, WorkID: workID,
		HolderID: d.config.Owner, BootID: d.config.BootID, LeaseToken: d.config.Token,
		RequestedTTLms: d.config.LeaseTTL.Milliseconds(), Proof: proof,
	})
	if completeErr != nil {
		d.rescheduled.Add(1)
		return completeErr
	}
	if complete.Status != authority.WorkLeaseCompleted && complete.Status != authority.WorkLeaseAlreadyCompleted {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: complete status=%s", ErrDirectNotClaimed, complete.Status)
	}
	d.completed.Add(1)
	return nil
}

func (w *Worker) processPlantClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult) itemResult {
	return w.processFarmDestClaim(ctx, bucket, claim, claimStarted, out, string(identity.WorkTypePlant),
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCommitPlantPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.PlantPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.PlantInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			return decoded.GetOwnerPlayerId(), decoded.GetFarmId(), invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLANT,
		typedProof("PLAYER_ACTOR", "PLAYER", string(identity.StepCommitPlant)),
	)
}

func (w *Worker) processUnlockClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult) itemResult {
	return w.processFarmDestClaim(ctx, bucket, claim, claimStarted, out, string(identity.WorkTypeUnlockPlot),
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCommitPlotUnlockPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.UnlockPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.UnlockInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			return decoded.GetOwnerPlayerId(), decoded.GetFarmId(), invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLOT_UNLOCK,
		typedProof("PLAYER_ACTOR", "PLAYER", string(identity.StepCommitPlotUnlock)),
	)
}

func (w *Worker) processTheftVisitorClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult) itemResult {
	return w.processPlayerDestClaim(ctx, bucket, claim, claimStarted, out, string(identity.WorkTypeTheftVisitor),
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeSettleTheftVisitorPayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.TheftVisitorPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.TheftVisitorInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			farmID := uint64(0)
			if decoded.GetTheftTargetDecision() != nil {
				farmID = decoded.GetTheftTargetDecision().GetFarmId()
			}
			return decoded.GetVisitorPlayerId(), farmID, invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantStep_PARTICIPANT_STEP_SETTLE_THEFT_VISITOR,
		typedProof("FARM_ACTOR", "FARM", string(identity.StepSettleTheftVisitor)),
	)
}

func (w *Worker) processTheftGuardClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult) itemResult {
	return w.processPlayerDestClaim(ctx, bucket, claim, claimStarted, out, string(identity.WorkTypeTheftGuard),
		func(workID string, payload []byte) (uint64, uint64, string, []byte, error) {
			decoded, err := sourceorder.DecodeCreditGuardIncomePayload(workID, payload)
			if err != nil {
				return 0, 0, "", nil, err
			}
			hash, err := sourceorder.TheftGuardPayloadHash(decoded)
			if err != nil {
				return 0, 0, "", nil, err
			}
			invocation, err := sourceorder.TheftGuardInvocation(workID)
			if err != nil {
				return 0, 0, "", nil, err
			}
			farmID := uint64(0)
			if decoded.GetTheftTargetDecision() != nil {
				farmID = decoded.GetTheftTargetDecision().GetFarmId()
			}
			return decoded.GetOwnerPlayerId(), farmID, invocation, hash[:], nil
		},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME,
		contractspb.ParticipantStep_PARTICIPANT_STEP_CREDIT_GUARD_INCOME,
		typedProof("FARM_ACTOR", "FARM", string(identity.StepCreditGuardIncome)),
	)
}

func (w *Worker) processFarmDestClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult, family string, identityFn migratedIdentityFn, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, proofFn migratedProofFn) itemResult {
	if w.config.Farm == nil || w.config.FarmRoute == nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, fmt.Errorf("%w: Farm Recovery dest is required", ErrInvalidConfig))
		return out
	}
	subjectID, destActorID, invocation, payloadHash, err := identityFn(claim.WorkID, claim.Payload)
	if err == nil && (claim.PayloadHash != hex.EncodeToString(payloadHash) || claim.SubjectPlayerID != subjectID) {
		err = fmt.Errorf("%w: %s payload binding", ErrPayloadInvalid, family)
	}
	if err == nil {
		err = w.validateSourcePlayerBucket(bucket, subjectID)
	}
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		out.Rescheduled = 1
		out.Err = err
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	address, err := sourceorder.WorkAddress(bucket, family, claim.WorkID, workidentity.RouteDomainPlayer, subjectID)
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	opCtx, cancel := boundedOperationContext(ctx, w.config.OperationTimeout, window.safeDeadline, w.config.Now)
	defer cancel()
	route, err := w.config.FarmRoute(opCtx, destActorID)
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	request, err := farmRecoveryRequest(route, address, claim, invocation, command, step, destActorID, w.config.Owner, w.config.BootID, w.config.Token, w.config.ParticipantBudgetMs, bucket.LayoutVersion())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	return w.finishDestClaim(ctx, bucket, claim, out, family, command, invocation, request, w.config.Farm, proofFn, bucket.LayoutVersion(), strconv.FormatUint(subjectID, 10))
}

func (w *Worker) processPlayerDestClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult, family string, identityFn migratedIdentityFn, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, proofFn migratedProofFn) itemResult {
	if w.config.Player == nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, fmt.Errorf("%w: PLAYER Recovery dest is required", ErrInvalidConfig))
		return out
	}
	subjectID, sourceFarmID, invocation, payloadHash, err := identityFn(claim.WorkID, claim.Payload)
	if err == nil && (claim.PayloadHash != hex.EncodeToString(payloadHash) || claim.SubjectPlayerID != subjectID) {
		err = fmt.Errorf("%w: %s payload binding", ErrPayloadInvalid, family)
	}
	if err == nil {
		err = w.validateSourceBucket(bucket, sourceFarmID)
	}
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		out.Rescheduled = 1
		out.Err = err
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	address, err := sourceorder.WorkAddress(bucket, family, claim.WorkID, workidentity.RouteDomainFarm, sourceFarmID)
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	opCtx, cancel := boundedOperationContext(ctx, w.config.OperationTimeout, window.safeDeadline, w.config.Now)
	defer cancel()
	route, err := w.config.Route(opCtx, subjectID)
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	request, err := playerRecoveryRequest(route, address, claim, invocation, command, step, w.config.Owner, w.config.BootID, w.config.Token, w.config.ParticipantBudgetMs, bucket.LayoutVersion())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	return w.finishDestClaim(ctx, bucket, claim, out, family, command, invocation, request, w.config.Player, proofFn, bucket.LayoutVersion(), strconv.FormatUint(sourceFarmID, 10))
}

func (w *Worker) finishDestClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, out itemResult, family string, command contractspb.ParticipantCommandType, invocation string, request *contractspb.ParticipantRequest, dest WorkerRecoveryParticipantClient, proofFn migratedProofFn, layout, canonicalID string) itemResult {
	opCtx, cancel := context.WithTimeout(ctx, w.config.OperationTimeout)
	defer cancel()
	response, rpcErr := dest.ResumeStep(opCtx, request)
	if rpcErr != nil {
		out.Unknown = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, rpcErr)
		return out
	}
	if response != nil && response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		out.Unknown = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, errors.New("Participant TIMEOUT_UNKNOWN; receipt-first retry required"))
		return out
	}
	if err := validateTypedTerminalProof(response, command, invocation); err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	completionProof, proofErr := proofFn(claim, response, layout, canonicalID)
	if proofErr != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, proofErr)
		return out
	}
	completeCtx, completeCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, family, claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, completionProof)
	completeCancel()
	if completeErr != nil {
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %w", claim.WorkID, completeErr)
		return out
	}
	switch complete.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		out.Completed = 1
	default:
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %s", claim.WorkID, complete.Status)
	}
	return out
}

func farmCoordinatorRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, order authority.SourceOrderSnapshot, invocation string, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, destActorID uint64, budget uint32) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM || route.GetActorId() != destActorID {
		return nil, fmt.Errorf("%w: Coordinator FARM route", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, order.AuthorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{RouteHeader: route, ParticipantCommand: command,
		TypedPayload: append([]byte(nil), order.TypedPayload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: wire, ExpectedWorkVersion: order.WorkVersion, ParticipantStep: step,
			ParticipantInvocationId: invocation, OperationId: order.OperationID, SubjectPlayerId: order.SubjectPlayerID, RemainingBudgetMs: budget,
		}},
	}, nil
}

func playerCoordinatorRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, order authority.SourceOrderSnapshot, invocation string, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, budget uint32) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != order.SubjectPlayerID {
		return nil, fmt.Errorf("%w: Coordinator PLAYER route", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, order.AuthorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{RouteHeader: route, ParticipantCommand: command,
		TypedPayload: append([]byte(nil), order.TypedPayload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: wire, ExpectedWorkVersion: order.WorkVersion, ParticipantStep: step,
			ParticipantInvocationId: invocation, OperationId: order.OperationID, SubjectPlayerId: order.SubjectPlayerID, RemainingBudgetMs: budget,
		}},
	}, nil
}

func farmRecoveryRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, claim authority.WorkClaim, invocation string, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, destActorID uint64, callerID, bootID, leaseToken string, budget uint32, authorityLayoutVersion string) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM || route.GetActorId() != destActorID {
		return nil, fmt.Errorf("%w: FARM route does not match dest", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, authorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: route, ParticipantCommand: command,
		TypedPayload: append([]byte(nil), claim.Payload...), CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: callerID, CallerBootId: bootID, Work: wire, ExpectedWorkVersion: claim.WorkVersion,
			ParticipantStep: step, ParticipantInvocationId: invocation, LeaseOwnerToken: leaseToken, LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: budget,
		}},
	}, nil
}

func playerRecoveryRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, claim authority.WorkClaim, invocation string, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, callerID, bootID, leaseToken string, budget uint32, authorityLayoutVersion string) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != claim.SubjectPlayerID {
		return nil, fmt.Errorf("%w: PLAYER route does not match dest", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, authorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: route, ParticipantCommand: command,
		TypedPayload: append([]byte(nil), claim.Payload...), CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: callerID, CallerBootId: bootID, Work: wire, ExpectedWorkVersion: claim.WorkVersion,
			ParticipantStep: step, ParticipantInvocationId: invocation, LeaseOwnerToken: leaseToken, LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: budget,
		}},
	}, nil
}

func validateTypedTerminalProof(response *contractspb.ParticipantResponse, command contractspb.ParticipantCommandType, invocation string) error {
	if response == nil || response.GetParticipantCommand() != command {
		return fmt.Errorf("%w: dest command", ErrTerminalProof)
	}
	switch response.GetOutcome() {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
	default:
		return fmt.Errorf("%w: dest outcome", ErrTerminalProof)
	}
	if response.GetParticipantInvocationId() != invocation || len(response.GetParticipantResultHash()) != 32 || response.GetCommittedRecordVersion() == 0 {
		return fmt.Errorf("%w: dest proof fields", ErrTerminalProof)
	}
	return nil
}

func buildTypedCompletionProof(claim authority.WorkClaim, response *contractspb.ParticipantResponse, authorityLayoutVersion, sourceKind, actorDomain, step, canonicalID string) (authority.WorkCompletionProof, error) {
	outcome := "APPLIED"
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		outcome = "NO_CHANGE"
	}
	if canonicalID == "" {
		canonicalID = strconv.FormatUint(claim.SubjectPlayerID, 10)
	}
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        sourceKind,
		CanonicalSourceAuthorityID: canonicalID,
		AuthorityLayoutVersion:     authorityLayoutVersion,
		ActorRouteDomain:           actorDomain,
		PayloadHash:                claim.PayloadHash,
		InvocationID:               response.GetParticipantInvocationId(),
		ParticipantStep:            step,
		ParticipantOutcome:         outcome,
		ParticipantResultHash:      hex.EncodeToString(response.GetParticipantResultHash()),
		ParticipantResultVersion:   response.GetCommittedRecordVersion(),
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}
