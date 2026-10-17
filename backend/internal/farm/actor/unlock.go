package actor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const unlockParticipantProofMagic = "U1RP"

// CommitPlotUnlockTurnResult carries the one Farm authority commit and the
// exact change candidate produced from that commit. The observer is invoked
// only after the actor mailbox returns, matching Plant's delivery boundary.
type CommitPlotUnlockTurnResult struct {
	Outcome *contracts.StepOutcome
	Action  *ActionResult

	deliveryErr error
}

// ValidateCommitPlotUnlockPayload is the Farm-side immutable binding check.
// Price, level and order are frozen in the workflow root; this validator
// checks their P0 rule projection again so a forged participant payload cannot
// turn the FarmActor into a client-controlled pricing path.
func ValidateCommitPlotUnlockPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK {
		return status.Error(codes.Unimplemented, "farm participant command is not CommitPlotUnlock")
	}
	payload, ok := call.Payload.(*contractspb.CommitPlotUnlockPayload)
	if !ok || payload == nil {
		return status.Error(codes.Internal, "farm participant CommitPlotUnlock payload schema mismatch")
	}
	rule := domain.DefaultPlotUnlockRule()
	entry, entryOK := rule.EntryOf(payload.GetPlotId())
	ruleHash := domain.PlotUnlockRuleSnapshotHash(rule)
	if call.RouteHeader.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		payload.GetFarmId() == 0 || payload.GetFarmId() != call.RouteHeader.GetActorId() ||
		payload.GetOwnerPlayerId() == 0 || payload.GetOwnerPlayerId() != payload.GetFarmId() ||
		payload.GetPlotId() < domain.P0InitialUnlockedPlots+1 || payload.GetPlotId() > domain.P0MaxPlots ||
		payload.GetExpectedPlotVersion() == 0 || payload.GetExpectedUnlockRuleVersion() == 0 ||
		!entryOK || payload.GetExpectedUnlockRuleVersion() != rule.RuleVersion ||
		payload.GetUnlockOrder() != entry.UnlockOrder || payload.GetRequiredPlayerLevel() != entry.RequiredLevel ||
		payload.GetAmountNormalCoin() != entry.NormalCoinPrice ||
		len(payload.GetReserveUnlockResultHash()) != sha256.Size || len(payload.GetUnlockRuleSnapshotHash()) != sha256.Size ||
		!bytes.Equal(payload.GetUnlockRuleSnapshotHash(), ruleHash[:]) {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock target/rule binding is invalid")
	}
	if payload.GetPlotId() == domain.P0InitialUnlockedPlots+1 {
		if payload.PredecessorPlotId != nil {
			return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock predecessor must be absent for plot 6")
		}
	} else if payload.GetPredecessorPlotId() != payload.GetPlotId()-1 {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock predecessor binding is invalid")
	}
	if call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetOwnerPlayerId() {
		return status.Error(codes.FailedPrecondition, "farm participant CommitPlotUnlock owner binding is invalid")
	}
	if call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT || call.Work.GetWorkId() == "" {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock Work binding is invalid")
	}
	wantReservation, err := identity.UnlockCoinReservationID(call.Work.GetWorkId())
	if err != nil || payload.GetReservationId() != wantReservation {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock reservation binding is invalid")
	}
	return nil
}

func (a *FarmActor) CommitPlotUnlock(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*contracts.StepOutcome, error) {
	turn, err := a.commitPlotUnlockWithAction(ctx, call, guard, nowMs)
	if err != nil || turn == nil || turn.Outcome == nil {
		if turn == nil {
			return nil, err
		}
		return turn.Outcome, err
	}
	if turn.deliveryErr != nil {
		return nil, turn.deliveryErr
	}
	return turn.Outcome, nil
}

func (a *FarmActor) CommitPlotUnlockWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitPlotUnlockTurnResult, error) {
	return a.commitPlotUnlockWithAction(ctx, call, guard, nowMs)
}

func (a *FarmActor) commitPlotUnlockWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitPlotUnlockTurnResult, error) {
	if a == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant CommitPlotUnlock actor call is incomplete")
	}
	if replay, found, err := ResolveCommitPlotUnlockParticipantReceipt(a.record, a.recordVersion, call); err != nil {
		return nil, err
	} else if found {
		return &CommitPlotUnlockTurnResult{Outcome: replay}, nil
	}
	if err := ValidateCommitPlotUnlockPayload(call); err != nil {
		return nil, err
	}
	trustedNowMs := time.Now().UnixMilli()
	if nowMs != nil {
		trustedNowMs = nowMs()
	}
	if trustedNowMs <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "farm participant trusted clock is unavailable")
	}
	if participantReceiptRetentionFull(a.record, trustedNowMs) {
		return nil, farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm participant receipt retention window is full")
	}
	payload := call.Payload.(*contractspb.CommitPlotUnlockPayload)
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	state := a.buildParticipantState()
	rule := domain.DefaultPlotUnlockRule()
	if err := domain.ValidatePlotUnlockRule(rule); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "farm participant unlock rule is invalid")
	}
	decisionState, decision, domainErr := domain.DecidePlotUnlock(state, domain.UnlockPlotCommand{
		PlotID: payload.GetPlotId(), ExpectedPlotVersion: payload.GetExpectedPlotVersion(),
		ExpectedUnlockRuleVersion: payload.GetExpectedUnlockRuleVersion(), UnlockWorkflowID: call.Work.GetWorkId(),
	}, rule, trustedNowMs)

	outcome := contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	errorCode := commonpb.ErrorCode_OK
	needsChange := domainErr == nil
	if domainErr != nil {
		switch domainErr.Status {
		case domain.StatusNoChange:
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
		default:
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
		}
		errorCode = mapUnlockDomainError(domainErr)
	}
	plotVersion := currentParticipantPlotVersion(&state, payload.GetPlotId())
	farmVersion := state.FarmVersion
	if needsChange {
		plotVersion = decision.CommittedPlotVersion
		farmVersion = decision.CommittedFarmVersion
	}
	header := state.Header
	if needsChange {
		header = decisionState.State.Header
	}
	result := &contractspb.CommitPlotUnlockResult{
		FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), AppliedUnlockRuleVersion: rule.RuleVersion,
		CommittedPlotVersion: plotVersion, CommittedFarmVersion: farmVersion,
		OpportunitySourceFarmVersion: header.SourceFarmVersion,
		OpportunityDirtyGeneration:   header.DirtyGeneration,
		OpportunityNextAtMs:          header.NextOpportunityAtMs,
		OpportunityCandidateMask:     uint32(header.CandidateMask),
	}
	decisionHash, err := unlockDecisionHash(result)
	if err != nil {
		return nil, err
	}
	result.UnlockDecisionHash = decisionHash[:]
	resultHash, err := contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	committedRecordVersion := a.recordVersion + 1
	proof, err := encodeUnlockParticipantProof(call, outcome, errorCode, result, resultHash[:], committedRecordVersion)
	if err != nil {
		return nil, err
	}
	if err := appendParticipantReceipt(candidateRecord, call.InvocationID, call.CanonicalStepPayloadHash[:], proof, outcome, errorCode, trustedNowMs); err != nil {
		return nil, err
	}

	var committedChange *farmpb.FarmChange
	if needsChange {
		publicPlot := participantPublicPlot(candidateRecord, payload.GetPlotId())
		if publicPlot == nil {
			return nil, status.Error(codes.Internal, "farm participant CommitPlotUnlock plot projection is missing")
		}
		publicPlot.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
		publicPlot.UnlockWorkflowId = call.Work.GetWorkId()
		publicPlot.UnlockRuleVersion = rule.RuleVersion
		publicPlot.UnlockedAtMs = trustedNowMs
		publicPlot.PlotVersion = decision.CommittedPlotVersion
		if candidateRecord.Core == nil {
			candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
		}
		candidateRecord.Core.FarmVersion = decision.CommittedFarmVersion
		// Unlock changes two public eligibility facts atomically: the purchased
		// plot becomes UNLOCKED and, unless this was the final plot, the next
		// locked plot becomes the sole unlock candidate. A partial FarmChange
		// must carry both complete PlotViews; otherwise clients retain the old
		// candidate=false value until a Fresh Join snapshot.
		changedPlots := []*farmpb.PlotView{plotView(candidateRecord, publicPlot, time.UnixMilli(trustedNowMs))}
		if nextOrder := nextLockedPlotOrder(candidateRecord); nextOrder != 0 {
			if nextPlot := findRecordPlot(candidateRecord, int32(nextOrder-1)); nextPlot != nil {
				changedPlots = append(changedPlots, plotView(candidateRecord, nextPlot, time.UnixMilli(trustedNowMs)))
			}
		}
		committedChange = &farmpb.FarmChange{
			OwnerId: a.farmID, FarmVersion: decision.CommittedFarmVersion, ChangedPlots: changedPlots,
		}
	}
	a.record = candidateRecord
	if err := guard.CheckModelRecord(candidateRecord); err != nil {
		a.record = before
		return nil, err
	}
	if needsChange {
		if err := a.saveWithCandidate(ctx, before, call.InvocationID, commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED, committedChange); err != nil {
			return nil, err
		}
		a.participantState = &participantFarmState{state: decisionState.State}
		change, recipients, resyncs, deliveryErr := a.publishCandidate(participantPublicPlot(candidateRecord, payload.GetPlotId()), time.UnixMilli(trustedNowMs))
		if change == nil {
			change = committedChange
		}
		return &CommitPlotUnlockTurnResult{
			Outcome:     &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result, CommittedRecordVersion: committedRecordVersion, Failure: participantFailure(errorCode)},
			Action:      &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(trustedNowMs))}, Change: change, Recipients: recipients, ActivationEpoch: a.activationEpoch, Resyncs: resyncs},
			deliveryErr: deliveryErr,
		}, nil
	}
	// Receipt-only rejected/no-change outcomes are durable but do not advance
	// FarmVersion and never publish a FarmChange.
	if err := a.save(ctx, before, call.InvocationID, commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED); err != nil {
		return nil, err
	}
	a.guardClear()
	a.participantState = &participantFarmState{state: state}
	return &CommitPlotUnlockTurnResult{Outcome: &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result, CommittedRecordVersion: committedRecordVersion, Failure: participantFailure(errorCode)}}, nil
}

func mapUnlockDomainError(err *domain.Error) commonpb.ErrorCode {
	if err == nil {
		return commonpb.ErrorCode_OK
	}
	switch err.Code {
	case domain.CodePreviousPlotLocked:
		return commonpb.ErrorCode_ERR_PREVIOUS_PLOT_LOCKED
	case domain.CodeRuleVersionChanged:
		return commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH
	case domain.CodeStalePlotVersion:
		return commonpb.ErrorCode_ERR_VERSION_CONFLICT
	case domain.CodeAlreadyUnlocked:
		return commonpb.ErrorCode_OK
	case domain.CodePlotLocked:
		return commonpb.ErrorCode_ERR_PLOT_LOCKED
	case domain.CodePlotNotFound:
		return commonpb.ErrorCode_ERR_PLOT_NOT_FOUND
	default:
		return commonpb.ErrorCode_ERR_INTERNAL
	}
}

func unlockDecisionHash(result *contractspb.CommitPlotUnlockResult) ([32]byte, error) {
	if result == nil {
		return [32]byte{}, errors.New("farm participant CommitPlotUnlock result is missing")
	}
	withoutHash := proto.Clone(result).(*contractspb.CommitPlotUnlockResult)
	withoutHash.UnlockDecisionHash = nil
	return contracts.CanonicalHash(withoutHash)
}

type unlockParticipantProof struct {
	outcome       contractspb.ParticipantOutcome
	errorCode     commonpb.ErrorCode
	command       contractspb.ParticipantCommandType
	invocationID  string
	stepHash      []byte
	resultHash    []byte
	recordVersion uint64
	result        *contractspb.CommitPlotUnlockResult
}

func encodeUnlockParticipantProof(call *contracts.ParticipantCall, outcome contractspb.ParticipantOutcome, code commonpb.ErrorCode, result *contractspb.CommitPlotUnlockResult, resultHash []byte, recordVersion uint64) ([]byte, error) {
	if call == nil || result == nil || len(resultHash) != sha256.Size || recordVersion == 0 {
		return nil, errors.New("farm participant CommitPlotUnlock result proof input is incomplete")
	}
	typedResult, canonicalHash, err := contracts.ValidateTypedResult(call.Spec, result)
	if err != nil || !bytes.Equal(canonicalHash, resultHash) {
		return nil, errors.New("farm participant CommitPlotUnlock result proof hash is inconsistent")
	}
	buf := make([]byte, 0, len(typedResult)+256)
	writeParticipantLPBytes(&buf, []byte(unlockParticipantProofMagic))
	putParticipantUint32(&buf, participantProofVersion)
	putParticipantUint32(&buf, uint32(call.Spec.Command))
	writeParticipantLPBytes(&buf, []byte(call.InvocationID))
	writeParticipantLPBytes(&buf, call.CanonicalStepPayloadHash[:])
	writeParticipantLPBytes(&buf, resultHash)
	putParticipantUint64(&buf, recordVersion)
	putParticipantUint32(&buf, uint32(code))
	putParticipantUint32(&buf, uint32(outcome))
	writeParticipantLPBytes(&buf, typedResult)
	if len(buf) > participantMaxProofBytes {
		return nil, errors.New("farm participant CommitPlotUnlock result proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeCommitPlotUnlockProof(data []byte) (unlockParticipantProof, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK)
	if err != nil {
		return unlockParticipantProof{}, err
	}
	return decodeUnlockParticipantProof(data, spec)
}

func decodeUnlockParticipantProof(data []byte, spec contracts.ParticipantCommandSpec) (unlockParticipantProof, error) {
	var proof unlockParticipantProof
	if len(data) == 0 || len(data) > participantMaxProofBytes {
		return proof, errors.New("farm participant CommitPlotUnlock result proof size is invalid")
	}
	r := participantProofReader{data: data}
	magic, err := r.lp()
	if err != nil || string(magic) != unlockParticipantProofMagic {
		return proof, errors.New("farm participant CommitPlotUnlock result proof magic is invalid")
	}
	version, err := r.u32()
	if err != nil || version != participantProofVersion {
		return proof, errors.New("farm participant CommitPlotUnlock result proof version is invalid")
	}
	command, err := r.u32()
	if err != nil || command != uint32(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK) || command != uint32(spec.Command) {
		return proof, errors.New("farm participant CommitPlotUnlock result proof command is invalid")
	}
	invocation, err := r.lp()
	if err != nil || len(invocation) == 0 {
		return proof, errors.New("farm participant CommitPlotUnlock result proof invocation is invalid")
	}
	stepHash, err := r.lp()
	if err != nil || len(stepHash) != sha256.Size {
		return proof, errors.New("farm participant CommitPlotUnlock result proof step hash is invalid")
	}
	resultHash, err := r.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, errors.New("farm participant CommitPlotUnlock result proof result hash is invalid")
	}
	recordVersion, err := r.u64()
	if err != nil || recordVersion == 0 {
		return proof, errors.New("farm participant CommitPlotUnlock result proof record version is invalid")
	}
	errorValue, err := r.u32()
	if err != nil {
		return proof, err
	}
	if _, ok := commonpb.ErrorCode_name[int32(errorValue)]; !ok {
		return proof, errors.New("farm participant CommitPlotUnlock result proof error code is unknown")
	}
	outcomeValue, err := r.u32()
	if err != nil || outcomeValue < uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED) || outcomeValue > uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) {
		return proof, errors.New("farm participant CommitPlotUnlock result proof outcome is invalid")
	}
	typed, err := r.lp()
	if err != nil || len(typed) == 0 || r.remaining() != 0 {
		return proof, errors.New("farm participant CommitPlotUnlock result proof typed result is invalid")
	}
	result := &contractspb.CommitPlotUnlockResult{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(typed, result); err != nil || len(result.ProtoReflect().GetUnknown()) != 0 {
		return proof, errors.New("farm participant CommitPlotUnlock result proof typed result cannot decode")
	}
	canonicalBytes, canonicalHash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonicalBytes, typed) || !bytes.Equal(canonicalHash, resultHash) || len(result.GetUnlockDecisionHash()) != sha256.Size {
		return proof, errors.New("farm participant CommitPlotUnlock result proof typed result is not canonical")
	}
	proof = unlockParticipantProof{outcome: contractspb.ParticipantOutcome(outcomeValue), errorCode: commonpb.ErrorCode(errorValue), command: contractspb.ParticipantCommandType(command), invocationID: string(invocation), stepHash: append([]byte(nil), stepHash...), resultHash: append([]byte(nil), resultHash...), recordVersion: recordVersion, result: result}
	return proof, nil
}

func ResolveCommitPlotUnlockParticipantReceipt(record *farmpb.FarmRecord, recordVersion uint64, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK || record == nil {
		return nil, false, nil
	}
	wantKey := ParticipantReceiptOperationKey(call.InvocationID)
	var receipt *farmpb.FarmOperationReceipt
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		candidate := record.GetRecentOperationReceipts()[index]
		if candidate != nil && candidate.GetOperationId() == wantKey {
			receipt = candidate
			break
		}
	}
	if receipt == nil {
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	proof, err := decodeUnlockParticipantProof(receipt.GetResultPayload(), call.Spec)
	if err != nil {
		return nil, true, err
	}
	if proof.invocationID != call.InvocationID || !bytes.Equal(proof.stepHash, call.CanonicalStepPayloadHash[:]) || proof.recordVersion == 0 || proof.recordVersion > recordVersion || receipt.GetErrorCode() != proof.errorCode || receipt.GetStatus() != participantCommandStatus(proof.outcome) {
		return nil, true, status.Error(codes.Internal, "farm participant CommitPlotUnlock durable receipt binding is corrupt")
	}
	return &contracts.StepOutcome{Outcome: proof.outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true, CommittedResultHash: proof.resultHash, Result: proof.result, CommittedRecordVersion: proof.recordVersion, Failure: participantFailure(proof.errorCode)}, true, nil
}

// EstimateParticipantCommitPlotUnlockModelBytes is the detached capacity
// estimate used before the FarmActor mailbox. The exact turn repeats the
// guard against the candidate carrying the real receipt and Plot projection.
func EstimateParticipantCommitPlotUnlockModelBytes(record *farmpb.FarmRecord) (int, error) {
	if record == nil {
		return 0, ErrParticipantCapacityUnavailable
	}
	candidate := proto.Clone(record).(*farmpb.FarmRecord)
	if candidate.Core == nil {
		candidate.Core = &farmpb.FarmCore{FarmId: candidate.GetFarmId()}
	}
	if err := appendParticipantReceipt(candidate, "capacity-estimate-unlock", make([]byte, sha256.Size), make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
		return 0, err
	}
	return proto.Size(candidate) + len("record"), nil
}
