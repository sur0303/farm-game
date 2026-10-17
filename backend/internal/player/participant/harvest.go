package participant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/contracts"
	playeractor "farmgame/internal/player/actor"
	"farmgame/internal/player/compendium"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/player/occupancy"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/taskcatalog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// These are the logical P0 compendium fences used by the PlayerActor
	// adapter. Catalog publication owns the real catalog/page limits; the
	// reward turn only needs a positive, bounded membership fence and never
	// reads the current catalog.
	harvestCompendiumLimitProfileVersion uint64 = 1
	harvestCompendiumMaxEntries          uint32 = math.MaxUint32
	harvestCompendiumMaxStateBytes       uint32 = math.MaxUint32
	harvestCompendiumMaxPageEntries      uint32 = math.MaxUint32
	harvestCompendiumMaxPageBytes        uint32 = math.MaxUint32
)

// HarvestDecisionHash computes the frozen HarvestDecision hash over all
// semantic fields except harvest_decision_hash itself. It is exported as a
// small producer/consumer seam so Farm-side tests and recovery fixtures can
// construct the exact value the Player Participant verifies.
func HarvestDecisionHash(decision *contractspb.HarvestDecision) ([32]byte, error) {
	if decision == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "harvest decision is required")
	}
	copyOf, ok := proto.Clone(decision).(*contractspb.HarvestDecision)
	if !ok {
		return [32]byte{}, status.Error(codes.Internal, "harvest decision clone failed")
	}
	copyOf.HarvestDecisionHash = nil
	return contracts.CanonicalHash(copyOf)
}

// CommitHarvestResultHash computes the Farm Participant result hash carried
// by GrantHarvestRewardPayload.commit_harvest_result_hash.
func CommitHarvestResultHash(result *contractspb.CommitHarvestResult) ([32]byte, error) {
	if result == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "commit harvest result is required")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, result)
}

func isHarvestPlayerCommand(call *contracts.ParticipantCall) bool {
	return call != nil && call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
}

// validateHarvestPayload is pure and intentionally does not inspect Farm or
// Player persistence. The physical Work/Farm proof is injected separately by
// HarvestDependencyVerifier and is required before this call can mutate the
// PlayerActor record.
func validateHarvestPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "player participant Harvest call is incomplete")
	}
	payload, ok := call.Payload.(*contractspb.GrantHarvestRewardPayload)
	if !ok {
		return status.Error(codes.Internal, "player participant Harvest payload schema mismatch")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD {
		return status.Error(codes.InvalidArgument, "player participant Harvest command mismatch")
	}
	if payload.GetPlayerId() == 0 || payload.GetPlayerId() != call.RouteHeader.GetActorId() ||
		len(payload.GetCommitHarvestResultHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant Harvest payload player/result binding is invalid")
	}
	decision := payload.GetHarvestDecision()
	if decision == nil || decision.GetWorkflowId() == "" || decision.GetWorkflowId() != call.Work.GetWorkId() ||
		decision.GetOwnerPlayerId() == 0 || decision.GetOwnerPlayerId() != payload.GetPlayerId() ||
		decision.GetFarmId() == 0 || decision.GetPlotId() == 0 || decision.GetCropCycleId() == "" ||
		decision.GetCropDefinitionId() == 0 || decision.GetRewardItemId() == 0 ||
		len(decision.GetHarvestDecisionHash()) != sha256.Size {
		return status.Error(codes.FailedPrecondition, "player participant Harvest decision binding is invalid")
	}
	computedDecisionHash, err := HarvestDecisionHash(decision)
	if err != nil || !bytes.Equal(computedDecisionHash[:], decision.GetHarvestDecisionHash()) {
		return status.Error(codes.FailedPrecondition, "player participant Harvest decision hash is inconsistent")
	}
	snapshot := decision.GetCompendiumUnlockSnapshot()
	if snapshot == nil || snapshot.GetCropDefinitionId() != decision.GetCropDefinitionId() || snapshot.GetPlantedByPlayerId() == 0 {
		return status.Error(codes.FailedPrecondition, "player participant Harvest compendium snapshot binding is invalid")
	}
	return nil
}

func harvestCompendiumLimits() compendium.LimitProfile {
	return compendium.LimitProfile{
		ProfileVersion:           harvestCompendiumLimitProfileVersion,
		MaxCatalogEntries:        harvestCompendiumMaxEntries,
		MaxActivityArchives:      harvestCompendiumMaxEntries,
		MaxUnlockedEntries:       harvestCompendiumMaxEntries,
		MaxCompendiumStateBytes:  harvestCompendiumMaxStateBytes,
		MaxCompendiumPageEntries: harvestCompendiumMaxPageEntries,
		MaxCompendiumPageBytes:   harvestCompendiumMaxPageBytes,
	}
}

func compendiumStateFromPlayerRecord(rec *playerpb.PlayerRecord) (compendium.State, error) {
	if rec == nil {
		return compendium.State{}, errors.New("player participant Harvest PlayerRecord is nil")
	}
	facts := make([]compendium.UnlockFact, 0, len(rec.GetCodexEntries()))
	seen := make(map[uint32]struct{}, len(rec.GetCodexEntries()))
	for _, entry := range rec.GetCodexEntries() {
		if entry == nil {
			continue
		}
		cropID := uint32(entry.GetCropDefinitionId())
		if cropID == 0 {
			return compendium.State{}, errors.New("player participant Harvest PlayerRecord contains an invalid compendium entry")
		}
		if _, exists := seen[cropID]; exists {
			return compendium.State{}, errors.New("player participant Harvest PlayerRecord contains duplicate compendium entry")
		}
		seen[cropID] = struct{}{}
		facts = append(facts, compendium.UnlockFact{
			CropDefinitionID:          cropID,
			SourceActivityID:          entry.GetSourceOperationId(),
			CatalogVersionAtUnlock:    0,
			DefinitionVersionAtUnlock: 0,
		})
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].CropDefinitionID < facts[j].CropDefinitionID })
	for i := range facts {
		facts[i].UnlockRevision = uint64(i + 1)
	}
	return compendium.State{Facts: facts, Revision: uint64(len(facts))}, nil
}

func applyCompendiumCandidate(rec *playerpb.PlayerRecord, before compendium.State, candidate compendium.State, operationID string, nowMs int64) error {
	if rec == nil {
		return errors.New("player participant Harvest PlayerRecord is nil")
	}
	existing := make(map[uint32]struct{}, len(before.Facts))
	for _, fact := range before.Facts {
		existing[fact.CropDefinitionID] = struct{}{}
	}
	for _, fact := range candidate.Facts {
		if _, ok := existing[fact.CropDefinitionID]; ok {
			continue
		}
		rec.CodexEntries = append(rec.CodexEntries, &playerpb.CodexEntry{
			CropDefinitionId:   int32(fact.CropDefinitionID),
			FirstHarvestedAtMs: nowMs,
			SourceOperationId:  operationID,
		})
		existing[fact.CropDefinitionID] = struct{}{}
	}
	sort.Slice(rec.CodexEntries, func(i, j int) bool {
		if rec.CodexEntries[i] == nil {
			return false
		}
		if rec.CodexEntries[j] == nil {
			return true
		}
		return rec.CodexEntries[i].GetCropDefinitionId() < rec.CodexEntries[j].GetCropDefinitionId()
	})
	return nil
}

func harvestUnlockDeltaProto(delta compendium.UnlockDelta) *contractspb.CompendiumUnlockDelta {
	result := &contractspb.CompendiumUnlockDelta{
		NewlyUnlocked:           delta.NewlyUnlocked,
		CompendiumRevisionAfter: delta.RevisionAfter,
	}
	if delta.NewlyUnlocked {
		cropID := delta.CropDefinitionID
		result.CropDefinitionId = &cropID
	}
	return result
}

// nextHarvestCount reads the prior committed Harvest result receipts. The
// receipt is the existing durable Participant carrier and is retained after
// hot eviction, so this does not introduce a second counter state source into
// PlayerRecord. A corrupt prior Harvest proof fails closed rather than risking
// a non-monotonic counter.
func nextHarvestCount(rec *playerpb.PlayerRecord) (uint64, error) {
	if rec == nil {
		return 0, errors.New("player participant Harvest PlayerRecord is nil")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
	if err != nil {
		return 0, err
	}
	participantReceiptPrefix := playeringress.ParticipantReceiptOperationKey("")
	var count uint64
	for _, receipt := range rec.GetRecentOperationReceipts() {
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptPrefix) || len(receipt.GetResultPayload()) < 9 ||
			!bytes.Equal(receipt.GetResultPayload()[:4], []byte(participantResultProofMagic)) ||
			receipt.GetResultPayload()[4] != participantResultProofVersion ||
			binary.BigEndian.Uint32(receipt.GetResultPayload()[5:9]) != uint32(spec.Command) {
			continue
		}
		stepOffset, _, _, _, ok := participantResultProofOffsets(receipt.GetResultPayload())
		if !ok {
			return 0, errors.New("player participant Harvest prior result proof is corrupt")
		}
		invocationID := string(receipt.GetResultPayload()[11:stepOffset])
		result, _, decodeErr := decodeParticipantResultProof(spec, invocationID, receipt.GetPayloadHash(), receipt.GetResultPayload())
		if decodeErr != nil {
			return 0, decodeErr
		}
		harvestResult, ok := result.(*contractspb.GrantHarvestRewardResult)
		if !ok {
			return 0, errors.New("player participant Harvest prior result schema mismatch")
		}
		if harvestResult.GetHarvestCountAfter() > count {
			count = harvestResult.GetHarvestCountAfter()
		}
	}
	if count == math.MaxUint64 {
		return 0, errors.New("player participant Harvest counter overflow")
	}
	return count + 1, nil
}

func harvestOperationResultCapacityEstimate() authoritystore.StoredOperationOutcome {
	return authoritystore.StoredOperationOutcome{
		Status:  authoritystore.TerminalSucceeded,
		Payload: make([]byte, maxHarvestTypedResultBytes),
	}
}

func harvestStoredOperationOutcome(outcome contractspb.ParticipantOutcome, errorCode commonpb.ErrorCode, typedResult []byte) authoritystore.StoredOperationOutcome {
	statusValue := authoritystore.TerminalRejected
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		statusValue = authoritystore.TerminalSucceeded
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		statusValue = authoritystore.TerminalNoChange
	}
	errorCodeText := ""
	if errorCode != commonpb.ErrorCode_OK && errorCode != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		errorCodeText = errorCode.String()
	}
	return authoritystore.StoredOperationOutcome{Status: statusValue, ErrorCode: errorCodeText, Payload: append([]byte(nil), typedResult...)}
}

func (h *plantHandler) grantHarvestReward(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.GrantHarvestRewardPayload) (*contractspb.GrantHarvestRewardResult, contractspb.ParticipantOutcome, commonpb.ErrorCode, error) {
	decision := payload.GetHarvestDecision()
	countAfter, err := nextHarvestCount(rec)
	if err != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
	}
	compendiumState, err := compendiumStateFromPlayerRecord(rec)
	if err != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
	}
	snapshot := decision.GetCompendiumUnlockSnapshot()
	unlockOutcome := compendium.DecideUnlock(compendiumState, compendium.UnlockCommand{
		PlayerID:         payload.GetPlayerId(),
		OwnerPlayerID:    decision.GetOwnerPlayerId(),
		CropDefinitionID: decision.GetCropDefinitionId(),
		Snapshot: compendium.UnlockSnapshot{
			CropDefinitionID:         snapshot.GetCropDefinitionId(),
			UnlockEligible:           snapshot.GetUnlockEligible(),
			PlantedByPlayerID:        snapshot.GetPlantedByPlayerId(),
			ActivityID:               snapshot.GetActivityId(),
			CatalogVersionAtPlant:    snapshot.GetCatalogVersionAtPlant(),
			DefinitionVersionAtPlant: snapshot.GetDefinitionVersionAtPlant(),
		},
	}, harvestCompendiumLimits())
	if unlockOutcome.Status == compendium.StatusRejected {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, fmt.Errorf("harvest compendium decision rejected: %v", unlockOutcome.Err)
	}
	nowMs := h.nowMs()
	operationID := call.OperationID
	if strings.TrimSpace(operationID) == "" {
		operationID = call.Work.GetWorkId()
	}
	if unlockOutcome.Status == compendium.StatusSucceeded {
		if err := applyCompendiumCandidate(rec, compendiumState, unlockOutcome.Candidate, operationID, nowMs); err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
	}
	if decision.GetRewardQuantity() > math.MaxInt64 || decision.GetRewardExp() > math.MaxInt64 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest reward exceeds PlayerRecord integer range")
	}
	if decision.GetRewardItemId() > math.MaxInt32 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest reward item id exceeds PlayerRecord integer range")
	}
	quantity := int64(decision.GetRewardQuantity())
	if playeractor.ItemCount(rec, int32(decision.GetRewardItemId())) > math.MaxInt64-quantity {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest inventory quantity overflow")
	}
	if !playeractor.AddItem(rec, int32(decision.GetRewardItemId()), quantity) {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest inventory grant failed")
	}
	if rec.GetProgression() == nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest progression is unavailable")
	}
	rewardExp := int64(decision.GetRewardExp())
	if rec.Progression.Exp > math.MaxInt64-rewardExp {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest experience overflow")
	}
	rec.Progression.Exp += rewardExp
	level64 := rec.Progression.Exp/20 + 1
	if level64 > math.MaxInt32 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest level overflow")
	}
	rec.Progression.Level = int32(level64)
	if h.taskDomain == nil || h.taskDomainErr != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("harvest TaskDomain configuration is unavailable")
	}
	taskState, err := playertask.DomainStateFromRecord(rec)
	if err != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
	}
	// P0's active growth metric is total harvest. Keep the monotonic level
	// indicator in the same single-version task turn without creating a second
	// TaskStateVersion bump after RecordHarvest.
	if rec.Progression.Level > taskState.HighestLevel {
		taskState.HighestLevel = rec.Progression.Level
	}
	harvestFact, err := h.taskDomain.RecordHarvest(&taskState, 1, nowMs)
	if err != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
	}
	if harvestFact.StateChanged {
		if err := playertask.ApplyDomainStateToRecord(rec, &taskState); err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
	}
	playeractor.AdvanceTask(rec, taskcatalog.EventHarvest, 1, nowMs)
	rec.EconomyVersion++
	if plotID := decision.GetPlotId(); plotID != 0 {
		occupancy.ClearPlot(rec, plotID)
	}

	delta := harvestUnlockDeltaProto(unlockOutcome.Delta)
	result := &contractspb.GrantHarvestRewardResult{
		CropCycleId:            decision.GetCropCycleId(),
		RewardItemId:           decision.GetRewardItemId(),
		GrantedQuantity:        decision.GetRewardQuantity(),
		InventoryQuantityAfter: uint64(playeractor.ItemCount(rec, int32(decision.GetRewardItemId()))),
		GrantedExp:             decision.GetRewardExp(),
		PlayerLevelAfter:       uint32(rec.Progression.Level),
		HarvestCountAfter:      countAfter,
		CompendiumUnlockDelta:  delta,
	}
	return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, nil
}

func setHarvestRecordVersion(result proto.Message, version uint64) error {
	harvestResult, ok := result.(*contractspb.GrantHarvestRewardResult)
	if !ok {
		return status.Error(codes.Internal, "player participant Harvest result schema mismatch")
	}
	harvestResult.PlayerRecordVersion = version
	return nil
}

func harvestRecordVersion(result proto.Message) uint64 {
	if harvestResult, ok := result.(*contractspb.GrantHarvestRewardResult); ok {
		return harvestResult.GetPlayerRecordVersion()
	}
	return 0
}

func decodeHarvestReceipt(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, receipt *playerpb.PlayerOperationReceipt) (contractspb.ParticipantOutcome, proto.Message, []byte, *contractspb.ParticipantFailure, uint64, error) {
	var outcome contractspb.ParticipantOutcome
	switch receipt.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	case commonpb.CommandStatus_REJECTED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
	case commonpb.CommandStatus_NO_CHANGE:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
	default:
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Harvest receipt has non-terminal status")
	}
	if len(receipt.GetResultPayload()) == 0 || !bytes.Equal(receipt.GetPayloadHash(), expectedStepHash) {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Harvest receipt/proof binding is corrupt")
	}
	result, resultHash, err := decodeParticipantResultProof(spec, expectedInvocationID, expectedStepHash, receipt.GetResultPayload())
	if err != nil {
		return 0, nil, nil, nil, 0, err
	}
	version := harvestRecordVersion(result)
	if version == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Harvest receipt missing record version")
	}
	var failure *contractspb.ParticipantFailure
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK && receipt.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		failure = &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: receipt.GetErrorCode()}}
	}
	return outcome, result, resultHash, failure, version, nil
}

func (h *plantHandler) executeHarvest(ctx context.Context, call *contracts.ParticipantCall, validate func() error) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	budgetCtx, cancel := context.WithTimeout(ctx, timeDurationMs(call.RemainingBudgetMs))
	defer cancel()
	result, err := h.registry.SubmitParticipantVersionedWithOutcomeFromAdmission(budgetCtx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, func(rec *playerpb.PlayerRecord, currentVersion uint64, guard playeractor.ParticipantCapacityGuard) (any, bool, authoritystore.StoredOperationOutcome, error) {
		if receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID); receipt != nil {
			if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
				return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
			}
			outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodeHarvestReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
			if decodeErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, decodeErr
			}
			if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, err
			}
			return &contracts.StepOutcome{Outcome: outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: typedResult, Failure: failure, CommittedRecordVersion: committedVersion}, false, authoritystore.StoredOperationOutcome{}, nil
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := validate(); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		capacityNowMs := h.nowMs()
		if guard.PhysicalProjectionRequired() {
			estimate, estimateErr := playeringress.EstimateParticipantRecordCapacityAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxHarvestResultProofEnvelopeBytes(call.InvocationID), capacityNowMs)
			if estimateErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, estimateErr
			}
			if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			if err := guard.CheckWithOutcome(estimate.ModelBytes, harvestOperationResultCapacityEstimate()); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
			if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		typedResult, outcome, errorCode, decisionErr := h.grantHarvestReward(rec, call, call.Payload.(*contractspb.GrantHarvestRewardPayload))
		if decisionErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.Internal, decisionErr.Error())
		}
		committedVersion := currentVersion + 1
		if err := setHarvestRecordVersion(typedResult, committedVersion); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		resultProof, resultHash, proofErr := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, typedResult)
		if proofErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, proofErr
		}
		typedResultBytes, _, typedResultErr := contracts.ValidateTypedResult(call.Spec, typedResult)
		if typedResultErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, typedResultErr
		}
		storedOutcome := harvestStoredOperationOutcome(outcome, errorCode, typedResultBytes)
		if guard.PhysicalProjectionRequired() {
			candidate := proto.Clone(rec).(*playerpb.PlayerRecord)
			playeringress.AppendParticipantOperationReceiptAt(candidate, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, h.nowMs())
			candidateModelBytes := len("record") + proto.Size(candidate)
			if err := guard.CheckWithOutcome(candidateModelBytes, storedOutcome); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		playeringress.AppendParticipantOperationReceiptAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, h.nowMs())
		return &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: typedResult, Failure: plantFailure(errorCode), CommittedRecordVersion: committedVersion}, true, storedOutcome, nil
	})
	if err != nil {
		if errorsIsTimeoutUnknown(err) {
			return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN, Failure: &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT}, RetryableAfterResolution: true}}, nil
		}
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		}
		if errors.Is(err, blobstore.ErrCASConflict) {
			return nil, status.Error(codes.Aborted, "RECORD_VERSION_CONFLICT")
		}
		if errors.Is(err, blobstore.ErrUnavailable) {
			return nil, status.Error(codes.Unavailable, "PLAYER_AUTHORITY_UNAVAILABLE")
		}
		return nil, normalizeParticipantError(err)
	}
	outcome, ok := result.Value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		return nil, status.Error(codes.Internal, "player participant Harvest returned invalid turn result")
	}
	return outcome, nil
}

// timeDurationMs keeps the Harvest execution path independent from Plant's
// existing helper names while preserving the same trusted budget bound.
func timeDurationMs(ms uint32) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
