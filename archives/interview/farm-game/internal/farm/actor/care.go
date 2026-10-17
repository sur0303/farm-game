package actor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

const (
	careProofMagic    = "FARM-CARE-RESULT-PROOF-V1"
	careOutcomeMagic  = "FARM-CARE-OUTCOME-V1"
	careProofVersion  = uint32(1)
	careRewardExpBase = uint64(1)
	careProofMaxBytes = participantMaxProofBytes
	careOrderType     = string(identity.WorkTypeCareReward)
)

// ErrCareOrderAuthorityUnavailable means the caller tried to execute the
// production CARE path on a legacy blobstore binding. A CareRewardOrder must
// never be written after a separate farm record write.
var ErrCareOrderAuthorityUnavailable = errors.New("farm actor: CARE source-order authority unavailable")

// CareResolutionRequest is the trusted FarmActor-side CARE adapter. The
// ingress owns Pair Authority ActionAdmission; the actor receives only the
// frozen ActionPermitSnapshot and rechecks its binding in the same mailbox
// turn as the domain decision. Friend Care never re-reads the live relation.
type CareResolutionRequest struct {
	OperationID      string
	PayloadHash      []byte
	ResolverPlayerID uint64
	PlotID           uint32
	CycleID          string
	IncidentID       string
	CareType         domain.CareType
	Context          contractspb.CareContext
	RelationEpoch    uint64
	// AdmittedPermit is required for FRIEND Care. It is the Pair Authority
	// Execution-Accept snapshot; OWNER Care must leave it nil.
	AdmittedPermit *friend.ActionPermitSnapshot
	TrustedNowMs   int64
	// WantSourceCommitEvidence opts into the expensive exact commit carrier
	// used only by explicit shadow/prefer proof modes. The zero value is off.
	WantSourceCommitEvidence bool
}

// CareActionResult is the internal stable result carried beside the existing
// FarmCommandResult projection. It contains the immutable source outcome, not
// Player reward delivery state.
type CareActionResult struct {
	Outcome                   domain.CareOutcome
	PlotID                    uint32
	CycleID                   string
	IncidentID                string
	CareType                  domain.CareType
	ResolvedByPlayerID        uint64
	ResolvedAtMs              int64
	CommittedPlotVersion      uint64
	CommittedFarmVersion      uint64
	CareRewardOrderID         string
	RewardObligationCreated   bool
	FriendInteractionEligible bool
	Replayed                  bool
}

type careProof struct {
	outcome              domain.CareOutcome
	operationID          string
	payloadHash          []byte
	recordVersion        uint64
	resultHash           []byte
	snapshot             *contractspb.CareResolutionSnapshot
	orderID              string
	rewardCreated        bool
	plotID               uint32
	cycleID              string
	incidentID           string
	careType             domain.CareType
	resolvedAtMs         int64
	resolvedByPlayerID   uint64
	committedPlotVersion uint64
	committedFarmVersion uint64
	header               domain.OpportunityHeader
}

// ResolveCareIncident is the production FarmActor CARE turn. A successful
// turn creates the resolved incident, business receipt, reward snapshot,
// source order and DueIndex entry in one writer-fenced Authority Commit.
func (a *FarmActor) ResolveCareIncident(ctx context.Context, req CareResolutionRequest) (*ActionResult, error) {
	if a == nil || a.record == nil || a.record.Core == nil {
		return nil, errors.New("farm actor: CARE record is unavailable")
	}
	if req.OperationID == "" || len(req.PayloadHash) != sha256.Size || req.ResolverPlayerID == 0 ||
		req.PlotID == 0 || req.CycleID == "" || req.IncidentID == "" || req.TrustedNowMs <= 0 {
		return nil, farmError(commonpb.ErrorCode_ERR_DECODE, "CARE request is incomplete")
	}
	if replay, err := a.replayCommand(req.OperationID, req.PayloadHash); replay != nil || err != nil {
		return replay, err
	}
	if err := validateCareAuthorization(a.farmID, req); err != nil {
		return nil, err
	}
	if err := a.ensureParticipantCycles(); err != nil {
		return nil, err
	}
	if err := a.ensureReceiptCapacity(time.UnixMilli(req.TrustedNowMs)); err != nil {
		return nil, err
	}

	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	state := a.buildParticipantState()
	candidate, decision, domainErr := domain.DecideCare(state, domain.CareCommand{
		ResolverPlayerID: req.ResolverPlayerID,
		PlotID:           req.PlotID,
		CycleID:          req.CycleID,
		IncidentID:       req.IncidentID,
		CareType:         req.CareType,
	}, req.TrustedNowMs)
	if domainErr != nil && domainErr.Status != domain.StatusNoChange {
		return nil, mapCareDomainError(domainErr)
	}

	if domainErr != nil && domainErr.Status == domain.StatusNoChange {
		result := careResultFromDecision(req, state, decision, "", false)
		header := careCurrentHeader(state, req.TrustedNowMs)
		proof, err := encodeCareProof(req, result, nil, maxUint64(a.recordVersion+1, 1), header)
		if err != nil {
			return nil, err
		}
		candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
		if err := appendCareReceipt(candidateRecord, req.OperationID, req.PayloadHash, proof,
			commonpb.CommandStatus_NO_CHANGE, req.TrustedNowMs); err != nil {
			return nil, err
		}
		a.record = candidateRecord
		outcome := authoritystore.StoredOperationOutcome{
			Status:  authoritystore.TerminalNoChange,
			Payload: encodeCareOutcomePayload("", false),
		}
		if err := a.saveWithCandidateAndOutcomeAndOrders(ctx, before, req.OperationID, commonpb.CommandType_CARE, nil, outcome, nil); err != nil {
			return nil, err
		}
		// NO_CHANGE has no business-version candidate and therefore no delivery
		// event. The receipt commit still advances the Authority RecordVersion.
		a.guardClear()
		a.participantState = &participantFarmState{state: state}
		result.Replayed = false
		return &ActionResult{
			Status:          commonpb.CommandStatus_NO_CHANGE,
			Result:          &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(req.TrustedNowMs))},
			CareResult:      result,
			ActivationEpoch: a.activationEpoch,
		}, nil
	}

	if !decision.RewardObligationCreated || candidate.State.FindCycle(req.CycleID) == nil {
		return nil, errors.New("farm actor: CARE candidate is incomplete")
	}
	if err := syncCareCandidateToRecord(a.record, candidate.State, req); err != nil {
		a.record = before
		return nil, err
	}
	snapshot, order, orderID, err := buildCareRewardOrder(a.farmID, req, candidate.State, decision)
	if err != nil {
		a.record = before
		return nil, err
	}
	result := careResultFromDecision(req, candidate.State, decision, orderID, true)
	result.FriendInteractionEligible = req.Context == contractspb.CareContext_CARE_CONTEXT_FRIEND
	header := candidate.State.Header
	proof, err := encodeCareProof(req, result, snapshot, maxUint64(a.recordVersion+1, 1), header)
	if err != nil {
		a.record = before
		return nil, err
	}
	if err := appendCareReceipt(a.record, req.OperationID, req.PayloadHash, proof,
		commonpb.CommandStatus_SUCCEEDED, req.TrustedNowMs); err != nil {
		a.record = before
		return nil, err
	}

	plot := participantPublicPlot(a.record, req.PlotID)
	if plot == nil {
		a.record = before
		return nil, farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "plot %d is not in the public Farm record", req.PlotID)
	}
	change := &farmpb.FarmChange{
		OwnerId:      a.farmID,
		FarmVersion:  candidate.State.FarmVersion,
		ChangedPlots: []*farmpb.PlotView{plotView(a.record, plot, time.UnixMilli(req.TrustedNowMs))},
	}
	outcome := authoritystore.StoredOperationOutcome{
		Status:  authoritystore.TerminalSucceeded,
		Payload: encodeCareOutcomePayload(orderID, true),
	}
	var commitEvidence *authoritystore.ExactCommitEvidence
	if req.WantSourceCommitEvidence {
		commitEvidence, err = a.saveWithCandidateAndOutcomeAndOrdersExact(ctx, before, req.OperationID, commonpb.CommandType_CARE, change, outcome, []authoritystore.AttachedOrder{order})
	} else {
		err = a.saveWithCandidateAndOutcomeAndOrders(ctx, before, req.OperationID, commonpb.CommandType_CARE, change, outcome, []authoritystore.AttachedOrder{order})
	}
	if err != nil {
		return nil, err
	}
	a.applyParticipantCareState(candidate.State, req.CycleID)
	change, recipients, resyncs, err := a.publishCandidate(plot, time.UnixMilli(req.TrustedNowMs))
	if err != nil {
		return nil, err
	}
	committedOrder := order
	committedOrder.Payload = append([]byte(nil), order.Payload...)
	return &ActionResult{
		Status:     commonpb.CommandStatus_SUCCEEDED,
		Result:     &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(req.TrustedNowMs))},
		CareResult: result, Change: change, CommittedSourceOrder: &committedOrder,
		SourceCommitEvidence:         commitEvidence,
		FriendOpportunityInvalidated: req.CareType == domain.CareTypeWater,
		Recipients:                   recipients, Resyncs: resyncs,
		ActivationEpoch: a.activationEpoch,
	}, nil
}

// ResolveCare is a short alias for callers that use the domain command name.
func (a *FarmActor) ResolveCare(ctx context.Context, req CareResolutionRequest) (*ActionResult, error) {
	return a.ResolveCareIncident(ctx, req)
}

func (a *FarmActor) replayCareProof(statusValue commonpb.CommandStatus, proof careProof) *ActionResult {
	result := &CareActionResult{
		Outcome:                   proof.outcome,
		PlotID:                    proof.plotID,
		CycleID:                   proof.cycleID,
		IncidentID:                proof.incidentID,
		CareType:                  proof.careType,
		ResolvedByPlayerID:        proof.resolvedByPlayerID,
		ResolvedAtMs:              proof.resolvedAtMs,
		CommittedPlotVersion:      proof.committedPlotVersion,
		CommittedFarmVersion:      proof.committedFarmVersion,
		CareRewardOrderID:         proof.orderID,
		RewardObligationCreated:   proof.rewardCreated,
		FriendInteractionEligible: proof.snapshot != nil && proof.snapshot.GetFriendInteractionEligible(),
		Replayed:                  true,
	}
	now := time.Now()
	if proof.resolvedAtMs != 0 {
		now = time.UnixMilli(proof.resolvedAtMs)
	}
	return &ActionResult{
		Status:     statusValue,
		Result:     &farmpb.FarmCommandResult{Snapshot: a.Snapshot(now)},
		CareResult: result, Replayed: true, ActivationEpoch: a.activationEpoch,
	}
}

func validateCareReceiptBinding(receipt *farmpb.FarmOperationReceipt, proof careProof) error {
	if receipt == nil || receipt.GetOperationId() != proof.operationID ||
		!bytes.Equal(receipt.GetPayloadHash(), proof.payloadHash) || receipt.GetErrorCode() != commonpb.ErrorCode_OK {
		return errors.New("farm actor: CARE receipt envelope binding is invalid")
	}
	wantStatus := commonpb.CommandStatus_SUCCEEDED
	if proof.outcome == domain.CareOutcomeAlreadyResolved {
		wantStatus = commonpb.CommandStatus_NO_CHANGE
	}
	if receipt.GetStatus() != wantStatus {
		return errors.New("farm actor: CARE receipt status binding is invalid")
	}
	return nil
}

func validateCareAuthorization(farmID uint64, req CareResolutionRequest) error {
	switch req.Context {
	case contractspb.CareContext_CARE_CONTEXT_OWNER:
		if req.ResolverPlayerID != farmID || req.RelationEpoch != 0 || req.AdmittedPermit != nil {
			return farmError(commonpb.ErrorCode_ERR_PERMISSION_DENIED, "owner CARE binding is invalid")
		}
	case contractspb.CareContext_CARE_CONTEXT_FRIEND:
		if req.ResolverPlayerID == farmID || req.AdmittedPermit == nil {
			return farmError(commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, "friend CARE permit binding is invalid")
		}
		permit := *req.AdmittedPermit
		if err := friend.VerifyAdmittedPermit(permit); err != nil {
			return farmError(commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, "friend CARE admitted permit is invalid")
		}
		wantBinding := CareTargetBindingHash(req.PlotID, req.CycleID, req.IncidentID, req.CareType)
		if permit.VisitorPlayerID != req.ResolverPlayerID ||
			permit.OwnerPlayerID != farmID ||
			permit.TargetFarmID != farmID ||
			permit.OperationID != req.OperationID ||
			permit.Scope != friend.ActionScopeCare ||
			permit.RelationEpoch == 0 ||
			permit.RelationEpoch != req.RelationEpoch ||
			!bytes.Equal(permit.PayloadHash, req.PayloadHash) ||
			!bytes.Equal(permit.TargetBindingHash, wantBinding) {
			return farmError(commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, "friend CARE admitted permit binding mismatch")
		}
	default:
		return farmError(commonpb.ErrorCode_ERR_PERMISSION_DENIED, "CARE context is invalid")
	}
	return nil
}

// CareTargetBindingHash is the server-side plot/cycle/incident/type binding
// frozen into CommitFriendActionAdmission (33 §8.3). FarmActor recomputes it
// from the trusted request and refuses any permit that does not match.
func CareTargetBindingHash(plotID uint32, cycleID, incidentID string, careType domain.CareType) []byte {
	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"care-target-binding-v1|%d|%s|%s|%s",
		plotID, cycleID, incidentID, careType.String(),
	)))
	return sum[:]
}

func mapCareDomainError(err *domain.Error) error {
	if err == nil {
		return nil
	}
	switch err.Code {
	case domain.CodePlotNotFound:
		return farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "%s", err.Message)
	case domain.CodeCycleMismatch, domain.CodeCycleEnded, domain.CodeIncidentUnknown,
		domain.CodeIncidentNotDue, domain.CodeCareTypeMismatch:
		return farmError(commonpb.ErrorCode_ERR_VERSION_CONFLICT, "%s", err.Message)
	case domain.CodeInvalidArgument:
		return farmError(commonpb.ErrorCode_ERR_DECODE, "%s", err.Message)
	default:
		return farmError(commonpb.ErrorCode_ERR_INTERNAL, "%s", err.Message)
	}
}

func careResultFromDecision(req CareResolutionRequest, state domain.FarmState, decision domain.CareDecision, orderID string, rewardCreated bool) *CareActionResult {
	plotVersion := decision.CommittedPlotVersion
	if plotVersion == 0 {
		if plot := state.FindPlot(req.PlotID); plot != nil {
			plotVersion = plot.PlotVersion
		}
	}
	farmVersion := decision.CommittedFarmVersion
	if farmVersion == 0 {
		farmVersion = state.FarmVersion
	}
	return &CareActionResult{
		Outcome:                   decision.Outcome,
		PlotID:                    req.PlotID,
		CycleID:                   req.CycleID,
		IncidentID:                req.IncidentID,
		CareType:                  decision.CareType,
		ResolvedByPlayerID:        decision.ResolvedByPlayerID,
		ResolvedAtMs:              decision.ResolvedAtMs,
		CommittedPlotVersion:      plotVersion,
		CommittedFarmVersion:      farmVersion,
		CareRewardOrderID:         orderID,
		RewardObligationCreated:   rewardCreated,
		FriendInteractionEligible: req.Context == contractspb.CareContext_CARE_CONTEXT_FRIEND && rewardCreated,
	}
}

func buildCareRewardOrder(farmID uint64, req CareResolutionRequest, state domain.FarmState, decision domain.CareDecision) (*contractspb.CareResolutionSnapshot, authoritystore.AttachedOrder, string, error) {
	cycle := state.FindCycle(req.CycleID)
	if cycle == nil || cycle.CarePlan.CareRuleVersion == 0 {
		return nil, authoritystore.AttachedOrder{}, "", farmError(commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH, "CARE reward rule snapshot is unavailable")
	}
	snapshot := &contractspb.CareResolutionSnapshot{
		SourceFarmId:              farmID,
		SourceOwnerPlayerId:       farmID,
		CarerPlayerId:             req.ResolverPlayerID,
		PlotId:                    req.PlotID,
		CropCycleId:               req.CycleID,
		IncidentId:                req.IncidentID,
		CareType:                  contractCareType(decision.CareType),
		CareContext:               req.Context,
		ResolvedAtMs:              decision.ResolvedAtMs,
		CommittedPlotVersion:      decision.CommittedPlotVersion,
		CommittedFarmVersion:      decision.CommittedFarmVersion,
		CareRewardRuleVersion:     cycle.CarePlan.CareRuleVersion,
		RewardExp:                 careRewardExpBase,
		FriendInteractionEligible: req.Context == contractspb.CareContext_CARE_CONTEXT_FRIEND,
	}
	withoutHash := proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot)
	withoutHash.CareResolutionHash = nil
	hash, err := contracts.CanonicalHash(withoutHash)
	if err != nil {
		return nil, authoritystore.AttachedOrder{}, "", err
	}
	snapshot.CareResolutionHash = append([]byte(nil), hash[:]...)
	orderID, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor,
		strconv.FormatUint(farmID, 10), req.IncidentID, decision.CommittedFarmVersion)
	if err != nil {
		return nil, authoritystore.AttachedOrder{}, "", err
	}
	payloadMessage := &contractspb.ApplyCareRewardPayload{
		CarerPlayerId:          req.ResolverPlayerID,
		CareRewardOrderId:      orderID,
		CareResolutionSnapshot: proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot),
		CareResolutionHash:     append([]byte(nil), snapshot.GetCareResolutionHash()...),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payloadMessage)
	if err != nil {
		return nil, authoritystore.AttachedOrder{}, "", err
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		return nil, authoritystore.AttachedOrder{}, "", err
	}
	payloadHash, err := contracts.CanonicalParticipantHash(spec, payloadMessage)
	if err != nil {
		return nil, authoritystore.AttachedOrder{}, "", err
	}
	return snapshot, authoritystore.AttachedOrder{
		OrderID: orderID, OrderType: careOrderType, Payload: payload, PayloadHash: authoritystore.PayloadHash(payloadHash),
		DueAtMs: decision.ResolvedAtMs, OperationID: req.OperationID, SubjectPlayerID: req.ResolverPlayerID,
	}, orderID, nil
}

func contractCareType(value domain.CareType) contractspb.CareType {
	switch value {
	case domain.CareTypeWater:
		return contractspb.CareType_CARE_TYPE_WATER
	case domain.CareTypeRemovePest:
		return contractspb.CareType_CARE_TYPE_REMOVE_PEST
	default:
		return contractspb.CareType_CARE_TYPE_UNSPECIFIED
	}
}

func domainCareType(value contractspb.CareType) domain.CareType {
	switch value {
	case contractspb.CareType_CARE_TYPE_WATER:
		return domain.CareTypeWater
	case contractspb.CareType_CARE_TYPE_REMOVE_PEST:
		return domain.CareTypeRemovePest
	default:
		return domain.CareTypeUnspecified
	}
}

func syncCareCandidateToRecord(record *farmpb.FarmRecord, candidate domain.FarmState, req CareResolutionRequest) error {
	if record == nil || record.Core == nil {
		return errors.New("farm actor: CARE candidate record is unavailable")
	}
	plot := participantPublicPlot(record, req.PlotID)
	if plot == nil {
		return farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "plot %d is not in the Farm record", req.PlotID)
	}
	candidatePlot := candidate.FindPlot(req.PlotID)
	if candidatePlot == nil {
		return farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "plot %d is not in the CARE candidate", req.PlotID)
	}
	plot.PlotVersion = candidatePlot.PlotVersion
	record.Core.FarmVersion = candidate.FarmVersion
	cycle := recordCycle(record, req.CycleID)
	if cycle == nil {
		return farmError(commonpb.ErrorCode_ERR_VERSION_CONFLICT, "cycle %s is not in the Farm record", req.CycleID)
	}
	if req.CareType == domain.CareTypeWater {
		// Compatibility projection only. The incident resolution remains in
		// the frozen Participant proof/domain state, not in legacy Watered.
		cycle.Watered = true
	}
	return nil
}

func recordCycle(record *farmpb.FarmRecord, cycleID string) *farmpb.CropCycle {
	if record == nil || record.Core == nil {
		return nil
	}
	for _, cycle := range record.Core.CropCycles {
		if cycle != nil && cycle.GetCropCycleId() == cycleID {
			return cycle
		}
	}
	return nil
}

func (a *FarmActor) applyParticipantCareState(candidate domain.FarmState, cycleID string) {
	a.participantState = &participantFarmState{state: candidate}
	if a.participantCycles == nil {
		return
	}
	cycle := candidate.FindCycle(cycleID)
	frozen := a.participantCycles[cycleID]
	if cycle != nil && frozen != nil {
		frozen.cycle = cloneDomainCycle(*cycle)
	}
}

func careCurrentHeader(state domain.FarmState, trustedNowMs int64) domain.OpportunityHeader {
	if state.Header.SourceFarmVersion == state.FarmVersion && state.Header.DirtyGeneration != 0 {
		return state.Header
	}
	dirty := state.Header.DirtyGeneration
	if dirty == 0 {
		dirty = 1
	}
	return domain.ComputeOpportunity(&state, trustedNowMs, state.FarmVersion, dirty)
}

func appendCareReceipt(record *farmpb.FarmRecord, operationID string, payloadHash, proof []byte, statusValue commonpb.CommandStatus, nowMs int64) error {
	if record == nil || operationID == "" || len(payloadHash) != sha256.Size || len(proof) == 0 || len(proof) > careProofMaxBytes {
		return errors.New("farm actor: CARE receipt is incomplete")
	}
	normalizeRecord(record)
	cutoff := nowMs - operationReceiptRetention.Milliseconds()
	retained := make([]*farmpb.FarmOperationReceipt, 0, len(record.GetRecentOperationReceipts())+1)
	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt != nil && receipt.GetCommittedAtMs() >= cutoff {
			retained = append(retained, receipt)
		}
	}
	if len(retained) >= maxRecentReceipts {
		return farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "CARE receipt retention window is full")
	}
	record.RecentOperationReceipts = append(retained, &farmpb.FarmOperationReceipt{
		OperationId: operationID, Status: statusValue, ErrorCode: commonpb.ErrorCode_OK,
		ResultPayload: append([]byte(nil), proof...), CommittedAtMs: nowMs,
		PayloadHash: append([]byte(nil), payloadHash...),
	})
	return nil
}

func encodeCareOutcomePayload(orderID string, rewardCreated bool) []byte {
	buf := make([]byte, 0, len(careOutcomeMagic)+16+len(orderID))
	writeParticipantLPBytes(&buf, []byte(careOutcomeMagic))
	putParticipantUint32(&buf, careProofVersion)
	writeParticipantLPBytes(&buf, []byte(orderID))
	if rewardCreated {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func encodeCareProof(req CareResolutionRequest, result *CareActionResult, snapshot *contractspb.CareResolutionSnapshot, recordVersion uint64, header domain.OpportunityHeader) ([]byte, error) {
	if result == nil || req.OperationID == "" || len(req.PayloadHash) != sha256.Size || recordVersion == 0 {
		return nil, errors.New("farm actor: CARE proof input is incomplete")
	}
	outcomePayload := encodeCareOutcomePayload(result.CareRewardOrderID, result.RewardObligationCreated)
	resultHash := sha256.Sum256(outcomePayload)
	var snapshotBytes []byte
	var err error
	if snapshot != nil {
		snapshotBytes, err = proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
	}
	buf := make([]byte, 0, len(careProofMagic)+len(snapshotBytes)+256)
	writeParticipantLPBytes(&buf, []byte(careProofMagic))
	putParticipantUint32(&buf, careProofVersion)
	writeParticipantLPBytes(&buf, []byte(req.OperationID))
	writeParticipantLPBytes(&buf, req.PayloadHash)
	putParticipantUint32(&buf, uint32(result.Outcome))
	putParticipantUint64(&buf, recordVersion)
	writeParticipantLPBytes(&buf, resultHash[:])
	writeParticipantLPBytes(&buf, snapshotBytes)
	writeParticipantLPBytes(&buf, []byte(result.CareRewardOrderID))
	if result.RewardObligationCreated {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	putParticipantUint32(&buf, result.PlotID)
	writeParticipantLPBytes(&buf, []byte(result.CycleID))
	writeParticipantLPBytes(&buf, []byte(result.IncidentID))
	putParticipantUint32(&buf, uint32(result.CareType))
	putParticipantInt64(&buf, result.ResolvedAtMs)
	putParticipantUint64(&buf, result.ResolvedByPlayerID)
	putParticipantUint64(&buf, result.CommittedPlotVersion)
	putParticipantUint64(&buf, result.CommittedFarmVersion)
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	if len(buf) > careProofMaxBytes {
		return nil, errors.New("farm actor: CARE proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeCareProof(data []byte) (careProof, bool, error) {
	var proof careProof
	if len(data) < 4+len(careProofMagic) || data[0] != 0 || data[1] != 0 || data[2] != 0 || data[3] != byte(len(careProofMagic)) ||
		!bytes.Equal(data[4:4+len(careProofMagic)], []byte(careProofMagic)) {
		return proof, false, nil
	}
	if len(data) == 0 || len(data) > careProofMaxBytes {
		return proof, true, errors.New("farm actor: CARE proof size is invalid")
	}
	reader := participantProofReader{data: data}
	magic, err := reader.lp()
	if err != nil || string(magic) != careProofMagic {
		return proof, true, errors.New("farm actor: CARE proof magic is invalid")
	}
	version, err := reader.u32()
	if err != nil || version != careProofVersion {
		return proof, true, errors.New("farm actor: CARE proof version is invalid")
	}
	operationID, err := reader.lp()
	if err != nil || len(operationID) == 0 {
		return proof, true, errors.New("farm actor: CARE proof operation is invalid")
	}
	payloadHash, err := reader.lp()
	if err != nil || len(payloadHash) != sha256.Size {
		return proof, true, errors.New("farm actor: CARE proof payload hash is invalid")
	}
	outcomeValue, err := reader.u32()
	if err != nil || (outcomeValue != uint32(domain.CareOutcomeResolved) && outcomeValue != uint32(domain.CareOutcomeAlreadyResolved)) {
		return proof, true, errors.New("farm actor: CARE proof outcome is invalid")
	}
	recordVersion, err := reader.u64()
	if err != nil || recordVersion == 0 {
		return proof, true, errors.New("farm actor: CARE proof record version is invalid")
	}
	resultHash, err := reader.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, true, errors.New("farm actor: CARE proof result hash is invalid")
	}
	snapshotBytes, err := reader.lp()
	if err != nil {
		return proof, true, err
	}
	orderID, err := reader.lp()
	if err != nil {
		return proof, true, err
	}
	rewardByte, err := reader.byte()
	if err != nil || rewardByte > 1 {
		return proof, true, errors.New("farm actor: CARE proof reward flag is invalid")
	}
	plotID, err := reader.u32()
	if err != nil {
		return proof, true, err
	}
	cycleID, err := reader.lp()
	if err != nil {
		return proof, true, err
	}
	incidentID, err := reader.lp()
	if err != nil {
		return proof, true, err
	}
	careTypeValue, err := reader.u32()
	if err != nil {
		return proof, true, err
	}
	resolvedAtMs, err := reader.i64()
	if err != nil {
		return proof, true, err
	}
	resolvedBy, err := reader.u64()
	if err != nil {
		return proof, true, err
	}
	committedPlot, err := reader.u64()
	if err != nil {
		return proof, true, err
	}
	committedFarm, err := reader.u64()
	if err != nil {
		return proof, true, err
	}
	headerSource, err := reader.u64()
	if err != nil {
		return proof, true, err
	}
	headerDirty, err := reader.u64()
	if err != nil {
		return proof, true, err
	}
	headerNext, err := reader.i64()
	if err != nil {
		return proof, true, err
	}
	headerMask, err := reader.u32()
	if err != nil || reader.remaining() != 0 {
		return proof, true, errors.New("farm actor: CARE proof header is malformed")
	}
	proof = careProof{
		outcome: domain.CareOutcome(outcomeValue), operationID: string(operationID),
		payloadHash: append([]byte(nil), payloadHash...), recordVersion: recordVersion,
		resultHash: append([]byte(nil), resultHash...), orderID: string(orderID),
		rewardCreated: rewardByte == 1, plotID: plotID, cycleID: string(cycleID), incidentID: string(incidentID),
		careType: domain.CareType(careTypeValue), resolvedAtMs: resolvedAtMs, resolvedByPlayerID: resolvedBy,
		committedPlotVersion: committedPlot, committedFarmVersion: committedFarm,
		header: domain.OpportunityHeader{SourceFarmVersion: headerSource, DirtyGeneration: headerDirty, NextOpportunityAtMs: headerNext, CandidateMask: domain.OpportunityMask(headerMask)},
	}
	expectedResultHash := sha256.Sum256(encodeCareOutcomePayload(proof.orderID, proof.rewardCreated))
	if !bytes.Equal(proof.resultHash, expectedResultHash[:]) {
		return careProof{}, true, errors.New("farm actor: CARE proof result hash binding is invalid")
	}
	if proof.plotID == 0 || proof.cycleID == "" || proof.incidentID == "" || proof.resolvedAtMs <= 0 ||
		proof.resolvedByPlayerID == 0 || proof.committedPlotVersion == 0 || proof.committedFarmVersion == 0 {
		return careProof{}, true, errors.New("farm actor: CARE proof business binding is incomplete")
	}
	switch proof.careType {
	case domain.CareTypeWater, domain.CareTypeRemovePest:
	default:
		return careProof{}, true, errors.New("farm actor: CARE proof type is invalid")
	}
	if len(snapshotBytes) != 0 {
		proof.snapshot = &contractspb.CareResolutionSnapshot{}
		if err := proto.Unmarshal(snapshotBytes, proof.snapshot); err != nil {
			return careProof{}, true, errors.New("farm actor: CARE snapshot is malformed")
		}
		if proof.snapshot.GetSourceFarmId() == 0 || proof.snapshot.GetSourceOwnerPlayerId() != proof.snapshot.GetSourceFarmId() ||
			proof.snapshot.GetCarerPlayerId() == 0 || proof.snapshot.GetIncidentId() != proof.incidentID || proof.snapshot.GetCropCycleId() != proof.cycleID ||
			proof.snapshot.GetPlotId() != proof.plotID || domainCareType(proof.snapshot.GetCareType()) != proof.careType ||
			proof.snapshot.GetCommittedFarmVersion() != proof.committedFarmVersion || proof.snapshot.GetCommittedPlotVersion() != proof.committedPlotVersion ||
			proof.snapshot.GetResolvedAtMs() != proof.resolvedAtMs || proof.snapshot.GetCarerPlayerId() != proof.resolvedByPlayerID ||
			proof.snapshot.GetCareRewardRuleVersion() == 0 || proof.snapshot.GetRewardExp() == 0 ||
			(proof.snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_OWNER && proof.snapshot.GetCarerPlayerId() != proof.snapshot.GetSourceOwnerPlayerId()) ||
			(proof.snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && proof.snapshot.GetCarerPlayerId() == proof.snapshot.GetSourceOwnerPlayerId()) ||
			(proof.snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_OWNER && proof.snapshot.GetFriendInteractionEligible()) ||
			(proof.snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && !proof.snapshot.GetFriendInteractionEligible()) ||
			(proof.snapshot.GetCareContext() != contractspb.CareContext_CARE_CONTEXT_OWNER && proof.snapshot.GetCareContext() != contractspb.CareContext_CARE_CONTEXT_FRIEND) ||
			!bytes.Equal(proof.snapshot.GetCareResolutionHash(), careResolutionHash(proof.snapshot)) {
			return careProof{}, true, errors.New("farm actor: CARE snapshot binding is invalid")
		}
	}
	if proof.outcome == domain.CareOutcomeResolved && (proof.snapshot == nil || !proof.rewardCreated || proof.orderID == "") {
		return careProof{}, true, errors.New("farm actor: resolved CARE proof has no reward order")
	}
	if proof.outcome == domain.CareOutcomeAlreadyResolved && (proof.snapshot != nil || proof.rewardCreated || proof.orderID != "") {
		return careProof{}, true, errors.New("farm actor: NO_CHANGE CARE proof has reward order")
	}
	return proof, true, nil
}

func careResolutionHash(snapshot *contractspb.CareResolutionSnapshot) []byte {
	if snapshot == nil {
		return nil
	}
	withoutHash := proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot)
	withoutHash.CareResolutionHash = nil
	want, err := contracts.CanonicalHash(withoutHash)
	if err != nil {
		return nil
	}
	return want[:]
}

func (r *participantProofReader) byte() (byte, error) {
	if r == nil || r.off >= len(r.data) {
		return 0, errors.New("farm actor: truncated CARE proof")
	}
	value := r.data[r.off]
	r.off++
	return value, nil
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
