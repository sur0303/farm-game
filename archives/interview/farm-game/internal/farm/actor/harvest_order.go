package actor

import (
	"context"
	"fmt"
	"strconv"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

const harvestOrderType = string(identity.WorkTypeHarvest)

// HarvestOwnerRequest is the public HARVEST FirstHop once HARVEST_SOURCE_ORDER
// is on. Farm is the source: one Lua writes the empty plot, receipt, GRANT
// order and DueIndex. Player grant is Direct/worker, never a WorkRecord step.
type HarvestOwnerRequest struct {
	OperationID         string
	PayloadHash         []byte
	OwnerPlayerID       uint64
	PlotID              uint32
	CycleID             string
	ExpectedPlotVersion uint64
	TrustedNowMs        int64
	// WantSourceCommitEvidence opts into the expensive exact commit carrier
	// used only by explicit shadow/prefer proof modes. The zero value is off.
	WantSourceCommitEvidence bool
}

// HarvestSourceOrderID is the deterministic SourceOrder identity used by
// ingress Direct enqueue. Fact version is frozen at 1 so poll/recovery can
// recompute it from (farm, operation) without a second farm-version lookup.
func HarvestSourceOrderID(farmID uint64, operationID string) (string, error) {
	return identity.SourceOrderID(identity.WorkTypeHarvest, identity.SourceKindFarmActor,
		strconv.FormatUint(farmID, 10), operationID, 1)
}

// HarvestOwner is the production FarmActor HARVEST turn under SourceOrder.
func (a *FarmActor) HarvestOwner(ctx context.Context, req HarvestOwnerRequest) (*ActionResult, error) {
	if a == nil || a.record == nil || a.record.Core == nil {
		return nil, farmError(commonpb.ErrorCode_ERR_INTERNAL, "farm actor: HARVEST record is unavailable")
	}
	if req.OperationID == "" || len(req.PayloadHash) != 32 || req.OwnerPlayerID == 0 ||
		req.PlotID == 0 || req.CycleID == "" || req.TrustedNowMs <= 0 {
		return nil, farmError(commonpb.ErrorCode_ERR_DECODE, "HARVEST request is incomplete")
	}
	if req.OwnerPlayerID != a.farmID {
		return nil, farmError(commonpb.ErrorCode_ERR_PERMISSION_DENIED, "只能收获自己的农场")
	}
	if replay, err := a.replayCommand(req.OperationID, req.PayloadHash); replay != nil || err != nil {
		return replay, err
	}
	if err := a.ensureParticipantCycles(); err != nil {
		return nil, err
	}
	if err := a.ensureReceiptCapacity(time.UnixMilli(req.TrustedNowMs)); err != nil {
		return nil, err
	}

	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	state := a.buildParticipantState()
	if err := requireParticipantOpportunityContinuity(a.record, &state); err != nil {
		return nil, err
	}

	plotVersion := currentParticipantPlotVersion(&state, req.PlotID)
	var domainErr *domain.Error
	var decision domain.HarvestDecision
	var domainCandidate domain.CandidateState
	candidate := state
	if plot := state.FindPlot(req.PlotID); plot != nil && req.ExpectedPlotVersion != 0 &&
		req.ExpectedPlotVersion != plot.PlotVersion &&
		!participantHarvestMayUseTheftAdvancedPlot(a.record, &state, &contractspb.CommitHarvestPayload{
			OwnerPlayerId: req.OwnerPlayerID, FarmId: a.farmID, PlotId: req.PlotID, CropCycleId: req.CycleID,
			ExpectedPlotVersion: req.ExpectedPlotVersion,
		}, plot.PlotVersion) {
		domainErr = &domain.Error{Status: domain.StatusRejected, Code: domain.CodeStalePlotVersion,
			Message: fmt.Sprintf("plot %d version %d, expected %d", req.PlotID, plot.PlotVersion, req.ExpectedPlotVersion)}
	} else {
		domainCandidate, decision, domainErr = domain.DecideHarvest(state, domain.HarvestCommand{
			PlayerID: req.OwnerPlayerID, PlotID: req.PlotID, CycleID: req.CycleID,
		}, req.TrustedNowMs)
	}
	if domainErr != nil {
		a.record = before
		if domainErr.Status == domain.StatusNoChange {
			result := &ActionResult{
				Status:          commonpb.CommandStatus_NO_CHANGE,
				Result:          &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(req.TrustedNowMs))},
				ActivationEpoch: a.activationEpoch,
			}
			if err := a.putReceiptWithOpportunityHeader(req.OperationID, req.PayloadHash, commonpb.CommandStatus_NO_CHANGE,
				result.Result, state.Header, time.UnixMilli(req.TrustedNowMs)); err != nil {
				return nil, err
			}
			if err := a.save(ctx, before, req.OperationID, commonpb.CommandType_HARVEST); err != nil {
				return nil, err
			}
			return result, nil
		}
		return nil, mapParticipantHarvestDomainErrorAsFarm(domainErr)
	}
	candidate = domainCandidate.State

	frozen := a.participantCycles[req.CycleID]
	if frozen == nil || frozen.rule == nil || frozen.compendium == nil {
		a.record = before
		return nil, farmError(commonpb.ErrorCode_ERR_INTERNAL, "HARVEST frozen CropCycle facts are missing")
	}
	cycle := state.FindCycle(req.CycleID)
	if cycle == nil {
		a.record = before
		return nil, farmError(commonpb.ErrorCode_ERR_INTERNAL, "HARVEST CropCycle is missing")
	}
	orderID, err := HarvestSourceOrderID(a.farmID, req.OperationID)
	if err != nil {
		a.record = before
		return nil, err
	}
	decisionResult := &contractspb.HarvestDecision{
		WorkflowId: orderID, OwnerPlayerId: req.OwnerPlayerID, FarmId: a.farmID,
		PlotId: req.PlotID, CropCycleId: req.CycleID,
		CropDefinitionId: cycle.CropDefinitionID, RewardItemId: frozen.rule.GetHarvestItemId(),
		RewardQuantity: uint64(decision.RewardQuantity), RewardExp: decision.FrozenHarvestExp,
		CropRuleVersion: cycle.RuleVersion, CompendiumUnlockSnapshot: proto.Clone(frozen.compendium).(*contractspb.CompendiumUnlockSnapshot),
	}
	decisionHash, err := harvestDecisionHash(decisionResult)
	if err != nil {
		a.record = before
		return nil, err
	}
	decisionResult.HarvestDecisionHash = append([]byte(nil), decisionHash[:]...)

	commitResult := &contractspb.CommitHarvestResult{
		CommittedPlotVersion: decision.CommittedPlotVersion,
		CommittedFarmVersion: decision.CommittedFarmVersion,
		FarmRecordVersion:    a.recordVersion + 1,
		HarvestDecision:      decisionResult,
	}
	spec, specErr := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if specErr != nil {
		a.record = before
		return nil, specErr
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, commitResult)
	if err != nil {
		a.record = before
		return nil, err
	}
	order, err := buildHarvestRewardOrder(req.OwnerPlayerID, orderID, req.OperationID, resultHash[:], decisionResult, req.TrustedNowMs)
	if err != nil {
		a.record = before
		return nil, err
	}

	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	if candidateRecord.Core == nil {
		candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
	}
	publicPlot := participantPublicPlot(candidateRecord, req.PlotID)
	if publicPlot == nil {
		publicIndex, ok := participantPublicPlotIndex(req.PlotID)
		if !ok {
			a.record = before
			return nil, farmError(commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, "plot %d", req.PlotID)
		}
		publicPlot = &farmpb.Plot{Index: publicIndex, PlotVersion: plotVersion}
		candidateRecord.Core.Plots = append(candidateRecord.Core.Plots, publicPlot)
	}
	publicPlot.ActiveCropCycleId = ""
	publicPlot.PlotVersion = decision.CommittedPlotVersion
	removeParticipantPublicCycle(candidateRecord, req.CycleID)
	candidateRecord.Core.FarmVersion = decision.CommittedFarmVersion
	if candidate.Header.SourceFarmVersion != candidateRecord.Core.FarmVersion || candidate.Header.DirtyGeneration == 0 {
		a.record = before
		return nil, farmError(commonpb.ErrorCode_ERR_INTERNAL, "HARVEST candidate Opportunity header is invalid")
	}
	a.record = candidateRecord
	if err := a.putReceiptWithOpportunityHeader(req.OperationID, req.PayloadHash, commonpb.CommandStatus_SUCCEEDED,
		&farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(req.TrustedNowMs))}, candidate.Header,
		time.UnixMilli(req.TrustedNowMs)); err != nil {
		a.record = before
		return nil, err
	}
	committedChange := &farmpb.FarmChange{
		OwnerId: a.farmID, FarmVersion: candidateRecord.Core.FarmVersion,
		ChangedPlots: []*farmpb.PlotView{plotView(candidateRecord, publicPlot, time.UnixMilli(req.TrustedNowMs))},
	}
	var commitEvidence *authoritystore.ExactCommitEvidence
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	if req.WantSourceCommitEvidence {
		commitEvidence, err = a.saveWithCandidateAndOutcomeAndOrdersExact(ctx, before, req.OperationID, commonpb.CommandType_HARVEST, committedChange, outcome, []authoritystore.AttachedOrder{order})
	} else {
		err = a.saveWithCandidateAndOutcomeAndOrders(ctx, before, req.OperationID, commonpb.CommandType_HARVEST, committedChange, outcome, []authoritystore.AttachedOrder{order})
	}
	if err != nil {
		return nil, err
	}
	a.participantState = &participantFarmState{state: candidate}
	delete(a.participantCycles, req.CycleID)
	change, recipients, resyncs, _ := a.publishCandidate(publicPlot, time.UnixMilli(req.TrustedNowMs))
	if change == nil {
		change = committedChange
	}
	harvested := decision.RewardQuantity
	committedOrder := order
	committedOrder.Payload = append([]byte(nil), order.Payload...)
	return &ActionResult{
		Status: commonpb.CommandStatus_SUCCEEDED,
		Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(req.TrustedNowMs))},
		Change: change, FriendOpportunityInvalidated: true, Recipients: recipients,
		ActivationEpoch: a.activationEpoch, Resyncs: resyncs,
		HarvestOrderID: orderID, CommittedSourceOrder: &committedOrder,
		SourceCommitEvidence: commitEvidence,
		HarvestPublic: &publicpb.HarvestResult{
			TargetFarmId:         a.farmID,
			PlotId:               req.PlotID - 1,
			CropCycleId:          req.CycleID,
			CommittedFarmVersion: decision.CommittedFarmVersion,
			HarvestedQuantity:    harvested,
		},
	}, nil
}

func buildHarvestRewardOrder(playerID uint64, orderID, operationID string, farmResultHash []byte, decision *contractspb.HarvestDecision, dueAtMs int64) (authoritystore.AttachedOrder, error) {
	payloadMessage := &contractspb.GrantHarvestRewardPayload{
		PlayerId:                playerID,
		CommitHarvestResultHash: append([]byte(nil), farmResultHash...),
		HarvestDecision:         proto.Clone(decision).(*contractspb.HarvestDecision),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payloadMessage)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	payloadHash, err := contracts.CanonicalParticipantHash(spec, payloadMessage)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	return authoritystore.AttachedOrder{
		OrderID: orderID, OrderType: harvestOrderType, Payload: payload,
		PayloadHash: authoritystore.PayloadHash(payloadHash), DueAtMs: dueAtMs,
		OperationID: operationID, SubjectPlayerID: playerID,
	}, nil
}

func mapParticipantHarvestDomainErrorAsFarm(err *domain.Error) error {
	if err == nil {
		return nil
	}
	return farmError(mapParticipantHarvestDomainError(err), "%s", err.Message)
}
