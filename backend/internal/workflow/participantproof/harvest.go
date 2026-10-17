package participantproof

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	harvestscenario "farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// VerifyCommitHarvestDependency proves the Farm-side first step from the
// durable Harvest root.  The FarmActor remains the authority for crop state;
// this adapter only proves that the typed call is the frozen WorkRecord call.
func (v *Verifier) VerifyCommitHarvestDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if err := v.requireHarvestRecord(record, call, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST); err != nil {
		return err
	}
	if record.CurrentStep != 0 || call.RouteHeader.GetActorId() != record.SubjectPlayerID {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: COMMIT_HARVEST is not bound to the Farm root actor")
	}
	root, err := harvestscenario.DecodeRoot(record)
	if err != nil {
		return harvestDependencyError(err, "Harvest root")
	}
	payload, ok := call.Payload.(*contractspb.CommitHarvestPayload)
	if !ok || !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST, root, payload) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: COMMIT_HARVEST payload is not the frozen Harvest root")
	}
	return nil
}

// VerifyHarvestDependency proves the Farm result that authorizes the Player
// reward.  It intentionally reloads the same WorkRecord used by WorkVersion
// proof and compares the complete canonical Grant payload, so a forged
// reward quantity or decision cannot enter the PlayerActor mailbox.
func (v *Verifier) VerifyHarvestDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if err := v.requireHarvestRecord(record, call, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD); err != nil {
		return err
	}
	if record.CurrentStep != 1 || call.RouteHeader.GetActorId() != record.SubjectPlayerID {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: GRANT_HARVEST_REWARD is not bound to the Player reward step")
	}
	if len(record.Steps) < 2 || record.Steps[0].Name != string(identity.StepCommitHarvest) ||
		record.Steps[0].Status != workflow.StepApplied {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Harvest Farm step is not an applied durable receipt")
	}
	root, err := harvestscenario.DecodeRoot(record)
	if err != nil {
		return harvestDependencyError(err, "Harvest root")
	}
	farmResult, farmHash, err := harvestscenario.DecodeCommitHarvestResult(record)
	if err != nil {
		return harvestDependencyError(err, "COMMIT_HARVEST result")
	}
	if err := verifyHarvestFarmResult(record, root, farmResult); err != nil {
		return err
	}
	payload, ok := call.Payload.(*contractspb.GrantHarvestRewardPayload)
	if !ok {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Grant Harvest payload schema is invalid")
	}
	want := &contractspb.GrantHarvestRewardPayload{
		PlayerId:                root.GetOwnerPlayerId(),
		CommitHarvestResultHash: append([]byte(nil), farmHash[:]...),
		HarvestDecision:         proto.Clone(farmResult.GetHarvestDecision()).(*contractspb.HarvestDecision),
	}
	if !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD, want, payload) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: GRANT_HARVEST_REWARD payload is not the persisted Farm proof")
	}
	return nil
}

func (v *Verifier) requireHarvestRecord(record *workflow.WorkRecord, call *contracts.ParticipantCall, command contractspb.ParticipantCommandType) error {
	if record == nil || record.Address.WorkType != string(identity.WorkTypeHarvest) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Harvest dependency used for a non-Harvest WorkRecord")
	}
	if record.Terminal() {
		return proofError(ErrTerminalWork, codes.FailedPrecondition,
			"participant proof: Harvest WorkRecord is terminal")
	}
	if call == nil || call.ExpectedWorkVersion == 0 || call.ExpectedWorkVersion != record.WorkVersion {
		return proofError(ErrWorkVersionMismatch, codes.FailedPrecondition,
			"participant proof: Harvest dependency WorkVersion does not match")
	}
	if call.Spec.Command != command {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Harvest dependency command does not match")
	}
	if err := v.verifyStepBinding(record, call); err != nil {
		return err
	}
	return nil
}

func verifyHarvestFarmResult(record *workflow.WorkRecord, root *contractspb.CommitHarvestPayload, result *contractspb.CommitHarvestResult) error {
	decision := result.GetHarvestDecision()
	if record == nil || root == nil || result == nil || decision == nil ||
		decision.GetWorkflowId() != record.Address.WorkID ||
		decision.GetOwnerPlayerId() != root.GetOwnerPlayerId() ||
		decision.GetFarmId() != root.GetFarmId() ||
		decision.GetPlotId() != root.GetPlotId() ||
		decision.GetCropCycleId() != root.GetCropCycleId() ||
		decision.GetCropDefinitionId() == 0 || decision.GetRewardItemId() == 0 ||
		decision.GetCropRuleVersion() == 0 || decision.GetCompendiumUnlockSnapshot() == nil ||
		result.GetCommittedPlotVersion() == 0 || result.GetCommittedFarmVersion() == 0 ||
		result.GetFarmRecordVersion() == 0 || len(decision.GetHarvestDecisionHash()) != sha256.Size {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: COMMIT_HARVEST result identity is incomplete")
	}
	snapshot := decision.GetCompendiumUnlockSnapshot()
	if snapshot.GetCropDefinitionId() != decision.GetCropDefinitionId() ||
		snapshot.GetPlantedByPlayerId() != root.GetOwnerPlayerId() {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Harvest compendium snapshot is not Farm-frozen")
	}
	withoutHash := proto.Clone(decision).(*contractspb.HarvestDecision)
	withoutHash.HarvestDecisionHash = nil
	wantHash, err := contracts.CanonicalHash(withoutHash)
	if err != nil || !bytes.Equal(wantHash[:], decision.GetHarvestDecisionHash()) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: HarvestDecision hash is not authoritative")
	}
	return nil
}

func harvestDependencyError(err error, subject string) error {
	return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
		"participant proof: %s is not authoritative: %v", subject, fmt.Sprint(err))
}
