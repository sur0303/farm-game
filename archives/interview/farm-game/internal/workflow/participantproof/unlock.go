package participantproof

import (
	"bytes"
	"context"
	"crypto/sha256"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const unlockParticipantMaxPlot = domain.P0MaxPlots

// VerifyUnlockDependency proves the immutable UnlockPlot WorkRecord before a
// Player or Farm participant can mutate its authority. The proof is shared by
// both sides: Player owns the coin reservation, while Farm owns the plot
// decision, but neither side accepts an unproven cross-step result hash.
func (v *Verifier) VerifyUnlockDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if record.Address.WorkType != string(identity.WorkTypeUnlockPlot) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot dependency used for work type %q", record.Address.WorkType)
	}
	if record.Terminal() {
		return proofError(ErrTerminalWork, codesFailedPrecondition, "participant proof: UnlockPlot WorkRecord is terminal")
	}
	if call.ExpectedWorkVersion == 0 || call.ExpectedWorkVersion != record.WorkVersion {
		return proofError(ErrWorkVersionMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot WorkVersion got=%d want=%d", call.ExpectedWorkVersion, record.WorkVersion)
	}
	if err := v.verifyStepBinding(record, call); err != nil {
		return err
	}
	root, err := verifyUnlockRoot(record)
	if err != nil {
		return err
	}
	current := &record.Steps[record.CurrentStep]
	payload, err := decodePersistedStepPayload(current)
	if err != nil {
		return err
	}
	switch payload := payload.(type) {
	case *contractspb.ReserveUnlockCoinPayload:
		return verifyReserveUnlockPayload(root, payload)
	case *contractspb.CommitPlotUnlockPayload:
		return verifyCommitPlotUnlockPayload(record, root, payload)
	case *contractspb.CommitUnlockCoinReservationPayload:
		return verifyCommitUnlockPayload(record, root, payload)
	case *contractspb.ReleaseUnlockCoinReservationPayload:
		return verifyReleaseUnlockPayload(record, root, payload)
	default:
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: current UnlockPlot step payload schema is not registered")
	}
}

func verifyUnlockRoot(record *workflow.WorkRecord) (*contractspb.CommitPlotUnlockPayload, error) {
	if record == nil || record.Address.WorkType != string(identity.WorkTypeUnlockPlot) ||
		len(record.Payload) == 0 || record.PayloadHash == ([32]byte{}) {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot WorkRecord root payload is missing")
	}
	root := &contractspb.CommitPlotUnlockPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, proofErrorWithCause(ErrDependencyMismatch, err, codesFailedPrecondition,
			"participant proof: UnlockPlot root payload cannot be decoded: %v", err)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot root payload is not deterministic")
	}
	rootHash, err := contracts.CanonicalHash(root)
	if err != nil || rootHash != record.PayloadHash {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot root payload hash is not authoritative")
	}
	if root.GetOwnerPlayerId() == 0 || root.GetOwnerPlayerId() != record.SubjectPlayerID ||
		root.GetFarmId() == 0 || root.GetFarmId() != root.GetOwnerPlayerId() ||
		root.GetPlotId() < domain.P0InitialUnlockedPlots+1 || root.GetPlotId() > unlockParticipantMaxPlot ||
		root.GetExpectedPlotVersion() == 0 || root.GetExpectedUnlockRuleVersion() == 0 ||
		root.GetUnlockOrder() != root.GetPlotId() || root.GetReservationId() == "" ||
		root.GetRequiredPlayerLevel() == 0 || root.GetAmountNormalCoin() == 0 ||
		len(root.GetUnlockRuleSnapshotHash()) != sha256.Size || len(root.GetReserveUnlockResultHash()) != 0 {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot root identity or frozen fields are invalid")
	}
	rule := domain.DefaultPlotUnlockRule()
	entry, ok := rule.EntryOf(root.GetPlotId())
	ruleHash := domain.PlotUnlockRuleSnapshotHash(rule)
	if !ok || root.GetExpectedUnlockRuleVersion() != rule.RuleVersion ||
		root.GetRequiredPlayerLevel() != entry.RequiredLevel || root.GetAmountNormalCoin() != entry.NormalCoinPrice ||
		!bytes.Equal(root.GetUnlockRuleSnapshotHash(), ruleHash[:]) {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot root rule projection is not authoritative")
	}
	if root.GetPlotId() == domain.P0InitialUnlockedPlots+1 {
		if root.PredecessorPlotId != nil {
			return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
				"participant proof: UnlockPlot plot 6 predecessor must be absent")
		}
	} else if root.GetPredecessorPlotId() != root.GetPlotId()-1 {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot predecessor is not the fixed previous plot")
	}
	wantReservation, err := workidentity.UnlockCoinReservationID(record.Address.WorkID)
	if err != nil || root.GetReservationId() != wantReservation {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: UnlockPlot reservation identity is invalid")
	}
	return root, nil
}

func verifyReserveUnlockPayload(root *contractspb.CommitPlotUnlockPayload, payload *contractspb.ReserveUnlockCoinPayload) error {
	if root == nil || payload == nil || payload.GetPlayerId() != root.GetOwnerPlayerId() ||
		payload.GetFarmId() != root.GetFarmId() || payload.GetPlotId() != root.GetPlotId() ||
		payload.GetReservationId() != root.GetReservationId() ||
		payload.GetRequiredPlayerLevel() != root.GetRequiredPlayerLevel() ||
		payload.GetAmountNormalCoin() != root.GetAmountNormalCoin() ||
		payload.GetUnlockRuleVersion() != root.GetExpectedUnlockRuleVersion() ||
		!bytes.Equal(payload.GetUnlockRuleSnapshotHash(), root.GetUnlockRuleSnapshotHash()) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: RESERVE_UNLOCK_COIN payload does not match frozen UnlockPlot root")
	}
	return nil
}

func verifyCommitPlotUnlockPayload(record *workflow.WorkRecord, root *contractspb.CommitPlotUnlockPayload, payload *contractspb.CommitPlotUnlockPayload) error {
	if record == nil || root == nil || payload == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: COMMIT_PLOT_UNLOCK payload is incomplete")
	}
	reserveStep, err := findStep(record, string(identity.StepReserveUnlockCoin))
	if err != nil {
		return err
	}
	reserveResult, reserveHash, err := terminalStepResult(record, reserveStep,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		workflow.StepApplied, workflow.StepNoChange)
	if err != nil {
		return err
	}
	reserve, ok := reserveResult.(*contractspb.ReserveUnlockCoinResult)
	if !ok || reserve.GetReservationId() != root.GetReservationId() ||
		reserve.GetReservedAmountNormalCoin() != root.GetAmountNormalCoin() ||
		reserve.GetPlayerLevelAtReserve() < root.GetRequiredPlayerLevel() ||
		!bytes.Equal(payload.GetReserveUnlockResultHash(), reserveHash[:]) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_PLOT_UNLOCK lacks persisted RESERVE_UNLOCK_COIN result proof")
	}
	want := proto.Clone(root).(*contractspb.CommitPlotUnlockPayload)
	want.ReserveUnlockResultHash = append([]byte(nil), reserveHash[:]...)
	if !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK, want, payload) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_PLOT_UNLOCK payload is not the frozen root plus reserve proof")
	}
	return nil
}

func verifyCommitUnlockPayload(record *workflow.WorkRecord, root *contractspb.CommitPlotUnlockPayload, payload *contractspb.CommitUnlockCoinReservationPayload) error {
	if record == nil || root == nil || payload == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: COMMIT_UNLOCK_COIN_RESERVATION payload is incomplete")
	}
	farmStep, err := findStep(record, string(identity.StepCommitPlotUnlock))
	if err != nil {
		return err
	}
	farmResult, farmHash, err := terminalStepResult(record, farmStep,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		workflow.StepApplied)
	if err != nil {
		return err
	}
	result, ok := farmResult.(*contractspb.CommitPlotUnlockResult)
	if !ok || !unlockResultMatchesRoot(root, result) ||
		!bytes.Equal(payload.GetCommitPlotUnlockResultHash(), farmHash[:]) ||
		payload.GetPlayerId() != root.GetOwnerPlayerId() || payload.GetFarmId() != root.GetFarmId() ||
		payload.GetPlotId() != root.GetPlotId() || payload.GetReservationId() != root.GetReservationId() ||
		payload.GetAmountNormalCoin() != root.GetAmountNormalCoin() {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_UNLOCK_COIN_RESERVATION lacks persisted Farm result proof")
	}
	return nil
}

func verifyReleaseUnlockPayload(record *workflow.WorkRecord, root *contractspb.CommitPlotUnlockPayload, payload *contractspb.ReleaseUnlockCoinReservationPayload) error {
	if record == nil || root == nil || payload == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: RELEASE_UNLOCK_COIN_RESERVATION payload is incomplete")
	}
	farmStep, err := findStep(record, string(identity.StepCommitPlotUnlock))
	if err != nil {
		return err
	}
	farmResult, farmHash, err := terminalStepResult(record, farmStep,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		workflow.StepRejected, workflow.StepNoChange)
	if err != nil {
		return err
	}
	result, ok := farmResult.(*contractspb.CommitPlotUnlockResult)
	reason := "FARM_REJECTED"
	if farmStep.Status == workflow.StepNoChange {
		reason = "FARM_NO_CHANGE"
	}
	if !ok || !unlockResultMatchesRoot(root, result) ||
		!bytes.Equal(payload.GetTerminalFarmResultHash(), farmHash[:]) ||
		payload.GetPlayerId() != root.GetOwnerPlayerId() || payload.GetFarmId() != root.GetFarmId() ||
		payload.GetPlotId() != root.GetPlotId() || payload.GetReservationId() != root.GetReservationId() ||
		payload.GetAmountNormalCoin() != root.GetAmountNormalCoin() || payload.GetReleaseReason() != reason {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: RELEASE_UNLOCK_COIN_RESERVATION lacks persisted Farm rejection proof")
	}
	return nil
}

func unlockResultMatchesRoot(root *contractspb.CommitPlotUnlockPayload, result *contractspb.CommitPlotUnlockResult) bool {
	return root != nil && result != nil && result.GetFarmId() == root.GetFarmId() &&
		result.GetPlotId() == root.GetPlotId() &&
		result.GetAppliedUnlockRuleVersion() == root.GetExpectedUnlockRuleVersion() &&
		result.GetCommittedPlotVersion() != 0 && result.GetCommittedFarmVersion() != 0 &&
		len(result.GetUnlockDecisionHash()) == sha256.Size
}
