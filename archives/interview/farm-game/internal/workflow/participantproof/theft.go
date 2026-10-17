package participantproof

import (
	"bytes"
	"context"
	"crypto/sha256"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// VerifyDecideTheftDependency proves the Farm-side first step from the
// admitted Pair-backed WorkRecord. Pair ActionAdmission remains the only
// execution acceptance point; this verifier does not re-admit or mutate the
// Pair order, it only proves that the typed Farm call is the exact persisted
// root created from that accepted order.
func (v *Verifier) VerifyDecideTheftDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if record == nil || record.Address.WorkType != string(identity.WorkTypeTheft) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Theft dependency used for a non-Theft WorkRecord")
	}
	if record.Terminal() {
		return proofError(ErrTerminalWork, codes.FailedPrecondition,
			"participant proof: Theft WorkRecord is terminal")
	}
	if record.State != workflow.WorkStateCreated && record.State != workflow.WorkStateRunning && record.State != workflow.WorkStateStepUnknown {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Theft WorkRecord is not dispatchable")
	}
	if call == nil || call.ExpectedWorkVersion == 0 || call.ExpectedWorkVersion != record.WorkVersion {
		return proofError(ErrWorkVersionMismatch, codes.FailedPrecondition,
			"participant proof: Theft Farm WorkVersion does not match")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: Theft Farm command does not match")
	}
	if err := v.verifyStepBinding(record, call); err != nil {
		return err
	}
	root, err := decodeTheftRoot(record)
	if err != nil {
		return err
	}
	if record.CurrentStep != 0 || call.RouteHeader.GetActorId() != root.GetFarmId() {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: DECIDE_THEFT is not bound to the Farm root step")
	}
	payload, ok := call.Payload.(*contractspb.DecideTheftPayload)
	if !ok || !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, root, payload) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition,
			"participant proof: DECIDE_THEFT payload is not the admitted Theft root")
	}
	return nil
}

// VerifyTheftDependency proves either Player-side Theft settlement against
// the same durable WorkRecord. No caller-provided quantity is authoritative:
// the exact FARM_DECIDED TheftTargetDecision is reconstructed from the
// terminal Farm result and the downstream payload is compared canonically.
func (v *Verifier) VerifyTheftDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if record.Address.WorkType != string(identity.WorkTypeTheft) || record.Terminal() {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft WorkRecord is not executable")
	}
	if record.State != workflow.WorkStateRunning && record.State != workflow.WorkStateStepUnknown {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft WorkRecord is not in a dispatchable settlement state")
	}
	if call == nil || call.ExpectedWorkVersion == 0 || call.ExpectedWorkVersion != record.WorkVersion {
		return proofError(ErrWorkVersionMismatch, codes.FailedPrecondition, "participant proof: Theft WorkVersion does not match")
	}
	if call.Spec.WorkType != identity.WorkTypeTheft {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft command has the wrong WorkType")
	}
	if err := v.verifyStepBinding(record, call); err != nil {
		return err
	}
	root, err := decodeTheftRoot(record)
	if err != nil {
		return err
	}
	decisionStep, decisionIndex, err := theftStep(record, identity.StepDecideTheft)
	if err != nil || decisionIndex != 0 || decisionStep.Status != workflow.StepApplied {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: FARM_DECIDED Theft step is not an applied receipt")
	}
	decisionResult, decisionHash, err := terminalStepResult(record, decisionStep, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, workflow.StepApplied)
	if err != nil {
		return err
	}
	decided, ok := decisionResult.(*contractspb.DecideTheftResult)
	if !ok {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: FARM_DECIDED result schema is invalid")
	}
	if err := verifyTheftDecision(record, root, decided.GetTheftTargetDecision()); err != nil {
		return err
	}
	if decided.GetCommittedPlotVersion() == 0 || decided.GetCommittedFarmVersion() == 0 || decided.GetFarmRecordVersion() == 0 {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: FARM_DECIDED Theft result versions are incomplete")
	}

	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR:
		return v.verifyTheftVisitor(record, root, decided, decisionHash, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		return v.verifyTheftOwner(record, root, decided, decisionHash, call)
	default:
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: command is not a Player Theft settlement")
	}
}

func (v *Verifier) verifyTheftVisitor(record *workflow.WorkRecord, root *contractspb.DecideTheftPayload, decided *contractspb.DecideTheftResult, decisionHash [32]byte, call *contracts.ParticipantCall) error {
	if record.CurrentStep != 1 || call.RouteHeader.GetActorId() != root.GetVisitorPlayerId() {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft visitor step is not bound to the visitor")
	}
	payload, ok := call.Payload.(*contractspb.SettleTheftVisitorPayload)
	if !ok {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft visitor payload schema is invalid")
	}
	want := &contractspb.SettleTheftVisitorPayload{
		VisitorPlayerId:       root.GetVisitorPlayerId(),
		DecideTheftResultHash: append([]byte(nil), decisionHash[:]...),
		TheftTargetDecision:   proto.Clone(decided.GetTheftTargetDecision()).(*contractspb.TheftTargetDecision),
	}
	if !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, want, payload) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft visitor payload is not the persisted Farm proof")
	}
	if record.UpdatedAtMs <= 0 {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft resolved time is missing")
	}
	call.TrustedResolvedAtMs = record.UpdatedAtMs
	return nil
}

func (v *Verifier) verifyTheftOwner(record *workflow.WorkRecord, root *contractspb.DecideTheftPayload, decided *contractspb.DecideTheftResult, decisionHash [32]byte, call *contracts.ParticipantCall) error {
	if record.CurrentStep != 2 || call.RouteHeader.GetActorId() != root.GetOwnerPlayerId() || decided.GetTheftTargetDecision().GetGuardCoinValue() == 0 {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft owner step is not a positive guard-income settlement")
	}
	visitorStep, visitorIndex, err := theftStep(record, identity.StepSettleTheftVisitor)
	if err != nil || visitorIndex != 1 || visitorStep.Status != workflow.StepApplied {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft visitor settlement is not an applied receipt")
	}
	visitorResultMessage, visitorHash, err := terminalStepResult(record, visitorStep, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, workflow.StepApplied)
	if err != nil {
		return err
	}
	visitorResult, ok := visitorResultMessage.(*contractspb.SettleTheftVisitorResult)
	if !ok || visitorResult.GetSettlementId() != decided.GetTheftTargetDecision().GetSettlementId() ||
		visitorResult.GetRewardItemId() != decided.GetTheftTargetDecision().GetRewardItemId() ||
		visitorResult.GetGrantedNetQuantity() != decided.GetTheftTargetDecision().GetNetReceivedQuantity() ||
		!visitorResult.GetFriendInteractionFactCommitted() ||
		visitorResult.GetPlayerRecordVersion() == 0 {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft visitor result is not bound to the Farm decision")
	}
	payload, ok := call.Payload.(*contractspb.CreditGuardIncomePayload)
	if !ok {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: guard-income payload schema is invalid")
	}
	want := &contractspb.CreditGuardIncomePayload{
		OwnerPlayerId:           root.GetOwnerPlayerId(),
		DecideTheftResultHash:   append([]byte(nil), decisionHash[:]...),
		SettleVisitorResultHash: append([]byte(nil), visitorHash[:]...),
		TheftTargetDecision:     proto.Clone(decided.GetTheftTargetDecision()).(*contractspb.TheftTargetDecision),
	}
	if !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME, want, payload) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: guard-income payload is not the persisted settlement proof")
	}
	return nil
}

func theftStep(record *workflow.WorkRecord, name identity.ParticipantStep) (*workflow.StepRecord, int, error) {
	if record == nil {
		return nil, -1, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft WorkRecord is missing")
	}
	var found *workflow.StepRecord
	index := -1
	for i := range record.Steps {
		if record.Steps[i].Name != string(name) {
			continue
		}
		if found != nil {
			return nil, -1, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: duplicate Theft step %q", name)
		}
		found = &record.Steps[i]
		index = i
	}
	if found == nil {
		return nil, -1, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft step %q is missing", name)
	}
	return found, index, nil
}

func decodeTheftRoot(record *workflow.WorkRecord) (*contractspb.DecideTheftPayload, error) {
	if record == nil {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft root is missing")
	}
	root := &contractspb.DecideTheftPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft root protobuf is invalid")
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft root bytes are not frozen deterministic protobuf")
	}
	hash, err := contracts.CanonicalHash(root)
	if err != nil || hash != record.PayloadHash {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft root hash is not authoritative")
	}
	permit := root.GetActionPermitSnapshot()
	if root.GetVisitorPlayerId() == 0 || root.GetOwnerPlayerId() == 0 || root.GetVisitorPlayerId() == root.GetOwnerPlayerId() ||
		root.GetFarmId() == 0 || root.GetPlotId() == 0 || root.GetCropCycleId() == "" || root.GetTheftAttemptId() == "" || root.GetSettlementId() == "" ||
		permit == nil || permit.GetPermitId() == "" || permit.GetVisitorPlayerId() != root.GetVisitorPlayerId() ||
		permit.GetOwnerPlayerId() != root.GetOwnerPlayerId() || permit.GetTargetFarmId() != root.GetFarmId() ||
		permit.GetOperationId() != record.OperationID || permit.GetActionScope() != contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT ||
		permit.GetFriendRelationEpoch() == 0 || permit.GetIssuedAtAuthorityMs() <= 0 ||
		permit.GetExpireAtAuthorityMs() <= permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() < permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() >= permit.GetExpireAtAuthorityMs() ||
		len(root.GetActionPermitSnapshotHash()) != sha256.Size || len(permit.GetCanonicalPayloadHash()) != sha256.Size || len(permit.GetTargetBindingHash()) != sha256.Size {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft root or ActionPermit binding is incomplete")
	}
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil || !bytes.Equal(permitHash[:], root.GetActionPermitSnapshotHash()) {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft ActionPermit hash is not authoritative")
	}
	pairID, pairErr := identity.CanonicalFriendRelationID(root.GetVisitorPlayerId(), root.GetOwnerPlayerId())
	if pairErr != nil {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft pair identity is invalid")
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: root.GetVisitorPlayerId(), StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: record.OperationID, Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID}}
	wantWorkflow, err := identity.WorkflowID(key)
	if err != nil || wantWorkflow != record.Address.WorkID {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft workflow identity is not authoritative")
	}
	wantSettlement, settlementErr := identity.TheftSettlementID(record.Address.WorkID)
	wantAttempt, attemptErr := identity.TheftAttemptID(root.GetFarmId(), root.GetCropCycleId(), root.GetVisitorPlayerId())
	if settlementErr != nil || attemptErr != nil || root.GetSettlementId() != wantSettlement || root.GetTheftAttemptId() != wantAttempt {
		return nil, proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: Theft settlement/attempt identity is not derived from Work")
	}
	return root, nil
}

func verifyTheftDecision(record *workflow.WorkRecord, root *contractspb.DecideTheftPayload, decision *contractspb.TheftTargetDecision) error {
	if record == nil || root == nil || decision == nil || decision.GetWorkflowId() != record.Address.WorkID ||
		decision.GetSettlementId() != root.GetSettlementId() || decision.GetTheftAttemptId() != root.GetTheftAttemptId() ||
		decision.GetVisitorPlayerId() != root.GetVisitorPlayerId() || decision.GetOwnerPlayerId() != root.GetOwnerPlayerId() ||
		decision.GetFarmId() != root.GetFarmId() || decision.GetPlotId() != root.GetPlotId() || decision.GetCropCycleId() != root.GetCropCycleId() ||
		decision.GetRewardItemId() == 0 || decision.GetCropRuleVersion() == 0 || len(decision.GetTheftDecisionHash()) != sha256.Size {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: FARM_DECIDED Theft identity is incomplete")
	}
	if decision.GetGuardLostQuantity() > decision.GetGrossStolenQuantity() || decision.GetNetReceivedQuantity() != decision.GetGrossStolenQuantity()-decision.GetGuardLostQuantity() ||
		decision.GetGuardLostQuantity() > 0 && decision.GetCropUnitCoinValue() > ^uint64(0)/decision.GetGuardLostQuantity() ||
		decision.GetGuardCoinValue() != decision.GetGuardLostQuantity()*decision.GetCropUnitCoinValue() {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: FARM_DECIDED Theft quantity or coin relation is inconsistent")
	}
	withoutHash := proto.Clone(decision).(*contractspb.TheftTargetDecision)
	withoutHash.TheftDecisionHash = nil
	wantHash, err := contracts.CanonicalHash(withoutHash)
	if err != nil || !bytes.Equal(wantHash[:], decision.GetTheftDecisionHash()) {
		return proofError(ErrDependencyMismatch, codes.FailedPrecondition, "participant proof: TheftTargetDecision hash is not authoritative")
	}
	return nil
}
