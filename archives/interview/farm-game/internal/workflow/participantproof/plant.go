package participantproof

import (
	"bytes"
	"context"
	"crypto/sha256"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const plantQuantity uint32 = 1

// VerifyPlantDependency proves the durable dependency chain for Player-side
// Plant steps: RESERVE_SEED, COMMIT_SEED_RESERVATION and
// RELEASE_SEED_RESERVATION. It is suitable for player/participant.Options.
func (v *Verifier) VerifyPlantDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return v.verifyPlantDependency(ctx, call)
}

// VerifyCommitPlantDependency proves the Farm-side COMMIT_PLANT dependency
// from the same WorkRecord. It is suitable for farm/ingress.Options.
func (v *Verifier) VerifyCommitPlantDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return v.verifyPlantDependency(ctx, call)
}

func (v *Verifier) verifyPlantDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if record.Address.WorkType != "PLANT" {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant dependency used for work type %q", record.Address.WorkType)
	}
	if record.Terminal() {
		return proofError(ErrTerminalWork, codesFailedPrecondition, "participant proof: Plant WorkRecord is terminal")
	}
	if call.ExpectedWorkVersion == 0 || call.ExpectedWorkVersion != record.WorkVersion {
		return proofError(ErrWorkVersionMismatch, codesFailedPrecondition,
			"participant proof: Plant dependency WorkVersion got=%d want=%d", call.ExpectedWorkVersion, record.WorkVersion)
	}
	if err := v.verifyStepBinding(record, call); err != nil {
		return err
	}
	if err := verifyPlantRoot(record); err != nil {
		return err
	}
	root := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return proofErrorWithCause(ErrDependencyMismatch, err, codesFailedPrecondition,
			"participant proof: Plant root payload cannot be decoded: %v", err)
	}
	current := &record.Steps[record.CurrentStep]
	stepPayload, err := decodePersistedStepPayload(current)
	if err != nil {
		return err
	}
	switch payload := stepPayload.(type) {
	case *contractspb.ReserveSeedPayload:
		return verifyReserveSeedPayload(root, payload)
	case *contractspb.CommitPlantPayload:
		return verifyCommitPlantPayload(record, root, payload)
	case *contractspb.CommitSeedReservationPayload:
		return verifyCommitSeedReservationPayload(record, root, payload)
	case *contractspb.ReleaseSeedReservationPayload:
		return verifyReleaseSeedReservationPayload(record, root, payload)
	default:
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: current Plant step payload schema is not registered")
	}
}

// codesFailedPrecondition is kept local to make the dependency checks read as
// proof failures without importing gRPC status at every branch.
const codesFailedPrecondition = codes.FailedPrecondition

func verifyPlantRoot(record *workflow.WorkRecord) error {
	if record == nil || record.Address.WorkType != "PLANT" || len(record.Payload) == 0 || record.PayloadHash == ([32]byte{}) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant WorkRecord root payload is missing")
	}
	root := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return proofErrorWithCause(ErrDependencyMismatch, err, codesFailedPrecondition,
			"participant proof: Plant root payload is not valid protobuf: %v", err)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant root payload is not deterministic")
	}
	rootHash, err := contracts.CanonicalHash(root)
	if err != nil || rootHash != record.PayloadHash {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant root payload hash is not authoritative")
	}
	if root.GetOwnerPlayerId() == 0 || root.GetOwnerPlayerId() != record.SubjectPlayerID ||
		root.GetFarmId() == 0 || root.GetFarmId() != root.GetOwnerPlayerId() ||
		root.GetPlotId() == 0 || root.GetExpectedPlotVersion() == 0 ||
		root.GetReservationId() == "" || root.GetCropCycleId() == "" ||
		root.GetCropRuleSnapshot() == nil || len(root.GetCropRuleSnapshotHash()) != sha256.Size ||
		len(root.GetReserveSeedResultHash()) != 0 {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant root identity or frozen dependency fields are invalid")
	}
	snapshotHash, err := contracts.CanonicalHash(root.GetCropRuleSnapshot())
	if err != nil || !bytes.Equal(snapshotHash[:], root.GetCropRuleSnapshotHash()) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant CropRuleSnapshot hash is invalid")
	}
	wantReservation, err := workidentity.SeedReservationID(record.Address.WorkID)
	if err != nil || wantReservation != root.GetReservationId() {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant reservation identity is invalid")
	}
	wantCycle, err := workidentity.CropCycleID(record.Address.WorkID)
	if err != nil || wantCycle != root.GetCropCycleId() {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant crop-cycle identity is invalid")
	}
	return nil
}

func decodePersistedStepPayload(step *workflow.StepRecord) (proto.Message, error) {
	if step == nil || len(step.Payload) == 0 || step.PayloadHash == ([32]byte{}) {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant StepRecord payload is missing")
	}
	if sha256.Sum256(step.Payload) != step.PayloadHash {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant StepRecord payload hash is corrupt")
	}
	spec, err := participantSpecForStep(step.Name)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(step.Payload, payload); err != nil {
		return nil, proofErrorWithCause(ErrDependencyMismatch, err, codesFailedPrecondition,
			"participant proof: Plant StepRecord payload is not valid protobuf: %v", err)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, step.Payload) {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant StepRecord payload is not deterministic")
	}
	return payload, nil
}

func verifyReserveSeedPayload(root *contractspb.CommitPlantPayload, payload *contractspb.ReserveSeedPayload) error {
	if root == nil || payload == nil || payload.GetPlayerId() != root.GetOwnerPlayerId() ||
		payload.GetFarmId() != root.GetFarmId() || payload.GetPlotId() != root.GetPlotId() ||
		payload.GetReservationId() != root.GetReservationId() ||
		payload.GetCropDefinitionId() != root.GetCropRuleSnapshot().GetCropDefinitionId() ||
		payload.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() ||
		payload.GetQuantity() != plantQuantity ||
		payload.GetCropRuleVersion() != root.GetCropRuleSnapshot().GetCropRuleVersion() ||
		!bytes.Equal(payload.GetCropRuleSnapshotHash(), root.GetCropRuleSnapshotHash()) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: RESERVE_SEED payload does not match persisted Plant root")
	}
	return nil
}

func verifyCommitPlantPayload(record *workflow.WorkRecord, root, payload *contractspb.CommitPlantPayload) error {
	if root == nil || payload == nil || record == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: COMMIT_PLANT payload is incomplete")
	}
	reserveStep, err := findStep(record, "RESERVE_SEED")
	if err != nil {
		return err
	}
	reserveResult, reserveHash, err := terminalStepResult(record, reserveStep, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		workflow.StepApplied, workflow.StepNoChange)
	if err != nil {
		return err
	}
	reserve, ok := reserveResult.(*contractspb.ReserveSeedResult)
	if !ok || reserve.GetReservationId() != root.GetReservationId() ||
		reserve.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() || reserve.GetReservedQuantity() != plantQuantity ||
		!bytes.Equal(payload.GetReserveSeedResultHash(), reserveHash[:]) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_PLANT lacks the persisted RESERVE_SEED result proof")
	}
	want := proto.Clone(root).(*contractspb.CommitPlantPayload)
	want.ReserveSeedResultHash = append([]byte(nil), reserveHash[:]...)
	if !sameParticipantPayload(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, want, payload) {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_PLANT payload is not the frozen Plant root plus RESERVE_SEED proof")
	}
	return nil
}

func verifyCommitSeedReservationPayload(record *workflow.WorkRecord, root *contractspb.CommitPlantPayload, payload *contractspb.CommitSeedReservationPayload) error {
	if root == nil || payload == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: COMMIT_SEED_RESERVATION payload is incomplete")
	}
	farmStep, err := findStep(record, "COMMIT_PLANT")
	if err != nil {
		return err
	}
	farmResult, farmHash, err := terminalStepResult(record, farmStep, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		workflow.StepApplied, workflow.StepNoChange)
	if err != nil {
		return err
	}
	result, ok := farmResult.(*contractspb.CommitPlantResult)
	if !ok || !plantResultMatchesRoot(root, result) || !bytes.Equal(payload.GetCommitPlantResultHash(), farmHash[:]) ||
		payload.GetPlayerId() != root.GetOwnerPlayerId() || payload.GetFarmId() != root.GetFarmId() ||
		payload.GetPlotId() != root.GetPlotId() || payload.GetCropCycleId() != root.GetCropCycleId() ||
		payload.GetReservationId() != root.GetReservationId() {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: COMMIT_SEED_RESERVATION lacks the persisted Farm result proof")
	}
	return nil
}

func verifyReleaseSeedReservationPayload(record *workflow.WorkRecord, root *contractspb.CommitPlantPayload, payload *contractspb.ReleaseSeedReservationPayload) error {
	if root == nil || payload == nil {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: RELEASE_SEED_RESERVATION payload is incomplete")
	}
	farmStep, err := findStep(record, "COMMIT_PLANT")
	if err != nil {
		return err
	}
	farmResult, farmHash, err := terminalStepResult(record, farmStep, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		workflow.StepRejected)
	if err != nil {
		return err
	}
	result, ok := farmResult.(*contractspb.CommitPlantResult)
	if !ok || !plantResultMatchesRoot(root, result) || !bytes.Equal(payload.GetTerminalFarmResultHash(), farmHash[:]) ||
		payload.GetPlayerId() != root.GetOwnerPlayerId() || payload.GetFarmId() != root.GetFarmId() ||
		payload.GetPlotId() != root.GetPlotId() || payload.GetReservationId() != root.GetReservationId() ||
		payload.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() ||
		payload.GetQuantity() != plantQuantity || payload.GetReleaseReason() != "FARM_REJECTED" {
		return proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: RELEASE_SEED_RESERVATION lacks the persisted rejected Farm result proof")
	}
	return nil
}

func findStep(record *workflow.WorkRecord, name string) (*workflow.StepRecord, error) {
	if record == nil {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition, "participant proof: WorkRecord is missing")
	}
	var found *workflow.StepRecord
	for index := range record.Steps {
		step := &record.Steps[index]
		if step.Name != name {
			continue
		}
		if found != nil {
			return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
				"participant proof: Plant WorkRecord contains duplicate step %q", name)
		}
		found = step
	}
	if found == nil {
		return nil, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: Plant WorkRecord step %q is missing", name)
	}
	return found, nil
}

func terminalStepResult(record *workflow.WorkRecord, step *workflow.StepRecord, command contractspb.ParticipantCommandType, allowed ...workflow.StepStatus) (proto.Message, [32]byte, error) {
	var zero [32]byte
	if record == nil || step == nil {
		return nil, zero, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: dependency StepRecord is missing")
	}
	statusAllowed := false
	for _, candidate := range allowed {
		if step.Status == candidate {
			statusAllowed = true
			break
		}
	}
	if !statusAllowed || !workflow.StepTerminal(step.Status) || len(step.Result) == 0 || step.ResultHash == zero {
		return nil, zero, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: dependency step %q has no allowed durable terminal receipt", step.Name)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return nil, zero, err
	}
	result := spec.NewResult()
	if err := proto.Unmarshal(step.Result, result); err != nil {
		return nil, zero, proofErrorWithCause(ErrDependencyMismatch, err, codesFailedPrecondition,
			"participant proof: dependency step %q result is not valid protobuf: %v", step.Name, err)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil || !bytes.Equal(deterministic, step.Result) {
		return nil, zero, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: dependency step %q result bytes are not deterministic", step.Name)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil || hash != step.ResultHash {
		return nil, zero, proofError(ErrDependencyMismatch, codesFailedPrecondition,
			"participant proof: dependency step %q result hash is not authoritative", step.Name)
	}
	return result, hash, nil
}

func plantResultMatchesRoot(root *contractspb.CommitPlantPayload, result *contractspb.CommitPlantResult) bool {
	return root != nil && result != nil && result.GetFarmId() == root.GetFarmId() &&
		result.GetPlotId() == root.GetPlotId() && result.GetCropCycleId() == root.GetCropCycleId() &&
		result.GetCropDefinitionId() == root.GetCropRuleSnapshot().GetCropDefinitionId() &&
		result.GetAppliedCropRuleVersion() == root.GetCropRuleSnapshot().GetCropRuleVersion()
}

func sameParticipantPayload(command contractspb.ParticipantCommandType, left, right proto.Message) bool {
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return false
	}
	leftHash, err := contracts.CanonicalParticipantHash(spec, left)
	if err != nil {
		return false
	}
	rightHash, err := contracts.CanonicalParticipantHash(spec, right)
	return err == nil && leftHash == rightHash
}
