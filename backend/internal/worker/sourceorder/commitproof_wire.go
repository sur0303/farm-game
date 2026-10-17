package sourceorder

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

func CommitProofToProto(proof CommitProof) (*contractspb.SourceOrderCommitProof, error) {
	if !validProofToken(proof.KeyID, 128) || len(proof.Signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: wire envelope", ErrCommitProofInvalid)
	}
	if err := validateCommitProofStatement(proof.Statement); err != nil {
		return nil, err
	}
	work, err := commitProofWorkToProto(proof.Statement.Work, proof.Statement.AuthorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	destinationDomain, err := commitProofRouteToProto(proof.Statement.DestinationRouteDomain)
	if err != nil {
		return nil, err
	}
	destinationStep, err := commitProofStepToProto(proof.Statement.DestinationStep)
	if err != nil {
		return nil, err
	}
	statement := proof.Statement
	return &contractspb.SourceOrderCommitProof{
		SchemaVersion: CommitProofSchemaVersion,
		Statement: &contractspb.SourceOrderCommitProofStatement{
			Work: work, WorkVersion: statement.WorkVersion, OperationId: statement.OperationID,
			SubjectPlayerId: statement.SubjectPlayerID, PayloadHash: append([]byte(nil), statement.PayloadHash[:]...),
			DueAtMs: statement.DueAtMs, SourceRealm: statement.SourceRealm,
			AuthorityLayoutVersion: statement.AuthorityLayoutVersion,
			SourceHolderId:         statement.SourceHolderID, SourceHolderBootId: statement.SourceHolderBootID,
			AssignmentRevision: statement.AssignmentRevision, WriterEpoch: statement.WriterEpoch,
			LeaseEpoch: statement.LeaseEpoch, LeaseUntilMs: statement.LeaseUntilMs,
			SourceRecordVersion: statement.SourceRecordVersion, CommittedAtMs: statement.CommittedAtMs,
			ExpiresAtMs: statement.ExpiresAtMs, DestinationRouteDomain: destinationDomain,
			DestinationActorId: statement.DestinationActorID, DestinationStep: destinationStep,
			DestinationInvocationId: statement.DestinationInvocationID,
		},
		KeyId: proof.KeyID, Signature: append([]byte(nil), proof.Signature...),
	}, nil
}

func CommitProofFromProto(wire *contractspb.SourceOrderCommitProof) (CommitProof, error) {
	if wire == nil {
		return CommitProof{}, ErrCommitProofMissing
	}
	if wire.GetSchemaVersion() != CommitProofSchemaVersion || wire.GetStatement() == nil ||
		!validProofToken(wire.GetKeyId(), 128) || len(wire.GetSignature()) != ed25519.SignatureSize {
		return CommitProof{}, fmt.Errorf("%w: wire envelope", ErrCommitProofInvalid)
	}
	input := wire.GetStatement()
	work, err := commitProofWorkFromProto(input.GetWork())
	if err != nil {
		return CommitProof{}, err
	}
	if input.GetWork().GetSourceAuthority().GetAuthorityLayoutVersion() != input.GetAuthorityLayoutVersion() {
		return CommitProof{}, fmt.Errorf("%w: wire authority layout", ErrCommitProofInvalid)
	}
	if len(input.GetPayloadHash()) != sha256.Size {
		return CommitProof{}, fmt.Errorf("%w: wire payload hash", ErrCommitProofInvalid)
	}
	destinationDomain, err := commitProofRouteFromProto(input.GetDestinationRouteDomain())
	if err != nil {
		return CommitProof{}, err
	}
	destinationStep, err := commitProofStepFromProto(input.GetDestinationStep())
	if err != nil {
		return CommitProof{}, err
	}
	var payloadHash [sha256.Size]byte
	copy(payloadHash[:], input.GetPayloadHash())
	statement := CommitProofStatement{
		Work: work, WorkVersion: input.GetWorkVersion(), OperationID: input.GetOperationId(),
		SubjectPlayerID: input.GetSubjectPlayerId(), PayloadHash: payloadHash, DueAtMs: input.GetDueAtMs(),
		SourceRealm: input.GetSourceRealm(), AuthorityLayoutVersion: input.GetAuthorityLayoutVersion(),
		SourceHolderID: input.GetSourceHolderId(), SourceHolderBootID: input.GetSourceHolderBootId(),
		AssignmentRevision: input.GetAssignmentRevision(), WriterEpoch: input.GetWriterEpoch(),
		LeaseEpoch: input.GetLeaseEpoch(), LeaseUntilMs: input.GetLeaseUntilMs(),
		SourceRecordVersion: input.GetSourceRecordVersion(), CommittedAtMs: input.GetCommittedAtMs(),
		ExpiresAtMs: input.GetExpiresAtMs(), DestinationRouteDomain: destinationDomain,
		DestinationActorID: input.GetDestinationActorId(), DestinationStep: destinationStep,
		DestinationInvocationID: input.GetDestinationInvocationId(),
	}
	if err := validateCommitProofStatement(statement); err != nil {
		return CommitProof{}, err
	}
	return CommitProof{Statement: statement, KeyID: wire.GetKeyId(), Signature: append([]byte(nil), wire.GetSignature()...)}, nil
}

// CommitProofExpectedFromParticipantCall reconstructs every destination-owned
// binding from the already validated Participant call. It never copies a
// holder, fence, due time, commit version, or expiry from the proof.
func CommitProofExpectedFromParticipantCall(call *contracts.ParticipantCall, sourceRealm string) (CommitProofExpected, error) {
	if call == nil || call.CallerKind != contracts.ParticipantCallerCoordinator || call.Work == nil ||
		call.RouteHeader == nil || !validProofToken(sourceRealm, 128) {
		return CommitProofExpected{}, fmt.Errorf("%w: Participant call", ErrCommitProofInvalid)
	}
	work, err := commitProofWorkFromProto(call.Work)
	if err != nil {
		return CommitProofExpected{}, err
	}
	layout := call.Work.GetSourceAuthority().GetAuthorityLayoutVersion()
	destinationDomain, err := commitProofRouteFromProto(call.RouteHeader.GetRouteDomain())
	if err != nil {
		return CommitProofExpected{}, err
	}
	if work.SourceRouteDomain != workidentity.RouteDomainFarm || destinationDomain != workidentity.RouteDomainPlayer ||
		call.RouteHeader.GetActorId() == 0 || call.RouteHeader.GetActorId() != call.SubjectPlayerID ||
		call.ExpectedWorkVersion != 1 || !validProofToken(call.OperationID, 128) ||
		call.CanonicalStepPayloadHash == ([sha256.Size]byte{}) || !validProofToken(layout, 64) ||
		!validProofToken(call.ParticipantStep, 64) || !validProofToken(call.InvocationID, 128) {
		return CommitProofExpected{}, fmt.Errorf("%w: Participant binding", ErrCommitProofInvalid)
	}
	switch work.WorkType {
	case string(identity.WorkTypeCareReward):
		payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
		if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD ||
			call.ParticipantStep != string(identity.StepApplyCareReward) || !ok || payload == nil ||
			payload.GetCareResolutionSnapshot() == nil || payload.GetCareResolutionSnapshot().GetSourceFarmId() != work.SourceActorID {
			return CommitProofExpected{}, fmt.Errorf("%w: CARE Participant binding", ErrCommitProofInvalid)
		}
	case string(identity.WorkTypeHarvest):
		payload, ok := call.Payload.(*contractspb.GrantHarvestRewardPayload)
		if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD ||
			call.ParticipantStep != string(identity.StepGrantHarvestReward) || !ok || payload == nil ||
			payload.GetHarvestDecision() == nil || payload.GetHarvestDecision().GetFarmId() != work.SourceActorID {
			return CommitProofExpected{}, fmt.Errorf("%w: HARVEST Participant binding", ErrCommitProofInvalid)
		}
	default:
		return CommitProofExpected{}, fmt.Errorf("%w: unsupported Participant work", ErrCommitProofInvalid)
	}
	return CommitProofExpected{
		Work: work, WorkVersion: call.ExpectedWorkVersion, OperationID: call.OperationID,
		SubjectPlayerID: call.SubjectPlayerID, PayloadHash: call.CanonicalStepPayloadHash,
		SourceRealm: sourceRealm, AuthorityLayoutVersion: layout,
		DestinationRouteDomain: destinationDomain, DestinationActorID: call.RouteHeader.GetActorId(),
		DestinationStep: call.ParticipantStep, DestinationInvocationID: call.InvocationID,
	}, nil
}

// VerifyParticipant validates the optional wire proof with the composed
// keyring+route validator. The method only reports proof validity; rollout
// policy decides whether shadow still reads Redis or explicit prefer bypasses
// it. A caller must never infer authority from proof presence alone.
func (v *CommitProofValidator) VerifyParticipant(ctx context.Context, call *contracts.ParticipantCall, sourceRealm string) (CommitProofStatement, error) {
	if call == nil || call.SourceOrderCommitProof == nil {
		return CommitProofStatement{}, ErrCommitProofMissing
	}
	expected, err := CommitProofExpectedFromParticipantCall(call, sourceRealm)
	if err != nil {
		return CommitProofStatement{}, err
	}
	proof, err := CommitProofFromProto(call.SourceOrderCommitProof)
	if err != nil {
		return CommitProofStatement{}, err
	}
	if err := v.Verify(ctx, proof, expected); err != nil {
		return CommitProofStatement{}, err
	}
	return proof.Statement, nil
}

func commitProofWorkToProto(address workidentity.WorkAddress, layout string) (*contractspb.WorkAddress, error) {
	if err := AcceptsDirect(address); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCommitProofInvalid, err)
	}
	workType, err := commitProofWorkTypeToProto(address.WorkType)
	if err != nil {
		return nil, err
	}
	route, err := commitProofRouteToProto(address.SourceRouteDomain)
	if err != nil {
		return nil, err
	}
	authorityKind := contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR
	if address.SourceRouteDomain == workidentity.RouteDomainFarm {
		authorityKind = contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR
	}
	return &contractspb.WorkAddress{
		WorkId: address.WorkID, WorkType: workType,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: address.BucketVersion, BucketId: address.BucketID,
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind: authorityKind, CanonicalAuthorityId: strconv.FormatUint(address.SourceActorID, 10),
			AuthorityLayoutVersion: layout, ActorRouteDomain: &route,
		},
	}, nil
}

func commitProofWorkFromProto(input *contractspb.WorkAddress) (workidentity.WorkAddress, error) {
	if input == nil || input.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		input.GetSourceAuthority() == nil || input.GetBucketVersion() == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: wire work", ErrCommitProofInvalid)
	}
	workType, err := commitProofWorkTypeFromProto(input.GetWorkType())
	if err != nil {
		return workidentity.WorkAddress{}, err
	}
	route, err := commitProofRouteFromProto(input.GetSourceAuthority().GetActorRouteDomain())
	if err != nil {
		return workidentity.WorkAddress{}, err
	}
	if (route == workidentity.RouteDomainFarm && input.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR) ||
		(route == workidentity.RouteDomainPlayer && input.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR) {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: wire source authority", ErrCommitProofInvalid)
	}
	actorID, err := strconv.ParseUint(input.GetSourceAuthority().GetCanonicalAuthorityId(), 10, 64)
	if err != nil || actorID == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: wire source actor", ErrCommitProofInvalid)
	}
	address := workidentity.WorkAddress{WorkID: input.GetWorkId(), WorkType: workType,
		PlacementKind: workidentity.SourcePersistenceBucket, BucketVersion: input.GetBucketVersion(),
		BucketID: input.GetBucketId(), SourceRouteDomain: route, SourceActorID: actorID}
	if err := AcceptsDirect(address); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: wire work: %v", ErrCommitProofInvalid, err)
	}
	return address, nil
}

func commitProofRouteToProto(domain workidentity.RouteDomain) (logicrpcpb.RouteDomain, error) {
	switch domain {
	case workidentity.RouteDomainPlayer:
		return logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, nil
	case workidentity.RouteDomainFarm:
		return logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, nil
	default:
		return logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED, fmt.Errorf("%w: route domain", ErrCommitProofInvalid)
	}
}

func commitProofRouteFromProto(domain logicrpcpb.RouteDomain) (workidentity.RouteDomain, error) {
	switch domain {
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER:
		return workidentity.RouteDomainPlayer, nil
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM:
		return workidentity.RouteDomainFarm, nil
	default:
		return workidentity.RouteDomainUnspecified, fmt.Errorf("%w: wire route domain", ErrCommitProofInvalid)
	}
}

func commitProofWorkTypeToProto(value string) (contractspb.WorkType, error) {
	switch value {
	case string(identity.WorkTypeCareReward):
		return contractspb.WorkType_WORK_TYPE_CARE_REWARD, nil
	case string(identity.WorkTypeHarvest):
		return contractspb.WorkType_WORK_TYPE_HARVEST, nil
	default:
		return contractspb.WorkType_WORK_TYPE_UNSPECIFIED, fmt.Errorf("%w: work type", ErrCommitProofInvalid)
	}
}

func commitProofWorkTypeFromProto(value contractspb.WorkType) (string, error) {
	switch value {
	case contractspb.WorkType_WORK_TYPE_CARE_REWARD:
		return string(identity.WorkTypeCareReward), nil
	case contractspb.WorkType_WORK_TYPE_HARVEST:
		return string(identity.WorkTypeHarvest), nil
	default:
		return "", fmt.Errorf("%w: wire work type", ErrCommitProofInvalid)
	}
}

func commitProofStepToProto(value string) (contractspb.ParticipantStep, error) {
	switch value {
	case string(identity.StepApplyCareReward):
		return contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_CARE_REWARD, nil
	case string(identity.StepGrantHarvestReward):
		return contractspb.ParticipantStep_PARTICIPANT_STEP_GRANT_HARVEST_REWARD, nil
	default:
		return contractspb.ParticipantStep_PARTICIPANT_STEP_UNSPECIFIED, fmt.Errorf("%w: destination step", ErrCommitProofInvalid)
	}
}

func commitProofStepFromProto(value contractspb.ParticipantStep) (string, error) {
	switch value {
	case contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_CARE_REWARD:
		return string(identity.StepApplyCareReward), nil
	case contractspb.ParticipantStep_PARTICIPANT_STEP_GRANT_HARVEST_REWARD:
		return string(identity.StepGrantHarvestReward), nil
	default:
		return "", fmt.Errorf("%w: wire destination step", ErrCommitProofInvalid)
	}
}
