package guardrecovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const guardSnapshotSlot = "PET_GUARD_SLOT"

func DecodeApplyGuardSnapshotPayload(workID string, payloadBytes []byte) (*contractspb.ApplyGuardSnapshotPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("%w: work id or payload bytes", ErrPayloadInvalid)
	}
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		return nil, err
	}
	if spec.MaxPayloadBytes > 0 && len(payloadBytes) > spec.MaxPayloadBytes {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrPayloadInvalid, spec.MaxPayloadBytes)
	}
	payload := spec.NewPayload().(*contractspb.ApplyGuardSnapshotPayload)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("%w: protobuf decode: %v", ErrPayloadInvalid, err)
	}
	if hasUnknownGuardPayload(payload) {
		return nil, fmt.Errorf("%w: protobuf contains unknown fields", ErrPayloadInvalid)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("%w: payload is not deterministic protobuf", ErrPayloadInvalid)
	}
	if err := validateApplyGuardSnapshotPayload(workID, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func ApplyGuardSnapshotPayloadHash(payloadBytes []byte) string {
	payload := &contractspb.ApplyGuardSnapshotPayload{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payloadBytes, payload); err != nil || hasUnknownGuardPayload(payload) {
		return ""
	}
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		return ""
	}
	sum, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(sum[:])
}

func validateApplyGuardSnapshotPayload(workID string, payload *contractspb.ApplyGuardSnapshotPayload) error {
	if payload == nil || payload.GetGuardSnapshot() == nil ||
		payload.GetSourcePlayerId() == 0 || payload.GetTargetFarmId() == 0 ||
		payload.GetSourcePlayerId() != payload.GetTargetFarmId() ||
		payload.GetOrderId() != workID ||
		len(payload.GetSnapshotHash()) != sha256.Size {
		return fmt.Errorf("%w: order, subject, target or snapshot binding", ErrPayloadInvalid)
	}
	snapshot := payload.GetGuardSnapshot()
	if snapshot.GetSchemaVersion() != uint32(playerpet.GuardSnapshotSchemaVersion) ||
		snapshot.GetGuardVersion() == 0 ||
		snapshot.ActiveGuardPetId == nil ||
		snapshot.GetActiveGuardPetId() == 0 ||
		snapshot.GetGuardRule() == nil ||
		len(snapshot.GetSnapshotHash()) != sha256.Size ||
		!bytes.Equal(snapshot.GetSnapshotHash(), payload.GetSnapshotHash()) {
		return fmt.Errorf("%w: guard snapshot version or hash binding", ErrPayloadInvalid)
	}
	rule := snapshot.GetGuardRule()
	if rule.GetPetConfigVersion() == 0 ||
		rule.GetGuardRuleVersion() == 0 ||
		rule.GetRandomAlgorithmVersion() == 0 ||
		rule.GetRandomKeyVersion() == 0 ||
		rule.GetTriggerProbabilityBp() > 10000 ||
		rule.GetInterceptPolicy() != contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS {
		return fmt.Errorf("%w: guard rule is invalid", ErrPayloadInvalid)
	}
	domainSnapshot := playerpet.GuardSnapshot{
		SchemaVersion:    uint64(snapshot.GetSchemaVersion()),
		GuardVersion:     snapshot.GetGuardVersion(),
		ActiveGuardPetID: int32(snapshot.GetActiveGuardPetId()),
		GuardUntilMs:     snapshot.GetGuardUntilMs(),
		Rule: playerpet.GuardRuleSnapshot{
			PetConfigVersion:       rule.GetPetConfigVersion(),
			GuardRuleVersion:       rule.GetGuardRuleVersion(),
			TriggerProbabilityBp:   rule.GetTriggerProbabilityBp(),
			InterceptPolicy:        playerpet.InterceptPolicy(rule.GetInterceptPolicy()),
			RandomAlgorithmVersion: rule.GetRandomAlgorithmVersion(),
			RandomKeyVersion:       rule.GetRandomKeyVersion(),
		},
	}
	wantSnapshotHash := domainSnapshot.Hash()
	if !bytes.Equal(wantSnapshotHash[:], snapshot.GetSnapshotHash()) {
		return fmt.Errorf("%w: canonical guard snapshot hash", ErrPayloadInvalid)
	}
	wantOrderID, err := identity.SourceOrderID(
		identity.WorkTypeGuardSyncOrder,
		identity.SourceKindPlayerActor,
		strconv.FormatUint(payload.GetSourcePlayerId(), 10),
		guardSnapshotSlot,
		snapshot.GetGuardVersion(),
	)
	if err != nil || wantOrderID != workID {
		return fmt.Errorf("%w: deterministic source order generation", ErrPayloadInvalid)
	}
	return nil
}

func hasUnknownGuardPayload(payload *contractspb.ApplyGuardSnapshotPayload) bool {
	if payload == nil {
		return true
	}
	if len(payload.ProtoReflect().GetUnknown()) != 0 {
		return true
	}
	snapshot := payload.GetGuardSnapshot()
	if snapshot == nil || len(snapshot.ProtoReflect().GetUnknown()) != 0 {
		return true
	}
	rule := snapshot.GetGuardRule()
	return rule == nil || len(rule.ProtoReflect().GetUnknown()) != 0
}
