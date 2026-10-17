package participant

import (
	"bytes"
	"encoding/binary"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// participantResultProofV1 is stored only inside the explicitly namespaced
// PlayerOperationReceipt.result_payload field. It is intentionally not a
// public protobuf: changing public/generated files would alter the frozen RPC
// contract, while this small binary envelope can be strictly versioned and
// decoded independently of protobuf unknown-field behavior.
//
// Layout (all integers unsigned big endian):
//
//	magic[4] | version[1] | command[4] | invocation_length[2]
//	| participant_invocation_id[N] | step_hash[32] | result_hash[32]
//	| typed_result_length[4] | deterministic_typed_result[M]
const (
	participantResultProofMagic           = "P1RP"
	participantResultProofVersion    byte = 1
	participantResultProofFixedBytes      = 4 + 1 + 4 + 2 + 32 + 32 + 4
	maxParticipantInvocationIDBytes       = 128
	// Plant results are small scalar/Stable ID messages. Harvest results carry
	// one nested CompendiumUnlockDelta, so keep a separate bound without
	// changing the Plant admission envelope.
	maxPlantTypedResultBytes   = 256
	maxHarvestTypedResultBytes = 512
	maxUnlockTypedResultBytes  = 256
)

func maxPlantResultProofEnvelopeBytes(invocationID string) int {
	return participantResultProofFixedBytes + len(invocationID) + maxPlantTypedResultBytes
}

func maxHarvestResultProofEnvelopeBytes(invocationID string) int {
	return participantResultProofFixedBytes + len(invocationID) + maxHarvestTypedResultBytes
}

func maxUnlockResultProofEnvelopeBytes(invocationID string) int {
	return participantResultProofFixedBytes + len(invocationID) + maxUnlockTypedResultBytes
}

func maxParticipantResultBytes(command contractspb.ParticipantCommandType) int {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION:
		return maxUnlockTypedResultBytes
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		return maxHarvestTypedResultBytes
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD:
		return maxCareTypedResultBytes
	}
	return maxPlantTypedResultBytes
}

func encodeParticipantResultProof(spec contracts.ParticipantCommandSpec, invocationID string, stepHash [32]byte, result proto.Message) ([]byte, []byte, error) {
	typedResult, resultHash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil {
		return nil, nil, err
	}
	if len(invocationID) == 0 || len(invocationID) > maxParticipantInvocationIDBytes ||
		len(typedResult) == 0 || len(typedResult) > maxParticipantResultBytes(spec.Command) || len(resultHash) != 32 {
		return nil, nil, status.Error(codes.Internal, "player participant typed result exceeds durable proof bounds")
	}

	proof := make([]byte, participantResultProofFixedBytes+len(invocationID)+len(typedResult))
	copy(proof[:4], participantResultProofMagic)
	proof[4] = participantResultProofVersion
	binary.BigEndian.PutUint32(proof[5:9], uint32(spec.Command))
	binary.BigEndian.PutUint16(proof[9:11], uint16(len(invocationID)))
	copy(proof[11:], invocationID)
	stepOffset := 11 + len(invocationID)
	copy(proof[stepOffset:stepOffset+32], stepHash[:])
	resultHashOffset := stepOffset + 32
	copy(proof[resultHashOffset:resultHashOffset+32], resultHash)
	typedLengthOffset := resultHashOffset + 32
	binary.BigEndian.PutUint32(proof[typedLengthOffset:typedLengthOffset+4], uint32(len(typedResult)))
	copy(proof[typedLengthOffset+4:], typedResult)
	return proof, append([]byte(nil), resultHash...), nil
}

func participantResultProofOffsets(proof []byte) (stepOffset, resultHashOffset, typedLengthOffset, typedOffset int, ok bool) {
	if len(proof) < participantResultProofFixedBytes {
		return 0, 0, 0, 0, false
	}
	invocationLen := int(binary.BigEndian.Uint16(proof[9:11]))
	if invocationLen <= 0 || invocationLen > maxParticipantInvocationIDBytes {
		return 0, 0, 0, 0, false
	}
	stepOffset = 11 + invocationLen
	resultHashOffset = stepOffset + 32
	typedLengthOffset = resultHashOffset + 32
	typedOffset = typedLengthOffset + 4
	return stepOffset, resultHashOffset, typedLengthOffset, typedOffset, typedOffset <= len(proof)
}

func decodeParticipantResultProof(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, proof []byte) (proto.Message, []byte, error) {
	corrupt := func() (proto.Message, []byte, error) {
		return nil, nil, status.Error(codes.Internal, "player participant durable result proof is corrupt")
	}
	stepOffset, resultHashOffset, typedLengthOffset, typedOffset, ok := participantResultProofOffsets(proof)
	if !ok || len(expectedInvocationID) == 0 || len(expectedInvocationID) > maxParticipantInvocationIDBytes || len(expectedStepHash) != 32 ||
		!bytes.Equal(proof[:4], []byte(participantResultProofMagic)) || proof[4] != participantResultProofVersion ||
		binary.BigEndian.Uint32(proof[5:9]) != uint32(spec.Command) ||
		!bytes.Equal(proof[11:stepOffset], []byte(expectedInvocationID)) ||
		!bytes.Equal(proof[stepOffset:resultHashOffset], expectedStepHash) {
		return corrupt()
	}
	typedLen := uint64(binary.BigEndian.Uint32(proof[typedLengthOffset:typedOffset]))
	if typedLen == 0 || typedLen > uint64(maxParticipantResultBytes(spec.Command)) || typedLen != uint64(len(proof)-typedOffset) {
		return corrupt()
	}
	typedBytes := proof[typedOffset:]
	result := spec.NewResult()
	if result == nil {
		return corrupt()
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(typedBytes, result); err != nil || len(result.ProtoReflect().GetUnknown()) != 0 {
		return corrupt()
	}
	canonicalBytes, canonicalHash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonicalBytes, typedBytes) || len(canonicalHash) != 32 ||
		!bytes.Equal(canonicalHash, proof[resultHashOffset:typedLengthOffset]) {
		return corrupt()
	}
	return result, append([]byte(nil), proof[resultHashOffset:typedLengthOffset]...), nil
}
