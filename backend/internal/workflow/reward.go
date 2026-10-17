package workflow

import (
	"crypto/sha256"
	"errors"

	farmpb "farmgame/api/gen/farm"

	"google.golang.org/protobuf/proto"
)

// RewardPayloadHash binds a PlayerActor reward receipt to the exact durable
// FarmSettlement participant, not only to the outer client command.
func RewardPayloadHash(workflowPayloadHash []byte, participant *farmpb.FarmSettlementParticipant) ([]byte, error) {
	if len(workflowPayloadHash) == 0 || participant == nil || participant.PlayerId == 0 {
		return nil, errors.New("workflow: invalid reward payload")
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(participant)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("farm-reward-v1\x00"))
	_, _ = hash.Write(workflowPayloadHash)
	_, _ = hash.Write(data)
	return hash.Sum(nil), nil
}
