package participant

import (
	"errors"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

// createdSourceOrderEnvelope is returned only with a definitively committed
// source Participant turn. It is an in-process Direct latency hint; the
// destination still verifies Redis SourceOrder/DueIndex and Worker recovery
// never trusts this envelope.
func createdSourceOrderEnvelope(call *contracts.ParticipantCall, order authoritystore.AttachedOrder) (*contractspb.CreatedSourceOrderEnvelope, error) {
	if call == nil || call.Work == nil || order.OrderID == "" || order.OrderID != call.Work.GetWorkId() ||
		order.OrderType == "" || order.OperationID == "" || order.SubjectPlayerID == 0 || order.DueAtMs <= 0 || len(order.Payload) == 0 ||
		order.PayloadHash == (authoritystore.PayloadHash{}) {
		return nil, errors.New("player participant: incomplete committed SourceOrder envelope")
	}
	return &contractspb.CreatedSourceOrderEnvelope{
		Work:            proto.Clone(call.Work).(*contractspb.WorkAddress),
		WorkVersion:     1,
		OperationId:     order.OperationID,
		SubjectPlayerId: order.SubjectPlayerID,
		TypedPayload:    append([]byte(nil), order.Payload...),
		PayloadHash:     append([]byte(nil), order.PayloadHash[:]...),
		DueAtMs:         order.DueAtMs,
	}, nil
}
