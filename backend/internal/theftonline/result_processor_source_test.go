package theftonline

import (
	"context"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/sourceorder"
)

type sourceWorkReaderFunc func(context.Context, authoritystore.ActorAddress, string, string) (authoritystore.SourceWorkRecord, error)

func (f sourceWorkReaderFunc) ReadSourceWork(ctx context.Context, address authoritystore.ActorAddress, workType, workID string) (authoritystore.SourceWorkRecord, error) {
	return f(ctx, address, workType, workID)
}

func TestEnsureTheftSourceCommittedRequiresFarmVisitorOrder(t *testing.T) {
	const visitorID = uint64(1001)
	const ownerID = uint64(2002)
	const operationID = "theft-source-proof"
	orderID, err := sourceorder.TheftVisitorSourceOrderID(ownerID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	var gotAddress authoritystore.ActorAddress
	var gotType, gotID string
	reader := sourceWorkReaderFunc(func(_ context.Context, address authoritystore.ActorAddress, workType, workID string) (authoritystore.SourceWorkRecord, error) {
		gotAddress, gotType, gotID = address, workType, workID
		return authoritystore.SourceWorkRecord{
			WorkID: orderID, WorkType: "THEFT_VISITOR", ActorRouteDomain: actorrouting.DomainFarm,
			SubjectPlayerID: visitorID, OperationID: operationID, PayloadHash: "payload-proof",
		}, nil
	})
	processor := &ResultOrderProcessor{sourceOrderStore: reader}
	if err := processor.ensureTheftSourceCommitted(context.Background(), friend.PairActionResultBinding{
		SubjectPlayerID: visitorID, OwnerPlayerID: ownerID, OperationID: operationID,
	}); err != nil {
		t.Fatal(err)
	}
	if gotAddress.Domain != actorrouting.DomainFarm || gotAddress.ActorID != ownerID || gotType != "THEFT_VISITOR" || gotID != orderID {
		t.Fatalf("source lookup=%+v type=%q id=%q", gotAddress, gotType, gotID)
	}
}
