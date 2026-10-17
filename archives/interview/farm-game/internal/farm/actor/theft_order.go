package actor

import (
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/sourceorder"

	contractspb "farmgame/api/gen/contracts"

	"google.golang.org/protobuf/proto"
)

func buildTheftSourceOrders(farmID uint64, operationID string, visitorID, ownerID uint64, farmResultHash []byte, decision *contractspb.TheftTargetDecision, dueAtMs int64) ([]authoritystore.AttachedOrder, string, string, error) {
	visitorOrderID, err := sourceorder.TheftVisitorSourceOrderID(farmID, operationID)
	if err != nil {
		return nil, "", "", err
	}
	visitorDecision := proto.Clone(decision).(*contractspb.TheftTargetDecision)
	visitorDecision.WorkflowId = visitorOrderID
	settlement, err := identity.TheftSettlementID(visitorOrderID)
	if err != nil {
		return nil, "", "", err
	}
	visitorDecision.SettlementId = settlement
	visitorDecision.TheftDecisionHash = nil
	visitorHashBytes, err := theftDecisionHash(visitorDecision)
	if err != nil {
		return nil, "", "", err
	}
	visitorDecision.TheftDecisionHash = append([]byte(nil), visitorHashBytes[:]...)
	visitorPayload := &contractspb.SettleTheftVisitorPayload{
		VisitorPlayerId: visitorID, DecideTheftResultHash: append([]byte(nil), farmResultHash...),
		TheftTargetDecision: visitorDecision,
	}
	visitorBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(visitorPayload)
	if err != nil {
		return nil, "", "", err
	}
	visitorHash, err := sourceorder.TheftVisitorPayloadHash(visitorPayload)
	if err != nil {
		return nil, "", "", err
	}
	orders := []authoritystore.AttachedOrder{{
		OrderID: visitorOrderID, OrderType: string(identity.WorkTypeTheftVisitor), Payload: visitorBytes,
		PayloadHash: authoritystore.PayloadHash(visitorHash), DueAtMs: dueAtMs, OperationID: operationID, SubjectPlayerID: visitorID,
	}}
	var guardOrderID string
	if decision.GetGuardCoinValue() > 0 {
		guardOrderID, err = sourceorder.TheftGuardSourceOrderID(farmID, operationID)
		if err != nil {
			return nil, "", "", err
		}
		guardDecision := proto.Clone(decision).(*contractspb.TheftTargetDecision)
		guardDecision.WorkflowId = guardOrderID
		guardSettlement, err := identity.TheftSettlementID(guardOrderID)
		if err != nil {
			return nil, "", "", err
		}
		guardDecision.SettlementId = guardSettlement
		guardDecision.TheftDecisionHash = nil
		guardHashBytes, err := theftDecisionHash(guardDecision)
		if err != nil {
			return nil, "", "", err
		}
		guardDecision.TheftDecisionHash = append([]byte(nil), guardHashBytes[:]...)
		guardPayload := &contractspb.CreditGuardIncomePayload{
			OwnerPlayerId: ownerID, DecideTheftResultHash: append([]byte(nil), farmResultHash...),
			SettleVisitorResultHash: append([]byte(nil), farmResultHash...),
			TheftTargetDecision:     guardDecision,
		}
		guardBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(guardPayload)
		if err != nil {
			return nil, "", "", err
		}
		guardHash, err := sourceorder.TheftGuardPayloadHash(guardPayload)
		if err != nil {
			return nil, "", "", err
		}
		orders = append(orders, authoritystore.AttachedOrder{
			OrderID: guardOrderID, OrderType: string(identity.WorkTypeTheftGuard), Payload: guardBytes,
			PayloadHash: authoritystore.PayloadHash(guardHash), DueAtMs: dueAtMs, OperationID: operationID, SubjectPlayerID: ownerID,
		})
	}
	return orders, visitorOrderID, guardOrderID, nil
}
