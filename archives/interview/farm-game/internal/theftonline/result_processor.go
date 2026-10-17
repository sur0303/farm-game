package theftonline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"
)

// ResultOrderProcessor consumes Pair RESULT_PUBLISH_ORDER. Dest execution is
// SourceOrder; this only publishes a rebuildable terminal copy from Pair
// admission and then releases the source order's capacity charge.
type ResultOrderProcessor struct {
	pair             *friend.RedisFriendshipRepositories
	publisher        theft.ResultPublisher
	sourceOrderStore authoritystore.SourceWorkReader
}

func NewResultOrderProcessor(
	pair *friend.RedisFriendshipRepositories,
	publisher theft.ResultPublisher,
	sourceOrderStore authoritystore.SourceWorkReader,
) (*ResultOrderProcessor, error) {
	if pair == nil || publisher == nil {
		return nil, fmt.Errorf("%w: Pair and ResultPublisher are required", ErrInvalidComposition)
	}
	return &ResultOrderProcessor{pair: pair, publisher: publisher, sourceOrderStore: sourceOrderStore}, nil
}

var _ theftrecovery.PairResultOrderProcessor = (*ResultOrderProcessor)(nil)

func (p *ResultOrderProcessor) ProcessResultOrder(ctx context.Context, candidate theftrecovery.DispatchCandidate) (bool, error) {
	if p == nil || p.pair == nil || p.publisher == nil {
		return false, ErrInvalidComposition
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil || ref.WorkType != theftrecovery.ResultPublishOrder {
		return false, theftrecovery.ErrMalformedMember
	}
	binding, err := p.pair.ReadPairActionResultOrder(ctx, candidate.BucketID, ref.WorkType, ref.WorkID)
	if err != nil {
		return false, err
	}
	if binding.Scope != friend.ActionScopeTheft {
		return false, theftrecovery.ErrNotTheft
	}
	// A stale candidate can race the dispatch worker's explicit NO_CHANGE
	// finalization. The result order has already been durably completed in that
	// case; never re-enter source-order lookup for a terminal order.
	switch binding.ResultState {
	case "SUCCEEDED", "REJECTED", "NO_CHANGE", "RECONCILE_REQUIRED":
		return true, nil
	}
	terminal := "SUCCEEDED"
	outcome := &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_THEFT, OriginalOperationId: binding.OperationID,
		FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: binding.OwnerPlayerID},
	}
	proof := sourceOrderResultProof(binding)
	switch binding.AdmissionStatus {
	case friend.StatusRejected:
		terminal = "REJECTED"
		outcome.Status = commonpb.CommandStatus_REJECTED
		outcome.Error = &commonpb.ErrorDetail{
			ErrorCode: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, ErrorMessage: "Theft admission was rejected",
		}
		proof = pairRejectionProof(binding)
	case friend.StatusSucceeded:
		if err := p.ensureTheftSourceCommitted(ctx, binding); err != nil {
			if errors.Is(err, authoritystore.ErrSourceWorkNotFound) {
				// Pair admission can become due before Farm's SourceCommit has
				// reached Redis. Keep the result order observable and retry it
				// instead of publishing success without a source fact.
				if deferErr := p.pair.DeferPairActionResultOrder(ctx, binding, 250*time.Millisecond); deferErr != nil {
					return false, deferErr
				}
				return false, nil
			}
			return false, err
		}
		outcome.Status = commonpb.CommandStatus_SUCCEEDED
	default:
		return false, friend.ErrFriendReconcile
	}
	if err := p.publisher.Publish(ctx, theft.ResultPublication{
		OperationID: binding.OperationID, OperationDigest: binding.OperationDigest,
		SubjectPlayerID: binding.SubjectPlayerID, PairID: binding.PairID,
		SourceRef: "pair-result-v1:" + binding.OperationDigest, PayloadHash: binding.PayloadHash, Outcome: outcome,
	}); err != nil {
		return false, err
	}
	if err := p.pair.CompletePairActionResultOrder(ctx, binding, terminal, proof[:]); err != nil {
		return false, err
	}
	return true, nil
}

// FinalizeNoChange publishes and terminalizes a Pair result when Farm has
// authoritatively serialized the theft after the target crop was already
// harvested/consumed. There is intentionally no THEFT_VISITOR SourceOrder in
// this case: Player settlement validates a positive theft quantity and cannot
// represent a zero-effect theft as a settlement call.
func (p *ResultOrderProcessor) FinalizeNoChange(ctx context.Context, order theftrecovery.DispatchOrder) error {
	if p == nil || p.pair == nil || p.publisher == nil || order.OperationID == "" ||
		order.VisitorPlayerID == 0 || order.OwnerPlayerID == 0 {
		return ErrInvalidComposition
	}
	binding, found, err := p.pair.LookupPairActionResult(ctx, order.VisitorPlayerID, order.OwnerPlayerID, order.OperationID)
	if err != nil {
		return err
	}
	if !found || binding.AdmissionStatus != friend.StatusSucceeded ||
		binding.DispatchOrderID != order.Work.WorkID {
		return fmt.Errorf("%w: NO_CHANGE Pair result binding", friend.ErrFriendReconcile)
	}
	if binding.ResultState != "PENDING" {
		return nil
	}
	outcome := &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: binding.OperationID,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: binding.OwnerPlayerID},
		Status:              commonpb.CommandStatus_NO_CHANGE,
	}
	if err := p.publisher.Publish(ctx, theft.ResultPublication{
		OperationID: binding.OperationID, OperationDigest: binding.OperationDigest,
		SubjectPlayerID: binding.SubjectPlayerID, PairID: binding.PairID,
		SourceRef:   "pair-result-v1:" + binding.OperationDigest,
		PayloadHash: binding.PayloadHash, Outcome: outcome,
	}); err != nil {
		return err
	}
	proof := sourceOrderResultProof(binding)
	return p.pair.CompletePairActionResultOrder(ctx, binding, "NO_CHANGE", proof[:])
}

func (p *ResultOrderProcessor) ensureTheftSourceCommitted(ctx context.Context, binding friend.PairActionResultBinding) error {
	if p.sourceOrderStore == nil {
		return fmt.Errorf("%w: theft SourceOrder reader is required", ErrInvalidComposition)
	}
	visitorOrderID, err := sourceorder.TheftVisitorSourceOrderID(binding.OwnerPlayerID, binding.OperationID)
	if err != nil {
		return fmt.Errorf("%w: theft visitor SourceOrder id: %v", friend.ErrFriendReconcile, err)
	}
	record, err := p.sourceOrderStore.ReadSourceWork(ctx,
		authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: binding.OwnerPlayerID},
		string(identity.WorkTypeTheftVisitor), visitorOrderID)
	if err != nil {
		return err
	}
	if record.WorkID != visitorOrderID || record.WorkType != string(identity.WorkTypeTheftVisitor) ||
		record.ActorRouteDomain != actorrouting.DomainFarm || record.SubjectPlayerID != binding.SubjectPlayerID ||
		record.OperationID != binding.OperationID || record.PayloadHash == "" {
		return fmt.Errorf("%w: theft visitor SourceOrder binding", friend.ErrFriendReconcile)
	}
	return nil
}

func sourceOrderResultProof(binding friend.PairActionResultBinding) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("theft-source-order-result-v1"))
	_, _ = h.Write([]byte(binding.OperationDigest))
	_, _ = h.Write(binding.PayloadHash)
	_, _ = h.Write(binding.TargetBindingHash)
	var proof [sha256.Size]byte
	copy(proof[:], h.Sum(nil))
	return proof
}

func pairRejectionProof(binding friend.PairActionResultBinding) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("pair-action-rejection-v1"))
	_, _ = h.Write([]byte(binding.OperationDigest))
	_, _ = h.Write(binding.PayloadHash)
	_, _ = h.Write([]byte(binding.AdmissionReason))
	var proof [sha256.Size]byte
	copy(proof[:], h.Sum(nil))
	return proof
}
