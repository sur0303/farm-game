package friend

// Friendship domain service: the deterministic AddFriend redemption
// orchestration and termination-order execution (contract 33 §4/§5, 33A).
//
// The service is stateless except for the terminal-outcome store that models
// the AddFriendWorkflow's persisted StoredOperationOutcome at the
// FRIEND_PAIR root. Every step delegates atomicity to the authority
// repositories; the two quota participants always run in canonical
// min(player) -> max(player) order so concurrent redemptions from opposite
// directions converge (33 §5.2).

import (
	"context"
	"fmt"
	"sync"
)

// RedeemShareCommand is the domain input of one REDEEM_SHARE operation. The
// token fields are the already-authenticated envelope claims; envelope
// cryptography is SECURITY_PLATFORM_PENDING (33A §6.2) and out of scope here.
type RedeemShareCommand struct {
	RedeemerPlayerID uint64
	InviterPlayerID  uint64
	TokenID          string
	TokenGeneration  uint64
	OperationID      string
	PayloadHash      []byte
	NowMs            int64
}

// RedeemShareResult is the public two-phase result (33 §4.4): it only covers
// the friendship itself; entering the inviter's farm is a separate Join.
type RedeemShareResult struct {
	Outcome         Outcome
	InviterPlayerID uint64
	RelationID      string
	RelationEpoch   uint64
	WorkflowID      string
}

// DomainService orchestrates the three friendship authorities.
type DomainService struct {
	Pairs  PairRepository
	Quotas QuotaRepository
	Shares ShareIssuerRepository

	mu       sync.Mutex
	outcomes map[string]Outcome // redeem operation digest -> terminal outcome
}

// NewDomainService binds the service to its authority ports.
func NewDomainService(pairs PairRepository, quotas QuotaRepository, shares ShareIssuerRepository) (*DomainService, error) {
	if pairs == nil || quotas == nil || shares == nil {
		return nil, fmt.Errorf("%w: authority ports", ErrInvalidCommand)
	}
	return &DomainService{Pairs: pairs, Quotas: quotas, Shares: shares, outcomes: make(map[string]Outcome)}, nil
}

func redeemOutcomeKey(redeemer uint64, operationID string) string {
	return "redeem:" + itoa(redeemer) + ":" + operationID
}

func (s *DomainService) storedRedeemOutcome(redeemer uint64, operationID string, payloadHash []byte) (Outcome, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.outcomes[redeemOutcomeKey(redeemer, operationID)]
	if !ok {
		return Outcome{}, false, nil
	}
	if !payloadEqual(stored.PayloadHash, payloadHash) {
		return Outcome{}, false, ErrIdempotentConflict
	}
	return stored, true, nil
}

func (s *DomainService) storeRedeemOutcome(redeemer uint64, operationID string, payloadHash []byte, outcome Outcome) {
	outcome.PayloadHash = append([]byte(nil), payloadHash...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomes[redeemOutcomeKey(redeemer, operationID)] = outcome
}

func publicIdentity(subject uint64, commandType, operationID string) CommitIdentity {
	return CommitIdentity{SubjectPlayerID: subject, CommandType: commandType, OperationID: operationID}
}

func participantIdentity(workflowID, step string) CommitIdentity {
	invocationID, err := stableID("farm-friend-participant-invocation-id-v1", workflowID, step)
	if err != nil {
		panic(err)
	}
	return CommitIdentity{WorkAddressID: workflowID, ParticipantInvocationID: invocationID, ParticipantStep: step}
}

// RedeemShare executes the AddFriendWorkflow step chain deterministically
// (33 §5.2): admission -> validate -> reserve(min,max) -> activate ->
// finalize(min,max), with compensation on failure. The relation becoming
// ACTIVE is the irreversible point.
func (s *DomainService) RedeemShare(ctx context.Context, cmd RedeemShareCommand) (RedeemShareResult, error) {
	result := RedeemShareResult{InviterPlayerID: cmd.InviterPlayerID}
	if cmd.RedeemerPlayerID == 0 || cmd.InviterPlayerID == 0 || cmd.OperationID == "" || cmd.TokenID == "" {
		return result, fmt.Errorf("%w: redeem fields", ErrInvalidCommand)
	}
	if cmd.RedeemerPlayerID == cmd.InviterPlayerID {
		result.Outcome = Outcome{Status: StatusRejected, Reason: ReasonSelfAdd}
		return result, nil
	}
	if stored, replay, err := s.storedRedeemOutcome(cmd.RedeemerPlayerID, cmd.OperationID, cmd.PayloadHash); err != nil {
		return result, err
	} else if replay {
		result.Outcome = stored
		return result, nil
	}

	finish := func(outcome Outcome, relationID string, epoch uint64, workflowID string) (RedeemShareResult, error) {
		s.storeRedeemOutcome(cmd.RedeemerPlayerID, cmd.OperationID, cmd.PayloadHash, outcome)
		result.Outcome = outcome
		result.RelationID = relationID
		result.RelationEpoch = epoch
		result.WorkflowID = workflowID
		return result, nil
	}

	low, high := CanonicalPair(cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	pairID := pairIDMust(low, high)
	workflowID, err := AddFriendWorkflowID(cmd.RedeemerPlayerID, cmd.OperationID, pairID)
	if err != nil {
		return result, err
	}

	// Step 1: ShareToken admission (acceptance point, 33 §4.2.1).
	_, admission, err := s.Shares.AcceptShareTokenAdmission(ctx, cmd.InviterPlayerID, cmd.TokenID,
		cmd.TokenGeneration, cmd.RedeemerPlayerID, workflowID,
		publicIdentity(cmd.RedeemerPlayerID, "REDEEM_SHARE", cmd.OperationID), cmd.PayloadHash, cmd.NowMs)
	if err != nil {
		return result, err
	}
	if admission.Status == StatusRejected {
		return finish(admission, "", 0, workflowID)
	}

	// Step 2: pair validation (block / barrier / already friends).
	snap, err := s.Pairs.ValidatePairForAdd(ctx, cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	if err != nil {
		return result, err
	}
	if snap.AlreadyFriends {
		return finish(Outcome{Status: StatusNoChange, Reason: ReasonAlreadyFriends, CommittedEpoch: snap.CurrentRelationEpoch},
			snap.RelationID, snap.CurrentRelationEpoch, workflowID)
	}
	if snap.Rejected {
		return finish(Outcome{Status: StatusRejected, Reason: snap.Reason}, "", 0, workflowID)
	}
	var rotateReceipt RotateReceipt
	if snap.BarrierState == BarrierReady {
		// READY: token generation must exceed the inviter-side floor and the
		// inviter must have explicitly rotated for THIS barrier (33 §4.3).
		if cmd.TokenGeneration <= snap.RequiredInviterFloor {
			return finish(Outcome{Status: StatusRejected, Reason: ReasonShareTokenPredatesRelationDel}, "", 0, workflowID)
		}
		issuer, err := s.Shares.GetIssuer(ctx, cmd.InviterPlayerID)
		if err != nil {
			return result, err
		}
		receipt, ok := issuer.HasRotateReceiptFor(snap.PairID, snap.BarrierRevision)
		if !ok || receipt.PairID != snap.PairID || receipt.TerminatedRelationEpoch != snap.CurrentRelationEpoch-1 ||
			receipt.TokenID != cmd.TokenID || receipt.TokenGeneration != cmd.TokenGeneration ||
			receipt.LowShareGenerationFloor != snap.LowShareGenerationFloor ||
			receipt.HighShareGenerationFloor != snap.HighShareGenerationFloor {
			return finish(Outcome{Status: StatusRejected, Reason: ReasonReactivationRotateRequired}, "", 0, workflowID)
		}
		rotateReceipt = receipt
	}

	// Step 3: reserve both quota slots in canonical order.
	reservations := make(map[uint64]QuotaReservation, 2)
	for _, player := range []uint64{low, high} {
		reservationID, err := QuotaReservationID(workflowID, player, snap.RelationID)
		if err != nil {
			return result, err
		}
		reservation := QuotaReservation{
			ReservationID:       reservationID,
			WorkflowID:          workflowID,
			RelationID:          snap.RelationID,
			TargetRelationEpoch: snap.TargetRelationEpoch,
		}
		reservations[player] = reservation
		outcome, err := s.Quotas.ReserveFriendSlot(ctx, player, reservation,
			participantIdentity(workflowID, "RESERVE_FRIEND_SLOT:"+itoa(player)), cmd.PayloadHash, cmd.NowMs)
		if err != nil {
			return result, err
		}
		if outcome.Status == StatusNoChange && outcome.Reason == ReasonAlreadyFriends {
			// Concurrent opposite-direction workflow already activated.
			s.releaseReservation(ctx, low, reservations[low], workflowID, cmd)
			s.releaseReservation(ctx, high, reservations[high], workflowID, cmd)
			return finish(Outcome{Status: StatusNoChange, Reason: ReasonAlreadyFriends, CommittedEpoch: outcome.CommittedEpoch},
				snap.RelationID, outcome.CommittedEpoch, workflowID)
		}
		if outcome.Status != StatusSucceeded {
			// Compensate whatever was reserved before reporting the limit.
			s.releaseReservation(ctx, low, reservations[low], workflowID, cmd)
			s.releaseReservation(ctx, high, reservations[high], workflowID, cmd)
			rejected := outcome
			rejected.Detail = "player=" + itoa(player)
			return finish(rejected, "", 0, workflowID)
		}
	}

	// Step 4: atomic activation (re-validates every fence).
	activate, err := s.Pairs.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID:      cmd.InviterPlayerID,
		RedeemerPlayerID:     cmd.RedeemerPlayerID,
		TargetRelationEpoch:  snap.TargetRelationEpoch,
		TokenID:              cmd.TokenID,
		TokenGeneration:      cmd.TokenGeneration,
		RotateReceipt:        rotateReceipt,
		RotateReceiptPresent: rotateReceipt.ReceiptID != "",
		OperationID:          cmd.OperationID,
		NowMs:                cmd.NowMs,
	}, participantIdentity(workflowID, "ACTIVATE_FRIEND_RELATION"), cmd.PayloadHash)
	if err != nil {
		return result, err
	}
	if activate.Status != StatusSucceeded && activate.Reason != ReasonAlreadyFriends {
		// Pair moved concurrently (epoch conflict / fresh block): compensate
		// reservations, never guess (33 §5.2).
		s.releaseReservation(ctx, low, reservations[low], workflowID, cmd)
		s.releaseReservation(ctx, high, reservations[high], workflowID, cmd)
		return finish(activate, "", 0, workflowID)
	}

	// Step 5: finalize both slots. The relation is already the irreversible
	// fact; finalize lag never rolls it back (33 §5.2).
	for _, player := range []uint64{low, high} {
		if _, err := s.Quotas.FinalizeFriendSlot(ctx, player, reservations[player],
			participantIdentity(workflowID, "FINALIZE_FRIEND_SLOT:"+itoa(player)), cmd.PayloadHash, cmd.NowMs); err != nil {
			return result, err
		}
	}
	return finish(Outcome{Status: StatusSucceeded, CommittedEpoch: activate.CommittedEpoch},
		snap.RelationID, activate.CommittedEpoch, workflowID)
}

func (s *DomainService) releaseReservation(ctx context.Context, player uint64, reservation QuotaReservation, workflowID string, cmd RedeemShareCommand) {
	if reservation.ReservationID == "" {
		return
	}
	// Compensation is best-effort at domain level; the worker layer retries
	// with the same participant identity (33A §8.4).
	_, _ = s.Quotas.ReleaseFriendReservation(ctx, player, reservation.ReservationID,
		participantIdentity(workflowID, "RELEASE_FRIEND_RESERVATION:"+itoa(player)), cmd.PayloadHash, cmd.NowMs)
}

// DeleteFriend terminates the relation immediately (33 §3.1). The public
// success boundary is the pair commit itself; quota cleanup and barrier
// floors are backfilled by ExecuteTerminationOrders.
func (s *DomainService) DeleteFriend(ctx context.Context, caller, peer uint64, operationID string, payloadHash []byte, nowMs int64) (Outcome, error) {
	return s.Pairs.DeleteFriend(ctx, PairMutationCommand{
		Identity:       publicIdentity(caller, "DELETE_FRIEND", operationID),
		PayloadHash:    payloadHash,
		CallerPlayerID: caller,
		PeerPlayerID:   peer,
		NowMs:          nowMs,
	})
}

// BlockPlayer blocks one direction and terminates any active relation in
// the same commit (33 §3.2).
func (s *DomainService) BlockPlayer(ctx context.Context, caller, peer uint64, operationID string, payloadHash []byte, nowMs int64) (Outcome, error) {
	return s.Pairs.BlockPlayer(ctx, PairMutationCommand{
		Identity:       publicIdentity(caller, "BLOCK_PLAYER", operationID),
		PayloadHash:    payloadHash,
		CallerPlayerID: caller,
		PeerPlayerID:   peer,
		NowMs:          nowMs,
	})
}

// UnblockPlayer removes one directed block; it never restores friendship.
func (s *DomainService) UnblockPlayer(ctx context.Context, caller, peer uint64, operationID string, payloadHash []byte, nowMs int64) (Outcome, error) {
	return s.Pairs.UnblockPlayer(ctx, PairMutationCommand{
		Identity:       publicIdentity(caller, "UNBLOCK_PLAYER", operationID),
		PayloadHash:    payloadHash,
		CallerPlayerID: caller,
		PeerPlayerID:   peer,
		NowMs:          nowMs,
	})
}

// ExecuteTerminationOrder runs the deterministic quota/barrier part of one
// open FriendTerminationOrder (33 §3.3): capture both share generation
// snapshots, commit floors (PENDING -> READY), release both quota slots at
// the captured epoch, then complete the order. Hub subscription revocation
// belongs to the farm side and is out of SO0 scope.
func (s *DomainService) ExecuteTerminationOrder(ctx context.Context, playerA, playerB uint64, nowMs int64) (Outcome, error) {
	snap, err := s.Pairs.GetPair(ctx, playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	order := snap.PendingTermination
	if order == nil || order.Completed {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}, nil
	}
	lowFloor, err := s.Shares.GetGenerationSnapshot(ctx, snap.LowPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	highFloor, err := s.Shares.GetGenerationSnapshot(ctx, snap.HighPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	deletedEpoch := order.CapturedActiveEpoch + 1
	floors, err := s.Pairs.CommitReactivationGenerationFloors(ctx, playerA, playerB,
		deletedEpoch, order.BarrierRevision, lowFloor, highFloor, nowMs)
	if err != nil {
		return Outcome{}, err
	}
	if floors.Status != StatusSucceeded {
		// A retry after the floor commit may legitimately observe READY and
		// continue quota cleanup.  Only continue when the same order and the
		// same deleted epoch are still present; a moved pair is a late order.
		current, readErr := s.Pairs.GetPair(ctx, playerA, playerB)
		if readErr != nil {
			return Outcome{}, readErr
		}
		if current.PendingTermination == nil || current.PendingTermination.OrderID != order.OrderID ||
			current.Relation.RelationEpoch != deletedEpoch || current.Barrier.State != BarrierReady {
			return floors, nil
		}
		snap = current
		order = current.PendingTermination
	}
	for _, player := range []uint64{snap.LowPlayerID, snap.HighPlayerID} {
		if _, err := s.Quotas.RemoveActiveRelationIfEpochMatches(ctx, player, snap.Relation.RelationID,
			order.CapturedActiveEpoch,
			participantIdentity(order.OrderID, "REMOVE_ACTIVE_RELATION:"+itoa(player)), nil, nowMs); err != nil {
			return Outcome{}, err
		}
		progress, ok := s.Pairs.(interface {
			MarkTerminationQuotaReleased(context.Context, uint64, uint64, string, uint64, int64) (Outcome, error)
		})
		if !ok {
			return Outcome{}, fmt.Errorf("%w: pair termination progress", ErrRedisContractViolation)
		}
		if _, err := progress.MarkTerminationQuotaReleased(ctx, playerA, playerB, order.OrderID, player, nowMs); err != nil {
			return Outcome{}, err
		}
	}
	return s.Pairs.CompleteTerminationOrder(ctx, playerA, playerB, order.OrderID, nowMs)
}
