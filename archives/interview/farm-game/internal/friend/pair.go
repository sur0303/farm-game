package friend

// Canonical pair authority (contract 33 §2, §3; 33A §4).
//
// FriendshipPairRecord co-locates the FriendRelation and both directed
// BlockRelations of one canonical pair in a single authority record so that
// "set block + terminate relation + create follow-up work" commits atomically
// (33 §2.2). relation_epoch is monotonic across activate/delete cycles; old
// permits and late commands against a stale epoch fail closed.

import "fmt"

// ReactivationBarrier blocks re-adding a deleted/blocked pair until the
// inviter explicitly rotates a fresh share token bound to the current
// barrier_revision (33 §3.3, Q33-06).
type ReactivationBarrier struct {
	State                    BarrierState
	TerminatedRelationEpoch  uint64
	LowShareGenerationFloor  uint64 // has value only when State is READY/CONSUMED
	HighShareGenerationFloor uint64
	BarrierRevision          uint64
}

// BlockRelation is one directed block fact (33 §2.4).
type BlockRelation struct {
	Status      BlockStatus
	BlockEpoch  uint64
	CreatedAtMs int64
	RemovedAtMs int64
}

// PairRelation is the canonical FriendRelation (33 §2.3).
type PairRelation struct {
	RelationID                string
	Status                    RelationStatus
	RelationEpoch             uint64
	ActivatedAtMs             int64
	DeletedAtMs               int64
	ActivationTokenID         string
	ActivationTokenGeneration uint64
	TerminationReason         TerminationReason
	TerminatedByPlayerID      uint64
	LastChangeOperationID     string
}

// TerminationOrder is the domain record of the Pair-sourced
// FriendTerminationOrder (33 §3.3). It is created atomically with the
// relation termination and executed later by the workflow layer; SO0 only
// freezes its domain content and deterministic completion rule.
type TerminationOrder struct {
	OrderID                  string
	CapturedActiveEpoch      uint64
	BarrierRevision          uint64
	LowFloorCaptured         bool
	HighFloorCaptured        bool
	LowShareGenerationFloor  uint64
	HighShareGenerationFloor uint64
	QuotaReleasedLow         bool
	QuotaReleasedHigh        bool
	Completed                bool
}

// Ready returns true once both share generation floors have been captured
// and the order may commit them back to the pair barrier.
func (o *TerminationOrder) floorsReady() bool {
	return o != nil && o.LowFloorCaptured && o.HighFloorCaptured
}

// PairRecord is the canonical pair authority record.
type PairRecord struct {
	PairID        string
	LowPlayerID   uint64
	HighPlayerID  uint64
	Relation      PairRelation
	LowBlocksHigh BlockRelation
	HighBlocksLow BlockRelation
	Barrier       ReactivationBarrier
	// PendingTermination is the at-most-one open FriendTerminationOrder.
	PendingTermination *TerminationOrder
	RecordVersion      uint64
	UpdatedAtMs        int64
}

// NewPairRecord creates the minimal record for a canonical pair. A record
// may exist without any relation history (e.g. block-before-friend).
func NewPairRecord(low, high uint64) (*PairRecord, error) {
	if !ValidPair(low, high) {
		return nil, ErrInvalidPair
	}
	low, high = CanonicalPair(low, high)
	relationID, err := RelationIDFor(low, high)
	if err != nil {
		return nil, err
	}
	return &PairRecord{
		PairID:       relationID,
		LowPlayerID:  low,
		HighPlayerID: high,
		Relation:     PairRelation{RelationID: relationID, Status: RelationStatusNone},
	}, nil
}

// RelationIDFor derives the canonical relation id (33A §2.1) using the
// shared StableOpaqueID algorithm; golden vectors live in pair_test.go.
func RelationIDFor(low, high uint64) (string, error) {
	low, high = CanonicalPair(low, high)
	return stableID("farm-friend-relation-id-v1", itoa(low), itoa(high))
}

// HasActiveBlock reports whether either direction blocks the other.
func (r *PairRecord) HasActiveBlock() bool {
	return r.LowBlocksHigh.Status == BlockStatusActive || r.HighBlocksLow.Status == BlockStatusActive
}

// blockOf returns the directed block blocker→blocked.
func (r *PairRecord) blockOf(blocker, blocked uint64) (*BlockRelation, error) {
	switch {
	case blocker == r.LowPlayerID && blocked == r.HighPlayerID:
		return &r.LowBlocksHigh, nil
	case blocker == r.HighPlayerID && blocked == r.LowPlayerID:
		return &r.HighBlocksLow, nil
	default:
		return nil, fmt.Errorf("%w: players %d/%d not in pair %s", ErrInvalidPair, blocker, blocked, r.PairID)
	}
}

// CanIssuePermit implements the linearized authorization read shared by
// VisitPermit and ActionPermit issuance (33 §7.1, §8.3): relation ACTIVE,
// no active block in either direction, viewer/owner really are the pair.
func (r *PairRecord) CanIssuePermit(viewer, owner uint64) bool {
	if r.Relation.Status != RelationStatusActive || r.HasActiveBlock() {
		return false
	}
	return (viewer == r.LowPlayerID && owner == r.HighPlayerID) ||
		(viewer == r.HighPlayerID && owner == r.LowPlayerID)
}

// ApplyDelete terminates an ACTIVE relation (33 §3.1). It is the permission
// revocation linearization point: after it commits, no new permits are
// issued. Deleting a non-ACTIVE relation is a deterministic NO_CHANGE.
// When an ACTIVE relation is terminated the barrier goes PENDING with the
// captured epoch and a TerminationOrder opens in the same commit.
func (r *PairRecord) ApplyDelete(caller uint64, operationID string, nowMs int64) (Outcome, *TerminationOrder, error) {
	if caller != r.LowPlayerID && caller != r.HighPlayerID {
		return Outcome{}, nil, fmt.Errorf("%w: caller %d not in pair", ErrInvalidCommand, caller)
	}
	if r.Relation.Status != RelationStatusActive {
		return Outcome{Status: StatusNoChange, Reason: ReasonNotFriends, CommittedEpoch: r.Relation.RelationEpoch}, nil, nil
	}
	captured := r.Relation.RelationEpoch
	r.Relation.Status = RelationStatusDeleted
	r.Relation.RelationEpoch++
	r.Relation.DeletedAtMs = nowMs
	r.Relation.TerminationReason = TerminationUserDelete
	r.Relation.TerminatedByPlayerID = caller
	r.Relation.LastChangeOperationID = operationID
	r.Barrier = ReactivationBarrier{
		State:                   BarrierPending,
		TerminatedRelationEpoch: captured,
		BarrierRevision:         r.Barrier.BarrierRevision + 1,
	}
	order := &TerminationOrder{
		OrderID:             terminationOrderID(r.PairID, r.Relation.RelationID, captured),
		CapturedActiveEpoch: captured,
		BarrierRevision:     r.Barrier.BarrierRevision,
	}
	r.PendingTermination = order
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Relation.RelationEpoch}, order, nil
}

// ApplyBlock sets one directed block and, if the relation is ACTIVE,
// terminates it in the same commit (33 §3.2). Blocking a non-friend pair is
// still SUCCEEDED but opens no termination work.
func (r *PairRecord) ApplyBlock(blocker, blocked uint64, operationID string, nowMs int64) (Outcome, *TerminationOrder, error) {
	dir, err := r.blockOf(blocker, blocked)
	if err != nil {
		return Outcome{}, nil, err
	}
	if dir.Status == BlockStatusActive {
		return Outcome{Status: StatusNoChange, Reason: ReasonAlreadyBlocked, CommittedEpoch: r.Relation.RelationEpoch}, nil, nil
	}
	dir.Status = BlockStatusActive
	dir.BlockEpoch++
	dir.CreatedAtMs = nowMs
	dir.RemovedAtMs = 0

	var order *TerminationOrder
	if r.Relation.Status == RelationStatusActive {
		captured := r.Relation.RelationEpoch
		r.Relation.Status = RelationStatusDeleted
		r.Relation.RelationEpoch++
		r.Relation.DeletedAtMs = nowMs
		r.Relation.TerminationReason = TerminationBlocked
		r.Relation.TerminatedByPlayerID = blocker
		r.Relation.LastChangeOperationID = operationID
		r.Barrier = ReactivationBarrier{
			State:                   BarrierPending,
			TerminatedRelationEpoch: captured,
			BarrierRevision:         r.Barrier.BarrierRevision + 1,
		}
		order = &TerminationOrder{
			OrderID:             terminationOrderID(r.PairID, r.Relation.RelationID, captured),
			CapturedActiveEpoch: captured,
			BarrierRevision:     r.Barrier.BarrierRevision,
		}
		r.PendingTermination = order
	}
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Relation.RelationEpoch}, order, nil
}

// ApplyUnblock removes one directed block. It never recreates the relation
// and never touches quota (33 §3.2).
func (r *PairRecord) ApplyUnblock(blocker, blocked uint64, nowMs int64) (Outcome, error) {
	dir, err := r.blockOf(blocker, blocked)
	if err != nil {
		return Outcome{}, err
	}
	if dir.Status != BlockStatusActive {
		return Outcome{Status: StatusNoChange, Reason: ReasonAlreadyUnblocked, CommittedEpoch: r.Relation.RelationEpoch}, nil
	}
	dir.Status = BlockStatusRemoved
	dir.BlockEpoch++
	dir.RemovedAtMs = nowMs
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Relation.RelationEpoch}, nil
}

// PairValidationSnapshot is the read model returned by ValidatePairForAdd
// (33 §5.2). target_relation_epoch binds both reservations and the final
// activation.
type PairValidationSnapshot struct {
	PairID                   string
	RelationID               string
	CurrentRelationEpoch     uint64
	TargetRelationEpoch      uint64
	AlreadyFriends           bool
	Rejected                 bool
	Reason                   ReasonCode
	BarrierState             BarrierState
	BarrierRevision          uint64
	RequiredInviterFloor     uint64 // share generation floor for a given inviter, READY only
	LowShareGenerationFloor  uint64
	HighShareGenerationFloor uint64
	HasOpenTermination       bool
}

// ValidateForAdd implements the pair-side preconditions of redemption
// (33 §4.2 items 3–7 reduced to pair facts; token validity itself is the
// ShareIssuer's job and happens earlier).
func (r *PairRecord) ValidateForAdd(inviter, redeemer uint64) (PairValidationSnapshot, error) {
	snap := PairValidationSnapshot{
		PairID:               r.PairID,
		RelationID:           r.Relation.RelationID,
		CurrentRelationEpoch: r.Relation.RelationEpoch,
		TargetRelationEpoch:  r.Relation.RelationEpoch + 1,
		BarrierState:         r.Barrier.State,
		BarrierRevision:      r.Barrier.BarrierRevision,
		HasOpenTermination:   r.PendingTermination != nil && !r.PendingTermination.Completed,
	}
	if inviter == redeemer {
		snap.Rejected, snap.Reason = true, ReasonSelfAdd
		return snap, nil
	}
	if inviter != r.LowPlayerID && inviter != r.HighPlayerID {
		return snap, fmt.Errorf("%w: inviter %d not in pair", ErrInvalidCommand, inviter)
	}
	if redeemer != r.LowPlayerID && redeemer != r.HighPlayerID {
		return snap, fmt.Errorf("%w: redeemer %d not in pair", ErrInvalidCommand, redeemer)
	}
	if r.Relation.Status == RelationStatusActive {
		snap.AlreadyFriends = true
		return snap, nil
	}
	if r.HasActiveBlock() {
		snap.Rejected, snap.Reason = true, ReasonBlocked
		return snap, nil
	}
	switch r.Barrier.State {
	case BarrierPending:
		snap.Rejected, snap.Reason = true, ReasonRelationReactivationPending
		return snap, nil
	case BarrierReady:
		snap.LowShareGenerationFloor = r.Barrier.LowShareGenerationFloor
		snap.HighShareGenerationFloor = r.Barrier.HighShareGenerationFloor
		if inviter == r.LowPlayerID {
			snap.RequiredInviterFloor = r.Barrier.LowShareGenerationFloor
		} else {
			snap.RequiredInviterFloor = r.Barrier.HighShareGenerationFloor
		}
	case BarrierConsumed:
		// A consumed barrier paired with a non-ACTIVE relation is
		// unreachable through legal transitions; fail closed.
		snap.Rejected, snap.Reason = true, ReasonRelationReactivationPending
		return snap, nil
	}
	return snap, nil
}

// ActivateCommand carries everything the atomic ActivateFriendRelation
// commit must re-validate (33 §5.2, 33A §4.6).
type ActivateCommand struct {
	InviterPlayerID      uint64
	RedeemerPlayerID     uint64
	TargetRelationEpoch  uint64
	TokenID              string
	TokenGeneration      uint64
	RotateReceipt        RotateReceipt
	RotateReceiptPresent bool // deprecated compatibility field; never proves reactivation
	OperationID          string
	NowMs                int64
}

// ApplyActivate performs the atomic activation. On reactivation it consumes
// the READY barrier in the same commit (33 §3.3).
func (r *PairRecord) ApplyActivate(cmd ActivateCommand) (Outcome, error) {
	if r.Relation.Status == RelationStatusActive {
		return Outcome{Status: StatusNoChange, Reason: ReasonAlreadyFriends, CommittedEpoch: r.Relation.RelationEpoch}, nil
	}
	if r.Relation.RelationEpoch+1 != cmd.TargetRelationEpoch {
		return Outcome{Status: StatusRejected, Reason: ReasonDomainEpochConflict, CommittedEpoch: r.Relation.RelationEpoch}, nil
	}
	if r.HasActiveBlock() {
		return Outcome{Status: StatusRejected, Reason: ReasonBlocked, CommittedEpoch: r.Relation.RelationEpoch}, nil
	}
	reactivation := r.Relation.RelationEpoch > 0 || r.Barrier.State != BarrierNone
	if reactivation {
		if r.Barrier.State != BarrierReady {
			return Outcome{Status: StatusRejected, Reason: ReasonRelationReactivationPending, CommittedEpoch: r.Relation.RelationEpoch}, nil
		}
		floor := r.Barrier.LowShareGenerationFloor
		if cmd.InviterPlayerID == r.HighPlayerID {
			floor = r.Barrier.HighShareGenerationFloor
		}
		if cmd.TokenGeneration <= floor {
			return Outcome{Status: StatusRejected, Reason: ReasonShareTokenPredatesRelationDel, CommittedEpoch: r.Relation.RelationEpoch}, nil
		}
		receipt := cmd.RotateReceipt
		expectedReceiptID, err := ReactivationRotateReceiptID(r.PairID, r.Barrier.BarrierRevision, cmd.TokenID)
		if err != nil || receipt.ReceiptID != expectedReceiptID || receipt.PairID != r.PairID ||
			receipt.TerminatedRelationEpoch != r.Barrier.TerminatedRelationEpoch ||
			receipt.BarrierRevision != r.Barrier.BarrierRevision || receipt.TokenID != cmd.TokenID ||
			receipt.TokenGeneration != cmd.TokenGeneration ||
			receipt.LowShareGenerationFloor != r.Barrier.LowShareGenerationFloor ||
			receipt.HighShareGenerationFloor != r.Barrier.HighShareGenerationFloor || receipt.CommittedAtMs <= 0 {
			return Outcome{Status: StatusRejected, Reason: ReasonReactivationRotateRequired, CommittedEpoch: r.Relation.RelationEpoch}, nil
		}
	}
	r.Relation.Status = RelationStatusActive
	r.Relation.RelationEpoch = cmd.TargetRelationEpoch
	r.Relation.ActivatedAtMs = cmd.NowMs
	r.Relation.DeletedAtMs = 0
	r.Relation.ActivationTokenID = cmd.TokenID
	r.Relation.ActivationTokenGeneration = cmd.TokenGeneration
	r.Relation.TerminationReason = TerminationNone
	r.Relation.TerminatedByPlayerID = 0
	r.Relation.LastChangeOperationID = cmd.OperationID
	if reactivation {
		r.Barrier.State = BarrierConsumed
	}
	r.PendingTermination = nil
	r.UpdatedAtMs = cmd.NowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Relation.RelationEpoch}, nil
}

// CommitReactivationGenerationFloors writes both share generation floors
// and flips PENDING→READY, only if the pair is still the same DELETED epoch
// and the barrier is still PENDING (33A §4.4). A late order is NO_CHANGE.
func (r *PairRecord) CommitReactivationGenerationFloors(expectedDeletedEpoch, barrierRevision, lowFloor, highFloor uint64, nowMs int64) Outcome {
	if r.Barrier.State != BarrierPending ||
		r.Barrier.TerminatedRelationEpoch+1 != expectedDeletedEpoch ||
		r.Barrier.BarrierRevision != barrierRevision ||
		r.Relation.RelationEpoch != expectedDeletedEpoch {
		return Outcome{Status: StatusNoChange, Reason: ReasonDomainEpochConflict, CommittedEpoch: r.Relation.RelationEpoch}
	}
	r.Barrier.LowShareGenerationFloor = lowFloor
	r.Barrier.HighShareGenerationFloor = highFloor
	r.Barrier.State = BarrierReady
	if r.PendingTermination != nil && r.PendingTermination.OrderID != "" {
		r.PendingTermination.LowFloorCaptured = true
		r.PendingTermination.HighFloorCaptured = true
		r.PendingTermination.LowShareGenerationFloor = lowFloor
		r.PendingTermination.HighShareGenerationFloor = highFloor
	}
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Relation.RelationEpoch}
}

// BarrierProof is the signed-by-read proof the inviter must present to
// RotateForPairReactivation (33A §6.4).
type BarrierProof struct {
	State                    BarrierState
	PairID                   string
	TerminatedRelationEpoch  uint64
	BarrierRevision          uint64
	LowShareGenerationFloor  uint64
	HighShareGenerationFloor uint64
}

// GetBarrierProof reads the current READY barrier for the inviter's client
// flow (29 §7.12B GetReactivationBarrierProof).
func (r *PairRecord) GetBarrierProof() (BarrierProof, error) {
	if r.Barrier.State != BarrierReady {
		return BarrierProof{}, fmt.Errorf("%w: barrier state %s", ErrBarrierProofMismatch, r.Barrier.State)
	}
	return BarrierProof{
		State:                    r.Barrier.State,
		PairID:                   r.PairID,
		TerminatedRelationEpoch:  r.Barrier.TerminatedRelationEpoch,
		BarrierRevision:          r.Barrier.BarrierRevision,
		LowShareGenerationFloor:  r.Barrier.LowShareGenerationFloor,
		HighShareGenerationFloor: r.Barrier.HighShareGenerationFloor,
	}, nil
}

func terminationOrderID(pairID, relationID string, capturedEpoch uint64) string {
	id, err := stableID("farm-friend-termination-order-id-v1", pairID, relationID, itoa(capturedEpoch))
	if err != nil {
		// stableID only fails on invalid characters; pair/relation ids are
		// already stable opaque ids, so this is unreachable in practice.
		panic(err)
	}
	return id
}
