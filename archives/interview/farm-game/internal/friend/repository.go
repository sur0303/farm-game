package friend

// Repository ports of the three friendship authorities (contract 33 §1,
// 33A §0.2). Every mutating method is one atomic authority commit — the
// logical equivalent of one 33A Lua script. SO0 ships the memory adapter
// and contract tests; the production Redis adapter binds these ports to the
// R1 shared primitives in a later work package without changing semantics.
//
// Idempotency rule for every mutating method: replaying the same
// CommitIdentity with the same payload hash returns the stored Outcome;
// the same identity with a different payload hash returns
// ErrIdempotentConflict (33A §3.2). Business REJECTED/NO_CHANGE results are
// values inside Outcome, never Go errors.

import "context"

// PairMutationCommand covers DeleteFriend / BlockPlayer / UnblockPlayer.
type PairMutationCommand struct {
	Identity       CommitIdentity
	PayloadHash    []byte
	CallerPlayerID uint64
	PeerPlayerID   uint64
	NowMs          int64
}

// PairSnapshot is the linearized read model of one canonical pair. It is
// the only read allowed to issue permits or validate roster candidates;
// index/projection reads never authorize.
type PairSnapshot struct {
	PairID             string
	LowPlayerID        uint64
	HighPlayerID       uint64
	Relation           PairRelation
	LowBlocksHigh      BlockRelation
	HighBlocksLow      BlockRelation
	Barrier            ReactivationBarrier
	PendingTermination *TerminationOrder
	RecordVersion      uint64
}

// Exists reports whether any pair fact has ever been persisted.
func (s PairSnapshot) Exists() bool {
	return s.Relation.Status != RelationStatusNone ||
		s.LowBlocksHigh.Status != BlockStatusNone || s.HighBlocksLow.Status != BlockStatusNone
}

// CanIssuePermit mirrors PairRecord.CanIssuePermit on the snapshot.
func (s PairSnapshot) CanIssuePermit(viewer, owner uint64) bool {
	r := PairRecord{
		LowPlayerID: s.LowPlayerID, HighPlayerID: s.HighPlayerID,
		Relation: s.Relation, LowBlocksHigh: s.LowBlocksHigh, HighBlocksLow: s.HighBlocksLow,
	}
	return r.CanIssuePermit(viewer, owner)
}

// HasActiveBlock reports whether either direction blocks the other.
func (s PairSnapshot) HasActiveBlock() bool {
	return s.LowBlocksHigh.Status == BlockStatusActive || s.HighBlocksLow.Status == BlockStatusActive
}

// ActionAdmissionCommand is the input of CommitFriendActionAdmission
// (33 §8.3). The pair authority derives the canonical pair itself; callers
// never supply relation_id or relation_epoch.
type ActionAdmissionCommand struct {
	Identity          CommitIdentity
	PayloadHash       []byte
	VisitorPlayerID   uint64
	OwnerPlayerID     uint64
	TargetFarmID      uint64
	Scope             ActionScope // CARE | THEFT
	TargetBindingHash []byte
	NowMs             int64
	PermitTTLMs       int64
	WorkflowTTLMs     int64
}

// ActionAdmissionOutcome carries the frozen permit and dispatch anchor
// produced by the admission commit.
type ActionAdmissionOutcome struct {
	Outcome              Outcome
	Permit               ActionPermitSnapshot
	DispatchOrderID      string
	WorkflowDeadlineAtMs int64
}

// PairRepository is the FRIEND_PAIR authority port.
type PairRepository interface {
	// GetPair performs the linearized primary read (33A §4.2). A pair with
	// no facts returns a zero snapshot with Exists() == false and no error.
	GetPair(ctx context.Context, playerA, playerB uint64) (PairSnapshot, error)

	DeleteFriend(ctx context.Context, cmd PairMutationCommand) (Outcome, error)
	BlockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error)
	UnblockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error)

	// ValidatePairForAdd is the pair-side pre-check of redemption.
	ValidatePairForAdd(ctx context.Context, inviter, redeemer uint64) (PairValidationSnapshot, error)

	// ActivateFriendRelation is only invoked by the trusted AddFriend
	// workflow participant; it re-validates every fence atomically.
	ActivateFriendRelation(ctx context.Context, cmd ActivateCommand, identity CommitIdentity, payloadHash []byte) (Outcome, error)

	// CommitReactivationGenerationFloors backfills both share floors and
	// flips the barrier PENDING→READY (33A §4.4).
	CommitReactivationGenerationFloors(ctx context.Context, playerA, playerB uint64, expectedDeletedEpoch, barrierRevision, lowFloor, highFloor uint64, nowMs int64) (Outcome, error)

	// CompleteTerminationOrder marks the open FriendTerminationOrder done
	// after floors and quota releases committed.
	CompleteTerminationOrder(ctx context.Context, playerA, playerB uint64, orderID string, nowMs int64) (Outcome, error)

	// GetBarrierProof reads the current READY barrier for the inviter's
	// explicit RotateForPairReactivation flow.
	GetBarrierProof(ctx context.Context, playerA, playerB uint64) (BarrierProof, error)

	// CommitFriendActionAdmission is the Care/Theft Execution-Accept
	// (33 §8.3, Q33-13).
	CommitFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error)

	// GetFriendActionAdmission resolves an admission after TIMEOUT_UNKNOWN:
	// same identity + same payload hash returns the original permit.
	GetFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error)
}

// PairRelationReader is the recovery-only lookup from an authoritative quota
// relation id to its canonical Pair. It lets FriendIndex reconciliation repair
// missing rows without scanning Redis keys.
type PairRelationReader interface {
	GetPairByRelationID(context.Context, string) (PairSnapshot, error)
}

// QuotaRepository is the FRIEND_QUOTA authority port. Each method touches
// exactly one player's record.
type QuotaRepository interface {
	GetQuota(ctx context.Context, playerID uint64) (QuotaSnapshot, error)
	ReserveFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error)
	FinalizeFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error)
	ReleaseFriendReservation(ctx context.Context, playerID uint64, reservationID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error)
	RemoveActiveRelationIfEpochMatches(ctx context.Context, playerID uint64, relationID string, capturedActiveEpoch uint64, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error)
}

// QuotaSnapshot is the read model of one quota record.
type QuotaSnapshot struct {
	PlayerID            uint64
	ActiveRelations     map[string]uint64
	OpenReservations    int
	EffectiveMaxFriends int
	RecordVersion       uint64
}

// Occupancy implements the reserve invariant on the snapshot.
func (s QuotaSnapshot) Occupancy() int { return len(s.ActiveRelations) + s.OpenReservations }

// ShareIssuerRepository is the FRIEND_SHARE_ISSUER authority port.
type ShareIssuerRepository interface {
	GetIssuer(ctx context.Context, inviter uint64) (ShareIssuerSnapshot, error)
	GenerateShareToken(ctx context.Context, inviter uint64, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error)
	RotateShareToken(ctx context.Context, inviter uint64, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error)
	RotateForPairReactivation(ctx context.Context, inviter uint64, proof BarrierProof, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, RotateReceipt, Outcome, error)
	RevokeShareToken(ctx context.Context, inviter uint64, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error)
	AcceptShareTokenAdmission(ctx context.Context, inviter uint64, tokenID string, tokenGeneration uint64, redeemer uint64, workflowID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ValidationReceipt, Outcome, error)
	// GetGenerationSnapshot is used by FriendTerminationOrder to backfill
	// reactivation floors; a missing issuer reports generation 0.
	GetGenerationSnapshot(ctx context.Context, inviter uint64) (uint64, error)
}

// ShareIssuerSnapshot is the read model of one issuer record.
type ShareIssuerSnapshot struct {
	InviterPlayerID uint64
	Current         ShareToken
	RotateReceipts  map[string]RotateReceipt
	RecordVersion   uint64
}

// HasRotateReceiptFor mirrors ShareIssuerRecord.HasRotateReceiptFor.
func (s ShareIssuerSnapshot) HasRotateReceiptFor(pairID string, barrierRevision uint64) (RotateReceipt, bool) {
	receipt, ok := s.RotateReceipts[pairID]
	if !ok || receipt.BarrierRevision != barrierRevision {
		return RotateReceipt{}, false
	}
	return receipt, true
}
