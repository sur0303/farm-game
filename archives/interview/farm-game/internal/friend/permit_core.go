package friend

// VisitPermit and ActionPermit core (contract 33 §7/§8).
//
// The two permits are separate capabilities: a VisitPermit only authorizes
// creating one watching subscription (AUTHORIZED at Join time); an
// ActionPermit is issued per operation_id by an atomic Pair admission commit
// and is the Care/Theft Execution-Accept. Both bind the relation_epoch at
// issue time; stale epochs fail closed. Permits are internal capabilities —
// H5 never receives, stores or replays them.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// DefaultVisitPermitTTLMs is the configured VisitPermit TTL candidate
// (33 §7.1: 10 seconds).
const DefaultVisitPermitTTLMs int64 = 10_000

// DefaultActionWorkflowTTLMs is independent from the short admission permit.
// Pair freezes its absolute deadline at Execution-Accept so recovery never
// derives workflow lifetime from permit expiry or from a restart clock.
const DefaultActionWorkflowTTLMs int64 = 24 * 60 * 60 * 1000

// VisitPermitSnapshot is the internal short-lived join authorization
// (33 §7.1). It is evaluated once at Join authorization and bound to one
// join attempt; it never authorizes Care/Theft.
type VisitPermitSnapshot struct {
	PermitID            string
	ViewerPlayerID      uint64
	OwnerPlayerID       uint64
	TargetFarmID        uint64
	RelationID          string
	RelationEpoch       uint64
	JoinOperationID     string
	JoinAttemptID       string
	Scope               ActionScope // always ActionScopeVisitFarm
	IssuedAtAuthorityMs int64
	ExpireAtAuthorityMs int64
	PayloadHash         []byte
}

// ActionPermitSnapshot is the per-operation business authorization frozen
// by CommitFriendActionAdmission (33 §8.3). Worker recovery validates the
// frozen execution_accepted timestamp, never the current wall clock.
type ActionPermitSnapshot struct {
	PermitID                       string
	RelationID                     string
	RelationEpoch                  uint64
	VisitorPlayerID                uint64
	OwnerPlayerID                  uint64
	TargetFarmID                   uint64
	OperationID                    string
	Scope                          ActionScope // CARE | THEFT
	PayloadHash                    []byte
	TargetBindingHash              []byte
	IssuedAtAuthorityMs            int64
	ExpireAtAuthorityMs            int64
	ExecutionAcceptedAtAuthorityMs int64
}

// VisitAuthorizer issues VisitPermit snapshots off the linearized pair
// read. AUTHORIZING is only a pre-check: every later Care/Theft still
// validates relation_epoch and target facts (33 §7.2).
type VisitAuthorizer struct {
	Pairs PairRepository
}

// NewVisitAuthorizer binds the authorizer to the pair authority port.
func NewVisitAuthorizer(pairs PairRepository) (*VisitAuthorizer, error) {
	if pairs == nil {
		return nil, fmt.Errorf("%w: pair repository", ErrInvalidCommand)
	}
	return &VisitAuthorizer{Pairs: pairs}, nil
}

// AuthorizeVisit performs the linearized authorization read of one friend
// Join (33 §7.2/7.3). The Hub has already registered its AUTHORIZING record;
// a delete committing before this read yields VISIT_NOT_PERMITTED.
func (a *VisitAuthorizer) AuthorizeVisit(ctx context.Context, viewer, owner, targetFarmID uint64, joinOperationID, joinAttemptID string, nowMs, ttlMs int64) (VisitPermitSnapshot, Outcome, error) {
	if !ValidPair(viewer, owner) || targetFarmID != owner || joinOperationID == "" || joinAttemptID == "" {
		return VisitPermitSnapshot{}, Outcome{}, fmt.Errorf("%w: visit fields", ErrInvalidCommand)
	}
	if ttlMs <= 0 {
		ttlMs = DefaultVisitPermitTTLMs
	}
	snap, err := a.Pairs.GetPair(ctx, viewer, owner)
	if err != nil {
		return VisitPermitSnapshot{}, Outcome{}, err
	}
	if !snap.CanIssuePermit(viewer, owner) {
		return VisitPermitSnapshot{}, Outcome{Status: StatusRejected, Reason: ReasonVisitNotPermitted}, nil
	}
	permitID, err := stableID("farm-visit-permit-id-v1", snap.PairID, itoa(viewer), joinOperationID, joinAttemptID)
	if err != nil {
		return VisitPermitSnapshot{}, Outcome{}, err
	}
	permit := VisitPermitSnapshot{
		PermitID:            permitID,
		ViewerPlayerID:      viewer,
		OwnerPlayerID:       owner,
		TargetFarmID:        targetFarmID,
		RelationID:          snap.Relation.RelationID,
		RelationEpoch:       snap.Relation.RelationEpoch,
		JoinOperationID:     joinOperationID,
		JoinAttemptID:       joinAttemptID,
		Scope:               ActionScopeVisitFarm,
		IssuedAtAuthorityMs: nowMs,
		ExpireAtAuthorityMs: nowMs + ttlMs,
		PayloadHash:         visitPermitHash(permitID, snap, viewer, owner, joinOperationID, joinAttemptID, nowMs, ttlMs),
	}
	return permit, Outcome{Status: StatusSucceeded, CommittedEpoch: snap.Relation.RelationEpoch}, nil
}

// BindCheck revalidates a permit when the Hub binds it to the pre-existing
// AUTHORIZING record (33 §7.3): the permit must be unexpired at bind time
// and the pair must still authorize at exactly the permit's epoch.
func (a *VisitAuthorizer) BindCheck(ctx context.Context, permit VisitPermitSnapshot, nowMs int64) (Outcome, error) {
	if permit.Scope != ActionScopeVisitFarm || permit.RelationEpoch == 0 || permit.PermitID == "" {
		return Outcome{}, fmt.Errorf("%w: permit shape", ErrInvalidCommand)
	}
	if nowMs >= permit.ExpireAtAuthorityMs {
		return Outcome{Status: StatusRejected, Reason: ReasonVisitNotPermitted, Detail: "VISIT_PERMIT_EXPIRED"}, nil
	}
	snap, err := a.Pairs.GetPair(ctx, permit.ViewerPlayerID, permit.OwnerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	if !snap.CanIssuePermit(permit.ViewerPlayerID, permit.OwnerPlayerID) ||
		snap.Relation.RelationEpoch != permit.RelationEpoch {
		// Stale epoch or revoked relation: fail closed (33 §2.3).
		return Outcome{Status: StatusRejected, Reason: ReasonDomainEpochConflict}, nil
	}
	return Outcome{Status: StatusSucceeded, CommittedEpoch: permit.RelationEpoch}, nil
}

// VerifyAdmittedPermit validates a frozen ActionPermitSnapshot during
// dispatch/worker recovery (33 §8.5): the permit must be well-formed and its
// execution_accepted timestamp must have been inside the valid window at
// acceptance time. It never re-reads the current relation — deletion after
// acceptance only blocks NEW operations.
func VerifyAdmittedPermit(permit ActionPermitSnapshot) error {
	if permit.PermitID == "" || permit.RelationID == "" || permit.RelationEpoch == 0 ||
		!ValidPair(permit.VisitorPlayerID, permit.OwnerPlayerID) ||
		permit.TargetFarmID != permit.OwnerPlayerID ||
		(permit.Scope != ActionScopeCare && permit.Scope != ActionScopeTheft) ||
		permit.OperationID == "" || len(permit.PayloadHash) == 0 ||
		len(permit.TargetBindingHash) == 0 {
		return fmt.Errorf("%w: action permit shape", ErrInvalidCommand)
	}
	if permit.ExecutionAcceptedAtAuthorityMs < permit.IssuedAtAuthorityMs ||
		permit.ExecutionAcceptedAtAuthorityMs >= permit.ExpireAtAuthorityMs {
		return fmt.Errorf("%w: acceptance outside valid window", ErrInvalidCommand)
	}
	return nil
}

// ActionPermitHMAC derives an internal signature for an ActionPermitSnapshot
// (33A §12: permits cross services only over trusted RPC; the HMAC lets the
// Farm side verify integrity without remote I/O). The key is deployment
// configuration, never logged.
func ActionPermitHMAC(secret []byte, permit ActionPermitSnapshot) ([]byte, error) {
	if len(secret) < 16 {
		return nil, fmt.Errorf("%w: permit secret", ErrInvalidCommand)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(permit.PermitID))
	mac.Write([]byte(permit.RelationID))
	var num [8]byte
	for _, v := range []uint64{permit.RelationEpoch, permit.VisitorPlayerID, permit.OwnerPlayerID,
		permit.TargetFarmID, uint64(permit.IssuedAtAuthorityMs), uint64(permit.ExpireAtAuthorityMs),
		uint64(permit.ExecutionAcceptedAtAuthorityMs)} {
		binary.BigEndian.PutUint64(num[:], v)
		mac.Write(num[:])
	}
	mac.Write([]byte(permit.OperationID))
	mac.Write([]byte{byte(permit.Scope)})
	mac.Write(permit.PayloadHash)
	mac.Write(permit.TargetBindingHash)
	return mac.Sum(nil), nil
}

func visitPermitHash(permitID string, snap PairSnapshot, viewer, owner uint64, joinOperationID, joinAttemptID string, nowMs, ttlMs int64) []byte {
	h := sha256.New()
	h.Write([]byte(permitID))
	h.Write([]byte(snap.PairID))
	var num [8]byte
	for _, v := range []uint64{snap.Relation.RelationEpoch, viewer, owner, uint64(nowMs), uint64(ttlMs)} {
		binary.BigEndian.PutUint64(num[:], v)
		h.Write(num[:])
	}
	h.Write([]byte(joinOperationID))
	h.Write([]byte(joinAttemptID))
	return h.Sum(nil)
}
