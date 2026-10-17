package friend

// Share issuer authority (contract 33 §4; 33A §6).
//
// Each inviter has at most one current valid share token. The same token may
// be redeemed by many different logged-in players; every redemption writes a
// bounded ShareTokenValidationReceipt (the acceptance point) but the token
// keeps no global remaining-use counter. Rotate/Revoke only blocks future
// admissions; they never cancel an already-accepted in-flight redemption.

import (
	"fmt"

	"farmgame/internal/socialprofile"
)

func defaultShareTokenTTLMs() int64 { return socialprofile.Current().ShareTokenTTL.Milliseconds() }

// ShareToken is the server-issued bearer token state (33 §4.1).
type ShareToken struct {
	TokenID         string
	TokenGeneration uint64
	IssuedAtMs      int64
	ExpiresAtMs     int64
	Status          TokenStatus
	Purpose         SharePurpose
	FormatVersion   uint32
}

// ValidationReceipt is the persisted token admission acceptance point
// (33 §4.2.1, Q33-16).
type ValidationReceipt struct {
	ReceiptID        string
	WorkflowID       string
	TokenID          string
	TokenGeneration  uint64
	InviterPlayerID  uint64
	RedeemerPlayerID uint64
	CommittedAtMs    int64
}

// RotateReceipt proves the inviter explicitly rotated for one pair
// reactivation barrier (33 §3.3, 33A §6.4).
type RotateReceipt struct {
	ReceiptID                string
	PairID                   string
	TerminatedRelationEpoch  uint64
	BarrierRevision          uint64
	LowShareGenerationFloor  uint64
	HighShareGenerationFloor uint64
	TokenID                  string
	TokenGeneration          uint64
	// OperationID and PayloadHash bind the proof to the exact persisted
	// RotateForPairReactivation authority receipt. Pair activation must not
	// accept caller-supplied scalar fields as proof on their own.
	OperationID   string
	PayloadHash   []byte
	CommittedAtMs int64
}

// ShareIssuerRecord is the per-inviter share authority record (33 §4.5).
type ShareIssuerRecord struct {
	InviterPlayerID    uint64
	Current            ShareToken
	ValidationReceipts map[string]ValidationReceipt // receipt_id -> receipt (bounded by GC)
	RotateReceipts     map[string]RotateReceipt     // pair_id -> latest rotate receipt
	RecordVersion      uint64
	UpdatedAtMs        int64
}

// NewShareIssuerRecord creates an empty issuer record.
func NewShareIssuerRecord(inviter uint64) (*ShareIssuerRecord, error) {
	if inviter == 0 {
		return nil, fmt.Errorf("%w: inviter id", ErrInvalidCommand)
	}
	return &ShareIssuerRecord{
		InviterPlayerID:    inviter,
		ValidationReceipts: make(map[string]ValidationReceipt),
		RotateReceipts:     make(map[string]RotateReceipt),
	}, nil
}

// usable reports whether the current token can still be returned/accepted at
// trusted authority time nowMs. Expiry is derived, never a stored mutation.
func (r *ShareIssuerRecord) usable(nowMs int64) bool {
	return r.Current.Status == TokenStatusActive &&
		r.Current.TokenID != "" && nowMs < r.Current.ExpiresAtMs
}

// GenerateShareToken returns the current link while it is valid; otherwise
// advances generation and issues a fresh token (33 §4.5). Repeat "share"
// clicks must not invalidate a link already handed to others.
func (r *ShareIssuerRecord) GenerateShareToken(newTokenID string, ttlMs, nowMs int64) (ShareToken, Outcome, error) {
	if ttlMs <= 0 {
		ttlMs = defaultShareTokenTTLMs()
	}
	if r.usable(nowMs) {
		return r.Current, Outcome{Status: StatusNoChange, CommittedEpoch: r.Current.TokenGeneration}, nil
	}
	if newTokenID == "" {
		return ShareToken{}, Outcome{}, fmt.Errorf("%w: token id", ErrInvalidCommand)
	}
	r.Current = ShareToken{
		TokenID:         newTokenID,
		TokenGeneration: r.Current.TokenGeneration + 1,
		IssuedAtMs:      nowMs,
		ExpiresAtMs:     nowMs + ttlMs,
		Status:          TokenStatusActive,
		Purpose:         SharePurposeAddFriend,
		FormatVersion:   1,
	}
	r.UpdatedAtMs = nowMs
	return r.Current, Outcome{Status: StatusSucceeded, CommittedEpoch: r.Current.TokenGeneration}, nil
}

// RotateShareToken always advances generation, revokes the old token and
// installs the candidate (33 §4.5).
func (r *ShareIssuerRecord) RotateShareToken(newTokenID string, ttlMs, nowMs int64) (ShareToken, Outcome, error) {
	if newTokenID == "" {
		return ShareToken{}, Outcome{}, fmt.Errorf("%w: token id", ErrInvalidCommand)
	}
	if ttlMs <= 0 {
		ttlMs = defaultShareTokenTTLMs()
	}
	r.Current = ShareToken{
		TokenID:         newTokenID,
		TokenGeneration: r.Current.TokenGeneration + 1,
		IssuedAtMs:      nowMs,
		ExpiresAtMs:     nowMs + ttlMs,
		Status:          TokenStatusActive,
		Purpose:         SharePurposeAddFriend,
		FormatVersion:   1,
	}
	r.UpdatedAtMs = nowMs
	return r.Current, Outcome{Status: StatusSucceeded, CommittedEpoch: r.Current.TokenGeneration}, nil
}

// RevokeShareToken advances generation and revokes the current token without
// issuing a replacement (33 §4.5).
func (r *ShareIssuerRecord) RevokeShareToken(nowMs int64) Outcome {
	r.Current.TokenGeneration++
	r.Current.Status = TokenStatusRevoked
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: r.Current.TokenGeneration}
}

// RotateForPairReactivation rotates the token and records a typed
// RotateReceipt bound to the exact barrier proof read from the Pair primary
// (33A §6.4). A plain Rotate never satisfies a deleted pair's proof.
func (r *ShareIssuerRecord) RotateForPairReactivation(proof BarrierProof, newTokenID string, ttlMs, nowMs int64) (ShareToken, RotateReceipt, Outcome, error) {
	if proof.State != BarrierReady || proof.PairID == "" || proof.TerminatedRelationEpoch == 0 || proof.BarrierRevision == 0 {
		return ShareToken{}, RotateReceipt{}, Outcome{}, ErrBarrierProofMismatch
	}
	token, outcome, err := r.RotateShareToken(newTokenID, ttlMs, nowMs)
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	receiptID, err := ReactivationRotateReceiptID(proof.PairID, proof.BarrierRevision, token.TokenID)
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	receipt := RotateReceipt{
		ReceiptID:                receiptID,
		PairID:                   proof.PairID,
		TerminatedRelationEpoch:  proof.TerminatedRelationEpoch,
		BarrierRevision:          proof.BarrierRevision,
		LowShareGenerationFloor:  proof.LowShareGenerationFloor,
		HighShareGenerationFloor: proof.HighShareGenerationFloor,
		TokenID:                  token.TokenID,
		TokenGeneration:          token.TokenGeneration,
		CommittedAtMs:            nowMs,
	}
	r.RotateReceipts[proof.PairID] = receipt
	return token, receipt, outcome, nil
}

// HasRotateReceiptFor reports whether a typed rotate receipt exists for the
// pair at exactly the current barrier revision (33 §4.3).
func (r *ShareIssuerRecord) HasRotateReceiptFor(pairID string, barrierRevision uint64) (RotateReceipt, bool) {
	receipt, ok := r.RotateReceipts[pairID]
	if !ok || receipt.BarrierRevision != barrierRevision {
		return RotateReceipt{}, false
	}
	return receipt, true
}

// AcceptShareTokenAdmission is the redemption acceptance point (33 §4.2.1).
// It validates the presented token against the CURRENT issuer state and
// persists a bounded ValidationReceipt. It does not guarantee friendship —
// block/barrier/quota checks still run afterwards.
func (r *ShareIssuerRecord) AcceptShareTokenAdmission(tokenID string, tokenGeneration uint64, purpose SharePurpose, redeemer uint64, workflowID string, nowMs int64) (ValidationReceipt, Outcome, error) {
	if redeemer == 0 || workflowID == "" || tokenID == "" {
		return ValidationReceipt{}, Outcome{}, fmt.Errorf("%w: admission fields", ErrInvalidCommand)
	}
	if redeemer == r.InviterPlayerID {
		return ValidationReceipt{}, Outcome{Status: StatusRejected, Reason: ReasonSelfAdd}, nil
	}
	if purpose != SharePurposeAddFriend {
		return ValidationReceipt{}, Outcome{Status: StatusRejected, Reason: ReasonShareTokenPurposeMismatch}, nil
	}
	current := r.Current
	if current.Status == TokenStatusRevoked || current.TokenGeneration != tokenGeneration || current.TokenID != tokenID {
		return ValidationReceipt{}, Outcome{Status: StatusRejected, Reason: ReasonShareTokenRevoked}, nil
	}
	if current.Status != TokenStatusActive {
		return ValidationReceipt{}, Outcome{Status: StatusRejected, Reason: ReasonShareTokenStaleGeneration}, nil
	}
	if nowMs >= current.ExpiresAtMs {
		return ValidationReceipt{}, Outcome{Status: StatusRejected, Reason: ReasonShareTokenExpired}, nil
	}
	receiptID, err := ShareTokenValidationReceiptID(workflowID, r.InviterPlayerID, redeemer, tokenGeneration)
	if err != nil {
		return ValidationReceipt{}, Outcome{}, err
	}
	if existing, ok := r.ValidationReceipts[receiptID]; ok {
		return existing, Outcome{Status: StatusSucceeded, CommittedEpoch: tokenGeneration}, nil
	}
	receipt := ValidationReceipt{
		ReceiptID:        receiptID,
		WorkflowID:       workflowID,
		TokenID:          tokenID,
		TokenGeneration:  tokenGeneration,
		InviterPlayerID:  r.InviterPlayerID,
		RedeemerPlayerID: redeemer,
		CommittedAtMs:    nowMs,
	}
	r.ValidationReceipts[receiptID] = receipt
	r.UpdatedAtMs = nowMs
	return receipt, Outcome{Status: StatusSucceeded, CommittedEpoch: tokenGeneration}, nil
}

// GenerationSnapshot is the stable read used by FriendTerminationOrder to
// backfill reactivation floors (33A §6.7). Missing records report 0.
func (r *ShareIssuerRecord) GenerationSnapshot() uint64 {
	return r.Current.TokenGeneration
}
