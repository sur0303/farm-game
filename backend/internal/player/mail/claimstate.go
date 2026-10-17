package mail

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
)

// ErrInvalidClaimState rejects a malformed PlayerActor mail-claim carrier.
// The adapter is deliberately fail-closed: it never drops, merges or
// reconstructs a receipt that is already present in PlayerRecord.
var ErrInvalidClaimState = errors.New("mail: invalid claim state")

// MaxClaimReceipts is the shared Player Model V2 repeated-field bound.
const MaxClaimReceipts = fieldv2.MaxRepeatedItems

// MaxClaimRewardResultBytes keeps one opaque frozen reward outcome bounded by
// the same field envelope that carries MailClaimState. The state itself is
// additionally bounded by MaxClaimReceipts.
const MaxClaimRewardResultBytes = fieldv2.MaxFieldValueBytes

// ClaimReceipt is the domain value object for one successfully claimed mail
// attachment. ClaimMaterialHash is raw SHA-256 bytes; it is not a hex string.
type ClaimReceipt struct {
	MailID               string
	FirstOperationID     string
	MailVersion          uint64
	ClaimMaterialHash    []byte
	RewardResult         []byte
	ClaimDecisionAtMs    int64
	ClaimedAtMs          int64
	ReceiptRetainUntilMs int64
}

// ClaimState is the PlayerActor-side source of truth for mail attachment
// claims. Receipts are a sorted strict set keyed by MailID.
//
// A zero ClaimState is the valid empty state for a new player.
type ClaimState struct {
	StateVersion              uint64
	ProjectionDirtyGeneration uint64
	Receipts                  []ClaimReceipt
}

// MailClaimReceipt and MailClaimState are descriptive aliases for callers
// that want the protobuf/domain names to line up without importing proto
// types into business logic.
type MailClaimReceipt = ClaimReceipt
type MailClaimState = ClaimState

// ClaimLookupStatus is the exact result of a pure, already-loaded claim
// state lookup. UNKNOWN means the state or mail id cannot be trusted; absence
// from a valid state is UNCLAIMED.
type ClaimLookupStatus string

const (
	ClaimStatusClaimed   ClaimLookupStatus = "CLAIMED"
	ClaimStatusUnclaimed ClaimLookupStatus = "UNCLAIMED"
	ClaimStatusUnknown   ClaimLookupStatus = "UNKNOWN"

	// Short aliases keep the three-state vocabulary visible at call sites.
	CLAIMED   = ClaimStatusClaimed
	UNCLAIMED = ClaimStatusUnclaimed
	UNKNOWN   = ClaimStatusUnknown
)

// ClaimStatus is a compatibility alias for the three-state lookup result.
type ClaimStatus = ClaimLookupStatus

// Validate checks every persistence invariant of a ClaimReceipt.
func (r ClaimReceipt) Validate() error {
	if err := ValidateMailID(r.MailID); err != nil {
		return fmt.Errorf("%w: mail_id: %v", ErrInvalidClaimState, err)
	}
	if err := validateClaimOperationID(r.FirstOperationID); err != nil {
		return fmt.Errorf("%w: operation_id: %v", ErrInvalidClaimState, err)
	}
	if r.MailVersion == 0 {
		return fmt.Errorf("%w: mail_version", ErrInvalidClaimState)
	}
	if len(r.ClaimMaterialHash) != sha256.Size {
		return fmt.Errorf("%w: claim_material_hash must be %d raw bytes", ErrInvalidClaimState, sha256.Size)
	}
	if len(r.RewardResult) == 0 || len(r.RewardResult) > MaxClaimRewardResultBytes {
		return fmt.Errorf("%w: reward_result size", ErrInvalidClaimState)
	}
	if r.ClaimDecisionAtMs <= 0 || r.ClaimedAtMs <= 0 || r.ReceiptRetainUntilMs <= 0 {
		return fmt.Errorf("%w: receipt times", ErrInvalidClaimState)
	}
	if r.ClaimedAtMs < r.ClaimDecisionAtMs {
		return fmt.Errorf("%w: claimed_at_ms before claim_decision_at_ms", ErrInvalidClaimState)
	}
	if r.ReceiptRetainUntilMs <= r.ClaimedAtMs {
		return fmt.Errorf("%w: receipt_retain_until_ms", ErrInvalidClaimState)
	}
	return nil
}

// Validate checks the complete ClaimState invariant. Receipts must already
// be strictly sorted by mail id; the PlayerRecord adapter sorts its input
// before invoking this check.
func (s ClaimState) Validate() error {
	if len(s.Receipts) > MaxClaimReceipts {
		return fmt.Errorf("%w: receipts exceed %d", ErrInvalidClaimState, MaxClaimReceipts)
	}
	if len(s.Receipts) == 0 {
		if s.StateVersion == 0 && s.ProjectionDirtyGeneration == 0 {
			return nil
		}
		if s.StateVersion == 0 {
			return fmt.Errorf("%w: non-empty metadata without state_version", ErrInvalidClaimState)
		}
		return nil
	}
	if s.StateVersion == 0 {
		return fmt.Errorf("%w: receipts without state_version", ErrInvalidClaimState)
	}
	for i := range s.Receipts {
		if err := s.Receipts[i].Validate(); err != nil {
			return fmt.Errorf("%w: receipt %d: %v", ErrInvalidClaimState, i, err)
		}
		if i > 0 && s.Receipts[i-1].MailID >= s.Receipts[i].MailID {
			return fmt.Errorf("%w: receipts are not strictly sorted/deduplicated", ErrInvalidClaimState)
		}
	}
	return nil
}

// Clone returns a deep copy suitable for a candidate Actor turn.
func (s ClaimState) Clone() ClaimState {
	out := s
	if s.Receipts == nil {
		return out
	}
	out.Receipts = make([]ClaimReceipt, len(s.Receipts))
	for i := range s.Receipts {
		out.Receipts[i] = s.Receipts[i].clone()
	}
	return out
}

func (r ClaimReceipt) clone() ClaimReceipt {
	out := r
	out.ClaimMaterialHash = append([]byte(nil), r.ClaimMaterialHash...)
	out.RewardResult = append([]byte(nil), r.RewardResult...)
	return out
}

// StateFromPlayerRecord decodes only PlayerRecord.MailClaimState. A nil
// carrier is the valid empty state. Unordered receipts are normalized into
// mail-id order, while duplicate mail ids are rejected rather than silently
// choosing one operation as truth.
func StateFromPlayerRecord(record *playerpb.PlayerRecord) (ClaimState, error) {
	if record == nil {
		return ClaimState{}, fmt.Errorf("%w: nil PlayerRecord", ErrInvalidClaimState)
	}
	pb := record.GetMailClaimState()
	if pb == nil {
		return ClaimState{}, nil
	}
	state := ClaimState{
		StateVersion:              pb.GetStateVersion(),
		ProjectionDirtyGeneration: pb.GetProjectionDirtyGeneration(),
		Receipts:                  make([]ClaimReceipt, 0, len(pb.GetReceipts())),
	}
	if len(pb.GetReceipts()) > MaxClaimReceipts {
		return ClaimState{}, fmt.Errorf("%w: receipts exceed %d", ErrInvalidClaimState, MaxClaimReceipts)
	}
	for i, receipt := range pb.GetReceipts() {
		if receipt == nil {
			return ClaimState{}, fmt.Errorf("%w: nil receipt at index %d", ErrInvalidClaimState, i)
		}
		decoded, err := claimReceiptFromProto(receipt)
		if err != nil {
			return ClaimState{}, fmt.Errorf("%w: receipt %d: %v", ErrInvalidClaimState, i, err)
		}
		state.Receipts = append(state.Receipts, decoded)
	}
	if len(state.Receipts) == 0 {
		state.Receipts = nil
	} else {
		sort.Slice(state.Receipts, func(i, j int) bool {
			return state.Receipts[i].MailID < state.Receipts[j].MailID
		})
	}
	if err := state.Validate(); err != nil {
		return ClaimState{}, err
	}
	return state, nil
}

// ClaimStateFromPlayerRecord is the explicit mail-claim-named form of
// StateFromPlayerRecord.
func ClaimStateFromPlayerRecord(record *playerpb.PlayerRecord) (ClaimState, error) {
	return StateFromPlayerRecord(record)
}

// MailClaimStateFromPlayerRecord is an alias for integrations that use the
// full protobuf field name.
func MailClaimStateFromPlayerRecord(record *playerpb.PlayerRecord) (MailClaimState, error) {
	return StateFromPlayerRecord(record)
}

// ApplyStateToPlayerRecord writes a complete, validated MailClaimState.
// Empty state is canonicalized to a nil optional protobuf message.
func ApplyStateToPlayerRecord(record *playerpb.PlayerRecord, state ClaimState) error {
	if record == nil {
		return fmt.Errorf("%w: nil PlayerRecord", ErrInvalidClaimState)
	}
	if err := state.Validate(); err != nil {
		return err
	}
	if len(state.Receipts) == 0 && state.StateVersion == 0 && state.ProjectionDirtyGeneration == 0 {
		record.MailClaimState = nil
		return nil
	}
	pb := &playerpb.MailClaimState{
		StateVersion:              state.StateVersion,
		ProjectionDirtyGeneration: state.ProjectionDirtyGeneration,
	}
	if len(state.Receipts) > 0 {
		pb.Receipts = make([]*playerpb.MailClaimReceipt, len(state.Receipts))
		for i := range state.Receipts {
			pb.Receipts[i] = claimReceiptToProto(state.Receipts[i])
		}
	}
	record.MailClaimState = pb
	return nil
}

// ApplyClaimStateToPlayerRecord is the explicit mail-claim-named form of
// ApplyStateToPlayerRecord.
func ApplyClaimStateToPlayerRecord(record *playerpb.PlayerRecord, state ClaimState) error {
	return ApplyStateToPlayerRecord(record, state)
}

// ApplyMailClaimStateToPlayerRecord is an alias for integrations that use
// the full protobuf field name.
func ApplyMailClaimStateToPlayerRecord(record *playerpb.PlayerRecord, state MailClaimState) error {
	return ApplyStateToPlayerRecord(record, state)
}

func claimReceiptFromProto(pb *playerpb.MailClaimReceipt) (ClaimReceipt, error) {
	r := ClaimReceipt{
		MailID:               pb.GetMailId(),
		FirstOperationID:     pb.GetFirstOperationId(),
		MailVersion:          pb.GetMailVersion(),
		ClaimMaterialHash:    append([]byte(nil), pb.GetClaimMaterialHash()...),
		RewardResult:         append([]byte(nil), pb.GetRewardResult()...),
		ClaimDecisionAtMs:    pb.GetClaimDecisionAtMs(),
		ClaimedAtMs:          pb.GetClaimedAtMs(),
		ReceiptRetainUntilMs: pb.GetReceiptRetainUntilMs(),
	}
	if err := r.Validate(); err != nil {
		return ClaimReceipt{}, err
	}
	return r, nil
}

func claimReceiptToProto(r ClaimReceipt) *playerpb.MailClaimReceipt {
	return &playerpb.MailClaimReceipt{
		MailId:               r.MailID,
		FirstOperationId:     r.FirstOperationID,
		MailVersion:          r.MailVersion,
		ClaimMaterialHash:    append([]byte(nil), r.ClaimMaterialHash...),
		RewardResult:         append([]byte(nil), r.RewardResult...),
		ClaimDecisionAtMs:    r.ClaimDecisionAtMs,
		ClaimedAtMs:          r.ClaimedAtMs,
		ReceiptRetainUntilMs: r.ReceiptRetainUntilMs,
	}
}

// LookupClaimStatus is a pure lookup over an already-read ClaimState. It
// never reaches Redis or another repository. A valid empty state returns
// UNCLAIMED for a valid mail id; nil/invalid state or an invalid mail id is
// UNKNOWN.
func LookupClaimStatus(state *ClaimState, mailID string) ClaimLookupStatus {
	if state == nil || ValidateMailID(mailID) != nil || state.Validate() != nil {
		return ClaimStatusUnknown
	}
	index := sort.Search(len(state.Receipts), func(i int) bool {
		return state.Receipts[i].MailID >= mailID
	})
	if index < len(state.Receipts) && state.Receipts[index].MailID == mailID {
		return ClaimStatusClaimed
	}
	return ClaimStatusUnclaimed
}

// ClaimStatus returns the three-state result for one mail id.
func (s ClaimState) ClaimStatus(mailID string) ClaimLookupStatus {
	return LookupClaimStatus(&s, mailID)
}

// Lookup is a concise method form of ClaimStatus.
func (s ClaimState) Lookup(mailID string) ClaimLookupStatus {
	return s.ClaimStatus(mailID)
}

// ReceiptFor returns a defensive copy of the receipt for mailID.
func (s ClaimState) ReceiptFor(mailID string) (ClaimReceipt, bool) {
	if LookupClaimStatus(&s, mailID) != ClaimStatusClaimed {
		return ClaimReceipt{}, false
	}
	index := sort.Search(len(s.Receipts), func(i int) bool {
		return s.Receipts[i].MailID >= mailID
	})
	return s.Receipts[index].clone(), true
}

// RecordClaim returns the next authoritative claim state. MailID is a set
// key: a later operation may observe the original receipt, but it can never
// replace the operation that first granted the attachment. Versions advance
// exactly once for a newly claimed mail.
func (s ClaimState) RecordClaim(receipt ClaimReceipt) (ClaimState, bool, error) {
	if err := s.Validate(); err != nil {
		return ClaimState{}, false, err
	}
	if err := receipt.Validate(); err != nil {
		return ClaimState{}, false, err
	}
	index := sort.Search(len(s.Receipts), func(i int) bool {
		return s.Receipts[i].MailID >= receipt.MailID
	})
	if index < len(s.Receipts) && s.Receipts[index].MailID == receipt.MailID {
		return s.Clone(), false, nil
	}
	if len(s.Receipts) >= MaxClaimReceipts || s.StateVersion == ^uint64(0) || s.ProjectionDirtyGeneration == ^uint64(0) {
		return ClaimState{}, false, fmt.Errorf("%w: claim receipt capacity/version exhausted", ErrInvalidClaimState)
	}
	next := s.Clone()
	next.Receipts = append(next.Receipts, ClaimReceipt{})
	copy(next.Receipts[index+1:], next.Receipts[index:])
	next.Receipts[index] = receipt.clone()
	next.StateVersion++
	next.ProjectionDirtyGeneration++
	if err := next.Validate(); err != nil {
		return ClaimState{}, false, err
	}
	return next, true, nil
}

// ClaimStatusForMail is the value-input form for callers that already own a
// validated ClaimState value.
func ClaimStatusForMail(state ClaimState, mailID string) ClaimLookupStatus {
	return LookupClaimStatus(&state, mailID)
}

func validateClaimOperationID(operationID string) error {
	if len(operationID) == 0 || len(operationID) > 64 {
		return fmt.Errorf("operation_id must be 1..64 printable ASCII bytes")
	}
	for i := 0; i < len(operationID); i++ {
		if operationID[i] < 0x21 || operationID[i] > 0x7e {
			return fmt.Errorf("operation_id contains non-printable ASCII at byte %d", i)
		}
	}
	return nil
}
