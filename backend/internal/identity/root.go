package identity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"unicode/utf8"
)

// ErrInvalidIdentity rejects malformed stable identity input.
var ErrInvalidIdentity = errors.New("identity: invalid identity input")

// RootAuthorityKind is a frozen stable root-authority name (contract 28
// §3.2). The operation root is derived by CommandCatalog and trusted
// targets; clients can never supply it.
type RootAuthorityKind string

const (
	RootKindPlayerActor     RootAuthorityKind = "PLAYER_ACTOR"
	RootKindFarmActor       RootAuthorityKind = "FARM_ACTOR"
	RootKindFriendPair      RootAuthorityKind = "FRIEND_PAIR"
	RootKindIndependentWork RootAuthorityKind = "INDEPENDENT_WORK"
)

// RootAuthorityKinds lists every registered root authority kind.
var RootAuthorityKinds = []RootAuthorityKind{
	RootKindPlayerActor,
	RootKindFarmActor,
	RootKindFriendPair,
	RootKindIndependentWork,
}

// OperationRootRef identifies the authority that isolates one business
// idempotency scope (contract 28 §3.2 / 31 §8.1).
type OperationRootRef struct {
	Kind RootAuthorityKind
	// CanonicalID is a decimal actor ID or a canonical pair sid1_ ID.
	CanonicalID string
}

// Validate enforces the frozen shape without interpreting the ID.
func (r OperationRootRef) Validate() error {
	switch r.Kind {
	case RootKindPlayerActor, RootKindFarmActor, RootKindFriendPair, RootKindIndependentWork:
	default:
		return fmt.Errorf("%w: root authority kind %q", ErrInvalidIdentity, r.Kind)
	}
	if r.CanonicalID == "" || len(r.CanonicalID) > 128 || !utf8.ValidString(r.CanonicalID) {
		return fmt.Errorf("%w: canonical root authority id", ErrInvalidIdentity)
	}
	return nil
}

// EffectiveIdempotencyKey is the root-scoped business idempotency key
// (contract 27 §9.2 / 28 §3.2). Same key + same canonical payload hash
// returns the original result; same key + different hash is
// IDEMPOTENCY_CONFLICT.
type EffectiveIdempotencyKey struct {
	SubjectPlayerID uint64
	// StableCommandTypeName is the registry-frozen uppercase command name.
	StableCommandTypeName string
	OperationID           string
	Root                  OperationRootRef
}

// Validate enforces the frozen field shapes of contract 31 §8.1.
func (k EffectiveIdempotencyKey) Validate() error {
	if k.SubjectPlayerID == 0 {
		return fmt.Errorf("%w: subject player id", ErrInvalidIdentity)
	}
	if !validStableName(k.StableCommandTypeName) {
		return fmt.Errorf("%w: stable command type name", ErrInvalidIdentity)
	}
	if k.OperationID == "" || len(k.OperationID) > 64 || !utf8.ValidString(k.OperationID) {
		return fmt.Errorf("%w: operation id", ErrInvalidIdentity)
	}
	return k.Root.Validate()
}

// OperationDigest is the lowercase-hex SHA-256 locator frozen by contract
// 31 §8.1:
//
//	SHA-256(LP(subject) || LP(command) || LP(operation_id) ||
//	        LP(root_kind) || LP(canonical_root_id))
//
// The digest only locates a receipt/result; the stored full key and
// payload hash still decide conflict vs replay.
func (k EffectiveIdempotencyKey) OperationDigest() (string, error) {
	if err := k.Validate(); err != nil {
		return "", err
	}
	subject, err := CanonicalUint(k.SubjectPlayerID)
	if err != nil {
		return "", err
	}
	preimage := appendLP(nil, subject)
	preimage = appendLP(preimage, k.StableCommandTypeName)
	preimage = appendLP(preimage, k.OperationID)
	preimage = appendLP(preimage, string(k.Root.Kind))
	preimage = appendLP(preimage, k.Root.CanonicalID)
	digest := sha256.Sum256(preimage)
	return hex.EncodeToString(digest[:]), nil
}

// appendLP appends LP(x) = uint32_big_endian(len(UTF8(x))) || UTF8(x), the
// same frozen framing as internal/workidentity (contract 31 §8.1). It is
// duplicated here only because workidentity does not export its helper;
// both implementations are pinned by shared golden vectors.
func appendLP(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
