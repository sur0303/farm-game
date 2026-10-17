package mail

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// ErrMalformedDueRef rejects a non-canonical DueIndex member.
var ErrMalformedDueRef = errors.New("mail: malformed due ref")

// DueKind identifies the maintenance discovery index (35A §7.1).
type DueKind string

const (
	DueKindExpireVisibility DueKind = "EXPIRE_VISIBILITY"
	DueKindPhysicalGC       DueKind = "PHYSICAL_GC"
)

func (k DueKind) valid() bool {
	return k == DueKindExpireVisibility || k == DueKindPhysicalGC
}

// MailDueRefV1 is the versioned discovery reference placed in the bucket
// expire-due / gc-due ZSETs (35A §7.1). The index is discovery only: the
// scripts re-check entry/state and Redis TIME before acting, and a stale
// ref yields NO_CHANGE, never a delete of a newer version.
type MailDueRefV1 struct {
	Kind                 DueKind
	PlayerID             uint64
	MailID               string
	MailVersion          uint64
	ExpectedStateVersion uint64
}

const dueRefPrefix = "mdr1_"

// EncodeDueRefV1 renders mdr1_ || Base64URL_NoPadding(LP fields), mirroring
// the contract-31 §8.1B reversible reference format.
func EncodeDueRefV1(ref MailDueRefV1) (string, error) {
	player, err := CanonicalPlayerID(ref.PlayerID)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrMalformedDueRef, err)
	}
	if !ref.Kind.valid() {
		return "", fmt.Errorf("%w: kind", ErrMalformedDueRef)
	}
	if err := ValidateMailID(ref.MailID); err != nil {
		return "", fmt.Errorf("%w: %v", ErrMalformedDueRef, err)
	}
	if ref.MailVersion == 0 || ref.ExpectedStateVersion == 0 {
		return "", fmt.Errorf("%w: versions", ErrMalformedDueRef)
	}
	raw, _ := appendLP(nil, string(ref.Kind))
	raw, _ = appendLP(raw, player)
	raw, _ = appendLP(raw, ref.MailID)
	raw, _ = appendLP(raw, itoa(ref.MailVersion))
	raw, _ = appendLP(raw, itoa(ref.ExpectedStateVersion))
	return dueRefPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeDueRefV1 parses a member and enforces canonical round-trip equality
// (decode-then-reencode must reproduce the input exactly).
func DecodeDueRefV1(encoded string) (MailDueRefV1, error) {
	if !strings.HasPrefix(encoded, dueRefPrefix) || len(encoded) > 256 {
		return MailDueRefV1{}, fmt.Errorf("%w: prefix or size", ErrMalformedDueRef)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, dueRefPrefix))
	if err != nil || dueRefPrefix+base64.RawURLEncoding.EncodeToString(payload) != encoded {
		return MailDueRefV1{}, fmt.Errorf("%w: base64url", ErrMalformedDueRef)
	}
	fields, err := parseLP(payload, 5)
	if err != nil {
		return MailDueRefV1{}, fmt.Errorf("%w: %v", ErrMalformedDueRef, err)
	}
	ref := MailDueRefV1{Kind: DueKind(fields[0]), MailID: fields[2]}
	if ref.PlayerID, err = parseUint(fields[1]); err != nil {
		return MailDueRefV1{}, fmt.Errorf("%w: player", ErrMalformedDueRef)
	}
	if ref.MailVersion, err = parseUint(fields[3]); err != nil {
		return MailDueRefV1{}, fmt.Errorf("%w: mail version", ErrMalformedDueRef)
	}
	if ref.ExpectedStateVersion, err = parseUint(fields[4]); err != nil {
		return MailDueRefV1{}, fmt.Errorf("%w: state version", ErrMalformedDueRef)
	}
	canonical, err := EncodeDueRefV1(ref)
	if err != nil || canonical != encoded {
		return MailDueRefV1{}, fmt.Errorf("%w: non-canonical due ref", ErrMalformedDueRef)
	}
	return ref, nil
}
