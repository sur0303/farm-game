package playermail

import (
	"errors"
	"fmt"
	"strings"

	"farmgame/internal/player/mail"
)

const receiptDuePrefix = "rdr1_"

// ReceiptDueKind identifies the two receipt families stored in a mailbox.
// The value is part of the versioned receipt-due member, not a caller-chosen
// Redis field prefix.
type ReceiptDueKind string

const (
	ReceiptDueMutation   ReceiptDueKind = "MUTATION"
	ReceiptDueProjection ReceiptDueKind = "PROJECTION"
)

// ReceiptDueRef identifies one receipt field without relying on a mailbox
// scan. The member is discovery-only; GcReceipt re-reads the blob and its
// retain_until_ms before deleting anything.
type ReceiptDueRef struct {
	Kind         ReceiptDueKind
	PlayerID     uint64
	MailID       string
	ReceiptField string
}

// ReceiptDueCandidate is one bounded receipt-expiry discovery result.
type ReceiptDueCandidate struct {
	Ref     ReceiptDueRef
	DueAtMs int64
	Member  string
}

// ReceiptGcStatus is the typed result of GcReceipt.
type ReceiptGcStatus string

const (
	ReceiptGcPurged              ReceiptGcStatus = "PURGED"
	ReceiptGcNotDue              ReceiptGcStatus = "NOT_DUE"
	ReceiptGcTargetAlreadyPurged ReceiptGcStatus = "TARGET_ALREADY_PURGED"
	ReceiptGcUnavailableNotSent  ReceiptGcStatus = "UNAVAILABLE_NOT_SENT"
	ReceiptGcTimeoutUnknown      ReceiptGcStatus = "TIMEOUT_UNKNOWN"
)

// ReceiptGcResult reports one receipt cleanup attempt.
type ReceiptGcResult struct {
	Status      ReceiptGcStatus
	FreedBytes  int64
	RetainUntil int64
}

var ErrMalformedReceiptDueRef = errors.New("playermail: malformed receipt due ref")

func (r ReceiptDueRef) validate() error {
	if _, err := mail.CanonicalPlayerID(r.PlayerID); err != nil {
		return fmt.Errorf("%w: player", ErrMalformedReceiptDueRef)
	}
	if err := mail.ValidateMailID(r.MailID); err != nil {
		return fmt.Errorf("%w: mail id", ErrMalformedReceiptDueRef)
	}
	var prefix string
	switch r.Kind {
	case ReceiptDueMutation:
		prefix = "mutation:"
	case ReceiptDueProjection:
		prefix = "projection:"
	default:
		return fmt.Errorf("%w: kind", ErrMalformedReceiptDueRef)
	}
	if !strings.HasPrefix(r.ReceiptField, prefix) || len(r.ReceiptField) != len(prefix)+64 {
		return fmt.Errorf("%w: receipt field", ErrMalformedReceiptDueRef)
	}
	if err := mail.ValidateHash(strings.TrimPrefix(r.ReceiptField, prefix), false); err != nil {
		return fmt.Errorf("%w: receipt digest", ErrMalformedReceiptDueRef)
	}
	return nil
}

// EncodeReceiptDueRefV1 renders a canonical same-slot receipt-due member.
func EncodeReceiptDueRefV1(ref ReceiptDueRef) (string, error) {
	if err := ref.validate(); err != nil {
		return "", err
	}
	player, _ := mail.CanonicalPlayerID(ref.PlayerID)
	code := "M"
	if ref.Kind == ReceiptDueProjection {
		code = "P"
	}
	// The components are all canonical and mail_id contains no colon. Keeping
	// them visible lets the Lua GC validate the target mail before ZREM; an
	// opaque encoding would make cross-mail due-index corruption unverifiable
	// inside Redis.
	return receiptDuePrefix + code + ":" + player + ":" + ref.MailID + ":" + ref.ReceiptField, nil
}

// DecodeReceiptDueRefV1 parses and canonicalizes a receipt-due member.
func DecodeReceiptDueRefV1(encoded string) (ReceiptDueRef, error) {
	if !strings.HasPrefix(encoded, receiptDuePrefix) || len(encoded) > 512 {
		return ReceiptDueRef{}, fmt.Errorf("%w: prefix or size", ErrMalformedReceiptDueRef)
	}
	body := strings.TrimPrefix(encoded, receiptDuePrefix)
	if len(body) < 5 || body[1] != ':' {
		return ReceiptDueRef{}, fmt.Errorf("%w: kind", ErrMalformedReceiptDueRef)
	}
	var kind ReceiptDueKind
	switch body[0] {
	case 'M':
		kind = ReceiptDueMutation
	case 'P':
		kind = ReceiptDueProjection
	default:
		return ReceiptDueRef{}, fmt.Errorf("%w: kind", ErrMalformedReceiptDueRef)
	}
	rest := body[2:]
	first := strings.IndexByte(rest, ':')
	if first <= 0 {
		return ReceiptDueRef{}, fmt.Errorf("%w: player separator", ErrMalformedReceiptDueRef)
	}
	secondRelative := strings.IndexByte(rest[first+1:], ':')
	if secondRelative <= 0 {
		return ReceiptDueRef{}, fmt.Errorf("%w: mail separator", ErrMalformedReceiptDueRef)
	}
	second := first + 1 + secondRelative
	player, err := parseReceiptPlayer(rest[:first])
	if err != nil {
		return ReceiptDueRef{}, err
	}
	ref := ReceiptDueRef{
		Kind:         kind,
		PlayerID:     player,
		MailID:       rest[first+1 : second],
		ReceiptField: rest[second+1:],
	}
	if err := ref.validate(); err != nil {
		return ReceiptDueRef{}, err
	}
	canonical, err := EncodeReceiptDueRefV1(ref)
	if err != nil || canonical != encoded {
		return ReceiptDueRef{}, fmt.Errorf("%w: non-canonical", ErrMalformedReceiptDueRef)
	}
	return ref, nil
}

func parseReceiptPlayer(value string) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("%w: player", ErrMalformedReceiptDueRef)
	}
	var player uint64
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("%w: player", ErrMalformedReceiptDueRef)
		}
		if player > (^uint64(0)-uint64(value[i]-'0'))/10 {
			return 0, fmt.Errorf("%w: player", ErrMalformedReceiptDueRef)
		}
		player = player*10 + uint64(value[i]-'0')
	}
	if player == 0 {
		return 0, fmt.Errorf("%w: player", ErrMalformedReceiptDueRef)
	}
	return player, nil
}
