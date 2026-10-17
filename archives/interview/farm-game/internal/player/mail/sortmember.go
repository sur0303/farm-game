package mail

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrMalformedSortMember rejects a non-canonical index member or cursor.
var ErrMalformedSortMember = errors.New("mail: malformed sort member")

// MailSortMemberCodecV1 implements 35A §3: the visible index stores one
// member per mail with a fixed score of 0, so Redis lexicographic order is
// the page order:
//
//	SortMemberV1 = uint64_big_endian(MAX_UINT64 - created_at_ms)
//	             || 0x00 || UTF8(canonical_base64url_mail_id)
//
// Newer mail sorts first under ZRANGEBYLEX ascending; equal milliseconds
// break ties by mail_id. The H5 cursor is Base64URL_NoPadding(member) and
// is always used as an exclusive bound.
const (
	sortMemberTimeBytes = 8
	sortMemberSeparator = 0x00
)

// EncodeSortMember builds the physical index member.
func EncodeSortMember(createdAtMs int64, mailID string) ([]byte, error) {
	if createdAtMs <= 0 {
		return nil, fmt.Errorf("%w: created_at_ms", ErrMalformedSortMember)
	}
	if err := ValidateMailID(mailID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedSortMember, err)
	}
	member := make([]byte, 0, sortMemberTimeBytes+1+len(mailID))
	member = binary.BigEndian.AppendUint64(member, math.MaxUint64-uint64(createdAtMs))
	member = append(member, sortMemberSeparator)
	return append(member, mailID...), nil
}

// DecodeSortMember splits a member back into its logical cursor fields.
func DecodeSortMember(member []byte) (createdAtMs int64, mailID string, err error) {
	if len(member) <= sortMemberTimeBytes+1 {
		return 0, "", fmt.Errorf("%w: length", ErrMalformedSortMember)
	}
	inverted := binary.BigEndian.Uint64(member[:sortMemberTimeBytes])
	if member[sortMemberTimeBytes] != sortMemberSeparator {
		return 0, "", fmt.Errorf("%w: separator", ErrMalformedSortMember)
	}
	mailID = string(member[sortMemberTimeBytes+1:])
	if err := ValidateMailID(mailID); err != nil {
		return 0, "", fmt.Errorf("%w: %v", ErrMalformedSortMember, err)
	}
	return int64(math.MaxUint64 - inverted), mailID, nil
}

// MailIDFromSortMember extracts only the mail id (used by eviction code).
func MailIDFromSortMember(member []byte) (string, error) {
	_, mailID, err := DecodeSortMember(member)
	return mailID, err
}

// EncodeCursor renders the opaque client cursor (35A §3).
func EncodeCursor(member []byte) (string, error) {
	if _, _, err := DecodeSortMember(member); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(member), nil
}

// DecodeCursor parses a client cursor back into a validated member. Clients
// can never construct a Redis key or an internal member that fails this
// canonical check.
func DecodeCursor(cursor string) ([]byte, error) {
	if cursor == "" {
		return nil, fmt.Errorf("%w: empty cursor", ErrMalformedSortMember)
	}
	if len(cursor) > 128 {
		return nil, fmt.Errorf("%w: cursor length", ErrMalformedSortMember)
	}
	member, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor base64", ErrMalformedSortMember)
	}
	if base64.RawURLEncoding.EncodeToString(member) != cursor {
		return nil, fmt.Errorf("%w: non-canonical cursor", ErrMalformedSortMember)
	}
	if _, _, err := DecodeSortMember(member); err != nil {
		return nil, err
	}
	return member, nil
}
