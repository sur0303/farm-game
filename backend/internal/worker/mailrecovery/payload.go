package mailrecovery

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"

	"farmgame/internal/player/mail"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/encoding/protowire"
)

const maxProjectionPayloadBytes = 4 << 10

// MailClaimProjectionPayload is the validated worker-facing shape of
// contracts.ApplyMailClaimProjectionPayload. The strict decoder below keeps
// malformed and non-deterministic bytes out of the projection repository.
type MailClaimProjectionPayload struct {
	RecipientPlayerID uint64
	MailID            string
	ClaimReceiptRef   string
	ClaimMaterialHash string
	ClaimedAtMs       int64
}

// DecodeMailClaimProjectionPayload parses the frozen protobuf payload and
// rejects unknown, duplicate, non-canonical, or semantically incomplete
// fields before any mailbox mutation is attempted.
func DecodeMailClaimProjectionPayload(payloadBytes []byte) (MailClaimProjectionPayload, error) {
	return decodePayloadBytes(payloadBytes)
}

// MailClaimProjectionPayloadHash matches the physical SourceOrder contract:
// SHA-256 of the deterministic protobuf bytes. The ingress uses the same
// encoding before AuthorityCommit attaches the order.
func MailClaimProjectionPayloadHash(payload MailClaimProjectionPayload) string {
	digest := sha256.Sum256(marshalMailClaimProjectionPayload(payload))
	return fmt.Sprintf("%x", digest[:])
}

func marshalMailClaimProjectionPayload(payload MailClaimProjectionPayload) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, payload.RecipientPlayerID)
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendString(out, payload.MailID)
	out = protowire.AppendTag(out, 3, protowire.BytesType)
	out = protowire.AppendString(out, payload.ClaimReceiptRef)
	out = protowire.AppendTag(out, 4, protowire.BytesType)
	out = protowire.AppendString(out, payload.ClaimMaterialHash)
	out = protowire.AppendTag(out, 5, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(payload.ClaimedAtMs))
	return out
}

// decodePayloadBytes keeps the original bytes while using the strict parser.
// It is separate from DecodeMailClaimProjectionPayload so callers and tests
// can use the public helper without exposing parser bookkeeping.
func decodePayloadBytes(payloadBytes []byte) (MailClaimProjectionPayload, error) {
	original := append([]byte(nil), payloadBytes...)
	payload, err := decodeProjectionFields(payloadBytes)
	if err != nil {
		return MailClaimProjectionPayload{}, err
	}
	if !bytes.Equal(marshalMailClaimProjectionPayload(payload), original) {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: non-deterministic protobuf", ErrPayloadInvalid)
	}
	return payload, nil
}

func decodeProjectionFields(payloadBytes []byte) (MailClaimProjectionPayload, error) {
	if len(payloadBytes) == 0 || len(payloadBytes) > maxProjectionPayloadBytes {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: payload size", ErrPayloadInvalid)
	}
	var payload MailClaimProjectionPayload
	seen := [6]bool{}
	for len(payloadBytes) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(payloadBytes)
		if tagBytes < 0 {
			return MailClaimProjectionPayload{}, fmt.Errorf("%w: protobuf tag", ErrPayloadInvalid)
		}
		payloadBytes = payloadBytes[tagBytes:]
		if number < 1 || number > 5 || seen[number] {
			return MailClaimProjectionPayload{}, fmt.Errorf("%w: protobuf field %d", ErrPayloadInvalid, number)
		}
		seen[number] = true
		switch number {
		case 1:
			if wireType != protowire.VarintType {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: recipient wire type", ErrPayloadInvalid)
			}
			value, consumed := protowire.ConsumeVarint(payloadBytes)
			if consumed < 0 {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: recipient varint", ErrPayloadInvalid)
			}
			payload.RecipientPlayerID = value
			payloadBytes = payloadBytes[consumed:]
		case 2, 3, 4:
			if wireType != protowire.BytesType {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: field %d wire type", ErrPayloadInvalid, number)
			}
			value, consumed := protowire.ConsumeBytes(payloadBytes)
			if consumed < 0 || !utf8.Valid(value) {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: field %d bytes", ErrPayloadInvalid, number)
			}
			switch number {
			case 2:
				payload.MailID = string(value)
			case 3:
				payload.ClaimReceiptRef = string(value)
			case 4:
				payload.ClaimMaterialHash = string(value)
			}
			payloadBytes = payloadBytes[consumed:]
		case 5:
			if wireType != protowire.VarintType {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: claimed_at wire type", ErrPayloadInvalid)
			}
			value, consumed := protowire.ConsumeVarint(payloadBytes)
			if consumed < 0 {
				return MailClaimProjectionPayload{}, fmt.Errorf("%w: claimed_at varint", ErrPayloadInvalid)
			}
			payload.ClaimedAtMs = int64(value)
			payloadBytes = payloadBytes[consumed:]
		}
	}
	if payload.RecipientPlayerID == 0 || workidentity.ValidateStableOpaqueID(payload.MailID) != nil ||
		strings.TrimSpace(payload.ClaimReceiptRef) != payload.ClaimReceiptRef || payload.ClaimReceiptRef == "" || len(payload.ClaimReceiptRef) > 256 ||
		mail.ValidateHash(payload.ClaimMaterialHash, false) != nil || payload.ClaimedAtMs <= 0 {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: payload identity", ErrPayloadInvalid)
	}
	return payload, nil
}
