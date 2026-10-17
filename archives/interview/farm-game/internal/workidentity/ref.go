package workidentity

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	workRefPrefix     = "wr1_"
	scheduleRefPrefix = "sr1_"
	receiptRefPrefix  = "rr1_"
)

var ErrMalformedReference = errors.New("workidentity: malformed reference")

type WorkRef struct {
	WorkType string
	WorkID   string
}

// ScheduleRef is a distinct DueIndex member identity. Keeping schedules out
// of wr1_ members prevents a schedule and an immutable AttachedOrder with the
// same type/id from aliasing one sorted-set entry.
type ScheduleRef struct {
	ScheduleType string
	ScheduleID   string
}

func EncodeScheduleRefV1(scheduleType, scheduleID string) (string, error) {
	if !validStableName(scheduleType) || !validASCII(scheduleID, 1, 128) || ValidateStableOpaqueID(scheduleID) != nil {
		return "", fmt.Errorf("%w: schedule ref fields", ErrMalformedReference)
	}
	raw, _ := appendLP(nil, scheduleType)
	raw, _ = appendLP(raw, scheduleID)
	return scheduleRefPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeScheduleRefV1(encoded string) (ScheduleRef, error) {
	fields, err := decodeLPReference(encoded, scheduleRefPrefix, 2)
	if err != nil {
		return ScheduleRef{}, err
	}
	canonical, err := EncodeScheduleRefV1(fields[0], fields[1])
	if err != nil || canonical != encoded {
		return ScheduleRef{}, fmt.Errorf("%w: non-canonical schedule ref", ErrMalformedReference)
	}
	return ScheduleRef{ScheduleType: fields[0], ScheduleID: fields[1]}, nil
}

type ReceiptRef struct {
	ActorID         uint64
	ReceiptKind     string
	OperationDigest string
}

func EncodeWorkRefV1(workType, workID string) (string, error) {
	if !validStableName(workType) || !validASCII(workID, 1, 128) || ValidateStableOpaqueID(workID) != nil {
		return "", fmt.Errorf("%w: work ref fields", ErrMalformedReference)
	}
	raw, _ := appendLP(nil, workType)
	raw, _ = appendLP(raw, workID)
	return workRefPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeWorkRefV1(encoded string) (WorkRef, error) {
	fields, err := decodeLPReference(encoded, workRefPrefix, 2)
	if err != nil {
		return WorkRef{}, err
	}
	canonical, err := EncodeWorkRefV1(fields[0], fields[1])
	if err != nil || canonical != encoded {
		return WorkRef{}, fmt.Errorf("%w: non-canonical work ref", ErrMalformedReference)
	}
	return WorkRef{WorkType: fields[0], WorkID: fields[1]}, nil
}

func EncodeReceiptRefV1(actorID uint64, receiptKind, operationDigest string) (string, error) {
	actor := strconv.FormatUint(actorID, 10)
	if actorID == 0 || !validStableName(receiptKind) || !validLowerHexDigest(operationDigest) {
		return "", fmt.Errorf("%w: receipt ref fields", ErrMalformedReference)
	}
	raw, _ := appendLP(nil, actor)
	raw, _ = appendLP(raw, receiptKind)
	raw, _ = appendLP(raw, operationDigest)
	return receiptRefPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeReceiptRefV1(encoded string) (ReceiptRef, error) {
	fields, err := decodeLPReference(encoded, receiptRefPrefix, 3)
	if err != nil || !canonicalUnsignedDecimal(fields[0], 20) {
		return ReceiptRef{}, fmt.Errorf("%w: receipt actor id", ErrMalformedReference)
	}
	actorID, parseErr := strconv.ParseUint(fields[0], 10, 64)
	if parseErr != nil || actorID == 0 {
		return ReceiptRef{}, fmt.Errorf("%w: receipt actor id", ErrMalformedReference)
	}
	canonical, encodeErr := EncodeReceiptRefV1(actorID, fields[1], fields[2])
	if encodeErr != nil || canonical != encoded {
		return ReceiptRef{}, fmt.Errorf("%w: non-canonical receipt ref", ErrMalformedReference)
	}
	return ReceiptRef{ActorID: actorID, ReceiptKind: fields[1], OperationDigest: fields[2]}, nil
}

func decodeLPReference(encoded, prefix string, wantFields int) ([]string, error) {
	if !strings.HasPrefix(encoded, prefix) || len(encoded) > 1024 {
		return nil, fmt.Errorf("%w: prefix or size", ErrMalformedReference)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, prefix))
	if err != nil || prefix+base64.RawURLEncoding.EncodeToString(payload) != encoded {
		return nil, fmt.Errorf("%w: base64url", ErrMalformedReference)
	}
	fields := make([]string, 0, wantFields)
	for len(payload) > 0 {
		if len(payload) < 4 || len(fields) >= wantFields {
			return nil, fmt.Errorf("%w: LP field count", ErrMalformedReference)
		}
		size := uint64(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if size > uint64(len(payload)) {
			return nil, fmt.Errorf("%w: LP field length", ErrMalformedReference)
		}
		field := string(payload[:size])
		if !utf8.ValidString(field) {
			return nil, fmt.Errorf("%w: field UTF-8", ErrMalformedReference)
		}
		fields = append(fields, field)
		payload = payload[size:]
	}
	if len(fields) != wantFields {
		return nil, fmt.Errorf("%w: LP field count", ErrMalformedReference)
	}
	return fields, nil
}

func validASCII(value string, min, max int) bool {
	if len(value) < min || len(value) > max {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func validLowerHexDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func canonicalUnsignedDecimal(value string, max int) bool {
	return len(value) > 0 && len(value) <= max && value[0] != '0' && strings.IndexFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	}) == -1
}
