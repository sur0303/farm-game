package mail

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

// ErrMalformedBlob rejects a truncated, oversized or non-canonical LP blob.
var ErrMalformedBlob = errors.New("mail: malformed blob")

// appendLP appends LP(x) = uint32_big_endian(len(UTF8(x))) || UTF8(x), the
// frozen length-prefix of contract 31 §8.1. This local copy deliberately
// mirrors internal/workidentity's private helper so the mail package does
// not depend on unexported bytes of another contract.
func appendLP(dst []byte, value string) ([]byte, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: LP field too large", ErrMalformedBlob)
	}
	if !utf8.ValidString(value) {
		return nil, fmt.Errorf("%w: LP field is not UTF-8", ErrMalformedBlob)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func appendLPRaw(dst []byte, value []byte) ([]byte, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: LP field too large", ErrMalformedBlob)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

// parseLP decodes a blob of exactly `want` LP fields. It rejects trailing
// bytes, truncated fields and non-UTF-8 payloads.
func parseLP(blob []byte, want int) ([]string, error) {
	fields := make([]string, 0, want)
	for len(blob) > 0 {
		if len(blob) < 4 || len(fields) >= want {
			return nil, fmt.Errorf("%w: LP field count", ErrMalformedBlob)
		}
		size := uint64(binary.BigEndian.Uint32(blob[:4]))
		blob = blob[4:]
		if size > uint64(len(blob)) {
			return nil, fmt.Errorf("%w: LP field length", ErrMalformedBlob)
		}
		field := string(blob[:size])
		if !utf8.ValidString(field) {
			return nil, fmt.Errorf("%w: field UTF-8", ErrMalformedBlob)
		}
		fields = append(fields, field)
		blob = blob[size:]
	}
	if len(fields) != want {
		return nil, fmt.Errorf("%w: LP field count", ErrMalformedBlob)
	}
	return fields, nil
}

func itoa(v uint64) string {
	return fmt.Sprintf("%d", v)
}

func itoaSigned(v int64) string {
	return fmt.Sprintf("%d", v)
}

func atoi(v string) (int64, error) {
	var n int64
	if v == "" {
		return 0, fmt.Errorf("%w: empty integer", ErrMalformedBlob)
	}
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return 0, fmt.Errorf("%w: integer", ErrMalformedBlob)
		}
		n = n*10 + int64(v[i]-'0')
	}
	return n, nil
}
