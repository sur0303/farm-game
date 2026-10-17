// Package commitstamp owns the sealed storage-adapter capability for issuing
// first-COMMITTED authority stamps. Go's internal-package rule prevents
// ordinary actor and source-order packages from importing this capability.
package commitstamp

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"reflect"
	"sort"
)

var errUnsupportedRequest = errors.New("commit stamp request contains unsupported state")

// Stamp is a sealed witness. Code outside this package cannot implement it
// because seal is package-private; only IssueFirstCommitted can produce a
// non-nil implementation.
type Stamp interface {
	seal()
}

// issuedStamp deliberately contains no request object, slice, array, map, or
// interface-backed state. A string is immutable in Go, so safe reflection by a
// consumer cannot retarget a genuine stamp after issuance.
type issuedStamp struct {
	requestFingerprint string
	recordVersion      uint64
	committedAtMs      int64
}

func (*issuedStamp) seal() {}

// CapturedRequest is an immutable adapter-local fingerprint of the exact
// request submitted to a store. Its fields are deliberately private so capture
// cannot itself be converted into a Stamp.
type CapturedRequest struct {
	requestFingerprint string
	valid              bool
}

// CaptureExactRequest fingerprints a validated, bounded adapter request before
// an asynchronous store invocation can observe later caller-owned mutation.
func CaptureExactRequest(request any) (CapturedRequest, error) {
	fingerprint, err := fingerprintExactRequest(request)
	if err != nil {
		return CapturedRequest{}, err
	}
	return CapturedRequest{requestFingerprint: fingerprint, valid: true}, nil
}

// IssueFirstCommitted is the sealed issuance capability. Static architecture
// tests restrict its call sites to the MemoryStore commit linearization point
// and Redis's validated first-COMMITTED decoder.
func IssueFirstCommitted(request CapturedRequest, recordVersion uint64, committedAtMs int64) Stamp {
	if !request.valid || request.requestFingerprint == "" || recordVersion == 0 || committedAtMs <= 0 {
		return nil
	}
	return &issuedStamp{
		requestFingerprint: request.requestFingerprint,
		recordVersion:      recordVersion,
		committedAtMs:      committedAtMs,
	}
}

// VerifyExact opens a stamp only when a freshly computed canonical fingerprint
// binds it to the exact request. The concrete stamp and its fingerprint never
// cross this package seam.
func VerifyExact(stamp Stamp, request any) (recordVersion uint64, committedAtMs int64, ok bool) {
	issued, ok := stamp.(*issuedStamp)
	if !ok || issued == nil || issued.requestFingerprint == "" ||
		issued.recordVersion == 0 || issued.committedAtMs <= 0 {
		return 0, 0, false
	}
	fingerprint, err := fingerprintExactRequest(request)
	if err != nil || fingerprint != issued.requestFingerprint {
		return 0, 0, false
	}
	return issued.recordVersion, issued.committedAtMs, true
}

const (
	canonicalFingerprintDomain = "farmgame:authority-commit-request:fingerprint:v1"
	maxCanonicalDepth          = 64
	// This hard safety net is above every default adapter cardinality; each
	// issuance call is already constrained further by its validated Limits.
	maxCanonicalCollectionLen = 1 << 20
)

// fingerprintExactRequest hashes a self-delimiting canonical reflection
// encoding. AuthorityCommitRequest and all of its current nested values consist
// solely of these supported kinds. Struct declaration order and slice order are
// exact; map keys are sorted; and every nil-capable kind emits a separate nil
// marker before its length/content. Walking all exported struct fields also
// makes adding a request field fail exact binding unless it is fingerprinted.
func fingerprintExactRequest(request any) (string, error) {
	if request == nil {
		return "", errUnsupportedRequest
	}
	hasher := sha256.New()
	encoder := canonicalEncoder{hash: hasher}
	encoder.writeString(canonicalFingerprintDomain)
	if err := encoder.writeValue(reflect.ValueOf(request), 0); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

type canonicalEncoder struct {
	hash hash.Hash
}

func (e canonicalEncoder) writeByte(value byte) {
	_, _ = e.hash.Write([]byte{value})
}

func (e canonicalEncoder) writeUint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = e.hash.Write(encoded[:])
}

func (e canonicalEncoder) writeString(value string) {
	e.writeUint64(uint64(len(value)))
	_, _ = e.hash.Write([]byte(value))
}

func (e canonicalEncoder) writeType(valueType reflect.Type) {
	e.writeByte(byte(valueType.Kind()))
	e.writeString(valueType.PkgPath())
	e.writeString(valueType.Name())
}

func (e canonicalEncoder) writeValue(value reflect.Value, depth int) error {
	if depth > maxCanonicalDepth || !value.IsValid() {
		return errUnsupportedRequest
	}
	e.writeType(value.Type())
	switch value.Kind() {
	case reflect.Bool:
		if value.Bool() {
			e.writeByte(1)
		} else {
			e.writeByte(0)
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.writeUint64(uint64(value.Int()))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		e.writeUint64(value.Uint())
		return nil
	case reflect.String:
		e.writeString(value.String())
		return nil
	case reflect.Array:
		if value.Len() > maxCanonicalCollectionLen {
			return errUnsupportedRequest
		}
		e.writeUint64(uint64(value.Len()))
		for index := 0; index < value.Len(); index++ {
			if err := e.writeValue(value.Index(index), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Interface:
		if value.IsNil() {
			e.writeByte(0)
			return nil
		}
		e.writeByte(1)
		return e.writeValue(value.Elem(), depth+1)
	case reflect.Map:
		if value.IsNil() {
			e.writeByte(0)
			return nil
		}
		if value.Type().Key().Kind() != reflect.String || value.Len() > maxCanonicalCollectionLen {
			return errUnsupportedRequest
		}
		e.writeByte(1)
		e.writeUint64(uint64(value.Len()))
		keys := make([]string, 0, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			keys = append(keys, iterator.Key().String())
		}
		sort.Strings(keys)
		for _, key := range keys {
			mapKey := reflect.ValueOf(key).Convert(value.Type().Key())
			if err := e.writeValue(mapKey, depth+1); err != nil {
				return err
			}
			if err := e.writeValue(value.MapIndex(mapKey), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Pointer:
		if value.IsNil() {
			e.writeByte(0)
			return nil
		}
		e.writeByte(1)
		return e.writeValue(value.Elem(), depth+1)
	case reflect.Slice:
		if value.IsNil() {
			e.writeByte(0)
			return nil
		}
		e.writeByte(1)
		e.writeUint64(uint64(value.Len()))
		if value.Type().Elem().Kind() == reflect.Uint8 {
			// Payload byte lengths are bounded by each adapter's validated
			// MaxCommitBytes. Hash them directly so the fingerprint does not
			// impose a smaller public payload limit or retain backing storage.
			_, _ = e.hash.Write(value.Bytes())
			return nil
		}
		if value.Len() > maxCanonicalCollectionLen {
			return errUnsupportedRequest
		}
		for index := 0; index < value.Len(); index++ {
			if err := e.writeValue(value.Index(index), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		if value.NumField() > maxCanonicalCollectionLen {
			return errUnsupportedRequest
		}
		e.writeUint64(uint64(value.NumField()))
		for index := 0; index < value.NumField(); index++ {
			fieldType := value.Type().Field(index)
			if fieldType.PkgPath != "" {
				return errUnsupportedRequest
			}
			e.writeString(fieldType.Name)
			if err := e.writeValue(value.Field(index), depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return errUnsupportedRequest
	}
}
