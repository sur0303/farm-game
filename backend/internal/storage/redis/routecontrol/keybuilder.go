package routecontrol

import (
	"errors"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"

	"farmgame/internal/storage/redis/rediskey"
)

const (
	LayoutVersion                 = "v1"
	keyRoot                       = "classic-farm"
	maxTransitionIDBytes          = 256
	maxTransitionFingerprintBytes = 256
)

var (
	ErrInvalidOptions   = errors.New("redisroutecontrol: invalid options")
	ErrInvalidKey       = errors.New("redisroutecontrol: invalid key component")
	ErrScriptContract   = errors.New("redisroutecontrol: script contract violation")
	ErrMalformedRecord  = errors.New("redisroutecontrol: malformed durable record")
	ErrCapacityExceeded = errors.New("redisroutecontrol: durable capacity exceeded")
	ErrFingerprint      = errors.New("redisroutecontrol: invalid transition fingerprint")
)

// KeyBuilderV1 is the only constructor for route-control Redis keys. All
// records intentionally share one byte-identical hash tag because a single
// Lua operation may touch the snapshot and bounded transition indexes.
type KeyBuilderV1 struct {
	namespace rediskey.Namespace
}

type KeyBuilder = KeyBuilderV1

func NewKeyBuilderV1(namespace rediskey.Namespace) (KeyBuilderV1, error) {
	if err := namespace.Validate(); err != nil {
		return KeyBuilderV1{}, ErrInvalidOptions
	}
	return KeyBuilderV1{namespace: namespace}, nil
}

func NewKeyBuilder(namespace rediskey.Namespace) (KeyBuilderV1, error) {
	return NewKeyBuilderV1(namespace)
}

func (b KeyBuilderV1) Namespace() rediskey.Namespace { return b.namespace }

func (b KeyBuilderV1) HashTag() string {
	return keyRoot + ":" + b.namespace.Environment + ":" + b.namespace.Zone + ":route-control:" + LayoutVersion
}

func (b KeyBuilderV1) key(suffix string) rediskey.RedisKey {
	return rediskey.RedisKey("{" + b.HashTag() + "}:" + suffix)
}

func (b KeyBuilderV1) Snapshot() rediskey.RedisKey {
	return b.key("snapshot")
}

func (b KeyBuilderV1) Meta() rediskey.RedisKey {
	return b.key("meta")
}

func (b KeyBuilderV1) Recoveries() rediskey.RedisKey {
	return b.key("recoveries")
}

func (b KeyBuilderV1) RecoveryIndex() rediskey.RedisKey {
	return b.key("recovery-index")
}

func (b KeyBuilderV1) Tombstones() rediskey.RedisKey {
	return b.key("tombstones")
}

func (b KeyBuilderV1) TombstoneIndex() rediskey.RedisKey {
	return b.key("tombstone-index")
}

func (b KeyBuilderV1) AssertSameSlot(keys ...rediskey.RedisKey) error {
	return rediskey.AssertSameSlot(keys...)
}

func (b KeyBuilderV1) ValidateTransitionID(id string) error {
	if !validComponent(id, maxTransitionIDBytes) {
		return fmt.Errorf("%w: transition id", ErrInvalidKey)
	}
	return nil
}

func validateTransitionFingerprint(fingerprint string) error {
	if !validComponent(fingerprint, maxTransitionFingerprintBytes) {
		return ErrFingerprint
	}
	return nil
}

func validComponent(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func decimalUint64(value uint64) string {
	return strconv.FormatUint(value, 10)
}
