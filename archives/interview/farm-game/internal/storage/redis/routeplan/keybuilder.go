package routeplan

import (
	"encoding/hex"
	"fmt"

	model "farmgame/internal/routeplan"
	"farmgame/internal/storage/redis/rediskey"
)

const LayoutVersion = "v1"

var ErrInvalidKey = fmt.Errorf("redisrouteplan: invalid key")

// KeyBuilder is the only source of RoutePlan Redis keys. Every key belongs to
// one fixed hash tag so one CAS Lua invocation is single-slot and atomic.
type KeyBuilder struct{ namespace rediskey.Namespace }

func NewKeyBuilder(namespace rediskey.Namespace) (KeyBuilder, error) {
	if err := namespace.Validate(); err != nil {
		return KeyBuilder{}, ErrInvalidKey
	}
	return KeyBuilder{namespace: namespace}, nil
}

func NewKeyBuilderV1(namespace rediskey.Namespace) (KeyBuilder, error) {
	return NewKeyBuilder(namespace)
}

func (b KeyBuilder) Namespace() rediskey.Namespace { return b.namespace }

func (b KeyBuilder) HashTag() string {
	return "classic-farm:" + b.namespace.Environment + ":" + b.namespace.Zone + ":route-plan:" + LayoutVersion
}

func (b KeyBuilder) key(suffix string) rediskey.RedisKey {
	return rediskey.RedisKey("{" + b.HashTag() + "}:" + suffix)
}

func (b KeyBuilder) Meta() rediskey.RedisKey       { return b.key("meta") }
func (b KeyBuilder) Index() rediskey.RedisKey      { return b.key("index") }
func (b KeyBuilder) Restart() rediskey.RedisKey    { return b.key("restart") }
func (b KeyBuilder) Accounting() rediskey.RedisKey { return b.key("accounting") }

// Plan encodes the ID into the record-key suffix. The raw ID is stored inside
// the canonical JSON and is never concatenated into a Redis key.
func (b KeyBuilder) Plan(id string) (rediskey.RedisKey, error) {
	if err := validatePlanKeyID(id); err != nil {
		return "", err
	}
	return b.key("plan:" + hex.EncodeToString([]byte(id))), nil
}

func (b KeyBuilder) PlanKey(id string) (rediskey.RedisKey, error) { return b.Plan(id) }

func (b KeyBuilder) EncodedPlanID(id string) (string, error) {
	if err := validatePlanKeyID(id); err != nil {
		return "", err
	}
	return hex.EncodeToString([]byte(id)), nil
}

func (b KeyBuilder) DecodePlanMember(member string) (string, error) {
	if member == "" || len(member)%2 != 0 {
		return "", ErrInvalidKey
	}
	raw, err := hex.DecodeString(member)
	if err != nil {
		return "", ErrInvalidKey
	}
	id := string(raw)
	if err := validatePlanKeyID(id); err != nil {
		return "", err
	}
	return id, nil
}

func (b KeyBuilder) AssertSameSlot(keys ...rediskey.RedisKey) error {
	return rediskey.AssertSameSlot(keys...)
}

func validatePlanKeyID(value string) error {
	if err := model.ValidatePlanID(value); err != nil {
		return ErrInvalidKey
	}
	return nil
}
