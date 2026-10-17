package scaleplan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"farmgame/internal/storage/redis/rediskey"
)

const LayoutVersion = "v1"

var ErrInvalidKey = fmt.Errorf("redisscaleplan: invalid key")

// KeyBuilder is the sole source of the ScalePlan keyspace.  Realm bytes are
// retained in records, while only their digest is put in the hash tag.  This
// both prevents ACL pattern injection and keeps every operation in one slot.
type KeyBuilder struct {
	namespace rediskey.Namespace
	realm string
	realmDigest string
	tag string
}

func NewKeyBuilder(namespace rediskey.Namespace, realm string) (KeyBuilder, error) {
	if err := namespace.Validate(); err != nil || !validRealm(realm) { return KeyBuilder{}, ErrInvalidKey }
	digest := sha256.Sum256([]byte(realm))
	rd := hex.EncodeToString(digest[:])
	tag := fmt.Sprintf("classic-farm:%s:%s:scale-plan:%s:%s", namespace.Environment, namespace.Zone, LayoutVersion, rd)
	return KeyBuilder{namespace: namespace, realm: realm, realmDigest: rd, tag: tag}, nil
}

func NewKeyBuilderV1(namespace rediskey.Namespace, realm string) (KeyBuilder, error) { return NewKeyBuilder(namespace, realm) }
func (b KeyBuilder) Namespace() rediskey.Namespace { return b.namespace }
func (b KeyBuilder) Realm() string { return b.realm }
func (b KeyBuilder) RealmDigest() string { return b.realmDigest }
func (b KeyBuilder) HashTag() string { return b.tag }

func (b KeyBuilder) key(suffix string) rediskey.RedisKey { return rediskey.RedisKey("{" + b.tag + "}:" + suffix) }
func (b KeyBuilder) Meta() rediskey.RedisKey { return b.key("meta") }
func (b KeyBuilder) Lease() rediskey.RedisKey { return b.key("lease") }
func (b KeyBuilder) LeaseFence() rediskey.RedisKey { return b.key("lease-fence") }
func (b KeyBuilder) CorruptLatch() rediskey.RedisKey { return b.key("corrupt-latch") }
func (b KeyBuilder) Recoverable() rediskey.RedisKey { return b.key("recoverable") }
func (b KeyBuilder) Terminal() rediskey.RedisKey { return b.key("terminal") }
func (b KeyBuilder) Accounting() rediskey.RedisKey { return b.key("accounting") }

func (b KeyBuilder) Plan(id string) (rediskey.RedisKey, error) {
	member, err := b.PlanMember(id); if err != nil { return "", err }
	return b.key("plan:" + member), nil
}
func (b KeyBuilder) PlanKey(id string) (rediskey.RedisKey, error) { return b.Plan(id) }

func (b KeyBuilder) PlanMember(id string) (string, error) {
	if err := validateIdentity(id); err != nil { return "", err }
	return hex.EncodeToString([]byte(id)), nil
}
func (b KeyBuilder) EncodedPlanID(id string) (string, error) { return b.PlanMember(id) }
func (b KeyBuilder) DecodePlanMember(member string) (string, error) {
	if member == "" || len(member)%2 != 0 { return "", ErrInvalidKey }
	raw, err := hex.DecodeString(member); if err != nil { return "", ErrInvalidKey }
	id := string(raw); if err := validateIdentity(id); err != nil { return "", err }; return id, nil
}

func (b KeyBuilder) Idempotency(key string) (rediskey.RedisKey, error) {
	d, err := digestSuffix(key); if err != nil { return "", err }; return b.key("idempotency:" + d), nil
}
func (b KeyBuilder) IdempotencyDigest(key string) (string, error) { return digestSuffix(key) }
func (b KeyBuilder) IdempotencyKey(key string) (rediskey.RedisKey, error) { return b.Idempotency(key) }
func (b KeyBuilder) Active(workloadKey string) (rediskey.RedisKey, error) {
	d, err := digestSuffix(workloadKey); if err != nil { return "", err }; return b.key("active:" + d), nil
}
func (b KeyBuilder) ActiveWorkload(workloadKey string) (rediskey.RedisKey, error) { return b.Active(workloadKey) }
func (b KeyBuilder) ActiveDigest(workloadKey string) (string, error) { return digestSuffix(workloadKey) }

func (b KeyBuilder) AssertSameSlot(keys ...rediskey.RedisKey) error { return rediskey.AssertSameSlot(keys...) }

func validRealm(value string) bool {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) { return false }
	for i := 0; i < len(value); i++ { if value[i] < 0x21 || value[i] > 0x7e { return false } }
	return true
}
func validateIdentity(value string) error {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) { return ErrInvalidKey }
	for i := 0; i < len(value); i++ { if value[i] < 0x21 || value[i] > 0x7e { return ErrInvalidKey } }
	return nil
}
func digestSuffix(value string) (string, error) {
	if value == "" || len(value) > 1024 || !utf8.ValidString(value) { return "", ErrInvalidKey }
	d := sha256.Sum256([]byte(value)); return hex.EncodeToString(d[:]), nil
}
