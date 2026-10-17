package playermail

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"farmgame/internal/player/mail"
	"farmgame/internal/storage/redis/rediskey"
)

// Physical layout constants (35A §2). Once real data exists, the hash
// algorithm, seed, bucket count and layout version are immutable; changes
// require a new layout and explicit migration.
const (
	// BucketSeedLabel is the frozen LP label of the bucket hash input.
	BucketSeedLabel = "player-mail-persistence-bucket-v1"
	// LayoutVersion is the frozen player-mail-v1 layout.
	LayoutVersion = "v1"
	// DefaultBucketCount is the first-round candidate (35A §2.1).
	DefaultBucketCount = uint32(4096)
)

var (
	ErrInvalidProfile = errors.New("playermail: invalid physical profile")
	ErrInvalidBucket  = errors.New("playermail: invalid bucket reference")
)

// Profile carries the tunable placement parameters of the mail layout.
type Profile struct {
	Namespace   rediskey.Namespace
	BucketCount uint32
}

// Validate enforces a usable, versioned profile.
func (p Profile) Validate() error {
	if p.Namespace.Validate() != nil || p.BucketCount == 0 {
		return ErrInvalidProfile
	}
	return nil
}

// BucketFor computes the stable MailPersistenceBucket (35A §2.1):
//
//	hash_input = LP("player-mail-persistence-bucket-v1") || LP(canonical_player_id)
//	bucket_id  = XXH64(seed=0, hash_input) % bucket_count
//
// Clients, Gate and business callers never supply a bucket id; it is always
// derived here from the trusted recipient.
func (p Profile) BucketFor(recipientPlayerID uint64) (uint32, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	player, err := mail.CanonicalPlayerID(recipientPlayerID)
	if err != nil {
		return 0, err
	}
	input := make([]byte, 0, 4+len(BucketSeedLabel)+4+len(player))
	input = binary.BigEndian.AppendUint32(input, uint32(len(BucketSeedLabel)))
	input = append(input, BucketSeedLabel...)
	input = binary.BigEndian.AppendUint32(input, uint32(len(player)))
	input = append(input, player...)
	return uint32(xxhash.Sum64(input) % uint64(p.BucketCount)), nil
}

// KeyBuilderV1 is the only constructor of player-mail Redis keys (35A
// §2.2-2.3). All keys of one Lua call share the identical hash tag; the
// builder asserts it byte-for-byte via rediskey.AssertSameSlot.
type KeyBuilderV1 struct {
	profile Profile
}

// NewKeyBuilderV1 validates the profile and returns the builder.
func NewKeyBuilderV1(profile Profile) (KeyBuilderV1, error) {
	if err := profile.Validate(); err != nil {
		return KeyBuilderV1{}, err
	}
	return KeyBuilderV1{profile: profile}, nil
}

// Profile returns the validated physical profile.
func (b KeyBuilderV1) Profile() Profile { return b.profile }

// redisKeyRoot mirrors the frozen root segment of rediskey (35A §2.2 keeps
// the same root so all classic-farm keys share one namespace convention).
const redisKeyRoot = "classic-farm"

func (p Profile) namespacePrefix() string {
	return redisKeyRoot + ":" + p.Namespace.Environment + ":" + p.Namespace.Zone
}

// HashTag renders {classic-farm:<env>:<zone>:player-mail:v1:b:<bucket>}.
func (b KeyBuilderV1) HashTag(bucketID uint32) (string, error) {
	if err := b.profile.Validate(); err != nil || bucketID >= b.profile.BucketCount {
		return "", ErrInvalidBucket
	}
	return b.profile.namespacePrefix() + ":player-mail:" + LayoutVersion + ":b:" + strconv.FormatUint(uint64(bucketID), 10), nil
}

func (b KeyBuilderV1) key(bucketID uint32, suffix string) (rediskey.RedisKey, error) {
	tag, err := b.HashTag(bucketID)
	if err != nil {
		return "", err
	}
	return rediskey.RedisKey("{" + tag + "}:" + suffix), nil
}

// BucketOf derives the bucket for a recipient.
func (b KeyBuilderV1) BucketOf(recipientPlayerID uint64) (uint32, error) {
	return b.profile.BucketFor(recipientPlayerID)
}

// Mailbox is the per-player HASH holding Entry/State/Dedup/Receipt/Summary
// fields (35A §2.3-2.4).
func (b KeyBuilderV1) Mailbox(bucketID uint32, recipientPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.assertOwner(bucketID, recipientPlayerID); err != nil {
		return "", err
	}
	return b.key(bucketID, "box:"+strconv.FormatUint(recipientPlayerID, 10))
}

// VisibleIndex is the per-player ZSET of SortMemberV1 (score 0).
func (b KeyBuilderV1) VisibleIndex(bucketID uint32, recipientPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.assertOwner(bucketID, recipientPlayerID); err != nil {
		return "", err
	}
	return b.key(bucketID, "index:"+strconv.FormatUint(recipientPlayerID, 10))
}

// SafeEvictIndex holds only read-and-safe mails (35A §2.3).
func (b KeyBuilderV1) SafeEvictIndex(bucketID uint32, recipientPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.assertOwner(bucketID, recipientPlayerID); err != nil {
		return "", err
	}
	return b.key(bucketID, "safe-evict:"+strconv.FormatUint(recipientPlayerID, 10))
}

// ExpireDueIndex is the bucket-shared visibility-expiry discovery ZSET.
func (b KeyBuilderV1) ExpireDueIndex(bucketID uint32) (rediskey.RedisKey, error) {
	return b.key(bucketID, "expire-due")
}

// GcDueIndex is the bucket-shared physical-GC discovery ZSET.
func (b KeyBuilderV1) GcDueIndex(bucketID uint32) (rediskey.RedisKey, error) {
	return b.key(bucketID, "gc-due")
}

// ReceiptDueIndex is the bucket-shared receipt-expiry discovery ZSET. Its
// members carry the exact mailbox receipt field, so cleanup never needs an
// HSCAN or a digest guess.
func (b KeyBuilderV1) ReceiptDueIndex(bucketID uint32) (rediskey.RedisKey, error) {
	return b.key(bucketID, "receipt-due")
}

// BucketMeta records layout, watermarks and scan cursors (35A §2.3).
func (b KeyBuilderV1) BucketMeta(bucketID uint32) (rediskey.RedisKey, error) {
	return b.key(bucketID, "bucket-meta")
}

// ScanLease is the worker bucket scan short lease (35A §7.2).
func (b KeyBuilderV1) ScanLease(bucketID uint32) (rediskey.RedisKey, error) {
	return b.key(bucketID, "scan-lease")
}

// AssertSameSlot verifies every key carries the identical hash tag bytes.
func (b KeyBuilderV1) AssertSameSlot(keys ...rediskey.RedisKey) error {
	return rediskey.AssertSameSlot(keys...)
}

func (b KeyBuilderV1) assertOwner(bucketID uint32, recipientPlayerID uint64) error {
	want, err := b.profile.BucketFor(recipientPlayerID)
	if err != nil {
		return err
	}
	if want != bucketID {
		return fmt.Errorf("%w: recipient %d belongs to bucket %d not %d", ErrInvalidBucket, recipientPlayerID, want, bucketID)
	}
	return nil
}

// FieldBuilderV1 is the only constructor of mailbox HASH field names (35A
// §2.4). Identities are validated digests/ids; raw user input never reaches
// a field name.
type FieldBuilderV1 struct{}

// Meta fields of the mailbox HASH.
const (
	MetaSchemaVersion   = "_meta:schema_version"
	MetaMailboxRevision = "_meta:mailbox_revision"
	MetaVisibleCount    = "_meta:visible_count"
	MetaRetainedCount   = "_meta:retained_count"
	MetaRetainedBytes   = "_meta:retained_bytes"
	MetaUnreadCount     = "_meta:unread_count"
	MetaClaimableCount  = "_meta:claimable_count"
	MetaDeliveryRecords = "_meta:delivery_tombstone_count"
	MetaReceiptItems    = "_meta:receipt_count"
	MetaReceiptBytes    = "_meta:receipt_bytes"
	mailboxSchemaV1     = "player-mail-box-v1"
)

// EntryField is entry:<mail_id>.
func (FieldBuilderV1) EntryField(mailID string) (string, error) {
	if err := mail.ValidateMailID(mailID); err != nil {
		return "", err
	}
	return "entry:" + mailID, nil
}

// StateField is state:<mail_id>.
func (FieldBuilderV1) StateField(mailID string) (string, error) {
	if err := mail.ValidateMailID(mailID); err != nil {
		return "", err
	}
	return "state:" + mailID, nil
}

// DeliveryField is delivery:<delivery_digest>.
func (FieldBuilderV1) DeliveryField(deliveryDigest string) (string, error) {
	if err := mail.ValidateHash(deliveryDigest, false); err != nil {
		return "", err
	}
	return "delivery:" + deliveryDigest, nil
}

// MutationField is mutation:<mutation_digest>.
func (FieldBuilderV1) MutationField(mutationDigest string) (string, error) {
	if err := mail.ValidateHash(mutationDigest, false); err != nil {
		return "", err
	}
	return "mutation:" + mutationDigest, nil
}

// ProjectionField is projection:<work_digest>.
func (FieldBuilderV1) ProjectionField(workDigest string) (string, error) {
	if err := mail.ValidateHash(workDigest, false); err != nil {
		return "", err
	}
	return "projection:" + workDigest, nil
}

// ReceiptIndexField is the per-mail bounded receipt reference list used by
// physical GC. It is a mailbox field rather than a separate key, so all
// receipt and mail cleanup remains in the same Redis hash slot.
func (FieldBuilderV1) ReceiptIndexField(mailID string) (string, error) {
	if err := mail.ValidateMailID(mailID); err != nil {
		return "", err
	}
	return "receipt-index:" + mailID, nil
}
