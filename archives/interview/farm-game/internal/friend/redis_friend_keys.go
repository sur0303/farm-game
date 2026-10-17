package friend

// The public auth-v1 key builder intentionally only models PLAYER/FARM Actor
// buckets.  Friendship has a different non-Actor placement contract (33A),
// so this file contains the narrow FRIEND_PAIR/FRIEND_QUOTA/
// FRIEND_SHARE_ISSUER builder.  It reuses rediskey.RedisKey and the shared
// byte-identical slot assertion; it is not a second Actor key space.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// FriendshipAuthorityKind is the stable authority kind used in 33A bucket
// hashing and Hash Tags.  It must not be confused with an Actor RouteDomain.
type FriendshipAuthorityKind string

const (
	FriendPairAuthority        FriendshipAuthorityKind = "FRIEND_PAIR"
	FriendQuotaAuthority       FriendshipAuthorityKind = "FRIEND_QUOTA"
	FriendShareIssuerAuthority FriendshipAuthorityKind = "FRIEND_SHARE_ISSUER"
)

// FriendshipBucketRef is an application bucket, not a Redis slot and not an
// Actor VirtualShard.  Bucket counts are immutable once data is written.
type FriendshipBucketRef struct {
	Kind          FriendshipAuthorityKind
	LayoutVersion string
	BucketID      uint32
}

// FriendRedisProfile contains only the 33A placement parameters.  The
// 4096/16 values are the contract's first-round candidates, not production
// capacity claims.
type FriendRedisProfile struct {
	Namespace             rediskey.Namespace
	LayoutVersion         string
	PairBucketCount       uint32
	QuotaBucketCount      uint32
	ShareBucketCount      uint32
	ReceiptPartitionCount uint32
}

// DefaultFriendRedisProfile returns the 33A candidate layout for a validated
// namespace.
func DefaultFriendRedisProfile(namespace rediskey.Namespace) FriendRedisProfile {
	return FriendRedisProfile{
		Namespace:             namespace,
		LayoutVersion:         "friend-auth-v1",
		PairBucketCount:       4096,
		QuotaBucketCount:      4096,
		ShareBucketCount:      4096,
		ReceiptPartitionCount: 16,
	}
}

var (
	ErrInvalidFriendProfile = errors.New("friend: invalid redis profile")
	ErrInvalidFriendBucket  = errors.New("friend: invalid redis bucket")
	ErrFriendCrossSlot      = errors.New("friend: keys do not share one authority hash tag")
)

func (p FriendRedisProfile) Validate() error {
	if p.Namespace.Validate() != nil || !validFriendLayout(p.LayoutVersion) ||
		p.PairBucketCount == 0 || p.QuotaBucketCount == 0 || p.ShareBucketCount == 0 ||
		p.ReceiptPartitionCount == 0 || p.ReceiptPartitionCount > uint32(^uint16(0)) {
		return ErrInvalidFriendProfile
	}
	return nil
}

func (p FriendRedisProfile) bucketCount(kind FriendshipAuthorityKind) uint32 {
	switch kind {
	case FriendPairAuthority:
		return p.PairBucketCount
	case FriendQuotaAuthority:
		return p.QuotaBucketCount
	case FriendShareIssuerAuthority:
		return p.ShareBucketCount
	default:
		return 0
	}
}

// FriendshipKeyBuilder is the 33A-specific physical builder.  All business
// callers pass canonical IDs; no caller can supply a bucket or Redis key.
type FriendshipKeyBuilder struct{ profile FriendRedisProfile }

func NewFriendshipKeyBuilder(profile FriendRedisProfile) (FriendshipKeyBuilder, error) {
	if err := profile.Validate(); err != nil {
		return FriendshipKeyBuilder{}, err
	}
	return FriendshipKeyBuilder{profile: profile}, nil
}

func (b FriendshipKeyBuilder) Profile() FriendRedisProfile { return b.profile }

// FriendIndexKeys returns one viewer-local, same-slot projection key family.
// FriendIndex is rebuildable and deliberately lives outside Pair authority;
// these keys can never be used as a relationship authorization source.
func (b FriendshipKeyBuilder) FriendNavigationVisitedKey(viewerPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.profile.Validate(); err != nil || viewerPlayerID == 0 {
		return "", ErrInvalidFriendBucket
	}
	return rediskey.RedisKey(fmt.Sprintf("classic-farm:%s:%s:friend-navigation-v3:{friend-navigation-v3:%d}:visited",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone, viewerPlayerID)), nil
}

// FriendNavigationStateKey stores the complete lossy Navigation snapshot.
// State, visited and fence keys deliberately share one viewer-local hash tag so
// the CAS scripts remain single-slot operations on Redis Cluster.
func (b FriendshipKeyBuilder) FriendNavigationStateKey(viewerPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.profile.Validate(); err != nil || viewerPlayerID == 0 {
		return "", ErrInvalidFriendBucket
	}
	return rediskey.RedisKey(fmt.Sprintf("classic-farm:%s:%s:friend-navigation-v3:{friend-navigation-v3:%d}:state",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone, viewerPlayerID)), nil
}

// FriendNavigationTopKStateKey stores the versioned top-k snapshot record.
// Top-k payloads deliberately live in a separate hash from the legacy
// FriendNavigationStateKey payload so an old binary (which ignores the new
// record and the new JSON fields) can never accept a truncated top-k payload
// as a complete legacy full-tail. The legacy key stays the full-tail
// authority and rollback reads it unchanged.
func (b FriendshipKeyBuilder) FriendNavigationTopKStateKey(viewerPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.profile.Validate(); err != nil || viewerPlayerID == 0 {
		return "", ErrInvalidFriendBucket
	}
	return rediskey.RedisKey(fmt.Sprintf("classic-farm:%s:%s:friend-navigation-v3:{friend-navigation-v3:%d}:state-topk-v1",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone, viewerPlayerID)), nil
}

// FriendNavigationFencesKey stores the server-side subscription fences that
// make repeated Ready notifications idempotent across service instances.
func (b FriendshipKeyBuilder) FriendNavigationFencesKey(viewerPlayerID uint64) (rediskey.RedisKey, error) {
	if err := b.profile.Validate(); err != nil || viewerPlayerID == 0 {
		return "", ErrInvalidFriendBucket
	}
	return rediskey.RedisKey(fmt.Sprintf("classic-farm:%s:%s:friend-navigation-v3:{friend-navigation-v3:%d}:fences",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone, viewerPlayerID)), nil
}

func (b FriendshipKeyBuilder) FriendIndexRepairKeys() (due, remaining rediskey.RedisKey, err error) {
	if err = b.profile.Validate(); err != nil {
		return
	}
	prefix := fmt.Sprintf("classic-farm:%s:%s:friend-index-repair-v1:{friend-index-repair-v1}",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone)
	due = rediskey.RedisKey(prefix + ":due")
	remaining = rediskey.RedisKey(prefix + ":remaining")
	if err = rediskey.AssertSameSlot(due, remaining); err != nil {
		err = fmt.Errorf("%w: friend index repair keys", ErrFriendCrossSlot)
	}
	return
}

func (b FriendshipKeyBuilder) FriendIndexKeys(viewerPlayerID uint64) (relation, epoch, activated, sortMember, order, meta rediskey.RedisKey, err error) {
	if err = b.profile.Validate(); err != nil || viewerPlayerID == 0 {
		if err == nil {
			err = ErrInvalidFriendBucket
		}
		return
	}
	prefix := fmt.Sprintf("classic-farm:%s:%s:friend-index-v1:{friend-index-v1:%d}",
		b.profile.Namespace.Environment, b.profile.Namespace.Zone, viewerPlayerID)
	relation = rediskey.RedisKey(prefix + ":relation")
	epoch = rediskey.RedisKey(prefix + ":epoch")
	activated = rediskey.RedisKey(prefix + ":activated")
	sortMember = rediskey.RedisKey(prefix + ":sort")
	order = rediskey.RedisKey(prefix + ":order")
	meta = rediskey.RedisKey(prefix + ":meta")
	if err = rediskey.AssertSameSlot(relation, epoch, activated, sortMember, order, meta); err != nil {
		err = fmt.Errorf("%w: friend index keys", ErrFriendCrossSlot)
	}
	return
}

func (b FriendshipKeyBuilder) BucketFor(kind FriendshipAuthorityKind, authorityID string) (FriendshipBucketRef, error) {
	if err := b.profile.Validate(); err != nil {
		return FriendshipBucketRef{}, err
	}
	if err := validateFriendAuthorityID(kind, authorityID); err != nil {
		return FriendshipBucketRef{}, err
	}
	count := b.profile.bucketCount(kind)
	if count == 0 {
		return FriendshipBucketRef{}, ErrInvalidFriendBucket
	}
	preimage := appendFriendLP(nil, "friend-authority-bucket-v1")
	preimage = appendFriendLP(preimage, string(kind))
	preimage = appendFriendLP(preimage, authorityID)
	bucket := uint32(xxhash.Sum64(preimage) % uint64(count))
	return FriendshipBucketRef{Kind: kind, LayoutVersion: b.profile.LayoutVersion, BucketID: bucket}, nil
}

func (b FriendshipKeyBuilder) HashTag(ref FriendshipBucketRef) (string, error) {
	if err := b.profile.Validate(); err != nil || ref.LayoutVersion != b.profile.LayoutVersion ||
		b.profile.bucketCount(ref.Kind) == 0 || ref.BucketID >= b.profile.bucketCount(ref.Kind) {
		return "", ErrInvalidFriendBucket
	}
	segment := strings.ToLower(strings.TrimPrefix(string(ref.Kind), "FRIEND_"))
	if ref.Kind == FriendShareIssuerAuthority {
		// 33A freezes the physical segment as "share" even though the
		// logical authority kind is FRIEND_SHARE_ISSUER.
		segment = "share"
	}
	tagLayout := strings.TrimPrefix(ref.LayoutVersion, "friend-auth-")
	if tagLayout == ref.LayoutVersion {
		tagLayout = ref.LayoutVersion
	}
	return "classic-farm:" + b.profile.Namespace.Environment + ":" + b.profile.Namespace.Zone +
		":friend-auth:" + segment + ":" + tagLayout + ":b:" + strconv.FormatUint(uint64(ref.BucketID), 10), nil
}

func (b FriendshipKeyBuilder) key(ref FriendshipBucketRef, suffix string) (rediskey.RedisKey, error) {
	tag, err := b.HashTag(ref)
	if err != nil {
		return "", err
	}
	return rediskey.RedisKey("{" + tag + "}:" + suffix), nil
}

func (b FriendshipKeyBuilder) Record(ref FriendshipBucketRef, authorityID string) (rediskey.RedisKey, error) {
	if err := validateFriendAuthorityID(ref.Kind, authorityID); err != nil {
		return "", err
	}
	return b.key(ref, "record:"+authorityID)
}

func (b FriendshipKeyBuilder) Receipt(ref FriendshipBucketRef, authorityID string, digest [32]byte) (rediskey.RedisKey, error) {
	if err := validateFriendAuthorityID(ref.Kind, authorityID); err != nil {
		return "", err
	}
	partition := binary.BigEndian.Uint16(digest[:2]) % uint16(b.profile.ReceiptPartitionCount)
	return b.ReceiptPartition(ref, authorityID, uint32(partition))
}

// ReceiptPartition returns a bounded receipt partition for maintenance/read
// paths that must enumerate typed receipts without scanning or guessing a
// hash.  The caller is required to use the immutable profile partition count.
func (b FriendshipKeyBuilder) ReceiptPartition(ref FriendshipBucketRef, authorityID string, partition uint32) (rediskey.RedisKey, error) {
	if err := validateFriendAuthorityID(ref.Kind, authorityID); err != nil || partition >= b.profile.ReceiptPartitionCount {
		return "", ErrInvalidFriendBucket
	}
	return b.key(ref, "receipt:"+authorityID+":p:"+strconv.FormatUint(uint64(partition), 10))
}

func (b FriendshipKeyBuilder) CommitAttempt(ref FriendshipBucketRef, authorityID string, digest [32]byte) (rediskey.RedisKey, error) {
	if err := validateFriendAuthorityID(ref.Kind, authorityID); err != nil {
		return "", err
	}
	partition := binary.BigEndian.Uint16(digest[:2]) % uint16(b.profile.ReceiptPartitionCount)
	return b.key(ref, "attempt:"+authorityID+":p:"+strconv.FormatUint(uint64(partition), 10))
}

func (b FriendshipKeyBuilder) Resolution(ref FriendshipBucketRef, authorityID string, digest [32]byte) (rediskey.RedisKey, error) {
	if err := validateFriendAuthorityID(ref.Kind, authorityID); err != nil {
		return "", err
	}
	partition := binary.BigEndian.Uint16(digest[:2]) % uint16(b.profile.ReceiptPartitionCount)
	return b.key(ref, "resolution:"+authorityID+":p:"+strconv.FormatUint(uint64(partition), 10))
}

func (b FriendshipKeyBuilder) CommitAttemptDueIndex(ref FriendshipBucketRef) (rediskey.RedisKey, error) {
	return b.key(ref, "attempt-due")
}

func (b FriendshipKeyBuilder) SourceOrder(ref FriendshipBucketRef, workType, workID string) (rediskey.RedisKey, error) {
	if ref.Kind != FriendPairAuthority || !validFriendStableName(workType) || workidentity.ValidateStableOpaqueID(workID) != nil {
		return "", ErrInvalidFriendBucket
	}
	return b.key(ref, "order:"+workType+":"+workID)
}

func (b FriendshipKeyBuilder) SourceDueIndex(ref FriendshipBucketRef) (rediskey.RedisKey, error) {
	if ref.Kind != FriendPairAuthority {
		return "", ErrInvalidFriendBucket
	}
	return b.key(ref, "due")
}

func (b FriendshipKeyBuilder) ReceiptDueIndex(ref FriendshipBucketRef) (rediskey.RedisKey, error) {
	return b.key(ref, "receipt-due")
}

func (b FriendshipKeyBuilder) BucketMeta(ref FriendshipBucketRef) (rediskey.RedisKey, error) {
	return b.key(ref, "bucket-meta")
}

func (b FriendshipKeyBuilder) BucketScanLease(ref FriendshipBucketRef) (rediskey.RedisKey, error) {
	return b.key(ref, "scan-lease")
}

func (b FriendshipKeyBuilder) AssertSameSlot(keys ...rediskey.RedisKey) error {
	if err := rediskey.AssertSameSlot(keys...); err != nil {
		return fmt.Errorf("%w: %v", ErrFriendCrossSlot, err)
	}
	return nil
}

func validateFriendAuthorityID(kind FriendshipAuthorityKind, id string) error {
	switch kind {
	case FriendPairAuthority:
		if workidentity.ValidateStableOpaqueID(id) != nil {
			return ErrInvalidFriendBucket
		}
	case FriendQuotaAuthority, FriendShareIssuerAuthority:
		if id == "0" || id == "" || (len(id) > 1 && id[0] == '0') {
			return ErrInvalidFriendBucket
		}
		for i := 0; i < len(id); i++ {
			if id[i] < '0' || id[i] > '9' {
				return ErrInvalidFriendBucket
			}
		}
	default:
		return ErrInvalidFriendBucket
	}
	return nil
}

func validFriendLayout(value string) bool {
	if strings.HasPrefix(value, "friend-auth-") {
		value = strings.TrimPrefix(value, "friend-auth-")
	}
	if len(value) < 2 || value[0] != 'v' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func validFriendStableName(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func appendFriendLP(dst []byte, value string) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(value)))
	dst = append(dst, n[:]...)
	return append(dst, value...)
}

// friendReceiptDueMember is reversible and contains no client-controlled key
// material.  It is only a discovery member; the receipt HASH is the truth.
func friendReceiptDueMember(kind FriendshipAuthorityKind, authorityID, field string) string {
	blob := appendFriendLP(nil, string(kind))
	blob = appendFriendLP(blob, authorityID)
	blob = appendFriendLP(blob, field)
	return "fr1_" + base64.RawURLEncoding.EncodeToString(blob)
}

func decodeFriendReceiptDueMember(member string) (FriendshipAuthorityKind, string, string, error) {
	if !strings.HasPrefix(member, "fr1_") {
		return "", "", "", fmt.Errorf("%w: receipt due member prefix", ErrFriendReceiptContract)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(member, "fr1_"))
	if err != nil {
		return "", "", "", fmt.Errorf("%w: receipt due member encoding", ErrFriendReceiptContract)
	}
	read := func(input []byte) (string, []byte, error) {
		if len(input) < 4 {
			return "", nil, fmt.Errorf("%w: receipt due member field", ErrFriendReceiptContract)
		}
		n := int(binary.BigEndian.Uint32(input[:4]))
		if n < 0 || n > len(input)-4 {
			return "", nil, fmt.Errorf("%w: receipt due member length", ErrFriendReceiptContract)
		}
		return string(input[4 : 4+n]), input[4+n:], nil
	}
	kind, tail, err := read(raw)
	if err != nil {
		return "", "", "", err
	}
	authorityID, tail, err := read(tail)
	if err != nil {
		return "", "", "", err
	}
	field, tail, err := read(tail)
	if err != nil || len(tail) != 0 {
		if err != nil {
			return "", "", "", err
		}
		return "", "", "", fmt.Errorf("%w: receipt due member trailing bytes", ErrFriendReceiptContract)
	}
	return FriendshipAuthorityKind(kind), authorityID, field, nil
}

func friendAttemptDueMember(kind FriendshipAuthorityKind, authorityID, digestHex string, ordinal uint64) string {
	blob := appendFriendLP(nil, string(kind))
	blob = appendFriendLP(blob, authorityID)
	blob = appendFriendLP(blob, digestHex)
	blob = appendFriendLP(blob, strconv.FormatUint(ordinal, 10))
	return "fa1_" + base64.RawURLEncoding.EncodeToString(blob)
}

func decodeFriendAttemptDueMember(member string) (FriendshipAuthorityKind, string, string, uint64, error) {
	if !strings.HasPrefix(member, "fa1_") {
		return "", "", "", 0, fmt.Errorf("%w: attempt due member prefix", ErrFriendReceiptContract)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(member, "fa1_"))
	if err != nil {
		return "", "", "", 0, fmt.Errorf("%w: attempt due member encoding", ErrFriendReceiptContract)
	}
	read := func(input []byte) (string, []byte, error) {
		if len(input) < 4 {
			return "", nil, fmt.Errorf("%w: attempt due member field", ErrFriendReceiptContract)
		}
		n := int(binary.BigEndian.Uint32(input[:4]))
		if n > len(input)-4 {
			return "", nil, fmt.Errorf("%w: attempt due member length", ErrFriendReceiptContract)
		}
		return string(input[4 : 4+n]), input[4+n:], nil
	}
	kind, tail, err := read(raw)
	if err != nil {
		return "", "", "", 0, err
	}
	authorityID, tail, err := read(tail)
	if err != nil {
		return "", "", "", 0, err
	}
	digest, tail, err := read(tail)
	if err != nil {
		return "", "", "", 0, err
	}
	ordinalText, tail, err := read(tail)
	if err != nil || len(tail) != 0 {
		if err != nil {
			return "", "", "", 0, err
		}
		return "", "", "", 0, fmt.Errorf("%w: attempt due member trailing bytes", ErrFriendReceiptContract)
	}
	ordinal, err := strconv.ParseUint(ordinalText, 10, 64)
	if err != nil || ordinal == 0 {
		return "", "", "", 0, fmt.Errorf("%w: attempt due member ordinal", ErrFriendReceiptContract)
	}
	return FriendshipAuthorityKind(kind), authorityID, digest, ordinal, nil
}

func friendDigestBytes(value []byte) [32]byte { return sha256.Sum256(value) }
