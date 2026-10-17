package friend

// Redis implementations of the three SO0 authority ports. This file is
// deliberately private to the friendship package; these repositories use
// only the canonical bucket layout and never a global relationship slot or
// roster index.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// FriendRedisLimits are bounded physical limits for one 33A bucket.  They
// are configuration, not business rules; MaxFriends is the only domain limit
// and is copied into each player's quota record by the quota script.
type FriendRedisLimits struct {
	MaxFriends           int
	MaxReceiptCount      int
	MaxReceiptBytes      int
	MaxPendingOrders     int
	MaxPendingOrderBytes int
	MaxRecordBytes       int
	// The generic fields above are retained as a narrow compatibility input
	// for old callers.  New physical limits are authority-specific; a Pair or
	// Share record must never silently inherit Quota's 256 KiB budget.
	MaxPairRecordBytes       int
	MaxQuotaRecordBytes      int
	MaxShareRecordBytes      int
	MaxPairReceiptCount      int
	MaxQuotaReceiptCount     int
	MaxShareReceiptCount     int
	MaxPairReceiptBytes      int
	MaxQuotaReceiptBytes     int
	MaxShareReceiptBytes     int
	MaxPairPendingOrders     int
	MaxPairPendingOrderBytes int
	MaxSingleReceiptBytes    int
	MaxSourceOrderBytes      int
	MaxAttemptBytes          int
	MaxScriptInputBytes      int
	AttemptRetentionMs       int64
	ReceiptRetentionMs       int64
	ShareTokenTTLMs          int64
}

const (
	friendPairRecordHardLimit    = 16 << 10
	friendQuotaRecordHardLimit   = 256 << 10
	friendShareRecordHardLimit   = 16 << 10
	friendReceiptHardLimit       = 2 << 10
	friendSourceOrderHardLimit   = 16 << 10
	friendScriptInputHardLimit   = 64 << 10
	friendPairReceiptCountHard   = 512
	friendQuotaReceiptCountHard  = 512
	friendShareReceiptCountHard  = 4096
	friendQuotaOccupancyHard     = 512
	friendPairPendingCountHard   = 64
	friendPairPendingBytesHard   = 1 << 20
	friendReceiptAggregateHard   = 8 << 20
	friendAttemptCursorHardLimit = 2 << 10
	friendResolutionReserveBytes = 1 << 10
	friendMaxAutomaticAttempts   = 4
	// A completed FCM2 proof includes the exact receipt/attempt binding and up
	// to two immutable SourceOrder tuple locators. It is deliberately distinct
	// from the sub-2KiB cursor itself; 8KiB bounds the measured complete proof
	// shape without rejecting ordinary two-order Pair commits.
	friendAttemptHardLimit = 8 << 10
	// SourceOrder logical bytes are deliberately conservative.  The fixed
	// allowance covers Redis hash/listpack overhead and every immutable
	// WorkRecord field; the six tuple values are added exactly in Go and Lua.
	friendSourceOrderFixedBytes    = 512
	friendSourceOrderFieldOverhead = 16
)

func defaultFriendRedisLimits() FriendRedisLimits {
	return FriendRedisLimits{
		MaxFriends:               defaultMaxFriends(),
		MaxPairRecordBytes:       friendPairRecordHardLimit,
		MaxQuotaRecordBytes:      friendQuotaRecordHardLimit,
		MaxShareRecordBytes:      friendShareRecordHardLimit,
		MaxPairReceiptCount:      friendPairReceiptCountHard,
		MaxQuotaReceiptCount:     friendQuotaReceiptCountHard,
		MaxShareReceiptCount:     friendShareReceiptCountHard,
		MaxPairReceiptBytes:      1 << 20,
		MaxQuotaReceiptBytes:     1 << 20,
		MaxShareReceiptBytes:     friendReceiptAggregateHard,
		MaxPairPendingOrders:     friendPairPendingCountHard,
		MaxPairPendingOrderBytes: friendPairPendingBytesHard,
		MaxSingleReceiptBytes:    friendReceiptHardLimit,
		MaxSourceOrderBytes:      friendSourceOrderHardLimit,
		MaxAttemptBytes:          friendAttemptHardLimit,
		MaxScriptInputBytes:      friendScriptInputHardLimit,
		AttemptRetentionMs:       24 * 60 * 60 * 1000,
		ReceiptRetentionMs:       30 * 24 * 60 * 60 * 1000,
		ShareTokenTTLMs:          defaultShareTokenTTLMs(),
	}
}

// RedisFriendshipOptions selects the 33A physical layout and bounded limits.
// Profile must be created with DefaultFriendRedisProfile or
// NewFriendshipKeyBuilder; a zero namespace is rejected rather than silently
// producing keys in the wrong environment.
type RedisFriendshipOptions struct {
	Profile FriendRedisProfile
	Limits  FriendRedisLimits
}

func (o *RedisFriendshipOptions) normalize() error {
	if err := o.Profile.Validate(); err != nil {
		return err
	}
	raw := o.Limits
	d := defaultFriendRedisLimits()
	if o.Limits.MaxFriends <= 0 {
		o.Limits.MaxFriends = d.MaxFriends
	}
	if raw.MaxFriends > friendQuotaOccupancyHard {
		return fmt.Errorf("%w: quota occupancy hard limit", ErrInvalidCommand)
	}
	if raw.MaxRecordBytes > friendPairRecordHardLimit || raw.MaxRecordBytes < 0 ||
		raw.MaxPairRecordBytes > friendPairRecordHardLimit || raw.MaxPairRecordBytes < 0 ||
		raw.MaxQuotaRecordBytes > friendQuotaRecordHardLimit || raw.MaxQuotaRecordBytes < 0 ||
		raw.MaxShareRecordBytes > friendShareRecordHardLimit || raw.MaxShareRecordBytes < 0 {
		return fmt.Errorf("%w: record logical-byte hard limit", ErrInvalidCommand)
	}
	if raw.MaxSingleReceiptBytes > friendReceiptHardLimit || raw.MaxSingleReceiptBytes < 0 ||
		raw.MaxSourceOrderBytes > friendSourceOrderHardLimit || raw.MaxSourceOrderBytes < 0 ||
		raw.MaxAttemptBytes > friendAttemptHardLimit || raw.MaxAttemptBytes < 0 ||
		raw.MaxScriptInputBytes > friendScriptInputHardLimit || raw.MaxScriptInputBytes < 0 {
		return fmt.Errorf("%w: per-item/script hard limit", ErrInvalidCommand)
	}
	if raw.MaxReceiptCount > friendPairReceiptCountHard || raw.MaxReceiptCount < 0 ||
		raw.MaxPairReceiptCount > friendPairReceiptCountHard || raw.MaxPairReceiptCount < 0 ||
		raw.MaxQuotaReceiptCount > friendQuotaReceiptCountHard || raw.MaxQuotaReceiptCount < 0 ||
		raw.MaxShareReceiptCount > friendShareReceiptCountHard || raw.MaxShareReceiptCount < 0 ||
		raw.MaxPendingOrders > friendPairPendingCountHard || raw.MaxPendingOrders < 0 ||
		raw.MaxPairPendingOrders > friendPairPendingCountHard || raw.MaxPairPendingOrders < 0 {
		return fmt.Errorf("%w: receipt/order item hard limit", ErrInvalidCommand)
	}
	if raw.MaxReceiptBytes > friendReceiptAggregateHard || raw.MaxReceiptBytes < 0 ||
		raw.MaxPairReceiptBytes > friendReceiptAggregateHard || raw.MaxPairReceiptBytes < 0 ||
		raw.MaxQuotaReceiptBytes > friendReceiptAggregateHard || raw.MaxQuotaReceiptBytes < 0 ||
		raw.MaxShareReceiptBytes > friendReceiptAggregateHard || raw.MaxShareReceiptBytes < 0 ||
		raw.MaxPendingOrderBytes > friendPairPendingBytesHard || raw.MaxPendingOrderBytes < 0 ||
		raw.MaxPairPendingOrderBytes > friendPairPendingBytesHard || raw.MaxPairPendingOrderBytes < 0 {
		return fmt.Errorf("%w: receipt/order byte hard limit", ErrInvalidCommand)
	}
	// A non-zero legacy value is an explicit override, but it is constrained
	// by the narrowest frozen authority budget so it cannot recreate the old
	// one-size-fits-all 256 KiB Pair/Share layout.
	if raw.MaxRecordBytes > 0 {
		o.Limits.MaxPairRecordBytes, o.Limits.MaxQuotaRecordBytes, o.Limits.MaxShareRecordBytes = raw.MaxRecordBytes, raw.MaxRecordBytes, raw.MaxRecordBytes
	}
	if o.Limits.MaxPairRecordBytes <= 0 {
		o.Limits.MaxPairRecordBytes = d.MaxPairRecordBytes
	}
	if o.Limits.MaxQuotaRecordBytes <= 0 {
		o.Limits.MaxQuotaRecordBytes = d.MaxQuotaRecordBytes
	}
	if o.Limits.MaxShareRecordBytes <= 0 {
		o.Limits.MaxShareRecordBytes = d.MaxShareRecordBytes
	}
	if raw.MaxReceiptCount > 0 {
		o.Limits.MaxPairReceiptCount, o.Limits.MaxQuotaReceiptCount, o.Limits.MaxShareReceiptCount = raw.MaxReceiptCount, raw.MaxReceiptCount, raw.MaxReceiptCount
	}
	if o.Limits.MaxPairReceiptCount <= 0 {
		o.Limits.MaxPairReceiptCount = d.MaxPairReceiptCount
	}
	if o.Limits.MaxQuotaReceiptCount <= 0 {
		o.Limits.MaxQuotaReceiptCount = d.MaxQuotaReceiptCount
	}
	if o.Limits.MaxShareReceiptCount <= 0 {
		o.Limits.MaxShareReceiptCount = d.MaxShareReceiptCount
	}
	if raw.MaxReceiptBytes > 0 {
		o.Limits.MaxPairReceiptBytes, o.Limits.MaxQuotaReceiptBytes, o.Limits.MaxShareReceiptBytes = raw.MaxReceiptBytes, raw.MaxReceiptBytes, raw.MaxReceiptBytes
	}
	if o.Limits.MaxPairReceiptBytes <= 0 {
		o.Limits.MaxPairReceiptBytes = d.MaxPairReceiptBytes
	}
	if o.Limits.MaxQuotaReceiptBytes <= 0 {
		o.Limits.MaxQuotaReceiptBytes = d.MaxQuotaReceiptBytes
	}
	if o.Limits.MaxShareReceiptBytes <= 0 {
		o.Limits.MaxShareReceiptBytes = d.MaxShareReceiptBytes
	}
	if raw.MaxPendingOrders > 0 {
		o.Limits.MaxPairPendingOrders = raw.MaxPendingOrders
	}
	if o.Limits.MaxPairPendingOrders <= 0 {
		o.Limits.MaxPairPendingOrders = d.MaxPairPendingOrders
	}
	if raw.MaxPendingOrderBytes > 0 {
		o.Limits.MaxPairPendingOrderBytes = raw.MaxPendingOrderBytes
	}
	if o.Limits.MaxPairPendingOrderBytes <= 0 {
		o.Limits.MaxPairPendingOrderBytes = d.MaxPairPendingOrderBytes
	}
	if o.Limits.MaxSingleReceiptBytes <= 0 {
		o.Limits.MaxSingleReceiptBytes = d.MaxSingleReceiptBytes
	}
	if o.Limits.MaxSourceOrderBytes <= 0 {
		o.Limits.MaxSourceOrderBytes = d.MaxSourceOrderBytes
	}
	if o.Limits.MaxAttemptBytes <= 0 {
		o.Limits.MaxAttemptBytes = d.MaxAttemptBytes
	}
	if o.Limits.MaxScriptInputBytes <= 0 {
		o.Limits.MaxScriptInputBytes = d.MaxScriptInputBytes
	}
	if o.Limits.AttemptRetentionMs <= 0 {
		o.Limits.AttemptRetentionMs = d.AttemptRetentionMs
	}
	if o.Limits.ReceiptRetentionMs <= 0 {
		o.Limits.ReceiptRetentionMs = d.ReceiptRetentionMs
	}
	if o.Limits.ShareTokenTTLMs <= 0 {
		o.Limits.ShareTokenTTLMs = d.ShareTokenTTLMs
	}
	if o.Limits.AttemptRetentionMs > o.Limits.ReceiptRetentionMs {
		return fmt.Errorf("%w: attempt marker retention exceeds receipt evidence retention", ErrInvalidCommand)
	}
	return nil
}

func (l FriendRedisLimits) recordBytes(kind FriendshipAuthorityKind) int {
	if l.MaxRecordBytes > 0 {
		return l.MaxRecordBytes
	}
	switch kind {
	case FriendPairAuthority:
		return l.MaxPairRecordBytes
	case FriendQuotaAuthority:
		return l.MaxQuotaRecordBytes
	default:
		return l.MaxShareRecordBytes
	}
}
func (l FriendRedisLimits) receiptCount(kind FriendshipAuthorityKind) int {
	if l.MaxReceiptCount > 0 {
		return l.MaxReceiptCount
	}
	switch kind {
	case FriendPairAuthority:
		return l.MaxPairReceiptCount
	case FriendQuotaAuthority:
		return l.MaxQuotaReceiptCount
	default:
		return l.MaxShareReceiptCount
	}
}
func (l FriendRedisLimits) receiptBytes(kind FriendshipAuthorityKind) int {
	if l.MaxReceiptBytes > 0 {
		return l.MaxReceiptBytes
	}
	switch kind {
	case FriendPairAuthority:
		return l.MaxPairReceiptBytes
	case FriendQuotaAuthority:
		return l.MaxQuotaReceiptBytes
	default:
		return l.MaxShareReceiptBytes
	}
}

// attemptAggregateBytes reserves room for one bounded cursor, one compact
// resolution and one maximum-sized live marker for every receipt identity.
// It deliberately shares the receipt-count lifecycle: after that many
// identities, callers must archive/expire evidence rather than grow a bucket.
func (l FriendRedisLimits) attemptAggregateBytes(kind FriendshipAuthorityKind) int {
	return l.receiptCount(kind) * (friendAttemptCursorHardLimit + friendResolutionReserveBytes + l.MaxAttemptBytes)
}

// RedisFriendshipRepositories exposes all three frozen ports while keeping
// the shared physical authority object private.
type RedisFriendshipRepositories struct {
	Pair        *RedisPairRepository
	Quota       *RedisQuotaRepository
	Share       *RedisShareIssuerRepository
	Index       *RedisFriendIndexRepository
	RepairQueue *RedisFriendIndexRepairQueue
	Navigation  *RedisNavigationVisitedRepository

	authority *redisFriendAuthority
}

// NewRedisFriendshipRepositoriesForDomains constructs real Redis adapters
// with the canonical C6A domain split: Pair/Quota/Share authority records
// land on authorityClient while FriendIndex, index repair and navigation
// projections land on projectionClient.  Passing the same client twice is
// equivalent to the legacy single-domain layout and remains supported by
// NewRedisFriendshipRepositories for tests and legacy callers.  The caller
// owns client lifecycle; this constructor does not ping Redis or alter
// server state.  Call EnsureScriptsLoaded during startup when a warm cache is
// desired; individual writes also recover NOSCRIPT on the target node.
func NewRedisFriendshipRepositoriesForDomains(authorityClient, projectionClient redis.UniversalClient, options RedisFriendshipOptions) (*RedisFriendshipRepositories, error) {
	if authorityClient == nil {
		return nil, fmt.Errorf("%w: redis client", ErrInvalidCommand)
	}
	if projectionClient == nil {
		return nil, fmt.Errorf("%w: friend index redis client", ErrInvalidCommand)
	}
	if err := options.normalize(); err != nil {
		return nil, err
	}
	builder, err := NewFriendshipKeyBuilder(options.Profile)
	if err != nil {
		return nil, err
	}
	scripts, err := newFriendScriptSet(authorityClient)
	if err != nil {
		return nil, err
	}
	a := &redisFriendAuthority{client: authorityClient, keys: builder, limits: options.Limits, scripts: scripts}
	repairQueue, err := NewRedisFriendIndexRepairQueue(projectionClient, options.Profile)
	if err != nil {
		return nil, err
	}
	navigation, err := NewRedisNavigationVisitedRepository(projectionClient, options.Profile, DefaultNavigationVisitedTTL)
	if err != nil {
		return nil, err
	}
	return &RedisFriendshipRepositories{
		Pair: &RedisPairRepository{authority: a}, Quota: &RedisQuotaRepository{authority: a},
		Share: &RedisShareIssuerRepository{authority: a},
		Index: &RedisFriendIndexRepository{client: projectionClient, keys: builder}, RepairQueue: repairQueue,
		Navigation: navigation, authority: a,
	}, nil
}

// NewRedisFriendshipRepositories is the legacy single-client constructor.  It
// keeps the pre-C6A layout for tests and legacy compositions; production
// wiring must use NewRedisFriendshipRepositoriesForDomains to bind authority
// and projection repositories to their dedicated domains.
func NewRedisFriendshipRepositories(client redis.UniversalClient, options RedisFriendshipOptions) (*RedisFriendshipRepositories, error) {
	return NewRedisFriendshipRepositoriesForDomains(client, client, options)
}

// NewRedisFriendshipRepositoriesForNamespace is a convenience for callers
// that want the frozen candidate v1 layout without manually filling options.
func NewRedisFriendshipRepositoriesForNamespace(client redis.UniversalClient, namespace rediskey.Namespace) (*RedisFriendshipRepositories, error) {
	return NewRedisFriendshipRepositories(client, RedisFriendshipOptions{Profile: DefaultFriendRedisProfile(namespace)})
}

func (r *RedisFriendshipRepositories) EnsureScriptsLoaded(ctx context.Context) error {
	if r == nil || r.authority == nil {
		return fmt.Errorf("%w: nil repositories", ErrInvalidCommand)
	}
	return r.authority.scripts.ensureLoaded(ctx)
}

func (r *RedisFriendshipRepositories) ScriptSHA(name string) (string, bool) {
	if r == nil || r.authority == nil {
		return "", false
	}
	return r.authority.scripts.scriptSHA(name)
}

// EnvelopeReadMetrics returns a monotonic snapshot of the single-EVAL
// authority-envelope read path. It exposes atomic reads and reconciliation
// outcomes without adding a process lock or changing repository semantics.
func (r *RedisFriendshipRepositories) EnvelopeReadMetrics() FriendEnvelopeReadMetrics {
	if r == nil || r.authority == nil {
		return FriendEnvelopeReadMetrics{}
	}
	return r.authority.envelopeReads.snapshot()
}

func (r *RedisPairRepository) EnvelopeReadMetrics() FriendEnvelopeReadMetrics {
	if r == nil || r.authority == nil {
		return FriendEnvelopeReadMetrics{}
	}
	return r.authority.envelopeReads.snapshot()
}

// CompactCompletedAttemptMarkers performs one bounded maintenance batch for
// an exact authority. The attempt-due ZSET is the frozen recovery discovery
// key: OPEN attempts remain discoverable for resolution, while expired
// COMMITTED entries are converted to immutable FRS3 resolution evidence and
// their two-field FCM2 marker is deleted atomically. Callers enumerate
// authority buckets externally; this method never scans keys or a whole HASH.
func (r *RedisFriendshipRepositories) CompactCompletedAttemptMarkers(ctx context.Context,
	kind FriendshipAuthorityKind, authorityID string, limit int64) (int, error) {
	if r == nil || r.authority == nil || limit <= 0 || limit > 128 {
		return 0, fmt.Errorf("%w: attempt compaction batch", ErrInvalidCommand)
	}
	seed, err := r.authority.group(kind, authorityID, [32]byte{})
	if err != nil {
		return 0, err
	}
	now, err := r.authority.client.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	members, err := r.authority.client.ZRangeByScore(ctx, seed.attemptDue, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: limit,
	}).Result()
	if err != nil {
		return 0, err
	}
	compacted := 0
	for _, member := range members {
		memberKind, memberAuthority, digestHex, ordinal, decodeErr := decodeFriendAttemptDueMember(member)
		if decodeErr != nil || memberKind != kind || memberAuthority != authorityID || !validFriendHex(digestHex, sha256.Size) {
			return compacted, fmt.Errorf("%w: malformed attempt discovery member", ErrFriendReconcile)
		}
		digestBytes, _ := hex.DecodeString(digestHex)
		var digest [sha256.Size]byte
		copy(digest[:], digestBytes)
		group, groupErr := r.authority.group(kind, authorityID, digest)
		if groupErr != nil {
			return compacted, groupErr
		}
		state, cursorOrdinal, attemptID, candidateHash, cursorErr := r.authority.friendAttemptCursor(ctx,
			group.attempt, digestHex, kind, authorityID)
		if cursorErr != nil {
			return compacted, cursorErr
		}
		if state != "COMMITTED" || cursorOrdinal != ordinal {
			continue
		}
		resolutionID, resolutionErr := friendResolutionID(kind, authorityID, digestHex, ordinal)
		if resolutionErr != nil {
			return compacted, resolutionErr
		}
		markerDigest := friendCommitMarkerDigest(candidateHash, ordinal)
		compactKeys := []string{group.attempt, group.resolution, group.attemptDue, group.receipt, group.meta, group.receiptDue}
		if group.sourceDue != "" {
			compactKeys = append(compactKeys, group.sourceDue)
		}
		result, compactErr := r.authority.scripts.run(ctx, scriptFriendCompact,
			compactKeys, group.tag, digestHex,
			ordinal, attemptID, candidateHash, markerDigest, "resolve:"+digestHex+":"+itoa(ordinal),
			resolutionID, member, string(kind), group.ref.LayoutVersion, group.ref.BucketID)
		if compactErr != nil {
			return compacted, compactErr
		}
		if len(result) != 1 {
			return compacted, fmt.Errorf("%w: marker compaction reply", ErrRedisContractViolation)
		}
		switch result[0] {
		case "COMPACTED":
			compacted++
		case "SKIPPED":
		case "RECONCILE_REQUIRED":
			return compacted, ErrFriendReconcile
		default:
			return compacted, mapFriendScriptError(result[0])
		}
	}
	return compacted, nil
}

// These constructors are useful when a composition root wants to inject one
// port at a time without constructing the legacy store.
func NewRedisPairRepository(client redis.UniversalClient, options RedisFriendshipOptions) (*RedisPairRepository, error) {
	r, err := NewRedisFriendshipRepositories(client, options)
	if err != nil {
		return nil, err
	}
	return r.Pair, nil
}

func NewRedisQuotaRepository(client redis.UniversalClient, options RedisFriendshipOptions) (*RedisQuotaRepository, error) {
	r, err := NewRedisFriendshipRepositories(client, options)
	if err != nil {
		return nil, err
	}
	return r.Quota, nil
}

func NewRedisShareIssuerRepository(client redis.UniversalClient, options RedisFriendshipOptions) (*RedisShareIssuerRepository, error) {
	r, err := NewRedisFriendshipRepositories(client, options)
	if err != nil {
		return nil, err
	}
	return r.Share, nil
}

// FriendEnvelopeReadMetrics exposes the bounded atomic-read path without
// coupling the repository to a metrics backend. Composition roots may sample
// these monotonic counters for Prometheus/export; tests use them to prove no
// hidden client-side retry was introduced.
type FriendEnvelopeReadMetrics struct {
	AtomicReadTotal        uint64
	PresentTotal           uint64
	AbsentTotal            uint64
	ReconcileTotal         uint64
	TechnicalFailureTotal  uint64
	AtomicReadLatencyNanos uint64
}

type friendEnvelopeReadCounters struct {
	atomicReadTotal        atomic.Uint64
	presentTotal           atomic.Uint64
	absentTotal            atomic.Uint64
	reconcileTotal         atomic.Uint64
	technicalFailureTotal  atomic.Uint64
	atomicReadLatencyNanos atomic.Uint64
}

func (c *friendEnvelopeReadCounters) snapshot() FriendEnvelopeReadMetrics {
	if c == nil {
		return FriendEnvelopeReadMetrics{}
	}
	return FriendEnvelopeReadMetrics{
		AtomicReadTotal: c.atomicReadTotal.Load(), PresentTotal: c.presentTotal.Load(),
		AbsentTotal: c.absentTotal.Load(), ReconcileTotal: c.reconcileTotal.Load(),
		TechnicalFailureTotal: c.technicalFailureTotal.Load(), AtomicReadLatencyNanos: c.atomicReadLatencyNanos.Load(),
	}
}

type redisFriendAuthority struct {
	client        redis.UniversalClient
	keys          FriendshipKeyBuilder
	limits        FriendRedisLimits
	scripts       *friendScriptSet
	envelopeReads friendEnvelopeReadCounters
}

type friendRedisGroup struct {
	ref        FriendshipBucketRef
	tag        string
	record     string
	receipt    string
	attempt    string
	resolution string
	attemptDue string
	receiptDue string
	sourceDue  string
	meta       string
}

func (a *redisFriendAuthority) group(kind FriendshipAuthorityKind, authorityID string, digest [32]byte) (friendRedisGroup, error) {
	ref, err := a.keys.BucketFor(kind, authorityID)
	if err != nil {
		return friendRedisGroup{}, err
	}
	tag, err := a.keys.HashTag(ref)
	if err != nil {
		return friendRedisGroup{}, err
	}
	key := func(value rediskey.RedisKey, e error) (string, error) {
		if e != nil {
			return "", e
		}
		return string(value), nil
	}
	record, err := key(a.keys.Record(ref, authorityID))
	if err != nil {
		return friendRedisGroup{}, err
	}
	receipt, err := key(a.keys.Receipt(ref, authorityID, digest))
	if err != nil {
		return friendRedisGroup{}, err
	}
	attempt, err := key(a.keys.CommitAttempt(ref, authorityID, digest))
	if err != nil {
		return friendRedisGroup{}, err
	}
	resolution, err := key(a.keys.Resolution(ref, authorityID, digest))
	if err != nil {
		return friendRedisGroup{}, err
	}
	attemptDue, err := key(a.keys.CommitAttemptDueIndex(ref))
	if err != nil {
		return friendRedisGroup{}, err
	}
	receiptDue, err := key(a.keys.ReceiptDueIndex(ref))
	if err != nil {
		return friendRedisGroup{}, err
	}
	meta, err := key(a.keys.BucketMeta(ref))
	if err != nil {
		return friendRedisGroup{}, err
	}
	group := friendRedisGroup{ref: ref, tag: tag, record: record, receipt: receipt, attempt: attempt,
		resolution: resolution, attemptDue: attemptDue, receiptDue: receiptDue, meta: meta}
	if kind == FriendPairAuthority {
		group.sourceDue, err = key(a.keys.SourceDueIndex(ref))
		if err != nil {
			return friendRedisGroup{}, err
		}
	}
	return group, a.keys.AssertSameSlot(groupKeys(group)...)
}

func groupKeys(group friendRedisGroup) []rediskey.RedisKey {
	keys := []rediskey.RedisKey{rediskey.RedisKey(group.record), rediskey.RedisKey(group.receipt),
		rediskey.RedisKey(group.attempt), rediskey.RedisKey(group.resolution), rediskey.RedisKey(group.attemptDue),
		rediskey.RedisKey(group.receiptDue), rediskey.RedisKey(group.meta)}
	if group.sourceDue != "" {
		keys = append(keys, rediskey.RedisKey(group.sourceDue))
	}
	return keys
}

func (a *redisFriendAuthority) sourceOrderKey(group friendRedisGroup, workType, workID string) (string, error) {
	key, err := a.keys.SourceOrder(group.ref, workType, workID)
	if err != nil {
		return "", err
	}
	return string(key), nil
}

func (a *redisFriendAuthority) assertSameSlot(keys ...string) error {
	converted := make([]rediskey.RedisKey, len(keys))
	for i, key := range keys {
		converted[i] = rediskey.RedisKey(key)
	}
	return a.keys.AssertSameSlot(converted...)
}

func (a *redisFriendAuthority) readFields(ctx context.Context, key string, names ...string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	values, err := a.client.HMGet(ctx, key, names...).Result()
	if err != nil {
		return nil, err
	}
	fields := make(map[string]string, len(names))
	for i, value := range values {
		if value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			fields[names[i]] = typed
		case []byte:
			fields[names[i]] = string(typed)
		default:
			return nil, fmt.Errorf("%w: HMGET field %s type %T", ErrFriendReceiptContract, names[i], value)
		}
	}
	return fields, nil
}

var friendBucketMetaManifest = []string{
	"schema_version", "authority_kind", "layout_version", "bucket_id",
	"receipt_count", "receipt_bytes", "pending_work_count", "pending_work_bytes",
	"attempt_count", "attempt_bytes", "last_updated_at_ms",
}

// validateAuthorityEnvelope validates the fixed bucket metadata before any
// authoritative read can treat an absent or present canonical record as
// meaningful. The record type, metadata and discovery cardinalities come from
// one atomic Redis snapshot; a pipeline alone cannot provide that guarantee.
func (a *redisFriendAuthority) validateAuthorityEnvelope(ctx context.Context, group friendRedisGroup,
	kind FriendshipAuthorityKind) (bool, error) {
	if group.ref.Kind != kind {
		return false, fmt.Errorf("%w: authority snapshot kind", ErrFriendReconcile)
	}
	snapshot, err := a.readAuthorityCompleteSnapshot(ctx, group, []string{group.record}, nil)
	if err != nil {
		return false, err
	}
	return snapshot.records[0].keyType == "hash", nil
}

func (a *redisFriendAuthority) readCommitMarkerProof(ctx context.Context, attemptKey, markerDigest string) (map[string]string, bool, error) {
	base := "commit_marker:" + markerDigest + ":"
	completeField, proofField := base+"complete", base+"proof"
	completeBytes, err := a.client.HStrLen(ctx, attemptKey, completeField).Result()
	if err != nil {
		return nil, false, err
	}
	proofBytes, err := a.client.HStrLen(ctx, attemptKey, proofField).Result()
	if err != nil {
		return nil, false, err
	}
	if completeBytes == 0 && proofBytes == 0 {
		return nil, false, nil
	}
	if completeBytes != 1 || proofBytes <= 0 || proofBytes > int64(a.limits.MaxAttemptBytes) || proofBytes > friendAttemptHardLimit {
		return nil, false, fmt.Errorf("%w: marker physical bound", ErrFriendReconcile)
	}
	stored, err := a.readFields(ctx, attemptKey, completeField, proofField)
	if err != nil {
		return nil, false, err
	}
	if stored[completeField] != "1" || stored[proofField] == "" {
		return nil, false, fmt.Errorf("%w: incomplete marker", ErrFriendReconcile)
	}
	proof, err := parseFriendCommitMarkerProof(stored[proofField])
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrFriendReconcile, err)
	}
	return proof, true, nil
}

func validateFriendCommitMarkerBinding(proof map[string]string, markerDigest, attemptID, candidateHash string,
	ordinal uint64, receiptField string, receiptPrefix []byte, receiptKind string, receipt friendReceiptValue,
	layout, authorityID string) error {
	if proof["v"] != "FCM2" || proof["d"] != markerDigest || proof["c"] != candidateHash ||
		proof["a"] != attemptID || proof["o"] != itoa(ordinal) || proof["rf"] != receiptField ||
		proof["rp"] != string(receiptPrefix) || proof["rk"] != receiptKind ||
		proof["rs"] != friendStatusToken(receipt.Status) || proof["re"] != itoa(receipt.Epoch) ||
		proof["ly"] != layout || proof["au"] != authorityID || proof["rm"] == "" {
		return fmt.Errorf("%w: marker identity binding", ErrFriendReconcile)
	}
	if proof["e"] != "0" && proof["e"] != "1" {
		return fmt.Errorf("%w: marker record existence", ErrFriendReconcile)
	}
	for _, tag := range []string{"o", "rv", "rb", "re", "rx", "oc", "fc", "fb", "pc", "pb", "bu"} {
		if _, err := strconv.ParseUint(proof[tag], 10, 64); err != nil {
			return fmt.Errorf("%w: marker numeric tag %s", ErrFriendReconcile, tag)
		}
	}
	if proof["rd"] != "0" && !validFriendHex(proof["rd"], sha256.Size) {
		return fmt.Errorf("%w: marker record digest", ErrFriendReconcile)
	}
	if proof["xf"] == "" {
		if proof["xv"] != "" || proof["xm"] != "" || proof["xx"] != "" {
			return fmt.Errorf("%w: marker extra fact", ErrFriendReconcile)
		}
	} else if proof["xv"] == "" || proof["xm"] == "" {
		return fmt.Errorf("%w: marker extra fact", ErrFriendReconcile)
	} else if _, err := strconv.ParseUint(proof["xx"], 10, 64); err != nil {
		return fmt.Errorf("%w: marker extra due", ErrFriendReconcile)
	}
	orderCount, _ := strconv.ParseUint(proof["oc"], 10, 64)
	for i := uint64(1); i <= orderCount; i++ {
		prefix := "o" + strconv.FormatUint(i, 10)
		if proof[prefix+"i"] == "" || proof[prefix+"t"] == "" || proof[prefix+"r"] == "" ||
			!validFriendHex(proof[prefix+"p"], sha256.Size) {
			return fmt.Errorf("%w: marker order binding", ErrFriendReconcile)
		}
		if _, err := strconv.ParseUint(proof[prefix+"b"], 10, 64); err != nil {
			return fmt.Errorf("%w: marker order bytes", ErrFriendReconcile)
		}
		if !validFriendHex(proof[prefix+"z"], sha1.Size) {
			return fmt.Errorf("%w: marker order payload digest", ErrFriendReconcile)
		}
		if _, err := strconv.ParseUint(proof[prefix+"n"], 10, 64); err != nil {
			return fmt.Errorf("%w: marker order due", ErrFriendReconcile)
		}
	}
	return nil
}

func (a *redisFriendAuthority) validateCommittedReplayProof(ctx context.Context, group friendRedisGroup,
	kind FriendshipAuthorityKind, authorityID string, ci friendCommitIdentity, value friendReceiptValue) error {
	if !validFriendHex(value.CandidateHash, sha256.Size) || value.CommittedAttemptID == "" || value.AttemptOrdinal == 0 {
		return fmt.Errorf("%w: replay receipt attempt proof", ErrFriendReconcile)
	}
	state, ordinal, attemptID, candidateHash, cursorErr := a.friendAttemptCursor(ctx, group.attempt,
		ci.digestHex, kind, authorityID)
	if cursorErr != nil {
		if errors.Is(cursorErr, ErrFriendReconcile) {
			return cursorErr
		}
		return fmt.Errorf("%w: replay cursor proof: %v", ErrFriendReconcile, cursorErr)
	}
	if state != "COMMITTED" || ordinal != value.AttemptOrdinal || attemptID != value.CommittedAttemptID ||
		candidateHash != value.CandidateHash {
		return fmt.Errorf("%w: replay cursor binding", ErrFriendReconcile)
	}
	expectedAttemptID, deriveErr := friendAttemptID(kind, authorityID, ci.digestHex, ordinal)
	if deriveErr != nil {
		return fmt.Errorf("%w: replay deterministic attempt: %v", ErrFriendReconcile, deriveErr)
	}
	if expectedAttemptID != attemptID {
		return fmt.Errorf("%w: replay deterministic attempt", ErrFriendReconcile)
	}
	markerDigest := friendCommitMarkerDigest(candidateHash, ordinal)
	markerProof, markerPresent, markerErr := a.readCommitMarkerProof(ctx, group.attempt, markerDigest)
	if markerErr != nil {
		if errors.Is(markerErr, ErrFriendReconcile) {
			return markerErr
		}
		return fmt.Errorf("%w: replay marker proof: %v", ErrFriendReconcile, markerErr)
	}
	if markerPresent {
		return validateFriendCommitMarkerBinding(markerProof, markerDigest, attemptID, candidateHash, ordinal,
			ci.field, ci.prefix, value.Kind, value, group.ref.LayoutVersion, authorityID)
	}
	resolutionID, resolutionErr := friendResolutionID(kind, authorityID, ci.digestHex, ordinal)
	if resolutionErr != nil {
		return fmt.Errorf("%w: replay resolution identity: %v", ErrFriendReconcile, resolutionErr)
	}
	resolution, resolutionReadErr := a.client.HGet(ctx, group.resolution,
		"resolve:"+ci.digestHex+":"+itoa(ordinal)).Result()
	if resolutionReadErr != nil {
		return fmt.Errorf("%w: replay proof missing: %v", ErrFriendReconcile, resolutionReadErr)
	}
	evidence, evidenceErr := parseFriendResolutionEvidence(resolution)
	if evidenceErr != nil || evidence.Status != "COMMITTED" || evidence.AttemptID != attemptID ||
		evidence.CandidateHash != candidateHash || evidence.ResolutionID != resolutionID || evidence.Ordinal != ordinal ||
		evidence.MarkerDigest != markerDigest || evidence.ReceiptField != ci.field || evidence.ReceiptKind != value.Kind ||
		evidence.ReceiptEpoch != value.Epoch {
		if evidenceErr != nil {
			return fmt.Errorf("%w: replay compact evidence: %v", ErrFriendReconcile, evidenceErr)
		}
		return fmt.Errorf("%w: replay compact evidence", ErrFriendReconcile)
	}
	return nil
}

func (a *redisFriendAuthority) decodeCommittedReplay(ctx context.Context, group friendRedisGroup,
	kind FriendshipAuthorityKind, authorityID string, stored string, ci friendCommitIdentity) (friendReceiptValue, error) {
	value, err := decodeFriendReceipt(stored, ci)
	if err != nil {
		return friendReceiptValue{}, err
	}
	if err := a.validateCommittedReplayProof(ctx, group, kind, authorityID, ci, value); err != nil {
		return friendReceiptValue{}, err
	}
	return value, nil
}

func (a *redisFriendAuthority) commitIdentity(kind FriendshipAuthorityKind, authorityID string, identity CommitIdentity, payloadHash []byte, fields ...string) (friendCommitIdentity, error) {
	if len(payloadHash) == 0 {
		// Empty hashes are not useful as a payload binding.  Hashing the empty
		// payload keeps the port deterministic while making the Redis receipt
		// self-describing and collision-resistant.
		empty := friendDigestBytes(nil)
		payloadHash = empty[:]
	}
	return newFriendCommitIdentity(kind, authorityID, identity, payloadHash, fields...)
}

func friendAttemptID(kind FriendshipAuthorityKind, authorityID, digest string, ordinal uint64) (string, error) {
	return stableID("farm-friend-commit-attempt-id-v1", string(kind), authorityID, digest, itoa(ordinal))
}

func friendResolutionID(kind FriendshipAuthorityKind, authorityID, digest string, ordinal uint64) (string, error) {
	return stableID("farm-friend-resolution-id-v1", string(kind), authorityID, digest, itoa(ordinal))
}

func friendCommitMarkerDigest(candidateHash string, ordinal uint64) string {
	preimage := appendFriendLP(nil, "friend-commit-marker-v1")
	preimage = appendFriendLP(preimage, candidateHash)
	preimage = appendFriendLP(preimage, itoa(ordinal))
	sum := sha256.Sum256(preimage)
	return hex.EncodeToString(sum[:])
}

func friendStringArg(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func friendTextLPBytes(value string) int {
	return len(strconv.Itoa(len(value))) + 1 + len(value)
}

func friendReceiptEstimate(script string, args []interface{}) int {
	if len(args) < 8 {
		return friendReceiptHardLimit + 1
	}
	// receiptValue starts with the already encoded identity prefix, followed
	// by eight length-prefixed fields.  The fixed allowance covers status,
	// reason, epoch, authority time and the TLP1 header without trusting a
	// caller-provided byte counter.
	estimate := 192 + len(friendStringArg(args[2])) + friendTextLPBytes(friendStringArg(args[4])) +
		friendTextLPBytes(friendStringArg(args[6])) + friendTextLPBytes(friendStringArg(args[7])) +
		friendTextLPBytes(friendStringArg(args[5]))
	addTyped := func(values ...interface{}) {
		estimate += len("TLP1")
		for _, value := range values {
			estimate += friendTextLPBytes(friendStringArg(value))
		}
	}
	switch script {
	case scriptPairAction:
		// Accepted dispatch permits echo the immutable admission binding. The
		// timestamps are authority-generated and are covered by the fixed
		// allowance above.
		if len(args) > 45 {
			addTyped(args[39], args[40], args[32], args[33], args[34], args[41], args[35], args[42], args[43], args[45])
		}
	case scriptShareGenerate, scriptShareRotate, scriptShareReactivate, scriptShareRevoke, scriptShareAccept:
		// Share typed values are all derived from operation fields or the
		// current issuer record. Include every operation argument; this is a
		// conservative pre-Prepare bound for variable IDs and payloads.
		if len(args) > 18 {
			values := make([]interface{}, 0, len(args)-18)
			values = append(values, args[18:]...)
			addTyped(values...)
		}
	case scriptQuotaRemove:
		// Remove also creates one FRF1 ReleaseFence in a receipt partition;
		// bound that variable-sized authority item before Prepare as well.
		if len(args) > 22 {
			estimate += len("FRF1") + friendTextLPBytes(friendStringArg(args[16])) +
				friendTextLPBytes(friendStringArg(args[18])) + friendTextLPBytes(friendStringArg(args[19])) +
				friendTextLPBytes(friendStringArg(args[21])) + friendTextLPBytes(friendStringArg(args[22])) + 24
		}
	}
	return estimate
}

func (a *redisFriendAuthority) validateCommitArgs(script string, keys []string, args []interface{}) error {
	total := 0
	for _, key := range keys {
		total += len(key)
	}
	for _, arg := range args {
		total += len(friendStringArg(arg))
	}
	if total > a.limits.MaxScriptInputBytes {
		return fmt.Errorf("%w: script input %d > %d", ErrFriendAuthorityCapacity, total, a.limits.MaxScriptInputBytes)
	}
	// This estimate is intentionally before Prepare. Lua repeats the exact
	// serialized receipt check after deriving Redis-time status/typed fields;
	// the two checks ensure an oversized variable mutation cannot leave an
	// OPEN cursor behind merely because the later exact size was unknown.
	if estimate := friendReceiptEstimate(script, args); estimate > a.limits.MaxSingleReceiptBytes {
		return fmt.Errorf("%w: receipt estimate %d > %d", ErrFriendAuthorityCapacity, estimate, a.limits.MaxSingleReceiptBytes)
	}
	if estimate := friendCommitMarkerEstimate(script, args); estimate > a.limits.MaxAttemptBytes {
		return fmt.Errorf("%w: marker estimate %d > %d", ErrFriendAuthorityCapacity, estimate, a.limits.MaxAttemptBytes)
	}
	if script == scriptPairDelete || script == scriptPairBlock || script == scriptPairUnblock || script == scriptPairAction {
		orderCount, parseErr := strconv.Atoi(friendStringArg(args[16]))
		if parseErr != nil || orderCount < 1 || orderCount > 2 {
			return fmt.Errorf("%w: source order count", ErrRedisContractViolation)
		}
		for i := 0; i < orderCount; i++ {
			start := 17 + i*6
			if start+5 >= len(args)-5 {
				return fmt.Errorf("%w: source order tuple", ErrRedisContractViolation)
			}
			order := friendSourceOrder{id: friendStringArg(args[start]), workType: friendStringArg(args[start+1]),
				blob: friendStringArg(args[start+2]), workRef: friendStringArg(args[start+3]),
				payloadHash: friendStringArg(args[start+4]), dueMS: friendStringArg(args[start+5])}
			if tupleErr := validateFriendSourceOrderTuple(order); tupleErr != nil {
				return tupleErr
			}
			orderBytes := friendSourceOrderLogicalBytes(order)
			if orderBytes > a.limits.MaxSourceOrderBytes {
				return fmt.Errorf("%w: source order %d > %d", ErrFriendAuthorityCapacity, orderBytes, a.limits.MaxSourceOrderBytes)
			}
		}
	}
	return nil
}

func friendCommitMarkerEstimate(script string, args []interface{}) int {
	if len(args) < 17 {
		return friendAttemptHardLimit + 1
	}
	// The fixed FCM2 schema measured with a normal quota release proof is
	// roughly 3.5 KiB. Charge a conservative 4 KiB base, then the variable
	// receipt prefix and exact SourceOrder locator values. Lua enforces the
	// final serialized size before the first canonical write.
	estimate := 4<<10 + len(friendStringArg(args[2]))
	if script == scriptPairDelete || script == scriptPairBlock || script == scriptPairUnblock || script == scriptPairAction {
		orderCount, err := strconv.Atoi(friendStringArg(args[16]))
		if err != nil || orderCount < 0 || orderCount > 2 {
			return friendAttemptHardLimit + 1
		}
		for i := 0; i < orderCount; i++ {
			start := 17 + i*6
			for j := 0; j < 6; j++ {
				estimate += len(friendStringArg(args[start+j]))
			}
		}
	}
	return estimate
}

func friendPrepareOrderFacts(script string, mainKeys []string, args []interface{}) ([]string, []interface{}, error) {
	prepareKeys := []string{}
	extra := []interface{}{0}
	if script != scriptPairDelete && script != scriptPairBlock && script != scriptPairUnblock && script != scriptPairAction {
		return prepareKeys, extra, nil
	}
	if len(mainKeys) < 8 || len(args) < 17 {
		return nil, nil, fmt.Errorf("%w: prepare source-order shape", ErrRedisContractViolation)
	}
	count, err := strconv.Atoi(friendStringArg(args[16]))
	if err != nil || count < 1 || count > 2 || len(mainKeys) != 7+count {
		return nil, nil, fmt.Errorf("%w: prepare source-order count", ErrRedisContractViolation)
	}
	prepareKeys = append(prepareKeys, mainKeys[5])
	extra[0] = count
	for i := 0; i < count; i++ {
		start := 17 + i*6
		if start+5 >= len(args)-5 {
			return nil, nil, fmt.Errorf("%w: prepare source-order tuple", ErrRedisContractViolation)
		}
		prepareKeys = append(prepareKeys, mainKeys[7+i])
		extra = append(extra, args[start:start+6]...)
		shape := "BASE"
		if script == scriptPairAction {
			if friendStringArg(args[start+1]) == "FRIEND_ACTION_DISPATCH_ORDER" {
				shape = "DISPATCH"
			} else {
				shape = "ACTION_RESULT_OPTIONAL"
			}
		}
		extra = append(extra, shape)
	}
	return prepareKeys, extra, nil
}

func friendScriptAllowsAuthorityCreate(script string) bool {
	switch script {
	case scriptPairActivate, scriptPairBlock, scriptQuotaReserve, scriptQuotaRemove,
		scriptShareGenerate, scriptShareRotate, scriptShareReactivate, scriptShareRevoke:
		return true
	default:
		return false
	}
}

func (a *redisFriendAuthority) commit(ctx context.Context, kind FriendshipAuthorityKind, authorityID string,
	identity CommitIdentity, payloadHash []byte, candidateFields []string, script string,
	mainKeys []string, makeArgs func(ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{}) (friendReceiptValue, error) {
	return a.commitInternal(ctx, kind, authorityID, identity, payloadHash, candidateFields, script, mainKeys, makeArgs, false)
}

// commitAllowRecordVersionRefresh is used by quota's optimistic read/retry
// path.  The record version is generated by this adapter rather than supplied
// by the caller; a stale read may therefore be refreshed only after the old
// candidate hash is proven equal with the old before_version.  User-visible
// binding/TTL/fence changes still remain idempotency conflicts.
func (a *redisFriendAuthority) commitAllowRecordVersionRefresh(ctx context.Context, kind FriendshipAuthorityKind, authorityID string,
	identity CommitIdentity, payloadHash []byte, candidateFields []string, script string,
	mainKeys []string, makeArgs func(ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{}) (friendReceiptValue, error) {
	return a.commitInternal(ctx, kind, authorityID, identity, payloadHash, candidateFields, script, mainKeys, makeArgs, true)
}

func (a *redisFriendAuthority) commitInternal(ctx context.Context, kind FriendshipAuthorityKind, authorityID string,
	identity CommitIdentity, payloadHash []byte, candidateFields []string, script string,
	mainKeys []string, makeArgs func(ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{}, allowRecordVersionRefresh bool) (friendReceiptValue, error) {
	ci, err := a.commitIdentity(kind, authorityID, identity, payloadHash, candidateFields...)
	if err != nil {
		return friendReceiptValue{}, err
	}
	group, err := a.group(kind, authorityID, ci.digest)
	if err != nil {
		return friendReceiptValue{}, err
	}
	if len(mainKeys) == 0 {
		return friendReceiptValue{}, fmt.Errorf("%w: empty authority keys", ErrRedisContractViolation)
	}
	if err := a.assertSameSlot(mainKeys...); err != nil {
		return friendReceiptValue{}, err
	}
	// Keep a completed identity with a different payload hash on the
	// documented idempotency-conflict path even when its old attempt cursor is
	// already fenced.  The Lua commit repeats this check atomically; this read
	// never authorizes a mutation.
	if stored, readErr := a.client.HGet(ctx, group.receipt, ci.field).Result(); readErr == nil {
		replayCI, replayErr := a.candidateWithBeforeVersion(ctx, kind, authorityID, identity, payloadHash,
			candidateFields, group.attempt, ci)
		if replayErr != nil {
			return friendReceiptValue{}, replayErr
		}
		if !friendReceiptPrefixMatches([]byte(stored), replayCI.prefix) {
			return friendReceiptValue{}, ErrIdempotentConflict
		}
		return a.decodeCommittedReplay(ctx, group, kind, authorityID, stored, replayCI)
	} else if !errors.Is(readErr, redis.Nil) {
		return friendReceiptValue{}, readErr
	}
	state, oldOrdinal, oldAttempt, oldHash, cursorErr := a.friendAttemptCursor(ctx, group.attempt, ci.digestHex, kind, authorityID)
	if cursorErr != nil {
		return friendReceiptValue{}, cursorErr
	}
	refreshVersion := false
	var ordinal uint64 = 1
	if state == "OPEN" {
		attemptCI := ci
		if oldHash != ci.candidateHash {
			replayCI, replayErr := a.candidateWithBeforeVersion(ctx, kind, authorityID, identity, payloadHash,
				candidateFields, group.attempt, ci)
			if replayErr != nil {
				return friendReceiptValue{}, replayErr
			}
			if replayCI.candidateHash != oldHash {
				return friendReceiptValue{}, ErrIdempotentConflict
			}
			ci = replayCI
		}
		value, resolveErr := a.resolveUnknown(ctx, group, kind, authorityID, ci, oldOrdinal, oldAttempt,
			friendAttemptDueMember(kind, authorityID, ci.digestHex, oldOrdinal), mainKeys...)
		if resolveErr == nil {
			return value, nil
		}
		if !errors.Is(resolveErr, ErrFriendNotCommitted) {
			return value, resolveErr
		}
		if oldOrdinal == ^uint64(0) {
			return friendReceiptValue{}, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
		}
		ci = attemptCI
		if oldHash != ci.candidateHash {
			refreshVersion = true
		}
		ordinal = oldOrdinal + 1
	} else if state == "NOT_COMMITTED" {
		if oldHash != ci.candidateHash {
			if !allowRecordVersionRefresh {
				return friendReceiptValue{}, ErrIdempotentConflict
			}
			replayCI, replayErr := a.candidateWithBeforeVersion(ctx, kind, authorityID, identity, payloadHash,
				candidateFields, group.attempt, ci)
			if replayErr != nil {
				return friendReceiptValue{}, replayErr
			}
			if replayCI.candidateHash != oldHash {
				return friendReceiptValue{}, ErrIdempotentConflict
			}
			refreshVersion = true
		}
		if oldOrdinal == ^uint64(0) {
			return friendReceiptValue{}, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
		}
		ordinal = oldOrdinal + 1
	} else if state == "COMMITTED" {
		return friendReceiptValue{}, fmt.Errorf("%w: committed cursor without replay receipt", ErrFriendReconcile)
	} else if state != "" {
		return friendReceiptValue{}, ErrFriendAttemptConflict
	}
	receiptDue := friendReceiptDueMember(kind, authorityID, ci.field)
	prepareKeys := []string{group.record, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	iterations := 0
	for {
		iterations++
		if iterations > friendMaxAutomaticAttempts {
			return friendReceiptValue{}, fmt.Errorf("%w: automatic attempt bound", ErrFriendReconcile)
		}
		attemptID, err := friendAttemptID(kind, authorityID, ci.digestHex, ordinal)
		if err != nil {
			return friendReceiptValue{}, err
		}
		attemptDue := friendAttemptDueMember(kind, authorityID, ci.digestHex, ordinal)
		args := makeArgs(ci, ordinal, attemptID, attemptDue, receiptDue)
		markerDigest := friendCommitMarkerDigest(ci.candidateHash, ordinal)
		args = append(args, a.limits.MaxSingleReceiptBytes, a.limits.MaxSourceOrderBytes,
			a.limits.MaxAttemptBytes, a.limits.attemptAggregateBytes(kind), markerDigest)
		if inputErr := a.validateCommitArgs(script, mainKeys, args); inputErr != nil {
			return friendReceiptValue{}, inputErr
		}
		refreshMarker := ""
		if refreshVersion {
			refreshMarker = "REFRESH_VERSION"
		}
		expectedVersion, expectedErr := friendCandidateRecordVersion(candidateFields, ci)
		if expectedErr != nil {
			return friendReceiptValue{}, expectedErr
		}
		orderPrepareKeys, orderPrepareArgs, orderPrepareErr := friendPrepareOrderFacts(script, mainKeys, args)
		if orderPrepareErr != nil {
			return friendReceiptValue{}, orderPrepareErr
		}
		thisPrepareKeys := append(append([]string(nil), prepareKeys...), orderPrepareKeys...)
		prepareArgs := []interface{}{group.tag, ci.digestHex, ordinal, attemptID, ci.candidateHash, attemptDue,
			a.limits.AttemptRetentionMs, expectedVersion, refreshMarker, string(kind), group.ref.LayoutVersion,
			group.ref.BucketID, authorityID, a.limits.receiptCount(kind), a.limits.attemptAggregateBytes(kind),
			friendAttemptCursorHardLimit, friendResolutionReserveBytes}
		prepareArgs = append(prepareArgs, orderPrepareArgs...)
		prepareArgs = append(prepareArgs, boolString(friendScriptAllowsAuthorityCreate(script)))
		prepared, prepareErr := a.scripts.run(ctx, scriptFriendPrepare, thisPrepareKeys, prepareArgs...)
		if prepareErr != nil {
			if !errors.Is(prepareErr, ErrRedisTimeoutUnknown) {
				return friendReceiptValue{}, prepareErr
			}
			value, resolveErr := a.resolveUnknown(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, mainKeys...)
			if resolveErr == nil {
				return value, nil
			}
			if !errors.Is(resolveErr, ErrFriendNotCommitted) {
				return value, fmt.Errorf("%w: prepare: %v", resolveErr, prepareErr)
			}
			if ordinal == ^uint64(0) {
				return friendReceiptValue{}, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
			}
			// Prepare is not the business mutation, but its unknown outcome
			// still fences this ordinal before the next prepare.
			ordinal++
			continue
		}
		if len(prepared) == 0 {
			return friendReceiptValue{}, fmt.Errorf("%w: empty prepare result", ErrRedisContractViolation)
		}
		switch prepared[0] {
		case "PREPARED":
		case "ALREADY_PREPARED":
			// A concurrent caller may have observed the same OPEN ordinal.
			// Resolve it before any destructive EVAL; never resend an OPEN
			// attempt merely because the prepare was already present.
			value, resolveErr := a.resolveUnknown(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, mainKeys...)
			if resolveErr == nil {
				return value, nil
			}
			if !errors.Is(resolveErr, ErrFriendNotCommitted) {
				return value, resolveErr
			}
			if ordinal == ^uint64(0) {
				return friendReceiptValue{}, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
			}
			ordinal++
			continue
		case "ALREADY_COMMITTED":
			if stored, readErr := a.client.HGet(ctx, group.receipt, ci.field).Result(); readErr == nil {
				return a.decodeCommittedReplay(ctx, group, kind, authorityID, stored, ci)
			} else if !errors.Is(readErr, redis.Nil) {
				return friendReceiptValue{}, readErr
			}
			return friendReceiptValue{}, fmt.Errorf("%w: prepared committed cursor without receipt", ErrFriendReconcile)
		case "ATTEMPT_CONFLICT":
			if existing, readErr := a.client.HGet(ctx, group.receipt, ci.field).Result(); readErr == nil {
				if !friendReceiptPrefixMatches([]byte(existing), ci.prefix) {
					return friendReceiptValue{}, ErrIdempotentConflict
				}
				return a.decodeCommittedReplay(ctx, group, kind, authorityID, existing, ci)
			} else if !errors.Is(readErr, redis.Nil) {
				return friendReceiptValue{}, readErr
			}
			return friendReceiptValue{}, ErrFriendAttemptConflict
		default:
			return friendReceiptValue{}, mapFriendScriptError(prepared[0])
		}

		reply, runErr := a.scripts.run(ctx, script, mainKeys, args...)
		if runErr != nil {
			if !errors.Is(runErr, ErrRedisTimeoutUnknown) {
				return friendReceiptValue{}, runErr
			}
			value, resolveErr := a.resolveUnknown(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, mainKeys...)
			if resolveErr == nil {
				return value, nil
			}
			if errors.Is(resolveErr, ErrFriendNotCommitted) {
				if ordinal == ^uint64(0) {
					return friendReceiptValue{}, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
				}
				// Only a proven NOT_COMMITTED resolution permits a fresh
				// destructive attempt.
				ordinal++
				continue
			}
			return value, fmt.Errorf("%w: original write: %v", resolveErr, runErr)
		}
		if len(reply) == 1 && friendScriptResultProvesPrecommit(reply[0]) {
			mapped := mapFriendScriptError(reply[0])
			if fenceErr := a.fenceKnownNotCommitted(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, mainKeys...); fenceErr != nil {
				return friendReceiptValue{}, fmt.Errorf("%w: close attempt after %s: %v", fenceErr, reply[0], mapped)
			}
			return friendReceiptValue{}, mapped
		}
		return a.decodeFriendCommitReply(ctx, group, kind, authorityID, reply, ci)
	}
}

func (a *redisFriendAuthority) candidateWithBeforeVersion(ctx context.Context, kind FriendshipAuthorityKind,
	authorityID string, identity CommitIdentity, payloadHash []byte, candidateFields []string,
	attemptKey string, current friendCommitIdentity) (friendCommitIdentity, error) {
	beforeVersion, err := a.client.HGet(ctx, attemptKey, "cursor:"+current.digestHex+":expected_record_version").Result()
	if errors.Is(err, redis.Nil) {
		beforeVersion, err = a.client.HGet(ctx, attemptKey, "cursor:"+current.digestHex+":before_version").Result()
	}
	if errors.Is(err, redis.Nil) {
		return current, nil
	}
	if err != nil {
		return friendCommitIdentity{}, err
	}
	if _, err := strconv.ParseUint(beforeVersion, 10, 64); err != nil {
		return friendCommitIdentity{}, fmt.Errorf("%w: malformed attempt before version", ErrRedisContractViolation)
	}
	adjusted := append([]string(nil), candidateFields...)
	for i := 0; i+1 < len(adjusted); i++ {
		if adjusted[i] == "record_version" {
			adjusted[i+1] = beforeVersion
			break
		}
	}
	if sameFriendStrings(adjusted, candidateFields) {
		return current, nil
	}
	return a.commitIdentity(kind, authorityID, identity, payloadHash, adjusted...)
}

func friendCandidateRecordVersion(fields []string, current friendCommitIdentity) (uint64, error) {
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "record_version" {
			version, err := strconv.ParseUint(fields[i+1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("%w: candidate record version", ErrRedisContractViolation)
			}
			return version, nil
		}
	}
	// Internal participant candidates that predate record-version binding use
	// the current CI unchanged; all SO1 repository mutations include the field.
	return 0, fmt.Errorf("%w: candidate record version missing", ErrRedisContractViolation)
}

func sameFriendStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func friendCandidateWithRecordVersion(fields []string, version uint64) []string {
	adjusted := append([]string(nil), fields...)
	for i := 0; i+1 < len(adjusted); i++ {
		if adjusted[i] == "record_version" {
			adjusted[i+1] = itoa(version)
			break
		}
	}
	return adjusted
}

func (a *redisFriendAuthority) friendAttemptCursor(ctx context.Context, attemptKey, digestHex string,
	kind FriendshipAuthorityKind, authorityID string) (state string,
	ordinal uint64, attemptID, candidateHash string, err error) {
	base := "cursor:" + digestHex
	names := []string{base + ":schema_version", base + ":authority_kind", base + ":authority_id",
		base + ":commit_identity_digest", base + ":ordinal", base + ":attempt_id", base + ":candidate_hash",
		base + ":state", base + ":resolution_id", base + ":before_version", base + ":expected_record_version",
		base + ":before_digest", base + ":updated_at_ms"}
	fields, readErr := a.readFields(ctx, attemptKey, names...)
	if readErr != nil {
		return "", 0, "", "", readErr
	}
	state = fields[base+":state"]
	if state == "" {
		if len(fields) != 0 {
			return "", 0, "", "", fmt.Errorf("%w: partial attempt cursor", ErrFriendReconcile)
		}
		return "", 0, "", "", nil
	}
	if len(fields) != len(names) || fields[base+":schema_version"] != "friend-commit-attempt-v1" ||
		fields[base+":authority_kind"] != string(kind) || fields[base+":authority_id"] != authorityID ||
		fields[base+":commit_identity_digest"] != digestHex || !validFriendHex(fields[base+":candidate_hash"], sha256.Size) {
		return "", 0, "", "", fmt.Errorf("%w: malformed attempt cursor", ErrFriendReconcile)
	}
	ordinal, err = strconv.ParseUint(fields[base+":ordinal"], 10, 64)
	if err != nil || ordinal == 0 {
		return "", 0, "", "", fmt.Errorf("%w: malformed attempt ordinal", ErrFriendReconcile)
	}
	for _, field := range []string{"before_version", "expected_record_version", "updated_at_ms"} {
		if _, parseErr := strconv.ParseUint(fields[base+":"+field], 10, 64); parseErr != nil {
			return "", 0, "", "", fmt.Errorf("%w: malformed attempt %s", ErrFriendReconcile, field)
		}
	}
	beforeDigest := fields[base+":before_digest"]
	if beforeDigest != "0" && !validFriendHex(beforeDigest, sha256.Size) {
		return "", 0, "", "", fmt.Errorf("%w: malformed attempt before digest", ErrFriendReconcile)
	}
	attemptID = fields[base+":attempt_id"]
	candidateHash = fields[base+":candidate_hash"]
	if attemptID == "" || (state != "OPEN" && state != "COMMITTED" && state != "NOT_COMMITTED" && state != "RECONCILE_REQUIRED") {
		return "", 0, "", "", fmt.Errorf("%w: malformed attempt state", ErrFriendReconcile)
	}
	return state, ordinal, attemptID, candidateHash, nil
}

func validFriendHex(value string, bytes int) bool {
	if len(value) != bytes*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func (a *redisFriendAuthority) nextFriendAttemptOrdinal(ctx context.Context, attemptKey, digestHex string) (uint64, error) {
	state, err := a.client.HGet(ctx, attemptKey, "cursor:"+digestHex+":state").Result()
	if errors.Is(err, redis.Nil) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	ordinalText, err := a.client.HGet(ctx, attemptKey, "cursor:"+digestHex+":ordinal").Result()
	if err != nil {
		return 0, err
	}
	ordinal, err := strconv.ParseUint(ordinalText, 10, 64)
	if err != nil || ordinal == 0 {
		return 0, fmt.Errorf("%w: malformed attempt ordinal", ErrRedisContractViolation)
	}
	switch state {
	case "OPEN", "COMMITTED":
		return ordinal, nil
	case "NOT_COMMITTED":
		if ordinal == ^uint64(0) {
			return 0, fmt.Errorf("%w: attempt ordinal overflow", ErrRedisContractViolation)
		}
		return ordinal + 1, nil
	default:
		return 0, ErrFriendAttemptConflict
	}
}

func (a *redisFriendAuthority) resolveUnknown(ctx context.Context, group friendRedisGroup, kind FriendshipAuthorityKind,
	authorityID string, ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue string,
	verificationKeys ...string) (friendReceiptValue, error) {
	return a.resolveAttempt(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, "", verificationKeys...)
}

// fenceKnownNotCommitted closes an OPEN attempt after the business script
// returned a deterministic code that is guaranteed to precede its first
// canonical write (record-version/domain/capacity/schema/work validation).
// This is deliberately separate from timeout reconciliation: a runtime or
// transport error without an exact receipt/FCM2 proof is always fenced as
// RECONCILE_REQUIRED, regardless of the current business state.
func (a *redisFriendAuthority) fenceKnownNotCommitted(ctx context.Context, group friendRedisGroup, kind FriendshipAuthorityKind,
	authorityID string, ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue string,
	verificationKeys ...string) error {
	_, err := a.resolveAttempt(ctx, group, kind, authorityID, ci, ordinal, attemptID, attemptDue, "PRECOMMIT_NOT_COMMITTED", verificationKeys...)
	if errors.Is(err, ErrFriendNotCommitted) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%w: precommit fence unexpectedly committed", ErrFriendReconcile)
	}
	return err
}

func (a *redisFriendAuthority) resolveAttempt(ctx context.Context, group friendRedisGroup, kind FriendshipAuthorityKind,
	authorityID string, ci friendCommitIdentity, ordinal uint64, attemptID, attemptDue, resolutionMode string,
	verificationKeys ...string) (friendReceiptValue, error) {
	resolutionField := "resolve:" + ci.digestHex + ":" + itoa(ordinal)
	resolutionID, err := friendResolutionID(kind, authorityID, ci.digestHex, ordinal)
	if err != nil {
		return friendReceiptValue{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.resolution, group.attemptDue, group.receiptDue, group.meta}
	if kind == FriendPairAuthority && group.sourceDue != "" {
		keys = append(keys, group.sourceDue)
	}
	for _, candidate := range verificationKeys {
		found := false
		for _, existing := range keys {
			if existing == candidate {
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, candidate)
		}
	}
	markerDigest := friendCommitMarkerDigest(ci.candidateHash, ordinal)
	markerExtraKey := ""
	if kind == FriendQuotaAuthority {
		// The quota mutation appends its fixed release-fence hash as the only
		// non-standard verification key.  The bounded FCM2 proof records the
		// fence field/value itself; never read a marker snapshot or enumerate
		// the attempt hash to discover it.
		for _, candidate := range verificationKeys {
			known := candidate == group.record || candidate == group.receipt || candidate == group.attempt ||
				candidate == group.attemptDue || candidate == group.receiptDue || candidate == group.meta
			if !known {
				markerExtraKey = candidate
				break
			}
		}
	}
	reply, err := a.scripts.run(ctx, scriptFriendResolve, keys,
		group.tag, ci.digestHex, ci.field, ci.prefix, attemptID, ordinal, ci.candidateHash,
		resolutionField, resolutionID, attemptDue, resolutionMode, string(kind), group.ref.LayoutVersion,
		group.ref.BucketID, authorityID, group.sourceDue, markerExtraKey, markerDigest)
	if err != nil {
		return friendReceiptValue{}, err
	}
	if len(reply) == 0 {
		return friendReceiptValue{}, fmt.Errorf("%w: empty resolution result", ErrRedisContractViolation)
	}
	switch reply[0] {
	case "COMMITTED":
		if len(reply) < 3 {
			return friendReceiptValue{}, fmt.Errorf("%w: committed resolution shape", ErrRedisContractViolation)
		}
		return decodeFriendReceipt(reply[2], ci)
	case "RESOLUTION_REPLAY":
		if len(reply) < 2 {
			return friendReceiptValue{}, fmt.Errorf("%w: replay resolution shape", ErrRedisContractViolation)
		}
		switch reply[1] {
		case "COMMITTED":
			blob, readErr := a.client.HGet(ctx, group.receipt, ci.field).Result()
			if readErr != nil {
				if errors.Is(readErr, redis.Nil) {
					return friendReceiptValue{}, ErrFriendReconcile
				}
				return friendReceiptValue{}, readErr
			}
			return decodeFriendReceipt(blob, ci)
		case "NOT_COMMITTED":
			return friendReceiptValue{}, ErrFriendNotCommitted
		case "RECONCILE_REQUIRED":
			return friendReceiptValue{}, ErrFriendReconcile
		default:
			return friendReceiptValue{}, fmt.Errorf("%w: replay resolution status %s", ErrRedisContractViolation, reply[1])
		}
	case "NOT_COMMITTED":
		return friendReceiptValue{}, ErrFriendNotCommitted
	case "RECONCILE_REQUIRED":
		return friendReceiptValue{}, ErrFriendReconcile
	default:
		return friendReceiptValue{}, mapFriendScriptError(reply[0])
	}
}

func (a *redisFriendAuthority) decodeFriendCommitReply(ctx context.Context, group friendRedisGroup,
	kind FriendshipAuthorityKind, authorityID string, reply []string, ci friendCommitIdentity) (friendReceiptValue, error) {
	if len(reply) < 2 {
		if len(reply) == 1 {
			return friendReceiptValue{}, mapFriendScriptError(reply[0])
		}
		return friendReceiptValue{}, fmt.Errorf("%w: empty commit result", ErrRedisContractViolation)
	}
	switch reply[0] {
	case "COMMITTED":
		return decodeFriendReceipt(reply[1], ci)
	case "REPLAY":
		return a.decodeCommittedReplay(ctx, group, kind, authorityID, reply[1], ci)
	default:
		return friendReceiptValue{}, mapFriendScriptError(reply[0])
	}
}

func decodeFriendReceipt(blob string, ci friendCommitIdentity) (friendReceiptValue, error) {
	value, err := parseFriendReceipt([]byte(blob))
	if err != nil {
		return friendReceiptValue{}, err
	}
	if !friendReceiptPrefixMatches([]byte(blob), ci.prefix) {
		return friendReceiptValue{}, fmt.Errorf("%w: identity/payload prefix", ErrFriendReceiptContract)
	}
	if value.CandidateHash != "" && value.CandidateHash != ci.candidateHash {
		return friendReceiptValue{}, ErrIdempotentConflict
	}
	return value, nil
}

var ErrFriendAuthorityCapacity = errors.New("friend: authority capacity reached")

func friendScriptResultProvesPrecommit(code string) bool {
	if strings.HasPrefix(code, "CAPACITY_MARKER_") {
		return true
	}
	switch code {
	case "RECORD_VERSION_CONFLICT", "RELATION_EPOCH_CONFLICT", "CAPACITY_RECEIPT", "CAPACITY_WORK",
		"CAPACITY_ITEM_LIMIT", "CAPACITY_BYTE_LIMIT", "CAPACITY_MARKER", "CONTRACT_ERROR", "SCHEMA_MISMATCH",
		"LAYOUT_MISMATCH", "CORRUPT_RECORD", "WORK_CONFLICT", "WORK_RECONCILE", "LOGICAL_BYTES_MISSING",
		"MALFORMED_COUNTER", "RESERVATION_NOT_FOUND":
		return true
	default:
		return false
	}
}

func mapFriendScriptError(code string) error {
	if strings.HasPrefix(code, "CAPACITY_MARKER_") {
		return fmt.Errorf("%w: %s", ErrFriendAuthorityCapacity, code)
	}
	switch code {
	case "IDEMPOTENCY_CONFLICT", "WORK_CONFLICT":
		return ErrIdempotentConflict
	case "ATTEMPT_CONFLICT", "ATTEMPT_REQUIRED", "ATTEMPT_FENCED_OPEN":
		return ErrFriendAttemptConflict
	case "ATTEMPT_FENCED_NOT_COMMITTED":
		return ErrFriendNotCommitted
	case "ATTEMPT_FENCED_RECONCILE_REQUIRED", "ATTEMPT_FENCED_COMMITTED":
		return ErrFriendReconcile
	case "RESERVATION_NOT_FOUND":
		return ErrReservationNotFound
	case "RECORD_VERSION_CONFLICT", "RELATION_EPOCH_CONFLICT":
		return ErrFriendRecordVersionConflict
	case "CAPACITY_RECEIPT", "CAPACITY_WORK", "CAPACITY_ITEM_LIMIT", "CAPACITY_BYTE_LIMIT", "CAPACITY_MARKER",
		"CAPACITY_ATTEMPT", "CAPACITY_ATTEMPT_CURSOR":
		return ErrFriendAuthorityCapacity
	case "WORK_RECONCILE", "LOGICAL_BYTES_MISSING", "MALFORMED_COUNTER", "CORRUPT_ATTEMPT", "MARKER_INCOMPLETE", "RECONCILE_REQUIRED":
		return fmt.Errorf("%w: %s", ErrFriendReconcile, code)
	case "CROSS_SLOT", "CONTRACT_ERROR", "SCHEMA_MISMATCH", "LAYOUT_MISMATCH", "CORRUPT_RECORD":
		return fmt.Errorf("%w: %s", ErrRedisContractViolation, code)
	default:
		return fmt.Errorf("%w: script result %s", ErrRedisContractViolation, code)
	}
}

func outcomeFromReceipt(value friendReceiptValue, identity CommitIdentity, payloadHash []byte) Outcome {
	if len(payloadHash) == 0 {
		empty := friendDigestBytes(nil)
		payloadHash = empty[:]
	}
	return outcomeFromFriendReceipt(value, identity, payloadHash)
}

func parseUintField(fields map[string]string, key string) (uint64, error) {
	return parseCanonicalUintText(fields[key])
}

func parseIntField(fields map[string]string, key string) (int64, error) {
	return parseCanonicalIntText(fields[key])
}

func parseCanonicalUintText(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, fmt.Errorf("non-canonical unsigned integer %q", value)
	}
	return parsed, nil
}

func parseCanonicalIntText(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("non-canonical integer %q", value)
	}
	return parsed, nil
}

func friendHashLogicalBytes(fields map[string]string) uint64 {
	var total uint64
	for field, value := range fields {
		if field == "_meta:logical_bytes" {
			continue
		}
		total += uint64(len(field) + len(value))
	}
	return total
}

func hexHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func friendFenceDigestHex(fenceID string) string {
	digest := friendDigestBytes(appendFriendLP(nil, fenceID))
	return hex.EncodeToString(digest[:])
}

type friendSourceOrder struct {
	id, workType, blob, workRef, payloadHash, dueMS string
}

type friendSourceOrderShape uint8

const (
	friendSourceOrderBase friendSourceOrderShape = iota
	friendSourceOrderActionResult
	friendSourceOrderDispatch
)

var friendSourceOrderBaseManifest = []string{
	"schema_version", "work_id", "work_type", "placement_kind", "bucket_version", "bucket_id",
	"source_authority_kind", "canonical_source_authority_id", "state", "work_version", "typed_payload",
	"work_ref", "payload_hash", "next_due_at_ms", "retry_count", "lease_epoch", "lease_holder_id",
	"lease_token_hash", "lease_expire_at_ms", "created_at_ms", "updated_at_ms", "completed_at_ms",
}

var friendSourceOrderDispatchManifest = []string{
	"permit_id", "relation_id", "relation_epoch", "visitor_player_id", "owner_player_id", "target_farm_id",
	"operation_id", "scope", "permit_payload_hash", "target_binding_hash", "issued_at_authority_ms",
	"expire_at_authority_ms", "execution_accepted_at_authority_ms", "workflow_deadline_at_ms", "dispatch_order_id",
}

func friendSourceOrderManifest(shape friendSourceOrderShape) []string {
	manifest := append([]string(nil), friendSourceOrderBaseManifest...)
	switch shape {
	case friendSourceOrderActionResult:
		manifest = append(manifest, "dispatch_order_id")
	case friendSourceOrderDispatch:
		manifest = append(manifest, friendSourceOrderDispatchManifest...)
	}
	return manifest
}

func newFriendSourceOrder(workType, workID, source string, fields ...string) (friendSourceOrder, error) {
	ref, err := workidentity.EncodeWorkRefV1(workType, workID)
	if err != nil {
		return friendSourceOrder{}, err
	}
	blob := encodeFriendTyped(append([]string{source}, fields...)...)
	order := friendSourceOrder{id: workID, workType: workType, blob: blob, workRef: ref,
		payloadHash: hexHash([]byte(blob)), dueMS: "0"}
	if err := validateFriendSourceOrderTuple(order); err != nil {
		return friendSourceOrder{}, err
	}
	return order, nil
}

func friendOrderArgs(orders []friendSourceOrder) []interface{} {
	args := make([]interface{}, 0, len(orders)*6)
	for _, order := range orders {
		args = append(args, order.id, order.workType, order.blob, order.workRef, order.payloadHash, order.dueMS)
	}
	return args
}

// friendSourceOrderLogicalBytes is the one physical accounting formula used
// by both the adapter preflight and Lua.  It includes all six immutable tuple
// values plus a conservative fixed allowance for the WorkRecord hash fields,
// Redis hash/listpack overhead and the DueIndex member.  Keeping the formula
// explicit avoids Go accepting an order that Lua later rejects (or vice versa).
func friendSourceOrderLogicalBytes(order friendSourceOrder) int {
	values := []string{order.id, order.workType, order.blob, order.workRef, order.payloadHash, order.dueMS}
	total := friendSourceOrderFixedBytes
	for _, value := range values {
		total += friendSourceOrderFieldOverhead + len(value)
	}
	return total
}

func validateFriendSourceOrderTuple(order friendSourceOrder) error {
	if order.id == "" || workidentity.ValidateStableOpaqueID(order.id) != nil ||
		order.workType == "" || !validFriendStableName(order.workType) {
		return fmt.Errorf("%w: source order identity", ErrRedisContractViolation)
	}
	ref, err := workidentity.DecodeWorkRefV1(order.workRef)
	if err != nil || ref.WorkType != order.workType || ref.WorkID != order.id {
		return fmt.Errorf("%w: source order work ref", ErrRedisContractViolation)
	}
	if order.blob == "" || !strings.HasPrefix(order.blob, friendTypedVersion) {
		return fmt.Errorf("%w: source order typed payload", ErrRedisContractViolation)
	}
	if _, err := decodeFriendTypedPayload(order.blob); err != nil {
		return fmt.Errorf("%w: source order typed payload: %v", ErrRedisContractViolation, err)
	}
	if len(order.payloadHash) != sha256.Size*2 || strings.ToLower(order.payloadHash) != order.payloadHash ||
		order.payloadHash != hexHash([]byte(order.blob)) {
		return fmt.Errorf("%w: source order payload hash", ErrRedisContractViolation)
	}
	if order.dueMS == "" {
		return fmt.Errorf("%w: source order due time", ErrRedisContractViolation)
	}
	due, err := strconv.ParseInt(order.dueMS, 10, 64)
	if err != nil || due < 0 || strconv.FormatInt(due, 10) != order.dueMS {
		return fmt.Errorf("%w: source order due time", ErrRedisContractViolation)
	}
	if friendSourceOrderLogicalBytes(order) > friendSourceOrderHardLimit {
		return fmt.Errorf("%w: source order logical bytes", ErrFriendAuthorityCapacity)
	}
	return nil
}

// --- FRIEND_PAIR ---

type RedisPairRepository struct{ authority *redisFriendAuthority }

var _ PairRepository = (*RedisPairRepository)(nil)

func pairIDForPlayers(playerA, playerB uint64) (string, uint64, uint64, error) {
	if !ValidPair(playerA, playerB) {
		return "", 0, 0, ErrInvalidPair
	}
	low, high := CanonicalPair(playerA, playerB)
	pairID, err := RelationIDFor(low, high)
	return pairID, low, high, err
}

func (r *RedisPairRepository) recordKey(playerA, playerB uint64) (string, string, uint64, uint64, error) {
	pairID, low, high, err := pairIDForPlayers(playerA, playerB)
	if err != nil {
		return "", "", 0, 0, err
	}
	ref, err := r.authority.keys.BucketFor(FriendPairAuthority, pairID)
	if err != nil {
		return "", "", 0, 0, err
	}
	key, err := r.authority.keys.Record(ref, pairID)
	if err != nil {
		return "", "", 0, 0, err
	}
	return pairID, string(key), low, high, nil
}

func (r *RedisPairRepository) GetPairByRelationID(ctx context.Context, relationID string) (PairSnapshot, error) {
	if strings.TrimSpace(relationID) == "" {
		return PairSnapshot{}, ErrInvalidPair
	}
	ref, err := r.authority.keys.BucketFor(FriendPairAuthority, relationID)
	if err != nil {
		return PairSnapshot{}, err
	}
	key, err := r.authority.keys.Record(ref, relationID)
	if err != nil {
		return PairSnapshot{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, relationID, [32]byte{})
	if err != nil {
		return PairSnapshot{}, err
	}
	snapshot, err := r.authority.readAuthorityCompleteSnapshot(ctx, group, []string{key.String()}, pairCanonicalManifest)
	if err != nil {
		return PairSnapshot{}, err
	}
	record := snapshot.records[0]
	if record.keyType != "hash" {
		return PairSnapshot{}, ErrNotFound
	}
	low, lowErr := parseCanonicalUintText(record.fields["identity:low_player_id"])
	high, highErr := parseCanonicalUintText(record.fields["identity:high_player_id"])
	derived, deriveErr := RelationIDFor(low, high)
	if lowErr != nil || highErr != nil || deriveErr != nil || record.fields["identity:pair_id"] != relationID || derived != relationID {
		return PairSnapshot{}, fmt.Errorf("%w: pair relation lookup identity", ErrFriendReconcile)
	}
	return pairSnapshotFromRedis(record.fields, relationID, low, high,
		r.authority.keys.Profile().LayoutVersion, record.fieldCount)
}

func (r *RedisPairRepository) GetPair(ctx context.Context, playerA, playerB uint64) (PairSnapshot, error) {
	pairID, key, low, high, err := r.recordKey(playerA, playerB)
	if err != nil {
		return PairSnapshot{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, [32]byte{})
	if err != nil {
		return PairSnapshot{}, err
	}
	snapshot, err := r.authority.readAuthorityCompleteSnapshot(ctx, group, []string{key}, pairCanonicalManifest)
	if err != nil {
		return PairSnapshot{}, err
	}
	record := snapshot.records[0]
	if record.keyType != "hash" {
		return PairSnapshot{PairID: pairID, LowPlayerID: low, HighPlayerID: high,
			Relation: PairRelation{RelationID: pairID}}, nil
	}
	return pairSnapshotFromRedis(record.fields, pairID, low, high,
		r.authority.keys.Profile().LayoutVersion, record.fieldCount)
}

func pairSnapshotFromRedis(fields map[string]string, pairID string, low, high uint64, layout string, fieldCount int64) (PairSnapshot, error) {
	snapshot := PairSnapshot{PairID: pairID, LowPlayerID: low, HighPlayerID: high}
	if len(fields) == 0 {
		return PairSnapshot{}, fmt.Errorf("%w: pair canonical manifest", ErrFriendReconcile)
	}
	required := pairCanonicalManifest
	for _, field := range required {
		if _, ok := fields[field]; !ok {
			return PairSnapshot{}, fmt.Errorf("%w: pair canonical field %s", ErrFriendReconcile, field)
		}
	}
	if fieldCount != int64(len(required)) {
		return PairSnapshot{}, fmt.Errorf("%w: pair canonical manifest", ErrFriendReconcile)
	}
	if fields["_meta:schema_version"] != "friend-pair-v1" || fields["_meta:authority_layout_version"] != layout ||
		fields["identity:pair_id"] != pairID || fields["relation:id"] != pairID ||
		!validFriendHex(fields["_meta:business_state_digest"], sha256.Size) {
		return PairSnapshot{}, fmt.Errorf("%w: pair canonical schema", ErrFriendReconcile)
	}
	parseU := func(key string) (uint64, error) {
		value := fields[key]
		parsed, parseErr := parseCanonicalUintText(value)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: pair numeric field %s", ErrFriendReconcile, key)
		}
		return parsed, nil
	}
	parseI := func(key string) (int64, error) {
		value := fields[key]
		parsed, parseErr := parseCanonicalIntText(value)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: pair timestamp field %s", ErrFriendReconcile, key)
		}
		return parsed, nil
	}
	parsedLow, err := parseU("identity:low_player_id")
	if err != nil || parsedLow != low {
		return PairSnapshot{}, fmt.Errorf("%w: pair low player", ErrFriendReconcile)
	}
	parsedHigh, err := parseU("identity:high_player_id")
	if err != nil || parsedHigh != high {
		return PairSnapshot{}, fmt.Errorf("%w: pair high player", ErrFriendReconcile)
	}
	relationEpoch, err := parseU("relation:epoch")
	if err != nil {
		return PairSnapshot{}, err
	}
	activated, err := parseI("relation:activated_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	deleted, err := parseI("relation:deleted_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	tokenGeneration, err := parseU("relation:activation_token_generation")
	if err != nil {
		return PairSnapshot{}, err
	}
	terminatedBy, err := parseU("relation:terminated_by_player_id")
	if err != nil {
		return PairSnapshot{}, err
	}
	relationStatus, err := parseU("relation:status")
	if err != nil {
		return PairSnapshot{}, err
	}
	terminationReason, err := parseU("relation:termination_reason")
	if err != nil {
		return PairSnapshot{}, err
	}
	if relationStatus > uint64(RelationStatusDeleted) || terminationReason > uint64(TerminationBlocked) {
		return PairSnapshot{}, fmt.Errorf("%w: pair relation state", ErrFriendReconcile)
	}
	snapshot.Relation = PairRelation{RelationID: nonEmpty(fields["relation:id"], pairID),
		Status: RelationStatus(relationStatus), RelationEpoch: relationEpoch, ActivatedAtMs: activated,
		DeletedAtMs: deleted, ActivationTokenID: fields["relation:activation_token_id"],
		ActivationTokenGeneration: tokenGeneration, TerminationReason: TerminationReason(terminationReason),
		TerminatedByPlayerID: terminatedBy, LastChangeOperationID: fields["relation:last_change_operation_id"]}
	lowBlockEpoch, err := parseU("block:low_to_high:epoch")
	if err != nil {
		return PairSnapshot{}, err
	}
	highBlockEpoch, err := parseU("block:high_to_low:epoch")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowCreated, err := parseI("block:low_to_high:changed_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	highCreated, err := parseI("block:high_to_low:changed_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowRemoved, err := parseI("block:low_to_high:removed_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	highRemoved, err := parseI("block:high_to_low:removed_at_ms")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowStatus, err := parseU("block:low_to_high:status")
	if err != nil {
		return PairSnapshot{}, err
	}
	highStatus, err := parseU("block:high_to_low:status")
	if err != nil {
		return PairSnapshot{}, err
	}
	if lowStatus > uint64(BlockStatusRemoved) || highStatus > uint64(BlockStatusRemoved) {
		return PairSnapshot{}, fmt.Errorf("%w: pair block state", ErrFriendReconcile)
	}
	snapshot.LowBlocksHigh = BlockRelation{Status: BlockStatus(lowStatus), BlockEpoch: lowBlockEpoch, CreatedAtMs: lowCreated, RemovedAtMs: lowRemoved}
	snapshot.HighBlocksLow = BlockRelation{Status: BlockStatus(highStatus), BlockEpoch: highBlockEpoch, CreatedAtMs: highCreated, RemovedAtMs: highRemoved}
	barrierState, err := parseU("reactivation:state")
	if err != nil {
		return PairSnapshot{}, err
	}
	terminatedEpoch, err := parseU("reactivation:terminated_relation_epoch")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowFloor, err := parseU("reactivation:low_generation_floor")
	if err != nil {
		return PairSnapshot{}, err
	}
	highFloor, err := parseU("reactivation:high_generation_floor")
	if err != nil {
		return PairSnapshot{}, err
	}
	barrierRevision, err := parseU("reactivation:barrier_revision")
	if err != nil {
		return PairSnapshot{}, err
	}
	snapshot.Barrier = ReactivationBarrier{State: BarrierState(barrierState), TerminatedRelationEpoch: terminatedEpoch,
		LowShareGenerationFloor: lowFloor, HighShareGenerationFloor: highFloor, BarrierRevision: barrierRevision}
	captured, err := parseU("termination:captured_epoch")
	if err != nil {
		return PairSnapshot{}, err
	}
	orderRevision, err := parseU("termination:barrier_revision")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowCaptured, err := parseU("termination:low_floor_captured")
	if err != nil {
		return PairSnapshot{}, err
	}
	highCaptured, err := parseU("termination:high_floor_captured")
	if err != nil {
		return PairSnapshot{}, err
	}
	lowReleased, err := parseU("termination:quota_released_low")
	if err != nil {
		return PairSnapshot{}, err
	}
	highReleased, err := parseU("termination:quota_released_high")
	if err != nil {
		return PairSnapshot{}, err
	}
	completed, err := parseU("termination:completed")
	if err != nil {
		return PairSnapshot{}, err
	}
	if barrierState > uint64(BarrierConsumed) || lowCaptured > 1 || highCaptured > 1 || lowReleased > 1 || highReleased > 1 || completed > 1 {
		return PairSnapshot{}, fmt.Errorf("%w: pair barrier state", ErrFriendReconcile)
	}
	if orderID := fields["termination:order_id"]; orderID != "" {
		snapshot.PendingTermination = &TerminationOrder{OrderID: orderID, CapturedActiveEpoch: captured,
			BarrierRevision: orderRevision, LowFloorCaptured: lowCaptured == 1, HighFloorCaptured: highCaptured == 1,
			QuotaReleasedLow: lowReleased == 1, QuotaReleasedHigh: highReleased == 1, Completed: completed == 1,
			LowShareGenerationFloor: lowFloor, HighShareGenerationFloor: highFloor}
	}
	snapshot.RecordVersion, err = parseU("_meta:record_version")
	if err != nil {
		return PairSnapshot{}, err
	}
	updatedAt, err := parseI("_meta:updated_at_ms")
	if err != nil || updatedAt < 0 {
		return PairSnapshot{}, fmt.Errorf("%w: pair updated timestamp", ErrFriendReconcile)
	}
	logicalBytes, err := parseU("_meta:logical_bytes")
	if err != nil || logicalBytes > uint64(friendPairRecordHardLimit) || logicalBytes != friendHashLogicalBytes(fields) {
		return PairSnapshot{}, fmt.Errorf("%w: pair logical bytes", ErrFriendReconcile)
	}
	return snapshot, nil
}

func nonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func (r *RedisPairRepository) mutatePair(ctx context.Context, cmd PairMutationCommand, mutation, script, receiptKind string) (Outcome, error) {
	if !ValidPair(cmd.CallerPlayerID, cmd.PeerPlayerID) || !cmd.Identity.IsPublic() ||
		cmd.Identity.SubjectPlayerID != cmd.CallerPlayerID {
		return Outcome{}, fmt.Errorf("%w: pair mutation", ErrInvalidCommand)
	}
	pairID, low, high, err := pairIDForPlayers(cmd.CallerPlayerID, cmd.PeerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	// Validate the current canonical record before replay so corruption never
	// hides behind an old receipt. The replay path itself binds the exact public
	// identity/payload prefix, candidate hash, cursor and marker/resolution;
	// rebuilding from today's legitimate record version would invalidate an old
	// success after a later commit.
	snapshot, err := r.GetPair(ctx, cmd.CallerPlayerID, cmd.PeerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	if replay, found, replayErr := r.replayPairReceipt(ctx, pairID, cmd.Identity, cmd.PayloadHash, receiptKind); replayErr != nil {
		return Outcome{}, replayErr
	} else if found {
		return replay, nil
	}
	operationDigest, err := pairOperationDigest(cmd.Identity, pairID)
	if err != nil {
		return Outcome{}, err
	}
	orders := make([]friendSourceOrder, 0, 2)
	if (mutation == "DELETE" || mutation == "BLOCK") && snapshot.Relation.Status == RelationStatusActive {
		terminationID := terminationOrderID(pairID, snapshot.Relation.RelationID, snapshot.Relation.RelationEpoch)
		term, termErr := newFriendSourceOrder("FRIEND_TERMINATION_ORDER", terminationID, "TERMINATION",
			pairID, snapshot.Relation.RelationID, itoa(snapshot.Relation.RelationEpoch), cmd.Identity.OperationID)
		if termErr != nil {
			return Outcome{}, termErr
		}
		orders = append(orders, term)
	}
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		return Outcome{}, err
	}
	result, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "RESULT",
		pairID, operationDigest, "1", cmd.Identity.OperationID)
	if err != nil {
		return Outcome{}, err
	}
	orders = append(orders, result)
	candidateFields := []string{mutation, itoa(cmd.CallerPlayerID), itoa(cmd.PeerPlayerID),
		"record_version", itoa(snapshot.RecordVersion), "operation_digest", operationDigest,
		"authority_time", "REDIS_SERVER_TIME"}
	for _, order := range orders {
		candidateFields = append(candidateFields, order.id, order.workType, order.blob, order.workRef, order.payloadHash, "0")
	}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, cmd.Identity, cmd.PayloadHash, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	orderKeys := make([]string, 0, len(orders))
	for _, order := range orders {
		key, keyErr := r.authority.sourceOrderKey(group, order.workType, order.id)
		if keyErr != nil {
			return Outcome{}, keyErr
		}
		orderKeys = append(orderKeys, key)
	}
	mainKeys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.sourceDue, group.meta}
	mainKeys = append(mainKeys, orderKeys...)
	if err := r.authority.assertSameSlot(mainKeys...); err != nil {
		return Outcome{}, err
	}
	returnValue, err := r.authority.commit(ctx, FriendPairAuthority, pairID, cmd.Identity, cmd.PayloadHash,
		candidateFields, script, mainKeys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			args := []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, receiptKind, ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority),
				r.authority.limits.receiptBytes(FriendPairAuthority), r.authority.limits.MaxPairPendingOrders, r.authority.limits.MaxPairPendingOrderBytes,
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, len(orders)}
			args = append(args, friendOrderArgs(orders)...)
			args = append(args, pairID, group.ref.BucketID, group.ref.LayoutVersion, low, high,
				cmd.CallerPlayerID, cmd.PeerPlayerID, cmd.Identity.OperationID, snapshot.RecordVersion,
				r.authority.limits.recordBytes(FriendPairAuthority))
			return args
		})
	if err != nil {
		return Outcome{}, err
	}
	return outcomeFromReceipt(returnValue, cmd.Identity, cmd.PayloadHash), nil
}

func (r *RedisPairRepository) replayPairReceipt(ctx context.Context, pairID string, identity CommitIdentity,
	payloadHash []byte, receiptKind string) (Outcome, bool, error) {
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, identity, payloadHash)
	if err != nil {
		return Outcome{}, false, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, false, err
	}
	stored, err := r.authority.client.HGet(ctx, group.receipt, ci.field).Result()
	if errors.Is(err, redis.Nil) {
		return Outcome{}, false, nil
	}
	if err != nil {
		return Outcome{}, false, err
	}
	value, err := parseFriendReceipt([]byte(stored))
	if err != nil {
		return Outcome{}, false, err
	}
	if !friendReceiptPrefixMatches([]byte(stored), ci.prefix) || value.Kind != receiptKind {
		return Outcome{}, false, ErrIdempotentConflict
	}
	if proofErr := r.authority.validateCommittedReplayProof(ctx, group, FriendPairAuthority, pairID, ci, value); proofErr != nil {
		return Outcome{}, false, proofErr
	}
	return outcomeFromReceipt(value, identity, payloadHash), true, nil
}

func pairOperationDigest(identity CommitIdentity, pairID string) (string, error) {
	if identity.IsPublic() {
		sum, err := friendOperationLocatorDigest(identity, FriendPairAuthority, pairID)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(sum[:]), nil
	}
	sum, err := friendParticipantLocatorDigest(identity)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sum[:]), nil
}

func (r *RedisPairRepository) DeleteFriend(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return r.mutatePair(ctx, cmd, "DELETE", scriptPairDelete, "DELETE_FRIEND")
}

func (r *RedisPairRepository) BlockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return r.mutatePair(ctx, cmd, "BLOCK", scriptPairBlock, "BLOCK_PLAYER")
}

func (r *RedisPairRepository) UnblockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return r.mutatePair(ctx, cmd, "UNBLOCK", scriptPairUnblock, "UNBLOCK_PLAYER")
}

func (r *RedisPairRepository) ValidatePairForAdd(ctx context.Context, inviter, redeemer uint64) (PairValidationSnapshot, error) {
	snapshot, err := r.GetPair(ctx, inviter, redeemer)
	if err != nil {
		return PairValidationSnapshot{}, err
	}
	record := &PairRecord{PairID: snapshot.PairID, LowPlayerID: snapshot.LowPlayerID, HighPlayerID: snapshot.HighPlayerID,
		Relation: snapshot.Relation, LowBlocksHigh: snapshot.LowBlocksHigh, HighBlocksLow: snapshot.HighBlocksLow,
		Barrier: snapshot.Barrier}
	if !snapshot.Exists() {
		return PairValidationSnapshot{PairID: snapshot.PairID, RelationID: snapshot.PairID, TargetRelationEpoch: 1}, nil
	}
	return record.ValidateForAdd(inviter, redeemer)
}

func (r *RedisPairRepository) ActivateFriendRelation(ctx context.Context, cmd ActivateCommand, identity CommitIdentity, payloadHash []byte) (Outcome, error) {
	if !ValidPair(cmd.InviterPlayerID, cmd.RedeemerPlayerID) || cmd.OperationID == "" {
		return Outcome{}, fmt.Errorf("%w: activation", ErrInvalidCommand)
	}
	pairID, low, high, err := pairIDForPlayers(cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetPair(ctx, cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	expectedRotateReceiptID := ""
	rotateProof := cmd.RotateReceipt
	rotateProofValid := false
	lowReservationID, highReservationID := "", ""
	lowQuotaProof, highQuotaProof := "", ""
	if snapshot.Barrier.State == BarrierReady {
		expectedRotateReceiptID, err = ReactivationRotateReceiptID(pairID, snapshot.Barrier.BarrierRevision, cmd.TokenID)
		if err != nil {
			return Outcome{}, err
		}
		// The Pair adapter never treats caller-provided scalar fields as a
		// proof.  It reads the current persisted ShareIssuer receipt and only
		// forwards an exact match to the Pair Lua script.  The two authorities
		// remain separate hash-tag groups; this read is deliberately outside the
		// Pair EVAL rather than a cross-slot script.
		issuer, issuerErr := (&RedisShareIssuerRepository{authority: r.authority}).GetIssuer(ctx, cmd.InviterPlayerID)
		if issuerErr != nil {
			return Outcome{}, issuerErr
		}
		if persisted, ok := issuer.RotateReceipts[pairID]; ok && persisted.ReceiptID == expectedRotateReceiptID &&
			persisted.PairID == pairID && persisted.CommittedAtMs > 0 && len(persisted.PayloadHash) == sha256.Size &&
			sameFriendRotateReceipt(persisted, cmd.RotateReceipt) {
			rotateProof = persisted
			rotateProofValid = true
		}
		if !rotateProofValid {
			return Outcome{Status: StatusRejected, Reason: ReasonReactivationRotateRequired, CommittedEpoch: snapshot.Relation.RelationEpoch}, nil
		}
		if identity.WorkAddressID == "" {
			return Outcome{Status: StatusRejected, Reason: ReasonReactivationRotateRequired, CommittedEpoch: snapshot.Relation.RelationEpoch}, nil
		}
		lowReservationID, err = QuotaReservationID(identity.WorkAddressID, low, snapshot.Relation.RelationID)
		if err != nil {
			return Outcome{}, err
		}
		highReservationID, err = QuotaReservationID(identity.WorkAddressID, high, snapshot.Relation.RelationID)
		if err != nil {
			return Outcome{}, err
		}
		lowQuotaProof, err = r.quotaReactivationProof(ctx, low, lowReservationID, identity.WorkAddressID,
			snapshot.Relation.RelationID, cmd.TargetRelationEpoch, payloadHash)
		if err != nil {
			return Outcome{}, err
		}
		highQuotaProof, err = r.quotaReactivationProof(ctx, high, highReservationID, identity.WorkAddressID,
			snapshot.Relation.RelationID, cmd.TargetRelationEpoch, payloadHash)
		if err != nil {
			return Outcome{}, err
		}
		if lowQuotaProof == "" || highQuotaProof == "" {
			return Outcome{Status: StatusRejected, Reason: ReasonReactivationRotateRequired, CommittedEpoch: snapshot.Relation.RelationEpoch}, nil
		}
	}
	candidateFields := []string{"ACTIVATE", itoa(cmd.InviterPlayerID), itoa(cmd.RedeemerPlayerID),
		itoa(cmd.TargetRelationEpoch), cmd.TokenID, itoa(cmd.TokenGeneration), cmd.OperationID,
		"record_version", itoa(snapshot.RecordVersion), "rotate_proof_valid", boolString(rotateProofValid),
		rotateProof.ReceiptID, rotateProof.PairID, itoa(rotateProof.TerminatedRelationEpoch),
		itoa(rotateProof.BarrierRevision), rotateProof.TokenID, itoa(rotateProof.TokenGeneration),
		itoa(rotateProof.LowShareGenerationFloor), itoa(rotateProof.HighShareGenerationFloor),
		strconv.FormatInt(rotateProof.CommittedAtMs, 10), rotateProof.OperationID, hex.EncodeToString(rotateProof.PayloadHash),
		"quota_low_player", itoa(low), "quota_low_reservation", lowReservationID, "quota_low_proof", lowQuotaProof, "quota_low_state", "1",
		"quota_high_player", itoa(high), "quota_high_reservation", highReservationID, "quota_high_proof", highQuotaProof, "quota_high_state", "1",
		"authority_time", "REDIS_SERVER_TIME"}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, identity, payloadHash, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	value, err := r.authority.commit(ctx, FriendPairAuthority, pairID, identity, payloadHash,
		candidateFields, scriptPairActivate, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			return []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, "ACTIVATE_RELATION", ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority), r.authority.limits.receiptBytes(FriendPairAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion, group.ref.BucketID,
				pairID, r.authority.limits.recordBytes(FriendPairAuthority), pairID, low, high, cmd.InviterPlayerID, cmd.RedeemerPlayerID, cmd.TargetRelationEpoch, cmd.TokenID,
				cmd.TokenGeneration, boolString(rotateProofValid), cmd.OperationID, snapshot.RecordVersion,
				rotateProof.ReceiptID, rotateProof.PairID, rotateProof.TerminatedRelationEpoch,
				rotateProof.BarrierRevision, rotateProof.TokenID, rotateProof.TokenGeneration,
				rotateProof.LowShareGenerationFloor, rotateProof.HighShareGenerationFloor,
				rotateProof.CommittedAtMs, expectedRotateReceiptID, rotateProof.OperationID,
				hex.EncodeToString(rotateProof.PayloadHash), lowReservationID, lowQuotaProof, boolString(lowQuotaProof != ""),
				highReservationID, highQuotaProof, boolString(highQuotaProof != "")}
		})
	if err != nil {
		return Outcome{}, err
	}
	return outcomeFromReceipt(value, identity, payloadHash), nil
}

// quotaReactivationProof is a read-only cross-authority proof.  Pair Lua
// never receives Quota keys (their HashTag is intentionally different); the
// adapter therefore proves the live reservation and its participant receipt
// before issuing the single-slot Pair mutation and binds the resulting digest
// into that Pair candidate.
func (r *RedisPairRepository) quotaReactivationProof(ctx context.Context, playerID uint64, reservationID,
	workflowID, relationID string, targetEpoch uint64, expectedPayload []byte) (string, error) {
	authorityID := itoa(playerID)
	invocationID, err := stableID("farm-friend-participant-invocation-id-v1", workflowID, "RESERVE_FRIEND_SLOT:"+authorityID)
	if err != nil {
		return "", err
	}
	participant := CommitIdentity{WorkAddressID: workflowID, ParticipantInvocationID: invocationID,
		ParticipantStep: "RESERVE_FRIEND_SLOT:" + authorityID}
	ci, err := r.authority.commitIdentity(FriendQuotaAuthority, authorityID, participant, nil)
	if err != nil {
		return "", err
	}
	group, err := r.authority.group(FriendQuotaAuthority, authorityID, ci.digest)
	if err != nil {
		return "", err
	}
	// A reservation scalar is not proof by itself. Validate the complete
	// canonical Quota record and its bucket metadata before reading the exact
	// reservation/receipt/cursor tuple used by reactivation.
	if _, err := (&RedisQuotaRepository{authority: r.authority}).GetQuota(ctx, playerID); err != nil {
		return "", err
	}
	prefix := "reservation:" + reservationID + ":"
	fields, err := r.authority.readFields(ctx, group.record, prefix+"state", prefix+"workflow_id", prefix+"relation_id", prefix+"target_epoch")
	if err != nil {
		return "", err
	}
	if fields[prefix+"state"] != "1" || fields[prefix+"workflow_id"] != workflowID || fields[prefix+"relation_id"] != relationID || fields[prefix+"target_epoch"] != itoa(targetEpoch) {
		return "", nil
	}
	raw, err := r.authority.client.HGet(ctx, group.receipt, ci.field).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	receipt, err := parseFriendReceipt([]byte(raw))
	if err != nil {
		return "", err
	}
	if receipt.Kind != "QUOTA_RESERVE" || receipt.Status != StatusSucceeded || receipt.Epoch != targetEpoch ||
		!validFriendHex(receipt.CandidateHash, sha256.Size) || receipt.CommittedAttemptID == "" || receipt.AttemptOrdinal == 0 {
		return "", nil
	}
	state, ordinal, attemptID, candidateHash, cursorErr := r.authority.friendAttemptCursor(ctx, group.attempt,
		ci.digestHex, FriendQuotaAuthority, authorityID)
	if cursorErr != nil {
		return "", cursorErr
	}
	expectedAttemptID, attemptErr := friendAttemptID(FriendQuotaAuthority, authorityID, ci.digestHex, receipt.AttemptOrdinal)
	if attemptErr != nil {
		return "", attemptErr
	}
	if state != "COMMITTED" || ordinal != receipt.AttemptOrdinal || attemptID != receipt.CommittedAttemptID ||
		candidateHash != receipt.CandidateHash || expectedAttemptID != attemptID {
		return "", nil
	}
	markerDigest := friendCommitMarkerDigest(candidateHash, ordinal)
	marker, markerPresent, markerErr := r.authority.readCommitMarkerProof(ctx, group.attempt, markerDigest)
	if markerErr != nil {
		return "", markerErr
	}
	prefixMaterial := appendFriendLP(nil, string(ci.material))
	prefixMaterial = appendFriendLP(prefixMaterial, string(expectedPayload))
	if markerPresent {
		if bindingErr := validateFriendCommitMarkerBinding(marker, markerDigest, attemptID, candidateHash, ordinal,
			ci.field, prefixMaterial, "QUOTA_RESERVE", receipt, group.ref.LayoutVersion, authorityID); bindingErr != nil {
			return "", bindingErr
		}
	} else {
		resolutionID, resolutionErr := friendResolutionID(FriendQuotaAuthority, authorityID, ci.digestHex, ordinal)
		if resolutionErr != nil {
			return "", resolutionErr
		}
		resolution, resolutionReadErr := r.authority.client.HGet(ctx, group.resolution,
			"resolve:"+ci.digestHex+":"+itoa(ordinal)).Result()
		if resolutionReadErr != nil {
			if errors.Is(resolutionReadErr, redis.Nil) {
				return "", nil
			}
			return "", resolutionReadErr
		}
		evidence, evidenceErr := parseFriendResolutionEvidence(resolution)
		if evidenceErr != nil || evidence.Status != "COMMITTED" || evidence.AttemptID != attemptID ||
			evidence.CandidateHash != candidateHash || evidence.ResolutionID != resolutionID || evidence.Ordinal != ordinal ||
			evidence.MarkerDigest != markerDigest || evidence.ReceiptField != ci.field || evidence.ReceiptKind != "QUOTA_RESERVE" ||
			evidence.ReceiptEpoch != targetEpoch {
			return "", nil
		}
	}
	identityPart, rest, err := readFriendLP([]byte(raw))
	if err != nil || !bytes.Equal(identityPart, ci.material) {
		return "", nil
	}
	payloadPart, _, err := readFriendLP(rest)
	if err != nil || len(payloadPart) != sha256.Size || !bytes.Equal(payloadPart, expectedPayload) {
		return "", nil
	}
	resultHash := hexHash([]byte(raw))
	proofInput := appendFriendLP(nil, "friend-quota-reactivation-proof-v1")
	for _, value := range []string{authorityID, reservationID, workflowID, relationID, itoa(targetEpoch), fields[prefix+"state"],
		invocationID, receipt.CandidateHash, hex.EncodeToString(payloadPart), resultHash} {
		proofInput = appendFriendLP(proofInput, value)
	}
	proof := sha256.Sum256(proofInput)
	return hex.EncodeToString(proof[:]), nil
}

func sameFriendRotateReceipt(left, right RotateReceipt) bool {
	return left.ReceiptID == right.ReceiptID && left.PairID == right.PairID &&
		left.TerminatedRelationEpoch == right.TerminatedRelationEpoch && left.BarrierRevision == right.BarrierRevision &&
		left.LowShareGenerationFloor == right.LowShareGenerationFloor && left.HighShareGenerationFloor == right.HighShareGenerationFloor &&
		left.TokenID == right.TokenID && left.TokenGeneration == right.TokenGeneration &&
		left.OperationID == right.OperationID && left.CommittedAtMs == right.CommittedAtMs &&
		strings.EqualFold(hex.EncodeToString(left.PayloadHash), hex.EncodeToString(right.PayloadHash))
}

func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func internalFriendIdentity(namespace string, fields ...string) (CommitIdentity, []byte, error) {
	invocation, err := stableID(namespace, fields...)
	if err != nil {
		return CommitIdentity{}, nil, err
	}
	identity := CommitIdentity{WorkAddressID: fields[0], ParticipantInvocationID: invocation, ParticipantStep: namespace}
	var payload []byte
	for _, field := range fields {
		payload = appendFriendLP(payload, field)
	}
	sum := sha256.Sum256(payload)
	return identity, sum[:], nil
}

func (r *RedisPairRepository) CommitReactivationGenerationFloors(ctx context.Context, playerA, playerB uint64, expectedDeletedEpoch, barrierRevision, lowFloor, highFloor uint64, nowMs int64) (Outcome, error) {
	pairID, _, _, err := pairIDForPlayers(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetPair(ctx, playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	identity, payload, err := internalFriendIdentity("COMMIT_REACTIVATION_FLOORS", pairID, itoa(expectedDeletedEpoch), itoa(barrierRevision), itoa(lowFloor), itoa(highFloor))
	if err != nil {
		return Outcome{}, err
	}
	candidateFields := []string{"FLOORS", itoa(expectedDeletedEpoch), itoa(barrierRevision), itoa(lowFloor), itoa(highFloor), "record_version", itoa(snapshot.RecordVersion)}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	value, err := r.authority.commit(ctx, FriendPairAuthority, pairID, identity, payload, candidateFields, scriptPairFloors, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			return []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, "REACTIVATION_FLOORS", ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority), r.authority.limits.receiptBytes(FriendPairAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion, group.ref.BucketID,
				pairID, r.authority.limits.recordBytes(FriendPairAuthority), pairID, expectedDeletedEpoch, barrierRevision, lowFloor, highFloor, snapshot.RecordVersion}
		})
	if err != nil {
		return Outcome{}, err
	}
	return outcomeFromReceipt(value, identity, payload), nil
}

func (r *RedisPairRepository) CompleteTerminationOrder(ctx context.Context, playerA, playerB uint64, orderID string, nowMs int64) (Outcome, error) {
	pairID, _, _, err := pairIDForPlayers(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetPair(ctx, playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	if workidentity.ValidateStableOpaqueID(orderID) != nil {
		return Outcome{}, fmt.Errorf("%w: order id", ErrInvalidCommand)
	}
	identity, payload, err := internalFriendIdentity("COMPLETE_TERMINATION", pairID, orderID)
	if err != nil {
		return Outcome{}, err
	}
	candidateFields := []string{"COMPLETE", orderID, "record_version", itoa(snapshot.RecordVersion)}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	value, err := r.authority.commit(ctx, FriendPairAuthority, pairID, identity, payload, candidateFields, scriptPairComplete, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			return []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, "COMPLETE_TERMINATION", ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority), r.authority.limits.receiptBytes(FriendPairAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion, group.ref.BucketID,
				pairID, r.authority.limits.recordBytes(FriendPairAuthority), pairID, orderID, snapshot.RecordVersion}
		})
	if err != nil {
		return Outcome{}, err
	}
	return outcomeFromReceipt(value, identity, payload), nil
}

// MarkTerminationQuotaReleased advances one bounded termination progress
// flag.  It is an internal orchestration seam used by DomainService; the
// frozen PairRepository remains unchanged while CompleteTerminationOrder
// verifies both flags before setting completed.
func (r *RedisPairRepository) MarkTerminationQuotaReleased(ctx context.Context, playerA, playerB uint64,
	orderID string, playerID uint64, nowMs int64) (Outcome, error) {
	pairID, _, _, err := pairIDForPlayers(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetPair(ctx, playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	if orderID == "" || (playerID != snapshot.LowPlayerID && playerID != snapshot.HighPlayerID) {
		return Outcome{}, fmt.Errorf("%w: termination progress", ErrInvalidCommand)
	}
	identity, payload, err := internalFriendIdentity("MARK_TERMINATION_QUOTA_RELEASED", pairID, orderID, itoa(playerID))
	if err != nil {
		return Outcome{}, err
	}
	candidateFields := []string{"QUOTA_RELEASED", orderID, itoa(playerID), "record_version", itoa(snapshot.RecordVersion)}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	value, err := r.authority.commit(ctx, FriendPairAuthority, pairID, identity, payload,
		candidateFields, scriptPairQuotaReleased, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			return []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, "PAIR_QUOTA_RELEASED", ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority), r.authority.limits.receiptBytes(FriendPairAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion,
				group.ref.BucketID, pairID, r.authority.limits.recordBytes(FriendPairAuthority), pairID, orderID, playerID, snapshot.RecordVersion}
		})
	if err != nil {
		return Outcome{}, err
	}
	return outcomeFromReceipt(value, identity, payload), nil
}

func (r *RedisPairRepository) GetBarrierProof(ctx context.Context, playerA, playerB uint64) (BarrierProof, error) {
	snapshot, err := r.GetPair(ctx, playerA, playerB)
	if err != nil {
		return BarrierProof{}, err
	}
	if snapshot.Barrier.State != BarrierReady {
		return BarrierProof{}, ErrBarrierProofMismatch
	}
	return BarrierProof{State: snapshot.Barrier.State, PairID: snapshot.PairID,
		TerminatedRelationEpoch:  snapshot.Barrier.TerminatedRelationEpoch,
		BarrierRevision:          snapshot.Barrier.BarrierRevision,
		LowShareGenerationFloor:  snapshot.Barrier.LowShareGenerationFloor,
		HighShareGenerationFloor: snapshot.Barrier.HighShareGenerationFloor}, nil
}

func (r *RedisPairRepository) CommitFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error) {
	return r.commitFriendActionAdmissionAttempt(ctx, cmd, 0)
}

func (r *RedisPairRepository) commitFriendActionAdmissionAttempt(ctx context.Context, cmd ActionAdmissionCommand, retry int) (ActionAdmissionOutcome, error) {
	if !ValidPair(cmd.VisitorPlayerID, cmd.OwnerPlayerID) || !cmd.Identity.IsPublic() ||
		cmd.TargetFarmID != cmd.OwnerPlayerID || (cmd.Scope != ActionScopeCare && cmd.Scope != ActionScopeTheft) || len(cmd.TargetBindingHash) == 0 {
		return ActionAdmissionOutcome{}, fmt.Errorf("%w: admission target", ErrInvalidCommand)
	}
	pairID, _, _, err := pairIDForPlayers(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	snapshot, err := r.GetPair(ctx, cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	operationDigest, err := workOperationDigest(cmd, pairID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	permitID, err := stableID("farm-action-permit-id-v1", pairID, itoa(cmd.VisitorPlayerID), cmd.Identity.OperationID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	ttl := cmd.PermitTTLMs
	if ttl <= 0 {
		ttl = DefaultActionPermitTTL.Milliseconds()
	}
	workflowTTL := cmd.WorkflowTTLMs
	if workflowTTL <= 0 {
		workflowTTL = DefaultActionWorkflowTTLMs
	}
	if workflowTTL <= ttl {
		return ActionAdmissionOutcome{}, fmt.Errorf("%w: workflow TTL must exceed permit TTL", ErrInvalidCommand)
	}
	dispatchID, err := ActionDispatchOrderID(pairID, operationDigest, snapshot.Relation.RelationEpoch)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	// The result tuple is first so a rejected admission (which has no
	// dispatch work) still persists exactly its ResultPublishOrder.  An
	// accepted admission persists both tuples in the same Pair group.
	resultOrder, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "ACTION_RESULT",
		pairID, operationDigest, "1", cmd.Identity.OperationID, dispatchID,
		itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID),
		hex.EncodeToString(cmd.PayloadHash), hex.EncodeToString(cmd.TargetBindingHash))
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	dispatchOrder, err := newFriendSourceOrder("FRIEND_ACTION_DISPATCH_ORDER", dispatchID, "ACTION_DISPATCH",
		pairID, operationDigest, itoa(snapshot.Relation.RelationEpoch), permitID,
		hex.EncodeToString(cmd.PayloadHash), hex.EncodeToString(cmd.TargetBindingHash),
		itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID),
		strconv.FormatUint(uint64(cmd.Scope), 10), cmd.Identity.OperationID, strconv.FormatInt(ttl, 10),
		strconv.FormatInt(workflowTTL, 10))
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	recordVersion := snapshot.RecordVersion
	candidateFields := []string{"ACTION_ADMISSION", itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID),
		strconv.FormatUint(uint64(cmd.Scope), 10), hex.EncodeToString(cmd.TargetBindingHash), hex.EncodeToString(cmd.PayloadHash),
		operationDigest, permitID, dispatchID, resultID, snapshot.Relation.RelationID, itoa(snapshot.Relation.RelationEpoch),
		strconv.FormatInt(ttl, 10), strconv.FormatInt(workflowTTL, 10),
		"record_version", itoa(recordVersion), "authority_time", "REDIS_SERVER_TIME"}
	for _, order := range []friendSourceOrder{resultOrder, dispatchOrder} {
		candidateFields = append(candidateFields, order.id, order.workType, order.blob, order.workRef, order.payloadHash, "0")
	}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, cmd.Identity, cmd.PayloadHash, candidateFields...)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	resultKey, err := r.authority.sourceOrderKey(group, resultOrder.workType, resultOrder.id)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	dispatchKey, err := r.authority.sourceOrderKey(group, dispatchOrder.workType, dispatchOrder.id)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.sourceDue, group.meta, resultKey, dispatchKey}
	if err := r.authority.assertSameSlot(keys...); err != nil {
		return ActionAdmissionOutcome{}, err
	}
	// Pair admission reads the current record version before the atomic CAS.
	// Another Pod may legitimately advance the same Pair before this attempt
	// runs. Reuse the guarded refresh path: it proves the old candidate against
	// the persisted cursor and permits only record_version to change. Payload,
	// target, permit/workflow TTLs, relation epoch and both order tuples stay immutable.
	value, err := r.authority.commitAllowRecordVersionRefresh(ctx, FriendPairAuthority, pairID, cmd.Identity, cmd.PayloadHash,
		candidateFields, scriptPairAction, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			args := []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, "ACTION_ADMISSION", ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendPairAuthority),
				r.authority.limits.receiptBytes(FriendPairAuthority), r.authority.limits.MaxPairPendingOrders, r.authority.limits.MaxPairPendingOrderBytes,
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, 2}
			args = append(args, friendOrderArgs([]friendSourceOrder{resultOrder, dispatchOrder})...)
			args = append(args, pairID, group.ref.BucketID, group.ref.LayoutVersion,
				cmd.VisitorPlayerID, cmd.OwnerPlayerID, cmd.TargetFarmID, uint64(cmd.Scope),
				hex.EncodeToString(cmd.TargetBindingHash), cmd.Identity.OperationID, operationDigest,
				permitID, snapshot.Relation.RelationID, cmd.Identity.OperationID,
				hex.EncodeToString(cmd.PayloadHash), hex.EncodeToString(cmd.TargetBindingHash), ttl, dispatchID,
				workflowTTL, recordVersion, r.authority.limits.recordBytes(FriendPairAuthority))
			return args
		})
	if err != nil {
		if errors.Is(err, ErrFriendRecordVersionConflict) && retry < 256 {
			return r.commitFriendActionAdmissionAttempt(ctx, cmd, retry+1)
		}
		return ActionAdmissionOutcome{}, err
	}
	outcome := outcomeFromReceipt(value, cmd.Identity, cmd.PayloadHash)
	if len(value.Typed) == 0 {
		return ActionAdmissionOutcome{Outcome: outcome}, nil
	}
	permit, returnedDispatch, workflowDeadline, err := parseFriendAction(value.Typed)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	return ActionAdmissionOutcome{Outcome: outcome, Permit: permit, DispatchOrderID: returnedDispatch,
		WorkflowDeadlineAtMs: workflowDeadline}, nil
}

func (r *RedisPairRepository) GetFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error) {
	if !ValidPair(cmd.VisitorPlayerID, cmd.OwnerPlayerID) || !cmd.Identity.IsPublic() {
		return ActionAdmissionOutcome{}, fmt.Errorf("%w: admission identity", ErrInvalidCommand)
	}
	pairID, _, _, err := pairIDForPlayers(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	ci, err := r.authority.commitIdentity(FriendPairAuthority, pairID, cmd.Identity, cmd.PayloadHash,
		"ACTION_ADMISSION", itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID), strconv.FormatUint(uint64(cmd.Scope), 10))
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	group, err := r.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	blob, err := r.authority.client.HGet(ctx, group.receipt, ci.field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// The DispatchOrder is the durable recovery anchor after the
			// AdmissionReceipt has been compacted.  The stable
			// ResultPublishOrder locator is independent of the accepted
			// relation epoch and carries the DispatchOrder id, so recovery
			// does not guess a historical epoch or scan source keys.
			operationDigest, digestErr := workOperationDigest(cmd, pairID)
			if digestErr != nil {
				return ActionAdmissionOutcome{}, digestErr
			}
			resultID, resultErr := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
			if resultErr != nil {
				return ActionAdmissionOutcome{}, resultErr
			}
			resultKey, keyErr := r.authority.sourceOrderKey(group, "RESULT_PUBLISH_ORDER", resultID)
			if keyErr != nil {
				return ActionAdmissionOutcome{}, keyErr
			}
			resultManifest := friendSourceOrderManifest(friendSourceOrderActionResult)
			resultFields, resultReadErr := r.authority.readFields(ctx, resultKey, resultManifest...)
			if resultReadErr != nil {
				return ActionAdmissionOutcome{}, resultReadErr
			}
			resultFieldCount, countErr := r.authority.client.HLen(ctx, resultKey).Result()
			if countErr != nil {
				return ActionAdmissionOutcome{}, countErr
			}
			dispatchID := resultFields["dispatch_order_id"]
			if resultFieldCount != int64(len(resultManifest)) || len(resultFields) != len(resultManifest) ||
				workidentity.ValidateStableOpaqueID(dispatchID) != nil {
				return ActionAdmissionOutcome{}, fmt.Errorf("%w: action result order manifest", ErrFriendReconcile)
			}
			resultOrder, orderErr := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "ACTION_RESULT",
				pairID, operationDigest, "1", cmd.Identity.OperationID, dispatchID,
				itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID),
				hex.EncodeToString(cmd.PayloadHash), hex.EncodeToString(cmd.TargetBindingHash))
			if orderErr != nil {
				return ActionAdmissionOutcome{}, orderErr
			}
			if _, validateErr := r.authority.readAndValidatePersistedSourceOrder(ctx, group, resultOrder,
				friendSourceOrderActionResult, map[string]string{"dispatch_order_id": dispatchID}); validateErr != nil {
				return ActionAdmissionOutcome{}, validateErr
			}
			dispatchKey, keyErr := r.authority.sourceOrderKey(group, "FRIEND_ACTION_DISPATCH_ORDER", dispatchID)
			if keyErr != nil {
				return ActionAdmissionOutcome{}, keyErr
			}
			dispatchManifest := friendSourceOrderManifest(friendSourceOrderDispatch)
			orderFields, fieldsErr := r.authority.readFields(ctx, dispatchKey, dispatchManifest...)
			if fieldsErr != nil {
				return ActionAdmissionOutcome{}, fieldsErr
			}
			dispatchFieldCount, countErr := r.authority.client.HLen(ctx, dispatchKey).Result()
			if countErr != nil {
				return ActionAdmissionOutcome{}, countErr
			}
			if dispatchFieldCount != int64(len(dispatchManifest)) || len(orderFields) != len(dispatchManifest) {
				return ActionAdmissionOutcome{}, fmt.Errorf("%w: action dispatch order manifest", ErrFriendReconcile)
			}
			permit, permitErr := actionPermitFromOrder(orderFields)
			if permitErr != nil {
				return ActionAdmissionOutcome{}, fmt.Errorf("%w: action dispatch permit: %v", ErrFriendReconcile, permitErr)
			}
			workflowDeadline, deadlineErr := parseCanonicalIntText(orderFields["workflow_deadline_at_ms"])
			if deadlineErr != nil || workflowDeadline <= permit.ExpireAtAuthorityMs {
				return ActionAdmissionOutcome{}, fmt.Errorf("%w: action workflow deadline", ErrFriendReconcile)
			}
			if validateErr := validateActionRecoveryRequest(ctx, r.authority, group, pairID, cmd, permit, dispatchID, workflowDeadline); validateErr != nil {
				return ActionAdmissionOutcome{}, validateErr
			}
			return ActionAdmissionOutcome{Outcome: Outcome{Status: StatusSucceeded, Identity: cmd.Identity,
				PayloadHash: append([]byte(nil), cmd.PayloadHash...), CommittedAtMs: permit.ExecutionAcceptedAtAuthorityMs,
				CommittedEpoch: permit.RelationEpoch}, Permit: permit, DispatchOrderID: dispatchID,
				WorkflowDeadlineAtMs: workflowDeadline}, nil
		}
		return ActionAdmissionOutcome{}, err
	}
	value, err := decodeFriendReceiptReplay(blob, ci)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	if value.Kind != "ACTION_ADMISSION" {
		return ActionAdmissionOutcome{}, ErrIdempotentConflict
	}
	outcome := outcomeFromReceipt(value, cmd.Identity, cmd.PayloadHash)
	if len(value.Typed) == 0 {
		return ActionAdmissionOutcome{}, ErrOutcomeNotFound
	}
	permit, dispatchID, workflowDeadline, err := parseFriendAction(value.Typed)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	if err := validateActionRecoveryRequest(ctx, r.authority, group, pairID, cmd, permit, dispatchID, workflowDeadline); err != nil {
		return ActionAdmissionOutcome{}, err
	}
	return ActionAdmissionOutcome{Outcome: outcome, Permit: permit, DispatchOrderID: dispatchID,
		WorkflowDeadlineAtMs: workflowDeadline}, nil
}

func decodeFriendReceiptReplay(blob string, ci friendCommitIdentity) (friendReceiptValue, error) {
	value, err := parseFriendReceipt([]byte(blob))
	if err != nil {
		return friendReceiptValue{}, err
	}
	if !friendReceiptPrefixMatches([]byte(blob), ci.prefix) {
		return friendReceiptValue{}, ErrIdempotentConflict
	}
	return value, nil
}

func validateActionRecoveryRequest(ctx context.Context, authority *redisFriendAuthority, group friendRedisGroup,
	pairID string, cmd ActionAdmissionCommand, permit ActionPermitSnapshot, dispatchID string, workflowDeadline int64) error {
	if permit.PermitID == "" || permit.RelationID != pairID || permit.OperationID != cmd.Identity.OperationID ||
		permit.VisitorPlayerID != cmd.VisitorPlayerID || permit.OwnerPlayerID != cmd.OwnerPlayerID ||
		permit.TargetFarmID != cmd.TargetFarmID || permit.Scope != cmd.Scope ||
		!bytes.Equal(permit.PayloadHash, cmd.PayloadHash) || !bytes.Equal(permit.TargetBindingHash, cmd.TargetBindingHash) {
		return ErrIdempotentConflict
	}
	if cmd.PermitTTLMs > 0 && permit.ExpireAtAuthorityMs-permit.IssuedAtAuthorityMs != cmd.PermitTTLMs {
		return ErrIdempotentConflict
	}
	workflowTTL := workflowDeadline - permit.IssuedAtAuthorityMs
	if workflowDeadline <= permit.ExpireAtAuthorityMs ||
		(cmd.WorkflowTTLMs > 0 && workflowTTL != cmd.WorkflowTTLMs) {
		return ErrIdempotentConflict
	}
	if err := VerifyAdmittedPermit(permit); err != nil {
		return fmt.Errorf("%w: permit recovery: %v", ErrFriendReceiptContract, err)
	}
	operationDigest, err := workOperationDigest(cmd, pairID)
	if err != nil {
		return err
	}
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		return err
	}
	resultOrder, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "ACTION_RESULT",
		pairID, operationDigest, "1", cmd.Identity.OperationID, dispatchID,
		itoa(cmd.VisitorPlayerID), itoa(cmd.OwnerPlayerID), itoa(cmd.TargetFarmID),
		hex.EncodeToString(cmd.PayloadHash), hex.EncodeToString(cmd.TargetBindingHash))
	if err != nil {
		return err
	}
	if err := authority.validatePersistedSourceOrder(ctx, group, resultOrder, map[string]string{
		"canonical_source_authority_id": pairID,
		"dispatch_order_id":             dispatchID,
	}); err != nil {
		return err
	}
	dispatchOrder, err := newFriendSourceOrder("FRIEND_ACTION_DISPATCH_ORDER", dispatchID, "ACTION_DISPATCH",
		pairID, operationDigest, itoa(permit.RelationEpoch), permit.PermitID,
		hex.EncodeToString(permit.PayloadHash), hex.EncodeToString(permit.TargetBindingHash),
		itoa(permit.VisitorPlayerID), itoa(permit.OwnerPlayerID), itoa(permit.TargetFarmID),
		strconv.FormatUint(uint64(permit.Scope), 10), permit.OperationID,
		strconv.FormatInt(permit.ExpireAtAuthorityMs-permit.IssuedAtAuthorityMs, 10), strconv.FormatInt(workflowTTL, 10))
	if err != nil {
		return err
	}
	return authority.validatePersistedSourceOrder(ctx, group, dispatchOrder, map[string]string{
		"canonical_source_authority_id":      pairID,
		"permit_id":                          permit.PermitID,
		"relation_id":                        permit.RelationID,
		"relation_epoch":                     itoa(permit.RelationEpoch),
		"visitor_player_id":                  itoa(permit.VisitorPlayerID),
		"owner_player_id":                    itoa(permit.OwnerPlayerID),
		"target_farm_id":                     itoa(permit.TargetFarmID),
		"operation_id":                       permit.OperationID,
		"scope":                              strconv.FormatUint(uint64(permit.Scope), 10),
		"permit_payload_hash":                hex.EncodeToString(permit.PayloadHash),
		"target_binding_hash":                hex.EncodeToString(permit.TargetBindingHash),
		"dispatch_order_id":                  dispatchID,
		"issued_at_authority_ms":             strconv.FormatInt(permit.IssuedAtAuthorityMs, 10),
		"expire_at_authority_ms":             strconv.FormatInt(permit.ExpireAtAuthorityMs, 10),
		"execution_accepted_at_authority_ms": strconv.FormatInt(permit.ExecutionAcceptedAtAuthorityMs, 10),
		"workflow_deadline_at_ms":            strconv.FormatInt(workflowDeadline, 10),
	})
}

func (a *redisFriendAuthority) validatePersistedSourceOrder(ctx context.Context, group friendRedisGroup,
	expected friendSourceOrder, extra map[string]string) error {
	shape := friendSourceOrderBase
	if expected.workType == "FRIEND_ACTION_DISPATCH_ORDER" {
		shape = friendSourceOrderDispatch
	} else if _, ok := extra["dispatch_order_id"]; ok {
		shape = friendSourceOrderActionResult
	}
	_, err := a.readAndValidatePersistedSourceOrder(ctx, group, expected, shape, extra)
	return err
}

func (a *redisFriendAuthority) readAndValidatePersistedSourceOrder(ctx context.Context, group friendRedisGroup,
	expected friendSourceOrder, shape friendSourceOrderShape, extra map[string]string) (map[string]string, error) {
	if err := validateFriendSourceOrderTuple(expected); err != nil {
		return nil, err
	}
	key, err := a.sourceOrderKey(group, expected.workType, expected.id)
	if err != nil {
		return nil, err
	}
	manifest := friendSourceOrderManifest(shape)
	fields, err := a.readFields(ctx, key, manifest...)
	if err != nil {
		return nil, err
	}
	fieldCount, err := a.client.HLen(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if fieldCount != int64(len(manifest)) || len(fields) != len(manifest) {
		return nil, fmt.Errorf("%w: source order manifest", ErrFriendReconcile)
	}
	expectedFields := map[string]string{
		"schema_version":        "friend-source-order-v1",
		"work_id":               expected.id,
		"work_type":             expected.workType,
		"placement_kind":        "SOURCE_PERSISTENCE_BUCKET",
		"bucket_version":        group.ref.LayoutVersion,
		"bucket_id":             itoa(uint64(group.ref.BucketID)),
		"source_authority_kind": "FRIEND_PAIR",
		"typed_payload":         expected.blob,
		"work_ref":              expected.workRef,
		"payload_hash":          expected.payloadHash,
		"next_due_at_ms":        expected.dueMS,
	}
	for field, value := range expectedFields {
		if fields[field] != value {
			return nil, fmt.Errorf("%w: source order field %s", ErrFriendReconcile, field)
		}
	}
	for field, value := range extra {
		if fields[field] != value {
			return nil, fmt.Errorf("%w: source order binding %s", ErrFriendReconcile, field)
		}
	}
	for _, field := range manifest {
		if _, ok := fields[field]; !ok {
			return nil, fmt.Errorf("%w: source order field %s", ErrFriendReconcile, field)
		}
	}
	for field, value := range map[string]string{"state": "PENDING", "work_version": "1", "retry_count": "0",
		"lease_epoch": "0", "lease_holder_id": "", "lease_token_hash": "", "lease_expire_at_ms": "0",
		"completed_at_ms": "0"} {
		if fields[field] != value {
			return nil, fmt.Errorf("%w: source order frozen field %s", ErrFriendReconcile, field)
		}
	}
	created, createdErr := strconv.ParseInt(fields["created_at_ms"], 10, 64)
	updated, updatedErr := strconv.ParseInt(fields["updated_at_ms"], 10, 64)
	if createdErr != nil || updatedErr != nil || created <= 0 || updated != created ||
		strconv.FormatInt(created, 10) != fields["created_at_ms"] || strconv.FormatInt(updated, 10) != fields["updated_at_ms"] {
		return nil, fmt.Errorf("%w: source order authority timestamps", ErrFriendReconcile)
	}
	ref, err := workidentity.DecodeWorkRefV1(fields["work_ref"])
	if err != nil || ref.WorkType != expected.workType || ref.WorkID != expected.id {
		return nil, fmt.Errorf("%w: source order work ref", ErrFriendReconcile)
	}
	if _, err := decodeFriendTypedPayload(fields["typed_payload"]); err != nil || fields["payload_hash"] != hexHash([]byte(fields["typed_payload"])) {
		return nil, fmt.Errorf("%w: source order payload", ErrFriendReconcile)
	}
	score, err := a.client.ZScore(ctx, group.sourceDue, expected.workRef).Result()
	expectedScore, parseErr := strconv.ParseInt(expected.dueMS, 10, 64)
	if err != nil || parseErr != nil || score != float64(expectedScore) || strconv.FormatInt(int64(score), 10) != expected.dueMS {
		return nil, fmt.Errorf("%w: source order due index", ErrFriendReconcile)
	}
	if friendSourceOrderLogicalBytes(expected) > a.limits.MaxSourceOrderBytes {
		return nil, ErrFriendAuthorityCapacity
	}
	return fields, nil
}

func actionPermitFromOrder(fields map[string]string) (ActionPermitSnapshot, error) {
	parseU := func(name string) (uint64, error) {
		value := fields[name]
		if value == "" {
			return 0, fmt.Errorf("%w: action order %s", ErrFriendReceiptContract, name)
		}
		return strconv.ParseUint(value, 10, 64)
	}
	parseI := func(name string) (int64, error) {
		value := fields[name]
		if value == "" {
			return 0, fmt.Errorf("%w: action order %s", ErrFriendReceiptContract, name)
		}
		return strconv.ParseInt(value, 10, 64)
	}
	visitor, err := parseU("visitor_player_id")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	owner, err := parseU("owner_player_id")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	targetFarm, err := parseU("target_farm_id")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	epoch, err := parseU("relation_epoch")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	scope, err := parseU("scope")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	issued, err := parseI("issued_at_authority_ms")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	expires, err := parseI("expire_at_authority_ms")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	accepted, err := parseI("execution_accepted_at_authority_ms")
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	payload, err := hex.DecodeString(fields["permit_payload_hash"])
	if err != nil {
		return ActionPermitSnapshot{}, fmt.Errorf("%w: action order payload", ErrFriendReceiptContract)
	}
	binding, err := hex.DecodeString(fields["target_binding_hash"])
	if err != nil {
		return ActionPermitSnapshot{}, fmt.Errorf("%w: action order binding", ErrFriendReceiptContract)
	}
	permit := ActionPermitSnapshot{PermitID: fields["permit_id"], RelationID: fields["relation_id"],
		RelationEpoch: epoch, VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: targetFarm,
		OperationID: fields["operation_id"], Scope: ActionScope(scope), PayloadHash: payload,
		TargetBindingHash: binding, IssuedAtAuthorityMs: issued, ExpireAtAuthorityMs: expires,
		ExecutionAcceptedAtAuthorityMs: accepted}
	if err := VerifyAdmittedPermit(permit); err != nil {
		return ActionPermitSnapshot{}, fmt.Errorf("%w: recovered permit: %v", ErrFriendReceiptContract, err)
	}
	return permit, nil
}

// --- FRIEND_QUOTA ---

type RedisQuotaRepository struct{ authority *redisFriendAuthority }

var _ QuotaRepository = (*RedisQuotaRepository)(nil)

func quotaAuthorityID(playerID uint64) (string, error) {
	if playerID == 0 {
		return "", fmt.Errorf("%w: player id", ErrInvalidCommand)
	}
	return itoa(playerID), nil
}

func (r *RedisQuotaRepository) GetQuota(ctx context.Context, playerID uint64) (QuotaSnapshot, error) {
	// HLEN/HKEYS/HMGET are individually bounded but not a Redis transaction.
	// Retry a changing manifest a fixed number of times; stable corruption
	// continues to fail closed after the bound, while legitimate concurrent
	// quota commits do not surface as false corruption.
	var lastErr error
	for attempt := 0; attempt < 32; attempt++ {
		snapshot, err := r.getQuotaOnce(ctx, playerID)
		if err == nil || !errors.Is(err, ErrFriendReconcile) {
			return snapshot, err
		}
		lastErr = err
	}
	return QuotaSnapshot{}, lastErr
}

func (r *RedisQuotaRepository) getQuotaOnce(ctx context.Context, playerID uint64) (QuotaSnapshot, error) {
	authorityID, err := quotaAuthorityID(playerID)
	if err != nil {
		return QuotaSnapshot{}, err
	}
	group, err := r.authority.group(FriendQuotaAuthority, authorityID, [32]byte{})
	if err != nil {
		return QuotaSnapshot{}, err
	}
	snapshot := QuotaSnapshot{PlayerID: playerID, ActiveRelations: make(map[string]uint64), EffectiveMaxFriends: r.authority.limits.MaxFriends}
	exists, err := r.authority.validateAuthorityEnvelope(ctx, group, FriendQuotaAuthority)
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if !exists {
		return snapshot, nil
	}
	fields, err := r.authority.readFields(ctx, group.record,
		"_meta:schema_version", "_meta:authority_layout_version", "_meta:record_version",
		"_meta:updated_at_ms", "_meta:logical_bytes", "_meta:business_state_digest",
		"_meta:active_count", "_meta:reserved_count", "_meta:effective_max_friends")
	if err != nil {
		return QuotaSnapshot{}, err
	}
	for _, field := range []string{"_meta:schema_version", "_meta:authority_layout_version", "_meta:record_version",
		"_meta:updated_at_ms", "_meta:logical_bytes", "_meta:business_state_digest", "_meta:active_count",
		"_meta:reserved_count", "_meta:effective_max_friends"} {
		if _, ok := fields[field]; !ok {
			return QuotaSnapshot{}, fmt.Errorf("%w: quota canonical field %s", ErrFriendReconcile, field)
		}
	}
	if fields["_meta:schema_version"] != "friend-quota-v1" ||
		fields["_meta:authority_layout_version"] != r.authority.keys.Profile().LayoutVersion ||
		!validFriendHex(fields["_meta:business_state_digest"], sha256.Size) {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota canonical schema", ErrFriendReconcile)
	}
	if _, err := parseCanonicalUintText(fields["_meta:updated_at_ms"]); err != nil {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota updated timestamp", ErrFriendReconcile)
	}
	activeCount, err := parseCanonicalUintText(fields["_meta:active_count"])
	if err != nil {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota active_count", ErrFriendReconcile)
	}
	reserved, err := parseCanonicalUintText(fields["_meta:reserved_count"])
	if err != nil {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota reserved_count", ErrFriendReconcile)
	}
	limit, err := parseCanonicalUintText(fields["_meta:effective_max_friends"])
	if err != nil || limit == 0 || limit > friendQuotaOccupancyHard {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota effective limit", ErrFriendReconcile)
	}
	if activeCount+reserved > friendQuotaOccupancyHard {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota occupancy", ErrFriendReconcile)
	}
	logicalBytes, err := parseCanonicalUintText(fields["_meta:logical_bytes"])
	if err != nil || logicalBytes > uint64(r.authority.limits.recordBytes(FriendQuotaAuthority)) {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota logical bytes", ErrFriendReconcile)
	}
	recordVersion, err := parseCanonicalUintText(fields["_meta:record_version"])
	if err != nil {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota record version", ErrFriendReconcile)
	}
	snapshot.RecordVersion = recordVersion
	snapshot.OpenReservations = int(reserved)
	snapshot.EffectiveMaxFriends = int(limit)
	// The canonical Quota HASH is physically bounded by the frozen occupancy
	// and logical-byte limits. Validate HLEN before enumerating its field names;
	// this preserves the frozen key layout without an unfrozen shadow index and
	// makes HKEYS a bounded manifest read rather than an unbounded hot-path scan.
	const quotaFixedFields = 9
	const quotaFieldsPerReservation = 6
	maxFields := int64(quotaFixedFields + friendQuotaOccupancyHard + friendQuotaOccupancyHard*quotaFieldsPerReservation)
	fieldCount, err := r.authority.client.HLen(ctx, group.record).Result()
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if fieldCount > maxFields {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota field manifest", ErrFriendReconcile)
	}
	names, err := r.authority.client.HKeys(ctx, group.record).Result()
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if int64(len(names)) != fieldCount {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota field manifest changed count=%d names=%d", ErrFriendReconcile, fieldCount, len(names))
	}
	allFields, err := r.authority.readFields(ctx, group.record, names...)
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if uint64(logicalBytes) != friendHashLogicalBytes(allFields) {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota logical byte mismatch", ErrFriendReconcile)
	}
	fixed := map[string]struct{}{
		"_meta:schema_version": {}, "_meta:authority_layout_version": {}, "_meta:record_version": {},
		"_meta:updated_at_ms": {}, "_meta:logical_bytes": {}, "_meta:business_state_digest": {},
		"_meta:active_count": {}, "_meta:reserved_count": {}, "_meta:effective_max_friends": {},
	}
	activeNames := make([]string, 0, activeCount)
	reservationIDs := make(map[string]struct{}, reserved)
	for _, name := range names {
		if _, ok := fixed[name]; ok {
			continue
		}
		if strings.HasPrefix(name, "active:") {
			activeNames = append(activeNames, name)
			continue
		}
		if strings.HasPrefix(name, "reservation_by_relation:") {
			continue
		}
		if strings.HasPrefix(name, "reservation:") {
			for _, suffix := range []string{":workflow_id", ":relation_id", ":target_epoch", ":state", ":created_at_ms"} {
				if strings.HasSuffix(name, suffix) {
					id := strings.TrimSuffix(strings.TrimPrefix(name, "reservation:"), suffix)
					if id == "" {
						return QuotaSnapshot{}, fmt.Errorf("%w: quota reservation id", ErrFriendReconcile)
					}
					reservationIDs[id] = struct{}{}
					goto recognizedQuotaField
				}
			}
		}
		return QuotaSnapshot{}, fmt.Errorf("%w: quota canonical field %s", ErrFriendReconcile, name)
	recognizedQuotaField:
	}
	if uint64(len(activeNames)) != activeCount {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota active count", ErrFriendReconcile)
	}
	for _, field := range activeNames {
		value := allFields[field]
		relationID := strings.TrimPrefix(field, "active:")
		epoch, parseErr := parseCanonicalUintText(value)
		if parseErr != nil || epoch == 0 || relationID == "" {
			return QuotaSnapshot{}, fmt.Errorf("%w: quota active relation", ErrFriendReconcile)
		}
		snapshot.ActiveRelations[relationID] = epoch
	}
	if uint64(len(reservationIDs)) != reserved {
		return QuotaSnapshot{}, fmt.Errorf("%w: quota reservation count", ErrFriendReconcile)
	}
	for id := range reservationIDs {
		prefix := "reservation:" + id + ":"
		workflowID := allFields[prefix+"workflow_id"]
		relationID := allFields[prefix+"relation_id"]
		targetEpoch := allFields[prefix+"target_epoch"]
		createdAt := allFields[prefix+"created_at_ms"]
		if workflowID == "" || relationID == "" || allFields[prefix+"state"] != "1" {
			return QuotaSnapshot{}, fmt.Errorf("%w: quota reservation fields", ErrFriendReconcile)
		}
		epoch, epochErr := parseCanonicalUintText(targetEpoch)
		created, createdErr := parseCanonicalIntText(createdAt)
		if epochErr != nil || epoch == 0 || createdErr != nil || created < 0 ||
			allFields["reservation_by_relation:"+relationID+":"+targetEpoch] != id {
			return QuotaSnapshot{}, fmt.Errorf("%w: quota reservation reverse", ErrFriendReconcile)
		}
	}
	return snapshot, nil
}

func (r *RedisQuotaRepository) mutateQuota(ctx context.Context, playerID uint64, identity CommitIdentity, payloadHash []byte,
	candidateFields []string, script string, operationArgs func(group friendRedisGroup, ci friendCommitIdentity) []interface{}) (Outcome, error) {
	return r.mutateQuotaAttempt(ctx, playerID, identity, payloadHash, candidateFields, script, operationArgs, 0)
}

func (r *RedisQuotaRepository) mutateQuotaAttempt(ctx context.Context, playerID uint64, identity CommitIdentity, payloadHash []byte,
	candidateFields []string, script string, operationArgs func(group friendRedisGroup, ci friendCommitIdentity) []interface{}, retry int) (Outcome, error) {
	authorityID, err := quotaAuthorityID(playerID)
	if err != nil {
		return Outcome{}, err
	}
	versionGroup, err := r.authority.group(FriendQuotaAuthority, authorityID, [32]byte{})
	if err != nil {
		return Outcome{}, err
	}
	recordVersionText, err := r.authority.client.HGet(ctx, versionGroup.record, "_meta:record_version").Result()
	if errors.Is(err, redis.Nil) {
		recordVersionText = "0"
	} else if err != nil {
		return Outcome{}, err
	}
	recordVersion, err := strconv.ParseUint(recordVersionText, 10, 64)
	if err != nil {
		return Outcome{}, fmt.Errorf("%w: quota record version", ErrFriendReceiptContract)
	}
	candidateFields = friendCandidateWithRecordVersion(candidateFields, recordVersion)
	ci, err := r.authority.commitIdentity(FriendQuotaAuthority, authorityID, identity, payloadHash, candidateFields...)
	if err != nil {
		return Outcome{}, err
	}
	group, err := r.authority.group(FriendQuotaAuthority, authorityID, ci.digest)
	if err != nil {
		return Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	// RelationReleaseFence is located by its own digest partition, not by the
	// participant receipt partition of the current operation.  It is appended
	// as the final KEYS entry so Finalize/Reserve/Remove can atomically consult the stable
	// fence hash while retaining their own Receipt partition in KEYS[2].
	if script == scriptQuotaReserve || script == scriptQuotaFinalize || script == scriptQuotaRemove {
		var fenceID string
		switch script {
		case scriptQuotaRemove:
			if len(candidateFields) >= 4 {
				fenceID = candidateFields[3]
			}
		default:
			if len(candidateFields) >= 5 {
				epoch, parseErr := strconv.ParseUint(candidateFields[4], 10, 64)
				if parseErr != nil {
					return Outcome{}, parseErr
				}
				fenceID, err = QuotaReleaseFenceID(playerID, candidateFields[3], epoch)
				if err != nil {
					return Outcome{}, err
				}
			}
		}
		if fenceID == "" {
			return Outcome{}, fmt.Errorf("%w: missing release fence", ErrInvalidCommand)
		}
		fenceDigest := friendDigestBytes(appendFriendLP(nil, fenceID))
		fenceKey, keyErr := r.authority.keys.Receipt(group.ref, authorityID, fenceDigest)
		if keyErr != nil {
			return Outcome{}, keyErr
		}
		keys = append(keys, string(fenceKey))
	}
	value, err := r.authority.commitAllowRecordVersionRefresh(ctx, FriendQuotaAuthority, authorityID, identity, payloadHash, candidateFields, script, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			args := []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, quotaReceiptKind(script), ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendQuotaAuthority), r.authority.limits.receiptBytes(FriendQuotaAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion,
				group.ref.BucketID, authorityID, r.authority.limits.recordBytes(FriendQuotaAuthority)}
			operation := operationArgs(group, commit)
			if len(operation) > 0 {
				// Every quota Lua operation ends with the CAS record version.
				operation[len(operation)-1] = recordVersion
			}
			return append(args, operation...)
		})
	if err != nil {
		if errors.Is(err, ErrFriendRecordVersionConflict) && retry < 256 {
			return r.mutateQuotaAttempt(ctx, playerID, identity, payloadHash, candidateFields, script, operationArgs, retry+1)
		}
		return Outcome{}, err
	}
	return outcomeFromReceipt(value, identity, payloadHash), nil
}

func quotaReceiptKind(script string) string {
	switch script {
	case scriptQuotaReserve:
		return "QUOTA_RESERVE"
	case scriptQuotaFinalize:
		return "QUOTA_FINALIZE"
	case scriptQuotaRelease:
		return "QUOTA_RELEASE"
	default:
		return "QUOTA_REMOVE"
	}
}

func (r *RedisQuotaRepository) ReserveFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation,
	identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	if reservation.ReservationID == "" || reservation.WorkflowID == "" || reservation.RelationID == "" || reservation.TargetRelationEpoch == 0 {
		return Outcome{}, fmt.Errorf("%w: reservation fields", ErrInvalidCommand)
	}
	fenceID, err := QuotaReleaseFenceID(playerID, reservation.RelationID, reservation.TargetRelationEpoch)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetQuota(ctx, playerID)
	if err != nil {
		return Outcome{}, err
	}
	fields := []string{"RESERVE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, itoa(reservation.TargetRelationEpoch),
		"record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	return r.mutateQuota(ctx, playerID, identity, payloadHash, fields, scriptQuotaReserve,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{r.authority.limits.MaxFriends, reservation.ReservationID, reservation.WorkflowID,
				reservation.RelationID, reservation.TargetRelationEpoch, friendFenceDigestHex(fenceID), snapshot.RecordVersion}
		})
}

func (r *RedisQuotaRepository) FinalizeFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation,
	identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	if reservation.ReservationID == "" || reservation.WorkflowID == "" || reservation.RelationID == "" || reservation.TargetRelationEpoch == 0 {
		return Outcome{}, fmt.Errorf("%w: reservation fields", ErrInvalidCommand)
	}
	fenceID, err := QuotaReleaseFenceID(playerID, reservation.RelationID, reservation.TargetRelationEpoch)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetQuota(ctx, playerID)
	if err != nil {
		return Outcome{}, err
	}
	fields := []string{"FINALIZE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, itoa(reservation.TargetRelationEpoch),
		"record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	return r.mutateQuota(ctx, playerID, identity, payloadHash, fields, scriptQuotaFinalize,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, reservation.TargetRelationEpoch,
				friendFenceDigestHex(fenceID), snapshot.RecordVersion}
		})
}

func (r *RedisQuotaRepository) ReleaseFriendReservation(ctx context.Context, playerID uint64, reservationID string,
	identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	if reservationID == "" {
		return Outcome{}, fmt.Errorf("%w: reservation id", ErrInvalidCommand)
	}
	snapshot, err := r.GetQuota(ctx, playerID)
	if err != nil {
		return Outcome{}, err
	}
	fields := []string{"RELEASE", reservationID, "record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	return r.mutateQuota(ctx, playerID, identity, payloadHash, fields, scriptQuotaRelease,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{reservationID, snapshot.RecordVersion}
		})
}

func (r *RedisQuotaRepository) RemoveActiveRelationIfEpochMatches(ctx context.Context, playerID uint64, relationID string,
	capturedActiveEpoch uint64, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	if relationID == "" || capturedActiveEpoch == 0 {
		return Outcome{}, fmt.Errorf("%w: release fence", ErrInvalidCommand)
	}
	fenceID, err := QuotaReleaseFenceID(playerID, relationID, capturedActiveEpoch)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := r.GetQuota(ctx, playerID)
	if err != nil {
		return Outcome{}, err
	}
	terminationOrderID := identity.WorkAddressID
	if terminationOrderID == "" {
		terminationOrderID = identity.OperationID
	}
	participantInvocationID := identity.ParticipantInvocationID
	if participantInvocationID == "" {
		participantInvocationID = identity.OperationID
	}
	fields := []string{"REMOVE", relationID, itoa(capturedActiveEpoch), fenceID, terminationOrderID, participantInvocationID,
		"max_friends", itoa(uint64(r.authority.limits.MaxFriends)),
		"record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	return r.mutateQuota(ctx, playerID, identity, payloadHash, fields, scriptQuotaRemove,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{relationID, capturedActiveEpoch, friendFenceDigestHex(fenceID), terminationOrderID,
				participantInvocationID, r.authority.limits.MaxFriends, snapshot.RecordVersion}
		})
}

// --- FRIEND_SHARE_ISSUER ---

type RedisShareIssuerRepository struct{ authority *redisFriendAuthority }

var _ ShareIssuerRepository = (*RedisShareIssuerRepository)(nil)

func shareAuthorityID(inviter uint64) (string, error) {
	if inviter == 0 {
		return "", fmt.Errorf("%w: inviter id", ErrInvalidCommand)
	}
	return itoa(inviter), nil
}

func shareTokenFromFields(fields map[string]string) (ShareToken, error) {
	generation, err := parseUintField(fields, "token:current_generation")
	if err != nil {
		return ShareToken{}, err
	}
	issued, err := parseIntField(fields, "token:issued_at_ms")
	if err != nil {
		return ShareToken{}, err
	}
	expires, err := parseIntField(fields, "token:expires_at_ms")
	if err != nil {
		return ShareToken{}, err
	}
	status, err := parseUintField(fields, "token:status")
	if err != nil {
		return ShareToken{}, err
	}
	purpose, err := parseUintField(fields, "token:purpose")
	if err != nil {
		return ShareToken{}, err
	}
	format, err := parseUintField(fields, "token:format_version")
	if err != nil {
		return ShareToken{}, err
	}
	return ShareToken{TokenID: fields["token:current_token_id"], TokenGeneration: generation, IssuedAtMs: issued,
		ExpiresAtMs: expires, Status: TokenStatus(status), Purpose: SharePurpose(purpose), FormatVersion: uint32(format)}, nil
}

func (r *RedisShareIssuerRepository) GetIssuer(ctx context.Context, inviter uint64) (ShareIssuerSnapshot, error) {
	authorityID, err := shareAuthorityID(inviter)
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	group, err := r.authority.group(FriendShareIssuerAuthority, authorityID, [32]byte{})
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	snapshot := ShareIssuerSnapshot{InviterPlayerID: inviter, RotateReceipts: make(map[string]RotateReceipt)}
	exists, err := r.authority.validateAuthorityEnvelope(ctx, group, FriendShareIssuerAuthority)
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	if !exists {
		return snapshot, nil
	}
	recordFields, err := r.authority.readFields(ctx, group.record,
		"issuer:player_id", "token:current_generation", "token:current_token_id", "token:status", "token:purpose",
		"token:format_version", "token:key_id", "token:issued_at_ms", "token:expires_at_ms", "token:digest",
		"token:envelope", "token:last_operation_id", "_meta:schema_version", "_meta:authority_layout_version",
		"_meta:record_version", "_meta:updated_at_ms", "_meta:logical_bytes", "_meta:business_state_digest")
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	for _, field := range []string{"issuer:player_id", "token:current_generation", "token:current_token_id", "token:status", "token:purpose",
		"token:format_version", "token:key_id", "token:issued_at_ms", "token:expires_at_ms", "token:digest", "token:envelope",
		"token:last_operation_id", "_meta:schema_version", "_meta:authority_layout_version", "_meta:record_version",
		"_meta:updated_at_ms", "_meta:logical_bytes", "_meta:business_state_digest"} {
		if _, ok := recordFields[field]; !ok {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: share canonical field %s", ErrFriendReconcile, field)
		}
	}
	if recordFields["_meta:schema_version"] != "friend-share-issuer-v1" ||
		recordFields["_meta:authority_layout_version"] != r.authority.keys.Profile().LayoutVersion ||
		recordFields["issuer:player_id"] != authorityID || !validFriendHex(recordFields["token:digest"], sha256.Size) ||
		!validFriendHex(recordFields["_meta:business_state_digest"], sha256.Size) {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share canonical schema", ErrFriendReconcile)
	}
	fieldCount, err := r.authority.client.HLen(ctx, group.record).Result()
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	if fieldCount != int64(len(recordFields)) {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share canonical manifest", ErrFriendReconcile)
	}
	logicalBytes, logicalErr := parseCanonicalUintText(recordFields["_meta:logical_bytes"])
	if logicalErr != nil || logicalBytes > uint64(r.authority.limits.recordBytes(FriendShareIssuerAuthority)) ||
		logicalBytes != friendHashLogicalBytes(recordFields) {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share logical bytes", ErrFriendReconcile)
	}
	if _, parseErr := parseCanonicalUintText(recordFields["_meta:updated_at_ms"]); parseErr != nil {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share updated timestamp", ErrFriendReconcile)
	}
	snapshot.Current, err = shareTokenFromFields(recordFields)
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	if snapshot.Current.Status > TokenStatusRevoked || snapshot.Current.Purpose > SharePurposeAddFriend || snapshot.Current.FormatVersion > 1 {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share token state", ErrFriendReconcile)
	}
	if snapshot.Current.Status == TokenStatusActive &&
		(recordFields["token:current_token_id"] == "" || recordFields["token:key_id"] == "" || recordFields["token:envelope"] == "") {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share active token fields", ErrFriendReconcile)
	}
	// Reactivation receipts are discovered through the bounded ReceiptDueIndex;
	// no partition enumeration or HKEYS read is part of the adapter hot path.
	dueCount, err := r.authority.client.ZCard(ctx, group.receiptDue).Result()
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	if dueCount > int64(r.authority.limits.receiptCount(FriendShareIssuerAuthority)) {
		return ShareIssuerSnapshot{}, fmt.Errorf("%w: share receipt discovery capacity", ErrFriendReconcile)
	}
	dueEntries, err := r.authority.client.ZRangeWithScores(ctx, group.receiptDue, 0, dueCount-1).Result()
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	for _, entry := range dueEntries {
		member, ok := entry.Member.(string)
		if !ok {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: share receipt due member", ErrFriendReconcile)
		}
		kind, dueAuthority, field, decodeErr := decodeFriendReceiptDueMember(member)
		if decodeErr != nil || kind != FriendShareIssuerAuthority {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: share receipt due locator", ErrFriendReconcile)
		}
		// ReceiptDueIndex is bucket-scoped. Entries for another valid Share
		// issuer in the same bucket are expected and must not be interpreted as
		// corruption of the requested authority.
		if dueAuthority != authorityID {
			continue
		}
		digestHex := strings.TrimPrefix(field, "op:")
		if digestHex == field || len(digestHex) != sha256.Size*2 || strings.ToLower(digestHex) != digestHex {
			// Participant receipts are not rotate proofs, but their due entries
			// still have to be well formed and bounded.
			if !strings.HasPrefix(field, "participant:") {
				return ShareIssuerSnapshot{}, fmt.Errorf("%w: share receipt locator", ErrFriendReconcile)
			}
			digestHex = strings.TrimPrefix(field, "participant:")
		}
		digestBytes, decodeErr := hex.DecodeString(digestHex)
		if decodeErr != nil || len(digestBytes) != sha256.Size {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: share receipt digest", ErrFriendReconcile)
		}
		var digest [sha256.Size]byte
		copy(digest[:], digestBytes)
		receiptKey, keyErr := r.authority.keys.Receipt(group.ref, authorityID, digest)
		if keyErr != nil {
			return ShareIssuerSnapshot{}, keyErr
		}
		value, readErr := r.authority.client.HGet(ctx, string(receiptKey), field).Result()
		if errors.Is(readErr, redis.Nil) {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: share due receipt missing", ErrFriendReconcile)
		}
		if readErr != nil {
			return ShareIssuerSnapshot{}, readErr
		}
		outer, parseErr := parseFriendReceipt([]byte(value))
		if parseErr != nil {
			return ShareIssuerSnapshot{}, parseErr
		}
		if outer.Kind != "SHARE_REACTIVATE" || outer.Status != StatusSucceeded || len(outer.Typed) == 0 {
			continue
		}
		parsed, parseErr := parseFriendRotate(outer.Typed)
		if parseErr != nil {
			return ShareIssuerSnapshot{}, parseErr
		}
		if parsed.PairID == "" || parsed.TokenID == "" || parsed.ReceiptID == "" {
			return ShareIssuerSnapshot{}, fmt.Errorf("%w: rotate receipt identity", ErrFriendReceiptContract)
		}
		if previous, exists := snapshot.RotateReceipts[parsed.PairID]; !exists || parsed.BarrierRevision > previous.BarrierRevision ||
			(parsed.BarrierRevision == previous.BarrierRevision && parsed.CommittedAtMs > previous.CommittedAtMs) {
			snapshot.RotateReceipts[parsed.PairID] = parsed
		}
	}
	snapshot.RecordVersion, err = parseUintField(recordFields, "_meta:record_version")
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	return snapshot, nil
}

func (r *RedisShareIssuerRepository) GetGenerationSnapshot(ctx context.Context, inviter uint64) (uint64, error) {
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return 0, err
	}
	return snapshot.Current.TokenGeneration, nil
}

func shareReceiptKind(script string) string {
	switch script {
	case scriptShareGenerate:
		return "SHARE_GENERATE"
	case scriptShareRotate:
		return "SHARE_ROTATE"
	case scriptShareReactivate:
		return "SHARE_REACTIVATE"
	case scriptShareRevoke:
		return "SHARE_REVOKE"
	default:
		return "SHARE_ACCEPT"
	}
}

func (r *RedisShareIssuerRepository) mutateShare(ctx context.Context, inviter uint64, identity CommitIdentity, payloadHash []byte,
	candidateFields []string, script string, operationArgs func(group friendRedisGroup, ci friendCommitIdentity) []interface{}) (friendReceiptValue, Outcome, error) {
	authorityID, err := shareAuthorityID(inviter)
	if err != nil {
		return friendReceiptValue{}, Outcome{}, err
	}
	ci, err := r.authority.commitIdentity(FriendShareIssuerAuthority, authorityID, identity, payloadHash, candidateFields...)
	if err != nil {
		return friendReceiptValue{}, Outcome{}, err
	}
	group, err := r.authority.group(FriendShareIssuerAuthority, authorityID, ci.digest)
	if err != nil {
		return friendReceiptValue{}, Outcome{}, err
	}
	keys := []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}
	value, err := r.authority.commit(ctx, FriendShareIssuerAuthority, authorityID, identity, payloadHash, candidateFields, script, keys,
		func(commit friendCommitIdentity, ordinal uint64, attemptID, attemptDue, receiptDue string) []interface{} {
			args := []interface{}{group.tag, commit.digestHex, commit.prefix, commit.field, shareReceiptKind(script), ordinal, attemptID,
				commit.candidateHash, attemptDue, receiptDue, r.authority.limits.receiptCount(FriendShareIssuerAuthority), r.authority.limits.receiptBytes(FriendShareIssuerAuthority),
				r.authority.limits.AttemptRetentionMs, r.authority.limits.ReceiptRetentionMs, group.ref.LayoutVersion,
				group.ref.BucketID, authorityID, r.authority.limits.recordBytes(FriendShareIssuerAuthority)}
			return append(args, operationArgs(group, commit)...)
		})
	if err != nil {
		return friendReceiptValue{}, Outcome{}, err
	}
	return value, outcomeFromReceipt(value, identity, payloadHash), nil
}

func (r *RedisShareIssuerRepository) GenerateShareToken(ctx context.Context, inviter uint64, newTokenID string,
	identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	if newTokenID == "" {
		return ShareToken{}, Outcome{}, fmt.Errorf("%w: token id", ErrInvalidCommand)
	}
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	fields := []string{"GENERATE", newTokenID, "record_version", itoa(snapshot.RecordVersion),
		"ttl_ms", strconv.FormatInt(r.authority.limits.ShareTokenTTLMs, 10), "authority_time", "REDIS_SERVER_TIME"}
	value, outcome, err := r.mutateShare(ctx, inviter, identity, payloadHash, fields, scriptShareGenerate,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{newTokenID, r.authority.limits.ShareTokenTTLMs, hexHash([]byte(newTokenID)),
				newTokenID, "friend-adapter-v1", snapshot.RecordVersion}
		})
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	token, err := parseFriendToken(value.Typed)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	return token, outcome, nil
}

func (r *RedisShareIssuerRepository) RotateShareToken(ctx context.Context, inviter uint64, newTokenID string,
	identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	if newTokenID == "" {
		return ShareToken{}, Outcome{}, fmt.Errorf("%w: token id", ErrInvalidCommand)
	}
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	fields := []string{"ROTATE", newTokenID, "record_version", itoa(snapshot.RecordVersion),
		"ttl_ms", strconv.FormatInt(r.authority.limits.ShareTokenTTLMs, 10), "authority_time", "REDIS_SERVER_TIME"}
	value, outcome, err := r.mutateShare(ctx, inviter, identity, payloadHash, fields, scriptShareRotate,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{newTokenID, r.authority.limits.ShareTokenTTLMs, hexHash([]byte(newTokenID)),
				newTokenID, "friend-adapter-v1", snapshot.RecordVersion}
		})
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	token, err := parseFriendToken(value.Typed)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	return token, outcome, nil
}

func (r *RedisShareIssuerRepository) RotateForPairReactivation(ctx context.Context, inviter uint64, proof BarrierProof,
	newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, RotateReceipt, Outcome, error) {
	if proof.State != BarrierReady || proof.PairID == "" || proof.TerminatedRelationEpoch == 0 || proof.BarrierRevision == 0 || newTokenID == "" {
		return ShareToken{}, RotateReceipt{}, Outcome{}, ErrBarrierProofMismatch
	}
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	receiptID, err := ReactivationRotateReceiptID(proof.PairID, proof.BarrierRevision, newTokenID)
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	fields := []string{"REACTIVATE", proof.PairID, itoa(proof.BarrierRevision), newTokenID,
		itoa(proof.TerminatedRelationEpoch), itoa(proof.LowShareGenerationFloor), itoa(proof.HighShareGenerationFloor),
		"record_version", itoa(snapshot.RecordVersion), "operation_id", identity.OperationID,
		hex.EncodeToString(payloadHash)}
	value, outcome, err := r.mutateShare(ctx, inviter, identity, payloadHash,
		fields, scriptShareReactivate,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{newTokenID, r.authority.limits.ShareTokenTTLMs, proof.PairID,
				proof.TerminatedRelationEpoch, proof.BarrierRevision, proof.LowShareGenerationFloor,
				proof.HighShareGenerationFloor, receiptID, hexHash([]byte(newTokenID)), newTokenID,
				"friend-adapter-v1", snapshot.RecordVersion, identity.OperationID, hex.EncodeToString(payloadHash)}
		})
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	decoded, err := decodeFriendTyped(value.Typed)
	if err != nil || len(decoded) != 18 {
		return ShareToken{}, RotateReceipt{}, Outcome{}, fmt.Errorf("%w: reactivation receipt", ErrFriendReceiptContract)
	}
	token, err := parseFriendToken(append([]string{"RAW1"}, decoded[:7]...))
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	receipt, err := parseFriendRotate(append([]string{"RAW1"}, decoded[7:]...))
	if err != nil {
		return ShareToken{}, RotateReceipt{}, Outcome{}, err
	}
	return token, receipt, outcome, nil
}

func (r *RedisShareIssuerRepository) RevokeShareToken(ctx context.Context, inviter uint64, identity CommitIdentity,
	payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	fields := []string{"REVOKE", "record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	value, outcome, err := r.mutateShare(ctx, inviter, identity, payloadHash, fields, scriptShareRevoke,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{snapshot.RecordVersion}
		})
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	token, err := parseFriendToken(value.Typed)
	if err != nil {
		return ShareToken{}, Outcome{}, err
	}
	return token, outcome, nil
}

func (r *RedisShareIssuerRepository) AcceptShareTokenAdmission(ctx context.Context, inviter uint64, tokenID string,
	tokenGeneration uint64, redeemer uint64, workflowID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ValidationReceipt, Outcome, error) {
	if inviter == 0 || redeemer == 0 || tokenID == "" || workflowID == "" {
		return ValidationReceipt{}, Outcome{}, fmt.Errorf("%w: admission fields", ErrInvalidCommand)
	}
	receiptID, err := ShareTokenValidationReceiptID(workflowID, inviter, redeemer, tokenGeneration)
	if err != nil {
		return ValidationReceipt{}, Outcome{}, err
	}
	snapshot, err := r.GetIssuer(ctx, inviter)
	if err != nil {
		return ValidationReceipt{}, Outcome{}, err
	}
	fields := []string{"ACCEPT", tokenID, itoa(tokenGeneration), itoa(redeemer), workflowID,
		"record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	value, outcome, err := r.mutateShare(ctx, inviter, identity, payloadHash,
		fields, scriptShareAccept,
		func(group friendRedisGroup, ci friendCommitIdentity) []interface{} {
			return []interface{}{tokenID, tokenGeneration, redeemer, workflowID, uint64(1), receiptID}
		})
	if err != nil {
		return ValidationReceipt{}, Outcome{}, err
	}
	if outcome.Status != StatusSucceeded || len(value.Typed) == 0 {
		return ValidationReceipt{}, outcome, nil
	}
	receipt, err := parseFriendValidation(value.Typed)
	if err != nil {
		return ValidationReceipt{}, Outcome{}, err
	}
	return receipt, outcome, nil
}
