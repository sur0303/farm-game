package carerecovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

// SharedDueIndexCache is retained as a fail-closed compatibility surface.
//
// A physical page can contain refs for all six SourceOrder families. Safely
// sharing that page requires independent per-family consumption positions,
// exact Redis client/namespace/index identity, bounded retained bytes with an
// eviction protocol for lagging consumers, and a cache-wide command budget.
// The previous implementation provided none of those properties and could
// skip refs when one family advanced a page needed by another. Production does
// not construct this cache. Until a separately reviewed implementation can
// satisfy every invariant, all reads through this type fail unavailable and
// it retains no entries, bytes, trusted clock, cursor, or in-flight command.
type SharedDueIndexCache struct {
	ttl        time.Duration
	fetchLimit int
	now        func() time.Time
}

// SharedDueIndexCacheStats is a process-local monotonic snapshot. A disabled
// cache always reports zero commands and zero retained entries/bytes.
type SharedDueIndexCacheStats struct {
	Hits, Misses, Waits uint64
	Entries             int
	Bytes               int64
}

func NewSharedDueIndexCache(ttl time.Duration, fetchLimit int, now func() time.Time) *SharedDueIndexCache {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if fetchLimit <= 0 || fetchLimit > 4096 {
		fetchLimit = 4096
	}
	if now == nil {
		now = time.Now
	}
	return &SharedDueIndexCache{ttl: ttl, fetchLimit: fetchLimit, now: now}
}

// SharedFamilyDueIndexScanner supplies both recovery discovery and readiness
// observation while preserving the historical composition interface.
type SharedFamilyDueIndexScanner interface {
	DueIndexScanner
	DueIndexObserver
	BucketDueIndexObserver
}

// ForFamily returns a fail-closed scanner. In particular, a legacy
// decoded-ref-only upstream can never turn a bounded empty prefix into a proof
// of family absence.
func (c *SharedDueIndexCache) ForFamily(family string, upstream DueIndexScanner) SharedFamilyDueIndexScanner {
	family = strings.TrimSpace(family)
	if family == "" {
		family = FamilyCareReward
	}
	return &disabledFamilyDueIndexScanner{cache: c, upstream: upstream, family: family}
}

func (c *SharedDueIndexCache) Stats() SharedDueIndexCacheStats {
	return SharedDueIndexCacheStats{}
}

type disabledFamilyDueIndexScanner struct {
	cache    *SharedDueIndexCache
	upstream DueIndexScanner
	family   string
}

func (s *disabledFamilyDueIndexScanner) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	return s.unavailable(bucket, limit)
}

func (s *disabledFamilyDueIndexScanner) ListDueCareWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	return s.unavailable(bucket, limit)
}

func (s *disabledFamilyDueIndexScanner) unavailable(bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	if s == nil || s.cache == nil || s.upstream == nil || strings.TrimSpace(s.family) == "" || limit <= 0 || limit > 4096 {
		return nil, fmt.Errorf("%w: disabled shared DueIndex cache", ErrInvalidConfig)
	}
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: shared DueIndex cache disabled for family %s", ErrDueIndexFamilyUnavailable, s.family)
}

func (s *disabledFamilyDueIndexScanner) ObserveDue(context.Context, int) (DueIndexObservation, error) {
	return DueIndexObservation{}, fmt.Errorf("%w: shared DueIndex cache disabled", ErrDueIndexFamilyUnavailable)
}

func (s *disabledFamilyDueIndexScanner) ObserveBuckets(_ context.Context, buckets []rediskey.AuthorityBucketRef, limit int) (DueIndexObservation, error) {
	if s == nil || s.cache == nil || s.upstream == nil || len(buckets) == 0 || limit <= 0 || limit > 4096 {
		return DueIndexObservation{}, fmt.Errorf("%w: disabled shared DueIndex observation", ErrInvalidConfig)
	}
	for _, bucket := range buckets {
		if err := bucket.Validate(); err != nil {
			return DueIndexObservation{}, err
		}
	}
	return DueIndexObservation{}, fmt.Errorf("%w: shared DueIndex cache disabled", ErrDueIndexFamilyUnavailable)
}
