package theftrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// PairSourceOrderLoader is the authority-owned typed read for one source
// order. It must validate the exact Pair source-order manifest before
// returning DispatchOrder; the discovery adapter only supplies the bounded
// bucket/member locator.
type PairSourceOrderLoader func(ctx context.Context, bucketID uint32, workType, workID string) (DispatchOrder, error)

type RedisPairDiscoveryConfig struct {
	Client       redis.UniversalClient
	BucketCount  uint32
	DueIndexKey  func(bucketID uint32) (string, error)
	LoadDispatch PairSourceOrderLoader
	MaxBuckets   int
}

const (
	defaultPairBucketBudget = 64
	maxPairBucketBudget     = 256
)

// RedisPairDispatchRepository discovers only from the Pair Authority
// SourceDueIndex. It advances only a bucket cursor and always reads the due
// head at rank zero. A short fingerprint of a full head page lets the next
// round distinguish "the claimed prefix moved" from "the same malformed or
// held prefix is still present" without persisting a rank into a mutable ZSET.
type RedisPairDispatchRepository struct {
	client      redis.UniversalClient
	bucketCount uint32
	dueKey      func(uint32) (string, error)
	load        PairSourceOrderLoader
	maxBuckets  int
}

func NewRedisPairDispatchRepository(config RedisPairDiscoveryConfig) (*RedisPairDispatchRepository, error) {
	if config.Client == nil || config.BucketCount == 0 || config.DueIndexKey == nil || config.LoadDispatch == nil {
		return nil, fmt.Errorf("%w: Redis Pair discovery requires client, buckets, DueIndex and loader", ErrInvalidConfig)
	}
	if config.MaxBuckets <= 0 {
		config.MaxBuckets = defaultPairBucketBudget
	}
	if config.MaxBuckets > maxPairBucketBudget {
		config.MaxBuckets = maxPairBucketBudget
	}
	return &RedisPairDispatchRepository{client: config.Client, bucketCount: config.BucketCount,
		dueKey: config.DueIndexKey, load: config.LoadDispatch, maxBuckets: config.MaxBuckets}, nil
}

func (r *RedisPairDispatchRepository) Discover(ctx context.Context, request DiscoveryRequest) (DiscoveryPage, error) {
	if r == nil || r.client == nil || r.dueKey == nil || r.load == nil {
		return DiscoveryPage{}, fmt.Errorf("%w: nil Redis Pair discovery", ErrInvalidConfig)
	}
	if request.Limit <= 0 || request.Limit > 1024 {
		return DiscoveryPage{}, fmt.Errorf("%w: discovery limit", ErrInvalidConfig)
	}
	now, err := r.client.Time(ctx).Result()
	if err != nil {
		return DiscoveryPage{}, err
	}
	bucket, err := discoveryBucket(request.Cursor.Token)
	if err != nil {
		return DiscoveryPage{}, err
	}
	if bucket >= r.bucketCount {
		return DiscoveryPage{Exhausted: true}, nil
	}
	page := DiscoveryPage{Candidates: make([]DispatchCandidate, 0, request.Limit)}
	visited := 0
	for visited < r.maxBuckets && bucket < r.bucketCount && len(page.Candidates) < request.Limit {
		// Pair has thousands of physical buckets.  Issuing one ZRANGE per
		// bucket serially made a nominal 100ms poll spend ~145ms just waiting
		// on Redis round trips, turning a complete sweep into a multi-second
		// latency path.  Pipeline this bounded page instead.  The pipeline is
		// only a discovery hint: every candidate is still reloaded, validated,
		// and claimed through the existing WorkLease fence below.
		batch := make([]struct {
			bucket  uint32
			entries *redis.ZSliceCmd
		}, 0, r.maxBuckets)
		pipe := r.client.Pipeline()
		remaining := request.Limit - len(page.Candidates)
		for visited < r.maxBuckets && bucket < r.bucketCount && len(page.Candidates) < request.Limit {
			key, err := r.dueKey(bucket)
			if err != nil {
				return DiscoveryPage{}, err
			}
			batch = append(batch, struct {
				bucket  uint32
				entries *redis.ZSliceCmd
			}{bucket: bucket, entries: pipe.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
				Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: int64(remaining),
			})})
			visited++
			bucket++
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return DiscoveryPage{}, err
		}
		for _, item := range batch {
			entries, err := item.entries.Result()
			if err != nil {
				return DiscoveryPage{}, err
			}
			currentBucket := item.bucket
			remainingForItem := request.Limit - len(page.Candidates)
			if remainingForItem <= 0 {
				break
			}
			fingerprint := discoveryHeadFingerprint(entries)
			if request.Cursor.HeadFingerprint != "" && currentBucket == discoveryCursorBucket(request.Cursor.Token) &&
				fingerprint == request.Cursor.HeadFingerprint {
				// The previous full page did not move after processing. Advance the
				// bucket cursor so one malformed/held prefix cannot pin discovery.
				request.Cursor.HeadFingerprint = ""
				continue
			}
			for index, entry := range entries {
				if index >= remainingForItem {
					break
				}
				member := redisString(entry.Member)
				page.Candidates = append(page.Candidates, DispatchCandidate{BucketID: currentBucket, Member: member, ScoreMs: int64(entry.Score)})
			}
			if len(entries) >= remainingForItem {
				// Re-read this bucket's head next round. Claims/reschedules move the
				// returned prefix, exposing the next page without a mutable rank.
				page.Next = DiscoveryCursor{Token: discoveryBucketToken(currentBucket), HeadFingerprint: fingerprint}
				return page, nil
			}
			request.Cursor.HeadFingerprint = ""
		}
	}
	if bucket >= r.bucketCount {
		page.Exhausted = true
		page.Next = DiscoveryCursor{}
	} else {
		page.Next = DiscoveryCursor{Token: discoveryBucketToken(bucket)}
	}
	return page, nil
}

func discoveryCursorBucket(token string) uint32 {
	bucket, err := discoveryBucket(token)
	if err != nil {
		return ^uint32(0)
	}
	return bucket
}

func discoveryHeadFingerprint(entries []redis.Z) string {
	if len(entries) == 0 {
		return ""
	}
	h := sha256.New()
	var number [8]byte
	for _, entry := range entries {
		member := redisString(entry.Member)
		binary.BigEndian.PutUint64(number[:], uint64(len(member)))
		_, _ = h.Write(number[:])
		_, _ = h.Write([]byte(member))
		binary.BigEndian.PutUint64(number[:], uint64(int64(entry.Score)))
		_, _ = h.Write(number[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (r *RedisPairDispatchRepository) LoadDispatch(ctx context.Context, candidate DispatchCandidate) (DispatchOrder, error) {
	if r == nil || r.load == nil {
		return DispatchOrder{}, fmt.Errorf("%w: nil Redis Pair loader", ErrInvalidConfig)
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil || ref.WorkID == "" {
		return DispatchOrder{}, ErrMalformedMember
	}
	if ref.WorkType != FriendActionDispatchOrder {
		return DispatchOrder{}, ErrNotTheft
	}
	return r.load(ctx, candidate.BucketID, ref.WorkType, ref.WorkID)
}

func discoveryBucket(token string) (uint32, error) {
	if strings.TrimSpace(token) == "" {
		return 0, nil
	}
	if !strings.HasPrefix(token, "bucket:") {
		return 0, ErrMalformedMember
	}
	value, err := strconv.ParseUint(strings.TrimPrefix(token, "bucket:"), 10, 32)
	if err != nil {
		return 0, ErrMalformedMember
	}
	return uint32(value), nil
}

func discoveryBucketToken(bucket uint32) string {
	return "bucket:" + strconv.FormatUint(uint64(bucket), 10)
}

func redisString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}
