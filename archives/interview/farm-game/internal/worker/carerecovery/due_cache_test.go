package carerecovery

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

type disabledCacheUpstream struct {
	calls atomic.Uint64
}

func (s *disabledCacheUpstream) ListDueWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error) {
	s.calls.Add(1)
	return nil, nil
}

func TestSharedDueIndexCacheIsFailClosedAndRetainsNoState(t *testing.T) {
	_, _, bucket := testBucket(t)
	upstream := &disabledCacheUpstream{}
	cache := NewSharedDueIndexCache(time.Minute, 4096, time.Now)
	scanner := cache.ForFamily(FamilyCareReward, upstream)

	const callers = 64
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			refs, err := scanner.ListDueWork(context.Background(), bucket, 32)
			if len(refs) != 0 || !errors.Is(err, ErrDueIndexFamilyUnavailable) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("disabled cache read err=%v", err)
	}
	if calls := upstream.calls.Load(); calls != 0 {
		t.Fatalf("disabled cache issued %d upstream commands", calls)
	}
	if stats := cache.Stats(); stats != (SharedDueIndexCacheStats{}) {
		t.Fatalf("disabled cache retained state: %+v", stats)
	}
}

func TestSharedDueIndexCacheLegacyAbsenceIsUnavailable(t *testing.T) {
	_, _, bucket := testBucket(t)
	cache := NewSharedDueIndexCache(time.Minute, 4096, time.Now)
	refs, err := cache.ForFamily("HARVEST", &disabledCacheUpstream{}).ListDueWork(context.Background(), bucket, 1)
	if len(refs) != 0 || !errors.Is(err, ErrDueIndexFamilyUnavailable) {
		t.Fatalf("legacy refs=%+v err=%v", refs, err)
	}
	observation, err := cache.ForFamily("HARVEST", &disabledCacheUpstream{}).
		ObserveBuckets(context.Background(), []rediskey.AuthorityBucketRef{bucket}, 4096)
	if observation.Available || !errors.Is(err, ErrDueIndexFamilyUnavailable) {
		t.Fatalf("legacy observation=%+v err=%v", observation, err)
	}
}
