package theftrecovery

import (
	"context"
	"sync"
	"testing"
	"time"
)

type boundedPairDueFixture struct {
	*fakePairRepository
	mu     sync.Mutex
	limits []int
}

func (f *boundedPairDueFixture) ObserveDue(_ context.Context, limit int) (PairDueObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, limit)
	return PairDueObservation{Theft: PairDueFamilyObservation{Depth: int64(limit)}, Available: true}, nil
}

func TestPairDueObservationCacheNeverReusesProofAcrossBounds(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	pair := &boundedPairDueFixture{fakePairRepository: &fakePairRepository{}}
	worker := newTestWorker(t, pair, &forgedClaimLease{}, &fakeAdvancer{}, now)
	first, err := worker.ObserveDue(context.Background(), 8)
	if err != nil || !first.Available || first.Theft.Depth != 8 {
		t.Fatalf("first observation=%+v err=%v", first, err)
	}
	second, err := worker.ObserveDue(context.Background(), 2)
	if err != nil || !second.Available || second.Theft.Depth != 2 {
		t.Fatalf("smaller bounded observation=%+v err=%v", second, err)
	}
	pair.mu.Lock()
	defer pair.mu.Unlock()
	if len(pair.limits) != 2 || pair.limits[0] != 8 || pair.limits[1] != 2 {
		t.Fatalf("observer limits=%v", pair.limits)
	}
}

func TestPairDueObservationUsesConfiguredCacheTTL(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := now
	pair := &boundedPairDueFixture{fakePairRepository: &fakePairRepository{}}
	worker := newTestWorker(t, pair, &forgedClaimLease{}, &fakeAdvancer{}, now)
	worker.config.Now = func() time.Time { return clock }
	worker.config.DueObservationCacheTTL = 10 * time.Second

	if _, err := worker.ObserveDue(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(9 * time.Second)
	if _, err := worker.ObserveDue(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	pair.mu.Lock()
	if len(pair.limits) != 1 {
		pair.mu.Unlock()
		t.Fatalf("cached observer calls=%d want=1", len(pair.limits))
	}
	pair.mu.Unlock()

	clock = clock.Add(time.Second)
	if _, err := worker.ObserveDue(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	pair.mu.Lock()
	defer pair.mu.Unlock()
	if len(pair.limits) != 2 {
		t.Fatalf("expired observer calls=%d want=2", len(pair.limits))
	}
}
