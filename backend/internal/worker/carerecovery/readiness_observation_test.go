package carerecovery

import (
	"context"
	"sync"
	"testing"
	"time"
)

type boundedCareDueFixture struct {
	mu     sync.Mutex
	limits []int
}

func (f *boundedCareDueFixture) ObserveDue(_ context.Context, limit int) (DueIndexObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, limit)
	return DueIndexObservation{Depth: int64(limit), Available: true}, nil
}

func TestCareDueObservationCacheNeverReusesProofAcrossBounds(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	observer := &boundedCareDueFixture{}
	worker := &Worker{config: normalizedConfig{DueObserver: observer, Now: func() time.Time { return now }}}
	first, err := worker.ObserveDue(context.Background(), 8)
	if err != nil || !first.Available || first.Depth != 8 {
		t.Fatalf("first observation=%+v err=%v", first, err)
	}
	second, err := worker.ObserveDue(context.Background(), 2)
	if err != nil || !second.Available || second.Depth != 2 {
		t.Fatalf("smaller bounded observation=%+v err=%v", second, err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.limits) != 2 || observer.limits[0] != 8 || observer.limits[1] != 2 {
		t.Fatalf("observer limits=%v", observer.limits)
	}
}
