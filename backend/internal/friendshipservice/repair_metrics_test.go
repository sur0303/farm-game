package friendshipservice

import (
	"context"
	"strings"
	"testing"
	"time"

	"farmgame/internal/friend"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type claimRepairQueue struct{ lease friend.FriendIndexRepairLease }

func (*claimRepairQueue) Enqueue(context.Context, uint64, int64) error { return nil }
func (q *claimRepairQueue) Claim(context.Context, int64, int64, int) ([]friend.FriendIndexRepairLease, error) {
	return []friend.FriendIndexRepairLease{q.lease}, nil
}
func (*claimRepairQueue) Complete(context.Context, friend.FriendIndexRepairLease, int64, int64) error {
	return nil
}
func (*claimRepairQueue) Retry(context.Context, friend.FriendIndexRepairLease, int64) error {
	return nil
}

func TestInstrumentedRepairQueueRecordsBoundedRepairAge(t *testing.T) {
	delegate := &claimRepairQueue{lease: friend.FriendIndexRepairLease{ViewerPlayerID: 7, LeaseDueAtMs: 3000}}
	recorder := &MemoryProjectionMetrics{}
	queue := NewInstrumentedRepairQueue(delegate, recorder)
	if err := queue.Enqueue(context.Background(), 7, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(context.Background(), 1250, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if recorder.RepairAge != 250*time.Millisecond {
		t.Fatalf("repair age=%v", recorder.RepairAge)
	}
	if err := queue.Complete(context.Background(), delegate.lease, 1300, 100); err != nil {
		t.Fatal(err)
	}
	recorder.RepairAge = 0
	if _, err := queue.Claim(context.Background(), 1500, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if recorder.RepairAge != 0 {
		t.Fatalf("completed repair retained process-local age=%v", recorder.RepairAge)
	}
}

func TestProjectionMetricsExposeCanonicalSignalsOnly(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder, err := NewPrometheusProjectionMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	RecordFriendIndexReconcileReport(recorder)(friend.FriendIndexReconcileReport{AppliedRows: 2, RemovedRows: 1})
	recorder.SetProjectionRepairAge(17 * time.Millisecond)
	if got := testutil.ToFloat64(recorder.pairIndexMismatch); got != 3 {
		t.Fatalf("Pair/Index mismatch metric=%v", got)
	}
	if got := testutil.ToFloat64(recorder.repairAge); got != 17 {
		t.Fatalf("repair age metric=%v", got)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "friend_legacy_") {
			t.Fatalf("retired legacy metric still registered: %s", family.GetName())
		}
	}
	if _, err := NewPrometheusProjectionMetrics(registry); err != nil {
		t.Fatalf("repeat composition did not reuse registered metrics: %v", err)
	}
}
