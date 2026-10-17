package readiness

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"
)

// TestLocalWorkerKillReplacementRecoversDueWithoutDuplicateOrResidual is a
// deterministic local fixture, not a Kubernetes kill claim. The participant
// receipt survives the first worker context cancellation; repository-trusted
// lease expiry makes the same DueIndex work claimable by its replacement.
func TestLocalWorkerKillReplacementRecoversDueWithoutDuplicateOrResidual(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repository := workflow.NewMemoryWorkRepository(clock)
	workID, err := workidentity.StableOpaqueID("readiness-kill-fixture-v1", "PLANT", "replacement")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"fixture":"worker-kill"}`)
	record, err := workflow.NewWorkRecord(workidentity.WorkAddress{
		WorkID: workID, WorkType: "PLANT", PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: 7,
	}, 42, "replacement-fixture", payload, sha256.Sum256(payload), []string{"APPLY"}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	handler := &killFixtureHandler{repository: repository, applied: make(chan struct{}), receipts: make(map[string]bool)}
	first, err := workflow.NewRecoveryLoop(repository, map[string]workflow.WorkHandler{"PLANT": handler}, workflow.RecoveryConfig{
		Owner: "worker-a", Token: "token-a", ClaimLimit: 1, LeaseTTL: time.Second, SafetyMargin: 100 * time.Millisecond, ClaimNow: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, killFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = first.RunOnce(firstCtx)
	}()
	<-handler.applied
	killFirst()
	<-firstDone
	if effects := handler.effectCount(); effects != 1 {
		t.Fatalf("first worker side effects=%d want=1", effects)
	}

	now = now.Add(time.Second + time.Millisecond)
	second, err := workflow.NewRecoveryLoop(repository, map[string]workflow.WorkHandler{"PLANT": handler}, workflow.RecoveryConfig{
		Owner: "worker-b", Token: "token-b", ClaimLimit: 1, LeaseTTL: time.Second, SafetyMargin: 100 * time.Millisecond, ClaimNow: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := second.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || handler.effectCount() != 1 {
		t.Fatalf("replacement stats=%+v side_effects=%d", stats, handler.effectCount())
	}
	if due, err := repository.ObserveDueByTypes(context.Background(), []string{"PLANT"}, 8); err != nil || !due.Available || due.Depth != 0 {
		t.Fatalf("replacement DueIndex residual=%+v err=%v", due, err)
	}
	stored, err := repository.Get(context.Background(), "PLANT", workID)
	if err != nil || stored.State != workflow.WorkStateCompleted {
		t.Fatalf("replacement terminal record=%+v err=%v", stored, err)
	}
}

func TestDueIndexProofIsUnavailableRatherThanPartialBeyondBound(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repository := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	for index := 0; index < 3; index++ {
		workID, err := workidentity.StableOpaqueID("readiness-due-bound-v1", "PLANT", string(rune('A'+index)))
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte{byte(index + 1)}
		record, err := workflow.NewWorkRecord(workidentity.WorkAddress{
			WorkID: workID, WorkType: "PLANT", PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: 7,
		}, 42, "due-bound-fixture", payload, sha256.Sum256(payload), []string{"APPLY"}, now, now.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		record.NextDueAtMs = now.Add(-time.Duration(index) * time.Millisecond).UnixMilli()
		if _, _, err := repository.CreateIfAbsent(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	partial, err := repository.ObserveDueByTypes(context.Background(), []string{"PLANT"}, 2)
	if err != nil || partial.Available || partial.Depth != 0 || partial.OldestDueAtMs != 0 {
		t.Fatalf("bounded partial was exposed as truth: %+v err=%v", partial, err)
	}
	complete, err := repository.ObserveDueByTypes(context.Background(), []string{"PLANT"}, 3)
	if err != nil || !complete.Available || complete.Depth != 3 || complete.OldestDueAtMs != now.Add(-2*time.Millisecond).UnixMilli() {
		t.Fatalf("complete DueIndex proof=%+v err=%v", complete, err)
	}
}

type killFixtureHandler struct {
	repository *workflow.MemoryWorkRepository
	applied    chan struct{}
	once       sync.Once
	mu         sync.Mutex
	receipts   map[string]bool
	effects    int
}

func (h *killFixtureHandler) Resume(ctx context.Context, record *workflow.WorkRecord, lease workflow.LeaseController) error {
	invocation := record.Steps[0].InvocationID
	h.mu.Lock()
	if !h.receipts[invocation] {
		h.receipts[invocation] = true
		h.effects++
	}
	h.mu.Unlock()
	first := false
	h.once.Do(func() {
		first = true
		close(h.applied)
	})
	if first {
		<-ctx.Done()
		return ctx.Err()
	}
	next := *record
	next.Payload = append([]byte(nil), record.Payload...)
	next.Steps = append([]workflow.StepRecord(nil), record.Steps...)
	next.State = workflow.WorkStateCompleted
	_, err := h.repository.Advance(ctx, &next, workflow.AdvanceOptions{
		ExpectedWorkVersion: record.WorkVersion, LeaseOwner: lease.Fence().Owner, LeaseEpoch: lease.Fence().Epoch,
	})
	return err
}

func (h *killFixtureHandler) effectCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.effects
}
