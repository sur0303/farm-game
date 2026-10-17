package workflow

import (
	"context"
	"testing"
	"time"

	workflowpb "farmgame/api/gen/workflow"
)

// TestMemoryStorePendingDueCountsBacklog 验证 PendingDue 只统计非终态且到期的
// workflow(恢复风暴时 worker 的积压观测)。
func TestMemoryStorePendingDueCountsBacklog(t *testing.T) {
	store := NewMemoryStore()
	now := time.UnixMilli(1_800_000_000_000)
	due := testWorkflowRecord("due-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	due.NextRetryAtMs = now.UnixMilli()
	future := testWorkflowRecord("future-1", workflowpb.WorkflowKind_HARVEST, 9, 9, testHash(2), now)
	future.NextRetryAtMs = now.Add(time.Minute).UnixMilli()
	for _, record := range []*workflowpb.WorkflowRecord{due, future} {
		if _, _, err := store.CreateOrLoad(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	// 完成态即使到期也不计入 backlog。
	done := testWorkflowRecord("done-1", workflowpb.WorkflowKind_THEFT, 9, 10, testHash(3), now)
	if _, _, err := store.CreateOrLoad(context.Background(), done); err != nil {
		t.Fatal(err)
	}
	storedDone, _ := store.Get(context.Background(), done.WorkflowId)
	storedDone.State = workflowpb.WorkflowState_COMPLETED
	storedDone.NextRetryAtMs = now.UnixMilli()
	if _, err := store.Update(context.Background(), storedDone, UpdateOptions{
		ExpectedRevision: storedDone.WorkflowRevision,
		Now:              now,
	}); err != nil {
		t.Fatal(err)
	}

	pending, err := store.PendingDue(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending=%d, want 1 (due only)", pending)
	}
	later, err := store.PendingDue(context.Background(), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if later != 2 {
		t.Fatalf("pending later=%d, want 2 (future now due, completed excluded)", later)
	}
}
