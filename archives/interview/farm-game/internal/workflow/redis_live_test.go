package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	workflowpb "farmgame/api/gen/workflow"

	"github.com/redis/go-redis/v9"
)

// TestLiveRedisWorkflowStore verifies the Lua, Hash and sorted-set contract
// against the configured persistent Redis. It is skipped in hermetic test
// runs and enabled by FARM_REDIS_URL.
func TestLiveRedisWorkflowStore(t *testing.T) {
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live redis workflow test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redis.NewClient(options)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}

	prefix := fmt.Sprintf("farm:test:m1:%d", time.Now().UnixNano())
	store, err := NewRedisStore(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	operationID := "workflow-live"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, store.recordKey(operationID), store.dueKey()).Err()
	})

	now := time.Now().Truncate(time.Millisecond)
	candidate := testWorkflowRecord(operationID, workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	candidate.NextRetryAtMs = now.UnixMilli()
	created, inserted, err := store.CreateOrLoad(ctx, candidate)
	if err != nil || !inserted || created.WorkflowRevision != 1 {
		t.Fatalf("create record=%v inserted=%v err=%v", created, inserted, err)
	}
	loaded, inserted, err := store.CreateOrLoad(ctx, candidate)
	if err != nil || inserted || loaded.WorkflowRevision != 1 {
		t.Fatalf("load record=%v inserted=%v err=%v", loaded, inserted, err)
	}
	conflict := testWorkflowRecord(operationID, workflowpb.WorkflowKind_PLANT, 9, 9, testHash(2), now)
	if _, _, err := store.CreateOrLoad(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different payload accepted: %v", err)
	}

	claimed, err := store.ClaimDue(ctx, "live-worker", now, 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if _, err := store.Update(ctx, claimed[0], UpdateOptions{
		ExpectedRevision: claimed[0].WorkflowRevision,
		Now:              now,
	}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unfenced online update was accepted: %v", err)
	}
	claimed[0].State = workflowpb.WorkflowState_COMPLETED
	claimed[0].NextRetryAtMs = 0
	updated, err := store.Update(ctx, claimed[0], UpdateOptions{
		ExpectedRevision: claimed[0].WorkflowRevision,
		LeaseOwner:       claimed[0].LeaseOwner,
		LeaseEpoch:       claimed[0].LeaseEpoch,
		Now:              now,
	})
	if err != nil || updated.State != workflowpb.WorkflowState_COMPLETED ||
		updated.WorkflowRevision != claimed[0].WorkflowRevision+1 {
		t.Fatalf("complete=%v err=%v", updated, err)
	}
	if due, err := store.ClaimDue(ctx, "other-worker", now.Add(time.Minute), 1, time.Second); err != nil || len(due) != 0 {
		t.Fatalf("terminal workflow remained due: %v err=%v", due, err)
	}
}
