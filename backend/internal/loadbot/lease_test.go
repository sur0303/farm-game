package loadbot

import (
	"errors"
	"testing"
	"time"
)

func TestWorkerAssignmentLeaseRenewFenceAndRelease(t *testing.T) {
	model, err := NewWorkerAssignmentModel("lease-contract-test", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0).UTC()
	first, err := model.Assign("worker-a", 1, start)
	if err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if first.Lease.Epoch != 1 || !first.LeaseValidAt(start.Add(9*time.Second)) {
		t.Fatalf("first assignment lease = %+v", first.Lease)
	}
	if _, err := model.Assign("worker-b", 1, start.Add(time.Second)); !errors.Is(err, ErrAssignmentExists) {
		t.Fatalf("duplicate assignment error = %v, want ErrAssignmentExists", err)
	}
	renewed, err := model.Renew(first.AssignmentID, "worker-a", first.Lease.Epoch, start.Add(5*time.Second))
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed.Lease.ExpiresAt != start.Add(15*time.Second) {
		t.Fatalf("renewed expiry = %v, want %v", renewed.Lease.ExpiresAt, start.Add(15*time.Second))
	}
	if _, err := model.Renew(first.AssignmentID, "worker-b", first.Lease.Epoch, start.Add(6*time.Second)); !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("wrong worker renew error = %v, want ErrLeaseFenced", err)
	}
	if err := model.Release(first.AssignmentID, "worker-a", first.Lease.Epoch, start.Add(7*time.Second)); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	released, ok := model.Get(1)
	if !ok || released.State != WorkerAssignmentReleased || released.Lease.State != WorkerLeaseReleased {
		t.Fatalf("released assignment = %+v", released)
	}
	if _, err := model.Renew(first.AssignmentID, "worker-a", first.Lease.Epoch, start.Add(8*time.Second)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew released lease error = %v, want ErrLeaseExpired", err)
	}
}

func TestWorkerAssignmentExpiryFencesReassignment(t *testing.T) {
	model, err := NewWorkerAssignmentModel("lease-expiry-test", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(200, 0).UTC()
	old, err := model.Assign("worker-old", 7, start)
	if err != nil {
		t.Fatal(err)
	}
	expired := model.ReapExpired(start.Add(5 * time.Second))
	if len(expired) != 1 || expired[0].State != WorkerAssignmentExpired {
		t.Fatalf("expired assignments = %+v", expired)
	}
	next, err := model.Assign("worker-new", 7, start.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if next.Lease.Epoch != old.Lease.Epoch+1 || next.WorkerID != "worker-new" {
		t.Fatalf("reassignment = %+v, old=%+v", next, old)
	}
	if err := model.Release(old.AssignmentID, old.WorkerID, old.Lease.Epoch, start.Add(7*time.Second)); !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("stale release error = %v, want ErrLeaseFenced", err)
	}
	if len(model.List()) != 1 {
		t.Fatalf("model list = %+v, want one current assignment", model.List())
	}
}

func TestWorkerAssignmentRejectsInvalidModelInputs(t *testing.T) {
	if _, err := NewWorkerAssignmentModel("bad run", time.Second); err == nil {
		t.Fatal("invalid run id accepted")
	}
	if _, err := NewWorkerAssignmentModel("run", 0); err == nil {
		t.Fatal("zero lease TTL accepted")
	}
	model, err := NewWorkerAssignmentModel("lease-input-test", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.Assign("worker/id", 1, time.Unix(1, 0)); !errors.Is(err, ErrInvalidLeaseRequest) {
		t.Fatalf("invalid worker id error = %v, want ErrInvalidLeaseRequest", err)
	}
}

func TestWorkerAssignmentRenewAndReleaseRejectTimeRollback(t *testing.T) {
	model, err := NewWorkerAssignmentModel("lease-time-rollback-test", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_000, 0).UTC()
	assignment, err := model.Assign("worker-a", 1, start)
	if err != nil {
		t.Fatal(err)
	}
	renewedAt := start.Add(10 * time.Second)
	renewed, err := model.Renew(
		assignment.AssignmentID,
		assignment.WorkerID,
		assignment.Lease.Epoch,
		renewedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rollbackAt := renewedAt.Add(-time.Second)

	if _, err := model.Renew(
		assignment.AssignmentID,
		assignment.WorkerID,
		assignment.Lease.Epoch,
		rollbackAt,
	); !errors.Is(err, ErrLeaseTimeRollback) {
		t.Fatalf("Renew() rollback error = %v, want ErrLeaseTimeRollback", err)
	}
	if current, ok := model.Get(1); !ok || current != renewed {
		t.Fatalf("rollback renew mutated assignment: got=%+v want=%+v", current, renewed)
	}

	if err := model.Release(
		assignment.AssignmentID,
		assignment.WorkerID,
		assignment.Lease.Epoch,
		rollbackAt,
	); !errors.Is(err, ErrLeaseTimeRollback) {
		t.Fatalf("Release() rollback error = %v, want ErrLeaseTimeRollback", err)
	}
	if current, ok := model.Get(1); !ok || current != renewed {
		t.Fatalf("rollback release mutated assignment: got=%+v want=%+v", current, renewed)
	}
}

func TestWorkerAssignmentAssignRejectsTimeRollbackAgainstHistory(t *testing.T) {
	model, err := NewWorkerAssignmentModel("lease-assign-rollback-test", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(2_000, 0).UTC()
	assignment, err := model.Assign("worker-a", 3, start)
	if err != nil {
		t.Fatal(err)
	}
	releasedAt := start.Add(5 * time.Second)
	if err := model.Release(
		assignment.AssignmentID,
		assignment.WorkerID,
		assignment.Lease.Epoch,
		releasedAt,
	); err != nil {
		t.Fatal(err)
	}
	history, ok := model.Get(3)
	if !ok {
		t.Fatal("released assignment history is missing")
	}
	if _, err := model.Assign("worker-b", 3, releasedAt.Add(-time.Second)); !errors.Is(err, ErrLeaseTimeRollback) {
		t.Fatalf("Assign() rollback error = %v, want ErrLeaseTimeRollback", err)
	}
	if current, ok := model.Get(3); !ok || current != history {
		t.Fatalf("rollback assign mutated history: got=%+v want=%+v", current, history)
	}
}
