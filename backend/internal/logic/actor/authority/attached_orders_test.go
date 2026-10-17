package authority

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

func TestCASEpochAttachedOrdersAtomicReplayConflictAndUnknown(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 120, "farm-holder", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider := newProvider(t, fault, fence)
	binding := claimBinding(t, provider, actorrouting.DomainFarm, 120, authoritystore.ModelState{RecordModelField: []byte("v1")})
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 120}
	order := authoritystore.AttachedOrder{OrderID: "care-order-1", OrderType: "CARE_REWARD", Payload: []byte("snapshot"), DueAtMs: 1234}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("care-order-1\x00\x01")}

	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 1, 0, []byte("v2"), outcome, []authoritystore.AttachedOrder{order}); err != nil {
		t.Fatal(err)
	}
	if got := store.AttachedOrderCount(address); got != 1 {
		t.Fatalf("attached orders=%d, want 1", got)
	}
	snapshot, ok := store.Inspect(address)
	if !ok || string(snapshot.ModelFields[RecordModelField]) != "v2" || snapshot.RecordVersion != 2 {
		t.Fatalf("atomic snapshot=%+v ok=%v", snapshot, ok)
	}

	// The same candidate, outcome and order is a durable replay, not a second
	// source order or a second record-version increment.
	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 1, 0, []byte("v2"), outcome, []authoritystore.AttachedOrder{order}); err != nil {
		t.Fatalf("replay err=%v", err)
	}
	if got := store.AttachedOrderCount(address); got != 1 || binding.Version() != 2 {
		t.Fatalf("replay orders=%d binding_version=%d", got, binding.Version())
	}

	// Attached order bytes are part of the new seam's canonical hash. A stale
	// same-version writer carrying a different order is a CAS conflict, not a
	// false ALREADY_COMMITTED replay.
	changedOrder := order
	changedOrder.Payload = []byte("different-snapshot")
	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 1, 0, []byte("v2"), outcome, []authoritystore.AttachedOrder{changedOrder}); !errors.Is(err, blobstore.ErrCASConflict) {
		t.Fatalf("changed order err=%v", err)
	}
	if got := store.AttachedOrderCount(address); got != 1 {
		t.Fatalf("conflict changed order count=%d", got)
	}

	// TIMEOUT_UNKNOWN is resolved against the original order-bearing commit;
	// replay after resolution still returns the original atomic result.
	nextOrder := authoritystore.AttachedOrder{OrderID: "care-order-2", OrderType: "CARE_REWARD", Payload: []byte("snapshot-2"), DueAtMs: 1235}
	fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 2, 0, []byte("v3"), outcome, []authoritystore.AttachedOrder{nextOrder}); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown err=%v", err)
	}
	if got := store.AttachedOrderCount(address); got != 2 || !binding.HasPendingUnknown() {
		t.Fatalf("unknown applied orders=%d pending=%v", got, binding.HasPendingUnknown())
	}
	if resolution, err := binding.ResolveUnknown(ctx); err != nil || resolution != ResolutionCommitted {
		t.Fatalf("resolution=%v err=%v", resolution, err)
	}
	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 2, 0, []byte("v3"), outcome, []authoritystore.AttachedOrder{nextOrder}); err != nil {
		t.Fatalf("unknown replay err=%v", err)
	}
	if got := store.AttachedOrderCount(address); got != 2 || binding.Version() != 3 {
		t.Fatalf("unknown replay orders=%d binding_version=%d", got, binding.Version())
	}

	// A stale record version with no prior idempotency receipt is rejected by
	// the Authority Commit fence and cannot attach another order.
	if err := binding.CASEpochWithOutcomeAndOrders(ctx, 120, 99, 0, []byte("v100"), outcome, []authoritystore.AttachedOrder{{OrderID: "care-order-stale", OrderType: "CARE_REWARD", Payload: []byte("stale")}}); !errors.Is(err, blobstore.ErrCASConflict) {
		t.Fatalf("stale record version err=%v", err)
	}
	if got := store.AttachedOrderCount(address); got != 2 {
		t.Fatalf("stale record version order count=%d", got)
	}
}
