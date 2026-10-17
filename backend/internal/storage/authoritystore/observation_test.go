package authoritystore

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

type observationContextKey struct{}

type captureCommitObserver struct {
	observation CommitObservation
}

func (o *captureCommitObserver) BeginAuthorityCommit(ctx context.Context, _, _ string) (context.Context, func(CommitObservation)) {
	return context.WithValue(ctx, observationContextKey{}, true), func(observation CommitObservation) {
		o.observation = observation
	}
}

type observationStore struct {
	result       CommitResult
	err          error
	seenObserved bool
	auditedSeen  bool
	softSeen     bool
	exactSeen    bool
}

func (s *observationStore) LoadAndClaim(context.Context, LoadAndClaimRequest) (LoadAndClaimResult, error) {
	return LoadAndClaimResult{}, nil
}

func (s *observationStore) Commit(ctx context.Context, _ AuthorityCommitRequest) (CommitResult, error) {
	s.seenObserved, _ = ctx.Value(observationContextKey{}).(bool)
	return s.result, s.err
}

func (s *observationStore) CommitAuditedClaim(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	s.auditedSeen = true
	return s.Commit(ctx, req)
}

func (s *observationStore) CommitSoftFlush(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	s.softSeen = true
	return s.Commit(ctx, req)
}

func (s *observationStore) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	s.exactSeen = true
	return s.Commit(ctx, req)
}

func (s *observationStore) AdmissionSnapshotReuseSafe() bool { return true }

func (s *observationStore) FenceAndClaim(context.Context, FenceAndClaimRequest) (FenceAndClaimResult, error) {
	return FenceAndClaimResult{}, nil
}

func (s *observationStore) LookupIdempotencyOutcome(context.Context, IdempotencyLookupRequest) (IdempotencyLookupResult, error) {
	return IdempotencyLookupResult{}, nil
}

func (s *observationStore) PhysicalCapabilities() (PhysicalCapabilities, error) {
	return PhysicalCapabilities{MaxModelFields: 1, MaxModelBytes: 2, MaxReceiptItems: 3, MaxReceiptBytes: 4, MaxSingleReceiptBytes: 4, MaxCommitBytes: 5}, nil
}

func (s *observationStore) ProjectPhysicalCommit(AuthorityCommitRequest) (PhysicalCommitUsage, error) {
	return PhysicalCommitUsage{ModelBytes: 17, ReceiptItems: 1, ReceiptBytes: 211, SingleReceiptBytes: 211, CommitBytes: 333}, nil
}

func TestObservedActorStoreIsNonAdjudicatingAndUsesExactRequestBytes(t *testing.T) {
	wantErr := errors.New("unchanged store error")
	wantResult := CommitResult{
		Status: CommitTimeoutUnknown,
		Snapshot: AuthoritySnapshot{
			ActorAddress:  ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42},
			WriterClaimID: "claim", RecordVersion: 7,
			ModelFields: ModelState{"record": []byte("old")},
		},
	}
	inner := &observationStore{result: wantResult, err: wantErr}
	observer := &captureCommitObserver{}
	store := NewObservedActorStore(inner, observer)
	req := AuthorityCommitRequest{
		ActorAddress:    ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42},
		IdempotencyKey:  IdempotencyKey{StableCommandTypeName: "AR1_RECORD_COMMIT"},
		CandidateState:  ModelState{"record": []byte("candidate"), "public_profile": []byte("view")},
		Receipt:         AuthorityReceipt{ReceiptID: "receipt", Outcome: StoredOperationOutcome{Status: TerminalSucceeded, Payload: []byte("ok")}},
		OperationResult: StoredOperationOutcome{Status: TerminalSucceeded, Payload: []byte("ok")},
		AttachedOrders:  []AttachedOrder{{OrderID: "order", OrderType: "CARE_REWARD", Payload: []byte("payload")}},
	}
	started := time.Now()
	got, err := store.Commit(context.Background(), req)
	if !errors.Is(err, wantErr) || got.Status != wantResult.Status || got.Snapshot.RecordVersion != wantResult.Snapshot.RecordVersion {
		t.Fatalf("decorator changed result: got=%+v err=%v", got, err)
	}
	if !inner.seenObserved {
		t.Fatal("observer context did not reach the exact Store.Commit call")
	}
	observation := observer.observation
	if observation.Domain != "PLAYER" || observation.StableCommand != "AR1_RECORD_COMMIT" || observation.Status != "TIMEOUT_UNKNOWN" {
		t.Fatalf("identity observation=%+v", observation)
	}
	if observation.ActorRecordBytes != len("candidate") || observation.ModelStateBytes != ModelStateSerializedBytes(req.CandidateState) ||
		observation.CommitRequestBytes != AuthorityCommitSerializedBytes(req) ||
		observation.CommitResponseBytes != AuthorityCommitResponseSerializedBytes(wantResult) {
		t.Fatalf("byte observation=%+v", observation)
	}
	if observation.ReceiptBytes != 211 || observation.PhysicalCommitBytes != 333 || !observation.PhysicalProjectionKnown {
		t.Fatalf("physical projection=%+v", observation)
	}
	if len(observation.OrderBytes) != 1 || observation.OrderBytes[0] != AttachedOrderSerializedBytes(req.AttachedOrders[0]) ||
		observation.OrderPayloadBytes != len(req.AttachedOrders[0].Payload) {
		t.Fatalf("order observation=%+v", observation.OrderBytes)
	}
	if observation.SerializationDuration < 0 || observation.SerializationDuration > time.Since(started) {
		t.Fatalf("serialization duration=%v", observation.SerializationDuration)
	}
}

func TestObservedActorStoreNilObserverIsIdentity(t *testing.T) {
	inner := &observationStore{}
	if got := NewObservedActorStore(inner, nil); got != inner {
		t.Fatal("nil observer must return the exact inner store")
	}
}

func TestObservedActorStorePreservesOptionalInterfaceSet(t *testing.T) {
	observer := &captureCommitObserver{}
	inner := &observationStore{}
	physical := NewObservedActorStore(inner, observer)
	if _, ok := physical.(PhysicalCapabilitiesProvider); !ok {
		t.Fatal("physical capability contract was not preserved")
	}
	if _, ok := physical.(PhysicalCommitUsageProjector); !ok {
		t.Fatal("physical projector contract was not preserved")
	}
	if _, ok := physical.(AuditedClaimCommitter); !ok {
		t.Fatal("audited claim contract was not preserved")
	}
	if _, ok := physical.(SoftFlushCommitter); !ok {
		t.Fatal("soft flush contract was not preserved")
	}
	if _, ok := physical.(ExactCommitter); !ok {
		t.Fatal("exact commit contract was not preserved")
	}
	if reuse, ok := physical.(AdmissionSnapshotReuseCapable); !ok || !reuse.AdmissionSnapshotReuseSafe() {
		t.Fatal("admission snapshot reuse contract was not preserved")
	}
	if _, err := physical.(AuditedClaimCommitter).CommitAuditedClaim(context.Background(), AuthorityCommitRequest{}); err != nil || !inner.auditedSeen || !inner.seenObserved {
		t.Fatalf("audited commit was not observed/delegated: audited=%t observed=%t err=%v", inner.auditedSeen, inner.seenObserved, err)
	}
	inner.seenObserved = false
	if _, err := physical.(SoftFlushCommitter).CommitSoftFlush(context.Background(), AuthorityCommitRequest{}); err != nil || !inner.softSeen || !inner.seenObserved {
		t.Fatalf("soft flush was not observed/delegated: soft=%t observed=%t err=%v", inner.softSeen, inner.seenObserved, err)
	}
	inner.seenObserved = false
	if _, err := physical.(ExactCommitter).CommitExact(context.Background(), AuthorityCommitRequest{}); err != nil || !inner.exactSeen || !inner.seenObserved {
		t.Fatalf("exact commit was not observed/delegated: exact=%t observed=%t err=%v", inner.exactSeen, inner.seenObserved, err)
	}

	coreOnly := struct{ ActorAuthorityStore }{ActorAuthorityStore: &observationStore{}}
	observedCore := NewObservedActorStore(coreOnly, observer)
	if _, ok := observedCore.(PhysicalCapabilitiesProvider); ok {
		t.Fatal("decorator invented a physical capability contract")
	}
	if _, ok := observedCore.(PhysicalCommitUsageProjector); ok {
		t.Fatal("decorator invented a physical projector contract")
	}
	if reuse := observedCore.(AdmissionSnapshotReuseCapable); reuse.AdmissionSnapshotReuseSafe() {
		t.Fatal("decorator invented admission snapshot reuse safety")
	}
	// AuditedClaimCommitter is safe to expose because it falls back to ordinary
	// Commit. ExactCommitter is different: the decorator must not invent the
	// explicit proof capability when its delegate lacks it.
	if _, ok := observedCore.(AuditedClaimCommitter); !ok {
		t.Fatal("audited fallback contract missing")
	}
	if _, ok := observedCore.(SoftFlushCommitter); !ok {
		t.Fatal("soft flush fallback contract missing")
	}
	if _, ok := observedCore.(ExactCommitter); ok {
		t.Fatal("decorator invented exact commit capability")
	}
}
