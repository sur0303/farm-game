package authority

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/persistence"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

func TestSoftFlushCanonicalPayloadHashBindsStateAndSchedules(t *testing.T) {
	stateA := authoritystore.PayloadHash(sha256.Sum256([]byte("state-a")))
	stateB := authoritystore.PayloadHash(sha256.Sum256([]byte("state-b")))
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	schedules := []authoritystore.AttachedSchedule{{
		ScheduleID: "schedule-1", ScheduleType: "FARM_OPPORTUNITY_WAKEUP",
		Payload: []byte("payload"), DueAtMs: 100, Generation: 1, SubjectPlayerID: 7,
	}}

	first := softFlushCanonicalPayloadHash(stateA, outcome, schedules)
	if again := softFlushCanonicalPayloadHash(stateA, outcome, schedules); again != first {
		t.Fatal("soft flush hash is not deterministic")
	}
	if changedState := softFlushCanonicalPayloadHash(stateB, outcome, schedules); changedState == first {
		t.Fatal("soft flush hash did not bind state digest")
	}
	changedSchedules := cloneAttachedSchedules(schedules)
	changedSchedules[0].DueAtMs++
	if changedSchedule := softFlushCanonicalPayloadHash(stateA, outcome, changedSchedules); changedSchedule == first {
		t.Fatal("soft flush hash did not bind schedule")
	}
}

func newSoftProvider(t *testing.T, store authoritystore.ActorAuthorityStore, fence authoritystore.ShardWriteFence) *Provider {
	t.Helper()
	provider, err := NewProvider(ProviderOptions{
		Store: store, Fence: StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1", BootID: "holder-soft-boot-1", EnableSoftSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func TestSoftStageIsMemoryOnlyUntilFlush(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 71, "holder-soft", 1, 1)
	provider := newSoftProvider(t, store, fence)
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 71,
		authoritystore.ModelState{RecordModelField: []byte("initial")})

	version, err := binding.StageSoftModelState(71,
		authoritystore.ModelState{RecordModelField: []byte("memory-v2")}, nil)
	if err != nil || version != 2 || !binding.SoftDirty() {
		t.Fatalf("version=%d dirty=%v err=%v", version, binding.SoftDirty(), err)
	}
	current, currentVersion, err := binding.Get(ctx, 71)
	if err != nil || currentVersion != 2 || string(current) != "memory-v2" {
		t.Fatalf("memory data=%q version=%d err=%v", current, currentVersion, err)
	}
	if binding.persistedVersion != 1 {
		t.Fatalf("persistedVersion=%d", binding.persistedVersion)
	}

	flushed, err := binding.FlushSoftModelState(ctx, 71)
	if err != nil || !flushed.Flushed || flushed.MemoryVersion != 2 || flushed.PersistedRecordVersion != 2 {
		t.Fatalf("flush=%+v err=%v", flushed, err)
	}
	if binding.SoftDirty() {
		t.Fatal("binding remained dirty")
	}
	if snap := binding.Lane().Snapshot(); snap.Dirty || snap.PersistedThroughVersion != 2 {
		t.Fatalf("lane=%+v", snap)
	}
}

func TestSoftStageDefensiveAPIStillCopiesCallerState(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 710, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainPlayer, 710,
		authoritystore.ModelState{RecordModelField: []byte("initial")})
	candidate := authoritystore.ModelState{RecordModelField: []byte("memory-v2")}
	if _, err := binding.StageSoftModelState(710, candidate, nil); err != nil {
		t.Fatal(err)
	}
	candidate[RecordModelField][0] = 'X'
	data, _, err := binding.Get(context.Background(), 710)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "memory-v2" {
		t.Fatalf("defensive StageSoftModelState retained caller alias: %q", data)
	}
}

func TestVisitCurrentModelSnapshotReadsSoftTruth(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 711, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainFarm, 711,
		authoritystore.ModelState{RecordModelField: []byte("initial")})
	if _, err := binding.StageSoftModelStateOwned(711,
		authoritystore.ModelState{RecordModelField: []byte("owned-v2")}, nil); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := binding.VisitCurrentModelSnapshot(func(snapshot authoritystore.AuthoritySnapshot) error {
		got = string(snapshot.ModelFields[RecordModelField])
		if snapshot.RecordVersion != 2 {
			t.Fatalf("record version=%d, want 2", snapshot.RecordVersion)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != "owned-v2" {
		t.Fatalf("borrowed snapshot=%q", got)
	}
}

func TestSoftStageCoalescesManyTurnsIntoOneRedisVersion(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 72, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainFarm, 72,
		authoritystore.ModelState{RecordModelField: []byte("v1")})

	for i, value := range []string{"v2", "v3", "v4", "v5"} {
		version, err := binding.StageSoftModelState(72,
			authoritystore.ModelState{RecordModelField: []byte(value)}, nil)
		if err != nil || version != uint64(i+2) {
			t.Fatalf("stage %d version=%d err=%v", i, version, err)
		}
	}
	result, err := binding.FlushSoftModelState(ctx, 72)
	if err != nil {
		t.Fatal(err)
	}
	if result.MemoryVersion != 5 || result.PersistedRecordVersion != 2 {
		t.Fatalf("result=%+v", result)
	}
	data, version, err := binding.Get(ctx, 72)
	if err != nil || version != 5 || string(data) != "v5" {
		t.Fatalf("memory data=%q version=%d err=%v", data, version, err)
	}
}

func TestSoftStageRemainsOptIn(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 73, "holder-a", 1, 1)
	binding := claimBinding(t, newProvider(t, store, fence), actorrouting.DomainPlayer, 73,
		authoritystore.ModelState{RecordModelField: []byte("v1")})
	if _, err := binding.StageSoftModelState(73,
		authoritystore.ModelState{RecordModelField: []byte("v2")}, nil); !errors.Is(err, persistence.ErrSoftSaveDisabled) {
		t.Fatalf("err=%v", err)
	}
}

func TestSoftStageRejectsInvalidModelShapeBeforeDirtying(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 731, "holder-a", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainPlayer, 731,
		authoritystore.ModelState{RecordModelField: []byte("v1")})

	if _, err := binding.StageSoftModelStateOwned(731, nil, nil); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("nil state err=%v", err)
	}
	if _, err := binding.StageSoftModelStateOwned(731, authoritystore.ModelState{"other": []byte("v2")}, nil); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("missing v1 record err=%v", err)
	}
	if binding.SoftDirty() || binding.Version() != 1 {
		t.Fatalf("invalid stage dirtied binding: version=%d dirty=%t", binding.Version(), binding.SoftDirty())
	}
}

func TestSoftFlushStillHonorsWriterEpochFence(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 74, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainFarm, 74,
		authoritystore.ModelState{RecordModelField: []byte("v1")})
	if _, err := binding.StageSoftModelState(74,
		authoritystore.ModelState{RecordModelField: []byte("dirty")}, nil); err != nil {
		t.Fatal(err)
	}
	_ = setShardAuthority(t, store, actorrouting.DomainFarm, 74, "holder-b", 2, 2)
	if _, err := binding.FlushSoftModelState(context.Background(), 74); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestSoftFlushFrozenGenerationKeepsNewerMutationDirty(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 75, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainFarm, 75,
		authoritystore.ModelState{RecordModelField: []byte("v1")})

	if version, err := binding.StageSoftModelState(75,
		authoritystore.ModelState{RecordModelField: []byte("v2")}, nil); err != nil || version != 2 {
		t.Fatalf("stage v2 version=%d err=%v", version, err)
	}
	attempt, _, err := binding.PrepareSoftFlush(75)
	if err != nil || attempt == nil {
		t.Fatalf("prepare err=%v attempt=%v", err, attempt)
	}
	// This models a new Actor turn while the frozen v2 Redis operation is in
	// flight outside the mailbox.
	if version, err := binding.StageSoftModelState(75,
		authoritystore.ModelState{RecordModelField: []byte("v3")}, nil); err != nil || version != 3 {
		t.Fatalf("stage v3 version=%d err=%v", version, err)
	}
	result, err := binding.CompleteSoftFlush(attempt, attempt.Execute(ctx))
	if err != nil || !result.Flushed || result.MemoryVersion != 2 || result.PersistedRecordVersion != 2 {
		t.Fatalf("first flush=%+v err=%v", result, err)
	}
	if !binding.SoftDirty() || binding.Version() != 3 {
		t.Fatalf("newer generation lost: version=%d dirty=%t", binding.Version(), binding.SoftDirty())
	}
	if lane := binding.Lane().Snapshot(); !lane.Dirty || lane.MemoryStateVersion != 3 || lane.PersistedThroughVersion != 2 {
		t.Fatalf("lane after frozen completion=%+v", lane)
	}

	result, err = binding.FlushSoftModelState(ctx, 75)
	if err != nil || result.MemoryVersion != 3 || result.PersistedRecordVersion != 3 || binding.SoftDirty() {
		t.Fatalf("second flush=%+v dirty=%t err=%v", result, binding.SoftDirty(), err)
	}

	// A new writer generation must observe v3, proving the late v2 completion
	// never cleared or overwrote the newer process-local state.
	setShardAuthority(t, store, actorrouting.DomainFarm, 75, "holder-soft-b", 2, 2)
	fenceB := authoritystore.ShardWriteFence{
		ExpectedHolderID: "holder-soft-b", ExpectedHolderBootID: "holder-soft-b-boot-1",
		ExpectedAssignmentRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
	}
	providerB, err := NewProvider(ProviderOptions{
		Store: store, Fence: StaticFenceSource{Fence: fenceB},
		AuthorityLayoutVersion: "v1", BootID: "holder-soft-b-boot-1", EnableSoftSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingB := claimBinding(t, providerB, actorrouting.DomainFarm, 75, nil)
	data, physicalVersion, err := bindingB.Get(ctx, 75)
	if err != nil || string(data) != "v3" || physicalVersion != 3 {
		t.Fatalf("reactivated data=%q physicalVersion=%d err=%v", data, physicalVersion, err)
	}
}

func TestSoftFlushBorrowsImmutableGenerationWithoutFullStateClone(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 751, "holder-soft", 1, 1)
	binding := claimBinding(t, newSoftProvider(t, store, fence), actorrouting.DomainFarm, 751,
		authoritystore.ModelState{RecordModelField: []byte("v1")})

	owned := authoritystore.ModelState{
		RecordModelField: []byte("immutable-v2"),
		"header":         []byte("header-v2"),
	}
	if _, err := binding.StageSoftModelStateOwned(751, owned, nil); err != nil {
		t.Fatal(err)
	}
	attempt, _, err := binding.PrepareSoftFlush(751)
	if err != nil || attempt == nil {
		t.Fatalf("prepare err=%v attempt=%v", err, attempt)
	}
	if &attempt.state[RecordModelField][0] != &owned[RecordModelField][0] ||
		&attempt.state["header"][0] != &owned["header"][0] {
		t.Fatal("PrepareSoftFlush cloned the immutable actor generation")
	}

	// A later mailbox turn replaces b.models with a fresh generation. The
	// in-flight attempt must continue to observe exactly the old bytes.
	newer := authoritystore.ModelState{
		RecordModelField: []byte("immutable-v3"),
		"header":         []byte("header-v3"),
	}
	if _, err := binding.StageSoftModelStateOwned(751, newer, nil); err != nil {
		t.Fatal(err)
	}
	if got := string(attempt.state[RecordModelField]); got != "immutable-v2" {
		t.Fatalf("frozen generation changed to %q", got)
	}
	if got := string(attempt.request.ModelMutation.ModelUpserts[RecordModelField]); got != "immutable-v2" {
		t.Fatalf("request mutation changed to %q", got)
	}
	if &attempt.request.ModelMutation.ModelUpserts[RecordModelField][0] != &owned[RecordModelField][0] {
		t.Fatal("soft flush mutation copied its immutable frozen upsert")
	}
}

func TestSoftLogicalVersionRestoresAndFlushIdentityIsClaimScoped(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fenceA := setShardAuthority(t, store, actorrouting.DomainPlayer, 76, "holder-soft", 1, 1)
	bindingA := claimBinding(t, newSoftProvider(t, store, fenceA), actorrouting.DomainPlayer, 76,
		authoritystore.ModelState{RecordModelField: []byte("v1")})
	for _, value := range []string{"v2", "v3", "v4", "v5"} {
		if _, err := bindingA.StageSoftModelState(76,
			authoritystore.ModelState{RecordModelField: []byte(value)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	attemptA, _, err := bindingA.PrepareSoftFlush(76)
	if err != nil {
		t.Fatal(err)
	}
	opA := attemptA.request.IdempotencyKey.OperationID
	if _, err := bindingA.CompleteSoftFlush(attemptA, attemptA.Execute(ctx)); err != nil {
		t.Fatal(err)
	}
	if bindingA.persistedVersion != 2 || bindingA.Version() != 5 {
		t.Fatalf("before restart physical=%d logical=%d", bindingA.persistedVersion, bindingA.Version())
	}

	setShardAuthority(t, store, actorrouting.DomainPlayer, 76, "holder-soft-b", 2, 2)
	fenceB := authoritystore.ShardWriteFence{
		ExpectedHolderID: "holder-soft-b", ExpectedHolderBootID: "holder-soft-b-boot-1",
		ExpectedAssignmentRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
	}
	providerB, err := NewProvider(ProviderOptions{
		Store: store, Fence: StaticFenceSource{Fence: fenceB},
		AuthorityLayoutVersion: "v1", BootID: "holder-soft-b-boot-1", EnableSoftSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingB := claimBinding(t, providerB, actorrouting.DomainPlayer, 76, nil)
	if bindingB.Version() != 2 {
		t.Fatalf("physical version before restore=%d", bindingB.Version())
	}
	// The real Player/Farm codec supplies this value from actor_memory_version.
	if err := bindingB.RestoreSoftMemoryVersion(5); err != nil {
		t.Fatal(err)
	}
	if bindingB.Version() != 5 || bindingB.Lane().Snapshot().PersistedThroughVersion != 5 {
		t.Fatalf("restored binding version=%d lane=%+v", bindingB.Version(), bindingB.Lane().Snapshot())
	}
	if version, err := bindingB.StageSoftModelState(76,
		authoritystore.ModelState{RecordModelField: []byte("v6")}, nil); err != nil || version != 6 {
		t.Fatalf("post-restart stage version=%d err=%v", version, err)
	}
	attemptB, _, err := bindingB.PrepareSoftFlush(76)
	if err != nil {
		t.Fatal(err)
	}
	opB := attemptB.request.IdempotencyKey.OperationID
	if opA == opB {
		t.Fatalf("flush idempotency identity reused across writer claims: %q", opA)
	}
}
