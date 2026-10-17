package workflow

import (
	"context"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	workflowpb "farmgame/api/gen/workflow"
)

func TestRecoveryLoopDoesNotLeaseUnownedWorkType(t *testing.T) {
	f := newCrashFixture(t)
	f.create(t, "op-rec-1")
	// An unregistered work type remains available to its own recovery root; the
	// generic loop must not transiently lease it and then skip it.
	unknown, err := NewWorkRecord(testAddress("UNREGISTERED_TYPE", "op-rec-x"), 9001, "op-rec-x",
		[]byte(`{"x":1}`), testPayloadHash("op-rec-x"), []string{"STEP_A"}, f.now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.repo.CreateIfAbsent(context.Background(), unknown); err != nil {
		t.Fatal(err)
	}
	stats := f.recoverOnce(t, "worker-1")
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Skipped != 0 || stats.Failed != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if pending, _ := f.repo.PendingDue(context.Background()); pending != 1 {
		t.Fatalf("pending=%d", pending)
	}
	stored, err := f.repo.Get(context.Background(), "UNREGISTERED_TYPE", unknown.Address.WorkID)
	if err != nil || stored.LeaseOwner != "" || stored.LeaseEpoch != 0 {
		t.Fatalf("unowned record was leased: record=%+v err=%v", stored, err)
	}
	if _, err := f.repo.ClaimWork(context.Background(), "UNREGISTERED_TYPE", unknown.Address.WorkID,
		"owner-of-unknown", "token", 30*time.Second); err != nil {
		t.Fatalf("owning recovery root could not immediately claim: %v", err)
	}
}

func TestRecoveryLoopLeaseControllerRenew(t *testing.T) {
	f := newCrashFixture(t)
	f.create(t, "op-rec-renew")
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-1", "token-1", 1, 30*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	record := claimed[0].Record
	controller := &workLeaseController{
		repo:     f.repo,
		fence:    LeaseFence{Owner: "worker-1", Epoch: record.LeaseEpoch},
		deadline: time.Now().Add(25 * time.Second),
		token:    "token-1",
		ttl:      30 * time.Second,
		margin:   5 * time.Second,
		workType: record.Address.WorkType,
		workID:   record.Address.WorkID,
		mono:     time.Now,
	}
	f.now = f.now.Add(10 * time.Second)
	if err := controller.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored := f.get(t, record)
	if stored.LeaseExpireAtMs != f.now.Add(30*time.Second).UnixMilli() {
		t.Fatalf("renew did not extend expiry: %v", stored.LeaseExpireAtMs)
	}
	if stored.WorkVersion != record.WorkVersion {
		t.Fatalf("renew bumped work version: %d→%d", record.WorkVersion, stored.WorkVersion)
	}
	if controller.Expired() {
		t.Fatal("controller expired after renew")
	}
}

// TestRecoveryLoopDrivesLegacyRunner proves the single worker mainline: the
// same RecoveryLoop that drives generic works also recovers the existing
// legacy WorkflowRecord family through the adapter.
func TestRecoveryLoopDrivesLegacyRunner(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	legacyStore := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{Status: commonpb.CommandStatus_PROCESSING}}
	runner, _ := NewRunner(legacyStore, player, farm)
	runner.now = func() time.Time { return now }

	legacy := testWorkflowRecord("op-legacy-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	legacy.NextRetryAtMs = now.UnixMilli()
	if _, _, err := legacyStore.CreateOrLoad(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}

	repo, err := NewLegacyRepository(legacyStore, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	loop, err := NewRecoveryLoop(repo, map[string]WorkHandler{
		"LEGACY_PLANT": LegacyHandler{Runner: runner},
	}, RecoveryConfig{Owner: "worker-legacy", Token: "token", LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := loop.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored, err := legacyStore.Get(context.Background(), legacy.WorkflowId)
	if err != nil || stored.State != workflowpb.WorkflowState_COMPLETED {
		t.Fatalf("legacy=%v err=%v", stored, err)
	}
	if player.commits != 1 || farm.completes != 1 {
		t.Fatalf("player=%+v farm=%+v", player, farm)
	}
	// Duplicate recovery does not re-execute.
	stats, err = loop.RunOnce(context.Background())
	if err != nil || stats.Claimed != 0 {
		t.Fatalf("second=%+v err=%v", stats, err)
	}
	if player.commits != 1 {
		t.Fatalf("legacy participant re-executed: %+v", player)
	}
}

// TestLegacyRepositoryRoundTripsGenericView checks the adapter conversions.
func TestLegacyRepositoryRoundTripsGenericView(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	legacyStore := NewMemoryStore()
	repo, err := NewLegacyRepository(legacyStore, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	legacy := testWorkflowRecord("op-legacy-rt", workflowpb.WorkflowKind_HARVEST, 7, 11, testHash(2), now)
	legacy.NextRetryAtMs = now.UnixMilli()
	if _, _, err := legacyStore.CreateOrLoad(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	generic, err := repo.Get(context.Background(), "LEGACY_HARVEST", legacy.WorkflowId)
	if err != nil {
		t.Fatal(err)
	}
	if generic.Address.WorkID != legacy.WorkflowId || generic.SubjectPlayerID != 11 ||
		generic.OperationID != "op-legacy-rt" || generic.State != WorkStateCreated {
		t.Fatalf("generic=%+v", generic)
	}
	back, err := legacyFromWorkRecord(generic)
	if err != nil {
		t.Fatal(err)
	}
	if back.WorkflowId != legacy.WorkflowId || back.Kind != workflowpb.WorkflowKind_HARVEST ||
		back.FarmId != 7 || back.InitiatorPlayerId != 11 || string(back.PayloadHash) != string(legacy.PayloadHash) {
		t.Fatalf("round trip mismatch: %+v", back)
	}
	if _, err := repo.RenewLease(context.Background(), "LEGACY_HARVEST", legacy.WorkflowId, "w", "t", 1, time.Second); err == nil {
		t.Fatal("legacy renew unexpectedly supported")
	}
}
