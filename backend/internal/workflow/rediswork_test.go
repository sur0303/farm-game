package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type redisFixture struct {
	server *miniredis.Miniredis
	client *redis.Client
	repo   *RedisWorkRepository
	now    time.Time
}

func newRedisFixture(t *testing.T) *redisFixture {
	t.Helper()
	server := miniredis.RunT(t)
	f := &redisFixture{server: server, now: time.UnixMilli(1_800_000_000_000)}
	server.SetTime(f.now)
	f.client = redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = f.client.Close() })
	repo, err := NewRedisWorkRepository(f.client, fmt.Sprintf("farm:test:wf1:%d", f.now.UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	f.repo = repo
	return f
}

func (f *redisFixture) advance(d time.Duration) {
	f.now = f.now.Add(d)
	// miniredis FastForward only ages TTLs; SetTime moves the clock that
	// TIME (and therefore the lease Lua) observes.
	f.server.SetTime(f.now)
}

func (f *redisFixture) engine(t *testing.T, defs []Definition) *Engine {
	t.Helper()
	engine, err := NewEngine(f.repo, defs, WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestRedisWorkRepositoryCreateAdvanceAndConflict(t *testing.T) {
	f := newRedisFixture(t)
	ctx := context.Background()
	candidate := mustWorkRecord(t, "op-redis-1", f.now)
	created, inserted, err := f.repo.CreateIfAbsent(ctx, candidate)
	if err != nil || !inserted || created.WorkVersion != 1 {
		t.Fatalf("created=%v inserted=%v err=%v", created, inserted, err)
	}
	loaded, inserted, err := f.repo.CreateIfAbsent(ctx, candidate)
	if err != nil || inserted || loaded.WorkVersion != 1 {
		t.Fatalf("loaded=%v inserted=%v err=%v", loaded, inserted, err)
	}
	conflict := mustWorkRecord(t, "op-redis-1", f.now)
	conflict.PayloadHash = testPayloadHash("different")
	if _, _, err := f.repo.CreateIfAbsent(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict accepted: %v", err)
	}

	next := cloneWorkRecord(loaded)
	next.State = WorkStateRunning
	if _, err := f.repo.Advance(ctx, next, AdvanceOptions{ExpectedWorkVersion: 99}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale version accepted: %v", err)
	}
	updated, err := f.repo.Advance(ctx, next, AdvanceOptions{ExpectedWorkVersion: 1})
	if err != nil || updated.WorkVersion != 2 {
		t.Fatalf("updated=%v err=%v", updated, err)
	}
	terminal := cloneWorkRecord(updated)
	terminal.State = WorkStateCompleted
	completed, err := f.repo.Advance(ctx, terminal, AdvanceOptions{ExpectedWorkVersion: 2})
	if err != nil || completed.WorkVersion != 3 {
		t.Fatalf("completed=%v err=%v", completed, err)
	}
	stored, err := f.repo.Get(ctx, completed.Address.WorkType, completed.Address.WorkID)
	if err != nil || stored.State != WorkStateCompleted || stored.CompletedAtMs == 0 {
		t.Fatalf("stored=%v err=%v", stored, err)
	}
	if _, err := f.repo.Advance(ctx, cloneWorkRecord(stored), AdvanceOptions{ExpectedWorkVersion: 3}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("terminal mutated: %v", err)
	}
	if pending, err := f.repo.PendingDue(ctx); err != nil || pending != 0 {
		t.Fatalf("terminal still due: %d err=%v", pending, err)
	}
}

func TestRedisWorkRepositoryClaimFenceRenewAndReclaim(t *testing.T) {
	f := newRedisFixture(t)
	ctx := context.Background()
	record, _, err := f.repo.CreateIfAbsent(ctx, mustWorkRecord(t, "op-redis-2", f.now))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := f.repo.ClaimDue(ctx, "worker-a", "token-a", 4, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	leaseA := claimed[0].Record
	if leaseA.LeaseOwner != "worker-a" || leaseA.LeaseEpoch != 1 || leaseA.WorkVersion != record.WorkVersion+1 {
		t.Fatalf("lease=%v", leaseA)
	}
	if claimed[0].AcceptedTTL != 5*time.Second {
		t.Fatalf("accepted ttl=%v", claimed[0].AcceptedTTL)
	}
	if again, _ := f.repo.ClaimDue(ctx, "worker-b", "token-b", 4, 5*time.Second); len(again) != 0 {
		t.Fatalf("live lease did not hide record: %v", again)
	}
	online := cloneWorkRecord(leaseA)
	online.State = WorkStateRunning
	if _, err := f.repo.Advance(ctx, online, AdvanceOptions{ExpectedWorkVersion: leaseA.WorkVersion}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unleased advance bypassed lease: %v", err)
	}

	// Wrong token cannot renew; right token extends expiry without version bump.
	if _, err := f.repo.RenewLease(ctx, leaseA.Address.WorkType, leaseA.Address.WorkID,
		"worker-a", "wrong", leaseA.LeaseEpoch, 5*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong token renewed: %v", err)
	}
	f.advance(2 * time.Second)
	renewed, err := f.repo.RenewLease(ctx, leaseA.Address.WorkType, leaseA.Address.WorkID,
		"worker-a", "token-a", leaseA.LeaseEpoch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.WorkVersion != leaseA.WorkVersion {
		t.Fatalf("renew bumped version: %d→%d", leaseA.WorkVersion, renewed.WorkVersion)
	}

	// After the renewed lease expires, worker B reclaims with a higher epoch.
	f.advance(6 * time.Second)
	reclaimed, err := f.repo.ClaimDue(ctx, "worker-b", "token-b", 4, 5*time.Second)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaimed=%v err=%v", reclaimed, err)
	}
	if reclaimed[0].Record.LeaseEpoch != 2 {
		t.Fatalf("reclaim=%v", reclaimed[0].Record)
	}
	stale := cloneWorkRecord(leaseA)
	stale.State = WorkStateRunning
	if _, err := f.repo.Advance(ctx, stale, AdvanceOptions{
		ExpectedWorkVersion: leaseA.WorkVersion,
		LeaseOwner:          "worker-a",
		LeaseEpoch:          leaseA.LeaseEpoch,
	}); !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale worker advanced: %v", err)
	}
}

func TestRedisTypedClaimDoesNotStealTheftLease(t *testing.T) {
	f := newRedisFixture(t)
	ctx := context.Background()
	supported := mustWorkRecord(t, "op-redis-typed", f.now)
	if _, _, err := f.repo.CreateIfAbsent(ctx, supported); err != nil {
		t.Fatal(err)
	}
	// Exercise upgrade compatibility: filtered claim recovers the type from
	// the immutable stored record when the newer work_type hash field is absent.
	supportedKey, err := f.repo.recordKey(supported.Address.WorkType, supported.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.client.HDel(ctx, supportedKey, "work_type").Err(); err != nil {
		t.Fatal(err)
	}
	theft, err := NewWorkRecord(testAddress("THEFT", "op-redis-theft"), 9001, "op-redis-theft",
		[]byte(`{"x":1}`), testPayloadHash("op-redis-theft"), []string{"FARM"}, f.now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.repo.CreateIfAbsent(ctx, theft); err != nil {
		t.Fatal(err)
	}

	claimed, err := f.repo.ClaimDueByTypes(ctx, "generic-worker", "generic-token", []string{testWorkType}, 4, 5*time.Second)
	if err != nil || len(claimed) != 1 || claimed[0].Record.Address.WorkType != testWorkType {
		t.Fatalf("typed claim=%+v err=%v", claimed, err)
	}
	storedTheft, err := f.repo.Get(ctx, "THEFT", theft.Address.WorkID)
	if err != nil || storedTheft.LeaseOwner != "" || storedTheft.LeaseEpoch != 0 {
		t.Fatalf("THEFT lease was stolen: record=%+v err=%v", storedTheft, err)
	}
	if _, err := f.repo.ClaimWork(ctx, "THEFT", theft.Address.WorkID, "theft-worker", "theft-token", 5*time.Second); err != nil {
		t.Fatalf("THEFT recovery root could not claim immediately: %v", err)
	}
}

// TestRedisRecoveryMainline proves the whole generic framework over real
// Lua: create → dropped dispatch → crash → recovery from the due index with
// exactly-once participant effects.
func TestRedisRecoveryMainline(t *testing.T) {
	f := newRedisFixture(t)
	ctx := context.Background()
	p1 := newFakeParticipant("p1")
	p2 := newFakeParticipant("p2")
	def := newTestWorkflowDefinition(p1, p2)
	engine := f.engine(t, []Definition{def})

	record, created, err := engine.Create(ctx, def, testAddress(testWorkType, "op-redis-mainline"),
		9001, "op-redis-mainline", []byte(`{"x":1}`), testPayloadHash("op-redis-mainline"), time.Time{})
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	commitInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepCommit)
	p2.script(commitInvocation, faultApplyLoseResponse)

	// First fast-path attempt crashes mid-workflow (response lost at COMMIT).
	if err := engine.Resume(ctx, record, LeaseFence{}, nil); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("resume=%v", err)
	}
	stored, err := f.repo.Get(ctx, testWorkType, record.Address.WorkID)
	if err != nil || stored.State != WorkStateStepUnknown {
		t.Fatalf("stored=%v err=%v", stored, err)
	}

	// Process crash: a brand-new engine + recovery loop over the same Redis.
	f.advance(2 * time.Second)
	engine2 := f.engine(t, []Definition{def})
	loop, err := NewRecoveryLoop(f.repo, map[string]WorkHandler{testWorkType: EngineHandler{Engine: engine2}},
		RecoveryConfig{Owner: "worker-1", Token: "token-1", LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := loop.RunOnce(ctx)
	if err != nil || stats.Completed != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	stored, err = f.repo.Get(ctx, testWorkType, record.Address.WorkID)
	if err != nil || stored.State != WorkStateCompleted {
		t.Fatalf("stored=%v err=%v", stored, err)
	}
	if got := p2.applyCount(commitInvocation); got != 1 {
		t.Fatalf("commit applied %d times", got)
	}
	if p1.totalApplies() != 2 || p2.totalApplies() != 1 {
		t.Fatalf("applies p1=%d p2=%d", p1.totalApplies(), p2.totalApplies())
	}
	if pending, _ := f.repo.PendingDue(ctx); pending != 0 {
		t.Fatalf("pending=%d", pending)
	}
	// A second recovery round finds nothing.
	stats, err = loop.RunOnce(ctx)
	if err != nil || stats.Claimed != 0 {
		t.Fatalf("second=%+v err=%v", stats, err)
	}
}
