package main

import (
	"context"
	"sync"
	"testing"
	"time"

	workerreadiness "farmgame/internal/worker/readiness"
)

func TestMailObservedSourcesHaveIndependentTimers(t *testing.T) {
	registry := mailTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	fastRounds := make(chan struct{}, 4)
	var once sync.Once
	started := startMailObservedLoops(ctx, registry, nil, []mailObservedSource{
		{name: "claim_projection", interval: 10 * time.Millisecond, staleAfter: time.Second, scan: func(scanCtx context.Context) observedRound {
			once.Do(func() { close(blocked) })
			<-scanCtx.Done()
			return observedRound{err: scanCtx.Err()}
		}},
		{name: "maintenance", interval: 10 * time.Millisecond, staleAfter: time.Second, scan: func(context.Context) observedRound {
			select {
			case fastRounds <- struct{}{}:
			default:
			}
			return healthyMailRound()
		}},
	})
	<-started
	<-blocked
	for round := 0; round < 2; round++ {
		select {
		case <-fastRounds:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("maintenance timer was blocked by claim projection recovery")
		}
	}
	if state := familySnapshot(registry.Snapshot(), workerreadiness.FamilyMail).State; state != workerreadiness.StateDegraded {
		t.Fatalf("pending blocked source state=%s want DEGRADED", state)
	}
}

func TestMailObservedSourcesAggregateOnlyAfterAllSucceed(t *testing.T) {
	registry := mailTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := startMailObservedLoops(ctx, registry, nil, []mailObservedSource{
		{name: "claim_projection", interval: time.Second, staleAfter: time.Second, scan: func(context.Context) observedRound { return healthyMailRound() }},
		{name: "maintenance", interval: time.Second, staleAfter: time.Second, scan: func(context.Context) observedRound { return healthyMailRound() }},
	})
	<-started
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if familySnapshot(registry.Snapshot(), workerreadiness.FamilyMail).State == workerreadiness.StateReady {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("MAIL did not become ready after both sources succeeded: %+v", familySnapshot(registry.Snapshot(), workerreadiness.FamilyMail))
}

func TestMailObservedSourcesDeliveryFailureDegradesCombinedReadiness(t *testing.T) {
	registry := mailTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := startMailObservedLoops(ctx, registry, nil, []mailObservedSource{
		{name: "claim_projection", interval: time.Second, staleAfter: time.Second, scan: func(context.Context) observedRound { return healthyMailRound() }},
		{name: "maintenance", interval: time.Second, staleAfter: time.Second, scan: func(context.Context) observedRound { return healthyMailRound() }},
		{name: "delivery", interval: time.Second, staleAfter: time.Second, scan: func(context.Context) observedRound {
			return observedRound{err: context.DeadlineExceeded}
		}},
	})
	<-started
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if familySnapshot(registry.Snapshot(), workerreadiness.FamilyMail).State == workerreadiness.StateDegraded {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("MAIL did not degrade when delivery source failed: %+v", familySnapshot(registry.Snapshot(), workerreadiness.FamilyMail))
}

func mailTestRegistry(t *testing.T) *workerreadiness.Registry {
	t.Helper()
	registry, err := workerreadiness.NewRegistry(workerFamilyConfigs(time.Second, true, false, true), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.SetDependencies(workerreadiness.FamilyMail, workerreadiness.Dependencies{
		RepositoryInitialized: true, RouteProviderUsable: true, ParticipantClientUsable: true,
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func healthyMailRound() observedRound {
	return observedRound{
		stats: map[workerreadiness.Family]workerreadiness.ScanStats{workerreadiness.FamilyMail: {}},
		due: map[workerreadiness.Family]workerreadiness.DueObservation{
			workerreadiness.FamilyMail: {DepthAvailable: true, OldestDueAgeAvailable: true},
		},
	}
}
