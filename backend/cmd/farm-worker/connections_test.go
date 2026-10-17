package main

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workerreadiness "farmgame/internal/worker/readiness"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/connectivity"
)

type fakeHolderConnection struct {
	mu       sync.Mutex
	state    connectivity.State
	changed  chan struct{}
	connects int
}

func newFakeHolderConnection(state connectivity.State) *fakeHolderConnection {
	return &fakeHolderConnection{state: state, changed: make(chan struct{})}
}

func (c *fakeHolderConnection) Connect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connects++
	if c.state == connectivity.Idle {
		c.setStateLocked(connectivity.Connecting)
	}
}

func (c *fakeHolderConnection) GetState() connectivity.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *fakeHolderConnection) WaitForStateChange(ctx context.Context, source connectivity.State) bool {
	c.mu.Lock()
	if c.state != source {
		c.mu.Unlock()
		return true
	}
	changed := c.changed
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-changed:
		return true
	}
}

func (c *fakeHolderConnection) SetState(state connectivity.State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setStateLocked(state)
}

func (c *fakeHolderConnection) setStateLocked(state connectivity.State) {
	if state == c.state {
		return
	}
	c.state = state
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *fakeHolderConnection) ConnectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connects
}

func TestParticipantConnectionsFailClosedTransitionRecoverAndKeepLiveness(t *testing.T) {
	registry := mainTestRegistry(t)
	plantLoop, err := registry.StartLoop(workerreadiness.FamilyPlant)
	if err != nil {
		t.Fatal(err)
	}
	plantLoop.ScanSucceeded(time.Now(), workerreadiness.DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true})
	base := map[workerreadiness.Family]workerreadiness.Dependencies{
		workerreadiness.FamilyPlant: {
			RepositoryInitialized: true, RouteProviderUsable: true,
		},
		workerreadiness.FamilyHarvest: {},
		workerreadiness.FamilyCare:    {},
		workerreadiness.FamilyTheft:   {},
	}
	playerA := newFakeHolderConnection(connectivity.Idle)
	playerB := newFakeHolderConnection(connectivity.Idle)
	farm := newFakeHolderConnection(connectivity.Idle)
	monitor, err := newParticipantConnectionMonitor(registry, base, []holderConnection{playerA, playerB}, []holderConnection{farm})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	applied := make(chan struct{}, 8)
	stopped := make(chan struct{})
	go monitor.run(ctx, started, applied, stopped)
	<-started
	if playerA.ConnectCount() == 0 || playerB.ConnectCount() == 0 || farm.ConnectCount() == 0 {
		t.Fatalf("explicit Connect was not requested player=%d/%d farm=%d", playerA.ConnectCount(), playerB.ConnectCount(), farm.ConnectCount())
	}
	health := newHealthServer("", prometheus.NewRegistry(), registry)
	assertParticipantState(t, registry, workerreadiness.FamilyPlant, false)
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	drainSignals(applied)

	playerA.SetState(connectivity.Ready)
	farm.SetState(connectivity.Ready)
	waitForMonitorApply(t, applied)
	for _, family := range ownedCoreFamilies() {
		assertParticipantState(t, registry, family, false)
	}
	playerB.SetState(connectivity.Ready)
	waitForParticipantState(t, registry, workerreadiness.FamilyPlant, true, applied)
	for _, family := range ownedCoreFamilies() {
		assertParticipantState(t, registry, family, true)
	}
	assertStatus(t, health.Handler, "/readyz", http.StatusOK)

	playerA.SetState(connectivity.TransientFailure)
	waitForParticipantState(t, registry, workerreadiness.FamilyPlant, false, applied)
	for _, family := range ownedCoreFamilies() {
		assertParticipantState(t, registry, family, false)
	}
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	playerA.SetState(connectivity.Ready)
	waitForParticipantState(t, registry, workerreadiness.FamilyPlant, true, applied)
	assertStatus(t, health.Handler, "/readyz", http.StatusOK)

	farm.SetState(connectivity.Shutdown)
	waitForParticipantState(t, registry, workerreadiness.FamilyPlant, false, applied)
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	cancel()
	<-stopped
}

func TestPooledParticipantConnectionTracksCompleteGenerationHealth(t *testing.T) {
	var healthy atomic.Bool
	connection := &pooledParticipantConnection{healthy: healthy.Load}
	if got := connection.GetState(); got != connectivity.TransientFailure {
		t.Fatalf("initial state=%s", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	changed := make(chan bool, 1)
	go func() { changed <- connection.WaitForStateChange(ctx, connectivity.TransientFailure) }()
	healthy.Store(true)
	if !<-changed || connection.GetState() != connectivity.Ready {
		t.Fatal("pooled generation health did not transition to READY")
	}
	healthy.Store(false)
	if connection.GetState() != connectivity.TransientFailure {
		t.Fatal("pooled generation health did not fail closed")
	}
}

func TestEmptyHolderConnectionSetIsNeverUsable(t *testing.T) {
	if holderConnectionsUsable(nil) || holderConnectionsUsable([]holderConnection{}) {
		t.Fatal("empty Holder connection set was reported usable")
	}
}

func waitForParticipantState(t *testing.T, registry *workerreadiness.Registry, family workerreadiness.Family, want bool, applied <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		if familySnapshot(registry.Snapshot(), family).ParticipantClientUsable == want {
			return
		}
		select {
		case <-applied:
		case <-timer.C:
			t.Fatalf("Participant Client usable did not become %t: %+v", want, familySnapshot(registry.Snapshot(), family))
		}
	}
}

func waitForMonitorApply(t *testing.T, applied <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-applied:
	case <-timer.C:
		t.Fatal("connection monitor did not apply a state transition")
	}
}

func assertParticipantState(t *testing.T, registry *workerreadiness.Registry, family workerreadiness.Family, want bool) {
	t.Helper()
	if item := familySnapshot(registry.Snapshot(), family); item.ParticipantClientUsable != want {
		t.Fatalf("Participant Client usable=%t want=%t Family=%+v", item.ParticipantClientUsable, want, item)
	}
}

func drainSignals(signals <-chan struct{}) {
	for {
		select {
		case <-signals:
		default:
			return
		}
	}
}

func ownedCoreFamilies() []workerreadiness.Family {
	return []workerreadiness.Family{
		workerreadiness.FamilyPlant,
		workerreadiness.FamilyHarvest,
		workerreadiness.FamilyCare,
		workerreadiness.FamilyTheft,
	}
}
