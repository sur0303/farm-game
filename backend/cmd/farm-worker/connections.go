package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	workerreadiness "farmgame/internal/worker/readiness"

	"google.golang.org/grpc/connectivity"
)

const maxHolderConnectionsPerDomain = 4096

// holderConnection is the small production/test seam needed to observe an
// exact static Holder connection set. *grpc.ClientConn implements it.
type holderConnection interface {
	Connect()
	GetState() connectivity.State
	WaitForStateChange(context.Context, connectivity.State) bool
}

// pooledParticipantConnection adapts the dynamic complete-generation endpoint
// pool to the existing Family readiness monitor. Assignment health proves the
// Watch generation is current; pool health proves every pre-dialed gRPC
// connection in that generation is READY.
type pooledParticipantConnection struct {
	healthy func() bool
}

func (*pooledParticipantConnection) Connect() {}

func (c *pooledParticipantConnection) GetState() connectivity.State {
	if c != nil && c.healthy != nil && c.healthy() {
		return connectivity.Ready
	}
	return connectivity.TransientFailure
}

func (c *pooledParticipantConnection) WaitForStateChange(ctx context.Context, source connectivity.State) bool {
	if ctx == nil {
		return false
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.GetState() != source {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

type participantRequirement struct {
	family workerreadiness.Family
	player bool
	farm   bool
}

var ownedParticipantRequirements = [...]participantRequirement{
	{family: workerreadiness.FamilyPlant, player: true, farm: true},
	{family: workerreadiness.FamilyHarvest, player: true, farm: true},
	{family: workerreadiness.FamilyCare, player: true, farm: true},
	{family: workerreadiness.FamilyTheft, player: true, farm: true},
}

type participantConnectionMonitor struct {
	registry *workerreadiness.Registry
	base     map[workerreadiness.Family]workerreadiness.Dependencies
	player   []holderConnection
	farm     []holderConnection
}

func newParticipantConnectionMonitor(registry *workerreadiness.Registry,
	base map[workerreadiness.Family]workerreadiness.Dependencies,
	player, farm []holderConnection) (*participantConnectionMonitor, error) {
	if registry == nil || len(player) > maxHolderConnectionsPerDomain || len(farm) > maxHolderConnectionsPerDomain {
		return nil, fmt.Errorf("farm-worker Holder connection readiness: invalid bounded configuration")
	}
	for _, connection := range append(append([]holderConnection(nil), player...), farm...) {
		if connection == nil {
			return nil, fmt.Errorf("farm-worker Holder connection readiness: nil connection")
		}
	}
	baseCopy := make(map[workerreadiness.Family]workerreadiness.Dependencies, len(base))
	for family, dependencies := range base {
		baseCopy[family] = dependencies
	}
	for _, requirement := range ownedParticipantRequirements {
		if _, ok := baseCopy[requirement.family]; !ok {
			return nil, fmt.Errorf("farm-worker Holder connection readiness: missing Family %s", requirement.family)
		}
	}
	return &participantConnectionMonitor{
		registry: registry,
		base:     baseCopy,
		player:   append([]holderConnection(nil), player...),
		farm:     append([]holderConnection(nil), farm...),
	}, nil
}

func startParticipantConnectionMonitor(ctx context.Context, monitor *participantConnectionMonitor) <-chan struct{} {
	started := make(chan struct{})
	go monitor.run(ctx, started, nil, nil)
	return started
}

// run closes started only after Connect has been requested for every Holder,
// every watcher is installed, and the initial fail-closed dependency snapshot
// has been applied. applied/stopped are deterministic test barriers.
func (m *participantConnectionMonitor) run(ctx context.Context, started chan<- struct{}, applied chan<- struct{}, stopped chan<- struct{}) {
	if stopped != nil {
		defer close(stopped)
	}
	connections := append(append([]holderConnection(nil), m.player...), m.farm...)
	for _, connection := range connections {
		connection.Connect()
	}

	updates := make(chan struct{}, len(connections)+1)
	var watchersStarted sync.WaitGroup
	var watchersStopped sync.WaitGroup
	watchersStarted.Add(len(connections))
	watchersStopped.Add(len(connections))
	for _, connection := range connections {
		connection := connection
		go watchHolderConnection(ctx, connection, updates, &watchersStarted, &watchersStopped)
	}
	watchersStarted.Wait()
	m.apply(applied)
	if started != nil {
		close(started)
	}

	for {
		select {
		case <-ctx.Done():
			watchersStopped.Wait()
			return
		case <-updates:
			m.apply(applied)
		}
	}
}

func watchHolderConnection(ctx context.Context, connection holderConnection, updates chan<- struct{}, started, stopped *sync.WaitGroup) {
	defer stopped.Done()
	state := connection.GetState()
	if state == connectivity.Idle {
		connection.Connect()
		state = connection.GetState()
	}
	started.Done()
	for connection.WaitForStateChange(ctx, state) {
		notifyConnectionChange(updates)
		state = connection.GetState()
		if state == connectivity.Idle {
			connection.Connect()
			state = connection.GetState()
		}
	}
}

func notifyConnectionChange(channel chan<- struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (m *participantConnectionMonitor) apply(applied chan<- struct{}) {
	playerUsable := holderConnectionsUsable(m.player)
	farmUsable := holderConnectionsUsable(m.farm)
	for _, requirement := range ownedParticipantRequirements {
		dependencies := m.base[requirement.family]
		dependencies.ParticipantClientUsable = (!requirement.player || playerUsable) && (!requirement.farm || farmUsable)
		if err := m.registry.SetDependencies(requirement.family, dependencies); err != nil {
			log.Printf("update %s Participant Client readiness: %v", requirement.family, err)
		}
	}
	if applied != nil {
		notifyConnectionChange(applied)
	}
}

func holderConnectionsUsable(connections []holderConnection) bool {
	if len(connections) == 0 {
		return false
	}
	for _, connection := range connections {
		if connection == nil || connection.GetState() != connectivity.Ready {
			return false
		}
	}
	return true
}
