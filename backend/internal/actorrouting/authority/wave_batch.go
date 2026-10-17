package authority

import (
	"context"
	"fmt"
	"reflect"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
)

type authorityWavePublisher interface {
	BeginAuthorityWaveBatch(context.Context, string, actorrouting.RouteSnapshot, []routecontrol.AuthorityWaveMember) error
	AuthorityWaveBatchBaseRevision(string) (uint64, bool)
	AuthorityWaveBatchStage(string) (routecontrol.AuthorityWaveBatchStage, bool)
	AuthorityWaveBatchMemberStage(routecontrol.AuthorityWaveMember) (routecontrol.AuthorityWaveBatchStage, bool)
	AuthorityWaveBatchContains(string, routecontrol.AuthorityWaveMember) bool
	SealAuthorityWaveBatch(string) (routecontrol.AuthorityWaveBatchCapability, int, error)
	CommitAuthorityWaveBatch(context.Context, routecontrol.NestedControlOperationToken, routecontrol.AuthorityWaveBatchCapability) (routecontrol.SnapshotCASResult, error)
	AbortEmptyAuthorityWaveBatch(string) error
}

// BeginAuthorityWaveBatch opens an in-process publication buffer around an
// exact bounded ScalePlan wave. Authority transitions remain individually
// durable; only their complete RouteSnapshot projections are coalesced.
func (m *RouteAssignmentManager) BeginAuthorityWaveBatch(ctx context.Context, waveID string, members []routecontrol.AuthorityWaveMember) error {
	if m == nil || ctx == nil || waveID == "" {
		return ErrInvalidManager
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return routecontrol.ErrAuthorityWaveBatchUnsupported
	}
	m.mu.Lock()
	initial := actorrouting.CloneRouteSnapshot(m.snapshot)
	m.mu.Unlock()
	return publisher.BeginAuthorityWaveBatch(ctx, waveID, initial, members)
}

func (m *RouteAssignmentManager) AuthorityWaveBatchBaseRevision(waveID string) (uint64, bool) {
	if m == nil || waveID == "" {
		return 0, false
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return 0, false
	}
	return publisher.AuthorityWaveBatchBaseRevision(waveID)
}

func (m *RouteAssignmentManager) AuthorityWaveBatchStage(waveID string) (routecontrol.AuthorityWaveBatchStage, bool) {
	if m == nil || waveID == "" {
		return routecontrol.AuthorityWaveBatchStageInvalid, false
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return routecontrol.AuthorityWaveBatchStageInvalid, false
	}
	return publisher.AuthorityWaveBatchStage(waveID)
}

func (m *RouteAssignmentManager) authorityWaveBatchMemberStage(domain actorrouting.RouteDomain, shard uint32) (routecontrol.AuthorityWaveBatchStage, bool) {
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return routecontrol.AuthorityWaveBatchStageInvalid, false
	}
	return publisher.AuthorityWaveBatchMemberStage(routecontrol.AuthorityWaveMember{Domain: domain, VirtualShard: shard})
}

func (m *RouteAssignmentManager) AuthorityWaveBatchContains(waveID string, domain actorrouting.RouteDomain, shard uint32) bool {
	if m == nil || waveID == "" {
		return false
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	return ok && publisher.AuthorityWaveBatchContains(waveID, routecontrol.AuthorityWaveMember{Domain: domain, VirtualShard: shard})
}

// CommitAuthorityWaveBatch seals one complete bounded stage and performs one
// durable jump-revision CAS. The executor never commits a partial stage:
// ASSIGNING is published for every member before target cold-load starts, then
// ACTIVE is published for every member in a second bounded stage.
func (m *RouteAssignmentManager) CommitAuthorityWaveBatch(ctx context.Context, waveID string) (int, routecontrol.SnapshotCASResult, error) {
	if m == nil || ctx == nil || waveID == "" {
		return 0, routecontrol.SnapshotCASResult{}, ErrInvalidManager
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return 0, routecontrol.SnapshotCASResult{}, routecontrol.ErrAuthorityWaveBatchUnsupported
	}
	capability, count, err := publisher.SealAuthorityWaveBatch(waveID)
	if err != nil {
		return 0, routecontrol.SnapshotCASResult{}, err
	}
	token, err := m.controlToken(ctx)
	if err != nil {
		return count, routecontrol.SnapshotCASResult{}, err
	}
	result, err := publisher.CommitAuthorityWaveBatch(ctx, token, capability)
	if result.Committed() {
		m.mu.Lock()
		local := actorrouting.CloneRouteSnapshot(m.snapshot)
		m.mu.Unlock()
		if !reflect.DeepEqual(local, result.Stored.Snapshot) {
			return count, result, fmt.Errorf("%w: committed wave differs from manager snapshot", ErrSnapshotPublish)
		}
	}
	return count, result, err
}

func (m *RouteAssignmentManager) AbortEmptyAuthorityWaveBatch(waveID string) error {
	if m == nil || waveID == "" {
		return ErrInvalidManager
	}
	publisher, ok := m.publisher.(authorityWavePublisher)
	if !ok {
		return routecontrol.ErrAuthorityWaveBatchUnsupported
	}
	return publisher.AbortEmptyAuthorityWaveBatch(waveID)
}
