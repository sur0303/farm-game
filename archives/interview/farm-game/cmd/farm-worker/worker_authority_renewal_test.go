package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/opportunityrecovery"
)

type recordingWorkerRenewStore struct {
	requests []authoritystore.RenewShardAuthorityRequest
	now      time.Time
}

func (s *recordingWorkerRenewStore) RenewShardAuthority(
	_ context.Context,
	request authoritystore.RenewShardAuthorityRequest,
) (authoritystore.ShardAuthorityTransitionResult, error) {
	s.requests = append(s.requests, request)
	return authoritystore.ShardAuthorityTransitionResult{
		Status: authoritystore.ShardTransitionApplied,
		Authority: authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: request.AuthorityLayoutVersion,
			HashVersion:            actorrouting.HashVersionXXH64V2,
			HashSeed:               1,
			Domain:                 request.Domain,
			VirtualShard:           request.VirtualShard,
			HolderID:               request.ExpectedFence.ExpectedHolderID,
			HolderBootID:           request.ExpectedFence.ExpectedHolderBootID,
			AssignmentRevision:     request.ExpectedFence.ExpectedAssignmentRevision,
			WriterEpoch:            request.ExpectedFence.ExpectedWriterEpoch,
			LeaseEpoch:             request.ExpectedFence.ExpectedLeaseEpoch,
			State:                  authoritystore.ShardAuthorityActive,
			LeaseUntilMs:           s.now.Add(time.Minute).UnixMilli(),
		},
	}, nil
}

type recordingWorkerHolderRegistry struct {
	record          core.HolderRecord
	registrations   []core.HolderRegistration
	heartbeats      []core.HolderHeartbeat
	getErrors       []error
	registerErrors  []error
	heartbeatErrors []error
	getCalls        int
}

type gatedAssignmentFixture struct {
	assignment opportunityrecovery.WorkerShardAssignment
	claims     int
}

type recordingWorkerLeaseObserver struct {
	leases []opportunityrecovery.WorkerShardLeaseView
}

func (o *recordingWorkerLeaseObserver) ObserveAuthorityLease(lease opportunityrecovery.WorkerShardLeaseView) error {
	o.leases = append(o.leases, lease)
	return nil
}

func (f *gatedAssignmentFixture) Assignment(
	context.Context,
) (opportunityrecovery.WorkerShardAssignment, error) {
	return f.assignment, nil
}

func (f *gatedAssignmentFixture) CanClaimShard(
	context.Context,
	uint32,
	uint64,
) error {
	f.claims++
	return nil
}

func (r *recordingWorkerHolderRegistry) Register(
	_ context.Context,
	registration core.HolderRegistration,
) (core.HolderRecord, error) {
	r.registrations = append(r.registrations, registration)
	if len(r.registerErrors) > 0 {
		err := r.registerErrors[0]
		r.registerErrors = r.registerErrors[1:]
		return core.HolderRecord{}, err
	}
	r.record = registration
	return r.record, nil
}

func (r *recordingWorkerHolderRegistry) Get(
	_ context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	r.getCalls++
	if len(r.getErrors) > 0 {
		err := r.getErrors[0]
		r.getErrors = r.getErrors[1:]
		return core.HolderRecord{}, false, err
	}
	if r.record.Domain != domain || r.record.HolderID != holderID {
		return core.HolderRecord{}, false, nil
	}
	return r.record, true, nil
}

func (r *recordingWorkerHolderRegistry) Heartbeat(
	_ context.Context,
	heartbeat core.HolderHeartbeat,
) (core.HolderRecord, error) {
	r.heartbeats = append(r.heartbeats, heartbeat)
	if len(r.heartbeatErrors) > 0 {
		err := r.heartbeatErrors[0]
		r.heartbeatErrors = r.heartbeatErrors[1:]
		return core.HolderRecord{}, err
	}
	r.record.State = heartbeat.State
	r.record.ProcessReady = heartbeat.ProcessReady
	r.record.ServingShardCount = heartbeat.ServingShardCount
	r.record.SnapshotRevision = heartbeat.SnapshotRevision
	r.record.HeartbeatAt = heartbeat.HeartbeatAt
	r.record.LeaseUntil = heartbeat.LeaseUntil
	r.record.Sequence = heartbeat.Sequence
	return r.record, nil
}

func TestFarmWorkerAuthorityRenewerRenewsOnlyOwnedDynamicShards(t *testing.T) {
	now := time.Now()
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{record: core.HolderRecord{
		Domain: actorrouting.DomainFarmWorker, HolderID: "farm-worker-a",
		BootID: "worker-a-boot", State: core.HolderStateReadyZeroShard,
		Sequence: 1, CapacityWeight: 1, HeartbeatAt: now.Add(-time.Second),
	}}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a:9201",
		"farm-worker-a:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		30*time.Second,
		10*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	observer := &recordingWorkerLeaseObserver{}
	renewer.SetLeaseObserver(observer)
	if err := renewer.renewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observer.leases) != 2 || observer.leases[0].Shard != 0 || observer.leases[1].Shard != 2 {
		t.Fatalf("renewed authority leases were not projected locally: %+v", observer.leases)
	}
	if len(store.requests) != 2 {
		t.Fatalf("renew requests=%+v", store.requests)
	}
	for index, shard := range []uint32{0, 2} {
		request := store.requests[index]
		if request.Domain != actorrouting.DomainFarmWorker ||
			request.VirtualShard != shard ||
			request.Caller.InstanceID != "farm-worker-a" ||
			request.Caller.BootID != "worker-a-boot" ||
			request.ExpectedFence.ExpectedAssignmentRevision != 1 ||
			request.ExpectedFence.ExpectedWriterEpoch != 1 ||
			request.ExpectedFence.ExpectedLeaseEpoch != 1 ||
			request.RequestedTTLms != time.Minute.Milliseconds() {
			t.Fatalf("request[%d]=%+v", index, request)
		}
	}
	if len(holders.heartbeats) != 1 {
		t.Fatalf("holder heartbeats=%+v", holders.heartbeats)
	}
	heartbeat := holders.heartbeats[0]
	if heartbeat.State != core.HolderStateServing ||
		heartbeat.ServingShardCount != 2 ||
		heartbeat.SnapshotRevision != source.snapshot.GlobalRevision ||
		heartbeat.Sequence != 2 ||
		!heartbeat.ProcessReady ||
		heartbeat.LeaseUntil != time.UnixMilli(now.UnixMilli()).
			Add(30*time.Second) {
		t.Fatalf("holder heartbeat=%+v", heartbeat)
	}
}

func TestFarmWorkerAuthorityRenewerRequiresCrashTakeoverWindow(t *testing.T) {
	now := time.Now()
	_, err := newFarmWorkerAuthorityRenewer(
		&countingAssignmentProvider{snapshot: opportunityRouteSnapshot()},
		&recordingWorkerRenewStore{now: now},
		&recordingWorkerHolderRegistry{},
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a:9201",
		"farm-worker-a:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		40*time.Second,
		10*time.Second,
		time.Second,
	)
	if !errors.Is(err, errFarmWorkerAuthorityRenewal) {
		t.Fatalf("unsafe holder/authority expiry gap err=%v", err)
	}
}

func TestFarmWorkerAuthorityRenewerRejectsExpiredOwnedRoute(t *testing.T) {
	now := time.Now()
	snapshot := opportunityRouteSnapshot()
	snapshot.FarmWorker.Routes[0].LeaseUntilMs = now.Add(-time.Second).UnixMilli()
	source := &countingAssignmentProvider{snapshot: snapshot}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{record: core.HolderRecord{
		Domain: actorrouting.DomainFarmWorker, HolderID: "farm-worker-a",
		BootID: "worker-a-boot", State: core.HolderStateReadyZeroShard,
		Sequence: 1, CapacityWeight: 1, HeartbeatAt: now.Add(-time.Second),
	}}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a:9201",
		"farm-worker-a:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		30*time.Second,
		10*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	if err := renewer.renewOnce(context.Background()); err == nil {
		t.Fatal("expired owned route renewed")
	}
	if len(store.requests) != 0 {
		t.Fatalf("expired route reached Redis: %+v", store.requests)
	}
	if len(holders.heartbeats) != 0 {
		t.Fatalf("expired route heartbeated holder: %+v", holders.heartbeats)
	}
}

func TestFarmWorkerAuthorityRenewerDelegatedModeUsesRedisLeaseAfterSnapshotDeadline(t *testing.T) {
	now := time.Now()
	snapshot := opportunityRouteSnapshot()
	snapshot.FarmWorker.Routes[0].LeaseUntilMs = now.Add(-time.Second).UnixMilli()
	source := &countingAssignmentProvider{snapshot: snapshot}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{record: core.HolderRecord{
		Domain: actorrouting.DomainFarmWorker, HolderID: "farm-worker-a",
		BootID: "worker-a-boot", State: core.HolderStateReadyZeroShard,
		Sequence: 1, CapacityWeight: 1, HeartbeatAt: now.Add(-time.Second),
	}}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source, store, holders, "farm-worker-a", "worker-a-boot",
		"farm-worker-a:9201", "farm-worker-a:9201", "holder-control.test", "v1",
		time.Minute, 30*time.Second, 10*time.Second, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := renewer.SetLeaseCheckMode(actorrouting.LeaseCheckAuthorityDelegated); err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	if err := renewer.renewOnce(context.Background()); err != nil {
		t.Fatalf("delegated renewal rejected stale snapshot deadline: %v", err)
	}
	if len(store.requests) == 0 {
		t.Fatal("delegated renewal did not reach Redis Authority")
	}
}

func TestFarmWorkerAuthorityRenewerRegistersMissingHolderLifecycle(t *testing.T) {
	now := time.Now()
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a.test:9201",
		"farm-worker-a.test:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		30*time.Second,
		10*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	if err := renewer.renewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(holders.registrations) != 1 || len(holders.heartbeats) != 2 {
		t.Fatalf(
			"registrations=%+v heartbeats=%+v",
			holders.registrations,
			holders.heartbeats,
		)
	}
	registration := holders.registrations[0]
	if registration.LogicEndpoint != "farm-worker-a.test:9201" ||
		registration.HolderControlEndpoint != "farm-worker-a.test:9201" ||
		registration.TLSDNSIdentity != "holder-control.test" ||
		registration.LeaseUntil != time.UnixMilli(now.UnixMilli()).
			Add(30*time.Second) {
		t.Fatalf("registration=%+v", registration)
	}
	for name, value := range map[string]time.Time{
		"started":   registration.StartedAt,
		"heartbeat": registration.HeartbeatAt,
		"lease":     registration.LeaseUntil,
	} {
		if value != time.UnixMilli(value.UnixMilli()) {
			t.Fatalf("%s time is not Redis millisecond canonical: %s", name, value)
		}
	}
	if holders.heartbeats[0].State != core.HolderStateReadyZeroShard ||
		holders.heartbeats[0].ServingShardCount != 0 ||
		holders.heartbeats[1].State != core.HolderStateServing ||
		holders.heartbeats[1].ServingShardCount != 2 {
		t.Fatalf("holder lifecycle=%+v", holders.heartbeats)
	}
	for index, heartbeat := range holders.heartbeats {
		for name, value := range map[string]time.Time{
			"heartbeat": heartbeat.HeartbeatAt,
			"lease":     heartbeat.LeaseUntil,
		} {
			if value != time.UnixMilli(value.UnixMilli()) {
				t.Fatalf(
					"heartbeat[%d] %s time is not Redis millisecond canonical: %s",
					index,
					name,
					value,
				)
			}
		}
	}
}

func TestFarmWorkerAuthorityRenewerRetriesUnstableHolderRead(t *testing.T) {
	now := time.Now()
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{
		record: core.HolderRecord{
			Domain: actorrouting.DomainFarmWorker, HolderID: "farm-worker-a",
			BootID: "worker-a-boot", State: core.HolderStateReadyZeroShard,
			Sequence: 1, CapacityWeight: 1,
			HeartbeatAt: now.Add(-time.Second),
		},
		getErrors: []error{core.ErrHolderRegistryUnavailable},
	}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a.test:9201",
		"farm-worker-a.test:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		30*time.Second,
		5*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	if err := renewer.renewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if holders.getCalls != 2 || len(holders.heartbeats) != 1 {
		t.Fatalf(
			"get calls=%d heartbeats=%+v",
			holders.getCalls,
			holders.heartbeats,
		)
	}
}

func TestFarmWorkerAuthorityRenewerBootstrapOpensAssignmentGate(t *testing.T) {
	now := time.Now()
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a.test:9201",
		"farm-worker-a.test:9201",
		"holder-control.test",
		"v1",
		time.Minute,
		30*time.Second,
		10*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	base := &gatedAssignmentFixture{assignment: opportunityrecovery.WorkerShardAssignment{
		Revision: 13, OwnedShards: []uint32{0, 2},
	}}
	gated := &renewalGatedWorkerAssignment{
		provider: base, admission: base, renewer: renewer,
	}
	if _, err := gated.Assignment(context.Background()); !errors.Is(
		err,
		errFarmWorkerAuthorityRenewal,
	) {
		t.Fatalf("pre-bootstrap assignment err=%v", err)
	}
	if err := renewer.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !renewer.Healthy() {
		t.Fatal("renewer is unhealthy after successful bootstrap")
	}
	assignment, err := gated.Assignment(context.Background())
	if err != nil || assignment.Revision != 13 ||
		len(assignment.OwnedShards) != 2 {
		t.Fatalf("post-bootstrap assignment=%+v err=%v", assignment, err)
	}
	if len(store.requests) != 2 || len(holders.registrations) != 1 {
		t.Fatalf(
			"renew requests=%+v registrations=%+v",
			store.requests,
			holders.registrations,
		)
	}
}

func TestFarmWorkerAuthorityRenewerRetriesUnstableRegisterAndHeartbeat(t *testing.T) {
	now := time.Now()
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	store := &recordingWorkerRenewStore{now: now}
	holders := &recordingWorkerHolderRegistry{
		registerErrors:  []error{core.ErrHolderRegistryUnavailable},
		heartbeatErrors: []error{core.ErrHolderRegistryUnavailable},
	}
	renewer, err := newFarmWorkerAuthorityRenewer(
		source,
		store,
		holders,
		"farm-worker-a",
		"worker-a-boot",
		"farm-worker-a.test:9201",
		"farm-worker-a.test:9201",
		"holder-control.test",
		"authority-layout-v1",
		time.Minute,
		30*time.Second,
		10*time.Second,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewer.now = func() time.Time { return now }
	if err := renewer.renewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(holders.registrations) != 2 {
		t.Fatalf("registration attempts=%d", len(holders.registrations))
	}
	if len(holders.heartbeats) != 3 {
		t.Fatalf("heartbeat attempts=%d", len(holders.heartbeats))
	}
	if holders.record.State != core.HolderStateServing ||
		holders.record.SnapshotRevision != source.snapshot.GlobalRevision {
		t.Fatalf("record=%+v", holders.record)
	}
}

func TestRenewalGatedWorkerAssignmentFailsClosed(t *testing.T) {
	base := &gatedAssignmentFixture{assignment: opportunityrecovery.WorkerShardAssignment{
		Revision: 13, OwnedShards: []uint32{0, 2},
	}}
	renewer := &farmWorkerAuthorityRenewer{}
	gated := &renewalGatedWorkerAssignment{
		provider: base, admission: base, renewer: renewer,
	}
	if _, err := gated.Assignment(context.Background()); !errors.Is(
		err,
		errFarmWorkerAuthorityRenewal,
	) {
		t.Fatalf("unhealthy assignment err=%v", err)
	}
	if err := gated.CanClaimShard(context.Background(), 0, 13); !errors.Is(
		err,
		errFarmWorkerAuthorityRenewal,
	) {
		t.Fatalf("unhealthy claim err=%v", err)
	}
	renewer.healthy.Store(true)
	assignment, err := gated.Assignment(context.Background())
	if err != nil || assignment.Revision != 13 ||
		len(assignment.OwnedShards) != 2 {
		t.Fatalf("healthy assignment=%+v err=%v", assignment, err)
	}
	if err := gated.CanClaimShard(context.Background(), 0, 13); err != nil ||
		base.claims != 1 {
		t.Fatalf("healthy claim count=%d err=%v", base.claims, err)
	}
}
