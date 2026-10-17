package ingress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/metadata"
)

type privateStateCountingAuthorityStore struct {
	*authoritystore.FaultStore
	loads atomic.Uint64
}

type privateStateTestResolver struct {
	assignment actorrouting.Assignment
}

func (r privateStateTestResolver) Resolve(_ context.Context, _ actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

type privateStateTestLeaseAdmission struct {
	assignment actorrouting.Assignment
}

func (a privateStateTestLeaseAdmission) ValidateRouteGeneration(domain actorrouting.RouteDomain, shard uint32, holderID, holderBootID string, assignmentRevision, writerEpoch, leaseEpoch uint64) bool {
	want := a.assignment
	return domain == actorrouting.DomainPlayer && shard == want.ShardID && holderID == want.HolderPod &&
		holderBootID == want.HolderBootID && assignmentRevision == want.AssignmentRevision &&
		writerEpoch == want.WriterEpoch && leaseEpoch == want.LeaseEpoch
}

func (s *privateStateCountingAuthorityStore) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	s.loads.Add(1)
	return s.FaultStore.LoadAndClaim(ctx, req)
}

func (s *privateStateCountingAuthorityStore) AdmissionSnapshotReuseSafe() bool { return true }

func newPrivateStateHotpathService(t *testing.T, playerID uint64) (*Service, *privateStateCountingAuthorityStore, actorrouting.Assignment) {
	t.Helper()
	now := time.UnixMilli(bootstrapTestNowMs)
	calculator, err := actorrouting.NewCalculator(
		"private-state-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 1},
		1, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           "private-state-holder",
		ExpectedHolderBootID:       "private-state-holder-boot",
		ExpectedAssignmentRevision: 1,
		ExpectedWriterEpoch:        1,
		ExpectedLeaseEpoch:         1,
	}
	if err := memory.SetShardWriteAuthority(authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
		HolderID: fence.ExpectedHolderID, HolderBootID: fence.ExpectedHolderBootID,
		AssignmentRevision: fence.ExpectedAssignmentRevision, WriterEpoch: fence.ExpectedWriterEpoch,
		LeaseEpoch: fence.ExpectedLeaseEpoch, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	store := &privateStateCountingAuthorityStore{FaultStore: authoritystore.NewFaultStore(memory)}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1", BootID: fence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{
		MailboxSize: 8, Idle: playeractor.IdleOptions{Disable: true},
	})
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), playerID, "private-state-bootstrap"); err != nil {
		t.Fatal(err)
	}
	assignment := actorrouting.Assignment{
		Kind: actorrouting.DomainPlayer, ShardID: shard, HolderPod: fence.ExpectedHolderID,
		HolderBootID: fence.ExpectedHolderBootID, ShardRevision: fence.ExpectedAssignmentRevision,
		AssignmentRevision: fence.ExpectedAssignmentRevision, WriterEpoch: fence.ExpectedWriterEpoch,
		LeaseEpoch: fence.ExpectedLeaseEpoch, SnapshotRevision: 1,
	}
	service := NewService(registry)
	if err := service.SetPrivateStateRouteAdmission(privateStateTestResolver{assignment: assignment}, privateStateTestLeaseAdmission{assignment: assignment}, "private-state-test"); err != nil {
		t.Fatal(err)
	}
	return service, store, assignment
}

func privateStateIncomingRoute(t *testing.T, address actorrouting.Address, assignment actorrouting.Assignment) context.Context {
	t.Helper()
	outgoing := actorrouting.WithRoute(context.Background(), address, assignment)
	md, ok := metadata.FromOutgoingContext(outgoing)
	if !ok {
		t.Fatal("route metadata missing")
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestCompletePlayerRouteMetadataRequiresFullMatchingRoute(t *testing.T) {
	const playerID = uint64(71)
	assignment := actorrouting.Assignment{
		Kind: actorrouting.DomainPlayer, ShardID: 0, HolderPod: "holder", HolderBootID: "boot",
		ShardRevision: 2, WriterEpoch: 3, LeaseEpoch: 4, SnapshotRevision: 5,
	}
	address := actorrouting.Address{Realm: "realm", Kind: actorrouting.DomainPlayer, ID: playerID}
	missingHolderBoot := assignment
	missingHolderBoot.HolderBootID = ""
	missingLeaseGeneration := assignment
	missingLeaseGeneration.LeaseEpoch = 0
	missingSnapshotRevision := assignment
	missingSnapshotRevision.SnapshotRevision = 0
	cases := []struct {
		name       string
		ctx        context.Context
		playerID   uint64
		wantAccept bool
	}{
		{name: "complete route", ctx: privateStateIncomingRoute(t, address, assignment), playerID: playerID, wantAccept: true},
		{name: "no metadata", ctx: context.Background(), playerID: playerID},
		{name: "actor id mismatch", ctx: privateStateIncomingRoute(t, address, assignment), playerID: playerID + 1},
		{name: "wrong domain", ctx: privateStateIncomingRoute(t,
			actorrouting.Address{Realm: address.Realm, Kind: actorrouting.DomainFarm, ID: playerID},
			actorrouting.Assignment{Kind: actorrouting.DomainFarm, ShardID: assignment.ShardID, HolderPod: assignment.HolderPod,
				HolderBootID: assignment.HolderBootID, ShardRevision: assignment.ShardRevision, WriterEpoch: assignment.WriterEpoch,
				LeaseEpoch: assignment.LeaseEpoch, SnapshotRevision: assignment.SnapshotRevision}), playerID: playerID},
		{name: "missing holder boot", ctx: privateStateIncomingRoute(t, address, missingHolderBoot), playerID: playerID},
		{name: "missing lease generation", ctx: privateStateIncomingRoute(t, address, missingLeaseGeneration), playerID: playerID},
		{name: "missing snapshot revision", ctx: privateStateIncomingRoute(t, address, missingSnapshotRevision), playerID: playerID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := completePlayerRouteMetadata(tc.ctx, tc.playerID); got != tc.wantAccept {
				t.Fatalf("completePlayerRouteMetadata=%v want=%v", got, tc.wantAccept)
			}
		})
	}
}

func TestGetPrivateStateUsesResidentOnlyForCompleteRoutedCall(t *testing.T) {
	const playerID = uint64(72)
	service, store, assignment := newPrivateStateHotpathService(t, playerID)
	req := &logicrpcpb.GetPrivateStateRequest{PlayerId: playerID}
	address := actorrouting.Address{Realm: "private-state-test", Kind: actorrouting.DomainPlayer, ID: playerID}

	before := store.loads.Load()
	if _, err := service.GetPrivateState(privateStateIncomingRoute(t, address, assignment), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before {
		t.Fatalf("complete routed read refreshed authority state: before=%d after=%d", before, got)
	}

	before = store.loads.Load()
	if _, err := service.GetPrivateState(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before+1 {
		t.Fatalf("unrouted compatibility read loads=%d want=%d", got, before+1)
	}

	before = store.loads.Load()
	if _, err := service.GetPrivateState(privateStateIncomingRoute(t,
		actorrouting.Address{Realm: address.Realm, Kind: address.Kind, ID: playerID + 1}, assignment), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before+1 {
		t.Fatalf("actor-mismatched route took resident fast path: before=%d after=%d", before, got)
	}

	incomplete := assignment
	incomplete.LeaseEpoch = 0
	before = store.loads.Load()
	if _, err := service.GetPrivateState(privateStateIncomingRoute(t, address, incomplete), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before+1 {
		t.Fatalf("incomplete route took resident fast path: before=%d after=%d", before, got)
	}

	stale := assignment
	stale.WriterEpoch++
	before = store.loads.Load()
	if _, err := service.GetPrivateState(privateStateIncomingRoute(t, address, stale), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before+1 {
		t.Fatalf("stale route took resident fast path: before=%d after=%d", before, got)
	}

	service.privateStateLease = privateStateTestLeaseAdmission{assignment: actorrouting.Assignment{
		Kind: actorrouting.DomainPlayer, ShardID: assignment.ShardID, HolderPod: "other-holder",
		HolderBootID: "other-boot", AssignmentRevision: assignment.AssignmentRevision,
		WriterEpoch: assignment.WriterEpoch, LeaseEpoch: assignment.LeaseEpoch,
	}}
	before = store.loads.Load()
	if _, err := service.GetPrivateState(privateStateIncomingRoute(t, address, assignment), req); err != nil {
		t.Fatal(err)
	}
	if got := store.loads.Load(); got != before+1 {
		t.Fatalf("foreign holder route took resident fast path: before=%d after=%d", before, got)
	}
}
