package command

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
)

func firstHopSnapshot() actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 8, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040020,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7,
			ExpectedWriterEpoch: 9, ExpectedLeaseEpoch: 11, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 6,
			ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 10, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-boot", Endpoint: "player:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-boot", Endpoint: "farm:9001", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func firstHopRequest(domain logicrpcpb.RouteDomain, actorID uint64) *logicrpcpb.ExecuteCommandRequest {
	return &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_BUY},
		Dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY, ActorRoute: &logicrpcpb.ActorRouteHeader{
			RouteDomain: domain, ActorId: actorID,
		}},
	}
}

type testGenerationAdmission struct {
	allow  bool
	holder string
	boot   string
	assign uint64
	writer uint64
	lease  uint64
}

type cancelingGenerationAdmission struct {
	cancel context.CancelFunc
}

func (a cancelingGenerationAdmission) CanStartActorTurn(actorrouting.RouteDomain, uint32) bool {
	return true
}

func (a cancelingGenerationAdmission) ValidateRouteGeneration(actorrouting.RouteDomain, uint32, string, string, uint64, uint64, uint64) bool {
	a.cancel()
	return true
}

type countingResolver struct {
	assignment actorrouting.Assignment
	calls      int
}

func (r *countingResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	r.calls++
	return r.assignment, nil
}

func (a testGenerationAdmission) CanStartActorTurn(actorrouting.RouteDomain, uint32) bool {
	return a.allow
}

func (a testGenerationAdmission) ValidateRouteGeneration(_ actorrouting.RouteDomain, _ uint32, holderID, holderBootID string, assignmentRevision, writerEpoch, leaseEpoch uint64) bool {
	if !a.allow {
		return false
	}
	if a.holder == "" {
		return holderID != "" && holderBootID != "" && assignmentRevision != 0 && writerEpoch != 0 && leaseEpoch != 0
	}
	return holderID == a.holder && holderBootID == a.boot && assignmentRevision == a.assign && writerEpoch == a.writer && leaseEpoch == a.lease
}

func TestFirstHopCompletesAndValidatesFullRouteHeader(t *testing.T) {
	resolver, err := actorrouting.NewStaticResolver(firstHopSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	request, err := WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), assignment)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true, holder: assignment.HolderPod, boot: assignment.HolderBootID, assign: assignment.AssignmentRevision, writer: assignment.WriterEpoch, lease: assignment.LeaseEpoch},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := validator.ValidateBeforeAccept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	header := request.Dispatch.ActorRoute
	if got != assignment || header.VirtualShard != assignment.ShardID || header.ShardRevision != assignment.ShardRevision || header.ExpectedHolderId != assignment.HolderPod || header.ExpectedHolderBootId != assignment.HolderBootID || header.ExpectedWriterEpoch != assignment.WriterEpoch || header.SourceSnapshotRevision == nil || *header.SourceSnapshotRevision != assignment.SnapshotRevision {
		t.Fatalf("assignment/header mismatch: got=%+v assignment=%+v header=%+v", got, assignment, header)
	}
}

func TestFirstHopAllowsDeadlineOnlySnapshotRevisionWithSameRouteFence(t *testing.T) {
	base := firstHopSnapshot()
	resolver, err := actorrouting.NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	oldAssignment, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	request, err := WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), oldAssignment)
	if err != nil {
		t.Fatal(err)
	}
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs++
	if err := resolver.Replace(next); err != nil {
		t.Fatal(err)
	}
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true, holder: oldAssignment.HolderPod, boot: oldAssignment.HolderBootID, assign: oldAssignment.AssignmentRevision, writer: oldAssignment.WriterEpoch, lease: oldAssignment.LeaseEpoch},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := validator.Validate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got.SnapshotRevision != next.GlobalRevision || got.LeaseEpoch != oldAssignment.LeaseEpoch {
		t.Fatalf("deadline-only route was not accepted with the same fence: got=%+v", got)
	}
}

func TestFirstHopRejectsShortenedDeadlineAsDeadlineOnlyCompatibility(t *testing.T) {
	base := firstHopSnapshot()
	resolver, err := actorrouting.NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	request, err := WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), assignment)
	if err != nil {
		t.Fatal(err)
	}
	shortened := assignment
	shortened.SnapshotRevision++
	shortened.LeaseUntilMs--
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: &countingResolver{assignment: shortened}, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true, holder: assignment.HolderPod, boot: assignment.HolderBootID, assign: assignment.AssignmentRevision, writer: assignment.WriterEpoch, lease: assignment.LeaseEpoch},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.Validate(context.Background(), request); !errors.Is(err, ErrFirstHopStale) {
		t.Fatalf("shortened deadline accepted as deadline-only update: %v", err)
	}
}

func TestFirstHopRejectsStaleShardAndLeaseBeforeMailbox(t *testing.T) {
	resolver, err := actorrouting.NewStaticResolver(firstHopSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	request, err := WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), assignment)
	if err != nil {
		t.Fatal(err)
	}
	request.Dispatch.ActorRoute.VirtualShard++
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = validator.Validate(context.Background(), request)
	var firstHopErr *FirstHopError
	if !errors.As(err, &firstHopErr) || !errors.Is(err, ErrFirstHopShardMismatch) || firstHopErr.Redirect == nil || firstHopErr.Redirect.GlobalRevision <= 0 {
		t.Fatalf("stale shard err=%v first_hop=%+v", err, firstHopErr)
	}

	request, err = WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), assignment)
	if err != nil {
		t.Fatal(err)
	}
	validator, err = NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.Validate(context.Background(), request); !errors.Is(err, ErrFirstHopLease) {
		t.Fatalf("expired local lease was not fail-closed: %v", err)
	}
	validator, err = NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true, holder: assignment.HolderPod, boot: assignment.HolderBootID, assign: assignment.AssignmentRevision, writer: assignment.WriterEpoch + 1, lease: assignment.LeaseEpoch},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.Validate(context.Background(), request); !errors.Is(err, ErrFirstHopLease) {
		t.Fatalf("local writer epoch mismatch was accepted: %v", err)
	}
}

func TestFirstHopDomainIdentityDoesNotShareRouteSpace(t *testing.T) {
	resolver, err := actorrouting.NewStaticResolver(firstHopSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	player, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 77)
	if err != nil {
		t.Fatal(err)
	}
	farm, err := resolver.ResolveActor(context.Background(), actorrouting.DomainFarm, 77)
	if err != nil {
		t.Fatal(err)
	}
	if player.HolderPod == farm.HolderPod || player.WriterEpoch == farm.WriterEpoch {
		// The test snapshot intentionally gives the two domain tables distinct
		// holder/epoch tuples for the same actor ID.
		t.Fatalf("domain route spaces shared: player=%+v farm=%+v", player, farm)
	}
}

func TestFirstHopRejectsCanceledContextBeforeStaticResolve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver := &countingResolver{}
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: testGenerationAdmission{allow: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = validator.Validate(ctx, firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42))
	if !errors.Is(err, context.Canceled) || resolver.calls != 0 {
		t.Fatalf("canceled request reached resolver: err=%v calls=%d", err, resolver.calls)
	}
}

func TestFirstHopRejectsCancellationRacingExecutionAccept(t *testing.T) {
	resolver, err := actorrouting.NewStaticResolver(firstHopSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	request, err := WithResolvedActorRoute(firstHopRequest(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, 42), assignment)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	validator, err := NewFirstHopRouteValidator(FirstHopRouteValidatorOptions{
		Realm: "classic-farm", Resolver: resolver, HolderID: "player-a", HolderBootID: "player-boot",
		LeaseAdmission: cancelingGenerationAdmission{cancel: cancel},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = validator.ValidateBeforeAccept(ctx, request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation after local admission entered accept: %v", err)
	}
}
